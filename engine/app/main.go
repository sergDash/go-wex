// Точка сборки: читает настройки, находит интерфейс, собирает службы,
// перечисляет модули и запускает движок. Ни маршрутов, ни предметных правил
// здесь нет.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"go-wex/domains/demo"
	"go-wex/domains/health"
	"go-wex/domains/tasks"
	"go-wex/engine/activity"
	"go-wex/engine/clock"
	"go-wex/engine/config"
	"go-wex/engine/core"
	"go-wex/engine/events"
	"go-wex/engine/gate"
	"go-wex/engine/httpapi"
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/lang"
	"go-wex/engine/process"
	"go-wex/engine/schedule"
	"go-wex/engine/service"
	"go-wex/engine/store"
	"go-wex/engine/version"
)

func main() {
	os.Exit(run())
}

// run разбирает настройки и выбирает режим: подкоманда или службы.
func run() int {
	cfgPath := flag.String("config", filepath.Join("config", "httpapi.json"), "файл настроек")
	webDir := flag.String("web", "", "переопределить каталог интерфейса из настроек")
	flag.Parse()

	cfg, err := config.LoadHTTPAPI(*cfgPath)
	if err != nil {
		log.Printf("%v", err)
		return 1
	}

	// Первый аргумент выбирает режим: подкоманда выполняет одну работу
	// с настоящими зависимостями из тех же настроек, службы не поднимает.
	if args := flag.Args(); len(args) > 0 {
		return runCommand(cfg, args)
	}
	return runServer(cfg, *webDir)
}

// runCommand выполняет подкоманду вместо служб. Зависимости собираются из
// тех же настроек, что и у сервера: журнал открывается на время команды,
// очередь лежит поверх настоящего хранилища и открывает его при первом
// обращении. HTTP и службы не поднимаются: команда выполняет одну работу.
func runCommand(cfg config.HTTPAPI, args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clk := clock.NewReal()

	// Журнал команды: открывается до работы и закрывается после, чтобы её
	// записи не терялись. Без раздела настроек остаётся пустой приёмник.
	var jw journal.Writer = journal.Discard{}
	if cfg.Journal != nil {
		j := journal.New(cfg.Journal.Path, cfg.Journal.MaxSize, cfg.Journal.Keep, clk)
		if err := j.Start(ctx); err != nil {
			log.Printf("журнал: %v", err)
			return 1
		}
		defer func() { _ = j.Stop(context.Background()) }()
		jw = j
	}

	// Хранилище открывается при первом обращении: команда, которой оно
	// не нужно, не появляется вторым писателем рядом с сервером.
	queue := jobs.OpenStoreQueueLater(func() (*store.Store, error) {
		return store.Open(cfg.Store.Path, nil)
	})
	jobReg := jobs.NewRegistry()
	disp := jobs.NewDispatcher(queue, jobReg)
	disp.SetClock(clk)
	disp.SetJournal(jw)
	disp.SetKeep(cfg.Jobs.Keep)

	commands := process.NewRegistry()
	evReg := events.NewRegistry()

	// Служебных зависимостей в команде нет: службы не поднимаются, поэтому
	// список состояний пуст, а службы событий нет вовсе.
	modules, err := buildModules(deps{
		disp:     disp,
		journal:  jw,
		states:   func() []service.State { return nil },
		events:   nil,
		slot:     gate.New(1),
		settings: cfg.Modules.Settings,
	})
	if err != nil {
		log.Printf("модули: %v", err)
		return 1
	}
	modules, err = selectModules(modules, cfg.Modules.Disabled, cfg.Modules.Settings)
	if err != nil {
		log.Printf("настройки: %v", err)
		return 1
	}

	// HTTP в режиме подкоманды не нужен: маршруты собираются в пустой приёмник.
	reg := &registry{api: discardHTTP{}, jobs: jobReg, events: evReg, commands: commands}
	for _, m := range modules {
		if err := m.Register(reg); err != nil {
			log.Printf("модуль %s: %v", m.Name(), err)
			return 1
		}
	}

	name := args[0]
	handler, ok := commands.Lookup(name)
	if !ok {
		names := commands.Names()
		if len(names) == 0 {
			fmt.Fprintf(os.Stderr, "неизвестный режим %q; подкоманд нет\n", name)
		} else {
			fmt.Fprintf(os.Stderr, "неизвестный режим %q; известны: %s\n", name, strings.Join(names, ", "))
		}
		return 2
	}

	if err := handler(ctx, args[1:]); err != nil {
		log.Printf("подкоманда %s: %v", name, err)
		return 1
	}
	return 0
}

// runServer поднимает службы и работает до сигнала остановки.
func runServer(cfg config.HTTPAPI, webDir string) int {
	dir := cfg.Interface.Dir
	if webDir != "" {
		dir = webDir
	}
	dir, err := resolveDir(dir)
	if err != nil {
		log.Printf("интерфейс: %v", err)
		return 1
	}
	log.Printf("интерфейс: %s", dir)

	// Часы и учёт запросов общие: расписание считает простой по последней
	// активности, а занятость запросами убирает фон.
	clk := clock.NewReal()
	meter := activity.New(clk)

	// Журнал работы включается настройкой. Без неё используется пустой
	// приёмник и отдельной службы не появляется.
	var jw journal.Writer = journal.Discard{}
	var journalSvc *journal.Journal
	if cfg.Journal != nil {
		journalSvc = journal.New(cfg.Journal.Path, cfg.Journal.MaxSize, cfg.Journal.Keep, clk)
		jw = journalSvc
	}

	srv := httpapi.New(cfg.Server.Addr, os.DirFS(dir))
	srv.SetActivity(meter)
	srv.SetJournal(jw)

	// Пределы транспорта: медленный клиент не держит соединение, тело запроса
	// не растёт без границы. Значения приходят настройкой, а не из кода.
	readHeader, idle, err := serverTimeouts(cfg.Server)
	if err != nil {
		log.Printf("настройки: %v", err)
		return 1
	}
	srv.SetTimeouts(readHeader, idle)
	if cfg.Server.MaxBody != nil {
		srv.SetMaxBody(*cfg.Server.MaxBody)
	}

	// Хранилище открывается до служб: ошибка версии или повреждённого файла
	// должна быть видна сразу, а не при первом запросе.
	st, err := store.Open(cfg.Store.Path, nil)
	if err != nil {
		log.Printf("хранилище: %v", err)
		return 1
	}

	// Словари переводов открываются до регистрации: сломанный файл и
	// отсутствующая базовая локаль должны остановить запуск, а не всплыть
	// пустой страницей. Путь берётся относительно каталога запуска, как
	// у хранилища и журнала.
	var catalog *lang.Catalog
	if cfg.Lang != nil {
		catalog, err = lang.Open(cfg.Lang.Dir, cfg.Lang.Base)
		if err != nil {
			log.Printf("словари: %v", err)
			return 1
		}
		log.Printf("словари: %s, базовый язык %s", cfg.Lang.Dir, catalog.Base())
	}

	// Виды работ и очередь общие для модулей: модуль объявляет вид, а ставит
	// работу через разборщик. Очередь поверх хранилища переживает перезапуск.
	jobReg := jobs.NewRegistry()
	queue, err := jobs.OpenStoreQueue(st)
	if err != nil {
		log.Printf("очередь: %v", err)
		return 1
	}
	disp := jobs.NewDispatcher(queue, jobReg)
	disp.SetClock(clk)
	disp.SetJournal(jw)
	// Правило хранения: очередь ставит работу на каждый ход, поэтому старые
	// завершённые строки убираются, а не копятся до полной перезаписи файла.
	disp.SetKeep(cfg.Jobs.Keep)
	// Активность и занятость приходят из одного счётчика: простой считается
	// от последнего запроса, фон не берётся, пока запрос идёт. Пороги простоя
	// объявляют сами виды работ.
	disp.SetIdleFunc(func(time.Time) (time.Time, bool) {
		return meter.LastActive(), meter.Busy()
	})

	// Слот генерации: один ресурс на процесс. Синхронный ход и фоновая работа
	// берут одно и то же разрешение, поэтому не сталкиваются. Создаётся здесь
	// и передаётся зависимостью: глобальной переменной быть не должно.
	slot := gate.New(1)

	// Виды отложенных событий собираются от модулей. Срок каждого вида
	// лежит в том же хранилище, поэтому переживает перезапуск. Служба
	// создаётся до регистрации: модули получают её, чтобы добавлять события.
	evReg := events.NewRegistry()
	evSched := events.NewScheduler(st, disp, evReg)
	evSched.SetClock(clk)
	evSched.SetJournal(jw)

	// Расписание отвечает, какие классы допустимы при текущем простое;
	// пока идёт запрос, фон из набора убирается.
	rules, err := scheduleRules(cfg.Schedule.Rules)
	if err != nil {
		log.Printf("расписание: %v", err)
		return 1
	}
	sched, err := schedule.New(rules)
	if err != nil {
		log.Printf("расписание: %v", err)
		return 1
	}

	// Группа собирается после регистрации модулей, поэтому её видно только
	// через переменную: допуск к работам спрашивает её на каждом захвате.
	var group *service.Group

	disp.SetAllowedFunc(func(now time.Time) []jobs.Class {
		return allowedNow(sched, meter, now, readyAll(group))
	})

	commands := process.NewRegistry()
	reg := &registry{api: srv, jobs: jobReg, events: evReg, commands: commands}

	// Маршруты словаря объявляет сам механизм: словари — это движок, а не
	// модуль, поэтому список модулей на них не влияет.
	if catalog != nil {
		catalog.Register(reg)
	}

	// Состав служб известен только после регистрации модулей, поэтому модулю
	// передаётся функция: к моменту запроса группа уже собрана.
	var services []service.Service
	states := func() []service.State {
		if group == nil {
			return nil
		}
		return group.States()
	}

	modules, err := buildModules(deps{
		disp:     disp,
		journal:  jw,
		states:   states,
		events:   evSched,
		slot:     slot,
		settings: cfg.Modules.Settings,
	})
	if err != nil {
		log.Printf("модули: %v", err)
		return 1
	}
	modules, err = selectModules(modules, cfg.Modules.Disabled, cfg.Modules.Settings)
	if err != nil {
		log.Printf("настройки: %v", err)
		return 1
	}

	for _, name := range cfg.Modules.Disabled {
		log.Printf("модуль %s: выключен настройкой", name)
	}
	for _, m := range modules {
		if err := m.Register(reg); err != nil {
			log.Printf("модуль %s: %v", m.Name(), err)
			return 1
		}
		log.Printf("модуль: %s", m.Name())
	}

	// Журнал открывается первым, затем службы модулей, разборщик, события
	// и HTTP. При остановке порядок обратный: HTTP перестаёт принимать
	// запросы, события перестают ставить работы, разборщик доигрывает срок,
	// службы модулей и журнал закрываются после.
	services = make([]service.Service, 0, 3+len(reg.services))
	if journalSvc != nil {
		services = append(services, journalSvc)
	}
	services = append(services, reg.services...)
	services = append(services, disp, evSched, srv)
	group = service.New(services...)
	group.SetJournal(jw)

	// Правило перезапуска отказавших служб: без него группа только сообщает
	// об отказе, а процесс продолжает работу без службы.
	delay, attempts, err := restartRule(cfg.Services.Restart)
	if err != nil {
		log.Printf("настройки: %v", err)
		return 1
	}
	group.SetRestart(delay, attempts)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Страница открывается, когда службы уже подняты: отдельная горутина
	// ждёт порт, чтобы браузер не показал отказ соединения.
	if cfg.Server.Open {
		go openBrowser(ctx, cfg.Server.Addr)
	}

	log.Printf("go+wex %s: http://%s", version.String(), cfg.Server.Addr)
	if err := group.Run(ctx); err != nil {
		log.Printf("остановка: %v", err)
		return 1
	}
	return 0
}

// openBrowser открывает страницу, дождавшись, что служба заняла адрес.
// Запуск служб идёт после вызова, поэтому простой запуск браузера сразу
// приводил бы к отказу соединения.
func openBrowser(ctx context.Context, addr string) {
	url := "http://" + addr + "/"

	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			log.Printf("браузер: служба %s не заняла адрес за 5 с", addr)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	if err := exec.Command(browserCommand(), url).Run(); err != nil {
		log.Printf("браузер: %s: %v", url, err)
	}
}

// browserCommand — чем открыть страницу: xdg-open в Linux, open в macOS.
func browserCommand() string {
	if runtime.GOOS == "darwin" {
		return "open"
	}
	return "xdg-open"
}

func selectModules(all []core.Module, disabled []string, settings map[string]json.RawMessage) ([]core.Module, error) {
	known := make(map[string]bool, len(all))
	for _, m := range all {
		known[m.Name()] = true
	}
	for name := range settings {
		if !known[name] {
			return nil, fmt.Errorf("настройки модуля %q: модуль не объявлен; известны: %s", name, strings.Join(moduleNames(all), ", "))
		}
	}
	for _, name := range disabled {
		if !known[name] {
			return nil, fmt.Errorf("модуль %q не объявлен; известны: %s", name, strings.Join(moduleNames(all), ", "))
		}
		if _, ok := settings[name]; ok {
			return nil, fmt.Errorf("модуль %q выключен, а его настройки заданы", name)
		}
		delete(known, name)
	}

	out := make([]core.Module, 0, len(all))
	for _, m := range all {
		if known[m.Name()] {
			out = append(out, m)
		}
	}
	return out, nil
}

// moduleNames — имена модулей по возрастанию: для сообщения об опечатке
// и для порядка в журнале.
func moduleNames(modules []core.Module) []string {
	names := make([]string, 0, len(modules))
	for _, m := range modules {
		names = append(names, m.Name())
	}
	sort.Strings(names)
	return names
}

// deps — зависимости, которые получают модули. В режиме служб они
// настоящие; в режиме подкоманды очередь открывает хранилище при первом
// обращении, а службы событий нет: службы не поднимаются.
type deps struct {
	disp     *jobs.Dispatcher
	journal  journal.Writer
	states   func() []service.State
	events   *events.Scheduler
	slot     *gate.Gate
	settings map[string]json.RawMessage
}

// buildModules собирает модули с зависимостями и их настройками. Это
// единственное место, где приложение знает список модулей и то, какой
// раздел настроек кому принадлежит.
func buildModules(d deps) ([]core.Module, error) {
	section := func(name string) json.RawMessage {
		return d.settings[name]
	}

	healthModule, err := health.New(section("health"))
	if err != nil {
		return nil, err
	}
	tasksModule, err := tasks.New(d.disp, section("tasks"))
	if err != nil {
		return nil, err
	}
	demoModule, err := demo.New(d.disp, d.journal, d.states, d.events, d.slot, section("demo"))
	if err != nil {
		return nil, err
	}

	return []core.Module{healthModule, tasksModule, demoModule}, nil
}

// resolveDir выбирает каталог интерфейса.
//
// Явный -web используется как есть. Без флага проверяется значение из
// настроек рядом с каталогом запуска, на уровень выше и на два уровня выше:
// сервер запускают и из корня (go run ./engine/app), и из каталога сборки.
// Проверка идёт по index.html, чтобы ошибка была внятной сразу.
func resolveDir(dir string) (string, error) {
	candidates := []string{dir}
	if !filepath.IsAbs(dir) {
		candidates = append(candidates,
			filepath.Join("..", dir),
			filepath.Join("..", "..", dir),
		)
	}
	for _, c := range candidates {
		if err := checkIndex(c); err == nil {
			return c, nil
		}
	}
	return "", fmt.Errorf("каталог %q не найден; запустите из корня проекта или укажите -web", dir)
}

func checkIndex(dir string) error {
	info, err := os.Stat(filepath.Join(dir, "index.html"))
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("%s/index.html — каталог", dir)
	}
	return nil
}

// httpRegistrar — то, куда модуль объявляет маршруты. В режиме подкоманды
// это пустой приёмник: службы не поднимаются.
type httpRegistrar interface {
	HTTP(method, path string, h core.Handler)
	HTTPRaw(method, path string, h http.Handler)
}

// discardHTTP — пустой приёмник маршрутов для режима подкоманды.
type discardHTTP struct{}

func (discardHTTP) HTTP(string, string, core.Handler) {}

func (discardHTTP) HTTPRaw(string, string, http.Handler) {}

// registry собирает возможности модуля: маршруты уходят HTTP-службе,
// виды работ — реестру работ, сроки событий — реестру событий, службы —
// в общий список запуска, подкоманды — реестру режимов.
type registry struct {
	api      httpRegistrar
	jobs     *jobs.Registry
	events   *events.Registry
	commands *process.Registry
	services []service.Service
}

func (r *registry) HTTP(method, path string, h core.Handler) {
	r.api.HTTP(method, path, h)
}

func (r *registry) HTTPRaw(method, path string, h http.Handler) {
	r.api.HTTPRaw(method, path, h)
}

func (r *registry) Job(kind string, spec jobs.Spec) error {
	return r.jobs.Add(jobs.Kind(kind), spec)
}

func (r *registry) Event(kind string, rule events.Rule) error {
	return r.events.Add(events.Kind(kind), rule)
}

func (r *registry) Service(s service.Service) {
	r.services = append(r.services, s)
}

func (r *registry) Command(name string, run core.CommandHandler) error {
	return r.commands.Add(name, process.Handler(run))
}

// readyAll сообщает, готовы ли службы принимать работу. Пока служба
// прогревается, фоновые работы не берутся: иначе первые из них уйдут
// в отказ на пустом месте.
func readyAll(g *service.Group) bool {
	if g == nil {
		return true
	}
	return g.Ready()
}

// restartRule переводит настройку перезапуска служб. Без правила группа
// не перезапускает ничего: сколько пробовать — решение приложения.
func restartRule(r config.Restart) (time.Duration, int, error) {
	if r.Max <= 0 {
		return 0, 0, nil
	}

	delay := time.Second
	if r.After != "" {
		d, err := time.ParseDuration(r.After)
		if err != nil {
			return 0, 0, err
		}
		delay = d
	}
	return delay, r.Max, nil
}

// serverTimeouts переводит сроки транспорта из настроек. Пустое значение
// оставляет предел службы по умолчанию: строка настроек проверена при загрузке.
func serverTimeouts(s config.Server) (time.Duration, time.Duration, error) {
	var readHeader, idle time.Duration
	var err error

	if s.ReadHeaderTimeout != "" {
		readHeader, err = time.ParseDuration(s.ReadHeaderTimeout)
		if err != nil {
			return 0, 0, err
		}
	}
	if s.IdleTimeout != "" {
		idle, err = time.ParseDuration(s.IdleTimeout)
		if err != nil {
			return 0, 0, err
		}
	}
	return readHeader, idle, nil
}

// scheduleRules переводит настройки в пороги расписания. Без правил
// разрешены оба класса: расписание не ограничивает работу.
func scheduleRules(rules []config.ScheduleRule) ([]schedule.Rule, error) {
	if len(rules) == 0 {
		return []schedule.Rule{
			{Idle: 0, Class: jobs.ClassInteractive},
			{Idle: 0, Class: jobs.ClassBackground},
		}, nil
	}

	out := make([]schedule.Rule, 0, len(rules))
	for i, r := range rules {
		idle, err := time.ParseDuration(r.After)
		if err != nil {
			return nil, fmt.Errorf("порог %d: %w", i, err)
		}
		out = append(out, schedule.Rule{Idle: idle, Class: jobs.Class(r.Class)})
	}
	return out, nil
}

// allowedNow — классы, которые разборщик может взять сейчас: расписание
// по простою, кроме фона, пока есть активный запрос или службы не готовы.
func allowedNow(s *schedule.Schedule, m *activity.Meter, now time.Time, ready bool) []jobs.Class {
	classes := s.Allowed(m.LastActive(), now)
	if !m.Busy() && ready {
		return classes
	}

	out := make([]jobs.Class, 0, len(classes))
	for _, c := range classes {
		if c != jobs.ClassBackground {
			out = append(out, c)
		}
	}
	return out
}
