// Точка сборки: читает настройки, находит интерфейс, собирает службы,
// перечисляет модули и запускает движок. Ни маршрутов, ни предметных правил
// здесь нет.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	"go-wex/engine/httpapi"
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/process"
	"go-wex/engine/schedule"
	"go-wex/engine/service"
	"go-wex/engine/store"
)

func main() {
	os.Exit(run())
}

// run разбирает настройки и выбирает режим: подкоманда или службы.
func run() int {
	cfgPath := flag.String("config", filepath.Join("config", "httpapi.json"), "файл настроек")
	webDir := flag.String("web", "", "переопределить каталог интерфейса из настроек")
	flag.Parse()

	// Первый аргумент разбирается до настроек и служб: подкоманда не должна
	// зависеть от файла настроек, который нужен службам.
	if args := flag.Args(); len(args) > 0 {
		return runCommand(args)
	}

	cfg, err := config.LoadHTTPAPI(*cfgPath)
	if err != nil {
		log.Printf("%v", err)
		return 1
	}
	return runServer(cfg, *webDir)
}

// runCommand выполняет подкоманду вместо служб: HTTP, хранилище, журнал
// и разборщик не поднимаются. Модули всё равно регистрируются, потому что
// подкоманду объявляет модуль.
func runCommand(args []string) int {
	jobReg := jobs.NewRegistry()
	disp := jobs.NewDispatcher(jobs.NewMemoryQueue(), jobReg)
	commands := process.NewRegistry()
	evReg := events.NewRegistry()

	// HTTP в режиме подкоманды не нужен: маршруты собираются в пустой приёмник.
	reg := &registry{api: discardHTTP{}, jobs: jobReg, events: evReg, commands: commands}
	for _, m := range moduleList(disp, journal.Discard{}) {
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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

	// Хранилище открывается до служб: ошибка версии или повреждённого файла
	// должна быть видна сразу, а не при первом запросе.
	st, err := store.Open(cfg.Store.Path, nil)
	if err != nil {
		log.Printf("хранилище: %v", err)
		return 1
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

	// Виды отложенных событий собираются от модулей. Срок каждого вида
	// лежит в том же хранилище, поэтому переживает перезапуск.
	evReg := events.NewRegistry()

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
	disp.SetAllowedFunc(func(now time.Time) []jobs.Class {
		return allowedNow(sched, meter, now)
	})

	commands := process.NewRegistry()
	reg := &registry{api: srv, jobs: jobReg, events: evReg, commands: commands}
	for _, m := range moduleList(disp, jw) {
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
	evSched := events.NewScheduler(st, disp, evReg)
	evSched.SetClock(clk)
	evSched.SetJournal(jw)

	services := make([]service.Service, 0, 3+len(reg.services))
	if journalSvc != nil {
		services = append(services, journalSvc)
	}
	services = append(services, reg.services...)
	services = append(services, disp, evSched, srv)
	group := service.New(services...)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("go+wex: http://%s", cfg.Server.Addr)
	if err := group.Run(ctx); err != nil {
		log.Printf("остановка: %v", err)
		return 1
	}
	return 0
}

// moduleList — единственное место, где приложение знает о модулях.
func moduleList(disp *jobs.Dispatcher, jw journal.Writer) []core.Module {
	return []core.Module{
		health.New(),
		tasks.New(disp),
		demo.New(disp, jw),
	}
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
}

// discardHTTP — пустой приёмник маршрутов для режима подкоманды.
type discardHTTP struct{}

func (discardHTTP) HTTP(string, string, core.Handler) {}

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
// по простою, кроме фона, пока есть активный запрос.
func allowedNow(s *schedule.Schedule, m *activity.Meter, now time.Time) []jobs.Class {
	classes := s.Allowed(m.LastActive(), now)
	if !m.Busy() {
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
