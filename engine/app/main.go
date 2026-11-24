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
	"syscall"
	"time"

	"go-wex/domains/health"
	"go-wex/domains/tasks"
	"go-wex/engine/activity"
	"go-wex/engine/clock"
	"go-wex/engine/config"
	"go-wex/engine/core"
	"go-wex/engine/httpapi"
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/schedule"
	"go-wex/engine/service"
	"go-wex/engine/store"
)

func main() {
	cfgPath := flag.String("config", filepath.Join("config", "httpapi.json"), "файл настроек")
	webDir := flag.String("web", "", "переопределить каталог интерфейса из настроек")
	flag.Parse()

	cfg, err := config.LoadHTTPAPI(*cfgPath)
	if err != nil {
		log.Fatalf("%v", err)
	}

	dir := cfg.Interface.Dir
	if *webDir != "" {
		dir = *webDir
	}
	dir, err = resolveDir(dir)
	if err != nil {
		log.Fatalf("интерфейс: %v", err)
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
		log.Fatalf("хранилище: %v", err)
	}

	// Виды работ и очередь общие для модулей: модуль объявляет вид, а ставит
	// работу через разборщик. Очередь поверх хранилища переживает перезапуск.
	jobReg := jobs.NewRegistry()
	queue, err := jobs.OpenStoreQueue(st)
	if err != nil {
		log.Fatalf("очередь: %v", err)
	}
	disp := jobs.NewDispatcher(queue, jobReg)
	disp.SetClock(clk)
	disp.SetJournal(jw)

	// Расписание отвечает, какие классы допустимы при текущем простое;
	// пока идёт запрос, фон из набора убирается.
	rules, err := scheduleRules(cfg.Schedule.Rules)
	if err != nil {
		log.Fatalf("расписание: %v", err)
	}
	sched, err := schedule.New(rules)
	if err != nil {
		log.Fatalf("расписание: %v", err)
	}
	disp.SetAllowedFunc(func(now time.Time) []jobs.Class {
		return allowedNow(sched, meter, now)
	})

	// registry отдаёт модулю обе возможности: маршруты и работы.
	reg := registry{api: srv, jobs: jobReg}

	// Список модулей — единственное место, где приложение знает о них.
	modules := []core.Module{
		health.New(),
		tasks.New(disp),
	}
	for _, m := range modules {
		if err := m.Register(reg); err != nil {
			log.Fatalf("модуль %s: %v", m.Name(), err)
		}
		log.Printf("модуль: %s", m.Name())
	}

	// Журнал открывается первым, затем разборщик, затем HTTP. При остановке
	// порядок обратный: HTTP перестаёт принимать запросы, разборщик
	// доигрывает срок, журнал закрывает файл последним.
	services := []service.Service{disp, srv}
	if journalSvc != nil {
		services = append([]service.Service{journalSvc}, services...)
	}
	group := service.New(services...)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("go+wex: http://%s", cfg.Server.Addr)
	if err := group.Run(ctx); err != nil {
		log.Fatalf("остановка: %v", err)
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

// registry собирает возможности модуля: маршруты уходят HTTP-службе,
// виды работ — реестру работ. Сам модуль знает только форму из core.
type registry struct {
	api  *httpapi.Server
	jobs *jobs.Registry
}

func (r registry) HTTP(method, path string, h core.Handler) {
	r.api.HTTP(method, path, h)
}

func (r registry) Job(kind string, spec jobs.Spec) error {
	return r.jobs.Add(jobs.Kind(kind), spec)
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
