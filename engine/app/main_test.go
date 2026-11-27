package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"go-wex/engine/activity"
	"go-wex/engine/clock"
	"go-wex/engine/config"
	"go-wex/engine/core"
	"go-wex/engine/httpapi"
	"go-wex/engine/jobs"
	"go-wex/engine/lang"
	"go-wex/engine/process"
	"go-wex/engine/schedule"
)

// namedModule — модуль с заданным именем: для проверки отбора модулей.
type namedModule struct{ name string }

func (m namedModule) Name() string               { return m.name }
func (namedModule) Register(core.Registry) error { return nil }

// TestSelectModules — выключенные модули убираются, порядок остальных
// сохраняется, а неизвестное имя даёт ошибку с перечнем известных.
func TestSelectModules(t *testing.T) {
	all := []core.Module{namedModule{"health"}, namedModule{"tasks"}, namedModule{"demo"}}

	got, err := selectModules(all, nil, nil)
	if err != nil || len(got) != 3 {
		t.Fatalf("без отключений: %d модулей, ошибка %v", len(got), err)
	}

	got, err = selectModules(all, []string{"demo"}, nil)
	if err != nil {
		t.Fatalf("отключение demo: %v", err)
	}
	if len(got) != 2 || got[0].Name() != "health" || got[1].Name() != "tasks" {
		t.Fatalf("после отключения: %v", moduleNames(got))
	}

	_, err = selectModules(all, []string{"dmeo"}, nil)
	if err == nil {
		t.Fatal("опечатка в имени модуля принята")
	}
	if !strings.Contains(err.Error(), "health") {
		t.Fatalf("ошибка без перечня известных модулей: %v", err)
	}
}

// TestRestartRule — правило перезапуска читается из настроек: без предела
// перезапуска нет, срок по умолчанию — одна секунда.
func TestRestartRule(t *testing.T) {
	delay, attempts, err := restartRule(config.Restart{})
	if err != nil || delay != 0 || attempts != 0 {
		t.Fatalf("без правила: delay=%s attempts=%d err=%v", delay, attempts, err)
	}

	delay, attempts, err = restartRule(config.Restart{Max: 2})
	if err != nil || delay != time.Second || attempts != 2 {
		t.Fatalf("срок по умолчанию: delay=%s attempts=%d err=%v", delay, attempts, err)
	}

	delay, attempts, err = restartRule(config.Restart{After: "3s", Max: 1})
	if err != nil || delay != 3*time.Second || attempts != 1 {
		t.Fatalf("свой срок: delay=%s attempts=%d err=%v", delay, attempts, err)
	}
}

// TestOpenBrowserStopsOnCancel — открытие страницы не ждёт порт, если
// процесс останавливают: горутина завершается по отмене контекста.
func TestOpenBrowserStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		openBrowser(ctx, "127.0.0.1:1")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("открытие не остановилось по отмене контекста")
	}
}

// fakeService — служба для проверки сбора: ничего не делает.
type fakeService struct{}

func (fakeService) Name() string                { return "fake" }
func (fakeService) Start(context.Context) error { return nil }
func (fakeService) Stop(context.Context) error  { return nil }

// TestRegistryDelegates — реестр сборки отдаёт маршруты HTTP-службе,
// виды работ — реестру работ, службы — общему списку.
func TestRegistryDelegates(t *testing.T) {
	api := httpapi.New("127.0.0.1:0", fstest.MapFS{})
	reg := &registry{
		api:      api,
		jobs:     jobs.NewRegistry(),
		commands: process.NewRegistry(),
	}

	reg.HTTP("GET", "/api/check", func(*http.Request) (any, error) { return "ok", nil })
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/check", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("маршрут: код %d", rec.Code)
	}

	spec := jobs.Spec{Handler: func(context.Context, jobs.Job) error { return nil }}
	if err := reg.Job("check", spec); err != nil {
		t.Fatalf("Job: %v", err)
	}
	if err := reg.Job("check", spec); err == nil {
		t.Fatal("повторный вид работы принят")
	}

	reg.Service(fakeService{})
	if len(reg.services) != 1 || reg.services[0].Name() != "fake" {
		t.Fatalf("службы: %+v", reg.services)
	}

	if err := reg.Command("check", func(context.Context, []string) error { return nil }); err != nil {
		t.Fatalf("Command: %v", err)
	}
	if _, ok := reg.commands.Lookup("check"); !ok {
		t.Fatal("подкоманда не собрана")
	}
}

// TestScheduleRules — настройки переводятся в пороги; пустой список даёт
// оба класса без ограничения, неверный срок отвергается.
func TestScheduleRules(t *testing.T) {
	rules, err := scheduleRules(nil)
	if err != nil {
		t.Fatalf("scheduleRules: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("пустой список: %d порогов", len(rules))
	}

	rules, err = scheduleRules([]config.ScheduleRule{{After: "5m", Class: "background"}})
	if err != nil {
		t.Fatalf("scheduleRules: %v", err)
	}
	if len(rules) != 1 || rules[0].Idle != 5*time.Minute || rules[0].Class != jobs.ClassBackground {
		t.Fatalf("порог: %+v", rules)
	}

	if _, err := scheduleRules([]config.ScheduleRule{{After: "пять минут", Class: "background"}}); err == nil {
		t.Fatal("неверный срок принят")
	}
}

// TestAllowedNow — пока запрос активен или службы не готовы, фон убирается
// из набора классов.
func TestAllowedNow(t *testing.T) {
	clk := clock.NewManual(time.Unix(1000, 0))
	meter := activity.New(clk)
	sched, err := schedule.New([]schedule.Rule{
		{Idle: 0, Class: jobs.ClassInteractive},
		{Idle: 0, Class: jobs.ClassBackground},
	})
	if err != nil {
		t.Fatalf("schedule.New: %v", err)
	}

	got := allowedNow(sched, meter, clk.Now(), true)
	if len(got) != 2 {
		t.Fatalf("свободно: %v", got)
	}

	done := meter.Begin()
	got = allowedNow(sched, meter, clk.Now(), true)
	if len(got) != 1 || got[0] != jobs.ClassInteractive {
		t.Fatalf("запрос активен: %v", got)
	}

	done()
	got = allowedNow(sched, meter, clk.Now(), true)
	if len(got) != 2 {
		t.Fatalf("после запроса: %v", got)
	}

	got = allowedNow(sched, meter, clk.Now(), false)
	if len(got) != 1 || got[0] != jobs.ClassInteractive {
		t.Fatalf("службы не готовы: %v", got)
	}
}

// TestModuleSettings — настройки принимаются только для объявленных и
// включённых модулей: опечатка и мёртвая настройка — ошибка.
func TestModuleSettings(t *testing.T) {
	all := []core.Module{namedModule{"health"}, namedModule{"tasks"}, namedModule{"demo"}}
	ok := map[string]json.RawMessage{"demo": json.RawMessage(`{"minIdle":"1s"}`)}

	got, err := selectModules(all, nil, ok)
	if err != nil || len(got) != 3 {
		t.Fatalf("с настройками: %d модулей, ошибка %v", len(got), err)
	}

	if _, err := selectModules(all, nil, map[string]json.RawMessage{"dmeo": json.RawMessage(`{}`)}); err == nil {
		t.Fatal("настройки неизвестного модуля приняты")
	}
	if _, err := selectModules(all, []string{"demo"}, ok); err == nil {
		t.Fatal("настройки выключенного модуля приняты")
	}
}

// testCatalog собирает словари во временном каталоге: модуль записи берёт
// фразы оттуда, поэтому сборке нужен настоящий каталог, а не заглушка.
func testCatalog(t *testing.T) *lang.Catalog {
	t.Helper()

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ru-RU"), 0o755); err != nil {
		t.Fatalf("каталог локали: %v", err)
	}
	body := `{"written": "строка", "title": "Заголовок"}`
	if err := os.WriteFile(filepath.Join(dir, "ru-RU", "log_ru.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("словарь: %v", err)
	}

	catalog, err := lang.Open(dir, "ru-RU")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return catalog
}

// TestBuildModulesSettings — настройки доходят до модуля, а ошибка разбора
// останавливает сборку с понятным текстом.
func TestBuildModulesSettings(t *testing.T) {
	catalog := testCatalog(t)

	modules, err := buildModules(deps{lang: catalog, settings: map[string]json.RawMessage{
		"demo": json.RawMessage(`{"tickPeriod":"2s"}`),
	}})
	if err != nil {
		t.Fatalf("buildModules: %v", err)
	}
	if len(modules) != 4 {
		t.Fatalf("модулей: %d", len(modules))
	}

	_, err = buildModules(deps{lang: catalog, settings: map[string]json.RawMessage{
		"demo": json.RawMessage(`{"period":"2s"}`),
	}})
	if err == nil || !strings.Contains(err.Error(), "demo") {
		t.Fatalf("ошибка настроек: %v", err)
	}
}

// TestBuildModulesNeedsCatalog — модуль записи требует словарь: без него
// сборка останавливается с понятным текстом, а не пишет пустые строки.
func TestBuildModulesNeedsCatalog(t *testing.T) {
	if _, err := buildModules(deps{}); err == nil {
		t.Fatal("сборка без словаря прошла")
	}
}
