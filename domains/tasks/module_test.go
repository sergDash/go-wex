package tasks

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-wex/engine/clock"
	"go-wex/engine/core"
	"go-wex/engine/events"
	"go-wex/engine/jobs"
	"go-wex/engine/service"
)

// testRegistry — реестр для проверки объявлений модуля: запоминает маршруты
// и виды работ, ничего не запускает.
type testRegistry struct {
	routes   map[string]core.Handler
	specs    map[jobs.Kind]jobs.Spec
	commands map[string]core.CommandHandler
}

func newTestRegistry() *testRegistry {
	return &testRegistry{
		routes:   map[string]core.Handler{},
		specs:    map[jobs.Kind]jobs.Spec{},
		commands: map[string]core.CommandHandler{},
	}
}

func (r *testRegistry) HTTP(method, path string, h core.Handler) {
	r.routes[method+" "+path] = h
}

func (r *testRegistry) Job(kind string, spec jobs.Spec) error {
	r.specs[jobs.Kind(kind)] = spec
	return nil
}

func (r *testRegistry) Service(service.Service) {}

// Event не используется: модуль задач событий не объявляет.
func (r *testRegistry) Event(string, events.Rule) error { return nil }

func (r *testRegistry) Command(name string, h core.CommandHandler) error {
	r.commands[name] = h
	return nil
}

// newDispatcher собирает разборщик с тем же правилом вида, что объявляет
// модуль, но без запуска службы: проверки вызывают Step напрямую.
func newDispatcher(t *testing.T, m module) *jobs.Dispatcher {
	t.Helper()

	reg := jobs.NewRegistry()
	if err := reg.Add(kind, jobs.Spec{
		Class:       jobs.ClassInteractive,
		Handler:     m.run,
		MaxAttempts: 3,
		Backoff:     time.Second,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	return jobs.NewDispatcher(jobs.NewMemoryQueue(), reg)
}

// statusRequest собирает запрос с путём, как его разбирает ServeMux.
func statusRequest(id string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/api/tasks/"+id, nil)
	r.SetPathValue("id", id)
	return r
}

// TestRegister — модуль объявляет четыре маршрута и один вид работы.
func TestRegister(t *testing.T) {
	reg := newTestRegistry()
	if err := (module{}).Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for _, key := range []string{
		"POST /api/tasks",
		"GET /api/tasks/{id}",
		"POST /api/tasks/pause",
		"POST /api/tasks/resume",
	} {
		if _, ok := reg.routes[key]; !ok {
			t.Fatalf("маршрут %q не объявлен", key)
		}
	}

	spec, ok := reg.specs[kind]
	if !ok || spec.Handler == nil {
		t.Fatalf("вид работы %q: spec=%+v ok=%v", kind, spec, ok)
	}
	if spec.Class != jobs.ClassInteractive {
		t.Fatalf("класс вида: %q", spec.Class)
	}
	if _, ok := reg.commands["task"]; !ok {
		t.Fatal("подкоманда task не объявлена")
	}
}

// TestEnqueueAndRun — запрос ставит задачу, шаг разборщика выполняет её,
// статус виден отдельным запросом.
func TestEnqueueAndRun(t *testing.T) {
	m := module{}
	m.disp = newDispatcher(t, m)
	ctx := context.Background()

	value, err := m.enqueue(httptest.NewRequest(http.MethodPost, "/api/tasks",
		strings.NewReader(`{"text":"раз"}`)))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	queued, ok := value.(taskView)
	if !ok || queued.Status != jobs.StatusQueued || queued.ID == "" {
		t.Fatalf("ответ постановки: %+v", value)
	}
	if queued.Class != jobs.ClassInteractive {
		t.Fatalf("класс по умолчанию: %q", queued.Class)
	}

	if handled, err := m.disp.Step(ctx); err != nil || !handled {
		t.Fatalf("Step: handled=%v err=%v", handled, err)
	}

	value, err = m.status(statusRequest(queued.ID))
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	done, ok := value.(taskView)
	if !ok || done.Status != jobs.StatusDone || done.Attempts != 1 || done.Text != "раз" {
		t.Fatalf("ответ статуса: %+v", value)
	}
}

// TestRetry — нарочитая ошибка возвращает работу в очередь, следующая
// попытка выполняется после задержки.
func TestRetry(t *testing.T) {
	clk := clock.NewManual(time.Unix(1000, 0))
	m := module{}
	m.disp = newDispatcher(t, m)
	m.disp.SetClock(clk)
	ctx := context.Background()

	value, err := m.enqueue(httptest.NewRequest(http.MethodPost, "/api/tasks",
		strings.NewReader(`{"text":"раз","failUntil":1}`)))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	id := value.(taskView).ID

	if handled, err := m.disp.Step(ctx); !handled || err == nil {
		t.Fatalf("первая попытка: handled=%v err=%v", handled, err)
	}
	if _, status, _, _ := m.disp.Get(ctx, id); status != jobs.StatusQueued {
		t.Fatalf("после ошибки статус %q", status)
	}

	clk.Advance(time.Second)
	if handled, err := m.disp.Step(ctx); !handled || err != nil {
		t.Fatalf("вторая попытка: handled=%v err=%v", handled, err)
	}
	job, status, _, _ := m.disp.Get(ctx, id)
	if status != jobs.StatusDone || job.Attempts != 2 {
		t.Fatalf("после повтора: status=%q attempts=%d", status, job.Attempts)
	}
}

// TestPauseResume — пауза не даёт взять следующую задачу, продолжение даёт.
func TestPauseResume(t *testing.T) {
	m := module{}
	m.disp = newDispatcher(t, m)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := m.enqueue(httptest.NewRequest(http.MethodPost, "/api/tasks",
			strings.NewReader(`{"text":"раз"}`))); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	if _, err := m.pause(httptest.NewRequest(http.MethodPost, "/api/tasks/pause", nil)); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if !m.disp.Paused() {
		t.Fatal("пауза не отмечена")
	}
	if handled, _ := m.disp.Step(ctx); handled {
		t.Fatal("на паузе задача взята")
	}

	if _, err := m.resume(httptest.NewRequest(http.MethodPost, "/api/tasks/resume", nil)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if m.disp.Paused() {
		t.Fatal("продолжение не отмечено")
	}
	if handled, err := m.disp.Step(ctx); err != nil || !handled {
		t.Fatalf("после продолжения: handled=%v err=%v", handled, err)
	}
}

// TestEnqueueValidation — пустой текст даёт ошибку с кодом 400, а не 500.
func TestEnqueueValidation(t *testing.T) {
	_, err := (module{}).enqueue(httptest.NewRequest(http.MethodPost, "/api/tasks",
		strings.NewReader(`{"text":"  "}`)))
	if err == nil {
		t.Fatal("пустой текст принят")
	}

	var e *core.Error
	if !errors.As(err, &e) || e.Status != http.StatusBadRequest {
		t.Fatalf("ошибка: %v", err)
	}
}

// TestClassValidation — класс из запроса проверяется, интерактивный
// сохраняется в работе.
func TestClassValidation(t *testing.T) {
	_, err := (module{}).enqueue(httptest.NewRequest(http.MethodPost, "/api/tasks",
		strings.NewReader(`{"text":"раз","class":"urgent"}`)))
	var e *core.Error
	if !errors.As(err, &e) || e.Status != http.StatusBadRequest {
		t.Fatalf("неизвестный класс: %v", err)
	}

	m := module{}
	m.disp = newDispatcher(t, m)
	value, err := m.enqueue(httptest.NewRequest(http.MethodPost, "/api/tasks",
		strings.NewReader(`{"text":"раз","class":"interactive"}`)))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	view := value.(taskView)
	if view.Class != jobs.ClassInteractive {
		t.Fatalf("класс: %q", view.Class)
	}
}

// TestCommand — подкоманда выполняет задачу без служб и отвергает пустой
// текст и нарочитую ошибку.
func TestCommand(t *testing.T) {
	m := module{}
	if err := m.command(context.Background(), []string{"-text", "раз"}); err != nil {
		t.Fatalf("command: %v", err)
	}
	if err := m.command(context.Background(), nil); err == nil {
		t.Fatal("пустой текст принят")
	}
	if err := m.command(context.Background(), []string{"-text", "раз", "-failUntil", "1"}); err == nil {
		t.Fatal("нарочитая ошибка не вернулась")
	}
}
