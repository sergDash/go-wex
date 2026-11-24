package demo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-wex/engine/core"
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/listen"
	"go-wex/engine/service"
)

// testRegistry — реестр для проверки объявлений модуля: запоминает всё,
// ничего не запускает.
type testRegistry struct {
	routes   map[string]core.Handler
	specs    map[jobs.Kind]jobs.Spec
	services []service.Service
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

func (r *testRegistry) Service(s service.Service) {
	r.services = append(r.services, s)
}

func (r *testRegistry) Command(name string, h core.CommandHandler) error {
	r.commands[name] = h
	return nil
}

// TestRegister — модуль объявляет маршруты, работу, три службы и подкоманду.
func TestRegister(t *testing.T) {
	reg := newTestRegistry()
	if err := New(nil, nil).Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for _, key := range []string{
		"GET /api/demo",
		"POST /api/demo/jobs",
		"GET /api/demo/jobs/{id}",
	} {
		if _, ok := reg.routes[key]; !ok {
			t.Fatalf("маршрут %q не объявлен", key)
		}
	}
	if spec, ok := reg.specs[kind]; !ok || spec.Handler == nil || spec.Class != jobs.ClassBackground {
		t.Fatalf("вид работы: %+v", spec)
	}
	if len(reg.services) != 3 {
		t.Fatalf("служб: %d", len(reg.services))
	}
	if _, ok := reg.commands["demo"]; !ok {
		t.Fatal("подкоманда demo не объявлена")
	}
}

// TestJob — работа ставится, выполняется и видна по номеру.
func TestJob(t *testing.T) {
	reg := jobs.NewRegistry()
	if err := reg.Add(kind, jobs.Spec{
		Class:       jobs.ClassBackground,
		Handler:     (&module{}).run,
		MaxAttempts: 2,
		Backoff:     time.Second,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := jobs.NewDispatcher(jobs.NewMemoryQueue(), reg)
	m := &module{disp: d, journal: journal.Discard{}}
	ctx := context.Background()

	value, err := m.enqueue(httptest.NewRequest(http.MethodPost, "/api/demo/jobs",
		strings.NewReader(`{"text":"раз"}`)))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	queued, ok := value.(jobView)
	if !ok || queued.Status != jobs.StatusQueued || queued.ID == "" {
		t.Fatalf("ответ постановки: %+v", value)
	}

	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("Step: handled=%v err=%v", handled, err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/demo/jobs/"+queued.ID, nil)
	req.SetPathValue("id", queued.ID)
	value, err = m.status(req)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	done := value.(jobView)
	if done.Status != jobs.StatusDone || done.Text != "раз" || done.Attempts != 1 {
		t.Fatalf("ответ статуса: %+v", done)
	}
}

// TestCommand — обычный режим печатает снимок, режим child живёт до отмены.
func TestCommand(t *testing.T) {
	m := &module{journal: journal.Discard{}}
	m.tcp = listen.NewTCP("127.0.0.1:0", m.echoTCP, journal.Discard{})
	m.udp = listen.NewUDP("127.0.0.1:0", m.echoUDP, journal.Discard{})

	if err := m.command(context.Background(), nil); err != nil {
		t.Fatalf("command: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.command(ctx, []string{"child"}); err != nil {
		t.Fatalf("command child: %v", err)
	}
}

// TestEnqueueValidation — пустой текст даёт ошибку с кодом 400.
func TestEnqueueValidation(t *testing.T) {
	_, err := (&module{}).enqueue(httptest.NewRequest(http.MethodPost, "/api/demo/jobs",
		strings.NewReader(`{"text":"  "}`)))
	var e *core.Error
	if err == nil || !errors.As(err, &e) || e.Status != http.StatusBadRequest {
		t.Fatalf("ошибка: %v", err)
	}
}
