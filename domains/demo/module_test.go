package demo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-wex/engine/core"
	"go-wex/engine/events"
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/listen"
	"go-wex/engine/service"
	"go-wex/engine/store"
)

// testRegistry — реестр для проверки объявлений модуля: запоминает всё,
// ничего не запускает.
type testRegistry struct {
	routes   map[string]core.Handler
	specs    map[jobs.Kind]jobs.Spec
	events   map[events.Kind]events.Rule
	services []service.Service
	commands map[string]core.CommandHandler
}

func newTestRegistry() *testRegistry {
	return &testRegistry{
		routes:   map[string]core.Handler{},
		specs:    map[jobs.Kind]jobs.Spec{},
		events:   map[events.Kind]events.Rule{},
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

func (r *testRegistry) Event(kind string, rule events.Rule) error {
	r.events[events.Kind(kind)] = rule
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
	if err := New(nil, nil, nil, nil).Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for _, key := range []string{
		"GET /api/demo",
		"GET /api/services",
		"GET /api/events",
		"POST /api/events",
		"GET /api/demo/jobs",
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
	if spec, ok := reg.specs[eventKind]; !ok || spec.Handler == nil {
		t.Fatalf("вид работы события: %+v", spec)
	}
	if rule, ok := reg.events[events.Kind(eventKind)]; !ok || rule.Period != tickPeriod || rule.Payload == nil {
		t.Fatalf("отложенное событие: %+v", rule)
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

// TestServices — список служб отдаётся в том порядке, в каком его передала
// точка сборки; без поставщика список пуст.
func TestServices(t *testing.T) {
	m := &module{running: func() []string { return []string{"journal", "http"} }}

	value, err := m.services(httptest.NewRequest(http.MethodGet, "/api/services", nil))
	if err != nil {
		t.Fatalf("services: %v", err)
	}
	names, ok := value.([]string)
	if !ok || len(names) != 2 || names[0] != "journal" || names[1] != "http" {
		t.Fatalf("службы: %+v", value)
	}

	empty, err := (&module{}).services(httptest.NewRequest(http.MethodGet, "/api/services", nil))
	if err != nil {
		t.Fatalf("services без поставщика: %v", err)
	}
	if names, ok := empty.([]string); !ok || len(names) != 0 {
		t.Fatalf("пустой список: %+v", empty)
	}
}

// newEventScheduler собирает службу событий на временном хранилище.
func newEventScheduler(t *testing.T, d *jobs.Dispatcher) *events.Scheduler {
	t.Helper()

	st, err := store.Open(filepath.Join(t.TempDir(), "store.json"), nil)
	if err != nil {
		t.Fatalf("хранилище: %v", err)
	}
	return events.NewScheduler(st, d, events.NewRegistry())
}

// TestAddAndListEvents — форма добавляет событие, а список показывает его
// вместе с видом, объявленным кодом.
func TestAddAndListEvents(t *testing.T) {
	jobReg := jobs.NewRegistry()
	if err := jobReg.Add(kind, jobs.Spec{Class: jobs.ClassBackground, Handler: (&module{}).run}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := jobs.NewDispatcher(jobs.NewMemoryQueue(), jobReg)
	m := &module{disp: d, journal: journal.Discard{}, events: newEventScheduler(t, d)}

	value, err := m.addEvent(httptest.NewRequest(http.MethodPost, "/api/events",
		strings.NewReader(`{"text":"раз","seconds":5}`)))
	if err != nil {
		t.Fatalf("addEvent: %v", err)
	}
	if id := value.(map[string]string)["id"]; len(id) != 8 {
		t.Fatalf("номер события: %q", id)
	}

	value, err = m.eventsList(httptest.NewRequest(http.MethodGet, "/api/events", nil))
	if err != nil {
		t.Fatalf("eventsList: %v", err)
	}
	rows := value.([]eventRow)
	if len(rows) != 1 {
		t.Fatalf("событий %d, ожидалось 1", len(rows))
	}
	row := rows[0]
	if row.Kind != string(kind) || row.Period != "5s" || row.Declared || row.Next == nil {
		t.Fatalf("строка события: %+v", row)
	}
}

// TestEventValidation — пустой текст и период вне границ дают ответ 400.
func TestEventValidation(t *testing.T) {
	jobReg := jobs.NewRegistry()
	if err := jobReg.Add(kind, jobs.Spec{Class: jobs.ClassBackground, Handler: (&module{}).run}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := jobs.NewDispatcher(jobs.NewMemoryQueue(), jobReg)
	m := &module{journal: journal.Discard{}, events: newEventScheduler(t, d)}

	for _, body := range []string{
		`{"text":"  ","seconds":5}`,
		`{"text":"раз","seconds":0}`,
		`{"text":"раз","seconds":3601}`,
	} {
		_, err := m.addEvent(httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(body)))
		var e *core.Error
		if err == nil || !errors.As(err, &e) || e.Status != http.StatusBadRequest {
			t.Fatalf("тело %s: ошибка %v", body, err)
		}
	}
}

// TestQueue — очередь отдаётся таблицей: столбцы заполнены, свежая работа
// идёт первой, а работа в очереди показана без времени начала.
func TestQueue(t *testing.T) {
	reg := jobs.NewRegistry()
	if err := reg.Add(kind, jobs.Spec{
		Class:   jobs.ClassBackground,
		Handler: (&module{}).run,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := jobs.NewDispatcher(jobs.NewMemoryQueue(), reg)
	m := &module{disp: d, journal: journal.Discard{}}

	for _, text := range []string{"первая", "вторая"} {
		if _, err := m.enqueue(httptest.NewRequest(http.MethodPost, "/api/demo/jobs",
			strings.NewReader(`{"text":"`+text+`"}`))); err != nil {
			t.Fatalf("enqueue %q: %v", text, err)
		}
	}

	value, err := m.queue(httptest.NewRequest(http.MethodGet, "/api/demo/jobs", nil))
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	rows, ok := value.([]queueRow)
	if !ok {
		t.Fatalf("ответ %T, ожидался []queueRow", value)
	}
	if len(rows) != 2 {
		t.Fatalf("строк %d, ожидалось 2", len(rows))
	}
	// Порядок постановки: первая строка сверху, свежая внизу.
	if rows[0].Text != "первая" || rows[1].Text != "вторая" {
		t.Fatalf("порядок: %q, %q", rows[0].Text, rows[1].Text)
	}
	if rows[0].Status != jobs.StatusQueued || rows[0].CreatedAt == nil {
		t.Fatalf("строка: %+v", rows[0])
	}
	if rows[0].StartedAt != nil {
		t.Fatalf("у работы в очереди время начала: %v", rows[0].StartedAt)
	}
}

// TestTick — событие отмечает срабатывание и показывает его в снимке.
func TestTick(t *testing.T) {
	m := &module{journal: journal.Discard{}}
	job := jobs.Job{ID: "1", Kind: eventKind, Payload: []byte(`{"due":"2026-11-25T12:00:00Z"}`)}

	if err := m.tick(context.Background(), job); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := m.ticks.Load(); got != 1 {
		t.Fatalf("срабатываний %d, ожидалось 1", got)
	}

	view := m.event()
	if view.Kind != string(eventKind) || view.Period != tickPeriod.String() || view.Fired != 1 {
		t.Fatalf("снимок события: %+v", view)
	}
}
