package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go-wex/engine/clock"
	"go-wex/engine/jobs"
	"go-wex/engine/store"
)

// base — начало отсчёта в проверках: фиксированный момент, чтобы сроки
// сравнивались точно, без допуска на дрожание системных часов.
var base = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

// recorded — одна постановка, увиденная очередью-двойником.
type recorded struct {
	kind    jobs.Kind
	payload any
	class   jobs.Class
}

// recorder — очередь-двойник: запоминает постановки и умеет отказывать,
// чтобы проверить сбой постановки без настоящей очереди.
type recorder struct {
	mu    sync.Mutex
	calls []recorded
	fail  error
	known map[jobs.Kind]bool
}

func (r *recorder) Enqueue(_ context.Context, kind jobs.Kind, payload any, class jobs.Class) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fail != nil {
		return "", r.fail
	}
	r.calls = append(r.calls, recorded{kind: kind, payload: payload, class: class})
	return fmt.Sprintf("job-%d", len(r.calls)), nil
}

// Known по умолчанию считает известными все виды: проверки, которым вид
// работ безразличен, не должны перечислять его отдельно.
func (r *recorder) Known(kind jobs.Kind) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.known == nil {
		return true
	}
	return r.known[kind]
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recorder) call(i int) recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[i]
}

// openStore открывает хранилище во временном каталоге проверки.
func openStore(t *testing.T, path string) *store.Store {
	t.Helper()

	st, err := store.Open(path, nil)
	if err != nil {
		t.Fatalf("хранилище: %v", err)
	}
	return st
}

// nextAt читает сохранённый срок события.
func nextAt(t *testing.T, st *store.Store, kind Kind) time.Time {
	t.Helper()

	var s record
	ok, err := st.Get(collection, string(kind), &s)
	if err != nil {
		t.Fatalf("чтение срока %q: %v", kind, err)
	}
	if !ok {
		t.Fatalf("срок события %q не записан", kind)
	}
	return s.Next
}

// newScheduler собирает службу на хранилище во временном каталоге.
func newScheduler(t *testing.T, reg *Registry, q Enqueuer) (*Scheduler, *store.Store) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "store.json")
	st := openStore(t, path)
	return NewScheduler(st, q, reg), st
}

// TestFirstStepSchedulesFromNow — новый вид получает первый срок через
// период от текущего момента, а не срабатывает сразу при запуске.
func TestFirstStepSchedulesFromNow(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("tick", Rule{Period: time.Hour}); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	rec := &recorder{}
	s, st := newScheduler(t, reg, rec)

	fired, err := s.Step(context.Background(), base)
	if err != nil {
		t.Fatalf("шаг: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("на первом шаге срабатываний %d, ожидалось 0", len(fired))
	}
	if got, want := nextAt(t, st, "tick"), base.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("срок %s, ожидался %s", got, want)
	}
}

// TestFiresAtDeadlineAndOnlyOnce — срабатывание приходит ровно в срок,
// а повторный шаг в тот же момент второй работы не ставит.
func TestFiresAtDeadlineAndOnlyOnce(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("tick", Rule{Period: time.Hour}); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	rec := &recorder{}
	s, st := newScheduler(t, reg, rec)
	ctx := context.Background()

	if _, err := s.Step(ctx, base); err != nil {
		t.Fatalf("первый шаг: %v", err)
	}

	// За минуту до срока работа не ставится: срок ещё не наступил.
	fired, err := s.Step(ctx, base.Add(time.Hour-time.Minute))
	if err != nil {
		t.Fatalf("шаг до срока: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("до срока срабатываний %d, ожидалось 0", len(fired))
	}

	fired, err = s.Step(ctx, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("шаг в срок: %v", err)
	}
	if len(fired) != 1 {
		t.Fatalf("в срок срабатываний %d, ожидалось 1", len(fired))
	}
	if got, want := fired[0].Due, base.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("сработал срок %s, ожидался %s", got, want)
	}
	if got, want := fired[0].Kind, Kind("tick"); got != want {
		t.Fatalf("сработал вид %q, ожидался %q", got, want)
	}

	// Повторный шаг в тот же момент и в следующий: срок уже впереди.
	fired, err = s.Step(ctx, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("повторный шаг: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("повторный шаг дал срабатываний %d, ожидалось 0", len(fired))
	}
	if got, want := nextAt(t, st, "tick"), base.Add(2*time.Hour); !got.Equal(want) {
		t.Fatalf("следующий срок %s, ожидался %s", got, want)
	}
}

// TestCatchUpFiresOnce — после простоя событие срабатывает один раз,
// а срок прыгает к первому будущему с сохранением фазы.
func TestCatchUpFiresOnce(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("daily", Rule{Period: 24 * time.Hour}); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	rec := &recorder{}
	s, st := newScheduler(t, reg, rec)
	ctx := context.Background()

	if _, err := s.Step(ctx, base); err != nil {
		t.Fatalf("первый шаг: %v", err)
	}

	// Обращение через трое суток вместо одних: срок был base + 24 ч.
	fired, err := s.Step(ctx, base.Add(72*time.Hour))
	if err != nil {
		t.Fatalf("догон: %v", err)
	}
	if len(fired) != 1 {
		t.Fatalf("догон дал срабатываний %d, ожидалось 1", len(fired))
	}
	if got, want := fired[0].Due, base.Add(24*time.Hour); !got.Equal(want) {
		t.Fatalf("сработал срок %s, ожидался %s", got, want)
	}
	// Фаза сохранена: следующий срок кратен суткам от начала, а не от догона.
	if got, want := nextAt(t, st, "daily"), base.Add(96*time.Hour); !got.Equal(want) {
		t.Fatalf("следующий срок %s, ожидался %s", got, want)
	}

	fired, err = s.Step(ctx, base.Add(72*time.Hour))
	if err != nil {
		t.Fatalf("повторный догон: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("повторный догон дал срабатываний %d, ожидалось 0", len(fired))
	}
}

// TestFailedEnqueueKeepsDeadline — если работу поставить не удалось, срок
// остаётся прежним, и следующий шаг пробует снова с тем же сроком.
func TestFailedEnqueueKeepsDeadline(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("tick", Rule{Period: time.Hour}); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	rec := &recorder{}
	s, st := newScheduler(t, reg, rec)
	ctx := context.Background()

	if _, err := s.Step(ctx, base); err != nil {
		t.Fatalf("первый шаг: %v", err)
	}

	rec.fail = errors.New("очередь недоступна")
	fired, err := s.Step(ctx, base.Add(2*time.Hour))
	if err == nil {
		t.Fatal("сбой постановки не вернулся ошибкой")
	}
	if len(fired) != 0 {
		t.Fatalf("при сбое срабатываний %d, ожидалось 0", len(fired))
	}
	if got, want := nextAt(t, st, "tick"), base.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("срок сдвинулся на %s, ожидался прежний %s", got, want)
	}

	rec.fail = nil
	fired, err = s.Step(ctx, base.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("шаг после отказа: %v", err)
	}
	if len(fired) != 1 {
		t.Fatalf("после отказа срабатываний %d, ожидалось 1", len(fired))
	}
	// Сработал именно пропущенный срок: работа не потерялась.
	if got, want := fired[0].Due, base.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("сработал срок %s, ожидался %s", got, want)
	}
}

// TestPayloadAndClassReachQueue — полезная нагрузка собирается по сроку,
// а класс события доходит до очереди без подмены.
func TestPayloadAndClassReachQueue(t *testing.T) {
	reg := NewRegistry()
	rule := Rule{
		Class:  jobs.ClassInteractive,
		Period: time.Hour,
		Payload: func(due time.Time) any {
			return map[string]string{"due": due.Format(time.RFC3339)}
		},
	}
	if err := reg.Add("report", rule); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	rec := &recorder{}
	s, _ := newScheduler(t, reg, rec)
	ctx := context.Background()

	if _, err := s.Step(ctx, base); err != nil {
		t.Fatalf("первый шаг: %v", err)
	}
	if _, err := s.Step(ctx, base.Add(time.Hour)); err != nil {
		t.Fatalf("шаг в срок: %v", err)
	}

	call := rec.call(0)
	if got, want := call.kind, jobs.Kind("report"); got != want {
		t.Fatalf("поставлен вид %q, ожидался %q", got, want)
	}
	if got, want := call.class, jobs.ClassInteractive; got != want {
		t.Fatalf("поставлен класс %q, ожидался %q", got, want)
	}
	payload, ok := call.payload.(map[string]string)
	if !ok {
		t.Fatalf("нагрузка %T, ожидалась map[string]string", call.payload)
	}
	if got, want := payload["due"], base.Add(time.Hour).Format(time.RFC3339); got != want {
		t.Fatalf("нагрузка описывает срок %q, ожидался %q", got, want)
	}
}

// TestDeadlineSurvivesRestart — срок лежит в хранилище, поэтому новая служба
// на том же файле видит прежний срок, а не начинает отсчёт заново.
func TestDeadlineSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	reg := NewRegistry()
	if err := reg.Add("tick", Rule{Period: time.Hour}); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	rec := &recorder{}
	ctx := context.Background()

	first := NewScheduler(openStore(t, path), rec, reg)
	if _, err := first.Step(ctx, base); err != nil {
		t.Fatalf("первый шаг: %v", err)
	}

	second := NewScheduler(openStore(t, path), rec, reg)
	fired, err := second.Step(ctx, base.Add(time.Hour))
	if err != nil {
		t.Fatalf("шаг после перезапуска: %v", err)
	}
	if len(fired) != 1 {
		t.Fatalf("после перезапуска срабатываний %d, ожидалось 1", len(fired))
	}
	if got, want := fired[0].Due, base.Add(time.Hour); !got.Equal(want) {
		t.Fatalf("сработал срок %s, ожидался %s", got, want)
	}
}

// TestRegistryRejects — реестр не принимает вид без периода, с неизвестным
// классом и повторное объявление того же вида.
func TestRegistryRejects(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("tick", Rule{Period: time.Hour}); err != nil {
		t.Fatalf("первое объявление: %v", err)
	}

	cases := []struct {
		name string
		kind Kind
		rule Rule
	}{
		{name: "пустой вид", kind: "", rule: Rule{Period: time.Hour}},
		{name: "нулевой период", kind: "zero", rule: Rule{}},
		{name: "неизвестный класс", kind: "class", rule: Rule{Period: time.Hour, Class: "fast"}},
		{name: "повтор вида", kind: "tick", rule: Rule{Period: time.Hour}},
	}
	for _, c := range cases {
		if err := reg.Add(c.kind, c.rule); err == nil {
			t.Fatalf("%s: объявление принято, ожидался отказ", c.name)
		}
	}
}

// TestAddFiresAndPersists — событие, добавленное данными, ставит работу по
// сроку, а его запись переживает перезапуск: срок и тело лежат в хранилище.
func TestAddFiresAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	reg := NewRegistry()
	if err := reg.Add("tick", Rule{Period: time.Hour}); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	rec := &recorder{}
	s := NewScheduler(openStore(t, path), rec, reg)
	s.SetClock(clock.NewManual(base))

	id, err := s.Add("beat", 10*time.Second, jobs.ClassBackground, map[string]string{"text": "раз"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if len(id) != 8 {
		t.Fatalf("номер события %q, ожидались восемь знаков", id)
	}

	// До срока тишина: только что добавленное событие не срабатывает сразу.
	fired, err := s.Step(context.Background(), base)
	if err != nil {
		t.Fatalf("шаг до срока: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("до срока срабатываний %d, ожидалось 0", len(fired))
	}

	fired, err = s.Step(context.Background(), base.Add(10*time.Second))
	if err != nil {
		t.Fatalf("шаг в срок: %v", err)
	}
	if len(fired) != 1 || fired[0].ID != id || fired[0].Kind != Kind("beat") {
		t.Fatalf("срабатывание: %+v", fired)
	}
	call := rec.call(0)
	if call.kind != "beat" {
		t.Fatalf("поставлен вид %q", call.kind)
	}
	if len(call.payload.(json.RawMessage)) == 0 || string(call.payload.(json.RawMessage)) != `{"text":"раз"}` {
		t.Fatalf("тело: %s", call.payload)
	}

	// Перезапуск: запись осталась, срок продолжается, а не начинается заново.
	s2 := NewScheduler(openStore(t, path), rec, reg)
	fired, err = s2.Step(context.Background(), base.Add(20*time.Second))
	if err != nil {
		t.Fatalf("шаг после перезапуска: %v", err)
	}
	if len(fired) != 1 || fired[0].ID != id {
		t.Fatalf("после перезапуска: %+v", fired)
	}
}

// TestAddValidation — добавление проверяет период, класс и вид работ:
// событие без обработчика работу бы не нашло.
func TestAddValidation(t *testing.T) {
	reg := NewRegistry()
	rec := &recorder{known: map[jobs.Kind]bool{}}
	s, _ := newScheduler(t, reg, rec)

	if _, err := s.Add("beat", 0, jobs.ClassBackground, nil); err == nil {
		t.Fatal("нулевой период принят")
	}
	if _, err := s.Add("beat", time.Second, "fast", nil); err == nil {
		t.Fatal("неизвестный класс принят")
	}
	if _, err := s.Add("beat", time.Second, jobs.ClassBackground, nil); err == nil {
		t.Fatal("необъявленный вид работ принят")
	}
}

// TestList — список отдаёт и виды из кода, и добавленные данными, в порядке
// срока, и помечает, откуда взялось событие.
func TestList(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("tick", Rule{Period: time.Hour}); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	rec := &recorder{}
	s, _ := newScheduler(t, reg, rec)
	s.SetClock(clock.NewManual(base))

	if _, err := s.Step(context.Background(), base); err != nil {
		t.Fatalf("первый шаг: %v", err)
	}
	if _, err := s.Add("beat", 10*time.Second, jobs.ClassBackground, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}

	list, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("событий %d, ожидалось 2", len(list))
	}
	if list[0].Kind != "beat" || list[0].Declared {
		t.Fatalf("первое событие: %+v", list[0])
	}
	if list[1].Kind != "tick" || !list[1].Declared || list[1].Period != time.Hour {
		t.Fatalf("второе событие: %+v", list[1])
	}
}

// TestServiceStops — служба запускается и останавливается, не оставляя
// висящей горутины.
func TestServiceStops(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("tick", Rule{Period: time.Hour}); err != nil {
		t.Fatalf("объявление вида: %v", err)
	}
	s, _ := newScheduler(t, reg, &recorder{})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("запуск службы: %v", err)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("остановка службы: %v", err)
	}
}
