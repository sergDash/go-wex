// Package events — отложенные события: срок, повтор через период и догон
// после простоя. Событие не выполняет работу само: оно ставит её в очередь
// работ, а когда работу взять, решает разборщик.
//
// Одно событие — одна запись в хранилище: срок следующего срабатывания.
// Пропущенные сроки не разворачиваются в очередь: догон даёт одно
// срабатывание и сдвигает срок к первому будущему, сохраняя фазу.
package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"go-wex/engine/clock"
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/store"
)

// Kind — вид события. Он же вид работы, которую событие ставит: обработчик
// объявляется один раз, а срок лишь решает, когда его запускать.
type Kind string

// Rule — правило события. Period — промежуток между сроками, Class — класс
// ставимой работы; пустой класс означает класс вида работы, а если и там
// пусто — фоновый. Payload собирает полезную нагрузку по сроку: обработчику
// обычно нужен момент, за который он работает.
type Rule struct {
	Class   jobs.Class
	Period  time.Duration
	Payload func(due time.Time) any
}

// Registry — виды событий, объявленные приложением. Реестр только хранит
// правила: он не запускает службу и не ставит работы. Объявления приходят
// до запуска служб, поэтому мьютекса нет.
type Registry struct {
	rules map[Kind]Rule
}

// NewRegistry заводит пустой реестр.
func NewRegistry() *Registry {
	return &Registry{rules: map[Kind]Rule{}}
}

// Add объявляет вид события. Период обязателен: событие без периода —
// это не событие, а разовая работа, и её место в очереди.
func (r *Registry) Add(kind Kind, rule Rule) error {
	if kind == "" {
		return errors.New("события: пустой вид")
	}
	if rule.Period <= 0 {
		return fmt.Errorf("события: вид %q: период должен быть больше нуля", kind)
	}
	if rule.Class != "" && !rule.Class.Valid() {
		return fmt.Errorf("события: вид %q: неизвестный класс %q", kind, rule.Class)
	}
	if _, ok := r.rules[kind]; ok {
		return fmt.Errorf("события: вид %q уже объявлен", kind)
	}
	r.rules[kind] = rule
	return nil
}

// Rule отдаёт правило вида.
func (r *Registry) Rule(kind Kind) (Rule, bool) {
	rule, ok := r.rules[kind]
	return rule, ok
}

// Kinds отдаёт виды по возрастанию: обход не зависит от того, как легли
// ключи в карте, и срабатывания за один шаг идут в предсказуемом порядке.
func (r *Registry) Kinds() []Kind {
	kinds := make([]Kind, 0, len(r.rules))
	for kind := range r.rules {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
}

// record — запись события в хранилище. У вида, объявленного кодом, номер
// совпадает с видом и хранится только срок: период, класс и тело приходят
// из правила, иначе правка кода не подействовала бы на старые записи.
// У события, добавленного данными, хранится всё — другого источника у него нет.
type record struct {
	Kind    jobs.Kind       `json:"kind,omitempty"`
	Period  time.Duration   `json:"period,omitempty"`
	Class   jobs.Class      `json:"class,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Next    time.Time       `json:"next"`
}

// collection — коллекция сроков в хранилище.
const collection = "events"

// Enqueuer — постановка работы в очередь. Разборщик подходит целиком: он
// проверяет, что вид объявлен, подставляет класс по умолчанию и будит себя
// после постановки. Known нужен событиям, которые добавляют данными: вид
// работ должен существовать, иначе работа окажется без обработчика.
type Enqueuer interface {
	Enqueue(ctx context.Context, kind jobs.Kind, payload any, class jobs.Class) (string, error)
	Known(kind jobs.Kind) bool
}

// Firing — одно срабатывание: какое событие, какой срок и какая работа.
type Firing struct {
	ID    string
	Kind  Kind
	Due   time.Time
	JobID string
}

// Event — событие для показа снаружи: вид, период, срок и откуда оно взялось.
type Event struct {
	ID       string
	Kind     jobs.Kind
	Period   time.Duration
	Class    jobs.Class
	Next     time.Time
	Declared bool
}

// Scheduler — служба отложенных событий: по тикеру смотрит сроки, ставит
// работы и сдвигает срок. Шаг отделён от цикла, чтобы проверки не спали
// и не ждали тикера.
type Scheduler struct {
	store   *store.Store
	queue   Enqueuer
	reg     *Registry
	clock   clock.Clock
	nudge   chan struct{}
	log     *log.Logger
	journal journal.Writer

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewScheduler собирает службу событий.
func NewScheduler(st *store.Store, q Enqueuer, r *Registry) *Scheduler {
	return &Scheduler{
		store:   st,
		queue:   q,
		reg:     r,
		clock:   clock.Real{},
		nudge:   make(chan struct{}, 1),
		log:     log.Default(),
		journal: journal.Discard{},
	}
}

// SetClock подменяет часы; для проверок догона и границ срока.
func (s *Scheduler) SetClock(c clock.Clock) {
	if c != nil {
		s.clock = c
	}
}

// SetJournal подключает журнал работы: в него пишутся срабатывания и сбои.
func (s *Scheduler) SetJournal(j journal.Writer) {
	s.journal = j
}

// Name — имя службы для группы.
func (s *Scheduler) Name() string { return "events" }

// Start запускает обход сроков в отдельной горутине и сразу возвращается.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("события: служба уже запущена")
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.cancel = cancel
	s.done = done
	s.started = true
	s.mu.Unlock()

	go func() {
		defer close(done)
		if err := s.Run(runCtx); err != nil {
			s.log.Printf("события: %v", err)
		}
	}()
	return nil
}

// Stop отменяет обход и ждёт завершения его горутины.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	started, cancel, done := s.started, s.cancel, s.done
	s.mu.Unlock()
	if !started {
		return nil
	}

	cancel()
	select {
	case <-done:
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Notify будит службу, не дожидаясь тикера: так проверки и внешние вызовы
// не ждут секунду.
func (s *Scheduler) Notify() {
	select {
	case s.nudge <- struct{}{}:
	default:
	}
}

// Run обходит сроки до отмены контекста.
func (s *Scheduler) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		if _, err := s.Step(ctx, s.clock.Now()); err != nil {
			s.log.Printf("события: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.clock.After(time.Second):
		case <-s.nudge:
		}
	}
}

// Step делает один проход: ставит работы событиям, чей срок наступил,
// и сдвигает срок. Ошибка одного события не мешает остальным: она
// возвращается вместе с итогом, чтобы попасть в журнал.
func (s *Scheduler) Step(ctx context.Context, now time.Time) ([]Firing, error) {
	records, err := s.records()
	if err != nil {
		return nil, err
	}

	var fired []Firing
	var errs []error

	// Виды, объявленные кодом: период, класс и тело берутся из правила,
	// в хранилище лежит только срок.
	for _, kind := range s.reg.Kinds() {
		rule, ok := s.reg.Rule(kind)
		if !ok {
			continue
		}

		rec, known := records[string(kind)]
		if !known || rec.Next.IsZero() {
			// Вид объявлен впервые: срок отсчитывается от текущего момента,
			// иначе свежий проект сработал бы сразу на старте.
			if err := s.setNext(kind, now.Add(rule.Period)); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if rec.Next.After(now) {
			continue
		}

		var payload any
		if rule.Payload != nil {
			payload = rule.Payload(rec.Next)
		}

		due := rec.Next
		job, err := s.fire(ctx, string(kind), jobs.Kind(kind), payload, rule.Class, due)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := s.setNext(kind, advance(due, rule.Period, now)); err != nil {
			errs = append(errs, err)
		}

		fired = append(fired, Firing{ID: string(kind), Kind: kind, Due: due, JobID: job})
	}

	// События, добавленные данными: вид, период и тело лежат в записи.
	// Ключ, совпавший с видом из кода, сюда не попадает — его разобрал
	// проход выше.
	for _, key := range sortedKeys(records) {
		if _, declared := s.reg.Rule(Kind(key)); declared {
			continue
		}
		rec := records[key]
		if rec.Kind == "" || rec.Period <= 0 {
			// Запись без вида — остаток прежней формы; пропускаем.
			continue
		}
		if rec.Next.After(now) {
			continue
		}

		job, err := s.fire(ctx, key, rec.Kind, rec.Payload, rec.Class, rec.Next)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		next := advance(rec.Next, rec.Period, now)
		if err := s.advanceRecord(key, rec, next); err != nil {
			errs = append(errs, err)
		}

		fired = append(fired, Firing{ID: key, Kind: Kind(rec.Kind), Due: rec.Next, JobID: job})
	}

	return fired, errors.Join(errs...)
}

// fire ставит работу и пишет срабатывание в журнал. Порядок важен: сначала
// работа, затем срок. Обратный порядок при сбое между записями дал бы тихую
// потерю — срок сдвинут, работы нет. Цена прямого порядка записана
// в doc/capabilities/Недостатки.md: падение в этом окне даёт повторное
// срабатывание, но не потерю.
func (s *Scheduler) fire(ctx context.Context, id string, kind jobs.Kind, payload any, class jobs.Class, due time.Time) (string, error) {
	job, err := s.queue.Enqueue(ctx, kind, payload, class)
	if err != nil {
		// Срок не двигаем: работу поставить не удалось, попытка повторится
		// на следующем проходе с тем же сроком.
		return "", fmt.Errorf("события: %q: постановка: %w", id, err)
	}

	s.writeJournal("fired", map[string]any{
		"event": id,
		"kind":  kind,
		"due":   due,
		"job":   job,
	})
	return job, nil
}

// ErrDeclared — событие объявлено кодом: следующий проход создаст его заново,
// поэтому удалять нечего.
var ErrDeclared = errors.New("событие объявлено кодом")

// ErrNotFound — события с таким номером нет.
var ErrNotFound = errors.New("событие не найдено")

// Delete убирает событие, добавленное данными. Работы, которые оно уже
// поставило, остаются в очереди: событие и работа живут отдельно, связи
// между ними нет. Событие из кода не удаляется — оно вернётся на следующем
// проходе, и отказ честнее молчаливого возврата.
func (s *Scheduler) Delete(id string) error {
	if _, declared := s.reg.Rule(Kind(id)); declared {
		return fmt.Errorf("%w: %q", ErrDeclared, id)
	}

	var rec record
	ok, err := s.store.Get(collection, id, &rec)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return s.store.Delete(collection, id)
}

// advance — срок после срабатывания: первый из сроков, который ещё впереди.
// Деление вместо цикла: срок мог отстать на годы, и цикл по короткому периоду
// висел бы на каждом проходе.
func advance(due time.Time, period time.Duration, now time.Time) time.Time {
	steps := now.Sub(due) / period
	return due.Add((steps + 1) * period)
}

// records читает все записи событий: и объявленные кодом, и добавленные
// данными. Ключ — номер события; у вида из кода номер совпадает с видом.
func (s *Scheduler) records() (map[string]record, error) {
	keys := s.store.Keys(collection)
	out := make(map[string]record, len(keys))
	for _, key := range keys {
		var rec record
		ok, err := s.store.Get(collection, key, &rec)
		if err != nil {
			return nil, err
		}
		if ok {
			out[key] = rec
		}
	}
	return out, nil
}

// setNext записывает срок вида, объявленного кодом: остальные поля записи
// приходят из правила, поэтому хранить их незачем.
func (s *Scheduler) setNext(kind Kind, at time.Time) error {
	return s.store.Put(collection, string(kind), record{Next: at})
}

// advanceRecord сдвигает срок события, добавленного данными, сохраняя его
// вид, период, класс и тело.
func (s *Scheduler) advanceRecord(key string, rec record, next time.Time) error {
	rec.Next = next
	return s.store.Put(collection, key, rec)
}

// sortedKeys — ключи записей по возрастанию: обход не зависит от порядка
// карты, и срабатывания за один проход идут предсказуемо.
func sortedKeys(records map[string]record) []string {
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Add объявляет событие во время работы: вид работ, период, класс и тело.
// Запись уходит в хранилище, поэтому событие переживает перезапуск; срок
// отсчитывается от текущего момента, и только что добавленное событие
// не срабатывает сразу.
func (s *Scheduler) Add(kind jobs.Kind, period time.Duration, class jobs.Class, payload any) (string, error) {
	if period <= 0 {
		return "", errors.New("события: период должен быть больше нуля")
	}
	if class != "" && !class.Valid() {
		return "", fmt.Errorf("события: неизвестный класс %q", class)
	}
	if !s.queue.Known(kind) {
		return "", fmt.Errorf("события: вид работ %q не объявлен", kind)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("события: тело: %w", err)
	}

	id, err := s.newID()
	if err != nil {
		return "", err
	}

	rec := record{
		Kind:    kind,
		Period:  period,
		Class:   class,
		Payload: raw,
		Next:    s.clock.Now().Add(period),
	}
	if err := s.store.Put(collection, id, rec); err != nil {
		return "", err
	}
	return id, nil
}

// newID — номер события: восемь шестнадцатеричных знаков. Счётчик не нужен:
// события добавляют редко, а совпадение проверяется по хранилищу.
func (s *Scheduler) newID() (string, error) {
	for i := 0; i < 5; i++ {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", fmt.Errorf("события: номер: %w", err)
		}
		id := hex.EncodeToString(b[:])

		var rec record
		ok, err := s.store.Get(collection, id, &rec)
		if err != nil {
			return "", err
		}
		if !ok {
			return id, nil
		}
	}
	return "", errors.New("события: не удалось подобрать номер")
}

// List отдаёт события в порядке срока: сначала те, что сработают раньше.
func (s *Scheduler) List() ([]Event, error) {
	records, err := s.records()
	if err != nil {
		return nil, err
	}

	out := make([]Event, 0, len(records)+len(s.reg.Kinds()))
	for _, kind := range s.reg.Kinds() {
		rule, ok := s.reg.Rule(kind)
		if !ok {
			continue
		}
		ev := Event{ID: string(kind), Kind: jobs.Kind(kind), Period: rule.Period, Class: rule.Class, Declared: true}
		if rec, known := records[string(kind)]; known {
			ev.Next = rec.Next
		}
		out = append(out, ev)
	}
	for key, rec := range records {
		if _, declared := s.reg.Rule(Kind(key)); declared {
			continue
		}
		if rec.Kind == "" || rec.Period <= 0 {
			continue
		}
		out = append(out, Event{ID: key, Kind: rec.Kind, Period: rec.Period, Class: rec.Class, Next: rec.Next})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Next.Before(out[j].Next) })
	return out, nil
}

// writeJournal пишет событие и не мешает обходу, если журнал недоступен.
func (s *Scheduler) writeJournal(kind string, fields map[string]any) {
	if s.journal == nil {
		return
	}
	if err := s.journal.Write("events", kind, fields); err != nil {
		s.log.Printf("журнал: %v", err)
	}
}
