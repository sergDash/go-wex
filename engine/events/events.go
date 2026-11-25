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

// state — запись события в хранилище: только срок следующего срабатывания.
// Пересчитывать срок от времени запуска нельзя: фаза расписания потерялась бы
// при каждом перезапуске.
type state struct {
	Next time.Time `json:"next"`
}

// collection — коллекция сроков в хранилище.
const collection = "events"

// Enqueuer — постановка работы в очередь. Разборщик подходит целиком: он
// проверяет, что вид объявлен, подставляет класс по умолчанию и будит себя
// после постановки.
type Enqueuer interface {
	Enqueue(ctx context.Context, kind jobs.Kind, payload any, class jobs.Class) (string, error)
}

// Firing — одно срабатывание: какой срок отработали и какая работа поставлена.
type Firing struct {
	Kind  Kind
	Due   time.Time
	JobID string
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

// Step делает один проход по видам: ставит работы тем, чей срок наступил,
// и сдвигает срок. Ошибка одного вида не мешает остальным: она возвращается
// вместе с итогом, чтобы попасть в журнал.
func (s *Scheduler) Step(ctx context.Context, now time.Time) ([]Firing, error) {
	var fired []Firing
	var errs []error

	for _, kind := range s.reg.Kinds() {
		rule, ok := s.reg.Rule(kind)
		if !ok {
			continue
		}

		due, known, err := s.next(kind)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !known {
			// Вид объявлен впервые: срок отсчитывается от текущего момента,
			// иначе свежий проект сработал бы сразу на старте.
			if err := s.setNext(kind, now.Add(rule.Period)); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		if due.After(now) {
			continue
		}

		var payload any
		if rule.Payload != nil {
			payload = rule.Payload(due)
		}

		// Порядок важен: сначала работа, затем срок. Обратный порядок при
		// сбое между записями дал бы тихую потерю — срок сдвинут, работы нет.
		// Цена прямого порядка записана в doc/capabilities/Недостатки.md:
		// падение в этом окне даёт повторное срабатывание, но не потерю.
		id, err := s.queue.Enqueue(ctx, jobs.Kind(kind), payload, rule.Class)
		if err != nil {
			// Срок не двигаем: работу поставить не удалось, попытка повторится
			// на следующем проходе с тем же сроком.
			errs = append(errs, fmt.Errorf("события: вид %q: постановка: %w", kind, err))
			continue
		}

		if err := s.setNext(kind, advance(due, rule.Period, now)); err != nil {
			errs = append(errs, err)
		}

		s.writeJournal("fired", map[string]any{
			"kind": kind,
			"due":  due,
			"job":  id,
		})
		fired = append(fired, Firing{Kind: kind, Due: due, JobID: id})
	}

	return fired, errors.Join(errs...)
}

// advance — срок после срабатывания: первый из сроков, который ещё впереди.
// Деление вместо цикла: срок мог отстать на годы, и цикл по короткому периоду
// висел бы на каждом проходе.
func advance(due time.Time, period time.Duration, now time.Time) time.Time {
	steps := now.Sub(due) / period
	return due.Add((steps + 1) * period)
}

// next читает срок события. Отсутствие записи и нулевой срок считаются
// одинаково: срок ещё не назначался.
func (s *Scheduler) next(kind Kind) (time.Time, bool, error) {
	var st state
	ok, err := s.store.Get(collection, string(kind), &st)
	if err != nil {
		return time.Time{}, false, err
	}
	if !ok || st.Next.IsZero() {
		return time.Time{}, false, nil
	}
	return st.Next, true, nil
}

// setNext записывает срок следующего срабатывания.
func (s *Scheduler) setNext(kind Kind, at time.Time) error {
	return s.store.Put(collection, string(kind), state{Next: at})
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
