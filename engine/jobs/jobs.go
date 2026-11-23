// Package jobs — очередь работ и разборщик. Движок стоит на них: фоновая
// работа не теряется между запросами, повторяется после ошибки и видна
// снаружи через статусы.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Kind — вид работы; по нему выбирается обработчик.
type Kind string

// Job — работа в очереди. Attempts считается при захвате, поэтому обработчик
// видит номер текущей попытки.
type Job struct {
	ID          string
	Kind        Kind
	Payload     json.RawMessage
	Attempts    int
	AvailableAt time.Time
}

// Handler — обработчик работы. Ошибка означает, что работу нужно повторить
// или, если попытки кончились, отметить отказом.
type Handler func(context.Context, Job) error

// Queue — хранилище работ. Реализация в памяти годится для образца; долговечная
// очередь реализует тот же набор методов поверх хранилища.
type Queue interface {
	Enqueue(ctx context.Context, kind Kind, payload any) (string, error)
	Claim(ctx context.Context, now time.Time) (Job, bool, error)
	Done(ctx context.Context, id string) error
	Retry(ctx context.Context, id string, at time.Time) error
	Fail(ctx context.Context, id string) error
}

// Spec — правило вида работы.
type Spec struct {
	Handler     Handler
	MaxAttempts int
	Backoff     time.Duration
}

// Registry — виды работ, известные приложению.
type Registry struct {
	specs map[Kind]Spec
}

// NewRegistry заводит пустой реестр видов.
func NewRegistry() *Registry {
	return &Registry{specs: map[Kind]Spec{}}
}

// Add объявляет вид работы. Повторная регистрация того же вида — ошибка:
// два обработчика для одного вида делают поведение неявным.
func (r *Registry) Add(k Kind, s Spec) error {
	if _, ok := r.specs[k]; ok {
		return fmt.Errorf("jobs: вид %q уже объявлен", k)
	}
	r.specs[k] = s
	return nil
}

// Spec отдаёт правило вида.
func (r *Registry) Spec(k Kind) (Spec, bool) {
	s, ok := r.specs[k]
	return s, ok
}

// Статусы работы в очереди.
const (
	statusQueued  = "queued"
	statusRunning = "running"
	statusDone    = "done"
	statusFailed  = "failed"
)

// item — строка очереди в памяти: работа и её статус.
type item struct {
	job    Job
	status string
}

// MemoryQueue — очередь в памяти. Переживает перезапуск только вместе
// с процессом; долговечная очередь появится вместе с хранилищем.
type MemoryQueue struct {
	mu    sync.Mutex
	seq   int
	items map[string]*item
}

// NewMemoryQueue заводит пустую очередь.
func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{items: map[string]*item{}}
}

// Enqueue кладёт работу со статусом queued и возвращает её номер.
func (q *MemoryQueue) Enqueue(_ context.Context, kind Kind, payload any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("jobs: тело работы %q: %w", kind, err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq++
	id := fmt.Sprintf("%06d", q.seq)
	q.items[id] = &item{job: Job{ID: id, Kind: kind, Payload: raw}, status: statusQueued}
	return id, nil
}

// Claim забирает одну работу, доступную к моменту now: раньше всех по сроку,
// при равенстве — по номеру. Работа становится running, попытка растёт.
func (q *MemoryQueue) Claim(_ context.Context, now time.Time) (Job, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var chosen *item
	for _, it := range q.items {
		if it.status != statusQueued || it.job.AvailableAt.After(now) {
			continue
		}
		if chosen == nil || it.job.AvailableAt.Before(chosen.job.AvailableAt) ||
			(it.job.AvailableAt.Equal(chosen.job.AvailableAt) && it.job.ID < chosen.job.ID) {
			chosen = it
		}
	}
	if chosen == nil {
		return Job{}, false, nil
	}

	chosen.status = statusRunning
	chosen.job.Attempts++
	return chosen.job, true, nil
}

// Done отмечает работу выполненной.
func (q *MemoryQueue) Done(_ context.Context, id string) error {
	return q.setStatus(id, statusDone, time.Time{})
}

// Retry возвращает работу в очередь и назначает срок следующей попытки.
func (q *MemoryQueue) Retry(_ context.Context, id string, at time.Time) error {
	return q.setStatus(id, statusQueued, at)
}

// Fail отмечает работу отказом: попытки кончились.
func (q *MemoryQueue) Fail(_ context.Context, id string) error {
	return q.setStatus(id, statusFailed, time.Time{})
}

// setStatus меняет статус работы; отсутствие работы — ошибка.
func (q *MemoryQueue) setStatus(id, status string, at time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.items[id]
	if !ok {
		return fmt.Errorf("jobs: работа %q не найдена", id)
	}
	it.status = status
	it.job.AvailableAt = at
	return nil
}

// Dispatcher — разборщик работ: забирает по одной, вызывает обработчик и
// переводит статус. Шаг отделён от цикла, чтобы тесты не спали.
type Dispatcher struct {
	queue Queue
	reg   *Registry
	now   func() time.Time
	nudge chan struct{}
	log   *log.Logger
}

// NewDispatcher собирает разборщик.
func NewDispatcher(q Queue, r *Registry) *Dispatcher {
	return &Dispatcher{
		queue: q,
		reg:   r,
		now:   time.Now,
		nudge: make(chan struct{}, 1),
		log:   log.Default(),
	}
}

// SetNow подменяет часы; для проверок границ повтора.
func (d *Dispatcher) SetNow(fn func() time.Time) {
	if fn != nil {
		d.now = fn
	}
}

// Enqueue ставит работу и будит разборщик.
func (d *Dispatcher) Enqueue(ctx context.Context, kind Kind, payload any) (string, error) {
	id, err := d.queue.Enqueue(ctx, kind, payload)
	if err != nil {
		return "", err
	}
	d.Notify()
	return id, nil
}

// Notify будит разборщик, не дожидаясь тикера.
func (d *Dispatcher) Notify() {
	select {
	case d.nudge <- struct{}{}:
	default:
	}
}

// Step делает один шаг: забирает работу, выполняет, обновляет статус.
// Возвращает true, если работа была взята. Ошибка обработчика возвращается
// для журнала, но статус уже обновлён.
func (d *Dispatcher) Step(ctx context.Context) (bool, error) {
	job, ok, err := d.queue.Claim(ctx, d.now())
	if err != nil || !ok {
		return false, err
	}

	spec, ok := d.reg.Spec(job.Kind)
	if !ok {
		_ = d.queue.Fail(ctx, job.ID)
		return true, fmt.Errorf("jobs: нет обработчика вида %q", job.Kind)
	}

	if err := d.run(ctx, spec.Handler, job); err != nil {
		if job.Attempts >= attempts(spec) {
			return true, errors.Join(err, d.queue.Fail(ctx, job.ID))
		}
		return true, errors.Join(err, d.queue.Retry(ctx, job.ID, d.now().Add(backoff(spec))))
	}
	return true, d.queue.Done(ctx, job.ID)
}

// Run разбирает очередь до отмены контекста: сначала всё доступное, затем
// ждёт тикер или подсказку.
func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return nil
		}
		for {
			handled, err := d.Step(ctx)
			if err != nil {
				d.log.Printf("работа: %v", err)
			}
			if !handled {
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-d.nudge:
		}
	}
}

// run выполняет обработчик и превращает панику в ошибку: одна работа
// не должна ронять разборщик.
func (d *Dispatcher) run(ctx context.Context, h Handler, job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jobs: паника в работе %q: %v", job.Kind, r)
		}
	}()
	return h(ctx, job)
}

// attempts — предел попыток; ноль означает одну попытку.
func attempts(s Spec) int {
	if s.MaxAttempts <= 0 {
		return 1
	}
	return s.MaxAttempts
}

// backoff — задержка перед повтором; ноль означает одну секунду, чтобы
// разборщик не крутился на одной и той же работе.
func backoff(s Spec) time.Duration {
	if s.Backoff <= 0 {
		return time.Second
	}
	return s.Backoff
}
