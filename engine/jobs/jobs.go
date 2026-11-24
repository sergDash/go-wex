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
	"strconv"
	"sync"
	"time"

	"go-wex/engine/store"
)

// Kind — вид работы; по нему выбирается обработчик.
type Kind string

// Job — работа в очереди. Attempts считается при захвате, поэтому обработчик
// видит номер текущей попытки. Теги JSON — форма записи в хранилище.
type Job struct {
	ID          string          `json:"id"`
	Kind        Kind            `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	Attempts    int             `json:"attempts"`
	AvailableAt time.Time       `json:"availableAt"`
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
	Get(ctx context.Context, id string) (Job, string, bool, error)
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
	StatusQueued  = "queued"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
)

// item — строка очереди: работа и её статус. Поля экспортируются, чтобы
// состояние очереди сохранялось в хранилище как JSON.
type item struct {
	Job    Job    `json:"job"`
	Status string `json:"status"`
}

// state — то, что лежит в хранилище: счётчик номеров и строки очереди.
type state struct {
	Seq   int              `json:"seq"`
	Items map[string]*item `json:"items"`
}

// queue — очередь работ. Без save она живёт в памяти процесса; с save
// каждое изменение записывается в хранилище и переживает перезапуск.
type queue struct {
	mu    sync.Mutex
	seq   int
	items map[string]*item
	save  func(state) error
}

// Имена в хранилище: одна коллекция и одна запись на всю очередь, чтобы
// состояние менялось одной записью файла.
const (
	storeCollection = "jobs"
	storeKey        = "queue"
)

// NewMemoryQueue заводит очередь в памяти.
func NewMemoryQueue() Queue {
	return &queue{items: map[string]*item{}}
}

// OpenStoreQueue заводит очередь, которая сохраняет состояние в хранилище.
// Работа, прерванная падением в статусе running, возвращается в очередь:
// попытка уже посчитана, но результат неизвестен.
func OpenStoreQueue(s *store.Store) (Queue, error) {
	q := &queue{
		items: map[string]*item{},
		save: func(st state) error {
			return s.Put(storeCollection, storeKey, st)
		},
	}

	var st state
	ok, err := s.Get(storeCollection, storeKey, &st)
	if err != nil {
		return nil, err
	}
	if !ok {
		return q, nil
	}
	if st.Items != nil {
		q.items = st.Items
	}
	q.seq = maxSeq(st.Seq, q.items)

	recovered := false
	for _, it := range q.items {
		if it.Status == StatusRunning {
			it.Status = StatusQueued
			it.Job.AvailableAt = time.Time{}
			recovered = true
		}
	}
	if recovered {
		if err := q.persist(); err != nil {
			return nil, err
		}
	}
	return q, nil
}

// Enqueue кладёт работу со статусом queued и возвращает её номер.
func (q *queue) Enqueue(_ context.Context, kind Kind, payload any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("jobs: тело работы %q: %w", kind, err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq++
	id := fmt.Sprintf("%06d", q.seq)
	q.items[id] = &item{Job: Job{ID: id, Kind: kind, Payload: raw}, Status: StatusQueued}
	if err := q.persist(); err != nil {
		delete(q.items, id)
		q.seq--
		return "", err
	}
	return id, nil
}

// Claim забирает одну работу, доступную к моменту now: раньше всех по сроку,
// при равенстве — по номеру. Работа становится running, попытка растёт.
func (q *queue) Claim(_ context.Context, now time.Time) (Job, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var chosen *item
	for _, it := range q.items {
		if it.Status != StatusQueued || it.Job.AvailableAt.After(now) {
			continue
		}
		if chosen == nil || it.Job.AvailableAt.Before(chosen.Job.AvailableAt) ||
			(it.Job.AvailableAt.Equal(chosen.Job.AvailableAt) && it.Job.ID < chosen.Job.ID) {
			chosen = it
		}
	}
	if chosen == nil {
		return Job{}, false, nil
	}

	chosen.Status = StatusRunning
	chosen.Job.Attempts++
	if err := q.persist(); err != nil {
		chosen.Job.Attempts--
		chosen.Status = StatusQueued
		return Job{}, false, err
	}
	return chosen.Job, true, nil
}

// Get отдаёт работу и её статус. ok равно false, если работы нет.
func (q *queue) Get(_ context.Context, id string) (Job, string, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	it, ok := q.items[id]
	if !ok {
		return Job{}, "", false, nil
	}
	return it.Job, it.Status, true, nil
}

// Done отмечает работу выполненной.
func (q *queue) Done(_ context.Context, id string) error {
	return q.setStatus(id, StatusDone, time.Time{})
}

// Retry возвращает работу в очередь и назначает срок следующей попытки.
func (q *queue) Retry(_ context.Context, id string, at time.Time) error {
	return q.setStatus(id, StatusQueued, at)
}

// Fail отмечает работу отказом: попытки кончились.
func (q *queue) Fail(_ context.Context, id string) error {
	return q.setStatus(id, StatusFailed, time.Time{})
}

// setStatus меняет статус работы; отсутствие работы — ошибка. Сбой записи
// возвращает строку к прежнему состоянию, чтобы память не расходилась
// с файлом.
func (q *queue) setStatus(id, status string, at time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.items[id]
	if !ok {
		return fmt.Errorf("jobs: работа %q не найдена", id)
	}
	prevStatus, prevAt := it.Status, it.Job.AvailableAt
	it.Status = status
	it.Job.AvailableAt = at
	if err := q.persist(); err != nil {
		it.Status = prevStatus
		it.Job.AvailableAt = prevAt
		return err
	}
	return nil
}

// persist сохраняет состояние под уже взятым мьютексом. Без save очередь
// остаётся в памяти.
func (q *queue) persist() error {
	if q.save == nil {
		return nil
	}
	return q.save(state{Seq: q.seq, Items: q.items})
}

// maxSeq — счётчик не меньше самого большого номера в записях: после
// перезапуска новые работы не должны повторить старый номер.
func maxSeq(seq int, items map[string]*item) int {
	for id := range items {
		n, err := strconv.Atoi(id)
		if err != nil {
			continue
		}
		if n > seq {
			seq = n
		}
	}
	return seq
}

// Dispatcher — разборщик работ: забирает по одной, вызывает обработчик и
// переводит статус. Шаг отделён от цикла, чтобы тесты не спали.
type Dispatcher struct {
	queue Queue
	reg   *Registry
	now   func() time.Time
	nudge chan struct{}
	log   *log.Logger

	mu      sync.Mutex
	paused  bool
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
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

// Name — имя службы для группы.
func (d *Dispatcher) Name() string { return "jobs" }

// Start запускает разбор очереди в отдельной горутине и сразу возвращается:
// служба готова принимать работу. Остановка — через контекст, который
// передаётся в Stop.
func (d *Dispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return errors.New("jobs: разборщик уже запущен")
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	d.cancel = cancel
	d.done = done
	d.started = true
	d.mu.Unlock()

	go func() {
		defer close(done)
		if err := d.Run(runCtx); err != nil {
			d.log.Printf("разборщик: %v", err)
		}
	}()
	return nil
}

// Stop отменяет разбор и ждёт завершения его горутины. Текущая работа
// получает отмену через контекст; если она не успела, возвращается ошибка
// срока.
func (d *Dispatcher) Stop(ctx context.Context) error {
	d.mu.Lock()
	started, cancel, done := d.started, d.cancel, d.done
	d.mu.Unlock()
	if !started {
		return nil
	}

	cancel()
	select {
	case <-done:
		d.mu.Lock()
		d.started = false
		d.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

// Pause прекращает захват новых работ. Текущая работа доигрывается: Step
// выполняется в вызывающей горутине, и прервать её можно только отменой
// контекста, то есть остановкой. Повторный вызов ничего не меняет.
func (d *Dispatcher) Pause(_ context.Context) error {
	d.mu.Lock()
	d.paused = true
	d.mu.Unlock()
	return nil
}

// Resume возобновляет захват и будит разборщик, чтобы пауза не ждала тикера.
func (d *Dispatcher) Resume(_ context.Context) error {
	d.mu.Lock()
	d.paused = false
	d.mu.Unlock()
	d.Notify()
	return nil
}

// Paused сообщает, стоит ли разборщик на паузе.
func (d *Dispatcher) Paused() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.paused
}

// Get отдаёт работу и её статус для показа снаружи. ok равно false, если
// работы нет.
func (d *Dispatcher) Get(ctx context.Context, id string) (Job, string, bool, error) {
	return d.queue.Get(ctx, id)
}

// Step делает один шаг: забирает работу, выполняет, обновляет статус.
// Возвращает true, если работа была взята. Ошибка обработчика возвращается
// для журнала, но статус уже обновлён.
func (d *Dispatcher) Step(ctx context.Context) (bool, error) {
	// Остановка не берёт новые работы: уже взятая доигрывает или получает
	// отмену через контекст, но очередь после этого не разбирается.
	if ctx.Err() != nil {
		return false, nil
	}
	// Пауза проверяется до захвата: работа, взятая раньше, доигрывается.
	if d.Paused() {
		return false, nil
	}

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
