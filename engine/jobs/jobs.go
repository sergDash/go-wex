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
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"time"

	"go-wex/engine/clock"
	"go-wex/engine/journal"
	"go-wex/engine/store"
)

// Kind — вид работы; по нему выбирается обработчик.
type Kind string

// Class — класс работы. Интерактивная нужна ответу сейчас, фоновая может
// ждать: при выборе следующей работы интерактивная идёт первой.
type Class string

const (
	ClassInteractive Class = "interactive"
	ClassBackground  Class = "background"
)

// Valid сообщает, известен ли класс.
func (c Class) Valid() bool {
	return c == ClassInteractive || c == ClassBackground
}

// Job — работа в очереди. Attempts считается при захвате, поэтому обработчик
// видит номер текущей попытки. CreatedAt — момент постановки, StartedAt —
// начало последней попытки; у работы в очереди время начала пустое.
// Теги JSON — форма записи в хранилище.
type Job struct {
	ID          string          `json:"id"`
	Kind        Kind            `json:"kind"`
	Class       Class           `json:"class"`
	Payload     json.RawMessage `json:"payload"`
	Attempts    int             `json:"attempts"`
	AvailableAt time.Time       `json:"availableAt"`
	CreatedAt   time.Time       `json:"createdAt"`
	StartedAt   time.Time       `json:"startedAt"`
}

// Handler — обработчик работы. Ошибка означает, что работу нужно повторить
// или, если попытки кончились, отметить отказом.
type Handler func(context.Context, Job) error

// Entry — строка очереди для показа снаружи: работа и её статус. Статус
// хранится рядом с работой, а не внутри неё, поэтому отдаётся отдельным полем.
type Entry struct {
	Job    Job
	Status string
}

// Queue — хранилище работ. Реализация в памяти годится для образца; долговечная
// очередь реализует тот же набор методов поверх хранилища.
//
// Время приходит аргументом, а не берётся внутри: момент постановки —
// такое же данное, как вид и тело, и в проверках он задаётся точно.
type Queue interface {
	Enqueue(ctx context.Context, now time.Time, kind Kind, payload any, class Class) (string, error)
	Claim(ctx context.Context, now time.Time, allowed []Class) (Job, bool, error)
	Done(ctx context.Context, id string) error
	Retry(ctx context.Context, id string, at time.Time) error
	Fail(ctx context.Context, id string) error
	Get(ctx context.Context, id string) (Job, string, bool, error)
	List(ctx context.Context) ([]Entry, error)
}

// Spec — правило вида работы. Class — класс по умолчанию: если при
// постановке класс не задан, работа получает этот.
type Spec struct {
	Class       Class
	Handler     Handler
	MaxAttempts int
	Backoff     time.Duration
}

// AllowedFunc сообщает, какие классы разрешены в момент now. Пустой набор
// останавливает захват: это пауза по расписанию.
type AllowedFunc func(now time.Time) []Class

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
	if s.Class != "" && !s.Class.Valid() {
		return fmt.Errorf("jobs: вид %q: неизвестный класс %q", k, s.Class)
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

	changed := false
	for _, it := range q.items {
		// Записи без класса появились до его введения: фон — безопасное
		// умолчание, такая работа не обгоняет интерактивную.
		if it.Job.Class == "" {
			it.Job.Class = ClassBackground
			changed = true
		}
		if it.Status == StatusRunning {
			it.Status = StatusQueued
			it.Job.AvailableAt = time.Time{}
			// Прерванная работа ещё не начиналась заново: время начала
			// прошлой попытки осталось в журнале, а не в строке очереди.
			it.Job.StartedAt = time.Time{}
			changed = true
		}
	}
	if changed {
		if err := q.persist(); err != nil {
			return nil, err
		}
	}
	return q, nil
}

// Enqueue кладёт работу со статусом queued и возвращает её номер. Момент
// постановки записывается в работу: по нему видно, сколько она прождала.
func (q *queue) Enqueue(_ context.Context, now time.Time, kind Kind, payload any, class Class) (string, error) {
	if class == "" {
		class = ClassBackground
	}
	if !class.Valid() {
		return "", fmt.Errorf("jobs: неизвестный класс %q", class)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("jobs: тело работы %q: %w", kind, err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq++
	id := fmt.Sprintf("%06d", q.seq)
	q.items[id] = &item{
		Job: Job{
			ID:        id,
			Kind:      kind,
			Class:     class,
			Payload:   raw,
			CreatedAt: now,
		},
		Status: StatusQueued,
	}
	if err := q.persist(); err != nil {
		delete(q.items, id)
		q.seq--
		return "", err
	}
	return id, nil
}

// Claim забирает одну работу из разрешённых классов, доступную к моменту now:
// сначала интерактивные, внутри класса — раньше всех по сроку, при равенстве
// по номеру. Пустой allowed означает, что брать нечего: это пауза
// по расписанию.
func (q *queue) Claim(_ context.Context, now time.Time, allowed []Class) (Job, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var chosen *item
	for _, it := range q.items {
		if it.Status != StatusQueued || it.Job.AvailableAt.After(now) || !classAllowed(allowed, it.Job.Class) {
			continue
		}
		if chosen == nil || earlier(it, chosen) {
			chosen = it
		}
	}
	if chosen == nil {
		return Job{}, false, nil
	}

	chosen.Status = StatusRunning
	chosen.Job.Attempts++
	chosen.Job.StartedAt = now
	if err := q.persist(); err != nil {
		chosen.Job.Attempts--
		chosen.Job.StartedAt = time.Time{}
		chosen.Status = StatusQueued
		return Job{}, false, err
	}
	return chosen.Job, true, nil
}

// earlier сообщает, должна ли работа a быть взята раньше b: интерактивная
// раньше фоновой, внутри класса — по сроку, при равенстве — по номеру.
func earlier(a, b *item) bool {
	if a.Job.Class != b.Job.Class {
		return a.Job.Class == ClassInteractive
	}
	if !a.Job.AvailableAt.Equal(b.Job.AvailableAt) {
		return a.Job.AvailableAt.Before(b.Job.AvailableAt)
	}
	return a.Job.ID < b.Job.ID
}

// classAllowed сообщает, разрешён ли класс. Пустой список не разрешает ничего.
func classAllowed(allowed []Class, class Class) bool {
	for _, c := range allowed {
		if c == class {
			return true
		}
	}
	return false
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

// List отдаёт все строки очереди по возрастанию номера: порядок не зависит
// от того, как легли записи в карте, поэтому таблица снаружи не прыгает
// между обновлениями.
func (q *queue) List(_ context.Context) ([]Entry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]Entry, 0, len(q.items))
	for _, it := range q.items {
		out = append(out, Entry{Job: it.Job, Status: it.Status})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Job.ID < out[j].Job.ID })
	return out, nil
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
// с файлом. Возврат в очередь очищает время начала: работа снова ждёт,
// и прошлая попытка остаётся только в журнале.
func (q *queue) setStatus(id, status string, at time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.items[id]
	if !ok {
		return fmt.Errorf("jobs: работа %q не найдена", id)
	}
	prevStatus, prevAt, prevStarted := it.Status, it.Job.AvailableAt, it.Job.StartedAt
	it.Status = status
	it.Job.AvailableAt = at
	if status == StatusQueued {
		it.Job.StartedAt = time.Time{}
	}
	if err := q.persist(); err != nil {
		it.Status = prevStatus
		it.Job.AvailableAt = prevAt
		it.Job.StartedAt = prevStarted
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
	queue   Queue
	reg     *Registry
	clock   clock.Clock
	nudge   chan struct{}
	log     *log.Logger
	journal journal.Writer

	mu      sync.Mutex
	paused  bool
	allowed AllowedFunc
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewDispatcher собирает разборщик.
func NewDispatcher(q Queue, r *Registry) *Dispatcher {
	return &Dispatcher{
		queue:   q,
		reg:     r,
		clock:   clock.Real{},
		nudge:   make(chan struct{}, 1),
		log:     log.Default(),
		journal: journal.Discard{},
		allowed: func(time.Time) []Class {
			return []Class{ClassInteractive, ClassBackground}
		},
	}
}

// SetClock подменяет часы; для проверок границ повтора и ожидания.
func (d *Dispatcher) SetClock(c clock.Clock) {
	if c != nil {
		d.clock = c
	}
}

// SetJournal подключает журнал работы: в него пишутся выполнения, повторы,
// отказы и паники.
func (d *Dispatcher) SetJournal(j journal.Writer) {
	d.journal = j
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

// Enqueue ставит работу и будит разборщик. Пустой класс берётся из правила
// вида; если и там пусто, работа считается фоновой.
func (d *Dispatcher) Enqueue(ctx context.Context, kind Kind, payload any, class Class) (string, error) {
	spec, ok := d.reg.Spec(kind)
	if !ok {
		return "", fmt.Errorf("jobs: вид %q не объявлен", kind)
	}
	if class == "" {
		class = spec.Class
	}
	if class == "" {
		class = ClassBackground
	}
	if !class.Valid() {
		return "", fmt.Errorf("jobs: неизвестный класс %q", class)
	}

	id, err := d.queue.Enqueue(ctx, d.clock.Now(), kind, payload, class)
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

// SetAllowed задаёт классы, которые разборщик берёт сейчас. Пустой список
// останавливает захват, как пустое расписание; ручная пауза этим не
// отменяется. Возобновление будит разборщик подсказкой.
func (d *Dispatcher) SetAllowed(classes []Class) {
	allowed := append([]Class(nil), classes...)
	d.SetAllowedFunc(func(time.Time) []Class {
		return append([]Class(nil), allowed...)
	})
}

// SetAllowedFunc задаёт правило выбора классов на момент захвата. Так
// подключается расписание: оно учитывает простой и занятость запросами.
func (d *Dispatcher) SetAllowedFunc(fn AllowedFunc) {
	d.mu.Lock()
	d.allowed = fn
	d.mu.Unlock()
	d.Notify()
}

// allowedAt отдаёт классы на момент now. Функция копируется под мьютексом
// и вызывается без него: расписание не должно ждать разборщик.
func (d *Dispatcher) allowedAt(now time.Time) []Class {
	d.mu.Lock()
	fn := d.allowed
	d.mu.Unlock()
	if fn == nil {
		return []Class{ClassInteractive, ClassBackground}
	}
	return fn(now)
}

// Get отдаёт работу и её статус для показа снаружи. ok равно false, если
// работы нет.
func (d *Dispatcher) Get(ctx context.Context, id string) (Job, string, bool, error) {
	return d.queue.Get(ctx, id)
}

// Known сообщает, объявлен ли вид работ. События спрашивают это, когда их
// добавляют данными: без обработчика работа оказалась бы без исполнителя.
func (d *Dispatcher) Known(kind Kind) bool {
	_, ok := d.reg.Spec(kind)
	return ok
}

// List отдаёт все работы очереди по возрастанию номера: по нему строится
// таблица состояния снаружи.
func (d *Dispatcher) List(ctx context.Context) ([]Entry, error) {
	return d.queue.List(ctx)
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

	now := d.clock.Now()
	job, ok, err := d.queue.Claim(ctx, now, d.allowedAt(now))
	if err != nil || !ok {
		return false, err
	}

	spec, ok := d.reg.Spec(job.Kind)
	if !ok {
		_ = d.queue.Fail(ctx, job.ID)
		return true, fmt.Errorf("jobs: нет обработчика вида %q", job.Kind)
	}

	if err := d.run(ctx, spec.Handler, job); err != nil {
		fields := jobFields(job)
		fields["error"] = err.Error()
		fields["took_ms"] = d.clock.Now().Sub(now).Milliseconds()

		if job.Attempts >= attempts(spec) {
			d.writeJournal("fail", fields)
			return true, errors.Join(err, d.queue.Fail(ctx, job.ID))
		}

		delay := backoff(spec)
		fields["backoff_ms"] = delay.Milliseconds()
		d.writeJournal("retry", fields)
		return true, errors.Join(err, d.queue.Retry(ctx, job.ID, now.Add(delay)))
	}

	fields := jobFields(job)
	fields["took_ms"] = d.clock.Now().Sub(now).Milliseconds()
	d.writeJournal("done", fields)
	return true, d.queue.Done(ctx, job.ID)
}

// jobFields — общие поля события работы.
func jobFields(job Job) map[string]any {
	return map[string]any{
		"id":       job.ID,
		"kind":     string(job.Kind),
		"class":    string(job.Class),
		"attempts": job.Attempts,
	}
}

// writeJournal пишет событие и не мешает разбору, если журнал недоступен.
func (d *Dispatcher) writeJournal(kind string, fields map[string]any) {
	if d.journal == nil {
		return
	}
	if err := d.journal.Write("jobs", kind, fields); err != nil {
		d.log.Printf("журнал: %v", err)
	}
}

// Run разбирает очередь до отмены контекста: сначала всё доступное, затем
// ждёт срабатывание часов или подсказку.
func (d *Dispatcher) Run(ctx context.Context) error {
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
		case <-d.clock.After(time.Second):
		case <-d.nudge:
		}
	}
}

// run выполняет обработчик и превращает панику в ошибку: одна работа
// не должна ронять разборщик.
func (d *Dispatcher) run(ctx context.Context, h Handler, job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			d.writeJournal("panic", map[string]any{
				"id":    job.ID,
				"kind":  string(job.Kind),
				"class": string(job.Class),
				"panic": fmt.Sprint(r),
				"stack": string(debug.Stack()),
			})
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
