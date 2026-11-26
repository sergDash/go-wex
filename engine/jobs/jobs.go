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
	"go-wex/engine/service"
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
// видит номер текущей попытки. CreatedAt — момент постановки, он не меняется.
// StartedAt — начало последней попытки; у работы в очереди время начала пустое.
// NotBefore — момент, раньше которого работу не брать; ноль означает «можно
// сразу». Key — необязательный ключ живой работы: пока работа с этим ключом
// ждёт или выполняется, вторая с таким же ключом не ставится.
// Теги JSON — форма записи в хранилище.
type Job struct {
	ID        string          `json:"id"`
	Kind      Kind            `json:"kind"`
	Key       string          `json:"key,omitempty"`
	Class     Class           `json:"class"`
	Payload   json.RawMessage `json:"payload"`
	Attempts  int             `json:"attempts"`
	NotBefore time.Time       `json:"notBefore"`
	CreatedAt time.Time       `json:"createdAt"`
	StartedAt time.Time       `json:"startedAt"`
}

// Request — то, что ставится в очередь. Время постановки сюда не входит:
// его называет разборщик, поэтому в проверках момент задаётся точно.
//
// Class пустой берётся из правила вида, а если и там пусто — считается
// фоновым. NotBefore — срок, к которому нужен результат; порядок захвата
// строится по нему, а не по числовому приоритету.
type Request struct {
	Kind      Kind
	Payload   any
	Class     Class
	Key       string
	NotBefore time.Time
}

// Handler — обработчик работы. Ошибка означает, что работу нужно повторить
// или, если попытки кончились, отметить отказом.
type Handler func(context.Context, Job) error

// nextError — результат обработчика «работа не завершена, вернуть её через
// промежуток». Отдельный тип, а не строка: разборщик отличает этот исход
// от ошибки и от успеха.
type nextError struct {
	after time.Duration
}

func (e *nextError) Error() string {
	return fmt.Sprintf("jobs: работа вернётся в очередь через %s", e.after)
}

// Next сообщает, что работа не завершена и должна вернуться в очередь через
// after. Счётчик попыток обнуляется: это следующий круг, а не повтор после
// ошибки. Так работа, нужная периодически, ставит себя снова одной записью,
// и окна между «завершить» и «поставить заново» не возникает.
func Next(after time.Duration) error {
	return &nextError{after: after}
}

// permanentError — ошибка, которую бессмысленно повторять: нет смысла
// пробовать ещё раз то, что не станет возможным.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }

func (e *permanentError) Unwrap() error { return e.err }

// Permanent помечает ошибку как постоянную: работа сразу получает отказ,
// попытки не расходуются. Так отвечает обработчик, который понял, что вход
// неверен, а не что ресурс временно недоступен.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

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
	Enqueue(ctx context.Context, now time.Time, req Request) (string, error)
	Claim(ctx context.Context, now time.Time, ready func(Job) bool) (Job, bool, error)
	Done(ctx context.Context, id string) error
	Retry(ctx context.Context, id string, at time.Time) error
	Fail(ctx context.Context, id string) error
	Cancel(ctx context.Context, id string) error
	Postpone(ctx context.Context, id string, at time.Time) error
	Requeue(ctx context.Context, id string, at time.Time) error
	Get(ctx context.Context, id string) (Job, string, bool, error)
	List(ctx context.Context) ([]Entry, error)
}

// Spec — правило вида работы. Class — класс по умолчанию: если при
// постановке класс не задан, работа получает этот. Timeout — предел одной
// попытки: по нему разборщик перестаёт ждать обработчик. MinIdle — сколько
// простоя нужно, чтобы начать работу этого вида: пока человек действовал
// позже, чем MinIdle назад, работа ждёт.
type Spec struct {
	Class       Class
	Handler     Handler
	Timeout     time.Duration
	MinIdle     time.Duration
	MaxAttempts int
	Backoff     time.Duration
}

// AllowedFunc сообщает, какие классы разрешены в момент now. Пустой набор
// останавливает захват: это пауза по расписанию.
type AllowedFunc func(now time.Time) []Class

// IdleFunc сообщает, когда человек последний раз действовал и занят ли
// процесс запросом. Простой считается от этого момента, поэтому источник
// активности выбирает приложение: для одного проекта это запрос, для
// другого — только реплика игрока.
type IdleFunc func(now time.Time) (lastActive time.Time, busy bool)

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
	if s.Timeout < 0 {
		return fmt.Errorf("jobs: вид %q: предел попытки не может быть отрицательным", k)
	}
	if s.MinIdle < 0 {
		return fmt.Errorf("jobs: вид %q: порог простоя не может быть отрицательным", k)
	}
	r.specs[k] = s
	return nil
}

// Spec отдаёт правило вида.
func (r *Registry) Spec(k Kind) (Spec, bool) {
	s, ok := r.specs[k]
	return s, ok
}

// Статусы работы в очереди. Отменённая работа — та, которую убрали до начала:
// выполнять её не нужно, и в отличие от отказа это не ошибка.
const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusDone      = "done"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
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
// keep — сколько завершённых строк держать; ноль оставляет все.
type queue struct {
	mu    sync.Mutex
	seq   int
	items map[string]*item
	keep  int
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
			// Прерванная работа ещё не начиналась заново: время начала
			// прошлой попытки осталось в журнале, а не в строке очереди.
			// Срок не трогаем: он в прошлом, поэтому работу возьмут сразу,
			// а по строке видно, к какому моменту она была нужна.
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

// OpenStoreQueueLater заводит очередь поверх хранилища, которое открывается
// при первом обращении. Нужна подкоманде: команда, которой хранилище
// не нужно, не открывает файл и не становится вторым писателем.
func OpenStoreQueueLater(open func() (*store.Store, error)) Queue {
	return &laterQueue{open: open}
}

// laterQueue — очередь, которая ждёт первого обращения, чтобы открыть
// хранилище. Результат запоминается вместе с ошибкой: повторные обращения
// видят то же самое.
type laterQueue struct {
	once  sync.Once
	open  func() (*store.Store, error)
	inner Queue
	err   error
}

// queue открывает хранилище и очередь при первом обращении.
func (q *laterQueue) queue() (Queue, error) {
	q.once.Do(func() {
		st, err := q.open()
		if err != nil {
			q.err = err
			return
		}
		q.inner, q.err = OpenStoreQueue(st)
	})
	return q.inner, q.err
}

func (q *laterQueue) Enqueue(ctx context.Context, now time.Time, req Request) (string, error) {
	inner, err := q.queue()
	if err != nil {
		return "", err
	}
	return inner.Enqueue(ctx, now, req)
}

func (q *laterQueue) Claim(ctx context.Context, now time.Time, ready func(Job) bool) (Job, bool, error) {
	inner, err := q.queue()
	if err != nil {
		return Job{}, false, err
	}
	return inner.Claim(ctx, now, ready)
}

func (q *laterQueue) Done(ctx context.Context, id string) error {
	inner, err := q.queue()
	if err != nil {
		return err
	}
	return inner.Done(ctx, id)
}

func (q *laterQueue) Retry(ctx context.Context, id string, at time.Time) error {
	inner, err := q.queue()
	if err != nil {
		return err
	}
	return inner.Retry(ctx, id, at)
}

func (q *laterQueue) Fail(ctx context.Context, id string) error {
	inner, err := q.queue()
	if err != nil {
		return err
	}
	return inner.Fail(ctx, id)
}

func (q *laterQueue) Cancel(ctx context.Context, id string) error {
	inner, err := q.queue()
	if err != nil {
		return err
	}
	return inner.Cancel(ctx, id)
}

func (q *laterQueue) Postpone(ctx context.Context, id string, at time.Time) error {
	inner, err := q.queue()
	if err != nil {
		return err
	}
	return inner.Postpone(ctx, id, at)
}

func (q *laterQueue) Requeue(ctx context.Context, id string, at time.Time) error {
	inner, err := q.queue()
	if err != nil {
		return err
	}
	return inner.Requeue(ctx, id, at)
}

func (q *laterQueue) Get(ctx context.Context, id string) (Job, string, bool, error) {
	inner, err := q.queue()
	if err != nil {
		return Job{}, "", false, err
	}
	return inner.Get(ctx, id)
}

func (q *laterQueue) List(ctx context.Context) ([]Entry, error) {
	inner, err := q.queue()
	if err != nil {
		return nil, err
	}
	return inner.List(ctx)
}

// SetKeep задаёт, сколько завершённых строк держать. Ноль оставляет все:
// правило выбирает приложение, а не движок.
func (q *queue) SetKeep(n int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.keep = n
}

// Retention — очередь, которая умеет ограничивать число завершённых строк.
// Хранилище с собственным правилом (например, таблица модуля) этот метод
// может не объявлять; тогда настройка движка к нему не применяется.
type Retention interface {
	SetKeep(n int)
}

// Enqueue кладёт работу со статусом queued и возвращает её номер. Момент
// постановки записывается в работу: по нему видно, сколько она прождала.
//
// Если у работы есть ключ и живая работа с этим ключом уже стоит, новая
// не создаётся: возвращается номер прежней. Ключ действует только среди
// ждущих и выполняющихся строк, иначе периодическая работа не смогла бы
// встать снова, пока прошлая не убрана.
func (q *queue) Enqueue(_ context.Context, now time.Time, req Request) (string, error) {
	class := req.Class
	if class == "" {
		class = ClassBackground
	}
	if !class.Valid() {
		return "", fmt.Errorf("jobs: неизвестный класс %q", class)
	}

	raw, err := json.Marshal(req.Payload)
	if err != nil {
		return "", fmt.Errorf("jobs: тело работы %q: %w", req.Kind, err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if req.Key != "" {
		if id, ok := q.liveKey(req.Key); ok {
			return id, nil
		}
	}

	q.seq++
	id := fmt.Sprintf("%06d", q.seq)
	q.items[id] = &item{
		Job: Job{
			ID:        id,
			Kind:      req.Kind,
			Key:       req.Key,
			Class:     class,
			Payload:   raw,
			NotBefore: req.NotBefore,
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

// liveKey ищет работу с этим ключом среди ждущих и выполняющихся: завершённая
// работа ключ не держит.
func (q *queue) liveKey(key string) (string, bool) {
	for id, it := range q.items {
		if it.Job.Key != key {
			continue
		}
		if it.Status == StatusQueued || it.Status == StatusRunning {
			return id, true
		}
	}
	return "", false
}

// Claim забирает одну работу, которую пропускает ready, и которая доступна
// к моменту now: сначала интерактивные, внутри класса — раньше всех по сроку,
// при равенстве по номеру. Правило допуска проверяет разборщик: очередь
// хранит строки и ничего не знает о порогах простоя и правилах видов.
func (q *queue) Claim(_ context.Context, now time.Time, ready func(Job) bool) (Job, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var chosen *item
	for _, it := range q.items {
		if it.Status != StatusQueued || it.Job.NotBefore.After(now) || !ready(it.Job) {
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
	if !a.Job.NotBefore.Equal(b.Job.NotBefore) {
		return a.Job.NotBefore.Before(b.Job.NotBefore)
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
	return q.change(id, StatusDone, time.Time{}, nil)
}

// Retry возвращает работу в очередь и назначает срок следующей попытки.
// Попытка уже посчитана при захвате: повтор — это та же работа, взятая снова.
func (q *queue) Retry(_ context.Context, id string, at time.Time) error {
	return q.change(id, StatusQueued, at, nil)
}

// Postpone возвращает работу в очередь, не считая попытку: работа вернулась
// не из-за ошибки, а потому что момент неподходящий (идёт остановка, ресурс
// ещё не готов). У ждущей работы счётчик не меняется: она и не начиналась.
func (q *queue) Postpone(_ context.Context, id string, at time.Time) error {
	return q.change(id, StatusQueued, at, dropIfRunning)
}

// Requeue возвращает работу в очередь и обнуляет счётчик попыток: это
// следующий круг, а не повтор после ошибки.
func (q *queue) Requeue(_ context.Context, id string, at time.Time) error {
	return q.change(id, StatusQueued, at, resetAttempts)
}

// Fail отмечает работу отказом: попытки кончились.
func (q *queue) Fail(_ context.Context, id string) error {
	return q.change(id, StatusFailed, time.Time{}, nil)
}

// Cancel убирает ждущую работу: выполнять её не нужно. Взятую работу
// отменить нельзя — она уже выполняется, и остановить её может только отмена
// контекста разборщика.
func (q *queue) Cancel(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	it, ok := q.items[id]
	if !ok {
		return fmt.Errorf("jobs: работа %q не найдена", id)
	}
	if it.Status != StatusQueued {
		return fmt.Errorf("jobs: работа %q в статусе %q: отменить можно только ждущую", id, it.Status)
	}
	it.Status = StatusCancelled
	if err := q.persist(); err != nil {
		it.Status = StatusQueued
		return err
	}
	return nil
}

// change меняет статус работы; отсутствие работы — ошибка. Сбой записи
// возвращает строку к прежнему состоянию, чтобы память не расходилась
// с файлом. Возврат в очередь назначает новый срок и очищает время начала:
// работа снова ждёт, и прошлая попытка остаётся только в журнале.
//
// attempts задаёт, что делать со счётчиком попыток, и видит прежний статус:
// nil — не трогать.
func (q *queue) change(id, status string, at time.Time, attempts func(prevStatus string, n int) int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.items[id]
	if !ok {
		return fmt.Errorf("jobs: работа %q не найдена", id)
	}
	prevStatus, prevAt, prevStarted, prevAttempts := it.Status, it.Job.NotBefore, it.Job.StartedAt, it.Job.Attempts
	if attempts != nil {
		it.Job.Attempts = attempts(prevStatus, it.Job.Attempts)
	}
	it.Status = status
	// У завершённой работы срок остаётся: по нему видно, к какому моменту
	// она была нужна, — это история строки, а не условие захвата.
	if status == StatusQueued {
		it.Job.NotBefore = at
		it.Job.StartedAt = time.Time{}
	}
	if err := q.persist(); err != nil {
		it.Status = prevStatus
		it.Job.NotBefore = prevAt
		it.Job.StartedAt = prevStarted
		it.Job.Attempts = prevAttempts
		return err
	}
	return nil
}

// dropIfRunning — попытка не считается, если работу уже взяли: у ждущей
// работы счётчик и не менялся.
func dropIfRunning(prevStatus string, n int) int {
	if prevStatus == StatusRunning && n > 0 {
		return n - 1
	}
	return n
}

// resetAttempts — счёт начинается заново: работа идёт следующий круг.
func resetAttempts(string, int) int { return 0 }

// persist сохраняет состояние под уже взятым мьютексом. Без save очередь
// остаётся в памяти, но правило хранения действует и там: иначе поведение
// зависело бы от того, лежит очередь на диске или нет.
//
// Завершённые строки сверх правила хранения в файл не попадают, а память
// приводится к тому же виду только после успешной записи: сбой не должен
// уносить строки молча.
func (q *queue) persist() error {
	items := q.items
	if q.keep > 0 {
		items = retained(q.items, q.keep)
	}
	if q.save != nil {
		if err := q.save(state{Seq: q.seq, Items: items}); err != nil {
			return err
		}
	}
	q.items = items
	return nil
}

// retained отдаёт строки, где завершённых не больше keep: первыми уходят
// самые старые по номеру. Ждущие и выполняющиеся остаются всегда — их
// потеря означала бы потерю работы.
func retained(items map[string]*item, keep int) map[string]*item {
	finished := make([]string, 0, len(items))
	for id, it := range items {
		if it.Status == StatusQueued || it.Status == StatusRunning {
			continue
		}
		finished = append(finished, id)
	}
	if len(finished) <= keep {
		return items
	}

	sort.Slice(finished, func(i, j int) bool { return idLess(finished[i], finished[j]) })
	drop := make(map[string]bool, len(finished)-keep)
	for _, id := range finished[:len(finished)-keep] {
		drop[id] = true
	}

	out := make(map[string]*item, len(items)-len(drop))
	for id, it := range items {
		if drop[id] {
			continue
		}
		out[id] = it
	}
	return out
}

// idLess сравнивает номера как числа: строковое сравнение сломалось бы
// на седьмом знаке.
func idLess(a, b string) bool {
	na, errA := strconv.Atoi(a)
	nb, errB := strconv.Atoi(b)
	if errA == nil && errB == nil {
		return na < nb
	}
	return a < b
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
	idle    IdleFunc
	started bool
	cancel  context.CancelFunc
	done    chan error
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

// SetKeep передаёт очереди правило хранения завершённых строк. Очередь
// с собственным правилом (например, поверх таблицы) метод не объявляет —
// тогда настройка движка к ней не применяется.
func (d *Dispatcher) SetKeep(n int) {
	if r, ok := d.queue.(Retention); ok {
		r.SetKeep(n)
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
	done := make(chan error, 1)
	d.cancel = cancel
	d.done = done
	d.started = true
	d.mu.Unlock()

	// Граница службы: паника в цикле разбора становится ошибкой и уходит
	// в канал итога, а не завершает процесс.
	go func() {
		defer close(done)
		done <- service.Guard(func() error { return d.Run(runCtx) })
	}()
	return nil
}

// Done отдаёт канал итога работы разборщика: в него приходит причина и канал
// закрывается. Группа служб по нему узнаёт, что разборщик встал.
func (d *Dispatcher) Done() <-chan error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.done
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
// вида; если и там пусто, работа считается фоновой. Срок и ключ приходят
// в Request: очередь хранит их как данные, а не выводит из вида.
func (d *Dispatcher) Enqueue(ctx context.Context, req Request) (string, error) {
	spec, ok := d.reg.Spec(req.Kind)
	if !ok {
		return "", fmt.Errorf("jobs: вид %q не объявлен", req.Kind)
	}
	if req.Class == "" {
		req.Class = spec.Class
	}
	if req.Class == "" {
		req.Class = ClassBackground
	}
	if !req.Class.Valid() {
		return "", fmt.Errorf("jobs: неизвестный класс %q", req.Class)
	}

	id, err := d.queue.Enqueue(ctx, d.clock.Now(), req)
	if err != nil {
		return "", err
	}
	d.Notify()
	return id, nil
}

// Cancel убирает ждущую работу. Взятую работу отменить нельзя: она уже
// выполняется, и её ход останавливает только остановка разборщика.
func (d *Dispatcher) Cancel(ctx context.Context, id string) error {
	return d.queue.Cancel(ctx, id)
}

// Postpone возвращает взятую работу в очередь, не считая попытку.
func (d *Dispatcher) Postpone(ctx context.Context, id string, at time.Time) error {
	if err := d.queue.Postpone(ctx, id, at); err != nil {
		return err
	}
	d.Notify()
	return nil
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

// SetIdleFunc задаёт источник активности: когда человек действовал и занят
// ли процесс. Без него простой не ограничивает работу, а занятость считается
// нулевой — так проверки не заводят лишних зависимостей.
func (d *Dispatcher) SetIdleFunc(fn IdleFunc) {
	d.mu.Lock()
	d.idle = fn
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

// idleAt отдаёт активность на момент now. Как и расписание, источник
// активности копируется под мьютексом и вызывается без него.
func (d *Dispatcher) idleAt(now time.Time) (time.Time, bool) {
	d.mu.Lock()
	fn := d.idle
	d.mu.Unlock()
	if fn == nil {
		return time.Time{}, false
	}
	return fn(now)
}

// ready собирает правило допуска на момент now: класс разрешён расписанием,
// фон не берётся, пока идёт запрос, и у вида прошёл порог простоя.
func (d *Dispatcher) ready(now time.Time) func(Job) bool {
	allowed := d.allowedAt(now)
	lastActive, busy := d.idleAt(now)

	return func(job Job) bool {
		if !classAllowed(allowed, job.Class) {
			return false
		}
		if job.Class == ClassBackground && busy {
			return false
		}
		spec, ok := d.reg.Spec(job.Kind)
		if !ok {
			// Вид без правила берётся и получает отказ в Step: пропускать
			// такую строку молча — значит прятать ошибку объявления.
			return true
		}
		return idleEnough(now, lastActive, spec.MinIdle)
	}
}

// idleEnough сообщает, прошёл ли порог простоя. Нулевой порог не требует
// простоя вовсе, а отсутствие активности означает, что человек ещё не
// действовал и ждать нечего.
func idleEnough(now, lastActive time.Time, minIdle time.Duration) bool {
	if minIdle <= 0 || lastActive.IsZero() {
		return true
	}
	return now.Sub(lastActive) >= minIdle
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
	job, ok, err := d.queue.Claim(ctx, now, d.ready(now))
	if err != nil || !ok {
		return false, err
	}

	spec, ok := d.reg.Spec(job.Kind)
	if !ok {
		_ = d.queue.Fail(ctx, job.ID)
		return true, fmt.Errorf("jobs: нет обработчика вида %q", job.Kind)
	}

	err = d.run(ctx, spec, job)

	fields := jobFields(job)
	fields["took_ms"] = d.clock.Now().Sub(now).Milliseconds()

	if err == nil {
		d.writeJournal("done", fields)
		return true, d.queue.Done(ctx, job.ID)
	}

	fields["error"] = err.Error()

	// Остановка: работа не виновата в том, что процесс закрывается. Попытка
	// не считается, работа снова ждёт в очереди. Запись должна пережить
	// отмену контекста, поэтому берётся контекст без отмены.
	if ctx.Err() != nil {
		d.writeJournal("postponed", fields)
		return true, d.queue.Postpone(context.WithoutCancel(ctx), job.ID, time.Time{})
	}

	// Следующий круг: работа сама назначает себе срок. Это не ошибка,
	// поэтому шаг не возвращает её наружу.
	var next *nextError
	if errors.As(err, &next) {
		at := now.Add(next.after)
		fields["next"] = at
		d.writeJournal("next", fields)
		return true, d.queue.Requeue(ctx, job.ID, at)
	}

	// Постоянная ошибка: повторять нечего.
	var permanent *permanentError
	if errors.As(err, &permanent) {
		d.writeJournal("fail", fields)
		return true, errors.Join(err, d.queue.Fail(ctx, job.ID))
	}

	if job.Attempts >= attempts(spec) {
		d.writeJournal("fail", fields)
		return true, errors.Join(err, d.queue.Fail(ctx, job.ID))
	}

	kind := "retry"
	if errors.Is(err, context.DeadlineExceeded) {
		kind = "timeout"
	}
	delay := backoff(spec)
	fields["backoff_ms"] = delay.Milliseconds()
	d.writeJournal(kind, fields)
	return true, errors.Join(err, d.queue.Retry(ctx, job.ID, now.Add(delay)))
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

// run выполняет обработчик в границах вида: паника становится ошибкой,
// а предел попытки возвращает управление разборщику, не дожидаясь
// обработчика.
func (d *Dispatcher) run(ctx context.Context, spec Spec, job Job) error {
	if spec.Timeout <= 0 {
		return d.call(ctx, spec.Handler, job)
	}

	runCtx, cancel := context.WithTimeout(ctx, spec.Timeout)
	defer cancel()

	// Канал с запасом: обработчик, завершившийся после срока, не должен
	// ждать читателя.
	done := make(chan error, 1)
	go func() {
		done <- d.call(runCtx, spec.Handler, job)
	}()

	select {
	case err := <-done:
		return err
	case <-runCtx.Done():
		// Разборщик свободен. Обработчик, который не смотрит на контекст,
		// останется горутиной до своего завершения — цена записана
		// в doc/capabilities/Недостатки.md.
		return fmt.Errorf("jobs: работа %q: предел попытки %s: %w", job.Kind, spec.Timeout, runCtx.Err())
	}
}

// call выполняет обработчик и превращает панику в ошибку: одна работа
// не должна ронять разборщик.
func (d *Dispatcher) call(ctx context.Context, h Handler, job Job) (err error) {
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
