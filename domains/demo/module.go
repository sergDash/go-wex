// Package demo — демонстрационный модуль: в одном месте собраны HTTP,
// фоновая работа, слушатели TCP и UDP, управляемый дочерний процесс
// и подкоманда. Это пример возможностей движка, а не предметная область.
package demo

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go-wex/engine/config"
	"go-wex/engine/core"
	"go-wex/engine/events"
	"go-wex/engine/gate"
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/listen"
	"go-wex/engine/process"
	"go-wex/engine/service"
	"go-wex/engine/version"
)

const (
	// kind — вид работы для ручной постановки; класс фоновый, поэтому
	// она ждёт простоя.
	kind = jobs.Kind("demo.echo")

	// eventKind — вид отложенного события. Работа та же очередь, но срок
	// приходит не из запроса, а от службы событий.
	eventKind = jobs.Kind("demo.tick")

	// defaultTickPeriod — период демонстрационного события по умолчанию:
	// десять секунд, чтобы срабатывание было видно без долгого ожидания.
	// Настройка modules.settings.demo может его изменить.
	defaultTickPeriod = 10 * time.Second

	// generateKind — вид работы «ход по слоту»: та же работа, что синхронный
	// ход, но выполняется в очереди.
	generateKind = jobs.Kind("demo.generate")

	// defaultMinIdle — сколько простоя нужно виду demo.echo по умолчанию.
	// Порог у вида, а не у класса: разные виды ждут разное время.
	defaultMinIdle = 3 * time.Second

	// queueLimit — сколько последних работ показывать в таблице. Очередь
	// растёт от срабатываний события, и вся она в интерфейсе не нужна.
	queueLimit = 20
)

// module — демонстрационный модуль.
type module struct {
	disp    *jobs.Dispatcher
	journal journal.Writer
	states  func() []service.State
	events  *events.Scheduler
	slot    *gate.Gate
	watch   *watchService

	// period и minIdle приходят настройками модуля: период события и порог
	// простоя вида demo.echo.
	period  time.Duration
	minIdle time.Duration

	tcp   *listen.TCP
	udp   *listen.UDP
	lines atomic.Int64
	ticks atomic.Int64
}

// New отдаёт модуль для списка в точке сборки. states называет состояние
// служб: оно собирается после регистрации модулей, поэтому приходит
// функцией, а не значением. events добавляет и показывает события; в режиме
// подкоманды его нет. settings — раздел modules.settings.demo.
func New(disp *jobs.Dispatcher, j journal.Writer, states func() []service.State, ev *events.Scheduler, slot *gate.Gate, settings json.RawMessage) (core.Module, error) {
	if j == nil {
		j = journal.Discard{}
	}
	if slot == nil {
		slot = gate.New(1)
	}

	period, idle, err := parseSettings(settings)
	if err != nil {
		return nil, err
	}

	return &module{
		disp:    disp,
		journal: j,
		states:  states,
		events:  ev,
		slot:    slot,
		watch:   newWatch(),
		period:  period,
		minIdle: idle,
	}, nil
}

// settings — раздел настроек модуля. Сроки задаются строкой, как и в
// настройках приложения; пустое значение означает умолчание.
type settings struct {
	TickPeriod string `json:"tickPeriod"`
	MinIdle    string `json:"minIdle"`
}

// parseSettings читает настройки модуля: неизвестный ключ — ошибка, как
// и в настройках приложения.
func parseSettings(raw json.RawMessage) (time.Duration, time.Duration, error) {
	var in settings
	if err := config.DecodeSection(raw, &in); err != nil {
		return 0, 0, fmt.Errorf("demo: настройки: %w", err)
	}

	period, err := duration(in.TickPeriod, defaultTickPeriod)
	if err != nil {
		return 0, 0, fmt.Errorf("demo: tickPeriod: %w", err)
	}
	idle, err := duration(in.MinIdle, defaultMinIdle)
	if err != nil {
		return 0, 0, fmt.Errorf("demo: minIdle: %w", err)
	}
	return period, idle, nil
}

// duration читает необязательный срок: пустое значение даёт умолчание.
func duration(value string, def time.Duration) (time.Duration, error) {
	if value == "" {
		return def, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, errors.New("срок должен быть больше нуля")
	}
	return d, nil
}

func (*module) Name() string { return "demo" }

// Register объявляет все возможности: маршруты, работу, службы и подкоманду.
func (m *module) Register(r core.Registry) error {
	m.tcp = listen.NewTCP("127.0.0.1:0", m.echoTCP, m.journal)
	m.udp = listen.NewUDP("127.0.0.1:0", m.echoUDP, m.journal)

	r.Service(m.tcp)
	r.Service(m.udp)
	// Служба с границей паники: по команде она падает и перезапускается.
	r.Service(m.watch)
	r.Service(process.New(process.Spec{
		Name:    "demo",
		Args:    []string{"demo", "child"},
		Grace:   time.Second,
		Journal: m.journal,
	}))

	r.HTTP("GET", "/api/demo", m.snapshot)
	r.HTTP("GET", "/api/services", m.services)
	r.HTTP("POST", "/api/demo/watch/crash", m.crashWatch)
	r.HTTP("GET", "/api/events", m.eventsList)
	r.HTTP("POST", "/api/events", m.addEvent)
	r.HTTP("DELETE", "/api/events/{id}", m.deleteEvent)
	r.HTTP("GET", "/api/demo/jobs", m.queue)
	r.HTTP("POST", "/api/demo/jobs", m.enqueue)
	r.HTTP("GET", "/api/demo/jobs/{id}", m.status)
	r.HTTP("POST", "/api/demo/generate", m.generate)
	// Поток порций: ответ нельзя собрать заранее, поэтому маршрут сырой.
	r.HTTPRaw("GET", "/api/demo/stream", http.HandlerFunc(m.stream))

	if err := r.Job(string(kind), jobs.Spec{
		Class:       jobs.ClassBackground,
		Handler:     m.run,
		MaxAttempts: 2,
		Backoff:     time.Second,
		// Предел попытки: обработчик, который ждёт дольше, освобождает
		// разборщик по сроку и получает повтор.
		Timeout: 2 * time.Second,
		// Порог простоя: вид сам говорит, сколько тишины ему нужно.
		MinIdle: m.minIdle,
	}); err != nil {
		return err
	}

	// Ход по слоту: синхронный маршрут и работа в очереди берут одно и то же
	// разрешение, поэтому не выполняются одновременно.
	if err := r.Job(string(generateKind), jobs.Spec{
		Class:       jobs.ClassBackground,
		Handler:     m.generateJob,
		MaxAttempts: 1,
		Timeout:     30 * time.Second,
	}); err != nil {
		return err
	}

	// Событие ставит работу в ту же очередь: срок лежит в хранилище
	// и переживает перезапуск, а когда работу взять, решает расписание.
	if err := r.Job(string(eventKind), jobs.Spec{
		Class:       jobs.ClassBackground,
		Handler:     m.tick,
		MaxAttempts: 1,
	}); err != nil {
		return err
	}
	if err := r.Event(string(eventKind), events.Rule{
		Class:  jobs.ClassBackground,
		Period: m.period,
		Payload: func(due time.Time) any {
			return tick{Due: due.Format(time.RFC3339)}
		},
	}); err != nil {
		return err
	}
	return r.Command("demo", m.command)
}

// snapshot — что модуль объявил, сколько строк приняли слушатели, сколько
// раз сработало отложенное событие и сколько разрешений слота занято.
type snapshot struct {
	TCP     string    `json:"tcp"`
	UDP     string    `json:"udp"`
	Lines   int64     `json:"lines"`
	Job     string    `json:"job"`
	Event   eventView `json:"event"`
	Gate    gateView  `json:"gate"`
	Command string    `json:"command"`
}

// eventView — отложенное событие для клиента: вид, период и число
// срабатываний, которые модуль увидел у себя.
type eventView struct {
	Kind   string `json:"kind"`
	Period string `json:"period"`
	Fired  int64  `json:"fired"`
}

// queueRow — строка таблицы очереди для клиента.
type queueRow struct {
	ID        string     `json:"id"`
	Kind      string     `json:"kind"`
	Class     jobs.Class `json:"class"`
	Status    string     `json:"status"`
	Text      string     `json:"text"`
	Attempts  int        `json:"attempts"`
	NotBefore *time.Time `json:"notBefore"`
	CreatedAt *time.Time `json:"createdAt"`
	StartedAt *time.Time `json:"startedAt"`
}

// task — тело демонстрационной работы. DelaySeconds модуль переводит в срок
// постановки, а SlowMS задаёт ожидание внутри обработчика: так видно, что
// предел попытки освобождает разборщик, не дожидаясь конца работы.
type task struct {
	Text         string `json:"text"`
	FailUntil    int    `json:"failUntil"`
	DelaySeconds int    `json:"delaySeconds"`
	SlowMS       int    `json:"slowMs"`
}

// tick — тело отложенного события: момент, за который оно сработало.
type tick struct {
	Due string `json:"due"`
}

// jobView — состояние работы для клиента. Пустое время начала отдаётся
// как null: работа в очереди ещё не начиналась. Срок постановки нулевой,
// когда работа доступна сразу.
type jobView struct {
	ID        string     `json:"id"`
	Status    string     `json:"status"`
	Text      string     `json:"text"`
	Attempts  int        `json:"attempts"`
	NotBefore *time.Time `json:"notBefore"`
	CreatedAt *time.Time `json:"createdAt"`
	StartedAt *time.Time `json:"startedAt"`
}

// moment — момент для ответа: nil, если он не наступил, чтобы клиент
// отличал «ещё не начиналось» от нулевого времени.
func moment(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// view — состояние работы для клиента: тело разбирается в задачу, времена
// берутся из строки очереди.
func view(job jobs.Job, status string) (jobView, error) {
	var in task
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return jobView{}, fmt.Errorf("тело работы %s: %w", job.ID, err)
	}
	return jobView{
		ID:        job.ID,
		Status:    status,
		Text:      in.Text,
		Attempts:  job.Attempts,
		NotBefore: moment(job.NotBefore),
		CreatedAt: moment(job.CreatedAt),
		StartedAt: moment(job.StartedAt),
	}, nil
}

func (m *module) snapshot(*http.Request) (any, error) {
	return snapshot{
		TCP:     m.tcp.Addr(),
		UDP:     m.udp.Addr(),
		Lines:   m.lines.Load(),
		Job:     string(kind),
		Event:   m.event(),
		Gate:    m.gateView(),
		Command: "demo",
	}, nil
}

// serviceView — состояние службы для клиента: работает ли, готова ли,
// сколько раз перезапускалась и чем закончился последний отказ.
type serviceView struct {
	Name     string `json:"name"`
	Running  bool   `json:"running"`
	Ready    bool   `json:"ready"`
	Restarts int    `json:"restarts"`
	Failure  string `json:"failure"`
}

// services — состав приложения в порядке запуска. Модуль ничего не запускает
// сам: состояние ему передала точка сборки, когда службы уже собраны.
func (m *module) services(*http.Request) (any, error) {
	if m.states == nil {
		return []serviceView{}, nil
	}

	list := m.states()
	rows := make([]serviceView, 0, len(list))
	for _, s := range list {
		rows = append(rows, serviceView{
			Name:     s.Name,
			Running:  s.Running,
			Ready:    s.Ready,
			Restarts: s.Restarts,
			Failure:  s.Failure,
		})
	}
	return rows, nil
}

// crashWatch просит службу упасть на следующем тике: так видно, что паника
// в горутине службы не роняет процесс, а группа её перезапускает.
func (m *module) crashWatch(*http.Request) (any, error) {
	m.watch.crash.Store(true)
	return map[string]string{"planned": "падение на следующем тике"}, nil
}

func (m *module) enqueue(r *http.Request) (any, error) {
	var in task
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "тело задачи: " + err.Error(), Err: err}
	}
	if strings.TrimSpace(in.Text) == "" {
		return nil, &core.Error{
			Status:  http.StatusBadRequest,
			Message: "text обязателен",
			Data:    fieldError{Code: "text_required", Fields: []string{"text"}},
		}
	}
	if in.FailUntil < 0 {
		return nil, &core.Error{
			Status:  http.StatusBadRequest,
			Message: "failUntil не может быть отрицательным",
			Data:    fieldError{Code: "negative", Fields: []string{"failUntil"}},
		}
	}
	if in.DelaySeconds < 0 || in.DelaySeconds > 3600 {
		return nil, &core.Error{
			Status:  http.StatusBadRequest,
			Message: "delaySeconds: целое от 0 до 3600",
			Data:    fieldError{Code: "out_of_range", Fields: []string{"delaySeconds"}},
		}
	}
	if in.SlowMS < 0 || in.SlowMS > 60000 {
		return nil, &core.Error{
			Status:  http.StatusBadRequest,
			Message: "slowMs: целое от 0 до 60000",
			Data:    fieldError{Code: "out_of_range", Fields: []string{"slowMs"}},
		}
	}

	// Срок считает модуль: он знает, к какому моменту нужен результат,
	// а очередь только хранит момент и проверяет его при захвате.
	req := jobs.Request{Kind: kind, Payload: in, Class: jobs.ClassBackground}
	if in.DelaySeconds > 0 {
		req.NotBefore = time.Now().Add(time.Duration(in.DelaySeconds) * time.Second)
	}

	id, err := m.disp.Enqueue(r.Context(), req)
	if err != nil {
		return nil, err
	}

	// Постановка отдаёт только номер, поэтому работу читаем из очереди:
	// так в ответ попадает время создания, которое проставила очередь.
	job, status, ok, err := m.disp.Get(r.Context(), id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("demo: работа %s пропала после постановки", id)
	}
	return view(job, status)
}

// eventRow — строка таблицы событий для клиента.
type eventRow struct {
	ID       string     `json:"id"`
	Kind     string     `json:"kind"`
	Period   string     `json:"period"`
	Next     *time.Time `json:"next"`
	Declared bool       `json:"declared"`
}

// addEventBody — что присылает форма: текст работы и период в секундах.
type addEventBody struct {
	Text    string `json:"text"`
	Seconds int    `json:"seconds"`
}

// eventsList отдаёт события: и объявленные кодом, и добавленные формой.
func (m *module) eventsList(*http.Request) (any, error) {
	if m.events == nil {
		return []eventRow{}, nil
	}

	list, err := m.events.List()
	if err != nil {
		return nil, err
	}

	rows := make([]eventRow, 0, len(list))
	for _, e := range list {
		rows = append(rows, eventRow{
			ID:       e.ID,
			Kind:     string(e.Kind),
			Period:   e.Period.String(),
			Next:     moment(e.Next),
			Declared: e.Declared,
		})
	}
	return rows, nil
}

// addEvent добавляет событие данными: раз в указанный период оно ставит
// работу с этим текстом. Вид работ и класс модуль знает сам, форма их
// не выбирает — иначе пришлось бы объяснять, что такое вид работ.
func (m *module) addEvent(r *http.Request) (any, error) {
	if m.events == nil {
		return nil, &core.Error{Status: http.StatusServiceUnavailable, Message: "служба событий не поднята"}
	}

	var in addEventBody
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "тело события: " + err.Error()}
	}
	if strings.TrimSpace(in.Text) == "" {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "text обязателен"}
	}
	if in.Seconds < 1 || in.Seconds > 3600 {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "seconds: целое от 1 до 3600"}
	}

	id, err := m.events.Add(kind, time.Duration(in.Seconds)*time.Second, jobs.ClassBackground, task{Text: in.Text})
	if err != nil {
		return nil, err
	}
	return map[string]string{"id": id}, nil
}

// Режимы удаления события. По умолчанию убирается только событие: работы,
// которые оно уже поставило, остаются в очереди.
const (
	deleteEventOnly = "event"
	deleteWithJobs  = "event-and-jobs"
)

// deleteEvent убирает событие. Режим приходит параметром mode; удаление
// вместе с работами объявлено, но ещё не сделано — связи между событием
// и поставленными им работами пока нет.
func (m *module) deleteEvent(r *http.Request) (any, error) {
	if m.events == nil {
		return nil, &core.Error{Status: http.StatusServiceUnavailable, Message: "служба событий не поднята"}
	}

	id := r.PathValue("id")
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = deleteEventOnly
	}

	switch mode {
	case deleteEventOnly:
		if err := m.events.Delete(id); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, events.ErrNotFound) {
				status = http.StatusNotFound
			}
			if errors.Is(err, events.ErrDeclared) {
				status = http.StatusConflict
			}
			return nil, &core.Error{Status: status, Message: err.Error()}
		}
		return map[string]string{"deleted": id}, nil

	case deleteWithJobs:
		// Задел на будущую проработку: отказ честнее, чем удалить событие
		// и промолчать о том, что работы остались.
		return nil, &core.Error{
			Status:  http.StatusNotImplemented,
			Message: "удаление вместе с работами пока не сделано; событие не удалено",
		}

	default:
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "mode: event или event-and-jobs"}
	}
}

// queue отдаёт состояние очереди таблицей: новые работы сверху.
func (m *module) queue(r *http.Request) (any, error) {
	entries, err := m.disp.List(r.Context())
	if err != nil {
		return nil, err
	}

	rows := make([]queueRow, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, rowView(e.Job, e.Status))
	}

	// Прямой порядок: старые работы сверху, свежие внизу — так таблица
	// читается как журнал, а окно прокрутки остаётся в конце. Снаружи нужны
	// последние строки: событие добавляет работу каждые десять секунд.
	if len(rows) > queueLimit {
		rows = rows[len(rows)-queueLimit:]
	}
	return rows, nil
}

// rowView собирает строку таблицы. Тело разбирается нестрого: у работы может
// быть любое тело, а в таблицу попадает только текст задачи, если он есть.
func rowView(job jobs.Job, status string) queueRow {
	var in task
	_ = json.Unmarshal(job.Payload, &in)

	return queueRow{
		ID:        job.ID,
		Kind:      string(job.Kind),
		Class:     job.Class,
		Status:    status,
		Text:      in.Text,
		Attempts:  job.Attempts,
		NotBefore: moment(job.NotBefore),
		CreatedAt: moment(job.CreatedAt),
		StartedAt: moment(job.StartedAt),
	}
}

func (m *module) status(r *http.Request) (any, error) {
	id := r.PathValue("id")
	job, status, ok, err := m.disp.Get(r.Context(), id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &core.Error{Status: http.StatusNotFound, Message: fmt.Sprintf("работа %s не найдена", id)}
	}
	return view(job, status)
}

// gateView — слот генерации для клиента: сколько разрешений всего, занято
// сейчас и занимали одновременно.
type gateView struct {
	Capacity int `json:"capacity"`
	Active   int `json:"active"`
	Peak     int `json:"peak"`
}

// gateView собирает состояние слота.
func (m *module) gateView() gateView {
	return gateView{
		Capacity: m.slot.Capacity(),
		Active:   m.slot.Active(),
		Peak:     m.slot.Peak(),
	}
}

// generateBody — что присылает форма: сколько держать слот и нужен ли
// результат сразу или работой в очереди.
type generateBody struct {
	MS     int  `json:"ms"`
	Queued bool `json:"queued"`
}

// generateView — что увидел клиент. Для хода сейчас есть время ожидания
// слота и время работы; для работы в очереди — только номер и статус.
type generateView struct {
	ID       string   `json:"id,omitempty"`
	Status   string   `json:"status,omitempty"`
	WaitedMS int64    `json:"waitedMs"`
	TookMS   int64    `json:"tookMs"`
	Gate     gateView `json:"gate"`
}

// generate — один и тот же ход двумя контурами: синхронно в рамках запроса
// или работой в очереди. Контур выбирает модуль, а не движок: нужен ответ
// сейчас — работает обработчик запроса; ответ понадобится позже — работа.
// Слот у обоих общий, поэтому ход не сталкивается с фоном и не ждёт сам себя.
func (m *module) generate(r *http.Request) (any, error) {
	var in generateBody
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "тело хода: " + err.Error(), Err: err}
	}
	if in.MS < 0 || in.MS > 20000 {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "ms: целое от 0 до 20000"}
	}

	if in.Queued {
		id, err := m.disp.Enqueue(r.Context(), jobs.Request{
			Kind:    generateKind,
			Payload: in,
			Class:   jobs.ClassBackground,
		})
		if err != nil {
			return nil, err
		}
		job, status, ok, err := m.disp.Get(r.Context(), id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("demo: ход %s пропал после постановки", id)
		}
		return generateView{ID: job.ID, Status: status, Gate: m.gateView()}, nil
	}

	started := time.Now()
	release, err := m.slot.Acquire(r.Context())
	if err != nil {
		// Клиент ушёл, не дождавшись слота: ответ писать некому.
		return nil, &core.Error{Status: http.StatusRequestTimeout, Message: "слот не дождался: " + err.Error()}
	}
	defer release()

	// Ожидание слота — признак столкновения: второй ход ждёт первого.
	waited := time.Since(started)
	worked := time.Now()
	if err := wait(r.Context(), time.Duration(in.MS)*time.Millisecond); err != nil {
		return nil, &core.Error{Status: http.StatusRequestTimeout, Message: "ход прерван: " + err.Error()}
	}
	took := time.Since(worked)

	// Освобождаем до ответа, чтобы снимок слота показывал его свободным;
	// повторный вызов из defer ничего не делает.
	release()
	return generateView{
		WaitedMS: waited.Milliseconds(),
		TookMS:   took.Milliseconds(),
		Gate:     m.gateView(),
	}, nil
}

// generateJob — тот же ход, но из очереди: разрешение слота берётся на время
// обработчика, поэтому фон и синхронный ход делят один ресурс.
func (m *module) generateJob(ctx context.Context, job jobs.Job) error {
	var in generateBody
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("тело хода %s: %w", job.ID, err)
	}

	started := time.Now()
	release, err := m.slot.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()

	// Сколько фон ждал свободного слота, видно в журнале: этим и проверяется,
	// что синхронный ход и работа не выполняются одновременно.
	waited := time.Since(started)
	if err := wait(ctx, time.Duration(in.MS)*time.Millisecond); err != nil {
		return err
	}
	m.log("generate", map[string]any{"id": job.ID, "ms": in.MS, "waited_ms": waited.Milliseconds()})
	return nil
}

// wait ждёт срок или отмену контекста. Обработчик обязан смотреть на контекст:
// иначе предел попытки не освободит разборщик.
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
	}
	return nil
}

// fieldError — тело ошибки для формы: код и поля, которые нужно подсветить.
// Клиент показывает их рядом с вводом, а не одним текстом.
type fieldError struct {
	Code   string   `json:"code"`
	Fields []string `json:"fields"`
}

// stream отдаёт порции по мере готовности: обычный обработчик обязан вернуть
// значение целиком, поэтому такой ответ идёт сырым маршрутом. Модуль сам
// пишет формат, заголовки и тела порций; движок считает запрос и ловит панику.
func (m *module) stream(w http.ResponseWriter, r *http.Request) {
	parts := intParam(r, "parts", 5, 1, 20)
	ms := intParam(r, "ms", 300, 0, 5000)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "поток не поддерживается", http.StatusInternalServerError)
		return
	}

	// Формат порций — событие сервера: клиент читает data: и разбирает JSON.
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	for i := 1; i <= parts; i++ {
		if err := wait(r.Context(), time.Duration(ms)*time.Millisecond); err != nil {
			// Клиент закрыл соединение: писать больше некому.
			return
		}
		fmt.Fprintf(w, "data: {\"part\":%d}\n\n", i)
		flusher.Flush()
	}
	fmt.Fprint(w, "data: {\"done\":true}\n\n")
	flusher.Flush()

	m.log("stream", map[string]any{"parts": parts, "ms": ms})
}

// intParam читает целое из строки запроса в заданных границах: пример не
// должен занимать соединение дольше, чем нужно, чтобы увидеть порции.
func intParam(r *http.Request, name string, def, min, max int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if n < min {
		return min
	}
	if n > max {
		return max
	}
	return n
}

func (m *module) echoTCP(_ context.Context, conn net.Conn) error {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		m.lines.Add(1)
		m.log("line", map[string]any{"stream": "tcp", "line": line})
		if _, err := conn.Write([]byte("demo: " + line + "\n")); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// echoUDP отвечает на датаграмму тем же текстом с приставкой.
func (m *module) echoUDP(_ context.Context, pc net.PacketConn, data []byte, from net.Addr) error {
	m.lines.Add(1)
	m.log("line", map[string]any{"stream": "udp", "line": string(data)})

	reply := append([]byte("demo: "), data...)
	_, err := pc.WriteTo(reply, from)
	return err
}

// run — обработчик вида работы. FailUntil задаёт число первых неудачных
// попыток, чтобы повтор был виден снаружи. SlowMS удлиняет работу сверх
// предела попытки: обработчик ждёт, но смотрит на контекст, поэтому
// разборщик освобождается по сроку, а работа уходит в повтор.
func (m *module) run(ctx context.Context, job jobs.Job) error {
	var in task
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("тело работы %s: %w", job.ID, err)
	}
	if job.Attempts <= in.FailUntil {
		return fmt.Errorf("демо: попытка %d нарочно неудачна", job.Attempts)
	}

	if in.SlowMS > 0 {
		if err := wait(ctx, time.Duration(in.SlowMS)*time.Millisecond); err != nil {
			return err
		}
	}

	m.log("job", map[string]any{"id": job.ID, "attempts": job.Attempts})
	return nil
}

// tick — обработчик отложенного события: считает срабатывания и пишет
// в журнал срок, за который событие сработало.
func (m *module) tick(_ context.Context, job jobs.Job) error {
	var in tick
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("тело события %s: %w", job.ID, err)
	}
	m.ticks.Add(1)
	m.log("tick", map[string]any{"id": job.ID, "due": in.Due})
	return nil
}

// event — сведения об отложенном событии для снимка и подкоманды.
func (m *module) event() eventView {
	return eventView{
		Kind:   string(eventKind),
		Period: m.period.String(),
		Fired:  m.ticks.Load(),
	}
}

// command — подкоманда: обычный режим печатает снимок, режим child живёт
// до отмены и используется управляемым процессом.
func (m *module) command(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "child" {
		// Рукопожатие: версия протокола и версия сборки. По второй видно,
		// что родитель и ребёнок — один и тот же бинарник.
		line, err := json.Marshal(map[string]any{
			"version": 1,
			"build":   version.String(),
			"result":  "child",
		})
		if err != nil {
			return err
		}
		fmt.Println(string(line))
		<-ctx.Done()
		return nil
	}

	out := snapshot{
		TCP:     m.tcp.Addr(),
		UDP:     m.udp.Addr(),
		Lines:   m.lines.Load(),
		Job:     string(kind),
		Event:   m.event(),
		Gate:    m.gateView(),
		Command: "demo",
	}
	line, err := json.Marshal(out)
	if err != nil {
		return err
	}
	fmt.Println(string(line))
	return nil
}

// log пишет событие модуля в журнал работы.
func (m *module) log(kind string, fields map[string]any) {
	if m.journal == nil {
		return
	}
	if err := m.journal.Write("demo", kind, fields); err != nil {
		fmt.Printf("demo: журнал: %v\n", err)
	}
}
