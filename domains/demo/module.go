// Package demo — демонстрационный модуль: в одном месте собраны HTTP,
// фоновая работа, слушатели TCP и UDP, управляемый дочерний процесс
// и подкоманда. Это пример возможностей движка, а не предметная область.
package demo

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go-wex/engine/core"
	"go-wex/engine/events"
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/listen"
	"go-wex/engine/process"
)

const (
	// kind — вид работы для ручной постановки; класс фоновый, поэтому
	// она ждёт простоя.
	kind = jobs.Kind("demo.echo")

	// eventKind — вид отложенного события. Работа та же очередь, но срок
	// приходит не из запроса, а от службы событий.
	eventKind = jobs.Kind("demo.tick")

	// tickPeriod — период демонстрационного события: десять секунд, чтобы
	// срабатывание было видно без долгого ожидания.
	tickPeriod = 10 * time.Second

	// queueLimit — сколько последних работ показывать в таблице. Очередь
	// растёт от срабатываний события, и вся она в интерфейсе не нужна.
	queueLimit = 20
)

// module — демонстрационный модуль.
type module struct {
	disp    *jobs.Dispatcher
	journal journal.Writer
	running func() []string
	events  *events.Scheduler

	tcp   *listen.TCP
	udp   *listen.UDP
	lines atomic.Int64
	ticks atomic.Int64
}

// New отдаёт модуль для списка в точке сборки. running называет службы,
// которые подняло приложение: список собирается после регистрации модулей,
// поэтому приходит функцией, а не значением. events добавляет и показывает
// события; в режиме подкоманды его нет.
func New(disp *jobs.Dispatcher, j journal.Writer, running func() []string, ev *events.Scheduler) core.Module {
	if j == nil {
		j = journal.Discard{}
	}
	return &module{disp: disp, journal: j, running: running, events: ev}
}

func (*module) Name() string { return "demo" }

// Register объявляет все возможности: маршруты, работу, службы и подкоманду.
func (m *module) Register(r core.Registry) error {
	m.tcp = listen.NewTCP("127.0.0.1:0", m.echoTCP, m.journal)
	m.udp = listen.NewUDP("127.0.0.1:0", m.echoUDP, m.journal)

	r.Service(m.tcp)
	r.Service(m.udp)
	r.Service(process.New(process.Spec{
		Name:    "demo",
		Args:    []string{"demo", "child"},
		Grace:   time.Second,
		Journal: m.journal,
	}))

	r.HTTP("GET", "/api/demo", m.snapshot)
	r.HTTP("GET", "/api/services", m.services)
	r.HTTP("GET", "/api/events", m.eventsList)
	r.HTTP("POST", "/api/events", m.addEvent)
	r.HTTP("GET", "/api/demo/jobs", m.queue)
	r.HTTP("POST", "/api/demo/jobs", m.enqueue)
	r.HTTP("GET", "/api/demo/jobs/{id}", m.status)

	if err := r.Job(string(kind), jobs.Spec{
		Class:       jobs.ClassBackground,
		Handler:     m.run,
		MaxAttempts: 2,
		Backoff:     time.Second,
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
		Period: tickPeriod,
		Payload: func(due time.Time) any {
			return tick{Due: due.Format(time.RFC3339)}
		},
	}); err != nil {
		return err
	}
	return r.Command("demo", m.command)
}

// snapshot — что модуль объявил, сколько строк приняли слушатели и сколько
// раз сработало отложенное событие.
type snapshot struct {
	TCP     string    `json:"tcp"`
	UDP     string    `json:"udp"`
	Lines   int64     `json:"lines"`
	Job     string    `json:"job"`
	Event   eventView `json:"event"`
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
	CreatedAt *time.Time `json:"createdAt"`
	StartedAt *time.Time `json:"startedAt"`
}

// task — тело демонстрационной работы.
type task struct {
	Text      string `json:"text"`
	FailUntil int    `json:"failUntil"`
}

// tick — тело отложенного события: момент, за который оно сработало.
type tick struct {
	Due string `json:"due"`
}

// jobView — состояние работы для клиента. Пустое время начала отдаётся
// как null: работа в очереди ещё не начиналась.
type jobView struct {
	ID        string     `json:"id"`
	Status    string     `json:"status"`
	Text      string     `json:"text"`
	Attempts  int        `json:"attempts"`
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
		Command: "demo",
	}, nil
}

// services — состав приложения в порядке запуска. Модуль ничего не запускает
// сам: список ему передала точка сборки, когда службы уже собраны.
func (m *module) services(*http.Request) (any, error) {
	if m.running == nil {
		return []string{}, nil
	}
	return m.running(), nil
}

func (m *module) enqueue(r *http.Request) (any, error) {
	var in task
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "тело задачи: " + err.Error()}
	}
	if strings.TrimSpace(in.Text) == "" {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "text обязателен"}
	}
	if in.FailUntil < 0 {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "failUntil не может быть отрицательным"}
	}

	id, err := m.disp.Enqueue(r.Context(), kind, in, jobs.ClassBackground)
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

// echoTCP отвечает на каждую строку и считает принятые строки.
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
// попыток, чтобы повтор был виден снаружи.
func (m *module) run(_ context.Context, job jobs.Job) error {
	var in task
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("тело работы %s: %w", job.ID, err)
	}
	if job.Attempts <= in.FailUntil {
		return fmt.Errorf("демо: попытка %d нарочно неудачна", job.Attempts)
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
		Period: tickPeriod.String(),
		Fired:  m.ticks.Load(),
	}
}

// command — подкоманда: обычный режим печатает снимок, режим child живёт
// до отмены и используется управляемым процессом.
func (m *module) command(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "child" {
		fmt.Println(`{"version":1,"result":"child"}`)
		<-ctx.Done()
		return nil
	}

	out := snapshot{
		TCP:     m.tcp.Addr(),
		UDP:     m.udp.Addr(),
		Lines:   m.lines.Load(),
		Job:     string(kind),
		Event:   m.event(),
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
