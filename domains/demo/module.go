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
	"go-wex/engine/jobs"
	"go-wex/engine/journal"
	"go-wex/engine/listen"
	"go-wex/engine/process"
)

// kind — вид работы; класс фоновый, поэтому она ждёт простоя.
const kind = jobs.Kind("demo.echo")

// module — демонстрационный модуль.
type module struct {
	disp    *jobs.Dispatcher
	journal journal.Writer

	tcp   *listen.TCP
	udp   *listen.UDP
	lines atomic.Int64
}

// New отдаёт модуль для списка в точке сборки.
func New(disp *jobs.Dispatcher, j journal.Writer) core.Module {
	if j == nil {
		j = journal.Discard{}
	}
	return &module{disp: disp, journal: j}
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
	return r.Command("demo", m.command)
}

// snapshot — что модуль объявил и сколько строк приняли слушатели.
type snapshot struct {
	TCP     string `json:"tcp"`
	UDP     string `json:"udp"`
	Lines   int64  `json:"lines"`
	Job     string `json:"job"`
	Command string `json:"command"`
}

// task — тело демонстрационной работы.
type task struct {
	Text      string `json:"text"`
	FailUntil int    `json:"failUntil"`
}

// jobView — состояние работы для клиента.
type jobView struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Text     string `json:"text"`
	Attempts int    `json:"attempts"`
}

func (m *module) snapshot(*http.Request) (any, error) {
	return snapshot{
		TCP:     m.tcp.Addr(),
		UDP:     m.udp.Addr(),
		Lines:   m.lines.Load(),
		Job:     string(kind),
		Command: "demo",
	}, nil
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
	return jobView{ID: id, Status: jobs.StatusQueued, Text: in.Text}, nil
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

	var in task
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return nil, fmt.Errorf("тело работы %s: %w", id, err)
	}
	return jobView{ID: job.ID, Status: status, Text: in.Text, Attempts: job.Attempts}, nil
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
