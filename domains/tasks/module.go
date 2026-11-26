// Package tasks — рабочий пример модуля: HTTP-маршруты ставят задачу
// в очередь, фоновая работа её выполняет, состояние видно отдельным запросом.
// Модуль показывает оба пути запроса: короткий ответ сразу и работу позже.
package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go-wex/engine/config"
	"go-wex/engine/core"
	"go-wex/engine/jobs"
)

// kind — вид работы; по нему разборщик выбирает обработчик.
const kind = jobs.Kind("task.run")

// module — модуль задач: держит разборщик, через который ставит и смотрит
// работы. Своего хранилища и служб у модуля нет.
type module struct {
	disp *jobs.Dispatcher
}

// New отдаёт модуль для списка в точке сборки. settings — раздел
// modules.settings.tasks: своих настроек у модуля нет, поэтому любой ключ
// в нём — ошибка.
func New(disp *jobs.Dispatcher, settings json.RawMessage) (core.Module, error) {
	var none struct{}
	if err := config.DecodeSection(settings, &none); err != nil {
		return nil, fmt.Errorf("tasks: настройки: %w", err)
	}
	return module{disp: disp}, nil
}

func (module) Name() string { return "tasks" }

// Register объявляет маршруты, вид работы и подкоманду. Обработчик работы
// остаётся в модуле: разборщик лишь вызывает его по виду.
func (m module) Register(r core.Registry) error {
	r.HTTP("POST", "/api/tasks", m.enqueue)
	r.HTTP("GET", "/api/tasks/{id}", m.status)
	r.HTTP("POST", "/api/tasks/pause", m.pause)
	r.HTTP("POST", "/api/tasks/resume", m.resume)

	if err := r.Job(string(kind), jobs.Spec{
		Class:       jobs.ClassInteractive,
		Handler:     m.run,
		MaxAttempts: 3,
		Backoff:     time.Second,
	}); err != nil {
		return err
	}
	return r.Command("task", m.command)
}

// task — тело задачи: текст, класс, сколько первых попыток провалить нарочно
// и задержка перед результатом. FailUntil и DelayMS — для проверки повторов
// и состояния running, в обычной задаче они нулевые.
type task struct {
	Text      string     `json:"text"`
	Class     jobs.Class `json:"class"`
	FailUntil int        `json:"failUntil"`
	DelayMS   int        `json:"delayMs"`
}

// taskView — то, что видит клиент: номер, класс, статус, текст, попытка
// и времена. Пустое время начала отдаётся как null.
type taskView struct {
	ID        string     `json:"id"`
	Class     jobs.Class `json:"class"`
	Status    string     `json:"status"`
	Text      string     `json:"text"`
	Attempts  int        `json:"attempts"`
	CreatedAt *time.Time `json:"createdAt"`
	StartedAt *time.Time `json:"startedAt"`
}

// moment — момент для ответа: nil, если он не наступил.
func moment(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// view — состояние задачи для клиента: тело разбирается в задачу, времена
// берутся из строки очереди.
func view(job jobs.Job, status string) (taskView, error) {
	var in task
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return taskView{}, fmt.Errorf("тело задачи %s: %w", job.ID, err)
	}
	return taskView{
		ID:        job.ID,
		Class:     job.Class,
		Status:    status,
		Text:      in.Text,
		Attempts:  job.Attempts,
		CreatedAt: moment(job.CreatedAt),
		StartedAt: moment(job.StartedAt),
	}, nil
}

// pauseView — состояние паузы разборщика.
type pauseView struct {
	Paused bool `json:"paused"`
}

// enqueue принимает задачу и сразу отдаёт номер. Выполнение начинается
// в горутине разборщика, а не в этом запросе.
func (m module) enqueue(r *http.Request) (any, error) {
	var in task
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "тело задачи: " + err.Error()}
	}
	if strings.TrimSpace(in.Text) == "" {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "text обязателен"}
	}
	if in.FailUntil < 0 || in.DelayMS < 0 {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "failUntil и delayMs не могут быть отрицательными"}
	}
	if in.Class != "" && !in.Class.Valid() {
		return nil, &core.Error{Status: http.StatusBadRequest, Message: "class: interactive или background"}
	}

	id, err := m.disp.Enqueue(r.Context(), jobs.Request{
		Kind:    kind,
		Payload: in,
		Class:   in.Class,
	})
	if err != nil {
		return nil, err
	}

	// Постановка отдаёт только номер, поэтому задачу читаем из очереди:
	// так в ответ попадают класс и время создания, которые проставила очередь.
	job, status, ok, err := m.disp.Get(r.Context(), id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("задача %s пропала после постановки", id)
	}
	return view(job, status)
}

// status отдаёт работу и её статус по номеру. Очередь лежит в хранилище,
// поэтому после перезапуска номер находится; 404 — для несуществующего.
func (m module) status(r *http.Request) (any, error) {
	id := r.PathValue("id")
	job, status, ok, err := m.disp.Get(r.Context(), id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &core.Error{Status: http.StatusNotFound, Message: fmt.Sprintf("задача %s не найдена", id)}
	}
	return view(job, status)
}

// pause останавливает захват новых работ: начатая доигрывается.
func (m module) pause(r *http.Request) (any, error) {
	if err := m.disp.Pause(r.Context()); err != nil {
		return nil, err
	}
	return pauseView{Paused: true}, nil
}

// resume возвращает разборщик к работе и будит его подсказкой.
func (m module) resume(r *http.Request) (any, error) {
	if err := m.disp.Resume(r.Context()); err != nil {
		return nil, err
	}
	return pauseView{Paused: false}, nil
}

// command — подкоманда того же бинарника: выполняет одну задачу без служб
// и печатает строку протокола с версией и результатом.
func (m module) command(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("task", flag.ContinueOnError)
	text := fs.String("text", "", "текст задачи")
	class := fs.String("class", string(jobs.ClassInteractive), "класс работы")
	failUntil := fs.Int("failUntil", 0, "сколько первых попыток провалить")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*text) == "" {
		return errors.New("text обязателен")
	}
	if *failUntil < 0 {
		return errors.New("failUntil не может быть отрицательным")
	}

	c := jobs.Class(*class)
	if !c.Valid() {
		return errors.New("class: interactive или background")
	}

	payload, err := json.Marshal(task{Text: *text, Class: c, FailUntil: *failUntil})
	if err != nil {
		return err
	}
	job := jobs.Job{ID: "command", Kind: kind, Class: c, Payload: payload, Attempts: 1}
	if err := m.run(ctx, job); err != nil {
		return err
	}

	out := struct {
		Version int    `json:"version"`
		Result  string `json:"result"`
	}{Version: 1, Result: *text}
	line, err := json.Marshal(out)
	if err != nil {
		return err
	}
	fmt.Println(string(line))
	return nil
}

// run — обработчик вида работы. Сначала ждёт задержку, затем проверяет,
// не пора ли выполнить задачу по-настоящему: FailUntil задаёт число первых
// неудачных попыток, чтобы повтор был виден снаружи.
func (m module) run(ctx context.Context, job jobs.Job) error {
	var in task
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return fmt.Errorf("тело задачи %s: %w", job.ID, err)
	}

	if err := wait(ctx, in.DelayMS); err != nil {
		return err
	}
	if job.Attempts <= in.FailUntil {
		return fmt.Errorf("задача %s: попытка %d нарочно неудачна", job.ID, job.Attempts)
	}
	return nil
}

// wait ждёт задержку или отмену. Таймер, а не сон: остановка ядра
// прекращает ожидание сразу.
func wait(ctx context.Context, ms int) error {
	if ms <= 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
