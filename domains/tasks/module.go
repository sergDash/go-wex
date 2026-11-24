// Package tasks — рабочий пример модуля: HTTP-маршруты ставят задачу
// в очередь, фоновая работа её выполняет, состояние видно отдельным запросом.
// Модуль показывает оба пути запроса: короткий ответ сразу и работу позже.
package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

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

// New отдаёт модуль для списка в точке сборки.
func New(disp *jobs.Dispatcher) core.Module { return module{disp: disp} }

func (module) Name() string { return "tasks" }

// Register объявляет маршруты и вид работы. Обработчик остаётся в модуле:
// разборщик лишь вызывает его по виду.
func (m module) Register(r core.Registry) error {
	r.HTTP("POST", "/api/tasks", m.enqueue)
	r.HTTP("GET", "/api/tasks/{id}", m.status)
	r.HTTP("POST", "/api/tasks/pause", m.pause)
	r.HTTP("POST", "/api/tasks/resume", m.resume)

	return r.Job(string(kind), jobs.Spec{
		Handler:     m.run,
		MaxAttempts: 3,
		Backoff:     time.Second,
	})
}

// task — тело задачи: текст, сколько первых попыток провалить нарочно и
// задержка перед результатом. FailUntil и DelayMS — для проверки повторов
// и состояния running, в обычной задаче они нулевые.
type task struct {
	Text      string `json:"text"`
	FailUntil int    `json:"failUntil"`
	DelayMS   int    `json:"delayMs"`
}

// taskView — то, что видит клиент: номер, статус, текст и номер попытки.
type taskView struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Text     string `json:"text"`
	Attempts int    `json:"attempts"`
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

	id, err := m.disp.Enqueue(r.Context(), kind, in)
	if err != nil {
		return nil, err
	}
	return taskView{ID: id, Status: jobs.StatusQueued, Text: in.Text}, nil
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

	var in task
	if err := json.Unmarshal(job.Payload, &in); err != nil {
		return nil, fmt.Errorf("тело задачи %s: %w", id, err)
	}
	return taskView{ID: job.ID, Status: status, Text: in.Text, Attempts: job.Attempts}, nil
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
