// Package service — внутренние службы движка: долгоживущие части процесса
// с запуском и остановкой. Движок стоит на них: HTTP-сервер, разборщик
// очереди, слушатели и управляемые процессы оформляются как службы.
package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"go-wex/engine/journal"
)

// stopTimeout — сколько даётся всем службам на остановку вместе.
const stopTimeout = 5 * time.Second

// Service — внутренняя служба: запускается, работает и останавливается.
// Start возвращается, когда служба готова принимать работу; Stop дожидается
// завершения её горутин.
type Service interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Failable — служба, которая сообщает о конце своей работы. Канал получает
// причину и закрывается: nil — служба завершилась сама, ошибка — отказ,
// в том числе паника. После каждого запуска служба отдаёт новый канал,
// иначе группа не заметит второй отказ.
type Failable interface {
	Done() <-chan error
}

// Ready — служба, которая знает, готова ли принимать работу. Запуск и
// готовность — разные вещи: порт открыт, а модель ещё загружается.
type Ready interface {
	Ready() bool
}

// Pausable — служба, которая умеет приостановить работу и продолжить её.
// Пауза — необязательная возможность: HTTP-серверу она не нужна, разборщику
// и управляемому процессу нужна. Пауза не освобождает порты и файлы — для
// этого есть Stop; она прекращает брать новую работу, чтобы снизить нагрузку
// или разобрать состояние при отладке.
type Pausable interface {
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
}

// PanicError — паника в горутине службы: значение и стек на момент паники.
// Отдельный тип, чтобы группа записала стек в журнал, а не только текст.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("паника в службе: %v", e.Value)
}

// Guard — граница горутины службы: паника превращается в ошибку с отпечатком
// стека. Служба, которая запускает свою горутину, пропускает её через Guard:
// без этого одна паника завершает процесс.
func Guard(run func() error) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	return run()
}

// State — состояние службы для снаружи: работает ли, готова ли, сколько раз
// перезапускалась и чем закончился последний отказ.
type State struct {
	Name     string
	Running  bool
	Ready    bool
	Restarts int
	Failure  string
}

// Group — набор служб: запуск в порядке списка, остановка в обратном.
//
// Группа следит за службами, которые сообщают о конце своей работы
// (Failable): отказ записывается в журнал, а по заданному правилу служба
// останавливается и запускается заново. Служба без такого канала
// не наблюдается: группа не может узнать, что её горутина умерла.
type Group struct {
	services []Service

	mu      sync.Mutex
	states  map[string]*state
	journal journal.Writer
	delay   time.Duration
	max     int
}

// state — изменяемая часть состояния службы. Готовность спрашивается
// у самой службы, поэтому здесь не хранится.
type state struct {
	running  bool
	restarts int
	failure  string
}

// failure — сообщение о конце работы службы.
type failure struct {
	svc Service
	err error
}

// New собирает группу служб в порядке запуска.
func New(services ...Service) *Group {
	g := &Group{
		services: services,
		states:   make(map[string]*state, len(services)),
		journal:  journal.Discard{},
	}
	for _, s := range services {
		g.states[s.Name()] = &state{}
	}
	return g
}

// SetJournal подключает журнал работы: в него пишутся отказы, паники
// и перезапуски служб.
func (g *Group) SetJournal(j journal.Writer) {
	if j != nil {
		g.journal = j
	}
}

// SetRestart задаёт правило перезапуска отказавших служб: сколько ждать
// перед повтором и сколько раз пробовать. Ноль означает, что группа только
// сообщает об отказе: процесс продолжает работать без этой службы.
func (g *Group) SetRestart(delay time.Duration, attempts int) {
	g.mu.Lock()
	g.delay = delay
	g.max = attempts
	g.mu.Unlock()
}

// Run запускает службы, ждёт отмены контекста и останавливает их.
// Если служба не запустилась, уже запущенные останавливаются, а ошибка
// возвращается вместе с ошибками остановки.
//
// Отказ уже запущенной службы не прерывает работу остальных: он записывается
// в журнал, а служба перезапускается, если задано правило.
func (g *Group) Run(ctx context.Context) error {
	failures := make(chan failure, len(g.services))

	started := make([]Service, 0, len(g.services))
	for _, s := range g.services {
		if err := g.start(ctx, s); err != nil {
			return errors.Join(
				fmt.Errorf("служба %s: %w", s.Name(), err),
				g.stop(started),
			)
		}
		started = append(started, s)
		g.watch(ctx, s, failures)
	}

	for {
		select {
		case <-ctx.Done():
			return g.stop(started)
		case f := <-failures:
			g.fail(f)
			if err := g.restart(ctx, f, failures); err != nil {
				log.Printf("служба %s: %v", f.svc.Name(), err)
				g.writeJournal("failed", map[string]any{
					"service": f.svc.Name(),
					"error":   err.Error(),
				})
			}
		}
	}
}

// start запускает службу и отмечает её состояние.
func (g *Group) start(ctx context.Context, s Service) error {
	if err := s.Start(ctx); err != nil {
		g.setRunning(s.Name(), false)
		return err
	}
	g.setRunning(s.Name(), true)
	return nil
}

// watch ставит наблюдение за концом работы службы. Канал итога новый после
// каждого запуска, поэтому наблюдение ставится и после перезапуска.
func (g *Group) watch(ctx context.Context, s Service, out chan<- failure) {
	f, ok := s.(Failable)
	if !ok {
		return
	}
	done := f.Done()
	if done == nil {
		return
	}

	go func() {
		select {
		case <-ctx.Done():
		case err := <-done:
			out <- failure{svc: s, err: err}
		}
	}()
}

// fail запоминает отказ службы и пишет его в журнал. Паника получает
// отдельное событие со стеком: по нему видно, где именно упало.
func (g *Group) fail(f failure) {
	reason := "служба завершилась"
	if f.err != nil {
		reason = f.err.Error()
	}

	g.mu.Lock()
	st := g.stateOf(f.svc.Name())
	st.running = false
	st.failure = reason
	g.mu.Unlock()

	fields := map[string]any{"service": f.svc.Name(), "error": reason}

	var panicErr *PanicError
	if errors.As(f.err, &panicErr) {
		fields["stack"] = string(panicErr.Stack)
		g.writeJournal("panic", fields)
		return
	}
	g.writeJournal("failed", fields)
}

// restart возвращает отказавшую службу в работу по правилу: ждёт заданный
// срок, останавливает остатки и запускает заново. Ошибка означает, что
// служба осталась отказавшей, а процесс работает без неё.
func (g *Group) restart(ctx context.Context, f failure, failures chan<- failure) error {
	name := f.svc.Name()

	if err := g.restartBlocked(name); err != nil {
		return err
	}

	delay := g.restartDelay()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if err := f.svc.Stop(stopCtx); err != nil {
		return fmt.Errorf("остановка перед перезапуском: %w", err)
	}

	if err := g.start(ctx, f.svc); err != nil {
		return fmt.Errorf("повторный запуск: %w", err)
	}
	g.watch(ctx, f.svc, failures)

	g.mu.Lock()
	st := g.stateOf(name)
	st.restarts++
	restarts := st.restarts
	g.mu.Unlock()

	g.writeJournal("restart", map[string]any{"service": name, "restarts": restarts})
	return nil
}

// restartBlocked объясняет, почему службу не перезапускают: правило
// не задано или предел попыток исчерпан.
func (g *Group) restartBlocked(name string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.max <= 0 {
		return errors.New("перезапуск не задан настройкой")
	}
	if g.stateOf(name).restarts >= g.max {
		return fmt.Errorf("предел перезапусков исчерпан: %d", g.max)
	}
	return nil
}

// restartDelay отдаёт срок перед перезапуском.
func (g *Group) restartDelay() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.delay
}

// setRunning отмечает, работает ли служба.
func (g *Group) setRunning(name string, running bool) {
	g.mu.Lock()
	g.stateOf(name).running = running
	g.mu.Unlock()
}

// stateOf отдаёт состояние службы; вызывается под мьютексом.
func (g *Group) stateOf(name string) *state {
	st, ok := g.states[name]
	if !ok {
		st = &state{}
		g.states[name] = st
	}
	return st
}

// States отдаёт состояние служб в порядке запуска: по нему снаружи видно,
// что работает, что отказало и сколько было перезапусков.
func (g *Group) States() []State {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := make([]State, 0, len(g.services))
	for _, s := range g.services {
		st := g.stateOf(s.Name())
		out = append(out, State{
			Name:     s.Name(),
			Running:  st.running,
			Ready:    st.running && readyOf(s),
			Restarts: st.restarts,
			Failure:  st.failure,
		})
	}
	return out
}

// Ready сообщает, готовы ли все службы. Служба, которая не работает,
// не готова: остатки отказавшего процесса работу не принимают. Служба без
// Ready считается готовой, как только запущена.
func (g *Group) Ready() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, s := range g.services {
		if !g.stateOf(s.Name()).running || !readyOf(s) {
			return false
		}
	}
	return true
}

// readyOf спрашивает готовность у службы; отсутствие ответа считается
// готовностью.
func readyOf(s Service) bool {
	r, ok := s.(Ready)
	if !ok {
		return true
	}
	return r.Ready()
}

// stop останавливает службы в обратном порядке с общим сроком.
func (g *Group) stop(started []Service) error {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()

	var errs []error
	for i := len(started) - 1; i >= 0; i-- {
		name := started[i].Name()
		if err := started[i].Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("служба %s: %w", name, err))
		}
		g.setRunning(name, false)
	}
	return errors.Join(errs...)
}

// writeJournal пишет событие службы и не мешает работе, если журнал недоступен.
func (g *Group) writeJournal(kind string, fields map[string]any) {
	if g.journal == nil {
		return
	}
	if err := g.journal.Write("service", kind, fields); err != nil {
		log.Printf("журнал: %v", err)
	}
}

// Pause приостанавливает службы в обратном порядке запуска. Службы без
// Pausable пропускаются: пауза — необязательная возможность. Ошибка одной
// службы не мешает остальным, но возвращается вместе с остальными.
func (g *Group) Pause(ctx context.Context) error {
	var errs []error
	for i := len(g.services) - 1; i >= 0; i-- {
		p, ok := g.services[i].(Pausable)
		if !ok {
			continue
		}
		if err := p.Pause(ctx); err != nil {
			errs = append(errs, fmt.Errorf("служба %s: %w", g.services[i].Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Resume продолжает службы в порядке запуска.
func (g *Group) Resume(ctx context.Context) error {
	var errs []error
	for _, s := range g.services {
		p, ok := s.(Pausable)
		if !ok {
			continue
		}
		if err := p.Resume(ctx); err != nil {
			errs = append(errs, fmt.Errorf("служба %s: %w", s.Name(), err))
		}
	}
	return errors.Join(errs...)
}
