package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fake — служба для проверки порядка запуска и остановки.
type fake struct {
	name     string
	log      *[]string
	startErr error
}

func (f *fake) Name() string { return f.name }

func (f *fake) Start(context.Context) error {
	*f.log = append(*f.log, "start "+f.name)
	return f.startErr
}

func (f *fake) Stop(context.Context) error {
	*f.log = append(*f.log, "stop "+f.name)
	return nil
}

// TestRunOrder — службы запускаются в порядке списка, останавливаются
// в обратном. Контекст отменён заранее, поэтому тест не ждёт.
func TestRunOrder(t *testing.T) {
	var log []string
	g := New(
		&fake{name: "a", log: &log},
		&fake{name: "b", log: &log},
		&fake{name: "c", log: &log},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []string{"start a", "start b", "start c", "stop c", "stop b", "stop a"}
	if strings.Join(log, ",") != strings.Join(want, ",") {
		t.Fatalf("порядок: получили %v, ждали %v", log, want)
	}
}

// TestRunStartFailure — при отказе запуска уже запущенные службы
// останавливаются, ошибка называет виновника.
func TestRunStartFailure(t *testing.T) {
	var log []string
	g := New(
		&fake{name: "a", log: &log},
		&fake{name: "b", log: &log, startErr: context.Canceled},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := g.Run(ctx)
	if err == nil {
		t.Fatal("Run: ждали ошибку запуска")
	}
	if !strings.Contains(err.Error(), "служба b") {
		t.Fatalf("ошибка не называет службу: %v", err)
	}

	want := []string{"start a", "start b", "stop a"}
	if strings.Join(log, ",") != strings.Join(want, ",") {
		t.Fatalf("порядок: получили %v, ждали %v", log, want)
	}
}

// pausable — служба, которая записывает паузу и продолжение.
type pausable struct {
	name string
	log  *[]string
}

func (p *pausable) Name() string                { return p.name }
func (p *pausable) Start(context.Context) error { return nil }
func (p *pausable) Stop(context.Context) error  { return nil }

func (p *pausable) Pause(context.Context) error {
	*p.log = append(*p.log, "pause "+p.name)
	return nil
}

func (p *pausable) Resume(context.Context) error {
	*p.log = append(*p.log, "resume "+p.name)
	return nil
}

// TestPauseResume — пауза идёт в обратном порядке запуска, продолжение
// в прямом; службы без Pausable пропускаются.
func TestPauseResume(t *testing.T) {
	var log []string
	g := New(
		&pausable{name: "a", log: &log},
		&fake{name: "plain", log: &log},
		&pausable{name: "c", log: &log},
	)

	ctx := context.Background()
	if err := g.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := g.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	want := []string{"pause c", "pause a", "resume a", "resume c"}
	if strings.Join(log, ",") != strings.Join(want, ",") {
		t.Fatalf("порядок: получили %v, ждали %v", log, want)
	}
}

// failing — служба с каналом итога: первый запуск падает, следующие
// работают до отмены. Так проверяется и граница паники, и перезапуск.
type failing struct {
	name string

	mu     sync.Mutex
	runs   int
	done   chan error
	cancel context.CancelFunc
}

func (f *failing) Name() string { return f.name }

func (f *failing) Done() <-chan error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.done
}

func (f *failing) Start(ctx context.Context) error {
	f.mu.Lock()
	f.runs++
	first := f.runs == 1
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	f.cancel = cancel
	f.done = done
	f.mu.Unlock()

	go func() {
		defer close(done)
		done <- Guard(func() error {
			if first {
				panic("сломано")
			}
			<-runCtx.Done()
			return nil
		})
	}()
	return nil
}

func (f *failing) Stop(ctx context.Context) error {
	f.mu.Lock()
	cancel, done := f.cancel, f.done
	f.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestGuard — паника в горутине службы становится ошибкой со стеком.
func TestGuard(t *testing.T) {
	err := Guard(func() error { panic("сломано") })
	if err == nil {
		t.Fatal("паника не превратилась в ошибку")
	}

	var panicErr *PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("тип ошибки: %T", err)
	}
	if len(panicErr.Stack) == 0 || panicErr.Value == nil {
		t.Fatalf("паника без стека: %+v", panicErr)
	}

	if err := Guard(func() error { return nil }); err != nil {
		t.Fatalf("обычная работа: %v", err)
	}
}

// TestGroupRestarts — отказавшая служба перезапускается по правилу,
// процесс продолжает работу, а счётчик перезапусков виден снаружи.
func TestGroupRestarts(t *testing.T) {
	svc := &failing{name: "model"}
	g := New(svc)
	g.SetRestart(0, 2)

	j := &journalFake{}
	g.SetJournal(j)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		st := g.States()[0]
		if st.Restarts == 1 && st.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("служба не перезапустилась: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !strings.Contains(strings.Join(j.kinds, ","), "panic") || !strings.Contains(strings.Join(j.kinds, ","), "restart") {
		t.Fatalf("журнал: %v", j.kinds)
	}
}

// TestGroupReportsFailure — без правила перезапуска служба остаётся
// отказавшей, причина видна снаружи, остальные службы работают.
func TestGroupReportsFailure(t *testing.T) {
	svc := &failing{name: "model"}
	g := New(svc)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		st := g.States()[0]
		if !st.Running && st.Failure != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("отказ не отмечен: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}

	select {
	case err := <-done:
		t.Fatalf("Run завершился при отказе службы: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// notReady — служба, которая сама решает, готова ли она.
type notReady struct{ ready bool }

func (s *notReady) Name() string                { return "slow" }
func (s *notReady) Start(context.Context) error { return nil }
func (s *notReady) Stop(context.Context) error  { return nil }
func (s *notReady) Ready() bool                 { return s.ready }

// TestGroupReady — готовность группы зависит от служб, которые умеют
// отвечать на этот вопрос; отсутствие ответа считается готовностью, а
// остановленная служба не готова никогда.
func TestGroupReady(t *testing.T) {
	slow := &notReady{}
	g := New(&fake{name: "plain", log: &[]string{}}, slow)

	if g.Ready() {
		t.Fatal("до запуска группа уже готова")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()

	waitRunning(t, g, "slow")
	if g.Ready() {
		t.Fatal("неготовая служба не учтена")
	}

	slow.ready = true
	if !g.Ready() {
		t.Fatal("готовая служба считается неготовой")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// waitRunning ждёт, пока служба отметится запущенной: группа стартует её
// в своей горутине.
func waitRunning(t *testing.T, g *Group, name string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, st := range g.States() {
			if st.Name == name && st.Running {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("служба %s не запустилась", name)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// journalFake — журнал для проверки: запоминает источники и виды.
type journalFake struct {
	kinds []string
}

func (j *journalFake) Write(_, kind string, _ map[string]any) error {
	j.kinds = append(j.kinds, kind)
	return nil
}
