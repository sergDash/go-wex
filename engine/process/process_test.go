package process

import (
	"context"
	"testing"
	"time"
)

// event — запись журнала для проверок.
type event struct {
	kind   string
	fields map[string]any
}

// journalFake — журнал для проверок: складывает события в канал.
type journalFake struct {
	events chan event
}

func newJournalFake() *journalFake {
	return &journalFake{events: make(chan event, 64)}
}

func (j *journalFake) Write(_, kind string, fields map[string]any) error {
	select {
	case j.events <- event{kind: kind, fields: fields}:
	default:
	}
	return nil
}

// wait ждёт событие нужного вида, пропуская остальные.
func (j *journalFake) wait(t *testing.T, kind string) event {
	t.Helper()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-j.events:
			if e.kind == kind {
				return e
			}
		case <-deadline:
			t.Fatalf("нет события %q", kind)
			return event{}
		}
	}
}

// TestRegistry — подкоманды добавляются, ищутся и не повторяются.
func TestRegistry(t *testing.T) {
	r := NewRegistry()
	h := func(context.Context, []string) error { return nil }

	if err := r.Add("b", h); err != nil {
		t.Fatalf("Add b: %v", err)
	}
	if err := r.Add("a", h); err != nil {
		t.Fatalf("Add a: %v", err)
	}
	if got := r.Names(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("имена: %v", got)
	}
	if _, ok := r.Lookup("a"); !ok {
		t.Fatal("подкоманда a не найдена")
	}
	if _, ok := r.Lookup("нет"); ok {
		t.Fatal("нашлась несуществующая подкоманда")
	}
	if err := r.Add("a", h); err == nil {
		t.Fatal("повторная подкоманда принята")
	}
	if err := r.Add("", h); err == nil {
		t.Fatal("пустое имя принято")
	}
	if err := r.Add("c", nil); err == nil {
		t.Fatal("пустой обработчик принят")
	}
}

// TestServiceExit — код возврата и вывод процесса попадают в журнал,
// а без правила перезапуска отказ отмечается отдельно.
func TestServiceExit(t *testing.T) {
	j := newJournalFake()
	s := New(Spec{
		Name:    "child",
		Path:    "/bin/sh",
		Args:    []string{"-c", "echo привет; exit 3"},
		Journal: j,
	})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	out := j.wait(t, "output")
	if out.fields["line"] != "привет" {
		t.Fatalf("вывод: %+v", out.fields)
	}
	exit := j.wait(t, "exit")
	if exit.fields["code"] != 3 {
		t.Fatalf("код возврата: %+v", exit.fields)
	}
	fail := j.wait(t, "failed")
	if fail.fields["code"] != 3 {
		t.Fatalf("отказ: %+v", fail.fields)
	}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestServiceExitZero — успешный выход без перезапуска отмечается как
// остановка, а не как отказ.
func TestServiceExitZero(t *testing.T) {
	j := newJournalFake()
	s := New(Spec{
		Name:    "child",
		Path:    "/bin/sh",
		Args:    []string{"-c", "exit 0"},
		Journal: j,
	})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	j.wait(t, "stopped")
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestServiceRestart — при заданном сроке процесс запускается снова после
// выхода.
func TestServiceRestart(t *testing.T) {
	j := newJournalFake()
	s := New(Spec{
		Name:    "child",
		Path:    "/bin/sh",
		Args:    []string{"-c", "exit 0"},
		Restart: 10 * time.Millisecond,
		Journal: j,
	})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	j.wait(t, "exit")
	j.wait(t, "exit")

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// TestServiceStopKills — процесс, игнорирующий мягкий сигнал, получает
// принудительный после срока.
func TestServiceStopKills(t *testing.T) {
	j := newJournalFake()
	s := New(Spec{
		Name:    "child",
		Path:    "/bin/sh",
		Args:    []string{"-c", `echo ready; trap "" TERM; while true; do sleep 1; done`},
		Grace:   100 * time.Millisecond,
		Journal: j,
	})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	j.wait(t, "output") // ждём, пока оболочка установит обработчик TERM

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("принудительный сигнал пришёл раньше срока")
	}
}

// TestServiceBadSpec — без имени процесс не запускается.
func TestServiceBadSpec(t *testing.T) {
	s := New(Spec{Path: "/bin/sh"})
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("пустое имя принято")
	}
}
