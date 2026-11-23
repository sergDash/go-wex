package service

import (
	"context"
	"strings"
	"testing"
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
