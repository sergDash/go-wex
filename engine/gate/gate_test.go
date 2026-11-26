package gate

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestAcquireAndRelease — разрешение берётся, освобождается и снова свободно.
func TestAcquireAndRelease(t *testing.T) {
	g := New(1)
	ctx := context.Background()

	if g.Capacity() != 1 {
		t.Fatalf("разрешений: %d", g.Capacity())
	}

	release, err := g.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if g.Active() != 1 || g.Peak() != 1 {
		t.Fatalf("занято: active=%d peak=%d", g.Active(), g.Peak())
	}

	release()
	if g.Active() != 0 {
		t.Fatalf("после освобождения: active=%d", g.Active())
	}

	// Повторное освобождение не возвращает чужое место.
	release()
	if g.Active() != 0 {
		t.Fatalf("повторное освобождение: active=%d", g.Active())
	}
}

// TestWaitsForFreeSlot — при одном разрешении второй ждёт освобождения,
// а одновременных держателей не бывает больше одного.
func TestWaitsForFreeSlot(t *testing.T) {
	g := New(1)
	ctx := context.Background()

	release, err := g.Acquire(ctx)
	if err != nil {
		t.Fatalf("первый Acquire: %v", err)
	}

	got := make(chan struct{})
	go func() {
		second, err := g.Acquire(ctx)
		if err != nil {
			return
		}
		got <- struct{}{}
		second()
	}()

	select {
	case <-got:
		t.Fatal("второй взял занятое разрешение")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("второй не дождался освобождения")
	}

	if g.Peak() != 1 {
		t.Fatalf("одновременных держателей: %d", g.Peak())
	}
}

// TestCancelledWhileWaiting — отмена контекста освобождает ожидание и не
// занимает разрешение.
func TestCancelledWhileWaiting(t *testing.T) {
	g := New(1)
	release, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидание: %v", err)
	}
	if g.Active() != 1 {
		t.Fatalf("отменённое ожидание заняло место: active=%d", g.Active())
	}
}

// TestCapacity — два разрешения дают двум держателям работать одновременно.
func TestCapacity(t *testing.T) {
	g := New(2)
	ctx := context.Background()

	first, err := g.Acquire(ctx)
	if err != nil {
		t.Fatalf("первый Acquire: %v", err)
	}
	second, err := g.Acquire(ctx)
	if err != nil {
		t.Fatalf("второй Acquire: %v", err)
	}
	if g.Active() != 2 || g.Peak() != 2 {
		t.Fatalf("занято: active=%d peak=%d", g.Active(), g.Peak())
	}

	first()
	second()
	if g.Active() != 0 {
		t.Fatalf("после освобождения: active=%d", g.Active())
	}
}
