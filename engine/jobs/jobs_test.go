package jobs

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestMemoryQueueOrder — работы забираются по сроку, при равенстве по номеру.
func TestMemoryQueueOrder(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	first, err := q.Enqueue(ctx, "a", map[string]int{"n": 1})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	second, err := q.Enqueue(ctx, "b", map[string]int{"n": 2})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, ok, err := q.Claim(ctx, time.Now())
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if job.ID != first {
		t.Fatalf("первая работа: получили %s, ждали %s", job.ID, first)
	}
	if job.Attempts != 1 {
		t.Fatalf("попытка: %d", job.Attempts)
	}

	job, ok, err = q.Claim(ctx, time.Now())
	if err != nil || !ok {
		t.Fatalf("Claim второй: ok=%v err=%v", ok, err)
	}
	if job.ID != second {
		t.Fatalf("вторая работа: получили %s, ждали %s", job.ID, second)
	}

	if _, ok, _ := q.Claim(ctx, time.Now()); ok {
		t.Fatal("третья работа: очередь должна быть пуста")
	}
}

// TestStepSuccess — успешный обработчик переводит работу в done.
func TestStepSuccess(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(context.Context, Job) error { return nil }}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)

	if _, err := d.Enqueue(context.Background(), "ok", nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	handled, err := d.Step(context.Background())
	if err != nil || !handled {
		t.Fatalf("Step: handled=%v err=%v", handled, err)
	}
	if handled, _ := d.Step(context.Background()); handled {
		t.Fatal("работа выполнена дважды")
	}
}

// TestStepRetryThenFail — ошибка даёт повтор с задержкой, затем отказ.
func TestStepRetryThenFail(t *testing.T) {
	now := time.Unix(1000, 0)
	reg := NewRegistry()
	if err := reg.Add("bad", Spec{
		Handler:     func(context.Context, Job) error { return errors.New("нет связи") },
		MaxAttempts: 2,
		Backoff:     10 * time.Second,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	d.SetNow(func() time.Time { return now })
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, "bad", nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	handled, err := d.Step(ctx)
	if err != nil {
		// Ошибка работы возвращается для журнала — это ожидаемо.
		_ = err
	}
	if !handled {
		t.Fatal("первая попытка не взята")
	}

	// Срок не наступил: работа не должна браться.
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("повтор раньше срока")
	}

	now = now.Add(10 * time.Second)
	if handled, _ := d.Step(ctx); !handled {
		t.Fatal("вторая попытка не взята")
	}

	// Попытки кончились: работа больше не берётся.
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("отказная работа взята снова")
	}
}

// TestStepPanic — паника обработчика не роняет разборщик.
func TestStepPanic(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("panic", Spec{
		Handler:     func(context.Context, Job) error { panic("сломано") },
		MaxAttempts: 1,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, "panic", nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	handled, err := d.Step(ctx)
	if !handled {
		t.Fatal("работа не взята")
	}
	if err == nil {
		t.Fatal("паника не превратилась в ошибку")
	}
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("отказная работа взята снова")
	}
}
