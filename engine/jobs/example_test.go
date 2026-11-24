package jobs

import (
	"context"
	"fmt"
	"time"
)

// ExampleDispatcher — путь работы от постановки до выполнения: вид в
// реестре, тело в очереди, один шаг разборщика. Payload приходит
// обработчику в том виде, в каком его записал Enqueue: JSON.
func ExampleDispatcher() {
	reg := NewRegistry()
	err := reg.Add("report", Spec{
		Handler: func(_ context.Context, job Job) error {
			fmt.Printf("отчёт %s: %s\n", job.ID, job.Payload)
			return nil
		},
		MaxAttempts: 3,
		Backoff:     10 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	d := NewDispatcher(NewMemoryQueue(), reg)
	id, err := d.Enqueue(context.Background(), "report", map[string]string{"since": "2026-11-01"}, ClassBackground)
	if err != nil {
		panic(err)
	}
	fmt.Println("поставлена:", id)

	handled, err := d.Step(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("взята:", handled)

	// Output:
	// поставлена: 000001
	// отчёт 000001: {"since":"2026-11-01"}
	// взята: true
}
