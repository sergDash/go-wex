package jobs

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go-wex/engine/clock"
	"go-wex/engine/store"
)

// allClasses — набор, при котором разборщик берёт любую работу.
var allClasses = []Class{ClassInteractive, ClassBackground}

// testNow — момент постановки в проверках очереди: время создания
// сравнивается точно, без обращения к системным часам.
var testNow = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

// TestList — очередь отдаёт все строки по возрастанию номера и со статусом:
// по этому списку строится таблица снаружи.
func TestList(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	first, err := q.Enqueue(ctx, testNow, "a", nil, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	second, err := q.Enqueue(ctx, testNow, "b", nil, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok, err := q.Claim(ctx, testNow, allClasses); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}

	entries, err := q.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("строк %d, ожидалось 2", len(entries))
	}
	if entries[0].Job.ID != first || entries[1].Job.ID != second {
		t.Fatalf("порядок: %s, %s", entries[0].Job.ID, entries[1].Job.ID)
	}
	if entries[0].Status != StatusRunning || entries[1].Status != StatusQueued {
		t.Fatalf("статусы: %q, %q", entries[0].Status, entries[1].Status)
	}
}

// TestTimesOnEnqueueClaimAndRetry — при постановке записывается время
// создания, при захвате — время начала, а возврат в очередь его очищает:
// работа снова ждёт, и прошлая попытка остаётся только в журнале.
func TestTimesOnEnqueueClaimAndRetry(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()
	created := testNow
	started := created.Add(3 * time.Second)

	id, err := q.Enqueue(ctx, created, "ok", nil, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, status, ok, err := q.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if status != StatusQueued || !job.CreatedAt.Equal(created) || !job.StartedAt.IsZero() {
		t.Fatalf("после постановки: status=%q created=%s started=%s", status, job.CreatedAt, job.StartedAt)
	}

	job, ok, err = q.Claim(ctx, started, allClasses)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if !job.StartedAt.Equal(started) || !job.CreatedAt.Equal(created) {
		t.Fatalf("после захвата: created=%s started=%s", job.CreatedAt, job.StartedAt)
	}

	if err := q.Retry(ctx, id, started.Add(time.Minute)); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	job, status, _, err = q.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get после Retry: %v", err)
	}
	if status != StatusQueued || !job.StartedAt.IsZero() || !job.CreatedAt.Equal(created) {
		t.Fatalf("после возврата: status=%q created=%s started=%s", status, job.CreatedAt, job.StartedAt)
	}
}

// TestMemoryQueueOrder — работы забираются по сроку, при равенстве по номеру.
func TestMemoryQueueOrder(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	first, err := q.Enqueue(ctx, testNow, "a", map[string]int{"n": 1}, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	second, err := q.Enqueue(ctx, testNow, "b", map[string]int{"n": 2}, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, ok, err := q.Claim(ctx, time.Now(), allClasses)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if job.ID != first {
		t.Fatalf("первая работа: получили %s, ждали %s", job.ID, first)
	}
	if job.Attempts != 1 {
		t.Fatalf("попытка: %d", job.Attempts)
	}

	job, ok, err = q.Claim(ctx, time.Now(), allClasses)
	if err != nil || !ok {
		t.Fatalf("Claim второй: ok=%v err=%v", ok, err)
	}
	if job.ID != second {
		t.Fatalf("вторая работа: получили %s, ждали %s", job.ID, second)
	}

	if _, ok, _ := q.Claim(ctx, time.Now(), allClasses); ok {
		t.Fatal("третья работа: очередь должна быть пуста")
	}
}

// TestClaimClassOrder — интерактивная работа идёт раньше фоновой, даже если
// поставлена позже.
func TestClaimClassOrder(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	background, err := q.Enqueue(ctx, testNow, "bg", nil, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	interactive, err := q.Enqueue(ctx, testNow, "fg", nil, ClassInteractive)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, ok, err := q.Claim(ctx, time.Now(), allClasses)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if job.ID != interactive || job.Class != ClassInteractive {
		t.Fatalf("первая работа: id=%s class=%s", job.ID, job.Class)
	}

	job, ok, err = q.Claim(ctx, time.Now(), allClasses)
	if err != nil || !ok {
		t.Fatalf("Claim второй: ok=%v err=%v", ok, err)
	}
	if job.ID != background {
		t.Fatalf("вторая работа: id=%s", job.ID)
	}
}

// TestClaimAllowedClasses — берутся только разрешённые классы; пустой
// список не берёт ничего.
func TestClaimAllowedClasses(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	background, err := q.Enqueue(ctx, testNow, "bg", nil, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	interactive, err := q.Enqueue(ctx, testNow, "fg", nil, ClassInteractive)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, ok, _ := q.Claim(ctx, time.Now(), nil); ok {
		t.Fatal("пустой список разрешил работу")
	}

	job, ok, err := q.Claim(ctx, time.Now(), []Class{ClassBackground})
	if err != nil || !ok || job.ID != background {
		t.Fatalf("фон: ok=%v err=%v id=%s", ok, err, job.ID)
	}

	job, ok, err = q.Claim(ctx, time.Now(), []Class{ClassInteractive})
	if err != nil || !ok || job.ID != interactive {
		t.Fatalf("интерактив: ok=%v err=%v id=%s", ok, err, job.ID)
	}
}

// TestClassDefaultAndValidation — пустой класс берётся из вида, неизвестный
// класс отвергается и при объявлении вида, и при постановке.
func TestClassDefaultAndValidation(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{
		Class:   ClassInteractive,
		Handler: func(context.Context, Job) error { return nil },
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	ctx := context.Background()

	id, err := d.Enqueue(ctx, "ok", nil, "")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, _, ok, err := d.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if job.Class != ClassInteractive {
		t.Fatalf("класс по умолчанию: %q", job.Class)
	}

	if _, err := d.Enqueue(ctx, "ok", nil, Class("urgent")); err == nil {
		t.Fatal("неизвестный класс принят")
	}

	bad := NewRegistry()
	if err := bad.Add("bad", Spec{Class: Class("urgent"), Handler: func(context.Context, Job) error { return nil }}); err == nil {
		t.Fatal("вид с неизвестным классом принят")
	}
}

// TestSetAllowed — пустой набор классов останавливает захват, полный
// возвращает разборщик к работе.
func TestSetAllowed(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(context.Context, Job) error { return nil }}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, "ok", nil, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	d.SetAllowed(nil)
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("пустой набор классов разрешил работу")
	}

	d.SetAllowed(allClasses)
	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("полный набор: handled=%v err=%v", handled, err)
	}
}

// TestStepSuccess — успешный обработчик переводит работу в done.
func TestStepSuccess(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(context.Context, Job) error { return nil }}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)

	if _, err := d.Enqueue(context.Background(), "ok", nil, ""); err != nil {
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
	clk := clock.NewManual(time.Unix(1000, 0))
	reg := NewRegistry()
	if err := reg.Add("bad", Spec{
		Handler:     func(context.Context, Job) error { return errors.New("нет связи") },
		MaxAttempts: 2,
		Backoff:     10 * time.Second,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	d.SetClock(clk)
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, "bad", nil, ""); err != nil {
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

	clk.Advance(10 * time.Second)
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

	if _, err := d.Enqueue(ctx, "panic", nil, ""); err != nil {
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

// TestStepPaused — на паузе новые работы не берутся, после возобновления
// берутся снова.
func TestStepPaused(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(context.Context, Job) error { return nil }}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, "ok", nil, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := d.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if handled, err := d.Step(ctx); handled || err != nil {
		t.Fatalf("на паузе: handled=%v err=%v", handled, err)
	}
	if !d.Paused() {
		t.Fatal("пауза не отмечена")
	}

	if err := d.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("после возобновления: handled=%v err=%v", handled, err)
	}
}

// TestPauseDuringWork — пауза изнутри обработчика не прерывает текущую
// работу, но следующая уже не берётся.
func TestPauseDuringWork(t *testing.T) {
	var d *Dispatcher
	reg := NewRegistry()
	if err := reg.Add("slow", Spec{Handler: func(ctx context.Context, _ Job) error {
		return d.Pause(ctx)
	}}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d = NewDispatcher(NewMemoryQueue(), reg)
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, "slow", nil, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := d.Enqueue(ctx, "slow", nil, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("текущая работа: handled=%v err=%v", handled, err)
	}
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("после паузы взята следующая работа")
	}
}

// TestGetStatus — работа и её статус видны снаружи на каждом шаге.
func TestGetStatus(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(context.Context, Job) error { return nil }}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	ctx := context.Background()

	id, err := d.Enqueue(ctx, "ok", nil, "")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job, status, ok, err := d.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if job.ID != id || status != StatusQueued || job.Attempts != 0 {
		t.Fatalf("до выполнения: id=%s status=%s attempts=%d", job.ID, status, job.Attempts)
	}

	if _, err := d.Step(ctx); err != nil {
		t.Fatalf("Step: %v", err)
	}
	job, status, ok, err = d.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get после шага: ok=%v err=%v", ok, err)
	}
	if status != StatusDone || job.Attempts != 1 {
		t.Fatalf("после выполнения: status=%s attempts=%d", status, job.Attempts)
	}

	if _, _, ok, err := d.Get(ctx, "нет"); err != nil || ok {
		t.Fatalf("отсутствующая работа: ok=%v err=%v", ok, err)
	}
}

// TestStepStopped — после отмены контекста новые работы не берутся.
func TestStepStopped(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(context.Context, Job) error { return nil }}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)

	if _, err := d.Enqueue(context.Background(), "ok", nil, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if handled, err := d.Step(ctx); handled || err != nil {
		t.Fatalf("после отмены: handled=%v err=%v", handled, err)
	}
}

// TestServiceStartStop — разборщик работает службой: Start возвращается,
// подсказка будит цикл, Stop дожидается горутины.
func TestServiceStartStop(t *testing.T) {
	done := make(chan string, 1)
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(_ context.Context, job Job) error {
		done <- job.ID
		return nil
	}}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	id, err := d.Enqueue(ctx, "ok", nil, "")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case got := <-done:
		if got != id {
			t.Fatalf("выполнена работа %s, ждали %s", got, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("работа не выполнена")
	}

	if err := d.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := d.Stop(context.Background()); err != nil {
		t.Fatalf("повторный Stop: %v", err)
	}
}

// openStoreQueue открывает очередь поверх хранилища по пути. Каждое
// открытие — как новый запуск процесса.
func openStoreQueue(t *testing.T, path string) Queue {
	t.Helper()

	st, err := store.Open(path, nil)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	q, err := OpenStoreQueue(st)
	if err != nil {
		t.Fatalf("OpenStoreQueue: %v", err)
	}
	return q
}

// TestStoreQueueEnqueuePersists — поставленная работа и её тело переживают
// повторное открытие хранилища.
func TestStoreQueueEnqueuePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	ctx := context.Background()

	q := openStoreQueue(t, path)
	id, err := q.Enqueue(ctx, testNow, "ok", map[string]int{"n": 1}, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	q2 := openStoreQueue(t, path)
	job, status, ok, err := q2.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if status != StatusQueued || job.Kind != "ok" {
		t.Fatalf("после перезапуска: status=%q kind=%q", status, job.Kind)
	}
	if string(job.Payload) != `{"n":1}` {
		t.Fatalf("тело: %s", job.Payload)
	}
	if !job.CreatedAt.Equal(testNow) {
		t.Fatalf("время создания после перезапуска: %s", job.CreatedAt)
	}
}

// TestStoreQueueRunningRecovers — работа, прерванная падением в running,
// при открытии возвращается в очередь с уже посчитанной попыткой.
func TestStoreQueueRunningRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	ctx := context.Background()

	q := openStoreQueue(t, path)
	id, err := q.Enqueue(ctx, testNow, "ok", nil, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok, err := q.Claim(ctx, time.Now(), allClasses); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}

	q2 := openStoreQueue(t, path)
	job, status, ok, err := q2.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if status != StatusQueued || job.Attempts != 1 {
		t.Fatalf("восстановление: status=%q attempts=%d", status, job.Attempts)
	}

	job, ok, err = q2.Claim(ctx, time.Now(), allClasses)
	if err != nil || !ok {
		t.Fatalf("повторный Claim: ok=%v err=%v", ok, err)
	}
	if job.ID != id || job.Attempts != 2 {
		t.Fatalf("повторный захват: id=%s attempts=%d", job.ID, job.Attempts)
	}
}

// TestStoreQueueSeqContinues — после перезапуска новые номера продолжают
// прежнюю последовательность, а не начинаются с первого.
func TestStoreQueueSeqContinues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	ctx := context.Background()

	q := openStoreQueue(t, path)
	if _, err := q.Enqueue(ctx, testNow, "ok", nil, ClassBackground); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Enqueue(ctx, testNow, "ok", nil, ClassBackground); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	q2 := openStoreQueue(t, path)
	id, err := q2.Enqueue(ctx, testNow, "ok", nil, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue после перезапуска: %v", err)
	}
	if id != "000003" {
		t.Fatalf("номер после перезапуска: %s", id)
	}
}

// TestStoreQueueStatusPersists — выполненная работа остаётся выполненной
// после перезапуска.
func TestStoreQueueStatusPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	ctx := context.Background()

	q := openStoreQueue(t, path)
	id, err := q.Enqueue(ctx, testNow, "ok", nil, ClassBackground)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, ok, err := q.Claim(ctx, time.Now(), allClasses); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := q.Done(ctx, id); err != nil {
		t.Fatalf("Done: %v", err)
	}

	q2 := openStoreQueue(t, path)
	_, status, ok, err := q2.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if status != StatusDone {
		t.Fatalf("статус после перезапуска: %q", status)
	}
}

// journalFake — журнал для проверки: запоминает виды и поля.
type journalFake struct {
	kinds  []string
	fields []map[string]any
}

func (j *journalFake) Write(_ string, kind string, fields map[string]any) error {
	j.kinds = append(j.kinds, kind)
	j.fields = append(j.fields, fields)
	return nil
}

// TestJournalRecords — выполнение и отказ попадают в журнал с номером работы.
func TestJournalRecords(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(context.Context, Job) error { return nil }}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := reg.Add("bad", Spec{
		Handler:     func(context.Context, Job) error { return errors.New("нет связи") },
		MaxAttempts: 1,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	j := &journalFake{}
	d.SetJournal(j)
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, "ok", nil, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := d.Enqueue(ctx, "bad", nil, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := d.Step(ctx); err != nil {
		t.Fatalf("Step ok: %v", err)
	}
	if _, err := d.Step(ctx); err == nil {
		t.Fatal("Step bad: ждали ошибку")
	}

	if len(j.kinds) != 2 || j.kinds[0] != "done" || j.kinds[1] != "fail" {
		t.Fatalf("виды: %v", j.kinds)
	}
	if j.fields[0]["id"] == "" || j.fields[1]["error"] == "" {
		t.Fatalf("поля: %+v", j.fields)
	}
}

// TestJournalPanic — паника пишется в журнал отдельным событием.
func TestJournalPanic(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("panic", Spec{
		Handler:     func(context.Context, Job) error { panic("сломано") },
		MaxAttempts: 1,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	j := &journalFake{}
	d.SetJournal(j)
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, "panic", nil, ""); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := d.Step(ctx); err == nil {
		t.Fatal("Step: ждали ошибку паники")
	}

	if len(j.kinds) != 2 || j.kinds[0] != "panic" || j.kinds[1] != "fail" {
		t.Fatalf("виды: %v", j.kinds)
	}
	if j.fields[0]["stack"] == "" {
		t.Fatalf("нет стека: %+v", j.fields[0])
	}
}
