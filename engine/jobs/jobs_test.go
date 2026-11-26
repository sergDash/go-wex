package jobs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-wex/engine/clock"
	"go-wex/engine/store"
)

// allClasses — набор классов для расписания: так разборщик берёт любую работу.
var allClasses = []Class{ClassInteractive, ClassBackground}

// anyJob — правило допуска, которое пропускает всё: проверки очереди
// не задают ни классов, ни порогов.
func anyJob(Job) bool { return true }

// testNow — момент постановки в проверках очереди: время создания
// сравнивается точно, без обращения к системным часам.
var testNow = time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

// put кладёт работу без срока и ключа: проверки, которым важны только вид,
// тело и класс, не повторяют остальные поля запроса.
func put(t *testing.T, q Queue, now time.Time, kind Kind, payload any, class Class) string {
	t.Helper()

	id, err := q.Enqueue(context.Background(), now, Request{Kind: kind, Payload: payload, Class: class})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

// TestList — очередь отдаёт все строки по возрастанию номера и со статусом:
// по этому списку строится таблица снаружи.
func TestList(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	first := put(t, q, testNow, "a", nil, ClassBackground)
	second := put(t, q, testNow, "b", nil, ClassBackground)
	if _, ok, err := q.Claim(ctx, testNow, anyJob); err != nil || !ok {
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

	id := put(t, q, created, "ok", nil, ClassBackground)

	job, status, ok, err := q.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if status != StatusQueued || !job.CreatedAt.Equal(created) || !job.StartedAt.IsZero() {
		t.Fatalf("после постановки: status=%q created=%s started=%s", status, job.CreatedAt, job.StartedAt)
	}

	job, ok, err = q.Claim(ctx, started, anyJob)
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

	first := put(t, q, testNow, "a", map[string]int{"n": 1}, ClassBackground)
	second := put(t, q, testNow, "b", map[string]int{"n": 2}, ClassBackground)

	job, ok, err := q.Claim(ctx, time.Now(), anyJob)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if job.ID != first {
		t.Fatalf("первая работа: получили %s, ждали %s", job.ID, first)
	}
	if job.Attempts != 1 {
		t.Fatalf("попытка: %d", job.Attempts)
	}

	job, ok, err = q.Claim(ctx, time.Now(), anyJob)
	if err != nil || !ok {
		t.Fatalf("Claim второй: ok=%v err=%v", ok, err)
	}
	if job.ID != second {
		t.Fatalf("вторая работа: получили %s, ждали %s", job.ID, second)
	}

	if _, ok, _ := q.Claim(ctx, time.Now(), anyJob); ok {
		t.Fatal("третья работа: очередь должна быть пуста")
	}
}

// TestClaimNotBefore — работа со сроком не берётся раньше срока и берётся
// в срок; из двух доступных раньше идёт та, чей срок наступил раньше.
func TestClaimNotBefore(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	early := testNow.Add(10 * time.Minute)
	late := testNow.Add(time.Hour)

	// Порядок постановки обратный срокам: раньше поставлена та, что нужна
	// позже. Порядок захвата должен идти по сроку, а не по номеру.
	lateID, err := q.Enqueue(ctx, testNow, Request{Kind: "late", NotBefore: late, Class: ClassBackground})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	earlyID, err := q.Enqueue(ctx, testNow, Request{Kind: "early", NotBefore: early, Class: ClassBackground})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if _, ok, _ := q.Claim(ctx, testNow.Add(9*time.Minute), anyJob); ok {
		t.Fatal("работа взята раньше своего срока")
	}

	job, ok, err := q.Claim(ctx, early, anyJob)
	if err != nil || !ok || job.ID != earlyID {
		t.Fatalf("в срок: ok=%v err=%v id=%s, ждали %s", ok, err, job.ID, earlyID)
	}

	job, ok, err = q.Claim(ctx, late, anyJob)
	if err != nil || !ok || job.ID != lateID {
		t.Fatalf("второй срок: ok=%v err=%v id=%s, ждали %s", ok, err, job.ID, lateID)
	}

	// Срок остаётся у завершённой строки: по нему видно, к какому моменту
	// работа была нужна.
	if err := q.Done(ctx, earlyID); err != nil {
		t.Fatalf("Done: %v", err)
	}
	job, _, _, err = q.Get(ctx, earlyID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !job.NotBefore.Equal(early) {
		t.Fatalf("срок после выполнения: %s", job.NotBefore)
	}
}

// TestEnqueueKey — пока живая работа с ключом стоит, вторая с тем же ключом
// не создаётся; после завершения ключ свободен.
func TestEnqueueKey(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()
	req := Request{Kind: "report", Key: "day:2026-11-25", Class: ClassBackground}

	first, err := q.Enqueue(ctx, testNow, req)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	second, err := q.Enqueue(ctx, testNow, req)
	if err != nil {
		t.Fatalf("повторная Enqueue: %v", err)
	}
	if second != first {
		t.Fatalf("повторный ключ дал новую работу: %s вместо %s", second, first)
	}

	if _, ok, err := q.Claim(ctx, testNow, anyJob); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	// Выполняющаяся работа ключ тоже держит: второй разборщик не должен
	// поставить ту же работу заново.
	third, err := q.Enqueue(ctx, testNow, req)
	if err != nil {
		t.Fatalf("Enqueue во время выполнения: %v", err)
	}
	if third != first {
		t.Fatalf("ключ занятой работы не удержан: %s", third)
	}

	if err := q.Done(ctx, first); err != nil {
		t.Fatalf("Done: %v", err)
	}
	fourth, err := q.Enqueue(ctx, testNow, req)
	if err != nil {
		t.Fatalf("Enqueue после Done: %v", err)
	}
	if fourth == first {
		t.Fatal("завершённая работа держит ключ")
	}
}

// TestCancel — отменить можно только ждущую работу; взятая уже выполняется.
func TestCancel(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	id := put(t, q, testNow, "ok", nil, ClassBackground)
	if err := q.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	_, status, _, err := q.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if status != StatusCancelled {
		t.Fatalf("статус после отмены: %q", status)
	}
	if _, ok, _ := q.Claim(ctx, testNow, anyJob); ok {
		t.Fatal("отменённая работа взята")
	}

	// Повторная отмена и отмена взятой работы — ошибка: молчаливое «уже
	// отменено» скрыло бы, что работа уже выполняется.
	if err := q.Cancel(ctx, id); err == nil {
		t.Fatal("повторная отмена принята")
	}

	running := put(t, q, testNow, "ok", nil, ClassBackground)
	if _, ok, err := q.Claim(ctx, testNow, anyJob); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if err := q.Cancel(ctx, running); err == nil {
		t.Fatal("отмена выполняющейся работы принята")
	}
}

// TestPostpone — отсрочка возвращает работу в очередь и не тратит попытку.
func TestPostpone(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	id := put(t, q, testNow, "ok", nil, ClassBackground)
	job, ok, err := q.Claim(ctx, testNow, anyJob)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if job.Attempts != 1 {
		t.Fatalf("попытка после захвата: %d", job.Attempts)
	}

	at := testNow.Add(time.Minute)
	if err := q.Postpone(ctx, id, at); err != nil {
		t.Fatalf("Postpone: %v", err)
	}
	job, status, _, err := q.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if status != StatusQueued || job.Attempts != 0 || !job.NotBefore.Equal(at) {
		t.Fatalf("после отсрочки: status=%q attempts=%d notBefore=%s", status, job.Attempts, job.NotBefore)
	}
}

// TestNext — работа сама назначает себе следующий круг: статус ждущей,
// попытки с нуля, срок из промежутка.
func TestNext(t *testing.T) {
	const kind Kind = "self"
	var runs int
	reg := NewRegistry()
	if err := reg.Add(kind, Spec{
		MaxAttempts: 1,
		Handler: func(_ context.Context, _ Job) error {
			runs++
			if runs == 1 {
				return Next(time.Hour)
			}
			return nil
		},
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	d.SetClock(clock.NewManual(testNow))
	ctx := context.Background()

	id, err := d.Enqueue(ctx, Request{Kind: kind})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("первый круг: handled=%v err=%v", handled, err)
	}

	job, status, _, err := d.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if status != StatusQueued || job.Attempts != 0 {
		t.Fatalf("после круга: status=%q attempts=%d", status, job.Attempts)
	}
	if !job.NotBefore.Equal(testNow.Add(time.Hour)) {
		t.Fatalf("срок следующего круга: %s", job.NotBefore)
	}

	// Срок не наступил: работа не берётся.
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("следующий круг начался раньше срока")
	}
}

// TestPermanent — постоянная ошибка даёт отказ сразу, не расходуя попытки.
func TestPermanent(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("bad", Spec{
		MaxAttempts: 3,
		Handler: func(context.Context, Job) error {
			return Permanent(errors.New("вход неверен"))
		},
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	ctx := context.Background()

	id, err := d.Enqueue(ctx, Request{Kind: "bad"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if handled, err := d.Step(ctx); err == nil || !handled {
		t.Fatalf("Step: handled=%v err=%v", handled, err)
	}
	job, status, _, err := d.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if status != StatusFailed || job.Attempts != 1 {
		t.Fatalf("после отказа: status=%q attempts=%d", status, job.Attempts)
	}
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("отказная работа взята снова")
	}
}

// TestTimeout — обработчик, который смотрит на контекст, не держит разборщик
// дольше предела попытки; работа уходит в повтор.
func TestTimeout(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("stuck", Spec{
		Timeout:     20 * time.Millisecond,
		MaxAttempts: 2,
		Handler: func(ctx context.Context, _ Job) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	j := &journalFake{}
	d.SetJournal(j)
	ctx := context.Background()

	id, err := d.Enqueue(ctx, Request{Kind: "stuck"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	started := time.Now()
	handled, err := d.Step(ctx)
	took := time.Since(started)
	if !handled || err == nil {
		t.Fatalf("Step: handled=%v err=%v", handled, err)
	}
	if took > 2*time.Second {
		t.Fatalf("шаг ждал обработчик %s", took)
	}

	job, status, _, err := d.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if status != StatusQueued || job.Attempts != 1 || job.NotBefore.IsZero() {
		t.Fatalf("после срока: status=%q attempts=%d notBefore=%s", status, job.Attempts, job.NotBefore)
	}
	if len(j.kinds) == 0 || j.kinds[len(j.kinds)-1] != "timeout" {
		t.Fatalf("журнал: %v", j.kinds)
	}
}

// TestShutdownPostpones — остановка разборщика возвращает работу в очередь
// и не тратит попытку: работа не виновата в закрытии процесса.
func TestShutdownPostpones(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := NewRegistry()
	if err := reg.Add("ok", Spec{Handler: func(ctx context.Context, _ Job) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)

	id, err := d.Enqueue(context.Background(), Request{Kind: "ok"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("Step: handled=%v err=%v", handled, err)
	}

	job, status, _, err := d.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if status != StatusQueued || job.Attempts != 0 {
		t.Fatalf("после остановки: status=%q attempts=%d", status, job.Attempts)
	}
}

// TestRetention — сверх правила хранения остаются только последние
// завершённые строки; ждущие не убираются.
func TestRetention(t *testing.T) {
	q := NewMemoryQueue().(*queue)
	q.SetKeep(2)
	ctx := context.Background()

	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, put(t, q, testNow, "ok", nil, ClassBackground))
	}
	if _, ok, err := q.Claim(ctx, testNow, anyJob); err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	for _, id := range ids[:3] {
		if err := q.Done(ctx, id); err != nil {
			t.Fatalf("Done: %v", err)
		}
	}

	entries, err := q.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Три завершённые строки при правиле «держать две»: самая старая уходит,
	// остаются вторая и третья, а ждущая четвёртая не тронута.
	if len(entries) != 3 {
		t.Fatalf("строк %d, ожидалось 3: %+v", len(entries), entries)
	}
	if entries[0].Job.ID != ids[1] || entries[1].Job.ID != ids[2] || entries[2].Job.ID != ids[3] {
		t.Fatalf("оставлены: %s, %s, %s", entries[0].Job.ID, entries[1].Job.ID, entries[2].Job.ID)
	}
}

// TestClaimClassOrder — интерактивная работа идёт раньше фоновой, даже если
// поставлена позже.
func TestClaimClassOrder(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	background := put(t, q, testNow, "bg", nil, ClassBackground)
	interactive := put(t, q, testNow, "fg", nil, ClassInteractive)

	job, ok, err := q.Claim(ctx, time.Now(), anyJob)
	if err != nil || !ok {
		t.Fatalf("Claim: ok=%v err=%v", ok, err)
	}
	if job.ID != interactive || job.Class != ClassInteractive {
		t.Fatalf("первая работа: id=%s class=%s", job.ID, job.Class)
	}

	job, ok, err = q.Claim(ctx, time.Now(), anyJob)
	if err != nil || !ok {
		t.Fatalf("Claim второй: ok=%v err=%v", ok, err)
	}
	if job.ID != background {
		t.Fatalf("вторая работа: id=%s", job.ID)
	}
}

// TestClaimAllowedClasses — берутся только разрешённые классы; пустой
// список не берёт ничего. Классы — правило расписания, поэтому проверка
// идёт через разборщик, а не через очередь.
func TestClaimAllowedClasses(t *testing.T) {
	reg := NewRegistry()
	handler := func(context.Context, Job) error { return nil }
	if err := reg.Add("bg", Spec{Class: ClassBackground, Handler: handler}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := reg.Add("fg", Spec{Class: ClassInteractive, Handler: handler}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	ctx := context.Background()

	background, err := d.Enqueue(ctx, Request{Kind: "bg"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	interactive, err := d.Enqueue(ctx, Request{Kind: "fg"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	d.SetAllowed(nil)
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("пустой список разрешил работу")
	}

	d.SetAllowed([]Class{ClassBackground})
	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("фон: handled=%v err=%v", handled, err)
	}
	if _, status, _, _ := d.Get(ctx, background); status != StatusDone {
		t.Fatalf("фон не выполнен: %q", status)
	}
	if _, status, _, _ := d.Get(ctx, interactive); status != StatusQueued {
		t.Fatalf("интерактивная взята раньше разрешения: %q", status)
	}

	d.SetAllowed([]Class{ClassInteractive})
	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("интерактив: handled=%v err=%v", handled, err)
	}
	if _, status, _, _ := d.Get(ctx, interactive); status != StatusDone {
		t.Fatalf("интерактивная не выполнена: %q", status)
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

	id, err := d.Enqueue(ctx, Request{Kind: "ok"})
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

	if _, err := d.Enqueue(ctx, Request{Kind: "ok", Class: Class("urgent")}); err == nil {
		t.Fatal("неизвестный класс принят")
	}

	bad := NewRegistry()
	if err := bad.Add("bad", Spec{Class: Class("urgent"), Handler: func(context.Context, Job) error { return nil }}); err == nil {
		t.Fatal("вид с неизвестным классом принят")
	}
	if err := bad.Add("negative", Spec{Timeout: -time.Second, Handler: func(context.Context, Job) error { return nil }}); err == nil {
		t.Fatal("вид с отрицательным пределом попытки принят")
	}
}

// TestSpecMinIdle — у вида работы свой порог простоя: работа ждёт, пока
// человек действовал позже, чем порог назад, и берётся ровно в порог.
func TestSpecMinIdle(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Add("report", Spec{
		Handler: func(context.Context, Job) error { return nil },
		MinIdle: 10 * time.Minute,
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	d.SetClock(clock.NewManual(testNow))

	// Момент активности меняется между шагами: часы стоят, простой растёт.
	lastActive := testNow.Add(-5 * time.Minute)
	d.SetIdleFunc(func(time.Time) (time.Time, bool) {
		return lastActive, false
	})
	ctx := context.Background()

	if _, err := d.Enqueue(ctx, Request{Kind: "report"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if handled, _ := d.Step(ctx); handled {
		t.Fatal("работа взята раньше порога простоя")
	}

	lastActive = testNow.Add(-10 * time.Minute)
	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("в порог: handled=%v err=%v", handled, err)
	}
}

// TestBackgroundWaitsForRequest — фон не берётся, пока идёт запрос;
// интерактивная работа берётся в тот же момент.
func TestBackgroundWaitsForRequest(t *testing.T) {
	reg := NewRegistry()
	handler := func(context.Context, Job) error { return nil }
	if err := reg.Add("bg", Spec{Class: ClassBackground, Handler: handler}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := reg.Add("fg", Spec{Class: ClassInteractive, Handler: handler}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	d := NewDispatcher(NewMemoryQueue(), reg)
	d.SetIdleFunc(func(time.Time) (time.Time, bool) {
		return time.Time{}, true
	})
	ctx := context.Background()

	background, err := d.Enqueue(ctx, Request{Kind: "bg"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	interactive, err := d.Enqueue(ctx, Request{Kind: "fg"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Интерактивная обгоняет фон и по классу, и по правилу занятости.
	if handled, err := d.Step(ctx); err != nil || !handled {
		t.Fatalf("Step: handled=%v err=%v", handled, err)
	}
	if _, status, _, _ := d.Get(ctx, interactive); status != StatusDone {
		t.Fatalf("интерактивная не выполнена: %q", status)
	}
	if handled, _ := d.Step(ctx); handled {
		t.Fatal("фон взят во время запроса")
	}
	if _, status, _, _ := d.Get(ctx, background); status != StatusQueued {
		t.Fatalf("фон не ждёт: %q", status)
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

	if _, err := d.Enqueue(ctx, Request{Kind: "ok"}); err != nil {
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

	if _, err := d.Enqueue(context.Background(), Request{Kind: "ok"}); err != nil {
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

	if _, err := d.Enqueue(ctx, Request{Kind: "bad"}); err != nil {
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

	if _, err := d.Enqueue(ctx, Request{Kind: "panic"}); err != nil {
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

	if _, err := d.Enqueue(ctx, Request{Kind: "ok"}); err != nil {
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

	if _, err := d.Enqueue(ctx, Request{Kind: "slow"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := d.Enqueue(ctx, Request{Kind: "slow"}); err != nil {
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

	id, err := d.Enqueue(ctx, Request{Kind: "ok"})
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

	if _, err := d.Enqueue(context.Background(), Request{Kind: "ok"}); err != nil {
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

	id, err := d.Enqueue(ctx, Request{Kind: "ok"})
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
	id := put(t, q, testNow, "ok", map[string]int{"n": 1}, ClassBackground)

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
	id := put(t, q, testNow, "ok", nil, ClassBackground)
	if _, ok, err := q.Claim(ctx, time.Now(), anyJob); err != nil || !ok {
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

	job, ok, err = q2.Claim(ctx, time.Now(), anyJob)
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

	q := openStoreQueue(t, path)
	put(t, q, testNow, "ok", nil, ClassBackground)
	put(t, q, testNow, "ok", nil, ClassBackground)

	q2 := openStoreQueue(t, path)
	id := put(t, q2, testNow, "ok", nil, ClassBackground)
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
	id := put(t, q, testNow, "ok", nil, ClassBackground)
	if _, ok, err := q.Claim(ctx, time.Now(), anyJob); err != nil || !ok {
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

	if _, err := d.Enqueue(ctx, Request{Kind: "ok"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := d.Enqueue(ctx, Request{Kind: "bad"}); err != nil {
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

	if _, err := d.Enqueue(ctx, Request{Kind: "panic"}); err != nil {
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

// TestLaterQueue — очередь, открывающая хранилище при первом обращении:
// пока к ней не обратились, файла нет и второго писателя не появляется.
func TestLaterQueue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	ctx := context.Background()

	opens := 0
	q := OpenStoreQueueLater(func() (*store.Store, error) {
		opens++
		return store.Open(path, nil)
	})

	if opens != 0 {
		t.Fatalf("хранилище открыто до обращения: %d", opens)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("файл появился до обращения: %v", err)
	}

	id, err := q.Enqueue(ctx, testNow, Request{Kind: "ok", Class: ClassBackground})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if opens != 1 {
		t.Fatalf("открытий: %d", opens)
	}

	// Второе обращение пользуется тем же хранилищем.
	if _, status, ok, err := q.Get(ctx, id); err != nil || !ok || status != StatusQueued {
		t.Fatalf("Get: ok=%v status=%q err=%v", ok, status, err)
	}
	if opens != 1 {
		t.Fatalf("повторное открытие: %d", opens)
	}
}
