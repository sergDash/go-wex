package journal

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-wex/engine/clock"
)

// lines читает непустые строки файла.
func lines(t *testing.T, path string) []string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// TestWriteAndReopen — запись содержит время, источник, вид и поля;
// после перезапуска журнал дополняется, а не перезаписывается.
func TestWriteAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "journal.log")
	clk := clock.NewManual(time.Unix(1000, 0))
	ctx := context.Background()

	j := New(path, 1024, 3, clk)
	if err := j.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := j.Write("jobs", "retry", map[string]any{"id": "000001", "attempts": 1}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := j.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	j2 := New(path, 1024, 3, clk)
	if err := j2.Start(ctx); err != nil {
		t.Fatalf("Start второй: %v", err)
	}
	if err := j2.Write("http", "panic", map[string]any{"path": "/api/x"}); err != nil {
		t.Fatalf("Write второй: %v", err)
	}
	if err := j2.Stop(ctx); err != nil {
		t.Fatalf("Stop второй: %v", err)
	}

	got := lines(t, path)
	if len(got) != 2 {
		t.Fatalf("строк: %d", len(got))
	}

	var first record
	if err := json.Unmarshal([]byte(got[0]), &first); err != nil {
		t.Fatalf("разбор первой: %v", err)
	}
	if first.Time != "1970-01-01T00:16:40Z" || first.Source != "jobs" || first.Kind != "retry" {
		t.Fatalf("первая запись: %+v", first)
	}
	if first.Fields["id"] != "000001" {
		t.Fatalf("поля: %+v", first.Fields)
	}

	var second record
	if err := json.Unmarshal([]byte(got[1]), &second); err != nil {
		t.Fatalf("разбор второй: %v", err)
	}
	if second.Source != "http" || second.Kind != "panic" {
		t.Fatalf("вторая запись: %+v", second)
	}
}

// TestRotation — файл прокручивается по размеру, старые копии сохраняются,
// число копий ограничено.
func TestRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.log")
	ctx := context.Background()
	j := New(path, 120, 2, clock.NewManual(time.Unix(1000, 0)))
	if err := j.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i := 0; i < 6; i++ {
		if err := j.Write("jobs", "done", map[string]any{"n": i}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := j.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("нет копии .1: %v", err)
	}
	if _, err := os.Stat(path + ".2"); err != nil {
		t.Fatalf("нет копии .2: %v", err)
	}
	if _, err := os.Stat(path + ".3"); err == nil {
		t.Fatal("копия .3 сверх предела")
	}
	if got := lines(t, path); len(got) == 0 {
		t.Fatal("текущий файл пуст")
	}
}

// TestWriteBeforeStart — запись до открытия файла даёт ошибку, а не теряется
// молча.
func TestWriteBeforeStart(t *testing.T) {
	j := New(filepath.Join(t.TempDir(), "journal.log"), 1024, 1, clock.NewManual(time.Unix(1000, 0)))
	if err := j.Write("jobs", "done", nil); err == nil {
		t.Fatal("запись до Start принята")
	}
}

// TestDiscard — отключённый журнал принимает записи и ничего не делает.
func TestDiscard(t *testing.T) {
	if err := (Discard{}).Write("jobs", "done", nil); err != nil {
		t.Fatalf("Discard: %v", err)
	}
}
