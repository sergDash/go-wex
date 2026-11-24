package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// note — запись для проверок: форма простая, важна только сохранность.
type note struct {
	Text string `json:"text"`
}

// TestOpenCreatesAndPersists — новый файл создаётся, запись переживает
// повторное открытие.
func TestOpenCreatesAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "store.json")

	s, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s.Version() != 0 {
		t.Fatalf("версия нового хранилища: %d", s.Version())
	}
	if err := s.Put("notes", "a", note{Text: "раз"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Второе открытие читает файл, а не память первого экземпляра.
	s2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Open второй: %v", err)
	}
	var got note
	ok, err := s2.Get("notes", "a", &got)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if got.Text != "раз" {
		t.Fatalf("запись: получили %q", got.Text)
	}
}

// TestRefuseNewer — файл новее кода не открывается и не изменяется.
func TestRefuseNewer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	if err := os.WriteFile(path, []byte(`{"version":2,"records":{}}`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := Open(path, []Migration{{Name: "only", Run: func(Records) error { return nil }}})
	if err == nil || !strings.Contains(err.Error(), "новее") {
		t.Fatalf("Open: ждали отказ по версии, получили %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), `"version":2`) {
		t.Fatalf("данные изменены: %s", data)
	}
}

// TestMigrationAppliedOnce — миграция накатывается один раз, повторное
// открытие её не выполняет.
func TestMigrationAppliedOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	calls := 0
	mig := Migration{Name: "seed", Run: func(r Records) error {
		calls++
		raw, err := json.Marshal(note{Text: "семя"})
		if err != nil {
			return err
		}
		if r["notes"] == nil {
			r["notes"] = map[string]json.RawMessage{}
		}
		r["notes"]["seed"] = raw
		return nil
	}}

	s, err := Open(path, []Migration{mig})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s.Version() != 1 || calls != 1 {
		t.Fatalf("после наката: версия=%d вызовов=%d", s.Version(), calls)
	}

	s2, err := Open(path, []Migration{mig})
	if err != nil {
		t.Fatalf("Open второй: %v", err)
	}
	if s2.Version() != 1 || calls != 1 {
		t.Fatalf("повторный накат: версия=%d вызовов=%d", s2.Version(), calls)
	}

	var got note
	ok, err := s2.Get("notes", "seed", &got)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if got.Text != "семя" {
		t.Fatalf("запись миграции: получили %q", got.Text)
	}
}

// TestMigrationFailure — ошибка шага называет номер и имя, файл остаётся
// прежней версии.
func TestMigrationFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	mig := Migration{Name: "bad", Run: func(Records) error { return errors.New("сломано") }}

	if _, err := Open(path, []Migration{mig}); err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("Open: ждали ошибку шага, получили %v", err)
	}

	s, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Open без миграций: %v", err)
	}
	if s.Version() != 0 {
		t.Fatalf("версия после сбоя: %d", s.Version())
	}
}

// TestKeysAndDelete — ключи идут по возрастанию, удаление убирает запись.
func TestKeysAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	s, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, key := range []string{"b", "a", "c"} {
		if err := s.Put("notes", key, note{Text: key}); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	if got := strings.Join(s.Keys("notes"), ","); got != "a,b,c" {
		t.Fatalf("ключи: %s", got)
	}

	if err := s.Delete("notes", "b"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := strings.Join(s.Keys("notes"), ","); got != "a,c" {
		t.Fatalf("ключи после удаления: %s", got)
	}

	var got note
	ok, err := s.Get("notes", "b", &got)
	if err != nil || ok {
		t.Fatalf("удалённая запись: ok=%v err=%v", ok, err)
	}
}
