package logru

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"go-wex/engine/lang"
)

// journalFake запоминает записи и их поля: по ним видно, что в журнал ушёл
// тот же текст, что и в ответ.
type journalFake struct {
	sources []string
	kinds   []string
	fields  []map[string]any
}

func (j *journalFake) Write(source, kind string, fields map[string]any) error {
	j.sources = append(j.sources, source)
	j.kinds = append(j.kinds, kind)
	j.fields = append(j.fields, fields)
	return nil
}

// openTest перекладывает словарь модуля во временный каталог и открывает
// его так же, как это делает точка сборки.
func openTest(t *testing.T, body string) *lang.Catalog {
	t.Helper()

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ru-RU"), 0o755); err != nil {
		t.Fatalf("каталог локали: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ru-RU", "log_ru.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("файл словаря: %v", err)
	}

	catalog, err := lang.Open(dir, "ru-RU")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return catalog
}

// TestWriteUsesDictionary — фраза берётся из словаря, попадает в журнал
// и в ответ; язык назван рядом, чтобы было видно источник текста.
func TestWriteUsesDictionary(t *testing.T) {
	catalog := openTest(t, `{"written": "строка из словаря"}`)
	j := &journalFake{}

	built, err := New(catalog, j, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	value, err := built.(module).write((*http.Request)(nil))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	got := value.(written)
	if got.Text != "строка из словаря" || got.Lang != "ru-RU" {
		t.Fatalf("ответ: %+v", got)
	}

	if len(j.kinds) != 1 || j.kinds[0] != "written" || j.sources[0] != "log_ru" {
		t.Fatalf("журнал: %+v", j)
	}
	if j.fields[0]["text"] != "строка из словаря" || j.fields[0]["lang"] != "ru-RU" {
		t.Fatalf("поля записи: %+v", j.fields[0])
	}
}

// TestMissingKey — пустой ключ даёт ошибку, а не пустую запись: строка без
// текста в журнале бесполезна.
func TestMissingKey(t *testing.T) {
	catalog := openTest(t, `{}`)

	built, err := New(catalog, &journalFake{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := built.(module).write((*http.Request)(nil)); err == nil {
		t.Fatal("пустой ключ принят")
	}
}

// TestNeedsCatalog — модуль не собирается без словаря: сказать об этом при
// запуске лучше, чем писать пустые строки.
func TestNeedsCatalog(t *testing.T) {
	if _, err := New(nil, &journalFake{}, nil); err == nil {
		t.Fatal("сборка без словаря прошла")
	}
}
