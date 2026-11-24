// Package journal — журнал работы движка: дописывание событий в файл,
// ротация по размеру и сохранение старых копий. Журнал нужен, чтобы
// разбирать сбои, которые не роняют процесс: повторы, отказы, паники,
// медленные операции.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go-wex/engine/clock"
)

// Writer — приёмник записей журнала. Источник и вид называет вызывающий,
// поля — данные события.
type Writer interface {
	Write(source, kind string, fields map[string]any) error
}

// Discard — журнал, который ничего не пишет: когда журнал отключён.
type Discard struct{}

func (Discard) Write(string, string, map[string]any) error { return nil }

// record — одна строка файла журнала.
type record struct {
	Time   string         `json:"time"`
	Source string         `json:"source"`
	Kind   string         `json:"kind"`
	Fields map[string]any `json:"fields,omitempty"`
}

// Journal — файл журнала с ротацией. Один писатель: записи сериализуются
// мьютексом, файл дописывается, старые копии сохраняются.
type Journal struct {
	path    string
	maxSize int64
	keep    int
	clock   clock.Clock

	mu   sync.Mutex
	file *os.File
	size int64
}

// New собирает журнал. maxSize — предел файла в байтах, keep — сколько
// старых копий хранить.
func New(path string, maxSize int64, keep int, c clock.Clock) *Journal {
	return &Journal{path: path, maxSize: maxSize, keep: keep, clock: c}
}

// Name — имя службы для группы.
func (j *Journal) Name() string { return "journal" }

// Start открывает файл на дописывание. Существующий файл не перезаписывается.
func (j *Journal) Start(context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file != nil {
		return nil
	}
	return j.open()
}

// Stop закрывает файл.
func (j *Journal) Stop(context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file == nil {
		return nil
	}
	err := j.file.Close()
	j.file = nil
	return err
}

// Write добавляет запись. Если файл вырос больше предела, сначала
// прокручивается: текущий становится .1, старые сдвигаются.
func (j *Journal) Write(source, kind string, fields map[string]any) error {
	data, err := json.Marshal(record{
		Time:   j.clock.Now().UTC().Format(time.RFC3339Nano),
		Source: source,
		Kind:   kind,
		Fields: fields,
	})
	if err != nil {
		return fmt.Errorf("журнал: запись: %w", err)
	}
	data = append(data, '\n')

	j.mu.Lock()
	defer j.mu.Unlock()

	if j.file == nil {
		return errors.New("журнал: файл не открыт")
	}
	if j.maxSize > 0 && j.size+int64(len(data)) > j.maxSize {
		if err := j.rotate(); err != nil {
			return err
		}
	}

	n, err := j.file.Write(data)
	j.size += int64(n)
	if err != nil {
		return fmt.Errorf("журнал %s: запись: %w", j.path, err)
	}
	return nil
}

// open открывает файл и запоминает его размер: после перезапуска журнал
// дополняется, а не начинается заново.
func (j *Journal) open() error {
	if j.path == "" {
		return errors.New("журнал: пустой путь")
	}
	if err := os.MkdirAll(filepath.Dir(j.path), 0o755); err != nil {
		return fmt.Errorf("журнал %s: каталог: %w", j.path, err)
	}

	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("журнал %s: %w", j.path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("журнал %s: размер: %w", j.path, err)
	}

	j.file = f
	j.size = info.Size()
	return nil
}

// rotate закрывает текущий файл, сдвигает копии и открывает новый.
// Вызывается под мьютексом.
func (j *Journal) rotate() error {
	if err := j.file.Close(); err != nil {
		return fmt.Errorf("журнал %s: закрытие: %w", j.path, err)
	}
	j.file = nil

	// Старшая копия исчезает, остальные сдвигаются на шаг вверх:
	// .keep-1 → .keep, …, .1 → .2, затем текущий → .1.
	for i := j.keep - 1; i >= 1; i-- {
		old := fmt.Sprintf("%s.%d", j.path, i)
		next := fmt.Sprintf("%s.%d", j.path, i+1)
		if err := os.Rename(old, next); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("журнал %s: ротация %s: %w", j.path, old, err)
		}
	}
	if err := os.Rename(j.path, j.path+".1"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("журнал %s: ротация: %w", j.path, err)
	}
	return j.open()
}
