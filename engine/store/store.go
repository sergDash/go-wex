// Package store — долговечное хранилище JSON-записей: файл с версией схемы,
// миграциями и атомарной записью. Через него данные переживают перезапуск
// процесса, а не живут только в памяти.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Migration — шаг изменения формы записей. Номер шага — его место в списке,
// поэтому применённую миграцию не редактируют: новая форма — новый шаг.
type Migration struct {
	Name string
	Run  func(Records) error
}

// Records — записи по коллекциям: имя коллекции → ключ → JSON. Миграция
// получает их целиком и меняет на месте; форму записи проверяет тот, кто
// её читает.
type Records map[string]map[string]json.RawMessage

// document — форма файла на диске. Версия — максимальный применённый номер,
// а не константа в коде.
type document struct {
	Version int     `json:"version"`
	Records Records `json:"records"`
}

// Store — открытое хранилище. Записи держатся в памяти, каждое изменение
// целиком записывается на диск. Доступ сериализуется мьютексом: из нескольких
// горутин писать безопасно, соседний процесс не координируется.
type Store struct {
	path string
	mu   sync.Mutex
	doc  document
}

// Open открывает хранилище и доводит его до версии len(migrations).
// Файла нет — создаётся пустой. Файл новее кода — отказ: старый код не
// должен записывать поверх данных, которых не понимает.
func Open(path string, migrations []Migration) (*Store, error) {
	if path == "" {
		return nil, errors.New("хранилище: пустой путь")
	}

	doc, created, err := read(path)
	if err != nil {
		return nil, err
	}

	want := len(migrations)
	if doc.Version < 0 {
		return nil, fmt.Errorf("хранилище %s: недопустимая версия %d", path, doc.Version)
	}
	if doc.Version > want {
		return nil, fmt.Errorf("хранилище %s: версия %d новее, чем понимает код (%d)", path, doc.Version, want)
	}
	if doc.Records == nil {
		doc.Records = Records{}
	}

	s := &Store{path: path, doc: doc}
	if created {
		if err := s.save(); err != nil {
			return nil, err
		}
	}

	// Миграции идут только вперёд и по одной: после шага файл на диске уже
	// следующей версии, поэтому сбой на шаге N не откатывает шаги до N.
	for i := doc.Version; i < want; i++ {
		if err := migrations[i].Run(s.doc.Records); err != nil {
			return nil, fmt.Errorf("хранилище %s: миграция %d (%s): %w", path, i+1, migrations[i].Name, err)
		}
		s.doc.Version = i + 1
		if err := s.save(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Get читает запись в dst. Отсутствие записи — не ошибка: ok равно false.
func (s *Store) Get(collection, key string, dst any) (bool, error) {
	s.mu.Lock()
	raw, ok := s.doc.Records[collection][key]
	s.mu.Unlock()

	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return false, fmt.Errorf("хранилище: чтение %s/%s: %w", collection, key, err)
	}
	return true, nil
}

// Put записывает значение и сразу сохраняет файл. Если сохранить не удалось,
// память возвращается к состоянию файла: иначе следующая удачная запись
// сохранила бы то, что не удалось сохранить сейчас.
func (s *Store) Put(collection, key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("хранилище: запись %s/%s: %w", collection, key, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	col := s.doc.Records[collection]
	if col == nil {
		col = map[string]json.RawMessage{}
		s.doc.Records[collection] = col
	}
	prev, had := col[key]
	col[key] = raw
	if err := s.save(); err != nil {
		if had {
			col[key] = prev
		} else {
			delete(col, key)
		}
		return err
	}
	return nil
}

// Delete удаляет запись. Отсутствие записи — не ошибка.
func (s *Store) Delete(collection, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	col := s.doc.Records[collection]
	prev, had := col[key]
	if !had {
		return nil
	}
	delete(col, key)
	if err := s.save(); err != nil {
		col[key] = prev
		return err
	}
	return nil
}

// Keys отдаёт ключи коллекции по возрастанию: порядок обхода не зависит от
// того, как легли записи в карте.
func (s *Store) Keys(collection string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.doc.Records[collection]))
	for key := range s.doc.Records[collection] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Version отдаёт версию схемы, до которой доведён файл.
func (s *Store) Version() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doc.Version
}

// read читает файл. Отсутствие файла — не ошибка, а пустое хранилище.
func read(path string) (document, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return document{Records: Records{}}, true, nil
	}
	if err != nil {
		return document{}, false, fmt.Errorf("хранилище %s: %w", path, err)
	}

	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		return document{}, false, fmt.Errorf("хранилище %s: файл повреждён: %w", path, err)
	}
	return doc, false, nil
}

// save записывает документ целиком: временный файл в том же каталоге, сброс
// на диск, переименование. Сбой оставляет старую версию файла, а не половину.
func (s *Store) save() error {
	data, err := json.Marshal(s.doc)
	if err != nil {
		return fmt.Errorf("хранилище %s: %w", s.path, err)
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("хранилище %s: каталог: %w", s.path, err)
	}

	tmp, err := os.CreateTemp(dir, ".store-*")
	if err != nil {
		return fmt.Errorf("хранилище %s: временный файл: %w", s.path, err)
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("хранилище %s: запись: %w", s.path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("хранилище %s: сброс: %w", s.path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("хранилище %s: закрытие: %w", s.path, err)
	}
	if err := os.Rename(name, s.path); err != nil {
		return fmt.Errorf("хранилище %s: переименование: %w", s.path, err)
	}
	return nil
}
