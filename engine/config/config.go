// Package config — настройки приложения из файлов config/<предмет>.json.
// Здесь настройки HTTP-службы (адрес и каталог интерфейса), путь к файлу
// хранилища, пороги простоя и журнал работы. Проверка идёт при загрузке:
// неизвестный ключ и пустое обязательное значение считаются ошибкой, чтобы
// приложение не работало с половиной настроек.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go-wex/engine/jobs"
)

// HTTPAPI — настройки HTTP-службы.
//
// Server.Addr — адрес, который занимает служба. Interface.Dir — каталог
// статики, откуда читаются файлы интерфейса. Store.Path — файл хранилища,
// в котором очередь переживает перезапуск. Schedule.Rules — пороги простоя.
type HTTPAPI struct {
	Server    Server    `json:"server"`
	Interface Interface `json:"interface"`
	Store     Store     `json:"store"`
	Schedule  Schedule  `json:"schedule"`
	Journal   *Journal  `json:"journal"`
}

// Server — настройки адреса HTTP-службы. Open — открыть страницу
// в браузере после запуска: удобно при разработке, на сервере без рабочего
// стола выключается.
type Server struct {
	Addr string `json:"addr"`
	Open bool   `json:"open"`
}

// Interface — настройки каталога статики.
type Interface struct {
	Dir string `json:"dir"`
}

// Store — настройки файла хранилища.
type Store struct {
	Path string `json:"path"`
}

// Schedule — пороги простоя: после какого простоя разрешён класс работы.
// Пустой список означает, что расписание не задаёт ограничений.
type Schedule struct {
	Rules []ScheduleRule `json:"rules"`
}

// ScheduleRule — один порог: срок в виде duration и класс работы.
type ScheduleRule struct {
	After string `json:"after"`
	Class string `json:"class"`
}

// Journal — настройки журнала работы. Отсутствие раздела отключает журнал:
// движок пишет только в обычный журнал процесса.
type Journal struct {
	Path    string `json:"path"`
	MaxSize int64  `json:"maxSize"`
	Keep    int    `json:"keep"`
}

// LoadHTTPAPI читает настройки HTTP-службы по пути path. Неизвестные ключи
// запрещены: опечатка в имени настройки должна давать ошибку, а не тихо
// игнорироваться.
func LoadHTTPAPI(path string) (HTTPAPI, error) {
	f, err := os.Open(path)
	if err != nil {
		return HTTPAPI{}, fmt.Errorf("настройки: %w", err)
	}
	defer f.Close()

	var c HTTPAPI
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return HTTPAPI{}, fmt.Errorf("настройки %s: %w", path, err)
	}
	if err := c.validate(); err != nil {
		return HTTPAPI{}, fmt.Errorf("настройки %s: %w", path, err)
	}
	return c, nil
}

// validate проверяет обязательные значения. Список короткий: адрес сервера,
// каталог интерфейса и путь к хранилищу, без которых приложение не запустится.
func (c HTTPAPI) validate() error {
	if strings.TrimSpace(c.Server.Addr) == "" {
		return errors.New("server.addr обязателен")
	}
	if strings.TrimSpace(c.Interface.Dir) == "" {
		return errors.New("interface.dir обязателен")
	}
	if strings.TrimSpace(c.Store.Path) == "" {
		return errors.New("store.path обязателен")
	}
	for i, r := range c.Schedule.Rules {
		if _, err := time.ParseDuration(r.After); err != nil {
			return fmt.Errorf("schedule.rules[%d].after: %w", i, err)
		}
		if !jobs.Class(r.Class).Valid() {
			return fmt.Errorf("schedule.rules[%d].class: неизвестный класс %q", i, r.Class)
		}
	}
	if c.Journal != nil {
		if strings.TrimSpace(c.Journal.Path) == "" {
			return errors.New("journal.path обязателен")
		}
		if c.Journal.MaxSize <= 0 {
			return errors.New("journal.maxSize должен быть больше нуля")
		}
		if c.Journal.Keep <= 0 {
			return errors.New("journal.keep должен быть больше нуля")
		}
	}
	return nil
}
