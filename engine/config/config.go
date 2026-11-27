// Package config — настройки приложения из файлов config/<предмет>.json.
// Здесь настройки HTTP-службы (адрес и каталог интерфейса), путь к файлу
// хранилища, пороги простоя и журнал работы. Проверка идёт при загрузке:
// неизвестный ключ и пустое обязательное значение считаются ошибкой, чтобы
// приложение не работало с половиной настроек.
package config

import (
	"bytes"
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
	Lang      *Lang     `json:"lang"`
	Store     Store     `json:"store"`
	Jobs      Jobs      `json:"jobs"`
	Services  Services  `json:"services"`
	Schedule  Schedule  `json:"schedule"`
	Modules   Modules   `json:"modules"`
	Journal   *Journal  `json:"journal"`
}

// Значения по умолчанию для раздела lang: каталог словарей лежит в корне
// проекта, базовый язык — русский. Пустой раздел означает «включено
// с умолчаниями»: переводы нужны почти всегда, а путь и язык — нет.
const (
	DefaultLangDir  = "lang"
	DefaultLangBase = "ru-RU"
)

// Lang — словари переводов: каталог с локалями и базовая локаль, на которую
// опирается запасной путь. Отсутствие раздела означает, что переводов нет
// и механизм не подключается: приложению без интерфейса он не нужен.
type Lang struct {
	Dir  string `json:"dir"`
	Base string `json:"base"`
}

// Server — настройки адреса HTTP-службы. Open — открыть страницу
// в браузере после запуска: удобно при разработке, на сервере без рабочего
// стола выключается.
//
// ReadHeaderTimeout и IdleTimeout — пределы ожидания на транспорте: медленный
// клиент не должен держать соединение бесконечно. Пустое значение оставляет
// предел службы по умолчанию. MaxBody — предел тела запроса в байтах; ноль
// означает, что предела нет.
type Server struct {
	Addr              string `json:"addr"`
	Open              bool   `json:"open"`
	ReadHeaderTimeout string `json:"readHeaderTimeout"`
	IdleTimeout       string `json:"idleTimeout"`
	MaxBody           *int64 `json:"maxBody"`
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

// Services — правила для внутренних служб.
type Services struct {
	Restart Restart `json:"restart"`
}

// Restart — правило перезапуска отказавшей службы: сколько ждать перед
// повтором и сколько раз пробовать. Max == 0 означает, что перезапуска нет:
// группа только сообщает об отказе.
type Restart struct {
	After string `json:"after"`
	Max   int    `json:"max"`
}

// Jobs — правило хранения очереди: сколько завершённых строк держать.
// Ноль оставляет все: сколько хранить, решает проект по объёму.
type Jobs struct {
	Keep int `json:"keep"`
}

// Modules — какие модули включать и с какими настройками. Список Disabled
// перечисляет выключенные; пустой список означает, что работают все.
// Выключенный модуль не объявляет ничего: ни маршрутов, ни работ, ни событий,
// ни служб. Settings — разделы настроек по имени модуля; форму раздела знает
// только сам модуль, поэтому точка сборки передаёт его как есть.
type Modules struct {
	Disabled []string                   `json:"disabled"`
	Settings map[string]json.RawMessage `json:"settings"`
}

// DecodeSection читает раздел настроек модуля так же строго, как настройки
// приложения: неизвестный ключ — ошибка, а не тишина. Пустой раздел означает
// «настроек нет», и модуль подставляет умолчания.
func DecodeSection(raw json.RawMessage, into any) error {
	if len(raw) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(into)
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
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return HTTPAPI{}, fmt.Errorf("настройки %s: %w", path, err)
	}
	return c, nil
}

// applyDefaults заполняет необязательные значения раздела lang. Раздел
// присутствует — значит переводы включены; пустые поля получают умолчания,
// чтобы файл настроек не повторял путь к каталогу в каждом проекте.
func (c *HTTPAPI) applyDefaults() {
	if c.Lang == nil {
		return
	}
	if strings.TrimSpace(c.Lang.Dir) == "" {
		c.Lang.Dir = DefaultLangDir
	}
	if strings.TrimSpace(c.Lang.Base) == "" {
		c.Lang.Base = DefaultLangBase
	}
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
	if c.Jobs.Keep < 0 {
		return errors.New("jobs.keep не может быть отрицательным")
	}
	if c.Services.Restart.Max < 0 {
		return errors.New("services.restart.max не может быть отрицательным")
	}
	if c.Services.Restart.After != "" {
		d, err := time.ParseDuration(c.Services.Restart.After)
		if err != nil {
			return fmt.Errorf("services.restart.after: %w", err)
		}
		if d < 0 {
			return errors.New("services.restart.after не может быть отрицательным")
		}
	}
	for i, r := range c.Schedule.Rules {
		if _, err := time.ParseDuration(r.After); err != nil {
			return fmt.Errorf("schedule.rules[%d].after: %w", i, err)
		}
		if !jobs.Class(r.Class).Valid() {
			return fmt.Errorf("schedule.rules[%d].class: неизвестный класс %q", i, r.Class)
		}
	}
	if err := checkDuration("server.readHeaderTimeout", c.Server.ReadHeaderTimeout); err != nil {
		return err
	}
	if err := checkDuration("server.idleTimeout", c.Server.IdleTimeout); err != nil {
		return err
	}
	if c.Server.MaxBody != nil && *c.Server.MaxBody < 0 {
		return errors.New("server.maxBody не может быть отрицательным")
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

// checkDuration проверяет необязательный срок: пустое значение означает
// значение по умолчанию, а ноль и отрицательное — ошибка.
func checkDuration(name, value string) error {
	if value == "" {
		return nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if d <= 0 {
		return fmt.Errorf("%s: срок должен быть больше нуля", name)
	}
	return nil
}
