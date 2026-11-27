package lang

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"go-wex/engine/core"
)

// Registrar — то, куда механизм объявляет свои маршруты. Узкий интерфейс:
// словарю нужна только регистрация запроса, остальные возможности реестра
// его не касаются.
type Registrar interface {
	HTTP(method, path string, h core.Handler)
}

// Register объявляет маршруты словаря:
//
//	GET /api/lang                     — базовый язык и список доступных
//	GET /api/lang/ru-RU               — все контексты языка
//	GET /api/lang/ru-RU/jobs,events   — только названные контексты
//
// Адрес выбран так, чтобы читался как путь к каталогу: видно, что за ним
// лежат файлы и что перечень контекстов — это перечень разделов.
// TODO: выбрать путь для внедрения no-cache. Сейчас простой маршрут отдаёт
// данные, а заголовки ставит движок, поэтому словарь не может попросить
// браузер сверяться с сервером. Чем это оборачивается: правка
// lang/ru-RU/jobs.json не доедет до открытой страницы — браузер возьмёт
// словарь по эвристической свежести и покажет старый текст до Ctrl+Shift+R,
// как было со статикой до правки 033259a. Пути: помечать no-cache все ответы
// модулей в route (одна строка, закрывает весь класс случаев) или завести
// способ отдавать заголовки из простого маршрута. Решать вместе с политикой
// кэширования ответов, а не внутри словаря.
func (c *Catalog) Register(r Registrar) {
	r.HTTP("GET", "/api/lang", c.list)
	r.HTTP("GET", "/api/lang/{locale}", c.loadAll)
	r.HTTP("GET", "/api/lang/{locale}/{contexts}", c.loadSome)
}

// list — что есть в каталоге. Без списка доступных языков интерфейс
// предлагал бы переключиться туда, где перевода нет.
func (c *Catalog) list(*http.Request) (any, error) {
	locales, err := c.Locales()
	if err != nil {
		return nil, err
	}
	return struct {
		Base    string   `json:"base"`
		Locales []Locale `json:"locales"`
	}{Base: c.base, Locales: locales}, nil
}

// loadAll отдаёт все контексты запрошенного языка.
func (c *Catalog) loadAll(r *http.Request) (any, error) {
	return c.load(r, nil)
}

// loadSome отдаёт названные контексты: перечень приходит в адресе через
// запятую, потому что это перечень файлов, а не отдельные значения.
func (c *Catalog) loadSome(r *http.Request) (any, error) {
	return c.load(r, splitContexts(r.PathValue("contexts")))
}

func (c *Catalog) load(r *http.Request, contexts []string) (any, error) {
	bundle, err := c.Load(r.PathValue("locale"), contexts)
	if err != nil {
		var bad *badRequest
		if errors.As(err, &bad) {
			return nil, &core.Error{Status: http.StatusBadRequest, Message: bad.Error()}
		}
		return nil, err
	}
	return bundle, nil
}

// splitContexts разбирает перечень из адреса: пустые куски отбрасываются,
// чтобы хвостовая запятая не считалась контекстом.
func splitContexts(value string) []string {
	out := make([]string, 0, 4)

	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

// badRequest — ошибка входа: так словарь отличает неверное имя языка или
// контекста (клиенту 400) от сломанного файла (клиенту 500).
type badRequest struct {
	message string
}

func (e *badRequest) Error() string { return e.message }

func badf(format string, args ...any) error {
	return &badRequest{message: fmt.Sprintf(format, args...)}
}
