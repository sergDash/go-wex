// Package logru — модуль-пример: запись в журнал работы текстом из словаря.
//
// Показывает, что словарь переводов — механизм, а не принадлежность страницы:
// ту же фразу берёт серверный код, и в журнал попадает ровно то, что видно
// на экране. Прикладному проекту это нужно для записей и текстов, которые
// уходят другому получателю: язык выбирается один раз, а не в каждой строке.
package logru

import (
	"encoding/json"
	"fmt"
	"net/http"

	"go-wex/engine/config"
	"go-wex/engine/core"
	"go-wex/engine/journal"
	"go-wex/engine/lang"
)

// contextName — контекст словаря, откуда модуль берёт фразы: файл
// lang/<язык>/log_ru.json. Имя модуля и имя контекста совпадают намеренно —
// так видно, чей это файл, и не нужно второй таблицы соответствий.
const contextName = "log_ru"

// module держит словарь и журнал. Язык записей — базовый: выбор языка
// запуска появится вместе с настройкой, отдельной от интерфейса.
type module struct {
	texts   *lang.Catalog
	journal journal.Writer
	locale  string
}

// New собирает модуль. Словарь обязателен: без него модулю нечего писать,
// и лучше сказать об этом при запуске, чем пустой строкой в журнале.
func New(catalog *lang.Catalog, j journal.Writer, settings json.RawMessage) (core.Module, error) {
	var none struct{}
	if err := config.DecodeSection(settings, &none); err != nil {
		return nil, fmt.Errorf("log_ru: настройки: %w", err)
	}
	if catalog == nil {
		return nil, fmt.Errorf("log_ru: нужен раздел lang в настройках: модуль пишет фразы из словаря")
	}
	return module{texts: catalog, journal: j, locale: catalog.Base()}, nil
}

func (module) Name() string { return "log_ru" }

// Register объявляет запись. Работу выполняет обработчик, а не регистрация.
func (m module) Register(r core.Registry) error {
	r.HTTP("POST", "/api/log_ru/write", m.write)
	return nil
}

// written — что записано и на каком языке. Поля совпадают с именами
// в app.js: текст показывается как есть, язык — рядом, чтобы было видно
// источник фразы.
type written struct {
	Text string `json:"text"`
	Lang string `json:"lang"`
}

// write берёт фразу из словаря, пишет её в журнал и отдаёт клиенту.
// Одна и та же строка идёт в две стороны: в файл журнала и в ответ, поэтому
// расхождение между ними заметно сразу.
func (m module) write(*http.Request) (any, error) {
	text, err := m.phrase("written")
	if err != nil {
		return nil, err
	}

	if m.journal != nil {
		fields := map[string]any{"text": text, "lang": m.locale}
		if err := m.journal.Write("log_ru", "written", fields); err != nil {
			return nil, fmt.Errorf("log_ru: журнал: %w", err)
		}
	}
	return written{Text: text, Lang: m.locale}, nil
}

// phrase читает ключ из контекста модуля. Отсутствие ключа — ошибка, а не
// пустая строка: запись без текста в журнале бесполезна.
func (m module) phrase(key string) (string, error) {
	bundle, err := m.texts.Load(m.locale, []string{contextName})
	if err != nil {
		return "", fmt.Errorf("log_ru: словарь: %w", err)
	}

	value := bundle.Texts[contextName][key]
	if value == "" {
		return "", fmt.Errorf("log_ru: ключ %s.%s не найден", contextName, key)
	}
	return value, nil
}
