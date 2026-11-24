// Package core — стандарт подключения модулей к ядру: форма модуля и реестр
// возможностей. Реализации возможностей живут в других пакетах движка.
package core

import (
	"net/http"

	"go-wex/engine/jobs"
)

// Handler — обработчик запроса: возвращает данные для общего конверта или
// ошибку. Кодированием ответа занимается движок, а не модуль.
type Handler func(*http.Request) (any, error)

// Error — ошибка с кодом ответа. Модуль возвращает её, когда клиенту нужен
// не 500, а определённый код: 400 на неверный ввод, 404 на отсутствие.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return e.Message }

// Module — пакет, который подключается к ядру и объявляет свои возможности.
// Register не выполняет работу и не запускает службы: он только описывает,
// что модуль умеет.
type Module interface {
	Name() string
	Register(Registry) error
}

// Registry — то, через что модуль объявляет возможности. Набор методов растёт
// вместе с движком: HTTP, службы (в том числе слушатели), подкоманды, работы.
type Registry interface {
	HTTP(method, path string, h Handler)
	Job(kind string, spec jobs.Spec) error
}
