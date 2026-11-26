// Package core — стандарт подключения модулей к ядру: форма модуля и реестр
// возможностей. Реализации возможностей живут в других пакетах движка.
package core

import (
	"context"
	"net/http"

	"go-wex/engine/events"
	"go-wex/engine/jobs"
	"go-wex/engine/service"
)

// Handler — обработчик запроса: возвращает данные для общего конверта или
// ошибку. Кодированием ответа занимается движок, а не модуль.
type Handler func(*http.Request) (any, error)

// CommandHandler — обработчик подкоманды: получает аргументы после её имени
// и выполняет одну работу, не поднимая службы.
type CommandHandler func(ctx context.Context, args []string) error

// Error — ошибка с кодом ответа. Модуль возвращает её, когда клиенту нужен
// не 500, а определённый код: 400 на неверный ввод, 404 на отсутствие.
//
// Data — необязательное тело ошибки: данные, по которым форма подсвечивает
// поля или показывает подсказку. Клиент получает их рядом с текстом, в поле
// data; форма ответа остаётся прежней.
//
// Err — причина: исходная ошибка, если модуль её обернул. По ней движок
// отличает предел тела от прочих отказов, а проверки пользуются errors.Is.
type Error struct {
	Status  int
	Message string
	Data    any
	Err     error
}

func (e *Error) Error() string { return e.Message }

// Unwrap отдаёт причину: цепочка ошибок не рвётся на границе модуля.
func (e *Error) Unwrap() error { return e.Err }

// Module — пакет, который подключается к ядру и объявляет свои возможности.
// Register не выполняет работу и не запускает службы: он только описывает,
// что модуль умеет.
type Module interface {
	Name() string
	Register(Registry) error
}

// Registry — то, через что модуль объявляет возможности. Набор методов растёт
// вместе с движком: HTTP, службы (в том числе слушатели), подкоманды, работы,
// отложенные события.
type Registry interface {
	HTTP(method, path string, h Handler)
	HTTPRaw(method, path string, h http.Handler)
	Job(kind string, spec jobs.Spec) error
	Event(kind string, rule events.Rule) error
	Service(s service.Service)
	Command(name string, run CommandHandler) error
}
