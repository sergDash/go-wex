// Package health — пример модуля: один маршрут состояния сервера.
// Показывает минимальную форму модуля: объявить возможность и вернуть данные.
package health

import (
	"net/http"

	"go-wex/engine/core"
)

// module — модуль состояния; данных и служб не держит.
type module struct{}

// New отдаёт модуль для списка в точке сборки.
func New() core.Module { return module{} }

func (module) Name() string { return "health" }

// Register объявляет маршрут. Работу выполняет обработчик, а не регистрация.
func (module) Register(r core.Registry) error {
	r.HTTP("GET", "/api/health", handle)
	return nil
}

// health — снимок состояния сервера для стартовой страницы.
// Поля совпадают с именами, которые читает app.js, поэтому меняются вместе
// с интерфейсом.
type health struct {
	OK     bool   `json:"ok"`
	Status string `json:"status"`
}

func handle(*http.Request) (any, error) {
	return health{OK: true, Status: "сервер отвечает"}, nil
}
