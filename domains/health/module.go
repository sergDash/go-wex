// Package health — пример модуля: один маршрут состояния сервера.
// Показывает минимальную форму модуля: объявить возможность и вернуть данные.
package health

import (
	"encoding/json"
	"fmt"
	"net/http"

	"go-wex/engine/config"
	"go-wex/engine/core"
	"go-wex/engine/version"
)

// module — модуль состояния; данных и служб не держит.
type module struct{}

// New отдаёт модуль для списка в точке сборки. settings — раздел
// modules.settings.health: своих настроек у модуля нет, поэтому любой ключ
// в нём — ошибка, а не тишина.
func New(settings json.RawMessage) (core.Module, error) {
	var none struct{}
	if err := config.DecodeSection(settings, &none); err != nil {
		return nil, fmt.Errorf("health: настройки: %w", err)
	}
	return module{}, nil
}

func (module) Name() string { return "health" }

// Register объявляет маршрут. Работу выполняет обработчик, а не регистрация.
func (module) Register(r core.Registry) error {
	r.HTTP("GET", "/api/health", handle)
	return nil
}

// health — снимок состояния сервера для стартовой страницы.
// Поля совпадают с именами, которые читает app.js, поэтому меняются вместе
// с интерфейсом. Version — версия сборки: по ней видно, какой код запущен.
type health struct {
	OK      bool   `json:"ok"`
	Status  string `json:"status"`
	Version string `json:"version"`
}

func handle(*http.Request) (any, error) {
	return health{OK: true, Status: "сервер ответил", Version: version.String()}, nil
}
