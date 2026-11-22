// Пакет httpapi содержит маршруты HTTP и формирование JSON-ответов.
// Фронтенд получает данные только через эти обработчики; доменных правил
// здесь быть не должно — они живут в отдельных пакетах internal.
package httpapi

import (
	"io/fs"
	"net/http"
)

// New собирает маршруты приложения.
//
// assets — корень статики: файловая система, в которой лежат index.html,
// app.js, style.css и vendor/. Это может быть встроенная копия или каталог
// на диске; маршруты от этого не зависят.
func New(assets fs.FS) http.Handler {
	mux := http.NewServeMux()

	// API регистрируется до статики: более точный образец пути выигрывает
	// у общего "GET /".
	mux.HandleFunc("GET /api/health", handleHealth)

	// Всё остальное — статические файлы фронтенда.
	mux.Handle("GET /", http.FileServer(http.FS(assets)))

	return mux
}
