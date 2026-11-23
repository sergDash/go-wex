package httpapi

import (
	"encoding/json"
	"log"
	"net/http"
)

// envelope — общая форма ответа API.
//
// Успех:   {"result": {...}}
// Ошибка:  {"error": "что именно не так"}
//
// Поля взаимоисключающие. Клиент разбирает ответ деструктуризацией:
// const { result, error } = body; — и сначала проверяет error, как в Go
// сначала проверяют err, а потом используют значение.
type envelope struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func writeResult(w http.ResponseWriter, status int, value any) {
	writeJSON(w, status, envelope{Result: value})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, envelope{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, body envelope) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		// Заголовки и часть тела уже могли уйти клиенту; остаётся журнал.
		log.Printf("запись JSON-ответа: %v", err)
	}
}
