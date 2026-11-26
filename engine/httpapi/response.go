package httpapi

import (
	"encoding/json"
	"log"
	"net/http"
)

// envelope — общая форма ответа API.
//
// Успех:   {"result": {...}}
// Ошибка:  {"error": "что именно не так", "data": {...}}
//
// Поля result и error взаимоисключающие. data приходит только с ошибкой и
// только когда модуль её назвал: по этим данным форма подсвечивает поля.
// Клиент разбирает ответ деструктуризацией: const { result, error } = body;
// и сначала проверяет error, как в Go сначала проверяют err, а потом
// используют значение.
type envelope struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
	Data   any    `json:"data,omitempty"`
}

func writeResult(w http.ResponseWriter, status int, value any) {
	writeJSON(w, status, envelope{Result: value})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, envelope{Error: message})
}

// writeErrorData добавляет к тексту ошибки данные модуля: код и список полей
// для формы. Форма ответа та же, поэтому старые клиенты ничего не ломают.
func writeErrorData(w http.ResponseWriter, status int, message string, data any) {
	writeJSON(w, status, envelope{Error: message, Data: data})
}

func writeJSON(w http.ResponseWriter, status int, body envelope) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		// Заголовки и часть тела уже могли уйти клиенту; остаётся журнал.
		log.Printf("запись JSON-ответа: %v", err)
	}
}
