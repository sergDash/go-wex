package httpapi

import (
	"encoding/json"
	"log"
	"net/http"
)

// healthResponse — снимок состояния сервера для стартовой страницы.
// Поля совпадают с именами, которые читает app.js, поэтому менять их
// нужно вместе с фронтендом.
type healthResponse struct {
	OK     bool   `json:"ok"`
	Status string `json:"status"`
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	response := healthResponse{
		OK:     true,
		Status: "сервер отвечает",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		// Заголовок и часть тела уже могли уйти клиенту; остаётся записать
		// причину в журнал.
		log.Printf("ответ /api/health: %v", err)
	}
}
