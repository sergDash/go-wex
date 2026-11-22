package httpapi

import "net/http"

// health — снимок состояния сервера для стартовой страницы.
// Поля совпадают с именами, которые читает app.js, поэтому меняются
// вместе с фронтендом.
type health struct {
	OK     bool   `json:"ok"`
	Status string `json:"status"`
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	writeResult(w, http.StatusOK, health{
		OK:     true,
		Status: "сервер отвечает",
	})
}
