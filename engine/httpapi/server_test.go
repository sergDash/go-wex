package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"go-wex/engine/core"
)

// TestRegisterAndEncode — модуль объявляет маршрут, движок кодирует ответ
// в общий конверт. Ошибка обработчика даёт 500 и поле error.
func TestRegisterAndEncode(t *testing.T) {
	s := New("127.0.0.1:0", fstest.MapFS{})
	s.HTTP("GET", "/api/ok", func(*http.Request) (any, error) {
		return map[string]bool{"ok": true}, nil
	})
	s.HTTP("GET", "/api/fail", func(*http.Request) (any, error) {
		return nil, errors.New("отказ")
	})
	s.HTTP("GET", "/api/missing", func(*http.Request) (any, error) {
		return nil, &core.Error{Status: http.StatusNotFound, Message: "нет такой вещи"}
	})

	okRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(okRec, httptest.NewRequest(http.MethodGet, "/api/ok", nil))
	if okRec.Code != http.StatusOK {
		t.Fatalf("успех: код %d", okRec.Code)
	}
	var okBody struct {
		Result map[string]bool `json:"result"`
	}
	if err := json.Unmarshal(okRec.Body.Bytes(), &okBody); err != nil {
		t.Fatalf("разбор успеха: %v", err)
	}
	if !okBody.Result["ok"] {
		t.Fatalf("успех: тело %s", okRec.Body.String())
	}

	failRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(failRec, httptest.NewRequest(http.MethodGet, "/api/fail", nil))
	if failRec.Code != http.StatusInternalServerError {
		t.Fatalf("отказ: код %d", failRec.Code)
	}
	var failBody struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(failRec.Body.Bytes(), &failBody); err != nil {
		t.Fatalf("разбор отказа: %v", err)
	}
	if failBody.Error == "" {
		t.Fatalf("отказ без текста: %s", failRec.Body.String())
	}

	missingRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(missingRec, httptest.NewRequest(http.MethodGet, "/api/missing", nil))
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("ошибка с кодом: получили %d", missingRec.Code)
	}
}

// TestStartStop — служба занимает свободный порт, отвечает и освобождает
// порт при остановке.
func TestStartStop(t *testing.T) {
	s := New("127.0.0.1:0", fstest.MapFS{})
	s.HTTP("GET", "/api/ping", func(*http.Request) (any, error) {
		return "pong", nil
	})

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	addr := s.Addr()
	resp, err := http.Get("http://" + addr + "/api/ping")
	if err != nil {
		t.Fatalf("запрос: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("код: %d", resp.StatusCode)
	}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
