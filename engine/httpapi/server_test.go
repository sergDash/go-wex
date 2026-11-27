package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

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

// activityFake — учёт запросов для проверки: считает начало и завершение.
type activityFake struct {
	begins int
	ends   int
}

func (a *activityFake) Begin() func() {
	a.begins++
	return func() { a.ends++ }
}

// TestActivityCounts — каждый запрос проходит через учёт, включая
// обработчик маршрута.
func TestActivityCounts(t *testing.T) {
	s := New("127.0.0.1:0", fstest.MapFS{})
	a := &activityFake{}
	s.SetActivity(a)
	s.HTTP("GET", "/api/check", func(*http.Request) (any, error) { return "ok", nil })

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/check", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("код: %d", rec.Code)
	}
	if a.begins != 1 || a.ends != 1 {
		t.Fatalf("учёт: начал=%d завершил=%d", a.begins, a.ends)
	}
}

// TestActivityIgnoresStatic — загрузка статики не считается активностью:
// иначе открытая вкладка откладывала бы фоновую работу навсегда.
func TestActivityIgnoresStatic(t *testing.T) {
	assets := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("панель")}}
	s := New("127.0.0.1:0", assets)
	a := &activityFake{}
	s.SetActivity(a)

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("статика: код %d", rec.Code)
	}
	if a.begins != 0 || a.ends != 0 {
		t.Fatalf("статика прошла через учёт: начал=%d завершил=%d", a.begins, a.ends)
	}

	s.HTTP("GET", "/api/check", func(*http.Request) (any, error) { return "ok", nil })
	apiRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(apiRec, httptest.NewRequest(http.MethodGet, "/api/check", nil))
	if a.begins != 1 || a.ends != 1 {
		t.Fatalf("маршрут модуля: начал=%d завершил=%d", a.begins, a.ends)
	}
}

// TestStaticRevalidate — статика помечается no-cache и сверяется с сервером:
// правка файла видна на открытой странице без сброса кэша браузера.
func TestStaticRevalidate(t *testing.T) {
	modTime := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	assets := fstest.MapFS{
		"app.js": &fstest.MapFile{Data: []byte("панель"), ModTime: modTime},
	}
	s := New("127.0.0.1:0", assets)

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/app.js", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("статика: код %d", rec.Code)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control: %q", got)
	}
	// Эту же метку клиент присылает обратно при сверке.
	if got := rec.Header().Get("Last-Modified"); got == "" {
		t.Fatalf("нет Last-Modified: сверять нечего")
	}

	recheck := httptest.NewRequest(http.MethodGet, "/app.js", nil)
	recheck.Header.Set("If-Modified-Since", modTime.UTC().Format(http.TimeFormat))
	recheckRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(recheckRec, recheck)
	if recheckRec.Code != http.StatusNotModified {
		t.Fatalf("сверка: код %d", recheckRec.Code)
	}
}

// journalFake — журнал для проверки: запоминает записи.
type journalFake struct {
	sources []string
	kinds   []string
	fields  []map[string]any
}

func (j *journalFake) Write(source, kind string, fields map[string]any) error {
	j.sources = append(j.sources, source)
	j.kinds = append(j.kinds, kind)
	j.fields = append(j.fields, fields)
	return nil
}

// TestPanicRecovered — паника обработчика даёт 500 в конверте, попадает
// в журнал, а служба продолжает обслуживать запросы.
func TestPanicRecovered(t *testing.T) {
	s := New("127.0.0.1:0", fstest.MapFS{})
	j := &journalFake{}
	s.SetJournal(j)
	s.HTTP("GET", "/api/panic", func(*http.Request) (any, error) {
		panic("сломано")
	})
	s.HTTP("GET", "/api/ok", func(*http.Request) (any, error) { return "ok", nil })

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/panic", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("паника: код %d", rec.Code)
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("разбор ответа: %v", err)
	}
	if body.Error == "" {
		t.Fatalf("ответ без ошибки: %s", rec.Body.String())
	}

	if len(j.kinds) != 1 || j.kinds[0] != "panic" || j.sources[0] != "http" {
		t.Fatalf("журнал: %+v", j)
	}
	if j.fields[0]["path"] != "/api/panic" {
		t.Fatalf("поле пути: %+v", j.fields[0])
	}

	okRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(okRec, httptest.NewRequest(http.MethodGet, "/api/ok", nil))
	if okRec.Code != http.StatusOK {
		t.Fatalf("после паники: код %d", okRec.Code)
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

// TestRawRoute — сырой маршрут пишет ответ сам: конверт не добавляется,
// поэтому поток и скачивание возможны.
func TestRawRoute(t *testing.T) {
	s := New("127.0.0.1:0", fstest.MapFS{})
	s.HTTPRaw("GET", "/api/stream", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("порция"))
	}))

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/stream", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("код: %d", rec.Code)
	}
	if body := rec.Body.String(); body != "порция" {
		t.Fatalf("тело: %q", body)
	}
}

// TestErrorData — модуль называет данные ошибки, движок кладёт их рядом
// с текстом: по ним форма подсвечивает поля.
func TestErrorData(t *testing.T) {
	s := New("127.0.0.1:0", fstest.MapFS{})
	s.HTTP("POST", "/api/form", func(*http.Request) (any, error) {
		return nil, &core.Error{
			Status:  http.StatusBadRequest,
			Message: "text обязателен",
			Data:    map[string]any{"code": "text_required", "fields": []string{"text"}},
		}
	})

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/form", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("код: %d", rec.Code)
	}

	var body struct {
		Error string `json:"error"`
		Data  struct {
			Code   string   `json:"code"`
			Fields []string `json:"fields"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("разбор: %v", err)
	}
	if body.Error == "" || body.Data.Code != "text_required" || len(body.Data.Fields) != 1 {
		t.Fatalf("тело ошибки: %s", rec.Body.String())
	}
}

// TestMiddleware — посредник стоит над всеми запросами: без пропуска
// обработчик модуля не вызывается, статика тоже закрыта.
func TestMiddleware(t *testing.T) {
	assets := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("панель")}}
	s := New("127.0.0.1:0", assets)

	called := false
	s.HTTP("GET", "/api/check", func(*http.Request) (any, error) {
		called = true
		return "ok", nil
	})
	s.SetMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer token" {
				http.Error(w, "нет доступа", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	})

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/check", nil))
	if rec.Code != http.StatusUnauthorized || called {
		t.Fatalf("без пропуска: код %d, обработчик вызван=%v", rec.Code, called)
	}

	staticRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(staticRec, httptest.NewRequest(http.MethodGet, "/", nil))
	if staticRec.Code != http.StatusUnauthorized {
		t.Fatalf("статика без пропуска: код %d", staticRec.Code)
	}

	passRec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/check", nil)
	req.Header.Set("Authorization", "Bearer token")
	s.Handler().ServeHTTP(passRec, req)
	if passRec.Code != http.StatusOK || !called {
		t.Fatalf("с пропуском: код %d, обработчик вызван=%v", passRec.Code, called)
	}
}

// TestBodyLimit — предел тела закрывает запрос раньше, чем он займёт память.
func TestBodyLimit(t *testing.T) {
	s := New("127.0.0.1:0", fstest.MapFS{})
	s.SetMaxBody(4)
	s.HTTPRaw("POST", "/api/raw", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/raw", strings.NewReader("0123456789"))
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("код: %d", rec.Code)
	}
}

// TestBodyLimitStatus — предел тела виден клиенту как 413, даже если модуль
// вернул ошибку чтения как обычную.
func TestBodyLimitStatus(t *testing.T) {
	s := New("127.0.0.1:0", fstest.MapFS{})
	s.SetMaxBody(4)
	s.HTTP("POST", "/api/form", func(r *http.Request) (any, error) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			// Так модуль оборачивает ошибку чтения: причина остаётся в цепочке.
			return nil, &core.Error{Status: http.StatusBadRequest, Message: "тело: " + err.Error(), Err: err}
		}
		return body, nil
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/form", strings.NewReader(`{"text":"длинно"}`))
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("код: %d, тело %s", rec.Code, rec.Body.String())
	}
}
