// Пакет httpapi — HTTP-служба движка: общий конверт ответа, статика и
// регистрация маршрутов модулей. Предметных правил здесь нет.
package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"

	"go-wex/engine/core"
)

// Server — HTTP-служба: маршруты модулей и статика интерфейса.
type Server struct {
	addr string
	mux  *http.ServeMux

	httpSrv *http.Server
	ln      net.Listener
}

// New собирает HTTP-службу.
//
// addr — адрес из настроек; assets — корень статики: файловая система,
// в которой лежат index.html, app.js, style.css и vendor/. Каталог читается
// с диска, поэтому файлы правятся без пересборки бинарника.
func New(addr string, assets fs.FS) *Server {
	s := &Server{addr: addr, mux: http.NewServeMux()}

	// Статика — самый общий образец пути; точные пути модулей выигрывают у него.
	s.mux.Handle("GET /", http.FileServer(http.FS(assets)))
	return s
}

// Name — имя службы для группы.
func (s *Server) Name() string { return "http" }

// Start открывает сокет и начинает обслуживание. Ошибка занятого адреса
// возвращается сразу, а не теряется в горутине.
func (s *Server) Start(context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	s.ln = ln
	s.httpSrv = &http.Server{Handler: s.mux}
	go s.serve()
	return nil
}

// serve обслуживает запросы до закрытия сокета.
func (s *Server) serve() {
	err := s.httpSrv.Serve(s.ln)
	if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		log.Printf("HTTP-служба: %v", err)
	}
}

// Stop перестаёт принимать запросы и ждёт завершения активных.
func (s *Server) Stop(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

// Addr — фактический адрес сокета; удобен, когда в настройках порт 0.
func (s *Server) Addr() string {
	if s.ln == nil {
		return s.addr
	}
	return s.ln.Addr().String()
}

// HTTP регистрирует маршрут модуля. Обработчик возвращает данные, сервер сам
// кладёт их в конверт и выбирает код ответа.
func (s *Server) HTTP(method, path string, h core.Handler) {
	s.mux.HandleFunc(method+" "+path, func(w http.ResponseWriter, r *http.Request) {
		value, err := h(r)
		if err != nil {
			writeError(w, errorStatus(err), err.Error())
			return
		}
		writeResult(w, http.StatusOK, value)
	})
}

// errorStatus — код ответа по ошибке обработчика: модуль может назвать код
// через core.Error, остальные ошибки считаются внутренними.
func errorStatus(err error) int {
	var e *core.Error
	if errors.As(err, &e) && e.Status != 0 {
		return e.Status
	}
	return http.StatusInternalServerError
}

// Handler отдаёт обработчик для проверок без запуска сокета.
func (s *Server) Handler() http.Handler { return s.mux }
