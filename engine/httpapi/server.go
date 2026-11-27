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
	"runtime/debug"
	"time"

	"go-wex/engine/core"
	"go-wex/engine/journal"
)

// Пределы ожидания по умолчанию: медленный клиент не должен держать
// соединение и горутину бесконечно.
const (
	defaultReadHeaderTimeout = 5 * time.Second
	defaultIdleTimeout       = 60 * time.Second

	// defaultMaxBody — предел тела запроса по умолчанию: без предела один
	// клиент занимает память процесса. Загрузка файла поднимает предел
	// настройкой, а не отключает защиту молча.
	defaultMaxBody = 1 << 20
)

// Activity — учёт запросов: Begin отмечает начало и возвращает завершение.
// Счётчик видит запросы модулей; статика через него не проходит.
type Activity interface {
	Begin() func()
}

// Server — HTTP-служба: маршруты модулей и статика интерфейса.
type Server struct {
	addr     string
	mux      *http.ServeMux
	activity Activity
	journal  journal.Writer
	mw       func(http.Handler) http.Handler
	maxBody  int64

	readHeaderTimeout time.Duration
	idleTimeout       time.Duration

	httpSrv *http.Server
	ln      net.Listener
}

// New собирает HTTP-службу.
//
// addr — адрес из настроек; assets — корень статики: файловая система,
// в которой лежат index.html, app.js, style.css и vendor/. Каталог читается
// с диска, поэтому файлы правятся без пересборки бинарника.
func New(addr string, assets fs.FS) *Server {
	s := &Server{
		addr:              addr,
		mux:               http.NewServeMux(),
		readHeaderTimeout: defaultReadHeaderTimeout,
		idleTimeout:       defaultIdleTimeout,
		maxBody:           defaultMaxBody,
	}

	// Статика — самый общий образец пути; точные пути модулей выигрывают у него.
	s.mux.Handle("GET /", revalidate(http.FileServer(http.FS(assets))))
	return s
}

// revalidate просит браузер сверять каждый файл с сервером. Без заголовка
// браузер применяет эвристическую свежесть (доля от возраста файла) и берёт
// статику из кэша, не спрашивая сервер: правка interface/app.js не доезжает
// до открытой страницы. Выбран no-cache, а не no-store: файл кэшируется,
// а сверка опирается на Last-Modified от FileServer и стоит одного запроса
// с ответом 304, если файл не менялся.
func revalidate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		next.ServeHTTP(w, r)
	})
}

// Name — имя службы для группы.
func (s *Server) Name() string { return "http" }

// SetActivity подключает учёт запросов. Вызывается до Start: счётчик
// должен видеть запросы с первого соединения.
func (s *Server) SetActivity(a Activity) {
	s.activity = a
}

// SetJournal подключает журнал работы: в него пишутся паники обработчиков.
func (s *Server) SetJournal(j journal.Writer) {
	s.journal = j
}

// SetMiddleware задаёт посредника для всех запросов: проверку доступа,
// общие заголовки, ограничение частоты. Обёртка ставится над всем
// обработчиком, поэтому видит и статику, и маршруты. Вызывается до Start.
func (s *Server) SetMiddleware(mw func(http.Handler) http.Handler) {
	s.mw = mw
}

// SetMaxBody задаёт предел тела запроса в байтах. Ноль означает «без
// предела»: так бывает у маршрута загрузки, который сам решает, сколько
// принять. Без вызова действует предел по умолчанию.
func (s *Server) SetMaxBody(n int64) {
	s.maxBody = n
}

// SetTimeouts задаёт пределы ожидания на транспорте. Нулевое значение
// оставляет предел по умолчанию, отрицательное — снимает его.
func (s *Server) SetTimeouts(readHeader, idle time.Duration) {
	if readHeader != 0 {
		s.readHeaderTimeout = readHeader
	}
	if idle != 0 {
		s.idleTimeout = idle
	}
}

// writeJournal пишет событие и не мешает ответу, если журнал недоступен.
func (s *Server) writeJournal(kind string, fields map[string]any) {
	if s.journal == nil {
		return
	}
	if err := s.journal.Write("http", kind, fields); err != nil {
		log.Printf("журнал: %v", err)
	}
}

// withActivity оборачивает обработчик учётом запросов. Статика не
// оборачивается: загрузка страницы с десятками файлов — не действие человека,
// и по ней нельзя откладывать фоновую работу.
func (s *Server) withActivity(h http.Handler) http.Handler {
	if s.activity == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done := s.activity.Begin()
		defer done()
		h.ServeHTTP(w, r)
	})
}

// limitBody ограничивает тело запроса: без предела один клиент занимает
// память процесса. Закрывается тело самим обработчиком запроса, как обычно.
func (s *Server) limitBody(h http.Handler) http.Handler {
	if s.maxBody <= 0 {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, s.maxBody)
		h.ServeHTTP(w, r)
	})
}

// wrap применяет посредника ко всем запросам: и к статике, и к маршрутам.
func (s *Server) wrap(h http.Handler) http.Handler {
	if s.mw == nil {
		return h
	}
	return s.mw(h)
}

// Start открывает сокет и начинает обслуживание. Ошибка занятого адреса
// возвращается сразу, а не теряется в горутине.
func (s *Server) Start(context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("http: %w", err)
	}
	s.ln = ln

	// Учёт запросов уже внутри маршрутов модулей: статика его не проходит.
	// WriteTimeout не задаётся: он обрезал бы поток порций и скачивание,
	// у которых ответ не собирается заранее. Цена записана
	// в doc/capabilities/Недостатки.md.
	s.httpSrv = &http.Server{
		Handler:           s.wrap(s.mux),
		ReadHeaderTimeout: s.readHeaderTimeout,
		IdleTimeout:       s.idleTimeout,
	}
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

// HTTP регистрирует простой маршрут модуля. Обработчик возвращает данные,
// сервер сам кладёт их в конверт и выбирает код ответа.
func (s *Server) HTTP(method, path string, h core.Handler) {
	s.route(method, path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer s.recoverEnvelope(w, r)

		value, err := h(r)
		if err != nil {
			status, message, data := errorBody(err)
			if data != nil {
				writeErrorData(w, status, message, data)
				return
			}
			writeError(w, status, message)
			return
		}
		writeResult(w, http.StatusOK, value)
	}))
}

// HTTPRaw регистрирует маршрут, который пишет ответ сам: поток порций,
// скачивание файла, обновления в одном соединении. Конверт здесь не
// навязывается: ответ нельзя собрать заранее, а его форму выбирает модуль.
func (s *Server) HTTPRaw(method, path string, h http.Handler) {
	s.route(method, path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer s.recoverRaw(r)
		h.ServeHTTP(w, r)
	}))
}

// route собирает маршрут: предел тела, учёт запроса и обработчик. Посредник
// стоит выше и видит все запросы, включая статику.
func (s *Server) route(method, path string, h http.Handler) {
	s.mux.Handle(method+" "+path, s.withActivity(s.limitBody(h)))
}

// recoverEnvelope ловит панику обработчика простого маршрута: клиент получает
// конверт с 500, паника со стеком уходит в журнал, служба продолжает работу.
func (s *Server) recoverEnvelope(w http.ResponseWriter, r *http.Request) {
	v := recover()
	if v == nil {
		return
	}
	writeError(w, http.StatusInternalServerError, "внутренняя ошибка")
	s.writeJournal("panic", panicFields(r, v))
}

// recoverRaw ловит панику сырого маршрута. Ответ уже мог уйти клиенту
// частями, поэтому конверт не пишется: остаётся журнал.
func (s *Server) recoverRaw(r *http.Request) {
	v := recover()
	if v == nil {
		return
	}
	s.writeJournal("panic", panicFields(r, v))
}

// panicFields — поля события паники.
func panicFields(r *http.Request, v any) map[string]any {
	return map[string]any{
		"method": r.Method,
		"path":   r.URL.Path,
		"panic":  fmt.Sprint(v),
		"stack":  string(debug.Stack()),
	}
}

// errorBody разбирает ошибку обработчика: код ответа, текст и необязательные
// данные. Всё, что не названо через core.Error, считается внутренней ошибкой.
func errorBody(err error) (int, string, any) {
	// Предел тела — решение транспорта, а не модуля: обработчик просто
	// не смог прочитать запрос, и клиенту положен 413, а не 400.
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return http.StatusRequestEntityTooLarge, "тело запроса больше предела", nil
	}

	var e *core.Error
	if errors.As(err, &e) {
		status := e.Status
		if status == 0 {
			status = http.StatusInternalServerError
		}
		return status, e.Message, e.Data
	}
	return http.StatusInternalServerError, err.Error(), nil
}

// Handler отдаёт обработчик для проверок без запуска сокета.
func (s *Server) Handler() http.Handler { return s.wrap(s.mux) }
