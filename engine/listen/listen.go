// Package listen — слушатели TCP и UDP как внутренние службы. Слушатель
// открывает сокет в Start, принимает соединения или датаграммы, а в Stop
// закрывает сокет и дожидается обработчиков. Паника обработчика не роняет
// службу и не мешает остальным соединениям.
package listen

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"sync"

	"go-wex/engine/journal"
)

// ConnHandler — обработчик одного соединения. Контекст отменяется при
// остановке службы; соединение закрывает служба после возврата.
type ConnHandler func(context.Context, net.Conn) error

// PacketHandler — обработчик одной датаграммы. Соединение нужно, чтобы
// ответить отправителю.
type PacketHandler func(context.Context, net.PacketConn, []byte, net.Addr) error

// TCP — служба TCP-слушателя.
type TCP struct {
	addr    string
	handler ConnHandler
	journal journal.Writer

	mu         sync.Mutex
	ln         net.Listener
	cancel     context.CancelFunc
	acceptDone chan struct{}
	wg         sync.WaitGroup
}

// NewTCP собирает TCP-службу. Адрес с портом 0 занимает свободный порт,
// фактический адрес отдаёт Addr.
func NewTCP(addr string, h ConnHandler, j journal.Writer) *TCP {
	if j == nil {
		j = journal.Discard{}
	}
	return &TCP{addr: addr, handler: h, journal: j}
}

// Name — имя службы для группы.
func (s *TCP) Name() string { return "tcp " + s.addr }

// Start открывает сокет и начинает принимать соединения.
func (s *TCP) Start(ctx context.Context) error {
	if s.handler == nil {
		return errors.New("слушатель TCP: обработчик не задан")
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("слушатель TCP %s: %w", s.addr, err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	s.mu.Lock()
	s.ln = ln
	s.cancel = cancel
	s.acceptDone = done
	s.mu.Unlock()

	go s.accept(runCtx, ln, done)
	return nil
}

// Stop закрывает сокет, отменяет обработчики и ждёт их завершения.
func (s *TCP) Stop(ctx context.Context) error {
	s.mu.Lock()
	ln, cancel, done := s.ln, s.cancel, s.acceptDone
	s.ln, s.cancel, s.acceptDone = nil, nil, nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if ln != nil {
		_ = ln.Close()
	}
	if done == nil {
		return nil
	}

	// Сначала цикл приёма, потом обработчики: после закрытия done новые
	// обработчики не добавляются, и Wait не спорит с Add.
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return wait(&s.wg, ctx)
}

// Addr — фактический адрес сокета; удобен, когда в настройках порт 0.
func (s *TCP) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return s.addr
	}
	return s.ln.Addr().String()
}

// accept принимает соединения до закрытия сокета.
func (s *TCP) accept(ctx context.Context, ln net.Listener, done chan struct{}) {
	defer close(done)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.writeJournal("accept", map[string]any{
				"network": "tcp",
				"addr":    s.addr,
				"error":   err.Error(),
			})
			return
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			s.serve(ctx, conn)
		}()
	}
}

// serve выполняет обработчик и перехватывает его панику.
func (s *TCP) serve(ctx context.Context, conn net.Conn) {
	defer func() {
		if v := recover(); v != nil {
			s.writeJournal("panic", map[string]any{
				"network": "tcp",
				"addr":    s.addr,
				"remote":  conn.RemoteAddr().String(),
				"panic":   fmt.Sprint(v),
				"stack":   string(debug.Stack()),
			})
		}
	}()

	if err := s.handler(ctx, conn); err != nil {
		s.writeJournal("handler", map[string]any{
			"network": "tcp",
			"addr":    s.addr,
			"remote":  conn.RemoteAddr().String(),
			"error":   err.Error(),
		})
	}
}

func (s *TCP) writeJournal(kind string, fields map[string]any) {
	writeJournal(s.journal, kind, fields)
}

// UDP — служба UDP-слушателя.
type UDP struct {
	addr    string
	handler PacketHandler
	journal journal.Writer

	mu       sync.Mutex
	pc       net.PacketConn
	cancel   context.CancelFunc
	readDone chan struct{}
	wg       sync.WaitGroup
}

// NewUDP собирает UDP-службу. Адрес с портом 0 занимает свободный порт,
// фактический адрес отдаёт Addr.
func NewUDP(addr string, h PacketHandler, j journal.Writer) *UDP {
	if j == nil {
		j = journal.Discard{}
	}
	return &UDP{addr: addr, handler: h, journal: j}
}

// Name — имя службы для группы.
func (s *UDP) Name() string { return "udp " + s.addr }

// Start открывает сокет и начинает принимать датаграммы.
func (s *UDP) Start(ctx context.Context) error {
	if s.handler == nil {
		return errors.New("слушатель UDP: обработчик не задан")
	}
	pc, err := net.ListenPacket("udp", s.addr)
	if err != nil {
		return fmt.Errorf("слушатель UDP %s: %w", s.addr, err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	s.mu.Lock()
	s.pc = pc
	s.cancel = cancel
	s.readDone = done
	s.mu.Unlock()

	go s.read(runCtx, pc, done)
	return nil
}

// Stop закрывает сокет, отменяет обработчики и ждёт их завершения.
func (s *UDP) Stop(ctx context.Context) error {
	s.mu.Lock()
	pc, cancel, done := s.pc, s.cancel, s.readDone
	s.pc, s.cancel, s.readDone = nil, nil, nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if pc != nil {
		_ = pc.Close()
	}
	if done == nil {
		return nil
	}

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return wait(&s.wg, ctx)
}

// Addr — фактический адрес сокета.
func (s *UDP) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pc == nil {
		return s.addr
	}
	return s.pc.LocalAddr().String()
}

// read принимает датаграммы до закрытия сокета. Буфер копируется: следующее
// чтение перезапишет его, а обработчик может выполняться параллельно.
func (s *UDP) read(ctx context.Context, pc net.PacketConn, done chan struct{}) {
	defer close(done)

	buf := make([]byte, 64*1024)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.writeJournal("read", map[string]any{
				"network": "udp",
				"addr":    s.addr,
				"error":   err.Error(),
			})
			return
		}

		data := append([]byte(nil), buf[:n]...)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serve(ctx, pc, data, from)
		}()
	}
}

// serve выполняет обработчик и перехватывает его панику.
func (s *UDP) serve(ctx context.Context, pc net.PacketConn, data []byte, from net.Addr) {
	defer func() {
		if v := recover(); v != nil {
			s.writeJournal("panic", map[string]any{
				"network": "udp",
				"addr":    s.addr,
				"remote":  from.String(),
				"panic":   fmt.Sprint(v),
				"stack":   string(debug.Stack()),
			})
		}
	}()

	if err := s.handler(ctx, pc, data, from); err != nil {
		s.writeJournal("handler", map[string]any{
			"network": "udp",
			"addr":    s.addr,
			"remote":  from.String(),
			"error":   err.Error(),
		})
	}
}

func (s *UDP) writeJournal(kind string, fields map[string]any) {
	writeJournal(s.journal, kind, fields)
}

// wait ждёт завершения обработчиков или срока остановки.
func wait(wg *sync.WaitGroup, ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// writeJournal пишет событие и не мешает службе, если журнал недоступен.
func writeJournal(j journal.Writer, kind string, fields map[string]any) {
	if j == nil {
		return
	}
	if err := j.Write("listen", kind, fields); err != nil {
		log.Printf("журнал: %v", err)
	}
}
