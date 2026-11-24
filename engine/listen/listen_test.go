package listen

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go-wex/engine/journal"
)

// journalFake — журнал для проверки: запоминает виды событий.
type journalFake struct {
	mu    sync.Mutex
	kinds []string
}

func (j *journalFake) Write(_, kind string, _ map[string]any) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.kinds = append(j.kinds, kind)
	return nil
}

func (j *journalFake) has(kind string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, k := range j.kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// echoTCP отвечает «ok» на первый байт и паникует на «p».
func echoTCP(_ context.Context, conn net.Conn) error {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return err
	}
	if buf[0] == 'p' {
		panic("сломано")
	}
	_, err := conn.Write([]byte("ok"))
	return err
}

// TestTCPEchoAndStop — служба принимает соединение, отвечает, а после
// остановки освобождает порт.
func TestTCPEchoAndStop(t *testing.T) {
	s := NewTCP("127.0.0.1:0", echoTCP, journal.Discard{})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	addr := s.Addr()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(reply) != "ok" {
		t.Fatalf("ответ: %q", reply)
	}
	_ = conn.Close()

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("порт не освобождён: %v", err)
	}
	_ = ln.Close()
}

// TestTCPPanicIsolated — паника одного обработчика не мешает следующему
// соединению и попадает в журнал.
func TestTCPPanicIsolated(t *testing.T) {
	j := &journalFake{}
	s := NewTCP("127.0.0.1:0", echoTCP, j)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Stop(context.Background()) }()

	first, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("Dial первый: %v", err)
	}
	_ = first.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := first.Write([]byte("p")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := first.Read(buf); err == nil {
		t.Fatal("паника не закрыла соединение")
	}
	_ = first.Close()

	second, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("Dial второй: %v", err)
	}
	_ = second.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := second.Write([]byte("x")); err != nil {
		t.Fatalf("Write второй: %v", err)
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(second, reply); err != nil {
		t.Fatalf("Read второй: %v", err)
	}
	if string(reply) != "ok" {
		t.Fatalf("ответ второй: %q", reply)
	}
	_ = second.Close()

	if !j.has("panic") {
		t.Fatalf("паника не записана: %v", j.kinds)
	}
}

// TestTCPStopWaitsHandler — остановка отменяет обработчик и ждёт его.
func TestTCPStopWaitsHandler(t *testing.T) {
	started := make(chan struct{})
	s := NewTCP("127.0.0.1:0", func(ctx context.Context, _ net.Conn) error {
		close(started)
		<-ctx.Done()
		return nil
	}, journal.Discard{})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// echoUDP отвечает «ok» на датаграмму и паникует на «p».
func echoUDP(_ context.Context, pc net.PacketConn, data []byte, from net.Addr) error {
	if len(data) > 0 && data[0] == 'p' {
		panic("сломано")
	}
	_, err := pc.WriteTo([]byte("ok"), from)
	return err
}

// TestUDPEchoAndStop — служба отвечает на датаграмму и освобождает порт.
func TestUDPEchoAndStop(t *testing.T) {
	s := NewUDP("127.0.0.1:0", echoUDP, journal.Discard{})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	addr := s.Addr()

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	reply := make([]byte, 2)
	if _, err := conn.Read(reply); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(reply) != "ok" {
		t.Fatalf("ответ: %q", reply)
	}
	_ = conn.Close()

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		t.Fatalf("порт не освобождён: %v", err)
	}
	_ = pc.Close()
}

// TestUDPPanicIsolated — паника на одной датаграмме не останавливает приём.
func TestUDPPanicIsolated(t *testing.T) {
	panicked := make(chan struct{})
	var once sync.Once
	s := NewUDP("127.0.0.1:0", func(ctx context.Context, pc net.PacketConn, data []byte, from net.Addr) error {
		if len(data) > 0 && data[0] == 'p' {
			once.Do(func() { close(panicked) })
			panic("сломано")
		}
		_, err := pc.WriteTo([]byte("ok"), from)
		return err
	}, journal.Discard{})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = s.Stop(context.Background()) }()

	conn, err := net.Dial("udp", s.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

	if _, err := conn.Write([]byte("p")); err != nil {
		t.Fatalf("Write паники: %v", err)
	}
	<-panicked

	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatalf("Write второй: %v", err)
	}
	reply := make([]byte, 2)
	if _, err := conn.Read(reply); err != nil {
		t.Fatalf("Read второй: %v", err)
	}
	if string(reply) != "ok" {
		t.Fatalf("ответ: %q", reply)
	}
}
