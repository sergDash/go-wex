package activity

import (
	"testing"
	"time"

	"go-wex/engine/clock"
)

// TestMeter — активный запрос делает счётчик занятым, завершение
// освобождает его и сдвигает метку активности.
func TestMeter(t *testing.T) {
	start := time.Unix(1000, 0)
	c := clock.NewManual(start)
	m := New(c)

	if m.Busy() || m.Active() != 0 {
		t.Fatalf("до запроса: active=%d busy=%v", m.Active(), m.Busy())
	}
	if !m.LastActive().Equal(start) {
		t.Fatalf("стартовая активность: %s", m.LastActive())
	}

	done := m.Begin()
	if !m.Busy() || m.Active() != 1 {
		t.Fatalf("во время запроса: active=%d busy=%v", m.Active(), m.Busy())
	}

	c.Advance(time.Second)
	done()
	if m.Busy() || m.Active() != 0 {
		t.Fatalf("после запроса: active=%d busy=%v", m.Active(), m.Busy())
	}
	if !m.LastActive().Equal(start.Add(time.Second)) {
		t.Fatalf("активность после запроса: %s", m.LastActive())
	}
}

// TestMeterNested — два одновременных запроса держат счётчик занятым,
// пока не завершён последний.
func TestMeterNested(t *testing.T) {
	m := New(clock.NewManual(time.Unix(1000, 0)))

	first := m.Begin()
	second := m.Begin()
	if m.Active() != 2 {
		t.Fatalf("два запроса: active=%d", m.Active())
	}

	first()
	if !m.Busy() || m.Active() != 1 {
		t.Fatalf("после первого: active=%d busy=%v", m.Active(), m.Busy())
	}

	second()
	if m.Busy() {
		t.Fatal("после второго запрос всё ещё активен")
	}
}

// TestTouch — доменный сигнал сдвигает метку активности, не занимая счётчик:
// так приложение говорит «человек действовал», не выдавая это за запрос.
func TestTouch(t *testing.T) {
	start := time.Unix(1000, 0)
	c := clock.NewManual(start)
	m := New(c)

	c.Advance(30 * time.Second)
	m.Touch()

	if !m.LastActive().Equal(start.Add(30 * time.Second)) {
		t.Fatalf("активность после сигнала: %s", m.LastActive())
	}
	if m.Busy() || m.Active() != 0 {
		t.Fatalf("сигнал занял счётчик: active=%d busy=%v", m.Active(), m.Busy())
	}
}
