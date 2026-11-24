package clock

import (
	"testing"
	"time"
)

// TestManualAfter — срабатывание приходит только после сдвига времени
// до срока, и приходит один раз.
func TestManualAfter(t *testing.T) {
	start := time.Unix(1000, 0)
	c := NewManual(start)
	ch := c.After(10 * time.Second)

	select {
	case <-ch:
		t.Fatal("сработало раньше срока")
	default:
	}

	c.Advance(5 * time.Second)
	select {
	case <-ch:
		t.Fatal("сработало раньше срока после половины")
	default:
	}

	c.Advance(5 * time.Second)
	select {
	case got := <-ch:
		if !got.Equal(start.Add(10 * time.Second)) {
			t.Fatalf("момент: %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("не сработало после срока")
	}

	select {
	case <-ch:
		t.Fatal("сработало второй раз")
	default:
	}
}

// TestManualZero — нулевая задержка срабатывает сразу.
func TestManualZero(t *testing.T) {
	c := NewManual(time.Unix(1000, 0))
	select {
	case <-c.After(0):
	default:
		t.Fatal("нулевая задержка не сработала сразу")
	}
}
