// Package clock — источник времени движка. Механизмы берут время у часов,
// а не у time.Now по месту: в проверках часы подставляются, и тесты не спят.
package clock

import (
	"sync"
	"time"
)

// Clock — источник времени: текущий момент и одно срабатывание через
// промежуток. Повторяющееся ожидание — снова After.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// Real — обычные часы процесса.
type Real struct{}

// NewReal отдаёт часы, которые идут по системному времени.
func NewReal() Clock { return Real{} }

func (Real) Now() time.Time { return time.Now() }

func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Manual — часы для проверок: время стоит, пока Advance не сдвинет его.
type Manual struct {
	mu     sync.Mutex
	now    time.Time
	timers []*timer
}

// timer — ожидающее срабатывание: срок и канал, куда придёт момент.
type timer struct {
	at time.Time
	ch chan time.Time
}

// NewManual заводит остановленные часы в момент start.
func NewManual(start time.Time) *Manual {
	return &Manual{now: start}
}

func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// After ставит срабатывание через d. При d <= 0 канал срабатывает сразу.
func (m *Manual) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)

	m.mu.Lock()
	defer m.mu.Unlock()

	if d <= 0 {
		ch <- m.now
		return ch
	}
	m.timers = append(m.timers, &timer{at: m.now.Add(d), ch: ch})
	return ch
}

// Advance сдвигает время и срабатывает все сроки не позже нового момента.
func (m *Manual) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.now = m.now.Add(d)
	pending := m.timers[:0]
	for _, t := range m.timers {
		if t.at.After(m.now) {
			pending = append(pending, t)
			continue
		}
		t.ch <- m.now
	}
	m.timers = pending
}
