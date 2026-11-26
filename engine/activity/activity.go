// Package activity — учёт запросов: сколько их сейчас и когда был последний.
// По этим данным расписание решает, можно ли начинать фоновую работу:
// пока запрос активен, фон не должен занимать процессор.
package activity

import (
	"sync/atomic"
	"time"

	"go-wex/engine/clock"
)

// Meter — счётчик активных запросов и метка последней активности.
type Meter struct {
	active atomic.Int64
	last   atomic.Int64 // UnixNano
	clock  clock.Clock
}

// New заводит счётчик; до первого запроса последней активностью считается
// запуск, поэтому фон ждёт порог простоя и после старта.
func New(c clock.Clock) *Meter {
	m := &Meter{clock: c}
	m.last.Store(c.Now().UnixNano())
	return m
}

// Begin отмечает начало запроса и возвращает функцию завершения.
func (m *Meter) Begin() func() {
	m.active.Add(1)
	m.last.Store(m.clock.Now().UnixNano())

	return func() {
		m.last.Store(m.clock.Now().UnixNano())
		m.active.Add(-1)
	}
}

// Touch отмечает действие человека. Нужен приложению, у которого активность —
// не HTTP-запрос, а доменный сигнал: реплика игрока, ход в игре. Счётчик
// активных запросов при этом продолжает считать занятость отдельно.
func (m *Meter) Touch() {
	m.last.Store(m.clock.Now().UnixNano())
}

// Active отдаёт число незавершённых запросов.
func (m *Meter) Active() int64 { return m.active.Load() }

// Busy сообщает, есть ли активный запрос.
func (m *Meter) Busy() bool { return m.active.Load() > 0 }

// LastActive отдаёт момент последней активности.
func (m *Meter) LastActive() time.Time { return time.Unix(0, m.last.Load()) }
