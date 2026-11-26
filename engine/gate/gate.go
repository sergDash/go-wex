// Package gate — семафор разрешений: ограничивает число одновременных
// обращений к одному ресурсу. Разрешение берётся на время работы и
// возвращается функцией освобождения.
package gate

import (
	"context"
	"sync"
	"sync/atomic"
)

// Gate — семафор на несколько разрешений: место в канале и есть разрешение.
// Пока места заняты, Acquire ждёт освобождения или отмены контекста.
//
// Active и Peak считаются отдельно: по ним видно, сколько разрешений занято
// сейчас и сколько занимали одновременно. Это наблюдение, а не правило:
// решение, сколько разрешений нужно, принимает тот, кто создаёт семафор.
type Gate struct {
	slots  chan struct{}
	active atomic.Int64
	peak   atomic.Int64
}

// New заводит семафор на n разрешений. Значение меньше единицы считается
// единицей: слот, которого нет, не даёт работать никому.
func New(n int) *Gate {
	if n < 1 {
		n = 1
	}
	return &Gate{slots: make(chan struct{}, n)}
}

// Capacity отдаёт число разрешений.
func (g *Gate) Capacity() int { return cap(g.slots) }

// Active отдаёт число занятых разрешений.
func (g *Gate) Active() int { return int(g.active.Load()) }

// Peak отдаёт наибольшее число разрешений, занятых одновременно. Значение
// только растёт: оно показывает, доходило ли дело до столкновения.
func (g *Gate) Peak() int { return int(g.peak.Load()) }

// Acquire берёт разрешение и ждёт, если все заняты. Ошибка означает, что
// контекст отменён до освобождения места; тогда разрешение не занято.
//
// Возвращённую функцию освобождения вызывают один раз, обычно через defer.
// Повторный вызов ничего не делает: освободить дважды — значит вернуть
// чужое место.
func (g *Gate) Acquire(ctx context.Context) (func(), error) {
	select {
	case g.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	held := g.active.Add(1)
	for {
		peak := g.peak.Load()
		if held <= peak || g.peak.CompareAndSwap(peak, held) {
			break
		}
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			g.active.Add(-1)
			<-g.slots
		})
	}, nil
}
