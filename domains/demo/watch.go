package demo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go-wex/engine/service"
)

// watchService — служба демо: тикает каждые полсекунды и по команде паникует
// в своей горутине. Она показывает границу службы: паника не роняет процесс,
// группа видит отказ и перезапускает службу по правилу.
//
// Сверх Service служба отвечает на два вопроса: Done — чем закончилась её
// горутина, Ready — можно ли ей давать работу. Готовность приходит не сразу:
// служба «прогревается» секунду, как сервер модели.
type watchService struct {
	ticks atomic.Int64
	crash atomic.Bool
	ready atomic.Bool

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan error
}

// newWatch заводит службу наблюдения.
func newWatch() *watchService {
	return &watchService{}
}

func (s *watchService) Name() string { return "demo.watch" }

// Ready сообщает, прошла ли служба прогрев.
func (s *watchService) Ready() bool { return s.ready.Load() }

// Done отдаёт канал итога работы: группа служб по нему узнаёт, что горутина
// умерла. После каждого запуска канал новый — иначе второй отказ не заметить.
func (s *watchService) Done() <-chan error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

func (s *watchService) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("demo: служба наблюдения уже запущена")
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	s.cancel = cancel
	s.done = done
	s.started = true
	s.mu.Unlock()

	s.ready.Store(false)

	// Граница службы: без Guard паника в горутине завершила бы процесс.
	go func() {
		defer close(done)
		// Работа горутины кончилась — служба не готова, даже если это паника.
		defer s.ready.Store(false)
		done <- service.Guard(func() error { return s.run(runCtx) })
	}()
	return nil
}

// run — цикл службы: тикает, прогревается и падает по команде.
func (s *watchService) run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	readyAt := time.Now().Add(time.Second)
	for {
		select {
		case <-ctx.Done():
			s.ready.Store(false)
			return nil
		case <-ticker.C:
			if s.crash.Swap(false) {
				panic("demo: служба уронила себя по команде")
			}
			s.ticks.Add(1)
			if time.Now().After(readyAt) {
				s.ready.Store(true)
			}
		}
	}
}

func (s *watchService) Stop(ctx context.Context) error {
	s.mu.Lock()
	started, cancel, done := s.started, s.cancel, s.done
	s.mu.Unlock()
	if !started {
		return nil
	}

	cancel()
	select {
	case <-done:
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
