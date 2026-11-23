// Package service — внутренние службы движка: долгоживущие части процесса
// с запуском и остановкой. Движок стоит на них: HTTP-сервер, разборщик
// очереди, слушатели и управляемые процессы оформляются как службы.
package service

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// stopTimeout — сколько даётся всем службам на остановку вместе.
const stopTimeout = 5 * time.Second

// Service — внутренняя служба: запускается, работает и останавливается.
// Start возвращается, когда служба готова принимать работу; Stop дожидается
// завершения её горутин.
type Service interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Group — набор служб: запуск в порядке списка, остановка в обратном.
type Group struct {
	services []Service
}

// New собирает группу служб в порядке запуска.
func New(services ...Service) *Group {
	return &Group{services: services}
}

// Run запускает службы, ждёт отмены контекста и останавливает их.
// Если служба не запустилась, уже запущенные останавливаются, а ошибка
// возвращается вместе с ошибками остановки.
func (g *Group) Run(ctx context.Context) error {
	started := make([]Service, 0, len(g.services))
	for _, s := range g.services {
		if err := s.Start(ctx); err != nil {
			return errors.Join(
				fmt.Errorf("служба %s: %w", s.Name(), err),
				g.stop(started),
			)
		}
		started = append(started, s)
	}

	<-ctx.Done()
	return g.stop(started)
}

// stop останавливает службы в обратном порядке с общим сроком.
func (g *Group) stop(started []Service) error {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()

	var errs []error
	for i := len(started) - 1; i >= 0; i-- {
		if err := started[i].Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("служба %s: %w", started[i].Name(), err))
		}
	}
	return errors.Join(errs...)
}
