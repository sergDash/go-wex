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

// Pausable — служба, которая умеет приостановить работу и продолжить её.
// Пауза — необязательная возможность: HTTP-серверу она не нужна, разборщику
// и управляемому процессу нужна. Пауза не освобождает порты и файлы — для
// этого есть Stop; она прекращает брать новую работу, чтобы снизить нагрузку
// или разобрать состояние при отладке.
type Pausable interface {
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
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

// Pause приостанавливает службы в обратном порядке запуска. Службы без
// Pausable пропускаются: пауза — необязательная возможность. Ошибка одной
// службы не мешает остальным, но возвращается вместе с остальными.
func (g *Group) Pause(ctx context.Context) error {
	var errs []error
	for i := len(g.services) - 1; i >= 0; i-- {
		p, ok := g.services[i].(Pausable)
		if !ok {
			continue
		}
		if err := p.Pause(ctx); err != nil {
			errs = append(errs, fmt.Errorf("служба %s: %w", g.services[i].Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Resume продолжает службы в порядке запуска.
func (g *Group) Resume(ctx context.Context) error {
	var errs []error
	for _, s := range g.services {
		p, ok := s.(Pausable)
		if !ok {
			continue
		}
		if err := p.Resume(ctx); err != nil {
			errs = append(errs, fmt.Errorf("служба %s: %w", s.Name(), err))
		}
	}
	return errors.Join(errs...)
}
