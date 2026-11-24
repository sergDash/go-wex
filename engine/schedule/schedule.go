// Package schedule — правило, какие классы работ допустимы при простое.
// Расписание — чистая функция: оно не хранит времени и не запускает таймеров;
// тикер или часы только вызывают проверку.
package schedule

import (
	"fmt"
	"sort"
	"time"

	"go-wex/engine/jobs"
)

// Rule — порог простоя, после которого разрешён класс.
type Rule struct {
	Idle  time.Duration
	Class jobs.Class
}

// Schedule — набор порогов. Класс может упоминаться несколько раз; действует
// самый ранний пройденный порог.
type Schedule struct {
	rules []Rule
}

// New проверяет пороги и сортирует их по возрастанию простоя.
func New(rules []Rule) (*Schedule, error) {
	out := make([]Rule, len(rules))
	copy(out, rules)

	for _, r := range out {
		if r.Idle < 0 {
			return nil, fmt.Errorf("расписание: отрицательный порог %s для класса %q", r.Idle, r.Class)
		}
		if !r.Class.Valid() {
			return nil, fmt.Errorf("расписание: неизвестный класс %q", r.Class)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Idle < out[j].Idle })
	return &Schedule{rules: out}, nil
}

// Allowed отдаёт классы, чей порог пройден к моменту now. Простой считается
// от последней активности; момент в будущем даёт нулевой простой. Классы
// в ответе не повторяются.
func (s *Schedule) Allowed(lastActive, now time.Time) []jobs.Class {
	idle := now.Sub(lastActive)
	if idle < 0 {
		idle = 0
	}

	var out []jobs.Class
	for _, r := range s.rules {
		if idle < r.Idle {
			continue
		}
		if !hasClass(out, r.Class) {
			out = append(out, r.Class)
		}
	}
	return out
}

func hasClass(classes []jobs.Class, class jobs.Class) bool {
	for _, c := range classes {
		if c == class {
			return true
		}
	}
	return false
}
