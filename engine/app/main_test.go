package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
	"time"

	"go-wex/engine/activity"
	"go-wex/engine/clock"
	"go-wex/engine/config"
	"go-wex/engine/httpapi"
	"go-wex/engine/jobs"
	"go-wex/engine/schedule"
)

// TestRegistryDelegates — реестр сборки отдаёт маршруты HTTP-службе,
// а виды работ — реестру работ, включая отказ на повторный вид.
func TestRegistryDelegates(t *testing.T) {
	api := httpapi.New("127.0.0.1:0", fstest.MapFS{})
	reg := registry{api: api, jobs: jobs.NewRegistry()}

	reg.HTTP("GET", "/api/check", func(*http.Request) (any, error) { return "ok", nil })
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/check", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("маршрут: код %d", rec.Code)
	}

	spec := jobs.Spec{Handler: func(context.Context, jobs.Job) error { return nil }}
	if err := reg.Job("check", spec); err != nil {
		t.Fatalf("Job: %v", err)
	}
	if err := reg.Job("check", spec); err == nil {
		t.Fatal("повторный вид работы принят")
	}
}

// TestScheduleRules — настройки переводятся в пороги; пустой список даёт
// оба класса без ограничения, неверный срок отвергается.
func TestScheduleRules(t *testing.T) {
	rules, err := scheduleRules(nil)
	if err != nil {
		t.Fatalf("scheduleRules: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("пустой список: %d порогов", len(rules))
	}

	rules, err = scheduleRules([]config.ScheduleRule{{After: "5m", Class: "background"}})
	if err != nil {
		t.Fatalf("scheduleRules: %v", err)
	}
	if len(rules) != 1 || rules[0].Idle != 5*time.Minute || rules[0].Class != jobs.ClassBackground {
		t.Fatalf("порог: %+v", rules)
	}

	if _, err := scheduleRules([]config.ScheduleRule{{After: "пять минут", Class: "background"}}); err == nil {
		t.Fatal("неверный срок принят")
	}
}

// TestAllowedNow — пока запрос активен, фон убирается из набора классов.
func TestAllowedNow(t *testing.T) {
	clk := clock.NewManual(time.Unix(1000, 0))
	meter := activity.New(clk)
	sched, err := schedule.New([]schedule.Rule{
		{Idle: 0, Class: jobs.ClassInteractive},
		{Idle: 0, Class: jobs.ClassBackground},
	})
	if err != nil {
		t.Fatalf("schedule.New: %v", err)
	}

	got := allowedNow(sched, meter, clk.Now())
	if len(got) != 2 {
		t.Fatalf("свободно: %v", got)
	}

	done := meter.Begin()
	got = allowedNow(sched, meter, clk.Now())
	if len(got) != 1 || got[0] != jobs.ClassInteractive {
		t.Fatalf("запрос активен: %v", got)
	}

	done()
	got = allowedNow(sched, meter, clk.Now())
	if len(got) != 2 {
		t.Fatalf("после запроса: %v", got)
	}
}
