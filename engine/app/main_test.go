package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"go-wex/engine/httpapi"
	"go-wex/engine/jobs"
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
