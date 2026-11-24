package schedule

import (
	"strings"
	"testing"
	"time"

	"go-wex/engine/jobs"
)

// TestAllowedByIdle — три момента простоя дают три набора классов.
func TestAllowedByIdle(t *testing.T) {
	s, err := New([]Rule{
		{Idle: 0, Class: jobs.ClassInteractive},
		{Idle: 5 * time.Minute, Class: jobs.ClassBackground},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	last := time.Unix(1000, 0)
	cases := []struct {
		idle time.Duration
		want string
	}{
		{0, "interactive"},
		{4 * time.Minute, "interactive"},
		{5 * time.Minute, "interactive,background"},
		{time.Hour, "interactive,background"},
	}
	for _, c := range cases {
		got := names(s.Allowed(last, last.Add(c.idle)))
		if got != c.want {
			t.Fatalf("простой %s: получили %s, ждали %s", c.idle, got, c.want)
		}
	}
}

// TestAllowedFuture — последняя активность в будущем считается нулевым
// простоем, а не отрицательным.
func TestAllowedFuture(t *testing.T) {
	s, err := New([]Rule{{Idle: 0, Class: jobs.ClassInteractive}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	now := time.Unix(1000, 0)
	if got := names(s.Allowed(now.Add(time.Minute), now)); got != "interactive" {
		t.Fatalf("будущая активность: %s", got)
	}
}

// TestNewValidation — отрицательный порог и неизвестный класс отвергаются.
func TestNewValidation(t *testing.T) {
	if _, err := New([]Rule{{Idle: -time.Second, Class: jobs.ClassBackground}}); err == nil {
		t.Fatal("отрицательный порог принят")
	}
	if _, err := New([]Rule{{Idle: 0, Class: jobs.Class("urgent")}}); err == nil {
		t.Fatal("неизвестный класс принят")
	}
}

// TestNoDuplicates — класс упомянут дважды, в ответе он один раз.
func TestNoDuplicates(t *testing.T) {
	s, err := New([]Rule{
		{Idle: 0, Class: jobs.ClassBackground},
		{Idle: time.Minute, Class: jobs.ClassBackground},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := names(s.Allowed(time.Unix(1000, 0), time.Unix(2000, 0))); got != "background" {
		t.Fatalf("повтор класса: %s", got)
	}
}

func names(classes []jobs.Class) string {
	out := make([]string, len(classes))
	for i, c := range classes {
		out[i] = string(c)
	}
	return strings.Join(out, ",")
}
