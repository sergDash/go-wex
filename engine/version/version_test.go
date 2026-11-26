package version

import "testing"

// TestString — версия без коммита показывается как есть, с коммитом —
// вместе с ним.
func TestString(t *testing.T) {
	oldVersion, oldCommit := Version, Commit
	defer func() { Version, Commit = oldVersion, oldCommit }()

	Version, Commit = "1.2.3", "none"
	if got := String(); got != "1.2.3" {
		t.Fatalf("без коммита: %q", got)
	}

	Commit = "abc1234"
	if got := String(); got != "1.2.3 (abc1234)" {
		t.Fatalf("с коммитом: %q", got)
	}
}
