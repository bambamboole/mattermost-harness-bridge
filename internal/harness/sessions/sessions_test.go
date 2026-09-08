package sessions

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSessionsAreIsolatedByBotAndPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("alpha", "thread", "session_a", "infra", "claude"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("beta", "thread"); ok {
		t.Fatal("another bot inherited a session")
	}
	if err := s.Put("beta", "thread", "session_b", "app", "codex"); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := s.Get("alpha", "thread")
	if !ok || a.SessionID != "session_a" || a.Workspace != "infra" || a.Agent != "claude" {
		t.Fatalf("alpha: %+v", a)
	}
	b, ok := s.Get("beta", "thread")
	if !ok || b.SessionID != "session_b" || b.Workspace != "app" || b.Agent != "codex" {
		t.Fatalf("beta: %+v", b)
	}
	if err := s.Delete("alpha", "thread"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("beta", "thread"); !ok {
		t.Fatal("deleting alpha erased beta")
	}
}

func TestLegacySessionsCannotBeInheritedByAnyBot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	if err := os.WriteFile(path, []byte(`{"thread":{"session_id":"legacy","workspace":"infra","agent":"claude"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, bot := range []string{"alpha", "beta", ""} {
		if _, ok := s.Get(bot, "thread"); ok {
			t.Fatalf("%q inherited a legacy session with unknown bot identity", bot)
		}
	}
}
