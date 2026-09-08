package harness

import (
	"strings"
	"testing"

	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/sessions"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
)

func testHarness() *Harness {
	cfg := DefaultConfig()
	cfg.Workspaces = map[string]string{"infra": "/repos/infra", "app": "/repos/app"}
	cfg.DefaultWorkspace = "/home/me/.harness"
	cfg.Agent = "claude"
	return &Harness{cfg: cfg}
}

func TestResolveWorkspace(t *testing.T) {
	h := testHarness()
	cases := []struct {
		name       string
		dispatch   string
		session    sessions.Entry
		hasSession bool
		wantName   string
		wantDir    string
		wantOK     bool
	}{
		{"explicit", "infra", sessions.Entry{}, false, "infra", "/repos/infra", true},
		{"explicit unknown", "nope", sessions.Entry{}, false, "nope", "", false},
		{"explicit beats session", "app", sessions.Entry{Workspace: "infra"}, true, "app", "/repos/app", true},
		{"session", "", sessions.Entry{Workspace: "infra"}, true, "infra", "/repos/infra", true},
		{"session in default", "", sessions.Entry{Workspace: DefaultWorkspaceName}, true, DefaultWorkspaceName, "/home/me/.harness", true},
		{"session with removed workspace", "", sessions.Entry{Workspace: "gone"}, true, DefaultWorkspaceName, "/home/me/.harness", true},
		{"nothing", "", sessions.Entry{}, false, DefaultWorkspaceName, "/home/me/.harness", true},
	}
	for _, c := range cases {
		name, dir, ok := h.resolveWorkspace(protocol.JobDispatch{Workspace: c.dispatch}, c.session, c.hasSession)
		if name != c.wantName || dir != c.wantDir || ok != c.wantOK {
			t.Errorf("%s: got %q %q %v, want %q %q %v", c.name, name, dir, ok, c.wantName, c.wantDir, c.wantOK)
		}
	}
}

func TestResolveAgent(t *testing.T) {
	h := testHarness()
	if got := h.resolveAgent(protocol.JobDispatch{Agent: "codex"}, sessions.Entry{Agent: "claude"}, true); got != "codex" {
		t.Errorf("explicit: %s", got)
	}
	if got := h.resolveAgent(protocol.JobDispatch{}, sessions.Entry{Agent: "codex"}, true); got != "codex" {
		t.Errorf("session: %s", got)
	}
	if got := h.resolveAgent(protocol.JobDispatch{}, sessions.Entry{}, false); got != "claude" {
		t.Errorf("default: %s", got)
	}
}

func TestRenderHistory(t *testing.T) {
	posts := []protocol.HistoryPost{
		{Username: "manuel", At: 1_700_000_000_000, Text: "first"},
		{Username: "mallory", At: 1_700_000_060_000, Text: "  "},
		{Username: "manuel", At: 1_700_000_120_000, Text: "second"},
	}
	out := renderHistory(posts, 1000)
	if !strings.HasPrefix(out, "Earlier posts") || !strings.Contains(out, "@manuel: first") || !strings.Contains(out, "@manuel: second") || strings.Contains(out, "@mallory") {
		t.Fatalf("rendered: %q", out)
	}
	if !strings.Contains(out, "<thread>\n") || !strings.HasSuffix(out, "</thread>") {
		t.Fatalf("missing markers: %q", out)
	}
	// Budget drops the oldest posts first.
	short := renderHistory(posts, 40)
	if strings.Contains(short, "@manuel: first") || !strings.Contains(short, "@manuel: second") {
		t.Fatalf("budget: %q", short)
	}
	if renderHistory(nil, 100) != "" {
		t.Fatal("empty history must render nothing")
	}
}

func TestRedactConfig(t *testing.T) {
	raw := []byte(`{"broker_url":"https://b.example.com","token":"hrt_secret","unknown_key":42}`)
	out, err := RedactConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if strings.Contains(s, "hrt_secret") {
		t.Fatalf("token still present: %s", s)
	}
	if !strings.Contains(s, `"token": "redacted"`) || !strings.Contains(s, `"unknown_key": 42`) {
		t.Fatalf("unexpected output: %s", s)
	}
}
