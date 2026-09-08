package harness

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/agent"
	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/client"
	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/sessions"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
)

type recordingAgent struct {
	name string
	runs chan agent.Options
}

func (a recordingAgent) Name() string    { return a.name }
func (a recordingAgent) Approvals() bool { return false }
func (a recordingAgent) Check() error    { return nil }
func (a recordingAgent) Run(_ context.Context, opt agent.Options, _ func(agent.Event)) (agent.Result, error) {
	a.runs <- opt
	return agent.Result{Status: agent.StatusSucceeded, SessionID: "new_" + a.name}, nil
}

func TestDispatchIsolatesBotSessionWorkspaceAndAgent(t *testing.T) {
	h := testHarness()
	h.cfg.OwnerMMUserID = "owner"
	h.cfg.Agent = "codex"
	h.cfg.MaxJobs = 2
	h.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	h.jobs = map[string]*job{}
	var err error
	h.sessions, err = sessions.Open(filepath.Join(t.TempDir(), "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.sessions.Put("alpha", "thread", "alpha_session", "infra", "claude"); err != nil {
		t.Fatal(err)
	}
	outbox, err := client.OpenOutbox(filepath.Join(t.TempDir(), "outbox.json"))
	if err != nil {
		t.Fatal(err)
	}
	h.client = &client.Client{Outbox: outbox}
	claudeRuns, codexRuns := make(chan agent.Options, 1), make(chan agent.Options, 1)
	h.agents = agent.Registry{"claude": recordingAgent{"claude", claudeRuns}, "codex": recordingAgent{"codex", codexRuns}}
	d := protocol.JobDispatch{BotUserID: "beta", Prompt: "continue", Requester: protocol.Requester{MMUserID: "owner"}, Thread: protocol.Thread{RootPostID: "thread"}}
	if ack := h.OnDispatch(context.Background(), "beta_job", d); !ack.OK {
		t.Fatalf("beta: %+v", ack)
	}
	d.BotUserID = "alpha"
	if ack := h.OnDispatch(context.Background(), "alpha_job", d); !ack.OK {
		t.Fatalf("alpha: %+v", ack)
	}
	select {
	case opt := <-codexRuns:
		if opt.ResumeID != "" || opt.Dir != h.cfg.DefaultWorkspace {
			t.Fatalf("beta inherited alpha: %+v", opt)
		}
	case <-time.After(time.Second):
		t.Fatal("beta did not use the default agent")
	}
	select {
	case opt := <-claudeRuns:
		if opt.ResumeID != "alpha_session" || opt.Dir != "/repos/infra" {
			t.Fatalf("alpha lost session: %+v", opt)
		}
	case <-time.After(time.Second):
		t.Fatal("alpha did not inherit its agent")
	}
	deadline := time.Now().Add(time.Second)
	for {
		h.mu.Lock()
		n := len(h.jobs)
		h.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("jobs did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	a, _ := h.sessions.Get("alpha", "thread")
	b, _ := h.sessions.Get("beta", "thread")
	if a.SessionID != "new_claude" || b.SessionID != "new_codex" {
		t.Fatalf("session writes crossed bots: %+v %+v", a, b)
	}
}

func TestDispatchRequiresBotSessionIdentity(t *testing.T) {
	h := testHarness()
	d := protocol.JobDispatch{Requester: protocol.Requester{MMUserID: h.cfg.OwnerMMUserID}, Thread: protocol.Thread{RootPostID: "thread"}}
	if ack := h.OnDispatch(context.Background(), "job", d); ack.OK || ack.Code != protocol.NackBadPayload {
		t.Fatalf("missing bot accepted: %+v", ack)
	}
}

func testHarness() *Harness {
	cfg := DefaultConfig()
	cfg.Workspaces = map[string]string{"infra": "/repos/infra", "app": "/repos/app"}
	cfg.DefaultWorkspace = "/home/me/.harness"
	cfg.Agent = "claude"
	return &Harness{cfg: cfg}
}

func TestWSURLUsesCurrentProtocol(t *testing.T) {
	for _, scheme := range []string{"http", "https"} {
		cfg := Config{BrokerURL: scheme + "://broker.test/"}
		want := strings.Replace(scheme, "http", "ws", 1) + "://broker.test" + protocol.Path
		if got := cfg.WSURL(); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
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
