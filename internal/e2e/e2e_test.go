// Package e2e wires the real hub, the real harness daemon and a fake claude
// binary together over a live WebSocket and drives the whole flow.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/core"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/hub"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost/mmtest"
	"github.com/bambamboole/mattermost-harness-bridge/internal/harness"
	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/permission"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store/sqlite"
)

// TestMain doubles as the fake `claude` binary when FAKE_CLAUDE is set.
func TestMain(m *testing.M) {
	if os.Getenv("FAKE_CLAUDE") == "1" {
		fakeClaude()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// fakeClaude mimics `claude -p` just enough: init line, one permission
// request through the harness socket (the real CLI does this via the MCP
// subcommand), then a result echoing what it saw.
func fakeClaude() {
	args := os.Args[1:]
	get := func(flag string) string {
		for i, a := range args {
			if a == flag && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
	prompt, resume, mcpPath := get("-p"), get("--resume"), get("--mcp-config")
	sessionID := "sess-" + fmt.Sprint(time.Now().UnixNano())
	if resume != "" {
		sessionID = resume
	}
	emit := func(v any) {
		b, _ := json.Marshal(v)
		fmt.Println(string(b))
	}
	emit(map[string]any{"type": "system", "subtype": "init", "session_id": sessionID})
	emit(map[string]any{"type": "assistant", "session_id": sessionID, "message": map[string]any{
		"content": []map[string]any{{"type": "text", "text": "Working on: " + prompt}}}})

	decision := "skipped"
	if strings.Contains(prompt, "ask") {
		var cfg struct {
			Servers map[string]struct {
				Args []string `json:"args"`
			} `json:"mcpServers"`
		}
		raw, _ := os.ReadFile(mcpPath)
		_ = json.Unmarshal(raw, &cfg)
		a := cfg.Servers[permission.ServerName].Args
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		dec, err := permission.Ask(ctx, a[2], permission.Request{JobID: a[4], ToolName: "Bash",
			Input: json.RawMessage(`{"command":"rm -rf node_modules"}`), ToolUseID: "toolu_1"})
		if err != nil {
			decision = "error: " + err.Error()
		} else {
			decision = dec.Behavior
		}
	}
	if strings.Contains(prompt, "hang") {
		time.Sleep(30 * time.Second)
	}
	emit(map[string]any{"type": "result", "subtype": "success", "is_error": false, "num_turns": 2, "session_id": sessionID,
		"result": fmt.Sprintf("final decision=%s resume=%s cwd=%s", decision, resume, mustCwd()), "total_cost_usd": 0.01, "duration_ms": 10})
}

func mustCwd() string {
	d, _ := os.Getwd()
	return d
}

type world struct {
	t     *testing.T
	ctx   context.Context
	st    store.Store
	mm    *mmtest.Fake
	hub   *hub.Hub
	core  *core.Core
	srv   *httptest.Server
	hrn   store.Harness
	token string
	hcfg  harness.Config
	stop  context.CancelFunc
}

const (
	botID   = "bot"
	ownerID = "user_owner"
)

func newWorld(t *testing.T) *world {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mm := mmtest.New()
	mm.AddUser(ownerID, "manuel")

	h := hub.New(st, nil, log)
	h.HeartbeatInterval = 200 * time.Millisecond
	h.ReadTimeout = 2 * time.Second
	c := core.New(core.Config{BotUserID: botID, BotUsername: "cc", PublicURL: "https://broker.test",
		CallbackSecret: []byte("0123456789abcdef0123456789abcdef"), EditInterval: 10 * time.Millisecond}, st, mm, h, log)
	h.Handler = c
	mux := http.NewServeMux()
	mux.Handle(protocol.Path, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	token := "hrt_test"
	hrn := store.Harness{ID: "hrn_1", MMUserID: ownerID, Name: "mbp", TokenHash: hub.TokenHash(token), CreatedAt: time.Now()}
	if err := st.CreateHarness(ctx, hrn); err != nil {
		t.Fatal(err)
	}

	stateDir := t.TempDir()
	t.Setenv("MM_HARNESS_CONFIG_DIR", stateDir)
	t.Setenv("MM_HARNESS_STATE_DIR", stateDir)
	t.Setenv("FAKE_CLAUDE", "1")
	ws, _ := filepath.EvalSymlinks(t.TempDir()) // macOS: /var -> /private/var
	self, _ := os.Executable()
	hcfg := harness.DefaultConfig()
	hcfg.BrokerURL = srv.URL
	hcfg.HarnessID = hrn.ID
	hcfg.Token = token
	hcfg.OwnerMMUserID = ownerID
	hcfg.Workspaces = map[string]string{"ws": ws}
	hcfg.ClaudeBin = self
	hcfg.ApprovalTimeoutMin = 1
	hcfg.MaxJobs = 2

	w := &world{t: t, ctx: ctx, st: st, mm: mm, hub: h, core: c, srv: srv, hrn: hrn, token: token, hcfg: hcfg, stop: cancel}
	w.startHarness()
	return w
}

func (w *world) startHarness() {
	hctx, hstop := context.WithCancel(w.ctx)
	w.stop = hstop
	h, err := harness.New(w.hcfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		w.t.Fatal(err)
	}
	go func() {
		if err := h.Run(hctx); err != nil && hctx.Err() == nil {
			w.t.Errorf("harness run: %v", err)
		}
	}()
	w.wait("harness online", func() bool { return w.hub.Online(w.hrn.ID) })
}

func (w *world) wait(what string, cond func() bool) {
	w.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	w.t.Fatalf("timeout waiting for %s", what)
}

func (w *world) mention(msg, root string) *model.Post {
	p := &model.Post{Id: protocol.NewID("trig"), ChannelId: "chan", UserId: ownerID, Message: msg, RootId: root}
	w.core.HandlePost(w.ctx, mattermost.PostedEvent{Post: p, ChannelType: "O", Mentions: []string{botID}})
	return p
}

func (w *world) jobFor(root string) store.Job {
	jobs, _ := w.st.ListJobs(w.ctx, store.JobFilter{RootPostID: root, Limit: 1})
	if len(jobs) == 0 {
		w.t.Fatalf("no job for thread %s", root)
	}
	return jobs[0]
}

func (w *world) waitState(id string, states ...store.JobState) store.Job {
	w.t.Helper()
	var j store.Job
	w.wait(fmt.Sprintf("job %s in %v", id, states), func() bool {
		j, _ = w.st.JobByID(w.ctx, id)
		for _, s := range states {
			if j.State == s {
				return true
			}
		}
		return false
	})
	return j
}

func (w *world) buttonPost() *model.Post {
	var p *model.Post
	w.wait("approval button post", func() bool {
		p = w.mm.Last()
		return p != nil && len(w.mm.Attachments(p.Id)) == 1
	})
	return p
}

func (w *world) click(p *model.Post, action int) *model.PostActionIntegrationResponse {
	att := w.mm.Attachments(p.Id)
	return w.core.HandleCallback(w.ctx, &model.PostActionIntegrationRequest{
		UserId: ownerID, UserName: "manuel", PostId: p.Id, Context: att[0].Actions[action].Integration.Context,
	})
}

func TestFullFlowWithApprovalAndThreadContinuation(t *testing.T) {
	w := newWorld(t)

	// 1. Mention -> job runs in the workspace and asks for approval.
	trigger := w.mention("@cc ws:ws please ask before deleting", "")
	job := w.jobFor(trigger.Id)
	w.waitState(job.ID, store.JobAwaitingApproval)
	btn := w.buttonPost()
	if att := w.mm.Attachments(btn.Id); !strings.Contains(att[0].Text, "rm -rf node_modules") {
		t.Fatalf("button text: %q", att[0].Text)
	}

	// 2. Owner clicks Allow -> harness gets the decision -> job finishes.
	if res := w.click(btn, 0); res.Update == nil {
		t.Fatalf("allow click: %+v", res)
	}
	job = w.waitState(job.ID, store.JobSucceeded, store.JobFailed)
	if job.State != store.JobSucceeded || !strings.Contains(job.ResultText, "decision=allow") || !strings.Contains(job.ResultText, "cwd="+w.hcfg.Workspaces["ws"]) {
		t.Fatalf("job 1: %+v", job)
	}
	w.wait("final post", func() bool { return strings.Contains(w.mm.Message(job.StatusPostID), "Done") })

	// 3. Reply in the same thread -> resumed session, same workspace without ws: prefix.
	w.mention("@cc and now deny me, ask again", trigger.Id)
	job2 := w.jobFor(trigger.Id)
	if job2.ID == job.ID {
		t.Fatal("no second job")
	}
	w.waitState(job2.ID, store.JobAwaitingApproval)
	btn2 := w.buttonPost()
	if res := w.click(btn2, 1); res.Update == nil || !strings.Contains(res.Update.Message, "Denied") {
		t.Fatalf("deny click: %+v", res)
	}
	job2 = w.waitState(job2.ID, store.JobSucceeded, store.JobFailed)
	if !strings.Contains(job2.ResultText, "decision=deny") || !strings.Contains(job2.ResultText, "resume=sess-") {
		t.Fatalf("job 2 did not resume with a denied decision: %+v", job2)
	}

	// 4. Outbox is clean: every at-least-once message got acked.
	pending, _ := w.st.PendingOutbox(w.ctx, w.hrn.ID)
	if len(pending) != 0 {
		t.Fatalf("unacked outbox: %+v", pending)
	}
}

func TestCancelStopsRunningJob(t *testing.T) {
	w := newWorld(t)
	trigger := w.mention("@cc hang around", "")
	job := w.jobFor(trigger.Id)
	w.waitState(job.ID, store.JobRunning)
	w.mention("@cc cancel", trigger.Id)
	job = w.waitState(job.ID, store.JobCancelled, store.JobFailed, store.JobSucceeded)
	if job.State != store.JobCancelled {
		t.Fatalf("job: %+v", job)
	}
	w.wait("cancel post", func() bool { return strings.Contains(w.mm.Message(job.StatusPostID), "Cancelled") })
}

func TestUnknownWorkspaceIsNacked(t *testing.T) {
	w := newWorld(t)
	trigger := w.mention("@cc ws:nope hi", "")
	job := w.waitState(w.jobFor(trigger.Id).ID, store.JobFailed)
	if !strings.Contains(job.Error, "workspace_unknown") {
		t.Fatalf("job: %+v", job)
	}
}

func TestResultSurvivesBrokerDisconnect(t *testing.T) {
	w := newWorld(t)
	trigger := w.mention("@cc hang around", "")
	job := w.jobFor(trigger.Id)
	w.waitState(job.ID, store.JobRunning)

	// Kick the harness off; the fake claude keeps running for a while.
	w.hub.Close()
	w.wait("harness offline", func() bool { return !w.hub.Online(w.hrn.ID) })
	// The harness reconnects on its own, reports the job in hello, and the
	// broker resumes it instead of failing it.
	w.wait("harness back", func() bool { return w.hub.Online(w.hrn.ID) })
	if j, _ := w.st.JobByID(w.ctx, job.ID); j.State != store.JobRunning {
		t.Fatalf("after reconnect: %s", j.State)
	}
	w.mention("@cc cancel", trigger.Id)
	job = w.waitState(job.ID, store.JobCancelled)
}
