package core

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost/mmtest"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store/sqlite"
)

type fakeHub struct {
	mu     sync.Mutex
	online map[string]bool
	sent   []sentEnv
}

type sentEnv struct {
	harness string
	env     protocol.Envelope
}

func (f *fakeHub) Online(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.online[id]
}

func (f *fakeHub) Send(ctx context.Context, id string, env protocol.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, sentEnv{id, env})
	return nil
}

func (f *fakeHub) SendReliable(ctx context.Context, id string, env protocol.Envelope) error {
	return f.Send(ctx, id, env)
}

func (f *fakeHub) last(t *testing.T, typ string) sentEnv {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.sent) - 1; i >= 0; i-- {
		if f.sent[i].env.Type == typ {
			return f.sent[i]
		}
	}
	t.Fatalf("no %s sent; sent=%d", typ, len(f.sent))
	return sentEnv{}
}

func (f *fakeHub) count(typ string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.sent {
		if s.env.Type == typ {
			n++
		}
	}
	return n
}

type fixture struct {
	t    *testing.T
	st   store.Store
	mm   *mmtest.Fake
	hub  *fakeHub
	core *Core
	ctx  context.Context
	now  time.Time
}

const (
	botID    = "bot"
	ownerID  = "user_owner"
	otherID  = "user_other"
	harness1 = "hrn_1"
)

func newFixture(t *testing.T) *fixture {
	t.Helper()
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mm := mmtest.New()
	mm.AddUser(ownerID, "manuel")
	mm.AddUser(otherID, "mallory")
	hub := &fakeHub{online: map[string]bool{harness1: true}}
	c := New(Config{
		BotUserID: botID, BotUsername: "cc", PublicURL: "https://broker.test", CallbackSecret: []byte("0123456789abcdef0123456789abcdef"),
		EditInterval: time.Millisecond,
	}, st, mm, hub, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	f := &fixture{t: t, st: st, mm: mm, hub: hub, core: c, ctx: context.Background(), now: time.Now()}
	c.now = func() time.Time { return f.now }
	if err := st.CreateHarness(f.ctx, store.Harness{ID: harness1, MMUserID: ownerID, Name: "mbp", TokenHash: "h1", CreatedAt: f.now}); err != nil {
		t.Fatal(err)
	}
	return f
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func (f *fixture) mention(user, msg string, root string) *model.Post {
	p := &model.Post{Id: "trigger_" + protocol.NewID("p")[2:8], ChannelId: "chan", UserId: user, Message: msg, RootId: root}
	f.core.HandlePost(f.ctx, mattermost.PostedEvent{Post: p, ChannelType: "O", Mentions: []string{botID}})
	return p
}

func (f *fixture) harness() store.Harness {
	h, err := f.st.HarnessByID(f.ctx, harness1)
	if err != nil {
		f.t.Fatal(err)
	}
	return h
}

func (f *fixture) onlyJob() store.Job {
	jobs, err := f.st.ListJobs(f.ctx, store.JobFilter{})
	if err != nil || len(jobs) != 1 {
		f.t.Fatalf("want exactly one job, got %d (%v)", len(jobs), err)
	}
	return jobs[0]
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestMentionDispatchesToOwnersHarness(t *testing.T) {
	f := newFixture(t)
	trigger := f.mention(ownerID, "@cc ws:infra bump the provider", "")

	j := f.onlyJob()
	if j.State != store.JobDispatched || j.HarnessID != harness1 || j.MMUserID != ownerID || j.Workspace != "infra" || j.Prompt != "bump the provider" {
		t.Fatalf("job: %+v", j)
	}
	if j.RootPostID != trigger.Id || j.StatusPostID == "" {
		t.Fatalf("thread wiring: %+v", j)
	}
	sent := f.hub.last(t, protocol.TypeJobDispatch)
	var d protocol.JobDispatch
	if err := sent.env.Decode(&d); err != nil {
		t.Fatal(err)
	}
	if sent.harness != harness1 || d.Requester.MMUserID != ownerID || d.Requester.Username != "manuel" || d.Thread.RootPostID != trigger.Id || d.Workspace != "infra" {
		t.Fatalf("dispatch: %+v to %s", d, sent.harness)
	}
	if !strings.Contains(f.mm.Message(j.StatusPostID), "Starting") {
		t.Fatalf("status post: %q", f.mm.Message(j.StatusPostID))
	}

	// Ack moves it to running and the status post follows.
	f.core.OnAck(f.ctx, f.harness(), sent.env.ID, j.ID, protocol.Ack{OK: true})
	j, _ = f.st.JobByID(f.ctx, j.ID)
	if j.State != store.JobRunning {
		t.Fatalf("after ack: %s", j.State)
	}
	waitFor(t, func() bool { return strings.Contains(f.mm.Message(j.StatusPostID), "Running") })
}

func TestUnpairedUserGetsHint(t *testing.T) {
	f := newFixture(t)
	f.mention(otherID, "@cc do something", "")
	if jobs, _ := f.st.ListJobs(f.ctx, store.JobFilter{}); len(jobs) != 0 {
		t.Fatalf("job created for unpaired user: %+v", jobs)
	}
	if last := f.mm.Last(); last == nil || !strings.Contains(last.Message, "no paired harness") {
		t.Fatalf("reply: %+v", last)
	}
	if f.hub.count(protocol.TypeJobDispatch) != 0 {
		t.Fatal("dispatch sent for unpaired user")
	}
}

func TestOfflineHarnessQueuesThenExpires(t *testing.T) {
	f := newFixture(t)
	f.hub.online[harness1] = false
	f.mention(ownerID, "@cc hi", "")
	j := f.onlyJob()
	if j.State != store.JobQueued || j.ExpiresAt == nil {
		t.Fatalf("job: %+v", j)
	}
	if !strings.Contains(f.mm.Message(j.StatusPostID), "offline") {
		t.Fatalf("status: %q", f.mm.Message(j.StatusPostID))
	}
	// The dispatch is still handed to the hub: it lands in the outbox.
	f.hub.last(t, protocol.TypeJobDispatch)

	f.now = f.now.Add(6 * time.Minute)
	f.core.Sweep(f.ctx)
	j, _ = f.st.JobByID(f.ctx, j.ID)
	if j.State != store.JobExpired || !strings.Contains(f.mm.Message(j.StatusPostID), "Expired") {
		t.Fatalf("after sweep: %s %q", j.State, f.mm.Message(j.StatusPostID))
	}
}

func TestDispatchNackFailsJob(t *testing.T) {
	f := newFixture(t)
	f.mention(ownerID, "@cc ws:nope hi", "")
	j := f.onlyJob()
	sent := f.hub.last(t, protocol.TypeJobDispatch)
	f.core.OnAck(f.ctx, f.harness(), sent.env.ID, j.ID, protocol.Ack{OK: false, Code: protocol.NackWorkspaceUnknown, Message: "nope"})
	j, _ = f.st.JobByID(f.ctx, j.ID)
	if j.State != store.JobFailed || !strings.Contains(j.Error, "workspace_unknown") {
		t.Fatalf("job: %+v", j)
	}
	if !strings.Contains(f.mm.Message(j.StatusPostID), "rejected") {
		t.Fatalf("status: %q", f.mm.Message(j.StatusPostID))
	}
}

func TestSecondMentionInThreadIsRefusedWhileRunning(t *testing.T) {
	f := newFixture(t)
	trigger := f.mention(ownerID, "@cc first", "")
	f.mention(ownerID, "@cc second", trigger.Id)
	if jobs, _ := f.st.ListJobs(f.ctx, store.JobFilter{}); len(jobs) != 1 {
		t.Fatalf("jobs: %d", len(jobs))
	}
	if !strings.Contains(f.mm.Last().Message, "already running") {
		t.Fatalf("reply: %q", f.mm.Last().Message)
	}
}

func approvalFixture(t *testing.T) (*fixture, store.Job, *model.Post) {
	f := newFixture(t)
	f.mention(ownerID, "@cc rm stuff", "")
	j := f.onlyJob()
	f.core.OnAck(f.ctx, f.harness(), "x", j.ID, protocol.Ack{OK: true})
	ack := f.core.OnApprovalRequest(f.ctx, f.harness(), j.ID, protocol.ApprovalRequest{
		ApprovalID: "apr_1", Tool: "Bash", Summary: "rm -rf node_modules", Input: json.RawMessage(`{"command":"rm -rf node_modules"}`),
		CWD: "/repo", ExpiresAt: f.now.Add(time.Hour).UnixMilli(),
	})
	if !ack.OK {
		t.Fatalf("approval request nacked: %+v", ack)
	}
	j, _ = f.st.JobByID(f.ctx, j.ID)
	if j.State != store.JobAwaitingApproval {
		t.Fatalf("state: %s", j.State)
	}
	buttonPost := f.mm.Last()
	att := f.mm.Attachments(buttonPost.Id)
	if len(att) != 1 || len(att[0].Actions) != 2 {
		t.Fatalf("button post: %+v", att)
	}
	return f, j, buttonPost
}

func click(f *fixture, buttonPost *model.Post, action int, userID, userName string, mutate func(map[string]any)) *model.PostActionIntegrationResponse {
	att := f.mm.Attachments(buttonPost.Id)
	ctxMap := map[string]any{}
	for k, v := range att[0].Actions[action].Integration.Context {
		ctxMap[k] = v
	}
	if mutate != nil {
		mutate(ctxMap)
	}
	return f.core.HandleCallback(f.ctx, &model.PostActionIntegrationRequest{UserId: userID, UserName: userName, PostId: buttonPost.Id, Context: ctxMap})
}

func TestApprovalOnlyOwnerCanDecide(t *testing.T) {
	f, j, buttonPost := approvalFixture(t)

	res := click(f, buttonPost, 0, otherID, "mallory", nil)
	if res.Update != nil || !strings.Contains(res.EphemeralText, "owner") {
		t.Fatalf("non-owner click: %+v", res)
	}
	if a, _ := f.st.ApprovalByID(f.ctx, "apr_1"); !a.Pending() {
		t.Fatal("non-owner click decided the approval")
	}
	if f.hub.count(protocol.TypeApprovalResponse) != 0 {
		t.Fatal("approval response sent after non-owner click")
	}

	res = click(f, buttonPost, 0, ownerID, "manuel", func(m map[string]any) { m[ctxSig] = "deadbeef" })
	if res.Update != nil || !strings.Contains(res.EphemeralText, "signature") {
		t.Fatalf("bad signature click: %+v", res)
	}
	res = click(f, buttonPost, 0, ownerID, "manuel", func(m map[string]any) { m[ctxDecision] = protocol.DecisionDeny })
	if res.Update != nil || !strings.Contains(res.EphemeralText, "signature") {
		t.Fatalf("decision swap must break the signature: %+v", res)
	}

	res = click(f, buttonPost, 0, ownerID, "manuel", nil)
	if res.Update == nil || !strings.Contains(res.Update.Message, "Allowed by @manuel") {
		t.Fatalf("owner click: %+v", res)
	}
	sent := f.hub.last(t, protocol.TypeApprovalResponse)
	var ar protocol.ApprovalResponse
	_ = sent.env.Decode(&ar)
	if sent.harness != harness1 || ar.ApprovalID != "apr_1" || ar.Decision != protocol.DecisionAllow || ar.DecidedBy != "manuel" {
		t.Fatalf("approval response: %+v", ar)
	}
	j, _ = f.st.JobByID(f.ctx, j.ID)
	if j.State != store.JobRunning {
		t.Fatalf("state after decision: %s", j.State)
	}

	// Second click: already decided.
	res = click(f, buttonPost, 1, ownerID, "manuel", nil)
	if res.Update != nil || !strings.Contains(res.EphemeralText, "Already decided") {
		t.Fatalf("double click: %+v", res)
	}
}

func TestApprovalRequestFromForeignHarnessIsRefused(t *testing.T) {
	f := newFixture(t)
	f.mention(ownerID, "@cc hi", "")
	j := f.onlyJob()
	foreign := store.Harness{ID: "hrn_2", MMUserID: otherID, Name: "evil", TokenHash: "h2", CreatedAt: f.now}
	_ = f.st.CreateHarness(f.ctx, foreign)
	ack := f.core.OnApprovalRequest(f.ctx, foreign, j.ID, protocol.ApprovalRequest{ApprovalID: "apr_x", Tool: "Bash"})
	if ack.OK || ack.Code != protocol.NackForbidden {
		t.Fatalf("foreign approval: %+v", ack)
	}
	ack = f.core.OnResult(f.ctx, foreign, j.ID, protocol.JobResult{Status: protocol.StatusSucceeded, Text: "pwned"})
	if ack.OK || ack.Code != protocol.NackForbidden {
		t.Fatalf("foreign result: %+v", ack)
	}
}

func TestResultFinalizesPostAndIsIdempotent(t *testing.T) {
	f := newFixture(t)
	f.mention(ownerID, "@cc hi", "")
	j := f.onlyJob()
	f.core.OnAck(f.ctx, f.harness(), "x", j.ID, protocol.Ack{OK: true})
	res := protocol.JobResult{Status: protocol.StatusSucceeded, Text: "All done.", Usage: protocol.Usage{Turns: 3, DurationMS: 4000, CostUSD: 0.05},
		Files: []protocol.File{{Name: "diff.patch", Mime: "text/x-diff", DataB64: "aGVsbG8="}}}
	if ack := f.core.OnResult(f.ctx, f.harness(), j.ID, res); !ack.OK {
		t.Fatalf("result nacked: %+v", ack)
	}
	j, _ = f.st.JobByID(f.ctx, j.ID)
	if j.State != store.JobSucceeded || j.ResultText != "All done." || j.FinishedAt == nil {
		t.Fatalf("job: %+v", j)
	}
	msg := f.mm.Message(j.StatusPostID)
	if !strings.Contains(msg, "Done") || !strings.Contains(msg, "All done.") || !strings.Contains(msg, "3 turns") {
		t.Fatalf("final post: %q", msg)
	}
	if last := f.mm.Last(); len(last.FileIds) != 1 || string(f.mm.Files[last.FileIds[0]]) != "hello" {
		t.Fatalf("attachment post: %+v", last)
	}
	if ack := f.core.OnResult(f.ctx, f.harness(), j.ID, res); !ack.OK {
		t.Fatalf("resend must be acked: %+v", ack)
	}
	if ack := f.core.OnResult(f.ctx, f.harness(), "job_missing", res); ack.OK || ack.Code != protocol.NackUnknownJob {
		t.Fatalf("unknown job: %+v", ack)
	}
}

func TestLongResultGoesToFile(t *testing.T) {
	f := newFixture(t)
	f.mention(ownerID, "@cc hi", "")
	j := f.onlyJob()
	long := strings.Repeat("x", mattermost.MaxMessageLen+500)
	f.core.OnResult(f.ctx, f.harness(), j.ID, protocol.JobResult{Status: protocol.StatusSucceeded, Text: long})
	msg := f.mm.Message(j.StatusPostID)
	if len([]rune(msg)) > mattermost.MaxMessageLen || !strings.Contains(msg, "full output attached") {
		t.Fatalf("final post len=%d", len(msg))
	}
	if last := f.mm.Last(); len(last.FileIds) != 1 || len(f.mm.Files[last.FileIds[0]]) != len(long) {
		t.Fatalf("result file: %+v", last)
	}
}

func TestHelloReconcilesJobs(t *testing.T) {
	f := newFixture(t)
	f.mention(ownerID, "@cc one", "")
	running := f.onlyJob()
	f.core.OnAck(f.ctx, f.harness(), "x", running.ID, protocol.Ack{OK: true})

	// A second job that the harness lost (restart).
	lost := store.Job{ID: "job_lost", HarnessID: harness1, MMUserID: ownerID, ChannelID: "chan", RootPostID: "r2", TriggerPostID: "t2",
		Workspace: "", Prompt: "p", State: store.JobRunning, CreatedAt: f.now, UpdatedAt: f.now}
	_ = f.st.CreateJob(f.ctx, lost)

	welcome := f.core.OnHello(f.ctx, f.harness(), protocol.Hello{Jobs: []protocol.HelloJob{
		{JobID: running.ID, State: "running"},
		{JobID: "job_unknown", State: "running"},
	}})
	actions := map[string]string{}
	for _, w := range welcome {
		actions[w.JobID] = w.Action
	}
	if actions[running.ID] != protocol.ActionResume || actions["job_unknown"] != protocol.ActionAbort {
		t.Fatalf("welcome: %+v", welcome)
	}
	if j, _ := f.st.JobByID(f.ctx, "job_lost"); j.State != store.JobFailed {
		t.Fatalf("lost job: %s", j.State)
	}
	if j, _ := f.st.JobByID(f.ctx, running.ID); j.State != store.JobRunning {
		t.Fatalf("running job: %s", j.State)
	}
}

func TestSweepMarksLostAfterGrace(t *testing.T) {
	f := newFixture(t)
	f.mention(ownerID, "@cc hi", "")
	j := f.onlyJob()
	f.core.OnAck(f.ctx, f.harness(), "x", j.ID, protocol.Ack{OK: true})
	_ = f.st.TouchHarness(f.ctx, harness1, f.now, "0.1.0")

	f.hub.online[harness1] = false
	f.now = f.now.Add(5 * time.Minute)
	f.core.Sweep(f.ctx)
	if j, _ := f.st.JobByID(f.ctx, j.ID); j.State != store.JobRunning {
		t.Fatalf("within grace: %s", j.State)
	}
	f.now = f.now.Add(6 * time.Minute)
	f.core.Sweep(f.ctx)
	j, _ = f.st.JobByID(f.ctx, j.ID)
	if j.State != store.JobLost || !strings.Contains(f.mm.Message(j.StatusPostID), "Lost contact") {
		t.Fatalf("after grace: %s %q", j.State, f.mm.Message(j.StatusPostID))
	}
	// A late result still lands.
	if ack := f.core.OnResult(f.ctx, f.harness(), j.ID, protocol.JobResult{Status: protocol.StatusSucceeded, Text: "late"}); !ack.OK {
		t.Fatalf("late result: %+v", ack)
	}
	if j, _ := f.st.JobByID(f.ctx, j.ID); j.State != store.JobSucceeded {
		t.Fatalf("late result state: %s", j.State)
	}
}

func TestPairingFlow(t *testing.T) {
	f := newFixture(t)
	f.core.HandlePost(f.ctx, mattermost.PostedEvent{Post: &model.Post{Id: "dm1", ChannelId: "dm_user_other", UserId: otherID, Message: "pair"}, ChannelType: "D"})
	reply := f.mm.Last().Message
	i := strings.Index(reply, "mhb harness pair --broker https://broker.test ")
	if i < 0 {
		t.Fatalf("pair reply: %q", reply)
	}
	code := strings.Fields(reply[i:])[5]

	if _, err := f.core.HandlePair(f.ctx, PairRequest{Code: "WRONG", Name: "x"}); err != ErrBadPairingCode {
		t.Fatalf("wrong code: %v", err)
	}
	res, err := f.core.HandlePair(f.ctx, PairRequest{Code: strings.ToLower(code), Name: "laptop", Version: "0.1.0"})
	if err != nil || res.MMUserID != otherID || res.Username != "mallory" || !strings.HasPrefix(res.Token, "hrt_") {
		t.Fatalf("pair: %+v %v", res, err)
	}
	if _, err := f.core.HandlePair(f.ctx, PairRequest{Code: code}); err != ErrBadPairingCode {
		t.Fatalf("code reuse: %v", err)
	}
	h, err := f.st.HarnessByTokenHash(f.ctx, tokenHash(res.Token))
	if err != nil || h.MMUserID != otherID || h.Name != "laptop" {
		t.Fatalf("harness by token: %+v %v", h, err)
	}
	// Now mallory can dispatch to their own harness, and only theirs.
	f.hub.online[h.ID] = true
	f.mention(otherID, "@cc hi", "")
	sent := f.hub.last(t, protocol.TypeJobDispatch)
	if sent.harness != h.ID {
		t.Fatalf("dispatched to %s, want %s", sent.harness, h.ID)
	}
}

func TestCancelInThread(t *testing.T) {
	f := newFixture(t)
	trigger := f.mention(ownerID, "@cc hi", "")
	j := f.onlyJob()
	f.core.OnAck(f.ctx, f.harness(), "x", j.ID, protocol.Ack{OK: true})
	// Someone else cannot cancel the owner's job.
	f.mention(otherID, "@cc cancel", trigger.Id)
	if f.hub.count(protocol.TypeJobCancel) != 0 {
		t.Fatal("non-owner cancel was forwarded")
	}
	f.mention(ownerID, "@cc cancel", trigger.Id)
	sent := f.hub.last(t, protocol.TypeJobCancel)
	if sent.env.JobID != j.ID {
		t.Fatalf("cancel for %s", sent.env.JobID)
	}
}

func TestSplitWorkspace(t *testing.T) {
	cases := map[string][2]string{
		"ws:infra bump the provider":          {"infra", "bump the provider"},
		"bump the provider ws:infra":          {"infra", "bump the provider"},
		"bump the\nprovider\nws:infra please": {"infra", "bump the provider please"},
		"`ws:infra` bump":                     {"infra", "bump"},
		"ws:infra, bump":                      {"infra", "bump"},
		"bump the provider":                   {"", "bump the provider"},
		"ws: bump":                            {"", "ws: bump"},
		"  ws:mhb Lies die README  ":          {"mhb", "Lies die README"},
	}
	for in, want := range cases {
		ws, prompt := splitWorkspace(in)
		if ws != want[0] || prompt != want[1] {
			t.Errorf("splitWorkspace(%q) = %q, %q; want %q, %q", in, ws, prompt, want[0], want[1])
		}
	}
}

func TestSplitAgent(t *testing.T) {
	cases := map[string][2]string{
		"agent:codex fix the build":  {"codex", "fix the build"},
		"fix the build agent:Claude": {"claude", "fix the build"},
		"`agent:codex`, go":          {"codex", "go"},
		"fix the build":              {"", "fix the build"},
		"agent: go":                  {"", "agent: go"},
	}
	for in, want := range cases {
		ag, prompt := splitAgent(in)
		if ag != want[0] || prompt != want[1] {
			t.Errorf("splitAgent(%q) = %q, %q; want %q, %q", in, ag, prompt, want[0], want[1])
		}
	}
}

func TestMentionCarriesAgentAndThreadHistory(t *testing.T) {
	f := newFixture(t)
	root, _ := f.mm.CreatePost(f.ctx, &model.Post{ChannelId: "chan", UserId: otherID, Message: "@cc please look", CreateAt: 10})
	_, _ = f.mm.CreatePost(f.ctx, &model.Post{ChannelId: "chan", UserId: botID, RootId: root.Id, Message: "▶️ Running", CreateAt: 11})
	_, _ = f.mm.CreatePost(f.ctx, &model.Post{ChannelId: "chan", UserId: ownerID, RootId: root.Id, Message: "the build is red", CreateAt: 12})

	p := &model.Post{Id: "trig", ChannelId: "chan", UserId: ownerID, RootId: root.Id, Message: "@cc agent:codex ws:infra fix it", CreateAt: 13}
	f.core.HandlePost(f.ctx, mattermost.PostedEvent{Post: p, ChannelType: "O", Mentions: []string{botID}})
	sent := f.hub.last(t, protocol.TypeJobDispatch)
	var d protocol.JobDispatch
	_ = sent.env.Decode(&d)
	if d.Agent != "codex" || d.Workspace != "infra" || d.Prompt != "fix it" {
		t.Fatalf("dispatch: %+v", d)
	}
	if len(d.History) != 2 || d.History[0].Username != "mallory" || d.History[0].Text != "please look" || d.History[1].Text != "the build is red" {
		t.Fatalf("history: %+v", d.History)
	}

	// A mention that starts a thread carries no history.
	f.mention(ownerID, "@cc fresh", "")
	sent = f.hub.last(t, protocol.TypeJobDispatch)
	var fresh protocol.JobDispatch
	_ = sent.env.Decode(&fresh)
	if len(fresh.History) != 0 || fresh.Agent != "" {
		t.Fatalf("fresh dispatch: %+v", fresh)
	}
}

func TestDefaultBotName(t *testing.T) {
	if got := defaultBotName("Manuel"); got != "harness-manuel" {
		t.Fatalf("got %q", got)
	}
	if got := defaultBotName("a-very-long-username-here"); len(got) > maxBotNameLength || strings.HasSuffix(got, "-") {
		t.Fatalf("got %q", got)
	}
}

func TestMentionedNames(t *testing.T) {
	got := mentionedNames("hey @Harness-Mallory, ask @cc. mail me@example.com and @x_y-")
	for _, want := range []string{"harness-mallory", "cc", "x_y"} {
		if !got[want] {
			t.Errorf("missing %q in %v", want, got)
		}
	}
	if got["example.com"] {
		t.Errorf("email local part must not count as a mention: %v", got)
	}
}
