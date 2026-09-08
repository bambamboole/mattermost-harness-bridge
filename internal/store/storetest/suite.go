// Package storetest is a conformance suite every store.Store implementation
// must pass. It pins the CAS semantics the broker relies on.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

// Run executes the suite. open must return a fresh, empty store per call.
func Run(t *testing.T, open func(t *testing.T) store.Store) {
	t.Helper()
	tests := map[string]func(*testing.T, store.Store){
		"HarnessCRUD":           testHarnessCRUD,
		"PairingConsumeOnce":    testPairingConsumeOnce,
		"PairingExpired":        testPairingExpired,
		"JobTransitionCAS":      testJobTransitionCAS,
		"JobProgressMonotonic":  testJobProgressMonotonic,
		"JobExpireQueued":       testJobExpireQueued,
		"JobActiveByHarness":    testJobActiveByHarness,
		"ApprovalDecideOnce":    testApprovalDecideOnce,
		"ApprovalExpired":       testApprovalExpired,
		"ApprovalDuplicateID":   testApprovalDuplicateID,
		"OutboxPendingAckPurge": testOutboxPendingAckPurge,
		"BotsPerOwner":          testBotsPerOwner,
		"InitDeviceFlow":        testInitDeviceFlow,
		"JobKeepsBot":           testJobKeepsBot,
		"AuditAppend":           testAuditAppend,
	}
	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			t.Cleanup(func() { _ = s.Close() })
			fn(t, s)
		})
	}
}

var ctx = context.Background()

func now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

func seedHarness(t *testing.T, s store.Store) store.Harness {
	t.Helper()
	h := store.Harness{ID: "hrn_1", MMUserID: "user_1", Name: "mbp", TokenHash: "hash_1", Version: "0.1.0", CreatedAt: now()}
	if err := s.CreateHarness(ctx, h); err != nil {
		t.Fatal(err)
	}
	return h
}

func seedJob(t *testing.T, s store.Store, id string, state store.JobState) {
	t.Helper()
	j := store.Job{
		ID: id, HarnessID: "hrn_1", MMUserID: "user_1", ChannelID: "ch", RootPostID: "root_" + id,
		TriggerPostID: "trig", Workspace: "ws", Prompt: "do it", State: state,
		CreatedAt: now(), UpdatedAt: now(),
	}
	if err := s.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
}

func testHarnessCRUD(t *testing.T, s store.Store) {
	h := seedHarness(t, s)
	if err := s.CreateHarness(ctx, h); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate harness: want ErrConflict, got %v", err)
	}
	got, err := s.HarnessByTokenHash(ctx, "hash_1")
	if err != nil || got.ID != h.ID || got.LastSeenAt != nil {
		t.Fatalf("by token: %+v %v", got, err)
	}
	seen := now()
	if err := s.TouchHarness(ctx, h.ID, seen, "0.2.0"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.HarnessByID(ctx, h.ID)
	if got.Version != "0.2.0" || got.LastSeenAt == nil || !got.LastSeenAt.Equal(seen) {
		t.Fatalf("touch not applied: %+v", got)
	}
	list, _ := s.HarnessesByUser(ctx, "user_1")
	if len(list) != 1 {
		t.Fatalf("by user: %d", len(list))
	}
	if err := s.DeleteHarness(ctx, h.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HarnessByID(ctx, h.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if err := s.TouchHarness(ctx, "nope", seen, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("touch unknown: %v", err)
	}
}

func testPairingConsumeOnce(t *testing.T, s store.Store) {
	exp := now().Add(time.Minute)
	if err := s.CreatePairing(ctx, "code", "user_1", exp); err != nil {
		t.Fatal(err)
	}
	uid, err := s.ConsumePairing(ctx, "code", now())
	if err != nil || uid != "user_1" {
		t.Fatalf("first consume: %q %v", uid, err)
	}
	if _, err := s.ConsumePairing(ctx, "code", now()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second consume: %v", err)
	}
	if _, err := s.ConsumePairing(ctx, "unknown", now()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown consume: %v", err)
	}
}

func testPairingExpired(t *testing.T, s store.Store) {
	if err := s.CreatePairing(ctx, "code", "user_1", now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ConsumePairing(ctx, "code", now()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired consume: %v", err)
	}
}

func testJobTransitionCAS(t *testing.T, s store.Store) {
	seedHarness(t, s)
	seedJob(t, s, "job_1", store.JobDispatched)

	post := "post_1"
	j, err := s.TransitionJob(ctx, "job_1", []store.JobState{store.JobDispatched}, store.JobRunning, store.JobPatch{StatusPostID: &post})
	if err != nil || j.State != store.JobRunning || j.StatusPostID != "post_1" || j.FinishedAt != nil {
		t.Fatalf("dispatched->running: %+v %v", j, err)
	}

	// Wrong precondition.
	if _, err := s.TransitionJob(ctx, "job_1", []store.JobState{store.JobQueued}, store.JobRunning, store.JobPatch{}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("wrong from: want ErrConflict, got %v", err)
	}
	if _, err := s.TransitionJob(ctx, "missing", []store.JobState{store.JobRunning}, store.JobFailed, store.JobPatch{}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing job: want ErrNotFound, got %v", err)
	}

	// Terminal transition sets finished_at and keeps untouched fields.
	text := "done"
	j, err = s.TransitionJob(ctx, "job_1", store.ActiveStates, store.JobSucceeded, store.JobPatch{ResultText: &text})
	if err != nil || j.State != store.JobSucceeded || j.FinishedAt == nil || j.ResultText != "done" || j.StatusPostID != "post_1" {
		t.Fatalf("running->succeeded: %+v %v", j, err)
	}
	// A terminal job does not move again through the active-state guard.
	if _, err := s.TransitionJob(ctx, "job_1", store.ActiveStates, store.JobCancelled, store.JobPatch{}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("terminal re-transition: %v", err)
	}
}

func testJobProgressMonotonic(t *testing.T, s store.Store) {
	seedHarness(t, s)
	seedJob(t, s, "job_1", store.JobRunning)
	if ok, err := s.RecordProgress(ctx, "job_1", 5); err != nil || !ok {
		t.Fatalf("seq 5: %v %v", ok, err)
	}
	if ok, _ := s.RecordProgress(ctx, "job_1", 3); ok {
		t.Fatal("older seq must not apply")
	}
	if ok, _ := s.RecordProgress(ctx, "job_1", 5); ok {
		t.Fatal("same seq must not apply")
	}
	if ok, _ := s.RecordProgress(ctx, "job_1", 6); !ok {
		t.Fatal("newer seq must apply")
	}
	j, _ := s.JobByID(ctx, "job_1")
	if j.LastSeq != 6 {
		t.Fatalf("last_seq = %d", j.LastSeq)
	}
	seedJob(t, s, "job_2", store.JobSucceeded)
	if ok, _ := s.RecordProgress(ctx, "job_2", 1); ok {
		t.Fatal("progress on terminal job must not apply")
	}
}

func testJobExpireQueued(t *testing.T, s store.Store) {
	seedHarness(t, s)
	past := now().Add(-time.Minute)
	future := now().Add(time.Minute)
	seedJob(t, s, "job_old", store.JobQueued) // queued without expiry stays put
	old := store.Job{ID: "job_exp", HarnessID: "hrn_1", MMUserID: "user_1", ChannelID: "ch", RootPostID: "r", TriggerPostID: "t",
		Workspace: "ws", Prompt: "p", State: store.JobQueued, CreatedAt: now(), UpdatedAt: now(), ExpiresAt: &past}
	fresh := old
	fresh.ID = "job_fresh"
	fresh.ExpiresAt = &future
	for _, j := range []store.Job{old, fresh} {
		if err := s.CreateJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	expired, err := s.ExpireQueuedJobs(ctx, now())
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].ID != "job_exp" || expired[0].State != store.JobExpired || expired[0].FinishedAt == nil {
		t.Fatalf("expired: %+v", expired)
	}
	j, _ := s.JobByID(ctx, "job_fresh")
	if j.State != store.JobQueued {
		t.Fatalf("fresh job touched: %s", j.State)
	}
}

func testJobActiveByHarness(t *testing.T, s store.Store) {
	seedHarness(t, s)
	seedJob(t, s, "job_a", store.JobRunning)
	seedJob(t, s, "job_b", store.JobAwaitingApproval)
	seedJob(t, s, "job_c", store.JobSucceeded)
	active, err := s.ActiveJobsByHarness(ctx, "hrn_1")
	if err != nil || len(active) != 2 {
		t.Fatalf("active: %d %v", len(active), err)
	}
	byThread, _ := s.ListJobs(ctx, store.JobFilter{RootPostID: "root_job_a"})
	if len(byThread) != 1 || byThread[0].ID != "job_a" {
		t.Fatalf("by thread: %+v", byThread)
	}
}

func seedApproval(t *testing.T, s store.Store, id string, exp time.Time) store.Approval {
	t.Helper()
	a := store.Approval{ID: id, JobID: "job_1", Tool: "Bash", Summary: "rm -rf x", Input: []byte(`{"command":"rm -rf x"}`),
		Nonce: "n", RequestedAt: now(), ExpiresAt: exp}
	if err := s.CreateApproval(ctx, a); err != nil {
		t.Fatal(err)
	}
	return a
}

func testApprovalDecideOnce(t *testing.T, s store.Store) {
	seedHarness(t, s)
	seedJob(t, s, "job_1", store.JobAwaitingApproval)
	seedApproval(t, s, "apr_1", now().Add(time.Hour))

	pending, _ := s.PendingApprovalsByJob(ctx, "job_1")
	if len(pending) != 1 || !pending[0].Pending() || string(pending[0].Input) != `{"command":"rm -rf x"}` {
		t.Fatalf("pending: %+v", pending)
	}
	a, err := s.DecideApproval(ctx, "apr_1", store.Allow, "user_1", now())
	if err != nil || a.Decision != store.Allow || a.DecidedBy != "user_1" || a.DecidedAt == nil {
		t.Fatalf("decide: %+v %v", a, err)
	}
	if _, err := s.DecideApproval(ctx, "apr_1", store.Deny, "user_2", now()); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second decide: want ErrConflict, got %v", err)
	}
	if _, err := s.DecideApproval(ctx, "apr_x", store.Deny, "user_2", now()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown decide: %v", err)
	}
	pending, _ = s.PendingApprovalsByJob(ctx, "job_1")
	if len(pending) != 0 {
		t.Fatalf("still pending: %+v", pending)
	}
}

func testApprovalExpired(t *testing.T, s store.Store) {
	seedHarness(t, s)
	seedJob(t, s, "job_1", store.JobAwaitingApproval)
	seedApproval(t, s, "apr_1", now().Add(-time.Second))
	if _, err := s.DecideApproval(ctx, "apr_1", store.Allow, "user_1", now()); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expired decide: want ErrConflict, got %v", err)
	}
}

func testApprovalDuplicateID(t *testing.T, s store.Store) {
	seedHarness(t, s)
	seedJob(t, s, "job_1", store.JobAwaitingApproval)
	a := seedApproval(t, s, "apr_1", now().Add(time.Hour))
	if err := s.CreateApproval(ctx, a); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate approval: %v", err)
	}
}

func testOutboxPendingAckPurge(t *testing.T, s store.Store) {
	t0 := now()
	msgs := []store.OutboxMessage{
		{ID: "m1", HarnessID: "hrn_1", JobID: "job_1", Type: "job.dispatch", Payload: []byte(`{"a":1}`), CreatedAt: t0},
		{ID: "m2", HarnessID: "hrn_1", JobID: "job_1", Type: "job.cancel", Payload: []byte(`{}`), CreatedAt: t0.Add(time.Millisecond)},
		{ID: "m3", HarnessID: "hrn_2", Type: "job.dispatch", Payload: []byte(`{}`), CreatedAt: t0},
	}
	for _, m := range msgs {
		if err := s.Enqueue(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Enqueue(ctx, msgs[0]); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate enqueue: %v", err)
	}
	pending, err := s.PendingOutbox(ctx, "hrn_1")
	if err != nil || len(pending) != 2 || pending[0].ID != "m1" || pending[1].ID != "m2" || string(pending[0].Payload) != `{"a":1}` {
		t.Fatalf("pending: %+v %v", pending, err)
	}
	if err := s.AckOutbox(ctx, "m1", now()); err != nil {
		t.Fatal(err)
	}
	if err := s.AckOutbox(ctx, "m1", now()); err != nil {
		t.Fatalf("ack must be idempotent: %v", err)
	}
	pending, _ = s.PendingOutbox(ctx, "hrn_1")
	if len(pending) != 1 || pending[0].ID != "m2" {
		t.Fatalf("after ack: %+v", pending)
	}
	n, err := s.PurgeOutbox(ctx, now().Add(time.Second))
	if err != nil || n != 1 {
		t.Fatalf("purge: %d %v", n, err)
	}
	pending, _ = s.PendingOutbox(ctx, "hrn_2")
	if len(pending) != 1 {
		t.Fatalf("other harness affected: %+v", pending)
	}
}

func testAuditAppend(t *testing.T, s store.Store) {
	if err := s.Append(ctx, store.AuditEntry{Actor: "system", Action: "test", JobID: "job_1", Details: []byte(`{"k":"v"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, store.AuditEntry{At: now(), Actor: "user_1", Action: "test.nodetails"}); err != nil {
		t.Fatal(err)
	}
}

func testBotsPerOwner(t *testing.T, s store.Store) {
	b := store.Bot{UserID: "bot_1", MMUserID: "user_1", Username: "harness-manuel", Token: "tok", CreatedAt: now()}
	if err := s.CreateBot(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateBot(ctx, store.Bot{UserID: "bot_2", MMUserID: "user_1", Username: "other", Token: "t", CreatedAt: now()}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second bot for owner: %v", err)
	}
	if err := s.CreateBot(ctx, store.Bot{UserID: "bot_3", MMUserID: "user_2", Username: "harness-manuel", Token: "t", CreatedAt: now()}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate username: %v", err)
	}
	got, err := s.BotByOwner(ctx, "user_1")
	if err != nil || got.UserID != "bot_1" || got.Token != "tok" {
		t.Fatalf("by owner: %+v %v", got, err)
	}
	if _, err := s.BotByUserID(ctx, "bot_x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown bot: %v", err)
	}
	list, _ := s.ListBots(ctx)
	if len(list) != 1 {
		t.Fatalf("list: %d", len(list))
	}
}

func testInitDeviceFlow(t *testing.T, s store.Store) {
	exp := now().Add(10 * time.Minute)
	if err := s.CreateInit(ctx, store.InitRequest{CodeHash: "c1", BotName: "custom", HarnessName: "mbp", CreatedAt: now(), ExpiresAt: exp}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateInit(ctx, store.InitRequest{CodeHash: "c1", CreatedAt: now(), ExpiresAt: exp}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate code: %v", err)
	}
	// Pending: the laptop polls and gets no token.
	r, err := s.FetchInit(ctx, "c1", now())
	if err != nil || r.Claimed() || r.BotName != "custom" || r.HarnessName != "mbp" {
		t.Fatalf("pending fetch: %+v %v", r, err)
	}
	if err := s.ClaimInit(ctx, "c1", now(), "hrn_1", "hrt_secret"); err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimInit(ctx, "c1", now(), "hrn_2", "x"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second claim: %v", err)
	}
	if err := s.ClaimInit(ctx, "nope", now(), "hrn_2", "x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown claim: %v", err)
	}
	r, err = s.FetchInit(ctx, "c1", now())
	if err != nil || !r.Claimed() || r.HarnessID != "hrn_1" || r.HarnessToken != "hrt_secret" || r.FetchedAt == nil {
		t.Fatalf("claimed fetch: %+v %v", r, err)
	}
	if _, err := s.FetchInit(ctx, "c1", now()); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second fetch must not hand out the token again: %v", err)
	}
	if r, _ := s.InitByHarness(ctx, "hrn_1"); r.HarnessToken != "" {
		t.Fatal("token must be wiped after fetch")
	}
	// Expired codes are gone for both sides.
	_ = s.CreateInit(ctx, store.InitRequest{CodeHash: "old", CreatedAt: now(), ExpiresAt: now().Add(-time.Second)})
	if _, err := s.FetchInit(ctx, "old", now()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired fetch: %v", err)
	}
	if err := s.ClaimInit(ctx, "old", now(), "h", "t"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expired claim: %v", err)
	}
}

func testJobKeepsBot(t *testing.T, s store.Store) {
	seedHarness(t, s)
	j := store.Job{ID: "job_b", HarnessID: "hrn_1", MMUserID: "user_1", ChannelID: "ch", RootPostID: "r", TriggerPostID: "t",
		BotUserID: "bot_1", Workspace: "ws", Prompt: "p", State: store.JobRunning, CreatedAt: now(), UpdatedAt: now()}
	if err := s.CreateJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	got, _ := s.JobByID(ctx, "job_b")
	if got.BotUserID != "bot_1" {
		t.Fatalf("bot lost: %+v", got)
	}
}
