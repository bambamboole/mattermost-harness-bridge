package core

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

func routedFixture(t *testing.T) (*fixture, store.Bot) {
	f := newFixture(t)
	f.core.newPoster = func(token string) mattermost.API { return f.mm.WithToken(token) }
	f.core.mm = nil // Bot routing must never need the shared listener's credentials.
	b := store.Bot{UserID: "bot_a", MMUserID: ownerID, Username: "alpha", Token: "token_a", HarnessID: harness1, CreatedAt: time.Now()}
	if err := f.st.CreateBot(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	return f, b
}

func route(f *fixture, b store.Bot, id, root, channelType, text, user string) {
	f.core.HandleBotPost(f.ctx, b.UserID, mattermost.PostedEvent{ChannelType: channelType, Post: &model.Post{Id: id, RootId: root, ChannelId: "private_channel", UserId: user, Message: text}})
}

func TestBotRoutingUsesBoundHarnessAndBotAPI(t *testing.T) {
	f, b := routedFixture(t)
	if err := f.st.CreateHarness(f.ctx, store.Harness{ID: "other_harness", MMUserID: ownerID, Name: "other", TokenHash: "other_token", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	f.hub.online[harness1] = false
	f.hub.online["other_harness"] = true
	root, _ := f.mm.CreatePost(f.ctx, &model.Post{UserId: ownerID, ChannelId: "private_channel", Message: "earlier context", CreateAt: 1})
	f.mm.Files["attachment"] = []byte("hello")
	f.core.HandleBotPost(f.ctx, b.UserID, mattermost.PostedEvent{ChannelType: "P", Post: &model.Post{Id: "request", RootId: root.Id, ChannelId: "private_channel", UserId: ownerID, Message: "@alpha task", FileIds: []string{"attachment"}, CreateAt: 2}})
	j := f.onlyJob()
	var d protocol.JobDispatch
	sent := f.hub.last(t, protocol.TypeJobDispatch)
	if err := sent.env.Decode(&d); err != nil {
		t.Fatal(err)
	}
	if j.HarnessID != harness1 || sent.harness != harness1 || j.BotUserID != b.UserID || d.BotUserID != b.UserID || j.State != store.JobQueued {
		t.Fatalf("incorrect routing: %+v %+v", j, d)
	}
	if len(d.Attachments) != 1 || len(d.History) != 1 || f.mm.PostToken(j.StatusPostID) != b.Token {
		t.Fatalf("bot API/context missing: %+v", d)
	}
}

func TestBotRoutingNeverFallsBackFromMissingBinding(t *testing.T) {
	f, b := routedFixture(t)
	if err := f.st.DeleteBot(f.ctx, b.UserID); err != nil {
		t.Fatal(err)
	}
	b.HarnessID = ""
	if err := f.st.CreateBot(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	route(f, b, "request", "", "P", "@alpha work", ownerID)
	if f.hub.count(protocol.TypeJobDispatch) != 0 {
		t.Fatal("used another harness for an unbound bot")
	}
}

func TestBotRoutingDMAndOwnerOnly(t *testing.T) {
	f, b := routedFixture(t)
	route(f, b, "forbidden", "", "D", "do work", otherID)
	if jobs, _ := f.st.ListJobs(f.ctx, store.JobFilter{}); len(jobs) != 0 {
		t.Fatal("non-owner dispatched")
	}
	if last := f.mm.Last(); last == nil || f.mm.PostToken(last.Id) != b.Token {
		t.Fatal("refusal did not use the addressed bot")
	}
	route(f, b, "dm", "", "D", "do work", ownerID)
	if j := f.onlyJob(); j.Prompt != "do work" {
		t.Fatalf("DM did not start task: %+v", j)
	}
}

func TestBotRoutingIgnoresDeletedPosts(t *testing.T) {
	f, b := routedFixture(t)
	f.core.HandleBotPost(f.ctx, b.UserID, mattermost.PostedEvent{ChannelType: "D", Post: &model.Post{Id: "deleted", ChannelId: "dm", UserId: ownerID, Message: "do work", DeleteAt: 1}})
	if jobs, _ := f.st.ListJobs(f.ctx, store.JobFilter{}); len(jobs) != 0 {
		t.Fatal("deleted post dispatched")
	}
}

func TestBotTokenRotationTakesEffectOnNextPost(t *testing.T) {
	f, b := routedFixture(t)
	route(f, b, "before", "", "D", "first task", ownerID)
	if f.mm.PostToken(f.mm.Last().Id) != b.Token {
		t.Fatal("initial bot credentials were not used")
	}
	b.Token = "rotated_token"
	if err := f.st.UpdateBot(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	route(f, b, "after", "", "D", "second task", ownerID)
	if f.mm.PostToken(f.mm.Last().Id) != b.Token {
		t.Fatal("rotated bot still used old credentials")
	}
}

func TestDeletedBotResultNeverUsesCachedOrSharedCredentials(t *testing.T) {
	f, b := routedFixture(t)
	route(f, b, "task", "", "D", "do work", ownerID)
	j := f.onlyJob()
	before := f.mm.Message(j.StatusPostID)
	f.core.mm = f.mm // Even an available shared client must never replace this bot.
	if err := f.st.DeleteBot(f.ctx, b.UserID); err != nil {
		t.Fatal(err)
	}
	ack := f.core.OnResult(f.ctx, f.harness(), j.ID, protocol.JobResult{Status: protocol.StatusSucceeded, Text: "finished"})
	if !ack.OK {
		t.Fatalf("result was not persisted: %+v", ack)
	}
	if f.mm.Message(j.StatusPostID) != before {
		t.Fatal("deleted bot result posted with stale or shared credentials")
	}
	if _, err := f.core.api(f.ctx, "unknown_bot").CreatePost(f.ctx, &model.Post{Message: "wrong identity"}); err == nil {
		t.Fatal("unknown bot used the shared identity")
	}
}

func TestLegacyJobsWithoutSharedCredentialsRemainProcessable(t *testing.T) {
	for _, bot := range []string{"", "missing_bot"} {
		t.Run(bot, func(t *testing.T) {
			f := newFixture(t)
			f.core.mm = nil
			j := store.Job{ID: "legacy", HarnessID: harness1, MMUserID: ownerID, BotUserID: bot, State: store.JobRunning, StatusPostID: "old_status", CreatedAt: f.now, UpdatedAt: f.now}
			if err := f.st.CreateJob(f.ctx, j); err != nil {
				t.Fatal(err)
			}
			ack := f.core.OnResult(f.ctx, f.harness(), j.ID, protocol.JobResult{Status: protocol.StatusSucceeded, Text: "finished"})
			if !ack.OK {
				t.Fatalf("result: %+v", ack)
			}
			got, err := f.st.JobByID(f.ctx, j.ID)
			if err != nil || got.State != store.JobSucceeded || got.ResultText != "finished" {
				t.Fatalf("legacy result lost: %+v %v", got, err)
			}
			expires := f.now.Add(-time.Second)
			j.ID, j.State, j.ExpiresAt = "legacy_queued", store.JobQueued, &expires
			if err := f.st.CreateJob(f.ctx, j); err != nil {
				t.Fatal(err)
			}
			f.core.Sweep(f.ctx)
			got, err = f.st.JobByID(f.ctx, j.ID)
			if err != nil || got.State != store.JobExpired {
				t.Fatalf("legacy queue expiry failed: %+v %v", got, err)
			}
		})
	}
}

func TestBotApprovalAndResultUseBotAPI(t *testing.T) {
	f, b := routedFixture(t)
	route(f, b, "task", "", "O", "@alpha work", ownerID)
	j := f.onlyJob()
	f.core.OnAck(f.ctx, f.harness(), "dispatch", j.ID, protocol.Ack{OK: true})
	ack := f.core.OnApprovalRequest(f.ctx, f.harness(), j.ID, protocol.ApprovalRequest{ApprovalID: "approval", Tool: "Bash", Summary: "ls", ExpiresAt: f.now.Add(time.Hour).UnixMilli()})
	if !ack.OK || f.mm.PostToken(f.mm.Last().Id) != b.Token {
		t.Fatal("approval did not use the bot API")
	}
	ack = f.core.OnResult(f.ctx, f.harness(), j.ID, protocol.JobResult{Status: protocol.StatusSucceeded, Text: "finished"})
	if !ack.OK || !strings.Contains(f.mm.Message(j.StatusPostID), "finished") {
		t.Fatal("missing result")
	}
}

func TestBotRoutingDeduplicatesConcurrentAndLateDelivery(t *testing.T) {
	f, b := routedFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); route(f, b, "same", "", "O", "@alpha do work", ownerID) }()
	}
	wg.Wait()
	j := f.onlyJob()
	if _, err := f.st.TransitionJob(f.ctx, j.ID, store.ActiveStates, store.JobSucceeded, store.JobPatch{}); err != nil {
		t.Fatal(err)
	}
	// A new core simulates a broker restart before the webhook arrives late.
	f.core = New(f.core.cfg, f.st, nil, f.hub, f.core.log).WithOnboarding(nil, f.core.newPoster)
	route(f, b, "same", "", "O", "@alpha do work", ownerID)
	f.onlyJob()
	if f.hub.count(protocol.TypeJobDispatch) != 1 || len(*f.mm.Order) != 1 {
		t.Fatalf("duplicate dispatch/status: %d/%d", f.hub.count(protocol.TypeJobDispatch), len(*f.mm.Order))
	}
}

func TestBotsShareThreadsIndependently(t *testing.T) {
	f, a := routedFixture(t)
	b := store.Bot{UserID: "bot_b", MMUserID: ownerID, Username: "beta", Token: "token_b", HarnessID: harness1, CreatedAt: time.Now()}
	if err := f.st.CreateBot(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	route(f, a, "unaddressed", "thread", "P", "do work", ownerID)
	if f.hub.count(protocol.TypeJobDispatch) != 0 {
		t.Fatal("unaddressed thread started")
	}
	route(f, a, "shared_trigger", "thread", "P", "@alpha @beta work", ownerID)
	route(f, b, "shared_trigger", "thread", "P", "@alpha @beta work", ownerID)
	jobs, _ := f.st.ListJobs(f.ctx, store.JobFilter{})
	if len(jobs) != 2 {
		t.Fatalf("bots should share thread: %+v", jobs)
	}
	route(f, a, "cancel_a", "thread", "P", "cancel", ownerID)
	if f.hub.count(protocol.TypeJobCancel) != 1 {
		t.Fatal("cancel affected another bot")
	}
	for _, j := range jobs {
		_, _ = f.st.TransitionJob(f.ctx, j.ID, store.ActiveStates, store.JobSucceeded, store.JobPatch{})
	}
	route(f, a, "other_bot", "thread", "P", "@beta next", ownerID)
	if f.hub.count(protocol.TypeJobDispatch) != 2 {
		t.Fatal("another bot mention resumed receiver")
	}
	route(f, a, "followup", "thread", "P", "continue", ownerID)
	if f.hub.count(protocol.TypeJobDispatch) != 3 {
		t.Fatal("owner follow-up did not resume")
	}
}
