package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost/mmtest"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

type onboardingProvisioner struct {
	*mmtest.Fake
	fail     string
	incoming map[string]*model.IncomingWebhook
	outgoing map[string]*model.OutgoingWebhook
	disabled []string
}

func (p *onboardingProvisioner) failure(op string) error {
	if p.fail == op {
		return errors.New("secret-access-token must never be exposed")
	}
	return nil
}
func (p *onboardingProvisioner) CreateBotToken(ctx context.Context, id, desc string) (string, error) {
	if err := p.failure("token"); err != nil {
		return "", err
	}
	return p.Fake.CreateBotToken(ctx, id, desc)
}
func (p *onboardingProvisioner) AddChannelMember(ctx context.Context, channel, user string) error {
	if err := p.failure("membership"); err != nil {
		return err
	}
	return p.Fake.AddChannelMember(ctx, channel, user)
}
func (p *onboardingProvisioner) CreateIncomingWebhook(ctx context.Context, h *model.IncomingWebhook) (*model.IncomingWebhook, error) {
	if err := p.failure("incoming"); err != nil {
		return nil, err
	}
	cp := *h
	cp.Id = "in_" + h.UserId
	if p.fail == "incoming-owner" {
		cp.UserId = "installer"
	}
	p.incoming[cp.Id] = &cp
	return &cp, nil
}
func (p *onboardingProvisioner) CreateOutgoingWebhook(ctx context.Context, h *model.OutgoingWebhook) (*model.OutgoingWebhook, error) {
	if err := p.failure("outgoing"); err != nil {
		return nil, err
	}
	cp := *h
	cp.Id = fmt.Sprintf("out_%d", len(p.outgoing))
	cp.Token = "secret_" + cp.Id
	p.outgoing[cp.Id] = &cp
	return &cp, nil
}
func (p *onboardingProvisioner) DeleteIncomingWebhook(ctx context.Context, id string) error {
	delete(p.incoming, id)
	return nil
}
func (p *onboardingProvisioner) DeleteOutgoingWebhook(ctx context.Context, id string) error {
	delete(p.outgoing, id)
	return nil
}
func (p *onboardingProvisioner) DisableBot(ctx context.Context, id string) error {
	p.disabled = append(p.disabled, id)
	return nil
}
func oauthFixture(t *testing.T) (*fixture, *onboardingProvisioner) {
	f := newFixture(t)
	p := &onboardingProvisioner{Fake: f.mm, incoming: map[string]*model.IncomingWebhook{}, outgoing: map[string]*model.OutgoingWebhook{}}
	f.core.WithProvisioner(func(ctx context.Context, team string) (mattermost.Provisioner, error) {
		if team != "team_1" {
			return nil, errors.New("not installed")
		}
		return p, nil
	}, func(token string) mattermost.API { return f.mm.WithToken(token) })
	return f, p
}
func initCommand(f *fixture, code string) *model.CommandResponse {
	return f.core.HandleCommand(f.ctx, CommandRequest{UserID: ownerID, TeamID: "team_1", ChannelID: "channel", Text: "init " + code})
}

func TestOAuthOnboardingCreatesDistinctBoundBots(t *testing.T) {
	f, p := oauthFixture(t)
	var prior store.Bot
	for i := 0; i < 2; i++ {
		start, err := f.core.StartInit(f.ctx, "", fmt.Sprintf("machine-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if len(start.PollToken) < 32 {
			t.Fatal("missing secure poll token")
		}
		if _, err = f.core.PollInit(f.ctx, start.Code, start.PollToken); !errors.Is(err, ErrInitPending) {
			t.Fatal(err)
		}
		response := initCommand(f, start.Code)
		if !strings.Contains(response.Text, "Paired") {
			t.Fatal(response.Text)
		}
		if _, err = f.core.PollInit(f.ctx, start.Code, "wrong"); !errors.Is(err, ErrInitUnknown) {
			t.Fatal("poll without secret succeeded", err)
		}
		result, err := f.core.PollInit(f.ctx, start.Code, start.PollToken)
		if err != nil {
			t.Fatal(err)
		}
		bot, err := f.st.BotByUsername(f.ctx, result.BotUsername)
		if err != nil {
			t.Fatal(err)
		}
		if bot.HarnessID != result.HarnessID || bot.MMUserID != ownerID {
			t.Fatalf("wrong binding: %+v", bot)
		}
		if bot.IncomingHookID == "" || bot.OutgoingHookID == "" || bot.OutgoingToken == "" {
			t.Fatal("missing hooks")
		}
		if p.incoming[bot.IncomingHookID].UserId != bot.UserID {
			t.Fatal("incoming hook not owned by bot")
		}
		out := p.outgoing[bot.OutgoingHookID]
		if out.ChannelId != "" || out.TeamId != "team_1" || out.TriggerWords[0] != "@"+bot.Username || out.CallbackURLs[0] != "https://broker.test/webhooks/mattermost/"+bot.UserID {
			t.Fatalf("wrong outgoing config: %+v", out)
		}
		if i > 0 && (prior.UserID == bot.UserID || prior.HarnessID == bot.HarnessID || prior.Username == bot.Username) {
			t.Fatal("second pairing reused first bot")
		}
		prior = bot
		if _, err = f.core.PollInit(f.ctx, start.Code, start.PollToken); !errors.Is(err, ErrInitConsumed) {
			t.Fatal(err)
		}
		initCommand(f, start.Code)
	}
	bots, _ := f.st.BotsByOwner(f.ctx, ownerID)
	if len(bots) != 2 || len(p.Bots) != 2 {
		t.Fatal("replay created another bot")
	}
	if p.Memberships["channel:"+botID] || p.Memberships["team_1:"+botID] {
		t.Fatal("shared bot joined channel")
	}
	status := f.core.HandleCommand(f.ctx, CommandRequest{UserID: ownerID, Text: "status"})
	for _, b := range bots {
		if !strings.Contains(status.Text, b.Username) || !strings.Contains(status.Text, b.HarnessID) {
			t.Fatal(status.Text)
		}
	}
}

func TestOAuthOnboardingOwnershipAndJoin(t *testing.T) {
	f, p := oauthFixture(t)
	args := CommandRequest{UserID: ownerID, TeamID: "team_1", ChannelID: "channel", Text: "bot create worker-one " + harness1}
	if got := f.core.HandleCommand(f.ctx, args); !strings.Contains(got.Text, "worker-one") {
		t.Fatal(got.Text)
	}
	args.Text = "bot create worker-two " + harness1
	if got := f.core.HandleCommand(f.ctx, args); strings.Contains(got.Text, "failed") {
		t.Fatal(got.Text)
	}
	bots, _ := f.st.BotsByOwner(f.ctx, ownerID)
	if len(bots) != 2 {
		t.Fatalf("want two bots: %+v", bots)
	}
	args.UserID = otherID
	args.Text = "bot create stolen " + harness1
	f.core.HandleCommand(f.ctx, args)
	args.Text = "join worker-one"
	args.ChannelID = "other-channel"
	f.core.HandleCommand(f.ctx, args)
	if len(p.Bots) != 2 || p.Memberships["other-channel:"+bots[0].UserID] {
		t.Fatal("ownership check bypassed")
	}
	args.UserID = ownerID
	got := f.core.HandleCommand(f.ctx, args)
	if !strings.Contains(got.Text, "now in this channel") {
		t.Fatal(got.Text)
	}
}

type failingSaveBotStore struct{ store.Store }

func (s failingSaveBotStore) CreateBot(context.Context, store.Bot) error {
	return errors.New("database credential secret")
}

type failingCompleteStore struct{ store.Store }

func (s failingCompleteStore) CompleteInit(context.Context, string, time.Time, store.Harness, string, string) error {
	return errors.New("db unavailable")
}
func TestOAuthOnboardingRollsBack(t *testing.T) {
	for _, step := range []string{"token", "membership", "incoming", "incoming-owner", "outgoing", "save", "complete"} {
		t.Run(step, func(t *testing.T) {
			f, p := oauthFixture(t)
			p.fail = step
			if step == "save" {
				f.core.st = failingSaveBotStore{f.st}
			}
			if step == "complete" {
				f.core.st = failingCompleteStore{f.st}
			}
			start, err := f.core.StartInit(f.ctx, "worker", "machine")
			if err != nil {
				t.Fatal(err)
			}
			got := initCommand(f, start.Code)
			if !strings.Contains(got.Text, "failed") || strings.Contains(got.Text, "secret-access-token") {
				t.Fatal(got.Text)
			}
			bots, _ := f.st.BotsByOwner(f.ctx, ownerID)
			harnesses, _ := f.st.HarnessesByUser(f.ctx, ownerID)
			if len(bots) != 0 || len(harnesses) != 1 || len(p.incoming) != 0 || len(p.outgoing) != 0 || len(p.disabled) != 1 {
				t.Fatalf("partial resources remain bots=%d harnesses=%d incoming=%d outgoing=%d disabled=%d", len(bots), len(harnesses), len(p.incoming), len(p.outgoing), len(p.disabled))
			}
			req, _ := f.st.InitByCode(f.ctx, hashCode(start.Code))
			if req.Claimed() {
				t.Fatal("failed init consumed code")
			}
		})
	}
}
func TestOAuthOnboardingRejectsInvalidAndConcurrentClaims(t *testing.T) {
	f, p := oauthFixture(t)
	initCommand(f, "unknown")
	if len(p.Bots) != 0 {
		t.Fatal("unknown init provisioned")
	}
	old, _ := f.core.StartInit(f.ctx, "", "old")
	f.now = f.now.Add(InitCodeTTL)
	initCommand(f, old.Code)
	if len(p.Bots) != 0 {
		t.Fatal("expired init provisioned")
	}
	start, _ := f.core.StartInit(f.ctx, "", "new")
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); initCommand(f, start.Code) }()
	}
	wg.Wait()
	if len(p.Bots) != 1 {
		t.Fatalf("concurrent claims created %d bots", len(p.Bots))
	}
}
func TestOAuthOnboardingPrivateChannel(t *testing.T) {
	f, p := oauthFixture(t)
	p.Channels["channel"] = &model.Channel{Id: "channel", TeamId: "team_1", Type: model.ChannelTypePrivate}
	start, _ := f.core.StartInit(f.ctx, "private-worker", "machine")
	got := initCommand(f, start.Code)
	if !strings.Contains(got.Text, "Paired") {
		t.Fatal(got.Text)
	}
	for _, h := range p.outgoing {
		if h.ChannelId != "" {
			t.Fatal("outgoing webhook restricted to private channel")
		}
	}
}

func TestOAuthOnboardingNameSelectionAndValidation(t *testing.T) {
	f, p := oauthFixture(t)
	start, err := f.core.StartInit(f.ctx, "laptop-name", "machine")
	if err != nil {
		t.Fatal(err)
	}
	args := CommandRequest{UserID: ownerID, TeamID: "team_1", ChannelID: "channel", Text: "init " + start.Code + " slash-name"}
	if got := f.core.HandleCommand(f.ctx, args); !strings.Contains(got.Text, "Paired") {
		t.Fatal(got.Text)
	}
	result, err := f.core.PollInit(f.ctx, start.Code, start.PollToken)
	if err != nil || result.BotUsername != "slash-name" {
		t.Fatalf("slash override: %+v %v", result, err)
	}
	for _, command := range []string{"bot create slash-name " + harness1, "bot create Bad! " + harness1, "bot create"} {
		args.Text = command
		f.core.HandleCommand(f.ctx, args)
	}
	if len(p.Bots) != 1 {
		t.Fatal("invalid or duplicate names provisioned")
	}
	start, err = f.core.StartInit(f.ctx, "", "private")
	if err != nil {
		t.Fatal(err)
	}
	p.Channels["channel"] = &model.Channel{Id: "channel", Type: model.ChannelTypeDirect}
	args.Text = "init " + start.Code
	if got := f.core.HandleCommand(f.ctx, args); strings.Contains(got.Text, "Paired") {
		t.Fatal("paired in a DM")
	}
	if len(p.Bots) != 1 {
		t.Fatal("DM created a bot")
	}
}
