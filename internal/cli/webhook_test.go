package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store/sqlite"
)

type webhookFake struct {
	post    *model.Post
	channel *model.Channel
	reads   int
}

func (f *webhookFake) GetPost(context.Context, string) (*model.Post, error) {
	f.reads++
	return f.post, nil
}
func (f *webhookFake) GetChannel(context.Context, string) (*model.Channel, error) {
	return f.channel, nil
}

func TestOutgoingWebhookAuthenticatesAndFetchesOriginalPost(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	if err := st.CreateHarness(ctx, store.Harness{ID: "h", MMUserID: "owner", TokenHash: "hash", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBot(ctx, store.Bot{UserID: "bot", MMUserID: "owner", Username: "worker", Token: "bot-secret", HarnessID: "h", TeamID: "team", OutgoingToken: "hook-secret", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, token, owner, team, actualOwner, actualChannel string
		code                                                 int
	}{
		{"valid", "hook-secret", "owner", "team", "owner", "channel", 200},
		{"invalid token", "wrong", "owner", "team", "owner", "channel", 401},
		{"foreign owner", "hook-secret", "other", "team", "owner", "channel", 403},
		{"foreign team", "hook-secret", "owner", "other", "owner", "channel", 403},
		{"forged owner", "hook-secret", "owner", "team", "other", "channel", 403},
		{"forged channel", "hook-secret", "owner", "team", "owner", "other", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &webhookFake{post: &model.Post{Id: "post", UserId: tc.actualOwner, ChannelId: tc.actualChannel, Message: "@worker real task", RootId: "root"}, channel: &model.Channel{Id: "channel", TeamId: "team", Type: model.ChannelTypeOpen}}
			dispatched := 0
			mux := http.NewServeMux()
			mux.Handle("POST /webhooks/mattermost/{botID}", outgoingHandler(st, func(token string) webhookAPI {
				if token != "bot-secret" {
					t.Fatal("wrong API credentials")
				}
				return fake
			}, func(_ context.Context, bot string, e mattermost.PostedEvent) {
				dispatched++
				if bot != "bot" || e.Post.Message != "@worker real task" || e.Post.RootId != "root" {
					t.Fatal("did not use actual post")
				}
			}))
			body := url.Values{"token": {tc.token}, "user_id": {tc.owner}, "team_id": {tc.team}, "channel_id": {"channel"}, "post_id": {"post"}, "text": {"forged task"}}
			req := httptest.NewRequest("POST", "/webhooks/mattermost/bot", strings.NewReader(body.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			res := httptest.NewRecorder()
			mux.ServeHTTP(res, req)
			if res.Code != tc.code {
				t.Fatalf("got %d: %s", res.Code, res.Body.String())
			}
			if (dispatched == 1) != (tc.code == 200) {
				t.Fatalf("dispatches=%d", dispatched)
			}
			if tc.code == 401 && fake.reads != 0 {
				t.Fatal("unauthenticated API access")
			}
		})
	}
}

func TestBrokerRequiresOAuthConfiguration(t *testing.T) {
	o := brokerOptions{mmURL: "https://mm.example", publicURL: "https://bridge.example", callbackSecret: strings.Repeat("s", 32)}
	if err := o.validate(); err == nil || !strings.Contains(err.Error(), "oauth-client-id") {
		t.Fatalf("missing OAuth credentials: %v", err)
	}
	o.oauthClientID = "client"
	o.oauthClientSecret = "secret"
	o.botProvisioningToken = "admin-pat"
	if err := o.validate(); err != nil {
		t.Fatal(err)
	}
}
