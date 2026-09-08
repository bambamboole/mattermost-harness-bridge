package mattermost

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mattermost/mattermost/server/public/model"
)

func TestProvisionerNativeWebhookLifecycleWithSeparateTokenIssuer(t *testing.T) {
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedAuth := "BEARER oauth-installer"
		if r.URL.Path == "/api/v4/users/bot-user/tokens" {
			expectedAuth = "BEARER token-issuer"
		}
		if got := r.Header.Get("Authorization"); got != expectedAuth {
			t.Errorf("wrong installer authorization: %q", got)
		}
		key := r.Method + " " + r.URL.Path
		seen[key] = true
		w.Header().Set("Content-Type", "application/json")
		switch key {
		case "POST /api/v4/bots":
			_, _ = w.Write([]byte(`{"user_id":"bot-user","username":"worker"}`))
		case "POST /api/v4/users/bot-user/tokens":
			var token model.UserAccessToken
			if err := json.NewDecoder(r.Body).Decode(&token); err != nil {
				t.Error(err)
			}
			if token.Description != "mhb broker" {
				t.Error("lost bot token description")
			}
			_, _ = w.Write([]byte(`{"id":"bot-token-id","token":"created-bot-token","user_id":"bot-user"}`))
		case "POST /api/v4/teams/team/members":
			_, _ = w.Write([]byte(`{"team_id":"team","user_id":"bot-user"}`))
		case "POST /api/v4/channels/private-channel/members":
			_, _ = w.Write([]byte(`{"channel_id":"private-channel","user_id":"bot-user"}`))
		case "GET /api/v4/users/owner", "GET /api/v4/users/username/owner":
			_, _ = w.Write([]byte(`{"id":"owner","username":"owner"}`))
		case "GET /api/v4/channels/private-channel":
			_, _ = w.Write([]byte(`{"id":"private-channel","team_id":"team","type":"P"}`))
		case "POST /api/v4/hooks/incoming":
			var h model.IncomingWebhook
			if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
				t.Error(err)
			}
			if h.UserId != "bot-user" || h.ChannelId != "private-channel" {
				t.Errorf("lost bot ownership or channel: %+v", h)
			}
			h.Id = "incoming-id"
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(h)
		case "POST /api/v4/hooks/outgoing":
			var h model.OutgoingWebhook
			if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
				t.Error(err)
			}
			if h.TeamId != "team" || h.ChannelId != "" || len(h.CallbackURLs) != 1 || h.TriggerWords[0] != "@worker" {
				t.Errorf("wrong outgoing webhook: %+v", h)
			}
			h.Id = "outgoing-id"
			h.Token = "outgoing-secret"
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(h)
		case "DELETE /api/v4/hooks/incoming/incoming-id", "DELETE /api/v4/hooks/outgoing/outgoing-id":
			_, _ = w.Write([]byte(`{"status":"OK"}`))
		case "POST /api/v4/bots/bot-user/disable":
			_, _ = w.Write([]byte(`{"user_id":"bot-user","delete_at":1}`))
		default:
			t.Errorf("unexpected request %s", key)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := WithBotTokenIssuer(New(server.URL, "oauth-installer"), New(server.URL, "token-issuer"))
	ctx := context.Background()
	bot, err := client.CreateBot(ctx, "worker", "Worker", "Coding agent")
	if err != nil || bot.UserId != "bot-user" {
		t.Fatalf("bot: %+v %v", bot, err)
	}
	token, err := client.CreateBotToken(ctx, bot.UserId, "mhb broker")
	if err != nil || token != "created-bot-token" {
		t.Fatalf("bot token: %v", err)
	}
	if err := client.AddTeamMember(ctx, "team", bot.UserId); err != nil {
		t.Fatal(err)
	}
	if err := client.AddChannelMember(ctx, "private-channel", bot.UserId); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetUser(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetUserByUsername(ctx, "owner"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetChannel(ctx, "private-channel"); err != nil {
		t.Fatal(err)
	}
	incoming, err := client.CreateIncomingWebhook(ctx, &model.IncomingWebhook{UserId: "bot-user", ChannelId: "private-channel"})
	if err != nil || incoming.Id != "incoming-id" || incoming.UserId != "bot-user" {
		t.Fatalf("incoming: %+v %v", incoming, err)
	}
	outgoing, err := client.CreateOutgoingWebhook(ctx, &model.OutgoingWebhook{TeamId: "team", TriggerWords: model.StringArray{"@worker"}, CallbackURLs: model.StringArray{"https://broker.test/webhooks/mattermost/bot-user"}})
	if err != nil || outgoing.Token != "outgoing-secret" {
		t.Fatalf("outgoing: %+v %v", outgoing, err)
	}
	if err := client.DeleteOutgoingWebhook(ctx, outgoing.Id); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteIncomingWebhook(ctx, incoming.Id); err != nil {
		t.Fatal(err)
	}
	if err := client.DisableBot(ctx, "bot-user"); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 12 {
		t.Fatalf("got %d lifecycle operations", len(seen))
	}
}
