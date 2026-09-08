package cli

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"mime"
	"net/http"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

type webhookAPI interface {
	GetPost(context.Context, string) (*model.Post, error)
	GetChannel(context.Context, string) (*model.Channel, error)
}

func outgoingHandler(st store.BotStore, client func(string) webhookAPI, onPost func(context.Context, string, mattermost.PostedEvent)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		var payload struct {
			Token     string `json:"token"`
			PostID    string `json:"post_id"`
			TeamID    string `json:"team_id"`
			ChannelID string `json:"channel_id"`
			UserID    string `json:"user_id"`
		}
		contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		switch contentType {
		case "application/json":
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
		case "application/x-www-form-urlencoded":
			if err := r.ParseForm(); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			payload.Token = r.PostForm.Get("token")
			payload.PostID = r.PostForm.Get("post_id")
			payload.TeamID = r.PostForm.Get("team_id")
			payload.ChannelID = r.PostForm.Get("channel_id")
			payload.UserID = r.PostForm.Get("user_id")
		default:
			http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
			return
		}
		b, err := st.BotByUserID(r.Context(), r.PathValue("botID"))
		if err != nil || b.HarnessID == "" || b.OutgoingToken == "" || subtle.ConstantTimeCompare([]byte(payload.Token), []byte(b.OutgoingToken)) != 1 {
			http.Error(w, "invalid webhook token", http.StatusUnauthorized)
			return
		}
		if payload.PostID == "" || payload.TeamID != b.TeamID || payload.UserID != b.MMUserID {
			http.Error(w, "invalid webhook request", http.StatusForbidden)
			return
		}
		// Retrieve the actual post with the bot's credentials. A webhook token alone
		// must never allow a caller to invent the owner, channel, text or thread.
		api := client(b.Token)
		p, err := api.GetPost(r.Context(), payload.PostID)
		if err != nil {
			http.Error(w, "post unavailable", http.StatusBadGateway)
			return
		}
		if p == nil || p.Id != payload.PostID || p.UserId != b.MMUserID || p.ChannelId != payload.ChannelID || p.DeleteAt != 0 {
			http.Error(w, "post does not match webhook", http.StatusForbidden)
			return
		}
		ch, err := api.GetChannel(r.Context(), p.ChannelId)
		if err != nil {
			http.Error(w, "channel unavailable", http.StatusBadGateway)
			return
		}
		if ch == nil || ch.TeamId != b.TeamID || ch.Type != model.ChannelTypeOpen {
			http.Error(w, "channel does not match webhook", http.StatusForbidden)
			return
		}
		onPost(r.Context(), b.UserID, mattermost.PostedEvent{Post: p, ChannelType: string(ch.Type)})
		// No webhook response post: the bot API sends threaded, editable status.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
	})
}
