// Package bots maintains one Mattermost event connection per provisioned bot.
package bots

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

type listener interface {
	Listen(context.Context, *slog.Logger, func(context.Context, mattermost.PostedEvent)) error
}
type connection struct {
	token  string
	cancel context.CancelFunc
}
type Manager struct {
	st      store.BotStore
	factory func(string) listener
	onPost  func(context.Context, string, mattermost.PostedEvent)
	log     *slog.Logger
}

func New(st store.BotStore, url string, onPost func(context.Context, string, mattermost.PostedEvent), log *slog.Logger) *Manager {
	return &Manager{st: st, factory: func(token string) listener { return mattermost.New(url, token) }, onPost: onPost, log: log}
}

// Run discovers bots added by onboarding and replaces connections after token
// rotation. Every connection owns its reconnect loop and is stopped on removal.
func (m *Manager) Run(ctx context.Context) {
	active := map[string]connection{}
	var wg sync.WaitGroup
	defer func() {
		for _, c := range active {
			c.cancel()
		}
		wg.Wait()
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		list, err := m.st.ListBots(ctx)
		if err != nil {
			if ctx.Err() == nil {
				m.log.Error("list bot connections", "err", err)
			}
		} else {
			desired := map[string]bool{}
			for _, b := range list {
				if b.HarnessID == "" || b.Token == "" {
					continue
				}
				desired[b.UserID] = true
				if old, ok := active[b.UserID]; ok {
					if old.token == b.Token {
						continue
					}
					old.cancel()
				}
				child, cancel := context.WithCancel(ctx)
				active[b.UserID] = connection{token: b.Token, cancel: cancel}
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = m.factory(b.Token).Listen(child, m.log.With("bot", b.Username), func(ctx context.Context, ev mattermost.PostedEvent) { m.onPost(ctx, b.UserID, ev) })
				}()
			}
			for id, c := range active {
				if !desired[id] {
					c.cancel()
					delete(active, id)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
