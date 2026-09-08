package bots

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store/sqlite"
)

type testListener struct {
	token         string
	starts, stops chan string
}

func (l testListener) Listen(ctx context.Context, _ *slog.Logger, post func(context.Context, mattermost.PostedEvent)) error {
	l.starts <- l.token
	post(ctx, mattermost.PostedEvent{SenderName: l.token})
	<-ctx.Done()
	l.stops <- l.token
	return ctx.Err()
}
func TestManagerStartsAndStopsIndependentBotConnections(t *testing.T) {
	st, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := st.CreateHarness(ctx, store.Harness{ID: "h", MMUserID: "owner", TokenHash: "hash", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if err := st.CreateBot(ctx, store.Bot{UserID: id, MMUserID: "owner", Username: id, Token: id, HarnessID: "h", CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	starts, stops, events := make(chan string, 4), make(chan string, 4), make(chan string, 4)
	m := New(st, "unused", func(_ context.Context, id string, e mattermost.PostedEvent) { events <- id + ":" + e.SenderName }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.factory = func(token string) listener { return testListener{token, starts, stops} }
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	receive := func(ch <-chan string) string {
		t.Helper()
		select {
		case value := <-ch:
			return value
		case <-time.After(3 * time.Second):
			t.Fatal("connection event timeout")
			return ""
		}
	}
	seen := map[string]bool{receive(starts): true, receive(starts): true}
	if !seen["a"] || !seen["b"] {
		t.Fatal(seen)
	}
	seen = map[string]bool{receive(events): true, receive(events): true}
	if !seen["a:a"] || !seen["b:b"] {
		t.Fatal("bot identity lost", seen)
	}
	if err := st.DeleteBot(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if stopped := receive(stops); stopped != "a" {
		t.Fatalf("wrong bot stopped: %s", stopped)
	}
	cancel()
	if stopped := receive(stops); stopped != "b" {
		t.Fatal(stopped)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown timed out")
	}
}
