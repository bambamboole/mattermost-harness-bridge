package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

func TestMultipleBotsAndAtomicPairing(t *testing.T) {
	s, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	now := time.Now().UTC()
	for _, id := range []string{"b1", "b2"} {
		if err := s.CreateBot(ctx, store.Bot{UserID: id, MMUserID: "owner", Username: id, CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	bots, err := s.BotsByOwner(ctx, "owner")
	if err != nil || len(bots) != 2 {
		t.Fatalf("bots=%v err=%v", bots, err)
	}
	if err := s.CreateInit(ctx, store.InitRequest{CodeHash: "code", PollTokenHash: "poll", CreatedAt: now, ExpiresAt: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	h := store.Harness{ID: "h1", MMUserID: "other", TokenHash: "token", CreatedAt: now}
	if err := s.CompleteInit(ctx, "code", now, h, "b1", "secret"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("cross-owner pairing: %v", err)
	}
	if _, err := s.HarnessByID(ctx, "h1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("orphan harness: %v", err)
	}
	h.MMUserID = "owner"
	if err := s.CompleteInit(ctx, "code", now, h, "b1", "secret"); err != nil {
		t.Fatal(err)
	}
	b, err := s.BotByUserID(ctx, "b1")
	if err != nil || b.HarnessID != "h1" {
		t.Fatalf("binding=%v err=%v", b, err)
	}
	h.ID = "h2"
	h.TokenHash = "token2"
	if err := s.CompleteInit(ctx, "code", now, h, "b2", "secret2"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("replay: %v", err)
	}
	if _, err := s.HarnessByID(ctx, "h2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("orphan on replay: %v", err)
	}
	r, err := s.FetchInit(ctx, "code", now)
	if err != nil || r.BotUserID != "b1" || r.HarnessToken != "secret" || r.PollTokenHash != "poll" {
		t.Fatalf("result=%+v err=%v", r, err)
	}
}

func TestInstallationPersists(t *testing.T) {
	path := t.TempDir() + "/broker.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	want := store.Installation{TeamID: "team", UserID: "admin", Credentials: "encrypted", CommandID: "cmd", CommandToken: "token"}
	if err := s.SaveInstallation(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	got, err := s.InstallationByTeam(context.Background(), "team")
	if err != nil || got != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}
