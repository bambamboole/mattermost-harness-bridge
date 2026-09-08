// broker is the public component: it talks to Mattermost, accepts harness
// connections, and receives approval button callbacks.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/core"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/hub"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store/sqlite"
	"github.com/bambamboole/mattermost-harness-bridge/internal/version"
)

type config struct {
	MMURL             string
	MMToken           string
	PublicURL         string
	ListenAddr        string
	DBPath            string
	CallbackSecret    string
	MinHarnessVersion string
	QueueTTL          time.Duration
	GracePeriod       time.Duration
	JobTimeout        time.Duration
}

func loadConfig() (config, error) {
	c := config{
		MMURL:             os.Getenv("MM_URL"),
		MMToken:           os.Getenv("MM_BOT_TOKEN"),
		PublicURL:         strings.TrimRight(os.Getenv("PUBLIC_URL"), "/"),
		ListenAddr:        env("LISTEN_ADDR", ":8080"),
		DBPath:            env("DB_PATH", "broker.db"),
		CallbackSecret:    os.Getenv("CALLBACK_SECRET"),
		MinHarnessVersion: os.Getenv("MIN_HARNESS_VERSION"),
	}
	var missing []string
	for k, v := range map[string]string{"MM_URL": c.MMURL, "MM_BOT_TOKEN": c.MMToken, "PUBLIC_URL": c.PublicURL, "CALLBACK_SECRET": c.CallbackSecret} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("missing env: %s", strings.Join(missing, ", "))
	}
	if len(c.CallbackSecret) < 32 {
		return c, errors.New("CALLBACK_SECRET must be at least 32 characters")
	}
	var err error
	if c.QueueTTL, err = envDuration("QUEUE_TTL", 5*time.Minute); err != nil {
		return c, err
	}
	if c.GracePeriod, err = envDuration("GRACE_PERIOD", 10*time.Minute); err != nil {
		return c, err
	}
	if c.JobTimeout, err = envDuration("JOB_TIMEOUT", 30*time.Minute); err != nil {
		return c, err
	}
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return d, nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("broker exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := sqlite.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	mm := mattermost.New(cfg.MMURL, cfg.MMToken)
	me, err := mm.Me(ctx)
	if err != nil {
		return fmt.Errorf("mattermost login: %w", err)
	}
	log.Info("broker starting", "version", version.Version, "bot", me.Username, "bot_id", me.Id)

	h := hub.New(st, nil, log)
	h.MinHarnessVersion = cfg.MinHarnessVersion
	c := core.New(core.Config{
		BotUserID:      me.Id,
		BotUsername:    me.Username,
		PublicURL:      cfg.PublicURL,
		CallbackSecret: []byte(cfg.CallbackSecret),
		QueueTTL:       cfg.QueueTTL,
		GracePeriod:    cfg.GracePeriod,
		JobTimeout:     cfg.JobTimeout,
	}, st, mm, h, log)
	h.Handler = c

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.Handle("GET "+protocol.Path, h)
	mux.HandleFunc("POST /pair", func(w http.ResponseWriter, r *http.Request) {
		var req core.PairRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		res, err := c.HandlePair(r.Context(), req)
		switch {
		case errors.Is(err, core.ErrBadPairingCode):
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		case err != nil:
			log.Error("pair", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, res)
	})
	mux.HandleFunc("POST /callback/approval", func(w http.ResponseWriter, r *http.Request) {
		var req model.PostActionIntegrationRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		writeJSON(w, c.HandleCallback(r.Context(), &req))
	})

	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 2)
	go func() {
		log.Info("listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		errCh <- mm.Listen(ctx, log, c.HandlePost)
	}()
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.Sweep(ctx)
			}
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("component failed", "err", err)
		}
	}
	log.Info("shutting down")
	h.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
