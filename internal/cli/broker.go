package cli

import (
	"context"
	"crypto/sha256"
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
	"github.com/spf13/cobra"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/bots"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/core"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/hub"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/oauth"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store/sqlite"
	"github.com/bambamboole/mattermost-harness-bridge/internal/version"
)

type brokerOptions struct {
	mmURL                string
	oauthClientID        string
	oauthClientSecret    string
	botProvisioningToken string
	publicURL            string
	listen               string
	dbPath               string
	callbackSecret       string
	minHarnessVersion    string
	queueTTL             time.Duration
	gracePeriod          time.Duration
	jobTimeout           time.Duration
}

func newBrokerCmd() *cobra.Command {
	var o brokerOptions
	cmd := &cobra.Command{
		Use:   "broker",
		Short: "Run the broker (Mattermost bot, harness hub, approval callbacks)",
		Long: `Run the broker. Every flag falls back to the environment variable named
in its help text, so containers can be configured without arguments.`,
		Args: cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return o.validate()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			return runBroker(cmd.Context(), o, log)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.mmURL, "mm-url", env("MM_URL", ""), "Mattermost base URL [MM_URL]")
	f.StringVar(&o.oauthClientID, "oauth-client-id", env("MM_OAUTH_CLIENT_ID", ""), "Mattermost OAuth application client ID [MM_OAUTH_CLIENT_ID]")
	f.StringVar(&o.oauthClientSecret, "oauth-client-secret", env("MM_OAUTH_CLIENT_SECRET", ""), "Mattermost OAuth client secret [MM_OAUTH_CLIENT_SECRET]")
	f.StringVar(&o.botProvisioningToken, "bot-provisioning-token", env("MM_BOT_PROVISIONING_TOKEN", ""), "admin personal access token used only to mint bot tokens (Mattermost forbids OAuth for this operation) [MM_BOT_PROVISIONING_TOKEN]")
	f.StringVar(&o.publicURL, "public-url", env("PUBLIC_URL", ""), "URL under which Mattermost and harnesses reach this broker [PUBLIC_URL]")
	f.StringVar(&o.listen, "listen", env("LISTEN_ADDR", ":8080"), "listen address [LISTEN_ADDR]")
	f.StringVar(&o.dbPath, "db", env("DB_PATH", "broker.db"), "SQLite database path [DB_PATH]")
	f.StringVar(&o.callbackSecret, "callback-secret", env("CALLBACK_SECRET", ""), "HMAC secret for approval buttons, at least 32 chars [CALLBACK_SECRET]")
	f.StringVar(&o.minHarnessVersion, "min-harness-version", env("MIN_HARNESS_VERSION", ""), "reject older harnesses [MIN_HARNESS_VERSION]")
	f.DurationVar(&o.queueTTL, "queue-ttl", envDuration("QUEUE_TTL", 5*time.Minute), "how long a job waits for an offline harness [QUEUE_TTL]")
	f.DurationVar(&o.gracePeriod, "grace-period", envDuration("GRACE_PERIOD", 10*time.Minute), "offline harness with a running job counts as lost after this [GRACE_PERIOD]")
	f.DurationVar(&o.jobTimeout, "job-timeout", envDuration("JOB_TIMEOUT", 30*time.Minute), "maximum job runtime [JOB_TIMEOUT]")
	return cmd
}

func (o *brokerOptions) validate() error {
	var missing []string
	for _, kv := range []struct{ flag, val string }{
		{"mm-url", o.mmURL}, {"oauth-client-id", o.oauthClientID}, {"oauth-client-secret", o.oauthClientSecret}, {"bot-provisioning-token", o.botProvisioningToken}, {"public-url", o.publicURL}, {"callback-secret", o.callbackSecret},
	} {
		if kv.val == "" {
			missing = append(missing, "--"+kv.flag)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing: %s (or the matching environment variables)", strings.Join(missing, ", "))
	}
	if len(o.callbackSecret) < 32 {
		return errors.New("--callback-secret must be at least 32 characters")
	}
	o.publicURL = strings.TrimRight(o.publicURL, "/")
	return nil
}

func runBroker(ctx context.Context, o brokerOptions, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := sqlite.Open(o.dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	key := sha256.Sum256([]byte("mhb/oauth/" + o.callbackSecret))
	auth, err := oauth.New(oauth.Config{MMURL: o.mmURL, PublicURL: o.publicURL, ClientID: o.oauthClientID, ClientSecret: o.oauthClientSecret, EncryptionKey: key[:]}, st, log)
	if err != nil {
		return err
	}
	log.Info("broker starting", "version", version.Version, "setup", o.publicURL+"/oauth/start")

	h := hub.New(st, nil, log)
	h.MinHarnessVersion = o.minHarnessVersion
	c := core.New(core.Config{
		PublicURL:      o.publicURL,
		CallbackSecret: []byte(o.callbackSecret),
		QueueTTL:       o.queueTTL,
		GracePeriod:    o.gracePeriod,
		JobTimeout:     o.jobTimeout,
	}, st, nil, h, log)
	h.Handler = c
	issuer := mattermost.New(o.mmURL, o.botProvisioningToken)
	c.WithProvisioner(func(ctx context.Context, teamID string) (mattermost.Provisioner, error) {
		admin, err := auth.Client(ctx, teamID)
		if err != nil {
			return nil, err
		}
		return mattermost.WithBotTokenIssuer(admin, issuer), nil
	}, func(token string) mattermost.API { return mattermost.New(o.mmURL, token) })
	manager := bots.New(st, o.mmURL, c.HandleBotPost, log)

	mux := http.NewServeMux()
	auth.Register(mux)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/oauth/start", http.StatusSeeOther) })
	mux.Handle("POST /webhooks/mattermost/{botID}", outgoingHandler(st, func(token string) webhookAPI { return mattermost.New(o.mmURL, token) }, c.HandleBotPost))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.Handle("GET "+protocol.Path, h)
	// Device-flow onboarding: the laptop asks for a code, the owner claims it
	// with the slash command, the laptop polls for the result.
	mux.HandleFunc("POST /init", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			BotName     string `json:"bot_name"`
			HarnessName string `json:"harness_name"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		res, err := c.StartInit(r.Context(), req.BotName, req.HarnessName)
		switch {
		case errors.Is(err, core.ErrInitDisabled):
			http.Error(w, err.Error(), http.StatusNotImplemented)
			return
		case err != nil:
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, res)
	})
	mux.HandleFunc("GET /init/{code}", func(w http.ResponseWriter, r *http.Request) {
		res, err := c.PollInit(r.Context(), r.PathValue("code"), strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		switch {
		case errors.Is(err, core.ErrInitPending):
			w.WriteHeader(http.StatusAccepted)
			return
		case errors.Is(err, core.ErrInitUnknown):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case errors.Is(err, core.ErrInitConsumed):
			http.Error(w, err.Error(), http.StatusGone)
			return
		case err != nil:
			log.Error("poll init", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, res)
	})
	mux.HandleFunc("POST /commands/harness", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if !auth.VerifyCommand(r.Context(), r.PostForm.Get("team_id"), r.PostForm.Get("token")) {
			http.Error(w, "invalid command token", http.StatusUnauthorized)
			return
		}
		res := c.HandleCommand(r.Context(), core.CommandRequest{
			Token: r.PostForm.Get("token"), UserID: r.PostForm.Get("user_id"), UserName: r.PostForm.Get("user_name"),
			ChannelID: r.PostForm.Get("channel_id"), TeamID: r.PostForm.Get("team_id"), RootID: r.PostForm.Get("root_id"),
			Text: r.PostForm.Get("text"),
		})
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

	srv := &http.Server{Addr: o.listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 2)
	go func() {
		log.Info("listening", "addr", o.listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go manager.Run(ctx)
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
	stop()
	h.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
