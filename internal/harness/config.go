package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
)

// Config is what `harness pair` writes and `harness run` reads.
type Config struct {
	BrokerURL     string `json:"broker_url"` // https://broker.example.com
	HarnessID     string `json:"harness_id"`
	Token         string `json:"token"`
	OwnerMMUserID string `json:"owner_mm_user_id"`
	Name          string `json:"name"`

	// Workspaces maps a short name to an absolute directory. Jobs that name
	// none and continue no thread run in DefaultWorkspace, created on start.
	Workspaces       map[string]string `json:"workspaces"`
	DefaultWorkspace string            `json:"default_workspace"`

	// Agent is the coding agent jobs run on unless the message says
	// otherwise: "claude" or "codex".
	Agent        string `json:"agent"`
	CodexBin     string `json:"codex_bin"`
	CodexSandbox string `json:"codex_sandbox"`

	// AllowedTools run without asking; everything else goes through the
	// approval flow. DisallowedTools never run.
	AllowedTools    []string `json:"allowed_tools"`
	DisallowedTools []string `json:"disallowed_tools"`

	MaxJobs            int    `json:"max_jobs"`
	DefaultMaxTurns    int    `json:"default_max_turns"`
	ApprovalTimeoutMin int    `json:"approval_timeout_min"`
	ClaudeBin          string `json:"claude_bin"`
	Model              string `json:"model,omitempty"`
}

func DefaultConfig() Config {
	return Config{
		Workspaces:         map[string]string{},
		DefaultWorkspace:   filepath.Join(homeDir(), ".harness"),
		Agent:              "claude",
		CodexBin:           "codex",
		CodexSandbox:       "workspace-write",
		AllowedTools:       []string{"Read", "Grep", "Glob", "LS", "WebSearch", "WebFetch"},
		DisallowedTools:    []string{},
		MaxJobs:            1,
		DefaultMaxTurns:    40,
		ApprovalTimeoutMin: 30,
		ClaudeBin:          "claude",
	}
}

func ConfigDir() string {
	if d := os.Getenv("MM_HARNESS_CONFIG_DIR"); d != "" {
		return d
	}
	base, err := os.UserConfigDir()
	if err != nil {
		base = filepath.Join(os.Getenv("HOME"), ".config")
	}
	return filepath.Join(base, "mm-harness")
}

func ConfigPath() string { return filepath.Join(ConfigDir(), "config.json") }

// StateDir holds sessions, the outbox, the permission socket and per-job files.
func StateDir() string {
	if d := os.Getenv("MM_HARNESS_STATE_DIR"); d != "" {
		return d
	}
	return filepath.Join(ConfigDir(), "state")
}

func LoadConfig() (Config, error) {
	cfg := DefaultConfig()
	b, err := os.ReadFile(ConfigPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, fmt.Errorf("no config at %s; run `mhb harness pair` first", ConfigPath())
		}
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", ConfigPath(), err)
	}
	return cfg, cfg.Validate()
}

func SaveConfig(cfg Config) error {
	if err := os.MkdirAll(ConfigDir(), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := ConfigPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, ConfigPath())
}

// RedactConfig replaces the harness token in raw config JSON so the config
// can be pasted into a chat or a bug report. Unknown keys are preserved.
func RedactConfig(raw []byte) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", ConfigPath(), err)
	}
	if _, ok := m["token"]; ok {
		m["token"] = json.RawMessage(`"redacted"`)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func (c Config) Validate() error {
	if c.BrokerURL == "" || c.Token == "" || c.OwnerMMUserID == "" || c.HarnessID == "" {
		return errors.New("config incomplete: broker_url, harness_id, token and owner_mm_user_id are required (run `mhb harness pair`)")
	}
	for name, dir := range c.Workspaces {
		if !filepath.IsAbs(dir) {
			return fmt.Errorf("workspace %q: path must be absolute", name)
		}
		if st, err := os.Stat(dir); err != nil || !st.IsDir() {
			return fmt.Errorf("workspace %q: %s is not a directory", name, dir)
		}
	}
	if c.MaxJobs <= 0 {
		return errors.New("max_jobs must be >= 1")
	}
	if c.DefaultWorkspace == "" || !filepath.IsAbs(c.DefaultWorkspace) {
		return errors.New("default_workspace must be an absolute path")
	}
	switch c.Agent {
	case "claude", "codex":
	default:
		return fmt.Errorf("agent must be \"claude\" or \"codex\", got %q", c.Agent)
	}
	return nil
}

func homeDir() string {
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return os.Getenv("HOME")
}

// WSURL derives the WebSocket endpoint from BrokerURL.
func (c Config) WSURL() string {
	u := strings.TrimRight(c.BrokerURL, "/")
	switch {
	case strings.HasPrefix(u, "https://"):
		u = "wss://" + strings.TrimPrefix(u, "https://")
	case strings.HasPrefix(u, "http://"):
		u = "ws://" + strings.TrimPrefix(u, "http://")
	}
	return u + protocol.Path
}

func (c Config) WorkspaceNames() []string {
	names := make([]string, 0, len(c.Workspaces))
	for n := range c.Workspaces {
		names = append(names, n)
	}
	return names
}
