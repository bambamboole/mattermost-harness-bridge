package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/version"
)

// PairRequest is what the harness posts to the broker's /pair endpoint.
type PairRequest struct {
	Code     string `json:"code"`
	Name     string `json:"name"`
	Hostname string `json:"hostname"`
	Version  string `json:"version"`
}

type PairResponse struct {
	HarnessID string `json:"harness_id"`
	Token     string `json:"token"`
	MMUserID  string `json:"mm_user_id"`
	Username  string `json:"username"`
}

// Pair exchanges a one-time code (handed out by the bot in Mattermost) for a
// harness token and writes the config.
func Pair(ctx context.Context, brokerURL, code, name string) (Config, error) {
	brokerURL = strings.TrimRight(brokerURL, "/")
	host, _ := os.Hostname()
	if name == "" {
		name = host
	}
	body, _ := json.Marshal(PairRequest{Code: strings.TrimSpace(code), Name: name, Hostname: host, Version: version.Version})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, brokerURL+"/pair", bytes.NewReader(body))
	if err != nil {
		return Config{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return Config{}, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	if res.StatusCode != http.StatusOK {
		return Config{}, fmt.Errorf("pairing failed: %s: %s", res.Status, strings.TrimSpace(string(raw)))
	}
	var pr PairResponse
	if err := json.Unmarshal(raw, &pr); err != nil {
		return Config{}, fmt.Errorf("pairing: bad response: %w", err)
	}
	cfg := DefaultConfig()
	if existing, err := os.ReadFile(ConfigPath()); err == nil {
		_ = json.Unmarshal(existing, &cfg) // keep workspaces and tool lists
	}
	cfg.BrokerURL = brokerURL
	cfg.HarnessID = pr.HarnessID
	cfg.Token = pr.Token
	cfg.OwnerMMUserID = pr.MMUserID
	cfg.Name = name
	return cfg, SaveConfig(cfg)
}

func decodeB64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
