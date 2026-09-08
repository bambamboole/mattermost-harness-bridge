package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// InitStart mirrors the broker's response to POST /init.
type InitStart struct {
	Code      string    `json:"code"`
	PollToken string    `json:"poll_token"`
	ExpiresAt time.Time `json:"expires_at"`
	Command   string    `json:"command"`
}

// InitResult mirrors GET /init/{code} once the owner claimed the code.
type InitResult struct {
	HarnessID   string `json:"harness_id"`
	Token       string `json:"token"`
	MMUserID    string `json:"mm_user_id"`
	Username    string `json:"username"`
	BotUsername string `json:"bot_username"`
}

var (
	ErrInitExpired  = errors.New("the init code expired before it was claimed")
	ErrInitDisabled = errors.New("the broker has onboarding disabled; ask the admin to complete the OAuth installation")
)

// StartInit asks the broker for a device-flow code.
func StartInit(ctx context.Context, brokerURL, botName, harnessName string) (InitStart, error) {
	body, _ := json.Marshal(map[string]string{"bot_name": botName, "harness_name": harnessName})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(brokerURL, "/")+"/init", bytes.NewReader(body))
	if err != nil {
		return InitStart{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return InitStart{}, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusNotImplemented:
		return InitStart{}, ErrInitDisabled
	default:
		return InitStart{}, fmt.Errorf("init failed: %s: %s", res.Status, strings.TrimSpace(string(raw)))
	}
	var start InitStart
	if err := json.Unmarshal(raw, &start); err != nil {
		return InitStart{}, fmt.Errorf("init: bad response: %w", err)
	}
	return start, nil
}

// WaitInit polls until the owner claimed the code, the code expired, or ctx
// ended. onTick is called between polls for a progress indicator.
func WaitInit(ctx context.Context, brokerURL, code, pollToken string, expiresAt time.Time, onTick func()) (InitResult, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	url := strings.TrimRight(brokerURL, "/") + "/init/" + code
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return InitResult{}, err
		}
		req.Header.Set("Authorization", "Bearer "+pollToken)
		res, err := client.Do(req)
		if err != nil {
			return InitResult{}, err
		}
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
		_ = res.Body.Close()
		switch res.StatusCode {
		case http.StatusOK:
			var out InitResult
			if err := json.Unmarshal(raw, &out); err != nil {
				return InitResult{}, fmt.Errorf("init: bad response: %w", err)
			}
			return out, nil
		case http.StatusAccepted:
		case http.StatusNotFound, http.StatusGone:
			return InitResult{}, ErrInitExpired
		default:
			return InitResult{}, fmt.Errorf("init poll failed: %s: %s", res.Status, strings.TrimSpace(string(raw)))
		}
		if time.Now().After(expiresAt) {
			return InitResult{}, ErrInitExpired
		}
		if onTick != nil {
			onTick()
		}
		select {
		case <-ctx.Done():
			return InitResult{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// ApplyInit writes the pairing into the config, keeping workspaces and
// tool lists of an existing one.
func ApplyInit(brokerURL string, res InitResult, harnessName string) (Config, error) {
	cfg := DefaultConfig()
	if b, err := readConfigFile(); err == nil {
		_ = json.Unmarshal(b, &cfg)
	}
	cfg.BrokerURL = strings.TrimRight(brokerURL, "/")
	cfg.HarnessID = res.HarnessID
	cfg.Token = res.Token
	cfg.OwnerMMUserID = res.MMUserID
	cfg.Name = harnessName
	return cfg, SaveConfig(cfg)
}
