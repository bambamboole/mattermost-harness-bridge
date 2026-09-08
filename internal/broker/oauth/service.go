// Package oauth installs and maintains a team's confidential OAuth grant.
package oauth

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

const stateTTL = 10 * time.Minute
const cookieName = "harness_oauth_state"

// Config uses the OAuth application's confidential client credentials. EncryptionKey
// must contain 32 stable, secret bytes; changing it requires reinstalling teams.
type Config struct {
	MMURL, PublicURL, ClientID, ClientSecret string
	EncryptionKey                            []byte
}
type pendingState struct {
	teamRef string
	browser [32]byte
	expires time.Time
}
type credentials struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}
type Service struct {
	cfg      Config
	store    store.InstallationStore
	aead     cipher.AEAD
	http     *http.Client
	now      func() time.Time
	statesMu sync.Mutex
	states   map[string]pendingState
	// Serialize refreshes and installations so rotating refresh tokens and command
	// ownership cannot race within the broker's single process.
	mu sync.Mutex
}

func New(cfg Config, db store.InstallationStore, _ *slog.Logger) (*Service, error) {
	for _, raw := range []string{cfg.MMURL, cfg.PublicURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("oauth: URLs must be absolute HTTP(S) URLs without credentials, query, or fragment")
		}
	}
	if cfg.ClientID == "" || cfg.ClientSecret == "" || len(cfg.EncryptionKey) != 32 || db == nil {
		return nil, errors.New("oauth: client credentials, installation store, and 32-byte encryption key are required")
	}
	cfg.MMURL = strings.TrimRight(cfg.MMURL, "/")
	cfg.PublicURL = strings.TrimRight(cfg.PublicURL, "/")
	block, err := aes.NewCipher(cfg.EncryptionKey)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, store: db, aead: aead, now: time.Now, states: map[string]pendingState{}, http: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /oauth/start", s.start)
	mux.HandleFunc("GET /oauth/callback", s.callback)
}
func secureHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; form-action 'self'; frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
}
func (s *Service) stateCookie(value string, age int) *http.Cookie {
	return &http.Cookie{Name: cookieName, Value: value, Path: "/oauth", HttpOnly: true, Secure: strings.HasPrefix(s.cfg.PublicURL, "https://"), SameSite: http.SameSiteLaxMode, MaxAge: age}
}
func (s *Service) start(w http.ResponseWriter, r *http.Request) {
	secureHeaders(w)
	teamRef := strings.TrimSpace(r.URL.Query().Get("team_id"))
	if teamRef == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, `<!doctype html><html lang="en"><meta charset="utf-8"><title>Install Harness</title><h1>Install Harness</h1><p>A Mattermost system administrator who belongs to this team must authorize installation.</p><form action="/oauth/start" method="get"><label>Mattermost team name or ID <input name="team_id" required minlength="2" maxlength="64" aria-describedby="team-help"><span id="team-help">Use the team name from your Mattermost URL: /my-team/channels/…</span></label><button type="submit">Authorize in Mattermost</button></form></html>`)
		return
	}
	if !model.IsValidId(teamRef) && (len(teamRef) > model.TeamNameMaxLength || !model.IsValidTeamName(teamRef)) {
		http.Error(w, "Enter a Mattermost team name or a 26-character team ID.", http.StatusBadRequest)
		return
	}
	state, err := randomValue()
	if err != nil {
		http.Error(w, "Cannot start installation.", http.StatusInternalServerError)
		return
	}
	browser, err := randomValue()
	if err != nil {
		http.Error(w, "Cannot start installation.", http.StatusInternalServerError)
		return
	}
	s.statesMu.Lock()
	for key, p := range s.states {
		if !s.now().Before(p.expires) {
			delete(s.states, key)
		}
	}
	if len(s.states) >= 1000 {
		s.statesMu.Unlock()
		http.Error(w, "Too many pending installations. Try again later.", http.StatusServiceUnavailable)
		return
	}
	s.states[state] = pendingState{teamRef: teamRef, browser: sha256.Sum256([]byte(browser)), expires: s.now().Add(stateTTL)}
	s.statesMu.Unlock()
	http.SetCookie(w, s.stateCookie(browser, int(stateTTL.Seconds())))
	q := url.Values{"response_type": {"code"}, "client_id": {s.cfg.ClientID}, "redirect_uri": {s.cfg.PublicURL + "/oauth/callback"}, "state": {state}}
	http.Redirect(w, r, s.cfg.MMURL+"/oauth/authorize?"+q.Encode(), http.StatusFound)
}
func randomValue() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func (s *Service) callback(w http.ResponseWriter, r *http.Request) {
	secureHeaders(w)
	state := r.URL.Query().Get("state")
	cookie, err := r.Cookie(cookieName)
	if err != nil || state == "" {
		http.Error(w, "Invalid or expired installation session. Start again.", http.StatusBadRequest)
		return
	}
	binding := sha256.Sum256([]byte(cookie.Value))
	s.statesMu.Lock()
	pending, ok := s.states[state]
	valid := ok && s.now().Before(pending.expires) && subtle.ConstantTimeCompare(binding[:], pending.browser[:]) == 1
	if valid {
		delete(s.states, state)
	}
	s.statesMu.Unlock()
	if !valid {
		http.Error(w, "Invalid or expired installation session. Start again.", http.StatusBadRequest)
		return
	}
	http.SetCookie(w, s.stateCookie("", -1))
	if r.URL.Query().Get("error") != "" || r.URL.Query().Get("code") == "" {
		http.Error(w, "Mattermost authorization was declined or incomplete. Start again.", http.StatusBadRequest)
		return
	}
	// Mattermost has one grant per OAuth application and authorizing user.
	// Code exchange can rotate that grant too, so serialize it with refresh.
	s.mu.Lock()
	defer s.mu.Unlock()
	cred, err := s.exchange(r.Context(), url.Values{"grant_type": {"authorization_code"}, "code": {r.URL.Query().Get("code")}, "redirect_uri": {s.cfg.PublicURL + "/oauth/callback"}})
	if err != nil {
		http.Error(w, "Mattermost authorization failed. Start again.", http.StatusBadGateway)
		return
	}
	api := s.api(cred.AccessToken)
	user, _, err := api.GetMe(r.Context(), "")
	if err != nil || user == nil {
		http.Error(w, "Cannot verify the authorizing user.", http.StatusBadGateway)
		return
	}
	if !user.IsSystemAdmin() || user.DeleteAt != 0 {
		http.Error(w, "Installation requires a Mattermost system administrator.", http.StatusForbidden)
		return
	}
	// Preserve a newly rotated grant for this administrator's existing teams
	// even when the requested team's membership or command checks fail.
	if err := s.saveCredentials(r.Context(), user.Id, cred); err != nil {
		http.Error(w, "Cannot save Mattermost authorization. Try again.", http.StatusBadGateway)
		return
	}
	var team *model.Team
	if model.IsValidId(pending.teamRef) {
		var response *model.Response
		team, response, err = api.GetTeam(r.Context(), pending.teamRef, "")
		// Team URL names can themselves contain exactly 26 ID characters.
		if err != nil && response != nil && response.StatusCode == http.StatusNotFound {
			team, _, err = api.GetTeamByName(r.Context(), pending.teamRef, "")
		}
	} else {
		team, _, err = api.GetTeamByName(r.Context(), pending.teamRef, "")
	}
	if err != nil || team == nil || !model.IsValidId(team.Id) || team.DeleteAt != 0 {
		http.Error(w, "The selected team is unavailable.", http.StatusForbidden)
		return
	}
	member, _, err := api.GetTeamMember(r.Context(), team.Id, user.Id, "")
	if err != nil || member == nil || member.DeleteAt != 0 {
		http.Error(w, "Join the selected team before installing Harness.", http.StatusForbidden)
		return
	}
	err = s.install(r.Context(), api, team.Id, user.Id, cred)
	if errors.Is(err, errCollision) {
		http.Error(w, "This team already has an unrelated /harness command. Remove or rename it in Mattermost before installing.", http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, "Harness installation failed. Check Mattermost integration settings and try again.", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, `<!doctype html><html lang="en"><meta charset="utf-8"><title>Harness installed</title><h1>Harness installed</h1><p>Return to Mattermost and run <code>/harness init</code> to connect your laptop.</p></html>`)
}

var errCollision = errors.New("oauth: unrelated harness command exists")

func (s *Service) install(ctx context.Context, api *model.Client4, teamID, userID string, cred credentials) (err error) {
	existing, err := s.store.InstallationByTeam(ctx, teamID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return errors.New("oauth: installation lookup failed")
	}
	commands, _, err := api.ListCommands(ctx, teamID, true)
	if err != nil {
		return errors.New("oauth: list commands failed")
	}
	var command *model.Command
	for _, c := range commands {
		if c.Trigger != "harness" || c.DeleteAt != 0 {
			continue
		}
		if c.Id != existing.CommandID || existing.CommandID == "" {
			return errCollision
		}
		full, _, e := api.GetCommandById(ctx, c.Id)
		if e != nil || full == nil {
			return errors.New("oauth: get command failed")
		}
		if full.TeamId != teamID || full.Trigger != "harness" || full.URL != s.cfg.PublicURL+"/commands/harness" || full.Method != model.CommandMethodPost {
			return errCollision
		}
		command = full
	}
	created := false
	defer func() {
		if err != nil && created && command != nil && command.Id != "" {
			// A failed browser request can cancel its context after remote creation.
			// Cleanup still needs a short independent window to remove the orphan.
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_, _ = api.DeleteCommand(cleanupCtx, command.Id)
		}
	}()
	if command == nil {
		command, _, err = api.CreateCommand(ctx, &model.Command{TeamId: teamID, Trigger: "harness", Method: model.CommandMethodPost, URL: s.cfg.PublicURL + "/commands/harness", DisplayName: "Harness", Description: "Connect and manage local coding harnesses", AutoComplete: true, AutoCompleteDesc: "Connect and manage local coding harnesses", AutoCompleteHint: "init [code] [bot-name] | bot create <name> <harness-id> | join <bot-name> | status"})
		if err != nil || command == nil {
			return errors.New("oauth: create command failed")
		}
		created = true
	}
	if command.Id == "" || command.Token == "" {
		return errors.New("oauth: command credentials missing")
	}
	encrypted, err := s.encrypt(teamID, cred)
	if err != nil {
		return err
	}
	err = s.store.SaveInstallation(ctx, store.Installation{TeamID: teamID, UserID: userID, Credentials: encrypted, CommandID: command.Id, CommandToken: command.Token})
	if err != nil {
		return errors.New("oauth: save installation failed")
	}
	return nil
}
func (s *Service) api(token string) *model.Client4 {
	api := model.NewAPIv4Client(s.cfg.MMURL)
	api.SetToken(token)
	api.HTTPClient = s.http
	return api
}
func (s *Service) exchange(ctx context.Context, form url.Values) (credentials, error) {
	form.Set("client_id", s.cfg.ClientID)
	form.Set("client_secret", s.cfg.ClientSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.MMURL+"/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return credentials{}, errors.New("oauth: invalid token endpoint")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := s.http.Do(req)
	if err != nil {
		return credentials{}, errors.New("oauth: token request failed")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != 200 {
		return credentials{}, errors.New("oauth: token request rejected")
	}
	var token struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		TokenType    string `json:"token_type"`
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&token); err != nil || token.AccessToken == "" || token.ExpiresIn <= 0 || (token.TokenType != "" && !strings.EqualFold(token.TokenType, "bearer")) {
		return credentials{}, errors.New("oauth: invalid token response")
	}
	return credentials{AccessToken: token.AccessToken, RefreshToken: token.RefreshToken, ExpiresAt: s.now().Add(time.Duration(token.ExpiresIn) * time.Second), UpdatedAt: s.now()}, nil
}

// Client obtains current credentials, refreshing and persisting rotation before
// returning a client. Call for each operation rather than caching the client.
func (s *Service) Client(ctx context.Context, teamID string) (*mattermost.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	installation, err := s.store.InstallationByTeam(ctx, teamID)
	if err != nil {
		return nil, err
	}
	cred, err := s.decrypt(teamID, installation.Credentials)
	if err != nil {
		return nil, err
	}
	installations, err := s.store.ListInstallations(ctx)
	if err != nil {
		return nil, errors.New("oauth: list installations failed")
	}
	// A prior partial write can leave one team's copy behind. Always choose
	// the newest durable grant for this installer before attempting refresh.
	for _, other := range installations {
		if other.UserID != installation.UserID || other.TeamID == teamID {
			continue
		}
		candidate, err := s.decrypt(other.TeamID, other.Credentials)
		if err != nil {
			continue
		}
		if candidate.UpdatedAt.After(cred.UpdatedAt) || (candidate.UpdatedAt.Equal(cred.UpdatedAt) && candidate.ExpiresAt.After(cred.ExpiresAt)) {
			cred = candidate
		}
	}
	if !s.now().Add(time.Minute).Before(cred.ExpiresAt) {
		if cred.RefreshToken == "" {
			return nil, errors.New("oauth: grant expired; reinstall this team")
		}
		refreshed, err := s.exchange(ctx, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {cred.RefreshToken}})
		if err != nil {
			return nil, err
		}
		if refreshed.RefreshToken == "" {
			refreshed.RefreshToken = cred.RefreshToken
		}
		if err = s.saveCredentials(ctx, installation.UserID, refreshed); err != nil {
			return nil, err
		}
		cred = refreshed
	}
	return mattermost.New(s.cfg.MMURL, cred.AccessToken), nil
}

// saveCredentials updates only grants; each team's command ownership remains
// independent. The caller holds mu. Encrypt separately because team ID is AAD.
func (s *Service) saveCredentials(ctx context.Context, userID string, cred credentials) error {
	installations, err := s.store.ListInstallations(ctx)
	if err != nil {
		return errors.New("oauth: list installations failed")
	}
	var saveErr error
	for _, installation := range installations {
		if installation.UserID != userID {
			continue
		}
		installation.Credentials, err = s.encrypt(installation.TeamID, cred)
		if err != nil {
			return err
		}
		if err = s.store.SaveInstallation(ctx, installation); err != nil {
			saveErr = errors.New("oauth: persist shared grant failed")
		}
	}
	return saveErr
}

func (s *Service) VerifyCommand(ctx context.Context, teamID, token string) bool {
	if token == "" {
		return false
	}
	i, err := s.store.InstallationByTeam(ctx, teamID)
	return err == nil && i.CommandID != "" && i.CommandToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(i.CommandToken)) == 1
}
func (s *Service) encrypt(teamID string, c credentials) (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := s.aead.Seal(nonce, nonce, data, []byte(teamID))
	return base64.RawStdEncoding.EncodeToString(sealed), nil
}
func (s *Service) decrypt(teamID, encrypted string) (credentials, error) {
	data, err := base64.RawStdEncoding.DecodeString(encrypted)
	if err != nil || len(data) < s.aead.NonceSize() {
		return credentials{}, errors.New("oauth: invalid encrypted grant")
	}
	plaintext, err := s.aead.Open(nil, data[:s.aead.NonceSize()], data[s.aead.NonceSize():], []byte(teamID))
	if err != nil {
		return credentials{}, errors.New("oauth: cannot decrypt grant")
	}
	var cred credentials
	if err = json.Unmarshal(plaintext, &cred); err != nil || cred.AccessToken == "" {
		return credentials{}, errors.New("oauth: invalid stored grant")
	}
	return cred, nil
}
