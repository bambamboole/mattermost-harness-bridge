package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

const teamID = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
const userID = "bbbbbbbbbbbbbbbbbbbbbbbbbb"

type memoryStore struct {
	mu            sync.Mutex
	installations map[string]store.Installation
	saveErr       error
	onSave        func()
}

func (m *memoryStore) SaveInstallation(_ context.Context, i store.Installation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.onSave != nil {
		m.onSave()
	}
	if m.saveErr != nil {
		return m.saveErr
	}
	m.installations[i.TeamID] = i
	return nil
}
func (m *memoryStore) InstallationByTeam(_ context.Context, id string) (store.Installation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i, ok := m.installations[id]
	if !ok {
		return i, store.ErrNotFound
	}
	return i, nil
}
func (m *memoryStore) ListInstallations(_ context.Context) ([]store.Installation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := make([]store.Installation, 0, len(m.installations))
	for _, i := range m.installations {
		rows = append(rows, i)
	}
	return rows, nil
}

type fixture struct {
	s                                *Service
	mux                              *http.ServeMux
	db                               *memoryStore
	calls, created, refresh, deleted atomic.Int32
	rotateOnCode                     bool
	generation                       atomic.Int32
	missingToken                     bool
	admin                            bool
	command                          *model.Command
}

func setup(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{admin: true, db: &memoryStore{installations: map[string]store.Installation{}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/oauth/access_token":
			_ = r.ParseForm()
			if r.Form.Get("client_secret") != "secret" {
				t.Error("missing client secret")
			}
			if r.Form.Get("grant_type") == "refresh_token" {
				f.refresh.Add(1)
				if r.Form.Get("refresh_token") != f.token("refresh") {
					http.Error(w, "stale refresh token", http.StatusBadRequest)
					return
				}
				f.generation.Add(1)
			} else if f.rotateOnCode {
				f.generation.Add(1)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": f.token("access"), "refresh_token": f.token("refresh"), "token_type": "bearer", "expires_in": 3600})
		case r.URL.Path == "/api/v4/users/me":
			if r.Header.Get("Authorization") != model.HeaderBearer+" "+f.token("access") {
				http.Error(w, "stale access token", http.StatusUnauthorized)
				return
			}
			roles := "system_user"
			if f.admin {
				roles += " system_admin"
			}
			_ = json.NewEncoder(w).Encode(model.User{Id: userID, Roles: roles})
		case r.URL.Path == "/api/v4/teams/abcdefghijklmnopqrstuvwxyz":
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(model.AppError{Message: "not found", StatusCode: http.StatusNotFound})
		case r.URL.Path == "/api/v4/teams/name/my-team" || r.URL.Path == "/api/v4/teams/name/abcdefghijklmnopqrstuvwxyz":
			_ = json.NewEncoder(w).Encode(model.Team{Id: teamID, Name: "my-team"})
		case r.URL.Path == "/api/v4/teams/"+teamID:
			_ = json.NewEncoder(w).Encode(model.Team{Id: teamID})
		case r.URL.Path == "/api/v4/teams/"+teamID+"/members/"+userID:
			_ = json.NewEncoder(w).Encode(model.TeamMember{TeamId: teamID, UserId: userID})
		case r.URL.Path == "/api/v4/commands" && r.Method == "GET":
			commands := []*model.Command{}
			if f.command != nil {
				commands = append(commands, f.command)
			}
			_ = json.NewEncoder(w).Encode(commands)
		case r.URL.Path == "/api/v4/commands" && r.Method == "POST":
			f.created.Add(1)
			_ = json.NewDecoder(r.Body).Decode(&f.command)
			f.command.Id = "cccccccccccccccccccccccccc"
			f.command.Token = "command-secret"
			if f.missingToken {
				f.command.Token = ""
			}
			_ = json.NewEncoder(w).Encode(f.command)
		case strings.HasPrefix(r.URL.Path, "/api/v4/commands/") && r.Method == "DELETE":
			f.deleted.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "OK"})
		case strings.HasPrefix(r.URL.Path, "/api/v4/commands/"):
			_ = json.NewEncoder(w).Encode(f.command)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	var err error
	f.s, err = New(Config{MMURL: server.URL, PublicURL: "http://localhost:8080", ClientID: "client", ClientSecret: "secret", EncryptionKey: make([]byte, 32)}, f.db, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.mux = http.NewServeMux()
	f.s.Register(f.mux)
	return f
}
func (f *fixture) start(t *testing.T) (string, *http.Cookie) { return f.startTeam(t, teamID) }
func (f *fixture) startTeam(t *testing.T, team string) (string, *http.Cookie) {
	t.Helper()
	w := httptest.NewRecorder()
	f.mux.ServeHTTP(w, httptest.NewRequest("GET", "/oauth/start?team_id="+url.QueryEscape(team), nil))
	if w.Code != http.StatusFound {
		t.Fatalf("start: %d %s", w.Code, w.Body)
	}
	u, err := url.Parse(w.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("redirect_uri") != "http://localhost:8080/oauth/callback" {
		t.Fatal("wrong callback")
	}
	return u.Query().Get("state"), w.Result().Cookies()[0]
}
func (f *fixture) callback(state string, cookie *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/oauth/callback?code=code&state="+url.QueryEscape(state), nil)
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	f.mux.ServeHTTP(w, r)
	return w
}
func TestCallbackState(t *testing.T) {
	for _, kind := range []string{"missing_cookie", "wrong_cookie", "wrong_state", "expired"} {
		t.Run(kind, func(t *testing.T) {
			f := setup(t)
			state, cookie := f.start(t)
			switch kind {
			case "missing_cookie":
				cookie = nil
			case "wrong_cookie":
				cookie.Value = "another-browser"
			case "wrong_state":
				state = "bad"
			case "expired":
				f.s.now = func() time.Time { return time.Now().Add(11 * time.Minute) }
			}
			if w := f.callback(state, cookie); w.Code != 400 {
				t.Fatalf("got %d", w.Code)
			}
			if f.calls.Load() != 0 {
				t.Fatal("invalid state reached Mattermost")
			}
		})
	}
}
func TestInstallAdminAndReuse(t *testing.T) {
	f := setup(t)
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 200 {
		t.Fatalf("install: %d %s", w.Code, w.Body)
	}
	i, _ := f.db.InstallationByTeam(context.Background(), teamID)
	if strings.Contains(i.Credentials, "secret") {
		t.Fatal("unencrypted credentials")
	}
	if !f.s.VerifyCommand(context.Background(), teamID, "command-secret") || f.s.VerifyCommand(context.Background(), teamID, "bad") {
		t.Fatal("command verification")
	}
	if w := f.callback(state, cookie); w.Code != 400 {
		t.Fatal("state replay accepted")
	}
	state, cookie = f.start(t)
	if w := f.callback(state, cookie); w.Code != 200 {
		t.Fatalf("reinstall: %d %s", w.Code, w.Body)
	}
	if f.created.Load() != 1 {
		t.Fatal("duplicate command")
	}
}
func TestNonAdminCannotInstall(t *testing.T) {
	f := setup(t)
	f.admin = false
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 403 {
		t.Fatalf("got %d", w.Code)
	}
	if f.created.Load() != 0 {
		t.Fatal("non admin created command")
	}
}
func TestCommandCollision(t *testing.T) {
	f := setup(t)
	f.command = &model.Command{Id: "unrelated", TeamId: teamID, Trigger: "harness", URL: "https://elsewhere.example/command"}
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 409 {
		t.Fatalf("got %d: %s", w.Code, w.Body)
	}
	if f.created.Load() != 0 {
		t.Fatal("collision overwritten")
	}
}
func TestRefreshSerializedAndDurable(t *testing.T) {
	f := setup(t)
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 200 {
		t.Fatal(w.Body)
	}
	f.s.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if _, err := f.s.Client(context.Background(), teamID); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if f.refresh.Load() != 1 {
		t.Fatalf("refresh calls %d", f.refresh.Load())
	}
	restarted, err := New(f.s.cfg, f.db, nil)
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = f.s.now
	if _, err = restarted.Client(context.Background(), teamID); err != nil {
		t.Fatal(err)
	}
	if f.refresh.Load() != 1 {
		t.Fatal("refreshed token not persisted")
	}
}

func TestEncryptedGrantRejectsTamperingAndTeamSubstitution(t *testing.T) {
	f := setup(t)
	encrypted, err := f.s.encrypt(teamID, credentials{AccessToken: "secret", ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.decrypt(userID, encrypted); err == nil {
		t.Fatal("grant could be moved between teams")
	}
	replacement := "A"
	if encrypted[0] == 'A' {
		replacement = "B"
	}
	if _, err = f.s.decrypt(teamID, replacement+encrypted[1:]); err == nil {
		t.Fatal("tampered ciphertext accepted")
	}
}

func TestOwnedCommandURLChangeIsCollision(t *testing.T) {
	f := setup(t)
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 200 {
		t.Fatal(w.Body)
	}
	f.command.URL = "https://unrelated.example/command"
	state, cookie = f.start(t)
	if w := f.callback(state, cookie); w.Code != 409 {
		t.Fatalf("got %d", w.Code)
	}
	if f.created.Load() != 1 {
		t.Fatal("changed command overwritten")
	}
}

func TestUnrelatedCommandWithSameURLIsCollision(t *testing.T) {
	f := setup(t)
	f.command = &model.Command{Id: "unrelated", TeamId: teamID, Trigger: "harness", Method: model.CommandMethodPost, URL: f.s.cfg.PublicURL + "/commands/harness"}
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 409 {
		t.Fatalf("got %d", w.Code)
	}
}

func TestTokenEndpointDoesNotFollowRedirect(t *testing.T) {
	f := setup(t)
	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	f.s.cfg.MMURL = origin.URL
	if _, err := f.s.exchange(context.Background(), url.Values{"grant_type": {"authorization_code"}, "code": {"secret-code"}}); err == nil {
		t.Fatal("redirect accepted")
	}
	if leaked.Load() {
		t.Fatal("secret posted to redirect target")
	}
}

func TestInstallByTeamName(t *testing.T) {
	for _, name := range []string{"my-team", "abcdefghijklmnopqrstuvwxyz"} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			state, cookie := f.startTeam(t, name)
			if w := f.callback(state, cookie); w.Code != 200 {
				t.Fatalf("install: %d %s", w.Code, w.Body)
			}
			i, err := f.db.InstallationByTeam(context.Background(), teamID)
			if err != nil || i.TeamID != teamID || f.command.TeamId != teamID {
				t.Fatalf("team name was not resolved: %+v %v", i, err)
			}
			if f.command.AutoCompleteHint != "init [code] [bot-name] | bot create <name> <harness-id> | join <bot-name> | status" {
				t.Fatalf("incorrect hint %q", f.command.AutoCompleteHint)
			}
		})
	}
}

func TestInstallRollback(t *testing.T) {
	for _, reason := range []string{"missing_token", "save_failure", "cancelled_context"} {
		t.Run(reason, func(t *testing.T) {
			f := setup(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch reason {
			case "missing_token":
				f.missingToken = true
			case "save_failure":
				f.db.saveErr = errors.New("save failed")
			case "cancelled_context":
				f.db.saveErr = context.Canceled
				f.db.onSave = cancel
			}
			err := f.s.install(ctx, f.s.api("access-secret"), teamID, userID, credentials{AccessToken: "secret"})
			if err == nil {
				t.Fatal("expected failure")
			}
			if f.deleted.Load() != 1 {
				t.Fatalf("created command was not cleaned up: %d deletes", f.deleted.Load())
			}
		})
	}
}

func (f *fixture) token(kind string) string {
	if generation := f.generation.Load(); generation > 0 {
		return fmt.Sprintf("%s-secret-%d", kind, generation)
	}
	return kind + "-secret"
}

func (f *fixture) copyInstallation(t *testing.T, team, user string) {
	t.Helper()
	i, err := f.db.InstallationByTeam(context.Background(), teamID)
	if err != nil {
		t.Fatal(err)
	}
	cred, err := f.s.decrypt(teamID, i.Credentials)
	if err != nil {
		t.Fatal(err)
	}
	i.TeamID = team
	i.UserID = user
	i.CommandID = "other-command"
	i.CommandToken = "other-token"
	i.Credentials, err = f.s.encrypt(team, cred)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.db.SaveInstallation(context.Background(), i); err != nil {
		t.Fatal(err)
	}
}

func TestRefreshSharedAcrossInstallerTeams(t *testing.T) {
	f := setup(t)
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 200 {
		t.Fatal(w.Body)
	}
	const second = "dddddddddddddddddddddddddd"
	const unrelated = "eeeeeeeeeeeeeeeeeeeeeeeeee"
	f.copyInstallation(t, second, userID)
	f.copyInstallation(t, unrelated, "another-admin")
	before, _ := f.db.InstallationByTeam(context.Background(), unrelated)
	f.s.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	var wg sync.WaitGroup
	for _, team := range []string{teamID, second, teamID, second} {
		wg.Go(func() {
			client, err := f.s.Client(context.Background(), team)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err = client.Me(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if f.refresh.Load() != 1 {
		t.Fatalf("shared grant refreshed %d times", f.refresh.Load())
	}
	after, _ := f.db.InstallationByTeam(context.Background(), unrelated)
	if before.Credentials != after.Credentials {
		t.Fatal("another installer's grant changed")
	}
	secondRow, _ := f.db.InstallationByTeam(context.Background(), second)
	if secondRow.CommandID != "other-command" || secondRow.CommandToken != "other-token" {
		t.Fatal("team command overwritten")
	}
	restarted, err := New(f.s.cfg, f.db, nil)
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = f.s.now
	client, err := restarted.Client(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Me(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReauthorizationUpdatesSharedGrantEvenWhenInstallFails(t *testing.T) {
	f := setup(t)
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 200 {
		t.Fatal(w.Body)
	}
	const second = "dddddddddddddddddddddddddd"
	f.copyInstallation(t, second, userID)
	f.rotateOnCode = true
	f.command.URL = "https://unrelated.example/command"
	state, cookie = f.start(t)
	if w := f.callback(state, cookie); w.Code != 409 {
		t.Fatalf("expected collision, got %d", w.Code)
	}
	client, err := f.s.Client(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Me(context.Background()); err != nil {
		t.Fatalf("existing installation left with stale grant: %v", err)
	}
}

func TestClientUsesNewestDurableSharedGrant(t *testing.T) {
	f := setup(t)
	state, cookie := f.start(t)
	if w := f.callback(state, cookie); w.Code != 200 {
		t.Fatal(w.Body)
	}
	const second = "dddddddddddddddddddddddddd"
	f.copyInstallation(t, second, userID)
	stale, _ := f.db.InstallationByTeam(context.Background(), second)
	f.s.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if _, err := f.s.Client(context.Background(), teamID); err != nil {
		t.Fatal(err)
	}
	// Simulate one row left behind by a partial persistence failure.
	if err := f.db.SaveInstallation(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(f.s.cfg, f.db, nil)
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = f.s.now
	client, err := restarted.Client(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Me(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.refresh.Load() != 1 {
		t.Fatal("attempted to refresh an obsolete shared token")
	}
}
