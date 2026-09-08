package core

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

// Onboarding is a device flow: `mhb harness init` asks the broker for a
// code and polls; the owner types `/harness init <code>` in Mattermost,
// which is authenticated by the slash command's token and carries the
// user's id. The broker then creates the owner's bot with the admin token,
// issues the harness token, and the poll hands it to the laptop once.

const (
	InitCodeTTL      = 10 * time.Minute
	InitCodeLength   = 8
	botNamePrefix    = "harness-"
	maxBotNameLength = 22
)

var (
	ErrInitDisabled  = errors.New("onboarding is disabled: the broker has no admin token")
	ErrInitUnknown   = errors.New("unknown or expired init code")
	ErrInitPending   = errors.New("init code not claimed yet")
	ErrInitConsumed  = errors.New("init result was already fetched")
	botUsernameRegex = regexp.MustCompile(`^[a-z][a-z0-9._-]{2,21}$`)
)

// InitStart is what the laptop receives when it asks for a code.
type InitStart struct {
	Code      string    `json:"code"`
	ExpiresAt time.Time `json:"expires_at"`
	Command   string    `json:"command"` // what to type in Mattermost
}

// InitResult is what the laptop receives once the owner claimed the code.
type InitResult struct {
	HarnessID   string `json:"harness_id"`
	Token       string `json:"token"`
	MMUserID    string `json:"mm_user_id"`
	Username    string `json:"username"`
	BotUsername string `json:"bot_username"`
}

// StartInit creates a code for the laptop. botName may be "" for the default.
func (c *Core) StartInit(ctx context.Context, botName, harnessName string) (InitStart, error) {
	if c.admin == nil {
		return InitStart{}, ErrInitDisabled
	}
	botName = strings.TrimSpace(strings.ToLower(botName))
	if botName != "" && !botUsernameRegex.MatchString(botName) {
		return InitStart{}, fmt.Errorf("bot name %q: 3-22 chars, lowercase letters, digits, . _ -, starting with a letter", botName)
	}
	code := randomCode(InitCodeLength)
	now := c.now()
	exp := now.Add(InitCodeTTL)
	err := c.st.CreateInit(ctx, store.InitRequest{CodeHash: hashCode(code), BotName: botName, HarnessName: harnessName, CreatedAt: now, ExpiresAt: exp})
	if err != nil {
		return InitStart{}, err
	}
	return InitStart{Code: code, ExpiresAt: exp, Command: "/harness init " + code}, nil
}

// PollInit returns ErrInitPending until the owner claimed the code, then the
// result exactly once.
func (c *Core) PollInit(ctx context.Context, code string) (InitResult, error) {
	r, err := c.st.FetchInit(ctx, hashCode(code), c.now())
	switch {
	case errors.Is(err, store.ErrNotFound):
		return InitResult{}, ErrInitUnknown
	case errors.Is(err, store.ErrConflict):
		return InitResult{}, ErrInitConsumed
	case err != nil:
		return InitResult{}, err
	case !r.Claimed():
		return InitResult{}, ErrInitPending
	}
	h, err := c.st.HarnessByID(ctx, r.HarnessID)
	if err != nil {
		return InitResult{}, err
	}
	res := InitResult{HarnessID: h.ID, Token: r.HarnessToken, MMUserID: h.MMUserID}
	if u, err := c.mm.GetUser(ctx, h.MMUserID); err == nil {
		res.Username = u.Username
	}
	if b, err := c.st.BotByOwner(ctx, h.MMUserID); err == nil {
		res.BotUsername = b.Username
	}
	return res, nil
}

// CommandRequest is the form Mattermost posts for a slash command.
type CommandRequest struct {
	Token     string
	UserID    string
	UserName  string
	ChannelID string
	TeamID    string
	RootID    string
	Text      string // everything after the trigger word
}

// HandleCommand serves the /harness slash command. The caller has verified
// the command token.
func (c *Core) HandleCommand(ctx context.Context, args CommandRequest) *model.CommandResponse {
	fields := strings.Fields(args.Text)
	sub := ""
	if len(fields) > 0 {
		sub = strings.ToLower(fields[0])
	}
	ephemeral := func(text string) *model.CommandResponse {
		return &model.CommandResponse{ResponseType: model.CommandResponseTypeEphemeral, Text: text}
	}
	switch sub {
	case "init":
		if len(fields) < 2 {
			return ephemeral("Usage: `/harness init <code>` with the code shown by `mhb harness init` on your machine.")
		}
		text, err := c.claimInit(ctx, args, fields[1])
		if err != nil {
			c.log.Warn("harness init", "user", args.UserID, "err", err)
			return ephemeral("Init failed: " + err.Error())
		}
		return ephemeral(text)
	case "join":
		text, err := c.joinChannel(ctx, args)
		if err != nil {
			return ephemeral("Join failed: " + err.Error())
		}
		return ephemeral(text)
	case "status":
		return ephemeral(c.statusText(ctx, args.UserID))
	default:
		return ephemeral("Commands: `/harness init <code>` (pair this machine, creates your bot), `/harness join` (bring your bot into this channel), `/harness status`.")
	}
}

// claimInit binds a pending code to the calling user: bot (created on first
// init), memberships, harness token.
func (c *Core) claimInit(ctx context.Context, args CommandRequest, code string) (string, error) {
	if c.admin == nil {
		return "", ErrInitDisabled
	}
	now := c.now()
	req, err := c.st.InitByCode(ctx, hashCode(code))
	if errors.Is(err, store.ErrNotFound) || (err == nil && !req.ExpiresAt.After(now)) {
		return "", ErrInitUnknown
	}
	if err != nil {
		return "", err
	}
	if req.Claimed() {
		return "", errors.New("this code was already used")
	}
	user, err := c.mm.GetUser(ctx, args.UserID)
	if err != nil {
		return "", err
	}
	bot, created, err := c.ensureBot(ctx, user, req.BotName)
	if err != nil {
		return "", err
	}
	// The owner's bot posts in the channel; the shared bot must be there too,
	// because only its connection sees the mentions.
	for _, id := range []string{bot.UserID, c.cfg.BotUserID} {
		if args.TeamID != "" {
			if err := c.admin.AddTeamMember(ctx, args.TeamID, id); err != nil {
				c.log.Warn("add team member", "user", id, "err", err)
			}
		}
		if err := c.admin.AddChannelMember(ctx, args.ChannelID, id); err != nil {
			c.log.Warn("add channel member", "user", id, "err", err)
		}
	}
	name := strings.TrimSpace(req.HarnessName)
	if name == "" {
		name = "harness"
	}
	token := "hrt_" + randomToken(32)
	h := store.Harness{ID: protocol.NewID("hrn"), MMUserID: user.Id, Name: name, TokenHash: tokenHash(token), CreatedAt: now}
	if err := c.st.CreateHarness(ctx, h); err != nil {
		return "", err
	}
	if err := c.st.ClaimInit(ctx, hashCode(code), now, h.ID, token); err != nil {
		return "", err
	}
	c.audit(ctx, user.Id, "harness.init", "", map[string]any{"harness": h.ID, "bot": bot.Username, "bot_created": created})
	verb := "is"
	if created {
		verb = "was created and is"
	}
	return fmt.Sprintf("Paired `%s`. Your bot @%s %s in this channel: mention it to run jobs on your machine. `/harness join` brings it into other channels.", name, bot.Username, verb), nil
}

// ensureBot returns the owner's bot, creating it on first init.
func (c *Core) ensureBot(ctx context.Context, owner *model.User, requested string) (store.Bot, bool, error) {
	if b, err := c.st.BotByOwner(ctx, owner.Id); err == nil {
		return b, false, nil
	}
	username := requested
	if username == "" {
		username = defaultBotName(owner.Username)
	}
	if !botUsernameRegex.MatchString(username) {
		return store.Bot{}, false, fmt.Errorf("bot name %q is not a valid Mattermost username", username)
	}
	if _, err := c.admin.GetUserByUsername(ctx, username); err == nil {
		return store.Bot{}, false, fmt.Errorf("the name @%s is taken; run `mhb harness init --bot <name>` with another one", username)
	}
	mmBot, err := c.admin.CreateBot(ctx, username, owner.Username+"'s harness", "Runs coding-agent jobs on "+owner.Username+"'s machine through the mhb broker")
	if err != nil {
		return store.Bot{}, false, err
	}
	token, err := c.admin.CreateBotToken(ctx, mmBot.UserId, "mhb broker")
	if err != nil {
		return store.Bot{}, false, err
	}
	b := store.Bot{UserID: mmBot.UserId, MMUserID: owner.Id, Username: username, Token: token, CreatedAt: c.now()}
	if err := c.st.CreateBot(ctx, b); err != nil {
		return store.Bot{}, false, err
	}
	return b, true, nil
}

func (c *Core) joinChannel(ctx context.Context, args CommandRequest) (string, error) {
	if c.admin == nil {
		return "", ErrInitDisabled
	}
	b, err := c.st.BotByOwner(ctx, args.UserID)
	if err != nil {
		return "", errors.New("you have no bot yet; run `mhb harness init` on your machine first")
	}
	for _, id := range []string{b.UserID, c.cfg.BotUserID} {
		if args.TeamID != "" {
			_ = c.admin.AddTeamMember(ctx, args.TeamID, id)
		}
		if err := c.admin.AddChannelMember(ctx, args.ChannelID, id); err != nil {
			return "", err
		}
	}
	return "@" + b.Username + " is now in this channel.", nil
}

// defaultBotName derives "harness-<username>", trimmed to Mattermost's limit.
func defaultBotName(username string) string {
	name := botNamePrefix + strings.ToLower(username)
	if len(name) > maxBotNameLength {
		name = name[:maxBotNameLength]
	}
	return strings.TrimRight(name, "._-")
}
