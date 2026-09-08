package core

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

const (
	InitCodeTTL      = 10 * time.Minute
	InitCodeLength   = 8
	botNamePrefix    = "harness-"
	maxBotNameLength = 22
)

var (
	ErrInitDisabled  = errors.New("onboarding is unavailable; ask an administrator to install the Mattermost integration")
	ErrInitUnknown   = errors.New("unknown or expired init code")
	ErrInitPending   = errors.New("init code not claimed yet")
	ErrInitConsumed  = errors.New("init result was already fetched")
	botUsernameRegex = regexp.MustCompile(`^[a-z][a-z0-9._-]{2,21}$`)
)

// WithProvisioner resolves the OAuth installation for each verified command's team.
func (c *Core) WithProvisioner(provider func(context.Context, string) (mattermost.Provisioner, error), factory func(string) mattermost.API) *Core {
	c.provisioner = provider
	c.newPoster = factory
	return c
}

type InitStart struct {
	Code      string    `json:"code"`
	PollToken string    `json:"poll_token"`
	ExpiresAt time.Time `json:"expires_at"`
	Command   string    `json:"command"`
}
type InitResult struct {
	HarnessID   string `json:"harness_id"`
	Token       string `json:"token"`
	MMUserID    string `json:"mm_user_id"`
	Username    string `json:"username"`
	BotUsername string `json:"bot_username"`
}

func (c *Core) StartInit(ctx context.Context, botName, harnessName string) (InitStart, error) {
	if c.provisioner == nil {
		return InitStart{}, ErrInitDisabled
	}
	botName = strings.ToLower(strings.TrimSpace(botName))
	if botName != "" && !botUsernameRegex.MatchString(botName) {
		return InitStart{}, errors.New("bot name must be 3-22 lowercase letters, digits, . _ or -, starting with a letter")
	}
	code, pollToken := randomCode(InitCodeLength), randomToken(32)
	now := c.now()
	expires := now.Add(InitCodeTTL)
	if err := c.st.CreateInit(ctx, store.InitRequest{CodeHash: hashCode(code), PollTokenHash: tokenHash(pollToken), BotName: botName, HarnessName: harnessName, CreatedAt: now, ExpiresAt: expires}); err != nil {
		return InitStart{}, err
	}
	return InitStart{Code: code, PollToken: pollToken, ExpiresAt: expires, Command: "/harness init " + code}, nil
}

// PollInit requires the laptop's secret as well as the publicly typed code.
// Authenticate before FetchInit, which consumes a successful result exactly once.
func (c *Core) PollInit(ctx context.Context, code, pollToken string) (InitResult, error) {
	req, err := c.st.InitByCode(ctx, hashCode(code))
	if errors.Is(err, store.ErrNotFound) {
		return InitResult{}, ErrInitUnknown
	}
	if err != nil {
		return InitResult{}, err
	}
	if pollToken == "" || req.PollTokenHash == "" || subtle.ConstantTimeCompare([]byte(tokenHash(pollToken)), []byte(req.PollTokenHash)) != 1 {
		return InitResult{}, ErrInitUnknown
	}
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
	result := InitResult{HarnessID: h.ID, Token: r.HarnessToken, MMUserID: h.MMUserID}
	if bot, err := c.st.BotByUserID(ctx, r.BotUserID); err == nil {
		result.BotUsername = bot.Username
		if p, err := c.installation(ctx, bot.TeamID); err == nil {
			if user, err := p.GetUser(ctx, h.MMUserID); err == nil && user != nil {
				result.Username = user.Username
			}
		}
	}
	return result, nil
}

type CommandRequest struct {
	Token     string
	UserID    string
	UserName  string
	ChannelID string
	TeamID    string
	RootID    string
	Text      string
}

// HandleCommand trusts only identity carried by the verified team slash command.
func (c *Core) HandleCommand(ctx context.Context, args CommandRequest) *model.CommandResponse {
	fields := strings.Fields(args.Text)
	ephemeral := func(text string) *model.CommandResponse {
		return &model.CommandResponse{ResponseType: model.CommandResponseTypeEphemeral, Text: text}
	}
	if len(fields) == 0 {
		return ephemeral(commandHelp)
	}
	var text string
	var err error
	switch strings.ToLower(fields[0]) {
	case "init":
		if len(fields) == 1 {
			return ephemeral("Run `mhb harness init --broker " + c.cfg.PublicURL + "` on the machine that will run your coding agents. Then paste its `/harness init <code>` command here. Add a bot name with `/harness init <code> <bot-name>`.")
		}
		if len(fields) > 3 {
			return ephemeral("Usage: `/harness init <code> [bot-name]`.")
		}
		name := ""
		if len(fields) == 3 {
			name = fields[2]
		}
		text, err = c.claimInit(ctx, args, fields[1], name)
	case "bot":
		if len(fields) != 4 || strings.ToLower(fields[1]) != "create" {
			return ephemeral("Usage: `/harness bot create <name> <harness-id>`.")
		}
		text, err = c.createBoundBot(ctx, args, fields[2], fields[3])
	case "join":
		if len(fields) != 2 {
			return ephemeral("Usage: `/harness join <bot-name>`.")
		}
		text, err = c.joinChannel(ctx, args, fields[1])
	case "status":
		return ephemeral(c.onboardingStatusText(ctx, args.UserID))
	default:
		return ephemeral(commandHelp)
	}
	if err != nil {
		return ephemeral("Command failed: " + err.Error())
	}
	return ephemeral(text)
}

const commandHelp = "Commands: `/harness init <code> [bot-name]` pairs a machine and creates a bot; `/harness bot create <name> <harness-id>` adds a bot; `/harness join <bot-name>` brings it into this channel; `/harness status` lists your bots and harnesses. Start pairing with `mhb harness init` on your machine."

func (c *Core) installation(ctx context.Context, teamID string) (mattermost.Provisioner, error) {
	if c.provisioner == nil || teamID == "" {
		return nil, ErrInitDisabled
	}
	p, err := c.provisioner(ctx, teamID)
	if err != nil || p == nil {
		return nil, ErrInitDisabled
	}
	return p, nil
}
func (c *Core) claimInit(ctx context.Context, args CommandRequest, code, requested string) (string, error) {
	c.onboardingMu.Lock()
	defer c.onboardingMu.Unlock()
	req, err := c.st.InitByCode(ctx, hashCode(code))
	if errors.Is(err, store.ErrNotFound) || (err == nil && !req.ExpiresAt.After(c.now())) {
		return "", ErrInitUnknown
	}
	if err != nil {
		return "", errors.New("could not read pairing request")
	}
	if req.Claimed() {
		return "", errors.New("this code was already used")
	}
	p, err := c.installation(ctx, args.TeamID)
	if err != nil {
		return "", err
	}
	if requested == "" {
		requested = req.BotName
	}
	bot, err := c.provisionBot(ctx, p, args, requested, "")
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(req.HarnessName)
	if name == "" {
		name = "harness"
	}
	token := "hrt_" + randomToken(32)
	h := store.Harness{ID: protocol.NewID("hrn"), MMUserID: args.UserID, Name: name, TokenHash: tokenHash(token), CreatedAt: c.now()}
	if err := c.st.CompleteInit(ctx, hashCode(code), c.now(), h, bot.UserID, token); err != nil {
		c.rollbackBot(ctx, p, bot)
		return "", errors.New("could not complete pairing; try again with a new bot name")
	}
	c.audit(ctx, args.UserID, "harness.init", "", map[string]any{"harness": h.ID, "bot": bot.Username})
	return fmt.Sprintf("Paired `%s` (`%s`). Your bot @%s is ready in this channel. Mention it to run jobs on this machine. Use `/harness join %s` in another channel to add it there.", name, h.ID, bot.Username, bot.Username), nil
}
func (c *Core) createBoundBot(ctx context.Context, args CommandRequest, name, harnessID string) (string, error) {
	c.onboardingMu.Lock()
	defer c.onboardingMu.Unlock()
	h, err := c.st.HarnessByID(ctx, harnessID)
	if err != nil || h.MMUserID != args.UserID {
		return "", errors.New("harness not found among your paired machines; use `/harness status`")
	}
	p, err := c.installation(ctx, args.TeamID)
	if err != nil {
		return "", err
	}
	bot, err := c.provisionBot(ctx, p, args, name, h.ID)
	if err != nil {
		return "", err
	}
	c.audit(ctx, args.UserID, "bot.created", "", map[string]any{"harness": h.ID, "bot": bot.Username})
	return fmt.Sprintf("Created @%s, bound to `%s` (`%s`), in this channel.", bot.Username, h.Name, h.ID), nil
}

// provisionBot persists only a complete account and pair of native hooks. Each
// remote operation is compensated if any later operation fails.
func (c *Core) provisionBot(ctx context.Context, p mattermost.Provisioner, args CommandRequest, requested, harnessID string) (bot store.Bot, err error) {
	if args.UserID == "" {
		return bot, errors.New("missing command owner")
	}
	channel, err := p.GetChannel(ctx, args.ChannelID)
	if err != nil || channel == nil {
		return bot, errors.New("could not read this channel")
	}
	if channel.TeamId != args.TeamID || (channel.Type != model.ChannelTypeOpen && channel.Type != model.ChannelTypePrivate) {
		return bot, errors.New("create bots in a public or private team channel")
	}
	owner, err := p.GetUser(ctx, args.UserID)
	if err != nil || owner == nil {
		return bot, errors.New("could not look up your Mattermost account")
	}
	username := strings.ToLower(strings.TrimSpace(requested))
	if username == "" {
		base := defaultBotName(owner.Username)
		if len(base) > maxBotNameLength-7 {
			base = base[:maxBotNameLength-7]
		}
		username = strings.TrimRight(base, "._-") + "-" + randomToken(3)
	}
	if !botUsernameRegex.MatchString(username) {
		return bot, errors.New("bot name must be 3-22 lowercase letters, digits, . _ or -, starting with a letter")
	}
	if _, lookupErr := c.st.BotByUsername(ctx, username); lookupErr == nil {
		return bot, errors.New("that bot name is already taken; choose another name")
	} else if !errors.Is(lookupErr, store.ErrNotFound) {
		return bot, errors.New("could not check bot name")
	}
	account, err := p.CreateBot(ctx, username, owner.Username+"'s harness", "Runs coding-agent jobs for "+owner.Username+" through the mhb broker")
	if err != nil || account == nil || account.UserId == "" {
		return bot, errors.New("could not create bot; check installation permissions and choose an unused bot name")
	}
	bot = store.Bot{UserID: account.UserId, MMUserID: args.UserID, Username: username, HarnessID: harnessID, TeamID: args.TeamID, ChannelID: args.ChannelID, CreatedAt: c.now()}
	defer func() {
		if err != nil {
			c.rollbackBot(ctx, p, bot)
		}
	}()
	if bot.Token, err = p.CreateBotToken(ctx, bot.UserID, "mhb broker"); err != nil || bot.Token == "" {
		return bot, errors.New("could not create bot access token")
	}
	if err = p.AddTeamMember(ctx, args.TeamID, bot.UserID); err != nil {
		return bot, errors.New("could not add bot to this team")
	}
	if err = p.AddChannelMember(ctx, args.ChannelID, bot.UserID); err != nil {
		return bot, errors.New("could not add bot to this channel")
	}
	// An OAuth installer needs manage_others_incoming_webhooks to assign UserId.
	incoming, err := p.CreateIncomingWebhook(ctx, &model.IncomingWebhook{UserId: bot.UserID, TeamId: args.TeamID, ChannelId: args.ChannelID, DisplayName: username, Username: username})
	if incoming != nil {
		bot.IncomingHookID = incoming.Id
	}
	if err != nil || incoming == nil || incoming.Id == "" || incoming.UserId != bot.UserID {
		return bot, errors.New("could not create incoming webhook owned by the bot; check installation permissions")
	}
	// Team-wide trigger hooks cover public channels; the bot's WebSocket covers
	// private channels and DMs. Mattermost rejects outgoing private-channel hooks.
	outgoing, err := p.CreateOutgoingWebhook(ctx, &model.OutgoingWebhook{TeamId: args.TeamID, DisplayName: username, TriggerWords: model.StringArray{"@" + username}, CallbackURLs: model.StringArray{strings.TrimRight(c.cfg.PublicURL, "/") + "/webhooks/mattermost/" + bot.UserID}, ContentType: "application/x-www-form-urlencoded"})
	if outgoing != nil {
		bot.OutgoingHookID = outgoing.Id
		bot.OutgoingToken = outgoing.Token
	}
	if err != nil || outgoing == nil || outgoing.Id == "" || outgoing.Token == "" {
		return bot, errors.New("could not create outgoing webhook")
	}
	if err = c.st.CreateBot(ctx, bot); err != nil {
		return bot, errors.New("could not save bot configuration")
	}
	return bot, nil
}

func (c *Core) rollbackBot(ctx context.Context, p mattermost.Provisioner, bot store.Bot) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	clean := func(op string, err error) {
		if err != nil {
			c.log.Error("bot provisioning rollback failed", "operation", op, "bot", bot.UserID)
		}
	}
	if bot.OutgoingHookID != "" {
		clean("delete outgoing webhook", p.DeleteOutgoingWebhook(cleanup, bot.OutgoingHookID))
	}
	if bot.IncomingHookID != "" {
		clean("delete incoming webhook", p.DeleteIncomingWebhook(cleanup, bot.IncomingHookID))
	}
	if bot.UserID != "" {
		clean("disable bot", p.DisableBot(cleanup, bot.UserID))
		if err := c.st.DeleteBot(cleanup, bot.UserID); err != nil && !errors.Is(err, store.ErrNotFound) {
			clean("delete bot configuration", err)
		}
	}
}

func (c *Core) joinChannel(ctx context.Context, args CommandRequest, name string) (string, error) {
	bot, err := c.st.BotByUsername(ctx, strings.TrimPrefix(strings.ToLower(name), "@"))
	if err != nil || bot.MMUserID != args.UserID {
		return "", errors.New("bot not found among your bots; use `/harness status`")
	}
	if bot.TeamID != args.TeamID {
		return "", errors.New("use a channel in the bot's installed team")
	}
	p, err := c.installation(ctx, args.TeamID)
	if err != nil {
		return "", err
	}
	channel, err := p.GetChannel(ctx, args.ChannelID)
	if err != nil || channel == nil {
		return "", errors.New("could not read this channel")
	}
	if channel.TeamId != args.TeamID || (channel.Type != model.ChannelTypeOpen && channel.Type != model.ChannelTypePrivate) {
		return "", errors.New("use `/harness join <bot-name>` in a public or private team channel")
	}
	if err = p.AddChannelMember(ctx, args.ChannelID, bot.UserID); err != nil {
		return "", errors.New("could not add bot to this channel")
	}
	return "@" + bot.Username + " is now in this channel.", nil
}
func (c *Core) onboardingStatusText(ctx context.Context, userID string) string {
	harnesses, err := c.st.HarnessesByUser(ctx, userID)
	if err != nil {
		return "Could not read your harnesses."
	}
	bots, err := c.st.BotsByOwner(ctx, userID)
	if err != nil {
		return "Could not read your bots."
	}
	if len(harnesses) == 0 {
		return "No paired harness. Run `mhb harness init` on your machine, then paste the command here."
	}
	var text strings.Builder
	for _, h := range harnesses {
		state := "offline"
		if c.hub.Online(h.ID) {
			state = "online"
		}
		fmt.Fprintf(&text, "- `%s` (`%s`): %s", h.Name, h.ID, state)
		for _, bot := range bots {
			if bot.HarnessID == h.ID {
				fmt.Fprintf(&text, " · @%s", bot.Username)
			}
		}
		text.WriteByte('\n')
	}
	return text.String()
}
func defaultBotName(username string) string {
	name := botNamePrefix + strings.ToLower(username)
	if len(name) > maxBotNameLength {
		name = name[:maxBotNameLength]
	}
	return strings.TrimRight(name, "._-")
}
