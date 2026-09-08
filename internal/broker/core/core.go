// Package core is the broker's domain logic: mentions become jobs, jobs go
// to the owner's harness, progress and approvals become posts. It depends
// only on interfaces so it can be tested without Mattermost or sockets.
package core

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mattermost/mattermost/server/public/model"

	"github.com/bambamboole/mattermost-harness-bridge/internal/broker/mattermost"
	"github.com/bambamboole/mattermost-harness-bridge/internal/protocol"
	"github.com/bambamboole/mattermost-harness-bridge/internal/store"
)

type Config struct {
	BotUserID      string
	BotUsername    string
	PublicURL      string // where Mattermost reaches the callback endpoint
	CallbackSecret []byte
	// CommandToken authenticates the /harness slash command; "" disables it.
	CommandToken string

	QueueTTL        time.Duration // how long a job waits for an offline harness
	GracePeriod     time.Duration // offline harness with a running job -> lost
	ApprovalTTL     time.Duration
	JobTimeout      time.Duration
	DefaultMaxTurns int
	EditInterval    time.Duration // minimum gap between edits of one status post
	// OutboxRetention is how long acked outbox rows are kept before Sweep
	// deletes them. They exist only for debugging a delivery.
	OutboxRetention time.Duration
}

func (c *Config) defaults() {
	if c.QueueTTL == 0 {
		c.QueueTTL = 5 * time.Minute
	}
	if c.GracePeriod == 0 {
		c.GracePeriod = 10 * time.Minute
	}
	if c.ApprovalTTL == 0 {
		c.ApprovalTTL = 30 * time.Minute
	}
	if c.JobTimeout == 0 {
		c.JobTimeout = 30 * time.Minute
	}
	if c.DefaultMaxTurns == 0 {
		c.DefaultMaxTurns = 40
	}
	if c.EditInterval == 0 {
		c.EditInterval = 1200 * time.Millisecond
	}
	if c.OutboxRetention == 0 {
		c.OutboxRetention = 24 * time.Hour
	}
}

// Sender is the hub as the core sees it.
type Sender interface {
	Online(harnessID string) bool
	Send(ctx context.Context, harnessID string, env protocol.Envelope) error
	SendReliable(ctx context.Context, harnessID string, env protocol.Envelope) error
}

type Core struct {
	cfg   Config
	st    store.Store
	mm    mattermost.API   // the shared listener bot
	admin mattermost.Admin // nil: onboarding disabled
	hub   Sender
	log   *slog.Logger
	now   func() time.Time
	// newPoster makes an API client for a user bot's token.
	newPoster func(token string) mattermost.API

	mu      sync.Mutex
	edits   map[string]*editor        // job id -> status post debouncer
	posters map[string]mattermost.API // bot user id -> client
}

func New(cfg Config, st store.Store, mm mattermost.API, hub Sender, log *slog.Logger) *Core {
	cfg.defaults()
	return &Core{cfg: cfg, st: st, mm: mm, hub: hub, log: log, now: time.Now, edits: map[string]*editor{}, posters: map[string]mattermost.API{}}
}

// WithOnboarding enables /harness init: admin creates bots and memberships,
// newPoster builds the client that posts as a user's bot.
func (c *Core) WithOnboarding(admin mattermost.Admin, newPoster func(token string) mattermost.API) *Core {
	c.admin = admin
	c.newPoster = newPoster
	return c
}

// api returns the client that posts for a bot: the owner's bot by id, or
// the shared bot for "" and for bots the broker cannot build a client for.
func (c *Core) api(ctx context.Context, botUserID string) mattermost.API {
	if botUserID == "" || botUserID == c.cfg.BotUserID || c.newPoster == nil {
		return c.mm
	}
	c.mu.Lock()
	p, ok := c.posters[botUserID]
	c.mu.Unlock()
	if ok {
		return p
	}
	b, err := c.st.BotByUserID(ctx, botUserID)
	if err != nil {
		c.log.Warn("unknown bot, posting as shared bot", "bot", botUserID)
		return c.mm
	}
	p = c.newPoster(b.Token)
	c.mu.Lock()
	c.posters[botUserID] = p
	c.mu.Unlock()
	return p
}

// --- inbound posts ---------------------------------------------------------

// HandlePost is called for every post the bot sees.
func (c *Core) HandlePost(ctx context.Context, ev mattermost.PostedEvent) {
	p := ev.Post
	if p == nil || p.UserId == c.cfg.BotUserID || p.GetProp(model.PostPropsFromBot) != nil || p.Type != "" {
		return
	}
	if model.ChannelType(ev.ChannelType) == model.ChannelTypeDirect {
		c.handleDM(ctx, p)
		return
	}
	target, ok := c.mentionedBot(ctx, ev)
	if !ok {
		return
	}
	if target.owner != "" && target.owner != p.UserId {
		who := "its owner"
		if target.ownerName != "" {
			who = "@" + target.ownerName
		}
		c.replyAs(ctx, target.userID, p, fmt.Sprintf("Only %s can run jobs on this machine.", who))
		c.audit(ctx, p.UserId, "job.forbidden", "", map[string]any{"bot": target.username})
		return
	}
	text := stripMention(p.Message, target.username)
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "cancel", "stop":
		c.handleCancel(ctx, p, target)
		return
	case "", "help":
		c.replyAs(ctx, target.userID, p, helpText(target.username))
		return
	}
	c.createJob(ctx, p, text, target)
}

// mention is the bot a post addressed: the shared bot (owner "") or a
// user's bot (owner set). A user bot wins when both are mentioned.
// ownerName is the owner's @name, "" when Mattermost could not be asked.
type mention struct {
	userID    string
	username  string
	owner     string
	ownerName string
}

// Mattermost trims the `mentions` of a posted event to the receiving
// connection, so the listener only ever sees itself there. User bots are
// therefore recognised from the @names in the text.
func (c *Core) mentionedBot(ctx context.Context, ev mattermost.PostedEvent) (mention, bool) {
	names := mentionedNames(ev.Post.Message)
	if len(names) > 0 {
		bots, err := c.st.ListBots(ctx)
		if err != nil {
			c.log.Error("list bots", "err", err)
		}
		for _, b := range bots {
			if !names[b.Username] {
				continue
			}
			m := mention{userID: b.UserID, username: b.Username, owner: b.MMUserID}
			if u, err := c.mm.GetUser(ctx, b.MMUserID); err == nil {
				m.ownerName = u.Username
			}
			return m, true
		}
	}
	for _, id := range ev.Mentions {
		if id == c.cfg.BotUserID {
			return mention{userID: c.cfg.BotUserID, username: c.cfg.BotUsername}, true
		}
	}
	if names[c.cfg.BotUsername] {
		return mention{userID: c.cfg.BotUserID, username: c.cfg.BotUsername}, true
	}
	return mention{}, false
}

var mentionRegex = regexp.MustCompile(`(?:^|[^A-Za-z0-9_])@([A-Za-z0-9][A-Za-z0-9._-]*)`)

// mentionedNames returns the @names in a message, lower-cased, without
// trailing punctuation.
func mentionedNames(text string) map[string]bool {
	out := map[string]bool{}
	for _, m := range mentionRegex.FindAllStringSubmatch(text, -1) {
		name := strings.ToLower(strings.TrimRight(m[1], ".,;:!?-_"))
		if name != "" {
			out[name] = true
		}
	}
	return out
}

func (c *Core) handleDM(ctx context.Context, p *model.Post) {
	fields := strings.Fields(strings.TrimSpace(p.Message))
	cmd := ""
	if len(fields) > 0 {
		cmd = strings.ToLower(fields[0])
	}
	switch cmd {
	case "pair":
		if c.admin != nil {
			c.reply(ctx, p, "Run `mhb harness init --broker "+c.cfg.PublicURL+"` on your machine; it creates your own bot and pairs in one go.")
			return
		}
		code, err := c.newPairingCode(ctx, p.UserId)
		if err != nil {
			c.log.Error("pairing code", "err", err)
			c.reply(ctx, p, "Could not create a pairing code, see broker logs.")
			return
		}
		c.reply(ctx, p, fmt.Sprintf("Run this on the machine that should execute your jobs (code valid for 10 minutes):\n```\nmhb harness pair --broker %s %s\n```", c.cfg.PublicURL, code))
	case "status":
		c.reply(ctx, p, c.statusText(ctx, p.UserId))
	default:
		c.reply(ctx, p, helpText(c.cfg.BotUsername))
	}
}

func (c *Core) handleCancel(ctx context.Context, p *model.Post, target mention) {
	root := rootOf(p)
	jobs, _ := c.st.ListJobs(ctx, store.JobFilter{RootPostID: root, MMUserID: p.UserId, States: store.ActiveStates, Limit: 5})
	if len(jobs) == 0 {
		c.replyAs(ctx, target.userID, p, "Nothing running in this thread for you.")
		return
	}
	for _, j := range jobs {
		if err := c.cancelJob(ctx, j, "user"); err != nil {
			c.log.Error("cancel", "job", j.ID, "err", err)
		}
	}
	c.replyAs(ctx, target.userID, p, "Cancelling.")
}

func (c *Core) cancelJob(ctx context.Context, j store.Job, reason string) error {
	if j.State == store.JobQueued {
		if _, err := c.st.TransitionJob(ctx, j.ID, []store.JobState{store.JobQueued}, store.JobCancelled, store.JobPatch{}); err != nil {
			return err
		}
		c.finalEdit(ctx, j, "🚫 Cancelled before the harness picked it up.")
		return nil
	}
	env, err := protocol.New(protocol.TypeJobCancel, j.ID, protocol.JobCancel{Reason: reason})
	if err != nil {
		return err
	}
	c.audit(ctx, j.MMUserID, "job.cancel_requested", j.ID, map[string]any{"reason": reason})
	return c.hub.SendReliable(ctx, j.HarnessID, env)
}

// createJob turns a mention into a dispatched (or queued) job posted by
// the mentioned bot.
func (c *Core) createJob(ctx context.Context, p *model.Post, text string, target mention) {
	harness, ok := c.pickHarness(ctx, p.UserId)
	if !ok {
		c.replyAs(ctx, target.userID, p, "You have no paired harness. Run `mhb harness init` on your machine to set one up.")
		return
	}
	root := rootOf(p)
	if active, _ := c.st.ListJobs(ctx, store.JobFilter{RootPostID: root, States: store.ActiveStates, Limit: 1}); len(active) > 0 {
		c.replyAs(ctx, target.userID, p, "A job is already running in this thread. Say `@"+target.username+" cancel` to stop it.")
		return
	}
	workspace, prompt := splitWorkspace(text)
	agentName, prompt := splitAgent(prompt)
	if strings.TrimSpace(prompt) == "" {
		c.replyAs(ctx, target.userID, p, "Tell me what to do after the mention.")
		return
	}
	botUserID := ""
	if target.owner != "" {
		botUserID = target.userID
	}
	poster := c.api(ctx, botUserID)
	user, err := c.mm.GetUser(ctx, p.UserId)
	if err != nil {
		c.log.Error("get user", "err", err)
		return
	}
	attachments := c.downloadAttachments(ctx, p)
	history := c.threadHistory(ctx, p, root)

	online := c.hub.Online(harness.ID)
	now := c.now()
	status := "⏳ Starting on `" + harness.Name + "`…"
	if !online {
		status = fmt.Sprintf("💤 `%s` is offline. Holding the job for %s.", harness.Name, c.cfg.QueueTTL)
	}
	statusPost, err := poster.CreatePost(ctx, &model.Post{ChannelId: p.ChannelId, RootId: root, Message: status})
	if err != nil {
		c.log.Error("status post", "err", err)
		return
	}
	job := store.Job{
		ID: protocol.NewID("job"), HarnessID: harness.ID, MMUserID: p.UserId, ChannelID: p.ChannelId,
		RootPostID: root, TriggerPostID: p.Id, StatusPostID: statusPost.Id, BotUserID: botUserID, Workspace: workspace, Prompt: prompt,
		State: store.JobDispatched, CreatedAt: now, UpdatedAt: now,
	}
	if !online {
		job.State = store.JobQueued
		exp := now.Add(c.cfg.QueueTTL)
		job.ExpiresAt = &exp
	}
	if err := c.st.CreateJob(ctx, job); err != nil {
		c.log.Error("create job", "err", err)
		return
	}
	env, err := protocol.New(protocol.TypeJobDispatch, job.ID, protocol.JobDispatch{
		Workspace:   workspace,
		Agent:       agentName,
		Prompt:      prompt,
		Thread:      protocol.Thread{ChannelID: p.ChannelId, RootPostID: root, TriggerPostID: p.Id},
		Requester:   protocol.Requester{MMUserID: user.Id, Username: user.Username},
		Attachments: attachments,
		History:     history,
		Limits:      protocol.Limits{MaxTurns: c.cfg.DefaultMaxTurns, TimeoutMS: int(c.cfg.JobTimeout / time.Millisecond)},
	})
	if err == nil {
		err = c.hub.SendReliable(ctx, harness.ID, env)
	}
	if err != nil {
		c.log.Error("dispatch", "job", job.ID, "err", err)
		c.fail(ctx, job, "dispatch failed: "+err.Error())
		return
	}
	c.audit(ctx, p.UserId, "job.created", job.ID, map[string]any{"harness": harness.ID, "workspace": workspace, "agent": agentName, "history": len(history), "queued": !online})
	c.log.Info("job created", "job", job.ID, "user", user.Username, "harness", harness.ID, "workspace", workspace, "agent", agentName, "history", len(history), "queued", !online)
}

// pickHarness: the user's most recently seen harness. One per user is the
// expected setup; several are tolerated.
func (c *Core) pickHarness(ctx context.Context, userID string) (store.Harness, bool) {
	list, err := c.st.HarnessesByUser(ctx, userID)
	if err != nil || len(list) == 0 {
		return store.Harness{}, false
	}
	best := list[0]
	for _, h := range list[1:] {
		if c.hub.Online(h.ID) && !c.hub.Online(best.ID) {
			best = h
			continue
		}
		if h.LastSeenAt != nil && (best.LastSeenAt == nil || h.LastSeenAt.After(*best.LastSeenAt)) && c.hub.Online(h.ID) == c.hub.Online(best.ID) {
			best = h
		}
	}
	return best, true
}

// MaxHistoryPosts bounds the thread transcript sent with a dispatch.
const MaxHistoryPosts = 60

// threadHistory collects the thread's earlier posts for a mention inside an
// existing thread: everything before the trigger except the bot's own
// status and approval posts. The harness uses it only for a thread it has
// no session for yet.
func (c *Core) threadHistory(ctx context.Context, trigger *model.Post, root string) []protocol.HistoryPost {
	if trigger.RootId == "" {
		return nil // a fresh thread: the trigger is the whole conversation
	}
	posts, err := c.mm.GetThread(ctx, root)
	if err != nil {
		c.log.Warn("thread history", "root", root, "err", err)
		return nil
	}
	names := map[string]string{}
	var out []protocol.HistoryPost
	for _, p := range posts {
		if p.Id == trigger.Id || p.UserId == c.cfg.BotUserID || p.GetProp(model.PostPropsFromBot) != nil || p.Type != "" {
			continue
		}
		if _, err := c.st.BotByUserID(ctx, p.UserId); err == nil {
			continue
		}
		if p.CreateAt >= trigger.CreateAt && trigger.CreateAt > 0 {
			continue
		}
		name, ok := names[p.UserId]
		if !ok {
			name = p.UserId
			if u, err := c.mm.GetUser(ctx, p.UserId); err == nil {
				name = u.Username
			}
			names[p.UserId] = name
		}
		out = append(out, protocol.HistoryPost{Username: name, At: p.CreateAt, Text: stripMention(p.Message, c.cfg.BotUsername)})
	}
	if len(out) > MaxHistoryPosts {
		out = out[len(out)-MaxHistoryPosts:]
	}
	return out
}

func (c *Core) downloadAttachments(ctx context.Context, p *model.Post) []protocol.File {
	var out []protocol.File
	for _, id := range p.FileIds {
		fi, err := c.mm.GetFileInfo(ctx, id)
		if err != nil || fi.Size > protocol.MaxFileBytes {
			c.log.Warn("attachment skipped", "file", id, "err", err)
			continue
		}
		data, err := c.mm.DownloadFile(ctx, id)
		if err != nil {
			c.log.Warn("attachment download", "file", id, "err", err)
			continue
		}
		out = append(out, protocol.File{Name: fi.Name, Mime: fi.MimeType, DataB64: base64.StdEncoding.EncodeToString(data)})
	}
	return out
}

// --- harness events (hub.Handler) ------------------------------------------

func (c *Core) OnHello(ctx context.Context, h store.Harness, hello protocol.Hello) []protocol.WelcomeJob {
	active, err := c.st.ActiveJobsByHarness(ctx, h.ID)
	if err != nil {
		c.log.Error("active jobs", "err", err)
	}
	known := map[string]store.Job{}
	for _, j := range active {
		known[j.ID] = j
	}
	var out []protocol.WelcomeJob
	reported := map[string]bool{}
	for _, hj := range hello.Jobs {
		reported[hj.JobID] = true
		j, ok := known[hj.JobID]
		if !ok {
			out = append(out, protocol.WelcomeJob{JobID: hj.JobID, Action: protocol.ActionAbort, Reason: "unknown or finished"})
			continue
		}
		out = append(out, protocol.WelcomeJob{JobID: hj.JobID, Action: protocol.ActionResume})
		if j.State == store.JobQueued || j.State == store.JobDispatched {
			// The harness already runs it; the dispatch ack got lost.
			_, _ = c.st.TransitionJob(ctx, j.ID, []store.JobState{store.JobQueued, store.JobDispatched}, store.JobRunning, store.JobPatch{})
		}
	}
	for _, j := range active {
		if reported[j.ID] {
			continue
		}
		switch j.State {
		case store.JobRunning, store.JobAwaitingApproval:
			c.fail(ctx, j, "the harness restarted and no longer has this job")
		case store.JobQueued:
			// Its dispatch sits in the outbox; the hub flushes it right after welcome.
			if _, err := c.st.TransitionJob(ctx, j.ID, []store.JobState{store.JobQueued}, store.JobDispatched, store.JobPatch{}); err == nil {
				c.edit(j, "⏳ Harness is back, starting…")
			}
		}
	}
	return out
}

func (c *Core) OnAck(ctx context.Context, h store.Harness, ref, jobID string, a protocol.Ack) {
	if jobID == "" {
		return
	}
	if a.OK {
		if _, err := c.st.TransitionJob(ctx, jobID, []store.JobState{store.JobDispatched}, store.JobRunning, store.JobPatch{}); err == nil {
			if j, err := c.st.JobByID(ctx, jobID); err == nil {
				c.edit(j, "▶️ Running on `"+h.Name+"`…")
			}
		}
		return
	}
	j, err := c.st.JobByID(ctx, jobID)
	if err != nil || j.HarnessID != h.ID {
		return
	}
	if j.State == store.JobDispatched {
		c.fail(ctx, j, "harness rejected the job: "+nackText(a))
		return
	}
	c.log.Warn("harness nacked", "job", jobID, "ref", ref, "code", a.Code, "msg", a.Message)
}

func (c *Core) OnProgress(ctx context.Context, h store.Harness, jobID string, p protocol.JobProgress) {
	applied, err := c.st.RecordProgress(ctx, jobID, p.Seq)
	if err != nil || !applied {
		return
	}
	j, err := c.st.JobByID(ctx, jobID)
	if err != nil || j.HarnessID != h.ID {
		return
	}
	c.edit(j, renderProgress(h.Name, p))
}

func (c *Core) OnApprovalRequest(ctx context.Context, h store.Harness, jobID string, r protocol.ApprovalRequest) protocol.Ack {
	j, err := c.st.JobByID(ctx, jobID)
	if err != nil {
		return protocol.Ack{OK: false, Code: protocol.NackUnknownJob}
	}
	if j.HarnessID != h.ID {
		return protocol.Ack{OK: false, Code: protocol.NackForbidden}
	}
	if j.State.Terminal() {
		return protocol.Ack{OK: false, Code: protocol.NackUnknownJob, Message: "job is " + string(j.State)}
	}
	nonce := randomToken(16)
	err = c.st.CreateApproval(ctx, store.Approval{
		ID: r.ApprovalID, JobID: jobID, Tool: r.Tool, Summary: r.Summary, Input: r.Input, Nonce: nonce,
		RequestedAt: c.now(), ExpiresAt: time.UnixMilli(r.ExpiresAt),
	})
	if errors.Is(err, store.ErrConflict) {
		return protocol.Ack{OK: true} // resend; the button post exists
	}
	if err != nil {
		c.log.Error("create approval", "err", err)
		return protocol.Ack{OK: false, Code: "store", Message: err.Error()}
	}
	_, _ = c.st.TransitionJob(ctx, jobID, []store.JobState{store.JobDispatched, store.JobRunning}, store.JobAwaitingApproval, store.JobPatch{})
	post := &model.Post{ChannelId: j.ChannelID, RootId: j.RootPostID, Message: ""}
	post.AddProp(model.PostPropsAttachments, []*model.MessageAttachment{c.approvalAttachment(r, nonce)})
	if _, err := c.api(ctx, j.BotUserID).CreatePost(ctx, post); err != nil {
		c.log.Error("approval post", "err", err)
	}
	c.edit(j, fmt.Sprintf("⏸ Waiting for approval: **%s** `%s`", r.Tool, truncate(r.Summary, 200)))
	c.audit(ctx, h.ID, "approval.requested", jobID, map[string]any{"approval": r.ApprovalID, "tool": r.Tool, "summary": r.Summary})
	return protocol.Ack{OK: true}
}

func (c *Core) OnResult(ctx context.Context, h store.Harness, jobID string, r protocol.JobResult) protocol.Ack {
	j, err := c.st.JobByID(ctx, jobID)
	if err != nil {
		return protocol.Ack{OK: false, Code: protocol.NackUnknownJob}
	}
	if j.HarnessID != h.ID {
		return protocol.Ack{OK: false, Code: protocol.NackForbidden}
	}
	if j.State.Terminal() && j.State != store.JobLost {
		return protocol.Ack{OK: true} // resend
	}
	to := store.JobFailed
	switch r.Status {
	case protocol.StatusSucceeded:
		to = store.JobSucceeded
	case protocol.StatusCancelled:
		to = store.JobCancelled
	}
	text := r.Text
	patch := store.JobPatch{ResultText: &text}
	if r.Error != nil {
		msg := r.Error.Code + ": " + r.Error.Message
		patch.Error = &msg
	}
	from := append([]store.JobState{store.JobLost}, store.ActiveStates...)
	j, err = c.st.TransitionJob(ctx, jobID, from, to, patch)
	if err != nil {
		return protocol.Ack{OK: false, Code: "store", Message: err.Error()}
	}
	c.postResult(ctx, j, h, r)
	c.audit(ctx, h.ID, "job.finished", jobID, map[string]any{"status": r.Status, "turns": r.Usage.Turns, "cost_usd": r.Usage.CostUSD})
	return protocol.Ack{OK: true}
}

func (c *Core) OnDisconnect(ctx context.Context, h store.Harness) {
	jobs, _ := c.st.ListJobs(ctx, store.JobFilter{HarnessID: h.ID, States: []store.JobState{store.JobRunning, store.JobAwaitingApproval, store.JobDispatched}})
	for _, j := range jobs {
		c.edit(j, fmt.Sprintf("📡 `%s` disconnected. Waiting up to %s for it to come back.", h.Name, c.cfg.GracePeriod))
	}
}

// --- approvals via callback -------------------------------------------------

const (
	ctxApproval = "approval_id"
	ctxDecision = "decision"
	ctxSig      = "sig"
)

func (c *Core) sign(approvalID, decision, nonce string) string {
	m := hmac.New(sha256.New, c.cfg.CallbackSecret)
	m.Write([]byte(approvalID + "|" + decision + "|" + nonce))
	return hex.EncodeToString(m.Sum(nil))
}

func (c *Core) approvalAttachment(r protocol.ApprovalRequest, nonce string) *model.MessageAttachment {
	action := func(decision, label, style string) *model.PostAction {
		return &model.PostAction{
			Id: decision, Type: model.PostActionTypeButton, Name: label, Style: style,
			Integration: &model.PostActionIntegration{
				URL:     strings.TrimRight(c.cfg.PublicURL, "/") + "/callback/approval",
				Context: map[string]any{ctxApproval: r.ApprovalID, ctxDecision: decision, ctxSig: c.sign(r.ApprovalID, decision, nonce)},
			},
		}
	}
	return &model.MessageAttachment{
		Color:    "#f0ad4e",
		Title:    "Approval needed: " + r.Tool,
		Text:     "```\n" + truncate(r.Summary, 1500) + "\n```\nin `" + r.CWD + "`",
		Fallback: "Approval needed: " + r.Tool + " " + truncate(r.Summary, 100),
		Actions: []*model.PostAction{
			action(protocol.DecisionAllow, "Allow", "primary"),
			action(protocol.DecisionDeny, "Deny", "danger"),
		},
	}
}

// HandleCallback processes a button click. The returned response updates
// the button post in place.
func (c *Core) HandleCallback(ctx context.Context, req *model.PostActionIntegrationRequest) *model.PostActionIntegrationResponse {
	approvalID, _ := req.Context[ctxApproval].(string)
	decision, _ := req.Context[ctxDecision].(string)
	sig, _ := req.Context[ctxSig].(string)
	ephemeral := func(msg string) *model.PostActionIntegrationResponse {
		return &model.PostActionIntegrationResponse{EphemeralText: msg}
	}
	if approvalID == "" || (decision != protocol.DecisionAllow && decision != protocol.DecisionDeny) {
		return ephemeral("Malformed approval action.")
	}
	a, err := c.st.ApprovalByID(ctx, approvalID)
	if err != nil {
		return ephemeral("This approval no longer exists.")
	}
	if !hmac.Equal([]byte(sig), []byte(c.sign(approvalID, decision, a.Nonce))) {
		c.audit(ctx, req.UserId, "approval.bad_signature", a.JobID, map[string]any{"approval": approvalID})
		return ephemeral("Invalid approval signature.")
	}
	j, err := c.st.JobByID(ctx, a.JobID)
	if err != nil {
		return ephemeral("The job for this approval is gone.")
	}
	if req.UserId != j.MMUserID {
		c.audit(ctx, req.UserId, "approval.forbidden", j.ID, map[string]any{"approval": approvalID})
		return ephemeral("Only the job owner can decide this.")
	}
	if j.State.Terminal() {
		return ephemeral("The job already finished (" + string(j.State) + ").")
	}
	now := c.now()
	a, err = c.st.DecideApproval(ctx, approvalID, store.Decision(decision), req.UserId, now)
	if errors.Is(err, store.ErrConflict) {
		return ephemeral("Already decided or expired.")
	}
	if err != nil {
		c.log.Error("decide approval", "err", err)
		return ephemeral("Could not record the decision.")
	}
	env, err := protocol.New(protocol.TypeApprovalResponse, j.ID, protocol.ApprovalResponse{
		ApprovalID: approvalID, Decision: decision, DecidedBy: req.UserName, DecidedAt: now.UnixMilli(),
	})
	if err == nil {
		err = c.hub.SendReliable(ctx, j.HarnessID, env)
	}
	if err != nil {
		c.log.Error("send approval response", "err", err)
	}
	if pending, _ := c.st.PendingApprovalsByJob(ctx, j.ID); len(pending) == 0 {
		if _, err := c.st.TransitionJob(ctx, j.ID, []store.JobState{store.JobAwaitingApproval}, store.JobRunning, store.JobPatch{}); err == nil {
			c.edit(j, "▶️ Running…")
		}
	}
	c.audit(ctx, req.UserId, "approval.decided", j.ID, map[string]any{"approval": approvalID, "decision": decision, "tool": a.Tool})

	icon := "✅ Allowed"
	if decision == protocol.DecisionDeny {
		icon = "⛔ Denied"
	}
	update := &model.Post{Id: req.PostId, Message: fmt.Sprintf("%s by @%s: **%s** `%s`", icon, req.UserName, a.Tool, truncate(a.Summary, 200))}
	update.AddProp(model.PostPropsAttachments, []*model.MessageAttachment{})
	return &model.PostActionIntegrationResponse{Update: update}
}

// --- pairing ---------------------------------------------------------------

func (c *Core) newPairingCode(ctx context.Context, userID string) (string, error) {
	code := randomCode(8)
	return code, c.st.CreatePairing(ctx, hashCode(code), userID, c.now().Add(10*time.Minute))
}

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

var ErrBadPairingCode = errors.New("invalid or expired pairing code")

func (c *Core) HandlePair(ctx context.Context, req PairRequest) (PairResponse, error) {
	userID, err := c.st.ConsumePairing(ctx, hashCode(req.Code), c.now())
	if errors.Is(err, store.ErrNotFound) {
		return PairResponse{}, ErrBadPairingCode
	}
	if err != nil {
		return PairResponse{}, err
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = req.Hostname
	}
	if name == "" {
		name = "harness"
	}
	token := "hrt_" + randomToken(32)
	h := store.Harness{ID: protocol.NewID("hrn"), MMUserID: userID, Name: name, TokenHash: tokenHash(token), Version: req.Version, CreatedAt: c.now()}
	if err := c.st.CreateHarness(ctx, h); err != nil {
		return PairResponse{}, err
	}
	username := ""
	if u, err := c.mm.GetUser(ctx, userID); err == nil {
		username = u.Username
		if ch, err := c.mm.DirectChannel(ctx, c.cfg.BotUserID, userID); err == nil {
			_, _ = c.mm.CreatePost(ctx, &model.Post{ChannelId: ch, Message: fmt.Sprintf("Paired harness `%s` (%s). Mention @%s in any channel to use it.", name, h.ID, c.cfg.BotUsername)})
		}
	}
	c.audit(ctx, userID, "harness.paired", "", map[string]any{"harness": h.ID, "name": name})
	return PairResponse{HarnessID: h.ID, Token: token, MMUserID: userID, Username: username}, nil
}

// --- sweeper ---------------------------------------------------------------

// Sweep runs the time-based transitions: queue expiry, lost harnesses, job
// timeouts, and deletes acked outbox rows. Call it periodically.
func (c *Core) Sweep(ctx context.Context) {
	now := c.now()
	if n, err := c.st.PurgeOutbox(ctx, now.Add(-c.cfg.OutboxRetention)); err != nil {
		c.log.Error("purge outbox", "err", err)
	} else if n > 0 {
		c.log.Debug("purged acked outbox messages", "count", n)
	}
	expired, err := c.st.ExpireQueuedJobs(ctx, now)
	if err != nil {
		c.log.Error("expire queued", "err", err)
	}
	for _, j := range expired {
		c.finalEdit(ctx, j, "💤 Expired: the harness did not come online within "+c.cfg.QueueTTL.String()+".")
	}

	jobs, _ := c.st.ListJobs(ctx, store.JobFilter{States: []store.JobState{store.JobDispatched, store.JobRunning, store.JobAwaitingApproval}, Limit: 1000})
	for _, j := range jobs {
		if c.hub.Online(j.HarnessID) {
			if now.Sub(j.CreatedAt) > c.cfg.JobTimeout+time.Minute && j.State != store.JobAwaitingApproval {
				_ = c.cancelJob(ctx, j, "timeout")
			}
			continue
		}
		h, err := c.st.HarnessByID(ctx, j.HarnessID)
		if err != nil {
			continue
		}
		lastSeen := h.CreatedAt
		if h.LastSeenAt != nil {
			lastSeen = *h.LastSeenAt
		}
		if now.Sub(lastSeen) > c.cfg.GracePeriod {
			msg := "lost: harness offline for more than " + c.cfg.GracePeriod.String()
			if _, err := c.st.TransitionJob(ctx, j.ID, store.ActiveStates, store.JobLost, store.JobPatch{Error: &msg}); err == nil {
				c.finalEdit(ctx, j, "📡 Lost contact with `"+h.Name+"`. If it comes back and finishes, the result still lands here.")
			}
		}
	}
}

// --- posts -----------------------------------------------------------------

func (c *Core) reply(ctx context.Context, to *model.Post, msg string) {
	c.replyAs(ctx, "", to, msg)
}

func (c *Core) replyAs(ctx context.Context, botUserID string, to *model.Post, msg string) {
	if _, err := c.api(ctx, botUserID).CreatePost(ctx, &model.Post{ChannelId: to.ChannelId, RootId: rootOf(to), Message: msg}); err != nil {
		c.log.Error("reply", "err", err)
	}
}

func (c *Core) fail(ctx context.Context, j store.Job, reason string) {
	if _, err := c.st.TransitionJob(ctx, j.ID, store.ActiveStates, store.JobFailed, store.JobPatch{Error: &reason}); err != nil {
		return
	}
	c.finalEdit(ctx, j, "❌ "+reason)
}

func (c *Core) postResult(ctx context.Context, j store.Job, h store.Harness, r protocol.JobResult) {
	var head string
	switch r.Status {
	case protocol.StatusSucceeded:
		head = "✅ Done"
	case protocol.StatusCancelled:
		head = "🚫 Cancelled"
	default:
		head = "❌ Failed"
		if r.Error != nil {
			head += " (" + r.Error.Code + ")"
		}
	}
	meta := fmt.Sprintf("%s on `%s` · %d turns · %s", head, h.Name, r.Usage.Turns, (time.Duration(r.Usage.DurationMS) * time.Millisecond).Round(time.Second))
	if r.Usage.CostUSD > 0 {
		meta += fmt.Sprintf(" · $%.2f", r.Usage.CostUSD)
	}
	body := r.Text
	if r.Error != nil && strings.TrimSpace(body) == "" {
		body = r.Error.Message
	}
	msg := meta + "\n\n" + body
	api := c.api(ctx, j.BotUserID)
	var fileIDs []string
	if runeLen(msg) > mattermost.MaxMessageLen-200 {
		if fi, err := api.UploadFile(ctx, j.ChannelID, "result.md", []byte(body)); err == nil {
			fileIDs = append(fileIDs, fi.Id)
		}
		msg = meta + "\n\n" + truncate(body, mattermost.MaxMessageLen-runeLen(meta)-300) + "\n\n_(full output attached)_"
	}
	for _, f := range r.Files {
		data, err := base64.StdEncoding.DecodeString(f.DataB64)
		if err != nil || len(data) > protocol.MaxFileBytes {
			continue
		}
		if fi, err := api.UploadFile(ctx, j.ChannelID, f.Name, data); err == nil {
			fileIDs = append(fileIDs, fi.Id)
		}
	}
	c.finalEdit(ctx, j, msg)
	if len(fileIDs) > 0 {
		if _, err := api.CreatePost(ctx, &model.Post{ChannelId: j.ChannelID, RootId: j.RootPostID, Message: "📎 Attachments", FileIds: fileIDs}); err != nil {
			c.log.Error("attachment post", "err", err)
		}
	}
}

// editor debounces edits of one status post: latest text wins, at most one
// edit per EditInterval.
type editor struct {
	mu      sync.Mutex
	pending string
	timer   *time.Timer
	last    time.Time
}

// edit schedules a debounced update of the job's status post.
func (c *Core) edit(j store.Job, text string) {
	if j.StatusPostID == "" {
		return
	}
	c.mu.Lock()
	e := c.edits[j.ID]
	if e == nil {
		e = &editor{}
		c.edits[j.ID] = e
	}
	c.mu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = text
	if e.timer != nil {
		return // an edit is already scheduled; it will pick up the newest text
	}
	delay := c.cfg.EditInterval - time.Since(e.last)
	if delay < 0 {
		delay = 0
	}
	e.timer = time.AfterFunc(delay, func() {
		e.mu.Lock()
		text := e.pending
		e.timer = nil
		e.last = time.Now()
		e.mu.Unlock()
		if err := c.api(context.Background(), j.BotUserID).UpdatePost(context.Background(), j.StatusPostID, text, nil); err != nil {
			c.log.Warn("edit status post", "job", j.ID, "err", err)
		}
	})
}

// finalEdit writes immediately and drops the debouncer.
func (c *Core) finalEdit(ctx context.Context, j store.Job, text string) {
	c.mu.Lock()
	e := c.edits[j.ID]
	delete(c.edits, j.ID)
	c.mu.Unlock()
	if e != nil {
		e.mu.Lock()
		if e.timer != nil {
			e.timer.Stop()
			e.timer = nil
		}
		e.mu.Unlock()
	}
	if j.StatusPostID == "" {
		return
	}
	if err := c.api(ctx, j.BotUserID).UpdatePost(ctx, j.StatusPostID, text, nil); err != nil {
		c.log.Warn("final edit", "job", j.ID, "err", err)
	}
}

func (c *Core) statusText(ctx context.Context, userID string) string {
	list, _ := c.st.HarnessesByUser(ctx, userID)
	if len(list) == 0 {
		return "No paired harness. Send `pair` to get a code."
	}
	var b strings.Builder
	for _, h := range list {
		state := "offline"
		if c.hub.Online(h.ID) {
			state = "online"
		}
		seen := "never"
		if h.LastSeenAt != nil {
			seen = h.LastSeenAt.Local().Format(time.RFC822)
		}
		fmt.Fprintf(&b, "- `%s` (%s) %s, last seen %s, v%s\n", h.Name, h.ID, state, seen, h.Version)
	}
	jobs, _ := c.st.ListJobs(ctx, store.JobFilter{MMUserID: userID, States: store.ActiveStates, Limit: 10})
	if len(jobs) > 0 {
		b.WriteString("\nActive jobs:\n")
		for _, j := range jobs {
			fmt.Fprintf(&b, "- %s: %s since %s\n", j.ID, j.State, j.CreatedAt.Local().Format(time.Kitchen))
		}
	}
	return b.String()
}

func (c *Core) audit(ctx context.Context, actor, action, jobID string, details map[string]any) {
	var raw json.RawMessage
	if details != nil {
		raw, _ = json.Marshal(details)
	}
	if err := c.st.Append(ctx, store.AuditEntry{At: c.now(), Actor: actor, Action: action, JobID: jobID, Details: raw}); err != nil {
		c.log.Error("audit", "err", err)
	}
}

// --- helpers ---------------------------------------------------------------

func rootOf(p *model.Post) string {
	if p.RootId != "" {
		return p.RootId
	}
	return p.Id
}

func stripMention(msg, bot string) string {
	return strings.TrimSpace(strings.ReplaceAll(msg, "@"+bot, ""))
}

// splitWorkspace pulls the first "ws:<name>" token out of the prompt,
// wherever it stands: people write the mention first, last, or on its own
// line, and the token has to survive all of that.
func splitWorkspace(text string) (string, string) {
	fields := strings.Fields(text)
	for i, f := range fields {
		f = strings.Trim(f, "`\"'.,;:")
		if !strings.HasPrefix(f, "ws:") || len(f) == 3 {
			continue
		}
		name := f[3:]
		rest := append(append([]string{}, fields[:i]...), fields[i+1:]...)
		return name, strings.Join(rest, " ")
	}
	return "", strings.TrimSpace(text)
}

// splitAgent pulls the first "agent:<name>" token out of the prompt.
func splitAgent(text string) (string, string) {
	fields := strings.Fields(text)
	for i, f := range fields {
		f = strings.Trim(f, "`\"'.,;:")
		if !strings.HasPrefix(f, "agent:") || len(f) == len("agent:") {
			continue
		}
		rest := append(append([]string{}, fields[:i]...), fields[i+1:]...)
		return strings.ToLower(f[len("agent:"):]), strings.Join(rest, " ")
	}
	return "", strings.TrimSpace(text)
}

func renderProgress(harnessName string, p protocol.JobProgress) string {
	head := fmt.Sprintf("▶️ Running on `%s` · %d turns", harnessName, p.Turns)
	if p.Phase == protocol.PhaseAwaitingApproval {
		head = fmt.Sprintf("⏸ Waiting for approval on `%s` · %d turns", harnessName, p.Turns)
	}
	if p.CurrentTool != "" {
		head += " · " + p.CurrentTool
	}
	text := strings.TrimSpace(p.Text)
	if text == "" {
		return head
	}
	return head + "\n\n" + truncate(text, mattermost.MaxMessageLen-runeLen(head)-100)
}

func nackText(a protocol.Ack) string {
	if a.Message != "" {
		return a.Code + ": " + a.Message
	}
	return a.Code
}

// runeLen counts characters the way Mattermost counts them; every budget
// around MaxMessageLen is in runes, not bytes.
func runeLen(s string) int { return utf8.RuneCountInString(s) }

func truncate(s string, n int) string {
	if n < 0 {
		n = 0
	}
	if runeLen(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

func helpText(bot string) string {
	return "Mention me with a task in any channel or thread, e.g. `@" + bot + " ws:infra bump the mattermost provider`.\n" +
		"- `ws:<name>` picks a workspace configured on your harness; without it the thread's previous workspace or the harness's default folder is used.\n" +
		"- `agent:claude` or `agent:codex` picks the coding agent; without it the thread's previous agent or the harness's default is used.\n" +
		"- Reply in the same thread to continue the conversation.\n" +
		"- `@" + bot + " cancel` stops the running job in a thread.\n" +
		"- Onboarding: `mhb harness init` on your machine, then `/harness init <code>` here; `/harness join` brings your bot into a channel, `/harness status` lists your harnesses."
}

func hashCode(code string) string {
	sum := sha256.Sum256([]byte(strings.ToUpper(strings.TrimSpace(code))))
	return hex.EncodeToString(sum[:])
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// randomCode is short and unambiguous enough to type from a phone screen.
func randomCode(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	s := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)
	return s[:n]
}
