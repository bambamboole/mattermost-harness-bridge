// Package store is the broker's persistence boundary. SQLite is the first
// implementation; the interface avoids transactions and exposes
// compare-and-set methods instead so a Postgres port stays mechanical.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("store: not found")
	// ErrConflict: a CAS precondition failed, an ID already exists, or an
	// approval was already decided.
	ErrConflict = errors.New("store: conflict")
)

type JobState string

const (
	JobQueued           JobState = "queued"     // harness offline, waits until ExpiresAt
	JobDispatched       JobState = "dispatched" // sent, no ack yet
	JobRunning          JobState = "running"
	JobAwaitingApproval JobState = "awaiting_approval"
	JobSucceeded        JobState = "succeeded"
	JobFailed           JobState = "failed"
	JobCancelled        JobState = "cancelled"
	JobLost             JobState = "lost"    // harness gone beyond the grace period
	JobExpired          JobState = "expired" // queue TTL ran out
)

func (s JobState) Terminal() bool {
	switch s {
	case JobSucceeded, JobFailed, JobCancelled, JobLost, JobExpired:
		return true
	}
	return false
}

// ActiveStates lists every non-terminal state.
var ActiveStates = []JobState{JobQueued, JobDispatched, JobRunning, JobAwaitingApproval}

type Harness struct {
	ID         string
	MMUserID   string
	Name       string
	TokenHash  string // sha256 of the bearer token
	Version    string
	CreatedAt  time.Time
	LastSeenAt *time.Time
}

type Job struct {
	ID            string
	HarnessID     string
	MMUserID      string
	ChannelID     string
	RootPostID    string
	TriggerPostID string
	StatusPostID  string // the bot's reply, edited with progress
	BotUserID     string // the bot that posts for this job; "" is the shared bot
	Workspace     string
	Prompt        string
	State         JobState
	LastSeq       int64
	ResultText    string
	Error         string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	ExpiresAt     *time.Time // only while queued
	FinishedAt    *time.Time
}

// JobPatch: nil fields stay untouched.
type JobPatch struct {
	StatusPostID *string
	ResultText   *string
	Error        *string
}

type Decision string

const (
	Allow Decision = "allow"
	Deny  Decision = "deny"
)

type Approval struct {
	ID          string
	JobID       string
	Tool        string
	Summary     string
	Input       json.RawMessage
	Nonce       string // basis for the HMAC in the button context
	RequestedAt time.Time
	ExpiresAt   time.Time
	Decision    Decision // "" while pending
	DecidedBy   string
	DecidedAt   *time.Time
}

func (a Approval) Pending() bool { return a.Decision == "" }

type OutboxMessage struct {
	ID        string // envelope ID, reused on resend
	HarnessID string
	JobID     string
	Type      string
	Payload   json.RawMessage
	CreatedAt time.Time
	AckedAt   *time.Time
}

// Bot is a user's own bot account, created by /harness init.
type Bot struct {
	UserID         string // the bot's Mattermost user id
	MMUserID       string // the owner
	Username       string
	Token          string // posts as the bot
	CreatedAt      time.Time
	HarnessID      string // exactly one local harness; an owner may have many bots
	TeamID         string
	ChannelID      string // default incoming webhook channel
	IncomingHookID string
	OutgoingHookID string
	OutgoingToken  string
}

// Installation stores an OAuth-authorized team and its managed slash command.
// Credentials is encrypted by the OAuth service before persistence.
type Installation struct {
	TeamID       string
	UserID       string
	Credentials  string
	CommandID    string
	CommandToken string
}

// InitRequest is one device-flow onboarding: created by the laptop with a
// code, claimed by the owner in Mattermost, fetched once by the laptop.
type InitRequest struct {
	CodeHash      string
	BotName       string // requested bot username, "" for the default
	HarnessName   string
	CreatedAt     time.Time
	ExpiresAt     time.Time
	ClaimedAt     *time.Time
	HarnessID     string
	HarnessToken  string // plaintext until fetched
	FetchedAt     *time.Time
	BotUserID     string
	PollTokenHash string
}

func (r InitRequest) Claimed() bool { return r.ClaimedAt != nil }

type AuditEntry struct {
	At      time.Time
	Actor   string // mm_user_id, harness_id or "system"
	Action  string // job.created, approval.decided, harness.paired, ...
	JobID   string
	Details json.RawMessage
}

type JobFilter struct {
	BotUserID     string
	TriggerPostID string
	MMUserID      string
	HarnessID     string
	RootPostID    string
	States        []JobState
	Limit         int
}

type HarnessStore interface {
	CreateHarness(ctx context.Context, h Harness) error
	HarnessByID(ctx context.Context, id string) (Harness, error)
	HarnessByTokenHash(ctx context.Context, hash string) (Harness, error)
	HarnessesByUser(ctx context.Context, mmUserID string) ([]Harness, error)
	TouchHarness(ctx context.Context, id string, seenAt time.Time, version string) error
	DeleteHarness(ctx context.Context, id string) error
}

type PairingStore interface {
	CreatePairing(ctx context.Context, codeHash, mmUserID string, expiresAt time.Time) error
	// ConsumePairing is atomic. ErrNotFound if the code is unknown, used or expired.
	ConsumePairing(ctx context.Context, codeHash string, now time.Time) (mmUserID string, err error)
}

type JobStore interface {
	CreateJob(ctx context.Context, j Job) error
	JobByID(ctx context.Context, id string) (Job, error)
	ListJobs(ctx context.Context, f JobFilter) ([]Job, error)
	// ActiveJobsByHarness returns every non-terminal job; basis for welcome.jobs.
	ActiveJobsByHarness(ctx context.Context, harnessID string) ([]Job, error)
	// TransitionJob is CAS: ErrConflict unless the job is in one of the from states.
	TransitionJob(ctx context.Context, id string, from []JobState, to JobState, patch JobPatch) (Job, error)
	// RecordProgress stores seq only if it is greater than last_seq.
	RecordProgress(ctx context.Context, id string, seq int64) (applied bool, err error)
	// ExpireQueuedJobs moves queued -> expired and returns the affected jobs.
	ExpireQueuedJobs(ctx context.Context, now time.Time) ([]Job, error)
}

type ApprovalStore interface {
	// CreateApproval: ErrConflict on a duplicate ID; the caller treats that as a resend.
	CreateApproval(ctx context.Context, a Approval) error
	ApprovalByID(ctx context.Context, id string) (Approval, error)
	PendingApprovalsByJob(ctx context.Context, jobID string) ([]Approval, error)
	// DecideApproval is CAS: ErrConflict if already decided or expired.
	DecideApproval(ctx context.Context, id string, d Decision, by string, at time.Time) (Approval, error)
}

type OutboxStore interface {
	Enqueue(ctx context.Context, m OutboxMessage) error
	PendingOutbox(ctx context.Context, harnessID string) ([]OutboxMessage, error)
	AckOutbox(ctx context.Context, msgID string, at time.Time) error
	PurgeOutbox(ctx context.Context, ackedBefore time.Time) (int64, error)
}

type BotStore interface {
	// CreateBot: ErrConflict when the owner already has a bot or the username is taken.
	CreateBot(ctx context.Context, b Bot) error
	BotByOwner(ctx context.Context, mmUserID string) (Bot, error)
	BotByUserID(ctx context.Context, botUserID string) (Bot, error)
	ListBots(ctx context.Context) ([]Bot, error)
	BotsByOwner(ctx context.Context, mmUserID string) ([]Bot, error)
	BotByUsername(ctx context.Context, username string) (Bot, error)
	UpdateBot(ctx context.Context, b Bot) error
	DeleteBot(ctx context.Context, botUserID string) error
}

type InstallationStore interface {
	SaveInstallation(ctx context.Context, i Installation) error
	InstallationByTeam(ctx context.Context, teamID string) (Installation, error)
	ListInstallations(ctx context.Context) ([]Installation, error)
}

type InitStore interface {
	CreateInit(ctx context.Context, r InitRequest) error
	// InitByCode reads a request without side effects. ErrNotFound if unknown.
	InitByCode(ctx context.Context, codeHash string) (InitRequest, error)
	// ClaimInit binds the code to a harness. ErrNotFound if unknown or
	// expired, ErrConflict if already claimed.
	ClaimInit(ctx context.Context, codeHash string, now time.Time, harnessID, harnessToken string) error
	// FetchInit returns the request; a claimed one is marked fetched and its
	// token is handed out exactly once (ErrConflict afterwards). ErrNotFound
	// if unknown or expired.
	FetchInit(ctx context.Context, codeHash string, now time.Time) (InitRequest, error)
	InitByHarness(ctx context.Context, harnessID string) (InitRequest, error)
	// CompleteInit atomically creates the harness, binds its bot and consumes the code.
	CompleteInit(ctx context.Context, codeHash string, now time.Time, h Harness, botUserID, token string) error
}

type AuditStore interface {
	Append(ctx context.Context, e AuditEntry) error
}

type Store interface {
	HarnessStore
	PairingStore
	JobStore
	ApprovalStore
	OutboxStore
	BotStore
	InitStore
	AuditStore
	InstallationStore
	Close() error
}
