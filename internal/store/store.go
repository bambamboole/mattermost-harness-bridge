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

type AuditEntry struct {
	At      time.Time
	Actor   string // mm_user_id, harness_id or "system"
	Action  string // job.created, approval.decided, harness.paired, ...
	JobID   string
	Details json.RawMessage
}

type JobFilter struct {
	MMUserID   string
	HarnessID  string
	RootPostID string
	States     []JobState
	Limit      int
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

type AuditStore interface {
	Append(ctx context.Context, e AuditEntry) error
}

type Store interface {
	HarnessStore
	PairingStore
	JobStore
	ApprovalStore
	OutboxStore
	AuditStore
	Close() error
}
