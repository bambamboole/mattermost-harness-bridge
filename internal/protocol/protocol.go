// Package protocol defines the wire protocol between the broker and the
// harnesses. Both binaries import it; nothing here may depend on either side.
//
// Transport: WebSocket on /harness/v1, one JSON envelope per text frame.
// Breaking changes bump the path. Additive changes stay within v1; unknown
// message types are answered with an error and otherwise ignored.
package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

// Path is the WebSocket endpoint the harness connects to.
const Path = "/harness/v1"

// Message types, harness -> broker.
const (
	TypeHello           = "hello"
	TypePing            = "ping"
	TypeJobProgress     = "job.progress"
	TypeJobResult       = "job.result"
	TypeApprovalRequest = "approval.request"
)

// Message types, broker -> harness.
const (
	TypeWelcome          = "welcome"
	TypePong             = "pong"
	TypeJobDispatch      = "job.dispatch"
	TypeJobCancel        = "job.cancel"
	TypeApprovalResponse = "approval.response"
)

// Message types in both directions.
const (
	TypeAck   = "ack"
	TypeError = "error"
)

// NeedsAck reports whether a message type is delivered at-least-once and must
// be answered with an Ack. The sender keeps such messages in an outbox until
// the ack arrives and resends them after a reconnect.
func NeedsAck(typ string) bool {
	switch typ {
	case TypeJobDispatch, TypeJobCancel, TypeApprovalResponse,
		TypeJobResult, TypeApprovalRequest:
		return true
	}
	return false
}

// Close codes on the WebSocket connection.
const (
	CloseReplaced        = 4001 // a newer connection for the same harness won
	CloseUnauthorized    = 4003
	CloseVersionTooOld   = 4004
	CloseProtocolViolate = 4005
)

// Envelope wraps every message.
type Envelope struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	JobID   string          `json:"job_id,omitempty"`
	Ref     string          `json:"ref,omitempty"`
	TS      int64           `json:"ts"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// New builds an envelope with a fresh ID and the current timestamp.
func New(typ, jobID string, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("protocol: marshal %s: %w", typ, err)
	}
	return Envelope{
		ID:      NewID("msg"),
		Type:    typ,
		JobID:   jobID,
		TS:      time.Now().UnixMilli(),
		Payload: raw,
	}, nil
}

// Reply builds an envelope that refers to another one.
func Reply(typ string, to Envelope, payload any) (Envelope, error) {
	env, err := New(typ, to.JobID, payload)
	if err != nil {
		return Envelope{}, err
	}
	env.Ref = to.ID
	return env, nil
}

// Decode unmarshals the payload into v.
func (e Envelope) Decode(v any) error {
	if err := json.Unmarshal(e.Payload, v); err != nil {
		return fmt.Errorf("protocol: decode %s payload: %w", e.Type, err)
	}
	return nil
}

// Ack answers an at-least-once message. OK=false is a nack; Code then names
// the reason.
type Ack struct {
	OK      bool   `json:"ok"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Nack codes.
const (
	NackUnknownJob       = "unknown_job"
	NackWorkspaceUnknown = "workspace_unknown"
	NackBusy             = "busy"
	NackBadPayload       = "bad_payload"
	NackForbidden        = "forbidden"
	NackAgentUnknown     = "agent_unknown"
)

// Error reports a protocol-level problem with a previous message (Ref).
// Job-level failures travel in JobResult instead.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

const (
	ErrUnknownType = "unknown_type"
	ErrBadPayload  = "bad_payload"
)

// Hello is the first message after the upgrade. Jobs lists what the harness
// is still running so the broker can decide per job in Welcome.
type Hello struct {
	HarnessVersion string     `json:"harness_version"`
	Hostname       string     `json:"hostname"`
	Workspaces     []string   `json:"workspaces"`
	MaxJobs        int        `json:"max_jobs"`
	Jobs           []HelloJob `json:"jobs,omitempty"`
}

type HelloJob struct {
	JobID            string   `json:"job_id"`
	State            string   `json:"state"`
	LastSeq          int64    `json:"last_seq"`
	PendingApprovals []string `json:"pending_approvals,omitempty"`
}

// Welcome answers Hello.
type Welcome struct {
	HarnessID           string       `json:"harness_id"`
	HeartbeatIntervalMS int          `json:"heartbeat_interval_ms"`
	ServerTime          int64        `json:"server_time"`
	Jobs                []WelcomeJob `json:"jobs,omitempty"`
}

type WelcomeJob struct {
	JobID  string `json:"job_id"`
	Action string `json:"action"` // resume | abort
	Reason string `json:"reason,omitempty"`
}

const (
	ActionResume = "resume"
	ActionAbort  = "abort"
)

type Ping struct {
	Jobs int `json:"jobs"`
}

type Pong struct {
	ServerTime int64 `json:"server_time"`
}

// File carries a small attachment inline. Larger artefacts are out of scope
// for v1; the broker enforces MaxFileBytes.
type File struct {
	Name    string `json:"name"`
	Mime    string `json:"mime"`
	DataB64 string `json:"data_b64"`
}

const MaxFileBytes = 8 << 20

type Thread struct {
	ChannelID     string `json:"channel_id"`
	RootPostID    string `json:"root_post_id"`
	TriggerPostID string `json:"trigger_post_id"`
}

type Requester struct {
	MMUserID string `json:"mm_user_id"`
	Username string `json:"username"`
}

type Limits struct {
	MaxTurns  int `json:"max_turns,omitempty"`
	TimeoutMS int `json:"timeout_ms,omitempty"`
}

// HistoryPost is one earlier post of the thread a job was started in.
type HistoryPost struct {
	Username string `json:"username"`
	At       int64  `json:"at"` // unix ms
	Text     string `json:"text"`
}

// JobDispatch asks the harness to run a job. The harness verifies that
// Requester matches its owner and nacks with NackForbidden otherwise.
//
// Agent names the coding agent ("claude", "codex"); empty means the
// harness's default. History carries the thread's earlier posts; the
// harness uses it only when it has no session for the thread yet.
type JobDispatch struct {
	Workspace   string        `json:"workspace"`
	Agent       string        `json:"agent,omitempty"`
	Prompt      string        `json:"prompt"`
	Thread      Thread        `json:"thread"`
	Requester   Requester     `json:"requester"`
	Attachments []File        `json:"attachments,omitempty"`
	History     []HistoryPost `json:"history,omitempty"`
	Limits      Limits        `json:"limits"`
}

type JobCancel struct {
	Reason string `json:"reason"` // user | timeout | harness_replaced
}

// JobProgress is a snapshot, not a delta: Text is what the status post
// should show right now. Higher Seq wins; gaps are harmless.
type JobProgress struct {
	Seq         int64  `json:"seq"`
	Phase       string `json:"phase"` // starting | running | awaiting_approval
	Text        string `json:"text"`
	CurrentTool string `json:"current_tool,omitempty"`
	Turns       int    `json:"turns"`
}

const (
	PhaseStarting         = "starting"
	PhaseRunning          = "running"
	PhaseAwaitingApproval = "awaiting_approval"
)

type JobResult struct {
	Status string    `json:"status"` // succeeded | failed | cancelled
	Text   string    `json:"text"`
	Error  *JobError `json:"error,omitempty"`
	Files  []File    `json:"files,omitempty"`
	Usage  Usage     `json:"usage"`
}

const (
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

type JobError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Usage struct {
	Turns      int     `json:"turns"`
	DurationMS int64   `json:"duration_ms"`
	CostUSD    float64 `json:"cost_usd,omitempty"`
}

// ApprovalRequest asks the owner to allow a tool call. Summary is what the
// button post shows; Input is the raw tool input, truncated for audit.
type ApprovalRequest struct {
	ApprovalID string          `json:"approval_id"`
	Tool       string          `json:"tool"`
	Summary    string          `json:"summary"`
	Input      json.RawMessage `json:"input"`
	CWD        string          `json:"cwd"`
	ExpiresAt  int64           `json:"expires_at"`
}

type ApprovalResponse struct {
	ApprovalID string `json:"approval_id"`
	Decision   string `json:"decision"` // allow | deny
	DecidedBy  string `json:"decided_by"`
	DecidedAt  int64  `json:"decided_at"`
}

const (
	DecisionAllow = "allow"
	DecisionDeny  = "deny"
)
