// Package agent abstracts the coding agent a job runs on. The harness only
// knows this interface; claude/ and codex/ implement it.
package agent

import (
	"context"
	"errors"
	"fmt"
)

const (
	Claude = "claude"
	Codex  = "codex"
)

// Options describe one job run.
type Options struct {
	Dir      string
	Prompt   string
	ResumeID string // agent session to continue, "" for a fresh one
	MaxTurns int
	Model    string
	// SystemPrompt is appended to the agent's own instructions where the
	// agent supports it; otherwise it is prepended to the prompt.
	SystemPrompt string

	// Permission bridge, used by agents that can ask (Claude): the MCP
	// config that spawns the harness's permission server and the tool name.
	PermissionMCPConfigPath string
	PermissionTool          string
	AllowedTools            []string
	DisallowedTools         []string
}

// Event is a progress snapshot: Text is the accumulated output so far.
type Event struct {
	SessionID   string
	Text        string
	CurrentTool string
	Turns       int
}

type Result struct {
	Status     string // succeeded | failed | cancelled
	Text       string
	SessionID  string
	Turns      int
	CostUSD    float64
	DurationMS int64
	ErrCode    string
	ErrMessage string
}

const (
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

type Agent interface {
	Name() string
	// Approvals reports whether tool calls go through the permission bridge.
	// Agents without it run inside their own sandbox and never ask.
	Approvals() bool
	// Check verifies the agent's binary is runnable.
	Check() error
	// Run blocks until the agent finished; cancelling ctx stops it.
	Run(ctx context.Context, opt Options, onEvent func(Event)) (Result, error)
}

var ErrUnknown = errors.New("agent: unknown agent")

// Registry maps agent names to configured instances.
type Registry map[string]Agent

func (r Registry) Get(name string) (Agent, error) {
	a, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknown, name)
	}
	return a, nil
}

func (r Registry) Names() []string {
	names := make([]string, 0, len(r))
	for n := range r {
		names = append(names, n)
	}
	return names
}
