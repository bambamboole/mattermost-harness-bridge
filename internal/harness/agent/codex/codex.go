// Package codex drives one `codex exec --json` subprocess per job. Codex has
// no permission prompt in exec mode, so it runs inside its own sandbox
// (workspace-write by default) and never asks the owner.
//
// Event shapes verified against codex-cli 0.153.4:
//
//	{"type":"thread.started","thread_id":"…"}
//	{"type":"item.started","item":{"type":"command_execution","command":"…","status":"in_progress"}}
//	{"type":"item.completed","item":{"type":"agent_message","text":"…"}}
//	{"type":"item.completed","item":{"type":"error","message":"…"}}
//	{"type":"turn.completed","usage":{"input_tokens":…,"output_tokens":…}}
package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/agent"
)

const (
	SandboxReadOnly       = "read-only"
	SandboxWorkspaceWrite = "workspace-write"
)

type Agent struct {
	Bin     string
	Sandbox string
	// KillGrace is how long to wait after SIGINT before SIGKILL.
	KillGrace time.Duration
}

func New(bin, sandbox string) *Agent {
	if bin == "" {
		bin = "codex"
	}
	if sandbox == "" {
		sandbox = SandboxWorkspaceWrite
	}
	return &Agent{Bin: bin, Sandbox: sandbox}
}

var ErrNoCodex = errors.New("codex: binary not found")

func (a *Agent) Name() string    { return agent.Codex }
func (a *Agent) Approvals() bool { return false }

func (a *Agent) Check() error {
	if _, err := exec.LookPath(a.Bin); err != nil {
		return fmt.Errorf("%w: %v", ErrNoCodex, err)
	}
	return nil
}

func (a *Agent) Run(ctx context.Context, opt agent.Options, onEvent func(agent.Event)) (agent.Result, error) {
	prompt := opt.Prompt
	if opt.SystemPrompt != "" {
		// exec has no system prompt flag; the instructions lead the prompt.
		prompt = opt.SystemPrompt + "\n\n" + prompt
	}
	args := []string{"exec"}
	if opt.ResumeID != "" {
		args = append(args, "resume", opt.ResumeID)
	}
	args = append(args, "--json", "--skip-git-repo-check", "--color", "never", "-C", opt.Dir, "-s", a.Sandbox)
	if opt.Model != "" {
		args = append(args, "-m", opt.Model)
	}
	args = append(args, prompt)

	cmd := exec.Command(a.Bin, args...)
	cmd.Dir = opt.Dir
	cmd.Env = append(os.Environ(), "MHB_JOB=1")
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return agent.Result{}, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &limitedWriter{w: &stderr, n: 64 << 10}
	if err := cmd.Start(); err != nil {
		return agent.Result{}, fmt.Errorf("codex: start %s: %w", a.Bin, err)
	}

	killed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			grace := a.KillGrace
			if grace <= 0 {
				grace = 5 * time.Second
			}
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
			select {
			case <-killed:
			case <-time.After(grace):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		case <-killed:
		}
	}()

	start := time.Now()
	st := parse(stdout, onEvent)
	waitErr := cmd.Wait()
	close(killed)

	res := agent.Result{
		SessionID:  st.threadID,
		Text:       st.finalText(),
		Turns:      st.turns,
		DurationMS: time.Since(start).Milliseconds(),
	}
	switch {
	case ctx.Err() != nil:
		res.Status = agent.StatusCancelled
		res.ErrCode = "cancelled"
		res.ErrMessage = ctx.Err().Error()
	case st.failed != "":
		res.Status = agent.StatusFailed
		res.ErrCode = "turn_failed"
		res.ErrMessage = st.failed
	case st.completed && waitErr == nil:
		res.Status = agent.StatusSucceeded
	default:
		res.Status = agent.StatusFailed
		res.ErrCode = "codex_exit"
		msg := strings.TrimSpace(stderr.String())
		if msg == "" && len(st.errors) > 0 {
			msg = strings.Join(st.errors, "; ")
		}
		if msg == "" && waitErr != nil {
			msg = waitErr.Error()
		}
		if msg == "" {
			msg = "codex exited without completing the turn"
		}
		res.ErrMessage = msg
	}
	return res, nil
}

type streamState struct {
	threadID  string
	messages  []string
	tool      string
	turns     int
	completed bool
	failed    string
	errors    []string
}

func (s *streamState) snapshot() string {
	return strings.TrimSpace(strings.Join(s.messages, "\n\n"))
}

func (s *streamState) finalText() string {
	if len(s.messages) > 0 {
		return strings.TrimSpace(s.messages[len(s.messages)-1])
	}
	return ""
}

type line struct {
	Type     string `json:"type"`
	ThreadID string `json:"thread_id"`
	Item     struct {
		Type    string `json:"type"`
		Text    string `json:"text"`
		Command string `json:"command"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"item"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

func parse(r io.Reader, onEvent func(agent.Event)) *streamState {
	st := &streamState{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 32<<20)
	emit := func() {
		if onEvent != nil {
			onEvent(agent.Event{SessionID: st.threadID, Text: st.snapshot(), CurrentTool: st.tool, Turns: st.turns})
		}
	}
	for sc.Scan() {
		var l line
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			continue
		}
		switch l.Type {
		case "thread.started":
			st.threadID = l.ThreadID
			emit()
		case "item.started":
			if l.Item.Type == "command_execution" {
				st.tool = "shell"
				st.turns++
				emit()
			}
		case "item.completed":
			switch l.Item.Type {
			case "agent_message":
				st.tool = ""
				if strings.TrimSpace(l.Item.Text) != "" {
					st.messages = append(st.messages, l.Item.Text)
				}
				emit()
			case "command_execution":
				st.tool = ""
			case "error":
				st.errors = append(st.errors, l.Item.Message)
			}
		case "turn.completed":
			st.completed = true
		case "turn.failed":
			st.failed = l.Error.Message
			if st.failed == "" {
				st.failed = "turn failed"
			}
		}
	}
	return st
}

type limitedWriter struct {
	w io.Writer
	n int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.n <= 0 {
		return len(p), nil
	}
	if len(p) > l.n {
		p = p[:l.n]
	}
	l.n -= len(p)
	_, err := l.w.Write(p)
	return len(p), err
}
