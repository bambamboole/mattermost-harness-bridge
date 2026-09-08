// Package runner drives one `claude -p` subprocess per job and turns its
// stream-json output into progress events and a final result.
package runner

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
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Options struct {
	ClaudeBin          string
	Dir                string
	Prompt             string
	ResumeSessionID    string
	MaxTurns           int
	AllowedTools       []string
	DisallowedTools    []string
	MCPConfigPath      string
	PermissionTool     string
	SystemPromptAppend string
	Model              string
	// KillGrace is how long to wait after SIGINT before SIGKILL.
	KillGrace time.Duration
}

// Event is emitted as the run progresses. Text is the accumulated assistant
// output so far (a snapshot, ready for the status post).
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

// Run blocks until the subprocess exits. Cancelling ctx sends SIGINT, then
// SIGKILL after KillGrace, and yields StatusCancelled.
func Run(ctx context.Context, opt Options, onEvent func(Event)) (Result, error) {
	args := []string{
		"-p", opt.Prompt,
		"--output-format", "stream-json",
		"--verbose",
		"--permission-mode", "default",
		"--permission-prompts", "host",
	}
	if opt.MaxTurns > 0 {
		args = append(args, "--max-turns", strconv.Itoa(opt.MaxTurns))
	}
	if opt.ResumeSessionID != "" {
		args = append(args, "--resume", opt.ResumeSessionID)
	}
	if opt.MCPConfigPath != "" {
		args = append(args, "--mcp-config", opt.MCPConfigPath, "--strict-mcp-config")
	}
	if opt.PermissionTool != "" {
		args = append(args, "--permission-prompt-tool", opt.PermissionTool)
	}
	if len(opt.AllowedTools) > 0 {
		args = append(args, "--allowedTools", strings.Join(opt.AllowedTools, ","))
	}
	if len(opt.DisallowedTools) > 0 {
		args = append(args, "--disallowedTools", strings.Join(opt.DisallowedTools, ","))
	}
	if opt.SystemPromptAppend != "" {
		args = append(args, "--append-system-prompt", opt.SystemPromptAppend)
	}
	if opt.Model != "" {
		args = append(args, "--model", opt.Model)
	}

	bin := opt.ClaudeBin
	if bin == "" {
		bin = "claude"
	}
	// Detached from ctx on purpose: we do the SIGINT/SIGKILL dance ourselves.
	cmd := exec.Command(bin, args...)
	cmd.Dir = opt.Dir
	cmd.Env = append(os.Environ(), "CLAUDE_CODE_HARNESS_JOB=1")
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Result{}, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &limitedWriter{w: &stderr, n: 64 << 10}

	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("runner: start %s: %w", bin, err)
	}

	killed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			grace := opt.KillGrace
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

	st := parse(stdout, onEvent)
	waitErr := cmd.Wait()
	close(killed)

	res := Result{
		SessionID:  st.sessionID,
		Text:       st.finalText(),
		Turns:      st.turns,
		CostUSD:    st.cost,
		DurationMS: st.durationMS,
	}
	switch {
	case ctx.Err() != nil:
		res.Status = StatusCancelled
		res.ErrCode = "cancelled"
		res.ErrMessage = ctx.Err().Error()
	case st.sawResult && !st.isError:
		res.Status = StatusSucceeded
	case st.sawResult:
		res.Status = StatusFailed
		res.ErrCode = st.subtype
		res.ErrMessage = st.resultText
	default:
		res.Status = StatusFailed
		res.ErrCode = "claude_exit"
		msg := strings.TrimSpace(stderr.String())
		if msg == "" && waitErr != nil {
			msg = waitErr.Error()
		}
		if msg == "" {
			msg = "claude exited without a result message"
		}
		res.ErrMessage = msg
	}
	return res, nil
}

type streamState struct {
	sessionID  string
	texts      []string
	tool       string
	turns      int
	sawResult  bool
	isError    bool
	subtype    string
	resultText string
	cost       float64
	durationMS int64
}

func (s *streamState) snapshot() string {
	return strings.TrimSpace(strings.Join(s.texts, "\n\n"))
}

func (s *streamState) finalText() string {
	if s.sawResult && s.resultText != "" && !s.isError {
		return s.resultText
	}
	return s.snapshot()
}

type line struct {
	Type    string `json:"type"`
	Subtype string `json:"subtype"`
	Session string `json:"session_id"`
	Message struct {
		Content []struct {
			Type  string          `json:"type"`
			Text  string          `json:"text"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
	} `json:"message"`
	IsError    bool    `json:"is_error"`
	Result     string  `json:"result"`
	NumTurns   int     `json:"num_turns"`
	CostUSD    float64 `json:"total_cost_usd"`
	DurationMS int64   `json:"duration_ms"`
}

func parse(r io.Reader, onEvent func(Event)) *streamState {
	st := &streamState{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 32<<20)
	emit := func() {
		if onEvent != nil {
			onEvent(Event{SessionID: st.sessionID, Text: st.snapshot(), CurrentTool: st.tool, Turns: st.turns})
		}
	}
	for sc.Scan() {
		var l line
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			continue
		}
		if l.Session != "" {
			st.sessionID = l.Session
		}
		switch l.Type {
		case "system":
			if l.Subtype == "init" {
				emit()
			}
		case "assistant":
			st.tool = ""
			for _, c := range l.Message.Content {
				switch c.Type {
				case "text":
					if strings.TrimSpace(c.Text) != "" {
						st.texts = append(st.texts, c.Text)
					}
				case "tool_use":
					st.tool = c.Name
					st.turns++
				}
			}
			emit()
		case "user":
			// tool_result; nothing to show, the next assistant message carries it.
		case "result":
			st.sawResult = true
			st.isError = l.IsError
			st.subtype = l.Subtype
			st.resultText = l.Result
			if l.NumTurns > 0 {
				st.turns = l.NumTurns
			}
			st.cost = l.CostUSD
			st.durationMS = l.DurationMS
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

var ErrNoClaude = errors.New("runner: claude binary not found")

// Check verifies the claude binary is runnable.
func Check(bin string) error {
	if bin == "" {
		bin = "claude"
	}
	if _, err := exec.LookPath(bin); err != nil {
		return fmt.Errorf("%w: %v", ErrNoClaude, err)
	}
	return nil
}
