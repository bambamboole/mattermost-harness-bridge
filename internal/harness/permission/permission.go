// Package permission bridges the Claude CLI's --permission-prompt-tool to the
// harness daemon. The CLI spawns `mhb harness mcp-permissions` per job as an MCP
// stdio server; that subcommand forwards each tool call over a Unix socket to
// the daemon, which asks the owner through the broker and answers.
//
// Verified against claude 2.1.263: the tool receives
//
//	{"tool_name": "...", "input": {...}, "tool_use_id": "..."}
//
// and must return, as the text content of the tool result, a JSON string
//
//	{"behavior":"allow","updatedInput":{...}}  or
//	{"behavior":"deny","message":"..."}.
package permission

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/mcp"
)

// ToolName is the MCP tool the CLI calls; the CLI addresses it as
// mcp__<server>__<tool>.
const (
	ServerName = "harness"
	ToolName   = "approve"
	FullName   = "mcp__" + ServerName + "__" + ToolName
)

// Request is what the CLI sends to the permission tool, plus the job it
// belongs to (added by the subcommand from its --job flag).
type Request struct {
	JobID     string          `json:"job_id"`
	ToolName  string          `json:"tool_name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
}

// Decision is the reply in the CLI's own shape.
type Decision struct {
	Behavior     string          `json:"behavior"` // allow | deny
	UpdatedInput json.RawMessage `json:"updatedInput,omitempty"`
	Message      string          `json:"message,omitempty"`
}

func Allow(input json.RawMessage) Decision {
	return Decision{Behavior: "allow", UpdatedInput: input}
}

func Deny(msg string) Decision {
	return Decision{Behavior: "deny", Message: msg}
}

// Decider answers a request; it blocks until the owner decided or ctx ends.
type Decider func(ctx context.Context, req Request) (Decision, error)

// Server is the daemon side: a Unix socket that accepts one request per
// connection and answers with one Decision.
type Server struct {
	Path   string
	Decide Decider
	Log    func(format string, args ...any)

	ln   net.Listener
	wg   sync.WaitGroup
	once sync.Once
}

func (s *Server) Start() error {
	_ = os.Remove(s.Path)
	ln, err := net.Listen("unix", s.Path)
	if err != nil {
		return fmt.Errorf("permission: listen %s: %w", s.Path, err)
	}
	if err := os.Chmod(s.Path, 0o600); err != nil {
		_ = ln.Close()
		return err
	}
	s.ln = ln
	s.wg.Add(1)
	go s.accept()
	return nil
}

func (s *Server) Close() error {
	var err error
	s.once.Do(func() {
		if s.ln != nil {
			err = s.ln.Close()
		}
		s.wg.Wait()
		_ = os.Remove(s.Path)
	})
	return err
}

func (s *Server) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

func (s *Server) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	var req Request
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&req); err != nil {
		s.logf("permission: bad request: %v", err)
		return
	}
	dec, err := s.Decide(context.Background(), req)
	if err != nil {
		s.logf("permission: decide job=%s tool=%s: %v", req.JobID, req.ToolName, err)
		dec = Deny("harness error: " + err.Error())
	}
	if err := json.NewEncoder(conn).Encode(dec); err != nil {
		s.logf("permission: write decision: %v", err)
	}
}

func (s *Server) logf(format string, args ...any) {
	if s.Log != nil {
		s.Log(format, args...)
	}
}

// Ask is the subcommand side: connect, send, wait.
func Ask(ctx context.Context, socket string, req Request) (Decision, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return Decision{}, fmt.Errorf("permission: dial %s: %w", socket, err)
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return Decision{}, err
	}
	var dec Decision
	if err := json.NewDecoder(conn).Decode(&dec); err != nil {
		return Decision{}, fmt.Errorf("permission: read decision: %w", err)
	}
	if dec.Behavior != "allow" && dec.Behavior != "deny" {
		return Decision{}, errors.New("permission: invalid decision from daemon")
	}
	return dec, nil
}

// ServeStdio runs the MCP server the CLI talks to. Every tools/call is
// forwarded to the daemon socket with jobID attached.
func ServeStdio(ctx context.Context, socket, jobID string, timeout time.Duration) error {
	srv := &mcp.Server{
		Name:    ServerName,
		Version: "1",
		Tools: []mcp.Tool{{
			Name:        ToolName,
			Description: "Asks the harness owner whether a tool call may run.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"tool_name":{"type":"string"},"input":{"type":"object"},"tool_use_id":{"type":"string"}},"required":["tool_name","input"]}`),
		}},
		Handlers: map[string]mcp.Handler{
			ToolName: func(ctx context.Context, args json.RawMessage) (string, bool, error) {
				var req Request
				if err := json.Unmarshal(args, &req); err != nil {
					return "", false, fmt.Errorf("bad arguments: %w", err)
				}
				req.JobID = jobID
				actx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				dec, err := Ask(actx, socket, req)
				if err != nil {
					// Fail closed: the CLI treats the text as the decision.
					dec = Deny("harness unreachable: " + err.Error())
				}
				out, _ := json.Marshal(dec)
				return string(out), false, nil
			},
		},
	}
	return srv.Serve(ctx, os.Stdin, os.Stdout)
}

// MCPConfig renders the --mcp-config JSON that makes the CLI spawn the
// subcommand for a job.
func MCPConfig(harnessBin, socket, jobID string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"mcpServers": map[string]any{
			ServerName: map[string]any{
				"command": harnessBin,
				"args":    []string{"harness", "mcp-permissions", "--socket", socket, "--job", jobID},
			},
		},
	})
}
