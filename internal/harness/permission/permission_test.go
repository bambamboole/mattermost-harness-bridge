package permission

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestAskRoundTrip(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "p.sock")
	var got Request
	srv := &Server{Path: sock, Decide: func(ctx context.Context, req Request) (Decision, error) {
		got = req
		return Allow(req.Input), nil
	}}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dec, err := Ask(ctx, sock, Request{JobID: "job_1", ToolName: "Bash", Input: json.RawMessage(`{"command":"ls"}`), ToolUseID: "toolu_1"})
	if err != nil {
		t.Fatal(err)
	}
	if dec.Behavior != "allow" || string(dec.UpdatedInput) != `{"command":"ls"}` {
		t.Fatalf("decision: %+v", dec)
	}
	if got.JobID != "job_1" || got.ToolName != "Bash" || got.ToolUseID != "toolu_1" {
		t.Fatalf("request: %+v", got)
	}
}

func TestDeciderErrorDenies(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "p.sock")
	srv := &Server{Path: sock, Decide: func(ctx context.Context, req Request) (Decision, error) {
		return Decision{}, context.DeadlineExceeded
	}}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	dec, err := Ask(context.Background(), sock, Request{ToolName: "Bash"})
	if err != nil || dec.Behavior != "deny" {
		t.Fatalf("want deny, got %+v %v", dec, err)
	}
}

func TestMCPConfigShape(t *testing.T) {
	b, err := MCPConfig("/usr/local/bin/harness", "/tmp/x.sock", "job_1")
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Servers map[string]struct {
			Command string   `json:"command"`
			Args    []string `json:"args"`
		} `json:"mcpServers"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	s, ok := cfg.Servers[ServerName]
	if !ok || s.Command != "/usr/local/bin/harness" || len(s.Args) != 5 || s.Args[4] != "job_1" {
		t.Fatalf("config: %s", b)
	}
	if FullName != "mcp__harness__approve" {
		t.Fatalf("tool name: %s", FullName)
	}
}
