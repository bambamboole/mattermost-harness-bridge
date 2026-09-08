package claude

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/agent"
)

// Lines captured from claude 2.1.263, trimmed to the fields we read.
const sample = `{"type":"system","subtype":"init","cwd":"/tmp","session_id":"sess-1","tools":[],"model":"claude-opus-5"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"touch probe.txt"}}]},"session_id":"sess-1"}
{"type":"user","message":{"role":"user","content":[{"tool_use_id":"toolu_1","type":"tool_result","content":"done","is_error":false}]},"session_id":"sess-1"}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Created the file."}]},"session_id":"sess-1"}
{"type":"result","subtype":"success","is_error":false,"num_turns":2,"result":"Created the file.","session_id":"sess-1","total_cost_usd":0.016,"duration_ms":4027}
`

func TestParseSample(t *testing.T) {
	var events []agent.Event
	st := parse(strings.NewReader(sample), func(e agent.Event) { events = append(events, e) })
	if st.sessionID != "sess-1" || !st.sawResult || st.isError || st.turns != 2 || st.cost != 0.016 || st.durationMS != 4027 {
		t.Fatalf("state: %+v", st)
	}
	if st.finalText() != "Created the file." {
		t.Fatalf("final text %q", st.finalText())
	}
	if len(events) != 3 {
		t.Fatalf("events: %d", len(events))
	}
	if events[1].CurrentTool != "Bash" || events[2].CurrentTool != "" || events[2].Text != "Created the file." {
		t.Fatalf("events: %+v", events)
	}
}

func TestParseErrorResult(t *testing.T) {
	in := `{"type":"system","subtype":"init","session_id":"s"}
{"type":"result","subtype":"error_max_turns","is_error":true,"num_turns":3,"result":"hit max turns","session_id":"s"}
`
	st := parse(strings.NewReader(in), nil)
	if !st.isError || st.subtype != "error_max_turns" {
		t.Fatalf("state: %+v", st)
	}
}

func fakeClaude(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRunSuccess(t *testing.T) {
	bin := fakeClaude(t, `printf '%s' "$@" > "$FAKE_ARGS"
cat <<'EOF'
`+sample+`EOF
`)
	argsFile := filepath.Join(t.TempDir(), "args")
	t.Setenv("FAKE_ARGS", argsFile)
	res, err := New(bin).Run(context.Background(), agent.Options{
		Dir: t.TempDir(), Prompt: "hi", MaxTurns: 5,
		ResumeID: "old", PermissionMCPConfigPath: "/x/mcp.json", PermissionTool: "mcp__harness__approve",
		AllowedTools: []string{"Read", "Grep"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != agent.StatusSucceeded || res.SessionID != "sess-1" || res.Text != "Created the file." || res.Turns != 2 {
		t.Fatalf("result: %+v", res)
	}
	args, _ := os.ReadFile(argsFile)
	for _, want := range []string{"-p", "hi", "--max-turns", "5", "--resume", "old", "--mcp-config", "/x/mcp.json", "--strict-mcp-config",
		"--permission-prompt-tool", "mcp__harness__approve", "--allowedTools", "Read,Grep", "--permission-mode", "default"} {
		if !strings.Contains(string(args), want) {
			t.Errorf("args missing %q: %s", want, args)
		}
	}
}

func TestRunCrashWithoutResult(t *testing.T) {
	bin := fakeClaude(t, `echo '{"type":"system","subtype":"init","session_id":"s"}'
echo "boom: not logged in" >&2
exit 1
`)
	res, err := New(bin).Run(context.Background(), agent.Options{Dir: t.TempDir(), Prompt: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != agent.StatusFailed || res.ErrCode != "claude_exit" || !strings.Contains(res.ErrMessage, "not logged in") {
		t.Fatalf("result: %+v", res)
	}
}

func TestRunCancel(t *testing.T) {
	bin := fakeClaude(t, `echo '{"type":"system","subtype":"init","session_id":"s"}'
trap 'exit 130' INT
sleep 30 &
wait $!
`)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	a := New(bin)
	a.KillGrace = time.Second
	res, err := a.Run(ctx, agent.Options{Dir: t.TempDir(), Prompt: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != agent.StatusCancelled {
		t.Fatalf("result: %+v", res)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("cancel took %s", time.Since(start))
	}
}
