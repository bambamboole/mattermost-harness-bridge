package codex

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bambamboole/mattermost-harness-bridge/internal/harness/agent"
)

// Lines captured from codex-cli 0.153.4.
const sample = `{"type":"thread.started","thread_id":"01a081b0-431f-7e52-b9e5-9bf4732c4715"}
{"type":"turn.started"}
{"type":"item.started","item":{"id":"item_0","type":"command_execution","command":"/bin/zsh -lc 'echo codex-probe'","aggregated_output":"","exit_code":null,"status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_0","type":"command_execution","command":"/bin/zsh -lc 'echo codex-probe'","aggregated_output":"codex-probe\n","exit_code":0,"status":"completed"}}
{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"codex-probe"}}
{"type":"turn.completed","usage":{"input_tokens":38484,"cached_input_tokens":31232,"output_tokens":39}}
`

func TestParseSample(t *testing.T) {
	var events []agent.Event
	st := parse(strings.NewReader(sample), func(e agent.Event) { events = append(events, e) })
	if st.threadID != "01a081b0-431f-7e52-b9e5-9bf4732c4715" || !st.completed || st.turns != 1 || st.finalText() != "codex-probe" {
		t.Fatalf("state: %+v", st)
	}
	if len(events) != 3 || events[1].CurrentTool != "shell" || events[2].Text != "codex-probe" {
		t.Fatalf("events: %+v", events)
	}
}

func TestParseFailedTurn(t *testing.T) {
	in := `{"type":"thread.started","thread_id":"t"}
{"type":"item.completed","item":{"type":"error","message":"Model metadata for x not found"}}
{"type":"turn.failed","error":{"message":"boom"}}
`
	st := parse(strings.NewReader(in), nil)
	if st.failed != "boom" || len(st.errors) != 1 || st.completed {
		t.Fatalf("state: %+v", st)
	}
}

func fakeCodex(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRunBuildsArgsAndResumes(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	t.Setenv("FAKE_ARGS", argsFile)
	bin := fakeCodex(t, `printf '%s\0' "$@" > "$FAKE_ARGS"
cat <<'EOF'
`+sample+`EOF
`)
	dir := t.TempDir()
	res, err := New(bin, "").Run(context.Background(), agent.Options{Dir: dir, Prompt: "do it", ResumeID: "old-thread", Model: "gpt-5", SystemPrompt: "be brief"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != agent.StatusSucceeded || res.SessionID != "01a081b0-431f-7e52-b9e5-9bf4732c4715" || res.Text != "codex-probe" || res.Turns != 1 {
		t.Fatalf("result: %+v", res)
	}
	got, _ := os.ReadFile(argsFile)
	args := strings.Split(strings.TrimSuffix(string(got), "\x00"), "\x00")
	want := []string{"exec", "resume", "old-thread", "--json", "--skip-git-repo-check", "--color", "never", "-C", dir, "-s", SandboxWorkspaceWrite, "-m", "gpt-5", "be brief\n\ndo it"}
	if strings.Join(args, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args:\n got %q\nwant %q", args, want)
	}
}

func TestRunWithoutCompletionFails(t *testing.T) {
	bin := fakeCodex(t, `echo '{"type":"thread.started","thread_id":"t"}'
echo "not logged in" >&2
exit 1
`)
	res, err := New(bin, SandboxReadOnly).Run(context.Background(), agent.Options{Dir: t.TempDir(), Prompt: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != agent.StatusFailed || res.ErrCode != "codex_exit" || !strings.Contains(res.ErrMessage, "not logged in") {
		t.Fatalf("result: %+v", res)
	}
}
