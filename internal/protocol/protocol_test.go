package protocol

import (
	"encoding/json"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	env, err := New(TypeJobDispatch, "job_1", JobDispatch{
		Workspace: "ws",
		Prompt:    "hi",
		Thread:    Thread{ChannelID: "c", RootPostID: "r", TriggerPostID: "t"},
		Requester: Requester{MMUserID: "u", Username: "manuel"},
		Limits:    Limits{MaxTurns: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !HasPrefix(env.ID, "msg") {
		t.Fatalf("id %q lacks msg prefix", env.ID)
	}

	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var back Envelope
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	var d JobDispatch
	if err := back.Decode(&d); err != nil {
		t.Fatal(err)
	}
	if d.Prompt != "hi" || d.Requester.MMUserID != "u" || d.Limits.MaxTurns != 3 {
		t.Fatalf("payload mismatch: %+v", d)
	}
	if back.JobID != "job_1" || back.Type != TypeJobDispatch {
		t.Fatalf("envelope mismatch: %+v", back)
	}
}

func TestReplyCarriesRefAndJob(t *testing.T) {
	req, _ := New(TypeJobResult, "job_1", JobResult{Status: StatusSucceeded})
	ack, err := Reply(TypeAck, req, Ack{OK: true})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Ref != req.ID || ack.JobID != "job_1" {
		t.Fatalf("reply not linked: %+v", ack)
	}
}

func TestNeedsAck(t *testing.T) {
	for _, typ := range []string{TypeJobDispatch, TypeJobCancel, TypeApprovalResponse, TypeJobResult, TypeApprovalRequest} {
		if !NeedsAck(typ) {
			t.Errorf("%s should need ack", typ)
		}
	}
	for _, typ := range []string{TypeHello, TypeWelcome, TypePing, TypePong, TypeJobProgress, TypeAck, TypeError} {
		if NeedsAck(typ) {
			t.Errorf("%s should not need ack", typ)
		}
	}
}
