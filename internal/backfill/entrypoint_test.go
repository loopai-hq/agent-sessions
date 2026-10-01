package backfill

import (
	"testing"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

// Transcript records say how the harness was started; the walker lifts it onto
// every event as a named field so the server classifies without parsing Raw.
func TestWalkerStampsTheEntrypoint(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		`{"type":"user","sessionId":"`+sessA+`","timestamp":"`+ts1+`","uuid":"u1","entrypoint":"sdk-cli","cwd":"/","message":{"role":"user","content":"pong"}}`,
	)

	got, _ := collectEvents(t, Options{Root: tr.root})
	if len(got) == 0 {
		t.Fatal("no events emitted")
	}
	for _, e := range got {
		if e.Entrypoint != "sdk-cli" {
			t.Errorf("%s carries entrypoint %q, want sdk-cli", e.Type, e.Entrypoint)
		}
		if e.CaptureVersion != event.CaptureSchema {
			t.Errorf("%s carries capture version %d, want %d", e.Type, e.CaptureVersion, event.CaptureSchema)
		}
	}
}
