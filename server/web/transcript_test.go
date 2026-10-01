package web

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

func ev(seq int64, typ event.Type, at time.Time, mut func(*event.Event)) event.Event {
	e := event.Event{
		ID:         "e" + strings.Repeat("0", 0) + string(rune('a'+seq%26)),
		SessionID:  "s",
		Seq:        seq,
		Type:       typ,
		Source:     event.SourceClaudeCode,
		Origin:     event.OriginHook,
		OccurredAt: at,
	}
	if mut != nil {
		mut(&e)
	}
	return e
}

// workTurn builds a turn whose events are all work rows, which is what the
// pairing tests need: no prompt, no final, only tools.
func workTurn(events ...event.Event) Turn {
	t := Turn{Outcome: "no_answer_captured"}
	for _, e := range events {
		t.Events = append(t.Events, TurnEvent{Event: e, Role: "work"})
	}
	if len(events) > 0 {
		t.StartedAt = events[0].OccurredAt
	}
	return t
}

func TestToolResultMergesOntoItsCall(t *testing.T) {
	base := fixedNow
	in, _ := json.Marshal(map[string]string{"command": "ls -la"})
	v := buildTurnView(workTurn(
		ev(1, event.ToolCall, base, func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Bash", Input: in}
		}),
		ev(2, event.ToolResult, base.Add(time.Second), func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Bash", Output: "total 0"}
		}),
	), TranscriptOptions{Start: base})

	if len(v.Work) != 1 {
		t.Fatalf("want one merged block, got %d", len(v.Work))
	}
	b := v.Work[0]
	if b.Tool.Summary != "ls -la" {
		t.Errorf("summary = %q, want the command", b.Tool.Summary)
	}
	if len(b.Tool.Output) == 0 || b.Tool.Output[0].Text != "total 0" {
		t.Errorf("result did not merge onto the call: %+v", b.Tool.Output)
	}
}

// TestToolResultPairsByToolUseIDBeforeAdjacency: two calls in flight and the
// results arriving out of order pair by the harness's id, not by position.
func TestToolResultPairsByToolUseIDBeforeAdjacency(t *testing.T) {
	base := fixedNow
	v := buildTurnView(workTurn(
		ev(1, event.ToolCall, base, func(e *event.Event) {
			e.ToolUseID = "tu_a"
			e.Tool = &event.Tool{Name: "Bash", Input: json.RawMessage(`{"command":"a"}`)}
		}),
		ev(2, event.ToolCall, base.Add(time.Second), func(e *event.Event) {
			e.ToolUseID = "tu_b"
			e.Tool = &event.Tool{Name: "Bash", Input: json.RawMessage(`{"command":"b"}`)}
		}),
		ev(3, event.ToolResult, base.Add(2*time.Second), func(e *event.Event) {
			e.ToolUseID = "tu_a"
			e.Tool = &event.Tool{Name: "Bash", Output: "out a"}
		}),
		ev(4, event.ToolResult, base.Add(3*time.Second), func(e *event.Event) {
			e.ToolUseID = "tu_b"
			e.Tool = &event.Tool{Name: "Bash", Output: "out b"}
		}),
	), TranscriptOptions{Start: base})
	if len(v.Work) != 2 {
		t.Fatalf("blocks = %d, want the two calls with their results paired", len(v.Work))
	}
	if got := v.Work[0].Tool.Output[0].Text; got != "out a" {
		t.Errorf("first call got %q; adjacency won over the tool_use id", got)
	}
	if got := v.Work[1].Tool.Output[0].Text; got != "out b" {
		t.Errorf("second call got %q", got)
	}
}

// TestOrphanResultKeepsItsOwnBlock covers a call the fold placed elsewhere:
// the result must still be rendered rather than dropped.
func TestOrphanResultKeepsItsOwnBlock(t *testing.T) {
	base := fixedNow
	v := buildTurnView(workTurn(
		ev(9, event.ToolResult, base, func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Bash", Output: "orphaned output"}
		}),
	), TranscriptOptions{Start: base})

	if len(v.Work) != 1 {
		t.Fatal("the orphaned result was dropped")
	}
	if got := v.Work[0].Tool.Output[0].Text; got != "orphaned output" {
		t.Errorf("output = %q", got)
	}
}

func TestSecondResultDoesNotOverwriteTheFirst(t *testing.T) {
	base := fixedNow
	v := buildTurnView(workTurn(
		ev(1, event.ToolCall, base, func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Bash"}
		}),
		ev(2, event.ToolResult, base, func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Bash", Output: "first"}
		}),
		ev(3, event.ToolResult, base, func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Bash", Output: "second"}
		}),
	), TranscriptOptions{Start: base})

	if n := len(v.Work); n != 2 {
		t.Fatalf("blocks = %d, want the second result to stand alone", n)
	}
	if got := v.Work[1].Tool.Output[0].Text; got != "second" {
		t.Errorf("second block output = %q", got)
	}
}

func TestFailureMarksTheCall(t *testing.T) {
	base := fixedNow
	v := buildTurnView(workTurn(
		ev(1, event.ToolCall, base, func(e *event.Event) { e.Tool = &event.Tool{Name: "Edit"} }),
		ev(2, event.ToolFailed, base, func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Edit", Error: "permission denied"}
		}),
	), TranscriptOptions{Start: base})

	b := v.Work[0]
	if !b.Failed {
		t.Error("the merged block is not marked failed")
	}
	if b.Tool.Error != "permission denied" {
		t.Errorf("error = %q", b.Tool.Error)
	}
}

func TestOversizedBlocksAreTruncatedAndLinked(t *testing.T) {
	base := fixedNow
	huge := strings.Repeat("x", 40_000)
	turn := Turn{Kind: "human", Outcome: "no_work", PromptEventID: "big", StartedAt: base}
	turn.Events = []TurnEvent{{
		Event: ev(1, event.UserPrompt, base, func(e *event.Event) { e.Text = huge; e.ID = "big" }),
		Role:  "prompt", Kind: "human",
	}}
	v := buildTurnView(turn, TranscriptOptions{Start: base, MaxText: 100, SessionID: "s 1"})

	if v.Prompt == nil {
		t.Fatal("the prompt did not render")
	}
	b := *v.Prompt
	if !b.TextTruncated {
		t.Fatal("an oversized block was not truncated")
	}
	if n := len(b.Text[0].Text); n > 100 {
		t.Errorf("rendered %d bytes, want at most the limit", n)
	}
	if b.EventURL != "/sessions/s%201/events/big" {
		t.Errorf("EventURL = %q, want an escaped link to the full event", b.EventURL)
	}
	if b.Anchor() != "ev-big" {
		t.Errorf("anchor = %q, want the event id, which is unique where seq is not", b.Anchor())
	}
}

func TestTruncationRespectsRuneBoundaries(t *testing.T) {
	// A limit that lands mid-sequence must not split the rune, or the page
	// renders a replacement character in an archive whose value is fidelity.
	segs, truncated := textSegments(strings.Repeat("é", 20), 11, nil)
	if !truncated {
		t.Fatal("expected truncation")
	}
	for _, r := range segs[0].Text {
		if r != 'é' {
			t.Fatalf("truncation split a rune: %q", segs[0].Text)
		}
	}
}

func TestToolSummaryFallsBackToArgumentNames(t *testing.T) {
	in, _ := json.Marshal(map[string]int{"zeta": 1, "alpha": 2})
	got := toolSummary(&event.Tool{Name: "Weird", Input: in})
	if got != "alpha, zeta" {
		t.Errorf("summary = %q, want the sorted argument names", got)
	}
}

func TestElapsedLabel(t *testing.T) {
	for _, tc := range []struct {
		in   time.Duration
		want string
	}{
		{0, "+0s"},
		{45 * time.Second, "+45s"},
		{90 * time.Second, "+1m30s"},
		{2*time.Hour + 5*time.Minute, "+2h05m"},
	} {
		if got := ElapsedLabel(tc.in); got != tc.want {
			t.Errorf("ElapsedLabel(%s) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
