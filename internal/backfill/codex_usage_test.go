package backfill

import (
	"fmt"
	"testing"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

// Codex token accounting. Codex sessions were tool-rich and token-free from the
// day the walker was written: it handled three envelope types and the token
// counts arrive in a fourth, so 790 sessions recorded no tokens at all. These
// pin the mapping and, just as importantly, the two ways of getting it wrong
// that both produce a plausible-looking number.

// tokenCount is a Codex token_count record. Both usages are spelled out because
// the difference between them is the trap.
func tokenCount(ts string, totalIn, totalCached, totalOut, lastIn, lastCached, lastOut int64) string {
	return line(ts, "event_msg", fmt.Sprintf(
		`{"type":"token_count","info":{`+
			`"total_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":7,"total_tokens":%d},`+
			`"last_token_usage":{"input_tokens":%d,"cached_input_tokens":%d,"output_tokens":%d,"reasoning_output_tokens":3,"total_tokens":%d}}}`,
		totalIn, totalCached, totalOut, totalIn+totalOut,
		lastIn, lastCached, lastOut, lastIn+lastOut))
}

func usageOf(events []event.Event) []*event.Usage {
	var out []*event.Usage
	for _, e := range events {
		if e.Usage != nil {
			out = append(out, e.Usage)
		}
	}
	return out
}

func TestCodexTokenCountBillsTheTurnItBelongsTo(t *testing.T) {
	root := t.TempDir()
	write(t, root, "rollout.jsonl",
		meta(rootID, ""),
		line(t1, "turn_context", `{"model":"gpt-5.4","cwd":"/repo"}`),
		line(t1, "response_item", `{"type":"message","id":"u1","role":"user","content":[{"type":"input_text","text":"go"}]}`),
		line(t2, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"done"}]}`),
		tokenCount(t2, 9000, 8000, 500, 1000, 900, 50),
	)
	got, _ := walk(t, Options{Root: root})

	var turn *event.Event
	for i := range got {
		if got[i].Type == event.AssistantTurn {
			turn = &got[i]
		}
	}
	if turn == nil {
		t.Fatal("no assistant turn was emitted")
	}
	if turn.Usage == nil {
		t.Fatal("the assistant turn carries no usage; token_count was dropped again")
	}
	// input_tokens counts cached input inside itself — codex-rs derives its own
	// displayed figure the same way — while event.Usage and the pricer treat
	// the two as disjoint and bill them at different rates. Passing 1000
	// straight through would charge the 900 cached tokens twice.
	if turn.Usage.InputTokens != 100 {
		t.Errorf("input = %d, want 1000 - 900 cached = 100", turn.Usage.InputTokens)
	}
	if turn.Usage.CacheReadTokens != 900 {
		t.Errorf("cache read = %d, want 900", turn.Usage.CacheReadTokens)
	}
	if turn.Usage.OutputTokens != 50 {
		t.Errorf("output = %d, want 50; reasoning is inside output, not beside it", turn.Usage.OutputTokens)
	}
	// No message id anywhere in a Codex record, which is what sends the ledger
	// key to the event id.
	if turn.Usage.MessageID != "" || turn.Usage.RequestID != "" {
		t.Errorf("usage carries ids %q/%q; Codex has neither", turn.Usage.MessageID, turn.Usage.RequestID)
	}
	if turn.Model != "gpt-5.4" {
		t.Errorf("model = %q, want gpt-5.4, or the tokens cannot be priced", turn.Model)
	}
}

// The trap. Codex reports the session's running total beside the turn's own
// figure, and the running total is the one that looks like the answer. Adding
// those up counts turn one again on every later record, so the over-report
// grows with the square of the turn count rather than staying a fixed factor.
func TestCodexUsesThePerTurnFigureNotTheRunningTotal(t *testing.T) {
	root := t.TempDir()
	write(t, root, "rollout.jsonl",
		meta(rootID, ""),
		line(t1, "turn_context", `{"model":"gpt-5.4","cwd":"/repo"}`),
		line(t1, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"one"}]}`),
		tokenCount(t1, 100, 0, 10, 100, 0, 10),
		line(t2, "response_item", `{"type":"message","id":"a2","role":"assistant","content":[{"type":"output_text","text":"two"}]}`),
		tokenCount(t2, 300, 0, 30, 200, 0, 20),
		line(t3, "response_item", `{"type":"message","id":"a3","role":"assistant","content":[{"type":"output_text","text":"three"}]}`),
		tokenCount(t3, 600, 0, 60, 300, 0, 30),
	)
	got, _ := walk(t, Options{Root: root})

	var in, out int64
	for _, u := range usageOf(got) {
		in, out = in+u.InputTokens, out+u.OutputTokens
	}
	// 100 + 200 + 300 from last_token_usage. The cumulative field would give
	// 1000, and it would keep diverging as the session got longer.
	if in != 600 || out != 60 {
		t.Errorf("session totals = %d in / %d out, want 600/60 from the per-turn figures", in, out)
	}
}

// The invariant that makes this shippable alongside a capture-schema bump.
//
// Sequence numbers are assigned in emission order and hashed into every event
// id, so an extra event partway through a session renumbers everything after
// it — and the re-walk meant to add token counts to 790 stored sessions would
// instead insert a second copy of most of each one. Reading token_count must
// therefore change what events say and never which events there are.
func TestCodexTokenCountAddsNoEventsAndMovesNoIDs(t *testing.T) {
	body := []string{
		meta(rootID, ""),
		line(t1, "turn_context", `{"model":"gpt-5.4","cwd":"/repo"}`),
		line(t1, "response_item", `{"type":"message","id":"u1","role":"user","content":[{"type":"input_text","text":"go"}]}`),
		line(t2, "response_item", `{"type":"function_call","name":"exec","arguments":"{}","call_id":"c1"}`),
		line(t2, "response_item", `{"type":"function_call_output","call_id":"c1","output":"ok"}`),
		line(t3, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"done"}]}`),
	}
	withCounts := []string{
		body[0], body[1], body[2],
		body[3], body[4],
		tokenCount(t2, 500, 100, 50, 500, 100, 50),
		body[5],
		tokenCount(t3, 900, 200, 90, 400, 100, 40),
	}

	rootA := t.TempDir()
	write(t, rootA, "rollout.jsonl", body...)
	before, _ := walk(t, Options{Root: rootA})

	rootB := t.TempDir()
	write(t, rootB, "rollout.jsonl", withCounts...)
	after, _ := walk(t, Options{Root: rootB})

	if len(before) != len(after) {
		t.Fatalf("token_count changed the event count from %d to %d", len(before), len(after))
	}
	for i := range before {
		if before[i].ID != after[i].ID {
			t.Errorf("event %d (%s) changed id; a re-walk would duplicate it and everything after it",
				i, before[i].Type)
		}
		if before[i].Seq != after[i].Seq {
			t.Errorf("event %d (%s) changed seq %d -> %d", i, before[i].Type, before[i].Seq, after[i].Seq)
		}
	}
	if len(usageOf(after)) != 2 {
		t.Errorf("%d events carry usage, want one per token_count", len(usageOf(after)))
	}
	// A turn that ends in a tool call rather than prose still spent money, so
	// its cost rides the last event of the turn — the same rule the Claude
	// walker follows when a record has no prose to attach usage to.
	if after[len(after)-1].Usage == nil {
		t.Error("the final turn's cost was dropped because nothing followed it")
	}
}

// Re-importing the same rollout has to be free. Codex usage keys off the event
// id, so this is the property that keeps the ledger's ON CONFLICT effective:
// same file in, same ids and same numbers out.
func TestCodexReimportProducesTheSameIDsAndTheSameUsage(t *testing.T) {
	root := t.TempDir()
	write(t, root, "rollout.jsonl",
		meta(rootID, ""),
		line(t1, "turn_context", `{"model":"gpt-5.4","cwd":"/repo"}`),
		line(t1, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"one"}]}`),
		tokenCount(t1, 400, 100, 40, 400, 100, 40),
	)

	first, _ := walk(t, Options{Root: root})
	second, _ := walk(t, Options{Root: root})

	if len(first) != len(second) {
		t.Fatalf("re-import emitted %d events, first pass emitted %d", len(second), len(first))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Fatalf("event %d changed id between imports: %s -> %s", i, first[i].ID, second[i].ID)
		}
		switch {
		case first[i].Usage == nil && second[i].Usage == nil:
		case first[i].Usage == nil || second[i].Usage == nil:
			t.Fatalf("event %d gained or lost usage between imports", i)
		case *first[i].Usage != *second[i].Usage:
			t.Fatalf("event %d changed usage: %+v -> %+v", i, first[i].Usage, second[i].Usage)
		}
	}
}

// Codex writes a token_count on some turn boundaries with nothing between it
// and the last one. Two arriving back to back is that repeat, not two calls: a
// real call always writes at least one response item, which releases the event
// the first cost attached to.
func TestCodexRepeatedTokenCountBillsTheTurnOnce(t *testing.T) {
	root := t.TempDir()
	write(t, root, "rollout.jsonl",
		meta(rootID, ""),
		line(t1, "turn_context", `{"model":"gpt-5.4","cwd":"/repo"}`),
		line(t1, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"one"}]}`),
		tokenCount(t1, 400, 100, 40, 400, 100, 40),
		tokenCount(t1, 400, 100, 40, 400, 100, 40),
	)
	got, _ := walk(t, Options{Root: root})

	us := usageOf(got)
	if len(us) != 1 {
		t.Fatalf("%d events carry usage, want 1", len(us))
	}
	if us[0].InputTokens != 300 || us[0].CacheReadTokens != 100 {
		t.Errorf("usage = %+v, want the turn counted once (300 uncached + 100 cached)", us[0])
	}
}

// An empty token_count is what Codex writes before anything has been spent.
// Turning it into a zero-token usage row would put a priced-at-nothing entry in
// the ledger for a call that never happened.
func TestCodexEmptyTokenCountIsNotUsage(t *testing.T) {
	root := t.TempDir()
	write(t, root, "rollout.jsonl",
		meta(rootID, ""),
		line(t1, "turn_context", `{"model":"gpt-5.4","cwd":"/repo"}`),
		line(t1, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"one"}]}`),
		tokenCount(t1, 0, 0, 0, 0, 0, 0),
		line(t2, "event_msg", `{"type":"token_count","info":null}`),
		line(t2, "event_msg", `{"type":"agent_message","message":"restates a response_item"}`),
	)
	got, _ := walk(t, Options{Root: root})

	if us := usageOf(got); len(us) != 0 {
		t.Errorf("usage = %+v, want none", us)
	}
}
