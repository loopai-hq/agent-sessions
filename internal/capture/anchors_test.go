package capture

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/backfill"
	"github.com/loopai-hq/agent-sessions/internal/event"
)

// The anchors: what lets a hook copy of a turn and the transcript copy of the
// same turn be recognised as one thing. Before them the server held 437
// sessions with two unrelated copies of every turn and counted both.

const probePrompt = "bb03a102-4ccd-40e9-b146-8a67e8db1ec1"

func TestUserPromptStampsPromptID(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	if _, err := c.Handle(HookEvent{HookEventName: "UserPromptSubmit", SessionID: "s1", PromptID: probePrompt, Prompt: "hi"}); err != nil {
		t.Fatal(err)
	}
	if r.events[0].PromptID != probePrompt {
		t.Fatalf("prompt_id = %q, want the hook's", r.events[0].PromptID)
	}
}

// Every hook that carries prompt_id stamps it, on every event it emits.
func TestEveryHookAfterTheFirstPromptStampsPromptID(t *testing.T) {
	for _, hook := range []string{"PreToolUse", "PostToolUse", "PostToolUseFailure", "Stop", "SubagentStop", "PreCompact", "SessionEnd"} {
		t.Run(hook, func(t *testing.T) {
			r := &recorder{}
			c := newCap(t, r)
			resp := json.RawMessage(`{"filePath":"/repo/a.go","originalFile":"x","newString":"y"}`)
			if _, err := c.Handle(HookEvent{HookEventName: hook, SessionID: "s1", PromptID: probePrompt, ToolName: "Edit", ToolResponse: resp}); err != nil {
				t.Fatal(err)
			}
			if len(r.events) == 0 {
				t.Fatal("no events")
			}
			for _, e := range r.events {
				if e.PromptID != probePrompt {
					t.Errorf("%s emitted %s without the prompt id", hook, e.Type)
				}
			}
		})
	}
}

// SessionStart fires before any prompt exists and carries no prompt_id; its
// event must not invent one.
func TestEventsBeforeFirstPromptHaveNoPromptID(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	if _, err := c.Handle(HookEvent{HookEventName: "SessionStart", SessionID: "s1", Source: "startup"}); err != nil {
		t.Fatal(err)
	}
	if r.events[0].PromptID != "" {
		t.Fatalf("SessionStart carried prompt_id %q", r.events[0].PromptID)
	}
}

func TestPostToolUseStampsToolUseID(t *testing.T) {
	const tu = "toolu_012HH5HfZ73a2TttRtX5xLQU"
	r := &recorder{}
	c := newCap(t, r)
	resp := json.RawMessage(`{"filePath":"/repo/a.go","originalFile":"x","newString":"y","stdout":"ok"}`)
	for _, hook := range []string{"PreToolUse", "PostToolUse", "PostToolUseFailure"} {
		if _, err := c.Handle(HookEvent{HookEventName: hook, SessionID: "s1", ToolUseID: tu, ToolName: "Edit", ToolResponse: resp}); err != nil {
			t.Fatal(err)
		}
	}
	var types []event.Type
	for _, e := range r.events {
		types = append(types, e.Type)
		if e.ToolUseID != tu {
			t.Errorf("%s carries tool_use_id %q, want %q", e.Type, e.ToolUseID, tu)
		}
	}
	// tool_call, tool_result + file_changed, tool_failed + file_changed.
	if len(types) != 5 {
		t.Fatalf("emitted %v, want call, result, change, failed, change", types)
	}
}

// userRecord is a transcript user record carrying a promptId.
func userRecord(uuid, promptID, text string) string {
	return fmt.Sprintf(`{"type":"user","sessionId":"s1","timestamp":"2026-08-17T09:59:00.000Z","uuid":%q,"promptId":%q,"message":{"role":"user","content":%q}}`, uuid, promptID, text)
}

func TestStopStampsPromptIDAndRecordUUID(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t,
		userRecord("u1", probePrompt, "go"),
		assistantRecord("msg_7", "claude-fable-5", 120, 34),
	)
	if _, err := c.Handle(HookEvent{
		HookEventName: "Stop", SessionID: "s1", PromptID: probePrompt, TranscriptPath: path,
		LastAssistantMessage: json.RawMessage(`"done"`),
	}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.PromptID != probePrompt {
		t.Errorf("prompt_id = %q", e.PromptID)
	}
	if e.RecordUUID != "a1" {
		t.Errorf("record_uuid = %q, want the transcript's a1", e.RecordUUID)
	}
	if e.Usage == nil || e.Usage.MessageID != "msg_7" || e.Model != "claude-fable-5" {
		t.Errorf("usage/model lost alongside the anchor: %+v %q", e.Usage, e.Model)
	}
	// The live copy carries the record's request id alongside the message id,
	// the same pair the walked copy of a1 carries; the server's ledger keys on
	// the message id alone, so this cannot bill the call twice.
	if e.Usage.RequestID != "req_1" {
		t.Errorf("request id = %q on the live copy, want the record's req_1", e.Usage.RequestID)
	}
	if e.Text != "done" {
		t.Errorf("text = %q", e.Text)
	}
}

// A large attachment record between the prompt and the answer (a CLAUDE.md
// import, a hook's injected context; 104 KB measured on 2026-09-15) used to
// push the prompt out of the tail read's window, and the strict rule below
// then refused the anchor on every such turn: one hook copy in five across
// the fleet that day. The read widens until the prompt is in view.
func TestStopKeepsItsAnchorsBehindALargeAttachment(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t,
		userRecord("u1", probePrompt, "go"),
		fmt.Sprintf(`{"type":"attachment","sessionId":"s1","timestamp":"2026-08-17T10:00:00.500Z","uuid":"att1","attachment":{"type":"hook_additional_context","content":%q}}`, strings.Repeat("x", 104<<10)),
		assistantRecord("msg_7", "claude-fable-5", 120, 34),
	)
	if _, err := c.Handle(HookEvent{
		HookEventName: "Stop", SessionID: "s1", PromptID: probePrompt, TranscriptPath: path,
		LastAssistantMessage: json.RawMessage(`"done"`),
	}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.RecordUUID != "a1" || e.Usage == nil || e.Usage.MessageID != "msg_7" || e.Usage.RequestID != "req_1" {
		t.Errorf("anchors lost behind the attachment: uuid=%q usage=%+v", e.RecordUUID, e.Usage)
	}
}

// A fork's copied prefix carries this turn's prompt id on every copied user
// record, so before the fork's own first answer lands (transcript lag, or an
// API error that spent nothing) the origin session's last call sits under
// this turn's id. Its prompt matches; the fork's own prompt record below it
// is what gives it away. Anchoring here would hand this answer the origin's
// uuid, message id and tokens.
func TestStopRefusesTheOriginsCallInAForksCopiedPrefix(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t,
		userRecord("u-copied", probePrompt, "the origin's question"),
		assistantRecord("msg_origin", "claude-fable-5", 120, 34),
		userRecord("u-own", probePrompt, "the fork's question"),
		fmt.Sprintf(`{"type":"attachment","sessionId":"s1","timestamp":"2026-08-17T10:00:00.500Z","uuid":"att1","attachment":{"type":"skill_listing","content":%q}}`, strings.Repeat("x", 200<<10)),
	)
	if _, err := c.Handle(HookEvent{
		HookEventName: "Stop", SessionID: "s1", PromptID: probePrompt, TranscriptPath: path,
		LastAssistantMessage: json.RawMessage(`"the fork's answer"`),
	}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.RecordUUID != "" || e.Usage != nil || e.Model != "" {
		t.Errorf("anchored to the origin's call: uuid=%q usage=%+v model=%q", e.RecordUUID, e.Usage, e.Model)
	}
	if e.Text != "the fork's answer" {
		t.Errorf("text = %q", e.Text)
	}
}

// Under lag with tool calls the last spent record in view is the turn's own
// tool-use call, with its tool result (carrying the turn's id) below it. It
// is the same turn, and still not the answer: the copy stays anchorless and
// the walk supplies the answer's own record.
func TestStopRefusesTheTurnsToolCallWhenTheAnswerHasNotLanded(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t,
		userRecord("u1", probePrompt, "go"),
		toolUseRecord("a0", "msg_toolcall"),
		toolResultRecord("u2", probePrompt),
	)
	if _, err := c.Handle(HookEvent{
		HookEventName: "Stop", SessionID: "s1", PromptID: probePrompt, TranscriptPath: path,
		LastAssistantMessage: json.RawMessage(`"done"`),
	}); err != nil {
		t.Fatal(err)
	}
	if e := r.events[0]; e.RecordUUID != "" || e.Usage != nil {
		t.Errorf("anchored to the tool call: uuid=%q usage=%+v", e.RecordUUID, e.Usage)
	}
}

// toolUseRecord is an assistant record that called a tool: spent, with a
// message id, and never the answer that ends a turn.
func toolUseRecord(uuid, msgID string) string {
	return fmt.Sprintf(
		`{"type":"assistant","sessionId":"s1","timestamp":"2026-08-17T10:00:00.000Z","uuid":%q,"requestId":"req_0","message":{"role":"assistant","model":"claude-fable-5","id":%q,"usage":{"input_tokens":100,"output_tokens":20},"content":[{"type":"text","text":"on it"},{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"echo hi"}}]}}`,
		uuid, msgID)
}

func toolResultRecord(uuid, promptID string) string {
	return fmt.Sprintf(
		`{"type":"user","sessionId":"s1","timestamp":"2026-08-17T10:00:01.000Z","uuid":%q,"promptId":%q,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"hi"}]}}`,
		uuid, promptID)
}

// Before the tool result reaches the file, the tool-use call is the last
// spent record and nothing below it names the prompt; it is still a call, not
// the answer, and the copy must not be anchored to it.
func TestStopNeverAnchorsToAToolUseCall(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t,
		userRecord("u1", probePrompt, "go"),
		toolUseRecord("a0", "msg_call"),
	)
	if _, err := c.Handle(HookEvent{
		HookEventName: "Stop", SessionID: "s1", PromptID: probePrompt, TranscriptPath: path,
		LastAssistantMessage: json.RawMessage(`"done"`),
	}); err != nil {
		t.Fatal(err)
	}
	if e := r.events[0]; e.RecordUUID != "" || e.Usage != nil {
		t.Errorf("anchored to the tool-use call: uuid=%q usage=%+v", e.RecordUUID, e.Usage)
	}
}

// A user record under the turn's id can follow the answer (a teammate
// message, a blocking Stop hook's feedback, a cut-off continuation), landing
// 40 to 170 ms after the Stop hooks' own records. The hook's read never sees
// it; a reader after the fact always does, and the copy's text and time say
// the record is still the turn's own answer.
func TestAnchorForOpensTheAmbiguousCaseOnTheCopysTextAndTime(t *testing.T) {
	path := transcriptWith(t,
		userRecord("u1", probePrompt, "go"),
		assistantRecord("msg_7", "claude-fable-5", 120, 34), // stamped 2026-08-17T10:00:00Z, text "done"
		userRecord("u2", probePrompt, "Another Claude session sent a message: carry on"),
	)
	at := time.Date(2026, 8, 17, 10, 0, 0, 300000000, time.UTC)
	if tail, ok := AnchorFor(path, probePrompt, &StopCopy{Text: "done", At: at}); !ok || tail.UUID != "a1" {
		t.Errorf("the turn's own answer was refused: %+v ok=%v", tail, ok)
	}
	if _, ok := AnchorFor(path, probePrompt, &StopCopy{Text: "something else", At: at}); ok {
		t.Error("anchored on a text mismatch (a fork's copied prefix, or a scrubbed answer)")
	}
	// The previous answer under a prompt that ended in Stop before is older
	// than the copy by a model call; the same text does not open it.
	if _, ok := AnchorFor(path, probePrompt, &StopCopy{Text: "done", At: at.Add(3 * time.Second)}); ok {
		t.Error("anchored to an answer three seconds older than the copy")
	}
	// Nor does a record from after the copy (a later Stop's answer).
	if _, ok := AnchorFor(path, probePrompt, &StopCopy{Text: "done", At: at.Add(-2 * time.Second)}); ok {
		t.Error("anchored to an answer two seconds newer than the copy")
	}
	// The hook's own read stays strict: under lag its tail can be the
	// previous answer, and what it stamps is never re-read.
	if _, ok := AnchorFor(path, probePrompt, nil); ok {
		t.Error("the hook's read took the ambiguous case")
	}
}

// Through the hook itself, with the copy's text equal to the record's and
// the clock within a second of the record's stamp, the ambiguous case is
// still refused: the hook's read must never open the door, because under lag
// its tail can be the previous answer and what it stamps is never re-read.
func TestStopThroughTheHookNeverOpensTheAmbiguousCase(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	c.SetClock(func() time.Time { return time.Date(2026, 8, 17, 10, 0, 0, 300000000, time.UTC) })
	path := transcriptWith(t,
		userRecord("u1", probePrompt, "go"),
		assistantRecord("msg_7", "claude-fable-5", 120, 34), // stamped 10:00:00Z, text "done"
		userRecord("u2", probePrompt, "Another Claude session sent a message: carry on"),
	)
	if _, err := c.Handle(HookEvent{
		HookEventName: "Stop", SessionID: "s1", PromptID: probePrompt, TranscriptPath: path,
		LastAssistantMessage: json.RawMessage(`"done"`),
	}); err != nil {
		t.Fatal(err)
	}
	if e := r.events[0]; e.RecordUUID != "" || e.Usage != nil {
		t.Errorf("the hook opened the ambiguous case: uuid=%q usage=%+v", e.RecordUUID, e.Usage)
	}
}

// The transcript lags the turn. When the tail's last model call belongs to a
// different prompt, naming it would pair this answer with the wrong transcript
// row and its usage would bill this turn another turn's tokens, so the whole
// tail is left off: no record uuid, no message id or usage, no model.
func TestStopLeavesTheTailAloneWhenItBelongsToAnotherPrompt(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t,
		userRecord("u1", "older-prompt", "first"),
		assistantRecord("msg_old", "claude-fable-5", 1, 2),
	)
	if _, err := c.Handle(HookEvent{HookEventName: "Stop", SessionID: "s1", PromptID: probePrompt, TranscriptPath: path}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.RecordUUID != "" {
		t.Fatalf("record_uuid = %q for a tail from another prompt; want empty", e.RecordUUID)
	}
	if e.Usage != nil || e.Model != "" {
		t.Fatalf("usage/model = %+v %q from another prompt's record; want none", e.Usage, e.Model)
	}
	if e.PromptID != probePrompt {
		t.Errorf("prompt_id = %q; the hook's own key must survive the mismatch", e.PromptID)
	}
}

// The hook names a prompt and the tail names none: the turn's user records
// sit above the tail window, or the harness stamps prompts but not the
// records in view. Either way the record in view may be the previous turn's
// answer, so it is not taken on trust; a harness that stamps the hook stamps
// the transcript, and an empty tail prompt is "out of window", not "agrees".
func TestStopLeavesTheTailAloneWhenItCannotNameItsPrompt(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t, assistantRecord("msg_prev", "claude-fable-5", 1, 2))
	if _, err := c.Handle(HookEvent{HookEventName: "Stop", SessionID: "s1", PromptID: probePrompt, TranscriptPath: path}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.RecordUUID != "" || e.Usage != nil || e.Model != "" {
		t.Fatalf("record_uuid=%q usage=%+v model=%q stamped from a tail that cannot name its prompt; want none", e.RecordUUID, e.Usage, e.Model)
	}
	if e.PromptID != probePrompt {
		t.Errorf("prompt_id = %q; the hook's own key must survive", e.PromptID)
	}
}

// A transcript from a harness that predates promptId, read by a hook without
// one, cannot disagree with itself; the record is stamped.
func TestStopStampsRecordUUIDWhenNeitherSideNamesAPrompt(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t, assistantRecord("msg_7", "claude-fable-5", 1, 2))
	if _, err := c.Handle(HookEvent{HookEventName: "Stop", SessionID: "s1", TranscriptPath: path}); err != nil {
		t.Fatal(err)
	}
	if r.events[0].RecordUUID != "a1" {
		t.Fatalf("record_uuid = %q, want a1", r.events[0].RecordUUID)
	}
}

func TestSubagentStopCarriesTheSubagentsAnswer(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	if _, err := c.Handle(HookEvent{
		HookEventName: "SubagentStop", SessionID: "s1", AgentID: "a642e1e7c059e7d99", PromptID: probePrompt,
		LastAssistantMessage: json.RawMessage(`"SUB-OK with SEKRIT"`),
	}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.Type != event.SubagentEnd || e.AgentID != "a642e1e7c059e7d99" {
		t.Fatalf("wrong event: %+v", e)
	}
	if e.Text != "SUB-OK with [REDACTED:test]" {
		t.Fatalf("text = %q, want the scrubbed last_assistant_message", e.Text)
	}
}

func TestSessionStartCarriesTheLauncherFromTheEnvironment(t *testing.T) {
	t.Setenv("CLAUDE_CODE_ENTRYPOINT", "cli")
	t.Setenv("__CFBundleIdentifier", "dev.warp.Warp-Stable")
	t.Setenv("TERM_PROGRAM", "WarpTerminal")
	r := &recorder{}
	c := newCap(t, r)
	c.SetLauncher(launcherFromEnv)
	if _, err := c.Handle(HookEvent{HookEventName: "SessionStart", SessionID: "s1", Source: "startup"}); err != nil {
		t.Fatal(err)
	}
	l := r.events[0].Launcher
	if l == nil {
		t.Fatal("no launcher on SessionStart")
	}
	if l.Entrypoint != "cli" || l.BundleID != "dev.warp.Warp-Stable" || l.Term != "WarpTerminal" {
		t.Fatalf("launcher = %+v", l)
	}
	// The parent of this test process is whatever ran `go test`; the probe
	// must answer without forking and without failing. An empty answer is
	// allowed on platforms with no non-forking source.
	t.Logf("parent_comm = %q", l.ParentComm)

	// A GUI host that sets nothing still produces a launcher, so the server
	// can tell "looked and found nothing" from an old client.
	t.Setenv("CLAUDE_CODE_ENTRYPOINT", "")
	t.Setenv("__CFBundleIdentifier", "")
	t.Setenv("TERM_PROGRAM", "")
	r2 := &recorder{}
	c2 := newCap(t, r2)
	c2.SetLauncher(func() *event.Launcher { return launcherFromEnv() })
	_, _ = c2.Handle(HookEvent{HookEventName: "SessionStart", SessionID: "s2", Source: "startup"})
	if r2.events[0].Launcher == nil {
		t.Fatal("an empty environment dropped the launcher entirely")
	}
}

func TestSessionEndReportsWhetherATranscriptExistsAndTheHarnessTitle(t *testing.T) {
	cases := []struct {
		name   string
		path   func(t *testing.T) string
		exists bool
		title  string
	}{
		{"no path", func(*testing.T) string { return "" }, false, ""},
		{"a path to nothing", func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent.jsonl") }, false, ""},
		{"an empty file", func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "empty.jsonl")
			_ = os.WriteFile(p, nil, 0o644)
			return p
		}, false, ""},
		{"a transcript with a title", func(t *testing.T) string {
			return transcriptWith(t,
				`{"type":"last-prompt","sessionId":"s1","lastPrompt":"x"}`,
				`{"type":"ai-title","aiTitle":"Create gantry-fixture.txt","sessionId":"s1"}`,
				userRecord("u1", probePrompt, "create the fixture"),
			)
		}, true, "Create gantry-fixture.txt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{}
			c := newCap(t, r)
			if _, err := c.Handle(HookEvent{HookEventName: "SessionEnd", SessionID: "s1", Reason: "other", TranscriptPath: tc.path(t)}); err != nil {
				t.Fatal(err)
			}
			e := r.events[0]
			if e.TranscriptExists == nil {
				t.Fatal("transcript_exists not reported")
			}
			if *e.TranscriptExists != tc.exists {
				t.Errorf("transcript_exists = %v, want %v", *e.TranscriptExists, tc.exists)
			}
			if e.HarnessTitle != tc.title {
				t.Errorf("harness_title = %q, want %q", e.HarnessTitle, tc.title)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Output caps
// ---------------------------------------------------------------------------

// A tool that prints megabytes used to cost the whole turn: the item was
// refused by the server for size and quarantined on the laptop. Now the text
// is cut at the cap and says so.
func TestToolOutputIsCappedAndFlagged(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	big := strings.Repeat("y", backfill.MaxToolOutputBytes+4096)
	resp, _ := json.Marshal(map[string]any{"stdout": big})
	if _, err := c.Handle(HookEvent{HookEventName: "PostToolUse", SessionID: "s1", ToolName: "Bash", ToolResponse: resp}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if len(e.Tool.Output) != backfill.MaxToolOutputBytes || !e.Tool.Truncated {
		t.Fatalf("output len=%d truncated=%v, want %d and true", len(e.Tool.Output), e.Tool.Truncated, backfill.MaxToolOutputBytes)
	}
	small, _ := json.Marshal(map[string]any{"stdout": "short"})
	r2 := &recorder{}
	_, _ = newCap(t, r2).Handle(HookEvent{HookEventName: "PostToolUse", SessionID: "s1", ToolName: "Bash", ToolResponse: small})
	if r2.events[0].Tool.Truncated {
		t.Fatal("a short output was flagged truncated")
	}
}

func TestDiffSidesAreCappedIndependentlyAndFlagged(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	before := strings.Repeat("a", backfill.MaxDiffSideBytes+10)
	resp, _ := json.Marshal(map[string]any{"filePath": "/repo/big.lock", "originalFile": before, "newString": "small"})
	if _, err := c.Handle(HookEvent{HookEventName: "PostToolUse", SessionID: "s1", ToolName: "Edit", ToolResponse: resp}); err != nil {
		t.Fatal(err)
	}
	var d *event.Diff
	for _, e := range r.events {
		if e.Type == event.FileChanged {
			d = e.Tool.Diff
		}
	}
	if d == nil {
		t.Fatal("no file_changed event")
	}
	if len(d.Before) != backfill.MaxDiffSideBytes || d.After != "small" || !d.Truncated {
		t.Fatalf("before=%d after=%q truncated=%v", len(d.Before), d.After, d.Truncated)
	}
}

// The cap cuts on a rune boundary: a UTF-8 sequence split in half is text
// Postgres refuses.
func TestCapTextKeepsValidUTF8(t *testing.T) {
	s := strings.Repeat("e\u0301", 100) // e plus combining acute: 3 bytes, 2 runes
	out, cut := backfill.CapText(s, 100)
	if !cut || len(out) > 100 || !json.Valid(mustJSON(out)) {
		t.Fatalf("len=%d cut=%v", len(out), cut)
	}
	for i := 0; i < len(out); {
		_, size := decodeRune(out[i:])
		if size == 0 {
			t.Fatal("cut produced an invalid rune")
		}
		i += size
	}
}

func mustJSON(s string) []byte {
	b, _ := json.Marshal(s)
	return b
}

func decodeRune(s string) (rune, int) {
	for _, r := range s {
		return r, len(string(r))
	}
	return 0, 0
}

// ---------------------------------------------------------------------------
// The ledger
// ---------------------------------------------------------------------------

func TestLedgerRecordsMarkersAndCapturedAnchors(t *testing.T) {
	l := Ledger{Dir: t.TempDir()}
	clear, err := l.Inflight(Marker{SessionID: "s1", HookEvent: "Stop", PromptID: probePrompt})
	if err != nil {
		t.Fatal(err)
	}
	if !l.HasInflight("s1") {
		t.Fatal("marker not visible while in flight")
	}
	evs := []event.Event{
		{Type: event.UserPrompt, PromptID: probePrompt, Seq: 1, OccurredAt: time.Now()},
		{Type: event.ToolCall, PromptID: probePrompt, ToolUseID: "toolu_1", Seq: 2},
		{Type: event.AssistantTurn, PromptID: probePrompt, RecordUUID: "a1", Seq: 3},
	}
	if err := l.Captured("s1", EntriesOf(evs)); err != nil {
		t.Fatal(err)
	}
	clear()
	c, err := l.Read("s1")
	if err != nil {
		t.Fatal(err)
	}
	if !c.PromptIDs[probePrompt] || !c.ToolCallIDs["toolu_1"] || !c.RecordUUIDs["a1"] {
		t.Fatalf("anchors not read back: %+v", c)
	}
	// A tool_call vouches for the PreToolUse alone; the result set is fed by
	// tool_result and tool_failed lines only.
	if c.ToolResultIDs["toolu_1"] {
		t.Fatalf("a tool_call entry landed in the result set: %+v", c)
	}
	if c.Types[event.UserPrompt] != 1 || c.Types[event.ToolCall] != 1 {
		t.Fatalf("types = %v", c.Types)
	}
	if len(c.Inflight) != 0 || l.HasInflight("s1") {
		t.Fatal("a cleared marker is still reported")
	}
	// A marker whose hook never cleared it is reported for recovery.
	_, _ = l.Inflight(Marker{SessionID: "s1", HookEvent: "PostToolUse", ToolUseID: "toolu_2"})
	c, _ = l.Read("s1")
	if len(c.Inflight) != 1 || c.Inflight[0].ToolUseID != "toolu_2" {
		t.Fatalf("inflight = %+v", c.Inflight)
	}
	if c, _ := l.Read("never"); len(c.PromptIDs) != 0 || len(c.Inflight) != 0 {
		t.Fatal("an unknown session must read as empty")
	}
}

// A session that started and ended without a prompt is an empty start; one
// with a prompt is not; one older than the window is not counted.
func TestLedgerCountsEmptyStarts(t *testing.T) {
	l := Ledger{Dir: t.TempDir()}
	now := time.Now()
	_ = l.Captured("empty", EntriesOf([]event.Event{{Type: event.SessionStarted}, {Type: event.SessionEnded}}))
	_ = l.Captured("real", EntriesOf([]event.Event{{Type: event.SessionStarted}, {Type: event.UserPrompt}, {Type: event.SessionEnded}}))
	_ = l.Captured("open", EntriesOf([]event.Event{{Type: event.SessionStarted}}))
	_ = l.Captured("old", EntriesOf([]event.Event{{Type: event.SessionStarted}, {Type: event.SessionEnded}}))
	stale := now.Add(-48 * time.Hour)
	_ = os.Chtimes(filepath.Join(l.Dir, "old.captured"), stale, stale)

	if n := l.EmptyStarts(now, 24*time.Hour); n != 1 {
		t.Fatalf("empty starts = %d, want 1", n)
	}
	if n := l.Sweep(24*time.Hour, now); n != 1 {
		t.Fatalf("swept %d, want the one old ledger", n)
	}
	if _, err := os.Stat(filepath.Join(l.Dir, "real.captured")); err != nil {
		t.Fatal("a recent ledger was swept")
	}
}

// A hook that named no prompt takes the tail on trust, and a tool-use call
// is still not an answer there: the tool-use clause comes before the trust.
func TestAnchorForRefusesAToolUseTailWithoutAPrompt(t *testing.T) {
	path := transcriptWith(t,
		userRecord("u1", probePrompt, "go"),
		toolUseRecord("a0", "msg_call"),
	)
	if tail, ok := AnchorFor(path, "", nil); ok {
		t.Errorf("a tool-use call was taken on trust: %+v", tail)
	}
	path = transcriptWith(t,
		userRecord("u1", probePrompt, "go"),
		assistantRecord("msg_7", "claude-fable-5", 120, 34),
	)
	if tail, ok := AnchorFor(path, "", nil); !ok || tail.UUID != "a1" {
		t.Errorf("a plain answer was not taken on trust: %+v ok=%v", tail, ok)
	}
}
