package capture

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

type recorder struct {
	mu     sync.Mutex
	events []event.Event
	err    error
}

func (r *recorder) Put(e event.Event) error {
	if r.err != nil {
		return r.err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

// stubScrub redacts a single sentinel so tests can prove scrubbing ran on a
// given field without depending on the real rule set.
func stubScrub(s string) (string, map[string]int) {
	if strings.Contains(s, "SEKRIT") {
		return strings.ReplaceAll(s, "SEKRIT", "[REDACTED:test]"), map[string]int{"test": strings.Count(s, "SEKRIT")}
	}
	return s, nil
}

func newCap(t *testing.T, r *recorder) *Capturer {
	t.Helper()
	var n int64
	c, err := New(r, stubScrub, func(string) int64 { n++; return n })
	if err != nil {
		t.Fatal(err)
	}
	c.SetClock(func() time.Time { return time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC) })
	return c
}

func TestRefusesToConstructWithoutScrubber(t *testing.T) {
	// Shipping unscrubbed text is the failure this package exists to prevent,
	// so a missing scrubber must be loud rather than permissive.
	if _, err := New(&recorder{}, nil, func(string) int64 { return 1 }); err == nil {
		t.Fatal("expected construction to fail without a scrubber")
	}
}

func TestRefusesHookWithoutSessionID(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	if _, err := c.Handle(HookEvent{HookEventName: "SessionStart"}); err == nil {
		t.Fatal("expected an error when session_id is absent")
	}
}

func TestUserPromptIsCapturedAndScrubbed(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	n, err := c.Handle(HookEvent{
		HookEventName: "UserPromptSubmit",
		SessionID:     "s1",
		Prompt:        "deploy with token SEKRIT now",
		Cwd:           "/repo",
		Version:       "2.1.221",
	})
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	e := r.events[0]
	if e.Type != event.UserPrompt {
		t.Fatalf("type = %q", e.Type)
	}
	if strings.Contains(e.Text, "SEKRIT") {
		t.Fatal("prompt was not scrubbed")
	}
	if e.Redactions["test"] != 1 {
		t.Fatalf("redaction not counted: %v", e.Redactions)
	}
	if e.Origin != event.OriginHook || e.HarnessVersion != "2.1.221" || e.Cwd != "/repo" {
		t.Fatalf("context lost: %+v", e)
	}
}

// The headline capability: a hook payload carries the real before/after for an
// edit, so the archive records what was done, not just what was asked.
func TestEditProducesToolResultAndFileChangedWithRealDiff(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	resp, _ := json.Marshal(map[string]any{
		"filePath":        "/repo/main.go",
		"originalFile":    "package main\n",
		"newString":       "package main\n\nfunc main() {}\n",
		"structuredPatch": []map[string]any{{"oldStart": 1, "lines": []string{"+func main() {}"}}},
	})
	n, err := c.Handle(HookEvent{
		HookEventName: "PostToolUse",
		SessionID:     "s1",
		ToolName:      "Edit",
		ToolResponse:  resp,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("emitted %d events, want tool_result + file_changed", n)
	}
	var fc *event.Event
	for i := range r.events {
		if r.events[i].Type == event.FileChanged {
			fc = &r.events[i]
		}
	}
	if fc == nil {
		t.Fatal("no file_changed event emitted")
	}
	d := fc.Tool.Diff
	if d == nil || d.Path != "/repo/main.go" {
		t.Fatalf("diff missing or wrong path: %+v", d)
	}
	if d.Before != "package main\n" || !strings.Contains(d.After, "func main") {
		t.Fatalf("before/after not captured: %+v", d)
	}
	if d.Created {
		t.Fatal("an edit with an originalFile is not a creation")
	}
	if len(d.Patch) == 0 {
		t.Fatal("structured patch dropped")
	}
}

// A creation reports originalFile:null. Empty Before alone cannot distinguish
// this from editing an empty file, so the flag has to come from the null.
func TestFileCreationIsDistinguishedFromEmptyEdit(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	resp := json.RawMessage(`{"filePath":"/repo/new.txt","originalFile":null,"content":"hello"}`)
	if _, err := c.Handle(HookEvent{HookEventName: "PostToolUse", SessionID: "s1", ToolName: "Write", ToolResponse: resp}); err != nil {
		t.Fatal(err)
	}
	for _, e := range r.events {
		if e.Type == event.FileChanged {
			if !e.Tool.Diff.Created {
				t.Fatal("creation not flagged")
			}
			return
		}
	}
	t.Fatal("no file_changed event")
}

// Output over ~30k chars is elided in the payload but spooled to disk. Losing
// the pointer means silently archiving the truncated version of exactly the
// outputs that were long enough to matter.
func TestTruncatedOutputRecordsSidecarPath(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	resp := json.RawMessage(`{"stdout":"first 30k...","persistedOutputPath":"/p/s1/tool-results/abc.txt"}`)
	if _, err := c.Handle(HookEvent{HookEventName: "PostToolUse", SessionID: "s1", ToolName: "Bash", ToolResponse: resp}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.Tool.OutputPath != "/p/s1/tool-results/abc.txt" || !e.Tool.Truncated {
		t.Fatalf("sidecar not recorded: %+v", e.Tool)
	}
}

func TestToolOutputIsScrubbed(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	resp := json.RawMessage(`{"stdout":"env shows SEKRIT here"}`)
	if _, err := c.Handle(HookEvent{HookEventName: "PostToolUse", SessionID: "s1", ToolName: "Bash", ToolResponse: resp}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.events[0].Tool.Output, "SEKRIT") {
		t.Fatal("tool output was not scrubbed")
	}
}

func TestToolInputIsScrubbedAndStaysValidJSON(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	in := json.RawMessage(`{"command":"export TOKEN=SEKRIT"}`)
	if _, err := c.Handle(HookEvent{HookEventName: "PreToolUse", SessionID: "s1", ToolName: "Bash", ToolInput: in}); err != nil {
		t.Fatal(err)
	}
	got := r.events[0].Tool.Input
	if strings.Contains(string(got), "SEKRIT") {
		t.Fatal("tool input was not scrubbed")
	}
	if !json.Valid(got) {
		t.Fatalf("scrubbing produced invalid json: %s", got)
	}
}

func TestToolFailureIsItsOwnType(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	if _, err := c.Handle(HookEvent{
		HookEventName: "PostToolUseFailure", SessionID: "s1", ToolName: "Bash", Error: "exit 1",
	}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.Type != event.ToolFailed || e.Tool.Error != "exit 1" {
		t.Fatalf("failure not captured: %+v", e)
	}
}

func TestLifecycleHooksMapToEventTypes(t *testing.T) {
	cases := []struct {
		hook string
		want event.Type
	}{
		{"SessionStart", event.SessionStarted},
		{"SessionEnd", event.SessionEnded},
		{"Stop", event.AssistantTurn},
		{"SubagentStart", event.SubagentStart},
		{"SubagentStop", event.SubagentEnd},
		{"PreCompact", event.Compaction},
	}
	for _, tc := range cases {
		t.Run(tc.hook, func(t *testing.T) {
			r := &recorder{}
			c := newCap(t, r)
			if _, err := c.Handle(HookEvent{HookEventName: tc.hook, SessionID: "s1"}); err != nil {
				t.Fatal(err)
			}
			if len(r.events) != 1 || r.events[0].Type != tc.want {
				t.Fatalf("hook %q produced %v", tc.hook, r.events)
			}
		})
	}
}

// The hook set has grown to 31 events and keeps growing. A client that errored
// on an unknown name would break every laptop the day the harness ships a new
// one, so unknown hooks are ignored rather than fatal.
func TestUnknownHookIsIgnoredNotFatal(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	n, err := c.Handle(HookEvent{HookEventName: "SomeFutureHook", SessionID: "s1"})
	if err != nil {
		t.Fatalf("unknown hook should not error: %v", err)
	}
	if n != 0 || len(r.events) != 0 {
		t.Fatalf("unknown hook emitted %d events", n)
	}
}

func TestSubagentIdentityIsPreserved(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	// Subagent transcripts carry the PARENT session id, so without agent and
	// workflow ids their events are indistinguishable from the parent's.
	if _, err := c.Handle(HookEvent{
		HookEventName: "SubagentStart", SessionID: "parent-1",
		AgentID: "agent-abc", WorkflowID: "wf_9",
	}); err != nil {
		t.Fatal(err)
	}
	e := r.events[0]
	if e.SessionID != "parent-1" || e.AgentID != "agent-abc" || e.WorkflowID != "wf_9" {
		t.Fatalf("subagent identity lost: %+v", e)
	}
}

func TestEveryEventIsValidAndCarriesAnID(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	hooks := []string{"SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop", "SessionEnd"}
	for _, h := range hooks {
		if _, err := c.Handle(HookEvent{HookEventName: h, SessionID: "s1", ToolName: "Bash"}); err != nil {
			t.Fatalf("%s: %v", h, err)
		}
	}
	seen := map[string]bool{}
	for _, e := range r.events {
		if err := e.Validate(); err != nil {
			t.Fatalf("invalid event emitted: %v", err)
		}
		if seen[e.ID] {
			t.Fatalf("duplicate id %q", e.ID)
		}
		seen[e.ID] = true
	}
}

func TestSequenceNumbersAreMonotonicWithinSession(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	for range 5 {
		_, _ = c.Handle(HookEvent{HookEventName: "UserPromptSubmit", SessionID: "s1", Prompt: "x"})
	}
	for i := 1; i < len(r.events); i++ {
		if r.events[i].Seq <= r.events[i-1].Seq {
			t.Fatalf("sequence not monotonic: %d then %d", r.events[i-1].Seq, r.events[i].Seq)
		}
	}
}

func TestSinkErrorPropagates(t *testing.T) {
	r := &recorder{err: errSink}
	c := newCap(t, r)
	if _, err := c.Handle(HookEvent{HookEventName: "SessionStart", SessionID: "s1"}); err == nil {
		t.Fatal("sink failure should propagate so the caller can retry")
	}
}

var errSink = &sinkErr{}

type sinkErr struct{}

func (*sinkErr) Error() string { return "sink unavailable" }

func TestFileSeqSurvivesProcessRestart(t *testing.T) {
	// Each hook invocation is its own process, so the counter has to live on
	// disk or every event in a session would be numbered 1.
	dir := t.TempDir()
	f := FileSeq{Dir: dir}
	var last int64
	for range 5 {
		n := f.Next("session-a")
		if n <= last {
			t.Fatalf("sequence went backwards: %d then %d", last, n)
		}
		last = n
	}
	fresh := FileSeq{Dir: dir} // simulates a new process
	if n := fresh.Next("session-a"); n != last+1 {
		t.Fatalf("counter did not persist: got %d, want %d", n, last+1)
	}
}

func TestFileSeqIsolatesSessions(t *testing.T) {
	f := FileSeq{Dir: t.TempDir()}
	if a, b := f.Next("s-one"), f.Next("s-two"); a != 1 || b != 1 {
		t.Fatalf("sessions share a counter: %d %d", a, b)
	}
}

func TestFileSeqIsConcurrencySafe(t *testing.T) {
	f := FileSeq{Dir: t.TempDir()}
	const n = 25
	got := make([]int64, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) { defer wg.Done(); got[i] = f.Next("s") }(i)
	}
	wg.Wait()
	seen := map[int64]bool{}
	for _, v := range got {
		if seen[v] {
			t.Fatalf("duplicate sequence number %d handed out", v)
		}
		seen[v] = true
	}
}

// A session id reaches us from the harness; it must never be able to escape the
// state directory.
func TestFileSeqSanitizesHostileSessionIDs(t *testing.T) {
	dir := t.TempDir()
	f := FileSeq{Dir: dir}
	if n := f.Next("../../etc/passwd"); n != 1 {
		t.Fatalf("unexpected sequence %d", n)
	}
	if n := f.Next("../../etc/passwd"); n != 2 {
		t.Fatalf("counter not persisted for sanitized id: %d", n)
	}
}

func TestFileSeqWithoutDirStillReturnsUsableNumbers(t *testing.T) {
	f := FileSeq{}
	// Degrading to a timestamp rather than failing: a mis-ordered event is
	// recoverable at query time, a lost one is not.
	if n := f.Next("s"); n <= 0 {
		t.Fatalf("expected a usable fallback sequence, got %d", n)
	}
}

// The live path's tokens.
//
// A hook payload has no token accounting in it, so before this a session
// captured live carried prompts, tools and turns with a structural zero beside
// them, and only a later walk of the same transcript ever priced it. Whole
// fleets sat at $0.00 for days on end without anything failing.

// transcriptWith writes a Claude Code transcript and returns its path.
func transcriptWith(t *testing.T, lines ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "transcript.jsonl")
	if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func assistantRecord(msgID, model string, in, out int64) string {
	return fmt.Sprintf(
		`{"type":"assistant","sessionId":"s1","timestamp":"2026-08-17T10:00:00.000Z","uuid":"a1","requestId":"req_1","message":{"role":"assistant","model":%q,"id":%q,"usage":{"input_tokens":%d,"output_tokens":%d,"cache_read_input_tokens":5},"content":[{"type":"text","text":"done"}]}}`,
		model, msgID, in, out)
}

func TestStopCarriesTheTurnsTokensFromTheTranscript(t *testing.T) {
	r := &recorder{}
	c := newCap(t, r)
	path := transcriptWith(t, assistantRecord("msg_7", "claude-fable-5", 120, 34))

	if _, err := c.Handle(HookEvent{
		HookEventName:  "Stop",
		SessionID:      "s1",
		TranscriptPath: path,
		Message:        json.RawMessage(`{"role":"assistant"}`),
	}); err != nil {
		t.Fatal(err)
	}

	e := r.events[0]
	if e.Usage == nil {
		t.Fatal("a Stop with a transcript beside it still reported no tokens")
	}
	if e.Usage.InputTokens != 120 || e.Usage.OutputTokens != 34 || e.Usage.CacheReadTokens != 5 {
		t.Errorf("usage = %+v, want 120/34/5", e.Usage)
	}
	// The model has to ride along or the tokens cannot be priced: the pricer
	// takes its rate from the event's model and returns zero without one.
	if e.Model != "claude-fable-5" {
		t.Errorf("model = %q, want the transcript's", e.Model)
	}
	// The message id is the ledger's dedup key. Carrying it is what lets the
	// live event and a later transcript walk of the same call converge on one
	// ledger row instead of billing the turn twice.
	if e.Usage.MessageID != "msg_7" {
		t.Errorf("message id = %q, want msg_7", e.Usage.MessageID)
	}
}

// A missing, unreadable or usage-free transcript must cost the turn nothing.
// This runs inside somebody's Stop hook; a transcript that is not there yet is
// an ordinary Tuesday, not a reason to fail a capture.
func TestStopWithoutUsableUsageStillCapturesTheTurn(t *testing.T) {
	cases := []struct {
		name string
		path func(t *testing.T) string
	}{
		{"no transcript path at all", func(*testing.T) string { return "" }},
		{"a path to nothing", func(t *testing.T) string {
			return filepath.Join(t.TempDir(), "absent.jsonl")
		}},
		{"a transcript whose last turn is synthetic", func(t *testing.T) string {
			return transcriptWith(t, assistantRecord("msg_s", "<synthetic>", 900, 900))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{}
			c := newCap(t, r)
			if _, err := c.Handle(HookEvent{
				HookEventName: "Stop", SessionID: "s1", TranscriptPath: tc.path(t),
			}); err != nil {
				t.Fatal(err)
			}
			if len(r.events) != 1 || r.events[0].Type != event.AssistantTurn {
				t.Fatalf("the turn itself was lost: %v", r.events)
			}
			if r.events[0].Usage != nil {
				t.Errorf("usage = %+v, want none", r.events[0].Usage)
			}
		})
	}
}
