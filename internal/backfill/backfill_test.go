package backfill

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const (
	sessA = "aaaaaaaa-1111-2222-3333-444444444444"
	sessB = "bbbbbbbb-1111-2222-3333-444444444444"

	// Fixed instants well in the past, so a test that accidentally stamps
	// time.Now() fails loudly rather than looking plausible.
	ts1 = "2026-03-01T09:00:00.000Z"
	ts2 = "2026-03-01T09:00:05.500Z"
	ts3 = "2026-03-01T09:01:00.000Z"
	ts4 = "2026-03-02T14:30:00.000Z"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("bad fixture time %q: %v", s, err)
	}
	return ts.UTC()
}

// tree builds a synthetic discovery root.
type tree struct {
	t    *testing.T
	root string
}

func newTree(t *testing.T) *tree {
	t.Helper()
	return &tree{t: t, root: t.TempDir()}
}

// write creates a file at rel (slash-separated, relative to the root) whose
// contents are the given lines joined by newlines.
func (tr *tree) write(rel string, lines ...string) string {
	tr.t.Helper()
	p := filepath.Join(tr.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		tr.t.Fatalf("mkdir for %s: %v", rel, err)
	}
	body := strings.Join(lines, "\n")
	if body != "" {
		body += "\n"
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		tr.t.Fatalf("write %s: %v", rel, err)
	}
	return p
}

// userLine is a typed user prompt, whose content is a bare string.
func userLine(session, ts, uuid, text string) string {
	return fmt.Sprintf(
		`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":%q,"cwd":"/repo","gitBranch":"main","version":"2.1.221","message":{"role":"user","content":%q}}`,
		session, ts, uuid, text)
}

// assistantLine is a model turn with prose and usage.
func assistantLine(session, ts, uuid, model, text string) string {
	return fmt.Sprintf(
		`{"type":"assistant","sessionId":%q,"timestamp":%q,"uuid":%q,"cwd":"/repo","version":"2.1.221","requestId":"req_1","message":{"role":"assistant","model":%q,"id":"msg_1","usage":{"input_tokens":10,"output_tokens":20,"cache_read_input_tokens":300,"cache_creation_input_tokens":40,"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":10},"service_tier":"standard"},"content":[{"type":"text","text":%q}]}}`,
		session, ts, uuid, model, text)
}

// toolUseLine is an assistant turn that calls a tool.
func toolUseLine(session, ts, uuid, toolID, name string) string {
	return fmt.Sprintf(
		`{"type":"assistant","sessionId":%q,"timestamp":%q,"uuid":%q,"message":{"role":"assistant","model":"claude-fable-5","content":[{"type":"tool_use","id":%q,"name":%q,"input":{"file_path":"/repo/x.go"}}]}}`,
		session, ts, uuid, toolID, name)
}

// toolResultLine is the user-role record carrying a tool's output.
func toolResultLine(session, ts, uuid, toolID, out string, isErr bool) string {
	return fmt.Sprintf(
		`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":%q,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":%q,"content":%q,"is_error":%t}]}}`,
		session, ts, uuid, toolID, out, isErr)
}

// collectEvents runs a walk and returns everything it emitted.
func collectEvents(t *testing.T, opts Options) ([]event.Event, Result) {
	t.Helper()
	var got []event.Event
	res, err := Walk(opts, func(e event.Event) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	return got, res
}

func skipReasons(res Result) string {
	var b strings.Builder
	for _, s := range res.Skipped {
		fmt.Fprintf(&b, "\n  %s: %s", filepath.Base(s.Path), s.Reason)
	}
	return b.String()
}

func hasSkip(res Result, base, substr string) bool {
	for _, s := range res.Skipped {
		if filepath.Base(s.Path) == base && strings.Contains(s.Reason, substr) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// The headline property: event time is transcript time
// ---------------------------------------------------------------------------

func TestOriginalTimestampsPreserved(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "first"),
		assistantLine(sessA, ts2, "a1", "claude-fable-5", "reply"),
		userLine(sessA, ts3, "u2", "second"),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})
	if len(got) == 0 {
		t.Fatal("no events emitted")
	}

	want := map[string]time.Time{
		"u1": mustTime(t, ts1),
		"a1": mustTime(t, ts2),
		"u2": mustTime(t, ts3),
	}
	// Every event must carry one of the fixture instants, and nothing may be
	// stamped anywhere near now.
	now := time.Now()
	for _, e := range got {
		var matched bool
		for _, w := range want {
			if e.OccurredAt.Equal(w) {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("event %s has OccurredAt %s, which is not a fixture timestamp", e.Type, e.OccurredAt)
		}
		if now.Sub(e.OccurredAt) < time.Hour {
			t.Errorf("event %s appears stamped at import time (%s)", e.Type, e.OccurredAt)
		}
		if e.Origin != event.OriginTranscript {
			t.Errorf("Origin = %q, want %q", e.Origin, event.OriginTranscript)
		}
		if e.Source != event.SourceClaudeCode {
			t.Errorf("Source = %q, want %q", e.Source, event.SourceClaudeCode)
		}
	}
}

func TestEveryEmittedEventValidates(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "hello"),
		assistantLine(sessA, ts2, "a1", "claude-fable-5", "hi"),
		toolUseLine(sessA, ts2, "a2", "toolu_1", "Read"),
		toolResultLine(sessA, ts3, "u2", "toolu_1", "file contents", false),
	)
	tr.write("proj/"+sessA+"/subagents/agent-worker.jsonl",
		assistantLine(sessA, ts3, "s1", "claude-fable-5", "sub"),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})
	if len(got) == 0 {
		t.Fatal("no events emitted")
	}
	for _, e := range got {
		if err := e.Validate(); err != nil {
			t.Errorf("emitted event fails Validate: %v (type=%s seq=%d)", err, e.Type, e.Seq)
		}
	}
}

// A record with no timestamp must inherit the previous one rather than being
// stamped now, and the walk must say that it did.
func TestMissingTimestampCarriesForward(t *testing.T) {
	tr := newTree(t)
	// A last-prompt record is the real-world case: it carries a sessionId but
	// no timestamp. Use a content-bearing record so it produces an event.
	noTS := fmt.Sprintf(
		`{"type":"assistant","sessionId":%q,"uuid":"a2","message":{"role":"assistant","model":"claude-fable-5","content":[{"type":"text","text":"no clock"}]}}`,
		sessA)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "first"),
		noTS,
	)

	got, res := collectEvents(t, Options{Root: tr.root})

	var found bool
	for _, e := range got {
		if e.Text == "no clock" {
			found = true
			if !e.OccurredAt.Equal(mustTime(t, ts1)) {
				t.Errorf("carried timestamp = %s, want %s", e.OccurredAt, ts1)
			}
		}
	}
	if !found {
		t.Fatal("the timestamp-less record produced no event")
	}
	if !hasSkip(res, sessA+".jsonl", "carried forward") {
		t.Errorf("carry-forward not recorded in Skipped; got:%s", skipReasons(res))
	}
}

// With no previous timestamp there is nothing to carry, and fabricating one
// would be worse than dropping the record.
func TestMissingTimestampWithNoPriorIsDropped(t *testing.T) {
	tr := newTree(t)
	noTS := fmt.Sprintf(
		`{"type":"assistant","sessionId":%q,"uuid":"a1","message":{"role":"assistant","model":"claude-fable-5","content":[{"type":"text","text":"orphan"}]}}`,
		sessA)
	tr.write("proj/"+sessA+".jsonl", noTS)

	got, _ := collectEvents(t, Options{Root: tr.root})
	for _, e := range got {
		if e.Text == "orphan" {
			t.Fatalf("record with no clock and no predecessor was emitted at %s", e.OccurredAt)
		}
	}
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

func TestDeterministicIDsAcrossRuns(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "hello"),
		assistantLine(sessA, ts2, "a1", "claude-fable-5", "hi"),
		toolUseLine(sessA, ts2, "a2", "toolu_1", "Read"),
	)
	tr.write("proj/"+sessA+"/subagents/workflows/wf_abc/agent-w1.jsonl",
		assistantLine(sessA, ts3, "s1", "claude-fable-5", "sub"),
	)

	first, _ := collectEvents(t, Options{Root: tr.root})
	second, _ := collectEvents(t, Options{Root: tr.root})

	if len(first) != len(second) {
		t.Fatalf("event count differs between runs: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Errorf("event %d id differs: %s vs %s", i, first[i].ID, second[i].ID)
		}
		if first[i].Seq != second[i].Seq {
			t.Errorf("event %d seq differs: %d vs %d", i, first[i].Seq, second[i].Seq)
		}
	}

	// Ids must also be unique, or dedup silently drops real events.
	seen := map[string]bool{}
	for _, e := range first {
		if seen[e.ID] {
			t.Errorf("duplicate id %s (type=%s seq=%d)", e.ID, e.Type, e.Seq)
		}
		seen[e.ID] = true
	}
}

// Narrowing Since must not renumber the events it still emits. If it did, a
// re-import over a wider window would duplicate instead of dedup. The window
// applies per file: the file with a record inside it is emitted whole, the
// file entirely older than it is not emitted at all.
func TestIDsIndependentOfSinceWindow(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "old"),
		assistantLine(sessA, ts2, "a1", "claude-fable-5", "old reply"),
	)
	tr.write("proj/"+sessB+".jsonl",
		userLine(sessB, ts1, "u1", "old start"),
		userLine(sessB, ts4, "u2", "new"),
	)

	all, _ := collectEvents(t, Options{Root: tr.root})
	narrowed, _ := collectEvents(t, Options{Root: tr.root, Since: mustTime(t, ts4)})

	if len(narrowed) == 0 {
		t.Fatal("Since window emitted nothing")
	}
	if len(narrowed) >= len(all) {
		t.Fatalf("Since did not filter: %d of %d", len(narrowed), len(all))
	}
	for _, e := range narrowed {
		if e.SessionID == sessA {
			t.Errorf("a file entirely older than the window was emitted: %s", e.Type)
		}
	}

	byID := map[string]event.Event{}
	for _, e := range all {
		byID[e.ID] = e
	}
	for _, e := range narrowed {
		full, ok := byID[e.ID]
		if !ok {
			t.Errorf("Since-filtered event id %s does not appear in the unfiltered run", e.ID)
			continue
		}
		if full.Seq != e.Seq {
			t.Errorf("seq differs under Since: %d vs %d", full.Seq, e.Seq)
		}
	}
}

// The window is a per-file decision, made on the file's newest record. A
// per-event window cut sessions in half: roughly 590 stored sessions began
// mid-conversation because their head predated an import window, and a
// session is either worth importing or it is not.
func TestSinceFiltersFilesByTheirNewestRecord(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "wholly old"),
		userLine(sessA, ts2, "u2", "still old"),
	)
	tr.write("proj/"+sessB+".jsonl",
		userLine(sessB, ts1, "u1", "old head"),
		userLine(sessB, ts4, "u2", "new tail"),
	)

	got, _ := collectEvents(t, Options{Root: tr.root, Since: mustTime(t, ts4)})
	var sawOldHead, sawNewTail bool
	for _, e := range got {
		switch {
		case e.SessionID == sessA:
			t.Errorf("a file whose newest record predates the window was emitted: %q", e.Text)
		case e.Text == "old head":
			sawOldHead = true
		case e.Text == "new tail":
			sawNewTail = true
		}
	}
	if !sawOldHead || !sawNewTail {
		t.Errorf("a file with a record in the window must be emitted whole: head=%v tail=%v", sawOldHead, sawNewTail)
	}
}

// ---------------------------------------------------------------------------
// Lineage
// ---------------------------------------------------------------------------

// logicalParentUuid is a compaction marker naming a record in the same file.
// Read as a parent session it produced 384 parents of which none resolved; it
// is carried as ParentRecordUUID and never as ParentSessionID.
func TestCompactionMarkerIsARecordNotASession(t *testing.T) {
	tr := newTree(t)
	line := fmt.Sprintf(
		`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":"u1","logicalParentUuid":%q,"message":{"role":"user","content":"resumed"}}`,
		sessB, ts3, "9f90859f-aaaa-bbbb-cccc-dddddddddddd")
	tr.write("proj/"+sessB+".jsonl", line)

	got, _ := collectEvents(t, Options{Root: tr.root})
	if len(got) == 0 {
		t.Fatal("no events")
	}
	for _, e := range got {
		if e.SessionID != sessB {
			t.Errorf("SessionID = %q, want %q", e.SessionID, sessB)
		}
		if e.ParentSessionID != "" || e.LineageSource != "" {
			t.Errorf("a compaction marker became lineage: parent=%q source=%q", e.ParentSessionID, e.LineageSource)
		}
		if e.ParentRecordUUID != "9f90859f-aaaa-bbbb-cccc-dddddddddddd" {
			t.Errorf("ParentRecordUUID = %q, want the marker", e.ParentRecordUUID)
		}
	}
}

// The continuation marker does not live on a content record. In real
// transcripts it appears only on type:"system" rows, which have no message body,
// and anywhere from line 7 to line 4948 rather than in a header. Reading it only
// where it appears attaches it to nothing, so it has to stick to the stream.
func TestContinuationMarkerOnSystemRecordSticksToStream(t *testing.T) {
	tr := newTree(t)
	systemMarker := fmt.Sprintf(
		`{"type":"system","subtype":"continuation","sessionId":%q,"timestamp":%q,"uuid":"s1","logicalParentUuid":%q,"level":"info"}`,
		sessB, ts2, sessA)
	tr.write("proj/"+sessB+".jsonl",
		userLine(sessB, ts1, "u1", "before the marker"),
		systemMarker,
		userLine(sessB, ts3, "u2", "after the marker"),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})

	var after bool
	for _, e := range got {
		if e.Text == "after the marker" {
			after = true
			if e.ParentRecordUUID != sessA {
				t.Errorf("ParentRecordUUID = %q after the marker, want %q", e.ParentRecordUUID, sessA)
			}
		}
		if e.Text == "before the marker" && e.ParentRecordUUID != "" {
			t.Errorf("the marker was applied to a record before it")
		}
	}
	if !after {
		t.Fatal("no event followed the continuation marker")
	}

	// The rollup must not turn the marker into lineage.
	var s event.Session
	for _, e := range got {
		s.Apply(e)
	}
	if s.ParentSessionID != "" {
		t.Errorf("rollup ParentSessionID = %q from a compaction marker, want empty", s.ParentSessionID)
	}
}

// A system record carries no message body, so it must not manufacture a turn.
func TestSystemRecordProducesNoTurn(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "real prompt"),
		fmt.Sprintf(`{"type":"system","subtype":"hook","sessionId":%q,"timestamp":%q,"uuid":"s1","level":"warn","hookCount":2}`, sessA, ts2),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})
	for _, e := range got {
		if e.Type == event.UserPrompt && e.Text == "" {
			t.Error("a system record produced an empty turn")
		}
		if e.Type == event.AssistantTurn && e.Text == "" && e.Usage == nil {
			t.Error("a system record produced a contentless assistant turn")
		}
	}
}

func TestSubagentLineage(t *testing.T) {
	cases := []struct {
		name         string
		rel          string
		wantAgent    string
		wantWorkflow string
		wantType     event.Type
	}{
		{
			name:      "flat subagent",
			rel:       "proj/" + sessA + "/subagents/agent-aflat123.jsonl",
			wantAgent: "agent-aflat123",
			wantType:  event.SubagentStart,
		},
		{
			name:         "nested workflow subagent",
			rel:          "proj/" + sessA + "/subagents/workflows/wf_733c9f64/agent-anested456.jsonl",
			wantAgent:    "agent-anested456",
			wantWorkflow: "wf_733c9f64",
			wantType:     event.SubagentStart,
		},
		{
			name:      "main transcript is not a subagent",
			rel:       "proj/" + sessA + ".jsonl",
			wantAgent: "",
			wantType:  event.SessionStarted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTree(t)
			// Subagent files carry the PARENT's sessionId; that is precisely why
			// AgentID has to come from the path.
			tr.write(tc.rel, assistantLine(sessA, ts1, "x1", "claude-fable-5", "work"))

			got, _ := collectEvents(t, Options{Root: tr.root})
			if len(got) == 0 {
				t.Fatal("no events")
			}
			for _, e := range got {
				if e.SessionID != sessA {
					t.Errorf("SessionID = %q, want parent %q", e.SessionID, sessA)
				}
				if e.AgentID != tc.wantAgent {
					t.Errorf("AgentID = %q, want %q", e.AgentID, tc.wantAgent)
				}
				if e.WorkflowID != tc.wantWorkflow {
					t.Errorf("WorkflowID = %q, want %q", e.WorkflowID, tc.wantWorkflow)
				}
			}
			if got[0].Type != tc.wantType {
				t.Errorf("first event type = %q, want %q", got[0].Type, tc.wantType)
			}
		})
	}
}

// Parent and subagent share a session id, so without AgentID their events would
// be indistinguishable — and their ids would collide.
func TestParentAndSubagentDoNotCollide(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		assistantLine(sessA, ts1, "same-uuid", "claude-fable-5", "parent work"),
	)
	tr.write("proj/"+sessA+"/subagents/agent-w1.jsonl",
		assistantLine(sessA, ts1, "same-uuid", "claude-fable-5", "parent work"),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})

	ids := map[string]bool{}
	var parent, sub int
	for _, e := range got {
		if ids[e.ID] {
			t.Errorf("id collision between parent and subagent: %s", e.ID)
		}
		ids[e.ID] = true
		if e.AgentID == "" {
			parent++
		} else {
			sub++
		}
	}
	if parent == 0 || sub == 0 {
		t.Errorf("expected both parent and subagent events, got parent=%d sub=%d", parent, sub)
	}
}

// ---------------------------------------------------------------------------
// Robustness
// ---------------------------------------------------------------------------

func TestRobustness(t *testing.T) {
	t.Run("malformed line is counted not fatal", func(t *testing.T) {
		tr := newTree(t)
		tr.write("proj/"+sessA+".jsonl",
			userLine(sessA, ts1, "u1", "good"),
			`{"type":"user","sessionId":`, /* truncated JSON */
			`not json at all`,
			userLine(sessA, ts3, "u2", "also good"),
		)

		got, res := collectEvents(t, Options{Root: tr.root})
		if len(got) < 2 {
			t.Errorf("good records were lost alongside the bad ones: %d events", len(got))
		}
		if !hasSkip(res, sessA+".jsonl", "malformed") {
			t.Errorf("malformed lines not reported; got:%s", skipReasons(res))
		}
	})

	t.Run("empty file skipped with reason", func(t *testing.T) {
		tr := newTree(t)
		tr.write("proj/" + sessA + ".jsonl")

		_, res := collectEvents(t, Options{Root: tr.root})
		if !hasSkip(res, sessA+".jsonl", "empty") {
			t.Errorf("empty file not reported; got:%s", skipReasons(res))
		}
	})

	t.Run("file with no sessionId skipped with reason", func(t *testing.T) {
		tr := newTree(t)
		tr.write("proj/"+sessA+".jsonl",
			`{"type":"user","timestamp":"`+ts1+`","uuid":"u1","message":{"role":"user","content":"orphan"}}`,
		)

		got, res := collectEvents(t, Options{Root: tr.root})
		if len(got) != 0 {
			t.Errorf("emitted %d events from a file with no sessionId", len(got))
		}
		if !hasSkip(res, sessA+".jsonl", "no usable records") {
			t.Errorf("missing skip reason; got:%s", skipReasons(res))
		}
	})

	t.Run("unreadable file recorded not fatal", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("running as root; permission bits are not enforced")
		}
		tr := newTree(t)
		bad := tr.write("proj/"+sessB+".jsonl", userLine(sessB, ts1, "u1", "x"))
		if err := os.Chmod(bad, 0o000); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(bad, 0o644) })
		tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "good"))

		got, res := collectEvents(t, Options{Root: tr.root})
		if len(got) == 0 {
			t.Error("one unreadable file aborted the whole walk")
		}
		if !hasSkip(res, sessB+".jsonl", "open:") {
			t.Errorf("unreadable file not reported; got:%s", skipReasons(res))
		}
	})

	t.Run("oversized line handled without panic", func(t *testing.T) {
		// Real corpora contain lines over 1.4 MB, so the default 64 KB scanner
		// buffer is not survivable. Shrink the ceiling to prove the overflow
		// path is reported rather than silently truncating the file.
		orig := maxLineBytes
		maxLineBytes = 512
		t.Cleanup(func() { maxLineBytes = orig })

		tr := newTree(t)
		huge := userLine(sessA, ts1, "u1", strings.Repeat("x", 4096))
		tr.write("proj/"+sessA+".jsonl", huge)

		_, res := collectEvents(t, Options{Root: tr.root})
		if !hasSkip(res, sessA+".jsonl", "exceeds") {
			t.Errorf("oversized line not reported; got:%s", skipReasons(res))
		}
	})

	t.Run("real-world line sizes fit the default buffer", func(t *testing.T) {
		tr := newTree(t)
		// 1.4 MB is the largest line observed in the reference corpus.
		tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", strings.Repeat("y", 1_500_000)))

		got, res := collectEvents(t, Options{Root: tr.root})
		if len(got) == 0 {
			t.Errorf("a 1.5 MB line produced no events; skips:%s", skipReasons(res))
		}
	})

	t.Run("emit error aborts and returns partial result", func(t *testing.T) {
		tr := newTree(t)
		tr.write("proj/"+sessA+".jsonl",
			userLine(sessA, ts1, "u1", "a"),
			userLine(sessA, ts2, "u2", "b"),
		)

		boom := errors.New("consumer closed")
		var n int
		_, err := Walk(Options{Root: tr.root}, func(event.Event) error {
			n++
			return boom
		})
		if !errors.Is(err, boom) {
			t.Errorf("Walk err = %v, want %v", err, boom)
		}
		if n != 1 {
			t.Errorf("emit called %d times after returning an error, want 1", n)
		}
	})

	t.Run("missing root is an error", func(t *testing.T) {
		if _, err := Walk(Options{}, func(event.Event) error { return nil }); err == nil {
			t.Error("expected an error for an empty Root")
		}
	})
}

// ---------------------------------------------------------------------------
// Schema-specific handling
// ---------------------------------------------------------------------------

// journal.jsonl has an unrelated schema. It must be skipped by name, with a
// reason, rather than parsed into garbage.
func TestJournalSkippedExplicitly(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+"/subagents/workflows/wf_1/journal.jsonl",
		`{"type":"started","key":"k1","agentId":"agent-a1"}`,
		`{"type":"result","key":"k1","agentId":"agent-a1","result":"done"}`,
	)

	got, res := collectEvents(t, Options{Root: tr.root})
	if len(got) != 0 {
		t.Errorf("journal.jsonl produced %d events, want 0", len(got))
	}
	if !hasSkip(res, "journal.jsonl", "unrelated schema") {
		t.Errorf("journal not skipped with a reason; got:%s", skipReasons(res))
	}
}

// file-history records carry no sessionId and the header records carry no
// timestamp. A walker that did not recognise them by name would report roughly
// 150 files as damaged on a perfectly healthy machine, burying the real
// problems. They must be ignored silently, not skipped noisily.
func TestStructurallyIncompleteBoilerplateIsRecognised(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		// Header records: no timestamp, and they precede the first real record.
		fmt.Sprintf(`{"type":"mode","sessionId":%q,"mode":"default"}`, sessA),
		fmt.Sprintf(`{"type":"permission-mode","sessionId":%q,"permissionMode":"acceptEdits"}`, sessA),
		fmt.Sprintf(`{"type":"ai-title","sessionId":%q,"title":"some task"}`, sessA),
		fmt.Sprintf(`{"type":"agent-name","sessionId":%q,"name":"worker"}`, sessA),
		fmt.Sprintf(`{"type":"bridge-session","sessionId":%q}`, sessA),
		// File-history records: no sessionId at all.
		`{"type":"file-history-snapshot","snapshot":{"files":[]}}`,
		`{"type":"file-history-delta","delta":{"path":"/repo/x.go"}}`,
		userLine(sessA, ts1, "u1", "the real prompt"),
	)

	got, res := collectEvents(t, Options{Root: tr.root})

	if len(got) == 0 {
		t.Fatal("the real prompt was lost")
	}
	for _, e := range got {
		if e.OccurredAt.IsZero() {
			t.Errorf("event %s has a zero timestamp", e.Type)
		}
	}
	// Boilerplate must not generate advisories, or the report cries wolf.
	for _, s := range res.Skipped {
		if strings.Contains(s.Reason, "no sessionId") || strings.Contains(s.Reason, "no timestamp") {
			t.Errorf("routine boilerplate reported as an anomaly: %s", s.Reason)
		}
	}
}

func TestIgnoredRecordTypesProduceNoEvents(t *testing.T) {
	for _, typ := range []string{
		"attachment", "queue-operation", "last-prompt", "summary",
		"mode", "permission-mode", "bridge-session", "ai-title", "agent-name",
		"file-history-snapshot", "file-history-delta",
	} {
		t.Run(typ, func(t *testing.T) {
			tr := newTree(t)
			tr.write("proj/"+sessA+".jsonl",
				userLine(sessA, ts1, "u1", "real"),
				fmt.Sprintf(`{"type":%q,"sessionId":%q,"timestamp":%q,"uuid":"x1","attachment":{"k":"v"}}`, typ, sessA, ts2),
			)

			got, _ := collectEvents(t, Options{Root: tr.root})
			for _, e := range got {
				if strings.Contains(string(e.Raw), `"type":"`+typ+`"`) {
					t.Errorf("%s record produced an event (%s)", typ, e.Type)
				}
			}
		})
	}
}

// "<synthetic>" rows are not API calls. Counting their usage inflates cost.
func TestSyntheticUsageExcluded(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		assistantLine(sessA, ts1, "a1", syntheticModel, "synthetic turn"),
		assistantLine(sessA, ts2, "a2", "claude-fable-5", "real turn"),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})

	var synthUsage, realUsage int
	for _, e := range got {
		if e.Usage == nil {
			continue
		}
		if e.Model == syntheticModel {
			synthUsage++
		} else {
			realUsage++
		}
	}
	if synthUsage != 0 {
		t.Errorf("%d synthetic events carried usage, want 0", synthUsage)
	}
	if realUsage == 0 {
		t.Error("the real model turn lost its usage")
	}
}

func TestUsageMappedIncludingCacheTiers(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		assistantLine(sessA, ts1, "a1", "claude-fable-5", "reply"),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})

	var u *event.Usage
	for _, e := range got {
		if e.Usage != nil {
			u = e.Usage
			break
		}
	}
	if u == nil {
		t.Fatal("no event carried usage")
	}
	want := event.Usage{
		InputTokens: 10, OutputTokens: 20,
		CacheReadTokens: 300, CacheCreationTokens: 40,
		Ephemeral5m: 30, Ephemeral1h: 10,
		ServiceTier: "standard", MessageID: "msg_1", RequestID: "req_1",
	}
	if *u != want {
		t.Errorf("usage = %+v, want %+v", *u, want)
	}
}

// One transcript record is one API call, so its usage must land on exactly one
// event no matter how many blocks the record contains.
func TestUsageAttachedOncePerRecord(t *testing.T) {
	tr := newTree(t)
	multi := fmt.Sprintf(
		`{"type":"assistant","sessionId":%q,"timestamp":%q,"uuid":"a1","message":{"role":"assistant","model":"claude-fable-5","id":"msg_1","usage":{"input_tokens":10,"output_tokens":20},"content":[{"type":"text","text":"thinking out loud"},{"type":"tool_use","id":"toolu_1","name":"Read","input":{}},{"type":"tool_use","id":"toolu_2","name":"Grep","input":{}}]}}`,
		sessA, ts1)
	tr.write("proj/"+sessA+".jsonl", multi)

	got, _ := collectEvents(t, Options{Root: tr.root})

	var withUsage int
	for _, e := range got {
		if e.Usage != nil {
			withUsage++
		}
	}
	if withUsage != 1 {
		t.Errorf("%d events carried usage from one record, want exactly 1", withUsage)
	}
}

func TestContentShapes(t *testing.T) {
	tr := newTree(t)
	// A typed prompt has a bare string for content; everything else uses blocks.
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "string content"),
		fmt.Sprintf(`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":"u2","message":{"role":"user","content":[{"type":"text","text":"array content"}]}}`, sessA, ts2),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})

	var sawString, sawArray bool
	for _, e := range got {
		switch e.Text {
		case "string content":
			sawString = true
		case "array content":
			sawArray = true
		}
		if e.Text != "" && e.Type != event.UserPrompt {
			t.Errorf("user text produced type %q, want %q", e.Type, event.UserPrompt)
		}
	}
	if !sawString {
		t.Error("string-shaped message.content was not parsed")
	}
	if !sawArray {
		t.Error("array-shaped message.content was not parsed")
	}
}

func TestToolCallAndResult(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		toolUseLine(sessA, ts1, "a1", "toolu_1", "Read"),
		toolResultLine(sessA, ts2, "u1", "toolu_1", "the output", false),
		toolResultLine(sessA, ts3, "u2", "toolu_2", "it broke", true),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})

	var call, result, failed int
	for _, e := range got {
		switch e.Type {
		case event.ToolCall:
			call++
			if e.Tool == nil || e.Tool.Name != "Read" {
				t.Errorf("tool call missing name: %+v", e.Tool)
			}
		case event.ToolResult:
			result++
			if e.Tool == nil || e.Tool.Output != "the output" {
				t.Errorf("tool result output = %+v", e.Tool)
			}
		case event.ToolFailed:
			failed++
			if e.Tool == nil || e.Tool.Error == "" {
				t.Errorf("failed tool has no Error: %+v", e.Tool)
			}
		}
	}
	if call != 1 || result != 1 || failed != 1 {
		t.Errorf("call=%d result=%d failed=%d, want 1/1/1", call, result, failed)
	}
}

// Output over the inline limit is spooled beside the transcript. The spool
// file's existence is the only truncation signal the transcript gives.
func TestTruncatedToolOutputPointsAtSpool(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		toolResultLine(sessA, ts1, "u1", "toolu_big", "head of output...", false),
		toolResultLine(sessA, ts2, "u2", "toolu_small", "complete output", false),
	)
	spool := tr.write("proj/"+sessA+"/tool-results/toolu_big.txt", "the full untruncated output")

	got, _ := collectEvents(t, Options{Root: tr.root})

	var checkedBig, checkedSmall bool
	for _, e := range got {
		if e.Tool == nil {
			continue
		}
		switch e.Tool.Output {
		case "head of output...":
			checkedBig = true
			if !e.Tool.Truncated {
				t.Error("spooled tool result not marked Truncated")
			}
			if e.Tool.OutputPath != spool {
				t.Errorf("OutputPath = %q, want %q", e.Tool.OutputPath, spool)
			}
		case "complete output":
			checkedSmall = true
			if e.Tool.Truncated || e.Tool.OutputPath != "" {
				t.Errorf("unspooled result wrongly marked truncated: %+v", e.Tool)
			}
		}
	}
	if !checkedBig || !checkedSmall {
		t.Errorf("fixtures not both seen (big=%v small=%v)", checkedBig, checkedSmall)
	}
}

func TestCompactionEvent(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		fmt.Sprintf(`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":"c1","isCompactSummary":true,"message":{"role":"user","content":"summary of prior context"}}`, sessA, ts1),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})

	var n int
	for _, e := range got {
		if e.Type == event.Compaction {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d compaction events, want 1", n)
	}
}

// A transcript that stops is not evidence the session ended.
func TestNoSyntheticEndEvents(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "hi"))
	tr.write("proj/"+sessA+"/subagents/agent-w1.jsonl", userLine(sessA, ts2, "s1", "sub"))

	got, _ := collectEvents(t, Options{Root: tr.root})
	for _, e := range got {
		if e.Type == event.SessionEnded || e.Type == event.SubagentEnd {
			t.Errorf("fabricated an end event (%s) from a file that merely stopped", e.Type)
		}
	}
}

// ---------------------------------------------------------------------------
// Scrubbing
// ---------------------------------------------------------------------------

func TestScrubAppliedAndCounted(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "my key is SECRET-TOKEN here"),
		toolResultLine(sessA, ts2, "u2", "toolu_1", "output with SECRET-TOKEN inside", false),
	)

	var calls int
	scrub := func(s string) (string, map[string]int) {
		calls++
		if !strings.Contains(s, "SECRET-TOKEN") {
			return s, nil
		}
		n := strings.Count(s, "SECRET-TOKEN")
		return strings.ReplaceAll(s, "SECRET-TOKEN", "[REDACTED:test]"), map[string]int{"test": n}
	}

	got, _ := collectEvents(t, Options{Root: tr.root, Scrub: scrub})
	if calls == 0 {
		t.Fatal("Scrub was never invoked")
	}

	var totalRedactions int
	for _, e := range got {
		if strings.Contains(e.Text, "SECRET-TOKEN") {
			t.Errorf("secret survived in Text (type=%s)", e.Type)
		}
		if strings.Contains(string(e.Raw), "SECRET-TOKEN") {
			t.Errorf("secret survived in Raw (type=%s)", e.Type)
		}
		if e.Tool != nil && strings.Contains(e.Tool.Output, "SECRET-TOKEN") {
			t.Errorf("secret survived in Tool.Output (type=%s)", e.Type)
		}
		totalRedactions += e.Redactions["test"]
	}
	if totalRedactions == 0 {
		t.Error("redaction counts were not accumulated onto events")
	}

	// Raw must still be valid JSON after redaction, or downstream reprocessing
	// of the record of truth is impossible.
	for _, e := range got {
		if len(e.Raw) == 0 {
			continue
		}
		var any map[string]json.RawMessage
		if err := json.Unmarshal(e.Raw, &any); err != nil {
			t.Errorf("scrubbed Raw is not valid JSON: %v", err)
		}
	}
}

// The same secret appears in Raw and in the derived Text. Counting both would
// report one secret as two.
func TestRedactionCountsNotDoubleCounted(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "ONESECRET"))

	scrub := func(s string) (string, map[string]int) {
		n := strings.Count(s, "ONESECRET")
		if n == 0 {
			return s, nil
		}
		return strings.ReplaceAll(s, "ONESECRET", "[X]"), map[string]int{"k": n}
	}

	got, _ := collectEvents(t, Options{Root: tr.root, Scrub: scrub})

	for _, e := range got {
		if n := e.Redactions["k"]; n > 1 {
			t.Errorf("one secret counted %d times on a single event (type=%s)", n, e.Type)
		}
	}
}

func TestNilScrubIsAllowed(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "plain text"))

	got, _ := collectEvents(t, Options{Root: tr.root, Scrub: nil})
	if len(got) == 0 {
		t.Fatal("no events with a nil Scrub")
	}
	var sawText bool
	for _, e := range got {
		if e.Text == "plain text" {
			sawText = true
		}
	}
	if !sawText {
		t.Error("text was altered despite a nil Scrub")
	}
}

// ---------------------------------------------------------------------------
// Accounting
// ---------------------------------------------------------------------------

func TestResultAccounting(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "a"),
		assistantLine(sessA, ts2, "a1", "claude-fable-5", "b"),
	)
	tr.write("proj/"+sessB+".jsonl", userLine(sessB, ts3, "u1", "c"))

	got, res := collectEvents(t, Options{Root: tr.root})

	if res.Events != len(got) {
		t.Errorf("Result.Events = %d but %d were emitted", res.Events, len(got))
	}
	if res.Files != 2 {
		t.Errorf("Result.Files = %d, want 2", res.Files)
	}
	if res.Sessions != 2 {
		t.Errorf("Result.Sessions = %d, want 2", res.Sessions)
	}
	if res.Bytes <= 0 {
		t.Errorf("Result.Bytes = %d, want > 0", res.Bytes)
	}
}

// A session split across a main transcript and its subagents is one session.
func TestSessionCountedOncePerStream(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "main"))
	tr.write("proj/"+sessA+"/subagents/agent-w1.jsonl", userLine(sessA, ts2, "s1", "sub"))
	tr.write("proj/"+sessA+"/subagents/agent-w2.jsonl", userLine(sessA, ts3, "s2", "sub2"))

	_, res := collectEvents(t, Options{Root: tr.root})
	// Three streams: the parent plus two distinct subagents.
	if res.Sessions != 3 {
		t.Errorf("Result.Sessions = %d, want 3 (parent + 2 subagent streams)", res.Sessions)
	}
}

func TestProgressCallback(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "a"))
	tr.write("proj/"+sessB+".jsonl", userLine(sessB, ts2, "u1", "b"))

	var seen []Progress
	_, err := Walk(Options{
		Root:      tr.root,
		OnSession: func(p Progress) { seen = append(seen, p) },
	}, func(event.Event) error { return nil })
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}

	if len(seen) != 2 {
		t.Fatalf("OnSession called %d times, want 2", len(seen))
	}
	for _, p := range seen {
		if p.File == "" {
			t.Error("Progress.File is empty")
		}
		if p.Events == 0 {
			t.Errorf("Progress.Events = 0 for %s", filepath.Base(p.File))
		}
		if p.Bytes <= 0 {
			t.Errorf("Progress.Bytes = %d for %s", p.Bytes, filepath.Base(p.File))
		}
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		path       string
		wantKind   kind
		agentID    string
		workflowID string
		sessionDir string
	}{
		{
			path:       "/r/proj/" + sessA + ".jsonl",
			wantKind:   kindSession,
			sessionDir: "/r/proj/" + sessA,
		},
		{
			path:       "/r/proj/" + sessA + "/subagents/agent-a1.jsonl",
			wantKind:   kindAgent,
			agentID:    "agent-a1",
			sessionDir: "/r/proj/" + sessA,
		},
		{
			path:       "/r/proj/" + sessA + "/subagents/workflows/wf_9/agent-a2.jsonl",
			wantKind:   kindAgent,
			agentID:    "agent-a2",
			workflowID: "wf_9",
			sessionDir: "/r/proj/" + sessA,
		},
		{
			// A journal is skipped, but it still gets its workflow attributed —
			// the reason string is more useful when it says which workflow.
			path:       "/r/proj/" + sessA + "/subagents/workflows/wf_9/journal.jsonl",
			wantKind:   kindJournal,
			workflowID: "wf_9",
			sessionDir: "/r/proj/" + sessA,
		},
	}

	for _, tc := range cases {
		t.Run(filepath.Base(tc.path), func(t *testing.T) {
			got := classify(filepath.FromSlash(tc.path))
			if got.kind != tc.wantKind {
				t.Errorf("kind = %v, want %v", got.kind, tc.wantKind)
			}
			if got.agentID != tc.agentID {
				t.Errorf("agentID = %q, want %q", got.agentID, tc.agentID)
			}
			if got.workflowID != tc.workflowID {
				t.Errorf("workflowID = %q, want %q", got.workflowID, tc.workflowID)
			}
			if got.sessionDir != filepath.FromSlash(tc.sessionDir) {
				t.Errorf("sessionDir = %q, want %q", got.sessionDir, tc.sessionDir)
			}
		})
	}
}

// Events fold into the rollup the dashboard lists, so the two must agree.
func TestEventsFoldIntoSession(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "do the thing"),
		assistantLine(sessA, ts2, "a1", "claude-fable-5", "on it"),
		toolUseLine(sessA, ts3, "a2", "toolu_1", "Read"),
	)

	got, _ := collectEvents(t, Options{Root: tr.root})

	var s event.Session
	for _, e := range got {
		s.Apply(e)
	}

	if s.SessionID != sessA {
		t.Errorf("SessionID = %q, want %q", s.SessionID, sessA)
	}
	if s.StartedAt != mustTime(t, ts1) {
		t.Errorf("StartedAt = %s, want %s", s.StartedAt, ts1)
	}
	if s.EndedAt != mustTime(t, ts3) {
		t.Errorf("EndedAt = %s, want %s", s.EndedAt, ts3)
	}
	if s.Ended {
		t.Error("Ended is true; a transcript that stops is not a session that ended")
	}
	if s.UserTurns != 1 {
		t.Errorf("UserTurns = %d, want 1", s.UserTurns)
	}
	if s.ToolCalls != 1 {
		t.Errorf("ToolCalls = %d, want 1", s.ToolCalls)
	}
	if s.TotalUsage.InputTokens != 10 || s.TotalUsage.CacheReadTokens != 300 {
		t.Errorf("TotalUsage = %+v, want input=10 cache_read=300", s.TotalUsage)
	}
}

// ---------------------------------------------------------------------------
// Resume
// ---------------------------------------------------------------------------

// A resumed import must emit exactly the events an interrupted one had not
// reached, under exactly the ids the uninterrupted run would have given them.
//
// The fixture puts two files in one stream on purpose: sequence numbers are per
// stream and persist across files, so a resume that dropped a delivered file
// from the walk instead of merely muting it would renumber everything after it.
// That failure is invisible locally — every id is still deterministic, just
// deterministically different — and shows up server-side as a second copy of a
// session that was already stored.
func TestResumeEmitsTheRemainderUnderUnchangedIDs(t *testing.T) {
	build := func() *tree {
		tr := newTree(t)
		tr.write("proj-a/"+sessA+".jsonl",
			userLine(sessA, ts1, "u1", "first file"),
			assistantLine(sessA, ts2, "a1", "claude-fable-5", "first reply"),
		)
		tr.write("proj-b/"+sessA+".jsonl",
			userLine(sessA, ts3, "u2", "same stream, later file"),
			toolUseLine(sessA, ts4, "a2", "toolu_1", "Read"),
		)
		return tr
	}

	tr := build()
	full, fullRes := collectEvents(t, Options{Root: tr.root})
	if len(full) < 4 {
		t.Fatalf("fixture produced %d events, want the whole stream", len(full))
	}
	firstFile := filepath.Join(tr.root, "proj-a", sessA+".jsonl")
	secondFile := filepath.Join(tr.root, "proj-b", sessA+".jsonl")

	// How many events the first file contributed, so the expected tail is
	// derived from the fixture rather than hard-coded.
	var fromFirst int
	for _, e := range full {
		if e.OccurredAt.Before(mustTime(t, ts3)) {
			fromFirst++
		}
	}

	cases := []struct {
		name     string
		done     map[string]bool
		wantTail int // index into full where the resumed run should start
		wantRes  int
	}{
		{
			name:     "nothing delivered yet imports everything",
			done:     nil,
			wantTail: 0,
			wantRes:  0,
		},
		{
			name:     "the delivered file is muted and the rest keeps its ids",
			done:     map[string]bool{firstFile: true},
			wantTail: fromFirst,
			wantRes:  1,
		},
		{
			name:     "a fully delivered corpus emits nothing at all",
			done:     map[string]bool{firstFile: true, secondFile: true},
			wantTail: len(full),
			wantRes:  2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, res := collectEvents(t, Options{
				Root: tr.root,
				Done: func(p string) bool { return tc.done[p] },
			})

			want := full[tc.wantTail:]
			if len(got) != len(want) {
				t.Fatalf("emitted %d event(s), want %d", len(got), len(want))
			}
			for i := range want {
				if got[i].ID != want[i].ID || got[i].Seq != want[i].Seq {
					t.Errorf("event %d: id=%s seq=%d, want id=%s seq=%d (a resume renumbered the stream)",
						i, got[i].ID, got[i].Seq, want[i].ID, want[i].Seq)
				}
				if !got[i].OccurredAt.Equal(want[i].OccurredAt) {
					t.Errorf("event %d occurred at %s, want %s", i, got[i].OccurredAt, want[i].OccurredAt)
				}
			}

			if res.Resumed != tc.wantRes {
				t.Errorf("Result.Resumed = %d, want %d", res.Resumed, tc.wantRes)
			}
			// Files and Bytes describe the walk, so they do not change; Events
			// and Sessions describe this run's delivery, so they do.
			if res.Files != fullRes.Files {
				t.Errorf("Result.Files = %d, want %d: a resumed file is still read", res.Files, fullRes.Files)
			}
			if res.Events != len(got) {
				t.Errorf("Result.Events = %d but %d were emitted", res.Events, len(got))
			}
		})
	}
}

// Redaction is the slowest thing in a walk, and a file that emits nothing must
// not pay for it. This also pins the safety argument: the only reason skipping
// it is sound is that nothing from the file leaves the process.
func TestResumedFilesAreNotScrubbed(t *testing.T) {
	tr := newTree(t)
	delivered := tr.write("proj-a/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "sk-secret-one"))
	tr.write("proj-b/"+sessB+".jsonl", userLine(sessB, ts2, "u1", "sk-secret-two"))

	var scrubbed []string
	_, res := collectEvents(t, Options{
		Root: tr.root,
		Done: func(p string) bool { return p == delivered },
		Scrub: func(s string) (string, map[string]int) {
			scrubbed = append(scrubbed, s)
			return s, nil
		},
	})

	if res.Resumed != 1 {
		t.Fatalf("Result.Resumed = %d, want 1", res.Resumed)
	}
	for _, s := range scrubbed {
		if strings.Contains(s, "sk-secret-one") {
			t.Errorf("the resumed file was scrubbed; nothing from it is emitted, so that work is wasted:\n  %s", s)
		}
	}
	if len(scrubbed) == 0 {
		t.Error("nothing was scrubbed at all; the file that IS being imported must still be redacted")
	}
}

// Redaction is what a walk spends its time on, so a narrow window must not pay
// to redact the history it is about to discard — and must still redact, to the
// letter, everything it emits.
//
// The second half is the one that matters. The optimisation is only sound while
// the record-level test that mutes redaction and the event-level test that drops
// the event agree exactly; the moment they disagree, an unredacted secret is
// emitted. So this asserts both directions rather than the saving alone.
func TestANarrowWindowSkipsRedactingWhatItDrops(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "old secret"))
	tr.write("proj/"+sessB+".jsonl", userLine(sessB, ts4, "u2", "new secret"))

	cases := []struct {
		name        string
		since       string
		wantScrub   []string
		wantNoScrub []string
	}{
		{
			name:      "the whole corpus redacts everything",
			since:     "",
			wantScrub: []string{"old secret", "new secret"},
		},
		{
			name:        "a window redacts what it keeps and skips what it drops",
			since:       ts4,
			wantScrub:   []string{"new secret"},
			wantNoScrub: []string{"old secret"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var seen []string
			opts := Options{
				Root: tr.root,
				Scrub: func(s string) (string, map[string]int) {
					seen = append(seen, s)
					return strings.ReplaceAll(s, "secret", "[redacted]"), nil
				},
			}
			if tc.since != "" {
				opts.Since = mustTime(t, tc.since)
			}
			got, _ := collectEvents(t, opts)

			joined := strings.Join(seen, "\n")
			for _, want := range tc.wantScrub {
				if !strings.Contains(joined, want) {
					t.Errorf("%q was emitted but never redacted", want)
				}
			}
			for _, never := range tc.wantNoScrub {
				if strings.Contains(joined, never) {
					t.Errorf("%q was redacted although it is outside the window and never emitted", never)
				}
			}
			// Whatever did come out carries the redaction, always.
			for _, e := range got {
				if strings.Contains(e.Text, "secret") || strings.Contains(string(e.Raw), "secret") {
					t.Errorf("event %s left the walk unredacted: %s", e.ID, e.Text)
				}
			}
		})
	}
}

// A tally that counts what a filter threw away is a tally that lies. The live
// import of six hours of history said "6,141 event(s) from 6,233 session(s)",
// having credited every stream it walked past whether or not the window kept
// anything from it — which reads as an import that produced a session per event
// and tells the person nothing true.
func TestResultCountsWhatWasEmittedNotWhatWasWalkedPast(t *testing.T) {
	tr := newTree(t)
	// One stream entirely outside the window, one straddling it.
	tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "all old"))
	tr.write("proj/"+sessB+".jsonl",
		userLine(sessB, ts1, "u1", "old"),
		userLine(sessB, ts4, "u2", "new"),
	)

	cases := []struct {
		name         string
		since        string
		wantSessions int
	}{
		{name: "no window credits both streams", since: "", wantSessions: 2},
		{name: "a window credits only the stream it emitted from", since: ts4, wantSessions: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{Root: tr.root}
			if tc.since != "" {
				opts.Since = mustTime(t, tc.since)
			}
			got, res := collectEvents(t, opts)

			if res.Events != len(got) {
				t.Errorf("Result.Events = %d but %d event(s) were emitted", res.Events, len(got))
			}
			if res.Sessions != tc.wantSessions {
				t.Errorf("Result.Sessions = %d, want %d", res.Sessions, tc.wantSessions)
			}
		})
	}
}

// The advisory list is where a person looks for damaged files, so it must not
// fill up with files that are perfectly fine. A resumed run of a real corpus
// reported 6,410 files as unreadable out of 6,403 — every file an earlier run
// had already delivered, plus every file outside the window — and buried the 171
// that genuinely were.
func TestFilteredFilesAreNotReportedAsUnreadable(t *testing.T) {
	tr := newTree(t)
	good := tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts4, "u1", "kept"))
	old := tr.write("proj/"+sessB+".jsonl", userLine(sessB, ts1, "u1", "outside the window"))
	empty := tr.write("proj/empty.jsonl")

	cases := []struct {
		name     string
		opts     Options
		unusable []string
		fine     []string
	}{
		{
			name:     "a window does not make what it drops unreadable",
			opts:     Options{Since: mustTime(t, ts4)},
			unusable: []string{filepath.Base(empty)},
			fine:     []string{filepath.Base(old), filepath.Base(good)},
		},
		{
			name:     "a resumed file does not become unreadable either",
			opts:     Options{Done: func(p string) bool { return p == good }},
			unusable: []string{filepath.Base(empty)},
			fine:     []string{filepath.Base(good), filepath.Base(old)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.opts.Root = tr.root
			_, res := collectEvents(t, tc.opts)

			for _, base := range tc.unusable {
				if !hasSkip(res, base, "") {
					t.Errorf("%s produced nothing and was not reported:%s", base, skipReasons(res))
				}
			}
			for _, base := range tc.fine {
				if hasSkip(res, base, "no usable records") {
					t.Errorf("%s was reported as unreadable; it was filtered, not damaged:%s", base, skipReasons(res))
				}
			}
		})
	}
}
