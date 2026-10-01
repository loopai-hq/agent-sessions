package backfill

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

const (
	rootID  = "019fd644-3a15-7c60-a3d4-01d8df6849a0"
	childID = "019fd694-b8ba-7631-9c22-76b40e1756bb"
	t1      = "2026-08-06T10:00:00.000Z"
	t2      = "2026-08-06T10:01:00.000Z"
	t3      = "2026-08-06T10:02:00.000Z"
)

func line(ts, typ, payload string) string {
	return fmt.Sprintf(`{"timestamp":%q,"type":%q,"payload":%s}`, ts, typ, payload)
}
func meta(id, parent string) string {
	return line(t1, "session_meta", fmt.Sprintf(`{"id":%q,"timestamp":%q,"cwd":"/repo","cli_version":"0.147.0","parent_thread_id":%q,"git":{"branch":"feature"}}`, id, t1, parent))
}
func write(t *testing.T, root, name string, lines ...string) string {
	t.Helper()
	p := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}
func walk(t *testing.T, opts Options) ([]event.Event, Result) {
	t.Helper()
	var got []event.Event
	res, err := WalkCodex(opts, func(e event.Event) error { got = append(got, e); return nil })
	if err != nil {
		t.Fatal(err)
	}
	return got, res
}

func TestMessagesToolsAndCompaction(t *testing.T) {
	root := t.TempDir()
	write(t, root, "rollout.jsonl",
		meta(rootID, ""),
		line(t1, "turn_context", `{"model":"gpt-5.4","cwd":"/repo/sub"}`),
		line(t1, "response_item", `{"type":"message","id":"u1","role":"user","content":[{"type":"input_text","text":"fix it"}]}`),
		line(t2, "event_msg", `{"type":"user_message","message":"fix it"}`),
		line(t2, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"working"}]}`),
		line(t2, "response_item", `{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"go test\"}","call_id":"call-1"}`),
		line(t3, "response_item", `{"type":"function_call_output","call_id":"call-1","output":"ok"}`),
		line(t3, "compacted", `{"message":"summary"}`),
		line(t3, "event_msg", `{"type":"context_compacted"}`),
	)
	got, res := walk(t, Options{Root: root})
	want := []event.Type{event.SessionStarted, event.UserPrompt, event.AssistantTurn, event.ToolCall, event.ToolResult, event.Compaction}
	var types []event.Type
	for _, e := range got {
		types = append(types, e.Type)
		if err := e.Validate(); err != nil {
			t.Error(err)
		}
	}
	if !reflect.DeepEqual(types, want) {
		t.Fatalf("types=%v want %v", types, want)
	}
	if got[1].Text != "fix it" || got[2].Text != "working" {
		t.Fatalf("message text not mapped: %#v", got)
	}
	if got[3].Tool.Name != "exec_command" || string(got[3].Tool.Input) != `{"cmd":"go test"}` {
		t.Errorf("call=%+v", got[3].Tool)
	}
	if got[4].Tool.Output != "ok" {
		t.Errorf("output=%q", got[4].Tool.Output)
	}
	if got[0].Cwd != "/repo" || got[0].GitBranch != "feature" || got[2].Cwd != "/repo/sub" || got[2].Model != "gpt-5.4" {
		t.Errorf("metadata not propagated: %#v", got)
	}
	if res.Sessions != 1 || res.Events != len(want) {
		t.Errorf("result=%+v", res)
	}
}

// The window is per file, as it is for Claude transcripts: a rollout with a
// record inside it is emitted whole, one entirely older is not emitted at all,
// and neither decision changes an id.
func TestFilteringDoesNotChangeIDs(t *testing.T) {
	root := t.TempDir()
	write(t, root, "a-old.jsonl", meta(rootID, ""), line(t1, "response_item", `{"type":"message","id":"u1","role":"user","content":[{"type":"input_text","text":"wholly old"}]}`))
	write(t, root, "b-mixed.jsonl", meta(childID, ""), line(t1, "response_item", `{"type":"message","id":"u1","role":"user","content":[{"type":"input_text","text":"old head"}]}`), line(t3, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"new"}]}`))
	all, _ := walk(t, Options{Root: root})
	since, _ := time.Parse(time.RFC3339, t2)
	filtered, _ := walk(t, Options{Root: root, Since: since})

	var texts []string
	for _, e := range filtered {
		if e.SessionID == rootID {
			t.Errorf("a rollout entirely older than the window was emitted: %q", e.Text)
		}
		texts = append(texts, e.Text)
	}
	if !reflect.DeepEqual(texts, []string{"", "old head", "new"}) {
		t.Fatalf("filtered texts=%v, want the mixed rollout whole", texts)
	}
	ids := map[string]int64{}
	for _, e := range all {
		ids[e.ID] = e.Seq
	}
	for _, e := range filtered {
		if seq, ok := ids[e.ID]; !ok || seq != e.Seq {
			t.Errorf("filter changed identity: %s seq %d", e.ID, e.Seq)
		}
	}
	again, _ := walk(t, Options{Root: root})
	for i := range all {
		if all[i].ID != again[i].ID {
			t.Fatalf("id changed at %d", i)
		}
	}
}

func TestSubagentResolvesRootLineage(t *testing.T) {
	root := t.TempDir()
	write(t, root, "a-root.jsonl", meta(rootID, ""), line(t2, "response_item", `{"type":"message","id":"u","role":"user","content":[{"type":"input_text","text":"root"}]}`))
	write(t, root, "z-child.jsonl", meta(childID, rootID), line(t2, "response_item", `{"type":"message","id":"a","role":"assistant","content":[{"type":"output_text","text":"child"}]}`))
	got, res := walk(t, Options{Root: root})
	var child []event.Event
	for _, e := range got {
		if e.AgentID == childID {
			child = append(child, e)
		}
	}
	if len(child) != 2 || child[0].Type != event.SubagentStart {
		t.Fatalf("child=%#v", child)
	}
	for _, e := range child {
		if e.SessionID != rootID {
			t.Errorf("session=%q want root", e.SessionID)
		}
	}
	if res.Sessions != 2 {
		t.Errorf("streams=%d want 2", res.Sessions)
	}
}

func TestLaterSessionMetadataDoesNotSwitchRollout(t *testing.T) {
	root := t.TempDir()
	write(t, root, "rollout.jsonl",
		meta(rootID, ""),
		meta(childID, rootID),
		line(t2, "response_item", `{"type":"message","id":"u","role":"user","content":[{"type":"input_text","text":"still root"}]}`),
	)
	got, _ := walk(t, Options{Root: root})
	if len(got) != 2 {
		t.Fatalf("events = %d, want start and prompt", len(got))
	}
	for _, e := range got {
		if e.SessionID != rootID || e.AgentID != "" {
			t.Errorf("later metadata switched rollout attribution: session=%q agent=%q", e.SessionID, e.AgentID)
		}
	}
}

func TestMalformedLinesProduceOneAdvisoryPerFile(t *testing.T) {
	root := t.TempDir()
	write(t, root, "rollout.jsonl",
		meta(rootID, ""),
		`{bad`,
		`{also bad`,
		line(t2, "response_item", `{"type":"message","id":"u","role":"user","content":[{"type":"input_text","text":"valid"}]}`),
	)
	_, result := walk(t, Options{Root: root})
	if len(result.Skipped) != 1 || !strings.Contains(result.Skipped[0].Reason, "2 malformed JSON lines") {
		t.Fatalf("skipped=%+v", result.Skipped)
	}
}

// Every Codex event must carry the capture version, the same contract the
// Claude walker's push enforces. An unstamped event reads as version zero, so
// the first schema bump would re-upload the whole Codex corpus to overwrite
// byte-identical bodies — and an actually improved Codex extraction could never
// replace these rows, because zero is what they would already claim to be.
// This shipped missing once: the Codex walker grew its own push and the
// centralised stamp did not come with it.
func TestCodexEventsCarryTheCaptureVersion(t *testing.T) {
	root := t.TempDir()
	write(t, root, "2026/08/06/rollout-1.jsonl",
		meta(rootID, ""),
		line(t2, "response_item", `{"type":"message","id":"m1","role":"user","content":[{"type":"input_text","text":"hi"}]}`),
	)
	got, _ := walk(t, Options{Root: root})
	if len(got) == 0 {
		t.Fatal("nothing was emitted")
	}
	for _, e := range got {
		if e.CaptureVersion != event.CaptureSchema {
			t.Fatalf("event %s carries capture version %d, want %d",
				e.Type, e.CaptureVersion, event.CaptureSchema)
		}
	}
}

// Compressed rollouts cannot be read yet, and a gap in somebody's history has
// to be a declared gap: an import that silently collects only the plain files
// reports a complete history that quietly starts at whenever Codex last
// compressed.
func TestCompressedRolloutsAreDeclaredNotIgnored(t *testing.T) {
	root := t.TempDir()
	write(t, root, "2026/08/06/rollout-1.jsonl",
		meta(rootID, ""),
		line(t2, "response_item", `{"type":"message","id":"m1","role":"user","content":[{"type":"input_text","text":"hi"}]}`),
	)
	write(t, root, "2026/07/01/rollout-old.jsonl.zst", "not-actually-zstd-but-never-read")

	got, res := walk(t, Options{Root: root})
	if len(got) == 0 {
		t.Fatal("the plain rollout was not imported")
	}
	var declared bool
	for _, s := range res.Skipped {
		if strings.HasSuffix(s.Path, ".jsonl.zst") && strings.Contains(s.Reason, "not supported") {
			declared = true
		}
	}
	if !declared {
		t.Fatalf("the compressed rollout was silently ignored; skips: %+v", res.Skipped)
	}
}

// TestFindCodexRolloutResolvesBySessionMeta: the repair list names a Codex
// session and no file, because a rollout's name says nothing about which
// session it holds. The finder reads the same session_meta line the walker
// decides from, so the file it returns is the file the walker would import.
func TestFindCodexRolloutResolvesBySessionMeta(t *testing.T) {
	root := t.TempDir()
	other := write(t, root, "2026/08/06/rollout-other.jsonl", meta("11111111-1111-4111-8111-111111111111", ""),
		line(t1, "response_item", `{"type":"message","id":"u1","role":"user","content":[{"type":"input_text","text":"someone else"}]}`))
	mine := write(t, root, "2026/08/07/rollout-mine.jsonl", meta(rootID, ""),
		line(t1, "response_item", `{"type":"message","id":"u1","role":"user","content":[{"type":"input_text","text":"mine"}]}`))
	child := write(t, root, "2026/08/07/rollout-child.jsonl", meta(childID, rootID),
		line(t1, "response_item", `{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"child"}]}`))
	// A file with no metadata at all, which the finder must step over rather
	// than mistake for a match.
	write(t, root, "2026/08/07/not-a-rollout.jsonl", line(t1, "response_item", `{"type":"message","id":"x"}`))

	got, err := FindCodexRollout(root, rootID)
	if err != nil {
		t.Fatalf("FindCodexRollout: %v", err)
	}
	// The child names the root as its parent, so either file holds the
	// session; the walk order is sorted, so the child comes first.
	if got != mine && got != child {
		t.Errorf("FindCodexRollout = %q, want one of %q or %q", got, mine, child)
	}
	if got == other {
		t.Error("the finder returned another session's rollout")
	}

	// The child's own id resolves to the child's file and nothing else.
	if got, err := FindCodexRollout(root, childID); err != nil || got != child {
		t.Errorf("FindCodexRollout(child) = %q (%v), want %q", got, err, child)
	}
	// A session this machine does not hold is not found, and that is not an
	// error: the rollout may be compressed, deleted, or on another laptop.
	if got, err := FindCodexRollout(root, "99999999-9999-4999-8999-999999999999"); err != nil || got != "" {
		t.Errorf("FindCodexRollout(absent) = %q (%v), want an empty path and no error", got, err)
	}
	if _, err := FindCodexRollout(root, "  "); err == nil {
		t.Error("an empty session id was accepted")
	}
}

// TestFindCodexRolloutDoesNotOpenCompressedRollouts: the walker cannot read
// a .jsonl.zst yet and declares the gap rather than importing part of a
// history. The finder must report the same gap as not found rather than
// return a file nothing can walk.
func TestFindCodexRolloutDoesNotOpenCompressedRollouts(t *testing.T) {
	root := t.TempDir()
	write(t, root, "2026/07/01/rollout-old.jsonl.zst", "not-actually-zstd-but-never-read")
	got, err := FindCodexRollout(root, rootID)
	if err != nil || got != "" {
		t.Errorf("FindCodexRollout over a compressed rollout = %q (%v), want an empty path and no error", got, err)
	}
}
