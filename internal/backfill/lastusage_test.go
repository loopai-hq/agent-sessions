package backfill

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// LastAssistantUsage is what makes a live-captured session cost anything. The
// live path is handed a transcript path and nothing else, so everything these
// pin is about reading the end of a file that another process is still writing
// to: it has to find the right record, refuse the ones that are not API calls,
// survive a partial read, and never speak up when it is unsure.

// usageLine is an assistant record with the token counts spelled out, so a test
// can name the numbers it expects rather than sharing one fixture's.
func usageLine(uuid, model, msgID string, in, out, cacheRead int64) string {
	return usageLineWithRequest(uuid, model, msgID, "req_9", in, out, cacheRead)
}

// usageLineWithRequest is usageLine with the record's requestId spelled out,
// for the tests that check which record the id was read from.
func usageLineWithRequest(uuid, model, msgID, requestID string, in, out, cacheRead int64) string {
	return fmt.Sprintf(
		`{"type":"assistant","sessionId":"s1","timestamp":"2026-08-17T10:00:00.000Z","uuid":%q,"requestId":%q,"message":{"role":"assistant","model":%q,"id":%q,"usage":{"input_tokens":%d,"output_tokens":%d,"cache_read_input_tokens":%d},"content":[{"type":"text","text":"hi"}]}}`,
		uuid, requestID, model, msgID, in, out, cacheRead)
}

func TestLastAssistantUsageReadsTheFinalTurn(t *testing.T) {
	tr := newTree(t)
	p := tr.write("t.jsonl",
		userLine("s1", "2026-08-17T09:59:00.000Z", "u1", "go"),
		usageLine("a1", "claude-fable-5", "msg_old", 1, 2, 3),
		usageLine("a2", "claude-fable-5", "msg_new", 11, 22, 33),
	)

	u, model, ok := LastAssistantUsage(p)
	if !ok {
		t.Fatal("no usage found in a transcript that has some")
	}
	if model != "claude-fable-5" {
		t.Errorf("model = %q, want claude-fable-5", model)
	}
	// The LAST record, not the first: the Stop hook is asking what the turn
	// that just finished cost.
	if u.MessageID != "msg_new" {
		t.Errorf("message id = %q, want the final turn's msg_new", u.MessageID)
	}
	if u.InputTokens != 11 || u.OutputTokens != 22 || u.CacheReadTokens != 33 {
		t.Errorf("usage = %+v, want 11/22/33", u)
	}
	// The record's requestId rides along, exactly as the walker stamps it, so
	// the live copy and the walked copy of one call carry the same ids.
	if u.RequestID != "req_9" {
		t.Errorf("request id = %q, want the record's req_9 to match the walker", u.RequestID)
	}
}

// A transcript's last assistant record is very often "<synthetic>" — an
// interruption or an API error, which is not an API call. Reporting its usage
// would inflate cost, and stopping there would report nothing at all for a turn
// that did spend something.
func TestLastAssistantUsageSkipsSyntheticToFindTheRealTurn(t *testing.T) {
	tr := newTree(t)
	p := tr.write("t.jsonl",
		usageLineWithRequest("a1", "claude-fable-5", "msg_real", "req_real", 5, 6, 7),
		usageLineWithRequest("a2", syntheticModel, "msg_synthetic", "req_synthetic", 900, 900, 900),
	)

	u, model, ok := LastAssistantUsage(p)
	if !ok {
		t.Fatal("the synthetic tail hid a real turn behind it")
	}
	if u.MessageID != "msg_real" || model != "claude-fable-5" {
		t.Errorf("usage came from %q on %q, want msg_real on the real model", u.MessageID, model)
	}
	if u.InputTokens != 5 {
		t.Errorf("input = %d, want the real turn's 5", u.InputTokens)
	}
	// The request id comes from the record whose usage was chosen, not from
	// the last assistant record; a tail read that took the ids from one
	// record and the tokens from another would pair them wrongly.
	if u.RequestID != "req_real" {
		t.Errorf("request id = %q, want the real turn's req_real, not the synthetic tail's", u.RequestID)
	}
}

func TestLastAssistantUsageIsSilentWhenThereIsNothingToSay(t *testing.T) {
	tr := newTree(t)
	cases := []struct {
		name  string
		lines []string
	}{
		{"an empty transcript", nil},
		{"prompts only, no model call yet", []string{
			userLine("s1", "2026-08-17T09:59:00.000Z", "u1", "go"),
		}},
		{"an assistant record carrying no usage", []string{
			toolUseLine("s1", "2026-08-17T10:00:00.000Z", "a1", "tu1", "Read"),
		}},
		{"every field zero, which is not a call worth reporting", []string{
			usageLine("a1", "claude-fable-5", "msg_zero", 0, 0, 0),
		}},
		{"only a synthetic record", []string{
			usageLine("a1", syntheticModel, "msg_s", 10, 10, 10),
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := tr.write(strings.ReplaceAll(c.name, " ", "_")+".jsonl", c.lines...)
			if u, _, ok := LastAssistantUsage(p); ok {
				t.Errorf("reported %+v; silence is the only honest answer here", u)
			}
		})
	}
}

func TestLastAssistantUsageOnAMissingFileSaysNothing(t *testing.T) {
	if _, _, ok := LastAssistantUsage(filepath.Join(t.TempDir(), "gone.jsonl")); ok {
		t.Error("reported usage for a transcript that does not exist")
	}
	// The path is empty on hooks that do not carry one, and that is not an
	// error worth a stat syscall.
	if _, _, ok := LastAssistantUsage(""); ok {
		t.Error("reported usage for an empty path")
	}
}

// Only the tail is read, so on a big transcript the read starts mid-record.
// That leading fragment is not a record and must not be parsed as one — nor may
// it make the reader give up on the whole file.
func TestLastAssistantUsageReadsPastATruncatedFirstLine(t *testing.T) {
	tr := newTree(t)
	// Padding well past the tail window, so the read certainly starts inside a
	// record rather than at a line boundary.
	var lines []string
	for i := range 400 {
		lines = append(lines, userLine("s1", "2026-08-17T09:00:00.000Z", fmt.Sprintf("u%d", i),
			strings.Repeat("x", 300)))
	}
	lines = append(lines, usageLine("a-last", "claude-fable-5", "msg_tail", 7, 8, 9))
	p := tr.write("big.jsonl", lines...)

	u, _, ok := LastAssistantUsage(p)
	if !ok {
		t.Fatal("the tail read found nothing in a file whose last record has usage")
	}
	if u.MessageID != "msg_tail" || u.InputTokens != 7 {
		t.Errorf("usage = %+v from %q, want the tail record's 7/8/9", u, u.MessageID)
	}
}

// The record this wants can be pushed out of every tail window, up to the
// bound the read widens to, by a single enormous tool result. Reporting
// nothing is right; reporting whatever happens to be visible, reading the
// whole file, or failing, is not.
func TestLastAssistantUsageStaysSilentWhenTheTurnIsBeyondTheTail(t *testing.T) {
	tr := newTree(t)
	p := tr.write("far.jsonl",
		usageLine("a1", "claude-fable-5", "msg_far", 4, 5, 6),
		toolResultLine("s1", "2026-08-17T10:01:00.000Z", "r1", "tu1",
			strings.Repeat("y", usageTailMaxBytes+1024), false),
	)
	if u, _, ok := LastAssistantUsage(p); ok {
		t.Errorf("reported %+v from outside the tail window", u)
	}
}

// Garbage in the middle of a transcript must not stop the reader: a JSONL file
// being appended to by another process can end in a half-written line.
func TestLastAssistantUsageIgnoresUnparseableLines(t *testing.T) {
	tr := newTree(t)
	p := tr.write("torn.jsonl",
		usageLine("a1", "claude-fable-5", "msg_good", 2, 3, 4),
		`{"type":"assistant","message":{"role":"assis`,
	)
	u, _, ok := LastAssistantUsage(p)
	if !ok {
		t.Fatal("a half-written trailing line hid the record before it")
	}
	if u.MessageID != "msg_good" {
		t.Errorf("message id = %q, want msg_good", u.MessageID)
	}
}

// attachmentLine is a harness attachment record (a CLAUDE.md import, a hook's
// injected context) of about n bytes. They sit between a prompt and its
// answer in real transcripts and are what pushes the prompt out of a fixed
// tail window.
func attachmentLine(uuid string, n int) string {
	return fmt.Sprintf(
		`{"type":"attachment","sessionId":"s1","timestamp":"2026-08-17T10:00:00.500Z","uuid":%q,"attachment":{"type":"hook_additional_context","content":%q}}`,
		uuid, strings.Repeat("x", n))
}

// A 104 KB attachment between the prompt and the answer was measured on
// 2026-09-15; the first window then held the answer and not the prompt, and
// the strict prompt-match rule in the Stop path refused the anchor. The read
// widens until the prompt is in view.
func TestLastAssistantRecordWidensPastALargeAttachmentToFindThePrompt(t *testing.T) {
	tr := newTree(t)
	p := tr.write("t.jsonl",
		promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false),
		attachmentLine("att1", 104<<10),
		usageLine("a1", "claude-fable-5", "msg_1", 5, 6, 7),
	)
	tail, ok := LastAssistantRecord(p)
	if !ok {
		t.Fatal("no record found behind a large attachment")
	}
	if tail.PromptID != "prompt-1" || tail.MessageID != "msg_1" || tail.UUID != "a1" {
		t.Errorf("tail = %+v, want prompt-1 / msg_1 / a1", tail)
	}
}

// The attachment can also follow the answer (a PostToolUse hook's context
// after the final tool result, a session-end attachment), which leaves the
// first window with no model call in it at all.
func TestLastAssistantRecordWidensPastALargeAttachmentToFindTheAnswer(t *testing.T) {
	tr := newTree(t)
	p := tr.write("t.jsonl",
		promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false),
		usageLine("a1", "claude-fable-5", "msg_1", 5, 6, 7),
		attachmentLine("att1", 104<<10),
	)
	tail, ok := LastAssistantRecord(p)
	if !ok {
		t.Fatal("no record found in front of a large attachment")
	}
	if tail.PromptID != "prompt-1" || tail.MessageID != "msg_1" {
		t.Errorf("tail = %+v, want prompt-1 / msg_1", tail)
	}
}

// Past the bound the read stops: the answer is named, the prompt is not, and
// the Stop path's rule decides. Reading the whole file would put an unbounded
// read inside a hook.
func TestLastAssistantRecordStopsWideningAtTheBound(t *testing.T) {
	tr := newTree(t)
	p := tr.write("t.jsonl",
		promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false),
		attachmentLine("att1", usageTailMaxBytes+(64<<10)),
		usageLine("a1", "claude-fable-5", "msg_1", 5, 6, 7),
	)
	tail, ok := LastAssistantRecord(p)
	if !ok {
		t.Fatal("the answer sits in the first window and must be found")
	}
	if tail.PromptID != "" || tail.MessageID != "msg_1" {
		t.Errorf("tail = %+v, want msg_1 with no prompt (beyond the bound)", tail)
	}
}

// The attachment after the answer can still be mid-write at Stop: the first
// window then holds no newline at all, which used to end the read. Nothing in
// view is a reason to widen, not to give up.
func TestLastAssistantRecordWidensPastATornLineLongerThanTheWindow(t *testing.T) {
	p := filepath.Join(t.TempDir(), "torn.jsonl")
	body := promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false) + "\n" +
		usageLine("a1", "claude-fable-5", "msg_1", 5, 6, 7) + "\n" +
		strings.TrimSuffix(attachmentLine("att1", 104<<10), "}") // torn: no closing brace, no newline
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	tail, ok := LastAssistantRecord(p)
	if !ok {
		t.Fatal("no record found in front of a torn line longer than the first window")
	}
	if tail.PromptID != "prompt-1" || tail.MessageID != "msg_1" {
		t.Errorf("tail = %+v, want prompt-1 / msg_1", tail)
	}
}

// A fork's copied prefix carries the resuming prompt's id on every copied
// user record, so until the fork's own first answer is written the origin's
// last call sits under this turn's id, with the fork's own prompt record
// below it. The tail names that record below it so the Stop path can refuse.
func TestLastAssistantRecordNamesThePromptBelowTheRecord(t *testing.T) {
	tr := newTree(t)
	p := tr.write("fork.jsonl",
		promptLine("s1", "2026-08-17T09:00:00.000Z", "u-copied", "prompt-fork", "the origin's question", false),
		usageLine("a-origin", "claude-fable-5", "msg_origin", 5, 6, 7),
		promptLine("s1", "2026-08-17T09:59:00.000Z", "u-own", "prompt-fork", "the fork's question", false),
		attachmentLine("att1", 104<<10),
	)
	tail, ok := LastAssistantRecord(p)
	if !ok {
		t.Fatal("the origin's record is in the file and must be named")
	}
	if tail.UUID != "a-origin" || tail.PromptID != "prompt-fork" || tail.PromptAfter != "prompt-fork" {
		t.Errorf("tail = %+v, want a-origin under prompt-fork with prompt-fork after it", tail)
	}
	// The ordinary shape: nothing but attachments below the answer.
	p2 := tr.write("plain.jsonl",
		promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false),
		usageLine("a1", "claude-fable-5", "msg_1", 5, 6, 7),
		attachmentLine("att1", 1<<10),
	)
	if tail, ok := LastAssistantRecord(p2); !ok || tail.PromptAfter != "" {
		t.Errorf("tail = %+v ok=%v, want no prompt after the answer", tail, ok)
	}
}

// A line that starts exactly at the window's first byte is a whole record,
// not a fragment; only the byte before the window can tell.
func TestTailLinesKeepsALineThatStartsExactlyAtTheWindow(t *testing.T) {
	tr := newTree(t)
	prompt := promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false)
	answer := usageLine("a1", "claude-fable-5", "msg_1", 5, 6, 7)
	window := int64(usageTailBytes)
	// Pad an attachment so that prompt + "\n" + answer + "\n" + attachment + "\n" is exactly the window.
	pad := int(window) - (len(prompt) + 1 + len(answer) + 1 + len(attachmentLine("att1", 0)) + 1)
	p := tr.write("edge.jsonl",
		userLine("s1", "2026-08-17T09:00:00.000Z", "u0", "an older turn"),
		prompt,
		answer,
		attachmentLine("att1", pad),
	)
	lines, whole, ok := tailLines(p, window)
	if !ok || whole {
		t.Fatalf("ok=%v whole=%v, want a partial read", ok, whole)
	}
	if len(lines) == 0 || string(lines[0]) != prompt {
		t.Errorf("first line in view = %.60q, want the prompt that starts exactly at the window", firstLine(lines))
	}
}

func firstLine(lines [][]byte) string {
	if len(lines) == 0 {
		return ""
	}
	return string(lines[0])
}

// A record with a tool_use block is a call from inside the turn, not the
// answer that ends it; the tail says so and the Stop path never anchors to it.
func TestLastAssistantRecordReportsAToolUseCall(t *testing.T) {
	tr := newTree(t)
	call := `{"type":"assistant","sessionId":"s1","timestamp":"2026-08-17T10:00:00.000Z","uuid":"a0","requestId":"req_0","message":{"role":"assistant","model":"claude-fable-5","id":"msg_call","usage":{"input_tokens":100,"output_tokens":20},"content":[{"type":"text","text":"on it"},{"type":"tool_use","id":"toolu_1","name":"Bash","input":{}}]}}`
	p := tr.write("call.jsonl",
		promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false),
		call,
	)
	tail, ok := LastAssistantRecord(p)
	if !ok || !tail.ToolUse || tail.UUID != "a0" {
		t.Errorf("tail = %+v ok=%v, want a0 marked as a tool-use call", tail, ok)
	}
	if want := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC); !tail.Timestamp.Equal(want) {
		t.Errorf("timestamp = %v, want the record's %v", tail.Timestamp, want)
	}
	p2 := tr.write("answer.jsonl",
		promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false),
		usageLine("a1", "claude-fable-5", "msg_1", 5, 6, 7),
	)
	if tail, ok := LastAssistantRecord(p2); !ok || tail.ToolUse {
		t.Errorf("tail = %+v ok=%v, want a plain answer not marked as a call", tail, ok)
	}
}

// The tail carries the record's text, joined as the Stop payload joins text
// blocks, so a reader can match a copy against it.
func TestLastAssistantRecordCarriesTheText(t *testing.T) {
	tr := newTree(t)
	p := tr.write("t.jsonl",
		promptLine("s1", "2026-08-17T09:59:00.000Z", "u1", "prompt-1", "go", false),
		usageLine("a1", "claude-fable-5", "msg_1", 5, 6, 7),
	)
	if tail, ok := LastAssistantRecord(p); !ok || tail.Text != "hi" {
		t.Errorf("text = %q ok=%v, want the record's \"hi\"", tail.Text, ok)
	}
}
