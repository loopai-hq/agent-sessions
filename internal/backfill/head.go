package backfill

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// Bounded reads of a transcript's two ends.
//
// The live path is handed a transcript path on every hook and never its
// contents. Two things it needs live at the head of the file (how the harness
// was started, the title the harness generated, the identity of the first real
// record) and one at the tail (the last model call). Both reads are bounded
// because they run inside somebody's hook: a head scan of a few dozen lines
// and a tail read of 64 KB (widening to at most 4 MiB when the turn's prompt
// sits behind a large attachment) cost the same on a 4 KB transcript as on a
// 400 MB one, and neither can be talked into reading a whole file.

const (
	// headLines is how many lines the head scan reads. A transcript opens with
	// a handful of bare bookkeeping records (queue-operation, mode,
	// last-prompt, ai-title) before its first content record; 64 is several
	// times the longest such preamble measured across the reference corpus.
	headLines = 64
	// usageTailBytes is how much of a transcript the tail read takes first.
	//
	// The record it wants is the last one in the file, and 64 KB is several
	// times the largest assistant record measured across the reference corpus.
	// Reading the whole file instead would put a multi-hundred-megabyte read
	// inside a hook.
	usageTailBytes = 64 << 10
	// usageTailMaxBytes bounds how far the tail read widens when the first
	// window shows the answer but not the prompt it belongs to, or no answer
	// at all. The harness writes attachment records between a prompt and its
	// answer (a CLAUDE.md import, a hook's injected context): one of
	// 104 KB (103,966 bytes) was measured on 2026-09-15 in a session whose
	// every Stop copy stored without its anchor, and a session's first-turn
	// preamble (instructions, skill listing, prompt snapshot, deferred
	// tools) runs to 200 KB and more, so the first Stop of a session widens
	// twice as a rule. That day one hook copy in five across the fleet had
	// no anchor. Four megabytes is the bound past which a turn is treated
	// as unanchored rather than read whole.
	usageTailMaxBytes = 4 << 20
	// compactSummaryPrefix opens the text of a compaction summary. The record
	// usually carries isCompactSummary:true as well, but a forked file's copy
	// of one was seen with the flag missing and origin:human, so the text is
	// the guard of last resort.
	compactSummaryPrefix = "This session is being continued from a previous conversation"
)

// Head is what the first few lines of a transcript say.
type Head struct {
	// SessionID is the sessionId the first record carries.
	SessionID string
	// Entrypoint is how the harness was started ("cli", "sdk-cli", ...).
	Entrypoint string
	// Cwd is the project directory the first record names.
	Cwd string
	// AITitle is the harness's own generated title (the ai-title record).
	AITitle string
	// FirstUUID, FirstPromptID and FirstAt describe the first content record:
	// the one whose uuid a fork copies verbatim, and whose promptId a fork
	// rewrites. FirstUUID empty means no content record was found in the head.
	FirstUUID     string
	FirstPromptID string
	FirstAt       time.Time
	// FirstIsSidechain and FirstAgentID come off that same record and are how
	// a legacy root-level agent-*.jsonl reveals itself as a subagent stream.
	FirstIsSidechain bool
	FirstAgentID     string
}

// ReadHead scans the first headLines lines. A missing or empty file is an empty
// Head and false, never an error: it runs on the very first hook of a session,
// when the file may not exist yet.
func ReadHead(path string) (Head, bool) {
	var h Head
	if path == "" {
		return h, false
	}
	f, err := os.Open(path)
	if err != nil {
		return h, false
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 64<<10), maxLineBytes)
	var found bool
	for lines := 0; lines < headLines && s.Scan(); lines++ {
		var r record
		if json.Unmarshal(s.Bytes(), &r) != nil {
			continue
		}
		found = true
		if h.SessionID == "" && r.SessionID != "" {
			h.SessionID = r.SessionID
		}
		if r.Type == "ai-title" && r.AITitle != "" {
			h.AITitle = r.AITitle
		}
		if h.Entrypoint == "" && r.Entrypoint != "" {
			h.Entrypoint = r.Entrypoint
		}
		if h.Cwd == "" && r.Cwd != "" {
			h.Cwd = r.Cwd
		}
		if h.FirstUUID == "" && r.UUID != "" && isContentType(r.Type) {
			h.FirstUUID = r.UUID
			h.FirstPromptID = r.PromptID
			h.FirstIsSidechain = r.IsSidechain
			h.FirstAgentID = r.AgentID
			if ts, ok := parseTime(r.Timestamp); ok {
				h.FirstAt = ts
			}
		}
	}
	return h, found
}

// isContentType names the record types that carry a uuid worth anchoring on.
func isContentType(t string) bool {
	switch t {
	case "user", "assistant", "system":
		return true
	}
	return false
}

// Tail is the last model call a transcript records.
type Tail struct {
	// UUID is the assistant record's own uuid: the walker's identity for the
	// same record, and therefore the key that lets a hook copy of the turn
	// and the transcript copy be recognised as one.
	UUID string
	// PromptID is the human turn the record belongs to. Assistant records do
	// not carry promptId themselves; it is inherited from the nearest user
	// record before them, which is the record that opened the turn.
	PromptID string
	// PromptAfter is the prompt named by the nearest user record BELOW the
	// record, or empty when none follows it. An answer cannot precede its own
	// prompt, so a Stop for prompt P must not take a record that has a user
	// record for P after it: that record answered something earlier. Two
	// shapes produce one. A fork's copied prefix carries the resuming prompt's
	// id on every copied user record (the harness rewrites them), so before
	// the fork's own first answer is written, the origin session's last call
	// sits under this turn's id with the fork's own prompt record below it.
	// And under transcript lag with tool calls, the turn's tool-use call sits
	// above its tool result, which carries the turn's id too; that call is
	// not the answer either.
	PromptAfter string
	// ToolUse reports that the record carries a tool_use block: a call from
	// inside the turn. Stop fires when no tool call is pending, so the Stop
	// copy's answer is never such a record; anchoring to one would pair the
	// hook's final text with the transcript row of the call (the walker's
	// assistant_turn for it is the call's own prose) and bill the answer the
	// call's tokens. Under transcript lag it is the last spent record in
	// view before the answer lands.
	ToolUse bool
	// Text is the record's text, so a reader can tell whether the record IS
	// the answer a Stop copy carries: the copied prefix of a fork and the
	// answer of the same turn look alike by prompt id when a user record for
	// the prompt follows the record (a teammate message, a blocking Stop
	// hook's feedback, a cut-off continuation), and the text separates
	// them. The harness writes one content block per assistant record, so
	// this is one text block's text; a record with several (not seen in
	// any transcript on this machine) would be joined with newlines, which
	// can only fail to match, never match wrongly.
	Text string
	// Timestamp is the record's own timestamp (the harness stamps each
	// content-block record as that block completes, so an answer record,
	// the message's last block, precedes the Stop that follows it), or zero
	// when the record carries none. A reader that runs after the fact uses it
	// to refuse a record newer than the Stop copy it is anchoring: one
	// prompt can end in Stop more than once (a lead woken by teammate
	// messages, a blocking Stop hook), and each copy belongs to its own
	// answer.
	Timestamp time.Time
	// MessageID and RequestID are the API call's own ids, mirrored here from
	// Usage for callers that want the ids without the token counts. The live
	// copy of a turn carries the same ids as the walked copy, so the server
	// keys them as one call. (RequestID was withheld from Usage until the
	// ledger stopped keying on it; see (*message).usage.)
	MessageID string
	RequestID string
	Model     string
	Usage     *event.Usage
}

// LastAssistantRecord reports the most recent model call in a Claude Code
// transcript: its identity, the turn it belongs to, and what it cost.
//
// This exists for the live capture path. Without the usage a session captured
// live carries prompts, tools and turns and structurally zero tokens; without
// the uuid the server holds two unrelated rows per turn for every session
// captured both live and from its transcript. The mapping of usage is
// deliberately not reimplemented here: (*message).usage is the one place the
// wire form becomes an event.Usage, so a live event and the walker's copy of
// the same call can never disagree about what it cost.
//
// It returns the last record that yields usage rather than strictly the last
// assistant record. Transcripts end with a "<synthetic>" assistant record more
// often than not (an interruption or an API error, neither of which is an API
// call and whose usage is excluded), and stopping at the first assistant record
// would report nothing for a turn that really did spend something. Naming an
// older call instead is harmless: usage is keyed by message id in the ledger,
// so re-reporting one that was already credited changes nothing.
func LastAssistantRecord(path string) (Tail, bool) {
	// The first window is enough when the turn's own records sit at the end
	// of the file. When it is not (the answer is in view but the prompt is
	// not, or nothing in view is a model call), the read widens by four
	// each time up to usageTailMaxBytes; a window that reached the start of
	// the file is the whole answer. The prompt-match rule in the Stop path
	// is strict on purpose, so a tail that cannot name its prompt costs the
	// turn its anchor, and widening is what keeps that rule from firing on
	// every turn that a large attachment happens to follow.
	//
	// A transcript whose user records carry no promptId at all (a harness
	// that predates it) never satisfies the prompt test and widens to the
	// bound on every Stop: four reads, 5.3 MB in all, once per turn, which
	// the Stop path's budget allows (it is the per-tool-call path that is
	// tight). The Stop rule takes such a tail on trust anyway.
	var last Tail
	var found bool
	for window := int64(usageTailBytes); ; window = min(window*4, usageTailMaxBytes) {
		lines, whole, ok := tailLines(path, window)
		if !ok {
			// A wider read that fails (the file went away or shrank between
			// windows) does not unsay what the narrower one found.
			return last, found
		}
		if lines != nil {
			last, found = lastAssistantIn(lines)
		}
		if (found && last.PromptID != "") || whole || window >= usageTailMaxBytes {
			return last, found
		}
	}
}

// lastAssistantIn is LastAssistantRecord over lines already read: the last
// record that is a model call with something spent, and the prompt it
// answers when a user record above it in the same lines names one.
func lastAssistantIn(lines [][]byte) (Tail, bool) {
	for i := len(lines) - 1; i >= 0; i-- {
		var r record
		if json.Unmarshal(lines[i], &r) != nil || r.Type != "assistant" {
			continue
		}
		// usage() has an all-zero guard of its own, but it cannot fire on a
		// record that has a message id (the id is part of the struct it
		// compares), so a record carrying an empty usage object comes back
		// non-nil with nothing in it but the id. Stopping there would answer a
		// hook's "what did that turn cost" with a row that has no tokens in it,
		// while the turn that did cost something sits one line further up.
		u := r.Message.usage()
		if !spent(u) {
			continue
		}
		u.RequestID = r.RequestID
		t := Tail{UUID: r.UUID, PromptID: r.PromptID, RequestID: r.RequestID, Model: r.Message.model(), Usage: u}
		if r.Message != nil {
			t.MessageID = r.Message.ID
		}
		if t.PromptID == "" {
			t.PromptID = promptBefore(lines, i)
		}
		t.PromptAfter = promptAfter(lines, i)
		t.ToolUse = hasToolUse(r.Message)
		t.Text = recordText(r.Message)
		if ts, ok := parseTime(r.Timestamp); ok {
			t.Timestamp = ts
		}
		return t, true
	}
	return Tail{}, false
}

// recordText is the record's text: its string content, or its non-empty
// text blocks joined with newlines (the harness writes one block per record,
// so the join is for a shape not seen in practice).
func recordText(m *message) string {
	s, blocks := m.content()
	if s != "" {
		return s
	}
	var parts []string
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// hasToolUse reports whether an assistant record's content carries a tool_use
// block, reading the content the way the walker does (a string on typed
// user turns, an array of blocks otherwise; a string cannot carry one).
func hasToolUse(m *message) bool {
	_, blocks := m.content()
	for _, b := range blocks {
		if b.Type == "tool_use" {
			return true
		}
	}
	return false
}

// promptAfter finds the promptId of the nearest user record below line i,
// which is a turn the record at i cannot have answered.
func promptAfter(lines [][]byte, i int) string {
	for j := i + 1; j < len(lines); j++ {
		var r struct {
			Type     string `json:"type"`
			PromptID string `json:"promptId"`
		}
		if json.Unmarshal(lines[j], &r) == nil && r.Type == "user" && r.PromptID != "" {
			return r.PromptID
		}
	}
	return ""
}

// LastAssistantUsage reports what the most recent model call cost and which
// model made it. It is LastAssistantRecord without the identity, kept for the
// callers that only price a turn.
func LastAssistantUsage(path string) (*event.Usage, string, bool) {
	t, ok := LastAssistantRecord(path)
	if !ok {
		return nil, "", false
	}
	return t.Usage, t.Model, true
}

// promptBefore finds the promptId of the nearest user record above line i,
// which is the turn the assistant record at i answers.
func promptBefore(lines [][]byte, i int) string {
	for j := i - 1; j >= 0; j-- {
		var r struct {
			Type     string `json:"type"`
			PromptID string `json:"promptId"`
		}
		if json.Unmarshal(lines[j], &r) == nil && r.Type == "user" && r.PromptID != "" {
			return r.PromptID
		}
	}
	return ""
}

// tailLines reads the last window bytes of a file as whole lines. whole
// reports that the read started at the first byte, so nothing older exists.
func tailLines(path string, window int64) (lines [][]byte, whole bool, ok bool) {
	if path == "" {
		return nil, false, false
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false, false
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return nil, false, false
	}
	offset := int64(0)
	if info.Size() > window {
		// One byte before the window: whether the window's first byte opens
		// a record is only knowable from the byte before it.
		offset = info.Size() - window - 1
	}
	buf := make([]byte, info.Size()-offset)
	// io.EOF here means the file was truncated between the stat and the read;
	// the buffer is cut to what was read rather than parsed with a zero tail.
	n, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false, false
	}
	buf = buf[:n]
	// A read that starts at a byte offset starts mid-record unless the byte
	// before the window was a newline. That leading fragment is not a record
	// and must not be parsed as one. A window with no newline at all is one
	// record longer than the window (a large attachment still being written
	// after the answer): nothing is in view, which is an answer the caller
	// may widen on, not a failure.
	if offset > 0 {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			return nil, false, true
		}
		buf = buf[i+1:]
	}
	var out [][]byte
	for _, line := range bytes.Split(buf, []byte("\n")) {
		if line = bytes.TrimSpace(line); len(line) > 0 {
			out = append(out, line)
		}
	}
	return out, offset == 0, true
}

// LastTimestamp reports the newest timestamp among a file's final records, or
// the zero time when none parses. It is the cheap answer to "does this file
// have anything inside the import window": records are appended in time order,
// so the tail holds the file's latest instant.
func LastTimestamp(path string) time.Time {
	lines, _, ok := tailLines(path, usageTailBytes)
	if !ok {
		return time.Time{}
	}
	for i := len(lines) - 1; i >= 0; i-- {
		var r struct {
			Timestamp string `json:"timestamp"`
		}
		if json.Unmarshal(lines[i], &r) != nil {
			continue
		}
		if ts, ok := parseTime(r.Timestamp); ok {
			return ts
		}
	}
	return time.Time{}
}

// IsCompactSummaryText reports whether prose is a compaction summary by its
// opening words, independent of the isCompactSummary flag.
func IsCompactSummaryText(s string) bool {
	return strings.HasPrefix(strings.TrimSpace(s), compactSummaryPrefix)
}
