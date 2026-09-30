// Package capture turns Claude Code lifecycle hooks into spooled events.
//
// This is the live path. A hook fires, we translate its payload into one or
// more canonical events, scrub them, and hand them to the spool. What we do not
// do here is any network I/O, because a hook runs inline with someone's turn and
// the budget is tens of milliseconds. The measured cost of getting this wrong is
// concrete: a backgrounded child that inherits the hook's stdout pipe holds the
// parent's output reader for two seconds, versus twenty milliseconds when its
// file descriptors are redirected. Two seconds on every tool call is the
// difference between a tool nobody notices and one everybody uninstalls.
//
// The delivery guarantees of the hooks themselves shape the design more than
// anything else. SessionEnd fires on a clean exit and on SIGTERM, but not on
// SIGKILL, and all SessionEnd hooks share a budget of about 1.5 seconds unless
// a per-hook timeout is set. So SessionEnd is treated as a hint that a session
// is probably over, never as the mechanism that makes data durable. Durability
// comes from writing every event as it happens, and from the reconciliation
// pass that runs at the next SessionStart to finish whatever the last crash
// left open.
//
// Every event carries the anchors the transcript also carries (prompt_id,
// tool_use_id, the assistant record's uuid) so the server can recognise a
// hook copy and a transcript copy of one moment as the same thing. Before the
// anchors the server held two unrelated rows per turn for every session
// captured both ways, and the rollups counted both.
package capture

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/backfill"
	"github.com/loopai-hq/agent-sessions/internal/event"
)

// HookEvent is the payload Claude Code delivers on stdin. Fields we do not use
// are deliberately not modelled: the format is internal to the harness and
// documented as changing between versions, so the parser is a tolerant reader
// that takes what it recognises and ignores unknown fields.
type HookEvent struct {
	HookEventName  string `json:"hook_event_name"`
	SessionID      string `json:"session_id"`
	TranscriptPath string `json:"transcript_path"`
	Cwd            string `json:"cwd"`
	Version        string `json:"version"`
	// PromptID identifies the human turn in progress. Present on every hook
	// after the first prompt (verified live on 2.1.267: UserPromptSubmit,
	// PreToolUse, PostToolUse, SubagentStart, SubagentStop, Stop, SessionEnd);
	// absent on SessionStart. It is the same value the transcript writes as
	// promptId, which makes it the primary cross-origin key for a turn.
	PromptID string `json:"prompt_id"`
	// ToolUseID pairs PreToolUse with PostToolUse and both with the
	// transcript's tool_use block.
	ToolUseID            string          `json:"tool_use_id"`
	Prompt               string          `json:"prompt"`
	Source               string          `json:"source"`
	Reason               string          `json:"reason"`
	ToolName             string          `json:"tool_name"`
	ToolInput            json.RawMessage `json:"tool_input"`
	ToolResponse         json.RawMessage `json:"tool_response"`
	Error                string          `json:"error"`
	Message              json.RawMessage `json:"message"`
	LastAssistantMessage json.RawMessage `json:"last_assistant_message"`
	AgentID              string          `json:"agent_id"`
	AgentType            string          `json:"agent_type"`
	AgentTranscriptPath  string          `json:"agent_transcript_path"`
	WorkflowID           string          `json:"workflow_id"`
}

// toolResponse is the shape of tool_response for file-mutating tools. The
// originalFile and structuredPatch fields are what let us record what a tool
// actually did rather than only what it was asked to do.
type toolResponse struct {
	Stdout            string          `json:"stdout"`
	Output            string          `json:"output"`
	FilePath          string          `json:"filePath"`
	OriginalFile      *string         `json:"originalFile"`
	StructuredPatch   json.RawMessage `json:"structuredPatch"`
	NewString         string          `json:"newString"`
	Content           string          `json:"content"`
	PersistedOutputAt string          `json:"persistedOutputPath"`
	Interrupted       bool            `json:"interrupted"`
	IsError           bool            `json:"is_error"`
}

// Sink receives translated events. The spool implements it; tests substitute a
// recorder. Keeping capture unaware of the spool means a hook handler can be
// exercised without touching a filesystem.
type Sink interface {
	Put(event.Event) error
}

// Scrubber removes credentials from text before it is stored. Injected rather
// than imported so this package has no opinion about which rule set is in use
// and can be tested with a stub.
type Scrubber func(string) (string, map[string]int)

// Capturer translates hook payloads into events.
type Capturer struct {
	defaultMirror func() string
	sink          Sink
	scrub         Scrubber
	seq           func(sessionID string) int64
	now           func() time.Time
	launcher      func() *event.Launcher
}

// SeqFunc allocates a monotonic sequence number within a session. Ordering
// cannot be inferred from arrival time because retries reorder the wire, and it
// cannot be a global counter because sessions run concurrently.
type SeqFunc func(sessionID string) int64

// New builds a Capturer. A nil scrubber is a programming error rather than a
// permissive default: shipping unscrubbed text is the failure this package
// exists to prevent, so it fails loudly instead of silently.
func New(sink Sink, scrub Scrubber, seq SeqFunc) (*Capturer, error) {
	if sink == nil {
		return nil, fmt.Errorf("capture: sink is required")
	}
	if scrub == nil {
		return nil, fmt.Errorf("capture: scrubber is required; refusing to capture unscrubbed text")
	}
	if seq == nil {
		return nil, fmt.Errorf("capture: sequence allocator is required")
	}
	return &Capturer{sink: sink, scrub: scrub, seq: seq, now: time.Now, launcher: launcherFromEnv}, nil
}

// SetClock overrides the clock for tests.
func (c *Capturer) SetClock(f func() time.Time) { c.now = f }

// SetLauncher overrides the environment probe for tests.
func (c *Capturer) SetLauncher(f func() *event.Launcher) { c.launcher = f }

// SetDefaultMirror supplies the machine's standing mirror answer, consulted
// only when the environment says nothing.
func (c *Capturer) SetDefaultMirror(f func() string) { c.defaultMirror = f }

// Handle translates one hook payload and writes the resulting events.
//
// An unrecognised hook name is not an error. New hook events appear in the
// harness over time (the set has grown to 31), and a client that failed on an
// unknown name would break every machine the moment Claude Code shipped a new
// one. Unknown events are ignored and reported so the fleet can see the drift.
func (c *Capturer) Handle(h HookEvent) (int, error) {
	if h.SessionID == "" {
		return 0, fmt.Errorf("capture: hook %q carried no session_id", h.HookEventName)
	}
	evs := c.translate(h)
	for _, e := range evs {
		if err := e.Validate(); err != nil {
			return 0, err
		}
		if err := c.sink.Put(e); err != nil {
			return 0, err
		}
	}
	return len(evs), nil
}

// mirrorRequest normalises the opt-in: "on", "dm", a Slack channel id, or a
// group name pass through; anything longer or stranger (including attempts to
// smuggle text toward a chat.postMessage call) is dropped. The value crosses a
// trust boundary (shell env to server), so the whitelist is the parser; group
// names are re-validated server-side against what the owner may actually see.
func mirrorRequest(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || len(v) > 60 {
		return ""
	}
	for _, r := range v {
		ok := r == '-' || r == '_' || r == ' ' ||
			(r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		if !ok {
			return ""
		}
	}
	return v
}

// transcriptEntrypoint reads how the harness was started off the head of its
// own transcript. The hook payload does not say (entrypoint exists only on
// transcript records), and without it every live-captured session would read
// as a person, including the hourly cron runs the automation filter exists to
// separate. Best-effort by construction: a missing or still-empty file is an
// empty answer, never an error, because capture must not fail on the very
// first hook of a session.
func transcriptEntrypoint(path string) string {
	h, _ := backfill.ReadHead(path)
	return h.Entrypoint
}

func (c *Capturer) base(h HookEvent, typ event.Type) event.Event {
	seq := c.seq(h.SessionID)
	return event.Event{
		ID:             event.DeterministicID(h.SessionID, seq, typ, h.HookEventName),
		CaptureVersion: event.CaptureSchema,
		Source:         event.SourceClaudeCode,
		Origin:         event.OriginHook,
		Type:           typ,
		SessionID:      h.SessionID,
		AgentID:        h.AgentID,
		WorkflowID:     h.WorkflowID,
		// Stamped on every hook that carries it. SessionStart never does, so
		// events before the first prompt have none, which is itself the
		// server's signal that they belong to no turn.
		PromptID:       h.PromptID,
		Seq:            seq,
		OccurredAt:     c.now(),
		Cwd:            h.Cwd,
		HarnessVersion: h.Version,
	}
}

func (c *Capturer) translate(h HookEvent) []event.Event {
	switch h.HookEventName {
	case "SessionStart":
		e := c.base(h, event.SessionStarted)
		e.Text = h.Source // startup | resume | clear | compact | fork
		e.Entrypoint = transcriptEntrypoint(h.TranscriptPath)
		// Always attached, even when every field is empty: an empty launcher
		// from a client that looked is a different fact from no launcher at
		// all, and the server needs to tell them apart to classify GUI hosts.
		if c.launcher != nil {
			e.Launcher = c.launcher()
		}
		// The per-session Slack mirror request rides the launching shell's
		// environment: LOOP_SESSIONS_SLACK=on claude ... is the whole UX.
		// Carried as a request for the server to intersect with the owner's
		// preferences, never acted on client-side.
		e.MirrorRequest = mirrorRequest(os.Getenv("LOOP_SESSIONS_SLACK"))
		if e.MirrorRequest == "" && c.defaultMirror != nil {
			// The machine's standing (or one-shot) answer, set by
			// `loop-sessions mirror use`. Env wins because it is the more
			// deliberate, more scoped act.
			e.MirrorRequest = mirrorRequest(c.defaultMirror())
		}
		return []event.Event{e}

	case "SessionEnd":
		e := c.base(h, event.SessionEnded)
		e.Text = h.Reason
		// Stamped at both ends of the session: at SessionStart a brand-new
		// transcript may not have its first real record on disk yet, and a
		// session whose start raced the file is otherwise never classifiable
		// from live capture at all.
		head, _ := backfill.ReadHead(h.TranscriptPath)
		e.Entrypoint = head.Entrypoint
		e.HarnessTitle = head.AITitle
		// Whether a transcript exists at all is the one fact that classifies
		// a promptless session without waiting for a file that never comes:
		// a `claude -p` run with nothing on stdin fires SessionStart and
		// SessionEnd two seconds apart and writes no transcript.
		exists := transcriptExists(h.TranscriptPath)
		e.TranscriptExists = &exists
		return []event.Event{e}

	case "UserPromptSubmit":
		e := c.base(h, event.UserPrompt)
		e.Text = c.clean(&e, h.Prompt)
		return []event.Event{e}

	case "Stop":
		e := c.base(h, event.AssistantTurn)
		e.Raw = c.cleanRaw(&e, h.Message)
		e.Text = c.clean(&e, stopResponse(h))
		// The hook payload carries the turn's message but no token accounting
		// and no record identity, so both are read from the transcript the
		// hook points at. Without the usage a live-captured session is
		// structurally free; without the uuid it is a second, unrelated copy
		// of every turn the transcript walker later emits.
		//
		// This is the one place in this package that reads a file it was not
		// handed the contents of, and it is affordable for one reason: Stop
		// fires at a turn boundary, once, not on every tool call. The latency
		// budget this package's doc comment is written about is a per-tool-call
		// budget, and a bounded tail read is not on that path.
		//
		// The transcript is written asynchronously and may lag the turn. The
		// tail's last model call is stamped on this turn (its uuid, its
		// message id and usage, its model) only when it demonstrably belongs
		// to this prompt. A record from an earlier turn named here would pair
		// this answer with the wrong transcript row and bill it another
		// turn's tokens, both worse than an anchorless, tokenless copy: the
		// walk that follows carries the real record under its own identity.
		//
		// "Demonstrably" is strict on purpose. When the hook names a prompt,
		// the tail must name the same one; a tail that names none is not
		// taken as agreement, because a harness that stamps prompt_id on the
		// hook stamps promptId on the transcript too, so an empty tail prompt
		// means the turn's user records sit above the tail read's 4 MiB
		// bound (a large tool result between the prompt and the answer), and the record in
		// view could be the previous turn's. Only a hook with no prompt at
		// all (a harness that predates prompt_id) takes the tail on trust.
		// Refusing costs an anchor the repair walk supplies later; accepting
		// wrongly costs a turn its own tokens and gives them to another.
		// The usage carries the call's message id and request id, the same
		// pair the walked copy of this record carries.
		//
		// AnchorFor holds the whole rule ("demonstrably" in both directions
		// and never a tool-use call). This read happens once: the harness
		// flushes the turn's tail after Stop fires, and a hook that waited
		// for it was killed by the print-mode harness's exit, so a copy
		// spooled without its record is anchored by the daemon on its way
		// out of the spool instead (cmd/loop-sessions, stopAnchorer).
		if t, ok := AnchorFor(h.TranscriptPath, h.PromptID, nil); ok {
			Anchor(&e, t)
		}
		return []event.Event{e}

	case "PreToolUse":
		e := c.base(h, event.ToolCall)
		e.ToolUseID = h.ToolUseID
		e.Tool = &event.Tool{Name: h.ToolName, Input: c.cleanRaw(&e, h.ToolInput)}
		return []event.Event{e}

	case "PostToolUse":
		return c.toolResult(h, false)

	case "PostToolUseFailure":
		return c.toolResult(h, true)

	case "SubagentStart":
		e := c.base(h, event.SubagentStart)
		return []event.Event{e}

	case "SubagentStop":
		e := c.base(h, event.SubagentEnd)
		// The subagent's final answer, from the same field Stop reads. The
		// subagent transcript carries it too, but transcripts lag and are
		// deleted after thirty days; the payload is here now.
		e.Text = c.clean(&e, stopResponse(h))
		return []event.Event{e}

	case "PreCompact":
		// Worth recording even though compaction does not lose history: a
		// continuation spawns a new session file, and knowing where the seam
		// is makes the lineage explicable to a human reading the thread.
		e := c.base(h, event.Compaction)
		return []event.Event{e}

	default:
		return nil
	}
}

// transcriptExists reports whether the harness wrote anything for the session.
func transcriptExists(path string) bool {
	if path == "" {
		return false
	}
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir() && fi.Size() > 0
}

// toolResult translates PostToolUse and its failure variant, and emits a second
// FileChanged event when the tool mutated a file.
//
// The split is deliberate. A tool result and a file change are different things
// to a reader and to a query: "what did the agent run" and "what changed on
// disk" are separate questions, and collapsing them into one record makes the
// second one unanswerable without re-parsing payloads.
func (c *Capturer) toolResult(h HookEvent, failed bool) []event.Event {
	typ := event.ToolResult
	if failed {
		typ = event.ToolFailed
	}
	e := c.base(h, typ)
	e.ToolUseID = h.ToolUseID
	t := &event.Tool{Name: h.ToolName, Input: c.cleanRaw(&e, h.ToolInput), Error: h.Error}

	var tr toolResponse
	if len(h.ToolResponse) > 0 {
		_ = json.Unmarshal(h.ToolResponse, &tr)
	}

	out := tr.Stdout
	if out == "" {
		out = tr.Output
	}
	// Capped before scrubbing: the scrubber's cost is linear in the text, and
	// what is cut off is never stored, so it never needs scrubbing.
	out, cut := backfill.CapText(out, backfill.MaxToolOutputBytes)
	t.Output = c.clean(&e, out)
	t.Truncated = cut

	// Output over roughly 30k characters is elided in the hook payload but
	// spooled complete next to the transcript. Recording the path lets the
	// uploader ship the full text; without it the archive silently contains the
	// truncated version of exactly the outputs that were interesting enough to
	// be long.
	if tr.PersistedOutputAt != "" {
		t.OutputPath = tr.PersistedOutputAt
		t.Truncated = true
	}
	e.Tool = t
	evs := []event.Event{e}

	if d := c.diffFrom(&e, tr); d != nil {
		fc := c.base(h, event.FileChanged)
		fc.ToolUseID = h.ToolUseID
		fc.Tool = &event.Tool{Name: h.ToolName, Diff: d}
		evs = append(evs, fc)
	}
	return evs
}

func (c *Capturer) diffFrom(e *event.Event, tr toolResponse) *event.Diff {
	if tr.FilePath == "" {
		return nil
	}
	after := tr.NewString
	if after == "" {
		after = tr.Content
	}
	// A file creation reports a null originalFile; an edit reports the previous
	// contents. Distinguishing them matters because an empty Before is
	// ambiguous on its own: editing an empty file also has an empty Before.
	created := tr.OriginalFile == nil
	var before string
	if tr.OriginalFile != nil {
		before = *tr.OriginalFile
	}
	if before == "" && after == "" && len(tr.StructuredPatch) == 0 {
		return nil
	}
	// Each side capped on its own: a capped diff is still a diff, whereas an
	// item the server refuses for size is a lost turn.
	before, cutB := backfill.CapText(before, backfill.MaxDiffSideBytes)
	after, cutA := backfill.CapText(after, backfill.MaxDiffSideBytes)
	return &event.Diff{
		Path:      tr.FilePath,
		Before:    c.clean(e, before),
		After:     c.clean(e, after),
		Patch:     c.cleanRaw(e, tr.StructuredPatch),
		Created:   created,
		Truncated: cutB || cutA,
	}
}

// clean scrubs text and accumulates the redaction counts onto the event.
func (c *Capturer) clean(e *event.Event, s string) string {
	if s == "" {
		return ""
	}
	out, counts := c.scrub(s)
	for k, v := range counts {
		if e.Redactions == nil {
			e.Redactions = map[string]int{}
		}
		e.Redactions[k] += v
	}
	return out
}

// cleanRaw scrubs a JSON blob. Scrubbing preserves the prefix of anchored
// patterns so the result stays parseable; if a rule ever broke that invariant
// we would rather ship valid JSON with a marker than a corrupt document, so the
// result is re-validated and dropped to a string on failure.
func (c *Capturer) cleanRaw(e *event.Event, raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	cleaned := c.clean(e, string(raw))
	if json.Valid([]byte(cleaned)) {
		return json.RawMessage(cleaned)
	}
	b, err := json.Marshal(cleaned)
	if err != nil {
		return nil
	}
	return b
}

// FileSeq allocates sequence numbers backed by a file per session, so numbering
// survives the process exiting between hooks, which it does, because each hook
// invocation is its own short-lived process.
type FileSeq struct{ Dir string }

// Next returns the next sequence number for a session.
//
// The counter is stored per session rather than globally so concurrent sessions
// cannot interleave their numbering, and it is read-modify-written under an
// exclusive create so two hooks firing at once cannot both take the same value.
// A failure here degrades to a timestamp-derived number rather than returning
// an error: a mis-ordered event is recoverable at query time, a lost one is not.
// The server's head-state rule keys hook streams on occurred_at rather than on
// this number for exactly that reason: the fallback is nanoseconds, not 1.
func (f FileSeq) Next(sessionID string) int64 {
	if f.Dir == "" {
		return time.Now().UnixNano()
	}
	if err := os.MkdirAll(f.Dir, 0o700); err != nil {
		return time.Now().UnixNano()
	}
	p := filepath.Join(f.Dir, sanitize(sessionID)+".seq")

	// Retry briefly: contention here is two hooks in the same session firing
	// close together, which resolves in microseconds.
	for range 50 {
		lock := p + ".lock"
		lf, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			time.Sleep(time.Millisecond)
			continue
		}
		var n int64
		if b, err := os.ReadFile(p); err == nil {
			_, _ = fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &n)
		}
		n++
		_ = os.WriteFile(p, []byte(fmt.Sprintf("%d\n", n)), 0o600)
		_ = lf.Close()
		_ = os.Remove(lock)
		return n
	}
	return time.Now().UnixNano()
}

// sanitize keeps a session id usable as a filename. Session ids are UUIDs in
// practice, but a hostile or malformed one must not be able to escape the
// directory.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" {
		return "unknown"
	}
	if len(out) > 128 {
		out = out[:128]
	}
	return out
}
