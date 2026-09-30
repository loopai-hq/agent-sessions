// Package event defines the canonical, tool-agnostic session record.
//
// Everything the platform stores is one of these, whether it arrived from a
// live Claude Code hook, a historical transcript replayed during backfill, or
// (later) a Codex rollout. Keeping the shape tool-agnostic is what makes adding
// a second harness a parser change rather than a schema migration that ripples
// through ingest, storage, search, the ACL and the UI.
//
// Two design rules carry most of the weight:
//
// Event time is not ingest time. A backfilled session must be
// indistinguishable from one captured live except for when it arrived, so
// OccurredAt is always the moment the thing actually happened and the server
// stamps arrival separately. Anything that conflates them makes historical data
// lie about the past.
//
// Raw is preserved. Derived fields are a convenience layer over the original
// record, never a replacement for it: the source of truth stays verbatim so a
// parser bug is recoverable by reprocessing rather than by re-collecting data
// that has since been deleted from the laptop.
package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Type enumerates what happened. The set is deliberately small and maps onto
// concepts every agent harness has, rather than mirroring one vendor's hook
// names.
type Type string

// The event types, in the order a session tends to produce them.
const (
	// SessionStarted opens a session.
	SessionStarted Type = "session_started"
	// SessionEnded closes it.
	SessionEnded Type = "session_ended"
	// UserPrompt is what the person typed.
	UserPrompt Type = "user_prompt"
	// AssistantTurn is the model's answer, with its usage.
	AssistantTurn Type = "assistant_turn"
	// ToolCall is the model invoking a tool.
	ToolCall Type = "tool_call"
	// ToolResult is what the tool returned.
	ToolResult Type = "tool_result"
	// ToolFailed is a tool call that errored.
	ToolFailed Type = "tool_failed"
	// FileChanged is a file a tool wrote.
	FileChanged Type = "file_changed"
	// SubagentStart is a subagent being spawned.
	SubagentStart Type = "subagent_start"
	// SubagentEnd is that subagent finishing.
	SubagentEnd Type = "subagent_end"
	// Compaction is the harness compacting its context.
	Compaction Type = "compaction"
	// Artifact is an artifact the session produced.
	Artifact Type = "artifact"
)

// Source identifies the harness that produced the event.
type Source string

// The harnesses events come from.
const (
	// SourceClaudeCode is Claude Code.
	SourceClaudeCode Source = "claude_code"
	// SourceCodex is the Codex CLI.
	SourceCodex Source = "codex"
)

// Origin records how we came to have the event, which matters because a
// backfilled record carries weaker guarantees than a live one: hooks see tool
// results and diffs that the transcript alone does not preserve.
type Origin string

const (
	// OriginHook means a live lifecycle hook produced it. Highest fidelity.
	OriginHook Origin = "hook"
	// OriginTranscript means it was parsed from a session file, either during
	// backfill or by the reconciliation pass after a crash.
	OriginTranscript Origin = "transcript"
)

// Event is one thing that happened in a session.
type Event struct {
	// ID is the idempotency key. Deterministic wherever possible so a re-read
	// after a crash produces the same key and the server dedups rather than
	// double-counting. See DeterministicID.
	ID string `json:"id"`

	// CaptureVersion is the extraction-schema version of the agent that built
	// this event. It is what lets a later agent improve an event that is already
	// stored: the server keeps the copy with the highest version and discards
	// re-deliveries at or below it, so a re-walk after an extraction improvement
	// upgrades history instead of being deduplicated away by an id that has not
	// changed.
	//
	// Zero means an agent that predates the field, which every stored event does
	// at the time this was added. Zero is therefore lower than every real
	// version, and the first re-walk upgrades them.
	CaptureVersion int `json:"capture_version,omitempty"`

	Source Source `json:"source"`
	Origin Origin `json:"origin"`
	Type   Type   `json:"type"`

	// SessionID is the harness's own session identifier.
	SessionID string `json:"session_id"`
	// ParentSessionID links a continuation to the session it resumed from
	// (Claude Code's logicalParentUuid). Without this a single logical task
	// spread across several files is counted as several tasks, which corrupts
	// any rework or turns-to-completion metric.
	ParentSessionID string `json:"parent_session_id,omitempty"`
	// AgentID is set on subagent events and names the subagent file; subagent
	// transcripts carry the PARENT's session id, so this is the only thing that
	// distinguishes them.
	AgentID string `json:"agent_id,omitempty"`
	// WorkflowID groups subagents spawned by one workflow run.
	WorkflowID string `json:"workflow_id,omitempty"`

	// The anchors below are the identities that let a hook-captured event
	// and the transcript walker's copy of the same moment be recognised as
	// one thing. Without them the server holds two unrelated rows per turn
	// for every session captured both ways, and the rollup counts both.
	//
	// PromptID is the harness's id for the human turn in progress: the hook
	// payload's prompt_id and the transcript record's promptId are the same
	// value, which makes it the primary cross-origin key for a turn.
	PromptID string `json:"prompt_id,omitempty"`
	// ToolUseID pairs a tool call with its result and with the transcript's
	// tool_use block, on both capture paths.
	ToolUseID string `json:"tool_use_id,omitempty"`
	// RecordUUID is the transcript record's own uuid. It is the identity of a
	// transcript-origin event: a re-walk that renumbers a file still names
	// the same records, so the server can upgrade rather than duplicate.
	RecordUUID string `json:"record_uuid,omitempty"`
	// ParentRecordUUID carries the compaction marker (logicalParentUuid). It
	// names a record inside the same file, never another session, which is
	// why it is kept apart from ParentSessionID.
	ParentRecordUUID string `json:"parent_record_uuid,omitempty"`
	// LineageSource says how ParentSessionID was established ("fork_uuid",
	// "fork_hook"). ParentSessionID is meaningful only when this is set;
	// consumers must ignore it otherwise.
	LineageSource string `json:"lineage_source,omitempty"`
	// ForkPrefixSeq is the last sequence number of the history a fork copied
	// from its origin, stamped on the child's session_started, so a reader
	// can tell inherited turns from the child's own.
	ForkPrefixSeq int64 `json:"fork_prefix_seq,omitempty"`
	// Launcher records what started the harness (SessionStart only). It is
	// what tells a person's terminal session from a GUI host that spawns and
	// discards CLI processes, which is the class behind lifecycle-only
	// sessions.
	Launcher *Launcher `json:"launcher,omitempty"`
	// TranscriptExists reports, at SessionEnd, whether the harness ever wrote
	// a transcript for the session. A session with none never carried a
	// prompt, whatever hooks may have been lost.
	TranscriptExists *bool `json:"transcript_exists,omitempty"`
	// HarnessTitle is the harness's own generated title (the ai-title record),
	// a secondary label for readers; it is never the derived title source.
	HarnessTitle string `json:"harness_title,omitempty"`

	// Seq orders events within a session. Arrival order is not preserved across
	// retries, so the server sorts on this instead.
	Seq int64 `json:"seq"`

	// OccurredAt is when it happened, never when we shipped it.
	OccurredAt time.Time `json:"occurred_at"`

	// Cwd and GitBranch situate the work. Cwd is also how a session maps to a
	// repo, and therefore how repo-scoped capture policy is applied.
	Cwd       string `json:"cwd,omitempty"`
	GitBranch string `json:"git_branch,omitempty"`
	// HarnessVersion is recorded per event, not per session: a single machine
	// runs several versions concurrently, so a session-level version would be
	// a lie for some of its records.
	HarnessVersion string `json:"harness_version,omitempty"`
	// Entrypoint is the harness's own word for how it was started, extracted
	// as a named field so classification never has to parse Raw: "cli" and
	// "claude-desktop" are a person, "sdk-cli" is headless -p / SDK / cron,
	// and codex runs carry their session_meta originator here. Empty when the
	// producing record does not say — hook payloads do not, so the hook path
	// reads it off the transcript head instead.
	Entrypoint string `json:"entrypoint,omitempty"`
	// MirrorRequest is the per-session ask to mirror this session into Slack
	// live, read off the LOOP_SESSIONS_SLACK environment variable by the
	// SessionStart hook (the hook process inherits the launching shell's env).
	// "on" means the person's preferred destination; a C… id names a channel.
	// It is a REQUEST, not a grant: the server intersects it with the owner's
	// server-side Slack preferences, and a person whose mode is off stays off
	// whatever a shell variable says — env vars are not consent.
	MirrorRequest string `json:"mirror_request,omitempty"`

	Model string `json:"model,omitempty"`
	Usage *Usage `json:"usage,omitempty"`
	Tool  *Tool  `json:"tool,omitempty"`
	Text  string `json:"text,omitempty"`

	// Raw is the original record, scrubbed but otherwise untouched. Derived
	// fields above are conveniences; this is the record of truth.
	Raw json.RawMessage `json:"raw,omitempty"`

	// Redactions counts secrets removed from this event, by kind. Surfaced so a
	// machine whose scrubber is firing constantly is visible rather than
	// silently shipping near-misses.
	Redactions map[string]int `json:"redactions,omitempty"`
}

// Launcher identifies the process that started the harness, read from the
// environment the hook inherits. Every field is optional: a laptop without the
// variable simply reports nothing, which is itself a signal (a GUI host does
// not set a terminal program).
type Launcher struct {
	// Entrypoint is CLAUDE_CODE_ENTRYPOINT as the harness sets it for hooks.
	Entrypoint string `json:"entrypoint,omitempty"`
	// BundleID is the macOS __CFBundleIdentifier of the launching app.
	BundleID string `json:"bundle_id,omitempty"`
	// Term is TERM_PROGRAM, the terminal emulator when there is one.
	Term string `json:"term,omitempty"`
	// ParentComm is the parent process's command name when it can be read
	// without spawning anything.
	ParentComm string `json:"parent_comm,omitempty"`
}

// Usage is token accounting for one model call. The cache fields are not
// optional detail: on real sessions cache traffic is the overwhelming majority
// of cost, so a schema that only tracked input and output would misreport
// spending by roughly an order of magnitude.
type Usage struct {
	InputTokens         int64 `json:"input_tokens,omitempty"`
	OutputTokens        int64 `json:"output_tokens,omitempty"`
	CacheReadTokens     int64 `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens int64 `json:"cache_creation_tokens,omitempty"`
	// Ephemeral5m and Ephemeral1h split cache creation by TTL because they are
	// billed at different multipliers (1.25x and 2x).
	Ephemeral5m int64 `json:"cache_creation_5m,omitempty"`
	Ephemeral1h int64 `json:"cache_creation_1h,omitempty"`

	ServiceTier string `json:"service_tier,omitempty"`
	// MessageID is the dedup key for usage: the same message can legitimately
	// appear in more than one transcript record, and counting it twice
	// inflates cost. RequestID is the API call's other id, kept beside the
	// message id as a stored column on the event (never in the ledger key)
	// and carried to the analytics export; the server pairs a hook copy with
	// its transcript copy by message id.
	RequestID string `json:"request_id,omitempty"`
	MessageID string `json:"message_id,omitempty"`
}

// Tool describes a tool invocation and, where the harness gives it to us, what
// the tool actually did.
type Tool struct {
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input,omitempty"`

	// Output is the tool's result. Claude Code truncates this in the hook
	// payload at 30k characters but spools the full text alongside the
	// transcript; OutputPath points at that spool so the capture layer can ship
	// the untruncated version.
	Output     string `json:"output,omitempty"`
	OutputPath string `json:"output_path,omitempty"`
	Truncated  bool   `json:"truncated,omitempty"`

	// Diff carries the real before/after for file-mutating tools. This is the
	// difference between an archive that records what was asked and one that
	// records what was done.
	Diff *Diff `json:"diff,omitempty"`

	Error string `json:"error,omitempty"`
}

// Diff is a file change produced by a tool.
type Diff struct {
	Path string `json:"path"`
	// Before is empty for a file creation.
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
	// Patch is the harness's structured unified diff when available; cheaper to
	// store and render than two full file bodies.
	Patch json.RawMessage `json:"patch,omitempty"`
	// Created distinguishes a new file from an edit, which Before being empty
	// cannot do on its own (editing an empty file also has an empty Before).
	Created bool `json:"created,omitempty"`
	// Truncated reports that Before or After was cut at the capture cap. A
	// capped diff is still a diff; an item the server refuses for size is a
	// lost turn, and the cap exists to prevent the second outcome. It lives
	// here rather than on Tool because it describes these two fields alone.
	Truncated bool `json:"truncated,omitempty"`
}

// CaptureSchema is the version of the extraction that produces events.
//
// Bump it when a change makes the agent extract something BETTER or DIFFERENT
// from the same transcript: a field that was being dropped, text that was being
// mangled, a redaction rule that was letting something through. A machine
// recorded below CaptureBaseline re-walks its history once and the server
// replaces the stored copies with the improved ones; a machine at or above the
// baseline adopts the version and walks nothing.
//
// Do NOT bump it for:
//
//   - delivery, daemon, CLI, install or upgrade changes; they extract nothing
//   - server-only changes; those re-derive server-side, see DerivedSchema
//
// Changing WHICH events a transcript emits used to be a third exclusion, and
// the reason was arithmetic: sequence numbers are assigned in walk order and
// hashed into every id, so one extra event partway through a session renumbers
// everything after it. That is still true of the ids. It stopped mattering
// when ingest began recognising a transcript-origin event by its record
// identity (session, agent, record uuid, type, tool use id) rather than by its
// id alone: a re-walk that renumbers a file still names the same records, and
// the server upgrades the stored rows instead of storing a second copy. See
// docs/UPGRADES.md.
//
// Version history:
//
//	1  the extraction as it stood when versioning was introduced
//	2  events carry Entrypoint, so the server can tell an automation session
//	   from a user-initiated one; adds a field to existing events, emits
//	   nothing new, so ids are stable and the re-walk upgrades in place
//	3  turns carry Usage on both harnesses: a live Claude Code Stop reads the
//	   turn's cost from the transcript it is handed, and a Codex turn takes it
//	   from the token_count record that was previously dropped on the floor.
//	   Both attach a field to an event that was already being emitted, so ids
//	   are stable here too and the re-walk upgrades in place
//	4  every event carries the anchors the other capture path also carries
//	   (prompt_id, tool_use_id, the record uuid) and the compaction marker as
//	   parent_record_uuid rather than as a parent session; the live Stop and
//	   SubagentStop carry the final answer from last_assistant_message; forks
//	   are named on the child's session_started; legacy root-level agent files
//	   are their own streams; slash commands emit the person's words; tool
//	   output and diffs are capped instead of being refused whole; ten newer
//	   bookkeeping record types are ignored by name. The server derives every
//	   one of these keys for rows it already holds from the raw record it
//	   stored, so no machine re-walks for this version (CaptureBaseline is 4
//	   too); the sessions that need their answers or their Codex tokens
//	   repaired are named by the server and walked one file at a time
const CaptureSchema = 4

// CaptureBaseline is the highest version whose arrival needs no re-walk.
//
// A machine recorded below it adopts the current version and walks nothing.
// It is 4 because everything version 4 adds to a stored event is something the
// server can derive from the raw record it already holds, and a blanket
// re-walk of 4.8 million rows for keys the server has anyway is nine minutes
// of CPU and a few hundred megabytes per machine to rewrite bodies with a
// higher number in one column. What a re-walk genuinely cannot do (credit
// tokens to rows that were stored before the extraction read them, add the
// answers the old Stop handler never captured) is done by the repair walk,
// which the server drives one session at a time.
//
// This is the rule in docs/UPGRADES.md applied twice: at the rule's own
// introduction (version 1 changed no extraction) and again here.
//
// Raise this only alongside CaptureSchema, and only when the versions between
// them changed nothing the server cannot derive for stored rows.
const CaptureBaseline = 4

// DeterministicID derives a stable idempotency key from the identity of the
// event rather than from when we happened to process it.
//
// This is what makes at-least-once delivery safe. A crash mid-backfill, a
// truncation re-read, or a reconciliation pass that revisits a session all
// produce the same key for the same underlying record, so the server's upsert
// absorbs the duplicate. Using a random id here instead would turn every
// recovery path into silent data duplication.
func DeterministicID(sessionID string, seq int64, typ Type, discriminator string) string {
	h := sha256.New()
	// Field separators prevent adjacent fields from running together and
	// colliding: ("ab", 1) and ("a", 11) must not hash alike.
	fmt.Fprintf(h, "%s\x00%d\x00%s\x00%s", sessionID, seq, typ, discriminator)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// Validate reports why an event cannot be accepted.
//
// Validation happens client-side so a malformed record is caught on the machine
// that produced it, where the context to diagnose it still exists, rather than
// becoming an opaque server-side reject long after the session is gone.
func (e *Event) Validate() error {
	var problems []string
	if e.SessionID == "" {
		problems = append(problems, "session_id is required")
	}
	if e.Type == "" {
		problems = append(problems, "type is required")
	}
	if e.Source == "" {
		problems = append(problems, "source is required")
	}
	if e.Origin == "" {
		problems = append(problems, "origin is required")
	}
	if e.OccurredAt.IsZero() {
		problems = append(problems, "occurred_at is required and must be the real event time")
	}
	if e.ID == "" {
		problems = append(problems, "id is required for idempotent delivery")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid event: %s", strings.Join(problems, "; "))
	}
	return nil
}

// Session is the rollup the dashboard lists and the ACL protects. It is derived
// from events and can always be rebuilt from them, so it is a cache rather than
// a second source of truth.
type Session struct {
	SessionID       string    `json:"session_id"`
	ParentSessionID string    `json:"parent_session_id,omitempty"`
	Source          Source    `json:"source"`
	Cwd             string    `json:"cwd,omitempty"`
	Repo            string    `json:"repo,omitempty"`
	GitBranch       string    `json:"git_branch,omitempty"`
	StartedAt       time.Time `json:"started_at"`
	EndedAt         time.Time `json:"ended_at,omitempty"`

	// Ended distinguishes a session we saw finish from one that simply stopped
	// producing events. A crashed session has no end marker, and treating
	// "no more events" as "ended" would silently mislabel exactly the messy
	// sessions this system exists to capture.
	Ended bool `json:"ended"`

	UserTurns    int            `json:"user_turns"`
	ToolCalls    int            `json:"tool_calls"`
	Subagents    int            `json:"subagents"`
	Compactions  int            `json:"compactions"`
	Errors       int            `json:"errors"`
	TotalUsage   Usage          `json:"total_usage"`
	FirstPrompt  string         `json:"first_prompt,omitempty"`
	HarnessVers  []string       `json:"harness_versions,omitempty"`
	RedactionSum map[string]int `json:"redactions,omitempty"`
}

// Apply folds an event into a session rollup. Folding rather than recomputing
// keeps ingest incremental, which matters because sessions arrive in pieces
// over minutes and re-deriving a multi-thousand-event session on every arrival
// would not hold the freshness budget.
func (s *Session) Apply(e Event) {
	if s.SessionID == "" {
		s.SessionID = e.SessionID
		s.Source = e.Source
	}
	if e.ParentSessionID != "" {
		s.ParentSessionID = e.ParentSessionID
	}
	if e.Cwd != "" {
		s.Cwd = e.Cwd
	}
	if e.GitBranch != "" {
		s.GitBranch = e.GitBranch
	}
	if e.HarnessVersion != "" && !contains(s.HarnessVers, e.HarnessVersion) {
		s.HarnessVers = append(s.HarnessVers, e.HarnessVersion)
	}
	if s.StartedAt.IsZero() || e.OccurredAt.Before(s.StartedAt) {
		s.StartedAt = e.OccurredAt
	}
	if e.OccurredAt.After(s.EndedAt) {
		s.EndedAt = e.OccurredAt
	}

	switch e.Type {
	case UserPrompt:
		s.UserTurns++
		if s.FirstPrompt == "" {
			s.FirstPrompt = truncate(e.Text, 500)
		}
	case ToolCall:
		s.ToolCalls++
	case ToolFailed:
		s.Errors++
	case SubagentStart:
		s.Subagents++
	case Compaction:
		s.Compactions++
	case SessionEnded:
		s.Ended = true
	}

	if e.Usage != nil {
		s.TotalUsage.InputTokens += e.Usage.InputTokens
		s.TotalUsage.OutputTokens += e.Usage.OutputTokens
		s.TotalUsage.CacheReadTokens += e.Usage.CacheReadTokens
		s.TotalUsage.CacheCreationTokens += e.Usage.CacheCreationTokens
		s.TotalUsage.Ephemeral5m += e.Usage.Ephemeral5m
		s.TotalUsage.Ephemeral1h += e.Usage.Ephemeral1h
	}
	for k, v := range e.Redactions {
		if s.RedactionSum == nil {
			s.RedactionSum = map[string]int{}
		}
		s.RedactionSum[k] += v
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Trim at a rune boundary so the stored prefix stays valid UTF-8.
	for n > 0 && !utf8Start(s[n]) {
		n--
	}
	return s[:n]
}

func utf8Start(b byte) bool { return b&0xC0 != 0x80 }
