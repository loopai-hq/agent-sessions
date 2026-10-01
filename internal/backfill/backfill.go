// Package backfill turns historical Claude Code transcripts on disk into
// event.Event values that are temporally accurate.
//
// The output is meant to be indistinguishable from live hook capture except for
// Origin, which is always OriginTranscript. That single property, that every
// OccurredAt is the timestamp the transcript recorded and never the moment we
// imported it, is the entire point. A backfill that stamps import time answers
// "what did we import" instead of "what happened", and the second question is
// the only reason to backfill at all.
//
// # Layout
//
// Verified against 6349 files on a real machine, plus the legacy shape Claude
// Code 2.0.x wrote. Five path shapes exist under a discovery root:
//
//	<project>/<uuid>.jsonl                                        main transcript
//	<project>/<uuid>/subagents/agent-<name>.jsonl                 flat subagent
//	<project>/<uuid>/subagents/workflows/wf_<id>/agent-<name>.jsonl  workflow subagent
//	<project>/<uuid>/subagents/workflows/wf_<id>/journal.jsonl     workflow journal
//	<project>/agent-<id>.jsonl                                    legacy subagent (2.0.x)
//
// Subagent transcripts carry the PARENT session's sessionId, so AgentID and
// WorkflowID come from the path and are the only thing separating a subagent's
// events from its parent's. The legacy shape sits at the project root with
// nothing in its path to say so; its records say so instead (isSidechain,
// agentId), and the walker believes the records.
//
// # Anchors and lineage
//
// Every event carries the record's uuid, its promptId and its tool_use ids,
// which are the keys the server dedupes a hook copy against. logicalParentUuid
// is a compaction marker naming a record inside the same file and is carried
// as ParentRecordUUID; it is never a session. The only lineage the harness
// makes provable is a fork: a new file that begins with the origin's records,
// uuids and timestamps intact. Forks are found by indexing the first content
// uuid of every file in a project, and the child's session_started names its
// parent and how much of the file it inherited. The copied prefix is still
// emitted, because resuming the child needs it; the server folds it out.
//
// # What is not emitted
//
// SessionEnded and SubagentEnd are never produced. A transcript that stops
// could equally be a finished session or a crashed one, and the event contract
// is explicit that treating "no more events" as "ended" mislabels exactly the
// messy sessions this system exists to capture. Reaching the end of a file is
// not evidence of an ending.
package backfill

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/normalize"
)

// maxLineBytes is the scanner's per-line ceiling. bufio.Scanner defaults to
// 64 KB, which is far too small: the largest single line measured across a real
// 2.81 GB corpus was 1.4 MB, and tool output is only getting larger. A var
// rather than a const so tests can shrink it and exercise the overflow path.
var maxLineBytes = 16 << 20

// initialLineBytes is the starting buffer. Most lines are small, so start there
// and let bufio grow towards maxLineBytes only for the rare enormous record.
const initialLineBytes = 64 << 10

// LineageFork is the LineageSource stamped on a fork's session_started.
const LineageFork = "fork_uuid"

// Options configures a Walk.
type Options struct {
	// Root is a discovery root, e.g. ~/.claude/projects.
	Root string
	// Since is the import window, applied per FILE: a file with any record at
	// or after Since is emitted whole, and a file whose newest record is
	// older is emitted not at all. A per-event window cut sessions in half
	// (the brief's "sessions starting from random messages": ~590 of them),
	// and a session is either worth importing or it is not. Zero takes
	// everything. Numbering is unaffected either way.
	Since time.Time
	// Scrub redacts secrets and reports what it removed, by kind. Injected
	// rather than imported so this package stays testable and the dependency
	// runs one way. A nil Scrub disables redaction.
	Scrub func(string) (string, map[string]int)
	// Done reports that a file's events were already handed to the consumer by
	// an earlier walk, so a resumed import does not send them a second time.
	//
	// A file it reports true for is still opened and parsed. Sequence numbers
	// are assigned in walk order and an event's ID is derived from its sequence
	// number, so skipping the parse would renumber every later event in the same
	// stream: the same records would arrive under new ids, and the server would
	// store a second copy of history it already has. Re-reading a file costs
	// disk; renumbering costs a duplicate of somebody's past. Redaction, which
	// is the expensive part of a walk, is skipped for these files because
	// nothing from them is emitted.
	//
	// Done and Since compose per file: Done wins. A file first imported under
	// a narrow window was emitted whole and recorded, so a later, wider walk
	// emits nothing for it; a file that has grown since is no longer Done (the
	// journal compares size and mtime) and is emitted whole again, which the
	// server dedups by record identity.
	//
	// A nil Done walks everything, which is what a first import does.
	Done func(path string) bool
	// OnSession, if set, is called once per file after it is processed.
	OnSession func(Progress)
	// Session, when set, restricts the walk to the files that belong to one
	// session id and ignores Done and Since for them. It is what
	// `backfill --session <id>` and the repair pass use: a session named by
	// the server is walked whole whether or not the journal says it was sent.
	Session string
	// Only, when set, decides record by record whether its events are
	// emitted. It is how the recovery pass walks only the records the hooks
	// lost. Numbering is unaffected: a record it declines still consumes its
	// sequence numbers, so the ids of what it accepts are the ids a full walk
	// would give them.
	Only func(Anchor) bool
}

// Anchor is what a content record can be recognised by on the other capture
// path. The recovery pass subtracts a session's hook ledger from its
// transcript's anchors to find the records the hooks lost.
type Anchor struct {
	// Type is the record type: user, assistant or system.
	Type string
	UUID string
	// PromptID is the human turn the record belongs to, when the harness
	// wrote one.
	PromptID string
	// ToolUseIDs are the tool_use block ids on an assistant record or the
	// tool_use_id of a tool_result user record.
	ToolUseIDs []string
	// IsToolResult marks a user record that carries tool output rather than
	// a prompt.
	IsToolResult bool
	// IsMeta marks a harness-injected user record.
	IsMeta bool
}

// Progress reports one file's contribution as the walk proceeds.
type Progress struct {
	File     string
	Sessions int
	Events   int
	Bytes    int64
	Err      error
	// Done marks a file that Options.Done claimed: it was read for its sequence
	// numbers but nothing from it was emitted, so Sessions and Events are zero.
	// It is how a caller tells "this file was already sent" from "this file had
	// nothing in it", which otherwise look identical.
	Done bool
}

// Result is the tally for a whole walk.
//
// Sessions and Events count what this walk emitted, not what its files hold: a
// file Options.Done claimed contributes to Files and Bytes, because it was read,
// and to Resumed, but not to the two numbers a caller reports as "imported".
type Result struct {
	Sessions int
	Events   int
	Files    int
	Bytes    int64
	// Resumed counts files an earlier walk had already delivered.
	Resumed int
	// Skipped records files that produced nothing, and per-file advisories for
	// files that produced events with caveats (malformed lines dropped,
	// timestamps carried forward). Both belong here because both are things an
	// operator needs to see rather than infer from a count that looks low.
	Skipped []Skip
}

// Skip is one path and why it was skipped, or what was odd about it.
type Skip struct {
	Path   string
	Reason string
}

// Walk parses every transcript under opts.Root and hands each event to emit.
//
// One bad file never aborts the walk: unreadable, empty, malformed and
// schema-foreign files are recorded in Result.Skipped and the walk continues.
// Only an error returned by emit stops it, since that means the consumer cannot
// accept more, and in that case the partial Result is returned alongside.
func Walk(opts Options, emit func(event.Event) error) (Result, error) {
	var res Result
	if opts.Root == "" {
		return res, errors.New("backfill: Root is required")
	}
	if emit == nil {
		return res, errors.New("backfill: emit is required")
	}

	files, err := collect(opts.Root)
	if err != nil {
		return res, err
	}

	w := &walker{
		opts:   opts,
		emit:   emit,
		res:    &res,
		seq:    make(map[string]int64),
		seen:   make(map[string]bool),
		parent: make(map[string]string),
	}
	w.index(files)

	for _, f := range files {
		if opts.Session != "" && !w.belongsTo(f, opts.Session) {
			continue
		}
		if err := w.file(f); err != nil {
			return res, err
		}
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// File discovery and classification
// ---------------------------------------------------------------------------

type kind int

const (
	kindSession kind = iota
	kindAgent
	kindJournal
)

type target struct {
	path string
	kind kind
	// agentID names the subagent file, e.g. "agent-a0729299bbff7f031".
	agentID string
	// workflowID is the wf_* directory, e.g. "wf_733c9f64-e9d".
	workflowID string
	// sessionDir is <project>/<uuid>, where the tool-results spool lives.
	// Empty for a legacy root-level agent file, whose parent directory is
	// not knowable from its path.
	sessionDir string
}

// collect finds every .jsonl under root and returns it in a stable order.
//
// The sort is load-bearing, not cosmetic: sequence numbers are assigned in walk
// order, so an unstable order would produce different IDs on every run and
// defeat the whole point of DeterministicID.
func collect(root string) ([]target, error) {
	var out []target
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// An unreadable directory is not fatal; skip its subtree.
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(p, ".jsonl") {
			return nil
		}
		out = append(out, classify(p))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("backfill: walk %s: %w", root, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
}

// classify derives lineage from the path. Everything about which stream a file
// belongs to is encoded in its location, because the file contents cannot
// distinguish a subagent from its parent, with one exception: the 2.0.x
// layout put subagent files at the project root, named agent-<id>.jsonl, and
// nothing but the name and the records inside says what they are.
func classify(p string) target {
	t := target{path: p, kind: kindSession}
	base := filepath.Base(p)

	if base == "journal.jsonl" {
		t.kind = kindJournal
	}

	// Find the "subagents" element; everything to its left is the session dir.
	parts := strings.Split(filepath.ToSlash(p), "/")
	idx := -1
	for i, seg := range parts {
		if seg == "subagents" {
			idx = i
			break
		}
	}
	if idx <= 0 {
		if t.kind != kindJournal && strings.HasPrefix(base, "agent-") {
			// Legacy subagent at the project root. Walked as its own stream
			// rather than as the parent's: its records carry the parent's
			// sessionId, and appending them to the parent's stream produced
			// a spurious session_started, a "Warmup" prompt and renumbered
			// everything after it.
			t.kind = kindAgent
			t.agentID = strings.TrimSuffix(base, ".jsonl")
			return t
		}
		// Main transcript: <project>/<uuid>.jsonl. The spool directory is the
		// same path with the .jsonl suffix removed.
		t.sessionDir = strings.TrimSuffix(p, ".jsonl")
		return t
	}

	t.sessionDir = filepath.FromSlash(strings.Join(parts[:idx], "/"))
	if t.kind != kindJournal {
		t.kind = kindAgent
		t.agentID = strings.TrimSuffix(base, ".jsonl")
	}
	// A workflow tier inserts workflows/wf_<id>/ between subagents/ and the file.
	for _, seg := range parts[idx+1:] {
		if strings.HasPrefix(seg, "wf_") {
			t.workflowID = seg
			break
		}
	}
	return t
}

// sessionOf is the session a target's path names, or "" when the path does
// not say (a legacy agent file).
func sessionOf(t target) string {
	switch {
	case t.kind == kindSession:
		return strings.TrimSuffix(filepath.Base(t.path), ".jsonl")
	case t.sessionDir != "":
		return filepath.Base(t.sessionDir)
	}
	return ""
}

// belongsTo reports whether a target is one of a session's files. A legacy
// agent file's path names no session, so its head is read.
func (w *walker) belongsTo(t target, session string) bool {
	if s := sessionOf(t); s != "" {
		return s == session
	}
	h, _ := ReadHead(t.path)
	return h.SessionID == session
}

// ---------------------------------------------------------------------------
// Transcript records
// ---------------------------------------------------------------------------

// record is one transcript line. Fields absent from the struct still survive in
// Raw, which is the contract's record of truth.
type record struct {
	Type      string `json:"type"`
	SessionID string `json:"sessionId"`
	Timestamp string `json:"timestamp"`
	Cwd       string `json:"cwd"`
	GitBranch string `json:"gitBranch"`
	Version   string `json:"version"`
	UUID      string `json:"uuid"`
	RequestID string `json:"requestId"`

	// LogicalParentUUID is the compaction marker: the uuid of the last record
	// written before the conversation was compacted, inside this same file.
	// It was once read as a parent SESSION and produced 384 parents of which
	// none resolved; it is carried as ParentRecordUUID and nothing else.
	LogicalParentUUID string `json:"logicalParentUuid"`

	IsCompactSummary bool `json:"isCompactSummary"`
	IsSidechain      bool `json:"isSidechain"`

	// Entrypoint is how the harness was started; sdk-cli marks automation.
	Entrypoint string `json:"entrypoint"`

	// Provenance the harness writes on newer versions. None of it changes
	// which events are emitted; it is carried so the server's normalizer can
	// classify a user record without re-parsing Raw, and so the walker can
	// stamp the anchors a hook copy of the same moment also carries.
	//
	// PromptID is the human turn the record belongs to, the same value the
	// hook payload calls prompt_id. IsMeta marks harness-injected user records
	// (caveats, reminders, skill bodies). Origin.Kind and PromptSource say who
	// authored a user record when the harness knows. TurnCompanion marks the
	// expanded skill body that rides beside a slash command.
	// IsVisibleInTranscriptOnly marks the compaction summary the model never
	// saw as a prompt. AgentID on a record inside a main transcript means the
	// file is a subagent stream that was misfiled at the project root, which
	// Claude Code 2.0.x did. Subtype names system records (compact_boundary,
	// turn_duration). InterruptedMessageID ties an interruption to the turn it
	// cut short. AITitle is the ai-title header's value.
	IsMeta        bool   `json:"isMeta"`
	PromptID      string `json:"promptId"`
	PromptSource  string `json:"promptSource"`
	TurnCompanion bool   `json:"turnCompanion"`
	AgentID       string `json:"agentId"`
	Subtype       string `json:"subtype"`
	AITitle       string `json:"aiTitle"`

	IsVisibleInTranscriptOnly bool   `json:"isVisibleInTranscriptOnly"`
	InterruptedMessageID      string `json:"interruptedMessageId"`
	Origin                    *struct {
		Kind string `json:"kind"`
	} `json:"origin"`
	ToolUseResult json.RawMessage `json:"toolUseResult"`

	Message *message `json:"message"`
}

type message struct {
	Role  string `json:"role"`
	Model string `json:"model"`
	ID    string `json:"id"`
	Usage *usage `json:"usage"`
	// Content is a string on user turns typed at the prompt and an array of
	// blocks otherwise. Both shapes occur in real transcripts.
	Content json.RawMessage `json:"content"`
}

type usage struct {
	InputTokens         int64 `json:"input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
	CacheReadTokens     int64 `json:"cache_read_input_tokens"`
	CacheCreationTokens int64 `json:"cache_creation_input_tokens"`
	CacheCreation       *struct {
		Ephemeral5m int64 `json:"ephemeral_5m_input_tokens"`
		Ephemeral1h int64 `json:"ephemeral_1h_input_tokens"`
	} `json:"cache_creation"`
	ServiceTier string `json:"service_tier"`
}

type block struct {
	Type     string          `json:"type"`
	Text     string          `json:"text"`
	Thinking string          `json:"thinking"`
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Input    json.RawMessage `json:"input"`

	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
}

// syntheticModel marks rows that are not real API calls. Their usage must be
// excluded or reported cost inflates.
const syntheticModel = "<synthetic>"

// ignoredTypes are records that carry no session content: UI bookkeeping,
// editor state and file-history tracking. Inventing an event type for them
// would put noise into every metric derived from the stream.
//
// Recognising them by name matters for the skip report as much as for the
// output. Several of these shapes are structurally incomplete (file-history-*
// records have no sessionId at all, and the header records have no timestamp)
// so a walker that did not know them would flag roughly 150 files as anomalous
// on a healthy machine, and the genuine problems would be lost in the noise.
//
// The set was derived by scanning 6196 real transcripts and refreshed against
// a 2026-09 corpus of 1,062 files, which had grown ten types the first scan
// never saw. The three types most documentation mentions (user, assistant,
// system) are a minority of what actually appears.
var ignoredTypes = map[string]bool{
	// UI and queue bookkeeping.
	"attachment":      true,
	"queue-operation": true,
	"last-prompt":     true,
	"summary":         true,
	// Per-file header metadata, written before the first timestamped record.
	// ai-title is read by the head scan, not here.
	"mode":            true,
	"permission-mode": true,
	"bridge-session":  true,
	"ai-title":        true,
	"agent-name":      true,
	"custom-title":    true,
	// File-history tracking. These carry no sessionId whatsoever.
	"file-history-snapshot": true,
	"file-history-delta":    true,
	// Newer bookkeeping: editor latches, PR and frame links, worktree and
	// relocation state, artifact monitors, cost snapshots, progress.
	"atis-latch":                true,
	"pr-link":                   true,
	"worktree-state":            true,
	"relocated":                 true,
	"frame-link":                true,
	"history-suppression":       true,
	"artifact-autoreact-ledger": true,
	"artifact-comment-monitor":  true,
	"cost-state":                true,
	"progress":                  true,
}

// ---------------------------------------------------------------------------
// Walking
// ---------------------------------------------------------------------------

type walker struct {
	opts Options
	emit func(event.Event) error
	res  *Result

	// seq counts events per stream, keyed by session id plus agent id. A
	// subagent is its own stream even though it shares the parent's session id.
	seq map[string]int64
	// seen tracks streams already counted towards Result.Sessions.
	seen map[string]bool
	// parent remembers a stream's compaction marker once seen.
	//
	// logicalParentUuid does not sit on the records that carry content. In the
	// reference corpus every occurrence is on a type:"system" compact_boundary
	// row, which has no message body and so produces no event of its own, and
	// it appears anywhere from line 7 to line 4948. Remembering it per stream
	// and stamping it on every subsequent event as ParentRecordUUID keeps the
	// seam findable from any event after it.
	parent map[string]string

	// heads is the per-file head scan (title, first content record) and forks
	// the fork relation, both built by index before any file is walked.
	heads map[string]Head
	forks map[string]forkInfo

	// done is set for the duration of a file Options.Done claimed. It suppresses
	// emission and redaction while leaving the numbering above untouched, which
	// is the whole contract of a resume.
	done bool
	// outside is set for a file entirely older than the import window.
	outside bool
	// st is the file currently being read. It is held here so that push can do
	// the accounting where the emission happens: an event counts when it is
	// emitted and not before. Counting it at construction instead is how Result
	// came to report events that a filter had thrown away.
	st *fileState

	// mute is set for a record none of whose events will be emitted: the file
	// was already delivered, the file predates the window, or Only declined
	// the record. It suppresses redaction alone.
	//
	// Redaction is what a walk actually spends its time on (2.81 GB of real
	// transcripts take about nine minutes with it and well under one without),
	// so a thirty-day import that redacted the whole corpus before discarding
	// most of it would make the bounded window pointless. Nothing here changes
	// what is emitted or how it is numbered; the events are still built and
	// still consume their sequence numbers.
	mute bool
}

// forkInfo describes a file that begins with another file's records.
type forkInfo struct {
	parentSession string
	// prefixSeq is the seq of the last event built from a copied record.
	prefixSeq int64
}

func (w *walker) skip(path, reason string) {
	w.res.Skipped = append(w.res.Skipped, Skip{Path: path, Reason: reason})
}

// fileState is the per-file accumulator.
type fileState struct {
	// events counts what was emitted; built counts what was parsed into an
	// event before any filter saw it. The advisories below need the second: a
	// file every one of whose events fell outside the window, or whose events an
	// earlier run already delivered, is not a file we failed to read, and saying
	// so about six thousand files at once buries the handful that are genuinely
	// damaged.
	events    int
	built     int
	sessions  int
	malformed int
	noSession int
	noClock   int
	carried   int
	ignored   int
	sidechain int
	// lastTS is the carry-forward clock. A record with no timestamp inherits it
	// rather than being stamped with time.Now(), which would be a lie.
	lastTS    time.Time
	sessions0 map[string]bool
}

// fileBirth is birthTime behind a variable, so a test can stand in the mtime
// fallback that every platform but darwin lives with.
var fileBirth = birthTime

// index reads the head of every main transcript once, to find the harness
// titles and the forks. A fork is a file whose first content record's uuid
// also opens another file in the same project: the harness copies the
// origin's records verbatim, uuids and timestamps included, and rewrites only
// the sessionId and the promptId. No key in any record names the origin, so
// the overlap is the only evidence there is.
//
// Which file is the origin is decided by the promptId, birth time second. In
// the one pair the corpus holds (r1 section 2.5), the copy's prefix carries
// the promptId of the copy's own first turn: the harness stamps the records
// it loads with the id of the prompt that resumed them, so that id turns up
// again on records the origin never had. The origin's first-turn promptId
// turns up nowhere outside the shared prefix on a record that could open a
// turn. That is the whole test, and it holds on any filesystem; birth time
// (mtime where there is no birth time) only breaks the ties the test cannot,
// such as a copy abandoned before its first prompt or a harness that stamps
// the prefix differently.
//
// The test looks only at records that can open a turn because of the fork
// taken mid-turn: an origin copied between a tool call and its result keeps
// writing that turn, and every later record of it carries the first-turn id
// outside the shared prefix. Tool results never open a turn and are not
// counted (contentPrompts). The user records the harness injects mid-turn
// under the turn's promptId still count: r1 section 2.13 observes
// <teammate-message> and <task-notification> records taking the id of the
// human turn in progress, and section 2.7 the <local-command-stdout> that
// follows a local command. This walker has no classifier, telling those from
// a typed prompt is text matching that belongs to the server's normalizer,
// and most of those records carry no isMeta at all (r1 section 2.8: 382 of
// 454 teammate messages), so isMeta is no filter either. An origin that took
// a teammate message or a task notification in its first turn after the copy
// point therefore reads as rewritten and falls to the tie-break like the
// other two shapes.
//
// Cost: one bounded head read and one stat per main file, which the walk
// pays anyway a moment later, plus one full read of each member of a fork
// group, which is rare (one pair in 1,062 files on the reference machine). A
// walk for one named session reads the heads of every file in the project,
// because a fork candidate is identified by its head and nothing else, but
// it reads whole files only for the group that session belongs to.
func (w *walker) index(files []target) {
	w.heads = make(map[string]Head, len(files))
	w.forks = make(map[string]forkInfo)

	type born struct {
		t     target
		h     Head
		birth time.Time
		// prompts is uuid -> promptId for every content record; rewrote is
		// the promptId test above.
		prompts map[string]string
		rewrote bool
	}
	groups := map[string][]born{}
	for _, t := range files {
		if t.kind != kindSession {
			continue
		}
		h, ok := ReadHead(t.path)
		if !ok {
			continue
		}
		w.heads[t.path] = h
		if h.FirstUUID == "" {
			continue
		}
		fi, err := os.Stat(t.path)
		if err != nil {
			continue
		}
		key := filepath.Dir(t.path) + "\x00" + h.FirstUUID
		groups[key] = append(groups[key], born{t: t, h: h, birth: fileBirth(fi)})
	}

	for _, g := range groups {
		if len(g) < 2 {
			continue
		}
		if w.opts.Session != "" && !groupHolds(g, func(b born) bool { return sessionOf(b.t) == w.opts.Session }) {
			continue
		}
		sets := make([]map[string]string, len(g))
		for i := range g {
			g[i].prompts = contentPrompts(g[i].t.path)
			sets[i] = g[i].prompts
		}
		shared := sharedUUIDs(sets)
		for i := range g {
			g[i].rewrote = promptLeaks(g[i].prompts, shared, g[i].h.FirstPromptID)
		}
		// Origin first; then the file that existed first; then the earlier
		// path, which is deterministic if nothing else.
		sort.SliceStable(g, func(i, j int) bool {
			if g[i].rewrote != g[j].rewrote {
				return !g[i].rewrote
			}
			if !g[i].birth.Equal(g[j].birth) {
				return g[i].birth.Before(g[j].birth)
			}
			return g[i].t.path < g[j].t.path
		})
		parent := g[0]
		parentUUIDs := make(map[string]bool, len(parent.prompts))
		for id := range parent.prompts {
			parentUUIDs[id] = true
		}
		for _, child := range g[1:] {
			w.forks[child.t.path] = forkInfo{
				parentSession: sessionOf(parent.t),
				prefixSeq:     w.prefixSeq(child.t, parentUUIDs),
			}
		}
	}
}

// groupHolds reports whether any member of a fork group satisfies pred.
func groupHolds[T any](g []T, pred func(T) bool) bool {
	for _, m := range g {
		if pred(m) {
			return true
		}
	}
	return false
}

// contentPrompts reads a file once and maps every content record's uuid to
// the promptId the leak test may count: "" for the types that carry none and
// for a record that cannot open a turn. A tool_result user record belongs to
// the turn already in progress (r1 section 3.3: the human record of a turn
// has no tool_result block and no toolUseResult), so whatever promptId the
// harness stamps on it (r4 Q5 leaves open whether it stamps one) says
// nothing about which file rewrote the prefix. Counted, it would mark an
// origin forked mid-turn as rewritten, because the tool results it records
// after the copy point carry its first-turn id outside the shared prefix,
// and hand the decision to the tie-break; on mtime that tie-break picks the
// copy.
func contentPrompts(path string) map[string]string {
	out := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, initialLineBytes), maxLineBytes)
	for sc.Scan() {
		var r struct {
			Type          string          `json:"type"`
			UUID          string          `json:"uuid"`
			PromptID      string          `json:"promptId"`
			ToolUseResult json.RawMessage `json:"toolUseResult"`
			Message       *message        `json:"message"`
		}
		if json.Unmarshal(sc.Bytes(), &r) != nil || r.UUID == "" || !isContentType(r.Type) {
			continue
		}
		p := r.PromptID
		if _, blocks := r.Message.content(); hasToolResult(blocks) || isJSONValue(r.ToolUseResult) {
			p = ""
		}
		out[r.UUID] = p
	}
	return out
}

// isJSONValue reports whether a raw field was present and not null.
func isJSONValue(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// sharedUUIDs is the set of uuids every member of a fork group holds: the
// copied prefix, as far as the whole group agrees on it.
func sharedUUIDs(sets []map[string]string) map[string]bool {
	shared := map[string]bool{}
	if len(sets) == 0 {
		return shared
	}
	for id := range sets[0] {
		shared[id] = true
	}
	for _, s := range sets[1:] {
		for id := range shared {
			if _, ok := s[id]; !ok {
				delete(shared, id)
			}
		}
	}
	return shared
}

// promptLeaks reports whether a file's first-turn promptId is also carried by
// a record outside the shared prefix that could have opened a turn. In a
// copy it is: the harness stamps the loaded prefix with the id of the prompt
// that resumed it, and that prompt's own records follow. Records that cannot
// open a turn were already blanked by contentPrompts, so an origin forked
// mid-turn does not leak through its own tool results. A file without a
// promptId cannot be told either way and is left to the birth-time tie-break.
func promptLeaks(prompts map[string]string, shared map[string]bool, first string) bool {
	if first == "" {
		return false
	}
	for id, p := range prompts {
		if p == first && !shared[id] {
			return true
		}
	}
	return false
}

// prefixSeq counts the events the copied prefix of a fork child builds, which
// is the seq of the last inherited event: session_started takes seq 0 and the
// prefix records take the numbers after it, in the order the walk assigns
// them. The count uses the same builder the walk uses and skips the same
// records line skips, so it cannot drift from what the walk actually emits.
func (w *walker) prefixSeq(child target, parentUUIDs map[string]bool) int64 {
	f, err := os.Open(child.path)
	if err != nil {
		return 0
	}
	defer f.Close()
	saved := w.mute
	w.mute = true // no redaction while counting
	defer func() { w.mute = saved }()

	var n int64
	var lastTS time.Time
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, initialLineBytes), maxLineBytes)
	for sc.Scan() {
		var r record
		if json.Unmarshal(sc.Bytes(), &r) != nil || ignoredTypes[r.Type] || r.SessionID == "" {
			continue
		}
		if r.UUID != "" && isContentType(r.Type) && !parentUUIDs[r.UUID] {
			break
		}
		// The two skips line applies after attribution, mirrored so a prefix
		// that begins with a clockless record, or carries a misfiled
		// sidechain record, does not count an event the walk never emits.
		if (r.IsSidechain || r.AgentID != "") && n > 0 {
			continue
		}
		if ts, ok := parseTime(r.Timestamp); ok {
			lastTS = ts
		} else if lastTS.IsZero() {
			continue
		}
		n += int64(len(w.build(target{}, r, event.Event{})))
	}
	return n
}

func (w *walker) file(t target) error {
	fi, err := os.Stat(t.path)
	if err != nil {
		w.skip(t.path, "stat: "+err.Error())
		w.report(t.path, 0, 0, 0, err)
		return nil
	}
	if fi.Size() == 0 {
		w.skip(t.path, "empty file")
		w.report(t.path, 0, 0, 0, nil)
		return nil
	}
	if t.kind == kindJournal {
		// journal.jsonl has an unrelated schema: top-level type/key/agentId/
		// result, with neither sessionId nor timestamp. Nothing in it can be
		// made temporally accurate, so it is skipped by name rather than fed
		// through the parser to produce garbage.
		w.skip(t.path, "journal.jsonl: unrelated schema (no sessionId, no timestamp)")
		w.report(t.path, 0, 0, fi.Size(), nil)
		return nil
	}

	// Done and the window are per-file decisions, and a session the caller
	// named is walked regardless of both.
	forced := w.opts.Session != ""
	w.done = !forced && w.opts.Done != nil && w.opts.Done(t.path)
	w.outside = !forced && !w.inWindow(t.path, fi)

	f, err := os.Open(t.path)
	if err != nil {
		w.skip(t.path, "open: "+err.Error())
		w.report(t.path, 0, 0, 0, err)
		return nil
	}
	defer f.Close()

	st := &fileState{sessions0: map[string]bool{}}
	w.st = st
	sc := bufio.NewScanner(f)
	// The starting buffer must not exceed the ceiling. bufio only grows when a
	// token outruns the current buffer, so an initial buffer larger than
	// maxTokenSize silently raises the real limit and the overflow path becomes
	// unreachable.
	initial := initialLineBytes
	if initial > maxLineBytes {
		initial = maxLineBytes
	}
	sc.Buffer(make([]byte, initial), maxLineBytes)

	for sc.Scan() {
		line := sc.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		if err := w.line(&t, line, st); err != nil {
			return err
		}
	}

	scanErr := sc.Err()
	if scanErr != nil {
		// A line past maxLineBytes stops the scanner; the rest of the file is
		// unreachable. Record it loudly instead of silently returning a short
		// read that looks like a complete file.
		reason := "scan: " + scanErr.Error()
		if errors.Is(scanErr, bufio.ErrTooLong) {
			reason = fmt.Sprintf("scan aborted: line exceeds %d bytes; remainder of file not read", maxLineBytes)
		}
		w.skip(t.path, reason)
	}

	w.res.Files++
	w.res.Bytes += fi.Size()
	w.res.Events += st.events
	w.res.Sessions += st.sessions
	if w.done {
		w.res.Resumed++
	}

	// Advisories are reported whether or not the file yielded events. A file
	// that produced nothing is exactly when the operator needs to know which of
	// the causes below was responsible.
	if st.built == 0 && scanErr == nil {
		w.skip(t.path, "no usable records (no sessionId or no content)")
	}
	if st.malformed > 0 {
		w.skip(t.path, fmt.Sprintf("%d malformed JSON lines skipped", st.malformed))
	}
	if st.noSession > 0 {
		w.skip(t.path, fmt.Sprintf("%d records had no sessionId; skipped", st.noSession))
	}
	if st.noClock > 0 {
		w.skip(t.path, fmt.Sprintf("%d records had no timestamp and no predecessor; skipped rather than stamped with import time", st.noClock))
	}
	if st.carried > 0 {
		w.skip(t.path, fmt.Sprintf("%d records had no timestamp; carried forward last known", st.carried))
	}
	if st.sidechain > 0 {
		w.skip(t.path, fmt.Sprintf("%d subagent (isSidechain) records inside a main transcript; skipped rather than attributed to the main thread", st.sidechain))
	}

	w.report(t.path, st.sessions, st.events, fi.Size(), scanErr)
	return nil
}

// inWindow decides whether a file has anything inside the import window,
// from its newest record (the tail) or, failing a parseable one, its mtime.
func (w *walker) inWindow(path string, fi os.FileInfo) bool {
	if w.opts.Since.IsZero() {
		return true
	}
	if last := LastTimestamp(path); !last.IsZero() {
		return !last.Before(w.opts.Since)
	}
	return !fi.ModTime().Before(w.opts.Since)
}

func (w *walker) report(path string, sessions, events int, bytes int64, err error) {
	if w.opts.OnSession == nil {
		return
	}
	w.opts.OnSession(Progress{File: path, Sessions: sessions, Events: events, Bytes: bytes, Err: err, Done: w.done})
}

func (w *walker) line(t *target, line []byte, st *fileState) error {
	var r record
	if err := json.Unmarshal(line, &r); err != nil {
		st.malformed++
		return nil
	}
	// Boilerplate is recognised before attribution: several of these shapes
	// legitimately have no sessionId, and checking that first would report them
	// as damage rather than as the routine records they are.
	if ignoredTypes[r.Type] {
		st.ignored++
		return nil
	}
	if r.SessionID == "" {
		// Without a session id an event cannot be attributed or validated.
		st.noSession++
		return nil
	}

	// Belt and braces for the legacy layout: a record that says it belongs to
	// a subagent never lands in a main stream. On the first record the file is
	// reclassified; after that the record is skipped and counted.
	if t.kind == kindSession && (r.IsSidechain || r.AgentID != "") {
		if len(st.sessions0) == 0 {
			t.kind = kindAgent
			t.agentID = agentName(r.AgentID, filepath.Base(t.path))
			t.sessionDir = ""
		} else {
			st.sidechain++
			return nil
		}
	}

	ts, ok := parseTime(r.Timestamp)
	if !ok {
		if st.lastTS.IsZero() {
			// Nothing to carry forward yet. Stamping now() would fabricate
			// history, so drop the record instead.
			st.noClock++
			return nil
		}
		ts = st.lastTS
		st.carried++
	} else {
		st.lastTS = ts
	}
	_, blocks := r.Message.content()
	w.mute = w.done || w.outside || (w.opts.Only != nil && !w.opts.Only(anchorOf(r, blocks)))

	// Scrub the raw record once. Redaction counts come from this pass alone:
	// derived fields are substrings of the same record, so counting them again
	// would multiply every secret by the number of places it was copied to.
	rawText, redactions := w.scrub(string(line))

	stream := r.SessionID + "\x00" + t.agentID
	if r.LogicalParentUUID != "" {
		w.parent[stream] = r.LogicalParentUUID
	}

	base := event.Event{
		Source:           event.SourceClaudeCode,
		Origin:           event.OriginTranscript,
		SessionID:        r.SessionID,
		ParentRecordUUID: w.parent[stream],
		AgentID:          t.agentID,
		WorkflowID:       t.workflowID,
		PromptID:         r.PromptID,
		RecordUUID:       r.UUID,
		OccurredAt:       ts,
		Cwd:              r.Cwd,
		GitBranch:        r.GitBranch,
		HarnessVersion:   r.Version,
		Entrypoint:       r.Entrypoint,
		Model:            r.Message.model(),
		Raw:              json.RawMessage(rawText),
		Redactions:       redactions,
	}

	if !st.sessions0[stream] {
		st.sessions0[stream] = true
		// The first record of a stream is proof it started. There is no
		// corresponding end event; see the package doc.
		start := base
		start.Type = event.SessionStarted
		if t.kind == kindAgent {
			start.Type = event.SubagentStart
		}
		start.Usage = nil
		if h, ok := w.heads[t.path]; ok {
			start.HarnessTitle = h.AITitle
		}
		if fk, ok := w.forks[t.path]; ok {
			// Provable lineage, and only this: the file opens with another
			// file's records. The prefix is emitted below like any other
			// records; the server folds inherited turns out by this marker.
			start.ParentSessionID = fk.parentSession
			start.LineageSource = LineageFork
			start.ForkPrefixSeq = fk.prefixSeq
		}
		if err := w.push(*t, &start, r.UUID, "start"); err != nil {
			return err
		}
	}

	for _, b := range w.build(*t, r, base) {
		if err := w.push(*t, &b.ev, r.UUID, b.disc); err != nil {
			return err
		}
	}
	return nil
}

// agentName turns a record's agentId (or a legacy file name) into the stream's
// AgentID, in the form the path-based classifier produces: "agent-<id>".
func agentName(agentID, base string) string {
	if agentID == "" {
		return strings.TrimSuffix(base, ".jsonl")
	}
	if strings.HasPrefix(agentID, "agent-") {
		return agentID
	}
	return "agent-" + agentID
}

// built is one event a record produces, with the discriminator that keeps its
// id distinct from its siblings'.
type built struct {
	ev   event.Event
	disc string
}

// build constructs the content events of one transcript line, in the order
// they are numbered. It is the one place a record's events are enumerated:
// the walk pushes what it returns and the fork index counts it, so the two
// cannot disagree about how many sequence numbers a record consumes.
func (w *walker) build(t target, r record, base event.Event) []built {
	text, blocks := r.Message.content()
	isToolResult := hasToolResult(blocks)

	// A compaction summary, by flag or by its opening words. The observed fork
	// copy of one carried origin:human and no flag; the text is the guard of
	// last resort. Only user-role prose qualifies: a tool result quoting the
	// phrase is not a compaction.
	if r.IsCompactSummary || (r.Type == "user" && !isToolResult && IsCompactSummaryText(prose(text, blocks))) {
		e := base
		e.Type = event.Compaction
		e.Text = w.scrubText(prose(text, blocks))
		return []built{{ev: e, disc: "compaction"}}
	}

	u := r.Message.usage()
	if u != nil {
		u.RequestID = r.RequestID
	}
	var out []built

	// Usage belongs to exactly one event per record. A record is one API call,
	// so attaching it to every block would multiply reported spend by the block
	// count. It rides the turn event where there is one, else the first tool
	// call.
	attachUsage := func(e *event.Event) {
		if u != nil {
			e.Usage = u
			u = nil
		}
	}

	// A turn event exists when the record carried prose, reasoning, or usage.
	var turnText string
	hasProse := false
	for _, b := range blocks {
		switch b.Type {
		case "text":
			if b.Text != "" {
				if turnText != "" {
					turnText += "\n"
				}
				turnText += b.Text
				hasProse = true
			}
		case "thinking":
			// Reasoning is preserved verbatim in Raw. It is not folded into
			// Text, which is the user-visible turn, but it does mean the record
			// produced a turn.
			if b.Thinking != "" {
				hasProse = true
			}
		}
	}
	if text != "" {
		turnText = text
		hasProse = true
	}

	if hasProse || u != nil {
		e := base
		e.Type = event.AssistantTurn
		if r.Type == "user" {
			e.Type = event.UserPrompt
			// A slash command arrives wrapped: <command-message>, then
			// <command-name>, then the person's words in <command-args>. The
			// words are the prompt; the wrapper stays in Raw for the server's
			// classifier, and the hook path sends the same "/name args" shape.
			if args, ok := slashCommandText(turnText); ok {
				turnText = args
			}
		}
		e.Text = w.scrubText(turnText)
		attachUsage(&e)
		out = append(out, built{ev: e, disc: "turn"})
	}

	for i, b := range blocks {
		switch b.Type {
		case "tool_use":
			e := base
			e.Type = event.ToolCall
			e.ToolUseID = b.ID
			in, _ := w.scrub(string(b.Input))
			e.Tool = &event.Tool{Name: b.Name, Input: json.RawMessage(in)}
			attachUsage(&e)
			out = append(out, built{ev: e, disc: fmt.Sprintf("tool_use:%d:%s", i, b.ID)})

		case "tool_result":
			e := base
			e.Type = event.ToolResult
			e.ToolUseID = b.ToolUseID
			if b.IsError {
				e.Type = event.ToolFailed
			}
			// Capped at the same byte the live path caps at, so the two copies
			// of one result agree, and before scrubbing, because what is cut
			// is never stored.
			raw, cut := CapText(flatten(b.Content), MaxToolOutputBytes)
			outText := w.scrubText(raw)
			tl := &event.Tool{Output: outText, Truncated: cut}
			if b.IsError {
				tl.Error = outText
			}
			// Output over ~30k characters is spooled beside the transcript
			// rather than inlined. Presence of the spool file IS the truncation
			// signal; the transcript carries no flag for it.
			if p, ok := spoolPath(t.sessionDir, b.ToolUseID); ok {
				tl.OutputPath = p
				tl.Truncated = true
			}
			e.Tool = tl
			out = append(out, built{ev: e, disc: fmt.Sprintf("tool_result:%d:%s", i, b.ToolUseID)})
		}
	}
	return out
}

// anchorOf is the record's identity on the other capture path.
func anchorOf(r record, blocks []block) Anchor {
	a := Anchor{Type: r.Type, UUID: r.UUID, PromptID: r.PromptID, IsMeta: r.IsMeta, IsToolResult: hasToolResult(blocks)}
	for _, b := range blocks {
		switch b.Type {
		case "tool_use":
			a.ToolUseIDs = append(a.ToolUseIDs, b.ID)
		case "tool_result":
			a.ToolUseIDs = append(a.ToolUseIDs, b.ToolUseID)
		}
	}
	return a
}

func hasToolResult(blocks []block) bool {
	for _, b := range blocks {
		if b.Type == "tool_result" {
			return true
		}
	}
	return false
}

// prose is the record's text regardless of content shape.
func prose(text string, blocks []block) string {
	if text != "" {
		return text
	}
	var parts []string
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// slashCommandText extracts the human's words from a slash-command wrapper:
// <command-args> when present, else "/<command-name>". Anything that does not
// open with the wrapper is left alone.
func slashCommandText(s string) (string, bool) {
	trimmed := strings.TrimSpace(s)
	if !strings.HasPrefix(trimmed, "<command-message>") && !strings.HasPrefix(trimmed, "<command-name>") {
		return "", false
	}
	if args := strings.TrimSpace(between(trimmed, "<command-args>", "</command-args>")); args != "" {
		return args, true
	}
	if name := strings.TrimSpace(between(trimmed, "<command-name>", "</command-name>")); name != "" {
		if !strings.HasPrefix(name, "/") {
			name = "/" + name
		}
		return name, true
	}
	return "", false
}

func between(s, opener, closer string) string {
	i := strings.Index(s, opener)
	if i < 0 {
		return ""
	}
	rest := s[i+len(opener):]
	j := strings.Index(rest, closer)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// push assigns the sequence number and id, applies the emission filters, and
// hands the event to the consumer.
//
// Sequence numbers advance for every event constructed, including those a
// filter mutes. That is deliberate: if a filter changed the numbering, the same
// record would get a different ID depending on the window or resume point it
// was imported under, and re-importing a wider range would duplicate rather
// than dedup.
func (w *walker) push(t target, e *event.Event, recordUUID, discriminator string) error {
	stream := e.SessionID + "\x00" + t.agentID
	seq := w.seq[stream]
	w.seq[stream] = seq + 1

	e.Seq = seq
	disc := strings.Join([]string{t.agentID, t.workflowID, recordUUID, discriminator}, "|")
	e.ID = event.DeterministicID(e.SessionID, seq, e.Type, disc)
	// Stamped here rather than at each construction site so a new event type
	// cannot be added without it: an unstamped event reads as version zero and
	// would be replaced by the next re-walk that produced it properly.
	e.CaptureVersion = event.CaptureSchema
	w.st.built++

	// The filters run after the id is assigned, for the same reason: an
	// event's id must describe the record it came from, never the window or the
	// resume point it was imported under. See the note above and Options.Done.
	if w.mute {
		return nil
	}
	if err := w.emit(*e); err != nil {
		return err
	}

	w.st.events++
	// A stream counts as a session the first time one of its events is actually
	// emitted. A stream whose every event fell outside the window, or whose file
	// an earlier run already delivered, is not a session this walk imported, and
	// reporting it as one makes the totals disagree with the events.
	if !w.seen[stream] {
		w.seen[stream] = true
		w.st.sessions++
	}
	return nil
}

func (w *walker) scrub(s string) (string, map[string]int) {
	// Nothing from a muted record is emitted, so redacting it would be paid for
	// and thrown away, and redaction is the slowest thing a walk does.
	if w.opts.Scrub == nil || w.mute || s == "" {
		return s, nil
	}
	out, counts := w.opts.Scrub(s)
	if len(counts) == 0 {
		counts = nil
	}
	return out, counts
}

// scrubText redacts without reporting. Counts are taken once from the raw
// record; see the note in walker.line.
func (w *walker) scrubText(s string) string {
	out, _ := w.scrub(s)
	return out
}

// Anchors lists the content records of one transcript file, in file order. It
// is the transcript side of the recovery diff.
func Anchors(path string) ([]Anchor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Anchor
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, initialLineBytes), maxLineBytes)
	for sc.Scan() {
		var r record
		if json.Unmarshal(sc.Bytes(), &r) != nil || ignoredTypes[r.Type] || r.SessionID == "" || !isContentType(r.Type) {
			continue
		}
		_, blocks := r.Message.content()
		out = append(out, anchorOf(r, blocks))
	}
	return out, sc.Err()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseTime accepts the RFC3339 form Claude Code writes
// ("2026-08-04T15:40:33.123Z"). A false return means carry the clock forward.
func parseTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	if ts, err := time.Parse(time.RFC3339, s); err == nil {
		return ts.UTC(), true
	}
	return time.Time{}, false
}

func (m *message) model() string {
	if m == nil {
		return ""
	}
	return m.Model
}

// usage converts the wire form, dropping synthetic rows. "<synthetic>" records
// are not API calls; counting their usage inflates cost.
//
// The request id lives on the record, not the message, so the two callers
// stamp it after this returns (the walker at the record, LastAssistantRecord
// at the tail). Until 2026-09-15 it was deliberately left empty on both
// paths: the server's usage ledger keyed on (message_id, request_id) with
// every stored row carrying an empty request_id, so a copy that filled it in
// would have billed the same call a second time. The ledger now keys on the
// message id alone (usageKey and creditUsage in server/store/store.go write
// "" for request_id; the 0001 primary key is unchanged and no migration was
// needed), so the stamp can no longer make a
// second key. The server keeps the value in events.request_id beside the
// message id, as a stored column for the export and for queries; the derive
// fold pairs a hook copy with its transcript twin by message id.
func (m *message) usage() *event.Usage {
	if m == nil || m.Usage == nil || m.Model == syntheticModel {
		return nil
	}
	s := m.Usage
	u := &event.Usage{
		InputTokens:         s.InputTokens,
		OutputTokens:        s.OutputTokens,
		CacheReadTokens:     s.CacheReadTokens,
		CacheCreationTokens: s.CacheCreationTokens,
		ServiceTier:         s.ServiceTier,
		MessageID:           m.ID,
	}
	if s.CacheCreation != nil {
		u.Ephemeral5m = s.CacheCreation.Ephemeral5m
		u.Ephemeral1h = s.CacheCreation.Ephemeral1h
	}
	if *u == (event.Usage{}) {
		return nil
	}
	return u
}

// spent reports whether a usage record accounts for any tokens at all.
func spent(u *event.Usage) bool {
	return u != nil && u.InputTokens+u.OutputTokens+u.CacheReadTokens+
		u.CacheCreationTokens+u.Ephemeral5m+u.Ephemeral1h > 0
}

// content normalises the two shapes message.content takes: a bare string for a
// typed user prompt, or an array of blocks.
func (m *message) content() (string, []block) {
	if m == nil || len(m.Content) == 0 {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(m.Content, &s); err == nil {
		return s, nil
	}
	var bs []block
	if err := json.Unmarshal(m.Content, &bs); err == nil {
		return "", bs
	}
	return "", nil
}

// flatten renders a tool_result body, which is a string in most records and an
// array of content blocks in the rest. The rule lives in the normalize
// package so that the server's skill derivation reads a user record's
// content the way the walker does, and the two cannot drift.
func flatten(raw json.RawMessage) string {
	return normalize.FlattenContent(raw)
}

// spoolPath reports the sidecar holding a tool's full output, if it exists.
// Claude Code names these <session-dir>/tool-results/<tool_use_id>.txt.
func spoolPath(sessionDir, toolUseID string) (string, bool) {
	if sessionDir == "" || toolUseID == "" {
		return "", false
	}
	p := filepath.Join(sessionDir, "tool-results", toolUseID+".txt")
	if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
		return p, true
	}
	return "", false
}
