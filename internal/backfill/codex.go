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

	"github.com/loopai-hq/agent-sessions/internal/event"
)

type codexEnvelope struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type codexResponse struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Role      string          `json:"role"`
	Name      string          `json:"name"`
	CallID    string          `json:"call_id"`
	Arguments json.RawMessage `json:"arguments"`
	Input     json.RawMessage `json:"input"`
	Output    json.RawMessage `json:"output"`
	Content   json.RawMessage `json:"content"`
}

type codexContentBlock struct{ Type, Text string }

// codexTokenUsage is one turn's accounting as Codex reports it.
type codexTokenUsage struct {
	InputTokens       int64 `json:"input_tokens"`
	CachedInputTokens int64 `json:"cached_input_tokens"`
	OutputTokens      int64 `json:"output_tokens"`
	// ReasoningOutputTokens is read only to be explicit that it is not used.
	// Codex bills reasoning as output and reports it inside output_tokens, so
	// adding it in would count every reasoning token twice. It has no separate
	// home in event.Usage for the same reason.
	ReasoningOutputTokens int64 `json:"reasoning_output_tokens"`
}

// usage maps Codex's shape onto the canonical one.
//
// The subtraction is the whole point of this function. Codex counts cached
// input inside input_tokens — codex-rs derives its own displayed figure as
// input_tokens - cached_input_tokens — whereas event.Usage, and the pricer that
// reads it, treat the two as disjoint: cost is InputTokens at the full rate
// plus CacheReadTokens at the cache multiplier. Passing input_tokens through
// unchanged would bill every cached token twice, once at each rate, on a field
// that is usually the largest of the three.
//
// Codex reports no cache writes, so CacheCreationTokens and the TTL split that
// prices Claude's stay zero. There is no message id anywhere in the record,
// which is deliberate on the reading side too: usage with neither id keys off
// the event id in the ledger, and Codex event ids are deterministic, so a
// re-walk of the same rollout dedups against itself.
func (u *codexTokenUsage) usage() *event.Usage {
	if u == nil {
		return nil
	}
	out := &event.Usage{
		InputTokens:     max(u.InputTokens-u.CachedInputTokens, 0),
		OutputTokens:    u.OutputTokens,
		CacheReadTokens: u.CachedInputTokens,
	}
	if *out == (event.Usage{}) {
		return nil
	}
	return out
}

type codexWalker struct {
	opts   Options
	emit   func(event.Event) error
	result Result
	meta   map[string]codexMetadata
	seq    map[string]int64
}

type codexTarget struct {
	path    string
	meta    codexMetadata
	hasMeta bool
}

// WalkCodex parses Codex rollout files and emits their canonical events.
func WalkCodex(opts Options, emit func(event.Event) error) (Result, error) {
	if opts.Root == "" {
		return Result{}, errors.New("backfill: Codex Root is required")
	}
	if emit == nil {
		return Result{}, errors.New("backfill: Codex emit is required")
	}
	files, compressed, err := codexCollect(opts.Root)
	if err != nil {
		return Result{}, err
	}
	w := &codexWalker{
		opts: opts, emit: emit,
		meta: map[string]codexMetadata{}, seq: map[string]int64{},
	}
	for _, path := range compressed {
		w.skip(path, "compressed rollout (.jsonl.zst) is not supported yet; its sessions were not imported")
	}
	targets := make([]codexTarget, 0, len(files))
	for _, path := range files {
		m, ok := readCodexMetadata(path)
		targets = append(targets, codexTarget{path: path, meta: m, hasMeta: ok})
		if ok {
			w.meta[m.session()] = m
		}
	}
	for _, target := range targets {
		if opts.Session != "" && !w.codexBelongsTo(target, opts.Session) {
			continue
		}
		if err := w.file(target); err != nil {
			return w.result, err
		}
	}
	return w.result, nil
}

// CodexCwd is the project directory a Codex rollout's session_meta names, or
// "" when the file has none. It is what the exclusion policy is checked
// against before a rollout the server named is re-walked; a Claude head read
// of the same file would find no cwd and exclude nothing.
func CodexCwd(path string) string {
	m, ok := readCodexMetadata(path)
	if !ok {
		return ""
	}
	return m.Cwd
}

// codexBelongsTo reports whether a rollout is the named session or one of its
// subagents.
func (w *codexWalker) codexBelongsTo(t codexTarget, session string) bool {
	if !t.hasMeta {
		return false
	}
	root, _ := w.lineage(t.meta)
	return t.meta.session() == session || root == session
}

func codexCollect(root string) (files, compressed []string, err error) {
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		switch {
		case d.IsDir():
		case strings.HasSuffix(path, ".jsonl"):
			files = append(files, path)
		case strings.HasSuffix(path, ".jsonl.zst"):
			// Codex compresses older rollouts. This walker cannot read them yet,
			// and a gap in somebody's history has to be a declared gap: silently
			// collecting only the plain files would report a complete import
			// that quietly starts at whenever compression last ran.
			compressed = append(compressed, path)
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("backfill: walk Codex sessions under %s: %w", root, err)
	}
	sort.Strings(files)
	sort.Strings(compressed)
	return files, compressed, nil
}

func (w *codexWalker) lineage(m codexMetadata) (sessionID, agentID string) {
	id, parent := m.session(), m.parent()
	if parent == "" {
		return id, ""
	}
	agentID = id
	seen := map[string]bool{id: true}
	for parent != "" && !seen[parent] {
		seen[parent] = true
		pm, ok := w.meta[parent]
		if !ok {
			return parent, agentID
		}
		id, parent = pm.session(), pm.parent()
	}
	return id, agentID
}

func (w *codexWalker) file(target codexTarget) error {
	info, err := os.Stat(target.path)
	if err != nil {
		w.skip(target.path, "stat: "+err.Error())
		return nil
	}
	done := w.opts.Done != nil && w.opts.Done(target.path)
	if !target.hasMeta {
		w.skip(target.path, "no usable session_meta record")
		w.report(target.path, 0, 0, info.Size(), nil, done)
		w.result.Files++
		w.result.Bytes += info.Size()
		return nil
	}
	m := target.meta
	sessionID, agentID := w.lineage(m)
	f, err := os.Open(target.path)
	if err != nil {
		w.skip(target.path, "open: "+err.Error())
		return nil
	}
	defer f.Close()
	// The window is per file, as it is for Claude transcripts: a rollout with
	// any record inside it is emitted whole. A named session ignores both the
	// window and the journal.
	forced := w.opts.Session != ""
	done = done && !forced
	outside := !forced && !w.opts.Since.IsZero() && func() bool {
		if last := LastTimestamp(target.path); !last.IsZero() {
			return last.Before(w.opts.Since)
		}
		return info.ModTime().Before(w.opts.Since)
	}()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 64<<10), maxLineBytes)
	state := codexFileState{walker: w, meta: m, sessionID: sessionID, agentID: agentID, cwd: m.Cwd, done: done || outside}
	for s.Scan() {
		line := append([]byte(nil), s.Bytes()...)
		var env codexEnvelope
		if json.Unmarshal(line, &env) != nil {
			state.malformed++
			continue
		}
		ts, err := time.Parse(time.RFC3339Nano, env.Timestamp)
		if err != nil && env.Type == "session_meta" {
			ts, err = time.Parse(time.RFC3339Nano, m.Timestamp)
		}
		if err != nil {
			state.noTimestamp++
			continue
		}
		base := state.baseEvent(ts)
		if !state.started {
			state.started = true
			start := state.baseEvent(ts)
			start.Type = event.SessionStarted
			if state.agentID != "" {
				start.Type = event.SubagentStart
			}
			if err := state.emit(start, "start", line); err != nil {
				return err
			}
		}
		switch env.Type {
		case "turn_context":
			var c struct{ Model, Cwd string }
			if json.Unmarshal(env.Payload, &c) == nil {
				if c.Model != "" {
					state.model = c.Model
				}
				if c.Cwd != "" {
					state.cwd = c.Cwd
				}
			}
		case "response_item":
			var r codexResponse
			if json.Unmarshal(env.Payload, &r) != nil {
				continue
			}
			e, discriminator, ok := codexResponseEvent(base, r)
			if !ok {
				continue
			}
			e.Model, e.Cwd = state.model, state.cwd
			if err := state.emit(e, discriminator, line); err != nil {
				return err
			}
		case "compacted":
			var c struct {
				Message string `json:"message"`
			}
			_ = json.Unmarshal(env.Payload, &c)
			e := base
			e.Type, e.Text = event.Compaction, c.Message
			if err := state.emit(e, "compaction", line); err != nil {
				return err
			}
		case "event_msg":
			// The only event_msg worth reading. The rest of them — user_message,
			// agent_message, task_started — restate a response_item that is
			// already being turned into an event, and reading both would count
			// every turn twice.
			var p struct {
				Type string `json:"type"`
				Info *struct {
					// total_token_usage sits right beside this one and is the
					// running total for the whole session, not a delta. Adding
					// those up counts turn one again on every later record, so
					// a twenty-turn session reports something closer to two
					// hundred turns' worth of tokens. Only last_token_usage is
					// this turn, and it is the only one this reads.
					LastTokenUsage *codexTokenUsage `json:"last_token_usage"`
				} `json:"info"`
			}
			if json.Unmarshal(env.Payload, &p) != nil || p.Type != "token_count" || p.Info == nil {
				continue
			}
			state.attachUsage(p.Info.LastTokenUsage.usage())
		}
	}
	// The last event of the file is still held back; nothing follows it to
	// release it.
	if err := state.flush(); err != nil {
		return err
	}
	if err := s.Err(); err != nil {
		w.skip(target.path, "scan: "+err.Error())
	}
	if state.malformed > 0 {
		w.skip(target.path, fmt.Sprintf("%d malformed JSON lines skipped", state.malformed))
	}
	if state.noTimestamp > 0 {
		w.skip(target.path, fmt.Sprintf("%d records had no usable timestamp; skipped", state.noTimestamp))
	}
	w.result.Files++
	w.result.Bytes += info.Size()
	w.result.Events += state.events
	w.result.Sessions += state.sessionCount()
	if done {
		w.result.Resumed++
	}
	w.report(target.path, state.sessionCount(), state.events, info.Size(), s.Err(), done)
	return nil
}

type codexFileState struct {
	walker             *codexWalker
	meta               codexMetadata
	sessionID, agentID string
	model, cwd         string
	done, started      bool
	pending            *pendingCodexEvent
	ordinal            int64
	events             int
	malformed          int
	noTimestamp        int
}

func (s *codexFileState) baseEvent(at time.Time) event.Event {
	e := event.Event{
		Source: event.SourceCodex, Origin: event.OriginTranscript,
		SessionID: s.sessionID, AgentID: s.agentID, OccurredAt: at.UTC(),
		Cwd: s.cwd, Model: s.model, HarnessVersion: s.meta.CLIVersion,
		Entrypoint: s.meta.Originator,
	}
	if s.meta.Git != nil {
		e.GitBranch = s.meta.Git.Branch
	}
	return e
}

// pendingCodexEvent is the one event a file holds back, waiting to find out
// what it cost.
type pendingCodexEvent struct {
	event         event.Event
	discriminator string
	raw           []byte
}

// emit queues an event, releasing the one before it.
//
// Exactly one event is held back at a time, and that is what lets a token_count
// record bill the turn it belongs to. Codex writes token_count *after* the
// response items of the call it is reporting on, so by the time the cost is
// known the event it belongs to would already be gone.
//
// The alternative — emitting an event of its own for each token_count — is the
// thing internal/event's CaptureSchema comment forbids, and not as a matter of
// taste. Sequence numbers are assigned in emission order and hashed into every
// event id, so one extra event partway through a session renumbers everything
// after it. The re-walk that was supposed to add token counts to 790 stored
// Codex sessions would instead insert a second copy of most of each one.
// Holding an event back changes neither which events are emitted nor the order
// they are emitted in, so every id is exactly what it was.
func (s *codexFileState) emit(e event.Event, discriminator string, raw []byte) error {
	if err := s.flush(); err != nil {
		return err
	}
	s.pending = &pendingCodexEvent{event: e, discriminator: discriminator, raw: raw}
	return nil
}

// flush pushes the held-back event. Safe to call when there is none, which is
// what makes it usable both between records and once at end of file.
func (s *codexFileState) flush() error {
	p := s.pending
	if p == nil {
		return nil
	}
	s.pending = nil
	emitted, err := s.walker.push(&p.event, s.agentID, s.ordinal, p.discriminator, p.raw, s.done)
	s.ordinal++
	if emitted {
		s.events++
	}
	return err
}

// attachUsage bills the held-back event for the turn that just finished.
//
// First report wins. Codex repeats a token_count on some turn boundaries, and
// two arriving back to back with no event between them is that repeat rather
// than two genuinely separate calls — a real call always writes at least one
// response item, which releases the event the first cost attached to. Summing
// them would double the turn.
func (s *codexFileState) attachUsage(u *event.Usage) {
	if u == nil || s.pending == nil || s.pending.event.Usage != nil {
		return
	}
	s.pending.event.Usage = u
}

func (s *codexFileState) sessionCount() int {
	if s.events > 0 {
		return 1
	}
	return 0
}

func codexResponseEvent(base event.Event, r codexResponse) (event.Event, string, bool) {
	e := base
	switch r.Type {
	case "message":
		switch r.Role {
		case "user":
			e.Type = event.UserPrompt
		case "assistant":
			e.Type = event.AssistantTurn
		default:
			return e, "", false
		}
		e.Text = codexContentText(r.Content)
		return e, "message:" + r.ID + ":" + r.Role, true
	case "agent_message":
		e.Type, e.Text = event.AssistantTurn, codexContentText(r.Content)
		return e, "agent_message:" + r.ID, true
	case "function_call", "custom_tool_call", "tool_search_call":
		e.Type = event.ToolCall
		input := r.Arguments
		if len(input) == 0 {
			input = r.Input
		}
		e.Tool = &event.Tool{Name: r.Name, Input: normalizeCodexJSON(input)}
		return e, r.Type + ":" + firstCodexValue(r.CallID, r.ID), true
	case "function_call_output", "custom_tool_call_output", "tool_search_output":
		e.Type = event.ToolResult
		// Capped at the same byte the live path and the Claude walker cap at.
		out, cut := CapText(codexFlatten(r.Output), MaxToolOutputBytes)
		e.Tool = &event.Tool{Output: out, Truncated: cut}
		return e, r.Type + ":" + firstCodexValue(r.CallID, r.ID), true
	default:
		return e, "", false
	}
}

func (w *codexWalker) push(e *event.Event, agentID string, ordinal int64, discriminator string, raw []byte, done bool) (bool, error) {
	stream := e.SessionID + "\x00" + agentID
	seq := w.seq[stream]
	w.seq[stream] = seq + 1
	e.Seq = seq
	e.ID = event.DeterministicID(e.SessionID, seq, e.Type, strings.Join([]string{"codex", agentID, fmt.Sprint(ordinal), discriminator}, "|"))
	// Stamped here for the same reason the Claude walker stamps in its push:
	// an unstamped event reads as capture version zero, so the first schema
	// bump would re-upload the whole Codex corpus to overwrite byte-identical
	// bodies — and a genuinely improved Codex extraction could never replace
	// these rows, because zero is what they would already claim to be.
	e.CaptureVersion = event.CaptureSchema
	text, redactions := string(raw), map[string]int(nil)
	if w.opts.Scrub != nil && !done {
		text, redactions = w.opts.Scrub(text)
		e.Text, _ = w.opts.Scrub(e.Text)
		if e.Tool != nil {
			e.Tool.Output, _ = w.opts.Scrub(e.Tool.Output)
			e.Tool.Error, _ = w.opts.Scrub(e.Tool.Error)
			if len(e.Tool.Input) > 0 {
				input, _ := w.opts.Scrub(string(e.Tool.Input))
				e.Tool.Input = json.RawMessage(input)
			}
		}
	}
	e.Raw, e.Redactions = json.RawMessage(text), redactions
	if done {
		return false, nil
	}
	if err := w.emit(*e); err != nil {
		return false, err
	}
	return true, nil
}

func codexContentText(raw json.RawMessage) string {
	var blocks []codexContentBlock
	if json.Unmarshal(raw, &blocks) == nil {
		var out []string
		for _, b := range blocks {
			if (b.Type == "input_text" || b.Type == "output_text") && b.Text != "" {
				out = append(out, b.Text)
			}
		}
		return strings.Join(out, "\n")
	}
	var text string
	_ = json.Unmarshal(raw, &text)
	return text
}

func normalizeCodexJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var nested string
	if json.Unmarshal(raw, &nested) == nil && json.Valid([]byte(nested)) {
		return json.RawMessage(nested)
	}
	if json.Valid(raw) {
		return raw
	}
	b, _ := json.Marshal(string(raw))
	return b
}

func codexFlatten(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

func firstCodexValue(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (w *codexWalker) skip(path, reason string) {
	w.result.Skipped = append(w.result.Skipped, Skip{Path: path, Reason: reason})
}

func (w *codexWalker) report(path string, sessions, events int, bytes int64, err error, done bool) {
	if w.opts.OnSession != nil {
		w.opts.OnSession(Progress{File: path, Sessions: sessions, Events: events, Bytes: bytes, Err: err, Done: done})
	}
}

// FindCodexRollout returns the path of the rollout under root that holds the
// named session, or "" when the root holds none.
//
// It exists because the server cannot name a Codex file. A Claude transcript
// lives at a path the server can rebuild from the session's cwd and id
// (ClaudeTranscriptPath), so a repair entry carries it; a Codex rollout's
// name says nothing about which session it holds, and the repo has never
// relied on one: every part of the Codex walker decides from the
// session_meta line inside the file. This resolves the same way, so a
// rollout the walker would import is a rollout this finds, and the pair
// cannot drift into disagreeing about which file a session is in.
//
// Compressed rollouts are not opened, for the reason codexCollect gives:
// this walker cannot read them yet, and a gap has to be a declared one. A
// session whose only rollout is compressed reports as not found here, which
// is what the caller then says.
func FindCodexRollout(root, sessionID string) (string, error) {
	if strings.TrimSpace(sessionID) == "" {
		return "", errors.New("backfill: find Codex rollout: no session id")
	}
	files, _, err := codexCollect(root)
	if err != nil {
		return "", err
	}
	w := &codexWalker{}
	for _, path := range files {
		m, ok := readCodexMetadata(path)
		if !ok {
			continue
		}
		if w.codexBelongsTo(codexTarget{hasMeta: true, meta: m}, sessionID) {
			return path, nil
		}
	}
	return "", nil
}
