package capture

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// Ledger is the per-session record of what the hooks started and what they
// finished, kept in the state directory beside the daemon's session files.
//
// It exists because a hook can be abandoned. The harness kills async hooks at
// teardown of a `claude -p` run, a laptop closes, the old two-second self-
// timeout fired 266 times on one machine, and in every case the event was
// simply gone: nothing knew it had been started. Two files change that.
//
// <session>.inflight/ holds one marker per hook invocation, written before any
// scrubbing, diffing or transcript reading and removed after the spool write.
// A marker that is still there after the hook is dead is an event that was
// not captured, with enough on it (the hook, the prompt, the tool use) for the
// recovery pass to go and find it in the transcript.
//
// <session>.captured is append-only, one line per event the spool accepted,
// carrying the anchors the transcript also carries. Subtracting it from the
// transcript's anchors is how the recovery pass knows which records to walk
// and which it already has, without re-sending a session the server holds.
type Ledger struct{ Dir string }

// Marker is one hook invocation in flight.
type Marker struct {
	SessionID string `json:"session_id"`
	HookEvent string `json:"hook_event"`
	PromptID  string `json:"prompt_id,omitempty"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	// TranscriptPath and Cwd are what the recovery pass needs to find the
	// event when no daemon state exists for the session, which is the case
	// for every session that never got a prompt.
	TranscriptPath string `json:"transcript_path,omitempty"`
	Cwd            string `json:"cwd,omitempty"`
	// AgentTranscriptPath is the subagent's own file when the hook fired for
	// subagent work. A subagent's tool records live there, not in the main
	// transcript the marker also names, so a recovery pass that read only the
	// main file would find nothing missing for a lost subagent PostToolUse and
	// clear the marker with the event still gone.
	AgentTranscriptPath string    `json:"agent_transcript_path,omitempty"`
	At                  time.Time `json:"at"`
}

// Entry is one event the spool accepted.
type Entry struct {
	Type       event.Type `json:"type"`
	PromptID   string     `json:"prompt_id,omitempty"`
	ToolUseID  string     `json:"tool_use_id,omitempty"`
	RecordUUID string     `json:"record_uuid,omitempty"`
	Seq        int64      `json:"seq,omitempty"`
	At         time.Time  `json:"at"`
}

// Captured is a session's ledger, read back as sets.
type Captured struct {
	PromptIDs map[string]bool
	// ToolCallIDs are the tool uses whose PreToolUse landed; ToolResultIDs
	// the ones whose PostToolUse (or PostToolUseFailure) landed. They are two
	// sets because the two hooks fail differently: PreToolUse is the fast one
	// with nothing to scrub, PostToolUse carries up to 512 KiB of output and
	// is the one the harness kills at teardown. One set would let a landed
	// call vouch for its lost result, which is the likeliest loss there is.
	// A file_changed entry counts for neither: it rides the same PostToolUse
	// and proves nothing its tool_result line does not.
	ToolCallIDs   map[string]bool
	ToolResultIDs map[string]bool
	RecordUUIDs   map[string]bool
	// AssistantPrompts are the prompts for which an assistant_turn was
	// captured: a Stop hook that landed, whether or not it could name the
	// transcript record it answered.
	AssistantPrompts map[string]bool
	Types            map[event.Type]int
	// Inflight lists markers whose hook never finished.
	Inflight []Marker
	// Written is when the ledger was last appended to.
	Written time.Time
}

// Inflight writes a marker and returns the function that removes it. A marker
// is a file of its own rather than a line in a shared file because PostToolUse
// hooks fire in bursts and two of them rewriting one file to remove their own
// line would lose each other's. Failing to write the marker is reported but
// must not stop the capture: the marker is insurance for the event, not a
// precondition of it.
func (l Ledger) Inflight(m Marker) (func(), error) {
	dir := l.inflightDir(m.SessionID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return func() {}, fmt.Errorf("ledger: inflight dir: %w", err)
	}
	if m.At.IsZero() {
		m.At = time.Now()
	}
	b, err := json.Marshal(m)
	if err != nil {
		return func() {}, err
	}
	name := filepath.Join(dir, strconv.FormatInt(m.At.UnixNano(), 10)+"-"+strconv.Itoa(os.Getpid())+".json")
	if err := os.WriteFile(name, b, 0o600); err != nil {
		return func() {}, fmt.Errorf("ledger: write marker: %w", err)
	}
	return func() { _ = os.Remove(name) }, nil
}

// Captured appends the events a hook wrote. One write call for all of a hook's
// lines: each hook is its own process and O_APPEND makes a single write land
// whole, so bursts of hooks interleave by hook rather than by byte.
func (l Ledger) Captured(sessionID string, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := os.MkdirAll(l.Dir, 0o700); err != nil {
		return fmt.Errorf("ledger: dir: %w", err)
	}
	var buf strings.Builder
	for _, e := range entries {
		if e.At.IsZero() {
			e.At = time.Now()
		}
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	f, err := os.OpenFile(l.capturedPath(sessionID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("ledger: open: %w", err)
	}
	defer f.Close()
	_, err = f.WriteString(buf.String())
	return err
}

// Read loads a session's ledger. A session with no ledger is an empty
// Captured and no error.
func (l Ledger) Read(sessionID string) (Captured, error) {
	c := Captured{
		PromptIDs:        map[string]bool{},
		ToolCallIDs:      map[string]bool{},
		ToolResultIDs:    map[string]bool{},
		RecordUUIDs:      map[string]bool{},
		AssistantPrompts: map[string]bool{},
		Types:            map[event.Type]int{},
	}
	b, err := os.ReadFile(l.capturedPath(sessionID))
	if err != nil && !os.IsNotExist(err) {
		return c, fmt.Errorf("ledger: read: %w", err)
	}
	if fi, err := os.Stat(l.capturedPath(sessionID)); err == nil {
		c.Written = fi.ModTime()
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var e Entry
		if json.Unmarshal([]byte(line), &e) != nil {
			continue // a torn last line from a killed hook; the event it names is recovered from the transcript
		}
		c.Types[e.Type]++
		if e.PromptID != "" {
			switch e.Type {
			case event.UserPrompt:
				c.PromptIDs[e.PromptID] = true
			case event.AssistantTurn:
				c.AssistantPrompts[e.PromptID] = true
			}
		}
		if e.ToolUseID != "" {
			switch e.Type {
			case event.ToolCall:
				c.ToolCallIDs[e.ToolUseID] = true
			case event.ToolResult, event.ToolFailed:
				c.ToolResultIDs[e.ToolUseID] = true
			}
		}
		if e.RecordUUID != "" {
			c.RecordUUIDs[e.RecordUUID] = true
		}
	}
	if ents, err := os.ReadDir(l.inflightDir(sessionID)); err == nil {
		for _, ent := range ents {
			mb, err := os.ReadFile(filepath.Join(l.inflightDir(sessionID), ent.Name()))
			if err != nil {
				continue
			}
			var m Marker
			if json.Unmarshal(mb, &m) == nil {
				c.Inflight = append(c.Inflight, m)
			}
		}
	}
	return c, nil
}

// InflightSessions lists the sessions with at least one unfinished hook.
func (l Ledger) InflightSessions() []string {
	ents, err := os.ReadDir(l.Dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range ents {
		if !e.IsDir() || !strings.HasSuffix(e.Name(), ".inflight") {
			continue
		}
		if markers, err := os.ReadDir(filepath.Join(l.Dir, e.Name())); err == nil && len(markers) > 0 {
			out = append(out, strings.TrimSuffix(e.Name(), ".inflight"))
		}
	}
	return out
}

// HasInflight reports whether any hook for the session is unfinished.
func (l Ledger) HasInflight(sessionID string) bool {
	ents, err := os.ReadDir(l.inflightDir(sessionID))
	return err == nil && len(ents) > 0
}

// ClearInflight removes every marker for a session once its transcript has
// been walked; the events they named are either recovered or unrecoverable.
func (l Ledger) ClearInflight(sessionID string) {
	_ = os.RemoveAll(l.inflightDir(sessionID))
}

// ClearInflightBefore removes the session's markers written before cutoff and
// leaves the younger ones for a later pass. A marker is written before the
// hook does any work and removed after its spool write, so a young marker is
// as likely to be a hook in the middle of scrubbing as one that died, and
// deleting it would turn an abandoned hook into a loss nothing reports. The
// age is read off the marker's filename (<unix nanos>-<pid>.json) rather than
// its body so a torn marker is still datable.
func (l Ledger) ClearInflightBefore(sessionID string, cutoff time.Time) {
	dir := l.inflightDir(sessionID)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var kept int
	for _, e := range ents {
		if markerTime(e.Name()).Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
			continue
		}
		kept++
	}
	if kept == 0 {
		_ = os.Remove(dir)
	}
}

// markerTime reads the write time out of a marker's name. A name that does
// not parse reads as the zero time, which sorts before any cutoff: a file that
// is not one of ours in shape is not one a later pass could use either.
func markerTime(name string) time.Time {
	nanos, _, _ := strings.Cut(name, "-")
	n, err := strconv.ParseInt(nanos, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// EmptyStarts counts sessions whose ledger was last written inside the window
// and records an end but never a prompt: the signature of `claude` started
// by a script with nothing on stdin. It is the count the fleet alerts on per
// machine, and it is derived here because the hook ledger is the only record
// a promptless session leaves; no daemon is ever started for one.
func (l Ledger) EmptyStarts(now time.Time, window time.Duration) int {
	ents, err := os.ReadDir(l.Dir)
	if err != nil {
		return 0
	}
	var n int
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".captured") {
			continue
		}
		fi, err := e.Info()
		if err != nil || now.Sub(fi.ModTime()) > window {
			continue
		}
		c, err := l.Read(strings.TrimSuffix(e.Name(), ".captured"))
		if err != nil {
			continue
		}
		if c.Types[event.SessionEnded] > 0 && c.Types[event.UserPrompt] == 0 {
			n++
		}
	}
	return n
}

// Sweep removes ledgers not written for longer than keep, and the marker
// directories of sessions that no longer have a ledger. Markers are kept
// alongside their ledger for as long as it lives, because a marker is only
// useful while the transcript it points at still exists, and Claude Code keeps
// transcripts for about the same period.
func (l Ledger) Sweep(keep time.Duration, now time.Time) int {
	ents, err := os.ReadDir(l.Dir)
	if err != nil {
		return 0
	}
	var n int
	for _, e := range ents {
		name := e.Name()
		switch {
		case !e.IsDir() && strings.HasSuffix(name, ".captured"):
			fi, err := e.Info()
			if err != nil || now.Sub(fi.ModTime()) <= keep {
				continue
			}
			if os.Remove(filepath.Join(l.Dir, name)) == nil {
				n++
				_ = os.RemoveAll(l.inflightDir(strings.TrimSuffix(name, ".captured")))
			}
		case e.IsDir() && strings.HasSuffix(name, ".inflight"):
			fi, err := e.Info()
			if err != nil || now.Sub(fi.ModTime()) <= keep {
				continue
			}
			if _, err := os.Stat(filepath.Join(l.Dir, strings.TrimSuffix(name, ".inflight")+".captured")); os.IsNotExist(err) {
				if os.RemoveAll(filepath.Join(l.Dir, name)) == nil {
					n++
				}
			}
		}
	}
	return n
}

func (l Ledger) capturedPath(sessionID string) string {
	return filepath.Join(l.Dir, sanitize(sessionID)+".captured")
}

func (l Ledger) inflightDir(sessionID string) string {
	return filepath.Join(l.Dir, sanitize(sessionID)+".inflight")
}

// EntriesOf turns emitted events into ledger lines.
func EntriesOf(evs []event.Event) []Entry {
	out := make([]Entry, 0, len(evs))
	for _, e := range evs {
		out = append(out, Entry{
			Type: e.Type, PromptID: e.PromptID, ToolUseID: e.ToolUseID, RecordUUID: e.RecordUUID,
			Seq: e.Seq, At: e.OccurredAt,
		})
	}
	return out
}
