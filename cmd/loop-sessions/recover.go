package main

// Transcript recovery: the events the hooks lost, re-derived from the file
// the harness wrote.
//
// A hook can be abandoned (the harness tears down a `claude -p` run, a laptop
// closes, the spool refuses a write on a full disk) and the event it carried
// is gone from the live path. The transcript still has the record. The hooks
// leave two things behind that make the loss findable: an inflight marker
// for every hook that started, and a ledger line for every event the spool
// accepted, both keyed on the anchors the transcript also carries (promptId,
// tool_use ids, the assistant record's uuid). Subtracting the ledger from the
// transcript's anchors names exactly the records to walk, and the walker's
// Only filter walks exactly those, under the ids a full walk would have given
// them, so a second pass produces duplicates the server dedups.
//
// It runs in the daemon, never in a hook: at the owner's exit before the
// session is finalised, in the reconciler for sessions a crash left open, and
// at SessionStart for any session whose markers were never cleared.

import (
	"path/filepath"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/backfill"
	"github.com/loopai-hq/loop-sessions/internal/capture"
	"github.com/loopai-hq/loop-sessions/internal/config"
	"github.com/loopai-hq/loop-sessions/internal/daemon"
	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/scrub"
	"github.com/loopai-hq/loop-sessions/internal/spool"
)

// recoveredCounter is the durable counter recovery adds to, reported beside
// the drops so the fleet can see how often live capture needed the transcript.
const recoveredCounter = "recovered_from_transcript"

// inflightGrace is how old a marker must be before recovery treats its hook
// as dead. A marker is written before any work and removed after the spool
// write, so for the whole of a hook's life (its enrichment is bounded by
// hookTimeout, and the write it then waits for is a local rename) the marker
// looks exactly like an abandoned one. Nothing else tells them apart: no
// daemon exists before the first prompt, and another session's SessionStart
// runs this pass constantly on a machine where half the sessions are
// promptless script runs. Recovering a live hook's session re-emits turns the
// hooks are delivering right now and deletes the marker, so a hook that is
// then killed is never recovered. Twice the timeout leaves room for the
// write phase; the pid in the marker's name is deliberately not consulted,
// because a reused pid would hide a real loss behind an unrelated process
// for as long as that process lived.
const inflightGrace = 2 * hookTimeout

// reconcileRecover is the recovery the reconciler runs for each session a
// crash left open.
func reconcileRecover(p config.Paths, cfg config.Config) func(daemon.State) int {
	return func(st daemon.State) int {
		n, err := recoverSession(p, cfg, st.SessionID, st.TranscriptPath, st.Cwd, time.Now())
		if err != nil {
			logf("recover: session %s: %v", st.SessionID, err)
		}
		return n
	}
}

// recoverMarked runs recovery for every session with an unfinished hook whose
// daemon is not alive, which at SessionStart is every abandoned hook of a
// session that never got a daemon at all (a promptless `claude -p` run whose
// SessionEnd hook was killed at teardown) or whose daemon has since exited.
// A session with any marker younger than inflightGrace is left whole for a
// later pass, settled markers included: its hooks may still be running.
func recoverMarked(p config.Paths, cfg config.Config, ledger capture.Ledger, now time.Time) int {
	var total int
	cutoff := now.Add(-inflightGrace)
	for _, sid := range ledger.InflightSessions() {
		if daemon.AlreadyRunning(p.StateDir(), sid, nil, now, 0) {
			continue // its own daemon recovers it at exit
		}
		c, err := ledger.Read(sid)
		if err != nil {
			continue
		}
		var path, cwd string
		var settled, young int
		for _, m := range c.Inflight {
			if !m.At.Before(cutoff) {
				young++
				continue
			}
			settled++
			if m.TranscriptPath != "" {
				path, cwd = m.TranscriptPath, m.Cwd
			}
		}
		if young > 0 {
			// A hook of this session may still be running, and the record it
			// carries is already in the transcript, so walking the settled
			// markers now would spool a transcript copy beside the event that
			// hook is about to deliver. Every marker keeps its claim: the
			// settled ones wait for the next pass, which is at most one
			// SessionStart after the grace away, and if the young hook dies
			// its marker has settled by then and that pass recovers both.
			continue
		}
		if settled == 0 {
			// Nothing here parses (a young marker was caught above). A marker
			// that does not parse was torn by a hook killed mid-write; it
			// names no transcript and could never be walked, so once it is
			// old enough it goes the way of a path-less one; without this a
			// torn marker keeps the session in the inflight list forever.
			ledger.ClearInflightBefore(sid, cutoff)
			continue
		}
		if path == "" {
			// Nothing to walk; the markers cannot help and would be reported
			// forever.
			ledger.ClearInflightBefore(sid, cutoff)
			continue
		}
		n, err := recoverSession(p, cfg, sid, path, cwd, now)
		if err != nil {
			logf("recover: session %s: %v", sid, err)
			continue
		}
		total += n
	}
	return total
}

// recoverSession walks the records of one transcript that the hooks did not
// capture, spools them, records them in the ledger and clears the session's
// settled markers. It reports how many events it emitted.
//
// The main transcript is read whole. Subagent transcripts are read only when
// a settled marker names one, because a subagent's records live in its own
// file under <session>/subagents/ and the main file never mentions them;
// without that read a lost subagent PostToolUse is invisible and its marker
// is cleared regardless.
func recoverSession(p config.Paths, cfg config.Config, sessionID, transcript, cwd string, now time.Time) (int, error) {
	if transcript == "" || sessionID == "" {
		return 0, nil
	}
	ledger := capture.Ledger{Dir: p.StateDir()}
	cutoff := now.Add(-inflightGrace)
	if cwd != "" {
		if ok, why := cfg.ShouldCapture(cwd); !ok {
			logf("recover: session %s skipped: %s", sessionID, why)
			ledger.ClearInflightBefore(sessionID, cutoff)
			return 0, nil
		}
	}
	anchors, err := backfill.Anchors(transcript)
	if err != nil {
		// A transcript that is gone (30-day cleanup, a deleted worktree) is a
		// loss nothing can undo; the settled markers are cleared so the loss
		// stops being reported as pending work. A young marker keeps its
		// claim: its transcript may simply not exist yet.
		ledger.ClearInflightBefore(sessionID, cutoff)
		return 0, err
	}
	have, err := ledger.Read(sessionID)
	if err != nil {
		return 0, err
	}
	accepted := missingRecords(anchors, have)
	for _, path := range agentTranscripts(have.Inflight, cutoff) {
		agent, err := backfill.Anchors(path)
		if err != nil {
			continue // gone with its parent, or never written; nothing to recover
		}
		for uuid := range missingToolRecords(agent, have) {
			accepted[uuid] = true
		}
	}
	if len(accepted) == 0 {
		ledger.ClearInflightBefore(sessionID, cutoff)
		return 0, nil
	}

	sp, err := spool.Open(spool.Options{Dir: p.SpoolDir(), MaxBytes: cfg.SpoolMaxBytes, MinFreeRatio: cfg.MinFreeRatio})
	if err != nil {
		return 0, err
	}
	sink, err := pipelineSink(sp)
	if err != nil {
		return 0, err
	}
	var emitted []event.Event
	_, err = backfill.Walk(backfill.Options{
		Root:    filepath.Dir(transcript),
		Session: sessionID,
		Scrub:   scrub.Func,
		Only:    func(a backfill.Anchor) bool { return accepted[a.UUID] },
	}, func(e event.Event) error {
		if err := sink.Put(e); err != nil {
			return err
		}
		emitted = append(emitted, e)
		return nil
	})
	if len(emitted) > 0 {
		sp.Record(recoveredCounter, len(emitted))
		// Recorded in the ledger so the next pass finds nothing missing: the
		// recovered rows carry the same anchors a hook copy would have.
		if lerr := ledger.Captured(sessionID, capture.EntriesOf(emitted)); lerr != nil {
			logf("recover: could not record what was recovered for %s: %v", sessionID, lerr)
		}
		logf("recover: session %s: %d event(s) re-derived from the transcript", sessionID, len(emitted))
	}
	if err != nil {
		return len(emitted), err
	}
	ledger.ClearInflightBefore(sessionID, cutoff)
	return len(emitted), nil
}

// agentTranscripts lists the subagent files the session's settled markers
// name, each once.
func agentTranscripts(markers []capture.Marker, cutoff time.Time) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range markers {
		if m.AgentTranscriptPath == "" || !m.At.Before(cutoff) || seen[m.AgentTranscriptPath] {
			continue
		}
		seen[m.AgentTranscriptPath] = true
		out = append(out, m.AgentTranscriptPath)
	}
	return out
}

// missingRecords decides which transcript records the hooks would have
// captured and did not. It is deliberately narrow: the hooks never see an
// assistant's intermediate text before a tool call, so counting those as lost
// would report every session as needing recovery. What counts is the human
// prompt (by promptId), each tool use and result (by tool_use id), and the
// final answer of a turn whose Stop the ledger never recorded.
func missingRecords(anchors []backfill.Anchor, have capture.Captured) map[string]bool {
	accepted := missingToolRecords(anchors, have)
	// The prompt each assistant record answers: assistant records carry no
	// promptId of their own and inherit the nearest human prompt above them.
	var turn string
	// The last assistant record of each turn, which is the one a Stop hook
	// captures; anything before it is work the hooks never captured anyway.
	lastAssistant := map[string]backfill.Anchor{}
	for _, a := range anchors {
		switch {
		case a.Type == "user" && !a.IsToolResult && !a.IsMeta && a.PromptID != "":
			turn = a.PromptID
			if !have.PromptIDs[a.PromptID] {
				accepted[a.UUID] = true
			}
		case a.Type == "assistant" && turn != "" && len(a.ToolUseIDs) == 0:
			lastAssistant[turn] = a
		}
	}
	for prompt, a := range lastAssistant {
		if have.RecordUUIDs[a.UUID] || have.AssistantPrompts[prompt] {
			continue
		}
		accepted[a.UUID] = true
	}
	return accepted
}

// missingToolRecords is the tool half of the diff, on its own because it is
// the whole diff for a subagent transcript: the hooks a subagent fires are
// PreToolUse and PostToolUse (SubagentStop anchors no record), so its prompts
// and final answer are not the hooks' to have lost. A tool_use block is
// missing when no PreToolUse landed for its id, a tool_result record when no
// PostToolUse did; each hook vouches only for itself, since PreToolUse is the
// fast one and PostToolUse the one the harness kills at teardown.
func missingToolRecords(anchors []backfill.Anchor, have capture.Captured) map[string]bool {
	accepted := map[string]bool{}
	for _, a := range anchors {
		var landed map[string]bool
		switch {
		case a.Type == "user" && a.IsToolResult:
			landed = have.ToolResultIDs
		case a.Type == "assistant":
			landed = have.ToolCallIDs
		default:
			continue
		}
		for _, id := range a.ToolUseIDs {
			if id != "" && !landed[id] {
				accepted[a.UUID] = true
			}
		}
	}
	return accepted
}
