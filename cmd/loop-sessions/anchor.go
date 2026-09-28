package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/backfill"
	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/daemon"
	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// stopAnchorer anchors, on the way out of the spool, a hook Stop copy that was
// spooled without its record. The harness fires Stop and flushes the turn's
// tail about 100 ms later (measured 2026-09-16), so the hook's own read sees
// the file mid-flush; a Stop hook that waited for the flush was killed by the
// print-mode harness's exit before it could spool anything, and the copy was
// lost. The daemon survives the harness, so the wait lives here: bounded by
// capture.StopTailWaits once per batch, and only for copies younger than
// recent, so a backlog of old anchorless copies never delays a drain (each
// of those gets one read: at most four windows of the transcript's tail, a
// state file and a stat).
//
// Reading after the fact has one hazard the hook's read at Stop time did
// not: the answer in view may be a LATER one under the same prompt, since
// one prompt can end in Stop more than once (a lead woken by teammate
// messages, a blocking Stop hook). The record's own timestamp settles it:
// the harness stamps a record when its message completes, before Stop
// fires for it, so a record newer than the copy (past a small grace for
// the two clocks being read at different instants on one host) is not the
// copy's answer, and waiting cannot change that.
type stopAnchorer struct {
	stateDir string
	now      func() time.Time
	sleep    func(context.Context, time.Duration)
	waits    []time.Duration
	recent   time.Duration
}

// anchorGrace is how much newer than the Stop copy a record may be and still
// be its answer. The harness stamps each content-block record as that block
// completes, so the answer record (the message's last block) precedes the
// copy's time: 10 ms at least, 52 ms median, 695 ms at most over 331 hook
// Stops on one machine. What the grace must stay under is the NEXT message's
// FIRST record under the same prompt (a thinking or text block), which came
// 1.98 s after a Stop at the closest over 31 cases; two seconds would have
// let one through. A record with no parseable stamp is refused: anchorless
// is the design's preference over wrong.
const anchorGrace = time.Second

func newStopAnchorer(stateDir string) *stopAnchorer {
	return &stopAnchorer{stateDir: stateDir, now: time.Now, sleep: sleepUnlessDone, waits: capture.StopTailWaits, recent: 10 * time.Second}
}

// sleepUnlessDone is the production wait: a drain that is being shut down
// (the daemon's exit grace) does not sit out the schedule first.
func sleepUnlessDone(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// enrich is drain.Options.Enrich: for each hook Stop copy of a Claude Code
// turn without a record, find the session's transcript from its state file
// and try the anchor rule, waiting the schedule while the copy is recent.
func (a *stopAnchorer) enrich(ctx context.Context, batch []spool.Leased) {
	// One schedule per batch, not per item: a batch of copies that will
	// never anchor must not hold the drain (and the machine-wide delivery
	// lock) for longer than one copy would.
	budget := 0
	for i := range batch {
		it := &batch[i].Item
		if it.Kind != "event" {
			continue
		}
		var probe struct {
			Type       event.Type   `json:"type"`
			Origin     event.Origin `json:"origin"`
			Source     event.Source `json:"source"`
			SessionID  string       `json:"session_id"`
			PromptID   string       `json:"prompt_id"`
			RecordUUID string       `json:"record_uuid"`
			Text       string       `json:"text"`
		}
		if json.Unmarshal(it.Payload, &probe) != nil || probe.Type != event.AssistantTurn ||
			probe.Origin != event.OriginHook || probe.Source != event.SourceClaudeCode || probe.RecordUUID != "" {
			continue
		}
		recent := a.now().Sub(it.EventTime) < a.recent
		// A copy from a harness that predates prompt_id takes the tail on
		// trust, which is only safe while the tail is still this turn's: an
		// old promptless copy (an offline backlog) would be anchored to
		// whatever the session did last.
		if probe.PromptID == "" && !recent {
			continue
		}
		st, err := daemon.LoadState(a.stateDir, probe.SessionID)
		if err != nil || st.TranscriptPath == "" {
			continue
		}
		latest := it.EventTime.Add(anchorGrace)
		sc := &capture.StopCopy{Text: probe.Text, At: it.EventTime}
		t, ok := capture.AnchorFor(st.TranscriptPath, probe.PromptID, sc)
		if !ok {
			// Waiting only helps while the file exists and can still change;
			// a session run without a transcript (print mode with persistence
			// off) never gains one, and a deleted file never comes back.
			if _, statErr := os.Stat(st.TranscriptPath); statErr != nil {
				continue
			}
		}
		for !ok && recent && budget < len(a.waits) {
			if ctx.Err() != nil {
				return
			}
			a.sleep(ctx, a.waits[budget])
			budget++
			t, ok = capture.AnchorFor(st.TranscriptPath, probe.PromptID, sc)
		}
		if !ok || t.Timestamp.IsZero() || t.Timestamp.After(latest) {
			continue
		}
		patched, err := anchorPayload(it.Payload, t)
		if err != nil {
			continue
		}
		it.Payload = patched
		logf("anchor: session %s: Stop copy anchored to record %s before upload", probe.SessionID, t.UUID)
	}
}

// anchorPayload sets the record on a spooled payload without disturbing the
// rest of it: every other field, including ones this build does not know (an
// item an older client spooled) and the raw record, is carried as the bytes
// it arrived in; only the three anchor keys are written.
func anchorPayload(payload []byte, t backfill.Tail) ([]byte, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	set := func(key string, v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		m[key] = b
		return nil
	}
	if err := set("record_uuid", t.UUID); err != nil {
		return nil, err
	}
	if t.Model != "" {
		if err := set("model", t.Model); err != nil {
			return nil, err
		}
	}
	if t.Usage != nil {
		if err := set("usage", t.Usage); err != nil {
			return nil, err
		}
	}
	return json.Marshal(m)
}
