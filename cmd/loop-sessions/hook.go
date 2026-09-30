package main

// The hook path: one Claude Code lifecycle event in, spool files out.
//
// Every installed hook is registered async, and the harness does not time
// async hooks out at all ("Once an async hook is running in the background,
// Claude Code doesn't enforce timeout on it"). The only things that can kill
// one are the teardown of a `claude -p` run and the SessionEnd budget. So the
// two-second self-timeout this file used to carry protected nobody's turn and
// abandoned 266 events on one machine. It is sixty seconds now, and it no
// longer abandons a write in progress.
//
// The order of work is the durability argument. Read and parse the payload,
// then write an inflight marker to the state directory BEFORE scrubbing,
// diffing or reading the transcript, then translate and spool, then record
// what was spooled and remove the marker. A hook that dies anywhere after the
// marker leaves a marker behind, and the marker names the session, the prompt
// and the tool use, which is what the recovery pass needs to go and find the
// event in the transcript. A hook that dies before the marker had not yet
// read a payload worth losing.

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/daemon"
	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/pipeline"
	"github.com/loopai-hq/agent-sessions/internal/scrub"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

const (
	// hookTimeout bounds the enrichment phases of one hook: scrubbing,
	// diffing, the transcript tail read. Long, because nothing waits on it
	// (see the file comment) and because the alternative is a lost event.
	hookTimeout = 60 * time.Second
	// hookStdinLimit bounds the payload read. A tool result carrying a whole
	// file is tens of megabytes at most; anything larger is not a hook payload.
	hookStdinLimit = 64 << 20
	// stateKeep is how long finished session state and hook ledgers are kept.
	// Long enough that a laptop closed for a week reconciles on return; short
	// enough that Reconcile does not parse a directory of thousands of files
	// on every session start, which it did.
	stateKeep = 7 * 24 * time.Hour
)

// Seams the tests replace: the scrubber, so a test can hold a hook inside the
// scrub phase and look at the marker; the sink, so a test can make the spool
// write slow and prove the process waits for it.
var (
	hookScrub capture.Scrubber = scrub.Func
	hookSink                   = func(sp *spool.Spool) (capture.Sink, error) { return pipeline.NewSink(sp) }
	// onHookDone runs when the capture goroutine finishes, whether or not
	// runHook is still waiting for it. A test that abandons a hook needs to
	// know when the abandoned goroutine has actually stopped touching the
	// filesystem; in production the process exits and nothing observes it.
	onHookDone = func() {}
)

// Phases a hook passes through. The abandon log line names the phase so the
// slow one can be found; the old line did not, and 266 abandons on one machine
// could not be attributed to anything.
const (
	phaseMarker    = "marker"
	phaseTranslate = "translate"
	phaseWrite     = "write"
	phaseLedger    = "ledger"
	phasePost      = "post"
)

type hookPhase struct{ v atomic.Value }

func (p *hookPhase) set(s string) { p.v.Store(s) }
func (p *hookPhase) get() string {
	s, _ := p.v.Load().(string)
	return s
}

// runHook is the hot path. It reads a hook payload on stdin, converts it into
// events, and appends them to the spool.
//
// It returns 0 unconditionally. Claude Code treats a non-zero hook exit as
// meaningful, and there is no failure here worth interrupting somebody's work
// for: if capture breaks, the right outcome is a gap in telemetry, never a
// degraded editor. Failures are recorded to the agent log and surface later
// through `doctor` and the heartbeat.
func runHook(args []string) int {
	fs := flag.NewFlagSet("hook", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	timeout := fs.Duration("timeout", hookTimeout, "limit on the enrichment phases of hook work")
	if err := fs.Parse(args); err != nil {
		return 0
	}

	run, err := prepareHook(os.Stdin)
	if err != nil {
		logf("hook: %v", err)
		return 0
	}
	if run == nil {
		return 0
	}

	var phase hookPhase
	phase.set(phaseMarker)
	done := make(chan error, 1)
	go func() { done <- run.capture(&phase) }()

	select {
	case err := <-done:
		if err != nil {
			logf("hook: %v", err)
		}
	case <-time.After(*timeout):
		switch ph := phase.get(); ph {
		case phaseWrite, phaseLedger:
			// The spool write is a local file and a rename; exiting now would
			// leave a temp file and lose the event to save nobody any time.
			logf("hook: %s for session %s is past %s in phase %s; waiting for the spool write to finish",
				run.h.HookEventName, run.h.SessionID, *timeout, ph)
			if err := <-done; err != nil {
				logf("hook: %v", err)
			}
		case phasePost:
			// The event is spooled; what is slow is reconciliation or the
			// daemon spawn, neither of which loses anything if cut short.
			logf("hook: %s post-capture work exceeded %s; the event is already spooled", run.h.HookEventName, *timeout)
		default:
			logf("hook: exceeded %s, abandoning event=%s session=%s phase=%s bytes=%d",
				*timeout, run.h.HookEventName, run.h.SessionID, ph, run.bytes)
			run.sp.Record("hook_abandoned", 1)
		}
	}
	return 0
}

// hookRun is one hook invocation past the point where it is worth doing.
type hookRun struct {
	h     capture.HookEvent
	bytes int
	p     config.Paths
	cfg   config.Config
	sp    *spool.Spool
	// backlog is what the spool held BEFORE this hook wrote anything: the
	// leftovers of a previous daemon, which is the only pending count that
	// argues for starting a daemon at SessionStart. Measured only for
	// SessionStart, because it is a directory listing and every other hook
	// is on the hot path.
	backlog int
}

// prepareHook does the cheap, synchronous part: read, parse, config, spool.
// A nil run with a nil error means there is nothing to capture (not enrolled,
// an excluded path, an empty payload), which is ordinary and silent.
func prepareHook(stdin io.Reader) (*hookRun, error) {
	raw, err := io.ReadAll(io.LimitReader(stdin, hookStdinLimit))
	if err != nil {
		return nil, fmt.Errorf("read stdin: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var h capture.HookEvent
	if err := json.Unmarshal(raw, &h); err != nil {
		return nil, fmt.Errorf("parse hook payload: %w", err)
	}

	p := config.Paths{}
	cfg, err := config.Load(p)
	if err != nil {
		// Not configured yet is normal on a machine mid-install; say nothing.
		if errors.Is(err, config.ErrNotConfigured) {
			return nil, nil
		}
		return nil, err
	}
	// The end marker is written here, before the capture decision and
	// before the spool is opened, because it is not part of capture: it says
	// the harness reported this session over, and the only thing that reads
	// it is the session's own daemon, which ends within a poll of seeing it.
	// It used to be written inside capture(), past both, so a session whose
	// cwd stopped being captured or whose capture was paused between its
	// first prompt and its end left no marker at all. Its daemon then waited
	// on the owner pid, and on a machine where that pid is an IDE or the
	// desktop app hosting many sessions it waited for weeks, posting a
	// health report every five minutes and draining the shared spool: the
	// zombie shape PRs #66 and #67 removed, leaking back through the one
	// path that skips capture. Writing it costs one small file and ends a
	// daemon that should end; a resume clears it before its spawn.
	if h.HookEventName == "SessionEnd" && h.SessionID != "" {
		if err := daemon.MarkEnded(p.StateDir(), h.SessionID, time.Now()); err != nil {
			logf("hook: could not mark session %s ended; its daemon waits for the harness pid: %v", h.SessionID, err)
		}
	}
	if ok, why := cfg.ShouldCapture(h.Cwd); !ok {
		logf("hook: skipping session in %s: %s", h.Cwd, why)
		return nil, nil
	}

	sp, err := spool.Open(spool.Options{
		Dir:          p.SpoolDir(),
		MaxBytes:     cfg.SpoolMaxBytes,
		MinFreeRatio: cfg.MinFreeRatio,
	})
	if err != nil {
		return nil, fmt.Errorf("open spool: %w", err)
	}
	run := &hookRun{h: h, bytes: len(raw), p: p, cfg: cfg, sp: sp}
	if h.HookEventName == "SessionStart" {
		if st, err := sp.Stats(); err == nil {
			run.backlog = st.Pending + st.Parked
		}
	}
	return run, nil
}

// capture is the ledger-first pipeline: marker, translate and scrub, write,
// record, then whatever the event obliges the machine to do next.
func (r *hookRun) capture(phase *hookPhase) error {
	defer onHookDone()
	// The SessionEnd marker is already on disk: newHookRun writes it before
	// the capture decision, so a session this run is about to skip has ended
	// its daemon all the same. The session_ended event spooled below ships
	// with any daemon.
	ledger := capture.Ledger{Dir: r.p.StateDir()}
	unmark, err := ledger.Inflight(capture.Marker{
		SessionID: r.h.SessionID, HookEvent: r.h.HookEventName,
		PromptID: r.h.PromptID, ToolUseID: r.h.ToolUseID,
		TranscriptPath: r.h.TranscriptPath, AgentTranscriptPath: r.h.AgentTranscriptPath,
		Cwd: r.h.Cwd, At: time.Now(),
	})
	if err != nil {
		// The marker is insurance for the event, not a precondition of it.
		logf("hook: could not write the inflight marker, so an abandoned %s would not be recovered: %v", r.h.HookEventName, err)
	}

	inner, err := hookSink(r.sp)
	if err != nil {
		return err
	}
	sink := &recordingSink{inner: inner, phase: phase}
	seq := capture.FileSeq{Dir: r.p.SeqDir()}
	c, err := capture.New(sink, hookScrub, seq.Next)
	if err != nil {
		return err
	}
	c.SetDefaultMirror(r.defaultMirror)

	phase.set(phaseTranslate)
	n, err := c.Handle(r.h)
	if err != nil {
		// The marker stays: whatever was not written is the recovery pass's
		// job, and the marker is how it knows to look.
		return err
	}

	phase.set(phaseLedger)
	if err := ledger.Captured(r.h.SessionID, capture.EntriesOf(sink.events)); err != nil {
		logf("hook: could not record what was captured: %v", err)
	}
	unmark()

	phase.set(phasePost)
	r.afterCapture(ledger)
	logf("hook: %s -> %d event(s)", r.h.HookEventName, n)
	return nil
}

// afterCapture is what the machine owes a session beyond its own event.
//
// SessionStart is where recovery happens, because it is the one moment we know
// a harness is alive and can pay for the work without a timer. It is no longer
// where the daemon necessarily starts. Nearly half of all sessions on the
// fleet begin and end without a prompt (a script running `claude` with nothing
// on stdin), and each one used to cost a daemon: a fork, a flock, a state file
// that was never swept, a manifest fetch and a health report. The daemon now
// starts at the first prompt, which every real session has, unless there is
// work that cannot wait: a backlog a previous daemon left behind, or sessions
// a crash left open.
func (r *hookRun) afterCapture(ledger capture.Ledger) {
	switch r.h.HookEventName {
	case "SessionStart":
		now := time.Now()
		abandoned := reconcileAtStart(r.p, r.cfg, now)
		// Sessions whose hooks were abandoned and that no daemon will ever
		// close: recovered here, the one moment a harness is known to be
		// alive and can pay for the work.
		recovered := recoverMarked(r.p, r.cfg, ledger, now)
		if n, err := daemon.Sweep(r.p.StateDir(), stateKeep, now); err == nil && n > 0 {
			logf("hook: swept %d finished session state file(s) older than %s", n, stateKeep)
		}
		ledger.Sweep(stateKeep, now)

		if r.backlog == 0 && abandoned == 0 && recovered == 0 {
			// This session's own session_started waits in the spool for the
			// daemon the first prompt starts; a session that never gets one
			// ships it with the next session that does.
			return
		}
		daemon.ClearEnded(r.p.StateDir(), r.h.SessionID)
		if err := spawnDaemon(r.p, r.h); err != nil {
			logf("hook: could not start the delivery daemon, nothing will upload: %v", err)
		}
	case "UserPromptSubmit":
		// The first prompt proves this is a session with something in it.
		// launchDaemon is idempotent per session, so every later prompt is a
		// state-file read and nothing else.
		daemon.ClearEnded(r.p.StateDir(), r.h.SessionID)
		if err := spawnDaemon(r.p, r.h); err != nil {
			logf("hook: could not start the delivery daemon, nothing will upload: %v", err)
		}
	}
}

// reconcileAtStart closes sessions a dead daemon left open and reports how
// many there were. The recovery of their unspooled events is wired in
// through reconcileRecover so this file does not depend on the walker.
func reconcileAtStart(p config.Paths, cfg config.Config, now time.Time) int {
	ab, err := daemon.ReconcileWith(p.StateDir(), nil, now, 0, reconcileRecover(p, cfg))
	// After the reconcile: sessions whose daemon and owner both died are
	// recovered and finalized there first, and only what it leaves (live
	// stale daemons, and dead ones under a live owner) is the reaper's.
	reapStale(p, "", now)
	if err != nil {
		logf("hook: reconcile: %v", err)
		return 0
	}
	if len(ab) > 0 {
		logf("hook: reconciled %d abandoned session(s)", len(ab))
	}
	return len(ab)
}

// defaultMirror is the machine's standing mirror answer, consulted only when
// the env says nothing. A one-shot consumes itself here, at the first
// SessionStart that uses it: the property that makes it forgettable-safe.
func (r *hookRun) defaultMirror() string {
	v := r.cfg.MirrorDefault
	if v != "" && r.cfg.MirrorOnce {
		fresh, err := config.Load(r.p)
		if err == nil && fresh.MirrorOnce && fresh.MirrorDefault == v {
			fresh.MirrorDefault, fresh.MirrorOnce = "", false
			if err := config.Save(r.p, fresh); err != nil {
				logf("hook: could not consume the one-shot mirror default: %v", err)
			}
		}
	}
	return v
}

// recordingSink remembers what the spool accepted, so the ledger line can be
// written after the write rather than before it, and marks the moment the
// first write begins, which is the moment abandoning stops being safe.
type recordingSink struct {
	inner  capture.Sink
	phase  *hookPhase
	events []event.Event
}

func (s *recordingSink) Put(e event.Event) error {
	s.phase.set(phaseWrite)
	if err := s.inner.Put(e); err != nil {
		return err
	}
	s.events = append(s.events, e)
	return nil
}
