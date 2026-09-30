// Package daemon owns a session's lifetime on the client.
//
// It exists because of one stubborn fact: SessionEnd does not fire when the
// harness is killed. It fires on a clean exit and on SIGTERM, but a SIGKILL, a
// terminal window closed outright, or a laptop losing power all end a session
// with no notification at all. Those are not exotic cases — they are
// disproportionately the long, expensive, messy sessions this platform most
// wants to capture.
//
// So the daemon does not wait to be told a session ended. SessionStart spawns
// it, and it watches the harness process directly. When that process
// disappears, by any means, the daemon notices, finalises the session, flushes
// what is pending, and exits. Nothing about that path depends on the dying
// process cooperating.
//
// The second job is reconciliation. A daemon that is itself killed leaves a
// session marked open. Rather than run a background sweep — which would
// reintroduce exactly the polling the design rejects — the next SessionStart
// picks up the pieces: it looks for sessions whose owning process is gone,
// closes them, and re-queues anything they left behind. Recovery is therefore
// event-driven too, paid for by the next session rather than by a timer that
// runs whether or not it is needed.
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// State is the on-disk record of a live session, written by the daemon so a
// later process can tell what was running and whether it finished.
type State struct {
	SessionID string `json:"session_id"`
	// OwnerPID is the harness process this daemon shadows. Liveness of that
	// pid is the definition of "session still running".
	OwnerPID int `json:"owner_pid"`
	// DaemonPID lets a reconciler tell a live daemon from an abandoned state
	// file, so two daemons never shadow the same session.
	DaemonPID int `json:"daemon_pid"`
	// Build is the client build the daemon runs, so a reaper can tell a
	// daemon of an older build from one of its own; empty on states written
	// by builds before 2026-09-17, which is the same answer.
	Build string `json:"build,omitempty"`

	StartedAt time.Time `json:"started_at"`
	// Heartbeat is refreshed while the daemon runs. Its staleness is the
	// fallback signal when pid checks are ambiguous.
	Heartbeat time.Time `json:"heartbeat"`

	Cwd            string `json:"cwd,omitempty"`
	TranscriptPath string `json:"transcript_path,omitempty"`

	// Finalized records that the session was closed cleanly by a daemon rather
	// than abandoned. An unfinalized state file is the reconciler's input.
	Finalized bool      `json:"finalized"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
	// EndReason distinguishes how it ended, which is worth keeping: a session
	// that vanished is materially different from one that exited.
	EndReason string `json:"end_reason,omitempty"`
	// Recovered counts events the recovery pass re-derived from the
	// transcript at the end of this session.
	Recovered int `json:"recovered,omitempty"`
}

// Flusher performs ONE delivery cycle and reports how many items it sent.
//
// The progress count is not decoration: the daemon's shutdown path has to keep
// flushing until the backlog is gone, and "sent nothing this cycle" is the only
// safe termination signal. A cycle can legitimately return zero while items
// remain pending — a server that answers 200 without accepting or rejecting an
// item leaves it queued — so looping on "is the spool empty" would spin
// forever. Looping on progress terminates in both the healthy and the
// pathological case.
type Flusher interface {
	RunOnce(ctx context.Context) (sent int, err error)
}

// Reporter delivers ONE health sample about this machine.
//
// It is separate from Flusher, and driven on a separate goroutine, because the
// two have opposite failure contracts. A flush that fails must be retried until
// its data lands, since nothing else will ever carry it. A report that fails
// must be forgotten: the next sample supersedes it, and a daemon that spent its
// shutdown grace retrying telemetry would have spent it not delivering events.
//
// An implementation must bound its own work. The daemon waits for an in-flight
// report when the session ends, so a Report that never returns is a daemon that
// never exits.
type Reporter interface {
	Report(ctx context.Context) error
}

// Options configure a Daemon.
type Options struct {
	// Dir holds session state files.
	Dir string
	// SessionID is the session this daemon shadows.
	SessionID string
	// OwnerPID is the harness process to watch. Zero means the parent process.
	OwnerPID int
	// Build is the client build running this daemon, recorded in the state.
	Build string
	Cwd   string
	// TranscriptPath is recorded so a reconciler can find the session's file
	// even after the harness is gone.
	TranscriptPath string

	// Flush is called periodically and once more at shutdown.
	Flush Flusher
	// FlushInterval is how often to drain while the session runs.
	FlushInterval time.Duration

	// Report sends self-telemetry about this machine. Optional: a daemon
	// without one behaves exactly as it did before health reporting existed,
	// which is also what makes it safe to leave unset in tests about delivery.
	Report Reporter
	// ReportInterval is how often to send one.
	ReportInterval time.Duration
	// PollInterval is how often to check whether the owner is still alive.
	// This is a liveness check on one pid, not a filesystem scan, so it is
	// cheap enough to run often and is not the "background sweep" the design
	// rules out.
	PollInterval time.Duration
	// StaleAfter is how long a heartbeat may go unrefreshed before a
	// reconciler treats the daemon as dead even if its pid still resolves.
	StaleAfter time.Duration

	// ShutdownGrace bounds the final drain after the harness exits. The daemon
	// keeps flushing until the backlog stops shrinking or this elapses,
	// whichever comes first. It is a real deadline rather than an unbounded
	// loop because a daemon that refuses to exit while offline would linger on
	// the machine indefinitely; anything still queued when it expires stays in
	// the spool and ships on the next session.
	ShutdownGrace time.Duration

	// Recover, when set, runs once when the owner exits, before the final
	// drain and before Finalize, and returns how many events it recovered.
	// It is the transcript recovery pass: the daemon knows the session ended
	// and still has the transcript path, so this is the last moment the
	// events its hooks lost can be re-derived and shipped with the rest.
	Recover func(State) int

	// Alive reports whether a pid is running. Injected for tests.
	Alive func(pid int) bool
	Now   func() time.Time
}

const (
	defaultFlushInterval  = 5 * time.Second
	defaultPollInterval   = 2 * time.Second
	defaultStaleAfter     = 2 * time.Minute
	defaultShutdownGrace  = 30 * time.Second
	maxShutdownFlushCycle = 1000 // hard stop against a pathological progress loop

	// defaultReportInterval is how often the fleet hears from this machine, and
	// therefore how long a laptop that died mid-session goes on looking alive:
	// coverage subtracts the newest report time from now, so this interval is
	// the error bar on every "last seen" the fleet view prints.
	//
	// Not once per drain cycle. The drain runs every 5 seconds, so a report per
	// cycle is 720 an hour from a machine whose owner is at lunch, nearly all of
	// them byte-identical — a cost paid by every laptop, every request and every
	// row of the health table to learn nothing that the previous one did not say.
	//
	// Not only at exit either. SessionEnd does not fire on a SIGKILL, which is
	// the case this whole package exists for, so a sample written only on the way
	// out would be missing from exactly the machines whose sessions end badly —
	// and those are the ones a fleet operator needs to see.
	//
	// Five minutes sits between the two. The server calls a machine silent after
	// 24 hours, so a run of failed reports still leaves a working laptop covered,
	// while the conditions a report carries turn over on the scale of health's
	// own 15-minute delivery-stale threshold: three samples before a judgement
	// can change, which is enough to watch a machine go bad rather than to find
	// out afterwards.
	defaultReportInterval = 5 * time.Minute
)

// Daemon shadows one session.
type Daemon struct {
	opts  Options
	path  string
	state State
}

// New prepares a daemon and claims the session by writing its state file.
func New(opts Options) (*Daemon, error) {
	if opts.Dir == "" {
		return nil, errors.New("daemon: Dir is required")
	}
	if opts.SessionID == "" {
		return nil, errors.New("daemon: SessionID is required")
	}
	if opts.OwnerPID == 0 {
		opts.OwnerPID = os.Getppid()
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultPollInterval
	}
	if opts.StaleAfter <= 0 {
		opts.StaleAfter = defaultStaleAfter
	}
	if opts.ShutdownGrace <= 0 {
		opts.ShutdownGrace = defaultShutdownGrace
	}
	if opts.ReportInterval <= 0 {
		opts.ReportInterval = defaultReportInterval
	}
	if opts.Alive == nil {
		opts.Alive = processAlive
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("daemon: mkdir: %w", err)
	}
	d := &Daemon{
		opts: opts,
		path: statePath(opts.Dir, opts.SessionID),
		state: State{
			SessionID:      opts.SessionID,
			OwnerPID:       opts.OwnerPID,
			DaemonPID:      os.Getpid(),
			Build:          opts.Build,
			StartedAt:      opts.Now(),
			Heartbeat:      opts.Now(),
			Cwd:            opts.Cwd,
			TranscriptPath: opts.TranscriptPath,
		},
	}
	return d, d.save()
}

// ErrLocked reports that another process already holds a lock.
var ErrLocked = errors.New("daemon: already held by another process")

// SessionLockPath is the lock that grants the right to shadow one session. The
// suffix keeps it out of Reconcile's and Sweep's view, both of which walk only
// the .json state files.
func SessionLockPath(dir, sessionID string) string {
	return statePath(dir, sessionID) + ".lock"
}

// ClaimSession takes the exclusive right to shadow a session, returning
// ErrLocked when a daemon already has it.
//
// This is the gate that makes "one daemon per session" true. AlreadyRunning
// answers the same question more cheaply and is the right pre-check before
// paying for a process spawn, but it cannot be the decision: it reads a state
// file and the caller then writes one, and two SessionStart hooks racing through
// that gap both conclude they are alone. Two daemons draining one spool lease
// the same items, so every item is uploaded twice and an ack landing between
// another's write is how one goes missing.
func ClaimSession(dir, sessionID string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("daemon: mkdir: %w", err)
	}
	return TryLock(SessionLockPath(dir, sessionID))
}

// AlreadyRunning reports whether a live daemon is already shadowing this
// session. SessionStart can fire more than once for the same session (a resume,
// a fork), and two daemons flushing the same spool would duplicate work and
// race on the state file.
//
// It is advisory. ClaimSession is what actually decides, and a caller that acts
// on this answer alone is racing.
func AlreadyRunning(dir, sessionID string, alive func(int) bool, now time.Time, staleAfter time.Duration) bool {
	if alive == nil {
		alive = processAlive
	}
	if staleAfter <= 0 {
		staleAfter = defaultStaleAfter
	}
	st, err := load(statePath(dir, sessionID))
	if err != nil || st.Finalized {
		return false
	}
	if st.DaemonPID <= 0 || !alive(st.DaemonPID) {
		return false
	}
	// A pid can be reused after the original process dies. A stale heartbeat
	// alongside a live pid means the pid is almost certainly somebody else's,
	// so treat the daemon as gone rather than assume the session is covered.
	return now.Sub(st.Heartbeat) <= staleAfter
}

// Run shadows the session until the owner exits or ctx is cancelled.
//
// The loop is deliberately dumb: check liveness, flush on a cadence, refresh
// the heartbeat. All the subtlety is in what happens when the owner disappears,
// which is that we flush once more and finalise. That final flush is the whole
// reason the daemon outlives the harness rather than being killed with it.
func (d *Daemon) Run(ctx context.Context) error {
	poll := time.NewTicker(d.opts.PollInterval)
	defer poll.Stop()
	flush := time.NewTicker(d.opts.FlushInterval)
	defer flush.Stop()
	stopReporting := d.startReporting(ctx)
	defer stopReporting()

	for {
		select {
		case <-ctx.Done():
			// Cancellation is a shutdown, not an end-of-session: the harness
			// may still be running and another daemon may take over. Do not
			// finalise, but do try to get pending work out first.
			d.drainBacklog()
			return ctx.Err()

		case <-flush.C:
			// A periodic flush is one cycle on purpose. Falling behind here is
			// harmless because the next tick continues, and draining to empty
			// on every tick would let one large backlog monopolise the loop and
			// starve the liveness check.
			d.flushOnce(ctx)
			// Not after a cancel: a signal that landed during the flush may
			// have come from a reaper that finalized this state on disk, and
			// a beat here would write the live state back over it.
			if ctx.Err() == nil {
				d.beat()
			}

		case <-poll.C:
			if d.sessionEnded() {
				// The harness said goodbye (SessionEnd) and the hook left the
				// marker. The owner pid may live on for weeks: an IDE or the
				// desktop app hosts many sessions in one process, and a pid
				// can be reused. Without this a daemon per finished session
				// accumulated on such machines, each posting a health report every five minutes
				// and draining the shared spool with the build it was born
				// on (dozens per laptop were measured on a real fleet). The
				// transcript is complete at SessionEnd, so the recovery pass
				// runs here as it does when the owner dies.
				if d.opts.Recover != nil {
					d.state.Recovered = d.opts.Recover(d.state)
				}
				d.drainBacklog()
				return d.Finalize("session_ended")
			}
			if !d.opts.Alive(d.state.OwnerPID) {
				// The owner is gone by whatever means, including the SIGKILL
				// that no hook would have reported. This is the last chance to
				// deliver anything this session captured, so recover what the
				// hooks lost first, then drain the whole backlog rather than
				// a single batch before closing.
				if d.opts.Recover != nil {
					d.state.Recovered = d.opts.Recover(d.state)
				}
				d.drainBacklog()
				return d.Finalize("owner_exited")
			}
			d.beat()
		}
	}
}

// startReporting begins self-telemetry and returns the function that stops it.
//
// It gets its own goroutine because a health report is an HTTP request to the
// same server the events go to, and on a laptop behind a captive portal it
// blocks for the whole client timeout. Putting it in the loop above would stand
// that delay in front of the next flush and the next liveness check, which is
// telemetry about the pipeline degrading the pipeline — the one thing this must
// never do. A reporter that hangs costs a report; it must not cost an upload.
//
// The returned stop waits for an in-flight report so a test's fake server, and a
// real daemon's connection, are not written to after the session is over.
func (d *Daemon) startReporting(ctx context.Context) func() {
	if d.opts.Report == nil {
		return func() {}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(d.opts.ReportInterval)
		defer t.Stop()

		// The first sample goes now rather than one interval from now. Sessions
		// shorter than the interval are ordinary, and a machine whose owner only
		// ever runs short ones would otherwise never report at all — which in the
		// fleet view is indistinguishable from a laptop nobody ever installed
		// this on.
		d.report(ctx)

		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				d.report(ctx)
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// report sends one sample and drops the outcome on the floor.
//
// Dropped rather than returned: a Reporter records its own failures where a
// person can read them, and there is nothing this daemon could do with a failed
// report except stop doing the job the report describes. A telemetry error must
// not be able to become a delivery error.
func (d *Daemon) report(ctx context.Context) {
	_ = d.opts.Report.Report(ctx)
}

// Finalize closes the session's state file.
func (d *Daemon) Finalize(reason string) error {
	d.state.Finalized = true
	d.state.EndedAt = d.opts.Now()
	d.state.EndReason = reason
	return d.save()
}

// State returns a copy of the current state.
func (d *Daemon) State() State { return d.state }

// flushOnce runs a single delivery cycle. A failure is expected and
// unremarkable, because the laptop may simply be offline; the spool keeps the
// data, so there is nothing to do but try again later.
func (d *Daemon) flushOnce(ctx context.Context) int {
	if d.opts.Flush == nil {
		return 0
	}
	sent, _ := d.opts.Flush.RunOnce(ctx)
	return sent
}

// drainBacklog flushes repeatedly until the backlog stops shrinking or the
// grace period expires.
//
// This is the shutdown path, and getting it wrong loses data: the daemon is
// about to exit, and anything still queued waits for a future session that may
// not happen for days — by which point the harness's own 30-day cleanup may
// have removed the source transcript, making the loss permanent.
//
// It runs on a fresh context so that a cancelled parent cannot suppress the
// final delivery, and it terminates on lack of progress rather than on an empty
// spool, because an item the server neither accepts nor rejects stays pending
// legitimately and would otherwise spin here forever.
func (d *Daemon) drainBacklog() {
	if d.opts.Flush == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.opts.ShutdownGrace)
	defer cancel()

	for range maxShutdownFlushCycle {
		if ctx.Err() != nil {
			return
		}
		sent, err := d.opts.Flush.RunOnce(ctx)
		if sent == 0 {
			// Either the backlog is gone or nothing more can move right now.
			// Either way, continuing would spin; what remains ships next time.
			return
		}
		if err != nil {
			// Progress was made despite an error, so keep going: a partially
			// successful batch during a flaky connection is worth retrying.
			continue
		}
	}
}

func (d *Daemon) beat() {
	d.state.Heartbeat = d.opts.Now()
	// A reaper (StopStale) may have finalized this state on disk while this
	// daemon was draining after its signal; a heartbeat must not write the
	// live state back over that, or the session reads as a live daemon that
	// is dead. Only the heartbeat is guarded: a new daemon's first save and
	// a finalize write unconditionally, since a new image exec'd in place
	// keeps the pid of the one a reaper may just have finalized.
	if on, err := load(d.path); err == nil && on.Finalized && on.DaemonPID == d.state.DaemonPID {
		return
	}
	_ = d.save()
}

func (d *Daemon) save() error {
	b, err := json.MarshalIndent(d.state, "", " ")
	if err != nil {
		return err
	}
	// The temp name carries the pid: a reaper writes the same state through
	// its own temp file at the same moment.
	tmp := fmt.Sprintf("%s.%d.tmp", d.path, os.Getpid())
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, d.path)
}

// Abandoned is a session whose daemon died without finalising it.
type Abandoned struct {
	State  State
	Reason string
}

// Reconcile finds sessions left open by a dead daemon and closes them.
//
// This runs at SessionStart, which is what keeps recovery event-driven. The
// alternative — a periodic sweep — would mean a timer running on every laptop
// forever to handle a case that only matters when a new session begins anyway.
//
// It is deliberately conservative: a session is only reconciled when both the
// daemon and the owning harness are gone. A live daemon is left alone, and a
// dead daemon whose harness is somehow still running is reported but not
// closed, because closing it would mark a running session as finished.
func Reconcile(dir string, alive func(int) bool, now time.Time, staleAfter time.Duration) ([]Abandoned, error) {
	return ReconcileWith(dir, alive, now, staleAfter, nil)
}

// ReconcileWith is Reconcile with a recovery pass. For every session it
// closes, recover runs first with the session's state (the transcript path is
// in it), so the events a dead daemon's hooks lost are re-derived before the
// session is marked finished. A nil recover is Reconcile.
func ReconcileWith(dir string, alive func(int) bool, now time.Time, staleAfter time.Duration, recover func(State) int) ([]Abandoned, error) {
	if alive == nil {
		alive = processAlive
	}
	if staleAfter <= 0 {
		staleAfter = defaultStaleAfter
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // nothing has ever run here
		}
		return nil, err
	}

	var out []Abandoned
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		st, err := load(p)
		if err != nil {
			continue // unreadable or half-written; leave it for a human
		}
		if st.Finalized {
			continue
		}
		if daemonStillLive(st, alive, now, staleAfter) {
			continue
		}
		reason := "daemon_died"
		if alive(st.OwnerPID) {
			// The harness outlived its daemon. Something restarted or crashed
			// the daemon; do not declare the session over.
			out = append(out, Abandoned{State: st, Reason: "daemon_died_owner_alive"})
			continue
		}
		if recover != nil {
			st.Recovered = recover(st)
		}
		st.Finalized = true
		st.EndedAt = now
		st.EndReason = reason
		if b, err := json.MarshalIndent(st, "", " "); err == nil {
			tmp := p + ".tmp"
			if os.WriteFile(tmp, append(b, '\n'), 0o600) == nil {
				_ = os.Rename(tmp, p)
			}
		}
		out = append(out, Abandoned{State: st, Reason: reason})
	}
	return out, nil
}

func daemonStillLive(st State, alive func(int) bool, now time.Time, staleAfter time.Duration) bool {
	if st.DaemonPID <= 0 || !alive(st.DaemonPID) {
		return false
	}
	return now.Sub(st.Heartbeat) <= staleAfter
}

// Sweep removes finalized state files older than keep, so the state directory
// does not grow without bound on a machine that runs thousands of sessions.
func Sweep(dir string, keep time.Duration, now time.Time) (int, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var n int
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		st, err := load(p)
		if err != nil || !st.Finalized {
			continue
		}
		if !st.EndedAt.IsZero() && now.Sub(st.EndedAt) > keep {
			if os.Remove(p) == nil {
				n++
			}
			_ = os.Remove(p + endedSuffix)
		}
	}
	// A marker whose session never had a daemon (a session with no prompt
	// starts none) has no state file to leave with; it leaves by age.
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), endedSuffix) {
			continue
		}
		if fi, err := e.Info(); err == nil && now.Sub(fi.ModTime()) > keep {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return n, nil
}

// endedSuffix names the marker the SessionEnd hook leaves beside a session's
// state file; the daemon finalizes on its next poll when it sees it.
const endedSuffix = ".ended"

func endedPath(dir, sessionID string) string { return statePath(dir, sessionID) + endedSuffix }

// MarkEnded records that the harness reported the end of sessionID, so its
// daemon finalizes on its next poll instead of waiting for the owner pid.
func MarkEnded(dir, sessionID string, now time.Time) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(endedPath(dir, sessionID), []byte(now.UTC().Format(time.RFC3339)+"\n"), 0o600)
}

// ClearEnded forgets an earlier end of sessionID. The hook calls it when the
// session is live again (a start, a resume, a prompt) before it starts a
// daemon, so a marker from before cannot end the new one; a marker written
// after that is honoured.
func ClearEnded(dir, sessionID string) {
	_ = os.Remove(endedPath(dir, sessionID))
}

func (d *Daemon) sessionEnded() bool {
	_, err := os.Stat(endedPath(d.opts.Dir, d.opts.SessionID))
	return err == nil
}

// Stale is a session StopStale finalized: its daemon ran a build other than
// the current one (or none, for states written before builds were recorded)
// and was stopped, or was already dead under a live owner.
type Stale struct {
	SessionID string
	DaemonPID int
	Build     string
	Killed    bool
}

// StopStale finalizes every unfinalized session in dir whose daemon is not
// running the current build, and every session whose daemon is dead under a
// live owner (an IDE or the desktop app hosts many sessions in one process,
// and ReconcileWith closes a dead daemon only once its owner is gone too).
//
// A daemon is live when it holds the session lock (held for its whole life
// and released by the kernel at its death, so immune to a reused pid and to
// a heartbeat gap after a laptop wakes) or, for a build that might not hold
// one, when its pid exists and its heartbeat is fresh (daemonStillLive). A
// live daemon of another build (other than self) is signalled (SIGTERM in
// production; it drains its backlog and exits) and its session finalized as
// daemon_stale; a live daemon of this build is left alone. A dead daemon
// under a live owner is finalized as it stands, daemon_stale for another
// build and daemon_died for this one; under a dead owner the session is an
// abandoned one and left to ReconcileWith, which recovers its transcript
// first. A finalized session gets a fresh daemon at its next prompt, since
// AlreadyRunning is false for it; one that ended stops counting and posting.
// selfSession, when set, names the session the caller itself is the daemon
// of: its state is skipped, since the caller's own lock would read as live.
// The build is the key, not the binary's age: a reinstall of the same build,
// a touch, a clock step or a developer's binary change the file's time and
// nothing else.
func StopStale(dir, build string, self int, selfSession string, alive func(int) bool, held func(sessionID string) bool, kill func(int) error, now time.Time) ([]Stale, error) {
	if alive == nil {
		alive = processAlive
	}
	if held == nil {
		held = func(id string) bool { return lockHeld(dir, id) }
	}
	staleAfter := defaultStaleAfter
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []Stale
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		st, err := load(p)
		if err != nil || st.Finalized || st.DaemonPID <= 0 || st.DaemonPID == self {
			continue
		}
		if selfSession != "" && st.SessionID == selfSession {
			// A daemon starting for this session holds the session lock
			// itself while the state still names its predecessor; the
			// predecessor's pid is not this daemon's to signal (it may be
			// dead and reused). New overwrites that state in a moment.
			continue
		}
		current := st.Build == build
		live := held(st.SessionID) || daemonStillLive(st, alive, now, staleAfter)
		reason := "daemon_stale"
		switch {
		case live && current:
			continue
		case live:
			if err := kill(st.DaemonPID); err != nil {
				continue
			}
		case !alive(st.OwnerPID):
			continue
		case current:
			reason = "daemon_died"
		}
		st.Finalized = true
		st.EndedAt = now
		st.EndReason = reason
		if !writeState(dir, p, st) {
			continue
		}
		out = append(out, Stale{SessionID: st.SessionID, DaemonPID: st.DaemonPID, Build: st.Build, Killed: live})
	}
	return out, nil
}

// lockHeld reports whether some process holds the session's lock: an
// attempt to take it fails with ErrLocked. A lock taken here because it was
// free is released at once.
func lockHeld(dir, sessionID string) bool {
	l, err := TryLock(SessionLockPath(dir, sessionID))
	if errors.Is(err, ErrLocked) {
		return true
	}
	if err == nil {
		_ = l.Close()
	}
	return false
}

// writeState lands st at p through a temp file of its own, so a daemon
// writing its heartbeat at the same instant cannot rename a torn file into
// place; the last rename wins whole.
func writeState(dir, p string, st State) bool {
	b, err := json.MarshalIndent(st, "", " ")
	if err != nil {
		return false
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(p)+".stale-*")
	if err != nil {
		return false
	}
	name := tmp.Name()
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return false
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return false
	}
	if err := os.Rename(name, p); err != nil {
		_ = os.Remove(name)
		return false
	}
	return true
}

func load(p string) (State, error) {
	var st State
	b, err := os.ReadFile(p)
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, err
	}
	return st, nil
}

// LoadState reads one session's state file. The deliverer uses it to find
// the transcript a hook Stop copy belongs to.
func LoadState(dir, sessionID string) (State, error) {
	return load(statePath(dir, sessionID))
}

func statePath(dir, sessionID string) string {
	return filepath.Join(dir, sanitize(sessionID)+".json")
}

// processAlive reports whether a pid exists.
//
// Signal 0 performs the permission and existence checks without delivering
// anything, which is the standard way to ask this question. EPERM is treated as
// alive: the process exists, we merely may not signal it.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	return errors.Is(err, syscall.EPERM)
}

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
