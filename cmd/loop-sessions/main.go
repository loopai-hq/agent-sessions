// Command loop-sessions is the client agent.
//
// It has two audiences with opposite needs. Claude Code invokes `hook`
// thousands of times a day and needs it to be fast, silent, and incapable of
// failing in a way that affects a user's turn. A person invokes `install`,
// `status` and `pause` occasionally and needs plain language, no jargon, and
// output that tells them what is actually happening. The subcommands below are
// written to those two contracts rather than to one uniform style.
//
// The single rule that overrides everything else: the hook path never returns
// non-zero and never writes to stdout. A hook that errors or chatters degrades
// the harness for the person using it, and a telemetry tool that makes someone's
// editor worse is a telemetry tool they remove.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/daemon"
	"github.com/loopai-hq/agent-sessions/internal/discovery"
	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/hooks"
	"github.com/loopai-hq/agent-sessions/internal/spool"
	"github.com/loopai-hq/agent-sessions/internal/upgrade"
)

// Version is stamped at build time. BuildDate is stamped by `make release`
// for builds the toolchain cannot stamp itself (a git archive has no .git);
// health reports carry whichever the binary has.
var (
	Version   = "dev"
	BuildDate = ""
)

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	// The hook path is separated before anything else can go wrong. It must not
	// depend on config parsing, network, or any other subcommand's setup.
	if cmd == "hook" {
		os.Exit(runHook(args))
	}

	var err error
	switch cmd {
	case "install":
		err = runInstall(args)
	case "uninstall":
		err = runUninstall(args)
	case "discover":
		err = runDiscover(args)
	case "backfill":
		err = runBackfill(args)
	case "status":
		err = runStatus(args)
	case "mirror":
		err = runMirror(args)
	case "pause":
		err = runPause(args, true)
	case "resume":
		err = runPause(args, false)
	case "doctor":
		err = runDoctor(args)
	case "daemon":
		err = runDaemon(args)
	case "version", "--version", "-v":
		fmt.Println(Version)
	case "help", "--help", "-h":
		usage(os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "loop-sessions: unknown command %q\n\n", cmd)
		usage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "loop-sessions: %v\n", err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `loop-sessions - collate your local AI coding sessions

  install             set up: find sessions, sign in, start capturing
  uninstall           stop capturing and clean up
  status              show what is being captured and whether it is working
  discover            re-scan this machine for agent session files
  backfill            import the sessions already on this machine
                      (--since all|30d|2026-07-01, --session <id>, --dry-run)
  pause [--for 2h]    stop capturing until resumed
  resume              start capturing again
  doctor              diagnose problems and say how to fix them
                      (--redrive: queue stuck items again;
                       --replay-quarantine: post one stuck item, print the verdict)
  version             print the agent version

Fleet operator actions:
  install --hooks-only    put the capture hooks back without signing in again
  daemon --upgrade-now    check the release host now and replace this binary
                          (honours the pin and a pause, like the daemon does)

Internal (invoked by Claude Code and by the installer, not by you):
  hook                translate one lifecycle hook into a captured event
  daemon              shadow a session until the harness exits
`)
}

// ---------------------------------------------------------------- daemon

// runDaemon shadows one session and delivers what it captures.
//
// It has no terminal: it is started detached from a SessionStart hook, so its
// stdout and stderr go to the null device and the only way it can report
// anything is the agent log. Every early return below therefore says why in the
// log before it takes, because a daemon that exits silently is indistinguishable
// from one that was never started, and that ambiguity is what hid this whole
// pipeline being disconnected.
func runDaemon(args []string) error {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	sessionID := fs.String("session", "", "session id to shadow")
	ownerPID := fs.Int("owner-pid", 0, "harness pid to watch (default: parent)")
	cwd := fs.String("cwd", "", "session working directory")
	transcript := fs.String("transcript", "", "session transcript path")
	upgradeNow := fs.Bool("upgrade-now", false, "check the release host now, ignoring the throttle, and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	p := config.Paths{}
	if *upgradeNow {
		return runUpgradeNow(p)
	}
	if *sessionID == "" {
		return errors.New("daemon: --session is required")
	}

	// Exactly one daemon per session, decided atomically. Several editor windows
	// opening at once fire several SessionStart hooks, and two daemons leasing
	// one spool would send every item twice. The lock is held for this process's
	// whole life and released by the kernel if it is killed.
	// The daemon's own binary is identified before anything else runs: the
	// digest must be of the image this process is, and a replacement can
	// land during the first upload below (another session's daemon
	// upgrading, an install) and would otherwise be taken for the start.
	self, selfErr := os.Executable()
	start, digestErr := upgrade.Digest(self)
	// Stale daemons are reaped before this daemon claims its session: the
	// reaper's liveness test is the session lock, and this daemon's own
	// claim would otherwise vouch for whatever pid the state still names.
	reapStale(p, *sessionID, time.Now())
	claim, err := daemon.ClaimSession(p.StateDir(), *sessionID)
	if errors.Is(err, daemon.ErrLocked) {
		return nil
	}
	if err != nil {
		logf("daemon: could not claim session %s: %v", *sessionID, err)
		return err
	}
	defer claim.Close()

	cfg, err := config.Load(p)
	if err != nil {
		logf("daemon: not delivering: %v", err)
		return err
	}
	if cfg.Paused {
		// Paused means paused for delivery too, and a paused machine reads as a
		// choice in the fleet view rather than as a broken one.
		logf("daemon: capture is paused; not delivering")
		return nil
	}

	flusher, err := newDelivery(p, cfg)
	if err != nil {
		logf("daemon: nothing can be uploaded: %v", err)
		return err
	}
	// Parked items go back in the queue when the binary has changed since
	// they were parked, and once a day regardless: what the server kept not
	// deciding about is usually decided by the next deploy on either side.
	redriveParkedIfDue(p, flusher.sp, time.Now())
	refreshHooksIfDue(p)

	// The daemon is where health reporting belongs: it already runs for the life
	// of a session, already holds the config and the device credential, and is
	// already the only thing on this machine that talks to the server on a
	// cadence. A reporter that could not be built is logged and left nil, because
	// a machine that cannot describe itself must still deliver what it captured.
	var reporter daemon.Reporter
	if hr, err := newHealthReporter(p, cfg, flusher.sp); err != nil {
		logf("daemon: no health reports will be sent, so the fleet view will call this machine silent: %v", err)
	} else {
		reporter = hr
	}

	d, err := daemon.New(daemon.Options{
		Dir:            p.StateDir(),
		SessionID:      *sessionID,
		OwnerPID:       *ownerPID,
		Build:          Version,
		Cwd:            *cwd,
		TranscriptPath: *transcript,
		Flush:          flusher,
		Report:         reporter,
		Recover:        reconcileRecover(p, cfg),
	})
	if err != nil {
		logf("daemon: %v", err)
		return err
	}
	logf("daemon: shadowing session %s, watching harness pid %d", *sessionID, d.State().OwnerPID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Deliver the backlog now rather than one flush interval from now. Whatever
	// a previous session left behind — because its daemon was killed, or because
	// the machine was offline — is the reason SessionStart starts a daemon at
	// all, and making it wait would leave a queue that a person can see sitting
	// still for the first seconds of every session.
	if _, err := flusher.RunOnce(ctx); err != nil {
		// Already logged with its reason by the flusher. Not fatal: the spool
		// keeps the data and the loop below retries on a backoff.
		_ = err
	}

	// Check for a newer build, from here rather than from the hook that started
	// this daemon.
	//
	// The hooks are async and the harness never waits on them, so a network
	// call there would not slow anybody's turn; it would still be wrong. A hook
	// is a process per event, PostToolUse fires tens of thousands of times in a
	// working week, and the harness kills async hooks at the teardown of a
	// `claude -p` run. A manifest fetch belongs in the one process on the
	// machine that is long-lived, detached, and already talks to the server on
	// a cadence, and it is throttled through a stamp so fifty daemons a day are
	// one fetch.
	//
	// In a goroutine because delivery is this daemon's actual job and must not
	// wait behind a download on a hotel connection; after the first flush for
	// the same reason. A paused machine never reaches this line: the early
	// return above sees to that, and it should, because pausing means leave this
	// machine alone and that has to include replacing its binary.
	// The run context is the one a restart cancels (restart.go); it derives
	// from the signal context, so SIGTERM still ends everything, and the
	// three background passes stop with it before the exec cuts them.
	var restart atomic.Bool
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	go startUpgradeCheck(runCtx, p, cfg)
	go startCaptureRewalk(p, cfg)
	go runRepairIfDue(runCtx, p, cfg, false)
	// The binary this daemon runs is watched for an upgrade (restart.go):
	// a different build at the same path that runs ends the run through a
	// context of its own (the signal handlers stay registered for the
	// drain), and the process execs it in place, so no session keeps an
	// old daemon.
	if selfErr == nil && digestErr == nil {
		go watchBinary(runCtx, self, start, binaryWatchEvery, func() {
			restart.Store(true)
			cancelRun()
		})
	} else {
		logf("daemon: not watching the binary for upgrades (%v %v); this daemon keeps its build until its session ends", selfErr, digestErr)
	}
	runErr := d.Run(runCtx)
	if restart.Load() && !d.State().Finalized {
		now, _ := upgrade.Digest(self)
		logf("daemon: the binary on disk changed (%s -> %s); restarting in place on the new build", short(start), short(now))
		if err := restartInPlace(self); err != nil {
			logf("daemon: could not restart in place: %v; exiting, and the session has no daemon until the binary at %s runs again", err, self)
		}
		return nil
	}
	logf("daemon: exiting after %s", endReason(d, runErr))
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

// upgradeBudget bounds one upgrade attempt. Generous enough for a 7 MB download
// on a poor connection, short enough that a server which accepts the connection
// and then says nothing does not leave a goroutine holding it for the whole
// session.
const upgradeBudget = 10 * time.Minute

// upgradeRecheck is how long a daemon waits before looking again. One check at
// start was enough when sessions lasted hours; the fleet turned out to carry
// week-long sessions whose daemons — and therefore whose machines — sat on the
// build the session started with through four releases. A recheck this
// infrequent still converges every disk binary within a working day while
// costing one manifest read per interval.
const upgradeRecheck = 6 * time.Hour

// startUpgradeCheck replaces this machine's binary when the release host is
// serving a different one.
//
// Nothing here is fatal and nothing here is returned. The daemon's job is to
// deliver what was captured, and an upgrade that cannot happen must not stop
// that: the worst outcome of a failed check is a machine that stays on the build
// it was already happily running.
func startUpgradeCheck(ctx context.Context, p config.Paths, cfg config.Config) {
	if cfg.DisableAutoUpgrade {
		logf("daemon: auto-upgrade is switched off in the config; staying on %s", Version)
		return
	}
	// Throttled across daemons, not only within one. Every session used to
	// fetch the manifest at start, which on a machine starting fifty sessions
	// a day was fifty reads to learn nothing; the stamp makes one check per
	// interval per machine whichever daemon happens to be alive.
	if !stampDue(p, stampUpgradeCheck, upgradeRecheck, time.Now()) {
		if last, ok := readStamp(p, stampUpgradeCheck); ok {
			logf("daemon: release host checked %s ago; next check after %s", time.Since(last).Round(time.Minute), upgradeRecheck)
		}
	} else {
		checkUpgradeOnce(ctx, p, cfg)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(upgradeRecheck):
			// A daemon still here after the interval belongs to a session that
			// outlived it. Checking again is idempotent: once the disk binary
			// matches the manifest, every later pass is a single small read.
			if stampDue(p, stampUpgradeCheck, upgradeRecheck, time.Now()) {
				checkUpgradeOnce(ctx, p, cfg)
			}
		}
	}
}

// checkUpgradeOnce runs one check and records its outcome where the health
// report reads it, so the fleet can tell "cannot reach the release host" from
// "pinned" from "somebody else owns this binary" instead of calling all three
// stale.
func checkUpgradeOnce(ctx context.Context, p config.Paths, cfg config.Config) health.UpgradeStatus {
	ctx, cancel := context.WithTimeout(ctx, upgradeBudget)
	defer cancel()

	now := time.Now().UTC()
	res, err := upgrade.Run(ctx, upgrade.Options{
		BaseURL: cfg.Endpoint,
		Channel: cfg.Channel,
		Version: Version,
		Logf:    logf,
	})
	st := health.UpgradeStatus{LastCheckAt: now, PublishedSHA: res.PublishedSHA}
	switch {
	case errors.Is(err, context.Canceled):
		// The session ended mid-check. Ordinary, and the next session's daemon
		// will find the same stale binary and try again.
		st.Result, st.Error = "error", "interrupted by session end"
	case err != nil:
		logf("daemon: could not upgrade, so this machine stays on %s: %v", Version, err)
		st.Result, st.Error = "error", err.Error()
	case res.Skipped != "":
		st.Result, st.SkippedReason = "skipped", res.Skipped
	case res.Upgraded:
		logf("daemon: upgraded from %s; daemons on this build restart in place shortly, new sessions start on the new build", Version)
		st.Result = "upgraded"
	case res.Stale:
		st.Result = "stale"
	default:
		st.Result = "ok"
	}
	writeUpgradeStatus(p, st)
	if err := writeStamp(p, stampUpgradeCheck, now); err != nil {
		logf("daemon: could not record the upgrade check: %v", err)
	}
	return st
}

// runUpgradeNow is `daemon --upgrade-now`: the fleet CTA for a machine that
// is behind. It ignores the throttle, honours the pin and a pause, and says
// what it did. Pause is honoured here as it is in the daemon: it means leave
// this machine alone, and that has to include replacing its binary, whoever
// asks. The operator lifts it with `loop-sessions resume` first.
func runUpgradeNow(p config.Paths) error {
	cfg, err := config.Load(p)
	if err != nil {
		return err
	}
	if cfg.DisableAutoUpgrade {
		fmt.Println("Auto-upgrade is switched off in the config (disable_auto_upgrade); this machine stays on", Version)
		return nil
	}
	if cfg.Paused {
		fmt.Println("Capture is paused, and a paused machine is left alone, binary included; run `loop-sessions resume` first. Staying on", Version)
		return nil
	}
	st := checkUpgradeOnce(context.Background(), p, cfg)
	switch st.Result {
	case "upgraded":
		fmt.Printf("Upgraded from %s to the published build (%s); running daemons on this build restart in place shortly, new sessions start on it.\n", Version, short(st.PublishedSHA))
	case "ok":
		fmt.Printf("Already running the published build on the %s channel.\n", cfg.Channel)
	case "stale":
		fmt.Printf("The published build (%s) differs from this one and was not installed.\n", short(st.PublishedSHA))
	case "skipped":
		fmt.Printf("Not upgraded: %s.\n", st.SkippedReason)
	default:
		fmt.Printf("The check failed: %s\n", st.Error)
	}
	return nil
}

// short trims a digest for a sentence.
func short(sum string) string {
	if len(sum) > 12 {
		return sum[:12]
	}
	return sum
}

// refreshHooksIfDue brings the registered hooks up to this binary's defaults
// once per binary version. The self-upgrade replaces the binary and nothing
// else, so the SessionEnd timeout an older release wrote into settings.json
// stays there until something rewrites it; without this pass only the
// machines whose owner happens to run `install --hooks-only` would get the
// current budget. hooks.Refresh rewrites entries it finds by exact command
// and adds none, so a daemon started from a binary the hooks do not name (a
// development build) touches nothing.
func refreshHooksIfDue(p config.Paths) {
	if readText(p, stampHooksVer) == Version {
		return
	}
	self, err := os.Executable()
	if err != nil {
		return
	}
	updated, err := hooks.Refresh(hooks.Options{Binary: self})
	if err != nil {
		// Not stamped: a settings file that cannot be read today may be
		// readable tomorrow, and the log line is the only sign of it.
		logf("daemon: could not bring the capture hooks up to this release's defaults: %v", err)
		return
	}
	if err := writeText(p, stampHooksVer, Version); err != nil {
		logf("daemon: could not record the hook refresh: %v", err)
	}
	if len(updated) > 0 {
		logf("daemon: brought %d capture hook(s) up to this release's defaults: %s", len(updated), strings.Join(updated, ", "))
	}
}

// redriveParkedIfDue returns parked items to the queue when the binary has
// changed since they were parked, and once a day regardless.
func redriveParkedIfDue(p config.Paths, sp *spool.Spool, now time.Time) {
	versionChanged := readText(p, stampRedriveVer) != Version
	if !versionChanged && !stampDue(p, stampRedriveLast, 24*time.Hour, now) {
		return
	}
	n, err := sp.RedriveParked()
	if err != nil {
		logf("daemon: could not redrive parked items: %v", err)
		return
	}
	_ = writeText(p, stampRedriveVer, Version)
	_ = writeStamp(p, stampRedriveLast, now)
	if n > 0 {
		why := "daily"
		if versionChanged {
			why = "the binary changed"
		}
		logf("daemon: %d parked item(s) returned to the queue (%s)", n, why)
	}
}

// startCaptureRewalk re-imports this machine's history when this build extracts
// more from a transcript than the build that last walked it.
//
// The counterpart to startUpgradeCheck, and the reason that one matters: an
// agent that silently upgrades itself and then only applies its improvements to
// tomorrow's sessions leaves a corpus where the same transcript means different
// things depending on when it happened to be read.
//
// Gated on a recorded version rather than run on every upgrade. Most upgrades
// change delivery, the daemon, or the server contract and extract nothing new; a
// re-walk for those is nine minutes of CPU and a few hundred megabytes on the
// wire to produce rows the server already has and will discard.
func startCaptureRewalk(p config.Paths, cfg config.Config) {
	if cfg.CaptureSchemaVersion >= event.CaptureSchema {
		return
	}
	// Below the baseline is not stale, it is unrecorded. Adopt the version and
	// walk nothing: the extraction this machine already ran is the one the
	// baseline describes, so a re-walk would rewrite every row with identical
	// content and a higher number in one column.
	if event.CaptureSchema <= event.CaptureBaseline {
		if fresh, err := config.Load(p); err == nil {
			fresh.CaptureSchemaVersion = event.CaptureSchema
			if err := config.Save(p, fresh); err != nil {
				logf("daemon: could not record the capture version: %v", err)
				return
			}
			logf("daemon: adopted capture v%d without re-reading; extraction is unchanged at this version",
				event.CaptureSchema)
		}
		return
	}
	if cfg.DisableAutoUpgrade {
		// One switch for both halves. Somebody who has pinned their agent has
		// not asked for it to spend an evening re-uploading their history.
		logf("daemon: auto-upgrade is off, so history stays at capture v%d", cfg.CaptureSchemaVersion)
		return
	}

	from, to := cfg.CaptureSchemaVersion, event.CaptureSchema
	logf("daemon: capture improved from v%d to v%d; re-reading this machine's history", from, to)

	// io.Discard rather than stdout: the daemon has no terminal, and the agent
	// log is the only place anything it says can be read.
	if err := rewalkForCaptureSchema(p, cfg, from, to, io.Discard); err != nil {
		if errors.Is(err, context.Canceled) {
			logf("daemon: the re-read stopped with the session; the next one resumes it")
			return
		}
		logf("daemon: could not re-read history, so it stays at capture v%d: %v", from, err)
		return
	}

	// Recorded only on success, so an interrupted or failed re-walk is retried
	// by the next session rather than being silently written off.
	fresh, err := config.Load(p)
	if err != nil {
		logf("daemon: re-read finished but the version could not be recorded: %v", err)
		return
	}
	fresh.CaptureSchemaVersion = to
	if err := config.Save(p, fresh); err != nil {
		logf("daemon: re-read finished but the version could not be recorded: %v", err)
		return
	}
	logf("daemon: history re-read at capture v%d", to)
}

// endReason describes why the daemon stopped, in the words the state file uses
// where it has them. A shutdown signal leaves no EndReason because the session
// is not over — another daemon may take it over — so it is named separately.
func endReason(d *daemon.Daemon, runErr error) string {
	if r := d.State().EndReason; r != "" {
		return r
	}
	if errors.Is(runErr, context.Canceled) {
		return "shutdown signal"
	}
	if runErr != nil {
		return runErr.Error()
	}
	return "an unrecorded reason"
}

// ---------------------------------------------------------------- discover

func runDiscover(args []string) error {
	fs := flag.NewFlagSet("discover", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	p := config.Paths{}
	skip := map[discovery.Tool]bool{}
	roots := map[discovery.Tool]string{}
	if cfg, err := config.Load(p); err == nil {
		for _, s := range cfg.SkippedTools {
			skip[discovery.Tool(s)] = true
		}
		for k, v := range cfg.Roots {
			roots[discovery.Tool(k)] = v
		}
	}

	findings := discovery.Run(discovery.Options{Skip: skip, UserPaths: roots})
	sum := discovery.Summarize(findings, time.Now())
	_ = discovery.WriteSummary(p.DiscoveryFile(), sum)

	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(sum)
	}
	printDiscovery(sum)
	return nil
}

func printDiscovery(s discovery.Summary) {
	fmt.Printf("Scanned this machine (%s/%s)\n\n", s.OS, s.Arch)
	var found, absent int
	for _, f := range s.Findings {
		switch {
		case f.Skipped:
			fmt.Printf("  %-16s skipped by you\n", f.Tool)
		case f.State == discovery.Found:
			fmt.Printf("  %-16s %d sessions, %s\n", f.Tool, f.Sessions, humanBytes(f.Bytes))
			found++
		case f.State == discovery.Installed:
			fmt.Printf("  %-16s installed, but its sessions are not where expected\n", f.Tool)
		default:
			absent++
		}
	}
	if absent > 0 {
		fmt.Printf("  (%d other tools not present on this machine)\n", absent)
	}
	fmt.Printf("\n%d sessions, %s total\n", s.Sessions, humanBytes(s.Bytes))
	if len(s.NeedsAsk) > 0 {
		// Asking beats assuming: a tool someone uses daily reported as absent
		// is how a fleet ends up with silent coverage holes.
		fmt.Printf("\nThese look installed but I could not find their sessions:\n")
		for _, t := range s.NeedsAsk {
			fmt.Printf("  %s\n", t)
		}
		fmt.Printf("\nIf you use them, tell me where they are:\n")
		fmt.Printf("  loop-sessions discover --set <tool>=<path>\n")
	}
}

// ---------------------------------------------------------------- status

// deliveryState is the question `status` exists to answer.
//
// Nobody runs this command on a machine they believe is fine, so a single number
// is the wrong shape for its answer: "0 waiting" is simultaneously what a
// perfectly healthy laptop prints and what one whose hooks never fired prints,
// and a reading that cannot separate those two turns a recoverable problem into
// an invisible one. These five values are exhaustive and mutually exclusive, and
// every one of them names a different thing to do next.
type deliveryState string

const (
	// stateUnknown means the outbox itself could not be read. It exists so that
	// a failure to measure can never be rendered as a measurement of zero.
	stateUnknown deliveryState = "unknown"
	// stateNothingCaptured means no events have ever reached the spool: the
	// problem, if there is one, is upstream of delivery.
	stateNothingCaptured deliveryState = "nothing_captured"
	// stateWaiting means a queue exists and is young enough to be in normal
	// flight.
	stateWaiting deliveryState = "waiting"
	// stateNotUploading means data is captured and is not moving.
	stateNotUploading deliveryState = "not_uploading"
	// stateUpToDate means everything captured has been acknowledged by the
	// server.
	stateUpToDate deliveryState = "up_to_date"
)

// statusView is everything this client knows about its own machine. Both
// `status` and `doctor` render this one struct, so the two commands cannot
// disagree about whether a laptop is working.
//
// Timestamps here are pointers so that "never" serialises as null rather than
// as the year 1. A zero time.Time ignores omitempty and prints
// "0001-01-01T00:00:00Z", which a dashboard renders as a date — and a machine
// that has never uploaded appearing to have uploaded at some ancient timestamp
// is the same class of lie this whole command was rewritten to stop telling.
type statusView struct {
	Configured  bool       `json:"configured"`
	Email       string     `json:"email,omitempty"`
	Paused      bool       `json:"paused"`
	PausedSince *time.Time `json:"paused_since,omitempty"`
	Version     string     `json:"agent_version"`

	// State and Summary are the verdict: one machine-readable enum and one
	// sentence a person can act on.
	State   deliveryState `json:"state"`
	Summary string        `json:"summary"`
	// Reason is why delivery is failing, or why that is not known.
	Reason string `json:"reason,omitempty"`

	Spool spool.Stats `json:"spool"`
	// SpoolError, when set, means the numbers in Spool are not measurements and
	// must not be shown as if they were.
	SpoolError  string     `json:"spool_error,omitempty"`
	LastUpload  *time.Time `json:"last_upload,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	LastErrorAt *time.Time `json:"last_error_at,omitempty"`

	Problems []string `json:"problems,omitempty"`
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	p := config.Paths{}
	v := collectStatus(p, time.Now())

	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(v)
	}
	printStatus(v)
	return nil
}

func printStatus(v statusView) {
	if !v.Configured {
		fmt.Println("Not set up yet. Run: loop-sessions install")
		if len(v.Problems) > 0 {
			for _, s := range v.Problems {
				fmt.Printf("  - %s\n", s)
			}
		}
		return
	}
	fmt.Printf("Capturing as %s\n", v.Email)

	// Capture states the setting; Uploads states the outcome. They were one line
	// reading "Status: active" before, which conflated a flag the user set with
	// a claim about whether the machine works — and that line was printed by a
	// laptop which had at that moment uploaded nothing, ever.
	switch {
	case v.Paused && v.PausedSince != nil:
		fmt.Printf("Capture:  PAUSED since %s\n", v.PausedSince.Format(time.RFC1123))
	case v.Paused:
		// A config that says paused without saying when. Still paused; saying so
		// without a date beats not saying so.
		fmt.Printf("Capture:  PAUSED\n")
	default:
		fmt.Printf("Capture:  on\n")
	}
	fmt.Printf("Uploads:  %s\n", v.Summary)
	if v.Reason != "" {
		fmt.Printf("Reason:   %s\n", v.Reason)
	}
	if v.SpoolError != "" {
		fmt.Printf("Queue:    could not be read: %s\n", v.SpoolError)
	} else {
		fmt.Printf("Queue:    %d item(s), %s\n", v.Spool.Pending, humanBytes(v.Spool.Bytes))
	}
	if v.Spool.Quarantine > 0 {
		fmt.Printf("Stuck:    %d item(s) will not retry on their own\n", v.Spool.Quarantine)
	}
	if len(v.Problems) > 0 {
		fmt.Println("\nProblems:")
		for _, s := range v.Problems {
			fmt.Printf("  - %s\n", s)
		}
		fmt.Println("\nRun `loop-sessions doctor` for details.")
	}
}

// collectStatus reads the machine once and derives everything from that reading.
//
// Every failure below becomes a stated fact rather than a default. The previous
// version discarded the error from opening the spool and the error from counting
// it, so an outbox that could not be read printed the same "0 item(s), 0 B" as
// an empty one. A measurement that failed and a measurement of nothing are
// different answers, and a status command that renders them identically is worse
// than one that refuses to answer.
func collectStatus(p config.Paths, now time.Time) statusView {
	cfg, cfgErr := config.Load(p)
	v := statusView{Configured: cfgErr == nil, Version: Version, State: stateUnknown}
	if cfgErr != nil {
		v.Summary = "not set up on this machine yet"
		if !errors.Is(cfgErr, config.ErrNotConfigured) {
			v.Problems = append(v.Problems, cfgErr.Error())
		}
		return v
	}
	v.Email, v.Paused = cfg.Email, cfg.Paused
	v.PausedSince = whenever(cfg.PausedSince)

	var (
		st       spool.Stats
		sd       spool.State
		spoolErr error
	)
	sp, openErr := spool.Open(spool.Options{
		Dir:          p.SpoolDir(),
		MaxBytes:     cfg.SpoolMaxBytes,
		MinFreeRatio: cfg.MinFreeRatio,
	})
	guard := spool.DefaultGuard()
	if openErr != nil {
		spoolErr = openErr
	} else {
		guard = sp.Guard()
		st, spoolErr = sp.Stats()
		// The delivery record is read separately from the item count because the
		// two fail independently: a spool we can list but whose history we cannot
		// read still knows how much is queued, and should say so.
		var stateErr error
		if sd, stateErr = sp.State(); stateErr != nil {
			v.Problems = append(v.Problems,
				"this machine's delivery history could not be read, so \"never uploaded\" below may be wrong: "+stateErr.Error())
		}
	}
	if spoolErr != nil {
		v.SpoolError = spoolErr.Error()
	}
	v.Spool = st
	v.LastUpload = whenever(sd.LastSuccess)
	v.LastError, v.LastErrorAt = sd.LastError, whenever(sd.LastErrorAt)

	v.State, v.Summary = verdict(st, spoolErr, sd, now)
	v.Reason = reason(v.State, sd, p)

	// Derivation of what is wrong belongs to internal/health, which already
	// separates "never delivered" from "delivery stopped" from "capture is
	// blocked" and is the same derivation the fleet view uses. Re-implementing a
	// second, quieter version here is how a machine ends up healthy in one
	// window and broken in another.
	rep := health.Build(health.Inputs{
		AgentVersion: Version,
		// For a command that lives for milliseconds, process uptime says nothing.
		// The clock that makes "has never delivered" meaningful is how long this
		// machine has been enrolled.
		StartedAt:   cfg.InstalledAt,
		Spool:       st,
		SpoolErr:    spoolErr,
		LastSuccess: sd.LastSuccess,
		LastError:   currentFailure(sd),
		Paused:      cfg.Paused,
		PausedSince: cfg.PausedSince,
		SpoolDir:    p.SpoolDir(),
		DiskFree:    spool.DiskFree,
		Now:         func() time.Time { return now },
		Thresholds:  health.Thresholds{DiskGuard: guard, SpoolMaxBytes: cfg.SpoolMaxBytes},
	})
	v.Problems = append(v.Problems, problems(rep, st)...)
	return v
}

// verdict decides which of the five situations this machine is in.
//
// The ordering matters: everything below the first branch assumes the numbers
// are real, and everything below the second assumes something was captured at
// all. Both assumptions have been silently wrong on a real laptop.
func verdict(st spool.Stats, spoolErr error, sd spool.State, now time.Time) (deliveryState, string) {
	if spoolErr != nil {
		return stateUnknown, "unknown: the outbox could not be read, so nothing here can be trusted"
	}

	// Evidence that capture has ever produced anything. Drops count: data that
	// was captured and then discarded is still proof the hooks fired, and a
	// machine losing events must not be described as one that captured none.
	captured := st.Pending > 0 || st.Quarantine > 0 || sd.Delivered() || total(st.Dropped) > 0
	if !captured {
		return stateNothingCaptured,
			"nothing has been captured yet, so there is nothing to upload"
	}

	if st.Pending == 0 {
		switch {
		case st.Quarantine > 0 && !sd.Delivered():
			return stateNotUploading, fmt.Sprintf(
				"NOT UPLOADING: nothing has ever been uploaded from this machine, and %d item(s) have given up retrying",
				st.Quarantine)
		case st.Quarantine > 0:
			return stateNotUploading, fmt.Sprintf(
				"%d item(s) have given up retrying and will not upload without an operator",
				st.Quarantine)
		case !sd.Delivered():
			// Captured, nothing queued, never delivered: everything that was
			// captured was thrown away before it could ship.
			return stateNotUploading,
				"NOT UPLOADING: nothing has ever been uploaded from this machine, and the queue is empty because data was dropped"
		default:
			return stateUpToDate, fmt.Sprintf(
				"everything captured has been uploaded (last upload %s ago)", humanAge(now.Sub(sd.LastSuccess)))
		}
	}

	// A queue older than the point at which delivery is considered dead is not
	// waiting, whatever else may have succeeded in the meantime. The threshold is
	// health's, so this sentence and the fleet's alarm fire together.
	if st.OldestAge > health.DefaultThresholds().DeliveryStale {
		if !sd.Delivered() {
			return stateNotUploading, fmt.Sprintf(
				"NOT UPLOADING: %d item(s) queued, the oldest for %s, and nothing has ever been uploaded from this machine",
				st.Pending, humanAge(st.OldestAge))
		}
		return stateNotUploading, fmt.Sprintf(
			"NOT UPLOADING: %d item(s) queued, the oldest for %s, and the last successful upload was %s ago",
			st.Pending, humanAge(st.OldestAge), humanAge(now.Sub(sd.LastSuccess)))
	}
	return stateWaiting, fmt.Sprintf(
		"%d item(s) captured and waiting to upload (oldest queued %s ago)", st.Pending, humanAge(st.OldestAge))
}

// reason explains a failing queue, including when it cannot.
//
// "No error recorded" is printed rather than omitted on purpose. A blank line
// where a reason belongs reads as "no problem", and the whole point of this
// command is that an absence of information must never look like health.
func reason(state deliveryState, sd spool.State, p config.Paths) string {
	if msg, at, failing := sd.FailingSince(); failing {
		return fmt.Sprintf("%s (last attempt %s)", msg, at.Format(time.RFC1123))
	}
	if state != stateNotUploading {
		return ""
	}
	return "no reason was recorded by the uploader — see " + p.LogFile()
}

// currentFailure is the last delivery error if it is still the live one. A
// failure older than the last success describes an outage that is over, and
// reporting it would send somebody after a problem that no longer exists.
func currentFailure(sd spool.State) error {
	if msg, _, failing := sd.FailingSince(); failing {
		return errors.New(msg)
	}
	return nil
}

// problems renders health's conditions as sentences somebody can act on.
//
// The derivation is health's and the words are this command's: health's details
// are written for an operator reading a fleet dashboard, and the person running
// this may be neither. Anything without a translation falls through to health's
// own text rather than being dropped, so a condition added later is loud by
// default rather than silently invisible here.
func problems(rep health.Report, st spool.Stats) []string {
	var out []string
	for _, c := range rep.Conditions {
		switch c.Kind {
		case health.KindPaused:
			// Already stated as a first-class line, and not a fault.
			continue
		case health.KindDropsRecorded:
			// Rendered below from the counters, per reason, in plain language.
			continue
		case health.KindCaptureBlocked:
			out = append(out, "capture is off: the disk is nearly full, so events are being dropped as they happen")
		case health.KindNeverDelivered:
			out = append(out, "nothing has ever been uploaded from this machine")
		case health.KindBacklogStalled:
			out = append(out, "uploads have stopped and the queue is aging: "+c.Detail)
		case health.KindBacklogGrowing:
			out = append(out, "the queue is growing faster than it uploads: "+c.Detail)
		case health.KindQuarantineNonEmpty:
			out = append(out, fmt.Sprintf("%d item(s) are stuck and will not retry on their own; `loop-sessions doctor --replay-quarantine` shows why", st.Quarantine))
		case health.KindParkedNonEmpty:
			out = append(out, fmt.Sprintf("%d item(s) are parked after the server kept not deciding about them; they retry after the next upgrade and daily", st.Parked))
		case health.KindSpoolUnreadable:
			out = append(out, "the outbox could not be read, so nothing about the queue below is measured")
		case health.KindSpoolNearCap:
			out = append(out, "the outbox is nearly full; the oldest events will start being discarded")
		default:
			out = append(out, c.Detail)
		}
	}

	// Drops are reported from the counter rather than from health's condition so
	// the reason keeps its plain-language sentence. Sorted so repeated runs of
	// this command do not appear to change.
	for _, reason := range sortedReasons(st.Dropped) {
		n := st.Dropped[reason]
		// The counters in this map that are not losses are named once, in
		// internal/health, because the fleet's condition and ledger read the
		// same list. This used to be a case of its own here, which is how
		// the two came to disagree.
		if !health.IsLoss(reason) {
			continue
		}
		switch reason {
		case "disk_full":
			out = append(out, fmt.Sprintf("%d event(s) were dropped because the disk is nearly full", n))
		case "queue_overflow":
			out = append(out, fmt.Sprintf("%d oldest event(s) were dropped to stay within the disk budget", n))
		case "hook_abandoned":
			out = append(out, fmt.Sprintf("%d hook(s) were abandoned before their event was spooled; the transcript recovery pass re-derives them", n))
		default:
			out = append(out, fmt.Sprintf("%d event(s) dropped (%s)", n, reason))
		}
	}
	return out
}

func sortedReasons(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if v > 0 {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func total(m map[string]int) int {
	var n int
	for _, v := range m {
		n += v
	}
	return n
}

// whenever turns "no such moment" into an absence rather than into a date.
func whenever(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// humanAge writes a duration the way somebody says it out loud. time.Duration's
// own format ("20m0s", "1h0m0s") is precise and reads as machine output in a
// sentence meant for a person.
func humanAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// ---------------------------------------------------------------- pause

func runPause(args []string, pause bool) error {
	fs := flag.NewFlagSet("pause", flag.ExitOnError)
	forDur := fs.Duration("for", 0, "pause for a period, then resume automatically")
	if err := fs.Parse(args); err != nil {
		return err
	}
	p := config.Paths{}
	cfg, err := config.Load(p)
	if err != nil {
		return err
	}
	if pause {
		cfg = cfg.Pause(time.Now())
	} else {
		cfg = cfg.Resume()
	}
	if err := config.Save(p, cfg); err != nil {
		return err
	}
	if pause {
		if *forDur > 0 {
			fmt.Printf("Paused. Capture will not resume automatically yet; run `loop-sessions resume` when ready.\n")
		} else {
			fmt.Println("Paused. Nothing will be captured or uploaded until you run `loop-sessions resume`.")
		}
		fmt.Println("Your team's dashboard will show this machine as paused, not broken.")
	} else {
		fmt.Println("Resumed.")
	}
	return nil
}

// ---------------------------------------------------------------- doctor

// runDoctor exists because the most common support question for a background
// agent is "is this thing working", and the honest answer needs several facts
// the user cannot easily gather. It reports what it checked, not just a verdict.
func runDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	replay := fs.Bool("replay-quarantine", false, "post one quarantined item to the server on its own and print the verdict")
	redrive := fs.Bool("redrive", false, "move quarantined and parked items back into the queue")
	if err := fs.Parse(args); err != nil {
		return err
	}
	p := config.Paths{}
	fmt.Printf("loop-sessions %s on %s/%s\n\n", Version, runtime.GOOS, runtime.GOARCH)

	cfg, cfgErr := config.Load(p)
	switch {
	case errors.Is(cfgErr, config.ErrNotConfigured):
		fmt.Println("  [ ] not set up   -> run: loop-sessions install")
		return nil
	case cfgErr != nil:
		fmt.Printf("  [x] config       %v\n", cfgErr)
		return nil
	default:
		fmt.Printf("  [ok] config      %s\n", p.ConfigFile())
		fmt.Printf("  [ok] identity    %s\n", cfg.Email)
	}

	if cfg.Paused {
		fmt.Printf("  [--] capture     paused since %s (this is a choice, not a fault)\n",
			cfg.PausedSince.Format(time.RFC1123))
	} else {
		fmt.Printf("  [ok] capture     on\n")
	}

	// The same reading `status` renders, so the two commands cannot disagree
	// about this machine. Doctor adds the checks a person cannot run themselves;
	// it does not re-derive the verdict.
	v := collectStatus(p, time.Now())
	if v.SpoolError != "" {
		fmt.Printf("  [x] spool        %s\n", v.SpoolError)
	} else {
		fmt.Printf("  [ok] spool       %d pending, %d stuck, %s\n",
			v.Spool.Pending, v.Spool.Quarantine, humanBytes(v.Spool.Bytes))
	}
	fmt.Printf("  [%s] uploads     %s\n", uploadMark(v.State), v.Summary)
	if v.Reason != "" {
		fmt.Printf("                   %s\n", v.Reason)
	}

	findings := discovery.Run(discovery.Options{})
	sum := discovery.Summarize(findings, time.Now())
	fmt.Printf("  [ok] discovery   %d sessions across %d tool(s)\n", sum.Sessions, countFound(findings))

	if len(v.Problems) > 0 {
		fmt.Println("\nProblems found:")
		for _, s := range v.Problems {
			fmt.Printf("  - %s\n", s)
		}
	} else {
		fmt.Println("\nNo problems found.")
	}

	if *redrive {
		if err := doctorRedrive(p, cfg); err != nil {
			return err
		}
	}
	if *replay {
		if err := doctorReplay(p, cfg); err != nil {
			return err
		}
	}
	return nil
}

// doctorRedrive is the operator CTA after a server-side fix: everything the
// server refused or kept ignoring goes back in the queue.
func doctorRedrive(p config.Paths, cfg config.Config) error {
	sp, err := spool.Open(spool.Options{Dir: p.SpoolDir(), MaxBytes: cfg.SpoolMaxBytes, MinFreeRatio: cfg.MinFreeRatio})
	if err != nil {
		return err
	}
	q, err := sp.Redrive()
	if err != nil {
		return fmt.Errorf("doctor: redrive quarantine: %w", err)
	}
	pk, err := sp.RedriveParked()
	if err != nil {
		return fmt.Errorf("doctor: redrive parked: %w", err)
	}
	fmt.Printf("\nRedriven: %d quarantined and %d parked item(s) are back in the queue; the next session's daemon delivers them.\n", q, pk)
	logf("doctor: redrove %d quarantined and %d parked item(s)", q, pk)
	return nil
}

// doctorReplay posts one quarantined item, alone, and prints what the server
// says. It is the fastest way to learn why nine machines held quarantines:
// the reason is on the item now, but a server that has since been fixed may
// simply accept it.
func doctorReplay(p config.Paths, cfg config.Config) error {
	sp, err := spool.Open(spool.Options{Dir: p.SpoolDir(), MaxBytes: cfg.SpoolMaxBytes, MinFreeRatio: cfg.MinFreeRatio})
	if err != nil {
		return err
	}
	items, err := sp.Quarantined(1)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		fmt.Println("\nNothing is quarantined on this machine.")
		return nil
	}
	tr, err := newTransport(p, cfg)
	if err != nil {
		return err
	}
	it := items[0]
	fmt.Printf("\nReplaying quarantined item %s (session %s, %d bytes)...\n", it.Item.ID, it.Item.SessionID, len(it.Item.Payload))
	resp, err := tr.Send(context.Background(), items)
	if err != nil {
		fmt.Printf("  transport: %v\n", err)
		return nil
	}
	for _, id := range resp.Accepted {
		if id == it.Item.ID {
			if err := sp.Ack(it); err != nil {
				return err
			}
			fmt.Println("  verdict: ACCEPTED; the item left quarantine. Run `loop-sessions doctor --redrive` to retry the rest.")
			return nil
		}
	}
	for _, r := range resp.Rejected {
		if r.ID == it.Item.ID {
			fmt.Printf("  verdict: REJECTED: %s\n", r.Reason)
			return nil
		}
	}
	fmt.Println("  verdict: UNDECIDED; the server named the item in neither list, which is a server-side bug (see the ingest runbook)")
	return nil
}

// uploadMark keeps doctor's checklist column honest: only a machine that has
// actually delivered gets the mark that reads as "fine".
func uploadMark(s deliveryState) string {
	switch s {
	case stateUpToDate:
		return "ok"
	case stateWaiting:
		return "--"
	default:
		return "x"
	}
}

func countFound(fs []discovery.Finding) int {
	var n int
	for _, f := range fs {
		if f.State == discovery.Found {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------- helpers

// logf appends to the agent log. It never writes to stdout or stderr, because
// the hook path shares this process and anything printed there would land in
// the user's terminal mid-session.
func logf(format string, args ...any) {
	p := config.Paths{}
	path := p.LogFile()
	if err := os.MkdirAll(strings.TrimSuffix(path, "/agent.log"), 0o700); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s %s\n", time.Now().UTC().Format(time.RFC3339), fmt.Sprintf(format, args...))
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
