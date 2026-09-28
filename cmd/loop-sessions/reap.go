package main

import (
	"os"
	"syscall"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/daemon"
)

// Stale daemons are stopped at every session start and at every daemon start.
//
// A daemon lives as long as the harness pid it watches, and that pid can
// outlive its session by weeks: an IDE or the desktop app hosts every session
// in one process, and a pid can be reused. On a real fleet the "behind"
// machines were all of that shape: daemons weeks old with no session, dozens
// on one laptop, each posting a health report every five
// minutes under the build it was born on and draining the shared spool with
// that build's code. The SessionEnd marker (daemon.MarkEnded) ends new ones
// with their session; this stops the ones already running, by the build
// recorded in their state (none, for the builds before this one): a daemon
// of another build is stopped, and a dead one under a live harness is
// finalized as it stands, whatever its build (the reconcile closes a dead
// daemon only once its harness is gone too). A live session gets a fresh
// daemon at its next prompt. The
// key is exact: a developer binary (build "dev") and a release build reap
// each other's daemons, which is right, since one machine runs one build.

// killTerm delivers the stop signal; a seam for tests.
var killTerm = func(pid int) error { return syscall.Kill(pid, syscall.SIGTERM) }

// reapStale finalizes the sessions whose daemon is not of this build,
// stopping the live ones, and reports how many. A daemon passes its own
// session id, which is skipped: it holds that session's lock itself.
func reapStale(p config.Paths, selfSession string, now time.Time) int {
	stale, err := daemon.StopStale(p.StateDir(), Version, os.Getpid(), selfSession, nil, nil, killTerm, now)
	if err != nil {
		logf("reap: %v", err)
		return 0
	}
	for _, s := range stale {
		build := s.Build
		if build == "" {
			build = "a build before builds were recorded"
		}
		if s.Killed {
			logf("reap: stopped daemon %d for session %s (%s); a live session starts a fresh one at its next prompt", s.DaemonPID, s.SessionID, build)
		} else {
			logf("reap: finalized session %s whose daemon %d (%s) had died under a live harness", s.SessionID, s.DaemonPID, build)
		}
	}
	return len(stale)
}
