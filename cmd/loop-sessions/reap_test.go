package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/daemon"
)

// reapStale keys on this binary's build and signals through the seam: a
// daemon of another build is stopped, one of this build is left alone.
func TestReapStaleStopsDaemonsOfOtherBuilds(t *testing.T) {
	home := t.TempDir()
	t.Setenv("LOOP_SESSIONS_HOME", home)
	prevVersion := Version
	Version = "test-build"
	defer func() { Version = prevVersion }()
	p := config.Paths{}
	if err := os.MkdirAll(p.StateDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	live := os.Getppid() // a pid that exists and is not this process
	write := func(id, build string) {
		b, _ := json.Marshal(daemon.State{SessionID: id, OwnerPID: 1, DaemonPID: live, Build: build, StartedAt: time.Now().Add(-time.Hour), Heartbeat: time.Now()})
		if err := os.WriteFile(filepath.Join(p.StateDir(), id+".json"), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("older", "old-build")
	write("unrecorded", "")
	write("mine", "test-build")
	var killed []int
	prev := killTerm
	killTerm = func(pid int) error { killed = append(killed, pid); return nil }
	defer func() { killTerm = prev }()
	if n := reapStale(p, "", time.Now()); n != 2 || len(killed) != 2 {
		t.Errorf("reaped %d, killed %v; want the two daemons of other builds", n, killed)
	}
	for _, id := range []string{"older", "unrecorded"} {
		if st, _ := daemon.LoadState(p.StateDir(), id); !st.Finalized || st.EndReason != "daemon_stale" {
			t.Errorf("session %s not finalized: %+v", id, st)
		}
	}
	if st, _ := daemon.LoadState(p.StateDir(), "mine"); st.Finalized {
		t.Error("a daemon of this build was reaped")
	}
}
