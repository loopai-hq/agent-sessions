package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// TestAnInstalledAgentReportsItsOwnHealthWithNobodyDoingAnything is the test
// that would have caught the empty fleet page.
//
// internal/health had a hundred tests and server/ingest had a route, and the
// health_reports table had no rows in it because no process on any laptop ever
// called either. Nothing short of the real binary, started the way a harness
// starts it, proves that gap is closed: this builds the agent, feeds it a real
// SessionStart payload, and waits for a report to arrive at a server that
// records what it received.
func TestAnInstalledAgentReportsItsOwnHealthWithNobodyDoingAnything(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the agent and runs it against a live server")
	}

	srv := newFleetServer(t)
	defer srv.Close()

	home := startedSession(t, srv, "e2e-health")

	waitFor(t, 30*time.Second, "a health report to reach the server", func() error {
		if len(srv.reports()) == 0 {
			return errors.New("no report yet")
		}
		return nil
	})

	got := srv.reports()[0]
	if got.auth != "Bearer loops_v1_e2e" {
		t.Errorf("Authorization = %q, want the device credential the events carry", got.auth)
	}
	if got.report.SchemaVersion != health.SchemaVersion {
		t.Errorf("schema_version = %d, want %d", got.report.SchemaVersion, health.SchemaVersion)
	}
	if got.report.EmittedAt.IsZero() || time.Since(got.report.EmittedAt) > time.Minute {
		t.Errorf("emitted_at = %v; coverage is measured from it", got.report.EmittedAt)
	}
	if got.report.Hostname == "" || got.report.OS == "" || got.report.AgentVersion == "" {
		t.Errorf("report does not identify the machine or the build: %+v", got.report)
	}
	if got.report.Disk.TotalBytes == 0 {
		t.Error("the agent reported no disk measurement, so capture_blocked cannot be evaluated")
	}
	if log := readAgentLogAt(t, home); strings.Contains(log, "health:") {
		t.Errorf("the agent logged a health failure against a server that accepted the report:\n%s", log)
	}
}

// TestAHealthEndpointReturning500LosesNoCapturedEvent is the isolation rule
// proven end to end, in the process tree it actually runs in.
//
// Telemetry about the pipeline must not be able to break the pipeline. The
// server here accepts every event and refuses every report, which is exactly
// what a bad deploy of the health route looks like from a laptop.
func TestAHealthEndpointReturning500LosesNoCapturedEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the agent and runs it against a live server")
	}

	srv := newFleetServer(t)
	defer srv.Close()
	srv.setHealthStatus(http.StatusInternalServerError)

	home := startedSession(t, srv, "e2e-health-500", "captured-1", "captured-2", "captured-3")

	waitFor(t, 30*time.Second, "the agent to try to report its health", func() error {
		if len(srv.reports()) == 0 {
			return errors.New("no report attempted yet")
		}
		return nil
	})
	waitFor(t, 30*time.Second, "every captured event to reach the server", func() error {
		var missing []string
		for _, id := range []string{"captured-1", "captured-2", "captured-3"} {
			if srv.deliveries(id) == 0 {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("not delivered: %s", strings.Join(missing, ", "))
		}
		return nil
	})

	// Nothing was sent twice to work around the failing report, and nothing was
	// given up on because of it.
	for _, id := range []string{"captured-1", "captured-2", "captured-3"} {
		if n := srv.deliveries(id); n != 1 {
			t.Errorf("%s was delivered %d times, want exactly once", id, n)
		}
	}
	waitFor(t, 30*time.Second, "the session's own events to leave the queue", func() error {
		if n := pendingAt(t, home); n != 0 {
			return fmt.Errorf("%d captured event(s) still queued while only the health route was down", n)
		}
		return nil
	})
	if n := quarantinedAt(t, home); n != 0 {
		t.Errorf("%d event(s) were quarantined by a failing health endpoint", n)
	}

	// The agent said so, in the one place a person can read, and said it about
	// health rather than about delivery.
	log := readAgentLogAt(t, home)
	if !strings.Contains(log, "health: this machine's report was not sent") {
		t.Errorf("the agent log does not record the failed report:\n%s", log)
	}
	if !strings.Contains(log, "drain: uploaded") {
		t.Errorf("the agent log does not record the uploads that succeeded anyway:\n%s", log)
	}

	// And the verdict the user gets is about the machine they have, not about
	// the endpoint that describes it.
	if v := statusOf(t, home); v.State != stateUpToDate {
		t.Errorf("`status` says %q (%s) on a laptop that uploaded everything", v.State, v.Summary)
	}
}

// ---------------------------------------------------------------- fixtures

// startedSession installs an agent into a private home, seeds any backlog, and
// fires one real SessionStart hook from a stand-in harness — the only path on
// which anything starts a daemon.
func startedSession(t *testing.T, srv *fleetServer, sessionID string, backlog ...string) string {
	t.Helper()

	bin := buildAgent(t)
	home := installedHome(t, srv.URL)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("agent log:\n%s", readAgentLogAt(t, home))
		}
	})
	if len(backlog) > 0 {
		seedSpoolAt(t, home, backlog...)
	}

	payload := writePayload(t, capture.HookEvent{
		HookEventName:  "SessionStart",
		SessionID:      sessionID,
		Cwd:            home,
		Source:         "startup",
		TranscriptPath: filepath.Join(home, "transcript.jsonl"),
	})
	// The first prompt is what starts the daemon; a session with none never
	// gets one.
	prompt := writePayload(t, capture.HookEvent{
		HookEventName: "UserPromptSubmit", SessionID: sessionID, Cwd: home, Prompt: "hello",
		TranscriptPath: filepath.Join(home, "transcript.jsonl"),
	})

	// A stand-in harness: the shell runs several commands, so it forks for the
	// hooks rather than exec'ing them and the daemon has a live parent to shadow.
	owner := exec.Command("/bin/sh", "-c", fmt.Sprintf("%q hook < %q; %q hook < %q; sleep 120", bin, payload, bin, prompt))
	owner.Env = fleetEnv(home)
	owner.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := owner.Start(); err != nil {
		t.Fatalf("start the stand-in harness: %v", err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-owner.Process.Pid, syscall.SIGKILL)
		_ = owner.Wait()
		if st, err := loadState(home, sessionID); err == nil && st.DaemonPID > 0 {
			// The daemon outlives its harness on purpose; a test that leaves one
			// behind leaves a process posting to a closed server.
			_ = syscall.Kill(st.DaemonPID, syscall.SIGKILL)
		}
	})
	return home
}

// fleetEnv points the agent at a private home for its own state AND for the
// machine it believes it is running on. Without the HOME override, discovery
// would walk the developer's real session tree on every report and the hook
// check would read their real harness settings, which makes the test both slow
// and dependent on the laptop running it.
func fleetEnv(home string) []string {
	return append(agentEnv(home), "HOME="+home, "CLAUDE_CONFIG_DIR="+filepath.Join(home, ".claude"))
}

func quarantinedAt(t *testing.T, home string) int {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: config.Paths{Home: home}.SpoolDir()})
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	st, err := sp.Stats()
	if err != nil {
		t.Fatalf("spool stats: %v", err)
	}
	return st.Quarantine
}

// statusOf runs the command a person runs, in the agent binary they have.
func statusOf(t *testing.T, home string) statusView {
	t.Helper()
	cmd := exec.Command(buildAgent(t), "status", "--json")
	cmd.Env = fleetEnv(home)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("status --json: %v", err)
	}
	var v statusView
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("status --json produced %q: %v", out, err)
	}
	return v
}
