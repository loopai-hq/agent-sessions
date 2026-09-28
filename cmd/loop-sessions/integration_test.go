package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/daemon"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// TestAnInstalledAgentDrainsItsSpoolWithNobodyDoingAnything is the test that
// would have caught the defect.
//
// Every piece of this pipeline had unit tests and passed them. What nothing
// asserted was the whole chain in one process tree: a real binary, a real hook
// payload on stdin, a real harness process to shadow, and a real server. So the
// chain ran in tests that stubbed the join and never once ran on a machine.
//
// The properties it defends, in order of how much they cost when they break:
// items already in the spool leave it without anybody typing a command; items
// captured later in the same session leave it too; the hook does not wait for any
// of that; and the daemon dies with the harness rather than accumulating one per
// session until the laptop has fifty.
func TestAnInstalledAgentDrainsItsSpoolWithNobodyDoingAnything(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the agent and runs it against a live server")
	}

	srv := newIngestServer(t)
	defer srv.Close()

	bin := buildAgent(t)
	home := installedHome(t, srv.URL)
	// The agent's only voice is this file, so a failure here is unreadable
	// without it.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("agent log:\n%s", readAgentLogAt(t, home))
		}
	})

	// The backlog a previous session left behind, which is the case that was
	// reported: ten files sitting in pending with nothing to move them.
	seedSpoolAt(t, home, "backlog-1", "backlog-2", "backlog-3")

	const sessionID = "e2e-session"
	payload := writePayload(t, capture.HookEvent{
		HookEventName:  "SessionStart",
		SessionID:      sessionID,
		Cwd:            home,
		Source:         "startup",
		TranscriptPath: filepath.Join(home, "transcript.jsonl"),
	})

	// A stand-in harness. The shell runs two commands so it forks for the hook
	// rather than exec'ing it, which makes the shell the hook's parent and gives
	// the test a process it can kill to play "the window was closed".
	owner := exec.Command("/bin/sh", "-c", fmt.Sprintf("%q hook < %q; sleep 120", bin, payload))
	owner.Env = agentEnv(home)
	owner.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := owner.Start(); err != nil {
		t.Fatalf("start the stand-in harness: %v", err)
	}
	killed := false
	defer func() {
		if !killed {
			_ = syscall.Kill(-owner.Process.Pid, syscall.SIGKILL)
			_ = owner.Wait()
		}
	}()

	// 1. The backlog leaves the machine. Nobody ran a command; a hook fired.
	waitFor(t, 30*time.Second, "the backlog to reach the server", func() error {
		return srv.missing("backlog-1", "backlog-2", "backlog-3")
	})

	// 2. And the daemon keeps delivering while the session runs, rather than
	// draining once at start and going quiet.
	seedSpoolAt(t, home, "midsession-1")
	waitFor(t, 30*time.Second, "a mid-session item to reach the server", func() error {
		return srv.missing("midsession-1")
	})

	// 3. The session's own SessionStart event went too, which is what proves the
	// hook and the daemon are looking at the same spool.
	waitFor(t, 30*time.Second, "the session's own captured event to reach the server", func() error {
		if srv.sessionCount(sessionID) == 0 {
			return errors.New("no event for this session yet")
		}
		return nil
	})

	// 4. The daemon shadows the harness rather than the hook that started it.
	state := readState(t, home, sessionID)
	if state.OwnerPID != owner.Process.Pid {
		t.Fatalf("daemon is watching pid %d; the harness is %d", state.OwnerPID, owner.Process.Pid)
	}
	if state.DaemonPID == owner.Process.Pid || !alive(state.DaemonPID) {
		t.Fatalf("daemon pid %d is not a live process of its own", state.DaemonPID)
	}
	if state.Finalized {
		t.Fatal("daemon finalised the session while the harness is still running")
	}

	// 5. Killing the harness ends the daemon. This is the SIGKILL case: no
	// SessionEnd fires, and the daemon has to notice by itself.
	if err := syscall.Kill(-owner.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill the stand-in harness: %v", err)
	}
	_ = owner.Wait() // reap, or the pid lingers as a zombie and still looks alive
	killed = true

	waitFor(t, 30*time.Second, "the daemon to exit with its harness", func() error {
		if alive(state.DaemonPID) {
			return fmt.Errorf("daemon %d is still running", state.DaemonPID)
		}
		return nil
	})

	final := readState(t, home, sessionID)
	if !final.Finalized || final.EndReason != "owner_exited" {
		t.Fatalf("session state after the harness died: finalized=%v reason=%q, want a session closed as owner_exited",
			final.Finalized, final.EndReason)
	}

	// 6. Nothing is left behind, and the log says what happened. A spool that
	// empties silently is only half the fix: the log is where somebody looks
	// when it does not.
	if n := pendingAt(t, home); n != 0 {
		t.Fatalf("%d item(s) still pending after the session ended", n)
	}
	log := readAgentLogAt(t, home)
	for _, want := range []string{"daemon: shadowing session", "drain: uploaded", "daemon: exiting after owner_exited"} {
		if !strings.Contains(log, want) {
			t.Errorf("agent log never mentions %q:\n%s", want, log)
		}
	}
}

// TestTheSessionStartHookReturnsWithoutWaitingForTheDaemon defends the user's
// turn. Hooks share a small budget, and a SessionStart that blocked while a
// daemon started, connected and uploaded a backlog would stall the first moments
// of every session — which is when a person is watching.
func TestTheSessionStartHookReturnsWithoutWaitingForTheDaemon(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the agent")
	}

	// A server that never answers. If the hook waited on delivery in any way,
	// this is where it would hang.
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-blocked }))
	defer func() { close(blocked); srv.Close() }()

	bin := buildAgent(t)
	home := installedHome(t, srv.URL)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("agent log:\n%s", readAgentLogAt(t, home))
		}
	})
	seedSpoolAt(t, home, "never-lands")

	const sessionID = "hook-latency"
	payload := writePayload(t, capture.HookEvent{
		HookEventName: "SessionStart",
		SessionID:     sessionID,
		Cwd:           home,
		Source:        "startup",
	})

	in, err := os.Open(payload)
	if err != nil {
		t.Fatalf("open payload: %v", err)
	}
	defer in.Close()

	hook := exec.Command(bin, "hook")
	hook.Env = agentEnv(home)
	hook.Stdin = in

	start := time.Now()
	if err := hook.Run(); err != nil {
		t.Fatalf("hook exited non-zero (%v); a hook must never fail the harness", err)
	}
	elapsed := time.Since(start)

	// The daemon this started is watching the test process, so it has to be shut
	// down explicitly or it outlives the test run.
	t.Cleanup(func() {
		if st, err := loadState(home, sessionID); err == nil && st.DaemonPID > 0 {
			_ = syscall.Kill(st.DaemonPID, syscall.SIGKILL)
		}
	})

	// The budget the hook enforces on its own work is two seconds; anything
	// approaching it means delivery leaked onto the hot path.
	if elapsed > 2*time.Second {
		t.Fatalf("SessionStart hook took %s against an unreachable server; it must not wait for delivery", elapsed)
	}

	waitFor(t, 20*time.Second, "the daemon to be running despite the hook having returned", func() error {
		st, err := loadState(home, sessionID)
		if err != nil {
			return err
		}
		if !alive(st.DaemonPID) {
			return fmt.Errorf("daemon %d is not running", st.DaemonPID)
		}
		return nil
	})
}

// ---------------------------------------------------------------- fixtures

// ingestServer is the ingest contract, reduced to what the client needs from it:
// per-item acceptance keyed by the idempotency id.
type ingestServer struct {
	*httptest.Server

	mu       sync.Mutex
	accepted map[string]bool
	sessions map[string]int
}

func newIngestServer(t *testing.T) *ingestServer {
	t.Helper()
	s := &ingestServer{accepted: map[string]bool{}, sessions: map[string]int{}}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != eventsPath {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		var items []spool.Item
		if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
			http.Error(w, "malformed", http.StatusBadRequest)
			return
		}
		out := struct {
			Accepted []string `json:"accepted"`
		}{Accepted: []string{}}

		s.mu.Lock()
		for _, it := range items {
			s.accepted[it.ID] = true
			s.sessions[it.SessionID]++
			out.Accepted = append(out.Accepted, it.ID)
		}
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	return s
}

// missing names what has not arrived, so a timeout says which item is stuck
// rather than only that something is.
func (s *ingestServer) missing(ids ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var absent []string
	for _, id := range ids {
		if !s.accepted[id] {
			absent = append(absent, id)
		}
	}
	if len(absent) > 0 {
		return fmt.Errorf("not delivered: %s", strings.Join(absent, ", "))
	}
	return nil
}

func (s *ingestServer) sessionCount(sessionID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[sessionID]
}

// buildAgent compiles the real binary. The test is about what an installed agent
// does, and `go run` or an in-process call would test something else.
func buildAgent(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "loop-sessions")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/loopai-hq/agent-sessions/cmd/loop-sessions")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the agent: %v\n%s", err, out)
	}
	return bin
}

// installedHome is what `loop-sessions install` leaves behind on a machine that
// signed in. The test does not run install itself because that opens a browser.
func installedHome(t *testing.T, endpoint string) string {
	t.Helper()
	home := t.TempDir()

	p := config.Paths{Home: home}
	cfg := config.Defaults()
	cfg.Email = "dev@example.com"
	cfg.DeviceID = "d-e2e"
	cfg.Endpoint = endpoint
	if err := config.Save(p, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	writeFile(t, filepath.Join(p.Root(), "device.token"), "loops_v1_e2e")
	return home
}

// agentEnv points a child process at this test's private agent home. The env
// var overrides the whole root rather than a home directory, so the two forms
// must be derived from one place or the test and the binary read different
// directories and every assertion becomes a timeout.
func agentEnv(home string) []string {
	return append(os.Environ(), "LOOP_SESSIONS_HOME="+config.Paths{Home: home}.Root())
}

func readAgentLogAt(t *testing.T, home string) string {
	t.Helper()
	b, err := os.ReadFile(config.Paths{Home: home}.LogFile())
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		t.Fatalf("read agent log: %v", err)
	}
	return string(b)
}

func writePayload(t *testing.T, h capture.HookEvent) string {
	t.Helper()
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal hook payload: %v", err)
	}
	path := filepath.Join(t.TempDir(), "payload.json")
	writeFile(t, path, string(b))
	return path
}

// seedSpoolAt writes items directly into a home's spool, standing in for hooks
// that fired earlier.
func seedSpoolAt(t *testing.T, home string, ids ...string) {
	t.Helper()
	p := config.Paths{Home: home}
	sp, err := spool.Open(spool.Options{Dir: p.SpoolDir()})
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	for _, id := range ids {
		item := spool.Item{
			ID:        id,
			Kind:      "event",
			SessionID: "seeded",
			Payload:   json.RawMessage(`{"id":"` + id + `","session_id":"seeded"}`),
		}
		if err := sp.Add(item); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
}

func pendingAt(t *testing.T, home string) int {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: config.Paths{Home: home}.SpoolDir()})
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	st, err := sp.Stats()
	if err != nil {
		t.Fatalf("spool stats: %v", err)
	}
	return st.Pending
}

func loadState(home, sessionID string) (daemon.State, error) {
	var st daemon.State
	dir := config.Paths{Home: home}.StateDir()
	b, err := os.ReadFile(filepath.Join(dir, sessionID+".json"))
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, err
	}
	return st, nil
}

func readState(t *testing.T, home, sessionID string) daemon.State {
	t.Helper()
	st, err := loadState(home, sessionID)
	if err != nil {
		t.Fatalf("read session state: %v", err)
	}
	return st
}

func alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

// waitFor polls until cond is satisfied, and reports the last reason it was not.
func waitFor(t *testing.T, limit time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(limit)
	var last error
	for time.Now().Before(deadline) {
		if last = cond(); last == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s: %v", limit, what, last)
}
