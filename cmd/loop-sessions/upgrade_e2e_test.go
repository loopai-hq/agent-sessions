package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
)

// TestAStaleAgentUpgradesItselfWhenAHookRuns is the test that proves the wiring
// rather than the package.
//
// internal/upgrade has thorough unit tests, and every one of them would still
// pass if nothing ever called it — which is exactly how six components in this
// repository came to exist, pass their own tests, and never run. So this drives
// the real binary: a real hook payload on stdin, the real daemon it spawns, a
// real release host, and an assertion on the bytes that end up on disk.
//
// The two builds differ only in their stamped version, which makes the assertion
// unambiguous. After the upgrade the file at the installed path must REPORT the
// new version, not merely differ from the old one: a test that only checked the
// checksum changed would pass on a corrupt download too.
func TestAStaleAgentUpgradesItselfWhenAHookRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two agents and runs one against a live release host")
	}

	const (
		installedVersion = "e2e-old"
		publishedVersion = "e2e-new"
	)

	home := t.TempDir()
	// Installed where the installer puts it, under the home directory, because
	// the ownership guard refuses to replace anything outside it.
	binDir := filepath.Join(home, ".local", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	installed := filepath.Join(binDir, "loop-sessions")
	buildAgentVersion(t, installed, installedVersion)

	// What the release host serves: the same program, stamped differently.
	published := filepath.Join(t.TempDir(), "loop-sessions")
	buildAgentVersion(t, published, publishedVersion)
	publishedBytes, err := os.ReadFile(published)
	if err != nil {
		t.Fatal(err)
	}
	publishedSum := sha256.Sum256(publishedBytes)
	publishedHex := hex.EncodeToString(publishedSum[:])
	asset := fmt.Sprintf("loop-sessions_%s_%s", runtime.GOOS, runtime.GOARCH)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/dl/latest/SHA256SUMS"):
			fmt.Fprintf(w, "%s  %s\n", publishedHex, asset)
		case strings.HasSuffix(r.URL.Path, "/dl/latest/"+asset):
			_, _ = w.Write(publishedBytes)
		default:
			// Ingest and health, answering 200 with a body that is deliberately
			// not a valid verdict. Delivery therefore fails and retries for the
			// life of this test, which is the point: upgrading must not depend
			// on uploads succeeding. A machine whose delivery is broken is
			// exactly the machine most likely to need a newer build, and an
			// upgrade path that ran only after a good upload could never reach
			// it.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"accepted":0}`))
		}
	}))
	defer srv.Close()

	writeInstalledConfig(t, home, srv.URL)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("agent log:\n%s", readAgentLogAt(t, home))
		}
	})

	// Sanity: before anything runs, the installed binary is the old one. Without
	// this the test could pass on a build that was never stale.
	if got := reportedVersion(t, installed); got != installedVersion {
		t.Fatalf("the installed agent reports %q before the hook, want %q", got, installedVersion)
	}

	payload := writePayload(t, capture.HookEvent{
		HookEventName:  "SessionStart",
		SessionID:      "e2e-upgrade",
		Cwd:            home,
		Source:         "startup",
		TranscriptPath: filepath.Join(home, "transcript.jsonl"),
	})
	// The first prompt is what starts the daemon; a session with none never
	// gets one.
	prompt := writePayload(t, capture.HookEvent{
		HookEventName: "UserPromptSubmit", SessionID: "e2e-upgrade", Cwd: home, Prompt: "hello",
		TranscriptPath: filepath.Join(home, "transcript.jsonl"),
	})

	// A stand-in harness, so the daemon has a process to shadow and does not
	// exit the moment it starts. The shell forks for the hook, which makes it
	// the hook's parent exactly as a real harness would be.
	owner := exec.Command("/bin/sh", "-c", fmt.Sprintf("%q hook < %q; %q hook < %q; sleep 120", installed, payload, installed, prompt))
	owner.Env = upgradeEnv(home)
	owner.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := owner.Start(); err != nil {
		t.Fatalf("start the stand-in harness: %v", err)
	}
	defer func() { _ = syscall.Kill(-owner.Process.Pid, syscall.SIGKILL) }()

	// The upgrade happens in the background, so this waits for the outcome
	// rather than assuming a duration.
	deadline := time.Now().Add(90 * time.Second)
	for sha256File(t, installed) != publishedHex {
		if time.Now().After(deadline) {
			t.Fatalf("the installed agent was never replaced; it is still %q\nlog:\n%s",
				reportedVersion(t, installed), readAgentLogAt(t, home))
		}
		time.Sleep(250 * time.Millisecond)
	}

	// The bytes match, and the file is something that runs and says who it is.
	if got := reportedVersion(t, installed); got != publishedVersion {
		t.Fatalf("after the upgrade the agent reports %q, want %q", got, publishedVersion)
	}

	// Nothing half-downloaded left in the directory people have on their PATH.
	entries, err := os.ReadDir(binDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".loop-sessions-") {
			t.Errorf("a staging file was left in the install directory: %s", e.Name())
		}
	}
}

// TestAPausedAgentIsNotUpgraded defends the promise pause makes.
//
// Pausing is how somebody says "leave this machine alone". An agent that honours
// that for capture and then quietly rewrites its own binary anyway has not
// honoured it at all, and it is the kind of thing discovered by somebody who
// paused precisely because they were debugging the build they were pinned to.
func TestAPausedAgentIsNotUpgraded(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two agents and runs one against a live release host")
	}

	home := t.TempDir()
	binDir := filepath.Join(home, ".local", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	installed := filepath.Join(binDir, "loop-sessions")
	buildAgentVersion(t, installed, "e2e-paused")
	before := sha256File(t, installed)

	published := filepath.Join(t.TempDir(), "loop-sessions")
	buildAgentVersion(t, published, "e2e-should-not-arrive")
	publishedBytes, err := os.ReadFile(published)
	if err != nil {
		t.Fatal(err)
	}
	pubSum := sha256.Sum256(publishedBytes)
	asset := fmt.Sprintf("loop-sessions_%s_%s", runtime.GOOS, runtime.GOARCH)

	var assetHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/dl/latest/SHA256SUMS"):
			fmt.Fprintf(w, "%s  %s\n", hex.EncodeToString(pubSum[:]), asset)
		case strings.HasSuffix(r.URL.Path, "/dl/latest/"+asset):
			assetHits++
			_, _ = w.Write(publishedBytes)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"accepted":0}`))
		}
	}))
	defer srv.Close()

	p := config.Paths{Home: home}
	cfg := config.Defaults()
	cfg.Email = "dev@example.com"
	cfg.DeviceID = "d-e2e"
	cfg.Endpoint = srv.URL
	cfg.Paused = true
	cfg.PausedSince = time.Now()
	if err := config.Save(p, cfg); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(p.Root(), "device.token"), "loops_v1_e2e")

	payload := writePayload(t, capture.HookEvent{
		HookEventName:  "SessionStart",
		SessionID:      "e2e-paused",
		Cwd:            home,
		Source:         "startup",
		TranscriptPath: filepath.Join(home, "transcript.jsonl"),
	})
	owner := exec.Command("/bin/sh", "-c", fmt.Sprintf("%q hook < %q; sleep 10", installed, payload))
	owner.Env = upgradeEnv(home)
	owner.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := owner.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-owner.Process.Pid, syscall.SIGKILL) }()

	// Long enough that an upgrade which was going to happen would have.
	time.Sleep(6 * time.Second)

	if got := sha256File(t, installed); got != before {
		t.Fatal("a paused agent replaced its own binary")
	}
	if assetHits != 0 {
		t.Errorf("a paused agent downloaded a new build %d time(s)", assetHits)
	}
}

// buildAgentVersion builds the agent to path with an explicit version stamp.
// The stamp is the whole point: it is how the test tells two otherwise identical
// builds apart, and it is what keeps the dev-build guard from skipping the
// upgrade under test.
func buildAgentVersion(t *testing.T, path, version string) {
	t.Helper()
	cmd := exec.Command("go", "build",
		"-ldflags", "-X main.Version="+version,
		"-o", path, "github.com/loopai-hq/agent-sessions/cmd/loop-sessions")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the agent at %s: %v\n%s", version, err, out)
	}
}

// upgradeEnv points the child at this test's agent home AND at a HOME of its
// own, because the ownership guard asks the operating system where home is and
// would otherwise find the real one.
func upgradeEnv(home string) []string {
	env := append(os.Environ(), "LOOP_SESSIONS_HOME="+config.Paths{Home: home}.Root())
	return append(env, "HOME="+home)
}

func reportedVersion(t *testing.T, bin string) string {
	t.Helper()
	out, err := exec.Command(bin, "version").Output()
	if err != nil {
		t.Fatalf("run %s version: %v", bin, err)
	}
	return strings.TrimSpace(string(out))
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// writeInstalledConfig is installedHome for a home directory the caller already
// made, which this test needs because the binary has to live inside it.
func writeInstalledConfig(t *testing.T, home, endpoint string) {
	t.Helper()
	p := config.Paths{Home: home}
	cfg := config.Defaults()
	cfg.Email = "dev@example.com"
	cfg.DeviceID = "d-e2e"
	cfg.Endpoint = endpoint
	if err := config.Save(p, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	writeFile(t, filepath.Join(p.Root(), "device.token"), "loops_v1_e2e")
}
