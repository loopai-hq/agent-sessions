package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/hooks"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// captureStdout runs f and returns what it printed.
func captureStdout(t *testing.T, f func() error) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	runErr := f()
	os.Stdout = saved
	_ = w.Close()
	out, _ := io.ReadAll(r)
	if runErr != nil {
		t.Fatalf("command returned an error: %v", runErr)
	}
	return string(out)
}

// The health report the daemon posts carries the version 2 fields from the
// config, the ledger, the upgrade check and the spool guard.
func TestHealthReportCarriesVersionTwoFields(t *testing.T) {
	srv := newFleetServer(t)
	defer srv.Close()
	home := hermeticHome(t, srv.URL)
	p := config.Paths{}
	withConfig(t, func(c *config.Config) { c.CaptureSchemaVersion = 4; c.Channel = "canary" })
	writeUpgradeStatus(p, health.UpgradeStatus{LastCheckAt: time.Now(), Result: "error", Error: "dial tcp: no such host"})
	ledger := capture.Ledger{Dir: p.StateDir()}
	_ = ledger.Captured("empty-1", capture.EntriesOf([]event.Event{{Type: event.SessionStarted}, {Type: event.SessionEnded}}))
	_ = ledger.Captured("real-1", capture.EntriesOf([]event.Event{{Type: event.SessionStarted}, {Type: event.UserPrompt}}))

	if err := mustReporter(t, home).Report(context.Background()); err != nil {
		t.Fatal(err)
	}
	rep := srv.reports()[0].report
	if rep.SchemaVersion != 2 {
		t.Errorf("schema_version = %d", rep.SchemaVersion)
	}
	if rep.CaptureSchemaVersion != 4 || rep.Channel != "canary" {
		t.Errorf("capture_schema_version=%d channel=%q", rep.CaptureSchemaVersion, rep.Channel)
	}
	if rep.Upgrade == nil || rep.Upgrade.Result != "error" {
		t.Errorf("upgrade = %+v", rep.Upgrade)
	}
	if rep.EmptyStarts24h != 1 {
		t.Errorf("empty_starts_24h = %d, want 1", rep.EmptyStarts24h)
	}
	if rep.Disk.ThresholdBytes == 0 {
		t.Error("the disk threshold was not reported")
	}
	if rep.AgentVersion == "" {
		t.Error("agent_version missing")
	}
	// A test binary carries no VCS stamp; the fields are absent, not wrong.
	t.Logf("commit=%q date=%q dirty=%v", rep.AgentCommit, rep.AgentBuildDate, rep.AgentDirty)
}

// Two daemons on one machine within five minutes send one report.
func TestFirstHealthReportIsRateLimitedPerMachine(t *testing.T) {
	srv := newFleetServer(t)
	defer srv.Close()
	home := hermeticHome(t, srv.URL)
	if err := mustReporter(t, home).Report(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mustReporter(t, home).Report(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(srv.reports()); n != 1 {
		t.Fatalf("server received %d reports from two daemons within the interval, want 1", n)
	}
	// Once the stamp is old enough the next daemon reports.
	if err := writeStamp(config.Paths{}, stampHealthReport, time.Now().Add(-healthMinInterval-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := mustReporter(t, home).Report(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(srv.reports()); n != 2 {
		t.Fatalf("server received %d reports after the interval, want 2", n)
	}
}

func TestDoctorRedriveReturnsStuckItemsToTheQueue(t *testing.T) {
	hermeticHome(t, "https://example.invalid")
	p := config.Paths{}
	sp, err := spool.Open(spool.Options{Dir: p.SpoolDir(), MaxUndecided: 1})
	if err != nil {
		t.Fatal(err)
	}
	_ = sp.Add(spool.Item{ID: "q", Kind: "event", Payload: json.RawMessage(`{}`)})
	_ = sp.Add(spool.Item{ID: "pk", Kind: "event", Payload: json.RawMessage(`{}`)})
	leased, _ := sp.Lease(2)
	for _, l := range leased {
		switch l.Item.ID {
		case "q":
			_ = sp.Quarantine(l, "event failed validation")
		case "pk":
			_, _ = sp.Undecided(l)
		}
	}
	if st, _ := sp.Stats(); st.Quarantine != 1 || st.Parked != 1 || st.Pending != 0 {
		t.Fatalf("fixture: %+v", st)
	}

	out := captureStdout(t, func() error { return runDoctor([]string{"--redrive"}) })
	if !strings.Contains(out, "Redriven: 1 quarantined and 1 parked") {
		t.Fatalf("doctor said:\n%s", out)
	}
	if st, _ := sp.Stats(); st.Quarantine != 0 || st.Parked != 0 || st.Pending != 2 {
		t.Fatalf("after redrive: %+v", st)
	}
}

func TestDoctorReplayPostsOneQuarantinedItemAndPrintsTheVerdict(t *testing.T) {
	cases := []struct {
		name    string
		verdict string
		want    string
		left    int
	}{
		{"accepted now", `{"accepted":["q"]}`, "ACCEPTED", 0},
		{"still rejected", `{"accepted":[],"rejected":[{"id":"q","reason":"payload too large"}]}`, "REJECTED: payload too large", 1},
		{"undecided", `{"accepted":[]}`, "UNDECIDED", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFleetServer(t)
			defer srv.Close()
			srv.verdict = tc.verdict
			hermeticHome(t, srv.URL)
			p := config.Paths{}
			sp, _ := spool.Open(spool.Options{Dir: p.SpoolDir()})
			_ = sp.Add(spool.Item{ID: "q", Kind: "event", Payload: json.RawMessage(`{"id":"q"}`)})
			leased, _ := sp.Lease(1)
			_ = sp.Quarantine(leased[0], "old reason")

			out := captureStdout(t, func() error { return runDoctor([]string{"--replay-quarantine"}) })
			if !strings.Contains(out, "Replaying quarantined item q") || !strings.Contains(out, tc.want) {
				t.Fatalf("doctor said:\n%s", out)
			}
			if st, _ := sp.Stats(); st.Quarantine != tc.left {
				t.Fatalf("quarantine = %d after replay, want %d", st.Quarantine, tc.left)
			}
		})
	}
}

// install --hooks-only puts the hooks back and touches nothing else: no
// sign-in, no discovery file, no import.
func TestInstallHooksOnlyRegistersHooksAndNothingElse(t *testing.T) {
	home := hermeticHome(t, "https://example.invalid")
	settings := filepath.Join(home, ".claude", "settings.json")
	if err := runInstall([]string{"--hooks-only"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(settings)
	if err != nil {
		t.Fatalf("hooks were not registered: %v", err)
	}
	if !strings.Contains(string(b), "#loop-sessions") {
		t.Fatalf("settings do not carry our hooks:\n%s", b)
	}
	if _, err := os.Stat(config.Paths{}.DiscoveryFile()); !os.IsNotExist(err) {
		t.Error("install --hooks-only wrote the discovery file")
	}
	// Idempotent: a second run changes nothing.
	if err := runInstall([]string{"--hooks-only"}); err != nil {
		t.Fatal(err)
	}
}

// daemon --upgrade-now says what it did; on a pinned machine it says so.
func TestDaemonUpgradeNowHonoursThePin(t *testing.T) {
	hermeticHome(t, "https://example.invalid")
	withConfig(t, func(c *config.Config) { c.DisableAutoUpgrade = true })
	out := captureStdout(t, func() error { return runDaemon([]string{"--upgrade-now"}) })
	if !strings.Contains(out, "switched off") {
		t.Fatalf("output:\n%s", out)
	}
}

// Pause means leave this machine alone, binary included, and the operator
// CTA is no exception: it says so instead of checking the release host.
func TestDaemonUpgradeNowHonoursAPause(t *testing.T) {
	hermeticHome(t, "https://example.invalid")
	withConfig(t, func(c *config.Config) { c.Paused = true })
	out := captureStdout(t, func() error { return runDaemon([]string{"--upgrade-now"}) })
	if !strings.Contains(out, "paused") || !strings.Contains(out, "loop-sessions resume") {
		t.Fatalf("output:\n%s", out)
	}
	if _, ok := readStamp(config.Paths{}, stampUpgradeCheck); ok {
		t.Fatal("a paused machine's --upgrade-now ran the check anyway")
	}
}

// The daemon brings the registered hooks up to its own defaults once per
// binary version. A machine whose SessionEnd entry an earlier release wrote
// with a shorter timeout converges without anyone running install
// --hooks-only; a later daemon of the same version leaves the file alone.
func TestDaemonRefreshesTheRegisteredHooksOncePerVersion(t *testing.T) {
	hermeticHome(t, "https://example.invalid")
	p := config.Paths{}
	dir := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", dir)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	settings := filepath.Join(dir, "settings.json")
	if _, err := hooks.Register(hooks.Options{SettingsPath: settings, Binary: self, Timeout: 10}); err != nil {
		t.Fatal(err)
	}

	refreshHooksIfDue(p)
	plan, err := hooks.Preview(hooks.Options{SettingsPath: settings, Binary: self})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Update) != 0 || len(plan.Add) != 0 {
		t.Fatalf("after the refresh the plan still wants %+v", plan)
	}
	if readText(p, stampHooksVer) != Version {
		t.Fatal("the refresh was not stamped with the binary version")
	}

	// Same version again: the entry is put back to 10 and stays there,
	// because the stamp says this binary has already had its turn.
	if _, err := hooks.Register(hooks.Options{SettingsPath: settings, Binary: self, Timeout: 10}); err != nil {
		t.Fatal(err)
	}
	refreshHooksIfDue(p)
	if plan, _ := hooks.Preview(hooks.Options{SettingsPath: settings, Binary: self}); len(plan.Update) != 1 {
		t.Fatalf("a second daemon of the same version rewrote the hooks: %+v", plan)
	}

	// A new binary version gets its turn.
	_ = writeText(p, stampHooksVer, "some-older-build")
	refreshHooksIfDue(p)
	if plan, _ := hooks.Preview(hooks.Options{SettingsPath: settings, Binary: self}); len(plan.Update) != 0 {
		t.Fatalf("a new version did not refresh the hooks: %+v", plan)
	}
}

// pause --for records a deadline, says so, and status shows it; when the
// deadline has passed the machine reads as capturing again without anyone
// running resume, and resume itself clears every pause field.
func TestPauseForEndsOnItsOwnAndSaysSo(t *testing.T) {
	hermeticHome(t, "https://example.invalid")
	before := time.Now()
	out := captureStdout(t, func() error { return runPause([]string{"--for", "2h"}, true) })
	if !strings.Contains(out, "Paused until") || !strings.Contains(out, "resumes on its own") {
		t.Fatalf("pause --for must say when it ends:\n%s", out)
	}
	cfg := mustConfig(t)
	if !cfg.Paused || cfg.PausedUntil.Before(before.Add(2*time.Hour)) || cfg.PausedUntil.After(time.Now().Add(2*time.Hour)) {
		t.Fatalf("config after pause --for 2h: %+v", cfg)
	}

	now := time.Now()
	v := collectStatus(config.Paths{}, now)
	if !v.Paused || v.PausedUntil == nil {
		t.Fatalf("status inside the window: %+v", v)
	}
	status := captureStdout(t, func() error { printStatus(v); return nil })
	if !strings.Contains(status, "PAUSED") || !strings.Contains(status, "until "+v.PausedUntil.Format(time.RFC1123)) {
		t.Fatalf("status must show the deadline:\n%s", status)
	}
	if later := collectStatus(config.Paths{}, now.Add(3*time.Hour)); later.Paused || later.PausedUntil != nil {
		t.Fatalf("status after the deadline still reads paused: %+v", later)
	}

	out = captureStdout(t, func() error { return runPause(nil, false) })
	if !strings.Contains(out, "Resumed.") {
		t.Fatalf("resume output:\n%s", out)
	}
	if cfg := mustConfig(t); cfg.Paused || !cfg.PausedSince.IsZero() || !cfg.PausedUntil.IsZero() {
		t.Fatalf("resume left pause fields behind: %+v", cfg)
	}
}

// A negative --for is refused rather than treated as "until resume".
func TestPauseForRefusesANegativeDuration(t *testing.T) {
	hermeticHome(t, "https://example.invalid")
	if err := runPause([]string{"--for", "-1h"}, true); err == nil {
		t.Fatal("pause --for -1h was accepted")
	}
	if cfg := mustConfig(t); cfg.Paused {
		t.Fatal("a refused pause still paused the machine")
	}
}
