package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/spool"
	"strings"
)

// A SessionEnd hook leaves the marker that ends the session's daemon; a
// later prompt for the same id (a resume) clears it before the spawn.
func TestSessionEndLeavesTheMarkerAndAPromptClearsIt(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	var spawned int
	restore := stubSpawn(func(config.Paths, capture.HookEvent) error { spawned++; return nil })
	defer restore()
	marker := filepath.Join(config.Paths{}.StateDir(), "s-end.json.ended")

	feedHook(t, capture.HookEvent{HookEventName: "SessionEnd", SessionID: "s-end", Cwd: home, Reason: "other"})
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("SessionEnd left no marker at %s: %v", marker, err)
	}
	feedHook(t, capture.HookEvent{HookEventName: "UserPromptSubmit", SessionID: "s-end", Cwd: home, Prompt: "again"})
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("the prompt did not clear the marker before its spawn (err %v)", err)
	}
	if spawned != 1 {
		t.Errorf("spawned %d daemon(s), want the prompt's one", spawned)
	}
}

// TestSessionEndMarksEvenWhenTheSessionIsNotCaptured is the hole an
// adversarial review named. The marker used to be written inside
// capture(), past the capture decision, so a session whose capture was
// paused or whose cwd stopped being captured between its first prompt and
// its end left none. Its daemon then waited on the owner pid, which on a
// machine hosting many sessions in one process (an IDE, the desktop app) it
// did for weeks: the zombie shape PRs #66 and #67 removed, leaking back
// through the one path that skips capture. The marker says the harness
// reported the session over, which is true whether or not this machine
// captures it.
func TestSessionEndMarksEvenWhenTheSessionIsNotCaptured(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	restore := stubSpawn(func(config.Paths, capture.HookEvent) error {
		t.Error("a skipped session started a daemon")
		return nil
	})
	defer restore()

	cfg, err := config.Load(config.Paths{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Paused = true
	if err := config.Save(config.Paths{}, cfg); err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(config.Paths{}.StateDir(), "s-paused.json.ended")
	feedHook(t, capture.HookEvent{HookEventName: "SessionEnd", SessionID: "s-paused", Cwd: home, Reason: "other"})
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("a paused session's end left no marker at %s: %v", marker, err)
	}
	// And nothing was captured: the marker is not a way in for a session the
	// config excludes.
	if entries, err := os.ReadDir(config.Paths{}.SpoolDir()); err == nil {
		for _, e := range entries {
			if !e.IsDir() && e.Name() != ".gitignore" {
				t.Errorf("a paused session wrote %s to the spool", e.Name())
			}
		}
	}
}

// TestAnUnconfiguredMachineIsLeftAlone: the marker is written once the
// config has loaded, so a machine part way through an install writes
// nothing at all.
func TestAnUnconfiguredMachineIsLeftAlone(t *testing.T) {
	home := t.TempDir()
	t.Setenv("LOOP_SESSIONS_HOME", home)
	restore := stubSpawn(func(config.Paths, capture.HookEvent) error {
		t.Error("an unconfigured machine started a daemon")
		return nil
	})
	defer restore()

	feedHook(t, capture.HookEvent{HookEventName: "SessionEnd", SessionID: "s-new", Cwd: home, Reason: "other"})
	if _, err := os.Stat(filepath.Join(config.Paths{}.StateDir(), "s-new.json.ended")); !os.IsNotExist(err) {
		t.Errorf("an unconfigured machine wrote a marker (err %v)", err)
	}
}

// TestTheDoctorAndTheFleetAgreeOnWhatADropIs: the doctor's advice and the
// health report's drops_recorded condition read the same list, because they
// used to carry two and disagreed. The doctor had it right.
func TestTheDoctorAndTheFleetAgreeOnWhatADropIs(t *testing.T) {
	st := spool.Stats{Dropped: map[string]int{
		"recovered_from_transcript": 40,
		"parked":                    2,
		"tmp_swept":                 1,
		"disk_full":                 3,
	}}
	lines := strings.Join(problems(health.Report{}, st), "\n")
	if !strings.Contains(lines, "disk is nearly full") {
		t.Errorf("the doctor does not report the one real loss:\n%s", lines)
	}
	for _, notALoss := range []string{"recovered_from_transcript", "parked", "tmp_swept"} {
		if strings.Contains(lines, notALoss) {
			t.Errorf("the doctor reports %q as a drop:\n%s", notALoss, lines)
		}
	}
	if !health.IsLoss("disk_full") {
		t.Error("the shared list calls disk_full something other than a loss")
	}
}
