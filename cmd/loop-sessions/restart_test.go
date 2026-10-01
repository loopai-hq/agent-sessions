package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/upgrade"
)

// writeBinary lands content at path the way the upgrade does, by renaming a
// whole file into place, so a tick never sees a truncated file.
func writeBinary(t *testing.T, path, content string) string {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	sum, err := upgrade.Digest(path)
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

// A different build at the same path ends the watch with one call; the same
// bytes rewritten (a new mtime, the same digest) do not.
func passProbe(t *testing.T) {
	t.Helper()
	prev := probeBinary
	probeBinary = func(context.Context, string) error { return nil }
	t.Cleanup(func() { probeBinary = prev })
}

// retryProbesAtOnce makes a refused file be tried again on every tick, so
// a test need not wait a minute.
func retryProbesAtOnce(t *testing.T) {
	t.Helper()
	prev := probeRetryEvery
	probeRetryEvery = 0
	t.Cleanup(func() { probeRetryEvery = prev })
}

func TestWatchBinaryFiresOnceOnADifferentBuild(t *testing.T) {
	passProbe(t)
	path := filepath.Join(t.TempDir(), "loop-sessions")
	start := writeBinary(t, path, "build one")
	fired := make(chan struct{}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		watchBinary(ctx, path, start, 5*time.Millisecond, func() { fired <- struct{}{} })
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	writeBinary(t, path, "build one") // same bytes, new mtime
	time.Sleep(30 * time.Millisecond)
	select {
	case <-fired:
		t.Fatal("fired on a same-digest rewrite")
	default:
	}
	writeBinary(t, path, "build two, longer")
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("did not fire on a different build")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the watch did not return after firing")
	}
	if len(fired) != 0 {
		t.Error("fired more than once")
	}
}

// A binary that disappears (an uninstall) is not a change, and the watch
// returns quietly when the daemon's context ends.
func TestWatchBinaryIgnoresAMissingFileAndStopsWithTheContext(t *testing.T) {
	passProbe(t)
	path := filepath.Join(t.TempDir(), "loop-sessions")
	start := writeBinary(t, path, "build one")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		watchBinary(ctx, path, start, 5*time.Millisecond, func() { t.Error("fired on a missing file") })
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the watch did not stop with the context")
	}
}

// The restart execs this process's own path, arguments and environment.
func TestRestartInPlaceExecsSelfWithTheSameArguments(t *testing.T) {
	var got struct {
		path string
		args []string
		env  int
	}
	prev := execSelf
	execSelf = func(path string, args []string, env []string) error {
		got.path, got.args, got.env = path, args, len(env)
		return nil
	}
	defer func() { execSelf = prev }()
	if err := restartInPlace("/tmp/self"); err != nil {
		t.Fatal(err)
	}
	if got.path != "/tmp/self" || len(got.args) != len(os.Args) || got.args[0] != os.Args[0] || got.env == 0 {
		t.Errorf("exec(%q, %v, %d env) does not carry this process's arguments", got.path, got.args, got.env)
	}
}

// A different file that does not run (still being written, not a binary)
// is not a change: the watch keeps reading it until it runs, then fires
// once. A watch started after the replacement landed still notices it.
func TestWatchBinaryWaitsForABuildThatRuns(t *testing.T) {
	retryProbesAtOnce(t)
	path := filepath.Join(t.TempDir(), "loop-sessions")
	start := writeBinary(t, path, "build one")
	writeBinary(t, path, "build two, longer") // replaced before the watch begins
	var probes int32
	prev := probeBinary
	probeBinary = func(context.Context, string) error {
		if atomic.AddInt32(&probes, 1) < 3 {
			return errors.New("exec format error")
		}
		return nil
	}
	t.Cleanup(func() { probeBinary = prev })
	fired := make(chan struct{}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go watchBinary(ctx, path, start, 5*time.Millisecond, func() { fired <- struct{}{} })
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("did not fire once the build ran")
	}
	if n := atomic.LoadInt32(&probes); n != 3 {
		t.Errorf("probed %d times, want 3 (two refusals, then the run)", n)
	}
	time.Sleep(30 * time.Millisecond)
	if len(fired) != 0 {
		t.Error("fired more than once")
	}
}

// A file that does not run is probed once and then left alone until its
// size or mtime changes or the retry cadence elapses: with the cadence at a
// minute, three ticks cost one probe.
func TestWatchBinaryDoesNotReprobeAnUnchangedRefusedFileEveryTick(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loop-sessions")
	start := writeBinary(t, path, "build one")
	writeBinary(t, path, "build two, longer")
	var probes int32
	prev := probeBinary
	probeBinary = func(context.Context, string) error {
		atomic.AddInt32(&probes, 1)
		return errors.New("permission denied")
	}
	t.Cleanup(func() { probeBinary = prev })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		watchBinary(ctx, path, start, 5*time.Millisecond, func() { t.Error("fired on a file that does not run") })
		close(done)
	}()
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done
	if n := atomic.LoadInt32(&probes); n != 1 {
		t.Errorf("probed %d times over a dozen ticks, want 1", n)
	}
}
