package main

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/upgrade"
)

// A daemon follows an upgrade of its own binary by restarting in place.
//
// Before this change a replaced binary "took effect next session": the daemons
// already running kept the old code until their sessions ended, and a session
// can last days. On a machine with several sessions the oldest daemon drains
// the shared spool for all of them, so everything the daemon does on the way
// out of the spool (the Stop anchoring first of all) stayed on the old build
// for that long: one laptop uploaded anchorless copies for several sessions
// for hours after an upgrade, from one daemon started minutes before it. The
// fleet page saw the same as "running <old> alongside
// <new>".
//
// The watch is a stat of the executable every binaryWatchEvery; a changed
// size or mtime is followed by a digest, a digest that differs from the one
// taken at the daemon's first instruction is followed by a probe (the file
// must run its version verb, as an upgrade download must before it is
// renamed into place), and a build that runs ends the run (the daemon
// drains its backlog and leaves the session state unfinalized, as a signal
// would) and execs the binary at the same path with the same arguments. The
// pid does not change, so the session's state file and the fleet's view of
// the daemon stay valid; every descriptor is close-on-exec, so the session
// lock is released by the exec itself and the new image claims it afresh
// and rewrites the state with its own start time. Work the other goroutines
// had in flight is cut: the history rewalk starts over in the new image (it
// records its version only on success), and a repair list cut mid-walk
// waits for the next day's walk (the stamp is written before the walk).
// The daemon's flag set is append-only across builds, since the exec hands
// the old image's arguments to the new one.

// binaryWatchEvery is how often the daemon stats its own binary. The upgrade
// path writes a temp file and renames it into place, so the path never names
// a half-written binary; a copy made in place by hand can, which is what the
// probe below is for.
const binaryWatchEvery = 5 * time.Second

// execSelf replaces the process image. A seam so tests can see the call
// without becoming the new process.
var execSelf = syscall.Exec

// probeBinary runs a candidate once before the daemon commits to it. A seam
// so tests can stand in for a build that does not run yet.
var probeBinary = upgrade.Probe

// probeRetryEvery is how often a file that does not run is tried again while
// nothing about it changes (a copy without the execute bit that is chmod'd
// later changes no size or mtime). A seam so tests need not wait.
var probeRetryEvery = time.Minute

// binaryChanged reports whether the file at path now holds a different build
// from start that runs, and, when it holds one that does not, the probe's
// error. A stat error (the file is gone, an uninstall) and a same-digest
// rewrite (the same build copied over itself) are neither.
func binaryChanged(ctx context.Context, path, start string) (changed bool, fi os.FileInfo, refused error) {
	fi, err := os.Stat(path)
	if err != nil {
		return false, nil, nil
	}
	sum, err := upgrade.Digest(path)
	if err != nil {
		// A file the daemon cannot read is treated like one that does not
		// run: remembered, logged once, tried again on the minute, so a
		// chmod that restores read permission is picked up.
		return false, fi, err
	}
	if sum == start {
		return false, fi, nil
	}
	if err := probeBinary(ctx, path); err != nil {
		return false, fi, err
	}
	return true, fi, nil
}

func sameFile(a, b os.FileInfo) bool {
	return a != nil && b != nil && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

// watchBinary calls changed once, then returns, when the binary at path is
// replaced by a different build that runs; it returns without calling it
// when ctx ends first. The first tick always reads the file (nothing is
// remembered from before the watch), so a replacement that landed between
// the start digest and the watch is not missed. A file that does not run is
// logged once, then tried again only when its size or mtime changes or
// every probeRetryEvery, so a broken file costs a digest and a fork a
// minute rather than every tick.
func watchBinary(ctx context.Context, path, start string, every time.Duration, changed func()) {
	var (
		last      os.FileInfo
		refused   bool
		lastProbe time.Time
	)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if fi, err := os.Stat(path); err == nil && sameFile(fi, last) {
				if !refused || time.Since(lastProbe) < probeRetryEvery {
					continue
				}
			}
			yes, fi, err := binaryChanged(ctx, path, start)
			if fi != nil {
				last = fi
			}
			if err != nil {
				if !refused {
					logf("daemon: the file at %s changed but cannot be read or run (%v); staying on the current build until it can", path, err)
				}
				refused = true
				lastProbe = time.Now()
				continue
			}
			if !yes {
				refused = false
				continue
			}
			if refused {
				logf("daemon: the file at %s runs now; restarting", path)
			}
			changed()
			return
		}
	}
}

// restartInPlace execs the binary at self with this process's own arguments
// and environment. It returns only when the exec failed.
func restartInPlace(self string) error {
	return execSelf(self, os.Args, os.Environ())
}
