package daemon

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestClaimSessionAdmitsExactlyOneHolderUnderConcurrency defends the property
// that several SessionStart hooks racing for one session produce one daemon.
//
// The failure it guards against is not theoretical for this product: a person
// with four editor windows open starts four sessions at once, and the naive
// gate — ask AlreadyRunning, then write a state file — lets every one of them
// through, after which four daemons lease the same spool files and upload every
// captured event four times.
func TestClaimSessionAdmitsExactlyOneHolderUnderConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sessions  []string
		claimers  int
		wantHeld  int
		wantAfter int // holders admitted once the first round releases
	}{
		{
			name:      "one session admits one holder however many race for it",
			sessions:  []string{"s-1"},
			claimers:  16,
			wantHeld:  1,
			wantAfter: 1,
		},
		{
			name:      "distinct sessions do not block each other",
			sessions:  []string{"s-1", "s-2", "s-3"},
			claimers:  8,
			wantHeld:  3,
			wantAfter: 3,
		},
		{
			name:      "a session id that sanitises to the same file is still one session",
			sessions:  []string{"a/b", "a:b"},
			claimers:  8,
			wantHeld:  1,
			wantAfter: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()

			held, release := claimRace(t, dir, tc.sessions, tc.claimers)
			if held != tc.wantHeld {
				t.Fatalf("concurrent claims admitted %d holder(s), want %d", held, tc.wantHeld)
			}

			// A released claim must become available again, or a daemon that
			// exits cleanly would lock its session out of ever being shadowed
			// again on this machine.
			release()
			after, releaseAfter := claimRace(t, dir, tc.sessions, tc.claimers)
			releaseAfter()
			if after != tc.wantAfter {
				t.Fatalf("after release, claims admitted %d holder(s), want %d", after, tc.wantAfter)
			}
		})
	}
}

// claimRace fires claimers goroutines at every session id at once and reports
// how many were admitted, along with a function that releases them.
func claimRace(t *testing.T, dir string, sessions []string, claimers int) (int, func()) {
	t.Helper()

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	var mu sync.Mutex
	var locks []*Lock

	for _, s := range sessions {
		for range claimers {
			done.Add(1)
			go func(sessionID string) {
				defer done.Done()
				start.Wait()
				lock, err := ClaimSession(dir, sessionID)
				switch {
				case errors.Is(err, ErrLocked):
					return
				case err != nil:
					t.Errorf("ClaimSession(%q): %v", sessionID, err)
					return
				}
				mu.Lock()
				locks = append(locks, lock)
				mu.Unlock()
			}(s)
		}
	}
	start.Done()
	done.Wait()

	return len(locks), func() {
		for _, l := range locks {
			if err := l.Close(); err != nil {
				t.Errorf("release: %v", err)
			}
		}
	}
}

// TestTryLockReportsErrLockedRatherThanBlocking defends the caller's ability to
// decide "somebody else is delivering, do nothing" in bounded time. A blocking
// lock here would stall a flush behind another daemon's whole HTTP request, and
// that flush may be the shutdown drain of a session that is already over.
func TestTryLockReportsErrLockedRatherThanBlocking(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "delivery.lock")

	held, err := TryLock(path)
	if err != nil {
		t.Fatalf("first TryLock: %v", err)
	}
	defer held.Close()

	returned := make(chan error, 1)
	go func() {
		second, err := TryLock(path)
		if second != nil {
			_ = second.Close()
		}
		returned <- err
	}()

	select {
	case err := <-returned:
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("second TryLock returned %v, want ErrLocked", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second TryLock blocked; it must report ErrLocked and return")
	}
}

// TestClaimSessionLockIsInvisibleToReconcile defends the state directory's
// contract: Reconcile and Sweep walk it looking for sessions, and a lock file
// they mistook for a half-written state file would be reported as an abandoned
// session on every start.
func TestClaimSessionLockIsInvisibleToReconcile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	lock, err := ClaimSession(dir, "s-1")
	if err != nil {
		t.Fatalf("ClaimSession: %v", err)
	}
	defer lock.Close()

	ab, err := Reconcile(dir, func(int) bool { return false }, time.Now(), 0)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(ab) != 0 {
		t.Fatalf("Reconcile reported %d abandoned session(s) from a lock file alone: %+v", len(ab), ab)
	}

	n, err := Sweep(dir, 0, time.Now())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n != 0 {
		t.Fatalf("Sweep removed %d file(s); a lock file is not a finished session", n)
	}
}
