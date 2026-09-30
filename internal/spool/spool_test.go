package spool

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// healthyDisk reports a filesystem with plenty of room. Tests inject it so the
// suite is deterministic: the real machine's free space is not a property of
// the code under test, and running on a nearly-full disk would otherwise make
// every write test fail for the wrong reason. The numbers are real byte
// counts because the guard has an absolute floor now: a toy "90 of 100" would
// be refused as 90 bytes free.
func healthyDisk(string) (free, total uint64, err error) { return 500 << 30, 1 << 40, nil }

func newSpool(t *testing.T, mut ...func(*Options)) *Spool {
	t.Helper()
	o := Options{Dir: t.TempDir(), DiskFree: healthyDisk}
	for _, m := range mut {
		m(&o)
	}
	s, err := Open(o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func item(payload string) Item {
	return Item{Kind: "event", SessionID: "s1", Payload: json.RawMessage(`{"p":"` + payload + `"}`)}
}

func TestAddThenLeaseRoundTrips(t *testing.T) {
	s := newSpool(t)
	if err := s.Add(item("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Lease(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("leased %d, want 1", len(got))
	}
	if got[0].Item.Kind != "event" || got[0].Item.SessionID != "s1" {
		t.Fatalf("round-trip lost fields: %+v", got[0].Item)
	}
	if got[0].Item.ID == "" {
		t.Fatal("ID should be minted when absent")
	}
	if got[0].Item.EventTime.IsZero() {
		t.Fatal("EventTime should default to now")
	}
}

// Data must survive until the server has it. Nothing but Ack may delete.
func TestAckDeletesAndNothingElseDoes(t *testing.T) {
	s := newSpool(t)
	_ = s.Add(item("a"))

	leased, _ := s.Lease(10)
	// Leasing alone must not remove anything: a drain that crashes mid-request
	// has to find the item still there on restart.
	again, _ := s.Lease(10)
	if len(again) != 1 {
		t.Fatal("lease must not consume the item")
	}
	if err := s.Ack(leased[0]); err != nil {
		t.Fatal(err)
	}
	after, _ := s.Lease(10)
	if len(after) != 0 {
		t.Fatalf("ack should have removed the item, %d remain", len(after))
	}
}

func TestAckIsIdempotent(t *testing.T) {
	s := newSpool(t)
	_ = s.Add(item("a"))
	leased, _ := s.Lease(10)
	if err := s.Ack(leased[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Ack(leased[0]); err != nil {
		t.Fatalf("second ack should be a no-op, got %v", err)
	}
}

func TestFailIncrementsAndQuarantinesAtCutoff(t *testing.T) {
	s := newSpool(t, func(o *Options) { o.MaxAttempts = 3 })
	_ = s.Add(item("poison"))

	for i := 1; i <= 3; i++ {
		leased, _ := s.Lease(1)
		if i < 3 && len(leased) != 1 {
			t.Fatalf("attempt %d: expected item still pending", i)
		}
		if len(leased) == 0 {
			break
		}
		if err := s.Fail(leased[0]); err != nil {
			t.Fatal(err)
		}
	}
	pending, _ := s.Lease(10)
	if len(pending) != 0 {
		t.Fatalf("poison item should be quarantined, %d still pending", len(pending))
	}
	st, _ := s.Stats()
	if st.Quarantine != 1 {
		t.Fatalf("quarantine = %d, want 1", st.Quarantine)
	}
	if st.Dropped["max_attempts"] != 1 {
		t.Fatalf("dropped counter not recorded: %v", st.Dropped)
	}
}

// One undeliverable item must not block the ones behind it.
func TestPoisonItemDoesNotBlockQueue(t *testing.T) {
	s := newSpool(t, func(o *Options) { o.MaxAttempts = 1 })
	_ = s.Add(item("poison"))
	time.Sleep(2 * time.Millisecond)
	_ = s.Add(item("good"))

	leased, _ := s.Lease(1)
	_ = s.Fail(leased[0]) // exhausts attempts, quarantines

	rest, _ := s.Lease(10)
	if len(rest) != 1 {
		t.Fatalf("expected the good item to remain, got %d", len(rest))
	}
	var p map[string]string
	_ = json.Unmarshal(rest[0].Item.Payload, &p)
	if p["p"] != "good" {
		t.Fatalf("wrong item survived: %v", p)
	}
}

func TestCorruptItemIsQuarantinedImmediately(t *testing.T) {
	s := newSpool(t)
	// A truncated write that somehow got published: unparseable forever, so
	// retrying it MaxAttempts times would be pure waste.
	bad := filepath.Join(s.opts.Dir, pendingDir, "0000000000001-bad.json")
	if err := os.WriteFile(bad, []byte(`{"kind":"event"`), 0o600); err != nil {
		t.Fatal(err)
	}
	leased, err := s.Lease(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 0 {
		t.Fatalf("corrupt item should not be leased, got %d", len(leased))
	}
	st, _ := s.Stats()
	if st.Quarantine != 1 || st.Dropped["corrupt"] != 1 {
		t.Fatalf("corrupt handling wrong: quarantine=%d dropped=%v", st.Quarantine, st.Dropped)
	}
}

func TestRedriveReturnsQuarantinedItems(t *testing.T) {
	s := newSpool(t, func(o *Options) { o.MaxAttempts = 1 })
	_ = s.Add(item("retryable"))
	leased, _ := s.Lease(1)
	_ = s.Fail(leased[0])

	n, err := s.Redrive()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("redrove %d, want 1", n)
	}
	back, _ := s.Lease(10)
	if len(back) != 1 {
		t.Fatalf("expected item back in pending, got %d", len(back))
	}
	if back[0].Item.Attempts != 0 {
		t.Fatalf("attempts = %d, want reset to 0", back[0].Item.Attempts)
	}
}

// Ordering matters: a session's events should reach the server adjacent and in
// roughly the order they happened.
func TestLeaseReturnsOldestFirst(t *testing.T) {
	s := newSpool(t)
	for _, p := range []string{"first", "second", "third"} {
		if err := s.Add(item(p)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	leased, _ := s.Lease(10)
	if len(leased) != 3 {
		t.Fatalf("leased %d, want 3", len(leased))
	}
	want := []string{"first", "second", "third"}
	for i, l := range leased {
		var p map[string]string
		_ = json.Unmarshal(l.Item.Payload, &p)
		if p["p"] != want[i] {
			t.Fatalf("position %d = %q, want %q", i, p["p"], want[i])
		}
	}
}

func TestLeaseRespectsLimit(t *testing.T) {
	s := newSpool(t)
	for range 5 {
		_ = s.Add(item("x"))
		time.Sleep(time.Millisecond)
	}
	leased, _ := s.Lease(2)
	if len(leased) != 2 {
		t.Fatalf("leased %d, want 2", len(leased))
	}
}

// Partial writes must never be visible. The temp-then-rename protocol is what
// makes a SIGKILL mid-write harmless.
func TestInFlightTempFilesAreNotLeased(t *testing.T) {
	s := newSpool(t)
	tmp := filepath.Join(s.opts.Dir, pendingDir, ".0000000000001-x.json.tmp")
	if err := os.WriteFile(tmp, []byte(`{"kind":"event"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	leased, _ := s.Lease(10)
	if len(leased) != 0 {
		t.Fatalf("temp file was leased: %d items", len(leased))
	}
}

// On a long-offline laptop the newest session is the valuable one.
func TestCapDropsOldestNotNewest(t *testing.T) {
	s := newSpool(t, func(o *Options) { o.MaxBytes = 400 })
	for _, p := range []string{"oldest", "middle", "newest"} {
		if err := s.Add(item(p)); err != nil {
			t.Fatalf("add %s: %v", p, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	leased, _ := s.Lease(10)
	if len(leased) == 0 {
		t.Fatal("cap dropped everything")
	}
	var last map[string]string
	_ = json.Unmarshal(leased[len(leased)-1].Item.Payload, &last)
	if last["p"] != "newest" {
		t.Fatalf("newest item was evicted; last = %q", last["p"])
	}
	st, _ := s.Stats()
	if st.Dropped["queue_overflow"] == 0 {
		t.Fatal("overflow drops must be counted, not silent")
	}
}

// Never be the reason a laptop wedges.
func TestRefusesToWriteWhenDiskNearlyFull(t *testing.T) {
	s := newSpool(t, func(o *Options) {
		o.MinFreeRatio = 0.10
		o.DiskFree = func(string) (uint64, uint64, error) { return 5, 100, nil } // 5% free
	})
	err := s.Add(item("x"))
	if err == nil {
		t.Fatal("expected refusal when disk is nearly full")
	}
	if !strings.Contains(err.Error(), "free disk") {
		t.Fatalf("unexpected error: %v", err)
	}
	st, _ := s.Stats()
	if st.Dropped["disk_full"] != 1 {
		t.Fatalf("disk_full drop not counted: %v", st.Dropped)
	}
}

// A stat failure must not stop capture: not knowing is not a reason to lose data.
func TestDiskStatFailureDoesNotBlockWrites(t *testing.T) {
	s := newSpool(t, func(o *Options) {
		o.DiskFree = func(string) (uint64, uint64, error) { return 0, 0, os.ErrPermission }
	})
	if err := s.Add(item("x")); err != nil {
		t.Fatalf("write should proceed when disk state is unknown: %v", err)
	}
}

// Event time is the thing that happened; it must survive untouched so a
// backfilled session is indistinguishable from a live one.
func TestEventTimeIsPreservedNotOverwritten(t *testing.T) {
	s := newSpool(t)
	historical := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	it := item("old")
	it.EventTime = historical
	if err := s.Add(it); err != nil {
		t.Fatal(err)
	}
	leased, _ := s.Lease(1)
	if !leased[0].Item.EventTime.Equal(historical) {
		t.Fatalf("event time = %v, want %v", leased[0].Item.EventTime, historical)
	}
}

// A caller-supplied idempotency key must survive, because the server dedups on
// it and a re-read after a crash has to produce the same key.
func TestCallerSuppliedIDIsPreserved(t *testing.T) {
	s := newSpool(t)
	it := item("x")
	it.ID = "sess1:chunk:0-4096"
	_ = s.Add(it)
	leased, _ := s.Lease(1)
	if leased[0].Item.ID != "sess1:chunk:0-4096" {
		t.Fatalf("ID = %q, want the deterministic key", leased[0].Item.ID)
	}
}

func TestStatsReportPendingBytesAndAge(t *testing.T) {
	now := time.Now()
	s := newSpool(t, func(o *Options) {
		o.Now = func() time.Time { return now }
	})
	_ = s.Add(item("a"))
	_ = s.Add(item("b"))
	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Pending != 2 {
		t.Fatalf("pending = %d, want 2", st.Pending)
	}
	if st.Bytes <= 0 {
		t.Fatal("bytes should be positive")
	}
	if st.OldestAge < 0 {
		t.Fatal("oldest age should not be negative")
	}
}

func TestConcurrentAddsAreAllDurable(t *testing.T) {
	s := newSpool(t)
	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_ = s.Add(item("concurrent"))
		}()
	}
	wg.Wait()
	leased, _ := s.Lease(0)
	if len(leased) != n {
		t.Fatalf("leased %d of %d concurrent writes", len(leased), n)
	}
}

func TestOpenRequiresDir(t *testing.T) {
	if _, err := Open(Options{}); err == nil {
		t.Fatal("expected an error when Dir is empty")
	}
}

func TestReopenSeesExistingItems(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(Options{Dir: dir, DiskFree: healthyDisk})
	if err != nil {
		t.Fatal(err)
	}
	_ = s1.Add(item("survives"))

	// Simulates the agent restarting after a crash: pending work must still be
	// there, which is the entire durability claim.
	s2, err := Open(Options{Dir: dir, DiskFree: healthyDisk})
	if err != nil {
		t.Fatal(err)
	}
	leased, _ := s2.Lease(10)
	if len(leased) != 1 {
		t.Fatalf("after restart leased %d, want 1", len(leased))
	}
}

func TestNewIDIsUnique(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := NewID()
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

// ---------------------------------------------------------------------------
// Durable state
//
// Everything above this line can be answered by listing a directory. The tests
// below defend the facts that cannot be: they are events observed by a process
// that has since exited, and if they do not survive it, the only command anyone
// runs when something is wrong has nothing to report but a count.
// ---------------------------------------------------------------------------

// nearlyFullDisk is a filesystem the spool must refuse to write to.
func nearlyFullDisk(string) (free, total uint64, err error) { return 1, 100, nil }

func TestDropsSurviveTheProcessThatObservedThem(t *testing.T) {
	dir := t.TempDir()

	// The hook process: sees a full disk, discards an event, exits.
	hook, err := Open(Options{Dir: dir, DiskFree: nearlyFullDisk})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.Add(item("lost")); err == nil {
		t.Fatal("a nearly-full disk must refuse the write")
	}

	// The reporting process, minutes later, on a machine whose disk has since
	// been cleared. Without a durable count it reports a clean machine at the
	// exact moment capture is throwing data away.
	reporter, err := Open(Options{Dir: dir, DiskFree: healthyDisk})
	if err != nil {
		t.Fatal(err)
	}
	st, err := reporter.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Dropped["disk_full"] != 1 {
		t.Fatalf("drops did not outlive the process that recorded them: %v", st.Dropped)
	}
}

func TestAckIsWhatMakesDeliveryObservable(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, DiskFree: healthyDisk})
	if err != nil {
		t.Fatal(err)
	}
	if st, err := s.State(); err != nil || st.Delivered() {
		t.Fatalf("a spool that has delivered nothing must say so: %+v %v", st, err)
	}

	_ = s.Add(item("shipped"))
	leased, err := s.Lease(1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v %d", err, len(leased))
	}
	if err := s.Ack(leased[0]); err != nil {
		t.Fatal(err)
	}

	// A different process over the same directory: this is how `status` sees it.
	other, err := Open(Options{Dir: dir, DiskFree: healthyDisk})
	if err != nil {
		t.Fatal(err)
	}
	st, err := other.State()
	if err != nil {
		t.Fatal(err)
	}
	if !st.Delivered() {
		t.Fatal("acking an item must durably record that this machine has delivered")
	}
}

func TestRecordedFailureIsCurrentOnlyUntilTheNextSuccess(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 8, 4, 18, 30, 0, 0, time.UTC)
	now := base
	s, err := Open(Options{Dir: dir, DiskFree: healthyDisk, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}

	s.RecordDeliveryFailure(errors.New("delivery: rejected this device's credential (http 401)"))
	st, err := s.State()
	if err != nil {
		t.Fatal(err)
	}
	msg, at, failing := st.FailingSince()
	if !failing || !strings.Contains(msg, "http 401") || !at.Equal(base) {
		t.Fatalf("a recorded failure must read as the current one: %q %v %v", msg, at, failing)
	}

	// The outage ends. A failure older than the last success describes a problem
	// that is over, and reporting it would send somebody after nothing.
	now = base.Add(time.Minute)
	_ = s.Add(item("after"))
	leased, _ := s.Lease(1)
	if err := s.Ack(leased[0]); err != nil {
		t.Fatal(err)
	}
	st, err = s.State()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, failing := st.FailingSince(); failing {
		t.Fatalf("a failure older than the last success must not read as current: %+v", st)
	}
}

func TestRecordedErrorCannotGrowWithoutBound(t *testing.T) {
	s := newSpool(t)
	s.RecordDeliveryFailure(errors.New(strings.Repeat("x", 4096)))
	st, err := s.State()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.LastError) > maxErrorBytes+3 {
		t.Fatalf("recorded error is %d bytes; a server answering with a page must not be able to do that",
			len(st.LastError))
	}
}

func TestUnreadableStateIsReportedRatherThanReadAsEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, DiskFree: healthyDisk})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFile), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.State(); err == nil {
		t.Fatal("a state file that cannot be parsed must be an error, not a zero value reading as \"never delivered\"")
	}
}
