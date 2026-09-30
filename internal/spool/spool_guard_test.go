package spool

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// The disk guard
//
// Every number here is a real disk somebody has. The old ratio-only guard
// refused writes on this machine at 46 GB free of 494 GB while Finder showed
// tens of gigabytes available, and 17,340 events were dropped for it.
// ---------------------------------------------------------------------------

const (
	gib = uint64(1) << 30
	mib = uint64(1) << 20
)

// A 500 GB disk with 10 GB free is 2% free. The old 5% ratio refused it; the
// only thing a 2 GiB spool can threaten on such a disk is nothing.
func TestGuardAllowsLargeDiskWithSmallRatio(t *testing.T) {
	ok, threshold := WriteAllowed(10*gib, 500*gib)
	if !ok {
		t.Fatalf("10 GiB free of 500 GiB was refused (threshold %d)", threshold)
	}
	if threshold != 4*gib {
		t.Fatalf("threshold = %d, want the 4 GiB cap on a 500 GiB disk", threshold)
	}
}

// Below the absolute floor nothing is written, however large the disk: the
// floor is what a full laptop needs to keep functioning, and the spool must
// never be the thing that takes it.
func TestGuardRefusesBelowAbsoluteFloor(t *testing.T) {
	if ok, _ := WriteAllowed(300*mib, 500*gib); ok {
		t.Fatal("300 MiB free was allowed; the 512 MiB floor must refuse it")
	}
}

// On a small disk 1% is less than the floor, so the floor is the threshold.
func TestGuardFloorDominatesOnSmallDisk(t *testing.T) {
	if ok, threshold := WriteAllowed(400*mib, 32*gib); ok || threshold != MinFreeBytes {
		t.Fatalf("400 MiB free of 32 GiB: ok=%v threshold=%d, want refused at the %d floor", ok, threshold, MinFreeBytes)
	}
	if ok, _ := WriteAllowed(600*mib, 32*gib); !ok {
		t.Fatal("600 MiB free of 32 GiB was refused; it is above the floor")
	}
}

// A 4 TB volume at 1% would demand 40 GiB free. The cap keeps the ratio from
// growing into a number nobody needs.
func TestGuardRatioIsCappedAtFourGiB(t *testing.T) {
	if ok, threshold := WriteAllowed(5*gib, 4096*gib); !ok || threshold != 4*gib {
		t.Fatalf("5 GiB free of 4 TiB: ok=%v threshold=%d, want allowed under the 4 GiB cap", ok, threshold)
	}
}

// The refusal happens at exactly the threshold byte, so a reporter that calls
// WriteAllowed with the same numbers flips at the same moment the spool does.
func TestGuardBoundaryIsExact(t *testing.T) {
	_, threshold := WriteAllowed(0, 200*gib)
	if ok, _ := WriteAllowed(threshold, 200*gib); !ok {
		t.Fatal("exactly the threshold must be allowed")
	}
	if ok, _ := WriteAllowed(threshold-1, 200*gib); ok {
		t.Fatal("one byte under the threshold must be refused")
	}
}

// The 0.05 every old config carries explicitly is the old default, not a
// choice, and is treated as unset. Any other explicit ratio is honoured, still
// floored and capped.
func TestLegacyRatioIsTreatedAsUnsetAndOthersAreHonoured(t *testing.T) {
	legacy := Guard{MinFreeRatio: LegacyMinFreeRatio}
	if got, want := legacy.Threshold(200*gib), DefaultGuard().Threshold(200*gib); got != want {
		t.Fatalf("legacy 0.05 produced threshold %d, want the default %d", got, want)
	}
	custom := Guard{MinFreeRatio: 0.10}
	if got := custom.Threshold(20 * gib); got != 2*gib {
		t.Fatalf("an explicit 10%% on 20 GiB gave %d, want 2 GiB", got)
	}
	if got := custom.Threshold(400 * gib); got != MaxRatioBytes {
		t.Fatalf("an explicit ratio escaped the cap: %d", got)
	}

	s := newSpool(t, func(o *Options) { o.MinFreeRatio = LegacyMinFreeRatio })
	if s.Guard() != DefaultGuard() {
		t.Fatalf("a spool opened with the legacy ratio runs %+v, want the default guard", s.Guard())
	}
}

// The spool's own refusal uses the same decision. 4 GiB free on a 500 GiB
// disk is fine; 300 MiB is not; and the drop is counted either way.
func TestSpoolRefusesWithTheSharedGuard(t *testing.T) {
	fine := newSpool(t, func(o *Options) {
		o.DiskFree = func(string) (uint64, uint64, error) { return 4 * gib, 500 * gib, nil }
	})
	if err := fine.Add(item("x")); err != nil {
		t.Fatalf("4 GiB free of 500 GiB refused: %v", err)
	}
	full := newSpool(t, func(o *Options) {
		o.DiskFree = func(string) (uint64, uint64, error) { return 300 * mib, 500 * gib, nil }
	})
	err := full.Add(item("x"))
	if err == nil || !strings.Contains(err.Error(), "free disk") {
		t.Fatalf("300 MiB free was not refused with a free-disk reason: %v", err)
	}
	if st, _ := full.Stats(); st.Dropped["disk_full"] != 1 {
		t.Fatalf("refusal not counted: %v", st.Dropped)
	}
}

// ---------------------------------------------------------------------------
// Abandoned temp files
// ---------------------------------------------------------------------------

// A hook killed between write and rename leaves ".<name>.tmp" behind, which
// Lease never returns. Old ones are swept; a fresh one may still be about to
// be renamed and is left alone.
func TestStaleTmpFilesAreSwept(t *testing.T) {
	now := time.Now()
	s := newSpool(t, func(o *Options) { o.Now = func() time.Time { return now } })
	dir := filepath.Join(s.opts.Dir, pendingDir)

	stale := filepath.Join(dir, ".0000000000001-stale.json.tmp")
	fresh := filepath.Join(dir, ".0000000000002-fresh.json.tmp")
	// The shapes a rewrite or a redrive leaves: per-process now, and the
	// undotted name an earlier release used, which a killed daemon of that
	// release may still have left behind.
	staleRewrite := filepath.Join(dir, ".0000000000003-rw.json.4242.tmp")
	staleLegacy := filepath.Join(dir, "0000000000004-legacy.json.tmp")
	for _, p := range []string{stale, fresh, staleRewrite, staleLegacy} {
		if err := os.WriteFile(p, []byte(`{"kind":"event"`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := now.Add(-tmpSweepAge - time.Minute)
	for _, p := range []string{stale, staleRewrite, staleLegacy} {
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}

	leased, err := s.Lease(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 0 {
		t.Fatalf("Lease returned %d temp files as items", len(leased))
	}
	for _, p := range []string{stale, staleRewrite, staleLegacy} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("the abandoned temp file %s was not swept", filepath.Base(p))
		}
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatal("a temp file a live hook may still rename was swept")
	}
	if st, _ := s.Stats(); st.Dropped["tmp_swept"] != 3 {
		t.Fatalf("sweep not counted: %v", st.Dropped)
	}

	// Open sweeps too, for the case where no drain ever runs on this machine.
	reopened, err := Open(Options{Dir: s.opts.Dir, DiskFree: healthyDisk, Now: func() time.Time { return now.Add(time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fresh); !os.IsNotExist(err) {
		t.Fatal("an hour later the second temp file is stale and must be swept on Open")
	}
	_ = reopened
}

// ---------------------------------------------------------------------------
// The cached byte total
// ---------------------------------------------------------------------------

// Add far from the cap must not stat every queued file: the estimate in the
// state file is trusted. Near the cap the directory is measured for real and
// the estimate corrected, which is how a stale number cannot evict wrongly.
func TestEnforceCapUsesTheCachedTotalFarFromTheCap(t *testing.T) {
	s := newSpool(t, func(o *Options) { o.MaxBytes = 1 << 20 })
	for range 5 {
		if err := s.Add(item("x")); err != nil {
			t.Fatal(err)
		}
	}
	st, _ := s.State()
	if st.PendingBytes <= 0 {
		t.Fatal("Add did not maintain the cached pending byte total")
	}
	// Poison the estimate low. Far from the cap Add trusts it and does not
	// scan, so nothing is evicted even though the truth is unknown to it.
	s.mutateState(func(x *State) { x.PendingBytes = 1 })
	if err := s.Add(item("y")); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Lease(0); len(n) != 6 {
		t.Fatalf("pending = %d, want 6; nothing may be evicted below the cap", len(n))
	}
	// A real measurement corrects the estimate.
	measured, _ := s.Stats()
	after, _ := s.State()
	if after.PendingBytes != measured.Bytes {
		t.Fatalf("Stats left the cache at %d, want the measured %d", after.PendingBytes, measured.Bytes)
	}
	// Ack subtracts.
	leased, _ := s.Lease(1)
	if err := s.Ack(leased[0]); err != nil {
		t.Fatal(err)
	}
	acked, _ := s.State()
	if acked.PendingBytes >= after.PendingBytes {
		t.Fatalf("Ack did not reduce the cached total: %d -> %d", after.PendingBytes, acked.PendingBytes)
	}
}

// ---------------------------------------------------------------------------
// Undecided, parked, quarantined with a reason
// ---------------------------------------------------------------------------

// An item the server names in neither list is not a failed attempt. Its
// Attempts stay at zero, a separate counter advances, and at the cutoff it is
// parked, never quarantined.
func TestUndecidedParksAtTheCutoffAndNeverQuarantines(t *testing.T) {
	s := newSpool(t, func(o *Options) { o.MaxUndecided = 3; o.MaxAttempts = 1 })
	_ = s.Add(item("ignored"))

	for i := 1; i <= 3; i++ {
		leased, _ := s.Lease(1)
		if len(leased) != 1 {
			t.Fatalf("round %d: item is no longer pending", i)
		}
		if leased[0].Item.Attempts != 0 {
			t.Fatalf("round %d: Attempts = %d; an undecided verdict is not a failed attempt", i, leased[0].Item.Attempts)
		}
		parked, err := s.Undecided(leased[0])
		if err != nil {
			t.Fatal(err)
		}
		if parked != (i == 3) {
			t.Fatalf("round %d: parked = %v", i, parked)
		}
	}
	st, _ := s.Stats()
	if st.Pending != 0 || st.Parked != 1 || st.Quarantine != 0 {
		t.Fatalf("pending=%d parked=%d quarantine=%d, want 0/1/0", st.Pending, st.Parked, st.Quarantine)
	}
	if st.Dropped["parked"] != 1 || st.Dropped["max_attempts"] != 0 {
		t.Fatalf("counters = %v, want one parked and no max_attempts", st.Dropped)
	}

	// Parked items come back with their counters reset, into their old place.
	n, err := s.RedriveParked()
	if err != nil || n != 1 {
		t.Fatalf("redrive parked: n=%d err=%v", n, err)
	}
	back, _ := s.Lease(10)
	if len(back) != 1 || back[0].Item.Undecided != 0 {
		t.Fatalf("parked item did not return cleanly: %+v", back)
	}
}

// An explicit rejection quarantines at once, keeps the server's reason in the
// filename, and counts under that reason so the fleet can tell reasons apart.
func TestRejectionQuarantinesWithTheReasonInTheName(t *testing.T) {
	s := newSpool(t)
	_ = s.Add(item("bad"))
	leased, _ := s.Lease(1)
	if err := s.Quarantine(leased[0], "Payload Too Large (4 MiB)"); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(filepath.Join(s.opts.Dir, quarantineDir))
	if len(ents) != 1 {
		t.Fatalf("quarantine holds %d files, want 1", len(ents))
	}
	name := ents[0].Name()
	if !strings.Contains(name, ".rejected-payload_too_large_4_mib.json") {
		t.Fatalf("quarantined as %q; the reason must be in the name", name)
	}
	st, _ := s.Stats()
	if st.Dropped["rejected:payload_too_large_4_mib"] != 1 {
		t.Fatalf("rejection not counted under its reason: %v", st.Dropped)
	}

	// Redrive strips the tag so the item sorts back into its place.
	if n, _ := s.Redrive(); n != 1 {
		t.Fatalf("redrove %d, want 1", n)
	}
	pending, _ := os.ReadDir(filepath.Join(s.opts.Dir, pendingDir))
	if len(pending) != 1 || strings.Contains(pending[0].Name(), "rejected") {
		t.Fatalf("redriven name = %v, want the reason tag stripped", pending)
	}
}

// The operator's replay acks an item straight out of quarantine. That item
// already left the pending total when it was moved out; the ack must not
// subtract it a second time and leave the cached total short of the truth.
func TestAckOfAQuarantinedItemLeavesPendingBytesAlone(t *testing.T) {
	s := newSpool(t)
	_ = s.Add(item("kept"))
	_ = s.Add(item("bad"))
	leased, _ := s.Lease(2)
	for _, l := range leased {
		if strings.Contains(string(l.Item.Payload), "bad") {
			if err := s.Quarantine(l, "schema"); err != nil {
				t.Fatal(err)
			}
		}
	}
	before, _ := s.State()
	q, err := s.Quarantined(1)
	if err != nil || len(q) != 1 {
		t.Fatalf("quarantined = %d (%v), want 1", len(q), err)
	}
	if err := s.Ack(q[0]); err != nil {
		t.Fatal(err)
	}
	after, _ := s.State()
	if after.PendingBytes != before.PendingBytes {
		t.Fatalf("pending bytes went %d -> %d on an ack out of quarantine; the item was not in pending/", before.PendingBytes, after.PendingBytes)
	}
	st, _ := s.Stats()
	if after.PendingBytes != st.Bytes {
		t.Fatalf("cached pending bytes %d, measured %d", after.PendingBytes, st.Bytes)
	}
	if ents, _ := os.ReadDir(filepath.Join(s.opts.Dir, quarantineDir)); len(ents) != 0 {
		t.Fatalf("the acked item is still quarantined: %v", ents)
	}
}

func TestReasonTagIsFilenameAndLabelSafe(t *testing.T) {
	cases := map[string]string{
		"":                                     "unspecified",
		"   ":                                  "unspecified",
		"event failed validation":              "event_failed_validation",
		"Payload/too;large!!":                  "payload_too_large",
		"session belongs to another principal": "session_belongs_to_another_principal",
		strings.Repeat("a", 100):               strings.Repeat("a", 48),
	}
	for in, want := range cases {
		if got := reasonTag(in); got != want {
			t.Errorf("reasonTag(%q) = %q, want %q", in, got, want)
		}
	}
	if got := baseName("0000000000001-abc.rejected-too_large.json"); got != "0000000000001-abc.json" {
		t.Errorf("baseName = %q", got)
	}
}

// Record is the counter for things that are not drops but belong beside them.
func TestRecordCountsDurably(t *testing.T) {
	dir := t.TempDir()
	a, _ := Open(Options{Dir: dir, DiskFree: healthyDisk})
	a.Record("recovered_from_transcript", 3)
	a.Record("hook_abandoned", 0) // a zero must not create a key

	b, _ := Open(Options{Dir: dir, DiskFree: healthyDisk})
	st, _ := b.Stats()
	if st.Dropped["recovered_from_transcript"] != 3 {
		t.Fatalf("counter did not survive the process: %v", st.Dropped)
	}
	if _, ok := st.Dropped["hook_abandoned"]; ok {
		t.Fatalf("a zero record created a key: %v", st.Dropped)
	}
}

// Item counters round-trip through the file, so a parked item redriven by a
// different process still knows its history.
func TestItemCountersRoundTrip(t *testing.T) {
	it := Item{ID: "x", Kind: "event", Attempts: 2, Undecided: 5, Payload: json.RawMessage(`{}`), EventTime: time.Now()}
	b, _ := json.Marshal(it)
	var back Item
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Attempts != 2 || back.Undecided != 5 {
		t.Fatalf("counters lost: %+v", back)
	}
}

// A rewrite goes through a per-process temp name. Two daemons redriving the
// same parked item share nothing, and a killed rewrite leaves a dotted file
// Lease never returns and sweepTemp removes.
func TestRewriteUsesAPerProcessTempName(t *testing.T) {
	s := newSpool(t, func(*Options) {})
	if err := s.Add(item("x")); err != nil {
		t.Fatal(err)
	}
	leased, _ := s.Lease(1)
	name := tempName(leased[0].path)
	if filepath.Dir(name) != filepath.Dir(leased[0].path) {
		t.Fatalf("temp name %s is not beside its item", name)
	}
	base := filepath.Base(name)
	if !strings.HasPrefix(base, ".") || !strings.HasSuffix(base, "."+strconv.Itoa(os.Getpid())+".tmp") {
		t.Fatalf("temp name %s is not dot-prefixed and pid-suffixed", base)
	}
	if _, err := s.Undecided(leased[0]); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(filepath.Join(s.opts.Dir, pendingDir))
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("a completed rewrite left %s behind", e.Name())
		}
	}
}

// A state file from before the cached total existed says nothing about the
// queue. Trusting its zero would let a laptop back from a week offline write
// past the cap until the next report; the first Add measures instead, and
// from then on the cache is trusted as before.
func TestUnmeasuredPendingBytesAreScannedOnce(t *testing.T) {
	s := newSpool(t, func(o *Options) { o.MaxBytes = 400 })
	dir := filepath.Join(s.opts.Dir, pendingDir)
	// A backlog written by the previous release: items on disk, no estimate.
	body := []byte(`{"kind":"event","session_id":"s","payload":` + strings.Repeat("x", 150) + `}`)
	for i := 1; i <= 3; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%013d-old%d.json", i, i)), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s.mutateState(func(x *State) { x.PendingBytes, x.PendingMeasured = 0, false })

	if err := s.Add(item("new")); err != nil {
		t.Fatal(err)
	}
	st, _ := s.State()
	if !st.PendingMeasured {
		t.Fatal("the first Add over an unmeasured estimate did not measure")
	}
	stats, _ := s.Stats()
	if stats.Dropped["queue_overflow"] == 0 {
		t.Fatal("the backlog was not evicted down to the cap; the zero estimate was trusted")
	}
	if stats.Bytes > s.opts.MaxBytes {
		t.Fatalf("pending holds %d bytes over a cap of %d", stats.Bytes, s.opts.MaxBytes)
	}

	// Measured now: a poisoned estimate far from the cap is trusted again, so
	// the scan really was a one-off.
	s.mutateState(func(x *State) { x.PendingBytes = 1 })
	before := s.countItems(pendingDir)
	if err := s.Add(item("later")); err != nil {
		t.Fatal(err)
	}
	if after := s.countItems(pendingDir); after != before+1 {
		t.Fatalf("pending went %d -> %d on an Add the cache should have trusted", before, after)
	}
}
