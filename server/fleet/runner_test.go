package fleet

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestATickYoungerThanTheIntervalIsAnotherInstances is F8. The advisory
// lock only stops two ticks that overlap; Cloud Run runs several instances
// whose timers fire at their own moments, so each would log its own set of
// lines per interval and the line-count metrics would read N fleets. A tick
// that finds another instance's tick on record younger than the interval
// (less thirty seconds of clock slack) is that instance's interval and does
// nothing.
func TestATickYoungerThanTheIntervalIsAnotherInstances(t *testing.T) {
	// The store's clock and the instances', moved together wherever the test
	// means time to pass: the gate is the store's to judge.
	db, clock := now, now
	st := &fakeStore{held: true, dbNow: func() time.Time { return db },
		devices: []Device{{ID: "d1", Email: "a@example.com"}},
		reports: []Report{v1Report("a@example.com", "d1", "23713ea", now.Add(-time.Minute), 0)}}
	var outA, outB bytes.Buffer
	a := &Runner{Store: st, Manifest: fakeManifest{err: ErrManifestMissing}, Log: slog.New(slog.NewJSONHandler(&outA, nil)), Now: func() time.Time { return clock }}
	b := &Runner{Store: st, Manifest: fakeManifest{err: ErrManifestMissing}, Log: slog.New(slog.NewJSONHandler(&outB, nil)), Now: func() time.Time { return clock }}

	if _, did, err := a.Tick(context.Background()); err != nil || !did {
		t.Fatalf("A's tick: did=%v err=%v", did, err)
	}
	db, clock = db.Add(time.Second), clock.Add(time.Second)
	if _, did, err := b.Tick(context.Background()); err != nil || did {
		t.Fatalf("B's tick one second after A's: did=%v err=%v, want nothing done", did, err)
	}
	if outB.Len() != 0 {
		t.Errorf("B emitted lines for A's interval:\n%s", outB.String())
	}
	// B's next timer, a whole interval later, is B's interval.
	db, clock = db.Add(Interval), clock.Add(Interval)
	if _, did, err := b.Tick(context.Background()); err != nil || !did {
		t.Fatalf("B's tick an interval later: did=%v err=%v", did, err)
	}
	if n := strings.Count(outB.String(), `"msg":"fleet summary"`); n != 1 {
		t.Errorf("B logged %d summary lines, want one", n)
	}
	// A, thirty seconds before its own interval is up, is inside the slack
	// and runs; the gate is the interval less the slack, never more.
	db, clock = db.Add(Interval-30*time.Second), clock.Add(Interval-30*time.Second)
	if _, did, err := a.Tick(context.Background()); err != nil || !did {
		t.Fatalf("A's tick at the slack edge: did=%v err=%v", did, err)
	}
}

// TestAnInstanceAnHourAheadDoesNotSuppressTheFleetsNextTick pins a review
// finding. The gate used to be handed the ticking instance's own clock: an instance an
// hour ahead stamped the row an hour into the future and every instance
// keeping correct time was refused until wall time reached the stamp, so
// the fleet's cadence belonged to whichever clock was most wrong. TickDue
// takes no clock now, and this test is what goes red if one is wired back
// in: the fake stores the moment the gate read, and an interval of the
// store's time is all the next instance may be made to wait.
func TestAnInstanceAnHourAheadDoesNotSuppressTheFleetsNextTick(t *testing.T) {
	db := now
	st := &fakeStore{held: true, dbNow: func() time.Time { return db },
		devices: []Device{{ID: "d1", Email: "a@example.com"}},
		reports: []Report{v1Report("a@example.com", "d1", "23713ea", now.Add(-time.Minute), 0)}}
	var outAhead, outRight bytes.Buffer
	ahead := &Runner{Store: st, Manifest: fakeManifest{err: ErrManifestMissing},
		Log: slog.New(slog.NewJSONHandler(&outAhead, nil)), Now: func() time.Time { return db.Add(time.Hour) }}
	right := &Runner{Store: st, Manifest: fakeManifest{err: ErrManifestMissing},
		Log: slog.New(slog.NewJSONHandler(&outRight, nil)), Now: func() time.Time { return db }}

	if _, did, err := ahead.Tick(context.Background()); err != nil || !did {
		t.Fatalf("the fast instance's tick: did=%v err=%v", did, err)
	}
	if !st.lastTick.Equal(db) {
		t.Errorf("the tick on record is %s, want the store's clock at %s: the instance's hour reached the row", st.lastTick, db)
	}
	// One interval of the store's time, not of the fast instance's.
	db = db.Add(Interval)
	if _, did, err := right.Tick(context.Background()); err != nil || !did {
		t.Fatalf("a correct clock an interval after a clock an hour ahead: did=%v err=%v, want its tick to run", did, err)
	}
	if n := strings.Count(outRight.String(), `"msg":"fleet summary"`); n != 1 {
		t.Errorf("the correct-clock instance logged %d summary lines, want one", n)
	}
}

// countingManifest counts what the release bucket would have been asked.
type countingManifest struct {
	m     Manifest
	calls int
}

func (c *countingManifest) Latest(context.Context) (Manifest, error) {
	c.calls++
	return c.m, nil
}

// TestTheViewCachesTheManifestForAnInterval is F10's second half: the fleet
// page read latest.json from the bucket on every load. A page load reads
// the copy the last tick or page fetched, for one interval.
func TestTheViewCachesTheManifestForAnInterval(t *testing.T) {
	src := &countingManifest{m: Manifest{Commit: "788dcb3", BuildDate: now.Add(-48 * time.Hour)}}
	clock := now
	r := &Runner{Store: &fakeStore{held: true}, Manifest: src, Now: func() time.Time { return clock }}
	for range 3 {
		if _, err := r.View(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if src.calls != 1 {
		t.Errorf("three page loads fetched latest.json %d times, want once", src.calls)
	}
	clock = clock.Add(Interval + time.Second)
	if _, err := r.View(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.calls != 2 {
		t.Errorf("a page load after the interval fetched %d times in all, want a second fetch", src.calls)
	}
	// A tick always reads the bucket, and the page after it reads the
	// tick's copy.
	clock = clock.Add(Interval + time.Second)
	if _, _, err := r.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.View(context.Background()); err != nil {
		t.Fatal(err)
	}
	if src.calls != 3 {
		t.Errorf("a tick and the page after it fetched %d times in all, want three", src.calls)
	}
}
