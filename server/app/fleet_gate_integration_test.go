//go:build integration

package app

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/loop-sessions/internal/health"
	"github.com/loopai-hq/loop-sessions/server/fleet"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// TestIntegrationTwoInstancesWhoseTicksDoNotOverlapEmitOneSetOfLines is the
// adversarial pass's F8 over a real store. The advisory lock covers two
// ticks that overlap; two instances whose timers fire a second apart both
// held it in turn and both logged the fleet. The fleet_ticks row, written
// under the lock, makes the second tick another instance's interval.
//
// The gate is measured by the database's clock, so an interval passes here
// by ageing the stored stamp rather than by moving an instance's clock: an
// instance's clock cannot move the gate, which is the property review-3's
// finding 2 asked for and the test below pins.
func TestIntegrationTwoInstancesWhoseTicksDoNotOverlapEmitOneSetOfLines(t *testing.T) {
	ctx := context.Background()
	pool, st := newFleetSchema(t)
	if _, err := pool.Exec(ctx, `INSERT INTO principals (email, role, added_by) VALUES ('one@example.com', 'member', 'test')`); err != nil {
		t.Fatal(err)
	}
	dev, err := st.EnrollDevice(ctx, store.Device{Email: "one@example.com", Hostname: "one-mbp"}, []byte("h1"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	r := health.Report{SchemaVersion: 1, Hostname: "one-mbp", AgentVersion: "788dcb3", EmittedAt: now.Add(-time.Minute)}
	if err := st.PutHealthReport(ctx, "one@example.com", dev.ID, r); err != nil {
		t.Fatal(err)
	}
	clock := now
	newRunner := func(out *bytes.Buffer) *fleet.Runner {
		return &fleet.Runner{
			Store:       fleetStore{s: st},
			Manifest:    fakeManifestSource{m: fleet.Manifest{Version: "788dcb3", Commit: "788dcb3", BuildDate: now.Add(-48 * time.Hour)}},
			ServerBuild: "788dcb3",
			Log:         slog.New(slog.NewJSONHandler(out, nil)),
			Now:         func() time.Time { return clock },
		}
	}
	var outA, outB bytes.Buffer
	a, b := newRunner(&outA), newRunner(&outB)
	if _, did, err := a.Tick(ctx); err != nil || !did {
		t.Fatalf("A's tick: did=%v err=%v", did, err)
	}
	clock = now.Add(time.Second)
	if _, did, err := b.Tick(ctx); err != nil || did {
		t.Fatalf("B's tick a second after A's: did=%v err=%v, want nothing done", did, err)
	}
	if outB.Len() != 0 {
		t.Errorf("B emitted lines for A's interval:\n%s", outB.String())
	}
	// An interval of the database's time, which is the only clock the gate
	// reads: the stamp is aged rather than the instance moved.
	aged := ageTheFleetTick(t, ctx, pool, fleet.Interval)
	clock = now.Add(fleet.Interval)
	if _, did, err := b.Tick(ctx); err != nil || !did {
		t.Fatalf("B's tick an interval later: did=%v err=%v", did, err)
	}
	if n := strings.Count(outB.String(), `"msg":"fleet summary"`); n != 1 {
		t.Errorf("B logged %d summary lines, want one", n)
	}
	last := readTheFleetTick(t, ctx, pool)
	if !last.After(aged) {
		t.Errorf("fleet_ticks still records %s, want B's tick after the aged stamp at %s", last, aged)
	}
}

// TestIntegrationTheTickGateIgnoresAnInstancesClock is review-3's finding 2
// over a real store. The gate used to be handed the ticking instance's own
// clock, so an instance an hour ahead stamped the row an hour into the
// future and every instance keeping correct time was refused until wall
// time reached the stamp: the fleet's cadence belonged to whichever clock
// was most wrong. Both sides of the comparison are now the database's, and
// this is what goes red if an instance's clock is wired back in.
func TestIntegrationTheTickGateIgnoresAnInstancesClock(t *testing.T) {
	ctx := context.Background()
	pool, st := newFleetSchema(t)
	if _, err := pool.Exec(ctx, `INSERT INTO principals (email, role, added_by) VALUES ('two@example.com', 'member', 'test')`); err != nil {
		t.Fatal(err)
	}
	dev, err := st.EnrollDevice(ctx, store.Device{Email: "two@example.com", Hostname: "two-mbp"}, []byte("h2"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := st.PutHealthReport(ctx, "two@example.com", dev.ID,
		health.Report{SchemaVersion: 1, Hostname: "two-mbp", AgentVersion: "788dcb3", EmittedAt: now.Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}
	newRunner := func(out *bytes.Buffer, clock func() time.Time) *fleet.Runner {
		return &fleet.Runner{
			Store:       fleetStore{s: st},
			Manifest:    fakeManifestSource{m: fleet.Manifest{Version: "788dcb3", Commit: "788dcb3", BuildDate: now.Add(-48 * time.Hour)}},
			ServerBuild: "788dcb3",
			Log:         slog.New(slog.NewJSONHandler(out, nil)),
			Now:         clock,
		}
	}
	var outAhead, outRight bytes.Buffer
	ahead := newRunner(&outAhead, func() time.Time { return now.Add(time.Hour) })
	right := newRunner(&outRight, func() time.Time { return now })

	before := time.Now().UTC()
	if _, did, err := ahead.Tick(ctx); err != nil || !did {
		t.Fatalf("the fast instance's tick: did=%v err=%v", did, err)
	}
	// The row holds the database's moment, not the instance's hour.
	stamped := readTheFleetTick(t, ctx, pool)
	if stamped.After(before.Add(30 * time.Minute)) {
		t.Fatalf("fleet_ticks records %s, half an hour or more after this test started at %s: the instance's clock reached the row", stamped, before)
	}
	// One interval of the database's time is all the next instance waits.
	ageTheFleetTick(t, ctx, pool, fleet.Interval)
	if _, did, err := right.Tick(ctx); err != nil || !did {
		t.Fatalf("a correct clock an interval after a clock an hour ahead: did=%v err=%v, want its tick to run", did, err)
	}
	if n := strings.Count(outRight.String(), `"msg":"fleet summary"`); n != 1 {
		t.Errorf("the correct-clock instance logged %d summary lines, want one", n)
	}
}

// ageTheFleetTick moves the stored tick back by d and returns the stamp it
// left, which is how these tests make an interval pass: the gate reads the
// database's clock, so nothing an instance does can stand in for time.
func ageTheFleetTick(t *testing.T, ctx context.Context, pool *pgxpool.Pool, d time.Duration) time.Time {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE fleet_ticks SET last_tick_at = last_tick_at - make_interval(secs => $1) WHERE name = 'fleet'`, d.Seconds()); err != nil {
		t.Fatalf("age the fleet tick: %v", err)
	}
	return readTheFleetTick(t, ctx, pool)
}

func readTheFleetTick(t *testing.T, ctx context.Context, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var at time.Time
	if err := pool.QueryRow(ctx, `SELECT last_tick_at FROM fleet_ticks WHERE name = 'fleet'`).Scan(&at); err != nil {
		t.Fatalf("fleet_ticks: %v", err)
	}
	return at
}
