package app

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loopai-hq/loop-sessions/server/fleet"
	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// fleetDB is the least connection source the fleet evaluator's loop can be
// observed over: the lock is granted, every read is empty, and it records
// what was asked so the test can see the evaluator run at all. Its reason
// for existing is the derive loop test's: a component nothing invokes is
// this codebase's recurring defect.
type fleetDB struct {
	mu    sync.Mutex
	calls []string
	// ticked receives once per tick recorded on fleet_ticks, which is how a
	// test waits for a tick rather than sleeping and hoping.
	ticked chan struct{}
}

func (d *fleetDB) record(sql string) {
	d.mu.Lock()
	d.calls = append(d.calls, sql)
	d.mu.Unlock()
}

func (d *fleetDB) countMatching(sub string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, c := range d.calls {
		if strings.Contains(c, sub) {
			n++
		}
	}
	return n
}

func (d *fleetDB) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	d.record(sql)
	return &bootRows{}, nil
}

func (d *fleetDB) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	d.record(sql)
	switch {
	case strings.Contains(sql, "schema_migrations"):
		return bootRow{values: []any{false}}
	case strings.Contains(sql, "FROM derived_schema"):
		return bootRow{values: []any{store.DerivedSchema}}
	case strings.Contains(sql, "pg_try_advisory_xact_lock"):
		return bootRow{values: []any{true}}
	case strings.Contains(sql, "min(emitted_at)"):
		return bootRow{values: []any{(*time.Time)(nil)}}
	case strings.Contains(sql, "count(*)"):
		return bootRow{values: []any{int64(0)}}
	}
	return bootErrRow{err: pgx.ErrNoRows}
}

func (d *fleetDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	d.record(sql)
	if strings.Contains(sql, "INSERT INTO fleet_ticks") {
		// Every tick is due: the fake stands in for a database no other
		// instance shares, and the upsert answers with the row it wrote.
		select {
		case d.ticked <- struct{}{}:
		default:
		}
		return 1, nil
	}
	return 0, nil
}

func (d *fleetDB) Begin(ctx context.Context) (store.Tx, error) { return &fleetTx{db: d}, nil }

type fleetTx struct{ db *fleetDB }

func (t *fleetTx) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return t.db.Query(ctx, sql, args...)
}
func (t *fleetTx) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	return t.db.QueryRow(ctx, sql, args...)
}
func (t *fleetTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return t.db.Exec(ctx, sql, args...)
}
func (t *fleetTx) Commit(ctx context.Context) error   { return nil }
func (t *fleetTx) Rollback(ctx context.Context) error { return nil }

// TestTheAssembledServerRunsTheFleetEvaluatorOnItsCadence: the assembled
// server ticks the evaluator, each tick takes the lock, reads the latest
// reports and rolls the ledger up, the summary line reaches the server's own
// logger, the missing manifest is said once, and nothing runs after serve
// returns.
func TestTheAssembledServerRunsTheFleetEvaluatorOnItsCadence(t *testing.T) {
	db := &fleetDB{ticked: make(chan struct{}, 8)}
	var out bytes.Buffer
	log := NewLogger(slog.LevelInfo, &out)
	cfg, err := Load(bootGetenv(bootEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	a, err := build(context.Background(), cfg, log, store.NewWithDB(db, ingest.NewPricer(nil, log)),
		func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if a.fleet == nil {
		t.Fatal("the assembled server has no fleet evaluator")
	}
	// A cadence a test can wait for; the shipped one is five minutes.
	a.fleetFirst = time.Millisecond
	a.fleet.Interval = 20 * time.Millisecond
	// No manifest source is wired into the test's config: the bucket is
	// absent, so the evaluator warns once and never alerts lag.
	a.deriveFirst, a.retentionFirst = time.Hour, time.Hour

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- a.serve(ctx, ln) }()

	for i := range 3 {
		select {
		case <-db.ticked:
		case <-time.After(5 * time.Second):
			t.Fatalf("the assembled server ticked the fleet evaluator %d times in 5 s, want 3: fleet.Runner has no caller", i)
		}
	}
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(drainTimeout + 2*time.Second):
		t.Fatal("serve did not return; the fleet join is holding the shutdown open")
	}
	settled := db.countMatching("FROM health_latest")
	time.Sleep(100 * time.Millisecond)
	if after := db.countMatching("FROM health_latest"); after != settled {
		t.Errorf("the evaluator issued %d further reads after serve returned", after-settled)
	}
	if settled == 0 || db.countMatching("INSERT INTO health_hourly") == 0 || db.countMatching("FROM fleet_mutes") == 0 {
		t.Errorf("a tick did not read the latest reports, roll the ledger up and read the mutes: %d/%d/%d",
			settled, db.countMatching("INSERT INTO health_hourly"), db.countMatching("FROM fleet_mutes"))
	}
	lines := out.String()
	if !strings.Contains(lines, `"msg":"fleet summary"`) {
		t.Errorf("no fleet summary line in the server's own logger:\n%s", lines)
	}
	if n := strings.Count(lines, `"msg":"release manifest missing"`); n != 1 {
		t.Errorf("the missing manifest was said %d times, want once", n)
	}
	if strings.Contains(lines, `"msg":"fleet evaluation failed"`) {
		t.Errorf("a tick failed:\n%s", lines)
	}
}

// TestTheFleetEvaluatorIsSkippedOnAnAppWithNoStore: the drain tests
// construct an App literally; nothing must start for it.
func TestTheFleetEvaluatorIsSkippedOnAnAppWithNoStore(t *testing.T) {
	a := &App{fleet: &fleet.Runner{}}
	done := a.startFleet(context.Background())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("startFleet started a goroutine for an App with no store")
	}
}
