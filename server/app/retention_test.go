package app

// These tests exist because of a specific defect, repeated six times in this
// codebase before it was noticed: a component that was written, unit-tested and
// never invoked by anything. A retention sweep is the easiest possible instance
// of it — every test in server/store can pass while the database goes on growing
// exactly as it did, because nothing outside a test ever calls SweepRetention.
//
// So the assertions below are deliberately not about what a sweep does. They are
// about whether the assembled server calls one at all, whether it stops calling
// one when it shuts down, and whether the policy it would sweep under is visible
// in the log before it acts. The first of those drives the real serve() over a
// real listener, because that is the only place the answer is honest.

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

	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// ---------------------------------------------------------------------------
// A connection source that answers migration, preview and sweep
// ---------------------------------------------------------------------------

// retDB is bootDB's sibling for the statements retention issues. It is separate
// rather than an extension of bootDB because what it has to script — an advisory
// lock that is granted, a backlog that has a size — is the opposite of bootDB's
// premise that nothing below the routes is reachable without a credential.
type retDB struct {
	mu    sync.Mutex
	calls []string

	// granted is what pg_try_advisory_xact_lock answers. False models the other
	// Cloud Run instance already sweeping.
	granted bool
	// bodiesDue and sessionsDue are what the boot preview counts.
	bodiesDue, sessionsDue int64

	// swept receives once per body-expiry statement, which is how a test waits
	// for the sweeper to have actually run rather than sleeping and hoping.
	swept chan struct{}
}

func (d *retDB) record(sql string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, sql)
}

func (d *retDB) countMatching(sub string) int {
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

func (d *retDB) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	d.record(sql)
	return &bootRows{}, nil
}

func (d *retDB) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	d.record(sql)
	switch {
	case strings.Contains(sql, "schema_migrations"):
		return bootRow{values: []any{false}}
	case strings.Contains(sql, "pg_try_advisory_xact_lock"):
		return bootRow{values: []any{d.granted}}
	// The preview's two counts, told apart from the sweep's own lookups by the
	// aggregate: only the preview counts.
	case strings.Contains(sql, "count(*)") && strings.Contains(sql, "FROM events"):
		return bootRow{values: []any{d.bodiesDue}}
	case strings.Contains(sql, "count(*)") && strings.Contains(sql, "FROM sessions"):
		return bootRow{values: []any{d.sessionsDue}}
	}
	// Everything else is "no such row", which for the sweep's cutoff query means
	// nothing is past its window and the pass is over.
	return bootErrRow{err: pgx.ErrNoRows}
}

func (d *retDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	d.record(sql)
	if strings.Contains(sql, "UPDATE events") && d.swept != nil {
		select {
		case d.swept <- struct{}{}:
		default:
		}
	}
	return 0, nil
}

func (d *retDB) Begin(ctx context.Context) (store.Tx, error) { return &retTx{db: d}, nil }

type retTx struct{ db *retDB }

func (t *retTx) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return t.db.Query(ctx, sql, args...)
}
func (t *retTx) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	return t.db.QueryRow(ctx, sql, args...)
}
func (t *retTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return t.db.Exec(ctx, sql, args...)
}
func (t *retTx) Commit(ctx context.Context) error   { return nil }
func (t *retTx) Rollback(ctx context.Context) error { return nil }

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// retConfig loads a configuration with the retention variables set as given. An
// empty value means the variable is absent, which is how the shipped defaults
// are exercised.
func retConfig(t *testing.T, bodyDays, sessionDays string) Config {
	t.Helper()
	env := bootEnv()
	if bodyDays != "" {
		env["RETENTION_BODY_DAYS"] = bodyDays
	}
	if sessionDays != "" {
		env["RETENTION_SESSION_DAYS"] = sessionDays
	}
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

// retBuild assembles the whole server over the fake connection source and hands
// back the log it wrote while doing so.
func retBuild(t *testing.T, db *retDB, cfg Config) (*App, string) {
	t.Helper()
	var out bytes.Buffer
	log := NewLogger(slog.LevelInfo, &out)
	a, err := build(context.Background(), cfg, log, store.NewWithDB(db, ingest.NewPricer(nil, log)),
		func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return a, out.String()
}

// ---------------------------------------------------------------------------
// The caller
// ---------------------------------------------------------------------------

// TestServeActuallySweepsAndStopsSweepingWhenItReturns is the assertion this
// whole file exists for.
//
// It drives the real serve() over a real listener rather than calling the
// sweeper directly, because "the sweep runs" and "the assembled server runs the
// sweep" are different claims and only the second one keeps a database from
// growing forever. A retention sweep nothing invokes is the seventh instance of
// the defect this codebase already had six of.
//
// The second half is the other side of the same wiring. main defers App.Close
// immediately after New, so a sweeper still issuing statements after serve
// returns is a sweeper issuing them against a closed pool.
func TestServeActuallySweepsAndStopsSweepingWhenItReturns(t *testing.T) {
	db := &retDB{granted: true, swept: make(chan struct{}, 4)}
	// Windows named explicitly, because the shipped default is to keep
	// everything and a test that took the default would assert that a disabled
	// sweeper does not sweep.
	a, _ := retBuild(t, db, retConfig(t, "90", "365"))
	// A schedule a test can wait for. The shipped values are two minutes and
	// six hours, which is the right cadence for a policy measured in months and
	// the wrong one for a test.
	a.retentionFirst = time.Millisecond
	a.retentionEvery = 50 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- a.serve(ctx, ln) }()

	select {
	case <-db.swept:
	case <-time.After(5 * time.Second):
		t.Fatal("the assembled server never swept: store.SweepRetention has no caller, which is the defect this was written to fix")
	}

	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(drainTimeout + 2*time.Second):
		t.Fatal("serve did not return; the sweeper join is holding the shutdown open")
	}

	// serve has returned, so the pool is about to be closed. Nothing may run
	// after this point.
	settled := db.countMatching("pg_try_advisory_xact_lock")
	time.Sleep(150 * time.Millisecond)
	if after := db.countMatching("pg_try_advisory_xact_lock"); after != settled {
		t.Errorf("the sweeper issued %d further statements after serve returned; it outlives the pool it queries",
			after-settled)
	}
}

// TestTheShippedDefaultKeepsEverythingAndStartsNoSweeper. Keeping everything is
// the decision this service ships with, and it must cost nothing: no goroutine,
// no timer, and above all no transaction that could delete anything. An unset
// pair of variables and an explicit pair of zeroes have to reach the same place,
// so both are driven here.
func TestTheShippedDefaultKeepsEverythingAndStartsNoSweeper(t *testing.T) {
	for _, tc := range []struct {
		name               string
		bodyDays, sessDays string
	}{
		{name: "the variables are absent, which is how this ships", bodyDays: "", sessDays: ""},
		{name: "the variables are set to zero explicitly", bodyDays: "0", sessDays: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := retConfig(t, tc.bodyDays, tc.sessDays)
			if cfg.Retention.Enabled() {
				t.Fatalf("the configuration is not the keep-everything one: %+v", cfg.Retention)
			}
			db := &retDB{granted: true, swept: make(chan struct{}, 4)}
			a, _ := retBuild(t, db, cfg)
			a.retentionFirst = time.Millisecond
			a.retentionEvery = time.Millisecond

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			select {
			case <-a.startRetention(ctx):
			case <-time.After(2 * time.Second):
				t.Fatal("the default configuration left a sweeper running")
			}
			time.Sleep(50 * time.Millisecond)
			for _, forbidden := range []string{"pg_try_advisory_xact_lock", "UPDATE events", "DELETE FROM"} {
				if n := db.countMatching(forbidden); n != 0 {
					t.Errorf("the default configuration issued %q %d times", forbidden, n)
				}
			}
		})
	}
}

// TestADisabledSweeperStaysDisabledOnItsSchedule is the same property observed
// through the timer rather than the constructor, because "no goroutine was
// started" and "the goroutine that was started never deletes anything" are
// different claims and only the first is asserted above.
func TestADisabledSweeperStaysDisabledOnItsSchedule(t *testing.T) {
	db := &retDB{granted: true, swept: make(chan struct{}, 4)}
	a, _ := retBuild(t, db, retConfig(t, "0", "0"))
	a.retentionFirst = time.Millisecond
	a.retentionEvery = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := a.startRetention(ctx)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a disabled policy left a goroutine running")
	}
	time.Sleep(50 * time.Millisecond)
	if n := db.countMatching("pg_try_advisory_xact_lock"); n != 0 {
		t.Errorf("a disabled policy swept %d times", n)
	}
}

// TestTheSweeperIsSkippedOnAnAppWithNoStore. The drain tests construct an App
// literally, with no store at all, and a sweeper that dereferenced it would turn
// those tests — and any future one like them — into a nil panic inside a
// goroutine, where it takes the process down rather than failing a test.
func TestTheSweeperIsSkippedOnAnAppWithNoStore(t *testing.T) {
	// Retention switched on, so that standing down is attributable to the
	// missing store rather than to the shipped default having switched it off.
	a := &App{cfg: retConfig(t, "90", "365"), log: bootLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	select {
	case <-a.startRetention(ctx):
	case <-time.After(2 * time.Second):
		t.Fatal("startRetention did not stand down on an App with no store")
	}
}

// ---------------------------------------------------------------------------
// What boot says about it
// ---------------------------------------------------------------------------

// TestBootStatesThePolicyAndWhatItIsAboutToRemove. This log line is the only
// place a policy that deletes colleagues' transcripts is visible without reading
// the source or the deployment file, and it is written before the listener
// opens, so it sits above the first request rather than inside the traffic.
func TestBootStatesThePolicyAndWhatItIsAboutToRemove(t *testing.T) {
	db := &retDB{granted: true, bodiesDue: 41, sessionsDue: 2}
	_, logged := retBuild(t, db, retConfig(t, "90", "365"))

	for _, want := range []string{
		"retention policy",
		// The windows in force, spelled in the unit the deployment uses.
		"90 days",
		"365 days",
		// And what those windows mean for this database right now.
		"retention backlog at boot",
		`"bodies_due":41`,
		`"sessions_due":2`,
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("the boot log does not state %q:\n%s", want, logged)
		}
	}
}

// TestBootWarnsThatNothingWillEverBeDeletedAndPricesTheDecision.
//
// This is what the service ships as: the owner chose to keep everything, having
// been shown the arithmetic. That makes the boot line carry more weight rather
// than less. It has to read as a decision — "forever", in as many words — rather
// than as a variable that failed to load, and it has to keep saying what the
// decision costs, because a standing choice nobody re-prices is a standing
// choice nobody can revisit.
func TestBootWarnsThatNothingWillEverBeDeletedAndPricesTheDecision(t *testing.T) {
	db := &retDB{granted: true, bodiesDue: 71924, sessionsDue: 891}
	_, logged := retBuild(t, db, retConfig(t, "", ""))

	if !strings.Contains(logged, `"level":"WARN"`) {
		t.Errorf("keeping everything forever was not warned about:\n%s", logged)
	}
	if !strings.Contains(logged, "without bound") {
		t.Errorf("the warning does not say what the consequence is:\n%s", logged)
	}
	// Stated as a decision, not as an empty value.
	if !strings.Contains(logged, "forever") {
		t.Errorf("the policy in force is not spelled out as forever:\n%s", logged)
	}
	// And priced against the windows nobody applied, so reversing the decision
	// later is a change to two variables rather than a fresh analysis.
	for _, want := range []string{
		"recommended windows would remove today",
		"90 days",
		"365 days",
		`"bodies_due":71924`,
		`"sessions_due":891`,
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("the boot log does not price the decision with %q:\n%s", want, logged)
		}
	}
	// The applied-policy line is absent, because no policy is applied.
	if strings.Contains(logged, "retention backlog at boot") {
		t.Error("a disabled policy reported a backlog as though it were about to act on it")
	}
}

// TestAnUncountableBacklogDoesNotStopTheServer. The server's job is to serve.
// Refusing to start because a housekeeping figure was unavailable would turn a
// retention problem into an outage, and the sweeper reports what it actually
// removed in any case.
func TestAnUncountableBacklogDoesNotStopTheServer(t *testing.T) {
	// bodiesDue is answered by a row of the wrong shape for the scan, which is
	// what a schema that has not caught up looks like from here.
	db := &retFailingPreview{retDB: retDB{granted: true}}
	var out bytes.Buffer
	log := NewLogger(slog.LevelInfo, &out)
	a, err := build(context.Background(), retConfig(t, "", ""), log,
		store.NewWithDB(db, ingest.NewPricer(nil, log)), func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("a failed backlog count stopped the server from starting: %v", err)
	}
	if a.handler == nil {
		t.Fatal("build returned an App with no handler")
	}
	if !strings.Contains(out.String(), "cannot report what retention would remove") {
		t.Errorf("the failure was swallowed rather than logged:\n%s", out.String())
	}
}

// retFailingPreview answers the backlog count with an error and everything else
// as retDB does.
type retFailingPreview struct{ retDB }

func (d *retFailingPreview) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	if strings.Contains(sql, "count(*)") {
		d.record(sql)
		return bootErrRow{err: pgx.ErrTxClosed}
	}
	return d.retDB.QueryRow(ctx, sql, args...)
}
