package app

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// deriveDB is the least connection source the derive runner's loop can be
// observed over: it answers the version stamp as already current, the dirty
// set as empty, and records what was asked, so the test can count passes
// without a Postgres and without waiting for the shipped cadence.
type deriveDB struct {
	mu    sync.Mutex
	calls []string
	// version is what derived_schema answers; the code's own version means
	// the versioned pass has nothing to do and sleeps for its recheck.
	version int
	// asked receives once per dirty-set query, which is how a test waits for
	// the loop to have run rather than sleeping and hoping.
	asked chan struct{}
}

func (d *deriveDB) record(sql string) {
	d.mu.Lock()
	d.calls = append(d.calls, sql)
	d.mu.Unlock()
}

func (d *deriveDB) countMatching(sub string) int {
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

func (d *deriveDB) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	d.record(sql)
	if strings.Contains(sql, "WHERE derive_dirty") {
		select {
		case d.asked <- struct{}{}:
		default:
		}
	}
	return &bootRows{}, nil
}

func (d *deriveDB) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	d.record(sql)
	switch {
	case strings.Contains(sql, "schema_migrations"):
		return bootRow{values: []any{false}}
	case strings.Contains(sql, "FROM derived_schema"):
		return bootRow{values: []any{d.version}}
	case strings.Contains(sql, "pg_try_advisory_xact_lock"):
		return bootRow{values: []any{true}}
	case strings.Contains(sql, "count(*)"):
		return bootRow{values: []any{int64(0)}}
	}
	return bootErrRow{err: pgx.ErrNoRows}
}

func (d *deriveDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	d.record(sql)
	return 0, nil
}

func (d *deriveDB) Begin(ctx context.Context) (store.Tx, error) { return &deriveTx{db: d}, nil }

type deriveTx struct{ db *deriveDB }

func (t *deriveTx) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return t.db.Query(ctx, sql, args...)
}
func (t *deriveTx) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	return t.db.QueryRow(ctx, sql, args...)
}
func (t *deriveTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return t.db.Exec(ctx, sql, args...)
}
func (t *deriveTx) Commit(ctx context.Context) error   { return nil }
func (t *deriveTx) Rollback(ctx context.Context) error { return nil }

func deriveBuild(t *testing.T, db *deriveDB) (*App, *bytes.Buffer) {
	t.Helper()
	var out bytes.Buffer
	log := NewLogger(slog.LevelInfo, &out)
	a, err := build(context.Background(), bootConfig(t), log, store.NewWithDB(db, ingest.NewPricer(nil, log)),
		func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return a, &out
}

// The runner stands down on an App with no store, as the sweeper does: the
// drain tests construct one literally, and a goroutine dereferencing a nil
// store would take the process down rather than fail a test.
func TestTheDeriveRunnerIsSkippedOnAnAppWithNoStore(t *testing.T) {
	a := &App{cfg: bootConfig(t), log: bootLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	select {
	case <-a.startDerive(ctx):
	case <-time.After(2 * time.Second):
		t.Fatal("startDerive did not stand down on an App with no store")
	}
}

// Both passes run on their schedule: the dirty pass keeps asking, the
// versioned pass asks once, finds the version current, and sleeps for its
// recheck rather than asking again. The shutdown ends both.
func TestTheDeriveRunnerRunsTheDirtyPassOnItsCadenceAndTheVersionedPassOnce(t *testing.T) {
	db := &deriveDB{version: store.DerivedSchema, asked: make(chan struct{}, 8)}
	a, out := deriveBuild(t, db)
	a.deriveFirst = time.Millisecond
	a.deriveDirtyEvery = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := a.startDerive(ctx)
	for i := range 3 {
		select {
		case <-db.asked:
		case <-time.After(5 * time.Second):
			t.Fatalf("the dirty pass ran %d times in five seconds, want at least 3", i)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the runner did not stop on shutdown")
	}

	if n := db.countMatching("FROM derived_schema"); n != 1 {
		t.Errorf("the versioned pass read the version %d times, want once: a current version sleeps for its recheck", n)
	}
	if n := db.countMatching("INSERT INTO derive_jobs"); n != 0 {
		t.Errorf("a current version seeded %d job ledgers", n)
	}
	// The store's own lines land in the server's stream, through the logger
	// build hands it: the migration line per applied file, and the pass line.
	if !strings.Contains(out.String(), `"migration applied"`) || !strings.Contains(out.String(), `"file":"0019_derive_jobs.sql"`) {
		t.Errorf("no \"migration applied\" line for 0019 reached the server's logger:\n%s", out.String())
	}
	if !strings.Contains(out.String(), `"derive pass"`) {
		t.Errorf("no \"derive pass\" line was written; that line is the standing evidence the runner is alive:\n%s", out.String())
	}
	// The runner's version is derive_version inside the pass group and the
	// group carries no "version" of its own: that key is the build, stamped
	// on the line by the server's logger.
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if !strings.Contains(line, `"derive pass"`) {
			continue
		}
		var m struct {
			Pass map[string]any `json:"pass"`
		}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("derive pass line is not JSON: %v\n%s", err, line)
		}
		if m.Pass["derive_version"] != float64(store.DerivedSchema) || m.Pass["version"] != nil {
			t.Errorf("derive pass group = %v, want derive_version %d and no version key", m.Pass, store.DerivedSchema)
		}
	}
	if strings.Contains(out.String(), `"derive dirty pass"`) {
		t.Errorf("a dirty pass that folded nothing wrote a line; at a 30 s cadence that is most of the log")
	}
}

// The shipped cadence is stated in code, not in a variable: two minutes
// after boot, then thirty seconds between dirty passes. A test that pins
// the numbers is what stops a refactor quietly changing how stale a live
// session's turns may be.
func TestTheDeriveScheduleIsTheDocumentedOne(t *testing.T) {
	if deriveFirstPass != 2*time.Minute {
		t.Errorf("deriveFirstPass = %s, want 2m", deriveFirstPass)
	}
	if deriveDirtyInterval != 30*time.Second {
		t.Errorf("deriveDirtyInterval = %s, want 30s", deriveDirtyInterval)
	}
	if deriveDoneRecheck < time.Hour || deriveFailedRetry < 10*time.Minute {
		t.Errorf("recheck %s and failed retry %s are short enough to loop on a parked pass", deriveDoneRecheck, deriveFailedRetry)
	}
}
