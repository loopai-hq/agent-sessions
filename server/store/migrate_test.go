//go:build integration

package store

// The migration runner's properties that only Postgres can answer: that the
// files apply from empty and again on a migrated database, that the run waits
// for another server's run, that a file which loses a lock race is retried on
// its own and lands, and that one which keeps losing fails the boot inside its
// budget with an error that says so. Same gate and same per-run schema as
// integration_test.go.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migratePool opens a pool onto a fresh schema that no migration has touched,
// so each test starts from the state a first deploy sees. Four connections,
// because the lock tests need a second one open alongside the run.
func migratePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return migratePoolWith(t, nil)
}

// migratePoolWith is migratePool with a hook on the pool's configuration,
// for the test that needs a session-level statement_timeout the way the
// deployed pool sets one.
func migratePoolWith(t *testing.T, tweak func(*pgxpool.Config)) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LOOP_SESSIONS_TEST_DSN")
	if dsn == "" {
		t.Skip("LOOP_SESSIONS_TEST_DSN is unset; skipping the tests that need a database")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("loop_sessions_migrate_%d", time.Now().UnixNano())

	bootstrap, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := bootstrap.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		bootstrap.Close()
		t.Fatalf("create schema: %v", err)
	}
	bootstrap.Close()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 4
	if tweak != nil {
		tweak(cfg)
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to schema: %v", err)
	}
	t.Cleanup(func() {
		p.Close()
		if cleanup, err := pgxpool.New(ctx, dsn); err == nil {
			_, _ = cleanup.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
			cleanup.Close()
		}
	})
	return p
}

// ledger reads the migration ledger as name -> applied_at.
func ledger(t *testing.T, p *pgxpool.Pool) map[string]time.Time {
	t.Helper()
	rows, err := p.Query(context.Background(), `SELECT name, applied_at FROM schema_migrations`)
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var name string
		var at time.Time
		if err := rows.Scan(&name, &at); err != nil {
			t.Fatalf("scan ledger: %v", err)
		}
		out[name] = at
	}
	return out
}

// testPolicy is the deployed policy with every number shrunk so a lock race
// and a retry take milliseconds rather than the seconds production allows.
var testPolicy = migratePolicy{
	lockWait:    5 * time.Second,
	lockTimeout: 150 * time.Millisecond,
	retries:     4,
	budget:      5 * time.Second,
	// Well above lockTimeout, as the deployed policy's is, so a lock wait is
	// never mistaken for a slow body.
	statementTimeout: 5 * time.Second,
	sleep:            40 * time.Millisecond,
}

// TestIntegrationMigrateAppliesFromEmptyAndAgainOnAMigratedDatabase is the
// apply-twice property under the per-file runner: every file lands with its
// own ledger row, and a second run finds nothing to do and touches nothing.
func TestIntegrationMigrateAppliesFromEmptyAndAgainOnAMigratedDatabase(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	names, err := migrationNames()
	if err != nil {
		t.Fatalf("migrationNames: %v", err)
	}
	first := ledger(t, p)
	if len(first) != len(names) {
		t.Fatalf("ledger has %d rows after the first run, want %d", len(first), len(names))
	}
	for _, n := range names {
		if _, ok := first[n]; !ok {
			t.Errorf("ledger lacks %s", n)
		}
	}
	// The schema is usable: the tables the server needs are there, from the
	// first file (principals, empty by design: the roster is bootstrapped
	// from ADMIN_EMAILS, not seeded) to the last (the skill catalog).
	var principals int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM principals`).Scan(&principals); err != nil {
		t.Fatalf("count principals: %v", err)
	}
	if principals != 0 {
		t.Errorf("a fresh database holds %d principals; no migration may seed a person", principals)
	}
	var catalog int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM skill_catalog_entries`).Scan(&catalog); err != nil {
		t.Errorf("the last migration's table is missing; the files did not all run: %v", err)
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	second := ledger(t, p)
	if len(second) != len(first) {
		t.Fatalf("ledger has %d rows after the second run, want %d", len(second), len(first))
	}
	for n, at := range first {
		if !second[n].Equal(at) {
			t.Errorf("%s was re-recorded on the second run (%s -> %s); the ledger check inside the file transaction is not being honoured", n, at, second[n])
		}
	}
	// Nothing is left holding the run's lock: a third caller takes it at once.
	// Both statements on one acquired connection: a session lock taken
	// through the pool and unlocked through the pool may land on two
	// different connections, in which case the unlock is a no-op and the
	// lock lives until the pool closes. That is the exact leak the Migrate
	// comment describes, and not one this test should commit itself.
	conn, err := p.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, migrationLockKey).Scan(&got); err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !got {
		t.Fatal("the migration lock is still held after Migrate returned")
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

// TestIntegrationMigrateWaitsForAnotherServersRun is the rolling-deploy
// property: the second instance waits for the first rather than failing or
// applying the same file alongside it.
func TestIntegrationMigrateWaitsForAnotherServersRun(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)

	// Another instance, mid-run.
	other, err := p.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := other.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		t.Fatalf("hold the lock: %v", err)
	}
	const hold = 600 * time.Millisecond
	released := make(chan time.Time, 1)
	go func() {
		time.Sleep(hold)
		released <- time.Now()
		_ = other.Rollback(ctx)
	}()

	start := time.Now()
	if err := s.migrate(ctx, testPolicy); err != nil {
		t.Fatalf("Migrate while another run held the lock: %v", err)
	}
	if elapsed := time.Since(start); elapsed < hold {
		t.Errorf("Migrate finished in %s, before the other run released the lock at %s; it did not wait", elapsed, hold)
	}
	<-released
	if n := len(ledger(t, p)); n == 0 {
		t.Error("nothing was applied after the wait")
	}
}

// TestIntegrationMigrateGivesUpOnALockHeldPastItsWait is the other half:
// a run that is stuck must not stall the deploy forever, and the error must
// say what it waited for.
func TestIntegrationMigrateGivesUpOnALockHeldPastItsWait(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)

	other, err := p.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = other.Rollback(ctx) }()
	if _, err := other.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		t.Fatalf("hold the lock: %v", err)
	}

	policy := testPolicy
	policy.lockWait = 300 * time.Millisecond
	start := time.Now()
	err = s.migrate(ctx, policy)
	if err == nil {
		t.Fatal("Migrate succeeded while another run held the lock for good")
	}
	if !strings.Contains(err.Error(), "migration lock") || !strings.Contains(err.Error(), "another server") {
		t.Errorf("error does not say the lock was the problem: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("giving up took %s; the wait is not bounded by the policy", elapsed)
	}
	// Nothing ran: the run gave up before it even created the ledger, which
	// is the correct order (the lock comes first) and the table's absence is
	// the evidence.
	var exists bool
	if err := p.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatalf("check ledger: %v", err)
	}
	if exists {
		t.Error("the ledger was created without the lock")
	}
}

// holdTableLock takes ACCESS EXCLUSIVE on the table from a second connection
// for the duration, which is what a long ingest transaction looks like to a
// migration's ALTER TABLE. It reports when the lock was released.
func holdTableLock(t *testing.T, p *pgxpool.Pool, table string, hold time.Duration) <-chan struct{} {
	t.Helper()
	ctx := context.Background()
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE `+table+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock %s: %v", table, err)
	}
	done := make(chan struct{})
	go func() {
		time.Sleep(hold)
		_ = tx.Rollback(ctx)
		close(done)
	}()
	return done
}

// TestIntegrationMigrateRetriesAFileThatLostItsLockRace is the reason the
// runner has per-file transactions at all. The file's ALTER TABLE waits on a
// table an ingest transaction holds, times out at lock_timeout, and is run
// again once the table is free: it lands, on a later attempt, with its ledger
// row, inside the budget.
func TestIntegrationMigrateRetriesAFileThatLostItsLockRace(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)
	if err := s.migrationLedger(ctx); err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if _, err := p.Exec(ctx, `CREATE TABLE busy (x INT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Held for longer than one lock_timeout and shorter than the budget, so
	// the first attempt must fail and a later one must succeed.
	released := holdTableLock(t, p, "busy", 3*testPolicy.lockTimeout)

	start := time.Now()
	attempts, err := s.migrateFile(ctx, testPolicy, "9999_busy.sql", `ALTER TABLE busy ADD COLUMN y INT`)
	elapsed := time.Since(start)
	<-released
	if err != nil {
		t.Fatalf("migrateFile: %v (after %d attempts)", err, attempts)
	}
	if attempts < 2 {
		t.Errorf("the file landed on attempt %d; the lock was held for %s so the first attempt should have timed out", attempts, 3*testPolicy.lockTimeout)
	}
	if elapsed > testPolicy.budget {
		t.Errorf("the file took %s, over the %s budget", elapsed, testPolicy.budget)
	}
	var cols int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'busy' AND column_name = 'y'`).Scan(&cols); err != nil {
		t.Fatalf("read columns: %v", err)
	}
	if cols != 1 {
		t.Error("the retried file did not land")
	}
	if _, ok := ledger(t, p)["9999_busy.sql"]; !ok {
		t.Error("the retried file has no ledger row; it would run again at the next boot")
	}
}

// TestIntegrationMigrateFailsInsideItsBudgetWhenTheLockNeverFrees: a table
// that stays locked past the budget fails the boot with an error naming the
// file and the cause, within the budget, and leaves neither the change nor a
// ledger row behind.
func TestIntegrationMigrateFailsInsideItsBudgetWhenTheLockNeverFrees(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)
	if err := s.migrationLedger(ctx); err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if _, err := p.Exec(ctx, `CREATE TABLE stuck (x INT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	policy := testPolicy
	policy.budget = 900 * time.Millisecond
	released := holdTableLock(t, p, "stuck", 3*time.Second)

	start := time.Now()
	attempts, err := s.migrateFile(ctx, policy, "9999_stuck.sql", `ALTER TABLE stuck ADD COLUMN y INT`)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("migrateFile succeeded against a table that was never unlocked")
	}
	if !strings.Contains(err.Error(), "9999_stuck.sql") || !strings.Contains(err.Error(), "could not take its lock") {
		t.Errorf("error does not name the file and the cause: %v", err)
	}
	if attempts < 2 || attempts > policy.retries+1 {
		t.Errorf("attempts = %d, want between 2 and %d", attempts, policy.retries+1)
	}
	// Some slack over the budget for the last attempt's cancellation to be
	// observed, but nowhere near the 3 s the lock was held for.
	if elapsed > policy.budget+500*time.Millisecond {
		t.Errorf("giving up took %s against a %s budget", elapsed, policy.budget)
	}
	<-released
	var cols int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'stuck' AND column_name = 'y'`).Scan(&cols); err != nil {
		t.Fatalf("read columns: %v", err)
	}
	if cols != 0 {
		t.Error("a failed file left its change behind")
	}
	if _, ok := ledger(t, p)["9999_stuck.sql"]; ok {
		t.Error("a failed file left a ledger row behind; it would be skipped forever")
	}
}

// TestIntegrationWithStatementTimeoutIsLocalToTheTransaction: the override
// callers use for long passes must end with the transaction, or the next
// borrower of the connection inherits a ceiling it never asked for.
func TestIntegrationWithStatementTimeoutIsLocalToTheTransaction(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)

	var before string
	if err := p.QueryRow(ctx, `SHOW statement_timeout`).Scan(&before); err != nil {
		t.Fatalf("SHOW: %v", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := WithStatementTimeout(ctx, tx, 7*time.Second); err != nil {
		t.Fatalf("WithStatementTimeout: %v", err)
	}
	var inside string
	if err := tx.QueryRow(ctx, `SHOW statement_timeout`).Scan(&inside); err != nil {
		t.Fatalf("SHOW inside: %v", err)
	}
	if inside != "7s" {
		t.Errorf("statement_timeout inside the transaction = %q, want 7s", inside)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Every connection in the pool, not merely a fresh one: the override must
	// not have survived on the connection the transaction used.
	for range 8 {
		var after string
		if err := p.QueryRow(ctx, `SHOW statement_timeout`).Scan(&after); err != nil {
			t.Fatalf("SHOW after: %v", err)
		}
		if after != before {
			t.Fatalf("statement_timeout after the transaction = %q, want %q; SET LOCAL leaked", after, before)
		}
	}

	// Zero switches it off for the transaction, which is Postgres' own
	// meaning of 0 and what a pass bounded some other way asks for.
	tx, err = s.db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := WithStatementTimeout(ctx, tx, 0); err != nil {
		t.Fatalf("WithStatementTimeout(0): %v", err)
	}
	var off string
	if err := tx.QueryRow(ctx, `SHOW statement_timeout`).Scan(&off); err != nil {
		t.Fatalf("SHOW: %v", err)
	}
	if off != "0" {
		t.Errorf("statement_timeout after WithStatementTimeout(0) = %q, want 0", off)
	}
}

// TestIntegrationMigrateGivesTheBodyItsOwnCeilingNotTheLockBudget: the
// budget bounds the lock phase and nothing else. A file whose SQL runs
// longer than the whole lock budget, with no contention at all, lands on its
// first attempt. The runner used to put the budget's deadline over the body
// too, so such a file was cancelled on every boot with an error blaming
// ingest for a lock it never lost.
func TestIntegrationMigrateGivesTheBodyItsOwnCeilingNotTheLockBudget(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)
	if err := s.migrationLedger(ctx); err != nil {
		t.Fatalf("ledger: %v", err)
	}
	policy := testPolicy
	policy.budget = 300 * time.Millisecond
	start := time.Now()
	attempts, err := s.migrateFile(ctx, policy, "9999_slow.sql", `SELECT pg_sleep(1)`)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("a 1 s file under a %s lock budget failed after %d attempts: %v", policy.budget, attempts, err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1; nothing held a lock", attempts)
	}
	if elapsed < time.Second {
		t.Errorf("the file finished in %s, before its body could have; it was cut short", elapsed)
	}
	if _, ok := ledger(t, p)["9999_slow.sql"]; !ok {
		t.Error("the slow file has no ledger row")
	}
}

// TestIntegrationMigrateNamesTheCeilingWhenABodyRunsPastIt: a file slower
// than the statement ceiling fails once, is not retried (it is the file, not
// a lock loss, and would run past the ceiling again), and the error names
// the file and the ceiling. The operator is sent to the file; the old text
// sent them looking for an ingest transaction that did not exist.
func TestIntegrationMigrateNamesTheCeilingWhenABodyRunsPastIt(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)
	if err := s.migrationLedger(ctx); err != nil {
		t.Fatalf("ledger: %v", err)
	}
	policy := testPolicy
	policy.statementTimeout = 300 * time.Millisecond
	start := time.Now()
	attempts, err := s.migrateFile(ctx, policy, "9999_slower.sql", `SELECT pg_sleep(1)`)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a 1 s statement under a 300 ms ceiling succeeded")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1; a slow body is not a lock loss and must not be retried", attempts)
	}
	if sqlState(err) != "57014" {
		t.Errorf("error is not the statement timeout (57014): %v", err)
	}
	if !strings.Contains(err.Error(), "9999_slower.sql") || !strings.Contains(err.Error(), "statement ceiling") || !strings.Contains(err.Error(), "300ms") {
		t.Errorf("error does not name the file and the ceiling: %v", err)
	}
	if strings.Contains(err.Error(), "could not take its lock") {
		t.Errorf("a slow body is reported as a lock loss: %v", err)
	}
	if elapsed > policy.budget {
		t.Errorf("giving up took %s; the ceiling did not end the statement", elapsed)
	}
	if _, ok := ledger(t, p)["9999_slower.sql"]; ok {
		t.Error("a file that failed on its body left a ledger row behind")
	}
}

// TestIntegrationMigrationBodiesAreNotBoundByThePoolsStatementTimeout: the
// deployed pool sets statement_timeout on every connection (server/app,
// thirty seconds, sized for a request). A migration body runs under the
// policy's ceiling instead; without the override a file slower than a
// request would be cancelled by a number nothing in the migration code
// names, and the README's rules for a file would be wrong about what one
// may do.
func TestIntegrationMigrationBodiesAreNotBoundByThePoolsStatementTimeout(t *testing.T) {
	p := migratePoolWith(t, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = "300ms"
	})
	ctx := context.Background()
	var pooled string
	if err := p.QueryRow(ctx, `SHOW statement_timeout`).Scan(&pooled); err != nil {
		t.Fatalf("SHOW: %v", err)
	}
	if pooled != "300ms" {
		t.Fatalf("the pool's statement_timeout is %q; the test did not set what it meant to", pooled)
	}
	s := New(p, nil)
	if err := s.migrationLedger(ctx); err != nil {
		t.Fatalf("ledger: %v", err)
	}
	attempts, err := s.migrateFile(ctx, testPolicy, "9999_pool.sql", `SELECT pg_sleep(1)`)
	if err != nil {
		t.Fatalf("the pool's 300 ms statement_timeout cancelled a migration body (%d attempts): %v", attempts, err)
	}
	if _, ok := ledger(t, p)["9999_pool.sql"]; !ok {
		t.Error("the file has no ledger row")
	}
	// And the override ended with the file's transaction.
	var after string
	if err := p.QueryRow(ctx, `SHOW statement_timeout`).Scan(&after); err != nil {
		t.Fatalf("SHOW after: %v", err)
	}
	if after != "300ms" {
		t.Errorf("statement_timeout after the file = %q, want the pool's 300ms; SET LOCAL leaked", after)
	}
}

// TestIntegrationMigrateRefusesAPolicyWhoseCeilingIsInsideItsLockWait: a
// statement ceiling at or below lock_timeout would end every lock wait as a
// statement timeout and have it reported as a slow file. The runner refuses
// the policy before touching the database, so the mistake is a failed boot
// with a sentence rather than a misdiagnosis under contention.
func TestIntegrationMigrateRefusesAPolicyWhoseCeilingIsInsideItsLockWait(t *testing.T) {
	p := migratePool(t)
	ctx := context.Background()
	s := New(p, nil)
	policy := testPolicy
	policy.statementTimeout = policy.lockTimeout
	err := s.migrate(ctx, policy)
	if err == nil || !strings.Contains(err.Error(), "statementTimeout") {
		t.Fatalf("migrate accepted a ceiling inside the lock wait: %v", err)
	}
	var exists bool
	if err := p.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatalf("check ledger: %v", err)
	}
	if exists {
		t.Error("the ledger was created under a refused policy")
	}
}
