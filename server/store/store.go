// Package store is the persistence layer for loop-sessions.
//
// Three properties shape almost every decision in here.
//
// Events are the durable record and sessions is a rollup over them. Delivery
// from a laptop is at-least-once by construction, so every write path has to
// absorb a duplicate as a no-op rather than as a second count. That is why the
// event insert keys on the client's idempotency id, why token usage is credited
// through a ledger keyed by the identity of the model call rather than by the
// event that carried it, and why the rollup is folded forward from the events a
// statement actually inserted rather than recomputed from a session's history.
// Sessions reach hundreds of thousands of events and arrive in pieces over
// minutes; a rollup that re-read the history on every arrival would cost more
// with every batch, which is the wrong direction for a system whose whole value
// is that it keeps up.
//
// Event time and ingest time never share a column. Everything a reader sees is
// ordered by when it happened, and when it arrived is separate metadata, so a
// session imported from six months of history is indistinguishable from one
// captured live except for the arrival stamp.
//
// Authorization is the store's job, not the handler's. A session the viewer may
// not see is reported as absent, because a distinct "forbidden" answer confirms
// that the session exists and therefore leaks that a colleague ran something.
// The reads that expose a colleague's work take the viewer and write their own
// audit row inside the same transaction as the read, so a caller cannot forget
// to audit and an audit that fails takes the read down with it. Unaudited access
// to someone else's transcript is worse than a failed page load.
package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/normalize"
	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

// noRows reports the driver's "nothing matched" signal.
//
// Every authorized read leans on this: the authorization predicate is inside the
// SELECT, so a session the viewer may not see is returned as no rows and has to
// become ErrNotFound rather than an error the caller might render differently.
func noRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

//go:embed migrations/*.sql
var migrations embed.FS

// SyntheticModel is the model name Claude Code stamps on transcript records that
// never became an API call. They carry usage-shaped numbers that were never
// billed, so counting them would inflate every cost figure in the product.
const SyntheticModel = "<synthetic>"

// SearchCandidateCap bounds how many matching messages are ranked.
//
// ts_rank has to read the tsvector of every row it scores, so ranking is linear
// in the size of the match set and a common word across a fleet's corpus matches
// far too much of it. Filtering and capping first turns an unbounded scan into a
// bounded one, at the cost of ranking only the most recent slice of a very large
// match set, which is the trade a human searching their colleagues' work would
// make anyway.
const SearchCandidateCap = 500

// Errors the API layer is expected to translate. ErrNotFound deliberately covers
// both "no such session" and "not yours", because the two must be
// indistinguishable from outside; see the package comment.
var (
	// ErrNotFound means the row does not exist, or exists and the viewer has no
	// right to know that. It must become a 404 in both cases.
	ErrNotFound = errors.New("store: not found")

	// ErrNotAdmin means an administrative surface was reached by a member. It is
	// safe to return as a 403 because the resource is the admin page itself,
	// which everyone already knows exists; it must never be used for a session.
	ErrNotAdmin = errors.New("store: caller is not an admin")

	// ErrLastAdmin means the requested change would leave nobody able to
	// administer the system, which is unrecoverable without database access.
	ErrLastAdmin = errors.New("store: refusing to leave the system with no admin")

	// ErrOwnerMismatch means a write tried to attach data to a session that
	// belongs to a different principal. Session ids come from the client, so this
	// is the check that stops one enrolled laptop writing into another person's
	// transcript.
	ErrOwnerMismatch = errors.New("store: session belongs to another principal")

	// ErrInvalidCursor means a pagination cursor was not one we issued. Treated
	// as a client error rather than silently restarting from the first page,
	// because a silently restarted page looks to the user like duplicated data.
	ErrInvalidCursor = errors.New("store: invalid cursor")
)

// Rows is the part of a pgx result set this package consumes. It exists so the
// query-shaping logic can be exercised without a database: the properties worth
// testing here are which statement is issued and inside which transaction, and
// those do not need a server to observe.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// Row is a single-row result.
type Row interface {
	Scan(dest ...any) error
}

// Queryer is everything the store does against a connection, transaction or
// pool. Keeping it this narrow is what lets a transaction and a pool be passed
// to the same helper, which in turn is what makes "audit in the same
// transaction as the read" a structural property rather than a convention.
type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
}

// Tx is an open transaction.
type Tx interface {
	Queryer
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// DB is a pool that can start transactions.
type DB interface {
	Queryer
	Begin(ctx context.Context) (Tx, error)
}

// Pricer turns token usage into dollars at the rates in force when the call
// happened. It is an interface rather than a function on this package because
// the rate table changes and a stored cost has to stay reproducible: the caller
// owns the rates, the store owns making sure each model call is priced exactly
// once.
type Pricer interface {
	CostUSD(model string, u event.Usage, at time.Time) float64
}

// Store is the data access layer over a Postgres pool.
type Store struct {
	db     DB
	pricer Pricer
	// log receives the lines this package writes on its own account: the
	// migration runner's per-file line and the derive runner's step lines.
	// Nil means slog's default; the server sets its cloud logger at boot.
	log *slog.Logger

	// deployment is the allowed-domain list and the domain aliases the
	// server was configured with, set once at boot through SetDeployment
	// (deployment.go). The zero value allows no domain and aliases none.
	deployment Deployment

	// identityIdx caches whether events_record_identity_idx is valid, which
	// decides whether ingest may probe record identity at all. See
	// identityIndexReady.
	identityIdx struct {
		mu      sync.Mutex
		ready   bool
		checked time.Time
		// warned is set once the boot has said the index is not there, so
		// the WARNING is one per boot rather than one per batch.
		warned bool
	}

	// dirtyParked is the dirty pass's memory of sessions whose fold failed,
	// each with a backoff, so one unfoldable session cannot head the dirty
	// queue on every pass and starve the sessions behind it. See DeriveDirty.
	dirtyParked dirtyParking

	// AliasOracle confirms a skill name the derivation read from a typed
	// line rather than from the transcript's own envelope (skills.go,
	// design 4a): the shape rule reads prose as a command, so such a name
	// becomes a row only when the catalog knows it. Nil is no confirmation
	// for any name; the catalog PR sets it to ConfirmSkillAlias at
	// construction. A name from the envelope never consults it.
	//
	// It reads on the Queryer the derivation is already holding, never on
	// s.db: a read on the pool would take a SECOND connection while the
	// ingest transaction holds the first, and a burst of batches whose
	// candidates all consult the oracle would then hold two connections
	// each and wedge the pool against itself.
	AliasOracle func(ctx context.Context, q Queryer, plugin, skill string) (bool, error)
}

// dirtyParking is the dirty pass's memory of failed folds; see DeriveDirty.
type dirtyParking struct {
	mu   sync.Mutex
	rows map[string]parkedSession
}

// parkedSession is one session the dirty pass is leaving alone for a while.
type parkedSession struct {
	attempts int
	until    time.Time
}

// New builds a Store over a pgx pool.
func New(pool *pgxpool.Pool, pricer Pricer) *Store {
	return NewWithDB(poolDB{pool: pool}, pricer)
}

// NewWithDB builds a Store over any connection source satisfying DB. Tests use
// it to drive the query shapes without a server, and it leaves room to route
// reads at a replica later without changing every call site. The alias
// oracle is wired here (skill_catalog.go): both constructors leave it
// non-nil, so a typed command the shape rule read is confirmed against the
// catalog from the first boot that carries 0023.
func NewWithDB(db DB, pricer Pricer) *Store {
	s := &Store{db: db, pricer: pricer}
	s.AliasOracle = s.ConfirmSkillAlias
	return s
}

// SetLogger names the logger the store's own lines go to. Called once at
// boot, before Migrate, so the migration lines land in the same stream as
// everything else the server writes.
func (s *Store) SetLogger(l *slog.Logger) {
	s.log = l
}

func (s *Store) logger() *slog.Logger {
	if s.log == nil {
		return slog.Default()
	}
	return s.log
}

// migrationLockKey serialises schema migration across servers. A rolling deploy
// starts several processes at once and each runs migrations; without the lock
// two of them race on CREATE INDEX and one fails its startup for no reason.
const migrationLockKey int64 = 7266794526548561

// migratePolicy is the lock and retry budget a migration run works within.
//
// A type rather than constants so the integration tests can shrink every
// number and observe a retry in milliseconds; the deployed values are
// defaultMigratePolicy and nothing else constructs one.
type migratePolicy struct {
	// lockWait bounds how long a booting server waits for another server's
	// run to finish before it gives up its own boot. Cloud Run's startup probe
	// allows two minutes (cloudrun.yaml, failureThreshold 60 at 2 s); this
	// leaves the rest of that for the ping before and the files after.
	lockWait time.Duration
	// lockTimeout is the SET LOCAL lock_timeout inside each file transaction:
	// how long a file's ALTER TABLE waits for ingest to let go of the table
	// before the file is abandoned and retried. Five seconds is longer than
	// any ingest transaction and short enough that a retry is cheap.
	lockTimeout time.Duration
	// retries is how many times a file is re-run after a lock timeout.
	retries int
	// budget caps the wall clock one file may spend waiting for its lock,
	// across all its attempts: the last attempt's lock_timeout is clamped to
	// what is left of it, so a file that keeps losing the lock fails the boot
	// inside the budget rather than at retries x lockTimeout plus the sleeps.
	// It bounds the lock phase only. The file's own SQL runs under
	// statementTimeout, and a file whose body is slower than this budget is
	// not a lock loss and is not cut off by it.
	budget time.Duration
	// statementTimeout is the SET LOCAL statement_timeout inside each file
	// transaction: the ceiling on any one statement of the file's body. It
	// is there to override the pool's default, which is sized for a request
	// and would otherwise cancel a slower migration with nothing in this
	// file naming the number, and so that a file which runs past it fails
	// with an error that names the ceiling instead of one that blames ingest
	// for a lock the file never lost. Must exceed lockTimeout, or a lock
	// wait would end as a statement timeout and be misread as a slow body.
	// Zero switches it off, which is the Postgres meaning.
	statementTimeout time.Duration
	// sleep is the base of the jittered pause between attempts, doubled each
	// time. The jitter is what keeps two instances that timed out together
	// from retrying together.
	sleep time.Duration
}

var defaultMigratePolicy = migratePolicy{
	lockWait:    60 * time.Second,
	lockTimeout: 5 * time.Second,
	retries:     4,
	budget:      25 * time.Second,
	// Sixty seconds per statement. A statement that needs longer does not
	// fit a 120 s boot (cloudrun.yaml, startupProbe) beside the ping, the
	// lock wait and the other files, and belongs in a runner step; the
	// README's migration rules say so.
	statementTimeout: 60 * time.Second,
	sleep:            250 * time.Millisecond,
}

// Migrate applies every embedded migration that has not been applied yet.
//
// Each file is idempotent on its own, so the ledger is an optimisation and an
// audit trail rather than the thing that makes re-running safe. Both properties
// are wanted: the ledger tells an operator what a database has seen, and the
// idempotent SQL means a ledger that is somehow wrong cannot wedge a deploy.
//
// The shape is one advisory lock around the whole run and one transaction per
// file, with the file's ledger row inside it. It used to be one transaction
// for everything, which had two consequences that only show under live
// ingest: every ACCESS EXCLUSIVE lock any file took was held until the last
// file committed, so ingest stalled for the sum of the files rather than for
// each; and a lock_timeout in any file aborted the transaction, so nothing
// could be retried without starting the whole run again. Per-file
// transactions make a retry a retry of one file, and make the locks each file
// takes last exactly as long as that file.
//
// The lock is held on a connection of its own for the duration: a transaction
// opened first, holding only the transaction-scoped advisory lock, and rolled
// back last. A transaction rather than a session-level pg_advisory_lock on an
// acquired connection, for a failure mode that matters more than the wording:
// a session lock that is not released, because the process died between
// unlock and release or the unlock statement itself failed, stays held by a
// connection that goes back into the pool, and every later boot then waits on
// it and fails. A transaction that does not end cleanly is something pgxpool
// already refuses to return to the pool, so the lock can only outlive this
// function by outliving the connection, which is the property a session lock
// was supposed to provide. The visible cost: for as long as the run lasts the
// holder is one "idle in transaction" backend in pg_stat_activity, which the
// cloudsql runbook names so nobody reads it as a leak, and an
// idle_in_transaction_session_timeout on the instance would terminate it
// mid-run; the instance sets none (examples/deploy-gcp/README.md#runbook-migrations).
func (s *Store) Migrate(ctx context.Context) error {
	return s.migrate(ctx, defaultMigratePolicy)
}

func (s *Store) migrate(ctx context.Context, p migratePolicy) error {
	// Checked before anything touches the database: a policy whose statement
	// ceiling is inside its lock wait would end every lock wait as a
	// statement timeout, and migrateFile would report each one as a slow
	// file instead of retrying it.
	if p.statementTimeout > 0 && p.statementTimeout <= p.lockTimeout {
		return fmt.Errorf("store: migration policy: statementTimeout %s must exceed lockTimeout %s", p.statementTimeout, p.lockTimeout)
	}
	release, err := s.migrationLock(ctx, p)
	if err != nil {
		return err
	}
	defer release()

	// The ledger table is created in a transaction of its own, committed
	// before any file runs. It cannot live in the lock transaction: that one
	// is rolled back at the end, and a table created inside it would be
	// invisible to the file transactions and gone afterwards.
	if err := s.migrationLedger(ctx); err != nil {
		return err
	}

	names, err := migrationNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		body, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("store: read migration %s: %w", name, err)
		}
		started := time.Now()
		attempts, applied, err := s.migrateFileApplied(ctx, p, name, string(body))
		if err != nil {
			return err
		}
		// One line per file that actually ran, with what it cost. Files the
		// ledger already held are silent: a boot that applies nothing is the
		// ordinary boot, and a line per skipped file would bury the one that
		// matters. The clone rehearsal on 2026-09-11 found the runner applying
		// files with no trace at all, which is what this line ends.
		if applied {
			s.logger().InfoContext(ctx, "migration applied",
				slog.String("file", name),
				slog.Int("attempts", attempts),
				slog.Float64("seconds", time.Since(started).Seconds()))
		}
	}
	return nil
}

// migrationLock takes the advisory lock for the whole run and returns what
// releases it. See Migrate for why the lock lives in a transaction.
func (s *Store) migrationLock(ctx context.Context, p migratePolicy) (func(), error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: begin migration lock: %w", err)
	}
	// The wait is bounded by the policy rather than by the pool's default
	// statement_timeout, which is sized for a request and is shorter than
	// another instance's run may legitimately take. SET LOCAL, so the override
	// ends with this transaction. lock_timeout is switched off for the same
	// statement: waiting for the lock is the whole point of it.
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", p.lockWait.Milliseconds())); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("store: bound the migration lock wait: %w", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = 0`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("store: bound the migration lock wait: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		_ = tx.Rollback(ctx)
		if sqlState(err) == "57014" {
			return nil, fmt.Errorf("store: migration lock: another server has held it for longer than %s; its run is stuck or still going: %w", p.lockWait, err)
		}
		return nil, fmt.Errorf("store: migration lock: %w", err)
	}
	// Background rather than ctx for the rollback: a boot cancelled mid-run
	// must still release the lock, and a cancelled context would refuse to.
	return func() { _ = tx.Rollback(context.Background()) }, nil
}

// migrationLedger creates the ledger table if it is missing, in its own
// committed transaction so the file transactions that follow can see it.
func (s *Store) migrationLedger(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin migration ledger: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name       TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("store: migration ledger: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit migration ledger: %w", err)
	}
	return nil
}

// migrateFile applies one file, retrying it when it lost a lock race, and
// reports how many attempts it took.
//
// Only a lock timeout is retried. A statement that ran past the policy's
// ceiling is named as such and not retried: it is the file, and it would run
// past it again. Any other failure is the file itself too and will fail the
// same way again; retrying either would spend the boot budget on a certainty.
// The attempt count is returned for the tests, which are the only caller
// that can see the difference between "applied" and "applied on the third
// try".
func (s *Store) migrateFile(ctx context.Context, p migratePolicy, name, body string) (attempts int, err error) {
	attempts, _, err = s.migrateFileApplied(ctx, p, name, body)
	return attempts, err
}

// migrateFileApplied is migrateFile reporting, in addition, whether the file
// ran at all: false when the ledger already held it. The migration log line
// is written only for files that ran, and only this function knows.
func (s *Store) migrateFileApplied(ctx context.Context, p migratePolicy, name, body string) (attempts int, applied bool, err error) {
	deadline := time.Now().Add(p.budget)
	gaveUp := func(err error) error {
		return fmt.Errorf("store: migration %s could not take its lock within %s (%d attempts); ingest is holding the table it alters, see examples/deploy-gcp/README.md#runbook-migrations: %w",
			name, p.budget, attempts, err)
	}
	for attempt := 0; ; attempt++ {
		attempts = attempt + 1
		applied, err = s.applyMigration(ctx, p, deadline, name, body)
		if err == nil {
			return attempts, applied, nil
		}
		// The two ways Postgres ends an attempt on time are told apart by
		// their codes, and the distinction is the whole diagnosis: 55P03 is a
		// lock this file lost to ingest, which frees itself; 57014 is the
		// file's own SQL running past the ceiling, which does not.
		switch sqlState(err) {
		case "55P03":
		case "57014":
			return attempts, false, fmt.Errorf("store: migration %s ran past the %s statement ceiling (store.defaultMigratePolicy); split the file or move the work to a runner step, see examples/deploy-gcp/README.md#runbook-migrations: %w",
				name, p.statementTimeout, err)
		default:
			return attempts, false, err
		}
		if attempt >= p.retries || !time.Now().Before(deadline) {
			return attempts, false, gaveUp(err)
		}
		pause := jitteredBackoff(p.sleep, attempt)
		if time.Now().Add(pause).After(deadline) {
			return attempts, false, gaveUp(err)
		}
		select {
		case <-ctx.Done():
			return attempts, false, fmt.Errorf("store: migration %s: %w", name, ctx.Err())
		case <-time.After(pause):
		}
	}
}

// applyMigration is one attempt at one file: check the ledger, run the body,
// record the row, commit. All inside a single transaction, with the lock
// wait bounded by what is left of the file's budget and each statement of
// the body by the policy's ceiling.
//
// Both bounds are Postgres' own, set with SET LOCAL, and there is no context
// deadline over the attempt. There used to be one, derived from the lock
// budget, and it covered the body as well as the wait: a file whose SQL took
// longer than the budget was cancelled on every boot, with an error blaming
// ingest for a lock it never lost, and cancelling through the context closes
// the connection mid-statement where a server-side timeout ends it cleanly.
func (s *Store) applyMigration(ctx context.Context, p migratePolicy, deadline time.Time, name, body string) (applied bool, err error) {
	// The lock wait for this attempt: the policy's, or what is left of the
	// budget if that is shorter. Clamping here means the last attempt ends
	// the way the others did, with Postgres' own lock_timeout error, inside
	// the budget. Never below a millisecond, because 0 switches the wait off.
	lockTimeout := p.lockTimeout
	if remaining := time.Until(deadline); remaining < lockTimeout {
		lockTimeout = remaining
	}
	if lockTimeout < time.Millisecond {
		lockTimeout = time.Millisecond
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("store: begin migration %s: %w", name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Checked inside the file's own transaction, under the run-wide lock, so a
	// retried file re-reads the ledger and a file another server applied
	// while this one was waiting for the lock is skipped rather than re-run.
	var already bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name,
	).Scan(&already); err != nil {
		return false, fmt.Errorf("store: check migration %s: %w", name, err)
	}
	if already {
		return false, nil
	}
	// The wait for any table lock the file needs. Without it an ALTER TABLE
	// queues behind every ingest transaction and every ingest transaction
	// queues behind the ALTER, for as long as the busiest table stays busy.
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockTimeout.Milliseconds())); err != nil {
		return false, fmt.Errorf("store: bound migration %s: %w", name, err)
	}
	// The ceiling on the body's own statements, in place of the pool's
	// request-sized default, which would otherwise apply here unnamed.
	if err := WithStatementTimeout(ctx, tx, p.statementTimeout); err != nil {
		return false, fmt.Errorf("store: bound migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, body); err != nil {
		return false, fmt.Errorf("store: apply migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, name,
	); err != nil {
		return false, fmt.Errorf("store: record migration %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("store: commit migration %s: %w", name, err)
	}
	return true, nil
}

// jitteredBackoff doubles the base per attempt and adds up to half of that
// again, from the clock rather than a seeded generator: the point is that two
// instances which failed at the same instant do not retry at the same
// instant, and nanoseconds of drift between them are enough for that.
func jitteredBackoff(base time.Duration, attempt int) time.Duration {
	pause := base << uint(attempt)
	if pause <= 0 {
		return base
	}
	return pause + time.Duration(time.Now().UnixNano()%int64(pause/2+1))
}

// sqlState reports the Postgres error code behind err, or "" when there is
// none. Read through the driver's own accessor rather than by importing its
// error type, which keeps this file's dependence on pgx to the pool it wraps.
func sqlState(err error) string {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState()
	}
	return ""
}

// WithStatementTimeout overrides the pool's default statement_timeout for the
// rest of tx.
//
// The pool sets thirty seconds on every connection (server/app, poolConfig),
// which is the right ceiling for a request and the wrong one for the passes
// that legitimately run longer: the derive runner's body-reading steps and
// the export job's per-partition loads. Those take what they need here, and
// because it is SET LOCAL the override ends with the transaction instead of
// riding the connection back into the pool for the next request to inherit.
// Zero or negative switches the timeout off for the transaction, which is the
// Postgres meaning of 0 and is deliberate for a pass that is bounded some
// other way.
func WithStatementTimeout(ctx context.Context, tx Tx, d time.Duration) error {
	ms := d.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", ms)); err != nil {
		return fmt.Errorf("store: set statement_timeout: %w", err)
	}
	return nil
}

func migrationNames() ([]string, error) {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("store: list migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	// Lexical order is deployment order because the files are numbered.
	sort.Strings(names)
	return names, nil
}

// ---------------------------------------------------------------------------
// Identity and authorization
// ---------------------------------------------------------------------------

// Role is what a principal may do. There are two, because the manager hierarchy
// this replaced needed a directory integration to answer a question that a
// four-row table answers just as well.
type Role string

// The two roles. A member sees their own work plus whatever has been shared
// with them; an admin sees everything, audited.
const (
	RoleAdmin  Role = "admin"
	RoleMember Role = "member"
)

// Viewer is the authenticated identity a read is performed on behalf of. It is
// a required argument on every method that can expose someone else's work, so
// that "who is asking" is impossible to omit at a call site.
type Viewer struct {
	Email string
	Role  Role
}

// IsAdmin reports whether the viewer sees everything.
func (v Viewer) IsAdmin() bool { return v.Role == RoleAdmin }

// Principal is a row of the access list.
type Principal struct {
	Email       string     `json:"email"`
	Role        Role       `json:"role"`
	DisplayName string     `json:"display_name,omitempty"`
	AddedBy     string     `json:"added_by,omitempty"`
	AddedAt     time.Time  `json:"added_at"`
	DisabledAt  *time.Time `json:"disabled_at,omitempty"`
}

// Viewer converts a principal into the identity reads are performed under.
func (p Principal) Viewer() Viewer { return Viewer{Email: p.Email, Role: p.Role} }

// canRead renders the authorization predicate from the contract against a table
// alias exposing email and session_id.
//
// It is generated from one place on purpose. This predicate is the only thing
// standing between a member and their colleagues' transcripts, and three
// hand-written copies of it in three queries is three chances for one of them to
// drift into being subtly more permissive than the other two.
func canRead(alias, adminParam, viewerParam string) string {
	return fmt.Sprintf(`(%[2]s OR %[1]s.email = %[3]s OR EXISTS (
		SELECT 1 FROM shares sh
		WHERE sh.session_id = %[1]s.session_id
		  AND sh.revoked_at IS NULL
		  AND (sh.expires_at IS NULL OR sh.expires_at > now())
		  AND (sh.grantee IS NULL OR sh.grantee = %[3]s)))`, alias, adminParam, viewerParam)
}

// viaFor names how an allowed read was justified, following the contract's
// order: your own work first, then the admin role, then an explicit share.
func viaFor(v Viewer, owner string) string {
	switch {
	case owner == v.Email:
		return AccessViaOwn
	case v.IsAdmin():
		return AccessViaAdmin
	default:
		return AccessViaShare
	}
}

// How an access was justified, as recorded in the audit log.
const (
	AccessViaOwn   = "own"
	AccessViaAdmin = "admin"
	AccessViaShare = "share"
)

// Access is one audited read of a session.
type Access struct {
	// ID is the row's primary key, surfaced because the admin access log pages
	// by keyset and has nothing else stable to page on: `at` is not unique, so
	// a timestamp cursor either repeats or skips rows written in the same
	// instant. A zero here makes the caller emit a cursor of 0, which its own
	// validation then rejects, and the log silently ends at page one.
	ID        int64     `json:"id"`
	Viewer    string    `json:"viewer"`
	SessionID string    `json:"session_id"`
	Owner     string    `json:"owner"`
	Via       string    `json:"via"`
	At        time.Time `json:"at"`
}

// RecordAccess writes one audit row.
//
// The audited readers on this type call it for you inside their own
// transaction, which is where it belongs; this exported form exists for a path
// that hands a colleague's transcript to someone from outside the store, such as
// assembling a resume bundle. Reaching for it means the audit is no longer
// atomic with the read, so prefer the readers.
func (s *Store) RecordAccess(ctx context.Context, a Access) error {
	if a.Via == AccessViaOwn {
		return nil
	}
	return recordAccess(ctx, s.db, []Access{a})
}

// recordAccess writes audit rows for every non-own access in one statement.
// Callers pass the transaction that performed the read, which is what makes a
// failed audit fail the read.
func recordAccess(ctx context.Context, q Queryer, as []Access) error {
	viewers := make([]string, 0, len(as))
	sessionIDs := make([]string, 0, len(as))
	owners := make([]string, 0, len(as))
	vias := make([]string, 0, len(as))
	for _, a := range as {
		if a.Via == AccessViaOwn || a.SessionID == "" {
			continue
		}
		viewers = append(viewers, a.Viewer)
		sessionIDs = append(sessionIDs, a.SessionID)
		owners = append(owners, a.Owner)
		vias = append(vias, a.Via)
	}
	if len(viewers) == 0 {
		return nil
	}
	_, err := q.Exec(ctx, `
		INSERT INTO access_log (viewer, session_id, owner, via)
		SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])`,
		viewers, sessionIDs, owners, vias)
	if err != nil {
		return fmt.Errorf("store: record access: %w", err)
	}
	return nil
}

// AccessLogFilter narrows the audit trail. The two dimensions that matter are
// the two questions people ask of it: what did this person read, and who read
// this session.
type AccessLogFilter struct {
	SessionID string
	Viewer    string
	Owner     string
	// Via narrows to how the read was authorised, for example an ownership
	// read against a share-link read. The admin UI offers it as a filter, and a
	// filter that is accepted and then ignored is worse than one that does not
	// exist: the operator reads the narrowed page as the whole answer.
	Via string
	// Since is inclusive and Until exclusive, so adjacent windows tile without
	// double-counting the row on the boundary.
	Since time.Time
	Until time.Time
	// Before is a keyset cursor: return rows with a lower id than this. Zero
	// means the first page. Offset paging would repeat and drop rows here,
	// because the access log grows while somebody is reading it and every new
	// row lands at the top of the exact ordering being paged.
	Before int64
	Limit  int
}

// ListAccessLog returns audit rows newest first, for the admin page.
func (s *Store) ListAccessLog(ctx context.Context, v Viewer, f AccessLogFilter) ([]Access, error) {
	if !v.IsAdmin() {
		return nil, ErrNotAdmin
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, viewer, session_id, owner, via, at
		FROM access_log
		WHERE ($1 = '' OR session_id = $1)
		  AND ($2 = '' OR viewer = $2)
		  AND ($3 = '' OR owner = $3)
		  AND ($4::timestamptz IS NULL OR at >= $4)
		  AND ($5::timestamptz IS NULL OR at < $5)
		  AND ($6 = '' OR via = $6)
		  AND ($7 = 0 OR id < $7)
		ORDER BY at DESC, id DESC
		LIMIT $8`,
		f.SessionID, f.Viewer, f.Owner, nullTime(f.Since), nullTime(f.Until), f.Via, f.Before, clampLimit(f.Limit))
	if err != nil {
		return nil, fmt.Errorf("store: list access log: %w", err)
	}
	defer rows.Close()

	var out []Access
	for rows.Next() {
		var a Access
		if err := rows.Scan(&a.ID, &a.Viewer, &a.SessionID, &a.Owner, &a.Via, &a.At); err != nil {
			return nil, fmt.Errorf("store: scan access log: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

// Ingest is one spooled event plus the attribution the server derives from the
// credential that delivered it. The email is never taken from the payload: a
// device token proves who is uploading, and the body is whatever the client
// chose to send.
type Ingest struct {
	Event event.Event

	// Email is the principal the delivering device belongs to.
	Email string
	// DeviceID is the enrolled machine, empty when unknown.
	DeviceID string
	// Repo is the repository the session's working directory resolves to. It is
	// derived by the caller because it needs knowledge of the checkout layout
	// that the database does not have.
	Repo string
	// Body is the scrubbed record exactly as delivered. Storing the delivered
	// bytes rather than a re-marshalled struct means a parser change later can
	// be replayed against what actually arrived. Empty falls back to marshalling
	// Event.
	Body json.RawMessage
	// AgentVersion is the build of the client that delivered the batch, read
	// from the request's User-Agent by the adapter. It is recorded per session
	// because the question it answers is "which client builds captured this
	// session", the cohort key for whether a fix to the capture path reached
	// a machine. Empty when the adapter had nothing to say.
	AgentVersion string
}

// Rejected is an event that will never be accepted, with the reason. Rejection
// is permanent: the client stops retrying on it, so it must only be used for
// things that cannot become true later.
type Rejected struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// UpsertResult reports the fate of a batch.
//
// Inserted and Duplicate are split because the two audiences differ. The client
// dedups on our answer and must treat both as accepted, otherwise a retried
// batch is retried forever. Metrics must count only Inserted, otherwise a
// network flap looks like a burst of activity.
type UpsertResult struct {
	Inserted  []string   `json:"inserted"`
	Duplicate []string   `json:"duplicate"`
	Rejected  []Rejected `json:"rejected"`
	// Sessions lists the session ids whose rollup moved, for cache invalidation.
	Sessions []string `json:"sessions"`
}

// Accepted is everything the client may delete from its spool.
func (r UpsertResult) Accepted() []string {
	out := make([]string, 0, len(r.Inserted)+len(r.Duplicate))
	out = append(out, r.Inserted...)
	return append(out, r.Duplicate...)
}

// UpsertEvents stores a batch idempotently and folds it into the session
// rollups, in one transaction.
//
// The whole batch is one statement per table rather than one statement per
// event: a laptop that has been offline delivers thousands at a time, and a
// round trip per event would make catching up slower than falling behind.
//
// Only the events this call actually inserted are folded into a rollup. That is
// the difference between an idempotent ingest and one that merely does not
// error on a duplicate: re-delivering a batch must not move a single counter.
//
// A failed call returns a zero result. Everything after the transaction opens
// rolls back together, so a partly filled result travelling with an error would
// tell the client to delete spool items that were never stored.
func (s *Store) UpsertEvents(ctx context.Context, items []Ingest) (UpsertResult, error) {
	var res UpsertResult
	if len(items) == 0 {
		return res, nil
	}

	// Deduplicating inside the batch first keeps the newly-inserted set
	// unambiguous. A client that re-reads a truncated transcript legitimately
	// produces the same id twice in one upload.
	seen := make(map[string]bool, len(items))
	kept := make([]Ingest, 0, len(items))
	for _, it := range items {
		if reason := validateIngest(it); reason != "" {
			res.Rejected = append(res.Rejected, Rejected{ID: it.Event.ID, Reason: reason})
			continue
		}
		if seen[it.Event.ID] {
			res.Duplicate = append(res.Duplicate, it.Event.ID)
			continue
		}
		seen[it.Event.ID] = true
		kept = append(kept, it)
	}
	if len(kept) == 0 {
		return res, nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return UpsertResult{}, fmt.Errorf("store: begin ingest: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Claim each session before anything is written under it. The claim takes
	// the row lock, so two devices racing to create the same session id resolve
	// to one owner rather than to a half-merged transcript, and the losing
	// device's events are rejected instead of being filed under the winner.
	//
	// Claims are taken in session-id order whatever order the batch arrived
	// in. The lock is held to commit, and two batches that touch the same two
	// sessions in opposite orders would each wait on the other's first claim:
	// a deadlock Postgres breaks after deadlock_timeout by failing one of
	// them, which the client sees as a 503 and a whole-batch retry. One
	// global order turns the wait into a queue. The sort is stable, so a
	// session's own events keep the order they were delivered in.
	sort.SliceStable(kept, func(i, j int) bool { return kept[i].Event.SessionID < kept[j].Event.SessionID })
	owners := make(map[string]string, 4)
	admitted := make([]Ingest, 0, len(kept))
	for _, it := range kept {
		sid := it.Event.SessionID
		owner, ok := owners[sid]
		if !ok {
			owner, err = claimSession(ctx, tx, it)
			if err != nil {
				return UpsertResult{}, err
			}
			owners[sid] = owner
		}
		if owner != it.Email {
			res.Rejected = append(res.Rejected, Rejected{
				ID:     it.Event.ID,
				Reason: "session belongs to another principal",
			})
			continue
		}
		admitted = append(admitted, it)
	}
	if len(admitted) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return UpsertResult{}, fmt.Errorf("store: commit ingest: %w", err)
		}
		return res, nil
	}

	inserted, held, upgraded, err := s.insertEvents(ctx, tx, admitted)
	if err != nil {
		return UpsertResult{}, err
	}

	fresh := make([]Ingest, 0, len(inserted))
	for _, it := range admitted {
		if inserted[it.Event.ID] {
			fresh = append(fresh, it)
			res.Inserted = append(res.Inserted, it.Event.ID)
		} else {
			res.Duplicate = append(res.Duplicate, it.Event.ID)
		}
	}
	// What the ledger, the messages refresh and the rollup see: the batch as
	// the database holds it, one item per stored row. The client is
	// acknowledged on the id it sent (above); the database is credited on
	// the row it holds.
	stored := collapseByStoredID(admitted, held)

	// Launcher evidence is read from the whole admitted batch, fresh or not. A
	// re-walk re-delivers a session's events with better extraction; its rows
	// upgrade in place rather than count as fresh, but the entrypoint they
	// carry still has to reach the rollup, or a session misclassified before
	// the field existed stays misclassified forever. Safe where folding
	// counters from duplicates would not be: the merge only ever promotes.
	auto := automationSessions(admitted)

	// Usage is credited from the whole admitted batch too, for the same
	// reason and with the same safety: the ledger is keyed by the model call,
	// so a re-delivery credits nothing and a row upgraded in place with the
	// usage its first delivery lacked (a Codex turn re-walked by a client that
	// learned to read token_count records) credits exactly once. Crediting
	// only fresh rows was what left 1,362 Codex sessions at zero cost after
	// the fleet had the extraction that could have priced them.
	counted, err := s.creditUsage(ctx, tx, stored)
	if err != nil {
		return UpsertResult{}, err
	}

	if len(fresh) > 0 {
		if err := writeMessages(ctx, tx, fresh, false); err != nil {
			return UpsertResult{}, err
		}
		// Derived in the same transaction as the events they come from, and
		// only from freshly inserted ones. Both properties matter: an artifact
		// whose event never landed would violate its foreign key, and a link
		// counted from a re-delivered event would turn "mentioned twice" into
		// "the client retried twice".
		if err := insertArtifacts(ctx, tx, fresh); err != nil {
			return UpsertResult{}, err
		}
		if err := insertLinks(ctx, tx, fresh); err != nil {
			return UpsertResult{}, err
		}
		// Skill invocations come from the same fresh rows, under a savepoint
		// with its own lock bound, so a skill row never fails an events
		// batch (skills.go, design 4a): a failure rolls the skill work back,
		// the events commit, and the sessions queue for the dirty tick.
		s.deriveSkillsAtIngest(ctx, tx, fresh)
	}

	// A row whose body moved to a newer extraction, by id or by record
	// identity, carries new text: its messages row is rewritten so search,
	// the title and the fold's text hash see the extraction the row now
	// holds, and its session is marked dirty below so the fold runs again.
	// Before this, a batch that only upgraded rows left the session clean and
	// the corpus on the first extraction until the next versioned pass
	// (review-1 finding 10).
	var moved []Ingest
	touched := map[string]bool{}
	for _, it := range stored {
		if upgraded[it.Event.ID] {
			moved = append(moved, it)
			touched[it.Event.SessionID] = true
		}
	}
	if len(moved) > 0 {
		if err := writeMessages(ctx, tx, moved, true); err != nil {
			return UpsertResult{}, err
		}
	}

	deltas := foldDeltas(fresh, counted)
	for sid := range deltas {
		deltas[sid].Automation = auto[sid]
	}
	// Tokens the ledger accepted on rows that were not fresh reach the rollup
	// through an increment of their own: the row was stored before, its cost
	// was not, and the fold above only sees fresh rows. The increment carries
	// the event's own time as its bounds so the LEAST/GREATEST merge cannot
	// move a session's dates anywhere its events were not. stored holds each
	// row once, so a batch that named one row twice adds its one credit once
	// (adversarial finding 2).
	for _, it := range stored {
		if inserted[it.Event.ID] {
			continue
		}
		c, ok := counted[it.Event.ID]
		if !ok {
			continue
		}
		d := deltas[it.Event.SessionID]
		if d == nil {
			d = &SessionDelta{
				SessionID: it.Event.SessionID,
				Email:     it.Email,
				DeviceID:  it.DeviceID,
				Source:    string(it.Event.Source),
				StartedAt: it.Event.OccurredAt,
				EndedAt:   it.Event.OccurredAt,
			}
			deltas[it.Event.SessionID] = d
		}
		d.TokensInput += c.Usage.InputTokens
		d.TokensOutput += c.Usage.OutputTokens
		d.TokensCacheRead += c.Usage.CacheReadTokens
		if write := c.Usage.Ephemeral5m + c.Usage.Ephemeral1h; write > 0 {
			d.TokensCacheWrite += write
		} else {
			d.TokensCacheWrite += c.Usage.CacheCreationTokens
		}
		d.CostUSD += c.CostUSD
	}
	// Evidence can arrive on a re-delivered event for a session whose fresh
	// rows are in some other batch entirely.
	for _, sid := range sortedKeys(auto) {
		if _, ok := deltas[sid]; !ok {
			if err := markAutomation(ctx, tx, sid); err != nil {
				return UpsertResult{}, err
			}
		}
	}
	// Upgraded rows for a session with no delta of its own this batch: the
	// delta path sets derive_dirty; this is the other path to the fold.
	var redo []string
	for _, sid := range sortedKeys(touched) {
		if _, ok := deltas[sid]; !ok {
			redo = append(redo, sid)
		}
	}
	if len(redo) > 0 {
		// updated_at moves with the mark, and the mark is written whether or
		// not the row is already dirty. The fold clears derive_dirty only
		// when the row's updated_at still equals the one it read before
		// folding (deriveOne); a mark that left updated_at alone, or matched
		// nothing because the row was dirty under a fold in flight, was
		// cleared by that fold and the new extraction waited for the next
		// versioned pass (adversarial finding 3). The delta path moves
		// updated_at in applyDelta for the same reason.
		if _, err := tx.Exec(ctx, `UPDATE sessions SET derive_dirty = true, updated_at = now() WHERE session_id = ANY($1::text[])`, redo); err != nil {
			return UpsertResult{}, fmt.Errorf("store: mark upgraded sessions dirty: %w", err)
		}
	}
	for _, sid := range sortedKeys(deltas) {
		d := deltas[sid]
		if err := applyDelta(ctx, tx, d); err != nil {
			return UpsertResult{}, err
		}
		// The title is re-derived when the batch carried a prompt, and when
		// it carried launcher evidence: evidence that trails the prompts
		// promotes a session whose only prompts were wrappers, and an
		// automation session is named by its first prompt of any kind, so
		// the promotion is what makes a stored row eligible to be the title.
		if d.SawUserPrompt || d.Automation {
			if err := refreshFirstPrompt(ctx, tx, d); err != nil {
				return UpsertResult{}, err
			}
		}
		res.Sessions = append(res.Sessions, sid)
	}

	if err := tx.Commit(ctx); err != nil {
		return UpsertResult{}, fmt.Errorf("store: commit ingest: %w", err)
	}
	return res, nil
}

// collapseByStoredID is the admitted batch as the database holds it: every
// id that matched a stored record identity replaced by the rows that
// identity holds, and one item per stored row where the batch carried
// several copies of one record (the walker's rare double re-walked, or a
// re-walked copy beside the id it re-walked). The copy kept per row is the
// one insertEvents applied, elected by the rule it uses: the highest
// capture version, ties to the last in batch order. One item per row is
// what lets the messages refresh name each row once, which ON CONFLICT DO
// UPDATE requires, and the rollup add each ledger credit once (adversarial
// findings 1 and 2). An identity the rare double left as two rows yields
// two items carrying the same copy, one per row, so both rows are
// refreshed (review-4 finding N2); the ledger keys them by the one model
// call and prices it once. Rows keep the order of their first sighting.
// Without identity matches the batch is already one item per row, because
// UpsertEvents deduplicated its ids.
func collapseByStoredID(admitted []Ingest, held map[string][]string) []Ingest {
	if len(held) == 0 {
		return admitted
	}
	out := make([]Ingest, 0, len(admitted))
	at := make(map[string]int, len(admitted))
	place := func(it Ingest) {
		i, seen := at[it.Event.ID]
		if !seen {
			at[it.Event.ID] = len(out)
			out = append(out, it)
			return
		}
		if it.Event.CaptureVersion >= out[i].Event.CaptureVersion {
			out[i] = it
		}
	}
	for _, it := range admitted {
		ids, ok := held[it.Event.ID]
		if !ok {
			place(it)
			continue
		}
		for _, id := range ids {
			it.Event.ID = id
			place(it)
		}
	}
	return out
}

// validateIngest returns the permanent reason an event cannot be stored, or the
// empty string. Only conditions that can never become true later belong here,
// because the client treats a rejection as final and deletes the item.
func validateIngest(it Ingest) string {
	switch {
	case it.Event.ID == "":
		return "missing event id"
	case it.Event.SessionID == "":
		return "missing session id"
	case len(it.Event.ID) > derive.KeyMaxBytes:
		// Both ids are btree entries (events_pkey, sessions_pkey) and one
		// over the entry ceiling fails the whole batch it arrived in, on
		// every re-send. The bound is the key columns' bound because the
		// usage ledger's stand-in key is "event:" plus this id. Every id a
		// client makes is a hash or a uuid, far under it.
		return "event id too long"
	case len(it.Event.SessionID) > derive.KeyMaxBytes:
		return "session id too long"
	case it.Event.Type == "":
		return "missing event type"
	case it.Event.Origin == "":
		return "missing event origin"
	case it.Event.Source == "":
		return "missing event source"
	case it.Event.OccurredAt.IsZero():
		return "missing occurred_at"
	case it.Email == "":
		return "unattributed event"
	case len(it.Body) > 0 && !json.Valid(it.Body):
		// Caught here rather than at the insert, because the insert is one
		// statement for the whole batch and Postgres would reject all of it
		// over one malformed body.
		return "body is not valid JSON"
	}
	return ""
}

// claimSession establishes who owns a session id and returns the owner. The
// insert is the claim; the conflict path pulls the session's start time back if
// this batch carries something earlier, which is the ordinary case for a
// backfill arriving after live capture has already created the row.
func claimSession(ctx context.Context, q Queryer, it Ingest) (string, error) {
	var owner string
	err := q.QueryRow(ctx, `
		INSERT INTO sessions (session_id, email, device_id, source, started_at)
		VALUES ($1, $2, nullif($3,'')::uuid, $4, $5)
		ON CONFLICT (session_id) DO UPDATE
		SET started_at = LEAST(sessions.started_at, EXCLUDED.started_at)
		RETURNING email`,
		it.Event.SessionID, it.Email, it.DeviceID, string(it.Event.Source), it.Event.OccurredAt,
	).Scan(&owner)
	if err != nil {
		return "", fmt.Errorf("store: claim session %s: %w", it.Event.SessionID, err)
	}
	return owner, nil
}

// eventKeys is one event's identity columns as they are written: the keys
// KeysOf found, with the request id kept beside the message id for the
// row even though the usage ledger keys on the message id alone.
type eventKeys struct {
	prompt, record, parentRecord, request, message, toolUse string
}

func keysFor(it Ingest) eventKeys {
	k := derive.KeysOf(it.Event, it.Event.Raw)
	return eventKeys{
		prompt: k.PromptID, record: k.RecordUUID, parentRecord: k.ParentRecordUUID,
		request: k.RequestID, message: k.MessageID, toolUse: k.ToolUseID,
	}
}

// identityIndexReadyEvery is how long a negative answer about the identity
// index is believed before the catalog is asked again. The index is built
// once by the runner, after which the answer never changes; until then a
// probe per batch would be a catalog read on every upload for nothing.
const identityIndexReadyEvery = time.Minute

// identityIndexReady reports whether events_record_identity_idx exists and
// is valid, which is the precondition for probing record identity at
// ingest. Without the index the identity join is a heap walk of every row
// of every session in the batch, per batch; a 172,000-event live session
// would pay that on every upload. The runner builds the index CONCURRENTLY
// in its first window after this ships, and until then rows insert by id
// as they always did.
//
// While the index is missing or invalid the server says so once per boot,
// at WARNING, because the interval is an operational fact the rollout has
// to respect: until the runner's index step and its event_keys step have
// run, a re-walked transcript row arrives under a new id and is stored as
// a second row, and a Codex answer re-walked with usage is priced again
// under that id. Repair walks and client re-walks wait for the "derive
// step" line with step=event_keys and finished=true (review-1 finding 6).
func (s *Store) identityIndexReady(ctx context.Context, q Queryer) bool {
	s.identityIdx.mu.Lock()
	defer s.identityIdx.mu.Unlock()
	if s.identityIdx.ready {
		return true
	}
	if !s.identityIdx.checked.IsZero() && time.Since(s.identityIdx.checked) < identityIndexReadyEvery {
		return false
	}
	s.identityIdx.checked = time.Now()
	var valid bool
	err := q.QueryRow(ctx, `
		SELECT indisvalid FROM pg_index
		WHERE indexrelid = to_regclass('events_record_identity_idx')`).Scan(&valid)
	if err != nil || !valid {
		if !s.identityIdx.warned {
			s.identityIdx.warned = true
			state := "invalid"
			if noRows(err) {
				state = "missing"
			} else if err != nil {
				state = "unreadable: " + err.Error()
			}
			s.logger().WarnContext(ctx, "record identity index not ready",
				"index", "events_record_identity_idx",
				"state", state,
				"effect", "transcript rows insert by id; a re-walked row under a new id becomes a second row until the runner's index and event_keys steps finish")
		}
		return false
	}
	s.identityIdx.ready = true
	return true
}

// identityIndexInvalidate forgets a positive answer about the identity
// index, so the next batch asks the catalog again: the identity join
// failed, and an index dropped or left invalid behind the cache is one of
// the reasons it can.
func (s *Store) identityIndexInvalidate() {
	s.identityIdx.mu.Lock()
	s.identityIdx.ready = false
	s.identityIdx.checked = time.Time{}
	s.identityIdx.mu.Unlock()
}

// insertEvents writes the batch and reports which ids were new.
//
// ON CONFLICT (id) DO NOTHING ... RETURNING id is what makes a duplicate free:
// Postgres returns only the rows it actually inserted, so one round trip both
// stores the batch and answers which half of it we had already seen. Counting
// duplicates by reading first and writing second would be two round trips and
// still racy.
//
// Before the insert, transcript rows are matched by record identity. A
// transcript event's id is a hash of its walk-order position, so a file
// re-walked under a rule that emits one more or one fewer event names every
// later record by a new id, and an insert by id alone would store the rest
// of the session a second time beside the first. The record's own uuid does
// not move. An incoming transcript row whose (session, agent, record uuid,
// type, tool_use id) is already stored under another id is that row: it
// upgrades the stored one when its extraction is newer and is acknowledged
// as a duplicate either way, so the client deletes it from its spool and no
// second row is born. Hook rows have no record uuid and keep id identity.
//
// It reports the ids that were new; the incoming ids that matched a stored
// record identity, each mapped to the rows that identity holds (usage is
// credited against the stored ids, because the ledger row has to name the
// event row it prices and an incoming id under another name names no row
// at all); and the stored ids whose body moved to a newer extraction, by
// either path, so the caller can mark their sessions dirty and refresh
// their messages rows.
func (s *Store) insertEvents(ctx context.Context, q Queryer, items []Ingest) (map[string]bool, map[string][]string, map[string]bool, error) {
	keys := make([]eventKeys, len(items))
	for i, it := range items {
		keys[i] = keysFor(it)
	}
	held, upgraded, err := s.upgradeByIdentity(ctx, q, items, keys)
	if err != nil {
		return nil, nil, nil, err
	}

	// An incoming id that is not itself one of its identity's rows names a
	// stored row only through that identity: upgradeByIdentity applied it
	// or found it older, it is acknowledged as a duplicate, and it stays
	// out of the insert. An id that is one of the rows goes through the
	// insert's conflict path as it always did.
	known := make(map[string]bool, len(held))
	for id, ids := range held {
		if !slices.Contains(ids, id) {
			known[id] = true
		}
	}

	n := len(items) - len(known)
	ids := make([]string, 0, n)
	sessionIDs := make([]string, 0, n)
	emails := make([]string, 0, n)
	seqs := make([]int64, 0, n)
	types := make([]string, 0, n)
	origins := make([]string, 0, n)
	occurred := make([]time.Time, 0, n)
	agents := make([]string, 0, n)
	workflows := make([]string, 0, n)
	models := make([]string, 0, n)
	tools := make([]string, 0, n)
	bodies := make([]string, 0, n)
	captureVersions := make([]int32, 0, n)
	promptIDs := make([]string, 0, n)
	recordUUIDs := make([]string, 0, n)
	parentRecordUUIDs := make([]string, 0, n)
	requestIDs := make([]string, 0, n)
	messageIDs := make([]string, 0, n)
	toolUseIDs := make([]string, 0, n)

	for i, it := range items {
		if known[it.Event.ID] {
			continue
		}
		body, err := ingestBody(it)
		if err != nil {
			return nil, nil, nil, err
		}
		ids = append(ids, it.Event.ID)
		sessionIDs = append(sessionIDs, it.Event.SessionID)
		emails = append(emails, it.Email)
		seqs = append(seqs, it.Event.Seq)
		types = append(types, string(it.Event.Type))
		origins = append(origins, string(it.Event.Origin))
		occurred = append(occurred, it.Event.OccurredAt)
		agents = append(agents, it.Event.AgentID)
		workflows = append(workflows, it.Event.WorkflowID)
		models = append(models, it.Event.Model)
		toolName := ""
		if it.Event.Tool != nil {
			toolName = it.Event.Tool.Name
		}
		tools = append(tools, toolName)
		bodies = append(bodies, body)
		captureVersions = append(captureVersions, int32(it.Event.CaptureVersion))
		k := keys[i]
		promptIDs = append(promptIDs, k.prompt)
		recordUUIDs = append(recordUUIDs, k.record)
		parentRecordUUIDs = append(parentRecordUUIDs, k.parentRecord)
		requestIDs = append(requestIDs, k.request)
		messageIDs = append(messageIDs, k.message)
		toolUseIDs = append(toolUseIDs, k.toolUse)
	}
	fresh := make(map[string]bool, len(ids))
	if len(ids) == 0 {
		return fresh, held, upgraded, nil
	}

	// DO UPDATE guarded by a strictly-greater capture version, rather than DO
	// NOTHING.
	//
	// The guard is what preserves idempotency: a re-delivery carries the same
	// version, the WHERE fails, and the row is neither written nor returned —
	// exactly what DO NOTHING did. Only a copy produced by a newer extraction
	// replaces what is stored, which is what lets an agent that learns to read a
	// transcript better fix history it has already uploaded, instead of having
	// the improvement discarded by an id that did not change.
	//
	// xmax distinguishes the two outcomes. It is zero on a row this statement
	// inserted and non-zero on one it updated, and the difference matters
	// enormously: usage is credited from freshly inserted events, so counting an
	// upgraded row as fresh would bill its tokens a second time. Upgraded rows
	// therefore refresh the event and nothing else; everything derived from
	// events is rebuilt by the derivation pass, which is versioned separately.
	//
	// The key columns are written on insert and refreshed on upgrade, from
	// the named fields when the client sent them and from body.raw otherwise,
	// so every row is keyed from the moment it lands; the runner's event_keys
	// step is for the rows that were stored before the columns existed.
	// superseded_by is not among them: it is the runner's alone.
	rows, err := q.Query(ctx, `
		INSERT INTO events (id, session_id, email, seq, type, origin, occurred_at,
		                    agent_id, workflow_id, model, tool_name, body, capture_version,
		                    prompt_id, record_uuid, parent_record_uuid, request_id, message_id, tool_use_id)
		SELECT u.id, u.session_id, u.email, u.seq, u.type, u.origin, u.occurred_at,
		       nullif(u.agent_id,''), nullif(u.workflow_id,''), nullif(u.model,''),
		       nullif(u.tool_name,''), u.body, u.capture_version,
		       nullif(u.prompt_id,''), nullif(u.record_uuid,''), nullif(u.parent_record_uuid,''),
		       nullif(u.request_id,''), nullif(u.message_id,''), nullif(u.tool_use_id,'')
		FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::text[], $6::text[],
		            $7::timestamptz[], $8::text[], $9::text[], $10::text[], $11::text[], $12::jsonb[],
		            $13::int[], $14::text[], $15::text[], $16::text[], $17::text[], $18::text[], $19::text[])
		     AS u(id, session_id, email, seq, type, origin, occurred_at,
		          agent_id, workflow_id, model, tool_name, body, capture_version,
		          prompt_id, record_uuid, parent_record_uuid, request_id, message_id, tool_use_id)
		ON CONFLICT (id) DO UPDATE SET
			body               = EXCLUDED.body,
			model              = EXCLUDED.model,
			tool_name          = EXCLUDED.tool_name,
			agent_id           = EXCLUDED.agent_id,
			workflow_id        = EXCLUDED.workflow_id,
			capture_version    = EXCLUDED.capture_version,
			prompt_id          = COALESCE(EXCLUDED.prompt_id, events.prompt_id),
			record_uuid        = COALESCE(EXCLUDED.record_uuid, events.record_uuid),
			parent_record_uuid = COALESCE(EXCLUDED.parent_record_uuid, events.parent_record_uuid),
			request_id         = COALESCE(EXCLUDED.request_id, events.request_id),
			message_id         = COALESCE(EXCLUDED.message_id, events.message_id),
			tool_use_id        = COALESCE(EXCLUDED.tool_use_id, events.tool_use_id)
		WHERE EXCLUDED.capture_version > events.capture_version
		RETURNING id, (xmax = 0) AS inserted`,
		ids, sessionIDs, emails, seqs, types, origins, occurred,
		agents, workflows, models, tools, bodies, captureVersions,
		promptIDs, recordUUIDs, parentRecordUUIDs, requestIDs, messageIDs, toolUseIDs)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("store: insert events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id       string
			inserted bool
		)
		if err := rows.Scan(&id, &inserted); err != nil {
			return nil, nil, nil, fmt.Errorf("store: scan inserted event: %w", err)
		}
		if !inserted {
			// Upgraded, not new. Deliberately absent from fresh so nothing
			// downstream counts it twice.
			upgraded[id] = true
			continue
		}
		fresh[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("store: insert events: %w", err)
	}
	return fresh, held, upgraded, nil
}

// upgradeByIdentity matches the batch's transcript rows against stored rows
// by record identity and upgrades every stored row of the identity in
// place where the incoming extraction is newer. It reports the incoming
// ids that matched, each with the rows its identity holds; the ones that
// are not themselves among those rows are acknowledged as duplicates and
// kept out of the insert.
//
// The join is written against the partial identity index and runs only
// once that index is valid (identityIndexReady): the runner builds it in
// its first window after the columns ship, and until then this is a no-op.
// The first map is incoming id to the identity's stored ids, in id order;
// the second is the stored ids whose body was replaced.
func (s *Store) upgradeByIdentity(ctx context.Context, q Queryer, items []Ingest, keys []eventKeys) (map[string][]string, map[string]bool, error) {
	held := map[string][]string{}
	upgraded := map[string]bool{}
	var (
		ords     []int32
		sids     []string
		agents   []string
		records  []string
		types    []string
		toolUses []string
	)
	for i, it := range items {
		k := derive.Keys{RecordUUID: keys[i].record, ToolUseID: keys[i].toolUse}
		id, ok := derive.IdentityOf(it.Event, k)
		if !ok {
			continue
		}
		ords = append(ords, int32(i))
		sids = append(sids, id.SessionID)
		agents = append(agents, id.AgentID)
		records = append(records, id.Record)
		types = append(types, string(id.Type))
		toolUses = append(toolUses, id.ToolUseID)
	}
	if len(ords) == 0 || !s.identityIndexReady(ctx, q) {
		return held, upgraded, nil
	}

	rows, err := q.Query(ctx, `
		SELECT u.ord, e.id, e.capture_version
		FROM unnest($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])
		     AS u(ord, session_id, agent_id, record_uuid, type, tool_use_id)
		JOIN events e
		  ON e.session_id = u.session_id
		 AND coalesce(e.agent_id, '') = u.agent_id
		 AND e.record_uuid = u.record_uuid
		 AND e.type = u.type
		 AND coalesce(e.tool_use_id, '') = u.tool_use_id
		WHERE e.origin = 'transcript'
		ORDER BY u.ord, e.id`,
		ords, sids, agents, records, types, toolUses)
	if err != nil {
		// The join can fail because the index the cache vouched for is gone
		// or invalid; the next batch asks the catalog again.
		s.identityIndexInvalidate()
		return nil, nil, fmt.Errorf("store: match record identity: %w", err)
	}
	type storedRow struct {
		id      string
		version int32
	}
	// Every stored row of each copy's identity, in id order. The walker's
	// rare double stores one record as two rows on first sight, and the
	// fold elects a turn's final answer among its rows by position, so an
	// upgrade that reached the lower-id row alone left the other as the
	// answer at the old text (review-4 finding N2): every row of the
	// identity is upgraded from the same elected copy.
	matched := map[int32][]storedRow{}
	for rows.Next() {
		var (
			ord int32
			r   storedRow
		)
		if err := rows.Scan(&ord, &r.id, &r.version); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("store: scan record identity: %w", err)
		}
		matched[ord] = append(matched[ord], r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store: match record identity: %w", err)
	}
	if len(matched) == 0 {
		return held, upgraded, nil
	}

	// Several copies of one record can share a batch: the walker's rare
	// double re-walked under a newer rule, two pending walks in one upload,
	// or a re-walked copy beside the id it re-walked. Each stored row is
	// upgraded at most once, from the copy with the highest capture version
	// (ties: the last in batch order), so the UPDATE below names every id
	// once and the row ends the same whichever order the copies arrived
	// in: named twice, UPDATE ... FROM applies an arbitrary one of them, and
	// the messages refresh downstream refuses a second hit on one row
	// (adversarial finding 1). Every copy of an identity competes for every
	// row the identity holds, so twin rows elect the same copy. Every
	// matched id is recorded in held whichever copy wins: the aliases among
	// them are acknowledged as duplicates and kept out of the insert
	// (insertEvents). The stored row's own id, when it is among the copies,
	// competes on the same terms, and when it wins the ordinary insert's
	// version guard applies its body to that row and this UPDATE applies it
	// to the row's twin. UpsertEvents collapses the batch by the same rule
	// (collapseByStoredID), so what it credits and refreshes is the copy
	// each row holds.
	elected := map[string]int32{}
	version := map[string]int32{}
	var storedIDs []string
	for _, ord := range sortedInt32Keys(matched) {
		it := items[ord]
		ids := make([]string, 0, len(matched[ord]))
		for _, r := range matched[ord] {
			ids = append(ids, r.id)
			version[r.id] = r.version
			w, seen := elected[r.id]
			if !seen {
				storedIDs = append(storedIDs, r.id)
				elected[r.id] = ord
				continue
			}
			if it.Event.CaptureVersion >= items[w].Event.CaptureVersion {
				elected[r.id] = ord
			}
		}
		held[it.Event.ID] = ids
	}

	var (
		upIDs, upBodies, upModels, upTools                       []string
		upVersions                                               []int32
		upPrompt, upRecord, upParent, upRequest, upMessage, upTU []string
	)
	for _, storedID := range storedIDs {
		ord := elected[storedID]
		it := items[ord]
		if it.Event.ID == storedID {
			// Same id: the ordinary insert's version guard decides.
			continue
		}
		if int32(it.Event.CaptureVersion) <= version[storedID] {
			continue
		}
		body, err := ingestBody(it)
		if err != nil {
			return nil, nil, err
		}
		toolName := ""
		if it.Event.Tool != nil {
			toolName = it.Event.Tool.Name
		}
		k := keys[ord]
		upgraded[storedID] = true
		upIDs = append(upIDs, storedID)
		upBodies = append(upBodies, body)
		upModels = append(upModels, it.Event.Model)
		upTools = append(upTools, toolName)
		upVersions = append(upVersions, int32(it.Event.CaptureVersion))
		upPrompt = append(upPrompt, k.prompt)
		upRecord = append(upRecord, k.record)
		upParent = append(upParent, k.parentRecord)
		upRequest = append(upRequest, k.request)
		upMessage = append(upMessage, k.message)
		upTU = append(upTU, k.toolUse)
	}
	if len(upIDs) == 0 {
		return held, upgraded, nil
	}
	// The stored row keeps its id, its seq and its place; the body, the model,
	// the tool name, the version and the keys move to the newer extraction.
	// seq is deliberately not touched: it is an ordering column that the
	// stored row's neighbours were numbered against.
	if _, err := q.Exec(ctx, `
		UPDATE events e SET
			body               = u.body,
			model              = nullif(u.model, ''),
			tool_name          = nullif(u.tool_name, ''),
			capture_version    = u.capture_version,
			prompt_id          = COALESCE(nullif(u.prompt_id, ''), e.prompt_id),
			record_uuid        = COALESCE(nullif(u.record_uuid, ''), e.record_uuid),
			parent_record_uuid = COALESCE(nullif(u.parent_record_uuid, ''), e.parent_record_uuid),
			request_id         = COALESCE(nullif(u.request_id, ''), e.request_id),
			message_id         = COALESCE(nullif(u.message_id, ''), e.message_id),
			tool_use_id        = COALESCE(nullif(u.tool_use_id, ''), e.tool_use_id)
		FROM unnest($1::text[], $2::jsonb[], $3::text[], $4::text[], $5::int[],
		            $6::text[], $7::text[], $8::text[], $9::text[], $10::text[], $11::text[])
		     AS u(id, body, model, tool_name, capture_version,
		          prompt_id, record_uuid, parent_record_uuid, request_id, message_id, tool_use_id)
		WHERE e.id = u.id AND u.capture_version > e.capture_version`,
		upIDs, upBodies, upModels, upTools, upVersions,
		upPrompt, upRecord, upParent, upRequest, upMessage, upTU); err != nil {
		return nil, nil, fmt.Errorf("store: upgrade events by record identity: %w", err)
	}
	return held, upgraded, nil
}

func sortedInt32Keys[V any](m map[int32]V) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func ingestBody(it Ingest) (string, error) {
	if len(it.Body) > 0 {
		return string(it.Body), nil
	}
	b, err := json.Marshal(it.Event)
	if err != nil {
		return "", fmt.Errorf("store: marshal event %s: %w", it.Event.ID, err)
	}
	return string(b), nil
}

// messageRole maps an event onto the search corpus, or reports that it has no
// place in it. Search is at message granularity because a session's text does
// not fit in a single tsvector, so the corpus is exactly the events a human
// would recognise as something that was said.
func messageRole(t event.Type) (string, bool) {
	switch t {
	case event.UserPrompt:
		return "user", true
	case event.AssistantTurn:
		return "assistant", true
	case event.ToolResult, event.ToolFailed:
		return "tool", true
	}
	return "", false
}

// classifyIngest names what a text-bearing event is, for the search corpus and
// for the rollup, through the one normalizer. Only a transcript record's own
// line is handed over as raw: a hook payload's keys mean other things, and the
// normalizer is told to ignore one anyway, but not passing it is cheaper than
// having it refused.
//
// A subagent stream's driving prompt is the first user record of the stream.
// The walker numbers each stream from zero with the SubagentStart marker at 0,
// so the prompt the parent agent wrote is at seq 1; the hook path emits no
// user prompts inside subagents at all. The derive runner recomputes kinds for
// history, so a later walker that numbers differently is corrected there.
func classifyIngest(it Ingest) (normalize.Message, bool) {
	switch it.Event.Type {
	case event.UserPrompt:
		var raw []byte
		if it.Event.Origin == event.OriginTranscript {
			raw = it.Event.Raw
		}
		agentStream := it.Event.AgentID != ""
		return normalize.ClassifyUser(it.Event.Text, raw, agentStream, agentStream && it.Event.Seq <= 1), true
	case event.AssistantTurn:
		return normalize.Message{
			Kind: normalize.ClassifyAssistant(it.Event.Text, it.Event.Usage != nil),
			Text: it.Event.Text,
		}, true
	case event.ToolResult, event.ToolFailed:
		return normalize.Message{Kind: normalize.KindTool, Text: it.Event.Text}, true
	}
	return normalize.Message{}, false
}

// writeMessages writes the search-corpus rows for items. With refresh off
// it inserts and leaves an existing row alone; with refresh on it rewrites
// the text, kind and thread of an existing row, which is what an upgraded
// event needs.
func writeMessages(ctx context.Context, q Queryer, items []Ingest, refresh bool) error {
	var (
		eventIDs   []string
		sessionIDs []string
		emails     []string
		seqs       []int64
		roles      []string
		occurred   []time.Time
		texts      []string
		kinds      []string
		agents     []string
	)
	for _, it := range items {
		role, ok := messageRole(it.Event.Type)
		if !ok || strings.TrimSpace(it.Event.Text) == "" {
			continue
		}
		m, _ := classifyIngest(it)
		eventIDs = append(eventIDs, it.Event.ID)
		sessionIDs = append(sessionIDs, it.Event.SessionID)
		emails = append(emails, it.Email)
		seqs = append(seqs, it.Event.Seq)
		roles = append(roles, role)
		occurred = append(occurred, it.Event.OccurredAt)
		texts = append(texts, it.Event.Text)
		kinds = append(kinds, string(m.Kind))
		agents = append(agents, it.Event.AgentID)
	}
	if len(eventIDs) == 0 {
		return nil
	}
	// Same transaction as the event insert, because a message whose event never
	// landed would violate the foreign key and an event whose message never
	// landed would be permanently invisible to search with nothing to detect it.
	//
	// The kind is written here and only here at ingest; the text stays whole
	// (a search for a word inside a notification must still find it), and the
	// kind is what lets titles, counts and the default search corpus leave the
	// harness's own messages out.
	//
	// On the refresh path the row named is the stored one and the position
	// written is the stored row's: a copy that matched by record identity
	// arrives at its new walk position, and the stored row's neighbours
	// were numbered against the old one, which is why the identity upgrade
	// leaves seq alone too. The refresh inserts when the first extraction
	// carried no text and wrote no messages row, so seq and occurred_at are
	// read from events there rather than from the copy (review-4 finding
	// N4); the conflict path never touched them.
	position, source := `u.seq, u.role, u.occurred_at`, ``
	conflict := `ON CONFLICT (event_id) DO NOTHING`
	if refresh {
		position, source = `e.seq, u.role, e.occurred_at`, `JOIN events e ON e.id = u.event_id`
		conflict = `ON CONFLICT (event_id) DO UPDATE SET
			text = EXCLUDED.text, kind = EXCLUDED.kind, agent_id = EXCLUDED.agent_id
			WHERE (messages.text, messages.kind, messages.agent_id)
			      IS DISTINCT FROM (EXCLUDED.text, EXCLUDED.kind, EXCLUDED.agent_id)`
	}
	_, err := q.Exec(ctx, `
		INSERT INTO messages (event_id, session_id, email, seq, role, occurred_at, text, kind, agent_id)
		SELECT u.event_id, u.session_id, u.email, `+position+`, u.text,
		       u.kind, nullif(u.agent_id, '')
		FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[],
		            $5::text[], $6::timestamptz[], $7::text[], $8::text[], $9::text[])
		     AS u(event_id, session_id, email, seq, role, occurred_at, text, kind, agent_id)
		`+source+`
		`+conflict,
		eventIDs, sessionIDs, emails, seqs, roles, occurred, texts, kinds, agents)
	if err != nil {
		return fmt.Errorf("store: insert messages: %w", err)
	}
	return nil
}

// usageKey identifies a model call for deduplication.
//
// The ledger's key is the message id alone. The table's primary key is still
// (message_id, request_id), from the day the contract named both, but the
// request id is normalised to ” here and never written: every row the fleet
// has ever stored carries ” there, because no client stamped
// Usage.RequestID before 2026-09-15, and the clients that stamp it now (the
// hook copy and the walked copy of one call carry the same id) would give
// the repair walk's copy of every older call a second key and bill it twice
// if the value reached the key. Keeping the column empty makes the primary
// key degenerate to the message id with no schema change; the stamped value
// lives in events.request_id and body.raw instead. When the
// transcript carried no message id at all the event id stands in, which is
// safe because events are already deduplicated by primary key, and it keeps
// every usage row on one code path instead of two.
//
// A message id over derive.KeyMaxBytes takes the same stand-in. The ledger's
// primary key is a btree with the entry ceiling the events keys are capped
// under, and one such value from a corrupt or hostile client failed the
// whole batch it arrived in, on every re-send (review-4 finding N1); the
// event id is bounded by validateIngest, so the stand-in always fits.
func usageKey(it Ingest) (messageID, requestID string) {
	u := it.Event.Usage
	if u == nil {
		return "", ""
	}
	if u.MessageID == "" || len(u.MessageID) > derive.KeyMaxBytes {
		return "event:" + it.Event.ID, ""
	}
	return u.MessageID, ""
}

// credited is usage the ledger accepted, and therefore usage that this call is
// allowed to add to a session's totals.
type credited struct {
	Usage   event.Usage
	CostUSD float64
}

// creditUsage records token usage through the ledger and reports which events
// were credited.
//
// This is the second and independent idempotency layer. Event ids stop the same
// record being stored twice; the ledger stops the same model call being counted
// twice when it legitimately appears in two different records, which is the
// failure the contract calls out because it silently inflates every cost figure
// in the product rather than producing a visible error.
func (s *Store) creditUsage(ctx context.Context, q Queryer, items []Ingest) (map[string]credited, error) {
	var (
		messageIDs []string
		requestIDs []string
		eventIDs   []string
		sessionIDs []string
		emails     []string
		models     []string
		occurred   []time.Time
		inTok      []int64
		outTok     []int64
		cacheRead  []int64
		write5m    []int64
		write1h    []int64
		costs      []float64
	)
	usageByEvent := make(map[string]event.Usage, len(items))
	batchSeen := make(map[string]bool, len(items))

	for _, it := range items {
		u := it.Event.Usage
		if u == nil {
			continue
		}
		// Synthetic records carry usage-shaped numbers for calls that never
		// happened, so they are excluded before they can reach a total.
		if it.Event.Model == SyntheticModel {
			continue
		}
		mid, rid := usageKey(it)
		if mid == "" {
			continue
		}
		// Within one batch the same call may arrive as a hook Stop and its
		// transcript twin, or as an assistant record and the tool rows split
		// out of it; the first carrier is priced and the rest are the same
		// call.
		if batchSeen[mid] {
			continue
		}
		batchSeen[mid] = true

		var cost float64
		if s.pricer != nil {
			cost = s.pricer.CostUSD(it.Event.Model, *u, it.Event.OccurredAt)
		}
		messageIDs = append(messageIDs, mid)
		requestIDs = append(requestIDs, rid)
		eventIDs = append(eventIDs, it.Event.ID)
		sessionIDs = append(sessionIDs, it.Event.SessionID)
		emails = append(emails, it.Email)
		models = append(models, it.Event.Model)
		occurred = append(occurred, it.Event.OccurredAt)
		inTok = append(inTok, u.InputTokens)
		outTok = append(outTok, u.OutputTokens)
		cacheRead = append(cacheRead, u.CacheReadTokens)
		write5m = append(write5m, u.Ephemeral5m)
		write1h = append(write1h, u.Ephemeral1h)
		costs = append(costs, cost)
		usageByEvent[it.Event.ID] = *u
	}
	if len(messageIDs) == 0 {
		return nil, nil
	}

	rows, err := q.Query(ctx, `
		INSERT INTO usage_ledger (message_id, request_id, event_id, session_id, email, model, occurred_at,
		                          input_tokens, output_tokens, cache_read_tokens,
		                          cache_write_5m_tokens, cache_write_1h_tokens, cost_usd)
		SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
		                     $7::timestamptz[], $8::bigint[], $9::bigint[], $10::bigint[],
		                     $11::bigint[], $12::bigint[], $13::numeric[])
		ON CONFLICT (message_id, request_id) DO NOTHING
		RETURNING event_id, cost_usd::float8`,
		messageIDs, requestIDs, eventIDs, sessionIDs, emails, models, occurred,
		inTok, outTok, cacheRead, write5m, write1h, costs)
	if err != nil {
		return nil, fmt.Errorf("store: credit usage: %w", err)
	}
	defer rows.Close()

	counted := make(map[string]credited, len(messageIDs))
	for rows.Next() {
		var id string
		var cost float64
		if err := rows.Scan(&id, &cost); err != nil {
			return nil, fmt.Errorf("store: scan credited usage: %w", err)
		}
		counted[id] = credited{Usage: usageByEvent[id], CostUSD: cost}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: credit usage: %w", err)
	}
	return counted, nil
}

// ---------------------------------------------------------------------------
// Session rollup
// ---------------------------------------------------------------------------

// SessionDelta is a rollup increment: what a set of newly stored events adds to
// a session, never what the session totals. Everything here is folded into the
// stored row with an addition or a monotone merge, which is what lets batches
// for one session arrive in any order, overlap a backfill, and still land on the
// same totals.
type SessionDelta struct {
	SessionID       string
	Email           string
	DeviceID        string
	Source          string
	ParentSessionID string
	Cwd             string
	Repo            string
	GitBranch       string

	// StartedAt and EndedAt are the extremes of this increment's event times.
	// They are merged with LEAST and GREATEST rather than assigned.
	StartedAt time.Time
	EndedAt   time.Time
	// Ended is true only when this increment contained an end marker. A session
	// that merely stopped producing events is not ended, and conflating the two
	// would mislabel exactly the crashed sessions this system exists to capture.
	Ended bool

	UserTurns int
	ToolCalls int
	Subagents int
	Errors    int
	// HumanTurns counts the main-thread prompts a person typed (kind human or
	// slash_command). UserTurns counts every user_prompt event, which includes
	// the harness's own notifications and every subagent's task prompt, and
	// was the reason a session with one question and eight background tasks
	// read as nine turns.
	HumanTurns int
	// ContentEvents counts the events that mean something happened: prompts,
	// turns, tools, file changes, subagents. Lifecycle markers do not count.
	// It is what decides whether a session is empty, and it is folded rather
	// than derived because the decision has to be made on every batch.
	ContentEvents int

	HarnessVersions []string
	// AgentVersions is the set of client builds that delivered this session's
	// batches, from the ingest User-Agent.
	AgentVersions []string

	// LineageSource says how ParentSessionID was proven, and ParentSessionID is
	// only ever carried when it is set. A parent without a source is exactly
	// the fiction 0015 removed (a compaction marker mistaken for a session
	// id), so an event that names a parent without saying how is filed under
	// ParentRecordUUID instead, where the value means what it is.
	LineageSource    string
	ParentRecordUUID string

	// Launcher facts, each set once: the harness's entrypoint word, the
	// process that started it, whether a transcript was ever written, and the
	// harness's own generated title. They survive the session's type, which
	// is what lets a per-machine alert say how the empties were started.
	Entrypoint       string
	Launcher         *event.Launcher
	TranscriptExists *bool
	HarnessTitle     string
	// EndReason is the harness's word for why the session ended, from the end
	// marker in this increment. It decides whether a lifecycle-only session is
	// a spawn that died or a prompt a person opened and abandoned.
	EndReason string

	// Title and AnyPrompt are this increment's candidates for the session's
	// opening prompt: the earliest main-thread prompt a person typed, and the
	// earliest main-thread prompt of any kind, each already rendered as a
	// title. The refresh compares them by event id against the session's
	// stored earliest rows, so the title is rendered in Go by the normalizer
	// and the choice of row is made by the database, in one statement.
	Title     *titleCandidate
	AnyPrompt *titleCandidate

	TokensInput      int64
	TokensOutput     int64
	TokensCacheRead  int64
	TokensCacheWrite int64
	CostUSD          float64

	Redactions map[string]int

	// SawUserPrompt tells the caller whether the session's first prompt might
	// have changed, so the refresh is skipped for the overwhelming majority of
	// batches that carry no prompt at all.
	SawUserPrompt bool

	// MirrorRequest is the per-session Slack ask carried on the SessionStarted
	// event. Set-once at the rollup: it describes how the session was started,
	// and a later batch carrying nothing must not clear it.
	MirrorRequest string

	// Automation means this increment carried positive evidence the session was
	// machine-launched (a headless entrypoint or a codex exec originator). It
	// merges monotonically: once any batch proves automation, the session stays
	// automation, because batches arrive in any order and a later batch's
	// silence is not evidence of a person. Set by the caller from the WHOLE
	// admitted batch, not just its fresh half: a re-walk re-delivers known
	// events with better extraction, and the evidence in a re-delivered event
	// is as true as it was the first time — unlike the counters here, marking
	// twice cannot overcount.
	Automation bool
}

// titleCandidate is one prompt rendered as a title, with the event it came
// from and where in its stream it sat.
type titleCandidate struct {
	EventID string
	Seq     int64
	At      time.Time
	Title   string
	Command bool
}

// earlier reports whether this candidate precedes other in stream order,
// which is seq first and event time second, as the stored rows are read.
func (c *titleCandidate) earlier(other *titleCandidate) bool {
	if other == nil {
		return true
	}
	if c.Seq != other.Seq {
		return c.Seq < other.Seq
	}
	return c.At.Before(other.At)
}

// automationSessions reports which sessions in a batch carry launcher
// evidence. The named field is free to check and every current agent stamps
// it; the Raw parse runs at most once per session per batch, for events from
// agents that predate the field — transcript records repeat the entrypoint on
// every line, so parsing more than one buys nothing.
func automationSessions(items []Ingest) map[string]bool {
	out := map[string]bool{}
	probed := map[string]bool{}
	for _, it := range items {
		sid := it.Event.SessionID
		if out[sid] {
			continue
		}
		if ep := it.Event.Entrypoint; ep != "" {
			if automationEntrypoint(ep) {
				out[sid] = true
			}
			continue
		}
		if probed[sid] {
			continue
		}
		if a, decided := automationEvidenceRaw(it.Event.Raw); decided {
			probed[sid] = true
			if a {
				out[sid] = true
			}
		}
	}
	return out
}

// markSessionType folds one piece of type evidence into a session outside the
// delta path, for batches whose evidence rode entirely on re-delivered events.
// It goes through session_type_merge like the delta does, so it obeys the same
// lattice and can only ever promote, and it leaves the row shaped the way the
// delta's promotion leaves it: empty_kind refines empty rows alone, so a row
// that stops being empty here drops its provisional kind exactly as one that
// content promoted. Without that, a spawn whose sdk-cli entrypoint arrived on
// a re-walk would read as automation still carrying "aborted", a refinement
// no automation row is owed and one the fleet's empty-start count would then
// have to know to ignore.
//
// It reports whether the row changed, so the caller can do what a promotion
// obliges and nothing more: a re-walk re-delivers every automation session's
// start marker, and touching each of those rows again would be work with no
// change to show for it.
func markSessionType(ctx context.Context, q Queryer, sessionID, typ string) (promoted bool, err error) {
	n, err := q.Exec(ctx, `
		UPDATE sessions s
		SET session_type = session_type_merge(s.session_type, $2),
		    empty_kind   = CASE WHEN session_type_merge(s.session_type, $2) <> 'empty'
		                        THEN NULL ELSE s.empty_kind END,
		    derive_dirty = true,
		    updated_at   = now()
		WHERE s.session_id = $1 AND session_type_merge(s.session_type, $2) <> s.session_type`, sessionID, typ)
	if err != nil {
		return false, fmt.Errorf("store: mark %s %s: %w", sessionID, typ, err)
	}
	return n > 0, nil
}

// markAutomation folds re-delivered launcher evidence into a session and,
// when that promoted the row, names it. An automation session is titled by
// its first prompt of any kind, and the prompt may have been stored batches
// ago under a wrapper kind that could not title a user session; the
// promotion is what makes it the title, so the refresh runs here with no
// candidates of its own and lets the statement adopt the stored row.
func markAutomation(ctx context.Context, q Queryer, sessionID string) error {
	promoted, err := markSessionType(ctx, q, sessionID, "automation")
	if err != nil || !promoted {
		return err
	}
	return refreshFirstPrompt(ctx, q, &SessionDelta{SessionID: sessionID})
}

// automationEntrypoint reports whether an entrypoint value names a machine
// launcher.
func automationEntrypoint(ep string) bool {
	return ep == "sdk-cli" || strings.HasPrefix(ep, "codex_exec")
}

// automationEvidenceRaw decides a session's launcher from one raw record,
// reporting both the verdict and whether the record answered at all.
//
// It decodes the raw record and reads named top-level fields, never a substring:
// tool output inside raw routinely QUOTES other transcripts — a person grepping
// a cron log has "entrypoint":"sdk-cli" verbatim in their session — and a
// substring match would classify the investigator as the robot. The gate below
// is only ever a reason to skip the parse, never a verdict, and it can only
// skip records whose top-level key cannot exist. (A key spelled with JSON
// unicode escapes would slip the gate; no harness writes one, and the cost of
// closing that hole is parsing every event ever ingested.)
//
// This is the fallback for events from agents that predate the Entrypoint
// field; newer agents answer through it and never reach a parse.
func automationEvidenceRaw(raw []byte) (automation, decided bool) {
	if len(raw) == 0 {
		return false, false
	}
	if !bytes.Contains(raw, []byte(`"entrypoint"`)) && !bytes.Contains(raw, []byte(`"originator"`)) {
		return false, false
	}
	var probe struct {
		Entrypoint string `json:"entrypoint"`
		Payload    struct {
			Originator string `json:"originator"`
		} `json:"payload"`
	}
	if json.Unmarshal(raw, &probe) != nil {
		return false, false
	}
	switch {
	case probe.Entrypoint != "":
		return automationEntrypoint(probe.Entrypoint), true
	case probe.Payload.Originator != "":
		return automationEntrypoint(probe.Payload.Originator), true
	}
	return false, false
}

// knownLineageSource reports whether a value names a way a parent can be
// proven: the fork uuid the transcript carries, or the fork hook. The list is
// the contract section 1's, and it is a list rather than "anything non-empty"
// because the gate exists to keep unproven parents out of parent_session_id,
// and a source word the server has never heard of proves nothing.
func knownLineageSource(source string) bool {
	return source == "fork_uuid" || source == "fork_hook"
}

// Caps on the strings an event carries into a sessions column. first_prompt
// is bounded by the normalizer's title length; these are the other columns a
// client-written string lands in, and without a bound one event would decide
// how wide a row can be. The launcher cap is generous for what the fields
// hold (an entrypoint word, a bundle id, a terminal name, a process name)
// and still a bound.
const (
	harnessTitleMax  = normalize.TitleMax
	entrypointMax    = 64
	launcherFieldMax = 128
)

// clipRunes bounds a string to max runes, cutting on a rune boundary so a
// multi-byte character is never split into an invalid tail.
func clipRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	n := 0
	for i := range s {
		if n == max {
			return s[:i]
		}
		n++
	}
	return s
}

// clipLauncher returns a copy of the launcher facts with every field bounded.
// A copy, because the event's own struct is the delivered record and is not
// this function's to rewrite.
func clipLauncher(l *event.Launcher) *event.Launcher {
	return &event.Launcher{
		Entrypoint: clipRunes(l.Entrypoint, launcherFieldMax),
		BundleID:   clipRunes(l.BundleID, launcherFieldMax),
		Term:       clipRunes(l.Term, launcherFieldMax),
		ParentComm: clipRunes(l.ParentComm, launcherFieldMax),
	}
}

// foldDeltas turns newly stored events into one increment per session. Only
// usage that the ledger actually credited contributes tokens or cost.
func foldDeltas(items []Ingest, counted map[string]credited) map[string]*SessionDelta {
	out := make(map[string]*SessionDelta, 2)
	for _, it := range items {
		d := out[it.Event.SessionID]
		if d == nil {
			d = &SessionDelta{
				SessionID: it.Event.SessionID,
				Email:     it.Email,
				DeviceID:  it.DeviceID,
				Source:    string(it.Event.Source),
			}
			out[it.Event.SessionID] = d
		}
		// The lineage gate. A parent is carried only when the event says how it
		// was proven; the old walker stamps a compaction marker here on every
		// event after the compaction, and a value with no source is that marker,
		// so it goes where a marker belongs. Old clients keep sending it for
		// weeks after this ships, which is why the gate is here and not in the
		// client.
		switch {
		case knownLineageSource(it.Event.LineageSource) && it.Event.ParentSessionID != "":
			d.ParentSessionID = it.Event.ParentSessionID
			d.LineageSource = it.Event.LineageSource
		case it.Event.ParentSessionID != "" && d.ParentRecordUUID == "":
			d.ParentRecordUUID = it.Event.ParentSessionID
		}
		if it.Event.ParentRecordUUID != "" {
			d.ParentRecordUUID = it.Event.ParentRecordUUID
		}
		if it.Event.Cwd != "" {
			d.Cwd = it.Event.Cwd
		}
		if it.Repo != "" {
			d.Repo = it.Repo
		}
		if it.Event.GitBranch != "" {
			d.GitBranch = it.Event.GitBranch
		}
		if it.Event.HarnessVersion != "" && !containsString(d.HarnessVersions, it.Event.HarnessVersion) {
			d.HarnessVersions = append(d.HarnessVersions, it.Event.HarnessVersion)
		}
		if it.AgentVersion != "" && !containsString(d.AgentVersions, it.AgentVersion) {
			d.AgentVersions = append(d.AgentVersions, it.AgentVersion)
		}
		if it.Event.Entrypoint != "" && d.Entrypoint == "" {
			d.Entrypoint = clipRunes(it.Event.Entrypoint, entrypointMax)
		}
		if it.Event.HarnessTitle != "" {
			d.HarnessTitle = clipRunes(it.Event.HarnessTitle, harnessTitleMax)
		}
		if d.StartedAt.IsZero() || it.Event.OccurredAt.Before(d.StartedAt) {
			d.StartedAt = it.Event.OccurredAt
		}
		if it.Event.OccurredAt.After(d.EndedAt) {
			d.EndedAt = it.Event.OccurredAt
		}

		switch it.Event.Type {
		case event.SessionStarted:
			if it.Event.MirrorRequest != "" {
				d.MirrorRequest = it.Event.MirrorRequest
			}
			if it.Event.Launcher != nil && d.Launcher == nil {
				d.Launcher = clipLauncher(it.Event.Launcher)
			}
		case event.UserPrompt:
			d.UserTurns++
			d.ContentEvents++
			d.SawUserPrompt = true
			if m, _ := classifyIngest(it); it.Event.AgentID == "" && strings.TrimSpace(it.Event.Text) != "" {
				c := &titleCandidate{
					EventID: it.Event.ID, Seq: it.Event.Seq, At: it.Event.OccurredAt,
					Title: normalize.Title(m), Command: m.Kind == normalize.KindSlashCommand,
				}
				if c.earlier(d.AnyPrompt) {
					d.AnyPrompt = c
				}
				if normalize.IsHumanKind(m.Kind) {
					d.HumanTurns++
					if c.earlier(d.Title) {
						d.Title = c
					}
				}
			}
		case event.AssistantTurn:
			// A turn with neither text nor usage is a hook that captured
			// nothing; it proves a Stop fired, not that anything was said.
			if strings.TrimSpace(it.Event.Text) != "" || it.Event.Usage != nil {
				d.ContentEvents++
			}
		case event.ToolCall:
			d.ToolCalls++
			d.ContentEvents++
		case event.ToolResult, event.FileChanged, event.SubagentEnd:
			d.ContentEvents++
		case event.ToolFailed:
			d.Errors++
			d.ContentEvents++
		case event.SubagentStart:
			d.Subagents++
			d.ContentEvents++
		case event.SessionEnded:
			d.Ended = true
			if it.Event.Text != "" {
				d.EndReason = it.Event.Text
			}
			if it.Event.TranscriptExists != nil {
				d.TranscriptExists = it.Event.TranscriptExists
			}
		}

		if c, ok := counted[it.Event.ID]; ok {
			d.TokensInput += c.Usage.InputTokens
			d.TokensOutput += c.Usage.OutputTokens
			d.TokensCacheRead += c.Usage.CacheReadTokens
			// The cache-creation total and its per-TTL split are alternative
			// reports of the same tokens, so taking both would double them.
			// The split is preferred where present because it is what prices
			// correctly, and the aggregate is the fallback.
			if write := c.Usage.Ephemeral5m + c.Usage.Ephemeral1h; write > 0 {
				d.TokensCacheWrite += write
			} else {
				d.TokensCacheWrite += c.Usage.CacheCreationTokens
			}
			d.CostUSD += c.CostUSD
		}
		for k, v := range it.Event.Redactions {
			if d.Redactions == nil {
				d.Redactions = map[string]int{}
			}
			d.Redactions[k] += v
		}
	}
	return out
}

// UpsertSessionRollup applies one increment to a session.
//
// UpsertEvents does this for you as part of ingest. The exported form is for a
// reconciler that recomputes a session from a transcript after a crash, and it
// enforces the same ownership rule, so it cannot be used to write into someone
// else's session either.
func (s *Store) UpsertSessionRollup(ctx context.Context, d SessionDelta) error {
	if d.SessionID == "" || d.Email == "" {
		return fmt.Errorf("store: rollup needs a session id and an owner")
	}
	// Without a start time the claim would create a session dated to year one,
	// which sorts ahead of everything real and is invisible in any date filter.
	if d.StartedAt.IsZero() {
		return fmt.Errorf("store: rollup needs the event time it starts from")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin rollup: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	owner, err := claimSession(ctx, tx, Ingest{
		Email:    d.Email,
		DeviceID: d.DeviceID,
		Event: event.Event{
			SessionID:  d.SessionID,
			Source:     event.Source(d.Source),
			OccurredAt: d.StartedAt,
		},
	})
	if err != nil {
		return err
	}
	if owner != d.Email {
		return ErrOwnerMismatch
	}
	if err := applyDelta(ctx, tx, &d); err != nil {
		return err
	}
	if d.SawUserPrompt {
		if err := refreshFirstPrompt(ctx, tx, &d); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit rollup: %w", err)
	}
	return nil
}

// applyDelta folds an increment into the stored rollup.
//
// Every column is either added to, merged monotonically, or filled in only when
// it was previously absent. Nothing is assigned outright, because assignment
// would make the result depend on which batch arrived last, and batches for one
// session arrive out of order as a matter of course.
//
// The session type goes through session_type_merge, the one merge rule (see
// migration 0015): automation absorbs, internal beats user, and empty is a
// floor a row can only be born on. empty_kind refines an empty row and
// nothing else: it is NULL whenever the merged type is not empty, so a row
// promoted by content, a pre-lattice user row that a late end marker reaches
// with its counters still at zero, and an automation spawn that died before
// it produced anything all carry none. An automation run with no content is
// still findable, as automation with content_events zero. The value is
// provisional here, decided from what this statement can see (the end
// marker's reason and the merged duration); the derive runner refines it
// with the health and grace rules that need more than one row. head_state is
// the runner's column, touched here only to move a fresh row from unknown to
// complete once content exists, so the default filter is right from the first
// batch. derive_dirty is set on every touched row: that is the runner's
// queue.
//
// The INSERT branch is never taken in practice: both callers claim the row
// first, and the claim is what gives a new session the empty default. It is
// kept complete rather than stubbed so the statement is correct on its own,
// and its CASEs are the UPDATE branch's with no stored row to merge against
// (session_type_merge(NULL, $23) is $23); the integration suite folds one
// increment through both branches and expects the same row.
func applyDelta(ctx context.Context, q Queryer, d *SessionDelta) error {
	redactions, err := marshalCounters(d.Redactions)
	if err != nil {
		return err
	}
	launcher, err := marshalLauncher(d.Launcher)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `
		INSERT INTO sessions (
			session_id, email, device_id, source, parent_session_id, cwd, repo, git_branch,
			started_at, ended_at, ended, user_turns, tool_calls, subagents, errors,
			harness_versions, tokens_input, tokens_output, tokens_cache_read, tokens_cache_write,
			cost_usd, redactions, session_type, mirror_request,
			lineage_source, parent_record_uuid, human_turns, content_events, agent_versions,
			entrypoint, launcher, transcript_exists, harness_title,
			empty_kind, head_state, derive_dirty, updated_at)
		VALUES ($1, $2, nullif($3,'')::uuid, $4, nullif($5,''), nullif($6,''), nullif($7,''),
		        nullif($8,''), $9, $10, $11, $12, $13, $14, $15, $16::text[],
		        $17, $18, $19, $20, $21::numeric, $22::jsonb, $23, $24,
		        $25, nullif($26,''), $27, $28, $29::text[],
		        $30, $31::jsonb, $32, nullif($33,''),
		        CASE WHEN $23 = 'empty' AND $11 THEN
		               CASE WHEN $34 = 'prompt_input_exit'
		                      OR $10::timestamptz - $9::timestamptz >= interval '5 seconds'
		                    THEN 'blank' ELSE 'aborted' END
		             END,
		        CASE WHEN $28 > 0 THEN 'complete' ELSE 'unknown' END,
		        true, now())
		ON CONFLICT (session_id) DO UPDATE SET
			session_type       = session_type_merge(sessions.session_type, EXCLUDED.session_type),
			empty_kind         = CASE
			                       WHEN session_type_merge(sessions.session_type, EXCLUDED.session_type) <> 'empty' THEN NULL
			                       WHEN sessions.ended OR EXCLUDED.ended THEN
			                         CASE WHEN sessions.empty_kind = 'blank'
			                                OR $34 = 'prompt_input_exit'
			                                OR GREATEST(sessions.ended_at, EXCLUDED.ended_at)
			                                   - LEAST(sessions.started_at, EXCLUDED.started_at) >= interval '5 seconds'
			                              THEN 'blank' ELSE 'aborted' END
			                       ELSE sessions.empty_kind END,
			head_state         = CASE WHEN sessions.head_state = 'unknown'
			                            AND sessions.content_events + EXCLUDED.content_events > 0
			                          THEN 'complete' ELSE sessions.head_state END,
			mirror_request     = CASE WHEN sessions.mirror_request = ''
			                          THEN EXCLUDED.mirror_request
			                          ELSE sessions.mirror_request END,
			device_id          = COALESCE(sessions.device_id, EXCLUDED.device_id),
			parent_session_id  = COALESCE(EXCLUDED.parent_session_id, sessions.parent_session_id),
			lineage_source     = CASE WHEN EXCLUDED.parent_session_id IS NOT NULL
			                          THEN EXCLUDED.lineage_source ELSE sessions.lineage_source END,
			parent_record_uuid = COALESCE(EXCLUDED.parent_record_uuid, sessions.parent_record_uuid),
			cwd                = COALESCE(EXCLUDED.cwd, sessions.cwd),
			repo               = COALESCE(EXCLUDED.repo, sessions.repo),
			git_branch         = COALESCE(EXCLUDED.git_branch, sessions.git_branch),
			entrypoint         = CASE WHEN sessions.entrypoint = ''
			                          THEN EXCLUDED.entrypoint ELSE sessions.entrypoint END,
			launcher           = COALESCE(sessions.launcher, EXCLUDED.launcher),
			transcript_exists  = CASE WHEN sessions.transcript_exists IS NULL AND EXCLUDED.transcript_exists IS NULL
			                          THEN NULL
			                          ELSE COALESCE(sessions.transcript_exists, false)
			                               OR COALESCE(EXCLUDED.transcript_exists, false) END,
			harness_title      = COALESCE(EXCLUDED.harness_title, sessions.harness_title),
			started_at         = LEAST(sessions.started_at, EXCLUDED.started_at),
			ended_at           = GREATEST(sessions.ended_at, EXCLUDED.ended_at),
			ended              = sessions.ended OR EXCLUDED.ended,
			user_turns         = sessions.user_turns + EXCLUDED.user_turns,
			human_turns        = sessions.human_turns + EXCLUDED.human_turns,
			content_events     = sessions.content_events + EXCLUDED.content_events,
			tool_calls         = sessions.tool_calls + EXCLUDED.tool_calls,
			subagents          = sessions.subagents + EXCLUDED.subagents,
			errors             = sessions.errors + EXCLUDED.errors,
			harness_versions   = session_array_union(sessions.harness_versions, EXCLUDED.harness_versions),
			agent_versions     = session_array_union(sessions.agent_versions, EXCLUDED.agent_versions),
			tokens_input       = sessions.tokens_input + EXCLUDED.tokens_input,
			tokens_output      = sessions.tokens_output + EXCLUDED.tokens_output,
			tokens_cache_read  = sessions.tokens_cache_read + EXCLUDED.tokens_cache_read,
			tokens_cache_write = sessions.tokens_cache_write + EXCLUDED.tokens_cache_write,
			cost_usd           = sessions.cost_usd + EXCLUDED.cost_usd,
			redactions         = jsonb_counter_add(sessions.redactions, EXCLUDED.redactions),
			derive_dirty       = true,
			updated_at         = now()`,
		d.SessionID, d.Email, d.DeviceID, d.Source, d.ParentSessionID, d.Cwd, d.Repo, d.GitBranch,
		d.StartedAt, nullTime(d.EndedAt), d.Ended, d.UserTurns, d.ToolCalls, d.Subagents, d.Errors,
		d.HarnessVersions, d.TokensInput, d.TokensOutput, d.TokensCacheRead, d.TokensCacheWrite,
		d.CostUSD, redactions, sessionTypeOf(d.Automation, d.ContentEvents), d.MirrorRequest,
		d.LineageSource, d.ParentRecordUUID, d.HumanTurns, d.ContentEvents, d.AgentVersions,
		d.Entrypoint, launcher, d.TranscriptExists, d.HarnessTitle, d.EndReason)
	if err != nil {
		return fmt.Errorf("store: apply rollup %s: %w", d.SessionID, err)
	}
	return nil
}

// marshalLauncher renders the launcher facts for the jsonb column, or NULL
// when the batch carried none, so that a later batch's silence cannot erase
// what the SessionStart said.
func marshalLauncher(l *event.Launcher) (any, error) {
	if l == nil {
		return nil, nil
	}
	b, err := json.Marshal(l)
	if err != nil {
		return nil, fmt.Errorf("store: marshal launcher: %w", err)
	}
	return string(b), nil
}

// refreshFirstPrompt re-derives the session's opening prompt, where it came
// from, and whether it makes the session one of the harness's own.
//
// The opening prompt is the first thing a PERSON said on the main thread: the
// lowest-seq main-thread message whose kind is human or slash_command. The
// first user row of any kind was what this used to pick, and it produced 431
// sessions titled by the local-command caveat and lists where a subagent's
// task prompt named its parent's session. When no person spoke, an automation
// or internal session is named by its first prompt of any kind, because the
// template is the honest name of a helper run; any other session gets no
// title at all rather than the working directory's basename, which is what
// made forty sessions in one repo indistinguishable.
//
// The same row decides internal. It is recomputed from scratch on every batch
// that carried a prompt rather than merged forward, because batches arrive in
// any order: a template landing before the real opening prompt must not make
// the session internal for good, and the opening prompt landing later must
// take the verdict back. Automation is never taken back; the lattice's top is
// absolute.
//
// One statement, and a single-row index probe on (session_id, seq) for each
// candidate, not a scan of the session's history. The row is chosen by the
// database, because only it knows every batch that has arrived; the title is
// rendered in Go by the normalizer (a slash command stored as its on-disk
// envelope renders as "/name args"), because only Go knows how. The two meet
// on the event id: the increment carries its earliest candidates already
// rendered, and the statement adopts a rendering only when that candidate IS
// the session's earliest row. When the earliest row arrived in some earlier
// batch, the stored title was rendered then and is kept. Rows stored before
// the kind column existed carry an empty kind and count as a person's, which
// keeps every historical title as it was until the derive runner recomputes
// them; such a row keeps its stored provenance too, because an unclassified
// winner does not know that a person typed it. The internal-template
// prefixes cross as data, and so does the whitespace stripped before they
// are compared, so the decision is the normalizer's list applied by the
// database with the normalizer's own trimmer.
//
// One rendering happens in SQL: an automation or internal session whose
// first prompt was stored batches ago under a wrapper kind, and which had no
// title to keep, is named by that row's first line, bounded like a title.
// That is the case launcher evidence trailing the prompts produces (the
// start marker arrives after the prompt; the prompt was a notification), and
// the delta has no candidate for it because the row is not the delta's. The
// first line is what the normalizer renders for every wrapper kind but the
// two notifications (whose summary or stripped text it shows instead); the
// derive runner's titles step re-renders history through the normalizer and
// settles those.
func refreshFirstPrompt(ctx context.Context, q Queryer, d *SessionDelta) error {
	var humanID, humanTitle, anyID, anyTitle string
	var command bool
	if d.Title != nil {
		humanID, humanTitle, command = d.Title.EventID, d.Title.Title, d.Title.Command
	}
	if d.AnyPrompt != nil {
		anyID, anyTitle = d.AnyPrompt.EventID, d.AnyPrompt.Title
	}
	_, err := q.Exec(ctx, `
		UPDATE sessions s
		SET first_prompt = CASE
		        WHEN h.event_id IS NOT NULL THEN
		          CASE WHEN h.event_id = $2 THEN $3 ELSE s.first_prompt END
		        WHEN s.session_type = 'automation' OR i.internal THEN
		          CASE WHEN a.event_id = $4 THEN $5 ELSE COALESCE(s.first_prompt, a.title) END
		        ELSE NULL END,
		    title_source = CASE
		        WHEN h.event_id IS NOT NULL THEN
		          CASE WHEN h.kind = '' THEN s.title_source
		               WHEN h.kind = 'slash_command' OR (h.event_id = $2 AND $6::bool) THEN 'command'
		               ELSE 'human' END
		        WHEN (s.session_type = 'automation' OR i.internal) AND a.event_id IS NOT NULL
		          THEN 'automation_template'
		        ELSE 'none' END,
		    session_type = CASE
		        WHEN i.internal AND s.session_type <> 'automation' THEN 'internal'
		        WHEN NOT i.internal AND s.session_type = 'internal' THEN 'user'
		        ELSE s.session_type END,
		    derive_dirty = true,
		    updated_at = now()
		FROM (SELECT 1) AS one
		LEFT JOIN LATERAL (
			SELECT event_id, kind, text FROM messages
			WHERE session_id = $1 AND role = 'user' AND agent_id IS NULL
			  AND kind IN ('', 'human', 'slash_command')
			ORDER BY seq, occurred_at
			LIMIT 1) AS h ON true
		LEFT JOIN LATERAL (
			SELECT event_id, text,
			       left(btrim(split_part(ltrim(text, $8), E'\n', 1), $8), $9) AS title
			FROM messages
			WHERE session_id = $1 AND role = 'user' AND agent_id IS NULL
			ORDER BY seq, occurred_at
			LIMIT 1) AS a ON true
		CROSS JOIN LATERAL (
			SELECT EXISTS (
				SELECT 1 FROM unnest($7::text[]) AS p(prefix)
				WHERE left(ltrim(coalesce(h.text, a.text, ''), $8), length(p.prefix)) = p.prefix
			) AS internal) AS i
		WHERE s.session_id = $1`,
		d.SessionID, humanID, humanTitle, anyID, anyTitle, command,
		normalize.InternalTemplatePrefixes(), normalize.TemplateCutset, normalize.TitleMax)
	if err != nil {
		return fmt.Errorf("store: refresh first prompt %s: %w", d.SessionID, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Session reads
// ---------------------------------------------------------------------------

// Session is the stored rollup as the dashboard sees it.
type Session struct {
	SessionID        string         `json:"session_id"`
	Email            string         `json:"email"`
	DeviceID         string         `json:"device_id,omitempty"`
	Source           string         `json:"source"`
	Type             string         `json:"session_type"`
	ParentSessionID  string         `json:"parent_session_id,omitempty"`
	Cwd              string         `json:"cwd,omitempty"`
	Repo             string         `json:"repo,omitempty"`
	GitBranch        string         `json:"git_branch,omitempty"`
	StartedAt        time.Time      `json:"started_at"`
	EndedAt          *time.Time     `json:"ended_at,omitempty"`
	Ended            bool           `json:"ended"`
	UserTurns        int            `json:"user_turns"`
	ToolCalls        int            `json:"tool_calls"`
	Subagents        int            `json:"subagents"`
	Errors           int            `json:"errors"`
	FirstPrompt      string         `json:"first_prompt,omitempty"`
	HarnessVersions  []string       `json:"harness_versions,omitempty"`
	TokensInput      int64          `json:"tokens_input"`
	TokensOutput     int64          `json:"tokens_output"`
	TokensCacheRead  int64          `json:"tokens_cache_read"`
	TokensCacheWrite int64          `json:"tokens_cache_write"`
	CostUSD          float64        `json:"cost_usd"`
	Redactions       map[string]int `json:"redactions,omitempty"`
	// IngestedAt is when the record reached us, kept apart from StartedAt so a
	// backfilled session reads as historical rather than as brand new.
	IngestedAt time.Time `json:"ingested_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// sessionColumns is the projection every session read shares, so that adding a
// column cannot leave one reader silently behind the others.
//
// The lattice, title-provenance, launcher and lineage columns that 0014 and
// 0015 add are not projected yet. Every fake that scans this projection lives
// in a package another PR is editing in parallel, and the reader that would
// show those columns is that PR's; they join the projection together.
const sessionColumns = `s.session_id, s.email, s.device_id::text, s.source, s.session_type, s.parent_session_id,
	s.cwd, s.repo, s.git_branch, s.started_at, s.ended_at, s.ended,
	s.user_turns, s.tool_calls, s.subagents, s.errors, s.first_prompt, s.harness_versions,
	s.tokens_input, s.tokens_output, s.tokens_cache_read, s.tokens_cache_write,
	s.cost_usd::float8, s.redactions, s.ingested_at, s.updated_at`

// SessionTypes is every value session_type may hold, in display order. The
// list is the single authority: the filter whitelist, the URL parser and the
// chart legend all range over it rather than repeating the strings.
var SessionTypes = []string{"user", "internal", "automation", "empty"}

// sessionTypeOf names the type an increment is evidence for. Automation is
// positive evidence and absorbs everything; content proves a person or a
// machine did something; an increment with neither is a lifecycle-only
// session, which is empty until a later batch says otherwise. The merge
// function decides what the evidence does to a row that already has a type.
func sessionTypeOf(automation bool, contentEvents int) string {
	switch {
	case automation:
		return "automation"
	case contentEvents > 0:
		return "user"
	default:
		return "empty"
	}
}

// sessionTypeDefault is the predicate a read applies when the caller names no
// types: the lattice's two hidden classes leave every default view, unless the
// row's device reported capture loss around it, in which case hiding it would
// present a data loss as a session in which nothing happened.
func sessionTypeDefault(alias string) string {
	return fmt.Sprintf(`NOT (%[1]s.session_type IN ('empty', 'internal') AND %[1]s.head_state <> 'capture_loss')`, alias)
}

// sessionTypePredicate renders the session-type filter for a query. The list
// parameter carries three meanings and the SQL keeps them apart: NULL is a
// caller that named nothing, which gets the store default; a non-empty array
// is the caller's selection; an empty array is a caller whose selection
// survived validation with nothing in it, which selects nothing. The third
// case is not folded into the first because a request for "robot" answered
// with the default view hands back rows the caller never asked for, and a
// consumer paging that answer cannot tell it from a real result. Rendered
// from one place so the three meanings cannot drift between the list, the
// search and the analytics queries.
func sessionTypePredicate(alias, listParam string) string {
	return fmt.Sprintf(`(CASE WHEN %[2]s::text[] IS NULL THEN %[3]s
		ELSE %[1]s.session_type = ANY(%[2]s::text[]) END)`, alias, listParam, sessionTypeDefault(alias))
}

// validSessionTypes keeps only known values, preserving order and dropping
// duplicates. URL parameters are attacker-typed; anything unknown vanishes
// rather than reaching a WHERE clause. The shape of the result is the
// predicate's switch: nil, which pgx sends as a NULL array, when the caller
// named nothing; a non-nil slice when the caller named something, kept
// non-nil even when nothing survived so a selection made only of unknown
// names reaches SQL as '{}' and matches no row instead of becoming the
// default.
func validSessionTypes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := []string{}
	for _, want := range SessionTypes {
		for _, t := range in {
			if t == want {
				out = append(out, want)
				break
			}
		}
	}
	return out
}

func scanSession(row Row) (Session, error) {
	var (
		s          Session
		deviceID   *string
		parent     *string
		cwd        *string
		repo       *string
		branch     *string
		endedAt    *time.Time
		firstProm  *string
		versions   []string
		redactions []byte
	)
	err := row.Scan(&s.SessionID, &s.Email, &deviceID, &s.Source, &s.Type, &parent,
		&cwd, &repo, &branch, &s.StartedAt, &endedAt, &s.Ended,
		&s.UserTurns, &s.ToolCalls, &s.Subagents, &s.Errors, &firstProm, &versions,
		&s.TokensInput, &s.TokensOutput, &s.TokensCacheRead, &s.TokensCacheWrite,
		&s.CostUSD, &redactions, &s.IngestedAt, &s.UpdatedAt)
	if err != nil {
		return Session{}, err
	}
	s.DeviceID = deref(deviceID)
	s.ParentSessionID = deref(parent)
	s.Cwd = deref(cwd)
	s.Repo = deref(repo)
	s.GitBranch = deref(branch)
	s.FirstPrompt = deref(firstProm)
	s.EndedAt = endedAt
	s.HarnessVersions = versions
	if len(redactions) > 0 {
		_ = json.Unmarshal(redactions, &s.Redactions)
	}
	return s, nil
}

// SessionFilter narrows a listing. Times are event times, so a date range means
// when the work happened and not when it was uploaded.
type SessionFilter struct {
	Query  string
	Email  string
	Source string
	Repo   string
	// Types narrows to the given session types. Empty means the store default:
	// every type except the empty and internal classes, which no page wants
	// unless it asks for them by name (a capture_loss row is always in). The
	// default lives in the store rather than in each handler because the JSON
	// API, the web and the mirror all list sessions, and three handlers with
	// three ideas of "no filter" is how empties reached the API. The web
	// always sends an explicit list.
	Types  []string
	From   time.Time
	To     time.Time
	Limit  int
	Cursor string
}

// SessionPage is one page of a listing plus the cursor for the next.
type SessionPage struct {
	Sessions   []Session `json:"sessions"`
	NextCursor string    `json:"next_cursor,omitempty"`
}

// ListSessions returns sessions the viewer may see, newest first.
//
// Listing is not audited. It exposes metadata rather than transcript content,
// and an audit row per row of a paged list would bury the reads that matter
// under noise generated by the act of browsing.
func (s *Store) ListSessions(ctx context.Context, v Viewer, f SessionFilter) (SessionPage, error) {
	limit := clampLimit(f.Limit)
	cur, err := decodeCursor(f.Cursor)
	if err != nil {
		return SessionPage{}, err
	}

	// One extra row decides whether a next page exists without a second query.
	query := `
		SELECT ` + sessionColumns + `
		FROM sessions s
		WHERE ` + canRead("s", "$1::bool", "$2") + `
		  AND ($3 = '' OR s.email = $3)
		  AND ($4 = '' OR s.source = $4)
		  AND ($5 = '' OR s.repo = $5)
		  AND ($6::timestamptz IS NULL OR s.started_at >= $6)
		  AND ($7::timestamptz IS NULL OR s.started_at < $7)
		  AND ($8 = '' OR s.first_prompt ILIKE '%' || $8 || '%'
		               OR s.repo ILIKE '%' || $8 || '%'
		               OR s.cwd ILIKE '%' || $8 || '%'
		               OR s.git_branch ILIKE '%' || $8 || '%')
		  AND ` + sessionTypePredicate("s", "$9") + `
		  AND ($10::timestamptz IS NULL OR (s.started_at, s.session_id) < ($10, $11::text))
		ORDER BY s.started_at DESC, s.session_id DESC
		LIMIT $12`

	rows, err := s.db.Query(ctx, query,
		v.IsAdmin(), v.Email, f.Email, f.Source, f.Repo,
		nullTime(f.From), nullTime(f.To), f.Query, validSessionTypes(f.Types),
		nullTime(cur.StartedAt), cur.SessionID, limit+1)
	if err != nil {
		return SessionPage{}, fmt.Errorf("store: list sessions: %w", err)
	}
	defer rows.Close()

	var page SessionPage
	for rows.Next() {
		sess, err := scanSession(rows)
		if err != nil {
			return SessionPage{}, fmt.Errorf("store: scan session: %w", err)
		}
		page.Sessions = append(page.Sessions, sess)
	}
	if err := rows.Err(); err != nil {
		return SessionPage{}, fmt.Errorf("store: list sessions: %w", err)
	}
	if len(page.Sessions) > limit {
		last := page.Sessions[limit-1]
		page.Sessions = page.Sessions[:limit]
		page.NextCursor = encodeCursor(cursor{StartedAt: last.StartedAt, SessionID: last.SessionID})
	}
	return page, nil
}

// GetSession returns one session and audits the read when it is not the
// viewer's own.
//
// The authorization is part of the SELECT rather than a check around it, so a
// session the viewer may not see produces no row and is reported as absent.
// There is no code path that can distinguish "does not exist" from "not yours",
// which is the point: a 403 would confirm that a colleague ran something.
func (s *Store) GetSession(ctx context.Context, v Viewer, sessionID string) (Session, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Session{}, fmt.Errorf("store: begin session read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sess, err := authorizeSession(ctx, tx, v, sessionID)
	if err != nil {
		return Session{}, err
	}
	// Audited before the transaction commits, so a failed audit fails the read.
	if err := recordAccess(ctx, tx, []Access{{
		Viewer: v.Email, SessionID: sess.SessionID, Owner: sess.Email, Via: viaFor(v, sess.Email),
	}}); err != nil {
		return Session{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Session{}, fmt.Errorf("store: commit session read: %w", err)
	}
	return sess, nil
}

// authorizeSession loads a session the viewer is allowed to see, or reports it
// as absent. Every audited read starts here so the predicate is applied once.
func authorizeSession(ctx context.Context, q Queryer, v Viewer, sessionID string) (Session, error) {
	row := q.QueryRow(ctx, `
		SELECT `+sessionColumns+`
		FROM sessions s
		WHERE s.session_id = $3 AND `+canRead("s", "$1::bool", "$2"),
		v.IsAdmin(), v.Email, sessionID)
	sess, err := scanSession(row)
	if err != nil {
		if noRows(err) {
			return Session{}, ErrNotFound
		}
		return Session{}, fmt.Errorf("store: read session: %w", err)
	}
	return sess, nil
}

// StoredEvent is one row of the durable record.
type StoredEvent struct {
	ID         string          `json:"id"`
	SessionID  string          `json:"session_id"`
	Email      string          `json:"email"`
	Seq        int64           `json:"seq"`
	Type       string          `json:"type"`
	Origin     string          `json:"origin"`
	OccurredAt time.Time       `json:"occurred_at"`
	IngestedAt time.Time       `json:"ingested_at"`
	AgentID    string          `json:"agent_id,omitempty"`
	WorkflowID string          `json:"workflow_id,omitempty"`
	Model      string          `json:"model,omitempty"`
	ToolName   string          `json:"tool_name,omitempty"`
	Body       json.RawMessage `json:"body"`
}

// TimelineCursor is a position in a session's chronological event order:
// (occurred_at, agent thread, seq). Three fields because none alone is unique
// across a session — seq restarts per stream, timestamps collide across
// parallel agents, and the agent id breaks exactly that tie.
type TimelineCursor struct {
	At    time.Time
	Agent string
	Seq   int64
}

// TimelineRange selects a chronological window of a session's events.
type TimelineRange struct {
	After *TimelineCursor
	// AgentID, when set, restricts the window to one thread.
	AgentID string
	Limit   int
	// IncludeSuperseded returns the hook copies the runner elected out
	// alongside the transcript rows that stand in for them. The default
	// leaves them out, which is what makes a dual-origin session read once
	// per moment; a forensic read asks for them by name.
	IncludeSuperseded bool
}

// GetTimeline reads a window of a session's events in time order, with the
// agent id as tiebreak so one thread's simultaneous rows stay contiguous.
//
// It exists beside GetEvents rather than replacing it because seq order is the
// published API contract and this ordering is the READING contract: seq
// restarts per subagent stream, so ORDER BY seq round-robins every thread of a
// multi-agent session and the transcript renders as a shuffle. Authorization
// and audit mirror GetEvents exactly: the transcript is the sensitive read.
func (s *Store) GetTimeline(ctx context.Context, v Viewer, sessionID string, r TimelineRange) (EventPage, error) {
	limit := clampLimit(r.Limit)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return EventPage{}, fmt.Errorf("store: begin timeline read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sess, err := authorizeSession(ctx, tx, v, sessionID)
	if err != nil {
		return EventPage{}, err
	}
	if err := recordAccess(ctx, tx, []Access{{
		Viewer: v.Email, SessionID: sess.SessionID, Owner: sess.Email, Via: viaFor(v, sess.Email),
	}}); err != nil {
		return EventPage{}, err
	}

	var afterAt any
	var afterAgent, afterSeq any
	if r.After != nil {
		afterAt, afterAgent, afterSeq = r.After.At, r.After.Agent, r.After.Seq
	}
	// superseded_by is set on hook rows only (migration 0017), so the filter
	// can only ever remove a hook copy whose transcript twin is in the same
	// result; nothing a transcript recorded leaves a page.
	rows, err := tx.Query(ctx, `
		SELECT id, session_id, email, seq, type, origin, occurred_at, ingested_at,
		       agent_id, workflow_id, model, tool_name, body
		FROM events
		WHERE session_id = $1
		  AND ($2 = '' OR coalesce(agent_id, '') = $2)
		  AND ($3::timestamptz IS NULL
		       OR (occurred_at, coalesce(agent_id, ''), seq) > ($3, $4::text, $5::bigint))
		  AND ($7::bool OR superseded_by IS NULL)
		ORDER BY occurred_at, coalesce(agent_id, ''), seq
		LIMIT $6`, sessionID, r.AgentID, afterAt, afterAgent, afterSeq, limit+1, r.IncludeSuperseded)
	if err != nil {
		return EventPage{}, fmt.Errorf("store: read timeline: %w", err)
	}
	stored, err := scanStoredEvents(rows)
	if err != nil {
		return EventPage{}, err
	}

	var page EventPage
	page.Events = stored
	if len(page.Events) > limit {
		page.Events = page.Events[:limit]
		page.HasMore = true
	}
	if err := tx.Commit(ctx); err != nil {
		return EventPage{}, fmt.Errorf("store: commit timeline read: %w", err)
	}
	return page, nil
}

// EventRange selects a slice of a session's events.
type EventRange struct {
	// AfterSeq is an exclusive lower bound, and nil means start at the
	// beginning. The distinction is load-bearing rather than stylistic: the
	// backfill walker numbers a session's events from zero, so treating a zero
	// bound as "from the start" would silently hide the first event of every
	// imported session and nothing about the result would look wrong.
	AfterSeq *int64
	Limit    int
	// IncludeSuperseded returns the hook copies the runner elected out. Off
	// by default, as on the timeline; the resume bundle is unaffected either
	// way, because it is rebuilt from transcript rows and those are never
	// superseded.
	IncludeSuperseded bool
}

// EventPage is one page of a session's events in sequence order.
type EventPage struct {
	Events  []StoredEvent `json:"events"`
	HasMore bool          `json:"has_more"`
	// NextAfter feeds straight back into EventRange.AfterSeq.
	NextAfter *int64 `json:"next_after,omitempty"`
}

// GetEvents returns a session's events ordered by sequence, and audits the read.
//
// Ordering is by seq and not by arrival, because retries and backfill do not
// preserve arrival order and a transcript rendered in arrival order is
// nonsense. Paging is keyset on seq for the same reason it is elsewhere: an
// offset into a table that is still being written to skips rows.
func (s *Store) GetEvents(ctx context.Context, v Viewer, sessionID string, r EventRange) (EventPage, error) {
	limit := clampLimit(r.Limit)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return EventPage{}, fmt.Errorf("store: begin event read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	sess, err := authorizeSession(ctx, tx, v, sessionID)
	if err != nil {
		return EventPage{}, err
	}
	if err := recordAccess(ctx, tx, []Access{{
		Viewer: v.Email, SessionID: sess.SessionID, Owner: sess.Email, Via: viaFor(v, sess.Email),
	}}); err != nil {
		return EventPage{}, err
	}

	stored, err := readEvents(ctx, tx, sessionID, r.AfterSeq, limit+1, r.IncludeSuperseded)
	if err != nil {
		return EventPage{}, err
	}

	var page EventPage
	page.Events = stored
	if len(page.Events) > limit {
		page.Events = page.Events[:limit]
		page.HasMore = true
		next := page.Events[limit-1].Seq
		page.NextAfter = &next
	}
	if err := tx.Commit(ctx); err != nil {
		return EventPage{}, fmt.Errorf("store: commit event read: %w", err)
	}
	return page, nil
}

// readEvents is separated from GetEvents so that the result set is closed when
// it returns. A transaction cannot be committed while a result set on it is
// still open, and a deferred close in the caller would run after the commit.
//
// The superseded filter is the timeline's, applied to the same hook-only
// column, so the seq-ordered read and the time-ordered read agree about
// which rows a page holds.
func readEvents(ctx context.Context, q Queryer, sessionID string, afterSeq *int64, limit int, includeSuperseded bool) ([]StoredEvent, error) {
	rows, err := q.Query(ctx, `
		SELECT id, session_id, email, seq, type, origin, occurred_at, ingested_at,
		       agent_id, workflow_id, model, tool_name, body
		FROM events
		WHERE session_id = $1 AND ($2::bigint IS NULL OR seq > $2)
		  AND ($4::bool OR superseded_by IS NULL)
		ORDER BY seq
		LIMIT $3`, sessionID, afterSeq, limit, includeSuperseded)
	if err != nil {
		return nil, fmt.Errorf("store: read events: %w", err)
	}
	return scanStoredEvents(rows)
}

// scanStoredEvents drains one events result set. Shared by the seq-ordered and
// timeline reads, whose SELECT lists must stay identical for this to hold.
func scanStoredEvents(rows Rows) ([]StoredEvent, error) {
	defer rows.Close()
	var out []StoredEvent
	for rows.Next() {
		var (
			e        StoredEvent
			agent    *string
			workflow *string
			model    *string
			tool     *string
			body     []byte
		)
		if err := rows.Scan(&e.ID, &e.SessionID, &e.Email, &e.Seq, &e.Type, &e.Origin,
			&e.OccurredAt, &e.IngestedAt, &agent, &workflow, &model, &tool, &body); err != nil {
			return nil, fmt.Errorf("store: scan event: %w", err)
		}
		e.AgentID = deref(agent)
		e.WorkflowID = deref(workflow)
		e.Model = deref(model)
		e.ToolName = deref(tool)
		e.Body = append(json.RawMessage(nil), body...)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read events: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

// SearchFilter narrows a full-text search. Email, source and the date range are
// applied before ranking, which is what keeps the query inside its latency
// budget; see SearchCandidateCap.
type SearchFilter struct {
	Query  string
	Email  string
	Source string
	// Types narrows hits to sessions of the given types; empty means the store
	// default, exactly as on SessionFilter. It must mirror the list filter:
	// the search results render on the same page as the list, and a type the
	// list suppresses reappearing as a transcript hit is the filter lying.
	Types  []string
	From   time.Time
	To     time.Time
	Limit  int
	Offset int
	// CandidateCap overrides SearchCandidateCap. Present so an operator can tune
	// the ranking budget without a deploy, not because callers should set it.
	CandidateCap int
	// IncludeHarness widens the corpus to the harness's own user-role
	// messages: caveats, command output, notifications, reminders, compaction
	// summaries. Off by default, because a search for a word that appears in
	// every <system-reminder> the harness injects would rank the reminders
	// above the person who typed it. Rows stored before kinds existed carry
	// kind '' and are read as a person's, which is what they mostly are.
	IncludeHarness bool
}

// Hit is one matching message.
type Hit struct {
	EventID   string `json:"event_id"`
	SessionID string `json:"session_id"`
	Email     string `json:"email"`
	Seq       int64  `json:"seq"`
	Role      string `json:"role"`
	// Kind is messages.kind: what the message is (human, slash_command, a
	// harness wrapper, assistant_text, tool), so a result can be labelled by
	// what matched rather than by which side of the conversation it sat on.
	Kind       string    `json:"kind,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
	Snippet    string    `json:"snippet"`
	Rank       float64   `json:"rank"`
}

// SearchResult is a page of hits.
type SearchResult struct {
	Hits []Hit `json:"hits"`
	// Candidates is how many matches were considered, saturating at the cap. It
	// is reported so the UI can say "500+" honestly instead of implying the
	// number is a total.
	Candidates int  `json:"candidates"`
	Capped     bool `json:"capped"`
}

// SearchMessages runs a full-text search scoped to what the viewer may read.
//
// The query shape is fixed by a property of Postgres rather than by preference.
// ts_rank has to read the tsvector of every row it ranks, so ranking is the part
// that scales with the size of the match set, and a search for a common word
// across a whole fleet's corpus matches far more than any page will show. So the
// cheap predicates run first, the survivors are capped, only the capped set is
// ranked, and ts_headline, which is more expensive still because it re-parses
// the document text, touches only the rows actually being returned.
//
// Hits in other people's sessions are audited, in the same transaction, for the
// same reason opening one of those sessions is: the search result already shows
// their words.
func (s *Store) SearchMessages(ctx context.Context, v Viewer, f SearchFilter) (SearchResult, error) {
	if strings.TrimSpace(f.Query) == "" {
		return SearchResult{}, nil
	}
	limit := clampLimit(f.Limit)
	candidateCap := f.CandidateCap
	if candidateCap <= 0 {
		candidateCap = SearchCandidateCap
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return SearchResult{}, fmt.Errorf("store: begin search: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	res, err := runSearch(ctx, tx, v, f, candidateCap, limit, offset)
	if err != nil {
		return SearchResult{}, err
	}

	// One audit row per distinct session, not per hit: the audited fact is that
	// this viewer saw content from that session, and a page of twenty hits in
	// one transcript is one such fact.
	var audits []Access
	seen := make(map[string]bool, len(res.Hits))
	for _, h := range res.Hits {
		if h.Email == v.Email || seen[h.SessionID] {
			continue
		}
		seen[h.SessionID] = true
		audits = append(audits, Access{
			Viewer: v.Email, SessionID: h.SessionID, Owner: h.Email, Via: viaFor(v, h.Email),
		})
	}
	if err := recordAccess(ctx, tx, audits); err != nil {
		return SearchResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SearchResult{}, fmt.Errorf("store: commit search: %w", err)
	}
	return res, nil
}

// runSearch issues the ranked query. It is separate from SearchMessages so the
// result set is closed before the audit write and the commit reuse the same
// transaction.
func runSearch(ctx context.Context, q Queryer, v Viewer, f SearchFilter, candidateCap, limit, offset int) (SearchResult, error) {
	query := `
		WITH q AS (
			SELECT websearch_to_tsquery('english', $1) AS tsq
		),
		candidates AS (
			SELECT m.event_id, m.session_id, m.email, m.seq, m.role, m.kind, m.occurred_at, m.text, m.tsv
			FROM messages m, q
			WHERE m.tsv @@ q.tsq
			  AND ` + canRead("m", "$2::bool", "$3") + `
			  AND ($4 = '' OR m.email = $4)
			  AND ($5::timestamptz IS NULL OR m.occurred_at >= $5)
			  AND ($6::timestamptz IS NULL OR m.occurred_at < $6)
			  AND ($7 = '' OR EXISTS (
			        SELECT 1 FROM sessions s
			        WHERE s.session_id = m.session_id AND s.source = $7))
			  AND EXISTS (
			        SELECT 1 FROM sessions s
			        WHERE s.session_id = m.session_id AND ` + sessionTypePredicate("s", "$8") + `
			          -- The harness's own user-role messages leave the corpus
			          -- unless asked for, except inside an internal session:
			          -- there the harness prompt is the session's only content
			          -- and its title, and a caller who asked for internal
			          -- sessions asked for exactly that. '' is a row stored
			          -- before kinds existed.
			          AND ($12::bool OR m.role <> 'user' OR m.kind IN ('', 'human', 'slash_command')
			               OR s.session_type = 'internal'))
			ORDER BY m.occurred_at DESC
			LIMIT $9
		),
		ranked AS (
			-- The rank is weighted by what matched, in the same statement as
			-- the match: a person's words first, the answer a turn closed on
			-- next, other assistant text after, tool output last. The final
			-- answer is the row turns.final_event_id names, probed per
			-- candidate over the session's own turns.
			SELECT c.event_id, c.session_id, c.email, c.seq, c.role, c.kind, c.occurred_at, c.text,
			       ts_rank(c.tsv, q.tsq) * CASE
			         WHEN c.kind IN ('human', 'slash_command') THEN 2.0
			         WHEN c.role = 'assistant' AND EXISTS (
			              SELECT 1 FROM turns t
			              WHERE t.session_id = c.session_id AND t.final_event_id = c.event_id) THEN 1.8
			         WHEN c.role = 'assistant' THEN 1.3
			         ELSE 1.0
			       END AS rank
			FROM candidates c, q
			ORDER BY rank DESC, c.occurred_at DESC
			LIMIT $10 OFFSET $11
		)
		SELECT r.event_id, r.session_id, r.email, r.seq, r.role, r.kind, r.occurred_at,
		       ts_headline('english', r.text, q.tsq,
		                   'MaxFragments=2,MinWords=5,MaxWords=24,ShortWord=3'),
		       r.rank,
		       (SELECT count(*) FROM candidates)
		FROM ranked r, q
		ORDER BY r.rank DESC, r.occurred_at DESC`

	rows, err := q.Query(ctx, query,
		f.Query, v.IsAdmin(), v.Email, f.Email,
		nullTime(f.From), nullTime(f.To), f.Source, validSessionTypes(f.Types),
		candidateCap, limit, offset, f.IncludeHarness)
	if err != nil {
		return SearchResult{}, fmt.Errorf("store: search: %w", err)
	}
	defer rows.Close()

	var res SearchResult
	var candidates int64
	for rows.Next() {
		var h Hit
		if err := rows.Scan(&h.EventID, &h.SessionID, &h.Email, &h.Seq, &h.Role, &h.Kind,
			&h.OccurredAt, &h.Snippet, &h.Rank, &candidates); err != nil {
			return SearchResult{}, fmt.Errorf("store: scan hit: %w", err)
		}
		res.Hits = append(res.Hits, h)
	}
	if err := rows.Err(); err != nil {
		return SearchResult{}, fmt.Errorf("store: search: %w", err)
	}
	res.Candidates = int(candidates)
	res.Capped = res.Candidates >= candidateCap
	return res, nil
}

// ---------------------------------------------------------------------------
// Shares
// ---------------------------------------------------------------------------

// Share is an explicit grant on one session, on top of the role model.
type Share struct {
	ID        string     `json:"id"`
	SessionID string     `json:"session_id"`
	CreatedBy string     `json:"created_by"`
	Grantee   string     `json:"grantee,omitempty"`
	Token     string     `json:"token,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// ShareRequest asks for a grant. An empty Grantee means any authenticated
// employee holding the link, which is the weaker of the two grants and so is
// the one that has to be asked for explicitly rather than being the default of
// a missing field.
type ShareRequest struct {
	SessionID string
	Grantee   string
	ExpiresAt time.Time
}

// CreateShare mints a share link for a session.
//
// Only the owner or an admin may share, and a viewer who is neither is told the
// session does not exist rather than that they may not share it, for the same
// leakage reason as every other read here.
func (s *Store) CreateShare(ctx context.Context, v Viewer, req ShareRequest) (Share, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Share{}, fmt.Errorf("store: begin share: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var owner string
	err = tx.QueryRow(ctx, `
		SELECT email FROM sessions
		WHERE session_id = $1 AND (email = $2 OR $3::bool)`,
		req.SessionID, v.Email, v.IsAdmin()).Scan(&owner)
	if err != nil {
		if noRows(err) {
			return Share{}, ErrNotFound
		}
		return Share{}, fmt.Errorf("store: read session for share: %w", err)
	}

	sh := Share{
		ID:        newUUID(),
		SessionID: req.SessionID,
		CreatedBy: v.Email,
		Grantee:   req.Grantee,
		Token:     newToken(),
	}
	var expires *time.Time
	if !req.ExpiresAt.IsZero() {
		expires = &req.ExpiresAt
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO shares (id, session_id, created_by, grantee, token, expires_at)
		VALUES ($1::uuid, $2, $3, nullif($4,''), $5, $6)
		RETURNING created_at`,
		sh.ID, sh.SessionID, sh.CreatedBy, sh.Grantee, sh.Token, expires).Scan(&sh.CreatedAt)
	if err != nil {
		return Share{}, fmt.Errorf("store: create share: %w", err)
	}
	sh.ExpiresAt = expires

	// Creating a share on someone else's session is an admin action against
	// their work, so it is audited like a read of it.
	if err := recordAccess(ctx, tx, []Access{{
		Viewer: v.Email, SessionID: req.SessionID, Owner: owner, Via: viaFor(v, owner),
	}}); err != nil {
		return Share{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Share{}, fmt.Errorf("store: commit share: %w", err)
	}
	return sh, nil
}

// ResolveShare exchanges a link secret for the session it grants, and audits it.
//
// The caller still has to be an authenticated employee: the token names which
// session the link is for, it does not replace knowing who is looking, because
// an unattributable read cannot be audited and an unauditable read of a
// colleague's transcript is the thing this system promises not to have.
func (s *Store) ResolveShare(ctx context.Context, v Viewer, token string) (Session, Share, error) {
	if token == "" {
		return Session{}, Share{}, ErrNotFound
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Session{}, Share{}, fmt.Errorf("store: begin resolve share: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		sh      Share
		grantee *string
		expires *time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT id::text, session_id, created_by, grantee, created_at, expires_at
		FROM shares
		WHERE token = $1
		  AND revoked_at IS NULL
		  AND (expires_at IS NULL OR expires_at > now())
		  AND (grantee IS NULL OR grantee = $2)`,
		token, v.Email).Scan(&sh.ID, &sh.SessionID, &sh.CreatedBy, &grantee, &sh.CreatedAt, &expires)
	if err != nil {
		if noRows(err) {
			return Session{}, Share{}, ErrNotFound
		}
		return Session{}, Share{}, fmt.Errorf("store: resolve share: %w", err)
	}
	sh.Grantee = deref(grantee)
	sh.ExpiresAt = expires

	sess, err := authorizeSession(ctx, tx, v, sh.SessionID)
	if err != nil {
		return Session{}, Share{}, err
	}
	if err := recordAccess(ctx, tx, []Access{{
		Viewer: v.Email, SessionID: sess.SessionID, Owner: sess.Email, Via: viaFor(v, sess.Email),
	}}); err != nil {
		return Session{}, Share{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Session{}, Share{}, fmt.Errorf("store: commit resolve share: %w", err)
	}
	return sess, sh, nil
}

// RevokeShare withdraws a grant. Revocation is a timestamp rather than a delete
// so that a share that was live during an incident can still be explained
// afterwards.
func (s *Store) RevokeShare(ctx context.Context, v Viewer, shareID string) error {
	n, err := s.db.Exec(ctx, `
		UPDATE shares sh
		SET revoked_at = now()
		FROM sessions s
		WHERE sh.id = $1::uuid
		  AND sh.session_id = s.session_id
		  AND sh.revoked_at IS NULL
		  AND (sh.created_by = $2 OR s.email = $2 OR $3::bool)`,
		shareID, v.Email, v.IsAdmin())
	if err != nil {
		return fmt.Errorf("store: revoke share: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// ListShares returns the live grants on a session, for the owner or an admin.
func (s *Store) ListShares(ctx context.Context, v Viewer, sessionID string) ([]Share, error) {
	rows, err := s.db.Query(ctx, `
		SELECT sh.id::text, sh.session_id, sh.created_by, sh.grantee, sh.token,
		       sh.created_at, sh.expires_at, sh.revoked_at
		FROM shares sh
		JOIN sessions s ON s.session_id = sh.session_id
		WHERE sh.session_id = $1 AND (s.email = $2 OR $3::bool)
		ORDER BY sh.created_at DESC`,
		sessionID, v.Email, v.IsAdmin())
	if err != nil {
		return nil, fmt.Errorf("store: list shares: %w", err)
	}
	defer rows.Close()

	var out []Share
	for rows.Next() {
		var (
			sh      Share
			grantee *string
			token   *string
		)
		if err := rows.Scan(&sh.ID, &sh.SessionID, &sh.CreatedBy, &grantee, &token,
			&sh.CreatedAt, &sh.ExpiresAt, &sh.RevokedAt); err != nil {
			return nil, fmt.Errorf("store: scan share: %w", err)
		}
		sh.Grantee = deref(grantee)
		sh.Token = deref(token)
		out = append(out, sh)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Principals and devices
// ---------------------------------------------------------------------------

const principalColumns = `email, role, display_name, added_by, added_at, disabled_at`

func scanPrincipal(row Row) (Principal, error) {
	var (
		p       Principal
		name    *string
		addedBy *string
	)
	if err := row.Scan(&p.Email, &p.Role, &name, &addedBy, &p.AddedAt, &p.DisabledAt); err != nil {
		return Principal{}, err
	}
	p.DisplayName = deref(name)
	p.AddedBy = deref(addedBy)
	return p, nil
}

// Principal loads one access-list row. The authentication layer calls this on
// every request to turn a verified email into a role, which is why disabled
// principals are returned rather than hidden: the caller has to be able to tell
// "not enrolled" from "switched off".
func (s *Store) Principal(ctx context.Context, email string) (Principal, error) {
	p, err := scanPrincipal(s.db.QueryRow(ctx,
		`SELECT `+principalColumns+` FROM principals WHERE email = $1`, email))
	if err != nil {
		if noRows(err) {
			return Principal{}, ErrNotFound
		}
		return Principal{}, fmt.Errorf("store: read principal: %w", err)
	}
	return p, nil
}

// ListPrincipals returns the access list for the admin page.
func (s *Store) ListPrincipals(ctx context.Context, v Viewer) ([]Principal, error) {
	if !v.IsAdmin() {
		return nil, ErrNotAdmin
	}
	rows, err := s.db.Query(ctx, `SELECT `+principalColumns+` FROM principals ORDER BY email`)
	if err != nil {
		return nil, fmt.Errorf("store: list principals: %w", err)
	}
	defer rows.Close()

	var out []Principal
	for rows.Next() {
		p, err := scanPrincipal(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan principal: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// PrincipalUpdate is the admin page's edit. A nil pointer leaves the field
// alone, which is what makes "disable this person" and "rename this person"
// separate operations against the same endpoint.
type PrincipalUpdate struct {
	Email       string
	Role        *Role
	DisplayName *string
	Disabled    *bool
}

// PrincipalSave is what a roster edit did: the row as it now stands, and the
// audit record of the move if there was one.
//
// Change is nil when the submitted values were the ones already on the row.
// That is neither a failure nor an edit, and the caller has to be able to tell
// it from both. A page that renders every accepted submission as "saved" says
// the change landed on precisely the occasions when there was no change, and
// from outside that is indistinguishable from a write that was lost — which is
// the worst thing a permissions page can be wrong about.
type PrincipalSave struct {
	Principal
	Change *PrincipalChange
}

// PutPrincipal creates or edits an access-list row.
//
// The guard is the interesting part. Removing the last admin, whether by
// demotion or by disabling them, leaves a system nobody can administer without
// direct database access, so it is refused. The check runs inside the
// transaction and takes row locks on the current admins first, because two
// admins demoting each other at the same moment would each see the other and
// both succeed.
//
// The roster write and the audit row are the same transaction, for the same
// reason AdminTx exists: an audit row that can fail on its own is not an audit
// trail. This is the door a browser reaches and AdminTx is the door the JSON
// API reaches, and the obligation to leave a trail cannot depend on which one
// was used — an operator who grants somebody standing visibility over
// colleagues' transcripts from the page they were given has to be as
// accountable as one who does it with curl.
//
// A submission carrying the values already on the row is not an edit and writes
// nothing at all, not even a rewritten tuple. Rewriting the row would leave the
// database unable to distinguish a request that changed nothing from one whose
// change was lost, which is precisely the question somebody asks when a
// permissions page has said "saved" and the roster looks untouched.
func (s *Store) PutPrincipal(ctx context.Context, actor Viewer, u PrincipalUpdate) (PrincipalSave, error) {
	if !actor.IsAdmin() {
		return PrincipalSave{}, ErrNotAdmin
	}
	if u.Email == "" {
		return PrincipalSave{}, fmt.Errorf("store: principal update needs an email")
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return PrincipalSave{}, fmt.Errorf("store: begin principal update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var others int
	if err := tx.QueryRow(ctx, `
		WITH locked AS (
			SELECT email FROM principals
			WHERE role = 'admin' AND disabled_at IS NULL
			-- Deterministic lock order. Without it two concurrent demotions can
			-- take the same admin rows in opposite orders and deadlock, where
			-- the point of taking the locks at all is to make the second one
			-- queue behind the first and then observe the roster it left.
			ORDER BY email
			FOR UPDATE
		)
		SELECT count(*) FROM locked WHERE email <> $1`, u.Email).Scan(&others); err != nil {
		return PrincipalSave{}, fmt.Errorf("store: count admins: %w", err)
	}

	current, err := scanPrincipal(tx.QueryRow(ctx,
		`SELECT `+principalColumns+` FROM principals WHERE email = $1`, u.Email))
	exists := true
	if err != nil {
		if !noRows(err) {
			return PrincipalSave{}, fmt.Errorf("store: read principal: %w", err)
		}
		exists = false
	}

	role := RoleMember
	disabled := false
	if exists {
		role = current.Role
		disabled = current.DisabledAt != nil
	}
	if u.Role != nil {
		role = *u.Role
	}
	if u.Disabled != nil {
		disabled = *u.Disabled
	}
	if role != RoleAdmin && role != RoleMember {
		return PrincipalSave{}, fmt.Errorf("store: unknown role %q", role)
	}
	wasActiveAdmin := exists && current.Role == RoleAdmin && current.DisabledAt == nil
	if wasActiveAdmin && (role != RoleAdmin || disabled) && others == 0 {
		return PrincipalSave{}, ErrLastAdmin
	}

	name := ""
	if exists {
		name = current.DisplayName
	}
	if u.DisplayName != nil {
		name = *u.DisplayName
	}

	// Compared on the three fields an edit is about, matching the admin
	// package's own rule. The disable stamp is compared as a boolean because
	// re-disabling somebody who is already disabled would otherwise move the
	// timestamp and read, a year later, as a second offboarding.
	if exists && role == current.Role && name == current.DisplayName &&
		disabled == (current.DisabledAt != nil) {
		// The deferred rollback ends the transaction and releases the admin row
		// locks. There is nothing to commit: no statement below this point ran.
		return PrincipalSave{Principal: current}, nil
	}

	// One instant for both the disable stamp and the audit row, so the column
	// that says when access ended and the trail row that explains it cannot
	// disagree by the width of the transaction.
	now := time.Now().UTC()
	var disabledAt *time.Time
	switch {
	case disabled && exists && current.DisabledAt != nil:
		disabledAt = current.DisabledAt
	case disabled:
		disabledAt = &now
	}

	p, err := scanPrincipal(tx.QueryRow(ctx, `
		INSERT INTO principals (email, role, display_name, added_by, disabled_at)
		VALUES ($1, $2, nullif($3,''), $4, $5)
		ON CONFLICT (email) DO UPDATE SET
			role         = EXCLUDED.role,
			display_name = COALESCE(EXCLUDED.display_name, principals.display_name),
			disabled_at  = EXCLUDED.disabled_at
		RETURNING `+principalColumns,
		u.Email, string(role), name, actor.Email, disabledAt))
	if err != nil {
		return PrincipalSave{}, fmt.Errorf("store: write principal: %w", err)
	}

	c := PrincipalChange{
		Actor:        actor.Email,
		Target:       u.Email,
		ToRole:       role,
		ToDisabled:   disabled,
		FromDisabled: exists && current.DisabledAt != nil,
		Created:      !exists,
		At:           now,
	}
	if exists {
		c.FromRole = current.Role
	}
	// Written through the statement AdminTx uses rather than a second copy of
	// it. Two spellings of the same trail row are two things to keep correct,
	// and a divergence would show up as a trail where a creation and a
	// promotion are no longer distinguishable — which is the one question the
	// table is asked.
	if err := (adminTx{tx: tx, domains: s.deployment.AllowedDomains}).RecordPrincipalChange(ctx, c); err != nil {
		return PrincipalSave{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return PrincipalSave{}, fmt.Errorf("store: commit principal update: %w", err)
	}
	return PrincipalSave{Principal: p, Change: &c}, nil
}

// Device is an enrolled machine.
type Device struct {
	ID           string     `json:"id"`
	Email        string     `json:"email"`
	Hostname     string     `json:"hostname,omitempty"`
	OS           string     `json:"os,omitempty"`
	Arch         string     `json:"arch,omitempty"`
	AgentVersion string     `json:"agent_version,omitempty"`
	EnrolledAt   time.Time  `json:"enrolled_at"`
	LastSeenAt   *time.Time `json:"last_seen_at,omitempty"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
}

// DeviceIdentity is what a presented device token resolves to.
type DeviceIdentity struct {
	DeviceID string
	Email    string
	Role     Role
}

// EnrollDevice records a machine and the hash of the token it will present.
//
// Only the hash is stored. The token itself is shown to the enrolling employee
// once and never again, so a dump of this table is not a set of working
// credentials.
//
// d.ID is the laptop's claim to a row that already exists, and is a claim rather
// than an instruction: it is honoured only when it names a live device of
// d.Email's, and is otherwise replaced by a fresh id. A claim that is malformed,
// unknown, revoked or somebody else's must still enrol the machine rather than
// fail it, because whoever presents it has already proved which person they are
// — what they have not proved is which machine.
func (s *Store) EnrollDevice(ctx context.Context, d Device, tokenHash []byte, expiresAt time.Time) (Device, error) {
	if d.Email == "" || len(tokenHash) == 0 {
		return Device{}, fmt.Errorf("store: enrollment needs an email and a token hash")
	}
	claimed := d.ID
	d.ID = ""

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Device{}, fmt.Errorf("store: begin enrollment: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if isUUID(claimed) {
		// Locked rather than merely read, because the row is decided here and
		// written at the bottom of this function. A revocation committing
		// between the two would otherwise be undone by the update; holding the
		// row makes both orders correct, since either this sees the revocation
		// and declines the claim, or the revocation lands afterwards and the
		// machine ends up revoked, which is what the admin asked for.
		var id string
		err := tx.QueryRow(ctx, `
			SELECT id::text FROM devices
			WHERE id = $1::uuid AND email = $2 AND revoked_at IS NULL
			FOR UPDATE`, claimed, d.Email).Scan(&id)
		switch {
		case err == nil:
			d.ID = id
		case noRows(err):
			// Unknown, revoked, or another person's. All three get the same
			// answer, and it is not an error: this enrolment takes a row of its
			// own and leaves whatever the claim named exactly as it was.
		default:
			return Device{}, fmt.Errorf("store: resolve the claimed device: %w", err)
		}
	}

	if d.ID == "" {
		d.ID = newUUID()
		if d.Hostname != "" {
			if err := supersedeHost(ctx, tx, d.Email, d.Hostname); err != nil {
				return Device{}, err
			}
		}
	}

	// The two predicates on the conflict branch are the enrolment invariants,
	// stated where they are enforced rather than left to the code above to
	// uphold. The email one keeps one person's enrolment off another person's
	// row; the revocation one is why this no longer clears revoked_at, which
	// would have made "revoke a stolen laptop" undoable by re-running the
	// installer on it. Neither is reachable given the resolution above, and both
	// stay because the resolution is the sort of thing a later change loosens.
	err = tx.QueryRow(ctx, `
		INSERT INTO devices (id, email, hostname, os, arch, agent_version)
		SELECT $1::uuid, p.email, nullif($3,''), nullif($4,''), nullif($5,''), nullif($6,'')
		FROM principals p
		WHERE p.email = $2 AND p.disabled_at IS NULL
		ON CONFLICT (id) DO UPDATE SET
			hostname      = COALESCE(EXCLUDED.hostname, devices.hostname),
			os            = COALESCE(EXCLUDED.os, devices.os),
			arch          = COALESCE(EXCLUDED.arch, devices.arch),
			agent_version = COALESCE(EXCLUDED.agent_version, devices.agent_version)
		WHERE devices.email = EXCLUDED.email AND devices.revoked_at IS NULL
		RETURNING enrolled_at`,
		d.ID, d.Email, d.Hostname, d.OS, d.Arch, d.AgentVersion).Scan(&d.EnrolledAt)
	if err != nil {
		// No row means the principal is absent or switched off, which is the
		// enrollment refusal the contract describes rather than a server fault.
		if noRows(err) {
			return Device{}, ErrNotFound
		}
		return Device{}, fmt.Errorf("store: enroll device: %w", err)
	}

	var expires *time.Time
	if !expiresAt.IsZero() {
		expires = &expiresAt
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO device_tokens (id, device_id, token_hash, expires_at)
		VALUES ($1::uuid, $2::uuid, $3, $4)`,
		newUUID(), d.ID, tokenHash, expires); err != nil {
		return Device{}, fmt.Errorf("store: issue device token: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Device{}, fmt.Errorf("store: commit enrollment: %w", err)
	}
	return d, nil
}

// supersedeHost retires this person's other live devices carrying the same
// hostname, and the credentials they hold.
//
// This is the weaker of the two ways an enrolling laptop is recognised as one
// already on file, and it is consulted only when the stronger one had nothing to
// say. The stronger one is the id the client kept from its last enrolment: that
// id was minted for that machine and stored nowhere else, so it is proof, and
// where it is present the row is simply reused and this never runs. A hostname
// is a guess — one person can own two machines named the same thing, and a
// rename changes it — so it can both miss a laptop that came back and hit one
// that never left.
//
// The guess is still worth making, because the case it covers is the case that
// produces phantoms in practice: a machine whose config was deleted has no id to
// send and is nonetheless the same machine, so every reinstall would otherwise
// leave a row behind that can never report again. admin/fleet.go counts coverage
// per (email, device_id) deliberately, so that somebody with one working and one
// dead laptop is not counted healthy — which means an abandoned row is
// indistinguishable from a genuinely dead machine forever, and drags the number
// down by one per reinstall with nothing on the page to explain it.
//
// The two failure modes decide the order. Guessing wrong revokes a machine that
// is alive: it stops delivering, what it captures stays spooled on its own disk,
// the fleet page shows it going silent, and a human puts it back with one
// UPDATE. Not guessing leaves a wrong coverage number that no one can explain
// and no one can repair, because nothing anywhere records that the two rows were
// one laptop. Revoking is reversible; a lost identity is not.
//
// One statement so the device and its tokens cannot disagree, and devices are
// updated before device_tokens because RevokeDevice takes them in that order and
// two orders is a deadlock.
func supersedeHost(ctx context.Context, tx Tx, email, hostname string) error {
	if _, err := tx.Exec(ctx, `
		WITH superseded AS (
			UPDATE devices SET revoked_at = now()
			WHERE email = $1 AND hostname = $2 AND revoked_at IS NULL
			RETURNING id
		)
		UPDATE device_tokens t SET revoked_at = now()
		FROM superseded s
		WHERE t.device_id = s.id AND t.revoked_at IS NULL`, email, hostname); err != nil {
		return fmt.Errorf("store: supersede the prior device on %s: %w", hostname, err)
	}
	return nil
}

// isUUID reports whether s is shaped like the ids newUUID mints.
//
// Checked here because the value arrives from a client: a malformed one reaching
// the ::uuid cast is a statement error, which would fail an enrolment that this
// package would rather answer by handing out a fresh id.
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// AuthenticateDevice resolves a presented token hash to the identity behind it.
//
// The joins are the authorization: a token is only good while its own row is
// live, the device it belongs to has not been revoked, and the principal is not
// disabled. Checking those separately would leave three windows in which a
// revoked laptop still uploads.
func (s *Store) AuthenticateDevice(ctx context.Context, tokenHash []byte) (DeviceIdentity, error) {
	var id DeviceIdentity
	err := s.db.QueryRow(ctx, `
		WITH used AS (
			UPDATE device_tokens t
			SET last_used_at = now()
			WHERE t.token_hash = $1
			  AND t.revoked_at IS NULL
			  AND (t.expires_at IS NULL OR t.expires_at > now())
			RETURNING t.device_id
		)
		SELECT d.id::text, d.email, p.role
		FROM used
		JOIN devices d ON d.id = used.device_id AND d.revoked_at IS NULL
		JOIN principals p ON p.email = d.email AND p.disabled_at IS NULL`,
		tokenHash).Scan(&id.DeviceID, &id.Email, &id.Role)
	if err != nil {
		if noRows(err) {
			return DeviceIdentity{}, ErrNotFound
		}
		return DeviceIdentity{}, fmt.Errorf("store: authenticate device: %w", err)
	}
	return id, nil
}

// RevokeDevice retires a machine and every token it holds. The owner may retire
// their own laptop without involving an admin, because the common case is a
// stolen machine and waiting for someone else is the wrong shape for that.
func (s *Store) RevokeDevice(ctx context.Context, v Viewer, deviceID string) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin revoke device: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	n, err := tx.Exec(ctx, `
		UPDATE devices SET revoked_at = now()
		WHERE id = $1::uuid AND revoked_at IS NULL AND (email = $2 OR $3::bool)`,
		deviceID, v.Email, v.IsAdmin())
	if err != nil {
		return fmt.Errorf("store: revoke device: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	if _, err := tx.Exec(ctx, `
		UPDATE device_tokens SET revoked_at = now()
		WHERE device_id = $1::uuid AND revoked_at IS NULL`, deviceID); err != nil {
		return fmt.Errorf("store: revoke device tokens: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit revoke device: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Health and fleet coverage
// ---------------------------------------------------------------------------

// PutHealthReport stores one self-telemetry sample.
//
// Reports are the only thing the fleet ever learns about a laptop, and the
// alerting is on their ABSENCE, so the write has to be as close to unfailable as
// a write can be. It is idempotent on (email, device, emitted_at) because health
// delivery is at-least-once like everything else the client sends, and a
// re-delivered sample must not look like a second machine checking in.
func (s *Store) PutHealthReport(ctx context.Context, email, deviceID string, r health.Report) error {
	if email == "" {
		return fmt.Errorf("store: health report needs an owning principal")
	}
	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("store: marshal health report: %w", err)
	}
	emitted := r.EmittedAt
	if emitted.IsZero() {
		emitted = time.Now().UTC()
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin health report: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO health_reports (email, device_id, emitted_at, worst, report)
		VALUES ($1, nullif($2,'')::uuid, $3, $4, $5::jsonb)
		ON CONFLICT (email, device_id, emitted_at) DO NOTHING`,
		email, deviceID, emitted, string(r.Worst()), string(body)); err != nil {
		return fmt.Errorf("store: store health report: %w", err)
	}
	// The newest report per machine, kept beside the raw row so no reader
	// scans the history for it (migration 0020). Newest by the report's own
	// clock: a queued sample delivered late must not replace what the machine
	// said afterwards, which is the same rule the device version follows.
	if _, err := tx.Exec(ctx, `
		INSERT INTO health_latest (email, device_id, emitted_at, received_at, worst, agent_version, report)
		VALUES ($1, nullif($2,'')::uuid, $3, now(), $4, nullif($5, ''), $6::jsonb)
		ON CONFLICT (email, device_id) DO UPDATE SET
			emitted_at    = EXCLUDED.emitted_at,
			received_at   = EXCLUDED.received_at,
			worst         = EXCLUDED.worst,
			agent_version = EXCLUDED.agent_version,
			report        = EXCLUDED.report
		WHERE EXCLUDED.emitted_at > health_latest.emitted_at`,
		email, deviceID, emitted, string(r.Worst()), r.AgentVersion, string(body)); err != nil {
		return fmt.Errorf("store: store latest health: %w", err)
	}
	// Liveness is stamped from the report rather than from the request, so a
	// machine that has been queuing offline does not read as freshly seen.
	// The version rides along, and only forward in report time: the enroll
	// upsert wrote it once and nothing refreshed it after, so the fleet view
	// showed enrollment-day builds for machines that had been self-upgrading
	// all along — staleness manufactured by the display, not the binary.
	if deviceID != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE devices SET
			       agent_version = CASE WHEN $3 <> '' AND $2 >= coalesce(last_seen_at, '-infinity')
			                            THEN $3 ELSE agent_version END,
			       last_seen_at = GREATEST(last_seen_at, $2)
			WHERE id = $1::uuid`, deviceID, emitted, r.AgentVersion); err != nil {
			return fmt.Errorf("store: stamp device liveness: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit health report: %w", err)
	}
	return nil
}

// FleetMember is one person's coverage.
type FleetMember struct {
	Email        string     `json:"email"`
	Role         Role       `json:"role"`
	Devices      int        `json:"devices"`
	LastReportAt *time.Time `json:"last_report_at,omitempty"`
	Worst        string     `json:"worst,omitempty"`
	// Silent means enrolled and not reporting. This is the condition the fleet
	// view exists for: a machine that has stopped reporting cannot report that
	// it has stopped.
	Silent bool `json:"silent"`
}

// Fleet is enrollment against reporting, for the admin page.
type Fleet struct {
	Window    time.Duration `json:"window"`
	Enrolled  int           `json:"enrolled"`
	Reporting int           `json:"reporting"`
	Silent    int           `json:"silent"`
	Members   []FleetMember `json:"members"`
}

// FleetCoverage reports who is enrolled, who is reporting, and who has gone
// quiet within the window.
//
// It reads health_latest (migration 0020), one row per machine, rather than
// the raw history: the raw rows are swept after seven days, and a person
// whose only machine last reported eight days ago has to still be on this
// report as silent, with what the machine last said, rather than vanish.
func (s *Store) FleetCoverage(ctx context.Context, v Viewer, window time.Duration) (Fleet, error) {
	if !v.IsAdmin() {
		return Fleet{}, ErrNotAdmin
	}
	if window <= 0 {
		window = 24 * time.Hour
	}
	rows, err := s.db.Query(ctx, `
		SELECT p.email,
		       p.role,
		       count(DISTINCT d.id) AS devices,
		       max(h.emitted_at) FILTER (WHERE h.emitted_at > now() - $1::interval) AS last_report_at,
		       (SELECT h2.worst FROM health_latest h2
		        WHERE h2.email = p.email
		        ORDER BY h2.emitted_at DESC
		        LIMIT 1) AS worst
		FROM principals p
		LEFT JOIN devices d ON d.email = p.email AND d.revoked_at IS NULL
		LEFT JOIN health_latest h ON h.email = p.email
		WHERE p.disabled_at IS NULL
		GROUP BY p.email, p.role
		ORDER BY p.email`,
		intervalString(window))
	if err != nil {
		return Fleet{}, fmt.Errorf("store: fleet coverage: %w", err)
	}
	defer rows.Close()

	fleet := Fleet{Window: window}
	for rows.Next() {
		var (
			m     FleetMember
			worst *string
		)
		if err := rows.Scan(&m.Email, &m.Role, &m.Devices, &m.LastReportAt, &worst); err != nil {
			return Fleet{}, fmt.Errorf("store: scan fleet member: %w", err)
		}
		m.Worst = deref(worst)
		if m.Devices > 0 {
			fleet.Enrolled++
			switch {
			case m.LastReportAt != nil:
				fleet.Reporting++
			default:
				m.Silent = true
				fleet.Silent++
			}
		}
		fleet.Members = append(fleet.Members, m)
	}
	return fleet, rows.Err()
}

// ---------------------------------------------------------------------------
// Model rates
// ---------------------------------------------------------------------------

// ModelPrice is the rate card for one model at one moment.
type ModelPrice struct {
	Model                  string    `json:"model"`
	EffectiveFrom          time.Time `json:"effective_from"`
	InputPerMTok           float64   `json:"input_per_mtok"`
	OutputPerMTok          float64   `json:"output_per_mtok"`
	CacheReadMultiplier    float64   `json:"cache_read_multiplier"`
	CacheWrite5mMultiplier float64   `json:"cache_write_5m_multiplier"`
	CacheWrite1hMultiplier float64   `json:"cache_write_1h_multiplier"`
}

// ModelPrices returns the rate in force for each model at the given moment.
//
// Rates are dated rows rather than constants so that a price change does not
// retroactively rewrite what past work cost, and so that a stored cost figure
// can be re-derived and defended months later.
func (s *Store) ModelPrices(ctx context.Context, at time.Time) (map[string]ModelPrice, error) {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT ON (model)
		       model, effective_from, input_per_mtok::float8, output_per_mtok::float8,
		       cache_read_multiplier::float8, cache_write_5m_multiplier::float8,
		       cache_write_1h_multiplier::float8
		FROM model_prices
		WHERE effective_from <= $1
		ORDER BY model, effective_from DESC`, at)
	if err != nil {
		return nil, fmt.Errorf("store: read model prices: %w", err)
	}
	defer rows.Close()

	out := map[string]ModelPrice{}
	for rows.Next() {
		var p ModelPrice
		if err := rows.Scan(&p.Model, &p.EffectiveFrom, &p.InputPerMTok, &p.OutputPerMTok,
			&p.CacheReadMultiplier, &p.CacheWrite5mMultiplier, &p.CacheWrite1hMultiplier); err != nil {
			return nil, fmt.Errorf("store: scan model price: %w", err)
		}
		out[p.Model] = p
	}
	return out, rows.Err()
}

// PutModelPrice records a rate change.
func (s *Store) PutModelPrice(ctx context.Context, v Viewer, p ModelPrice) error {
	if !v.IsAdmin() {
		return ErrNotAdmin
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO model_prices (model, effective_from, input_per_mtok, output_per_mtok,
		                          cache_read_multiplier, cache_write_5m_multiplier,
		                          cache_write_1h_multiplier)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (model, effective_from) DO UPDATE SET
			input_per_mtok            = EXCLUDED.input_per_mtok,
			output_per_mtok           = EXCLUDED.output_per_mtok,
			cache_read_multiplier     = EXCLUDED.cache_read_multiplier,
			cache_write_5m_multiplier = EXCLUDED.cache_write_5m_multiplier,
			cache_write_1h_multiplier = EXCLUDED.cache_write_1h_multiplier`,
		p.Model, p.EffectiveFrom, p.InputPerMTok, p.OutputPerMTok,
		p.CacheReadMultiplier, p.CacheWrite5mMultiplier, p.CacheWrite1hMultiplier)
	if err != nil {
		return fmt.Errorf("store: write model price: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

// cursor is a keyset position, not an offset. An offset into a table that is
// still being written to skips rows, and this table is written to constantly.
type cursor struct {
	StartedAt time.Time `json:"t"`
	SessionID string    `json:"s"`
}

func encodeCursor(c cursor) string {
	b, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (cursor, error) {
	if s == "" {
		return cursor{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, ErrInvalidCursor
	}
	var c cursor
	if err := json.Unmarshal(b, &c); err != nil || c.SessionID == "" || c.StartedAt.IsZero() {
		return cursor{}, ErrInvalidCursor
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

const (
	defaultLimit = 50
	maxLimit     = 500
)

func clampLimit(n int) int {
	switch {
	case n <= 0:
		return defaultLimit
	case n > maxLimit:
		return maxLimit
	default:
		return n
	}
}

// nullTime turns a zero time into a SQL NULL, so that "no filter" and "the epoch"
// stay distinguishable in a query that tests the parameter for NULL.
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func marshalCounters(m map[string]int) (any, error) {
	if len(m) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("store: marshal counters: %w", err)
	}
	return string(b), nil
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// intervalString renders a duration for Postgres. Seconds rather than a Go
// duration string because Postgres does not parse "1h30m0s".
func intervalString(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', 3, 64) + " seconds"
}

// newUUID mints a version 4 UUID without a dependency. The only property we
// need is that identifiers are unguessable and do not collide, which
// crypto/rand gives directly.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A failing system entropy source is not something this package can
		// recover from, and a predictable share id would be a security bug.
		panic("store: system entropy unavailable: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// newToken mints a share-link secret. 256 bits because the link is the entire
// credential for whoever holds it.
func newToken() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("store: system entropy unavailable: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// ---------------------------------------------------------------------------
// pgx adapters
// ---------------------------------------------------------------------------

// poolDB adapts a pgx pool to the narrow interfaces above. The adapter exists
// so the store's own interfaces stay small enough to fake; pgx's Rows and Tx
// carry a lot of surface this package never touches.
type poolDB struct{ pool *pgxpool.Pool }

// connSource is the optional side of a DB that can pin one connection for a
// few statements. The runner's index builds need it: a concurrent index
// build cannot run inside a transaction, runs past the pool's statement
// ceiling, and the SET that lifts the ceiling has to land on the same
// connection as the build. A DB that cannot pin (the test fakes) runs the
// statement through Exec instead.
type connSource interface {
	Acquire(ctx context.Context) (Conn, error)
}

// Conn is one pinned connection; Release returns it to the pool.
type Conn interface {
	Queryer
	Release()
}

type poolConn struct{ conn *pgxpool.Conn }

func (c poolConn) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return c.conn.Query(ctx, sql, args...)
}

func (c poolConn) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return c.conn.QueryRow(ctx, sql, args...)
}

func (c poolConn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := c.conn.Exec(ctx, sql, args...)
	return tag.RowsAffected(), err
}

func (c poolConn) Release() { c.conn.Release() }

func (d poolDB) Acquire(ctx context.Context) (Conn, error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return poolConn{conn: conn}, nil
}

func (d poolDB) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return d.pool.Query(ctx, sql, args...)
}

func (d poolDB) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return d.pool.QueryRow(ctx, sql, args...)
}

func (d poolDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := d.pool.Exec(ctx, sql, args...)
	return tag.RowsAffected(), err
}

func (d poolDB) Begin(ctx context.Context) (Tx, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return pgxTx{tx: tx}, nil
}

type pgxTx struct{ tx pgx.Tx }

func (t pgxTx) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}

func (t pgxTx) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

func (t pgxTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := t.tx.Exec(ctx, sql, args...)
	return tag.RowsAffected(), err
}

func (t pgxTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t pgxTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }
