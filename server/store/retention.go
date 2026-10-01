package store

// Retention is the only code in this system that removes anything, and it
// exists because nothing did.
//
// The measured shape of the problem: one heavy user for seven days is 407 MB
// and 71,687 events, so 58 MB per user per day, and 65% of the database is the
// TOAST behind events.body. Fifty people is roughly 2.9 GB a day and about a
// terabyte a year against an instance with 7.5 GB of RAM. Disk auto-resize is on
// with no ceiling, so there is no failure to wait for — the bill simply grows,
// and what it grows on is colleagues' full transcripts.
//
// Two knobs, because the two costs trade differently. Expiring a body reclaims
// the 65% and leaves the event row, the session rollup, the cost ledger and the
// search index untouched: an old transcript stops being readable in full while
// every number derived from it stays exactly as it was, and every message
// remains findable. Deleting a session removes the work entirely. A single knob
// would force the cheap, mostly-harmless reclamation to happen on the same
// schedule as the irreversible one.
//
// Three properties are what make this safe to run against a live database, and
// each one is a decision somewhere below rather than a claim.
//
// Bounded. Every statement that writes carries a LIMIT, and one pass stops on a
// wall-clock budget with whatever is left over deliberately left over. The
// alternative — one DELETE across a year of events — takes locks for the length
// of a maintenance window and holds a transaction open long enough to stall
// autovacuum across the whole database. A sweep that stalls ingest is worse than
// unbounded growth: growth costs money, and a stalled ingest path costs the
// captured work of everybody who was mid-task.
//
// Idempotent and resumable. Nothing here remembers where it was. Each batch
// re-derives its own work from the cutoff, so an interrupted pass, a cancelled
// context, a killed instance and a redeploy all resume by simply running again,
// and a pass with nothing to do is one indexed lookup that returns no rows.
//
// Mutually exclusive. Every batch takes a transaction-scoped advisory lock
// before it writes, so two Cloud Run instances cannot be inside a batch at the
// same time, and a second instance that finds the lock held stops its pass
// rather than spinning. See retentionLockKey.

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// retentionLockKey serialises sweeps across instances.
//
// Deliberately transaction-scoped rather than session-scoped, and re-taken by
// every batch rather than held for a whole pass. A session-scoped lock has to be
// acquired and released on one connection, which a pool does not promise, and
// holding either kind for a whole pass means one long-lived transaction — the
// exact thing that keeps autovacuum from reclaiming what the sweep just freed.
//
// Two instances can therefore interleave batches, which is harmless because each
// batch selects its own work from the cutoff and commits independently. What
// they cannot do is overlap inside one, which is what stops two sweeps racing on
// the same rows.
//
// Distinct from migrationLockKey: a sweep waiting on a rolling deploy's
// migration, or a migration waiting on a sweep, is a startup that hangs for no
// reason anybody can see.
const retentionLockKey int64 = 7266794526548562

// retentionTombstone replaces a body that has passed its retention window.
//
// It is not empty, and that is the whole point. server/web/transcript.go drops
// an assistant turn that has neither text nor usage, so an emptied body would
// remove that event from the rendered transcript entirely: a page that looks
// complete, in seq order, with rows silently missing from the middle. A reader
// who cannot see something must be told that, not shown a shorter transcript.
//
// Short on purpose. It is stored once per expired event and this table reaches
// hundreds of millions of rows, so every byte here is paid a hundred million
// times over; forty bytes against the several kilobytes it replaces is under one
// percent of what expiry reclaims.
//
// The key is "text" because that is the field every renderer already reads, so
// no consumer package has to learn about retention to display it honestly.
const retentionTombstone = `{"text":"[body removed by retention]"}`

// MinRetention is the shortest window this store will accept.
//
// A floor exists because the failure it prevents is unrecoverable and the
// mistake that causes it is a typo. RETENTION_SESSION_DAYS=1 where 100 was meant
// deletes very nearly everything on the next tick, and there is no undo: the
// events are gone, and the laptops that produced them long ago dropped their
// spools. A week is short enough to be a legitimate policy for somebody who
// genuinely wants one and long enough that a fat-fingered value is refused at
// boot instead of applied at 03:00.
//
// Zero is not a value below the floor. Zero means "keep forever" and is checked
// before this is.
const MinRetention = 7 * 24 * time.Hour

const (
	// retentionBatchSize bounds the rows one statement touches.
	//
	// Two thousand event bodies is a few tens of megabytes of TOAST released
	// per statement and a transaction that commits in well under a second, which
	// is the number that matters: the row locks a batch takes are held until it
	// commits, and the ingest path must never queue behind this.
	retentionBatchSize = 2000

	// retentionBudget bounds one pass.
	//
	// A pass is not required to finish. The first pass after this ships has
	// months of accumulated data to work through and would otherwise run for
	// hours, holding a pooled connection the request path is sized to use. It
	// stops at the budget and the next pass continues, because nothing here
	// remembers where it was and it therefore does not need to.
	retentionBudget = 5 * time.Minute

	// retentionPreviewCap bounds the counting the preview does.
	//
	// An exact count of what is due means reading every candidate row, which is
	// the scan the sweep itself is written to avoid. The preview exists to be
	// logged at boot and read by a human deciding whether to let this run, and
	// "at least 100,000" answers that question exactly as well as a precise
	// number would.
	retentionPreviewCap = 100000
)

// RetentionPolicy is how long this deployment keeps what it captured.
//
// The two durations are the deployment's decision and arrive from the
// environment; the two bounds below them are this package's mechanics and are
// defaulted. Zero disables a half — see Validate, which is what stops zero from
// also being what a misconfigured deployment silently gets.
type RetentionPolicy struct {
	// BodyAfter is how long the raw events.body is kept. Past it the body
	// becomes the tombstone and the event row, the rollup it feeds, the cost
	// ledger and the search index are all untouched. This is the 65%.
	BodyAfter time.Duration

	// SessionAfter is how long a session is kept at all. Past it the session,
	// its events, its extracted messages, its shares and its usage ledger rows
	// are deleted. Its access_log rows are not; see deleteSessionBatch.
	SessionAfter time.Duration

	// BatchSize bounds one statement and Budget bounds one pass. Zero takes the
	// package defaults, which is what every caller outside a test should do.
	BatchSize int
	Budget    time.Duration
}

// ExpiresBodies reports whether the body half of the policy is switched on.
// Zero is "keep forever", which is a policy a deployment may legitimately hold
// and must state deliberately rather than fall into.
func (p RetentionPolicy) ExpiresBodies() bool { return p.BodyAfter > 0 }

// DeletesSessions reports whether the session half is switched on; zero is
// "keep forever", as for ExpiresBodies.
func (p RetentionPolicy) DeletesSessions() bool { return p.SessionAfter > 0 }

// Enabled reports whether this policy removes anything at all.
func (p RetentionPolicy) Enabled() bool { return p.ExpiresBodies() || p.DeletesSessions() }

// Validate refuses a policy that would surprise whoever configured it.
//
// It lives here rather than in the configuration loader because the rules are
// about what the sweep does, not about how the values were spelled in an
// environment variable, and a second copy of them beside a second caller is a
// second thing to keep correct.
func (p RetentionPolicy) Validate() error {
	if p.BodyAfter < 0 || p.SessionAfter < 0 {
		return fmt.Errorf("store: a retention window cannot be negative (bodies %s, sessions %s)",
			p.BodyAfter, p.SessionAfter)
	}
	if p.ExpiresBodies() && p.BodyAfter < MinRetention {
		return fmt.Errorf("store: keeping bodies for %s is below the %s floor; use 0 to keep them forever",
			p.BodyAfter, MinRetention)
	}
	if p.DeletesSessions() && p.SessionAfter < MinRetention {
		return fmt.Errorf("store: keeping sessions for %s is below the %s floor; use 0 to keep them forever",
			p.SessionAfter, MinRetention)
	}
	// A body window longer than the session window is not dangerous, it is
	// inert: the session and every body in it are deleted before any of those
	// bodies reaches its own cutoff. Refused rather than ignored, because a
	// knob that is read, respected and can never take effect is how somebody
	// ends up certain that bodies are being kept for a year.
	if p.ExpiresBodies() && p.DeletesSessions() && p.BodyAfter > p.SessionAfter {
		return fmt.Errorf("store: keeping bodies for %s outlives the %s session window, so it can never take effect",
			p.BodyAfter, p.SessionAfter)
	}
	return nil
}

// withDefaults fills in the mechanics a caller has no reason to choose.
func (p RetentionPolicy) withDefaults() RetentionPolicy {
	if p.BatchSize <= 0 {
		p.BatchSize = retentionBatchSize
	}
	if p.Budget <= 0 {
		p.Budget = retentionBudget
	}
	return p
}

// LogValue is what a policy looks like in the line logged at boot.
//
// Days rather than Go durations because the configuration is in days and an
// operator comparing the log against the deployment should not have to convert
// "8760h0m0s" in their head. Zero is spelled out as "forever" for the same
// reason: a printed 0 reads like a value that failed to load.
func (p RetentionPolicy) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("bodies", retentionWindowString(p.BodyAfter)),
		slog.String("sessions", retentionWindowString(p.SessionAfter)),
	)
}

func retentionWindowString(d time.Duration) string {
	if d <= 0 {
		return "forever"
	}
	return fmt.Sprintf("%d days", int(d.Hours()/24))
}

// RetentionSweep is what one pass removed. Every field is reported even when it
// is zero: a sweep that removed nothing is the normal case and the line that
// says so is the evidence the sweeper is still running at all.
type RetentionSweep struct {
	// BodiesExpired counts event rows whose body became the tombstone.
	BodiesExpired int64
	// EventsDeleted, SessionsDeleted and LedgerRowsDeleted count rows removed
	// by the session half. Messages and shares are not counted because they go
	// by cascade and the server never sees the number.
	EventsDeleted     int64
	SessionsDeleted   int64
	LedgerRowsDeleted int64

	// Batches counts committed transactions, which is what makes the cost of a
	// pass legible next to what it achieved.
	Batches int
	Elapsed time.Duration

	// Deferred means another instance held the lock and this pass stood down.
	Deferred bool
	// Incomplete means work remained when the pass stopped, either because the
	// budget ran out or because the process is shutting down. It is not an
	// error; it is the sweep behaving as designed, and the next pass continues.
	Incomplete bool
}

// LogValue keeps the sweep line to one group rather than ten top-level fields.
func (r RetentionSweep) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("bodies_expired", r.BodiesExpired),
		slog.Int64("events_deleted", r.EventsDeleted),
		slog.Int64("sessions_deleted", r.SessionsDeleted),
		slog.Int64("ledger_rows_deleted", r.LedgerRowsDeleted),
		slog.Int("batches", r.Batches),
		slog.String("elapsed", r.Elapsed.Round(time.Millisecond).String()),
		slog.Bool("deferred", r.Deferred),
		slog.Bool("incomplete", r.Incomplete),
	)
}

// RetentionDue is what a policy would remove if it ran now, counted without
// removing anything. It is read-only by construction: the two statements it
// issues are SELECTs.
type RetentionDue struct {
	// Bodies is the number of events still carrying a body that is past the
	// body window; Sessions is the number of sessions past the session window.
	Bodies   int64
	Sessions int64

	// Capped reports that counting stopped at retentionPreviewCap, so both
	// figures are floors rather than totals.
	Capped bool

	// The cutoffs the figures were counted against, carried so the log line
	// states the dates rather than leaving a reader to subtract days from a
	// timestamp they cannot see.
	BodyCutoff    time.Time
	SessionCutoff time.Time
}

// LogValue reports the backlog as one group, with the cutoffs as dates.
func (d RetentionDue) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("bodies_due", d.Bodies),
		slog.Int64("sessions_due", d.Sessions),
		slog.Bool("counted_up_to_the_cap", d.Capped),
		slog.String("bodies_older_than", retentionCutoffString(d.BodyCutoff)),
		slog.String("sessions_older_than", retentionCutoffString(d.SessionCutoff)),
	)
}

func retentionCutoffString(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return t.Format(time.RFC3339)
}

// RetentionDueNow counts what the policy would remove, and removes nothing.
//
// It exists to be logged at boot, where it is the one thing that makes a
// retention policy visible before it acts: an operator reading the startup line
// learns both what this deployment has decided to keep and what that decision
// is about to cost the next time the sweeper ticks. A policy nobody can see
// until after it has deleted something is a policy nobody consented to.
//
// now is a parameter rather than time.Now so a test can state the clock it
// means, which is the only way to assert what a cutoff was computed from.
func (s *Store) RetentionDueNow(ctx context.Context, p RetentionPolicy, now time.Time) (RetentionDue, error) {
	var due RetentionDue
	if p.ExpiresBodies() {
		due.BodyCutoff = now.Add(-p.BodyAfter)
		// Counted through a capped subselect rather than a bare count(*): an
		// exact count reads every candidate row, and the candidate set is the
		// backlog, which on the first run after this ships is the whole table.
		if err := s.db.QueryRow(ctx, `
			SELECT count(*) FROM (
				SELECT 1 FROM events
				WHERE body_expired_at IS NULL AND occurred_at < $1
				LIMIT $2
			) AS capped`, due.BodyCutoff, retentionPreviewCap).Scan(&due.Bodies); err != nil {
			return RetentionDue{}, fmt.Errorf("store: count bodies due for expiry: %w", err)
		}
	}
	if p.DeletesSessions() {
		due.SessionCutoff = now.Add(-p.SessionAfter)
		if err := s.db.QueryRow(ctx, `
			SELECT count(*) FROM (
				SELECT 1 FROM sessions
				WHERE started_at < $1
				LIMIT $2
			) AS capped`, due.SessionCutoff, retentionPreviewCap).Scan(&due.Sessions); err != nil {
			return RetentionDue{}, fmt.Errorf("store: count sessions due for deletion: %w", err)
		}
	}
	due.Capped = due.Bodies >= retentionPreviewCap || due.Sessions >= retentionPreviewCap
	return due, nil
}

// SweepRetention removes what the policy says is past its window.
//
// One pass, bounded by the policy's budget, made of independently committed
// batches. It returns what it removed rather than logging it, because the caller
// owns the logger and the store owns the counting; see the sweeper in
// server/app/app.go, which is what calls this.
//
// A cancelled context ends the pass and is not an error: the process is shutting
// down, the batch in flight rolls back untouched, and the work is picked up by
// whichever instance sweeps next. Anything else is returned wrapped, with
// whatever the pass had already committed reported alongside it — a sweep that
// removed forty thousand rows and then failed must not report zero.
func (s *Store) SweepRetention(ctx context.Context, p RetentionPolicy, now time.Time) (RetentionSweep, error) {
	p = p.withDefaults()
	started := time.Now()
	var sweep RetentionSweep
	if !p.Enabled() {
		return sweep, nil
	}

	// Bodies first, deliberately. It is the reclamation that is nearly free to
	// get wrong — the row, its rollup, its cost and its search entry all
	// survive — and it is where 65% of the space is. If a pass only has budget
	// for one half, this is the half to spend it on.
	if p.ExpiresBodies() {
		cutoff := now.Add(-p.BodyAfter)
		for {
			if stop, incomplete := retentionStop(ctx, started, p.Budget); stop {
				sweep.Incomplete = sweep.Incomplete || incomplete
				break
			}
			n, locked, err := s.expireBodyBatch(ctx, cutoff, p.BatchSize, now)
			if err != nil {
				return sweep, err
			}
			if !locked {
				sweep.Deferred = true
				break
			}
			sweep.Batches++
			sweep.BodiesExpired += n
			// A short batch means the candidate set is exhausted — or that
			// SKIP LOCKED stepped over rows somebody else holds, which is the
			// same thing as far as this pass is concerned. Either way there is
			// nothing more to do now and the next pass will see it.
			if n < int64(p.BatchSize) {
				break
			}
		}
	}

	// Not attempted when the body half stood down: the lock it could not take
	// is the same lock this half needs, and a second failed attempt one
	// statement later tells nobody anything they do not already know.
	if p.DeletesSessions() && !sweep.Deferred {
		cutoff := now.Add(-p.SessionAfter)
		for {
			if stop, incomplete := retentionStop(ctx, started, p.Budget); stop {
				sweep.Incomplete = sweep.Incomplete || incomplete
				break
			}
			b, err := s.deleteSessionBatch(ctx, cutoff, p.BatchSize)
			if err != nil {
				return sweep, err
			}
			if !b.locked {
				sweep.Deferred = true
				break
			}
			sweep.Batches++
			sweep.EventsDeleted += b.events
			sweep.SessionsDeleted += b.sessions
			sweep.LedgerRowsDeleted += b.ledger
			if !b.progressed {
				break
			}
		}
	}

	sweep.Elapsed = time.Since(started)
	return sweep, nil
}

// retentionStop reports whether the pass should end, and whether ending leaves
// work behind. Shutdown and an exhausted budget are both "stop with work left":
// neither is a failure, and both are recovered from by running again.
func retentionStop(ctx context.Context, started time.Time, budget time.Duration) (stop, incomplete bool) {
	if ctx.Err() != nil {
		return true, true
	}
	if time.Since(started) >= budget {
		return true, true
	}
	return false, false
}

// expireBodyBatch replaces up to limit bodies with the tombstone.
//
// The WHERE clause is written to match the partial index in migration 0005
// exactly — body_expired_at IS NULL, spelled as a literal — because the planner
// can only use that index if it can prove the query's predicate implies the
// index's. Written any other way this is a sequential scan of every event ever
// captured, on every pass.
//
// FOR UPDATE SKIP LOCKED is what keeps the sweep off the ingest path. A row
// another transaction holds is stepped over rather than waited for, so the
// worst case for an upload arriving mid-batch is that its events are expired on
// some later pass instead of this one — as against a batch that blocks, and an
// upload that times out behind it.
func (s *Store) expireBodyBatch(ctx context.Context, cutoff time.Time, limit int, at time.Time) (expired int64, locked bool, err error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("store: begin body expiry batch: %w", err)
	}
	// Rollback on every path that is not an explicit commit, including the one
	// where the lock was not granted. A batch that returns without ending its
	// transaction leaks a pooled connection holding locks.
	defer func() { _ = tx.Rollback(ctx) }()

	locked, err = retentionLocked(ctx, tx)
	if err != nil || !locked {
		return 0, locked, err
	}

	// Joined on ctid rather than on id, which is the difference between a batch
	// whose cost is fixed and one whose cost is the size of the table.
	//
	// Measured, because it is not visible in the SQL. Joining the CTE back to
	// events on the primary key plans as a hash join whose outer side is a
	// SEQUENTIAL SCAN of every event ever captured — 200,007 rows read to update
	// 2,000, at 29.8ms, and the planner still chooses it when the table is told
	// it holds 180 million. Every batch pays it, so a pass of 500 batches
	// re-reads the whole table 500 times. At today's 72k rows that is invisible;
	// at a year of fifty laptops it is roughly 24 GB of buffers per batch against
	// an instance with 7.5 GB of RAM, which does not merely run slowly, it evicts
	// the cache the ingest path is living in. The ctid form plans as a Tid Scan
	// per row, 11.7ms for the same batch, and does not change as the table grows.
	//
	// The tuple ids are only safe to carry because FOR UPDATE above holds those
	// exact rows for the length of this transaction: nothing can move a row out
	// from under a ctid we hold. Removing the lock would make this a race.
	n, err := tx.Exec(ctx, `
		WITH doomed AS (
			SELECT ctid FROM events
			WHERE body_expired_at IS NULL AND occurred_at < $1
			ORDER BY occurred_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE events e
		SET body = $3::jsonb, body_expired_at = $4
		FROM doomed d
		WHERE e.ctid = d.ctid`, cutoff, limit, retentionTombstone, at)
	if err != nil {
		return 0, true, fmt.Errorf("store: expire event bodies before %s: %w", cutoff.Format(time.RFC3339), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, true, fmt.Errorf("store: commit body expiry batch: %w", err)
	}
	return n, true, nil
}

// retentionBatch is one committed unit of session deletion.
type retentionBatch struct {
	events   int64
	sessions int64
	ledger   int64
	// progressed reports that this batch removed something, which is the
	// loop's continuation condition. A batch that found nothing to do ends the
	// pass; a batch that emptied part of a session leaves the rest for the next
	// one.
	progressed bool
	locked     bool
}

// deleteSessionBatch removes one session, or part of one.
//
// One session at a time, in bounded slices of its events, because sessions are
// not the same size: most are a few hundred events and some are hundreds of
// thousands. A batch defined as "n sessions" would be a batch that occasionally
// deletes millions of rows in one transaction, which is the unbounded DELETE
// this design exists to avoid, wearing a LIMIT.
//
// The order of the deletions is the order the foreign keys require. Events go
// first and take the extracted messages with them by cascade; the usage ledger
// has no foreign key at all and has to be named; the session row goes last and
// takes its shares. A session whose events are only partly gone is a legitimate
// intermediate state that the next batch continues from, which is what makes an
// interrupted sweep resumable rather than corrupt.
//
// access_log rows are deliberately kept. They record who read whose transcript
// and carry none of its content, and the premise of capturing colleagues' work
// is that the reading of it is auditable. An audit trail that is deleted on a
// schedule by the thing it audits is not one.
func (s *Store) deleteSessionBatch(ctx context.Context, cutoff time.Time, limit int) (retentionBatch, error) {
	var b retentionBatch
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return b, fmt.Errorf("store: begin session deletion batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	b.locked, err = retentionLocked(ctx, tx)
	if err != nil || !b.locked {
		return b, err
	}

	// FOR UPDATE holds the rollup row for the length of the batch, so a laptop
	// delivering a late event for this session waits rather than recreating it
	// underneath the deletion. If it wins the race after this commits it
	// recreates a session whose started_at is still older than the cutoff, and
	// the next pass removes it again; the sweep converges either way.
	var sessionID string
	err = tx.QueryRow(ctx, `
		SELECT session_id FROM sessions
		WHERE started_at < $1
		ORDER BY started_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`, cutoff).Scan(&sessionID)
	if err != nil {
		if noRows(err) {
			// Nothing is past the window, or what is left is locked by another
			// sweep. Both mean this pass is done.
			return b, nil
		}
		return b, fmt.Errorf("store: find a session past its retention window: %w", err)
	}

	// By ctid for the same reason as the body batch: `WHERE id IN (SELECT id
	// ...)` plans as a hash semi-join over a sequential scan of every event in
	// the database, measured at 203,834 rows read to delete 2,000 and costed at
	// 6301 against the ctid form's 979. The subselect is bounded and ordered by
	// the (session_id, seq) index either way; it is only the write half that
	// changes.
	//
	// No FOR UPDATE here, deliberately, and specifically not SKIP LOCKED. The
	// test below treats a short batch as "this session is empty now" and goes on
	// to delete the session row; a batch that came back short because SKIP LOCKED
	// stepped over somebody's rows would delete the session out from under events
	// that still exist, and an event with no session is unreachable by every
	// phase here — nothing would ever look for it again. Without SKIP LOCKED a
	// short batch can only mean the rows are gone. Nothing else in this system
	// updates an event, so there is no lock to lose the race to.
	events, err := tx.Exec(ctx, `
		DELETE FROM events
		WHERE ctid IN (
			SELECT ctid FROM events WHERE session_id = $1 ORDER BY seq LIMIT $2
		)`, sessionID, limit)
	if err != nil {
		return b, fmt.Errorf("store: delete events of session %s: %w", sessionID, err)
	}
	b.events = events
	b.progressed = true

	// A full batch means there is more of this session to delete, so the
	// session row stays and the next batch picks the same session up again. The
	// rollup is what the sweep's cutoff query selects on, so removing it before
	// its events would strand them: they have no foreign key to follow and
	// nothing would ever look for them again.
	if events >= int64(limit) {
		if err := tx.Commit(ctx); err != nil {
			return b, fmt.Errorf("store: commit session deletion batch: %w", err)
		}
		return b, nil
	}

	ledger, err := tx.Exec(ctx, `DELETE FROM usage_ledger WHERE session_id = $1`, sessionID)
	if err != nil {
		return b, fmt.Errorf("store: delete usage ledger of session %s: %w", sessionID, err)
	}
	b.ledger = ledger

	// Skill rows outlive the session, since the 90-day pruning report needs
	// the counts whatever the session window is; the person leaves with
	// the session (design 3.4). The first term freezes the session type
	// while the row still exists, because ingest writes '' and a blank
	// session_ref ends the join that would otherwise fill it, which would
	// return an automation session's rows to the default views. The token
	// and device references go too, or a per-person laptop token's rows
	// would join back to the person through its bound email. Hook and lsd_
	// API rows have no session row to follow, so the second statement
	// applies the same treatment to the ones past the window, once each.
	if _, err := tx.Exec(ctx, `
		UPDATE skill_invocations SET
			session_type = COALESCE(NULLIF(session_type, ''), (SELECT s.session_type FROM sessions s WHERE s.session_id = $1), ''),
			actor_email = NULL, device_id = NULL, source_token_id = NULL, preempted_by = NULL, preempted_device = NULL,
			session_ref = '', event_id = NULL, prompt_id = NULL, tool_use_id = NULL, dedupe_key = 'anon:' || id
		WHERE session_ref = $1 AND origin = 'derived'`, sessionID); err != nil {
		return b, fmt.Errorf("store: anonymise skill rows of session %s: %w", sessionID, err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE skill_invocations SET
			actor_email = NULL, device_id = NULL, source_token_id = NULL, preempted_by = NULL, preempted_device = NULL,
			session_ref = '', event_id = NULL, prompt_id = NULL, tool_use_id = NULL, dedupe_key = 'anon:' || id
		WHERE origin <> 'derived' AND occurred_at < $1 AND dedupe_key NOT LIKE 'anon:%'`, cutoff); err != nil {
		return b, fmt.Errorf("store: anonymise aged skill rows: %w", err)
	}

	sessions, err := tx.Exec(ctx, `DELETE FROM sessions WHERE session_id = $1`, sessionID)
	if err != nil {
		return b, fmt.Errorf("store: delete session %s: %w", sessionID, err)
	}
	b.sessions = sessions

	if err := tx.Commit(ctx); err != nil {
		return b, fmt.Errorf("store: commit session deletion batch: %w", err)
	}
	return b, nil
}

// retentionLocked takes the sweep's advisory lock inside the batch transaction.
//
// try rather than wait: a second instance that finds the lock held has nothing
// useful to do with the time it would spend blocked, and a sweeper that queues
// behind another sweeper is a pooled connection held for the length of somebody
// else's pass.
func retentionLocked(ctx context.Context, q Queryer) (bool, error) {
	var got bool
	if err := q.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, retentionLockKey).Scan(&got); err != nil {
		return false, fmt.Errorf("store: take the retention lock: %w", err)
	}
	return got, nil
}
