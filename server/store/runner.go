package store

// The derive runner: the one place derived rows are rebuilt from stored
// events, resumable, versioned, and bounded.
//
// Two passes share the per-session code in turns.go. The dirty-set pass runs
// every thirty seconds over the sessions ingest has touched
// (sessions.derive_dirty), which is how a live session's turns stay current.
// The versioned pass runs the steps of DerivedSchema over the whole corpus,
// once, with a cursor per step in derive_jobs that commits with the step's
// own work, so a deploy mid-way resumes where it stopped and two instances
// never do one batch twice. The previous rebuild read every event with text
// into memory at boot, on every instance of a rolling deploy at once; this
// one reads a batch, writes it, commits, and forgets it.
//
// Three bounds, each a decision here. Batches hold a transaction-scoped
// advisory lock and a 60 s statement timeout. Body-reading steps run only
// inside the derive window and under a rows-per-second cap, because
// detoasting five million bodies is minutes of sustained reads on the
// instance that also serves ingest. A step that fails five batches stops
// the pass and stays visible in derive_jobs with its error, so a fault is a
// logged, alertable fact rather than a loop.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/normalize"
	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

// deriveLockKey serialises versioned batches across instances. It is the
// key the old rebuild used, kept so no other lock in this package can be
// mistaken for it; distinct from the migration and retention keys.
const deriveLockKey int64 = 7266794526548563

const (
	// deriveRetryCap is how many failed batches a step may accumulate before
	// the pass stops on it. Five is enough for a transient (a lock timeout
	// against a long ingest transaction, a cancelled statement) and few
	// enough that a step that cannot succeed is noticed within minutes.
	deriveRetryCap = 5
	// deriveStatementTimeout bounds every batch statement, in place of the
	// pool's request-sized default.
	deriveStatementTimeout = 60 * time.Second
	// deriveSessionsPerBatch is how many sessions a per-session step folds
	// under one lock and one transaction.
	deriveSessionsPerBatch = 50
	// deriveEventRowsPerBatch is the hard cap on rows one event_keys
	// sub-batch reads: measured at about 4 s per 5,000 bodies on the clone,
	// well inside the statement timeout.
	deriveEventRowsPerBatch = 5000
	// deriveEventRowsFloor is how far a sub-batch shrinks when one cannot
	// meet the ceiling: a window of the heaviest production bodies (365 KB
	// rows) is about a second at 250 rows, so a batch that still times out
	// there is a fault to park on, not a size to halve again.
	deriveEventRowsFloor = 250
	// deriveDirtyPerPass bounds one dirty pass.
	deriveDirtyPerPass = 200
	// deriveSkipCapPerBatch is how many sessions one batch may pass over
	// after they failed alone before the failure is read as the step's
	// rather than the sessions': a poison session is one or two, a table
	// that rejects every row is all of them, and only the first is skipped.
	deriveSkipCapPerBatch = 5
	// deriveLockedBeat is the pause before a batch whose first session
	// another fold holds is tried again; the dirty pass holds a session for
	// one fold, so the wait is short. deriveLockedBeatCap bounds the beats
	// in a row (a minute) before the pass stands down as it does for any
	// held lock: the beats are not batches and are not counted as such.
	deriveLockedBeat    = 250 * time.Millisecond
	deriveLockedBeatCap = 240
	// deriveErrorBytes bounds derive_jobs.last_error.
	deriveErrorBytes = 2000
	// dirtyParkBase and dirtyParkCap bound how long a session whose fold
	// failed is left out of the dirty pass: a minute, doubling per failure,
	// an hour at most. Without it the failing session heads the queue on
	// every pass and every session with a newer updated_at is starved.
	dirtyParkBase = time.Minute
	dirtyParkCap  = time.Hour
	// dirtyParkedMax bounds the in-memory parking so a systemic failure
	// cannot grow it without limit; expired entries go first, then the one
	// whose backoff ends soonest.
	dirtyParkedMax = 10_000
)

// The event_keys batch bounds, exported for the configuration layer that
// clamps the environment's DERIVE_ROWS_PER_BATCH to them.
const (
	DeriveEventRowsPerBatch = deriveEventRowsPerBatch
	DeriveEventRowsFloor    = deriveEventRowsFloor
)

// DeriveConfig is what a deployment decides about the runner.
type DeriveConfig struct {
	// Window is when body-reading steps may run. The zero value means the
	// default window; see derive.ParseWindow.
	Window derive.Window
	// RowsPerSec caps body-reading steps. Zero takes derive.DefaultRowsPerSec.
	RowsPerSec int

	// EventRowsPerBatch is the rows one event_keys sub-batch reads, at most
	// deriveEventRowsPerBatch; the environment's DERIVE_ROWS_PER_BATCH.
	EventRowsPerBatch int

	// The knobs below are for tests and default to the constants above.
	SessionsPerBatch int
	DirtyPerPass     int
	StatementTimeout time.Duration
	Now              func() time.Time
	Sleep            func(context.Context, time.Duration) error
}

func (c DeriveConfig) withDefaults() DeriveConfig {
	if c.Window.Loc == nil && !c.Window.Always && c.Window.Start == 0 && c.Window.End == 0 {
		c.Window, _ = derive.ParseWindow("")
	}
	if c.RowsPerSec <= 0 {
		c.RowsPerSec = derive.DefaultRowsPerSec
	}
	if c.SessionsPerBatch <= 0 {
		c.SessionsPerBatch = deriveSessionsPerBatch
	}
	if c.EventRowsPerBatch <= 0 || c.EventRowsPerBatch > deriveEventRowsPerBatch {
		c.EventRowsPerBatch = deriveEventRowsPerBatch
	}
	if c.DirtyPerPass <= 0 {
		c.DirtyPerPass = deriveDirtyPerPass
	}
	if c.StatementTimeout <= 0 {
		c.StatementTimeout = deriveStatementTimeout
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Sleep == nil {
		c.Sleep = derive.Sleep
	}
	return c
}

// LogValue is what the configuration looks like in the boot line.
func (c DeriveConfig) LogValue() slog.Value {
	c = c.withDefaults()
	return slog.GroupValue(
		slog.String("window", c.Window.String()),
		slog.Int("rows_per_sec", c.RowsPerSec),
		slog.Int("rows_per_batch", c.EventRowsPerBatch),
	)
}

// DerivePass is what one versioned pass came to.
type DerivePass struct {
	Version int
	// Done means every step of the version has finished and the version is
	// stamped; later passes have nothing to do until the code's version moves.
	Done bool
	// Deferred means another instance held the lock; this pass stood down.
	Deferred bool
	// Waiting means the next step reads bodies and the window is closed;
	// Until is how long until it opens.
	Waiting bool
	Until   time.Duration
	// Failed names the step that reached the retry cap, with its last error.
	Failed    string
	LastError string
	// StalledMinutes is how long the first unfinished step has gone without
	// committing a batch, reported on a pass that advanced nothing (deferred,
	// waiting, failed) and zero otherwise. It is the figure the
	// derive_stalled_minutes metric extracts from the "derive pass" line, so
	// a runner that is alive but never progressing is alertable without
	// anybody reading derive_jobs.
	StalledMinutes float64
	// Skipped counts the sessions the per-session steps passed over this
	// pass because they failed alone; each is named by a "derive session
	// failed" line and left dirty for the dirty pass.
	Skipped int
	// Steps are the steps this pass touched, in order.
	Steps   []DeriveStepResult
	Elapsed time.Duration
}

// DeriveStepResult is one step's progress in one pass.
type DeriveStepResult struct {
	Step     string
	Rows     int64
	Batches  int
	Skipped  int
	Seconds  float64
	Finished bool
}

// LogValue keeps the pass line to one group. The runner's version is
// derive_version: the log contract stamps the build on every line as
// "version", and a second key of that name would be the one a JSON reader
// keeps.
func (p DerivePass) LogValue() slog.Value {
	steps := make([]string, 0, len(p.Steps))
	for _, s := range p.Steps {
		steps = append(steps, s.Step)
	}
	return slog.GroupValue(
		slog.Int("derive_version", p.Version),
		slog.Bool("done", p.Done),
		slog.Bool("deferred", p.Deferred),
		slog.Bool("waiting", p.Waiting),
		slog.String("until", p.Until.Round(time.Second).String()),
		slog.String("failed", p.Failed),
		slog.Float64("stalled_minutes", p.StalledMinutes),
		slog.Int("skipped", p.Skipped),
		slog.Any("steps", steps),
		slog.String("elapsed", p.Elapsed.Round(time.Millisecond).String()),
	)
}

// DeriveDirtyResult is what one dirty-set pass did.
type DeriveDirtyResult struct {
	Folded int
	// Skipped are the sessions another fold held or retention removed.
	Skipped int
	// Failed are the sessions whose fold failed this pass; each is named by
	// a "derive session failed" line, stays dirty, and is parked for a
	// backoff so it cannot head the next pass.
	Failed int
	// Parked are the sessions left out of this pass because a recent fold
	// of theirs failed and their backoff has not run out.
	Parked int
	Turns  int
	// Rederived are the sessions the skill re-derive queue gave back this
	// pass; RederiveParked the ones that failed their fifth attempt in it.
	Rederived      int
	RederiveParked int
	Elapsed        time.Duration
}

// LogValue keeps the dirty line to one group.
func (r DeriveDirtyResult) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("folded", r.Folded),
		slog.Int("skipped", r.Skipped),
		slog.Int("failed", r.Failed),
		slog.Int("parked", r.Parked),
		slog.Int("turns", r.Turns),
		slog.Int("rederived", r.Rederived),
		slog.Int("rederive_parked", r.RederiveParked),
		slog.String("elapsed", r.Elapsed.Round(time.Millisecond).String()),
	)
}

// batchResult is what one batch of a step came to.
type batchResult struct {
	rows    int
	skipped int
	// done means the step finished with this batch; deferred means another
	// instance holds the lock and this pass stands down; waited means the
	// batch's first session was held elsewhere and nothing was done.
	done     bool
	deferred bool
	waited   bool
}

// deriveStep is one step of the versioned pass.
type deriveStep struct {
	name string
	// bodyReading steps run only inside the window and under the pacer.
	bodyReading bool
	// since is the derivation version that introduced the step's current
	// rule. ensureDeriveJobs seeds a step finished when the stored version
	// is at or past it, so a bump for one new step runs that step alone
	// rather than every step before it again during ingest hours; a fresh
	// database stamps 0 and runs them all. A zero value would seed every
	// step finished on that fresh database, so every step names its
	// version.
	since int
	// index names the index an index:* step builds; run is nil for those.
	index string
	// run performs one batch and reports the rows it processed, the
	// sessions it passed over and whether the step is finished; deferred
	// means the lock was held elsewhere.
	run func(ctx context.Context, s *Store, cfg DeriveConfig, version int) (batchResult, error)
}

// deriveIndexes are the indexes the runner owns, built CONCURRENTLY in this
// order: the two the fold and ingest identity need first, then the marker
// index the reads filter on, then the messages index the titles read, then
// the ingested_at index the export job's planning read and per-day COPY
// range over (the contract section 3).
var deriveIndexes = []struct{ name, ddl string }{
	{"events_session_prompt_idx",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS events_session_prompt_idx ON events (session_id, prompt_id) WHERE prompt_id IS NOT NULL`},
	{"events_record_identity_idx",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS events_record_identity_idx ON events (session_id, coalesce(agent_id, ''), record_uuid, type) WHERE record_uuid IS NOT NULL`},
	{"events_superseded_idx",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS events_superseded_idx ON events (session_id, seq) WHERE superseded_by IS NOT NULL`},
	{"messages_session_human_idx",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS messages_session_human_idx ON messages (session_id, seq) WHERE role = 'user' AND kind IN ('human', 'slash_command') AND agent_id IS NULL`},
	{"events_ingested_at_idx",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS events_ingested_at_idx ON events (ingested_at)`},
}

// deriveSteps is the versioned pass, in order. Indexes first, because
// identity at ingest and the fold need them; keys before kinds because the
// keys step has the body in hand and classifies with it; kinds before
// titles because titles read kinds; turns before rollups and class because
// both read turns; artifacts and links last because they are the slowest
// and nothing else waits on them.
func deriveSteps() []deriveStep {
	steps := make([]deriveStep, 0, len(deriveIndexes)+8)
	for _, ix := range deriveIndexes {
		steps = append(steps, deriveStep{name: "index:" + ix.name, index: ix.name, bodyReading: true, since: 3})
	}
	steps = append(steps,
		deriveStep{name: "event_keys", bodyReading: true, since: 3, run: func(ctx context.Context, s *Store, cfg DeriveConfig, version int) (batchResult, error) {
			return s.eventKeysBatch(ctx, cfg, version)
		}},
		// messages_kind reads messages.text and the columns beside it, never
		// a body, so it is not gated by the window: on the first pass the
		// steps behind it would otherwise wait a day for nothing
		// (adversarial finding 6).
		deriveStep{name: "messages_kind", since: 3, run: perSession("messages_kind", func(ctx context.Context, s *Store, q Queryer, _ DeriveConfig, version int, sid string) (int, error) {
			return s.classifyMessages(ctx, q, sid)
		})},
		deriveStep{name: "titles", since: 3, run: perSession("titles", func(ctx context.Context, _ *Store, q Queryer, cfg DeriveConfig, version int, sid string) (int, error) {
			changed, err := retitleSession(ctx, q, sid)
			return boolRows(changed), err
		})},
		deriveStep{name: "turns", since: 3, run: perSession("turns", func(ctx context.Context, s *Store, q Queryer, _ DeriveConfig, version int, sid string) (int, error) {
			n, err := s.foldSession(ctx, q, sid, version)
			if err != nil {
				return 0, err
			}
			return n, rollupFromTurns(ctx, q, sid)
		})},
		deriveStep{name: "rollups", since: 3, run: perSession("rollups", func(ctx context.Context, _ *Store, q Queryer, cfg DeriveConfig, version int, sid string) (int, error) {
			return 1, rollupFromTurns(ctx, q, sid)
		})},
		deriveStep{name: "session_class", since: 3, run: perSession("session_class", func(ctx context.Context, s *Store, q Queryer, cfg DeriveConfig, _ int, sid string) (int, error) {
			changed, err := s.classifySession(ctx, q, sid, cfg.Now(), s.healthHourlyExists(ctx, q))
			return boolRows(changed), err
		})},
		deriveStep{name: "artifacts", bodyReading: true, since: 3, run: perSession("artifacts", func(ctx context.Context, _ *Store, q Queryer, cfg DeriveConfig, version int, sid string) (int, error) {
			return rebuildSessionArtifacts(ctx, q, sid)
		})},
		deriveStep{name: "links", bodyReading: true, since: 3, run: perSession("links", func(ctx context.Context, _ *Store, q Queryer, cfg DeriveConfig, version int, sid string) (int, error) {
			return rebuildSessionLinks(ctx, q, sid)
		})},
		// Skill invocations rebuild through the live path's derivation
		// (skills.go). Ingest never takes the session lock, and the merge
		// locks a conflicting row for the whole batch even when it leaves
		// it unchanged, so this step bounds its wait and hands a lock wait
		// back as errSkillLockWait, which perSession turns into the same
		// batch again rather than a skipped session.
		deriveStep{name: "skill_invocations", bodyReading: true, since: 4, run: perSession("skill_invocations", func(ctx context.Context, s *Store, q Queryer, _ DeriveConfig, version int, sid string) (int, error) {
			if _, err := q.Exec(ctx, "SET LOCAL lock_timeout = '5s'"); err != nil {
				return 0, fmt.Errorf("store: bound the skill rebuild lock wait: %w", err)
			}
			return s.rebuildSessionSkillInvocations(ctx, q, sid)
		})},
	)
	return steps
}

func boolRows(b bool) int {
	if b {
		return 1
	}
	return 0
}

// RunDerive runs one versioned pass: every unfinished step of DerivedSchema,
// batch by batch, until they are all done, the window closes on a
// body-reading step, another instance holds the lock, a step reaches its
// retry cap, or the context ends. It never OOMs and never empties a table:
// each batch is bounded and each session's rows are replaced inside one
// transaction.
//
// The results are named so the deferred Elapsed lands in what the caller
// receives; with unnamed results it was written after the return values had
// been copied and every pass line read "0s" (review-2 finding 24).
func (s *Store) RunDerive(ctx context.Context, cfg DeriveConfig) (pass DerivePass, err error) {
	cfg = cfg.withDefaults()
	started := time.Now()
	pass = DerivePass{Version: DerivedSchema}
	defer func() { pass.Elapsed = time.Since(started) }()

	var stored int
	if err := s.db.QueryRow(ctx, `SELECT version FROM derived_schema WHERE only_row`).Scan(&stored); err != nil {
		return pass, fmt.Errorf("store: read derivation version: %w", err)
	}
	if stored >= DerivedSchema {
		pass.Done = true
		return pass, nil
	}
	if err := s.ensureDeriveJobs(ctx, DerivedSchema, stored); err != nil {
		return pass, err
	}

	pacer := derive.Pacer{RowsPerSec: cfg.RowsPerSec}
	for _, step := range deriveSteps() {
		job, err := s.readDeriveJob(ctx, DerivedSchema, step.name)
		if err != nil {
			return pass, err
		}
		if job.finished {
			continue
		}
		if job.attempts >= deriveRetryCap {
			s.logStepFailed(ctx, step.name, job.attempts, job.lastError, job.processed)
			pass.Failed, pass.LastError = step.name, job.lastError
			pass.StalledMinutes = s.stalledMinutes(ctx, DerivedSchema, step.name, cfg.Now())
			return pass, nil
		}
		result := DeriveStepResult{Step: step.name}
		stepStart := time.Now()
		waits := 0
		for {
			if ctx.Err() != nil {
				return pass, ctx.Err()
			}
			if step.bodyReading {
				if wait := cfg.Window.Until(cfg.Now()); wait > 0 {
					pass.Waiting, pass.Until = true, wait
					pass.Steps = append(pass.Steps, result)
					if result.Batches == 0 {
						pass.StalledMinutes = s.stalledMinutes(ctx, DerivedSchema, step.name, cfg.Now())
					}
					return pass, nil
				}
			}
			batchStart := time.Now()
			var br batchResult
			if step.index != "" {
				br, err = s.indexStep(ctx, DerivedSchema, step)
			} else {
				br, err = step.run(ctx, s, cfg, DerivedSchema)
			}
			if err != nil {
				if ctx.Err() != nil {
					return pass, ctx.Err()
				}
				// A keys batch that cannot meet the ceiling is halved, not
				// retried unchanged: five identical attempts on one window
				// of heavy bodies would park the step with no way past but
				// a code change (review-2 finding 22). The halving is not
				// an attempt; a batch that times out at the floor is.
				if step.name == "event_keys" && sqlState(err) == "57014" {
					if next, ok := nextEventBatch(cfg.EventRowsPerBatch); ok {
						s.logBatchShrunk(ctx, step.name, cfg.EventRowsPerBatch, next, err)
						cfg.EventRowsPerBatch = next
						continue
					}
				}
				attempts, recErr := s.recordDeriveFailure(ctx, DerivedSchema, step.name, err)
				if recErr != nil {
					return pass, recErr
				}
				if attempts >= deriveRetryCap {
					s.logStepFailed(ctx, step.name, attempts, err.Error(), result.Rows)
					pass.Failed, pass.LastError = step.name, err.Error()
					pass.Steps = append(pass.Steps, result)
					pass.StalledMinutes = s.stalledMinutes(ctx, DerivedSchema, step.name, cfg.Now())
					return pass, nil
				}
				// A failed batch is retried after a pause that grows with the
				// attempt, so a lock held by a long ingest transaction has
				// time to clear.
				if err := cfg.Sleep(ctx, jitteredBackoff(time.Second, attempts)); err != nil {
					return pass, err
				}
				continue
			}
			if br.waited {
				// The batch's first session is held by another fold. Not a
				// batch, so not counted as one; a minute of them in a row
				// and the pass stands down as it does for any held lock.
				waits++
				if waits < deriveLockedBeatCap {
					continue
				}
				br.deferred = true
			}
			if br.deferred {
				pass.Deferred = true
				pass.Steps = append(pass.Steps, result)
				if result.Batches == 0 {
					pass.StalledMinutes = s.stalledMinutes(ctx, DerivedSchema, step.name, cfg.Now())
				}
				return pass, nil
			}
			waits = 0
			result.Rows += int64(br.rows)
			result.Skipped += br.skipped
			pass.Skipped += br.skipped
			result.Batches++
			if step.bodyReading {
				if d := pacer.Delay(br.rows, time.Since(batchStart)); d > 0 {
					if err := cfg.Sleep(ctx, d); err != nil {
						return pass, err
					}
				}
			}
			if br.done {
				result.Finished = true
				break
			}
		}
		result.Seconds = time.Since(stepStart).Seconds()
		pass.Steps = append(pass.Steps, result)
		s.logStep(ctx, result)
	}

	stamped, err := s.stampDerived(ctx, DerivedSchema)
	if err != nil {
		return pass, err
	}
	pass.Done = stamped
	return pass, nil
}

// logStep is the "derive step" line: one per finished step, with the rows
// and seconds the metrics read. rows_per_sec is the figure the
// derive_rows_per_sec metric extracts. The runner's version is
// derive_version on every derive line: "version" is the build, stamped by
// the server's logger on every line, and a runner attribute of that name
// produced a JSON object with two "version" members of which the reader
// kept the runner's (review-1 finding 18).
func (s *Store) logStep(ctx context.Context, r DeriveStepResult) {
	rate := 0.0
	if r.Seconds > 0 {
		rate = float64(r.Rows) / r.Seconds
	}
	s.logger().InfoContext(ctx, "derive step",
		slog.String("step", r.Step),
		slog.Int("derive_version", DerivedSchema),
		slog.Int64("rows", r.Rows),
		slog.Int("batches", r.Batches),
		slog.Int("skipped", r.Skipped),
		slog.Float64("seconds", r.Seconds),
		slog.Float64("rows_per_sec", rate),
		slog.Bool("finished", r.Finished))
}

// logStepFailed is the "derive step failed" line the derive_step_failed
// metric counts. It carries the runbook's own recovery statement, because
// the operator reading it is the one who has to run it.
func (s *Store) logStepFailed(ctx context.Context, step string, attempts int, lastError string, rows int64) {
	s.logger().ErrorContext(ctx, "derive step failed",
		slog.String("step", step),
		slog.Int("derive_version", DerivedSchema),
		slog.Int("attempts", attempts),
		slog.Int64("rows", rows),
		slog.String("error", lastError),
		slog.String("recover", fmt.Sprintf("UPDATE derive_jobs SET attempts = 0 WHERE version = %d AND step = '%s'", DerivedSchema, step)))
}

// logBatchShrunk is the "derive batch shrunk" line: an event_keys batch met
// the statement ceiling and the next attempt reads fewer rows. The size is
// what the operator reads to know where the corpus is heavy.
func (s *Store) logBatchShrunk(ctx context.Context, step string, from, to int, cause error) {
	s.logger().WarnContext(ctx, "derive batch shrunk",
		slog.String("step", step),
		slog.Int("derive_version", DerivedSchema),
		slog.Int("rows_per_batch", to),
		slog.Int("previous", from),
		slog.String("error", cause.Error()))
}

// nextEventBatch halves an event_keys batch that met the ceiling, down to
// the floor, and reports whether there was room to shrink.
func nextEventBatch(cur int) (int, bool) {
	if cur <= deriveEventRowsFloor {
		return cur, false
	}
	return max(cur/2, deriveEventRowsFloor), true
}

// logSessionFailed is the "derive session failed" line: one session whose
// fold failed on its own, named, so a poison session is an alertable count
// rather than a parked step or a starved queue. pass is "dirty" or
// "versioned"; step is the versioned step or "" for the dirty pass.
func (s *Store) logSessionFailed(ctx context.Context, pass, step, sessionID string, attempts int, cause error) {
	s.logger().ErrorContext(ctx, "derive session failed",
		slog.String("pass", pass),
		slog.String("step", step),
		slog.String("session_id", sessionID),
		slog.Int("derive_version", DerivedSchema),
		slog.Int("attempts", attempts),
		slog.String("error", cause.Error()))
}

// stalledMinutes is how long a step has gone without committing a batch:
// since its last cursor commit, else since its first attempt, else zero for a
// step nothing has touched yet. Read outside the batch lock, so a pass that
// stood down for another instance can still report it.
func (s *Store) stalledMinutes(ctx context.Context, version int, step string, now time.Time) float64 {
	var since *time.Time
	if err := s.db.QueryRow(ctx, `
		SELECT coalesce(started_at, updated_at) FROM derive_jobs
		WHERE version = $1 AND step = $2 AND processed = 0
		UNION ALL
		SELECT updated_at FROM derive_jobs
		WHERE version = $1 AND step = $2 AND processed > 0
		LIMIT 1`, version, step).Scan(&since); err != nil || since == nil {
		return 0
	}
	if d := now.Sub(*since); d > 0 {
		return d.Minutes()
	}
	return 0
}

// ensureDeriveJobs inserts the ledger rows for a version, once. A step
// whose rule the stored version already carries (since at or below it) is
// seeded finished: the work it would do was done under that version, and a
// pass that redid it would run thirteen unrelated steps for hours ahead of
// the one the bump is for.
func (s *Store) ensureDeriveJobs(ctx context.Context, version, stored int) error {
	steps := deriveSteps()
	names := make([]string, len(steps))
	ordinals := make([]int32, len(steps))
	finished := make([]bool, len(steps))
	for i, st := range steps {
		names[i] = st.name
		ordinals[i] = int32(i)
		finished[i] = st.since <= stored
	}
	if _, err := s.db.Exec(ctx, `
		INSERT INTO derive_jobs (version, step, ordinal, started_at, finished_at)
		SELECT $1, u.step, u.ordinal, CASE WHEN u.finished THEN now() END, CASE WHEN u.finished THEN now() END
		FROM unnest($2::text[], $3::int[], $4::bool[]) AS u(step, ordinal, finished)
		ON CONFLICT (version, step) DO NOTHING`, version, names, ordinals, finished); err != nil {
		return fmt.Errorf("store: seed derive jobs: %w", err)
	}
	return nil
}

type deriveJob struct {
	cursor    string
	processed int64
	attempts  int
	finished  bool
	lastError string
}

func (s *Store) readDeriveJob(ctx context.Context, version int, step string) (deriveJob, error) {
	var (
		j        deriveJob
		finished *time.Time
	)
	if err := s.db.QueryRow(ctx, `
		SELECT cursor, processed, attempts, finished_at, last_error
		FROM derive_jobs WHERE version = $1 AND step = $2`, version, step).Scan(
		&j.cursor, &j.processed, &j.attempts, &finished, &j.lastError); err != nil {
		return j, fmt.Errorf("store: read derive job %s: %w", step, err)
	}
	j.finished = finished != nil
	return j, nil
}

// recordDeriveFailure counts a failed batch against its step and keeps the
// error, and reports the attempts so far.
func (s *Store) recordDeriveFailure(ctx context.Context, version int, step string, cause error) (int, error) {
	msg := clipError(cause.Error())
	var attempts int
	if err := s.db.QueryRow(ctx, `
		UPDATE derive_jobs SET attempts = attempts + 1, last_error = $3, updated_at = now()
		WHERE version = $1 AND step = $2
		RETURNING attempts`, version, step, msg).Scan(&attempts); err != nil {
		return 0, fmt.Errorf("store: record derive failure on %s: %w", step, err)
	}
	return attempts, nil
}

// clipError bounds an error text for derive_jobs.last_error, on a rune
// boundary: a cut inside a multibyte character is invalid UTF-8, which the
// TEXT column refuses, and a failure record that cannot be written turns a
// parked step into a retry loop.
func clipError(msg string) string {
	if len(msg) <= deriveErrorBytes {
		return msg
	}
	return strings.ToValidUTF8(msg[:deriveErrorBytes], "")
}

// stampDerived writes derived_schema.version once every step of the version
// has finished, and reports whether it did. Under the lock, so two
// instances finishing together stamp once.
func (s *Store) stampDerived(ctx context.Context, version int) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("store: begin derive stamp: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var got bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, deriveLockKey).Scan(&got); err != nil {
		return false, fmt.Errorf("store: take the derive lock: %w", err)
	}
	if !got {
		return false, nil
	}
	var pending int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM derive_jobs WHERE version = $1 AND finished_at IS NULL`, version).Scan(&pending); err != nil {
		return false, fmt.Errorf("store: count pending derive steps: %w", err)
	}
	if pending > 0 {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE derived_schema
		SET version = $1, rebuilt_at = now(),
		    artifacts = (SELECT count(*) FROM artifacts),
		    links = (SELECT count(*) FROM links)
		WHERE only_row AND version < $1`, version); err != nil {
		return false, fmt.Errorf("store: stamp derivation version: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("store: commit derive stamp: %w", err)
	}
	return true, nil
}

// deriveBatch opens one batch: a transaction with the statement ceiling and
// the advisory lock, and the step's job row locked for the cursor update.
// It reports deferred when another instance holds the lock, and done when
// the step already finished.
func (s *Store) deriveBatch(ctx context.Context, cfg DeriveConfig, version int, step string) (tx Tx, job deriveJob, done, deferred bool, err error) {
	tx, err = s.db.Begin(ctx)
	if err != nil {
		return nil, job, false, false, fmt.Errorf("store: begin derive batch %s: %w", step, err)
	}
	fail := func(err error) (Tx, deriveJob, bool, bool, error) {
		_ = tx.Rollback(ctx)
		return nil, job, false, false, err
	}
	if err := WithStatementTimeout(ctx, tx, cfg.StatementTimeout); err != nil {
		return fail(err)
	}
	var got bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, deriveLockKey).Scan(&got); err != nil {
		return fail(fmt.Errorf("store: take the derive lock: %w", err))
	}
	if !got {
		_ = tx.Rollback(ctx)
		return nil, job, false, true, nil
	}
	var finished *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT cursor, processed, attempts, finished_at, last_error
		FROM derive_jobs WHERE version = $1 AND step = $2 FOR UPDATE`, version, step).Scan(
		&job.cursor, &job.processed, &job.attempts, &finished, &job.lastError); err != nil {
		return fail(fmt.Errorf("store: lock derive job %s: %w", step, err))
	}
	if finished != nil {
		_ = tx.Rollback(ctx)
		return nil, job, true, false, nil
	}
	return tx, job, false, false, nil
}

// commitBatch advances the cursor with the batch's work and commits.
func commitBatch(ctx context.Context, tx Tx, version int, step, cursor string, rows int, done bool) error {
	if _, err := tx.Exec(ctx, `
		UPDATE derive_jobs SET
			cursor      = $3,
			processed   = processed + $4,
			started_at  = coalesce(started_at, now()),
			updated_at  = now(),
			finished_at = CASE WHEN $5::bool THEN now() ELSE finished_at END
		WHERE version = $1 AND step = $2`, version, step, cursor, rows, done); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("store: advance derive job %s: %w", step, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit derive batch %s: %w", step, err)
	}
	return nil
}

// perSessionWork is what a per-session step does for one session.
type perSessionWork func(ctx context.Context, s *Store, q Queryer, cfg DeriveConfig, version int, sid string) (int, error)

// perSession wraps a per-session function as a batch: the next N sessions
// after the cursor, in session-id order, each under its own advisory lock,
// in one transaction.
//
// Two things can stop a batch short. A session another fold holds ends the
// batch before it: the cursor stays there and the session is done on the
// next batch, rather than passed over and left with stale rows for the
// steps the dirty pass never redoes (artifacts, links). A session whose
// work fails rolls the batch back, and the batch is retried one session per
// transaction: the sessions that succeed alone are committed one by one,
// and a session that fails alone is recorded and passed over, so one poison
// session cannot park a step for the whole corpus (review-1 findings 3 and
// 16).
func perSession(step string, work perSessionWork) func(context.Context, *Store, DeriveConfig, int) (batchResult, error) {
	return func(ctx context.Context, s *Store, cfg DeriveConfig, version int) (batchResult, error) {
		tx, job, done, deferred, err := s.deriveBatch(ctx, cfg, version, step)
		if err != nil || done || deferred {
			return batchResult{done: done, deferred: deferred}, err
		}
		ids, err := nextSessions(ctx, tx, step, job.cursor, cfg.SessionsPerBatch)
		if err != nil {
			_ = tx.Rollback(ctx)
			return batchResult{}, err
		}
		finished := len(ids) < cfg.SessionsPerBatch

		total, did := 0, 0
		for _, sid := range ids {
			locked, err := sessionLocked(ctx, tx, sid)
			if err != nil {
				_ = tx.Rollback(ctx)
				return batchResult{}, err
			}
			if !locked {
				finished = false
				break
			}
			n, err := work(ctx, s, tx, cfg, version, sid)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					// Deleted by retention between the select and the work.
					did++
					continue
				}
				_ = tx.Rollback(ctx)
				if errors.Is(err, errSkillLockWait) {
					// A row ingest holds: the same batch again after a
					// pause, cursor unmoved, nothing skipped.
					return batchResult{waited: true}, cfg.Sleep(ctx, deriveLockedBeat)
				}
				return s.perSessionOneByOne(ctx, cfg, version, step, ids, work)
			}
			total += n
			did++
		}
		if did == 0 {
			// The first session of the batch is held elsewhere; nothing to
			// commit. A short pause, then the caller asks again.
			_ = tx.Rollback(ctx)
			if len(ids) == 0 {
				return batchResult{}, commitBatchAlone(ctx, s, cfg, version, step, job.cursor, 0, true)
			}
			return batchResult{waited: true}, cfg.Sleep(ctx, deriveLockedBeat)
		}
		cursor := ids[did-1]
		if err := commitBatch(ctx, tx, version, step, cursor, did, finished); err != nil {
			return batchResult{}, err
		}
		return batchResult{rows: max(total, did), done: finished}, nil
	}
}

// nextSessions reads the next batch of session ids after the cursor.
func nextSessions(ctx context.Context, q Queryer, step, cursor string, limit int) ([]string, error) {
	rows, err := q.Query(ctx, `
		SELECT session_id FROM sessions WHERE session_id > $1 ORDER BY session_id LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("store: next sessions for %s: %w", step, err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan session id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: next sessions for %s: %w", step, err)
	}
	return ids, nil
}

// commitBatchAlone opens a batch of its own to advance the cursor, for a
// batch that found nothing to do but has something to record.
func commitBatchAlone(ctx context.Context, s *Store, cfg DeriveConfig, version int, step, cursor string, rows int, done bool) error {
	tx, _, finished, deferred, err := s.deriveBatch(ctx, cfg, version, step)
	if err != nil || finished || deferred {
		return err
	}
	return commitBatch(ctx, tx, version, step, cursor, rows, done)
}

// perSessionOneByOne retries a failed batch one session per transaction. A
// session that fails alone, twice in a row now, is recorded in the step's
// last_error, left dirty for the dirty pass, and passed over; more than
// deriveSkipCapPerBatch of them in one batch is the step failing, not the
// sessions, and is reported as the batch's error so the attempt counts.
func (s *Store) perSessionOneByOne(ctx context.Context, cfg DeriveConfig, version int, step string, ids []string, work perSessionWork) (batchResult, error) {
	var res batchResult
	for _, sid := range ids {
		tx, job, done, deferred, err := s.deriveBatch(ctx, cfg, version, step)
		if err != nil || done || deferred {
			res.done, res.deferred = done, deferred
			return res, err
		}
		// Another instance may have taken the step past this session
		// between two of these transactions; the comparison is the
		// database's, the same one ORDER BY session_id uses.
		var ahead bool
		if err := tx.QueryRow(ctx, `SELECT $1::text > $2::text`, sid, job.cursor).Scan(&ahead); err != nil {
			_ = tx.Rollback(ctx)
			return res, fmt.Errorf("store: compare cursor for %s: %w", step, err)
		}
		if !ahead {
			_ = tx.Rollback(ctx)
			continue
		}
		locked, err := sessionLocked(ctx, tx, sid)
		if err != nil {
			_ = tx.Rollback(ctx)
			return res, err
		}
		if !locked {
			_ = tx.Rollback(ctx)
			if res.rows == 0 && res.skipped == 0 {
				res.waited = true
				return res, cfg.Sleep(ctx, deriveLockedBeat)
			}
			return res, nil
		}
		n, err := work(ctx, s, tx, cfg, version, sid)
		if err != nil && errors.Is(err, errSkillLockWait) {
			_ = tx.Rollback(ctx)
			if res.rows == 0 && res.skipped == 0 {
				res.waited = true
				return res, cfg.Sleep(ctx, deriveLockedBeat)
			}
			return res, nil
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			_ = tx.Rollback(ctx)
			res.skipped++
			if res.skipped > deriveSkipCapPerBatch {
				return res, fmt.Errorf("store: derive %s for %s (the %dth session of one batch to fail alone): %w", step, sid, res.skipped, err)
			}
			cause := fmt.Errorf("store: derive %s for %s: %w", step, sid, err)
			if err := s.skipSession(ctx, cfg, version, step, sid, cause); err != nil {
				return res, err
			}
			s.logSessionFailed(ctx, "versioned", step, sid, 2, cause)
			continue
		}
		if err := commitBatch(ctx, tx, version, step, sid, 1, false); err != nil {
			return res, err
		}
		res.rows += max(n, 1)
	}
	return res, nil
}

// skipSession records a session a versioned step passed over: the cursor
// moves past it, its error is kept on the step, and the session is left
// dirty so the dirty pass brings it back once the cause is fixed.
func (s *Store) skipSession(ctx context.Context, cfg DeriveConfig, version int, step, sessionID string, cause error) error {
	tx, _, done, deferred, err := s.deriveBatch(ctx, cfg, version, step)
	if err != nil || done || deferred {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET derive_dirty = true WHERE session_id = $1`, sessionID); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("store: leave %s dirty: %w", sessionID, err)
	}
	if step == "skill_invocations" {
		// The dirty pass folds and classifies only, so a skill row this
		// step passed over would never come back without the queue.
		if err := s.EnqueueSkillRederive(ctx, tx, []string{sessionID}, "skipped"); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE derive_jobs SET last_error = $3, updated_at = now()
		WHERE version = $1 AND step = $2`, version, step, clipError("skipped "+cause.Error())); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("store: record skipped session on %s: %w", step, err)
	}
	return commitBatch(ctx, tx, version, step, sessionID, 1, false)
}

// eventKeysBatch is the one body-reading step over events: it fills the key
// columns 0017 added, and the message kind for the rows whose body it
// already has open, for the rows stored before ingest wrote either. Keyset
// by (session_id, seq, id) on the session index, at most EventRowsPerBatch
// rows, and only the rows whose keys would change are written, so a second
// pass over the same rows produces no dead tuples.
func (s *Store) eventKeysBatch(ctx context.Context, cfg DeriveConfig, version int) (batchResult, error) {
	const step = "event_keys"
	tx, job, done, deferred, err := s.deriveBatch(ctx, cfg, version, step)
	if err != nil || done || deferred {
		return batchResult{done: done, deferred: deferred}, err
	}
	curSession, curSeq, curID := decodeKeysCursor(job.cursor)

	rows, err := tx.Query(ctx, `
		SELECT id, session_id, seq, type, origin, coalesce(agent_id, ''), body,
		       coalesce(prompt_id, ''), coalesce(record_uuid, ''), coalesce(parent_record_uuid, ''),
		       coalesce(request_id, ''), coalesce(message_id, ''), coalesce(tool_use_id, '')
		FROM events
		WHERE (session_id, seq, id) > ($1, $2, $3)
		ORDER BY session_id, seq, id
		LIMIT $4`, curSession, curSeq, curID, cfg.EventRowsPerBatch)
	if err != nil {
		_ = tx.Rollback(ctx)
		return batchResult{}, fmt.Errorf("store: read events for keys: %w", err)
	}
	type keyed struct {
		id   string
		keys eventKeys
	}
	var (
		read                           int
		lastSession, lastID            string
		lastSeq                        int64
		changed                        []keyed
		kindIDs, kindKinds, kindAgents []string
		kindRoleOK                     bool
	)
	for rows.Next() {
		var (
			id, sid, typ, origin, agent string
			seq                         int64
			body                        []byte
			stored                      eventKeys
		)
		if err := rows.Scan(&id, &sid, &seq, &typ, &origin, &agent, &body,
			&stored.prompt, &stored.record, &stored.parentRecord, &stored.request, &stored.message, &stored.toolUse); err != nil {
			rows.Close()
			_ = tx.Rollback(ctx)
			return batchResult{}, fmt.Errorf("store: scan event for keys: %w", err)
		}
		read++
		lastSession, lastSeq, lastID = sid, seq, id
		var ev event.Event
		if err := json.Unmarshal(body, &ev); err != nil {
			// An expired or malformed body has nothing to read; the row keeps
			// what it has.
			continue
		}
		ev.ID, ev.SessionID, ev.Seq = id, sid, seq
		ev.Type, ev.Origin, ev.AgentID = event.Type(typ), event.Origin(origin), agent
		k := derive.KeysOf(ev, ev.Raw)
		want := eventKeys{
			prompt: k.PromptID, record: k.RecordUUID, parentRecord: k.ParentRecordUUID,
			request: k.RequestID, message: k.MessageID, toolUse: k.ToolUseID,
		}
		merged := eventKeys{
			prompt: firstNonEmpty(stored.prompt, want.prompt), record: firstNonEmpty(stored.record, want.record),
			parentRecord: firstNonEmpty(stored.parentRecord, want.parentRecord), request: firstNonEmpty(stored.request, want.request),
			message: firstNonEmpty(stored.message, want.message), toolUse: firstNonEmpty(stored.toolUse, want.toolUse),
		}
		if merged != stored {
			changed = append(changed, keyed{id: id, keys: merged})
		}
		if m, ok := derive.KindOf(ev); ok && strings.TrimSpace(ev.Text) != "" {
			kindRoleOK = true
			kindIDs = append(kindIDs, id)
			kindKinds = append(kindKinds, string(m.Kind))
			kindAgents = append(kindAgents, agent)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		return batchResult{}, fmt.Errorf("store: read events for keys: %w", err)
	}

	if len(changed) > 0 {
		n := len(changed)
		ids := make([]string, n)
		prompts, records, parents, requests, messages, toolUses := make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
		for i, c := range changed {
			ids[i] = c.id
			prompts[i], records[i], parents[i] = c.keys.prompt, c.keys.record, c.keys.parentRecord
			requests[i], messages[i], toolUses[i] = c.keys.request, c.keys.message, c.keys.toolUse
		}
		if _, err := tx.Exec(ctx, `
			UPDATE events e SET
				prompt_id          = nullif(u.prompt_id, ''),
				record_uuid        = nullif(u.record_uuid, ''),
				parent_record_uuid = nullif(u.parent_record_uuid, ''),
				request_id         = nullif(u.request_id, ''),
				message_id         = nullif(u.message_id, ''),
				tool_use_id        = nullif(u.tool_use_id, '')
			FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
			     AS u(id, prompt_id, record_uuid, parent_record_uuid, request_id, message_id, tool_use_id)
			WHERE e.id = u.id`, ids, prompts, records, parents, requests, messages, toolUses); err != nil {
			_ = tx.Rollback(ctx)
			return batchResult{}, fmt.Errorf("store: write event keys: %w", err)
		}
	}
	if kindRoleOK {
		if err := writeMessageKinds(ctx, tx, kindIDs, kindKinds, kindAgents); err != nil {
			_ = tx.Rollback(ctx)
			return batchResult{}, err
		}
	}

	cursor := job.cursor
	if read > 0 {
		cursor = encodeKeysCursor(lastSession, lastSeq, lastID)
	}
	finished := read < cfg.EventRowsPerBatch
	if err := commitBatch(ctx, tx, version, step, cursor, read, finished); err != nil {
		return batchResult{}, err
	}
	return batchResult{rows: read, done: finished}, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// keysCursorSep separates the three parts of the event_keys cursor. The
// unit separator (0x1f) is valid UTF-8, which NUL is not in a Postgres TEXT
// column, and appears in no session id or event id either capture path
// produces (uuids, hashes, hook-stamped tokens).
const keysCursorSep = "\x1f"

// The event_keys cursor: "session_id<US>seq<US>id", with the empty cursor
// standing for the start. seq is stored decimal so the cursor stays text.
func encodeKeysCursor(sid string, seq int64, id string) string {
	return sid + keysCursorSep + strconv.FormatInt(seq, 10) + keysCursorSep + id
}

func decodeKeysCursor(c string) (string, int64, string) {
	if c == "" {
		return "", -1, ""
	}
	parts := strings.SplitN(c, keysCursorSep, 3)
	if len(parts) != 3 {
		return "", -1, ""
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", -1, ""
	}
	return parts[0], seq, parts[2]
}

// writeMessageKinds sets the kind and thread of messages whose stored value
// differs. Only differing rows are written, so a re-run is free.
func writeMessageKinds(ctx context.Context, q Queryer, ids, kinds, agents []string) error {
	if _, err := q.Exec(ctx, `
		UPDATE messages m SET kind = u.kind, agent_id = nullif(u.agent_id, '')
		FROM unnest($1::text[], $2::text[], $3::text[]) AS u(event_id, kind, agent_id)
		WHERE m.event_id = u.event_id
		  AND (m.kind IS DISTINCT FROM u.kind OR m.agent_id IS DISTINCT FROM nullif(u.agent_id, ''))`,
		ids, kinds, agents); err != nil {
		return fmt.Errorf("store: write message kinds: %w", err)
	}
	return nil
}

// classifyMessages classifies the messages of one session that still carry
// no kind, from their text alone: the rows whose bodies the keys step could
// not read (expired, malformed) or that ingest stored before the column
// existed and the keys step has not reached. Text is what both capture
// paths share, so the verdict is the same one ingest would have reached
// without the record's flags.
func (s *Store) classifyMessages(ctx context.Context, q Queryer, sessionID string) (int, error) {
	rows, err := q.Query(ctx, `
		SELECT m.event_id, m.role, m.text, coalesce(e.agent_id, ''), e.seq
		FROM messages m JOIN events e ON e.id = m.event_id
		WHERE m.session_id = $1 AND (m.kind = '' OR m.agent_id IS DISTINCT FROM e.agent_id)`, sessionID)
	if err != nil {
		return 0, fmt.Errorf("store: read unclassified messages of %s: %w", sessionID, err)
	}
	var ids, kinds, agents []string
	for rows.Next() {
		var (
			id, role, text, agent string
			seq                   int64
		)
		if err := rows.Scan(&id, &role, &text, &agent, &seq); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: scan message for kind: %w", err)
		}
		var kind normalize.Kind
		switch role {
		case "user":
			agentStream := agent != ""
			kind = normalize.ClassifyUser(text, nil, agentStream, agentStream && seq <= 1).Kind
		case "assistant":
			kind = normalize.ClassifyAssistant(text, true)
		default:
			kind = normalize.KindTool
		}
		ids = append(ids, id)
		kinds = append(kinds, string(kind))
		agents = append(agents, agent)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: read unclassified messages of %s: %w", sessionID, err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return len(ids), writeMessageKinds(ctx, q, ids, kinds, agents)
}

// healthHourlyExists reports whether PR E's rollup table is there yet; the
// capture-loss rule reads it and a query against an absent table would fail
// every batch of the class step.
func (s *Store) healthHourlyExists(ctx context.Context, q Queryer) bool {
	var ok bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('health_hourly') IS NOT NULL`).Scan(&ok); err != nil {
		return false
	}
	return ok
}

// indexStep builds one of the runner's indexes CONCURRENTLY, outside any
// transaction, and records the step. An index left INVALID by a cancelled
// build is dropped and rebuilt rather than honoured by IF NOT EXISTS, which
// would otherwise skip it forever.
//
// The whole of it runs under the derive lock, taken session-level on the
// one pinned connection that does the work. Built outside the lock, two
// instances booting together would both reach this step: one would start
// the build, the other would read the in-progress index as invalid, wait
// for the first on DROP INDEX CONCURRENTLY, drop the valid index the first
// had just finished, and build it again, a minute per events index each
// time (review-1 finding 5). Session-level rather than transaction-level
// because CONCURRENTLY cannot run inside a transaction block; the lock
// conflicts with the transaction-level lock every other batch takes, so a
// pass that finds it held stands down as it does for any other batch.
func (s *Store) indexStep(ctx context.Context, version int, step deriveStep) (batchResult, error) {
	ddl := ""
	for _, ix := range deriveIndexes {
		if ix.name == step.index {
			ddl = ix.ddl
		}
	}
	if ddl == "" {
		return batchResult{}, fmt.Errorf("store: unknown derive index %s", step.index)
	}
	acquirer, ok := s.db.(connSource)
	if !ok {
		return batchResult{}, fmt.Errorf("store: building %s needs a pinned connection", step.index)
	}
	conn, err := acquirer.Acquire(ctx)
	if err != nil {
		return batchResult{}, fmt.Errorf("store: acquire a connection: %w", err)
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, deriveLockKey).Scan(&got); err != nil {
		return batchResult{}, fmt.Errorf("store: take the derive lock: %w", err)
	}
	if !got {
		return batchResult{deferred: true}, nil
	}
	// Released whatever happened, on a context of its own: the connection
	// goes back to the pool, and a session lock left on it would hold every
	// other instance's runner for as long as the connection lived.
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, deriveLockKey) }()

	var finished *time.Time
	if err := conn.QueryRow(ctx, `
		SELECT finished_at FROM derive_jobs WHERE version = $1 AND step = $2`, version, step.name).Scan(&finished); err != nil {
		return batchResult{}, fmt.Errorf("store: read derive job %s: %w", step.name, err)
	}
	if finished != nil {
		return batchResult{done: true}, nil
	}
	var valid *bool
	if err := conn.QueryRow(ctx, `
		SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)`, step.index).Scan(&valid); err != nil && !noRows(err) {
		return batchResult{}, fmt.Errorf("store: check index %s: %w", step.index, err)
	}
	if valid != nil && !*valid {
		if err := execUnbounded(ctx, conn, "DROP INDEX CONCURRENTLY IF EXISTS "+step.index); err != nil {
			return batchResult{}, fmt.Errorf("store: drop invalid index %s: %w", step.index, err)
		}
		valid = nil
	}
	if valid == nil {
		if err := execUnbounded(ctx, conn, ddl); err != nil {
			return batchResult{}, fmt.Errorf("store: build index %s: %w", step.index, err)
		}
	}
	// Recorded on the same connection while the lock is still held, so no
	// other instance can be between the build and the record.
	if _, err := conn.Exec(ctx, `
		UPDATE derive_jobs SET
			cursor      = $3,
			processed   = processed + 1,
			started_at  = coalesce(started_at, now()),
			updated_at  = now(),
			finished_at = now()
		WHERE version = $1 AND step = $2 AND finished_at IS NULL`, version, step.name, step.index); err != nil {
		return batchResult{}, fmt.Errorf("store: advance derive job %s: %w", step.name, err)
	}
	// No rows: an index step is one build, and the pacer that follows a
	// body-reading step must not be handed a row count to pace.
	return batchResult{done: true}, nil
}

// statementTimeoutShape is what SHOW statement_timeout answers: a number
// with an optional unit. Anything else is not put back into a SET.
var statementTimeoutShape = regexp.MustCompile(`^[0-9]+(us|ms|s|min|h|d)?$`)

// execUnbounded runs one statement on a pinned connection outside any
// transaction and with no statement timeout: a concurrent index build on
// the events table runs a minute past the pool's thirty-second ceiling. The
// ceiling is read first and put back afterwards, by value rather than by
// RESET, because the pool sets it with a plain SET on connect and RESET
// would return the connection to the server's default, which is none.
func execUnbounded(ctx context.Context, conn Conn, sql string) error {
	var prev string
	if err := conn.QueryRow(ctx, `SHOW statement_timeout`).Scan(&prev); err != nil {
		return fmt.Errorf("store: read statement_timeout: %w", err)
	}
	if !statementTimeoutShape.MatchString(prev) {
		return fmt.Errorf("store: statement_timeout reads %q; not touching it", prev)
	}
	if _, err := conn.Exec(ctx, `SET statement_timeout = 0`); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SET statement_timeout = '`+prev+`'`) }()
	_, err := conn.Exec(ctx, sql)
	return err
}

// DeriveDirty runs one dirty-set pass: the sessions ingest has touched since
// they were last derived, oldest first, each folded, classified and
// cleared inside its own transaction. The flag is cleared only if nothing
// touched the row while the fold ran, judged under the row lock at the end,
// so a batch that landed mid-fold is folded again on the next pass rather
// than lost.
//
// A session whose fold fails does not fail the pass: it is counted, named
// by a "derive session failed" line, left dirty, and parked in memory for a
// backoff that doubles per failure. Without the parking it would head the
// next pass too, and since the pass runs in updated_at order and the failing
// row keeps its updated_at, every session touched after it would never be
// reached (review-1 finding 2).
func (s *Store) DeriveDirty(ctx context.Context, cfg DeriveConfig) (res DeriveDirtyResult, err error) {
	cfg = cfg.withDefaults()
	started := time.Now()
	// Named results, so the deferred Elapsed reaches the caller (finding 24).
	defer func() { res.Elapsed = time.Since(started) }()

	now := cfg.Now()
	parked := s.parkedSessions(now)
	res.Parked = len(parked)
	rows, err := s.db.Query(ctx, `
		SELECT session_id FROM sessions
		WHERE derive_dirty AND NOT (session_id = ANY($2::text[]))
		ORDER BY updated_at LIMIT $1`, cfg.DirtyPerPass, parked)
	if err != nil {
		return res, fmt.Errorf("store: read dirty sessions: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return res, fmt.Errorf("store: scan dirty session: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("store: read dirty sessions: %w", err)
	}

	healthHourly := s.healthHourlyExists(ctx, s.db)
	for _, sid := range ids {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		turns, err := s.deriveOne(ctx, cfg, sid, healthHourly)
		switch {
		case errors.Is(err, errSessionBusy):
			res.Skipped++
		case errors.Is(err, ErrNotFound):
			res.Skipped++
			s.unparkSession(sid)
		case err != nil:
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			res.Failed++
			attempts := s.parkSession(sid, now)
			s.logSessionFailed(ctx, "dirty", "", sid, attempts, err)
		default:
			res.Folded++
			res.Turns += turns
			s.unparkSession(sid)
		}
	}
	// The skill re-derive queue rides the same tick: the sessions whose
	// skill rows a savepoint rollback, a lock wait, a step skip or a
	// rollback window lost, twenty per pass, oldest first.
	drained, drainParked, err := s.DrainSkillRederive(ctx, skillRederivePerTick)
	res.Rederived, res.RederiveParked = drained, drainParked
	if err != nil {
		return res, err
	}
	return res, nil
}

// skillRederivePerTick bounds one tick's drain of the skill re-derive
// queue: 2,400 sessions an hour at the dirty cadence, enough to recover a
// rollback window inside a working day without contending with the fold.
const skillRederivePerTick = 20

// parkSession records a failed fold and reports how many in a row.
func (s *Store) parkSession(sessionID string, now time.Time) int {
	s.dirtyParked.mu.Lock()
	defer s.dirtyParked.mu.Unlock()
	if s.dirtyParked.rows == nil {
		s.dirtyParked.rows = map[string]parkedSession{}
	}
	if _, known := s.dirtyParked.rows[sessionID]; !known && len(s.dirtyParked.rows) >= dirtyParkedMax {
		for id, p := range s.dirtyParked.rows {
			if !p.until.After(now) {
				delete(s.dirtyParked.rows, id)
			}
		}
		// Nothing expired: the entry whose backoff ends soonest goes, so the
		// map never grows past its cap (review-2 finding 27). That session
		// is tried a little early, which is the cheaper mistake.
		if len(s.dirtyParked.rows) >= dirtyParkedMax {
			var oldest string
			var oldestUntil time.Time
			for id, p := range s.dirtyParked.rows {
				if oldest == "" || p.until.Before(oldestUntil) || (p.until.Equal(oldestUntil) && id < oldest) {
					oldest, oldestUntil = id, p.until
				}
			}
			delete(s.dirtyParked.rows, oldest)
		}
	}
	p := s.dirtyParked.rows[sessionID]
	p.attempts++
	wait := dirtyParkBase << uint(min(p.attempts-1, 10))
	if wait > dirtyParkCap || wait <= 0 {
		wait = dirtyParkCap
	}
	p.until = now.Add(wait)
	s.dirtyParked.rows[sessionID] = p
	return p.attempts
}

// unparkSession forgets a session whose fold succeeded.
func (s *Store) unparkSession(sessionID string) {
	s.dirtyParked.mu.Lock()
	delete(s.dirtyParked.rows, sessionID)
	s.dirtyParked.mu.Unlock()
}

// parkedSessions lists the sessions still inside their backoff, sorted so
// the query text is stable.
func (s *Store) parkedSessions(now time.Time) []string {
	s.dirtyParked.mu.Lock()
	defer s.dirtyParked.mu.Unlock()
	out := make([]string, 0, len(s.dirtyParked.rows))
	for id, p := range s.dirtyParked.rows {
		if p.until.After(now) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// deriveOne folds and classifies one dirty session in its own transaction.
func (s *Store) deriveOne(ctx context.Context, cfg DeriveConfig, sessionID string, healthHourly bool) (int, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: begin derive of %s: %w", sessionID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := WithStatementTimeout(ctx, tx, cfg.StatementTimeout); err != nil {
		return 0, err
	}
	locked, err := sessionLocked(ctx, tx, sessionID)
	if err != nil {
		return 0, err
	}
	if !locked {
		return 0, errSessionBusy
	}
	facts, err := readSessionFacts(ctx, tx, sessionID)
	if err != nil {
		return 0, err
	}
	// Kinds first, from text, for the rows that have none: sessions stored
	// before kinds existed are all dirty on the first boot after the column
	// shipped, and a fold over unclassified prompts would count nobody's
	// prompts as a person's until the versioned pass reached them. The
	// versioned event_keys step re-classifies with the record in hand and
	// overwrites where the verdict differs.
	if _, err := s.classifyMessages(ctx, tx, sessionID); err != nil {
		return 0, err
	}
	turns, err := s.foldSession(ctx, tx, sessionID, DerivedSchema)
	if err != nil {
		return 0, err
	}
	// The row lock, taken now rather than before the fold, so ingest waited
	// for the few statements below rather than for the whole fold, and
	// before this transaction's own writes to the row, so only what ingest
	// committed between the facts read and this lock reads as a newer
	// updated_at, which keeps the row dirty for the next pass.
	var touched bool
	if err := tx.QueryRow(ctx, `
		SELECT updated_at <> $2 FROM sessions WHERE session_id = $1 FOR UPDATE`,
		sessionID, facts.UpdatedAt).Scan(&touched); err != nil {
		return 0, fmt.Errorf("store: lock %s to clear dirty: %w", sessionID, err)
	}
	if err := rollupFromTurns(ctx, tx, sessionID); err != nil {
		return 0, err
	}
	if _, err := s.classifySession(ctx, tx, sessionID, cfg.Now(), healthHourly); err != nil {
		return 0, err
	}
	if _, err := retitleSession(ctx, tx, sessionID); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET derive_dirty = $2 WHERE session_id = $1`, sessionID, touched); err != nil {
		return 0, fmt.Errorf("store: clear dirty on %s: %w", sessionID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("store: commit derive of %s: %w", sessionID, err)
	}
	return turns, nil
}
