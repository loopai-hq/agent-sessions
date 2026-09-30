//go:build integration

package store

// The runner's mechanics against a real Postgres: a pass interrupted after
// one committed batch resumes from its cursor and ends exactly where a clean
// pass ends; a pass over a stamped corpus does nothing; a runner that finds
// the lock held stands down; a step that fails five times parks the pass
// with its error and the runbook's reset statement brings it back.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

// dumpDerived renders every row the runner derives, in a fixed order, minus
// the timestamps that say when rather than what.
func dumpDerived(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	var out []string
	for _, sql := range []string{
		`SELECT row_to_json(x)::text FROM (
			SELECT session_id, thread, turn_index, turn_key, kind, prompt_event_id, final_event_id, outcome,
			       inherited, started_at, first_activity_at, last_activity_at, answered_at, wall_ms, active_ms,
			       idle_ms, waiting_for_human_ms, origins, merged, prompts, tool_calls, errors, subagents,
			       files_changed, tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, cost_usd,
			       model, derived_version
			FROM turns ORDER BY session_id, thread, turn_index) x`,
		`SELECT row_to_json(x)::text FROM (SELECT * FROM turn_events ORDER BY session_id, thread, turn_index, event_id) x`,
		`SELECT row_to_json(x)::text FROM (
			SELECT session_id, session_type, empty_kind, head_state, user_turns, human_turns, tool_calls, errors,
			       subagents, content_events, first_prompt, title_source, parent_session_id, lineage_source
			FROM sessions ORDER BY session_id) x`,
		`SELECT row_to_json(x)::text FROM (
			SELECT id, prompt_id, record_uuid, parent_record_uuid, request_id, message_id, tool_use_id, superseded_by
			FROM events ORDER BY id) x`,
		`SELECT row_to_json(x)::text FROM (SELECT event_id, kind, agent_id FROM messages ORDER BY event_id) x`,
		`SELECT row_to_json(x)::text FROM (SELECT session_id, path, version_count, latest_sha256 FROM artifacts ORDER BY session_id, path) x`,
		`SELECT row_to_json(x)::text FROM (SELECT session_id, url, occurrences FROM links ORDER BY session_id, url) x`,
	} {
		rows, err := pool.Query(ctx, sql)
		if err != nil {
			t.Fatalf("dump: %v", err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("dump scan: %v", err)
			}
			out = append(out, line)
		}
		rows.Close()
	}
	return out
}

// clearDerived takes every derived row and column back to what a corpus
// stored before the derive layer existed looks like, and resets the
// runner's ledger, so a pass starts from the same place twice.
func clearDerived(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, sql := range []string{
		`DELETE FROM turns`,
		`UPDATE events SET superseded_by = NULL, prompt_id = NULL, record_uuid = NULL, parent_record_uuid = NULL,
		                  request_id = NULL, message_id = NULL, tool_use_id = NULL`,
		`UPDATE messages SET kind = ''`,
		`UPDATE sessions SET user_turns = 0, human_turns = 0, tool_calls = 0, errors = 0, subagents = 0,
		                    first_prompt = NULL, title_source = 'none', derive_dirty = true`,
		`DELETE FROM artifacts`,
		`DELETE FROM links`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("clear: %v", err)
		}
	}
	resetDerived(t)
}

func runnerSeed(t *testing.T, s *Store) {
	t.Helper()
	mustPrincipal(t, s, dvEmail, RoleMember)
	for i, sid := range []string{"s-run-a", "s-run-b", "s-run-c", "s-run-d"} {
		dualOriginSession(t, s, sid, dv0.Add(time.Duration(i)*time.Hour), 2)
	}
	// A file change and a link, so the artifacts and links steps have work.
	fc := fileChange("s-run-a-file", "s-run-a", dvEmail, "/tmp/x.go", "package x\n", 90, dv0.Add(30*time.Second), true)
	// The same PR named by two events: occurrences count events that
	// mention a URL, and two batches would have accumulated to two as well.
	link := hookAt("s-run-a-link", "s-run-a", dvEmail, event.AssistantTurn, 91, dv0.Add(31*time.Second), "see https://github.com/loopai-hq/agent-sessions/pull/7")
	again := hookAt("s-run-a-link-2", "s-run-a", dvEmail, event.AssistantTurn, 92, dv0.Add(32*time.Second), "merged https://github.com/loopai-hq/agent-sessions/pull/7")
	ingestBatch(t, s, fc, link, again)
}

var noSleep = func(context.Context, time.Duration) error { return nil }

func TestIntegrationTheVersionedPassIsResumableIdempotentAndSingleWinner(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	runnerSeed(t, s)
	clearDerived(t)
	always := derive.Window{Always: true}

	// Interrupted: the pacer's first sleep is where the context is cut, which
	// is after the first event_keys batch has committed and before the next.
	ctx, cancel := context.WithCancel(context.Background())
	interrupted := DeriveConfig{
		Window: always, RowsPerSec: 1, EventRowsPerBatch: 8, SessionsPerBatch: 1,
		Sleep: func(ctx context.Context, d time.Duration) error {
			cancel()
			return ctx.Err()
		},
	}
	if _, err := s.RunDerive(ctx, interrupted); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted pass returned %v, want context.Canceled", err)
	}
	var processed int64
	var finished *time.Time
	if err := pool.QueryRow(context.Background(), `
		SELECT processed, finished_at FROM derive_jobs WHERE version = $1 AND step = 'event_keys'`, DerivedSchema).Scan(&processed, &finished); err != nil {
		t.Fatal(err)
	}
	if processed != 8 || finished != nil {
		t.Fatalf("after the interruption event_keys processed %d finished %v, want one batch of 8 committed and the step open", processed, finished)
	}
	if n := countRows(t, `SELECT count(*) FROM derive_jobs WHERE version = $1 AND finished_at IS NOT NULL`, DerivedSchema); n != len(deriveIndexes) {
		t.Errorf("%d steps finished before the interruption, want the %d index steps", n, len(deriveIndexes))
	}

	// Resumed: the cursor carries on; nothing is re-read from the start.
	resumed := DeriveConfig{Window: always, RowsPerSec: 1_000_000, EventRowsPerBatch: 8, SessionsPerBatch: 1, Sleep: noSleep}
	pass, err := s.RunDerive(context.Background(), resumed)
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done || pass.Failed != "" {
		t.Fatalf("resumed pass = %+v, want done", pass)
	}
	for _, st := range pass.Steps {
		if st.Step == "event_keys" && st.Rows+8 != int64(countRows(t, `SELECT count(*) FROM events`)) {
			t.Errorf("resumed event_keys read %d rows; with 8 already done that is not the whole table", st.Rows)
		}
	}
	if err := pool.QueryRow(context.Background(), `SELECT processed FROM derive_jobs WHERE version = $1 AND step = 'event_keys'`, DerivedSchema).Scan(&processed); err != nil {
		t.Fatal(err)
	}
	if processed != int64(countRows(t, `SELECT count(*) FROM events`)) {
		t.Errorf("event_keys processed %d in total, want every event once", processed)
	}
	var version int
	if err := pool.QueryRow(context.Background(), `SELECT version FROM derived_schema WHERE only_row`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != DerivedSchema {
		t.Fatalf("derived_schema.version = %d after a complete pass, want %d", version, DerivedSchema)
	}
	afterResume := dumpDerived(t)
	if len(afterResume) == 0 {
		t.Fatal("the pass derived nothing")
	}
	// The per-session artifacts and links steps rebuilt what the live path
	// had derived and recounted the mentions from the whole session.
	if n := countRows(t, `SELECT count(*) FROM artifacts WHERE session_id = 's-run-a' AND path = '/tmp/x.go' AND version_count = 1`); n != 1 {
		t.Errorf("artifacts of s-run-a after the pass = %d matching rows, want 1", n)
	}
	if n := countRows(t, `SELECT occurrences FROM links WHERE session_id = 's-run-a' AND url = 'https://github.com/loopai-hq/agent-sessions/pull/7'`); n != 2 {
		t.Errorf("the PR link is counted %d times, want 2: occurrences are recounted from the whole session, never accumulated across passes", n)
	}

	// Idempotent: a pass over the stamped corpus reads one row and stops.
	pass, err = s.RunDerive(context.Background(), resumed)
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done || len(pass.Steps) != 0 {
		t.Errorf("pass over a stamped corpus = %+v, want done with no steps", pass)
	}
	if got := dumpDerived(t); !reflect.DeepEqual(got, afterResume) {
		t.Errorf("an idle pass changed derived rows")
	}

	// Clean: the same corpus from the same blank state, in one go, is the
	// same rows.
	clearDerived(t)
	pass, err = s.RunDerive(context.Background(), DeriveConfig{Window: always, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done {
		t.Fatalf("clean pass = %+v", pass)
	}
	if clean := dumpDerived(t); !reflect.DeepEqual(clean, afterResume) {
		t.Errorf("the resumed pass and a clean pass derived different rows:\nresumed %d lines\nclean   %d lines", len(afterResume), len(clean))
		for i := range clean {
			if i < len(afterResume) && clean[i] != afterResume[i] {
				t.Errorf("first difference:\n  resumed %s\n  clean   %s", afterResume[i], clean[i])
				break
			}
		}
	}

	// Stands down: another instance holds the lock, this pass does nothing
	// and says so.
	clearDerived(t)
	holder, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	htx, err := holder.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := htx.Exec(context.Background(), `SELECT pg_advisory_xact_lock($1)`, deriveLockKey); err != nil {
		t.Fatal(err)
	}
	pass, err = s.RunDerive(context.Background(), DeriveConfig{Window: always, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Deferred || pass.Done {
		t.Errorf("pass under another holder's lock = %+v, want deferred", pass)
	}
	if n := countRows(t, `SELECT count(*) FROM derive_jobs WHERE version = $1 AND (finished_at IS NOT NULL OR processed > 0)`, DerivedSchema); n != 0 {
		t.Errorf("%d steps advanced under another holder's lock", n)
	}
	_ = htx.Rollback(context.Background())
	holder.Release()

	// Two runners at once: both return without error, the corpus is derived
	// once, and the rows are the clean pass's.
	var wg sync.WaitGroup
	results := make([]DerivePass, 2)
	errs := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = s.RunDerive(context.Background(), DeriveConfig{Window: always, RowsPerSec: 1_000_000, SessionsPerBatch: 1, Sleep: noSleep})
		}(i)
	}
	wg.Wait()
	for i := range results {
		if errs[i] != nil {
			t.Errorf("concurrent runner %d: %v", i, errs[i])
		}
	}
	// Whichever stood down, the work finishes: run until done.
	for range 20 {
		pass, err = s.RunDerive(context.Background(), DeriveConfig{Window: always, RowsPerSec: 1_000_000, Sleep: noSleep})
		if err != nil {
			t.Fatal(err)
		}
		if pass.Done {
			break
		}
	}
	if !pass.Done {
		t.Fatalf("the corpus never finished after two concurrent runners: %+v", pass)
	}
	if got := dumpDerived(t); !reflect.DeepEqual(got, afterResume) {
		t.Errorf("two concurrent runners derived different rows from one runner")
	}
}

// A step that keeps failing stops the pass with its error after five
// attempts, later steps do not run, and the statement the failure line
// prints is what brings the pass back. The failure is the step's own (its
// cursor cannot be committed for the turns step), not one session's: a
// session that fails alone is passed over instead (the poison test below).
func TestIntegrationAFailingStepParksThePassAndTheResetStatementResumesIt(t *testing.T) {
	s := newStore(t, nil)
	runnerSeed(t, s)
	clearDerived(t)
	ctx := context.Background()
	cfg := DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, SessionsPerBatch: 1, Sleep: noSleep}
	// clearDerived leaves the stamp under every step's since, so the whole
	// pass runs; what the parked pass must not do is move it. Read rather
	// than spelled as DerivedSchema-1: since version 4 a stamp one below
	// the code's seeds the thirteen earlier steps finished, so a full pass
	// starts lower than that.
	var stampBefore int
	if err := pool.QueryRow(ctx, `SELECT version FROM derived_schema WHERE only_row`).Scan(&stampBefore); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION test_block() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.step = 'turns' AND NEW.cursor IS DISTINCT FROM OLD.cursor THEN
				RAISE EXCEPTION 'test_block';
			END IF;
			RETURN NEW;
		END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER test_block BEFORE UPDATE ON derive_jobs FOR EACH ROW EXECUTE FUNCTION test_block()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_block ON derive_jobs`)
		_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS test_block()`)
	})

	pass, err := s.RunDerive(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if pass.Failed != "turns" || !strings.Contains(pass.LastError, "test_block") || pass.Done {
		t.Fatalf("pass = %+v, want parked on turns with the trigger named", pass)
	}
	var attempts int
	var lastError string
	if err := pool.QueryRow(ctx, `SELECT attempts, last_error FROM derive_jobs WHERE version = $1 AND step = 'turns'`, DerivedSchema).Scan(&attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if attempts != deriveRetryCap || !strings.Contains(lastError, "test_block") {
		t.Errorf("derive_jobs turns = %d attempts, error %q", attempts, lastError)
	}
	var version int
	if err := pool.QueryRow(ctx, `SELECT version FROM derived_schema WHERE only_row`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != stampBefore {
		t.Errorf("derived_schema.version = %d while a step is parked, want %d: the stamp waits for every step", version, stampBefore)
	}
	if n := countRows(t, `SELECT count(*) FROM derive_jobs WHERE version = $1 AND step IN ('rollups', 'session_class', 'artifacts', 'links') AND (processed > 0 OR finished_at IS NOT NULL)`, DerivedSchema); n != 0 {
		t.Errorf("%d later steps ran past the failed one", n)
	}
	// The steps before the failing one were committed: the cursor moved
	// with the work up to the step that could not commit.
	if n := countRows(t, `SELECT count(*) FROM derive_jobs WHERE version = $1 AND step IN ('messages_kind', 'titles') AND finished_at IS NOT NULL`, DerivedSchema); n != 2 {
		t.Errorf("%d steps before the failure finished, want 2", n)
	}

	// The next pass is parked too, without re-running the step.
	pass, err = s.RunDerive(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if pass.Failed != "turns" || len(pass.Steps) != 0 {
		t.Errorf("second pass = %+v, want parked without work", pass)
	}

	// The operator's recovery: remove the cause, reset the attempts with the
	// statement the failure line prints, run again.
	if _, err := pool.Exec(ctx, `DROP TRIGGER test_block ON derive_jobs`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE derive_jobs SET attempts = 0 WHERE version = $1 AND step = 'turns'`, DerivedSchema); err != nil {
		t.Fatal(err)
	}
	pass, err = s.RunDerive(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done {
		t.Fatalf("pass after the reset = %+v, want done", pass)
	}
	if n := countRows(t, `SELECT count(*) FROM turns WHERE session_id = 's-run-c'`); n != 2 {
		t.Errorf("s-run-c has %d turns after recovery, want 2", n)
	}
}

// An index left INVALID by a cancelled concurrent build is dropped and
// rebuilt, where IF NOT EXISTS alone would honour the broken one forever.
func TestIntegrationAnInvalidIndexIsDroppedAndRebuilt(t *testing.T) {
	s := newStore(t, nil)
	runnerSeed(t, s)
	clearDerived(t)
	ctx := context.Background()

	// Make the runner's name refer to an invalid index: a UNIQUE build over a
	// column with duplicates fails, and a failed CONCURRENTLY build leaves
	// its index behind marked invalid.
	if _, err := pool.Exec(ctx, `DROP INDEX IF EXISTS events_superseded_idx`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE events SET superseded_by = 'x' WHERE session_id = 's-run-a' AND origin = 'hook'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE UNIQUE INDEX CONCURRENTLY events_superseded_idx ON events (session_id) WHERE superseded_by IS NOT NULL`); err == nil {
		t.Fatal("the unique build over duplicates succeeded; the fixture cannot produce an invalid index")
	}
	var valid bool
	if err := pool.QueryRow(ctx, `SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('events_superseded_idx')`).Scan(&valid); err != nil {
		t.Fatalf("the failed build left no index: %v", err)
	}
	if valid {
		t.Fatal("the failed build left a valid index; nothing to recover from")
	}
	if _, err := pool.Exec(ctx, `UPDATE events SET superseded_by = NULL`); err != nil {
		t.Fatal(err)
	}

	pass, err := s.RunDerive(ctx, DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done {
		t.Fatalf("pass = %+v", pass)
	}
	var unique bool
	if err := pool.QueryRow(ctx, `SELECT indisvalid, indisunique FROM pg_index WHERE indexrelid = to_regclass('events_superseded_idx')`).Scan(&valid, &unique); err != nil {
		t.Fatalf("index after the pass: %v", err)
	}
	if !valid || unique {
		t.Errorf("events_superseded_idx after the pass: valid %v unique %v, want the runner's own valid non-unique index", valid, unique)
	}
}

// One failing session must not stop the dirty pass for every session with a
// newer updated_at (review-1 finding 2). Three dirty sessions in updated_at
// order, the middle one unfoldable: the pass folds the other two, returns no
// error, names the bad one in a "derive session failed" line, leaves it
// dirty and parks it for a backoff that doubles per failure, so it cannot
// head the next pass; once the cause is gone it folds like any other.
func TestIntegrationADirtyPassIsolatesAFailingSession(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	var logs bytes.Buffer
	s.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	for i, sid := range []string{"s-dirty-old", "s-dirty-bad", "s-dirty-new"} {
		dualOriginSession(t, s, sid, dv0.Add(time.Duration(i)*time.Hour), 1)
		if _, err := pool.Exec(ctx, `UPDATE sessions SET updated_at = $2, derive_dirty = true WHERE session_id = $1`,
			sid, dv0.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE turns ADD CONSTRAINT test_block_dirty CHECK (session_id <> 's-dirty-bad')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `ALTER TABLE turns DROP CONSTRAINT IF EXISTS test_block_dirty`)
	})

	now := dv0.Add(24 * time.Hour)
	pass := func(at time.Time) DeriveDirtyResult {
		res, err := s.DeriveDirty(ctx, DeriveConfig{Now: func() time.Time { return at }})
		if err != nil {
			t.Fatalf("DeriveDirty returned %v; one bad session must not fail the pass", err)
		}
		return res
	}
	res := pass(now)
	if res.Folded != 2 || res.Failed != 1 || res.Parked != 0 {
		t.Errorf("first pass = %+v, want the two good sessions folded and one failure", res)
	}
	for sid, wantDirty := range map[string]bool{"s-dirty-old": false, "s-dirty-bad": true, "s-dirty-new": false} {
		if got := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = $1 AND derive_dirty`, sid) == 1; got != wantDirty {
			t.Errorf("%s dirty = %v, want %v", sid, got, wantDirty)
		}
	}
	if n := countRows(t, `SELECT count(*) FROM turns WHERE session_id = 's-dirty-new'`); n != 1 {
		t.Errorf("s-dirty-new has %d turns after the pass, want 1: the session behind the failure was starved", n)
	}
	line := logs.String()
	if !strings.Contains(line, `"msg":"derive session failed"`) || !strings.Contains(line, `"session_id":"s-dirty-bad"`) ||
		!strings.Contains(line, `"pass":"dirty"`) || !strings.Contains(line, `"level":"ERROR"`) || !strings.Contains(line, "test_block_dirty") {
		t.Errorf("no ERROR \"derive session failed\" line names the session and its error:\n%s", line)
	}

	// Parked: the next pass at the same clock leaves it out, and the pass
	// after the first backoff tries it again.
	if res := pass(now); res.Folded != 0 || res.Failed != 0 || res.Parked != 1 {
		t.Errorf("second pass = %+v, want the failed session parked and nothing else to do", res)
	}
	if res := pass(now.Add(dirtyParkBase + time.Second)); res.Failed != 1 || res.Parked != 0 {
		t.Errorf("pass after the first backoff = %+v, want the session tried and failed again", res)
	}
	if res := pass(now.Add(dirtyParkBase + 2*time.Second)); res.Parked != 1 {
		t.Errorf("pass inside the second backoff = %+v, want the session parked", res)
	}
	if n := strings.Count(logs.String(), `"derive session failed"`); n != 2 {
		t.Errorf("%d failure lines, want one per attempt (2)", n)
	}

	// The cause is fixed: past its backoff the session folds and is forgotten.
	if _, err := pool.Exec(ctx, `ALTER TABLE turns DROP CONSTRAINT test_block_dirty`); err != nil {
		t.Fatal(err)
	}
	if res := pass(now.Add(dirtyParkBase + 2*dirtyParkBase + 3*time.Second)); res.Folded != 1 || res.Failed != 0 || res.Parked != 0 {
		t.Errorf("pass after the fix = %+v, want the session folded", res)
	}
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = 's-dirty-bad' AND derive_dirty`); n != 0 {
		t.Error("the recovered session is still dirty")
	}
	if len(s.parkedSessions(now.Add(24*time.Hour))) != 0 || len(s.dirtyParked.rows) != 0 {
		t.Errorf("parking still remembers %v after the session folded", s.dirtyParked.rows)
	}
}

// A poison session must not park a versioned per-session step with no way
// past it (review-1 finding 3): the batch that holds it is retried one
// session per transaction, the session that fails alone is recorded and
// passed over, and the step, and the pass, finish.
func TestIntegrationAPoisonSessionIsSkippedAndTheVersionedPassFinishes(t *testing.T) {
	s := newStore(t, nil)
	runnerSeed(t, s)
	clearDerived(t)
	ctx := context.Background()
	var logs bytes.Buffer
	s.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	if _, err := pool.Exec(ctx, `ALTER TABLE turns ADD CONSTRAINT test_poison CHECK (session_id <> 's-run-c')`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `ALTER TABLE turns DROP CONSTRAINT IF EXISTS test_poison`)
	})

	// Fifty per batch: the poison shares its batch with the other three.
	pass, err := s.RunDerive(ctx, DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done || pass.Failed != "" || pass.Skipped != 1 {
		t.Fatalf("pass = %+v, want done with the one poison session passed over", pass)
	}
	for _, st := range pass.Steps {
		if st.Step == "turns" && st.Skipped != 1 {
			t.Errorf("turns step = %+v, want skipped 1", st)
		}
	}
	line := logs.String()
	if !strings.Contains(line, `"msg":"derive session failed"`) || !strings.Contains(line, `"session_id":"s-run-c"`) ||
		!strings.Contains(line, `"pass":"versioned"`) || !strings.Contains(line, `"step":"turns"`) || !strings.Contains(line, "test_poison") {
		t.Errorf("no \"derive session failed\" line names the session, the step and the error:\n%s", line)
	}
	if n := strings.Count(line, `"derive step failed"`); n != 0 {
		t.Errorf("%d \"derive step failed\" lines for a per-session failure, want 0", n)
	}
	if n := countRows(t, `SELECT count(*) FROM turns WHERE session_id IN ('s-run-a', 's-run-b', 's-run-d')`); n != 6 {
		t.Errorf("the good sessions have %d turns, want 6", n)
	}
	if n := countRows(t, `SELECT count(*) FROM turns WHERE session_id = 's-run-c'`); n != 0 {
		t.Errorf("the poison session has %d turns", n)
	}
	var lastError string
	if err := pool.QueryRow(ctx, `SELECT last_error FROM derive_jobs WHERE version = $1 AND step = 'turns'`, DerivedSchema).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lastError, "s-run-c") || !strings.Contains(lastError, "test_poison") {
		t.Errorf("derive_jobs.last_error = %q, want the skipped session and its error named", lastError)
	}
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = 's-run-c' AND derive_dirty`); n != 1 {
		t.Errorf("the skipped session is not dirty; the dirty pass is what brings it back once the cause is fixed")
	}

	// One per batch: the poison is its own batch and is still passed over.
	clearDerived(t)
	pass, err = s.RunDerive(ctx, DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, SessionsPerBatch: 1, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done || pass.Failed != "" {
		t.Fatalf("pass with one session per batch = %+v, want done", pass)
	}
}

// Index builds run under the derive lock (review-1 finding 5): two
// instances booting together must not both build, and one must not see the
// other's in-progress index as invalid and drop it. A pass that finds the
// lock held stands down before touching the catalog.
func TestIntegrationIndexBuildsRunUnderTheDeriveLock(t *testing.T) {
	s := newStore(t, nil)
	runnerSeed(t, s)
	clearDerived(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DROP INDEX IF EXISTS events_session_prompt_idx`); err != nil {
		t.Fatal(err)
	}

	holder, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_, _ = holder.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, deriveLockKey)
			holder.Release()
		}
	}
	t.Cleanup(release)
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, deriveLockKey); err != nil {
		t.Fatal(err)
	}
	pass, err := s.RunDerive(ctx, DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Deferred || pass.Done {
		t.Errorf("pass under another holder's lock = %+v, want deferred", pass)
	}
	if n := countRows(t, `SELECT count(*) FROM pg_index WHERE indexrelid = to_regclass('events_session_prompt_idx')`); n != 0 {
		t.Errorf("the index was built while another instance held the derive lock")
	}
	release()

	pass, err = s.RunDerive(ctx, DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done {
		t.Fatalf("pass after the lock was released = %+v, want done", pass)
	}
	var valid bool
	if err := pool.QueryRow(ctx, `SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('events_session_prompt_idx')`).Scan(&valid); err != nil || !valid {
		t.Errorf("events_session_prompt_idx after the pass: valid %v err %v", valid, err)
	}
}

// A session another fold holds is not passed over by a versioned per-session
// step (review-1 finding 16): the batch ends before it and the cursor stays
// there, so the session is revisited once the lock is free rather than left
// with stale artifacts and links.
func TestIntegrationALockedSessionEndsTheBatchInsteadOfBeingPassedOver(t *testing.T) {
	s := newStore(t, nil)
	runnerSeed(t, s)
	clearDerived(t)
	ctx := context.Background()
	// The index and keys steps first, so the per-session steps are next.
	always := derive.Window{Always: true}
	holder, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	htx, err := holder.Begin(ctx)
	if err != nil {
		holder.Release()
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = htx.Rollback(context.Background())
			holder.Release()
		}
	}
	t.Cleanup(release)
	if _, err := htx.Exec(ctx, `SELECT pg_advisory_xact_lock($1::int, hashtext($2))`, deriveSessionLockKey, "s-run-b"); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := s.RunDerive(short, DeriveConfig{Window: always, RowsPerSec: 1_000_000}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("pass with s-run-b locked returned %v, want the deadline: the pass waits at the locked session", err)
	}
	var cursor string
	if err := pool.QueryRow(ctx, `SELECT cursor FROM derive_jobs WHERE version = $1 AND step = 'messages_kind'`, DerivedSchema).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if cursor != "s-run-a" {
		t.Errorf("messages_kind cursor = %q with s-run-b locked, want s-run-a: the batch must end before the locked session, not pass over it", cursor)
	}
	release()

	pass, err := s.RunDerive(ctx, DeriveConfig{Window: always, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done {
		t.Fatalf("pass after the lock was released = %+v, want done", pass)
	}
	if n := countRows(t, `SELECT count(*) FROM turns WHERE session_id = 's-run-b'`); n != 2 {
		t.Errorf("s-run-b has %d turns, want 2", n)
	}
}

// A batch of event_keys that cannot meet the statement ceiling must not be
// retried unchanged until the step parks (review-2 finding 22; the laptop
// rehearsal parked at 325,000 rows after five identical attempts). On a
// statement timeout the retry halves the batch, down to a floor of 250 rows,
// and says so in a log line, without counting an attempt; only a batch at
// the floor counts. The rows here carry bodies of 16 KiB (production p99 is
// 43 KB), and a statement-level trigger sleeps 1.4 ms per row the keys
// write touched (one sleep per statement, from the transition table; a
// sleep per row has a timer granularity that varies by host) so the batch's
// cost is proportional to its size whatever the machine: 2,000 rows and
// 1,000 rows run past a 1 s ceiling, 500 do not (0.7 s).
func TestIntegrationEventKeysShrinksItsBatchOnAStatementTimeout(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	var logs bytes.Buffer
	s.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	const rows = 2000
	if _, err := pool.Exec(ctx, `
		INSERT INTO sessions (session_id, email, source, started_at, ended_at, ended, session_type, updated_at, derive_dirty)
		VALUES ('s-heavy', $1, 'claude_code', $2, $2::timestamptz + interval '1 hour', true, 'user', now(), false)`, dvEmail, dv0); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, session_id, email, seq, type, origin, occurred_at, body, capture_version)
		SELECT 'heavy-' || i, 's-heavy', $1, i, 'user_prompt', 'transcript', $2::timestamptz + i * interval '2 seconds',
		       jsonb_build_object('id', 'heavy-' || i, 'type', 'user_prompt', 'text', repeat('x', 16384),
		                          'raw', jsonb_build_object('type', 'user', 'uuid', md5('u' || i), 'promptId', md5('p' || i),
		                                                    'message', jsonb_build_object('role', 'user', 'content', 'prompt ' || i))),
		       3
		FROM generate_series(0, $3 - 1) i`, dvEmail, dv0, rows); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION test_slow_keys() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_sleep(0.0014 * (SELECT count(*) FROM changed WHERE record_uuid IS NOT NULL));
			RETURN NULL;
		END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TRIGGER test_slow_keys AFTER UPDATE ON events REFERENCING NEW TABLE AS changed
		FOR EACH STATEMENT EXECUTE FUNCTION test_slow_keys()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_slow_keys ON events`)
		_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS test_slow_keys()`)
	})
	resetDerived(t)

	cfg := DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, EventRowsPerBatch: rows, StatementTimeout: time.Second, Sleep: noSleep}
	pass, err := s.RunDerive(ctx, cfg)
	if err != nil {
		t.Fatalf("RunDerive: %v", err)
	}
	if !pass.Done || pass.Failed != "" {
		t.Fatalf("pass = %+v, want done: a batch past the ceiling must shrink, not park the step", pass)
	}
	var keys DeriveStepResult
	for _, st := range pass.Steps {
		if st.Step == "event_keys" {
			keys = st
		}
	}
	if !keys.Finished || keys.Rows != rows {
		t.Errorf("event_keys = %+v, want finished over %d rows", keys, rows)
	}
	if keys.Batches < 4 {
		t.Errorf("event_keys ran %d batches, want at least 4 (2,000 rows in batches of 500 after two halvings)", keys.Batches)
	}
	var attempts int
	var lastError string
	if err := pool.QueryRow(ctx, `SELECT attempts, last_error FROM derive_jobs WHERE version = $1 AND step = 'event_keys'`, DerivedSchema).Scan(&attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Errorf("event_keys attempts = %d (%q), want 0: a halving is not an attempt", attempts, lastError)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = 's-heavy' AND record_uuid IS NOT NULL AND prompt_id IS NOT NULL`); n != rows {
		t.Errorf("%d of %d rows keyed", n, rows)
	}
	line := logs.String()
	for _, want := range []string{
		`"msg":"derive batch shrunk"`, `"step":"event_keys"`, `"rows_per_batch":1000`, `"rows_per_batch":500`, `"derive_version":` + strconv.Itoa(DerivedSchema),
	} {
		if !strings.Contains(line, want) {
			t.Errorf("no %s in the log:\n%s", want, line)
		}
	}
	if strings.Contains(line, `"rows_per_batch":250`) {
		t.Errorf("the batch shrank to the floor although 500 rows fit the ceiling:\n%s", line)
	}
	if strings.Contains(line, `"derive step failed"`) {
		t.Errorf("a step failed line was written:\n%s", line)
	}
}

// A batch whose first session is held by another fold is not a batch: the
// beats it waits are not counted on the "derive step" line, and after a
// minute of them in a row the pass stands down as deferred rather than
// spinning at 250 ms for as long as the context lives (review-2 finding 26).
func TestIntegrationAHeldSessionIsWaitedForAndThenDeferredWithoutCountingBeats(t *testing.T) {
	s := newStore(t, nil)
	runnerSeed(t, s)
	clearDerived(t)
	ctx := context.Background()
	holder, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	htx, err := holder.Begin(ctx)
	if err != nil {
		holder.Release()
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = htx.Rollback(context.Background())
			holder.Release()
		}
	}
	t.Cleanup(release)
	if _, err := htx.Exec(ctx, `SELECT pg_advisory_xact_lock($1::int, hashtext($2))`, deriveSessionLockKey, "s-run-a"); err != nil {
		t.Fatal(err)
	}
	beats := 0
	counting := func(ctx context.Context, d time.Duration) error {
		if d == deriveLockedBeat {
			beats++
		}
		return nil
	}
	pass, err := s.RunDerive(ctx, DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: counting})
	if err != nil {
		t.Fatalf("RunDerive with s-run-a held: %v", err)
	}
	if !pass.Deferred || pass.Done || pass.Failed != "" {
		t.Errorf("pass = %+v, want deferred: the first session of every per-session batch is held", pass)
	}
	if beats != deriveLockedBeatCap {
		t.Errorf("%d beats waited, want the cap %d", beats, deriveLockedBeatCap)
	}
	var kind DeriveStepResult
	for _, st := range pass.Steps {
		if st.Step == "messages_kind" {
			kind = st
		}
	}
	if kind.Step == "" || kind.Batches != 0 || kind.Rows != 0 {
		t.Errorf("messages_kind = %+v, want the step reported with no batches: a beat is not a batch", kind)
	}
	if pass.StalledMinutes <= 0 {
		t.Errorf("stalled_minutes = %v, want the time since the step was first attempted", pass.StalledMinutes)
	}
	release()
	pass, err = s.RunDerive(ctx, DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil || !pass.Done {
		t.Fatalf("pass after the release = %+v, %v; want done", pass, err)
	}
}
