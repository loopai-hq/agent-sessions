package store

// What is worth pinning down here is which statements a sweep issues, in which
// order, inside which transaction, and when it stops. All of that is observable
// over the scriptable connection in store_test.go, and none of it needs a
// Postgres — which matters, because the mistake these tests exist to catch is
// one somebody makes on a laptop with no database running.
//
// What a real server has to answer for — that the cascades reach the messages
// and shares of a deleted session, that the partial index is actually the plan
// the sweep gets, that a second sweep removes nothing — lives in
// retention_integration_test.go behind the integration build tag.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// retNow is the clock every test states explicitly, because a cutoff is
// meaningless without one and time.Now would make the assertion unwritable.
var retNow = time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)

const (
	retDay  = 24 * time.Hour
	retYear = 365 * retDay
)

// retStore builds a store over the scriptable connection.
func retStore(db *fakeDB) *Store { return NewWithDB(db, nil) }

// retLockGranted is the stub every batch needs before it may write.
func retLockGranted() *stub {
	return &stub{match: "pg_try_advisory_xact_lock", rows: [][]any{{true}}}
}

// retSQL returns the statements issued, in order.
func retSQL(db *fakeDB) []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make([]string, 0, len(db.calls))
	for _, c := range db.calls {
		out = append(out, c.sql)
	}
	return out
}

// retFind reports the index of the first statement containing sub, or -1.
func retFind(sqls []string, sub string) int {
	for i, s := range sqls {
		if strings.Contains(s, sub) {
			return i
		}
	}
	return -1
}

func retCount(sqls []string, sub string) int {
	n := 0
	for _, s := range sqls {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// The policy itself
// ---------------------------------------------------------------------------

// TestValidateRefusesTheWindowsThatWouldSurpriseWhoeverSetThem is the guard on
// the one setting in this system that destroys data. Each case is a mistake
// somebody can make in a deployment file, and the property is that it is refused
// at boot rather than applied at three in the morning.
func TestValidateRefusesTheWindowsThatWouldSurpriseWhoeverSetThem(t *testing.T) {
	cases := []struct {
		name   string
		policy RetentionPolicy
		want   string // substring of the refusal, empty means it must be accepted
	}{
		{
			name:   "keeping everything forever is a policy, not an error",
			policy: RetentionPolicy{},
		},
		{
			name:   "the shipped default is accepted",
			policy: RetentionPolicy{BodyAfter: 90 * retDay, SessionAfter: retYear},
		},
		{
			name:   "one half switched off does not invalidate the other",
			policy: RetentionPolicy{SessionAfter: retYear},
		},
		{
			name:   "a body window below the floor is a typo, not a policy",
			policy: RetentionPolicy{BodyAfter: retDay, SessionAfter: retYear},
			want:   "below the",
		},
		{
			name:   "a session window below the floor deletes very nearly everything",
			policy: RetentionPolicy{SessionAfter: 2 * retDay},
			want:   "below the",
		},
		{
			name:   "a negative window is a cutoff in the future, which is everything",
			policy: RetentionPolicy{SessionAfter: -retYear},
			want:   "cannot be negative",
		},
		{
			name:   "bodies kept longer than the sessions carrying them can never take effect",
			policy: RetentionPolicy{BodyAfter: 2 * retYear, SessionAfter: retYear},
			want:   "can never take effect",
		},
		{
			name:   "the floor is exactly a week, not more than one",
			policy: RetentionPolicy{BodyAfter: MinRetention, SessionAfter: MinRetention},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.Validate()
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("refused a policy it should accept: %v", err)
			case tc.want == "":
				return
			case err == nil:
				t.Fatalf("accepted %+v, which it must refuse", tc.policy)
			case !strings.Contains(err.Error(), tc.want):
				t.Errorf("refusal does not say why: %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestTombstoneStillRendersAsAnEventSomebodyCanSee defends the reason the
// tombstone is not an empty object.
//
// server/web/transcript.go drops an assistant turn carrying neither text nor
// usage. An emptied body would therefore remove that event from the rendered
// transcript altogether — a page that looks complete, in seq order, with rows
// missing from the middle and nothing to say so. This is the assertion that
// fails if somebody later shortens the tombstone to "{}".
func TestTombstoneStillRendersAsAnEventSomebodyCanSee(t *testing.T) {
	var e event.Event
	if err := json.Unmarshal([]byte(retentionTombstone), &e); err != nil {
		t.Fatalf("the tombstone is not decodable as an event, so every expired row would fail its page: %v", err)
	}
	if strings.TrimSpace(e.Text) == "" {
		t.Fatal("the tombstone carries no text, so an expired assistant turn is dropped from the transcript instead of shown as removed")
	}
	if !strings.Contains(strings.ToLower(e.Text), "retention") {
		t.Errorf("the tombstone does not say what happened to the body: %q", e.Text)
	}
}

// ---------------------------------------------------------------------------
// What a sweep issues, and what it refuses to issue
// ---------------------------------------------------------------------------

// TestADisabledPolicySweepsNothingAndAsksTheDatabaseNothing. Keeping everything
// is a legitimate choice, and a deployment that made it must not pay for a
// connection, a transaction and a lock attempt four times a day to be told there
// is nothing to do.
func TestADisabledPolicySweepsNothingAndAsksTheDatabaseNothing(t *testing.T) {
	db := &fakeDB{}
	sweep, err := retStore(db).SweepRetention(context.Background(), RetentionPolicy{}, retNow)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if got := retSQL(db); len(got) != 0 {
		t.Errorf("a disabled policy issued %d statements: %v", len(got), got)
	}
	if db.begun != 0 {
		t.Errorf("a disabled policy opened %d transactions", db.begun)
	}
	if sweep.BodiesExpired+sweep.EventsDeleted+sweep.SessionsDeleted != 0 {
		t.Errorf("a disabled policy reported removals: %+v", sweep)
	}
}

// TestAWindowOfZeroMeansForeverAndNeverACutoffOfNow is the most load-bearing
// assertion in this package, and it exists because of what the alternative
// costs.
//
// Zero is the shipped default: this deployment has chosen to keep everything.
// Every window becomes a cutoff by subtracting it from the clock, so a zero
// window that reached that arithmetic would produce a cutoff of now — and a
// cutoff of now selects the entire corpus. The gap between "keeps everything
// forever" and "deletes everything on the first boot" is the predicate that
// treats zero as off, checked before any subtraction happens.
//
// Each case therefore asserts the absence of a statement rather than the
// presence of one, in both halves independently, because a future edit could
// break either.
func TestAWindowOfZeroMeansForeverAndNeverACutoffOfNow(t *testing.T) {
	cases := []struct {
		name       string
		policy     RetentionPolicy
		wantBodies bool
		wantDelete bool
	}{
		{
			name:   "both windows off touches nothing at all",
			policy: RetentionPolicy{BatchSize: 4},
		},
		{
			name:       "a body window of zero leaves bodies alone while sessions are still swept",
			policy:     RetentionPolicy{SessionAfter: retYear, BatchSize: 4},
			wantDelete: true,
		},
		{
			name:       "a session window of zero deletes nothing while bodies are still expired",
			policy:     RetentionPolicy{BodyAfter: 90 * retDay, BatchSize: 4},
			wantBodies: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &fakeDB{stubs: []*stub{
				retLockGranted(),
				{match: "UPDATE events", affected: 0},
				{match: "FROM sessions", rows: [][]any{{"s-1"}}, once: true},
				{match: "DELETE FROM events", affected: 0},
				{match: "DELETE FROM usage_ledger", affected: 0},
				{match: "DELETE FROM sessions", affected: 1},
			}}

			if _, err := retStore(db).SweepRetention(context.Background(), tc.policy, retNow); err != nil {
				t.Fatalf("SweepRetention: %v", err)
			}
			sqls := retSQL(db)
			if got := retFind(sqls, "UPDATE events") >= 0; got != tc.wantBodies {
				t.Errorf("expired bodies = %v, want %v", got, tc.wantBodies)
			}
			if got := retFind(sqls, "DELETE FROM sessions") >= 0; got != tc.wantDelete {
				t.Errorf("deleted sessions = %v, want %v", got, tc.wantDelete)
			}

			// And no statement was handed a cutoff at or after the clock, which
			// is exactly what a zero window would produce and what would select
			// the entire corpus.
			//
			// The first argument only. Every statement that carries a cutoff
			// carries it there, and later arguments legitimately hold the clock
			// itself — the expiry stamp written into body_expired_at is "now" by
			// definition and says nothing about which rows were chosen.
			db.mu.Lock()
			defer db.mu.Unlock()
			for _, c := range db.calls {
				if len(c.args) == 0 {
					continue
				}
				if ts, ok := c.args[0].(time.Time); ok && !ts.Before(retNow) {
					t.Errorf("statement %q selects on %v, which is not in the past and therefore selects everything",
						strings.TrimSpace(c.sql), ts)
				}
			}
		})
	}
}

// TestAPolicyThatKeepsEverythingIsPricedWithoutBeingApplied. With the sweep
// switched off by default, the preview is what keeps the standing decision
// legible: it is asked about windows nobody has applied, and must still read
// nothing but counts.
func TestAPolicyThatKeepsEverythingIsPricedWithoutBeingApplied(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "FROM events", rows: [][]any{{int64(7)}}},
		{match: "FROM sessions", rows: [][]any{{int64(1)}}},
	}}
	s := retStore(db)

	// Nothing is due under the policy in force, and nothing is asked.
	due, err := s.RetentionDueNow(context.Background(), RetentionPolicy{}, retNow)
	if err != nil {
		t.Fatalf("RetentionDueNow: %v", err)
	}
	if due.Bodies != 0 || due.Sessions != 0 || !due.BodyCutoff.IsZero() || !due.SessionCutoff.IsZero() {
		t.Errorf("a policy that keeps everything reported %+v", due)
	}
	if got := retSQL(db); len(got) != 0 {
		t.Errorf("pricing a disabled policy issued %d statements: %v", len(got), got)
	}

	// The same store, asked what a policy it is not running would remove.
	priced, err := s.RetentionDueNow(context.Background(),
		RetentionPolicy{BodyAfter: 90 * retDay, SessionAfter: retYear}, retNow)
	if err != nil {
		t.Fatalf("RetentionDueNow: %v", err)
	}
	if priced.Bodies != 7 || priced.Sessions != 1 {
		t.Errorf("priced %+v, want 7 bodies and 1 session", priced)
	}
	for _, s := range retSQL(db) {
		if strings.Contains(s, "UPDATE") || strings.Contains(s, "DELETE") {
			t.Errorf("pricing a policy applied it: %s", strings.TrimSpace(s))
		}
	}
}

// TestBodyExpiryIsWrittenSoThePartialIndexApplies. The predicate has to be
// spelled exactly as migration 0005 spells it, because the planner can only use
// the index if it can prove the query's predicate implies the index's. Written
// any other way this is a sequential scan of every event ever captured, on every
// pass, forever — which is the shape of work this whole file exists to avoid.
func TestBodyExpiryIsWrittenSoThePartialIndexApplies(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "UPDATE events", affected: 3},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay, BatchSize: 10}

	sweep, err := retStore(db).SweepRetention(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if sweep.BodiesExpired != 3 {
		t.Errorf("BodiesExpired = %d, want 3", sweep.BodiesExpired)
	}

	sqls := retSQL(db)
	i := retFind(sqls, "UPDATE events")
	if i < 0 {
		t.Fatalf("no body expiry statement was issued: %v", sqls)
	}
	for _, fragment := range []string{
		// The index predicate, verbatim.
		"body_expired_at IS NULL",
		// The cutoff, and the bound on the batch.
		"occurred_at < $1",
		"LIMIT $2",
		// Never waiting on a row the ingest path holds.
		"FOR UPDATE SKIP LOCKED",
	} {
		if !strings.Contains(sqls[i], fragment) {
			t.Errorf("the body expiry statement is missing %q:\n%s", fragment, sqls[i])
		}
	}

	// And it expires bodies rather than removing anything.
	for _, forbidden := range []string{"DELETE FROM events", "DELETE FROM sessions", "DELETE FROM messages"} {
		if retFind(sqls, forbidden) >= 0 {
			t.Errorf("expiring a body issued %q, which deletes rows the policy said to keep", forbidden)
		}
	}
}

// TestBodyExpiryCarriesTheCutoffTheClockImplies. The cutoff is the whole
// decision, and an off-by-a-window bug here deletes a quarter of the corpus with
// no other symptom.
func TestBodyExpiryCarriesTheCutoffTheClockImplies(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "UPDATE events", affected: 0},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay, BatchSize: 10}
	if _, err := retStore(db).SweepRetention(context.Background(), policy, retNow); err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	for _, c := range db.calls {
		if !strings.Contains(c.sql, "UPDATE events") {
			continue
		}
		if len(c.args) != 4 {
			t.Fatalf("body expiry takes %d arguments, want 4: %v", len(c.args), c.args)
		}
		want := retNow.Add(-90 * retDay)
		if got, ok := c.args[0].(time.Time); !ok || !got.Equal(want) {
			t.Errorf("cutoff = %v, want %v", c.args[0], want)
		}
		if got, ok := c.args[1].(int); !ok || got != 10 {
			t.Errorf("batch bound = %v, want 10", c.args[1])
		}
		if got, ok := c.args[2].(string); !ok || got != retentionTombstone {
			t.Errorf("replacement body = %v, want the tombstone", c.args[2])
		}
		if got, ok := c.args[3].(time.Time); !ok || !got.Equal(retNow) {
			t.Errorf("expiry stamp = %v, want the sweep's clock %v", c.args[3], retNow)
		}
		return
	}
	t.Fatal("no body expiry statement was issued")
}

// TestEveryBatchTakesTheLockBeforeItWritesAnything. Two Cloud Run instances
// sweep on their own schedules, and the lock is the only thing standing between
// them and two transactions deleting the same session's events at once.
func TestEveryBatchTakesTheLockBeforeItWritesAnything(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "UPDATE events", affected: 0},
		{match: "FROM sessions", rows: [][]any{{"s-1"}}, once: true},
		{match: "DELETE FROM events", affected: 0},
		{match: "DELETE FROM usage_ledger", affected: 0},
		{match: "DELETE FROM sessions", affected: 1},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay, SessionAfter: retYear, BatchSize: 10}
	if _, err := retStore(db).SweepRetention(context.Background(), policy, retNow); err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	locked := false
	for _, c := range db.calls {
		switch {
		case strings.Contains(c.sql, "pg_try_advisory_xact_lock"):
			if !c.inTx {
				t.Error("the lock was taken outside a transaction, so it is released before the write it protects")
			}
			locked = true
		case strings.Contains(c.sql, "UPDATE events"), strings.Contains(c.sql, "DELETE FROM"):
			if !locked {
				t.Errorf("a write ran before any lock was taken: %s", strings.TrimSpace(c.sql))
			}
			if !c.inTx {
				t.Errorf("a write ran outside a transaction: %s", strings.TrimSpace(c.sql))
			}
		}
	}
	if !locked {
		t.Fatal("no batch took the advisory lock")
	}
	if db.begun < 2 {
		t.Errorf("the pass ran in %d transactions; each batch must commit on its own so autovacuum can reclaim what it freed", db.begun)
	}
}

// TestALockHeldElsewhereStandsThePassDownWithoutWriting. The second instance has
// nothing useful to do with the time it would spend blocked, and a sweeper that
// queues behind another sweeper holds a pooled connection the request path is
// sized to use.
func TestALockHeldElsewhereStandsThePassDownWithoutWriting(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "pg_try_advisory_xact_lock", rows: [][]any{{false}}},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay, SessionAfter: retYear, BatchSize: 10}

	sweep, err := retStore(db).SweepRetention(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("a held lock is not an error: %v", err)
	}
	if !sweep.Deferred {
		t.Error("the sweep did not report that it stood down")
	}
	sqls := retSQL(db)
	if retCount(sqls, "pg_try_advisory_xact_lock") != 1 {
		t.Errorf("it retried the lock %d times; a pass that cannot start should wait for its next tick",
			retCount(sqls, "pg_try_advisory_xact_lock"))
	}
	for _, s := range sqls {
		if strings.Contains(s, "UPDATE events") || strings.Contains(s, "DELETE FROM") {
			t.Errorf("it wrote without the lock: %s", strings.TrimSpace(s))
		}
	}
	if db.committed != 0 {
		t.Errorf("it committed %d transactions without the lock", db.committed)
	}
	if db.rolled != db.begun {
		t.Errorf("%d of %d transactions were left unfinished, which leaks a pooled connection", db.begun-db.rolled, db.begun)
	}
}

// TestAFullBatchContinuesAndAShortOneEndsThePass. This is what makes a pass
// bounded work rather than a scan: it keeps going only while there is evidence
// that more remains.
func TestAFullBatchContinuesAndAShortOneEndsThePass(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "UPDATE events", affected: 4, once: true},
		{match: "UPDATE events", affected: 4, once: true},
		{match: "UPDATE events", affected: 1, once: true},
		{match: "UPDATE events", affected: 4},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay, BatchSize: 4}

	sweep, err := retStore(db).SweepRetention(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if got := retCount(retSQL(db), "UPDATE events"); got != 3 {
		t.Errorf("ran %d batches, want 3: it must stop at the first batch that comes back short", got)
	}
	if sweep.BodiesExpired != 9 {
		t.Errorf("BodiesExpired = %d, want 9", sweep.BodiesExpired)
	}
	if sweep.Incomplete {
		t.Error("a pass that exhausted its work reported that work remained")
	}
}

// TestAPassStopsOnItsBudgetAndSaysWorkRemains. The first pass after this ships
// has months of accumulated data behind it. Stopping is the design; reporting
// that it stopped early is what keeps that from looking like a finished sweep.
func TestAPassStopsOnItsBudgetAndSaysWorkRemains(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "UPDATE events", affected: 4},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay, SessionAfter: retYear, BatchSize: 4, Budget: time.Nanosecond}

	sweep, err := retStore(db).SweepRetention(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if !sweep.Incomplete {
		t.Error("a pass that ran out of budget did not say so")
	}
	// Bounded rather than pinned at zero. A one-nanosecond budget races the
	// clock: time.Since can read 0 on the first check and let a single batch
	// through before the deadline is observed, which made this fail on a fast
	// machine and pass under -race, where the extra instrumentation is enough
	// to lose the race reliably. A test whose result depends on how quickly the
	// machine runs it teaches people to re-run rather than to read it.
	//
	// The property is that the pass STOPPED EARLY, not that it did nothing:
	// stopping before the first batch and stopping after it are both correct
	// answers to an exhausted budget, and the one that matters is that it did
	// not carry on to completion while reporting itself finished.
	if got := retSQL(db); len(got) > 2 {
		t.Errorf("an exhausted budget issued %d statements rather than stopping early: %v", len(got), got)
	}
}

// TestACancelledContextEndsThePassWithoutAnError. Cloud Run's SIGTERM lands
// mid-sweep on every deploy. The batch in flight rolls back, nothing is
// half-removed, and the next instance continues from the same cutoff — so this
// is the drain working, and an error here would page somebody for it.
func TestACancelledContextEndsThePassWithoutAnError(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "UPDATE events", affected: 4},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	policy := RetentionPolicy{BodyAfter: 90 * retDay, SessionAfter: retYear, BatchSize: 4}
	sweep, err := retStore(db).SweepRetention(ctx, policy, retNow)
	if err != nil {
		t.Fatalf("a cancelled sweep reported an error: %v", err)
	}
	if !sweep.Incomplete {
		t.Error("a cancelled pass did not report that work remained")
	}
	if got := retSQL(db); len(got) != 0 {
		t.Errorf("a cancelled pass issued %d statements: %v", len(got), got)
	}
}

// ---------------------------------------------------------------------------
// Deleting a session
// ---------------------------------------------------------------------------

// TestASessionIsDeletedInTheOrderItsForeignKeysRequire, and the audit trail
// survives it. Events first, taking the extracted messages by cascade; the usage
// ledger, which has no foreign key and would otherwise be orphaned; the session
// row last, taking its shares. access_log is never named: it records who read
// whose transcript, carries none of its content, and an audit trail deleted on a
// schedule by the thing it audits is not one.
func TestASessionIsDeletedInTheOrderItsForeignKeysRequire(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "FROM sessions", rows: [][]any{{"s-old"}}, once: true},
		{match: "DELETE FROM events", affected: 2},
		{match: "DELETE FROM usage_ledger", affected: 7},
		{match: "DELETE FROM sessions", affected: 1},
	}}
	policy := RetentionPolicy{SessionAfter: retYear, BatchSize: 10}

	sweep, err := retStore(db).SweepRetention(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if sweep.EventsDeleted != 2 || sweep.LedgerRowsDeleted != 7 || sweep.SessionsDeleted != 1 {
		t.Errorf("counts = %+v, want 2 events, 7 ledger rows, 1 session", sweep)
	}

	sqls := retSQL(db)
	events := retFind(sqls, "DELETE FROM events")
	ledger := retFind(sqls, "DELETE FROM usage_ledger")
	session := retFind(sqls, "DELETE FROM sessions")
	switch {
	case events < 0 || ledger < 0 || session < 0:
		t.Fatalf("a session deletion is missing a step: %v", sqls)
	case !(events < ledger && ledger < session):
		t.Errorf("order was events=%d ledger=%d session=%d; the rollup must go last or its events are stranded with nothing to find them",
			events, ledger, session)
	}
	if retFind(sqls, "access_log") >= 0 {
		t.Error("the sweep deleted access_log rows; the record of who read a transcript outlives the transcript")
	}
	if retFind(sqls, "DELETE FROM messages") >= 0 {
		t.Error("the sweep deleted messages explicitly; they go by cascade from events, and a second path is a second thing to keep correct")
	}
}

// TestASessionBiggerThanOneBatchKeepsItsRowForTheNextBatch. Sessions reach
// hundreds of thousands of events. Deleting one whole is the unbounded DELETE
// this design exists to avoid, and the session row is what the next batch finds
// it by — removing it first would strand every event still under it, because
// events carry no foreign key and nothing would ever look for them again.
func TestASessionBiggerThanOneBatchKeepsItsRowForTheNextBatch(t *testing.T) {
	// Three answers to the cutoff query and no more: the same session is found
	// again by each batch until it is empty, and once its row is gone the query
	// matches nothing — which is what ends the pass on a real server.
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "FROM sessions", rows: [][]any{{"s-huge"}}, once: true},
		{match: "FROM sessions", rows: [][]any{{"s-huge"}}, once: true},
		{match: "FROM sessions", rows: [][]any{{"s-huge"}}, once: true},
		{match: "DELETE FROM events", affected: 4, once: true},
		{match: "DELETE FROM events", affected: 4, once: true},
		{match: "DELETE FROM events", affected: 0},
		{match: "DELETE FROM usage_ledger", affected: 0},
		{match: "DELETE FROM sessions", affected: 1},
	}}
	policy := RetentionPolicy{SessionAfter: retYear, BatchSize: 4}

	sweep, err := retStore(db).SweepRetention(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}

	sqls := retSQL(db)
	if got := retCount(sqls, "DELETE FROM events"); got != 3 {
		t.Errorf("deleted events in %d batches, want 3", got)
	}
	// The first two batches came back full, so neither may have touched the
	// rollup; only the third, which came back short, finishes the session.
	if got := retCount(sqls, "DELETE FROM sessions"); got != 1 {
		t.Errorf("removed the session row %d times, want exactly once and only after its events were gone", got)
	}
	if sweep.EventsDeleted != 8 || sweep.SessionsDeleted != 1 {
		t.Errorf("counts = %+v, want 8 events and 1 session", sweep)
	}
	first := retFind(sqls, "DELETE FROM sessions")
	last := 0
	for i, s := range sqls {
		if strings.Contains(s, "DELETE FROM events") {
			last = i
		}
	}
	if first < last {
		t.Error("the session row was removed while it still had events")
	}
}

// TestNothingPastTheWindowEndsThePassQuietly. The steady state of a healthy
// deployment is a sweep that finds nothing, four times a day, forever. It must
// cost one indexed lookup and no error.
func TestNothingPastTheWindowEndsThePassQuietly(t *testing.T) {
	db := &fakeDB{stubs: []*stub{retLockGranted()}}
	policy := RetentionPolicy{SessionAfter: retYear, BatchSize: 10}

	sweep, err := retStore(db).SweepRetention(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if sweep.SessionsDeleted != 0 || sweep.EventsDeleted != 0 {
		t.Errorf("it removed something with nothing past the window: %+v", sweep)
	}
	if retFind(retSQL(db), "DELETE FROM") >= 0 {
		t.Error("it issued a DELETE with no session past the window")
	}
	// Ended, not necessarily committed: a batch that found nothing wrote
	// nothing, and rolling back is the cheaper way to release the connection.
	// What must never happen is a transaction left open, which holds a pooled
	// connection and, with it, the snapshot autovacuum is waiting on.
	if db.begun != db.committed+db.rolled {
		t.Errorf("begun=%d committed=%d rolled=%d; a transaction was left open", db.begun, db.committed, db.rolled)
	}
}

// TestAFailedStatementReportsWhatHadAlreadyBeenRemoved. A pass that expired
// forty thousand bodies and then failed is a different event from one that
// failed on its first statement, and only the counts tell them apart. Wrapped,
// never swallowed, and never reported as a clean sweep.
func TestAFailedStatementReportsWhatHadAlreadyBeenRemoved(t *testing.T) {
	boom := errors.New("connection reset")
	db := &fakeDB{stubs: []*stub{
		retLockGranted(),
		{match: "UPDATE events", affected: 4, once: true},
		{match: "UPDATE events", err: boom},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay, BatchSize: 4}

	sweep, err := retStore(db).SweepRetention(context.Background(), policy, retNow)
	if err == nil {
		t.Fatal("a failed statement was reported as a clean sweep")
	}
	if !errors.Is(err, boom) {
		t.Errorf("the cause was not wrapped: %v", err)
	}
	if !strings.Contains(err.Error(), "store:") {
		t.Errorf("the failure does not say which layer produced it: %v", err)
	}
	if sweep.BodiesExpired != 4 {
		t.Errorf("BodiesExpired = %d, want the 4 the committed batch had already removed", sweep.BodiesExpired)
	}
}

// ---------------------------------------------------------------------------
// The preview
// ---------------------------------------------------------------------------

// TestThePreviewOnlyReads is the property that makes it safe to run at boot, and
// safe for a human to run against production before deciding whether to let any
// of this happen at all.
func TestThePreviewOnlyReads(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "FROM events", rows: [][]any{{int64(12)}}},
		{match: "FROM sessions", rows: [][]any{{int64(3)}}},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay, SessionAfter: retYear}

	due, err := retStore(db).RetentionDueNow(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("RetentionDueNow: %v", err)
	}
	if due.Bodies != 12 || due.Sessions != 3 {
		t.Errorf("due = %+v, want 12 bodies and 3 sessions", due)
	}
	if due.Capped {
		t.Error("a count well under the cap was reported as capped")
	}
	if !due.BodyCutoff.Equal(retNow.Add(-90*retDay)) || !due.SessionCutoff.Equal(retNow.Add(-retYear)) {
		t.Errorf("cutoffs = %v / %v, want them derived from the policy and the clock", due.BodyCutoff, due.SessionCutoff)
	}
	if db.begun != 0 {
		t.Errorf("the preview opened %d transactions", db.begun)
	}
	for _, s := range retSQL(db) {
		if !strings.Contains(s, "SELECT") || strings.Contains(s, "DELETE") || strings.Contains(s, "UPDATE") {
			t.Errorf("the preview issued a statement that is not a read: %s", strings.TrimSpace(s))
		}
	}
}

// TestThePreviewCountsAgainstACapAndSaysWhenItHitIt. An exact count reads every
// candidate row, which is the scan the sweep is written to avoid; a figure
// reported as exact when it is a floor would be worse than no figure.
func TestThePreviewCountsAgainstACapAndSaysWhenItHitIt(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "FROM events", rows: [][]any{{int64(retentionPreviewCap)}}},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay}

	due, err := retStore(db).RetentionDueNow(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("RetentionDueNow: %v", err)
	}
	if !due.Capped {
		t.Error("a count that reached the cap was reported as a total")
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	for _, c := range db.calls {
		if !strings.Contains(c.sql, "LIMIT $2") {
			t.Errorf("the preview counted without a bound: %s", strings.TrimSpace(c.sql))
		}
		if len(c.args) == 2 {
			if got, ok := c.args[1].(int); !ok || got != retentionPreviewCap {
				t.Errorf("cap argument = %v, want %d", c.args[1], retentionPreviewCap)
			}
		}
	}
}

// TestThePreviewAsksNothingAboutAHalfThatIsSwitchedOff. A cutoff of "never" must
// not be counted as a cutoff of the zero time, which is a date in the year 1 and
// would report the entire corpus as due for deletion.
func TestThePreviewAsksNothingAboutAHalfThatIsSwitchedOff(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "FROM events", rows: [][]any{{int64(5)}}},
	}}
	policy := RetentionPolicy{BodyAfter: 90 * retDay}

	due, err := retStore(db).RetentionDueNow(context.Background(), policy, retNow)
	if err != nil {
		t.Fatalf("RetentionDueNow: %v", err)
	}
	if due.Sessions != 0 || !due.SessionCutoff.IsZero() {
		t.Errorf("a disabled session window produced %d sessions due before %v", due.Sessions, due.SessionCutoff)
	}
	if retFind(retSQL(db), "FROM sessions") >= 0 {
		t.Error("the preview counted sessions for a policy that never deletes one")
	}
}
