//go:build integration

package store

// The half of retention only a real Postgres can answer.
//
// retention_test.go pins which statements are issued and when the loops stop,
// which needs no server. What needs one is everything the server decides:
// whether deleting an event really takes its indexed message with it, whether
// the partial index in migration 0005 is actually the plan the sweep gets or
// merely an index that exists, whether a second sweep finds anything left, and
// whether two sweeps running at once serialise. Every one of those is a claim
// this code makes about Postgres rather than about itself.
//
//	createdb loop_sessions_test
//	LOOP_SESSIONS_TEST_DSN=postgres:///loop_sessions_test go test -tags integration ./server/store/...

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

// retentionClock is the "now" every sweep in this file is given. Stating it
// makes each cutoff an arithmetic fact rather than a race with the wall clock.
var retentionClock = time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)

const (
	retentionDay  = 24 * time.Hour
	retentionYear = 365 * retentionDay
)

// retentionSeed writes one five-event session that occurred at when, including
// the assistant turn that produces a usage ledger row and the events that
// produce search rows.
//
// Every id is derived from the session id rather than taken from the shared
// session() fixture, whose ids are fixed strings. Seeding two sessions from that
// fixture writes the first one's events and then silently absorbs the second's
// as duplicates, because the event insert keys on the client's idempotency id —
// which leaves the second session with no events at all and every "the session
// inside the window was untouched" assertion passing vacuously. It cost a
// confusing failure here to notice, and the failure was the honest one.
func retentionSeed(t *testing.T, s *Store, email, sessionID string, when time.Time) {
	t.Helper()
	mustPrincipal(t, s, email, RoleMember)

	prompt := ingestOf(sessionID+"-prompt", sessionID, email, event.UserPrompt, 1)
	prompt.Event.Text = "ship the ingest service"
	prompt.Repo = "loop-sessions"

	turn := withUsage(ingestOf(sessionID+"-turn", sessionID, email, event.AssistantTurn, 2),
		"claude-opus-5", sessionID+"-msg", sessionID+"-req", 1000, 200)
	turn.Event.Text = "starting on the ingest service now"

	items := []Ingest{
		prompt,
		turn,
		ingestOf(sessionID+"-tool", sessionID, email, event.ToolCall, 3),
		ingestOf(sessionID+"-fail", sessionID, email, event.ToolFailed, 4),
		ingestOf(sessionID+"-end", sessionID, email, event.SessionEnded, 5),
	}
	for i := range items {
		items[i].Event.OccurredAt = when.Add(time.Duration(i) * time.Second)
	}

	res, err := s.UpsertEvents(context.Background(), items)
	if err != nil {
		t.Fatalf("seed %s: %v", sessionID, err)
	}
	if len(res.Rejected) != 0 {
		t.Fatalf("seed %s rejected %v", sessionID, res.Rejected)
	}
	if n := len(res.Accepted()); n != len(items) {
		t.Fatalf("seed %s stored %d of %d events; a fixture that collides with another session's ids proves nothing",
			sessionID, n, len(items))
	}
}

// retentionCount answers a scalar count, which is how every assertion below
// reads the database rather than trusting what the sweep reported about itself.
func retentionCount(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count (%s): %v", sql, err)
	}
	return n
}

// TestIntegrationExpiringABodyKeepsTheRowTheRollupTheCostAndTheSearchIndex is
// the property the first knob promises: an old transcript stops being readable
// in full, and nothing derived from it changes at all. If this fails, the cheap
// half of retention is not cheap — it is data loss with extra steps.
func TestIntegrationExpiringABodyKeepsTheRowTheRollupTheCostAndTheSearchIndex(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	ctx := context.Background()
	const owner = "keeper@example.com"

	old := retentionClock.Add(-100 * retentionDay)
	retentionSeed(t, s, owner, "sess-old", old)
	retentionSeed(t, s, owner, "sess-new", retentionClock.Add(-2*retentionDay))

	before := loadSession(t, s, "sess-old")
	messagesBefore := retentionCount(t, `SELECT count(*) FROM messages WHERE session_id = 'sess-old'`)
	ledgerBefore := retentionCount(t, `SELECT count(*) FROM usage_ledger WHERE session_id = 'sess-old'`)
	if messagesBefore == 0 || ledgerBefore == 0 {
		t.Fatalf("the fixture produced %d messages and %d ledger rows, so this test would prove nothing",
			messagesBefore, ledgerBefore)
	}

	sweep, err := s.SweepRetention(ctx, RetentionPolicy{BodyAfter: 90 * retentionDay, BatchSize: 3}, retentionClock)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if sweep.BodiesExpired != 5 {
		t.Errorf("expired %d bodies, want the 5 of the old session", sweep.BodiesExpired)
	}
	if sweep.EventsDeleted != 0 || sweep.SessionsDeleted != 0 {
		t.Errorf("a body-only policy deleted rows: %+v", sweep)
	}

	// Every event row survives, and only the old ones are marked.
	if n := retentionCount(t, `SELECT count(*) FROM events WHERE session_id = 'sess-old'`); n != 5 {
		t.Errorf("the old session has %d event rows, want 5: expiring a body must not remove the event", n)
	}
	if n := retentionCount(t,
		`SELECT count(*) FROM events WHERE session_id = 'sess-old' AND body_expired_at IS NOT NULL`); n != 5 {
		t.Errorf("%d of 5 old events are marked expired", n)
	}
	if n := retentionCount(t,
		`SELECT count(*) FROM events WHERE session_id = 'sess-new' AND body_expired_at IS NOT NULL`); n != 0 {
		t.Errorf("%d events inside the window were expired", n)
	}

	// The body is the tombstone, and it is still an event a page can render.
	var body []byte
	if err := pool.QueryRow(ctx,
		`SELECT body FROM events WHERE id = 'sess-old-turn'`).Scan(&body); err != nil {
		t.Fatalf("read expired body: %v", err)
	}
	var e event.Event
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("an expired body no longer decodes as an event, so its page fails: %v", err)
	}
	if strings.TrimSpace(e.Text) == "" {
		t.Error("an expired assistant turn carries no text, so the renderer drops it and the transcript silently loses a row")
	}
	if strings.Contains(string(body), "starting on the ingest service") {
		t.Error("the original body survived the sweep")
	}

	// Nothing derived from it moved.
	after := loadSession(t, s, "sess-old")
	if after.CostUSD != before.CostUSD || after.TokensInput != before.TokensInput || after.UserTurns != before.UserTurns {
		t.Errorf("the rollup changed: before %+v after %+v", before, after)
	}
	if n := retentionCount(t, `SELECT count(*) FROM messages WHERE session_id = 'sess-old'`); n != messagesBefore {
		t.Errorf("messages = %d, want the %d it had: the search index is what the body window deliberately keeps", n, messagesBefore)
	}
	if n := retentionCount(t, `SELECT count(*) FROM usage_ledger WHERE session_id = 'sess-old'`); n != ledgerBefore {
		t.Errorf("usage ledger = %d, want %d", n, ledgerBefore)
	}

	// And the text is still findable, which is the whole reason the search rows
	// are kept when the body is not.
	res, err := s.SearchMessages(ctx, Viewer{Email: owner, Role: RoleMember},
		SearchFilter{Query: "ingest", Limit: 10})
	if err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	found := false
	for _, h := range res.Hits {
		if h.SessionID == "sess-old" {
			found = true
		}
	}
	if !found {
		t.Error("an expired session is no longer searchable; expiring a body must not touch the search index")
	}
}

// TestIntegrationASecondSweepFindsNothingLeftToExpire is idempotence measured
// rather than asserted. The sweeper runs after every boot and four times a day
// forever, so a pass that re-expired what it had already expired would rewrite
// the whole table on every tick and bloat it faster than growth ever did.
func TestIntegrationASecondSweepFindsNothingLeftToExpire(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	ctx := context.Background()
	retentionSeed(t, s, "twice@example.com", "sess-twice", retentionClock.Add(-100*retentionDay))
	policy := RetentionPolicy{BodyAfter: 90 * retentionDay, BatchSize: 2}

	first, err := s.SweepRetention(ctx, policy, retentionClock)
	if err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	if first.BodiesExpired == 0 {
		t.Fatal("the first sweep expired nothing, so the second proves nothing")
	}

	var stamped time.Time
	if err := pool.QueryRow(ctx, `SELECT body_expired_at FROM events WHERE id = 'sess-twice-turn'`).Scan(&stamped); err != nil {
		t.Fatalf("read the expiry stamp: %v", err)
	}

	second, err := s.SweepRetention(ctx, policy, retentionClock.Add(time.Hour))
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if second.BodiesExpired != 0 {
		t.Errorf("the second sweep expired %d bodies that were already expired", second.BodiesExpired)
	}
	var restamped time.Time
	if err := pool.QueryRow(ctx, `SELECT body_expired_at FROM events WHERE id = 'sess-twice-turn'`).Scan(&restamped); err != nil {
		t.Fatalf("re-read the expiry stamp: %v", err)
	}
	if !restamped.Equal(stamped) {
		t.Errorf("the row was rewritten: stamp moved from %v to %v", stamped, restamped)
	}
}

// TestIntegrationDeletingASessionTakesItsDerivedRowsAndLeavesTheAuditTrail is
// the second knob, and the one that cannot be undone. Two halves matter: the
// cascades really do reach the message rows and the shares, and the record of
// who read the transcript survives the transcript. An audit trail deleted on a
// schedule by the thing it audits is not an audit trail.
func TestIntegrationDeletingASessionTakesItsDerivedRowsAndLeavesTheAuditTrail(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	ctx := context.Background()
	const owner = "goer@example.com"

	retentionSeed(t, s, owner, "sess-doomed", retentionClock.Add(-2*retentionYear))
	retentionSeed(t, s, owner, "sess-kept", retentionClock.Add(-10*retentionDay))

	if _, err := s.CreateShare(ctx, Viewer{Email: owner, Role: RoleMember},
		ShareRequest{SessionID: "sess-doomed", Grantee: "reader@example.com"}); err != nil {
		t.Fatalf("CreateShare: %v", err)
	}
	if err := s.RecordAccess(ctx, Access{
		Viewer: "reader@example.com", SessionID: "sess-doomed", Owner: owner, Via: AccessViaShare,
	}); err != nil {
		t.Fatalf("RecordAccess: %v", err)
	}

	sweep, err := s.SweepRetention(ctx, RetentionPolicy{SessionAfter: retentionYear, BatchSize: 2}, retentionClock)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if sweep.SessionsDeleted != 1 {
		t.Errorf("deleted %d sessions, want 1", sweep.SessionsDeleted)
	}
	if sweep.EventsDeleted != 5 {
		t.Errorf("deleted %d events, want the doomed session's 5", sweep.EventsDeleted)
	}
	if sweep.LedgerRowsDeleted == 0 {
		t.Error("the usage ledger has no foreign key to follow, so a sweep that leaves it behind orphans it forever")
	}

	for _, tc := range []struct {
		what string
		sql  string
	}{
		{"sessions", `SELECT count(*) FROM sessions WHERE session_id = 'sess-doomed'`},
		{"events", `SELECT count(*) FROM events WHERE session_id = 'sess-doomed'`},
		{"messages", `SELECT count(*) FROM messages WHERE session_id = 'sess-doomed'`},
		{"usage_ledger", `SELECT count(*) FROM usage_ledger WHERE session_id = 'sess-doomed'`},
		{"shares", `SELECT count(*) FROM shares WHERE session_id = 'sess-doomed'`},
	} {
		if n := retentionCount(t, tc.sql); n != 0 {
			t.Errorf("%s still holds %d rows of a deleted session", tc.what, n)
		}
	}
	if n := retentionCount(t, `SELECT count(*) FROM access_log WHERE session_id = 'sess-doomed'`); n != 1 {
		t.Errorf("access_log holds %d rows, want the 1 that records who read this: the audit outlives the transcript", n)
	}

	// The session inside the window is untouched, including its bodies: this
	// policy switched body expiry off entirely.
	if n := retentionCount(t, `SELECT count(*) FROM events WHERE session_id = 'sess-kept'`); n != 5 {
		t.Errorf("the kept session has %d events, want 5", n)
	}
	if n := retentionCount(t,
		`SELECT count(*) FROM events WHERE session_id = 'sess-kept' AND body_expired_at IS NOT NULL`); n != 0 {
		t.Errorf("%d bodies were expired by a policy that only deletes sessions", n)
	}
}

// TestIntegrationAPartlyDeletedSessionIsAResumableState. An interrupted sweep —
// a SIGTERM, a killed instance, a budget that ran out — leaves a session with
// some of its events gone. That has to be a state the next pass simply
// continues from, because it is the state every deploy will eventually produce.
func TestIntegrationAPartlyDeletedSessionIsAResumableState(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	ctx := context.Background()
	retentionSeed(t, s, "resume@example.com", "sess-partial", retentionClock.Add(-2*retentionYear))

	// One batch, deliberately smaller than the session, which is what an
	// interrupted pass leaves behind.
	b, err := s.deleteSessionBatch(ctx, retentionClock.Add(-retentionYear), 2)
	if err != nil {
		t.Fatalf("deleteSessionBatch: %v", err)
	}
	if b.events != 2 || b.sessions != 0 {
		t.Fatalf("batch removed %d events and %d sessions, want 2 and 0", b.events, b.sessions)
	}
	if n := retentionCount(t, `SELECT count(*) FROM sessions WHERE session_id = 'sess-partial'`); n != 1 {
		t.Fatal("the rollup was removed while its events remained; nothing would ever find them again")
	}
	if n := retentionCount(t, `SELECT count(*) FROM events WHERE session_id = 'sess-partial'`); n != 3 {
		t.Fatalf("%d events remain, want 3", n)
	}

	sweep, err := s.SweepRetention(ctx, RetentionPolicy{SessionAfter: retentionYear, BatchSize: 2}, retentionClock)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if sweep.EventsDeleted != 3 || sweep.SessionsDeleted != 1 {
		t.Errorf("the resumed pass removed %d events and %d sessions, want 3 and 1", sweep.EventsDeleted, sweep.SessionsDeleted)
	}
	if n := retentionCount(t, `SELECT count(*) FROM events WHERE session_id = 'sess-partial'`); n != 0 {
		t.Errorf("%d events survived the resumed pass", n)
	}
}

// TestIntegrationTheBodySweepPlanUsesThePartialIndex. The index exists either
// way; what this asserts is that the sweep's predicate is spelled so the planner
// can prove the index applies. Written any other way the statement is a
// sequential scan of every event ever captured, on every pass, and the failure
// is invisible until the table is large enough for it to matter.
//
// Sequential scans are disabled for the plan so that the assertion is about
// whether the index CAN serve the query rather than about which plan is cheaper
// on a table holding a handful of rows.
func TestIntegrationTheBodySweepPlanUsesThePartialIndex(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	ctx := context.Background()
	retentionSeed(t, s, "planner@example.com", "sess-plan", retentionClock.Add(-100*retentionDay))

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatalf("disable seqscan: %v", err)
	}

	rows, err := tx.Query(ctx, `
		EXPLAIN SELECT id FROM events
		WHERE body_expired_at IS NULL AND occurred_at < $1
		ORDER BY occurred_at
		LIMIT 2000`, retentionClock.Add(-90*retentionDay))
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}

	if !strings.Contains(plan.String(), "events_body_retention_idx") {
		t.Errorf("the sweep does not reach the partial index, so it scans the whole table:\n%s", plan.String())
	}
}

// TestIntegrationASweepStandsDownWhileAnotherHoldsTheLock is the mutual
// exclusion two Cloud Run instances rely on, measured against a real lock rather
// than a scripted one.
func TestIntegrationASweepStandsDownWhileAnotherHoldsTheLock(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	ctx := context.Background()
	retentionSeed(t, s, "locked@example.com", "sess-locked", retentionClock.Add(-100*retentionDay))

	// Another instance, mid-batch.
	other, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := other.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, retentionLockKey); err != nil {
		t.Fatalf("hold the lock: %v", err)
	}

	done := make(chan RetentionSweep, 1)
	go func() {
		sweep, err := s.SweepRetention(ctx, RetentionPolicy{BodyAfter: 90 * retentionDay, BatchSize: 2}, retentionClock)
		if err != nil {
			t.Errorf("SweepRetention: %v", err)
		}
		done <- sweep
	}()

	select {
	case sweep := <-done:
		if !sweep.Deferred {
			t.Error("the second sweep did not report standing down")
		}
		if sweep.BodiesExpired != 0 {
			t.Errorf("it expired %d bodies while another sweep held the lock", sweep.BodiesExpired)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second sweep blocked on the lock instead of standing down, holding a pooled connection for the length of somebody else's pass")
	}

	if n := retentionCount(t, `SELECT count(*) FROM events WHERE body_expired_at IS NOT NULL`); n != 0 {
		t.Errorf("%d bodies were expired without the lock", n)
	}
	_ = other.Rollback(ctx)
}

// TestIntegrationAWindowOfZeroLeavesAPopulatedDatabaseExactlyAsItWas is the
// shipped default, measured against real rows rather than against a count of
// statements.
//
// Both windows are zero because the owner chose to keep everything: the corpus
// is the asset, and paying for storage beats losing history. Zero reaches the
// same arithmetic every other window does — a cutoff is the clock minus the
// window — so a zero that was not caught before that subtraction would become a
// cutoff of now, and a cutoff of now selects every row in the database. This is
// the difference between the default being "keep everything" and the default
// being "delete everything on the first boot after the deploy".
//
// The oldest fixture here is two years old, which is past any window anybody
// would plausibly configure. Nothing may touch it.
func TestIntegrationAWindowOfZeroLeavesAPopulatedDatabaseExactlyAsItWas(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	ctx := context.Background()
	retentionSeed(t, s, "keepall@example.com", "sess-ancient", retentionClock.Add(-2*retentionYear))
	retentionSeed(t, s, "keepall@example.com", "sess-recent", retentionClock.Add(-retentionDay))

	counts := func() (events, sessions, messages, ledger, expired int64) {
		return retentionCount(t, `SELECT count(*) FROM events`),
			retentionCount(t, `SELECT count(*) FROM sessions`),
			retentionCount(t, `SELECT count(*) FROM messages`),
			retentionCount(t, `SELECT count(*) FROM usage_ledger`),
			retentionCount(t, `SELECT count(*) FROM events WHERE body_expired_at IS NOT NULL`)
	}
	e0, s0, m0, l0, x0 := counts()
	if e0 == 0 || s0 == 0 || m0 == 0 || l0 == 0 {
		t.Fatalf("the fixture is empty (%d events, %d sessions, %d messages, %d ledger rows), so this proves nothing",
			e0, s0, m0, l0)
	}
	if x0 != 0 {
		t.Fatalf("%d bodies were already expired before the sweep", x0)
	}

	cases := []struct {
		name   string
		policy RetentionPolicy
	}{
		{name: "the shipped default keeps everything forever", policy: RetentionPolicy{}},
		{name: "a zero session window keeps every session even with bodies expiring", policy: RetentionPolicy{SessionAfter: 0, BodyAfter: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sweep, err := s.SweepRetention(ctx, tc.policy, retentionClock)
			if err != nil {
				t.Fatalf("SweepRetention: %v", err)
			}
			if sweep.BodiesExpired != 0 || sweep.EventsDeleted != 0 || sweep.SessionsDeleted != 0 || sweep.LedgerRowsDeleted != 0 {
				t.Fatalf("a policy that keeps everything reported removals: %+v", sweep)
			}
			e1, s1, m1, l1, x1 := counts()
			if e1 != e0 || s1 != s0 || m1 != m0 || l1 != l0 || x1 != 0 {
				t.Fatalf("the database changed under a policy that deletes nothing: events %d->%d, sessions %d->%d, messages %d->%d, ledger %d->%d, expired bodies %d",
					e0, e1, s0, s1, m0, m1, l0, l1, x1)
			}
		})
	}

	// And the preview says the same thing: nothing due, and no cutoff at all
	// rather than a cutoff of now.
	due, err := s.RetentionDueNow(ctx, RetentionPolicy{}, retentionClock)
	if err != nil {
		t.Fatalf("RetentionDueNow: %v", err)
	}
	if due.Bodies != 0 || due.Sessions != 0 || !due.BodyCutoff.IsZero() || !due.SessionCutoff.IsZero() {
		t.Errorf("the preview reported %+v for a policy that keeps everything", due)
	}
}
