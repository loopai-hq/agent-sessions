//go:build integration

package slack

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/agent-sessions/server/store"
)

// These exercise the half of this package only Postgres can answer, and it is
// the half that matters most: whether the claim really is at-most-once, whether
// the eligibility predicates select what the design says they select, and
// whether migration 0006's constraints hold.
//
// The unit tests in this package drive the mirror's control flow over a fake
// connection source that models state rather than SQL. That fake deliberately
// does not reimplement ON CONFLICT, the anti-join or the lease predicate,
// because a fake that reimplemented them would be a test of the
// reimplementation. This file is where those claims are settled.
//
//	docker run -d -e POSTGRES_PASSWORD=test -e POSTGRES_DB=loop_sessions_test -p 55433:5432 postgres:16
//	LOOP_SESSIONS_TEST_DSN=postgres://postgres:test@127.0.0.1:55433/loop_sessions_test \
//	  go test -tags integration ./server/slack/...
//
// Each run works inside its own schema and drops it afterwards, so a shared
// development database is safe to point at. Postgres 15 is the floor: migration
// 0001 uses NULLS NOT DISTINCT.

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	dsn := os.Getenv("LOOP_SESSIONS_TEST_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "LOOP_SESSIONS_TEST_DSN is unset; skipping integration tests")
		os.Exit(0)
	}
	ctx := context.Background()
	schema := fmt.Sprintf("loop_sessions_slack_%d", time.Now().UnixNano())

	bootstrap, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	if _, err := bootstrap.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		fmt.Fprintf(os.Stderr, "create schema: %v\n", err)
		os.Exit(1)
	}
	bootstrap.Close()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse dsn: %v\n", err)
		os.Exit(1)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to schema: %v\n", err)
		os.Exit(1)
	}
	// The mirror's own schema arrives in 0006, and applying every migration is
	// also the assertion that 0006 is applicable at all.
	if err := store.New(pool, nil).Migrate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	pool.Close()
	if cleanup, err := pgxpool.New(ctx, dsn); err == nil {
		_, _ = cleanup.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		cleanup.Close()
	}
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func freshDB(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE slack_posts, slack_prefs, sessions CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM principals WHERE added_by IS DISTINCT FROM 'seed'`); err != nil {
		t.Fatalf("reset principals: %v", err)
	}
}

func addPerson(t *testing.T, email string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO principals (email, role, added_by) VALUES ($1, 'member', 'test')
		 ON CONFLICT (email) DO NOTHING`, email); err != nil {
		t.Fatalf("add principal: %v", err)
	}
}

// sessionRow is the shape a test varies. Zero values are filled with something
// substantial, so a test names only the field it is about.
type sessionRow struct {
	id        string
	email     string
	parent    string
	ended     bool
	endedAt   time.Time
	updatedAt time.Time
	turns     int
	tools     int
}

func addSession(t *testing.T, s sessionRow) {
	t.Helper()
	if s.turns == 0 && s.tools == 0 {
		s.turns, s.tools = 6, 40
	}
	if s.endedAt.IsZero() {
		s.endedAt = time.Now().Add(-2 * time.Hour)
	}
	if s.updatedAt.IsZero() {
		s.updatedAt = time.Now().Add(-time.Hour)
	}
	addPerson(t, s.email)

	var parent any
	if s.parent != "" {
		parent = s.parent
	}
	_, err := pool.Exec(context.Background(), `
		INSERT INTO sessions (session_id, email, source, parent_session_id, repo, git_branch,
		                      started_at, ended_at, ended, user_turns, tool_calls, updated_at, session_type)
		VALUES ($1, $2, 'claude-code', $3, 'loop-sessions', 'main', $4, $5, $6, $7, $8, $9, 'user')`,
		s.id, s.email, parent, s.endedAt.Add(-30*time.Minute), s.endedAt, s.ended,
		s.turns, s.tools, s.updatedAt)
	if err != nil {
		t.Fatalf("add session %s: %v", s.id, err)
	}
}

// wideOpen is a window that admits everything, so a test can narrow exactly one
// predicate and attribute the result to it.
func wideOpen() window {
	now := time.Now()
	return window{
		endedBefore: now.Add(time.Hour),
		quietBefore: now.Add(time.Hour),
		freshAfter:  now.Add(-365 * 24 * time.Hour),
		leaseBefore: now.Add(time.Hour),
		maxAttempts: 5,
		minTurns:    2,
		minTools:    10,
		limit:       100,
	}
}

func mustEligible(t *testing.T, w window) []candidate {
	t.Helper()
	got, err := eligible(context.Background(), pool, w)
	if err != nil {
		t.Fatalf("eligible: %v", err)
	}
	return got
}

func ids(cs []candidate) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.SessionID)
	}
	return out
}

// ---------------------------------------------------------------------------
// The claim
// ---------------------------------------------------------------------------

// TestExactlyOneCallerEverWinsAKey is the property the whole idempotency design
// rests on, and it is the one a fake cannot establish.
//
// chat.postMessage has no idempotency key, so a duplicate is a second message
// in somebody's channel that cannot be taken back. Two Cloud Run instances
// sweep at the same instant by construction — minScale is 1 and a rolling
// deploy briefly runs two — so this is the ordinary case, not the exotic one.
func TestExactlyOneCallerEverWinsAKey(t *testing.T) {
	freshDB(t)
	addPerson(t, "ana@example.org")
	addSession(t, sessionRow{id: "s-race", email: "ana@example.org"})

	const racers = 16
	var wg sync.WaitGroup
	won := make(chan bool, racers)
	start := make(chan struct{})
	now := time.Now()

	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := claimKey(context.Background(), pool,
				sessionKey("s-race"), "ana@example.org", "s-race", now, now.Add(-5*time.Minute), 5)
			if err != nil {
				t.Errorf("claimKey: %v", err)
				return
			}
			won <- ok
		}()
	}
	close(start)
	wg.Wait()
	close(won)

	winners := 0
	for ok := range won {
		if ok {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d of %d concurrent callers claimed the same key; every one of them would post a message", winners, racers)
	}
}

// TestASettledKeyIsNeverClaimedAgain. This is what makes a retry safe: once the
// message is out and recorded, no later pass on any instance may say it twice.
func TestASettledKeyIsNeverClaimedAgain(t *testing.T) {
	freshDB(t)
	addSession(t, sessionRow{id: "s-settled", email: "ana@example.org"})
	ctx := context.Background()
	now := time.Now()
	key := sessionKey("s-settled")

	if ok, err := claimKey(ctx, pool, key, "ana@example.org", "s-settled", now, now.Add(-5*time.Minute), 5); err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	if err := markPosted(ctx, pool, key, "C1", "1.0", now); err != nil {
		t.Fatalf("markPosted: %v", err)
	}

	// Even with the lease long expired, which is the condition that otherwise
	// permits a reclaim.
	ok, err := claimKey(ctx, pool, key, "ana@example.org", "s-settled", now.Add(time.Hour), now.Add(time.Hour), 5)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if ok {
		t.Fatal("a key that has already been posted was claimed again; the message would be sent twice")
	}
}

// TestAClaimAbandonedByACrashIsReclaimedAfterItsLease. The window this design
// trades a duplicate for a loss in: a process killed between claiming and
// posting must not strand the message forever.
func TestAClaimAbandonedByACrashIsReclaimedAfterItsLease(t *testing.T) {
	freshDB(t)
	addSession(t, sessionRow{id: "s-crash", email: "ana@example.org"})
	ctx := context.Background()
	key := sessionKey("s-crash")
	crashed := time.Now().Add(-30 * time.Minute)

	if ok, _ := claimKey(ctx, pool, key, "ana@example.org", "s-crash", crashed, crashed.Add(-5*time.Minute), 5); !ok {
		t.Fatal("first claim failed")
	}
	// Still inside its lease: another pass must leave it alone.
	if ok, _ := claimKey(ctx, pool, key, "ana@example.org", "s-crash", time.Now(), crashed.Add(-time.Minute), 5); ok {
		t.Fatal("a claim still inside its lease was taken by a second pass")
	}
	// Past it: recoverable.
	if ok, _ := claimKey(ctx, pool, key, "ana@example.org", "s-crash", time.Now(), time.Now().Add(-time.Minute), 5); !ok {
		t.Fatal("a claim whose lease expired was never reclaimed; the message is stranded forever")
	}
}

// TestAKeyThatSpentItsAttemptBudgetIsAbandoned, so a message Slack will never
// accept stops being retried every minute forever.
func TestAKeyThatSpentItsAttemptBudgetIsAbandoned(t *testing.T) {
	freshDB(t)
	addSession(t, sessionRow{id: "s-doomed", email: "ana@example.org"})
	ctx := context.Background()
	key := sessionKey("s-doomed")

	for i := range 3 {
		past := time.Now().Add(-time.Duration(10-i) * time.Minute)
		ok, err := claimKey(ctx, pool, key, "ana@example.org", "s-doomed", past, time.Now(), 3)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("claim %d was refused before the budget was spent", i)
		}
	}
	if ok, _ := claimKey(ctx, pool, key, "ana@example.org", "s-doomed", time.Now(), time.Now(), 3); ok {
		t.Fatal("a key past its attempt budget was claimed again")
	}
}

// TestAPermanentFailureSpendsTheBudgetAtOnce rather than waiting for three
// passes to discover the same answer, and the row stays behind carrying Slack's
// own word for why.
func TestAPermanentFailureSpendsTheBudgetAtOnce(t *testing.T) {
	freshDB(t)
	addSession(t, sessionRow{id: "s-perm", email: "ana@example.org"})
	ctx := context.Background()
	key := sessionKey("s-perm")
	now := time.Now()

	if ok, _ := claimKey(ctx, pool, key, "ana@example.org", "s-perm", now, now.Add(-time.Hour), 5); !ok {
		t.Fatal("claim failed")
	}
	cause := &APIError{Method: "chat.postMessage", Code: "channel_not_found"}
	if err := markFailed(ctx, pool, key, cause, true, 5); err != nil {
		t.Fatalf("markFailed: %v", err)
	}

	var attempts int
	var lastError string
	if err := pool.QueryRow(ctx,
		`SELECT attempts, coalesce(last_error, '') FROM slack_posts WHERE key = $1`, key,
	).Scan(&attempts, &lastError); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if attempts != 5 {
		t.Errorf("attempts = %d, want the whole budget spent", attempts)
	}
	if !strings.Contains(lastError, "channel_not_found") {
		t.Errorf("last_error = %q, want Slack's own word for it", lastError)
	}
	if ok, _ := claimKey(ctx, pool, key, "ana@example.org", "s-perm", now.Add(time.Hour), now.Add(time.Hour), 5); ok {
		t.Error("a permanently failed key was retried")
	}
}

// TestALongErrorIsClippedRatherThanRefused. A wrapped transport failure names
// hosts and can run long, and a write that failed on length would lose the
// reason the message was not sent.
func TestALongErrorIsClippedRatherThanRefused(t *testing.T) {
	freshDB(t)
	addSession(t, sessionRow{id: "s-long", email: "ana@example.org"})
	ctx := context.Background()
	key := sessionKey("s-long")
	now := time.Now()
	if ok, _ := claimKey(ctx, pool, key, "ana@example.org", "s-long", now, now.Add(-time.Hour), 5); !ok {
		t.Fatal("claim failed")
	}

	if err := markFailed(ctx, pool, key, fmt.Errorf("%s", strings.Repeat("x", 10_000)), false, 5); err != nil {
		t.Fatalf("markFailed: %v", err)
	}
	var lastError string
	if err := pool.QueryRow(ctx, `SELECT last_error FROM slack_posts WHERE key = $1`, key).Scan(&lastError); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if len(lastError) > 400 {
		t.Errorf("last_error is %d characters; a human reads this column in psql", len(lastError))
	}
}

// TestTheDailyCountIsOfMessagesReceivedNotClaimsMade. The ceiling exists to
// protect the people reading a channel, and a claimed-but-unsent row is not a
// message anybody received.
func TestTheDailyCountIsOfMessagesReceivedNotClaimsMade(t *testing.T) {
	freshDB(t)
	addSession(t, sessionRow{id: "s-a", email: "ana@example.org"})
	addSession(t, sessionRow{id: "s-b", email: "ana@example.org"})
	ctx := context.Background()
	now := time.Now()

	for _, id := range []string{"s-a", "s-b"} {
		if ok, _ := claimKey(ctx, pool, sessionKey(id), "ana@example.org", id, now, now.Add(-time.Hour), 5); !ok {
			t.Fatalf("claim %s failed", id)
		}
	}
	if err := markPosted(ctx, pool, sessionKey("s-a"), "C1", "1.0", now); err != nil {
		t.Fatalf("markPosted: %v", err)
	}

	n, err := postedSince(ctx, pool, "ana@example.org", now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("postedSince: %v", err)
	}
	if n != 1 {
		t.Errorf("counted %d messages, want only the 1 that was actually sent", n)
	}
}

// ---------------------------------------------------------------------------
// Eligibility
// ---------------------------------------------------------------------------

// TestOnlySomebodyWhoAskedForAMirrorGetsOne is the privacy default, enforced by
// the join rather than by a branch anybody could forget.
func TestOnlySomebodyWhoAskedForAMirrorGetsOne(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addSession(t, sessionRow{id: "s-optedin", email: "ana@example.org"})
	addSession(t, sessionRow{id: "s-neverheard", email: "bo@example.org"})
	addSession(t, sessionRow{id: "s-optedout", email: "cy@example.org"})

	long := time.Now().Add(-48 * time.Hour)
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeChannel, Channel: "C1"}, long); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}
	if err := savePrefs(ctx, pool, Prefs{Email: "cy@example.org", Mode: ModeOff}, long); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}

	got := ids(mustEligible(t, wideOpen()))
	if len(got) != 1 || got[0] != "s-optedin" {
		t.Fatalf("eligible = %v, want only the session of the one person who asked", got)
	}
}

// TestOptingInDoesNotPostYourHistory. Without the watermark, the day somebody
// enables this every session they have ever run is settled, unposted,
// substantial and theirs. That is the single most likely way this feature gets
// the bot muted, and it happens on the first person's first day.
func TestOptingInDoesNotPostYourHistory(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addSession(t, sessionRow{id: "s-old", email: "ana@example.org", updatedAt: time.Now().Add(-6 * time.Hour)})
	addSession(t, sessionRow{id: "s-new", email: "ana@example.org", updatedAt: time.Now().Add(-time.Minute)})

	// Enabled between the two.
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeChannel, Channel: "C1"},
		time.Now().Add(-3*time.Hour)); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}

	got := ids(mustEligible(t, wideOpen()))
	if len(got) != 1 || got[0] != "s-new" {
		t.Fatalf("eligible = %v, want only the session that settled after the mirror was switched on", got)
	}
}

// TestChangingDestinationRebasesTheWatermark. Somebody moving from a DM to a
// channel has just changed the audience, and the sessions that settled while
// they were deciding were summarised for the old one.
func TestChangingDestinationRebasesTheWatermark(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addPerson(t, "ana@example.org")

	first := time.Now().Add(-2 * time.Hour)
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeDM}, first); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}
	second := time.Now()
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeChannel, Channel: "C1"}, second); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}

	p, err := readPrefs(ctx, pool, "ana@example.org")
	if err != nil {
		t.Fatalf("readPrefs: %v", err)
	}
	if p.MirrorFrom.Before(second.Add(-time.Second)) {
		t.Errorf("mirror_from = %v, want it re-based to the moment the audience changed (%v)", p.MirrorFrom, second)
	}
	if p.Channel != "C1" || p.Mode != ModeChannel {
		t.Errorf("stored %+v, want the new destination", p)
	}
}

// TestTurningTheMirrorOffLeavesTheWatermarkAlone, because it is about to be
// overwritten by whatever is turned on next, and clearing a channel that is no
// longer in use keeps a preference from carrying a destination somebody forgot.
func TestTurningTheMirrorOffLeavesTheWatermarkAlone(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addPerson(t, "ana@example.org")

	on := time.Now().Add(-2 * time.Hour)
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeChannel, Channel: "C1"}, on); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}
	before, _ := readPrefs(ctx, pool, "ana@example.org")

	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeOff}, time.Now()); err != nil {
		t.Fatalf("savePrefs off: %v", err)
	}
	after, err := readPrefs(ctx, pool, "ana@example.org")
	if err != nil {
		t.Fatalf("readPrefs: %v", err)
	}
	if !after.MirrorFrom.Equal(before.MirrorFrom) {
		t.Errorf("mirror_from moved from %v to %v when the mirror was switched off", before.MirrorFrom, after.MirrorFrom)
	}
	if after.Channel != "" {
		t.Errorf("channel = %q after switching off, want it cleared", after.Channel)
	}
	if !after.Off() {
		t.Error("the preference does not read as off")
	}
}

// TestCachingASlackIdDoesNotMoveTheWatermark. Resolving an id is the mirror
// catching up on work it could not do earlier, not the person changing their
// mind; re-basing here would silently skip every session that settled while the
// lookup was failing.
func TestCachingASlackIdDoesNotMoveTheWatermark(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addPerson(t, "ana@example.org")
	enabled := time.Now().Add(-3 * time.Hour)
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeDM}, enabled); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}
	before, _ := readPrefs(ctx, pool, "ana@example.org")

	if err := cacheUserID(ctx, pool, "ana@example.org", "U12345"); err != nil {
		t.Fatalf("cacheUserID: %v", err)
	}
	after, err := readPrefs(ctx, pool, "ana@example.org")
	if err != nil {
		t.Fatalf("readPrefs: %v", err)
	}
	if after.SlackUserID != "U12345" {
		t.Errorf("slack_user_id = %q, want it cached", after.SlackUserID)
	}
	if !after.MirrorFrom.Equal(before.MirrorFrom) {
		t.Errorf("mirror_from moved from %v to %v when an id was cached", before.MirrorFrom, after.MirrorFrom)
	}
	if after.Mode != ModeDM {
		t.Errorf("mode = %q, want it untouched", after.Mode)
	}
}

// TestWhatIsAndIsNotDueAMessage walks the remaining predicates one at a time.
func TestWhatIsAndIsNotDueAMessage(t *testing.T) {
	tests := []struct {
		name    string
		row     sessionRow
		narrow  func(*window)
		wantDue bool
	}{
		{
			name:    "an ordinary finished session is due",
			row:     sessionRow{id: "s", email: "ana@example.org", ended: true},
			wantDue: true,
		},
		{
			name: "a subagent is part of the session that dispatched it, not a session of its own",
			row:  sessionRow{id: "s", email: "ana@example.org", parent: "s-parent", ended: true},
		},
		{
			name: "a one-turn pipeline call with no tools is not work anybody follows",
			row:  sessionRow{id: "s", email: "ana@example.org", ended: true, turns: 1, tools: 2},
		},
		{
			name:    "a long autonomous run is one turn and hundreds of tools, and is still work",
			row:     sessionRow{id: "s", email: "ana@example.org", ended: true, turns: 1, tools: 300},
			wantDue: true,
		},
		{
			name:    "a conversation is turns with few tools, and is also work",
			row:     sessionRow{id: "s", email: "ana@example.org", ended: true, turns: 8, tools: 1},
			wantDue: true,
		},
		{
			name: "a session that announced its end has still not settled",
			row:  sessionRow{id: "s", email: "ana@example.org", ended: true, updatedAt: time.Now()},
			narrow: func(w *window) {
				w.endedBefore = time.Now().Add(-5 * time.Minute)
			},
		},
		{
			name: "a session that merely went quiet waits far longer before it is summarised",
			row:  sessionRow{id: "s", email: "ana@example.org", ended: false, updatedAt: time.Now().Add(-10 * time.Minute)},
			narrow: func(w *window) {
				w.endedBefore = time.Now().Add(-5 * time.Minute)
				w.quietBefore = time.Now().Add(-45 * time.Minute)
			},
		},
		{
			name:    "the same session once it really has been quiet for long enough",
			row:     sessionRow{id: "s", email: "ana@example.org", ended: false, updatedAt: time.Now().Add(-2 * time.Hour)},
			wantDue: true,
			narrow: func(w *window) {
				w.endedBefore = time.Now().Add(-5 * time.Minute)
				w.quietBefore = time.Now().Add(-45 * time.Minute)
			},
		},
		{
			name: "a backfilled session that ended last week is not announced today",
			row: sessionRow{
				id: "s", email: "ana@example.org", ended: true,
				// Old by event time, and it only arrived a minute ago.
				endedAt: time.Now().Add(-7 * 24 * time.Hour), updatedAt: time.Now().Add(-time.Minute),
			},
			narrow: func(w *window) { w.freshAfter = time.Now().Add(-24 * time.Hour) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			freshDB(t)
			ctx := context.Background()
			if tc.row.parent != "" {
				addSession(t, sessionRow{id: tc.row.parent, email: tc.row.email, ended: true})
			}
			addSession(t, tc.row)
			if err := savePrefs(ctx, pool, Prefs{Email: tc.row.email, Mode: ModeChannel, Channel: "C1"},
				time.Now().Add(-72*time.Hour)); err != nil {
				t.Fatalf("savePrefs: %v", err)
			}

			w := wideOpen()
			if tc.narrow != nil {
				tc.narrow(&w)
			}
			got := ids(mustEligible(t, w))
			var due bool
			for _, id := range got {
				if id == tc.row.id {
					due = true
				}
			}
			if due != tc.wantDue {
				t.Errorf("session due = %v, want %v (eligible: %v)", due, tc.wantDue, got)
			}
		})
	}
}

// TestASessionAlreadyPostedIsNeverOfferedAgain. The anti-join in eligible and
// the WHERE clause in claimKey have to agree exactly: if this query offers a
// row the claim then refuses, the pass spins on it every minute.
func TestASessionAlreadyPostedIsNeverOfferedAgain(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addSession(t, sessionRow{id: "s-done", email: "ana@example.org", ended: true})
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeChannel, Channel: "C1"},
		time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}
	if got := ids(mustEligible(t, wideOpen())); len(got) != 1 {
		t.Fatalf("eligible = %v before posting, want the one session", got)
	}

	now := time.Now()
	if ok, _ := claimKey(ctx, pool, sessionKey("s-done"), "ana@example.org", "s-done", now, now.Add(-time.Hour), 5); !ok {
		t.Fatal("claim failed")
	}
	if err := markPosted(ctx, pool, sessionKey("s-done"), "C1", "1.0", now); err != nil {
		t.Fatalf("markPosted: %v", err)
	}

	if got := ids(mustEligible(t, wideOpen())); len(got) != 0 {
		t.Errorf("eligible = %v after posting, want none", got)
	}
}

// TestAFailedMessageIsOfferedAgainWhileItsBudgetLasts, which is the other half
// of the same agreement: a query that never re-offered a failed row would mean
// a transient Slack outage silently dropped every message it touched.
func TestAFailedMessageIsOfferedAgainWhileItsBudgetLasts(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addSession(t, sessionRow{id: "s-retry", email: "ana@example.org", ended: true})
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeChannel, Channel: "C1"},
		time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}

	claimedAt := time.Now().Add(-30 * time.Minute)
	if ok, _ := claimKey(ctx, pool, sessionKey("s-retry"), "ana@example.org", "s-retry", claimedAt, claimedAt.Add(-time.Hour), 5); !ok {
		t.Fatal("claim failed")
	}
	if err := markFailed(ctx, pool, sessionKey("s-retry"), fmt.Errorf("slack is unwell"), false, 5); err != nil {
		t.Fatalf("markFailed: %v", err)
	}

	// The lease has expired, so the row is due again.
	w := wideOpen()
	w.leaseBefore = time.Now().Add(-5 * time.Minute)
	if got := ids(mustEligible(t, w)); len(got) != 1 || got[0] != "s-retry" {
		t.Errorf("eligible = %v, want the failed message offered again", got)
	}

	// And once its budget is spent it is not.
	if _, err := pool.Exec(ctx, `UPDATE slack_posts SET attempts = 5 WHERE key = $1`, sessionKey("s-retry")); err != nil {
		t.Fatalf("spend the budget: %v", err)
	}
	if got := ids(mustEligible(t, w)); len(got) != 0 {
		t.Errorf("eligible = %v after the attempt budget was spent, want none", got)
	}
}

// TestABatchIsBoundedAndOldestFirst. A backlog drains over several passes
// rather than in one burst, which is what keeps a recovery from looking like
// the firehose this design exists to avoid.
func TestABatchIsBoundedAndOldestFirst(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	base := time.Now().Add(-10 * time.Hour)
	for i := range 8 {
		addSession(t, sessionRow{
			id: fmt.Sprintf("s-%02d", i), email: "ana@example.org", ended: true,
			updatedAt: base.Add(time.Duration(i) * time.Minute),
		})
	}
	if err := savePrefs(ctx, pool, Prefs{Email: "ana@example.org", Mode: ModeChannel, Channel: "C1"},
		time.Now().Add(-72*time.Hour)); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}

	w := wideOpen()
	w.limit = 3
	got := ids(mustEligible(t, w))
	if len(got) != 3 {
		t.Fatalf("got %d candidates, want the limit of 3", len(got))
	}
	want := []string{"s-00", "s-01", "s-02"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("candidate %d is %s, want %s: a backlog must drain oldest first", i, got[i], want[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Migration 0006's constraints
// ---------------------------------------------------------------------------

// TestThePreferenceTableRefusesWhatThePosterCouldNotAct.
//
// Enforced in the database as well as in the handler, because the handler is
// not the only thing that will ever write this table. A psql session during an
// incident is the other one, and that is exactly when nobody is checking.
func TestThePreferenceTableRefusesWhatThePosterCouldNotAct(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addPerson(t, "ana@example.org")

	tests := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "a mode nothing implements",
			sql:  `INSERT INTO slack_prefs (email, mode) VALUES ($1, 'carrier-pigeon')`,
			args: []any{"ana@example.org"},
		},
		{
			name: "channel mode with no channel, which could never post",
			sql:  `INSERT INTO slack_prefs (email, mode) VALUES ($1, 'channel')`,
			args: []any{"ana@example.org"},
		},
		{
			name: "channel mode with an empty channel",
			sql:  `INSERT INTO slack_prefs (email, mode, channel) VALUES ($1, 'channel', '')`,
			args: []any{"ana@example.org"},
		},
		{
			name: "a preference belonging to nobody on the roster",
			sql:  `INSERT INTO slack_prefs (email, mode) VALUES ($1, 'dm')`,
			args: []any{"stranger@example.com"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, tc.sql, tc.args...); err == nil {
				t.Fatal("the database accepted a preference it should have refused")
			}
		})
	}
}

// TestOffboardingSomebodyStopsTheirMirror. The join to sessions is the only
// thing between a departed colleague's late-arriving backfill and a channel, so
// the preference has to leave with the person.
func TestOffboardingSomebodyStopsTheirMirror(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addPerson(t, "leaver@example.org")
	if err := savePrefs(ctx, pool, Prefs{Email: "leaver@example.org", Mode: ModeChannel, Channel: "C1"}, time.Now()); err != nil {
		t.Fatalf("savePrefs: %v", err)
	}

	if _, err := pool.Exec(ctx, `DELETE FROM principals WHERE email = $1`, "leaver@example.org"); err != nil {
		t.Fatalf("offboard: %v", err)
	}
	p, err := readPrefs(ctx, pool, "leaver@example.org")
	if err != nil {
		t.Fatalf("readPrefs: %v", err)
	}
	if !p.Off() {
		t.Errorf("an offboarded colleague still has a live mirror: %+v", p)
	}
}

// TestRetentionCarriesTheOutboxOutWithTheSessionsItDescribes. The alternative
// is a table that outlives its subject forever and a mirror that gets slower
// every quarter.
func TestRetentionCarriesTheOutboxOutWithTheSessionsItDescribes(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addSession(t, sessionRow{id: "s-expire", email: "ana@example.org", ended: true})
	now := time.Now()
	if ok, _ := claimKey(ctx, pool, sessionKey("s-expire"), "ana@example.org", "s-expire", now, now.Add(-time.Hour), 5); !ok {
		t.Fatal("claim failed")
	}

	if _, err := pool.Exec(ctx, `DELETE FROM sessions WHERE session_id = $1`, "s-expire"); err != nil {
		t.Fatalf("delete session: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM slack_posts WHERE key = $1`, sessionKey("s-expire")).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("%d outbox rows survived the session they describe", n)
	}
}

// TestACapNoticeSurvivesTheSessionsItWasAbout. Its session_id is null, so it is
// not carried out by a session deletion, and a person's ceiling cannot be reset
// by retention removing their history mid-day.
func TestACapNoticeSurvivesTheSessionsItWasAbout(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addPerson(t, "ana@example.org")
	now := time.Now()
	key := capKey("ana@example.org", now)

	if ok, err := claimKey(ctx, pool, key, "ana@example.org", "", now, now.Add(-time.Hour), 5); err != nil || !ok {
		t.Fatalf("claim the notice: ok=%v err=%v", ok, err)
	}
	var sessionID *string
	if err := pool.QueryRow(ctx, `SELECT session_id FROM slack_posts WHERE key = $1`, key).Scan(&sessionID); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if sessionID != nil {
		t.Errorf("a cap notice carries session_id %q, want null", *sessionID)
	}
}

// TestTheCapKeyIsPerPersonPerDay, so tomorrow gets its own notice and two
// people never share one.
func TestTheCapKeyIsPerPersonPerDay(t *testing.T) {
	day := time.Date(2026, 8, 5, 23, 30, 0, 0, time.UTC)
	same := capKey("ana@example.org", day.Add(-time.Hour))
	if capKey("ana@example.org", day) != same {
		t.Error("two moments on the same day produced different keys")
	}
	if capKey("ana@example.org", day.Add(24*time.Hour)) == same {
		t.Error("the next day reuses today's key, so tomorrow's silence is never explained")
	}
	if capKey("bo@example.org", day) == same {
		t.Error("two people share one cap key")
	}
}
