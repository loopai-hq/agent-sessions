//go:build integration

package store

// These exercise the half of this package that only Postgres can answer: that
// ON CONFLICT really absorbs a duplicate, that the rollup's merge expressions
// commute, that the generated tsvector and ts_headline behave, and that the
// authorization predicate returns nothing rather than something for a session
// the viewer may not see. They are behind a build tag so the default test run
// stays hermetic and can be run on a laptop with no database.
//
//	createdb loop_sessions_test
//	LOOP_SESSIONS_TEST_DSN=postgres:///loop_sessions_test go test -tags integration ./server/store/...
//
// Each run works inside its own schema and drops it afterwards, so a shared
// development database is safe to point at.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	dsn := os.Getenv("LOOP_SESSIONS_TEST_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "LOOP_SESSIONS_TEST_DSN is unset; skipping integration tests")
		os.Exit(0)
	}
	ctx := context.Background()
	schema := fmt.Sprintf("loop_sessions_test_%d", time.Now().UnixNano())

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
	if err := New(pool, nil).Migrate(ctx); err != nil {
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

// fresh empties everything a test could have written, leaving the seeded
// admins in place.
func fresh(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE messages, events, usage_ledger, shares, access_log,
		health_reports, sessions, device_tokens, devices, model_prices,
		source_tokens, skill_invocations, skill_rederive_queue, admin_actions,
		skill_catalog_entries, skill_catalog_aliases, skill_catalog_publishes, reconciler_runs CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM principals WHERE added_by IS DISTINCT FROM 'seed'`); err != nil {
		t.Fatalf("reset principals: %v", err)
	}
}

func newStore(t *testing.T, p Pricer) *Store {
	t.Helper()
	s := New(pool, p)
	fresh(t, s)
	return s
}

func mustPrincipal(t *testing.T, s *Store, email string, role Role) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO principals (email, role, added_by) VALUES ($1, $2, 'test')
		ON CONFLICT (email) DO UPDATE SET role = EXCLUDED.role`, email, string(role)); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
}

func loadSession(t *testing.T, s *Store, id string) Session {
	t.Helper()
	sess, err := authorizeSession(context.Background(), poolDB{pool: pool},
		Viewer{Email: "admin@example.com", Role: RoleAdmin}, id)
	if err != nil {
		t.Fatalf("load session %s: %v", id, err)
	}
	return sess
}

// bootstrappedAdmins counts the rows BootstrapAdmins wrote, which is what a
// second boot would duplicate if ON CONFLICT DO NOTHING ever stopped holding.
func bootstrappedAdmins(t *testing.T) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM principals WHERE role = 'admin' AND added_by = $1`, bootstrapAddedBy).Scan(&n); err != nil {
		t.Fatalf("count bootstrapped admins: %v", err)
	}
	return n
}

// TestIntegrationMigrationSeedsNobody: a fresh database's roster is empty
// until ADMIN_EMAILS is applied, and a second run of every migration file
// (the ledger cleared, so the SQL itself re-executes, which is what a fresh
// pod restarting actually needs) writes no row either.
func TestIntegrationMigrationSeedsNobody(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM principals`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("a fresh database holds %d principals; no migration may seed a person", n)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations`); err != nil {
		t.Fatalf("clear ledger: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM principals`).Scan(&n); err != nil || n != 0 {
		t.Errorf("principals after a second migration = %d, err = %v", n, err)
	}
}

// TestIntegrationBootstrapAdminsIsIdempotent: applying ADMIN_EMAILS twice,
// as two instances of a rolling deploy do, leaves one row and one trail row
// per address.
func TestIntegrationBootstrapAdminsIsIdempotent(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	emails := []string{"root@alpha.example", "Ops@Alpha.Example"}
	first, err := s.BootstrapAdmins(ctx, emails)
	if err != nil {
		t.Fatalf("first bootstrap: %v", err)
	}
	if len(first) != 2 || !first[0].Created || !first[1].Created || first[1].Email != "ops@alpha.example" {
		t.Errorf("first outcomes = %+v", first)
	}
	second, err := s.BootstrapAdmins(ctx, emails)
	if err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}
	for _, o := range second {
		if o.Created || o.Role != RoleAdmin || o.Disabled {
			t.Errorf("second outcome = %+v, want an existing active admin reported", o)
		}
	}
	if n := bootstrappedAdmins(t); n != 2 {
		t.Errorf("bootstrapped admins = %d, want 2", n)
	}
	for _, email := range []string{"root@alpha.example", "ops@alpha.example"} {
		p, err := s.Principal(ctx, email)
		if err != nil {
			t.Fatalf("Principal %s: %v", email, err)
		}
		if p.Role != RoleAdmin || p.DisabledAt != nil || p.AddedBy != bootstrapAddedBy {
			t.Errorf("%s = %+v", email, p)
		}
		if got := auditRows(t, email); len(got) != 1 || got[0].actor != email || got[0].fromRole != nil || got[0].toRole != "admin" {
			t.Errorf("%s trail = %+v, want one creation row", email, got)
		}
	}
}

// TestIntegrationBootstrapDoesNotOverwriteAnAdminPageEdit: a demotion or a
// disable made in the admin page survives every later boot that still
// names the address, and the boot reports the row it left alone.
func TestIntegrationBootstrapDoesNotOverwriteAnAdminPageEdit(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	emails := []string{"root@alpha.example", "second@alpha.example", "third@alpha.example"}
	if _, err := s.BootstrapAdmins(ctx, emails); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	root := Viewer{Email: "root@alpha.example", Role: RoleAdmin}
	member := RoleMember
	if _, err := s.PutPrincipal(ctx, root, PrincipalUpdate{Email: "second@alpha.example", Role: &member}); err != nil {
		t.Fatalf("demote: %v", err)
	}
	disabled := true
	if _, err := s.PutPrincipal(ctx, root, PrincipalUpdate{Email: "third@alpha.example", Disabled: &disabled}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	out, err := s.BootstrapAdmins(ctx, emails)
	if err != nil {
		t.Fatalf("second bootstrap: %v", err)
	}
	if len(out) != 3 || out[1].Created || out[1].Role != RoleMember || out[2].Created || !out[2].Disabled {
		t.Errorf("outcomes = %+v", out)
	}
	p, err := s.Principal(ctx, "second@alpha.example")
	if err != nil || p.Role != RoleMember {
		t.Errorf("a boot silently re-granted an admin role removed in the UI: %+v, %v", p, err)
	}
	p, err = s.Principal(ctx, "third@alpha.example")
	if err != nil || p.DisabledAt == nil || p.Role != RoleAdmin {
		t.Errorf("a boot silently re-enabled an admin disabled in the UI: %+v, %v", p, err)
	}
}

func session(t *testing.T, email, sessionID string) []Ingest {
	t.Helper()
	prompt := ingestOf("evt-prompt", sessionID, email, event.UserPrompt, 1)
	prompt.Event.Text = "ship the ingest service"
	prompt.Event.HarnessVersion = "2.0.1"
	prompt.Repo = "loop-sessions"

	turn := withUsage(ingestOf("evt-turn", sessionID, email, event.AssistantTurn, 2),
		"claude-opus-5", "msg-1", "req-1", 1000, 200)
	turn.Event.Text = "starting on the ingest service now"

	tool := ingestOf("evt-tool", sessionID, email, event.ToolCall, 3)
	fail := ingestOf("evt-fail", sessionID, email, event.ToolFailed, 4)
	fail.Event.Redactions = map[string]int{"api_key": 2}
	end := ingestOf("evt-end", sessionID, email, event.SessionEnded, 5)

	return []Ingest{prompt, turn, tool, fail, end}
}

func TestIntegrationIngestIsIdempotentAcrossDeliveries(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	ctx := context.Background()
	mustPrincipal(t, s, "me@example.com", RoleMember)
	batch := session(t, "me@example.com", "sess-1")

	first, err := s.UpsertEvents(ctx, batch)
	if err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if len(first.Inserted) != 5 {
		t.Fatalf("inserted %d events, want 5", len(first.Inserted))
	}
	before := loadSession(t, s, "sess-1")

	second, err := s.UpsertEvents(ctx, batch)
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if len(second.Inserted) != 0 {
		t.Errorf("redelivery inserted %v", second.Inserted)
	}
	if len(second.Accepted()) != 5 {
		t.Errorf("redelivery accepted %d, want all 5 so the client stops retrying",
			len(second.Accepted()))
	}
	after := loadSession(t, s, "sess-1")

	if before.UserTurns != after.UserTurns || before.ToolCalls != after.ToolCalls ||
		before.Errors != after.Errors || before.TokensInput != after.TokensInput ||
		before.CostUSD != after.CostUSD {
		t.Errorf("redelivery moved the rollup:\nbefore %+v\nafter  %+v", before, after)
	}
	if after.UserTurns != 1 || after.ToolCalls != 1 || after.Errors != 1 {
		t.Errorf("counters = %d/%d/%d, want 1/1/1", after.UserTurns, after.ToolCalls, after.Errors)
	}
	if !after.Ended {
		t.Errorf("the end marker did not land")
	}
	if after.FirstPrompt != "ship the ingest service" {
		t.Errorf("first prompt = %q", after.FirstPrompt)
	}
	if after.Redactions["api_key"] != 2 {
		t.Errorf("redactions = %v", after.Redactions)
	}
}

func TestIntegrationRollupMergesBatchesInAnyOrder(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "me@example.com", RoleMember)

	late := ingestOf("evt-late", "sess-2", "me@example.com", event.ToolCall, 9)
	late.Event.OccurredAt = at.Add(time.Hour)
	late.Event.HarnessVersion = "2.0.2"
	late.Event.Redactions = map[string]int{"token": 1}
	if _, err := s.UpsertEvents(ctx, []Ingest{late}); err != nil {
		t.Fatalf("late batch: %v", err)
	}

	early := ingestOf("evt-early", "sess-2", "me@example.com", event.UserPrompt, 1)
	early.Event.OccurredAt = at.Add(-time.Hour)
	early.Event.Text = "the real opening prompt"
	early.Event.HarnessVersion = "2.0.1"
	early.Event.Redactions = map[string]int{"token": 2}
	if _, err := s.UpsertEvents(ctx, []Ingest{early}); err != nil {
		t.Fatalf("backfill batch: %v", err)
	}

	got := loadSession(t, s, "sess-2")
	if !got.StartedAt.Equal(early.Event.OccurredAt) {
		t.Errorf("started_at = %v, want the backfilled earlier time %v",
			got.StartedAt, early.Event.OccurredAt)
	}
	if got.EndedAt == nil || !got.EndedAt.Equal(late.Event.OccurredAt) {
		t.Errorf("ended_at = %v, want the latest event time", got.EndedAt)
	}
	if got.FirstPrompt != "the real opening prompt" {
		t.Errorf("first prompt = %q, want the backfilled opening turn", got.FirstPrompt)
	}
	if len(got.HarnessVersions) != 2 {
		t.Errorf("harness versions = %v, want both merged", got.HarnessVersions)
	}
	if got.Redactions["token"] != 3 {
		t.Errorf("redaction tally = %v, want the two batches added", got.Redactions)
	}
}

func TestIntegrationUsageIsCountedOnceAcrossBatches(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.01})
	ctx := context.Background()
	mustPrincipal(t, s, "me@example.com", RoleMember)

	// Two genuinely different events reporting the same model call, delivered
	// in separate batches so event-level idempotency cannot help.
	first := withUsage(ingestOf("evt-a", "sess-3", "me@example.com", event.AssistantTurn, 1),
		"claude-opus-5", "msg-9", "req-9", 100, 50)
	second := withUsage(ingestOf("evt-b", "sess-3", "me@example.com", event.ToolResult, 2),
		"claude-opus-5", "msg-9", "req-9", 100, 50)

	if _, err := s.UpsertEvents(ctx, []Ingest{first}); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := s.UpsertEvents(ctx, []Ingest{second}); err != nil {
		t.Fatalf("second: %v", err)
	}

	got := loadSession(t, s, "sess-3")
	if got.TokensInput != 100 || got.TokensOutput != 50 {
		t.Errorf("tokens = %d/%d, want 100/50 counted once", got.TokensInput, got.TokensOutput)
	}
	if got.CostUSD != 1.5 {
		t.Errorf("cost = %v, want 1.5 counted once", got.CostUSD)
	}
}

func TestIntegrationSyntheticUsageNeverReachesATotal(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 1})
	ctx := context.Background()
	mustPrincipal(t, s, "me@example.com", RoleMember)

	in := withUsage(ingestOf("evt-syn", "sess-4", "me@example.com", event.AssistantTurn, 1),
		SyntheticModel, "msg-s", "req-s", 999, 999)
	if _, err := s.UpsertEvents(ctx, []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	got := loadSession(t, s, "sess-4")
	if got.TokensInput != 0 || got.CostUSD != 0 {
		t.Errorf("a <synthetic> record was billed: tokens=%d cost=%v", got.TokensInput, got.CostUSD)
	}
}

func TestIntegrationEventsForAnotherPrincipalsSessionAreRefused(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "victim@example.com", RoleMember)
	mustPrincipal(t, s, "attacker@example.com", RoleMember)

	if _, err := s.UpsertEvents(ctx, []Ingest{
		ingestOf("evt-v", "shared-id", "victim@example.com", event.UserPrompt, 1),
	}); err != nil {
		t.Fatalf("victim ingest: %v", err)
	}
	res, err := s.UpsertEvents(ctx, []Ingest{
		ingestOf("evt-a", "shared-id", "attacker@example.com", event.UserPrompt, 2),
	})
	if err != nil {
		t.Fatalf("attacker ingest: %v", err)
	}
	if len(res.Rejected) != 1 {
		t.Fatalf("rejected = %+v, want the attacker's event refused", res.Rejected)
	}
	page, err := s.GetEvents(ctx, Viewer{Email: "victim@example.com"}, "shared-id", EventRange{Limit: 50})
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(page.Events) != 1 {
		t.Errorf("victim's transcript has %d events, want only their own", len(page.Events))
	}
}

func TestIntegrationAuthorizationReturnsNothingRatherThanForbidden(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "owner@example.com", RoleMember)
	mustPrincipal(t, s, "nosy@example.com", RoleMember)
	if _, err := s.UpsertEvents(ctx, session(t, "owner@example.com", "sess-5")); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	nosy := Viewer{Email: "nosy@example.com", Role: RoleMember}
	if _, err := s.GetSession(ctx, nosy, "sess-5"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetSession = %v, want ErrNotFound", err)
	}
	if _, err := s.GetSession(ctx, nosy, "no-such-session"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetSession on an absent id = %v, want the same ErrNotFound", err)
	}
	page, err := s.ListSessions(ctx, nosy, SessionFilter{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(page.Sessions) != 0 {
		t.Errorf("a member saw %d sessions belonging to someone else", len(page.Sessions))
	}
}

func TestIntegrationAdminReadIsAudited(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "owner@example.com", RoleMember)
	if _, err := s.UpsertEvents(ctx, session(t, "owner@example.com", "sess-6")); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	admin := Viewer{Email: "admin@example.com", Role: RoleAdmin}
	if _, err := s.GetSession(ctx, admin, "sess-6"); err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if _, err := s.GetEvents(ctx, admin, "sess-6", EventRange{Limit: 10}); err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	log, err := s.ListAccessLog(ctx, admin, AccessLogFilter{SessionID: "sess-6", Limit: 10})
	if err != nil {
		t.Fatalf("ListAccessLog: %v", err)
	}
	if len(log) != 2 {
		t.Fatalf("audit rows = %d, want one per read", len(log))
	}
	for _, a := range log {
		if a.Via != AccessViaAdmin || a.Owner != "owner@example.com" || a.Viewer != admin.Email {
			t.Errorf("audit row = %+v", a)
		}
	}

	// The owner reading their own work leaves no trail.
	if _, err := s.GetSession(ctx, Viewer{Email: "owner@example.com"}, "sess-6"); err != nil {
		t.Fatalf("owner GetSession: %v", err)
	}
	log, _ = s.ListAccessLog(ctx, admin, AccessLogFilter{SessionID: "sess-6", Limit: 10})
	if len(log) != 2 {
		t.Errorf("audit rows = %d after an own read, want still 2", len(log))
	}
}

func TestIntegrationSharesGrantAndRevoke(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "owner@example.com", RoleMember)
	mustPrincipal(t, s, "guest@example.com", RoleMember)
	if _, err := s.UpsertEvents(ctx, session(t, "owner@example.com", "sess-7")); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	owner := Viewer{Email: "owner@example.com", Role: RoleMember}
	guest := Viewer{Email: "guest@example.com", Role: RoleMember}

	if _, err := s.CreateShare(ctx, guest, ShareRequest{SessionID: "sess-7"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("a stranger sharing someone else's session = %v, want ErrNotFound", err)
	}

	sh, err := s.CreateShare(ctx, owner, ShareRequest{SessionID: "sess-7", Grantee: "guest@example.com"})
	if err != nil {
		t.Fatalf("CreateShare: %v", err)
	}
	if _, err := s.GetSession(ctx, guest, "sess-7"); err != nil {
		t.Fatalf("the grantee cannot read the shared session: %v", err)
	}
	resolved, _, err := s.ResolveShare(ctx, guest, sh.Token)
	if err != nil {
		t.Fatalf("ResolveShare: %v", err)
	}
	if resolved.SessionID != "sess-7" {
		t.Errorf("resolved %q", resolved.SessionID)
	}

	if err := s.RevokeShare(ctx, owner, sh.ID); err != nil {
		t.Fatalf("RevokeShare: %v", err)
	}
	if _, err := s.GetSession(ctx, guest, "sess-7"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a revoked share still grants access: %v", err)
	}
	if _, _, err := s.ResolveShare(ctx, guest, sh.Token); !errors.Is(err, ErrNotFound) {
		t.Errorf("a revoked link still resolves: %v", err)
	}
}

func TestIntegrationExpiredShareDoesNotGrant(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "owner@example.com", RoleMember)
	mustPrincipal(t, s, "guest@example.com", RoleMember)
	if _, err := s.UpsertEvents(ctx, session(t, "owner@example.com", "sess-8")); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := s.CreateShare(ctx, Viewer{Email: "owner@example.com"}, ShareRequest{
		SessionID: "sess-8", ExpiresAt: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("CreateShare: %v", err)
	}
	if _, err := s.GetSession(ctx, Viewer{Email: "guest@example.com"}, "sess-8"); !errors.Is(err, ErrNotFound) {
		t.Errorf("an expired share still grants access: %v", err)
	}
}

func TestIntegrationSearchRanksAndHighlightsWithinScope(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "owner@example.com", RoleMember)
	mustPrincipal(t, s, "other@example.com", RoleMember)
	if _, err := s.UpsertEvents(ctx, session(t, "owner@example.com", "sess-9")); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	res, err := s.SearchMessages(ctx, Viewer{Email: "owner@example.com"},
		SearchFilter{Query: "ingest service"})
	if err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	if len(res.Hits) == 0 {
		t.Fatal("no hits for text that is definitely in the corpus")
	}
	for _, h := range res.Hits {
		if h.Rank <= 0 {
			t.Errorf("hit %s has rank %v", h.EventID, h.Rank)
		}
		if h.Snippet == "" {
			t.Errorf("hit %s has no highlighted snippet", h.EventID)
		}
	}

	blind, err := s.SearchMessages(ctx, Viewer{Email: "other@example.com"},
		SearchFilter{Query: "ingest service"})
	if err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	if len(blind.Hits) != 0 {
		t.Errorf("a member searched into a colleague's transcript: %d hits", len(blind.Hits))
	}
}

func TestIntegrationSearchAuditsAnAdminReadingSomeoneElse(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "owner@example.com", RoleMember)
	if _, err := s.UpsertEvents(ctx, session(t, "owner@example.com", "sess-10")); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	admin := Viewer{Email: "admin@example.com", Role: RoleAdmin}
	if _, err := s.SearchMessages(ctx, admin, SearchFilter{Query: "ingest"}); err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	log, err := s.ListAccessLog(ctx, admin, AccessLogFilter{SessionID: "sess-10", Limit: 10})
	if err != nil {
		t.Fatalf("ListAccessLog: %v", err)
	}
	if len(log) != 1 {
		t.Errorf("audit rows = %d, want one for the searched session", len(log))
	}
}

func TestIntegrationEventsArePagedInSequenceOrder(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "me@example.com", RoleMember)

	var batch []Ingest
	for i := int64(5); i >= 1; i-- { // delivered out of order on purpose
		batch = append(batch, ingestOf(fmt.Sprintf("evt-%d", i), "sess-11", "me@example.com", event.ToolCall, i))
	}
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	me := Viewer{Email: "me@example.com"}
	page, err := s.GetEvents(ctx, me, "sess-11", EventRange{Limit: 3})
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(page.Events) != 3 || !page.HasMore {
		t.Fatalf("page = %d events, more=%v", len(page.Events), page.HasMore)
	}
	for i, e := range page.Events {
		if e.Seq != int64(i+1) {
			t.Errorf("event %d has seq %d, want %d", i, e.Seq, i+1)
		}
	}
	rest, err := s.GetEvents(ctx, me, "sess-11", EventRange{AfterSeq: page.NextAfter, Limit: 10})
	if err != nil {
		t.Fatalf("GetEvents page 2: %v", err)
	}
	if len(rest.Events) != 2 || rest.Events[0].Seq != 4 {
		t.Errorf("second page = %+v", rest.Events)
	}
}

func TestIntegrationBackfilledEventsStartingAtSequenceZeroAreAllReturned(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "me@example.com", RoleMember)

	// The backfill walker numbers each session's events from zero.
	var batch []Ingest
	for i := int64(0); i < 4; i++ {
		in := ingestOf(fmt.Sprintf("bf-%d", i), "sess-bf", "me@example.com", event.ToolCall, i)
		in.Event.Origin = event.OriginTranscript
		batch = append(batch, in)
	}
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	page, err := s.GetEvents(ctx, Viewer{Email: "me@example.com"}, "sess-bf", EventRange{Limit: 10})
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	if len(page.Events) != 4 || page.Events[0].Seq != 0 {
		t.Errorf("returned %d events starting at seq %d, want 4 starting at 0",
			len(page.Events), page.Events[0].Seq)
	}
}

func TestIntegrationLastAdminCannotBeRemoved(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := Viewer{Email: "root@alpha.example", Role: RoleAdmin}
	member := RoleMember
	// The roster starts empty; the admins come from a bootstrap, as they do
	// on a fresh deployment.
	if _, err := s.BootstrapAdmins(ctx, []string{admin.Email, "second@alpha.example", "third@alpha.example"}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// Read from the table rather than listed here, so the test demotes
	// whoever is there and asserts that the last demotion is refused.
	rows, err := pool.Query(ctx, `
		SELECT email FROM principals
		WHERE role = 'admin' AND disabled_at IS NULL AND email <> $1`, admin.Email)
	if err != nil {
		t.Fatalf("list the other admins: %v", err)
	}
	var others []string
	for rows.Next() {
		var email string
		if err := rows.Scan(&email); err != nil {
			t.Fatalf("scan admin: %v", err)
		}
		others = append(others, email)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("list the other admins: %v", err)
	}
	for _, email := range others {
		if _, err := s.PutPrincipal(ctx, admin, PrincipalUpdate{Email: email, Role: &member}); err != nil {
			t.Fatalf("demote %s: %v", email, err)
		}
	}
	if _, err := s.PutPrincipal(ctx, admin,
		PrincipalUpdate{Email: admin.Email, Role: &member}); !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("demoting the last admin = %v, want ErrLastAdmin", err)
	}
	disabled := true
	if _, err := s.PutPrincipal(ctx, admin,
		PrincipalUpdate{Email: admin.Email, Disabled: &disabled}); !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("disabling the last admin = %v, want ErrLastAdmin", err)
	}
	p, err := s.Principal(ctx, admin.Email)
	if err != nil {
		t.Fatalf("Principal: %v", err)
	}
	if p.Role != RoleAdmin || p.DisabledAt != nil {
		t.Errorf("the last admin was changed anyway: %+v", p)
	}
}

// principalAudit is the newest trail row for a target, read straight from the
// table rather than from anything the store returns, because the question this
// answers is what an investigator would find a year later.
type principalAudit struct {
	actor        string
	target       string
	fromRole     *string
	toRole       string
	fromDisabled bool
	toDisabled   bool
}

func auditRows(t *testing.T, target string) []principalAudit {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT actor, target, from_role, to_role, from_disabled, to_disabled
		FROM principal_changes WHERE target = $1 ORDER BY id`, target)
	if err != nil {
		t.Fatalf("read audit trail: %v", err)
	}
	defer rows.Close()
	var out []principalAudit
	for rows.Next() {
		var a principalAudit
		if err := rows.Scan(&a.actor, &a.target, &a.fromRole, &a.toRole,
			&a.fromDisabled, &a.toDisabled); err != nil {
			t.Fatalf("scan audit row: %v", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read audit trail: %v", err)
	}
	return out
}

// The browser's admin form reaches PutPrincipal. Whatever it changes has to be
// answerable later, so every step here asserts the roster row and the trail row
// the same transaction should have written.
func TestIntegrationPutPrincipalRecordsEveryRosterEditInTheAuditTrail(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DELETE FROM principal_changes`); err != nil {
		t.Fatalf("clear audit trail: %v", err)
	}
	actor := Viewer{Email: "admin@example.org", Role: RoleAdmin}
	target := "newcomer@example.com"
	admin, member := RoleAdmin, RoleMember
	yes, no := true, false
	// The roster starts empty; the acting admin and a second one come from
	// a bootstrap, so demoting the target is never the last-admin case.
	if _, err := s.BootstrapAdmins(ctx, []string{actor.Email, "second@example.org"}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM principal_changes`); err != nil {
		t.Fatalf("clear audit trail: %v", err)
	}

	for _, step := range []struct {
		name        string
		update      PrincipalUpdate
		wantRole    Role
		wantOff     bool
		wantFrom    *string // nil is a creation, stored as NULL from_role
		wantTo      string
		wantWasOff  bool
		wantNowOff  bool
		wantAddedBy string
	}{
		{
			name:     "creation",
			update:   PrincipalUpdate{Email: target, Role: &member},
			wantRole: RoleMember, wantTo: "member",
			// added_by is written once, at creation, and never rewritten: it is the
			// only record of how somebody got onto the roster in the first place.
			wantAddedBy: actor.Email,
		},
		{
			name:     "promotion",
			update:   PrincipalUpdate{Email: target, Role: &admin},
			wantRole: RoleAdmin, wantTo: "admin",
			wantFrom: ptr("member"), wantAddedBy: actor.Email,
		},
		{
			name:     "disable",
			update:   PrincipalUpdate{Email: target, Disabled: &yes},
			wantRole: RoleAdmin, wantOff: true, wantTo: "admin",
			wantFrom: ptr("admin"), wantNowOff: true, wantAddedBy: actor.Email,
		},
		{
			name:     "re-enable and demote in one submission",
			update:   PrincipalUpdate{Email: target, Role: &member, Disabled: &no},
			wantRole: RoleMember, wantTo: "member",
			wantFrom: ptr("admin"), wantWasOff: true, wantAddedBy: actor.Email,
		},
	} {
		t.Run(step.name, func(t *testing.T) {
			saved, err := s.PutPrincipal(ctx, actor, step.update)
			if err != nil {
				t.Fatalf("PutPrincipal: %v", err)
			}
			if saved.Change == nil {
				t.Fatalf("reported no change for an edit that moved the row")
			}

			p, err := s.Principal(ctx, target)
			if err != nil {
				t.Fatalf("Principal: %v", err)
			}
			if p.Role != step.wantRole {
				t.Errorf("role = %q, want %q", p.Role, step.wantRole)
			}
			if (p.DisabledAt != nil) != step.wantOff {
				t.Errorf("disabled = %v, want %v", p.DisabledAt != nil, step.wantOff)
			}
			if p.AddedBy != step.wantAddedBy {
				t.Errorf("added_by = %q, want %q", p.AddedBy, step.wantAddedBy)
			}

			trail := auditRows(t, target)
			if len(trail) == 0 {
				t.Fatalf("the roster changed and the audit trail is empty")
			}
			got := trail[len(trail)-1]
			want := principalAudit{
				actor: actor.Email, target: target,
				fromRole: step.wantFrom, toRole: step.wantTo,
				fromDisabled: step.wantWasOff, toDisabled: step.wantNowOff,
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("audit row = %+v, want %+v", got, want)
			}
		})
	}

	if n := len(auditRows(t, target)); n != 4 {
		t.Errorf("audit trail has %d rows, want one per edit (4)", n)
	}
}

func ptr[T any](v T) *T { return &v }

// A submission that carries the values already on the row is not an edit. It
// must not rewrite the roster row, because a rewritten tuple with no trail row
// beside it is exactly what makes a lost write and a no-op look the same from
// the database afterwards.
func TestIntegrationPutPrincipalLeavesAnUnchangedRowAndItsTrailAlone(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DELETE FROM principal_changes`); err != nil {
		t.Fatalf("clear audit trail: %v", err)
	}
	actor := Viewer{Email: "alex@example.org", Role: RoleAdmin}
	target := "steady@example.com"
	member := RoleMember
	off := false

	if _, err := s.PutPrincipal(ctx, actor, PrincipalUpdate{Email: target, Role: &member}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// xmin names the transaction that last wrote the tuple, which is the only
	// way to tell a row that was rewritten with identical values from one that
	// was never touched.
	var before string
	if err := pool.QueryRow(ctx,
		`SELECT xmin::text FROM principals WHERE email = $1`, target).Scan(&before); err != nil {
		t.Fatalf("read xmin: %v", err)
	}

	saved, err := s.PutPrincipal(ctx, actor,
		PrincipalUpdate{Email: target, Role: &member, Disabled: &off})
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if saved.Change != nil {
		t.Errorf("reported a change of %+v for a resubmission that moved nothing", *saved.Change)
	}
	if saved.Principal.Role != RoleMember {
		t.Errorf("returned %+v, want the row as it already stood", saved.Principal)
	}

	var after string
	if err := pool.QueryRow(ctx,
		`SELECT xmin::text FROM principals WHERE email = $1`, target).Scan(&after); err != nil {
		t.Fatalf("read xmin: %v", err)
	}
	if after != before {
		t.Errorf("the row was rewritten (xmin %s -> %s) by a submission that changed nothing", before, after)
	}
	if n := len(auditRows(t, target)); n != 1 {
		t.Errorf("audit trail has %d rows, want only the creation", n)
	}
}

func TestIntegrationDeviceAuthenticationFollowsRevocation(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "me@example.com", RoleMember)
	hash := []byte("0123456789abcdef0123456789abcdef")

	dev, err := s.EnrollDevice(ctx, Device{Email: "me@example.com", Hostname: "laptop"}, hash, time.Time{})
	if err != nil {
		t.Fatalf("EnrollDevice: %v", err)
	}
	id, err := s.AuthenticateDevice(ctx, hash)
	if err != nil {
		t.Fatalf("AuthenticateDevice: %v", err)
	}
	if id.Email != "me@example.com" || id.Role != RoleMember {
		t.Errorf("identity = %+v", id)
	}
	if err := s.RevokeDevice(ctx, Viewer{Email: "me@example.com"}, dev.ID); err != nil {
		t.Fatalf("RevokeDevice: %v", err)
	}
	if _, err := s.AuthenticateDevice(ctx, hash); !errors.Is(err, ErrNotFound) {
		t.Errorf("a revoked device still authenticates: %v", err)
	}
}

func TestIntegrationDisabledPrincipalCannotEnroll(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "gone@example.com", RoleMember)
	if _, err := pool.Exec(ctx,
		`UPDATE principals SET disabled_at = now() WHERE email = 'gone@example.com'`); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := s.EnrollDevice(ctx, Device{Email: "gone@example.com"},
		[]byte("hash"), time.Time{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("a disabled principal enrolled a device: %v", err)
	}
}

func TestIntegrationHealthReportsAndFleetCoverage(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "reporting@example.com", RoleMember)
	mustPrincipal(t, s, "silent@example.com", RoleMember)

	live, err := s.EnrollDevice(ctx, Device{Email: "reporting@example.com"}, []byte("h1"), time.Time{})
	if err != nil {
		t.Fatalf("enroll reporting: %v", err)
	}
	if _, err := s.EnrollDevice(ctx, Device{Email: "silent@example.com"}, []byte("h2"), time.Time{}); err != nil {
		t.Fatalf("enroll silent: %v", err)
	}

	report := healthSample()
	report.EmittedAt = time.Now().Add(-time.Minute)
	for i := 0; i < 2; i++ { // delivered twice, as at-least-once delivery does
		if err := s.PutHealthReport(ctx, "reporting@example.com", live.ID, report); err != nil {
			t.Fatalf("PutHealthReport: %v", err)
		}
	}
	var stored int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM health_reports`).Scan(&stored); err != nil {
		t.Fatalf("count reports: %v", err)
	}
	if stored != 1 {
		t.Errorf("stored %d reports for one sample delivered twice", stored)
	}

	// The report's version replaces the enrollment-day one, and an older
	// replayed report must not roll it back: a machine that self-upgraded
	// yesterday and had a queued sample delivered today stays current.
	report2 := healthSample()
	report2.AgentVersion = "fresh01"
	report2.EmittedAt = time.Now().Add(-30 * time.Second)
	if err := s.PutHealthReport(ctx, "reporting@example.com", live.ID, report2); err != nil {
		t.Fatalf("PutHealthReport fresh: %v", err)
	}
	replay := healthSample()
	replay.AgentVersion = "old0001"
	replay.EmittedAt = time.Now().Add(-time.Hour)
	if err := s.PutHealthReport(ctx, "reporting@example.com", live.ID, replay); err != nil {
		t.Fatalf("PutHealthReport replay: %v", err)
	}
	var ver string
	if err := pool.QueryRow(ctx,
		`SELECT coalesce(agent_version,'') FROM devices WHERE id = $1::uuid`, live.ID).Scan(&ver); err != nil {
		t.Fatalf("read device version: %v", err)
	}
	if ver != "fresh01" {
		t.Errorf("device version = %q; the newest report's version must win and a replay must not roll it back", ver)
	}

	fleet, err := s.FleetCoverage(ctx, Viewer{Email: "admin@example.com", Role: RoleAdmin}, time.Hour)
	if err != nil {
		t.Fatalf("FleetCoverage: %v", err)
	}
	if fleet.Enrolled != 2 || fleet.Reporting != 1 || fleet.Silent != 1 {
		t.Errorf("coverage = enrolled %d / reporting %d / silent %d",
			fleet.Enrolled, fleet.Reporting, fleet.Silent)
	}
	for _, m := range fleet.Members {
		if m.Email == "reporting@example.com" && m.Worst != "critical" {
			t.Errorf("worst level = %q, want the report's derived level", m.Worst)
		}
	}
}

func TestIntegrationModelPricesAreEffectiveDated(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := Viewer{Email: "admin@example.com", Role: RoleAdmin}

	old := ModelPrice{Model: "claude-opus-5", EffectiveFrom: at.AddDate(0, -2, 0),
		InputPerMTok: 15, OutputPerMTok: 75, CacheReadMultiplier: 0.1,
		CacheWrite5mMultiplier: 1.25, CacheWrite1hMultiplier: 2}
	newer := old
	newer.EffectiveFrom = at.AddDate(0, -1, 0)
	newer.InputPerMTok = 12
	for _, p := range []ModelPrice{old, newer} {
		if err := s.PutModelPrice(ctx, admin, p); err != nil {
			t.Fatalf("PutModelPrice: %v", err)
		}
	}

	before, err := s.ModelPrices(ctx, at.AddDate(0, -2, 1))
	if err != nil {
		t.Fatalf("ModelPrices: %v", err)
	}
	if before["claude-opus-5"].InputPerMTok != 15 {
		t.Errorf("a later rate change rewrote what an earlier month cost: %+v", before)
	}
	after, err := s.ModelPrices(ctx, at)
	if err != nil {
		t.Fatalf("ModelPrices: %v", err)
	}
	if after["claude-opus-5"].InputPerMTok != 12 {
		t.Errorf("the current rate is not in force: %+v", after)
	}
}

func TestIntegrationListSessionsFiltersOnEventTime(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "me@example.com", RoleMember)

	oldOne := ingestOf("evt-old", "sess-old", "me@example.com", event.UserPrompt, 1)
	oldOne.Event.OccurredAt = at.AddDate(0, -6, 0)
	oldOne.Event.Origin = event.OriginTranscript
	recent := ingestOf("evt-new", "sess-new", "me@example.com", event.UserPrompt, 1)
	recent.Event.OccurredAt = at
	if _, err := s.UpsertEvents(ctx, []Ingest{oldOne, recent}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	me := Viewer{Email: "me@example.com"}
	page, err := s.ListSessions(ctx, me, SessionFilter{From: at.AddDate(0, -1, 0)})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(page.Sessions) != 1 || page.Sessions[0].SessionID != "sess-new" {
		t.Errorf("a date filter on event time returned %+v", page.Sessions)
	}
	// The backfilled session is historical by event time yet arrived now, which
	// is exactly the pair of facts the two columns exist to keep apart.
	all, err := s.ListSessions(ctx, me, SessionFilter{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	for _, sess := range all.Sessions {
		if sess.SessionID == "sess-old" && !sess.IngestedAt.After(sess.StartedAt) {
			t.Errorf("backfilled session has ingested_at %v and started_at %v",
				sess.IngestedAt, sess.StartedAt)
		}
	}
}
