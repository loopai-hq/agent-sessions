package store

// Two kinds of test live here. Most of them drive the scriptable fake from
// store_test.go, because the properties that matter for these queries are which
// statement is issued, with which arguments, and inside which transaction, and
// none of that needs a server. The lockout guard is the exception: "two
// simultaneous demotions cannot both succeed" is a claim about row locks, and a
// fake cannot hold a row lock. Those tests open a real database and skip when
// none is configured, so the default run still passes on a laptop with no
// Postgres.
//
//	createdb loop_sessions_test
//	LOOP_SESSIONS_TEST_DSN=postgres:///loop_sessions_test go test -race ./server/store/...

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/agent-sessions/internal/health"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func healthRow(email, deviceID string, emitted, received time.Time, worst health.Level, r health.Report) []any {
	body, err := json.Marshal(r)
	if err != nil {
		panic("marshal health fixture: " + err.Error())
	}
	var device any
	if deviceID != "" {
		device = deviceID
	}
	return []any{email, device, emitted, received, string(worst), string(body)}
}

func deviceRow(id, email, hostname string, revoked *time.Time) []any {
	return []any{id, email, hostname, "darwin", "arm64", "2.0.1", at, nil, revoked}
}

func accessRow(id int64, viewer, sessionID, owner, via string, when time.Time) []any {
	return []any{id, viewer, sessionID, owner, via, when}
}

// ---------------------------------------------------------------------------
// LatestHealth
// ---------------------------------------------------------------------------

func TestLatestHealthPicksTheNewestArrivalPerDeviceRatherThanPerPerson(t *testing.T) {
	cases := []struct {
		name    string
		want    string
		absent  bool
		because string
	}{
		{
			name:    "grouped by device",
			want:    "DISTINCT ON (email, device_id)",
			because: "a person with a working laptop and a dead one is not covered, and a per-person maximum reports them as healthy",
		},
		{
			name:    "ordered by arrival",
			want:    "received_at DESC",
			because: "emitted_at is the laptop's clock, so a machine whose clock jumped forward would pin a stale sample at the top of its own history forever",
		},
		{
			name:    "arrival wins over the machine's own stamp",
			want:    "ORDER BY email, device_id, emitted_at DESC",
			absent:  true,
			because: "ordering the pick by emitted_at makes ReceivedAt something other than the device's true maximum, and coverage subtracts ReceivedAt from now",
		},
		{
			name:    "deterministic within one arrival instant",
			want:    "id DESC",
			because: "two reports can share a received_at, and the snapshot shown must not depend on which one the planner happened to reach first",
		},
	}

	db := &fakeDB{stubs: []*stub{{match: "FROM health_reports", rows: [][]any{
		healthRow("me@example.com", "11111111-1111-4111-8111-111111111111", at, at, health.LevelInfo, healthSample()),
	}}}}
	if _, err := NewWithDB(db, nil).LatestHealth(context.Background()); err != nil {
		t.Fatalf("LatestHealth: %v", err)
	}
	sql := db.find(t, "FROM health_reports").sql

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := strings.Contains(sql, c.want); got == c.absent {
				t.Errorf("statement %s %q; %s:\n%s",
					map[bool]string{true: "contains", false: "does not contain"}[got], c.want, c.because, sql)
			}
		})
	}
}

func TestLatestHealthReturnsARowForEveryDeviceAPersonOwns(t *testing.T) {
	working := "11111111-1111-4111-8111-111111111111"
	dead := "22222222-2222-4222-8222-222222222222"
	db := &fakeDB{stubs: []*stub{{match: "FROM health_reports", rows: [][]any{
		healthRow("me@example.com", working, at, at, health.LevelInfo, healthSample()),
		healthRow("me@example.com", dead, at.Add(-48*time.Hour), at.Add(-48*time.Hour), health.LevelCritical, healthSample()),
	}}}}

	snaps, err := NewWithDB(db, nil).LatestHealth(context.Background())
	if err != nil {
		t.Fatalf("LatestHealth: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("got %d snapshots, want one per device; a collapsed row hides the dead laptop", len(snaps))
	}
	if snaps[0].DeviceID != working || snaps[1].DeviceID != dead {
		t.Errorf("device ids = %q, %q", snaps[0].DeviceID, snaps[1].DeviceID)
	}
}

func TestLatestHealthKeepsTheLaptopsClockAndOursApart(t *testing.T) {
	emitted := at.Add(90 * time.Minute)
	received := at
	db := &fakeDB{stubs: []*stub{{match: "FROM health_reports", rows: [][]any{
		healthRow("me@example.com", "11111111-1111-4111-8111-111111111111",
			emitted, received, health.LevelDegraded, healthSample()),
	}}}}

	snaps, err := NewWithDB(db, nil).LatestHealth(context.Background())
	if err != nil {
		t.Fatalf("LatestHealth: %v", err)
	}
	if !snaps[0].EmittedAt.Equal(emitted) || !snaps[0].ReceivedAt.Equal(received) {
		t.Errorf("emitted %v received %v, want %v and %v; collapsing the two clocks hides drift",
			snaps[0].EmittedAt, snaps[0].ReceivedAt, emitted, received)
	}
	if snaps[0].Worst != health.LevelDegraded {
		t.Errorf("worst = %q, want the level the agent assigned itself", snaps[0].Worst)
	}
	if len(snaps[0].Report.Conditions) != len(healthSample().Conditions) {
		t.Errorf("report body did not survive the round trip: %+v", snaps[0].Report)
	}
}

func TestLatestHealthFailsRatherThanDroppingAnUndecodableReport(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "FROM health_reports", rows: [][]any{
		{"me@example.com", "11111111-1111-4111-8111-111111111111", at, at, "info", "{not json"},
	}}}}

	snaps, err := NewWithDB(db, nil).LatestHealth(context.Background())
	if err == nil {
		t.Fatalf("a report that cannot be decoded was skipped silently; the machine then reads as silent while it is reporting")
	}
	if snaps != nil {
		t.Errorf("returned %d snapshots alongside an error", len(snaps))
	}
}

func TestLatestHealthReportsAReadFailureRatherThanAnEmptyFleet(t *testing.T) {
	down := errors.New("connection refused")
	db := &fakeDB{stubs: []*stub{{match: "FROM health_reports", err: down}}}

	snaps, err := NewWithDB(db, nil).LatestHealth(context.Background())
	if !errors.Is(err, down) {
		t.Fatalf("error = %v, want the underlying failure; an empty fleet looks like every laptop having gone quiet", err)
	}
	if snaps != nil {
		t.Errorf("returned snapshots alongside an error")
	}
}

// ---------------------------------------------------------------------------
// EnrolledDevices
// ---------------------------------------------------------------------------

func TestEnrolledDevicesIncludesRevokedMachines(t *testing.T) {
	revoked := at.Add(-time.Hour)
	db := &fakeDB{stubs: []*stub{{match: "FROM devices", rows: [][]any{
		deviceRow("11111111-1111-4111-8111-111111111111", "me@example.com", "laptop", nil),
		deviceRow("22222222-2222-4222-8222-222222222222", "me@example.com", "old-laptop", &revoked),
	}}}}

	devices, err := NewWithDB(db, nil).EnrolledDevices(context.Background())
	if err != nil {
		t.Fatalf("EnrolledDevices: %v", err)
	}
	if len(devices) != 2 {
		t.Fatalf("got %d devices, want both; a revoked laptop that is still reporting is exactly what coverage exists to catch", len(devices))
	}
	if devices[1].RevokedAt == nil {
		t.Errorf("the revocation stamp was dropped, so the caller cannot tell the two machines apart")
	}
	if sql := db.find(t, "FROM devices").sql; strings.Contains(sql, "revoked_at IS NULL") {
		t.Errorf("the query filters revoked devices out:\n%s", sql)
	}
}

func TestEnrolledDevicesFillsTheOptionalColumnsRatherThanLeavingThemNil(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "FROM devices", rows: [][]any{
		{"11111111-1111-4111-8111-111111111111", "me@example.com", nil, nil, nil, nil, at, nil, nil},
	}}}}

	devices, err := NewWithDB(db, nil).EnrolledDevices(context.Background())
	if err != nil {
		t.Fatalf("EnrolledDevices: %v", err)
	}
	if devices[0].Hostname != "" || devices[0].OS != "" || devices[0].Arch != "" || devices[0].AgentVersion != "" {
		t.Errorf("a device that never reported its details did not scan cleanly: %+v", devices[0])
	}
}

// ---------------------------------------------------------------------------
// AccessEvents
// ---------------------------------------------------------------------------

func TestAccessEventsBindsEveryFilterItAdvertises(t *testing.T) {
	since := at.Add(-24 * time.Hour)
	until := at.Add(time.Hour)
	cases := []struct {
		name string
		f    AccessLogFilter
		arg  int
		want any
	}{
		{"a session narrows to that transcript's readers", AccessLogFilter{SessionID: "s-1"}, 0, "s-1"},
		{"a viewer narrows to what one person read", AccessLogFilter{Viewer: "me@example.com"}, 1, "me@example.com"},
		{"an owner narrows to who read one person's work", AccessLogFilter{Owner: "them@example.com"}, 2, "them@example.com"},
		{"a justification narrows to how the read was authorised", AccessLogFilter{Via: AccessViaShare}, 5, AccessViaShare},
		{"a cursor pages backwards by key rather than by offset", AccessLogFilter{Before: 4096}, 6, int64(4096)},
		{"no cursor asks for the first page", AccessLogFilter{}, 6, int64(0)},
		{"an over-large page is clamped rather than refused", AccessLogFilter{Limit: 100000}, 7, maxLimit},
		{"no page size takes the default", AccessLogFilter{}, 7, defaultLimit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := &fakeDB{}
			if _, err := NewWithDB(db, nil).AccessEvents(context.Background(), c.f); err != nil {
				t.Fatalf("AccessEvents: %v", err)
			}
			if got := db.find(t, "FROM access_log").args[c.arg]; got != c.want {
				t.Errorf("argument %d = %v, want %v", c.arg, got, c.want)
			}
		})
	}

	// The window bounds get their own cases because the zero time has to reach
	// the statement as a NULL: rendered as a value it would read as the epoch,
	// and "no lower bound" and "since 1970" are the same page only by accident.
	bounds := []struct {
		name string
		f    AccessLogFilter
		arg  int
		want *time.Time
	}{
		{"an absent lower bound is a NULL rather than the epoch", AccessLogFilter{}, 3, nil},
		{"an absent upper bound is a NULL rather than the epoch", AccessLogFilter{}, 4, nil},
		{"a lower bound is passed through", AccessLogFilter{Since: since}, 3, &since},
		{"an upper bound is passed through", AccessLogFilter{Until: until}, 4, &until},
	}
	for _, c := range bounds {
		t.Run(c.name, func(t *testing.T) {
			db := &fakeDB{}
			if _, err := NewWithDB(db, nil).AccessEvents(context.Background(), c.f); err != nil {
				t.Fatalf("AccessEvents: %v", err)
			}
			got, _ := db.find(t, "FROM access_log").args[c.arg].(*time.Time)
			switch {
			case c.want == nil && got != nil:
				t.Errorf("argument %d = %v, want NULL", c.arg, got)
			case c.want != nil && (got == nil || !got.Equal(*c.want)):
				t.Errorf("argument %d = %v, want %v", c.arg, got, *c.want)
			}
		})
	}
}

func TestAccessEventsCarriesTheRowKeyThePageCursorIsBuiltFrom(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "FROM access_log", rows: [][]any{
		accessRow(4096, "me@example.com", "s-1", "them@example.com", AccessViaAdmin, at),
		accessRow(4095, "me@example.com", "s-2", "them@example.com", AccessViaShare, at),
	}}}}
	events, err := NewWithDB(db, nil).AccessEvents(context.Background(), AccessLogFilter{})
	if err != nil {
		t.Fatalf("AccessEvents: %v", err)
	}
	if len(events) != 2 || events[0].Via != AccessViaAdmin {
		t.Fatalf("events = %+v", events)
	}
	// The caller pages by handing the last row's id back as Before. A zero here
	// becomes a cursor its own validation rejects, and the audit log silently
	// ends at page one.
	if events[len(events)-1].ID != 4095 {
		t.Errorf("last row id = %d, want the key the next page is asked for", events[len(events)-1].ID)
	}
	// Two reads in the same millisecond are ordinary, and a page boundary that
	// falls between two rows sharing a timestamp either repeats one or drops one
	// depending on how the planner ordered them that time.
	if sql := db.find(t, "FROM access_log").sql; !strings.Contains(sql, "ORDER BY at DESC, id DESC") {
		t.Errorf("audit rows are not totally ordered:\n%s", sql)
	}
}

// ---------------------------------------------------------------------------
// PrincipalLookup
// ---------------------------------------------------------------------------

func TestPrincipalLookupSeparatesAbsenceFromAZeroRow(t *testing.T) {
	broken := errors.New("connection reset")
	cases := []struct {
		name      string
		db        *fakeDB
		wantFound bool
		wantRole  Role
		wantErr   error
	}{
		{
			name: "an enrolled person is found with their role",
			db: &fakeDB{stubs: []*stub{{match: "FROM principals WHERE email",
				rows: [][]any{principalRow("them@example.com", RoleAdmin, nil)}}}},
			wantFound: true,
			wantRole:  RoleAdmin,
		},
		{
			name: "somebody who was never enrolled is absent, not an error",
			db:   &fakeDB{},
		},
		{
			name:    "a failed read stays an error rather than becoming absence",
			db:      &fakeDB{stubs: []*stub{{match: "FROM principals WHERE email", err: broken}}},
			wantErr: broken,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, found, err := NewWithDB(c.db, nil).PrincipalLookup(context.Background(), "them@example.com")
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("error = %v, want %v", err, c.wantErr)
			}
			if found != c.wantFound {
				t.Errorf("found = %v, want %v; a zero row reads as a member and would answer authorization questions",
					found, c.wantFound)
			}
			if p.Role != c.wantRole {
				t.Errorf("role = %q, want %q", p.Role, c.wantRole)
			}
		})
	}
}

func TestPrincipalKeepsItsOlderNotFoundSpelling(t *testing.T) {
	// The two-value form is what callers inside this package use, and they treat
	// a missing row as ErrNotFound. Adding the boolean form must not change what
	// they see.
	if _, err := NewWithDB(&fakeDB{}, nil).Principal(context.Background(), "nobody@example.com"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Principal = %v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// InTx and the roster writes
// ---------------------------------------------------------------------------

func TestInTxCommitsOnlyWhatTheFunctionCompleted(t *testing.T) {
	refused := errors.New("refusing to leave the roster with no admin")
	cases := []struct {
		name          string
		fn            func(context.Context, AdminTx) error
		wantCommitted int
		wantRolled    int
		wantErr       error
	}{
		{
			name:          "a completed change is committed once",
			fn:            func(context.Context, AdminTx) error { return nil },
			wantCommitted: 1,
		},
		{
			name:       "a refused change leaves nothing behind",
			fn:         func(context.Context, AdminTx) error { return refused },
			wantRolled: 1,
			wantErr:    refused,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := &fakeDB{}
			err := NewWithDB(db, nil).InTx(context.Background(), c.fn)
			if err != c.wantErr { //nolint:errorlint // identity is the assertion: InTx hands back the caller's own value, not a wrapper around it
				t.Fatalf("error = %v, want the caller's own value %v; the caller matches its sentinels on this", err, c.wantErr)
			}
			if db.committed != c.wantCommitted || db.rolled != c.wantRolled {
				t.Errorf("committed %d rolled back %d, want %d and %d",
					db.committed, db.rolled, c.wantCommitted, c.wantRolled)
			}
		})
	}
}

func TestInTxReportsAFailureToOpenTheTransaction(t *testing.T) {
	down := errors.New("pool exhausted")
	db := &fakeDB{beginErr: down}
	called := false
	err := NewWithDB(db, nil).InTx(context.Background(), func(context.Context, AdminTx) error {
		called = true
		return nil
	})
	if !errors.Is(err, down) {
		t.Fatalf("error = %v, want %v", err, down)
	}
	if called {
		t.Errorf("ran the change outside a transaction")
	}
}

func TestCountActiveAdminsExceptLocksEveryActiveAdminInAStableOrder(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "SELECT count(*) FROM locked", rows: [][]any{{1}}}}}
	var got int
	if err := NewWithDB(db, nil).InTx(context.Background(), func(ctx context.Context, tx AdminTx) error {
		n, err := tx.CountActiveAdminsExcept(ctx, "them@example.com")
		got = n
		return err
	}); err != nil {
		t.Fatalf("InTx: %v", err)
	}
	if got != 1 {
		t.Errorf("count = %d, want 1", got)
	}

	sql := db.find(t, "SELECT count(*) FROM locked").sql
	cases := []struct {
		want    string
		because string
	}{
		{"FOR UPDATE", "without the lock two admins demoting each other each read the other as the survivor and both succeed"},
		{"role = 'admin'", "the count is about who could administer the system, not about who has a row"},
		{"disabled_at IS NULL", "a disabled admin cannot sign in, so counting them as a survivor permits the lockout the guard exists to prevent"},
		{"ORDER BY email", "two transactions taking the same rows in opposite orders deadlock instead of queueing"},
	}
	for _, c := range cases {
		if !strings.Contains(sql, c.want) {
			t.Errorf("guard does not contain %q; %s:\n%s", c.want, c.because, sql)
		}
	}
	if !db.find(t, "SELECT count(*) FROM locked").inTx {
		t.Errorf("the guard ran outside the transaction it is supposed to protect")
	}
}

func TestTheRosterWriteAndItsAuditRowShareOneTransaction(t *testing.T) {
	db := &fakeDB{}
	err := NewWithDB(db, nil).InTx(context.Background(), func(ctx context.Context, tx AdminTx) error {
		if err := tx.SavePrincipal(ctx, Principal{
			Email: "them@example.com", Role: RoleMember, AddedBy: "me@example.com", AddedAt: at,
		}); err != nil {
			return err
		}
		return tx.RecordPrincipalChange(ctx, PrincipalChange{
			Actor: "me@example.com", Target: "them@example.com",
			FromRole: RoleAdmin, ToRole: RoleMember, At: at,
		})
	})
	if err != nil {
		t.Fatalf("InTx: %v", err)
	}
	// An audit row that can fail independently of the change it describes is not
	// an audit trail.
	if save := db.find(t, "INSERT INTO principals"); !save.inTx {
		t.Errorf("the roster write ran outside the transaction")
	}
	if audit := db.find(t, "INSERT INTO principal_changes"); !audit.inTx {
		t.Errorf("the audit write ran outside the transaction that carried the change")
	}
	if db.committed != 1 {
		t.Errorf("committed %d times, want 1", db.committed)
	}
}

func TestSavePrincipalDoesNotRewriteProvenanceOnAnEdit(t *testing.T) {
	db := &fakeDB{}
	if err := NewWithDB(db, nil).InTx(context.Background(), func(ctx context.Context, tx AdminTx) error {
		return tx.SavePrincipal(ctx, Principal{Email: "them@example.com", Role: RoleAdmin})
	}); err != nil {
		t.Fatalf("InTx: %v", err)
	}
	sql := db.find(t, "INSERT INTO principals").sql
	_, update, ok := strings.Cut(sql, "DO UPDATE SET")
	if !ok {
		t.Fatalf("the write is not an upsert, so joining the roster and being edited are different statements:\n%s", sql)
	}
	for _, col := range []string{"added_by", "added_at"} {
		if strings.Contains(update, col) {
			t.Errorf("an edit rewrites %s, erasing the only record of how this person first got access:\n%s", col, update)
		}
	}
	for _, col := range []string{"role", "display_name", "disabled_at"} {
		if !strings.Contains(update, col) {
			t.Errorf("an edit does not write %s, so the caller's folded row is not what lands:\n%s", col, update)
		}
	}
}

func TestSavePrincipalRefusesARoleTheSchemaWouldReject(t *testing.T) {
	cases := []struct {
		name string
		p    Principal
	}{
		{"a role outside the two the system has", Principal{Email: "them@example.com", Role: Role("owner")}},
		{"an empty role, which a zero-valued struct carries", Principal{Email: "them@example.com"}},
		{"no email at all", Principal{Role: RoleMember}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := &fakeDB{}
			err := NewWithDB(db, nil).InTx(context.Background(), func(ctx context.Context, tx AdminTx) error {
				return tx.SavePrincipal(ctx, c.p)
			})
			if err == nil {
				t.Fatalf("accepted %+v", c.p)
			}
			if db.count("INSERT INTO principals") != 0 {
				t.Errorf("sent the write to the database anyway")
			}
			if db.committed != 0 {
				t.Errorf("committed a refused write")
			}
		})
	}
}

func TestPrincipalChangeRecordsACreationAsTheAbsenceOfAPriorRole(t *testing.T) {
	cases := []struct {
		name         string
		change       PrincipalChange
		wantFromRole string
	}{
		{
			name:         "somebody joining the roster has no prior role",
			change:       PrincipalChange{Target: "new@example.com", ToRole: RoleMember, Created: true},
			wantFromRole: "",
		},
		{
			name:         "a creation carrying a stray prior role is still a creation",
			change:       PrincipalChange{Target: "new@example.com", FromRole: RoleMember, ToRole: RoleAdmin, Created: true},
			wantFromRole: "",
		},
		{
			name:         "a promotion keeps what it was promoted from",
			change:       PrincipalChange{Target: "them@example.com", FromRole: RoleMember, ToRole: RoleAdmin},
			wantFromRole: string(RoleMember),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := &fakeDB{}
			if err := NewWithDB(db, nil).InTx(context.Background(), func(ctx context.Context, tx AdminTx) error {
				return tx.RecordPrincipalChange(ctx, c.change)
			}); err != nil {
				t.Fatalf("InTx: %v", err)
			}
			if got := db.find(t, "INSERT INTO principal_changes").args[2]; got != c.wantFromRole {
				t.Errorf("from_role = %q, want %q; it is the only thing telling a creation from a promotion later",
					got, c.wantFromRole)
			}
		})
	}
}

func TestRecordPrincipalChangeRefusesAnUntargetedRow(t *testing.T) {
	db := &fakeDB{}
	err := NewWithDB(db, nil).InTx(context.Background(), func(ctx context.Context, tx AdminTx) error {
		return tx.RecordPrincipalChange(ctx, PrincipalChange{Actor: "me@example.com", ToRole: RoleAdmin})
	})
	if err == nil {
		t.Fatalf("accepted an audit row naming nobody")
	}
	if db.count("INSERT INTO principal_changes") != 0 {
		t.Errorf("wrote it anyway")
	}
}

// ---------------------------------------------------------------------------
// Against a real database
// ---------------------------------------------------------------------------

// adminPool opens a private schema on the development database and migrates
// into it. It skips rather than fails when none is configured: the default test
// run has to work on a laptop with no Postgres, and everything in this file
// except the row-locking properties below is already covered without one.
func adminPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LOOP_SESSIONS_TEST_DSN")
	if dsn == "" {
		t.Skip("LOOP_SESSIONS_TEST_DSN is unset; skipping the tests that need a database")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("loop_sessions_admin_%d", time.Now().UnixNano())

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
	// The concurrency test needs two transactions open at once, so the pool must
	// never be the thing that serialises them.
	cfg.MaxConns = 4
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
	if err := New(p, nil).Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return p
}

// demoteUnderGuard reproduces the sequence the admin handler runs inside InTx:
// read the target, ask who would be left, refuse if the answer is nobody, then
// write and audit.
//
// counted is called between the guard and the write. It is the seam the test
// below uses to hold both transactions in the state the guard exists for, which
// is both of them having concluded that a survivor remains.
func demoteUnderGuard(ctx context.Context, s *Store, actor, target string, counted func()) error {
	return s.InTx(ctx, func(ctx context.Context, tx AdminTx) error {
		current, found, err := tx.Principal(ctx, target)
		if err != nil {
			return err
		}
		if !found {
			return ErrNotFound
		}
		remaining, err := tx.CountActiveAdminsExcept(ctx, target)
		if err != nil {
			return err
		}
		if counted != nil {
			counted()
		}
		if remaining == 0 {
			return ErrLastAdmin
		}
		next := current
		next.Role = RoleMember
		if err := tx.SavePrincipal(ctx, next); err != nil {
			return err
		}
		return tx.RecordPrincipalChange(ctx, PrincipalChange{
			Actor:    actor,
			Target:   target,
			FromRole: current.Role,
			ToRole:   RoleMember,
			At:       time.Now().UTC(),
		})
	})
}

func TestInTxSerializesTwoConcurrentDemotionsSoOneAdminSurvives(t *testing.T) {
	pool := adminPool(t)
	s := New(pool, nil)
	ctx := context.Background()

	// Exactly two active admins, so each demotion is safe only if the other has
	// not also happened.
	const ada, grace = "ada@example.com", "grace@example.com"
	if _, err := pool.Exec(ctx, `DELETE FROM principals`); err != nil {
		t.Fatalf("clear roster: %v", err)
	}
	for _, email := range []string{ada, grace} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO principals (email, role, added_by) VALUES ($1, 'admin', 'test')`, email); err != nil {
			t.Fatalf("seed %s: %v", email, err)
		}
	}

	// Racing the two transactions and hoping for the bad interleaving makes a
	// test that passes for the wrong reason nearly every run: one demotion
	// ordinarily finishes before the other has begun. Instead each side, once its
	// guard has answered, waits for the other side's guard to answer too before
	// it writes. Without a row lock both answers arrive, both say a survivor
	// remains, and both transactions commit. With one, the second guard cannot
	// answer at all until the first has committed, so the wait times out, which
	// is the pause this deadline pays for.
	var counted atomic.Int32
	meet := func() {
		counted.Add(1)
		deadline := time.Now().Add(500 * time.Millisecond)
		for counted.Load() < 2 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}

	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		errs[0] = demoteUnderGuard(ctx, s, ada, grace, meet)
	}()
	go func() {
		defer wg.Done()
		<-start
		errs[1] = demoteUnderGuard(ctx, s, grace, ada, meet)
	}()
	close(start)
	wg.Wait()

	var succeeded int
	for i, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrLastAdmin):
		default:
			t.Errorf("demotion %d failed with %v, want either success or ErrLastAdmin; "+
				"a serialization failure or a deadlock means the guard queues badly rather than refusing cleanly", i, err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("%d of 2 demotions succeeded, want exactly 1", succeeded)
	}

	var admins int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM principals WHERE role = 'admin' AND disabled_at IS NULL`).Scan(&admins); err != nil {
		t.Fatalf("count admins: %v", err)
	}
	if admins != 1 {
		t.Errorf("%d active admins left, want 1; zero is unrecoverable without database access", admins)
	}

	// The refused demotion must not have left an audit row claiming it happened.
	var changes int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM principal_changes`).Scan(&changes); err != nil {
		t.Fatalf("count principal changes: %v", err)
	}
	if changes != 1 {
		t.Errorf("%d change records for 1 successful demotion", changes)
	}
}

func TestLatestHealthIsOneRowPerDeviceAgainstARealPlanner(t *testing.T) {
	pool := adminPool(t)
	s := New(pool, nil)
	ctx := context.Background()

	const (
		email   = "ada@example.com"
		working = "11111111-1111-4111-8111-111111111111"
		dead    = "22222222-2222-4222-8222-222222222222"
	)
	if _, err := pool.Exec(ctx, `TRUNCATE health_reports`); err != nil {
		t.Fatalf("clear health: %v", err)
	}
	body, err := json.Marshal(healthSample())
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	// The middle row is the clock-jumped-forward case: it was stamped furthest in
	// the future by the laptop and arrived before the row that follows it.
	rows := []struct {
		device            string
		emitted, received time.Time
		worst             health.Level
	}{
		{working, at.Add(-2 * time.Hour), at.Add(-2 * time.Hour), health.LevelInfo},
		{working, at.Add(48 * time.Hour), at.Add(-time.Hour), health.LevelInfo},
		{working, at, at, health.LevelDegraded},
		{dead, at.Add(-72 * time.Hour), at.Add(-72 * time.Hour), health.LevelCritical},
	}
	for _, r := range rows {
		if _, err := pool.Exec(ctx, `
			INSERT INTO health_reports (email, device_id, emitted_at, received_at, worst, report)
			VALUES ($1, $2::uuid, $3, $4, $5, $6::jsonb)`,
			email, r.device, r.emitted, r.received, string(r.worst), string(body)); err != nil {
			t.Fatalf("insert health report: %v", err)
		}
	}

	snaps, err := s.LatestHealth(ctx)
	if err != nil {
		t.Fatalf("LatestHealth: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("got %d snapshots for 2 devices, want one each: %+v", len(snaps), snaps)
	}
	byDevice := map[string]HealthSnapshot{}
	for _, s := range snaps {
		byDevice[s.DeviceID] = s
	}
	if got := byDevice[working]; !got.ReceivedAt.Equal(at) {
		t.Errorf("working laptop received_at = %v, want %v; a clock that jumped forward pinned a stale sample as newest",
			got.ReceivedAt, at)
	}
	if got := byDevice[dead].Worst; got != health.LevelCritical {
		t.Errorf("dead laptop worst = %q, want critical; a per-person maximum would have hidden it behind the working one", got)
	}
}

func TestEnrolledDevicesReturnsRevokedMachinesFromARealTable(t *testing.T) {
	pool := adminPool(t)
	s := New(pool, nil)
	ctx := context.Background()

	const (
		email   = "ada@example.com"
		live    = "11111111-1111-4111-8111-111111111111"
		retired = "22222222-2222-4222-8222-222222222222"
	)
	if _, err := pool.Exec(ctx, `TRUNCATE device_tokens, devices CASCADE`); err != nil {
		t.Fatalf("clear devices: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO principals (email, role, added_by) VALUES ($1, 'member', 'test')
		ON CONFLICT (email) DO NOTHING`, email); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO devices (id, email, hostname, revoked_at)
		VALUES ($1::uuid, $3, 'laptop', NULL), ($2::uuid, $3, 'old-laptop', now())`,
		live, retired, email); err != nil {
		t.Fatalf("seed devices: %v", err)
	}

	devices, err := s.EnrolledDevices(ctx)
	if err != nil {
		t.Fatalf("EnrolledDevices: %v", err)
	}
	if len(devices) != 2 {
		t.Fatalf("got %d devices, want both the live and the revoked one: %+v", len(devices), devices)
	}
	var sawRevoked bool
	for _, d := range devices {
		if d.ID == retired {
			sawRevoked = d.RevokedAt != nil
		}
	}
	if !sawRevoked {
		t.Errorf("the revoked machine came back without its revocation stamp, so a revoked laptop that is still reporting looks ordinary")
	}
}
