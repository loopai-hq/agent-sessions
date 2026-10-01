package app

// These tests drive the adapter through the real *store.Store over a scriptable
// connection. Going through the store rather than around it is the point: the
// thing that has never been exercised before is the join between the two
// packages, and a test that stubs the store out would prove only that this file
// compiles against a port it was written from.
//
// The properties worth pinning here are which statement runs inside which
// transaction, and which fields survive the crossing. Neither needs a database
// to observe, so these run on a laptop with no Postgres; the store's own
// integration tests own everything that needs a server to evaluate the SQL.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loopai-hq/loop-sessions/internal/health"
	"github.com/loopai-hq/loop-sessions/server/admin"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// ---------------------------------------------------------------------------
// A scriptable fake connection
// ---------------------------------------------------------------------------

// Statements this package's tests reach for, named so the assertions read as
// claims about the roster rather than as substring matching. They are matched by
// substring so that whitespace and column order in the store's SQL can change
// without failing an assertion that is not about them.
const (
	admSQLRoster    = "FROM principals ORDER BY email"
	admSQLLookup    = "FROM principals WHERE email = $1"
	admSQLSave      = "INSERT INTO principals"
	admSQLAudit     = "INSERT INTO principal_changes"
	admSQLCount     = "SELECT count(*) FROM locked"
	admSQLHealth    = "FROM health_reports"
	admSQLDevices   = "FROM devices"
	admSQLAccessLog = "FROM access_log"
)

type admCall struct {
	kind string // query | queryrow | exec
	sql  string
	args []any
	// tx is zero when the statement ran on the pool and otherwise numbers the
	// transaction it ran in, so "these two writes were one unit" is an assertion
	// about a value rather than about ordering.
	tx int
}

// admStub answers the first statement whose SQL contains match.
type admStub struct {
	match string
	rows  [][]any
	err   error
}

type admFakeDB struct {
	mu        sync.Mutex
	stubs     []admStub
	calls     []admCall
	begun     int
	committed int
	rolled    int
	beginErr  error
}

func (f *admFakeDB) answer(sql string) *admStub {
	for i := range f.stubs {
		if strings.Contains(sql, f.stubs[i].match) {
			return &f.stubs[i]
		}
	}
	return nil
}

func (f *admFakeDB) record(kind, sql string, args []any, tx int) *admStub {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, admCall{kind: kind, sql: sql, args: args, tx: tx})
	return f.answer(sql)
}

func (f *admFakeDB) query(sql string, args []any, tx int) (store.Rows, error) {
	s := f.record("query", sql, args, tx)
	if s == nil {
		return &admRows{}, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	return &admRows{rows: s.rows}, nil
}

func (f *admFakeDB) queryRow(sql string, args []any, tx int) store.Row {
	s := f.record("queryrow", sql, args, tx)
	switch {
	case s == nil, len(s.rows) == 0 && s.err == nil:
		return admErrRow{err: pgx.ErrNoRows}
	case s.err != nil:
		return admErrRow{err: s.err}
	default:
		return &admRows{rows: s.rows[:1], i: 1}
	}
}

func (f *admFakeDB) exec(sql string, args []any, tx int) (int64, error) {
	if s := f.record("exec", sql, args, tx); s != nil {
		return 1, s.err
	}
	return 1, nil
}

func (f *admFakeDB) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return f.query(sql, args, 0)
}
func (f *admFakeDB) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	return f.queryRow(sql, args, 0)
}
func (f *admFakeDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return f.exec(sql, args, 0)
}

func (f *admFakeDB) Begin(ctx context.Context) (store.Tx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	f.mu.Lock()
	f.begun++
	id := f.begun
	f.mu.Unlock()
	return &admFakeTx{db: f, id: id}, nil
}

type admFakeTx struct {
	db   *admFakeDB
	id   int
	done bool
}

func (t *admFakeTx) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return t.db.query(sql, args, t.id)
}
func (t *admFakeTx) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	return t.db.queryRow(sql, args, t.id)
}
func (t *admFakeTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return t.db.exec(sql, args, t.id)
}

func (t *admFakeTx) Commit(ctx context.Context) error {
	if t.done {
		return errors.New("commit after the transaction finished")
	}
	t.done = true
	t.db.mu.Lock()
	t.db.committed++
	t.db.mu.Unlock()
	return nil
}

// Rollback after a commit is a no-op rather than an error: the store defers one
// unconditionally, which is what makes an early return roll back.
func (t *admFakeTx) Rollback(ctx context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	t.db.mu.Lock()
	t.db.rolled++
	t.db.mu.Unlock()
	return nil
}

type admRows struct {
	rows [][]any
	i    int
}

func (r *admRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *admRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("scan outside a row")
	}
	return admAssign(r.rows[r.i-1], dest)
}

func (r *admRows) Err() error { return nil }
func (r *admRows) Close()     {}

type admErrRow struct{ err error }

func (e admErrRow) Scan(dest ...any) error { return e.err }

// admAssign moves scripted column values into scan destinations, allocating for
// a pointer destination so a nullable column can be scripted as nil.
func admAssign(src, dest []any) error {
	if len(src) != len(dest) {
		return fmt.Errorf("scan: %d values into %d destinations", len(src), len(dest))
	}
	for i := range dest {
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Pointer || dv.IsNil() {
			return fmt.Errorf("scan: destination %d is not a usable pointer", i)
		}
		out := dv.Elem()
		if src[i] == nil {
			out.Set(reflect.Zero(out.Type()))
			continue
		}
		in := reflect.ValueOf(src[i])
		switch {
		case in.Type().AssignableTo(out.Type()):
			out.Set(in)
		case out.Kind() == reflect.Pointer && in.Type().AssignableTo(out.Type().Elem()):
			p := reflect.New(out.Type().Elem())
			p.Elem().Set(in)
			out.Set(p)
		case in.Type().ConvertibleTo(out.Type()):
			out.Set(in.Convert(out.Type()))
		default:
			return fmt.Errorf("scan: cannot put %s into %s", in.Type(), out.Type())
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var admAt = time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)

func admNewStore(db *admFakeDB) admin.Store { return NewAdminStore(store.NewWithDB(db, nil)) }

func (f *admFakeDB) find(t *testing.T, match string) admCall {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c.sql, match) {
			return c
		}
	}
	t.Fatalf("no statement containing %q; saw:\n%s", match, f.summary())
	return admCall{}
}

func (f *admFakeDB) missing(t *testing.T, match string) {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c.sql, match) {
			t.Fatalf("statement containing %q ran and should not have:\n%s", match, f.summary())
		}
	}
}

func (f *admFakeDB) summary() string {
	var b strings.Builder
	for _, c := range f.calls {
		line := strings.Join(strings.Fields(c.sql), " ")
		if len(line) > 110 {
			line = line[:110]
		}
		fmt.Fprintf(&b, "  [%s tx=%d] %s\n", c.kind, c.tx, line)
	}
	return b.String()
}

// admHasArg reports whether want was bound to the statement, dereferencing
// pointers so a nullable argument compares by value. Position is deliberately not
// asserted: the store owns its own argument order and pinning it here would fail
// on a change that is not about this adapter.
func admHasArg(args []any, want any) bool {
	for _, a := range args {
		if reflect.DeepEqual(admDeref(a), want) {
			return true
		}
	}
	return false
}

func admDeref(v any) any {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		return rv.Elem().Interface()
	}
	return v
}

// admPrincipalRow scripts one principals row in the column order the store reads.
func admPrincipalRow(email, role string, disabledAt *time.Time) []any {
	return []any{email, role, "Ada Lovelace", "root@example.com", admAt, disabledAt}
}

// ---------------------------------------------------------------------------
// The audit trail
// ---------------------------------------------------------------------------

// The admin surface changes who may read colleagues' transcripts, so the record
// of that change is the thing under test. A roster write that commits without its
// audit row is an unaccountable grant of visibility; an audit row that commits
// without its roster write is a record of something that never happened. Both are
// worse than the edit failing.
func TestARosterWriteAndItsAuditRowCommitTogetherOrNotAtAll(t *testing.T) {
	next := admin.Principal{
		Email: "ada@example.com", Role: admin.RoleAdmin,
		AddedBy: "root@example.com", AddedAt: admAt,
	}
	change := admin.PrincipalChange{
		Actor: "root@example.com", Target: "ada@example.com",
		FromRole: admin.RoleMember, ToRole: admin.RoleAdmin, At: admAt,
	}
	boom := errors.New("connection reset by peer")

	tests := []struct {
		name string
		// fail names the statement the database refuses, empty when both succeed.
		fail        string
		wantErr     bool
		wantCommits int
		wantRolls   int
		// wantAudit is false when the audit statement must never have been reached.
		wantAudit bool
	}{
		{
			name: "both writes land in one committed transaction",
			// The change and its record are one unit, so one commit and no
			// rollback is the only acceptable outcome of a successful edit.
			wantCommits: 1,
			wantAudit:   true,
		},
		{
			name:      "a failed audit takes the role change down with it",
			fail:      admSQLAudit,
			wantErr:   true,
			wantRolls: 1,
			wantAudit: true,
		},
		{
			name:      "a failed role change is never audited as having happened",
			fail:      admSQLSave,
			wantErr:   true,
			wantRolls: 1,
			wantAudit: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := &admFakeDB{}
			if tc.fail != "" {
				db.stubs = append(db.stubs, admStub{match: tc.fail, err: boom})
			}
			err := admNewStore(db).InTx(context.Background(), func(ctx context.Context, tx admin.Tx) error {
				if err := tx.SavePrincipal(ctx, next); err != nil {
					return err
				}
				return tx.RecordPrincipalChange(ctx, change)
			})

			if tc.wantErr != (err != nil) {
				t.Fatalf("InTx error = %v, want error %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, boom) {
				t.Fatalf("InTx error = %v, want it to carry %v", err, boom)
			}
			if db.committed != tc.wantCommits || db.rolled != tc.wantRolls {
				t.Fatalf("committed = %d, rolled = %d; want %d and %d\n%s",
					db.committed, db.rolled, tc.wantCommits, tc.wantRolls, db.summary())
			}

			save := db.find(t, admSQLSave)
			if save.tx == 0 {
				t.Fatalf("the roster write ran outside a transaction:\n%s", db.summary())
			}
			if !tc.wantAudit {
				db.missing(t, admSQLAudit)
				return
			}
			audit := db.find(t, admSQLAudit)
			// Same transaction number, not merely both inside some transaction:
			// two writes in two transactions can each succeed on their own, which
			// is precisely the failure this test exists to rule out.
			if audit.tx != save.tx {
				t.Fatalf("roster write ran in tx %d and its audit row in tx %d; want one transaction\n%s",
					save.tx, audit.tx, db.summary())
			}
			if !admHasArg(audit.args, change.Actor) || !admHasArg(audit.args, change.Target) {
				t.Fatalf("audit statement lost the actor or the target: %v", audit.args)
			}
		})
	}
}

// The handler decides between a 404, a 409 and a 500 by matching its own
// sentinels on whatever InTx returns. An adapter that wrapped or replaced that
// value would turn a refused last-admin demotion into an opaque 500, and the
// operator would retry it.
func TestInTxReturnsTheCallersOwnErrorSoItsSentinelsStillMatch(t *testing.T) {
	sentinel := errors.New("admin: refusing to remove the last active admin")
	tests := []struct {
		name string
		give error
	}{
		{name: "a bare sentinel comes back by identity", give: sentinel},
		{name: "a wrapped sentinel still matches", give: fmt.Errorf("guard: %w", sentinel)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := &admFakeDB{}
			err := admNewStore(db).InTx(context.Background(), func(ctx context.Context, tx admin.Tx) error {
				return tc.give
			})
			if err != tc.give { //nolint:errorlint // identity is the assertion: InTx hands back the caller's own value, not a wrapper around it
				t.Fatalf("InTx error = %#v, want the caller's own value %#v", err, tc.give)
			}
			if !errors.Is(err, sentinel) {
				t.Fatalf("InTx error = %v, want errors.Is to find the sentinel", err)
			}
			if db.committed != 0 || db.rolled != 1 {
				t.Fatalf("committed = %d, rolled = %d; want 0 and 1", db.committed, db.rolled)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

// A zero-valued principal reads as a member, and a member is somebody the admin
// package will answer authorization questions about. The boolean is what keeps
// "not on the roster" from arriving as "on the roster with no rights", and a read
// failure must arrive as neither.
func TestPrincipalKeepsAbsenceDistinctFromAZeroRowAndFromAFailure(t *testing.T) {
	boom := errors.New("connection reset by peer")
	tests := []struct {
		name      string
		stubs     []admStub
		wantFound bool
		wantRole  admin.Role
		wantErr   bool
	}{
		{
			name:      "an enrolled admin comes back with their role",
			stubs:     []admStub{{match: admSQLLookup, rows: [][]any{admPrincipalRow("ada@example.com", "admin", nil)}}},
			wantFound: true,
			wantRole:  admin.RoleAdmin,
		},
		{
			name:      "an unknown address is absent rather than an error",
			stubs:     nil,
			wantFound: false,
		},
		{
			name:    "a failed read is an error rather than an absence",
			stubs:   []admStub{{match: admSQLLookup, err: boom}},
			wantErr: true,
		},
	}

	// Both spellings of the lookup are exercised: the admin package calls the
	// pool-backed one to authorize a request and the transaction-backed one to
	// re-authorize inside the write, and the second is the one that survives a
	// demotion landing mid-request.
	readers := map[string]func(admin.Store, context.Context) (admin.Principal, bool, error){
		"outside a transaction": func(s admin.Store, ctx context.Context) (admin.Principal, bool, error) {
			return s.Principal(ctx, "ada@example.com")
		},
		"inside the transaction that guards the write": func(s admin.Store, ctx context.Context) (admin.Principal, bool, error) {
			var (
				p     admin.Principal
				found bool
			)
			err := s.InTx(ctx, func(ctx context.Context, tx admin.Tx) error {
				var err error
				p, found, err = tx.Principal(ctx, "ada@example.com")
				return err
			})
			return p, found, err
		},
	}

	for _, tc := range tests {
		for where, read := range readers {
			t.Run(tc.name+", "+where, func(t *testing.T) {
				db := &admFakeDB{stubs: tc.stubs}
				p, found, err := read(admNewStore(db), context.Background())
				if tc.wantErr {
					if err == nil {
						t.Fatalf("Principal succeeded; want the read failure reported")
					}
					if found {
						t.Fatalf("Principal reported found alongside an error")
					}
					return
				}
				if err != nil {
					t.Fatalf("Principal: %v", err)
				}
				if found != tc.wantFound {
					t.Fatalf("found = %v, want %v", found, tc.wantFound)
				}
				if !tc.wantFound {
					if p != (admin.Principal{}) {
						t.Fatalf("absent principal came back populated: %+v", p)
					}
					return
				}
				if p.Role != tc.wantRole {
					t.Fatalf("role = %q, want %q", p.Role, tc.wantRole)
				}
				if !p.ActiveAdmin() {
					t.Fatalf("%+v does not read as an active admin", p)
				}
			})
		}
	}
}

// Every field crosses in both directions. AddedBy and AddedAt are the only record
// of how somebody got access; DisabledAt is the difference between a revoked
// colleague and an active one, and a dropped pointer would re-enable them on the
// next edit that writes the row back.
func TestPrincipalTranslationCarriesEveryFieldInBothDirections(t *testing.T) {
	disabled := admAt.Add(-24 * time.Hour)
	tests := []struct {
		name string
		give store.Principal
	}{
		{
			name: "an active admin",
			give: store.Principal{
				Email: "ada@example.com", Role: store.RoleAdmin, DisplayName: "Ada Lovelace",
				AddedBy: "root@example.com", AddedAt: admAt,
			},
		},
		{
			name: "a disabled member keeps the moment they were switched off",
			give: store.Principal{
				Email: "eve@example.com", Role: store.RoleMember, DisplayName: "Eve",
				AddedBy: "ada@example.com", AddedAt: admAt, DisabledAt: &disabled,
			},
		},
		{
			name: "a row with every optional field empty",
			give: store.Principal{Email: "new@example.com", Role: store.RoleMember},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := toAdminPrincipal(tc.give)
			if out.Email != tc.give.Email || string(out.Role) != string(tc.give.Role) ||
				out.DisplayName != tc.give.DisplayName || out.AddedBy != tc.give.AddedBy ||
				!out.AddedAt.Equal(tc.give.AddedAt) {
				t.Fatalf("admin principal = %+v, want the fields of %+v", out, tc.give)
			}
			if (out.DisabledAt == nil) != (tc.give.DisabledAt == nil) {
				t.Fatalf("disabled_at = %v, want %v", out.DisabledAt, tc.give.DisabledAt)
			}
			if out.Disabled() != (tc.give.DisabledAt != nil) {
				t.Fatalf("Disabled() = %v on %+v", out.Disabled(), out)
			}
			// Back again, because a roster edit reads a row through one direction
			// and writes it through the other. A field that survives only one way
			// is erased by the first edit that touches the person.
			if back := toStorePrincipal(out); !reflect.DeepEqual(back, tc.give) {
				t.Fatalf("round trip = %+v, want %+v", back, tc.give)
			}
		})
	}
}

// The Role casts in this file are safe only while the two packages spell the
// roles identically. A rename on either side still compiles, and the failure
// would surface as a CHECK constraint violation inside a transaction in
// production rather than here.
func TestRoleVocabulariesAreIdenticalOnBothSidesOfTheBoundary(t *testing.T) {
	tests := []struct {
		name  string
		admin admin.Role
		store store.Role
	}{
		{name: "admin", admin: admin.RoleAdmin, store: store.RoleAdmin},
		{name: "member", admin: admin.RoleMember, store: store.RoleMember},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if string(tc.admin) != string(tc.store) {
				t.Fatalf("admin spells the role %q and the store spells it %q", tc.admin, tc.store)
			}
			if !tc.admin.Valid() {
				t.Fatalf("%q is not a role the admin package accepts", tc.admin)
			}
		})
	}
}

// A NULL from_role is the only thing that tells a creation from a promotion once
// the change is old enough that nobody remembers. The store derives that NULL
// from Created, so Created has to survive the crossing.
func TestPrincipalChangeTranslationKeepsWhatTellsACreationFromAPromotion(t *testing.T) {
	tests := []struct {
		name string
		give admin.PrincipalChange
		want store.PrincipalChange
	}{
		{
			name: "a creation carries no prior role",
			give: admin.PrincipalChange{
				Actor: "root@example.com", Target: "new@example.com",
				ToRole: admin.RoleMember, Created: true, At: admAt,
			},
			want: store.PrincipalChange{
				Actor: "root@example.com", Target: "new@example.com",
				ToRole: store.RoleMember, Created: true, At: admAt,
			},
		},
		{
			name: "a promotion keeps what it was promoted from",
			give: admin.PrincipalChange{
				Actor: "root@example.com", Target: "ada@example.com",
				FromRole: admin.RoleMember, ToRole: admin.RoleAdmin, At: admAt,
			},
			want: store.PrincipalChange{
				Actor: "root@example.com", Target: "ada@example.com",
				FromRole: store.RoleMember, ToRole: store.RoleAdmin, At: admAt,
			},
		},
		{
			name: "a disable keeps both sides of the status it changed",
			give: admin.PrincipalChange{
				Actor: "root@example.com", Target: "eve@example.com",
				FromRole: admin.RoleAdmin, ToRole: admin.RoleAdmin,
				FromDisabled: false, ToDisabled: true, At: admAt,
			},
			want: store.PrincipalChange{
				Actor: "root@example.com", Target: "eve@example.com",
				FromRole: store.RoleAdmin, ToRole: store.RoleAdmin,
				FromDisabled: false, ToDisabled: true, At: admAt,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := toStorePrincipalChange(tc.give); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("store change = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The access log
// ---------------------------------------------------------------------------

// A filter accepted at the edge and dropped on the way to the SQL is worse than
// one that does not exist: the operator reads the narrowed page as the whole
// answer, and the read they were looking for is the one that is missing. Each
// case sets exactly one field, so a dropped filter and a crossed pair of filters
// both fail.
func TestEveryAccessFilterTheAdminUIOffersReachesTheStore(t *testing.T) {
	from := admAt.Add(-time.Hour)
	to := admAt
	tests := []struct {
		name string
		give admin.AccessQuery
		want store.AccessLogFilter
	}{
		{name: "viewer", give: admin.AccessQuery{Viewer: "ada@example.com"}, want: store.AccessLogFilter{Viewer: "ada@example.com"}},
		{name: "owner", give: admin.AccessQuery{Owner: "eve@example.com"}, want: store.AccessLogFilter{Owner: "eve@example.com"}},
		{name: "session id", give: admin.AccessQuery{SessionID: "s-1"}, want: store.AccessLogFilter{SessionID: "s-1"}},
		{name: "how the read was authorised", give: admin.AccessQuery{Via: store.AccessViaShare}, want: store.AccessLogFilter{Via: store.AccessViaShare}},
		{name: "the inclusive lower bound", give: admin.AccessQuery{From: from}, want: store.AccessLogFilter{Since: from}},
		{name: "the exclusive upper bound", give: admin.AccessQuery{To: to}, want: store.AccessLogFilter{Until: to}},
		{name: "the keyset cursor", give: admin.AccessQuery{Before: 4242}, want: store.AccessLogFilter{Before: 4242}},
		{name: "the page size", give: admin.AccessQuery{Limit: 50}, want: store.AccessLogFilter{Limit: 50}},
		{
			name: "all of them at once",
			give: admin.AccessQuery{
				Viewer: "ada@example.com", Owner: "eve@example.com", SessionID: "s-1",
				Via: store.AccessViaAdmin, From: from, To: to, Before: 4242, Limit: 50,
			},
			want: store.AccessLogFilter{
				Viewer: "ada@example.com", Owner: "eve@example.com", SessionID: "s-1",
				Via: store.AccessViaAdmin, Since: from, Until: to, Before: 4242, Limit: 50,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := toStoreAccessFilter(tc.give); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("store filter = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The handler builds its next_before cursor from the last event's id, and its own
// validation rejects a cursor of zero. An adapter that dropped the id would end
// the access log at page one with no error to explain it, which is a log that
// looks complete and is not.
func TestAccessEventsKeepTheRowIdTheCursorPagesOn(t *testing.T) {
	db := &admFakeDB{stubs: []admStub{{match: admSQLAccessLog, rows: [][]any{
		{int64(91), "ada@example.com", "s-1", "eve@example.com", store.AccessViaAdmin, admAt},
		{int64(90), "ada@example.com", "s-2", "eve@example.com", store.AccessViaShare, admAt.Add(-time.Minute)},
	}}}}

	got, err := admNewStore(db).AccessEvents(context.Background(), admin.AccessQuery{
		Owner: "eve@example.com", Limit: 2,
	})
	if err != nil {
		t.Fatalf("AccessEvents: %v", err)
	}
	want := []admin.AccessEvent{
		{ID: 91, Viewer: "ada@example.com", SessionID: "s-1", Owner: "eve@example.com", Via: store.AccessViaAdmin, At: admAt},
		{ID: 90, Viewer: "ada@example.com", SessionID: "s-2", Owner: "eve@example.com", Via: store.AccessViaShare, At: admAt.Add(-time.Minute)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %+v, want %+v", got, want)
	}
	if c := db.find(t, admSQLAccessLog); !admHasArg(c.args, "eve@example.com") {
		t.Fatalf("the owner filter never reached the statement: %v", c.args)
	}
}

// ---------------------------------------------------------------------------
// The fleet
// ---------------------------------------------------------------------------

// A revoked laptop that is still sending health reports is the finding the
// coverage report exists to surface. A device that arrives without its revocation
// stamp is indistinguishable from a live one, and the finding disappears.
func TestEnrolledDevicesKeepRevokedMachinesAndTheirRevocationStamp(t *testing.T) {
	revoked := admAt.Add(-48 * time.Hour)
	seen := admAt.Add(-time.Minute)
	db := &admFakeDB{stubs: []admStub{{match: admSQLDevices, rows: [][]any{
		{"dev-live", "ada@example.com", "ada-laptop", "darwin", "arm64", "1.2.0", admAt, seen, nil},
		{"dev-gone", "ada@example.com", "old-laptop", "darwin", "arm64", "1.0.0", admAt, seen, revoked},
	}}}}

	got, err := admNewStore(db).EnrolledDevices(context.Background())
	if err != nil {
		t.Fatalf("EnrolledDevices: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d devices, want the live one and the revoked one", len(got))
	}
	if got[0].Revoked() {
		t.Fatalf("the live device came back revoked: %+v", got[0])
	}
	if !got[1].Revoked() || !got[1].RevokedAt.Equal(revoked) {
		t.Fatalf("revoked_at = %v, want %v", got[1].RevokedAt, revoked)
	}
	if got[1].ID != "dev-gone" || got[1].Email != "ada@example.com" || got[1].Hostname != "old-laptop" {
		t.Fatalf("device lost its identity in translation: %+v", got[1])
	}
	if got[0].LastSeenAt == nil || !got[0].LastSeenAt.Equal(seen) {
		t.Fatalf("last_seen_at = %v, want %v", got[0].LastSeenAt, seen)
	}
}

// Coverage is a per-machine question: somebody with a working laptop and a dead
// one is not covered. A snapshot that loses its device id collapses the two into
// one row and the working machine answers for the dead one. The two clocks are
// carried separately because coverage measures silence from the moment we
// received a report, never from the moment the laptop says it sent one.
func TestLatestHealthStaysOneRowPerDeviceAndKeepsBothClocks(t *testing.T) {
	emitted := admAt.Add(36 * time.Hour) // a laptop whose clock has run away
	received := admAt.Add(-2 * time.Hour)
	// The stored bodies carry the schema version the agent speaks today, because
	// the property under test is that the body survives the crossing, not which
	// version it was written in; a literal here turned this test red every time
	// the client bumped health.SchemaVersion.
	db := &admFakeDB{stubs: []admStub{{match: admSQLHealth, rows: [][]any{
		{"ada@example.com", "dev-live", admAt, admAt, string(health.LevelInfo),
			fmt.Sprintf(`{"schema_version":%d,"agent_version":"1.2.0"}`, health.SchemaVersion)},
		{"ada@example.com", "dev-dead", emitted, received, string(health.LevelCritical),
			fmt.Sprintf(`{"schema_version":%d,"agent_version":"1.0.0"}`, health.SchemaVersion)},
	}}}}

	got, err := admNewStore(db).LatestHealth(context.Background())
	if err != nil {
		t.Fatalf("LatestHealth: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d snapshots for one person's two machines, want 2", len(got))
	}
	if got[0].DeviceID != "dev-live" || got[1].DeviceID != "dev-dead" {
		t.Fatalf("device ids = %q and %q, want them carried through", got[0].DeviceID, got[1].DeviceID)
	}
	dead := got[1]
	if dead.Worst != health.LevelCritical {
		t.Fatalf("worst = %q, want the level the agent assigned itself", dead.Worst)
	}
	if !dead.EmittedAt.Equal(emitted) || !dead.ReceivedAt.Equal(received) {
		t.Fatalf("clocks = emitted %v received %v, want %v and %v",
			dead.EmittedAt, dead.ReceivedAt, emitted, received)
	}
	if dead.EmittedAt.Equal(dead.ReceivedAt) {
		t.Fatalf("the two clocks collapsed into one, hiding the drift")
	}
	if dead.Report.AgentVersion != "1.0.0" || dead.Report.SchemaVersion != health.SchemaVersion {
		t.Fatalf("report body did not survive: %+v", dead.Report)
	}
}

// ---------------------------------------------------------------------------
// Failure and emptiness
// ---------------------------------------------------------------------------

// admReads names the reads whose failure mode is identical, so the two
// properties below are stated once each rather than four times each.
var admReads = map[string]struct {
	sql  string
	call func(admin.Store, context.Context) (int, error)
}{
	"the roster": {sql: admSQLRoster, call: func(s admin.Store, ctx context.Context) (int, error) {
		ps, err := s.ListPrincipals(ctx)
		return len(ps), err
	}},
	"the device inventory": {sql: admSQLDevices, call: func(s admin.Store, ctx context.Context) (int, error) {
		ds, err := s.EnrolledDevices(ctx)
		return len(ds), err
	}},
	"the health snapshots": {sql: admSQLHealth, call: func(s admin.Store, ctx context.Context) (int, error) {
		hs, err := s.LatestHealth(ctx)
		return len(hs), err
	}},
	"the access log": {sql: admSQLAccessLog, call: func(s admin.Store, ctx context.Context) (int, error) {
		es, err := s.AccessEvents(ctx, admin.AccessQuery{Limit: 10})
		return len(es), err
	}},
}

// A database that cannot answer must not look like a fleet with no laptops, a
// roster with nobody on it or an access log in which nothing has been read. Each
// of those is a statement an operator would act on, and none of them is true.
func TestAFailedReadIsAnErrorRatherThanAnEmptyPage(t *testing.T) {
	boom := errors.New("connection reset by peer")
	for name, read := range admReads {
		t.Run(name, func(t *testing.T) {
			db := &admFakeDB{stubs: []admStub{{match: read.sql, err: boom}}}
			n, err := read.call(admNewStore(db), context.Background())
			if err == nil {
				t.Fatalf("read succeeded with %d rows; want the failure reported", n)
			}
			if !errors.Is(err, boom) {
				t.Fatalf("error = %v, want it to carry %v", err, boom)
			}
			if n != 0 {
				t.Fatalf("read returned %d rows alongside an error", n)
			}
		})
	}
}

// The admin routes marshal these results straight into a JSON body, where a nil
// slice becomes null. An operator's page would then have to tell "no rows" from
// "field absent" for a difference that does not exist, and a client iterating the
// field faults on the empty case only.
func TestAnEmptyReadIsAnEmptyListRatherThanAJSONNull(t *testing.T) {
	for name := range admReads {
		t.Run(name, func(t *testing.T) {
			s := admNewStore(&admFakeDB{})
			ctx := context.Background()
			var got any
			switch name {
			case "the roster":
				got, _ = s.ListPrincipals(ctx)
			case "the device inventory":
				got, _ = s.EnrolledDevices(ctx)
			case "the health snapshots":
				got, _ = s.LatestHealth(ctx)
			case "the access log":
				got, _ = s.AccessEvents(ctx, admin.AccessQuery{Limit: 10})
			}
			if reflect.ValueOf(got).IsNil() {
				t.Fatalf("%s came back as a nil slice, which marshals as null", name)
			}
			if reflect.ValueOf(got).Len() != 0 {
				t.Fatalf("%s came back populated from an empty database", name)
			}
		})
	}
}

// admin.Store.ListPrincipals carries no viewer while store.ListPrincipals still
// gates on one, so this adapter hands it a fabricated admin. That fabrication is
// load-bearing in one direction: if it stops satisfying the store's gate the
// roster and fleet pages go permanently empty, and if this adapter is ever
// mounted behind a non-admin surface it hands the whole roster to a member.
func TestTheFabricatedRosterReaderSatisfiesTheStoresOwnAdminGate(t *testing.T) {
	if !adminRosterReader.IsAdmin() {
		t.Fatalf("roster reader %+v does not satisfy the store's admin gate", adminRosterReader)
	}
	db := &admFakeDB{stubs: []admStub{{match: admSQLRoster, rows: [][]any{
		admPrincipalRow("ada@example.com", "admin", nil),
	}}}}
	got, err := admNewStore(db).ListPrincipals(context.Background())
	if err != nil {
		t.Fatalf("ListPrincipals: %v", err)
	}
	// The statement having run at all is the assertion: the store returns
	// ErrNotAdmin before it queries, so a refused viewer leaves no call behind.
	db.find(t, admSQLRoster)
	if len(got) != 1 || got[0].Email != "ada@example.com" || !got[0].ActiveAdmin() {
		t.Fatalf("roster = %+v, want the one active admin", got)
	}
}
