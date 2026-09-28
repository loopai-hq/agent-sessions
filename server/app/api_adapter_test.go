package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loopai-hq/agent-sessions/server/api"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// These tests drive the adapter through the real *store.Store over a fake
// connection rather than over a fake store. The property this file exists to
// defend lives in the seam between the two: the store collapses "not yours" into
// "no rows" inside the SELECT, and the adapter has to keep them collapsed on the
// way out. A hand-written fake store would let the test author decide what the
// store returns, which is the one thing this test must not be free to assume.
//
// Every helper here is named with an api prefix. Package app has one file per
// adapter and each carries its own fixtures, so a bare fakeDB would collide with
// the next adapter's the moment both exist.

// ---------------------------------------------------------------------------
// A store.DB that models the sliver of the schema these tests need
// ---------------------------------------------------------------------------

var apiFixedTime = time.Date(2026, 8, 4, 9, 30, 0, 0, time.UTC)

type apiFakeCall struct {
	sql  string
	args []any
}

type apiFakeShare struct {
	id        string
	token     string
	sessionID string
	createdBy string
	grantee   string
}

// apiFakeResult scripts one statement the fake does not model, matched on a
// substring of its SQL.
type apiFakeResult struct {
	match string
	rows  [][]any
}

// apiFakeDB answers the statements the store issues.
//
// The session and share reads are modelled rather than scripted because the
// central test needs a fixture in which a session genuinely exists and is
// genuinely invisible. A fake that simply answered "no rows" to both fixtures
// would agree that denial and absence look alike without ever having been asked
// two different questions.
type apiFakeDB struct {
	sessions map[string]string
	shares   []apiFakeShare
	results  []apiFakeResult

	// fail stands in for the class of failure that must never be mistaken for
	// absence: the pool is gone, the query timed out, the column is not there.
	fail     error
	beginErr error

	calls []apiFakeCall
}

func (f *apiFakeDB) record(sql string, args []any) {
	f.calls = append(f.calls, apiFakeCall{sql: sql, args: args})
}

func (f *apiFakeDB) scripted(sql string) ([][]any, bool) {
	for _, r := range f.results {
		if strings.Contains(sql, r.match) {
			return r.rows, true
		}
	}
	return nil, false
}

func (f *apiFakeDB) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	f.record(sql, args)
	if f.fail != nil {
		return nil, f.fail
	}
	if rows, ok := f.scripted(sql); ok {
		return &apiFakeRows{rows: rows}, nil
	}
	return &apiFakeRows{}, nil
}

func (f *apiFakeDB) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	f.record(sql, args)
	if f.fail != nil {
		return apiFakeErrRow{err: f.fail}
	}
	switch {
	case strings.Contains(sql, "WHERE s.session_id = $3"):
		return f.readSession(args)
	case strings.Contains(sql, "SELECT email FROM sessions"):
		return f.readSessionOwner(args)
	case strings.Contains(sql, "WHERE token = $1"):
		return f.readShareByToken(args)
	case strings.Contains(sql, "INSERT INTO shares"):
		return apiFakeRow{values: []any{apiFixedTime}}
	}
	if rows, ok := f.scripted(sql); ok && len(rows) > 0 {
		return apiFakeRow{values: rows[0]}
	}
	return apiFakeErrRow{err: pgx.ErrNoRows}
}

func (f *apiFakeDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	f.record(sql, args)
	if f.fail != nil {
		return 0, f.fail
	}
	if strings.Contains(sql, "UPDATE shares") {
		return f.revokeShare(args), nil
	}
	return 1, nil
}

func (f *apiFakeDB) Begin(ctx context.Context) (store.Tx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	return &apiFakeTx{db: f}, nil
}

// visible is the fake's copy of the store's authorization predicate, in Go
// instead of SQL. It exists so a fixture can hold a session the viewer may not
// read; the store's own copy is the one under test everywhere else.
func (f *apiFakeDB) visible(sessionID, owner, viewer string, admin bool) bool {
	if admin || owner == viewer {
		return true
	}
	for _, sh := range f.shares {
		if sh.sessionID == sessionID && (sh.grantee == "" || sh.grantee == viewer) {
			return true
		}
	}
	return false
}

func (f *apiFakeDB) readSession(args []any) store.Row {
	admin, _ := args[0].(bool)
	viewer, _ := args[1].(string)
	id, _ := args[2].(string)
	owner, ok := f.sessions[id]
	if !ok || !f.visible(id, owner, viewer, admin) {
		return apiFakeErrRow{err: pgx.ErrNoRows}
	}
	return apiFakeRow{values: apiSessionRow(id, owner)}
}

func (f *apiFakeDB) readSessionOwner(args []any) store.Row {
	id, _ := args[0].(string)
	viewer, _ := args[1].(string)
	admin, _ := args[2].(bool)
	owner, ok := f.sessions[id]
	if !ok || !(admin || owner == viewer) {
		return apiFakeErrRow{err: pgx.ErrNoRows}
	}
	return apiFakeRow{values: []any{owner}}
}

func (f *apiFakeDB) readShareByToken(args []any) store.Row {
	token, _ := args[0].(string)
	viewer, _ := args[1].(string)
	for _, sh := range f.shares {
		if sh.token != token || (sh.grantee != "" && sh.grantee != viewer) {
			continue
		}
		var grantee any
		if sh.grantee != "" {
			grantee = sh.grantee
		}
		return apiFakeRow{values: []any{sh.id, sh.sessionID, sh.createdBy, grantee, apiFixedTime, nil}}
	}
	return apiFakeErrRow{err: pgx.ErrNoRows}
}

func (f *apiFakeDB) revokeShare(args []any) int64 {
	id, _ := args[0].(string)
	viewer, _ := args[1].(string)
	admin, _ := args[2].(bool)
	for _, sh := range f.shares {
		if sh.id != id {
			continue
		}
		if admin || sh.createdBy == viewer || f.sessions[sh.sessionID] == viewer {
			return 1
		}
	}
	return 0
}

type apiFakeTx struct {
	db   *apiFakeDB
	done bool
}

func (t *apiFakeTx) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return t.db.Query(ctx, sql, args...)
}

func (t *apiFakeTx) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	return t.db.QueryRow(ctx, sql, args...)
}

func (t *apiFakeTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return t.db.Exec(ctx, sql, args...)
}

func (t *apiFakeTx) Commit(ctx context.Context) error {
	if t.done {
		return errors.New("commit after the transaction finished")
	}
	t.done = true
	return nil
}

func (t *apiFakeTx) Rollback(ctx context.Context) error {
	t.done = true
	return nil
}

type apiFakeRows struct {
	rows [][]any
	i    int
}

func (r *apiFakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *apiFakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("scan outside a row")
	}
	return apiAssign(r.rows[r.i-1], dest)
}

func (r *apiFakeRows) Err() error { return nil }
func (r *apiFakeRows) Close()     {}

type apiFakeRow struct{ values []any }

func (r apiFakeRow) Scan(dest ...any) error { return apiAssign(r.values, dest) }

type apiFakeErrRow struct{ err error }

func (e apiFakeErrRow) Scan(dest ...any) error { return e.err }

// apiAssign copies a scripted row into the store's scan destinations, following
// the same widening pgx does: a value lands in a pointer destination as a new
// pointer, and a nil lands as the zero value, so a fixture can express a NULL
// column without knowing which of the store's fields is a pointer.
func apiAssign(src, dest []any) error {
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

// apiSessionRow is the minimum a session projection needs to scan. Tests that
// care about a particular field script their own row instead.
func apiSessionRow(sessionID, owner string) []any {
	return []any{
		sessionID, owner, nil, "claude_code", "user", nil,
		nil, nil, nil, apiFixedTime, nil,
		false, 0, 0, 0, 0,
		nil, nil,
		int64(0), int64(0), int64(0), int64(0),
		float64(0), nil, apiFixedTime, apiFixedTime,
	}
}

func apiAdapter(db *apiFakeDB) api.Store {
	// The pricer is nil because nothing on the read path prices anything; a
	// stub here would be a second thing to keep in step with the real one.
	return NewAPIStore(store.NewWithDB(db, nil))
}

func (f *apiFakeDB) find(t *testing.T, match string) apiFakeCall {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c.sql, match) {
			return c
		}
	}
	t.Fatalf("no statement containing %q was issued", match)
	return apiFakeCall{}
}

// ---------------------------------------------------------------------------
// The error-collapse property
// ---------------------------------------------------------------------------

// TestAPIStoreReportsDenialAndAbsenceAsOneError pins the rule the whole api port
// is built around: a session that exists but is not the viewer's to see, and a
// session that does not exist, leave this adapter as the same error value with
// the same text. Anything that separates them tells the caller that a named
// colleague ran something at a particular time, which is the fact the viewer was
// not allowed to learn.
//
// Each case runs a third fixture in which the call is permitted. Without it the
// two refusals could both be an artefact of a fixture that refuses everything,
// and the test would pass against an adapter that had stopped working.
func TestAPIStoreReportsDenialAndAbsenceAsOneError(t *testing.T) {
	const (
		viewerEmail = "member@example.com"
		colleague   = "colleague@example.com"
		sessionID   = "sess-1"
		shareID     = "share-1"
		shareToken  = "share-token-1"
	)
	v := api.Viewer{Email: viewerEmail, Role: api.RoleMember}

	// The viewer owns the session and the share on it.
	permitted := func() *apiFakeDB {
		return &apiFakeDB{
			sessions: map[string]string{sessionID: viewerEmail},
			shares: []apiFakeShare{{
				id: shareID, token: shareToken, sessionID: sessionID, createdBy: viewerEmail,
			}},
		}
	}
	// The session and the share exist and belong to somebody else.
	denied := func() *apiFakeDB {
		return &apiFakeDB{
			sessions: map[string]string{sessionID: colleague},
			shares: []apiFakeShare{{
				id: shareID, token: shareToken, sessionID: sessionID,
				createdBy: colleague, grantee: colleague,
			}},
		}
	}
	// Nothing by that name was ever stored.
	absent := func() *apiFakeDB { return &apiFakeDB{} }

	cases := []struct {
		name string
		call func(context.Context, api.Store) error
	}{
		{"get session", func(ctx context.Context, s api.Store) error {
			_, err := s.GetSession(ctx, v, sessionID)
			return err
		}},
		{"get events", func(ctx context.Context, s api.Store) error {
			_, err := s.GetEvents(ctx, v, sessionID, api.EventRange{Limit: 10})
			return err
		}},
		{"create share", func(ctx context.Context, s api.Store) error {
			_, err := s.CreateShare(ctx, v, api.ShareRequest{SessionID: sessionID})
			return err
		}},
		{"revoke share", func(ctx context.Context, s api.Store) error {
			return s.RevokeShare(ctx, v, shareID)
		}},
		{"resolve share", func(ctx context.Context, s api.Store) error {
			_, _, err := s.ResolveShare(ctx, v, shareToken)
			return err
		}},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(ctx, apiAdapter(permitted())); err != nil {
				t.Fatalf("permitted fixture must succeed or the refusals below prove nothing: %v", err)
			}

			denialErr := tc.call(ctx, apiAdapter(denied()))
			absenceErr := tc.call(ctx, apiAdapter(absent()))

			if !errors.Is(denialErr, api.ErrNotFound) {
				t.Fatalf("denial: got %v, want api.ErrNotFound", denialErr)
			}
			if !errors.Is(absenceErr, api.ErrNotFound) {
				t.Fatalf("absence: got %v, want api.ErrNotFound", absenceErr)
			}
			if denialErr != absenceErr {
				t.Errorf("denial and absence are distinguishable error values: %#v vs %#v",
					denialErr, absenceErr)
			}
			if denialErr.Error() != absenceErr.Error() {
				t.Errorf("denial and absence read differently: %q vs %q",
					denialErr.Error(), absenceErr.Error())
			}
		})
	}
}

// TestAPIStoreNeverWidensAnUnknownErrorIntoNotFound is the other half of the
// same rule. Absence and denial converge; everything else must not join them. A
// database outage rendered as not-found reaches the dashboard as a person with
// no sessions, which is indistinguishable from a quiet week and gets diagnosed
// as one.
func TestAPIStoreNeverWidensAnUnknownErrorIntoNotFound(t *testing.T) {
	boom := errors.New("connection reset by peer")
	v := api.Viewer{Email: "member@example.com", Role: api.RoleMember}

	cases := []struct {
		name string
		call func(context.Context, api.Store) error
	}{
		{"principal", func(ctx context.Context, s api.Store) error {
			_, _, err := s.Principal(ctx, "member@example.com")
			return err
		}},
		{"list sessions", func(ctx context.Context, s api.Store) error {
			_, err := s.ListSessions(ctx, v, api.SessionFilter{})
			return err
		}},
		{"get session", func(ctx context.Context, s api.Store) error {
			_, err := s.GetSession(ctx, v, "sess-1")
			return err
		}},
		{"get events", func(ctx context.Context, s api.Store) error {
			_, err := s.GetEvents(ctx, v, "sess-1", api.EventRange{})
			return err
		}},
		{"search messages", func(ctx context.Context, s api.Store) error {
			_, err := s.SearchMessages(ctx, v, api.SearchFilter{Query: "deploy"})
			return err
		}},
		{"list shares", func(ctx context.Context, s api.Store) error {
			_, err := s.ListShares(ctx, v, "sess-1")
			return err
		}},
		{"create share", func(ctx context.Context, s api.Store) error {
			_, err := s.CreateShare(ctx, v, api.ShareRequest{SessionID: "sess-1"})
			return err
		}},
		{"revoke share", func(ctx context.Context, s api.Store) error {
			return s.RevokeShare(ctx, v, "share-1")
		}},
		{"resolve share", func(ctx context.Context, s api.Store) error {
			_, _, err := s.ResolveShare(ctx, v, "share-token-1")
			return err
		}},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Both doors are shut: the ones that open a transaction fail at
			// Begin, the ones that go straight to the pool fail at the
			// statement, and neither is allowed to look like an empty result.
			s := apiAdapter(&apiFakeDB{fail: boom, beginErr: boom})
			err := tc.call(ctx, s)
			if err == nil {
				t.Fatal("a failing database produced no error")
			}
			if !errors.Is(err, boom) {
				t.Errorf("cause was dropped: got %v, want it to wrap %v", err, boom)
			}
			if errors.Is(err, api.ErrNotFound) {
				t.Errorf("an infrastructure failure was widened into not-found: %v", err)
			}
			if errors.Is(err, api.ErrInvalidCursor) {
				t.Errorf("an infrastructure failure was widened into an invalid cursor: %v", err)
			}
			if !strings.Contains(err.Error(), "app: ") {
				t.Errorf("error lost its adapter context: %q", err.Error())
			}
		})
	}
}

// TestAPIStoreTranslatesAnInvalidCursorRatherThanRestarting defends the second
// sentinel the routes translate. A cursor the store did not issue has to reach
// the caller as a 400; swallowing it and starting from the first page again
// looks to a reader like duplicated rows and to a script like a list with no
// end.
func TestAPIStoreTranslatesAnInvalidCursorRatherThanRestarting(t *testing.T) {
	cases := []struct {
		name   string
		cursor string
	}{
		{"not base64 at all", "not a cursor!"},
		{"base64 of something we did not issue", base64.RawURLEncoding.EncodeToString([]byte(`{}`))},
	}

	ctx := context.Background()
	v := api.Viewer{Email: "member@example.com", Role: api.RoleMember}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := apiAdapter(&apiFakeDB{})
			_, err := s.ListSessions(ctx, v, api.SessionFilter{Cursor: tc.cursor})
			if err != api.ErrInvalidCursor {
				t.Fatalf("got %v, want api.ErrInvalidCursor", err)
			}
			if errors.Is(err, api.ErrNotFound) {
				t.Error("an invalid cursor was reported as a missing session")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Field translation
// ---------------------------------------------------------------------------

// TestAPIStoreCarriesEverySessionFieldAcross guards against the quiet failure of
// a hand-written translation: a field left behind reads as an empty value on the
// dashboard rather than as an error, so a session whose cost or repository
// silently disappeared looks like a session that never had one.
func TestAPIStoreCarriesEverySessionFieldAcross(t *testing.T) {
	var (
		started  = time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
		ended    = time.Date(2026, 7, 1, 11, 30, 0, 0, time.UTC)
		ingested = time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC)
		updated  = time.Date(2026, 7, 2, 8, 5, 0, 0, time.UTC)
	)
	full := []any{
		"sess-a", "author@example.com", "device-7", "claude_code", "automation", "sess-parent",
		"/home/author/work", "loop-sessions", "feature/assembly", started, ended,
		true, 7, 21, 2, 1,
		"bind the packages together", []string{"1.4.0", "1.5.0"},
		int64(1200), int64(340), int64(9000), int64(450),
		1.25, []byte(`{"email":3}`), ingested, updated,
	}

	db := &apiFakeDB{results: []apiFakeResult{{
		match: "ORDER BY s.started_at DESC",
		// Two rows against a limit of one, which is how the store decides a next
		// page exists. It also proves the limit reached the query.
		rows: [][]any{full, apiSessionRow("sess-b", "author@example.com")},
	}}}

	page, err := apiAdapter(db).ListSessions(context.Background(),
		api.Viewer{Email: "author@example.com", Role: api.RoleMember},
		api.SessionFilter{Limit: 1})
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(page.Sessions) != 1 {
		t.Fatalf("got %d sessions, want 1", len(page.Sessions))
	}
	if page.NextCursor == "" {
		t.Error("a truncated page carried no next cursor, so the second page is unreachable")
	}

	want := api.Session{
		SessionID:        "sess-a",
		Email:            "author@example.com",
		DeviceID:         "device-7",
		Source:           "claude_code",
		ParentSessionID:  "sess-parent",
		Cwd:              "/home/author/work",
		Repo:             "loop-sessions",
		GitBranch:        "feature/assembly",
		StartedAt:        started,
		EndedAt:          &ended,
		Ended:            true,
		UserTurns:        7,
		ToolCalls:        21,
		Subagents:        2,
		Errors:           1,
		FirstPrompt:      "bind the packages together",
		HarnessVersions:  []string{"1.4.0", "1.5.0"},
		TokensInput:      1200,
		TokensOutput:     340,
		TokensCacheRead:  9000,
		TokensCacheWrite: 450,
		CostUSD:          1.25,
		Redactions:       map[string]int{"email": 3},
		IngestedAt:       ingested,
		UpdatedAt:        updated,
	}
	if !reflect.DeepEqual(page.Sessions[0], want) {
		t.Errorf("session translation lost or crossed a field\n got: %+v\nwant: %+v", page.Sessions[0], want)
	}
}

// TestAPIStoreDoesNotCrossSessionFilterFields checks the narrowing arrives in
// the order the store reads it. Every field here is a string, so two of them
// swapped still compiles, still returns rows and still looks like a working
// filter; the only symptom is a listing scoped by the wrong thing.
func TestAPIStoreDoesNotCrossSessionFilterFields(t *testing.T) {
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)

	db := &apiFakeDB{}
	_, err := apiAdapter(db).ListSessions(context.Background(),
		api.Viewer{Email: "admin@example.com", Role: api.RoleAdmin},
		api.SessionFilter{
			Query:  "migration",
			Email:  "author@example.com",
			Source: "claude_code",
			Repo:   "loop-sessions",
			From:   from,
			To:     to,
			Limit:  25,
		})
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}

	args := db.find(t, "ORDER BY s.started_at DESC").args
	if len(args) != 12 {
		t.Fatalf("got %d query arguments, want 12", len(args))
	}
	checks := []struct {
		at   int
		name string
		want any
	}{
		{0, "is admin", true},
		{1, "viewer", "admin@example.com"},
		{2, "filter email", "author@example.com"},
		{3, "source", "claude_code"},
		{4, "repo", "loop-sessions"},
		{7, "query", "migration"},
		{11, "limit plus the lookahead row", 26},
	}
	for _, c := range checks {
		if args[c.at] != c.want {
			t.Errorf("%s: argument %d is %#v, want %#v", c.name, c.at+1, args[c.at], c.want)
		}
	}
	for _, c := range []struct {
		at   int
		name string
		want time.Time
	}{{5, "from", from}, {6, "to", to}} {
		got, ok := args[c.at].(*time.Time)
		if !ok || got == nil {
			t.Errorf("%s: argument %d is %#v, want a non-nil time", c.name, c.at+1, args[c.at])
			continue
		}
		if !got.Equal(c.want) {
			t.Errorf("%s: argument %d is %v, want %v", c.name, c.at+1, *got, c.want)
		}
	}
}

// TestAPIStorePrincipalSeparatesAbsentFromDisabled pins the three answers the
// roster can give, because the routes act differently on each: an unknown
// address is somebody who was never enrolled, a disabled row is a colleague
// whose access was withdrawn, and an error is neither. Collapsing the first two
// would either lock out the fleet or let a revoked colleague keep reading.
func TestAPIStorePrincipalSeparatesAbsentFromDisabled(t *testing.T) {
	added := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
	disabled := time.Date(2026, 7, 20, 17, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		db        *apiFakeDB
		want      api.Principal
		wantFound bool
		wantErr   bool
	}{
		{
			name: "an enrolled admin comes back with their role",
			db: &apiFakeDB{results: []apiFakeResult{{
				match: "FROM principals",
				rows:  [][]any{{"admin@example.com", "admin", "An Admin", "root@example.com", added, nil}},
			}}},
			want: api.Principal{
				Email: "admin@example.com", Role: api.RoleAdmin, DisplayName: "An Admin",
			},
			wantFound: true,
		},
		{
			name: "a disabled row keeps its role and reports disabled",
			db: &apiFakeDB{results: []apiFakeResult{{
				match: "FROM principals",
				rows:  [][]any{{"gone@example.com", "member", "Departed", "root@example.com", added, disabled}},
			}}},
			want: api.Principal{
				Email: "gone@example.com", Role: api.RoleMember, DisplayName: "Departed", Disabled: true,
			},
			wantFound: true,
		},
		{
			name:      "an address on nobody's roster is absent, not an error",
			db:        &apiFakeDB{},
			wantFound: false,
		},
		{
			name:    "a failed lookup is an error, not an absence",
			db:      &apiFakeDB{fail: errors.New("statement timeout")},
			wantErr: true,
		},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, found, err := apiAdapter(tc.db).Principal(ctx, "someone@example.com")
			if tc.wantErr {
				if err == nil {
					t.Fatal("a failed lookup was reported as a clean absence")
				}
				if found {
					t.Error("a failed lookup reported a principal as found")
				}
				return
			}
			if err != nil {
				t.Fatalf("principal: %v", err)
			}
			if found != tc.wantFound {
				t.Fatalf("found is %v, want %v", found, tc.wantFound)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestAPIStoreCarriesEveryEventFieldAcross does for the transcript what the
// session test does for the rollup. Body is checked as bytes: it is the scrubbed
// event as it was delivered, and a reader looking at a re-rendered copy of it is
// looking at this server's opinion rather than at what arrived.
func TestAPIStoreCarriesEveryEventFieldAcross(t *testing.T) {
	occurred := time.Date(2026, 7, 1, 10, 15, 0, 0, time.UTC)
	ingested := time.Date(2026, 7, 1, 10, 15, 2, 0, time.UTC)

	db := &apiFakeDB{
		sessions: map[string]string{"sess-a": "author@example.com"},
		results: []apiFakeResult{{
			match: "FROM events",
			rows: [][]any{{
				"evt-1", "sess-a", "author@example.com", int64(42), "assistant_message", "agent",
				occurred, ingested, "agent-3", "wf-9", "claude-opus-5", "Bash",
				[]byte(`{"text":"ok"}`),
			}},
		}},
	}

	page, err := apiAdapter(db).GetEvents(context.Background(),
		api.Viewer{Email: "author@example.com", Role: api.RoleMember},
		"sess-a", api.EventRange{Limit: 10})
	if err != nil {
		t.Fatalf("get events: %v", err)
	}
	if len(page.Events) != 1 {
		t.Fatalf("got %d events, want 1", len(page.Events))
	}
	want := api.StoredEvent{
		ID:         "evt-1",
		SessionID:  "sess-a",
		Email:      "author@example.com",
		Seq:        42,
		Type:       "assistant_message",
		Origin:     "agent",
		OccurredAt: occurred,
		IngestedAt: ingested,
		AgentID:    "agent-3",
		WorkflowID: "wf-9",
		Model:      "claude-opus-5",
		ToolName:   "Bash",
		Body:       []byte(`{"text":"ok"}`),
	}
	if !reflect.DeepEqual(page.Events[0], want) {
		t.Errorf("event translation lost or crossed a field\n got: %+v\nwant: %+v", page.Events[0], want)
	}
}

// TestAPIStoreKeepsNoEventBoundApartFromAZeroBound defends a distinction that
// has no symptom when it breaks. Both packages document a nil AfterSeq as "start
// at the beginning" and a zero one as "after event zero", and the backfill
// walker numbers a session's events from zero, so flattening nil into zero hides
// the first event of every imported session and the page still looks right.
func TestAPIStoreKeepsNoEventBoundApartFromAZeroBound(t *testing.T) {
	zero := int64(0)
	seq := int64(99)

	cases := []struct {
		name string
		give *int64
		want *int64
	}{
		{"no bound stays absent", nil, nil},
		{"a zero bound survives as a bound", &zero, &zero},
		{"a real bound is carried", &seq, &seq},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &apiFakeDB{sessions: map[string]string{"sess-a": "author@example.com"}}
			_, err := apiAdapter(db).GetEvents(ctx,
				api.Viewer{Email: "author@example.com", Role: api.RoleMember},
				"sess-a", api.EventRange{AfterSeq: tc.give, Limit: 10})
			if err != nil {
				t.Fatalf("get events: %v", err)
			}
			got, _ := db.find(t, "FROM events").args[1].(*int64)
			switch {
			case tc.want == nil && got != nil:
				t.Errorf("no bound became a bound of %d", *got)
			case tc.want != nil && got == nil:
				t.Errorf("a bound of %d was dropped", *tc.want)
			case tc.want != nil && got != nil && *got != *tc.want:
				t.Errorf("bound is %d, want %d", *got, *tc.want)
			}
		})
	}
}

// TestAPIStoreCarriesEverySearchHitAcross covers the ranked page, including the
// two counters. Capped is what lets the dashboard say "500+" honestly, and a
// dropped Capped turns a truncated count into a stated total.
func TestAPIStoreCarriesEverySearchHitAcross(t *testing.T) {
	occurred := time.Date(2026, 7, 3, 14, 0, 0, 0, time.UTC)
	db := &apiFakeDB{results: []apiFakeResult{{
		match: "ts_headline",
		rows: [][]any{{
			"evt-7", "sess-b", "colleague@example.com", int64(11), "user", "human",
			occurred, "…the <b>migration</b> ran…", 0.42, int64(3),
		}},
	}}}

	res, err := apiAdapter(db).SearchMessages(context.Background(),
		api.Viewer{Email: "member@example.com", Role: api.RoleAdmin},
		api.SearchFilter{Query: "migration", Limit: 10})
	if err != nil {
		t.Fatalf("search messages: %v", err)
	}
	want := api.SearchResult{
		Hits: []api.Hit{{
			EventID:    "evt-7",
			SessionID:  "sess-b",
			Email:      "colleague@example.com",
			Seq:        11,
			Role:       "user",
			OccurredAt: occurred,
			Snippet:    "…the <b>migration</b> ran…",
			Rank:       0.42,
		}},
		Candidates: 3,
		Capped:     false,
	}
	if !reflect.DeepEqual(res, want) {
		t.Errorf("search translation lost or crossed a field\n got: %+v\nwant: %+v", res, want)
	}
}

// TestAPIStoreCarriesShareGrantsAcross checks the grant shape and the empty
// case. A session nobody has shared has to come back as no grants rather than as
// an error: the routes render the list on the session page, and the port says
// plainly that a viewer who may not see the grants sees an empty list.
func TestAPIStoreCarriesShareGrantsAcross(t *testing.T) {
	created := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	expires := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name string
		rows [][]any
		want []api.Share
	}{
		{
			name: "a live grant carries every field",
			rows: [][]any{{
				"share-1", "sess-a", "author@example.com", "grantee@example.com", "share-token-1",
				created, expires, nil,
			}},
			want: []api.Share{{
				ID:        "share-1",
				SessionID: "sess-a",
				CreatedBy: "author@example.com",
				Grantee:   "grantee@example.com",
				Token:     "share-token-1",
				CreatedAt: created,
				ExpiresAt: &expires,
			}},
		},
		{
			name: "no grants is an empty list and not an error",
			rows: nil,
			want: nil,
		},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &apiFakeDB{results: []apiFakeResult{{match: "FROM shares sh", rows: tc.rows}}}
			got, err := apiAdapter(db).ListShares(ctx,
				api.Viewer{Email: "author@example.com", Role: api.RoleMember}, "sess-a")
			if err != nil {
				t.Fatalf("list shares: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("share translation lost or crossed a field\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

// TestAPIStoreCarriesTheViewerRoleUnchanged makes sure an admin arrives at the
// store as an admin and a member as a member. The two packages spell the roles
// with the same strings and the conversion is a cast, so nothing but a test
// notices if one of those spellings ever changes: a member silently promoted
// reads the whole fleet, and an admin silently demoted stops being able to
// answer a question the admin page exists to answer.
func TestAPIStoreCarriesTheViewerRoleUnchanged(t *testing.T) {
	cases := []struct {
		name        string
		role        api.Role
		wantIsAdmin bool
	}{
		{"an admin reads as an admin", api.RoleAdmin, true},
		{"a member reads as a member", api.RoleMember, false},
	}

	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &apiFakeDB{}
			_, err := apiAdapter(db).ListSessions(ctx,
				api.Viewer{Email: "someone@example.com", Role: tc.role}, api.SessionFilter{})
			if err != nil {
				t.Fatalf("list sessions: %v", err)
			}
			got, _ := db.find(t, "ORDER BY s.started_at DESC").args[0].(bool)
			if got != tc.wantIsAdmin {
				t.Errorf("the store was told is-admin=%v, want %v", got, tc.wantIsAdmin)
			}
		})
	}
}
