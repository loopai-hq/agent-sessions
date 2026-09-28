package app

// These tests drive the dashboard adapter through the real *store.Store over a
// fake connection rather than over a fake store. The properties worth defending
// here live in the seam between the two: the store collapses "not yours" into
// "no rows" inside the SELECT and decides how a read was justified inside the
// transaction it audits in, and this adapter has to keep the first collapsed and
// re-derive the second identically. A hand-written fake store would let the test
// author decide what the store says, which is the one thing these tests must not
// be free to assume.
//
// Every helper is named with a wd prefix. Package app carries one test file per
// adapter and each brings its own fixtures, so a bare fakeDB would collide with
// the next adapter's.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/server/store"
	"github.com/loopai-hq/agent-sessions/server/web"
)

// ---------------------------------------------------------------------------
// A store.DB modelling the sliver of the schema the dashboard reads
// ---------------------------------------------------------------------------

var (
	// wdNow is the instant coverage and share expiry are measured against. Fixed
	// so the boundary cases can be stated exactly instead of being waited for.
	wdNow = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	// wdStart is a plausible session start, three hours before wdNow.
	wdStart = time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC)
)

const (
	wdOwner     = "owner@example.com"
	wdColleague = "colleague@example.com"
	wdAdmin     = "admin@example.com"
	wdStranger  = "stranger@example.com"
)

type wdCall struct {
	sql  string
	args []any
}

// wdShare is a grant as the shares table holds it, revoked and expired ones
// included: the store's ListShares does not filter either, and the narrowing is
// the adapter's job.
type wdShare struct {
	id        string
	token     string
	sessionID string
	createdBy string
	grantee   string
	expiresAt *time.Time
	revokedAt *time.Time
}

// live is the fake's copy of the store's grant predicate, in Go rather than SQL,
// so a fixture can hold a dead grant that is still a row.
func (s wdShare) live(at time.Time) bool {
	return s.revokedAt == nil && (s.expiresAt == nil || s.expiresAt.After(at))
}

// wdScript answers one statement the fake does not model, matched on a substring
// of its SQL.
type wdScript struct {
	match string
	rows  [][]any
	err   error
}

// wdFakeDB answers the statements the store issues on the dashboard's behalf.
//
// Sessions and shares are modelled rather than scripted because the central test
// needs a fixture in which a session genuinely exists and is genuinely invisible.
// A fake that answered "no rows" to both would agree that denial and absence look
// alike without ever having been asked two different questions.
type wdFakeDB struct {
	sessions map[string]string
	shares   []wdShare
	scripts  []wdScript

	// fail stands in for the class of failure that must never be mistaken for
	// absence: the pool is gone, the statement timed out, the column is not there.
	// failOn narrows it to statements naming that fragment, which is how a single
	// read inside a multi-statement method is made to fail on its own.
	fail     error
	failOn   string
	beginErr error

	calls []wdCall
}

func (f *wdFakeDB) record(sql string, args []any) {
	f.calls = append(f.calls, wdCall{sql: sql, args: args})
}

func (f *wdFakeDB) failing(sql string) bool {
	return f.fail != nil && (f.failOn == "" || strings.Contains(sql, f.failOn))
}

func (f *wdFakeDB) scripted(sql string) (wdScript, bool) {
	for _, s := range f.scripts {
		if strings.Contains(sql, s.match) {
			return s, true
		}
	}
	return wdScript{}, false
}

func (f *wdFakeDB) Query(_ context.Context, sql string, args ...any) (store.Rows, error) {
	f.record(sql, args)
	if f.failing(sql) {
		return nil, f.fail
	}
	// Matched on the join rather than on "FROM shares sh", which also appears
	// inside the authorization predicate of every session and message read.
	if strings.Contains(sql, "JOIN sessions s ON s.session_id = sh.session_id") {
		return &wdRows{rows: f.listShares(args)}, nil
	}
	if s, ok := f.scripted(sql); ok {
		if s.err != nil {
			return nil, s.err
		}
		return &wdRows{rows: s.rows}, nil
	}
	return &wdRows{}, nil
}

func (f *wdFakeDB) QueryRow(_ context.Context, sql string, args ...any) store.Row {
	f.record(sql, args)
	if f.failing(sql) {
		return wdErrRow{err: f.fail}
	}
	switch {
	case strings.Contains(sql, "WHERE s.session_id = $3"):
		return f.readSession(args)
	case strings.Contains(sql, "WHERE token = $1"):
		return f.readShareByToken(args)
	}
	if s, ok := f.scripted(sql); ok {
		if s.err != nil {
			return wdErrRow{err: s.err}
		}
		if len(s.rows) > 0 {
			return wdRow{values: s.rows[0]}
		}
	}
	return wdErrRow{err: pgx.ErrNoRows}
}

func (f *wdFakeDB) Exec(_ context.Context, sql string, args ...any) (int64, error) {
	f.record(sql, args)
	if f.failing(sql) {
		return 0, f.fail
	}
	return 1, nil
}

func (f *wdFakeDB) Begin(_ context.Context) (store.Tx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	return &wdTx{db: f}, nil
}

// visible is the fake's copy of the store's authorization predicate. It exists so
// a fixture can hold a session the viewer may not read; the store's own copy is
// what is under test everywhere else.
func (f *wdFakeDB) visible(sessionID, owner, viewer string, admin bool) bool {
	if admin || owner == viewer {
		return true
	}
	for _, sh := range f.shares {
		if sh.sessionID != sessionID || !sh.live(wdNow) {
			continue
		}
		if sh.grantee == "" || sh.grantee == viewer {
			return true
		}
	}
	return false
}

func (f *wdFakeDB) readSession(args []any) store.Row {
	admin, _ := args[0].(bool)
	viewer, _ := args[1].(string)
	id, _ := args[2].(string)
	owner, ok := f.sessions[id]
	if !ok || !f.visible(id, owner, viewer, admin) {
		return wdErrRow{err: pgx.ErrNoRows}
	}
	if s, ok := f.scripted("SESSION:" + id); ok && len(s.rows) > 0 {
		return wdRow{values: s.rows[0]}
	}
	return wdRow{values: wdSessionRow(id, owner)}
}

func (f *wdFakeDB) readShareByToken(args []any) store.Row {
	token, _ := args[0].(string)
	viewer, _ := args[1].(string)
	for _, sh := range f.shares {
		if sh.token != token || !sh.live(wdNow) {
			continue
		}
		if sh.grantee != "" && sh.grantee != viewer {
			continue
		}
		return wdRow{values: []any{sh.id, sh.sessionID, sh.createdBy, wdNullable(sh.grantee), wdStart, sh.expiresAt}}
	}
	return wdErrRow{err: pgx.ErrNoRows}
}

// listShares answers the detail page's grant read with every row the SQL would
// return, dead grants included.
func (f *wdFakeDB) listShares(args []any) [][]any {
	sessionID, _ := args[0].(string)
	viewer, _ := args[1].(string)
	admin, _ := args[2].(bool)
	if !admin && f.sessions[sessionID] != viewer {
		return nil
	}
	var out [][]any
	for _, sh := range f.shares {
		if sh.sessionID != sessionID {
			continue
		}
		out = append(out, []any{
			sh.id, sh.sessionID, sh.createdBy, wdNullable(sh.grantee), wdNullable(sh.token),
			wdStart, sh.expiresAt, sh.revokedAt,
		})
	}
	return out
}

type wdTx struct {
	db   *wdFakeDB
	done bool
}

func (t *wdTx) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return t.db.Query(ctx, sql, args...)
}

func (t *wdTx) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	return t.db.QueryRow(ctx, sql, args...)
}

func (t *wdTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return t.db.Exec(ctx, sql, args...)
}

func (t *wdTx) Commit(context.Context) error {
	if t.done {
		return errors.New("commit after the transaction finished")
	}
	t.done = true
	return nil
}

func (t *wdTx) Rollback(context.Context) error {
	t.done = true
	return nil
}

type wdRows struct {
	rows [][]any
	i    int
}

func (r *wdRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *wdRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("scan outside a row")
	}
	return wdAssign(r.rows[r.i-1], dest)
}

func (r *wdRows) Err() error { return nil }
func (r *wdRows) Close()     {}

type wdRow struct{ values []any }

func (r wdRow) Scan(dest ...any) error { return wdAssign(r.values, dest) }

type wdErrRow struct{ err error }

func (e wdErrRow) Scan(...any) error { return e.err }

// wdAssign copies a scripted row into the store's scan destinations, widening the
// way pgx does: a value lands in a pointer destination as a new pointer and a nil
// lands as the zero value, so a fixture can express a NULL column without knowing
// which of the store's fields happens to be a pointer.
func wdAssign(src, dest []any) error {
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

// wdNullable turns the empty string into a SQL NULL, which is how the store's
// nullable text columns actually arrive.
func wdNullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// wdSessionRow is the minimum a session projection needs to scan. Tests that care
// about a particular field script their own row.
func wdSessionRow(sessionID, owner string) []any {
	return []any{
		sessionID, owner, nil, "claude_code", "user", nil,
		nil, nil, nil, wdStart, nil,
		false, 0, 0, 0, 0,
		nil, nil,
		int64(0), int64(0), int64(0), int64(0),
		float64(0), nil, wdStart, wdStart,
	}
}

func wdViewer(email string, admin bool) web.Viewer {
	return web.Viewer{Email: email, Name: "", Admin: admin}
}

// wdAdapter builds the adapter the way the composition root does.
func wdAdapter(db *wdFakeDB) web.Data {
	// The pricer is nil because nothing the dashboard reads prices anything; a
	// stub here would be a second thing to keep in step with the real one.
	return NewWebData(store.NewWithDB(db, nil))
}

// wdAdapterAt is the same adapter with the clock held still, for the two surfaces
// whose answer is a function of the current instant.
func wdAdapterAt(db *wdFakeDB, at time.Time) web.Data {
	return webData{s: store.NewWithDB(db, nil), now: func() time.Time { return at }}
}

func (f *wdFakeDB) find(t *testing.T, match string) wdCall {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c.sql, match) {
			return c
		}
	}
	t.Fatalf("no statement containing %q was issued; issued %d statements", match, len(f.calls))
	return wdCall{}
}

func (f *wdFakeDB) issued(match string) bool {
	for _, c := range f.calls {
		if strings.Contains(c.sql, match) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// The error-collapse property
// ---------------------------------------------------------------------------

// TestWebDataReportsDenialAndAbsenceAsOneError pins the rule the dashboard is
// built around: a session that exists and is not this viewer's to see, and a
// session that does not exist at all, leave this adapter as the same error value
// with the same text. Anything that separates them tells the viewer that a named
// colleague ran something at a particular time, which is exactly the fact they
// were not allowed to learn.
//
// Every case runs a third fixture in which the read is permitted, because two
// matching refusals could otherwise both be an artefact of a fixture that refuses
// everything, and the test would keep passing against an adapter that had stopped
// working altogether.
func TestWebDataReportsDenialAndAbsenceAsOneError(t *testing.T) {
	newDB := func() *wdFakeDB {
		return &wdFakeDB{
			sessions: map[string]string{"s-colleague": wdColleague, "s-own": wdOwner},
			shares: []wdShare{
				{id: "sh-1", token: "live-token", sessionID: "s-own", createdBy: wdOwner},
			},
		}
	}

	cases := []struct {
		name string
		// call performs the read. denied names a real row the viewer may not see,
		// missing names nothing at all, and allowed must succeed.
		call    func(web.Data, string) error
		denied  string
		missing string
		allowed string
	}{
		{
			name: "session rollup",
			call: func(d web.Data, id string) error {
				_, err := d.Session(context.Background(), wdViewer(wdOwner, false), id)
				return err
			},
			denied:  "s-colleague",
			missing: "s-nothing",
			allowed: "s-own",
		},
		{
			name: "transcript window",
			call: func(d web.Data, id string) error {
				_, err := d.Events(context.Background(), wdViewer(wdOwner, false), id, web.EventQuery{Limit: 10})
				return err
			},
			denied:  "s-colleague",
			missing: "s-nothing",
			allowed: "s-own",
		},
		{
			name: "share link",
			call: func(d web.Data, token string) error {
				_, err := d.ResolveShare(context.Background(), wdViewer(wdOwner, false), token)
				return err
			},
			// A revoked link and a token nobody ever minted are the share-shaped
			// spelling of the same pair: one names a grant that exists, the other
			// names nothing.
			denied:  "dead-token",
			missing: "never-minted",
			allowed: "live-token",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newDB()
			db.shares = append(db.shares, wdShare{
				id: "sh-dead", token: "dead-token", sessionID: "s-colleague",
				createdBy: wdColleague, revokedAt: &wdStart,
			})
			d := wdAdapter(db)

			deniedErr := tc.call(d, tc.denied)
			missingErr := tc.call(d, tc.missing)

			if !errors.Is(deniedErr, web.ErrNotFound) {
				t.Fatalf("denial produced %v, want web.ErrNotFound", deniedErr)
			}
			if !errors.Is(missingErr, web.ErrNotFound) {
				t.Fatalf("absence produced %v, want web.ErrNotFound", missingErr)
			}
			// Identity, not merely errors.Is: a wrapped sentinel carries text, and
			// text is where the two cases would start to differ.
			if deniedErr != missingErr {
				t.Fatalf("denial and absence are distinguishable: %#v vs %#v", deniedErr, missingErr)
			}
			if deniedErr.Error() != missingErr.Error() {
				t.Fatalf("denial reads %q and absence reads %q", deniedErr, missingErr)
			}
			if err := tc.call(d, tc.allowed); err != nil {
				t.Fatalf("the permitted read failed, so the refusals prove nothing: %v", err)
			}
		})
	}
}

// TestWebDataNeverWidensAFailureIntoAbsence covers the other direction. A pool
// that has gone away must not reach the dashboard as "no such session" or as an
// empty page: a member whose colleagues' sessions all vanished for an hour would
// have no way to tell that from a quiet week, and the outage would be invisible
// on the only page anyone was looking at.
func TestWebDataNeverWidensAFailureIntoAbsence(t *testing.T) {
	boom := errors.New("connection refused")

	cases := []struct {
		name string
		call func(web.Data) error
	}{
		{"list sessions", func(d web.Data) error {
			p, err := d.ListSessions(context.Background(), wdViewer(wdOwner, false), web.SessionQuery{})
			if err == nil && len(p.Sessions) == 0 {
				return errors.New("returned an empty page and no error")
			}
			return err
		}},
		{"session rollup", func(d web.Data) error {
			_, err := d.Session(context.Background(), wdViewer(wdOwner, false), "s-own")
			return err
		}},
		{"transcript window", func(d web.Data) error {
			_, err := d.Events(context.Background(), wdViewer(wdOwner, false), "s-own", web.EventQuery{Limit: 5})
			return err
		}},
		{"search", func(d web.Data) error {
			r, err := d.Search(context.Background(), wdViewer(wdOwner, false), web.SearchQuery{Q: "deploy"})
			if err == nil && len(r.Hits) == 0 {
				return errors.New("returned no hits and no error")
			}
			return err
		}},
		{"share link", func(d web.Data) error {
			_, err := d.ResolveShare(context.Background(), wdViewer(wdOwner, false), "live-token")
			return err
		}},
		{"principals", func(d web.Data) error {
			ps, err := d.Principals(context.Background(), wdViewer(wdAdmin, true))
			if err == nil && len(ps) == 0 {
				return errors.New("returned an empty roster and no error")
			}
			return err
		}},
		{"set principal", func(d web.Data) error {
			_, err := d.SetPrincipal(context.Background(), wdViewer(wdAdmin, true),
				web.PrincipalUpdate{Email: wdColleague, Role: web.RoleMember})
			return err
		}},
		{"fleet", func(d web.Data) error {
			_, err := d.Fleet(context.Background(), wdViewer(wdAdmin, true))
			return err
		}},
		{"access log", func(d web.Data) error {
			rows, err := d.AccessLog(context.Background(), wdViewer(wdAdmin, true), web.AccessQuery{})
			if err == nil && len(rows) == 0 {
				return errors.New("returned an empty log and no error")
			}
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &wdFakeDB{
				sessions: map[string]string{"s-own": wdOwner},
				shares:   []wdShare{{id: "sh-1", token: "live-token", sessionID: "s-own", createdBy: wdOwner}},
				fail:     boom,
			}
			err := tc.call(wdAdapter(db))
			if err == nil {
				t.Fatal("a failed read reported success")
			}
			if !errors.Is(err, boom) {
				t.Fatalf("the cause was dropped: %v", err)
			}
			if errors.Is(err, web.ErrNotFound) || errors.Is(err, web.ErrDenied) {
				t.Fatalf("a failure was widened into a refusal: %v", err)
			}
			var ue web.UserError
			if errors.As(err, &ue) {
				t.Fatalf("a failure was rendered as advice to the operator: %q", ue.Message)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Translation
// ---------------------------------------------------------------------------

// TestWebDataCarriesEverySessionFieldOntoThePage checks the field-for-field half
// of the adapter, including the two conversions that are not copies: the stored
// end time is a pointer and the page's is a value, and a session that never ended
// has to arrive as the zero time rather than as a distant date, because the port's
// Duration subtracts from it.
func TestWebDataCarriesEverySessionFieldOntoThePage(t *testing.T) {
	ended := wdStart.Add(90 * time.Minute)
	full := []any{
		"s-full", wdOwner, "device-1", "claude_code", "automation", "s-parent",
		"/w/backend", "loop/backend", "main", wdStart, ended,
		true, 12, 34, 3, 2,
		"fix the ingest lag", []string{"1.2.3", "1.3.0"},
		int64(1000), int64(2000), int64(3000), int64(4000),
		12.5, []byte(`{"api_key":2}`), wdStart.Add(2 * time.Hour), wdStart.Add(3 * time.Hour),
	}

	cases := []struct {
		name string
		row  []any
		want web.Session
		dur  time.Duration
	}{
		{
			name: "every column present",
			row:  full,
			want: web.Session{
				ID: "s-full", ParentID: "s-parent", Email: wdOwner, Name: "Owner Name",
				Source: "claude_code", Type: "automation", Repo: "loop/backend", Cwd: "/w/backend", Branch: "main",
				StartedAt: wdStart, EndedAt: ended, IngestedAt: wdStart.Add(2 * time.Hour), Ended: true,
				UserTurns: 12, ToolCalls: 34, Subagents: 3, Errors: 2,
				FirstPrompt: "fix the ingest lag", HarnessVersions: []string{"1.2.3", "1.3.0"},
				TokensInput: 1000, TokensOutput: 2000, TokensCacheRead: 3000, TokensCacheWrite: 4000,
				CostUSD: 12.5, Redactions: map[string]int{"api_key": 2},
			},
			dur: 90 * time.Minute,
		},
		{
			name: "a session that has not ended",
			row: []any{
				"s-full", wdOwner, nil, "codex", "user", nil,
				nil, nil, nil, wdStart, nil,
				false, 1, 0, 0, 0,
				nil, nil,
				int64(0), int64(0), int64(0), int64(0),
				0.0, nil, wdStart, wdStart,
			},
			want: web.Session{
				ID: "s-full", Email: wdOwner, Name: "Owner Name", Source: "codex", Type: "user",
				StartedAt: wdStart, IngestedAt: wdStart, UserTurns: 1,
			},
			// Zero rather than the span from the epoch to the start, which is what
			// a nil end time rendered as "now" or as time.Time{} minus start would
			// put on the page.
			dur: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &wdFakeDB{
				sessions: map[string]string{"s-full": wdOwner},
				scripts: []wdScript{
					{match: "SESSION:s-full", rows: [][]any{tc.row}},
					{match: "FROM principals WHERE email = $1", rows: [][]any{
						{wdOwner, "member", "Owner Name", nil, wdStart, nil},
					}},
				},
			}
			det, err := wdAdapter(db).Session(context.Background(), wdViewer(wdOwner, false), "s-full")
			if err != nil {
				t.Fatalf("read the session: %v", err)
			}
			if !reflect.DeepEqual(det.Session, tc.want) {
				t.Fatalf("session translated to\n %#v\nwant\n %#v", det.Session, tc.want)
			}
			if got := det.Session.Duration(); got != tc.dur {
				t.Fatalf("duration %s, want %s", got, tc.dur)
			}
		})
	}
}

// TestWebDataNamesTheReadTheWayTheAuditRowDoes defends the one value on the page
// that this adapter invents. The store settles how a read was justified inside
// the transaction it audits in and does not hand the answer back, so the banner
// telling a reader their read was recorded is re-derived here. If the two ever
// disagreed the banner would be describing a log entry that says something else.
//
// The check is against the store's own audit write rather than against a constant,
// so a change to either side's precedence fails this test.
func TestWebDataNamesTheReadTheWayTheAuditRowDoes(t *testing.T) {
	cases := []struct {
		name    string
		viewer  web.Viewer
		session string
		owner   string
		want    string
	}{
		{"own work", wdViewer(wdOwner, false), "s-own", wdOwner, store.AccessViaOwn},
		{"an admin reading a colleague", wdViewer(wdAdmin, true), "s-colleague", wdColleague, store.AccessViaAdmin},
		{"a member holding a share", wdViewer(wdStranger, false), "s-shared", wdOwner, store.AccessViaShare},
		// An admin who also holds a share is still an admin: the store's
		// precedence puts the role ahead of the grant, and the banner has to say
		// the same thing the audit row does.
		{"an admin who also holds a share", wdViewer(wdAdmin, true), "s-shared", wdOwner, store.AccessViaAdmin},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &wdFakeDB{
				sessions: map[string]string{
					"s-own": wdOwner, "s-colleague": wdColleague, "s-shared": wdOwner,
				},
				shares: []wdShare{{id: "sh-1", token: "tok", sessionID: "s-shared", createdBy: wdOwner}},
			}
			det, err := wdAdapter(db).Session(context.Background(), tc.viewer, tc.session)
			if err != nil {
				t.Fatalf("read the session: %v", err)
			}
			if det.Via != tc.want {
				t.Fatalf("banner says via=%q, want %q", det.Via, tc.want)
			}

			if tc.want == store.AccessViaOwn {
				// Reading your own work is not audited, so there is no row to agree
				// with. Asserting the absence keeps the banner honest in the one
				// case where it is not backed by a log entry.
				if db.issued("INSERT INTO access_log") {
					t.Fatal("a read of the viewer's own session wrote an audit row")
				}
				return
			}
			call := db.find(t, "INSERT INTO access_log")
			vias, ok := call.args[3].([]string)
			if !ok || len(vias) != 1 {
				t.Fatalf("audit row carried via=%#v", call.args[3])
			}
			if vias[0] != det.Via {
				t.Fatalf("audit row says via=%q and the banner says %q", vias[0], det.Via)
			}
		})
	}
}

// TestWebDataListsOnlyGrantsThatWouldStillLetSomebodyIn pins the narrowing the
// store does not do. ListShares returns revoked and expired grants because the
// JSON API has to be able to explain a link that used to work, and web.Share has
// no field to say a grant is dead, so a dead one listed under "active shares"
// reads to an owner as a live link on their session.
//
// The expiry boundary is the store's: "expires_at > now()", so a grant expiring at
// this exact instant is already refused there and must be dropped here too.
func TestWebDataListsOnlyGrantsThatWouldStillLetSomebodyIn(t *testing.T) {
	past := wdNow.Add(-time.Hour)
	future := wdNow.Add(time.Hour)
	atNow := wdNow

	db := &wdFakeDB{
		sessions: map[string]string{"s-own": wdOwner},
		shares: []wdShare{
			{id: "live-forever", token: "t1", sessionID: "s-own", createdBy: wdOwner},
			{id: "live-until", token: "t2", sessionID: "s-own", createdBy: wdOwner, expiresAt: &future},
			{id: "expired", token: "t3", sessionID: "s-own", createdBy: wdOwner, expiresAt: &past},
			{id: "expiring-now", token: "t4", sessionID: "s-own", createdBy: wdOwner, expiresAt: &atNow},
			{id: "revoked", token: "t5", sessionID: "s-own", createdBy: wdOwner},
			{id: "revoked-but-unexpired", token: "t6", sessionID: "s-own", createdBy: wdOwner, expiresAt: &future},
		},
	}
	db.shares[4].revokedAt = &past
	db.shares[5].revokedAt = &past

	det, err := wdAdapterAt(db, wdNow).Session(context.Background(), wdViewer(wdOwner, false), "s-own")
	if err != nil {
		t.Fatalf("read the session: %v", err)
	}

	var got []string
	for _, sh := range det.Shares {
		got = append(got, sh.ID)
	}
	want := []string{"live-forever", "live-until"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("live grants %v, want %v", got, want)
	}
	if det.Shares[0].ExpiresAt != (time.Time{}) {
		t.Fatalf("a grant with no expiry carried %v, want the zero time", det.Shares[0].ExpiresAt)
	}
	if det.Shares[1].ExpiresAt != future {
		t.Fatalf("expiry %v, want %v", det.Shares[1].ExpiresAt, future)
	}
	// The link secret must not cross: web.Share has no field for it and a token in
	// a rendered page is a credential in somebody's browser history.
	rendered := fmt.Sprintf("%#v", det.Shares)
	for _, tok := range []string{"t1", "t2"} {
		if strings.Contains(rendered, `"`+tok+`"`) {
			t.Fatalf("a share token reached the page: %s", rendered)
		}
	}
}

// TestWebDataTranscriptWindowLetsTheIndexedColumnsWin covers the rebuild of a
// delivered event. The body is the bytes the laptop sent and the columns beside it
// are what the server indexed, ordered and authorised on; where the two disagree
// the columns have to win, or a body claiming another seq renders one event inside
// another's ordering and a body claiming another session id puts a row from
// somebody else's transcript on this page.
func TestWebDataTranscriptWindowLetsTheIndexedColumnsWin(t *testing.T) {
	body := []byte(`{
		"id":"claimed-id","session_id":"s-somebody-else","seq":9999,
		"type":"session_ended","origin":"hook","agent_id":"claimed-agent",
		"model":"claimed-model","source":"claude_code","text":"the body survives",
		"usage":{"input_tokens":11}
	}`)
	db := &wdFakeDB{
		sessions: map[string]string{"s-own": wdOwner},
		scripts: []wdScript{{match: "FROM events", rows: [][]any{{
			"stored-id", "s-own", wdOwner, int64(7), "user_prompt", "transcript",
			wdStart, wdStart, "stored-agent", "wf-1", "stored-model", "Bash", body,
		}}}},
	}

	page, err := wdAdapter(db).Events(context.Background(), wdViewer(wdOwner, false), "s-own", web.EventQuery{Limit: 10})
	if err != nil {
		t.Fatalf("read the window: %v", err)
	}
	if len(page.Events) != 1 {
		t.Fatalf("got %d events, want 1", len(page.Events))
	}
	got := page.Events[0]

	want := map[string]struct{ got, want any }{
		"id":          {got.ID, "stored-id"},
		"session id":  {got.SessionID, "s-own"},
		"seq":         {got.Seq, int64(7)},
		"type":        {got.Type, event.Type("user_prompt")},
		"origin":      {got.Origin, event.Origin("transcript")},
		"occurred at": {got.OccurredAt, wdStart},
		"agent id":    {got.AgentID, "stored-agent"},
		"workflow id": {got.WorkflowID, "wf-1"},
		"model":       {got.Model, "stored-model"},
		// Nothing indexed the harness's own source or the text, so those are the
		// body's to keep.
		"source": {got.Source, event.Source("claude_code")},
		"text":   {got.Text, "the body survives"},
	}
	for field, pair := range want {
		if !reflect.DeepEqual(pair.got, pair.want) {
			t.Errorf("%s is %#v, want %#v", field, pair.got, pair.want)
		}
	}
	if got.Usage == nil || got.Usage.InputTokens != 11 {
		t.Errorf("usage from the body was lost: %#v", got.Usage)
	}
}

// TestWebDataUndecodableEventFailsTheWindowRatherThanVanishing defends the choice
// to fail loudly. Dropping the record instead leaves a page that looks complete,
// is in seq order, and has an event missing from the middle with nothing on it to
// say so.
func TestWebDataUndecodableEventFailsTheWindowRatherThanVanishing(t *testing.T) {
	row := func(id string, seq int64, body string) []any {
		return []any{id, "s-own", wdOwner, seq, "user_prompt", "hook",
			wdStart, wdStart, nil, nil, nil, nil, []byte(body)}
	}
	db := &wdFakeDB{
		sessions: map[string]string{"s-own": wdOwner},
		scripts: []wdScript{{match: "FROM events", rows: [][]any{
			row("e-1", 1, `{"text":"fine"}`),
			row("e-2", 2, `{"text":`),
			row("e-3", 3, `{"text":"also fine"}`),
		}}},
	}

	page, err := wdAdapter(db).Events(context.Background(), wdViewer(wdOwner, false), "s-own", web.EventQuery{Limit: 10})
	if err == nil {
		t.Fatalf("a corrupt event was rendered as a shorter page of %d events", len(page.Events))
	}
	if errors.Is(err, web.ErrNotFound) {
		t.Fatalf("a decode failure was reported as absence: %v", err)
	}
	if !strings.Contains(err.Error(), "e-2") {
		t.Fatalf("the failure does not name the event that could not be decoded: %v", err)
	}
}

// TestWebDataSearchCarriesEveryHitAndTheCapFlag checks the results page. Capped
// travels because a user whose query matched half the corpus should be told to
// narrow it rather than left believing they saw everything.
func TestWebDataSearchCarriesEveryHitAndTheCapFlag(t *testing.T) {
	hitAt := wdStart.Add(time.Minute)
	db := &wdFakeDB{
		scripts: []wdScript{{match: "FROM messages m", rows: [][]any{
			{"e-1", "s-colleague", wdColleague, int64(4), "user", "", hitAt, "the <b>deploy</b> failed", 0.9, int64(store.SearchCandidateCap)},
		}}},
	}

	res, err := wdAdapter(db).Search(context.Background(), wdViewer(wdAdmin, true), web.SearchQuery{
		Q: "deploy", Email: wdColleague, Source: "claude_code",
		From: wdStart, To: wdNow, Limit: 20,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	want := web.SearchHit{
		Session:    web.Session{ID: "s-colleague", Email: wdColleague},
		EventID:    "e-1",
		Seq:        4,
		Role:       "user",
		OccurredAt: hitAt,
		Text:       "the <b>deploy</b> failed",
	}
	if len(res.Hits) != 1 || !reflect.DeepEqual(res.Hits[0], want) {
		t.Fatalf("hits %#v, want one %#v", res.Hits, want)
	}
	if !res.Capped {
		t.Fatal("a result set at the candidate cap was reported as complete")
	}

	// Every filter the search form offers has to reach the store; one accepted and
	// then dropped produces a wider result set under a narrower heading.
	call := db.find(t, "FROM messages m")
	for i, want := range []any{"deploy", true, wdAdmin, wdColleague} {
		if !reflect.DeepEqual(call.args[i], want) {
			t.Errorf("search argument %d is %#v, want %#v", i, call.args[i], want)
		}
	}
	if got := call.args[6]; got != "claude_code" {
		t.Errorf("source filter reached the store as %#v", got)
	}
}

// ---------------------------------------------------------------------------
// The queries the store cannot answer
// ---------------------------------------------------------------------------

// TestWebDataRefusesAQueryTheStoreCannotAnswer covers the four places where the
// dashboard can express something the store has no way to serve. Each refuses
// rather than answering with the nearest thing it can run, because every
// approximation renders as a normal page with nothing on it to tell the reader
// they are looking at the wrong events: a "page back" link that returns the page
// they are on, a subagent filter that returns the whole session, a repository
// filter that returns every repository, an event page for the wrong event.
//
// The test also asserts that no statement was issued, which is the difference
// between refusing a query and running a wider one and then apologising.
func TestWebDataRefusesAQueryTheStoreCannotAnswer(t *testing.T) {
	after := int64(10)

	cases := []struct {
		name string
		call func(web.Data) error
		// names is a fragment of the missing capability the failure has to name, so
		// whoever hits it in a log learns what to add rather than that something
		// broke.
		names string
	}{
		{
			name: "one event by id",
			call: func(d web.Data) error {
				_, err := d.Event(context.Background(), wdViewer(wdOwner, false), "s-own", "e-1")
				return err
			},
			names: "(session_id, id)",
		},
		{
			name: "search narrowed to a repository",
			call: func(d web.Data) error {
				_, err := d.Search(context.Background(), wdViewer(wdOwner, false),
					web.SearchQuery{Q: "deploy", Repo: "loop/backend"})
				return err
			},
			names: "store.SearchFilter",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &wdFakeDB{sessions: map[string]string{"s-own": wdOwner}}
			err := tc.call(wdAdapter(db))
			if err == nil {
				t.Fatal("a query the store cannot answer was answered anyway")
			}
			if !errors.Is(err, errWebUnsupported) {
				t.Fatalf("refusal %v is not marked as an unsupported query", err)
			}
			if errors.Is(err, web.ErrNotFound) {
				// Absence would render as "no session at this address" on a page
				// the reader just clicked a link from.
				t.Fatalf("a missing capability was reported as a missing session: %v", err)
			}
			if !strings.Contains(err.Error(), tc.names) {
				t.Fatalf("refusal does not name what is missing (%q): %v", tc.names, err)
			}
			if len(db.calls) != 0 {
				t.Fatalf("a refused query still issued %d statements: %s", len(db.calls), db.calls[0].sql)
			}
		})
	}

	// The control: the same two methods answer normally when the caller stays
	// inside what the store can serve, so the refusals above are about the
	// unsupported field and not about the method being broken.
	db := &wdFakeDB{
		sessions: map[string]string{"s-own": wdOwner},
		scripts: []wdScript{
			{match: "FROM events", rows: [][]any{{
				"e-1", "s-own", wdOwner, int64(11), "user_prompt", "hook",
				wdStart, wdStart, nil, nil, nil, nil, []byte(`{"text":"hi"}`),
			}}},
			{match: "FROM messages m", rows: [][]any{
				{"e-1", "s-own", wdOwner, int64(11), "user", "human", wdStart, "hi", 0.5, int64(1)},
			}},
		},
	}
	d := wdAdapter(db)
	cursor := web.EventCursor{At: wdStart, Seq: after}.Encode()
	if _, err := d.Events(context.Background(), wdViewer(wdOwner, false), "s-own", web.EventQuery{After: cursor, Limit: 10}); err != nil {
		t.Fatalf("a window the store can serve was refused: %v", err)
	}
	if _, err := d.Search(context.Background(), wdViewer(wdOwner, false), web.SearchQuery{Q: "hi"}); err != nil {
		t.Fatalf("a search the store can serve was refused: %v", err)
	}
}

// TestWebDataStatesNothingItCannotCompute is the tripwire over the fields left
// blank on purpose. Each one is a value the store has no way to produce, and each
// is left at whatever the port defines as "unknown" rather than filled with a
// number that would read as measured.
//
// The last case is the exception and the reason this test exists: the roster's
// session count has no "unknown" and renders as a literal 0 in the People table.
// It is asserted here so that adding the store method that can answer it breaks
// this test and forces the page to be updated with it.
func TestWebDataStatesNothingItCannotCompute(t *testing.T) {
	ctx := context.Background()
	db := &wdFakeDB{
		sessions: map[string]string{"s-own": wdOwner},
		scripts: []wdScript{
			{match: "FROM sessions s", rows: [][]any{wdSessionRow("s-own", wdOwner)}},
			{match: "FROM events", rows: [][]any{{
				"e-1", "s-own", wdOwner, int64(1), "user_prompt", "hook",
				wdStart, wdStart, nil, nil, nil, nil, []byte(`{"text":"hi"}`),
			}}},
			{match: "FROM principals ORDER BY email", rows: [][]any{
				{wdOwner, "member", "Owner Name", nil, wdStart, nil},
			}},
		},
	}
	d := wdAdapter(db)

	page, err := d.ListSessions(ctx, wdViewer(wdOwner, false), web.SessionQuery{})
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(page.Sessions) == 0 {
		t.Fatal("the fixture returned no sessions, so the blanks below prove nothing")
	}
	if page.Totals != nil {
		t.Fatalf("a summary strip was asserted over a set the store cannot aggregate: %#v", page.Totals)
	}
	if page.Sessions[0].Events != 0 {
		t.Fatalf("an event count was asserted from a rollup that does not carry one: %d", page.Sessions[0].Events)
	}

	window, err := d.Events(ctx, wdViewer(wdOwner, false), "s-own", web.EventQuery{Limit: 10})
	if err != nil {
		t.Fatalf("read the window: %v", err)
	}
	if window.Total != 0 {
		// The port renders a non-zero Total as "showing 200 of N". The size of the
		// window presented as the size of the session would tell a reader they had
		// seen all of it.
		t.Fatalf("a session-wide event total was asserted from one window: %d", window.Total)
	}

	det, err := d.Session(ctx, wdViewer(wdOwner, false), "s-own")
	if err != nil {
		t.Fatalf("read the session: %v", err)
	}
	if det.Agents != nil || det.Continuations != nil {
		t.Fatalf("subagents or continuations were asserted without a store read behind them: %#v %#v",
			det.Agents, det.Continuations)
	}

	people, err := d.Principals(ctx, wdViewer(wdAdmin, true))
	if err != nil {
		t.Fatalf("list principals: %v", err)
	}
	if len(people) != 1 {
		t.Fatalf("got %d roster rows, want 1", len(people))
	}
	if people[0].Sessions != 0 {
		t.Fatal("the roster now reports a session count: the People page renders this " +
			"number, so update the column and this assertion together")
	}
}

// ---------------------------------------------------------------------------
// Fleet coverage
// ---------------------------------------------------------------------------

// wdHealthRow builds one row of LatestHealth's projection.
func wdHealthRow(email, deviceID string, emitted, received time.Time, worst health.Level, conds ...health.Condition) []any {
	r := health.Report{
		SchemaVersion: health.SchemaVersion,
		Hostname:      "host-" + deviceID,
		OS:            "darwin",
		Arch:          "arm64",
		AgentVersion:  "1.4.0",
		EmittedAt:     emitted,
		Conditions:    conds,
	}
	body, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return []any{email, wdNullable(deviceID), emitted, received, string(worst), string(body)}
}

// wdDeviceRow builds one row of EnrolledDevices' projection.
func wdDeviceRow(id, email string, revokedAt *time.Time) []any {
	return []any{id, email, "host-" + id, "darwin", "arm64", "1.4.0", wdStart, nil, revokedAt}
}

func wdFleetDB(devices, health, people [][]any) *wdFakeDB {
	return &wdFakeDB{scripts: []wdScript{
		{match: "FROM devices", rows: devices},
		{match: "FROM health_reports", rows: health},
		{match: "FROM principals ORDER BY email", rows: people},
	}}
}

func wdMachine(t *testing.T, f web.Fleet, deviceID string) web.Machine {
	t.Helper()
	for _, m := range f.Machines {
		if m.DeviceID == deviceID {
			return m
		}
	}
	t.Fatalf("no machine %q on the fleet page; got %d machines", deviceID, len(f.Machines))
	return web.Machine{}
}

// TestWebDataFleetCountsCoveragePerMachineAndNotPerPerson is the reason the store
// returns one health row per (email, device_id). Somebody with a working laptop
// and a dead one is not covered, and a per-person roll-up reports them as healthy
// because the working machine's report answers for both.
func TestWebDataFleetCountsCoveragePerMachineAndNotPerPerson(t *testing.T) {
	fresh := wdNow.Add(-time.Hour)
	stale := wdNow.Add(-48 * time.Hour)

	db := wdFleetDB(
		[][]any{wdDeviceRow("dev-fresh", wdOwner, nil), wdDeviceRow("dev-stale", wdOwner, nil)},
		[][]any{
			wdHealthRow(wdOwner, "dev-fresh", fresh, fresh, health.LevelInfo),
			wdHealthRow(wdOwner, "dev-stale", stale, stale, health.LevelInfo),
		},
		[][]any{{wdOwner, "member", "Owner Name", nil, wdStart, nil}},
	)

	f, err := wdAdapterAt(db, wdNow).Fleet(context.Background(), wdViewer(wdAdmin, true))
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	if len(f.Machines) != 2 {
		t.Fatalf("got %d machines, want both of this person's", len(f.Machines))
	}
	if f.Enrolled != 2 || f.Reporting != 1 || f.Silent != 1 {
		t.Fatalf("enrolled=%d reporting=%d silent=%d, want 2/1/1: one person with a "+
			"working and a dead laptop is not covered", f.Enrolled, f.Reporting, f.Silent)
	}
	if got := wdMachine(t, f, "dev-stale"); !got.Silent {
		t.Fatal("the machine that stopped reporting is not marked silent")
	}
	if got := wdMachine(t, f, "dev-fresh"); got.Silent {
		t.Fatal("the machine that reported an hour ago is marked silent")
	}
	// The display name comes from the roster; the machine only knows its hostname.
	if got := wdMachine(t, f, "dev-fresh").Name; got != "Owner Name" {
		t.Fatalf("machine names the person as %q, want %q", got, "Owner Name")
	}
}

// TestWebDataFleetSilenceIsMeasuredFromArrival pins both the clock and the
// threshold. emitted_at is the reporting machine's own clock, so a laptop whose
// clock has jumped forward would otherwise read as permanently fresh and one whose
// clock has jumped back as permanently silent while it reports every minute.
func TestWebDataFleetSilenceIsMeasuredFromArrival(t *testing.T) {
	cases := []struct {
		name     string
		emitted  time.Time
		received time.Time
		silent   bool
	}{
		{"reported minutes ago", wdNow.Add(-5 * time.Minute), wdNow.Add(-5 * time.Minute), false},
		{"exactly at the threshold", wdNow.Add(-fleetStaleAfter), wdNow.Add(-fleetStaleAfter), false},
		{"a minute past the threshold", wdNow.Add(-fleetStaleAfter - time.Minute), wdNow.Add(-fleetStaleAfter - time.Minute), true},
		// The clock cases: the stamp the machine chose says one thing and arrival
		// says another, and arrival is what decides.
		{"a clock that jumped forward", wdNow.Add(720 * time.Hour), wdNow.Add(-48 * time.Hour), true},
		{"a clock that jumped back", wdStart.AddDate(-2, 0, 0), wdNow.Add(-time.Minute), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := wdFleetDB(
				[][]any{wdDeviceRow("dev-1", wdOwner, nil)},
				[][]any{wdHealthRow(wdOwner, "dev-1", tc.emitted, tc.received, health.LevelInfo)},
				nil,
			)
			f, err := wdAdapterAt(db, wdNow).Fleet(context.Background(), wdViewer(wdAdmin, true))
			if err != nil {
				t.Fatalf("fleet: %v", err)
			}
			m := wdMachine(t, f, "dev-1")
			if m.Silent != tc.silent {
				t.Fatalf("silent=%v, want %v (emitted %s, received %s)", m.Silent, tc.silent, tc.emitted, tc.received)
			}
			if m.ReceivedAt != tc.received {
				t.Fatalf("last report shown as %s, want the arrival time %s", m.ReceivedAt, tc.received)
			}
			if m.Age(wdNow) != wdNow.Sub(tc.received) {
				t.Fatalf("age %s, want %s", m.Age(wdNow), wdNow.Sub(tc.received))
			}
		})
	}
}

// TestWebDataFleetKeepsExactlyTheMachinesThatAreTheFinding covers the four
// membership decisions. Each one is on the page, or off it, because of what its
// presence would tell an operator.
func TestWebDataFleetKeepsExactlyTheMachinesThatAreTheFinding(t *testing.T) {
	fresh := wdNow.Add(-time.Hour)
	revoked := wdStart

	cases := []struct {
		name    string
		devices [][]any
		health  [][]any
		listed  []string
		why     string
	}{
		{
			name:    "a revoked machine that is still reporting",
			devices: [][]any{wdDeviceRow("dev-revoked", wdOwner, &revoked)},
			health:  [][]any{wdHealthRow(wdOwner, "dev-revoked", fresh, fresh, health.LevelInfo)},
			listed:  []string{"dev-revoked"},
			why:     "a withdrawn credential still sending reports is the case the coverage page exists to catch",
		},
		{
			name:    "a revoked machine that has gone quiet",
			devices: [][]any{wdDeviceRow("dev-revoked", wdOwner, &revoked)},
			health:  nil,
			listed:  nil,
			why:     "revoked and quiet is the intended end state of a revocation, not a coverage gap",
		},
		{
			name:    "a report from a machine with no device row",
			devices: nil,
			health:  [][]any{wdHealthRow(wdOwner, "dev-unknown", fresh, fresh, health.LevelInfo)},
			listed:  []string{"dev-unknown"},
			why:     "a report is proof the machine exists whatever the devices table says",
		},
		{
			name:    "an enrolled machine that has never reported",
			devices: [][]any{wdDeviceRow("dev-quiet", wdOwner, nil)},
			health:  nil,
			listed:  []string{"dev-quiet"},
			why:     "a failed install is still a machine somebody has to be told about",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := wdFleetDB(tc.devices, tc.health, nil)
			f, err := wdAdapterAt(db, wdNow).Fleet(context.Background(), wdViewer(wdAdmin, true))
			if err != nil {
				t.Fatalf("fleet: %v", err)
			}
			var got []string
			for _, m := range f.Machines {
				got = append(got, m.DeviceID)
			}
			if !reflect.DeepEqual(got, tc.listed) {
				t.Fatalf("machines %v, want %v: %s", got, tc.listed, tc.why)
			}
			if f.Enrolled != len(tc.listed) {
				t.Fatalf("enrolled=%d with %d machines listed; the cards and the rows have to agree",
					f.Enrolled, len(tc.listed))
			}
		})
	}

	// A machine that has never reported is not silent. The port reserves that
	// count for machines that reported once and stopped, which is the coverage hole
	// nobody has noticed, as against a failed install somebody already knows about.
	db := wdFleetDB([][]any{wdDeviceRow("dev-quiet", wdOwner, nil)}, nil, nil)
	f, err := wdAdapterAt(db, wdNow).Fleet(context.Background(), wdViewer(wdAdmin, true))
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	if f.Silent != 0 || f.Reporting != 0 {
		t.Fatalf("silent=%d reporting=%d for a machine that has never reported, want 0/0", f.Silent, f.Reporting)
	}
	m := wdMachine(t, f, "dev-quiet")
	if m.Silent || !m.ReceivedAt.IsZero() {
		t.Fatalf("a machine that never reported reads as silent-at-%s", m.ReceivedAt)
	}
	// The row still names the host, from what enrolment recorded. Nothing here is
	// invented: these are the values the laptop sent when it enrolled.
	if m.Report.Hostname != "host-dev-quiet" || m.Report.OS != "darwin" || m.Report.AgentVersion != "1.4.0" {
		t.Fatalf("enrolment facts missing from a never-reported machine: %#v", m.Report)
	}
	if len(m.Report.Conditions) != 0 {
		t.Fatalf("conditions were invented for a machine that has said nothing: %#v", m.Report.Conditions)
	}
}

// TestWebDataFleetCountsLevelsOnlyForMachinesStillReporting keeps the cards and
// the rows in agreement. The page renders a silent machine's pill as "silent"
// rather than as its last level, so counting that machine as critical as well puts
// a number on a card with no row beneath it to explain the number.
func TestWebDataFleetCountsLevelsOnlyForMachinesStillReporting(t *testing.T) {
	fresh := wdNow.Add(-time.Hour)
	stale := wdNow.Add(-72 * time.Hour)
	critical := health.Condition{Level: health.LevelCritical, Kind: health.KindCaptureBlocked, Detail: "disk full"}
	degraded := health.Condition{Level: health.LevelDegraded, Kind: health.KindBacklogGrowing, Detail: "queue rising"}

	db := wdFleetDB(
		[][]any{
			wdDeviceRow("dev-critical", wdOwner, nil),
			wdDeviceRow("dev-degraded", wdColleague, nil),
			wdDeviceRow("dev-silent-critical", wdStranger, nil),
		},
		[][]any{
			wdHealthRow(wdOwner, "dev-critical", fresh, fresh, health.LevelCritical, critical),
			wdHealthRow(wdColleague, "dev-degraded", fresh, fresh, health.LevelDegraded, degraded),
			wdHealthRow(wdStranger, "dev-silent-critical", stale, stale, health.LevelCritical, critical),
		},
		nil,
	)

	f, err := wdAdapterAt(db, wdNow).Fleet(context.Background(), wdViewer(wdAdmin, true))
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	if f.Enrolled != 3 || f.Reporting != 2 || f.Silent != 1 {
		t.Fatalf("enrolled=%d reporting=%d silent=%d, want 3/2/1", f.Enrolled, f.Reporting, f.Silent)
	}
	if f.Critical != 1 || f.Degraded != 1 {
		t.Fatalf("critical=%d degraded=%d, want 1/1: the silent machine is counted as silent and nothing else",
			f.Critical, f.Degraded)
	}
	// The report travels verbatim so the page can render conditions this server
	// has never heard of.
	if got := wdMachine(t, f, "dev-critical").Report.Conditions; !reflect.DeepEqual(got, []health.Condition{critical}) {
		t.Fatalf("conditions %#v, want the report's own %#v", got, critical)
	}
}

// TestWebDataFleetOrdersMachinesStably defends the refresh. The store returns the
// snapshots in one order and the map that groups them has none of its own, so
// without an explicit sort an otherwise unchanged fleet shuffles between page
// loads and nobody can tell a real change from a redraw.
func TestWebDataFleetOrdersMachinesStably(t *testing.T) {
	fresh := wdNow.Add(-time.Hour)
	db := wdFleetDB(
		[][]any{
			wdDeviceRow("dev-b", wdOwner, nil),
			wdDeviceRow("dev-a", wdOwner, nil),
		},
		[][]any{
			wdHealthRow(wdStranger, "dev-z", fresh, fresh, health.LevelInfo),
			wdHealthRow(wdColleague, "dev-y", fresh, fresh, health.LevelInfo),
		},
		nil,
	)
	d := wdAdapterAt(db, wdNow)

	var first []string
	for i := 0; i < 5; i++ {
		f, err := d.Fleet(context.Background(), wdViewer(wdAdmin, true))
		if err != nil {
			t.Fatalf("fleet: %v", err)
		}
		var got []string
		for _, m := range f.Machines {
			got = append(got, m.Email+"/"+m.DeviceID)
		}
		if i == 0 {
			first = got
			continue
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("the fleet reordered between refreshes: %v then %v", first, got)
		}
	}
	want := []string{
		wdColleague + "/dev-y",
		wdOwner + "/dev-a",
		wdOwner + "/dev-b",
		wdStranger + "/dev-z",
	}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("machine order %v, want %v", first, want)
	}
}

// ---------------------------------------------------------------------------
// The roster
// ---------------------------------------------------------------------------

// TestWebDataRosterCountsLiveDevicesAndDatesTheLastReport covers the two columns
// the People page shows that are not on the roster row. Both are derived from the
// same fleet-wide reads the coverage page uses, so the two pages cannot disagree
// about whether somebody is covered.
func TestWebDataRosterCountsLiveDevicesAndDatesTheLastReport(t *testing.T) {
	revoked := wdStart
	older := wdNow.Add(-6 * time.Hour)
	newer := wdNow.Add(-time.Hour)

	db := &wdFakeDB{scripts: []wdScript{
		{match: "FROM principals ORDER BY email", rows: [][]any{
			{wdOwner, "admin", "Owner Name", wdAdmin, wdStart, nil},
			{wdColleague, "member", nil, wdAdmin, wdStart, &revoked},
		}},
		{match: "FROM devices", rows: [][]any{
			wdDeviceRow("dev-1", wdOwner, nil),
			wdDeviceRow("dev-2", wdOwner, nil),
			wdDeviceRow("dev-gone", wdOwner, &revoked),
			wdDeviceRow("dev-3", wdColleague, nil),
		}},
		{match: "FROM health_reports", rows: [][]any{
			// The first machine's clock has jumped a month into the future. Last
			// seen has to follow arrival, or this row pins itself at the top of the
			// column forever.
			wdHealthRow(wdOwner, "dev-1", wdNow.AddDate(0, 1, 0), older, health.LevelInfo),
			wdHealthRow(wdOwner, "dev-2", newer, newer, health.LevelInfo),
		}},
	}}

	people, err := wdAdapter(db).Principals(context.Background(), wdViewer(wdAdmin, true))
	if err != nil {
		t.Fatalf("principals: %v", err)
	}
	if len(people) != 2 {
		t.Fatalf("got %d roster rows, want 2", len(people))
	}

	owner, colleague := people[0], people[1]
	if owner.Email != wdOwner || colleague.Email != wdColleague {
		t.Fatalf("roster order changed: %q then %q", owner.Email, colleague.Email)
	}
	if owner.Devices != 2 {
		t.Fatalf("owner has %d devices, want 2: a withdrawn credential is not a laptop they have", owner.Devices)
	}
	if owner.LastSeen != newer {
		t.Fatalf("last seen %s, want the newest arrival %s", owner.LastSeen, newer)
	}
	if owner.Role != web.RoleAdmin || owner.DisplayName != "Owner Name" || owner.AddedBy != wdAdmin || owner.AddedAt != wdStart {
		t.Fatalf("roster row lost a field: %#v", owner)
	}
	if owner.Disabled() {
		t.Fatal("an active principal reads as disabled")
	}
	if !colleague.Disabled() || colleague.DisabledAt != revoked {
		t.Fatalf("a disabled principal reads as %v/%s", colleague.Disabled(), colleague.DisabledAt)
	}
	if colleague.LastSeen != (time.Time{}) {
		t.Fatalf("somebody who has never reported is dated %s, want the zero time", colleague.LastSeen)
	}
}

// TestWebDataRosterEditStatesBothFieldsAndKeepsTheName pins what a submission from
// the People form means. Both role and disabled are always sent, because a form
// that described a delta would leave "clear the disabled box" saying nothing about
// the box. The display name is deliberately not sent: the form has no field for
// it, and a whole-field write of the empty string would erase the name of whoever
// was edited.
func TestWebDataRosterEditStatesBothFieldsAndKeepsTheName(t *testing.T) {
	cases := []struct {
		name string
		// currentRole is the role already on the row. It is per case because the
		// store now writes nothing at all when a submission carries the values the
		// row already holds, so a fixture whose "demotion" starts from member is a
		// fixture in which no edit happens and there is no statement to inspect.
		currentRole string
		update      web.PrincipalUpdate
		wantRole    string
		disabled    bool
	}{
		{"promotion", "member", web.PrincipalUpdate{Email: wdColleague, Role: web.RoleAdmin}, "admin", false},
		{"demotion", "admin", web.PrincipalUpdate{Email: wdColleague, Role: web.RoleMember}, "member", false},
		{"disable", "member", web.PrincipalUpdate{Email: wdColleague, Role: web.RoleMember, Disabled: true}, "member", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &wdFakeDB{scripts: []wdScript{
				{match: "WITH locked AS", rows: [][]any{{2}}},
				{match: "FROM principals WHERE email = $1", rows: [][]any{
					{wdColleague, tc.currentRole, "Colleague Name", wdAdmin, wdStart, nil},
				}},
				{match: "INSERT INTO principals", rows: [][]any{
					{wdColleague, tc.wantRole, "Colleague Name", wdAdmin, wdStart, nil},
				}},
			}}

			if _, err := wdAdapter(db).SetPrincipal(context.Background(), wdViewer(wdAdmin, true), tc.update); err != nil {
				t.Fatalf("save the roster row: %v", err)
			}
			call := db.find(t, "INSERT INTO principals")
			if got := call.args[1]; got != tc.wantRole {
				t.Fatalf("role written as %#v, want %q", got, tc.wantRole)
			}
			// The name the store writes is the one already on the row: passing a
			// nil DisplayName is what makes it fold the current value forward. Any
			// other value here would mean an edit from the People form had renamed
			// the person it edited.
			if got := call.args[2]; got != "Colleague Name" {
				t.Fatalf("the edit wrote the display name as %#v, want the stored %q", got, "Colleague Name")
			}
			disabledAt, _ := call.args[4].(*time.Time)
			if tc.disabled && disabledAt == nil {
				t.Fatal("disabling a principal wrote no disabled_at")
			}
			if !tc.disabled && disabledAt != nil {
				t.Fatalf("an edit that did not disable anybody wrote disabled_at=%s", disabledAt)
			}
		})
	}
}

// TestWebDataRosterEditTranslatesOnlyTheRefusalTheOperatorCanActOn distinguishes
// the three ways a roster edit fails. The lockout guard is the operator's own
// doing and they can undo it, so it becomes words on the page; everything else
// stays a generic failure, because storage error text is written for operators and
// routinely names hosts, columns and constraints.
func TestWebDataRosterEditTranslatesOnlyTheRefusalTheOperatorCanActOn(t *testing.T) {
	lastAdminDB := func() *wdFakeDB {
		return &wdFakeDB{scripts: []wdScript{
			// Nobody else would be left holding the role.
			{match: "WITH locked AS", rows: [][]any{{0}}},
			{match: "FROM principals WHERE email = $1", rows: [][]any{
				{wdAdmin, "admin", "The Admin", wdAdmin, wdStart, nil},
			}},
		}}
	}

	t.Run("the last admin", func(t *testing.T) {
		db := lastAdminDB()
		_, err := wdAdapter(db).SetPrincipal(context.Background(), wdViewer(wdAdmin, true),
			web.PrincipalUpdate{Email: wdAdmin, Role: web.RoleMember})
		var ue web.UserError
		if !errors.As(err, &ue) {
			t.Fatalf("the lockout guard produced %v, want a message for the operator", err)
		}
		if !strings.Contains(ue.Message, "admin") {
			t.Fatalf("the message does not say what went wrong: %q", ue.Message)
		}
		// The guard runs inside the transaction that would have performed the
		// write, so a refusal means nothing was written.
		if db.issued("INSERT INTO principals") {
			t.Fatal("the refused edit was written anyway")
		}
	})

	t.Run("a member editing the roster", func(t *testing.T) {
		db := &wdFakeDB{}
		_, err := wdAdapter(db).SetPrincipal(context.Background(), wdViewer(wdColleague, false),
			web.PrincipalUpdate{Email: wdOwner, Role: web.RoleAdmin})
		if !errors.Is(err, web.ErrDenied) {
			t.Fatalf("a member's edit produced %v, want web.ErrDenied", err)
		}
		// A write, not a read: the actor is looking at the roster and already knows
		// the row exists, so hiding it here would answer a refusal with a lie about
		// the page in front of them.
		if errors.Is(err, web.ErrNotFound) {
			t.Fatalf("a refused write was reported as a missing row: %v", err)
		}
		if len(db.calls) != 0 {
			t.Fatalf("a refused edit still issued %d statements", len(db.calls))
		}
	})

	t.Run("a failure of ours", func(t *testing.T) {
		boom := errors.New("deadlock detected")
		db := &wdFakeDB{fail: boom}
		_, err := wdAdapter(db).SetPrincipal(context.Background(), wdViewer(wdAdmin, true),
			web.PrincipalUpdate{Email: wdColleague, Role: web.RoleMember})
		var ue web.UserError
		if errors.As(err, &ue) {
			t.Fatalf("a storage failure was shown to the operator as advice: %q", ue.Message)
		}
		if !errors.Is(err, boom) {
			t.Fatalf("the cause was dropped: %v", err)
		}
	})
}

// TestWebDataAccessLogCarriesEveryFilterAndKeepsTheWindowHalfOpen defends the
// audit view. A filter accepted here and dropped on the way to the store produces
// a narrowed page an operator reads as the whole answer, and a window with two
// inclusive ends counts the row on the boundary twice when two windows are read
// back to back.
func TestWebDataAccessLogCarriesEveryFilterAndKeepsTheWindowHalfOpen(t *testing.T) {
	from := wdNow.Add(-24 * time.Hour)
	to := wdNow
	db := &wdFakeDB{scripts: []wdScript{{match: "FROM access_log", rows: [][]any{
		{int64(41), wdAdmin, "s-colleague", wdColleague, store.AccessViaAdmin, wdStart},
		{int64(40), wdStranger, "s-shared", wdOwner, store.AccessViaShare, wdStart},
	}}}}

	rows, err := wdAdapter(db).AccessLog(context.Background(), wdViewer(wdAdmin, true), web.AccessQuery{
		Viewer: wdAdmin, SessionID: "s-colleague", Owner: wdColleague,
		From: from, To: to, Limit: 25,
	})
	if err != nil {
		t.Fatalf("access log: %v", err)
	}
	want := []web.AccessEntry{
		{Viewer: wdAdmin, SessionID: "s-colleague", Owner: wdColleague, Via: store.AccessViaAdmin, At: wdStart},
		{Viewer: wdStranger, SessionID: "s-shared", Owner: wdOwner, Via: store.AccessViaShare, At: wdStart},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("entries %#v, want %#v", rows, want)
	}

	call := db.find(t, "FROM access_log")
	for i, want := range []any{"s-colleague", wdAdmin, wdColleague} {
		if call.args[i] != want {
			t.Errorf("filter argument %d is %#v, want %#v", i, call.args[i], want)
		}
	}
	since, _ := call.args[3].(*time.Time)
	until, _ := call.args[4].(*time.Time)
	if since == nil || *since != from {
		t.Errorf("since reached the store as %v, want %s inclusive", call.args[3], from)
	}
	if until == nil || *until != to {
		t.Errorf("until reached the store as %v, want %s exclusive", call.args[4], to)
	}
	if got := call.args[7]; got != 25 {
		t.Errorf("limit reached the store as %#v, want 25", got)
	}
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

// TestNewWebDataWiresAClockAndSatisfiesThePort checks the two things about the
// constructor that a caller depends on and no other test exercises: the returned
// value really is the port the dashboard asks for, and the clock the coverage page
// measures silence against is the wall clock rather than the zero value. A nil
// clock here would panic on the first fleet page; a zero one would report every
// machine in the fleet as silent since 1970.
func TestNewWebDataWiresAClockAndSatisfiesThePort(t *testing.T) {
	var _ web.Data = NewWebData(nil)

	d, ok := NewWebData(store.NewWithDB(&wdFakeDB{}, nil)).(webData)
	if !ok {
		t.Fatalf("NewWebData returned %T, want the package's own adapter", d)
	}
	if d.now == nil {
		t.Fatal("no clock was wired, so the fleet page panics on its first read")
	}
	if drift := time.Since(d.now()); drift < 0 || drift > time.Minute {
		t.Fatalf("the clock reads %s away from now", drift)
	}
	if d.s == nil {
		t.Fatal("no store was wired")
	}
}

// TestWebDataCarriesTheViewerRoleUnchanged makes sure the identity the auth layer
// resolved is the identity the store's predicate sees. The admin bit is re-read
// from the roster on every request, so carrying it is what makes revoking somebody
// take effect on their next click; re-deriving it here would answer the same
// question from the same table one round trip later, and dropping it would give an
// admin a member's view of the fleet.
func TestWebDataCarriesTheViewerRoleUnchanged(t *testing.T) {
	cases := []struct {
		name  string
		admin bool
		want  store.Role
	}{
		{"a member", false, store.RoleMember},
		{"an admin", true, store.RoleAdmin},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := webStoreViewer(wdViewer(wdOwner, tc.admin)); got.Email != wdOwner || got.Role != tc.want {
				t.Fatalf("viewer crossed as %#v, want %s/%s", got, wdOwner, tc.want)
			}

			db := &wdFakeDB{scripts: []wdScript{{match: "FROM sessions s", rows: [][]any{wdSessionRow("s-own", wdOwner)}}}}
			if _, err := wdAdapter(db).ListSessions(context.Background(), wdViewer(wdOwner, tc.admin), web.SessionQuery{
				Q: "deploy", Email: wdOwner, Source: "claude_code", Repo: "loop/backend",
				From: wdStart, To: wdNow, Cursor: "", Limit: 10,
			}); err != nil {
				t.Fatalf("list sessions: %v", err)
			}
			call := db.find(t, "FROM sessions s")
			if got := call.args[0]; got != tc.admin {
				t.Fatalf("the store's predicate was told admin=%#v, want %v", got, tc.admin)
			}
			// Every filter the list form offers reaches the store, in its own
			// parameter: one crossed with another silently answers a different
			// question from the one that was asked.
			for i, want := range []any{wdOwner, wdOwner, "claude_code", "loop/backend"} {
				if call.args[i+1] != want {
					t.Errorf("filter argument %d is %#v, want %#v", i+1, call.args[i+1], want)
				}
			}
			if got := call.args[7]; got != "deploy" {
				t.Errorf("the text filter reached the store as %#v", got)
			}
		})
	}
}

// TestWebDataListWindowsOnStartedAtInclusiveOfFromAndExclusiveOfTo covers the
// half of the list filter bar that TestWebDataCarriesTheViewerRoleUnchanged
// leaves alone. The four text filters are asserted there by value; the date
// range is not, and it is the pair where being wrong is hardest to see, because
// a window off by a day still returns sessions and still looks filtered.
//
// Two properties, and the second is the one that bites. An unset end of the
// range has to cross as NULL rather than as the zero time: the SQL reads NULL as
// "no bound", and a zero timestamp compared against started_at would exclude
// every session ever recorded from a list whose form has one date box filled in.
func TestWebDataListWindowsOnStartedAtInclusiveOfFromAndExclusiveOfTo(t *testing.T) {
	cases := []struct {
		name     string
		from, to time.Time
	}{
		{"both ends bounded", wdStart, wdNow},
		{"only a lower bound", wdStart, time.Time{}},
		{"only an upper bound", time.Time{}, wdNow},
		{"no date range at all", time.Time{}, time.Time{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &wdFakeDB{scripts: []wdScript{{match: "FROM sessions s", rows: [][]any{wdSessionRow("s-own", wdOwner)}}}}
			if _, err := wdAdapter(db).ListSessions(context.Background(), wdViewer(wdOwner, false), web.SessionQuery{
				From: tc.from, To: tc.to, Limit: 10,
			}); err != nil {
				t.Fatalf("list sessions: %v", err)
			}

			call := db.find(t, "FROM sessions s")
			for _, bound := range []struct {
				name string
				arg  int
				want time.Time
			}{
				{"from", 5, tc.from},
				{"to", 6, tc.to},
			} {
				got, _ := call.args[bound.arg].(*time.Time)
				switch {
				case bound.want.IsZero() && got != nil:
					t.Errorf("%s was unset and reached the store as %s, bounding a window the form did not ask for", bound.name, *got)
				case bound.want.IsZero():
				case got == nil:
					t.Errorf("%s reached the store as NULL, so the window the form asked for was dropped", bound.name)
				case !got.Equal(bound.want):
					t.Errorf("%s reached the store as %s, want %s", bound.name, *got, bound.want)
				}
			}
		})
	}
}

// TestWebDataListCarriesTheCursorTheNextPageIsFetchedWith closes the pagination
// loop. The cursor is opaque to the dashboard and comes back on the next request,
// so an adapter that dropped it would leave the list permanently on page one with
// no visible symptom beyond a missing link.
func TestWebDataListCarriesTheCursorTheNextPageIsFetchedWith(t *testing.T) {
	db := &wdFakeDB{scripts: []wdScript{{match: "FROM sessions s", rows: [][]any{
		wdSessionRow("s-1", wdOwner),
		wdSessionRow("s-2", wdOwner),
	}}}}

	// A limit of one makes the store fetch two rows and keep one, which is how it
	// discovers there is a next page.
	page, err := wdAdapter(db).ListSessions(context.Background(), wdViewer(wdOwner, false), web.SessionQuery{Limit: 1})
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(page.Sessions) != 1 {
		t.Fatalf("got %d sessions for a limit of 1", len(page.Sessions))
	}
	if page.NextCursor == "" {
		t.Fatal("the next page's cursor was dropped, so the list ends at page one")
	}

	// And it goes back the way it came.
	db.calls = nil
	if _, err := wdAdapter(db).ListSessions(context.Background(), wdViewer(wdOwner, false),
		web.SessionQuery{Limit: 1, Cursor: page.NextCursor}); err != nil {
		t.Fatalf("follow the cursor: %v", err)
	}
	call := db.find(t, "FROM sessions s")
	if call.args[10] != "s-1" {
		t.Fatalf("the cursor reached the store as %#v, want the last row of the previous page", call.args[9])
	}
}
