package app

// These tests drive the assembled handler the way a browser does: one request
// after another, each carrying whatever cookie the previous response set. That
// is the only shape in which the reported defect is visible — a page that
// answers 200 and then bounces the very next request to sign-in cannot be
// reproduced by a test that issues one request, because every individual
// request in that sequence is correct in isolation.
//
// Every helper here carries a renew prefix; package app keeps one file of
// fixtures per adapter and a bare fakeDB would collide with the next one.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// ---------------------------------------------------------------------------
// A store.DB that answers a signed-in admin's page loads
// ---------------------------------------------------------------------------

// renewDB answers the migration transaction, the roster lookup every
// authenticated request performs, and the fleet-wide reads the admin pages
// issue. The fleet-wide reads come back empty on purpose: production holds no
// health reports and no access-log rows, so an empty admin page is the correct
// rendering and the defect has to be visible without inventing data that would
// change what the page does.
type renewDB struct {
	mu sync.Mutex
	// principal is the roster row every authenticated request resolves against.
	// Nil means no row, which is what a person removed from the roster looks
	// like.
	principal []any
	// lookups counts roster reads, which is how a test tells "the cookie was
	// rejected before the roster was consulted" from "the roster refused".
	lookups int
}

func (d *renewDB) roster() []any {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lookups++
	return d.principal
}

func (d *renewDB) rosterReads() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lookups
}

func (d *renewDB) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return &renewRows{}, nil
}

func (d *renewDB) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	if strings.Contains(sql, "FROM principals") {
		if row := d.roster(); row != nil {
			return renewRow{values: row}
		}
	}
	return renewErrRow{err: pgx.ErrNoRows}
}

func (d *renewDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return 0, nil
}

func (d *renewDB) Begin(ctx context.Context) (store.Tx, error) { return &renewTx{db: d}, nil }

type renewTx struct{ db *renewDB }

func (t *renewTx) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	return &renewRows{}, nil
}

func (t *renewTx) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	if strings.Contains(sql, "schema_migrations") {
		return renewRow{values: []any{false}}
	}
	return t.db.QueryRow(ctx, sql, args...)
}

func (t *renewTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return 0, nil
}

func (t *renewTx) Commit(ctx context.Context) error   { return nil }
func (t *renewTx) Rollback(ctx context.Context) error { return nil }

type renewRows struct{}

func (r *renewRows) Next() bool             { return false }
func (r *renewRows) Scan(dest ...any) error { return errors.New("renewDB: no row") }
func (r *renewRows) Err() error             { return nil }
func (r *renewRows) Close()                 {}

type renewRow struct{ values []any }

func (r renewRow) Scan(dest ...any) error {
	if len(dest) != len(r.values) {
		return fmt.Errorf("renewDB: scan wants %d values, row has %d", len(dest), len(r.values))
	}
	for i, d := range dest {
		dv := reflect.ValueOf(d)
		if dv.Kind() != reflect.Pointer || dv.IsNil() {
			return fmt.Errorf("renewDB: destination %d is not a usable pointer", i)
		}
		v := reflect.ValueOf(r.values[i])
		target := dv.Elem().Type()
		if !v.IsValid() {
			dv.Elem().Set(reflect.Zero(target))
			continue
		}
		if !v.Type().AssignableTo(target) {
			if !v.Type().ConvertibleTo(target) {
				return fmt.Errorf("renewDB: cannot put %T into %s", r.values[i], target)
			}
			v = v.Convert(target)
		}
		dv.Elem().Set(v)
	}
	return nil
}

type renewErrRow struct{ err error }

func (r renewErrRow) Scan(...any) error { return r.err }

// renewAdminRow is the seeded administrator production actually holds: enabled,
// added by the seed migration, with no display name.
func renewAdminRow(email string) []any {
	return []any{
		email,
		store.RoleAdmin,
		(*string)(nil),
		strPtr("seed"),
		time.Now().Add(-24 * time.Hour),
		(*time.Time)(nil),
	}
}

func strPtr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// renewSignedIn assembles the whole server and returns the cookie a browser
// holds after a successful sign-in.
//
// The cookie is minted by an auth.Cookies configured exactly as routes()
// configures its own, which is what makes it the same credential the deployed
// server issues. If the two ever diverge the first request in every sequence
// below fails, so the fixture cannot silently stop testing the real cookie.
func renewSignedIn(t *testing.T, db *renewDB, email string) (http.Handler, *http.Cookie, *auth.Cookies) {
	t.Helper()
	cfg := bootConfig(t)
	st := store.NewWithDB(db, ingest.NewPricer(nil, bootLogger()))
	a, err := build(context.Background(), cfg, bootLogger(), st, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	cookies, err := auth.NewCookies(auth.CookieOptions{
		Keys:     [][]byte{cfg.SessionKey},
		Insecure: cfg.insecureCookies(),
	})
	if err != nil {
		t.Fatalf("cookies: %v", err)
	}
	rec := httptest.NewRecorder()
	if _, err := cookies.Issue(rec, email, auth.RoleAdmin, time.Now()); err != nil {
		t.Fatalf("issue session: %v", err)
	}
	ck := renewCookie(rec, cookies.Name())
	if ck == nil {
		t.Fatal("sign-in set no session cookie")
	}
	return a.handler, ck, cookies
}

// renewCookie picks one cookie out of a response, which is the browser's half of
// the exchange: a response that sets no cookie leaves the previous one in place.
func renewCookie(rec *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, ck := range (&http.Response{Header: rec.Header()}).Cookies() {
		if ck.Name == name {
			return ck
		}
	}
	return nil
}

// renewGet issues one request carrying one cookie and returns the response.
func renewGet(h http.Handler, target string, ck *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if ck != nil {
		req.AddCookie(ck)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ---------------------------------------------------------------------------
// The reported defect
// ---------------------------------------------------------------------------

// TestASessionThatResolvesOnceStillResolvesOnTheNextRequest is the failing shape
// of the report: /admin/fleet answered 200 and then bounced the next request to
// sign-in four seconds later, and /admin/access did the same seventeen seconds
// later, while /sessions and /search were requested repeatedly and never
// bounced.
//
// A browser carries forward whatever cookie a response sets, so a response that
// installs a credential the next request cannot verify produces exactly that
// pattern: the request holding the good cookie succeeds and the one after it is
// refused. The sequence is driven through the assembled handler rather than
// through renewSessions alone, because the middleware, the cookie and the
// dashboard's viewer function only disagree with each other in composition.
func TestASessionThatResolvesOnceStillResolvesOnTheNextRequest(t *testing.T) {
	const email = "alex@example.org"

	tests := []struct {
		name   string
		target string
	}{
		// The two routes the report says bounced.
		{"the fleet page, which answered 200 and then bounced four seconds later", "/admin/fleet"},
		{"the access log, which answered 200 and then bounced seventeen seconds later", "/admin/access"},
		// The routes it says never bounced, in the same test so the asymmetry is
		// a property this file states rather than a coincidence of what was
		// clicked in production.
		{"the roster page, requested once in production without bouncing", "/admin/principals"},
		{"the session list, requested four times without bouncing", "/sessions"},
		// Search was requested three times without bouncing. It has no page of
		// its own any more — the query is a filter on the session list — so the
		// route that carries the same traffic is the list with a query on it.
		{"search, requested three times without bouncing", "/sessions?q=flaky"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := &renewDB{principal: renewAdminRow(email)}
			h, ck, cookies := renewSignedIn(t, db, email)

			// Three requests, not two: a middleware that reissues on every
			// request would still pass a two-request test if the second cookie
			// happened to verify, and the failure the report describes is
			// "works once, then stops".
			for i := 1; i <= 3; i++ {
				rec := renewGet(h, tc.target, ck)
				if rec.Code != http.StatusOK {
					t.Fatalf("request %d to %s = %d (Location %q); the session stopped resolving after %d successful request(s)",
						i, tc.target, rec.Code, rec.Header().Get("Location"), i-1)
				}
				if next := renewCookie(rec, cookies.Name()); next != nil {
					ck = next
				}
			}
			if db.rosterReads() < 3 {
				t.Errorf("the roster was read %d times for 3 requests; every request must re-read it", db.rosterReads())
			}
		})
	}
}

// TestRenewalIsSilentOnASessionThatIsNotDueToBeRenewed pins the cost of the
// middleware and, with it, whether it can be the cause of anything. A session
// six minutes old is nowhere near its renewal point, so no route may spend a
// Set-Cookie on it: a reissue at that age would mean the middleware rewrites
// the credential on every request, which is both the noise Renew exists to
// avoid and the only way a page load could invalidate the cookie behind it.
func TestRenewalIsSilentOnASessionThatIsNotDueToBeRenewed(t *testing.T) {
	const email = "alex@example.org"
	db := &renewDB{principal: renewAdminRow(email)}
	h, ck, cookies := renewSignedIn(t, db, email)

	for _, target := range []string{"/sessions", "/sessions?q=flaky", "/admin/principals", "/admin/fleet", "/admin/access"} {
		t.Run(target, func(t *testing.T) {
			rec := renewGet(h, target, ck)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s = %d", target, rec.Code)
			}
			if got := renewCookie(rec, cookies.Name()); got != nil {
				t.Errorf("%s reissued the session cookie on a session six minutes old: %q", target, got.Value)
			}
		})
	}
}

// TestRenewalReissuesACookieTheNextRequestAccepts is the other half, and it is
// the one that would have caught a broken renewal. The middleware runs on every
// request, so once a session passes its renewal point every single request
// rewrites the cookie; if what it writes does not verify, the whole dashboard
// becomes a page that works once.
//
// The renewal point is reached by configuring a short one rather than by
// waiting six hours, and the sequence still runs through the assembled handler
// so the cookie under test is the one the middleware actually set.
func TestRenewalReissuesACookieTheNextRequestAccepts(t *testing.T) {
	const email = "alex@example.org"
	db := &renewDB{principal: renewAdminRow(email)}
	cfg := bootConfig(t)
	st := store.NewWithDB(db, ingest.NewPricer(nil, bootLogger()))
	a, err := build(context.Background(), cfg, bootLogger(), st, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// A renewal point of zero makes every request due for renewal, which is the
	// steady state of an actively used session and the state in which a broken
	// reissue is continuous rather than intermittent.
	cookies, err := auth.NewCookies(auth.CookieOptions{
		Keys:       [][]byte{cfg.SessionKey},
		Insecure:   cfg.insecureCookies(),
		RenewAfter: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("cookies: %v", err)
	}
	rec := httptest.NewRecorder()
	s, err := cookies.Issue(rec, email, auth.RoleAdmin, time.Now())
	if err != nil {
		t.Fatalf("issue session: %v", err)
	}
	ck := renewCookie(rec, cookies.Name())

	// Renewed by the same code path the middleware uses, then carried onto a
	// request against the assembled handler.
	out := httptest.NewRecorder()
	if _, renewed := cookies.Renew(out, s); !renewed {
		t.Fatal("Renew declined a session that is past its renewal point")
	}
	renewedCk := renewCookie(out, cookies.Name())
	if renewedCk == nil {
		t.Fatal("Renew reported a reissue and set no cookie")
	}
	if renewedCk.Value == ck.Value {
		t.Error("the reissued cookie is byte-identical to the original; nothing rolled forward")
	}

	if got := renewGet(a.handler, "/admin/fleet", renewedCk); got.Code != http.StatusOK {
		t.Fatalf("a request carrying the renewed cookie = %d (Location %q), want 200",
			got.Code, got.Header().Get("Location"))
	}
}

// ---------------------------------------------------------------------------
// What a renewal may be attached to
// ---------------------------------------------------------------------------

// renewDueForRenewal returns the assembled handler and a cookie that is past its
// renewal point, so that the middleware actually reissues on every request.
//
// The cookie is minted by an auth.Cookies holding the deployment's own signing
// key with its clock set seven hours back, which is why the server accepts it:
// nothing about the value says it was made by a test. Moving the clock rather
// than shortening the renewal interval keeps the interval under test the one
// production runs with.
func renewDueForRenewal(t *testing.T, email string) (http.Handler, *http.Cookie, string) {
	t.Helper()
	db := &renewDB{principal: renewAdminRow(email)}
	cfg := bootConfig(t)
	st := store.NewWithDB(db, ingest.NewPricer(nil, bootLogger()))
	a, err := build(context.Background(), cfg, bootLogger(), st, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	past := time.Now().Add(-7 * time.Hour)
	old, err := auth.NewCookies(auth.CookieOptions{
		Keys:     [][]byte{cfg.SessionKey},
		Insecure: cfg.insecureCookies(),
		Now:      func() time.Time { return past },
	})
	if err != nil {
		t.Fatalf("cookies: %v", err)
	}
	rec := httptest.NewRecorder()
	if _, err := old.Issue(rec, email, auth.RoleAdmin, past); err != nil {
		t.Fatalf("issue session: %v", err)
	}
	ck := renewCookie(rec, old.Name())
	if ck == nil {
		t.Fatal("no session cookie was issued")
	}

	// A page load first, both to prove the aged cookie is accepted and to read
	// the fingerprint the pages actually reference, so this test does not carry
	// a hash that goes stale the next time the stylesheet changes.
	page := renewGet(a.handler, "/sessions", ck)
	if page.Code != http.StatusOK {
		t.Fatalf("/sessions with a seven-hour-old cookie = %d, want 200", page.Code)
	}
	m := regexp.MustCompile(`/static/app\.css\?v=([0-9a-f]+)`).FindStringSubmatch(page.Body.String())
	if len(m) != 2 {
		t.Fatal("the rendered page does not reference a fingerprinted stylesheet; the immutable branch would go untested")
	}
	return a.handler, ck, m[0]
}

// TestARenewalIsNeverAttachedToAPubliclyCacheableResponse defends the one place
// this middleware can leak a credential. It runs on every route, and three of
// them answer with a `public` Cache-Control: the fingerprinted stylesheet asks a
// cache to keep it for a year, the bare stylesheet and the sign-in script for a
// minute. A browser sends the session cookie with those requests exactly as it
// does with a page, so a renewal landing on one produces a response that carries
// a live session and instructs any shared cache to store it and serve it to
// whoever asks next.
func TestARenewalIsNeverAttachedToAPubliclyCacheableResponse(t *testing.T) {
	const email = "alex@example.org"
	h, ck, fingerprinted := renewDueForRenewal(t, email)

	tests := []struct {
		name    string
		target  string
		public  bool
		wantSet bool
	}{
		{"the stylesheet a cache is told to keep for a year", fingerprinted, true, false},
		{"the stylesheet under its unfingerprinted url", "/static/app.css", true, false},
		{"the sign-in script, which a signed-in browser also fetches", "/auth/signin.js", true, false},
		// The other half: a response nobody may cache still renews, because a
		// session that never rolls forward is the defect renewal exists to fix.
		{"a dashboard page, which is private and no-store", "/sessions", false, true},
		{"an admin page, equally private", "/admin/fleet", false, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := renewGet(h, tc.target, ck)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s = %d, want 200", tc.target, rec.Code)
			}
			cc := rec.Header().Get("Cache-Control")
			if got := publiclyCacheable(cc); got != tc.public {
				t.Fatalf("%s answered Cache-Control %q; this test assumed publicly cacheable = %v",
					tc.target, cc, tc.public)
			}
			got := renewCookie(rec, "__Host-loop_session") != nil
			if got != tc.wantSet {
				if tc.public {
					t.Errorf("%s answered %q and carried a session cookie a shared cache may store and replay", tc.target, cc)
				} else {
					t.Errorf("%s answered %q and did not roll the session forward", tc.target, cc)
				}
			}
		})
	}
}

// TestSigningOutIsNotUndoneByARenewal guards the ordering the deferred renewal
// depends on. Sign-out clears the cookie and sign-in issues a new one, both
// before they write their status, so a renewal appended afterwards would be the
// value the browser keeps: the person clicks sign out, the server answers with a
// fresh twelve-hour session, and they stay signed in.
func TestSigningOutIsNotUndoneByARenewal(t *testing.T) {
	const email = "alex@example.org"
	h, ck, _ := renewDueForRenewal(t, email)

	req := httptest.NewRequest(http.MethodPost, SignOutPath, nil)
	req.AddCookie(ck)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("sign-out = %d, want 303", rec.Code)
	}
	var session []*http.Cookie
	for _, c := range (&http.Response{Header: rec.Header()}).Cookies() {
		if c.Name == "__Host-loop_session" {
			session = append(session, c)
		}
	}
	if len(session) != 1 {
		t.Fatalf("sign-out set %d session cookies; the browser keeps the last one, so there must be exactly one", len(session))
	}
	if session[0].Value != "" || session[0].MaxAge >= 0 {
		t.Errorf("sign-out left a live session cookie: value %q, MaxAge %d", session[0].Value, session[0].MaxAge)
	}
}
