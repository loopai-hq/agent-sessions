package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loopai-hq/loop-sessions/server/api"
	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// These tests drive the composition root over a fake connection source rather
// than a fake store, because what this file decides is which routes exist, what
// each handler was given and what a request with no credential sees. None of
// that needs a Postgres to be wrong in an observable way, and a test that needed
// one is a test that gets skipped on the laptop where the mistake is made.
//
// Every helper carries a boot prefix; package app holds one file of fixtures per
// adapter and a bare fakeDB would collide with the next one.

// ---------------------------------------------------------------------------
// A store.DB that answers the statements boot issues
// ---------------------------------------------------------------------------

type bootCall struct {
	sql  string
	args []any
}

// bootDB models exactly two things: the migration transaction, and the single
// statement device authentication issues. Everything else in this package's
// routes needs a credential first, and none of these tests carries one.
type bootDB struct {
	mu    sync.Mutex
	calls []bootCall

	beginErr  error
	commitErr error
	// failExec fails any Exec whose SQL contains this substring, which is how a
	// migration is made to fail at a chosen statement.
	failExec string

	// deviceRow and deviceErr script the device-token lookup. Both nil means the
	// credential matched no row, which is what an unknown token looks like.
	deviceRow []any
	deviceErr error
}

func (d *bootDB) record(sql string, args []any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, bootCall{sql: sql, args: args})
}

func (d *bootDB) snapshot() []bootCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]bootCall(nil), d.calls...)
}

func (d *bootDB) countMatching(sub string) int {
	n := 0
	for _, c := range d.snapshot() {
		if strings.Contains(c.sql, sub) {
			n++
		}
	}
	return n
}

func (d *bootDB) setDevice(row []any, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.deviceRow, d.deviceErr = row, err
}

func (d *bootDB) device() ([]any, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deviceRow, d.deviceErr
}

func (d *bootDB) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	d.record(sql, args)
	return &bootRows{}, nil
}

func (d *bootDB) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	d.record(sql, args)
	row, err := d.device()
	switch {
	case err != nil:
		return bootErrRow{err: err}
	case row != nil:
		return bootRow{values: row}
	}
	return bootErrRow{err: pgx.ErrNoRows}
}

func (d *bootDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	d.record(sql, args)
	return 0, nil
}

func (d *bootDB) Begin(ctx context.Context) (store.Tx, error) {
	if d.beginErr != nil {
		return nil, d.beginErr
	}
	return &bootTx{db: d}, nil
}

type bootTx struct{ db *bootDB }

func (t *bootTx) Query(ctx context.Context, sql string, args ...any) (store.Rows, error) {
	t.db.record(sql, args)
	return &bootRows{}, nil
}

// QueryRow inside the migration transaction answers the ledger check. False for
// every file, so a fresh database applies all of them.
func (t *bootTx) QueryRow(ctx context.Context, sql string, args ...any) store.Row {
	t.db.record(sql, args)
	if strings.Contains(sql, "schema_migrations") {
		return bootRow{values: []any{false}}
	}
	return bootErrRow{err: pgx.ErrNoRows}
}

func (t *bootTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	t.db.record(sql, args)
	if t.db.failExec != "" && strings.Contains(sql, t.db.failExec) {
		return 0, fmt.Errorf("bootDB: refusing %q", t.db.failExec)
	}
	return 0, nil
}

func (t *bootTx) Commit(ctx context.Context) error   { return t.db.commitErr }
func (t *bootTx) Rollback(ctx context.Context) error { return nil }

type bootRows struct{ rows [][]any }

func (r *bootRows) Next() bool { return len(r.rows) > 0 }
func (r *bootRows) Scan(dest ...any) error {
	if len(r.rows) == 0 {
		return errors.New("bootDB: no row")
	}
	row := r.rows[0]
	r.rows = r.rows[1:]
	return bootAssign(dest, row)
}
func (r *bootRows) Err() error { return nil }
func (r *bootRows) Close()     {}

type bootRow struct{ values []any }

func (r bootRow) Scan(dest ...any) error { return bootAssign(dest, r.values) }

type bootErrRow struct{ err error }

func (r bootErrRow) Scan(...any) error { return r.err }

func bootAssign(dest []any, values []any) error {
	if len(dest) != len(values) {
		return fmt.Errorf("bootDB: scan wants %d values, row has %d", len(dest), len(values))
	}
	for i, d := range dest {
		dv := reflect.ValueOf(d)
		if dv.Kind() != reflect.Pointer || dv.IsNil() {
			return fmt.Errorf("bootDB: destination %d is not a usable pointer", i)
		}
		v := reflect.ValueOf(values[i])
		target := dv.Elem().Type()
		if !v.Type().AssignableTo(target) {
			if !v.Type().ConvertibleTo(target) {
				return fmt.Errorf("bootDB: cannot put %T into %s", values[i], target)
			}
			v = v.Convert(target)
		}
		dv.Elem().Set(v)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func bootLogger() *slog.Logger { return NewLogger(slog.LevelError, io.Discard) }

func bootConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := Load(bootGetenv(bootEnv()))
	if err != nil {
		t.Fatalf("bootConfig: %v", err)
	}
	return cfg
}

// bootApp assembles the whole server over the fake connection source.
func bootApp(t *testing.T, db *bootDB, ping func(context.Context) error) *App {
	t.Helper()
	if ping == nil {
		ping = func(context.Context) error { return nil }
	}
	st := store.NewWithDB(db, ingest.NewPricer(nil, bootLogger()))
	a, err := build(context.Background(), bootConfig(t), bootLogger(), st, ping)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return a
}

func bootDo(t *testing.T, h http.Handler, method, target string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ---------------------------------------------------------------------------
// Migrations
// ---------------------------------------------------------------------------

// TestBootAppliesEveryMigrationBeforeAnythingCanServe defends the ordering the
// contract pins. Migrations run at boot, under the advisory lock so a rolling
// deploy's second instance waits rather than applying the same file twice, and
// they run before a listener exists: an instance Cloud Run has started routing
// traffic to cannot be one that is still deciding what its schema is.
func TestBootAppliesEveryMigrationBeforeAnythingCanServe(t *testing.T) {
	db := &bootDB{}
	a := bootApp(t, db, nil)

	if a.handler == nil {
		t.Fatal("build returned an App with no handler")
	}
	if db.countMatching("pg_advisory_xact_lock") != 1 {
		t.Errorf("the migration did not take the advisory lock exactly once: %d", db.countMatching("pg_advisory_xact_lock"))
	}
	if db.countMatching("CREATE TABLE IF NOT EXISTS schema_migrations") != 1 {
		t.Error("the migration ledger was not created")
	}

	// Every file, not merely the first. A boot path that stopped after 0001
	// would pass a test that only counted "some migration ran", and the schema
	// the admin pages need lives in 0002.
	want := bootMigrationCount(t)
	if got := db.countMatching("INSERT INTO schema_migrations"); got != want {
		t.Errorf("recorded %d migrations, want %d", got, want)
	}

	before := len(db.snapshot())
	if rec := bootDo(t, a.handler, http.MethodGet, HealthzPath, nil); rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d", rec.Code)
	}
	if after := len(db.snapshot()); after != before {
		t.Errorf("serving issued %d further statements; migrations must not be part of the request path", after-before)
	}
}

// bootMigrationCount counts the files store.Migrate will apply, read from the
// directory rather than hard-coded, so adding a migration does not silently
// weaken the assertion above.
func bootMigrationCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join("..", "store", "migrations"))
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			n++
		}
	}
	if n == 0 {
		t.Fatal("no migration files found; the assertion above would be vacuous")
	}
	return n
}

// TestBootRefusesToServeWhenTheDatabaseCannotBeMigrated is the other half of the
// same property. A server that started anyway would answer the platform's probe
// while every route below it failed against a schema that is not there, and the
// revision would be marked healthy and promoted.
func TestBootRefusesToServeWhenTheDatabaseCannotBeMigrated(t *testing.T) {
	tests := []struct {
		name string
		db   *bootDB
	}{
		{"the transaction cannot be opened", &bootDB{beginErr: errors.New("pool exhausted")}},
		{"the advisory lock cannot be taken", &bootDB{failExec: "pg_advisory_xact_lock"}},
		{"the ledger cannot be created", &bootDB{failExec: "CREATE TABLE IF NOT EXISTS schema_migrations"}},
		{"a migration body is rejected", &bootDB{failExec: "CREATE TABLE IF NOT EXISTS principals"}},
		{"the transaction cannot be committed", &bootDB{commitErr: errors.New("connection reset")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewWithDB(tc.db, ingest.NewPricer(nil, bootLogger()))
			a, err := build(context.Background(), bootConfig(t), bootLogger(), st, nil)
			if err == nil {
				t.Fatal("build succeeded against a database it could not migrate")
			}
			if a != nil {
				t.Error("build returned an App alongside its failure; Serve must be unreachable")
			}
			if !strings.Contains(err.Error(), "migrat") {
				t.Errorf("failure does not say migrations were what failed: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Liveness and readiness
// ---------------------------------------------------------------------------

// TestLivenessNeverReadsTheDatabaseAndReadinessAlwaysDoes defends the split the
// contract calls out. A liveness probe that fails on database trouble makes
// Cloud Run kill and restart every instance during an incident, turning a
// degraded read path into a total outage; a readiness probe that does not read
// the database reports an instance as able to serve when nothing it serves
// works.
func TestLivenessNeverReadsTheDatabaseAndReadinessAlwaysDoes(t *testing.T) {
	var (
		mu        sync.Mutex
		pings     int
		deadlines []time.Time
		pingErr   error
	)
	ping := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		pings++
		if dl, ok := ctx.Deadline(); ok {
			deadlines = append(deadlines, dl)
		} else {
			deadlines = append(deadlines, time.Time{})
		}
		return pingErr
	}

	db := &bootDB{}
	a := bootApp(t, db, ping)
	settled := len(db.snapshot())

	// Both paths, because Google's front end swallows /healthz on a run.app URL
	// and /livez is the one an uptime check can actually reach. Two paths that
	// are meant to be the same handler are two paths that can drift into
	// different answers.
	for _, path := range []string{HealthzPath, LivezPath} {
		t.Run("liveness at "+path+" answers while the database is refusing every read", func(t *testing.T) {
			mu.Lock()
			pingErr = errors.New("the database is gone")
			pings = 0
			mu.Unlock()

			rec := bootDo(t, a.handler, http.MethodGet, path, nil)
			if rec.Code != http.StatusOK {
				t.Errorf("%s = %d, want 200 while the database is down", path, rec.Code)
			}
			mu.Lock()
			got := pings
			mu.Unlock()
			if got != 0 {
				t.Errorf("%s pinged the database %d times; liveness must decide nothing from it", path, got)
			}
			if n := len(db.snapshot()); n != settled {
				t.Errorf("%s issued %d statements", path, n-settled)
			}
		})
	}

	t.Run("readiness reports the instance cannot serve", func(t *testing.T) {
		mu.Lock()
		pingErr = errors.New("the database is gone")
		pings = 0
		mu.Unlock()

		rec := bootDo(t, a.handler, http.MethodGet, ReadyzPath, nil)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("readyz = %d, want 503 while the database is down", rec.Code)
		}
		mu.Lock()
		got := pings
		mu.Unlock()
		if got != 1 {
			t.Errorf("readyz pinged %d times, want 1", got)
		}
		// The driver's error text names hosts, users and constraints, and this
		// endpoint is reachable from the internet.
		if strings.Contains(rec.Body.String(), "the database is gone") {
			t.Errorf("readyz echoed the storage failure: %s", rec.Body.String())
		}
	})

	t.Run("readiness reports the instance can serve", func(t *testing.T) {
		mu.Lock()
		pingErr = nil
		pings = 0
		deadlines = nil
		mu.Unlock()

		rec := bootDo(t, a.handler, http.MethodGet, ReadyzPath, nil)
		if rec.Code != http.StatusOK {
			t.Errorf("readyz = %d, want 200", rec.Code)
		}
		mu.Lock()
		dls := append([]time.Time(nil), deadlines...)
		mu.Unlock()
		if len(dls) != 1 || dls[0].IsZero() {
			t.Fatalf("the readiness check ran without a deadline: %v", dls)
		}
		// Bounded by this handler rather than by whatever the caller was willing
		// to wait for: a probe that hangs reports nothing at all.
		if until := time.Until(dls[0]); until > readyTimeout {
			t.Errorf("readiness deadline is %s away, want at most %s", until, readyTimeout)
		}
	})
}

// ---------------------------------------------------------------------------
// The route tree
// ---------------------------------------------------------------------------

// TestEveryPackagesRoutesAreReachableOnOneMux defends the assembly itself. Six
// packages register on one tree and the dashboard is the catch-all, so the way
// this goes wrong is silent: a shadowed pattern answers with somebody else's
// handler, and the symptom is an endpoint that returns the wrong content type
// rather than an error anybody notices at boot.
func TestEveryPackagesRoutesAreReachableOnOneMux(t *testing.T) {
	db := &bootDB{}
	a := bootApp(t, db, nil)

	tests := []struct {
		name     string
		method   string
		target   string
		want     int
		wantBody string
		wantLoc  string
	}{
		{
			name: "the upload endpoint refuses an unauthenticated laptop",
			// A device token, not a cookie: the agent uploads while nobody is
			// watching and cannot perform an interactive sign-in.
			method: http.MethodPost, target: ingest.EventsPath, want: http.StatusUnauthorized,
			wantBody: "unauthenticated",
		},
		{
			name:   "the health report endpoint is mounted alongside it",
			method: http.MethodPost, target: ingest.HealthPath, want: http.StatusUnauthorized,
		},
		{
			name:   "the read api refuses a browser with no cookie",
			method: http.MethodGet, target: "/v1/sessions", want: http.StatusUnauthorized,
			wantBody: "sign in to continue",
		},
		{
			name:   "the admin api is mounted and equally closed",
			method: http.MethodGet, target: "/v1/admin/principals", want: http.StatusUnauthorized,
		},
		{
			name:   "enrollment is reachable and validates its body",
			method: http.MethodPost, target: EnrollCompletePath, want: http.StatusBadRequest,
			wantBody: "malformed request",
		},
		{
			// Renders rather than redirects, which is the visible difference
			// between this and the OAuth design it replaced. The browser now
			// talks to Firebase from this page instead of being bounced to
			// Google by the server, so a 302 here would mean the old
			// authorization-code flow had survived the rehaul somewhere.
			name:   "sign-in renders its own page rather than redirecting to google",
			method: http.MethodGet, target: SignInPath, want: http.StatusOK,
			wantBody: `action="/auth/session"`,
		},
		{
			name:   "sign-out is a POST and clears the cookie",
			method: http.MethodPost, target: SignOutPath, want: http.StatusSeeOther,
		},
		{
			name:   "the dashboard is the catch-all and sends a signed-out browser to sign in",
			method: http.MethodGet, target: "/sessions", want: http.StatusFound,
			wantLoc: SignInPath + "?next=%2Fsessions",
		},
		{
			name:   "the root redirects into the dashboard",
			method: http.MethodGet, target: "/", want: http.StatusFound,
			wantLoc: "/sessions",
		},
		{
			name:   "the stylesheet the pages reference is served",
			method: http.MethodGet, target: "/static/app.css", want: http.StatusOK,
		},
		{
			name:   "liveness is mounted",
			method: http.MethodGet, target: HealthzPath, want: http.StatusOK,
		},
		{
			name:   "liveness is also mounted where a run.app uptime check can reach it",
			method: http.MethodGet, target: LivezPath, want: http.StatusOK,
		},
		{
			name:   "readiness is mounted",
			method: http.MethodGet, target: ReadyzPath, want: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := bootDo(t, a.handler, tc.method, tc.target, nil)
			if rec.Code != tc.want {
				t.Fatalf("%s %s = %d, want %d (body %q)", tc.method, tc.target, rec.Code, tc.want, rec.Body.String())
			}
			if tc.wantBody != "" && !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("body = %q, want it to contain %q", rec.Body.String(), tc.wantBody)
			}
			if tc.wantLoc != "" {
				if loc := rec.Header().Get("Location"); !strings.HasPrefix(loc, tc.wantLoc) {
					t.Errorf("Location = %q, want it to start with %q", loc, tc.wantLoc)
				}
			}
		})
	}

	t.Run("the dashboard carries its content security policy", func(t *testing.T) {
		// Mounting the dashboard by its Routes instead of its Handler would
		// quietly drop these headers, which is what this subtest is for. Both
		// halves of the split are checked, because a composition-root mistake
		// that dropped the header entirely would still satisfy either one on
		// its own: 'none' would be missing from the list page and 'self' from
		// the transcript, and each absence reads as the other's policy.
		for target, want := range map[string]string{
			// A transcript body: script-src 'none' is what keeps an escaping
			// mistake there from becoming code execution.
			"/sessions/2f4c": "script-src 'none'",
			// A filter page: combo.js is same-origin and needs 'self'.
			"/sessions": "script-src 'self'",
		} {
			rec := bootDo(t, a.handler, http.MethodGet, target, nil)
			if csp := rec.Header().Get("Content-Security-Policy"); !strings.Contains(csp, want) {
				t.Errorf("%s Content-Security-Policy = %q, want it to contain %q", target, csp, want)
			}
		}
	})
}

// TestAnUnknownAPIPathAnswersExactlyAsAHiddenSessionDoes defends the leak the
// whole service is shaped around. If an address that does not exist answered
// differently from an address the caller may not see, a caller could enumerate
// which sessions exist by the shape of the refusal, and that a named colleague
// ran something at a particular time is the fact they were not allowed to learn.
func TestAnUnknownAPIPathAnswersExactlyAsAHiddenSessionDoes(t *testing.T) {
	a := bootApp(t, &bootDB{}, nil)

	// The bytes the read API itself produces for "not found", whatever the
	// reason: an unknown session id, somebody else's session, a revoked share.
	canonical := httptest.NewRecorder()
	api.NotFound(canonical, httptest.NewRequest(http.MethodGet, "/v1/sessions/whatever", nil))

	tests := []struct {
		name   string
		method string
		target string
	}{
		{"a path under /v1 that no route claims", http.MethodGet, "/v1/nonsense"},
		{"a verb a route does not serve", http.MethodPost, "/v1/sessions/abc"},
		{"a nested path below a real route", http.MethodGet, "/v1/sessions/abc/nonsense"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := bootDo(t, a.handler, tc.method, tc.target, nil)
			if rec.Code != canonical.Code {
				t.Errorf("status = %d, want %d", rec.Code, canonical.Code)
			}
			if rec.Body.String() != canonical.Body.String() {
				t.Errorf("body = %q, want the API's own not-found body %q",
					rec.Body.String(), canonical.Body.String())
			}
			if got, want := rec.Header().Get("Content-Type"), canonical.Header().Get("Content-Type"); got != want {
				t.Errorf("Content-Type = %q, want %q", got, want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Device credentials
// ---------------------------------------------------------------------------

// TestADeadCredentialIsToldApartFromADeadDatabase defends the classification the
// upload path turns into a status code. 401 sends a laptop to re-enrol and 503
// makes it keep everything it captured and retry, so getting this backwards
// during a database blip tells a fleet of working machines that their
// credentials are dead — and the enrollment endpoint is the next thing to fall
// over.
func TestADeadCredentialIsToldApartFromADeadDatabase(t *testing.T) {
	unreachable := errors.New("dial tcp: connection refused")

	tests := []struct {
		name            string
		token           string
		row             []any
		err             error
		wantUnauthentic bool
		wantCause       error
		wantStatements  int
	}{
		{
			name:            "a credential that is not ours by shape is refused without a lookup",
			token:           "not-a-loop-token",
			wantUnauthentic: true,
			wantCause:       auth.ErrDeviceTokenMalformed,
			wantStatements:  0,
		},
		{
			name:            "an empty credential is refused without a lookup",
			token:           "",
			wantUnauthentic: true,
			wantCause:       auth.ErrDeviceTokenMalformed,
			wantStatements:  0,
		},
		{
			name:            "a credential matching no live row is the caller's problem",
			token:           auth.TokenPrefix + "aaaabbbbccccdddd",
			wantUnauthentic: true,
			wantCause:       store.ErrNotFound,
			wantStatements:  1,
		},
		{
			name:            "a database that cannot answer is ours",
			token:           auth.TokenPrefix + "aaaabbbbccccdddd",
			err:             unreachable,
			wantUnauthentic: false,
			wantCause:       unreachable,
			wantStatements:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := &bootDB{}
			db.setDevice(tc.row, tc.err)
			devices := newStoreDevices(store.NewWithDB(db, ingest.NewPricer(nil, bootLogger())))

			id, err := devices.Verify(context.Background(), tc.token)
			if err == nil {
				t.Fatal("Verify accepted the credential")
			}
			if got := errors.Is(err, ingest.ErrUnauthenticated); got != tc.wantUnauthentic {
				t.Errorf("errors.Is(err, ErrUnauthenticated) = %v, want %v (err: %v)", got, tc.wantUnauthentic, err)
			}
			if !errors.Is(err, tc.wantCause) {
				t.Errorf("the cause was not carried through: %v", err)
			}
			if id != (ingest.Identity{}) {
				t.Errorf("an identity travelled with a failure: %+v", id)
			}
			if got := len(db.snapshot()); got != tc.wantStatements {
				t.Errorf("issued %d statements, want %d", got, tc.wantStatements)
			}
		})
	}

	t.Run("a live credential yields the attribution and nothing else", func(t *testing.T) {
		db := &bootDB{}
		db.setDevice([]any{"device-1", "dev@example.com", "admin"}, nil)
		devices := newStoreDevices(store.NewWithDB(db, ingest.NewPricer(nil, bootLogger())))

		id, err := devices.Verify(context.Background(), auth.TokenPrefix+"aaaabbbbccccdddd")
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		// Email and device only. The role stays behind because the upload path
		// makes no permission decision, and a role carried into it would be a
		// second place authorization could be decided from.
		want := ingest.Identity{Email: "dev@example.com", DeviceID: "device-1"}
		if id != want {
			t.Errorf("identity = %+v, want %+v", id, want)
		}
	})

	t.Run("the endpoint turns the two classes into different statuses", func(t *testing.T) {
		db := &bootDB{}
		a := bootApp(t, db, nil)

		req := httptest.NewRequest(http.MethodPost, ingest.EventsPath, strings.NewReader(`{"items":[]}`))
		req.Header.Set("Authorization", "Bearer "+auth.TokenPrefix+"aaaabbbbccccdddd")

		db.setDevice(nil, nil) // no live row
		rec := httptest.NewRecorder()
		a.handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("unknown credential = %d, want 401", rec.Code)
		}

		db.setDevice(nil, errors.New("connection refused"))
		req2 := httptest.NewRequest(http.MethodPost, ingest.EventsPath, strings.NewReader(`{"items":[]}`))
		req2.Header.Set("Authorization", "Bearer "+auth.TokenPrefix+"aaaabbbbccccdddd")
		rec2 := httptest.NewRecorder()
		a.handler.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusServiceUnavailable {
			t.Errorf("unreachable database = %d, want 503", rec2.Code)
		}
	})
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// TestDrainBudgetFitsInsideTheGraceCloudRunAllows is a tripwire on the one
// number that has to stay below a platform constant. Cloud Run sends SIGTERM
// and sends SIGKILL ten seconds later; a drain budget at or above that is a
// budget the platform never lets us spend, and the process is killed
// mid-response with the pool still open.
func TestDrainBudgetFitsInsideTheGraceCloudRunAllows(t *testing.T) {
	const cloudRunGrace = 10 * time.Second
	if drainTimeout >= cloudRunGrace {
		t.Fatalf("drainTimeout = %s, which is not below Cloud Run's %s grace", drainTimeout, cloudRunGrace)
	}
	// Room left over for closing the pool and exiting, which happen after the
	// drain and are also inside the same ten seconds.
	if cloudRunGrace-drainTimeout < 2*time.Second {
		t.Errorf("drainTimeout = %s leaves only %s to close the pool and exit",
			drainTimeout, cloudRunGrace-drainTimeout)
	}
}

// TestServeFinishesRequestsAlreadyInFlightWhenTheSignalArrives defends the
// drain. Cloud Run sends SIGTERM before it removes the instance from the load
// balancer, so requests are in flight at the moment the signal lands: a server
// that returns on the signal turns every one of them into a 502 in somebody's
// browser, and every deploy into a handful of them.
func TestServeFinishesRequestsAlreadyInFlightWhenTheSignalArrives(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	inFlightCtx := make(chan error, 1)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		// The request's own context must still be live. Rooting request
		// contexts at the signal context would cancel every in-flight query at
		// the instant the drain starts, which is the abrupt shutdown the drain
		// replaced.
		inFlightCtx <- r.Context().Err()
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "finished")
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	a := &App{cfg: Config{}, log: bootLogger(), handler: handler}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- a.serve(ctx, ln) }()

	type reply struct {
		status int
		body   string
		err    error
	}
	replies := make(chan reply, 1)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	go func() {
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			replies <- reply{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		replies <- reply{status: resp.StatusCode, body: string(b), err: err}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the handler")
	}

	// SIGTERM lands while the request is inside the handler.
	cancel()
	// Long enough for the shutdown to have begun and stopped accepting.
	time.Sleep(100 * time.Millisecond)
	close(release)

	select {
	case got := <-replies:
		if got.err != nil {
			t.Fatalf("the in-flight request failed: %v", got.err)
		}
		if got.status != http.StatusOK || got.body != "finished" {
			t.Errorf("in-flight request got %d %q, want 200 \"finished\"", got.status, got.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight request never completed")
	}

	if err := <-inFlightCtx; err != nil {
		t.Errorf("the in-flight request's context was cancelled by the shutdown: %v", err)
	}

	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("serve returned %v, want a clean shutdown", err)
		}
	case <-time.After(drainTimeout + 2*time.Second):
		t.Fatal("serve did not return within the drain budget")
	}

	// And nothing new is accepted afterwards: the socket is closed rather than
	// left listening by a shutdown that only stopped the handler.
	if resp, err := client.Get("http://" + addr + "/"); err == nil {
		resp.Body.Close()
		t.Error("the listener still accepts connections after the drain")
	}
}

// TestServeReportsAListenerItCannotBind defends the startup failure mode. A port
// already in use has to be a failed start with a message naming the port, not a
// process that logs nothing and idles while the platform waits for a listener.
func TestServeReportsAListenerItCannotBind(t *testing.T) {
	// The wildcard address rather than a loopback one, because Serve binds the
	// wildcard: holding 127.0.0.1:p does not conflict with binding :p.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	// A deadline rather than Background, so a Serve that wrongly succeeds ends
	// the test instead of hanging it.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	a := &App{cfg: Config{Port: port}, log: bootLogger(), handler: http.NotFoundHandler()}
	err = a.Serve(ctx)
	if err == nil {
		t.Fatal("Serve succeeded on a port already in use")
	}
	if !strings.Contains(err.Error(), fmt.Sprint(port)) {
		t.Errorf("failure does not name the port: %v", err)
	}
}

// TestCloseIsSafeOnAnAppThatNeverOpenedAPool keeps the shutdown path honest for
// the failure case main relies on: Close is deferred immediately after New, and
// a nil pool must not turn a startup failure into a panic that hides it.
func TestCloseIsSafeOnAnAppThatNeverOpenedAPool(t *testing.T) {
	a := bootApp(t, &bootDB{}, nil)
	a.Close()
	a.Close()
}
