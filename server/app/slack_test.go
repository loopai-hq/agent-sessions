package app

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/ingest"
	"github.com/loopai-hq/agent-sessions/server/slack"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// This file answers one question and it is the question this codebase has got
// wrong six times: who calls the thing that was built.
//
// server/slack has thorough tests of its own, and every one of them would still
// pass if this package had never imported it. The mirror is only a feature when
// the assembled server mounts its route and the running server drives its loop,
// so both are asserted here against the real handler and the real serve().

// ---------------------------------------------------------------------------
// A connection source for the mirror
// ---------------------------------------------------------------------------

// mirDB is the raw pgx source the mirror is given. It answers the eligibility
// query with no rows, which is what a healthy pass over a database nobody has
// opted into looks like, and reports the moment that query is issued.
//
// Deliberately not a store.DB: the mirror owns two tables nothing else reads
// and talks pgx directly, which is why the composition root hands it the pool
// rather than the store.
type mirDB struct {
	mu         sync.Mutex
	statements []string
	// swept receives once per eligibility query. Buffered, so a mirror that
	// sweeps faster than the test reads is not blocked by it.
	swept chan struct{}
}

func newMirDB() *mirDB { return &mirDB{swept: make(chan struct{}, 8)} }

func (d *mirDB) record(sql string) {
	d.mu.Lock()
	d.statements = append(d.statements, sql)
	d.mu.Unlock()
}

func (d *mirDB) countMatching(sub string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, s := range d.statements {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

func (d *mirDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	d.record(sql)
	if strings.Contains(sql, "FROM sessions s") {
		select {
		case d.swept <- struct{}{}:
		default:
		}
	}
	return mirRows{}, nil
}

func (d *mirDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	d.record(sql)
	return mirRow{}
}

func (d *mirDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.record(sql)
	return pgconn.CommandTag{}, nil
}

// mirRows is an empty result set. Only Next, Scan, Err and Close are reachable
// from server/slack; the rest satisfy pgx.Rows.
type mirRows struct{}

func (mirRows) Next() bool                                   { return false }
func (mirRows) Scan(...any) error                            { return nil }
func (mirRows) Err() error                                   { return nil }
func (mirRows) Close()                                       {}
func (mirRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (mirRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (mirRows) Values() ([]any, error)                       { return nil, nil }
func (mirRows) RawValues() [][]byte                          { return nil }
func (mirRows) Conn() *pgx.Conn                              { return nil }

type mirRow struct{}

func (mirRow) Scan(...any) error { return pgx.ErrNoRows }

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// mirConfig loads a configuration with the Slack token set as given. An empty
// value means the variable is absent, which is the shipped default.
func mirConfig(t *testing.T, token string) Config {
	t.Helper()
	env := bootEnv()
	if token != "" {
		env["SLACK_BOT_TOKEN"] = token
	}
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return cfg
}

// mirBuild assembles the whole server the way New does, over fakes, and hands
// back the log it wrote while doing so.
func mirBuild(t *testing.T, cfg Config, mdb slack.DB) (*App, string) {
	t.Helper()
	var out bytes.Buffer
	log := NewLogger(slog.LevelInfo, &out)
	st := store.NewWithDB(&bootDB{}, ingest.NewPricer(nil, log))
	var a *App
	var err error
	if mdb == nil {
		// The five-argument form every caller that predates the mirror uses.
		a, err = build(context.Background(), cfg, log, st, func(context.Context) error { return nil })
	} else {
		a, err = build(context.Background(), cfg, log, st, func(context.Context) error { return nil }, mdb)
	}
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return a, out.String()
}

// mirDo sends a request to the preference path through the assembled handler.
// The content type is always JSON, because that is what the route requires and
// a 415 here would mask the 404 these tests are actually looking for.
func mirDo(t *testing.T, h http.Handler, method, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, slack.PrefsPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ---------------------------------------------------------------------------
// The caller
// ---------------------------------------------------------------------------

// TestServeActuallyRunsTheMirrorAndStopsWhenItReturns is the assertion this
// file exists for.
//
// It drives the real serve() over a real listener rather than calling Run
// directly, because "the mirror posts" and "the assembled server runs the
// mirror" are different claims and only the second one puts a message in
// somebody's Slack. A poster nothing invokes is the seventh instance of the
// defect this codebase already had six of, and every test under server/slack
// would still be green.
//
// The second half is the other side of the same wiring. main defers App.Close
// immediately after New, so a mirror still issuing statements after serve
// returns is one issuing them against a closed pool.
func TestServeActuallyRunsTheMirrorAndStopsWhenItReturns(t *testing.T) {
	mdb := newMirDB()
	a, _ := mirBuild(t, mirConfig(t, "xoxb-test-token"), mdb)
	if a.mirror == nil {
		t.Fatal("build produced no mirror from a configuration that has a bot token and a connection source")
	}
	// A cadence a test can wait for. The shipped interval is a minute, which is
	// the right gap between passes and the wrong one for a test, so the mirror
	// is rebuilt over the same dependencies with a shorter one.
	a.mirror = mirWithInterval(t, a.cfg, mdb, time.Millisecond)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- a.serve(ctx, ln) }()

	select {
	case <-mdb.swept:
	case <-time.After(5 * time.Second):
		t.Fatal("the assembled server never swept for sessions to mirror: slack.Mirror.Run has no caller, which is the defect this was written to fix")
	}

	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(drainTimeout + 2*time.Second):
		t.Fatal("serve did not return; the mirror join is holding the shutdown open")
	}

	// serve has returned, so the pool is about to be closed. Nothing may run
	// after this point.
	settled := mdb.countMatching("FROM sessions s")
	time.Sleep(50 * time.Millisecond)
	if after := mdb.countMatching("FROM sessions s"); after != settled {
		t.Errorf("the mirror issued %d further queries after serve returned; it would be reading a closed pool", after-settled)
	}
}

// mirWithInterval rebuilds the mirror over the same dependencies the
// composition root gave it, with a cadence a test can observe.
func mirWithInterval(t *testing.T, cfg Config, mdb slack.DB, every time.Duration) *slack.Mirror {
	t.Helper()
	client, err := slack.NewClient(slack.ClientOptions{Token: cfg.SlackBotToken})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	m, err := slack.New(slack.Options{
		DB:        mdb,
		Slack:     client,
		PublicURL: cfg.PublicURL,
		Viewer:    func(*http.Request) (string, bool) { return "", false },
		Logger:    bootLogger(),
		Interval:  every,
	})
	if err != nil {
		t.Fatalf("slack.New: %v", err)
	}
	return m
}

// TestThePreferenceRouteIsMountedOnTheAssembledServer is the other half of the
// wiring, and it fails for the same reason the loop test does: a route that
// server/slack registers on its own mux is not a route this service serves.
//
// The assertion is that the path answers as itself. A 401 is this route
// refusing an unauthenticated caller; a 404 is the read API's fallback saying
// nothing is mounted there at all, which is exactly what an unwired Register
// produces.
func TestThePreferenceRouteIsMountedOnTheAssembledServer(t *testing.T) {
	a, _ := mirBuild(t, mirConfig(t, "xoxb-test-token"), newMirDB())

	tests := []struct {
		name   string
		method string
		body   string
	}{
		{"reading a preference", http.MethodGet, ""},
		{"setting a preference", http.MethodPut, `{"mode":"dm"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := mirDo(t, a.handler, tc.method, tc.body)

			if rec.Code == http.StatusNotFound {
				t.Fatalf("%s %s answered 404: the mirror's Register was never called from routes()", tc.method, slack.PrefsPath)
			}
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("answered %d, want 401 from the route itself", rec.Code)
			}
			// And it is this package's JSON error, not the standard library's
			// plain-text one, so a client under /v1/ sees one vocabulary.
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("the refusal is not JSON: %v (%s)", err, rec.Body)
			}
			if body.Error.Code != "unauthenticated" {
				t.Errorf("error code %q, want unauthenticated", body.Error.Code)
			}
		})
	}
}

// TestAVerbThePreferenceRouteDoesNotServeIsAFourOhFour, matching every other
// route under /v1/. What is at an address should not vary with how the question
// was asked.
func TestAVerbThePreferenceRouteDoesNotServeIsAFourOhFour(t *testing.T) {
	a, _ := mirBuild(t, mirConfig(t, "xoxb-test-token"), newMirDB())
	rec := mirDo(t, a.handler, http.MethodPost, `{"mode":"dm"}`)
	if rec.Code != http.StatusNotFound {
		t.Errorf("POST answered %d, want the read API's 404", rec.Code)
	}
}

// TestWithoutABotTokenThereIsNoMirrorAndNoWayToAskForOne.
//
// The two have to move together. A preference somebody can set that nothing
// will ever act on is worse than a feature that is plainly absent: the first
// produces a bug report nobody can reproduce, and the second produces a
// question with an answer. So a server with no token mounts no route, and the
// path falls through to the same 404 as any other address that has nothing at
// it.
func TestWithoutABotTokenThereIsNoMirrorAndNoWayToAskForOne(t *testing.T) {
	tests := []struct {
		name  string
		token string
		mdb   slack.DB
	}{
		{"no token, which is the shipped default", "", newMirDB()},
		{"a token but no connection source", "xoxb-test-token", nil},
		{"neither", "", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, logged := mirBuild(t, mirConfig(t, tc.token), tc.mdb)
			if a.mirror != nil {
				t.Fatal("a mirror was built from an incomplete configuration")
			}

			rec := mirDo(t, a.handler, http.MethodPut, `{"mode":"dm"}`)
			if rec.Code != http.StatusNotFound {
				t.Errorf("the preference route answered %d with no mirror behind it, want 404", rec.Code)
			}

			// Said once at boot. "I turned the mirror on and nothing happened"
			// is otherwise only answerable by reading routes().
			if !strings.Contains(logged, "slack mirror is not configured") {
				t.Errorf("the boot log does not say the mirror is off:\n%s", logged)
			}

			// And starting it is a no-op rather than a panic on the first tick.
			done := a.startMirror(context.Background())
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("startMirror started a goroutine for a mirror that does not exist")
			}
		})
	}
}

// TestASlackDisabledServerStillServesEverythingElse. The mirror is an addition,
// and a deployment with no workspace must not be a deployment with no
// dashboard.
func TestASlackDisabledServerStillServesEverythingElse(t *testing.T) {
	a, _ := mirBuild(t, mirConfig(t, ""), nil)
	if rec := bootDo(t, a.handler, http.MethodGet, HealthzPath, nil); rec.Code != http.StatusOK {
		t.Errorf("healthz answered %d on a server with no mirror", rec.Code)
	}
}

// TestTheBotTokenIsNeverLogged. It is the one credential this service holds
// that authorises it to act somewhere else, so its exposure is not bounded by
// this database; Cloud Logging keeps a boot line for thirty days and shows it
// to everyone with project read access.
func TestTheBotTokenIsNeverLogged(t *testing.T) {
	const token = "xoxb-0000000000000-secret-value-nobody-should-see"
	cfg := mirConfig(t, token)

	var out bytes.Buffer
	log := NewLogger(slog.LevelInfo, &out)
	log.Info("config", "config", cfg)
	if strings.Contains(out.String(), token) {
		t.Fatalf("the bot token reached the log:\n%s", out.String())
	}
	if !strings.Contains(out.String(), `"slack_bot_token":"set"`) {
		t.Errorf("the log does not report whether the token is set, which is the whole answer to 'why is nothing mirrored':\n%s", out.String())
	}

	// And the absent case reports absent rather than being omitted, so the
	// question is answerable from the line either way.
	out.Reset()
	NewLogger(slog.LevelInfo, &out).Info("config", "config", mirConfig(t, ""))
	if !strings.Contains(out.String(), `"slack_bot_token":"unset"`) {
		t.Errorf("an unset token is not reported at all:\n%s", out.String())
	}
}

// TestTheMirrorViewerReportsTheCallerAndNobodyElse. The port has no way to
// express a different subject, which is what makes the preference route's
// authorization argument structural rather than a check somebody could forget.
func TestTheMirrorViewerReportsTheCallerAndNobodyElse(t *testing.T) {
	cookies, err := auth.NewCookies(auth.CookieOptions{Keys: [][]byte{bytes.Repeat([]byte("k"), 32)}})
	if err != nil {
		t.Fatalf("cookies: %v", err)
	}
	db := &bootDB{}
	st := store.NewWithDB(db, ingest.NewPricer(nil, bootLogger()))
	viewer := NewSlackViewer(cookies, st)

	// No cookie at all is the ordinary unauthenticated case.
	if _, ok := viewer(httptest.NewRequest(http.MethodGet, slack.PrefsPath, nil)); ok {
		t.Error("a request with no session resolved to somebody")
	}
}
