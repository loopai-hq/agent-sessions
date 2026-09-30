package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// The fixtures every test in this package shares.
//
// Two of them, and the split is deliberate. fakeSlack is an httptest server
// answering Slack's own wire format, so the real *Client is what every test
// exercises: the pacing, the 429 handling and the fact that ok:false inside a
// 200 is a failure are the interesting parts of that file and a mocked
// interface would test none of them. fakeDB is the opposite — it stands in for
// Postgres so the mirror's control flow can be driven without one, and it
// deliberately does NOT reimplement the SQL. What the statements actually mean
// is settled against a real Postgres in mirror_integration_test.go, because a
// fake that reimplemented ON CONFLICT would be a test of the reimplementation.

// ---------------------------------------------------------------------------
// A fake Slack
// ---------------------------------------------------------------------------

// slackCall is one request the bot made.
type slackCall struct {
	Method  string
	Channel string
	Text    string
	Blocks  string
	Email   string
	Users   string
	Token   string
}

// fakeSlack answers the four methods this package calls.
//
// Its default is success, so a test names only the failure it is about. Every
// call is recorded, which is what lets a test assert the thing that matters
// most here: that nothing was posted at all.
type fakeSlack struct {
	srv *httptest.Server

	mu    sync.Mutex
	calls []slackCall

	// reply overrides the response for a method. Nil means the default success.
	reply map[string]func(w http.ResponseWriter, r *http.Request)
	// postErr, when set, makes chat.postMessage answer ok:false with this code.
	postErr string
	// lookupErr does the same for users.lookupByEmail.
	lookupErr string
	// joined records the channels conversations.join was asked for.
	joined []string
	// postsBeforeJoin makes chat.postMessage answer not_in_channel until the
	// channel has been joined, which is the sequence the real API produces.
	needJoin map[string]bool
}

func newFakeSlack(t *testing.T) *fakeSlack {
	t.Helper()
	f := &fakeSlack{
		reply:    map[string]func(http.ResponseWriter, *http.Request){},
		needJoin: map[string]bool{},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeSlack) serve(w http.ResponseWriter, r *http.Request) {
	method := strings.TrimPrefix(r.URL.Path, "/")
	_ = r.ParseForm()

	f.mu.Lock()
	f.calls = append(f.calls, slackCall{
		Method:  method,
		Channel: r.PostForm.Get("channel"),
		Text:    r.PostForm.Get("text"),
		Blocks:  r.PostForm.Get("blocks"),
		Email:   r.PostForm.Get("email"),
		Users:   r.PostForm.Get("users"),
		Token:   strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "),
	})
	override := f.reply[method]
	postErr, lookupErr := f.postErr, f.lookupErr
	needJoin := f.needJoin[r.PostForm.Get("channel")]
	f.mu.Unlock()

	if override != nil {
		override(w, r)
		return
	}

	switch method {
	case "chat.postMessage":
		switch {
		case postErr != "":
			writeSlackJSON(w, map[string]any{"ok": false, "error": postErr})
		case needJoin:
			writeSlackJSON(w, map[string]any{"ok": false, "error": "not_in_channel"})
		default:
			writeSlackJSON(w, map[string]any{"ok": true, "ts": fmt.Sprintf("%d.000100", time.Now().UnixNano())})
		}
	case "chat.update":
		writeSlackJSON(w, map[string]any{"ok": true, "ts": r.PostForm.Get("ts")})
	case "conversations.join":
		f.mu.Lock()
		f.joined = append(f.joined, r.PostForm.Get("channel"))
		delete(f.needJoin, r.PostForm.Get("channel"))
		f.mu.Unlock()
		writeSlackJSON(w, map[string]any{"ok": true})
	case "users.lookupByEmail":
		if lookupErr != "" {
			writeSlackJSON(w, map[string]any{"ok": false, "error": lookupErr})
			return
		}
		writeSlackJSON(w, map[string]any{"ok": true, "user": map[string]any{"id": "U" + strings.ToUpper(shortHash(r.PostForm.Get("email")))}})
	case "conversations.open":
		writeSlackJSON(w, map[string]any{"ok": true, "channel": map[string]any{"id": "D" + strings.ToUpper(shortHash(r.PostForm.Get("users")))}})
	default:
		writeSlackJSON(w, map[string]any{"ok": false, "error": "unknown_method"})
	}
}

func writeSlackJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(v)
}

// shortHash keeps a fake id stable per input without pulling in a hash import
// for something nothing depends on.
func shortHash(s string) string {
	var n uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		n = (n ^ uint32(s[i])) * 16777619
	}
	return fmt.Sprintf("%08x", n)
}

func (f *fakeSlack) client(t *testing.T) *Client {
	t.Helper()
	c, err := NewClient(ClientOptions{
		Token:   "xoxb-fake-token",
		BaseURL: f.srv.URL,
		// The pacing is real code and is exercised by its own test; everywhere
		// else it would add a second per message to a test run for nothing.
		Now:   func() time.Time { return time.Time{} },
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func (f *fakeSlack) snapshot() []slackCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]slackCall(nil), f.calls...)
}

// posts returns just the messages that were actually sent.
func (f *fakeSlack) posts() []slackCall {
	var out []slackCall
	for _, c := range f.snapshot() {
		if c.Method == "chat.postMessage" {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeSlack) set(fn func(*fakeSlack)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

// ---------------------------------------------------------------------------
// A fake connection source
// ---------------------------------------------------------------------------

// fakeDB answers the six statements this package issues, dispatching on a
// substring that identifies each one unambiguously.
//
// It models state rather than SQL: a set of due candidates, a map of claimed
// keys and a count of what has been posted. That is enough to drive every
// branch of a pass, and it is honest about its limits — ON CONFLICT, the
// anti-join and the lease predicate are Postgres' to enforce and are asserted
// against a real one elsewhere.
type fakeDB struct {
	mu sync.Mutex

	// due is what the eligibility query will offer, in order.
	due []candidate
	// groupActs is what the group-activation query offers; roots computed
	// from ledger state like the live rows below.
	groupActs []groupActivation
	// live is what the live-eligibility query will offer. RootTS/RootChannel
	// are computed from the ledger state at read time, the way the real join
	// does, so a test that lets a pass open a root sees it on the next read.
	live []liveCandidate
	// turns and miles are what the live turn/milestone reads offer; posted
	// ones are filtered against the ledger the way the real anti-join does.
	turns map[string][]liveTurn
	miles map[string][]liveMilestone
	// claimed maps a key to whether it has been posted. A key present and false
	// is claimed-but-unsent, which is what a crashed pass leaves behind.
	claimed map[string]bool
	// postedFor counts settled messages per person, which is what the daily cap
	// is measured against.
	postedFor map[string]int
	// attempts counts claims per key.
	attempts map[string]int

	// prefs backs readPrefs and savePrefs.
	prefs map[string]Prefs

	// statements records every SQL string issued, so a test can assert that a
	// query was made at all — which is the whole of the caller proof.
	statements []string
	// bound records the arguments each statement was issued with, so a test can
	// assert not just that a query ran but that a bound value reached it.
	bound [][]any

	// failOn fails any statement containing this substring.
	failOn string
	// maxAttempts mirrors the mirror's own budget so a claim can be refused the
	// way the real WHERE clause refuses it.
	maxAttempts int
}

func newFakeDB() *fakeDB {
	return &fakeDB{
		turns:       map[string][]liveTurn{},
		miles:       map[string][]liveMilestone{},
		claimed:     map[string]bool{},
		postedFor:   map[string]int{},
		attempts:    map[string]int{},
		prefs:       map[string]Prefs{},
		maxAttempts: defaultMaxAttempts,
	}
}

// record logs a statement and refuses one whose placeholders and arguments
// disagree.
//
// This fake does not interpret SQL, on purpose — but arity is not
// interpretation, it is arithmetic over the text, and it is the one class of
// error a fake that ignores SQL entirely will wave through forever. A query
// referencing $4 while binding three arguments is rejected by Postgres every
// single time it runs, so a fake that accepts it turns a permanent production
// failure into a green test. One shipped: liveMilestones filtered on $4 and
// bound three, and the live Slack mirror logged "expected 4 arguments, got 3"
// and stalled on every pass until it was found in the logs rather than here.
//
// The error text copies pgx's so a test that hits this reads like the incident.
func (d *fakeDB) record(sql string, args []any) error {
	d.statements = append(d.statements, sql)
	d.bound = append(d.bound, args)
	if want := maxPlaceholder(sql); want != len(args) {
		return fmt.Errorf("expected %d arguments, got %d", want, len(args))
	}
	return nil
}

// maxPlaceholder returns the highest $N referenced in a statement.
//
// The highest rather than the count of distinct ones: pgx sizes the parameter
// list by the largest ordinal, so a statement using $1 and $3 needs three
// arguments whether or not anything mentions $2.
func maxPlaceholder(sql string) int {
	high := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] != '$' {
			continue
		}
		n, digits := 0, 0
		for j := i + 1; j < len(sql) && sql[j] >= '0' && sql[j] <= '9'; j++ {
			n, digits = n*10+int(sql[j]-'0'), digits+1
		}
		if digits > 0 && n > high {
			high = n
		}
		i += digits
	}
	return high
}

func (d *fakeDB) issued(sub string) int {
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

// argsFor returns the arguments bound to the first statement matching sub.
// endedFor reports whether the candidate for a session has announced its
// end, which is what settles its last turn. Called with the lock held.
func (d *fakeDB) endedFor(sid string) bool {
	for _, c := range d.live {
		if c.SessionID == sid {
			return c.Ended
		}
	}
	for _, a := range d.groupActs {
		if a.SessionID == sid {
			return a.Ended
		}
	}
	return false
}

func (d *fakeDB) argsFor(sub string) []any {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, s := range d.statements {
		if strings.Contains(s, sub) {
			return d.bound[i]
		}
	}
	return nil
}

func (d *fakeDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.record(sql, args); err != nil {
		return nil, err
	}
	if d.failOn != "" && strings.Contains(sql, d.failOn) {
		return nil, fmt.Errorf("fakeDB: %s refused", d.failOn)
	}
	switch {
	case strings.Contains(sql, "JOIN slack_groups g"):
		var rows [][]any
		for _, a := range d.groupActs {
			prefix := groupThreadPrefix(a.SessionID, a.GroupID)
			rootTS, rootCh := "", ""
			if d.claimed[liveKey(prefix)] {
				rootTS, rootCh = "200.000001", a.RootChannel
				if rootCh == "" {
					rootCh = a.Destination
				}
			}
			if d.claimed[liveCloseKey(prefix)] {
				continue
			}
			rows = append(rows, []any{
				a.SessionID, a.Email, a.Repo, a.Branch,
				a.StartedAt, a.UpdatedAt, a.Ended,
				a.UserTurns, a.ToolCalls, a.Errors, a.CostUSD,
				a.SlackUserID,
				a.GroupID, a.GroupName, a.Destination, a.AttachedAt,
				rootTS, rootCh,
			})
		}
		return &fakeRows{rows: rows}, nil

	case strings.Contains(sql, "mirror_request"):
		var rows [][]any
		for _, c := range d.live {
			rootTS, rootCh := "", ""
			if d.claimed[liveKey(threadPrefix(c.SessionID))] {
				rootTS, rootCh = "100.000001", c.RootChannel
				if rootCh == "" {
					rootCh = "C123"
				}
			}
			rows = append(rows, []any{
				c.SessionID, c.Email, c.Repo, c.Branch,
				c.StartedAt, c.UpdatedAt, c.Ended,
				c.UserTurns, c.ToolCalls, c.Errors, c.CostUSD,
				c.Request, c.Mode, c.Channel, c.SlackUserID,
				rootTS, rootCh,
				d.claimed[liveCloseKey(threadPrefix(c.SessionID))],
			})
		}
		return &fakeRows{rows: rows}, nil

	case strings.Contains(sql, "FROM turns t"):
		sid, _ := args[0].(string)
		prefix, _ := args[2].(string)
		after, _ := args[3].(time.Time)
		turns := d.turns[sid]
		var rows [][]any
		for i, t := range turns {
			if d.claimed[liveTurnKey(prefix, t.Seq)] || t.OccurredAt.Before(after) {
				continue
			}
			// The last turn of a running session is the fold's in_progress
			// outcome and waits; the end of the session settles it, which
			// the real query reads off turns.outcome.
			if i == len(turns)-1 && !d.endedFor(sid) {
				continue
			}
			rows = append(rows, []any{t.Seq, t.OccurredAt, t.Kind, t.Text, t.Reply})
		}
		return &fakeRows{rows: rows}, nil

	case strings.Contains(sql, "SELECT kind, about"):
		sid, _ := args[0].(string)
		prefix, _ := args[2].(string)
		var rows [][]any
		for _, m := range d.miles[sid] {
			if d.claimed[liveMilestoneKey(prefix, m.About)] {
				continue
			}
			rows = append(rows, []any{m.Kind, m.About})
		}
		return &fakeRows{rows: rows}, nil // the after horizon is exercised in the integration test

	case strings.Contains(sql, "FROM sessions s"):
		return &fakeRows{rows: rowsFor(d.due)}, nil
	}
	return nil, fmt.Errorf("fakeDB: unexpected Query: %s", sql)
}

func (d *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.record(sql, args); err != nil {
		return errRow{err: err}
	}
	if d.failOn != "" && strings.Contains(sql, d.failOn) {
		return errRow{err: fmt.Errorf("fakeDB: %s refused", d.failOn)}
	}

	switch {
	case strings.Contains(sql, "INSERT INTO slack_posts"):
		key, _ := args[0].(string)
		posted, exists := d.claimed[key]
		// The conflict branch: a settled key is never re-claimed, and a key that
		// has spent its attempt budget is abandoned. The lease is not modelled —
		// see the type comment.
		if exists && (posted || d.attempts[key] >= d.maxAttempts) {
			return errRow{err: pgx.ErrNoRows}
		}
		d.claimed[key] = false
		d.attempts[key]++
		return valueRow{values: []any{d.attempts[key]}}

	case strings.Contains(sql, "count(*) FROM slack_posts") && strings.Contains(sql, "key LIKE"):
		sid, _ := args[0].(string)
		n := 0
		for key, posted := range d.claimed {
			if posted && strings.HasPrefix(key, "thread:"+sid+":") {
				n++
			}
		}
		return valueRow{values: []any{n}}

	case strings.Contains(sql, "count(*) FROM slack_posts"):
		email, _ := args[0].(string)
		return valueRow{values: []any{d.postedFor[email]}}

	case strings.Contains(sql, "FROM slack_prefs"):
		email, _ := args[0].(string)
		p, ok := d.prefs[email]
		if !ok {
			return errRow{err: pgx.ErrNoRows}
		}
		return valueRow{values: []any{p.Mode, p.Channel, p.SlackUserID, p.MirrorFrom, p.UpdatedAt}}
	}
	return errRow{err: fmt.Errorf("fakeDB: unexpected QueryRow: %s", sql)}
}

func (d *fakeDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.record(sql, args); err != nil {
		return pgconn.CommandTag{}, err
	}
	if strings.Contains(sql, "INSERT INTO session_mirrors") {
		return pgconn.NewCommandTag("INSERT 0 0"), nil
	}
	if d.failOn != "" && strings.Contains(sql, d.failOn) {
		return pgconn.CommandTag{}, fmt.Errorf("fakeDB: %s refused", d.failOn)
	}

	switch {
	case strings.Contains(sql, "SET posted_at"):
		key, _ := args[0].(string)
		d.claimed[key] = true
		for _, c := range d.due {
			if sessionKey(c.SessionID) == key {
				d.postedFor[c.Email]++
			}
		}
		if strings.HasPrefix(key, "cap:") {
			// cap:<email>:<day>
			parts := strings.Split(key, ":")
			if len(parts) >= 2 {
				d.postedFor[parts[1]]++
			}
		}
	case strings.Contains(sql, "SET last_error"), strings.Contains(sql, "last_error = $2"):
		if len(args) >= 3 {
			key, _ := args[0].(string)
			if n, ok := args[2].(int); ok {
				d.attempts[key] = n
			}
		}
	case strings.Contains(sql, "INSERT INTO slack_prefs"):
		email, _ := args[0].(string)
		mode, _ := args[1].(string)
		channel, _ := args[2].(string)
		userID, _ := args[3].(string)
		now, _ := args[4].(time.Time)
		prev := d.prefs[email]
		from := now
		if Mode(mode) == ModeOff {
			from = prev.MirrorFrom
		}
		d.prefs[email] = Prefs{
			Email: email, Mode: Mode(mode), Channel: channel,
			SlackUserID: userID, MirrorFrom: from, UpdatedAt: now,
		}
	case strings.Contains(sql, "UPDATE slack_prefs SET slack_user_id"):
		email, _ := args[0].(string)
		userID, _ := args[1].(string)
		p := d.prefs[email]
		p.SlackUserID = userID
		d.prefs[email] = p
	}
	return pgconn.CommandTag{}, nil
}

// rowsFor flattens candidates into the column order eligible scans.
func rowsFor(cs []candidate) [][]any {
	out := make([][]any, 0, len(cs))
	for _, c := range cs {
		out = append(out, []any{
			c.SessionID, c.Email, c.Repo, c.Branch,
			c.StartedAt, c.EndedAt, c.Ended,
			c.UserTurns, c.ToolCalls, c.Subagents, c.Errors, c.CostUSD,
			c.Mode, c.Channel, c.SlackUserID,
		})
	}
	return out
}

// fakeRows is the pgx.Rows this package consumes. Only Next, Scan, Err and
// Close are reachable from server/slack; the rest satisfy the interface.
type fakeRows struct {
	rows [][]any
	i    int
}

func (r *fakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fakeRows) Scan(dest ...any) error { return assign(r.rows[r.i-1], dest) }
func (r *fakeRows) Err() error             { return nil }
func (r *fakeRows) Close()                 {}

func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) TypeMap() *pgtype.Map                         { return pgtype.NewMap() }

type valueRow struct{ values []any }

func (r valueRow) Scan(dest ...any) error { return assign(r.values, dest) }

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

// assign copies a fixture row into scan destinations, converting where the
// types differ the way the driver would.
func assign(values []any, dest []any) error {
	if len(values) != len(dest) {
		return fmt.Errorf("fakeDB: scanning %d values into %d destinations", len(values), len(dest))
	}
	for i := range dest {
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Pointer || dv.IsNil() {
			return fmt.Errorf("fakeDB: destination %d is not a pointer", i)
		}
		v := reflect.ValueOf(values[i])
		target := dv.Elem().Type()
		if v.Type() != target {
			if !v.Type().ConvertibleTo(target) {
				return fmt.Errorf("fakeDB: cannot put %T into %s", values[i], target)
			}
			v = v.Convert(target)
		}
		dv.Elem().Set(v)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testMirror assembles a Mirror over the fakes, with the options a test would
// otherwise repeat.
func testMirror(t *testing.T, db DB, poster Poster, mutate func(*Options)) *Mirror {
	t.Helper()
	o := Options{
		DB:        db,
		Slack:     poster,
		PublicURL: "https://sessions.example.com",
		Viewer:    func(*http.Request) (string, bool) { return "", false },
		Logger:    testLogger(),
		Now:       func() time.Time { return time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC) },
	}
	if mutate != nil {
		mutate(&o)
	}
	m, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

// sampleCandidate is a session substantial enough to be mirrored.
func sampleCandidate(sessionID, email string, mode Mode) candidate {
	start := time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)
	return candidate{
		Summary: Summary{
			SessionID: sessionID,
			Email:     email,
			Repo:      "loop-sessions",
			Branch:    "claude/session-platform",
			StartedAt: start,
			EndedAt:   start.Add(35 * time.Minute),
			Ended:     true,
			UserTurns: 6,
			ToolCalls: 40,
			CostUSD:   1.25,
		},
		Mode:    mode,
		Channel: map[Mode]string{ModeChannel: "C123"}[mode],
	}
}
