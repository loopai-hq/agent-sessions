package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/health"
	"github.com/loopai-hq/loop-sessions/server/store/derive"
)

// The properties worth pinning down in this package are which statement is
// issued, with which arguments, and inside which transaction. None of those
// need a database to observe, and a test that needs one does not run on a
// laptop with no Postgres, which is where most of these will be run. Anything
// that genuinely depends on the server evaluating SQL lives in
// integration_test.go behind the integration build tag.

// ---------------------------------------------------------------------------
// A scriptable fake connection
// ---------------------------------------------------------------------------

type call struct {
	kind string // query | queryrow | exec
	sql  string
	args []any
	inTx bool
}

// stub answers the first statement whose SQL contains match.
type stub struct {
	match    string
	rows     [][]any
	affected int64
	err      error
	used     bool
	// once limits the stub to a single statement, for tests where the same
	// table is written twice with different expectations.
	once bool
}

type fakeDB struct {
	mu        sync.Mutex
	stubs     []*stub
	calls     []call
	begun     int
	committed int
	rolled    int
	beginErr  error
}

func (f *fakeDB) record(c call) { f.mu.Lock(); f.calls = append(f.calls, c); f.mu.Unlock() }

func (f *fakeDB) answer(sql string) *stub {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.stubs {
		if s.once && s.used {
			continue
		}
		if strings.Contains(sql, s.match) {
			s.used = true
			return s
		}
	}
	return nil
}

func (f *fakeDB) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	f.record(call{kind: "query", sql: sql, args: args})
	if s := f.answer(sql); s != nil {
		if s.err != nil {
			return nil, s.err
		}
		return &fakeRows{rows: s.rows}, nil
	}
	return &fakeRows{}, nil
}

func (f *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) Row {
	f.record(call{kind: "queryrow", sql: sql, args: args})
	if s := f.answer(sql); s != nil {
		if s.err != nil {
			return errRow{err: s.err}
		}
		if len(s.rows) > 0 {
			return &fakeRows{rows: s.rows[:1], i: 1}
		}
	}
	return errRow{err: pgx.ErrNoRows}
}

func (f *fakeDB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	f.record(call{kind: "exec", sql: sql, args: args})
	if s := f.answer(sql); s != nil {
		return s.affected, s.err
	}
	return 1, nil
}

func (f *fakeDB) Begin(ctx context.Context) (Tx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	f.mu.Lock()
	f.begun++
	f.mu.Unlock()
	return &fakeTx{db: f}, nil
}

type fakeTx struct {
	db   *fakeDB
	done bool
}

func (t *fakeTx) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := t.db.Query(ctx, sql, args...)
	t.db.markTx()
	return rows, err
}

func (t *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) Row {
	row := t.db.QueryRow(ctx, sql, args...)
	t.db.markTx()
	return row
}

func (t *fakeTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	n, err := t.db.Exec(ctx, sql, args...)
	t.db.markTx()
	return n, err
}

func (t *fakeTx) Commit(ctx context.Context) error {
	if t.done {
		return errors.New("commit after finish")
	}
	t.done = true
	t.db.mu.Lock()
	t.db.committed++
	t.db.mu.Unlock()
	return nil
}

func (t *fakeTx) Rollback(ctx context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	t.db.mu.Lock()
	t.db.rolled++
	t.db.mu.Unlock()
	return nil
}

// markTx flags the statement just recorded as having run inside a transaction,
// which is the property the audit tests actually care about.
func (f *fakeDB) markTx() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n := len(f.calls); n > 0 {
		f.calls[n-1].inTx = true
	}
}

type fakeRows struct {
	rows [][]any
	i    int
	err  error
}

func (r *fakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("scan outside a row")
	}
	return assign(r.rows[r.i-1], dest)
}

func (r *fakeRows) Err() error { return r.err }
func (r *fakeRows) Close()     {}

type errRow struct{ err error }

func (e errRow) Scan(dest ...any) error { return e.err }

func assign(src, dest []any) error {
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

func (f *fakeDB) find(t *testing.T, match string) call {
	t.Helper()
	for _, c := range f.calls {
		if strings.Contains(c.sql, match) {
			return c
		}
	}
	t.Fatalf("no statement containing %q; saw:\n%s", match, f.summary())
	return call{}
}

func (f *fakeDB) count(match string) int {
	var n int
	for _, c := range f.calls {
		if strings.Contains(c.sql, match) {
			n++
		}
	}
	return n
}

func (f *fakeDB) summary() string {
	var b strings.Builder
	for _, c := range f.calls {
		line := strings.Join(strings.Fields(c.sql), " ")
		if len(line) > 110 {
			line = line[:110]
		}
		fmt.Fprintf(&b, "  [%s tx=%v] %s\n", c.kind, c.inTx, line)
	}
	return b.String()
}

const (
	sqlEventInsert  = "INSERT INTO events"
	sqlClaimSession = "INSERT INTO sessions (session_id, email, device_id, source, started_at)"
	sqlRollup       = "sessions.user_turns + EXCLUDED.user_turns"
	sqlUsageLedger  = "INSERT INTO usage_ledger"
	sqlMessages     = "INSERT INTO messages"
	sqlAccessLog    = "INSERT INTO access_log"
	sqlFirstPrompt  = "SET first_prompt = "
)

// Argument positions inside the rollup statement, named so the assertions read
// as claims about the rollup rather than as index arithmetic.
const (
	argUserTurns   = 11
	argToolCalls   = 12
	argSubagents   = 13
	argErrors      = 14
	argTokensIn    = 16
	argTokensOut   = 17
	argCacheRead   = 18
	argCacheWrite  = 19
	argCostUSD     = 20
	argRollupEnded = 10
)

var at = time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC)

func ingestOf(id, sessionID, email string, typ event.Type, seq int64) Ingest {
	return Ingest{
		Email: email,
		Event: event.Event{
			ID:         id,
			SessionID:  sessionID,
			Source:     event.SourceClaudeCode,
			Origin:     event.OriginHook,
			Type:       typ,
			Seq:        seq,
			OccurredAt: at.Add(time.Duration(seq) * time.Second),
		},
	}
}

// ingestDB wires the minimum a successful ingest needs: an owner for the
// session claim and a set of ids the event insert reports as new.
//
// The insert returns (id, inserted). These ids are all reported as genuinely
// inserted; the upgraded case — same id, newer capture version, which must NOT
// be credited as new — is exercised against real Postgres, where xmax is what
// distinguishes them and a fake cannot honestly stand in for it.
func ingestDB(owner string, insertedIDs ...string) *fakeDB {
	rows := make([][]any, 0, len(insertedIDs))
	for _, id := range insertedIDs {
		rows = append(rows, []any{id, true})
	}
	return &fakeDB{stubs: []*stub{
		{match: sqlClaimSession, rows: [][]any{{owner}}},
		{match: sqlEventInsert, rows: rows},
	}}
}

func sessionRow(sessionID, owner string) []any {
	return []any{
		sessionID, owner, nil, "claude_code", "user", nil,
		nil, nil, nil, at, nil, false,
		3, 7, 1, 0, nil, []string{"2.0.1"},
		int64(10), int64(20), int64(30), int64(40),
		1.5, []byte(`{"api_key":1}`), at, at,
	}
}

// ---------------------------------------------------------------------------
// Ingest idempotency
// ---------------------------------------------------------------------------

func TestUpsertEventsInsertsIdempotentlyAndSplitsTheAnswer(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)

	res, err := s.UpsertEvents(context.Background(), []Ingest{
		ingestOf("e1", "sess", "me@example.com", event.ToolCall, 1),
		ingestOf("e2", "sess", "me@example.com", event.ToolCall, 2),
	})
	if err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}

	// Idempotency on id is now carried by the version guard rather than by DO
	// NOTHING: a re-delivery arrives at the same capture version, the WHERE
	// fails, and nothing is written or returned. Losing that predicate would
	// turn every redelivery into an overwrite that re-credits its tokens, so it
	// is asserted rather than assumed.
	got := db.find(t, sqlEventInsert).sql
	if !strings.Contains(got, "ON CONFLICT (id) DO UPDATE") {
		t.Errorf("event insert does not upsert on id:\n%s", got)
	}
	if !strings.Contains(got, "WHERE EXCLUDED.capture_version > events.capture_version") {
		t.Errorf("event insert is not idempotent: an equal or older extraction would overwrite:\n%s", got)
	}
	if !strings.Contains(got, "(xmax = 0) AS inserted") {
		t.Errorf("event insert cannot tell a new row from an upgraded one, so usage would be credited twice:\n%s", got)
	}
	if !reflect.DeepEqual(res.Inserted, []string{"e1"}) {
		t.Errorf("Inserted = %v, want [e1]", res.Inserted)
	}
	if !reflect.DeepEqual(res.Duplicate, []string{"e2"}) {
		t.Errorf("Duplicate = %v, want [e2]", res.Duplicate)
	}
	// The client deletes what we accept, so a duplicate has to be accepted or
	// it is retried until the spool quarantines it.
	if got := res.Accepted(); len(got) != 2 {
		t.Errorf("Accepted() = %v, want both ids", got)
	}
}

func TestUpsertEventsCountsOnlyNewEventsIntoTheRollup(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)

	in := []Ingest{
		ingestOf("e1", "sess", "me@example.com", event.UserPrompt, 1),
		ingestOf("e2", "sess", "me@example.com", event.UserPrompt, 2),
	}
	in[0].Event.Text = "first"
	in[1].Event.Text = "second"
	if _, err := s.UpsertEvents(context.Background(), in); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}

	rollup := db.find(t, sqlRollup)
	if got := rollup.args[argUserTurns]; got != 1 {
		t.Errorf("user_turns delta = %v, want 1: a re-delivered event moved a counter", got)
	}
}

func TestUpsertEventsDedupesWithinOneBatch(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)

	res, err := s.UpsertEvents(context.Background(), []Ingest{
		ingestOf("e1", "sess", "me@example.com", event.ToolCall, 1),
		ingestOf("e1", "sess", "me@example.com", event.ToolCall, 1),
	})
	if err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	ids := db.find(t, sqlEventInsert).args[0].([]string)
	if len(ids) != 1 {
		t.Errorf("sent %d ids to the insert, want 1", len(ids))
	}
	if len(res.Duplicate) != 1 {
		t.Errorf("Duplicate = %v, want the repeat reported", res.Duplicate)
	}
}

func TestUpsertEventsRejectsSessionsOwnedByAnotherPrincipal(t *testing.T) {
	db := ingestDB("victim@example.com", "e1")
	s := NewWithDB(db, nil)

	res, err := s.UpsertEvents(context.Background(), []Ingest{
		ingestOf("e1", "victims-session", "attacker@example.com", event.UserPrompt, 1),
	})
	if err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	if len(res.Rejected) != 1 || !strings.Contains(res.Rejected[0].Reason, "another principal") {
		t.Fatalf("Rejected = %+v, want the event refused", res.Rejected)
	}
	if db.count(sqlEventInsert) != 0 {
		t.Errorf("events were written into another principal's session:\n%s", db.summary())
	}
}

func TestUpsertEventsRejectsMalformedEventsPermanently(t *testing.T) {
	db := ingestDB("me@example.com")
	s := NewWithDB(db, nil)

	bad := ingestOf("", "sess", "me@example.com", event.ToolCall, 1)
	noSession := ingestOf("e2", "", "me@example.com", event.ToolCall, 1)
	noTime := ingestOf("e3", "sess", "me@example.com", event.ToolCall, 1)
	noTime.Event.OccurredAt = time.Time{}
	unattributed := ingestOf("e4", "sess", "", event.ToolCall, 1)

	res, err := s.UpsertEvents(context.Background(), []Ingest{bad, noSession, noTime, unattributed})
	if err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	if len(res.Rejected) != 4 {
		t.Fatalf("Rejected = %+v, want all four refused", res.Rejected)
	}
	if db.begun != 0 {
		t.Errorf("opened a transaction for a batch with nothing storable in it")
	}
}

func TestUpsertEventsWritesEverythingInOneTransaction(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)

	in := ingestOf("e1", "sess", "me@example.com", event.UserPrompt, 1)
	in.Event.Text = "hello"
	if _, err := s.UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	for _, want := range []string{sqlClaimSession, sqlEventInsert, sqlMessages, sqlRollup, sqlFirstPrompt} {
		if c := db.find(t, want); !c.inTx {
			t.Errorf("%q ran outside the ingest transaction", want)
		}
	}
	if db.committed != 1 {
		t.Errorf("committed %d times, want 1", db.committed)
	}
}

func TestUpsertEventsRefreshesFirstPromptOnlyWhenAPromptArrived(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)
	if _, err := s.UpsertEvents(context.Background(), []Ingest{
		ingestOf("e1", "sess", "me@example.com", event.ToolCall, 1),
	}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	if db.count(sqlFirstPrompt) != 0 {
		t.Errorf("re-derived the first prompt for a batch that carried no prompt")
	}
}

func TestFirstPromptIsDerivedFromAnIndexLookupNotAScan(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)
	in := ingestOf("e1", "sess", "me@example.com", event.UserPrompt, 1)
	in.Event.Text = "hello"
	if _, err := s.UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	sql := db.find(t, sqlFirstPrompt).sql
	// ORDER BY seq LIMIT 1 against the (session_id, seq) index is a single-row
	// lookup. Anything that aggregates or scans the session would make ingest
	// cost more with every event a session accumulates.
	if !strings.Contains(sql, "ORDER BY seq") || !strings.Contains(sql, "LIMIT 1") {
		t.Errorf("first prompt is not a single-row index lookup:\n%s", sql)
	}
}

// ---------------------------------------------------------------------------
// Event time against ingest time
// ---------------------------------------------------------------------------

func TestIngestNeverWritesIngestedAt(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)
	if _, err := s.UpsertEvents(context.Background(), []Ingest{
		ingestOf("e1", "sess", "me@example.com", event.ToolCall, 1),
	}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	sql := db.find(t, sqlEventInsert).sql
	if strings.Contains(sql, "ingested_at") {
		t.Errorf("ingest supplies its own arrival time, so a backfill could pass "+
			"as live:\n%s", sql)
	}
	if !strings.Contains(sql, "occurred_at") {
		t.Errorf("event time is not stored:\n%s", sql)
	}
}

func TestBackfilledEventsKeepTheirOwnEventTime(t *testing.T) {
	db := ingestDB("me@example.com", "old")
	s := NewWithDB(db, nil)

	old := ingestOf("old", "sess", "me@example.com", event.UserPrompt, 1)
	old.Event.Origin = event.OriginTranscript
	old.Event.OccurredAt = at.AddDate(0, -6, 0)
	old.Event.Text = "six months ago"

	if _, err := s.UpsertEvents(context.Background(), []Ingest{old}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	times := db.find(t, sqlEventInsert).args[6].([]time.Time)
	if !times[0].Equal(old.Event.OccurredAt) {
		t.Errorf("stored occurred_at %v, want the original %v", times[0], old.Event.OccurredAt)
	}
	// The rollup start time has to move backwards for the same reason.
	claim := db.find(t, sqlClaimSession)
	if !strings.Contains(claim.sql, "LEAST(sessions.started_at, EXCLUDED.started_at)") {
		t.Errorf("a late backfill cannot pull a session's start earlier:\n%s", claim.sql)
	}
}

// ---------------------------------------------------------------------------
// Cost
// ---------------------------------------------------------------------------

type flatPricer struct{ perToken float64 }

func (p flatPricer) CostUSD(model string, u event.Usage, _ time.Time) float64 {
	return float64(u.InputTokens+u.OutputTokens) * p.perToken
}

func withUsage(in Ingest, model, messageID, requestID string, input, output int64) Ingest {
	in.Event.Model = model
	in.Event.Usage = &event.Usage{
		InputTokens:  input,
		OutputTokens: output,
		MessageID:    messageID,
		RequestID:    requestID,
	}
	return in
}

func TestUsageExcludesTheSyntheticModel(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, flatPricer{perToken: 1})

	in := withUsage(ingestOf("e1", "sess", "me@example.com", event.AssistantTurn, 1),
		SyntheticModel, "msg-1", "req-1", 100, 10)
	if _, err := s.UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	if db.count(sqlUsageLedger) != 0 {
		t.Errorf("a <synthetic> record was priced; it was never an API call")
	}
	if got := db.find(t, sqlRollup).args[argCostUSD]; got != 0.0 {
		t.Errorf("cost delta = %v, want 0", got)
	}
}

func TestUsageDedupesOnMessageAndRequestID(t *testing.T) {
	db := ingestDB("me@example.com", "e1", "e2")
	s := NewWithDB(db, flatPricer{perToken: 1})

	// The same assistant message appearing in two transcript records: two real
	// and distinct events, one model call.
	first := withUsage(ingestOf("e1", "sess", "me@example.com", event.AssistantTurn, 1),
		"claude-opus-5", "msg-1", "req-1", 100, 10)
	second := withUsage(ingestOf("e2", "sess", "me@example.com", event.ToolResult, 2),
		"claude-opus-5", "msg-1", "req-1", 100, 10)

	if _, err := s.UpsertEvents(context.Background(), []Ingest{first, second}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	ledger := db.find(t, sqlUsageLedger)
	if !strings.Contains(ledger.sql, "ON CONFLICT (message_id, request_id) DO NOTHING") {
		t.Errorf("the ledger does not dedupe on the contract's key:\n%s", ledger.sql)
	}
	if got := len(ledger.args[0].([]string)); got != 1 {
		t.Errorf("sent %d usage rows for one model call, want 1", got)
	}
}

func TestUsageCreditsOnlyWhatTheLedgerAccepted(t *testing.T) {
	// The ledger returns nothing, meaning this model call was already counted by
	// an earlier batch. Nothing may reach the session totals.
	db := &fakeDB{stubs: []*stub{
		{match: sqlClaimSession, rows: [][]any{{"me@example.com"}}},
		{match: sqlEventInsert, rows: [][]any{{"e1", true}}},
		{match: sqlUsageLedger, rows: nil},
	}}
	s := NewWithDB(db, flatPricer{perToken: 0.5})

	in := withUsage(ingestOf("e1", "sess", "me@example.com", event.AssistantTurn, 1),
		"claude-opus-5", "msg-1", "req-1", 100, 10)
	if _, err := s.UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	rollup := db.find(t, sqlRollup)
	if got := rollup.args[argTokensIn]; got != int64(0) {
		t.Errorf("tokens_input delta = %v, want 0 for usage the ledger rejected", got)
	}
	if got := rollup.args[argCostUSD]; got != 0.0 {
		t.Errorf("cost delta = %v, want 0 for usage the ledger rejected", got)
	}
}

func TestUsageIsCreditedWhenTheLedgerAcceptsIt(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: sqlClaimSession, rows: [][]any{{"me@example.com"}}},
		{match: sqlEventInsert, rows: [][]any{{"e1", true}}},
		{match: sqlUsageLedger, rows: [][]any{{"e1", 55.0}}},
	}}
	s := NewWithDB(db, flatPricer{perToken: 0.5})

	in := withUsage(ingestOf("e1", "sess", "me@example.com", event.AssistantTurn, 1),
		"claude-opus-5", "msg-1", "req-1", 100, 10)
	in.Event.Usage.CacheReadTokens = 7
	in.Event.Usage.Ephemeral5m = 3
	in.Event.Usage.Ephemeral1h = 2

	if _, err := s.UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	rollup := db.find(t, sqlRollup)
	if got := rollup.args[argTokensIn]; got != int64(100) {
		t.Errorf("tokens_input = %v, want 100", got)
	}
	if got := rollup.args[argCacheRead]; got != int64(7) {
		t.Errorf("tokens_cache_read = %v, want 7", got)
	}
	if got := rollup.args[argCacheWrite]; got != int64(5) {
		t.Errorf("tokens_cache_write = %v, want the 5m and 1h split summed", got)
	}
	// The cost stored in the ledger is what the session is credited, not a
	// number recomputed here, so the two can never disagree.
	if got := rollup.args[argCostUSD]; got != 55.0 {
		t.Errorf("cost = %v, want the ledger's 55", got)
	}
}

func TestCacheCreationAggregateIsUsedOnlyWithoutTheSplit(t *testing.T) {
	counted := map[string]credited{"e1": {Usage: event.Usage{CacheCreationTokens: 9}}}
	items := []Ingest{ingestOf("e1", "sess", "me@example.com", event.AssistantTurn, 1)}
	items[0].Event.Usage = &event.Usage{CacheCreationTokens: 9}

	d := foldDeltas(items, counted)["sess"]
	if d.TokensCacheWrite != 9 {
		t.Errorf("cache write = %d, want the aggregate when no split is present", d.TokensCacheWrite)
	}

	counted["e1"] = credited{Usage: event.Usage{CacheCreationTokens: 9, Ephemeral5m: 4, Ephemeral1h: 5}}
	d = foldDeltas(items, counted)["sess"]
	if d.TokensCacheWrite != 9 {
		t.Errorf("cache write = %d, want 9 rather than the split added to the aggregate",
			d.TokensCacheWrite)
	}
}

// ---------------------------------------------------------------------------
// Authorization and audit
// ---------------------------------------------------------------------------

func readableSession(sessionID, owner string) *fakeDB {
	return &fakeDB{stubs: []*stub{
		{match: "FROM sessions s", rows: [][]any{sessionRow(sessionID, owner)}},
	}}
}

func TestGetSessionAuditsAForeignReadInsideTheSameTransaction(t *testing.T) {
	db := readableSession("sess", "colleague@example.com")
	s := NewWithDB(db, nil)

	if _, err := s.GetSession(context.Background(),
		Viewer{Email: "admin@example.com", Role: RoleAdmin}, "sess"); err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	audit := db.find(t, sqlAccessLog)
	if !audit.inTx {
		t.Fatalf("the audit row was written outside the read's transaction")
	}
	if got := audit.args[3].([]string); got[0] != AccessViaAdmin {
		t.Errorf("via = %v, want %q", got, AccessViaAdmin)
	}
	if got := audit.args[2].([]string); got[0] != "colleague@example.com" {
		t.Errorf("owner = %v, want the session's owner", got)
	}
	if db.committed != 1 {
		t.Errorf("committed %d times, want 1", db.committed)
	}
}

func TestGetSessionDoesNotAuditYourOwnWork(t *testing.T) {
	db := readableSession("sess", "me@example.com")
	s := NewWithDB(db, nil)

	if _, err := s.GetSession(context.Background(),
		Viewer{Email: "me@example.com", Role: RoleMember}, "sess"); err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if db.count(sqlAccessLog) != 0 {
		t.Errorf("audited a read of the viewer's own session, which is noise that "+
			"buries the reads that matter:\n%s", db.summary())
	}
}

func TestGetSessionFailsWhenTheAuditWriteFails(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "FROM sessions s", rows: [][]any{sessionRow("sess", "colleague@example.com")}},
		{match: sqlAccessLog, err: errors.New("disk full")},
	}}
	s := NewWithDB(db, nil)

	_, err := s.GetSession(context.Background(),
		Viewer{Email: "admin@example.com", Role: RoleAdmin}, "sess")
	if err == nil {
		t.Fatal("read succeeded with no audit trail")
	}
	if db.committed != 0 {
		t.Errorf("committed a transcript read whose audit row failed")
	}
	if db.rolled != 1 {
		t.Errorf("rolled back %d times, want 1", db.rolled)
	}
}

func TestUnauthorizedSessionIsIndistinguishableFromAbsent(t *testing.T) {
	// The predicate lives inside the SELECT, so a session the viewer may not
	// see comes back as no rows and there is no branch that could report it
	// differently.
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	v := Viewer{Email: "nosy@example.com", Role: RoleMember}

	if _, err := s.GetSession(context.Background(), v, "someone-elses"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetSession error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetEvents(context.Background(), v, "someone-elses", EventRange{Limit: 10}); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetEvents error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetSession(context.Background(), v, "does-not-exist"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetSession error = %v, want ErrNotFound", err)
	}
}

func TestNoErrorInThisPackageDistinguishesForbiddenFromAbsentForASession(t *testing.T) {
	// ErrNotAdmin exists, and it must never escape a session read: a viewer who
	// learns "you are not allowed" has learned that the session exists.
	db := readableSession("sess", "colleague@example.com")
	s := NewWithDB(db, nil)
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"GetSession", func() error {
			_, err := s.GetSession(context.Background(), Viewer{Email: "x@example.com"}, "sess")
			return err
		}},
		{"GetEvents", func() error {
			_, err := s.GetEvents(context.Background(), Viewer{Email: "x@example.com"}, "sess", EventRange{Limit: 5})
			return err
		}},
		{"ResolveShare", func() error {
			_, _, err := s.ResolveShare(context.Background(), Viewer{Email: "x@example.com"}, "tok")
			return err
		}},
		{"CreateShare", func() error {
			_, err := s.CreateShare(context.Background(), Viewer{Email: "x@example.com"},
				ShareRequest{SessionID: "sess"})
			return err
		}},
	} {
		if err := tc.run(); errors.Is(err, ErrNotAdmin) {
			t.Errorf("%s returned ErrNotAdmin, which confirms the session exists", tc.name)
		}
	}
}

func TestEveryTranscriptReadAppliesTheSameAuthorizationPredicate(t *testing.T) {
	db := readableSession("sess", "me@example.com")
	s := NewWithDB(db, nil)
	v := Viewer{Email: "me@example.com", Role: RoleMember}
	ctx := context.Background()

	_, _ = s.GetSession(ctx, v, "sess")
	_, _ = s.GetEvents(ctx, v, "sess", EventRange{Limit: 5})
	_, _ = s.ListSessions(ctx, v, SessionFilter{})
	_, _ = s.SearchMessages(ctx, v, SearchFilter{Query: "deploy"})

	// The share clause is the part most easily left out of one of four copies,
	// so it is the marker the test looks for.
	const marker = "sh.revoked_at IS NULL"
	scoped := 0
	for _, c := range db.calls {
		if strings.Contains(c.sql, marker) {
			scoped++
		}
	}
	if scoped < 4 {
		t.Errorf("only %d statements carry the authorization predicate:\n%s", scoped, db.summary())
	}
}

func TestRecordAccessIgnoresOwnReads(t *testing.T) {
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	if err := s.RecordAccess(context.Background(), Access{
		Viewer: "me@example.com", SessionID: "sess", Owner: "me@example.com", Via: AccessViaOwn,
	}); err != nil {
		t.Fatalf("RecordAccess: %v", err)
	}
	if db.count(sqlAccessLog) != 0 {
		t.Errorf("wrote an audit row for a viewer reading their own session")
	}
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

func TestSearchFiltersThenCapsThenRanksThenHighlights(t *testing.T) {
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	if _, err := s.SearchMessages(context.Background(),
		Viewer{Email: "me@example.com", Role: RoleAdmin},
		SearchFilter{Query: "deploy"}); err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	sql := db.find(t, "ts_rank").sql

	candidates := strings.Index(sql, "candidates AS")
	ranked := strings.Index(sql, "ranked AS")
	rank := strings.Index(sql, "ts_rank(")
	headline := strings.Index(sql, "ts_headline(")
	capIdx := strings.Index(sql, "LIMIT $9")

	if candidates >= capIdx || capIdx >= ranked {
		t.Errorf("the candidate cap is not applied before ranking:\n%s", sql)
	}
	if capIdx >= rank {
		t.Errorf("ts_rank runs before the candidate set is capped, which is the "+
			"thing that blows the latency budget:\n%s", sql)
	}
	if ranked >= headline {
		t.Errorf("ts_headline runs over more than the returned page:\n%s", sql)
	}
	if strings.Count(sql, "ts_headline(") != 1 {
		t.Errorf("ts_headline appears more than once:\n%s", sql)
	}
	if got := db.find(t, "ts_rank").args[8]; got != SearchCandidateCap {
		t.Errorf("candidate cap = %v, want %d", got, SearchCandidateCap)
	}
}

func TestSearchAuditsOncePerForeignSession(t *testing.T) {
	hits := [][]any{
		{"ev1", "sess-a", "colleague@example.com", int64(1), "user", "human", at, "snip", 0.9, int64(2)},
		{"ev2", "sess-a", "colleague@example.com", int64(2), "assistant", "assistant_text", at, "snip", 0.8, int64(2)},
		{"ev3", "sess-b", "me@example.com", int64(1), "user", "human", at, "snip", 0.7, int64(2)},
	}
	db := &fakeDB{stubs: []*stub{{match: "ts_rank", rows: hits}}}
	s := NewWithDB(db, nil)

	if _, err := s.SearchMessages(context.Background(),
		Viewer{Email: "me@example.com", Role: RoleAdmin},
		SearchFilter{Query: "deploy"}); err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	audit := db.find(t, sqlAccessLog)
	sessions := audit.args[1].([]string)
	if len(sessions) != 1 || sessions[0] != "sess-a" {
		t.Errorf("audited sessions = %v, want exactly the one foreign session", sessions)
	}
	if !audit.inTx {
		t.Errorf("search audit was written outside the search transaction")
	}
}

func TestSearchWithNoQueryDoesNotTouchTheDatabase(t *testing.T) {
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	res, err := s.SearchMessages(context.Background(), Viewer{Email: "me@example.com"},
		SearchFilter{Query: "   "})
	if err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	if len(res.Hits) != 0 || len(db.calls) != 0 {
		t.Errorf("an empty query reached the database")
	}
}

func TestSearchReportsWhenTheCandidateSetWasCapped(t *testing.T) {
	row := []any{"ev1", "sess", "me@example.com", int64(1), "user", "human", at, "snip", 0.5, int64(SearchCandidateCap)}
	db := &fakeDB{stubs: []*stub{{match: "ts_rank", rows: [][]any{row}}}}
	s := NewWithDB(db, nil)

	res, err := s.SearchMessages(context.Background(), Viewer{Email: "me@example.com"},
		SearchFilter{Query: "deploy"})
	if err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	if !res.Capped {
		t.Errorf("a saturated candidate set was reported as a complete total")
	}
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

func TestOnlySpeechBecomesSearchableText(t *testing.T) {
	for _, tc := range []struct {
		typ  event.Type
		role string
		ok   bool
	}{
		{event.UserPrompt, "user", true},
		{event.AssistantTurn, "assistant", true},
		{event.ToolResult, "tool", true},
		{event.ToolFailed, "tool", true},
		{event.ToolCall, "", false},
		{event.SessionStarted, "", false},
		{event.FileChanged, "", false},
	} {
		role, ok := messageRole(tc.typ)
		if ok != tc.ok || role != tc.role {
			t.Errorf("messageRole(%s) = (%q,%v), want (%q,%v)", tc.typ, role, ok, tc.role, tc.ok)
		}
	}
}

func TestEmptyTextIsNotIndexed(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)
	in := ingestOf("e1", "sess", "me@example.com", event.UserPrompt, 1)
	in.Event.Text = "   "
	if _, err := s.UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	if db.count(sqlMessages) != 0 {
		t.Errorf("indexed a message with no text in it")
	}
}

// ---------------------------------------------------------------------------
// Rollup folding
// ---------------------------------------------------------------------------

func TestFoldDeltasCountsWhatTheDashboardShows(t *testing.T) {
	items := []Ingest{
		ingestOf("a", "sess", "me@example.com", event.UserPrompt, 1),
		ingestOf("b", "sess", "me@example.com", event.ToolCall, 2),
		ingestOf("c", "sess", "me@example.com", event.ToolCall, 3),
		ingestOf("d", "sess", "me@example.com", event.ToolFailed, 4),
		ingestOf("e", "sess", "me@example.com", event.SubagentStart, 5),
		ingestOf("f", "sess", "me@example.com", event.SessionEnded, 6),
	}
	items[0].Event.HarnessVersion = "2.0.1"
	items[1].Event.HarnessVersion = "2.0.1"
	items[2].Event.HarnessVersion = "2.0.2"
	items[3].Event.Redactions = map[string]int{"api_key": 2}
	items[4].Event.Redactions = map[string]int{"api_key": 1}

	d := foldDeltas(items, nil)["sess"]
	if d.UserTurns != 1 || d.ToolCalls != 2 || d.Errors != 1 || d.Subagents != 1 {
		t.Errorf("counters = %+v", d)
	}
	if !d.Ended {
		t.Errorf("an end marker did not mark the session ended")
	}
	if !d.SawUserPrompt {
		t.Errorf("a prompt in the batch did not flag the first-prompt refresh")
	}
	if len(d.HarnessVersions) != 2 {
		t.Errorf("harness versions = %v, want the two distinct ones", d.HarnessVersions)
	}
	if d.Redactions["api_key"] != 3 {
		t.Errorf("redaction tally = %v, want 3", d.Redactions)
	}
	if !d.StartedAt.Equal(items[0].Event.OccurredAt) || !d.EndedAt.Equal(items[5].Event.OccurredAt) {
		t.Errorf("time extremes = %v..%v", d.StartedAt, d.EndedAt)
	}
}

func TestFoldDeltasSeparatesSessionsInOneBatch(t *testing.T) {
	items := []Ingest{
		ingestOf("a", "sess-1", "me@example.com", event.UserPrompt, 1),
		ingestOf("b", "sess-2", "me@example.com", event.ToolCall, 1),
	}
	d := foldDeltas(items, nil)
	if len(d) != 2 {
		t.Fatalf("got %d deltas, want one per session", len(d))
	}
	if d["sess-1"].UserTurns != 1 || d["sess-2"].ToolCalls != 1 {
		t.Errorf("a batch spanning two sessions was folded together: %+v", d)
	}
}

func TestRollupMergesRatherThanAssigns(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)
	if _, err := s.UpsertEvents(context.Background(), []Ingest{
		ingestOf("e1", "sess", "me@example.com", event.ToolCall, 1),
	}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	sql := db.find(t, sqlRollup).sql
	for _, want := range []string{
		"LEAST(sessions.started_at, EXCLUDED.started_at)",
		"GREATEST(sessions.ended_at, EXCLUDED.ended_at)",
		"sessions.ended OR EXCLUDED.ended",
		"sessions.tool_calls + EXCLUDED.tool_calls",
		"sessions.cost_usd + EXCLUDED.cost_usd",
		"session_array_union(sessions.harness_versions, EXCLUDED.harness_versions)",
		"jsonb_counter_add(sessions.redactions, EXCLUDED.redactions)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("rollup does not merge %q, so batch order changes the result:\n%s", want, sql)
		}
	}
	// The owner is fixed at creation. Updating it would let a second device
	// take over a session by sending one event.
	if strings.Contains(sql, "email              = EXCLUDED.email") {
		t.Errorf("the rollup reassigns session ownership:\n%s", sql)
	}
}

func TestUpsertSessionRollupRefusesAnotherPrincipalsSession(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: sqlClaimSession, rows: [][]any{{"victim@example.com"}}},
	}}
	s := NewWithDB(db, nil)
	err := s.UpsertSessionRollup(context.Background(), SessionDelta{
		SessionID: "victims-session", Email: "attacker@example.com",
		Source: "claude_code", StartedAt: at,
	})
	if !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("error = %v, want ErrOwnerMismatch", err)
	}
	if db.count(sqlRollup) != 0 {
		t.Errorf("applied an increment to another principal's session")
	}
	if db.committed != 0 {
		t.Errorf("committed a refused rollup")
	}
}

// ---------------------------------------------------------------------------
// Principals
// ---------------------------------------------------------------------------

func principalRow(email string, role Role, disabled *time.Time) []any {
	return []any{email, string(role), nil, nil, at, disabled}
}

func TestPutPrincipalRefusesToRemoveTheLastAdmin(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "SELECT count(*) FROM locked", rows: [][]any{{0}}},
		{match: "FROM principals WHERE email", rows: [][]any{principalRow("last@example.com", RoleAdmin, nil)}},
	}}
	s := NewWithDB(db, nil)

	member := RoleMember
	_, err := s.PutPrincipal(context.Background(),
		Viewer{Email: "last@example.com", Role: RoleAdmin},
		PrincipalUpdate{Email: "last@example.com", Role: &member})
	if !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("error = %v, want ErrLastAdmin", err)
	}
	if db.count("INSERT INTO principals") != 0 {
		t.Errorf("wrote the demotion anyway")
	}
	if db.committed != 0 {
		t.Errorf("committed a refused demotion")
	}
}

func TestPutPrincipalRefusesToDisableTheLastAdmin(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "SELECT count(*) FROM locked", rows: [][]any{{0}}},
		{match: "FROM principals WHERE email", rows: [][]any{principalRow("last@example.com", RoleAdmin, nil)}},
	}}
	s := NewWithDB(db, nil)

	disabled := true
	_, err := s.PutPrincipal(context.Background(),
		Viewer{Email: "other@example.com", Role: RoleAdmin},
		PrincipalUpdate{Email: "last@example.com", Disabled: &disabled})
	if !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("error = %v, want ErrLastAdmin", err)
	}
}

func TestPutPrincipalAllowsDemotionWhileAnotherAdminRemains(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "SELECT count(*) FROM locked", rows: [][]any{{1}}},
		{match: "FROM principals WHERE email", rows: [][]any{principalRow("them@example.com", RoleAdmin, nil)}, once: true},
		{match: "INSERT INTO principals", rows: [][]any{principalRow("them@example.com", RoleMember, nil)}},
	}}
	s := NewWithDB(db, nil)

	member := RoleMember
	p, err := s.PutPrincipal(context.Background(),
		Viewer{Email: "me@example.com", Role: RoleAdmin},
		PrincipalUpdate{Email: "them@example.com", Role: &member})
	if err != nil {
		t.Fatalf("PutPrincipal: %v", err)
	}
	if p.Role != RoleMember {
		t.Errorf("role = %q, want member", p.Role)
	}
	if db.committed != 1 {
		t.Errorf("committed %d times, want 1", db.committed)
	}
}

func TestPutPrincipalTakesRowLocksBeforeCountingAdmins(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "SELECT count(*) FROM locked", rows: [][]any{{1}}},
		{match: "FROM principals WHERE email", rows: [][]any{principalRow("them@example.com", RoleMember, nil)}, once: true},
		{match: "INSERT INTO principals", rows: [][]any{principalRow("them@example.com", RoleAdmin, nil)}},
	}}
	s := NewWithDB(db, nil)
	admin := RoleAdmin
	if _, err := s.PutPrincipal(context.Background(),
		Viewer{Email: "me@example.com", Role: RoleAdmin},
		PrincipalUpdate{Email: "them@example.com", Role: &admin}); err != nil {
		t.Fatalf("PutPrincipal: %v", err)
	}
	// Without FOR UPDATE two admins demoting each other at the same instant each
	// see the other and both succeed.
	if sql := db.find(t, "SELECT count(*) FROM locked").sql; !strings.Contains(sql, "FOR UPDATE") {
		t.Errorf("the admin count is not serialised:\n%s", sql)
	}
}

// The browser's admin form reaches PutPrincipal, and the JSON API reaches
// SavePrincipal plus RecordPrincipalChange on an AdminTx. The two are the same
// operation through two doors, so the obligation to leave a trail cannot depend
// on which door was used: an operator who grants somebody standing visibility
// over colleagues' transcripts from the page they are given must be as
// accountable as one who does it with curl.
func TestPutPrincipalRecordsAnAuditRowForEveryRosterEdit(t *testing.T) {
	admin, member := RoleAdmin, RoleMember
	yes, no := true, false

	for _, tc := range []struct {
		name    string
		current []any // nil means the person is not on the roster yet
		update  PrincipalUpdate
		want    []any // actor, target, from_role, to_role, from_disabled, to_disabled
	}{
		{
			name:    "promotion",
			current: principalRow("them@example.com", RoleMember, nil),
			update:  PrincipalUpdate{Email: "them@example.com", Role: &admin},
			want:    []any{"me@example.com", "them@example.com", "member", "admin", false, false},
		},
		{
			name:    "demotion",
			current: principalRow("them@example.com", RoleAdmin, nil),
			update:  PrincipalUpdate{Email: "them@example.com", Role: &member},
			want:    []any{"me@example.com", "them@example.com", "admin", "member", false, false},
		},
		{
			name:    "disable",
			current: principalRow("them@example.com", RoleMember, nil),
			update:  PrincipalUpdate{Email: "them@example.com", Disabled: &yes},
			want:    []any{"me@example.com", "them@example.com", "member", "member", false, true},
		},
		{
			name:    "re-enable",
			current: principalRow("them@example.com", RoleMember, &at),
			update:  PrincipalUpdate{Email: "them@example.com", Disabled: &no},
			want:    []any{"me@example.com", "them@example.com", "member", "member", true, false},
		},
		{
			// A creation stores an empty from_role, which the statement turns into
			// NULL. That absence is the only thing telling a creation from a
			// promotion once the change is a year old.
			name:   "creation",
			update: PrincipalUpdate{Email: "new@example.com", Role: &member},
			want:   []any{"me@example.com", "new@example.com", "", "member", false, false},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubs := []*stub{{match: "SELECT count(*) FROM locked", rows: [][]any{{1}}}}
			if tc.current != nil {
				stubs = append(stubs, &stub{
					match: "FROM principals WHERE email",
					rows:  [][]any{tc.current},
					once:  true,
				})
			}
			stubs = append(stubs, &stub{
				match: "INSERT INTO principals",
				rows:  [][]any{principalRow(tc.update.Email, RoleMember, nil)},
			})
			db := &fakeDB{stubs: stubs}
			s := NewWithDB(db, nil)

			if _, err := s.PutPrincipal(context.Background(),
				Viewer{Email: "me@example.com", Role: RoleAdmin}, tc.update); err != nil {
				t.Fatalf("PutPrincipal: %v", err)
			}

			c := db.find(t, "INSERT INTO principal_changes")
			if !c.inTx {
				t.Errorf("the audit row was written outside the transaction that made the change")
			}
			if len(c.args) < len(tc.want) {
				t.Fatalf("audit row has %d arguments, want at least %d", len(c.args), len(tc.want))
			}
			for i, want := range tc.want {
				if c.args[i] != want {
					t.Errorf("audit argument %d = %v, want %v", i+1, c.args[i], want)
				}
			}
			if db.committed != 1 {
				t.Errorf("committed %d times, want 1", db.committed)
			}
		})
	}
}

// An audit row that can fail on its own is not an audit trail, so its failure
// has to take the change it describes down with it.
func TestPutPrincipalRollsBackWhenTheAuditRowFails(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "SELECT count(*) FROM locked", rows: [][]any{{1}}},
		{match: "FROM principals WHERE email", rows: [][]any{principalRow("them@example.com", RoleMember, nil)}, once: true},
		{match: "INSERT INTO principals", rows: [][]any{principalRow("them@example.com", RoleAdmin, nil)}},
		{match: "INSERT INTO principal_changes", err: errors.New("audit table is full")},
	}}
	s := NewWithDB(db, nil)

	admin := RoleAdmin
	_, err := s.PutPrincipal(context.Background(),
		Viewer{Email: "me@example.com", Role: RoleAdmin},
		PrincipalUpdate{Email: "them@example.com", Role: &admin})
	if err == nil {
		t.Fatalf("PutPrincipal = nil, want the audit failure")
	}
	if !strings.Contains(err.Error(), "audit table is full") {
		t.Errorf("error = %v, want the audit failure to survive wrapping", err)
	}
	if db.committed != 0 {
		t.Errorf("committed an unaudited promotion")
	}
}

// A form submitted with the values already on the row is not an edit. Writing
// the roster row anyway leaves a tuple whose only evidence of the request is
// that it was rewritten, and reporting it as saved tells an operator their
// change landed when there was no change: the two together are what made a
// no-op indistinguishable from a lost write.
func TestPutPrincipalWritesNothingWhenTheSubmittedValuesMatchTheRow(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "SELECT count(*) FROM locked", rows: [][]any{{1}}},
		{match: "FROM principals WHERE email", rows: [][]any{principalRow("them@example.com", RoleAdmin, nil)}, once: true},
		{match: "INSERT INTO principals", rows: [][]any{principalRow("them@example.com", RoleAdmin, nil)}},
	}}
	s := NewWithDB(db, nil)

	admin := RoleAdmin
	disabled := false
	saved, err := s.PutPrincipal(context.Background(),
		Viewer{Email: "me@example.com", Role: RoleAdmin},
		PrincipalUpdate{Email: "them@example.com", Role: &admin, Disabled: &disabled})
	if err != nil {
		t.Fatalf("PutPrincipal: %v", err)
	}
	if db.count("INSERT INTO principals") != 0 {
		t.Errorf("rewrote a roster row that nothing had changed")
	}
	if db.count("INSERT INTO principal_changes") != 0 {
		t.Errorf("recorded a change that did not happen")
	}
	if saved.Email != "them@example.com" || saved.Role != RoleAdmin {
		t.Errorf("returned %+v, want the row as it already stood", saved)
	}
}

func TestAdminSurfacesRefuseMembers(t *testing.T) {
	s := NewWithDB(&fakeDB{}, nil)
	member := Viewer{Email: "member@example.com", Role: RoleMember}
	ctx := context.Background()

	if _, err := s.ListPrincipals(ctx, member); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("ListPrincipals = %v, want ErrNotAdmin", err)
	}
	if _, err := s.PutPrincipal(ctx, member, PrincipalUpdate{Email: "x@example.com"}); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("PutPrincipal = %v, want ErrNotAdmin", err)
	}
	if _, err := s.FleetCoverage(ctx, member, time.Hour); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("FleetCoverage = %v, want ErrNotAdmin", err)
	}
	if _, err := s.ListAccessLog(ctx, member, AccessLogFilter{}); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("ListAccessLog = %v, want ErrNotAdmin", err)
	}
}

// ---------------------------------------------------------------------------
// Devices
// ---------------------------------------------------------------------------

func TestAuthenticateDeviceRequiresTokenDeviceAndPrincipalToAllBeLive(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "device_tokens", rows: [][]any{{"11111111-1111-4111-8111-111111111111", "me@example.com", "member"}}},
	}}
	s := NewWithDB(db, nil)
	id, err := s.AuthenticateDevice(context.Background(), []byte("hash"))
	if err != nil {
		t.Fatalf("AuthenticateDevice: %v", err)
	}
	if id.Email != "me@example.com" || id.Role != RoleMember {
		t.Errorf("identity = %+v", id)
	}
	sql := db.find(t, "device_tokens").sql
	for _, want := range []string{
		"t.revoked_at IS NULL",
		"d.revoked_at IS NULL",
		"p.disabled_at IS NULL",
		"t.expires_at IS NULL OR t.expires_at > now()",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("authentication does not check %q:\n%s", want, sql)
		}
	}
}

func TestAuthenticateDeviceRejectsAnUnknownToken(t *testing.T) {
	s := NewWithDB(&fakeDB{}, nil)
	if _, err := s.AuthenticateDevice(context.Background(), []byte("nope")); !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestEnrollDeviceStoresOnlyTheTokenHash(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "INSERT INTO devices", rows: [][]any{{at}}},
	}}
	s := NewWithDB(db, nil)
	if _, err := s.EnrollDevice(context.Background(),
		Device{Email: "me@example.com", Hostname: "laptop"},
		[]byte("sha256-digest"), time.Time{}); err != nil {
		t.Fatalf("EnrollDevice: %v", err)
	}
	tok := db.find(t, "INSERT INTO device_tokens")
	if _, ok := tok.args[2].([]byte); !ok {
		t.Errorf("stored a token as %T rather than as a digest", tok.args[2])
	}
}

// enrolledDeviceID is the id the upsert was asked to write, which is the whole
// question this group of tests asks: a fresh one means a second row for a laptop
// that already had one.
func enrolledDeviceID(t *testing.T, db *fakeDB) string {
	t.Helper()
	id, ok := db.find(t, "INSERT INTO devices").args[0].(string)
	if !ok {
		t.Fatalf("the device id reached the statement as %T", db.find(t, "INSERT INTO devices").args[0])
	}
	return id
}

// Property: a laptop that names a row it still owns is written back into that
// row. Minting a fresh id here is the defect: the row nobody writes to again is
// counted by admin/fleet.go forever as a machine that enrolled and went silent.
func TestEnrollDeviceReusesTheRowTheLaptopNames(t *testing.T) {
	const prior = "2f3a1b90-4c5d-4e6f-8a9b-0c1d2e3f4a5b"
	db := &fakeDB{stubs: []*stub{
		{match: "FOR UPDATE", rows: [][]any{{prior}}},
		{match: "INSERT INTO devices", rows: [][]any{{at}}},
	}}
	s := NewWithDB(db, nil)
	if _, err := s.EnrollDevice(context.Background(),
		Device{ID: prior, Email: "me@example.com", Hostname: "laptop"},
		[]byte("sha256-digest"), time.Time{}); err != nil {
		t.Fatalf("EnrollDevice: %v", err)
	}
	if got := enrolledDeviceID(t, db); got != prior {
		t.Errorf("enrolled %s, want the row the laptop named (%s)", got, prior)
	}
	// The weaker signal is not consulted at all when the stronger one answered.
	// A person's second machine can carry the same hostname, and retiring it on
	// a guess when the laptop has already identified itself exactly would stop a
	// working machine delivering.
	if n := db.count("UPDATE devices SET revoked_at"); n != 0 {
		t.Errorf("superseded %d device(s) on hostname despite the laptop naming its own row", n)
	}
}

// Property: the claim is resolved against ownership and liveness in the
// database, and the resolution is locked, because between reading a row and
// writing it a revocation can commit — and a revocation that an enrolment
// silently undoes is a stolen laptop back on the fleet.
func TestEnrollDeviceResolvesAClaimAgainstOwnershipAndRevocation(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "INSERT INTO devices", rows: [][]any{{at}}}}}
	s := NewWithDB(db, nil)
	if _, err := s.EnrollDevice(context.Background(),
		Device{ID: "2f3a1b90-4c5d-4e6f-8a9b-0c1d2e3f4a5b", Email: "me@example.com"},
		[]byte("sha256-digest"), time.Time{}); err != nil {
		t.Fatalf("EnrollDevice: %v", err)
	}
	resolve := db.find(t, "FOR UPDATE").sql
	for _, want := range []string{"email = $2", "revoked_at IS NULL", "FOR UPDATE"} {
		if !strings.Contains(resolve, want) {
			t.Errorf("resolving a claimed device does not check %q:\n%s", want, resolve)
		}
	}
	upsert := db.find(t, "INSERT INTO devices").sql
	if strings.Contains(upsert, "revoked_at    = NULL") {
		t.Errorf("re-enrolling clears revoked_at, so revoking a laptop is undone by re-running the installer:\n%s", upsert)
	}
	for _, want := range []string{"devices.email = EXCLUDED.email", "devices.revoked_at IS NULL"} {
		if !strings.Contains(upsert, want) {
			t.Errorf("the conflict branch does not guard %q:\n%s", want, upsert)
		}
	}
}

// Property: a claim that could not possibly name a row never reaches the ::uuid
// cast, where it would be a statement error and would fail an enrolment that
// should simply have been given a new id.
func TestEnrollDeviceIgnoresAClaimThatIsNotAnID(t *testing.T) {
	for name, claim := range map[string]string{
		"free text":                    "not-a-uuid",
		"a quoted string":              `' OR true--`,
		"the right length but not hex": "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",
	} {
		t.Run(name, func(t *testing.T) {
			db := &fakeDB{stubs: []*stub{{match: "INSERT INTO devices", rows: [][]any{{at}}}}}
			s := NewWithDB(db, nil)
			dev, err := s.EnrollDevice(context.Background(),
				Device{ID: claim, Email: "me@example.com"}, []byte("sha256-digest"), time.Time{})
			if err != nil {
				t.Fatalf("EnrollDevice: %v", err)
			}
			if n := db.count("FOR UPDATE"); n != 0 {
				t.Errorf("a claim of %q was taken to the database", claim)
			}
			if dev.ID == claim || !isUUID(dev.ID) {
				t.Errorf("device id = %q, want a fresh one", dev.ID)
			}
		})
	}
}

// Property: when the laptop cannot name its row, the same person's device with
// the same hostname is retired in the same transaction as the new one is
// written. This is the purged-config case, and it is the one that actually
// produces the phantom rows.
func TestEnrollDeviceSupersedesThePriorDeviceOnThatHostname(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "INSERT INTO devices", rows: [][]any{{at}}}}}
	s := NewWithDB(db, nil)
	if _, err := s.EnrollDevice(context.Background(),
		Device{Email: "me@example.com", Hostname: "laptop"},
		[]byte("sha256-digest"), time.Time{}); err != nil {
		t.Fatalf("EnrollDevice: %v", err)
	}
	sup := db.find(t, "UPDATE devices SET revoked_at")
	if sup.args[0] != "me@example.com" || sup.args[1] != "laptop" {
		t.Errorf("superseded on %v, want this person and this hostname only", sup.args)
	}
	if !strings.Contains(sup.sql, "email = $1") {
		t.Errorf("supersession is not scoped to one person:\n%s", sup.sql)
	}
	// The credential goes with the row. A live token against a revoked device
	// authenticates nothing today, and leaving it behind would make that a
	// property of one join rather than of the token table.
	if !strings.Contains(sup.sql, "UPDATE device_tokens") {
		t.Errorf("the superseded device keeps a live credential:\n%s", sup.sql)
	}
	if db.begun != 1 || db.committed != 1 {
		t.Errorf("began %d and committed %d transactions; the supersession and the new row are one write",
			db.begun, db.committed)
	}
}

// Property: a machine with no hostname supersedes nothing. Hostname is the only
// evidence this path has, and an empty one would match every row of that
// person's whose hostname was never recorded.
func TestEnrollDeviceSupersedesNothingWithoutAHostname(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "INSERT INTO devices", rows: [][]any{{at}}}}}
	s := NewWithDB(db, nil)
	if _, err := s.EnrollDevice(context.Background(),
		Device{Email: "me@example.com"}, []byte("sha256-digest"), time.Time{}); err != nil {
		t.Fatalf("EnrollDevice: %v", err)
	}
	if n := db.count("UPDATE devices SET revoked_at"); n != 0 {
		t.Errorf("an enrolment with no hostname revoked %d device(s)", n)
	}
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func healthSample() health.Report {
	return health.Report{
		SchemaVersion: health.SchemaVersion,
		Hostname:      "laptop",
		EmittedAt:     at,
		Conditions: []health.Condition{
			{Level: health.LevelCritical, Kind: health.KindCaptureBlocked, Detail: "disk full"},
			{Level: health.LevelInfo, Kind: health.KindPaused},
		},
	}
}

func TestHealthReportsAreIdempotentOnRetry(t *testing.T) {
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	if err := s.PutHealthReport(context.Background(), "me@example.com",
		"11111111-1111-4111-8111-111111111111", healthSample()); err != nil {
		t.Fatalf("PutHealthReport: %v", err)
	}
	sql := db.find(t, "INSERT INTO health_reports").sql
	if !strings.Contains(sql, "ON CONFLICT (email, device_id, emitted_at) DO NOTHING") {
		t.Errorf("a re-delivered report would look like a second check-in:\n%s", sql)
	}
	touch := db.find(t, "last_seen_at = GREATEST").sql
	if !strings.Contains(touch, "GREATEST") {
		t.Errorf("a late report could pull a device's liveness backwards")
	}
	// The version refresh rides the same statement, and its guard must compare
	// report time against the recorded liveness so a replayed old sample cannot
	// roll a self-upgraded machine's version backwards.
	if !strings.Contains(touch, "agent_version = CASE") ||
		!strings.Contains(touch, ">= coalesce(last_seen_at") {
		t.Errorf("the device version is not refreshed monotonically:\n%s", touch)
	}
}

func TestHealthReportStoresTheDerivedWorstLevel(t *testing.T) {
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	if err := s.PutHealthReport(context.Background(), "me@example.com", "", healthSample()); err != nil {
		t.Fatalf("PutHealthReport: %v", err)
	}
	c := db.find(t, "INSERT INTO health_reports")
	if got := c.args[3]; got != "critical" {
		t.Errorf("worst = %v, want critical", got)
	}
	var round map[string]any
	if err := json.Unmarshal([]byte(c.args[4].(string)), &round); err != nil {
		t.Fatalf("stored report is not JSON: %v", err)
	}
	if round["hostname"] != "laptop" {
		t.Errorf("stored report lost its identity: %v", round)
	}
}

func TestHealthReportNeedsAnOwner(t *testing.T) {
	s := NewWithDB(&fakeDB{}, nil)
	if err := s.PutHealthReport(context.Background(), "", "", healthSample()); err == nil {
		t.Error("stored an unattributable health report")
	}
}

func TestFleetCoverageSeparatesEnrolledFromReporting(t *testing.T) {
	reported := at
	db := &fakeDB{stubs: []*stub{{match: "FROM principals p", rows: [][]any{
		{"reporting@example.com", "member", 1, reported, "info"},
		{"silent@example.com", "member", 2, nil, "degraded"},
		{"never-enrolled@example.com", "member", 0, nil, nil},
	}}}}
	s := NewWithDB(db, nil)

	fleet, err := s.FleetCoverage(context.Background(),
		Viewer{Email: "admin@example.com", Role: RoleAdmin}, 24*time.Hour)
	if err != nil {
		t.Fatalf("FleetCoverage: %v", err)
	}
	if fleet.Enrolled != 2 || fleet.Reporting != 1 || fleet.Silent != 1 {
		t.Errorf("coverage = %+v, want 2 enrolled / 1 reporting / 1 silent", fleet)
	}
	if !fleet.Members[1].Silent {
		t.Errorf("an enrolled machine with no recent report was not called silent")
	}
	if fleet.Members[2].Silent {
		t.Errorf("someone who never enrolled was counted as a silent machine")
	}
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

func TestCursorRoundTrips(t *testing.T) {
	want := cursor{StartedAt: at, SessionID: "sess-1"}
	got, err := decodeCursor(encodeCursor(want))
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !got.StartedAt.Equal(want.StartedAt) || got.SessionID != want.SessionID {
		t.Errorf("cursor = %+v, want %+v", got, want)
	}
}

func TestCursorRejectsAnythingWeDidNotIssue(t *testing.T) {
	for _, bad := range []string{"not-base64!!", "e30", "eyJ0IjoiIn0"} {
		if _, err := decodeCursor(bad); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("decodeCursor(%q) = %v, want ErrInvalidCursor", bad, err)
		}
	}
	if c, err := decodeCursor(""); err != nil || c.SessionID != "" {
		t.Errorf("an absent cursor should mean the first page, got %+v %v", c, err)
	}
}

func TestListSessionsPagesByKeysetAndReportsTheNextCursor(t *testing.T) {
	rows := make([][]any, 0, 3)
	for i := range 3 {
		rows = append(rows, sessionRow(fmt.Sprintf("sess-%d", i), "me@example.com"))
	}
	db := &fakeDB{stubs: []*stub{{match: "FROM sessions s", rows: rows}}}
	s := NewWithDB(db, nil)

	page, err := s.ListSessions(context.Background(),
		Viewer{Email: "me@example.com"}, SessionFilter{Limit: 2})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(page.Sessions) != 2 {
		t.Fatalf("returned %d sessions, want the requested 2", len(page.Sessions))
	}
	if page.NextCursor == "" {
		t.Fatal("no next cursor despite an extra row being available")
	}
	c, err := decodeCursor(page.NextCursor)
	if err != nil {
		t.Fatalf("issued a cursor we cannot decode: %v", err)
	}
	if c.SessionID != "sess-1" {
		t.Errorf("cursor points at %q, want the last returned row", c.SessionID)
	}
	sql := db.find(t, "FROM sessions s").sql
	if strings.Contains(sql, "OFFSET") {
		t.Errorf("listing pages by offset, which skips rows in a table still "+
			"being written to:\n%s", sql)
	}
}

func TestListSessionsIsNotAudited(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "FROM sessions s",
		rows: [][]any{sessionRow("sess", "colleague@example.com")}}}}
	s := NewWithDB(db, nil)
	if _, err := s.ListSessions(context.Background(),
		Viewer{Email: "admin@example.com", Role: RoleAdmin}, SessionFilter{}); err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if db.count(sqlAccessLog) != 0 {
		t.Errorf("browsing a list wrote audit rows, which buries the reads that matter")
	}
}

func TestReadingFromTheBeginningDoesNotSkipSequenceZero(t *testing.T) {
	db := readableSession("sess", "me@example.com")
	s := NewWithDB(db, nil)
	if _, err := s.GetEvents(context.Background(),
		Viewer{Email: "me@example.com"}, "sess", EventRange{}); err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	c := db.find(t, "FROM events")
	// The backfill walker numbers a session's events from zero, so a bound of
	// zero would hide the opening event of every imported session and nothing
	// about the page would look wrong.
	if c.args[1] != (*int64)(nil) {
		t.Errorf("first page bound = %#v, want no bound at all", c.args[1])
	}
	if !strings.Contains(c.sql, "$2::bigint IS NULL OR seq > $2") {
		t.Errorf("the sequence bound is not optional:\n%s", c.sql)
	}
}

func TestReadingAfterASequenceIsExclusive(t *testing.T) {
	db := readableSession("sess", "me@example.com")
	s := NewWithDB(db, nil)
	after := int64(0)
	if _, err := s.GetEvents(context.Background(),
		Viewer{Email: "me@example.com"}, "sess", EventRange{AfterSeq: &after}); err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	got, ok := db.find(t, "FROM events").args[1].(*int64)
	if !ok || got == nil || *got != 0 {
		t.Errorf("bound = %#v, want an explicit zero", db.find(t, "FROM events").args[1])
	}
}

func TestAMalformedBodyIsRejectedWithoutPoisoningTheBatch(t *testing.T) {
	db := ingestDB("me@example.com", "e2")
	s := NewWithDB(db, nil)

	bad := ingestOf("e1", "sess", "me@example.com", event.ToolCall, 1)
	bad.Body = json.RawMessage(`{"unterminated":`)
	good := ingestOf("e2", "sess", "me@example.com", event.ToolCall, 2)

	res, err := s.UpsertEvents(context.Background(), []Ingest{bad, good})
	if err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	// One statement carries the whole batch, so a body Postgres cannot parse
	// would otherwise reject every event delivered alongside it.
	if len(res.Rejected) != 1 || res.Rejected[0].ID != "e1" {
		t.Errorf("Rejected = %+v, want only the malformed event", res.Rejected)
	}
	if !reflect.DeepEqual(res.Inserted, []string{"e2"}) {
		t.Errorf("Inserted = %v, want the well-formed event stored", res.Inserted)
	}
}

// An id or session id over the key cap is a permanent rejection with a
// stable reason: both are btree entries (events_pkey, sessions_pkey), and
// one over the entry ceiling would otherwise fail the whole batch on every
// re-send. The reasons are what the client records, so they are pinned.
func TestAnOversizedIDIsRejectedWithoutPoisoningTheBatch(t *testing.T) {
	db := ingestDB("me@example.com", "e2")
	s := NewWithDB(db, nil)

	long := strings.Repeat("x", derive.KeyMaxBytes+1)
	badID := ingestOf(long, "sess", "me@example.com", event.ToolCall, 1)
	badSession := ingestOf("e1", long, "me@example.com", event.ToolCall, 1)
	good := ingestOf("e2", "sess", "me@example.com", event.ToolCall, 2)
	atCap := ingestOf("e3", strings.Repeat("s", derive.KeyMaxBytes), "me@example.com", event.ToolCall, 3)
	atCap.Event.ID = strings.Repeat("e", derive.KeyMaxBytes)

	res, err := s.UpsertEvents(context.Background(), []Ingest{badID, badSession, good, atCap})
	if err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	want := []Rejected{{ID: long, Reason: "event id too long"}, {ID: "e1", Reason: "session id too long"}}
	if !reflect.DeepEqual(res.Rejected, want) {
		t.Errorf("Rejected = %+v, want the two oversized ids with their reasons", res.Rejected)
	}
	if !reflect.DeepEqual(res.Inserted, []string{"e2"}) {
		t.Errorf("Inserted = %v, want the well-formed event stored", res.Inserted)
	}
	if !reflect.DeepEqual(res.Duplicate, []string{atCap.Event.ID}) {
		t.Errorf("Duplicate = %v, want the id exactly at the cap admitted (the stub reports it stored already)", res.Duplicate)
	}
}

func TestAFailedIngestReportsNothingAsStored(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: sqlClaimSession, rows: [][]any{{"me@example.com"}}},
		{match: sqlEventInsert, rows: [][]any{{"e1", true}}},
		{match: sqlMessages, err: errors.New("connection reset")},
	}}
	s := NewWithDB(db, nil)

	in := ingestOf("e1", "sess", "me@example.com", event.UserPrompt, 1)
	in.Event.Text = "hello"
	res, err := s.UpsertEvents(context.Background(), []Ingest{in})
	if err == nil {
		t.Fatal("UpsertEvents hid a failed write")
	}
	// The transaction rolled back, so telling the client anything was accepted
	// would have it delete spool items that were never stored.
	if len(res.Accepted()) != 0 || len(res.Inserted) != 0 {
		t.Errorf("failed ingest reported %+v as accepted", res)
	}
	if db.committed != 0 {
		t.Errorf("committed a failed ingest")
	}
}

func TestRollupWithoutAStartTimeIsRefused(t *testing.T) {
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	err := s.UpsertSessionRollup(context.Background(), SessionDelta{
		SessionID: "sess", Email: "me@example.com", Source: "claude_code",
	})
	if err == nil {
		t.Fatal("accepted a rollup that would date the session to year one")
	}
	if db.begun != 0 {
		t.Errorf("opened a transaction for a rollup it could not apply")
	}
}

func TestLimitsAreClamped(t *testing.T) {
	if got := clampLimit(0); got != defaultLimit {
		t.Errorf("clampLimit(0) = %d, want %d", got, defaultLimit)
	}
	if got := clampLimit(100000); got != maxLimit {
		t.Errorf("clampLimit(100000) = %d, want %d", got, maxLimit)
	}
	if got := clampLimit(7); got != 7 {
		t.Errorf("clampLimit(7) = %d, want 7", got)
	}
}

// ---------------------------------------------------------------------------
// Migration
// ---------------------------------------------------------------------------

func migrationSQL(t *testing.T) string {
	t.Helper()
	b, err := migrations.ReadFile("migrations/0001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	return string(b)
}

// testDomainOf is the domain of a fixture address, so a test's allowlist is
// derived from the fixture it must admit rather than spelled twice.
func testDomainOf(email string) string { return email[strings.LastIndex(email, "@")+1:] }

// emailShape matches an email address in SQL text; the array operator <@
// in 0022 is not one.
var emailShape = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+`)

// TestNoMigrationSeedsAPerson pins the roster's starting point: the schema
// creates an empty principals table and the first admins come from
// ADMIN_EMAILS at boot (BootstrapAdmins), so no migration in this repository
// may write an address. A seed in a migration would name one organisation's
// people in every deployment's database.
func TestNoMigrationSeedsAPerson(t *testing.T) {
	names, err := migrationNames()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		b, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		sql := string(b)
		if strings.Contains(sql, "INSERT INTO principals") {
			t.Errorf("%s writes to principals; the roster is bootstrapped from ADMIN_EMAILS, not seeded", name)
		}
		if m := emailShape.FindString(sql); m != "" {
			t.Errorf("%s carries an email address: %s", name, m)
		}
	}
	if !strings.Contains(migrationSQL(t), "CREATE TABLE IF NOT EXISTS principals") {
		t.Error("0001 no longer creates principals")
	}
}

// TestBootstrapAdminsOnlyEverAdds pins the statement shape ADMIN_EMAILS is
// applied with: an insert of an active admin row under ON CONFLICT DO
// NOTHING with no UPDATE arm, one change-trail row per row actually
// written, nothing at all for an empty list, and the existing row read back
// untouched when the insert was a no-op.
func TestBootstrapAdminsOnlyEverAdds(t *testing.T) {
	ctx := context.Background()
	t.Run("an empty list issues no statement", func(t *testing.T) {
		db := &fakeDB{}
		out, err := NewWithDB(db, nil).BootstrapAdmins(ctx, nil)
		if err != nil || len(out) != 0 || len(db.calls) != 0 || db.begun != 0 {
			t.Fatalf("out=%v err=%v calls=%d begun=%d", out, err, len(db.calls), db.begun)
		}
	})
	t.Run("a new address becomes an active admin with a trail row", func(t *testing.T) {
		db := &fakeDB{stubs: []*stub{{match: "INSERT INTO principals", rows: [][]any{{true}}}}}
		out, err := NewWithDB(db, nil).BootstrapAdmins(ctx, []string{" Root@Example.COM "})
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 1 || !out[0].Created || out[0].Email != "root@example.com" {
			t.Errorf("outcome = %+v", out)
		}
		var ins, trail *call
		for i := range db.calls {
			switch {
			case strings.Contains(db.calls[i].sql, "INSERT INTO principals"):
				ins = &db.calls[i]
			case strings.Contains(db.calls[i].sql, "INSERT INTO principal_changes"):
				trail = &db.calls[i]
			}
		}
		if ins == nil {
			t.Fatal("no insert into principals")
		}
		for _, want := range []string{"'admin'", "ON CONFLICT (email) DO NOTHING"} {
			if !strings.Contains(ins.sql, want) {
				t.Errorf("insert lacks %q:\n%s", want, ins.sql)
			}
		}
		if strings.Contains(ins.sql, "DO UPDATE") || strings.Contains(ins.sql, "disabled_at") {
			t.Errorf("the bootstrap can change an existing row:\n%s", ins.sql)
		}
		if len(ins.args) != 2 || ins.args[0] != "root@example.com" || ins.args[1] != bootstrapAddedBy {
			t.Errorf("insert args = %v", ins.args)
		}
		if trail == nil {
			t.Fatal("no principal_changes row for the creation")
		}
		if !strings.Contains(trail.sql, "NULL, 'admin', false, false") || len(trail.args) != 1 || trail.args[0] != "root@example.com" {
			t.Errorf("trail = %s %v", trail.sql, trail.args)
		}
		if db.committed != 1 || db.rolled != 0 {
			t.Errorf("committed %d, rolled back %d", db.committed, db.rolled)
		}
	})
	t.Run("an existing row is read back and left as it is", func(t *testing.T) {
		disabled := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
		db := &fakeDB{stubs: []*stub{
			{match: "INSERT INTO principals", rows: [][]any{{false}}},
			{match: "FROM principals WHERE email", rows: [][]any{{"root@example.com", "member", "", "", time.Time{}, &disabled}}},
		}}
		out, err := NewWithDB(db, nil).BootstrapAdmins(ctx, []string{"root@example.com"})
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 1 || out[0].Created || out[0].Role != RoleMember || !out[0].Disabled {
			t.Errorf("outcome = %+v", out)
		}
		for _, c := range db.calls {
			if strings.Contains(c.sql, "principal_changes") || strings.Contains(c.sql, "UPDATE") {
				t.Errorf("a no-op bootstrap wrote: %s", c.sql)
			}
		}
	})
	t.Run("a value that is not an address is refused", func(t *testing.T) {
		db := &fakeDB{}
		if _, err := NewWithDB(db, nil).BootstrapAdmins(ctx, []string{"not-an-address"}); err == nil || len(db.calls) != 0 {
			t.Errorf("err = %v, calls = %d", err, len(db.calls))
		}
	})
}

func TestMigrationIsSafeToRerun(t *testing.T) {
	sql := migrationSQL(t)
	creates := strings.Count(sql, "CREATE TABLE")
	guarded := strings.Count(sql, "CREATE TABLE IF NOT EXISTS")
	if creates != guarded {
		t.Errorf("%d of %d CREATE TABLE statements are unguarded", creates-guarded, creates)
	}
	indexes := strings.Count(sql, "CREATE INDEX") + strings.Count(sql, "CREATE UNIQUE INDEX")
	guardedIdx := strings.Count(sql, "CREATE INDEX IF NOT EXISTS") +
		strings.Count(sql, "CREATE UNIQUE INDEX IF NOT EXISTS")
	if indexes != guardedIdx {
		t.Errorf("%d of %d index statements are unguarded", indexes-guardedIdx, indexes)
	}
	if n := strings.Count(sql, "CREATE FUNCTION"); n != 0 {
		t.Errorf("%d functions are created without OR REPLACE", n)
	}
}

func TestMigrationKeepsEventTimeAndIngestTimeApart(t *testing.T) {
	sql := migrationSQL(t)
	for _, want := range []string{
		"occurred_at TIMESTAMPTZ NOT NULL",
		"ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("events table is missing %q", want)
		}
	}
}

func TestMigrationCarriesTheUsageDedupKey(t *testing.T) {
	sql := migrationSQL(t)
	if !strings.Contains(sql, "PRIMARY KEY (message_id, request_id)") {
		t.Errorf("usage cannot be deduped on the contract's key")
	}
}

func TestMigrationNamesAreAppliedInOrder(t *testing.T) {
	names, err := migrationNames()
	if err != nil {
		t.Fatalf("migrationNames: %v", err)
	}
	if len(names) == 0 || names[0] != "0001_init.sql" {
		t.Fatalf("migrations = %v", names)
	}
	sorted := make([]string, len(names))
	copy(sorted, names)
	for i := 1; i < len(sorted); i++ {
		if sorted[i-1] > sorted[i] {
			t.Fatalf("migrations are not returned in deployment order: %v", names)
		}
	}
}

func TestMigrateSkipsWhatIsAlreadyApplied(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "SELECT EXISTS", rows: [][]any{{true}}},
	}}
	s := NewWithDB(db, nil)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if db.count("CREATE TABLE IF NOT EXISTS principals") != 0 {
		t.Errorf("re-applied a migration the ledger already recorded")
	}
	if db.count("pg_advisory_xact_lock") != 1 {
		t.Errorf("migrations are not serialised across concurrently starting servers")
	}
}

func TestMigrateAppliesAndRecordsAFreshDatabase(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "SELECT EXISTS", rows: [][]any{{false}}},
	}}
	s := NewWithDB(db, nil)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if db.count("CREATE TABLE IF NOT EXISTS principals") != 1 {
		t.Errorf("the schema was not applied:\n%s", db.summary())
	}
	// Counted against the number of migration files rather than a literal.
	// Every applied migration must leave a ledger row or it is applied again on
	// the next boot, and hardcoding the count here would make adding the second
	// migration look like a regression in this property rather than the routine
	// event it is.
	names, err := migrationNames()
	if err != nil {
		t.Fatalf("migrationNames: %v", err)
	}
	if got := db.count("INSERT INTO schema_migrations"); got != len(names) {
		t.Errorf("recorded %d of %d applied migrations; an unrecorded migration reapplies on every boot", got, len(names))
	}
	// One commit for the ledger table and one per file, each with its own
	// ledger row. A single commit for the whole run was the previous shape,
	// and it meant every ACCESS EXCLUSIVE lock any file took was held until
	// the last file finished, and a lock timeout in file three rolled back
	// files one and two with it.
	if want := len(names) + 1; db.committed != want {
		t.Errorf("committed %d times, want %d (the ledger table, then one per file)", db.committed, want)
	}
}

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

func TestGeneratedIdentifiersAreUniqueAndWellShaped(t *testing.T) {
	seen := map[string]bool{}
	for range 2000 {
		id := newUUID()
		if len(id) != 36 || id[14] != '4' {
			t.Fatalf("newUUID produced %q", id)
		}
		if seen[id] {
			t.Fatalf("newUUID collided on %q", id)
		}
		seen[id] = true
	}
	tokens := map[string]bool{}
	for range 2000 {
		tok := newToken()
		if len(tok) < 40 {
			t.Fatalf("share token %q is too short to be a credential", tok)
		}
		if tokens[tok] {
			t.Fatalf("share token collided on %q", tok)
		}
		tokens[tok] = true
	}
}

func TestIntervalIsRenderedForPostgres(t *testing.T) {
	if got := intervalString(90 * time.Minute); got != "5400.000 seconds" {
		t.Errorf("intervalString = %q, want seconds Postgres can parse", got)
	}
}

// Codex usage has neither a message id nor a request id — the rollout format
// carries no per-call identity — so it keys off the event id instead. That is
// what makes a re-import of a Codex session free, and it is worth pinning
// because the whole reason for reading Codex token counts is a re-walk of 790
// sessions that are already stored.
func TestCodexShapedUsageKeysOffTheEventID(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, flatPricer{perToken: 1})

	in := withUsage(ingestOf("e1", "sess", "me@example.com", event.AssistantTurn, 1),
		"gpt-5.4", "", "", 100, 10)
	if _, err := s.UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}

	args := db.find(t, sqlUsageLedger).args
	if got := args[0].([]string); len(got) != 1 || got[0] != "event:e1" {
		t.Fatalf("ledger message ids = %v, want the event-id fallback", got)
	}
	if got := args[1].([]string); len(got) != 1 || got[0] != "" {
		t.Errorf("ledger request ids = %v, want empty", got)
	}
}

// The second half of the same claim: a re-delivered event is not billed again.
//
// Usage is credited from the whole admitted batch, fresh or not, so that a
// row upgraded in place with usage its first delivery lacked is priced once
// (the Codex repair walk). What stops the repeat being billed is therefore
// the ledger alone: the statement it reaches is keyed on the message id and
// does nothing on conflict, and a session's totals move only by what the
// ledger reports back. This pins both halves: the repeat reaches a DO
// NOTHING statement under the same key, and with the ledger answering
// nothing, no rollup increment is written at all.
func TestARepeatedImportCreditsNoUsageASecondTime(t *testing.T) {
	in := withUsage(ingestOf("e1", "sess", "me@example.com", event.AssistantTurn, 1),
		"gpt-5.4", "", "", 100, 10)

	first := ingestDB("me@example.com", "e1")
	if _, err := NewWithDB(first, flatPricer{perToken: 1}).
		UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("first import: %v", err)
	}
	if n := first.count(sqlUsageLedger); n != 1 {
		t.Fatalf("first import wrote %d ledger statements, want 1", n)
	}

	// The same event again. Postgres returns no inserted ids for it, which is
	// what "already stored" looks like to this code, and the ledger returns
	// nothing, which is what "already priced" looks like.
	second := ingestDB("me@example.com")
	if _, err := NewWithDB(second, flatPricer{perToken: 1}).
		UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("second import: %v", err)
	}
	ledger := second.find(t, sqlUsageLedger)
	if !strings.Contains(ledger.sql, "ON CONFLICT (message_id, request_id) DO NOTHING") {
		t.Errorf("the repeat's ledger statement is not the DO NOTHING form:\n%s", ledger.sql)
	}
	if got := ledger.args[0].([]string); len(got) != 1 || got[0] != "event:e1" {
		t.Errorf("the repeat was keyed %v, want the same event-id fallback as the first import", got)
	}
	if n := second.count(sqlRollup); n != 0 {
		t.Errorf("the repeat wrote %d rollup increments with nothing credited; the turn would be billed twice", n)
	}
}
