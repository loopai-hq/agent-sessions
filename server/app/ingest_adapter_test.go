package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/ingest"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// The properties this file defends are not properties of the twenty lines of
// translation in ingest_adapter.go. They are properties of the path a captured
// event travels: adapter, storage layer, and the two independent deduplication
// keys the storage layer relies on. A double that merely recorded what it was
// handed would leave every one of them untested, because each of them is about
// what a SECOND delivery does.
//
// ingPG is therefore the slice of Postgres that store.UpsertEvents actually
// depends on: the six statements it issues, the conflict keys those statements
// declare, and transaction semantics strong enough that a rolled back batch
// leaves nothing behind. That Postgres really behaves this way is proven by the
// integration suite in server/store; what is proven here is that the adapter does
// not undo it.

// ---------------------------------------------------------------------------
// A fake Postgres, narrowed to the statements this path issues
// ---------------------------------------------------------------------------

// ingRollup is one applyDelta increment as it was written.
type ingRollup struct {
	SessionID  string
	Repo       string
	UserTurns  int
	Input      int64
	Output     int64
	CacheRead  int64
	CacheWrite int64
	CostUSD    float64
}

// ingEventRow is the subset of an events row the assertions read back.
type ingEventRow struct {
	SessionID string
	Email     string
	Body      string
	// CaptureVersion is what decides whether a re-delivered id is discarded or
	// replaces what is stored.
	CaptureVersion int32
}

type ingPG struct {
	mu sync.Mutex

	owners  map[string]string      // sessions.session_id -> owning email, set by the claim
	claimed map[string]string      // sessions.session_id -> device id presented by the claim
	events  map[string]ingEventRow // events, keyed by primary key
	ledger  map[string]bool        // usage_ledger, keyed on (message_id, request_id)
	reports map[string]bool        // health_reports, keyed on (email, device, emitted_at)

	rollups  []ingRollup
	messages int
	stmts    []string

	// failOn stands in for a database that is unreachable, deadlocked or out of
	// connections: any statement containing it fails, which is a transient
	// condition with no per-item meaning.
	failOn string

	begun, committed, rolledBack int
}

func newIngPG() *ingPG {
	return &ingPG{
		owners:  map[string]string{},
		claimed: map[string]string{},
		events:  map[string]ingEventRow{},
		ledger:  map[string]bool{},
		reports: map[string]bool{},
	}
}

// Query, QueryRow and Exec outside a transaction are refused rather than served.
// Every statement on this path is issued inside one, and a statement that
// escaped its transaction is exactly the kind of change these tests exist to
// catch, so it fails loudly instead of quietly working.
func (p *ingPG) Query(context.Context, string, ...any) (store.Rows, error) {
	return nil, errors.New("ingPG: query issued outside a transaction")
}

func (p *ingPG) QueryRow(context.Context, string, ...any) store.Row {
	return ingRow{err: errors.New("ingPG: query issued outside a transaction")}
}

func (p *ingPG) Exec(context.Context, string, ...any) (int64, error) {
	return 0, errors.New("ingPG: statement issued outside a transaction")
}

func (p *ingPG) Begin(context.Context) (store.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begun++
	return &ingPGTx{
		db:      p,
		owners:  map[string]string{},
		claimed: map[string]string{},
		events:  map[string]ingEventRow{},
		ledger:  map[string]bool{},
		reports: map[string]bool{},
	}, nil
}

func (p *ingPG) snapshot() (events, ledger, rollups, messages int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events), len(p.ledger), len(p.rollups), p.messages
}

// issued counts the statements so far containing the fragment.
func (p *ingPG) issued(fragment string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, s := range p.stmts {
		if strings.Contains(s, fragment) {
			n++
		}
	}
	return n
}

func (p *ingPG) credited() ingRollup {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total ingRollup
	for _, r := range p.rollups {
		total.UserTurns += r.UserTurns
		total.Input += r.Input
		total.Output += r.Output
		total.CacheRead += r.CacheRead
		total.CacheWrite += r.CacheWrite
		total.CostUSD += r.CostUSD
	}
	return total
}

// ingPGTx stages writes and applies them only on commit, so a failed batch
// really does leave nothing behind and the assertions about it mean something.
type ingPGTx struct {
	db *ingPG

	owners   map[string]string
	claimed  map[string]string
	events   map[string]ingEventRow
	ledger   map[string]bool
	reports  map[string]bool
	rollups  []ingRollup
	messages int
	done     bool
}

func (t *ingPGTx) record(sql string) error {
	t.db.mu.Lock()
	t.db.stmts = append(t.db.stmts, sql)
	fail := t.db.failOn
	t.db.mu.Unlock()
	if fail != "" && strings.Contains(sql, fail) {
		return fmt.Errorf("ingPG: %w", errIngUnreachable)
	}
	return nil
}

var errIngUnreachable = errors.New("connection refused")

// owner resolves a session's owner across the committed rows and this
// transaction's own pending ones.
func (t *ingPGTx) owner(sessionID string) (string, bool) {
	if e, ok := t.owners[sessionID]; ok {
		return e, true
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	e, ok := t.db.owners[sessionID]
	return e, ok
}

// event resolves a stored event across the committed rows and this
// transaction's own pending ones, which is what the upsert's conflict target
// sees. Reading only the pending map would make every redelivery look new.
func (t *ingPGTx) event(id string) (ingEventRow, bool) {
	if e, ok := t.events[id]; ok {
		return e, true
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	e, ok := t.db.events[id]
	return e, ok
}

func (t *ingPGTx) haveEvent(id string) bool {
	if _, ok := t.events[id]; ok {
		return true
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	_, ok := t.db.events[id]
	return ok
}

func (t *ingPGTx) haveLedger(key string) bool {
	if t.ledger[key] {
		return true
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	return t.db.ledger[key]
}

func (t *ingPGTx) haveReport(key string) bool {
	if t.reports[key] {
		return true
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	return t.db.reports[key]
}

func (t *ingPGTx) Query(_ context.Context, sql string, args ...any) (store.Rows, error) {
	if err := t.record(sql); err != nil {
		return nil, err
	}
	switch {
	case strings.Contains(sql, "INSERT INTO events"):
		return t.insertEvents(args)
	case strings.Contains(sql, "INSERT INTO usage_ledger"):
		return t.creditUsage(args)
	}
	return nil, fmt.Errorf("ingPG: unexpected query: %s", ingFirstLine(sql))
}

func (t *ingPGTx) QueryRow(_ context.Context, sql string, args ...any) store.Row {
	if err := t.record(sql); err != nil {
		return ingRow{err: err}
	}
	if strings.Contains(sql, "INSERT INTO sessions") && strings.Contains(sql, "RETURNING email") {
		return t.claimSession(args)
	}
	return ingRow{err: fmt.Errorf("ingPG: unexpected row query: %s", ingFirstLine(sql))}
}

func (t *ingPGTx) Exec(_ context.Context, sql string, args ...any) (int64, error) {
	if err := t.record(sql); err != nil {
		return 0, err
	}
	switch {
	case strings.Contains(sql, "INSERT INTO messages"):
		t.messages++
		return 1, nil
	case strings.Contains(sql, "INSERT INTO sessions"):
		return t.applyDelta(args)
	case strings.Contains(sql, "UPDATE sessions s"):
		return 1, nil
	case strings.Contains(sql, "INSERT INTO health_reports"):
		return t.putHealthReport(args)
	case strings.Contains(sql, "INSERT INTO health_latest"):
		// The newest-report upsert rides the raw insert's transaction
		// (migration 0020); the raw insert above is what this fake models.
		return 1, nil
	case strings.Contains(sql, "UPDATE devices"):
		return 1, nil
	}
	return 0, fmt.Errorf("ingPG: unexpected statement: %s", ingFirstLine(sql))
}

// claimSession reproduces INSERT ... ON CONFLICT (session_id) DO UPDATE
// ... RETURNING email: the first writer owns the session and every later claim
// reads back that owner rather than replacing it.
func (t *ingPGTx) claimSession(args []any) store.Row {
	sessionID, err := ingString(args, 0)
	if err != nil {
		return ingRow{err: err}
	}
	email, err := ingString(args, 1)
	if err != nil {
		return ingRow{err: err}
	}
	deviceID, err := ingString(args, 2)
	if err != nil {
		return ingRow{err: err}
	}
	if owner, ok := t.owner(sessionID); ok {
		return ingRow{vals: []any{owner}}
	}
	t.owners[sessionID] = email
	t.claimed[sessionID] = deviceID
	return ingRow{vals: []any{email}}
}

// insertEvents reproduces the upsert: ON CONFLICT (id) DO UPDATE guarded by a
// strictly greater capture_version, RETURNING id and whether the row was
// inserted rather than updated.
//
// Both halves matter to the caller. Only genuinely new ids may come back as
// inserted, which is what makes a redelivery free; and an event re-extracted by
// a newer agent must come back as updated rather than new, because usage is
// credited from inserts and counting an upgrade would bill it twice.
func (t *ingPGTx) insertEvents(args []any) (store.Rows, error) {
	ids, err := ingStrings(args, 0)
	if err != nil {
		return nil, err
	}
	sessionIDs, err := ingStrings(args, 1)
	if err != nil {
		return nil, err
	}
	emails, err := ingStrings(args, 2)
	if err != nil {
		return nil, err
	}
	bodies, err := ingStrings(args, 11)
	if err != nil {
		return nil, err
	}
	versions, err := ingInts(args, 12)
	if err != nil {
		return nil, err
	}

	var out [][]any
	for i, id := range ids {
		prev, seen := t.event(id)
		switch {
		case !seen:
			t.events[id] = ingEventRow{
				SessionID: sessionIDs[i], Email: emails[i], Body: bodies[i],
				CaptureVersion: versions[i],
			}
			out = append(out, []any{id, true}) // inserted
		case versions[i] > prev.CaptureVersion:
			prev.Body = bodies[i]
			prev.CaptureVersion = versions[i]
			t.events[id] = prev
			out = append(out, []any{id, false}) // updated, not new
		default:
			// Same or older extraction: the WHERE fails, nothing is written and
			// nothing is returned.
		}
	}
	return &ingRows{rows: out}, nil
}

// ingInts reads an int array argument, which unnest receives as $13.
func ingInts(args []any, i int) ([]int32, error) {
	if i >= len(args) {
		return nil, fmt.Errorf("ingPG: argument %d is missing", i)
	}
	v, ok := args[i].([]int32)
	if !ok {
		return nil, fmt.Errorf("ingPG: argument %d is %T, want []int32", i, args[i])
	}
	return v, nil
}

// creditUsage reproduces ON CONFLICT (message_id, request_id) DO NOTHING
// ... RETURNING event_id, cost_usd. The key is the identity of the model call and
// not the id of the event carrying it, which is the distinction the whole cost
// arithmetic rests on.
func (t *ingPGTx) creditUsage(args []any) (store.Rows, error) {
	messageIDs, err := ingStrings(args, 0)
	if err != nil {
		return nil, err
	}
	requestIDs, err := ingStrings(args, 1)
	if err != nil {
		return nil, err
	}
	eventIDs, err := ingStrings(args, 2)
	if err != nil {
		return nil, err
	}
	costs, err := ingFloats(args, 12)
	if err != nil {
		return nil, err
	}

	var out [][]any
	for i := range messageIDs {
		key := messageIDs[i] + "\x00" + requestIDs[i]
		if t.haveLedger(key) {
			continue
		}
		t.ledger[key] = true
		out = append(out, []any{eventIDs[i], costs[i]})
	}
	return &ingRows{rows: out}, nil
}

func (t *ingPGTx) applyDelta(args []any) (int64, error) {
	sessionID, err := ingString(args, 0)
	if err != nil {
		return 0, err
	}
	repo, err := ingString(args, 6)
	if err != nil {
		return 0, err
	}
	userTurns, err := ingInt(args, 11)
	if err != nil {
		return 0, err
	}
	in, err := ingInt64(args, 16)
	if err != nil {
		return 0, err
	}
	out, err := ingInt64(args, 17)
	if err != nil {
		return 0, err
	}
	cacheRead, err := ingInt64(args, 18)
	if err != nil {
		return 0, err
	}
	cacheWrite, err := ingInt64(args, 19)
	if err != nil {
		return 0, err
	}
	cost, err := ingFloat(args, 20)
	if err != nil {
		return 0, err
	}
	t.rollups = append(t.rollups, ingRollup{
		SessionID:  sessionID,
		Repo:       repo,
		UserTurns:  userTurns,
		Input:      in,
		Output:     out,
		CacheRead:  cacheRead,
		CacheWrite: cacheWrite,
		CostUSD:    cost,
	})
	return 1, nil
}

// putHealthReport reproduces ON CONFLICT (email, device_id, emitted_at) DO
// NOTHING, so a redelivered sample does not read as a second machine checking in.
func (t *ingPGTx) putHealthReport(args []any) (int64, error) {
	email, err := ingString(args, 0)
	if err != nil {
		return 0, err
	}
	deviceID, err := ingString(args, 1)
	if err != nil {
		return 0, err
	}
	emitted, ok := args[2].(time.Time)
	if !ok {
		return 0, fmt.Errorf("ingPG: health report emitted_at is %T", args[2])
	}
	key := email + "\x00" + deviceID + "\x00" + emitted.String()
	if t.haveReport(key) {
		return 0, nil
	}
	t.reports[key] = true
	return 1, nil
}

func (t *ingPGTx) Commit(context.Context) error {
	if t.done {
		return errors.New("ingPG: commit after the transaction ended")
	}
	t.done = true
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	for k, v := range t.owners {
		t.db.owners[k] = v
	}
	for k, v := range t.claimed {
		t.db.claimed[k] = v
	}
	for k, v := range t.events {
		t.db.events[k] = v
	}
	for k := range t.ledger {
		t.db.ledger[k] = true
	}
	for k := range t.reports {
		t.db.reports[k] = true
	}
	t.db.rollups = append(t.db.rollups, t.rollups...)
	t.db.messages += t.messages
	t.db.committed++
	return nil
}

func (t *ingPGTx) Rollback(context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.db.rolledBack++
	return nil
}

// ---------------------------------------------------------------------------
// Result plumbing for the fake
// ---------------------------------------------------------------------------

type ingRows struct {
	rows [][]any
	i    int
}

func (r *ingRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *ingRows) Scan(dest ...any) error { return ingScan(r.rows[r.i-1], dest) }
func (r *ingRows) Err() error             { return nil }
func (r *ingRows) Close()                 {}

type ingRow struct {
	vals []any
	err  error
}

func (r ingRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return ingScan(r.vals, dest)
}

func ingScan(vals []any, dest []any) error {
	if len(vals) != len(dest) {
		return fmt.Errorf("ingPG: scan wants %d columns, row has %d", len(dest), len(vals))
	}
	for i, d := range dest {
		switch d := d.(type) {
		case *string:
			v, ok := vals[i].(string)
			if !ok {
				return fmt.Errorf("ingPG: column %d is %T, not string", i, vals[i])
			}
			*d = v
		case *float64:
			v, ok := vals[i].(float64)
			if !ok {
				return fmt.Errorf("ingPG: column %d is %T, not float64", i, vals[i])
			}
			*d = v
		case *bool:
			// The upsert's second column: inserted rather than updated.
			v, ok := vals[i].(bool)
			if !ok {
				return fmt.Errorf("ingPG: column %d is %T, not bool", i, vals[i])
			}
			*d = v
		default:
			return fmt.Errorf("ingPG: unsupported scan target %T", d)
		}
	}
	return nil
}

func ingString(args []any, i int) (string, error) {
	if i >= len(args) {
		return "", fmt.Errorf("ingPG: argument %d is missing", i+1)
	}
	v, ok := args[i].(string)
	if !ok {
		return "", fmt.Errorf("ingPG: argument %d is %T, not string", i+1, args[i])
	}
	return v, nil
}

func ingStrings(args []any, i int) ([]string, error) {
	if i >= len(args) {
		return nil, fmt.Errorf("ingPG: argument %d is missing", i+1)
	}
	v, ok := args[i].([]string)
	if !ok {
		return nil, fmt.Errorf("ingPG: argument %d is %T, not []string", i+1, args[i])
	}
	return v, nil
}

func ingFloats(args []any, i int) ([]float64, error) {
	if i >= len(args) {
		return nil, fmt.Errorf("ingPG: argument %d is missing", i+1)
	}
	v, ok := args[i].([]float64)
	if !ok {
		return nil, fmt.Errorf("ingPG: argument %d is %T, not []float64", i+1, args[i])
	}
	return v, nil
}

func ingInt(args []any, i int) (int, error) {
	if i >= len(args) {
		return 0, fmt.Errorf("ingPG: argument %d is missing", i+1)
	}
	v, ok := args[i].(int)
	if !ok {
		return 0, fmt.Errorf("ingPG: argument %d is %T, not int", i+1, args[i])
	}
	return v, nil
}

func ingInt64(args []any, i int) (int64, error) {
	if i >= len(args) {
		return 0, fmt.Errorf("ingPG: argument %d is missing", i+1)
	}
	v, ok := args[i].(int64)
	if !ok {
		return 0, fmt.Errorf("ingPG: argument %d is %T, not int64", i+1, args[i])
	}
	return v, nil
}

func ingFloat(args []any, i int) (float64, error) {
	if i >= len(args) {
		return 0, fmt.Errorf("ingPG: argument %d is missing", i+1)
	}
	v, ok := args[i].(float64)
	if !ok {
		return 0, fmt.Errorf("ingPG: argument %d is %T, not float64", i+1, args[i])
	}
	return v, nil
}

func ingFirstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// ingPricer charges a power-of-two rate per token, so a credited total is
// exactly representable in float64 and the assertions can compare it without a
// tolerance. That is the difference between a test that fails on a real defect
// and one that fails on the last bit of a sum.
type ingPricer struct{}

func (ingPricer) CostUSD(_ string, u event.Usage, _ time.Time) float64 {
	return float64(u.InputTokens+u.OutputTokens) / 1024
}

// ingCallCost is what ingPricer charges for the single priced call in the
// fixtures below.
const ingCallCost = (1000 + 200) / 1024.0

var ingClock = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

const (
	ingAlice   = "alice@example.com"
	ingBob     = "bob@example.com"
	ingDevice  = "11111111-2222-3333-4444-555555555555"
	ingSession = "session-1"
)

func ingEvent(id string, typ event.Type, seq int64) event.Event {
	return event.Event{
		ID:         id,
		SessionID:  ingSession,
		Source:     event.SourceClaudeCode,
		Origin:     event.OriginTranscript,
		Type:       typ,
		Seq:        seq,
		OccurredAt: ingClock.Add(time.Duration(seq) * time.Second),
	}
}

func ingRecord(t *testing.T, ev event.Event) ingest.Record {
	t.Helper()
	body, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event %s: %v", ev.ID, err)
	}
	return ingest.Record{
		Event:    ev,
		Email:    ingAlice,
		DeviceID: ingDevice,
		Repo:     "loop-sessions",
		Body:     body,
	}
}

// ingBatch is one user prompt, one priced assistant turn and one tool call: the
// smallest batch that exercises both deduplication keys at once.
func ingBatch(t *testing.T) []ingest.Record {
	t.Helper()
	turn := ingEvent("event-2", event.AssistantTurn, 2)
	turn.Model = "claude-opus"
	turn.Text = "an answer"
	turn.Usage = &event.Usage{
		InputTokens:  1000,
		OutputTokens: 200,
		MessageID:    "msg_01",
		RequestID:    "req_01",
	}
	prompt := ingEvent("event-1", event.UserPrompt, 1)
	prompt.Text = "a question"
	return []ingest.Record{
		ingRecord(t, prompt),
		ingRecord(t, turn),
		ingRecord(t, ingEvent("event-3", event.ToolCall, 3)),
	}
}

func ingNewStore(db *ingPG) ingest.Store {
	return NewIngestStore(store.NewWithDB(db, ingPricer{}))
}

// ---------------------------------------------------------------------------
// The write path
// ---------------------------------------------------------------------------

// A redelivery is the ordinary shape of a healthy agent recovering from an
// interruption, so the second delivery of a batch must leave the database exactly
// as the first did: no second event row, no second ledger row, and no rollup
// increment at all. Anything less turns every network flap into inflated activity
// and inflated cost.
func TestIngestStoreRedeliveredBatchMovesNoEventAndNoTokenCounter(t *testing.T) {
	db := newIngPG()
	adapter := ingNewStore(db)
	batch := ingBatch(t)

	credited := ingRollup{UserTurns: 1, Input: 1000, Output: 200, CostUSD: ingCallCost}
	deliveries := []struct {
		name string
		want ingest.Result
		// The totals below are cumulative: every one of them is the same after
		// the second delivery as after the first, which is the property.
		wantEvents       int
		wantLedger       int
		wantRollups      int
		wantLedgerStmts  int
		wantCredited     ingRollup
		wantRolledBackTx int
	}{
		{
			name:             "first delivery stores every event and credits the call once",
			want:             ingest.Result{Inserted: []string{"event-1", "event-2", "event-3"}},
			wantEvents:       3,
			wantLedger:       1,
			wantRollups:      1,
			wantLedgerStmts:  1,
			wantCredited:     credited,
			wantRolledBackTx: 0,
		},
		{
			name:        "redelivery accepts every event again and changes nothing",
			want:        ingest.Result{Duplicate: []string{"event-1", "event-2", "event-3"}},
			wantEvents:  3,
			wantLedger:  1,
			wantRollups: 1,
			// The repeat reaches the ledger: usage is credited from the whole
			// admitted batch, fresh or not, so a row upgraded in place with
			// the usage its first delivery lacked is priced once. What stops
			// the repeat being billed is the ledger's key, which is why the
			// row count and the credited totals above do not move.
			wantLedgerStmts:  2,
			wantCredited:     credited,
			wantRolledBackTx: 0,
		},
	}

	for _, d := range deliveries {
		t.Run(d.name, func(t *testing.T) {
			got, err := adapter.UpsertEvents(context.Background(), batch)
			if err != nil {
				t.Fatalf("UpsertEvents: %v", err)
			}
			// Duplicate rather than Rejected is what makes a redelivery
			// terminate: the endpoint acknowledges the union of the two accepted
			// lists, and an item reported as neither is retried forever.
			if !reflect.DeepEqual(got, d.want) {
				t.Errorf("result = %+v, want %+v", got, d.want)
			}
			events, ledger, rollups, _ := db.snapshot()
			if events != d.wantEvents {
				t.Errorf("stored events = %d, want %d", events, d.wantEvents)
			}
			if ledger != d.wantLedger {
				t.Errorf("usage ledger rows = %d, want %d", ledger, d.wantLedger)
			}
			if rollups != d.wantRollups {
				t.Errorf("rollup increments = %d, want %d", rollups, d.wantRollups)
			}
			if n := db.issued("INSERT INTO usage_ledger"); n != d.wantLedgerStmts {
				t.Errorf("usage ledger statements = %d, want %d: every admitted batch"+
					" is offered to the ledger, and the ledger's key is what refuses the repeat", n, d.wantLedgerStmts)
			}
			if got := db.credited(); !reflect.DeepEqual(got, d.wantCredited) {
				t.Errorf("credited totals = %+v, want %+v", got, d.wantCredited)
			}
			if db.rolledBack != d.wantRolledBackTx {
				t.Errorf("rolled back %d transactions, want %d", db.rolledBack, d.wantRolledBackTx)
			}
		})
	}
}

// Usage is credited through a ledger keyed on the identity of the model call, not
// on the id of the event that reported it, because one call appears in two
// transcript records and therefore arrives as two events with two different ids.
// Conflating the two keys is silent: it produces no error and inflates every token
// and cost figure in the product.
func TestIngestStoreCreditsOneModelCallOnceAcrossTwoEventIDs(t *testing.T) {
	db := newIngPG()
	adapter := ingNewStore(db)

	call := event.Usage{InputTokens: 1000, OutputTokens: 200, MessageID: "msg_01", RequestID: "req_01"}

	first := ingEvent("event-live", event.AssistantTurn, 2)
	first.Model = "claude-opus"
	first.Usage = &call

	// The same model call, re-reported by a transcript backfill under its own
	// event id. The event is genuinely new and must be stored; its tokens are not.
	second := ingEvent("event-backfill", event.AssistantTurn, 2)
	second.Model = "claude-opus"
	second.Usage = &call

	for _, rec := range []ingest.Record{ingRecord(t, first), ingRecord(t, second)} {
		res, err := adapter.UpsertEvents(context.Background(), []ingest.Record{rec})
		if err != nil {
			t.Fatalf("UpsertEvents %s: %v", rec.Event.ID, err)
		}
		if want := (ingest.Result{Inserted: []string{rec.Event.ID}}); !reflect.DeepEqual(res, want) {
			t.Fatalf("result for %s = %+v, want %+v", rec.Event.ID, res, want)
		}
	}

	events, ledger, _, _ := db.snapshot()
	if events != 2 {
		t.Errorf("stored events = %d, want 2: both events are real records and both must be kept", events)
	}
	if ledger != 1 {
		t.Errorf("usage ledger rows = %d, want 1: one model call is one ledger row", ledger)
	}
	want := ingRollup{Input: 1000, Output: 200, CostUSD: ingCallCost}
	if got := db.credited(); !reflect.DeepEqual(got, want) {
		t.Errorf("credited totals = %+v, want %+v: the second event must contribute nothing", got, want)
	}
}

// The agent deletes exactly what it is told was accepted and permanently
// quarantines exactly what it is told was rejected, so a mixed batch has to be
// answered item by item and each verdict has to be the storage layer's own.
func TestIngestStoreReportsPartialAcceptanceItemByItem(t *testing.T) {
	db := newIngPG()
	adapter := ingNewStore(db)

	good := ingRecord(t, ingEvent("event-good", event.UserPrompt, 1))

	// Permanently unstorable: no type, and no later delivery can add one.
	malformed := ingRecord(t, ingEvent("event-malformed", event.UserPrompt, 2))
	malformed.Event.Type = ""

	// Storable in principle, but filed under a session another principal already
	// owns. Session ids come from the client, so this is the check that stops one
	// enrolled laptop writing into a colleague's transcript.
	intruder := ingRecord(t, ingEvent("event-intruder", event.UserPrompt, 3))
	intruder.Email = ingBob

	got, err := adapter.UpsertEvents(context.Background(),
		[]ingest.Record{good, malformed, intruder})
	if err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}

	want := ingest.Result{
		Inserted: []string{"event-good"},
		Rejected: []ingest.Rejection{
			{ID: "event-malformed", Reason: "missing event type"},
			{ID: "event-intruder", Reason: "session belongs to another principal"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result = %+v, want %+v", got, want)
	}

	events, _, _, _ := db.snapshot()
	if events != 1 {
		t.Errorf("stored events = %d, want 1: only the accepted item may be stored", events)
	}
	if owner := db.owners[ingSession]; owner != ingAlice {
		t.Errorf("session owner = %q, want %q", owner, ingAlice)
	}
}

// A transient failure has no per-item meaning. Everything in the batch stays the
// agent's, including the items the storage layer had already judged unacceptable,
// because a verdict alongside a failed write would be a verdict on a transaction
// that did not happen.
func TestIngestStoreClaimsNothingAcceptedWhenTheWriteFails(t *testing.T) {
	db := newIngPG()
	db.failOn = "INSERT INTO events"
	adapter := ingNewStore(db)

	got, err := adapter.UpsertEvents(context.Background(), ingBatch(t))
	if err == nil {
		t.Fatal("UpsertEvents returned no error for a failed write")
	}
	if !errors.Is(err, errIngUnreachable) {
		t.Errorf("error = %v, want it to wrap the database failure", err)
	}
	if !reflect.DeepEqual(got, ingest.Result{}) {
		t.Errorf("result = %+v, want the zero Result: nothing was stored, so nothing may be acknowledged", got)
	}

	events, ledger, rollups, _ := db.snapshot()
	if events != 0 || ledger != 0 || rollups != 0 {
		t.Errorf("after a failed batch: events=%d ledger=%d rollups=%d, want all zero", events, ledger, rollups)
	}
	if db.committed != 0 || db.rolledBack != 1 {
		t.Errorf("committed=%d rolledBack=%d, want 0 and 1", db.committed, db.rolledBack)
	}
}

// Attribution is derived from the delivering credential and is what every later
// permission check keys on, so each field has to survive the crossing. The
// delivered body has to survive it verbatim as well: it is the only copy of what
// actually arrived once the laptop drops the spool item.
func TestIngestStoreCarriesAttributionAndBodyOntoTheStoredRow(t *testing.T) {
	db := newIngPG()
	adapter := ingNewStore(db)

	rec := ingRecord(t, ingEvent("event-1", event.UserPrompt, 1))
	rec.Body = json.RawMessage(`{"id":"event-1","note":"the delivered bytes, not a re-marshalling"}`)

	if _, err := adapter.UpsertEvents(context.Background(), []ingest.Record{rec}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}

	stored, ok := db.events["event-1"]
	if !ok {
		t.Fatal("event-1 was not stored")
	}
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"email", stored.Email, ingAlice},
		{"session id", stored.SessionID, ingSession},
		{"body", stored.Body, string(rec.Body)},
		{"device id on the session claim", db.claimed[ingSession], ingDevice},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.field, c.got, c.want)
		}
	}
	if len(db.rollups) != 1 || db.rollups[0].Repo != "loop-sessions" {
		t.Errorf("rollup repo = %+v, want one increment carrying the resolved repo", db.rollups)
	}
}

// The fleet view alerts on the absence of health reports, so a report the storage
// layer refused must reach the caller as an error. Swallowing it would make a
// machine that is reporting indistinguishable from one that has gone quiet, in
// the direction that hides the problem.
func TestIngestStoreHealthReportFailureReachesTheCaller(t *testing.T) {
	cases := []struct {
		name        string
		failOn      string
		wantErr     bool
		wantReports int
	}{
		{name: "a stored report reports success", wantReports: 1},
		{
			name:        "a refused report is an error, never a silent success",
			failOn:      "INSERT INTO health_reports",
			wantErr:     true,
			wantReports: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := newIngPG()
			db.failOn = c.failOn
			adapter := ingNewStore(db)

			err := adapter.PutHealthReport(context.Background(), ingAlice, ingDevice,
				health.Report{EmittedAt: ingClock})
			switch {
			case c.wantErr && err == nil:
				t.Fatal("PutHealthReport returned no error for a refused write")
			case c.wantErr && !errors.Is(err, errIngUnreachable):
				t.Fatalf("error = %v, want it to wrap the database failure", err)
			case !c.wantErr && err != nil:
				t.Fatalf("PutHealthReport: %v", err)
			}
			if len(db.reports) != c.wantReports {
				t.Errorf("stored reports = %d, want %d", len(db.reports), c.wantReports)
			}
		})
	}
}

// A redelivered health sample is idempotent on (email, device, emitted_at), and
// the adapter must not make a second call look like a second machine.
func TestIngestStoreRedeliveredHealthReportStoresOnce(t *testing.T) {
	db := newIngPG()
	adapter := ingNewStore(db)
	report := health.Report{EmittedAt: ingClock}

	for i := range 2 {
		if err := adapter.PutHealthReport(context.Background(), ingAlice, ingDevice, report); err != nil {
			t.Fatalf("PutHealthReport %d: %v", i+1, err)
		}
	}
	if len(db.reports) != 1 {
		t.Errorf("stored reports = %d, want 1", len(db.reports))
	}
}

// The client acknowledges spool items for exactly what the server reports as
// accepted, so a Result that overstates acceptance is permanent, silent data loss
// on the laptop. Two ways to overstate it are covered here: carrying acceptance
// out beside a failure, and letting the session ids reappear as a verdict on an
// item.
//
// The failing case is driven directly rather than through the storage layer,
// because the storage layer currently zeroes its own result on the way out and a
// test routed through it would pass whether this rule were implemented or not.
// The rule has to hold on its own.
func TestIngestVerdictNeverReportsAcceptanceItCannotVouchFor(t *testing.T) {
	full := store.UpsertResult{
		Inserted:  []string{"event-1"},
		Duplicate: []string{"event-2"},
		Rejected:  []store.Rejected{{ID: "event-3", Reason: "missing event type"}},
		Sessions:  []string{ingSession},
	}
	failed := errors.New("store: commit ingest: connection reset")

	cases := []struct {
		name string
		res  store.UpsertResult
		err  error
		want ingest.Result
	}{
		{
			name: "a verdict the storage layer stands behind is passed through item by item",
			res:  full,
			want: ingest.Result{
				Inserted:  []string{"event-1"},
				Duplicate: []string{"event-2"},
				Rejected:  []ingest.Rejection{{ID: "event-3", Reason: "missing event type"}},
			},
		},
		{
			name: "a populated result arriving with a failure claims nothing",
			res:  full,
			err:  failed,
			want: ingest.Result{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ingestVerdict(c.res, c.err)
			if !errors.Is(err, c.err) {
				t.Fatalf("error = %v, want %v", err, c.err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("result = %+v, want %+v", got, c.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Device credentials
// ---------------------------------------------------------------------------

// ingDeviceStore is the credential lookup auth.Devices runs against.
type ingDeviceStore struct {
	row auth.TokenRow
	err error
}

func (s *ingDeviceStore) InsertDeviceToken(context.Context, auth.TokenRecord) error { return nil }

func (s *ingDeviceStore) DeviceTokenByHash(_ context.Context, hash []byte) (auth.TokenRow, error) {
	if s.err != nil {
		return auth.TokenRow{}, s.err
	}
	row := s.row
	// Verify re-checks the hash in constant time, so the row has to carry the one
	// it was looked up by or every case below would fail as "unknown" instead of
	// for the reason it is testing.
	row.TokenHash = hash
	return row, nil
}

func (s *ingDeviceStore) RevokeDeviceToken(context.Context, string, time.Time) error { return nil }
func (s *ingDeviceStore) TouchDeviceToken(context.Context, string, time.Time) error  { return nil }

func ingNewDevices(t *testing.T, ds *ingDeviceStore) ingest.Devices {
	t.Helper()
	d, err := auth.NewDevices(auth.DeviceOptions{
		Store: ds,
		Now:   func() time.Time { return ingClock },
	})
	if err != nil {
		t.Fatalf("auth.NewDevices: %v", err)
	}
	return NewIngestDevices(d)
}

func ingLiveRow() auth.TokenRow {
	return auth.TokenRow{
		TokenID:  "token-1",
		DeviceID: ingDevice,
		Email:    ingAlice,
		Role:     auth.RoleMember,
	}
}

// The endpoint answers 401 for a bad credential and 503 for a failure of ours,
// and those two answers mean opposite things to an agent: the first sends a
// laptop to re-enrol, the second makes it keep everything it captured and retry.
// Classifying a database outage as a credential failure would therefore tell a
// fleet of working laptops that their credentials are dead.
func TestIngestDevicesSeparatesBadCredentialsFromFailuresOfOurs(t *testing.T) {
	past := ingClock.Add(-time.Hour)
	valid := auth.TokenPrefix + "aaaabbbbccccdddd"

	cases := []struct {
		name      string
		presented string
		lookup    *ingDeviceStore
		// unauthenticated is true when the caller must be told its credential is
		// unusable, false when the failure is ours and must not be blamed on them.
		unauthenticated bool
		under           error
	}{
		{
			name:            "a token in the wrong format is the caller's problem",
			presented:       "not-a-loop-token",
			lookup:          &ingDeviceStore{},
			unauthenticated: true,
			under:           auth.ErrDeviceTokenMalformed,
		},
		{
			name:            "an unrecognised token is the caller's problem",
			presented:       valid,
			lookup:          &ingDeviceStore{err: fmt.Errorf("lookup: %w", auth.ErrDeviceTokenUnknown)},
			unauthenticated: true,
			under:           auth.ErrDeviceTokenUnknown,
		},
		{
			name:            "a lookup that matched no row is an unrecognised token",
			presented:       valid,
			lookup:          &ingDeviceStore{err: store.ErrNotFound},
			unauthenticated: true,
			under:           store.ErrNotFound,
		},
		{
			name:      "a revoked token is the caller's problem",
			presented: valid,
			lookup: func() *ingDeviceStore {
				row := ingLiveRow()
				row.RevokedAt = past
				return &ingDeviceStore{row: row}
			}(),
			unauthenticated: true,
			under:           auth.ErrDeviceTokenRevoked,
		},
		{
			name:      "an expired token is the caller's problem",
			presented: valid,
			lookup: func() *ingDeviceStore {
				row := ingLiveRow()
				row.ExpiresAt = past
				return &ingDeviceStore{row: row}
			}(),
			unauthenticated: true,
			under:           auth.ErrDeviceTokenExpired,
		},
		{
			name:      "a revoked device is the caller's problem",
			presented: valid,
			lookup: func() *ingDeviceStore {
				row := ingLiveRow()
				row.DeviceRevokedAt = past
				return &ingDeviceStore{row: row}
			}(),
			unauthenticated: true,
			under:           auth.ErrDeviceRevoked,
		},
		{
			name:      "an offboarded colleague is the caller's problem",
			presented: valid,
			lookup: func() *ingDeviceStore {
				row := ingLiveRow()
				row.PrincipalDisabledAt = past
				return &ingDeviceStore{row: row}
			}(),
			unauthenticated: true,
			under:           auth.ErrPrincipalDisabled,
		},
		{
			name:            "a database we cannot reach is ours, not theirs",
			presented:       valid,
			lookup:          &ingDeviceStore{err: fmt.Errorf("dial: %w", errIngUnreachable)},
			unauthenticated: false,
			under:           errIngUnreachable,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			devices := ingNewDevices(t, c.lookup)
			id, err := devices.Verify(context.Background(), c.presented)
			if err == nil {
				t.Fatal("Verify returned no error")
			}
			if id != (ingest.Identity{}) {
				t.Errorf("identity = %+v, want the zero Identity alongside a failure", id)
			}
			if got := errors.Is(err, ingest.ErrUnauthenticated); got != c.unauthenticated {
				t.Errorf("errors.Is(err, ErrUnauthenticated) = %v, want %v (err: %v)",
					got, c.unauthenticated, err)
			}
			if !errors.Is(err, c.under) {
				t.Errorf("error %v no longer wraps %v: the specific condition must stay recoverable", err, c.under)
			}
		})
	}
}

// The identity is the attribution every stored row keys on, and the role is
// deliberately not part of it: the upload path makes no permission decision, and
// a role carried into it would be a second place authorization could be decided.
func TestIngestDevicesCarriesEmailAndDeviceIDAndNothingElse(t *testing.T) {
	row := ingLiveRow()
	row.Email = "  Alice@Example.com  "
	row.Role = auth.RoleAdmin

	devices := ingNewDevices(t, &ingDeviceStore{row: row})
	got, err := devices.Verify(context.Background(), auth.TokenPrefix+"aaaabbbbccccdddd")
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	want := ingest.Identity{Email: ingAlice, DeviceID: ingDevice}
	if got != want {
		t.Errorf("identity = %+v, want %+v", got, want)
	}
}

// A nil dependency inside a non-nil interface passes ingest.New's own nil check
// and becomes a nil dereference on the first upload a laptop attempts. Refusing
// it at composition time is what keeps that from being discovered by the fleet.
func TestIngestConstructorsRefuseANilDependency(t *testing.T) {
	cases := []struct {
		name string
		call func()
	}{
		{"a nil store is refused", func() { NewIngestStore(nil) }},
		{"a nil device verifier is refused", func() { NewIngestDevices(nil) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("constructor accepted a nil dependency")
				}
			}()
			c.call()
		})
	}
}
