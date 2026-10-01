package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/health"
	"github.com/loopai-hq/loop-sessions/internal/spool"
)

const (
	testToken  = "lsd_test-token"
	testEmail  = "dev@example.com"
	testDevice = "11111111-1111-4111-8111-111111111111"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// capturingLogger returns a logger and the buffer it writes to. Requests are
// served on the calling goroutine, so the buffer needs no synchronisation.
func capturingLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

type fakeDevices struct {
	tokens map[string]Identity
	// err stands in for the credential lookup itself failing, which is not the
	// caller's fault and must not be answered as a bad credential.
	err error
}

func (f fakeDevices) Verify(_ context.Context, presented string) (Identity, error) {
	if f.err != nil {
		return Identity{}, f.err
	}
	id, ok := f.tokens[presented]
	if !ok {
		return Identity{}, fmt.Errorf("token not recognised: %w", ErrUnauthenticated)
	}
	return id, nil
}

func goodDevices() fakeDevices {
	return fakeDevices{tokens: map[string]Identity{
		testToken: {Email: testEmail, DeviceID: testDevice},
	}}
}

func newHandler(t *testing.T, st Store, dv Devices, tweak ...func(*Options)) *Handler {
	t.Helper()
	opts := Options{Store: st, Devices: dv, Logger: discardLogger()}
	for _, f := range tweak {
		f(&opts)
	}
	h, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return h
}

// post sends a request the way the agent does, with a bearer device token.
func post(t *testing.T, h *Handler, path, token string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func postItems(t *testing.T, h *Handler, items []spool.Item) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	return post(t, h, EventsPath, testToken, body)
}

func decodeResponse(t *testing.T, w *httptest.ResponseRecorder) Response {
	t.Helper()
	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response %q: %v", w.Body.String(), err)
	}
	return resp
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

var fixedTime = time.Date(2026, 3, 1, 4, 5, 6, 789012345, time.FixedZone("IST", 5*60*60+30*60))

func mkEvent(id string, seq int64, typ event.Type) event.Event {
	return event.Event{
		ID:         id,
		Source:     event.SourceClaudeCode,
		Origin:     event.OriginHook,
		Type:       typ,
		SessionID:  "sess-1",
		Seq:        seq,
		OccurredAt: fixedTime.Add(time.Duration(seq) * time.Second),
		Cwd:        "/home/dev/src/loop-sessions",
		Text:       "hello",
	}
}

func mkItem(t *testing.T, e event.Event) spool.Item {
	t.Helper()
	body, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return spool.Item{
		ID:        e.ID,
		Kind:      "event",
		SessionID: e.SessionID,
		Seq:       e.Seq,
		EventTime: e.OccurredAt,
		Payload:   body,
	}
}

func rawItem(id, sessionID, payload string) spool.Item {
	return spool.Item{ID: id, Kind: "event", SessionID: sessionID, Payload: json.RawMessage(payload)}
}

var testRates = []Rate{{
	Model:         "claude-opus-5",
	EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	InputPerMTok:  15,
	OutputPerMTok: 75,
}}

// sameTotals compares the counters a redelivery must not move.
func sameTotals(a, b rollup) bool {
	return a.StartedAt.Equal(b.StartedAt) &&
		a.EndedAt.Equal(b.EndedAt) &&
		a.Ended == b.Ended &&
		a.UserTurns == b.UserTurns &&
		a.ToolCalls == b.ToolCalls &&
		a.Subagents == b.Subagents &&
		a.Errors == b.Errors &&
		a.Input == b.Input &&
		a.Output == b.Output &&
		a.CacheRead == b.CacheRead &&
		a.CacheWrite == b.CacheWrite &&
		a.CostUSD == b.CostUSD &&
		maps.Equal(a.Redactions, b.Redactions)
}

// ---------------------------------------------------------------------------
// At-least-once delivery
// ---------------------------------------------------------------------------

// TestRedeliveryMovesNothing is the property the whole endpoint exists to hold.
// The agent resends after any interruption, so the second delivery of a batch
// has to leave the database exactly as the first did, through the rollup and the
// cost and not only through the insert.
func TestRedeliveryMovesNothing(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	prompt := mkEvent("e1", 1, event.UserPrompt)
	tool := mkEvent("e2", 2, event.ToolCall)
	turn := mkEvent("e3", 3, event.AssistantTurn)
	turn.Model = "claude-opus-5"
	turn.Usage = &event.Usage{
		InputTokens: 1000, OutputTokens: 500, CacheReadTokens: 20000,
		Ephemeral5m: 4000, MessageID: "msg_1", RequestID: "req_1",
	}
	batch := []spool.Item{mkItem(t, prompt), mkItem(t, tool), mkItem(t, turn)}

	first := postItems(t, h, batch)
	if first.Code != http.StatusOK {
		t.Fatalf("first delivery: status %d, body %s", first.Code, first.Body)
	}
	firstResp := decodeResponse(t, first)
	if len(firstResp.Accepted) != 3 || len(firstResp.Rejected) != 0 {
		t.Fatalf("first delivery: accepted %v rejected %v", firstResp.Accepted, firstResp.Rejected)
	}
	after := st.snapshot("sess-1")
	if after.UserTurns != 1 || after.ToolCalls != 1 {
		t.Fatalf("rollup after first delivery: %+v", after)
	}
	if after.CostUSD == 0 {
		t.Fatal("first delivery priced at zero; the cost path never ran")
	}

	second := postItems(t, h, batch)
	if second.Code != http.StatusOK {
		t.Fatalf("redelivery: status %d, body %s", second.Code, second.Body)
	}
	secondResp := decodeResponse(t, second)
	// Everything must still be acknowledged. An unacknowledged redelivery is
	// retried forever and the item is never freed from the laptop's spool.
	if len(secondResp.Accepted) != 3 || len(secondResp.Rejected) != 0 {
		t.Fatalf("redelivery: accepted %v rejected %v", secondResp.Accepted, secondResp.Rejected)
	}
	if got := st.snapshot("sess-1"); !sameTotals(after, got) {
		t.Fatalf("redelivery moved the rollup:\n before %+v\n after  %+v", after, got)
	}
	if st.count() != 3 {
		t.Fatalf("stored %d events after redelivering 3", st.count())
	}
}

// TestUsageCreditedOncePerModelCall covers the case event-id idempotency cannot
// reach: one model call reported by two transcript records is two events with
// two ids, and counting both inflates every token and cost figure silently.
func TestUsageCreditedOncePerModelCall(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	usage := &event.Usage{InputTokens: 1000, OutputTokens: 500, MessageID: "msg_7", RequestID: "req_7"}

	first := mkEvent("e1", 1, event.AssistantTurn)
	first.Model = "claude-opus-5"
	first.Usage = usage

	// Same call, different record, therefore a different event id.
	second := mkEvent("e2", 2, event.AssistantTurn)
	second.Model = "claude-opus-5"
	second.Origin = event.OriginTranscript
	second.Usage = usage

	w := postItems(t, h, []spool.Item{mkItem(t, first), mkItem(t, second)})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	if resp := decodeResponse(t, w); len(resp.Accepted) != 2 {
		t.Fatalf("both records should be stored: %+v", resp)
	}

	got := st.snapshot("sess-1")
	if got.Input != 1000 || got.Output != 500 {
		t.Fatalf("tokens double-counted: %+v", got)
	}
	want := (1000 * 15.0 / 1e6) + (500 * 75.0 / 1e6)
	if diff := got.CostUSD - want; diff > 1e-12 || diff < -1e-12 {
		t.Fatalf("cost %v, want %v", got.CostUSD, want)
	}
}

// TestSyntheticModelIsNotPriced keeps records that never became an API call out
// of the totals.
func TestSyntheticModelIsNotPriced(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	e := mkEvent("e1", 1, event.AssistantTurn)
	e.Model = SyntheticModel
	e.Usage = &event.Usage{InputTokens: 900000, OutputTokens: 900000, MessageID: "m", RequestID: "r"}

	if w := postItems(t, h, []spool.Item{mkItem(t, e)}); w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	got := st.snapshot("sess-1")
	if got.Input != 0 || got.Output != 0 || got.CostUSD != 0 {
		t.Fatalf("synthetic usage reached the totals: %+v", got)
	}
}

// TestConcurrentRedelivery exercises the same batch arriving twice at once,
// which is what a retry racing a slow original looks like.
func TestConcurrentRedelivery(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())
	batch := []spool.Item{mkItem(t, mkEvent("e1", 1, event.UserPrompt))}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodPost, EventsPath, bytes.NewReader(body))
			r.Header.Set("Authorization", "Bearer "+testToken)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Errorf("status %d", w.Code)
			}
		}()
	}
	wg.Wait()

	if st.count() != 1 {
		t.Fatalf("stored %d events, want 1", st.count())
	}
	if got := st.snapshot("sess-1"); got.UserTurns != 1 {
		t.Fatalf("user turns %d, want 1", got.UserTurns)
	}
}

// ---------------------------------------------------------------------------
// Per-item verdicts
// ---------------------------------------------------------------------------

// TestUnparseablePayloadIsRejectedNotAccepted covers a payload that survived the
// envelope but is not an event: a truncated record the agent stored as a string,
// a number, an array. An item like this can never become storable, so accepting
// it would tell the agent to delete something the server does not have.
func TestUnparseablePayloadIsRejectedNotAccepted(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	w := postItems(t, h, []spool.Item{
		rawItem("bad-1", "sess-1", `"{\"id\":\"bad-1\","`),
		rawItem("bad-2", "sess-1", `[1,2,3]`),
		rawItem("bad-3", "sess-1", `42`),
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w)
	if len(resp.Accepted) != 0 {
		t.Fatalf("accepted an unstorable item: %v", resp.Accepted)
	}
	if len(resp.Rejected) != 3 {
		t.Fatalf("rejected %+v", resp.Rejected)
	}
	for _, r := range resp.Rejected {
		if r.Reason != ReasonBadPayload {
			t.Fatalf("reason %q, want %q", r.Reason, ReasonBadPayload)
		}
	}
	if st.count() != 0 {
		t.Fatalf("stored %d events", st.count())
	}
}

func TestCaptureVersionOutsideInt32IsRejected(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	// The store converts capture_version to int32 for its int4 column, so a
	// value the conversion would wrap must never reach it. The boundary
	// values are accepted; one past them, and a negative, are rejected per
	// item with their own reason, and nothing else in the batch is affected.
	// The out-of-range values are built through int64 arithmetic so the
	// file compiles where int is 32 bits wide (the conversion wraps to a
	// negative there, which is still out of range).
	atMax := mkEvent("at-max", 1, event.UserPrompt)
	atMax.CaptureVersion = math.MaxInt32
	zero := mkEvent("zero", 2, event.UserPrompt)
	zero.CaptureVersion = 0
	over := mkEvent("over", 3, event.UserPrompt)
	over.CaptureVersion = int(int64(math.MaxInt32) + 1)
	negative := mkEvent("negative", 4, event.UserPrompt)
	negative.CaptureVersion = -1

	w := postItems(t, h, []spool.Item{
		mkItem(t, atMax), mkItem(t, over), mkItem(t, zero), mkItem(t, negative),
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w)

	wantAccepted := []string{"at-max", "zero"}
	if strings.Join(resp.Accepted, ",") != strings.Join(wantAccepted, ",") {
		t.Fatalf("accepted %v, want %v", resp.Accepted, wantAccepted)
	}
	want := []Reject{
		{ID: "over", Reason: ReasonCaptureVersion},
		{ID: "negative", Reason: ReasonCaptureVersion},
	}
	if len(resp.Rejected) != len(want) {
		t.Fatalf("rejected %+v, want %+v", resp.Rejected, want)
	}
	for i, r := range resp.Rejected {
		if r != want[i] {
			t.Fatalf("rejected[%d] = %+v, want %+v", i, r, want[i])
		}
	}
	if st.count() != 2 {
		t.Fatalf("stored %d events, want 2", st.count())
	}
}

func TestMixedBatchGetsOneVerdictPerItem(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	good := mkItem(t, mkEvent("good", 1, event.UserPrompt))
	noSession := mkItem(t, mkEvent("no-session", 2, event.ToolCall))
	noSession.SessionID = ""

	// An event that parses but cannot pass validation: no occurred_at, which the
	// whole system's separation of event time from ingest time depends on.
	invalid := mkEvent("invalid", 3, event.ToolCall)
	invalid.OccurredAt = time.Time{}

	alsoGood := mkItem(t, mkEvent("good-2", 4, event.ToolCall))

	items := []spool.Item{
		good,
		rawItem("", "sess-1", `{"id":"x"}`),
		noSession,
		rawItem("nul", "sess-1", `null`),
		mkItem(t, invalid),
		alsoGood,
	}

	w := postItems(t, h, items)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w)

	wantAccepted := []string{"good", "good-2"}
	if strings.Join(resp.Accepted, ",") != strings.Join(wantAccepted, ",") {
		t.Fatalf("accepted %v, want %v", resp.Accepted, wantAccepted)
	}
	want := []Reject{
		{ID: "", Reason: ReasonNoID},
		{ID: "no-session", Reason: ReasonNoSessionID},
		{ID: "nul", Reason: ReasonBadPayload},
		{ID: "invalid", Reason: ReasonInvalid},
	}
	if len(resp.Rejected) != len(want) {
		t.Fatalf("rejected %+v, want %+v", resp.Rejected, want)
	}
	for i, r := range resp.Rejected {
		if r != want[i] {
			t.Fatalf("rejected[%d] = %+v, want %+v", i, r, want[i])
		}
	}
	if st.count() != 2 {
		t.Fatalf("stored %d events, want 2", st.count())
	}
}

// TestStoreRejectionsReachTheItem checks that a permanent refusal decided by the
// storage layer, such as an attempt to write into somebody else's session, is
// relayed as a rejection for that item and nothing else in the batch suffers.
func TestStoreRejectionsReachTheItem(t *testing.T) {
	st := newMemStore(testRates)
	st.owners["sess-1"] = "someone-else@example.com"
	h := newHandler(t, st, goodDevices())

	mine := mkEvent("mine", 1, event.UserPrompt)
	mine.SessionID = "sess-2"

	w := postItems(t, h, []spool.Item{
		mkItem(t, mkEvent("theirs", 1, event.UserPrompt)),
		mkItem(t, mine),
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w)
	if len(resp.Accepted) != 1 || resp.Accepted[0] != "mine" {
		t.Fatalf("accepted %v", resp.Accepted)
	}
	if len(resp.Rejected) != 1 || resp.Rejected[0].ID != "theirs" {
		t.Fatalf("rejected %+v", resp.Rejected)
	}
}

// silentStore answers a batch without mentioning any of it, which is what a
// storage bug looks like from here.
type silentStore struct{}

func (silentStore) UpsertEvents(context.Context, []Record) (Result, error) { return Result{}, nil }

func (silentStore) PutHealthReport(context.Context, string, string, health.Report) error { return nil }

// TestUnconfirmedItemsAreNotAcknowledged is the conservative half of the
// per-item contract. An item the store did not confirm is claimed neither way,
// so the agent keeps it and advances its own attempt counter. Acknowledging it
// on the strength of a 200 is how a client deletes work the server never wrote.
func TestUnconfirmedItemsAreNotAcknowledged(t *testing.T) {
	h := newHandler(t, silentStore{}, goodDevices())

	w := postItems(t, h, []spool.Item{mkItem(t, mkEvent("e1", 1, event.UserPrompt))})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	resp := decodeResponse(t, w)
	if len(resp.Accepted) != 0 || len(resp.Rejected) != 0 {
		t.Fatalf("claimed an unconfirmed item: %+v", resp)
	}
}

// TestAcknowledgementUsesTheItemID keeps the two idempotency keys straight. The
// agent frees a spool file by the item's own id; answering with the id inside
// the payload would leave every item pending forever if an agent ever set them
// independently.
func TestAcknowledgementUsesTheItemID(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	item := mkItem(t, mkEvent("event-id", 1, event.UserPrompt))
	item.ID = "item-id"

	w := postItems(t, h, []spool.Item{item})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w)
	if len(resp.Accepted) != 1 || resp.Accepted[0] != "item-id" {
		t.Fatalf("accepted %v, want the item id", resp.Accepted)
	}
	if _, ok := st.stored("event-id"); !ok {
		t.Fatal("the event was stored under the wrong key")
	}
}

func TestEmptyBatchIsAccepted(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	w := post(t, h, EventsPath, testToken, []byte(`[]`))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	// The field must be present rather than null: a client decoding into a typed
	// response should see an empty list, not a missing one.
	if body := strings.TrimSpace(w.Body.String()); body != `{"accepted":[]}` {
		t.Fatalf("body %s", body)
	}
	if st.upserts != 0 {
		t.Fatal("empty batch reached the store")
	}
}

// ---------------------------------------------------------------------------
// Size limits, per contract section 4a
// ---------------------------------------------------------------------------

// TestOverCapBatchIsRejectedPerItem is the behaviour a 413 would break. The
// agent retries transport errors forever and sends an over-limit item alone, so
// a transport-level refusal here is a request that can never succeed and never
// stops being made. A per-item rejection turns it into a quarantine.
func TestOverCapBatchIsRejectedPerItem(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.MaxBatchBytes = 512 })

	big := mkEvent("big", 1, event.UserPrompt)
	big.Text = strings.Repeat("x", 2000)
	small := mkEvent("small", 2, event.ToolCall)

	w := postItems(t, h, []spool.Item{mkItem(t, big), mkItem(t, small)})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w)
	if len(resp.Accepted) != 0 {
		t.Fatalf("accepted %v from an over-cap batch", resp.Accepted)
	}
	if len(resp.Rejected) != 2 {
		t.Fatalf("rejected %+v, want one verdict per item", resp.Rejected)
	}
	for _, r := range resp.Rejected {
		if r.Reason != ReasonBatchTooBig {
			t.Fatalf("reason %q, want %q", r.Reason, ReasonBatchTooBig)
		}
	}
	if st.count() != 0 {
		t.Fatalf("stored %d events from an over-cap batch", st.count())
	}
}

// TestBodyOverHardLimitFailsTheRequest checks the second limit: past it there
// are no items to answer about, because they were never read.
func TestBodyOverHardLimitFailsTheRequest(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices(), func(o *Options) {
		o.MaxBatchBytes = 512
		o.HardBodyLimit = 4096
	})

	e := mkEvent("huge", 1, event.UserPrompt)
	e.Text = strings.Repeat("x", 8192)
	body, err := json.Marshal([]spool.Item{mkItem(t, e)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	w := post(t, h, EventsPath, testToken, body)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413; body %s", w.Code, w.Body)
	}
	if st.count() != 0 {
		t.Fatalf("stored %d events", st.count())
	}
}

// TestCollapsedLimitsAreRefused guards the configuration that silently defeats
// the policy: with one limit, reading stops at exactly the size the policy needs
// to read past, so an over-cap batch becomes a parse failure instead of a
// per-item answer.
func TestCollapsedLimitsAreRefused(t *testing.T) {
	_, err := New(Options{
		Store:         newMemStore(nil),
		Devices:       goodDevices(),
		MaxBatchBytes: 4096,
		HardBodyLimit: 4096,
	})
	if err == nil {
		t.Fatal("New accepted equal size limits")
	}
}

func TestMalformedBodyIsFourHundred(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	w := post(t, h, EventsPath, testToken, []byte(`{"not":"an array"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400; body %s", w.Code, w.Body)
	}
}

// ---------------------------------------------------------------------------
// Transient failures, per contract section 4b
// ---------------------------------------------------------------------------

// TestStoreFailureIsFiveHundred is the rule that keeps a database blip from
// becoming permanent data loss. The agent quarantines what it is told was
// rejected, so a transient failure reported per item is deleted work.
func TestStoreFailureIsFiveHundred(t *testing.T) {
	st := newMemStore(testRates)
	st.upsertErr = errors.New("timeout: context deadline exceeded")
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.RetryAfter = 30 * time.Second })

	w := postItems(t, h, []spool.Item{mkItem(t, mkEvent("e1", 1, event.UserPrompt))})
	if w.Code < 500 {
		t.Fatalf("status %d, want 5xx; body %s", w.Code, w.Body)
	}
	resp := decodeResponse(t, w)
	if len(resp.Accepted) != 0 || len(resp.Rejected) != 0 {
		t.Fatalf("a failed transaction reported verdicts: %+v", resp)
	}
	if resp.RetryAfterMS != 30000 {
		t.Fatalf("retry_after_ms %d, want 30000", resp.RetryAfterMS)
	}
	if got := w.Header().Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After header %q", got)
	}
}

// TestStoreFailureSuppressesLocalRejections is the subtler half of the same
// rule. The batch had an item this server had already judged unstorable, but the
// transaction covering the rest failed, and answering with that one rejection
// would quarantine it on the strength of a request that did not complete.
func TestStoreFailureSuppressesLocalRejections(t *testing.T) {
	st := newMemStore(testRates)
	st.upsertErr = errors.New("pool exhausted")
	h := newHandler(t, st, goodDevices())

	w := postItems(t, h, []spool.Item{
		rawItem("bad", "sess-1", `"truncated"`),
		mkItem(t, mkEvent("good", 1, event.UserPrompt)),
	})
	if w.Code < 500 {
		t.Fatalf("status %d, want 5xx", w.Code)
	}
	if resp := decodeResponse(t, w); len(resp.Rejected) != 0 {
		t.Fatalf("rejected %+v alongside a failed transaction", resp.Rejected)
	}
}

// ---------------------------------------------------------------------------
// Event time
// ---------------------------------------------------------------------------

// TestOccurredAtPreservedExactly protects the separation the whole product rests
// on: a backfilled session must be indistinguishable from a live one, which it
// is not if ingest rounds, re-zones or restamps the moment something happened.
func TestOccurredAtPreservedExactly(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	e := mkEvent("e1", 0, event.UserPrompt)
	e.OccurredAt = fixedTime
	item := mkItem(t, e)

	if w := postItems(t, h, []spool.Item{item}); w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	rec, ok := st.stored("e1")
	if !ok {
		t.Fatal("event not stored")
	}
	if !rec.Event.OccurredAt.Equal(fixedTime) {
		t.Fatalf("occurred_at %s, want %s", rec.Event.OccurredAt, fixedTime)
	}
	if _, off := rec.Event.OccurredAt.Zone(); off != 5*3600+30*60 {
		t.Fatalf("zone offset %d, want the delivered +05:30", off)
	}
	// The stored bytes are the delivered bytes when nothing needed redacting, so
	// the timestamp is not even re-rendered.
	if !bytes.Equal(rec.Body, item.Payload) {
		t.Fatalf("body was rewritten:\n got %s\nwant %s", rec.Body, item.Payload)
	}
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

func TestMissingCredentialIsUnauthorized(t *testing.T) {
	h := newHandler(t, newMemStore(nil), goodDevices())
	if w := post(t, h, EventsPath, "", []byte(`[]`)); w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
}

func TestUnknownCredentialIsUnauthorized(t *testing.T) {
	st := newMemStore(nil)
	h := newHandler(t, st, goodDevices())
	w := post(t, h, EventsPath, "lsd_nope", []byte(`[]`))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
	if st.upserts != 0 {
		t.Fatal("an unauthenticated request reached the store")
	}
}

// TestCredentialLookupFailureIsFiveHundred keeps a database blip from telling a
// working laptop that its credential is bad, which would send it to re-enrol.
func TestCredentialLookupFailureIsFiveHundred(t *testing.T) {
	h := newHandler(t, newMemStore(nil), fakeDevices{err: errors.New("connection refused")})
	w := post(t, h, EventsPath, testToken, []byte(`[]`))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503; body %s", w.Code, w.Body)
	}
}

func TestIdentityWithoutEmailIsRefused(t *testing.T) {
	devices := fakeDevices{tokens: map[string]Identity{testToken: {DeviceID: testDevice}}}
	h := newHandler(t, newMemStore(nil), devices)
	if w := post(t, h, EventsPath, testToken, []byte(`[]`)); w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
}

// TestAttributionComesFromTheCredential checks that a payload cannot file events
// under somebody else by claiming to.
func TestAttributionComesFromTheCredential(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	// The event type carries no email field, so the attempt is made the only way
	// it can be: an unknown field in the payload, which must simply be ignored.
	item := rawItem("e1", "sess-1", `{"id":"e1","source":"claude_code","origin":"hook",`+
		`"type":"user_prompt","session_id":"sess-1","seq":1,`+
		`"occurred_at":"2026-03-01T04:05:06Z","email":"boss@example.com"}`)

	if w := postItems(t, h, []spool.Item{item}); w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body)
	}
	rec, ok := st.stored("e1")
	if !ok {
		t.Fatal("event not stored")
	}
	if rec.Email != testEmail {
		t.Fatalf("attributed to %q, want %q", rec.Email, testEmail)
	}
	if rec.DeviceID != testDevice {
		t.Fatalf("device %q, want %q", rec.DeviceID, testDevice)
	}
}

func TestWrongMethodIsRefused(t *testing.T) {
	h := newHandler(t, newMemStore(nil), goodDevices())
	r := httptest.NewRequest(http.MethodGet, EventsPath, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status %d, want 405", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func mkReport(at time.Time) health.Report {
	return health.Report{
		SchemaVersion: health.SchemaVersion,
		AgentVersion:  "1.2.3",
		Hostname:      "laptop",
		EmittedAt:     at,
		Spool:         health.SpoolHealth{Pending: 3},
	}
}

func postReport(t *testing.T, h *Handler, r health.Report) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	return post(t, h, HealthPath, testToken, body)
}

func TestHealthReportIsAccepted(t *testing.T) {
	st := newMemStore(nil)
	h := newHandler(t, st, goodDevices())

	w := postReport(t, h, mkReport(fixedTime))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202; body %s", w.Code, w.Body)
	}
	if st.reportCount() != 1 {
		t.Fatalf("stored %d reports", st.reportCount())
	}
	if st.reports[0].Email != testEmail || st.reports[0].DeviceID != testDevice {
		t.Fatalf("stored %+v", st.reports[0])
	}
	if !st.reports[0].Report.EmittedAt.Equal(fixedTime) {
		t.Fatalf("emitted_at %s, want %s", st.reports[0].Report.EmittedAt, fixedTime)
	}
}

// TestHealthRedeliveryIsNotASecondMachine covers the reason the schema carries a
// unique index on (email, device, emitted_at): health is delivered at-least-once
// like everything else, and a retried sample would otherwise inflate coverage.
func TestHealthRedeliveryIsNotASecondMachine(t *testing.T) {
	st := newMemStore(nil)
	h := newHandler(t, st, goodDevices())
	report := mkReport(fixedTime)

	for range 3 {
		if w := postReport(t, h, report); w.Code != http.StatusAccepted {
			t.Fatalf("status %d", w.Code)
		}
	}
	if st.reportCount() != 1 {
		t.Fatalf("stored %d reports for one sample", st.reportCount())
	}
}

func TestHealthStoreFailureIsFiveHundred(t *testing.T) {
	st := newMemStore(nil)
	st.healthErr = errors.New("deadlock detected")
	h := newHandler(t, st, goodDevices())

	if w := postReport(t, h, mkReport(fixedTime)); w.Code < 500 {
		t.Fatalf("status %d, want 5xx; body %s", w.Code, w.Body)
	}
}

func TestHealthMalformedBodyIsFourHundred(t *testing.T) {
	h := newHandler(t, newMemStore(nil), goodDevices())
	if w := post(t, h, HealthPath, testToken, []byte(`{`)); w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", w.Code)
	}
}

func TestHealthRequiresACredential(t *testing.T) {
	h := newHandler(t, newMemStore(nil), goodDevices())
	if w := post(t, h, HealthPath, "", []byte(`{}`)); w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", w.Code)
	}
}

// TestHealthErrorTextIsScrubbed applies the same rule to telemetry as to
// transcripts: the delivery error a report carries is whatever the last request
// failed with, and a request URL is a place credentials appear.
func TestHealthErrorTextIsScrubbed(t *testing.T) {
	st := newMemStore(nil)
	h := newHandler(t, st, goodDevices())

	report := mkReport(fixedTime)
	report.LastError = `Post "https://ingest.example.com/v1/events": 401 with Authorization: Bearer QUJDRA1234567890abcdefXYZ=`

	if w := postReport(t, h, report); w.Code != http.StatusAccepted {
		t.Fatalf("status %d", w.Code)
	}
	stored := st.reports[0].Report.LastError
	if strings.Contains(stored, "QUJDRA1234567890abcdefXYZ") {
		t.Fatalf("bearer token survived into storage: %s", stored)
	}
	if !strings.Contains(stored, "[REDACTED:auth_bearer]") {
		t.Fatalf("no redaction marker in %q", stored)
	}
}

// ---------------------------------------------------------------------------
// Repository resolution
// ---------------------------------------------------------------------------

func TestRepoFromCwd(t *testing.T) {
	cases := map[string]string{
		"/home/dev/src/loop-sessions": "loop-sessions",
		"/home/dev/src/backend.git":   "backend",
		`C:\Users\dev\src\backend`:    "backend",
		"":                            "",
		".":                           "",
	}
	for in, want := range cases {
		if got := RepoFromCwd(in); got != want {
			t.Errorf("RepoFromCwd(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRepoIsDerivedForTheStore(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices())

	if w := postItems(t, h, []spool.Item{mkItem(t, mkEvent("e1", 1, event.UserPrompt))}); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	rec, _ := st.stored("e1")
	if rec.Repo != "loop-sessions" {
		t.Fatalf("repo %q", rec.Repo)
	}
}

func TestRepoResolverIsInjectable(t *testing.T) {
	st := newMemStore(testRates)
	h := newHandler(t, st, goodDevices(), func(o *Options) {
		o.Repo = func(cwd string) string { return "fixed/" + cwd }
	})
	if w := postItems(t, h, []spool.Item{mkItem(t, mkEvent("e1", 1, event.UserPrompt))}); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	rec, _ := st.stored("e1")
	if rec.Repo != "fixed//home/dev/src/loop-sessions" {
		t.Fatalf("repo %q", rec.Repo)
	}
}
