package skillusage

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/ingest"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// The properties this package is responsible for, observed without a
// database: which credential port a bearer reaches and that the other is
// never called, which 4b rule a body fails and the field it names, what
// the row the store is handed carries and what it never carries, which
// status a store answer becomes, and what the lines say. The fakes stand
// in for the store and the two credential ports with call counters, since
// "one verify per request" and "no lookup for a junk bearer" are counts.

const (
	deviceToken   = "lsd_device-token-for-tests"
	deviceEmail   = "dev@example.com"
	deviceID      = "11111111-1111-4111-8111-111111111111"
	sourceToken   = "lss_source-token-for-tests"
	sourceTokenID = "22222222-2222-4222-8222-222222222222"
	testNowRFC    = "2026-09-21T12:00:00Z"
)

var testNow = time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)

// lockWaitErr carries the sqlstate the store reads for a lock wait.
type lockWaitErr struct{ code string }

func (e lockWaitErr) Error() string    { return "lock wait " + e.code }
func (e lockWaitErr) SQLState() string { return e.code }

type fakeDevices struct {
	mu     sync.Mutex
	tokens map[string]ingest.Identity
	err    error
	calls  int
}

func (f *fakeDevices) Verify(_ context.Context, presented string) (ingest.Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return ingest.Identity{}, f.err
	}
	id, ok := f.tokens[presented]
	if !ok {
		return ingest.Identity{}, fmt.Errorf("token not recognised: %w", ingest.ErrUnauthenticated)
	}
	return id, nil
}

func goodDevices() *fakeDevices {
	return &fakeDevices{tokens: map[string]ingest.Identity{deviceToken: {Email: deviceEmail, DeviceID: deviceID}}}
}

// fakeStore is the store port: the verify, which commits on its own, and
// the transactional half. Tokens are keyed by the hex of their hash, as
// the route presents them, so a test never hands the fake a plaintext the
// handler could have logged.
type fakeStore struct {
	mu        sync.Mutex
	tokens    map[string]store.SourceToken
	authErr   error
	authCalls int
	// authInTx counts verifies made on a context a transaction handed
	// out: the touch must commit before the row's transaction opens, or
	// every post under a token serialises on its row for a whole request.
	authInTx  int
	owners    map[string]string
	known     map[string]bool
	knownErr  error
	upsertErr error
	upsertOut store.UpsertOutcome
	rows      []store.SkillRow
	txCalls   int
	// block, when set, holds every transaction open until it is closed
	// (or, with holdFirst, only the first), signalling entered first; for
	// the semaphore and the concurrency tests.
	block     chan struct{}
	entered   chan struct{}
	holdFirst bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{tokens: map[string]store.SourceToken{}, owners: map[string]string{}, known: map[string]bool{},
		upsertOut: store.UpsertOutcome{Inserted: true}}
}

func (f *fakeStore) addToken(plaintext string, tok store.SourceToken) {
	f.tokens[hex.EncodeToString(auth.HashToken(plaintext))] = tok
}

// inTxKey marks the context a fake transaction runs fn under, so the
// verify can tell whether it was called inside one.
type inTxKey struct{}

func (f *fakeStore) AuthenticateSource(ctx context.Context, hash []byte, scope string) (store.SourceToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authCalls++
	if ctx.Value(inTxKey{}) != nil {
		f.authInTx++
	}
	if f.authErr != nil {
		return store.SourceToken{}, f.authErr
	}
	tok, ok := f.tokens[hex.EncodeToString(hash)]
	if !ok {
		return store.SourceToken{State: store.TokenStateNone}, nil
	}
	if tok.State == "" {
		tok.State = store.TokenStateLive
	}
	if tok.State == store.TokenStateLive && tok.Scope != scope {
		tok.State = store.TokenStateWrongScope
	}
	if tok.State == store.TokenStateLive {
		tok.WindowCount++
		f.tokens[hex.EncodeToString(hash)] = tok
	}
	return tok, nil
}

func (f *fakeStore) InTx(ctx context.Context, fn func(context.Context, Tx) error) error {
	f.mu.Lock()
	f.txCalls++
	ordinal := f.txCalls
	f.mu.Unlock()
	if f.block != nil && (!f.holdFirst || ordinal == 1) {
		f.entered <- struct{}{}
		<-f.block
	}
	return fn(context.WithValue(ctx, inTxKey{}, true), fakeTx{f})
}

type fakeTx struct{ s *fakeStore }

func (t fakeTx) SessionOwner(_ context.Context, ref string) (string, bool, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	e, ok := t.s.owners[ref]
	return e, ok, nil
}

func (t fakeTx) ActorKnown(_ context.Context, email string) (bool, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	return t.s.known[email], t.s.knownErr
}

func (t fakeTx) Upsert(_ context.Context, row store.SkillRow) (store.UpsertOutcome, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	if t.s.upsertErr != nil {
		return store.UpsertOutcome{}, t.s.upsertErr
	}
	t.s.rows = append(t.s.rows, row)
	return t.s.upsertOut, nil
}

func (f *fakeStore) lastRow(t *testing.T) store.SkillRow {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.rows) == 0 {
		t.Fatal("no row reached the store")
	}
	return f.rows[len(f.rows)-1]
}

func (f *fakeStore) counts() (auth, tx, rows int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.authCalls, f.txCalls, len(f.rows)
}

// verifiedInsideATransaction reports whether any verify ran on a
// transaction's context, which no request may do.
func (f *fakeStore) verifiedInsideATransaction() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.authInTx > 0
}

// beaconToken is the shared Devin token of design 4c.
func beaconToken() store.SourceToken {
	return store.SourceToken{ID: sourceTokenID, Platform: store.PlatformDevin, Environment: "default",
		Scope: Scope, RateLimitPerMin: 1200, AllowedOrigins: []string{store.OriginBeacon, store.OriginReconciler}}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func jsonLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

func newHandler(t *testing.T, st Store, dv Devices, tweak ...func(*Options)) *Handler {
	t.Helper()
	o := Options{Store: st, Devices: dv, Logger: discardLogger(), Now: func() time.Time { return testNow }}
	for _, f := range tweak {
		f(&o)
	}
	h, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return h
}

func body(m map[string]any) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}

// beacon is the smallest body a beacon posts.
func beacon() map[string]any {
	return map[string]any{"origin": "beacon", "skill": "engg:git", "idempotency_key": "k-1"}
}

// hook is the smallest body the hook posts under a device token.
func hook() map[string]any {
	return map[string]any{"origin": "hook", "agent_platform": "claude_code", "skill": "engg:git",
		"session_ref": "sess-1", "tool_use_id": "toolu_1", "trigger": "agent"}
}

func post(t *testing.T, h *Handler, bearer string, b []byte, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, InvocationsPath, bytes.NewReader(b))
	r.RemoteAddr = "10.0.0.1:12345"
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

type errBody struct {
	Error string `json:"error"`
	Field string `json:"field"`
}

func decodeErr(t *testing.T, w *httptest.ResponseRecorder) errBody {
	t.Helper()
	var e errBody
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return e
}

func wantStatus(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status = %d, want %d: %s", w.Code, status, w.Body.String())
	}
}

func wantRejected(t *testing.T, w *httptest.ResponseRecorder, status int, code, field string) {
	t.Helper()
	wantStatus(t, w, status)
	e := decodeErr(t, w)
	if e.Error != code || e.Field != field {
		t.Fatalf("body = %+v, want error %q field %q", e, code, field)
	}
}

func wantDuplicate(t *testing.T, w *httptest.ResponseRecorder, dup bool) {
	t.Helper()
	wantStatus(t, w, http.StatusOK)
	var resp response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	if resp.Duplicate != dup {
		t.Fatalf("duplicate = %v, want %v", resp.Duplicate, dup)
	}
	if strings.Contains(w.Body.String(), `"id"`) {
		t.Fatalf("the response carries an id: %s", w.Body.String())
	}
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %v (%s)", err, line)
		}
		out = append(out, entry)
	}
	return out
}

func linesNamed(lines []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Auth (criterion d)
// ---------------------------------------------------------------------------

func TestPrefixGateRefusesWithoutAnyLookup(t *testing.T) {
	st := newFakeStore()
	dv := goodDevices()
	log, buf := jsonLogger()
	h := newHandler(t, st, dv, func(o *Options) { o.Logger = log })

	for _, c := range []struct{ name, bearer string }{
		{"no bearer", ""}, {"another prefix", "abc_" + strings.Repeat("x", 40)}, {"a cookie alone", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			var w *httptest.ResponseRecorder
			if c.name == "a cookie alone" {
				w = post(t, h, "", body(beacon()), "Cookie", "__Host-loop_session=abc")
			} else {
				w = post(t, h, c.bearer, body(beacon()))
			}
			wantRejected(t, w, http.StatusUnauthorized, "unauthenticated", "")
		})
	}
	if a, tx, _ := st.counts(); a != 0 || tx != 0 || dv.calls != 0 {
		t.Errorf("a refused prefix reached a port: source calls %d, transactions %d, device calls %d", a, tx, dv.calls)
	}
	for _, l := range linesNamed(logLines(t, buf), lineRejected) {
		if l["token_shape"] != shapeUnknown || l["reason"] != reasonUnauthenticated || l["level"] != "WARN" {
			t.Errorf("rejected line = %v", l)
		}
	}
}

func TestEachPrefixReachesItsOwnPortAndSetsTrust(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	dv := goodDevices()
	h := newHandler(t, st, dv)

	wantDuplicate(t, post(t, h, deviceToken, body(hook())), false)
	if a, _, _ := st.counts(); a != 0 || dv.calls != 1 {
		t.Fatalf("a device bearer: source calls %d, device calls %d", a, dv.calls)
	}
	row := st.lastRow(t)
	if row.Trust != store.TrustDevice || row.SourceTokenID != nil || row.DeviceID == nil || *row.DeviceID != deviceID ||
		row.ActorEmail == nil || *row.ActorEmail != deviceEmail || !row.ActorKnown {
		t.Errorf("device row = %+v", row)
	}

	wantDuplicate(t, post(t, h, sourceToken, body(beacon())), false)
	if a, _, _ := st.counts(); a != 1 || dv.calls != 1 {
		t.Fatalf("a source bearer: source calls %d, device calls %d", a, dv.calls)
	}
	if st.verifiedInsideATransaction() {
		t.Error("the verify ran inside the row's transaction; its touch must commit on its own first")
	}
	row = st.lastRow(t)
	if row.Trust != store.TrustClaimed || row.DeviceID != nil || row.SourceTokenID == nil || *row.SourceTokenID != sourceTokenID ||
		row.ActorEmail != nil || row.ActorKnown {
		t.Errorf("source row = %+v", row)
	}

	// A device cannot yield claimed and a source cannot yield device: the
	// body has no say. A trust key is an unknown field.
	b := hook()
	b["trust"] = "claimed"
	wantRejected(t, post(t, h, deviceToken, body(b)), http.StatusBadRequest, "invalid_payload", "trust")
}

func TestARevokedDeviceAndADeadTokenAre401WithTheirState(t *testing.T) {
	st := newFakeStore()
	revoked := beaconToken()
	at := testNow.Add(-time.Hour)
	revoked.RevokedAt, revoked.State = &at, store.TokenStateRevoked
	st.addToken("lss_revoked", revoked)
	expired := beaconToken()
	expired.State = store.TokenStateExpired
	st.addToken("lss_expired", expired)
	catalog := beaconToken()
	catalog.Scope = store.ScopeSkillCatalog
	st.addToken("lss_catalog", catalog)
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	wantRejected(t, post(t, h, "lsd_revoked-device", body(hook())), http.StatusUnauthorized, "unauthenticated", "")
	for _, c := range []struct{ bearer, state string }{
		{"lss_revoked", store.TokenStateRevoked}, {"lss_expired", store.TokenStateExpired},
		{"lss_catalog", store.TokenStateWrongScope}, {"lss_unknown", store.TokenStateNone},
	} {
		b := beacon()
		b["agent_platform"] = "not-a-platform-label"
		wantRejected(t, post(t, h, c.bearer, body(b)), http.StatusUnauthorized, "unauthenticated", "")
	}
	if a, _, rows := st.counts(); a != 4 || rows != 0 {
		t.Errorf("source calls %d (want one per lss_ request), rows %d", a, rows)
	}
	lines := linesNamed(logLines(t, buf), lineRejected)
	if len(lines) != 5 {
		t.Fatalf("got %d rejected lines, want 5", len(lines))
	}
	if lines[0]["token_shape"] != shapeDevice || lines[0]["token_state"] != store.TokenStateNone {
		t.Errorf("device line = %v", lines[0])
	}
	for i, state := range []string{store.TokenStateRevoked, store.TokenStateExpired, store.TokenStateWrongScope, store.TokenStateNone} {
		l := lines[i+1]
		if l["token_shape"] != shapeSource || l["token_state"] != state {
			t.Errorf("line %d = %v, want state %s", i+1, l, state)
		}
		wantID, wantPlatform := sourceTokenID, store.PlatformDevin
		if state == store.TokenStateNone {
			wantID, wantPlatform = "", ""
		}
		// The platform is the token row's, never the body's: the junk
		// agent_platform above must not become a label.
		if l["source_token_id"] != wantID || l["platform"] != wantPlatform {
			t.Errorf("line %d carries id %v platform %v, want %q %q", i+1, l["source_token_id"], l["platform"], wantID, wantPlatform)
		}
	}
}

// ---------------------------------------------------------------------------
// Body and fields (criterion e)
// ---------------------------------------------------------------------------

func TestBodyBoundsAndUnknownKeys(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	big := beacon()
	big["harness_version"] = strings.Repeat("x", 5*1024)
	w := post(t, h, sourceToken, body(big))
	wantRejected(t, w, http.StatusRequestEntityTooLarge, "body_too_large", "")

	withArgs := beacon()
	withArgs["args"] = "customer acme jane@example.net"
	wantRejected(t, post(t, h, sourceToken, body(withArgs)), http.StatusBadRequest, "invalid_payload", "args")

	wantRejected(t, post(t, h, sourceToken, []byte(`{"origin": "beacon"`)), http.StatusBadRequest, "invalid_payload", "body")
	wantRejected(t, post(t, h, sourceToken, []byte(`{"origin": "beacon", "skill": "x", "idempotency_key": "k"} {}`)), http.StatusBadRequest, "invalid_payload", "body")
	wantRejected(t, post(t, h, sourceToken, []byte(`{"origin": 7}`)), http.StatusBadRequest, "invalid_payload", "origin")
	wantRejected(t, post(t, h, sourceToken, nil), http.StatusBadRequest, "invalid_payload", "body")

	if a, _, _ := st.counts(); a != 0 {
		t.Errorf("a body refusal reached the verify %d times; the body is read first", a)
	}
	for _, l := range linesNamed(logLines(t, buf), lineRejected) {
		if l["reason"] != reasonBodyTooLarge && l["reason"] != reasonInvalidPayload {
			t.Errorf("rejected line reason = %v", l["reason"])
		}
	}
}

// TestEvery4bRuleNamesItsField is the design 4b table: one row per rule,
// each answering 400 invalid_payload with the field.
func TestEvery4bRuleNamesItsField(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	laptop := store.SourceToken{ID: "33333333-3333-4333-8333-333333333333", Platform: store.PlatformClaudeCode, Environment: "laptop",
		Scope: Scope, RateLimitPerMin: 1200, AllowedOrigins: []string{store.OriginHook}, BoundActorEmail: ptr("dev@example.com")}
	st.addToken("lss_laptop", laptop)
	h := newHandler(t, st, goodDevices())

	with := func(base map[string]any, kv ...any) map[string]any {
		m := map[string]any{}
		for k, v := range base {
			m[k] = v
		}
		for i := 0; i+1 < len(kv); i += 2 {
			if kv[i+1] == nil {
				delete(m, kv[i].(string))
			} else {
				m[kv[i].(string)] = kv[i+1]
			}
		}
		return m
	}
	reconciler := map[string]any{"origin": "reconciler", "skill": "git", "idempotency_key": "r-1",
		"window_start": "2026-09-21T10:00:00Z", "window_end": "2026-09-21T11:00:00Z", "occurred_at": "2026-09-21T10:30:00Z"}
	cases := []struct {
		name   string
		bearer string
		body   map[string]any
		field  string
	}{
		{"origin missing", sourceToken, with(beacon(), "origin", nil), "origin"},
		{"origin off enum", sourceToken, with(beacon(), "origin", "webhook"), "origin"},
		{"origin derived under lss_", sourceToken, with(beacon(), "origin", "derived"), "origin"},
		{"origin derived under lsd_", deviceToken, with(hook(), "origin", "derived"), "origin"},
		{"origin outside allowed_origins", sourceToken, with(beacon(), "origin", "hook"), "origin"},
		{"device origin must be hook", deviceToken, with(hook(), "origin", "beacon"), "origin"},
		{"device agent_platform required", deviceToken, with(hook(), "agent_platform", nil), "agent_platform"},
		{"device agent_platform is claude_code or codex", deviceToken, with(hook(), "agent_platform", "devin"), "agent_platform"},
		{"source agent_platform differs from the token", sourceToken, with(beacon(), "agent_platform", "capy"), "agent_platform"},
		{"skill missing", sourceToken, with(beacon(), "skill", nil), "skill"},
		{"skill empty", sourceToken, with(beacon(), "skill", "  "), "skill"},
		{"trigger off enum", sourceToken, with(beacon(), "trigger", "cron"), "trigger"},
		{"outcome off enum", sourceToken, with(beacon(), "outcome", "done"), "outcome"},
		{"error_class off enum", sourceToken, with(beacon(), "outcome", "error", "error_class", "boom"), "error_class"},
		{"error_class without error", sourceToken, with(beacon(), "outcome", "success", "error_class", "timeout"), "error_class"},
		{"actor_email under lss_", sourceToken, with(beacon(), "actor_email", "dev@example.com"), "actor_email"},
		{"actor_email under a laptop token", "lss_laptop", with(hook(), "agent_platform", nil, "actor_email", "boss@example.com"), "actor_email"},
		{"repo off shape", sourceToken, with(beacon(), "repo", "a/b"), "repo"},
		{"session_ref off shape", sourceToken, with(beacon(), "session_ref", "has space"), "session_ref"},
		{"prompt_id off shape", sourceToken, with(beacon(), "session_ref", "s", "prompt_id", strings.Repeat("p", 65)), "prompt_id"},
		{"tool_use_id off shape", sourceToken, with(beacon(), "session_ref", "s", "tool_use_id", "t/1"), "tool_use_id"},
		{"tool_use_id without session_ref", sourceToken, with(beacon(), "tool_use_id", "toolu_1", "idempotency_key", nil), "session_ref"},
		{"prompt_id without session_ref", deviceToken, with(hook(), "tool_use_id", nil, "prompt_id", "p1", "session_ref", nil), "session_ref"},
		{"form 4: no id and no key", sourceToken, with(beacon(), "idempotency_key", nil), "idempotency_key"},
		{"idempotency_key off shape", sourceToken, with(beacon(), "idempotency_key", "k 1"), "idempotency_key"},
		{"link_ref outside reconciler", sourceToken, with(beacon(), "link_ref", "lsref-1"), "link_ref"},
		{"session_type outside reconciler", sourceToken, with(beacon(), "session_type", "user"), "session_type"},
		{"session_type off enum", sourceToken, with(reconciler, "session_type", "bot"), "session_type"},
		{"link_ref off shape", sourceToken, with(reconciler, "link_ref", "has space"), "link_ref"},
		{"window_start on a beacon", sourceToken, with(beacon(), "window_start", "2026-09-21T10:00:00Z"), "window_start"},
		{"reconciler without window_start", sourceToken, with(reconciler, "window_start", nil), "window_start"},
		{"reconciler without window_end", sourceToken, with(reconciler, "window_end", nil), "window_end"},
		{"reconciler window reversed", sourceToken, with(reconciler, "window_start", "2026-09-21T11:30:00Z"), "window_start"},
		{"reconciler window in the future", sourceToken, with(reconciler, "window_end", "2026-09-21T13:00:00Z", "occurred_at", "2026-09-21T10:30:00Z"), "window_end"},
		{"reconciler occurred_at outside its window", sourceToken, with(reconciler, "occurred_at", "2026-09-21T09:00:00Z"), "occurred_at"},
		{"reconciler without occurred_at", sourceToken, with(reconciler, "occurred_at", nil), "occurred_at"},
		{"occurred_at not RFC 3339", sourceToken, with(beacon(), "occurred_at", "yesterday"), "occurred_at"},
		{"args_bytes over the ceiling", sourceToken, with(beacon(), "args_bytes", 1_000_001), "args_bytes"},
		{"args_bytes negative", sourceToken, with(beacon(), "args_bytes", -1), "args_bytes"},
		{"harness_version off shape", sourceToken, with(beacon(), "harness_version", "v 1"), "harness_version"},
		{"an args key", deviceToken, with(hook(), "args", "x"), "args"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantRejected(t, post(t, h, c.bearer, body(c.body)), http.StatusBadRequest, "invalid_payload", c.field)
		})
	}
	if _, _, rows := st.counts(); rows != 0 {
		t.Errorf("%d rows reached the store from refused bodies", rows)
	}
}

func TestPlatformMismatchIsLoggedByItsOwnReason(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })
	b := beacon()
	b["agent_platform"] = "capy"
	wantRejected(t, post(t, h, sourceToken, body(b)), http.StatusBadRequest, "invalid_payload", "agent_platform")
	lines := linesNamed(logLines(t, buf), lineRejected)
	if len(lines) != 1 || lines[0]["reason"] != reasonPlatformMismatch || lines[0]["platform"] != store.PlatformDevin || lines[0]["status"] != float64(400) {
		t.Errorf("rejected line = %v", lines)
	}
}

func TestOffEnumSkillSourceAndOffShapeSkillAreStoredNotRefused(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	h := newHandler(t, st, goodDevices())

	b := beacon()
	b["skill_source"] = "marketplace"
	wantDuplicate(t, post(t, h, sourceToken, body(b)), false)
	if row := st.lastRow(t); row.SkillSource != store.SkillSourceUnknown || row.Plugin != "engg" || row.Skill == nil || *row.Skill != "git" || row.RawName != "engg:git" {
		t.Errorf("row = %+v", row)
	}

	b = beacon()
	b["skill"] = "value-summary acme jane@example.net"
	wantDuplicate(t, post(t, h, sourceToken, body(b)), false)
	if row := st.lastRow(t); row.RawName != store.OffShapeName || row.Skill != nil || row.Plugin != "" {
		t.Errorf("off-shape row = %+v", row)
	}
}

// ---------------------------------------------------------------------------
// Rate accounting and shedding (criteria b, c)
// ---------------------------------------------------------------------------

func TestTheRowCounterDecidesTheCap(t *testing.T) {
	st := newFakeStore()
	limited := beaconToken()
	limited.RateLimitPerMin, limited.WindowCount = 10, 9
	st.addToken("lss_limited", limited)
	soft := beaconToken()
	soft.ID, soft.RateLimitPerMin = "44444444-4444-4444-8444-444444444444", 0
	st.addToken("lss_soft", soft)
	flooded := beaconToken()
	flooded.RateLimitPerMin, flooded.WindowCount = 0, softRevokedCeiling
	st.addToken("lss_flooded", flooded)
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	// The fake touches the count on each verify: 9 becomes 10, within the
	// cap; 10 becomes 11, over it.
	wantDuplicate(t, post(t, h, "lss_limited", body(beacon())), false)
	w := post(t, h, "lss_limited", body(beacon()))
	wantRejected(t, w, http.StatusTooManyRequests, "rate_limited", "")
	if w.Header().Get("Retry-After") != "30" {
		t.Errorf("Retry-After = %q, want 30", w.Header().Get("Retry-After"))
	}

	// Limit 0: touched, nothing inserted, a duplicate, soft_revoked on
	// the line.
	_, _, before := st.counts()
	wantDuplicate(t, post(t, h, "lss_soft", body(beacon())), true)
	if _, _, after := st.counts(); after != before {
		t.Errorf("a soft-revoked token wrote a row")
	}
	accepted := linesNamed(logLines(t, buf), lineAccepted)
	if len(accepted) != 2 || accepted[1]["soft_revoked"] != true || accepted[1]["duplicate"] != true || accepted[1]["source_token_id"] != soft.ID {
		t.Errorf("accepted lines = %v", accepted)
	}

	// Limit 0 above the ceiling is still 429: the flood is bounded.
	w = post(t, h, "lss_flooded", body(beacon()))
	wantRejected(t, w, http.StatusTooManyRequests, "rate_limited", "")
	if w.Header().Get("Retry-After") != "30" {
		t.Errorf("Retry-After = %q, want 30", w.Header().Get("Retry-After"))
	}
	if l := linesNamed(logLines(t, buf), lineRejected); len(l) == 0 || l[len(l)-1]["platform"] != store.PlatformDevin || l[len(l)-1]["reason"] != reasonRateLimited {
		t.Errorf("rejected line = %v", l)
	}
}

// A lock wait on the upsert carries the token's platform, since the verify
// returned the row; one on the verify itself carries none, since the
// statement that would have read the row is the one that timed out.
func TestALockWaitOnEitherStatementIs429WithAOneSecondRetry(t *testing.T) {
	for _, code := range []string{"55P03", "40P01"} {
		st := newFakeStore()
		st.addToken(sourceToken, beaconToken())
		st.upsertErr = lockWaitErr{code}
		log, buf := jsonLogger()
		h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })
		w := post(t, h, sourceToken, body(beacon()))
		wantRejected(t, w, http.StatusTooManyRequests, "rate_limited", "")
		if w.Header().Get("Retry-After") != "1" {
			t.Errorf("Retry-After = %q, want 1", w.Header().Get("Retry-After"))
		}
		if l := linesNamed(logLines(t, buf), lineRejected); len(l) != 1 || l[0]["platform"] != store.PlatformDevin || l[0]["source_token_id"] != sourceTokenID {
			t.Errorf("upsert lock wait line = %v, want the token's platform and id", l)
		}
		w = post(t, h, deviceToken, body(hook()))
		wantRejected(t, w, http.StatusTooManyRequests, "rate_limited", "")

		st = newFakeStore()
		st.authErr = lockWaitErr{code}
		log, buf = jsonLogger()
		h = newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })
		w = post(t, h, sourceToken, body(beacon()))
		wantRejected(t, w, http.StatusTooManyRequests, "rate_limited", "")
		if w.Header().Get("Retry-After") != "1" {
			t.Errorf("verify lock wait: Retry-After = %q, want 1", w.Header().Get("Retry-After"))
		}
		if l := linesNamed(logLines(t, buf), lineRejected); len(l) != 1 || l[0]["platform"] != "" || l[0]["token_state"] != store.TokenStateNone || l[0]["reason"] != reasonRateLimited {
			t.Errorf("verify lock wait line = %v, want an empty platform", l)
		}
		if _, tx, _ := st.counts(); tx != 0 {
			t.Errorf("a verify that timed out opened %d row transactions", tx)
		}
	}
}

// TestTwoConcurrentPostsUnderOneTokenBothAnswer200 is the burst a shared
// token sees (parallel cloud-hook sessions, a reconciler with a pool):
// with the first post's row transaction still open, the second post's
// verify does not wait on it, because the touch committed on its own, so
// the second completes while the first is held and both are 200.
func TestTwoConcurrentPostsUnderOneTokenBothAnswer200(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	st.block, st.entered, st.holdFirst = make(chan struct{}), make(chan struct{}, 1), true
	h := newHandler(t, st, goodDevices())

	first := make(chan int, 1)
	go func() { first <- post(t, h, sourceToken, body(beacon()), "X-Forwarded-For", "10.0.0.1").Code }()
	select {
	case <-st.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first post did not reach its row transaction")
	}
	// The first post holds its transaction; the second runs to completion
	// under the same token.
	b := beacon()
	b["idempotency_key"] = "k-2"
	wantDuplicate(t, post(t, h, sourceToken, body(b), "X-Forwarded-For", "10.0.0.2"), false)
	if a, tx, rows := st.counts(); a != 2 || tx != 2 || rows != 1 {
		t.Errorf("with the first held: verifies %d, transactions %d, rows %d; want 2, 2, 1", a, tx, rows)
	}
	close(st.block)
	select {
	case code := <-first:
		if code != http.StatusOK {
			t.Errorf("the held post answered %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the held post never finished")
	}
	if st.verifiedInsideATransaction() {
		t.Error("a verify ran inside a row transaction")
	}
	if _, _, rows := st.counts(); rows != 2 {
		t.Errorf("rows = %d, want both", rows)
	}
}

func TestADeviceHasAnInProcessBucketOfSixtyPerMinute(t *testing.T) {
	st := newFakeStore()
	now := testNow
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Now = func() time.Time { return now } })
	for i := 0; i < 60; i++ {
		wantDuplicate(t, post(t, h, deviceToken, body(hook())), false)
	}
	wantRejected(t, post(t, h, deviceToken, body(hook())), http.StatusTooManyRequests, "rate_limited", "")
	now = now.Add(2 * time.Second)
	wantDuplicate(t, post(t, h, deviceToken, body(hook())), false)
	wantDuplicate(t, post(t, h, deviceToken, body(hook())), false)
	wantRejected(t, post(t, h, deviceToken, body(hook())), http.StatusTooManyRequests, "rate_limited", "")
}

func TestTheShedIsBoundedAndKeyedOnTheAddressBeforeTheVerify(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, goodDevices())
	for i := 0; i < 100_000; i++ {
		r := httptest.NewRequest(http.MethodPost, InvocationsPath, bytes.NewReader(body(beacon())))
		r.Header.Set("Authorization", "Bearer lss_junk-"+fmt.Sprint(i))
		// The front end appends the connection's address last; the fixed
		// first hop is what a client could write and must open no bucket.
		r.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.1, 10.%d.%d.%d", i>>16&255, i>>8&255, i&255))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("junk bearer %d: status %d", i, w.Code)
		}
	}
	if got := h.shed.size(); got != defaultShedBuckets {
		t.Errorf("shed holds %d buckets after 100k distinct addresses, want %d", got, defaultShedBuckets)
	}

	// One address over the allowance is refused before any verify, and
	// told to try again in a second, since the bucket refills within the
	// minute rather than at its end.
	st = newFakeStore()
	h = newHandler(t, st, goodDevices(), func(o *Options) { o.ShedPerMinute = 3 })
	for i := 0; i < 3; i++ {
		wantRejected(t, post(t, h, "lss_junk", body(beacon())), http.StatusUnauthorized, "unauthenticated", "")
	}
	w := post(t, h, "lss_junk", body(beacon()))
	wantRejected(t, w, http.StatusTooManyRequests, "rate_limited", "")
	if w.Header().Get("Retry-After") != "1" {
		t.Errorf("address shed: Retry-After = %q, want 1", w.Header().Get("Retry-After"))
	}
	if a, _, _ := st.counts(); a != 3 {
		t.Errorf("the shed let %d verifies through, want 3", a)
	}
}

func TestTheShedKeysOnTheHashOnlyOnceTheVerifyReturnedLive(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.ShedPerMinute = 2 })
	// Two addresses share one token: the address buckets allow two each,
	// the hash bucket allows two in all. The hash bucket's allowance is
	// the token's own, so this one keeps the cap's Retry-After.
	wantDuplicate(t, post(t, h, sourceToken, body(beacon()), "X-Forwarded-For", "10.0.0.1"), false)
	wantDuplicate(t, post(t, h, sourceToken, body(beacon()), "X-Forwarded-For", "10.0.0.2"), false)
	w := post(t, h, sourceToken, body(beacon()), "X-Forwarded-For", "10.0.0.3")
	wantRejected(t, w, http.StatusTooManyRequests, "rate_limited", "")
	if w.Header().Get("Retry-After") != "30" {
		t.Errorf("hash shed: Retry-After = %q, want 30", w.Header().Get("Retry-After"))
	}
	if h.shed.size() != 4 {
		t.Errorf("shed holds %d buckets, want three addresses and one hash", h.shed.size())
	}
}

func TestAFifthRequestWhileFourHoldConnectionsIs429(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	st.block, st.entered = make(chan struct{}), make(chan struct{}, 8)
	h := newHandler(t, st, goodDevices())

	var wg sync.WaitGroup
	results := make(chan int, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- post(t, h, sourceToken, body(beacon()), "X-Forwarded-For", fmt.Sprintf("10.0.0.%d", i)).Code
		}()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-st.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the four requests did not all reach the store")
		}
	}
	w := post(t, h, sourceToken, body(beacon()), "X-Forwarded-For", "10.0.0.9")
	wantRejected(t, w, http.StatusTooManyRequests, "rate_limited", "")
	// A full semaphore clears as soon as one of the four finishes, so the
	// header says a second, not the cap's thirty.
	if w.Header().Get("Retry-After") != "1" {
		t.Errorf("semaphore full: Retry-After = %q, want 1", w.Header().Get("Retry-After"))
	}
	if a, tx, _ := st.counts(); tx != 4 || a != 4 {
		t.Errorf("the fifth request reached the store: verifies %d, transactions %d, want 4 and 4", a, tx)
	}
	close(st.block)
	wg.Wait()
	close(results)
	for code := range results {
		if code != http.StatusOK {
			t.Errorf("a held request answered %d", code)
		}
	}
}

// ---------------------------------------------------------------------------
// Row assembly (criteria f, g, i)
// ---------------------------------------------------------------------------

func TestClaimedRowsCarryTheTokensPlatformEnvironmentAndActor(t *testing.T) {
	st := newFakeStore()
	tok := beaconToken()
	tok.Environment = "eu"
	st.addToken(sourceToken, tok)
	rotated := tok
	rotated.ID = "55555555-5555-4555-8555-555555555555"
	st.addToken("lss_rotated", rotated)
	laptop := store.SourceToken{ID: "33333333-3333-4333-8333-333333333333", Platform: store.PlatformClaudeCode, Environment: "laptop",
		Scope: Scope, RateLimitPerMin: 1200, AllowedOrigins: []string{store.OriginHook}, BoundActorEmail: ptr("dev@example.com")}
	st.addToken("lss_laptop", laptop)
	st.known["dev@example.com"] = true
	h := newHandler(t, st, goodDevices())

	rec := map[string]any{"origin": "reconciler", "skill": "git", "idempotency_key": "r-1", "session_ref": "devin-1", "link_ref": "lsref-1",
		"session_type": "automation", "window_start": "2026-09-21T10:00:00Z", "window_end": "2026-09-21T11:00:00Z", "occurred_at": "2026-09-21T10:30:00Z"}
	wantDuplicate(t, post(t, h, sourceToken, body(rec)), false)
	first := st.lastRow(t)
	if first.TokenPlatform != store.PlatformDevin || first.TokenEnvironment != "eu" || first.SessionType != "automation" ||
		first.LinkRef == nil || *first.LinkRef != "lsref-1" || !first.OccurredAt.Equal(time.Date(2026, 9, 21, 10, 30, 0, 0, time.UTC)) || first.TimeClamped {
		t.Errorf("reconciler row = %+v", first)
	}
	wantDuplicate(t, post(t, h, "lss_rotated", body(rec)), false)
	second := st.lastRow(t)
	k1, _ := store.DedupeKey(first)
	k2, _ := store.DedupeKey(second)
	if k1 != k2 || !strings.HasPrefix(k1, "k:devin:eu:") {
		t.Errorf("a re-post under a rotated token keys %q then %q", k1, k2)
	}

	// The laptop token: the bound actor, known through the roster.
	wantDuplicate(t, post(t, h, "lss_laptop", body(map[string]any{"origin": "hook", "skill": "engg:git", "session_ref": "s", "prompt_id": "p1", "trigger": "user"})), false)
	row := st.lastRow(t)
	if row.ActorEmail == nil || *row.ActorEmail != "dev@example.com" || !row.ActorKnown || row.AgentPlatform != store.PlatformClaudeCode || row.TokenEnvironment != "laptop" {
		t.Errorf("laptop row = %+v", row)
	}
	// The laptop token's first hook post is 200; a beacon post under it
	// is 400 on origin (C1).
	wantRejected(t, post(t, h, "lss_laptop", body(beacon())), http.StatusBadRequest, "invalid_payload", "origin")
}

func TestOccurredAtIsClampedOnBothBoundsUnderBothBearers(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })
	cases := []struct {
		name    string
		at      string
		clamped bool
	}{
		{"eight days back", testNow.Add(-8 * 24 * time.Hour).Format(time.RFC3339), true},
		{"six minutes ahead", testNow.Add(6 * time.Minute).Format(time.RFC3339), true},
		{"six days back", testNow.Add(-6 * 24 * time.Hour).Format(time.RFC3339), false},
		{"four minutes ahead", testNow.Add(4 * time.Minute).Format(time.RFC3339), false},
	}
	for _, c := range cases {
		for _, bearer := range []string{sourceToken, deviceToken} {
			t.Run(c.name+" under "+bearer[:4], func(t *testing.T) {
				b := beacon()
				if bearer == deviceToken {
					b = hook()
				}
				b["occurred_at"] = c.at
				wantDuplicate(t, post(t, h, bearer, body(b)), false)
				row := st.lastRow(t)
				want, _ := time.Parse(time.RFC3339, c.at)
				if c.clamped {
					want = testNow
				}
				if row.TimeClamped != c.clamped || !row.OccurredAt.Equal(want) {
					t.Errorf("row occurred_at %v clamped %v, want %v clamped %v", row.OccurredAt, row.TimeClamped, want, c.clamped)
				}
			})
		}
	}
	accepted := linesNamed(logLines(t, buf), lineAccepted)
	clamped := 0
	for _, l := range accepted {
		if l["time_clamped"] == true {
			clamped++
		}
	}
	if clamped != 4 {
		t.Errorf("%d accepted lines say time_clamped, want 4", clamped)
	}
	// Absent, the moment is now and not a clamp.
	wantDuplicate(t, post(t, h, sourceToken, body(beacon())), false)
	if row := st.lastRow(t); !row.OccurredAt.Equal(testNow) || row.TimeClamped {
		t.Errorf("no occurred_at: row = %v clamped %v", row.OccurredAt, row.TimeClamped)
	}
}

func TestDuplicateFollowsTheStoreOutcome(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	h := newHandler(t, st, goodDevices())
	st.upsertOut = store.UpsertOutcome{Inserted: true}
	wantDuplicate(t, post(t, h, sourceToken, body(beacon())), false)
	st.upsertOut = store.UpsertOutcome{Changed: true}
	wantDuplicate(t, post(t, h, sourceToken, body(beacon())), false)
	st.upsertOut = store.UpsertOutcome{}
	wantDuplicate(t, post(t, h, sourceToken, body(beacon())), true)
}

func TestADeviceMayPostOnlyOnItsOwnersSessions(t *testing.T) {
	st := newFakeStore()
	st.owners["theirs"] = "boss@example.com"
	st.owners["mine"] = "Dev@Example.com"
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	b := hook()
	b["session_ref"] = "theirs"
	wantRejected(t, post(t, h, deviceToken, body(b)), http.StatusForbidden, "forbidden", "")
	if _, _, rows := st.counts(); rows != 0 {
		t.Fatal("a forbidden post wrote a row")
	}
	b["session_ref"] = "mine"
	wantDuplicate(t, post(t, h, deviceToken, body(b)), false)
	b["session_ref"] = "unseen"
	wantDuplicate(t, post(t, h, deviceToken, body(b)), false)
	lines := linesNamed(logLines(t, buf), lineRejected)
	if len(lines) != 1 || lines[0]["reason"] != reasonForbidden || lines[0]["status"] != float64(403) || lines[0]["token_shape"] != shapeDevice {
		t.Errorf("rejected line = %v", lines)
	}
}

// ---------------------------------------------------------------------------
// Failures are 503, never 401 (criterion k)
// ---------------------------------------------------------------------------

func TestOwnFailuresAre503BehindTheStoreFailedLine(t *testing.T) {
	boom := errors.New("dial tcp: connection refused")
	cases := []struct {
		name  string
		setup func(*fakeStore, *fakeDevices)
		token string
		op    string
	}{
		{"the verify", func(s *fakeStore, _ *fakeDevices) { s.authErr = boom }, sourceToken, "verify source token"},
		{"the upsert", func(s *fakeStore, _ *fakeDevices) { s.upsertErr = boom }, sourceToken, "write skill invocation"},
		{"the roster read", func(s *fakeStore, _ *fakeDevices) { s.knownErr = boom }, "lss_laptop", "read roster"},
		{"the device verifier", func(_ *fakeStore, d *fakeDevices) { d.err = boom }, deviceToken, "verify device"},
		{"the device upsert", func(s *fakeStore, _ *fakeDevices) { s.upsertErr = boom }, deviceToken, "write skill invocation"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := newFakeStore()
			st.addToken(sourceToken, beaconToken())
			st.addToken("lss_laptop", store.SourceToken{ID: "33333333-3333-4333-8333-333333333333", Platform: store.PlatformClaudeCode, Environment: "laptop",
				Scope: Scope, RateLimitPerMin: 1200, AllowedOrigins: []string{store.OriginHook}, BoundActorEmail: ptr("dev@example.com")})
			dv := goodDevices()
			c.setup(st, dv)
			log, buf := jsonLogger()
			h := newHandler(t, st, dv, func(o *Options) { o.Logger = log })
			b := beacon()
			if c.token != sourceToken {
				b = hook()
				if c.token == "lss_laptop" {
					delete(b, "agent_platform")
				}
			}
			wantRejected(t, post(t, h, c.token, body(b)), http.StatusServiceUnavailable, "unavailable", "")
			failed := linesNamed(logLines(t, buf), lineStoreFailed)
			if len(failed) != 1 || failed[0]["level"] != "ERROR" || failed[0]["op"] != c.op || failed[0]["method"] != "POST" || failed[0]["path"] != InvocationsPath {
				t.Errorf("store failed line = %v", failed)
			}
			if _, has := failed[0]["error"]; !has {
				t.Errorf("the line carries no error")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Routes (criterion k)
// ---------------------------------------------------------------------------

func TestRoutesCarryTheInvocationsPathAndRegisterMountsThem(t *testing.T) {
	h := newHandler(t, newFakeStore(), goodDevices())
	found := false
	for _, r := range h.Routes() {
		if r.Pattern == "POST "+InvocationsPath {
			found = true
		}
	}
	if !found {
		t.Fatalf("Routes() = %v, want POST %s among them", h.Routes(), InvocationsPath)
	}
	own := http.NewServeMux()
	h.Register(own)
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		r := httptest.NewRequest(method, InvocationsPath, nil)
		_, want := h.mux.Handler(r)
		_, got := own.Handler(r)
		if got != want {
			t.Errorf("%s: ServeHTTP resolves %q, Register %q", method, want, got)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, InvocationsPath, nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET = %d", w.Code)
	}
}
