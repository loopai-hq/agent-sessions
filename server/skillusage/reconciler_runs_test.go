package skillusage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/loopai-hq/loop-sessions/server/store"
)

// The reconciler-runs route's properties, observed without a database:
// which credentials reach the ledger (a source token minted with the
// reconciler origin, and nothing else), which body rule a request fails
// and the field it names, that the run the ledger is handed carries the
// token's platform and id, and which status each ledger answer becomes.

// fakeRunStore is the invocation route's fake with the ledger port added:
// the handler asserts the port on its store, and this is the store that
// has it. The plain fakeStore is the one that does not.
type fakeRunStore struct {
	*fakeStore
	mu       sync.Mutex
	runs     []store.ReconcilerRun
	tokenIDs []string
	dup      bool
	conflict bool
	runErr   error
	softRuns []store.ReconcilerRun
}

func newFakeRunStore() *fakeRunStore { return &fakeRunStore{fakeStore: newFakeStore()} }

func (f *fakeRunStore) RecordReconcilerRun(_ context.Context, tokenID string, r store.ReconcilerRun) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.runErr != nil {
		return false, false, f.runErr
	}
	f.runs = append(f.runs, r)
	f.tokenIDs = append(f.tokenIDs, tokenID)
	return f.dup, f.conflict, nil
}

// LogSoftRevokedReconcilerRun records that the limit-0 post wrote its line
// and nothing else, which is what the route promises the operator.
func (f *fakeRunStore) LogSoftRevokedReconcilerRun(_ context.Context, tokenID string, r store.ReconcilerRun) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.softRuns = append(f.softRuns, r)
	f.tokenIDs = append(f.tokenIDs, tokenID)
}

func (f *fakeRunStore) lastRun(t *testing.T) (store.ReconcilerRun, string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.runs) == 0 {
		t.Fatal("no run reached the ledger")
	}
	return f.runs[len(f.runs)-1], f.tokenIDs[len(f.tokenIDs)-1]
}

// reconcilerToken is the Devin reconciler's own token (design 3.7 DEV-c):
// its own row, its own environment, the reconciler origin alone.
func reconcilerToken() store.SourceToken {
	return store.SourceToken{ID: sourceTokenID, Platform: store.PlatformDevin, Environment: "reconciler",
		Scope: Scope, RateLimitPerMin: 1200, AllowedOrigins: []string{store.OriginReconciler}}
}

// run is the smallest body a reconciler posts.
func run() map[string]any {
	return map[string]any{"idempotency_key": "2026-09-21", "window_start": "2026-09-20T12:00:00Z", "window_end": "2026-09-21T11:00:00Z",
		"sessions_scanned": 5, "sessions_with_events": 2, "rows_posted": 2, "truncated": 0}
}

func postRun(t *testing.T, h *Handler, bearer string, b []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, ReconcilerRunsPath, bytes.NewReader(b))
	r.RemoteAddr = "10.0.0.2:12345"
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestReconcilerRunsRouteIsMountedBesideTheInvocationRoute(t *testing.T) {
	h := newHandler(t, newFakeRunStore(), goodDevices())
	patterns := map[string]bool{}
	for _, r := range h.Routes() {
		patterns[r.Pattern] = true
	}
	if !patterns["POST "+ReconcilerRunsPath] || !patterns["POST "+InvocationsPath] {
		t.Errorf("routes = %v", patterns)
	}
	if ReconcilerRunsPath != "/v1/skill-invocations/reconciler-runs" {
		t.Errorf("path = %q", ReconcilerRunsPath)
	}
}

// Only a source token minted with the reconciler origin reaches the ledger
// (design 3.7, 10.1): a beacon token is 401, a device token is 401 without
// a device lookup, a junk bearer is 401 without any lookup, and a dead or
// wrong-scope token is 401 with its state on the line.
func TestReconcilerRunsRefuseEveryCredentialButAReconcilerToken(t *testing.T) {
	st := newFakeRunStore()
	// The shared beacon fixture of the invocation tests carries the
	// reconciler origin too; the token every Devin session can read holds
	// beacon alone (design 3.7 DEV-c), and that is the one refused here.
	beacon := beaconToken()
	beacon.AllowedOrigins = []string{store.OriginBeacon}
	st.addToken(sourceToken, beacon)
	catalog := beaconToken()
	catalog.Scope = store.ScopeSkillCatalog
	st.addToken("lss_catalog-token-for-tests", catalog)
	dv := goodDevices()
	log, buf := jsonLogger()
	h := newHandler(t, st, dv, func(o *Options) { o.Logger = log })

	for _, c := range []struct{ name, bearer, state string }{
		{"a beacon token", sourceToken, store.TokenStateLive},
		{"a device token", deviceToken, store.TokenStateNone},
		{"a junk bearer", "abc_" + strings.Repeat("x", 40), store.TokenStateNone},
		{"an unknown source token", "lss_" + strings.Repeat("y", 40), store.TokenStateNone},
		{"a catalog token", "lss_catalog-token-for-tests", store.TokenStateWrongScope},
		{"no bearer", "", store.TokenStateNone},
	} {
		t.Run(c.name, func(t *testing.T) {
			buf.Reset()
			wantRejected(t, postRun(t, h, c.bearer, body(run())), http.StatusUnauthorized, "unauthenticated", "")
			lines := linesNamed(logLines(t, buf), lineRejected)
			if len(lines) != 1 || lines[0]["token_state"] != c.state || lines[0]["reason"] != reasonUnauthenticated {
				t.Errorf("rejected line = %v, want token_state %s", lines, c.state)
			}
		})
	}
	if dv.calls != 0 {
		t.Errorf("a device bearer reached the device port %d times", dv.calls)
	}
	if len(st.runs) != 0 {
		t.Errorf("%d runs reached the ledger", len(st.runs))
	}
}

// A well-formed run under a reconciler token: one verify, the run handed to
// the ledger with the token's platform and id, 200 {"duplicate": false}.
func TestReconcilerRunCarriesTheTokensPlatformAndID(t *testing.T) {
	st := newFakeRunStore()
	st.addToken(sourceToken, reconcilerToken())
	h := newHandler(t, st, goodDevices())

	wantDuplicate(t, postRun(t, h, sourceToken, body(run())), false)
	got, tokenID := st.lastRun(t)
	if tokenID != sourceTokenID || got.AgentPlatform != store.PlatformDevin || got.IdempotencyKey != "2026-09-21" {
		t.Errorf("run = %+v under token %s", got, tokenID)
	}
	if got.SessionsScanned != 5 || got.SessionsWithEvents != 2 || got.RowsPosted != 2 || got.Truncated != 0 {
		t.Errorf("counts = %+v", got)
	}
	if got.WindowStart.Format("2006-01-02T15:04:05Z") != "2026-09-20T12:00:00Z" || got.WindowEnd.Format("2006-01-02T15:04:05Z") != "2026-09-21T11:00:00Z" {
		t.Errorf("window = %v to %v", got.WindowStart, got.WindowEnd)
	}
	if a, tx, rows := st.counts(); a != 1 || tx != 0 || rows != 0 {
		t.Errorf("verify calls %d, transactions %d, invocation rows %d; want one verify and no row transaction", a, tx, rows)
	}
	if st.verifiedInsideATransaction() {
		t.Error("the verify ran inside a transaction")
	}

	// A body repeating the token's platform is fine; one naming another is
	// 400 on agent_platform with the mismatch reason on the line.
	b := run()
	b["agent_platform"] = store.PlatformDevin
	wantDuplicate(t, postRun(t, h, sourceToken, body(b)), false)
	log, buf := jsonLogger()
	h = newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })
	b["agent_platform"] = store.PlatformVorflux
	wantRejected(t, postRun(t, h, sourceToken, body(b)), http.StatusBadRequest, "invalid_payload", "agent_platform")
	if lines := linesNamed(logLines(t, buf), lineRejected); len(lines) != 1 || lines[0]["reason"] != reasonPlatformMismatch || lines[0]["platform"] != store.PlatformDevin {
		t.Errorf("mismatch line = %v", lines)
	}
}

// Every body rule names its field (design 3.7, 4b).
func TestReconcilerRunBodyRulesNameTheirField(t *testing.T) {
	st := newFakeRunStore()
	st.addToken(sourceToken, reconcilerToken())
	h := newHandler(t, st, goodDevices())

	cases := []struct {
		name, field string
		mutate      func(map[string]any)
		status      int
	}{
		{"no idempotency_key", "idempotency_key", func(b map[string]any) { delete(b, "idempotency_key") }, 400},
		{"idempotency_key off shape", "idempotency_key", func(b map[string]any) { b["idempotency_key"] = "has space" }, 400},
		{"idempotency_key too long", "idempotency_key", func(b map[string]any) { b["idempotency_key"] = strings.Repeat("k", 65) }, 400},
		{"no window_start", "window_start", func(b map[string]any) { delete(b, "window_start") }, 400},
		{"no window_end", "window_end", func(b map[string]any) { delete(b, "window_end") }, 400},
		{"window_end not a time", "window_end", func(b map[string]any) { b["window_end"] = "yesterday" }, 400},
		{"window_end in the future", "window_end", func(b map[string]any) { b["window_end"] = "2026-09-21T13:00:00Z" }, 400},
		{"window_start after window_end", "window_start", func(b map[string]any) { b["window_start"] = "2026-09-21T11:30:00Z" }, 400},
		{"negative sessions_scanned", "sessions_scanned", func(b map[string]any) { b["sessions_scanned"] = -1 }, 400},
		{"negative rows_posted", "rows_posted", func(b map[string]any) { b["rows_posted"] = -2 }, 400},
		{"negative truncated", "truncated", func(b map[string]any) { b["truncated"] = -1 }, 400},
		// The 0023 columns are INTEGER, so a count above 2^31-1 used to
		// reach Postgres, come back "integer out of range" and be answered
		// 503: the reconciler then retried a permanently invalid body
		// forever and the operator saw a store failure rather than a client
		// one (adversarial finding 5).
		{"sessions_scanned over int32", "sessions_scanned", func(b map[string]any) { b["sessions_scanned"] = int64(3000000000) }, 400},
		{"sessions_with_events over int32", "sessions_with_events", func(b map[string]any) { b["sessions_with_events"] = int64(math.MaxInt32) + 1 }, 400},
		{"rows_posted over int32", "rows_posted", func(b map[string]any) { b["rows_posted"] = int64(math.MaxInt32) + 1 }, 400},
		{"truncated over int32", "truncated", func(b map[string]any) { b["truncated"] = int64(math.MaxInt32) + 1 }, 400},
		{"a string count", "sessions_with_events", func(b map[string]any) { b["sessions_with_events"] = "two" }, 400},
		{"an unknown key", "session_type", func(b map[string]any) { b["session_type"] = "automation" }, 400},
		{"args", "args", func(b map[string]any) { b["args"] = "x" }, 400},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := run()
			c.mutate(b)
			wantRejected(t, postRun(t, h, sourceToken, body(b)), c.status, "invalid_payload", c.field)
		})
	}
	if len(st.runs) != 0 {
		t.Errorf("%d refused bodies reached the ledger", len(st.runs))
	}
	// The largest count the columns hold is accepted, so the bound refuses
	// what Postgres would and nothing else.
	edge := run()
	edge["sessions_scanned"] = int64(math.MaxInt32)
	if w := postRun(t, h, sourceToken, body(edge)); w.Code != http.StatusOK {
		t.Errorf("sessions_scanned at 2^31-1 = %d: %s", w.Code, w.Body.String())
	}
	// A window ending inside the clamp the row route allows is accepted, so
	// a reconciler whose clock runs a minute ahead is not refused.
	b := run()
	b["window_end"] = "2026-09-21T12:03:00Z"
	wantDuplicate(t, postRun(t, h, sourceToken, body(b)), false)
	// The counts default to zero when absent.
	wantDuplicate(t, postRun(t, h, sourceToken, body(map[string]any{"idempotency_key": "2026-09-22", "window_start": "2026-09-21T00:00:00Z", "window_end": "2026-09-21T11:00:00Z"})), false)
	if got, _ := st.lastRun(t); got.SessionsScanned != 0 || got.RowsPosted != 0 {
		t.Errorf("absent counts = %+v, want zero", got)
	}
	// A body over the cap is 413; two documents are 400 on body.
	w := postRun(t, h, sourceToken, []byte(`{"idempotency_key":"`+strings.Repeat("k", 5000)+`"}`))
	wantRejected(t, w, http.StatusRequestEntityTooLarge, reasonBodyTooLarge, "")
	wantRejected(t, postRun(t, h, sourceToken, []byte(`{} {}`)), http.StatusBadRequest, "invalid_payload", "body")
}

// The ledger's three answers: a duplicate is 200 {"duplicate": true}, a
// conflict is 409 run_conflict with the seventh rejection reason on the
// line, a failure is 503 with the store failed line, a lock wait is 429.
func TestReconcilerRunLedgerAnswersBecomeStatuses(t *testing.T) {
	st := newFakeRunStore()
	st.addToken(sourceToken, reconcilerToken())
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	st.dup = true
	wantDuplicate(t, postRun(t, h, sourceToken, body(run())), true)
	st.dup, st.conflict = false, true
	w := postRun(t, h, sourceToken, body(run()))
	wantRejected(t, w, http.StatusConflict, reasonRunConflict, "")
	lines := linesNamed(logLines(t, buf), lineRejected)
	if len(lines) != 1 || lines[0]["reason"] != reasonRunConflict || lines[0]["status"] != float64(409) || lines[0]["source_token_id"] != sourceTokenID || lines[0]["platform"] != store.PlatformDevin {
		t.Errorf("conflict line = %v", lines)
	}
	buf.Reset()
	st.conflict = false
	st.runErr = errors.New("connection refused")
	w = postRun(t, h, sourceToken, body(run()))
	wantStatus(t, w, http.StatusServiceUnavailable)
	if failed := linesNamed(logLines(t, buf), lineStoreFailed); len(failed) != 1 || failed[0]["op"] != "record reconciler run" || failed[0]["platform"] != store.PlatformDevin || failed[0]["path"] != ReconcilerRunsPath {
		t.Errorf("store failed line = %v", failed)
	}
	st.runErr = lockWaitErr{code: "55P03"}
	w = postRun(t, h, sourceToken, body(run()))
	wantStatus(t, w, http.StatusTooManyRequests)
	if w.Header().Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q on a lock wait, want 1", w.Header().Get("Retry-After"))
	}
}

// A store without the ledger port is the server's own mistake: 503, never
// a 200 that recorded nothing. The app adapter is pinned to carry it.
func TestReconcilerRunWithoutALedgerIs503(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, reconcilerToken())
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })
	wantStatus(t, postRun(t, h, sourceToken, body(run())), http.StatusServiceUnavailable)
	if failed := linesNamed(logLines(t, buf), lineStoreFailed); len(failed) != 1 || !strings.Contains(failed[0]["error"].(string), "ledger") {
		t.Errorf("store failed line = %v", failed)
	}
}

// Limit 0 is the soft revoke on this route too: verified and counted,
// nothing recorded, the caller told its run was a duplicate, and the run's
// line goes out all the same, so a reconciler still posting on a rotated
// token is visible instead of silent (review-1 finding 3). Over the cap
// is 429. A token of a platform the reconciler CHECK does not hold is 400
// on agent_platform before the ledger.
func TestReconcilerRunSoftRevokeCapAndPlatform(t *testing.T) {
	st := newFakeRunStore()
	soft := reconcilerToken()
	soft.RateLimitPerMin = 0
	st.addToken(sourceToken, soft)
	h := newHandler(t, st, goodDevices())
	wantDuplicate(t, postRun(t, h, sourceToken, body(run())), true)
	if len(st.runs) != 0 {
		t.Error("a soft-revoked token recorded a run")
	}
	if len(st.softRuns) != 1 || st.softRuns[0].IdempotencyKey != run()["idempotency_key"].(string) || st.tokenIDs[0] != soft.ID {
		t.Errorf("the soft-revoked post wrote %d lines: %v", len(st.softRuns), st.softRuns)
	}

	st = newFakeRunStore()
	capped := reconcilerToken()
	capped.RateLimitPerMin = 1
	st.addToken(sourceToken, capped)
	h = newHandler(t, st, goodDevices())
	wantDuplicate(t, postRun(t, h, sourceToken, body(run())), false)
	w := postRun(t, h, sourceToken, body(run()))
	wantRejected(t, w, http.StatusTooManyRequests, reasonRateLimited, "")
	if w.Header().Get("Retry-After") != "30" {
		t.Errorf("Retry-After = %q over the cap", w.Header().Get("Retry-After"))
	}

	st = newFakeRunStore()
	laptop := reconcilerToken()
	laptop.Platform = store.PlatformClaudeCode
	st.addToken(sourceToken, laptop)
	h = newHandler(t, st, goodDevices())
	wantRejected(t, postRun(t, h, sourceToken, body(run())), http.StatusBadRequest, "invalid_payload", "agent_platform")
	if len(st.runs) != 0 {
		t.Error("a claude_code token recorded a run")
	}
}

// No line carries the body's text or the bearer, on any path.
func TestReconcilerRunLinesCarryNoPayloadText(t *testing.T) {
	st := newFakeRunStore()
	st.addToken(sourceToken, reconcilerToken())
	st.conflict = true
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })
	canary := func(k string) string { return secretFragment + "-" + k }
	for _, field := range []string{"agent_platform", "idempotency_key", "window_start", "window_end"} {
		b := run()
		b[field] = canary(field)
		postRun(t, h, sourceToken, body(b))
	}
	postRun(t, h, sourceToken, body(map[string]any{canary("key"): 1}))
	postRun(t, h, "lss_"+canary("bearer"), body(run()))
	postRun(t, h, sourceToken, body(run()))
	for _, l := range logLines(t, buf) {
		raw, _ := json.Marshal(l)
		if strings.Contains(string(raw), secretFragment) || strings.Contains(string(raw), sourceToken) {
			t.Fatalf("a line carries payload text or the bearer: %s", raw)
		}
	}
}
