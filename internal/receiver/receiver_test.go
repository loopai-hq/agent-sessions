// Every credential-shaped value in this file is synthetic. The planted
// Anthropic key and Postgres DSN below are AbCdEf/1234567890 filler and a
// made-up password on a host that does not exist, shaped like the real thing
// only so the scrubber's actual rules fire on them; neither has ever been a
// live credential. .gitleaks.toml and .github/secret_scanning.yml allowlist
// this file by path for that reason.

package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/backfill"
	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/drain"
	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/scrub"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// ---------------------------------------------------------------------------
// Planted credentials
//
// Obviously fake, but shaped exactly like the real thing so the scrubber's
// actual rules fire. If either of these reaches the receiver, the client shipped
// a secret.
// ---------------------------------------------------------------------------

const (
	fakeAnthropicKey = "sk-ant-api03-AbCdEf1234567890AbCdEf1234567890AbCdEf12"
	fakeDSN          = "postgres://svcuser:S3cr3tP4ssw0rd@db.internal:5432/loop"
	fakeDSNPassword  = "S3cr3tP4ssw0rd"
)

// ---------------------------------------------------------------------------
// The adapters the real packages need to be wired together
// ---------------------------------------------------------------------------

// spoolSink adapts a spool to capture.Sink. capture speaks event.Event and the
// spool speaks spool.Item, so something has to marshal between them; this is
// that something.
type spoolSink struct{ sp *spool.Spool }

func (s spoolSink) Put(e event.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return s.sp.Add(spool.Item{
		ID:        e.ID,
		Kind:      "event",
		SessionID: e.SessionID,
		Seq:       e.Seq,
		// The spool stamps EventTime itself when zero, so passing OccurredAt is
		// what keeps a backfilled item's age honest rather than import-stamped.
		EventTime: e.OccurredAt,
		Payload:   body,
	})
}

// scrubAdapter converts scrub.Scrub to the func(string) (string, map[string]int)
// that both capture and drain ask for. The only difference is that scrub keys
// its counts by scrub.Kind and everyone else wants a plain string.
func scrubAdapter(s string) (string, map[string]int) {
	res := scrub.Scrub(s)
	if len(res.Counts) == 0 {
		return res.Text, nil
	}
	out := make(map[string]int, len(res.Counts))
	for k, v := range res.Counts {
		out[string(k)] = v
	}
	return res.Text, out
}

// httpTransport is drain.Transport over net/http. The module has no HTTP client
// yet, so this is also a sketch of the one it will need.
type httpTransport struct {
	url    string
	client *http.Client
}

func (t *httpTransport) Send(ctx context.Context, batch []spool.Leased) (drain.Response, error) {
	items := make([]spool.Item, 0, len(batch))
	for _, l := range batch {
		items = append(items, l.Item)
	}
	body, err := json.Marshal(items)
	if err != nil {
		return drain.Response{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return drain.Response{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return drain.Response{}, err
	}
	defer resp.Body.Close()

	var wire Response
	// A hijacked connection or an error page may carry no JSON at all; the
	// status is what decides, so a decode failure here is not fatal.
	_ = json.NewDecoder(resp.Body).Decode(&wire)

	out := drain.Response{
		Accepted:   wire.Accepted,
		RetryAfter: time.Duration(wire.RetryAfterMS) * time.Millisecond,
	}
	for _, rj := range wire.Rejected {
		out.Rejected = append(out.Rejected, drain.Reject{ID: rj.ID, Reason: rj.Reason})
	}

	if resp.StatusCode/100 != 2 {
		// Both a populated Response and an error. This is the only way the
		// drain can see RetryAfter on a 429 or 503, since those are errors.
		return out, &ErrHTTP{Status: resp.StatusCode, RetryAfter: out.RetryAfter}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// rig is the whole client data path, wired from real packages.
type rig struct {
	t   *testing.T
	sp  *spool.Spool
	cap *capture.Capturer
	d   *drain.Drain
	rec *Receiver
	srv *httptest.Server
	dir string
}

func newRig(t *testing.T, opts ...func(*drain.Options)) *rig {
	t.Helper()
	dir := t.TempDir()

	// DiskFree is stubbed. The spool refuses writes below 5% free, and the
	// machine this is developed on sits at 98% used, so without this every test
	// here fails on a guard none of them are testing.
	sp, err := spool.Open(spool.Options{
		Dir:         filepath.Join(dir, "spool"),
		MaxAttempts: 4,
		DiskFree:    func(string) (uint64, uint64, error) { return 1 << 40, 1 << 40, nil },
	})
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}

	c, err := capture.New(spoolSink{sp}, scrubAdapter, capture.FileSeq{Dir: filepath.Join(dir, "seq")}.Next)
	if err != nil {
		t.Fatalf("capture.New: %v", err)
	}

	rec := New(Options{})
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)

	dopts := drain.Options{
		Spool:     sp,
		Transport: &httpTransport{url: srv.URL + Path, client: srv.Client()},
		BatchSize: 10,
		Interval:  time.Millisecond,
		Backoff:   drain.BackoffPolicy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2},
	}
	for _, o := range opts {
		o(&dopts)
	}
	d, err := drain.New(dopts)
	if err != nil {
		t.Fatalf("drain.New: %v", err)
	}

	return &rig{t: t, sp: sp, cap: c, d: d, rec: rec, srv: srv, dir: dir}
}

// pending reports how many items are still queued.
func (r *rig) pending() int {
	r.t.Helper()
	st, err := r.sp.Stats()
	if err != nil {
		r.t.Fatalf("spool.Stats: %v", err)
	}
	return st.Pending
}

func (r *rig) quarantined() int {
	r.t.Helper()
	st, err := r.sp.Stats()
	if err != nil {
		r.t.Fatalf("spool.Stats: %v", err)
	}
	return st.Quarantine
}

// drainAll cycles until the spool empties or the budget runs out. RunOnce is one
// batch by design, so draining is the caller's loop.
func (r *rig) drainAll(maxCycles int) {
	r.t.Helper()
	ctx := context.Background()
	for i := 0; i < maxCycles; i++ {
		if r.pending() == 0 {
			return
		}
		// Errors are expected while a failure is injected; the point of the loop
		// is that the client keeps trying.
		_, _ = r.d.RunOnce(ctx)
	}
}

// feed pushes a realistic hook sequence for one session and returns the number
// of events captured.
func (r *rig) feed(sessionID string, hooks []capture.HookEvent) int {
	r.t.Helper()
	var n int
	for _, h := range hooks {
		h.SessionID = sessionID
		got, err := r.cap.Handle(h)
		if err != nil {
			r.t.Fatalf("capture.Handle(%s): %v", h.HookEventName, err)
		}
		n += got
	}
	return n
}

// realisticSession is a hook sequence resembling a short piece of work, with a
// credential planted in the prompt and another in a tool input.
func realisticSession() []capture.HookEvent {
	return []capture.HookEvent{
		{HookEventName: "SessionStart", Source: "startup", Cwd: "/repo", Version: "2.1.221"},
		{
			HookEventName: "UserPromptSubmit",
			Prompt:        "wire the ingest client, my key is " + fakeAnthropicKey + " and the db is " + fakeDSN,
			Cwd:           "/repo", Version: "2.1.221",
		},
		{
			HookEventName: "PreToolUse", ToolName: "Read",
			ToolInput: json.RawMessage(`{"file_path":"/repo/main.go"}`),
			Cwd:       "/repo", Version: "2.1.221",
		},
		{
			HookEventName: "PostToolUse", ToolName: "Read",
			ToolInput:    json.RawMessage(`{"file_path":"/repo/main.go"}`),
			ToolResponse: json.RawMessage(`{"stdout":"package main"}`),
			Cwd:          "/repo", Version: "2.1.221",
		},
		{
			HookEventName: "PreToolUse", ToolName: "Bash",
			ToolInput: json.RawMessage(`{"command":"psql ` + fakeDSN + `"}`),
			Cwd:       "/repo", Version: "2.1.221",
		},
		{
			HookEventName: "PostToolUse", ToolName: "Write",
			ToolInput:    json.RawMessage(`{"file_path":"/repo/x.go"}`),
			ToolResponse: json.RawMessage(`{"filePath":"/repo/x.go","content":"package x","originalFile":null}`),
			Cwd:          "/repo", Version: "2.1.221",
		},
		{HookEventName: "Stop", Message: json.RawMessage(`{"role":"assistant"}`), Cwd: "/repo", Version: "2.1.221"},
		{HookEventName: "SessionEnd", Reason: "clear", Cwd: "/repo", Version: "2.1.221"},
	}
}

// assertNoSecrets fails if anything the receiver stored carries a planted
// credential.
func (r *rig) assertNoSecrets() {
	r.t.Helper()
	blob, err := r.rec.Raw()
	if err != nil {
		r.t.Fatalf("receiver.Raw: %v", err)
	}
	body := string(blob)
	for _, secret := range []string{fakeAnthropicKey, fakeDSNPassword, "sk-ant-api03-AbCdEf"} {
		if strings.Contains(body, secret) {
			r.t.Errorf("a planted credential reached the server: %q", secret)
		}
	}
}

// ---------------------------------------------------------------------------
// Receiver unit tests
// ---------------------------------------------------------------------------

func post(t *testing.T, srv *httptest.Server, items []spool.Item) (*http.Response, Response) {
	t.Helper()
	body, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := srv.Client().Post(srv.URL+Path, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	var wire Response
	_ = json.NewDecoder(resp.Body).Decode(&wire)
	return resp, wire
}

// item builds a well-formed spooled item.
func item(id, sessionID string, seq int64) spool.Item {
	e := event.Event{
		ID: id, Source: event.SourceClaudeCode, Origin: event.OriginHook,
		Type: event.UserPrompt, SessionID: sessionID, Seq: seq,
		OccurredAt: time.Unix(1700000000+seq, 0).UTC(),
		Text:       fmt.Sprintf("prompt %d", seq),
	}
	body, _ := json.Marshal(e)
	return spool.Item{ID: id, Kind: "event", SessionID: sessionID, Seq: seq, EventTime: e.OccurredAt, Payload: body}
}

func TestReceiverStoresAndReports(t *testing.T) {
	t.Parallel()
	rec := New(Options{})
	srv := httptest.NewServer(rec)
	defer srv.Close()

	_, wire := post(t, srv, []spool.Item{item("a", "s1", 1), item("b", "s1", 2)})

	if len(wire.Accepted) != 2 {
		t.Errorf("accepted %v, want 2", wire.Accepted)
	}
	if rec.Count() != 2 {
		t.Errorf("Count() = %d, want 2", rec.Count())
	}
	if got := rec.Sessions()["s1"]; len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Errorf("session not ordered by Seq: %+v", got)
	}
}

// At-least-once delivery is guaranteed by the client, so redelivery is the
// normal path after any crash. It must acknowledge without storing again.
func TestReceiverIsIdempotent(t *testing.T) {
	t.Parallel()
	rec := New(Options{})
	srv := httptest.NewServer(rec)
	defer srv.Close()

	batch := []spool.Item{item("a", "s1", 1), item("b", "s1", 2)}

	_, first := post(t, srv, batch)
	countAfterFirst := rec.Count()

	for i := 0; i < 5; i++ {
		_, again := post(t, srv, batch)
		if len(again.Accepted) != len(first.Accepted) {
			t.Errorf("redelivery %d accepted %v, want the same ids as the first", i, again.Accepted)
		}
	}

	if rec.Count() != countAfterFirst {
		t.Errorf("Count() moved from %d to %d on redelivery; the server double-counted",
			countAfterFirst, rec.Count())
	}
	if got := rec.Sessions()["s1"]; len(got) != 2 {
		t.Errorf("session has %d events after 6 deliveries, want 2", len(got))
	}
	if rec.RedundantCount() != 10 {
		t.Errorf("RedundantCount() = %d, want 10", rec.RedundantCount())
	}
}

func TestReceiverRejectsMalformed(t *testing.T) {
	t.Parallel()

	invalidEvent := item("e", "s1", 5)
	// Strip the fields Validate insists on.
	invalidEvent.Payload = json.RawMessage(`{"id":"e","session_id":"s1"}`)

	cases := []struct {
		name   string
		it     spool.Item
		reason string
	}{
		{"missing id", func() spool.Item { i := item("", "s1", 1); return i }(), ReasonNoID},
		{"missing session id", func() spool.Item { i := item("b", "", 2); i.SessionID = ""; return i }(), ReasonNoSessionID},
		{"event fails validation", invalidEvent, ReasonInvalid},
		{"empty payload", func() spool.Item { i := item("f", "s1", 6); i.Payload = nil; return i }(), ReasonBadPayload},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := New(Options{})
			srv := httptest.NewServer(rec)
			defer srv.Close()

			_, wire := post(t, srv, []spool.Item{tc.it})

			if len(wire.Accepted) != 0 {
				t.Errorf("accepted %v, want none", wire.Accepted)
			}
			if len(wire.Rejected) != 1 || wire.Rejected[0].Reason != tc.reason {
				t.Errorf("rejected = %+v, want reason %q", wire.Rejected, tc.reason)
			}
			if rec.Count() != 0 {
				t.Errorf("stored %d items despite rejecting them", rec.Count())
			}
		})
	}
}

// A good item alongside a bad one must still land: partial acceptance is the
// mechanism that stops one poison payload blocking a session.
func TestReceiverAcceptsGoodAlongsideBad(t *testing.T) {
	t.Parallel()
	rec := New(Options{})
	srv := httptest.NewServer(rec)
	defer srv.Close()

	// Valid JSON, but not a storable event. An outright unparseable payload
	// cannot be marshalled by a Go client at all — see the raw-bytes test.
	bad := item("bad", "s1", 2)
	bad.Payload = json.RawMessage(`{"id":"bad"}`)

	_, wire := post(t, srv, []spool.Item{item("ok1", "s1", 1), bad, item("ok2", "s1", 3)})

	if len(wire.Accepted) != 2 {
		t.Errorf("accepted %v, want the two good ids", wire.Accepted)
	}
	if len(wire.Rejected) != 1 {
		t.Errorf("rejected %+v, want one", wire.Rejected)
	}
	if rec.Count() != 2 {
		t.Errorf("Count() = %d, want 2", rec.Count())
	}
}

// An over-cap batch is answered per item rather than with a 413, because a 413
// is a transport error the client would retry forever.
func TestReceiverRejectsOversizedBatchPerItem(t *testing.T) {
	t.Parallel()
	rec := New(Options{MaxBatchBytes: 512})
	srv := httptest.NewServer(rec)
	defer srv.Close()

	big := item("big", "s1", 1)
	e := event.Event{
		ID: "big", Source: event.SourceClaudeCode, Origin: event.OriginHook,
		Type: event.UserPrompt, SessionID: "s1", Seq: 1,
		OccurredAt: time.Unix(1700000000, 0).UTC(),
		// Over the 512-byte cap but well under the 4 KiB hard limit, so the body
		// is still parseable and can be answered per item.
		Text: strings.Repeat("x", 800),
	}
	big.Payload, _ = json.Marshal(e)

	resp, wire := post(t, srv, []spool.Item{big})

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 so the client gets a per-item verdict it can act on", resp.StatusCode)
	}
	if len(wire.Rejected) != 1 || wire.Rejected[0].Reason != ReasonBatchTooBig {
		t.Errorf("rejected = %+v, want a %q rejection", wire.Rejected, ReasonBatchTooBig)
	}
	if rec.Count() != 0 {
		t.Errorf("stored %d oversized items", rec.Count())
	}
}

func TestReceiverFailureInjection(t *testing.T) {
	t.Parallel()

	t.Run("503 then recovery", func(t *testing.T) {
		t.Parallel()
		rec := New(Options{})
		srv := httptest.NewServer(rec)
		defer srv.Close()

		rec.FailNext(2, Fail503, 0)
		for i := 0; i < 2; i++ {
			resp, _ := post(t, srv, []spool.Item{item("a", "s1", 1)})
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("call %d: status %d, want 503", i, resp.StatusCode)
			}
		}
		resp, wire := post(t, srv, []spool.Item{item("a", "s1", 1)})
		if resp.StatusCode != http.StatusOK || len(wire.Accepted) != 1 {
			t.Errorf("recovery call: status %d accepted %v", resp.StatusCode, wire.Accepted)
		}
	})

	t.Run("429 carries retry after", func(t *testing.T) {
		t.Parallel()
		rec := New(Options{})
		srv := httptest.NewServer(rec)
		defer srv.Close()

		rec.FailNext(1, Fail429, 3*time.Second)
		resp, wire := post(t, srv, []spool.Item{item("a", "s1", 1)})

		if resp.StatusCode != http.StatusTooManyRequests {
			t.Errorf("status = %d, want 429", resp.StatusCode)
		}
		if wire.RetryAfterMS != 3000 {
			t.Errorf("retry_after_ms = %d, want 3000", wire.RetryAfterMS)
		}
		if got := resp.Header.Get("Retry-After"); got != "3" {
			t.Errorf("Retry-After header = %q, want 3", got)
		}
	})

	t.Run("connection drop", func(t *testing.T) {
		t.Parallel()
		rec := New(Options{})
		srv := httptest.NewServer(rec)
		defer srv.Close()

		rec.FailNext(1, FailDrop, 0)
		body, _ := json.Marshal([]spool.Item{item("a", "s1", 1)})
		if _, err := srv.Client().Post(srv.URL+Path, "application/json", bytes.NewReader(body)); err == nil {
			t.Error("expected a transport error from a dropped connection")
		}
		if rec.Count() != 0 {
			t.Errorf("stored %d despite dropping the connection", rec.Count())
		}
	})

	t.Run("fail forever until healed", func(t *testing.T) {
		t.Parallel()
		rec := New(Options{})
		srv := httptest.NewServer(rec)
		defer srv.Close()

		rec.FailNext(-1, Fail503, 0)
		for i := 0; i < 5; i++ {
			resp, _ := post(t, srv, []spool.Item{item("a", "s1", 1)})
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("call %d: status %d, want 503", i, resp.StatusCode)
			}
		}
		rec.Heal()
		resp, _ := post(t, srv, []spool.Item{item("a", "s1", 1)})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("after Heal: status %d, want 200", resp.StatusCode)
		}
	})
}

func TestReceiverRoutingAndMethods(t *testing.T) {
	t.Parallel()
	rec := New(Options{})
	srv := httptest.NewServer(rec)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + Path)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", resp.StatusCode)
	}

	resp, err = srv.Client().Post(srv.URL+"/nope", "application/json", strings.NewReader("[]"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("wrong-path status = %d, want 404", resp.StatusCode)
	}

	resp, err = srv.Client().Post(srv.URL+Path, "application/json", strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("malformed-body status = %d, want 400", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// End-to-end: hook -> spool -> drain -> server
// ---------------------------------------------------------------------------

func TestEndToEndHappyPath(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	const sid = "11111111-2222-3333-4444-555555555555"
	captured := r.feed(sid, realisticSession())
	if captured == 0 {
		t.Fatal("capture produced no events")
	}
	if r.pending() != captured {
		t.Fatalf("spool holds %d, capture produced %d", r.pending(), captured)
	}

	r.drainAll(20)

	if n := r.pending(); n != 0 {
		t.Errorf("spool still holds %d items", n)
	}
	if r.rec.Count() != captured {
		t.Errorf("server stored %d of %d captured events", r.rec.Count(), captured)
	}
	if r.quarantined() != 0 {
		t.Errorf("%d items quarantined on the happy path", r.quarantined())
	}

	// Ordering within the session must follow Seq, and the sequence must be
	// complete: a gap means an event was acked without being stored.
	got := r.rec.Sessions()[sid]
	if len(got) != captured {
		t.Fatalf("session has %d events, want %d", len(got), captured)
	}
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Errorf("events out of order at %d: seq %d then %d", i, got[i-1].Seq, got[i].Seq)
		}
	}

	// Scrubbing happens before the spool, not at the network boundary.
	r.assertNoSecrets()

	// And the redaction actually fired rather than the secret simply not being
	// in a field we looked at.
	var sawRedaction bool
	for _, e := range got {
		if len(e.Redactions) > 0 {
			sawRedaction = true
		}
	}
	if !sawRedaction {
		t.Error("no event recorded a redaction; the planted credentials were never scrubbed")
	}
}

// The secret must be absent from the SPOOL FILES too, not merely from what the
// server stored. A spool file leaking on a laptop is a credential leak even if
// nothing was ever uploaded.
func TestSecretsNeverReachDisk(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.feed("aaaa-bbbb", realisticSession())

	var checked int
	err := filepath.Walk(filepath.Join(r.dir, "spool"), func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return nil
		}
		checked++
		for _, secret := range []string{fakeAnthropicKey, fakeDSNPassword} {
			if bytes.Contains(b, []byte(secret)) {
				t.Errorf("%s contains a planted credential", filepath.Base(p))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if checked == 0 {
		t.Fatal("no spool files were inspected")
	}
}

// A server that is down for a while must cost nothing but time.
func TestEndToEndSurvivesOutage(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	const sid = "outage-session"
	captured := r.feed(sid, realisticSession())

	r.rec.FailNext(4, Fail503, 0)
	r.drainAll(4)

	if r.pending() != captured {
		t.Errorf("pending = %d during the outage, want all %d still queued", r.pending(), captured)
	}
	if r.quarantined() != 0 {
		t.Errorf("%d items quarantined during an outage; a network failure must not consume attempts",
			r.quarantined())
	}

	r.drainAll(20)

	if n := r.pending(); n != 0 {
		t.Errorf("spool still holds %d after recovery", n)
	}
	if r.rec.Count() != captured {
		t.Errorf("server stored %d of %d after recovery", r.rec.Count(), captured)
	}
	if got := len(r.rec.Sessions()[sid]); got != captured {
		t.Errorf("session has %d events after recovery, want %d", got, captured)
	}
}

// A dropped connection leaves the client unable to tell whether the batch
// committed. It must redeliver, and the server must absorb the duplicate.
func TestEndToEndConnectionDropRedeliversWithoutDuplication(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	const sid = "drop-session"
	captured := r.feed(sid, realisticSession())

	r.rec.FailNext(2, FailDrop, 0)
	r.drainAll(30)

	if n := r.pending(); n != 0 {
		t.Errorf("spool still holds %d", n)
	}
	if r.rec.Count() != captured {
		t.Errorf("server stored %d of %d captured events", r.rec.Count(), captured)
	}
	if got := len(r.rec.Sessions()[sid]); got != captured {
		t.Errorf("session has %d events, want %d — a redelivery was stored twice", got, captured)
	}
}

// One bad item must not block the rest, and must end in quarantine rather than
// being silently dropped or retried forever.
func TestEndToEndRejectionQuarantinesAndTheRestDeliver(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	const sid = "reject-session"
	captured := r.feed(sid, realisticSession())

	// Plant one item the server will refuse, by corrupting a spooled payload in
	// place. This is also the only realistic way an unstorable item exists: the
	// client validated it on the way in.
	leased, err := r.sp.Lease(1)
	if err != nil || len(leased) == 0 {
		t.Fatalf("lease: %v", err)
	}
	victim := leased[0].Item.ID

	ents, err := os.ReadDir(filepath.Join(r.dir, "spool", "pending"))
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, ent := range ents {
		p := filepath.Join(r.dir, "spool", "pending", ent.Name())
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var it spool.Item
		if json.Unmarshal(b, &it) != nil || it.ID != victim {
			continue
		}
		it.Payload = json.RawMessage(`{"id":"` + victim + `"}`) // fails event.Validate
		nb, _ := json.Marshal(it)
		if err := os.WriteFile(p, nb, 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		break
	}

	r.drainAll(30)

	if n := r.pending(); n != 0 {
		t.Errorf("spool still holds %d; the rejected item blocked the queue", n)
	}
	if r.quarantined() != 1 {
		t.Errorf("quarantine holds %d, want exactly the one rejected item", r.quarantined())
	}
	if r.rec.Count() != captured-1 {
		t.Errorf("server stored %d, want %d (all but the rejected item)", r.rec.Count(), captured-1)
	}
	if r.rec.Has(victim) {
		t.Error("the rejected item was stored after all")
	}
}

// Nothing may be acked that the server did not accept.
func TestEndToEndNothingAckedThatWasNotAccepted(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	const sid = "partial-session"
	captured := r.feed(sid, realisticSession())

	// A transport that drops half of each batch's acceptances on the floor,
	// simulating a server that stored only part of what it was sent.
	r.d = mustDrain(t, r.sp, &halfAckTransport{
		inner: &httpTransport{url: r.srv.URL + Path, client: r.srv.Client()},
	})

	ctx := context.Background()
	_, _ = r.d.RunOnce(ctx)

	// The server received and stored the whole batch. Withholding an ack does
	// not un-store anything server-side, which is why the naive invariant
	// "stored + pending == captured" is wrong.
	if r.rec.Count() != captured {
		t.Errorf("server stored %d of %d sent", r.rec.Count(), captured)
	}
	// The client may only delete what it was told was accepted, so the withheld
	// half must still be queued.
	remaining := r.pending()
	if remaining == 0 {
		t.Fatal("items whose acceptance was withheld were acked anyway")
	}
	if want := captured - captured/2; remaining != want {
		t.Errorf("pending = %d, want the %d whose acks were withheld", remaining, want)
	}

	// Redelivering them must not duplicate. This reaches the idempotency path
	// with no injected failure at all, which is how it happens in production.
	r.d = mustDrain(t, r.sp, &httpTransport{url: r.srv.URL + Path, client: r.srv.Client()})
	r.drainAll(20)

	if n := r.pending(); n != 0 {
		t.Errorf("spool still holds %d", n)
	}
	if r.rec.Count() != captured {
		t.Errorf("server stored %d after redelivery, want %d — a duplicate was appended",
			r.rec.Count(), captured)
	}
	if r.rec.RedundantCount() == 0 {
		t.Error("no redelivery was absorbed, so idempotency was never exercised")
	}
}

// A Go client cannot send an unparseable payload: json.RawMessage validates on
// marshal. The rejection still has to exist for corrupted bodies and non-Go
// clients, but it can only be reached by posting raw bytes.
func TestReceiverRejectsUnparseablePayloadFromRawBytes(t *testing.T) {
	t.Parallel()
	rec := New(Options{})
	srv := httptest.NewServer(rec)
	defer srv.Close()

	raw := `[{"id":"x","kind":"event","session_id":"s1","seq":1,"payload":"not-an-object"}]`
	resp, err := srv.Client().Post(srv.URL+Path, "application/json", strings.NewReader(raw))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	var wire Response
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(wire.Rejected) != 1 || wire.Rejected[0].Reason != ReasonBadPayload {
		t.Errorf("rejected = %+v, want a %q rejection", wire.Rejected, ReasonBadPayload)
	}
	if rec.Count() != 0 {
		t.Errorf("stored %d items with an unusable payload", rec.Count())
	}
}

// halfAckTransport forwards the request but reports only the first half of the
// server's acceptances, so the client must keep the rest pending.
type halfAckTransport struct{ inner drain.Transport }

func (h *halfAckTransport) Send(ctx context.Context, batch []spool.Leased) (drain.Response, error) {
	resp, err := h.inner.Send(ctx, batch)
	if err != nil {
		return resp, err
	}
	resp.Accepted = resp.Accepted[:len(resp.Accepted)/2]
	return resp, nil
}

func mustDrain(t *testing.T, sp *spool.Spool, tr drain.Transport) *drain.Drain {
	t.Helper()
	d, err := drain.New(drain.Options{
		Spool: sp, Transport: tr, BatchSize: 100, Interval: time.Millisecond,
		Backoff: drain.BackoffPolicy{Initial: time.Millisecond, Max: 2 * time.Millisecond, Multiplier: 2},
	})
	if err != nil {
		t.Fatalf("drain.New: %v", err)
	}
	return d
}

// ---------------------------------------------------------------------------
// End-to-end: historical transcripts through the same chain
// ---------------------------------------------------------------------------

// The backfill path must reach the server with its original timestamps intact.
// That is the property most likely to regress silently, because an
// import-stamped event looks perfectly healthy.
func TestEndToEndBackfillPreservesEventTime(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	root := filepath.Join(r.dir, "projects")
	sid := "99999999-8888-7777-6666-555555555555"
	const (
		ts1 = "2026-03-01T09:00:00.000Z"
		ts2 = "2026-03-01T09:00:05.500Z"
		ts3 = "2026-03-01T09:01:00.000Z"
	)
	writeTranscript(t, filepath.Join(root, "proj", sid+".jsonl"),
		userLine(sid, ts1, "u1", "historical prompt with "+fakeAnthropicKey),
		assistantLine(sid, ts2, "a1", "on it"),
		userLine(sid, ts3, "u2", "second prompt"),
	)

	sink := spoolSink{r.sp}
	res, err := backfill.Walk(backfill.Options{Root: root, Scrub: scrubAdapter}, sink.Put)
	if err != nil {
		t.Fatalf("backfill.Walk: %v", err)
	}
	if res.Events == 0 {
		t.Fatal("backfill produced no events")
	}

	r.drainAll(30)

	if n := r.pending(); n != 0 {
		t.Errorf("spool still holds %d", n)
	}
	if r.rec.Count() != res.Events {
		t.Errorf("server stored %d of %d backfilled events", r.rec.Count(), res.Events)
	}

	want := map[time.Time]bool{
		mustTime(t, ts1): true, mustTime(t, ts2): true, mustTime(t, ts3): true,
	}
	got := r.rec.Sessions()[sid]
	if len(got) == 0 {
		t.Fatalf("nothing stored for the backfilled session; sessions: %v", keysOf(r.rec.Sessions()))
	}

	now := time.Now()
	for _, e := range got {
		if !want[e.OccurredAt.UTC()] {
			t.Errorf("event %s has OccurredAt %s, which is not one of the transcript timestamps",
				e.Type, e.OccurredAt)
		}
		if now.Sub(e.OccurredAt) < time.Hour {
			t.Errorf("event %s arrived stamped at import time (%s)", e.Type, e.OccurredAt)
		}
		if e.Origin != event.OriginTranscript {
			t.Errorf("Origin = %q, want %q", e.Origin, event.OriginTranscript)
		}
	}

	r.assertNoSecrets()
}

// Live and historical events for the same session must coexist, since a
// backfill runs while the harness is still in use.
func TestEndToEndLiveAndBackfillCoexist(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	const sid = "mixed-session"
	live := r.feed(sid, realisticSession())

	root := filepath.Join(r.dir, "projects")
	writeTranscript(t, filepath.Join(root, "proj", "77777777-1111-2222-3333-444444444444.jsonl"),
		userLine("77777777-1111-2222-3333-444444444444", "2026-03-01T09:00:00.000Z", "u1", "old work"),
	)
	sink := spoolSink{r.sp}
	res, err := backfill.Walk(backfill.Options{Root: root, Scrub: scrubAdapter}, sink.Put)
	if err != nil {
		t.Fatalf("backfill.Walk: %v", err)
	}

	r.drainAll(40)

	if n := r.pending(); n != 0 {
		t.Errorf("spool still holds %d", n)
	}
	if want := live + res.Events; r.rec.Count() != want {
		t.Errorf("server stored %d, want %d live+historical", r.rec.Count(), want)
	}

	var hook, transcript int
	for _, evs := range r.rec.Sessions() {
		for _, e := range evs {
			switch e.Origin {
			case event.OriginHook:
				hook++
			case event.OriginTranscript:
				transcript++
			}
		}
	}
	if hook == 0 || transcript == 0 {
		t.Errorf("expected both origins to arrive; hook=%d transcript=%d", hook, transcript)
	}
}

// ---------------------------------------------------------------------------
// Transcript fixtures
// ---------------------------------------------------------------------------

func writeTranscript(t *testing.T, path string, lines ...string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func userLine(session, ts, uuid, text string) string {
	return fmt.Sprintf(
		`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":%q,"cwd":"/repo","version":"2.1.221","message":{"role":"user","content":%q}}`,
		session, ts, uuid, text)
}

func assistantLine(session, ts, uuid, text string) string {
	return fmt.Sprintf(
		`{"type":"assistant","sessionId":%q,"timestamp":%q,"uuid":%q,"cwd":"/repo","version":"2.1.221","message":{"role":"assistant","model":"claude-fable-5","id":"msg_1","usage":{"input_tokens":10,"output_tokens":20},"content":[{"type":"text","text":%q}]}}`,
		session, ts, uuid, text)
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("bad fixture time %q: %v", s, err)
	}
	return ts.UTC()
}

func keysOf(m map[string][]event.Event) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
