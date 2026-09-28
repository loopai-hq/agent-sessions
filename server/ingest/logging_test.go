package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// The lines this package promises the metrics layer. Their names are the
// filters in examples/deploy-gcp/monitoring/metrics, so a rename here is a metric
// that silently reads zero.
const (
	lineBatch      = "ingest batch"
	lineRejected   = "ingest item rejected"
	lineNoVerdict  = "store returned no verdict"
	secretFragment = "SECRET-PROMPT-CONTENT-do-not-log"
)

// jsonLogger captures the handler's lines as JSON so a test can read fields
// rather than grep text. Requests are served on the calling goroutine.
func jsonLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// logLines parses the buffer, one map per line.
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

// linesNamed filters the parsed lines by message.
func linesNamed(lines []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}

// postAs sends a batch the way the agent does, with its User-Agent, as the
// named credential.
func postAs(t *testing.T, h *Handler, token string, items []spool.Item) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	r := httptest.NewRequest(http.MethodPost, EventsPath, bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("User-Agent", "loop-sessions/abc1234")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// secretEvent is an event whose text must never reach the log.
func secretEvent(id string, seq int64) event.Event {
	e := mkEvent(id, seq, event.UserPrompt)
	e.Text = secretFragment + " " + id
	return e
}

// TestBatchLineCountsEveryOutcomeAndCarriesNoContent is the "ingest batch"
// contract: one INFO line per request, with the principal, the device, the
// agent build from the User-Agent, and a count for every outcome an item can
// have, summing to the batch. The payload text is asserted absent from every
// line the request produced, because the log is a second, less guarded copy
// of whatever reaches it.
func TestBatchLineCountsEveryOutcomeAndCarriesNoContent(t *testing.T) {
	log, buf := jsonLogger()
	st := newMemStore(nil)
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	fresh := secretEvent("e1", 1)
	repeat := secretEvent("e2", 2)
	// Missing occurred_at, source and origin: invalid, but typed, so the
	// rejection line can say what kind of thing was refused.
	invalid := rawItem("bad1", "sess-1", `{"id":"bad1","session_id":"sess-1","type":"tool_result","text":"`+secretFragment+`"}`)
	// Valid JSON that is not an event at all: the batch marshals, the item
	// does not decode, and the string inside it must still stay out of the log.
	unparseable := rawItem("bad2", "sess-1", `"`+secretFragment+`"`)

	w := postAs(t, h, testToken, []spool.Item{
		mkItem(t, fresh), mkItem(t, repeat), mkItem(t, repeat), invalid, unparseable,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	lines := logLines(t, buf)
	for _, l := range lines {
		if raw, _ := json.Marshal(l); strings.Contains(string(raw), secretFragment) {
			t.Fatalf("a log line carries session content: %s", raw)
		}
	}

	batches := linesNamed(lines, lineBatch)
	if len(batches) != 1 {
		t.Fatalf("got %d %q lines, want 1:\n%s", len(batches), lineBatch, buf.String())
	}
	b := batches[0]
	want := map[string]any{
		"level":         "INFO",
		"email":         testEmail,
		"device_id":     testDevice,
		"agent_version": "abc1234",
		"items":         float64(5),
		"accepted":      float64(2),
		"duplicate":     float64(1),
		"rejected":      float64(2),
		"undecided":     float64(0),
		"status":        float64(200),
	}
	for k, v := range want {
		if b[k] != v {
			t.Errorf("batch line %s = %v, want %v (line: %v)", k, b[k], v, b)
		}
	}
	if n, _ := b["bytes"].(float64); n <= 0 {
		t.Errorf("batch line bytes = %v, want the body size", b["bytes"])
	}

	rejected := linesNamed(lines, lineRejected)
	if len(rejected) != 2 {
		t.Fatalf("got %d %q lines, want 2:\n%s", len(rejected), lineRejected, buf.String())
	}
	byReason := map[string]map[string]any{}
	for _, l := range rejected {
		if l["level"] != "WARN" {
			t.Errorf("rejection line is %v, want WARN", l["level"])
		}
		if l["email"] != testEmail || l["device_id"] != testDevice {
			t.Errorf("rejection line is not attributed: %v", l)
		}
		byReason[l["reason"].(string)] = l
	}
	if l := byReason[ReasonInvalid]; l == nil || l["event_type"] != "tool_result" {
		t.Errorf("the validation rejection does not name the event type: %v", l)
	}
	if l := byReason[ReasonBadPayload]; l == nil || l["event_type"] != "" {
		t.Errorf("the unparseable rejection should carry no type: %v", l)
	}
	if got := st.count(); got != 2 {
		t.Errorf("stored %d events, want 2; the logging must not change what is kept", got)
	}
}

// TestStoreRejectionsAreLoggedWithTheStoresReason: a rejection the store
// decided is logged exactly as one the handler decided, with the store's own
// reason, because the agent quarantines both alike.
func TestStoreRejectionsAreLoggedWithTheStoresReason(t *testing.T) {
	log, buf := jsonLogger()
	st := newMemStore(nil)
	const otherToken = "lsd_other"
	devices := fakeDevices{tokens: map[string]Identity{
		testToken:  {Email: testEmail, DeviceID: testDevice},
		otherToken: {Email: "other@example.com", DeviceID: "22222222-2222-4222-8222-222222222222"},
	}}
	h := newHandler(t, st, devices, func(o *Options) { o.Logger = log })

	if w := postAs(t, h, testToken, []spool.Item{mkItem(t, secretEvent("e1", 1))}); w.Code != http.StatusOK {
		t.Fatalf("first post: %d", w.Code)
	}
	buf.Reset()
	// The same session from another principal's device: the store refuses it.
	if w := postAs(t, h, otherToken, []spool.Item{mkItem(t, secretEvent("e2", 2))}); w.Code != http.StatusOK {
		t.Fatalf("second post: %d", w.Code)
	}

	lines := logLines(t, buf)
	rejected := linesNamed(lines, lineRejected)
	if len(rejected) != 1 {
		t.Fatalf("got %d rejection lines, want 1:\n%s", len(rejected), buf.String())
	}
	if rejected[0]["reason"] != "session belongs to another principal" || rejected[0]["event_type"] != "user_prompt" ||
		rejected[0]["email"] != "other@example.com" {
		t.Errorf("store rejection line = %v", rejected[0])
	}
	batches := linesNamed(lines, lineBatch)
	if len(batches) != 1 || batches[0]["rejected"] != float64(1) || batches[0]["accepted"] != float64(0) {
		t.Errorf("batch line = %v, want rejected=1 accepted=0", batches)
	}
	if strings.Contains(buf.String(), secretFragment) {
		t.Error("a log line carries session content")
	}
}

// TestAnUndecidedItemIsLoggedAtErrorWithItsEventID is the undecided signal.
// The store answered without mentioning an item; the agent keeps it and
// retries, an old agent gives up after eight such answers, and the only
// server-side evidence is this line. It has to be ERROR (it is a server bug)
// and it has to carry the event id (it is how the item is found).
func TestAnUndecidedItemIsLoggedAtErrorWithItsEventID(t *testing.T) {
	log, buf := jsonLogger()
	h := newHandler(t, silentStore{}, goodDevices(), func(o *Options) { o.Logger = log })

	if w := postAs(t, h, testToken, []spool.Item{mkItem(t, secretEvent("e1", 1))}); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	lines := logLines(t, buf)
	undecided := linesNamed(lines, lineNoVerdict)
	if len(undecided) != 1 {
		t.Fatalf("got %d %q lines, want 1:\n%s", len(undecided), lineNoVerdict, buf.String())
	}
	if undecided[0]["level"] != "ERROR" || undecided[0]["event_id"] != "e1" || undecided[0]["email"] != testEmail {
		t.Errorf("undecided line = %v", undecided[0])
	}
	batches := linesNamed(lines, lineBatch)
	if len(batches) != 1 || batches[0]["undecided"] != float64(1) || batches[0]["accepted"] != float64(0) {
		t.Errorf("batch line = %v, want undecided=1", batches)
	}
	if strings.Contains(buf.String(), secretFragment) {
		t.Error("a log line carries session content")
	}
}

// TestBatchLineIsWrittenForFailedRequestsToo: the line is per request, not
// per success. A 503 and a 400 are exactly the requests an operator is
// looking for, and a metric built from the line has to see them.
func TestBatchLineIsWrittenForFailedRequestsToo(t *testing.T) {
	t.Run("store failure", func(t *testing.T) {
		log, buf := jsonLogger()
		st := newMemStore(nil)
		st.upsertErr = errors.New("connection refused")
		h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })
		if w := postAs(t, h, testToken, []spool.Item{mkItem(t, secretEvent("e1", 1))}); w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status %d, want 503", w.Code)
		}
		batches := linesNamed(logLines(t, buf), lineBatch)
		if len(batches) != 1 || batches[0]["status"] != float64(503) || batches[0]["items"] != float64(1) ||
			batches[0]["accepted"] != float64(0) {
			t.Errorf("batch line = %v, want status=503 items=1 accepted=0", batches)
		}
		if strings.Contains(buf.String(), secretFragment) {
			t.Error("a log line carries session content")
		}
	})
	t.Run("malformed body", func(t *testing.T) {
		log, buf := jsonLogger()
		h := newHandler(t, newMemStore(nil), goodDevices(), func(o *Options) { o.Logger = log })
		r := httptest.NewRequest(http.MethodPost, EventsPath, strings.NewReader(`{"text":"`+secretFragment+`"`))
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400", w.Code)
		}
		batches := linesNamed(logLines(t, buf), lineBatch)
		if len(batches) != 1 || batches[0]["status"] != float64(400) || batches[0]["items"] != float64(0) {
			t.Errorf("batch line = %v, want status=400 items=0", batches)
		}
		if n, _ := batches[0]["bytes"].(float64); n <= 0 {
			t.Errorf("batch line bytes = %v, want the bytes read before the parse failed", batches[0]["bytes"])
		}
		if batches[0]["agent_version"] != "" {
			t.Errorf("a request with no loop-sessions User-Agent reports agent_version %q, want empty", batches[0]["agent_version"])
		}
		if strings.Contains(buf.String(), secretFragment) {
			t.Error("a log line carries session content")
		}
	})
	t.Run("no credential", func(t *testing.T) {
		// Nothing to attribute the line to, so there is no line. The request
		// log Cloud Run writes carries the 401.
		log, buf := jsonLogger()
		h := newHandler(t, newMemStore(nil), goodDevices(), func(o *Options) { o.Logger = log })
		if w := post(t, h, EventsPath, "", []byte(`[]`)); w.Code != http.StatusUnauthorized {
			t.Fatalf("status %d, want 401", w.Code)
		}
		if n := len(linesNamed(logLines(t, buf), lineBatch)); n != 0 {
			t.Errorf("an unauthenticated request produced %d batch lines", n)
		}
	})
}

// TestOverCapItemsAreLoggedAsRejected: the over-cap path rejects every item
// without decoding any, and each rejection is still logged, with the batch
// reason and no type, because the batch never got as far as a type.
func TestOverCapItemsAreLoggedAsRejected(t *testing.T) {
	log, buf := jsonLogger()
	h := newHandler(t, newMemStore(nil), goodDevices(), func(o *Options) {
		o.Logger = log
		o.MaxBatchBytes = 64
		o.HardBodyLimit = 1 << 20
	})
	w := postAs(t, h, testToken, []spool.Item{mkItem(t, secretEvent("e1", 1)), mkItem(t, secretEvent("e2", 2))})
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	lines := logLines(t, buf)
	rejected := linesNamed(lines, lineRejected)
	if len(rejected) != 2 {
		t.Fatalf("got %d rejection lines, want 2", len(rejected))
	}
	for _, l := range rejected {
		if l["reason"] != ReasonBatchTooBig || l["event_type"] != "" {
			t.Errorf("over-cap rejection line = %v", l)
		}
	}
	if b := linesNamed(lines, lineBatch); len(b) != 1 || b[0]["rejected"] != float64(2) {
		t.Errorf("batch line = %v, want rejected=2", b)
	}
	if strings.Contains(buf.String(), secretFragment) {
		t.Error("a log line carries session content")
	}
}

// TestAgentVersionIsReadFromTheUserAgent pins the one derivation the fleet
// comparison depends on: "loop-sessions/<build>" and nothing else, with the
// build shaped the way a real one is. The value is a metric label and the
// header is the caller's to write, so a build of two thousand characters, or
// one carrying a path, is dropped the way a curl's User-Agent is.
func TestAgentVersionIsReadFromTheUserAgent(t *testing.T) {
	for _, c := range []struct{ ua, want string }{
		{"loop-sessions/23713ea", "23713ea"},
		{"loop-sessions/dev", "dev"},
		{"loop-sessions/v1.2.3-rc1", "v1.2.3-rc1"},
		{"loop-sessions/abc123 extra", "abc123"},
		{"loop-sessions/" + strings.Repeat("f", 40), strings.Repeat("f", 40)},
		{"loop-sessions/" + strings.Repeat("f", 41), ""},
		{"loop-sessions/" + strings.Repeat("v", 2000) + secretFragment, ""},
		{"loop-sessions/abc/../x", ""},
		{"loop-sessions/", ""},
		{"curl/8.4.0", ""},
		{"", ""},
		{"Loop-Sessions/abc", ""},
		{"prefix loop-sessions/abc123", ""},
	} {
		r := httptest.NewRequest(http.MethodPost, EventsPath, nil)
		if c.ua != "" {
			r.Header.Set("User-Agent", c.ua)
		}
		if got := agentVersion(r); got != c.want {
			t.Errorf("agentVersion(%q) = %q, want %q", c.ua, got, c.want)
		}
	}
}

// TestEventTypeOnLogLinesIsBoundedToTheKnownSet: the payload's type is the
// one client-written string that reaches a log line and a metric label, and
// Validate asks only that it is non-empty. A rejection carrying a made-up
// type and an undecided item carrying a four-kilobyte one are both logged as
// "other", so the "never content" rule holds for a client that is not well
// behaved, and one device cannot mint labels on three metrics.
func TestEventTypeOnLogLinesIsBoundedToTheKnownSet(t *testing.T) {
	t.Run("rejection line", func(t *testing.T) {
		log, buf := jsonLogger()
		h := newHandler(t, newMemStore(nil), goodDevices(), func(o *Options) { o.Logger = log })
		item := rawItem("bad1", "sess-1", `{"id":"bad1","session_id":"sess-1","type":"`+secretFragment+`"}`)
		if w := postAs(t, h, testToken, []spool.Item{item}); w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}
		rejected := linesNamed(logLines(t, buf), lineRejected)
		if len(rejected) != 1 || rejected[0]["event_type"] != "other" || rejected[0]["reason"] != ReasonInvalid {
			t.Errorf("rejection line = %v, want event_type other with reason %q", rejected, ReasonInvalid)
		}
		if strings.Contains(buf.String(), secretFragment) {
			t.Error("a client-chosen type reached the log verbatim")
		}
	})
	t.Run("undecided line", func(t *testing.T) {
		log, buf := jsonLogger()
		h := newHandler(t, silentStore{}, goodDevices(), func(o *Options) { o.Logger = log })
		huge := strings.Repeat("x", 4096) + secretFragment
		item := rawItem("e1", "sess-1", `{"id":"e1","session_id":"sess-1","type":"`+huge+`","source":"claude_code","origin":"hook","occurred_at":"2026-01-01T00:00:00Z"}`)
		if w := postAs(t, h, testToken, []spool.Item{item}); w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		undecided := linesNamed(logLines(t, buf), lineNoVerdict)
		if len(undecided) != 1 || undecided[0]["event_type"] != "other" || undecided[0]["event_id"] != "e1" {
			t.Errorf("undecided line = %v, want event_type other for event e1", undecided)
		}
		if strings.Contains(buf.String(), secretFragment) {
			t.Error("a client-chosen type reached the log verbatim")
		}
	})
	t.Run("the known set passes through", func(t *testing.T) {
		for _, typ := range []string{"", "user_prompt", "tool_result", "session_started", "compaction", "artifact", healthReportType} {
			if got := logEventType(typ); got != typ {
				t.Errorf("logEventType(%q) = %q, want it unchanged", typ, got)
			}
		}
		for _, typ := range []string{"Tool_Result", "tool_result ", "x", "other", secretFragment} {
			if got := logEventType(typ); got != "other" {
				t.Errorf("logEventType(%q) = %q, want other", typ, got)
			}
		}
	})
}

// The ids are the last payload strings a log line carries verbatim, and
// Validate only requires them to be non-empty. An undecided item whose id is
// four kilobytes of transcript must not put that text into an ERROR line.
func TestIDsOnLogLinesAreBoundedToTheirShape(t *testing.T) {
	t.Run("shapes", func(t *testing.T) {
		for _, ok := range []string{"", "0192d1f0a3b14c8e9f0a1b2c3d4e5f60", "9091a504-96bb-4830-a7ea-01666112a016", "rollout-2026-09-10T12-00-00-abc:1"} {
			if got := logID(ok); got != ok {
				t.Errorf("logID(%q) = %q, want it unchanged", ok, got)
			}
		}
		for _, bad := range []string{"has space", secretFragment + " leaked", strings.Repeat("a", 65), "a/b", "x\n"} {
			if got := logID(bad); got != "other" {
				t.Errorf("logID(%q) = %q, want other", bad, got)
			}
		}
	})
	t.Run("undecided line", func(t *testing.T) {
		log, buf := jsonLogger()
		h := newHandler(t, silentStore{}, goodDevices(), func(o *Options) { o.Logger = log })
		huge := strings.Repeat("x", 4096) + " " + secretFragment
		item := rawItem(huge, "sess-1", `{"id":"`+huge+`","session_id":"sess-1","type":"user_prompt","source":"claude_code","origin":"hook","occurred_at":"2026-01-01T00:00:00Z"}`)
		if w := postAs(t, h, testToken, []spool.Item{item}); w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		undecided := linesNamed(logLines(t, buf), lineNoVerdict)
		if len(undecided) != 1 || undecided[0]["event_id"] != "other" {
			t.Errorf("undecided line = %v, want event_id other", undecided)
		}
		if strings.Contains(buf.String(), secretFragment) {
			t.Error("a client-chosen id reached the log verbatim")
		}
	})
}
