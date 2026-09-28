package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/daemon"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

const anchorPrompt = "0deb775f-3848-4adb-9cdc-a246b25137bd"

func anchorFixture(t *testing.T, answerPresent bool) (stateDir, transcript string) {
	t.Helper()
	dir := t.TempDir()
	// The anchorer logs through logf, which writes under the client home;
	// point it at this test's directory so the real agent.log stays clean.
	t.Setenv("LOOP_SESSIONS_HOME", dir)
	stateDir = filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	transcript = filepath.Join(dir, "t.jsonl")
	lines := []string{
		`{"type":"user","sessionId":"s1","timestamp":"2026-08-17T09:59:00.000Z","uuid":"u1","promptId":"` + anchorPrompt + `","message":{"role":"user","content":"go"}}`,
		`{"type":"assistant","sessionId":"s1","timestamp":"2026-08-17T10:00:00.000Z","uuid":"a0","requestId":"req_0","message":{"role":"assistant","model":"claude-fable-5","id":"msg_call","usage":{"input_tokens":100,"output_tokens":20},"content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{}}]}}`,
		`{"type":"user","sessionId":"s1","timestamp":"2026-08-17T10:00:01.000Z","uuid":"u2","promptId":"` + anchorPrompt + `","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"hi"}]}}`,
	}
	if answerPresent {
		lines = append(lines, anchorAnswer)
	}
	body := ""
	for _, l := range lines {
		body += l + "\n"
	}
	if err := os.WriteFile(transcript, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := json.Marshal(daemon.State{SessionID: "s1", TranscriptPath: transcript})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "s1.json"), st, 0o644); err != nil {
		t.Fatal(err)
	}
	return stateDir, transcript
}

const anchorAnswer = `{"type":"assistant","sessionId":"s1","timestamp":"2026-08-17T10:00:02.000Z","uuid":"a1","requestId":"req_1","message":{"role":"assistant","model":"claude-fable-5","id":"msg_7","usage":{"input_tokens":120,"output_tokens":34,"cache_read_input_tokens":5},"content":[{"type":"text","text":"done"}]}}`

// stopPayload is what the hook spools for a Stop whose answer had not reached
// the transcript, plus a field this build does not know and a large number,
// both of which must come out the other side untouched.
const stopPayload = `{"id":"e1","type":"assistant_turn","source":"claude_code","origin":"hook","session_id":"s1","prompt_id":"` + anchorPrompt + `","seq":5,"occurred_at":"2026-08-17T10:00:02.100Z","text":"done","capture_version":4,"raw":{"b":2,"a":1},"zz_unknown":{"big":12345678901234567890}}`

func leasedStop(now time.Time, payload string) []spool.Leased {
	return []spool.Leased{{Item: spool.Item{ID: "e1", Kind: "event", SessionID: "s1", Seq: 5, EventTime: now, Payload: json.RawMessage(payload)}}}
}

func decodePayload(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// The answer lands during the first wait: the copy leaves the spool anchored
// to it, with the rest of the payload untouched.
func TestStopAnchorerFillsTheRecordWhenTheAnswerLands(t *testing.T) {
	stateDir, transcript := anchorFixture(t, false)
	now := time.Date(2026, 8, 17, 10, 0, 2, 200000000, time.UTC)
	var waits []time.Duration
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return now }, recent: 10 * time.Second,
		waits: []time.Duration{50 * time.Millisecond, 100 * time.Millisecond},
		sleep: func(_ context.Context, d time.Duration) {
			waits = append(waits, d)
			f, err := os.OpenFile(transcript, os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteString(anchorAnswer + "\n"); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
		}}
	batch := leasedStop(now, stopPayload)
	a.enrich(context.Background(), batch)
	m := decodePayload(t, batch[0].Item.Payload)
	if m["record_uuid"] != "a1" || m["model"] != "claude-fable-5" {
		t.Errorf("record_uuid=%v model=%v, want a1 / claude-fable-5", m["record_uuid"], m["model"])
	}
	usage, _ := m["usage"].(map[string]any)
	if usage["message_id"] != "msg_7" || usage["request_id"] != "req_1" || usage["input_tokens"] != float64(120) {
		t.Errorf("usage = %v, want msg_7 / req_1 / 120 input tokens", usage)
	}
	if m["text"] != "done" || m["prompt_id"] != anchorPrompt || m["capture_version"] != float64(4) {
		t.Errorf("payload fields disturbed: %v", m)
	}
	if string(batch[0].Item.Payload) == stopPayload || !containsAll(string(batch[0].Item.Payload), `"zz_unknown":{"big":12345678901234567890}`, `"raw":{"b":2,"a":1}`) {
		t.Errorf("an untouched field changed or lost its bytes: %s", batch[0].Item.Payload)
	}
	if len(waits) != 1 || waits[0] != 50*time.Millisecond {
		t.Errorf("waits = %v, want one wait of 50ms", waits)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

// A copy that is no longer recent gets one read and no wait; an answer that
// is not there leaves it as it was.
func TestStopAnchorerDoesNotWaitOnOldCopies(t *testing.T) {
	stateDir, _ := anchorFixture(t, false)
	spooled := time.Date(2026, 8, 17, 10, 0, 2, 0, time.UTC)
	waits := 0
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return spooled.Add(time.Minute) }, recent: 10 * time.Second,
		waits: []time.Duration{50 * time.Millisecond}, sleep: func(context.Context, time.Duration) { waits++ }}
	batch := leasedStop(spooled, stopPayload)
	a.enrich(context.Background(), batch)
	if waits != 0 || string(batch[0].Item.Payload) != stopPayload {
		t.Errorf("waits=%d payload changed=%v; an old copy gets one read and no wait", waits, string(batch[0].Item.Payload) != stopPayload)
	}
}

// An answer already in the file is taken on the first read with no wait.
func TestStopAnchorerTakesAPresentAnswerAtOnce(t *testing.T) {
	stateDir, _ := anchorFixture(t, true)
	now := time.Date(2026, 8, 17, 10, 0, 3, 0, time.UTC)
	waits := 0
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return now }, recent: 10 * time.Second,
		waits: []time.Duration{50 * time.Millisecond}, sleep: func(context.Context, time.Duration) { waits++ }}
	batch := leasedStop(now, stopPayload)
	a.enrich(context.Background(), batch)
	if m := decodePayload(t, batch[0].Item.Payload); m["record_uuid"] != "a1" || waits != 0 {
		t.Errorf("record_uuid=%v waits=%d, want a1 with no wait", m["record_uuid"], waits)
	}
}

// Items that are not an anchorless Claude Code hook Stop copy, or whose
// session has no state file, pass through untouched.
func TestStopAnchorerLeavesOtherItemsAlone(t *testing.T) {
	stateDir, _ := anchorFixture(t, true)
	now := time.Date(2026, 8, 17, 10, 0, 3, 0, time.UTC)
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return now }, recent: 10 * time.Second,
		waits: nil, sleep: func(context.Context, time.Duration) { t.Fatal("slept") }}
	cases := map[string]string{
		"already anchored":  `{"id":"e1","type":"assistant_turn","source":"claude_code","origin":"hook","session_id":"s1","prompt_id":"` + anchorPrompt + `","record_uuid":"a9","text":"done"}`,
		"a transcript copy": `{"id":"e1","type":"assistant_turn","source":"claude_code","origin":"transcript","session_id":"s1","prompt_id":"` + anchorPrompt + `","text":"done"}`,
		"a tool call":       `{"id":"e1","type":"tool_call","source":"claude_code","origin":"hook","session_id":"s1","prompt_id":"` + anchorPrompt + `"}`,
		"a Codex turn":      `{"id":"e1","type":"assistant_turn","source":"codex","origin":"hook","session_id":"s1","text":"done"}`,
		"no state file":     `{"id":"e1","type":"assistant_turn","source":"claude_code","origin":"hook","session_id":"s-unknown","prompt_id":"` + anchorPrompt + `","text":"done"}`,
	}
	for name, payload := range cases {
		batch := leasedStop(now, payload)
		a.enrich(context.Background(), batch)
		if string(batch[0].Item.Payload) != payload {
			t.Errorf("%s: payload changed to %s", name, batch[0].Item.Payload)
		}
	}
	heartbeat := []spool.Leased{{Item: spool.Item{ID: "h1", Kind: "heartbeat", EventTime: now, Payload: json.RawMessage(`{"x":1}`)}}}
	a.enrich(context.Background(), heartbeat)
	if string(heartbeat[0].Item.Payload) != `{"x":1}` {
		t.Error("a heartbeat was touched")
	}
}

// A session whose transcript does not exist (print mode with persistence
// off, or a deleted file) gets no wait at all: nothing can land in it.
func TestStopAnchorerDoesNotWaitOnAMissingTranscript(t *testing.T) {
	stateDir, transcript := anchorFixture(t, false)
	if err := os.Remove(transcript); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 17, 10, 0, 2, 200000000, time.UTC)
	waits := 0
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return now }, recent: 10 * time.Second,
		waits: []time.Duration{50 * time.Millisecond}, sleep: func(context.Context, time.Duration) { waits++ }}
	batch := leasedStop(now, stopPayload)
	a.enrich(context.Background(), batch)
	if waits != 0 || string(batch[0].Item.Payload) != stopPayload {
		t.Errorf("waits=%d changed=%v; a missing transcript must cost no wait", waits, string(batch[0].Item.Payload) != stopPayload)
	}
}

// A promptless copy (a harness that predates prompt_id) takes the tail on
// trust only while it is recent; an old one is left alone.
func TestStopAnchorerDoesNotTrustAStalePromptlessCopy(t *testing.T) {
	stateDir, _ := anchorFixture(t, true)
	spooled := time.Date(2026, 8, 17, 10, 0, 3, 0, time.UTC)
	promptless := `{"id":"e1","type":"assistant_turn","source":"claude_code","origin":"hook","session_id":"s1","text":"done"}`
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return spooled.Add(time.Minute) }, recent: 10 * time.Second,
		waits: nil, sleep: func(context.Context, time.Duration) { t.Fatal("slept") }}
	batch := leasedStop(spooled, promptless)
	a.enrich(context.Background(), batch)
	if string(batch[0].Item.Payload) != promptless {
		t.Errorf("a stale promptless copy was anchored: %s", batch[0].Item.Payload)
	}
	a.now = func() time.Time { return spooled.Add(time.Second) }
	batch = leasedStop(spooled, promptless)
	a.enrich(context.Background(), batch)
	if m := decodePayload(t, batch[0].Item.Payload); m["record_uuid"] != "a1" {
		t.Errorf("a recent promptless copy was not taken on trust: %v", m["record_uuid"])
	}
}

func appendLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lines {
		if _, err := f.WriteString(l + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	_ = f.Close()
}

// One prompt can end in Stop more than once. A copy read after a later
// answer under the same prompt has landed must not take it: the record is
// newer than the copy, so it is another Stop's answer.
func TestStopAnchorerRefusesAnAnswerNewerThanTheCopy(t *testing.T) {
	stateDir, transcript := anchorFixture(t, true) // a1 at 10:00:02
	later := `{"type":"assistant","sessionId":"s1","timestamp":"2026-08-17T10:00:20.000Z","uuid":"a2","requestId":"req_2","message":{"role":"assistant","model":"claude-fable-5","id":"msg_9","usage":{"input_tokens":50,"output_tokens":9},"content":[{"type":"text","text":"and more"}]}}`
	appendLines(t, transcript, later)
	first := time.Date(2026, 8, 17, 10, 0, 5, 0, time.UTC) // the first Stop, between a1 and a2
	waits := 0
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return first.Add(2 * time.Second) }, recent: 10 * time.Second,
		waits: []time.Duration{50 * time.Millisecond}, sleep: func(context.Context, time.Duration) { waits++ }}
	batch := leasedStop(first, stopPayload)
	a.enrich(context.Background(), batch)
	if string(batch[0].Item.Payload) != stopPayload || waits != 0 {
		t.Errorf("the first copy took a later answer (changed=%v waits=%d)", string(batch[0].Item.Payload) != stopPayload, waits)
	}
	second := time.Date(2026, 8, 17, 10, 0, 21, 0, time.UTC) // the second Stop, after a2
	batch = leasedStop(second, stopPayload)
	a.now = func() time.Time { return second.Add(2 * time.Second) }
	a.enrich(context.Background(), batch)
	if m := decodePayload(t, batch[0].Item.Payload); m["record_uuid"] != "a2" {
		t.Errorf("the second copy was not anchored to its own answer a2: %v", m["record_uuid"])
	}
}

// A copy exactly ten seconds old is no longer recent.
func TestStopAnchorerRecentIsStrict(t *testing.T) {
	stateDir, _ := anchorFixture(t, false)
	spooled := time.Date(2026, 8, 17, 10, 0, 2, 0, time.UTC)
	waits := 0
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return spooled.Add(10 * time.Second) }, recent: 10 * time.Second,
		waits: []time.Duration{50 * time.Millisecond}, sleep: func(context.Context, time.Duration) { waits++ }}
	batch := leasedStop(spooled, stopPayload)
	a.enrich(context.Background(), batch)
	if waits != 0 {
		t.Errorf("waited %d times for a copy exactly ten seconds old", waits)
	}
}

// The schedule is spent once per batch: three recent copies whose answers
// never land cost one schedule, not three.
func TestStopAnchorerSpendsTheScheduleOncePerBatch(t *testing.T) {
	stateDir, _ := anchorFixture(t, false)
	now := time.Date(2026, 8, 17, 10, 0, 3, 0, time.UTC)
	var slept time.Duration
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return now }, recent: 10 * time.Second,
		waits: []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}, sleep: func(_ context.Context, d time.Duration) { slept += d }}
	batch := append(append(leasedStop(now, stopPayload), leasedStop(now, stopPayload)...), leasedStop(now, stopPayload)...)
	a.enrich(context.Background(), batch)
	if slept != 150*time.Millisecond {
		t.Errorf("slept %v for three copies, want one schedule of 150ms", slept)
	}
}

// A record with no parseable stamp is refused: nothing says which Stop it
// belongs to, and anchorless is preferred to wrong.
func TestStopAnchorerRefusesAStamplessRecord(t *testing.T) {
	stateDir, transcript := anchorFixture(t, false)
	appendLines(t, transcript, `{"type":"assistant","sessionId":"s1","uuid":"a3","requestId":"req_3","message":{"role":"assistant","model":"claude-fable-5","id":"msg_3","usage":{"input_tokens":5,"output_tokens":6},"content":[{"type":"text","text":"done"}]}}`)
	now := time.Date(2026, 8, 17, 10, 0, 3, 0, time.UTC)
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return now }, recent: 10 * time.Second,
		waits: nil, sleep: func(context.Context, time.Duration) { t.Fatal("slept") }}
	batch := leasedStop(now, stopPayload)
	a.enrich(context.Background(), batch)
	if string(batch[0].Item.Payload) != stopPayload {
		t.Errorf("a stampless record was taken: %s", batch[0].Item.Payload)
	}
}

// A user record under the turn's id after the answer (a teammate message,
// a blocking Stop hook's feedback) is always in the daemon's view; the copy's
// text and time are what say the record is still this turn's answer.
func TestStopAnchorerAcceptsTheAnswerByTextWhenAUserRecordFollowsIt(t *testing.T) {
	stateDir, transcript := anchorFixture(t, true) // a1 at 10:00:02, text "done"
	appendLines(t, transcript, `{"type":"user","sessionId":"s1","timestamp":"2026-08-17T10:00:02.200Z","uuid":"u3","promptId":"`+anchorPrompt+`","message":{"role":"user","content":"Another Claude session sent a message: carry on"}}`)
	now := time.Date(2026, 8, 17, 10, 0, 3, 0, time.UTC)
	a := &stopAnchorer{stateDir: stateDir, now: func() time.Time { return now }, recent: 10 * time.Second,
		waits: nil, sleep: func(context.Context, time.Duration) { t.Fatal("slept") }}
	batch := leasedStop(now, stopPayload) // text "done", spooled 10:00:03
	a.enrich(context.Background(), batch)
	if m := decodePayload(t, batch[0].Item.Payload); m["record_uuid"] != "a1" {
		t.Errorf("the turn's own answer was refused behind a same-prompt user record: %v", m["record_uuid"])
	}
	other := strings.Replace(stopPayload, `"text":"done"`, `"text":"another answer"`, 1)
	batch = leasedStop(now, other)
	a.enrich(context.Background(), batch)
	if string(batch[0].Item.Payload) != other {
		t.Errorf("a copy whose text differs was anchored: %s", batch[0].Item.Payload)
	}
	// A copy made a minute after the record is a later Stop under the same
	// prompt; the record is the previous answer even when the text repeats.
	later := leasedStop(now.Add(time.Minute), stopPayload)
	a.now = func() time.Time { return now.Add(time.Minute + time.Second) }
	a.enrich(context.Background(), later)
	if string(later[0].Item.Payload) != stopPayload {
		t.Errorf("a later copy was anchored to the previous answer: %s", later[0].Item.Payload)
	}
}
