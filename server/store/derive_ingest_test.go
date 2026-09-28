package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// The record shapes are production's (research/evidence/scan*.out and
// hookprobe3.log): a user record with promptId, an assistant record with
// message.id, requestId and two tool_use blocks, and a hook payload, which
// is never read for keys.
const (
	ingestRawUser = `{"parentUuid":"616a3c51-7e6e-41bc-9ebb-bc89a3842107","isSidechain":false,"promptId":"9bacf634-5c61-42e2-bb26-7072f8eaa574","type":"user","message":{"role":"user","content":"Make this artifact public"},"uuid":"d3c7608a-de5a-4258-95cd-cd8f0c6c0d32","timestamp":"2026-08-25T12:54:00.810Z","sessionId":"10db14c9-6373-4c36-a800-db092907faf6","version":"2.1.236","entrypoint":"cli"}`

	ingestRawAssistant = `{"parentUuid":"d3c7608a-de5a-4258-95cd-cd8f0c6c0d32","isSidechain":false,"type":"assistant","uuid":"9f90859f-ba1b-4d9d-8292-5c182e2c831f","timestamp":"2026-08-25T12:54:05.000Z","sessionId":"10db14c9-6373-4c36-a800-db092907faf6","requestId":"req_011CTS6fcv","message":{"id":"msg_01Mk9x","type":"message","role":"assistant","model":"claude-opus-4-1","content":[{"type":"text","text":"Running it."},{"type":"tool_use","id":"toolu_012HH5HfZ73a2TttRtX5xLQU","name":"Bash","input":{"command":"echo probe-one"}},{"type":"tool_use","id":"toolu_01NnMRjoXabzKRRd4B5iVKjn","name":"Bash","input":{"command":"echo probe-two"}}],"usage":{"input_tokens":3,"output_tokens":40}}}`

	ingestRawHook = `{"hook_event_name":"UserPromptSubmit","prompt_id":"72442f84-fe82-4691-a49a-8e194aa00a5d","session_id":"s","uuid":"never-read","prompt":"hello"}`
)

// Argument positions of the key columns in the event insert.
const (
	argPromptID   = 13
	argRecordUUID = 14
	argParentUUID = 15
	argRequestID  = 16
	argMessageID  = 17
	argToolUseID  = 18
)

// Every stored row is keyed from the moment it lands: from the named fields
// when the client stamped them, from the transcript record otherwise, and
// never from a hook payload.
func TestInsertEventsWritesKeyColumnsFromNamedFieldsElseRaw(t *testing.T) {
	db := ingestDB("me@example.com", "h1", "t1", "t2", "t3")

	// An old client's hook prompt: prompt_id as a named field, a hook payload
	// in raw that carries a uuid nobody may read as a record.
	hook := ingestOf("h1", "sess", "me@example.com", event.UserPrompt, 1)
	hook.Event.PromptID = "72442f84-fe82-4691-a49a-8e194aa00a5d"
	hook.Event.Raw = json.RawMessage(ingestRawHook)

	// The walker's copy of a prompt and an answer, keys inside the record.
	user := ingestOf("t1", "sess", "me@example.com", event.UserPrompt, 0)
	user.Event.Origin = event.OriginTranscript
	user.Event.Raw = json.RawMessage(ingestRawUser)
	asst := ingestOf("t2", "sess", "me@example.com", event.AssistantTurn, 1)
	asst.Event.Origin = event.OriginTranscript
	asst.Event.Raw = json.RawMessage(ingestRawAssistant)

	// A tool call split out of that assistant record: its block is found by
	// name and input.
	call := ingestOf("t3", "sess", "me@example.com", event.ToolCall, 2)
	call.Event.Origin = event.OriginTranscript
	call.Event.Raw = json.RawMessage(ingestRawAssistant)
	call.Event.Tool = &event.Tool{Name: "Bash", Input: json.RawMessage(`{"command":"echo probe-two"}`)}

	if _, err := NewWithDB(db, nil).UpsertEvents(t.Context(), []Ingest{hook, user, asst, call}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	insert := db.find(t, sqlEventInsert)
	col := func(pos int) []string { return insert.args[pos].([]string) }
	want := map[string][]string{
		"prompt_id":          {"72442f84-fe82-4691-a49a-8e194aa00a5d", "9bacf634-5c61-42e2-bb26-7072f8eaa574", "", ""},
		"record_uuid":        {"", "d3c7608a-de5a-4258-95cd-cd8f0c6c0d32", "9f90859f-ba1b-4d9d-8292-5c182e2c831f", "9f90859f-ba1b-4d9d-8292-5c182e2c831f"},
		"parent_record_uuid": {"", "", "", ""},
		"request_id":         {"", "", "req_011CTS6fcv", "req_011CTS6fcv"},
		"message_id":         {"", "", "msg_01Mk9x", "msg_01Mk9x"},
		"tool_use_id":        {"", "", "", "toolu_01NnMRjoXabzKRRd4B5iVKjn"},
	}
	for name, pos := range map[string]int{
		"prompt_id": argPromptID, "record_uuid": argRecordUUID, "parent_record_uuid": argParentUUID,
		"request_id": argRequestID, "message_id": argMessageID, "tool_use_id": argToolUseID,
	} {
		if got := col(pos); strings.Join(got, "|") != strings.Join(want[name], "|") {
			t.Errorf("%s = %q, want %q", name, got, want[name])
		}
	}
	for _, colName := range []string{"prompt_id", "record_uuid", "parent_record_uuid", "request_id", "message_id", "tool_use_id"} {
		if !strings.Contains(insert.sql, colName) {
			t.Errorf("the insert does not write %s", colName)
		}
	}
	if strings.Contains(insert.sql, "superseded_by") {
		t.Error("ingest writes superseded_by; that column is the runner's alone")
	}

	// The identity probe is skipped until the runner's index is valid: the
	// catalog answered nothing, so no join against events was issued.
	if n := db.count("coalesce(e.tool_use_id, '') = u.tool_use_id"); n != 0 {
		t.Errorf("the identity join ran %d times with no valid index; that is a heap walk per batch", n)
	}
}

// Once the index is valid the probe runs, once per batch, over the
// transcript rows that have a record identity and nothing else.
func TestRecordIdentityIsProbedOnlyForTranscriptRowsOnceTheIndexIsValid(t *testing.T) {
	db := ingestDB("me@example.com", "h1", "t1")
	db.stubs = append(db.stubs, &stub{match: "indisvalid", rows: [][]any{{true}}})

	hook := ingestOf("h1", "sess", "me@example.com", event.UserPrompt, 1)
	hook.Event.PromptID = "pid"
	user := ingestOf("t1", "sess", "me@example.com", event.UserPrompt, 0)
	user.Event.Origin = event.OriginTranscript
	user.Event.Raw = json.RawMessage(ingestRawUser)

	s := NewWithDB(db, nil)
	if _, err := s.UpsertEvents(t.Context(), []Ingest{hook, user}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	probe := db.find(t, "coalesce(e.tool_use_id, '') = u.tool_use_id")
	records := probe.args[3].([]string)
	if len(records) != 1 || records[0] != "d3c7608a-de5a-4258-95cd-cd8f0c6c0d32" {
		t.Errorf("probed records = %v, want only the transcript row's uuid", records)
	}
	if !strings.Contains(probe.sql, "WHERE e.origin = 'transcript'") {
		t.Errorf("the probe does not restrict itself to transcript rows:\n%s", probe.sql)
	}

	// The answer is cached: a second batch does not ask the catalog again.
	if _, err := s.UpsertEvents(t.Context(), []Ingest{user}); err != nil {
		t.Fatalf("second batch: %v", err)
	}
	if n := db.count("indisvalid"); n != 1 {
		t.Errorf("the catalog was asked %d times about the index, want once", n)
	}
}

// The event_keys cursor round-trips, and its separator is valid UTF-8: a
// NUL there is what Postgres refuses in a TEXT column, and the first
// rehearsal of the step failed on exactly that.
func TestEventKeysCursorRoundTripsAsValidText(t *testing.T) {
	c := encodeKeysCursor("10db14c9-6373-4c36-a800-db092907faf6", 41, "abc123")
	if !utf8.ValidString(c) || strings.ContainsRune(c, 0) {
		t.Fatalf("cursor %q is not storable text", c)
	}
	sid, seq, id := decodeKeysCursor(c)
	if sid != "10db14c9-6373-4c36-a800-db092907faf6" || seq != 41 || id != "abc123" {
		t.Errorf("decoded (%q, %d, %q)", sid, seq, id)
	}
	if sid, seq, id := decodeKeysCursor(""); sid != "" || seq != -1 || id != "" {
		t.Errorf("the empty cursor decodes to (%q, %d, %q), want the start", sid, seq, id)
	}
	if sid, seq, _ := decodeKeysCursor("garbage"); sid != "" || seq != -1 {
		t.Errorf("a malformed cursor decodes to (%q, %d), want the start rather than a guess", sid, seq)
	}
}

// Until the runner has built events_record_identity_idx, ingest cannot
// probe record identity, and the server says so once per boot at WARNING
// (review-1 finding 6): the interval is the rollout constraint under which
// re-walks store second rows. A failed identity join forgets the positive
// answer, so an index dropped behind the cache is noticed on the next batch
// rather than trusted for the life of the process (finding 5).
func TestTheMissingIdentityIndexIsWarnedAboutOncePerBoot(t *testing.T) {
	db := ingestDB("me@example.com", "t1")
	user := ingestOf("t1", "sess", "me@example.com", event.UserPrompt, 0)
	user.Event.Origin = event.OriginTranscript
	user.Event.Raw = json.RawMessage(ingestRawUser)

	var out bytes.Buffer
	s := NewWithDB(db, nil)
	s.SetLogger(slog.New(slog.NewJSONHandler(&out, nil)))
	for i := 0; i < 3; i++ {
		if _, err := s.UpsertEvents(t.Context(), []Ingest{user}); err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	if n := strings.Count(out.String(), `"record identity index not ready"`); n != 1 {
		t.Errorf("%d warnings over three batches, want one per boot:\n%s", n, out.String())
	}
	if !strings.Contains(out.String(), `"level":"WARN"`) || !strings.Contains(out.String(), `"state":"missing"`) {
		t.Errorf("the warning is not at WARN or does not say the index is missing:\n%s", out.String())
	}
	if db.count("coalesce(e.tool_use_id, '') = u.tool_use_id") != 0 {
		t.Error("the identity join ran without the index")
	}

	// The index appears and the join fails: the cache is dropped and the
	// catalog is asked again on the next batch.
	s.identityIndexInvalidate()
	db.stubs = append(db.stubs,
		&stub{match: "indisvalid", rows: [][]any{{true}}},
		&stub{match: "coalesce(e.tool_use_id, '') = u.tool_use_id", err: errors.New("index gone")})
	if _, err := s.UpsertEvents(t.Context(), []Ingest{user}); err == nil || !strings.Contains(err.Error(), "index gone") {
		t.Fatalf("batch with a failing join returned %v, want the join's error", err)
	}
	s.identityIdx.mu.Lock()
	ready := s.identityIdx.ready
	s.identityIdx.mu.Unlock()
	if ready {
		t.Error("a failed identity join left the index cached as ready")
	}
	asked := db.count("indisvalid")
	if _, err := s.UpsertEvents(t.Context(), []Ingest{user}); err == nil {
		t.Fatal("the failing join did not fail the next batch")
	}
	if db.count("indisvalid") != asked+1 {
		t.Errorf("the catalog was not asked again after the join failed")
	}
}
