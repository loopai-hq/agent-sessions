//go:build integration

package store

// The derive layer against a real Postgres: record identity at ingest, the
// ledger keyed on the model call, the fold of a dual-origin session into
// turns, the superseded marker's one-way hook-only invariant and the reads
// that honour it, the fork prefix flag, and the session rules the runner
// applies to history. The runner's own mechanics (cursor, lock, window) are
// in runner_integration_test.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

const dvEmail = "derive@example.com"

var dv0 = time.Date(2026, 9, 1, 9, 0, 0, 0, time.UTC)

// The transcript record shapes the walker stores in body.raw, minimal: the
// uuid and promptId of a user record, the uuid, message id and request id of
// an assistant record.
func userRecord(uuid, promptID, text string) string {
	return fmt.Sprintf(`{"type":"user","uuid":%q,"promptId":%q,"message":{"role":"user","content":%q}}`, uuid, promptID, text)
}

func assistantRecord(uuid, messageID, text string) string {
	return fmt.Sprintf(`{"type":"assistant","uuid":%q,"requestId":"req_%s","message":{"id":%q,"role":"assistant","content":[{"type":"text","text":%q}]}}`, uuid, uuid[:4], messageID, text)
}

// transcriptAt builds one transcript-origin event carrying its record.
func transcriptAt(id, sid string, typ event.Type, seq int64, at time.Time, text, raw string) Ingest {
	in := hookAt(id, sid, dvEmail, typ, seq, at, text)
	in.Event.Origin = event.OriginTranscript
	in.Event.CaptureVersion = 3
	if raw != "" {
		in.Event.Raw = json.RawMessage(raw)
	}
	return in
}

// buildIdentityIndex builds the runner's identity index the way a test can
// afford to (not CONCURRENTLY), before the store's first ingest, so the
// store's one-minute negative cache never sees the index missing.
func buildIdentityIndex(t *testing.T) {
	t.Helper()
	for _, ix := range deriveIndexes {
		ddl := strings.Replace(ix.ddl, "CONCURRENTLY ", "", 1)
		if _, err := pool.Exec(context.Background(), ddl); err != nil {
			t.Fatalf("build %s: %v", ix.name, err)
		}
	}
}

// resetDerived puts the runner's ledger back to "nothing ran for this
// version": fresh() truncates the data tables but not derive_jobs or the
// version stamp, and a test of the runner needs both at the start.
func resetDerived(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DELETE FROM derive_jobs`); err != nil {
		t.Fatalf("reset derive_jobs: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE derived_schema SET version = 2 WHERE only_row`); err != nil {
		t.Fatalf("reset derived_schema: %v", err)
	}
}

func countRows(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// turnRow is what a test reads back from turns.
type turnRow struct {
	Thread, Key, Kind, Prompt, Final, Outcome string
	Inherited                                 bool
	Origins                                   []string
	Merged, Prompts, ToolCalls, Errors        int
	TokensInput                               int64
}

func turnsOf(t *testing.T, sid string) []turnRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT thread, turn_key, kind, coalesce(prompt_event_id, ''), coalesce(final_event_id, ''), outcome,
		       inherited, origins, merged, prompts, tool_calls, errors, tokens_input
		FROM turns WHERE session_id = $1 ORDER BY thread, turn_index`, sid)
	if err != nil {
		t.Fatalf("read turns: %v", err)
	}
	defer rows.Close()
	var out []turnRow
	for rows.Next() {
		var r turnRow
		if err := rows.Scan(&r.Thread, &r.Key, &r.Kind, &r.Prompt, &r.Final, &r.Outcome,
			&r.Inherited, &r.Origins, &r.Merged, &r.Prompts, &r.ToolCalls, &r.Errors, &r.TokensInput); err != nil {
			t.Fatalf("scan turn: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// dualOriginSession ingests one session captured both live and from its
// transcript: n prompts, each with a hook copy and a transcript copy, a tool
// call and result on both paths, and an answer on both paths sharing a
// message id. It returns the ids of the transcript prompts.
func dualOriginSession(t *testing.T, s *Store, sid string, at time.Time, n int) []string {
	t.Helper()
	var batch []Ingest
	var prompts []string
	batch = append(batch, hookAt(sid+"-start", sid, dvEmail, event.SessionStarted, 1, at, "startup"))
	seq := int64(2)
	for i := range n {
		base := at.Add(time.Duration(i) * 10 * time.Minute)
		pid := fmt.Sprintf("%s-pid-%d", sid, i)
		text := fmt.Sprintf("ask number %d", i)
		mid := fmt.Sprintf("msg_%s_%d", sid, i)
		tu := fmt.Sprintf("toolu_%s_%d", sid, i)

		hp := hookAt(fmt.Sprintf("%s-h-prompt-%d", sid, i), sid, dvEmail, event.UserPrompt, seq, base, text)
		hp.Event.PromptID = pid
		tp := transcriptAt(fmt.Sprintf("%s-t-prompt-%d", sid, i), sid, event.UserPrompt, int64(i*4), base.Add(time.Second), text,
			userRecord(fmt.Sprintf("uuid-%s-p%d", sid, i), pid, text))
		prompts = append(prompts, tp.Event.ID)

		hc := hookAt(fmt.Sprintf("%s-h-call-%d", sid, i), sid, dvEmail, event.ToolCall, seq+1, base.Add(5*time.Second), "")
		hc.Event.PromptID, hc.Event.ToolUseID = pid, tu
		hc.Event.Tool = &event.Tool{Name: "Bash", Input: json.RawMessage(`{"command":"true"}`)}
		tc := transcriptAt(fmt.Sprintf("%s-t-call-%d", sid, i), sid, event.ToolCall, int64(i*4+1), base.Add(5*time.Second), "", "")
		tc.Event.ToolUseID = tu
		tc.Event.Tool = &event.Tool{Name: "Bash", Input: json.RawMessage(`{"command":"true"}`)}

		hr := hookAt(fmt.Sprintf("%s-h-result-%d", sid, i), sid, dvEmail, event.ToolResult, seq+2, base.Add(6*time.Second), "ok")
		hr.Event.PromptID, hr.Event.ToolUseID = pid, tu
		hr.Event.Tool = &event.Tool{Name: "Bash", Output: "ok"}
		tr := transcriptAt(fmt.Sprintf("%s-t-result-%d", sid, i), sid, event.ToolResult, int64(i*4+2), base.Add(6*time.Second), "ok", "")
		tr.Event.ToolUseID = tu
		tr.Event.Tool = &event.Tool{Name: "Bash", Output: "ok"}

		answer := fmt.Sprintf("done with %d", i)
		ha := withUsage(hookAt(fmt.Sprintf("%s-h-answer-%d", sid, i), sid, dvEmail, event.AssistantTurn, seq+3, base.Add(20*time.Second), answer),
			"claude-opus-5", mid, "", 100, 10)
		ha.Event.PromptID = pid
		ta := withUsage(transcriptAt(fmt.Sprintf("%s-t-answer-%d", sid, i), sid, event.AssistantTurn, int64(i*4+3), base.Add(21*time.Second), answer,
			assistantRecord(fmt.Sprintf("uuid-%s-a%d", sid, i), mid, answer)),
			"claude-opus-5", mid, "", 100, 10)
		batch = append(batch, hp, tp, hc, tc, hr, tr, ha, ta)
		seq += 4
	}
	ingestBatch(t, s, batch...)
	return prompts
}

func deriveAll(t *testing.T, s *Store, now time.Time) DeriveDirtyResult {
	t.Helper()
	res, err := s.DeriveDirty(context.Background(), DeriveConfig{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("DeriveDirty: %v", err)
	}
	return res
}

// ---------------------------------------------------------------------------
// Record identity (criterion b)
// ---------------------------------------------------------------------------

// A transcript event's id is a hash of its walk-order position, so a
// re-walk that emits one more event names every later record by a new id.
// The record's uuid does not move, and it is what identifies the row: the
// re-walked copy upgrades the stored row when its extraction is newer, is
// acknowledged as a duplicate either way, and never becomes a second row.
func TestIntegrationARewalkedTranscriptRowWithANewIDIsOneRow(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-rewalk"

	first := []Ingest{
		transcriptAt("w1-0", sid, event.UserPrompt, 0, dv0, "hello", userRecord("u-p1", "pid-1", "hello")),
		transcriptAt("w1-1", sid, event.AssistantTurn, 1, dv0.Add(time.Second), "hi", assistantRecord("u-a1", "msg_1", "hi")),
		transcriptAt("w1-2", sid, event.UserPrompt, 2, dv0.Add(2*time.Second), "more", userRecord("u-p2", "pid-2", "more")),
	}
	ingestBatch(t, s, first...)
	if got := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); got != 3 {
		t.Fatalf("stored %d rows, want 3", got)
	}
	if got := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1 AND record_uuid IS NOT NULL AND prompt_id IS NOT NULL AND type = 'user_prompt'`, sid); got != 2 {
		t.Errorf("%d prompt rows carry record_uuid and prompt_id from raw, want 2", got)
	}

	// The re-walk: a new record at the head shifts every position, so every
	// id changes; the extraction is newer (capture 4) and the text of one
	// record reads differently.
	rewalk := []Ingest{
		transcriptAt("w2-0", sid, event.SessionStarted, 0, dv0.Add(-time.Second), "", `{"type":"system","uuid":"u-s0"}`),
		transcriptAt("w2-1", sid, event.UserPrompt, 1, dv0, "hello", userRecord("u-p1", "pid-1", "hello")),
		transcriptAt("w2-2", sid, event.AssistantTurn, 2, dv0.Add(time.Second), "hi (re-extracted)", assistantRecord("u-a1", "msg_1", "hi (re-extracted)")),
		transcriptAt("w2-3", sid, event.UserPrompt, 3, dv0.Add(2*time.Second), "more", userRecord("u-p2", "pid-2", "more")),
	}
	for i := range rewalk {
		rewalk[i].Event.CaptureVersion = 4
	}
	res, err := s.UpsertEvents(ctx, rewalk)
	if err != nil {
		t.Fatalf("re-walk: %v", err)
	}
	if !reflect.DeepEqual(res.Inserted, []string{"w2-0"}) {
		t.Errorf("inserted = %v, want only the new head record", res.Inserted)
	}
	if !reflect.DeepEqual(res.Duplicate, []string{"w2-1", "w2-2", "w2-3"}) {
		t.Errorf("duplicate = %v, want the three re-walked copies acked as duplicates", res.Duplicate)
	}
	if got := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); got != 4 {
		t.Errorf("stored %d rows after the re-walk, want 4: the re-walked copies must not become second rows", got)
	}
	var text string
	var version int
	if err := pool.QueryRow(ctx, `SELECT body->>'text', capture_version FROM events WHERE id = 'w1-1'`).Scan(&text, &version); err != nil {
		t.Fatal(err)
	}
	if text != "hi (re-extracted)" || version != 4 {
		t.Errorf("stored row w1-1 = %q at capture %d, want the re-walked body at capture 4", text, version)
	}

	// The same walk again, at the same capture version: duplicates, no
	// upgrade, no new row.
	if _, err := pool.Exec(ctx, `UPDATE events SET body = jsonb_set(body, '{text}', '"stale"') WHERE id = 'w1-1'`); err != nil {
		t.Fatal(err)
	}
	res, err = s.UpsertEvents(ctx, rewalk)
	if err != nil {
		t.Fatalf("re-walk again: %v", err)
	}
	if len(res.Inserted) != 0 || len(res.Duplicate) != 4 {
		t.Errorf("second re-walk: inserted %v duplicate %v", res.Inserted, res.Duplicate)
	}
	if err := pool.QueryRow(ctx, `SELECT body->>'text' FROM events WHERE id = 'w1-1'`).Scan(&text); err != nil {
		t.Fatal(err)
	}
	if text != "stale" {
		t.Errorf("a same-version re-walk rewrote the body to %q", text)
	}
	if got := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); got != 4 {
		t.Errorf("stored %d rows after the second re-walk, want 4", got)
	}

	// A batch that only upgrades stored rows by identity (a newer extraction
	// of a session already derived) has to reach the fold and the search
	// corpus (review-1 finding 10): the session is dirty again and the
	// messages row carries the new text.
	deriveAll(t, s, dv0.Add(time.Hour))
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = $1 AND derive_dirty`, sid); n != 0 {
		t.Fatalf("the session is dirty before the upgrade-only batch")
	}
	upgradeOnly := []Ingest{
		transcriptAt("w3-2", sid, event.AssistantTurn, 2, dv0.Add(time.Second), "hi (third extraction)", assistantRecord("u-a1", "msg_1", "hi (third extraction)")),
	}
	upgradeOnly[0].Event.CaptureVersion = 5
	res, err = s.UpsertEvents(ctx, upgradeOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 0 || len(res.Duplicate) != 1 {
		t.Fatalf("upgrade-only batch verdict = %+v", res)
	}
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = $1 AND derive_dirty`, sid); n != 1 {
		t.Errorf("an upgrade-only batch left the session clean; the fold and the title keep the old extraction")
	}
	var msgText string
	if err := pool.QueryRow(ctx, `SELECT text FROM messages WHERE event_id = 'w1-1'`).Scan(&msgText); err != nil {
		t.Fatal(err)
	}
	if msgText != "hi (third extraction)" {
		t.Errorf("messages.text for the upgraded row = %q, want the new extraction", msgText)
	}
}

// ---------------------------------------------------------------------------
// Usage crediting (criterion c)
// ---------------------------------------------------------------------------

// The ledger's key is the message id alone, request_id is always ” at
// insert, and every admitted row is offered to it: a Codex answer stored
// without usage and re-walked with it is priced once, against the stored
// row; a re-delivery credits nothing; a hook Stop and its transcript twin
// share one message id and credit once.
func TestIntegrationUsageIsCreditedOnceAcrossUpgradesRedeliveriesAndTwins(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, flatPricer{perToken: 0.001})
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()

	tokensOf := func(sid string) (int64, float64) {
		var in int64
		var cost float64
		if err := pool.QueryRow(ctx, `SELECT tokens_input, cost_usd::float8 FROM sessions WHERE session_id = $1`, sid).Scan(&in, &cost); err != nil {
			t.Fatalf("read tokens of %s: %v", sid, err)
		}
		return in, cost
	}

	// Codex: the first walk could not read token_count records, the second
	// can. The transcript record carries no message id, so the ledger key
	// falls back to the event id, and that id must be the stored row's.
	const codex = "s-codex"
	answer := transcriptAt("cx-1", codex, event.AssistantTurn, 1, dv0, "sure", `{"type":"assistant","uuid":"u-cx-1"}`)
	answer.Event.Source = event.SourceCodex
	answer.Event.Model = "gpt-5.4"
	ingestBatch(t, s, transcriptAt("cx-0", codex, event.UserPrompt, 0, dv0.Add(-time.Second), "do it", userRecord("u-cx-0", "", "do it")), answer)
	if in, _ := tokensOf(codex); in != 0 {
		t.Fatalf("tokens before the repair walk = %d, want 0", in)
	}
	repaired := answer
	repaired.Event.ID = "cx-1-rewalk"
	repaired.Event.CaptureVersion = 4
	repaired.Event.Usage = &event.Usage{InputTokens: 300, OutputTokens: 30}
	res, err := s.UpsertEvents(ctx, []Ingest{repaired})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 0 || !reflect.DeepEqual(res.Duplicate, []string{"cx-1-rewalk"}) {
		t.Errorf("repair walk verdict = %+v, want the copy acked as a duplicate of the stored row", res)
	}
	if in, cost := tokensOf(codex); in != 300 || cost == 0 {
		t.Errorf("after the repair walk tokens_input = %d cost = %v, want 300 and a cost", in, cost)
	}
	var ledgerEvent string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM usage_ledger WHERE session_id = $1`, codex).Scan(&ledgerEvent); err != nil {
		t.Fatalf("one ledger row for the Codex session: %v", err)
	}
	if ledgerEvent != "cx-1" {
		t.Errorf("ledger names event %q, want the stored row cx-1 so the turn can be priced through it", ledgerEvent)
	}
	// The same repair walk again credits nothing.
	if _, err := s.UpsertEvents(ctx, []Ingest{repaired}); err != nil {
		t.Fatal(err)
	}
	if in, _ := tokensOf(codex); in != 300 {
		t.Errorf("a re-delivery of the repair walk moved tokens_input to %d", in)
	}
	if n := countRows(t, `SELECT count(*) FROM usage_ledger WHERE session_id = $1`, codex); n != 1 {
		t.Errorf("%d ledger rows for one call, want 1", n)
	}

	// A hook Stop and its transcript twin: one message id, two event ids,
	// two batches, one credit; and request_id is '' on the ledger row
	// whatever the client stamped.
	const twin = "s-twin"
	hook := withUsage(hookAt("tw-h", twin, dvEmail, event.AssistantTurn, 2, dv0, "answer"), "claude-opus-5", "msg_tw", "req_tw", 50, 5)
	transcript := withUsage(transcriptAt("tw-t", twin, event.AssistantTurn, 1, dv0.Add(time.Second), "answer",
		assistantRecord("u-tw-a", "msg_tw", "answer")), "claude-opus-5", "msg_tw", "req_tw", 50, 5)
	ingestBatch(t, s, hookAt("tw-p", twin, dvEmail, event.UserPrompt, 1, dv0.Add(-time.Second), "ask"), hook)
	ingestBatch(t, s, transcript)
	if in, _ := tokensOf(twin); in != 50 {
		t.Errorf("twin session tokens_input = %d, want the one call's 50", in)
	}
	var requestID string
	if err := pool.QueryRow(ctx, `SELECT request_id FROM usage_ledger WHERE message_id = 'msg_tw'`).Scan(&requestID); err != nil {
		t.Fatalf("ledger row for msg_tw: %v", err)
	}
	if requestID != "" {
		t.Errorf("ledger request_id = %q, want '' so a later stamped value cannot make a second key", requestID)
	}
	if n := countRows(t, `SELECT count(*) FROM usage_ledger WHERE session_id = $1`, twin); n != 1 {
		t.Errorf("%d ledger rows for the twin session, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Turns, superseded, reads (criteria e, g)
// ---------------------------------------------------------------------------

func TestIntegrationADualOriginSessionFoldsToOneTurnPerPromptAndReadsOnce(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, flatPricer{perToken: 0.001})
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	v := Viewer{Email: dvEmail, Role: RoleMember}
	const sid = "s-dual"

	prompts := dualOriginSession(t, s, sid, dv0, 2)
	before := loadSession(t, s, sid)
	if before.UserTurns != 4 || before.ToolCalls != 4 {
		t.Fatalf("ingest counted user_turns %d tool_calls %d; the additive rollup double counts until the fold, want 4 and 4", before.UserTurns, before.ToolCalls)
	}
	// The bundle the resume endpoint rebuilds: every transcript row, in seq
	// order, body and all. Read before the fold, compared after.
	bundle := func() []StoredEvent {
		page, err := s.GetEvents(ctx, v, sid, EventRange{Limit: 100})
		if err != nil {
			t.Fatalf("GetEvents: %v", err)
		}
		var out []StoredEvent
		for _, e := range page.Events {
			if e.Origin == string(event.OriginTranscript) {
				e.IngestedAt = time.Time{}
				out = append(out, e)
			}
		}
		return out
	}
	bundleBefore := bundle()

	res := deriveAll(t, s, dv0.Add(time.Hour))
	if res.Folded != 1 {
		t.Fatalf("dirty pass = %+v, want the one session folded", res)
	}

	turns := turnsOf(t, sid)
	if len(turns) != 2 {
		t.Fatalf("%d turns, want 2: %+v", len(turns), turns)
	}
	for i, tr := range turns {
		if tr.Key != fmt.Sprintf("pid:%s-pid-%d", sid, i) || tr.Thread != "" {
			t.Errorf("turn %d key = %q thread %q", i, tr.Key, tr.Thread)
		}
		if tr.Prompt != prompts[i] || !strings.HasPrefix(tr.Final, sid+"-t-answer-") {
			t.Errorf("turn %d prompt %q final %q, want the transcript copies", i, tr.Prompt, tr.Final)
		}
		if !reflect.DeepEqual(tr.Origins, []string{"hook", "transcript"}) || tr.Merged != 2 || tr.Prompts != 1 {
			t.Errorf("turn %d origins %v merged %d prompts %d", i, tr.Origins, tr.Merged, tr.Prompts)
		}
		if tr.ToolCalls != 1 || tr.Outcome != "answered" || tr.Kind != "human" {
			t.Errorf("turn %d tool_calls %d outcome %s kind %s", i, tr.ToolCalls, tr.Outcome, tr.Kind)
		}
		if tr.TokensInput != 100 {
			t.Errorf("turn %d tokens_input = %d, want the one call's 100", i, tr.TokensInput)
		}
	}

	// The marker: on the hook prompt and the hook answer of each turn, and on
	// nothing else; never on a transcript row.
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1 AND superseded_by IS NOT NULL`, sid); n != 4 {
		t.Errorf("%d superseded rows, want the 2 hook prompts and 2 hook answers", n)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1 AND superseded_by IS NOT NULL AND origin <> 'hook'`, sid); n != 0 {
		t.Errorf("%d non-hook rows carry superseded_by", n)
	}
	if n := countRows(t, `SELECT count(*) FROM events e WHERE e.session_id = $1 AND e.superseded_by IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM events c WHERE c.id = e.superseded_by AND c.origin = 'transcript')`, sid); n != 0 {
		t.Errorf("%d superseded rows name something other than a transcript row", n)
	}
	// Tool output: hook canonical, expressed in the mapping, never on the
	// row.
	if n := countRows(t, `SELECT count(*) FROM turn_events WHERE session_id = $1 AND role = 'superseded' AND event_id LIKE '%-t-call-%'`, sid); n != 2 {
		t.Errorf("%d transcript tool calls elected out in turn_events, want 2", n)
	}

	// The counters, once each. content_events too (review-1 finding 13):
	// ingest added both origins' copies (16); the recount from the turns is
	// one per logical event, the superseded copies left out: 2 prompts, 2
	// tool calls, 2 tool results, 2 answers.
	after := loadSession(t, s, sid)
	if after.UserTurns != 2 || after.ToolCalls != 2 || after.Errors != 0 {
		t.Errorf("after the fold user_turns %d tool_calls %d errors %d, want 2 2 0", after.UserTurns, after.ToolCalls, after.Errors)
	}
	if got := latticeOf(t, s, sid); got.Human != 2 || got.Content != 8 {
		t.Errorf("human_turns = %d content_events = %d, want 2 and 8", got.Human, got.Content)
	}

	// Reads: 17 rows stored, 4 superseded.
	page, err := s.GetTimeline(ctx, v, sid, TimelineRange{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 13 {
		t.Errorf("timeline default = %d rows, want 13 (17 stored minus 4 superseded hook copies)", len(page.Events))
	}
	page, err = s.GetTimeline(ctx, v, sid, TimelineRange{Limit: 100, IncludeSuperseded: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 17 {
		t.Errorf("timeline with superseded = %d rows, want 17", len(page.Events))
	}
	page, err = s.GetEvents(ctx, v, sid, EventRange{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 13 {
		t.Errorf("events default = %d rows, want 13", len(page.Events))
	}
	page, err = s.GetEvents(ctx, v, sid, EventRange{Limit: 100, IncludeSuperseded: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 17 {
		t.Errorf("events with superseded = %d rows, want 17", len(page.Events))
	}
	if got := bundle(); !reflect.DeepEqual(got, bundleBefore) {
		t.Errorf("the resume bundle changed across the fold:\nbefore %+v\nafter  %+v", bundleBefore, got)
	}

	// The invariant the fold relies on is a constraint, by name.
	if n := countRows(t, `SELECT count(*) FROM pg_constraint WHERE conrelid = 'turns'::regclass AND contype = 'u'`); n != 1 {
		t.Errorf("%d UNIQUE constraints on turns, want the (session_id, thread, turn_key) one", n)
	}

	// Nothing touched the session while it folded, so it is clean; a second
	// pass has nothing to do and changes nothing.
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = $1 AND derive_dirty`, sid); n != 0 {
		t.Errorf("the session is still dirty after a fold nothing interrupted")
	}
	if res := deriveAll(t, s, dv0.Add(2*time.Hour)); res.Folded != 0 {
		t.Errorf("second dirty pass folded %d sessions, want 0", res.Folded)
	}
	if got := turnsOf(t, sid); !reflect.DeepEqual(got, turns) {
		t.Errorf("turns changed across an idle pass")
	}
}

// The fleet's case: hook prompts with no prompt id beside keyed transcript
// copies. Two seconds apart and the same text is one turn, keyed by the
// transcript's id; two different prompts a second apart are two turns.
func TestIntegrationOldHookPromptsPairWithKeyedTranscriptCopies(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	const sid = "s-oldhook"
	ingestBatch(t, s,
		hookAt("oh-1", sid, dvEmail, event.UserPrompt, 1, dv0, "fix the bug"),
		transcriptAt("ot-1", sid, event.UserPrompt, 0, dv0.Add(2*time.Second), "fix the bug", userRecord("u-oh-1", "pid-fix", "fix the bug")),
		hookAt("oh-2", sid, dvEmail, event.UserPrompt, 2, dv0.Add(time.Minute), "now the tests"),
		hookAt("oh-3", sid, dvEmail, event.UserPrompt, 3, dv0.Add(time.Minute+time.Second), "and the docs"),
	)
	deriveAll(t, s, dv0.Add(time.Hour))
	turns := turnsOf(t, sid)
	if len(turns) != 3 {
		t.Fatalf("%d turns, want 3: %+v", len(turns), turns)
	}
	if turns[0].Key != "pid:pid-fix" || turns[0].Prompt != "ot-1" || turns[0].Merged != 1 {
		t.Errorf("paired turn = %+v, want key pid:pid-fix, the transcript prompt canonical, one merge", turns[0])
	}
	if !strings.HasPrefix(turns[1].Key, "ts:") || !strings.HasPrefix(turns[2].Key, "ts:") || turns[1].Key == turns[2].Key {
		t.Errorf("distinct prompts keyed %q and %q, want two distinct ts: keys", turns[1].Key, turns[2].Key)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'oh-1' AND superseded_by = 'ot-1'`); n != 1 {
		t.Errorf("the old hook prompt is not superseded by its transcript twin")
	}
	if got := loadSession(t, s, sid); got.UserTurns != 3 {
		t.Errorf("user_turns = %d, want 3", got.UserTurns)
	}
}

// The marker's SQL refuses any origin but hook, whatever the fold said.
func TestIntegrationMarkSupersededRefusesTranscriptRows(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	const sid = "s-refuse"
	ingestBatch(t, s,
		hookAt("rf-h", sid, dvEmail, event.UserPrompt, 1, dv0, "hello"),
		transcriptAt("rf-t", sid, event.UserPrompt, 0, dv0.Add(time.Second), "hello", userRecord("u-rf", "pid-rf", "hello")),
	)
	if err := markSuperseded(context.Background(), s.db, sid, map[string]string{"rf-t": "rf-h", "rf-h": "rf-t"}); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'rf-t' AND superseded_by IS NOT NULL`); n != 0 {
		t.Error("a transcript row was marked superseded")
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'rf-h' AND superseded_by = 'rf-t'`); n != 1 {
		t.Error("the hook row was not marked")
	}
}

// Within one fold the runner owns the session's markers (contract e as
// amended after review-1 finding 15): a hook row paired by an earlier fold
// and not paired by this one has its marker cleared, so a first-boot fold
// over unkeyed history cannot hide a hook row behind a twin that is not its
// twin forever. Nothing but a fold of the same session moves a marker, and
// no transcript row ever carries one.
func TestIntegrationAFoldClearsTheMarkersItNoLongerNames(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-unpair"
	ingestBatch(t, s,
		hookAt("up-h", sid, dvEmail, event.UserPrompt, 1, dv0, "fix the bug"),
		transcriptAt("up-t", sid, event.UserPrompt, 0, dv0.Add(2*time.Second), "fix the bug", userRecord("u-up", "pid-up", "fix the bug")),
	)
	deriveAll(t, s, dv0.Add(time.Hour))
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'up-h' AND superseded_by = 'up-t'`); n != 1 {
		t.Fatal("the hook prompt was not paired with its transcript twin")
	}
	// The transcript copy turns out to be a different prompt: outside the
	// window, no longer a twin. The next fold must not leave the old marker.
	if _, err := pool.Exec(ctx, `UPDATE events SET occurred_at = $1 WHERE id = 'up-t'`, dv0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE sessions SET derive_dirty = true WHERE session_id = $1`, sid); err != nil {
		t.Fatal(err)
	}
	deriveAll(t, s, dv0.Add(2*time.Hour))
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'up-h' AND superseded_by IS NOT NULL`); n != 0 {
		t.Errorf("the hook prompt keeps a marker naming a row that is no longer its twin")
	}
	if got := turnsOf(t, sid); len(got) != 2 {
		t.Errorf("%d turns after the copies parted, want 2", len(got))
	}
	v := Viewer{Email: dvEmail, Role: RoleMember}
	page, err := s.GetTimeline(ctx, v, sid, TimelineRange{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 {
		t.Errorf("timeline shows %d rows, want both prompts once the marker is cleared", len(page.Events))
	}
}

// A fork's copied prefix is stored (the resume bundle needs it) and flagged
// inherited: the turn whose prompt record the proven parent also holds is
// the parent's work.
func TestIntegrationForkPrefixTurnsAreInherited(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	const parent, child = "s-parent", "s-child"
	ingestBatch(t, s,
		transcriptAt("pa-0", parent, event.UserPrompt, 0, dv0, "start here", userRecord("u-shared-p", "pid-shared", "start here")),
		transcriptAt("pa-1", parent, event.AssistantTurn, 1, dv0.Add(time.Second), "ok", assistantRecord("u-shared-a", "msg_shared", "ok")),
	)
	start := transcriptAt("ch-0", child, event.SessionStarted, 0, dv0.Add(time.Hour), "", `{"type":"system","uuid":"u-ch-start"}`)
	start.Event.LineageSource = "fork_uuid"
	start.Event.ParentSessionID = parent
	start.Event.ForkPrefixSeq = 2
	ingestBatch(t, s,
		start,
		transcriptAt("ch-1", child, event.UserPrompt, 1, dv0, "start here", userRecord("u-shared-p", "pid-shared", "start here")),
		transcriptAt("ch-2", child, event.AssistantTurn, 2, dv0.Add(time.Second), "ok", assistantRecord("u-shared-a", "msg_shared", "ok")),
		transcriptAt("ch-3", child, event.UserPrompt, 3, dv0.Add(time.Hour+time.Second), "and now this", userRecord("u-child-p", "pid-child", "and now this")),
	)
	deriveAll(t, s, dv0.Add(2*time.Hour))
	if got := latticeOf(t, s, child); got.Parent == nil || *got.Parent != parent || got.Lineage != "fork_uuid" {
		t.Fatalf("child lineage = %+v, want parent %s via fork_uuid", got, parent)
	}
	turns := turnsOf(t, child)
	if len(turns) != 2 {
		t.Fatalf("%d child turns, want 2: %+v", len(turns), turns)
	}
	if !turns[0].Inherited || turns[1].Inherited {
		t.Errorf("inherited = %v, %v; want the copied prefix flagged and the child's own turn not", turns[0].Inherited, turns[1].Inherited)
	}
	if pt := turnsOf(t, parent); len(pt) != 1 || pt[0].Inherited {
		t.Errorf("parent turns = %+v, want one, not inherited", pt)
	}
}

// ---------------------------------------------------------------------------
// Session rules (criterion f: session_class, head_state, lineage)
// ---------------------------------------------------------------------------

func TestIntegrationSessionClassDemotesContentlessUserRowsAfterTheGrace(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	now := dv0.Add(48 * time.Hour)

	// Rows born user by the pre-lattice revision: lifecycle only, but typed
	// user with content_events zero. Ingest cannot demote them (the merge
	// only promotes); the runner's rule can, once the grace has passed.
	seed := func(sid string, started time.Time, ended bool) {
		batch := []Ingest{hookAt(sid+"-start", sid, dvEmail, event.SessionStarted, 1, started, "startup")}
		if ended {
			batch = append(batch, hookAt(sid+"-end", sid, dvEmail, event.SessionEnded, 2, started.Add(time.Minute), "other"))
		}
		ingestBatch(t, s, batch...)
		if _, err := pool.Exec(ctx, `
			UPDATE sessions SET session_type = 'user', empty_kind = NULL, content_events = 0, derive_dirty = true
			WHERE session_id = $1`, sid); err != nil {
			t.Fatal(err)
		}
	}
	seed("s-born-old", now.Add(-25*time.Hour), false)
	seed("s-born-young", now.Add(-time.Hour), false)
	seed("s-born-ended", now.Add(-time.Hour), true)
	// An empty row whose content arrived (a later batch under another origin
	// that the ingest merge would have promoted; here forced) is promoted by
	// the same rule in the other direction.
	ingestBatch(t, s,
		hookAt("pr-start", "s-promote", dvEmail, event.SessionStarted, 1, now.Add(-2*time.Hour), "startup"),
		hookAt("pr-prompt", "s-promote", dvEmail, event.UserPrompt, 2, now.Add(-2*time.Hour+time.Second), "hello"))
	if _, err := pool.Exec(ctx, `UPDATE sessions SET session_type = 'empty', empty_kind = 'aborted', derive_dirty = true WHERE session_id = 's-promote'`); err != nil {
		t.Fatal(err)
	}
	// Automation with nothing in it stays automation.
	auto := hookAt("au-start", "s-auto", dvEmail, event.SessionStarted, 1, now.Add(-30*time.Hour), "startup")
	auto.Event.Entrypoint = "sdk-cli"
	ingestBatch(t, s, auto)
	// Internal is recomputed from the opening prompt: a user row whose first
	// human prompt is one of the harness's own templates is the harness's
	// session, whichever revision typed it user.
	ingestBatch(t, s,
		hookAt("in-start", "s-internal", dvEmail, event.SessionStarted, 1, now.Add(-2*time.Hour), "startup"),
		hookAt("in-prompt", "s-internal", dvEmail, event.UserPrompt, 2, now.Add(-2*time.Hour+time.Second), "Reply with exactly: pong"))
	if _, err := pool.Exec(ctx, `UPDATE sessions SET session_type = 'user', derive_dirty = true WHERE session_id = 's-internal'`); err != nil {
		t.Fatal(err)
	}

	deriveAll(t, s, now)

	for sid, want := range map[string]latticeRow{
		"s-born-old":   {Type: "empty", EmptyKind: "tail_truncated", Content: 0},
		"s-born-young": {Type: "user", EmptyKind: "", Content: 0},
		"s-born-ended": {Type: "empty", EmptyKind: "aborted", Content: 0},
		"s-promote":    {Type: "user", EmptyKind: "", Content: 1},
		"s-auto":       {Type: "automation", EmptyKind: "", Content: 0},
		"s-internal":   {Type: "internal", EmptyKind: "", Content: 1},
	} {
		got := latticeOf(t, s, sid)
		if got.Type != want.Type || got.EmptyKind != want.EmptyKind || got.Content != want.Content {
			t.Errorf("%s = type %s kind %q content %d, want %s %q %d", sid, got.Type, got.EmptyKind, got.Content, want.Type, want.EmptyKind, want.Content)
		}
	}
	// The production acceptance, stated as the query the rollout runs.
	if n := countRows(t, `
		SELECT count(*) FROM sessions s
		WHERE s.session_type = 'user' AND s.content_events = 0 AND s.started_at < $1::timestamptz - interval '24 hours'
		  AND NOT EXISTS (SELECT 1 FROM events e WHERE e.session_id = s.session_id AND e.type = ANY($2::text[]))`,
		now, contentEventTypes); n != 0 {
		t.Errorf("%d user rows with no content older than 24 h remain after the pass, want 0", n)
	}
}

func TestIntegrationHeadStateIsJudgedByOriginAndByTime(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	now := dv0.Add(time.Hour)

	// Transcript stream whose lowest seq is 7: the head fell outside an
	// import window.
	ingestBatch(t, s,
		transcriptAt("hw-7", "s-window", event.UserPrompt, 7, dv0, "late start", userRecord("u-hw-7", "pid-hw", "late start")),
		transcriptAt("hw-8", "s-window", event.AssistantTurn, 8, dv0.Add(time.Second), "ok", assistantRecord("u-hw-8", "msg_hw", "ok")))
	// Transcript stream from seq 0: complete.
	ingestBatch(t, s,
		transcriptAt("hc-0", "s-complete", event.UserPrompt, 0, dv0, "hello", userRecord("u-hc-0", "pid-hc", "hello")))
	// Hook-only stream whose earliest row by time is a tool result: the
	// start was dropped. Its seq is nanoseconds (the FileSeq fallback), which
	// is why the rule reads the type by time and not min(seq).
	lost := hookAt("hl-1", "s-lost", dvEmail, event.ToolResult, 1_757_000_000_000_000_000, dv0, "out")
	lost.Event.Tool = &event.Tool{Name: "Bash", Output: "out"}
	ingestBatch(t, s, lost, hookAt("hl-2", "s-lost", dvEmail, event.UserPrompt, 1_757_000_000_000_000_001, dv0.Add(time.Second), "next"))
	// Hook stream that starts with its marker: complete.
	ingestBatch(t, s,
		hookAt("hh-1", "s-hook", dvEmail, event.SessionStarted, 1, dv0, "startup"),
		hookAt("hh-2", "s-hook", dvEmail, event.UserPrompt, 2, dv0.Add(time.Second), "hello"))

	deriveAll(t, s, now)
	for sid, want := range map[string]string{
		"s-window":   "truncated_window",
		"s-complete": "complete",
		"s-lost":     "start_lost",
		"s-hook":     "complete",
	} {
		if got := latticeOf(t, s, sid).HeadState; got != want {
			t.Errorf("%s head_state = %s, want %s", sid, got, want)
		}
	}

	// Capture loss overrides the rest, read from health_hourly (migration
	// 0020). The row is removed afterwards rather than the table dropped: the
	// suite's schema is shared, and a dropped table fails every later test
	// that reads the ledger.
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO health_hourly (email, device_id, hour, drops) VALUES ($1, NULL, date_trunc('hour', $2::timestamptz), 3)`, dvEmail, dv0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM health_hourly WHERE email = $1`, dvEmail) })
	if _, err := pool.Exec(ctx, `UPDATE sessions SET derive_dirty = true WHERE session_id IN ('s-hook', 's-window')`); err != nil {
		t.Fatal(err)
	}
	deriveAll(t, s, now)
	if got := latticeOf(t, s, "s-hook").HeadState; got != "capture_loss" {
		t.Errorf("s-hook head_state = %s after a drop in its window, want capture_loss", got)
	}
	if got := latticeOf(t, s, "s-window").HeadState; got != "capture_loss" {
		t.Errorf("s-window head_state = %s after a drop in its window, want capture_loss", got)
	}
}

func TestIntegrationLineageResolvesOnlyOneOlderSessionOfTheSamePrincipal(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	mustPrincipal(t, s, "other@example.com", RoleMember)
	now := dv0.Add(time.Hour)

	owner := func(sid, email, record string, at time.Time) {
		in := transcriptAt(sid+"-0", sid, event.UserPrompt, 0, at, "hello", userRecord(record, "pid-"+sid, "hello"))
		in.Email = email
		ingestBatch(t, s, in)
	}
	compacted := func(sid, record string, at time.Time) {
		in := transcriptAt(sid+"-c", sid, event.Compaction, 0, at, "summary", `{"type":"system","subtype":"compact_boundary","uuid":"u-`+sid+`"}`)
		in.Event.ParentRecordUUID = record
		ingestBatch(t, s, in)
	}
	// One older owner: resolved.
	owner("s-one-owner", dvEmail, "rec-one", dv0)
	compacted("s-one", "rec-one", dv0.Add(time.Minute))
	// Two older owners: ambiguous, left alone.
	owner("s-two-owner-a", dvEmail, "rec-two", dv0)
	owner("s-two-owner-b", dvEmail, "rec-two", dv0.Add(time.Second))
	compacted("s-two", "rec-two", dv0.Add(time.Minute))
	// Another principal's session owns it: not this person's parent.
	owner("s-theirs", "other@example.com", "rec-theirs", dv0)
	compacted("s-foreign", "rec-theirs", dv0.Add(time.Minute))
	// The session itself owns the record: never its own parent.
	in := transcriptAt("s-self-0", "s-self", event.UserPrompt, 0, dv0, "hello", userRecord("rec-self", "pid-self", "hello"))
	ingestBatch(t, s, in)
	compacted("s-self", "rec-self", dv0.Add(time.Minute))

	deriveAll(t, s, now)
	for sid, want := range map[string]string{
		"s-one":     "s-one-owner",
		"s-two":     "",
		"s-foreign": "",
		"s-self":    "",
	} {
		got := latticeOf(t, s, sid)
		parent := ""
		if got.Parent != nil {
			parent = *got.Parent
		}
		if parent != want {
			t.Errorf("%s parent = %q (source %q), want %q", sid, parent, got.Lineage, want)
		}
		if want != "" && got.Lineage != "record_uuid" {
			t.Errorf("%s lineage_source = %q, want record_uuid", sid, got.Lineage)
		}
		if want == "" && got.Lineage != "" {
			t.Errorf("%s lineage_source = %q, want none", sid, got.Lineage)
		}
		if got.ParentRecord == nil {
			t.Errorf("%s lost its parent_record_uuid", sid)
		}
	}
}

// The lineage probe on the shape production has (review-1 finding 19): one
// person owning hundreds of sessions, each with hundreds of keyed transcript
// rows. The identity index leads with session_id, so a probe that binds only
// record_uuid reads the whole index once per candidate session: 1.5 s for
// 245 candidates on a 200k-row fixture, about four minutes for the person
// with 1,641 sessions in production against the 60 s statement ceiling. The
// probe must reach the index by both columns, in one scan, and still resolve
// the one owner.
func TestIntegrationTheLineageProbeReadsTheIndexOnceByBothColumns(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sessions, perSession = 400, 200

	// Seeded by SQL rather than ingest: the probe's cost is the question, and
	// twenty thousand keyed rows through UpsertEvents is a minute of setup.
	if _, err := pool.Exec(ctx, `
		INSERT INTO sessions (session_id, email, source, started_at, ended_at, ended, session_type, updated_at, derive_dirty)
		SELECT 'many-' || g, $1, 'claude_code', $2::timestamptz + g * interval '1 minute',
		       $2::timestamptz + g * interval '1 minute' + interval '30 seconds', true, 'user', now(), false
		FROM generate_series(0, $3 - 1) g`, dvEmail, dv0, sessions); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, session_id, email, seq, type, origin, occurred_at, body, capture_version, record_uuid, prompt_id)
		SELECT 'many-' || g || '-' || i, 'many-' || g, $1, i,
		       CASE WHEN i % 2 = 0 THEN 'user_prompt' ELSE 'assistant_turn' END, 'transcript',
		       $2::timestamptz + g * interval '1 minute' + i * interval '100 milliseconds',
		       jsonb_build_object('id', 'many-' || g || '-' || i, 'session_id', 'many-' || g, 'text', 'row ' || i),
		       3, md5('rec-' || g || '-' || i), CASE WHEN i % 2 = 0 THEN md5('pid-' || g || '-' || i) END
		FROM generate_series(0, $3 - 1) g, generate_series(0, $4 - 1) i`, dvEmail, dv0, sessions, perSession); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE sessions, events`); err != nil {
		t.Fatal(err)
	}
	// The session asking: compacted from a record that session many-7 holds.
	var record string
	if err := pool.QueryRow(ctx, `SELECT md5('rec-7-4')`).Scan(&record); err != nil {
		t.Fatal(err)
	}
	asking := dv0.Add(time.Duration(sessions+1) * time.Minute)
	in := transcriptAt("many-child-c", "many-child", event.Compaction, 0, asking, "summary",
		`{"type":"system","subtype":"compact_boundary","uuid":"u-many-child"}`)
	in.Event.ParentRecordUUID = record
	ingestBatch(t, s, in)

	// The plan of the probe the runner runs, with the runner's own arguments.
	rows, err := pool.Query(ctx, `EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) `+lineageOwnersSQL, "many-child", dvEmail, asking, record)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	rows.Close()
	planText := strings.Join(plan, "\n")
	t.Logf("lineage probe plan:\n%s", planText)
	scans := 0
	for i, line := range plan {
		if !strings.Contains(line, "events_record_identity_idx") {
			continue
		}
		scans++
		if !strings.Contains(line, "loops=1)") {
			t.Errorf("the identity index is scanned more than once (once per candidate session):\n%s", line)
		}
		cond := ""
		if i+1 < len(plan) {
			cond = plan[i+1]
		}
		if !strings.Contains(cond, "Index Cond:") || !strings.Contains(cond, "session_id") || !strings.Contains(cond, "record_uuid") {
			t.Errorf("the identity index is not entered by session_id AND record_uuid; the condition after the scan is:\n%s", cond)
		}
	}
	if scans == 0 {
		t.Errorf("the probe does not read events_record_identity_idx at all:\n%s", planText)
	}
	for _, line := range plan {
		if strings.HasPrefix(strings.TrimSpace(line), "Execution Time:") {
			t.Logf("%s over %d candidate sessions", strings.TrimSpace(line), sessions)
		}
	}

	// And the answer is still the one owner.
	if _, err := pool.Exec(ctx, `UPDATE sessions SET derive_dirty = true WHERE session_id = 'many-child'`); err != nil {
		t.Fatal(err)
	}
	deriveAll(t, s, asking.Add(time.Hour))
	got := latticeOf(t, s, "many-child")
	if got.Parent == nil || *got.Parent != "many-7" || got.Lineage != "record_uuid" {
		t.Errorf("many-child lineage = %+v, want parent many-7 via record_uuid", got)
	}
}

// The window: a body-reading step never runs outside it, and the pass
// reports how long until it opens rather than polling.
func TestIntegrationTheVersionedPassWaitsForTheWindow(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	resetDerived(t)
	dualOriginSession(t, s, "s-window-1", dv0, 1)

	noon := time.Date(2026, 9, 11, 12, 0, 0, 0, time.Local)
	cfg := DeriveConfig{Now: func() time.Time { return noon }}
	pass, err := s.RunDerive(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Waiting || pass.Until != 14*time.Hour || pass.Done {
		t.Fatalf("pass at noon = %+v, want waiting 14h for 02:00 local", pass)
	}
	if n := countRows(t, `SELECT count(*) FROM derive_jobs WHERE finished_at IS NOT NULL`); n != 0 {
		t.Errorf("%d steps finished outside the window", n)
	}

	three := time.Date(2026, 9, 12, 3, 0, 0, 0, time.Local)
	cfg.Now = func() time.Time { return three }
	pass, err = s.RunDerive(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !pass.Done || pass.Waiting {
		t.Fatalf("pass at 03:00 IST = %+v, want done", pass)
	}
}

// ---------------------------------------------------------------------------
// Pricing under the statement ceiling (review-2 finding 20)
// ---------------------------------------------------------------------------

// seedPricedSessions writes n small transcript-only sessions of one to three
// prompts, each answered and priced in the ledger, with a tool call: the way
// a corpus of small sessions looks once folded, which is the statistics the
// planner had on the rehearsal clone when the pricing statement timed out.
// The count of prompts varies so that turn_index has more than one value in
// the statistics; with every session at one turn the planner ties the primary
// key with the (session_id, thread, turn_key) index for the turn_events
// foreign-key check and scans a large session's entries once per row, which
// is a fixture artefact, not the shape under test.
func seedPricedSessions(t *testing.T, prefix string, n int, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO sessions (session_id, email, source, started_at, ended_at, ended, session_type, updated_at, derive_dirty)
		SELECT $1 || g, $2, 'claude_code', $3::timestamptz + g * interval '1 hour',
		       $3::timestamptz + g * interval '1 hour' + interval '1 minute', true, 'user', now(), true
		FROM generate_series(0, $4 - 1) g`, prefix, dvEmail, at, n); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, session_id, email, seq, type, origin, occurred_at, body, capture_version, prompt_id, message_id, tool_use_id)
		SELECT $1 || g || '-' || j || '-' || i, $1 || g, $2, j * 3 + i,
		       CASE i WHEN 0 THEN 'user_prompt' WHEN 1 THEN 'assistant_turn' ELSE 'tool_call' END, 'transcript',
		       $3::timestamptz + g * interval '1 hour' + j * interval '10 seconds' + i * interval '2 seconds',
		       jsonb_build_object('id', $1 || g || '-' || j || '-' || i),
		       4, 'p-' || $1 || g || '-' || j, CASE WHEN i = 1 THEN 'm-' || $1 || g || '-' || j END, CASE WHEN i = 2 THEN 'toolu-' || $1 || g || '-' || j END
		FROM generate_series(0, $4 - 1) g, generate_series(0, 2) j, generate_series(0, 2) i
		WHERE j <= g % 3`, prefix, dvEmail, at, n); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO messages (event_id, session_id, email, seq, role, occurred_at, text, kind)
		SELECT id, session_id, email, seq, CASE type WHEN 'user_prompt' THEN 'user' ELSE 'assistant' END, occurred_at,
		       CASE type WHEN 'user_prompt' THEN 'ask ' || id ELSE 'answer ' || id END,
		       CASE type WHEN 'user_prompt' THEN 'human' ELSE 'assistant_text' END
		FROM events WHERE session_id LIKE $1 || '%' AND type IN ('user_prompt', 'assistant_turn')`, prefix); err != nil {
		t.Fatalf("seed messages: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO usage_ledger (message_id, request_id, event_id, session_id, email, model, occurred_at, input_tokens, output_tokens, cost_usd)
		SELECT message_id, '', id, session_id, email, 'claude-opus-5', occurred_at, 100, 10, 0.001
		FROM events WHERE session_id LIKE $1 || '%' AND type = 'assistant_turn'`, prefix); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
}

// The per-turn tokens and cost must not depend on a plan. The old pricing
// UPDATE joined turn_events to the ledger inside the fold's own transaction,
// where the planner could not see the rows the fold had just inserted: with
// statistics from a corpus of one-turn sessions it estimated one turn and a
// few turn_events for any session and nested the whole ledger aggregate
// under a per-turn loop with a filter join. On the rehearsal clone that
// timed out at 60 s on every dirty pass for a production session of 9,632
// events, 4,305 answers over 73 threads and 1,973 ledger rows, and with
// per-session isolation such sessions were parked with zeroed counters for
// good (review-2 finding 20). The fix sums the ledger per turn in Go from
// the one scan readProjection already performs and writes the columns with
// the turn rows. This test folds a session of twice that shape (8,000
// prompts, 18,000 turn_events, 4,000 ledger rows), after the statistics that
// produced the plan, under a 5 s ceiling: at the review's own shape the old
// statement took 4.0 s on the fixer's laptop and 3.4 s on the reviewer's,
// inside the ceiling by the luck of the machine, and the doubling puts it at
// four times that, so the test is red without the fix wherever it runs.
func TestIntegrationALargeSessionIsPricedInsideTheStatementCeiling(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()

	// Statistics: hundreds of small sessions folded, then ANALYZE, so the
	// planner believes a session has a turn or two and a handful of
	// turn_events.
	seedPricedSessions(t, "small-", 400, dv0)
	if res, err := s.DeriveDirty(ctx, DeriveConfig{DirtyPerPass: 1000, Now: func() time.Time { return dv0.Add(48 * time.Hour) }}); err != nil || res.Folded != 400 {
		t.Fatalf("fold of the small sessions = %+v, %v", res, err)
	}

	// The session: 8,000 prompts each answered, a tool call on every fourth
	// (18,000 turn_events), every other answer priced (4,000 ledger rows).
	const (
		sid     = "big-priced"
		prompts = 8000
	)
	if _, err := pool.Exec(ctx, `
		INSERT INTO sessions (session_id, email, source, started_at, ended_at, ended, session_type, updated_at, derive_dirty)
		VALUES ($1, $2, 'claude_code', $3, $3::timestamptz + interval '12 hours', true, 'user', now(), true)`, sid, dvEmail, dv0.Add(10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, session_id, email, seq, type, origin, occurred_at, body, capture_version, prompt_id, message_id, tool_use_id)
		SELECT $1 || '-' || k || '-' || i, $1, $2, i * 3 + k,
		       CASE k WHEN 0 THEN 'user_prompt' WHEN 1 THEN 'assistant_turn' ELSE 'tool_call' END, 'transcript',
		       $3::timestamptz + i * interval '10 seconds' + k * interval '2 seconds',
		       jsonb_build_object('id', $1 || '-' || k || '-' || i),
		       4, 'bp-' || i, CASE WHEN k = 1 THEN 'bm-' || i END, CASE WHEN k = 2 THEN 'btu-' || i END
		FROM generate_series(0, $4 - 1) i, generate_series(0, 2) k
		WHERE k < 2 OR i % 4 = 0`, sid, dvEmail, dv0.Add(10*24*time.Hour), prompts); err != nil {
		t.Fatalf("seed the large session: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO messages (event_id, session_id, email, seq, role, occurred_at, text, kind)
		SELECT id, session_id, email, seq, CASE type WHEN 'user_prompt' THEN 'user' ELSE 'assistant' END, occurred_at,
		       CASE type WHEN 'user_prompt' THEN 'ask ' || seq ELSE 'answer ' || seq END,
		       CASE type WHEN 'user_prompt' THEN 'human' ELSE 'assistant_text' END
		FROM events WHERE session_id = $1 AND type IN ('user_prompt', 'assistant_turn')`, sid); err != nil {
		t.Fatalf("seed messages: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO usage_ledger (message_id, request_id, event_id, session_id, email, model, occurred_at,
		                          input_tokens, output_tokens, cache_read_tokens, cache_write_5m_tokens, cache_write_1h_tokens, cost_usd)
		SELECT message_id, '', id, session_id, email, 'claude-opus-5', occurred_at, 100, 10, 5, 1, 2, 0.001234
		FROM events WHERE session_id = $1 AND type = 'assistant_turn' AND seq % 6 = 1`, sid); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	if n := countRows(t, `SELECT count(*) FROM usage_ledger WHERE session_id = $1`, sid); n != prompts/2 {
		t.Fatalf("%d ledger rows seeded, want %d", n, prompts/2)
	}
	if _, err := pool.Exec(ctx, `ANALYZE sessions, events, messages, usage_ledger, turns, turn_events`); err != nil {
		t.Fatal(err)
	}

	// The fold, under the ceiling the runner's own batch would not reach
	// before the retry cap: 5 s, not 60 s.
	tx, err := s.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := WithStatementTimeout(ctx, tx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	n, err := s.foldSession(ctx, tx, sid, DerivedSchema)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("fold of a %d-turn session under a 5 s ceiling failed after %s: %v", prompts, elapsed.Round(time.Millisecond), err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("folded %d turns in %s under a 5 s statement ceiling", n, elapsed.Round(time.Millisecond))
	if n != prompts {
		t.Errorf("fold wrote %d turns, want %d", n, prompts)
	}
	if got := countRows(t, `SELECT count(*) FROM turn_events WHERE session_id = $1`, sid); got != prompts*2+prompts/4 {
		t.Errorf("%d turn_events, want %d", got, prompts*2+prompts/4)
	}

	// Priced exactly: every other turn carries its call, the rest zero, and
	// the sums are the ledger's.
	var turns, priced, unpriced int
	var input, output, cacheRead, cacheWrite int64
	var cost string
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE tokens_input = 100 AND tokens_output = 10 AND tokens_cache_read = 5 AND tokens_cache_write = 3 AND cost_usd = 0.001234),
		       count(*) FILTER (WHERE tokens_input = 0 AND tokens_output = 0 AND tokens_cache_read = 0 AND tokens_cache_write = 0 AND cost_usd = 0),
		       sum(tokens_input), sum(tokens_output), sum(tokens_cache_read), sum(tokens_cache_write), sum(cost_usd)::text
		FROM turns WHERE session_id = $1`, sid).Scan(&turns, &priced, &unpriced, &input, &output, &cacheRead, &cacheWrite, &cost); err != nil {
		t.Fatal(err)
	}
	if turns != prompts || priced != prompts/2 || unpriced != prompts/2 {
		t.Errorf("turns %d priced %d unpriced %d, want %d, %d, %d", turns, priced, unpriced, prompts, prompts/2, prompts/2)
	}
	if input != 100*prompts/2 || output != 10*prompts/2 || cacheRead != 5*prompts/2 || cacheWrite != 3*prompts/2 || cost != "4.936000" {
		t.Errorf("sums = %d %d %d %d %s, want the ledger's 400000 40000 20000 12000 4.936000", input, output, cacheRead, cacheWrite, cost)
	}
	// The small sessions were priced the same way, every turn.
	smallTurns := countRows(t, `SELECT count(*) FROM turns WHERE session_id LIKE 'small-%'`)
	if got := countRows(t, `SELECT count(*) FROM turns WHERE session_id LIKE 'small-%' AND tokens_input = 100 AND cost_usd = 0.001`); smallTurns == 0 || got != smallTurns {
		t.Errorf("%d of %d small turns priced, want all", got, smallTurns)
	}
}
