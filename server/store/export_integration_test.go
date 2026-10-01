//go:build integration

package store

// The export reads against Postgres: that a COPY in the CSV form gives back
// row_to_json lines that parse and round-trip every awkward byte, that the
// projections carry what the BigQuery DDL expects (viewer_emails resolved
// through the alias rule, UTC timestamps, no raw), that a partition is the
// session's start day and holds a turn from after midnight, that a bundle
// is ordered and stamped by kind, and that the watermarks read back what
// was advanced. Same gate and schema as integration_test.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

// exportLines runs one COPY and parses every line as JSON.
func exportLines(t *testing.T, run func(w *bytes.Buffer) (int64, error)) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	n, err := run(&buf)
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	text := strings.TrimSuffix(buf.String(), "\n")
	var out []map[string]any
	if text == "" {
		if n != 0 {
			t.Fatalf("COPY reported %d rows and wrote nothing", n)
		}
		return nil
	}
	for i, line := range strings.Split(text, "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("line %d is not JSON (%v): %q", i, err, line)
		}
		out = append(out, row)
	}
	if int64(len(out)) != n {
		t.Fatalf("COPY reported %d rows, %d lines were written", n, len(out))
	}
	return out
}

func TestIntegrationExportCopiesPartitionsBundlesAndWatermarks(t *testing.T) {
	s := newStore(t, nil)
	// The deployment declares alpha.example and beta.example as one
	// Workspace (DOMAIN_ALIASES); gamma.example is allowed but not aliased.
	s.SetDeployment(Deployment{
		AllowedDomains: []string{"alpha.example", "beta.example", "gamma.example"},
		DomainAliases:  []DomainAlias{{A: "alpha.example", B: "beta.example"}},
	})
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DELETE FROM export_watermarks`); err != nil {
		t.Fatal(err)
	}
	// The owner enrolled on alpha.example; a principals row for the same
	// local part exists on the third, unaliased domain, and nobody sits on
	// beta.example.
	mustPrincipal(t, s, "owner@alpha.example", RoleMember)
	mustPrincipal(t, s, "owner@gamma.example", RoleMember)
	mustPrincipal(t, s, "other@beta.example", RoleMember)

	// Every byte COPY's text format would have mangled, and both bytes the
	// CSV form uses as quote and delimiter.
	awkward := "back\\slash \"quote\" new\nline tab\tcomma, ctrl\x01\x02 end é"
	batch := session(t, "owner@alpha.example", "sess-x")
	batch[0].Event.Text = awkward
	batch[0].Event.Raw = json.RawMessage(`{"type":"user","message":{"content":"the raw line"}}`)
	batch[2].Event.Tool = &event.Tool{Name: "Bash", Output: strings.Repeat("o", 100)}
	batch[2].Event.Raw = json.RawMessage(`{"raw":"tool"}`)
	// Two rows the bundle leaves out and the events partition keeps: a
	// compaction record and a prompt that is a system reminder on its own.
	compaction := ingestOf("evt-compaction", "sess-x", "owner@alpha.example", event.Compaction, 6)
	compaction.Event.Text = "This session is being continued from a previous conversation"
	reminder := ingestOf("evt-reminder", "sess-x", "owner@alpha.example", event.UserPrompt, 7)
	reminder.Event.Text = "<system-reminder>the harness speaking</system-reminder>"
	batch = append(batch, compaction, reminder)
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	// The kind is what the export tests, however the reminder was classified
	// at ingest; pin it.
	if tag, err := pool.Exec(ctx, `UPDATE messages SET kind = 'system_reminder' WHERE event_id = 'evt-reminder'`); err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("pin the reminder's kind: %v (rows %d)", err, tag.RowsAffected())
	}
	// A second person's session, with ids of its own (the helper's ids are
	// per test, and events.id is the primary key).
	other := session(t, "other@beta.example", "sess-y")
	for i := range other {
		other[i].Event.ID = "y-" + other[i].Event.ID
	}
	if _, err := s.UpsertEvents(ctx, other); err != nil {
		t.Fatalf("ingest other: %v", err)
	}
	// Pin the session's start to 23:50 UTC on the 10th and give it a turn
	// that started after midnight: the turn belongs to the 10th's
	// partition because its session does.
	if _, err := pool.Exec(ctx, `UPDATE sessions SET started_at = '2026-09-10T23:50:00Z', updated_at = '2026-09-12T03:00:00Z' WHERE session_id = 'sess-x'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE sessions SET started_at = '2026-09-11T10:00:00Z', updated_at = '2026-09-12T03:10:00Z' WHERE session_id = 'sess-y'`); err != nil {
		t.Fatal(err)
	}
	for _, turn := range []struct {
		idx     int
		started string
	}{{0, "2026-09-10T23:51:00Z"}, {1, "2026-09-11T00:05:00Z"}} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO turns (session_id, thread, turn_index, turn_key, kind, outcome, started_at, last_activity_at, origins, model)
			VALUES ('sess-x', '', $1, $3, 'human', 'answered', $2, $2, '{hook}', 'claude-opus-5')`, turn.idx, turn.started, fmt.Sprintf("pid:%d", turn.idx)); err != nil {
			t.Fatal(err)
		}
	}
	// Planning reads before PR E's table exists: the plan says so and reads
	// nothing from it.
	plan := func(req ExportPlanRequest) ExportPlanReads {
		reads, err := s.ExportPlan(ctx, req)
		if err != nil {
			t.Fatalf("plan %+v: %v", req, err)
		}
		return reads
	}
	// health_hourly is PR E's table (0020) and exists on every migrated
	// database now, so the table-absent path is exercised by the export
	// package's fake, not here; this test owns only the rows it inserts.
	today := time.Now().UTC().Format("2006-01-02")
	reads := plan(ExportPlanRequest{SessionsLimit: 10, EventsLimit: 10})
	if !reads.HealthTableExists || reads.HealthDays != nil {
		t.Errorf("health with the table present and empty: %+v", reads)
	}
	if len(reads.Sessions) != 2 || reads.Sessions[0].SessionID != "sess-x" || reads.Sessions[1].SessionID != "sess-y" || reads.Sessions[0].Email != "owner@alpha.example" {
		t.Errorf("touched = %+v, want sess-x then sess-y by updated_at", reads.Sessions)
	}
	if len(reads.EventDays) != 1 || reads.EventDays[0] != today {
		t.Errorf("event days = %v, want today (%s)", reads.EventDays, today)
	}

	// One rollup row in PR E's table; removed again at the end so the health
	// tests that truncate the table find it where 0020 left it.
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM health_hourly WHERE email = 'owner@alpha.example'`)
	})
	if _, err := pool.Exec(ctx, `INSERT INTO health_hourly (email, device_id, hour, reports, worst, conditions, drops) VALUES ('owner@alpha.example', NULL, '2026-09-11T03:00:00Z', 12, 'ok', '{}', 4)`); err != nil {
		t.Fatal(err)
	}

	// Planning reads with everything in place, and the since positions
	// honoured.
	reads = plan(ExportPlanRequest{SessionsSince: reads.Sessions[0].UpdatedAt, SessionsLimit: 10, EventsSince: time.Now().Add(time.Hour), EventsLimit: 10})
	if len(reads.Sessions) != 1 || reads.Sessions[0].SessionID != "sess-y" {
		t.Errorf("since sess-x's updated_at: %+v", reads.Sessions)
	}
	if len(reads.EventDays) != 0 {
		t.Errorf("event days after the future: %v", reads.EventDays)
	}
	if !reads.HealthTableExists || len(reads.HealthDays) != 1 || reads.HealthDays[0] != "2026-09-11" {
		t.Errorf("health days = %+v", reads)
	}
	if reads = plan(ExportPlanRequest{SessionsLimit: 10, EventsLimit: 10, HealthSince: time.Date(2026, 9, 11, 3, 0, 0, 0, time.UTC)}); len(reads.HealthDays) != 0 {
		t.Errorf("health days after the only hour: %v", reads.HealthDays)
	}
	// The plan's transaction left nothing on the pool's connections.
	var timeout string
	if err := pool.QueryRow(ctx, `SELECT current_setting('statement_timeout')`).Scan(&timeout); err != nil || timeout == "10min" {
		t.Errorf("statement_timeout after the plan = %q (%v); the ceiling leaked past the transaction", timeout, err)
	}

	// turns: the 10th holds both turns of sess-x, with the session's day
	// beside them, and nothing of sess-y.
	turns := exportLines(t, func(w *bytes.Buffer) (int64, error) { return s.ExportCopyPartition(ctx, "turns", "2026-09-10", w) })
	if len(turns) != 2 {
		t.Fatalf("turns partition has %d rows, want 2 (one started after midnight)", len(turns))
	}
	// The owner and its alias on the paired domain; the principals row on
	// the unaliased domain is not a viewer (a same-local-part row there may
	// be someone else).
	viewers, _ := json.Marshal(turns[0]["viewer_emails"])
	if string(viewers) != `["owner@alpha.example","owner@beta.example"]` {
		t.Errorf("viewer_emails = %s, want the owner and the Workspace alias only", viewers)
	}
	if got := turns[0]["session_started_at"]; got != "2026-09-10T23:50:00+00:00" {
		t.Errorf("session_started_at = %v, want UTC with an offset BigQuery reads", got)
	}
	if turns[0]["model"] != "claude-opus-5" || turns[0]["exported_at"] == nil || turns[0]["source"] != "claude_code" {
		t.Errorf("turn row %v", turns[0])
	}
	if next := exportLines(t, func(w *bytes.Buffer) (int64, error) { return s.ExportCopyPartition(ctx, "turns", "2026-09-11", w) }); len(next) != 0 {
		t.Errorf("the 11th's turns partition holds %d rows of a session that started on the 10th", len(next))
	}

	// sessions and messages, by the same day; the awkward text is intact.
	sessions := exportLines(t, func(w *bytes.Buffer) (int64, error) { return s.ExportCopyPartition(ctx, "sessions", "2026-09-10", w) })
	if len(sessions) != 1 || sessions[0]["session_id"] != "sess-x" || sessions[0]["session_type"] != "user" {
		t.Errorf("sessions partition %v", sessions)
	}
	if _, leaked := sessions[0]["mirror_request"]; leaked {
		t.Error("sessions row carries mirror_request")
	}
	messages := exportLines(t, func(w *bytes.Buffer) (int64, error) { return s.ExportCopyPartition(ctx, "messages", "2026-09-10", w) })
	var prompt, reminderRow map[string]any
	for _, m := range messages {
		switch m["event_id"] {
		case "evt-prompt":
			prompt = m
		case "evt-reminder":
			reminderRow = m
		}
	}
	if prompt == nil {
		t.Fatalf("no user message in %v", messages)
	}
	// The reminder is in the messages partition (with its kind) even though
	// the bundle leaves it out.
	if reminderRow == nil || reminderRow["kind"] != "system_reminder" {
		t.Errorf("the system reminder is missing from the messages partition or lost its kind: %v", reminderRow)
	}
	if prompt["text"] != awkward {
		t.Errorf("message text did not round-trip:\n got %q\nwant %q", prompt["text"], awkward)
	}
	if prompt["kind"] != "human" || prompt["origin"] != "hook" || prompt["superseded_by"] != nil || prompt["session_started_at"] != "2026-09-10T23:50:00+00:00" {
		t.Errorf("message row %v", prompt)
	}

	// events, by ingested day: the derived body without the raw record.
	events := exportLines(t, func(w *bytes.Buffer) (int64, error) { return s.ExportCopyPartition(ctx, "events", today, w) })
	if len(events) != 12 {
		t.Errorf("events partition has %d rows, want the 12 ingested today (the compaction and the reminder included)", len(events))
	}
	var eventTypes []string
	for _, e := range events {
		if e["session_id"] == "sess-x" {
			eventTypes = append(eventTypes, e["type"].(string))
		}
	}
	if joined := strings.Join(eventTypes, ","); !strings.Contains(joined, "compaction") {
		t.Errorf("the events partition lacks the compaction record: %s", joined)
	}
	for _, e := range events {
		body, ok := e["body"].(map[string]any)
		if !ok {
			t.Fatalf("event body is not an object: %v", e["body"])
		}
		if _, raw := body["raw"]; raw {
			t.Errorf("event %v exports the raw record", e["id"])
		}
		if e["body_expired"] != false || e["viewer_emails"] == nil {
			t.Errorf("event row %v", e)
		}
		if e["id"] == "evt-prompt" && e["session_id"] == "sess-x" && body["text"] != awkward {
			t.Errorf("event text did not round-trip: %q", body["text"])
		}
		if e["id"] == "evt-tool" && e["session_id"] == "sess-x" {
			if tool, _ := body["tool"].(map[string]any); tool == nil || tool["output"] != strings.Repeat("o", 100) {
				t.Errorf("derived tool output missing from %v", body)
			}
		}
	}

	// health_hourly by day.
	health := exportLines(t, func(w *bytes.Buffer) (int64, error) {
		return s.ExportCopyPartition(ctx, "health_hourly", "2026-09-11", w)
	})
	if len(health) != 1 || health[0]["drops"] != float64(4) || health[0]["hour"] != "2026-09-11T03:00:00+00:00" {
		t.Errorf("health partition %v", health)
	}

	// The bundle: session, turns, events, in that order, each stamped.
	bundle := exportLines(t, func(w *bytes.Buffer) (int64, error) { return s.ExportCopyBundle(ctx, "sess-x", w) })
	var records []string
	for _, l := range bundle {
		records = append(records, l["record"].(string))
	}
	// Five of the session's seven events: the compaction record and the
	// system reminder are the harness's text, not the exchange, and stay
	// out of the bundle (they are in the events partition above).
	if strings.Join(records, ",") != "session,turn,turn,event,event,event,event,event" {
		t.Errorf("bundle records %v", records)
	}
	for _, l := range bundle {
		if l["id"] == "evt-compaction" || l["id"] == "evt-reminder" {
			t.Errorf("the bundle carries %v, which is harness text", l["id"])
		}
	}
	// The turn's own kind survives beside the record label.
	if bundle[1]["kind"] != "human" {
		t.Errorf("turn line kind = %v, want the turn's own kind", bundle[1]["kind"])
	}
	if bundle[1]["turn_index"] != float64(0) || bundle[2]["turn_index"] != float64(1) {
		t.Errorf("turns out of order: %v %v", bundle[1]["turn_index"], bundle[2]["turn_index"])
	}
	var seqs []float64
	for _, l := range bundle[3:] {
		seqs = append(seqs, l["seq"].(float64))
		if _, raw := l["body"].(map[string]any)["raw"]; raw {
			t.Error("bundle event carries the raw record")
		}
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] < seqs[i-1] {
			t.Errorf("bundle events out of order: %v", seqs)
		}
	}
	if none := exportLines(t, func(w *bytes.Buffer) (int64, error) { return s.ExportCopyBundle(ctx, "no-such-session", w) }); len(none) != 0 {
		t.Errorf("a bundle for an unknown session has %d lines", len(none))
	}

	// Watermarks: absent, then advanced, then moved again.
	marks, err := s.ExportWatermarks(ctx)
	if err != nil || len(marks) != 0 {
		t.Errorf("fresh watermarks = %v (%v)", marks, err)
	}
	first := time.Date(2026, 9, 12, 3, 55, 0, 0, time.UTC)
	if err := s.ExportAdvanceWatermarks(ctx, map[string]time.Time{ExportWatermarkSessions: first, ExportWatermarkEvents: first, ExportWatermarkHealth: first}); err != nil {
		t.Fatal(err)
	}
	if err := s.ExportAdvanceWatermarks(ctx, map[string]time.Time{ExportWatermarkSessions: first.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	marks, _ = s.ExportWatermarks(ctx)
	if !marks[ExportWatermarkSessions].Equal(first.Add(time.Hour)) || !marks[ExportWatermarkEvents].Equal(first) || !marks[ExportWatermarkHealth].Equal(first) {
		t.Errorf("watermarks = %v", marks)
	}
	if now, err := s.ExportNow(ctx); err != nil || time.Since(now) > time.Minute {
		t.Errorf("ExportNow = %v (%v)", now, err)
	}
}

// TestIntegrationExportCopyIsReadOnlyBoundedAndLeavesTheConnectionClean:
// the transaction is READ ONLY, carries the export ceiling and the UTC
// zone only for its own duration, and the connection goes back to the pool
// with the pool's settings.
func TestIntegrationExportCopyIsReadOnlyBoundedAndLeavesTheConnectionClean(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `SET statement_timeout = '30s'`); err != nil {
		t.Fatal(err)
	}
	// A COPY of a statement that tries to write fails as read-only.
	settings := [][2]string{{"export.day", "2026-09-10"}}
	_, err := s.exportCopy(ctx, settings, `COPY (SELECT current_setting('transaction_read_only')) TO STDOUT`, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	var buf bytes.Buffer
	if _, err := s.exportCopy(ctx, settings, `COPY (SELECT current_setting('transaction_read_only') || ' ' || current_setting('statement_timeout') || ' ' || current_setting('TimeZone') || ' ' || current_setting('export.day', true)) TO STDOUT`, &buf); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(buf.String()); got != "on 10min UTC 2026-09-10" {
		t.Errorf("inside the export transaction: %q, want read-only, the 10 min ceiling, UTC and the day setting", got)
	}
	if _, err := s.exportCopy(ctx, nil, `COPY (SELECT 1) TO STDOUT`, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	// After the transaction, on any pooled connection, the settings are the
	// connection's own again. The pool is small enough that this is likely
	// the same connection; either way nothing export set may be visible.
	var tz, timeout, day string
	if err := pool.QueryRow(ctx, `SELECT current_setting('TimeZone'), current_setting('statement_timeout'), coalesce(current_setting('export.day', true), '')`).Scan(&tz, &timeout, &day); err != nil {
		t.Fatal(err)
	}
	if tz == "UTC" && timeout == "10min" {
		t.Errorf("the export settings leaked past the transaction: tz %s timeout %s", tz, timeout)
	}
	if day != "" {
		t.Errorf("export.day leaked past the transaction: %q", day)
	}
}
