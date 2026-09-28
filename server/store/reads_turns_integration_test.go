//go:build integration

package store

// The reads over the derived tables against a real Postgres: the turns page
// with its events and nested subagent turns, the facets read, and the repair
// list.

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// TestIntegrationGetTurnsPagesADualOriginSessionOncePerMoment: the reader's
// page holds each turn once, with the canonical copies only (the superseded
// hook prompt and answer are not on it, the transcript tool call elected out
// is not on it), the message kind on every prompt row, and a subagent's own
// turn nested under the main turn in progress when it started. Paging by
// turn index never splits a turn.
func TestIntegrationGetTurnsPagesADualOriginSessionOncePerMoment(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, flatPricer{perToken: 0.001})
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	v := Viewer{Email: dvEmail, Role: RoleMember}
	const sid = "s-turnpage"

	prompts := dualOriginSession(t, s, sid, dv0, 3)
	// A subagent spawned inside turn 1: its task prompt and its answer, on
	// the hook path, in its own stream.
	agentID := "a0123456789abcdef"
	task := hookAt(sid+"-agent-task", sid, dvEmail, event.UserPrompt, 1, dv0.Add(10*time.Minute+2*time.Second), "Review the diff")
	task.Event.AgentID = agentID
	task.Event.PromptID = sid + "-pid-1"
	reply := hookAt(sid+"-agent-reply", sid, dvEmail, event.AssistantTurn, 2, dv0.Add(10*time.Minute+8*time.Second), "Looks fine.")
	reply.Event.AgentID = agentID
	reply.Event.PromptID = sid + "-pid-1"
	ingestBatch(t, s, task, reply)
	deriveAll(t, s, dv0.Add(time.Hour))

	page, err := s.GetTurns(ctx, v, sid, TurnRange{Limit: 25})
	if err != nil {
		t.Fatalf("GetTurns: %v", err)
	}
	if len(page.Turns) != 3 || page.HasMore || page.NextAfter != nil || page.EventsCapped {
		t.Fatalf("page = %d turns more=%v next=%v capped=%v, want the 3 turns on one page",
			len(page.Turns), page.HasMore, page.NextAfter, page.EventsCapped)
	}
	for i, tr := range page.Turns {
		if tr.Index != i || tr.Thread != "" || tr.Kind != "human" || tr.Outcome != "answered" {
			t.Errorf("turn %d = index %d thread %q kind %q outcome %q", i, tr.Index, tr.Thread, tr.Kind, tr.Outcome)
		}
		if tr.PromptEventID != prompts[i] {
			t.Errorf("turn %d prompt %q, want the transcript copy %q", i, tr.PromptEventID, prompts[i])
		}
		// Prompt, call (hook copy), result (hook copy), final: four rows,
		// none of them the superseded hook prompt, hook answer or transcript
		// tool call.
		var roles []string
		for _, e := range tr.Events {
			roles = append(roles, e.Role)
			if e.Role == "superseded" {
				t.Errorf("turn %d carries a superseded row %s", i, e.ID)
			}
			if e.Type == string(event.UserPrompt) && e.Kind != "human" {
				t.Errorf("turn %d prompt row %s carries kind %q", i, e.ID, e.Kind)
			}
			var body map[string]json.RawMessage
			if err := json.Unmarshal(e.Body, &body); err != nil {
				t.Fatalf("turn %d row %s body: %v", i, e.ID, err)
			}
			if _, ok := body["raw"]; ok {
				t.Errorf("turn %d row %s still carries raw", i, e.ID)
			}
		}
		if !reflect.DeepEqual(roles, []string{"prompt", "work", "work", "final"}) {
			t.Errorf("turn %d roles = %v", i, roles)
		}
		if tr.Events[0].ID != prompts[i] || tr.Events[3].ID != tr.FinalEventID {
			t.Errorf("turn %d first/last rows %s/%s, want the canonical prompt and final", i, tr.Events[0].ID, tr.Events[3].ID)
		}
		for _, e := range tr.Events[1:3] {
			if e.Origin != string(event.OriginHook) {
				t.Errorf("turn %d tool row %s is %s, want the hook copy with the fuller output", i, e.ID, e.Origin)
			}
		}
		if tr.TokensInput != 100 || tr.CostUSD <= 0 || tr.Model != "claude-opus-5" {
			t.Errorf("turn %d tokens %d cost %f model %q", i, tr.TokensInput, tr.CostUSD, tr.Model)
		}
	}
	if len(page.Agents) != 1 || page.Agents[0].Thread != "0123456789abcdef" || page.Agents[0].Kind != "subagent_task" {
		t.Fatalf("agents = %+v, want the one subagent turn under its canonical thread", page.Agents)
	}
	agent := page.Agents[0]
	if !agent.StartedAt.After(page.Turns[1].StartedAt) || !agent.StartedAt.Before(page.Turns[2].StartedAt) {
		t.Errorf("the subagent turn started at %s, outside turn 1's span", agent.StartedAt)
	}
	if len(agent.Events) != 2 || agent.Events[0].Kind != "subagent_task" || agent.Events[1].Role != "final" {
		t.Errorf("agent events = %+v", agent.Events)
	}
	if page.Turns[1].Subagents != 1 {
		t.Errorf("turn 1 counts %d subagents, want 1", page.Turns[1].Subagents)
	}

	// Paging: one turn per page, the cursor is the index, the subagent rides
	// with the page whose span holds it and no other.
	first, err := s.GetTurns(ctx, v, sid, TurnRange{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Turns) != 1 || !first.HasMore || first.NextAfter == nil || *first.NextAfter != 0 || len(first.Agents) != 0 {
		t.Fatalf("first page = %d turns more=%v next=%v agents=%d", len(first.Turns), first.HasMore, first.NextAfter, len(first.Agents))
	}
	second, err := s.GetTurns(ctx, v, sid, TurnRange{Limit: 1, After: first.NextAfter})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Turns) != 1 || second.Turns[0].Index != 1 || len(second.Agents) != 1 || !second.HasMore {
		t.Fatalf("second page = %+v agents=%d more=%v", second.Turns, len(second.Agents), second.HasMore)
	}
	third, err := s.GetTurns(ctx, v, sid, TurnRange{Limit: 1, After: second.NextAfter})
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Turns) != 1 || third.Turns[0].Index != 2 || third.HasMore || len(third.Agents) != 0 {
		t.Fatalf("third page = %+v agents=%d more=%v", third.Turns, len(third.Agents), third.HasMore)
	}
	// A thread range reads the subagent's own turns top-level.
	thread, err := s.GetTurns(ctx, v, sid, TurnRange{Thread: "0123456789abcdef"})
	if err != nil {
		t.Fatal(err)
	}
	if len(thread.Turns) != 1 || thread.Turns[0].Kind != "subagent_task" || len(thread.Agents) != 0 {
		t.Fatalf("thread page = %+v", thread.Turns)
	}

	// Authorisation and audit follow the transcript's rules.
	if _, err := s.GetTurns(ctx, Viewer{Email: "other@example.com", Role: RoleMember}, sid, TurnRange{}); err != ErrNotFound {
		t.Errorf("a stranger read the turns page: %v", err)
	}
	if n := countRows(t, `SELECT count(*) FROM access_log WHERE session_id = $1`, sid); n != 0 {
		t.Errorf("%d audit rows for an owner's own reads, want none", n)
	}
	if _, err := s.GetTurns(ctx, Viewer{Email: "alex@example.com", Role: RoleAdmin}, sid, TurnRange{}); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, `SELECT count(*) FROM access_log WHERE session_id = $1 AND via = 'admin'`, sid); n != 1 {
		t.Errorf("%d audit rows for an admin's read, want 1", n)
	}
}

// TestIntegrationSessionFacetsReadThePageInOneQuery: the facets of a page of
// sessions come back keyed by id, scoped to the viewer, with the capture-loss
// drop count only where the head state says so.
func TestIntegrationSessionFacetsReadThePageInOneQuery(t *testing.T) {
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	mustPrincipal(t, s, "other@example.com", RoleMember)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE health_hourly`); err != nil {
		t.Fatal(err)
	}
	dualOriginSession(t, s, "s-facet-a", dv0, 1)
	dualOriginSession(t, s, "s-facet-b", dv0.Add(time.Hour), 2)
	deriveAll(t, s, dv0.Add(2*time.Hour))
	if _, err := pool.Exec(ctx, `
		UPDATE sessions SET head_state = 'capture_loss', lineage_source = 'fork_uuid', parent_session_id = 's-facet-a'
		WHERE session_id = 's-facet-b'`); err != nil {
		t.Fatal(err)
	}
	// Session b's first turn lost its answer (the old Stop handler); its
	// second has one. The snippet is the first turn's, so it is empty: a
	// later turn's answer under the first turn's title would be a lie.
	if _, err := pool.Exec(ctx, `
		UPDATE turns SET final_event_id = NULL, outcome = 'no_answer_captured'
		WHERE session_id = 's-facet-b' AND thread = '' AND turn_index = 0`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO health_hourly (email, device_id, hour, reports, worst, drops)
		VALUES ($1, NULL, date_trunc('hour', $2::timestamptz), 12, 'critical', 17)`, dvEmail, dv0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	facets, err := s.SessionFacets(ctx, Viewer{Email: dvEmail, Role: RoleMember}, []string{"s-facet-a", "s-facet-b", "s-missing"})
	if err != nil {
		t.Fatalf("SessionFacets: %v", err)
	}
	if len(facets) != 2 {
		t.Fatalf("facets = %+v, want the two sessions that exist", facets)
	}
	a, b := facets["s-facet-a"], facets["s-facet-b"]
	if a.Type != "user" || a.HeadState == "" || a.HumanTurns != 1 || a.TitleSource != "human" || a.CaptureLossDrops != 0 {
		t.Errorf("facet a = %+v", a)
	}
	if a.FirstAnswer != "done with 0" {
		t.Errorf("first answer = %q, want the first turn's final text", a.FirstAnswer)
	}
	if b.HeadState != "capture_loss" || b.LineageSource != "fork_uuid" || b.ParentSessionID != "s-facet-a" || b.CaptureLossDrops != 17 {
		t.Errorf("facet b = %+v", b)
	}
	if b.FirstAnswer != "" {
		t.Errorf("first answer = %q, want none: the first turn has no answer and the second turn's is not its", b.FirstAnswer)
	}
	// Scoped: a stranger sees nothing.
	if got, _ := s.SessionFacets(ctx, Viewer{Email: "other@example.com", Role: RoleMember}, []string{"s-facet-a"}); len(got) != 0 {
		t.Errorf("a stranger read facets: %+v", got)
	}
}

// TestIntegrationRepairListNamesWhatARewalkWouldComplete: hook-only sessions
// whose turns ended without an answer, and Codex sessions stored before the
// walker read token counts, owned by the calling device; not a session the
// transcript already answered, not a turn still in progress, not another
// device's.
func TestIntegrationRepairListNamesWhatARewalkWouldComplete(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	dev, err := s.EnrollDevice(ctx, Device{Email: dvEmail}, []byte("h-repair"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.EnrollDevice(ctx, Device{Email: dvEmail}, []byte("h-repair-2"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	// answerless: two hook prompts, Stops with no text, session ended.
	hookOnly := func(sid, device string, ended bool, at time.Time) {
		var batch []Ingest
		start := hookAt(sid+"-start", sid, dvEmail, event.SessionStarted, 1, at, "startup")
		start.Event.Cwd = "/home/dev/work/api"
		start.DeviceID = device
		batch = append(batch, start)
		for i := 0; i < 2; i++ {
			p := hookAt(fmt.Sprintf("%s-p%d", sid, i), sid, dvEmail, event.UserPrompt, int64(2+2*i), at.Add(time.Duration(i)*time.Minute), fmt.Sprintf("ask %d", i))
			p.Event.PromptID = fmt.Sprintf("%s-pid-%d", sid, i)
			p.DeviceID = device
			a := hookAt(fmt.Sprintf("%s-a%d", sid, i), sid, dvEmail, event.AssistantTurn, int64(3+2*i), at.Add(time.Duration(i)*time.Minute+10*time.Second), "")
			a.Event.PromptID = p.Event.PromptID
			a.DeviceID = device
			batch = append(batch, p, a)
		}
		if ended {
			end := hookAt(sid+"-end", sid, dvEmail, event.SessionEnded, 9, at.Add(5*time.Minute), "")
			end.DeviceID = device
			batch = append(batch, end)
		}
		ingestBatch(t, s, batch...)
	}
	hookOnly("s-rep-answerless", dev.ID, true, dv0)
	hookOnly("s-rep-live", dev.ID, false, dv0.Add(time.Hour))
	hookOnly("s-rep-other-device", other.ID, true, dv0.Add(2*time.Hour))

	// Walked: the same shape plus a transcript answer; nothing to repair.
	hookOnly("s-rep-walked", dev.ID, true, dv0.Add(3*time.Hour))
	ta := transcriptAt("s-rep-walked-t-a0", "s-rep-walked", event.AssistantTurn, 3, dv0.Add(3*time.Hour+11*time.Second), "done",
		assistantRecord("uuid-walked-a0", "msg_walked_0", "done"))
	ta.Event.PromptID = "s-rep-walked-pid-0"
	ta.DeviceID = dev.ID
	ingestBatch(t, s, ta)

	// Codex with content and no tokens, never ended, quiet for two days.
	cx := transcriptAt("s-rep-codex-p", "s-rep-codex", event.UserPrompt, 0, dv0.Add(-48*time.Hour), "hello codex", "")
	cx.Event.Source = event.SourceCodex
	cx.DeviceID = dev.ID
	cx.Event.Cwd = "/home/dev/proj"
	cxa := transcriptAt("s-rep-codex-a", "s-rep-codex", event.AssistantTurn, 1, dv0.Add(-48*time.Hour+5*time.Second), "hi", "")
	cxa.Event.Source = event.SourceCodex
	cxa.DeviceID = dev.ID
	ingestBatch(t, s, cx, cxa)

	deriveAll(t, s, dv0.Add(24*time.Hour))
	// Settled means untouched for a day: updated_at, which every fold bumps
	// to the wall clock. The fixture pins it to each session's last event,
	// which is what the wall clock would have read: the Codex session two
	// days before dv0, the live session an hour after it.
	for sid, at := range map[string]time.Time{
		"s-rep-codex": dv0.Add(-48 * time.Hour),
		"s-rep-live":  dv0.Add(time.Hour + time.Minute + 10*time.Second),
	} {
		if _, err := pool.Exec(ctx, `UPDATE sessions SET updated_at = $1 WHERE session_id = $2`, at, sid); err != nil {
			t.Fatal(err)
		}
	}

	// As of two hours after dv0: the live session's last event is an hour
	// old and it has no end marker, so it is still being written.
	list, err := s.RepairList(ctx, dev.ID, dv0.Add(2*time.Hour), 200)
	if err != nil {
		t.Fatalf("RepairList: %v", err)
	}
	got := map[string]RepairEntry{}
	for _, e := range list {
		got[e.SessionID] = e
	}
	if len(list) != 2 {
		t.Fatalf("repair list = %+v, want the answerless session and the Codex session", list)
	}
	if e := got["s-rep-answerless"]; e.Reason != "missing_answers" || e.Turns != 2 || e.Cwd != "/home/dev/work/api" || e.Source != "claude_code" {
		t.Errorf("answerless entry = %+v", e)
	}
	if e := got["s-rep-codex"]; e.Reason != "codex_tokens" || e.Source != "codex" {
		t.Errorf("codex entry = %+v", e)
	}
	// Newest first.
	if list[0].SessionID != "s-rep-answerless" {
		t.Errorf("list order = %s first, want the newest session", list[0].SessionID)
	}
	for _, absent := range []string{"s-rep-live", "s-rep-other-device", "s-rep-walked"} {
		if _, ok := got[absent]; ok {
			t.Errorf("%s is on the list", absent)
		}
	}
	if other, _ := s.RepairList(ctx, other.ID, dv0.Add(2*time.Hour), 200); len(other) != 1 || other[0].SessionID != "s-rep-other-device" {
		t.Errorf("the other device's list = %+v", other)
	}
	// A day later the quiet session without an end marker is settled and
	// joins the list.
	if later, _ := s.RepairList(ctx, dev.ID, dv0.Add(30*time.Hour), 200); len(later) != 3 {
		t.Errorf("a day later the list has %d entries, want the quiet session too", len(later))
	}
}
