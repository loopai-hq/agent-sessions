//go:build integration

package slack

// The live thread on the derive layer's turns, against a real Postgres: the
// ledger keeps its prompt-seq key so rows written before the fold existed
// stay valid, and no wrapper the harness injects is ever quoted as the
// person.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/normalize"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// ingestTurn writes one hook-captured exchange and, when transcript is set,
// its transcript copy under the same prompt id, so the fold pairs the two.
func ingestTurn(t *testing.T, st *store.Store, sid string, i int, at time.Time, prompt, answer string, transcript bool) {
	t.Helper()
	pid := fmt.Sprintf("%s-pid-%d", sid, i)
	hp := store.Ingest{Email: "ana@example.org", Event: event.Event{
		ID: fmt.Sprintf("%s-h-p%d", sid, i), SessionID: sid, Seq: int64(10 * i), Type: event.UserPrompt,
		Source: event.SourceClaudeCode, Origin: event.OriginHook, OccurredAt: at, Text: prompt, PromptID: pid,
	}}
	ha := store.Ingest{Email: "ana@example.org", Event: event.Event{
		ID: fmt.Sprintf("%s-h-a%d", sid, i), SessionID: sid, Seq: int64(10*i + 1), Type: event.AssistantTurn,
		Source: event.SourceClaudeCode, Origin: event.OriginHook, OccurredAt: at.Add(20 * time.Second), Text: answer, PromptID: pid,
	}}
	batch := []store.Ingest{hp, ha}
	if transcript {
		tp := store.Ingest{Email: "ana@example.org", Event: event.Event{
			ID: fmt.Sprintf("%s-t-p%d", sid, i), SessionID: sid, Seq: int64(2 * i), Type: event.UserPrompt,
			Source: event.SourceClaudeCode, Origin: event.OriginTranscript, CaptureVersion: 3,
			OccurredAt: at.Add(time.Second), Text: prompt, PromptID: pid, RecordUUID: fmt.Sprintf("uuid-%s-p%d", sid, i),
		}}
		ta := store.Ingest{Email: "ana@example.org", Event: event.Event{
			ID: fmt.Sprintf("%s-t-a%d", sid, i), SessionID: sid, Seq: int64(2*i + 1), Type: event.AssistantTurn,
			Source: event.SourceClaudeCode, Origin: event.OriginTranscript, CaptureVersion: 3,
			OccurredAt: at.Add(21 * time.Second), Text: answer, PromptID: pid, RecordUUID: fmt.Sprintf("uuid-%s-a%d", sid, i),
		}}
		batch = append(batch, tp, ta)
	}
	if _, err := st.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("ingest turn %d: %v", i, err)
	}
}

// TestIntegrationLiveLedgerKeyedOnTheCanonicalPromptSeqKeepsOldRowsValid: a
// row the old query wrote under the hook prompt's seq still counts as posted
// once the fold elects the transcript copy canonical, and a new post is keyed
// on the canonical prompt's seq.
func TestIntegrationLiveLedgerKeyedOnTheCanonicalPromptSeqKeepsOldRowsValid(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addPerson(t, "ana@example.org")
	now := time.Now().UTC()
	addSession(t, sessionRow{id: "s-led", email: "ana@example.org", ended: true, updatedAt: now})
	st := store.New(pool, nil)

	ingestTurn(t, st, "s-led", 1, now.Add(-20*time.Minute), "first ask", "first answer", true)
	ingestTurn(t, st, "s-led", 2, now.Add(-10*time.Minute), "second ask", "second answer", true)
	if _, err := pool.Exec(ctx, `UPDATE sessions SET ended = true WHERE session_id = 's-led'`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DeriveDirty(ctx, store.DeriveConfig{Now: func() time.Time { return now }}); err != nil {
		t.Fatalf("fold: %v", err)
	}

	// The pre-existing ledger row: the old query posted turn 1 under the
	// HOOK prompt's seq (10), before the fold made the transcript copy
	// (seq 2) canonical.
	prefix := threadPrefix("s-led")
	if _, err := pool.Exec(ctx, `
		INSERT INTO slack_posts (key, email, session_id, claimed_at, attempts, posted_at, channel, message_ts)
		VALUES ($1, 'ana@example.org', 's-led', $2, 1, $2, 'C1', '1.1')`,
		liveTurnKey(prefix, 10), now.Add(-15*time.Minute)); err != nil {
		t.Fatal(err)
	}

	turns, err := liveTurns(ctx, pool, "s-led", prefix, now.Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("liveTurns: %v", err)
	}
	if len(turns) != 1 || turns[0].Text != "second ask" {
		t.Fatalf("offered %+v, want only the second turn: the first was posted under its hook copy's seq", turns)
	}
	// The new key is the canonical (transcript) prompt's seq, 4.
	if turns[0].Seq != 4 {
		t.Errorf("turn 2 keyed on seq %d, want the canonical transcript prompt's 4", turns[0].Seq)
	}
	if turns[0].Reply != "second answer" {
		t.Errorf("reply = %q", turns[0].Reply)
	}
	// Once posted under the canonical seq it is not offered again either.
	if _, err := pool.Exec(ctx, `
		INSERT INTO slack_posts (key, email, session_id, claimed_at, attempts, posted_at, channel, message_ts)
		VALUES ($1, 'ana@example.org', 's-led', $2, 1, $2, 'C1', '1.2')`,
		liveTurnKey(prefix, 4), now); err != nil {
		t.Fatal(err)
	}
	if turns, _ = liveTurns(ctx, pool, "s-led", prefix, now.Add(-time.Hour), 10); len(turns) != 0 {
		t.Errorf("a posted turn was offered again: %+v", turns)
	}
}

// TestIntegrationNoWrapperIsEverQuotedAsThePerson: every kind the normalizer
// gives a harness record is ingested as a user prompt beside two human
// prompts; the live thread offers the two human turns only, and no wrapper's
// text appears in any rendered exchange.
func TestIntegrationNoWrapperIsEverQuotedAsThePerson(t *testing.T) {
	freshDB(t)
	ctx := context.Background()
	addPerson(t, "ana@example.org")
	now := time.Now().UTC()
	addSession(t, sessionRow{id: "s-wrap", email: "ana@example.org", ended: true, updatedAt: now})
	st := store.New(pool, nil)

	wrappers := map[normalize.Kind]string{
		normalize.KindCaveat:             "<local-command-caveat>Caveat: the messages below were generated by the user while running local commands.</local-command-caveat>",
		normalize.KindCommandOutput:      "<local-command-stdout>WRAPPER-OUTPUT total 0</local-command-stdout>",
		normalize.KindTaskNotification:   "<task-notification>\n<task-id>abc</task-id>\n<status>completed</status>\n<summary>WRAPPER-TASK done</summary>\n</task-notification>",
		normalize.KindSystemReminder:     "<system-reminder>\nWRAPPER-REMINDER be brief\n</system-reminder>",
		normalize.KindSystemNotification: "[SYSTEM NOTIFICATION] WRAPPER-SYSTEM the queue is full",
		normalize.KindTeammateMessage:    "<teammate-message teammate_id=\"x\">WRAPPER-TEAMMATE hello</teammate-message>",
		normalize.KindInterrupted:        "[Request interrupted by user]",
		normalize.KindIDEContext:         "<ide_opened_file>WRAPPER-IDE main.go</ide_opened_file>",
		normalize.KindCompactSummary:     "This session is being continued from a previous conversation that ran out of context. WRAPPER-SUMMARY",
		normalize.KindHarnessInjected:    "<command-message>WRAPPER-EMPTY</command-message>",
		normalize.KindEnvContext:         "<environment_context>\n  <cwd>/home/WRAPPER-ENV</cwd>\n  <shell>zsh</shell>\n</environment_context>",
	}
	// The normalizer must agree the fixtures are what they claim, or the
	// test proves nothing about wrappers.
	for kind, text := range wrappers {
		if got := normalize.ClassifyUser(text, nil, false, false).Kind; got != kind {
			t.Fatalf("fixture for %s classifies as %s", kind, got)
		}
	}

	var batch []store.Ingest
	seq := int64(1)
	add := func(typ event.Type, at time.Time, text string) {
		batch = append(batch, store.Ingest{Email: "ana@example.org", Event: event.Event{
			ID: fmt.Sprintf("s-wrap-%d", seq), SessionID: "s-wrap", Seq: seq, Type: typ,
			Source: event.SourceClaudeCode, Origin: event.OriginHook, OccurredAt: at, Text: text,
		}})
		seq++
	}
	base := now.Add(-30 * time.Minute)
	add(event.UserPrompt, base, "human ask one")
	i := 0
	for _, text := range wrappers {
		add(event.UserPrompt, base.Add(time.Duration(i+1)*time.Second), text)
		i++
	}
	add(event.AssistantTurn, base.Add(time.Minute), "answer one")
	add(event.UserPrompt, base.Add(2*time.Minute), "human ask two")
	add(event.AssistantTurn, base.Add(3*time.Minute), "answer two")
	add(event.SessionEnded, base.Add(4*time.Minute), "")
	if _, err := st.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := st.DeriveDirty(ctx, store.DeriveConfig{Now: func() time.Time { return now }}); err != nil {
		t.Fatalf("fold: %v", err)
	}

	turns, err := liveTurns(ctx, pool, "s-wrap", threadPrefix("s-wrap"), now.Add(-time.Hour), 50)
	if err != nil {
		t.Fatalf("liveTurns: %v", err)
	}
	if len(turns) != 2 || turns[0].Text != "human ask one" || turns[1].Text != "human ask two" {
		t.Fatalf("offered %+v, want exactly the two human turns", turns)
	}
	for _, tr := range turns {
		rendered := liveTurnText(tr, "ana@example.org")
		if strings.Contains(rendered, "WRAPPER-") || strings.Contains(rendered, "<") && strings.Contains(rendered, "-message>") {
			t.Errorf("a wrapper was quoted as the person: %q", rendered)
		}
	}
	if turns[0].Reply != "answer one" || turns[1].Reply != "answer two" {
		t.Errorf("replies = %q / %q", turns[0].Reply, turns[1].Reply)
	}
}
