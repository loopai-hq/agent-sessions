//go:build integration

package slack

// The group SQL against real Postgres: visibility scoping, the activation
// read's joins (disabled groups out, master kill honoured, closed threads
// out), and the promotion of flag spellings into activations — name shapes
// must become activations and never fall through to the channel path.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/server/store"
)

func TestIntegrationGroupVisibilityAndResolution(t *testing.T) {
	freshDB(t)
	db := pool
	ctx := context.Background()
	addPerson(t, "ana@example.org")
	addPerson(t, "bob@example.org")

	now := time.Now().UTC()
	mine, err := saveGroup(ctx, db, Group{Name: "mine", OwnerEmail: "ana@example.org",
		Visibility: "private", Destination: "dm"}, now)
	if err != nil {
		t.Fatalf("create private: %v", err)
	}
	_, err = saveGroup(ctx, db, Group{Name: "eng", OwnerEmail: "bob@example.org",
		Visibility: "org", Destination: "C1ENG12345"}, now)
	if err != nil {
		t.Fatalf("create org: %v", err)
	}

	// Ana sees her private group and Bob's org one; Bob does not see Ana's.
	anas, err := listGroups(ctx, db, "ana@example.org")
	if err != nil || len(anas) != 2 {
		t.Fatalf("ana sees %d groups (%v), want 2", len(anas), err)
	}
	// Deleting one's own group works and cascades; deleting another's refuses.
	if err := deleteGroup(ctx, db, "bob@example.org", mine.ID); err == nil {
		t.Fatal("bob deleted ana's group")
	}
	bobs, err := listGroups(ctx, db, "bob@example.org")
	if err != nil || len(bobs) != 1 {
		t.Fatalf("bob sees %d groups (%v), want only his own", len(bobs), err)
	}

	// A person destination (U…) survives the schema CHECK end to end.
	if _, err := saveGroup(ctx, db, Group{Name: "morgans-dm", OwnerEmail: "ana@example.org",
		Visibility: "private", Destination: "U1MORGAN23"}, now); err != nil {
		t.Fatalf("a U… destination was refused by the schema: %v", err)
	}

	// Resolution prefers one's own name over an org duplicate.
	if _, err := saveGroup(ctx, db, Group{Name: "eng", OwnerEmail: "ana@example.org",
		Visibility: "private", Destination: "C1ANA98765"}, now); err != nil {
		t.Fatalf("duplicate name across owners must be allowed: %v", err)
	}
	g, err := resolveGroup(ctx, db, "ana@example.org", "eng")
	if err != nil || g.OwnerEmail != "ana@example.org" {
		t.Fatalf("resolution picked %s's group (%v), want ana's own", g.OwnerEmail, err)
	}
	if _, err := resolveGroup(ctx, db, "bob@example.org", "mine"); err == nil {
		t.Fatal("bob resolved ana's private group")
	}
	_ = mine
}

func TestIntegrationGroupEligibilityGates(t *testing.T) {
	freshDB(t)
	db := pool
	ctx := context.Background()
	addPerson(t, "ana@example.org")
	now := time.Now().UTC()

	g, err := saveGroup(ctx, db, Group{Name: "eng", OwnerEmail: "ana@example.org",
		Visibility: "org", Destination: "C1ENG12345"}, now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	addSession(t, sessionRow{id: "s-1", email: "ana@example.org", updatedAt: now.Add(-10 * time.Minute)})
	if err := attachSession(ctx, db, "s-1", g.ID, "ana@example.org", now); err != nil {
		t.Fatalf("attach: %v", err)
	}

	acts, err := groupEligible(ctx, db, now.Add(-24*time.Hour), 10)
	if err != nil || len(acts) != 1 {
		t.Fatalf("eligible = %d (%v), want the attached pair", len(acts), err)
	}
	if acts[0].Destination != "C1ENG12345" || acts[0].GroupID != g.ID {
		t.Fatalf("activation = %+v", acts[0])
	}

	// The master kill silences everything without touching the attachment.
	if _, err := db.Exec(ctx, `
		INSERT INTO slack_prefs (email, mode, mirror_from, updated_at, live_disabled)
		VALUES ('ana@example.org', 'off', now(), now(), TRUE)
		ON CONFLICT (email) DO UPDATE SET live_disabled = TRUE`); err != nil {
		t.Fatalf("set kill: %v", err)
	}
	acts, err = groupEligible(ctx, db, now.Add(-24*time.Hour), 10)
	if err != nil || len(acts) != 0 {
		t.Fatalf("master kill left %d eligible (%v)", len(acts), err)
	}
	if _, err := db.Exec(ctx,
		`UPDATE slack_prefs SET live_disabled = FALSE WHERE email = 'ana@example.org'`); err != nil {
		t.Fatal(err)
	}

	// A ten-day session that is active RIGHT NOW is eligible: freshness is
	// judged by updated_at, and a compaction continuation's parent id does
	// not disqualify an explicit attachment. The fleet's longest sessions
	// are precisely the ones people attach mid-flight.
	addSession(t, sessionRow{id: "s-old-parent", email: "ana@example.org",
		updatedAt: now.Add(-30 * 24 * time.Hour)})
	addSession(t, sessionRow{id: "s-long", email: "ana@example.org", parent: "s-old-parent",
		updatedAt: now.Add(-2 * time.Minute)})
	if _, err := db.Exec(ctx,
		`UPDATE sessions SET started_at = now() - interval '10 days' WHERE session_id = 's-long'`); err != nil {
		t.Fatal(err)
	}
	if err := attachSession(ctx, db, "s-long", g.ID, "ana@example.org", now); err != nil {
		t.Fatalf("attach long: %v", err)
	}
	acts, err = groupEligible(ctx, db, now.Add(-12*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	var sawLong bool
	for _, a := range acts {
		if a.SessionID == "s-long" {
			sawLong = true
		}
		if a.SessionID == "s-old-parent" {
			t.Fatal("an unattended stale session became eligible")
		}
	}
	if !sawLong {
		t.Fatal("an active ten-day continuation, explicitly attached, was not eligible")
	}
	if err := detachSession(ctx, db, "s-long", g.ID, now); err != nil {
		t.Fatal(err)
	}

	// A disabled group stops narrating; a detached pair stops narrating.
	if _, err := db.Exec(ctx, `UPDATE slack_groups SET disabled = TRUE WHERE id = $1`, g.ID); err != nil {
		t.Fatal(err)
	}
	if acts, _ := groupEligible(ctx, db, now.Add(-24*time.Hour), 10); len(acts) != 0 {
		t.Fatal("a disabled group stayed eligible")
	}
	if _, err := db.Exec(ctx, `UPDATE slack_groups SET disabled = FALSE WHERE id = $1`, g.ID); err != nil {
		t.Fatal(err)
	}
	if err := detachSession(ctx, db, "s-1", g.ID, now); err != nil {
		t.Fatal(err)
	}
	if acts, _ := groupEligible(ctx, db, now.Add(-24*time.Hour), 10); len(acts) != 0 {
		t.Fatal("a detached pair stayed eligible")
	}
}

func TestIntegrationPromotionResolvesNamesAndDefaults(t *testing.T) {
	freshDB(t)
	db := pool
	ctx := context.Background()
	addPerson(t, "ana@example.org")
	now := time.Now().UTC()

	g, err := saveGroup(ctx, db, Group{Name: "eng-sessions", OwnerEmail: "ana@example.org",
		Visibility: "private", Destination: "C1ENG12345"}, now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// A name-shaped request becomes an activation…
	addSession(t, sessionRow{id: "s-name", email: "ana@example.org", updatedAt: now.Add(-5 * time.Minute)})
	if _, err := db.Exec(ctx,
		`UPDATE sessions SET mirror_request = 'eng-sessions' WHERE session_id = 's-name'`); err != nil {
		t.Fatal(err)
	}
	// …an 'on' request follows the default group…
	if _, err := db.Exec(ctx, `
		INSERT INTO slack_prefs (email, mode, mirror_from, updated_at, default_group)
		VALUES ('ana@example.org', 'off', now(), now(), $1)
		ON CONFLICT (email) DO UPDATE SET default_group = $1`, g.ID); err != nil {
		t.Fatal(err)
	}
	addSession(t, sessionRow{id: "s-on", email: "ana@example.org", updatedAt: now.Add(-4 * time.Minute)})
	if _, err := db.Exec(ctx,
		`UPDATE sessions SET mirror_request = 'on' WHERE session_id = 's-on'`); err != nil {
		t.Fatal(err)
	}

	m := &Mirror{db: db, now: func() time.Time { return now }}
	if err := m.promoteDefaults(ctx, now); err != nil {
		t.Fatalf("promote: %v", err)
	}
	acts, err := groupEligible(ctx, db, now.Add(-24*time.Hour), 10)
	if err != nil || len(acts) != 2 {
		t.Fatalf("promotion produced %d activations (%v), want 2", len(acts), err)
	}

	// …and the legacy path refuses the name shape outright, so nothing ever
	// treats "eng-sessions" as a channel id.
	legacy, err := liveEligible(ctx, db, now.Add(-24*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range legacy {
		if c.SessionID == "s-name" {
			t.Fatal("a name-shaped request reached the legacy channel path")
		}
	}
}

// TestIntegrationLiveTurnsPairsExchangesFromTheHorizon runs the real SQL over
// the derive layer's turns: an exchange is the turn's prompt paired with the
// answer the fold closed it on (the LAST assistant text); nothing before the
// attachment horizon is offered; the in-flight turn of a running session
// waits until the session ends and the fold settles it.
func TestIntegrationLiveTurnsPairsExchangesFromTheHorizon(t *testing.T) {
	freshDB(t)
	db := pool
	ctx := context.Background()
	addPerson(t, "ana@example.org")
	now := time.Now().UTC()
	addSession(t, sessionRow{id: "s-x", email: "ana@example.org", updatedAt: now})

	st := store.New(pool, nil)
	type ev struct {
		seq  int64
		typ  event.Type
		at   time.Time
		text string
	}
	evs := []ev{
		{1, event.UserPrompt, now.Add(-60 * time.Minute), "ancient ask"},
		{2, event.AssistantTurn, now.Add(-59 * time.Minute), "ancient answer"},
		{3, event.UserPrompt, now.Add(-10 * time.Minute), "recent ask"},
		{4, event.AssistantTurn, now.Add(-9 * time.Minute), "thinking aloud"},
		{5, event.AssistantTurn, now.Add(-8 * time.Minute), "final answer"},
		{6, event.UserPrompt, now.Add(-2 * time.Minute), "in-flight ask"},
	}
	var batch []store.Ingest
	for _, e := range evs {
		batch = append(batch, store.Ingest{Email: "ana@example.org", Event: event.Event{
			ID: fmt.Sprintf("s-x-%d", e.seq), SessionID: "s-x", Seq: e.seq, Type: e.typ,
			Source: event.SourceClaudeCode, Origin: event.OriginHook, OccurredAt: e.at, Text: e.text,
		}})
	}
	if _, err := st.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	fold := func() {
		if _, err := st.DeriveDirty(ctx, store.DeriveConfig{Now: func() time.Time { return now }}); err != nil {
			t.Fatalf("fold: %v", err)
		}
	}
	fold()

	horizon := now.Add(-30 * time.Minute)
	turns, err := liveTurns(ctx, db, "s-x", "thread:s-x:g1", horizon, 10)
	if err != nil {
		t.Fatalf("liveTurns: %v", err)
	}
	if len(turns) != 1 {
		t.Fatalf("got %d exchanges, want exactly the completed post-horizon one: %+v", len(turns), turns)
	}
	if turns[0].Text != "recent ask" || turns[0].Reply != "final answer" || turns[0].Seq != 3 || turns[0].Kind != "human" {
		t.Fatalf("exchange = %+v; the reply must be the LAST assistant text of the turn, keyed on the prompt's seq", turns[0])
	}

	// The end of the session settles the in-flight turn, replyless or not.
	end := store.Ingest{Email: "ana@example.org", Event: event.Event{
		ID: "s-x-end", SessionID: "s-x", Seq: 7, Type: event.SessionEnded,
		Source: event.SourceClaudeCode, Origin: event.OriginHook, OccurredAt: now.Add(-time.Minute),
	}}
	if _, err := st.UpsertEvents(ctx, []store.Ingest{end}); err != nil {
		t.Fatal(err)
	}
	fold()
	turns, err = liveTurns(ctx, db, "s-x", "thread:s-x:g1", horizon, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 || turns[1].Text != "in-flight ask" || turns[1].Reply != "" || turns[1].Seq != 6 {
		t.Fatalf("ended session offers %+v, want the in-flight turn released", turns)
	}
}
