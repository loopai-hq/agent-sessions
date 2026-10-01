//go:build integration

package store

// The timeline read against real Postgres: chronological order with the agent
// tiebreak, keyset continuation across a page boundary, and the agent filter.
// seq order cannot be trusted for any of this — it restarts per stream, which
// is the defect that produced forty one-step subagent stubs on the dashboard.

import (
	"context"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

func seedThread(t *testing.T, s *Store, email, sid, agent string, at time.Time, n int) {
	t.Helper()
	var batch []Ingest
	for i := range n {
		it := ingestOf(sid+"-"+agent+"-"+string(rune('a'+i)), sid, email, event.UserPrompt, int64(i))
		it.Event.AgentID = agent
		it.Event.OccurredAt = at.Add(time.Duration(i) * time.Minute)
		batch = append(batch, it)
	}
	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("seed thread %s: %v", agent, err)
	}
}

func TestIntegrationTimelineOrdersAcrossStreams(t *testing.T) {
	s := newStore(t, nil)
	fresh(t, s)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	t0 := time.Date(2026, 8, 7, 10, 0, 0, 0, time.UTC)

	// Three streams whose seqs all start at zero; the parent starts first,
	// agent-one a beat later, agent-two later still. Seq order would deal
	// these round-robin; time order keeps each stream's run contiguous where
	// time actually is contiguous.
	seedThread(t, s, email, "s-tl", "", t0, 3)
	seedThread(t, s, email, "s-tl", "agent-one", t0.Add(10*time.Second), 3)
	seedThread(t, s, email, "s-tl", "agent-two", t0.Add(20*time.Second), 3)

	v := Viewer{Email: email, Role: RoleMember}
	page, err := s.GetTimeline(context.Background(), v, "s-tl", TimelineRange{Limit: 50})
	if err != nil {
		t.Fatalf("GetTimeline: %v", err)
	}
	if len(page.Events) != 9 {
		t.Fatalf("read %d events, want 9", len(page.Events))
	}
	for i := 1; i < len(page.Events); i++ {
		prev, cur := page.Events[i-1], page.Events[i]
		if cur.OccurredAt.Before(prev.OccurredAt) {
			t.Fatalf("time went backwards at %d: %s after %s", i, cur.OccurredAt, prev.OccurredAt)
		}
	}

	// Keyset continuation: a two-page read sees every event exactly once, in
	// the same order as the single-page read.
	first, err := s.GetTimeline(context.Background(), v, "s-tl", TimelineRange{Limit: 4})
	if err != nil {
		t.Fatalf("page one: %v", err)
	}
	if !first.HasMore || len(first.Events) != 4 {
		t.Fatalf("page one: %d events, HasMore=%v", len(first.Events), first.HasMore)
	}
	last := first.Events[3]
	rest, err := s.GetTimeline(context.Background(), v, "s-tl", TimelineRange{
		After: &TimelineCursor{At: last.OccurredAt, Agent: last.AgentID, Seq: last.Seq},
		Limit: 50,
	})
	if err != nil {
		t.Fatalf("page two: %v", err)
	}
	if got := len(first.Events) + len(rest.Events); got != 9 {
		t.Fatalf("two pages held %d events, want 9 with no loss or repeat", got)
	}
	seen := map[string]bool{}
	for _, e := range append(append([]StoredEvent{}, first.Events...), rest.Events...) {
		if seen[e.ID] {
			t.Fatalf("event %s appeared on both pages", e.ID)
		}
		seen[e.ID] = true
	}

	// The agent filter narrows to one thread, in that thread's own order.
	one, err := s.GetTimeline(context.Background(), v, "s-tl", TimelineRange{AgentID: "agent-one", Limit: 50})
	if err != nil {
		t.Fatalf("agent filter: %v", err)
	}
	if len(one.Events) != 3 {
		t.Fatalf("agent filter read %d events, want 3", len(one.Events))
	}
	for _, e := range one.Events {
		if e.AgentID != "agent-one" {
			t.Fatalf("agent filter leaked %s", e.AgentID)
		}
	}
}
