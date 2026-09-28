//go:build integration

package store

// The analytics queries are aggregate SQL with three things a fake cannot
// vouch for: the timezone day-bucketing, the whitelisted ORDER BY expressions,
// and the scoping that keeps a member's aggregates to themselves. All three run
// against real Postgres here.

import (
	"context"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// seedUsage ingests a session for email with a known token spend at a given
// start time.
func seedUsage(t *testing.T, s *Store, email, sid string, at time.Time, tokens int64) {
	t.Helper()
	batch := session(t, email, sid)
	for i := range batch {
		batch[i].Event.OccurredAt = at.Add(time.Duration(i) * time.Minute)
	}
	batch[1] = withUsage(ingestOf(sid+"-turn", sid, email, event.AssistantTurn, 2),
		"claude-opus-5", "m-"+sid, "r-"+sid, tokens, tokens/10)
	batch[1].Event.OccurredAt = at.Add(time.Minute)
	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("seed %s: %v", sid, err)
	}
}

func TestIntegrationDailyUsageBucketsInTheRequestedZone(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	fresh(t, s)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)

	// 20:00 UTC on the 10th is 01:30 on the 11th in Kolkata. Which day this
	// session lands on is the whole point of passing a zone.
	at := time.Date(2026, 8, 10, 20, 0, 0, 0, time.UTC)
	seedUsage(t, s, email, "s-tz", at, 1000)

	v := Viewer{Email: email, Role: RoleMember}
	from := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)

	utc, err := s.DailyUsage(context.Background(), v, from, to, "UTC", "day", nil, nil)
	if err != nil {
		t.Fatalf("DailyUsage UTC: %v", err)
	}
	kolkata, err := s.DailyUsage(context.Background(), v, from, to, "Asia/Kolkata", "day", nil, nil)
	if err != nil {
		t.Fatalf("DailyUsage Kolkata: %v", err)
	}
	if len(utc) != 1 || utc[0].Day.Day() != 10 {
		t.Errorf("UTC bucketing put the session on day %v, want the 10th", utc)
	}
	if len(kolkata) != 1 || kolkata[0].Day.Day() != 11 {
		t.Errorf("Kolkata bucketing put the session on day %v, want the 11th", kolkata)
	}

	// A zone Postgres would choke on is refused with a message naming it.
	if _, err := s.DailyUsage(context.Background(), v, from, to, "Mars/Olympus_Mons", "day", nil, nil); err == nil {
		t.Error("a nonsense timezone was accepted")
	}
}

func TestIntegrationAnalyticsScopesAMemberToThemselves(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	fresh(t, s)
	const me, other = "me@example.com", "other@example.com"
	mustPrincipal(t, s, me, RoleMember)
	mustPrincipal(t, s, other, RoleMember)

	at := time.Now().UTC().Add(-time.Hour)
	seedUsage(t, s, me, "s-mine", at, 1000)
	seedUsage(t, s, other, "s-theirs", at, 500_000)

	from, to := at.Add(-24*time.Hour), at.Add(24*time.Hour)
	ctx := context.Background()

	// The member's totals contain none of the other person's spend, however
	// loudly the emails parameter asks.
	days, err := s.DailyUsage(ctx, Viewer{Email: me, Role: RoleMember}, from, to, "UTC", "day", []string{other}, nil)
	if err != nil {
		t.Fatalf("DailyUsage: %v", err)
	}
	var tokens int64
	for _, d := range days {
		tokens += d.TokensIn
	}
	if tokens >= 500_000 {
		t.Fatalf("a member's aggregate includes a colleague's tokens: %d", tokens)
	}

	people, err := s.PersonUsage(ctx, Viewer{Email: me, Role: RoleMember}, from, to, "tokens", "", 50, nil)
	if err != nil {
		t.Fatalf("PersonUsage: %v", err)
	}
	for _, p := range people {
		if p.Email != me {
			t.Errorf("a member's person table lists %s", p.Email)
		}
	}

	// And the admin sees both.
	all, err := s.PersonUsage(ctx, Viewer{Email: "admin@x", Role: RoleAdmin}, from, to, "tokens", "", 50, nil)
	if err != nil {
		t.Fatalf("admin PersonUsage: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("admin sees %d people, want 2", len(all))
	}
	// Sorted by tokens: the heavy spender first. This exercises the ORDER BY
	// expression that repeats the aggregates, which an alias would have broken.
	if len(all) == 2 && all[0].Email != other {
		t.Errorf("tokens sort put %s first", all[0].Email)
	}
}

func TestIntegrationPersonUsagePrefixAndSorts(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	fresh(t, s)
	mustPrincipal(t, s, "ann@example.com", RoleMember)
	mustPrincipal(t, s, "bea@example.com", RoleMember)

	at := time.Now().UTC().Add(-time.Hour)
	seedUsage(t, s, "ann@example.com", "s-ann", at, 1000)
	seedUsage(t, s, "bea@example.com", "s-bea", at, 2000)

	from, to := at.Add(-24*time.Hour), at.Add(24*time.Hour)
	admin := Viewer{Email: "admin@x", Role: RoleAdmin}
	ctx := context.Background()

	got, err := s.PersonUsage(ctx, admin, from, to, "tokens", "AN", 50, nil)
	if err != nil {
		t.Fatalf("PersonUsage prefix: %v", err)
	}
	if len(got) != 1 || got[0].Email != "ann@example.com" {
		t.Errorf("case-insensitive prefix AN matched %+v", got)
	}

	// Every whitelisted sort must execute; a typo'd entry in the map is a
	// runtime error on a page, not a compile error.
	for _, sortKey := range []string{"sessions", "tokens", "cost", "recent", "nonsense"} {
		if _, err := s.PersonUsage(ctx, admin, from, to, sortKey, "", 10, nil); err != nil {
			t.Errorf("sort %q failed: %v", sortKey, err)
		}
	}

	// An ILIKE metacharacter in the prefix is literal input from a search box,
	// not a pattern; it must not error and must not match everything.
	got, err = s.PersonUsage(ctx, admin, from, to, "tokens", "%", 50, nil)
	if err != nil {
		t.Fatalf("metacharacter prefix errored: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("%% matched %d people; a literal %% prefix matches nobody", len(got))
	}
}

func TestIntegrationSessionCountsAndFilterOptions(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	fresh(t, s)
	mustPrincipal(t, s, "ann@example.com", RoleMember)
	mustPrincipal(t, s, "bea@example.com", RoleMember)

	at := time.Now().UTC().Add(-time.Hour)
	seedUsage(t, s, "ann@example.com", "s-1", at, 100)
	seedUsage(t, s, "ann@example.com", "s-2", at.Add(time.Minute), 100)
	seedUsage(t, s, "bea@example.com", "s-3", at, 100)

	counts, err := s.SessionCounts(context.Background())
	if err != nil {
		t.Fatalf("SessionCounts: %v", err)
	}
	if counts["ann@example.com"] != 2 || counts["bea@example.com"] != 1 {
		t.Errorf("counts = %v", counts)
	}

	// An admin's dropdowns list everyone; a member's list only themselves. The
	// member case is the one that matters: the org's people and repos are a
	// directory the visibility model must not leak through an autocomplete.
	people, repos, err := s.FilterOptions(context.Background(), Viewer{Email: "admin@x", Role: RoleAdmin})
	if err != nil {
		t.Fatalf("FilterOptions admin: %v", err)
	}
	if len(people) != 2 {
		t.Errorf("admin sees %d people in the dropdown, want 2", len(people))
	}
	if len(repos) != 1 || repos[0] != "loop-sessions" {
		t.Errorf("admin repos = %v", repos)
	}

	people, _, err = s.FilterOptions(context.Background(), Viewer{Email: "ann@example.com", Role: RoleMember})
	if err != nil {
		t.Fatalf("FilterOptions member: %v", err)
	}
	if len(people) != 1 || people[0] != "ann@example.com" {
		t.Errorf("a member's people dropdown lists %v", people)
	}
}
