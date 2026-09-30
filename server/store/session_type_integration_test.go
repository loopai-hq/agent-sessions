//go:build integration

package store

// Session-type derivation end to end against real Postgres: the ingest path
// stamps the rollup, the merge is monotone whatever order batches arrive in,
// and the listing and analytics filters read the column the writer wrote.
//
// The second half of this file is the lattice: empty is a floor a row can
// only be born on, internal is recomputed from the opening prompt, automation
// absorbs, titles never come from wrappers, lineage is written only with a
// source, and the store default hides the two classes nobody asked for.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/normalize"
)

func seedTypedSession(t *testing.T, s *Store, email, sid, entrypoint string, at time.Time) {
	t.Helper()
	batch := session(t, email, sid)
	for i := range batch {
		// The shared fixture names its events the same way for every session;
		// two sessions seeded from it would otherwise share ids, and the
		// second batch would be all duplicates that never fold.
		batch[i].Event.ID = sid + ":" + batch[i].Event.ID
		batch[i].Event.OccurredAt = at.Add(time.Duration(i) * time.Minute)
		batch[i].Event.Raw = json.RawMessage(`{"entrypoint":"` + entrypoint + `"}`)
	}
	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("seed %s: %v", sid, err)
	}
}

func sessionTypeInDB(t *testing.T, s *Store, sid string) string {
	t.Helper()
	var ty string
	if err := s.db.QueryRow(context.Background(),
		`SELECT session_type FROM sessions WHERE session_id = $1`, sid).Scan(&ty); err != nil {
		t.Fatalf("read session_type: %v", err)
	}
	return ty
}

func TestIntegrationSessionTypeDerivedAtIngest(t *testing.T) {
	s := newStore(t, nil)
	fresh(t, s)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	seedTypedSession(t, s, email, "s-robot", "sdk-cli", at)
	seedTypedSession(t, s, email, "s-person", "cli", at)

	if got := sessionTypeInDB(t, s, "s-robot"); got != "automation" {
		t.Errorf("headless session stored as %q, want automation", got)
	}
	if got := sessionTypeInDB(t, s, "s-person"); got != "user" {
		t.Errorf("interactive session stored as %q, want user", got)
	}
}

// A later batch with no evidence must not demote the session: batches arrive in
// any order, and only one of them carries the record with the entrypoint.
func TestIntegrationSessionTypeMergeIsMonotone(t *testing.T) {
	s := newStore(t, nil)
	fresh(t, s)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	seedTypedSession(t, s, email, "s-mono", "sdk-cli", at)
	// A second increment for the same session, evidence-free.
	later := ingestOf("s-mono-late", "s-mono", email, event.UserPrompt, 50)
	later.Event.OccurredAt = at.Add(time.Hour)
	later.Event.Raw = json.RawMessage(`{}`)
	if _, err := s.UpsertEvents(context.Background(), []Ingest{later}); err != nil {
		t.Fatalf("late batch: %v", err)
	}
	if got := sessionTypeInDB(t, s, "s-mono"); got != "automation" {
		t.Errorf("an evidence-free later batch demoted the session to %q", got)
	}
}

// A re-walk re-delivers a session's events; every row is a duplicate or an
// in-place upgrade, none is fresh. The evidence riding on them must still
// reach the rollup — this is exactly how sessions captured live before the
// entrypoint existed get corrected after the fleet re-walks.
func TestIntegrationRedeliveryWithEvidencePromotesTheSession(t *testing.T) {
	s := newStore(t, nil)
	fresh(t, s)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	// First delivery: an old agent's events, no evidence anywhere.
	batch := session(t, email, "s-live")
	for i := range batch {
		batch[i].Event.OccurredAt = at.Add(time.Duration(i) * time.Minute)
	}
	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("live delivery: %v", err)
	}
	if got := sessionTypeInDB(t, s, "s-live"); got != "user" {
		t.Fatalf("pre-rewalk session stored as %q, want user", got)
	}

	// Re-delivery of the same ids by a newer agent that stamps the field.
	for i := range batch {
		batch[i].Event.Entrypoint = "sdk-cli"
	}
	res, err := s.UpsertEvents(context.Background(), batch)
	if err != nil {
		t.Fatalf("re-delivery: %v", err)
	}
	if len(res.Inserted) != 0 {
		t.Fatalf("re-delivery inserted %d rows; the fixture is broken", len(res.Inserted))
	}
	if got := sessionTypeInDB(t, s, "s-live"); got != "automation" {
		t.Errorf("an all-duplicate re-delivery with evidence left the session %q", got)
	}
}

func TestIntegrationListAndAnalyticsFilterBySessionType(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	fresh(t, s)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	seedTypedSession(t, s, email, "s-a1", "sdk-cli", at)
	seedTypedSession(t, s, email, "s-u1", "cli", at)
	seedTypedSession(t, s, email, "s-u2", "claude-desktop", at.Add(time.Hour))

	ctx := context.Background()
	v := Viewer{Email: email, Role: RoleMember}

	list := func(types ...string) int {
		page, err := s.ListSessions(ctx, v, SessionFilter{Types: types})
		if err != nil {
			t.Fatalf("ListSessions %v: %v", types, err)
		}
		return len(page.Sessions)
	}
	if n := list("user"); n != 2 {
		t.Errorf("user filter listed %d sessions, want 2", n)
	}
	if n := list("automation"); n != 1 {
		t.Errorf("automation filter listed %d sessions, want 1", n)
	}
	if n := list(); n != 3 {
		t.Errorf("no filter listed %d sessions, want 3", n)
	}
	// An unknown value vanishes before SQL rather than erroring or matching,
	// and a selection made only of unknown values is still a selection: it
	// lists nothing, rather than quietly becoming the default view the caller
	// did not ask for. Known values beside it are honoured as usual.
	if n := list("robot"); n != 0 {
		t.Errorf("an unknown-only type selection listed %d rows, want 0", n)
	}
	if n := list("robot", "automation"); n != 1 {
		t.Errorf("an unknown value beside a known one listed %d rows, want 1", n)
	}

	from, to := at.Add(-time.Hour), at.Add(24*time.Hour)
	byType, err := s.DailyUsageByType(ctx, v, from, to, "UTC", "day", nil)
	if err != nil {
		t.Fatalf("DailyUsageByType: %v", err)
	}
	counts := map[string]int64{}
	for _, d := range byType {
		counts[d.SessionType] += d.Sessions
	}
	if counts["user"] != 2 || counts["automation"] != 1 {
		t.Errorf("stacked rows counted %v, want user:2 automation:1", counts)
	}

	days, err := s.DailyUsage(ctx, v, from, to, "UTC", "day", nil, []string{"automation"})
	if err != nil {
		t.Fatalf("DailyUsage typed: %v", err)
	}
	var n int64
	for _, d := range days {
		n += d.Sessions
	}
	if n != 1 {
		t.Errorf("automation-filtered daily usage counted %d sessions, want 1", n)
	}

	people, err := s.PersonUsage(ctx, v, from, to, "tokens", "", 10, []string{"user"})
	if err != nil {
		t.Fatalf("PersonUsage typed: %v", err)
	}
	if len(people) != 1 || people[0].Sessions != 2 {
		t.Errorf("user-filtered person usage = %+v, want one person with 2 sessions", people)
	}
}

// ---------------------------------------------------------------------------
// The lattice
// ---------------------------------------------------------------------------

// hookAt builds one hook-origin event at an explicit time.
func hookAt(id, sid, email string, typ event.Type, seq int64, at time.Time, text string) Ingest {
	in := ingestOf(id, sid, email, typ, seq)
	in.Event.OccurredAt = at
	in.Event.Text = text
	return in
}

// sessionRow reads the lattice columns of one session.
type latticeRow struct {
	Type, EmptyKind, HeadState, TitleSource string
	FirstPrompt, Parent, ParentRecord       *string
	Lineage                                 string
	Content, Human                          int
	AgentVersions                           []string
}

func latticeOf(t *testing.T, s *Store, sid string) latticeRow {
	t.Helper()
	var r latticeRow
	var emptyKind *string
	if err := s.db.QueryRow(context.Background(), `
		SELECT session_type, empty_kind, head_state, title_source, first_prompt,
		       parent_session_id, parent_record_uuid, lineage_source,
		       content_events, human_turns, agent_versions
		FROM sessions WHERE session_id = $1`, sid).Scan(
		&r.Type, &emptyKind, &r.HeadState, &r.TitleSource, &r.FirstPrompt,
		&r.Parent, &r.ParentRecord, &r.Lineage, &r.Content, &r.Human, &r.AgentVersions); err != nil {
		t.Fatalf("read lattice columns of %s: %v", sid, err)
	}
	r.EmptyKind = deref(emptyKind)
	return r
}

func ingestBatch(t *testing.T, s *Store, items ...Ingest) {
	t.Helper()
	if _, err := s.UpsertEvents(context.Background(), items); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
}

// The SQL merge is exercised over every (old, new) pair, against the lattice
// the design states: NULL takes the new value; automation absorbs; internal
// beats user and empty; a user row never drops to empty; empty takes
// whatever arrives.
func TestIntegrationSessionTypeMergeOverEveryPair(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	want := map[[2]string]string{
		{"", "empty"}: "empty", {"", "user"}: "user", {"", "internal"}: "internal", {"", "automation"}: "automation",
		{"empty", "empty"}: "empty", {"empty", "user"}: "user", {"empty", "internal"}: "internal", {"empty", "automation"}: "automation",
		{"user", "empty"}: "user", {"user", "user"}: "user", {"user", "internal"}: "internal", {"user", "automation"}: "automation",
		{"internal", "empty"}: "internal", {"internal", "user"}: "internal", {"internal", "internal"}: "internal", {"internal", "automation"}: "automation",
		{"automation", "empty"}: "automation", {"automation", "user"}: "automation", {"automation", "internal"}: "automation", {"automation", "automation"}: "automation",
	}
	for pair, expect := range want {
		var old any
		if pair[0] != "" {
			old = pair[0]
		}
		var got string
		if err := s.db.QueryRow(ctx, `SELECT session_type_merge($1, $2)`, old, pair[1]).Scan(&got); err != nil {
			t.Fatalf("merge(%q, %q): %v", pair[0], pair[1], err)
		}
		if got != expect {
			t.Errorf("session_type_merge(%q, %q) = %q, want %q", pair[0], pair[1], got, expect)
		}
	}
	if len(want) != 20 {
		t.Fatalf("the table covers %d pairs, want all 20", len(want))
	}
}

// (i) A lifecycle-only session is empty: not listed, not counted, not in the
// stacked default totals, but listed the moment somebody asks for empties by
// name, and counted as its own class in the stack.
func TestIntegrationLifecycleOnlySessionIsEmptyAndHiddenByDefault(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	v := Viewer{Email: email, Role: RoleMember}
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	ingestBatch(t, s,
		hookAt("a1", "s-aborted", email, event.SessionStarted, 1, at, "startup"),
		hookAt("a2", "s-aborted", email, event.SessionEnded, 2, at.Add(600*time.Millisecond), "other"))
	ingestBatch(t, s,
		hookAt("b1", "s-blank", email, event.SessionStarted, 1, at, "startup"),
		hookAt("b2", "s-blank", email, event.SessionEnded, 2, at.Add(90*time.Second), "prompt_input_exit"))
	ingestBatch(t, s,
		hookAt("c1", "s-open", email, event.SessionStarted, 1, at, "startup"))
	ingestBatch(t, s,
		hookAt("d1", "s-real", email, event.SessionStarted, 1, at, "startup"),
		hookAt("d2", "s-real", email, event.UserPrompt, 2, at.Add(time.Second), "fix the bug"))

	for sid, want := range map[string]latticeRow{
		"s-aborted": {Type: "empty", EmptyKind: "aborted", HeadState: "unknown", TitleSource: "none"},
		"s-blank":   {Type: "empty", EmptyKind: "blank", HeadState: "unknown", TitleSource: "none"},
		"s-open":    {Type: "empty", EmptyKind: "", HeadState: "unknown", TitleSource: "none"},
		"s-real":    {Type: "user", EmptyKind: "", HeadState: "complete", TitleSource: "human"},
	} {
		got := latticeOf(t, s, sid)
		if got.Type != want.Type || got.EmptyKind != want.EmptyKind || got.HeadState != want.HeadState || got.TitleSource != want.TitleSource {
			t.Errorf("%s = %+v, want type %s kind %q head %s title %s", sid, got, want.Type, want.EmptyKind, want.HeadState, want.TitleSource)
		}
	}

	page, err := s.ListSessions(ctx, v, SessionFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 1 || page.Sessions[0].SessionID != "s-real" {
		t.Errorf("default listing = %v, want only s-real", ids(page.Sessions))
	}
	page, err = s.ListSessions(ctx, v, SessionFilter{Types: []string{"empty"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 3 {
		t.Errorf("explicit empty filter listed %d sessions, want 3", len(page.Sessions))
	}
	page, err = s.ListSessions(ctx, v, SessionFilter{Types: []string{"user", "empty"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 4 {
		t.Errorf("user+empty filter listed %d sessions, want 4", len(page.Sessions))
	}

	from, to := at.Add(-time.Hour), at.Add(time.Hour)
	days, err := s.DailyUsage(ctx, v, from, to, "UTC", "day", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, d := range days {
		n += d.Sessions
	}
	if n != 1 {
		t.Errorf("default daily usage counted %d sessions, want 1", n)
	}
	counts, err := s.SessionCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[email] != 1 {
		t.Errorf("roster count = %d, want 1", counts[email])
	}
	byType, err := s.DailyUsageByType(ctx, v, from, to, "UTC", "day", nil)
	if err != nil {
		t.Fatal(err)
	}
	stack := map[string]int64{}
	for _, d := range byType {
		stack[d.SessionType] += d.Sessions
	}
	if stack["empty"] != 3 || stack["user"] != 1 {
		t.Errorf("stack = %v, want empty:3 user:1", stack)
	}
	people, err := s.PersonUsage(ctx, v, from, to, "sessions", "", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(people) != 1 || people[0].Sessions != 1 {
		t.Errorf("default person usage = %+v, want 1 session", people)
	}
	// Search: the empty sessions have no messages, and the one hit there is
	// belongs to the listed session.
	hits, err := s.SearchMessages(ctx, v, SearchFilter{Query: "bug"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits.Hits) != 1 || hits.Hits[0].SessionID != "s-real" {
		t.Errorf("search hits = %+v", hits.Hits)
	}
	// A selection naming only unknown types is a selection of nothing on
	// every read that takes a list, never a fall-through to the default view.
	hits, err = s.SearchMessages(ctx, v, SearchFilter{Query: "bug", Types: []string{"robot"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits.Hits) != 0 {
		t.Errorf("an unknown-only search selection returned %+v, want no hits", hits.Hits)
	}
	days, err = s.DailyUsage(ctx, v, from, to, "UTC", "day", nil, []string{"robot"})
	if err != nil {
		t.Fatal(err)
	}
	n = 0
	for _, d := range days {
		n += d.Sessions
	}
	if n != 0 {
		t.Errorf("an unknown-only daily usage selection counted %d sessions, want 0", n)
	}
	people, err = s.PersonUsage(ctx, v, from, to, "sessions", "", 10, []string{"robot"})
	if err != nil {
		t.Fatal(err)
	}
	if len(people) != 0 {
		t.Errorf("an unknown-only person usage selection = %+v, want nobody", people)
	}
}

func ids(ss []Session) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		out = append(out, s.SessionID)
	}
	return out
}

// (ii) and (iii): empty is not sticky. Content promotes the row whichever
// batch arrives first, and the provisional empty_kind is cleared with it.
func TestIntegrationEmptyIsPromotedByContentInEitherOrder(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	// Lifecycle first, then the prompt.
	ingestBatch(t, s,
		hookAt("a1", "s-late", email, event.SessionStarted, 1, at, "startup"),
		hookAt("a3", "s-late", email, event.SessionEnded, 3, at.Add(time.Second), "other"))
	if got := latticeOf(t, s, "s-late"); got.Type != "empty" || got.EmptyKind != "aborted" {
		t.Fatalf("before the prompt: %+v", got)
	}
	ingestBatch(t, s, hookAt("a2", "s-late", email, event.UserPrompt, 2, at.Add(500*time.Millisecond), "hello"))
	if got := latticeOf(t, s, "s-late"); got.Type != "user" || got.EmptyKind != "" || got.HeadState != "complete" || got.Human != 1 {
		t.Errorf("after the prompt: %+v, want user with no empty_kind", got)
	}

	// Prompt first, then the lifecycle pair.
	ingestBatch(t, s, hookAt("b2", "s-early", email, event.UserPrompt, 2, at.Add(500*time.Millisecond), "hello"))
	ingestBatch(t, s,
		hookAt("b1", "s-early", email, event.SessionStarted, 1, at, "startup"),
		hookAt("b3", "s-early", email, event.SessionEnded, 3, at.Add(time.Second), "other"))
	if got := latticeOf(t, s, "s-early"); got.Type != "user" || got.EmptyKind != "" {
		t.Errorf("a lifecycle batch after content demoted the session: %+v", got)
	}

	// A tool call alone is content too; a blank hook turn is not.
	ingestBatch(t, s, hookAt("c1", "s-tool", email, event.AssistantTurn, 1, at, ""))
	if got := latticeOf(t, s, "s-tool"); got.Type != "empty" {
		t.Errorf("a textless, usage-less turn counted as content: %+v", got)
	}
	ingestBatch(t, s, hookAt("c2", "s-tool", email, event.ToolCall, 2, at.Add(time.Second), ""))
	if got := latticeOf(t, s, "s-tool"); got.Type != "user" || got.Content != 1 {
		t.Errorf("a tool call did not promote the session: %+v", got)
	}
}

// (iv) A row from before the lattice, with content_events still zero, must
// not fall to empty on the first content-free batch after the deploy.
func TestIntegrationPreMigrationUserRowStaysUserOnALateEnd(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	// The pre-migration shape: a user row that was folded at least once (so
	// ended_at is set) with the counters at zero and content_events at its
	// column default.
	if _, err := s.db.Exec(ctx, `
		INSERT INTO sessions (session_id, email, source, started_at, ended_at, session_type, content_events, user_turns)
		VALUES ('s-old', $1, 'claude_code', $2, $2, 'user', 0, 0)`, email, at); err != nil {
		t.Fatal(err)
	}
	ingestBatch(t, s, hookAt("e1", "s-old", email, event.SessionEnded, 9, at.Add(time.Hour), "other"))
	if got := latticeOf(t, s, "s-old"); got.Type != "user" || got.EmptyKind != "" {
		t.Errorf("a late end demoted a pre-migration user row: %+v, want user with no empty_kind", got)
	}
	// The same row can still be promoted upward, and the mark says so.
	promoted, err := markSessionType(ctx, s.db, "s-old", "automation")
	if err != nil {
		t.Fatal(err)
	}
	if got := latticeOf(t, s, "s-old"); got.Type != "automation" || !promoted {
		t.Errorf("markSessionType did not promote (reported %v): %+v", promoted, got)
	}
	// Marking it again changes nothing and says so, which is what keeps a
	// re-walk from re-titling every automation session it re-delivers.
	if promoted, err = markSessionType(ctx, s.db, "s-old", "automation"); err != nil || promoted {
		t.Errorf("a second mark reported promoted=%v err=%v, want false and nil", promoted, err)
	}
}

// (v) Automation is the top of the lattice: evidence in any batch sets it,
// and nothing after it, including an internal template or a lifecycle-only
// batch, takes it back. It never carries an empty_kind: the refinement
// belongs to empty rows alone, and a spawn that died is still findable as
// automation with content_events zero.
func TestIntegrationAutomationAbsorbsEverything(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	start := hookAt("a1", "s-auto", email, event.SessionStarted, 1, at, "startup")
	start.Event.Entrypoint = "sdk-cli"
	end := hookAt("a2", "s-auto", email, event.SessionEnded, 2, at.Add(time.Second), "other")
	ingestBatch(t, s, start, end)
	if got := latticeOf(t, s, "s-auto"); got.Type != "automation" || got.EmptyKind != "" || got.Content != 0 {
		t.Errorf("an automation spawn that died: %+v, want automation with no empty_kind and no content", got)
	}
	ingestBatch(t, s, hookAt("a3", "s-auto", email, event.UserPrompt, 3, at.Add(2*time.Second), "Reply with exactly: pong"))
	got := latticeOf(t, s, "s-auto")
	if got.Type != "automation" || got.EmptyKind != "" {
		t.Errorf("a template prompt changed an automation row: %+v", got)
	}
	if got.FirstPrompt == nil || *got.FirstPrompt != "Reply with exactly: pong" || got.TitleSource != "human" {
		t.Errorf("an automation session is named by its prompt: %+v", got)
	}
	ingestBatch(t, s, hookAt("a4", "s-auto", email, event.SessionEnded, 4, at.Add(3*time.Second), "other"))
	if got := latticeOf(t, s, "s-auto"); got.Type != "automation" {
		t.Errorf("a lifecycle batch after content demoted automation: %+v", got)
	}
}

// (v, the other door) Evidence that rides only on re-delivered events
// promotes through markSessionType rather than applyDelta: a re-walk
// re-delivers a spawn's session_started with the entrypoint the live agent
// did not know to stamp, and every row of it is a duplicate. The row that
// promotion leaves must be indistinguishable from one applyDelta promoted,
// automation with no empty_kind, whichever branch carried the evidence: the
// batch that is all duplicates, or the batch whose only fresh rows belong to
// some other session.
func TestIntegrationEvidenceOnlyPromotionClearsEmptyKind(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	for _, sid := range []string{"s-rewalk", "s-rewalk-2"} {
		ingestBatch(t, s,
			hookAt(sid+":1", sid, email, event.SessionStarted, 1, at, "startup"),
			hookAt(sid+":2", sid, email, event.SessionEnded, 2, at.Add(time.Second), "other"))
		if got := latticeOf(t, s, sid); got.Type != "empty" || got.EmptyKind != "aborted" {
			t.Fatalf("%s before the re-walk = %+v, want empty/aborted", sid, got)
		}
	}

	// All duplicates: the start marker again, now carrying the entrypoint.
	start := hookAt("s-rewalk:1", "s-rewalk", email, event.SessionStarted, 1, at, "startup")
	start.Event.Entrypoint = "sdk-cli"
	res, err := s.UpsertEvents(ctx, []Ingest{start})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 0 || len(res.Duplicate) != 1 {
		t.Fatalf("re-delivery inserted %v, duplicate %v; the fixture is broken", res.Inserted, res.Duplicate)
	}
	if got := latticeOf(t, s, "s-rewalk"); got.Type != "automation" || got.EmptyKind != "" || got.Content != 0 {
		t.Errorf("re-delivered evidence left %+v, want automation with no empty_kind and no content", got)
	}

	// The duplicate rides in a batch whose fresh rows are another session's.
	start2 := hookAt("s-rewalk-2:1", "s-rewalk-2", email, event.SessionStarted, 1, at, "startup")
	start2.Event.Entrypoint = "sdk-cli"
	other := hookAt("s-other:1", "s-other", email, event.UserPrompt, 1, at.Add(time.Minute), "fix the bug")
	res, err = s.UpsertEvents(ctx, []Ingest{start2, other})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 1 || len(res.Duplicate) != 1 {
		t.Fatalf("mixed batch inserted %v, duplicate %v; the fixture is broken", res.Inserted, res.Duplicate)
	}
	if got := latticeOf(t, s, "s-rewalk-2"); got.Type != "automation" || got.EmptyKind != "" || got.Content != 0 {
		t.Errorf("evidence beside another session's fresh rows left %+v, want automation with no empty_kind", got)
	}
	if got := latticeOf(t, s, "s-other"); got.Type != "user" || got.Content != 1 {
		t.Errorf("the fresh session in the same batch = %+v, want user with one content event", got)
	}
}

// (vi) internal is decided by the opening prompt and recomputed on every
// batch that carries one: a template that arrives first does not stick once
// the real opening prompt lands at a lower seq, and a template at the opening
// makes the session internal however it was ordered on arrival.
func TestIntegrationInternalIsRecomputedFromTheOpeningPrompt(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	v := Viewer{Email: email, Role: RoleMember}
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	ingestBatch(t, s, hookAt("t5", "s-mixed", email, event.UserPrompt, 5, at.Add(5*time.Second),
		"You are generating a short conversation title for this session."))
	if got := latticeOf(t, s, "s-mixed"); got.Type != "internal" {
		t.Fatalf("a template at the opening did not make the session internal: %+v", got)
	}
	ingestBatch(t, s, hookAt("t1", "s-mixed", email, event.UserPrompt, 1, at.Add(time.Second), "fix the flaky test"))
	got := latticeOf(t, s, "s-mixed")
	if got.Type != "user" || got.FirstPrompt == nil || *got.FirstPrompt != "fix the flaky test" {
		t.Errorf("the real opening prompt did not take the verdict back: %+v", got)
	}

	// A helper run whose only prompt is a harness-injected instruction has no
	// human row; the template is its honest name, and it is internal.
	ingestBatch(t, s, hookAt("c1", "s-conductor", email, event.UserPrompt, 1, at, "<system_instruction>You are Conductor.</system_instruction>"))
	got = latticeOf(t, s, "s-conductor")
	if got.Type != "internal" || got.TitleSource != "automation_template" || got.FirstPrompt == nil ||
		!strings.HasPrefix(*got.FirstPrompt, "<system_instruction>") {
		t.Errorf("conductor session = %+v, want internal named by its template", got)
	}
	if got.Human != 0 {
		t.Errorf("a harness-injected prompt counted as a human turn: %+v", got)
	}

	// The search default is the list default. "Conductor" occurs in no
	// listed session's messages, so a hit under the default filter would be
	// the search resurfacing what the list hides; the explicit filter finds
	// it, because hiding is not deleting.
	hits, err := s.SearchMessages(ctx, v, SearchFilter{Query: "Conductor"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits.Hits) != 0 {
		t.Errorf("default search surfaced an internal session's message: %+v", hits.Hits)
	}
	if hits, err = s.SearchMessages(ctx, v, SearchFilter{Query: "Conductor", Types: []string{"internal"}}); err != nil {
		t.Fatal(err)
	}
	if len(hits.Hits) != 1 || hits.Hits[0].SessionID != "s-conductor" {
		t.Errorf("explicit internal search = %+v, want the conductor session's one hit", hits.Hits)
	}

	page, err := s.ListSessions(ctx, v, SessionFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 1 || page.Sessions[0].SessionID != "s-mixed" {
		t.Errorf("default listing = %v, want s-mixed alone (internal hidden)", ids(page.Sessions))
	}
	if page, err = s.ListSessions(ctx, v, SessionFilter{Types: []string{"internal"}}); err != nil || len(page.Sessions) != 1 {
		t.Errorf("explicit internal filter listed %v (%v)", ids(page.Sessions), err)
	}
}

// (vii) The title is the first thing a person said. Every wrapper shape that
// used to win the title loses to the prompt behind it, a slash command is
// named as a command, a subagent's task never names its parent, and a
// wrapper-only session has no title at all rather than a wrapper or a cwd.
func TestIntegrationTitlesNeverComeFromWrappers(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	wrappers := map[string]string{
		"caveat":       "<local-command-caveat>Caveat: The messages below were generated by the user while running local commands.</local-command-caveat>",
		"stdout":       "<local-command-stdout>Set effort level to high</local-command-stdout>",
		"task":         "<task-notification>\n<task-id>a1</task-id>\n<status>completed</status>\n<summary>done</summary>\n</task-notification>",
		"reminder":     "<system-reminder>\nThe user named this session \"Closure\".\n</system-reminder>",
		"teammate":     "Another Claude session sent a message:\n<teammate-message teammate_id=\"peer\" color=\"blue\">\nhi\n</teammate-message>",
		"notification": "[SYSTEM NOTIFICATION] Background task completed",
		"interrupted":  "[Request interrupted by user]",
		"ide":          "<ide_opened_file>The user opened x.go</ide_opened_file>",
		"compaction":   "This session is being continued from a previous conversation that ran out of context.",
		"environment":  "<environment_context>\n<cwd>/x</cwd>\n<shell>zsh</shell>\n</environment_context>",
		"plugins":      "<recommended_plugins>\n- a\n</recommended_plugins>",
		"agents":       "# AGENTS.md instructions for the repo",
	}
	for name, text := range wrappers {
		sid := "s-" + name
		first := hookAt(sid+"-1", sid, email, event.UserPrompt, 1, at, text)
		first.Event.Cwd = "/home/riley"
		ingestBatch(t, s, first, hookAt(sid+"-2", sid, email, event.UserPrompt, 2, at.Add(time.Second), "fix the bug"))
		got := latticeOf(t, s, sid)
		if got.FirstPrompt == nil || *got.FirstPrompt != "fix the bug" || got.TitleSource != "human" {
			t.Errorf("%s: title = %v (%s), want \"fix the bug\" from a human", name, deref(got.FirstPrompt), got.TitleSource)
		}
		if got.Type != "user" || got.Human != 1 {
			t.Errorf("%s: type %s human_turns %d, want user with 1 human turn", name, got.Type, got.Human)
		}
	}

	// A wrapper alone: no title, never the working directory.
	only := hookAt("w-1", "s-only", email, event.UserPrompt, 1, at, wrappers["caveat"])
	only.Event.Cwd = "/home/riley"
	ingestBatch(t, s, only)
	if got := latticeOf(t, s, "s-only"); got.FirstPrompt != nil || got.TitleSource != "none" || got.Type != "user" {
		t.Errorf("wrapper-only session = %+v, want no title and source none", got)
	}

	// A slash command, in its on-disk envelope, is named as a command.
	cmd := hookAt("c-1", "s-cmd", email, event.UserPrompt, 1, at,
		"<command-message>platform-engineer:platform-engineer</command-message>\n<command-name>/platform-engineer:platform-engineer</command-name>\n<command-args>Git fetch latest main.</command-args>")
	cmd.Event.Origin = event.OriginTranscript
	ingestBatch(t, s, cmd)
	if got := latticeOf(t, s, "s-cmd"); got.FirstPrompt == nil || *got.FirstPrompt != "/platform-engineer:platform-engineer Git fetch latest main." || got.TitleSource != "command" {
		t.Errorf("command session = %+v", got)
	}

	// A subagent's task prompt sits at seq 1 of its own stream and must not
	// name the parent; the person's later prompt on the main thread does.
	task := hookAt("sub-1", "s-sub", email, event.UserPrompt, 1, at, "Review server/store and report.")
	task.Event.AgentID = "agent-1"
	task.Event.Origin = event.OriginTranscript
	ingestBatch(t, s, task, hookAt("sub-2", "s-sub", email, event.UserPrompt, 3, at.Add(time.Second), "please review the store"))
	if got := latticeOf(t, s, "s-sub"); got.FirstPrompt == nil || *got.FirstPrompt != "please review the store" || got.Human != 1 {
		t.Errorf("subagent task named the parent: %+v", got)
	}

	// The opening prompt arriving after a later one replaces it, and a later
	// prompt arriving after the opening one does not.
	ingestBatch(t, s, hookAt("o-3", "s-order", email, event.UserPrompt, 3, at.Add(3*time.Second), "third"))
	ingestBatch(t, s, hookAt("o-1", "s-order", email, event.UserPrompt, 1, at.Add(time.Second), "first"))
	ingestBatch(t, s, hookAt("o-2", "s-order", email, event.UserPrompt, 2, at.Add(2*time.Second), "second"))
	if got := latticeOf(t, s, "s-order"); got.FirstPrompt == nil || *got.FirstPrompt != "first" {
		t.Errorf("out-of-order arrival titled the session %v", deref(got.FirstPrompt))
	}
}

// (viii) parent_session_id is written only with a lineage source. An event in
// the old shape, a parent with no source, files its value as a record uuid;
// the fiction-moving UPDATE in 0015 is idempotent and never touches a proven
// parent or one that resolves.
func TestIntegrationLineageIsWrittenOnlyWithASource(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	old := hookAt("l-1", "s-child", email, event.UserPrompt, 1, at, "continue")
	old.Event.ParentSessionID = "9f90859f-0000-0000-0000-000000000000"
	ingestBatch(t, s, old)
	got := latticeOf(t, s, "s-child")
	if got.Parent != nil || got.Lineage != "" {
		t.Errorf("an old-shape parent reached parent_session_id: %+v", got)
	}
	if deref(got.ParentRecord) != old.Event.ParentSessionID {
		t.Errorf("the marker was not filed as a record uuid: %+v", got)
	}

	ingestBatch(t, s, hookAt("p-1", "s-origin", email, event.UserPrompt, 1, at, "origin"))
	fork := hookAt("f-1", "s-fork", email, event.SessionStarted, 1, at.Add(time.Minute), "fork")
	fork.Event.ParentSessionID = "s-origin"
	fork.Event.LineageSource = "fork_uuid"
	ingestBatch(t, s, fork)
	if got := latticeOf(t, s, "s-fork"); deref(got.Parent) != "s-origin" || got.Lineage != "fork_uuid" {
		t.Errorf("a proven parent was not written: %+v", got)
	}

	// A source the server does not know is no source: the parent is filed
	// as a record uuid, and parent_session_id stays NULL.
	guessed := hookAt("g-1", "s-guessed", email, event.SessionStarted, 1, at.Add(time.Minute), "fork")
	guessed.Event.ParentSessionID = "s-origin"
	guessed.Event.LineageSource = "guess"
	ingestBatch(t, s, guessed)
	if got := latticeOf(t, s, "s-guessed"); got.Parent != nil || got.Lineage != "" || deref(got.ParentRecord) != "s-origin" {
		t.Errorf("an unknown lineage source unlocked the parent: %+v", got)
	}

	// The fiction move, re-run: rows with a source or a resolvable parent are
	// untouched; the unresolvable, sourceless one moves once and stays moved.
	if _, err := s.db.Exec(ctx, `
		INSERT INTO sessions (session_id, email, source, started_at, parent_session_id, lineage_source) VALUES
		  ('s-fiction', $1, 'claude_code', $2, 'ghost-uuid', ''),
		  ('s-resolves', $1, 'claude_code', $2, 's-origin', ''),
		  ('s-proven', $1, 'claude_code', $2, 'ghost-2', 'fork_hook')`, email, at); err != nil {
		t.Fatal(err)
	}
	move := migrationStatement(t, "0015_session_lattice.sql", "parent_record_uuid = parent_session_id")
	for i := range 2 {
		if _, err := s.db.Exec(ctx, move); err != nil {
			t.Fatalf("fiction move run %d: %v", i+1, err)
		}
		if got := latticeOf(t, s, "s-fiction"); got.Parent != nil || deref(got.ParentRecord) != "ghost-uuid" {
			t.Errorf("run %d: fiction row = %+v", i+1, got)
		}
		if got := latticeOf(t, s, "s-resolves"); deref(got.Parent) != "s-origin" {
			t.Errorf("run %d: a resolvable parent was moved: %+v", i+1, got)
		}
		if got := latticeOf(t, s, "s-proven"); deref(got.Parent) != "ghost-2" || got.Lineage != "fork_hook" {
			t.Errorf("run %d: a proven parent was moved: %+v", i+1, got)
		}
	}
}

// migrationStatement extracts the one statement of an embedded migration
// that contains marker, so a test re-runs the migration's own SQL rather than
// a copy of it.
func migrationStatement(t *testing.T, name, marker string) string {
	t.Helper()
	body, err := migrations.ReadFile("migrations/" + name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	// Comment lines are dropped first: the headers are prose, and prose has
	// semicolons in it.
	var code []string
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			code = append(code, line)
		}
	}
	for _, stmt := range strings.Split(strings.Join(code, "\n"), ";") {
		if strings.Contains(stmt, marker) {
			return stmt
		}
	}
	t.Fatalf("%s has no statement containing %q", name, marker)
	return ""
}

// (ix) A session whose device reported capture loss is listed whatever its
// type: hiding it would present a data loss as a session in which nothing
// happened.
func TestIntegrationCaptureLossRowsAreAlwaysListed(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	v := Viewer{Email: email, Role: RoleMember}
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	ingestBatch(t, s,
		hookAt("a1", "s-lost", email, event.SessionStarted, 1, at, "startup"),
		hookAt("a2", "s-lost", email, event.SessionEnded, 2, at.Add(10*time.Second), "other"))
	if page, err := s.ListSessions(ctx, v, SessionFilter{}); err != nil || len(page.Sessions) != 0 {
		t.Fatalf("an empty session was listed by default: %v %v", ids(page.Sessions), err)
	}
	if _, err := s.db.Exec(ctx, `UPDATE sessions SET head_state = 'capture_loss' WHERE session_id = 's-lost'`); err != nil {
		t.Fatal(err)
	}
	page, err := s.ListSessions(ctx, v, SessionFilter{})
	if err != nil || len(page.Sessions) != 1 || page.Sessions[0].Type != "empty" {
		t.Errorf("a capture_loss row was hidden: %v %v", ids(page.Sessions), err)
	}
	counts, err := s.SessionCounts(ctx)
	if err != nil || counts[email] != 1 {
		t.Errorf("a capture_loss row was not counted: %v %v", counts, err)
	}
	people, err := s.PersonUsage(ctx, v, at.Add(-time.Hour), at.Add(time.Hour), "sessions", "", 10, nil)
	if err != nil || len(people) != 1 || people[0].Sessions != 1 {
		t.Errorf("a capture_loss row was left out of person usage: %+v %v", people, err)
	}
}

// The retroactive classification in 0016 is safe to run again: it reclassifies
// exactly the rows with no content event and leaves everything else alone,
// on the second run as on the first.
func TestIntegrationEmptyBackfillIsSafeToRerun(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	ingestBatch(t, s,
		hookAt("a1", "s-old-empty", email, event.SessionStarted, 1, at, "startup"),
		hookAt("a2", "s-old-empty", email, event.SessionEnded, 2, at.Add(time.Second), "other"))
	ingestBatch(t, s, hookAt("b1", "s-old-open", email, event.SessionStarted, 1, at, "startup"))
	ingestBatch(t, s, hookAt("c1", "s-old-turn", email, event.AssistantTurn, 1, at, "just a turn"))
	ingestBatch(t, s, hookAt("d1", "s-old-real", email, event.UserPrompt, 1, at, "hello"))
	// Put the rows back into their pre-migration shape.
	if _, err := s.db.Exec(ctx, `UPDATE sessions SET session_type = 'user', empty_kind = NULL, content_events = 0, user_turns = 0`); err != nil {
		t.Fatal(err)
	}
	backfill := migrationStatement(t, "0016_empty_backfill.sql", "session_type = 'empty'")
	for i := range 2 {
		if _, err := s.db.Exec(ctx, backfill); err != nil {
			t.Fatalf("backfill run %d: %v", i+1, err)
		}
		for sid, want := range map[string][2]string{
			"s-old-empty": {"empty", "aborted"},
			"s-old-open":  {"empty", "tail_truncated"},
			"s-old-turn":  {"user", ""},
			"s-old-real":  {"user", ""},
		} {
			if got := latticeOf(t, s, sid); got.Type != want[0] || got.EmptyKind != want[1] {
				t.Errorf("run %d: %s = %s/%q, want %s/%q", i+1, sid, got.Type, got.EmptyKind, want[0], want[1])
			}
		}
	}
}

// The corpus carries the kind and the thread of every message from ingest on,
// and the client build that delivered the batch reaches the session.
func TestIntegrationMessagesCarryKindAndAgentAndBuilds(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	note := hookAt("m-1", "s-msg", email, event.UserPrompt, 1, at, "<task-notification>\n<task-id>x</task-id>\n<status>completed</status>\n<summary>done</summary>\n</task-notification>")
	note.AgentVersion = "23713ea"
	task := hookAt("m-2", "s-msg", email, event.UserPrompt, 1, at.Add(time.Second), "Investigate the flake.")
	task.Event.AgentID = "agent-1"
	task.Event.Origin = event.OriginTranscript
	task.AgentVersion = "23713ea"
	turn := hookAt("m-3", "s-msg", email, event.AssistantTurn, 2, at.Add(2*time.Second), "Found it.")
	turn.AgentVersion = "580e794"
	result := hookAt("m-4", "s-msg", email, event.ToolResult, 3, at.Add(3*time.Second), "ok")
	ingestBatch(t, s, note, task, turn, result)

	rows, err := s.db.Query(ctx, `SELECT event_id, kind, coalesce(agent_id, '') FROM messages WHERE session_id = 's-msg' ORDER BY event_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string][2]string{}
	for rows.Next() {
		var id, kind, agent string
		if err := rows.Scan(&id, &kind, &agent); err != nil {
			t.Fatal(err)
		}
		got[id] = [2]string{kind, agent}
	}
	want := map[string][2]string{
		"m-1": {"task_notification", ""},
		"m-2": {"subagent_task", "agent-1"},
		"m-3": {"assistant_text", ""},
		"m-4": {"tool", ""},
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("message %s = %v, want %v", id, got[id], w)
		}
	}
	lat := latticeOf(t, s, "s-msg")
	if len(lat.AgentVersions) != 2 || lat.AgentVersions[0] != "23713ea" || lat.AgentVersions[1] != "580e794" {
		t.Errorf("agent_versions = %v, want both builds", lat.AgentVersions)
	}
	if lat.Human != 0 || lat.Content != 4 {
		t.Errorf("counts = human %d content %d, want 0 and 4", lat.Human, lat.Content)
	}
	// Notifications never become the title: no person spoke here.
	if lat.FirstPrompt != nil || lat.TitleSource != "none" {
		t.Errorf("a notification named the session: %+v", lat)
	}
}

// applyDelta's INSERT branch is unreachable from both of its callers, which
// claim the row before they fold, so nothing but this test keeps its CASEs
// in step with the UPDATE branch's. One lifecycle-only increment, folded
// straight into a fresh row and folded onto a claimed one, must land on the
// same lattice columns: a plain spawn that died is empty and aborted, and one
// carrying automation evidence is automation with no empty_kind.
func TestIntegrationApplyDeltaInsertBranchMirrorsTheUpdateBranch(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name, fresh, claimed, entrypoint string
		want                             latticeRow
	}{
		{"spawn that died", "s-fresh", "s-claimed", "",
			latticeRow{Type: "empty", EmptyKind: "aborted", HeadState: "unknown"}},
		{"automation that died", "s-fresh-auto", "s-claimed-auto", "sdk-cli",
			latticeRow{Type: "automation", EmptyKind: "", HeadState: "unknown"}},
	} {
		for _, sid := range []string{tc.fresh, tc.claimed} {
			start := hookAt(sid+":1", sid, email, event.SessionStarted, 1, at, "startup")
			start.Event.Entrypoint = tc.entrypoint
			end := hookAt(sid+":2", sid, email, event.SessionEnded, 2, at.Add(600*time.Millisecond), "other")
			d := foldDeltas([]Ingest{start, end}, nil)[sid]
			d.Automation = tc.entrypoint != ""
			if sid == tc.claimed {
				if _, err := claimSession(ctx, s.db, start); err != nil {
					t.Fatal(err)
				}
			}
			if err := applyDelta(ctx, s.db, d); err != nil {
				t.Fatalf("%s via %s: %v", tc.name, sid, err)
			}
			got := latticeOf(t, s, sid)
			if got.Type != tc.want.Type || got.EmptyKind != tc.want.EmptyKind || got.HeadState != tc.want.HeadState {
				t.Errorf("%s via %s = %+v, want type %s kind %q head %s",
					tc.name, sid, got, tc.want.Type, tc.want.EmptyKind, tc.want.HeadState)
			}
		}
	}
}

// The template list is applied on both sides with the same trimmer. A
// template behind a newline is a template to the normalizer and to the
// store, and one behind a character outside the exported cutset is a person
// to both; the runner will call IsInternalTemplate on the same text the
// store classified, and a row that flipped between the two would be the
// drift the one list exists to prevent.
func TestIntegrationTemplateTrimmingAgreesWithTheNormalizer(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	const tpl = "You are generating a short conversation title for a Claude Code session."

	for sid, text := range map[string]string{
		"s-newline": "\n" + tpl,
		"s-tab":     "\t \r\n" + tpl,
		"s-nbsp":    "\u00a0" + tpl,
	} {
		ingestBatch(t, s, hookAt(sid+"-1", sid, email, event.UserPrompt, 1, at, text))
		_, goSays := normalize.IsInternalTemplate(text)
		got := latticeOf(t, s, sid)
		if goSays != (got.Type == "internal") {
			t.Errorf("%s: normalizer says template=%v, store classified %s", sid, goSays, got.Type)
		}
	}
	if got := latticeOf(t, s, "s-newline"); got.Type != "internal" {
		t.Errorf("a template behind a newline was not internal: %+v", got)
	}
	if got := latticeOf(t, s, "s-nbsp"); got.Type != "user" {
		t.Errorf("a template behind a character outside the cutset was not a person: %+v", got)
	}
}

// A row stored before messages carried a kind wins the title with an empty
// kind, which keeps the historical title in place until the runner
// recomputes it.
// Its provenance has to stay in place too: the row does not know a person
// typed it, and a later prompt must not make it claim so.
func TestIntegrationUnclassifiedWinnerKeepsItsProvenance(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	const caveat = "<local-command-caveat>Caveat: The messages below were generated by the user while running local commands.</local-command-caveat>"

	ingestBatch(t, s, hookAt("h-1", "s-hist", email, event.UserPrompt, 1, at, caveat))
	// The row as the old code left it: an unclassified message that titled
	// the session, with no provenance recorded.
	if _, err := s.db.Exec(ctx, `UPDATE messages SET kind = '' WHERE session_id = 's-hist'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(ctx, `UPDATE sessions SET first_prompt = left($1, 500), title_source = 'none' WHERE session_id = 's-hist'`, caveat); err != nil {
		t.Fatal(err)
	}
	ingestBatch(t, s, hookAt("h-2", "s-hist", email, event.UserPrompt, 2, at.Add(time.Second), "fix the bug"))
	got := latticeOf(t, s, "s-hist")
	if got.FirstPrompt == nil || !strings.HasPrefix(*got.FirstPrompt, "<local-command-caveat>") {
		t.Errorf("the historical title was rewritten before the runner: %+v", got)
	}
	if got.TitleSource != "none" {
		t.Errorf("an unclassified winner claimed provenance %q, want none", got.TitleSource)
	}
	if got.Type != "user" || got.Human != 1 {
		t.Errorf("type %s human_turns %d, want user with the one typed prompt counted", got.Type, got.Human)
	}
}

// Launcher evidence that trails the prompts names the session it promotes.
// A session whose only prompt was a wrapper has no title as a user session;
// when the start marker arrives afterwards and says sdk-cli, the row is
// automation, and automation is named by its first prompt of any kind. Both
// doors the evidence can come through must name it: the fresh start marker
// (applyDelta), and the re-delivered one (markSessionType). The first line
// of the stored text is the title, which is what the normalizer renders for
// the caveat.
func TestIntegrationEvidenceTrailingThePromptsStillNamesTheSession(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	const caveat = "<local-command-caveat>Caveat: The messages below were generated by the user while running local commands.</local-command-caveat>"

	// The fresh door: the prompt first, the start marker in a later batch.
	ingestBatch(t, s, hookAt("l-1", "s-late", email, event.UserPrompt, 1, at.Add(time.Second), caveat))
	if got := latticeOf(t, s, "s-late"); got.Type != "user" || got.FirstPrompt != nil || got.TitleSource != "none" {
		t.Fatalf("before the evidence = %+v, want an untitled user session", got)
	}
	start := hookAt("l-0", "s-late", email, event.SessionStarted, 0, at, "startup")
	start.Event.Entrypoint = "sdk-cli"
	ingestBatch(t, s, start)
	got := latticeOf(t, s, "s-late")
	if got.Type != "automation" || got.FirstPrompt == nil || *got.FirstPrompt != caveat || got.TitleSource != "automation_template" {
		t.Errorf("evidence through applyDelta left %+v, want automation named by its first prompt", got)
	}

	// The duplicate door: the start marker was stored without an entrypoint
	// and is re-delivered with one, in a batch that inserts nothing.
	ingestBatch(t, s,
		hookAt("d-0", "s-late-2", email, event.SessionStarted, 0, at, "startup"),
		hookAt("d-1", "s-late-2", email, event.UserPrompt, 1, at.Add(time.Second), caveat))
	again := hookAt("d-0", "s-late-2", email, event.SessionStarted, 0, at, "startup")
	again.Event.Entrypoint = "sdk-cli"
	res, err := s.UpsertEvents(ctx, []Ingest{again})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 0 || len(res.Duplicate) != 1 {
		t.Fatalf("re-delivery inserted %v, duplicate %v; the fixture is broken", res.Inserted, res.Duplicate)
	}
	got = latticeOf(t, s, "s-late-2")
	if got.Type != "automation" || got.FirstPrompt == nil || *got.FirstPrompt != caveat || got.TitleSource != "automation_template" {
		t.Errorf("evidence through markSessionType left %+v, want automation named by its first prompt", got)
	}

	// A title a person gave is never replaced by the fallback: the human
	// row wins the title, and evidence afterwards only changes the type.
	ingestBatch(t, s, hookAt("k-1", "s-kept", email, event.UserPrompt, 1, at.Add(time.Second), "Reply with exactly: pong"))
	kept := hookAt("k-0", "s-kept", email, event.SessionStarted, 0, at, "startup")
	kept.Event.Entrypoint = "sdk-cli"
	ingestBatch(t, s, kept)
	if got := latticeOf(t, s, "s-kept"); got.Type != "automation" || deref(got.FirstPrompt) != "Reply with exactly: pong" || got.TitleSource != "human" {
		t.Errorf("evidence rewrote a person's title: %+v", got)
	}
}

// Two batches that touch the same two sessions in opposite orders must not
// deadlock on the claim. Each claim is a row lock held to commit; taken in
// arrival order, the two batches would each wait on the other's first lock
// until Postgres broke the cycle by failing one of them, and the client
// would see a 503 and retry the whole batch. Claims are taken in session-id
// order instead, so the second batch queues behind the first.
func TestIntegrationOppositeOrderBatchesDoNotDeadlock(t *testing.T) {
	s := newStore(t, nil)
	const email = "dev@example.com"
	mustPrincipal(t, s, email, RoleMember)
	ctx := context.Background()
	at := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	const rounds = 40
	errs := make(chan error, 2*rounds)
	for i := range rounds {
		var wg sync.WaitGroup
		wg.Add(2)
		for _, order := range [][2]string{{"s-dl-a", "s-dl-b"}, {"s-dl-b", "s-dl-a"}} {
			go func(first, second string, round int) {
				defer wg.Done()
				// Ids carry the batch's order so the two batches of a round
				// insert rather than one duplicating the other.
				batch := []Ingest{
					hookAt(fmt.Sprintf("%s:%d:%s", first, round, first), first, email, event.ToolCall, int64(round), at, ""),
					hookAt(fmt.Sprintf("%s:%d:%s", second, round, first), second, email, event.ToolCall, int64(round), at, ""),
				}
				if _, err := s.UpsertEvents(ctx, batch); err != nil {
					errs <- err
				}
			}(order[0], order[1], i)
		}
		wg.Wait()
	}
	close(errs)
	for err := range errs {
		t.Errorf("opposite-order batches failed: %v", err)
	}
	if got := latticeOf(t, s, "s-dl-a"); got.Content != 2*rounds {
		t.Errorf("s-dl-a content_events = %d, want every batch's event counted once", got.Content)
	}
}
