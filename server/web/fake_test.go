package web

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/server/fleet"
)

// fakeData is an in-memory Data. The handlers are tested against it rather than
// against Postgres so the default test run needs no container; the SQL that
// implements this interface is tested where it lives.
type fakeData struct {
	sessions map[string]SessionDetail
	events   map[string][]event.Event
	hits     []SearchHit
	capped   bool
	people   []Principal
	fleet    Fleet
	access   []AccessEntry
	shares   map[string]string

	// Artifacts and links, keyed by session. Absent for most fixtures, which is
	// the realistic case: only live hook capture produces file diffs.
	artifacts map[string][]Artifact
	links     map[string][]Link
	versions  map[int64][]ArtifactVersion
	contents  map[string]string

	// Analytics fixtures. usage rows with an Email are per-person series rows;
	// blank-email rows are whole-scope days.
	usage     []DayUsage
	perPerson []PersonUsage
	// byType rows back the stacked charts; rows carry SessionType.
	byType []DayUsage
	// The type selections each reader was handed, recorded so tests can assert
	// the handler passed the right filter to the right query.
	tokenTypes        []string
	personTypes       []string
	personSeriesTypes []string
	peopleOptions     []string
	repoOptions       []string
	// artifactErr, when set, is returned by Artifacts so the handler's promise
	// that a failing panel does not cost somebody the transcript can be tested.
	artifactErr error

	// readable, when set, gates every session read the way storage would.
	// Returning ErrNotFound from here is how the "denied looks like absent"
	// behaviour gets exercised.
	readable func(v Viewer, id string) error

	saved   []PrincipalUpdate
	saveErr error
	// saveNoChange makes SetPrincipal report that nothing moved, which the
	// handler renders as a third message distinct from both success and error.
	saveNoChange bool
	listErr      error
	lastQuery    SessionQuery
	lastEvent    EventQuery

	// turns and facets are the derived rows the reader and the list read;
	// the counters record how many reads the handlers issued, so a test can
	// hold the list to one facets query per page.
	turns      map[string][]Turn
	facets     map[string]SessionFacet
	lastTurns  TurnQuery
	turnReads  int
	facetReads int

	// eval is what the fleet evaluator says; muted and unmuted record the
	// mute controls the admin page posted.
	eval    *fleet.Evaluation
	evalErr error
	muted   []FleetMute
	unmuted []string
	// skills is the /skills page's and the session strip's fixture set.
	skills *fakeSkills
	// searches records every message search the handlers issued. It is a slice
	// rather than a "last" field because the interesting assertions are about
	// how many ran: the session list must not pay for one when nobody typed a
	// query, and must not issue one it knows storage will refuse.
	searches []SearchQuery
}

func newFake() *fakeData {
	return &fakeData{
		sessions: map[string]SessionDetail{},
		events:   map[string][]event.Event{},
		shares:   map[string]string{},
		turns:    map[string][]Turn{},
		facets:   map[string]SessionFacet{},
	}
}

func (f *fakeData) gate(v Viewer, id string) error {
	if f.readable != nil {
		return f.readable(v, id)
	}
	d, ok := f.sessions[id]
	if !ok {
		return ErrNotFound
	}
	if d.Email != v.Email && !v.Admin {
		return ErrNotFound
	}
	return nil
}

func (f *fakeData) ListSessions(_ context.Context, v Viewer, q SessionQuery) (SessionPage, error) {
	f.lastQuery = q
	if f.listErr != nil {
		return SessionPage{}, f.listErr
	}
	var out []Session
	for _, d := range f.sessions {
		if d.Email != v.Email && !v.Admin {
			continue
		}
		if q.Repo != "" && d.Repo != q.Repo {
			continue
		}
		out = append(out, d.Session)
	}
	// Newest first with the id as the tiebreak, which is the store's own
	// order, so a page of fixtures renders in one order rather than the map's.
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].StartedAt.After(out[j].StartedAt)
		}
		return out[i].ID > out[j].ID
	})
	return SessionPage{Sessions: out, NextCursor: "next"}, nil
}

func (f *fakeData) Session(_ context.Context, v Viewer, id string) (SessionDetail, error) {
	if err := f.gate(v, id); err != nil {
		return SessionDetail{}, err
	}
	return f.sessions[id], nil
}

func (f *fakeData) Events(_ context.Context, v Viewer, id string, q EventQuery) (EventPage, error) {
	if err := f.gate(v, id); err != nil {
		return EventPage{}, err
	}
	f.lastEvent = q
	all := f.events[id]
	// Time order with agent-then-seq tiebreak, mirroring the store's timeline
	// read, so pagination fixtures exercise the same order the page renders.
	sorted := append([]event.Event(nil), all...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if !sorted[i].OccurredAt.Equal(sorted[j].OccurredAt) {
			return sorted[i].OccurredAt.Before(sorted[j].OccurredAt)
		}
		if sorted[i].AgentID != sorted[j].AgentID {
			return sorted[i].AgentID < sorted[j].AgentID
		}
		return sorted[i].Seq < sorted[j].Seq
	})
	after := DecodeEventCursor(q.After)
	var out []event.Event
	for _, e := range sorted {
		if !after.At.IsZero() {
			if e.OccurredAt.Before(after.At) {
				continue
			}
			if e.OccurredAt.Equal(after.At) &&
				(e.AgentID < after.Agent || (e.AgentID == after.Agent && e.Seq <= after.Seq)) {
				continue
			}
		}
		if q.AgentID != "" && e.AgentID != q.AgentID {
			continue
		}
		out = append(out, e)
	}
	more := false
	if q.Limit > 0 && len(out) > q.Limit {
		out, more = out[:q.Limit], true
	}
	page := EventPage{Events: out, HasMore: more, Total: int64(len(all))}
	if more && len(out) > 0 {
		last := out[len(out)-1]
		page.NextCursor = EventCursor{At: last.OccurredAt, Agent: last.AgentID, Seq: last.Seq}.Encode()
	}
	return page, nil
}

func (f *fakeData) Event(_ context.Context, v Viewer, id, eventID string) (event.Event, error) {
	if err := f.gate(v, id); err != nil {
		return event.Event{}, err
	}
	for _, e := range f.events[id] {
		if e.ID == eventID {
			return e, nil
		}
	}
	return event.Event{}, ErrNotFound
}

func (f *fakeData) Search(_ context.Context, v Viewer, q SearchQuery) (SearchResults, error) {
	f.searches = append(f.searches, q)
	var out []SearchHit
	for _, h := range f.hits {
		if h.Session.Email != v.Email && !v.Admin {
			continue
		}
		if q.Q != "" && !strings.Contains(strings.ToLower(h.Text), strings.ToLower(q.Q)) {
			continue
		}
		out = append(out, h)
	}
	return SearchResults{Hits: out, Capped: f.capped}, nil
}

func (f *fakeData) ResolveShare(_ context.Context, _ Viewer, token string) (SessionDetail, error) {
	id, ok := f.shares[token]
	if !ok {
		return SessionDetail{}, ErrNotFound
	}
	return f.sessions[id], nil
}

func (f *fakeData) Principals(_ context.Context, _ Viewer) ([]Principal, error) {
	return f.people, nil
}

func (f *fakeData) SetPrincipal(_ context.Context, _ Viewer, u PrincipalUpdate) (bool, error) {
	if f.saveErr != nil {
		return false, f.saveErr
	}
	f.saved = append(f.saved, u)
	// The fake reports a change for every accepted save. Tests that need the
	// no-change branch set saveNoChange, because "nothing moved" is a distinct
	// outcome from both success and failure and the handler renders a third
	// message for it.
	return !f.saveNoChange, nil
}

func (f *fakeData) Fleet(_ context.Context, _ Viewer) (Fleet, error) { return f.fleet, nil }

func (f *fakeData) AccessLog(_ context.Context, _ Viewer, _ AccessQuery) ([]AccessEntry, error) {
	return f.access, nil
}

func (f *fakeData) Artifacts(_ context.Context, _ Viewer, sessionID string) ([]Artifact, error) {
	if f.artifactErr != nil {
		return nil, f.artifactErr
	}
	return f.artifacts[sessionID], nil
}

func (f *fakeData) Links(_ context.Context, _ Viewer, sessionID string) ([]Link, error) {
	return f.links[sessionID], nil
}

func (f *fakeData) Artifact(_ context.Context, _ Viewer, id int64) (Artifact, []ArtifactVersion, error) {
	for _, arts := range f.artifacts {
		for _, a := range arts {
			if a.ID == id {
				return a, f.versions[id], nil
			}
		}
	}
	return Artifact{}, nil, ErrNotFound
}

func (f *fakeData) ArtifactContent(_ context.Context, _ Viewer, _ int64, eventID string) (string, error) {
	body, ok := f.contents[eventID]
	if !ok {
		return "", ErrNotFound
	}
	return body, nil
}

func (f *fakeData) DailyUsage(_ context.Context, v Viewer, _, _ time.Time, _ string, types []string) ([]DayUsage, error) {
	f.tokenTypes = types
	// Whole-scope rows only: blank-email rows are the fleet totals an admin
	// sees, and a member's whole scope is their own per-person rows.
	var out []DayUsage
	for _, d := range f.scopedUsage(v) {
		if v.Admin && d.Email == "" {
			out = append(out, d)
		}
		if !v.Admin && d.Email == v.Email {
			out = append(out, d)
		}
	}
	return out, nil
}

func (f *fakeData) DailyUsageByPerson(_ context.Context, v Viewer, _, _ time.Time, _ string, emails, types []string) ([]DayUsage, error) {
	f.personSeriesTypes = types
	want := map[string]bool{}
	for _, e := range emails {
		want[e] = true
	}
	var out []DayUsage
	for _, d := range f.scopedUsage(v) {
		if d.Email != "" && want[d.Email] {
			out = append(out, d)
		}
	}
	return out, nil
}

func (f *fakeData) DailyUsageByType(_ context.Context, v Viewer, _, _ time.Time, _ string) ([]DayUsage, error) {
	var out []DayUsage
	for _, d := range f.byType {
		if v.Admin || d.Email == "" || d.Email == v.Email {
			out = append(out, d)
		}
	}
	return out, nil
}

// scopedUsage applies the store's privacy rule to the fixture: a member's rows
// are only their own. The fake enforces it so a handler that leaked the scope
// decision to the template would still fail the member-visibility test.
func (f *fakeData) scopedUsage(v Viewer) []DayUsage {
	if v.Admin {
		return f.usage
	}
	var out []DayUsage
	for _, d := range f.usage {
		if d.Email == "" || d.Email == v.Email {
			out = append(out, d)
		}
	}
	return out
}

func (f *fakeData) PersonUsage(_ context.Context, v Viewer, _, _ time.Time, _, prefix string, top int, types []string) ([]PersonUsage, error) {
	f.personTypes = types
	var out []PersonUsage
	for _, p := range f.perPerson {
		if !v.Admin && p.Email != v.Email {
			continue
		}
		if prefix != "" && !strings.HasPrefix(strings.ToLower(p.Email), strings.ToLower(prefix)) &&
			!strings.HasPrefix(strings.ToLower(p.DisplayName), strings.ToLower(prefix)) {
			continue
		}
		out = append(out, p)
		if len(out) == top {
			break
		}
	}
	return out, nil
}

func (f *fakeData) FilterOptions(_ context.Context, v Viewer) ([]string, []string, error) {
	if !v.Admin {
		return []string{v.Email}, f.repoOptions, nil
	}
	return f.peopleOptions, f.repoOptions, nil
}

// Turns pages the seeded turns by index, mirroring the store's read: main
// thread by default, the subagent turns whose start falls inside the page's
// span, and a cursor that is the last index on the page.
func (f *fakeData) Turns(_ context.Context, v Viewer, id string, q TurnQuery) (TurnPage, error) {
	if err := f.gate(v, id); err != nil {
		return TurnPage{}, err
	}
	f.lastTurns = q
	f.turnReads++
	seeded, ok := f.turns[id]
	if !ok && len(f.events[id]) > 0 {
		// A fixture that seeded events but no turns: everything the session
		// holds as one head turn, which is what a page shows for rows the
		// fold has not placed under a prompt.
		head := Turn{Outcome: "no_answer_captured", StartedAt: f.events[id][0].OccurredAt}
		for _, e := range f.events[id] {
			head.Events = append(head.Events, TurnEvent{Event: e, Role: "work"})
		}
		seeded = []Turn{head}
	}
	var main, agents []Turn
	for _, t := range seeded {
		if t.Thread == q.Thread {
			main = append(main, t)
		} else if q.Thread == "" && t.Thread != "" {
			agents = append(agents, t)
		}
	}
	sort.SliceStable(main, func(i, j int) bool { return main[i].Index < main[j].Index })
	after := -1
	if q.After != nil {
		after = *q.After
	}
	var page TurnPage
	for _, t := range main {
		if t.Index > after {
			page.Turns = append(page.Turns, t)
		}
	}
	if q.Limit > 0 && len(page.Turns) > q.Limit {
		page.Turns, page.HasMore = page.Turns[:q.Limit], true
		next := page.Turns[q.Limit-1].Index
		page.NextAfter = &next
	}
	if len(page.Turns) > 0 {
		var until time.Time
		if page.HasMore {
			for _, t := range main {
				if t.Index == *page.NextAfter+1 {
					until = t.StartedAt
				}
			}
		}
		for _, a := range agents {
			if !a.StartedAt.Before(page.Turns[0].StartedAt) && (until.IsZero() || a.StartedAt.Before(until)) {
				page.Agents = append(page.Agents, a)
			}
		}
	}
	return page, nil
}

// FleetEvaluation answers the seeded evaluation; nil renders the page without
// the evaluator's sections, which is what a store failure degrades to.
func (f *fakeData) FleetEvaluation(_ context.Context, v Viewer) (*fleet.Evaluation, error) {
	if !v.Admin {
		return nil, ErrNotFound
	}
	if f.evalErr != nil {
		return nil, f.evalErr
	}
	return f.eval, nil
}

func (f *fakeData) MuteFleet(_ context.Context, v Viewer, m FleetMute) error {
	if !v.Admin {
		return ErrDenied
	}
	f.muted = append(f.muted, m)
	return nil
}

func (f *fakeData) UnmuteFleet(_ context.Context, v Viewer, email, kind string) error {
	if !v.Admin {
		return ErrDenied
	}
	f.unmuted = append(f.unmuted, email+"/"+kind)
	return nil
}

// Facets answers from the seeded facets; a session absent from the fixture
// gets the zero facet, which is what a store row with defaults reads as.
func (f *fakeData) Facets(_ context.Context, v Viewer, ids []string) (map[string]SessionFacet, error) {
	f.facetReads++
	out := map[string]SessionFacet{}
	for _, id := range ids {
		if f.gate(v, id) != nil {
			continue
		}
		if fc, ok := f.facets[id]; ok {
			out[id] = fc
		} else {
			out[id] = SessionFacet{Type: "user", HeadState: "complete", TitleSource: "human"}
		}
	}
	return out, nil
}

// fixedNow keeps relative timestamps deterministic across the whole suite.
var fixedNow = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

func newServer(t *testing.T, f *fakeData, v Viewer) *Server {
	t.Helper()
	s, err := New(Options{
		Data:    f,
		Viewer:  func(*http.Request) (Viewer, bool) { return v, v.Email != "" },
		CSRFKey: []byte("test-key-for-signing-form-tokens"),
		Now:     func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// seedSession installs a session owned by email with a small, deliberately
// hostile transcript: content that is valid HTML is the normal case in a corpus
// scraped from tool output.
func (f *fakeData) seedSession(id, email string) SessionDetail {
	start := fixedNow.Add(-time.Hour)
	d := SessionDetail{
		Session: Session{
			ID:               id,
			Email:            email,
			Name:             "Test Person",
			Source:           "claude_code",
			Repo:             "acme/api",
			Branch:           "main",
			StartedAt:        start,
			EndedAt:          start.Add(22 * time.Minute),
			IngestedAt:       start.Add(23 * time.Minute),
			Ended:            true,
			UserTurns:        2,
			ToolCalls:        3,
			Subagents:        1,
			Errors:           1,
			FirstPrompt:      "fix the flaky test",
			HarnessVersions:  []string{"1.2.3"},
			TokensInput:      1200,
			TokensOutput:     3400,
			TokensCacheRead:  99000,
			TokensCacheWrite: 4500,
			CostUSD:          0.4213,
		},
		Via:    "own",
		Agents: []Agent{{AgentID: "agent-1", Name: "reviewer", ToolCalls: 2}},
	}
	f.sessions[id] = d

	input, _ := json.Marshal(map[string]string{"command": "go test ./..."})
	f.events[id] = []event.Event{
		{
			ID: "ev1", SessionID: id, Seq: 1, Type: event.SessionStarted,
			Source: event.SourceClaudeCode, Origin: event.OriginHook,
			OccurredAt: start, Cwd: "/repo", GitBranch: "main",
		},
		{
			ID: "ev2", SessionID: id, Seq: 2, Type: event.UserPrompt,
			Source: event.SourceClaudeCode, Origin: event.OriginHook,
			OccurredAt: start.Add(time.Second),
			Text:       `fix <script>alert("xss")</script> please`,
		},
		{
			ID: "ev3", SessionID: id, Seq: 3, Type: event.AssistantTurn,
			Source: event.SourceClaudeCode, Origin: event.OriginHook,
			OccurredAt: start.Add(5 * time.Minute), Model: "claude-opus-5",
			Text:  "Running the tests now.",
			Usage: &event.Usage{InputTokens: 1200, OutputTokens: 3400},
		},
		{
			ID: "ev4", SessionID: id, Seq: 4, Type: event.ToolCall,
			Source: event.SourceClaudeCode, Origin: event.OriginHook,
			OccurredAt: start.Add(6 * time.Minute),
			Tool:       &event.Tool{Name: "Bash", Input: input},
		},
		{
			ID: "ev5", SessionID: id, Seq: 5, Type: event.ToolResult,
			Source: event.SourceClaudeCode, Origin: event.OriginHook,
			OccurredAt: start.Add(8 * time.Minute),
			Tool: &event.Tool{
				Name:   "Bash",
				Output: `<img src=x onerror="alert(1)"> FAIL TestThing`,
			},
		},
		{
			ID: "ev6", SessionID: id, Seq: 6, Type: event.ToolCall,
			Source: event.SourceClaudeCode, Origin: event.OriginHook,
			OccurredAt: start.Add(9 * time.Minute), AgentID: "agent-1",
			Tool: &event.Tool{
				Name: "Edit",
				Diff: &event.Diff{
					Path:   "main.go",
					Before: "package main\n\nfunc main() {}\n",
					After:  "package main\n\nfunc main() { println(\"<b>hi</b>\") }\n",
				},
			},
		},
		{
			ID: "ev7", SessionID: id, Seq: 7, Type: event.ToolFailed,
			Source: event.SourceClaudeCode, Origin: event.OriginHook,
			OccurredAt: start.Add(20 * time.Minute), AgentID: "agent-1",
			Tool: &event.Tool{Name: "Edit", Error: "permission denied"},
		},
	}
	f.seedTurns(id)
	return d
}

// seedTurns folds the seeded events into the turns the runner would have
// written: one main turn opened by the prompt, answered by the one assistant
// text, with the two tool rows as work; and the subagent's stream as a turn
// of its own with no prompt, a failed tool and no answer.
func (f *fakeData) seedTurns(id string) {
	evs := map[string]event.Event{}
	for _, e := range f.events[id] {
		evs[e.ID] = e
	}
	te := func(eid, role, kind string) TurnEvent {
		return TurnEvent{Event: evs[eid], Role: role, Kind: kind}
	}
	main := Turn{
		Thread: "", Index: 0, Kind: "human", Outcome: "answered",
		PromptEventID: "ev2", FinalEventID: "ev3",
		StartedAt: evs["ev2"].OccurredAt, FirstActivityAt: evs["ev3"].OccurredAt,
		LastActivityAt: evs["ev5"].OccurredAt, AnsweredAt: evs["ev3"].OccurredAt,
		Wall: 8 * time.Minute, Active: 3*time.Minute + 59*time.Second, Idle: 4*time.Minute + time.Second,
		Origins: []string{"hook"}, Prompts: 1, ToolCalls: 1, Subagents: 1,
		TokensInput: 1200, TokensOutput: 3400, CostUSD: 0.4213, Model: "claude-opus-5",
		Events: []TurnEvent{te("ev2", "prompt", "human"), te("ev3", "final", "assistant_text"), te("ev4", "work", ""), te("ev5", "work", "tool")},
	}
	agent := Turn{
		Thread: "agent-1", Index: 0, Kind: "", Outcome: "no_answer_captured",
		StartedAt: evs["ev6"].OccurredAt, FirstActivityAt: evs["ev6"].OccurredAt, LastActivityAt: evs["ev7"].OccurredAt,
		Wall: 11 * time.Minute, Active: 0, Idle: 11 * time.Minute,
		Origins: []string{"hook"}, ToolCalls: 1, Errors: 1,
		Events: []TurnEvent{te("ev6", "work", ""), te("ev7", "work", "tool")},
	}
	f.turns[id] = []Turn{main, agent}
}

func (f *fakeData) seedFleet() {
	f.fleet = Fleet{
		Enrolled: 3, Reporting: 2, Degraded: 1, Critical: 0, Silent: 1,
		Machines: []Machine{
			{
				Email: "a@example.com", Name: "A", DeviceID: "d1",
				ReceivedAt: fixedNow.Add(-2 * time.Minute),
				Report: health.Report{
					Hostname: "a-mbp", OS: "darwin", Arch: "arm64", AgentVersion: "0.4.1",
					Spool:      health.SpoolHealth{Pending: 12, OldestAge: 90 * time.Second},
					Conditions: []health.Condition{{Level: health.LevelDegraded, Kind: health.KindBacklogGrowing, Detail: "42 pending"}},
				},
			},
			{
				Email: "b@example.com", DeviceID: "d2", Silent: true,
				ReceivedAt: fixedNow.Add(-72 * time.Hour),
				Report:     health.Report{Hostname: "b-mbp", OS: "darwin", Arch: "arm64"},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Skills
// ---------------------------------------------------------------------------

// The skill fixtures: the summary as an admin would read it, narrowed by
// role the way the store narrows it, so a handler that leaked the scope
// decision to the template would still fail the member-visibility test.
type fakeSkills struct {
	summary    SkillSummary
	unused     []SkillUnusedRow
	unknown    []SkillUnknownRow
	pruning    []SkillPruningRow
	compliance []SkillComplianceRow
	rebuild    SkillRebuild
	// sessionSkills is keyed by session; every origin is seeded so the
	// template's own derived-only rule can be seen to hold.
	sessionSkills map[string][]SkillInvocation
	lastQuery     SkillQuery
	err           error
	// The four supplementary reads fail on their own so the page's
	// per-panel state can be seen: the handler swallows each error and the
	// panel must then say unavailable rather than state a positive fact
	// about the data (adversarial finding 3).
	unusedErr, unknownErr, pruningErr, complianceErr error
}

func (f *fakeData) skillFixtures() *fakeSkills {
	if f.skills == nil {
		f.skills = &fakeSkills{sessionSkills: map[string][]SkillInvocation{}}
	}
	return f.skills
}

func (f *fakeData) SkillSummary(_ context.Context, v Viewer, q SkillQuery) (SkillSummary, error) {
	s := f.skillFixtures()
	s.lastQuery = q
	if s.err != nil {
		return SkillSummary{}, s.err
	}
	out := s.summary
	if !v.Admin {
		var own []SkillPersonRow
		for _, p := range out.ByPerson {
			if p.Email == v.Email {
				own = append(own, p)
			}
		}
		out.ByPerson = own
	}
	return out, nil
}

func (f *fakeData) SkillUnused(_ context.Context, _ Viewer, q SkillQuery) ([]SkillUnusedRow, error) {
	s := f.skillFixtures()
	if err := firstErr(s.unusedErr, s.err); err != nil {
		return nil, err
	}
	return s.unused, nil
}

// firstErr is the panel's own failure, then the fixture-wide one.
func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeData) SkillUnknown(_ context.Context, v Viewer, q SkillQuery) ([]SkillUnknownRow, error) {
	s := f.skillFixtures()
	if err := firstErr(s.unknownErr, s.err); err != nil {
		return nil, err
	}
	if v.Admin {
		return s.unknown, nil
	}
	var out []SkillUnknownRow
	for _, r := range s.unknown {
		r.RawName, r.Suggested = "", nil
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeData) SkillPruning(_ context.Context, v Viewer, q SkillQuery) ([]SkillPruningRow, error) {
	s := f.skillFixtures()
	if !v.Admin {
		return nil, ErrNotFound
	}
	if err := firstErr(s.pruningErr, s.err); err != nil {
		return nil, err
	}
	return s.pruning, nil
}

func (f *fakeData) SkillCompliance(_ context.Context, v Viewer, q SkillQuery) ([]SkillComplianceRow, error) {
	s := f.skillFixtures()
	if !v.Admin {
		return nil, ErrNotFound
	}
	if err := firstErr(s.complianceErr, s.err); err != nil {
		return nil, err
	}
	return s.compliance, nil
}

func (f *fakeData) SkillRebuild(_ context.Context) (SkillRebuild, error) {
	s := f.skillFixtures()
	return s.rebuild, s.err
}

func (f *fakeData) SessionSkills(_ context.Context, v Viewer, id string) ([]SkillInvocation, error) {
	if err := f.gate(v, id); err != nil {
		return nil, err
	}
	return f.skillFixtures().sessionSkills[id], nil
}
