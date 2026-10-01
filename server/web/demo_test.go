package web

import (
	"math"
	"net/http"
	"os"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

// TestServeDemo serves the dashboard against the in-memory fake so the pages
// can be looked at in a browser without a database, a Google account or a
// deploy, and so screenshots.cjs has something to photograph.
//
//	LOOP_WEB_DEMO=:8099 go test ./server/web -run TestServeDemo -timeout 0
//
// It is a test rather than a command because the fixtures it needs already
// exist here, and because a second binary in the repo would have to be kept
// building forever for the sake of an afternoon's design work.
//
// The clock is the suite's fixedNow, for the server and the fixtures alike.
// The templates render "3h ago" against the clock the server was built with,
// so a fixture stamped from the wall clock sits in that clock's future and
// every row reads "just now"; and a fixed clock means a regenerated screenshot
// differs from the last one only where a page changed. Every identity is a
// placeholder: people on example.com, repositories under acme/, home
// directories under /home.
func TestServeDemo(t *testing.T) {
	addr := os.Getenv("LOOP_WEB_DEMO")
	if addr == "" {
		t.Skip("set LOOP_WEB_DEMO=:8099 to serve the dashboard against fixtures")
	}

	f := newFake()
	now := fixedNow
	demoSessions(f, now)
	demoAnalytics(f, now)

	f.people = []Principal{
		{Email: "alex@example.com", DisplayName: "Alex Rivera", Role: RoleAdmin, AddedBy: "system", AddedAt: now.Add(-720 * time.Hour), LastSeen: now.Add(-2 * time.Minute), Sessions: 412, Devices: 2},
		{Email: "sam@example.com", DisplayName: "Sam Chen", Role: RoleAdmin, AddedBy: "alex@example.com", AddedAt: now.Add(-700 * time.Hour), LastSeen: now.Add(-3 * time.Hour), Sessions: 88, Devices: 1},
		{Email: "jordan@example.com", DisplayName: "Jordan Lee", Role: RoleMember, AddedBy: "alex@example.com", AddedAt: now.Add(-96 * time.Hour), LastSeen: now.Add(-26 * time.Hour), Sessions: 9, Devices: 1},
		{Email: "former@example.com", DisplayName: "Former Colleague", Role: RoleMember, AddedBy: "alex@example.com", AddedAt: now.Add(-2000 * time.Hour), DisabledAt: now.Add(-48 * time.Hour), Sessions: 140, Devices: 3},
	}
	f.seedFleet()
	f.access = []AccessEntry{
		{Viewer: "alex@example.com", SessionID: "s-02", Owner: "jordan@example.com", Via: "admin", At: now.Add(-40 * time.Minute)},
		{Viewer: "sam@example.com", SessionID: "s-04", Owner: "alex@example.com", Via: "share", At: now.Add(-5 * time.Hour)},
	}
	f.hits = []SearchHit{
		{Session: f.sessions["s-02"].Session, EventID: "x", Seq: 44, Role: "tool", OccurredAt: f.sessions["s-02"].StartedAt.Add(9 * time.Minute),
			Text: "psycopg2.errors.UndefinedColumn: column store.vb_name does not exist. The handler swallowed it with except Exception: pass, which is why the rows came back empty rather than failing."},
		{Session: f.sessions["s-04"].Session, EventID: "y", Seq: 12, Role: "assistant", OccurredAt: f.sessions["s-04"].StartedAt.Add(40 * time.Minute),
			Text: "The column does not exist on that table yet, so the model referencing it will return empty until the migration lands."},
	}

	s := newServer(t, f, Viewer{Email: "alex@example.com", Name: "Alex Rivera", Admin: true})
	t.Logf("serving the dashboard fixtures on %s", addr)
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
}

// demoPeople are the roster's active members, by the local part of their
// placeholder address.
var demoPeople = map[string]string{
	"alex":   "Alex Rivera",
	"sam":    "Sam Chen",
	"jordan": "Jordan Lee",
}

// demoSession is one row of the session list, described the way a reader
// sees it: who, where, what was asked and answered, when, and for how long.
type demoSession struct {
	id, who, repo, branch, prompt, answer string
	// ago is how long before now the session started; length is its span.
	ago, length                time.Duration
	turns, tools, agents, errs int
	tokensIn, tokensOut        int64
	harness                    string
	// live marks a session with no end marker yet; auto marks one a machine
	// started, which carries no human turns and a templated title.
	live, auto bool
}

// demoSessions seeds a page of sessions spread over the last week: three
// people, a live one, a nightly automation, and turn counts, lengths and
// costs that vary the way real work does, so the list reads like a dashboard
// somebody uses rather than five copies of one row. Each session's facet is
// seeded too, because the Turns column and the answer snippet come from the
// facet, not from the session row; without one every row shows zero turns.
func demoSessions(f *fakeData, now time.Time) {
	specs := []demoSession{
		{
			id: "s-03", who: "alex", repo: "acme/api", branch: "fix/lease-renewal",
			prompt: "resolve_handler_with_lease is raising on the ledger sync, trace it",
			answer: "The raise comes from renewing a lease the ledger sync already released; tracing where the release happens.",
			ago:    12 * time.Minute, length: 11 * time.Minute, turns: 2, tools: 14,
			tokensIn: 41_000, tokensOut: 6_200, harness: "2.1.4", live: true,
		},
		{
			id: "s-01", who: "alex", repo: "acme/api", branch: "claude/server-web",
			prompt: "build the dashboard for loop-sessions and make the session list fast to scan",
			answer: "Reading the transcript builder first. The grouping currently keys off a start/end stack, which breaks at a page boundary because the start event can be several pages back.",
			ago:    3 * time.Hour, length: 74 * time.Minute, turns: 6, tools: 58, agents: 1, errs: 2,
			tokensIn: 182_000, tokensOut: 27_000, harness: "2.1.4",
		},
		{
			id: "s-07", who: "alex", repo: "acme/tools", branch: "main",
			prompt: "Nightly: re-run the cost backfill for yesterday's sessions and report any drift",
			answer: "Backfilled 212 sessions; the total moved by $0.14 and no single session changed by more than a cent.",
			ago:    10 * time.Hour, length: 6 * time.Minute, tools: 22,
			tokensIn: 58_000, tokensOut: 5_400, harness: "2.1.4", auto: true,
		},
		{
			id: "s-05", who: "jordan",
			prompt: "what changed in the deploy last night",
			answer: "Two commits went out: the fleet page's mute controls and a fix to the access log cursor.",
			ago:    26 * time.Hour, length: 3 * time.Minute, turns: 1, tools: 6,
			tokensIn: 23_000, tokensOut: 3_100, harness: "2.1.3",
		},
		{
			id: "s-06", who: "sam", repo: "acme/api", branch: "main",
			prompt: "add retry with jittered backoff to the ledger client and cover the timeout path",
			answer: "Wrapped the three ledger calls in a retry with full jitter, capped at five attempts; the timeout test passes on the third attempt now.",
			ago:    50 * time.Hour, length: 41 * time.Minute, turns: 5, tools: 33, errs: 1,
			tokensIn: 104_000, tokensOut: 15_000, harness: "2.1.4",
		},
		{
			id: "s-02", who: "jordan", repo: "acme/integrations", branch: "main",
			prompt: "why is the payments webhook handler returning zero rows for every store since Tuesday",
			answer: "The handler swallows UndefinedColumn from a migration that has not landed, so every store's query fails quietly and returns nothing.",
			ago:    53 * time.Hour, length: 22 * time.Minute, turns: 3, tools: 36, errs: 5,
			tokensIn: 112_000, tokensOut: 17_000, harness: "2.1.3",
		},
		{
			id: "s-04", who: "alex", repo: "acme/tools", branch: "main",
			prompt: "add the origin filter to activity.py before the imported sessions corrupt the cost metrics",
			answer: "Imported sessions arrive with origin=import, so the filter drops them before the per-day cost is computed; the metric no longer moves when a backfill lands.",
			ago:    68 * time.Hour, length: 2*time.Hour + 21*time.Minute, turns: 14, tools: 96, agents: 1, errs: 1,
			tokensIn: 420_000, tokensOut: 71_000, harness: "2.1.4",
		},
		{
			id: "s-08", who: "sam", repo: "acme/integrations", branch: "feat/webhook-replay",
			prompt: "write an integration test that replays a captured webhook and asserts the stored row",
			answer: "The replay test posts the captured payload through the real handler and reads the row back; it caught the missing store_id on the first run.",
			ago:    76 * time.Hour, length: 58 * time.Minute, turns: 7, tools: 44, errs: 3,
			tokensIn: 138_000, tokensOut: 21_000, harness: "2.1.4",
		},
		{
			id: "s-10", who: "jordan", repo: "acme/tools", branch: "main",
			prompt: "how do I run the dashboard locally against the fake",
			answer: "Set LOOP_WEB_DEMO to a port and run TestServeDemo; it serves the fixtures with no database behind it.",
			ago:    90 * time.Hour, length: 9 * time.Minute, turns: 2, tools: 4,
			tokensIn: 12_000, tokensOut: 2_400, harness: "2.1.3",
		},
		{
			id: "s-09", who: "alex", repo: "acme/api", branch: "migrate/lattice",
			prompt: "migrate the sessions table to the lattice columns without taking the list page down",
			answer: "The migration adds the columns nullable, backfills in batches of a thousand and flips the read path last, so the list keeps answering throughout.",
			ago:    94 * time.Hour, length: 3*time.Hour + 5*time.Minute, turns: 21, tools: 187, agents: 2, errs: 4,
			tokensIn: 690_000, tokensOut: 118_000, harness: "2.1.4",
		},
		{
			id: "s-11", who: "sam", repo: "acme/tools", branch: "main",
			prompt: "rename activity.py's origin flag to --origin and update the docs",
			answer: "Renamed the flag, kept the old spelling as a hidden alias for one release and updated the two docs pages that mention it.",
			ago:    146 * time.Hour, length: 15 * time.Minute, turns: 3, tools: 11,
			tokensIn: 34_000, tokensOut: 5_100, harness: "2.1.3",
		},
	}
	agents := []Agent{
		{AgentID: "agent-web", Name: "web-deploy", ToolCalls: 31},
		{AgentID: "agent-review", Name: "reviewer", ToolCalls: 12},
	}
	for _, sp := range specs {
		start := now.Add(-sp.ago)
		end := start.Add(sp.length)
		cwd := "/home/" + sp.who + "/scratch"
		if sp.repo != "" {
			cwd = "/home/" + sp.who + "/work/" + path.Base(sp.repo)
		}
		typ := "user"
		if sp.auto {
			typ = "automation"
		}
		// A hook flushes the last event about a minute after the session
		// ends; a live session's newest event has only just arrived.
		ingested := end.Add(time.Minute)
		if sp.live {
			ingested = end.Add(20 * time.Second)
		}
		cacheRead, cacheWrite := sp.tokensIn*24, sp.tokensIn/3
		d := SessionDetail{
			Session: Session{
				ID: sp.id, Email: sp.who + "@example.com", Name: demoPeople[sp.who],
				Source: "claude_code", Type: typ, Repo: sp.repo, Branch: sp.branch, Cwd: cwd,
				StartedAt: start, EndedAt: end, IngestedAt: ingested, Ended: !sp.live,
				UserTurns: sp.turns, ToolCalls: sp.tools, Subagents: sp.agents, Errors: sp.errs,
				FirstPrompt:      sp.prompt,
				HarnessVersions:  []string{sp.harness},
				TokensInput:      sp.tokensIn,
				TokensOutput:     sp.tokensOut,
				TokensCacheRead:  cacheRead,
				TokensCacheWrite: cacheWrite,
				CostUSD:          demoCost(sp.tokensIn, sp.tokensOut, cacheRead, cacheWrite),
			},
			Via:    "own",
			Agents: agents[:sp.agents],
		}
		f.sessions[sp.id] = d
		f.events[sp.id] = demoEvents(sp.id, start, sp.prompt, cwd, !sp.live)
		facet := SessionFacet{Type: typ, HeadState: "complete", TitleSource: "human", HumanTurns: sp.turns, FirstAnswer: sp.answer}
		if sp.auto {
			facet.TitleSource = "automation_template"
		}
		f.facets[sp.id] = facet
	}
}

// demoCost prices tokens at the rates the price seed carries for the
// fixtures' model: $5 per million input tokens, $25 output, cache reads at a
// tenth of input and cache writes at one and a quarter times it. Deriving
// every cost from the same rule keeps a row's tokens and its dollars telling
// the same story.
func demoCost(in, out, cacheRead, cacheWrite int64) float64 {
	const perMillion = 1e6
	return (float64(in)*5 + float64(out)*25 + float64(cacheRead)*0.5 + float64(cacheWrite)*6.25) / perMillion
}

// demoAnalytics seeds thirteen weeks of synthetic usage, the widest window
// /analytics offers, so every range has data behind its tiles and charts: the
// roster's people at different weights, a weekday rhythm with quiet weekends,
// a today that is half over, and two session types so the stacked charts have
// a split to show. The whole-scope rows, the per-type rows and the per-person
// rows are all summed from the same per-person, per-type figures, so the
// tiles, the stacks and the table cannot disagree with each other. The table's
// totals cover the default seven-day range, which is the one the fake serves
// whatever range is asked for.
func demoAnalytics(f *fakeData, now time.Time) {
	type person struct {
		email, name string
		// weight is the person's share of a full day's sessions; since and
		// until bound their usage in days ago, so the newest member has a
		// few days of history and the disabled one stops two days back.
		weight       float64
		since, until int
		lastActive   time.Duration
	}
	people := []person{
		{"alex@example.com", "Alex Rivera", 1.0, 90, 0, 2 * time.Minute},
		{"sam@example.com", "Sam Chen", 0.6, 90, 0, 3 * time.Hour},
		{"former@example.com", "Former Colleague", 0.45, 90, 2, 50 * time.Hour},
		{"jordan@example.com", "Jordan Lee", 0.3, 4, 0, 26 * time.Hour},
	}
	// What one session of each type reads, writes and calls, and how many a
	// full weekday holds at weight one.
	type shape struct {
		typ                                   string
		perDay                                float64
		in, out, cacheRead, cacheWrite, tools int64
		errsPerHundredTools                   int64
	}
	shapes := []shape{
		{"user", 8, 150_000, 24_000, 3_600_000, 380_000, 41, 3},
		{"automation", 3, 48_000, 5_000, 900_000, 60_000, 12, 1},
	}

	y, m, d := now.Date()
	today := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
	const days = 91
	const tableDays = 7
	totals := map[string]*PersonUsage{}
	for daysAgo := days - 1; daysAgo >= 0; daysAgo-- {
		day := today.AddDate(0, 0, -daysAgo)
		load := 1.0
		if wd := day.Weekday(); wd == time.Saturday || wd == time.Sunday {
			load = 0.2
		}
		if daysAgo == 0 {
			load *= 0.45 // today is half over
		}
		load *= 0.7 + float64((daysAgo*7+3)%12)/20 // a deterministic wobble, 0.7 to 1.25

		whole := DayUsage{Day: day}
		byType := make([]DayUsage, len(shapes))
		for i, sh := range shapes {
			byType[i] = DayUsage{Day: day, SessionType: sh.typ}
		}
		for pi, p := range people {
			if daysAgo > p.since || daysAgo < p.until {
				continue
			}
			row := DayUsage{Day: day, Email: p.email}
			for si, sh := range shapes {
				n := int64(math.Round(sh.perDay * p.weight * load))
				if n == 0 {
					continue
				}
				// Sessions are not all the same size either.
				scale := 0.8 + float64((daysAgo*5+pi*3)%9)/20
				u := DayUsage{
					Sessions:   n,
					TokensIn:   int64(float64(n*sh.in) * scale),
					TokensOut:  int64(float64(n*sh.out) * scale),
					CacheRead:  int64(float64(n*sh.cacheRead) * scale),
					CacheWrite: int64(float64(n*sh.cacheWrite) * scale),
					ToolCalls:  int64(float64(n*sh.tools) * scale),
				}
				u.Errors = u.ToolCalls * sh.errsPerHundredTools / 100
				u.CostUSD = demoCost(u.TokensIn, u.TokensOut, u.CacheRead, u.CacheWrite)
				addUsage(&row, u)
				addUsage(&byType[si], u)
			}
			if row.Sessions == 0 {
				continue
			}
			whole.People++
			addUsage(&whole, row)
			f.usage = append(f.usage, row)
			if daysAgo < tableDays {
				t := totals[p.email]
				if t == nil {
					t = &PersonUsage{Email: p.email, DisplayName: p.name, LastActive: now.Add(-p.lastActive)}
					totals[p.email] = t
				}
				t.Sessions += row.Sessions
				t.TokensIn += row.TokensIn
				t.TokensOut += row.TokensOut
				t.CacheRead += row.CacheRead
				t.CostUSD += row.CostUSD
				t.ToolCalls += row.ToolCalls
				t.Errors += row.Errors
			}
		}
		f.usage = append(f.usage, whole)
		f.byType = append(f.byType, byType...)
	}
	for _, t := range totals {
		f.perPerson = append(f.perPerson, *t)
	}
	// The table's default sort, which the fake does not apply itself.
	sort.Slice(f.perPerson, func(i, j int) bool { return f.perPerson[i].TokensIn > f.perPerson[j].TokensIn })
	for _, p := range people {
		f.peopleOptions = append(f.peopleOptions, p.email)
	}
	sort.Strings(f.peopleOptions)
	f.repoOptions = []string{"acme/api", "acme/integrations", "acme/tools"}
}

// addUsage folds one set of figures into a rollup row; the row keeps its own
// day, email, type and people count.
func addUsage(dst *DayUsage, u DayUsage) {
	dst.Sessions += u.Sessions
	dst.TokensIn += u.TokensIn
	dst.TokensOut += u.TokensOut
	dst.CacheRead += u.CacheRead
	dst.CacheWrite += u.CacheWrite
	dst.CostUSD += u.CostUSD
	dst.ToolCalls += u.ToolCalls
	dst.Errors += u.Errors
}

func demoEvents(id string, start time.Time, prompt, cwd string, ended bool) []event.Event {
	mk := func(seq int64, typ event.Type, offset time.Duration, mut func(*event.Event)) event.Event {
		e := event.Event{
			ID: id + "-" + string(rune('a'+seq)), SessionID: id, Seq: seq, Type: typ,
			Source: event.SourceClaudeCode, Origin: event.OriginHook,
			OccurredAt: start.Add(offset),
		}
		if mut != nil {
			mut(&e)
		}
		return e
	}
	bash := []byte(`{"command":"go test ./... -race","description":"run the suite"}`)
	edit := []byte(`{"file_path":"server/web/transcript.go","old_string":"depth","new_string":"agent id"}`)

	evs := []event.Event{
		mk(1, event.SessionStarted, 0, func(e *event.Event) { e.Cwd = cwd; e.GitBranch = "main" }),
		mk(2, event.UserPrompt, time.Second, func(e *event.Event) { e.Text = prompt }),
		mk(3, event.AssistantTurn, 12*time.Second, func(e *event.Event) {
			e.Model = "claude-opus-5"
			e.Text = "Reading the transcript builder first. The grouping currently keys off a start/end stack, which breaks at a page boundary because the start event can be several pages back."
			e.Usage = &event.Usage{InputTokens: 41_233, OutputTokens: 812, CacheReadTokens: 1_204_000}
		}),
		mk(4, event.ToolCall, 20*time.Second, func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Bash", Input: bash}
		}),
		mk(5, event.ToolResult, 74*time.Second, func(e *event.Event) {
			e.Tool = &event.Tool{Name: "Bash", Output: "ok  \tgithub.com/acme/api/server/web\t1.602s\n--- FAIL: TestDiffIgnoresTheTrailingNewline\n    diff_test.go:131: an unchanged file produced \"~\"\nFAIL"}
		}),
		mk(6, event.ToolCall, 6*time.Minute, func(e *event.Event) {
			e.Tool = &event.Tool{
				Name:  "Edit",
				Input: edit,
				Diff: &event.Diff{
					Path:   "server/web/transcript.go",
					Before: "package web\n\n// Group is a run of blocks.\ntype Group struct {\n\tDepth int\n\tBlocks []Block\n}\n\nfunc build() {}\n",
					After:  "package web\n\n// Group is a run of blocks belonging to one actor thread.\ntype Group struct {\n\tAgentID string\n\tLabel   string\n\tBlocks  []Block\n}\n\nfunc build() {}\n",
				},
			}
		}),
		mk(7, event.SubagentStart, 8*time.Minute, func(e *event.Event) {
			e.AgentID = "agent-web"
			e.Text = "review the transcript renderer for escaping mistakes"
		}),
		mk(8, event.ToolCall, 9*time.Minute, func(e *event.Event) {
			e.AgentID = "agent-web"
			e.Tool = &event.Tool{Name: "Grep", Input: []byte(`{"pattern":"template.HTML","path":"server/web"}`), Output: "no matches"}
		}),
		mk(9, event.ToolFailed, 10*time.Minute, func(e *event.Event) {
			e.AgentID = "agent-web"
			e.Tool = &event.Tool{Name: "Write", Error: "open /etc/hosts: permission denied", Input: []byte(`{"file_path":"/etc/hosts"}`)}
		}),
		mk(10, event.AssistantTurn, 24*time.Minute, func(e *event.Event) {
			e.Model = "claude-opus-5"
			e.Text = "Everything renders through the segment partial, so a match inside <script> comes out as escaped text wrapped in a mark element. Nothing in the package converts payload content to template.HTML."
			e.Usage = &event.Usage{InputTokens: 88_120, OutputTokens: 1_402, CacheReadTokens: 2_400_000, CacheCreationTokens: 12_000}
		}),
	}
	// A live session has no end marker yet; that is what makes it live.
	if ended {
		evs = append(evs, mk(11, event.SessionEnded, 26*time.Minute, nil))
	}
	return evs
}
