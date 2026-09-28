package web

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// TestServeDemo serves the dashboard against the in-memory fake so the pages
// can be looked at in a browser without a database, a Google account or a
// deploy.
//
//	LOOP_WEB_DEMO=:8099 go test ./server/web -run TestServeDemo
//
// It is a test rather than a command because the fixtures it needs already
// exist here, and because a second binary in the repo would have to be kept
// building forever for the sake of an afternoon's design work.
func TestServeDemo(t *testing.T) {
	addr := os.Getenv("LOOP_WEB_DEMO")
	if addr == "" {
		t.Skip("set LOOP_WEB_DEMO=:8099 to serve the dashboard against fixtures")
	}

	f := newFake()
	now := time.Now()
	for i, spec := range []struct {
		id, repo, branch, prompt string
		mins                     int
		cost                     float64
		errs                     int
		ended                    bool
	}{
		{"s-01", "acme/api", "claude/server-web", "build the dashboard for loop-sessions and make the session list fast to scan", 74, 4.8102, 2, true},
		{"s-02", "acme/integrations", "main", "why is the payments webhook handler returning zero rows for every store since Tuesday", 22, 0.9134, 5, true},
		{"s-03", "acme/api", "fix/lease-renewal", "resolve_handler_with_lease is raising on the ledger sync, trace it", 8, 0.0043, 0, false},
		{"s-04", "acme/tools", "main", "add the origin filter to activity.py before the imported sessions corrupt the cost metrics", 141, 12.44, 1, true},
		{"s-05", "", "", "what changed in the deploy last night", 3, 0.0008, 0, true},
	} {
		start := now.Add(-time.Duration(i*7+1) * time.Hour)
		d := SessionDetail{
			Session: Session{
				ID: spec.id, Email: "alex@example.com", Name: "Alex Rivera",
				Source: "claude_code", Repo: spec.repo, Branch: spec.branch,
				Cwd:       "/home/alex/work/api",
				StartedAt: start, EndedAt: start.Add(time.Duration(spec.mins) * time.Minute),
				IngestedAt: start.Add(time.Duration(spec.mins+1) * time.Minute),
				Ended:      spec.ended,
				UserTurns:  4 + i, ToolCalls: 18 * (i + 1), Subagents: i % 3, Errors: spec.errs,
				FirstPrompt:      spec.prompt,
				HarnessVersions:  []string{"2.1.4"},
				TokensInput:      82_000 * int64(i+1),
				TokensOutput:     14_500 * int64(i+1),
				TokensCacheRead:  2_400_000 * int64(i+1),
				TokensCacheWrite: 310_000,
				CostUSD:          spec.cost,
			},
			Via:    "own",
			Agents: []Agent{{AgentID: "agent-web", Name: "web-deploy", ToolCalls: 31}},
		}
		f.sessions[spec.id] = d
		f.events[spec.id] = demoEvents(spec.id, start, spec.prompt)
	}

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
		{Session: f.sessions["s-02"].Session, EventID: "x", Seq: 44, Role: "tool", OccurredAt: now.Add(-3 * time.Hour),
			Text: "psycopg2.errors.UndefinedColumn: column store.vb_name does not exist. The handler swallowed it with except Exception: pass, which is why the rows came back empty rather than failing."},
		{Session: f.sessions["s-04"].Session, EventID: "y", Seq: 12, Role: "assistant", OccurredAt: now.Add(-9 * time.Hour),
			Text: "The column does not exist on that table yet, so the model referencing it will return empty until the migration lands."},
	}

	s := newServer(t, f, Viewer{Email: "alex@example.com", Name: "Alex Rivera", Admin: true})
	s.now = time.Now
	t.Logf("serving the dashboard fixtures on %s", addr)
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
}

func demoEvents(id string, start time.Time, prompt string) []event.Event {
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

	return []event.Event{
		mk(1, event.SessionStarted, 0, func(e *event.Event) { e.Cwd = "/home/alex/work/api"; e.GitBranch = "main" }),
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
		mk(11, event.SessionEnded, 26*time.Minute, nil),
	}
}
