package web

import (
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// turnFixture builds one main-thread turn with a human prompt, one tool
// call and result as work, and an assistant final, with the given outcome.
func turnFixture(outcome string) Turn {
	base := fixedNow.Add(-time.Hour)
	prompt := event.Event{ID: "p1", Seq: 1, Type: event.UserPrompt, OccurredAt: base, Text: "fix the flaky test", Source: event.SourceClaudeCode}
	call := event.Event{ID: "c1", Seq: 2, Type: event.ToolCall, OccurredAt: base.Add(10 * time.Second), Tool: &event.Tool{Name: "Bash", Input: []byte(`{"command":"go test"}`)}, ToolUseID: "tu1"}
	result := event.Event{ID: "r1", Seq: 3, Type: event.ToolResult, OccurredAt: base.Add(40 * time.Second), Tool: &event.Tool{Name: "Bash", Output: "ok"}, ToolUseID: "tu1"}
	final := event.Event{ID: "f1", Seq: 4, Type: event.AssistantTurn, OccurredAt: base.Add(50 * time.Second), Text: "Done, the test passes now.", Model: "claude-opus-5"}
	t := Turn{
		Kind: "human", Outcome: outcome, PromptEventID: "p1", StartedAt: base,
		FirstActivityAt: call.OccurredAt, LastActivityAt: final.OccurredAt,
		Active: 50 * time.Second, Wall: 50 * time.Second, ToolCalls: 1, Origins: []string{"hook", "transcript"}, Merged: 2,
		Events: []TurnEvent{
			{Event: prompt, Role: "prompt", Kind: "human"},
			{Event: call, Role: "work"},
			{Event: result, Role: "work", Kind: "tool"},
		},
	}
	if outcome == "answered" || outcome == "interrupted" {
		t.FinalEventID = "f1"
		t.AnsweredAt = final.OccurredAt
		t.Events = append(t.Events, TurnEvent{Event: final, Role: "final", Kind: "assistant_text"})
	}
	return t
}

func renderTurn(t *testing.T, tv TurnView) string {
	t.Helper()
	return renderConversation(t, "turn", tv)
}

// TestTurnRendersEachOutcomeInItsAnswerSlot: the slot every turn ends with is
// decided by the fold's outcome. The final answer expanded; an interruption
// chip; a placeholder naming the client fix; a no-work line; in progress.
func TestTurnRendersEachOutcomeInItsAnswerSlot(t *testing.T) {
	cases := map[string][]string{
		"answered":           {"Done, the test passes now.", `class="blk blk-assistant"`},
		"interrupted":        {"Interrupted by you", "answer-interrupted"},
		"no_answer_captured": {"Answer not captured", "788dcb3", "loop-sessions daemon --upgrade-now", "answer-missing"},
		"no_work":            {"No work or answer followed this prompt", "answer-nowork"},
		"in_progress":        {"In progress", "answer-live"},
	}
	for outcome, wants := range cases {
		tv := buildTurnView(turnFixture(outcome), TranscriptOptions{Start: fixedNow.Add(-time.Hour)})
		out := renderTurn(t, tv)
		for _, want := range wants {
			if !strings.Contains(out, want) {
				t.Errorf("%s: missing %q in\n%s", outcome, want, out)
			}
		}
		if !strings.Contains(out, `data-outcome="`+outcome+`"`) {
			t.Errorf("%s: the section does not carry its outcome", outcome)
		}
		if outcome != "answered" && outcome != "interrupted" && strings.Contains(out, "blk-assistant") {
			t.Errorf("%s: an answer was rendered where the fold recorded none", outcome)
		}
	}
}

// TestTurnCollapsesWorkUnderItsTimingLabel: the tools sit inside a collapsed
// details whose label is the fold's active and idle time, whose hover names
// the wall-clock range with its zone, and which says when both capture paths
// contributed.
func TestTurnCollapsesWorkUnderItsTimingLabel(t *testing.T) {
	turn := turnFixture("answered")
	turn.Active, turn.Idle = 4*time.Minute+12*time.Second, 2*time.Minute
	tv := buildTurnView(turn, TranscriptOptions{Start: fixedNow.Add(-time.Hour)})
	if tv.WorkLabel() != "Worked 4m 12s, 2m 00s idle" {
		t.Errorf("label = %q", tv.WorkLabel())
	}
	out := renderTurn(t, tv)
	for _, want := range []string{`<details class="work-summary">`, "Worked 4m 12s, 2m 00s idle", "2 sources", `title="Recorded `, "go test", `class="blk blk-tool"`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if !strings.Contains(out, fixedNow.Add(-time.Hour).Add(10*time.Second).Local().Format("2006-01-02 15:04:05 MST")) {
		t.Error("the hover does not carry the wall-clock range with its zone")
	}
	// The prompt is a bubble above the work and the final sits below it.
	prompt, work, final := strings.Index(out, `class="blk blk-prompt"`), strings.Index(out, "work-summary"), strings.Index(out, `class="blk blk-assistant"`)
	if !(prompt < work && work < final) {
		t.Errorf("order prompt=%d work=%d final=%d", prompt, work, final)
	}
	// The tool result paired onto its call.
	if strings.Count(out, `class="blk blk-tool`) != 1 {
		t.Errorf("tool rows = %d, want the call with its result merged", strings.Count(out, `class="blk blk-tool`))
	}
	if tv.WorkLabel() == "" || buildTurnView(Turn{}, TranscriptOptions{}).WorkLabel() != "Recorded work" {
		t.Error("a turn with no timings has no label")
	}
}

// TestTurnRendersASlashCommandAsAChip: a command opens the turn as a chip
// with its arguments, never as a bubble of raw command markup.
func TestTurnRendersASlashCommandAsAChip(t *testing.T) {
	turn := turnFixture("answered")
	turn.Kind = "slash_command"
	turn.Events[0].Kind = "slash_command"
	turn.Events[0].Event.Text = "<command-message>git is running</command-message>\n<command-name>/git</command-name>\n<command-args>--autonomous</command-args>"
	tv := buildTurnView(turn, TranscriptOptions{Start: fixedNow.Add(-time.Hour)})
	if tv.Command != "/git" || tv.Args != "--autonomous" {
		t.Fatalf("command = %q args = %q", tv.Command, tv.Args)
	}
	out := renderTurn(t, tv)
	if !strings.Contains(out, `class="chip-cmd"`) || !strings.Contains(out, "/git") || !strings.Contains(out, `<span class="chip-args">--autonomous</span>`) {
		t.Errorf("no command chip in\n%s", out)
	}
	if strings.Contains(out, "message-body") && strings.Contains(out, "command-message") && !strings.Contains(out, "&lt;command-message&gt;") {
		t.Error("the raw command markup rendered as a bubble")
	}
}

// TestTurnRendersEveryContextKindAsACollapsedNeutralRow: every kind the
// normalizer can give a user-role record renders as a collapsed row titled
// by its kind, never as a person's bubble.
func TestTurnRendersEveryContextKindAsACollapsedNeutralRow(t *testing.T) {
	kinds := map[string]string{
		"caveat":              "Local command caveat",
		"command_output":      "Local command output",
		"task_notification":   "Task notification",
		"system_reminder":     "System reminder",
		"system_notification": "System notification",
		"teammate_message":    "Teammate message",
		"interrupted":         "Request interrupted",
		"ide_context":         "IDE context",
		"compact_summary":     "Context compacted",
		"env_context":         "Environment context",
		"harness_injected":    "Harness message",
	}
	for kind, title := range kinds {
		turn := turnFixture("answered")
		ctx := event.Event{ID: "ctx", Seq: 5, Type: event.UserPrompt, OccurredAt: turn.StartedAt.Add(5 * time.Second), Text: "<harness>" + kind + " body</harness>", Source: event.SourceClaudeCode}
		turn.Events = append(turn.Events, TurnEvent{Event: ctx, Role: "work", Kind: kind})
		tv := buildTurnView(turn, TranscriptOptions{Start: turn.StartedAt})
		if len(tv.Context) != 1 || tv.Context[0].Kind != KindInternal || tv.Context[0].Title != title {
			t.Errorf("%s: context = %+v", kind, tv.Context)
			continue
		}
		out := renderTurn(t, tv)
		if !strings.Contains(out, `<details class="internal-message">`) || !strings.Contains(out, title) {
			t.Errorf("%s: no collapsed neutral row titled %q", kind, title)
		}
		if strings.Count(out, `class="blk blk-prompt"`) != 1 {
			t.Errorf("%s: the harness record rendered as a person's bubble", kind)
		}
		if !strings.Contains(out, "&lt;harness&gt;") {
			t.Errorf("%s: the record's markup was not escaped", kind)
		}
	}
	// A row stored before kinds existed reads as a person's prompt.
	turn := turnFixture("answered")
	turn.Events[0].Kind = ""
	if tv := buildTurnView(turn, TranscriptOptions{}); tv.Prompt == nil || tv.Prompt.Kind != KindPrompt {
		t.Error("an unclassified prompt row did not render as a person's bubble")
	}
}

// TestSubagentTurnsNestUnderTheTurnThatSpawnedThem: a subagent's turn sits
// inside the main turn in progress when it started, labelled by its agent,
// with its task prompt titled Task and its own answer slot.
func TestSubagentTurnsNestUnderTheTurnThatSpawnedThem(t *testing.T) {
	main := turnFixture("answered")
	later := turnFixture("no_work")
	later.Index, later.StartedAt = 1, main.StartedAt.Add(10*time.Minute)
	later.Events = later.Events[:1]
	later.Events[0].Event.ID, later.PromptEventID = "p2", "p2"
	later.Events[0].Event.OccurredAt = later.StartedAt

	agentStart := main.StartedAt.Add(20 * time.Second)
	task := event.Event{ID: "at", Seq: 1, Type: event.UserPrompt, OccurredAt: agentStart, Text: "Review the diff for races", AgentID: "agent-reviewer-0123456789abcdef"}
	reply := event.Event{ID: "ar", Seq: 2, Type: event.AssistantTurn, OccurredAt: agentStart.Add(30 * time.Second), Text: "No races found."}
	reply.AgentID = task.AgentID
	agent := Turn{
		Thread: "0123456789abcdef", Kind: "subagent_task", Outcome: "answered", PromptEventID: "at", FinalEventID: "ar",
		StartedAt: agentStart, LastActivityAt: reply.OccurredAt, Active: 30 * time.Second,
		Events: []TurnEvent{{Event: task, Role: "prompt", Kind: "subagent_task"}, {Event: reply, Role: "final", Kind: "assistant_text"}},
	}
	// The start marker on the parent names the agent's type.
	main.Events = append(main.Events, TurnEvent{Event: event.Event{ID: "ss", Seq: 9, Type: event.SubagentStart, OccurredAt: agentStart, AgentID: "a0123456789abcdef", Text: "code-reviewer"}, Role: "work"})

	views := buildTurnViews(TurnPage{Turns: []Turn{main, later}, Agents: []Turn{agent}}, TranscriptOptions{Start: main.StartedAt}, "")
	if len(views) != 2 || len(views[0].Agents) != 1 || len(views[1].Agents) != 0 {
		t.Fatalf("agents nested under turns %d/%d, want under the first only", len(views[0].Agents), len(views[1].Agents))
	}
	av := views[0].Agents[0]
	if av.Label != "reviewer" || av.AgentType != "code-reviewer" || av.Anchor != "a0123456789abcdef-0" {
		t.Errorf("agent view label %q type %q anchor %q", av.Label, av.AgentType, av.Anchor)
	}
	out := renderTurn(t, views[0])
	for _, want := range []string{`class="grp grp-agent"`, "reviewer", `class="tag tag-type">code-reviewer`, `<div class="who">Task</div>`, "Review the diff for races", "No races found."} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if !strings.Contains(out, "answered &middot; 30s") {
		t.Errorf("agent stats missing: %s", out)
	}
}

// TestInheritedTurnsAreDividedFromTheSessionsOwnWork: a fork's copied
// prefix is drawn under a "copied from" divider and the session's own first
// turn under a second one.
func TestInheritedTurnsAreDividedFromTheSessionsOwnWork(t *testing.T) {
	a, b, c := turnFixture("answered"), turnFixture("answered"), turnFixture("answered")
	a.Inherited, b.Inherited = true, true
	b.Index, c.Index = 1, 2
	views := buildTurnViews(TurnPage{Turns: []Turn{a, b, c}}, TranscriptOptions{}, "parent-1")
	if views[0].Divider != "Copied from" || views[0].DividerLink != "parent-1" || views[1].Divider != "" || views[2].Divider != "This session's own work begins" {
		t.Errorf("dividers = %q / %q / %q", views[0].Divider, views[1].Divider, views[2].Divider)
	}
	out := renderTurn(t, views[0])
	if !strings.Contains(out, `<p class="turn-divider">Copied from <a href="/sessions/parent-1">`) || !strings.Contains(out, "turn-inherited") {
		t.Errorf("no copied-from divider in\n%s", out)
	}
}

// TestHeadBannerNamesTheWindowAndTheBackfillCommand: a session whose head
// was not imported opens with the window start and the CTA that imports it;
// a capture-loss session names the device condition instead.
func TestHeadBannerNamesTheWindowAndTheBackfillCommand(t *testing.T) {
	f := newFake()
	f.seedSession("s1", owner.Email)
	f.facets["s1"] = SessionFacet{Type: "user", HeadState: "truncated_window", TitleSource: "human"}
	s := newServer(t, f, owner)
	body := get(t, s, "/sessions/s1").Body.String()
	// The window start is the first turn's start, stamped with its zone.
	first := f.turns["s1"][0].StartedAt
	for _, want := range []string{"Capture begins here; earlier activity not imported (window started", "loop-sessions backfill --session s1", stamp(first), first.Local().Format("MST")} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
	// The banner belongs to the first page only.
	if second := get(t, s, "/sessions/s1?after=0").Body.String(); strings.Contains(second, "Capture begins here") {
		t.Error("the head banner repeated on a later page")
	}
	f.facets["s1"] = SessionFacet{Type: "user", HeadState: "capture_loss", CaptureLossDrops: 42}
	body = get(t, s, "/sessions/s1").Body.String()
	if !strings.Contains(body, "reported dropping 42 events") || !strings.Contains(body, "drops_recorded") {
		t.Errorf("capture loss banner missing: %s", body[strings.Index(body, "conversation"):min(len(body), strings.Index(body, "conversation")+600)])
	}
	f.facets["s1"] = SessionFacet{Type: "user", HeadState: "complete"}
	if body := get(t, s, "/sessions/s1").Body.String(); strings.Contains(body, "Capture begins here") || strings.Contains(body, "reported dropping") {
		t.Error("a complete session carries a head banner")
	}
}

// TestHeadTurnRendersWithoutAPrompt: rows before the first captured prompt
// are shown as work under a note, with no invented bubble.
func TestHeadTurnRendersWithoutAPrompt(t *testing.T) {
	turn := workTurn(event.Event{ID: "w", Seq: 1, Type: event.ToolResult, OccurredAt: fixedNow, Tool: &event.Tool{Name: "Bash", Output: "late"}})
	tv := buildTurnView(turn, TranscriptOptions{})
	if !tv.Head || tv.Prompt != nil {
		t.Fatalf("head=%v prompt=%v", tv.Head, tv.Prompt)
	}
	out := renderTurn(t, tv)
	if !strings.Contains(out, "Work recorded before the first captured prompt") || strings.Contains(out, "blk-prompt") {
		t.Errorf("head turn rendered wrong:\n%s", out)
	}
	if strings.Contains(out, "No work or answer followed") {
		t.Error("a head turn was given a no-work answer slot")
	}
}

// TestSearchMatchesOpenTheWorkTheyLandIn: a highlighted term inside a tool
// result opens the collapsed work so the reader sees the match.
func TestSearchMatchesOpenTheWorkTheyLandIn(t *testing.T) {
	turn := turnFixture("answered")
	turn.Events[2].Event.Tool.Output = "needle in the output"
	tv := buildTurnView(turn, TranscriptOptions{Terms: []string{"needle"}})
	if !tv.Matched {
		t.Fatal("the match inside the work was not noticed")
	}
	out := renderTurn(t, tv)
	if !strings.Contains(out, `<details class="work-summary" open>`) || !strings.Contains(out, "<mark>needle</mark>") {
		t.Errorf("match not revealed:\n%s", out)
	}
}

// TestAnswerAwaitingTheWalkIsNamedAsHarnessBehaviour: a turn whose answer
// was never captured live, which background work held open, and which the
// transcript walk has not reached, is named as harness behaviour instead of
// as a client to upgrade. Every near miss keeps the client line or no note.
func TestAnswerAwaitingTheWalkIsNamedAsHarnessBehaviour(t *testing.T) {
	const walkNote = "Claude Code runs no Stop hook"
	const clientFix = "loop-sessions daemon --upgrade-now"

	withSubagent := func(outcome string, origins ...string) Turn {
		turn := turnFixture(outcome)
		turn.Origins, turn.Subagents = origins, 1
		return turn
	}
	notified := func(outcome string, origins ...string) Turn {
		turn := turnFixture(outcome)
		turn.Origins = origins
		note := event.Event{ID: "tn", Seq: 5, Type: event.UserPrompt, OccurredAt: turn.StartedAt.Add(20 * time.Second),
			Text: "the reviewer agent finished", Source: event.SourceClaudeCode}
		turn.Events = append(turn.Events, TurnEvent{Event: note, Role: "work", Kind: "task_notification"})
		return turn
	}

	cases := []struct {
		name       string
		turn       Turn
		wantWalk   bool
		wantClient bool
	}{
		{"background agents, hook only", withSubagent("no_answer_captured", "hook"), true, false},
		{"a background task reported back", notified("no_answer_captured", "hook"), true, false},
		{"answered", withSubagent("answered", "hook"), false, false},
		{"no background work", func() Turn {
			turn := turnFixture("no_answer_captured")
			turn.Origins = []string{"hook"}
			return turn
		}(), false, true},
		{"the walk already reached this turn", withSubagent("no_answer_captured", "hook", "transcript"), false, true},
		{"still running", withSubagent("in_progress", "hook"), false, false},
		{"no work at all", withSubagent("no_work", "hook"), false, false},
	}
	for _, c := range cases {
		tv := buildTurnView(c.turn, TranscriptOptions{Start: c.turn.StartedAt})
		if tv.AnswerAwaitsWalk() != c.wantWalk {
			t.Errorf("%s: AnswerAwaitsWalk = %v, want %v", c.name, tv.AnswerAwaitsWalk(), c.wantWalk)
		}
		out := renderTurn(t, tv)
		if strings.Contains(out, walkNote) != c.wantWalk {
			t.Errorf("%s: walk note present = %v, want %v in\n%s", c.name, !c.wantWalk, c.wantWalk, out)
		}
		if strings.Contains(out, clientFix) != c.wantClient {
			t.Errorf("%s: client fix present = %v, want %v in\n%s", c.name, !c.wantClient, c.wantClient, out)
		}
		// The two readings of a missing answer are never shown together.
		if strings.Contains(out, walkNote) && strings.Contains(out, clientFix) {
			t.Errorf("%s: both readings of a missing answer rendered in\n%s", c.name, out)
		}
	}
}

// TestASubagentNestedOnThePageCountsAsBackgroundWork: a turn whose fold
// recorded no subagent count still carries the note when the page nested a
// subagent's own turn under it, which is the same background work seen from
// the other side.
func TestASubagentNestedOnThePageCountsAsBackgroundWork(t *testing.T) {
	main := turnFixture("no_answer_captured")
	main.Origins, main.Subagents = []string{"hook"}, 0
	agentStart := main.StartedAt.Add(20 * time.Second)
	task := event.Event{ID: "at", Seq: 1, Type: event.UserPrompt, OccurredAt: agentStart,
		Text: "Review the diff", AgentID: "agent-reviewer-0123456789abcdef"}
	agent := Turn{
		Thread: "0123456789abcdef", Kind: "subagent_task", Outcome: "in_progress", PromptEventID: "at",
		StartedAt: agentStart, LastActivityAt: agentStart, Origins: []string{"hook"},
		Events: []TurnEvent{{Event: task, Role: "prompt", Kind: "subagent_task"}},
	}
	views := buildTurnViews(TurnPage{Turns: []Turn{main}, Agents: []Turn{agent}}, TranscriptOptions{Start: main.StartedAt}, "")
	if len(views) != 1 || len(views[0].Agents) != 1 {
		t.Fatalf("agents nested = %d", len(views[0].Agents))
	}
	if !views[0].AnswerAwaitsWalk() {
		t.Error("a turn with a subagent nested under it was read as a client defect")
	}
	if out := renderTurn(t, views[0]); !strings.Contains(out, "Claude Code runs no Stop hook") {
		t.Errorf("no walk note in\n%s", out)
	}
}
