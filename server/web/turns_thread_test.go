package web

import (
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

// TestASubagentsOwnThreadPageLabelsItsTaskPromptTask is the adversarial
// pass's F3. A thread page (?agent=<id>) renders the agent's turns through
// the same turn partial as the main thread, and that partial printed the
// literal "User" over every prompt bubble: the task that drove the agent,
// a wrapper kind, read as a person's words. The bubble names its actor.
func TestASubagentsOwnThreadPageLabelsItsTaskPromptTask(t *testing.T) {
	start := fixedNow.Add(-time.Hour)
	task := event.Event{ID: "at", Seq: 1, Type: event.UserPrompt, OccurredAt: start, Text: "Review the diff for races", AgentID: "agent-reviewer-0123456789abcdef"}
	reply := event.Event{ID: "ar", Seq: 2, Type: event.AssistantTurn, OccurredAt: start.Add(30 * time.Second), Text: "No races found.", AgentID: task.AgentID}
	agent := Turn{
		Thread: "0123456789abcdef", Kind: "subagent_task", Outcome: "answered", PromptEventID: "at", FinalEventID: "ar",
		StartedAt: start, LastActivityAt: reply.OccurredAt, Active: 30 * time.Second,
		Events: []TurnEvent{{Event: task, Role: "prompt", Kind: "subagent_task"}, {Event: reply, Role: "final", Kind: "assistant_text"}},
	}
	views := buildTurnViews(TurnPage{Turns: []Turn{agent}}, TranscriptOptions{Start: start}, "")
	if len(views) != 1 || views[0].Prompt == nil || views[0].Prompt.Actor != "Task" {
		t.Fatalf("thread turn view = %+v", views)
	}
	out := renderTurn(t, views[0])
	if !strings.Contains(out, `<div class="who">Task</div>`) {
		t.Errorf("the thread page does not label the task prompt Task:\n%s", out)
	}
	if strings.Contains(out, `<div class="who">User</div>`) {
		t.Errorf("the thread page labels the task prompt User:\n%s", out)
	}
	// A person's prompt on the main thread still reads User.
	main := renderTurn(t, buildTurnView(turnFixture("answered"), TranscriptOptions{Start: start}))
	if !strings.Contains(main, `<div class="who">User</div>`) {
		t.Errorf("a human prompt lost its User label:\n%s", main)
	}
}

// TestNoWorkSentenceIsDroppedUnderAWrapperOpenedTurn is F14. A turn a
// wrapper opened has no bubble, so "No work or answer followed this prompt"
// under its collapsed context row names a prompt that is not there.
func TestNoWorkSentenceIsDroppedUnderAWrapperOpenedTurn(t *testing.T) {
	base := fixedNow.Add(-time.Hour)
	reminder := event.Event{ID: "w1", Seq: 1, Type: event.UserPrompt, OccurredAt: base, Text: "<system-reminder>The file changed on disk.</system-reminder>", Source: event.SourceClaudeCode}
	turn := Turn{Kind: "system_reminder", Outcome: "no_work", PromptEventID: "w1", StartedAt: base,
		Events: []TurnEvent{{Event: reminder, Role: "prompt", Kind: "system_reminder"}}}
	tv := buildTurnView(turn, TranscriptOptions{Start: base})
	if tv.Prompt != nil || len(tv.Context) != 1 {
		t.Fatalf("a wrapper-opened turn built prompt %v context %d", tv.Prompt, len(tv.Context))
	}
	out := renderTurn(t, tv)
	if strings.Contains(out, "No work or answer followed this prompt") {
		t.Errorf("the no-work sentence is printed under a turn with no prompt bubble:\n%s", out)
	}
	if !strings.Contains(out, "System reminder") {
		t.Errorf("the wrapper's context row is missing:\n%s", out)
	}
	// A person's prompt with nothing after it keeps the sentence.
	human := renderTurn(t, buildTurnView(turnFixture("no_work"), TranscriptOptions{Start: base}))
	if !strings.Contains(human, "No work or answer followed this prompt") {
		t.Errorf("a human no_work turn lost its sentence:\n%s", human)
	}
}
