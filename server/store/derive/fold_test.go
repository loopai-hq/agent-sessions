package derive

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/normalize"
)

var t0 = time.Date(2026, 9, 10, 10, 0, 0, 0, time.UTC)

func hash8(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])[:8]
}

// fixture builds rows for a session; each call stamps a fresh id so two
// origins of the same moment are two rows.
type fixture struct {
	rows []Row
	n    int
}

func (f *fixture) add(origin event.Origin, typ event.Type, at time.Time, mut ...func(*Row)) *Row {
	f.n++
	r := Row{
		ID:         fmt.Sprintf("%s-%s-%d", origin, typ, f.n),
		Seq:        int64(f.n),
		Type:       typ,
		Origin:     origin,
		OccurredAt: at,
	}
	for _, m := range mut {
		m(&r)
	}
	f.rows = append(f.rows, r)
	return &f.rows[len(f.rows)-1]
}

func prompt(text string, pid string) func(*Row) {
	return func(r *Row) {
		r.Kind = normalize.KindHuman
		r.HasText = true
		r.TextHash = hash8(text)
		r.PromptID = pid
	}
}

func answer(text string, pid string) func(*Row) {
	return func(r *Row) {
		r.Kind = normalize.KindAssistantText
		r.HasText = true
		r.HasUsage = true
		r.TextHash = hash8(text)
		r.PromptID = pid
	}
}

func notification(text, pid string) func(*Row) {
	return func(r *Row) {
		r.Kind = normalize.KindTaskNotification
		r.HasText = true
		r.TextHash = hash8(text)
		r.PromptID = pid
	}
}

func mainTurns(res Result) []Turn {
	var out []Turn
	for _, t := range res.Turns {
		if t.Thread == "" {
			out = append(out, t)
		}
	}
	return out
}

func findRole(t Turn, id string) Role {
	for _, e := range t.Events {
		if e.ID == id {
			return e.Role
		}
	}
	return ""
}

// The property the table exists for: a session captured by hooks alone, by
// its transcript alone, or by both, folds to the same turns, and the prompt
// each turn shows is the transcript's copy whenever one exists.
func TestFoldTurnsHookOnlyTranscriptOnlyAndBothAgree(t *testing.T) {
	build := func(hook, transcript bool) (Result, []string) {
		var f fixture
		var transcriptPrompts []string
		for i, text := range []string{"first ask", "second ask", "third ask"} {
			at := t0.Add(time.Duration(i) * 10 * time.Minute)
			pid := fmt.Sprintf("pid-%d", i)
			if hook {
				f.add(event.OriginHook, event.UserPrompt, at, prompt(text, pid))
				f.add(event.OriginHook, event.ToolCall, at.Add(5*time.Second), func(r *Row) { r.PromptID = pid; r.ToolUseID = "tu-" + pid })
				f.add(event.OriginHook, event.ToolResult, at.Add(6*time.Second), func(r *Row) { r.PromptID = pid; r.ToolUseID = "tu-" + pid })
				f.add(event.OriginHook, event.AssistantTurn, at.Add(20*time.Second), answer("done "+text, pid))
			}
			if transcript {
				p := f.add(event.OriginTranscript, event.UserPrompt, at.Add(time.Second), prompt(text, pid))
				transcriptPrompts = append(transcriptPrompts, p.ID)
				f.add(event.OriginTranscript, event.ToolCall, at.Add(5*time.Second), func(r *Row) { r.ToolUseID = "tu-" + pid })
				f.add(event.OriginTranscript, event.ToolResult, at.Add(6*time.Second), func(r *Row) { r.ToolUseID = "tu-" + pid })
				f.add(event.OriginTranscript, event.AssistantTurn, at.Add(21*time.Second), answer("done "+text, pid))
			}
		}
		return FoldTurns(f.rows, Options{SessionEnded: true}), transcriptPrompts
	}
	hookOnly, _ := build(true, false)
	transcriptOnly, tPrompts := build(false, true)
	both, bPrompts := build(true, true)

	for name, res := range map[string]Result{"hook": hookOnly, "transcript": transcriptOnly, "both": both} {
		turns := mainTurns(res)
		if len(turns) != 3 {
			t.Fatalf("%s: %d turns, want 3", name, len(turns))
		}
		for i, tr := range turns {
			if tr.Key != fmt.Sprintf("pid:pid-%d", i) {
				t.Errorf("%s: turn %d key = %q", name, i, tr.Key)
			}
			if tr.ToolCalls != 1 {
				t.Errorf("%s: turn %d tool_calls = %d, want 1 (origin election, never the sum)", name, i, tr.ToolCalls)
			}
			if tr.Outcome != OutcomeAnswered || tr.FinalEventID == "" {
				t.Errorf("%s: turn %d outcome = %s final = %q", name, i, tr.Outcome, tr.FinalEventID)
			}
			if tr.Prompts != 1 {
				t.Errorf("%s: turn %d prompts = %d, want 1", name, i, tr.Prompts)
			}
		}
	}
	for i := range tPrompts {
		if transcriptOnly.Turns[i].PromptEventID != tPrompts[i] {
			t.Errorf("transcript-only turn %d prompt = %q, want %q", i, transcriptOnly.Turns[i].PromptEventID, tPrompts[i])
		}
		if both.Turns[i].PromptEventID != bPrompts[i] {
			t.Errorf("dual-origin turn %d prompt = %q, want the transcript copy %q", i, both.Turns[i].PromptEventID, bPrompts[i])
		}
		if !strings.HasPrefix(both.Turns[i].FinalEventID, "transcript-") {
			t.Errorf("dual-origin turn %d final = %q, want the transcript copy", i, both.Turns[i].FinalEventID)
		}
		if both.Turns[i].Merged != 2 {
			t.Errorf("dual-origin turn %d merged = %d, want the hook prompt and the hook answer", i, both.Turns[i].Merged)
		}
		if got := both.Turns[i].Origins; len(got) != 2 || got[0] != "hook" || got[1] != "transcript" {
			t.Errorf("dual-origin turn %d origins = %v", i, got)
		}
	}
	// Only hook rows are superseded, and every superseded hook row names a
	// transcript row.
	if len(both.Superseded) != 6 {
		t.Errorf("superseded %d rows, want 3 hook prompts and 3 hook answers: %v", len(both.Superseded), both.Superseded)
	}
	for hook, by := range both.Superseded {
		if !strings.HasPrefix(hook, "hook-") || !strings.HasPrefix(by, "transcript-") {
			t.Errorf("superseded %s by %s: only hook rows are ever superseded, only by transcript rows", hook, by)
		}
	}
	// The transcript tool rows are elected out in the mapping alone: hook
	// output is canonical, but no transcript row carries the marker.
	for _, tr := range both.Turns {
		for _, e := range tr.Events {
			if strings.HasPrefix(e.ID, "transcript-tool_") && e.Role != RoleSuperseded {
				t.Errorf("%s has role %s, want superseded in the mapping (hook output is canonical)", e.ID, e.Role)
			}
			if strings.HasPrefix(e.ID, "hook-tool_") && e.Role != RoleWork {
				t.Errorf("%s has role %s, want work", e.ID, e.Role)
			}
		}
	}
}

// The case that exists today in production: the fleet's hook prompts carry
// no prompt id and the transcript copy does. Pairing runs before naming, so
// the pair is one turn and the turn is keyed by the transcript's id.
func TestFoldTurnsPairsAnOldHookPromptWithItsKeyedTranscriptTwin(t *testing.T) {
	var f fixture
	h := f.add(event.OriginHook, event.UserPrompt, t0, prompt("fix the bug", ""))
	tr := f.add(event.OriginTranscript, event.UserPrompt, t0.Add(2*time.Second), prompt("fix the bug", "pid-a"))
	f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(30*time.Second), answer("fixed", ""))

	res := FoldTurns(f.rows, Options{SessionEnded: true})
	turns := mainTurns(res)
	if len(turns) != 1 {
		t.Fatalf("%d turns, want 1", len(turns))
	}
	if turns[0].Key != "pid:pid-a" {
		t.Errorf("key = %q, want pid:pid-a", turns[0].Key)
	}
	if turns[0].PromptEventID != tr.ID {
		t.Errorf("prompt = %q, want the transcript copy", turns[0].PromptEventID)
	}
	if res.Superseded[h.ID] != tr.ID {
		t.Errorf("the hook prompt is superseded by %q, want %q", res.Superseded[h.ID], tr.ID)
	}
	if findRole(turns[0], h.ID) != RoleSuperseded {
		t.Errorf("hook prompt role = %s", findRole(turns[0], h.ID))
	}
}

// Two distinct prompts one second apart are two turns, and same-second
// groups that neither pair nor merge get ordinals, never a dropped turn and
// never a repeated key. Same-origin copies with identical text now merge
// (review-2 finding 29, below), so the ordinal path is exercised on the
// shape that still reaches it: prompts with no text at all, which have no
// text to compare. The ordinal counts on the base key (review-1 finding 1):
// the third group is #3, because turns has UNIQUE (session_id, thread,
// turn_key) and a repeated #2 makes the whole session unfoldable.
func TestFoldTurnsKeepsDistinctPromptsApart(t *testing.T) {
	textless := func(r *Row) { r.Kind = normalize.KindHuman }
	var f fixture
	f.add(event.OriginHook, event.UserPrompt, t0, prompt("ping", ""))
	f.add(event.OriginHook, event.UserPrompt, t0.Add(time.Second), textless)
	f.add(event.OriginHook, event.UserPrompt, t0.Add(time.Second), textless)
	f.add(event.OriginHook, event.UserPrompt, t0.Add(time.Second+300*time.Millisecond), textless)
	f.add(event.OriginHook, event.UserPrompt, t0.Add(time.Second+600*time.Millisecond), textless)
	res := FoldTurns(f.rows, Options{SessionEnded: true})
	turns := mainTurns(res)
	if len(turns) != 5 {
		t.Fatalf("%d turns, want 5", len(turns))
	}
	blank := "ts:" + fmt.Sprint(t0.Add(time.Second).Unix()) + ":notext"
	want := []string{
		"ts:" + fmt.Sprint(t0.Unix()) + ":" + hash8("ping"),
		blank,
		blank + "#2",
		blank + "#3",
		blank + "#4",
	}
	for i, tr := range turns {
		if tr.Key != want[i] {
			t.Errorf("turn %d key = %q, want %q", i, tr.Key, want[i])
		}
	}
	seen := map[string]bool{}
	for _, tr := range turns {
		if seen[tr.Key] {
			t.Errorf("key %q repeats inside one thread", tr.Key)
		}
		seen[tr.Key] = true
	}
}

// Same-origin copies of one prompt with identical text, in the same second,
// on the same thread and with no prompt id are walk duplicates: the Codex
// walker stores one prompt three to six times (production, review-1's
// rc/prod-tskey.out: 118 such groups in 22 sessions; "resume" and the like
// three times, the <turn_aborted> marker four and six times). The fold
// merges them into one turn counted once: merged grows, prompts does not,
// and the copies are elected out in the mapping without a marker on any
// transcript row (lead decision after review-2, finding 29). A person really
// typing the same text twice inside one second is the accepted cost.
// Distinct text, a different second, a prompt id of their own or no text at
// all keep groups apart.
func TestFoldTurnsMergesSameSecondSameTextWalkDuplicates(t *testing.T) {
	const aborted = "<turn_aborted> The user interrupted the previous turn on purpose. Any pending work should be stopped."
	interrupted := func(r *Row) {
		r.Kind = normalize.KindInterrupted
		r.HasText = true
		r.TextHash = hash8(aborted)
	}
	copies := func(f *fixture, at time.Time, n int, mut func(*Row)) {
		for i := 0; i < n; i++ {
			f.add(event.OriginTranscript, event.UserPrompt, at.Add(time.Duration(i)*100*time.Millisecond), mut)
		}
	}
	type turnWant struct {
		prompts, merged int
		outcome         Outcome
	}
	cases := []struct {
		name  string
		build func(f *fixture)
		want  []turnWant
	}{
		{
			name: "one prompt stored three times in one second",
			build: func(f *fixture) {
				copies(f, t0, 3, prompt("resume", ""))
				f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(30*time.Second), answer("resumed", ""))
			},
			want: []turnWant{{1, 2, OutcomeAnswered}},
		},
		{
			name: "the aborted marker stored six times",
			build: func(f *fixture) {
				f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("first", ""))
				f.add(event.OriginTranscript, event.ToolCall, t0.Add(time.Second), func(r *Row) { r.ToolUseID = "t1" })
				copies(f, t0.Add(3*time.Second), 6, interrupted)
			},
			want: []turnWant{{2, 5, OutcomeInterrupted}},
		},
		{
			name: "the production session: every prompt three times, one of them interrupted",
			build: func(f *fixture) {
				copies(f, t0, 3, prompt("also, that PR is much bigger than ours", ""))
				f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(30*time.Second), answer("because", ""))
				copies(f, t0.Add(time.Minute), 3, prompt("keep tracing", ""))
				f.add(event.OriginTranscript, event.ToolCall, t0.Add(time.Minute+time.Second), func(r *Row) { r.ToolUseID = "t2" })
				copies(f, t0.Add(time.Minute+3*time.Second), 6, interrupted)
				copies(f, t0.Add(2*time.Minute), 3, prompt("resume", ""))
				f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(2*time.Minute+30*time.Second), answer("resumed", ""))
			},
			want: []turnWant{{1, 2, OutcomeAnswered}, {2, 7, OutcomeInterrupted}, {1, 2, OutcomeAnswered}},
		},
		{
			name: "identical text in different seconds is two prompts",
			build: func(f *fixture) {
				f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("again", ""))
				f.add(event.OriginTranscript, event.UserPrompt, t0.Add(time.Second), prompt("again", ""))
			},
			want: []turnWant{{1, 0, OutcomeNoWork}, {1, 0, OutcomeNoWork}},
		},
		{
			name: "copies that carry prompt ids of their own are two prompts",
			build: func(f *fixture) {
				f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("ok", "A"))
				f.add(event.OriginTranscript, event.UserPrompt, t0.Add(100*time.Millisecond), prompt("ok", "B"))
			},
			want: []turnWant{{1, 0, OutcomeNoWork}, {1, 0, OutcomeNoWork}},
		},
		{
			name: "a hook copy pairs with the nearest transcript copy and the rest merge behind it",
			build: func(f *fixture) {
				f.add(event.OriginHook, event.UserPrompt, t0, prompt("resume", ""))
				copies(f, t0.Add(time.Second), 3, prompt("resume", ""))
			},
			want: []turnWant{{1, 3, OutcomeNoWork}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var f fixture
			tc.build(&f)
			res := FoldTurns(f.rows, Options{SessionEnded: true})
			turns := mainTurns(res)
			if len(turns) != len(tc.want) {
				t.Fatalf("%d turns, want %d: %+v", len(turns), len(tc.want), turns)
			}
			for i, w := range tc.want {
				tr := turns[i]
				if tr.Prompts != w.prompts || tr.Merged != w.merged || tr.Outcome != w.outcome {
					t.Errorf("turn %d prompts %d merged %d outcome %s, want %d %d %s", i, tr.Prompts, tr.Merged, tr.Outcome, w.prompts, w.merged, w.outcome)
				}
				if strings.Contains(tr.Key, "#") {
					t.Errorf("turn %d key %q carries an ordinal; a walk duplicate merges, it is not a second turn", i, tr.Key)
				}
			}
			for id, by := range res.Superseded {
				if !strings.HasPrefix(id, string(event.OriginHook)) {
					t.Errorf("transcript row %s was superseded by %s", id, by)
				}
			}
		})
	}

	// The copies are the turn's, elected out: the first copy is the prompt,
	// the others are superseded in the mapping only, and user_turns (the sum
	// of prompts) reads one.
	var f fixture
	a := f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("resume", ""))
	b := f.add(event.OriginTranscript, event.UserPrompt, t0.Add(200*time.Millisecond), prompt("resume", ""))
	c := f.add(event.OriginTranscript, event.UserPrompt, t0.Add(400*time.Millisecond), prompt("resume", ""))
	res := FoldTurns(f.rows, Options{SessionEnded: true})
	turns := mainTurns(res)
	if len(turns) != 1 || turns[0].PromptEventID != a.ID {
		t.Fatalf("turns = %+v, want one turn opened by the first copy", turns)
	}
	if findRole(turns[0], a.ID) != RolePrompt || findRole(turns[0], b.ID) != RoleSuperseded || findRole(turns[0], c.ID) != RoleSuperseded {
		t.Errorf("roles = %s %s %s, want prompt, superseded, superseded", findRole(turns[0], a.ID), findRole(turns[0], b.ID), findRole(turns[0], c.ID))
	}
	if len(res.Superseded) != 0 {
		t.Errorf("a transcript copy carries the marker: %v", res.Superseded)
	}
	if turns[0].Key != "ts:"+fmt.Sprint(t0.Unix())+":"+hash8("resume") {
		t.Errorf("key = %q", turns[0].Key)
	}
}

// Identical text more than the window apart is two prompts, on either side.
func TestFoldTurnsDoesNotPairBeyondTheWindow(t *testing.T) {
	var f fixture
	f.add(event.OriginHook, event.UserPrompt, t0, prompt("continue", ""))
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(20*time.Second), prompt("continue", ""))
	res := FoldTurns(f.rows, Options{SessionEnded: true})
	if got := len(mainTurns(res)); got != 2 {
		t.Fatalf("%d turns, want 2: copies twenty seconds apart are not one prompt", got)
	}
	if len(res.Superseded) != 0 {
		t.Errorf("superseded %v, want nothing", res.Superseded)
	}
}

// The ordering from review-design-correctness F9, built from the probe in
// research/evidence/hookprobe3.log: H1 answered, H2 typed, a background
// task's notification arrives (hook copy with its own prompt id B, transcript
// copy under H1's id A), then Stop(B). The notification and its answer
// belong to H1; H2 keeps no answer that was not its own.
func TestFoldTurnsAttachesANotificationByItsTranscriptPromptID(t *testing.T) {
	var f fixture
	f.add(event.OriginHook, event.UserPrompt, t0, prompt("run the probe", "A"))
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(time.Second), prompt("run the probe", "A"))
	f.add(event.OriginHook, event.AssistantTurn, t0.Add(30*time.Second), answer("waiting for the agent", "A"))
	f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(31*time.Second), answer("waiting for the agent", "A"))

	f.add(event.OriginHook, event.UserPrompt, t0.Add(60*time.Second), prompt("meanwhile, what time is it", "C"))
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(61*time.Second), prompt("meanwhile, what time is it", "C"))

	note := "<task-notification><task-id>a642</task-id><status>completed</status></task-notification>"
	hookNote := f.add(event.OriginHook, event.UserPrompt, t0.Add(90*time.Second), notification(note, "B"))
	trNote := f.add(event.OriginTranscript, event.UserPrompt, t0.Add(91*time.Second), notification(note, "A"))
	stopB := f.add(event.OriginHook, event.AssistantTurn, t0.Add(95*time.Second), answer("DONE", "B"))

	res := FoldTurns(f.rows, Options{SessionEnded: false})
	turns := mainTurns(res)
	if len(turns) != 2 {
		t.Fatalf("%d turns, want H1 and H2: %+v", len(turns), turns)
	}
	h1, h2 := turns[0], turns[1]
	if h1.Key != "pid:A" || h2.Key != "pid:C" {
		t.Fatalf("keys = %q, %q", h1.Key, h2.Key)
	}
	if findRole(h1, trNote.ID) != RoleWork {
		t.Errorf("the transcript notification is %q in H1, want work", findRole(h1, trNote.ID))
	}
	if findRole(h1, hookNote.ID) != RoleSuperseded {
		t.Errorf("the hook notification is %q in H1, want superseded by its transcript twin", findRole(h1, hookNote.ID))
	}
	if res.Superseded[hookNote.ID] != trNote.ID {
		t.Errorf("hook notification superseded by %q, want %q", res.Superseded[hookNote.ID], trNote.ID)
	}
	if h1.FinalEventID != stopB.ID {
		t.Errorf("H1 final = %q, want the notification's answer %q", h1.FinalEventID, stopB.ID)
	}
	if h1.Prompts != 2 {
		t.Errorf("H1 prompts = %d, want the opener and the notification", h1.Prompts)
	}
	if h2.FinalEventID != "" || h2.Outcome != OutcomeInProgress {
		t.Errorf("H2 final = %q outcome = %s; Stop(B) must never become H2's answer", h2.FinalEventID, h2.Outcome)
	}
	if findRole(h2, stopB.ID) != "" {
		t.Errorf("Stop(B) is in H2 as %q", findRole(h2, stopB.ID))
	}
}

// A hook Stop that captured neither text nor usage (the fleet build's
// 18,816 answerless turns) is never an answer, and stands aside for the
// transcript's answer when one exists.
func TestFoldTurnsNeverMakesAnEmptyHookStopTheFinal(t *testing.T) {
	var f fixture
	f.add(event.OriginHook, event.UserPrompt, t0, prompt("hello", ""))
	empty := f.add(event.OriginHook, event.AssistantTurn, t0.Add(10*time.Second))
	res := FoldTurns(f.rows, Options{SessionEnded: true})
	turns := mainTurns(res)
	if len(turns) != 1 {
		t.Fatalf("%d turns", len(turns))
	}
	if turns[0].FinalEventID != "" {
		t.Errorf("an empty Stop became the final answer")
	}
	if turns[0].Outcome != OutcomeNoAnswerCaptured {
		t.Errorf("outcome = %s, want no_answer_captured", turns[0].Outcome)
	}
	if findRole(turns[0], empty.ID) != RoleWork {
		t.Errorf("empty Stop role = %s, want work when there is no transcript answer to stand aside for", findRole(turns[0], empty.ID))
	}

	// With the transcript's answer present, the empty Stop is superseded by it.
	tr := f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(9*time.Second), answer("hi there", ""))
	res = FoldTurns(f.rows, Options{SessionEnded: true})
	turns = mainTurns(res)
	if turns[0].FinalEventID != tr.ID {
		t.Errorf("final = %q, want the transcript answer", turns[0].FinalEventID)
	}
	if res.Superseded[empty.ID] != tr.ID {
		t.Errorf("empty Stop superseded by %q, want %q", res.Superseded[empty.ID], tr.ID)
	}
}

// Outcomes: interrupted, no_work, in_progress, no_answer_captured.
func TestFoldTurnsOutcomes(t *testing.T) {
	var f fixture
	f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("do it", "p1"))
	f.add(event.OriginTranscript, event.ToolCall, t0.Add(time.Second), func(r *Row) { r.ToolUseID = "t1" })
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(2*time.Second), func(r *Row) {
		r.Kind = normalize.KindInterrupted
		r.HasText = true
		r.TextHash = hash8("[Request interrupted by user]")
	})
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(time.Minute), prompt("nothing happened here", "p2"))
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(2*time.Minute), prompt("tools but no answer", "p3"))
	f.add(event.OriginTranscript, event.ToolCall, t0.Add(2*time.Minute+time.Second), func(r *Row) { r.ToolUseID = "t2" })
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(3*time.Minute), prompt("still running", "p4"))

	res := FoldTurns(f.rows, Options{SessionEnded: false})
	turns := mainTurns(res)
	want := []Outcome{OutcomeInterrupted, OutcomeNoWork, OutcomeNoAnswerCaptured, OutcomeInProgress}
	if len(turns) != len(want) {
		t.Fatalf("%d turns, want %d", len(turns), len(want))
	}
	for i, tr := range turns {
		if tr.Outcome != want[i] {
			t.Errorf("turn %d outcome = %s, want %s", i, tr.Outcome, want[i])
		}
	}
	// Once the session ends the last turn is no longer in progress.
	res = FoldTurns(f.rows, Options{SessionEnded: true})
	if got := mainTurns(res)[3].Outcome; got != OutcomeNoWork {
		t.Errorf("after the end marker the last turn is %s, want no_work", got)
	}
	// Waiting for the person is the gap between one turn's last activity
	// and the next turn's start.
	if got := turns[0].WaitingForHumanMS; got != (time.Minute - 2*time.Second).Milliseconds() {
		t.Errorf("waiting_for_human_ms = %d", got)
	}
}

// Rows from before the first prompt belong to a head turn, so a session
// whose head was never imported still shows its work; lifecycle markers
// alone open nothing.
func TestFoldTurnsHeadTurnAndLifecycle(t *testing.T) {
	var f fixture
	f.add(event.OriginHook, event.SessionStarted, t0)
	f.add(event.OriginHook, event.ToolResult, t0.Add(time.Second), func(r *Row) { r.ToolUseID = "x" })
	f.add(event.OriginHook, event.AssistantTurn, t0.Add(2*time.Second), answer("partial", ""))
	f.add(event.OriginHook, event.UserPrompt, t0.Add(time.Minute), prompt("next", "p"))
	f.add(event.OriginHook, event.SessionEnded, t0.Add(2*time.Minute))
	res := FoldTurns(f.rows, Options{SessionEnded: true})
	turns := mainTurns(res)
	if len(turns) != 2 {
		t.Fatalf("%d turns, want the head turn and one prompt", len(turns))
	}
	if turns[0].Key != HeadKey || turns[0].PromptEventID != "" || turns[0].Prompts != 0 {
		t.Errorf("head turn = %+v", turns[0])
	}
	if turns[0].Outcome != OutcomeAnswered {
		t.Errorf("head outcome = %s", turns[0].Outcome)
	}
	if turns[0].Index != 0 || turns[1].Index != 1 {
		t.Errorf("indexes = %d, %d", turns[0].Index, turns[1].Index)
	}
	for _, tr := range turns {
		for _, e := range tr.Events {
			if strings.Contains(e.ID, "session_") {
				t.Errorf("lifecycle row %s is inside a turn", e.ID)
			}
		}
	}

	var g fixture
	g.add(event.OriginHook, event.SessionStarted, t0)
	g.add(event.OriginHook, event.SessionEnded, t0.Add(time.Second))
	if got := FoldTurns(g.rows, Options{SessionEnded: true}); len(got.Turns) != 0 {
		t.Errorf("a lifecycle-only session folded to %d turns", len(got.Turns))
	}
}

// Bookkeeping ahead of any turn on a thread (a subagent's start marker, a
// compaction marker) is not dropped: it joins the first turn the thread
// opens, since the harness spawns an agent and then writes its task, or a
// head turn of its own when the thread never opens one (a hook-only subagent
// has start and end markers and nothing else). Dropped, the rows were absent
// from turn_events and from the content_events recount while ingest had
// counted them (review-2 finding 28). The marker is the turn's but not its
// activity: first_activity_at stays the first row after the prompt.
func TestFoldTurnsKeepsPrePromptMarkersInTheFirstTurn(t *testing.T) {
	const agent = "agent-general-a642e1e7c059e7d99"
	var f fixture
	f.add(event.OriginHook, event.SessionStarted, t0)
	compaction := f.add(event.OriginTranscript, event.Compaction, t0.Add(time.Second))
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(10*time.Second), prompt("carry on", "p1"))
	start := f.add(event.OriginTranscript, event.SubagentStart, t0.Add(15*time.Second), func(r *Row) { r.AgentID = agent })
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(16*time.Second), func(r *Row) {
		r.AgentID = agent
		r.Kind = normalize.KindSubagentTask
		r.HasText = true
		r.TextHash = hash8("look things up")
	})
	subAnswer := f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(20*time.Second), func(r *Row) {
		r.AgentID = agent
		answer("SUB-OK", "")(r)
	})
	f.add(event.OriginTranscript, event.SubagentEnd, t0.Add(21*time.Second), func(r *Row) { r.AgentID = agent })
	mainAnswer := f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(30*time.Second), answer("done", "p1"))
	// A second subagent captured by hooks alone: its start and end markers
	// are all the thread holds.
	hookStart := f.add(event.OriginHook, event.SubagentStart, t0.Add(40*time.Second), func(r *Row) { r.AgentID = "b0000000000000000"; r.PromptID = "p1" })
	hookEnd := f.add(event.OriginHook, event.SubagentEnd, t0.Add(50*time.Second), func(r *Row) { r.AgentID = "b0000000000000000"; r.PromptID = "p1" })

	res := FoldTurns(f.rows, Options{SessionEnded: true})
	byThread := map[string][]Turn{}
	for _, tr := range res.Turns {
		byThread[tr.Thread] = append(byThread[tr.Thread], tr)
	}
	main := byThread[""]
	if len(main) != 1 || main[0].Key != "pid:p1" {
		t.Fatalf("main thread = %+v, want one prompt turn and no head turn", main)
	}
	if findRole(main[0], compaction.ID) != RoleWork {
		t.Errorf("the compaction marker ahead of the first prompt has role %q, want work inside the first turn", findRole(main[0], compaction.ID))
	}
	if !main[0].FirstActivityAt.Equal(mainAnswer.OccurredAt) {
		t.Errorf("main first_activity_at = %s, want the answer's %s: a marker ahead of the prompt is not activity", main[0].FirstActivityAt, mainAnswer.OccurredAt)
	}
	if main[0].Subagents != 2 {
		t.Errorf("subagents = %d, want 2", main[0].Subagents)
	}
	sub := byThread["642e1e7c059e7d99"]
	if len(sub) != 1 || sub[0].Kind != normalize.KindSubagentTask {
		t.Fatalf("subagent thread = %+v, want the one task turn (a start marker must not open a head turn ahead of it)", sub)
	}
	if findRole(sub[0], start.ID) != RoleWork {
		t.Errorf("subagent_start role = %q, want work inside the task turn", findRole(sub[0], start.ID))
	}
	if !sub[0].FirstActivityAt.Equal(subAnswer.OccurredAt) {
		t.Errorf("subagent first_activity_at = %s, want the answer's", sub[0].FirstActivityAt)
	}
	hookOnly := byThread["0000000000000000"]
	if len(hookOnly) != 1 || hookOnly[0].Key != HeadKey || hookOnly[0].Prompts != 0 {
		t.Fatalf("hook-only subagent thread = %+v, want one head turn holding its markers", hookOnly)
	}
	if findRole(hookOnly[0], hookStart.ID) != RoleWork || findRole(hookOnly[0], hookEnd.ID) != RoleWork {
		t.Errorf("hook-only markers have roles %q %q, want work", findRole(hookOnly[0], hookStart.ID), findRole(hookOnly[0], hookEnd.ID))
	}
	placed := 0
	for _, tr := range res.Turns {
		placed += len(tr.Events)
	}
	if placed != len(f.rows)-1 {
		t.Errorf("%d of %d rows are inside a turn, want every row but the lifecycle one", placed, len(f.rows)-1)
	}
}

// A subagent's stream is its own thread, folded on its own, and counted on
// the main-thread turn that spawned it, once, whatever both origins named it.
func TestFoldTurnsCountsASubagentOnceAcrossOrigins(t *testing.T) {
	var f fixture
	f.add(event.OriginHook, event.UserPrompt, t0, prompt("research this", "p1"))
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(time.Second), prompt("research this", "p1"))
	f.add(event.OriginHook, event.SubagentStart, t0.Add(5*time.Second), func(r *Row) { r.AgentID = "a642e1e7c059e7d99"; r.PromptID = "p1" })
	f.add(event.OriginTranscript, event.SubagentStart, t0.Add(5*time.Second), func(r *Row) { r.AgentID = "agent-general-a642e1e7c059e7d99" })
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(6*time.Second), func(r *Row) {
		r.AgentID = "agent-general-a642e1e7c059e7d99"
		r.Kind = normalize.KindSubagentTask
		r.HasText = true
		r.TextHash = hash8("look things up")
		r.Seq = 1
	})
	f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(20*time.Second), func(r *Row) {
		r.AgentID = "agent-general-a642e1e7c059e7d99"
		answer("SUB-OK", "")(r)
	})
	f.add(event.OriginTranscript, event.SubagentEnd, t0.Add(21*time.Second), func(r *Row) { r.AgentID = "agent-general-a642e1e7c059e7d99" })
	f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(30*time.Second), answer("main answer", ""))

	res := FoldTurns(f.rows, Options{SessionEnded: false})
	var main, agent []Turn
	for _, tr := range res.Turns {
		if tr.Thread == "" {
			main = append(main, tr)
		} else {
			agent = append(agent, tr)
		}
	}
	if len(main) != 1 || len(agent) != 1 {
		t.Fatalf("main %d agent %d turns", len(main), len(agent))
	}
	if main[0].Subagents != 1 {
		t.Errorf("subagents = %d, want 1 (one thread, two spellings)", main[0].Subagents)
	}
	if agent[0].Thread != "642e1e7c059e7d99" {
		t.Errorf("agent thread = %q", agent[0].Thread)
	}
	if agent[0].Kind != normalize.KindSubagentTask || agent[0].Outcome != OutcomeAnswered {
		t.Errorf("agent turn = %+v", agent[0])
	}
}

// The same prompt id delivered three times from one origin (a file walked
// under several emission rules before record identity existed; production
// holds 2,420 such groups) is one turn AND one prompt: the copies are merged
// into the turn, not counted, because user_turns is the sum of prompts and
// "one logical turn counts once" is the table's reason to exist (review-1
// finding 4); like every walk duplicate they are elected out in the mapping,
// so the reader renders the prompt once and the content recount counts it
// once. A notification folded into the turn still counts as a prompt.
func TestFoldTurnsMergesASameOriginDuplicateByPromptID(t *testing.T) {
	var f fixture
	a := f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("once", "p1"))
	b := f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("once", "p1"))
	c := f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("once", "p1"))
	f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(time.Second), answer("ok", "p1"))
	res := FoldTurns(f.rows, Options{SessionEnded: true})
	turns := mainTurns(res)
	if len(turns) != 1 {
		t.Fatalf("%d turns, want 1", len(turns))
	}
	if turns[0].PromptEventID != a.ID || findRole(turns[0], b.ID) != RoleSuperseded || findRole(turns[0], c.ID) != RoleSuperseded {
		t.Errorf("prompt = %q, duplicate roles = %s, %s, want the copies elected out (superseded in the mapping, rendered by nobody, left out of the content recount)", turns[0].PromptEventID, findRole(turns[0], b.ID), findRole(turns[0], c.ID))
	}
	if turns[0].Prompts != 1 {
		t.Errorf("prompts = %d for one prompt delivered three times, want 1: user_turns = sum(prompts) would count it %d times", turns[0].Prompts, turns[0].Prompts)
	}
	if turns[0].Merged != 2 {
		t.Errorf("merged = %d, want the two copies", turns[0].Merged)
	}
	if len(res.Superseded) != 0 {
		t.Errorf("a transcript row was superseded: %v", res.Superseded)
	}

	// A notification under the same prompt id is not a copy of the prompt:
	// it is the harness's own record folded into the turn, and counts.
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(2*time.Second), notification("<task-notification><task-id>x</task-id></task-notification>", "p1"))
	res = FoldTurns(f.rows, Options{SessionEnded: true})
	if got := mainTurns(res); len(got) != 1 || got[0].Prompts != 2 || got[0].Merged != 2 {
		t.Errorf("with a notification: turns %d prompts %d merged %d, want 1, 2, 2", len(got), got[0].Prompts, got[0].Merged)
	}
}

// An interruption marker without a prompt id (Codex, and 1 in 300 Claude
// Code copies) belongs to the turn in progress, not to the previous turn
// that already has its answer (review-1 finding 8).
func TestFoldTurnsAttachesAnUnkeyedInterruptionToTheTurnInProgress(t *testing.T) {
	var f fixture
	f.add(event.OriginTranscript, event.UserPrompt, t0, prompt("first", "A"))
	f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(5*time.Second), answer("done", "A"))
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(time.Minute), prompt("second", "B"))
	f.add(event.OriginTranscript, event.ToolCall, t0.Add(time.Minute+time.Second), func(r *Row) { r.PromptID = "B"; r.ToolUseID = "t1" })
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(time.Minute+3*time.Second), func(r *Row) {
		r.Kind = normalize.KindInterrupted
		r.HasText = true
		r.TextHash = hash8("<turn_aborted> The user interrupted the previous turn on purpose.")
	})
	res := FoldTurns(f.rows, Options{SessionEnded: true})
	turns := mainTurns(res)
	if len(turns) != 2 {
		t.Fatalf("%d turns, want 2", len(turns))
	}
	if turns[0].Outcome != OutcomeAnswered {
		t.Errorf("turn A outcome = %s, want answered: the interruption belongs to B", turns[0].Outcome)
	}
	if turns[1].Outcome != OutcomeInterrupted {
		t.Errorf("turn B outcome = %s, want interrupted", turns[1].Outcome)
	}
}

// The fold is a function of its input: the same rows in any order give the
// same result.
func TestFoldTurnsIsOrderIndependent(t *testing.T) {
	var f fixture
	f.add(event.OriginHook, event.UserPrompt, t0, prompt("a", "p1"))
	f.add(event.OriginTranscript, event.UserPrompt, t0.Add(time.Second), prompt("a", "p1"))
	f.add(event.OriginHook, event.ToolCall, t0.Add(2*time.Second), func(r *Row) { r.ToolUseID = "t" })
	f.add(event.OriginTranscript, event.ToolCall, t0.Add(2*time.Second), func(r *Row) { r.ToolUseID = "t" })
	f.add(event.OriginTranscript, event.AssistantTurn, t0.Add(3*time.Second), answer("b", "p1"))
	f.add(event.OriginHook, event.UserPrompt, t0.Add(time.Minute), prompt("c", "p2"))
	forward := FoldTurns(f.rows, Options{})
	rev := make([]Row, len(f.rows))
	for i := range f.rows {
		rev[len(f.rows)-1-i] = f.rows[i]
	}
	backward := FoldTurns(rev, Options{})
	if fmt.Sprintf("%+v", forward) != fmt.Sprintf("%+v", backward) {
		t.Errorf("order changed the fold:\n%+v\n%+v", forward, backward)
	}
}
