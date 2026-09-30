package web

import (
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// TestContinuousReaderIncludesCompleteCapturedAssistantText: the scrollable
// reader and the reader=1 page fetches render the final answer whole; the
// archive page keeps its display cap and links to the event.
func TestContinuousReaderIncludesCompleteCapturedAssistantText(t *testing.T) {
	f := newFake()
	f.seedSession("s1", owner.Email)
	long := strings.Repeat("Captured reply. ", 2000) + "END OF REPLY"
	f.events["s1"] = append(f.events["s1"], event.Event{ID: "reply", Seq: 9, Type: event.AssistantTurn, OccurredAt: fixedNow, Text: long})
	f.turns["s1"] = []Turn{{
		Kind: "human", Outcome: "answered", PromptEventID: "ev2", FinalEventID: "reply", StartedAt: fixedNow.Add(-time.Hour),
		Events: []TurnEvent{
			{Event: f.events["s1"][1], Role: "prompt", Kind: "human"},
			{Event: f.events["s1"][len(f.events["s1"])-1], Role: "final", Kind: "assistant_text"},
		},
	}}
	s := newServer(t, f, owner)
	for _, path := range []string{"/sessions/s1/conversation", "/sessions/s1?reader=1"} {
		r := get(t, s, path)
		if r.Code != 200 || !strings.Contains(r.Body.String(), "END OF REPLY") || strings.Contains(r.Body.String(), "Shortened for display") {
			t.Fatalf("%s did not render the complete captured reply", path)
		}
		if f.lastTurns.Limit != turnsPerPage {
			t.Fatal("full reply rendering relaxed turn pagination")
		}
	}
	archive := get(t, s, "/sessions/s1")
	if strings.Contains(archive.Body.String(), "END OF REPLY") || !strings.Contains(archive.Body.String(), "Shortened for display") {
		t.Fatal("original archive truncation changed")
	}
	f.readable = func(Viewer, string) error { return ErrNotFound }
	for _, path := range []string{"/sessions/s1/conversation", "/sessions/s1?reader=1"} {
		if r := get(t, s, path); r.Code != 404 {
			t.Fatalf("%s bypassed session authorization: %d", path, r.Code)
		}
	}
}

// TestContinuousReaderHasScopedScriptAndBoundedContinuation: the reader
// loads one first-party script, pages by turn index, and the archive page
// stays script-free.
func TestContinuousReaderHasScopedScriptAndBoundedContinuation(t *testing.T) {
	f := newFake()
	f.seedSession("s1", owner.Email)
	f.turns["s1"] = manyTurns(f.events["s1"][1], 30)
	s := newServer(t, f, owner)
	r := get(t, s, "/sessions/s1/conversation")
	if r.Code != 200 {
		t.Fatal(r.Code)
	}
	body := r.Body.String()
	for _, want := range []string{"conversation.js", `data-next="/sessions/s1?after=24"`, `class="btn transcript-retry"`, `id="t0"`, `id="t24"`} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(body, `id="t25"`) {
		t.Error("the page holds more than twenty-five turns")
	}
	for _, unwanted := range []string{">Earlier<", ">Later<", "Captured activity in this window."} {
		if strings.Contains(body, unwanted) {
			t.Errorf("reader still contains %s", unwanted)
		}
	}
	if f.lastTurns.Limit != turnsPerPage {
		t.Fatal("continuous reader fetched an unbounded session")
	}
	csp := r.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "script-src 'self'") || !strings.Contains(csp, "connect-src 'self'") || strings.Contains(csp, "unsafe-inline") {
		t.Fatal(csp)
	}
	archive := get(t, s, "/sessions/s1")
	if !strings.Contains(archive.Header().Get("Content-Security-Policy"), "script-src 'none'") || strings.Contains(archive.Body.String(), "<script") {
		t.Fatal("legacy event archive security changed")
	}
	// The second page starts after the cursor and links back to the first.
	second := get(t, s, "/sessions/s1?after=24&reader=1").Body.String()
	if !strings.Contains(second, `id="t25"`) || strings.Contains(second, `id="t24"`) {
		t.Error("the second page did not start after the cursor")
	}
	if !strings.Contains(second, `href="/sessions/s1"`) {
		t.Error("no link back to the first page")
	}
	if f.lastTurns.After == nil || *f.lastTurns.After != 24 {
		t.Errorf("cursor did not reach the data layer: %v", f.lastTurns.After)
	}
}

// manyTurns fabricates n answered main-thread turns around one prompt event,
// ten minutes apart, for paging tests.
func manyTurns(prompt event.Event, n int) []Turn {
	var out []Turn
	for i := range n {
		p := prompt
		p.ID = "p" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		p.OccurredAt = prompt.OccurredAt.Add(time.Duration(i) * 10 * time.Minute)
		out = append(out, Turn{
			Index: i, Kind: "human", Outcome: "no_work", PromptEventID: p.ID, StartedAt: p.OccurredAt,
			LastActivityAt: p.OccurredAt,
			Events:         []TurnEvent{{Event: p, Role: "prompt", Kind: "human"}},
		})
	}
	return out
}

func TestContinuousRouteMatcherDoesNotRelaxOtherPages(t *testing.T) {
	for _, path := range []string{"/sessions//conversation", "/sessions/s1/events/conversation", "/sessions/s1/conversation/other", "/artifacts/conversation"} {
		if continuousPath(path) {
			t.Fatal(path)
		}
	}
}

func TestAssistantRepliesRemainExpanded(t *testing.T) {
	b := Block{Kind: KindAssistant, Text: Plain(strings.Repeat("Final answer. ", 100))}
	if b.LongMessage() {
		t.Fatal("assistant answer would be collapsed")
	}
	body := renderConversation(t, "message-body", b)
	if strings.Contains(body, "message-expand") || !strings.Contains(body, "Final answer.") {
		t.Fatal("answer is not directly visible")
	}
}

func TestContinuousInspectorRetainsReaderRoute(t *testing.T) {
	v := detailView{Page: Page{Continuous: true}, Session: SessionDetail{Session: Session{ID: "s1"}}}
	if got := v.FileURL(7, ""); got != "/sessions/s1/conversation?file=7#inspector-h" {
		t.Fatal(got)
	}
}
