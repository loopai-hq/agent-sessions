package backfill

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// Capture version 4: anchors, provable lineage only, the legacy layout, slash
// commands, per-file windows and caps. Each test here is a guard the design
// names; the fixtures are the shapes the reference corpus actually contains.

const (
	sessP = "10db14c9-0000-1111-2222-333333333333" // a fork's origin
	sessC = "567f73dd-0000-1111-2222-333333333333" // its child
)

// promptLine is a user record carrying a promptId and, optionally, isMeta.
func promptLine(session, ts, uuid, promptID, text string, meta bool) string {
	return fmt.Sprintf(
		`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":%q,"promptId":%q,"isMeta":%t,"cwd":"/repo","version":"2.1.267","message":{"role":"user","content":%q}}`,
		session, ts, uuid, promptID, meta, text)
}

func byType(evs []event.Event, typ event.Type) []event.Event {
	var out []event.Event
	for _, e := range evs {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

// Every event names the record it came from, the turn it belongs to, and the
// tool use it is about: the keys a hook copy of the same moment also carries.
func TestEventsCarryTheAnchors(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		promptLine(sessA, ts1, "u1", "prompt-1", "do it", false),
		fmt.Sprintf(`{"type":"assistant","sessionId":%q,"timestamp":%q,"uuid":"a1","promptId":"prompt-1","requestId":"req_1","message":{"role":"assistant","model":"claude-fable-5","id":"msg_1","usage":{"input_tokens":1,"output_tokens":2},"content":[{"type":"text","text":"on it"},{"type":"tool_use","id":"toolu_1","name":"Read","input":{}}]}}`, sessA, ts2),
		toolResultLine(sessA, ts3, "u2", "toolu_1", "the file", false),
	)
	got, _ := collectEvents(t, Options{Root: tr.root})

	want := map[event.Type]struct{ uuid, prompt, tool string }{
		event.SessionStarted: {"u1", "prompt-1", ""},
		event.UserPrompt:     {"u1", "prompt-1", ""},
		event.AssistantTurn:  {"a1", "prompt-1", ""},
		event.ToolCall:       {"a1", "prompt-1", "toolu_1"},
		event.ToolResult:     {"u2", "", "toolu_1"},
	}
	for typ, w := range want {
		evs := byType(got, typ)
		if len(evs) != 1 {
			t.Fatalf("%s: %d events, want 1", typ, len(evs))
		}
		e := evs[0]
		if e.RecordUUID != w.uuid || e.PromptID != w.prompt || e.ToolUseID != w.tool {
			t.Errorf("%s: uuid=%q prompt=%q tool=%q, want %q %q %q", typ, e.RecordUUID, e.PromptID, e.ToolUseID, w.uuid, w.prompt, w.tool)
		}
	}
	if u := byType(got, event.AssistantTurn)[0].Usage; u == nil || u.MessageID != "msg_1" || u.RequestID != "req_1" {
		t.Errorf("usage = %+v; want the record's message id msg_1 and request id req_1", u)
	}
}

// A compacted transcript: the boundary record carries logicalParentUuid and
// the summary follows it. Nothing in it is lineage.
func TestCompactedFixtureHasNoParentSessionAndKeepsTheMarker(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		promptLine(sessA, ts1, "u1", "p1", "first", false),
		fmt.Sprintf(`{"parentUuid":null,"logicalParentUuid":"9f90859f-tail","isSidechain":false,"type":"system","subtype":"compact_boundary","content":"Conversation compacted","level":"info","uuid":"b1","timestamp":%q,"sessionId":%q,"version":"2.1.236"}`, ts2, sessA),
		fmt.Sprintf(`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":"c1","isCompactSummary":true,"isVisibleInTranscriptOnly":true,"message":{"role":"user","content":"This session is being continued from a previous conversation that ran out of context. The summary below covers the earlier portion."}}`, sessA, ts3),
		promptLine(sessA, ts4, "u2", "p2", "after compaction", false),
	)
	got, _ := collectEvents(t, Options{Root: tr.root})

	for _, e := range got {
		if e.ParentSessionID != "" || e.LineageSource != "" {
			t.Errorf("%s carries lineage %q/%q from a compaction", e.Type, e.ParentSessionID, e.LineageSource)
		}
	}
	for _, e := range got {
		if e.Text == "after compaction" && e.ParentRecordUUID != "9f90859f-tail" {
			t.Errorf("the record after the boundary does not carry the marker: %q", e.ParentRecordUUID)
		}
		if e.Text == "first" && e.ParentRecordUUID != "" {
			t.Error("the record before the boundary carries the marker")
		}
	}
	if n := len(byType(got, event.Compaction)); n != 1 {
		t.Errorf("%d compaction events, want 1", n)
	}
	if len(byType(got, event.UserPrompt)) != 2 {
		t.Errorf("the summary was emitted as a prompt: %v", byType(got, event.UserPrompt))
	}
}

// A forked copy of a compaction summary was observed with the flag missing
// and origin:human. The opening words are the guard of last resort.
func TestCompactionSummaryIsRecognisedByItsTextWithoutTheFlag(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		fmt.Sprintf(`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":"c1","origin":{"kind":"human"},"promptSource":"typed","message":{"role":"user","content":"This session is being continued from a previous conversation that ran out of context.\nSummary: ..."}}`, sessA, ts1),
		promptLine(sessA, ts2, "u1", "p1", "real prompt", false),
		toolResultLine(sessA, ts3, "u2", "toolu_1", "This session is being continued from a previous conversation is a phrase in a file", false),
	)
	got, _ := collectEvents(t, Options{Root: tr.root})
	if n := len(byType(got, event.Compaction)); n != 1 {
		t.Fatalf("%d compaction events, want 1 (the tool result quoting the phrase is not one)", n)
	}
	if p := byType(got, event.UserPrompt); len(p) != 1 || p[0].Text != "real prompt" {
		t.Errorf("prompts = %v, want only the real one", p)
	}
}

// Claude Code 2.0.x wrote subagent files at the project root as
// agent-<id>.jsonl, every record isSidechain:true with the parent's sessionId.
// They are their own streams: appended to the parent they produced a spurious
// session_started, a "Warmup" prompt and renumbered everything after them.
func TestLegacyRootAgentFileIsASubagentStream(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl", promptLine(sessA, ts1, "u1", "p1", "the real prompt", false))
	tr.write("proj/agent-a1b2c3d.jsonl",
		fmt.Sprintf(`{"parentUuid":null,"isSidechain":true,"agentId":"a1b2c3d","type":"user","sessionId":%q,"timestamp":%q,"uuid":"w1","version":"2.0.76","message":{"role":"user","content":"Warmup"}}`, sessA, ts2),
		fmt.Sprintf(`{"isSidechain":true,"agentId":"a1b2c3d","type":"assistant","sessionId":%q,"timestamp":%q,"uuid":"w2","version":"2.0.76","message":{"role":"assistant","model":"claude-fable-5","content":[{"type":"text","text":"warm"}]}}`, sessA, ts3),
	)
	got, res := collectEvents(t, Options{Root: tr.root})

	var mainStarts, agentStarts int
	for _, e := range got {
		switch {
		case e.AgentID == "" && e.Type == event.SessionStarted:
			mainStarts++
		case e.AgentID == "agent-a1b2c3d" && e.Type == event.SubagentStart:
			agentStarts++
		}
		if e.Text == "Warmup" && e.AgentID != "agent-a1b2c3d" {
			t.Errorf("the Warmup prompt landed in the main stream: %+v", e)
		}
		if e.AgentID == "" && e.Text == "warm" {
			t.Error("a sidechain assistant turn landed in the main stream")
		}
	}
	if mainStarts != 1 || agentStarts != 1 {
		t.Errorf("main starts=%d agent starts=%d, want 1 and 1", mainStarts, agentStarts)
	}
	if res.Sessions != 2 {
		t.Errorf("Result.Sessions = %d, want the main stream and the agent stream", res.Sessions)
	}
	if got := classify(filepath.FromSlash("/r/proj/agent-a1b2c3d.jsonl")); got.kind != kindAgent || got.agentID != "agent-a1b2c3d" || got.sessionDir != "" {
		t.Errorf("classify(legacy) = %+v", got)
	}
}

// Belt and braces: a sidechain record that turns up mid-way through a main
// transcript is skipped with an advisory rather than attributed to the person.
func TestSidechainRecordInsideAMainTranscriptIsSkipped(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		promptLine(sessA, ts1, "u1", "p1", "real", false),
		fmt.Sprintf(`{"isSidechain":true,"agentId":"zzz","type":"assistant","sessionId":%q,"timestamp":%q,"uuid":"s1","message":{"role":"assistant","model":"claude-fable-5","content":[{"type":"text","text":"stray"}]}}`, sessA, ts2),
	)
	got, res := collectEvents(t, Options{Root: tr.root})
	for _, e := range got {
		if e.Text == "stray" {
			t.Fatal("a stray sidechain record was emitted into the main stream")
		}
	}
	if !hasSkip(res, sessA+".jsonl", "isSidechain") {
		t.Errorf("no advisory for the stray record:%s", skipReasons(res))
	}
}

// A slash command arrives wrapped. The person's words are in <command-args>;
// the first human text of the session is those words, not the wrapper.
func TestSlashCommandFixtureFirstHumanTextIsTheArgs(t *testing.T) {
	skill := "<command-message>platform-engineer:platform-engineer</command-message>\n<command-name>/platform-engineer:platform-engineer</command-name>\n<command-args>Git fetch latest main and rebase</command-args>"
	local := "<command-name>/effort</command-name>\n            <command-message>effort</command-message>\n            <command-args></command-args>"
	caveat := "<local-command-caveat>Caveat: The messages below were generated by the user while running local commands. DO NOT respond to these messages.</local-command-caveat>"

	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		promptLine(sessA, ts1, "u1", "p1", skill, false),
		fmt.Sprintf(`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":"u2","promptId":"p1","isMeta":true,"turnCompanion":true,"message":{"role":"user","content":"<command-message>platform-engineer</command-message>\n<skill-format>true</skill-format>\nExpanded skill body..."}}`, sessA, ts2),
	)
	tr.write("proj/"+sessB+".jsonl",
		promptLine(sessB, ts1, "u1", "p9", caveat, true),
		promptLine(sessB, ts2, "u2", "p9", local, false),
	)
	got, _ := collectEvents(t, Options{Root: tr.root})

	firstHuman := func(session string) string {
		for _, e := range got {
			if e.SessionID == session && e.Type == event.UserPrompt && !strings.HasPrefix(e.Text, "<local-command-caveat>") {
				return e.Text
			}
		}
		return ""
	}
	if got := firstHuman(sessA); got != "Git fetch latest main and rebase" {
		t.Errorf("first human text = %q, want the command args", got)
	}
	if got := firstHuman(sessB); got != "/effort" {
		t.Errorf("first human text for an argless command = %q, want /effort", got)
	}
	// The wrapper survives in Raw for the server's classifier.
	for _, e := range byType(got, event.UserPrompt) {
		if e.SessionID == sessA && e.Text == "Git fetch latest main and rebase" && !strings.Contains(string(e.Raw), "<command-name>") {
			t.Error("the wrapper was stripped from Raw")
		}
	}
	// The rollup's first prompt is the words, not the wrapper.
	var s event.Session
	for _, e := range got {
		if e.SessionID == sessA {
			s.Apply(e)
		}
	}
	if s.FirstPrompt != "Git fetch latest main and rebase" {
		t.Errorf("rollup first prompt = %q", s.FirstPrompt)
	}
}

// A fork: a new file that opens with the origin's records, uuids and
// timestamps intact, sessionId and promptId rewritten. The child's
// session_started names the parent and the seq of the last inherited event;
// the inherited events are still emitted.
func TestForkChildCarriesLineageAndItsPrefix(t *testing.T) {
	tr := newTree(t)
	parentFile := tr.write("proj/"+sessP+".jsonl",
		`{"type":"ai-title","aiTitle":"Multi harness task allocation","sessionId":"`+sessP+`"}`,
		promptLine(sessP, ts1, "13d18a2a", "9bacf634", "allocate the tasks", false),
		assistantLine(sessP, ts2, "4b170985", "claude-fable-5", "here is the plan"),
		promptLine(sessP, ts3, "40de963f", "0c1d2e3f", "go on", false),
	)
	// The copy is born after the origin. Its prefix carries the promptId of
	// the copy's own first turn, which is what the corpus pair shows.
	time.Sleep(20 * time.Millisecond)
	tr.write("proj/"+sessC+".jsonl",
		`{"type":"ai-title","aiTitle":"Multi harness task allocation","sessionId":"`+sessC+`"}`,
		promptLine(sessC, ts1, "13d18a2a", "4e848d60", "allocate the tasks", false),
		assistantLine(sessC, ts2, "4b170985", "claude-fable-5", "here is the plan"),
		promptLine(sessC, ts4, "d3c7608a", "4e848d60", "the child's own prompt", false),
	)
	got, _ := collectEvents(t, Options{Root: tr.root})

	starts := map[string]event.Event{}
	counts := map[string]int{}
	for _, e := range got {
		counts[e.SessionID]++
		if e.Type == event.SessionStarted {
			starts[e.SessionID] = e
		}
	}
	child := starts[sessC]
	if child.ParentSessionID != sessP || child.LineageSource != LineageFork {
		t.Fatalf("child session_started lineage = %q/%q, want %s/%s", child.ParentSessionID, child.LineageSource, sessP, LineageFork)
	}
	// session_started is seq 0; the two copied records build a prompt (1)
	// and a turn (2); the child's own prompt is seq 3.
	if child.ForkPrefixSeq != 2 {
		t.Errorf("ForkPrefixSeq = %d, want 2", child.ForkPrefixSeq)
	}
	if child.HarnessTitle != "Multi harness task allocation" {
		t.Errorf("harness title = %q", child.HarnessTitle)
	}
	if p := starts[sessP]; p.ParentSessionID != "" || p.LineageSource != "" || p.ForkPrefixSeq != 0 {
		t.Errorf("the parent carries lineage: %+v", p)
	}
	// The copied prefix is STILL emitted under the child: resume needs it.
	if counts[sessC] != 4 {
		t.Errorf("child emitted %d events, want start + 2 inherited + 1 own", counts[sessC])
	}
	for _, e := range got {
		if e.SessionID == sessC && e.Type == event.UserPrompt && e.Text == "allocate the tasks" && e.RecordUUID != "13d18a2a" {
			t.Error("the inherited prompt lost the origin's uuid")
		}
	}
	// A walk of the same tree gives the same answer: the index is deterministic.
	again, _ := collectEvents(t, Options{Root: tr.root})
	if len(again) != len(got) {
		t.Fatalf("second walk emitted %d, want %d", len(again), len(got))
	}
	for i := range got {
		if got[i].ID != again[i].ID || got[i].ForkPrefixSeq != again[i].ForkPrefixSeq {
			t.Fatalf("walk is not deterministic at %d", i)
		}
	}
	_ = parentFile
}

// forkStarts walks a tree and returns each session's session_started plus
// the seq of the last event built from an inherited record of the child.
func forkStarts(t *testing.T, root string) (starts map[string]event.Event, lastInherited int64) {
	t.Helper()
	got, _ := collectEvents(t, Options{Root: root})
	starts = map[string]event.Event{}
	for _, e := range got {
		if e.Type == event.SessionStarted {
			starts[e.SessionID] = e
		}
		if e.SessionID == sessC && e.RecordUUID == "4b170985" {
			lastInherited = e.Seq
		}
	}
	return starts, lastInherited
}

// Every platform but darwin reports mtime as birth time, and an origin that
// is still being written to is then the younger file. The promptId decides:
// the copy's prefix carries the id of the copy's own first prompt, the
// origin's first-turn id never leaves the shared prefix.
func TestForkOriginIsDecidedByPromptIDNotByFileAge(t *testing.T) {
	saved := fileBirth
	fileBirth = func(fi os.FileInfo) time.Time { return fi.ModTime().UTC() }
	defer func() { fileBirth = saved }()

	tr := newTree(t)
	parent := tr.write("proj/"+sessP+".jsonl",
		promptLine(sessP, ts1, "13d18a2a", "9bacf634", "allocate the tasks", false),
		assistantLine(sessP, ts2, "4b170985", "claude-fable-5", "here is the plan"),
		promptLine(sessP, ts4, "7a7a7a7a", "0c1d2e3f", "and after the fork, this", false),
	)
	child := tr.write("proj/"+sessC+".jsonl",
		promptLine(sessC, ts1, "13d18a2a", "4e848d60", "allocate the tasks", false),
		assistantLine(sessC, ts2, "4b170985", "claude-fable-5", "here is the plan"),
		promptLine(sessC, ts3, "df0550ea", "4e848d60", "resume where we left off", false),
	)
	// The origin was appended to after the fork: by mtime it is the newer file.
	older := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(child, older, older); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(parent, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}

	starts, last := forkStarts(t, tr.root)
	if c := starts[sessC]; c.ParentSessionID != sessP || c.LineageSource != LineageFork || c.ForkPrefixSeq != last {
		t.Fatalf("child lineage = %q/%q/%d, want %s/%s/%d", c.ParentSessionID, c.LineageSource, c.ForkPrefixSeq, sessP, LineageFork, last)
	}
	if p := starts[sessP]; p.ParentSessionID != "" || p.LineageSource != "" {
		t.Fatalf("the origin was taken for the copy because its mtime is newer: %+v", p)
	}
}

// toolResultPromptLine is a tool_result user record that also carries the
// promptId of the turn it belongs to: the shape r4 Q5 leaves open.
func toolResultPromptLine(session, ts, uuid, toolID, promptID, out string) string {
	return fmt.Sprintf(
		`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":%q,"promptId":%q,"toolUseResult":{"stdout":%q},"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":%q,"content":%q}]}}`,
		session, ts, uuid, promptID, out, toolID, out)
}

// A fork taken while the origin was still inside its first turn. The origin
// goes on to record that turn's tool result after the copy point, and if the
// harness stamps it with the turn's promptId it sits outside the shared
// prefix carrying the origin's first id. A tool result cannot open a turn, so
// it is not evidence of a rewrite; counted as one, both files look rewritten
// and the mtime tie-break names the copy as the origin.
func TestForkMidTurnOriginIsStillTheOrigin(t *testing.T) {
	saved := fileBirth
	fileBirth = func(fi os.FileInfo) time.Time { return fi.ModTime().UTC() }
	defer func() { fileBirth = saved }()

	tr := newTree(t)
	parent := tr.write("proj/"+sessP+".jsonl",
		promptLine(sessP, ts1, "13d18a2a", "9bacf634", "allocate the tasks", false),
		toolUseLine(sessP, ts2, "4b170985", "toolu_1", "Read"),
		// Written after the copy was taken, still inside the first turn.
		toolResultPromptLine(sessP, ts3, "5c5c5c5c", "toolu_1", "9bacf634", "the file"),
		assistantLine(sessP, ts3, "6d6d6d6d", "claude-fable-5", "here is the plan"),
		promptLine(sessP, ts4, "7a7a7a7a", "0c1d2e3f", "and after the fork, this", false),
	)
	child := tr.write("proj/"+sessC+".jsonl",
		promptLine(sessC, ts1, "13d18a2a", "4e848d60", "allocate the tasks", false),
		toolUseLine(sessC, ts2, "4b170985", "toolu_1", "Read"),
		promptLine(sessC, ts3, "df0550ea", "4e848d60", "resume where we left off", false),
	)
	// The origin was appended to after the fork: by mtime it is the newer file.
	older := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(child, older, older); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(parent, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}

	starts, last := forkStarts(t, tr.root)
	if c := starts[sessC]; c.ParentSessionID != sessP || c.LineageSource != LineageFork || c.ForkPrefixSeq != last {
		t.Fatalf("child lineage = %q/%q/%d, want %s/%s/%d", c.ParentSessionID, c.LineageSource, c.ForkPrefixSeq, sessP, LineageFork, last)
	}
	if p := starts[sessP]; p.ParentSessionID != "" || p.LineageSource != "" {
		t.Fatalf("the origin's own tool result was taken for a rewrite: %+v", p)
	}
}

// A harness that predates promptId leaves nothing for the rule to read; the
// file that existed first is the origin, as before.
func TestForkWithoutPromptIDsFallsBackToBirthTime(t *testing.T) {
	saved := fileBirth
	fileBirth = func(fi os.FileInfo) time.Time { return fi.ModTime().UTC() }
	defer func() { fileBirth = saved }()

	tr := newTree(t)
	parent := tr.write("proj/"+sessP+".jsonl",
		promptLine(sessP, ts1, "13d18a2a", "", "allocate the tasks", false),
		assistantLine(sessP, ts2, "4b170985", "claude-fable-5", "here is the plan"),
	)
	child := tr.write("proj/"+sessC+".jsonl",
		promptLine(sessC, ts1, "13d18a2a", "", "allocate the tasks", false),
		assistantLine(sessC, ts2, "4b170985", "claude-fable-5", "here is the plan"),
		promptLine(sessC, ts3, "df0550ea", "", "resume where we left off", false),
	)
	older := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(parent, older, older); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(child, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	starts, _ := forkStarts(t, tr.root)
	if c := starts[sessC]; c.ParentSessionID != sessP {
		t.Fatalf("child parent = %q, want the older file %s", c.ParentSessionID, sessP)
	}
}

// ForkPrefixSeq is the seq of the last inherited event the walk emits. A
// prefix that opens with a record the walk drops (no timestamp and no clock
// to carry yet) must not count it, or the seam lands one event late.
func TestForkPrefixSeqSkipsWhatTheWalkSkips(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessP+".jsonl",
		promptLine(sessP, "", "00000001", "9bacf634", "a record with no clock", false),
		promptLine(sessP, ts1, "13d18a2a", "9bacf634", "allocate the tasks", false),
		assistantLine(sessP, ts2, "4b170985", "claude-fable-5", "here is the plan"),
	)
	time.Sleep(20 * time.Millisecond)
	tr.write("proj/"+sessC+".jsonl",
		promptLine(sessC, "", "00000001", "4e848d60", "a record with no clock", false),
		promptLine(sessC, ts1, "13d18a2a", "4e848d60", "allocate the tasks", false),
		assistantLine(sessC, ts2, "4b170985", "claude-fable-5", "here is the plan"),
		promptLine(sessC, ts3, "df0550ea", "4e848d60", "resume where we left off", false),
	)
	starts, last := forkStarts(t, tr.root)
	if last == 0 {
		t.Fatal("the inherited turn was not emitted under the child")
	}
	if c := starts[sessC]; c.ParentSessionID != sessP || c.ForkPrefixSeq != last {
		t.Fatalf("child lineage = %q ForkPrefixSeq %d, want %s and %d (the last inherited event's seq)", c.ParentSessionID, c.ForkPrefixSeq, sessP, last)
	}
}

// Done wins over the window, per file. A file first imported under a narrow
// window went out whole and was recorded; the wider walk that follows must
// emit nothing for it, while a file the journal does not know is emitted whole.
func TestDoneWinsOverTheWindowPerFile(t *testing.T) {
	tr := newTree(t)
	sent := tr.write("proj/"+sessA+".jsonl",
		userLine(sessA, ts1, "u1", "old head"),
		userLine(sessA, ts4, "u2", "new tail"),
	)
	tr.write("proj/"+sessB+".jsonl", userLine(sessB, ts2, "u1", "unknown to the journal"))

	got, res := collectEvents(t, Options{
		Root:  tr.root,
		Since: mustTime(t, ts4),
		Done:  func(p string) bool { return p == sent },
	})
	for _, e := range got {
		if e.SessionID == sessA {
			t.Errorf("a file the journal recorded was emitted again: %q", e.Text)
		}
	}
	if len(got) != 0 {
		t.Errorf("a file older than the window was emitted: %d events", len(got))
	}
	if res.Resumed != 1 {
		t.Errorf("Resumed = %d, want 1", res.Resumed)
	}

	// The same walk with no window emits the unknown file whole.
	got, _ = collectEvents(t, Options{Root: tr.root, Done: func(p string) bool { return p == sent }})
	if n := len(byType(got, event.UserPrompt)); n != 1 || got[0].SessionID != sessB {
		t.Errorf("the unknown file was not emitted whole: %d prompts", n)
	}
}

// Session walks one session's files whole, ignoring Done and the window; the
// other sessions are not walked at all.
func TestSessionOptionWalksOneSessionWhole(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl", userLine(sessA, ts1, "u1", "other"))
	tr.write("proj/"+sessB+".jsonl", userLine(sessB, ts1, "u1", "wanted, and old"))
	tr.write("proj/"+sessB+"/subagents/agent-w1.jsonl", userLine(sessB, ts2, "s1", "its subagent"))

	full, _ := collectEvents(t, Options{Root: tr.root})
	got, res := collectEvents(t, Options{
		Root: tr.root, Session: sessB,
		Since: mustTime(t, ts4),
		Done:  func(string) bool { return true },
	})
	if len(got) == 0 {
		t.Fatal("the named session was not emitted")
	}
	for _, e := range got {
		if e.SessionID != sessB {
			t.Errorf("a session other than the named one was emitted: %s", e.SessionID)
		}
	}
	if res.Resumed != 0 {
		t.Errorf("Resumed = %d; a named session ignores the journal", res.Resumed)
	}
	// Under the same ids a full walk gives: numbering is per stream.
	ids := map[string]bool{}
	for _, e := range full {
		ids[e.ID] = true
	}
	for _, e := range got {
		if !ids[e.ID] {
			t.Errorf("%s emitted under an id the full walk never produced", e.Type)
		}
	}
	if len(byType(got, event.SubagentStart)) != 1 {
		t.Error("the session's subagent stream was not walked")
	}
}

// Only decides per record; numbering is unaffected, so what it accepts carries
// the ids a full walk gives.
func TestOnlyEmitsAcceptedRecordsUnderFullWalkIDs(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		promptLine(sessA, ts1, "u1", "p1", "captured by the hook", false),
		assistantLine(sessA, ts2, "a1", "claude-fable-5", "lost by the hook"),
		toolUseLine(sessA, ts3, "a2", "toolu_1", "Read"),
	)
	full, _ := collectEvents(t, Options{Root: tr.root})
	got, _ := collectEvents(t, Options{Root: tr.root, Only: func(a Anchor) bool { return a.UUID == "a1" }})

	if len(got) != 1 || got[0].Type != event.AssistantTurn || got[0].RecordUUID != "a1" {
		t.Fatalf("Only emitted %v, want exactly the a1 turn", got)
	}
	for _, e := range full {
		if e.RecordUUID == "a1" && e.Type == event.AssistantTurn && (e.ID != got[0].ID || e.Seq != got[0].Seq) {
			t.Errorf("the accepted record was renumbered: full %d/%s, only %d/%s", e.Seq, e.ID, got[0].Seq, got[0].ID)
		}
	}
	anchors, err := Anchors(filepath.Join(tr.root, "proj", sessA+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(anchors) != 3 || anchors[0].PromptID != "p1" || anchors[2].ToolUseIDs[0] != "toolu_1" {
		t.Errorf("anchors = %+v", anchors)
	}
}

// The ten record types the harness grew since the first census produce no
// events and no advisories.
func TestNewerBookkeepingTypesAreIgnoredByName(t *testing.T) {
	types := []string{"atis-latch", "pr-link", "worktree-state", "relocated", "frame-link", "history-suppression",
		"artifact-autoreact-ledger", "custom-title", "artifact-comment-monitor", "cost-state", "progress"}
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			tr := newTree(t)
			tr.write("proj/"+sessA+".jsonl",
				fmt.Sprintf(`{"type":%q,"sessionId":%q,"prNumber":1,"atis":"abcd"}`, typ, sessA),
				userLine(sessA, ts1, "u1", "real"),
			)
			got, res := collectEvents(t, Options{Root: tr.root})
			if len(got) != 2 {
				t.Errorf("%d events, want session_started and the prompt", len(got))
			}
			for _, s := range res.Skipped {
				if strings.Contains(s.Reason, "no timestamp") || strings.Contains(s.Reason, "no sessionId") {
					t.Errorf("bookkeeping reported as an anomaly: %s", s.Reason)
				}
			}
		})
	}
}

// The walker caps tool output at the same byte the live path does.
func TestWalkerCapsToolOutput(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		toolResultLine(sessA, ts1, "u1", "toolu_big", strings.Repeat("z", MaxToolOutputBytes+100), false),
	)
	got, _ := collectEvents(t, Options{Root: tr.root})
	rs := byType(got, event.ToolResult)
	if len(rs) != 1 || len(rs[0].Tool.Output) != MaxToolOutputBytes || !rs[0].Tool.Truncated {
		t.Fatalf("output len=%d truncated=%v", len(rs[0].Tool.Output), rs[0].Tool.Truncated)
	}
}

// The harness's own title rides the stream's session_started.
func TestHarnessTitleRidesSessionStarted(t *testing.T) {
	tr := newTree(t)
	tr.write("proj/"+sessA+".jsonl",
		`{"type":"last-prompt","sessionId":"`+sessA+`","lastPrompt":"x"}`,
		`{"type":"ai-title","aiTitle":"Create gantry-fixture.txt","sessionId":"`+sessA+`"}`,
		userLine(sessA, ts1, "u1", "create the fixture"),
	)
	got, _ := collectEvents(t, Options{Root: tr.root})
	s := byType(got, event.SessionStarted)
	if len(s) != 1 || s[0].HarnessTitle != "Create gantry-fixture.txt" {
		t.Fatalf("session_started = %+v", s)
	}
	if byType(got, event.UserPrompt)[0].HarnessTitle != "" {
		t.Error("the title was stamped on a prompt; it belongs on session_started alone")
	}
}

// ReadHead and LastTimestamp are the bounded reads the hooks and the window
// rule depend on.
func TestHeadAndTailReads(t *testing.T) {
	tr := newTree(t)
	p := tr.write("t.jsonl",
		`{"type":"queue-operation","operation":"enqueue"}`,
		`{"type":"ai-title","aiTitle":"Title","sessionId":"`+sessA+`"}`,
		fmt.Sprintf(`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":"u1","promptId":"p1","entrypoint":"sdk-cli","isSidechain":false,"message":{"role":"user","content":"hi"}}`, sessA, ts1),
		userLine(sessA, ts4, "u2", "later"),
	)
	h, ok := ReadHead(p)
	if !ok || h.SessionID != sessA || h.AITitle != "Title" || h.Entrypoint != "sdk-cli" || h.FirstUUID != "u1" || h.FirstPromptID != "p1" || !h.FirstAt.Equal(mustTime(t, ts1)) {
		t.Fatalf("head = %+v", h)
	}
	if last := LastTimestamp(p); !last.Equal(mustTime(t, ts4)) {
		t.Errorf("last timestamp = %s, want %s", last, ts4)
	}
	if _, ok := ReadHead(filepath.Join(tr.root, "absent.jsonl")); ok {
		t.Error("a missing file read as present")
	}
}
