package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/normalize"
)

// The classifier reads named top-level fields, never a substring. The trap it
// must survive is real and observed: a person greps a cron log, the grep's
// output lands inside their own transcript record, and that record now contains
// the literal text "entrypoint":"sdk-cli" inside a string. Substring matching
// would call the investigator a robot.
func TestAutomationEvidenceReadsFieldsNotSubstrings(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    bool
		decided bool
	}{
		{"headless entrypoint", `{"type":"user","entrypoint":"sdk-cli","cwd":"/"}`, true, true},
		{"interactive cli", `{"type":"user","entrypoint":"cli"}`, false, true},
		{"desktop app", `{"entrypoint":"claude-desktop"}`, false, true},
		{"no entrypoint at all", `{"type":"queue-operation"}`, false, false},
		{"empty raw", ``, false, false},
		{"not json", `not a record`, false, false},
		// The trap: the marker verbatim inside a string value. The top-level
		// entrypoint on this record says cli, and cli must win.
		{"quoted transcript in tool output",
			`{"entrypoint":"cli","toolUseResult":"{\"entrypoint\":\"sdk-cli\"}"}`, false, true},
		{"quoted transcript with no own entrypoint",
			`{"type":"tool_result","output":"line was {\"entrypoint\":\"sdk-cli\"}"}`, false, false},
		{"codex exec originator", `{"type":"session_meta","payload":{"originator":"codex_exec"}}`, true, true},
		{"codex interactive originator", `{"payload":{"originator":"codex_cli_rs"}}`, false, true},
		{"originator quoted in a string",
			`{"payload":{"notes":"saw {\"originator\":\"codex_exec\"} in a log"}}`, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, decided := automationEvidenceRaw([]byte(tc.raw))
			if got != tc.want || decided != tc.decided {
				t.Errorf("automationEvidenceRaw(%s) = (%v, %v), want (%v, %v)", tc.raw, got, decided, tc.want, tc.decided)
			}
		})
	}
}

// The named field decides without a parse and wins over Raw; a stamped
// interactive entrypoint must not fall through to a Raw that would say
// otherwise, because the field is the walker's own extraction of that Raw.
func TestAutomationSessionsPrefersTheEntrypointField(t *testing.T) {
	mk := func(id, ep, raw string) Ingest {
		return Ingest{
			Email: "dev@example.com",
			Event: event.Event{
				ID: id, SessionID: "s-f", Source: event.SourceClaudeCode,
				Type: event.UserPrompt, Entrypoint: ep, Raw: json.RawMessage(raw),
			},
		}
	}
	if !automationSessions([]Ingest{mk("e1", "sdk-cli", "")})["s-f"] {
		t.Error("a stamped sdk-cli entrypoint did not mark the session")
	}
	if automationSessions([]Ingest{mk("e1", "cli", `{"entrypoint":"sdk-cli"}`)})["s-f"] {
		t.Error("a stamped interactive entrypoint fell through to the raw parse")
	}
}

// One machine-launched event marks the whole session, and silence in later
// events does not unmark it: batches arrive in any order, and the record that
// carries the entrypoint may be in any of them.
func TestAutomationSessionsDerivesFromRaw(t *testing.T) {
	mk := func(id, raw string) Ingest {
		return Ingest{
			Email: "dev@example.com",
			Event: event.Event{
				ID: id, SessionID: "s-1", Source: event.SourceClaudeCode,
				Type: event.UserPrompt, Raw: json.RawMessage(raw),
			},
		}
	}
	if !automationSessions([]Ingest{
		mk("e1", `{"entrypoint":"sdk-cli"}`),
		mk("e2", `{}`),
	})["s-1"] {
		t.Error("an sdk-cli event did not mark the session automation")
	}
	if automationSessions([]Ingest{
		mk("e1", `{"entrypoint":"cli"}`),
		mk("e2", `{}`),
	})["s-1"] {
		t.Error("an interactive session was marked automation")
	}
}

// The URL is attacker-typed; anything not on the list vanishes before SQL.
// The shape of what is left is the predicate's switch: nil when nothing was
// named (the store default), a non-nil slice when something was, even when
// none of it was known, so that selection matches nothing rather than
// widening to the default behind the caller's back.
func TestValidSessionTypes(t *testing.T) {
	got := validSessionTypes([]string{"automation", "user", "robot'; DROP TABLE sessions;--", "automation"})
	if len(got) != 2 || got[0] != "user" || got[1] != "automation" {
		t.Errorf("validSessionTypes = %v, want [user automation] in canonical order", got)
	}
	if out := validSessionTypes(nil); out != nil {
		t.Errorf("nothing named produced %#v, want nil (a NULL array, the store default)", out)
	}
	if out := validSessionTypes([]string{}); out != nil {
		t.Errorf("an empty request list produced %#v, want nil (nothing was named)", out)
	}
	if out := validSessionTypes([]string{"robot"}); out == nil || len(out) != 0 {
		t.Errorf("an unknown-only selection produced %#v, want a non-nil empty slice (an empty array, matching nothing)", out)
	}
	if got := validSessionTypes([]string{"empty", "internal"}); len(got) != 2 || got[0] != "internal" || got[1] != "empty" {
		t.Errorf("the hidden classes are not selectable by name: %v", got)
	}
}

// An increment is evidence for exactly one type: automation absorbs, content
// proves a session, and a lifecycle-only increment is empty until something
// arrives. The merge function decides the rest.
func TestSessionTypeOfEvidence(t *testing.T) {
	for _, tc := range []struct {
		automation bool
		content    int
		want       string
	}{
		{true, 0, "automation"}, {true, 3, "automation"}, {false, 1, "user"}, {false, 0, "empty"},
	} {
		if got := sessionTypeOf(tc.automation, tc.content); got != tc.want {
			t.Errorf("sessionTypeOf(%v, %d) = %q, want %q", tc.automation, tc.content, got, tc.want)
		}
	}
}

// A parent without a source is the compaction marker the old walker stamps on
// every event after a compaction; it goes to parent_record_uuid, and
// parent_session_id is written only together with the source that proved it.
func TestFoldDeltasGatesLineageOnASource(t *testing.T) {
	old := ingestOf("e1", "sess", "me@example.com", event.UserPrompt, 1)
	old.Event.ParentSessionID = "9f90859f-1111-2222-3333-444444444444"
	d := foldDeltas([]Ingest{old}, nil)["sess"]
	if d.ParentSessionID != "" || d.LineageSource != "" {
		t.Errorf("an old-shape parent reached the lineage columns: %+v", d)
	}
	if d.ParentRecordUUID != old.Event.ParentSessionID {
		t.Errorf("the marker was dropped rather than filed as a record uuid: %q", d.ParentRecordUUID)
	}

	proven := ingestOf("e2", "sess-2", "me@example.com", event.SessionStarted, 0)
	proven.Event.ParentSessionID = "origin-session"
	proven.Event.LineageSource = "fork_uuid"
	proven.Event.ParentRecordUUID = "a-record"
	d = foldDeltas([]Ingest{proven}, nil)["sess-2"]
	if d.ParentSessionID != "origin-session" || d.LineageSource != "fork_uuid" {
		t.Errorf("a proven parent was not carried: %+v", d)
	}
	if d.ParentRecordUUID != "a-record" {
		t.Errorf("the record uuid was not carried beside the proven parent: %+v", d)
	}

	// A source word the server does not know proves nothing: the parent is
	// filed as a record uuid exactly as one with no source is.
	guessed := ingestOf("e3", "sess-3", "me@example.com", event.SessionStarted, 0)
	guessed.Event.ParentSessionID = "origin-session"
	guessed.Event.LineageSource = "guess"
	d = foldDeltas([]Ingest{guessed}, nil)["sess-3"]
	if d.ParentSessionID != "" || d.LineageSource != "" {
		t.Errorf("an unknown source unlocked parent_session_id: %+v", d)
	}
	if d.ParentRecordUUID != "origin-session" {
		t.Errorf("the unproven parent was dropped rather than filed as a record uuid: %q", d.ParentRecordUUID)
	}
}

// Strings an event carries into a sessions column are bounded before they
// reach SQL: a client decides what it sends, not how wide a row can be. The
// cut is on a rune, never inside one, and the event's own launcher struct is
// left as delivered.
func TestFoldDeltasBoundsEventCarriedStrings(t *testing.T) {
	long := strings.Repeat("\u00e9", 600)
	start := ingestOf("a", "sess", "me@example.com", event.SessionStarted, 1)
	start.Event.Entrypoint = long
	start.Event.HarnessTitle = long
	start.Event.Launcher = &event.Launcher{Entrypoint: long, BundleID: long, Term: long, ParentComm: long}
	d := foldDeltas([]Ingest{start}, nil)["sess"]
	if n := len([]rune(d.HarnessTitle)); n != harnessTitleMax {
		t.Errorf("harness title kept %d runes, want %d", n, harnessTitleMax)
	}
	if n := len([]rune(d.Entrypoint)); n != entrypointMax {
		t.Errorf("entrypoint kept %d runes, want %d", n, entrypointMax)
	}
	for name, v := range map[string]string{"entrypoint": d.Launcher.Entrypoint, "bundle_id": d.Launcher.BundleID, "term": d.Launcher.Term, "parent_comm": d.Launcher.ParentComm} {
		if n := len([]rune(v)); n != launcherFieldMax {
			t.Errorf("launcher %s kept %d runes, want %d", name, n, launcherFieldMax)
		}
		if !utf8.ValidString(v) {
			t.Errorf("launcher %s was cut inside a rune", name)
		}
	}
	if len([]rune(start.Event.Launcher.Term)) != 600 {
		t.Error("the delivered event's launcher was rewritten in place")
	}
	if got := clipRunes("short", 64); got != "short" {
		t.Errorf("clipRunes shortened a string under the bound to %q", got)
	}
}

// Content is what a session did: prompts, turns with something in them,
// tools, file changes, subagents. Lifecycle markers count for nothing, and
// neither does a hook turn that captured neither text nor usage. Human turns
// are the main-thread prompts a person typed and no others.
func TestFoldDeltasCountsContentAndHumanTurns(t *testing.T) {
	mk := func(id string, typ event.Type, seq int64) Ingest {
		return ingestOf(id, "sess", "me@example.com", typ, seq)
	}
	start := mk("a", event.SessionStarted, 1)
	start.Event.Text = "startup"
	human := mk("b", event.UserPrompt, 2)
	human.Event.Text = "fix the flaky test"
	note := mk("c", event.UserPrompt, 3)
	note.Event.Text = "<task-notification>\n<task-id>x</task-id>\n<status>completed</status>\n<summary>done</summary>\n</task-notification>"
	agent := mk("d", event.UserPrompt, 1)
	agent.Event.AgentID = "agent-1"
	agent.Event.Origin = event.OriginTranscript
	agent.Event.Text = "Review the diff and report."
	command := mk("e", event.UserPrompt, 4)
	command.Event.Text = "/git --autonomous"
	blankTurn := mk("f", event.AssistantTurn, 5)
	fullTurn := mk("g", event.AssistantTurn, 6)
	fullTurn.Event.Text = "Done."
	tool := mk("h", event.ToolCall, 7)
	result := mk("i", event.ToolResult, 8)
	changed := mk("j", event.FileChanged, 9)
	sub := mk("k", event.SubagentStart, 10)
	subEnd := mk("l", event.SubagentEnd, 11)
	end := mk("m", event.SessionEnded, 12)
	end.Event.Text = "other"

	d := foldDeltas([]Ingest{start, human, note, agent, command, blankTurn, fullTurn, tool, result, changed, sub, subEnd, end}, nil)["sess"]
	if d.UserTurns != 4 {
		t.Errorf("user_turns = %d, want every user_prompt event (4)", d.UserTurns)
	}
	if d.HumanTurns != 2 {
		t.Errorf("human_turns = %d, want the typed prompt and the command only (2)", d.HumanTurns)
	}
	// 4 prompts + 1 turn with text + tool + result + file change + subagent start + end.
	if d.ContentEvents != 10 {
		t.Errorf("content_events = %d, want 10", d.ContentEvents)
	}
	if !d.Ended || d.EndReason != "other" {
		t.Errorf("end marker not folded: ended=%v reason=%q", d.Ended, d.EndReason)
	}
	lifecycle := foldDeltas([]Ingest{start, end}, nil)["sess"]
	if lifecycle.ContentEvents != 0 || lifecycle.HumanTurns != 0 {
		t.Errorf("a lifecycle-only increment counted content: %+v", lifecycle)
	}
}

// The launcher facts ride the lifecycle events and are set once; the client
// build comes from the request rather than the payload.
func TestFoldDeltasCarriesLauncherFacts(t *testing.T) {
	start := ingestOf("a", "sess", "me@example.com", event.SessionStarted, 1)
	start.Event.Entrypoint = "claude-desktop"
	start.Event.Launcher = &event.Launcher{Entrypoint: "cli", BundleID: "com.anthropic.claudefordesktop"}
	start.Event.HarnessTitle = "Fix the flaky test"
	start.AgentVersion = "23713ea"
	later := ingestOf("b", "sess", "me@example.com", event.UserPrompt, 2)
	later.Event.Entrypoint = "sdk-cli"
	later.AgentVersion = "580e794"
	end := ingestOf("c", "sess", "me@example.com", event.SessionEnded, 3)
	exists := false
	end.Event.TranscriptExists = &exists

	d := foldDeltas([]Ingest{start, later, end}, nil)["sess"]
	if d.Entrypoint != "claude-desktop" {
		t.Errorf("entrypoint = %q, want the first one seen", d.Entrypoint)
	}
	if d.Launcher == nil || d.Launcher.BundleID != "com.anthropic.claudefordesktop" {
		t.Errorf("launcher not carried: %+v", d.Launcher)
	}
	if d.HarnessTitle != "Fix the flaky test" {
		t.Errorf("harness title = %q", d.HarnessTitle)
	}
	if d.TranscriptExists == nil || *d.TranscriptExists {
		t.Errorf("transcript_exists not carried: %v", d.TranscriptExists)
	}
	if len(d.AgentVersions) != 2 || d.AgentVersions[0] != "23713ea" || d.AgentVersions[1] != "580e794" {
		t.Errorf("agent versions = %v, want both builds once each", d.AgentVersions)
	}
	if got, err := marshalLauncher(d.Launcher); err != nil || !strings.Contains(got.(string), `"bundle_id"`) {
		t.Errorf("launcher did not marshal for the jsonb column: %v %v", got, err)
	}
	if got, err := marshalLauncher(nil); err != nil || got != nil {
		t.Errorf("a missing launcher must reach SQL as NULL, got %v %v", got, err)
	}
}

// The title candidates are the earliest main-thread prompts of the batch,
// rendered: a command stored as its on-disk envelope becomes "/name args", a
// subagent's task never competes, and a wrapper is a candidate for the
// any-kind slot only.
func TestFoldDeltasPicksTheEarliestTitleCandidates(t *testing.T) {
	note := ingestOf("n", "sess", "me@example.com", event.UserPrompt, 1)
	note.Event.Text = "<local-command-caveat>Caveat: local commands.</local-command-caveat>"
	cmd := ingestOf("c", "sess", "me@example.com", event.UserPrompt, 2)
	cmd.Event.Origin = event.OriginTranscript
	cmd.Event.Text = "<command-message>review</command-message>\n<command-name>/review</command-name>\n<command-args>pr 12</command-args>"
	human := ingestOf("h", "sess", "me@example.com", event.UserPrompt, 3)
	human.Event.Text = "and then fix the bug\nsecond line"
	agent := ingestOf("a", "sess", "me@example.com", event.UserPrompt, 1)
	agent.Event.AgentID = "agent-1"
	agent.Event.Text = "Task for the subagent"

	d := foldDeltas([]Ingest{human, agent, cmd, note}, nil)["sess"]
	if d.AnyPrompt == nil || d.AnyPrompt.EventID != "n" {
		t.Fatalf("any-kind candidate = %+v, want the caveat at seq 1", d.AnyPrompt)
	}
	if d.Title == nil || d.Title.EventID != "c" || d.Title.Title != "/review pr 12" || !d.Title.Command {
		t.Fatalf("human candidate = %+v, want the command at seq 2 rendered as /review pr 12", d.Title)
	}
	if !d.SawUserPrompt {
		t.Error("a batch with prompts did not flag the refresh")
	}
}

// A hook payload is never handed to the normalizer as a transcript record: its
// isMeta would be read as the harness's word when it is the hook's own key,
// and a stamped prompt is not a transcript line.
func TestClassifyIngestPassesRawOnlyForTranscripts(t *testing.T) {
	hook := ingestOf("h", "sess", "me@example.com", event.UserPrompt, 1)
	hook.Event.Text = "hello"
	hook.Event.Raw = json.RawMessage(`{"isMeta":true}`)
	if m, _ := classifyIngest(hook); m.Kind != normalize.KindHuman {
		t.Errorf("a hook prompt's raw changed the verdict to %s", m.Kind)
	}
	transcript := hook
	transcript.Event.Origin = event.OriginTranscript
	if m, _ := classifyIngest(transcript); m.Kind != normalize.KindHarnessInjected {
		t.Errorf("a transcript record's isMeta was ignored: %s", m.Kind)
	}
	agent := ingestOf("a", "sess", "me@example.com", event.UserPrompt, 1)
	agent.Event.Origin = event.OriginTranscript
	agent.Event.AgentID = "agent-1"
	agent.Event.Text = "Investigate X"
	if m, _ := classifyIngest(agent); m.Kind != normalize.KindSubagentTask {
		t.Errorf("a subagent's driving prompt classified as %s", m.Kind)
	}
	agent.Event.Seq = 7
	if m, _ := classifyIngest(agent); m.Kind != normalize.KindHuman {
		t.Errorf("a later prompt in the agent stream classified as %s", m.Kind)
	}
	turn := ingestOf("t", "sess", "me@example.com", event.AssistantTurn, 2)
	turn.Event.Text = "Done."
	if m, _ := classifyIngest(turn); m.Kind != normalize.KindAssistantText {
		t.Errorf("assistant prose classified as %s", m.Kind)
	}
	if _, ok := classifyIngest(ingestOf("x", "sess", "me@example.com", event.SessionEnded, 3)); ok {
		t.Error("a lifecycle event has no place in the corpus")
	}
}

// The refresh reads only the two candidate rows by index and hands the
// template list across as data, so the decision made in SQL is the
// normalizer's list and not a second copy of it.
func TestRefreshFirstPromptShape(t *testing.T) {
	db := ingestDB("me@example.com", "e1")
	s := NewWithDB(db, nil)
	in := ingestOf("e1", "sess", "me@example.com", event.UserPrompt, 1)
	in.Event.Text = "<command-name>/effort</command-name>\n<command-message>effort</command-message>\n<command-args></command-args>"
	if _, err := s.UpsertEvents(context.Background(), []Ingest{in}); err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	c := db.find(t, sqlFirstPrompt)
	for _, want := range []string{
		"agent_id IS NULL", "kind IN ('', 'human', 'slash_command')", "ORDER BY seq", "LIMIT 1", "unnest($7::text[])",
		// The template comparison trims with the normalizer's cutset, not
		// btrim's default, so the two sides strip the same characters.
		"ltrim(coalesce(h.text, a.text, ''), $8)",
		// An unclassified winner keeps the provenance the row already had.
		"WHEN h.kind = '' THEN s.title_source",
		// A promoted row with no title adopts its stored first prompt.
		"COALESCE(s.first_prompt, a.title)",
	} {
		if !strings.Contains(c.sql, want) {
			t.Errorf("refresh statement lacks %q:\n%s", want, c.sql)
		}
	}
	if got := c.args[7]; got != normalize.TemplateCutset {
		t.Errorf("cutset = %q, want the normalizer's", got)
	}
	if got := c.args[8]; got != normalize.TitleMax {
		t.Errorf("title bound = %v, want the normalizer's", got)
	}
	if got := c.args[1]; got != "e1" {
		t.Errorf("human candidate id = %v, want e1", got)
	}
	if got := c.args[2]; got != "/effort" {
		t.Errorf("human candidate title = %v, want the rendered command", got)
	}
	if got := c.args[5]; got != true {
		t.Errorf("command flag = %v, want true", got)
	}
	prefixes, _ := c.args[6].([]string)
	if len(prefixes) != len(normalize.InternalTemplatePrefixes()) || prefixes[0] != normalize.InternalTemplatePrefixes()[0] {
		t.Errorf("template prefixes = %v", c.args[6])
	}
	if !strings.Contains(db.find(t, sqlMessages).sql, "kind, agent_id") {
		t.Error("messages are inserted without their kind")
	}
	if !strings.Contains(db.find(t, sqlRollup).sql, "session_type_merge(sessions.session_type, EXCLUDED.session_type)") {
		t.Error("the rollup does not merge the type through session_type_merge")
	}
}

// The three lattice migrations are additive and bounded: no index builds
// under the migration lock, nothing dropped, and only the backfill file reads
// the events table, by the anti-join shape that was measured.
func TestLatticeMigrationsAreAdditiveAndBounded(t *testing.T) {
	for _, name := range []string{"0014_message_kinds.sql", "0015_session_lattice.sql", "0016_empty_backfill.sql"} {
		body, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		sql := strings.ToUpper(string(body))
		for _, forbidden := range []string{"CREATE INDEX", "DROP COLUMN", "DROP TABLE"} {
			if strings.Contains(sql, forbidden) {
				t.Errorf("%s contains %s", name, forbidden)
			}
		}
		if !strings.Contains(string(body), "Rollback floor") {
			t.Errorf("%s does not state its rollback floor", name)
		}
		reads := strings.Contains(sql, "FROM EVENTS")
		if reads != (name == "0016_empty_backfill.sql") {
			t.Errorf("%s reads events = %v; only 0016 may", name, reads)
		}
		if name == "0016_empty_backfill.sql" && !strings.Contains(sql, "NOT EXISTS") {
			t.Errorf("%s is not the anti-join that was measured", name)
		}
		if name == "0014_message_kinds.sql" && !strings.Contains(string(body), "NOT done") {
			t.Errorf("%s does not say that back-population is left to the runner", name)
		}
	}
	// A session type the CHECK does not know cannot be written by this code.
	for _, typ := range []string{sessionTypeOf(true, 0), sessionTypeOf(false, 1), sessionTypeOf(false, 0), "internal"} {
		if !containsString(SessionTypes, typ) {
			t.Errorf("%q is written but not in SessionTypes", typ)
		}
	}
}
