package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/backfill"
	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/daemon"
	"github.com/loopai-hq/agent-sessions/internal/discovery"
	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// A three-turn transcript, the shape the recovery diff runs against.
func threeTurnTranscript(t *testing.T, dir, sid string) string {
	t.Helper()
	var lines []string
	for i := 1; i <= 3; i++ {
		ts := fmt.Sprintf("2026-08-17T10:0%d:00.000Z", i)
		lines = append(lines,
			fmt.Sprintf(`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":"u%d","promptId":"p%d","cwd":"/repo","message":{"role":"user","content":"prompt %d"}}`, sid, ts, i, i, i),
			fmt.Sprintf(`{"type":"assistant","sessionId":%q,"timestamp":%q,"uuid":"a%d","cwd":"/repo","message":{"role":"assistant","model":"claude-fable-5","id":"msg_%d","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"answer %d"}]}}`, sid, ts, i, i, i),
		)
	}
	path := filepath.Join(dir, sid+".jsonl")
	writeFile(t, path, strings.Join(lines, "\n")+"\n")
	return path
}

func pendingEvents(t *testing.T) []event.Event {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: config.Paths{}.SpoolDir()})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := sp.Lease(0)
	if err != nil {
		t.Fatal(err)
	}
	var out []event.Event
	for _, l := range leased {
		var e event.Event
		if err := json.Unmarshal(l.Item.Payload, &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

// Two of three turns were captured by the hooks; recovery emits the third
// prompt and its answer, and nothing the hooks already have.
func TestRecoveryEmitsOnlyAnchorsMissingFromLedger(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	const sid = "rec-1"
	path := threeTurnTranscript(t, t.TempDir(), sid)
	ledger := capture.Ledger{Dir: p.StateDir()}
	_ = ledger.Captured(sid, capture.EntriesOf([]event.Event{
		{Type: event.UserPrompt, PromptID: "p1"}, {Type: event.AssistantTurn, PromptID: "p1", RecordUUID: "a1"},
		{Type: event.UserPrompt, PromptID: "p2"}, {Type: event.AssistantTurn, PromptID: "p2"}, // a Stop whose tail did not match
	}))

	cfg := mustConfig(t)
	n, err := recoverSession(p, cfg, sid, path, "/repo", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("recovered %d events, want the third prompt and its answer", n)
	}
	got := pendingEvents(t)
	var texts []string
	for _, e := range got {
		texts = append(texts, e.Text)
		if e.Origin != event.OriginTranscript || e.SessionID != sid {
			t.Errorf("recovered event is not a transcript copy of the session: %+v", e)
		}
	}
	if strings.Join(texts, "|") != "prompt 3|answer 3" {
		t.Fatalf("recovered texts = %v", texts)
	}
	sp, _ := spool.Open(spool.Options{Dir: p.SpoolDir()})
	if st, _ := sp.Stats(); st.Dropped[recoveredCounter] != 2 {
		t.Errorf("counter = %v, want recovered_from_transcript=2", st.Dropped)
	}
	c, _ := ledger.Read(sid)
	if !c.PromptIDs["p3"] || !c.RecordUUIDs["a3"] {
		t.Errorf("the ledger was not updated with what was recovered: %+v", c)
	}
}

func TestRecoveryIsIdempotent(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	const sid = "rec-2"
	path := threeTurnTranscript(t, t.TempDir(), sid)
	cfg := mustConfig(t)

	// Nothing in the ledger: every turn is missing, and the transcript's own
	// session_started rides along with the first record it emits.
	first, err := recoverSession(p, cfg, sid, path, "/repo", time.Now())
	if err != nil || first != 7 {
		t.Fatalf("first pass recovered %d (%v), want the start and all six turns", first, err)
	}
	second, err := recoverSession(p, cfg, sid, path, "/repo", time.Now())
	if err != nil || second != 0 {
		t.Fatalf("second pass recovered %d (%v), want 0", second, err)
	}
	if n := len(pendingEvents(t)); n != 7 {
		t.Fatalf("pending = %d, want 7", n)
	}
}

// A hook that died leaves a marker with the transcript path on it. The next
// SessionStart recovers the session and, because there is now a backlog,
// starts the daemon that will ship it.
func TestRecoveryRunsForInflightMarkers(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	const sid = "rec-3"
	path := threeTurnTranscript(t, t.TempDir(), sid)
	ledger := capture.Ledger{Dir: p.StateDir()}
	_, _ = ledger.Inflight(capture.Marker{SessionID: sid, HookEvent: "Stop", PromptID: "p3", TranscriptPath: path, Cwd: "/repo", At: time.Now().Add(-time.Hour)})

	var started int
	restore := stubSpawn(func(config.Paths, capture.HookEvent) error { started++; return nil })
	defer restore()
	feedHook(t, capture.HookEvent{HookEventName: "SessionStart", SessionID: "s-next", Cwd: home, Source: "startup"})

	if ledger.HasInflight(sid) {
		t.Fatal("the marker was not cleared by the recovery pass")
	}
	var recovered int
	for _, e := range pendingEvents(t) {
		if e.SessionID == sid {
			recovered++
		}
	}
	if recovered != 7 {
		t.Fatalf("recovered %d events for the marked session, want the start and six turns", recovered)
	}
	if started != 1 {
		t.Fatalf("daemon started %d times; a recovery leaves a backlog that needs one", started)
	}
	if !strings.Contains(readAgentLog(t, home), "re-derived from the transcript") {
		t.Error("the log does not record the recovery")
	}
}

// A marker written moments ago belongs to a hook that is, as far as anything
// can tell, still running: no daemon exists before the first prompt, and the
// enrichment phase lasts up to a minute. Another session's SessionStart must
// leave it alone, both when the transcript does not exist yet (clearing the
// marker would make a hook killed a second later unrecoverable) and when it
// does (walking it re-emits turns the hooks are delivering right now).
func TestRecoveryLeavesAFreshMarkerForALaterPass(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	ledger := capture.Ledger{Dir: p.StateDir()}
	cfg := mustConfig(t)

	const young = "rec-young"
	notYet := filepath.Join(t.TempDir(), young+".jsonl")
	_, _ = ledger.Inflight(capture.Marker{SessionID: young, HookEvent: "SessionStart", TranscriptPath: notYet, Cwd: "/repo", At: time.Now()})
	if n := recoverMarked(p, cfg, ledger, time.Now()); n != 0 || !ledger.HasInflight(young) {
		t.Fatalf("recovered %d for a session whose SessionStart hook is still running; marker kept=%v", n, ledger.HasInflight(young))
	}

	const live = "rec-live"
	path := threeTurnTranscript(t, t.TempDir(), live)
	_, _ = ledger.Inflight(capture.Marker{SessionID: live, HookEvent: "PostToolUse", PromptID: "p3", TranscriptPath: path, Cwd: "/repo", At: time.Now()})
	if n := recoverMarked(p, cfg, ledger, time.Now()); n != 0 || !ledger.HasInflight(live) {
		t.Fatalf("recovered %d events for a session whose hook is seconds into its work; marker kept=%v", n, ledger.HasInflight(live))
	}
	if n := len(pendingEvents(t)); n != 0 {
		t.Fatalf("%d transcript copies were spooled beside the hooks' own", n)
	}

	// Once the grace has passed the same markers are what they look like.
	later := time.Now().Add(inflightGrace + time.Minute)
	if n := recoverMarked(p, cfg, ledger, later); n != 7 {
		t.Fatalf("recovered %d after the grace, want the live session's 7", n)
	}
	if ledger.HasInflight(live) {
		t.Error("the settled marker of the walked session was not cleared")
	}
	if ledger.HasInflight(young) {
		t.Error("the settled marker of a session with no transcript was not cleared")
	}
}

// A session with one settled marker and one young one is not walked either:
// the young hook's record is already in the transcript, so the walk would
// spool a copy of what that hook is about to deliver. Nothing is lost by
// waiting; both markers keep their claim and the next pass after the grace
// recovers everything the session lost.
func TestRecoveryWaitsWhileAnyMarkerOfTheSessionIsYoung(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	ledger := capture.Ledger{Dir: p.StateDir()}
	cfg := mustConfig(t)
	const sid = "rec-mixed"
	path := threeTurnTranscript(t, t.TempDir(), sid)
	now := time.Now()
	_, _ = ledger.Inflight(capture.Marker{SessionID: sid, HookEvent: "Stop", PromptID: "p3", TranscriptPath: path, Cwd: "/repo", At: now.Add(-inflightGrace - time.Hour)})
	_, _ = ledger.Inflight(capture.Marker{SessionID: sid, HookEvent: "SessionEnd", TranscriptPath: path, Cwd: "/repo", At: now})

	if n := recoverMarked(p, cfg, ledger, now); n != 0 {
		t.Fatalf("recovered %d events while the session's SessionEnd hook is still running", n)
	}
	if n := len(pendingEvents(t)); n != 0 {
		t.Fatalf("%d transcript copies were spooled beside what the live hook is delivering", n)
	}
	c, _ := ledger.Read(sid)
	if len(c.Inflight) != 2 {
		t.Fatalf("markers after the pass = %+v, want both kept for the next pass", c.Inflight)
	}

	later := now.Add(inflightGrace + time.Minute)
	if n := recoverMarked(p, cfg, ledger, later); n != 7 {
		t.Fatalf("recovered %d after the grace, want the session's 7", n)
	}
	if ledger.HasInflight(sid) {
		t.Error("the markers were not cleared once both had settled and the session was walked")
	}
}

// The session's own recovery (at its daemon's exit, or the reconciler's)
// clears only the markers old enough to be dead; a SessionEnd hook still
// scrubbing while the daemon finalises keeps its claim.
func TestRecoverySessionClearsOnlySettledMarkers(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	const sid = "rec-settle"
	path := threeTurnTranscript(t, t.TempDir(), sid)
	ledger := capture.Ledger{Dir: p.StateDir()}
	now := time.Now()
	_, _ = ledger.Inflight(capture.Marker{SessionID: sid, HookEvent: "Stop", PromptID: "p3", TranscriptPath: path, At: now.Add(-time.Hour)})
	_, _ = ledger.Inflight(capture.Marker{SessionID: sid, HookEvent: "SessionEnd", TranscriptPath: path, At: now})

	if _, err := recoverSession(p, mustConfig(t), sid, path, "/repo", now); err != nil {
		t.Fatal(err)
	}
	c, _ := ledger.Read(sid)
	if len(c.Inflight) != 1 || c.Inflight[0].HookEvent != "SessionEnd" {
		t.Fatalf("markers after recovery = %+v, want the young SessionEnd alone", c.Inflight)
	}
}

// PreToolUse is the fast hook and PostToolUse the slow one the harness kills
// at teardown, so "the call landed and the result did not" is the likeliest
// loss there is. The landed call must not vouch for the missing result.
func TestRecoveryFindsAToolResultWhoseCallLanded(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	ledger := capture.Ledger{Dir: p.StateDir()}
	const sid = "rec-toolresult"
	_ = ledger.Captured(sid, capture.EntriesOf([]event.Event{
		{Type: event.UserPrompt, PromptID: "p1"},
		{Type: event.ToolCall, PromptID: "p1", ToolUseID: "toolu_1"}, // PreToolUse landed; PostToolUse abandoned
		{Type: event.AssistantTurn, PromptID: "p1", RecordUUID: "a2"},
	}))
	have, err := ledger.Read(sid)
	if err != nil {
		t.Fatal(err)
	}
	anchors := []backfill.Anchor{
		{Type: "user", UUID: "u1", PromptID: "p1"},
		{Type: "assistant", UUID: "a1", ToolUseIDs: []string{"toolu_1"}},
		{Type: "user", UUID: "r1", IsToolResult: true, ToolUseIDs: []string{"toolu_1"}},
		{Type: "assistant", UUID: "a2"},
	}
	got := missingRecords(anchors, have)
	if !got["r1"] {
		t.Fatalf("the tool_result record r1 (PostToolUse abandoned, PreToolUse landed) is not recovered: accepted=%v", got)
	}
	if got["a1"] || got["u1"] || got["a2"] {
		t.Fatalf("records the hooks captured were accepted: %v", got)
	}

	// The mirror image: the result landed and the call did not.
	_ = ledger.Captured(sid+"-b", capture.EntriesOf([]event.Event{
		{Type: event.UserPrompt, PromptID: "p1"},
		{Type: event.ToolResult, PromptID: "p1", ToolUseID: "toolu_1"},
		{Type: event.AssistantTurn, PromptID: "p1", RecordUUID: "a2"},
	}))
	have, _ = ledger.Read(sid + "-b")
	if got := missingRecords(anchors, have); !got["a1"] || got["r1"] {
		t.Fatalf("PreToolUse abandoned, PostToolUse landed: accepted=%v, want a1 alone", got)
	}
}

// A subagent's tool records live in its own file under <session>/subagents/,
// which the main transcript never mentions. A marker for subagent work names
// that file, and the lost PostToolUse is found there and nowhere else.
func TestRecoveryReadsTheSubagentTranscriptAMarkerNames(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	const sid = "rec-agent"
	dir := t.TempDir()
	path := threeTurnTranscript(t, dir, sid)
	agent := filepath.Join(dir, sid, "subagents", "agent-a1b2.jsonl")
	rec := func(typ, uuid, prompt, msg string) string {
		return fmt.Sprintf(`{"type":%q,"sessionId":%q,"isSidechain":true,"agentId":"a1b2","timestamp":"2026-08-17T10:03:1%c.000Z","uuid":%q,"promptId":%q,"cwd":"/repo","message":%s}`,
			typ, sid, uuid[len(uuid)-1], uuid, prompt, msg)
	}
	writeFile(t, agent, strings.Join([]string{
		rec("user", "g1", "p3", `{"role":"user","content":"task"}`),
		rec("assistant", "g2", "", `{"role":"assistant","model":"claude-fable-5","id":"msg_g2","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"tool_use","id":"toolu_agent","name":"Bash","input":{"command":"ls"}}]}`),
		rec("user", "g3", "p3", `{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_agent","content":"file"}]}`),
		rec("assistant", "g4", "", `{"role":"assistant","model":"claude-fable-5","id":"msg_g4","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"done"}]}`),
	}, "\n")+"\n")

	ledger := capture.Ledger{Dir: p.StateDir()}
	// Everything in the main transcript landed, and so did the subagent's
	// PreToolUse; its PostToolUse was abandoned.
	var evs []event.Event
	for i := 1; i <= 3; i++ {
		evs = append(evs,
			event.Event{Type: event.UserPrompt, PromptID: fmt.Sprintf("p%d", i)},
			event.Event{Type: event.AssistantTurn, PromptID: fmt.Sprintf("p%d", i), RecordUUID: fmt.Sprintf("a%d", i)})
	}
	evs = append(evs, event.Event{Type: event.ToolCall, PromptID: "p3", ToolUseID: "toolu_agent", AgentID: "a1b2"})
	_ = ledger.Captured(sid, capture.EntriesOf(evs))
	_, _ = ledger.Inflight(capture.Marker{SessionID: sid, HookEvent: "PostToolUse", PromptID: "p3", ToolUseID: "toolu_agent",
		TranscriptPath: path, AgentTranscriptPath: agent, Cwd: "/repo", At: time.Now().Add(-time.Hour)})

	n, err := recoverSession(p, mustConfig(t), sid, path, "/repo", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("nothing was recovered from the subagent transcript the marker names")
	}
	var results int
	for _, e := range pendingEvents(t) {
		switch e.Type {
		case event.ToolResult:
			if e.ToolUseID != "toolu_agent" || e.AgentID == "" || e.SessionID != sid {
				t.Errorf("recovered tool_result is not the subagent's: %+v", e)
			}
			results++
		case event.UserPrompt, event.ToolCall, event.AssistantTurn:
			// The subagent's task and final answer are not the hooks' to have
			// lost, and its call landed; the main transcript is complete.
			t.Errorf("a record the hooks captured, or never capture, was re-emitted: %s %s", e.Type, e.RecordUUID)
		}
	}
	if results != 1 {
		t.Fatalf("recovered %d tool_result events, want the subagent's one", results)
	}
	if ledger.HasInflight(sid) {
		t.Error("the marker was not cleared after its file was walked")
	}
	c, _ := ledger.Read(sid)
	if !c.ToolResultIDs["toolu_agent"] {
		t.Errorf("the recovered result was not recorded in the ledger: %+v", c)
	}
}

// Old finished state and old ledgers are swept at SessionStart so Reconcile
// stops parsing thousands of files.
func TestSweepRunsAtSessionStart(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	withHookSeams(t, nil, nil)
	old := time.Now().Add(-9 * 24 * time.Hour)
	st := daemon.State{SessionID: "ancient", OwnerPID: 1, DaemonPID: 1, Finalized: true, EndedAt: old, StartedAt: old, Heartbeat: old}
	b, _ := json.Marshal(st)
	writeFile(t, filepath.Join(p.StateDir(), "ancient.json"), string(b))
	ledger := capture.Ledger{Dir: p.StateDir()}
	_ = ledger.Captured("ancient", capture.EntriesOf([]event.Event{{Type: event.SessionStarted}}))
	_ = os.Chtimes(filepath.Join(p.StateDir(), "ancient.captured"), old, old)

	feedHook(t, capture.HookEvent{HookEventName: "SessionStart", SessionID: "s-sweep", Cwd: home, Source: "startup"})

	for _, name := range []string{"ancient.json", "ancient.captured"} {
		if _, err := os.Stat(filepath.Join(p.StateDir(), name)); !os.IsNotExist(err) {
			t.Errorf("%s survived the sweep", name)
		}
	}
}

// The upgrade check is throttled across daemons through a stamp, so fifty
// session starts a day are one manifest read, not fifty.
func TestUpgradeCheckIsThrottledAcrossSpawns(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	cfg := mustConfig(t)
	prev := Version
	Version = "e2e-throttle"
	defer func() { Version = prev }()

	// The first check runs (and is skipped by the ownership guard, which is
	// the outcome recorded, not a network call).
	first := checkUpgradeOnce(context.Background(), p, cfg)
	if first.Result == "" || first.LastCheckAt.IsZero() {
		t.Fatalf("no outcome recorded: %+v", first)
	}
	if st := readUpgradeStatus(p); st == nil || st.Result != first.Result {
		t.Fatalf("upgrade.json = %+v, want the outcome just recorded", st)
	}
	if !stampDue(p, stampUpgradeCheck, time.Nanosecond, time.Now().Add(time.Second)) || stampDue(p, stampUpgradeCheck, upgradeRecheck, time.Now()) {
		t.Fatal("the check stamp was not written")
	}

	// A second daemon within the interval does not check again.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	startUpgradeCheck(ctx, p, cfg)
	after := readUpgradeStatus(p)
	if !after.LastCheckAt.Equal(first.LastCheckAt) {
		t.Fatalf("a second daemon re-checked within the interval: %v -> %v", first.LastCheckAt, after.LastCheckAt)
	}
	if !strings.Contains(readAgentLog(t, home), "release host checked") {
		t.Error("the throttled daemon did not say why it skipped")
	}

	// Once the stamp is old, the next daemon checks.
	if err := writeStamp(p, stampUpgradeCheck, time.Now().Add(-upgradeRecheck-time.Minute)); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	startUpgradeCheck(ctx2, p, cfg)
	if again := readUpgradeStatus(p); !again.LastCheckAt.After(first.LastCheckAt) {
		t.Fatal("an overdue check did not run")
	}
}

// Parked items go back to the queue when the binary changes and once a day.
func TestParkedItemsRedriveOnVersionChange(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	sp, err := spool.Open(spool.Options{Dir: p.SpoolDir(), MaxUndecided: 1})
	if err != nil {
		t.Fatal(err)
	}
	park := func() {
		_ = sp.Add(spool.Item{Kind: "event", SessionID: "s", Payload: json.RawMessage(`{}`)})
		leased, _ := sp.Lease(1)
		if _, err := sp.Undecided(leased[0]); err != nil {
			t.Fatal(err)
		}
	}
	park()
	now := time.Now()

	// Same version, redriven an hour ago: nothing happens.
	_ = writeText(p, stampRedriveVer, Version)
	_ = writeStamp(p, stampRedriveLast, now.Add(-time.Hour))
	redriveParkedIfDue(p, sp, now)
	if st, _ := sp.Stats(); st.Parked != 1 {
		t.Fatalf("parked = %d after an unneeded redrive, want 1", st.Parked)
	}

	// The binary changed: redriven at once.
	_ = writeText(p, stampRedriveVer, "some-older-build")
	redriveParkedIfDue(p, sp, now)
	if st, _ := sp.Stats(); st.Parked != 0 || st.Pending != 1 {
		t.Fatalf("after a version change parked=%d pending=%d, want 0 and 1", st.Parked, st.Pending)
	}
	if readText(p, stampRedriveVer) != Version {
		t.Error("the version stamp was not advanced")
	}

	// A day later, regardless of version.
	park()
	_ = writeStamp(p, stampRedriveLast, now.Add(-25*time.Hour))
	redriveParkedIfDue(p, sp, now)
	if st, _ := sp.Stats(); st.Parked != 0 {
		t.Fatalf("parked = %d after the daily redrive, want 0", st.Parked)
	}
}

// ---------------------------------------------------------------- repair

// repairServer answers /v1/repair with a scripted list and accepts events.
type repairServer struct {
	*httptest.Server
	mu     sync.Mutex
	status int
	list   string
	got    []spool.Item
	asked  int
}

func newRepairServer(t *testing.T, status int, list string) *repairServer {
	t.Helper()
	s := &repairServer{status: status, list: list}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case repairPath:
			s.mu.Lock()
			s.asked++
			s.mu.Unlock()
			if r.Header.Get("Authorization") != "Bearer loops_v1_test" {
				http.Error(w, "unauthenticated", http.StatusUnauthorized)
				return
			}
			w.WriteHeader(s.status)
			_, _ = w.Write([]byte(s.list))
		case eventsPath:
			var items []spool.Item
			_ = json.NewDecoder(r.Body).Decode(&items)
			out := struct {
				Accepted []string `json:"accepted"`
			}{Accepted: []string{}}
			s.mu.Lock()
			for _, it := range items {
				s.got = append(s.got, it)
				out.Accepted = append(out.Accepted, it.ID)
			}
			s.mu.Unlock()
			_ = json.NewEncoder(w).Encode(out)
		default:
			http.NotFound(w, r)
		}
	}))
	return s
}

func (s *repairServer) items() []spool.Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]spool.Item(nil), s.got...)
}

func TestRepairWalksTheSessionsTheServerNames(t *testing.T) {
	const sid = "repair-1"
	dir := t.TempDir()
	path := threeTurnTranscript(t, dir, sid)
	// A neighbour in the same project that the server did not name.
	threeTurnTranscript(t, dir, "repair-other")
	srv := newRepairServer(t, http.StatusOK, fmt.Sprintf(`{"sessions":[{"session_id":%q,"transcript_path":%q,"reason":"missing_answers"}]}`, sid, path))
	defer srv.Close()
	home := enrolledHome(t, srv.URL)
	p := config.Paths{}
	cfg := mustConfig(t)
	cfg.Roots = map[string]string{string(discovery.ClaudeCode): dir}

	runRepairIfDue(context.Background(), p, cfg, true)

	got := srv.items()
	if len(got) != 7 {
		t.Fatalf("the server received %d items, want the named session's 7 (start + 3 prompts + 3 answers)", len(got))
	}
	for _, it := range got {
		if it.SessionID != sid {
			t.Errorf("an unnamed session was walked: %s", it.SessionID)
		}
	}
	if _, ok := readStamp(p, stampRepairLast); !ok {
		t.Error("the repair stamp was not written")
	}
	if !strings.Contains(readAgentLog(t, home), "repair: re-walked 1 session(s)") {
		t.Errorf("log does not record the repair:\n%s", readAgentLog(t, home))
	}

	// Within a day, nothing is asked again unless forced.
	before := srv.asked
	runRepairIfDue(context.Background(), p, cfg, false)
	if srv.asked != before {
		t.Error("the daily gate did not hold")
	}
}

// codexRollout writes a one-turn Codex rollout whose token_count record is
// the reason the server lists Codex sessions: their tokens were never
// credited. The path is under .codex/, which is how the list marks the source
// when it does not say.
func codexRollout(t *testing.T, root, sid, cwd string) string {
	t.Helper()
	path := filepath.Join(root, ".codex", "sessions", "2026", "08", "17", "rollout-"+sid+".jsonl")
	writeFile(t, path, strings.Join([]string{
		fmt.Sprintf(`{"timestamp":"2026-08-17T10:00:00Z","type":"session_meta","payload":{"id":%q,"timestamp":"2026-08-17T10:00:00Z","cwd":%q,"cli_version":"0.147.0"}}`, sid, cwd),
		`{"timestamp":"2026-08-17T10:00:00Z","type":"turn_context","payload":{"model":"gpt-5.4","cwd":"/repo"}}`,
		`{"timestamp":"2026-08-17T10:00:01Z","type":"response_item","payload":{"type":"message","id":"u1","role":"user","content":[{"type":"input_text","text":"from codex"}]}}`,
		`{"timestamp":"2026-08-17T10:00:02Z","type":"response_item","payload":{"type":"message","id":"a1","role":"assistant","content":[{"type":"output_text","text":"done"}]}}`,
		`{"timestamp":"2026-08-17T10:00:02Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":9000,"cached_input_tokens":8000,"output_tokens":500,"reasoning_output_tokens":7,"total_tokens":9500},"last_token_usage":{"input_tokens":1000,"cached_input_tokens":900,"output_tokens":50,"reasoning_output_tokens":3,"total_tokens":1050}}}}`,
	}, "\n")+"\n")
	return path
}

// The list can name Codex sessions. The named rollout is walked through the
// Codex walker, alone among its neighbours, and its token_count reaches the
// server as usage on the turn, which is the whole point of listing it. A
// named rollout whose project is excluded is checked by its own cwd and
// left alone.
func TestRepairWalksACodexSessionAndItsTokens(t *testing.T) {
	const sid = "codex-repair-1"
	root := t.TempDir()
	path := codexRollout(t, root, sid, "/repo")
	codexRollout(t, root, "codex-neighbour", "/repo")
	excluded := codexRollout(t, root, "codex-excluded", "/private/scratch")
	srv := newRepairServer(t, http.StatusOK, fmt.Sprintf(
		`{"sessions":[{"session_id":%q,"transcript_path":%q,"reason":"codex_tokens","source":"codex"},{"session_id":"codex-excluded","transcript_path":%q,"reason":"codex_tokens"}]}`,
		sid, path, excluded))
	defer srv.Close()
	home := enrolledHome(t, srv.URL)
	cfg := mustConfig(t)
	cfg.ExcludePaths = []string{"/private/scratch"}
	cfg.Roots = map[string]string{string(discovery.Codex): filepath.Join(root, ".codex", "sessions")}

	runRepairIfDue(context.Background(), config.Paths{}, cfg, true)

	got := srv.items()
	if len(got) == 0 {
		t.Fatal("the server received nothing for the named Codex session")
	}
	var usage bool
	for _, it := range got {
		if it.SessionID != sid {
			t.Errorf("a session the server did not name, or one it excluded, was walked: %s", it.SessionID)
		}
		if strings.Contains(string(it.Payload), `"output_tokens":50`) {
			usage = true
		}
	}
	if !usage {
		t.Errorf("no item carries the turn's token_count as usage; got %d items", len(got))
	}
	if log := readAgentLog(t, home); !strings.Contains(log, "repair: re-walked 1 session(s)") {
		t.Errorf("log does not record one re-walk:\n%s", log)
	}
}

// The server's list is a set of paths the client then walks under. A path
// outside the roots this machine keeps transcripts in is refused before any
// directory is read, whatever the server says.
func TestRepairRefusesAPathOutsideTheSessionRoots(t *testing.T) {
	const sid = "repair-outside"
	elsewhere := threeTurnTranscript(t, t.TempDir(), sid)
	srv := newRepairServer(t, http.StatusOK, fmt.Sprintf(`{"sessions":[{"session_id":%q,"transcript_path":%q,"reason":"missing_answers"}]}`, sid, elsewhere))
	defer srv.Close()
	home := enrolledHome(t, srv.URL)
	cfg := mustConfig(t)
	cfg.Roots = map[string]string{string(discovery.ClaudeCode): t.TempDir()}

	runRepairIfDue(context.Background(), config.Paths{}, cfg, true)

	if n := len(srv.items()); n != 0 {
		t.Fatalf("%d items were sent for a transcript outside the session roots", n)
	}
	if log := readAgentLog(t, home); !strings.Contains(log, "outside this machine's session roots") {
		t.Errorf("the refusal was not logged:\n%s", log)
	}
	if underAny(filepath.Join(home, "..", "x.jsonl"), []string{home}) || !underAny(filepath.Join(home, "a", "b.jsonl"), []string{home}) {
		t.Error("underAny: a path that climbs out is inside, or a nested one is outside")
	}
}

// The roots a repair path is checked against are read from the discovery
// summary as written, candidates included: a default location a tool adopts
// after `discover` ran is still this machine's, and a config without roots
// (an install that predates them) is not a machine that keeps no transcripts.
func TestRepairAcceptsAPathUnderADiscoveredCandidate(t *testing.T) {
	const sid = "repair-candidate"
	// The second default layout for Claude Code, empty when discovery ran.
	root := filepath.Join(t.TempDir(), ".config", "claude", "projects")
	path := threeTurnTranscript(t, filepath.Join(root, "-repo"), sid)
	srv := newRepairServer(t, http.StatusOK, fmt.Sprintf(`{"sessions":[{"session_id":%q,"transcript_path":%q,"reason":"missing_answers"}]}`, sid, path))
	defer srv.Close()
	home := enrolledHome(t, srv.URL)
	p := config.Paths{}
	cfg := mustConfig(t)
	cfg.Roots = nil
	sum := discovery.Summary{Findings: []discovery.Finding{{
		Tool: discovery.ClaudeCode, State: discovery.Absent,
		Candidates: []discovery.Candidate{{Path: root, Exists: false}},
	}}}
	if err := discovery.WriteSummary(p.DiscoveryFile(), sum); err != nil {
		t.Fatal(err)
	}

	runRepairIfDue(context.Background(), p, cfg, true)

	got := srv.items()
	if len(got) != 7 {
		t.Fatalf("the server received %d items, want the named session's 7; log:\n%s", len(got), readAgentLog(t, home))
	}
	for _, it := range got {
		if it.SessionID != sid {
			t.Errorf("an unnamed session was walked: %s", it.SessionID)
		}
	}
	if strings.Contains(readAgentLog(t, home), "outside this machine's session roots") {
		t.Errorf("a path under a discovered candidate was refused:\n%s", readAgentLog(t, home))
	}
}

// A server without the route, or one that is failing, is skipped in silence,
// and the day is spent: the server answered. A request that never reached
// it leaves the day open for the next daemon.
func TestRepairSkipsMissingAndFailingEndpointsSilently(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := newRepairServer(t, status, "nope")
			defer srv.Close()
			home := enrolledHome(t, srv.URL)
			runRepairIfDue(context.Background(), config.Paths{}, mustConfig(t), true)
			if len(srv.items()) != 0 {
				t.Error("events were sent for a failed list")
			}
			if strings.Contains(readAgentLog(t, home), "repair:") {
				t.Errorf("a %d was logged; it must be skipped silently:\n%s", status, readAgentLog(t, home))
			}
			if _, ok := readStamp(config.Paths{}, stampRepairLast); !ok {
				t.Errorf("a %d is an answer; the daily stamp was not written", status)
			}
		})
	}
	t.Run("unreachable", func(t *testing.T) {
		srv := newRepairServer(t, http.StatusOK, `{"sessions":[]}`)
		srv.Close() // a connection refused, the shape of an offline laptop
		home := enrolledHome(t, srv.URL)
		runRepairIfDue(context.Background(), config.Paths{}, mustConfig(t), true)
		if _, ok := readStamp(config.Paths{}, stampRepairLast); ok {
			t.Error("a transport error spent the day's repair check; the next daemon must ask again")
		}
		if strings.Contains(readAgentLog(t, home), "repair:") {
			t.Errorf("a transport error was logged as a repair event:\n%s", readAgentLog(t, home))
		}
	})
}

// A marker a hook was killed while writing does not parse, so it can name no
// transcript and no pass could ever walk it; once it is old enough to be dead
// it is cleared like a path-less marker, and the session stops being listed
// as inflight forever. A young torn marker is left alone like any other.
func TestRecoveryClearsATornMarkerOnceItIsOld(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	p := config.Paths{}
	ledger := capture.Ledger{Dir: p.StateDir()}
	cfg := mustConfig(t)
	const sid = "rec-torn"
	dir := filepath.Join(p.StateDir(), sid+".inflight")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	old := filepath.Join(dir, fmt.Sprintf("%d-%d.json", now.Add(-inflightGrace-time.Minute).UnixNano(), 4242))
	young := filepath.Join(dir, fmt.Sprintf("%d-%d.json", now.UnixNano(), 4243))
	for _, name := range []string{old, young} {
		if err := os.WriteFile(name, []byte(`{"session_id":"rec-torn","hook_event":"Post`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if n := recoverMarked(p, cfg, ledger, now); n != 0 {
		t.Fatalf("recovered %d from a session with nothing walkable", n)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Error("the old torn marker was kept; the session would be listed as inflight forever")
	}
	if _, err := os.Stat(young); err != nil {
		t.Error("the young torn marker was cleared; its hook may still be writing it")
	}
}
