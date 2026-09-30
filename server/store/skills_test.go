package store

// The skill derivation's properties that need no database: which key a
// row lands on, which statement each copy goes through with which
// arguments, how both copies of one typed command and a Skill call with
// its result are folded, what the log lines carry and what they never
// carry, and the silence arithmetic behind the platform summary. What only
// Postgres can answer (the merge's CASE arms, the savepoint inside an
// events batch, the rebuild over stored events) is in
// skills_integration_test.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

const (
	skillTestDevice = "11111111-2222-4333-8444-555555555555"
	skillTestToken  = "66666666-7777-4888-9999-000000000000"
	skillTestEmail  = "dev@example.com"
)

// skillStore is a store over the fake connection whose lines the tests
// read back as JSON.
func skillStore(db *fakeDB) (*Store, *bytes.Buffer) {
	var buf bytes.Buffer
	s := NewWithDB(db, nil)
	s.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	return s, &buf
}

func skillLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %v (%s)", err, line)
		}
		out = append(out, entry)
	}
	return out
}

func linesWithMessage(lines []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, l := range lines {
		if l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}

// argText renders a statement argument the way a grep over the wire would
// see it, so an assertion that a secret is absent covers pointers and
// slices too.
func argText(a any) string {
	switch v := a.(type) {
	case *string:
		if v == nil {
			return ""
		}
		return *v
	case []string:
		return strings.Join(v, " ")
	default:
		return fmt.Sprint(v)
	}
}

func insertedRow() *stub {
	return &stub{match: "INSERT INTO skill_invocations", rows: [][]any{{true, false, nil, nil}}}
}

// skillCall is a hook-origin Skill tool_call as the agent ships it.
func skillCall(id, sid, promptID, toolUseID, input string, seq int64) Ingest {
	it := ingestOf(id, sid, skillTestEmail, event.ToolCall, seq)
	it.Event.PromptID, it.Event.ToolUseID = promptID, toolUseID
	it.Event.Tool = &event.Tool{Name: "Skill", Input: json.RawMessage(input)}
	it.Event.HarnessVersion = "2.1.273"
	it.DeviceID = skillTestDevice
	it.Repo = "loop-sessions"
	return it
}

// skillResult is the hook copy of a Skill tool's result, which carries the
// tool's name; a transcript result carries only the output.
func skillResult(id, sid, toolUseID string, typ event.Type, seq int64) Ingest {
	it := ingestOf(id, sid, skillTestEmail, typ, seq)
	it.Event.ToolUseID = toolUseID
	it.Event.Tool = &event.Tool{Name: "Skill", Output: "Launching skill"}
	it.DeviceID = skillTestDevice
	return it
}

// typedHook is the hook copy of a typed command: the line as typed, with
// the prompt id the hook payload carries.
func typedHook(id, sid, promptID, text string, seq int64) Ingest {
	it := ingestOf(id, sid, skillTestEmail, event.UserPrompt, seq)
	it.Event.PromptID, it.Event.Text = promptID, text
	it.DeviceID = skillTestDevice
	return it
}

// typedTranscript is the transcript copy of a typed command: the walker's
// Text is the arguments, the record's content carries the envelope, and
// promptId is on the record only when the harness wrote one (r2:64).
func typedTranscript(id, sid, promptID, name, args string, seq int64) Ingest {
	it := ingestOf(id, sid, skillTestEmail, event.UserPrompt, seq)
	it.Event.Origin = event.OriginTranscript
	it.Event.Text = args
	bare := name
	if i := strings.LastIndex(name, ":"); i >= 0 {
		bare = name[i+1:]
	}
	content := fmt.Sprintf("<command-message>%s</command-message>\n<command-name>/%s</command-name>\n<command-args>%s</command-args>", bare, name, args)
	rec := map[string]any{"type": "user", "uuid": "uuid-" + id, "message": map[string]any{"role": "user", "content": content}}
	if promptID != "" {
		rec["promptId"] = promptID
	}
	raw, _ := json.Marshal(rec)
	it.Event.Raw = raw
	it.DeviceID = skillTestDevice
	return it
}

// ---------------------------------------------------------------- keys and names

func TestDedupeKeyForms(t *testing.T) {
	derived := func(f func(*SkillRow)) SkillRow {
		r := SkillRow{Origin: OriginDerived, AgentPlatform: PlatformClaudeCode, SessionRef: "sess"}
		f(&r)
		return r
	}
	cases := []struct {
		name string
		row  SkillRow
		want string
	}{
		{"form 1, tool_use_id beats every other id", derived(func(r *SkillRow) {
			r.ToolUseID, r.PromptID, r.EventID, r.IdempotencyKey = strPtr("toolu_1"), strPtr("p1"), strPtr("e1"), strPtr("k1")
		}), "claude_code:sess:t:toolu_1"},
		{"form 2, prompt_id beats the event id", derived(func(r *SkillRow) {
			r.PromptID, r.EventID = strPtr("p1"), strPtr("e1")
		}), "claude_code:sess:p:p1"},
		{"form 2b, a codex mention carries the skill", derived(func(r *SkillRow) {
			r.AgentPlatform, r.EventID, r.Skill = PlatformCodex, strPtr("e1"), strPtr("git")
		}), "codex:sess:e:e1:git"},
		{"form 2c, a transcript slash prompt with no prompt_id", derived(func(r *SkillRow) {
			r.EventID = strPtr("e1")
		}), "claude_code:sess:e:e1"},
		{"form 3, a source token", SkillRow{Origin: OriginBeacon, AgentPlatform: PlatformClaudeCode,
			SourceTokenID: strPtr(skillTestToken), IdempotencyKey: strPtr("k1")}, "k:" + skillTestToken + ":k1"},
		{"form 3, an lsd_ device with no token", SkillRow{Origin: OriginHook, AgentPlatform: PlatformClaudeCode,
			DeviceID: strPtr(skillTestDevice), IdempotencyKey: strPtr("k1")}, "k:d:" + skillTestDevice + ":k1"},
		{"form 3, a reconciler keys on its token's platform and environment", SkillRow{Origin: OriginReconciler,
			AgentPlatform: PlatformDevin, SourceTokenID: strPtr(skillTestToken), TokenPlatform: PlatformDevin,
			TokenEnvironment: "reconciler", IdempotencyKey: strPtr("2026-09-18")}, "k:devin:reconciler:2026-09-18"},
		{"an API row's event id is never a key", SkillRow{Origin: OriginHook, AgentPlatform: PlatformClaudeCode,
			EventID: strPtr("e1"), DeviceID: strPtr(skillTestDevice), IdempotencyKey: strPtr("k1")}, "k:d:" + skillTestDevice + ":k1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DedupeKey(tc.row)
			if err != nil {
				t.Fatalf("DedupeKey: %v", err)
			}
			if got != tc.want {
				t.Errorf("key = %q, want %q", got, tc.want)
			}
		})
	}

	// Two codex mentions in one prompt are two keys (G4).
	a := derived(func(r *SkillRow) { r.AgentPlatform, r.EventID, r.Skill = PlatformCodex, strPtr("e1"), strPtr("git") })
	b := derived(func(r *SkillRow) {
		r.AgentPlatform, r.EventID, r.Skill = PlatformCodex, strPtr("e1"), strPtr("git-review")
	})
	ka, _ := DedupeKey(a)
	kb, _ := DedupeKey(b)
	if ka == kb {
		t.Errorf("two mentions in one prompt share the key %q", ka)
	}

	// Form 4: nothing to key on, or an idempotency key with no credential
	// behind it, is the route's 400.
	for _, r := range []SkillRow{
		{Origin: OriginHook, AgentPlatform: PlatformClaudeCode},
		{Origin: OriginHook, AgentPlatform: PlatformClaudeCode, IdempotencyKey: strPtr("k1")},
		{Origin: OriginHook, AgentPlatform: PlatformClaudeCode, EventID: strPtr("e1")},
	} {
		if _, err := DedupeKey(r); !errors.Is(err, ErrDedupeKey) {
			t.Errorf("DedupeKey(%+v) = %v, want ErrDedupeKey", r, err)
		}
	}
}

func TestNormalizeSkillName(t *testing.T) {
	long := strings.Repeat("x", 250)
	cases := []struct {
		in, raw, plugin string
		skill           *string
		off             bool
	}{
		{"git", "git", "", strPtr("git"), false},
		{"/git", "/git", "", strPtr("git"), false},
		{"engg:git", "engg:git", "engg", strPtr("git"), false},
		{"/Engg:Git", "/Engg:Git", "engg", strPtr("git"), false},
		{"$git", "$git", "", strPtr("git"), false},
		{"ralph-loop:help", "ralph-loop:help", "ralph-loop", strPtr("help"), false},
		{"gi\x00t", "git", "", strPtr("git"), false},
		// The shape holds but the slug does not: kept as raw_name for the
		// unknown report, with no (plugin, skill) to join on.
		{"a:b:c", "a:b:c", "", nil, false},
		{"x", "x", "", nil, false},
		{long, long[:200], "", nil, false},
		// Off shape: whitespace, an empty name, a leading punctuation
		// mark. Stored as the literal, the invocation still counts.
		{"has space", OffShapeName, "", nil, true},
		{"", OffShapeName, "", nil, true},
		{"-bad", OffShapeName, "", nil, true},
	}
	for _, tc := range cases {
		raw, plugin, skill, off := NormalizeSkillName(tc.in)
		if raw != tc.raw || plugin != tc.plugin || off != tc.off || argText(skill) != argText(tc.skill) || (skill == nil) != (tc.skill == nil) {
			t.Errorf("NormalizeSkillName(%q) = (%q, %q, %v, %v), want (%q, %q, %v, %v)",
				tc.in, raw, plugin, argText(skill), off, tc.raw, tc.plugin, argText(tc.skill), tc.off)
		}
	}
}

// ---------------------------------------------------------------- the merge

// The one statement every copy goes through, pinned: the column list, the
// three predicates spelled as design 3.3 spells them, the RETURNING shape
// and the twenty-eight arguments in column order with the key last among
// the ids.
func TestSkillUpsertIsTheDesignMergeWithItsArgumentsInOrder(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	at := time.Date(2026, 9, 17, 9, 0, 0, 0, time.UTC)
	row := SkillRow{
		Origin: OriginDerived, AgentPlatform: PlatformClaudeCode, Trust: TrustDevice,
		DeviceID: strPtr(skillTestDevice), RawName: "engg:git", Plugin: "engg", Skill: strPtr("git"),
		SkillSource: SkillSourcePlugin, Trigger: TriggerAgent, Outcome: OutcomeStarted,
		ActorEmail: strPtr(skillTestEmail), ActorKnown: true, Repo: "loop-sessions", SessionRef: "sess",
		SessionType: "", EventID: strPtr("e1"), PromptID: strPtr("p1"), ToolUseID: strPtr("toolu_1"),
		OccurredAt: at, ArgsPresent: true, ArgsBytes: 11, HarnessVersion: "2.1.273",
	}
	out, err := s.UpsertSkillInvocation(context.Background(), db, row)
	if err != nil {
		t.Fatalf("UpsertSkillInvocation: %v", err)
	}
	if !out.Inserted || out.Changed || out.Preempted {
		t.Errorf("outcome = %+v, want inserted", out)
	}
	c := db.find(t, "INSERT INTO skill_invocations AS s (")
	if c.kind != "queryrow" {
		t.Errorf("the merge ran as %s, want queryrow (it RETURNs)", c.kind)
	}
	if c.sql != skillMergeGolden {
		t.Errorf("the merge is not the design 3.3 statement; first difference at byte %d:\n%s", firstDiff(c.sql, skillMergeGolden), c.sql)
	}
	if len(c.args) != 28 {
		t.Fatalf("%d arguments, want 28", len(c.args))
	}
	wantArgs := map[int]string{
		0: at.Format(time.RFC3339), 2: OriginDerived, 3: PlatformClaudeCode, 4: TrustDevice, 5: skillTestDevice,
		7: "engg:git", 8: "engg", 9: "git", 10: SkillSourcePlugin, 11: TriggerAgent, 12: OutcomeStarted,
		14: skillTestEmail, 16: "loop-sessions", 17: "sess", 20: "e1", 21: "p1", 22: "toolu_1",
		24: "claude_code:sess:t:toolu_1", 27: "2.1.273",
	}
	for i, want := range wantArgs {
		got := argText(c.args[i])
		if v, ok := c.args[i].(time.Time); ok {
			got = v.Format(time.RFC3339)
		}
		if got != want {
			t.Errorf("arg $%d = %q, want %q", i+1, got, want)
		}
	}
	if c.args[6] != (*string)(nil) || c.args[23] != (*string)(nil) {
		t.Errorf("absent ids reach the statement as %v and %v, want typed NULLs", c.args[6], c.args[23])
	}
	if c.args[25] != true || c.args[26] != 11 {
		t.Errorf("args_present, args_bytes = %v, %v", c.args[25], c.args[26])
	}

	// No row back is a duplicate: not an error, not a change.
	db2 := &fakeDB{}
	s2, _ := skillStore(db2)
	out, err = s2.UpsertSkillInvocation(context.Background(), db2, row)
	if err != nil || out != (UpsertOutcome{}) {
		t.Errorf("no row back = (%+v, %v), want a zero outcome", out, err)
	}

	// A device id that is not a uuid never reaches the ::uuid cast.
	bad := row
	bad.DeviceID = strPtr("laptop-1")
	if _, err := s2.UpsertSkillInvocation(context.Background(), db2, bad); err == nil {
		t.Error("a non-uuid device id was sent to the statement")
	}
	if n := db2.count("INSERT INTO skill_invocations"); n != 1 {
		t.Errorf("%d merges issued, want 1 (the refused row never ran)", n)
	}
}

// skillMergeGolden is the design 3.3 statement as the builder renders it,
// with U, M and N spelled out where the design writes their letters and
// the uArm line repeated for its fifteen columns. Pinned whole so that a
// change to any arm, predicate or the RETURNING shape is a diff here and
// nowhere else (review-1 F9).
const skillMergeGolden = `INSERT INTO skill_invocations AS s (
  occurred_at, time_clamped, origin, agent_platform, trust, device_id, source_token_id,
  raw_name, plugin, skill, skill_source, trigger, outcome, error_class,
  actor_email, actor_known, repo, session_ref, link_ref, session_type,
  event_id, prompt_id, tool_use_id, idempotency_key, dedupe_key,
  args_present, args_bytes, harness_version)
VALUES ($1, $2, $3, $4, $5, $6::uuid, $7::uuid, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)
ON CONFLICT (dedupe_key) DO UPDATE SET
  preempted_by     = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN s.source_token_id ELSE s.preempted_by END,
  preempted_device = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') AND s.source_token_id IS NULL THEN s.device_id ELSE s.preempted_device END,
  source_token_id  = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN NULL ELSE s.source_token_id END,
  trust = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.trust ELSE s.trust END,
  origin = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.origin ELSE s.origin END,
  device_id = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.device_id ELSE s.device_id END,
  actor_email = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.actor_email ELSE s.actor_email END,
  actor_known = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.actor_known ELSE s.actor_known END,
  skill = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.skill ELSE s.skill END,
  trigger = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.trigger ELSE s.trigger END,
  occurred_at = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.occurred_at ELSE s.occurred_at END,
  time_clamped = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.time_clamped ELSE s.time_clamped END,
  repo = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.repo ELSE s.repo END,
  prompt_id = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.prompt_id ELSE s.prompt_id END,
  tool_use_id = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.tool_use_id ELSE s.tool_use_id END,
  args_present = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.args_present ELSE s.args_present END,
  args_bytes = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.args_bytes ELSE s.args_bytes END,
  harness_version = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.harness_version ELSE s.harness_version END,
  raw_name     = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') OR ((EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) AND (s.plugin = '' AND EXCLUDED.plugin <> '' AND EXCLUDED.skill = s.skill)) THEN EXCLUDED.raw_name ELSE s.raw_name END,
  plugin       = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') OR ((EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) AND (s.plugin = '' AND EXCLUDED.plugin <> '' AND EXCLUDED.skill = s.skill)) THEN EXCLUDED.plugin ELSE s.plugin END,
  skill_source = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') OR ((EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) AND s.skill_source = 'unknown') THEN EXCLUDED.skill_source ELSE s.skill_source END,
  outcome      = CASE WHEN ((EXCLUDED.origin = 'derived' AND s.origin <> 'derived') AND EXCLUDED.outcome <> 'started') OR ((EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) AND EXCLUDED.outcome IN ('success', 'error')) THEN EXCLUDED.outcome ELSE s.outcome END,
  error_class  = CASE WHEN ((EXCLUDED.origin = 'derived' AND s.origin <> 'derived') AND EXCLUDED.outcome <> 'started') OR ((EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) AND EXCLUDED.outcome IN ('success', 'error')) THEN EXCLUDED.error_class ELSE s.error_class END,
  event_id     = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.event_id WHEN (EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) THEN COALESCE(s.event_id, EXCLUDED.event_id) ELSE s.event_id END,
  link_ref     = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') THEN EXCLUDED.link_ref WHEN (EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) THEN COALESCE(s.link_ref, EXCLUDED.link_ref) ELSE s.link_ref END,
  session_type = CASE WHEN (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') OR ((EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) AND s.session_type = '') THEN EXCLUDED.session_type ELSE s.session_type END
WHERE (EXCLUDED.origin = 'derived' AND s.origin <> 'derived') OR ((EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))) AND (s.outcome, s.plugin, s.skill_source, s.event_id, s.link_ref, s.session_type)
  IS DISTINCT FROM (CASE WHEN EXCLUDED.outcome IN ('success', 'error') THEN EXCLUDED.outcome ELSE s.outcome END, CASE WHEN (s.plugin = '' AND EXCLUDED.plugin <> '' AND EXCLUDED.skill = s.skill) THEN EXCLUDED.plugin ELSE s.plugin END, CASE WHEN s.skill_source = 'unknown' THEN EXCLUDED.skill_source ELSE s.skill_source END, COALESCE(s.event_id, EXCLUDED.event_id), COALESCE(s.link_ref, EXCLUDED.link_ref), CASE WHEN s.session_type = '' THEN EXCLUDED.session_type ELSE s.session_type END))
RETURNING (xmax = 0) AS inserted, (s.preempted_by IS NOT NULL OR s.preempted_device IS NOT NULL) AS preempted, s.preempted_by::text, s.preempted_device::text`

func firstDiff(a, b string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) < len(b) {
		return len(a)
	}
	return len(b)
}

func TestRecordAdminActionAndTheRerunStatements(t *testing.T) {
	db := &fakeDB{}
	s, _ := skillStore(db)
	ctx := context.Background()
	since := time.Date(2026, 9, 17, 2, 0, 0, 0, time.UTC)
	if err := s.RecordAdminAction(ctx, db, "boss@example.com", "derive.skill_invocations.rerun", since.Format(time.RFC3339), map[string]any{"since": since}); err != nil {
		t.Fatalf("RecordAdminAction: %v", err)
	}
	c := db.find(t, "INSERT INTO admin_actions (actor, action, target, detail)")
	if argText(c.args[0]) != "boss@example.com" || argText(c.args[1]) != "derive.skill_invocations.rerun" || argText(c.args[2]) != "2026-09-17T02:00:00Z" {
		t.Errorf("audit args = %v", c.args)
	}
	if !strings.Contains(argText(c.args[3]), `"since":"2026-09-17T02:00:00Z"`) {
		t.Errorf("audit detail = %s", argText(c.args[3]))
	}
	if err := s.RecordAdminAction(ctx, db, "boss@example.com", "x", "", nil); err != nil {
		t.Fatal(err)
	}
	if got := argText(db.calls[len(db.calls)-1].args[3]); got != "{}" {
		t.Errorf("nil detail stored as %s, want {}", got)
	}

	if _, err := s.EnqueueSkillRederiveSince(ctx, db, since); err != nil {
		t.Fatal(err)
	}
	c = db.find(t, "SELECT session_id, 'rollback' FROM sessions WHERE updated_at >= $1")
	if !c.args[0].(time.Time).Equal(since) {
		t.Errorf("since = %v", c.args[0])
	}
	if !strings.Contains(c.sql, "ON CONFLICT (session_id) DO UPDATE SET reason = EXCLUDED.reason, attempts = 0") {
		t.Error("a re-queued session is not unparked")
	}

	if err := s.ResetSkillDeriveStep(ctx, db); err != nil {
		t.Fatal(err)
	}
	c = db.find(t, "UPDATE derive_jobs SET finished_at = NULL, started_at = NULL, updated_at = now(), last_error = '', cursor = '', attempts = 0, processed = 0")
	if c.args[0] != DerivedSchema || !strings.Contains(c.sql, "step = 'skill_invocations'") {
		t.Errorf("reset = %s %v", c.sql, c.args)
	}
	// LEAST: a stamp already below N-1 is left where it is (ADV-LS1 F4).
	c = db.find(t, "UPDATE derived_schema SET version = LEAST(version, $1) WHERE only_row")
	if c.args[0] != DerivedSchema-1 {
		t.Errorf("the stamp is lowered to %v, want %d", c.args[0], DerivedSchema-1)
	}
}

// ---------------------------------------------------------------- derivation

// Query 1 and query 2 of design 4a: a Skill call is one merge keyed on its
// tool_use_id with the arguments' presence and size but never their text,
// and its result is one UPDATE of the outcome keyed the same way, never an
// insert.
func TestSkillCallAndItsResultAreOneMergeAndOneUpdate(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	const args = "hello world"
	call := skillCall("evt-call", "sess-skill", "p1", "toolu_1", `{"skill":"engg:git","args":"`+args+`"}`, 1)
	result := skillResult("evt-result", "sess-skill", "toolu_1", event.ToolResult, 2)
	other := skillResult("evt-other", "sess-skill", "toolu_bash", event.ToolResult, 3)
	other.Event.Tool = &event.Tool{Name: "Bash", Output: "ok"}
	// A subagent's Skill call is a row like the main thread's, keyed on
	// its tool-use id (R2's nested-skill trigger; review-1 F2), and its
	// failure is its outcome.
	sub := skillCall("evt-sub", "sess-skill", "", "toolu_sub", `{"skill":"engg:standup"}`, 4)
	sub.Event.AgentID = "researcher"
	subResult := skillResult("evt-subres", "sess-skill", "toolu_sub", event.ToolFailed, 5)
	subResult.Event.AgentID = "researcher"

	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{call, result, other, sub, subResult})
	if err != nil {
		t.Fatalf("insertSkillInvocations: %v", err)
	}
	if st.Candidates != 4 || st.RowsAgent != 2 || st.RowsUser != 0 || st.OutcomeUpdates != 1 || !st.Touched {
		t.Errorf("stats = %+v", st)
	}
	if n := db.count("INSERT INTO skill_invocations"); n != 2 {
		t.Fatalf("%d merges, want 2", n)
	}
	// The live path sends every fresh row to the merge; only the rebuild
	// reads the stored rows first.
	if n := db.count(settledRead); n != 0 {
		t.Errorf("the live path read stored skill rows %d times", n)
	}
	var keys []string
	for _, call := range db.calls {
		if strings.Contains(call.sql, "INSERT INTO skill_invocations") {
			keys = append(keys, argText(call.args[24])+" "+argText(call.args[11]))
		}
	}
	if strings.Join(keys, ",") != "claude_code:sess-skill:t:toolu_1 agent,claude_code:sess-skill:t:toolu_sub agent" {
		t.Errorf("rows written = %v", keys)
	}
	c := db.find(t, "INSERT INTO skill_invocations")
	for i, want := range map[int]string{7: "engg:git", 8: "engg", 9: "git", 10: SkillSourcePlugin, 11: TriggerAgent,
		16: "loop-sessions", 17: "sess-skill", 20: "evt-call", 21: "p1", 22: "toolu_1", 24: "claude_code:sess-skill:t:toolu_1", 27: "2.1.273"} {
		if got := argText(c.args[i]); got != want {
			t.Errorf("arg $%d = %q, want %q", i+1, got, want)
		}
	}
	if c.args[25] != true || c.args[26] != len(args) {
		t.Errorf("args_present, args_bytes = %v, %v, want true, %d", c.args[25], c.args[26], len(args))
	}
	for _, call := range db.calls {
		for i, a := range call.args {
			if strings.Contains(argText(a), args) {
				t.Errorf("the arguments' text reached statement arg %d of %s", i+1, strings.Fields(call.sql)[0])
			}
		}
	}
	u := db.find(t, "UPDATE skill_invocations si")
	if strings.Contains(u.sql, "INSERT") || !strings.Contains(u.sql, "WHERE si.dedupe_key = u.dedupe_key AND si.outcome IN ('started', 'unknown')") {
		t.Errorf("the outcome statement is %s", u.sql)
	}
	// Every result of the batch is in the UPDATE (a transcript result
	// carries no tool name); a key with no started row updates nothing.
	keys, outcomes := u.args[0].([]string), u.args[1].([]string)
	if strings.Join(keys, ",") != "claude_code:sess-skill:t:toolu_1,claude_code:sess-skill:t:toolu_bash,claude_code:sess-skill:t:toolu_sub" ||
		strings.Join(outcomes, ",") != OutcomeSuccess+","+OutcomeSuccess+","+OutcomeError {
		t.Errorf("outcome update keyed %v -> %v", keys, outcomes)
	}

	// A failure is an error outcome.
	db2 := &fakeDB{}
	s2, _ := skillStore(db2)
	if _, err := s2.insertSkillInvocations(context.Background(), db2, []Ingest{skillResult("evt-f", "sess-skill", "toolu_1", event.ToolFailed, 4)}); err != nil {
		t.Fatal(err)
	}
	if got := db2.find(t, "UPDATE skill_invocations si").args[1].([]string); got[0] != OutcomeError {
		t.Errorf("a tool_failed updates to %v, want error", got)
	}
}

// A transcript Skill call and its nameless result carry their ids only in
// Raw; keysFor reads them there so ingest and rebuild key alike.
func TestTranscriptSkillCallIsKeyedFromRaw(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	call := ingestOf("evt-tcall", "sess-raw", skillTestEmail, event.ToolCall, 1)
	call.Event.Origin = event.OriginTranscript
	call.Event.Tool = &event.Tool{Name: "Skill", Input: json.RawMessage(`{"skill":"probe-skill"}`)}
	call.Event.Raw = json.RawMessage(`{"type":"assistant","uuid":"u1","message":{"id":"msg_1","role":"assistant","content":[{"type":"tool_use","id":"toolu_raw","name":"Skill","input":{"skill":"probe-skill"}}]}}`)
	result := ingestOf("evt-tresult", "sess-raw", skillTestEmail, event.ToolResult, 2)
	result.Event.Origin = event.OriginTranscript
	result.Event.Tool = &event.Tool{Output: "Launching skill: probe-skill"}
	result.Event.Raw = json.RawMessage(`{"type":"user","uuid":"u2","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_raw","content":"Launching skill: probe-skill"}]}}`)

	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{call, result})
	if err != nil {
		t.Fatal(err)
	}
	if st.RowsAgent != 1 || st.OutcomeUpdates != 1 {
		t.Errorf("stats = %+v", st)
	}
	if got := argText(db.find(t, "INSERT INTO skill_invocations").args[24]); got != "claude_code:sess-raw:t:toolu_raw" {
		t.Errorf("key = %q", got)
	}
	if got := db.find(t, "UPDATE skill_invocations si").args[0].([]string); got[0] != "claude_code:sess-raw:t:toolu_raw" {
		t.Errorf("outcome keyed %v", got)
	}
}

// Coercion before the insert (CODEBASE1-1): a cwd outside the repo CHECK
// stores ”, an id the key bound drops is one skip, and neither is an
// error.
func TestSkillDerivationCoercesRepoAndCountsAnOverlongID(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	call := skillCall("evt-a", "sess-co", "p1", "toolu_1", `{"skill":"git"}`, 1)
	call.Repo = "My Project"
	long := skillCall("evt-b", "sess-co", "p1", strings.Repeat("x", 300), `{"skill":"git"}`, 2)

	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{call, long})
	if err != nil {
		t.Fatalf("insertSkillInvocations: %v", err)
	}
	if st.Candidates != 2 || st.SkippedShape != 1 || st.RowsAgent != 1 {
		t.Errorf("stats = %+v, want 2 candidates, 1 skipped, 1 row", st)
	}
	if n := db.count("INSERT INTO skill_invocations"); n != 1 {
		t.Errorf("%d merges, want 1", n)
	}
	if got := argText(db.find(t, "INSERT INTO skill_invocations").args[16]); got != "" {
		t.Errorf("repo = %q, want '' for a cwd outside the CHECK", got)
	}
}

// Query 3, both copies of one typed command in one batch: the typed hook
// line and the transcript envelope share one form-2 key, fold to one row
// that carries the envelope's canonical name, and the copy absorbed counts
// as a duplicate on the 7.1 line.
func TestTypedCommandBothCopiesFoldToOneRow(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	s.AliasOracle = func(_ context.Context, _ Queryer, plugin, skill string) (bool, error) {
		return plugin == "" && skill == "git", nil
	}
	hook := typedHook("evt-h", "sess-typed", "p1", "/git ship it", 1)
	tr := typedTranscript("evt-t", "sess-typed", "p1", "engg:git", "ship it", 2)

	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{hook, tr})
	if err != nil {
		t.Fatal(err)
	}
	if st.Candidates != 2 || st.RowsUser != 1 || st.Duplicates != 1 || st.Unconfirmed != 0 {
		t.Errorf("stats = %+v, want 2 candidates folded to 1 row and 1 duplicate", st)
	}
	if n := db.count("INSERT INTO skill_invocations"); n != 1 {
		t.Fatalf("%d merges, want 1", n)
	}
	c := db.find(t, "INSERT INTO skill_invocations")
	for i, want := range map[int]string{7: "engg:git", 8: "engg", 9: "git", 11: TriggerUser, 12: OutcomeStarted, 21: "p1", 24: "claude_code:sess-typed:p:p1"} {
		if got := argText(c.args[i]); got != want {
			t.Errorf("arg $%d = %q, want %q", i+1, got, want)
		}
	}
	if c.args[25] != true || c.args[26] != len("ship it") {
		t.Errorf("args_present, args_bytes = %v, %v", c.args[25], c.args[26])
	}
	for _, call := range db.calls {
		for _, a := range call.args {
			if strings.Contains(argText(a), "ship it") {
				t.Error("the typed arguments reached a statement")
			}
		}
	}

	// Under a nil oracle (LS-1 as shipped) the typed hook copy is
	// unconfirmed and the transcript copy alone carries the row.
	db2 := &fakeDB{stubs: []*stub{insertedRow()}}
	s2, _ := skillStore(db2)
	s2.AliasOracle = nil
	st, err = s2.insertSkillInvocations(context.Background(), db2, []Ingest{hook, tr})
	if err != nil {
		t.Fatal(err)
	}
	if st.Candidates != 2 || st.RowsUser != 1 || st.Unconfirmed != 1 || st.Duplicates != 0 {
		t.Errorf("nil-oracle stats = %+v", st)
	}
	if got := argText(db2.find(t, "INSERT INTO skill_invocations").args[7]); got != "engg:git" {
		t.Errorf("raw_name = %q", got)
	}
}

func TestTypedCommandDropsBuiltinsAndHookCopiesWithoutAPromptID(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) { return true, nil }
	batch := []Ingest{
		typedHook("evt-1", "sess-b", "p1", "/compact", 1),
		typedTranscript("evt-2", "sess-b", "p2", "clear", "", 2),
		// No prompt id and no Raw for event_keys to fill from: never a row.
		typedHook("evt-3", "sess-b", "", "/git go", 3),
		// The built-in list applies to bare names only.
		typedTranscript("evt-4", "sess-b", "p4", "ralph-loop:help", "", 4),
		// A typed /plan is a row the unknown report shows (design 3.5).
		typedHook("evt-5", "sess-b", "p5", "/plan the thing", 5),
	}
	// A subagent's prompt is the harness handing it its task: not a
	// candidate on either copy (design 3.6 (2)).
	subHook := typedHook("evt-6", "sess-b", "p6", "/git go", 6)
	subHook.Event.AgentID = "researcher"
	subTranscript := typedTranscript("evt-7", "sess-b", "p7", "engg:git", "go", 7)
	subTranscript.Event.AgentID = "researcher"
	batch = append(batch, subHook, subTranscript)
	st, err := s.insertSkillInvocations(context.Background(), db, batch)
	if err != nil {
		t.Fatal(err)
	}
	if st.Candidates != 5 || st.SkippedBuiltin != 2 || st.RowsUser != 2 || st.Unconfirmed != 0 {
		t.Errorf("stats = %+v, want 5 candidates, 2 built-ins, 2 rows", st)
	}
	var names []string
	for _, c := range db.calls {
		if strings.Contains(c.sql, "INSERT INTO skill_invocations") {
			names = append(names, argText(c.args[7]))
		}
	}
	if strings.Join(names, ",") != "ralph-loop:help,plan" {
		t.Errorf("rows written for %v", names)
	}
}

// The 4a pairing rule (CA-12), in the batch: a transcript copy without a
// prompt_id adopts the prompt_id of the nearest same-name hook copy of its
// session within 120 s and keys form 2; each hook copy pairs once, so a
// second orphan keys 2c.
func TestPairingAdoptsTheNearestHookCopyInTheBatchOnce(t *testing.T) {
	base := at
	// The batch's events are in the table already when the stored copies
	// are read, so the read hands the batch's own hook copy back; it was
	// offered in the pass over the batch and pairs no second time.
	db := &fakeDB{stubs: []*stub{insertedRow(),
		{match: settledRead, rows: nil},
		{match: storedCopiesRead, rows: [][]any{{"evt-h", "hook", "sess-pair", "p9", base, "/git go"}}}}}
	s, _ := skillStore(db)
	s.AliasOracle = nil
	hook := typedHook("evt-h", "sess-pair", "p9", "/git go", 1)
	hook.Event.OccurredAt = base
	near := typedTranscript("evt-near", "sess-pair", "", "git", "go", 2)
	near.Event.OccurredAt = base.Add(time.Second)
	far := typedTranscript("evt-far", "sess-pair", "", "git", "again", 3)
	far.Event.OccurredAt = base.Add(90 * time.Second)
	// A different name in the window pairs with nothing.
	otherName := typedTranscript("evt-other", "sess-pair", "", "standup", "", 4)
	otherName.Event.OccurredAt = base.Add(2 * time.Second)

	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{far, hook, near, otherName})
	if err != nil {
		t.Fatal(err)
	}
	if st.RowsUser != 3 || st.Unconfirmed != 1 {
		t.Errorf("stats = %+v, want three rows and the typed hook copy unconfirmed", st)
	}
	// The two copies the batch could not pair are looked for in the store,
	// in one read over the batch's session.
	if n := db.count("FROM events"); n != 1 {
		t.Errorf("%d stored-copy reads, want 1 for the two unpaired copies", n)
	}
	if got := db.find(t, "FROM events").args[0].([]string); len(got) != 1 || got[0] != "sess-pair" {
		t.Errorf("stored copies read for %v", got)
	}
	var keys []string
	for _, c := range db.calls {
		if strings.Contains(c.sql, "INSERT INTO skill_invocations") {
			keys = append(keys, argText(c.args[24]))
		}
	}
	want := []string{"claude_code:sess-pair:e:evt-far", "claude_code:sess-pair:e:evt-other", "claude_code:sess-pair:p:p9"}
	if strings.Join(keys, " ") != strings.Join(want, " ") {
		t.Errorf("keys written (sorted) = %v, want %v", keys, want)
	}
}

// storedCopiesRead and settledRead name the two reads the derivation makes
// beside the merge: the stored copies of the batch's sessions for the
// pairing, and (the rebuild only) the stored rows of the keys it derived.
const (
	storedCopiesRead = "origin = 'transcript' AND coalesce(prompt_id, '') = ''"
	settledRead      = "FROM skill_invocations WHERE dedupe_key = ANY($1::text[]) AND origin = 'derived'"
)

// The same rule over stored hook copies: events has no text column, so
// the hook copy's name is read from its stored body through SlashCommand.
func TestPairingReadsStoredHookCopiesOfTheBatchSessions(t *testing.T) {
	// The envelope is canonical and the stored hook copy is the typed short
	// form (G8): they are one command.
	orphan := typedTranscript("evt-orphan", "sess-stored", "", "engg:git", "go", 1)
	orphan.Event.OccurredAt = at
	db := &fakeDB{stubs: []*stub{
		insertedRow(),
		{match: storedCopiesRead, rows: [][]any{
			{"evt-far", "hook", "sess-stored", "p-far", at.Add(-100 * time.Second), "/git earlier"},
			{"evt-near", "hook", "sess-stored", "p-near", at.Add(3 * time.Second), "/git go"},
			{"evt-prose", "hook", "sess-stored", "p-prose", at.Add(time.Second), "git is not a command here"},
		}},
	}}
	s, _ := skillStore(db)
	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{orphan})
	if err != nil {
		t.Fatal(err)
	}
	if st.RowsUser != 1 {
		t.Errorf("stats = %+v", st)
	}
	q := db.find(t, storedCopiesRead)
	if got := q.args[0].([]string); len(got) != 1 || got[0] != "sess-stored" {
		t.Errorf("stored copies read for %v, want the batch's session", got)
	}
	// Twice the window: a stored competitor within the window of a hook
	// copy that is itself within the window of the orphan.
	if lo, hi := q.args[1].(time.Time), q.args[2].(time.Time); !lo.Equal(at.Add(-2*pairWindow)) || !hi.Equal(at.Add(2*pairWindow)) {
		t.Errorf("window = [%v, %v]", lo, hi)
	}
	if got := argText(db.find(t, "INSERT INTO skill_invocations").args[24]); got != "claude_code:sess-stored:p:p-near" {
		t.Errorf("key = %q, want the nearest stored hook copy's prompt id", got)
	}
}

// Each hook copy pairs once across batches too (review-1 F7): a stored
// hook copy that a stored transcript copy without a prompt id sits nearer
// to is that copy's (the rebuild pairs them, design 3.6), so an orphan of
// a later batch keys 2c rather than adopting it a second time; the same
// hook copy with no nearer competitor is the orphan's.
func TestPairingLeavesAStoredHookCopyToItsNearerStoredOrphan(t *testing.T) {
	earlier := typedTranscript("evt-t1", "sess-x", "", "engg:git", "go", 1)
	hookRow := []any{"evt-h1", "hook", "sess-x", "p1", at, "/git go"}
	competitor := func(gap time.Duration) []any {
		return []any{"evt-t1", "transcript", "sess-x", "", at.Add(gap), string(earlier.Event.Raw)}
	}
	for _, tc := range []struct {
		name string
		rows [][]any
		want string
	}{
		{"a nearer stored orphan keeps the hook copy", [][]any{hookRow, competitor(time.Second)}, "claude_code:sess-x:e:evt-t2"},
		{"no competitor: the orphan adopts it", [][]any{hookRow}, "claude_code:sess-x:p:p1"},
		{"a farther stored orphan yields", [][]any{hookRow, competitor(100 * time.Second)}, "claude_code:sess-x:p:p1"},
		{"a competitor outside the hook copy's window is none", [][]any{hookRow, competitor(-121 * time.Second)}, "claude_code:sess-x:p:p1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &fakeDB{stubs: []*stub{insertedRow(), {match: storedCopiesRead, rows: tc.rows}}}
			s, _ := skillStore(db)
			orphan := typedTranscript("evt-t2", "sess-x", "", "engg:git", "again", 2)
			orphan.Event.OccurredAt = at.Add(30 * time.Second)
			st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{orphan})
			if err != nil {
				t.Fatal(err)
			}
			if st.RowsUser != 1 {
				t.Errorf("stats = %+v", st)
			}
			if got := argText(db.find(t, "INSERT INTO skill_invocations").args[24]); got != tc.want {
				t.Errorf("key = %q, want %q", got, tc.want)
			}
		})
	}
}

// No hook copy in the batch or the store: the copy keys 2c on its event id
// (uploads and backfills key 2c throughout).
func TestPairingLeavesAnUnpairedCopyOnItsEventID(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	orphan := typedTranscript("evt-alone", "sess-alone", "", "engg:git", "", 1)
	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{orphan})
	if err != nil {
		t.Fatal(err)
	}
	if st.RowsUser != 1 {
		t.Errorf("stats = %+v", st)
	}
	c := db.find(t, "INSERT INTO skill_invocations")
	if got := argText(c.args[24]); got != "claude_code:sess-alone:e:evt-alone" {
		t.Errorf("key = %q, want form 2c", got)
	}
	if c.args[21] != (*string)(nil) {
		t.Errorf("prompt_id = %v, want NULL", c.args[21])
	}
	if n := db.count("FROM events"); n != 1 {
		t.Errorf("%d stored-copy reads, want 1", n)
	}
}

// A hook copy whose transcript twin arrived in an earlier batch (ADV-LS1
// F7): the twin keyed 2c there, the hook copy keys form 2 here, and only a
// rebuild folds the two, so the batch queues the session with reason twin
// when a 2c row of the same session and skill sits within the window. A
// hook copy with its twin in the batch, a built-in, a stored row of another
// skill or outside the window, and the rebuild itself queue nothing.
func TestALoneHookCopyQueuesItsSessionWhenItsTwinIsAStored2cRow(t *testing.T) {
	const twinRead = "AND prompt_id IS NULL AND tool_use_id IS NULL AND skill IS NOT NULL"
	hook := typedHook("evt-h", "sess-tw", "p1", "/git go", 1)
	hook.Event.OccurredAt = at
	for _, tc := range []struct {
		name  string
		batch []Ingest
		rows  [][]any
		reads int
		want  string // the queued session, or none
	}{
		{"a stored 2c row in the window queues", []Ingest{hook}, [][]any{{"sess-tw", "git", at.Add(-40 * time.Second)}}, 1, "sess-tw"},
		{"outside the window", []Ingest{hook}, [][]any{{"sess-tw", "git", at.Add(-121 * time.Second)}}, 1, ""},
		{"another skill", []Ingest{hook}, [][]any{{"sess-tw", "standup", at}}, 1, ""},
		{"no stored row", []Ingest{hook}, nil, 1, ""},
		{"the twin in the batch", []Ingest{hook, typedTranscript("evt-t", "sess-tw", "p1", "engg:git", "go", 2)}, nil, 0, ""},
		{"a built-in", []Ingest{typedHook("evt-b", "sess-tw", "p2", "/compact", 3)}, nil, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &fakeDB{stubs: []*stub{insertedRow(), {match: twinRead, rows: tc.rows}}}
			s, _ := skillStore(db)
			s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) { return true, nil }
			if _, err := s.insertSkillInvocations(context.Background(), db, tc.batch); err != nil {
				t.Fatal(err)
			}
			if n := db.count(twinRead); n != tc.reads {
				t.Errorf("%d twin reads, want %d", n, tc.reads)
			}
			if tc.reads == 1 {
				r := db.find(t, twinRead)
				if got := r.args[0].([]string); len(got) != 1 || got[0] != "sess-tw" {
					t.Errorf("twins read for %v, want the batch's session", got)
				}
				if lo, hi := r.args[1].(time.Time), r.args[2].(time.Time); !lo.Equal(at.Add(-pairWindow)) || !hi.Equal(at.Add(pairWindow)) {
					t.Errorf("window = [%v, %v]", lo, hi)
				}
			}
			if tc.want == "" {
				if db.count("INSERT INTO skill_rederive_queue") != 0 {
					t.Errorf("queued a session:\n%s", db.summary())
				}
				return
			}
			q := db.find(t, "INSERT INTO skill_rederive_queue")
			if got := q.args[0].([]string); len(got) != 1 || got[0] != tc.want || argText(q.args[1]) != "twin" {
				t.Errorf("queued %v with reason %v, want %s with reason twin", q.args[0], q.args[1], tc.want)
			}
		})
	}

	// The rebuild's batch is the whole session: its pairing is the final
	// word and it looks for no stored twin.
	body, err := json.Marshal(hook.Event)
	if err != nil {
		t.Fatal(err)
	}
	db := &fakeDB{stubs: []*stub{insertedRow(),
		{match: "body_expired_at IS NOT NULL", rows: [][]any{{false}}},
		{match: "SELECT source, coalesce(repo, '')", rows: [][]any{{"claude_code", "loop-sessions", skillTestDevice}}},
		{match: "(type = 'tool_call' AND tool_name = 'Skill')", rows: [][]any{{"evt-h", skillTestEmail, int64(1), at, body}}}}}
	s, _ := skillStore(db)
	s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) { return true, nil }
	if _, err := s.rebuildSessionSkillInvocations(context.Background(), db, "sess-tw"); err != nil {
		t.Fatal(err)
	}
	if db.count(twinRead) != 0 || db.count("INSERT INTO skill_rederive_queue") != 0 {
		t.Errorf("the rebuild looked for stored twins:\n%s", db.summary())
	}
}

// The step's prefilter (design 4a): the rebuild reads the stored derived
// rows of the keys it derived, in one query, and sends the merge only the
// keys whose row an M arm would move, since ON CONFLICT DO UPDATE locks
// the row for the rest of the fifty-session transaction even when its
// WHERE is false (design 3.6; review-1 F5).
func TestRebuildSendsTheMergeOnlyTheKeysItWouldMove(t *testing.T) {
	body := func(it Ingest) []byte {
		b, err := json.Marshal(it.Event)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	settledCall := skillCall("evt-a", "sess-r", "p1", "toolu_a", `{"skill":"engg:git"}`, 1)
	movedCall := skillCall("evt-b", "sess-r", "p2", "toolu_b", `{"skill":"engg:git"}`, 2)
	newCall := skillCall("evt-c", "sess-r", "p3", "toolu_c", `{"skill":"engg:git"}`, 3)
	db := &fakeDB{stubs: []*stub{
		insertedRow(),
		{match: "body_expired_at IS NOT NULL", rows: [][]any{{false}}},
		{match: "SELECT source, coalesce(repo, '')", rows: [][]any{{"claude_code", "loop-sessions", skillTestDevice}}},
		{match: "(type = 'tool_call' AND tool_name = 'Skill')", rows: [][]any{
			{"evt-a", skillTestEmail, int64(1), at, body(settledCall)},
			{"evt-b", skillTestEmail, int64(2), at.Add(time.Second), body(movedCall)},
			{"evt-c", skillTestEmail, int64(3), at.Add(2 * time.Second), body(newCall)},
		}},
		{match: settledRead, rows: [][]any{
			// Equal on the six columns the merge's WHERE compares: settled.
			{"claude_code:sess-r:t:toolu_a", OutcomeStarted, "engg", "git", SkillSourcePlugin, "evt-a", nil, ""},
			// event_id NULL where the derived copy carries one: the M arm
			// COALESCEs it in, so the merge runs.
			{"claude_code:sess-r:t:toolu_b", OutcomeStarted, "engg", "git", SkillSourcePlugin, nil, nil, ""},
		}},
	}}
	s, _ := skillStore(db)
	n, err := s.rebuildSessionSkillInvocations(context.Background(), db, "sess-r")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if n != 2 {
		t.Errorf("rebuilt %d rows, want the moved and the new one", n)
	}
	read := db.find(t, settledRead)
	if got := read.args[0].([]string); strings.Join(got, ",") != "claude_code:sess-r:t:toolu_a,claude_code:sess-r:t:toolu_b,claude_code:sess-r:t:toolu_c" {
		t.Errorf("stored rows read for %v", got)
	}
	var keys []string
	for _, c := range db.calls {
		if strings.Contains(c.sql, "INSERT INTO skill_invocations") {
			keys = append(keys, argText(c.args[24]))
		}
	}
	if strings.Join(keys, ",") != "claude_code:sess-r:t:toolu_b,claude_code:sess-r:t:toolu_c" {
		t.Errorf("keys sent to the merge = %v, want the moved and the new one", keys)
	}
	// (3b) removes a 2c row this rebuild did not regenerate only when its
	// event is still stored (ADV-LS1 F6).
	del := db.find(t, "DELETE FROM skill_invocations si")
	if argText(del.args[0]) != "sess-r" || !strings.Contains(del.sql, "si.dedupe_key <> ALL($2::text[])") ||
		!strings.Contains(del.sql, "AND EXISTS (SELECT 1 FROM events e WHERE e.session_id = $1 AND e.id = si.event_id)") {
		t.Errorf("(3b) = %s %v", del.sql, del.args)
	}

	// The other M arms, one at a time on the settled row: each moves it.
	for name, stored := range map[string][]any{
		"plugin (N)":   {"claude_code:sess-r:t:toolu_a", OutcomeStarted, "", "git", SkillSourcePlugin, "evt-a", nil, ""},
		"skill_source": {"claude_code:sess-r:t:toolu_a", OutcomeStarted, "engg", "git", SkillSourceUnknown, "evt-a", nil, ""},
	} {
		db := &fakeDB{stubs: []*stub{insertedRow(),
			{match: "body_expired_at IS NOT NULL", rows: [][]any{{false}}},
			{match: "SELECT source, coalesce(repo, '')", rows: [][]any{{"claude_code", "loop-sessions", skillTestDevice}}},
			{match: "(type = 'tool_call' AND tool_name = 'Skill')", rows: [][]any{{"evt-a", skillTestEmail, int64(1), at, body(settledCall)}}},
			{match: settledRead, rows: [][]any{stored}}}}
		s, _ := skillStore(db)
		if n, err := s.rebuildSessionSkillInvocations(context.Background(), db, "sess-r"); err != nil || n != 1 {
			t.Errorf("%s: rebuilt %d, %v, want the row sent to the merge", name, n, err)
		}
	}
}

// The Codex branch (G4): $skill mentions on a person's own text only, one
// 2b row per distinct mention, kept only when the oracle confirms the name.
func TestCodexMentionsAreConfirmedRowsOnHumanTextOnly(t *testing.T) {
	mention := func(id, text string) Ingest {
		it := ingestOf(id, "sess-codex", skillTestEmail, event.UserPrompt, 1)
		it.Event.Source, it.Event.Origin, it.Event.Text = event.SourceCodex, event.OriginTranscript, text
		return it
	}
	db := &fakeDB{stubs: []*stub{insertedRow(), insertedRow()}}
	s, _ := skillStore(db)
	s.AliasOracle = nil
	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{mention("evt-c", "use $git and $git-review, then $git again; $PATH is shell")})
	if err != nil {
		t.Fatal(err)
	}
	if st.Candidates != 2 || st.Unconfirmed != 2 || st.RowsUser != 0 {
		t.Errorf("nil-oracle stats = %+v, want two unconfirmed mentions and no row", st)
	}

	db = &fakeDB{stubs: []*stub{insertedRow(), insertedRow()}}
	s, _ = skillStore(db)
	s.AliasOracle = func(_ context.Context, _ Queryer, plugin, skill string) (bool, error) { return skill == "git", nil }
	st, err = s.insertSkillInvocations(context.Background(), db, []Ingest{mention("evt-c", "use $git and $git-review")})
	if err != nil {
		t.Fatal(err)
	}
	if st.Candidates != 2 || st.Unconfirmed != 1 || st.RowsUser != 1 {
		t.Errorf("stats = %+v", st)
	}
	c := db.find(t, "INSERT INTO skill_invocations")
	for i, want := range map[int]string{3: PlatformCodex, 7: "git", 10: SkillSourceMirror, 11: TriggerUser, 24: "codex:sess-codex:e:evt-c:git"} {
		if got := argText(c.args[i]); got != want {
			t.Errorf("arg $%d = %q, want %q", i+1, got, want)
		}
	}

	// Not a person's text: an interruption, a slash command, a Claude
	// Code prompt.
	db = &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ = skillStore(db)
	s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) { return true, nil }
	cc := typedHook("evt-cc", "sess-cc", "p1", "please run $git", 2)
	st, err = s.insertSkillInvocations(context.Background(), db, []Ingest{mention("evt-i", "[Request interrupted by user] $git"), cc})
	if err != nil {
		t.Fatal(err)
	}
	if st.Candidates != 0 || db.count("INSERT INTO skill_invocations") != 0 {
		t.Errorf("stats = %+v with %d merges, want nothing derived", st, db.count("INSERT INTO skill_invocations"))
	}
}

// A failed oracle READ gives up the derivation; it is never counted as a
// name the catalog does not know. The two are opposite facts: an
// unconfirmed name is dropped for good, while a failed read has to come
// back, and the savepoint path is what brings it back (adversarial
// iteration 5, finding 2). The oracle also reads on the Queryer the
// derivation was handed, never on the store's pool.
func TestAFailedOracleReadEndsTheDeriveAndIsNotAnUnconfirmedName(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	var sawQ Queryer
	s.AliasOracle = func(_ context.Context, q Queryer, _, _ string) (bool, error) {
		sawQ = q
		return false, errors.New("connection refused")
	}
	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{typedHook("evt-o", "sess-o", "p1", "/git go", 1)})
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("insertSkillInvocations err = %v, want the oracle's read error", err)
	}
	if sawQ != Queryer(db) {
		t.Error("the oracle was not handed the derivation's own Queryer")
	}
	if st.OracleErrors != 1 || st.Unconfirmed != 0 {
		t.Errorf("stats = %+v, want one oracle error and no unconfirmed name", st)
	}
	if n := db.count("INSERT INTO skill_invocations"); n != 0 {
		t.Errorf("%d merges behind a failed oracle read, want none", n)
	}
}

// oracle_errors is one of the derived line's counters (design 7.1).
func TestTheDerivedLineCarriesOracleErrors(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, buf := skillStore(db)
	s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) { return true, nil }
	s.deriveSkillsAtIngest(context.Background(), db, []Ingest{typedHook("evt-l", "sess-l", "p1", "/git go", 1)})
	lines := linesWithMessage(skillLines(t, buf), "skill invocations derived")
	if len(lines) != 1 {
		t.Fatalf("derived lines = %v", lines)
	}
	if _, ok := lines[0]["oracle_errors"]; !ok {
		t.Errorf("the derived line has no oracle_errors field: %v", lines[0])
	}
}

func TestEnvelopeNamesNeverConsultTheOracle(t *testing.T) {
	db := &fakeDB{stubs: []*stub{insertedRow()}}
	s, _ := skillStore(db)
	s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) {
		t.Error("the oracle was consulted for a name the transcript envelope carries")
		return false, nil
	}
	st, err := s.insertSkillInvocations(context.Background(), db, []Ingest{typedTranscript("evt-e", "sess-e", "p1", "engg:git", "", 1)})
	if err != nil {
		t.Fatal(err)
	}
	if st.RowsUser != 1 || st.Unconfirmed != 0 {
		t.Errorf("stats = %+v", st)
	}
}

// ---------------------------------------------------------------- the ingest block

// The savepoint block UpsertEvents runs and the 7.1 lines it writes. Three
// endings: the skill work released with its counters logged, a statement
// failure rolled back to the savepoint with the ERROR line behind 17c and
// the sessions queued, and a lock wait rolled back the same way under the
// WARNING line. On every path the lock bound is reset last, because a SET
// LOCAL outlives RELEASE SAVEPOINT until commit. No line carries a token,
// an argument or prompt text.
func TestDeriveSkillsAtIngestLinesAndSavepoint(t *testing.T) {
	const secretArgs = "hello lss_SECRETSECRETSECRETSECRETSECRET1"
	const secretPrompt = "deploy with lsd_TOKENTOKENTOKENTOKENTOKEN"
	batch := func() []Ingest {
		return []Ingest{
			skillCall("evt-call", "sess-log", "p1", "toolu_1", `{"skill":"engg:git","args":"`+secretArgs+`"}`, 1),
			typedHook("evt-h", "sess-log", "p2", "/git "+secretPrompt, 2),
			typedTranscript("evt-t", "sess-log", "p2", "engg:git", secretPrompt, 3),
		}
	}
	forbidden := []string{"SECRET", "TOKEN", "hello", "deploy with", "engg:git"}
	assertClean := func(t *testing.T, lines []map[string]any) {
		t.Helper()
		for _, l := range lines {
			b, _ := json.Marshal(l)
			for _, f := range forbidden {
				if strings.Contains(string(b), f) {
					t.Errorf("line %s carries %q", b, f)
				}
			}
		}
	}
	lastSQL := func(db *fakeDB) string { return db.calls[len(db.calls)-1].sql }

	t.Run("derived", func(t *testing.T) {
		db := &fakeDB{stubs: []*stub{insertedRow(), insertedRow()}}
		s, buf := skillStore(db)
		s.AliasOracle = nil
		s.deriveSkillsAtIngest(context.Background(), &fakeTx{db: db}, batch())
		lines := skillLines(t, buf)
		got := linesWithMessage(lines, "skill invocations derived")
		if len(got) != 1 {
			t.Fatalf("%d derived lines, want 1: %s", len(got), buf.String())
		}
		l := got[0]
		if l["level"] != "INFO" || l["email"] != skillTestEmail || l["device_id"] != skillTestDevice || l["agent_platform"] != PlatformClaudeCode {
			t.Errorf("derived line = %v", l)
		}
		for _, k := range []string{"candidates", "rows_user", "rows_agent", "outcome_updates", "duplicates", "skipped_builtin", "skipped_shape", "unconfirmed", "oracle_errors", "touched"} {
			if _, ok := l[k]; !ok {
				t.Errorf("derived line lacks %q", k)
			}
		}
		if l["candidates"] != float64(3) || l["rows_agent"] != float64(1) || l["rows_user"] != float64(1) || l["unconfirmed"] != float64(1) || l["touched"] != true {
			t.Errorf("derived counters = %v", l)
		}
		assertClean(t, lines)
		if db.count("ROLLBACK TO SAVEPOINT") != 0 || db.count("RELEASE SAVEPOINT skill_inv") != 1 || db.count("SET LOCAL lock_timeout = '2s'") != 1 {
			t.Errorf("savepoint statements:\n%s", db.summary())
		}
		if lastSQL(db) != "SET LOCAL lock_timeout = 0" {
			t.Errorf("the block ended with %q, want the lock bound reset", lastSQL(db))
		}
		if db.count("skill_rederive_queue") != 0 {
			t.Error("a successful block queued its sessions")
		}
	})

	t.Run("statement failed", func(t *testing.T) {
		db := &fakeDB{stubs: []*stub{{match: "INSERT INTO skill_invocations", err: errors.New("boom: constraint")}}}
		s, buf := skillStore(db)
		s.AliasOracle = nil
		s.deriveSkillsAtIngest(context.Background(), &fakeTx{db: db}, batch())
		lines := skillLines(t, buf)
		got := linesWithMessage(lines, "skill invocation store failed")
		if len(got) != 1 {
			t.Fatalf("%d store-failed lines, want 1: %s", len(got), buf.String())
		}
		l := got[0]
		if l["level"] != "ERROR" || l["op"] != "derive" || l["method"] != "POST" || l["path"] != "/v1/events" || l["platform"] != PlatformClaudeCode || !strings.Contains(l["error"].(string), "boom") {
			t.Errorf("store-failed line = %v", l)
		}
		if len(linesWithMessage(lines, "skill invocation deferred")) != 0 || len(linesWithMessage(lines, "skill invocations derived")) != 0 {
			t.Errorf("a failure logged other lines: %s", buf.String())
		}
		assertClean(t, lines)
		if db.count("ROLLBACK TO SAVEPOINT skill_inv") != 1 || db.count("RELEASE SAVEPOINT skill_inv") != 0 {
			t.Errorf("savepoint statements:\n%s", db.summary())
		}
		q := db.find(t, "INSERT INTO skill_rederive_queue")
		if got := q.args[0].([]string); len(got) != 1 || got[0] != "sess-log" || argText(q.args[1]) != "ingest" {
			t.Errorf("queued %v with reason %v", q.args[0], q.args[1])
		}
		if db.count("RELEASE SAVEPOINT skill_q") != 1 || db.count("ROLLBACK TO SAVEPOINT skill_q") != 0 || db.calls[len(db.calls)-4].sql != "SAVEPOINT skill_q" {
			t.Errorf("the queue write ran outside its own savepoint:\n%s", db.summary())
		}
		if lastSQL(db) != "SET LOCAL lock_timeout = 0" {
			t.Errorf("the block ended with %q, want the lock bound reset", lastSQL(db))
		}
	})

	t.Run("lock wait deferred", func(t *testing.T) {
		for _, code := range []string{"55P03", "40P01"} {
			db := &fakeDB{stubs: []*stub{{match: "INSERT INTO skill_invocations", err: &pgconn.PgError{Code: code, Message: "lock"}}}}
			s, buf := skillStore(db)
			s.AliasOracle = nil
			s.deriveSkillsAtIngest(context.Background(), &fakeTx{db: db}, batch())
			lines := skillLines(t, buf)
			got := linesWithMessage(lines, "skill invocation deferred")
			if len(got) != 1 {
				t.Fatalf("%s: %d deferred lines, want 1: %s", code, len(got), buf.String())
			}
			l := got[0]
			if l["level"] != "WARN" || l["email"] != skillTestEmail || l["device_id"] != skillTestDevice || l["sqlstate"] != code || l["sessions"] != float64(1) {
				t.Errorf("deferred line = %v", l)
			}
			if len(linesWithMessage(lines, "skill invocation store failed")) != 0 {
				t.Errorf("%s: a lock wait logged the alert line", code)
			}
			assertClean(t, lines)
			if db.count("ROLLBACK TO SAVEPOINT skill_inv") != 1 || db.count("INSERT INTO skill_rederive_queue") != 1 || lastSQL(db) != "SET LOCAL lock_timeout = 0" {
				t.Errorf("%s statements:\n%s", code, db.summary())
			}
		}
	})

	// A panic inside the derivation (a Raw shape the parser did not
	// expect; here the oracle stands in for it) is contained: the block
	// takes the failure path, the events commit, nothing escapes.
	t.Run("panic contained", func(t *testing.T) {
		db := &fakeDB{}
		s, buf := skillStore(db)
		s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) { panic("unexpected shape") }
		s.deriveSkillsAtIngest(context.Background(), &fakeTx{db: db}, batch())
		got := linesWithMessage(skillLines(t, buf), "skill invocation store failed")
		if len(got) != 1 || !strings.Contains(got[0]["error"].(string), "skill derive panic: unexpected shape") {
			t.Fatalf("panic lines = %v", got)
		}
		if db.count("ROLLBACK TO SAVEPOINT skill_inv") != 1 || db.count("INSERT INTO skill_rederive_queue") != 1 || lastSQL(db) != "SET LOCAL lock_timeout = 0" {
			t.Errorf("statements:\n%s", db.summary())
		}
	})
}

// The 7.1 message strings LS-1 emits, exactly, since log metrics match
// message text. "derive step" is the runner's existing line
// (TestDeriveLogLinesCarryTheFieldsTheMetricsRead).
func TestSkillLogMessagesAreTheContractStrings(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "INSERT INTO skill_invocations", err: errors.New("boom"), once: true},
		{match: "INSERT INTO skill_invocations", err: &pgconn.PgError{Code: "55P03"}, once: true},
		insertedRow(),
		{match: "min(queued_at) FROM skill_rederive_queue", rows: [][]any{{int64(0), nil}}},
		{match: "FROM schema_migrations WHERE name", rows: [][]any{{at.Add(-72 * time.Hour)}}},
	}}
	s, buf := skillStore(db)
	one := []Ingest{typedTranscript("evt-t", "sess-m", "p1", "engg:git", "", 1)}
	for range 3 {
		s.deriveSkillsAtIngest(context.Background(), &fakeTx{db: db}, one)
	}
	s.LogSkillPlatformSummary(context.Background(), at)
	var msgs []string
	for _, l := range skillLines(t, buf) {
		msgs = append(msgs, l["msg"].(string))
	}
	for _, want := range []string{"skill invocation store failed", "skill invocation deferred", "skill invocations derived", "skill platform summary"} {
		found := false
		for _, m := range msgs {
			found = found || m == want
		}
		if !found {
			t.Errorf("no %q line among %v", want, msgs)
		}
	}
}

// ---------------------------------------------------------------- the queue drain

// One dirty tick's drain on the fake: the queue is read oldest first below
// the park threshold, a rebuilt session's queue row is deleted in the
// rebuild's own transaction, and a failure counts an attempt, keeps the
// error and logs the session as parked at the fifth.
func TestDrainSkillRederive(t *testing.T) {
	queue := func() *stub {
		return &stub{match: "FROM skill_rederive_queue WHERE attempts <", rows: [][]any{{"sess-q"}}}
	}
	locks := []*stub{
		{match: "FOR UPDATE SKIP LOCKED", rows: [][]any{{true}}},
		{match: "pg_try_advisory_xact_lock", rows: [][]any{{true}}},
		{match: "body_expired_at IS NOT NULL", rows: [][]any{{false}}},
	}

	t.Run("rebuilt and dequeued", func(t *testing.T) {
		db := &fakeDB{stubs: append([]*stub{queue(),
			{match: "SELECT source, coalesce(repo, '')", rows: [][]any{{"claude_code", "loop-sessions", nil}}}}, locks...)}
		s, buf := skillStore(db)
		done, parked, err := s.DrainSkillRederive(context.Background(), 20)
		if err != nil || done != 1 || parked != 0 {
			t.Fatalf("drain = %d done, %d parked, %v", done, parked, err)
		}
		q := db.find(t, "FROM skill_rederive_queue WHERE attempts <")
		if q.args[0] != 20 || q.args[1] != skillRederiveParkAt || !strings.Contains(q.sql, "ORDER BY queued_at") {
			t.Errorf("queue read = %s %v", q.sql, q.args)
		}
		if db.count("SET LOCAL lock_timeout = '5s'") != 1 {
			t.Error("the rebuild ran without its lock bound")
		}
		del := db.find(t, "DELETE FROM skill_rederive_queue WHERE session_id = $1")
		if !del.inTx || argText(del.args[0]) != "sess-q" {
			t.Errorf("dequeue = %+v", del)
		}
		if db.committed != 1 || db.rolled != 0 {
			t.Errorf("committed %d, rolled back %d", db.committed, db.rolled)
		}
		if got := skillLines(t, buf); len(got) != 0 {
			t.Errorf("a clean drain logged %v", got)
		}
	})

	t.Run("failure counts and parks at five", func(t *testing.T) {
		db := &fakeDB{stubs: append([]*stub{queue(),
			{match: "SELECT source, coalesce(repo, '')", err: errors.New("decode: bad body")},
			{match: "UPDATE skill_rederive_queue SET attempts = attempts + 1", rows: [][]any{{int64(skillRederiveParkAt)}}}}, locks...)}
		s, buf := skillStore(db)
		done, parked, err := s.DrainSkillRederive(context.Background(), 20)
		if err != nil || done != 0 || parked != 1 {
			t.Fatalf("drain = %d done, %d parked, %v", done, parked, err)
		}
		u := db.find(t, "UPDATE skill_rederive_queue SET attempts = attempts + 1")
		if argText(u.args[0]) != "sess-q" || !strings.Contains(argText(u.args[1]), "bad body") || !strings.Contains(u.sql, "last_error = $2") {
			t.Errorf("failure count = %s %v", u.sql, u.args)
		}
		if db.count("DELETE FROM skill_rederive_queue") != 0 || db.committed != 0 || db.rolled != 1 {
			t.Errorf("a failed rebuild committed or dequeued:\n%s", db.summary())
		}
		got := linesWithMessage(skillLines(t, buf), "derive session failed")
		if len(got) != 1 || got[0]["session_id"] != "sess-q" || got[0]["step"] != "skill_invocations" {
			t.Errorf("parked line = %v", got)
		}
	})

	t.Run("a failure under the threshold is quiet", func(t *testing.T) {
		db := &fakeDB{stubs: append([]*stub{queue(),
			{match: "SELECT source, coalesce(repo, '')", err: errors.New("transient")},
			{match: "UPDATE skill_rederive_queue SET attempts = attempts + 1", rows: [][]any{{int64(1)}}}}, locks...)}
		s, buf := skillStore(db)
		done, parked, err := s.DrainSkillRederive(context.Background(), 20)
		if err != nil || done != 0 || parked != 0 {
			t.Fatalf("drain = %d done, %d parked, %v", done, parked, err)
		}
		if got := skillLines(t, buf); len(got) != 0 {
			t.Errorf("an attempt under the threshold logged %v", got)
		}
	})

	// A row held by ingest or a derive batch is the next tick's business,
	// as it is the step's: no attempt counted, no error kept, nothing
	// logged, the queue row where it was (ADV-LS1 F5).
	t.Run("a lock wait is not an attempt", func(t *testing.T) {
		for _, code := range []string{"55P03", "40P01"} {
			db := &fakeDB{stubs: append([]*stub{queue(),
				{match: "SELECT source, coalesce(repo, '')", err: &pgconn.PgError{Code: code, Message: "lock"}}}, locks...)}
			s, buf := skillStore(db)
			done, parked, err := s.DrainSkillRederive(context.Background(), 20)
			if err != nil || done != 0 || parked != 0 {
				t.Fatalf("%s: drain = %d done, %d parked, %v", code, done, parked, err)
			}
			if db.count("UPDATE skill_rederive_queue SET attempts = attempts + 1") != 0 || db.count("DELETE FROM skill_rederive_queue") != 0 || db.committed != 0 || db.rolled != 1 {
				t.Errorf("%s: a lock wait counted an attempt, dequeued or committed:\n%s", code, db.summary())
			}
			if got := skillLines(t, buf); len(got) != 0 {
				t.Errorf("%s: a lock wait logged %v", code, got)
			}
		}
	})
}

// ---------------------------------------------------------------- the summary

// The silence a series reports: minutes since its last row with Saturday
// 00:00 to Monday 00:00 America/New_York not counted, the sentinel for a
// series with no row, and zero for a derived series inside the day after
// 0022 landed.
func TestSkillSilenceMinutesMasksTheUSEasternWeekend(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+1800)
	fri := time.Date(2026, 9, 11, 18, 0, 0, 0, ist) // Friday 18:00 IST = Friday 08:30 EDT
	mon := time.Date(2026, 9, 14, 9, 0, 0, 0, ist)  // Monday 09:00 IST = Sunday 23:30 EDT
	tue := time.Date(2026, 9, 15, 9, 0, 0, 0, ist)  // Tuesday 09:00 IST = Monday 23:30 EDT
	if fri.Weekday() != time.Friday || mon.Weekday() != time.Monday {
		t.Fatalf("the fixture days are %v and %v", fri.Weekday(), mon.Weekday())
	}
	// Friday 08:30 EDT to Saturday 00:00 EDT is 15.5 h; the rest of the
	// gap to Sunday 23:30 EDT is weekend.
	if got := countedMinutes(fri, mon); got != 930 {
		t.Errorf("Friday 18:00 IST to Monday 09:00 IST counts %v minutes, want 930", got)
	}
	if got := skillSilenceMinutes(&fri, mon, time.Time{}, true); got >= 1440 {
		t.Errorf("the derived series reads %v on Monday morning, want under 1440", got)
	}
	// One more working day: Monday 00:00 to Monday 23:30 EDT.
	if got := countedMinutes(fri, tue); got != 930+1410 {
		t.Errorf("to Tuesday 09:00 IST counts %v minutes, want 2340", got)
	}
	// A gap inside one working day loses nothing; one spanning two
	// weekends loses both.
	wed := time.Date(2026, 9, 16, 15, 0, 0, 0, ist)
	if got := countedMinutes(wed, wed.Add(90*time.Minute)); got != 90 {
		t.Errorf("a 90-minute weekday gap counts %v", got)
	}
	if got := countedMinutes(fri, fri.AddDate(0, 0, 14)); got != 14*1440-2*2880 {
		t.Errorf("a fortnight counts %v minutes, want two weekends off", got)
	}
	if got := countedMinutes(mon, fri); got != 0 {
		t.Errorf("a last row in the future counts %v, want 0", got)
	}

	if got := skillSilenceMinutes(nil, mon, time.Time{}, false); got != skillSilenceSentinel {
		t.Errorf("a series with no row reads %v, want the sentinel", got)
	}
	if got := skillSilenceMinutes(nil, mon, mon.Add(-time.Hour), true); got != 0 {
		t.Errorf("a derived series inside the post-migration grace reads %v, want 0", got)
	}
	if got := skillSilenceMinutes(nil, mon, mon.Add(-25*time.Hour), true); got != skillSilenceSentinel {
		t.Errorf("a derived series past the grace reads %v, want the sentinel", got)
	}
	if got := skillSilenceMinutes(nil, mon, mon.Add(-time.Hour), false); got != skillSilenceSentinel {
		t.Errorf("an emitter series gets the grace: %v", got)
	}
}

// The summary lines on the fake: one per expected derived series whether
// or not it has a row, the queue depth and age riding both, the sentinel
// for the series with no row once the grace has passed.
func TestSkillPlatformSummaryWritesOneLinePerExpectedSeries(t *testing.T) {
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC) // a Wednesday
	lastAgent := now.Add(-3 * time.Hour)
	oldest := now.Add(-30 * time.Minute)
	db := &fakeDB{stubs: []*stub{
		{match: "GROUP BY agent_platform, origin, trigger", rows: [][]any{{PlatformClaudeCode, OriginDerived, TriggerAgent, lastAgent, int64(3)}}},
		{match: "min(queued_at) FROM skill_rederive_queue", rows: [][]any{{int64(2), oldest}}},
		{match: "FROM schema_migrations WHERE name", rows: [][]any{{now.Add(-72 * time.Hour)}}},
	}}
	s, buf := skillStore(db)
	s.LogSkillPlatformSummary(context.Background(), now)
	lines := linesWithMessage(skillLines(t, buf), "skill platform summary")
	if len(lines) != 2 {
		t.Fatalf("%d summary lines, want 2:\n%s", len(lines), buf.String())
	}
	byTrigger := map[string]map[string]any{}
	for _, l := range lines {
		byTrigger[l["trigger"].(string)] = l
		for k, want := range map[string]any{"platform": PlatformClaudeCode, "environment": "", "origin": OriginDerived, "rederive_queue": float64(2), "rederive_oldest_minutes": float64(30)} {
			if l[k] != want {
				t.Errorf("%s series %s = %v, want %v", l["trigger"], k, l[k], want)
			}
		}
	}
	if a := byTrigger[TriggerAgent]; a["minutes_since_last_row"] != float64(180) || a["rows_24h"] != float64(3) {
		t.Errorf("agent series = %v", a)
	}
	if u := byTrigger[TriggerUser]; u["minutes_since_last_row"] != float64(skillSilenceSentinel) || u["rows_24h"] != float64(0) {
		t.Errorf("user series = %v, want the sentinel", u)
	}
	q := db.find(t, "min(queued_at) FROM skill_rederive_queue")
	if q.args[0] != skillRederiveParkAt {
		t.Errorf("the queue figures count rows at attempts < %v, want the park threshold", q.args[0])
	}

	// Inside the grace the row-less derived series reads 0 rather than
	// firing 18 on deploy night.
	db = &fakeDB{stubs: []*stub{
		{match: "min(queued_at) FROM skill_rederive_queue", rows: [][]any{{int64(0), nil}}},
		{match: "FROM schema_migrations WHERE name", rows: [][]any{{now.Add(-time.Hour)}}},
	}}
	s, buf = skillStore(db)
	s.LogSkillPlatformSummary(context.Background(), now)
	for _, l := range linesWithMessage(skillLines(t, buf), "skill platform summary") {
		if l["minutes_since_last_row"] != float64(0) || l["rederive_queue"] != float64(0) || l["rederive_oldest_minutes"] != float64(0) {
			t.Errorf("grace-period line = %v", l)
		}
	}
}
