//go:build integration

package store

// The half of the skill derivation only Postgres can answer: that the
// savepoint inside an events batch really does let the events commit when
// the skill statement fails, that the merge's CASE arms come to the same
// rows whether the derivation ran at ingest or over the stored events, and
// that the retention sweep keeps the counts and takes the person.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

const skillIntDevice = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"

// skillSeedPerson enrols the owner and one device: sessions.device_id
// references devices, so a batch naming a device the store never saw is
// refused before any skill row could be derived.
func skillSeedPerson(t *testing.T, s *Store, email, device string) {
	t.Helper()
	mustPrincipal(t, s, email, RoleMember)
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO devices (id, email, hostname) VALUES ($1::uuid, $2, 'laptop') ON CONFLICT (id) DO NOTHING`,
		device, email); err != nil {
		t.Fatalf("seed device: %v", err)
	}
}

// asSession stamps one session's fixtures the way one laptop delivers
// them: the same device and repo on every event (the rebuild reads both
// off the sessions row, so a batch that varied them could never agree
// with its own rebuild) and a moment one second apart per event.
func asSession(items []Ingest, device string, at time.Time) []Ingest {
	for i := range items {
		items[i].DeviceID = device
		items[i].Repo = "loop-sessions"
		items[i].Event.OccurredAt = at.Add(time.Duration(i) * time.Second)
	}
	return items
}

func skillIngest(t *testing.T, s *Store, items []Ingest) {
	t.Helper()
	res, err := s.UpsertEvents(context.Background(), items)
	if err != nil {
		t.Fatalf("UpsertEvents: %v", err)
	}
	if len(res.Rejected) != 0 || len(res.Accepted()) != len(items) {
		t.Fatalf("stored %d of %d events, rejected %v", len(res.Accepted()), len(items), res.Rejected)
	}
}

// skillSnap is one derived row as the agreement test compares it: every
// column but id and received_at, which the database fills, and
// session_type, which ingest leaves ” and the rebuild copies from the
// session (design 3.4 and 3.6 step 4; the reads join sessions for it while
// the session exists).
type skillSnap struct{ Key, Body string }

func skillSnapshot(t *testing.T) []skillSnap {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT dedupe_key, (to_jsonb(s) - 'id' - 'received_at' - 'session_type')::text
		FROM skill_invocations s WHERE origin = 'derived' ORDER BY dedupe_key`)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer rows.Close()
	var out []skillSnap
	for rows.Next() {
		var sn skillSnap
		if err := rows.Scan(&sn.Key, &sn.Body); err != nil {
			t.Fatalf("snapshot scan: %v", err)
		}
		out = append(out, sn)
	}
	return out
}

func skillKeys(t *testing.T, where string, args ...any) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT dedupe_key FROM skill_invocations WHERE `+where+` ORDER BY dedupe_key`, args...)
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	return keys
}

// runSkillStep runs the skill_invocations step alone, the way the rerun
// route's two statements make the runner run it (design 9.5): the step's
// ledger row cleared and the stamp one below the code's, so
// ensureDeriveJobs seeds the thirteen version-3 steps finished (or leaves
// them as an earlier pass in this schema left them) and the fourteenth
// runs. The earlier steps' processed counts are read before and after,
// since the schema is shared with the runner tests and "did not run" is a
// count that did not move, not a count of zero.
func runSkillStep(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	// A database that ran its version-3 pass, as production had before
	// LS-1 (design 9.2). The reset lowers the stamp and never raises it
	// (ADV-LS1 F4), so on this schema's fresh stamp of 0 the next pass
	// would be the honest fourteen-step run rather than the one step this
	// helper measures.
	if _, err := pool.Exec(ctx, `UPDATE derived_schema SET version = GREATEST(version, $1) WHERE only_row`, DerivedSchema-1); err != nil {
		t.Fatal(err)
	}
	processedBefore := map[string]int64{}
	rows, err := pool.Query(ctx, `SELECT step, processed FROM derive_jobs WHERE version = $1 AND step <> 'skill_invocations'`, DerivedSchema)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var step string
		var n int64
		if err := rows.Scan(&step, &n); err != nil {
			t.Fatal(err)
		}
		processedBefore[step] = n
	}
	rows.Close()
	if err := s.ResetSkillDeriveStep(ctx, s.db); err != nil {
		t.Fatal(err)
	}
	cfg := DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: noSleep}
	for i := 0; i < 20; i++ {
		pass, err := s.RunDerive(ctx, cfg)
		if err != nil {
			t.Fatalf("RunDerive: %v", err)
		}
		if pass.Failed != "" {
			t.Fatalf("the pass parked on %s: %s", pass.Failed, pass.LastError)
		}
		if pass.Done {
			break
		}
	}
	var finished, ranAlone int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE finished_at IS NOT NULL), count(*) FILTER (WHERE step = 'skill_invocations' AND processed > 0)
		FROM derive_jobs WHERE version = $1`, DerivedSchema).Scan(&finished, &ranAlone); err != nil {
		t.Fatal(err)
	}
	if finished != 14 || ranAlone != 1 {
		t.Fatalf("after the pass %d steps finished and the skill step processed %d sessions, want 14 finished with the skill step run", finished, ranAlone)
	}
	rows, err = pool.Query(ctx, `SELECT step, processed FROM derive_jobs WHERE version = $1 AND step <> 'skill_invocations'`, DerivedSchema)
	if err != nil {
		t.Fatal(err)
	}
	earlier := 0
	for rows.Next() {
		var step string
		var n int64
		if err := rows.Scan(&step, &n); err != nil {
			t.Fatal(err)
		}
		earlier++
		if n != processedBefore[step] {
			t.Errorf("%s processed %d sessions under a stamp one below the code's, want %d (untouched)", step, n, processedBefore[step])
		}
	}
	rows.Close()
	if earlier != 13 {
		t.Errorf("%d earlier steps in the ledger, want 13", earlier)
	}
	var version int
	if err := pool.QueryRow(ctx, `SELECT version FROM derived_schema WHERE only_row`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != DerivedSchema {
		t.Errorf("stamp = %d after the pass, want %d", version, DerivedSchema)
	}
}

// A skill row never fails an events batch (OPS1-1). Three ways it could
// have: a cwd outside the repo CHECK with both copies of one prompt in the
// batch, a statement that Postgres refuses, and a panic inside the
// derivation. Each answers 200 with the events stored; the last two leave
// the session in the queue, and the dirty tick's drain gives the row back.
func TestIntegrationIngestNeverFailsOnSkills(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	skillSeedPerson(t, s, skillTestEmail, skillIntDevice)

	both := asSession([]Ingest{
		typedHook("nf-h", "sess-nf", "p1", "/git ship it", 1),
		typedTranscript("nf-t", "sess-nf", "p1", "engg:git", "ship it", 2),
	}, skillIntDevice, at)
	for i := range both {
		both[i].Repo = "my repo (copy)"
	}
	skillIngest(t, s, both)
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = 'sess-nf'`); n != 2 {
		t.Errorf("%d events stored, want 2", n)
	}
	var repo, key, plugin string
	if err := pool.QueryRow(ctx, `SELECT repo, dedupe_key, plugin FROM skill_invocations WHERE session_ref = 'sess-nf'`).Scan(&repo, &key, &plugin); err != nil {
		t.Fatalf("the typed command has no row: %v", err)
	}
	if repo != "" || key != "claude_code:sess-nf:p:p1" || plugin != "engg" {
		t.Errorf("row = repo %q key %q plugin %q, want repo '' on the form-2 key with the envelope's plugin", repo, key, plugin)
	}

	// A statement Postgres refuses: the trigger stands in for a CHECK a
	// future shape trips.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION test_skill_block() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'test_skill_block'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER test_skill_block BEFORE INSERT ON skill_invocations FOR EACH ROW EXECUTE FUNCTION test_skill_block()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_skill_block ON skill_invocations`)
		_, _ = pool.Exec(context.Background(), `DROP FUNCTION IF EXISTS test_skill_block()`)
	})
	buf.Reset()
	blocked := asSession([]Ingest{
		skillCall("blk-call", "sess-blocked", "p1", "toolu_blk", `{"skill":"engg:git","args":"x"}`, 1),
		skillResult("blk-res", "sess-blocked", "toolu_blk", event.ToolResult, 2),
	}, skillIntDevice, at)
	skillIngest(t, s, blocked)
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = 'sess-blocked'`); n != 2 {
		t.Errorf("%d events committed behind the refused skill statement, want 2", n)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE session_ref = 'sess-blocked'`); n != 0 {
		t.Errorf("%d skill rows survived the rollback", n)
	}
	failed := linesWithMessage(skillLines(t, &buf), "skill invocation store failed")
	if len(failed) != 1 || failed[0]["op"] != "derive" || !strings.Contains(failed[0]["error"].(string), "test_skill_block") {
		t.Errorf("store-failed lines = %v", failed)
	}
	var reason string
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT reason, attempts FROM skill_rederive_queue WHERE session_id = 'sess-blocked'`).Scan(&reason, &attempts); err != nil {
		t.Fatalf("the session is not queued: %v", err)
	}
	if reason != "ingest" || attempts != 0 {
		t.Errorf("queued with reason %q attempts %d", reason, attempts)
	}

	// The statement fixed (the trigger gone), the drain rebuilds the
	// session and the row is back with its outcome.
	if _, err := pool.Exec(ctx, `DROP TRIGGER test_skill_block ON skill_invocations`); err != nil {
		t.Fatal(err)
	}
	done, parked, err := s.DrainSkillRederive(ctx, 20)
	if err != nil || done != 1 || parked != 0 {
		t.Fatalf("drain = %d done, %d parked, %v", done, parked, err)
	}
	var outcome string
	if err := pool.QueryRow(ctx, `SELECT outcome FROM skill_invocations WHERE dedupe_key = 'claude_code:sess-blocked:t:toolu_blk'`).Scan(&outcome); err != nil || outcome != OutcomeSuccess {
		t.Errorf("rebuilt row outcome = %q, %v, want success", outcome, err)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_rederive_queue`); n != 0 {
		t.Errorf("%d queue rows after the drain", n)
	}

	// A panic inside the derivation (the oracle stands in for a Raw shape
	// the parser did not expect) is contained the same way.
	buf.Reset()
	s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) { panic("unexpected shape") }
	skillIngest(t, s, asSession([]Ingest{typedHook("pn-h", "sess-panic", "p1", "/git go", 1)}, skillIntDevice, at))
	s.AliasOracle = nil
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = 'sess-panic'`); n != 1 {
		t.Errorf("%d events committed behind the panic, want 1", n)
	}
	failed = linesWithMessage(skillLines(t, &buf), "skill invocation store failed")
	if len(failed) != 1 || !strings.Contains(failed[0]["error"].(string), "skill derive panic: unexpected shape") {
		t.Errorf("panic lines = %v", failed)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_rederive_queue WHERE session_id = 'sess-panic'`); n != 1 {
		t.Errorf("the panicked session is not queued")
	}
	if done, _, err := s.DrainSkillRederive(ctx, 20); err != nil || done != 1 {
		t.Errorf("drain after the panic = %d, %v", done, err)
	}

	// A FAILED oracle read is not a non-confirmation. The two are opposite
	// facts about one name, and folding them together dropped the row for
	// good, with nothing short of a DerivedSchema bump to bring it back.
	// It takes the same savepoint path instead: the events commit, the
	// skill block rolls back, one store-failed line is written and the
	// session is queued, so the drain produces the row once the catalog
	// answers again (adversarial iteration 5, finding 2).
	buf.Reset()
	s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) {
		return false, errors.New("connection refused")
	}
	skillIngest(t, s, asSession([]Ingest{typedHook("or-h", "sess-oracle", "p1", "/git go", 1)}, skillIntDevice, at))
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = 'sess-oracle'`); n != 1 {
		t.Errorf("%d events committed behind the oracle error, want 1", n)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE session_ref = 'sess-oracle'`); n != 0 {
		t.Errorf("%d skill rows written behind a failed oracle read, want none", n)
	}
	failed = linesWithMessage(skillLines(t, &buf), "skill invocation store failed")
	if len(failed) != 1 || !strings.Contains(failed[0]["error"].(string), "connection refused") {
		t.Errorf("oracle-error lines = %v", failed)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_rederive_queue WHERE session_id = 'sess-oracle'`); n != 1 {
		t.Errorf("the session whose oracle read failed is not queued")
	}
	s.AliasOracle = func(context.Context, Queryer, string, string) (bool, error) { return true, nil }
	if done, _, err := s.DrainSkillRederive(ctx, 20); err != nil || done != 1 {
		t.Errorf("drain after the oracle error = %d, %v", done, err)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE session_ref = 'sess-oracle'`); n != 1 {
		t.Errorf("%d skill rows after the drain, want 1", n)
	}
	s.AliasOracle = nil
}

// Ingest and derive agree (design 10.2): the rows the ingest transaction
// wrote are, column for column, the rows the versioned step rebuilds from
// the stored events; a second run changes nothing; an API row is not the
// rebuild's to touch; and a 2c row whose hook twin arrived in a later
// batch is the one row the rebuild removes.
func TestIntegrationIngestAndDeriveAgree(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	skillSeedPerson(t, s, skillTestEmail, skillIntDevice)
	s.AliasOracle = func(_ context.Context, _ Queryer, plugin, skill string) (bool, error) {
		return plugin == "" && skill == "git", nil
	}

	// One Claude Code session: a Skill call and its nameless result, a
	// typed command with arguments on both capture paths (the transcript
	// copy without a prompt id, so it pairs), a built-in on both, and a
	// transcript Skill call whose ids exist only in Raw.
	rawCall := ingestOf("ag-tcall", "sess-agree", skillTestEmail, event.ToolCall, 7)
	rawCall.Event.Origin = event.OriginTranscript
	rawCall.Event.Tool = &event.Tool{Name: "Skill", Input: json.RawMessage(`{"skill":"probe-skill"}`)}
	rawCall.Event.Raw = json.RawMessage(`{"type":"assistant","uuid":"u-ag-1","message":{"id":"msg_ag_1","role":"assistant","content":[{"type":"tool_use","id":"toolu_raw","name":"Skill","input":{"skill":"probe-skill"}}]}}`)
	rawResult := ingestOf("ag-tresult", "sess-agree", skillTestEmail, event.ToolResult, 8)
	rawResult.Event.Origin = event.OriginTranscript
	rawResult.Event.Tool = &event.Tool{Output: "Launching skill: probe-skill"}
	rawResult.Event.Raw = json.RawMessage(`{"type":"user","uuid":"u-ag-2","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_raw","content":"Launching skill: probe-skill"}]}}`)
	// A subagent's Skill call and its failure: agent_id keeps subagent
	// prompts out of the derivation, never tool calls (design 3.6 (2);
	// review-1 F2), on both paths alike.
	subCall := skillCall("ag-sub", "sess-agree", "", "toolu_sub", `{"skill":"engg:standup"}`, 9)
	subCall.Event.AgentID = "researcher"
	subResult := skillResult("ag-subres", "sess-agree", "toolu_sub", event.ToolFailed, 10)
	subResult.Event.AgentID = "researcher"
	skillIngest(t, s, asSession([]Ingest{
		skillCall("ag-call", "sess-agree", "p1", "toolu_1", `{"skill":"engg:git","args":"hello world"}`, 1),
		skillResult("ag-result", "sess-agree", "toolu_1", event.ToolResult, 2),
		typedHook("ag-h", "sess-agree", "p2", "/git ship it", 3),
		typedTranscript("ag-t", "sess-agree", "", "engg:git", "ship it", 4),
		typedHook("ag-bh", "sess-agree", "p3", "/compact", 5),
		typedTranscript("ag-bt", "sess-agree", "p3", "compact", "", 6),
		rawCall,
		rawResult,
		subCall,
		subResult,
	}, skillIntDevice, at))

	// A Codex session with one confirmed $skill mention.
	codex := ingestOf("cx-prompt", "sess-codex", skillTestEmail, event.UserPrompt, 1)
	codex.Event.Source, codex.Event.Origin, codex.Event.Text = event.SourceCodex, event.OriginTranscript, "please $git this and $unknown-thing"
	skillIngest(t, s, asSession([]Ingest{codex}, skillIntDevice, at.Add(time.Hour)))

	// A transcript-only session with one slash copy and no prompt id.
	skillIngest(t, s, asSession([]Ingest{typedTranscript("only-t", "sess-only", "", "engg:git", "", 1)}, skillIntDevice, at.Add(2*time.Hour)))

	wantKeys := []string{
		"claude_code:sess-agree:p:p2",
		"claude_code:sess-agree:t:toolu_1",
		"claude_code:sess-agree:t:toolu_raw",
		"claude_code:sess-agree:t:toolu_sub",
		"claude_code:sess-only:e:only-t",
		"codex:sess-codex:e:cx-prompt:git",
	}
	if got := skillKeys(t, `origin = 'derived'`); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("ingest derived %v, want %v", got, wantKeys)
	}
	var outcome, plugin string
	var argsPresent bool
	if err := pool.QueryRow(ctx, `SELECT outcome FROM skill_invocations WHERE dedupe_key = 'claude_code:sess-agree:t:toolu_raw'`).Scan(&outcome); err != nil || outcome != OutcomeSuccess {
		t.Errorf("the transcript Skill call's outcome = %q, %v", outcome, err)
	}
	if err := pool.QueryRow(ctx, `SELECT plugin, args_present FROM skill_invocations WHERE dedupe_key = 'claude_code:sess-agree:p:p2'`).Scan(&plugin, &argsPresent); err != nil || plugin != "engg" || !argsPresent {
		t.Errorf("the paired typed command = plugin %q args_present %v, %v", plugin, argsPresent, err)
	}
	var trigger string
	if err := pool.QueryRow(ctx, `SELECT trigger, outcome FROM skill_invocations WHERE dedupe_key = 'claude_code:sess-agree:t:toolu_sub'`).Scan(&trigger, &outcome); err != nil || trigger != TriggerAgent || outcome != OutcomeError {
		t.Errorf("the subagent's Skill call = trigger %q outcome %q, %v", trigger, outcome, err)
	}

	before := skillSnapshot(t)
	if len(before) != 6 {
		t.Fatalf("%d derived rows in the ingest transaction, want 6", len(before))
	}

	// An API row, as the hook route would leave one: not derived, keyed
	// on its idempotency key, and none of the rebuild's business.
	if _, err := pool.Exec(ctx, `
		INSERT INTO skill_invocations (occurred_at, origin, agent_platform, trust, device_id, raw_name, plugin, skill, skill_source,
		                               trigger, outcome, actor_email, actor_known, session_ref, idempotency_key, dedupe_key)
		VALUES (now(), 'hook', 'claude_code', 'device', $1::uuid, 'git', '', 'git', 'project', 'user', 'started', $2, true, 'sess-api', 'k1', 'k:d:' || $1 || ':k1')`,
		skillIntDevice, skillTestEmail); err != nil {
		t.Fatalf("insert the API row: %v", err)
	}

	if _, err := pool.Exec(ctx, `DELETE FROM skill_invocations WHERE origin = 'derived'`); err != nil {
		t.Fatal(err)
	}
	runSkillStep(t, s)
	if got := skillSnapshot(t); !reflect.DeepEqual(got, before) {
		t.Errorf("the step rebuilt different rows than ingest wrote:\n got %v\nwant %v", got, before)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE origin = 'hook' AND dedupe_key = 'k:d:' || $1 || ':k1'`, skillIntDevice); n != 1 {
		t.Errorf("the API row did not survive the rebuild")
	}

	// A second run is a no-op.
	runSkillStep(t, s)
	if got := skillSnapshot(t); !reflect.DeepEqual(got, before) {
		t.Errorf("the second run changed rows:\n got %v\nwant %v", got, before)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations`); n != 7 {
		t.Errorf("%d rows after two runs, want the six derived and the API row", n)
	}

	// The hook twin arrives in a batch after its transcript copy: ingest
	// leaves a 2c row beside the form-2 row, and the rebuild's (3b)
	// removes it.
	skillIngest(t, s, asSession([]Ingest{typedTranscript("late-t", "sess-late", "", "git", "", 1)}, skillIntDevice, at.Add(3*time.Hour)))
	skillIngest(t, s, asSession([]Ingest{typedHook("late-h", "sess-late", "p9", "/git go", 2)}, skillIntDevice, at.Add(3*time.Hour+time.Second)))
	if got := skillKeys(t, `session_ref = 'sess-late'`); !reflect.DeepEqual(got, []string{"claude_code:sess-late:e:late-t", "claude_code:sess-late:p:p9"}) {
		t.Fatalf("after the late twin ingest = %v, want the 2c row beside the form-2 row", got)
	}
	// The twin's batch queued the session (ADV-LS1 F7), so the dirty tick
	// folds the two before any versioned pass; the step is what this test
	// runs, and the queued rebuild after it finds the fold done.
	var reason string
	if err := pool.QueryRow(ctx, `SELECT reason FROM skill_rederive_queue WHERE session_id = 'sess-late'`).Scan(&reason); err != nil || reason != "twin" {
		t.Errorf("the late twin's session is queued with reason %q, %v, want twin", reason, err)
	}
	runSkillStep(t, s)
	if got := skillKeys(t, `session_ref = 'sess-late'`); !reflect.DeepEqual(got, []string{"claude_code:sess-late:p:p9"}) {
		t.Errorf("after the rebuild = %v, want the form-2 row alone", got)
	}
	if got := skillSnapshot(t); len(got) != 7 {
		t.Errorf("%d derived rows after the late twin, want 7", len(got))
	}
	if done, parked, err := s.DrainSkillRederive(ctx, 20); err != nil || done != 1 || parked != 0 {
		t.Errorf("drain after the step = %d done, %d parked, %v", done, parked, err)
	}
	if got := skillKeys(t, `session_ref = 'sess-late'`); !reflect.DeepEqual(got, []string{"claude_code:sess-late:p:p9"}) {
		t.Errorf("after the queued rebuild = %v, want the form-2 row alone still", got)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_rederive_queue`); n != 0 {
		t.Errorf("%d queue rows after the drain", n)
	}
}

// The merge's U arm keeps an API row's result (design 3.3, ADV-LS1 F1):
// a derived copy always arrives started, so a hook's PostToolUse that
// already stored success or error is not downgraded when the daemon's
// Skill call lands in a batch without its result, while the rest of the
// row (origin, name) flips to the derived copy; a hook row still started
// takes its result from query 2 as before, and a settled row keeps its
// result whatever a later result says.
func TestIntegrationDerivedStartedCopyKeepsAnAPIRowsResult(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	skillSeedPerson(t, s, skillTestEmail, skillIntDevice)
	seedHook := func(toolUse, outcome string, errorClass *string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO skill_invocations (occurred_at, origin, agent_platform, trust, device_id, raw_name, plugin, skill, skill_source,
			                               trigger, outcome, error_class, actor_email, actor_known, session_ref, tool_use_id, dedupe_key)
			VALUES (now(), 'hook', 'claude_code', 'device', $1::uuid, 'git', '', 'git', 'unknown', 'agent', $2, $3, $4, true, 'sess-u', $5, 'claude_code:sess-u:t:' || $5)`,
			skillIntDevice, outcome, errorClass, skillTestEmail, toolUse); err != nil {
			t.Fatalf("seed the hook row %s: %v", toolUse, err)
		}
	}
	runtimeError := ErrorClassRuntime
	seedHook("toolu_ok", OutcomeSuccess, nil)
	seedHook("toolu_bad", OutcomeError, &runtimeError)
	seedHook("toolu_open", OutcomeStarted, nil)

	// The daemon's three Skill calls, no result among them.
	skillIngest(t, s, asSession([]Ingest{
		skillCall("u-ok", "sess-u", "p1", "toolu_ok", `{"skill":"engg:git"}`, 1),
		skillCall("u-bad", "sess-u", "p2", "toolu_bad", `{"skill":"engg:git"}`, 2),
		skillCall("u-open", "sess-u", "p3", "toolu_open", `{"skill":"engg:git"}`, 3),
	}, skillIntDevice, at))
	read := func(toolUse string) (origin, rawName, outcome string, errorClass *string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT origin, raw_name, outcome, error_class FROM skill_invocations WHERE dedupe_key = 'claude_code:sess-u:t:' || $1`, toolUse).Scan(&origin, &rawName, &outcome, &errorClass); err != nil {
			t.Fatalf("read %s: %v", toolUse, err)
		}
		return origin, rawName, outcome, errorClass
	}
	for _, tc := range []struct {
		toolUse, outcome string
		errorClass       *string
	}{{"toolu_ok", OutcomeSuccess, nil}, {"toolu_bad", OutcomeError, &runtimeError}, {"toolu_open", OutcomeStarted, nil}} {
		origin, rawName, outcome, errorClass := read(tc.toolUse)
		if origin != OriginDerived || rawName != "engg:git" {
			t.Errorf("%s: origin %q raw_name %q, want the derived copy's", tc.toolUse, origin, rawName)
		}
		if outcome != tc.outcome || !reflect.DeepEqual(errorClass, tc.errorClass) {
			t.Errorf("%s: outcome %q error_class %v, want %q %v", tc.toolUse, outcome, errorClass, tc.outcome, tc.errorClass)
		}
	}

	// The results in a later batch: the open row takes its result, the
	// settled one keeps the hook's.
	skillIngest(t, s, asSession([]Ingest{
		skillResult("u-okr", "sess-u", "toolu_ok", event.ToolFailed, 4),
		skillResult("u-openr", "sess-u", "toolu_open", event.ToolResult, 5),
	}, skillIntDevice, at.Add(time.Minute)))
	if _, _, outcome, _ := read("toolu_ok"); outcome != OutcomeSuccess {
		t.Errorf("a later failed result moved the hook's success to %q", outcome)
	}
	if _, _, outcome, _ := read("toolu_open"); outcome != OutcomeSuccess {
		t.Errorf("the open row's result = %q, want success", outcome)
	}
}

// The rebuild's (3b) DELETE takes only a 2c row whose event is still
// stored (ADV-LS1 F6): retention deletes a long session's events in
// batches and commits each, so a rebuild between two batches reads a
// partial session, and the 2c rows of the events already gone are the
// sweep's last batch's to anonymise for the 90-day count, not the
// rebuild's to remove.
func TestIntegrationRebuildKeepsA2cRowWhoseEventRetentionTook(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	skillSeedPerson(t, s, skillTestEmail, skillIntDevice)
	skillIngest(t, s, asSession([]Ingest{
		typedTranscript("gone-t", "sess-gone", "", "engg:git", "", 1),
		typedTranscript("kept-t", "sess-gone", "", "engg:standup", "", 2),
	}, skillIntDevice, at))
	want := []string{"claude_code:sess-gone:e:gone-t", "claude_code:sess-gone:e:kept-t"}
	if got := skillKeys(t, `session_ref = 'sess-gone'`); !reflect.DeepEqual(got, want) {
		t.Fatalf("ingest = %v, want %v", got, want)
	}
	// Retention's first batch took the earlier event and committed.
	if _, err := pool.Exec(ctx, `DELETE FROM events WHERE id = 'gone-t'`); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueSkillRederive(ctx, s.db, []string{"sess-gone"}, "ingest"); err != nil {
		t.Fatal(err)
	}
	if done, parked, err := s.DrainSkillRederive(ctx, 20); err != nil || done != 1 || parked != 0 {
		t.Fatalf("drain = %d done, %d parked, %v", done, parked, err)
	}
	if got := skillKeys(t, `session_ref = 'sess-gone'`); !reflect.DeepEqual(got, want) {
		t.Errorf("after the rebuild = %v, want both rows kept", got)
	}
}

// Retention (design 3.4): the counts stay, the person leaves. A deleted
// session's derived rows and the aged API rows lose their email, device,
// session and ids in the same batch as the session, an automation
// session's rows keep their type after the session is gone, and no
// anonymised row can be joined back to a person through a bound token.
func TestIntegrationRetentionAnonymisesSkillRowsAndKeepsTheCounts(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	const owner = "goer@example.com"
	skillSeedPerson(t, s, owner, skillIntDevice)
	old := retentionClock.Add(-2 * retentionYear)

	seed := func(sid string, when time.Time) {
		items := asSession([]Ingest{
			skillCall(sid+"-call", sid, "p1", "toolu_"+sid, `{"skill":"engg:git"}`, 1),
			skillResult(sid+"-res", sid, "toolu_"+sid, event.ToolResult, 2),
			typedTranscript(sid+"-t", sid, "p2", "engg:git", "", 3),
		}, skillIntDevice, when)
		for i := range items {
			items[i].Email = owner
		}
		skillIngest(t, s, items)
	}
	seed("sess-old", old)
	seed("sess-auto", old)
	seed("sess-kept", retentionClock.Add(-10*retentionDay))
	if _, err := pool.Exec(ctx, `UPDATE sessions SET session_type = 'automation' WHERE session_id = 'sess-auto'`); err != nil {
		t.Fatal(err)
	}

	// A per-person laptop token and two rows under it, one past the
	// window and one inside it.
	const token = "12121212-3434-4565-8787-989898989898"
	if _, err := pool.Exec(ctx, `
		INSERT INTO source_tokens (id, platform, environment, scope, token_hash, issued_by, allowed_origins, bound_actor_email)
		VALUES ($1::uuid, 'claude_code', 'laptop', 'skill-invocations', '\x00', $2, '{hook}', $2)`, token, owner); err != nil {
		t.Fatalf("seed token: %v", err)
	}
	for _, r := range []struct {
		key string
		at  time.Time
	}{{"k1", old}, {"k2", retentionClock.Add(-retentionDay)}} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO skill_invocations (occurred_at, origin, agent_platform, trust, source_token_id, raw_name, plugin, skill,
			                               trigger, outcome, actor_email, actor_known, session_ref, idempotency_key, dedupe_key)
			VALUES ($1, 'hook', 'claude_code', 'claimed', $2::uuid, 'git', '', 'git', 'user', 'started', $3, true, 'sess-hook-' || $4, $4, 'k:' || $2 || ':' || $4)`,
			r.at, token, owner, r.key); err != nil {
			t.Fatalf("seed API row %s: %v", r.key, err)
		}
	}

	total := countRows(t, `SELECT count(*) FROM skill_invocations`)
	recent := countRows(t, `SELECT count(*) FROM skill_invocations WHERE occurred_at >= $1`, retentionClock.Add(-90*retentionDay))
	if total != 8 || recent != 3 {
		t.Fatalf("seeded %d rows, %d in the last 90 days, want 8 and 3", total, recent)
	}

	sweep, err := s.SweepRetention(ctx, RetentionPolicy{SessionAfter: retentionYear, BatchSize: 10}, retentionClock)
	if err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if sweep.SessionsDeleted != 2 {
		t.Errorf("deleted %d sessions, want the two past the window", sweep.SessionsDeleted)
	}

	if n := countRows(t, `SELECT count(*) FROM skill_invocations`); n != total {
		t.Errorf("%d rows after the sweep, want %d: counts stay", n, total)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE occurred_at >= $1`, retentionClock.Add(-90*retentionDay)); n != recent {
		t.Errorf("%d rows in the last 90 days after the sweep, want %d", n, recent)
	}
	// Five rows are anonymised: the two sessions' four derived rows and the
	// aged API row. Every one of them names nobody and nothing.
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE dedupe_key LIKE 'anon:%'`); n != 5 {
		t.Errorf("%d anonymised rows, want 5", n)
	}
	if n := countRows(t, `
		SELECT count(*) FROM skill_invocations WHERE dedupe_key LIKE 'anon:%' AND (
		    actor_email IS NOT NULL OR device_id IS NOT NULL OR source_token_id IS NOT NULL OR preempted_by IS NOT NULL
		 OR preempted_device IS NOT NULL OR session_ref <> '' OR event_id IS NOT NULL OR prompt_id IS NOT NULL OR tool_use_id IS NOT NULL)`); n != 0 {
		t.Errorf("%d anonymised rows still name the person, the device, the session or an id", n)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE dedupe_key LIKE 'anon:%' AND session_type = 'automation'`); n != 2 {
		t.Errorf("%d anonymised rows kept the automation type, want the automation session's 2", n)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE dedupe_key LIKE 'anon:%' AND raw_name = 'engg:git'`); n != 4 {
		t.Errorf("%d anonymised derived rows kept their name, want 4", n)
	}
	// The session inside the window and the recent API row are untouched.
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE session_ref = 'sess-kept' AND actor_email = $1 AND device_id = $2::uuid`, owner, skillIntDevice); n != 2 {
		t.Errorf("%d rows of the kept session still whole, want 2", n)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE dedupe_key = 'k:' || $1 || ':k2' AND source_token_id = $1::uuid AND actor_email = $2`, token, owner); n != 1 {
		t.Errorf("the recent API row was touched")
	}
	// No anonymised row joins a bound token (SECURITY3-3).
	if n := countRows(t, `
		SELECT count(*) FROM skill_invocations si JOIN source_tokens st ON st.id = si.source_token_id
		WHERE si.dedupe_key LIKE 'anon:%' AND st.bound_actor_email IS NOT NULL`); n != 0 {
		t.Errorf("%d anonymised rows join a bound token", n)
	}

	// A second sweep finds nothing left to anonymise and moves nothing.
	if _, err := s.SweepRetention(ctx, RetentionPolicy{SessionAfter: retentionYear, BatchSize: 10}, retentionClock); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_invocations`); n != total {
		t.Errorf("%d rows after the second sweep, want %d", n, total)
	}
}

// 0022 applies twice on one database (design 10.2, migrations): TestMain
// applied every file cold on this schema, and a second Migrate finds them
// all recorded and the IF NOT EXISTS forms inert, so a boot that races
// another instance's boot changes nothing.
func TestIntegrationSkillMigrationsApplyTwice(t *testing.T) {
	ctx := context.Background()
	if err := New(pool, nil).Migrate(ctx); err != nil {
		t.Fatalf("second Migrate on the migrated schema: %v", err)
	}
	if n := countRows(t, `SELECT count(*) FROM schema_migrations WHERE name = '0022_skill_invocations.sql'`); n != 1 {
		t.Errorf("0022 recorded %d times, want 1", n)
	}
	for _, table := range []string{"source_tokens", "skill_invocations", "skill_rederive_queue", "admin_actions"} {
		if n := countRows(t, `SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1`, table); n != 1 {
			t.Errorf("%s exists %d times after two applies", table, n)
		}
	}
}
