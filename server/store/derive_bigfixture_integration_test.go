//go:build integration

package store

// The migration-safety evidence for the derive runner (contract C criterion
// h): the event_keys step, the index builds and the rest of the versioned
// pass over a synthetic corpus of at least 200,000 transcript events, with
// the wall-clock per step, the rows per second, the dead tuples the keys
// step leaves for autovacuum, and the batch size, recorded for the report.
//
// Guarded by LOOP_SESSIONS_BIG_FIXTURE=1 so the default integration suite
// stays seconds long; LOOP_SESSIONS_BIG_FIXTURE_ROWS overrides the size and
// LOOP_SESSIONS_BIG_FIXTURE_OUT names a file the measurements are appended
// to.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

func TestIntegrationEventKeysOverALargeCorpus(t *testing.T) {
	if os.Getenv("LOOP_SESSIONS_BIG_FIXTURE") == "" {
		t.Skip("LOOP_SESSIONS_BIG_FIXTURE is unset; the 200k-event fixture takes minutes")
	}
	rows := 200_000
	if v, err := strconv.Atoi(os.Getenv("LOOP_SESSIONS_BIG_FIXTURE_ROWS")); err == nil && v > 0 {
		rows = v
	}
	const perSession = 200
	sessions := rows / perSession

	s := newStore(t, flatPricer{perToken: 0.001})
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	report := func(format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		t.Log(line)
		if out := os.Getenv("LOOP_SESSIONS_BIG_FIXTURE_OUT"); out != "" {
			f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err == nil {
				fmt.Fprintln(f, line)
				f.Close()
			}
		}
	}

	// The corpus: sessions of 200 transcript rows each, four record shapes
	// in rotation (prompt, answer, tool call split from the answer's record,
	// tool result), every key inside body.raw and none in a column, which is
	// what every row stored before 0017 looks like. Messages for the prompts
	// and answers, unclassified, as PR B's column left history.
	seeded := time.Now()
	if _, err := pool.Exec(ctx, `
		INSERT INTO sessions (session_id, email, source, started_at, ended_at, ended, session_type, updated_at)
		SELECT 'big-' || g, $1, 'claude_code', $2::timestamptz + g * interval '1 hour',
		       $2::timestamptz + g * interval '1 hour' + interval '400 seconds', true, 'user', now()
		FROM generate_series(0, $3 - 1) g`, dvEmail, dv0, sessions); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, session_id, email, seq, type, origin, occurred_at, tool_name, body, capture_version)
		SELECT 'big-' || g || '-' || i, 'big-' || g, $1, i,
		       CASE i % 4 WHEN 0 THEN 'user_prompt' WHEN 1 THEN 'assistant_turn' WHEN 2 THEN 'tool_call' ELSE 'tool_result' END,
		       'transcript',
		       $2::timestamptz + g * interval '1 hour' + i * interval '2 seconds',
		       CASE WHEN i % 4 >= 2 THEN 'Bash' END,
		       jsonb_build_object(
		         'id', 'big-' || g || '-' || i,
		         'session_id', 'big-' || g,
		         'seq', i,
		         'source', 'claude_code',
		         'origin', 'transcript',
		         'type', CASE i % 4 WHEN 0 THEN 'user_prompt' WHEN 1 THEN 'assistant_turn' WHEN 2 THEN 'tool_call' ELSE 'tool_result' END,
		         'occurred_at', to_char($2::timestamptz + g * interval '1 hour' + i * interval '2 seconds', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
		         'text', CASE i % 4 WHEN 0 THEN 'please look at item ' || i WHEN 1 THEN 'looking at item ' || i || ' ' || repeat('lorem ipsum dolor sit amet ', 20) ELSE 'ok' END,
		         'tool', CASE i % 4
		                   WHEN 2 THEN jsonb_build_object('name', 'Bash', 'input', jsonb_build_object('command', 'true ' || i))
		                   WHEN 3 THEN jsonb_build_object('name', 'Bash', 'output', 'ok')
		                 END,
		         'raw', CASE i % 4
		           WHEN 0 THEN jsonb_build_object('type', 'user', 'uuid', md5('u' || g || '-' || i), 'promptId', md5('p' || g || '-' || i),
		                                          'message', jsonb_build_object('role', 'user', 'content', 'please look at item ' || i))
		           WHEN 1 THEN jsonb_build_object('type', 'assistant', 'uuid', md5('u' || g || '-' || i), 'requestId', 'req_' || g || '_' || i,
		                                          'message', jsonb_build_object('id', 'msg_' || g || '_' || i, 'role', 'assistant',
		                                            'content', jsonb_build_array(jsonb_build_object('type', 'text', 'text', 'looking at item ' || i))))
		           WHEN 2 THEN jsonb_build_object('type', 'assistant', 'uuid', md5('u' || g || '-' || i), 'requestId', 'req_' || g || '_' || i,
		                                          'message', jsonb_build_object('id', 'msg_' || g || '_' || (i - 1), 'role', 'assistant',
		                                            'content', jsonb_build_array(jsonb_build_object('type', 'tool_use', 'id', 'toolu_' || g || '_' || i, 'name', 'Bash', 'input', jsonb_build_object('command', 'true ' || i)))))
		           ELSE jsonb_build_object('type', 'user', 'uuid', md5('u' || g || '-' || i),
		                                   'message', jsonb_build_object('role', 'user',
		                                     'content', jsonb_build_array(jsonb_build_object('type', 'tool_result', 'tool_use_id', 'toolu_' || g || '_' || (i - 1), 'content', 'ok'))))
		         END),
		       3
		FROM generate_series(0, $3 - 1) g, generate_series(0, $4 - 1) i`, dvEmail, dv0, sessions, perSession); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO messages (event_id, session_id, email, seq, role, occurred_at, text, kind)
		SELECT id, session_id, email, seq, CASE type WHEN 'user_prompt' THEN 'user' WHEN 'assistant_turn' THEN 'assistant' ELSE 'tool' END,
		       occurred_at, body->>'text', ''
		FROM events WHERE session_id LIKE 'big-%' AND type IN ('user_prompt', 'assistant_turn', 'tool_result')`); err != nil {
		t.Fatalf("seed messages: %v", err)
	}
	if _, err := pool.Exec(ctx, `VACUUM ANALYZE events`); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	total := countRows(t, `SELECT count(*) FROM events WHERE session_id LIKE 'big-%'`)
	report("fixture: %d events in %d sessions (%d per session), seeded in %s", total, sessions, perSession, time.Since(seeded).Round(time.Millisecond))
	var heap, toast string
	if err := pool.QueryRow(ctx, `SELECT pg_size_pretty(pg_relation_size('events')), pg_size_pretty(pg_total_relation_size('events') - pg_relation_size('events'))`).Scan(&heap, &toast); err != nil {
		t.Fatal(err)
	}
	report("fixture: events heap %s, toast and indexes %s", heap, toast)

	deadTuples := func() int64 {
		// Stats are per backend until flushed; force the flush so the count
		// is the pass's and not the last idle tick's.
		_, _ = pool.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
		var dead int64
		if err := pool.QueryRow(ctx, `SELECT coalesce(n_dead_tup, 0) FROM pg_stat_user_tables WHERE relname = 'events' AND schemaname = current_schema()`).Scan(&dead); err != nil {
			t.Fatalf("dead tuples: %v", err)
		}
		return dead
	}
	deadBefore := deadTuples()

	resetDerived(t)
	cfg := DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: noSleep}
	started := time.Now()
	pass, err := s.RunDerive(ctx, cfg)
	if err != nil {
		t.Fatalf("RunDerive: %v", err)
	}
	if !pass.Done {
		t.Fatalf("pass = %+v, want done", pass)
	}
	report("pass: %d steps in %s, unpaced (rows/s cap lifted for the measurement), batch size %d rows for event_keys, %d sessions per batch for the per-session steps",
		len(pass.Steps), time.Since(started).Round(time.Millisecond), cfg.withDefaults().EventRowsPerBatch, cfg.withDefaults().SessionsPerBatch)
	for _, st := range pass.Steps {
		rate := 0.0
		if st.Seconds > 0 {
			rate = float64(st.Rows) / st.Seconds
		}
		report("step %-34s rows %8d batches %5d seconds %9.3f rows/s %9.0f", st.Step, st.Rows, st.Batches, st.Seconds, rate)
	}
	deadAfter := deadTuples()
	report("dead tuples on events: %d before, %d after the pass (delta %d; the keys step rewrites each keyed row once)", deadBefore, deadAfter, deadAfter-deadBefore)

	// What the keys step wrote, checked, not assumed.
	keyed := countRows(t, `SELECT count(*) FROM events WHERE session_id LIKE 'big-%' AND record_uuid IS NOT NULL`)
	prompts := countRows(t, `SELECT count(*) FROM events WHERE session_id LIKE 'big-%' AND type = 'user_prompt' AND prompt_id IS NOT NULL`)
	answers := countRows(t, `SELECT count(*) FROM events WHERE session_id LIKE 'big-%' AND type = 'assistant_turn' AND message_id IS NOT NULL AND request_id IS NOT NULL`)
	tools := countRows(t, `SELECT count(*) FROM events WHERE session_id LIKE 'big-%' AND type IN ('tool_call', 'tool_result') AND tool_use_id IS NOT NULL`)
	report("keys: record_uuid on %d of %d rows, prompt_id on %d prompts, message_id+request_id on %d answers, tool_use_id on %d tool rows", keyed, total, prompts, answers, tools)
	if keyed != total || prompts != total/4 || answers != total/4 || tools != total/2 {
		t.Errorf("keys incomplete: record_uuid %d/%d prompt_id %d message_id %d tool_use_id %d", keyed, total, prompts, answers, tools)
	}
	kinds := countRows(t, `SELECT count(*) FROM messages WHERE session_id LIKE 'big-%' AND kind <> ''`)
	report("kinds: %d of %d messages classified", kinds, countRows(t, `SELECT count(*) FROM messages WHERE session_id LIKE 'big-%'`))
	turns := countRows(t, `SELECT count(*) FROM turns WHERE session_id LIKE 'big-%'`)
	report("turns: %d (want %d: one per prompt)", turns, total/4)
	if turns != total/4 {
		t.Errorf("turns = %d, want %d", turns, total/4)
	}
	rolled := countRows(t, `SELECT count(*) FROM sessions WHERE session_id LIKE 'big-%' AND user_turns = $1 AND human_turns = $1 AND tool_calls = $1`, perSession/4)
	report("rollups: %d of %d sessions carry user_turns = human_turns = tool_calls = %d", rolled, sessions, perSession/4)
	if rolled != sessions {
		t.Errorf("rollups: %d of %d sessions correct", rolled, sessions)
	}
	for _, ix := range deriveIndexes {
		var valid bool
		if err := pool.QueryRow(ctx, `SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)`, ix.name).Scan(&valid); err != nil || !valid {
			t.Errorf("index %s is not valid after the pass: %v", ix.name, err)
		}
	}

	// A second keys pass over keyed rows writes nothing: only rows whose
	// keys would change are updated, so a re-run leaves no dead tuples.
	if _, err := pool.Exec(ctx, `UPDATE derive_jobs SET cursor = '', processed = 0, finished_at = NULL WHERE version = $1 AND step = 'event_keys'`, DerivedSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE derived_schema SET version = $1 WHERE only_row`, DerivedSchema-1); err != nil {
		t.Fatal(err)
	}
	deadBefore = deadTuples()
	again := time.Now()
	pass, err = s.RunDerive(ctx, cfg)
	if err != nil || !pass.Done {
		t.Fatalf("second pass: %+v %v", pass, err)
	}
	for _, st := range pass.Steps {
		if st.Step == "event_keys" {
			report("second event_keys pass over keyed rows: %d rows read in %.3f s, dead tuples delta %d", st.Rows, st.Seconds, deadTuples()-deadBefore)
		}
	}
	report("second pass total %s", time.Since(again).Round(time.Millisecond))

	// Paced: the shipped cap, over a slice, to show what the pacer costs.
	if _, err := pool.Exec(ctx, `UPDATE events SET prompt_id = NULL, record_uuid = NULL, message_id = NULL, request_id = NULL, tool_use_id = NULL WHERE session_id LIKE 'big-%' AND session_id < 'big-50'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE derive_jobs SET cursor = '', processed = 0, finished_at = NULL WHERE version = $1 AND step = 'event_keys'`, DerivedSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE derived_schema SET version = $1 WHERE only_row`, DerivedSchema-1); err != nil {
		t.Fatal(err)
	}
	paced := DeriveConfig{Window: derive.Window{Always: true}}
	pacedStart := time.Now()
	pass, err = s.RunDerive(ctx, paced)
	if err != nil || !pass.Done {
		t.Fatalf("paced pass: %+v %v", pass, err)
	}
	for _, st := range pass.Steps {
		if st.Step == "event_keys" {
			report("paced event_keys at the default cap (%d rows/s): %d rows in %.1f s wall (%.0f rows/s effective) over %d batches", derive.DefaultRowsPerSec, st.Rows, st.Seconds, float64(st.Rows)/st.Seconds, st.Batches)
		}
	}
	report("paced pass total %s", time.Since(pacedStart).Round(time.Millisecond))
}
