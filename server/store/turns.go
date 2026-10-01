package store

// Per-session derivation: the fold that turns one session's events into
// turns, the counters recomputed from those turns, the classification that
// decides what the session is, and the title recomputed from its messages.
// Every function here works on ONE session inside a transaction the caller
// owns, so the runner can run them per batch under its cursor and the
// dirty-set pass can run them per touched session, through the same code.
// There is no second fold anywhere: ingest writes events, messages, usage
// and the additive rollup, and everything derived from those is derived
// here.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/normalize"
	"github.com/loopai-hq/loop-sessions/server/store/derive"
)

// contentEventTypes are the event types that mean something happened in a
// session. The list is migration 0016's, kept in one place for the
// classification that re-applies its rule.
var contentEventTypes = []string{
	string(event.UserPrompt), string(event.ToolCall), string(event.ToolResult), string(event.ToolFailed),
	string(event.FileChanged), string(event.AssistantTurn), string(event.SubagentStart), string(event.SubagentEnd),
}

// emptyGrace is how long an un-ended session with no content is left alone
// before the runner calls it empty. A session that started an hour ago and
// has not spoken yet may be a person reading the welcome screen; one that
// started yesterday and never spoke is a spawn that died without its end
// marker.
const emptyGrace = 24 * time.Hour

// deriveSessionLockKey is the first half of the per-session advisory lock
// that serialises two folds of one session. Distinct from every other key
// in this package; the second half is hashtext(session_id).
const deriveSessionLockKey int32 = 726679

// sessionLocked takes the per-session derive lock inside the caller's
// transaction, without waiting. A session another fold holds is skipped: the
// dirty flag or the versioned cursor brings it back, and waiting would hold
// this batch's lock across somebody else's work.
func sessionLocked(ctx context.Context, q Queryer, sessionID string) (bool, error) {
	var got bool
	if err := q.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1::int, hashtext($2))`,
		deriveSessionLockKey, sessionID).Scan(&got); err != nil {
		return false, fmt.Errorf("store: take the derive lock for %s: %w", sessionID, err)
	}
	return got, nil
}

// sessionFacts is what the per-session steps read off the rollup row.
type sessionFacts struct {
	Email           string
	DeviceID        *string
	Type            string
	Ended           bool
	StartedAt       time.Time
	EndedAt         *time.Time
	EmptyKind       *string
	ContentEvents   int
	HeadState       string
	ParentSessionID *string
	ParentRecord    *string
	LineageSource   string
	FirstPrompt     *string
	TitleSource     string
	UpdatedAt       time.Time
}

func readSessionFacts(ctx context.Context, q Queryer, sessionID string) (sessionFacts, error) {
	var f sessionFacts
	err := q.QueryRow(ctx, `
		SELECT email, device_id::text, session_type, ended, started_at, ended_at, empty_kind,
		       content_events, head_state, parent_session_id, parent_record_uuid, lineage_source,
		       first_prompt, title_source, updated_at
		FROM sessions WHERE session_id = $1`, sessionID).Scan(
		&f.Email, &f.DeviceID, &f.Type, &f.Ended, &f.StartedAt, &f.EndedAt, &f.EmptyKind,
		&f.ContentEvents, &f.HeadState, &f.ParentSessionID, &f.ParentRecord, &f.LineageSource,
		&f.FirstPrompt, &f.TitleSource, &f.UpdatedAt)
	if err != nil {
		if noRows(err) {
			return f, ErrNotFound
		}
		return f, fmt.Errorf("store: read session %s: %w", sessionID, err)
	}
	return f, nil
}

// ledgerRow is one priced model call as the fold reads it: the token counts
// and the cost in micro-dollars, an integer so the per-turn sums are exact
// (cost_usd is NUMERIC(12,6)).
type ledgerRow struct {
	input, output, cacheRead, cacheWrite int64
	costMicros                           int64
}

// turnUsage is the ledger summed over one turn's events.
type turnUsage struct {
	input, output, cacheRead, cacheWrite int64
	costMicros                           int64
}

// readProjection reads the fold's column projection for one session. Bodies
// are never touched: the kind, the text presence and the text hash come from
// the messages row, the keys from the columns 0017 added. The hash is
// computed for prompts and answers only, which is where pairing needs it;
// tool output, the bulk of the corpus, is never hashed. The session's ledger
// is read in the same pass, once, by event id: it says which rows carried
// usage and it is what the turns are priced from.
func readProjection(ctx context.Context, q Queryer, sessionID string) ([]derive.Row, map[string]ledgerRow, error) {
	credited := map[string]ledgerRow{}
	rows, err := q.Query(ctx, `
		SELECT event_id, input_tokens, output_tokens, cache_read_tokens,
		       cache_write_5m_tokens + cache_write_1h_tokens, round(cost_usd * 1000000)::bigint
		FROM usage_ledger WHERE session_id = $1`, sessionID)
	if err != nil {
		return nil, nil, fmt.Errorf("store: read credited usage for %s: %w", sessionID, err)
	}
	for rows.Next() {
		var (
			id string
			u  ledgerRow
		)
		if err := rows.Scan(&id, &u.input, &u.output, &u.cacheRead, &u.cacheWrite, &u.costMicros); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("store: scan credited usage: %w", err)
		}
		// The ledger's key is the model call; a call can name one event
		// only, so a repeated id would be a repeated key, and the sum is
		// the same either way.
		credited[id] = u
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store: read credited usage for %s: %w", sessionID, err)
	}

	rows, err = q.Query(ctx, `
		SELECT e.id, e.seq, e.type, e.origin, coalesce(e.agent_id, ''), e.occurred_at,
		       coalesce(e.prompt_id, ''), coalesce(e.message_id, ''), coalesce(e.tool_use_id, ''),
		       coalesce(m.kind, ''), m.event_id IS NOT NULL,
		       CASE WHEN m.role IN ('user', 'assistant')
		            THEN left(encode(sha256(convert_to(m.text, 'UTF8')), 'hex'), 8)
		            ELSE '' END
		FROM events e
		LEFT JOIN messages m ON m.event_id = e.id
		WHERE e.session_id = $1
		ORDER BY coalesce(e.agent_id, ''), e.occurred_at, e.seq`, sessionID)
	if err != nil {
		return nil, nil, fmt.Errorf("store: read projection for %s: %w", sessionID, err)
	}
	defer rows.Close()
	var out []derive.Row
	for rows.Next() {
		var (
			r           derive.Row
			typ, origin string
			kind        string
			hasText     bool
			hash        string
		)
		if err := rows.Scan(&r.ID, &r.Seq, &typ, &origin, &r.AgentID, &r.OccurredAt,
			&r.PromptID, &r.MessageID, &r.ToolUseID, &kind, &hasText, &hash); err != nil {
			return nil, nil, fmt.Errorf("store: scan projection row: %w", err)
		}
		r.Type, r.Origin = event.Type(typ), event.Origin(origin)
		r.Kind = normalize.Kind(kind)
		r.HasText = hasText
		r.TextHash = hash
		// A message id names a model call, which is what usage is; the
		// ledger names the one carrier that was priced. Either says the row
		// carried usage, and the projection never opens the body to check.
		_, priced := credited[r.ID]
		r.HasUsage = priced || r.MessageID != ""
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store: read projection for %s: %w", sessionID, err)
	}
	return out, credited, nil
}

// foldSession derives one session's turns and rewrites what depends on
// them: turns, turn_events, the superseded marker on hook rows, the tokens
// and model per turn, and the inherited flag. It reports how many turns it
// wrote. The rollup row is deliberately not touched here: the callers roll
// the counters up (rollupFromTurns) after they have taken the row lock the
// dirty pass judges "touched while folding" by, so a fold's own write is
// never mistaken for ingest's.
//
// Delete-and-reinsert per session rather than an upsert: a turn's index and
// key both move when a copy arrives, and an upsert would leave the old row
// beside the new one. The two tables are empty for this one session for the
// length of this transaction and never for anyone else.
//
// The turns are priced here, in Go, from the one ledger scan readProjection
// performs, and the sums are written with the turn rows. They used to be an
// UPDATE joining turn_events to the ledger inside this transaction, where
// the planner could not see the rows just inserted: with statistics from a
// corpus of small sessions it estimated one turn for any session and
// re-executed the whole ledger aggregate once per turn, with a filter join
// underneath, and on the rehearsal clone a session of 4,305 answers timed
// out at 60 s on every pass (review-2 finding 20). A sum over a map has no
// plan to get wrong.
func (s *Store) foldSession(ctx context.Context, q Queryer, sessionID string, version int) (int, error) {
	facts, err := readSessionFacts(ctx, q, sessionID)
	if err != nil {
		return 0, err
	}
	rows, ledger, err := readProjection(ctx, q, sessionID)
	if err != nil {
		return 0, err
	}
	res := derive.FoldTurns(rows, derive.Options{SessionEnded: facts.Ended})

	if _, err := q.Exec(ctx, `DELETE FROM turns WHERE session_id = $1`, sessionID); err != nil {
		return 0, fmt.Errorf("store: clear turns of %s: %w", sessionID, err)
	}
	if len(res.Turns) > 0 {
		if err := insertTurns(ctx, q, sessionID, version, res.Turns, priceTurns(res.Turns, ledger)); err != nil {
			return 0, err
		}
	}
	if err := markSuperseded(ctx, q, sessionID, res.Superseded); err != nil {
		return 0, err
	}

	if _, err := q.Exec(ctx, `
		UPDATE turns t SET model = coalesce(e.model, '')
		FROM events e
		WHERE t.session_id = $1 AND t.final_event_id IS NOT NULL AND e.id = t.final_event_id`, sessionID); err != nil {
		return 0, fmt.Errorf("store: name models of %s: %w", sessionID, err)
	}

	// A fork's copied prefix is stored (the resume bundle needs it) and
	// flagged: a turn whose prompt record the proven parent also holds is
	// the parent's work, not this session's.
	if facts.ParentSessionID != nil && facts.LineageSource != "" {
		if _, err := q.Exec(ctx, `
			UPDATE turns t SET inherited = true
			FROM events c
			WHERE t.session_id = $1 AND c.id = t.prompt_event_id AND c.record_uuid IS NOT NULL
			  AND EXISTS (SELECT 1 FROM events p
			              WHERE p.session_id = $2 AND p.record_uuid = c.record_uuid)`,
			sessionID, *facts.ParentSessionID); err != nil {
			return 0, fmt.Errorf("store: flag inherited turns of %s: %w", sessionID, err)
		}
	}

	return len(res.Turns), nil
}

// priceTurns sums the ledger over each turn's events. The ledger holds each
// model call once, under one event id, so a turn whose answer was captured
// twice is priced once whichever copy the ledger named.
func priceTurns(turns []derive.Turn, ledger map[string]ledgerRow) []turnUsage {
	out := make([]turnUsage, len(turns))
	if len(ledger) == 0 {
		return out
	}
	for i, t := range turns {
		for _, e := range t.Events {
			u, ok := ledger[e.ID]
			if !ok {
				continue
			}
			out[i].input += u.input
			out[i].output += u.output
			out[i].cacheRead += u.cacheRead
			out[i].cacheWrite += u.cacheWrite
			out[i].costMicros += u.costMicros
		}
	}
	return out
}

func insertTurns(ctx context.Context, q Queryer, sessionID string, version int, turns []derive.Turn, usage []turnUsage) error {
	n := len(turns)
	if len(usage) != n {
		return fmt.Errorf("store: insert turns of %s: %d turns priced for %d", sessionID, len(usage), n)
	}
	var (
		threads                                        = make([]string, n)
		indexes                                        = make([]int32, n)
		keys, kinds, outcomes, prompts, finals         = make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
		inherited                                      = make([]bool, n)
		started, lasts                                 = make([]time.Time, n), make([]time.Time, n)
		firsts, answered                               = make([]*time.Time, n), make([]*time.Time, n)
		wall, active, idle, waiting                    = make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n)
		origins                                        = make([]string, n)
		merged, promptCounts, tools, errs, subs, files = make([]int32, n), make([]int32, n), make([]int32, n), make([]int32, n), make([]int32, n), make([]int32, n)
		input, output, cacheRead, cacheWrite, cost     = make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n), make([]int64, n)
		evSession, evThread, evID, evRole              []string
		evIndex                                        []int32
	)
	for i, t := range turns {
		input[i], output[i], cacheRead[i], cacheWrite[i], cost[i] = usage[i].input, usage[i].output, usage[i].cacheRead, usage[i].cacheWrite, usage[i].costMicros
		threads[i] = t.Thread
		indexes[i] = int32(t.Index)
		keys[i] = t.Key
		kinds[i] = string(t.Kind)
		outcomes[i] = string(t.Outcome)
		prompts[i] = t.PromptEventID
		finals[i] = t.FinalEventID
		inherited[i] = false
		started[i] = t.StartedAt
		lasts[i] = t.LastActivityAt
		firsts[i] = nullTime(t.FirstActivityAt)
		answered[i] = nullTime(t.AnsweredAt)
		wall[i], active[i], idle[i], waiting[i] = t.WallMS, t.ActiveMS, t.IdleMS, t.WaitingForHumanMS
		// Postgres array literal for the origins, which unnest cannot take
		// as a nested array.
		origins[i] = "{" + strings.Join(t.Origins, ",") + "}"
		merged[i], promptCounts[i] = int32(t.Merged), int32(t.Prompts)
		tools[i], errs[i], subs[i], files[i] = int32(t.ToolCalls), int32(t.Errors), int32(t.Subagents), int32(t.FilesChanged)
		for _, e := range t.Events {
			evSession = append(evSession, sessionID)
			evThread = append(evThread, t.Thread)
			evIndex = append(evIndex, int32(t.Index))
			evID = append(evID, e.ID)
			evRole = append(evRole, string(e.Role))
		}
	}
	if _, err := q.Exec(ctx, `
		INSERT INTO turns (session_id, thread, turn_index, turn_key, kind, prompt_event_id, final_event_id,
		                   outcome, inherited, started_at, first_activity_at, last_activity_at, answered_at,
		                   wall_ms, active_ms, idle_ms, waiting_for_human_ms, origins, merged, prompts,
		                   tool_calls, errors, subagents, files_changed,
		                   tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, cost_usd,
		                   derived_version, derived_at)
		SELECT $1, u.thread, u.turn_index, u.turn_key, u.kind, nullif(u.prompt_event_id, ''),
		       nullif(u.final_event_id, ''), u.outcome, u.inherited, u.started_at, u.first_activity_at,
		       u.last_activity_at, u.answered_at, u.wall_ms, u.active_ms, u.idle_ms, u.waiting_ms,
		       u.origins::text[], u.merged, u.prompts, u.tool_calls, u.errors, u.subagents, u.files_changed,
		       u.tokens_input, u.tokens_output, u.tokens_cache_read, u.tokens_cache_write,
		       u.cost_micros::numeric / 1000000,
		       $2, now()
		FROM unnest($3::text[], $4::int[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[],
		            $10::bool[], $11::timestamptz[], $12::timestamptz[], $13::timestamptz[], $14::timestamptz[],
		            $15::bigint[], $16::bigint[], $17::bigint[], $18::bigint[], $19::text[], $20::int[], $21::int[],
		            $22::int[], $23::int[], $24::int[], $25::int[],
		            $26::bigint[], $27::bigint[], $28::bigint[], $29::bigint[], $30::bigint[])
		     AS u(thread, turn_index, turn_key, kind, prompt_event_id, final_event_id, outcome,
		          inherited, started_at, first_activity_at, last_activity_at, answered_at,
		          wall_ms, active_ms, idle_ms, waiting_ms, origins, merged, prompts,
		          tool_calls, errors, subagents, files_changed,
		          tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, cost_micros)`,
		sessionID, version, threads, indexes, keys, kinds, prompts, finals, outcomes,
		inherited, started, firsts, lasts, answered,
		wall, active, idle, waiting, origins, merged, promptCounts,
		tools, errs, subs, files,
		input, output, cacheRead, cacheWrite, cost); err != nil {
		return fmt.Errorf("store: insert turns of %s: %w", sessionID, err)
	}
	if len(evID) == 0 {
		return nil
	}
	if _, err := q.Exec(ctx, `
		INSERT INTO turn_events (session_id, thread, turn_index, event_id, role)
		SELECT * FROM unnest($1::text[], $2::text[], $3::int[], $4::text[], $5::text[])
		ON CONFLICT DO NOTHING`, evSession, evThread, evIndex, evID, evRole); err != nil {
		return fmt.Errorf("store: insert turn events of %s: %w", sessionID, err)
	}
	return nil
}

// markSuperseded writes the fold's election onto the session's hook rows,
// and only hook rows: the statement refuses any other origin whatever the
// fold said, so the invariant that the resume bundle depends on holds in
// the SQL and not only in the fold.
//
// Within one fold the runner owns the session's markers: it sets the marker
// on the hook rows this fold paired and clears it on the hook rows it no
// longer pairs. Nothing else ever writes the column. The clearing is what
// keeps a first-boot fold honest: every session is dirty before event_keys
// has keyed history, so that fold pairs by text and time alone, and a hook
// row it paired then may not pair once the keyed fold runs; a marker that
// could never be cleared would hide that row behind a row that is not its
// twin for good (review-1 finding 15).
func markSuperseded(ctx context.Context, q Queryer, sessionID string, superseded map[string]string) error {
	ids := make([]string, 0, len(superseded))
	bys := make([]string, 0, len(superseded))
	for _, id := range sortedKeys(superseded) {
		ids = append(ids, id)
		bys = append(bys, superseded[id])
	}
	if _, err := q.Exec(ctx, `
		UPDATE events e SET superseded_by = NULL
		WHERE e.session_id = $1 AND e.origin = 'hook' AND e.superseded_by IS NOT NULL
		  AND NOT (e.id = ANY($2::text[]))`, sessionID, ids); err != nil {
		return fmt.Errorf("store: clear superseded hook rows of %s: %w", sessionID, err)
	}
	if len(ids) == 0 {
		return nil
	}
	if _, err := q.Exec(ctx, `
		UPDATE events e SET superseded_by = u.by
		FROM unnest($2::text[], $3::text[]) AS u(id, by)
		WHERE e.id = u.id AND e.session_id = $1 AND e.origin = 'hook' AND e.superseded_by IS DISTINCT FROM u.by`,
		sessionID, ids, bys); err != nil {
		return fmt.Errorf("store: mark superseded hook rows: %w", err)
	}
	return nil
}

// rollupFromTurns overwrites the counters of one session from its turns.
// user_turns is the number of prompt groups (every prompt, once, whichever
// paths captured it); human_turns counts the main-thread turns a person
// opened; tool_calls, errors and subagents are sums; content_events is the
// number of content events the turns hold with the superseded copies left
// out, one per logical event, where ingest's additive count had one per
// copy (review-1 finding 13). A session with no turns is zeroed, which is
// what a lifecycle-only session's counters should read.
func rollupFromTurns(ctx context.Context, q Queryer, sessionID string) error {
	if _, err := q.Exec(ctx, `
		UPDATE sessions s SET
			user_turns     = x.prompts,
			human_turns    = x.human,
			tool_calls     = x.tools,
			errors         = x.errors,
			subagents      = x.subagents,
			content_events = x.content,
			updated_at     = now()
		FROM (
			SELECT coalesce(sum(prompts), 0)::int AS prompts,
			       count(*) FILTER (WHERE thread = '' AND kind IN ('human', 'slash_command'))::int AS human,
			       coalesce(sum(tool_calls), 0)::int AS tools,
			       coalesce(sum(errors), 0)::int AS errors,
			       coalesce(sum(subagents), 0)::int AS subagents,
			       (SELECT count(*) FROM turn_events te JOIN events e ON e.id = te.event_id
			        WHERE te.session_id = $1 AND te.role <> 'superseded' AND e.type = ANY($2::text[]))::int AS content
			FROM turns WHERE session_id = $1
		) x
		WHERE s.session_id = $1
		  AND (s.user_turns, s.human_turns, s.tool_calls, s.errors, s.subagents, s.content_events)
		      IS DISTINCT FROM (x.prompts, x.human, x.tools, x.errors, x.subagents, x.content)`,
		sessionID, contentEventTypes); err != nil {
		return fmt.Errorf("store: roll up %s from turns: %w", sessionID, err)
	}
	return nil
}

// classifySession applies the session rules that need more than one row:
// the empty rule with its grace, the empty kind, capture loss from device
// health, the head state, the internal recompute and lineage resolution.
// It reports whether the row changed.
//
// now is a parameter so a test can state the clock the grace is measured
// against; healthHourly says whether the health_hourly table exists yet
// (PR E adds it), since a query against an absent table fails the batch.
func (s *Store) classifySession(ctx context.Context, q Queryer, sessionID string, now time.Time, healthHourly bool) (bool, error) {
	f, err := readSessionFacts(ctx, q, sessionID)
	if err != nil {
		return false, err
	}
	var hasContent bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM events e WHERE e.session_id = $1 AND e.type = ANY($2::text[]))`,
		sessionID, contentEventTypes).Scan(&hasContent); err != nil {
		return false, fmt.Errorf("store: probe content of %s: %w", sessionID, err)
	}

	typ, emptyKind, head := f.Type, f.EmptyKind, f.HeadState
	contentEvents := f.ContentEvents
	pastGrace := f.Ended || now.Sub(f.StartedAt) >= emptyGrace

	// The empty rule. A row can only be born empty and is promoted the
	// moment content arrives, so an empty row that has content is promoted
	// here too (its ingest batches never saw a content row that a later
	// batch delivered under another origin). The other direction is the one
	// the ingest merge refuses and this step is allowed: a user row with no
	// content at all, past the grace, is a session in which nothing
	// happened, whichever revision created it. Automation and internal rows
	// are never touched by it: they were proven, not counted.
	switch {
	case hasContent && typ == "empty":
		typ, emptyKind = "user", nil
	case !hasContent && (typ == "user" || typ == "empty") && pastGrace:
		typ = "empty"
		contentEvents = 0
		if emptyKind == nil {
			k := "tail_truncated"
			if f.Ended {
				k = "aborted"
			}
			emptyKind = &k
		}
	}
	if typ != "empty" {
		emptyKind = nil
	}

	// Head state, origin-aware. The transcript numbers each stream from
	// zero, so a transcript stream whose lowest main-thread seq is not zero
	// lost its head to an import window. Hook seq starts at one and may fall
	// back to nanoseconds, so a hook-only session is judged by the TYPE of
	// its earliest main-thread row by time: a start marker or a prompt is a
	// complete head, anything else is a start that was dropped.
	if hasContent {
		var minTranscript *int64
		if err := q.QueryRow(ctx, `
			SELECT seq FROM events
			WHERE session_id = $1 AND origin = 'transcript' AND agent_id IS NULL
			ORDER BY seq LIMIT 1`, sessionID).Scan(&minTranscript); err != nil && !noRows(err) {
			return false, fmt.Errorf("store: read transcript head of %s: %w", sessionID, err)
		}
		switch {
		case minTranscript != nil && *minTranscript > 0:
			head = "truncated_window"
		case minTranscript != nil:
			head = "complete"
		default:
			var firstType string
			if err := q.QueryRow(ctx, `
				SELECT type FROM events
				WHERE session_id = $1 AND coalesce(agent_id, '') = ''
				ORDER BY occurred_at, coalesce(agent_id, ''), seq LIMIT 1`, sessionID).Scan(&firstType); err != nil && !noRows(err) {
				return false, fmt.Errorf("store: read hook head of %s: %w", sessionID, err)
			}
			if firstType == string(event.SessionStarted) || firstType == string(event.UserPrompt) || firstType == "" {
				head = "complete"
			} else {
				head = "start_lost"
			}
		}
	}
	// Capture loss overrides the rest and is never hidden: the device
	// reported dropping events around this session, so "nothing happened" is
	// not something anyone can say about it.
	if healthHourly {
		var lost bool
		if err := q.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM health_hourly h
				WHERE h.email = $1 AND h.device_id IS NOT DISTINCT FROM $2::uuid
				  AND h.drops > 0
				  AND h.hour >= date_trunc('hour', $3::timestamptz - interval '5 minutes')
				  AND h.hour <= coalesce($4::timestamptz, now()) + interval '5 minutes')`,
			f.Email, f.DeviceID, f.StartedAt, f.EndedAt).Scan(&lost); err != nil {
			return false, fmt.Errorf("store: probe capture loss of %s: %w", sessionID, err)
		}
		if lost {
			head = "capture_loss"
		}
	}

	// Internal, recomputed from the opening prompt through the normalizer:
	// a template landing before the real opening prompt must not stick, and
	// the opening prompt landing later must take the verdict back.
	// Automation is never taken back.
	if typ != "automation" && typ != "empty" {
		var opening *string
		if err := q.QueryRow(ctx, `
			SELECT text FROM messages
			WHERE session_id = $1 AND role = 'user' AND agent_id IS NULL
			  AND kind IN ('human', 'slash_command')
			ORDER BY seq, occurred_at LIMIT 1`, sessionID).Scan(&opening); err != nil && !noRows(err) {
			return false, fmt.Errorf("store: read opening prompt of %s: %w", sessionID, err)
		}
		if opening == nil {
			if err := q.QueryRow(ctx, `
				SELECT text FROM messages
				WHERE session_id = $1 AND role = 'user' AND agent_id IS NULL
				ORDER BY seq, occurred_at LIMIT 1`, sessionID).Scan(&opening); err != nil && !noRows(err) {
				return false, fmt.Errorf("store: read first prompt of %s: %w", sessionID, err)
			}
		}
		internal := false
		if opening != nil {
			_, internal = normalize.IsInternalTemplate(*opening)
		}
		switch {
		case internal && typ == "user":
			typ = "internal"
		case !internal && typ == "internal":
			typ = "user"
		}
	}

	// Lineage from the compaction marker, resolved only when exactly one
	// older session of the same principal holds the record it names, and
	// never to the session itself. Probed per candidate session through the
	// identity index rather than by record uuid alone, which has no index of
	// its own; the candidates are one person's earlier sessions.
	parent, source := f.ParentSessionID, f.LineageSource
	if parent == nil && source == "" && f.ParentRecord != nil && *f.ParentRecord != "" {
		owners, err := lineageOwners(ctx, q, sessionID, f.Email, f.StartedAt, *f.ParentRecord)
		if err != nil {
			return false, err
		}
		if len(owners) == 1 {
			parent, source = &owners[0], "record_uuid"
		}
	}

	changed := typ != f.Type || head != f.HeadState || contentEvents != f.ContentEvents ||
		!equalPtr(emptyKind, f.EmptyKind) || !equalPtr(parent, f.ParentSessionID) || source != f.LineageSource
	if !changed {
		return false, nil
	}
	if _, err := q.Exec(ctx, `
		UPDATE sessions SET
			session_type      = $2,
			empty_kind        = $3,
			head_state        = $4,
			content_events    = $5,
			parent_session_id = $6,
			lineage_source    = $7,
			updated_at        = now()
		WHERE session_id = $1`,
		sessionID, typ, emptyKind, head, contentEvents, parent, source); err != nil {
		return false, fmt.Errorf("store: classify %s: %w", sessionID, err)
	}
	return true, nil
}

// lineageOwnersSQL finds up to two of a person's earlier sessions that hold
// the record a compaction marker names: $1 the session asking, $2 its email,
// $3 its start, $4 the record uuid.
//
// The candidates are gathered first and the identity index is then entered
// by session_id AND record_uuid in one scan. Written as an EXISTS per
// candidate session, the planner bound only record_uuid, the third column
// of events_record_identity_idx, and read the whole index once per
// candidate: 1.5 s for 245 sessions on a 200k-row fixture, minutes for the
// production person with 1,641 sessions, against the 60 s statement ceiling
// (review-1 finding 19). With both columns in the index condition each
// candidate costs one descent into its own entries.
const lineageOwnersSQL = `
	SELECT DISTINCT e.session_id FROM events e
	WHERE e.record_uuid = $4
	  AND e.session_id = ANY (ARRAY(SELECT p.session_id FROM sessions p
	                                WHERE p.email = $2 AND p.session_id <> $1 AND p.started_at <= $3))
	LIMIT 2`

// lineageOwners runs lineageOwnersSQL and returns the owning session ids.
func lineageOwners(ctx context.Context, q Queryer, sessionID, email string, startedAt time.Time, record string) ([]string, error) {
	rows, err := q.Query(ctx, lineageOwnersSQL, sessionID, email, startedAt, record)
	if err != nil {
		return nil, fmt.Errorf("store: resolve lineage of %s: %w", sessionID, err)
	}
	defer rows.Close()
	var owners []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan lineage of %s: %w", sessionID, err)
		}
		owners = append(owners, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: resolve lineage of %s: %w", sessionID, err)
	}
	return owners, nil
}

func equalPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// retitleSession recomputes a session's opening prompt and its provenance
// through the normalizer, from the messages as classified. It is
// refreshFirstPrompt's rule applied to history: the ingest statement adopts
// a rendering only for the row the batch carried, and every row stored
// before kinds existed, and every notification the SQL fallback named by
// its first line, is settled here.
func retitleSession(ctx context.Context, q Queryer, sessionID string) (bool, error) {
	f, err := readSessionFacts(ctx, q, sessionID)
	if err != nil {
		return false, err
	}
	var (
		title  *string
		source = "none"
	)
	var opening *string
	if err := q.QueryRow(ctx, `
		SELECT text FROM messages
		WHERE session_id = $1 AND role = 'user' AND agent_id IS NULL
		  AND kind IN ('human', 'slash_command')
		ORDER BY seq, occurred_at LIMIT 1`, sessionID).Scan(&opening); err != nil && !noRows(err) {
		return false, fmt.Errorf("store: read opening prompt of %s: %w", sessionID, err)
	}
	switch {
	case opening != nil:
		m := normalize.ClassifyUser(*opening, nil, false, false)
		t := normalize.Title(m)
		title = &t
		source = "human"
		if m.Kind == normalize.KindSlashCommand {
			source = "command"
		}
	case f.Type == "automation" || f.Type == "internal":
		var first *string
		if err := q.QueryRow(ctx, `
			SELECT text FROM messages
			WHERE session_id = $1 AND role = 'user' AND agent_id IS NULL
			ORDER BY seq, occurred_at LIMIT 1`, sessionID).Scan(&first); err != nil && !noRows(err) {
			return false, fmt.Errorf("store: read first prompt of %s: %w", sessionID, err)
		}
		if first != nil {
			t := normalize.Title(normalize.ClassifyUser(*first, nil, false, false))
			title = &t
			source = "automation_template"
		}
	}
	if equalPtr(title, f.FirstPrompt) && source == f.TitleSource {
		return false, nil
	}
	if _, err := q.Exec(ctx, `
		UPDATE sessions SET first_prompt = $2, title_source = $3, updated_at = now()
		WHERE session_id = $1`, sessionID, title, source); err != nil {
		return false, fmt.Errorf("store: retitle %s: %w", sessionID, err)
	}
	return true, nil
}

// errSessionBusy reports a session another fold holds; the caller moves on.
var errSessionBusy = errors.New("store: session is being derived elsewhere")
