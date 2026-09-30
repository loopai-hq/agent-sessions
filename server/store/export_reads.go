package store

// The export job's reads: COPY projections over the derived tables, and the
// watermark rows it advances. Everything here is read-only against the
// tables it copies (the transactions are opened READ ONLY) and bounded by
// its own statement ceiling; the only write is the watermark upsert.
//
// The export package owns the run (which partitions, in what order, when
// the watermark moves); this file owns the SQL, so a column added to a
// projection is a change here and in examples/deploy-gcp/analytics/bigquery/tables.sql
// and nowhere else. The types crossing the boundary are plain (strings,
// times, one small struct), because the export package imports this one and
// the reverse would be a cycle.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ExportStatementTimeout is the ceiling on one export COPY, set with SET
// LOCAL inside its own read-only transaction in place of the pool's 30 s.
// Ten minutes: an event day is about 125,000 rows whose bodies detoast at
// roughly 1,700 rows/s on the instance (design.md section 11), so a minute
// or two; the ceiling is there for the day that is ten times that, so it
// ends with an error naming the ceiling instead of holding a snapshot open
// past the job's own 30-minute timeout.
const ExportStatementTimeout = 10 * time.Minute

// Watermark names, one per export stream. They are the primary keys of
// export_watermarks (0021).
const (
	ExportWatermarkSessions = "sessions"
	ExportWatermarkEvents   = "events"
	ExportWatermarkHealth   = "health_hourly"
)

// ExportSession is a session whose row changed since a watermark.
type ExportSession struct {
	SessionID string
	Email     string
	StartedAt time.Time
	UpdatedAt time.Time
}

// errExportNeedsPool is returned when the store's DB cannot pin a
// connection, which COPY needs: the test fakes cannot, a pool can.
var errExportNeedsPool = errors.New("store: export COPY needs a pooled connection")

// ExportNow is the database clock. The watermarks are compared against
// updated_at and ingested_at, which this clock stamped; the job's own clock
// is not the same one.
func (s *Store) ExportNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := s.db.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("store: read the clock: %w", err)
	}
	return now, nil
}

// ExportWatermarks reads every watermark row, by name. A name with no row
// is absent from the map, which the caller reads as "from the beginning".
func (s *Store) ExportWatermarks(ctx context.Context) (map[string]time.Time, error) {
	rows, err := s.db.Query(ctx, `SELECT name, watermark FROM export_watermarks`)
	if err != nil {
		return nil, fmt.Errorf("store: read export watermarks: %w", err)
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var name string
		var at time.Time
		if err := rows.Scan(&name, &at); err != nil {
			return nil, fmt.Errorf("store: scan export watermark: %w", err)
		}
		out[name] = at
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read export watermarks: %w", err)
	}
	return out, nil
}

// ExportAdvanceWatermarks records the positions a successful run reached,
// all in one transaction, so a run's three streams move together or not at
// all. A zero time is a stream the run had no position for (health_hourly
// before its table (0020) exists) and writes no row: a stored year-1 watermark
// would read back as a position two thousand years behind and keep the lag
// alert on for good.
func (s *Store) ExportAdvanceWatermarks(ctx context.Context, marks map[string]time.Time) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for name, at := range marks {
		if at.IsZero() {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO export_watermarks (name, watermark, updated_at) VALUES ($1, $2, now())
			ON CONFLICT (name) DO UPDATE SET watermark = EXCLUDED.watermark, updated_at = now()`, name, at); err != nil {
			return fmt.Errorf("store: advance export watermark %s: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit export watermarks: %w", err)
	}
	return nil
}

// ExportPlanRequest is what a run wants to know before it writes anything:
// the sessions changed since one position, the event days since another,
// the health days since a third. The limits are one more than the run will
// take, so it can tell a cut set from a complete one (export.planSessions
// says how it uses that).
type ExportPlanRequest struct {
	SessionsSince time.Time
	SessionsLimit int
	EventsSince   time.Time
	EventsLimit   int
	HealthSince   time.Time
}

// ExportPlanReads is the answer. HealthTableExists is false while the
// health_hourly table is not there yet: the run then has no health days and,
// more to the point, must not move the health watermark, or the rollups the
// fleet runner writes for the retained week would be behind it before they were ever
// exported.
type ExportPlanReads struct {
	Sessions          []ExportSession
	EventDays         []string
	HealthDays        []string
	HealthTableExists bool
}

// ExportPlan issues the run's planning reads inside one READ ONLY
// transaction that carries the export ceiling. On a pool connection they
// would run under the pool's 30 s statement_timeout, and the events-day read
// is a scan of everything ingested since the watermark: on the first run,
// with no watermark, that is the whole events table, measured at 49 s on a
// production-sized instance without an index on ingested_at. Under 30 s the
// bootstrap fails
// before it has exported anything, every hour, for good. Under the export
// ceiling it finishes; with events_ingested_at_idx (the derive runner's
// index:* step) both this read and the per-day COPY
// are range scans.
//
// SET LOCAL and SET TRANSACTION end with the transaction, so nothing rides
// the connection back into the pool (the same property exportCopy has).
func (s *Store) ExportPlan(ctx context.Context, req ExportPlanRequest) (ExportPlanReads, error) {
	var out ExportPlanReads
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return out, fmt.Errorf("store: begin export plan: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// First, so no read of the plan runs outside the mode. Postgres would
	// accept the statement after a query as well (only the isolation level
	// and a switch back to READ WRITE must precede the first query); the
	// position is this code's choice, and the test pins it.
	if _, err := tx.Exec(ctx, `SET TRANSACTION READ ONLY`); err != nil {
		return out, fmt.Errorf("store: set export plan read-only: %w", err)
	}
	if err := WithStatementTimeout(ctx, tx, ExportStatementTimeout); err != nil {
		return out, err
	}
	if out.Sessions, err = exportTouchedSessions(ctx, tx, req.SessionsSince, req.SessionsLimit); err != nil {
		return out, err
	}
	// UTC days because the BigQuery partition a day is loaded into is
	// DATE(ingested_at) in UTC. The WHERE is a range on the bare column so an
	// index on ingested_at serves it; the to_char is on the output only.
	if out.EventDays, err = exportDays(ctx, tx, `
		SELECT DISTINCT to_char(ingested_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS day
		FROM events WHERE ingested_at > $1 ORDER BY day LIMIT $2`, req.EventsSince, req.EventsLimit); err != nil {
		return out, err
	}
	// health_hourly arrives with migration 0020; the same probe the fleet
	// runner uses. Absent: no days, and the caller keeps the watermark where it is.
	if out.HealthTableExists = s.healthHourlyExists(ctx, tx); out.HealthTableExists {
		if out.HealthDays, err = exportDays(ctx, tx, `
			SELECT DISTINCT to_char(hour AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS day
			FROM health_hourly WHERE hour > $1 ORDER BY day LIMIT $2`, req.HealthSince, 1000); err != nil {
			return out, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("store: end export plan: %w", err)
	}
	return out, nil
}

// exportTouchedSessions lists sessions with updated_at > since, oldest
// change first, then by id so the order is total; sessions_updated_at_idx
// (0006) serves the range.
func exportTouchedSessions(ctx context.Context, q Queryer, since time.Time, limit int) ([]ExportSession, error) {
	rows, err := q.Query(ctx, `
		SELECT session_id, email, started_at, updated_at
		FROM sessions WHERE updated_at > $1
		ORDER BY updated_at, session_id LIMIT $2`, since, limit)
	if err != nil {
		return nil, fmt.Errorf("store: read touched sessions: %w", err)
	}
	defer rows.Close()
	var out []ExportSession
	for rows.Next() {
		var r ExportSession
		if err := rows.Scan(&r.SessionID, &r.Email, &r.StartedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: scan touched session: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read touched sessions: %w", err)
	}
	return out, nil
}

// exportDays runs one distinct-days read and returns the days ascending.
func exportDays(ctx context.Context, q Queryer, sql string, since time.Time, limit int) ([]string, error) {
	rows, err := q.Query(ctx, sql, since, limit)
	if err != nil {
		return nil, fmt.Errorf("store: read export days: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("store: scan export day: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read export days: %w", err)
	}
	return out, nil
}

// exportViewerEmails is the viewer_emails expression for a row owned by
// the address expression owner. BigQuery's governed views filter on
// SESSION_USER() IN UNNEST(viewer_emails), so the array is who may read the
// row: the owner always, and, when the deployment declares DOMAIN_ALIASES,
// the owner's same-local-part address on the aliased domain plus any
// principals row with exactly that local part on either domain of the
// owner's pair. A Workspace that serves two domains lets a person sign in
// as whichever their account carries while their sessions are recorded
// under whichever address the device enrolled with, and the alias is what
// lets them see their own work under either address.
//
// With no aliases the array is the owner alone: no CASE, no principals
// read. The alias branch is generated only for the configured pairs and
// only within the owner's own pair, on purpose: on one Workspace the same
// local part is the same account, so the match cannot name a second person,
// but on any domain not declared as an alias a same-local-part row could be
// somebody else, who would then see this owner's sessions. The clause stays
// as narrow as the configuration so a row added tomorrow cannot create such
// a pair.
//
// The domains are spliced into the statement as literals. They come from
// DOMAIN_ALIASES, which config validates to the hostname alphabet before it
// reaches here, and sqlLiteral doubles any quote regardless.
func exportViewerEmails(owner string, aliases []DomainAlias) string {
	if len(aliases) == 0 {
		return fmt.Sprintf(`ARRAY[%s]`, owner)
	}
	var swap, pairs strings.Builder
	for i, a := range aliases {
		la, lb := sqlLiteral(a.A), sqlLiteral(a.B)
		fmt.Fprintf(&swap, `
			WHEN split_part(%[1]s, '@', 2) = %[2]s THEN split_part(%[1]s, '@', 1) || '@' || %[3]s
			WHEN split_part(%[1]s, '@', 2) = %[3]s THEN split_part(%[1]s, '@', 1) || '@' || %[2]s`, owner, la, lb)
		if i > 0 {
			pairs.WriteString(" OR ")
		}
		fmt.Fprintf(&pairs, `(split_part(%[1]s, '@', 2) IN (%[2]s, %[3]s) AND split_part(p.email, '@', 2) IN (%[2]s, %[3]s))`, owner, la, lb)
	}
	return fmt.Sprintf(`(SELECT array_agg(DISTINCT v ORDER BY v) FROM (
		SELECT %[1]s AS v
		UNION ALL SELECT CASE%[2]s
		END
		UNION ALL SELECT p.email FROM principals p
			WHERE split_part(p.email, '@', 1) = split_part(%[1]s, '@', 1)
			  AND (%[3]s)
	) a WHERE v IS NOT NULL)`, owner, swap.String(), pairs.String())
}

// sqlLiteral renders s as a single-quoted SQL string literal.
func sqlLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// The projections, one per exported table. Each is the column list of a
// SELECT whose FROM the caller supplies; the aliases are s (sessions), t
// (turns), m (messages), e (events), h (health_hourly). exported_at is the
// statement's clock, so a row in BigQuery says when it was copied.
//
// They are templates: viewerEmailsOf marks where the viewer_emails
// expression goes, and resolveViewerEmails fills it in for the deployment's
// domain aliases at the point of use, because the expression depends on
// DOMAIN_ALIASES and this block does not. It stays one var block of strings
// on purpose: deploy/analytics/provision_test.go reads it as text, from
// "exportTurnColumns = " to the closing paren, and checks every projected
// column against the BigQuery DDL.
//
// The session-keyed projections carry the session's start as
// session_started_at, which is the BigQuery partition column for turns and
// messages (export package comment says why the session's day and not the
// row's).
var (
	exportTurnColumns = `t.session_id, t.thread, t.turn_index, t.turn_key, s.email, ` +
		viewerEmailsOf("s.email") + ` AS viewer_emails, s.source, s.started_at AS session_started_at,
		t.kind, t.prompt_event_id, t.final_event_id, t.outcome, t.inherited,
		t.started_at, t.first_activity_at, t.last_activity_at, t.answered_at,
		t.wall_ms, t.active_ms, t.idle_ms, t.waiting_for_human_ms,
		t.origins, t.merged, t.prompts, t.tool_calls, t.errors, t.subagents, t.files_changed,
		t.tokens_input, t.tokens_output, t.tokens_cache_read, t.tokens_cache_write, t.cost_usd,
		t.model, t.derived_version, t.derived_at, now() AS exported_at`

	// mirror_request (a per-session Slack preference) and derive_dirty (the
	// runner's queue flag) are the two columns left out: neither describes
	// the session.
	exportSessionColumns = `s.session_id, s.email, ` + viewerEmailsOf("s.email") + ` AS viewer_emails,
		s.device_id, s.source, s.parent_session_id, s.lineage_source, s.parent_record_uuid,
		s.cwd, s.repo, s.git_branch, s.started_at, s.ended_at, s.ended,
		s.session_type, s.empty_kind, s.head_state, s.entrypoint, s.launcher, s.transcript_exists,
		s.user_turns, s.human_turns, s.tool_calls, s.subagents, s.errors, s.content_events,
		s.first_prompt, s.title_source, s.harness_title, s.harness_versions, s.agent_versions,
		s.tokens_input, s.tokens_output, s.tokens_cache_read, s.tokens_cache_write, s.cost_usd,
		s.redactions, s.ingested_at, s.updated_at, now() AS exported_at`

	// origin and superseded_by come from the event row so a query can keep
	// one copy of a turn's text (WHERE superseded_by IS NULL).
	exportMessageColumns = `m.event_id, m.session_id, m.email, ` + viewerEmailsOf("m.email") + ` AS viewer_emails,
		s.started_at AS session_started_at, m.seq, m.role, m.kind, m.agent_id, m.occurred_at,
		e.origin, e.superseded_by, m.text, now() AS exported_at`

	// body - 'raw': the derived half of the record. The raw transcript line
	// is the larger half and the one nothing analytical reads; it stays in
	// Postgres, where the record of truth is.
	exportEventColumns = `e.id, e.session_id, e.email, ` + viewerEmailsOf("e.email") + ` AS viewer_emails,
		e.seq, e.type, e.origin, e.occurred_at, e.ingested_at, e.agent_id, e.workflow_id,
		e.model, e.tool_name, e.capture_version, e.prompt_id, e.record_uuid, e.parent_record_uuid,
		e.tool_use_id, e.message_id, e.request_id, e.superseded_by,
		(e.body_expired_at IS NOT NULL) AS body_expired, e.body - 'raw' AS body, now() AS exported_at`

	exportHealthColumns = `h.email, ` + viewerEmailsOf("h.email") + ` AS viewer_emails, h.device_id, h.hour,
		h.reports, h.worst, h.conditions, h.drops, h.quarantined, h.parked, h.capture_blocked_secs,
		h.empty_starts, now() AS exported_at`
)

// viewerEmailsOf marks a projection template where the viewer_emails
// expression for the owner column goes. It is text, not SQL: no statement
// is built from a template until resolveViewerEmails has replaced it.
func viewerEmailsOf(owner string) string { return "{{viewer_emails " + owner + "}}" }

var viewerEmailsMark = regexp.MustCompile(`\{\{viewer_emails ([a-z]\.email)\}\}`)

// resolveViewerEmails fills a projection template in for the aliases.
func resolveViewerEmails(template string, aliases []DomainAlias) string {
	return viewerEmailsMark.ReplaceAllStringFunc(template, func(m string) string {
		return exportViewerEmails(viewerEmailsMark.FindStringSubmatch(m)[1], aliases)
	})
}

// exportDayBounds is the CTE every partition COPY reads its day from: the
// half-open UTC interval [lo, hi) of the day in the export.day setting,
// which the transaction sets with set_config before the COPY runs. COPY
// takes no parameters, and a day spliced into the SQL as text would be
// the one place this package built a statement from a string; a setting
// is read back typed.
const exportDayBounds = `WITH b AS (
	SELECT (current_setting('export.day', true)::date)::timestamp AT TIME ZONE 'UTC' AS lo,
	       (current_setting('export.day', true)::date + 1)::timestamp AT TIME ZONE 'UTC' AS hi
)`

// exportCopyFormat is the COPY output form. Text-format COPY escapes
// backslashes and newlines in its output, which turns the "\n" inside a
// JSON string into "\\n" and breaks every line that carries one. CSV
// format with a quote and a delimiter that no JSON text can contain (the
// two bytes 0x01 and 0x02; row_to_json writes control characters as \u
// escapes) writes each row_to_json value out exactly as it is, one per
// line. Verified on Postgres 17 with a value carrying a backslash, a quote,
// a newline and both bytes.
const exportCopyFormat = `TO STDOUT (FORMAT csv, QUOTE E'\x01', DELIMITER E'\x02', ESCAPE E'\x01')`

// exportPartitionSQL is the COPY for one table and one day, with
// viewer_emails resolved through the deployment's domain aliases.
func exportPartitionSQL(table string, aliases []DomainAlias) (string, error) {
	var from, where string
	var cols string
	switch table {
	case "turns":
		cols, from, where = resolveViewerEmails(exportTurnColumns, aliases), `turns t JOIN sessions s USING (session_id), b`, `s.started_at >= b.lo AND s.started_at < b.hi`
	case "sessions":
		cols, from, where = resolveViewerEmails(exportSessionColumns, aliases), `sessions s, b`, `s.started_at >= b.lo AND s.started_at < b.hi`
	case "messages":
		cols, from, where = resolveViewerEmails(exportMessageColumns, aliases), `messages m JOIN sessions s USING (session_id) JOIN events e ON e.id = m.event_id, b`, `s.started_at >= b.lo AND s.started_at < b.hi`
	case "events":
		cols, from, where = resolveViewerEmails(exportEventColumns, aliases), `events e, b`, `e.ingested_at >= b.lo AND e.ingested_at < b.hi`
	case "health_hourly":
		cols, from, where = resolveViewerEmails(exportHealthColumns, aliases), `health_hourly h, b`, `h.hour >= b.lo AND h.hour < b.hi`
	default:
		return "", fmt.Errorf("store: no export projection for table %q", table)
	}
	return fmt.Sprintf(`COPY (%s SELECT row_to_json(r) FROM (SELECT %s FROM %s WHERE %s) r) %s`,
		exportDayBounds, cols, from, where, exportCopyFormat), nil
}

// exportBundleSQL is the COPY for one session's bundle: the session line,
// then its turns by thread and index, then its events by thread and time,
// each line stamped with what it is in "record" (session, turn, event).
// Not "kind": a turn has a kind of its own (human, slash_command), and a
// second key of the same name would be the one a JSON reader kept. The
// session id comes from the export.sid setting for the reason
// exportDayBounds gives.
//
// Two kinds of event are left out of a bundle, and only of a bundle (the
// events partition carries every row): compaction records, whose body is
// the harness's own summary of the conversation so far (26,953 of them at
// 124 KB on average in research/r2, F5: a session with a few is several
// hundred KB of text nobody typed), and the prompts that are a
// <system-reminder> block on their own (messages.kind = 'system_reminder',
// internal/normalize), which the harness injects and a reader of the
// transcript mistakes for the person. A bundle is what an LLM batch job or
// a jq user takes whole, and what it should hold is the exchange.
func exportBundleSQL(aliases []DomainAlias) string {
	return fmt.Sprintf(`COPY (
	SELECT line FROM (
		SELECT 0 AS part, ''::text AS k1, s.started_at AS k2, 0::bigint AS k3, row_to_json(r)::text AS line
		FROM sessions s, LATERAL (SELECT 'session' AS record, %s) r
		WHERE s.session_id = current_setting('export.sid', true)
		UNION ALL
		SELECT 1, t.thread, t.started_at, t.turn_index::bigint, row_to_json(r)::text
		FROM turns t JOIN sessions s USING (session_id), LATERAL (SELECT 'turn' AS record, %s) r
		WHERE t.session_id = current_setting('export.sid', true)
		UNION ALL
		SELECT 2, coalesce(e.agent_id, ''), e.occurred_at, e.seq, row_to_json(r)::text
		FROM events e, LATERAL (SELECT 'event' AS record, %s) r
		WHERE e.session_id = current_setting('export.sid', true)
		  AND e.type <> 'compaction'
		  AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.event_id = e.id AND m.kind = 'system_reminder')
	) x ORDER BY part, k1, k2, k3
) %s`, resolveViewerEmails(exportSessionColumns, aliases), resolveViewerEmails(exportTurnColumns, aliases), resolveViewerEmails(exportEventColumns, aliases), exportCopyFormat)
}

// ExportCopyPartition streams one table's rows for one UTC day (2006-01-02)
// to w as JSON lines and reports how many.
func (s *Store) ExportCopyPartition(ctx context.Context, table, day string, w io.Writer) (int64, error) {
	if _, err := time.Parse("2006-01-02", day); err != nil {
		return 0, fmt.Errorf("store: export day %q: %w", day, err)
	}
	sql, err := exportPartitionSQL(table, s.deployment.DomainAliases)
	if err != nil {
		return 0, err
	}
	return s.exportCopy(ctx, [][2]string{{"export.day", day}}, sql, w)
}

// ExportCopyBundle streams one session's bundle to w as JSON lines.
func (s *Store) ExportCopyBundle(ctx context.Context, sessionID string, w io.Writer) (int64, error) {
	if strings.TrimSpace(sessionID) == "" {
		return 0, errors.New("store: export bundle needs a session id")
	}
	return s.exportCopy(ctx, [][2]string{{"export.sid", sessionID}}, exportBundleSQL(s.deployment.DomainAliases), w)
}

// exportCopy runs one COPY ... TO STDOUT on a pinned connection, inside a
// read-only transaction that carries the export ceiling, UTC as the session
// time zone (so row_to_json renders every timestamp with a +00:00 offset,
// which BigQuery's TIMESTAMP reads), and the settings the statement reads
// with current_setting. The transaction is what scopes all three: SET LOCAL
// and set_config(..., true) end with it, so nothing rides the connection
// back to the pool.
func (s *Store) exportCopy(ctx context.Context, settings [][2]string, sql string, w io.Writer) (int64, error) {
	acquirer, ok := s.db.(connSource)
	if !ok {
		return 0, errExportNeedsPool
	}
	c, err := acquirer.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: acquire a connection: %w", err)
	}
	defer c.Release()
	pc, ok := c.(poolConn)
	if !ok {
		return 0, errExportNeedsPool
	}
	tx, err := pc.conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, fmt.Errorf("store: begin export read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := WithStatementTimeout(ctx, pgxTx{tx: tx}, ExportStatementTimeout); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `SET LOCAL TIME ZONE 'UTC'`); err != nil {
		return 0, fmt.Errorf("store: set export time zone: %w", err)
	}
	for _, kv := range settings {
		if _, err := tx.Exec(ctx, `SELECT set_config($1, $2, true)`, kv[0], kv[1]); err != nil {
			return 0, fmt.Errorf("store: set %s: %w", kv[0], err)
		}
	}
	// The COPY runs on the transaction's connection, so it is inside the
	// transaction and under the settings above; pgx's Tx has no CopyTo of
	// its own, which is why the underlying connection is reached for.
	tag, err := pc.conn.Conn().PgConn().CopyTo(ctx, w, sql)
	if err != nil {
		return 0, fmt.Errorf("store: export copy: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("store: end export read: %w", err)
	}
	return tag.RowsAffected(), nil
}
