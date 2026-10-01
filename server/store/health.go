package store

// Health rollups: the newest report per machine, the hourly ledger the fleet
// alerts read, the seven-day sweep of the raw rows, and the mute table.
//
// health_reports is the only thing the fleet ever learns about a laptop, and
// until now it was also the only place it was kept: 20k rows a day that
// every fleet read scanned for the newest row per device. Migration 0020
// adds the two derived tables this file maintains. health_latest is written
// in PutHealthReport's transaction and read by everything that wants "what
// did this machine last say". health_hourly is recomputed from the raw rows
// for any window, which is what lets the raw rows go after seven days: the
// hourly ledger keeps the counts, the levels, the drops and the seconds spent
// capture-blocked, and the capture-loss rule in turns.go reads it.
//
// The sweep runs from two places on purpose. The retention loop calls it
// after a retention pass, which is where storage hygiene belongs; but the
// shipped retention policy keeps everything and starts no sweeper at all, so
// the fleet evaluator calls it once an hour as well. Both paths are
// idempotent and both take the retention lock per batch, so they never
// overlap and neither cares which one ran last. The tick's own two-hour
// rollup takes the same lock: it and a sweep batch upsert the same hours,
// and two ON CONFLICT DO UPDATE statements over the same keys in different
// orders can deadlock, so they take turns and a tick that finds a batch in
// flight stands down for one interval.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/health"
)

const (
	// healthRawRetention is how long a raw health report is kept. Seven days
	// covers a laptop that queued a week of reports offline and delivered
	// them in one burst, and every alert reads the hourly ledger rather than
	// the raw rows.
	healthRawRetention = 7 * 24 * time.Hour
	// healthSweepBatch bounds one delete statement. Five thousand rows is a
	// few megabytes of TOAST and a transaction that commits in well under a
	// second, so the health post path never queues behind the sweep.
	healthSweepBatch = 5000
	// healthSweepBudget bounds one sweep pass, for the retention sweep's
	// reason: a pass is not required to finish, and the next one continues.
	healthSweepBudget = 2 * time.Minute
	// healthSweepStatementTimeout caps each statement the sweep issues, so a
	// rollup over a day that a burst of late deliveries made unusually large
	// fails that batch alone rather than holding a connection.
	healthSweepStatementTimeout = 30 * time.Second
	// healthRollupHorizon bounds how far back a rollup reads the raw table
	// when the oldest raw row is older than anything the ledger wants: a
	// clock-skewed report stamped years ago must not widen the window.
	healthRollupHorizon = 60 * 24 * time.Hour
	// healthReportCadence is the client's report period. The seconds a
	// machine spent capture-blocked in an hour are counted as one cadence
	// per report that carried the condition, which is exact to within one
	// period and needs no second table.
	healthReportCadence = 5 * time.Minute
)

// HealthLatest is the newest report one machine sent. Report is the raw JSON
// rather than a decoded health.Report because a report from a client newer
// than this server carries fields this server's struct does not know, and
// the evaluator reads those fields (schema version 2 and later) by name,
// by schema version: absent on a version 2 report is the client's omitempty
// and means zero; a version 1 report has no such field and reads unknown.
type HealthLatest struct {
	Email        string
	DeviceID     string
	EmittedAt    time.Time
	ReceivedAt   time.Time
	Worst        string
	AgentVersion string
	Report       json.RawMessage
}

// HealthLatestRows returns the newest report per machine, every machine that
// ever reported, oldest arrival first so a page can sort stably.
func (s *Store) HealthLatestRows(ctx context.Context) ([]HealthLatest, error) {
	rows, err := s.db.Query(ctx, `
		SELECT email, device_id::text, emitted_at, received_at, worst, agent_version, report::text
		FROM health_latest
		ORDER BY email, device_id`)
	if err != nil {
		return nil, fmt.Errorf("store: latest health rows: %w", err)
	}
	defer rows.Close()
	var out []HealthLatest
	for rows.Next() {
		var (
			h                 HealthLatest
			deviceID, version *string
			body              string
		)
		if err := rows.Scan(&h.Email, &deviceID, &h.EmittedAt, &h.ReceivedAt, &h.Worst, &version, &body); err != nil {
			return nil, fmt.Errorf("store: scan latest health: %w", err)
		}
		h.DeviceID = deref(deviceID)
		h.AgentVersion = deref(version)
		h.Report = json.RawMessage(body)
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read latest health: %w", err)
	}
	return out, nil
}

// HealthBuildSighting is one build a machine reported in a window, with the
// last moment a report carried it.
type HealthBuildSighting struct {
	Email    string
	DeviceID string
	Build    string
	LastSeen time.Time
}

// HealthRecentBuilds reads every build each machine named in a report that
// arrived at or after since, most recently seen first per machine. Arrival
// stamped rather than report stamped, because the question is what the
// machine is running now and a queued sample delivered late still answers
// it; the build is the version 2 commit when present, else agent_version,
// the same key Report.Build reads. The window is small (a dozen reports per
// machine an hour) and health_reports_received_at_idx bounds the scan.
func (s *Store) HealthRecentBuilds(ctx context.Context, since time.Time) ([]HealthBuildSighting, error) {
	rows, err := s.db.Query(ctx, `
		SELECT email, device_id::text,
		       coalesce(nullif(report->>'agent_commit', ''), nullif(report->>'agent_version', ''), '') AS build,
		       max(emitted_at) AS last_seen
		FROM health_reports
		WHERE received_at >= $1
		GROUP BY 1, 2, 3
		HAVING coalesce(nullif(report->>'agent_commit', ''), nullif(report->>'agent_version', ''), '') <> ''
		ORDER BY 1, 2, 4 DESC`, since)
	if err != nil {
		return nil, fmt.Errorf("store: recent health builds: %w", err)
	}
	defer rows.Close()
	var out []HealthBuildSighting
	for rows.Next() {
		var (
			b        HealthBuildSighting
			deviceID *string
		)
		if err := rows.Scan(&b.Email, &deviceID, &b.Build, &b.LastSeen); err != nil {
			return nil, fmt.Errorf("store: scan recent health build: %w", err)
		}
		b.DeviceID = deref(deviceID)
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read recent health builds: %w", err)
	}
	return out, nil
}

// HealthHour is one machine's hour in the ledger.
type HealthHour struct {
	Email    string
	DeviceID string
	Hour     time.Time
	Reports  int
	Worst    string
	// Conditions is {"kinds": {"<kind>/<level>": reports}, "dropped":
	// {"<reason>": events}, "totals": {"<reason>": counter}, "last_at":
	// <moment>}: the condition census and the drops by reason, both as this
	// hour's counts, plus the counters the hour's last report showed and
	// when it was emitted, which is what a rollup seeds from once the raw
	// rows before an hour have been swept.
	Conditions  json.RawMessage
	Drops       int64
	Quarantined int64
	// Parked and EmptyStarts are nil for an hour built from version 1
	// reports, which carry neither; a reader must not take that for zero. A
	// version 2 report that omits them said zero.
	Parked             *int64
	CaptureBlockedSecs int
	EmptyStarts        *int
}

// DroppedByReason decodes the hour's drops by reason.
func (h HealthHour) DroppedByReason() map[string]int64 {
	var c struct {
		Dropped map[string]int64 `json:"dropped"`
	}
	if len(h.Conditions) > 0 {
		_ = json.Unmarshal(h.Conditions, &c)
	}
	return c.Dropped
}

// HealthHours returns the ledger rows at or after since, oldest first.
func (s *Store) HealthHours(ctx context.Context, since time.Time) ([]HealthHour, error) {
	rows, err := s.db.Query(ctx, `
		SELECT email, device_id::text, hour, reports, worst, conditions::text,
		       drops, quarantined, parked, capture_blocked_secs, empty_starts
		FROM health_hourly
		WHERE hour >= $1
		ORDER BY hour, email, device_id`, since)
	if err != nil {
		return nil, fmt.Errorf("store: health hours: %w", err)
	}
	defer rows.Close()
	var out []HealthHour
	for rows.Next() {
		var (
			h        HealthHour
			deviceID *string
			conds    string
		)
		if err := rows.Scan(&h.Email, &deviceID, &h.Hour, &h.Reports, &h.Worst, &conds,
			&h.Drops, &h.Quarantined, &h.Parked, &h.CaptureBlockedSecs, &h.EmptyStarts); err != nil {
			return nil, fmt.Errorf("store: scan health hour: %w", err)
		}
		h.DeviceID = deref(deviceID)
		h.Conditions = json.RawMessage(conds)
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read health hours: %w", err)
	}
	return out, nil
}

// healthRollupSQL recomputes health_hourly for every hour in [$1, $2) from
// the raw rows. Each machine's series is seeded with its newest state from
// before the window, so the first in-window report has the same predecessor
// it would have in any window and the delta it is charged with does not
// depend on where the window starts: the tick's two-hour window and the
// sweep's day window write the same row, and a machine that comes back
// after a gap longer than either (asleep overnight; the daemon dead while
// the hooks kept dropping) has its rise charged to the hour it came back in
// rather than zeroed by whichever rollup ran last. The seed is the newest
// raw row before the window or, once the sweep has deleted that, the
// ledger's newest hour before the window replayed as a row carrying the
// counters its last report showed (the totals and last_at the hour keeps):
// recomputing the oldest retained hour after its predecessors are gone
// therefore writes what it wrote before, and a machine back from a week
// away is charged from where the ledger last saw it. A machine with neither
// is on its first report ever, which is a baseline and not a loss.
//
// The counters a client reports are cumulative since its daemon started, so
// what an hour is charged with is the rise between consecutive reports; a
// counter that went down restarted from zero, and everything it now shows is
// new. Each reason is a series of its own, because the map's keys change as
// the client learns new ways to lose data and a sum across reasons would hide
// a reset in one of them. Worst is the highest level any report in the hour
// carried; quarantined and parked are the hour's high-water marks; the
// capture-blocked seconds are one cadence per report that carried the
// condition; empty starts is the largest 24 h count the client reported.
// Parked and empty starts are read by schema version, not by key presence:
// the version 2 client omits a zero, so on a version 2 report an absent
// key is 0, while an hour whose reports were all version 1 (which carry
// neither field) is NULL: unknown, never zero.
const healthRollupSQL = `
	WITH inside AS (
		SELECT email, device_id, emitted_at, worst, report
		  FROM health_reports
		 WHERE emitted_at >= $1::timestamptz AND emitted_at < $2::timestamptz
	),
	seed AS (
		-- Each machine's newest state before the window: its newest raw
		-- row, or the ledger's newest hour replayed as a row carrying the
		-- hour's closing counters. The newer wins; at the same moment the
		-- raw row does.
		SELECT p.email, p.device_id, p.emitted_at, p.worst, p.report
		  FROM (SELECT DISTINCT email, device_id FROM inside) m,
		       LATERAL (SELECT x.email, x.device_id, x.emitted_at, x.worst, x.report
		                  FROM ((SELECT 0 AS pri, h.email, h.device_id, h.emitted_at, h.worst, h.report
		                           FROM health_reports h
		                          WHERE h.email = m.email AND h.device_id IS NOT DISTINCT FROM m.device_id
		                            AND h.emitted_at < $1::timestamptz
		                          ORDER BY h.emitted_at DESC LIMIT 1)
		                        UNION ALL
		                        (SELECT 1 AS pri, l.email, l.device_id, (l.conditions->>'last_at')::timestamptz, l.worst,
		                                jsonb_build_object('spool', jsonb_build_object('dropped', coalesce(l.conditions->'totals', '{}'::jsonb)))
		                           FROM health_hourly l
		                          WHERE l.email = m.email AND l.device_id IS NOT DISTINCT FROM m.device_id
		                            AND l.hour < date_trunc('hour', $1::timestamptz)
		                            AND l.conditions->>'last_at' IS NOT NULL
		                          ORDER BY l.hour DESC LIMIT 1)) x
		                 ORDER BY x.emitted_at DESC, x.pri LIMIT 1) p
	),
	r AS (
		SELECT email, device_id, emitted_at, worst, report,
		       date_trunc('hour', emitted_at) AS hour,
		       lag(emitted_at) OVER (PARTITION BY email, device_id ORDER BY emitted_at) IS NULL AS device_first
		  FROM (SELECT * FROM inside UNION ALL SELECT * FROM seed) u
	),
	dropped AS (
		-- $4 is health.NotLossReasons(): the counters the spool keeps beside
		-- the drops that are not losses (a recovery from a transcript is the
		-- one today). Passing the list in rather than repeating it here is
		-- what keeps the ledger and the client's own drops_recorded
		-- condition from disagreeing about which counters are losses.
		SELECT r.email, r.device_id, r.emitted_at, r.hour, r.device_first, d.key AS reason,
		       CASE WHEN d.value ~ '^[0-9]+$' THEN d.value::bigint ELSE 0 END AS total
		  FROM r, jsonb_each_text(coalesce(r.report->'spool'->'dropped', '{}'::jsonb)) d
		 WHERE d.key <> ALL($4::text[])
	),
	deltas AS (
		SELECT email, device_id, hour, reason,
		       CASE
		         -- A reason seen for the first time on a machine that reported
		         -- before is a counter that went from absent to its value; on
		         -- a machine's first report ever it is history nothing can
		         -- place, not this hour's loss.
		         WHEN prev IS NULL THEN CASE WHEN device_first THEN 0 ELSE total END
		         WHEN total < prev THEN total
		         ELSE total - prev
		       END AS delta
		  FROM (SELECT dropped.*,
		               lag(total) OVER (PARTITION BY email, device_id, reason ORDER BY emitted_at) AS prev
		          FROM dropped) x
	),
	by_reason AS (
		SELECT email, device_id, hour, reason, sum(delta) AS delta
		  FROM deltas GROUP BY 1, 2, 3, 4
	),
	drops_by_hour AS (
		SELECT email, device_id, hour, sum(delta) AS drops,
		       jsonb_object_agg(reason, delta) FILTER (WHERE delta > 0) AS reasons
		  FROM by_reason GROUP BY 1, 2, 3
	),
	kinds AS (
		SELECT email, device_id, hour, jsonb_object_agg(k, n) AS kinds
		  FROM (SELECT r.email, r.device_id, r.hour,
		               coalesce(c->>'kind', '') || '/' || coalesce(c->>'level', '') AS k, count(*) AS n
		          FROM r, jsonb_array_elements(coalesce(r.report->'conditions', '[]'::jsonb)) c
		         GROUP BY 1, 2, 3, 4) z
		 GROUP BY 1, 2, 3
	),
	hours AS (
		SELECT r.email, r.device_id, r.hour,
		       count(*) AS reports,
		       max(CASE r.worst WHEN 'critical' THEN 3 WHEN 'degraded' THEN 2 ELSE 1 END) AS worst_rank,
		       max(CASE WHEN r.report->'spool'->>'quarantine' ~ '^[0-9]+$' THEN (r.report->'spool'->>'quarantine')::bigint ELSE 0 END) AS quarantined,
		       max(CASE WHEN r.report->'spool'->>'parked' ~ '^[0-9]+$' THEN (r.report->'spool'->>'parked')::bigint
		                WHEN r.report->>'schema_version' ~ '^[0-9]+$' AND (r.report->>'schema_version')::int >= 2 THEN 0 END) AS parked,
		       least(3600, $3::int * count(*) FILTER (WHERE EXISTS (
		           SELECT 1 FROM jsonb_array_elements(coalesce(r.report->'conditions', '[]'::jsonb)) c
		            WHERE c->>'kind' = 'capture_blocked')))::int AS blocked_secs,
		       max(CASE WHEN r.report->>'empty_starts_24h' ~ '^[0-9]+$' THEN (r.report->>'empty_starts_24h')::int
		                WHEN r.report->>'schema_version' ~ '^[0-9]+$' AND (r.report->>'schema_version')::int >= 2 THEN 0 END) AS empty_starts,
		       (array_agg(coalesce(r.report->'spool'->'dropped', '{}'::jsonb) ORDER BY r.emitted_at DESC))[1] AS totals,
		       max(r.emitted_at) AS last_at
		  FROM r
		 WHERE r.hour >= $1::timestamptz AND r.hour < $2::timestamptz
		 GROUP BY 1, 2, 3
	)
	INSERT INTO health_hourly (email, device_id, hour, reports, worst, conditions,
	                           drops, quarantined, parked, capture_blocked_secs, empty_starts)
	SELECT h.email, h.device_id, h.hour, h.reports,
	       CASE h.worst_rank WHEN 3 THEN 'critical' WHEN 2 THEN 'degraded' ELSE 'info' END,
	       jsonb_build_object('kinds', coalesce(k.kinds, '{}'::jsonb),
	                          'dropped', coalesce(d.reasons, '{}'::jsonb),
	                          'totals', coalesce(h.totals, '{}'::jsonb),
	                          'last_at', h.last_at),
	       coalesce(d.drops, 0), h.quarantined, h.parked, h.blocked_secs, h.empty_starts
	  FROM hours h
	  LEFT JOIN drops_by_hour d ON d.email = h.email AND d.device_id IS NOT DISTINCT FROM h.device_id AND d.hour = h.hour
	  LEFT JOIN kinds k ON k.email = h.email AND k.device_id IS NOT DISTINCT FROM h.device_id AND k.hour = h.hour
	ON CONFLICT (email, device_id, hour) DO UPDATE SET
		reports              = EXCLUDED.reports,
		worst                = EXCLUDED.worst,
		conditions           = EXCLUDED.conditions,
		drops                = EXCLUDED.drops,
		quarantined          = EXCLUDED.quarantined,
		parked               = EXCLUDED.parked,
		capture_blocked_secs = EXCLUDED.capture_blocked_secs,
		empty_starts         = EXCLUDED.empty_starts`

// RollupHealth recomputes the hourly ledger for [from, to) from the raw
// rows, under the retention lock, and reports how many hour rows it wrote
// and whether it ran. It is what the evaluator calls every tick for the
// current and previous hour, so the capture-loss rule and the drop deltas
// see a report within minutes of its arrival; the sweep runs the same batch
// a day at a time over the history. The lock is the sweep's on purpose:
// both upsert the same hours, and two ON CONFLICT DO UPDATE statements over
// the same keys in different orders can deadlock and fail a tick or a
// batch, so they take turns. A tick that finds a sweep batch in flight
// stands down (locked false) and its deltas are the next tick's.
func (s *Store) RollupHealth(ctx context.Context, from, to time.Time) (rolled int64, locked bool, err error) {
	return s.rollupHealthBatch(ctx, from, to)
}

// HealthSweep is what one sweep pass did.
type HealthSweep struct {
	HoursRolled int64
	RawDeleted  int64
	Batches     int
	Elapsed     time.Duration
	// Deferred means another sweep held the lock and this one stood down.
	Deferred bool
	// Incomplete means work remained when the budget ran out; the next pass
	// continues from the same cutoff.
	Incomplete bool
}

// LogValue keeps the sweep line to one group.
func (h HealthSweep) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("hours_rolled", h.HoursRolled),
		slog.Int64("raw_deleted", h.RawDeleted),
		slog.Int("batches", h.Batches),
		slog.String("elapsed", h.Elapsed.Round(time.Millisecond).String()),
		slog.Bool("deferred", h.Deferred),
		slog.Bool("incomplete", h.Incomplete),
	)
}

// SweepHealth rolls the raw history up into the hourly ledger and then
// deletes raw rows that arrived more than seven days ago, in bounded batches
// under the retention lock.
//
// The rollup runs first, over every hour the remaining raw rows cover, so
// that no row is deleted before the hour it belongs to has been written; a
// laptop that delivered a week-old backlog yesterday has its old hours
// recomputed on the next pass rather than lost. Bucketed by the report's own
// clock (emitted_at) and deleted by ours (received_at), which is the only
// clock a retention rule can trust.
func (s *Store) SweepHealth(ctx context.Context, now time.Time) (HealthSweep, error) {
	started := time.Now()
	var sweep HealthSweep

	var oldest *time.Time
	if err := s.db.QueryRow(ctx, `SELECT min(emitted_at) FROM health_reports`).Scan(&oldest); err != nil {
		return sweep, fmt.Errorf("store: find the oldest health report: %w", err)
	}
	if oldest != nil {
		from := oldest.UTC().Truncate(24 * time.Hour)
		if floor := now.Add(-healthRollupHorizon); from.Before(floor) {
			from = floor.UTC().Truncate(24 * time.Hour)
		}
		for day := from; day.Before(now); day = day.Add(24 * time.Hour) {
			if stop, incomplete := retentionStop(ctx, started, healthSweepBudget); stop {
				sweep.Incomplete = sweep.Incomplete || incomplete
				sweep.Elapsed = time.Since(started)
				return sweep, nil
			}
			n, locked, err := s.rollupHealthBatch(ctx, day, day.Add(24*time.Hour))
			if err != nil {
				return sweep, err
			}
			if !locked {
				sweep.Deferred = true
				sweep.Elapsed = time.Since(started)
				return sweep, nil
			}
			sweep.Batches++
			sweep.HoursRolled += n
		}
	}

	cutoff := now.Add(-healthRawRetention)
	for {
		if stop, incomplete := retentionStop(ctx, started, healthSweepBudget); stop {
			sweep.Incomplete = sweep.Incomplete || incomplete
			break
		}
		n, locked, err := s.deleteHealthBatch(ctx, cutoff, healthSweepBatch)
		if err != nil {
			return sweep, err
		}
		if !locked {
			sweep.Deferred = true
			break
		}
		sweep.Batches++
		sweep.RawDeleted += n
		if n < healthSweepBatch {
			break
		}
	}
	sweep.Elapsed = time.Since(started)
	return sweep, nil
}

// rollupHealthBatch recomputes one window of the ledger under the retention
// lock. The lock is transaction-scoped and re-taken per batch, as the
// retention sweep's is, so two instances interleave batches harmlessly and
// never sit inside one at the same time; the tick's rollup is one of these
// batches.
func (s *Store) rollupHealthBatch(ctx context.Context, from, to time.Time) (rolled int64, locked bool, err error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("store: begin health rollup batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	locked, err = retentionLocked(ctx, tx)
	if err != nil || !locked {
		return 0, locked, err
	}
	if err := WithStatementTimeout(ctx, tx, healthSweepStatementTimeout); err != nil {
		return 0, true, err
	}
	n, err := tx.Exec(ctx, healthRollupSQL, from, to, int(healthReportCadence.Seconds()), health.NotLossReasons())
	if err != nil {
		return 0, true, fmt.Errorf("store: roll up health hours %s to %s: %w",
			from.Format(time.RFC3339), to.Format(time.RFC3339), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, true, fmt.Errorf("store: commit health rollup batch: %w", err)
	}
	return n, true, nil
}

// deleteHealthBatch removes up to limit raw rows that arrived before cutoff,
// oldest first through health_reports_received_at_idx.
func (s *Store) deleteHealthBatch(ctx context.Context, cutoff time.Time, limit int) (deleted int64, locked bool, err error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("store: begin health delete batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	locked, err = retentionLocked(ctx, tx)
	if err != nil || !locked {
		return 0, locked, err
	}
	if err := WithStatementTimeout(ctx, tx, healthSweepStatementTimeout); err != nil {
		return 0, true, err
	}
	n, err := tx.Exec(ctx, `
		DELETE FROM health_reports
		WHERE id IN (
			SELECT id FROM health_reports
			WHERE received_at < $1
			ORDER BY received_at
			LIMIT $2
		)`, cutoff, limit)
	if err != nil {
		return 0, true, fmt.Errorf("store: delete health reports before %s: %w", cutoff.Format(time.RFC3339), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, true, fmt.Errorf("store: commit health delete batch: %w", err)
	}
	return n, true, nil
}

// ---------------------------------------------------------------------------
// Mutes
// ---------------------------------------------------------------------------

// FleetMute silences one (person, kind) pair until a moment. The note says
// why, because a mute with no reason is a mute nobody dares lift.
type FleetMute struct {
	Email     string    `json:"email"`
	Kind      string    `json:"kind"`
	Until     time.Time `json:"until"`
	Note      string    `json:"note,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
}

// Active reports whether the mute still holds at now.
func (m FleetMute) Active(now time.Time) bool { return m.Until.After(now) }

// FleetMutes returns every mute, expired ones included, so the page can show
// what lapsed and the evaluator can decide by the clock it is handed.
func (s *Store) FleetMutes(ctx context.Context) ([]FleetMute, error) {
	rows, err := s.db.Query(ctx, `
		SELECT email, kind, until, coalesce(note, ''), coalesce(created_by, '')
		FROM fleet_mutes
		ORDER BY email, kind`)
	if err != nil {
		return nil, fmt.Errorf("store: fleet mutes: %w", err)
	}
	defer rows.Close()
	var out []FleetMute
	for rows.Next() {
		var m FleetMute
		if err := rows.Scan(&m.Email, &m.Kind, &m.Until, &m.Note, &m.CreatedBy); err != nil {
			return nil, fmt.Errorf("store: scan fleet mute: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read fleet mutes: %w", err)
	}
	return out, nil
}

// PutFleetMute creates or replaces a mute. Admin only: a mute silences an
// alert for the whole fleet, which is an operator's decision.
func (s *Store) PutFleetMute(ctx context.Context, v Viewer, m FleetMute) error {
	if !v.IsAdmin() {
		return ErrNotAdmin
	}
	if m.Email == "" || m.Kind == "" || m.Until.IsZero() {
		return fmt.Errorf("store: a fleet mute needs an email, a kind and an until")
	}
	if _, err := s.db.Exec(ctx, `
		INSERT INTO fleet_mutes (email, kind, until, note, created_by)
		VALUES ($1, $2, $3, nullif($4, ''), nullif($5, ''))
		ON CONFLICT (email, kind) DO UPDATE SET
			until = EXCLUDED.until, note = EXCLUDED.note, created_by = EXCLUDED.created_by`,
		m.Email, m.Kind, m.Until, m.Note, v.Email); err != nil {
		return fmt.Errorf("store: put fleet mute: %w", err)
	}
	return nil
}

// DeleteFleetMute lifts a mute. Admin only, for the reason PutFleetMute is.
func (s *Store) DeleteFleetMute(ctx context.Context, v Viewer, email, kind string) error {
	if !v.IsAdmin() {
		return ErrNotAdmin
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM fleet_mutes WHERE email = $1 AND kind = $2`, email, kind); err != nil {
		return fmt.Errorf("store: delete fleet mute: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Fleet evaluator reads
// ---------------------------------------------------------------------------

// fleetLockKey is the fleet evaluator's single-flight lock. Transaction
// scoped and held for the length of one tick's transaction, so two instances
// evaluate one fleet once; distinct from the retention and derive keys so a
// sweep never waits on an evaluation or the reverse.
const fleetLockKey int64 = 7266794526548563

// FleetLock takes the evaluator's advisory lock inside a transaction it
// opens and hands back, so the caller holds the lock exactly as long as the
// transaction and releases it by ending the transaction. held is false when
// another instance holds it, in which case no transaction is returned.
func (s *Store) FleetLock(ctx context.Context) (tx Tx, held bool, err error) {
	tx, err = s.db.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("store: begin fleet lock: %w", err)
	}
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, fleetLockKey).Scan(&held); err != nil {
		_ = tx.Rollback(ctx)
		return nil, false, fmt.Errorf("store: take the fleet lock: %w", err)
	}
	if !held {
		_ = tx.Rollback(ctx)
		return nil, false, nil
	}
	return tx, true, nil
}

// FleetTickDue records the evaluator's tick and reports whether it should
// run. Cloud Run runs several instances whose timers fire at their own
// moments, and the advisory lock only stops ticks that overlap, so each
// instance logged its own set of fleet lines per interval and the
// line-count metrics read as many fleets as instances. One row, written
// under the lock: a tick that finds the newest tick younger than minAge is
// that instance's interval and does nothing. The statement is atomic on its
// own (the row lock serialises the compare and the write), so the advisory
// lock around it is belt and braces.
//
// Both sides of the comparison are the database clock, and the caller's own
// clock reaches none of it. The gate decides for the whole fleet from one
// shared row, so it can only be as right as the clock it is measured by: an
// instance an hour ahead used to stamp the row an hour into the future, and
// every instance keeping correct time was then refused for that hour, which
// handed the fleet's cadence to whichever instance was most wrong. One
// statement, one now(): the value written and the age it is compared with
// are the same reading, so the gate is the same for every instance whatever
// its own clock says. The caller still keeps a clock for the evaluation's
// timestamps; it has no say in when the evaluation is due.
func (s *Store) FleetTickDue(ctx context.Context, minAge time.Duration) (bool, error) {
	n, err := s.db.Exec(ctx, `
		INSERT INTO fleet_ticks (name, last_tick_at) VALUES ('fleet', now())
		ON CONFLICT (name) DO UPDATE SET last_tick_at = now()
		WHERE fleet_ticks.last_tick_at <= now() - make_interval(secs => $1)`,
		minAge.Seconds())
	if err != nil {
		return false, fmt.Errorf("store: fleet tick: %w", err)
	}
	return n > 0, nil
}

// FleetEmptyStart is one device's empty-start count over a window: how many
// of its sessions were lifecycle-only spawns that died (empty/aborted),
// against every session it started, with the directories they started in.
type FleetEmptyStart struct {
	Email    string
	DeviceID string
	Aborted  int
	Total    int
	Cwds     []string
}

// FleetEmptyStarts counts empty/aborted sessions per device since the given
// moment. Automation rows never carry empty_kind, so they cannot count as
// aborted, and a session's device is the one that delivered it.
func (s *Store) FleetEmptyStarts(ctx context.Context, since time.Time) ([]FleetEmptyStart, error) {
	rows, err := s.db.Query(ctx, `
		SELECT email, coalesce(device_id::text, ''),
		       count(*) FILTER (WHERE session_type = 'empty' AND empty_kind = 'aborted')::int,
		       count(*)::int,
		       coalesce((array_agg(DISTINCT cwd ORDER BY cwd)
		                 FILTER (WHERE session_type = 'empty' AND empty_kind = 'aborted'
		                           AND cwd IS NOT NULL AND cwd <> ''))[1:5], '{}'::text[])
		FROM sessions
		WHERE started_at >= $1
		GROUP BY email, device_id
		ORDER BY email, device_id`, since)
	if err != nil {
		return nil, fmt.Errorf("store: fleet empty starts: %w", err)
	}
	defer rows.Close()
	var out []FleetEmptyStart
	for rows.Next() {
		var e FleetEmptyStart
		if err := rows.Scan(&e.Email, &e.DeviceID, &e.Aborted, &e.Total, &e.Cwds); err != nil {
			return nil, fmt.Errorf("store: scan fleet empty start: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read fleet empty starts: %w", err)
	}
	return out, nil
}

// FleetAnswerCohort is one day's hook-captured human turns for one client
// build and entrypoint: how many there were and how many closed on an answer.
// The cohort key is the build that delivered the session's events, which is
// what says whether the Stop fix reached that machine.
type FleetAnswerCohort struct {
	Day          time.Time
	AgentVersion string
	Entrypoint   string
	Turns        int
	Answered     int
}

// FleetMissingAnswers reads the answer rate of hook-captured turns per day,
// build and entrypoint since the given day. Only a person's sessions and
// only turns the hook path captured count: a transcript-only turn has no
// Stop to lose an answer on, and a session that was walked later has its
// answers from the walk whatever the hook did.
//
// Three outcomes are left out, each because the turn had no answer to lose.
// in_progress has not finished. interrupted was stopped by the person, and
// RepairList leaves it out for the same reason (reads_turns.go). no_work is
// left out CONDITIONALLY, and the condition is the point: a no_work turn is
// one whose only captured event is its prompt (the fold adds no work group
// for the prompt itself), which is both the prompt the harness folded into
// the next turn -- a message sent while the agent was busy, or a
// programmatic subagent or cross-session message arriving as one -- and the
// shape a client produces when it captures UserPromptSubmit and then
// nothing at all. The two are told apart by the session: if any turn in it
// was answered, the client's stop path works and this prompt really was
// folded forward, so the turn leaves the cohort; if nothing in the session
// was ever answered, the turn stays and counts as missing, because a build
// whose hooks stop firing after the prompt would otherwise drop out of the
// metric entirely and a threshold does not fire on absent data.
//
// What that is worth, measured over eight days on a real fleet: counting
// every no_work turn put the healthy fleet build between 1.7% and 12.2% a
// day, above the 5% the alert watches on five of those days. Under the rule
// above the same days read 1.5% to 3.8%, and the build that really was
// losing answers (a few dozen turns, none answered) reads 100% either way. The window is on the
// session's start (sessions_started_at_idx) and the turns come through
// their primary key, because turns has no index on started_at and a scan
// of it every five minutes and every fleet page load is the one cost this
// read must not have; a session's turns start after the session, so the
// window bounds them, and a session older than the window that is still
// in use is left out, which the two-day cohorts can afford.
func (s *Store) FleetMissingAnswers(ctx context.Context, since time.Time) ([]FleetAnswerCohort, error) {
	rows, err := s.db.Query(ctx, `
		SELECT day, build, entrypoint,
		       count(*)::int,
		       count(*) FILTER (WHERE answered)::int
		FROM (
			SELECT date_trunc('day', t.started_at) AS day,
			       coalesce(s.agent_versions[array_upper(s.agent_versions, 1)], 'unknown') AS build,
			       coalesce(nullif(s.entrypoint, ''), 'unknown') AS entrypoint,
			       t.outcome AS outcome,
			       t.final_event_id IS NOT NULL AS answered,
			       count(*) FILTER (WHERE t.final_event_id IS NOT NULL)
			         OVER (PARTITION BY s.session_id) AS session_answered
			FROM sessions s
			JOIN turns t ON t.session_id = s.session_id AND t.thread = ''
			WHERE s.started_at >= $1
			  AND t.kind IN ('human', 'slash_command')
			  AND 'hook' = ANY(t.origins)
			  AND s.session_type = 'user'
		) q
		WHERE outcome NOT IN ('in_progress', 'interrupted')
		  AND NOT (outcome = 'no_work' AND session_answered > 0)
		GROUP BY 1, 2, 3
		ORDER BY 1, 2, 3`, since)
	if err != nil {
		return nil, fmt.Errorf("store: fleet missing answers: %w", err)
	}
	defer rows.Close()
	var out []FleetAnswerCohort
	for rows.Next() {
		var c FleetAnswerCohort
		if err := rows.Scan(&c.Day, &c.AgentVersion, &c.Entrypoint, &c.Turns, &c.Answered); err != nil {
			return nil, fmt.Errorf("store: scan fleet answer cohort: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read fleet missing answers: %w", err)
	}
	return out, nil
}
