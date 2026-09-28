//go:build integration

package store

// The half of the health rollups only Postgres can answer: that the upsert
// keeps the newest report, that the hourly ledger sums to the raw rows and
// charges each hour with the rise in the client's counters, that the sweep
// deletes exactly the raw rows past the window and nothing newer, and that
// the mute table round-trips.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/health"
)

// healthClock is the "now" the rollup tests measure against, on the hour so
// the bucket arithmetic is exact.
var healthClock = time.Date(2026, 9, 11, 14, 0, 0, 0, time.UTC)

// TestIntegrationHealthRollupR8 is research/r6 R8 #8: 300 reports for one
// device leave one latest row and one hourly row per hour whose reports sum
// to 300; the sweep deletes the raw rows older than seven days and nothing
// newer, and the ledger keeps what they said.
func TestIntegrationHealthRollupR8(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "rollup@example.com", RoleMember)
	dev, err := s.EnrollDevice(ctx, Device{Email: "rollup@example.com"}, []byte("h-rollup"), time.Time{})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE health_reports, health_latest, health_hourly`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// 300 reports, one every five minutes, ending an hour before the clock:
	// twenty-five hours, twelve reports each. The drop counter rises by one
	// every tenth report under one reason and resets once (the daemon
	// restarted), a second reason appears late, and the last two hours carry
	// capture_blocked on every report.
	const reports = 300
	first := healthClock.Add(-time.Hour - time.Duration(reports-1)*5*time.Minute)
	var wantDrops int64
	disk, overflow := 0, 0
	for i := 0; i < reports; i++ {
		at := first.Add(time.Duration(i) * 5 * time.Minute)
		if i%10 == 9 {
			disk++
			wantDrops++
		}
		if i == 150 {
			// A restart: the counter starts over, and everything it shows
			// afterwards is new.
			disk = 0
		}
		if i >= 280 {
			overflow++
			wantDrops++
		}
		r := health.Report{SchemaVersion: 2, Hostname: "rollup-mbp", AgentVersion: "788dcb3", EmittedAt: at}
		r.Spool.Dropped = map[string]int{"disk_full": disk}
		if overflow > 0 {
			r.Spool.Dropped["queue_overflow"] = overflow
		}
		r.Spool.Quarantine = i % 7
		if i >= reports-24 {
			r.Conditions = []health.Condition{{Level: health.LevelCritical, Kind: health.KindCaptureBlocked, Detail: "disk"}}
		}
		if err := s.PutHealthReport(ctx, "rollup@example.com", dev.ID, r); err != nil {
			t.Fatalf("PutHealthReport %d: %v", i, err)
		}
	}
	// The reset itself: report 150 shows 0 after 149 showed 15, and the
	// reports 150..159 climb back to 1 at 159. The rise from 0 to 1 counts;
	// the reset does not count the 15 as lost twice or as negative.
	// wantDrops already counts every rise once.

	latest, err := s.HealthLatestRows(ctx)
	if err != nil {
		t.Fatalf("HealthLatestRows: %v", err)
	}
	if len(latest) != 1 || latest[0].DeviceID != dev.ID || !latest[0].EmittedAt.Equal(first.Add(299*5*time.Minute)) {
		t.Fatalf("latest = %+v, want one row at the newest emitted_at", latest)
	}
	if latest[0].AgentVersion != "788dcb3" || latest[0].Worst != "critical" {
		t.Errorf("latest row carries version %q worst %q", latest[0].AgentVersion, latest[0].Worst)
	}

	// A replayed older sample does not move the latest row backwards.
	old := health.Report{SchemaVersion: 2, Hostname: "rollup-mbp", AgentVersion: "23713ea", EmittedAt: first.Add(-time.Hour)}
	if err := s.PutHealthReport(ctx, "rollup@example.com", dev.ID, old); err != nil {
		t.Fatalf("replay: %v", err)
	}
	latest, _ = s.HealthLatestRows(ctx)
	if latest[0].AgentVersion != "788dcb3" {
		t.Errorf("a replayed old sample rolled the latest row back to %q", latest[0].AgentVersion)
	}

	rolled, locked, err := s.RollupHealth(ctx, first.Add(-2*time.Hour), healthClock)
	if err != nil || !locked {
		t.Fatalf("RollupHealth: locked=%v err=%v", locked, err)
	}
	hours, err := s.HealthHours(ctx, first.Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("HealthHours: %v", err)
	}
	// The 300 reports run from 12:05 to 13:00 the next day, which touches
	// twenty-six hour buckets, plus the hour the replayed sample fell in.
	if len(hours) != 27 || rolled != 27 {
		t.Fatalf("rolled %d rows into %d hours, want 27", rolled, len(hours))
	}
	var sumReports int
	var sumDrops int64
	var blockedSecs int
	byReason := map[string]int64{}
	for _, h := range hours {
		sumReports += h.Reports
		sumDrops += h.Drops
		blockedSecs += h.CaptureBlockedSecs
		for reason, n := range h.DroppedByReason() {
			byReason[reason] += n
		}
		if h.Quarantined > 6 {
			t.Errorf("hour %s quarantined %d, want the high-water mark of i%%7", h.Hour, h.Quarantined)
		}
	}
	if sumReports != reports+1 {
		t.Errorf("hourly reports sum to %d, want %d", sumReports, reports+1)
	}
	if sumDrops != wantDrops {
		t.Errorf("hourly drops sum to %d, want %d (each rise counted once, the reset not counted)", sumDrops, wantDrops)
	}
	if byReason["queue_overflow"] != 20 || byReason["disk_full"] != wantDrops-20 {
		t.Errorf("drops by reason = %v", byReason)
	}
	// Twenty-four blocked reports at five minutes each, two full hours.
	if blockedSecs != 24*300 {
		t.Errorf("capture_blocked_secs sum to %d, want %d", blockedSecs, 24*300)
	}
	// The 12:00 bucket is the last full hour: twelve reports, every one of
	// them blocked. The 13:00 bucket holds only the report on the hour.
	full := hours[len(hours)-2]
	if full.Reports != 12 || full.Worst != "critical" {
		t.Errorf("last full hour = %d reports worst %q, want 12 critical", full.Reports, full.Worst)
	}
	var conds struct {
		Kinds map[string]int `json:"kinds"`
	}
	if err := json.Unmarshal(full.Conditions, &conds); err != nil || conds.Kinds["capture_blocked/critical"] != 12 {
		t.Errorf("last full hour conditions = %s (%v), want twelve capture_blocked/critical", full.Conditions, err)
	}

	// Rolling up again changes nothing: the ledger is a function of the raw
	// rows and the upsert is idempotent.
	again, err := s.HealthHours(ctx, first.Add(-2*time.Hour))
	if _, _, err2 := s.RollupHealth(ctx, first.Add(-2*time.Hour), healthClock); err != nil || err2 != nil {
		t.Fatalf("second rollup: %v %v", err, err2)
	}
	if len(again) != len(hours) {
		t.Errorf("a second rollup changed the row count")
	}

	// Age the first 120 raw rows past the window by their arrival stamp and
	// sweep: exactly those go, the newer ones stay, and the ledger still
	// carries their hours.
	if _, err := pool.Exec(ctx, `
		UPDATE health_reports SET received_at = $1
		WHERE id IN (SELECT id FROM health_reports ORDER BY emitted_at LIMIT 121)`,
		healthClock.Add(-8*24*time.Hour)); err != nil {
		t.Fatalf("age rows: %v", err)
	}
	sweep, err := s.SweepHealth(ctx, healthClock)
	if err != nil {
		t.Fatalf("SweepHealth: %v", err)
	}
	if sweep.RawDeleted != 121 || sweep.Deferred || sweep.Incomplete {
		t.Errorf("sweep = %+v, want 121 raw rows deleted", sweep)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM health_reports`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != reports+1-121 {
		t.Errorf("%d raw rows remain, want %d", remaining, reports+1-121)
	}
	hoursAfter, _ := s.HealthHours(ctx, first.Add(-2*time.Hour))
	if len(hoursAfter) != 27 {
		t.Errorf("the sweep left %d hourly rows, want the 27 the raw rows described", len(hoursAfter))
	}
	latestAfter, _ := s.HealthLatestRows(ctx)
	if len(latestAfter) != 1 {
		t.Errorf("the sweep removed the latest row")
	}

	// Coverage reads the latest table, so the machine is still reporting.
	fleet, err := s.FleetCoverage(ctx, Viewer{Email: "alex@example.com", Role: RoleAdmin}, 365*24*time.Hour)
	if err != nil {
		t.Fatalf("FleetCoverage: %v", err)
	}
	for _, m := range fleet.Members {
		if m.Email == "rollup@example.com" && (m.Silent || m.Worst != "critical") {
			t.Errorf("coverage row = %+v, want reporting and critical from health_latest", m)
		}
	}
}

// TestIntegrationFleetMutesRoundTrip: a mute is stored once per (person,
// kind), replaced on a second set, and lifted on delete.
func TestIntegrationFleetMutesRoundTrip(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE fleet_mutes`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	admin := Viewer{Email: "alex@example.com", Role: RoleAdmin}
	until := healthClock.Add(7 * 24 * time.Hour)
	if err := s.PutFleetMute(ctx, admin, FleetMute{Email: "casey@example.com", Kind: "empty_start", Until: until, Note: "asked on Slack"}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.PutFleetMute(ctx, admin, FleetMute{Email: "casey@example.com", Kind: "empty_start", Until: until.Add(24 * time.Hour), Note: "extended"}); err != nil {
		t.Fatalf("put again: %v", err)
	}
	mutes, err := s.FleetMutes(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(mutes) != 1 || !mutes[0].Until.Equal(until.Add(24*time.Hour)) || mutes[0].Note != "extended" || mutes[0].CreatedBy != admin.Email {
		t.Fatalf("mutes = %+v", mutes)
	}
	if err := s.DeleteFleetMute(ctx, admin, "casey@example.com", "empty_start"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if mutes, _ = s.FleetMutes(ctx); len(mutes) != 0 {
		t.Errorf("mute not lifted: %+v", mutes)
	}
}

// TestIntegrationHealthRollupBaselineIsWindowIndependent is review-1 F2: the
// rise a machine reports after a gap longer than the rollup window is charged
// to the hour it arrived in whatever window the rollup ran over, so the
// tick's two-hour window and the sweep's day window write the same row and
// the ledger never flaps between them. A machine's first report ever is
// still a baseline and not a loss. Also review-1 minor 8: an hour built from
// version 1 reports leaves parked and empty starts unknown, never zero.
func TestIntegrationHealthRollupBaselineIsWindowIndependent(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "gap@example.com", RoleMember)
	dev, err := s.EnrollDevice(ctx, Device{Email: "gap@example.com"}, []byte("h-gap"), time.Time{})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	dev2, err := s.EnrollDevice(ctx, Device{Email: "gap@example.com", Hostname: "second"}, []byte("h-gap-2"), time.Time{})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE health_reports, health_latest, health_hourly`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	// The daemon died at 09:00 with two drops on the counter and the hooks
	// kept dropping; it came back at 13:50 reporting nine.
	old := health.Report{SchemaVersion: 1, AgentVersion: "23713ea", EmittedAt: healthClock.Add(-5 * time.Hour)}
	old.Spool.Dropped = map[string]int{"disk_full": 2}
	back := health.Report{SchemaVersion: 1, AgentVersion: "23713ea", EmittedAt: healthClock.Add(-10 * time.Minute)}
	back.Spool.Dropped = map[string]int{"disk_full": 9}
	for _, r := range []health.Report{old, back} {
		if err := s.PutHealthReport(ctx, "gap@example.com", dev.ID, r); err != nil {
			t.Fatalf("PutHealthReport: %v", err)
		}
	}
	// A second machine whose first report ever falls inside the window.
	first := health.Report{SchemaVersion: 1, AgentVersion: "23713ea", EmittedAt: healthClock.Add(-10 * time.Minute)}
	first.Spool.Dropped = map[string]int{"disk_full": 4}
	if err := s.PutHealthReport(ctx, "gap@example.com", dev2.ID, first); err != nil {
		t.Fatalf("PutHealthReport: %v", err)
	}

	tickFrom, tickTo := healthClock.Add(-2*time.Hour).Truncate(time.Hour), healthClock.Add(time.Hour)
	hourOf := func(hours []HealthHour, device string, hour time.Time) *HealthHour {
		for i := range hours {
			if hours[i].DeviceID == device && hours[i].Hour.Equal(hour) {
				return &hours[i]
			}
		}
		return nil
	}
	current := healthClock.Add(-10 * time.Minute).Truncate(time.Hour)
	check := func(window string, from, to time.Time) {
		t.Helper()
		if _, locked, err := s.RollupHealth(ctx, from, to); err != nil || !locked {
			t.Fatalf("%s rollup: locked=%v err=%v", window, locked, err)
		}
		hours, err := s.HealthHours(ctx, healthClock.Add(-6*time.Hour))
		if err != nil {
			t.Fatalf("HealthHours: %v", err)
		}
		row := hourOf(hours, dev.ID, current)
		if row == nil {
			t.Fatalf("%s rollup wrote no row for the hour the machine came back in", window)
		}
		if row.Drops != 7 || row.DroppedByReason()["disk_full"] != 7 {
			t.Errorf("%s rollup charged the hour %d drops (%v), want the rise of 7 since the report five hours before", window, row.Drops, row.DroppedByReason())
		}
		if row.Parked != nil || row.EmptyStarts != nil {
			t.Errorf("%s rollup read a version 1 hour as parked %v empty starts %v, want unknown", window, row.Parked, row.EmptyStarts)
		}
		if fresh := hourOf(hours, dev2.ID, current); fresh == nil || fresh.Drops != 0 {
			t.Errorf("%s rollup charged a machine's first report ever with drops: %+v", window, fresh)
		}
	}
	// The tick's window, then the sweep's day window, then a whole sweep:
	// every one of them writes the same answer.
	check("tick", tickFrom, tickTo)
	day := healthClock.Truncate(24 * time.Hour)
	check("day", day, day.Add(24*time.Hour))
	if _, err := s.SweepHealth(ctx, healthClock); err != nil {
		t.Fatalf("SweepHealth: %v", err)
	}
	hours, err := s.HealthHours(ctx, healthClock.Add(-6*time.Hour))
	if err != nil {
		t.Fatalf("HealthHours: %v", err)
	}
	if row := hourOf(hours, dev.ID, current); row == nil || row.Drops != 7 {
		t.Errorf("the sweep rewrote the hour to %+v", row)
	}
	// The hour of the machine's first report ever is a baseline.
	if row := hourOf(hours, dev.ID, healthClock.Add(-5*time.Hour).Truncate(time.Hour)); row == nil || row.Drops != 0 {
		t.Errorf("the first report ever was charged as a loss: %+v", row)
	}

	// A version 2 report in the same hour carries parked and empty starts,
	// and the hour reads them; the version 1 machine's hour stays unknown.
	if _, err := pool.Exec(ctx, `
		INSERT INTO health_reports (email, device_id, emitted_at, worst, report)
		VALUES ('gap@example.com', $1::uuid, $2, 'info',
		        '{"schema_version":2,"agent_version":"788dcb3","agent_commit":"788dcb3","spool":{"parked":3,"dropped":{"disk_full":9}},"empty_starts_24h":7}'::jsonb)`,
		dev.ID, healthClock.Add(-5*time.Minute)); err != nil {
		t.Fatalf("insert v2 report: %v", err)
	}
	if _, _, err := s.RollupHealth(ctx, tickFrom, tickTo); err != nil {
		t.Fatalf("rollup: %v", err)
	}
	hours, _ = s.HealthHours(ctx, healthClock.Add(-6*time.Hour))
	row := hourOf(hours, dev.ID, current)
	if row == nil || row.Parked == nil || *row.Parked != 3 || row.EmptyStarts == nil || *row.EmptyStarts != 7 || row.Drops != 7 {
		t.Errorf("hour with a version 2 report = %+v, want parked 3, empty starts 7 and the same 7 drops", row)
	}
	if fresh := hourOf(hours, dev2.ID, current); fresh == nil || fresh.Parked != nil || fresh.EmptyStarts != nil {
		t.Errorf("the version 1 machine's hour = %+v, want parked and empty starts unknown", fresh)
	}
}

// TestIntegrationRecentBuildsSeeEveryDaemonAMachineRan is the lead's finding
// after review-1 at the store: two daemons on one laptop report by turns
// (the old one until its session ends, the new one for every new session),
// health_latest names whichever emitted last, and the window read names
// both builds with the last moment each was seen, and nothing that arrived
// before the window.
func TestIntegrationRecentBuildsSeeEveryDaemonAMachineRan(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "flap@example.com", RoleMember)
	dev, err := s.EnrollDevice(ctx, Device{Email: "flap@example.com"}, []byte("h-flap"), time.Time{})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE health_reports, health_latest, health_hourly`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	now := time.Now().UTC()
	// Six reports over the last half hour, alternating; the newest is the
	// old daemon's.
	for i := 0; i < 6; i++ {
		build := "788dcb3"
		if i%2 == 1 {
			build = "23713ea"
		}
		r := health.Report{SchemaVersion: 1, AgentVersion: build, EmittedAt: now.Add(-time.Duration(30-5*i) * time.Minute)}
		if err := s.PutHealthReport(ctx, "flap@example.com", dev.ID, r); err != nil {
			t.Fatalf("PutHealthReport %d: %v", i, err)
		}
	}
	// A report from three hours ago on a third build, delivered then.
	stale := health.Report{SchemaVersion: 1, AgentVersion: "5ed5cf3", EmittedAt: now.Add(-3 * time.Hour)}
	if err := s.PutHealthReport(ctx, "flap@example.com", dev.ID, stale); err != nil {
		t.Fatalf("PutHealthReport stale: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE health_reports SET received_at = $1 WHERE report->>'agent_version' = '5ed5cf3'`, now.Add(-3*time.Hour)); err != nil {
		t.Fatalf("age the stale report: %v", err)
	}

	latest, err := s.HealthLatestRows(ctx)
	if err != nil || len(latest) != 1 || latest[0].AgentVersion != "23713ea" {
		t.Fatalf("latest = %+v (%v), want the old daemon's report", latest, err)
	}
	seen, err := s.HealthRecentBuilds(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("HealthRecentBuilds: %v", err)
	}
	if len(seen) != 2 || seen[0].Build != "23713ea" || seen[1].Build != "788dcb3" || seen[0].DeviceID != dev.ID {
		t.Fatalf("recent builds = %+v, want the two daemons' builds, most recently seen first", seen)
	}
	if !seen[0].LastSeen.Equal(now.Add(-5*time.Minute).Truncate(time.Microsecond)) || !seen[1].LastSeen.Equal(now.Add(-10*time.Minute).Truncate(time.Microsecond)) {
		t.Errorf("last seen = %v / %v, want five and ten minutes ago", seen[0].LastSeen, seen[1].LastSeen)
	}
	for _, b := range seen {
		if b.Build == "5ed5cf3" {
			t.Error("a report that arrived before the window was counted")
		}
	}
}

// TestIntegrationFleetMissingAnswersCountOnlyTurnsThatCouldHaveAnswered:
// the answer cohort behind the fleet's missing-answer metric counts a turn
// that could have produced an answer and did not, and leaves out the three
// that could not. A turn still running has not finished; an interrupted one
// was stopped by the person; a no_work turn is a prompt the harness folded
// into the next one. The no_work rule is conditional, and the second
// session here is why: the same outcome is what a client produces when it
// captures the prompt and then nothing, so it is excused only in a session
// that answered something, which proves the client's stop path works.
func TestIntegrationFleetMissingAnswersCountOnlyTurnsThatCouldHaveAnswered(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "cohort@example.com", RoleMember)
	start := time.Now().UTC().Add(-2 * time.Hour)

	seed := func(sessionID, build string, turns []struct {
		outcome string
		final   string
	}) {
		t.Helper()
		batch := session(t, "cohort@example.com", sessionID)
		for i := range batch {
			batch[i].Event.ID = sessionID + "-" + batch[i].Event.ID
		}
		if _, err := s.UpsertEvents(ctx, batch); err != nil {
			t.Fatalf("ingest %s: %v", sessionID, err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE sessions SET started_at = $1, session_type = 'user', entrypoint = 'cli',
			                    agent_versions = ARRAY[$2::text]
			WHERE session_id = $3`, start, build, sessionID); err != nil {
			t.Fatalf("pin %s: %v", sessionID, err)
		}
		for i, turn := range turns {
			var final any
			if turn.final != "" {
				final = turn.final
			}
			if _, err := pool.Exec(ctx, `
				INSERT INTO turns (session_id, thread, turn_index, turn_key, kind, outcome,
				                   final_event_id, started_at, last_activity_at, origins)
				VALUES ($1, '', $2, $3, 'human', $4, $5, $6, $6, '{hook}')`,
				sessionID, i, fmt.Sprintf("pid:%d", i), turn.outcome, final,
				start.Add(time.Duration(i)*time.Minute)); err != nil {
				t.Fatalf("insert %s %s turn: %v", sessionID, turn.outcome, err)
			}
		}
	}

	// A working client: one turn of every outcome the fold can reach.
	seed("sess-working", "cdf9bae", []struct {
		outcome string
		final   string
	}{
		{"answered", "evt-turn"},
		{"no_answer_captured", ""},
		{"interrupted", ""},
		{"no_work", ""},
		{"in_progress", ""},
	})
	// A client that captured prompts and nothing else: every turn no_work,
	// nothing answered anywhere in the session.
	seed("sess-silent", "b0rk3d0", []struct {
		outcome string
		final   string
	}{
		{"no_work", ""},
		{"no_work", ""},
		{"no_work", ""},
	})

	cohorts, err := s.FleetMissingAnswers(ctx, start.Add(-time.Hour))
	if err != nil {
		t.Fatalf("FleetMissingAnswers: %v", err)
	}
	got := map[string]FleetAnswerCohort{}
	for _, c := range cohorts {
		got[c.AgentVersion] = c
	}

	// The working client: answered and no_answer_captured count, the other
	// three do not, so one of two turns is missing its answer.
	if c := got["cdf9bae"]; c.Turns != 2 || c.Answered != 1 {
		t.Errorf("working cohort = %d turns, %d answered; want 2 and 1 (in_progress, interrupted and an excused no_work left out): %+v", c.Turns, c.Answered, c)
	}
	// The silent client: nothing in the session was ever answered, so its
	// no_work turns are not excused and the build reads 100% missing. This
	// is the regression the conditional exists for: excusing them
	// unconditionally would drop the build out of the metric entirely, and
	// an alert threshold does not fire on a cohort that emits no rows.
	c, ok := got["b0rk3d0"]
	if !ok {
		t.Fatalf("the silent client's build has no cohort row at all; its turns were all excused: %+v", cohorts)
	}
	if c.Turns != 3 || c.Answered != 0 {
		t.Errorf("silent cohort = %d turns, %d answered; want 3 and 0: %+v", c.Turns, c.Answered, c)
	}
}

// TestIntegrationTheLedgerCountsLossesAndNotRecoveries: the hourly ledger's
// drops feed the fleet's drop metric and its alert. The spool's Dropped map
// also carries counters that are not losses (a recovery from a transcript
// is the one today), and summing all of them made a machine whose recovery
// pass worked look like one losing data: on 2026-09-23 the worst reporting
// machine had 8,643 recoveries against five real rejections.
func TestIntegrationTheLedgerCountsLossesAndNotRecoveries(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "drops@example.com", RoleMember)
	dev, err := s.EnrollDevice(ctx, Device{Email: "drops@example.com", Hostname: "drops-mbp"}, []byte("h-drops"), time.Time{})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE health_reports, health_latest, health_hourly`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	base := healthClock.Add(-2 * time.Hour)
	// Two reports an hour apart. The counters are cumulative, as the client's
	// are, so the hour's ledger is the rise between them: 40 recoveries and
	// 3 real discards.
	for i, dropped := range []map[string]int{
		{"recovered_from_transcript": 100, "spool_full": 1},
		{"recovered_from_transcript": 140, "spool_full": 4},
	} {
		r := health.Report{SchemaVersion: 2, Hostname: "drops-mbp", AgentVersion: "cdf9bae",
			EmittedAt: base.Add(time.Duration(i) * time.Hour)}
		r.Spool.Dropped = dropped
		if err := s.PutHealthReport(ctx, "drops@example.com", dev.ID, r); err != nil {
			t.Fatalf("PutHealthReport %d: %v", i, err)
		}
	}
	if _, _, err := s.RollupHealth(ctx, base.Add(-time.Hour), healthClock.Add(time.Hour)); err != nil {
		t.Fatalf("RollupHealth: %v", err)
	}

	var drops int64
	var reasons map[string]int64
	rows, err := s.HealthHours(ctx, base.Add(-time.Hour))
	if err != nil {
		t.Fatalf("HealthHours: %v", err)
	}
	for _, h := range rows {
		drops += h.Drops
		for k, v := range h.DroppedByReason() {
			if reasons == nil {
				reasons = map[string]int64{}
			}
			reasons[k] += v
		}
	}
	if drops != 3 {
		t.Errorf("the ledger charged %d drop(s), want the 3 real discards and none of the 40 recoveries", drops)
	}
	if reasons["recovered_from_transcript"] != 0 {
		t.Errorf("the ledger's reasons carry %d recoveries: %v", reasons["recovered_from_transcript"], reasons)
	}
	if reasons["spool_full"] != 3 {
		t.Errorf("the ledger's reasons carry %d spool_full, want 3: %v", reasons["spool_full"], reasons)
	}
}
