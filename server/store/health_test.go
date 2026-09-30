package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestHealthReportUpsertsTheLatestRowInTheSameTransaction: the newest report
// per machine is written beside the raw row, in the raw row's transaction,
// and only ever forward in report time. A queued sample delivered late must
// not replace what the machine said afterwards.
func TestHealthReportUpsertsTheLatestRowInTheSameTransaction(t *testing.T) {
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	sample := healthSample()
	sample.AgentVersion = "23713ea"
	if err := s.PutHealthReport(context.Background(), "me@example.com",
		"11111111-1111-4111-8111-111111111111", sample); err != nil {
		t.Fatalf("PutHealthReport: %v", err)
	}
	c := db.find(t, "INSERT INTO health_latest")
	for _, want := range []string{
		"ON CONFLICT (email, device_id) DO UPDATE",
		"WHERE EXCLUDED.emitted_at > health_latest.emitted_at",
		"agent_version = EXCLUDED.agent_version",
		"report        = EXCLUDED.report",
	} {
		if !strings.Contains(c.sql, want) {
			t.Errorf("the latest-row upsert lacks %q:\n%s", want, c.sql)
		}
	}
	if got := c.args[4]; got != "23713ea" {
		t.Errorf("agent_version = %v, want the report's build", got)
	}
	if db.committed != 1 {
		t.Errorf("committed %d transactions, want the one holding both rows", db.committed)
	}
	raw := db.find(t, "INSERT INTO health_reports")
	if raw.args[3] != c.args[3] {
		t.Errorf("worst differs between the raw row (%v) and the latest row (%v)", raw.args[3], c.args[3])
	}
}

// TestFleetCoverageReadsTheLatestTableNotTheHistory: the raw rows go after
// seven days, so coverage has to come from the table that outlives them.
func TestFleetCoverageReadsTheLatestTableNotTheHistory(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "FROM principals p", rows: [][]any{
		{"reporting@example.com", "member", 1, at, "info"},
	}}}}
	s := NewWithDB(db, nil)
	if _, err := s.FleetCoverage(context.Background(), Viewer{Email: "a@example.com", Role: RoleAdmin}, time.Hour); err != nil {
		t.Fatalf("FleetCoverage: %v", err)
	}
	c := db.find(t, "FROM principals p")
	if strings.Contains(c.sql, "health_reports") {
		t.Errorf("coverage still scans the raw history:\n%s", c.sql)
	}
	if !strings.Contains(c.sql, "LEFT JOIN health_latest h") || !strings.Contains(c.sql, "FROM health_latest h2") {
		t.Errorf("coverage does not read health_latest:\n%s", c.sql)
	}
}

// TestHealthSweepRollsUpBeforeItDeletesAndBoundsEveryStatement: the raw rows
// a batch deletes were rolled up first, every write takes the retention lock,
// every statement carries a timeout, and the delete is bounded by the batch.
func TestHealthSweepRollsUpBeforeItDeletesAndBoundsEveryStatement(t *testing.T) {
	oldest := retNow.Add(-2 * 24 * time.Hour)
	db := &fakeDB{stubs: []*stub{
		{match: "min(emitted_at)", rows: [][]any{{&oldest}}},
		{match: "pg_try_advisory_xact_lock", rows: [][]any{{true}}},
		{match: "DELETE FROM health_reports", affected: 5000, once: true},
		{match: "DELETE FROM health_reports", affected: 12},
	}}
	s := NewWithDB(db, nil)
	sweep, err := s.SweepHealth(context.Background(), retNow)
	if err != nil {
		t.Fatalf("SweepHealth: %v", err)
	}
	if sweep.RawDeleted != 5012 || sweep.Deferred || sweep.Incomplete {
		t.Errorf("sweep = %+v, want 5012 raw rows deleted in two full batches", sweep)
	}

	var rollups, deletes, locks, timeouts int
	firstDelete, lastRollup := -1, -1
	for i, c := range db.calls {
		switch {
		case strings.Contains(c.sql, "INSERT INTO health_hourly"):
			rollups++
			lastRollup = i
			if got := c.args[2]; got != 300 {
				t.Errorf("rollup cadence = %v, want 300 s", got)
			}
		case strings.Contains(c.sql, "DELETE FROM health_reports"):
			deletes++
			if firstDelete < 0 {
				firstDelete = i
			}
			if got := c.args[1]; got != healthSweepBatch {
				t.Errorf("delete batch = %v, want %d", got, healthSweepBatch)
			}
			if got, ok := c.args[0].(time.Time); !ok || !got.Equal(retNow.Add(-healthRawRetention)) {
				t.Errorf("delete cutoff = %v, want seven days before the clock", c.args[0])
			}
		case strings.Contains(c.sql, "pg_try_advisory_xact_lock"):
			locks++
		case strings.Contains(c.sql, "statement_timeout"):
			timeouts++
		}
	}
	// Two days of history from the oldest row to the clock, plus the day
	// the clock is in: three day windows, then two delete batches.
	if rollups != 3 || deletes != 2 {
		t.Errorf("issued %d rollups and %d deletes, want 3 and 2", rollups, deletes)
	}
	if lastRollup > firstDelete {
		t.Error("a delete ran before the rollup finished; a raw row could be lost before its hour was written")
	}
	if locks != rollups+deletes {
		t.Errorf("%d lock takes for %d writes; every batch must take the retention lock", locks, rollups+deletes)
	}
	if timeouts != rollups+deletes {
		t.Errorf("%d statement timeouts for %d batches", timeouts, rollups+deletes)
	}
}

// TestHealthSweepStandsDownWhenAnotherHoldsTheLock: a second instance that
// finds the lock held ends its pass without writing.
func TestHealthSweepStandsDownWhenAnotherHoldsTheLock(t *testing.T) {
	oldest := retNow.Add(-time.Hour)
	db := &fakeDB{stubs: []*stub{
		{match: "min(emitted_at)", rows: [][]any{{&oldest}}},
		{match: "pg_try_advisory_xact_lock", rows: [][]any{{false}}},
	}}
	sweep, err := NewWithDB(db, nil).SweepHealth(context.Background(), retNow)
	if err != nil {
		t.Fatalf("SweepHealth: %v", err)
	}
	if !sweep.Deferred || sweep.HoursRolled != 0 || sweep.RawDeleted != 0 {
		t.Errorf("sweep = %+v, want deferred with nothing written", sweep)
	}
	for _, c := range db.calls {
		if strings.Contains(c.sql, "INSERT INTO health_hourly") || strings.Contains(c.sql, "DELETE FROM health_reports") {
			t.Errorf("wrote without the lock: %s", c.sql)
		}
	}
}

// TestFleetMutesAreAnAdminsDecision: a member cannot silence an alert for
// the fleet, and a mute needs the three fields that make it one.
func TestFleetMutesAreAnAdminsDecision(t *testing.T) {
	db := &fakeDB{}
	s := NewWithDB(db, nil)
	member := Viewer{Email: "m@example.com", Role: RoleMember}
	admin := Viewer{Email: "a@example.com", Role: RoleAdmin}
	mute := FleetMute{Email: "casey@example.com", Kind: "empty_start", Until: at.Add(7 * 24 * time.Hour), Note: "asked on Slack"}
	if err := s.PutFleetMute(context.Background(), member, mute); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("a member set a mute: %v", err)
	}
	if err := s.DeleteFleetMute(context.Background(), member, mute.Email, mute.Kind); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("a member lifted a mute: %v", err)
	}
	if err := s.PutFleetMute(context.Background(), admin, FleetMute{Email: "x", Kind: "y"}); err == nil {
		t.Error("a mute with no until was accepted")
	}
	if err := s.PutFleetMute(context.Background(), admin, mute); err != nil {
		t.Fatalf("PutFleetMute: %v", err)
	}
	c := db.find(t, "INSERT INTO fleet_mutes")
	if !strings.Contains(c.sql, "ON CONFLICT (email, kind) DO UPDATE") {
		t.Errorf("a second mute for the same pair would fail rather than replace:\n%s", c.sql)
	}
	if got := c.args[4]; got != admin.Email {
		t.Errorf("created_by = %v, want the admin who set it", got)
	}
	if !mute.Active(at) || mute.Active(at.Add(8*24*time.Hour)) {
		t.Error("Active does not follow the until")
	}
}
