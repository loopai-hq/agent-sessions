//go:build integration

package app

// The fleet evaluator over a real Postgres: the store adapter reads every
// input, the lock makes one instance's tick the fleet's (a second evaluator
// under a held lock produces nothing), and a tick over real rows emits one
// set of lines with the contract's fields.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/server/fleet"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// newFleetSchema gives a test its own migrated schema on the scratch
// database, dropped when the test ends, so fleet tests never see each
// other's machines.
func newFleetSchema(t *testing.T) (*pgxpool.Pool, *store.Store) {
	t.Helper()
	dsn := os.Getenv("LOOP_SESSIONS_TEST_DSN")
	if dsn == "" {
		t.Skip("LOOP_SESSIONS_TEST_DSN is unset")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("loop_sessions_fleet_%d", time.Now().UnixNano())
	bootstrap, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	bootstrap.Close()
	t.Cleanup(func() {
		if cleanup, err := pgxpool.New(ctx, dsn); err == nil {
			_, _ = cleanup.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
			cleanup.Close()
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	st := store.New(pool, nil)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool, st
}

func TestIntegrationTwoFleetEvaluatorsProduceOneSetOfLines(t *testing.T) {
	ctx := context.Background()
	pool, st := newFleetSchema(t)

	// Two people: one reporting on the old build with a quarantine, one
	// silent for two days on the published build; a third device that
	// never reported.
	now := time.Now().UTC()
	for _, email := range []string{"quar@example.com", "quiet@example.com", "never@example.com"} {
		if _, err := pool.Exec(ctx, `INSERT INTO principals (email, role, added_by) VALUES ($1, 'member', 'test')`, email); err != nil {
			t.Fatal(err)
		}
	}
	quar, err := st.EnrollDevice(ctx, store.Device{Email: "quar@example.com", Hostname: "quar-mbp"}, []byte("h1"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	quiet, err := st.EnrollDevice(ctx, store.Device{Email: "quiet@example.com", Hostname: "quiet-mbp"}, []byte("h2"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnrollDevice(ctx, store.Device{Email: "never@example.com", Hostname: "never-mbp", AgentVersion: "dev"}, []byte("h3"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	// The quarantined machine's first report is the ledger's baseline (a
	// counter the window cannot place); the nine drops arrive on the second.
	r0 := health.Report{SchemaVersion: 1, Hostname: "quar-mbp", AgentVersion: "23713ea", EmittedAt: now.Add(-6 * time.Minute)}
	if err := st.PutHealthReport(ctx, "quar@example.com", quar.ID, r0); err != nil {
		t.Fatal(err)
	}
	r1 := health.Report{SchemaVersion: 1, Hostname: "quar-mbp", AgentVersion: "23713ea", EmittedAt: now.Add(-time.Minute),
		Conditions: []health.Condition{{Level: health.LevelCritical, Kind: health.KindQuarantineNonEmpty, Detail: "4 items", Since: now.Add(-72 * time.Hour)}}}
	r1.Spool.Quarantine = 4
	r1.Spool.Dropped = map[string]int{"disk_full": 9}
	if err := st.PutHealthReport(ctx, "quar@example.com", quar.ID, r1); err != nil {
		t.Fatal(err)
	}
	r2 := health.Report{SchemaVersion: 1, Hostname: "quiet-mbp", AgentVersion: "788dcb3", EmittedAt: now.Add(-50 * time.Hour)}
	if err := st.PutHealthReport(ctx, "quiet@example.com", quiet.ID, r2); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE health_latest SET received_at = $1 WHERE email = 'quiet@example.com'`, now.Add(-50*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Thirty lifecycle-only sessions on the quarantined device: the
	// empty-start rule.
	for i := 0; i < 30; i++ {
		if _, err := pool.Exec(ctx, `
			INSERT INTO sessions (session_id, email, device_id, source, started_at, ended, session_type, empty_kind, cwd)
			VALUES ($1, 'quar@example.com', $2::uuid, 'claude_code', $3, true, 'empty', 'aborted', '/home/quar')`,
			fmt.Sprintf("s-empty-%d", i), quar.ID, now.Add(-time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	// One answerless hook turn for the missing-answer cohort.
	p := store.Ingest{Email: "quar@example.com", DeviceID: quar.ID, AgentVersion: "23713ea", Event: event.Event{
		ID: "s-ma-p", SessionID: "s-ma", Seq: 1, Type: event.UserPrompt, Source: event.SourceClaudeCode, Origin: event.OriginHook,
		OccurredAt: now.Add(-3 * time.Hour), Text: "ask", PromptID: "pid",
	}}
	a := p
	a.Event.ID, a.Event.Seq, a.Event.Type, a.Event.Text = "s-ma-a", 2, event.AssistantTurn, ""
	a.Event.OccurredAt = now.Add(-3*time.Hour + 5*time.Second)
	end := p
	end.Event.ID, end.Event.Seq, end.Event.Type, end.Event.PromptID = "s-ma-end", 3, event.SessionEnded, ""
	end.Event.OccurredAt = now.Add(-3*time.Hour + time.Minute)
	if _, err := st.UpsertEvents(ctx, []store.Ingest{p, a, end}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DeriveDirty(ctx, store.DeriveConfig{Now: time.Now}); err != nil {
		t.Fatal(err)
	}

	newRunner := func(out *bytes.Buffer) *fleet.Runner {
		return &fleet.Runner{
			Store:       fleetStore{s: st},
			Manifest:    fakeManifestSource{m: fleet.Manifest{Version: "788dcb3", Commit: "788dcb3", BuildDate: now.Add(-48 * time.Hour)}},
			ServerBuild: "788dcb3",
			Log:         slog.New(slog.NewJSONHandler(out, nil)),
			Now:         func() time.Time { return now },
		}
	}

	// Instance A holds the lock for the length of its tick; instance B
	// ticking meanwhile does nothing. Modelled by holding the lock from the
	// test, which is what A's open transaction is.
	tx, held, err := st.FleetLock(ctx)
	if err != nil || !held {
		t.Fatalf("lock: held=%v err=%v", held, err)
	}
	var outB bytes.Buffer
	if _, did, err := newRunner(&outB).Tick(ctx); err != nil || did {
		t.Fatalf("a tick under another instance's lock did=%v err=%v", did, err)
	}
	if outB.Len() != 0 {
		t.Errorf("the deferred instance emitted lines:\n%s", outB.String())
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var outA bytes.Buffer
	ev, did, err := newRunner(&outA).Tick(ctx)
	if err != nil || !did {
		t.Fatalf("tick: did=%v err=%v", did, err)
	}
	lines := outA.String()
	if n := strings.Count(lines, `"msg":"fleet summary"`); n != 1 {
		t.Errorf("%d summary lines, want one", n)
	}
	for _, want := range []string{
		`"enrolled":3`, `"reporting":1`, `"silent":1`, `"never_reported":1`, `"quarantine_devices":1`, `"behind":1`, `"current":1`, `"unmanaged":1`,
		`"msg":"fleet condition"`, `"kind":"quarantine_nonempty"`,
		`"msg":"fleet silent"`, `"email":"quiet@example.com"`,
		`"msg":"fleet version_lag"`, `"agent_version":"23713ea"`, `"published_build":"788dcb3"`,
		`"msg":"fleet empty_start"`, `"empties":30`, `"cwds":["/home/quar"]`, `launcher.log`,
		`"msg":"fleet missing_answer"`, `"turns":1`, `"answered":0`, `"rate":1`,
		`"msg":"fleet drops"`, `"reason":"disk_full"`, `"delta":9`,
		`"msg":"health sweep"`,
	} {
		if !strings.Contains(lines, want) {
			t.Errorf("missing %s in\n%s", want, lines)
		}
	}
	if len(ev.CTAs) == 0 || ev.CTAs[0].Kind != "quarantine_nonempty" {
		t.Errorf("CTAs = %+v, want the quarantine row first", ev.CTAs)
	}
	// The ledger rows the tick rolled up are there for the capture-loss rule.
	var hours int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM health_hourly WHERE email = 'quar@example.com'`).Scan(&hours); err != nil || hours == 0 {
		t.Errorf("health_hourly rows = %d (%v)", hours, err)
	}
	// A second tick moves no counter, so no drops line repeats.
	var outC bytes.Buffer
	if _, _, err := newRunner(&outC).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(outC.String(), `"msg":"fleet drops"`) {
		t.Error("the second tick counted the same drops again")
	}
}

type fakeManifestSource struct{ m fleet.Manifest }

func (f fakeManifestSource) Latest(context.Context) (fleet.Manifest, error) { return f.m, nil }

// jsonLines returns the log lines carrying one message.
func jsonLines(out, msg string) []string {
	var got []string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, `"msg":"`+msg+`"`) {
			got = append(got, l)
		}
	}
	return got
}

// TestIntegrationTheTickReportsDropsAfterAGapAndCurrentBuildsThatAlternate
// covers review-1 F2 and the lead's finding over real rows and the real
// adapter. One laptop's daemon died five hours ago with two drops on its
// counter and came back ten minutes ago reporting nine: the tick charges
// the seven to the hour it came back in and says so once, although the gap
// is longer than the tick's rollup window. Another laptop is mid-upgrade,
// its two daemons reporting by turns with the old one's report the newest:
// it is current, not behind, and the row names the build it runs alongside.
func TestIntegrationTheTickReportsDropsAfterAGapAndCurrentBuildsThatAlternate(t *testing.T) {
	ctx := context.Background()
	pool, st := newFleetSchema(t)
	now := time.Now().UTC()
	for _, email := range []string{"gap@example.com", "flap@example.com"} {
		if _, err := pool.Exec(ctx, `INSERT INTO principals (email, role, added_by) VALUES ($1, 'member', 'test')`, email); err != nil {
			t.Fatal(err)
		}
	}
	gap, err := st.EnrollDevice(ctx, store.Device{Email: "gap@example.com", Hostname: "gap-mbp"}, []byte("h1"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	flap, err := st.EnrollDevice(ctx, store.Device{Email: "flap@example.com", Hostname: "flap-mbp"}, []byte("h2"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	dead := health.Report{SchemaVersion: 1, Hostname: "gap-mbp", AgentVersion: "23713ea", EmittedAt: now.Add(-5 * time.Hour)}
	dead.Spool.Dropped = map[string]int{"disk_full": 2}
	back := health.Report{SchemaVersion: 1, Hostname: "gap-mbp", AgentVersion: "23713ea", EmittedAt: now.Add(-10 * time.Minute)}
	back.Spool.Dropped = map[string]int{"disk_full": 9}
	for _, r := range []health.Report{dead, back} {
		if err := st.PutHealthReport(ctx, "gap@example.com", gap.ID, r); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 6; i++ {
		build := "788dcb3"
		if i%2 == 1 {
			build = "23713ea"
		}
		r := health.Report{SchemaVersion: 1, Hostname: "flap-mbp", AgentVersion: build, EmittedAt: now.Add(-time.Duration(30-5*i) * time.Minute)}
		if err := st.PutHealthReport(ctx, "flap@example.com", flap.ID, r); err != nil {
			t.Fatal(err)
		}
	}

	var out bytes.Buffer
	runner := &fleet.Runner{
		Store:       fleetStore{s: st},
		Manifest:    fakeManifestSource{m: fleet.Manifest{Version: "788dcb3", Commit: "788dcb3", BuildDate: now.Add(-48 * time.Hour)}},
		ServerBuild: "788dcb3",
		Log:         slog.New(slog.NewJSONHandler(&out, nil)),
		Now:         func() time.Time { return now },
	}
	ev, did, err := runner.Tick(ctx)
	if err != nil || !did {
		t.Fatalf("tick: did=%v err=%v", did, err)
	}
	lines := out.String()
	drops := jsonLines(lines, "fleet drops")
	if len(drops) != 1 || !strings.Contains(drops[0], `"email":"gap@example.com"`) || !strings.Contains(drops[0], `"delta":7`) {
		t.Errorf("drop lines = %v, want one for the gap machine with the rise of 7", drops)
	}
	for _, l := range jsonLines(lines, "fleet version_lag") {
		if strings.Contains(l, "flap@example.com") {
			t.Errorf("the mid-upgrade machine got a lag line: %s", l)
		}
	}
	if !strings.Contains(lines, `"behind":1`) || !strings.Contains(lines, `"current":1`) {
		t.Errorf("summary does not read behind 1 (the gap machine) current 1 (the alternating one):\n%s", jsonLines(lines, "fleet summary"))
	}
	for _, m := range ev.Machines {
		if m.DeviceID == flap.ID && (m.VersionState != fleet.VersionCurrent || m.Build != "788dcb3" || m.AlsoRunning != "23713ea") {
			t.Errorf("the alternating machine = %s on %q alongside %q, want current on 788dcb3 alongside 23713ea", m.VersionState, m.Build, m.AlsoRunning)
		}
	}
	// The next tick moves no counter, so the seven are said once.
	out.Reset()
	if _, _, err := runner.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(jsonLines(out.String(), "fleet drops")); n != 0 {
		t.Errorf("the second tick repeated %d drop lines", n)
	}
}
