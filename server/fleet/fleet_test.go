package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 9, 11, 14, 0, 0, 0, time.UTC)

// v2Report builds a schema-2 report as the 788dcb3 client sends it.
func v2Report(email, device, commit string, received time.Time, conds ...Condition) Report {
	raw, _ := json.Marshal(map[string]any{
		"schema_version": 2, "agent_version": commit, "agent_commit": commit, "channel": "latest",
		"hostname": "mbp-" + device, "spool": map[string]any{"pending": 3, "quarantine": 0, "parked": 0, "dropped": map[string]int{}},
		"empty_starts_24h": 0, "conditions": conds,
	})
	r, err := DecodeReport(email, device, received, received, worstOf(conds), commit, raw)
	if err != nil {
		panic(err)
	}
	return r
}

// v1Report builds a schema-1 report as the 23713ea fleet sends it: no
// commit, no parked queue, no empty-start count.
func v1Report(email, device, version string, received time.Time, quarantine int, conds ...Condition) Report {
	raw, _ := json.Marshal(map[string]any{
		"schema_version": 1, "agent_version": version, "hostname": "mbp-" + device,
		"spool": map[string]any{"pending": 0, "quarantine": quarantine}, "conditions": conds,
	})
	r, err := DecodeReport(email, device, received, received, worstOf(conds), version, raw)
	if err != nil {
		panic(err)
	}
	return r
}

func worstOf(conds []Condition) string {
	w := "info"
	for _, c := range conds {
		if c.Level == "critical" || (c.Level == "degraded" && w != "critical") {
			w = c.Level
		}
	}
	return w
}

// TestVersionStateFollowsTheManifestCommit is criterion l: a device on the
// manifest's commit is current, any other sha is lagging (behind once the
// publish is a day old), a non-sha version is unmanaged and never lags, and
// version 1 reports are judged by agent_version.
func TestVersionStateFollowsTheManifestCommit(t *testing.T) {
	manifest := &Manifest{Version: "788dcb3", Commit: "788dcb3a1b2c3d4e5f60718293a4b5c6d7e8f901", BuildDate: now.Add(-30 * time.Hour)}
	in := Inputs{Now: now, ServerBuild: "788dcb3", Manifest: manifest,
		Devices: []Device{
			{ID: "d-cur", Email: "a@example.com"}, {ID: "d-old", Email: "b@example.com"},
			{ID: "d-dev", Email: "c@example.com"}, {ID: "d-dirty", Email: "d@example.com"},
			{ID: "d-v1", Email: "e@example.com"}, {ID: "d-never", Email: "f@example.com", AgentVersion: "23713ea"},
		},
		Reports: []Report{
			v2Report("a@example.com", "d-cur", "788dcb3", now.Add(-time.Minute)),
			v2Report("b@example.com", "d-old", "23713ea", now.Add(-time.Minute)),
			v2Report("c@example.com", "d-dev", "dev", now.Add(-time.Minute)),
			v2Report("d@example.com", "d-dirty", "788dcb3-dirty", now.Add(-time.Minute)),
			v1Report("e@example.com", "d-v1", "23713ea", now.Add(-time.Minute), 0),
		},
	}
	ev := Evaluate(in)
	states := map[string]string{}
	for _, m := range ev.Machines {
		states[m.DeviceID] = m.VersionState
	}
	want := map[string]string{"d-cur": VersionCurrent, "d-old": VersionLagging, "d-dev": VersionUnmanaged, "d-dirty": VersionUnmanaged, "d-v1": VersionLagging, "d-never": VersionLagging}
	for id, w := range want {
		if states[id] != w {
			t.Errorf("%s = %s, want %s", id, states[id], w)
		}
	}
	if ev.Summary.Current != 1 || ev.Summary.Behind != 2 || ev.Summary.Unmanaged != 2 {
		t.Errorf("summary current=%d behind=%d unmanaged=%d, want 1/2/2 (the never-reported device is not a lag line)", ev.Summary.Current, ev.Summary.Behind, ev.Summary.Unmanaged)
	}
	if len(ev.Lag) != 2 {
		t.Fatalf("lag lines = %+v, want the two reporting machines on 23713ea", ev.Lag)
	}
	for _, l := range ev.Lag {
		if l.PublishedBuild != manifest.Commit || l.HoursBehind < 29.9 || l.HoursBehind > 30.1 {
			t.Errorf("lag line = %+v", l)
		}
	}
	for _, c := range ev.CTAs {
		if c.Kind == "version_lag" && (c.Email == "c@example.com" || c.Email == "d@example.com") {
			t.Errorf("an unmanaged build got a lag CTA: %+v", c)
		}
	}
	// A publish younger than a day is not lag yet.
	manifest.BuildDate = now.Add(-2 * time.Hour)
	if ev := Evaluate(in); ev.Summary.Behind != 0 || len(ev.Lag) != 2 {
		t.Errorf("a two-hour-old publish counted %d behind (lag lines %d)", ev.Summary.Behind, len(ev.Lag))
	}
	// No manifest: one warning, nothing lags, versions unknown.
	in.Manifest = nil
	ev = Evaluate(in)
	if len(ev.Warnings) != 1 || ev.Warnings[0] != "release manifest missing" || len(ev.Lag) != 0 || ev.Summary.ManifestPresent {
		t.Errorf("no manifest: warnings %v lag %d present %v", ev.Warnings, len(ev.Lag), ev.Summary.ManifestPresent)
	}
	if states := Evaluate(in).Machines; states[0].VersionState != VersionUnknown {
		t.Errorf("without a manifest the state is %s, want unknown", states[0].VersionState)
	}
}

// TestCTAListRanksQuarantineFirstAndNeverHidesItBehindLag is the fleet
// baseline at the 788dcb3 promotion: ten devices critical from historical
// quarantine, every one of them also behind the published build. The
// quarantine rows lead with the replay verb; the lag rows follow.
func TestCTAListRanksQuarantineFirstAndNeverHidesItBehindLag(t *testing.T) {
	manifest := &Manifest{Commit: "788dcb3", BuildDate: now.Add(-72 * time.Hour)}
	in := Inputs{Now: now, Manifest: manifest}
	for i := range 25 {
		email := string(rune('a'+i)) + "@example.com"
		id := "d" + string(rune('a'+i))
		in.Devices = append(in.Devices, Device{ID: id, Email: email})
		var conds []Condition
		q := 0
		if i < 10 {
			q = 5 + i
			conds = []Condition{{Level: "critical", Kind: "quarantine_nonempty", Detail: "items permanently undeliverable", Since: now.Add(-time.Duration(i+1) * 24 * time.Hour)}}
		}
		in.Reports = append(in.Reports, v1Report(email, id, "23713ea", now.Add(-time.Minute), q, conds...))
	}
	ev := Evaluate(in)
	if ev.Summary.QuarantineDevices != 10 || ev.Summary.Behind != 25 || ev.Summary.Enrolled != 25 {
		t.Fatalf("summary = %+v", ev.Summary)
	}
	if len(ev.CTAs) != 35 {
		t.Fatalf("%d CTAs, want 10 quarantine + 25 lag", len(ev.CTAs))
	}
	for i := range 10 {
		c := ev.CTAs[i]
		if c.Kind != "quarantine_nonempty" || c.Command != "loop-sessions doctor --replay-quarantine; loop-sessions doctor --redrive" || c.Anchor != "runbook-quarantine" {
			t.Errorf("CTA %d = %+v, want a quarantine row with the replay verb first", i, c)
		}
		if !strings.Contains(c.Action, "Ask ") || !strings.Contains(c.Action, "doctor --replay-quarantine") {
			t.Errorf("CTA %d action does not ask the person to run the verb: %q", i, c.Action)
		}
	}
	// Longest standing first within the kind.
	if ev.CTAs[0].Email != "j@example.com" {
		t.Errorf("first quarantine CTA is %s, want the one standing ten days", ev.CTAs[0].Email)
	}
	for _, c := range ev.CTAs[10:] {
		if c.Kind != "version_lag" || c.Command != "loop-sessions daemon --upgrade-now" || c.Anchor != "runbook-upgrade" {
			t.Errorf("trailing CTA = %+v, want version lag", c)
		}
	}
	// Every CTA kind names a real anchor.
	for kind := range ctaTable {
		if _, _, anchor, ok := CTAFor(kind); !ok || !strings.HasPrefix(anchor, "runbook-") {
			t.Errorf("%s has no runbook anchor", kind)
		}
	}
}

// TestEmptyStartRuleIsPerDeviceWithMutes: r8's rule fires at twenty aborted
// spawns that are at least half the device's sessions; a mute for the person
// moves the row to the muted list and drops the line.
func TestEmptyStartRuleIsPerDeviceWithMutes(t *testing.T) {
	in := Inputs{Now: now, EmptyStarts: []EmptyStart{
		{Email: "casey@example.com", DeviceID: "dv", Aborted: 48, Total: 50, Cwds: []string{"/home/casey", "/home/casey/work"}},
		{Email: "taylor@example.com", DeviceID: "da", Aborted: 9, Total: 136},
		{Email: "few@example.com", DeviceID: "df", Aborted: 19, Total: 19},
		{Email: "half@example.com", DeviceID: "dh", Aborted: 20, Total: 41},
	}}
	ev := Evaluate(in)
	if len(ev.EmptyStarts) != 1 || ev.EmptyStarts[0].Email != "casey@example.com" || ev.EmptyStarts[0].Rate < 0.95 {
		t.Fatalf("empty-start lines = %+v, want casey alone", ev.EmptyStarts)
	}
	if ev.EmptyStarts[0].Recipe != EmptyStartRecipe || len(ev.EmptyStarts[0].Cwds) != 2 {
		t.Errorf("line lacks the recipe or the cwd set: %+v", ev.EmptyStarts[0])
	}
	if got := ev.Summary.Empties; got != 96 {
		t.Errorf("summary empties = %d", got)
	}
	if len(ev.CTAs) != 1 || ev.CTAs[0].Kind != "empty_start" || !strings.Contains(ev.CTAs[0].Detail, "launcher.log") {
		t.Fatalf("CTAs = %+v", ev.CTAs)
	}
	in.Mutes = []Mute{{Email: "casey@example.com", Kind: "empty_start", Until: now.Add(24 * time.Hour), Note: "asked on Slack"}}
	ev = Evaluate(in)
	if len(ev.CTAs) != 0 || len(ev.Muted) != 1 || ev.Muted[0].Muted == nil || ev.Muted[0].Muted.Note != "asked on Slack" {
		t.Errorf("mute did not move the row: ctas %d muted %+v", len(ev.CTAs), ev.Muted)
	}
	in.Mutes[0].Until = now.Add(-time.Minute)
	if ev = Evaluate(in); len(ev.CTAs) != 1 {
		t.Error("an expired mute still silences the row")
	}
}

// TestSilentDropsAndParkedFromVersionTwoReports: a machine quiet for a day
// is silent (from arrival, not its own clock); drop deltas become lines and a
// CTA by reason; a parked count is read only where the report carries one.
func TestSilentDropsAndParkedFromVersionTwoReports(t *testing.T) {
	parkedRaw, _ := json.Marshal(map[string]any{"schema_version": 2, "agent_commit": "788dcb3", "spool": map[string]any{"parked": 3}})
	parked, err := DecodeReport("p@example.com", "dp", now, now, "info", "788dcb3", parkedRaw)
	if err != nil {
		t.Fatal(err)
	}
	in := Inputs{Now: now,
		Devices: []Device{{ID: "ds", Email: "s@example.com"}, {ID: "dp", Email: "p@example.com"}, {ID: "dv1", Email: "v@example.com"}},
		Reports: []Report{
			v2Report("s@example.com", "ds", "788dcb3", now.Add(-26*time.Hour), Condition{Level: "critical", Kind: "capture_blocked"}),
			parked,
			v1Report("v@example.com", "dv1", "23713ea", now, 0),
		},
		Drops: []DropDelta{{Email: "v@example.com", DeviceID: "dv1", Reason: "disk_full", Delta: 40}, {Email: "v@example.com", DeviceID: "dv1", Reason: "hook_abandoned", Delta: 0}},
	}
	ev := Evaluate(in)
	if ev.Summary.Silent != 1 || len(ev.Silent) != 1 || ev.Silent[0].Hours < 25.9 || ev.Silent[0].LastWorst != "critical" {
		t.Errorf("silent = %+v", ev.Silent)
	}
	// A silent machine's last conditions are not counted as live.
	if ev.Summary.CaptureBlocked != 0 {
		t.Errorf("a silent machine's stale capture_blocked was counted live")
	}
	if ev.Summary.ParkedDevices != 1 {
		t.Errorf("parked devices = %d", ev.Summary.ParkedDevices)
	}
	for _, m := range ev.Machines {
		if m.DeviceID == "dv1" && (m.Parked != nil || m.EmptyStarts24h != nil) {
			t.Error("a version 1 report was read as carrying parked or empty-start counts")
		}
		if m.DeviceID == "dp" && (m.Parked == nil || *m.Parked != 3) {
			t.Error("the version 2 parked count was lost")
		}
	}
	if len(ev.Drops) != 1 || ev.Drops[0].Reason != "disk_full" || ev.Drops[0].Delta != 40 || ev.Summary.Drops != 40 {
		t.Errorf("drops = %+v", ev.Drops)
	}
	kinds := map[string]bool{}
	for _, c := range ev.CTAs {
		kinds[c.Kind] = true
	}
	for _, want := range []string{"silent", "parked", "drops_recorded"} {
		if !kinds[want] {
			t.Errorf("no %s CTA in %+v", want, ev.CTAs)
		}
	}
}

// TestMissingAnswerRateIsPerDayBuildAndEntrypoint: a line per cohort, the
// summary rate is the latest day's, and the fleet CTA appears past 5%.
func TestMissingAnswerRateIsPerDayBuildAndEntrypoint(t *testing.T) {
	d0, d1 := now.Add(-48*time.Hour).Truncate(24*time.Hour), now.Truncate(24*time.Hour)
	in := Inputs{Now: now, Answers: []AnswerCohort{
		{Day: d0, AgentVersion: "23713ea", Entrypoint: "cli", Turns: 100, Answered: 1},
		{Day: d1, AgentVersion: "23713ea", Entrypoint: "cli", Turns: 40, Answered: 0},
		{Day: d1, AgentVersion: "788dcb3", Entrypoint: "cli", Turns: 60, Answered: 59},
		{Day: d1, AgentVersion: "788dcb3", Entrypoint: "sdk-cli", Turns: 0, Answered: 0},
	}}
	ev := Evaluate(in)
	if len(ev.MissingAnswers) != 3 {
		t.Fatalf("lines = %+v", ev.MissingAnswers)
	}
	if ev.Summary.AnswerTurns != 100 || ev.Summary.Answered != 59 || ev.Summary.MissingAnswerRate < 0.40 || ev.Summary.MissingAnswerRate > 0.42 {
		t.Errorf("summary = turns %d answered %d rate %.3f", ev.Summary.AnswerTurns, ev.Summary.Answered, ev.Summary.MissingAnswerRate)
	}
	if len(ev.CTAs) != 1 || ev.CTAs[0].Kind != "missing_answer" || ev.CTAs[0].Command != "loop-sessions daemon --upgrade-now" {
		t.Errorf("CTAs = %+v", ev.CTAs)
	}
	in.Answers = in.Answers[2:3]
	if ev = Evaluate(in); len(ev.CTAs) != 0 || ev.Summary.MissingAnswerRate > 0.02 {
		t.Errorf("a 98%% answered day raised a CTA: %+v", ev.CTAs)
	}
}

// fakeStore is the runner's port over fixtures.
type fakeStore struct {
	held     bool
	locks    int
	hours    [][]Hour
	rollups  int
	sweeps   int
	reports  []Report
	devices  []Device
	builds   []BuildSighting
	mutes    []Mute
	releases int
	// holding counts the locks not yet released; sweptUnderLock records a
	// sweep that ran while one was held, which contract f forbids.
	holding        int
	sweptUnderLock bool
	// lastTick is the fleet_ticks row: the moment of the newest tick any
	// instance sharing this store ran, by the store's clock. dbNow is that
	// clock, which a test moves on its own because no instance's clock may
	// move it; nil is the fixture moment, for the tests that tick once.
	lastTick time.Time
	dbNow    func() time.Time
}

// now is the store's clock, the only one the tick gate reads.
func (f *fakeStore) now() time.Time {
	if f.dbNow != nil {
		return f.dbNow()
	}
	return now
}

func (f *fakeStore) Lock(context.Context) (func(), bool, error) {
	f.locks++
	if !f.held {
		return nil, false, nil
	}
	f.holding++
	return func() { f.releases++; f.holding-- }, true, nil
}
func (f *fakeStore) TickDue(_ context.Context, minAge time.Duration) (bool, error) {
	at := f.now()
	if !f.lastTick.IsZero() && at.Sub(f.lastTick) < minAge {
		return false, nil
	}
	f.lastTick = at
	return true, nil
}
func (f *fakeStore) RecentBuilds(context.Context, time.Time) ([]BuildSighting, error) {
	return f.builds, nil
}
func (f *fakeStore) People(context.Context) ([]Person, error)  { return nil, nil }
func (f *fakeStore) Devices(context.Context) ([]Device, error) { return f.devices, nil }
func (f *fakeStore) Reports(context.Context) ([]Report, error) { return f.reports, nil }
func (f *fakeStore) Mutes(context.Context) ([]Mute, error)     { return f.mutes, nil }
func (f *fakeStore) Rollup(context.Context, time.Time, time.Time) (bool, error) {
	f.rollups++
	return false, nil
}
func (f *fakeStore) Hours(context.Context, time.Time) ([]Hour, error) {
	if len(f.hours) == 0 {
		return nil, nil
	}
	h := f.hours[0]
	f.hours = f.hours[1:]
	return h, nil
}
func (f *fakeStore) EmptyStarts(context.Context, time.Time) ([]EmptyStart, error) { return nil, nil }
func (f *fakeStore) MissingAnswers(context.Context, time.Time) ([]AnswerCohort, error) {
	return nil, nil
}
func (f *fakeStore) SweepHealth(context.Context, time.Time) (slog.Value, error) {
	f.sweeps++
	if f.holding > 0 {
		f.sweptUnderLock = true
	}
	return slog.StringValue("swept"), nil
}

type fakeManifest struct {
	m   Manifest
	err error
}

func (f fakeManifest) Latest(context.Context) (Manifest, error) { return f.m, f.err }

// TestRunnerTickEmitsTheContractLinesOnceAndSweepsHourly: one tick under
// the lock rolls up, reads, emits every line the contract names with its
// fields, warns about a missing manifest once, and sweeps once an hour.
func TestRunnerTickEmitsTheContractLinesOnceAndSweepsHourly(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	// The store's clock and this instance's, moved together wherever the
	// test means time to pass: the gate is judged by the first, and the
	// evaluation dates itself by the second.
	db, clock := now, now
	st := &fakeStore{held: true,
		dbNow:   func() time.Time { return db },
		devices: []Device{{ID: "d1", Email: "a@example.com"}},
		reports: []Report{v1Report("a@example.com", "d1", "23713ea", now.Add(-time.Minute), 2, Condition{Level: "critical", Kind: "quarantine_nonempty", Detail: "2 items"})},
		hours: [][]Hour{
			{{Email: "a@example.com", DeviceID: "d1", Hour: now.Truncate(time.Hour), Dropped: map[string]int64{"disk_full": 5}}},
			{{Email: "a@example.com", DeviceID: "d1", Hour: now.Truncate(time.Hour), Dropped: map[string]int64{"disk_full": 12}}},
		},
	}
	r := &Runner{Store: st, Manifest: fakeManifest{err: ErrManifestMissing}, ServerBuild: "788dcb3", Log: log, Now: func() time.Time { return clock }}
	ev, did, err := r.Tick(context.Background())
	if err != nil || !did {
		t.Fatalf("tick: did=%v err=%v", did, err)
	}
	if st.locks != 1 || st.releases != 1 || st.rollups != 1 || st.sweeps != 1 {
		t.Errorf("locks %d releases %d rollups %d sweeps %d", st.locks, st.releases, st.rollups, st.sweeps)
	}
	// The lock is held for the evaluation only: the sweep is minutes of
	// batches under its own lock, and holding the fleet lock across it would
	// make every other instance's tick a no-op for that long.
	if st.sweptUnderLock {
		t.Error("the sweep ran while the fleet lock was held; contract f wants a short transaction")
	}
	if len(ev.Drops) != 1 || ev.Drops[0].Delta != 7 {
		t.Errorf("drop deltas from the ledger before and after the rollup = %+v, want 12-5", ev.Drops)
	}
	lines := out.String()
	for _, want := range []string{
		`"msg":"fleet summary"`, `"enrolled":1`, `"quarantine_devices":1`, `"unmanaged":0`, `"manifest_present":false`,
		`"msg":"fleet condition"`, `"kind":"quarantine_nonempty"`, `"level":"critical"`, `"agent_version":"23713ea"`,
		`"msg":"fleet drops"`, `"reason":"disk_full"`, `"delta":7`,
		`"msg":"release manifest missing"`, `"msg":"health sweep"`,
	} {
		if !strings.Contains(lines, want) {
			t.Errorf("missing %s in\n%s", want, lines)
		}
	}
	if strings.Contains(lines, `"msg":"fleet version_lag"`) {
		t.Error("a lag line was emitted with no manifest")
	}
	// The second tick, twenty minutes later: no second manifest warning, no
	// second sweep, and the drop delta is zero because the ledger did not
	// move.
	out.Reset()
	db, clock = db.Add(20*time.Minute), clock.Add(20*time.Minute)
	st.hours = [][]Hour{
		{{Email: "a@example.com", DeviceID: "d1", Hour: now.Truncate(time.Hour), Dropped: map[string]int64{"disk_full": 12}}},
		{{Email: "a@example.com", DeviceID: "d1", Hour: now.Truncate(time.Hour), Dropped: map[string]int64{"disk_full": 12}}},
	}
	// did, so that a gate wrongly refusing this tick cannot pass the three
	// assertions below by having done nothing at all.
	if _, did, err := r.Tick(context.Background()); err != nil || !did {
		t.Fatalf("the second tick twenty minutes later: did=%v err=%v", did, err)
	}
	if strings.Contains(out.String(), "release manifest missing") || st.sweeps != 1 || strings.Contains(out.String(), `"msg":"fleet drops"`) {
		t.Errorf("second tick repeated the warning, swept again or re-counted drops:\n%s", out.String())
	}
	// Another instance holds the lock: nothing happens.
	st.held = false
	if _, did, err := r.Tick(context.Background()); did || err != nil {
		t.Errorf("a tick without the lock did work: did=%v err=%v", did, err)
	}
}

// linesOf splits a JSON log buffer into the lines carrying one message.
func linesOf(out string, msg string) []string {
	var got []string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, `"msg":"`+msg+`"`) {
			got = append(got, l)
		}
	}
	return got
}

// TestMutesHoldBackTheLinesAndTheGauges is review-1 F1: an active mute for a
// (person, kind) keeps the row on the page under muted and out of the action
// list, and also holds back that machine's alert line and its count in the
// summary gauge, so the line-count and gauge policies stay quiet for exactly
// what the operator silenced. The unmuted twin of every case still alerts,
// or the mute would be hiding more than it names.
func TestMutesHoldBackTheLinesAndTheGauges(t *testing.T) {
	manifest := &Manifest{Commit: "788dcb3", BuildDate: now.Add(-72 * time.Hour)}
	blocked := Condition{Level: "critical", Kind: "capture_blocked", Detail: "disk floor", Since: now.Add(-time.Hour)}
	in := Inputs{Now: now, Manifest: manifest,
		Devices: []Device{
			{ID: "d-silent-m", Email: "silent-muted@example.com"}, {ID: "d-silent", Email: "silent@example.com"},
			{ID: "d-lag-m", Email: "lag-muted@example.com"}, {ID: "d-lag", Email: "lag@example.com"},
			{ID: "d-blk-m", Email: "blocked-muted@example.com"}, {ID: "d-blk", Email: "blocked@example.com"},
			{ID: "d-drop-m", Email: "drops-muted@example.com"}, {ID: "d-drop", Email: "drops@example.com"},
		},
		Reports: []Report{
			v2Report("silent-muted@example.com", "d-silent-m", "788dcb3", now.Add(-30*time.Hour)),
			v2Report("silent@example.com", "d-silent", "788dcb3", now.Add(-30*time.Hour)),
			v2Report("lag-muted@example.com", "d-lag-m", "23713ea", now.Add(-time.Minute)),
			v2Report("lag@example.com", "d-lag", "23713ea", now.Add(-time.Minute)),
			v2Report("blocked-muted@example.com", "d-blk-m", "788dcb3", now.Add(-time.Minute), blocked),
			v2Report("blocked@example.com", "d-blk", "788dcb3", now.Add(-time.Minute), blocked),
			v2Report("drops-muted@example.com", "d-drop-m", "788dcb3", now.Add(-time.Minute)),
			v2Report("drops@example.com", "d-drop", "788dcb3", now.Add(-time.Minute)),
		},
		Drops: []DropDelta{
			{Email: "drops-muted@example.com", DeviceID: "d-drop-m", Reason: "disk_full", Delta: 40},
			{Email: "drops@example.com", DeviceID: "d-drop", Reason: "disk_full", Delta: 3},
		},
		EmptyStarts: []EmptyStart{
			{Email: "empty-muted@example.com", DeviceID: "d-empty-m", Aborted: 48, Total: 50, Cwds: []string{"/home/m"}},
			{Email: "empty@example.com", DeviceID: "d-empty", Aborted: 30, Total: 40, Cwds: []string{"/home/e"}},
		},
		Mutes: []Mute{
			{Email: "silent-muted@example.com", Kind: "silent", Until: now.Add(time.Hour), Note: "on leave"},
			{Email: "lag-muted@example.com", Kind: "version_lag", Until: now.Add(time.Hour), Note: "pinned build"},
			{Email: "blocked-muted@example.com", Kind: "capture_blocked", Until: now.Add(time.Hour), Note: "disk on order"},
			{Email: "drops-muted@example.com", Kind: "drops_recorded", Until: now.Add(time.Hour), Note: "known"},
			{Email: "empty-muted@example.com", Kind: "empty_start", Until: now.Add(time.Hour), Note: "asked on Slack"},
		},
	}
	ev := Evaluate(in)
	if len(ev.Silent) != 1 || ev.Silent[0].Email != "silent@example.com" {
		t.Errorf("silent lines = %+v, want the unmuted machine alone", ev.Silent)
	}
	if len(ev.Lag) != 1 || ev.Lag[0].Email != "lag@example.com" {
		t.Errorf("lag lines = %+v, want the unmuted machine alone", ev.Lag)
	}
	if len(ev.Drops) != 1 || ev.Drops[0].Email != "drops@example.com" {
		t.Errorf("drop lines = %+v, want the unmuted machine alone", ev.Drops)
	}
	if len(ev.EmptyStarts) != 1 || ev.EmptyStarts[0].Email != "empty@example.com" {
		t.Errorf("empty-start lines = %+v, want the unmuted machine alone", ev.EmptyStarts)
	}
	s := ev.Summary
	if s.Silent != 1 || s.Behind != 1 || s.Drops != 3 || s.CaptureBlocked != 1 || s.Empties != 30 || s.Sessions != 40 {
		t.Errorf("gauges count the muted machines: silent %d behind %d drops %d capture_blocked %d empties %d sessions %d",
			s.Silent, s.Behind, s.Drops, s.CaptureBlocked, s.Empties, s.Sessions)
	}
	if len(ev.Muted) != 5 || len(ev.CTAs) != 5 {
		t.Errorf("%d muted rows and %d CTAs, want 5 and 5", len(ev.Muted), len(ev.CTAs))
	}
	// The muted machines keep their state on the page.
	for _, m := range ev.Machines {
		switch m.DeviceID {
		case "d-silent-m":
			if !m.Silent {
				t.Error("the muted silent machine lost its silent state")
			}
		case "d-lag-m":
			if m.VersionState != VersionLagging {
				t.Errorf("the muted lagging machine reads %s", m.VersionState)
			}
		case "d-blk-m":
			if !hasKind(m.Conditions, "capture_blocked") {
				t.Error("the muted blocked machine lost its condition")
			}
		}
	}
	// Emit writes no alert line for a muted (person, kind); the condition
	// census still names the muted machine, because a mute silences the
	// alert, not the record of what the machine said.
	var out bytes.Buffer
	Emit(context.Background(), slog.New(slog.NewJSONHandler(&out, nil)), ev)
	lines := out.String()
	for _, msg := range []string{"fleet silent", "fleet version_lag", "fleet drops", "fleet empty_start"} {
		got := linesOf(lines, msg)
		if len(got) != 1 {
			t.Errorf("%d %q lines, want one:\n%s", len(got), msg, strings.Join(got, "\n"))
		}
		for _, l := range got {
			if strings.Contains(l, "-muted@") {
				t.Errorf("a muted machine was alerted: %s", l)
			}
		}
	}
	if got := linesOf(lines, "fleet condition"); len(got) != 2 {
		t.Errorf("%d condition lines, want the census of both blocked machines", len(got))
	}
	if !strings.Contains(lines, `"muted":5`) || !strings.Contains(lines, `"silent":1`) || !strings.Contains(lines, `"behind":1`) || !strings.Contains(lines, `"capture_blocked":1`) {
		t.Errorf("the summary line does not carry the muted count and the unmuted gauges:\n%s", linesOf(lines, "fleet summary"))
	}
	// A lapsed mute alerts again.
	for i := range in.Mutes {
		in.Mutes[i].Until = now.Add(-time.Minute)
	}
	if ev := Evaluate(in); len(ev.Silent) != 2 || len(ev.Lag) != 2 || len(ev.Drops) != 2 || len(ev.EmptyStarts) != 2 || ev.Summary.Behind != 2 || len(ev.Muted) != 0 {
		t.Errorf("lapsed mutes still hold lines back: silent %d lag %d drops %d empty %d behind %d muted %d",
			len(ev.Silent), len(ev.Lag), len(ev.Drops), len(ev.EmptyStarts), ev.Summary.Behind, len(ev.Muted))
	}
}

// TestVersionStateIsTheNewestBuildSeenInTheLastHour is the lead's finding
// after review-1: a laptop runs the old daemon until its session ends while
// new sessions spawn daemons from the new binary, so its newest report
// alternates between builds for hours. A device that named the published
// build in any report of the last hour is current whatever its newest report
// says, it is behind only when no report in the window did, and the page is
// told which other build it is still running alongside.
func TestVersionStateIsTheNewestBuildSeenInTheLastHour(t *testing.T) {
	manifest := &Manifest{Commit: "788dcb3a1b2c3d4e5f60718293a4b5c6d7e8f901", BuildDate: now.Add(-30 * time.Hour)}
	in := Inputs{Now: now, Manifest: manifest,
		Devices: []Device{{ID: "d-flap", Email: "flap@example.com"}, {ID: "d-old", Email: "old@example.com"}, {ID: "d-quiet", Email: "quiet@example.com"}},
		Reports: []Report{
			// The newest report is the old daemon's.
			v2Report("flap@example.com", "d-flap", "23713ea", now.Add(-time.Minute)),
			v2Report("old@example.com", "d-old", "23713ea", now.Add(-time.Minute)),
			// Last reported ninety minutes ago on the published build: not
			// silent, nothing in the window, so the newest report stands.
			v2Report("quiet@example.com", "d-quiet", "788dcb3", now.Add(-90*time.Minute)),
		},
		Builds: []BuildSighting{
			{Email: "flap@example.com", DeviceID: "d-flap", Build: "23713ea", LastSeen: now.Add(-time.Minute)},
			{Email: "flap@example.com", DeviceID: "d-flap", Build: "788dcb3", LastSeen: now.Add(-6 * time.Minute)},
			{Email: "old@example.com", DeviceID: "d-old", Build: "23713ea", LastSeen: now.Add(-time.Minute)},
		},
	}
	check := func(in Inputs) {
		t.Helper()
		ev := Evaluate(in)
		by := map[string]Machine{}
		for _, m := range ev.Machines {
			by[m.DeviceID] = m
		}
		if f := by["d-flap"]; f.VersionState != VersionCurrent || f.Build != "788dcb3" || f.AlsoRunning != "23713ea" {
			t.Errorf("the alternating device = %s on %q alongside %q, want current on 788dcb3 alongside 23713ea", f.VersionState, f.Build, f.AlsoRunning)
		}
		if o := by["d-old"]; o.VersionState != VersionLagging || o.AlsoRunning != "" {
			t.Errorf("the device only ever on the old build = %s alongside %q, want lagging alone", o.VersionState, o.AlsoRunning)
		}
		if q := by["d-quiet"]; q.VersionState != VersionCurrent || q.AlsoRunning != "" {
			t.Errorf("the device with nothing in the window = %s alongside %q, want current from its newest report", q.VersionState, q.AlsoRunning)
		}
		if ev.Summary.Behind != 1 || ev.Summary.Current != 2 {
			t.Errorf("behind %d current %d, want 1 and 2", ev.Summary.Behind, ev.Summary.Current)
		}
		if len(ev.Lag) != 1 || ev.Lag[0].Email != "old@example.com" {
			t.Errorf("lag lines = %+v, want the old device alone", ev.Lag)
		}
		for _, c := range ev.CTAs {
			if c.Kind == "version_lag" && c.Email != "old@example.com" {
				t.Errorf("a version_lag CTA for %s, whose window names the published build", c.Email)
			}
		}
	}
	check(in)
	// The order the sightings arrive in does not decide.
	in.Builds[0], in.Builds[1] = in.Builds[1], in.Builds[0]
	check(in)
	// Once the old daemon's session ends the window holds one build.
	in.Builds = []BuildSighting{{Email: "flap@example.com", DeviceID: "d-flap", Build: "788dcb3", LastSeen: now.Add(-time.Minute)}, in.Builds[2]}
	in.Reports[0] = v2Report("flap@example.com", "d-flap", "788dcb3", now.Add(-time.Minute))
	ev := Evaluate(in)
	for _, m := range ev.Machines {
		if m.DeviceID == "d-flap" && (m.AlsoRunning != "" || m.VersionState != VersionCurrent) {
			t.Errorf("a settled device still reads alongside %q (%s)", m.AlsoRunning, m.VersionState)
		}
	}
}

// TestSilentMachinesAreNotCountedBehind is review-1 minor 3: a machine that
// is off cannot upgrade, so it is the silent row's problem and not the
// upgrade runbook's; its version state stays on the row for the page.
func TestSilentMachinesAreNotCountedBehind(t *testing.T) {
	manifest := &Manifest{Commit: "788dcb3", BuildDate: now.Add(-72 * time.Hour)}
	in := Inputs{Now: now, Manifest: manifest,
		Devices: []Device{{ID: "d-off", Email: "off@example.com"}},
		Reports: []Report{v1Report("off@example.com", "d-off", "23713ea", now.Add(-26*time.Hour), 0)},
	}
	ev := Evaluate(in)
	if len(ev.Machines) != 1 || !ev.Machines[0].Silent || ev.Machines[0].VersionState != VersionLagging {
		t.Fatalf("machine = %+v, want silent with its lagging state kept", ev.Machines)
	}
	if ev.Summary.Behind != 0 || len(ev.Lag) != 0 {
		t.Errorf("a silent machine counted behind (%d) or got a lag line (%d)", ev.Summary.Behind, len(ev.Lag))
	}
	kinds := map[string]bool{}
	for _, c := range ev.CTAs {
		kinds[c.Kind] = true
	}
	if !kinds["silent"] || kinds["version_lag"] {
		t.Errorf("CTAs = %v, want silent without version_lag", kinds)
	}
}
