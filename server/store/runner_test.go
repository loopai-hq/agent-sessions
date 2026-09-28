package store

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
	"unicode/utf8"

	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

// The bounds the contract states, pinned: a sub-batch of at most 5,000
// rows under a 60 s ceiling, a default cap of 1,500 rows/s, five failed
// batches before a step parks, a floor of 250 rows for a sub-batch that
// met the ceiling. A larger sub-batch would run past the ceiling on the
// production clone (about 4 s per 5,000 bodies), and a configured value
// above the cap is clamped rather than honoured.
func TestDeriveBoundsAreTheRehearsedOnes(t *testing.T) {
	if deriveEventRowsPerBatch != 5000 || deriveEventRowsFloor != 250 {
		t.Errorf("deriveEventRowsPerBatch = %d floor %d, want 5000 and 250", deriveEventRowsPerBatch, deriveEventRowsFloor)
	}
	if deriveStatementTimeout != 60*time.Second || (DeriveConfig{}).withDefaults().StatementTimeout != deriveStatementTimeout {
		t.Errorf("deriveStatementTimeout = %s (default %s), want 60s", deriveStatementTimeout, (DeriveConfig{}).withDefaults().StatementTimeout)
	}
	if derive.DefaultRowsPerSec != 1500 {
		t.Errorf("DefaultRowsPerSec = %d, want 1500", derive.DefaultRowsPerSec)
	}
	if deriveRetryCap != 5 {
		t.Errorf("deriveRetryCap = %d, want 5", deriveRetryCap)
	}
	cfg := DeriveConfig{EventRowsPerBatch: 50_000}.withDefaults()
	if cfg.EventRowsPerBatch != deriveEventRowsPerBatch {
		t.Errorf("a configured sub-batch of 50,000 became %d, want clamped to %d", cfg.EventRowsPerBatch, deriveEventRowsPerBatch)
	}
	if cfg.RowsPerSec != derive.DefaultRowsPerSec || cfg.Window.String() != "02-06 "+time.Local.String() {
		t.Errorf("defaults = %d rows/s in window %s, want 1500 in 02-06 %s", cfg.RowsPerSec, cfg.Window, time.Local)
	}
	if got := (DeriveConfig{Window: derive.Window{Always: true}}).withDefaults().Window.String(); got != "always" {
		t.Errorf("an explicit always-window became %q", got)
	}
}

// A keys batch that met the ceiling is halved down to the floor, and at the
// floor there is nothing left to try but the attempt itself (review-2
// finding 22): 5,000 -> 2,500 -> 1,250 -> 625 -> 312 -> 250 -> park.
func TestNextEventBatchHalvesToTheFloorAndThenStops(t *testing.T) {
	var sizes []int
	cur := deriveEventRowsPerBatch
	for {
		next, ok := nextEventBatch(cur)
		if !ok {
			break
		}
		if next >= cur {
			t.Fatalf("nextEventBatch(%d) = %d grew", cur, next)
		}
		sizes = append(sizes, next)
		cur = next
	}
	want := []int{2500, 1250, 625, 312, 250}
	if strings.Trim(strings.Join(strings.Fields(fmt.Sprint(sizes)), ","), "[]") != strings.Trim(strings.Join(strings.Fields(fmt.Sprint(want)), ","), "[]") {
		t.Errorf("halvings = %v, want %v", sizes, want)
	}
	if next, ok := nextEventBatch(deriveEventRowsFloor); ok || next != deriveEventRowsFloor {
		t.Errorf("nextEventBatch(floor) = %d, %v; want the floor and no room", next, ok)
	}
	if next, ok := nextEventBatch(8); ok || next != 8 {
		t.Errorf("nextEventBatch(8) = %d, %v; a test-sized batch below the floor is never grown to it", next, ok)
	}
	if next, ok := nextEventBatch(300); !ok || next != 250 {
		t.Errorf("nextEventBatch(300) = %d, %v; want the floor", next, ok)
	}
}

// The parking map never grows past its cap: expired entries go first, and
// with none expired the session whose backoff ends soonest is forgotten so
// the new failure can be recorded (review-2 finding 27).
func TestParkingEvictsTheSoonestEntryWhenFull(t *testing.T) {
	s := NewWithDB(&fakeDB{}, nil)
	now := time.Date(2026, 9, 11, 3, 0, 0, 0, time.UTC)
	for i := 0; i < dirtyParkedMax; i++ {
		s.parkSession(fmt.Sprintf("s-%05d", i), now.Add(time.Duration(i)*time.Millisecond))
	}
	if n := len(s.dirtyParked.rows); n != dirtyParkedMax {
		t.Fatalf("%d parked after filling, want %d", n, dirtyParkedMax)
	}
	// Nothing has expired a second later; the newcomer displaces the entry
	// that was parked first (its backoff ends first).
	s.parkSession("s-new", now.Add(time.Second))
	if n := len(s.dirtyParked.rows); n != dirtyParkedMax {
		t.Errorf("%d parked after one more failure, want the cap %d", n, dirtyParkedMax)
	}
	if _, ok := s.dirtyParked.rows["s-00000"]; ok {
		t.Error("the entry whose backoff ends soonest survived")
	}
	if _, ok := s.dirtyParked.rows["s-new"]; !ok {
		t.Error("the new failure was not recorded")
	}
	// A repeat failure of a parked session does not evict anybody.
	s.parkSession("s-00001", now.Add(2*time.Second))
	if n := len(s.dirtyParked.rows); n != dirtyParkedMax {
		t.Errorf("%d parked after a repeat, want %d", n, dirtyParkedMax)
	}
	if p := s.dirtyParked.rows["s-00001"]; p.attempts != 2 {
		t.Errorf("repeat attempts = %d, want 2", p.attempts)
	}
	// Past every backoff, the expired go first: only the newcomer stays.
	s.parkSession("s-later", now.Add(2*time.Hour))
	if n := len(s.dirtyParked.rows); n != 1 {
		t.Errorf("%d parked once every backoff ran out, want the newcomer alone", n)
	}
}

// Elapsed reaches the caller on both passes: the deferred write lands in
// the named result rather than in a local the return had already copied
// (review-2 finding 24; the clone's log read "elapsed":"0s" on every line).
func TestPassAndDirtyResultsCarryTheirElapsedTime(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "FROM derived_schema", rows: [][]any{{DerivedSchema}}}}}
	s := NewWithDB(db, nil)
	ctx := context.Background()
	pass, err := s.RunDerive(ctx, DeriveConfig{})
	if err != nil || !pass.Done {
		t.Fatalf("RunDerive = %+v, %v", pass, err)
	}
	if pass.Elapsed <= 0 {
		t.Errorf("pass.Elapsed = %s, want the pass's wall-clock", pass.Elapsed)
	}
	res, err := s.DeriveDirty(ctx, DeriveConfig{})
	if err != nil {
		t.Fatalf("DeriveDirty: %v", err)
	}
	if res.Elapsed <= 0 {
		t.Errorf("dirty.Elapsed = %s, want the pass's wall-clock", res.Elapsed)
	}
}

// The two lines the derive metrics read, with the fields they extract:
// "derive step" carries rows, seconds and rows_per_sec; "derive step
// failed" carries attempts, the error and the reset statement. Both are
// JSON at the top level, because the log-based metric filters match on
// jsonPayload.message and extract jsonPayload.<field>.
//
// The logger is built the way the server builds its own (app.NewCloudLogger:
// .With("version", build)), because the log contract stamps the build on
// every line as "version" and a runner attribute of the same name made a
// JSON object with two "version" members, of which a reader keeps the
// runner's and loses the build (review-1 finding 18). The runner's own
// version is derive_version, and every line carries exactly one "version".
func TestDeriveLogLinesCarryTheFieldsTheMetricsRead(t *testing.T) {
	var out bytes.Buffer
	s := NewWithDB(&fakeDB{}, nil)
	s.SetLogger(slog.New(slog.NewJSONHandler(&out, nil)).With("version", "788dcb3-build"))
	ctx := context.Background()

	s.logStep(ctx, DeriveStepResult{Step: "event_keys", Rows: 5000, Batches: 1, Seconds: 2.5, Finished: true})
	s.logStepFailed(ctx, "turns", 5, "boom", 40)
	s.logger().InfoContext(ctx, "derive pass", slog.Any("pass", DerivePass{Version: DerivedSchema, Done: true}))
	s.logBatchShrunk(ctx, "event_keys", 5000, 2500, errors.New("canceling statement due to statement timeout"))

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("%d lines, want 4:\n%s", len(lines), out.String())
	}
	for _, line := range lines {
		if n := strings.Count(line, `"version"`); n != 1 {
			t.Errorf("%d \"version\" keys on one line, want exactly the build's:\n%s", n, line)
		}
	}
	var step, failed, pass map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &step); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &failed); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[2]), &pass); err != nil {
		t.Fatal(err)
	}
	if step["msg"] != "derive step" || step["step"] != "event_keys" || step["rows"] != float64(5000) || step["seconds"] != 2.5 || step["rows_per_sec"] != float64(2000) || step["finished"] != true {
		t.Errorf("derive step line = %v", step)
	}
	if step["version"] != "788dcb3-build" || step["derive_version"] != float64(DerivedSchema) {
		t.Errorf("derive step line: version = %v derive_version = %v, want the build and %d", step["version"], step["derive_version"], DerivedSchema)
	}
	if failed["msg"] != "derive step failed" || failed["level"] != "ERROR" || failed["step"] != "turns" || failed["attempts"] != float64(5) || failed["error"] != "boom" {
		t.Errorf("derive step failed line = %v", failed)
	}
	if failed["version"] != "788dcb3-build" || failed["derive_version"] != float64(DerivedSchema) {
		t.Errorf("derive step failed line: version = %v derive_version = %v", failed["version"], failed["derive_version"])
	}
	if recover, _ := failed["recover"].(string); !strings.Contains(recover, fmt.Sprintf("UPDATE derive_jobs SET attempts = 0 WHERE version = %d AND step = 'turns'", DerivedSchema)) {
		t.Errorf("the failure line's reset statement is %q", recover)
	}
	group, _ := pass["pass"].(map[string]any)
	if group["derive_version"] != float64(DerivedSchema) || group["version"] != nil {
		t.Errorf("derive pass group = %v, want derive_version and no version key", group)
	}
	var shrunk map[string]any
	if err := json.Unmarshal([]byte(lines[3]), &shrunk); err != nil {
		t.Fatal(err)
	}
	if shrunk["msg"] != "derive batch shrunk" || shrunk["level"] != "WARN" || shrunk["step"] != "event_keys" ||
		shrunk["rows_per_batch"] != float64(2500) || shrunk["previous"] != float64(5000) || shrunk["derive_version"] != float64(DerivedSchema) {
		t.Errorf("derive batch shrunk line = %v", shrunk)
	}
}

// The step list is the contract's, in the contract's order: the five
// indexes first (the fifth, on ingested_at, is the export job's planning
// read and per-day COPY; the contract section 3), then the one body-reading
// step, then the column-only steps that depend on what came before them.
func TestDeriveStepsRunInTheContractsOrder(t *testing.T) {
	var names []string
	for _, st := range deriveSteps() {
		names = append(names, st.name)
	}
	want := []string{
		"index:events_session_prompt_idx", "index:events_record_identity_idx",
		"index:events_superseded_idx", "index:messages_session_human_idx", "index:events_ingested_at_idx",
		"event_keys", "messages_kind", "titles", "turns", "rollups", "session_class", "artifacts", "links",
		"skill_invocations",
	}
	for _, ix := range deriveIndexes {
		if !strings.Contains(ix.ddl, "CREATE INDEX CONCURRENTLY IF NOT EXISTS "+ix.name+" ON ") {
			t.Errorf("%s is not built CONCURRENTLY under its own name: %s", ix.name, ix.ddl)
		}
	}
	if len(deriveIndexes) != 5 {
		t.Fatalf("%d runner-built indexes, want the contract's five", len(deriveIndexes))
	}
	if ddl := deriveIndexes[4].ddl; !strings.Contains(ddl, "events_ingested_at_idx ON events (ingested_at)") {
		t.Errorf("fifth index ddl = %s, want events_ingested_at_idx ON events (ingested_at)", ddl)
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("steps = %v\nwant    %v", names, want)
	}
	// messages_kind classifies from messages.text (classifyMessages reads no
	// body), so it belongs with the column-only steps.
	for _, st := range deriveSteps() {
		switch st.name {
		case "event_keys", "artifacts", "links", "skill_invocations":
			if !st.bodyReading {
				t.Errorf("%s reads bodies and must be gated by the window", st.name)
			}
		case "messages_kind", "titles", "turns", "rollups", "session_class":
			if st.bodyReading {
				t.Errorf("%s reads columns only and must not wait for the window", st.name)
			}
		}
		// Every step names the version that introduced it, since a zero
		// would seed it finished on a fresh database (stamp 0); the thirteen
		// version-3 steps carry 3 and the skill step alone carries 4.
		switch {
		case st.since == 0:
			t.Errorf("%s has no since; it would be seeded finished on a fresh database", st.name)
		case st.name == "skill_invocations" && st.since != 4:
			t.Errorf("skill_invocations since = %d, want 4", st.since)
		case st.name != "skill_invocations" && st.since != 3:
			t.Errorf("%s since = %d, want 3", st.name, st.since)
		}
	}
}

// derive_jobs.last_error is TEXT: a cut inside a multibyte character would
// make the failure record itself unwritable, and a parked step would turn
// into a retry loop (review-1 finding 14). The clip lands on a rune
// boundary and never grows the text.
func TestClipErrorKeepsValidUTF8(t *testing.T) {
	long := strings.Repeat("x", deriveErrorBytes-1) + "ééé" // the first e-acute straddles the cut
	got := clipError(long)
	if !utf8.ValidString(got) {
		t.Errorf("clipped error is not valid UTF-8: %q", got[len(got)-8:])
	}
	if len(got) > deriveErrorBytes {
		t.Errorf("clipped error is %d bytes, want at most %d", len(got), deriveErrorBytes)
	}
	if !strings.HasPrefix(got, strings.Repeat("x", deriveErrorBytes-1)) {
		t.Error("the clip removed more than the straddling character")
	}
	if short := "short"; clipError(short) != short {
		t.Errorf("a short error was changed to %q", clipError(short))
	}
}

// The ledger seeds a step finished when the stored version is at or past
// its since, so a bump for one new step runs that step alone: a stored 3
// seeds the thirteen version-3 steps finished and leaves skill_invocations
// pending, while a stored 0 (a fresh database) or 2 runs all fourteen.
func TestEnsureDeriveJobsSeedsFinishedBySince(t *testing.T) {
	for _, tc := range []struct{ stored, pending int }{{0, 14}, {2, 14}, {3, 1}} {
		db := &fakeDB{}
		s := NewWithDB(db, nil)
		if err := s.ensureDeriveJobs(context.Background(), DerivedSchema, tc.stored); err != nil {
			t.Fatalf("stored %d: %v", tc.stored, err)
		}
		c := db.find(t, "INSERT INTO derive_jobs (version, step, ordinal, started_at, finished_at)")
		if c.args[0] != DerivedSchema {
			t.Errorf("stored %d: seeded version %v", tc.stored, c.args[0])
		}
		names, finished := c.args[1].([]string), c.args[3].([]bool)
		if len(names) != 14 || len(finished) != 14 {
			t.Fatalf("stored %d: %d steps seeded, want 14", tc.stored, len(names))
		}
		pending := 0
		for _, f := range finished {
			if !f {
				pending++
			}
		}
		if pending != tc.pending {
			t.Errorf("stored %d: %d steps pending, want %d", tc.stored, pending, tc.pending)
		}
		if names[13] != "skill_invocations" || finished[13] {
			t.Errorf("stored %d: the last step is %s finished=%v, want skill_invocations pending", tc.stored, names[13], finished[13])
		}
	}
}
