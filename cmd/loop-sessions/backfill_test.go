package main

// These tests defend the property that was missing, not the parser that was
// already correct: internal/backfill has been able to read a transcript since
// before the first install, and every one of its own tests passed while the
// product never called it once. So what is asserted here is composition —
// install imports, the import uploads, and a second run does not send the same
// history again — with the parser treated as a dependency that already works.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/discovery"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// ---------------------------------------------------------------------------
// Composition: the defect itself
// ---------------------------------------------------------------------------

// TestInstallImportsTheHistoryAlreadyOnTheMachine is the test that would have
// caught it. Nothing in cmd/loop-sessions referenced internal/backfill, so a
// person who installed the agent got their future sessions and none of their
// past ones, and the agent said nothing about it.
func TestInstallImportsTheHistoryAlreadyOnTheMachine(t *testing.T) {
	srv := newHistoryServer(t)
	defer srv.Close()

	machine := newMachine(t, srv.URL)
	machine.transcript("proj-a", sessionOne,
		userRecord(sessionOne, recentOne, "u1", "/repo", "make the thing"),
		assistantRecord(sessionOne, recentTwo, "a1", "/repo", "done"),
	)

	// -skip-hooks keeps this test off the settings file, which is another
	// agent's territory; the import is what is under test.
	if err := runInstall([]string{"-endpoint", srv.URL, "-skip-hooks"}); err != nil {
		t.Fatalf("install: %v", err)
	}

	if got := srv.count(); got == 0 {
		t.Fatal("install finished having uploaded nothing; the history on this machine was never imported")
	}
	if n := pendingCount(t, machine.agentHome); n != 0 {
		t.Errorf("%d item(s) are still queued after install; the import must not report success with a backlog", n)
	}
	// Discovery's result has to be on disk, because that is what the import
	// walks and what `loop-sessions backfill` reads later.
	if _, err := os.Stat(config.Paths{}.DiscoveryFile()); err != nil {
		t.Errorf("install did not record what it found: %v", err)
	}
	if n := len(journalOf(t)); n == 0 {
		t.Error("install imported history without recording what it sent; a second run would send it all again")
	}
}

// TestImportedEventsCarryTheirOriginalTimestamp is the property the whole
// exercise is for. An import stamped with import time answers "what did we
// collect tonight" instead of "what happened", and there is no reason to import
// history that lies about when it happened.
//
// It is asserted on the wire rather than in the walker because the place it can
// be lost is the three-line adapter between them: the spool stamps now() over a
// zero EventTime, so an import that queued events itself instead of going
// through the sink would date the entire corpus to the moment it ran.
func TestImportedEventsCarryTheirOriginalTimestamp(t *testing.T) {
	srv := newHistoryServer(t)
	defer srv.Close()

	machine := newMachine(t, srv.URL)
	machine.transcript("proj-a", sessionOne,
		userRecord(sessionOne, recentOne, "u1", "/repo", "hello"),
	)
	machine.discovered()

	if err := importAtInstall(config.Paths{}, machine.config(t), "all", io.Discard); err != nil {
		t.Fatalf("import: %v", err)
	}

	want := mustParse(t, recentOne)
	items := srv.items()
	if len(items) == 0 {
		t.Fatal("nothing was uploaded")
	}
	for _, it := range items {
		if !it.EventTime.Equal(want) {
			t.Errorf("event %s arrived stamped %s, want the transcript's own %s",
				it.ID, it.EventTime.Format(time.RFC3339), want.Format(time.RFC3339))
		}
	}
}

// ---------------------------------------------------------------------------
// Resume
// ---------------------------------------------------------------------------

// A 2.8 GB corpus does not import in one sitting on a laptop that gets closed.
// The two halves of resumability are asserted together because either alone is
// useless: not starting over, and not sending again what was already sent.
func TestAnInterruptedImportResumesWithoutSendingAnythingTwice(t *testing.T) {
	srv := newHistoryServer(t)
	defer srv.Close()

	machine := newMachine(t, srv.URL)
	machine.transcript("proj-a", sessionOne,
		userRecord(sessionOne, recentOne, "u1", "/repo", "first session"),
	)
	machine.transcript("proj-b", sessionTwo,
		userRecord(sessionTwo, recentTwo, "u1", "/repo", "second session"),
	)
	machine.discovered()

	cfg := machine.config(t)
	if err := importAtInstall(config.Paths{}, cfg, "all", io.Discard); err != nil {
		t.Fatalf("first import: %v", err)
	}
	first := srv.items()
	if len(first) < 4 {
		t.Fatalf("first import delivered %d event(s), want both sessions", len(first))
	}

	cases := []struct {
		name  string
		prep  func()
		wantN int
	}{
		{
			name:  "a second run of a finished import sends nothing at all",
			prep:  func() {},
			wantN: 0,
		},
		{
			name: "a run interrupted after the first file sends only the rest",
			prep: func() {
				// What an interruption leaves behind: one file recorded, the
				// other not.
				machine.keepJournalFor(t, filepath.Join(machine.claudeRoot(), "proj-a", sessionOne+".jsonl"))
				srv.reset()
			},
			wantN: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv.reset()
			tc.prep()

			if err := importAtInstall(config.Paths{}, cfg, "all", io.Discard); err != nil {
				t.Fatalf("resumed import: %v", err)
			}
			got := srv.items()
			if len(got) != tc.wantN {
				t.Fatalf("resumed import uploaded %d event(s), want %d", len(got), tc.wantN)
			}
			// Whatever it did send must be identical to what the uninterrupted
			// run sent, or the server stores a second copy instead of dedup'ing.
			for _, it := range got {
				if !firstHas(first, it.ID) {
					t.Errorf("event %s was not in the complete import; a resume renumbered the stream", it.ID)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Pacing
// ---------------------------------------------------------------------------

// An import must not outrun delivery, because the spool makes room by deleting
// its OLDEST pending items. A walk that only wrote would therefore delete the
// history it had just written — and the live session's events queued beside it —
// and would report success while doing it. So the queue depth is bounded by
// construction, and this measures it from outside as the import runs.
func TestTheImportKeepsTheOutboxSmallerThanItsHighWaterMark(t *testing.T) {
	var (
		mu      sync.Mutex
		deepest int
	)
	machine := newMachineLater(t)
	srv := newHistoryServer(t)
	srv.observe(func() {
		// Sampled while the walk is blocked on this request, which is the
		// moment the queue is at its deepest. Counted without the testing
		// helpers, because this runs on the server's goroutine.
		n := pendingNow(config.Paths{}.SpoolDir())
		mu.Lock()
		if n > deepest {
			deepest = n
		}
		mu.Unlock()
	})
	defer srv.Close()
	machine.enrol(t, srv.URL)

	// Comfortably more than one high-water mark's worth, so the pacing has to
	// engage several times rather than once at the end.
	const records = 1200
	lines := make([]string, 0, records)
	for i := range records {
		lines = append(lines, userRecord(sessionOne, ago(time.Duration(records-i)*time.Minute),
			fmt.Sprintf("u%d", i), "/repo", fmt.Sprintf("turn %d", i)))
	}
	machine.transcript("proj-a", sessionOne, lines...)
	machine.discovered()

	if err := importAtInstall(config.Paths{}, machine.config(t), "all", io.Discard); err != nil {
		t.Fatalf("import: %v", err)
	}

	if got := srv.count(); got < records {
		t.Fatalf("delivered %d event(s) of %d; the import lost some", got, records)
	}
	mu.Lock()
	defer mu.Unlock()
	if limit := outboxHighItems + paceEvery; deepest > limit {
		t.Errorf("the outbox reached %d item(s); the import must keep it under %d or the spool starts dropping the oldest",
			deepest, limit)
	}
	if deepest == 0 {
		t.Error("the queue was never observed with anything in it; this test measured nothing")
	}
}

// ---------------------------------------------------------------------------
// Saying so
// ---------------------------------------------------------------------------

// An import that cannot upload must stop and say why. The alternative is what
// this project already shipped once: a queue that grows in silence, and a person
// who believes their history was collected.
//
// It stops rather than carrying on into the queue because the spool drops its
// OLDEST items at its byte cap — so an import that outran delivery would delete
// the history it had just written, and the live session's events with it.
func TestTheImportStopsAndSaysSoWhenNothingUploads(t *testing.T) {
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no", http.StatusInternalServerError)
	}))
	defer refusing.Close()

	machine := newMachine(t, refusing.URL)
	machine.transcript("proj-a", sessionOne,
		userRecord(sessionOne, recentOne, "u1", "/repo", "hello"),
	)
	machine.discovered()

	var out strings.Builder
	err := importAtInstall(config.Paths{}, machine.config(t), "all", &out)
	if err == nil {
		t.Fatal("the import reported success against a server that accepted nothing")
	}
	if !strings.Contains(err.Error(), "still queued") {
		t.Errorf("the failure does not say what is stuck: %v", err)
	}
	if strings.Contains(out.String(), "reached the server") {
		t.Errorf("the import claimed delivery it did not get:\n%s", out.String())
	}
	// The events are queued, not lost: the next daemon delivers them.
	if n := pendingCount(t, machine.agentHome); n == 0 {
		t.Error("the import discarded what it could not upload")
	}
	if log := readAgentLog(t, machine.agentHome); !strings.Contains(log, "backfill:") {
		t.Errorf("the agent log says nothing about the import failing:\n%s", log)
	}
}

// ---------------------------------------------------------------------------
// Scope
// ---------------------------------------------------------------------------

// The controls a person was promised apply to their past as well as their
// future. Somebody who excluded a directory did not mean "except for everything
// that happened in it before you arrived", and a paused agent is paused.
func TestTheImportHonoursWhatThePersonExcluded(t *testing.T) {
	cases := []struct {
		name    string
		adjust  func(*config.Config)
		wantErr string
		wantIDs int
	}{
		{
			name:    "an excluded project's history stays on the machine",
			adjust:  func(c *config.Config) { c.ExcludePaths = []string{"/private"} },
			wantIDs: 2, // the /repo session only
		},
		{
			name:    "an allowlist keeps everything outside it out",
			adjust:  func(c *config.Config) { c.CaptureOnlyPaths = []string{"/repo"} },
			wantIDs: 2,
		},
		{
			name:    "a paused agent imports nothing and says why",
			adjust:  func(c *config.Config) { *c = c.Pause(time.Now()) },
			wantErr: "paused",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newHistoryServer(t)
			defer srv.Close()

			machine := newMachine(t, srv.URL)
			machine.transcript("proj-a", sessionOne,
				userRecord(sessionOne, recentOne, "u1", "/repo", "work"),
			)
			machine.transcript("proj-b", sessionTwo,
				userRecord(sessionTwo, recentTwo, "u1", "/private/diary", "not work"),
			)
			machine.discovered()

			cfg := machine.config(t)
			tc.adjust(&cfg)

			err := importAtInstall(config.Paths{}, cfg, "all", io.Discard)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one mentioning %q", err, tc.wantErr)
				}
				if n := srv.count(); n != 0 {
					t.Errorf("%d event(s) were uploaded anyway", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("import: %v", err)
			}
			if n := srv.count(); n != tc.wantIDs {
				t.Fatalf("uploaded %d event(s), want %d", n, tc.wantIDs)
			}
			for _, it := range srv.items() {
				if strings.Contains(string(it.Payload), "/private") {
					t.Errorf("an excluded project's session was uploaded: %s", it.ID)
				}
			}
		})
	}
}

// The default install imports the whole corpus, however old.
//
// This is the product decision the default encodes, so it is pinned here rather
// than left to the constant: Claude Code deletes local transcripts after 30
// days, which makes what is on disk at install the only copy that will ever
// exist. A default that silently skipped the old ones would lose them, and
// would do it invisibly — the person would simply never see that history on
// their dashboard and have no reason to suspect it was ever there.
func TestTheDefaultInstallImportsEverythingHoweverOld(t *testing.T) {
	srv := newHistoryServer(t)
	defer srv.Close()

	machine := newMachine(t, srv.URL)
	machine.transcript("proj-old", sessionOne,
		userRecord(sessionOne, "2025-01-05T09:00:00.000Z", "u1", "/repo", "last year"),
	)
	machine.transcript("proj-new", sessionTwo,
		userRecord(sessionTwo, recentOne, "u1", "/repo", "this month"),
	)
	machine.discovered()

	var out strings.Builder
	if err := importAtInstall(config.Paths{}, machine.config(t), defaultInstallWindow, &out); err != nil {
		t.Fatalf("import: %v", err)
	}

	var sawOld bool
	for _, it := range srv.items() {
		if strings.Contains(string(it.Payload), "last year") {
			sawOld = true
		}
	}
	if !sawOld {
		t.Error("the default install skipped a year-old session; on-disk history is the only copy there is")
	}
	// The "older sessions were not imported" advisory is true only of a bounded
	// run. Printing it after importing everything would send people to run a
	// second import that has nothing left to do.
	if strings.Contains(out.String(), "Older sessions were not imported") {
		t.Errorf("the default install claims it left history behind:\n%s", out.String())
	}
}

// A bounded window must still be bounded, and must still say what it left.
// --backfill-since is the way out for somebody on a metered connection or a
// laptop with years on it, so the behaviour survives the default changing.
func TestABoundedInstallWindowLeavesOlderSessionsForLater(t *testing.T) {
	srv := newHistoryServer(t)
	defer srv.Close()

	machine := newMachine(t, srv.URL)
	machine.transcript("proj-old", sessionOne,
		userRecord(sessionOne, "2025-01-05T09:00:00.000Z", "u1", "/repo", "last year"),
	)
	machine.transcript("proj-new", sessionTwo,
		userRecord(sessionTwo, recentOne, "u1", "/repo", "this month"),
	)
	machine.discovered()

	var out strings.Builder
	if err := importAtInstall(config.Paths{}, machine.config(t), "7d", &out); err != nil {
		t.Fatalf("import: %v", err)
	}

	for _, it := range srv.items() {
		if strings.Contains(string(it.Payload), "last year") {
			t.Errorf("a session outside the install window was imported: %s", it.ID)
		}
	}
	if srv.count() == 0 {
		t.Fatal("the install window imported nothing at all")
	}
	if !strings.Contains(out.String(), "backfill --since all") {
		t.Errorf("the person is not told how to import the rest:\n%s", out.String())
	}
}

// A dry run is what somebody on a tethered connection runs first, so it must not
// touch the network, the queue, or the resume record.
func TestADryRunReportsWithoutSendingAnything(t *testing.T) {
	srv := newHistoryServer(t)
	defer srv.Close()

	machine := newMachine(t, srv.URL)
	machine.transcript("proj-a", sessionOne,
		userRecord(sessionOne, recentOne, "u1", "/repo", "hello"),
	)
	machine.discovered()

	var out strings.Builder
	err := importHistory(context.Background(), config.Paths{}, machine.config(t),
		request{Out: &out, DryRun: true})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if n := srv.count(); n != 0 {
		t.Errorf("a dry run uploaded %d event(s)", n)
	}
	if n := pendingCount(t, machine.agentHome); n != 0 {
		t.Errorf("a dry run queued %d item(s)", n)
	}
	if n := len(journalOf(t)); n != 0 {
		t.Errorf("a dry run wrote %d resume record(s); the next real import would skip those files", n)
	}
	if !strings.Contains(out.String(), "Would import") {
		t.Errorf("a dry run did not report what it would do:\n%s", out.String())
	}
}

func TestBackfillImportsClaudeAndCodexHistory(t *testing.T) {
	srv := newHistoryServer(t)
	defer srv.Close()

	machine := newMachine(t, srv.URL)
	machine.transcript("proj-a", sessionOne,
		userRecord(sessionOne, recentOne, "u1", "/repo", "from claude"),
	)
	codexRoot := filepath.Join(machine.home, ".codex", "sessions")
	writeFile(t, filepath.Join(codexRoot, "2026", "08", "06", "rollout.jsonl"), strings.Join([]string{
		`{"timestamp":"2026-08-05T10:00:00Z","type":"session_meta","payload":{"id":"codex-session","cwd":"/repo","source":"cli"}}`,
		`{"timestamp":"2026-08-05T10:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"from codex"}]}}`,
	}, "\n")+"\n")
	_ = discovery.WriteSummary(config.Paths{}.DiscoveryFile(), discovery.Summary{Findings: []discovery.Finding{
		{Tool: discovery.ClaudeCode, State: discovery.Found, Root: machine.claudeRoot(), Sessions: 1},
		{Tool: discovery.Codex, State: discovery.Found, Root: codexRoot, Sessions: 1},
	}})

	if err := importHistory(context.Background(), config.Paths{}, machine.config(t), request{Out: io.Discard}); err != nil {
		t.Fatalf("import: %v", err)
	}
	var got string
	for _, item := range srv.items() {
		got += string(item.Payload)
	}
	if !strings.Contains(got, "from claude") || !strings.Contains(got, "from codex") {
		t.Fatalf("both sources were not imported: %s", got)
	}
}

// ---------------------------------------------------------------------------
// Flags and formatting
// ---------------------------------------------------------------------------

func TestParseSince(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		in      string
		want    time.Time
		wantErr bool
	}{
		{name: "empty means the whole corpus", in: "", want: time.Time{}},
		{name: "all means the whole corpus", in: "all", want: time.Time{}},
		{name: "days is how people say a month", in: "30d", want: now.AddDate(0, 0, -30)},
		{name: "a go duration is accepted too", in: "48h", want: now.Add(-48 * time.Hour)},
		{name: "a date answers since when I joined", in: "2026-07-01", want: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)},
		{name: "nonsense is refused rather than guessed at", in: "last tuesday", wantErr: true},
		{name: "a negative window is refused", in: "-5d", wantErr: true},
		{name: "a bare unit is refused", in: "d", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSince(tc.in, now)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseSince(%q) = %s, want an error", tc.in, got)
				}
				if !strings.Contains(err.Error(), "30d") {
					t.Errorf("the error does not say what is accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSince(%q): %v", tc.in, err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("parseSince(%q) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

// A machine nobody has scanned and a machine with no history need opposite
// answers. Conflating them either sends somebody to fix a machine that is fine,
// or leaves a machine that needs `discover` silently importing nothing.
func TestAMachineWithNothingToImportIsToldWhichKindItIs(t *testing.T) {
	cases := []struct {
		name    string
		scan    func(*machine)
		wantErr string
		wantOut string
	}{
		{
			name:    "never scanned, so name the command that scans",
			scan:    func(*machine) {},
			wantErr: "loop-sessions discover",
		},
		{
			name: "scanned and empty, which is not a fault",
			scan: func(m *machine) {
				sum := discovery.Summary{Findings: []discovery.Finding{{
					Tool: discovery.ClaudeCode, State: discovery.Absent,
				}}}
				_ = discovery.WriteSummary(config.Paths{}.DiscoveryFile(), sum)
			},
			wantOut: "no history to import",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			machine := newMachine(t, "https://example.invalid")
			tc.scan(machine)

			var out strings.Builder
			err := importAtInstall(config.Paths{}, machine.config(t), "all", &out)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatal("the import claimed to work with nowhere to read from")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("the error does not name the fix: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("an empty machine reported a failure: %v", err)
			}
			if !strings.Contains(out.String(), tc.wantOut) {
				t.Errorf("output does not say there is nothing to import:\n%s", out.String())
			}
		})
	}
}

func TestHumanCount(t *testing.T) {
	cases := []struct {
		in   int
		want string
	}{
		{0, "0"},
		{999, "999"},
		{6196, "6,196"},
		{1000000, "1,000,000"},
		{-1, "-1"},
	}
	for _, tc := range cases {
		if got := humanCount(tc.in); got != tc.want {
			t.Errorf("humanCount(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const (
	sessionOne = "11111111-2222-3333-4444-555555555555"
	sessionTwo = "66666666-2222-3333-4444-555555555555"
)

// Relative to now, not fixed, because the install window is relative to now:
// hard-coded dates would quietly fall out of it and turn "the window works" into
// "the window imports nothing". Far enough in the past that an event stamped
// with import time instead of its own is unmistakable.
var (
	recentOne = ago(30 * time.Hour)
	recentTwo = ago(29 * time.Hour)
)

func ago(d time.Duration) string {
	return time.Now().UTC().Add(-d).Truncate(time.Second).Format(time.RFC3339)
}

// machine is a laptop: a home directory with Claude Code history in it, and an
// enrolled agent beside it.
type machine struct {
	t         *testing.T
	home      string
	agentHome string
}

// newMachine builds one and points this process at it. HOME is redirected
// because discovery probes the real one, and an install test that scanned the
// developer's own laptop would import their history into a test server.
func newMachine(t *testing.T, endpoint string) *machine {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	agentHome := enrolledHome(t, endpoint)
	return &machine{t: t, home: home, agentHome: agentHome}
}

// newMachineLater builds the laptop without enrolling it, for tests that need
// the machine to exist before the server it will talk to.
func newMachineLater(t *testing.T) *machine {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	return &machine{t: t, home: home}
}

func (m *machine) enrol(t *testing.T, endpoint string) {
	t.Helper()
	m.agentHome = enrolledHome(t, endpoint)
}

func (m *machine) claudeRoot() string { return filepath.Join(m.home, ".claude", "projects") }

// transcript writes one session file where Claude Code would have left it.
func (m *machine) transcript(project, session string, lines ...string) string {
	m.t.Helper()
	path := filepath.Join(m.claudeRoot(), project, session+".jsonl")
	writeFile(m.t, path, strings.Join(lines, "\n")+"\n")
	return path
}

// discovered writes the discovery result install would have left, for the tests
// that call the import directly instead of going through install.
func (m *machine) discovered() {
	sum := discovery.Summary{
		Findings: []discovery.Finding{{
			Tool:  discovery.ClaudeCode,
			State: discovery.Found,
			Root:  m.claudeRoot(),
		}},
	}
	_ = discovery.WriteSummary(config.Paths{}.DiscoveryFile(), sum)
}

func (m *machine) config(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Paths{})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

// keepJournalFor rewrites the resume record to name exactly one file, which is
// what an import interrupted after that file leaves behind.
func (m *machine) keepJournalFor(t *testing.T, path string) {
	t.Helper()
	var keep string
	for _, line := range journalOf(t) {
		if strings.Contains(line, path) {
			keep = line
		}
	}
	if keep == "" {
		t.Fatalf("no resume record for %s", path)
	}
	writeFile(t, filepath.Join(config.Paths{}.Root(), journalName), keep+"\n")
}

// journalOf returns the resume record's lines.
func journalOf(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(config.Paths{}.Root(), journalName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read the resume record: %v", err)
	}
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

func userRecord(session, ts, uuid, cwd, text string) string {
	return fmt.Sprintf(`{"type":"user","sessionId":%q,"timestamp":%q,"uuid":%q,"cwd":%q,"gitBranch":"main","version":"2.1.221","message":{"role":"user","content":%q}}`,
		session, ts, uuid, cwd, text)
}

func assistantRecord(session, ts, uuid, cwd, text string) string {
	return fmt.Sprintf(`{"type":"assistant","sessionId":%q,"timestamp":%q,"uuid":%q,"cwd":%q,"message":{"role":"assistant","model":"claude-fable-5","id":"msg_1","content":[{"type":"text","text":%q}]}}`,
		session, ts, uuid, cwd, text)
}

func mustParse(t *testing.T, ts string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatalf("bad fixture time %q: %v", ts, err)
	}
	return v.UTC()
}

// pendingNow counts queued items without a *testing.T, so it is safe to call
// from a handler goroutine.
func pendingNow(spoolDir string) int {
	ents, err := os.ReadDir(filepath.Join(spoolDir, "pending"))
	if err != nil {
		return 0
	}
	var n int
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			n++
		}
	}
	return n
}

func firstHas(items []spool.Item, id string) bool {
	for _, it := range items {
		if it.ID == id {
			return true
		}
	}
	return false
}

// historyServer keeps what it was sent, which is what the assertions are about:
// the ids, so a resend is visible, and the event times, so a lie about when
// something happened is visible.
type historyServer struct {
	*httptest.Server

	mu    sync.Mutex
	sent  []spool.Item
	watch func()
}

// observe registers a callback run inside each request, for assertions about
// what the client looks like mid-import rather than after it.
func (s *historyServer) observe(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watch = f
}

func newHistoryServer(t *testing.T) *historyServer {
	t.Helper()
	s := &historyServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != eventsPath {
			http.NotFound(w, r)
			return
		}
		var batch []spool.Item
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, "malformed", http.StatusBadRequest)
			return
		}
		out := struct {
			Accepted []string `json:"accepted"`
		}{Accepted: []string{}}

		s.mu.Lock()
		watch := s.watch
		s.mu.Unlock()
		if watch != nil {
			watch()
		}

		s.mu.Lock()
		for _, it := range batch {
			s.sent = append(s.sent, it)
			out.Accepted = append(out.Accepted, it.ID)
		}
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	return s
}

// items is a copy, so an assertion cannot race the handler.
func (s *historyServer) items() []spool.Item {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]spool.Item(nil), s.sent...)
}

func (s *historyServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func (s *historyServer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = nil
}
