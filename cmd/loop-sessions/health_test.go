package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/hooks"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// TestAHealthReportArrivesWithTheDeviceCredentialAndThisMachinesQueue is the
// shape of the thing that had never once been sent.
//
// The server's route, the authentication and the report body are all separately
// tested elsewhere; what was missing was any test that a client produces all
// three together against a server that records what it received.
func TestAHealthReportArrivesWithTheDeviceCredentialAndThisMachinesQueue(t *testing.T) {
	srv := newFleetServer(t)
	defer srv.Close()

	home := hermeticHome(t, srv.URL)
	seedSpool(t, home, "queued-1", "queued-2", "queued-3")

	if err := mustReporter(t, home).Report(context.Background()); err != nil {
		t.Fatalf("Report: %v", err)
	}

	got := srv.reports()
	if len(got) != 1 {
		t.Fatalf("server received %d health reports, want exactly 1", len(got))
	}
	r := got[0]

	if r.auth != "Bearer loops_v1_test" {
		t.Errorf("Authorization = %q, want the device credential delivery uses", r.auth)
	}
	if !strings.HasPrefix(r.agent, "loop-sessions/") {
		t.Errorf("User-Agent = %q, want the agent and its version", r.agent)
	}
	if r.contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", r.contentType)
	}
	if r.report.SchemaVersion != health.SchemaVersion {
		t.Errorf("schema_version = %d, want %d; a server cannot decode a report that does not say what it is",
			r.report.SchemaVersion, health.SchemaVersion)
	}
	if r.report.EmittedAt.IsZero() {
		t.Error("emitted_at is zero; coverage is measured from it and the store deduplicates on it")
	}
	if time.Since(r.report.EmittedAt) > time.Minute {
		t.Errorf("emitted_at is %s old on arrival; this is not a live sample", time.Since(r.report.EmittedAt))
	}
	if r.report.Spool.Pending != 3 {
		t.Errorf("spool.pending = %d, want the 3 items this machine has queued", r.report.Spool.Pending)
	}
	if r.report.OS == "" || r.report.Arch == "" || r.report.Hostname == "" {
		t.Errorf("report does not identify the machine: os=%q arch=%q hostname=%q",
			r.report.OS, r.report.Arch, r.report.Hostname)
	}
	if r.report.AgentVersion == "" {
		t.Error("agent_version is empty; a fleet cannot tell which build a condition came from")
	}
	// The disk reading is what separates capture_blocked from disk_unknown, and
	// it is the one input that comes from a syscall rather than from a file.
	if r.report.Disk.TotalBytes == 0 {
		t.Error("disk was not measured, so capture_blocked cannot be evaluated for this machine")
	}
}

// TestAReportTheServerRefusesChangesNothingOnTheMachine is the rule that keeps
// telemetry from becoming an outage.
//
// Health reporting is a second thing that can fail on the same connection as
// delivery. Every one of these outcomes must leave the spool byte-identical:
// nothing acked, nothing quarantined, and above all no delivery failure
// recorded, because `status` reads that record and would then blame uploads for
// a health endpoint being down.
func TestAReportTheServerRefusesChangesNothingOnTheMachine(t *testing.T) {
	tests := []struct {
		name   string
		status int
		// closed sends the request to a server that is not listening at all.
		closed bool
	}{
		{name: "the server is broken", status: http.StatusInternalServerError},
		{name: "the credential was revoked", status: http.StatusUnauthorized},
		{name: "the server predates the route", status: http.StatusNotFound},
		{name: "the laptop is offline", closed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFleetServer(t)
			if tc.status != 0 {
				srv.setHealthStatus(tc.status)
			}
			endpoint := srv.URL
			if tc.closed {
				srv.Close()
			} else {
				defer srv.Close()
			}

			home := hermeticHome(t, endpoint)
			seedSpool(t, home, "keep-1", "keep-2")
			before := spoolReading(t)

			err := mustReporter(t, home).Report(context.Background())
			if err == nil {
				t.Fatal("Report returned nil; a report that did not land must be reported as an error")
			}

			after := spoolReading(t)
			if after.Pending != before.Pending {
				t.Errorf("pending went from %d to %d; a health failure moved captured events",
					before.Pending, after.Pending)
			}
			if after.Quarantine != 0 {
				t.Errorf("%d item(s) quarantined by a failed health report", after.Quarantine)
			}
			if st := spoolState(t); st.LastError != "" {
				t.Errorf("the spool now blames delivery for a health failure: %q", st.LastError)
			}
			if log := readAgentLog(t, home); !strings.Contains(log, "health:") {
				t.Errorf("nothing in the agent log says the report failed:\n%s", log)
			}
		})
	}
}

// TestAHealthOutageStillDeliversEveryCapturedEvent runs both halves of the
// client against one server that accepts events and refuses every health report.
//
// This is the failure the fleet's own monitoring would otherwise cause: the
// endpoint that describes the pipeline is down, and the pipeline carries on
// exactly as if it were up.
func TestAHealthOutageStillDeliversEveryCapturedEvent(t *testing.T) {
	srv := newFleetServer(t)
	defer srv.Close()
	srv.setHealthStatus(http.StatusInternalServerError)

	home := hermeticHome(t, srv.URL)
	seedSpool(t, home, "event-1", "event-2", "event-3")

	flusher := mustDelivery(t, home)
	reporter, err := newHealthReporter(config.Paths{}, mustConfig(t), flusher.sp)
	if err != nil {
		t.Fatalf("newHealthReporter: %v", err)
	}

	// Reports either side of the delivery, because a report can fail before an
	// upload as easily as after one.
	if err := reporter.Report(context.Background()); err == nil {
		t.Fatal("the health endpoint is returning 500 and the report claimed success")
	}
	sent, err := flusher.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("delivery failed while only the health endpoint was down: %v", err)
	}
	if err := reporter.Report(context.Background()); err == nil {
		t.Fatal("the health endpoint is returning 500 and the report claimed success")
	}

	if sent != 3 {
		t.Errorf("delivered %d of 3 captured events", sent)
	}
	for _, id := range []string{"event-1", "event-2", "event-3"} {
		if n := srv.deliveries(id); n != 1 {
			t.Errorf("%s was delivered %d times, want exactly once", id, n)
		}
	}
	if r := spoolReading(t); r.Pending != 0 || r.Quarantine != 0 {
		t.Errorf("spool after the health outage: %d pending, %d quarantined; want an empty outbox",
			r.Pending, r.Quarantine)
	}
	if st := spoolState(t); st.LastError != "" || st.LastSuccess.IsZero() {
		t.Errorf("delivery history reads last_error=%q last_success=%v; a health outage rewrote it",
			st.LastError, st.LastSuccess)
	}
	// The verdict a person sees is the one that matters: a health outage must
	// not make a laptop that is uploading fine describe itself as broken.
	if v := collectStatus(config.Paths{}, time.Now()); v.State != stateUpToDate {
		t.Errorf("status says %q (%s) on a machine that delivered everything", v.State, v.Summary)
	}
}

// TestAPausedMachineReportsAChoiceRatherThanAFault: the fleet view exists partly
// to stop somebody chasing a laptop whose owner turned capture off, so the
// posture the user chose has to survive into the report.
func TestAPausedMachineReportsAChoiceRatherThanAFault(t *testing.T) {
	srv := newFleetServer(t)
	defer srv.Close()

	home := hermeticHome(t, srv.URL)
	paused := time.Now().Add(-90 * time.Minute)
	withConfig(t, func(c *config.Config) {
		c.Paused = true
		c.PausedSince = paused
	})

	if err := mustReporter(t, home).Report(context.Background()); err != nil {
		t.Fatalf("Report: %v", err)
	}
	rep := srv.reports()[0].report

	if !rep.Paused {
		t.Error("the report does not say this machine is paused")
	}
	c, ok := findCondition(rep, health.KindPaused)
	if !ok {
		t.Fatalf("no %s condition; conditions were %v", health.KindPaused, kindsOf(rep))
	}
	if c.Level != health.LevelInfo {
		t.Errorf("paused is reported at level %q; a user exercising a control we advertised is not a fault", c.Level)
	}
	if rep.Worst() == health.LevelCritical {
		t.Errorf("a machine that is merely paused reports critically: %v", kindsOf(rep))
	}
}

// TestAReportDescribesTheMachineNowRatherThanAtSessionStart: a session runs for
// hours and the decisions inside it change. Somebody pausing capture mid-session
// must show up in the fleet view as a pause, not as a laptop that quietly went
// on reporting that it was capturing.
func TestAReportDescribesTheMachineNowRatherThanAtSessionStart(t *testing.T) {
	srv := newFleetServer(t)
	defer srv.Close()

	home := hermeticHome(t, srv.URL)
	reporter := mustReporter(t, home) // built while capture is on, as a daemon builds it
	reporter.minInterval = 0          // two reports in one test, not one per five minutes

	if err := reporter.Report(context.Background()); err != nil {
		t.Fatalf("Report: %v", err)
	}
	if srv.reports()[0].report.Paused {
		t.Fatal("the first report says paused on a machine that is capturing")
	}

	withConfig(t, func(c *config.Config) {
		c.Paused = true
		c.PausedSince = time.Now()
	})
	if err := reporter.Report(context.Background()); err != nil {
		t.Fatalf("Report after the pause: %v", err)
	}

	latest := srv.reports()[1].report
	if !latest.Paused {
		t.Error("a pause during the session never reached the fleet")
	}
	if _, ok := findCondition(latest, health.KindPaused); !ok {
		t.Errorf("no paused condition after the pause; conditions were %v", kindsOf(latest))
	}
}

// TestCaptureStaleIsClaimedOnlyWhenTheHarnessConfigurationProvesIt guards the
// one condition in this report derived from a file we do not own.
//
// A machine whose hooks were rewritten away is online, healthy and recording
// nothing, which nothing else can detect. But "I could not read the harness
// settings" is not evidence of that, and a false critical on a fleet page is how
// a fleet page stops being read.
func TestCaptureStaleIsClaimedOnlyWhenTheHarnessConfigurationProvesIt(t *testing.T) {
	tests := []struct {
		name     string
		settings string // written verbatim when non-empty
		register bool   // register this binary's hooks honestly
		want     bool
	}{
		{name: "our hooks are registered", register: true, want: false},
		{name: "the settings file has no hooks of ours", settings: `{"model":"opus"}`, want: true},
		{name: "there is no settings file at all", want: true},
		{name: "the settings file cannot be parsed", settings: `{ this is not json`, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFleetServer(t)
			defer srv.Close()

			home := hermeticHome(t, srv.URL)
			// Old enough to be past health's capture-stale grace: a freshly
			// installed agent has had no opportunity to capture anything.
			withConfig(t, func(c *config.Config) { c.InstalledAt = time.Now().Add(-3 * time.Hour) })

			settings := filepath.Join(home, ".claude", "settings.json")
			switch {
			case tc.register:
				self, err := os.Executable()
				if err != nil {
					t.Skipf("this platform cannot name its own binary: %v", err)
				}
				if _, err := hooks.Register(hooks.Options{SettingsPath: settings, Binary: self}); err != nil {
					t.Fatalf("register hooks: %v", err)
				}
			case tc.settings != "":
				writeFile(t, settings, tc.settings)
			}

			if err := mustReporter(t, home).Report(context.Background()); err != nil {
				t.Fatalf("Report: %v", err)
			}
			rep := srv.reports()[0].report

			_, got := findCondition(rep, health.KindCaptureStale)
			if got != tc.want {
				t.Fatalf("capture_stale present = %v, want %v; conditions were %v", got, tc.want, kindsOf(rep))
			}
		})
	}
}

// ---------------------------------------------------------------- fixtures

// fleetServer is the server half reduced to what a client can observe: it
// records every health report and counts every event delivery, and its health
// route answers whatever the test tells it to.
type fleetServer struct {
	*httptest.Server

	mu           sync.Mutex
	got          []recordedReport
	delivered    map[string]int
	healthStatus int
	// verdict, when set, is returned verbatim for every events batch instead
	// of accepting everything.
	verdict string
}

type recordedReport struct {
	auth        string
	agent       string
	contentType string
	report      health.Report
}

func newFleetServer(t *testing.T) *fleetServer {
	t.Helper()
	s := &fleetServer{delivered: map[string]int{}, healthStatus: http.StatusAccepted}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case healthPath:
			s.takeReport(w, r)
		case eventsPath:
			s.takeEvents(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	return s
}

func (s *fleetServer) takeReport(w http.ResponseWriter, r *http.Request) {
	var rep health.Report
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		http.Error(w, "malformed report", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	status := s.healthStatus
	s.got = append(s.got, recordedReport{
		auth:        r.Header.Get("Authorization"),
		agent:       r.Header.Get("User-Agent"),
		contentType: r.Header.Get("Content-Type"),
		report:      rep,
	})
	s.mu.Unlock()
	w.WriteHeader(status)
}

func (s *fleetServer) takeEvents(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") == "" {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	var items []spool.Item
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		http.Error(w, "malformed", http.StatusBadRequest)
		return
	}
	out := struct {
		Accepted []string `json:"accepted"`
	}{Accepted: []string{}}
	s.mu.Lock()
	verdict := s.verdict
	for _, it := range items {
		s.delivered[it.ID]++
		out.Accepted = append(out.Accepted, it.ID)
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if verdict != "" {
		_, _ = w.Write([]byte(verdict))
		return
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *fleetServer) setHealthStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthStatus = code
}

func (s *fleetServer) reports() []recordedReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedReport(nil), s.got...)
}

func (s *fleetServer) deliveries(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delivered[id]
}

// hermeticHome is an enrolled machine whose harness settings and session files
// are its own. Without the HOME override, discovery would walk the developer's
// real session directories and the hook check would read their real settings
// file, which is both slow and a test that depends on the machine running it.
func hermeticHome(t *testing.T, endpoint string) string {
	t.Helper()
	home := enrolledHome(t, endpoint)
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, ".claude"))
	return home
}

func mustConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Paths{})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

// mustReporter builds the reporter over the delivery path's own spool handle,
// which is how the daemon builds it.
func mustReporter(t *testing.T, home string) *healthReporter {
	t.Helper()
	r, err := newHealthReporter(config.Paths{}, mustConfig(t), mustDelivery(t, home).sp)
	if err != nil {
		t.Fatalf("newHealthReporter: %v", err)
	}
	return r
}

func withConfig(t *testing.T, mutate func(*config.Config)) {
	t.Helper()
	cfg := mustConfig(t)
	mutate(&cfg)
	if err := config.Save(config.Paths{}, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
}

func spoolReading(t *testing.T) spool.Stats {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: config.Paths{}.SpoolDir()})
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	st, err := sp.Stats()
	if err != nil {
		t.Fatalf("spool stats: %v", err)
	}
	return st
}

func spoolState(t *testing.T) spool.State {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: config.Paths{}.SpoolDir()})
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	st, err := sp.State()
	if err != nil {
		t.Fatalf("spool state: %v", err)
	}
	return st
}

func findCondition(r health.Report, kind string) (health.Condition, bool) {
	for _, c := range r.Conditions {
		if c.Kind == kind {
			return c, true
		}
	}
	return health.Condition{}, false
}

func kindsOf(r health.Report) []string {
	out := make([]string, 0, len(r.Conditions))
	for _, c := range r.Conditions {
		out = append(out, c.Kind)
	}
	return out
}
