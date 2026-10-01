package web

// The fleet page over the evaluator's view: the action list ranked worst
// first with the real verbs and runbook anchors, the mute controls, the
// manifest comparison with the serving build as its second line, the lag in
// hours, and the two columns the rollout asked for (empty starts in the last
// day, unmanaged builds).

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/health"
	"github.com/loopai-hq/loop-sessions/server/fleet"
)

// fleetPageFixture is the 788dcb3 promotion in miniature: a machine on the
// old build with a quarantine (the CTA that must lead), one on the published
// build, one on a dev build, and the evaluator's view of all three.
func fleetPageFixture() *fakeData {
	f := newFake()
	f.seedFleet()
	f.fleet.Machines[0].Report.AgentVersion = "23713ea"
	f.fleet.Machines[0].Report.Spool.Quarantine = 4
	f.fleet.Machines = append(f.fleet.Machines, Machine{
		Email: "c@example.com", Name: "C", DeviceID: "d3", ReceivedAt: fixedNow.Add(-time.Minute),
		Report: health.Report{Hostname: "c-mbp", OS: "darwin", Arch: "arm64", AgentVersion: "dev"},
	})
	f.fleet.Machines[1].Report.AgentVersion = "788dcb3"
	twentySeven, zero := 27, 0
	published := fixedNow.Add(-36 * time.Hour)
	f.eval = &fleet.Evaluation{
		At: fixedNow,
		Summary: fleet.Summary{
			Enrolled: 3, Reporting: 2, Behind: 1, Current: 1, Unmanaged: 1,
			PublishedBuild: "788dcb3", PublishedAt: published, ServerBuild: "f9228f7", ManifestPresent: true,
		},
		Machines: []fleet.Machine{
			{Email: "a@example.com", DeviceID: "d1", Hostname: "a-mbp", Build: "23713ea", Reported: true, Quarantine: 4,
				VersionState: fleet.VersionLagging, HoursBehind: 36, EmptyStarts24h: &twentySeven},
			{Email: "b@example.com", DeviceID: "d2", Hostname: "b-mbp", Build: "788dcb3", Reported: true, Silent: true,
				VersionState: fleet.VersionCurrent, EmptyStarts24h: &zero},
			{Email: "c@example.com", DeviceID: "d3", Hostname: "c-mbp", Build: "dev", Reported: true,
				VersionState: fleet.VersionUnmanaged},
		},
		// Deliberately out of rank order: the page must not trust the slice.
		CTAs: []fleet.CTA{
			{Rank: 12, Email: "a@example.com", DeviceID: "d1", Hostname: "a-mbp", Kind: "version_lag", Level: "warning",
				Detail: "on 23713ea, published build 788dcb3 is 36 h old", Since: published,
				Action:  "The daemon self-upgrades at its next check; past a day, run the upgrade by hand",
				Command: "loop-sessions daemon --upgrade-now", Anchor: "runbook-upgrade"},
			{Rank: 5, Email: "b@example.com", DeviceID: "d2", Hostname: "b-mbp", Kind: "silent", Level: "error",
				Detail: "no report for 3 d; the last one was ok", Since: fixedNow.Add(-72 * time.Hour),
				Action: "Ping the person", Command: "loop-sessions install", Anchor: "runbook-silent"},
			{Rank: 0, Email: "a@example.com", DeviceID: "d1", Hostname: "a-mbp", Kind: "quarantine_nonempty", Level: "critical",
				Detail: "4 items permanently undeliverable", Since: fixedNow.Add(-72 * time.Hour),
				Action:  "Replay one quarantined item to see the server's verdict, then redrive the rest once the server accepts it",
				Command: "loop-sessions doctor --replay-quarantine; loop-sessions doctor --redrive", Anchor: "runbook-quarantine"},
			{Rank: 11, Email: "a@example.com", DeviceID: "d1", Hostname: "a-mbp", Kind: "empty_start", Level: "warning",
				Detail: "27 of 30 sessions in 24 h ended before a first prompt", Since: fixedNow.Add(-24 * time.Hour),
				Action: "A script on this machine runs claude non-interactively; share the launcher recipe", Anchor: "runbook-empty-sessions"},
		},
		Muted: []fleet.CTA{
			{Rank: 7, Email: "c@example.com", DeviceID: "d3", Kind: "capture_stale", Level: "degraded", Detail: "hooks gone",
				Command: "loop-sessions install --hooks-only", Anchor: "runbook-hooks",
				Muted: &fleet.Mute{Email: "c@example.com", Kind: "capture_stale", Until: fixedNow.Add(48 * time.Hour), Note: "on leave", CreatedBy: "admin@example.com"}},
		},
	}
	return f
}

// errTestBoom is the store failure the degrade test injects.
var errTestBoom = errors.New("boom: the evaluator store is unreachable")

func postFleet(t *testing.T, s *Server, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// TestFleetPageRanksTheActionListWorstFirst: the quarantine row leads with
// the replay verb, whatever order the evaluator handed the rows in, and
// version lag is last; every row links its runbook anchor and names the
// person who has to run the command.
func TestFleetPageRanksTheActionListWorstFirst(t *testing.T) {
	f := fleetPageFixture()
	s := newServer(t, f, admin)
	body := get(t, s, "/admin/fleet").Body.String()

	quarantine := strings.Index(body, `cta-quarantine_nonempty`)
	silent := strings.Index(body, `cta-silent`)
	empty := strings.Index(body, `cta-empty_start`)
	lag := strings.Index(body, `cta-version_lag`)
	if quarantine < 0 || silent < 0 || empty < 0 || lag < 0 {
		t.Fatalf("a CTA row is missing: quarantine=%d silent=%d empty=%d lag=%d", quarantine, silent, empty, lag)
	}
	if quarantine >= silent || silent >= empty || empty >= lag {
		t.Errorf("rows are not worst first: quarantine=%d silent=%d empty=%d lag=%d", quarantine, silent, empty, lag)
	}
	// The list sits above the coverage table, so the operator reads it first.
	if table := strings.Index(body, `<th>Conditions</th>`); table < lag {
		t.Error("the action list is not above the coverage table")
	}
	for _, want := range []string{
		`<code>loop-sessions doctor --replay-quarantine; loop-sessions doctor --redrive</code>`,
		`<code>loop-sessions daemon --upgrade-now</code>`,
		`<code>loop-sessions install</code>`,
		`ask A to run`,
		`ask b@example.com to run`,
		`examples/deploy-gcp/README.md#runbook-quarantine`,
		`examples/deploy-gcp/README.md#runbook-upgrade`,
		`examples/deploy-gcp/README.md#runbook-silent`,
		`examples/deploy-gcp/README.md#runbook-empty-sessions`,
		`pill-critical">critical<`,
		`pill-error">error<`,
		`pill-warning">warning<`,
		`4 items permanently undeliverable`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the action list is missing %q", want)
		}
	}
	// A row with no verb (the empty-start recipe) says what to do without
	// inventing a command.
	if strings.Contains(body, "ask A to run <code></code>") {
		t.Error("a row with no command still says ask ... to run")
	}
}

// TestFleetPageShowsTheManifestAndTheLagInHours: "Latest build" is the
// manifest's commit, the serving build is the second line, a machine on the
// commit is current, another sha is behind by hours, and a dev build is
// unmanaged and counted.
func TestFleetPageShowsTheManifestAndTheLagInHours(t *testing.T) {
	f := fleetPageFixture()
	s, err := New(Options{
		Data:      f,
		Viewer:    func(*http.Request) (Viewer, bool) { return admin, true },
		CSRFKey:   []byte("test-key-for-signing-form-tokens"),
		Now:       func() time.Time { return fixedNow },
		Version:   "f9228f7",
		BuildDate: fixedNow.Add(-3 * time.Hour),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	body := get(t, s, "/admin/fleet").Body.String()
	for _, want := range []string{
		`Latest build`, `<code>788dcb3</code>`, `published ` + stamp(fixedNow.Add(-36*time.Hour)),
		`Server build`, `<code>f9228f7</code>`, `built ` + stamp(fixedNow.Add(-3*time.Hour)),
		`title="On the published build.">788dcb3<`,
		`title="Behind: the published build is 36 h newer.">23713ea<`,
		`<span class="lag">behind 36 h</span>`,
		`pill-ver-unmanaged`, `<span class="lag">unmanaged</span>`,
		`<span class="card-l">unmanaged</span>`,
		`<span class="card-l">behind a day</span>`,
		`Empty starts (24 h)`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the page is missing %q", want)
		}
	}
	// The unmanaged card counts one machine (the dev build).
	if !strings.Contains(body, `<span class="card-n">1</span><span class="card-l">unmanaged</span>`) {
		t.Error("the unmanaged card does not count the dev build")
	}
	// The empty-start column carries the client's own count, and n/a where
	// the client predates the field.
	if !strings.Contains(body, `<td class="n">27</td>`) || !strings.Contains(body, `<td class="n">0</td>`) {
		t.Error("the empty starts column does not carry the clients' counts")
	}
	if !strings.Contains(body, `>n/a</span>`) {
		t.Error("a client that predates empty_starts_24h is not marked n/a")
	}
	// The serving build no longer stands in for the manifest once one exists.
	if strings.Contains(body, "the serving build stands in for it") {
		t.Error("the strip says the serving build stands in while a manifest is published")
	}
}

// TestFleetPageFallsBackToTheServingBuildWithoutAManifest: before the first
// release is published the evaluator has nothing to judge against, and the
// page keeps the comparison it always made.
func TestFleetPageFallsBackToTheServingBuildWithoutAManifest(t *testing.T) {
	f := fleetPageFixture()
	f.eval.Summary.ManifestPresent = false
	f.eval.Summary.PublishedBuild = ""
	s, err := New(Options{
		Data:    f,
		Viewer:  func(*http.Request) (Viewer, bool) { return admin, true },
		CSRFKey: []byte("test-key-for-signing-form-tokens"),
		Now:     func() time.Time { return fixedNow },
		Version: "788dcb3",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	body := get(t, s, "/admin/fleet").Body.String()
	if !strings.Contains(body, "the serving build stands in for it") {
		t.Error("the strip does not say the serving build stands in")
	}
	if !strings.Contains(body, `title="Running the serving build.">788dcb3<`) {
		t.Error("the machine on the serving build is not badged latest")
	}
	if !strings.Contains(body, `title="Behind: the serving build is newer.">23713ea<`) {
		t.Error("the machine on the old build is not badged stale")
	}
	if strings.Contains(body, `<span class="lag">behind`) {
		t.Error("the page claims a lag in hours with no manifest to measure from")
	}
}

// TestFleetPageMutesAndUnmutes: the mute form writes a fleet_mutes row with
// until and the note, the muted list shows it with an unmute button, and
// both routes demand the form token.
func TestFleetPageMutesAndUnmutes(t *testing.T) {
	f := fleetPageFixture()
	s := newServer(t, f, admin)

	body := get(t, s, "/admin/fleet").Body.String()
	for _, want := range []string{
		`action="/admin/fleet/mute"`, `name="hours" value="72" checked`, `name="note"`,
		`1 muted`, `on leave`, `by admin@example.com`, `action="/admin/fleet/unmute"`,
		`<code>capture_stale</code>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the page is missing %q", want)
		}
	}

	rec := postFleet(t, s, "/admin/fleet/mute", url.Values{
		"csrf": {s.csrfToken(admin)}, "email": {"a@example.com"}, "kind": {"quarantine_nonempty"},
		"hours": {"72"}, "note": {"replaying after the 788dcb3 rollout"},
	})
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), "ok=muted") {
		t.Fatalf("mute answered %d %q", rec.Code, rec.Header().Get("Location"))
	}
	if len(f.muted) != 1 {
		t.Fatalf("storage received %d mutes, want 1", len(f.muted))
	}
	m := f.muted[0]
	if m.Email != "a@example.com" || m.Kind != "quarantine_nonempty" || !m.Until.Equal(fixedNow.Add(72*time.Hour)) || m.Note != "replaying after the 788dcb3 rollout" {
		t.Errorf("mute = %+v", m)
	}

	// Unknown kinds and absurd durations are refused before storage.
	for _, form := range []url.Values{
		{"csrf": {s.csrfToken(admin)}, "email": {"a@example.com"}, "kind": {"made_up"}, "hours": {"72"}},
		{"csrf": {s.csrfToken(admin)}, "email": {"a@example.com"}, "kind": {"silent"}, "hours": {"0"}},
		{"csrf": {s.csrfToken(admin)}, "email": {"a@example.com"}, "kind": {"silent"}, "hours": {"9999"}},
		{"csrf": {s.csrfToken(admin)}, "email": {""}, "kind": {"silent"}, "hours": {"24"}},
	} {
		rec := postFleet(t, s, "/admin/fleet/mute", form)
		if !strings.Contains(rec.Header().Get("Location"), "err=mute") {
			t.Errorf("form %v answered %d %q, want the mute error", form, rec.Code, rec.Header().Get("Location"))
		}
	}
	if len(f.muted) != 1 {
		t.Errorf("a refused form reached storage: %d mutes", len(f.muted))
	}

	// No token, no write.
	rec = postFleet(t, s, "/admin/fleet/mute", url.Values{"email": {"a@example.com"}, "kind": {"silent"}, "hours": {"24"}})
	if !strings.Contains(rec.Header().Get("Location"), "err=stale") || len(f.muted) != 1 {
		t.Errorf("a form with no token answered %q and left %d mutes", rec.Header().Get("Location"), len(f.muted))
	}

	rec = postFleet(t, s, "/admin/fleet/unmute", url.Values{"csrf": {s.csrfToken(admin)}, "email": {"c@example.com"}, "kind": {"capture_stale"}})
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), "ok=unmuted") {
		t.Fatalf("unmute answered %d %q", rec.Code, rec.Header().Get("Location"))
	}
	if len(f.unmuted) != 1 || f.unmuted[0] != "c@example.com/capture_stale" {
		t.Errorf("unmuted = %v", f.unmuted)
	}

	// The flash says what happened.
	if !strings.Contains(get(t, s, "/admin/fleet?ok=muted").Body.String(), "Muted.") {
		t.Error("the muted flash is missing")
	}
}

// TestFleetPageMuteRoutesAreAdminOnly: a member cannot see the page and
// cannot post to its routes either.
func TestFleetPageMuteRoutesAreAdminOnly(t *testing.T) {
	f := fleetPageFixture()
	s := newServer(t, f, owner)
	for _, path := range []string{"/admin/fleet/mute", "/admin/fleet/unmute"} {
		rec := postFleet(t, s, path, url.Values{"csrf": {s.csrfToken(owner)}, "email": {"a@example.com"}, "kind": {"silent"}, "hours": {"24"}})
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s answered %d for a member, want 404", path, rec.Code)
		}
	}
	if len(f.muted)+len(f.unmuted) != 0 {
		t.Error("a member's post reached storage")
	}
}

// TestFleetPageStandsWithoutTheEvaluator: when the evaluator's view cannot
// be read the coverage table still renders and the page says which half is
// missing, rather than answering 500 during the incident it is open for.
func TestFleetPageStandsWithoutTheEvaluator(t *testing.T) {
	f := fleetPageFixture()
	f.evalErr = errTestBoom
	s := newServer(t, f, admin)
	rec := get(t, s, "/admin/fleet")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "view could not be read, so the action list") {
		t.Error("the page does not say the evaluator's half is missing")
	}
	if strings.Contains(body, "Do first") {
		t.Error("the action list heading renders with no evaluation behind it")
	}
	if !strings.Contains(body, `<th>Conditions</th>`) || !strings.Contains(body, "a-mbp") {
		t.Error("the coverage table is gone too")
	}
	if strings.Contains(body, errTestBoom.Error()) {
		t.Error("the raw error reached the page")
	}
}

// TestFleetPageSaysWhichMachineRunsTwoBuilds is the lead's finding after
// review-1: a laptop mid-upgrade reports the old daemon's build and the new
// binary's by turns, its newest report may be the old one, and the row must
// read current on the published build while saying which build it is still
// running alongside, rather than flapping between current and behind.
func TestFleetPageSaysWhichMachineRunsTwoBuilds(t *testing.T) {
	f := fleetPageFixture()
	// Coverage (health_latest) names the old daemon; the evaluator, over the
	// last hour of reports, names the published build and the one beside it.
	f.fleet.Machines[0].Report.AgentVersion = "23713ea"
	f.eval.Machines[0].Build = "788dcb3"
	f.eval.Machines[0].AlsoRunning = "23713ea"
	f.eval.Machines[0].VersionState = fleet.VersionCurrent
	f.eval.Machines[0].HoursBehind = 0
	f.eval.Summary.Behind, f.eval.Summary.Current = 0, 2
	s, err := New(Options{
		Data:    f,
		Viewer:  func(*http.Request) (Viewer, bool) { return admin, true },
		CSRFKey: []byte("test-key-for-signing-form-tokens"),
		Now:     func() time.Time { return fixedNow },
		Version: "f9228f7",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	body := get(t, s, "/admin/fleet").Body.String()
	if !strings.Contains(body, "running 23713ea alongside 788dcb3") {
		t.Error("the row does not say which build the machine still runs alongside the published one")
	}
	if strings.Contains(body, `title="Behind: the published build is`) || strings.Contains(body, `<span class="lag">behind`) {
		t.Error("a machine that reported the published build within the hour is still shown behind")
	}
	if strings.Count(body, `title="On the published build.">788dcb3<`) != 2 {
		t.Error("the mid-upgrade row does not name the published build in its pill")
	}
	// The unmanaged and silent rows are untouched by the rule.
	if !strings.Contains(body, `pill-ver-unmanaged`) {
		t.Error("the dev build is no longer badged unmanaged")
	}
}
