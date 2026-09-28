package web

// The fleet page's evaluator half: the CTA list ranked worst first, the mute
// controls, and the version judgement against the release manifest. The
// coverage table in admin.go predates the evaluator and stays; this file puts
// the evaluator's answer above it, from the same computation the alerts log
// from, so the page and the pager never disagree about what to do first.

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/loopai-hq/agent-sessions/server/fleet"
)

// runbookBase is where the CTA runbook anchors resolve. The deploy README
// holds them and the alert policies link there the same way, so a CTA row
// and the alert that fired for it land the operator on one heading.
const runbookBase = "https://github.com/loopai-hq/agent-sessions/blob/main/examples/deploy-gcp/README.md#"

// muteNoteMax bounds the note; a mute is a sentence, not a postmortem.
const muteNoteMax = 200

// muteMaxHours is the longest mute the form accepts: a quarter, past which
// the alert should be fixed or deleted rather than silenced.
const muteMaxHours = 24 * 90

// muteChoice is one duration the mute form offers.
type muteChoice struct {
	Hours int
	Label string
}

// muteChoices are the four durations offered. Three days is the default
// because it covers a long weekend, which is what most mutes are for.
var muteChoices = []muteChoice{{24, "1 day"}, {72, "3 days"}, {168, "1 week"}, {720, "30 days"}}

// ctaRow is one CTA as the page shows it: the evaluator's row plus the
// person's name and the resolved runbook link.
type ctaRow struct {
	fleet.CTA
	Person  string
	Runbook string
}

// Ask is the sentence for a command that runs on the laptop. The verb is
// real, and the operator cannot run it from here: the person has to. Empty
// for a row with no command and for a fleet-wide row, which names nobody;
// its command stands alone beside an action that already says who.
func (c ctaRow) Ask() string {
	if c.Command == "" || c.FleetWide() {
		return ""
	}
	return "ask " + c.Person + " to run"
}

// FleetWide reports a row that is the fleet's rather than a person's: the
// missing-answer rate, which no one machine causes.
func (c ctaRow) FleetWide() bool { return c.Email == "" }

// MuteEmail is the person the row's mute form posts: the row's, or the
// evaluator's sentinel for a fleet-wide row, which has none to post.
func (c ctaRow) MuteEmail() string {
	if c.FleetWide() {
		return fleet.Everyone
	}
	return c.Email
}

// LevelClass maps the evaluator's levels onto the page's pill classes: the
// health report's own levels have pills already, and the evaluator's two
// extra ones (error, warning) get their own.
func (c ctaRow) LevelClass() string {
	switch c.Level {
	case "critical", "degraded", "info", "error", "warning":
		return c.Level
	default:
		return "info"
	}
}

// ctaRows shapes and orders the evaluator's CTAs for the page. The evaluator
// already ranks them; the page sorts again so an evaluator bug can never
// put version lag above a quarantine row (rank, then level, then person).
func ctaRows(ctas []fleet.CTA, names map[string]string) []ctaRow {
	out := make([]ctaRow, 0, len(ctas))
	for _, c := range ctas {
		person := names[c.Email]
		if person == "" {
			person = c.Email
		}
		if person == "" {
			// A fleet-wide row: the column says whose it is.
			person = "fleet"
		}
		out = append(out, ctaRow{CTA: c, Person: person, Runbook: runbookBase + c.Anchor})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Rank != out[j].Rank {
			return out[i].Rank < out[j].Rank
		}
		if li, lj := levelRank(out[i].Level), levelRank(out[j].Level); li != lj {
			return li > lj
		}
		return out[i].Email < out[j].Email
	})
	return out
}

// levelRank orders the evaluator's levels, worst first.
func levelRank(level string) int {
	switch level {
	case "critical":
		return 4
	case "error":
		return 3
	case "degraded":
		return 2
	case "warning":
		return 1
	default:
		return 0
	}
}

// versionFromEvaluation turns the evaluator's version state into the page's
// badge and its hover text. The badge names are the ones the coverage table
// has always used (latest, stale, unknown) plus unmanaged, so the CSS and
// the older tests keep meaning what they meant.
func versionFromEvaluation(m *fleet.Machine) (badge, title string) {
	switch m.VersionState {
	case fleet.VersionCurrent:
		return "latest", "On the published build."
	case fleet.VersionLagging:
		return "stale", fmt.Sprintf("Behind: the published build is %s newer.", hoursLabel(m.HoursBehind))
	case fleet.VersionUnmanaged:
		return "unmanaged", "Unmanaged: not a release sha (a dev or dirty build), so never judged against the manifest."
	default:
		return "unknown", "No health report yet, so the version is unknown."
	}
}

// versionTitle is the hover text for the legacy comparison against the
// serving build, used while no release manifest is published.
func versionTitle(badge string) string {
	switch badge {
	case "stale":
		return "Behind: the serving build is newer."
	case "latest":
		return "Running the serving build."
	default:
		return "No health report yet, so the version is unknown."
	}
}

// hoursLabel says a lag in whole hours, or days past two of them; the
// alert threshold is a day and the page should read in the same unit.
func hoursLabel(hours float64) string {
	h := int(hours + 0.5)
	if h >= 48 {
		return fmt.Sprintf("%d d %d h", h/24, h%24)
	}
	return fmt.Sprintf("%d h", h)
}

// fleetMessage maps the redirect codes the mute routes set onto the flash.
func fleetMessage(q url.Values) (notice, problem string) {
	switch q.Get("ok") {
	case "muted":
		notice = "Muted. The alert stays quiet until the mute lapses; the row is listed under muted below."
	case "unmuted":
		notice = "Unmuted. The next evaluation alerts again."
	}
	switch q.Get("err") {
	case "stale":
		problem = "That form had been open too long. Try again."
	case "mute":
		problem = "The mute was not saved: it needs a person, a known alert kind and a duration of 1 to 2160 hours."
	}
	return notice, problem
}

// handleFleetMute creates or replaces one (person, kind) mute.
func (s *Server) handleFleetMute(w http.ResponseWriter, r *http.Request) {
	v, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	if err := s.checkWrite(r); err != nil {
		http.Error(w, "request rejected", http.StatusForbidden)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "malformed form", http.StatusBadRequest)
		return
	}
	if !s.csrfValid(v, r.PostFormValue("csrf")) {
		http.Redirect(w, r, "/admin/fleet?err=stale", http.StatusSeeOther)
		return
	}
	// The person is an address or the evaluator's sentinel (fleet.Everyone),
	// which mutes the kind for the fleet; only an empty one is refused.
	email := strings.TrimSpace(r.PostFormValue("email"))
	kind := strings.TrimSpace(r.PostFormValue("kind"))
	hours, err := strconv.Atoi(r.PostFormValue("hours"))
	if _, _, _, known := fleet.CTAFor(kind); !known || email == "" || err != nil || hours < 1 || hours > muteMaxHours {
		http.Redirect(w, r, "/admin/fleet?err=mute", http.StatusSeeOther)
		return
	}
	note := strings.TrimSpace(r.PostFormValue("note"))
	for utf8.RuneCountInString(note) > muteNoteMax {
		_, size := utf8.DecodeLastRuneInString(note)
		note = note[:len(note)-size]
	}
	err = s.data.MuteFleet(r.Context(), v, FleetMute{
		Email: email,
		Kind:  kind,
		Until: s.now().Add(time.Duration(hours) * time.Hour),
		Note:  note,
	})
	if err != nil {
		s.fail(w, r, v, err)
		return
	}
	http.Redirect(w, r, "/admin/fleet?ok=muted", http.StatusSeeOther)
}

// handleFleetUnmute lifts one mute.
func (s *Server) handleFleetUnmute(w http.ResponseWriter, r *http.Request) {
	v, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	if err := s.checkWrite(r); err != nil {
		http.Error(w, "request rejected", http.StatusForbidden)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "malformed form", http.StatusBadRequest)
		return
	}
	if !s.csrfValid(v, r.PostFormValue("csrf")) {
		http.Redirect(w, r, "/admin/fleet?err=stale", http.StatusSeeOther)
		return
	}
	email := strings.TrimSpace(r.PostFormValue("email"))
	kind := strings.TrimSpace(r.PostFormValue("kind"))
	if email == "" || kind == "" {
		http.Redirect(w, r, "/admin/fleet?err=mute", http.StatusSeeOther)
		return
	}
	if err := s.data.UnmuteFleet(r.Context(), v, email, kind); err != nil {
		s.fail(w, r, v, err)
		return
	}
	http.Redirect(w, r, "/admin/fleet?ok=unmuted", http.StatusSeeOther)
}

// evaluationProblem is the sentence the page shows when the evaluator's view
// could not be read. The coverage table still renders: half a page that
// says which half is missing beats a 500 during the incident the page is
// open for. The reason goes to the log, not the page.
func evaluationProblem(err error) string {
	if err == nil || errors.Is(err, ErrNotFound) {
		return ""
	}
	return "The fleet evaluator's view could not be read, so the action list, the manifest comparison and the empty-start counts are missing from this page. The server log line \"fleet page evaluation failed\" has the reason."
}
