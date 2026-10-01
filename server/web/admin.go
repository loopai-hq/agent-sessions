package web

import (
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/server/fleet"
)

type principalsView struct {
	Page       Page
	Principals []Principal
	Admins     int
	// Self marks the viewer's own row, which the template disables: the guard
	// against removing the last admin is enforced by storage, but an admin who
	// can click "make me a member" and then discover they cannot undo it has
	// been given a footgun with a confirmation dialog on it.
	Self string
	// Q narrows the roster to people whose name or address starts with it. The
	// Admins figure stays whole-roster: "how many admins exist" does not change
	// because the page is filtered to the letter k.
	Q string
	// Options feeds the search box's datalist.
	Options []string
}

// matchesPerson reports whether a prefix matches an email or a display name,
// case-insensitively. One function because four pages filter people and four
// hand-rolled comparisons would drift on case handling.
func matchesPerson(q, email, name string) bool {
	if q == "" {
		return true
	}
	q = strings.ToLower(q)
	return strings.HasPrefix(strings.ToLower(email), q) ||
		strings.HasPrefix(strings.ToLower(name), q)
}

func (s *Server) handlePrincipals(w http.ResponseWriter, r *http.Request) {
	v, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	ps, err := s.data.Principals(r.Context(), v)
	if err != nil {
		s.fail(w, r, v, err)
		return
	}
	sortPrincipals(ps)

	// The admin count and the datalist come from the whole roster; the rows
	// honour the filter. Counting after filtering would make "1 active admin"
	// a statement about the search box rather than about the system.
	admins := countAdmins(ps, "", "", false)
	options := make([]string, 0, len(ps)*2)
	for _, p := range ps {
		options = append(options, p.Email)
		if p.DisplayName != "" {
			options = append(options, p.DisplayName)
		}
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q != "" {
		kept := ps[:0]
		for _, p := range ps {
			if matchesPerson(q, p.Email, p.DisplayName) {
				kept = append(kept, p)
			}
		}
		ps = kept
	}

	p := s.page(v, "People", "admin")
	p.Notice, p.Error = adminMessage(r.URL.Query())
	s.rnd.render(w, http.StatusOK, "principals.html", principalsView{
		Page:       p,
		Principals: ps,
		Admins:     admins,
		Self:       v.Email,
		Q:          q,
		Options:    options,
	})
}

// handlePrincipalUpdate applies one row's edit.
//
// It is a POST to this path rather than the API's PUT because an HTML form can
// only issue GET and POST, and adding JavaScript to send a PUT would trade the
// whole no-script security posture for a verb. The JSON API keeps PUT; this is
// the browser's door to the same operation.
func (s *Server) handlePrincipalUpdate(w http.ResponseWriter, r *http.Request) {
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
		// A stale token is the common case by far, so say what to do about it
		// rather than implying wrongdoing.
		http.Redirect(w, r, "/admin/principals?err=stale", http.StatusSeeOther)
		return
	}

	email, err := url.PathUnescape(r.PathValue("email"))
	if err != nil || email == "" {
		s.notFound(w, r, v)
		return
	}
	role := r.PostFormValue("role")
	if role != RoleAdmin && role != RoleMember {
		http.Redirect(w, r, "/admin/principals?err=role", http.StatusSeeOther)
		return
	}
	disabled := r.PostFormValue("disabled") == "on"

	// The last-admin guard, evaluated here as well as in storage. Storage is
	// authoritative because it is the only layer that can make the check and
	// the write atomic; this copy exists so the common case produces a clear
	// message on the page instead of a generic failure.
	ps, err := s.data.Principals(r.Context(), v)
	if err != nil {
		s.fail(w, r, v, err)
		return
	}
	if countAdmins(ps, email, role, disabled) == 0 {
		http.Redirect(w, r, "/admin/principals?err=last_admin", http.StatusSeeOther)
		return
	}

	changed, err := s.data.SetPrincipal(r.Context(), v, PrincipalUpdate{
		Email:    email,
		Role:     role,
		Disabled: disabled,
	})
	switch {
	case err == nil && changed:
		http.Redirect(w, r, "/admin/principals?ok=saved", http.StatusSeeOther)
	case err == nil:
		// Distinguished on purpose. Reporting "Saved." for a submission that
		// moved nothing is how somebody comes to believe a role change took
		// effect when it did not, and it is indistinguishable from the real
		// thing right up until it matters.
		http.Redirect(w, r, "/admin/principals?ok=nochange", http.StatusSeeOther)
	case errors.Is(err, ErrNotFound):
		s.notFound(w, r, v)
	case asUserError(err) != "":
		http.Redirect(w, r, "/admin/principals?err=refused", http.StatusSeeOther)
	default:
		s.fail(w, r, v, err)
	}
}

// countAdmins reports how many enabled admins would remain if the given row
// were changed as described. Passing an empty email counts the current state.
func countAdmins(ps []Principal, email, role string, disabled bool) int {
	n := 0
	for _, p := range ps {
		r, d := p.Role, p.Disabled()
		if email != "" && strings.EqualFold(p.Email, email) {
			r, d = role, disabled
		}
		if r == RoleAdmin && !d {
			n++
		}
	}
	return n
}

func asUserError(err error) string {
	var ue UserError
	if errors.As(err, &ue) {
		return ue.Message
	}
	return ""
}

// adminMessage turns the redirect's outcome code into wording. Codes rather
// than a message in the query string, because a page that renders arbitrary
// text supplied in its own URL is a phishing surface even when the text is
// escaped.
func adminMessage(q url.Values) (notice, problem string) {
	switch q.Get("ok") {
	case "saved":
		notice = "Saved."
	case "nochange":
		notice = "No change: that person already had those settings."
	}
	switch q.Get("err") {
	case "stale":
		problem = "That form had been open too long. Try again."
	case "role":
		problem = "Pick either admin or member."
	case "last_admin":
		problem = "That would leave no active admin. Promote someone else first."
	case "refused":
		problem = "The change was refused. Reload and check the current roles."
	}
	return notice, problem
}

func sortPrincipals(ps []Principal) {
	sort.SliceStable(ps, func(i, j int) bool {
		// Admins first, then disabled accounts last, then alphabetical. The
		// order encodes what an operator scans this page for: who has power,
		// and who should not still be here.
		ai, aj := ps[i].Role == RoleAdmin, ps[j].Role == RoleAdmin
		if ai != aj {
			return ai
		}
		di, dj := ps[i].Disabled(), ps[j].Disabled()
		if di != dj {
			return dj
		}
		return ps[i].Email < ps[j].Email
	})
}

type fleetView struct {
	Page     Page
	Fleet    Fleet
	Machines []machineRow
	Now      time.Time
	// Q narrows the machine list by person or hostname prefix.
	Q       string
	Options []string
	// Latest is the serving build and LatestBuiltAt when it was cut. While no
	// release manifest is published it is also what a machine's version is
	// judged against, since agent releases are cut from it; once the manifest
	// exists the judgement moves to Published and the serving build is the
	// strip's second line.
	Latest        string
	LatestBuiltAt time.Time

	// Eval is the fleet evaluator's view; nil when it could not be read, in
	// which case Problem says so and the coverage table stands alone.
	Eval *fleet.Evaluation
	// CTAs is the action list, worst first, one row per (person, kind).
	CTAs []ctaRow
	// Published is the release manifest's commit and PublishedAt its build
	// date: "Latest build" in the strip. Empty while nothing is published.
	Published   string
	PublishedAt time.Time
	// Unmanaged counts machines whose build is not a release sha.
	Unmanaged   int
	MuteChoices []muteChoice
	Notice      string
	Problem     string
}

// Badge classifies a machine's version against the serving build: "latest",
// "stale", or "unknown" for a machine that has never reported. It follows the
// shape fleet-management pages settle on — a binary current/outdated state with
// dates, not release arithmetic, because "3 versions behind" needs a release
// ledger nobody maintains and a date answers the actual question, which is
// "how long has this machine been left out".
func versionBadge(reported, latest string) string {
	switch reported {
	case "":
		return "unknown"
	case latest:
		return "latest"
	default:
		return "stale"
	}
}

type machineRow struct {
	Machine Machine
	Age     time.Duration
	Worst   string
	// VersionBadge is "latest", "stale", "unmanaged" or "unknown" against the
	// published build (the serving build while nothing is published), and
	// VersionTitle its hover text; VersionSince dates the version's first
	// appearance in the fleet.
	VersionBadge string
	VersionTitle string
	VersionSince time.Time
	// Eval is the evaluator's row for this machine, nil without one.
	Eval *fleet.Machine
	// Lag is the version lag in hours as the page says it, empty unless the
	// machine is behind the published build.
	Lag string
	// Version is the build the row names: the evaluator's judgement over the
	// last hour of reports when there is one (a machine mid-upgrade names
	// the published build here and the old daemon's under AlsoRunning),
	// else the newest report's.
	Version     string
	AlsoRunning string
	// EmptyStarts is the machine's own count of lifecycle-only session
	// starts in the last day, empty when its client predates the field.
	EmptyStarts string
}

// handleFleet renders client coverage.
//
// Machines are ordered by severity and then by staleness, because the fleet
// page answers one question and it is not "how is everyone doing": it is
// "which machines have stopped telling us anything". A silent machine sorts
// alongside a critical one for that reason.
func (s *Server) handleFleet(w http.ResponseWriter, r *http.Request) {
	v, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	f, err := s.data.Fleet(r.Context(), v)
	if err != nil {
		s.fail(w, r, v, err)
		return
	}
	// The evaluator's view is read after coverage and separately from it:
	// coverage is the page's original question and still answers when the
	// evaluator cannot (its inputs are more, and newer, tables).
	ev, evErr := s.data.FleetEvaluation(r.Context(), v)
	if evErr != nil && !errors.Is(evErr, ErrNotFound) {
		s.log.Error("fleet page evaluation failed", "viewer", v.Email, "err", evErr)
		ev = nil
	}
	byDevice := map[string]*fleet.Machine{}
	names := map[string]string{}
	if ev != nil {
		for i := range ev.Machines {
			byDevice[ev.Machines[i].DeviceID] = &ev.Machines[i]
		}
	}
	for _, m := range f.Machines {
		if m.Name != "" {
			names[m.Email] = m.Name
		}
	}

	now := s.now()
	rows := make([]machineRow, 0, len(f.Machines))
	for _, m := range f.Machines {
		worst := string(m.Report.Worst())
		if m.Silent {
			worst = "silent"
		}
		rows = append(rows, machineRow{Machine: m, Age: m.Age(now), Worst: worst, Eval: byDevice[m.DeviceID]})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if severity(rows[i].Worst) != severity(rows[j].Worst) {
			return severity(rows[i].Worst) > severity(rows[j].Worst)
		}
		return rows[i].Age > rows[j].Age
	})

	// The version spread is counted over every machine, before the filter: it
	// answers "has the upgrade reached the fleet", which a filtered page must
	// not quietly narrow. Machines that have never reported have no version to
	// count and are named as such rather than folded into a real version.
	// With a manifest the judgement is the evaluator's (published commit =
	// current, another sha = behind, not a sha = unmanaged); without one the
	// serving build stands in, as it always has.
	manifest := ev != nil && ev.Summary.ManifestPresent
	options := make([]string, 0, len(rows)*2)
	for i := range rows {
		ver := rows[i].Machine.Report.AgentVersion
		rows[i].Version = ver
		if manifest && rows[i].Eval != nil {
			rows[i].VersionBadge, rows[i].VersionTitle = versionFromEvaluation(rows[i].Eval)
			if rows[i].Eval.VersionState == fleet.VersionLagging {
				rows[i].Lag = "behind " + hoursLabel(rows[i].Eval.HoursBehind)
			}
			if rows[i].Eval.Build != "" {
				rows[i].Version = rows[i].Eval.Build
			}
			rows[i].AlsoRunning = rows[i].Eval.AlsoRunning
		} else {
			rows[i].VersionBadge = versionBadge(ver, s.version)
			rows[i].VersionTitle = versionTitle(rows[i].VersionBadge)
		}
		if e := rows[i].Eval; e != nil && e.EmptyStarts24h != nil {
			rows[i].EmptyStarts = strconv.Itoa(*e.EmptyStarts24h)
		}
		rows[i].VersionSince = f.VersionFirstSeen[ver]
		options = append(options, rows[i].Machine.Email)
		if h := rows[i].Machine.Report.Hostname; h != "" {
			options = append(options, h)
		}
	}

	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q != "" {
		kept := rows[:0]
		for _, m := range rows {
			if matchesPerson(q, m.Machine.Email, m.Machine.Name) ||
				matchesPerson(q, m.Machine.Report.Hostname, "") {
				kept = append(kept, m)
			}
		}
		rows = kept
	}

	view := fleetView{
		Page:          s.page(v, "Fleet", "fleet"),
		Fleet:         f,
		Machines:      rows,
		Now:           now,
		Q:             q,
		Options:       options,
		Latest:        s.version,
		LatestBuiltAt: s.builtAt,
		Eval:          ev,
		MuteChoices:   muteChoices,
		Problem:       evaluationProblem(evErr),
	}
	if ev != nil {
		view.CTAs = ctaRows(ev.CTAs, names)
		view.Unmanaged = ev.Summary.Unmanaged
		if manifest {
			view.Published, view.PublishedAt = ev.Summary.PublishedBuild, ev.Summary.PublishedAt
		}
	}
	if notice, problem := fleetMessage(r.URL.Query()); notice != "" || problem != "" {
		view.Notice = notice
		if problem != "" {
			view.Problem = problem
		}
	}
	s.rnd.render(w, http.StatusOK, "fleet.html", view)
}

func severity(worst string) int {
	switch worst {
	case "silent":
		return 4
	case "critical":
		return 3
	case "degraded":
		return 2
	case "info":
		return 1
	default:
		return 0
	}
}

type accessView struct {
	Page    Page
	Entries []AccessEntry
	Filter  filters
	// People feeds the reader and owner datalists; both boxes complete from the
	// same set, because a reader is a principal and so is an owner.
	People []string
}

// handleAccessLog shows who read whose sessions. Capturing colleagues' work is
// only defensible if the reading is visible, and an audit table nobody can
// open is a promise rather than a control.
func (s *Server) handleAccessLog(w http.ResponseWriter, r *http.Request) {
	v, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	f := parseFilters(r)
	entries, err := s.data.AccessLog(r.Context(), v, AccessQuery{
		Viewer:    f.Email,
		SessionID: f.Q,
		Owner:     f.Repo,
		From:      parseDay(f.From, false),
		To:        parseDay(f.To, true),
		Limit:     s.pageN,
	})
	if err != nil {
		s.fail(w, r, v, err)
		return
	}
	view := accessView{
		Page:    s.page(v, "Access log", "access"),
		Entries: entries,
		Filter:  f,
	}
	if people, _, err := s.data.FilterOptions(r.Context(), v); err != nil {
		s.log.Error("filter options for the access log", "err", err)
	} else {
		view.People = people
	}
	s.rnd.render(w, http.StatusOK, "access.html", view)
}
