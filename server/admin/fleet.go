package admin

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/health"
)

// Device is an enrolled machine. Revoked devices are carried rather than
// filtered because a revoked device that is still sending health reports is a
// finding, and a filter here would delete the evidence of it.
type Device struct {
	ID         string     `json:"id"`
	Email      string     `json:"email"`
	Hostname   string     `json:"hostname,omitempty"`
	EnrolledAt time.Time  `json:"enrolled_at"`
	LastSeenAt *time.Time `json:"last_seen_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
}

// Revoked reports whether the device's credential has been withdrawn.
func (d Device) Revoked() bool { return d.RevokedAt != nil }

// HealthSnapshot is the newest health report from one machine, as stored.
type HealthSnapshot struct {
	Email    string `json:"email"`
	DeviceID string `json:"device_id,omitempty"`

	// EmittedAt is the laptop's clock and ReceivedAt is ours. Coverage measures
	// silence from ReceivedAt, because emitted_at is a value the reporting
	// machine chooses: a laptop whose clock has jumped forward would otherwise
	// look permanently fresh, and a clock that has jumped back would look
	// permanently silent. Emitted time is still carried so the difference between
	// the two remains visible.
	EmittedAt  time.Time `json:"emitted_at"`
	ReceivedAt time.Time `json:"received_at"`

	// Worst is the level the agent itself assigned, denormalised at ingest. It is
	// preferred over recomputing from Report so the fleet page agrees with what
	// the machine said about itself, even for a report from an agent newer than
	// this server.
	Worst  health.Level  `json:"worst,omitempty"`
	Report health.Report `json:"report"`
}

// Status is the coverage state of a person or a machine. It answers "are we
// receiving anything", which is a separate question from "is what we receive
// healthy"; that second question is answered by Level, taken from the agent's
// own conditions.
type Status string

const (
	// StatusReporting means a report arrived inside the freshness window.
	StatusReporting Status = "reporting"
	// StatusSilent means reports used to arrive and have stopped.
	StatusSilent Status = "silent"
	// StatusNeverReported means enrolled and never heard from once. This is the
	// state that absence-based alerting cannot see: there is no series to go
	// missing, so a monitor built on observed traffic will never fire for it, and
	// the person is silently uncovered for as long as nobody looks at this page.
	StatusNeverReported Status = "never_reported"
	// StatusDisabled means the person is no longer expected to report at all, so
	// they are excluded from the coverage denominator.
	StatusDisabled Status = "disabled"
)

// FleetOptions tune the coverage windows. Zero values take the defaults below.
type FleetOptions struct {
	// StaleAfter is how long without a report before a machine counts as silent.
	StaleAfter time.Duration
	// EnrollmentGrace is how long a newly added person has before never-reported
	// escalates from a note to a problem.
	EnrollmentGrace time.Duration
}

const (
	// A day. These are laptops: they are shut overnight, over weekends and
	// through holidays, and a shorter window would fill the page with people who
	// are simply asleep. A page that is mostly false positives is a page nobody
	// reads, and this one has to be read for the never-reported case to be caught
	// at all.
	defaultStaleAfter = 24 * time.Hour
	// The same day, for the same reason from the other end: somebody added at
	// 17:00 has not installed anything yet, and flagging them as uncovered before
	// they have had a working day to run the installer trains operators to ignore
	// the flag that matters most.
	defaultEnrollmentGrace = 24 * time.Hour
)

func (o FleetOptions) withDefaults() FleetOptions {
	if o.StaleAfter <= 0 {
		o.StaleAfter = defaultStaleAfter
	}
	if o.EnrollmentGrace <= 0 {
		o.EnrollmentGrace = defaultEnrollmentGrace
	}
	return o
}

// FleetInput is everything Coverage reads. Grouped into a struct because the
// three slices are meaningless in isolation and easy to transpose as arguments.
type FleetInput struct {
	Principals []Principal
	Devices    []Device
	Health     []HealthSnapshot
}

// Fleet is the coverage report.
type Fleet struct {
	GeneratedAt     time.Time     `json:"generated_at"`
	StaleAfter      time.Duration `json:"stale_after"`
	EnrollmentGrace time.Duration `json:"enrollment_grace"`

	Summary    FleetSummary      `json:"summary"`
	Principals []PrincipalHealth `json:"principals"`
}

// FleetSummary is the top line. Enrolled is the denominator for every ratio on
// this page, and Reporting, Silent and NeverReported always sum to it.
type FleetSummary struct {
	// Enrolled counts people who are on the roster and not disabled, whether or
	// not anything has ever been heard from them. Deriving this from observed
	// traffic instead is the mistake this whole file exists to avoid: a fleet
	// measured by who is reporting is 100% covered by definition.
	Enrolled      int `json:"enrolled"`
	Reporting     int `json:"reporting"`
	Silent        int `json:"silent"`
	NeverReported int `json:"never_reported"`
	Disabled      int `json:"disabled"`

	// Paused counts people whose agent is reporting and says the user turned
	// capture off. They are covered by monitoring and uncovered by capture, which
	// is a product question rather than an operational fault, so they are counted
	// separately instead of being coloured red.
	Paused   int `json:"paused"`
	Degraded int `json:"degraded"`
	Critical int `json:"critical"`

	Devices              int `json:"devices"`
	DevicesNeverReported int `json:"devices_never_reported"`

	// CoverageRatio is Reporting over Enrolled, in [0,1].
	CoverageRatio float64 `json:"coverage_ratio"`
}

// PrincipalHealth is one person's row.
type PrincipalHealth struct {
	Email       string `json:"email"`
	DisplayName string `json:"display_name,omitempty"`
	Role        Role   `json:"role"`

	Status Status       `json:"status"`
	Level  health.Level `json:"level"`
	Detail string       `json:"detail"`

	// Since is when the current status began and For is how long it has held.
	// "Silent" without a duration cannot be triaged: an hour is lunch and three
	// weeks is a person whose agent died the day they installed it.
	Since time.Time     `json:"since"`
	For   time.Duration `json:"for"`

	LastReportAt  time.Time `json:"last_report_at,omitempty"`
	LastEmittedAt time.Time `json:"last_emitted_at,omitempty"`

	Paused bool `json:"paused,omitempty"`

	// Conditions are the agent's own, verbatim. They are not re-derived from the
	// raw counters in the report: the client has the context to tell a busy
	// machine from a broken one and has already made that call, and a second,
	// server-side derivation would disagree with the first at the worst possible
	// time.
	Conditions []health.Condition `json:"conditions,omitempty"`

	Devices []DeviceHealth `json:"devices,omitempty"`
}

// DeviceHealth is one machine's row inside a person's.
type DeviceHealth struct {
	DeviceID string `json:"device_id,omitempty"`
	Hostname string `json:"hostname,omitempty"`

	Status Status       `json:"status"`
	Level  health.Level `json:"level"`
	Detail string       `json:"detail"`

	Since time.Time     `json:"since"`
	For   time.Duration `json:"for"`

	EnrolledAt   time.Time `json:"enrolled_at,omitempty"`
	LastReportAt time.Time `json:"last_report_at,omitempty"`
	Revoked      bool      `json:"revoked,omitempty"`

	Conditions []health.Condition `json:"conditions,omitempty"`
}

// Coverage computes fleet coverage from the roster outwards.
//
// The iteration order is the argument. It walks principals and asks what has
// been heard from each, rather than walking health reports and summarising what
// arrived. Those two produce the same answer for everybody who is working and
// opposite answers for everybody who is not, and the second one cannot represent
// the person who enrolled, never ran the installer successfully, and has been
// invisible since.
//
// It is a pure function of its inputs and the supplied clock so the same fleet
// can be re-derived from a stored snapshot, and so the interesting states can be
// tested without waiting a day for one to occur.
func Coverage(now time.Time, in FleetInput, opts FleetOptions) Fleet {
	opts = opts.withDefaults()

	devices := map[string][]Device{}
	for _, d := range in.Devices {
		e := normalizeEmail(d.Email)
		devices[e] = append(devices[e], d)
	}
	// Keyed by device so two laptops belonging to one person stay distinct. An
	// empty device id is a legitimate key: reports predating device enrolment, or
	// from an ingest path that did not resolve one, still prove the person is
	// reporting and must not be dropped.
	snaps := map[string]map[string]HealthSnapshot{}
	for _, s := range in.Health {
		e := normalizeEmail(s.Email)
		if snaps[e] == nil {
			snaps[e] = map[string]HealthSnapshot{}
		}
		if prev, ok := snaps[e][s.DeviceID]; ok && prev.ReceivedAt.After(s.ReceivedAt) {
			continue
		}
		snaps[e][s.DeviceID] = s
	}

	f := Fleet{
		GeneratedAt:     now,
		StaleAfter:      opts.StaleAfter,
		EnrollmentGrace: opts.EnrollmentGrace,
		Principals:      make([]PrincipalHealth, 0, len(in.Principals)),
	}

	for _, p := range in.Principals {
		email := normalizeEmail(p.Email)
		row := PrincipalHealth{
			Email:       email,
			DisplayName: p.DisplayName,
			Role:        p.Role,
		}

		if p.Disabled() {
			row.Status = StatusDisabled
			row.Level = health.LevelInfo
			row.Detail = "disabled, so no reports are expected"
			row.Since = *p.DisabledAt
			row.For = since(now, row.Since)
			f.Summary.Disabled++
			f.Principals = append(f.Principals, row)
			continue
		}

		row.Devices = deviceRows(now, devices[email], snaps[email], opts)
		f.Summary.Devices += len(row.Devices)
		for _, d := range row.Devices {
			if d.Status == StatusNeverReported {
				f.Summary.DevicesNeverReported++
			}
		}

		latest, ok := latestTrusted(snaps[email], devices[email])
		switch {
		case !ok:
			row.Status = StatusNeverReported
			row.Since = p.AddedAt
			row.For = since(now, row.Since)
			// Escalation on age, not on the state itself. A person added an hour
			// ago is mid-onboarding; the same person a week later is a hole in the
			// fleet that nothing else in the system will ever report, because
			// there is no missing series for an absence alert to notice.
			if row.For > opts.EnrollmentGrace {
				row.Level = health.LevelCritical
				row.Detail = fmt.Sprintf(
					"enrolled %s ago and has never reported once; no absence alert can fire for a machine that never produced a series",
					round(row.For))
			} else {
				row.Level = health.LevelInfo
				row.Detail = fmt.Sprintf("enrolled %s ago, first report not yet due", round(row.For))
			}

		case now.Sub(latest.ReceivedAt) > opts.StaleAfter:
			row.Status = StatusSilent
			row.Since = latest.ReceivedAt
			row.For = since(now, row.Since)
			row.Level, row.Detail = silentLevel(latest, row.For)

		default:
			row.Status = StatusReporting
			row.Since = latest.ReceivedAt
			row.For = since(now, row.Since)
			row.Level = worstOf(latest)
			row.Detail = reportingDetail(latest, row.Level)
		}

		if ok {
			row.LastReportAt = latest.ReceivedAt
			row.LastEmittedAt = latest.EmittedAt
			row.Paused = paused(latest)
			row.Conditions = append([]health.Condition(nil), latest.Report.Conditions...)
		}

		// A device whose credential we withdrew and which is still shipping
		// reports contradicts the revocation, so the person's row is escalated to
		// match the machine's. Leaving it visible only on a nested device row
		// hides it behind a disclosure triangle nobody opens.
		for _, d := range row.Devices {
			if d.Revoked && d.Status == StatusReporting {
				row.Level = worse(row.Level, health.LevelCritical)
				row.Detail = "a revoked device is still sending reports; " + row.Detail
				break
			}
		}

		f.Summary.Enrolled++
		switch row.Status {
		case StatusReporting:
			f.Summary.Reporting++
		case StatusSilent:
			f.Summary.Silent++
		case StatusNeverReported:
			f.Summary.NeverReported++
		}
		if row.Paused {
			f.Summary.Paused++
		}
		switch row.Level {
		case health.LevelCritical:
			f.Summary.Critical++
		case health.LevelDegraded:
			f.Summary.Degraded++
		}
		f.Principals = append(f.Principals, row)
	}

	if f.Summary.Enrolled > 0 {
		f.Summary.CoverageRatio = float64(f.Summary.Reporting) / float64(f.Summary.Enrolled)
	}
	sortRows(f.Principals)
	return f
}

// deviceRows expands one person's machines. Enrolled devices come first so a
// machine that was enrolled and never reported appears even though it has
// produced no data; reports from devices we have no row for are appended, since
// a report is proof the machine exists whatever the devices table says.
func deviceRows(now time.Time, ds []Device, snaps map[string]HealthSnapshot, opts FleetOptions) []DeviceHealth {
	var out []DeviceHealth
	seen := map[string]bool{}

	for _, d := range ds {
		snap, ok := snaps[d.ID]
		seen[d.ID] = true
		if !ok && d.Revoked() {
			// Revoked and quiet is the intended end state of a revocation, not a
			// coverage gap, so it is left off the page entirely.
			continue
		}
		row := DeviceHealth{
			DeviceID:   d.ID,
			Hostname:   d.Hostname,
			EnrolledAt: d.EnrolledAt,
			Revoked:    d.Revoked(),
		}
		if !ok {
			row.Status = StatusNeverReported
			row.Since = d.EnrolledAt
			row.For = since(now, row.Since)
			row.Level = health.LevelInfo
			row.Detail = fmt.Sprintf("enrolled %s ago, no report yet", round(row.For))
			if row.For > opts.EnrollmentGrace {
				row.Level = health.LevelCritical
				row.Detail = fmt.Sprintf("enrolled %s ago and has never reported", round(row.For))
			}
			out = append(out, row)
			continue
		}
		out = append(out, describeDevice(now, row, snap, opts))
	}

	for id, snap := range snaps {
		if seen[id] {
			continue
		}
		row := DeviceHealth{DeviceID: id, Hostname: snap.Report.Hostname}
		row = describeDevice(now, row, snap, opts)
		if id == "" {
			row.Detail += "; the report carried no device id"
		} else {
			row.Detail += "; this device is not in the devices table"
		}
		out = append(out, row)
	}

	slices.SortFunc(out, func(a, b DeviceHealth) int {
		if c := cmp.Compare(levelRank(b.Level), levelRank(a.Level)); c != 0 {
			return c
		}
		return cmp.Compare(a.DeviceID, b.DeviceID)
	})
	return out
}

func describeDevice(now time.Time, row DeviceHealth, snap HealthSnapshot, opts FleetOptions) DeviceHealth {
	row.LastReportAt = snap.ReceivedAt
	row.Since = snap.ReceivedAt
	row.For = since(now, row.Since)
	row.Conditions = append([]health.Condition(nil), snap.Report.Conditions...)
	if row.Hostname == "" {
		row.Hostname = snap.Report.Hostname
	}
	if now.Sub(snap.ReceivedAt) > opts.StaleAfter {
		row.Status = StatusSilent
		row.Level, row.Detail = silentLevel(snap, row.For)
	} else {
		row.Status = StatusReporting
		row.Level = worstOf(snap)
		row.Detail = reportingDetail(snap, row.Level)
	}
	if row.Revoked && row.Status == StatusReporting {
		row.Level = worse(row.Level, health.LevelCritical)
		row.Detail = "revoked but still reporting; " + row.Detail
	}
	return row
}

// latestTrusted picks the report that decides a person's coverage state.
//
// Reports from revoked devices are excluded: the revocation says that machine is
// no longer part of the fleet, and letting it satisfy the freshness check would
// mean a person whose only working laptop died still counts as covered because
// the laptop we retired is chattering. The revoked device is still surfaced on
// its own row.
func latestTrusted(snaps map[string]HealthSnapshot, ds []Device) (HealthSnapshot, bool) {
	revoked := map[string]bool{}
	for _, d := range ds {
		if d.Revoked() {
			revoked[d.ID] = true
		}
	}
	var best HealthSnapshot
	var found bool
	for id, s := range snaps {
		if revoked[id] {
			continue
		}
		if !found || s.ReceivedAt.After(best.ReceivedAt) {
			best, found = s, true
		}
	}
	return best, found
}

// silentLevel separates a machine that is quiet from one that is broken.
//
// Absence alone cannot tell them apart: a closed laptop and a dead agent both
// send nothing. What separates them is the last thing the machine said. Silence
// after a clean report is somebody's weekend; silence after a critical condition
// is a machine that was already failing when it went dark, and silence from an
// agent whose owner had paused capture is a deliberate quiet.
func silentLevel(s HealthSnapshot, quiet time.Duration) (health.Level, string) {
	last := worstOf(s)
	switch {
	case paused(s):
		return health.LevelInfo, fmt.Sprintf(
			"no report for %s; capture was paused by the user when it last reported", round(quiet))
	case last == health.LevelCritical:
		return health.LevelCritical, fmt.Sprintf(
			"no report for %s, and the last one was critical (%s); it was failing before it went dark",
			round(quiet), kinds(s))
	default:
		return health.LevelDegraded, fmt.Sprintf(
			"no report for %s; the last one was %s, so this is a machine that is off or asleep unless it stays quiet",
			round(quiet), last)
	}
}

func reportingDetail(s HealthSnapshot, level health.Level) string {
	if level == health.LevelInfo && len(s.Report.Conditions) == 0 {
		return "reporting, no conditions"
	}
	return fmt.Sprintf("reporting, %s: %s", level, kinds(s))
}

// kinds lists the condition names the agent raised. Kinds only: the details
// carry free text and this string ends up in a table cell.
func kinds(s HealthSnapshot) string {
	if len(s.Report.Conditions) == 0 {
		return "no conditions"
	}
	out := make([]string, 0, len(s.Report.Conditions))
	for _, c := range s.Report.Conditions {
		out = append(out, c.Kind)
	}
	return strings.Join(out, ", ")
}

// worstOf trusts the stored level first. The agent ranked its own conditions
// when it built the report, and that ranking is what its owner would see locally.
func worstOf(s HealthSnapshot) health.Level {
	if s.Worst != "" {
		return s.Worst
	}
	return s.Report.Worst()
}

// paused reads the posture from both places it can appear. The field is the
// agent's own statement; the condition is how it reaches an operator. Older or
// partial reports may carry only one.
func paused(s HealthSnapshot) bool {
	if s.Report.Paused {
		return true
	}
	for _, c := range s.Report.Conditions {
		if c.Kind == health.KindPaused {
			return true
		}
	}
	return false
}

// levelRank orders levels. health keeps its own rank unexported, so this is a
// deliberate duplicate over a three-value set that has been stable since the
// package was written.
func levelRank(l health.Level) int {
	switch l {
	case health.LevelCritical:
		return 3
	case health.LevelDegraded:
		return 2
	case health.LevelInfo:
		return 1
	default:
		return 0
	}
}

func worse(a, b health.Level) health.Level {
	if levelRank(b) > levelRank(a) {
		return b
	}
	return a
}

// sortRows puts the rows that need action at the top: worst level first, then
// longest in that state, then email so the order does not shuffle between
// refreshes of an otherwise unchanged fleet.
func sortRows(rows []PrincipalHealth) {
	slices.SortFunc(rows, func(a, b PrincipalHealth) int {
		if c := cmp.Compare(levelRank(b.Level), levelRank(a.Level)); c != 0 {
			return c
		}
		if c := cmp.Compare(b.For, a.For); c != 0 {
			return c
		}
		return cmp.Compare(a.Email, b.Email)
	})
}

// since is the elapsed time, clamped at zero. A row timestamped slightly in the
// future is ordinary on a fleet of laptops with drifting clocks, and a negative
// duration would sort as the freshest thing on the page.
func since(now, t time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}
	if d := now.Sub(t); d > 0 {
		return d
	}
	return 0
}

// round trims durations for display. Seconds below an hour, minutes above it:
// nobody triages a three-week silence to the second.
func round(d time.Duration) time.Duration {
	if d >= time.Hour {
		return d.Round(time.Minute)
	}
	return d.Round(time.Second)
}
