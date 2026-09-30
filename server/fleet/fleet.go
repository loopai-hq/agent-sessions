// Package fleet evaluates the fleet: what every enrolled laptop last said
// about itself, what the release manifest says it should be running, what
// the sessions table says its machines have been doing, and what an operator
// should do about each of those, ranked.
//
// The evaluation is a pure function (Evaluate) over rows the runner reads,
// so the alert lines the runner logs and the CTA list the fleet page draws
// come from one computation and cannot disagree. The package imports the
// standard library only: the dashboard renders its output, and the dashboard
// may not import the store.
package fleet

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	// SilentAfter is how long a machine may stay quiet before it counts as
	// silent. A day, because these are laptops: shut overnight, over weekends
	// and through holidays. The same figure the dashboard and the admin API
	// use, so a fleet never reads healthy on one page and silent on another.
	SilentAfter = 24 * time.Hour
	// LagAfter is how long after a publish a machine on another build counts
	// as behind. The daemon rechecks the release host every six hours, so a
	// day is four missed checks: the machine is off, or its upgrade is
	// failing, and either is worth a line.
	LagAfter = 24 * time.Hour
	// BuildWindow is how far back the evaluator looks for the builds one
	// machine has reported. A laptop keeps running the old daemon until the
	// session that started it ends, while the new binary is already on disk
	// and every new session spawns a daemon from it, so the newest report per
	// device alternates between builds for hours or days (on a real fleet,
	// most addresses had downloaded the new build while health_latest named
	// it on two or three devices, flapping). A device's
	// build is therefore the newest build any report in this window carried,
	// and it is behind only when no report in the window named the published
	// one.
	BuildWindow = time.Hour
	// EmptyStartMin and EmptyStartRatio are research/r8's rule: a device
	// that started at least this many lifecycle-only sessions in a day, and
	// at least this share of everything it started, has a script spawning
	// claude with no prompt.
	EmptyStartMin   = 20
	EmptyStartRatio = 0.5
	// EmptyStartRecipe is the one-liner that names the launcher on the
	// affected machine; it rides in every empty-start line and CTA so the
	// operator can paste it to the person.
	EmptyStartRecipe = `printf '%s %s ppid=%s %s bundle=%s term=%s ep=%s\n' "$(date -u +%FT%TZ)" "$PWD" "$PPID" "$(ps -o comm= -p $PPID)" "$__CFBundleIdentifier" "$TERM_PROGRAM" "$CLAUDE_CODE_ENTRYPOINT" >> ~/.loop/sessions/logs/launcher.log`
)

// Manifest is the release manifest (latest.json) the release step writes
// beside the binaries: the build every managed machine should converge on.
type Manifest struct {
	Version       string    `json:"version"`
	Commit        string    `json:"commit"`
	BuildDate     time.Time `json:"build_date"`
	CaptureSchema int       `json:"capture_schema"`
}

// Device is an enrolled machine.
type Device struct {
	ID           string
	Email        string
	Hostname     string
	AgentVersion string
	EnrolledAt   time.Time
	LastSeenAt   time.Time
	Revoked      bool
}

// Condition is one problem a machine reported about itself.
type Condition struct {
	Level  string    `json:"level"`
	Kind   string    `json:"kind"`
	Detail string    `json:"detail"`
	Since  time.Time `json:"since"`
}

// Report is the newest health report one machine sent, decoded by field
// name so a version 1 report (the whole fleet until the current client
// converges) and a version 2 report both read. Every version 2 field is a
// pointer, nil on a version 1 report, which has no such field. On a version
// 2 report an absent key is the client's omitempty and reads as zero: the
// the current client client leaves out every zero-valued field (in production, every
// version 2 report carried neither empty_starts_24h nor spool.parked), so a
// machine with nothing to report would otherwise
// read as one whose client cannot say.
type Report struct {
	Email      string
	DeviceID   string
	EmittedAt  time.Time
	ReceivedAt time.Time
	Worst      string

	SchemaVersion int
	AgentVersion  string
	// AgentCommit is the version 2 build stamp; nil on a version 1 report.
	AgentCommit *string
	Channel     *string
	Hostname    string
	Paused      bool
	Pending     int
	Quarantine  int
	// Parked is nil on a version 1 report, which has no parked queue.
	Parked  *int
	Dropped map[string]int
	// EmptyStarts24h is nil on a version 1 report.
	EmptyStarts24h *int
	Upgrade        *Upgrade
	Conditions     []Condition
}

// Upgrade is the version 2 report's account of its last self-upgrade check.
type Upgrade struct {
	LastCheckAt   time.Time `json:"last_check_at"`
	Result        string    `json:"result"`
	SkippedReason string    `json:"skipped_reason"`
	PublishedSHA  string    `json:"published_sha"`
	Error         string    `json:"error"`
}

// DecodeReport reads a stored report's JSON into a Report. A body that will
// not decode is an error rather than a blank machine: coverage alerts on
// absence, and a machine silently missing from the evaluation reads as
// silent when it is reporting perfectly well.
func DecodeReport(email, deviceID string, emitted, received time.Time, worst, version string, raw json.RawMessage) (Report, error) {
	var body struct {
		SchemaVersion int     `json:"schema_version"`
		AgentVersion  string  `json:"agent_version"`
		AgentCommit   *string `json:"agent_commit"`
		Channel       *string `json:"channel"`
		Hostname      string  `json:"hostname"`
		Paused        bool    `json:"paused"`
		Spool         struct {
			Pending    int            `json:"pending"`
			Quarantine int            `json:"quarantine"`
			Parked     *int           `json:"parked"`
			Dropped    map[string]int `json:"dropped"`
		} `json:"spool"`
		EmptyStarts24h *int        `json:"empty_starts_24h"`
		Upgrade        *Upgrade    `json:"upgrade"`
		Conditions     []Condition `json:"conditions"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			return Report{}, fmt.Errorf("fleet: decode health report for %s/%s: %w", email, deviceID, err)
		}
	}
	r := Report{
		Email: email, DeviceID: deviceID, EmittedAt: emitted, ReceivedAt: received, Worst: worst,
		SchemaVersion: body.SchemaVersion, AgentVersion: body.AgentVersion, AgentCommit: body.AgentCommit,
		Channel: body.Channel, Hostname: body.Hostname, Paused: body.Paused,
		Pending: body.Spool.Pending, Quarantine: body.Spool.Quarantine, Parked: body.Spool.Parked,
		Dropped: body.Spool.Dropped, EmptyStarts24h: body.EmptyStarts24h, Upgrade: body.Upgrade,
		Conditions: body.Conditions,
	}
	if r.AgentVersion == "" {
		r.AgentVersion = version
	}
	// Decoded by schema version, not by key presence: a version 2 report
	// says zero by leaving the key out.
	if body.SchemaVersion >= 2 {
		if r.Parked == nil {
			r.Parked = new(int)
		}
		if r.EmptyStarts24h == nil {
			r.EmptyStarts24h = new(int)
		}
	}
	return r, nil
}

// Build is the version the comparison reads off a report: the version 2
// commit when present, else the version 1 agent_version.
func (r Report) Build() string {
	if r.AgentCommit != nil && *r.AgentCommit != "" {
		return *r.AgentCommit
	}
	return r.AgentVersion
}

// DropDelta is the rise in one machine's dropped-event counter for one
// reason since the evaluator last looked, read as the difference between
// the hourly ledger before and after the tick's rollup.
type DropDelta struct {
	Email    string
	DeviceID string
	Reason   string
	Delta    int64
}

// BuildSighting is one build a machine reported inside the build window,
// with the last moment a report carried it. A machine has one per daemon
// it ran in the window: one while it is settled, two while an upgrade is
// still draining the old daemon's session.
type BuildSighting struct {
	Email    string
	DeviceID string
	Build    string
	LastSeen time.Time
}

// Everyone is the mute table's sentinel person. A mute filed under it
// silences its kind for the rows that name no person (the missing-answer
// rate is the fleet's, not anyone's) and for every person's machines
// besides: the fleet page files a fleet-wide row's mute under it, because
// a form that posted an empty person had nothing the table could key on,
// and an operator who wants a kind quiet for the whole fleet has one row
// to set and one to lift.
const Everyone = "*"

// MissingAnswerAlertRate is the share of answerable turns that may go
// unanswered before this is somebody's problem. It is the page's CTA rule and
// alert policy 08's threshold, deliberately one number: two surfaces that both
// mean "act on the missing answers" and disagree about when are worse than
// either alone, and a test in examples/deploy-gcp/monitoring keeps the policy file
// equal to this constant.
//
// It is 10% because the fleet has a chronic residual that is not a regression
// and differs by entrypoint. Over a week on a real fleet, excluding one
// broken developer build, the CLI lost about 2% of answerable turns and
// Claude Desktop about 4%, with ordinary desktop days reaching 6%. A build
// that has stopped capturing reads 100%. At
// 5% this fired on ordinary desktop variation rather than on a regression,
// which is what it exists to catch. 10% sits above the residual band and an
// order of magnitude below the failure signature.
//
// The cost is honest: a regression losing between 5 and 10 percent of answers
// does not trip this. Closing the desktop residual is what would let the
// number come back down, and until it does a lower one only teaches people to
// ignore the alert.
const MissingAnswerAlertRate = 0.10

// Mute silences one (person, kind) until a moment; a mute for Everyone
// silences the kind for the fleet.
type Mute struct {
	Email     string
	Kind      string
	Until     time.Time
	Note      string
	CreatedBy string
}

// Active reports whether the mute holds at now.
func (m Mute) Active(now time.Time) bool { return m.Until.After(now) }

// EmptyStart is one device's lifecycle-only session count over a day.
type EmptyStart struct {
	Email    string
	DeviceID string
	Aborted  int
	Total    int
	Cwds     []string
}

// AnswerCohort is one day's hook-captured human turns for one client build
// and entrypoint.
type AnswerCohort struct {
	Day          time.Time
	AgentVersion string
	Entrypoint   string
	Turns        int
	Answered     int
}

// Person is a roster row.
type Person struct {
	Email    string
	Disabled bool
}

// Inputs is everything one evaluation reads.
type Inputs struct {
	Now         time.Time
	ServerBuild string
	// Manifest is nil when the release host has none published yet.
	Manifest *Manifest
	People   []Person
	Devices  []Device
	Reports  []Report
	// Builds are the builds every machine reported in the last BuildWindow.
	Builds      []BuildSighting
	Drops       []DropDelta
	Mutes       []Mute
	EmptyStarts []EmptyStart
	Answers     []AnswerCohort
}

// Version states a machine can be in against the manifest.
const (
	VersionCurrent       = "current"
	VersionLagging       = "lagging"
	VersionUnmanaged     = "unmanaged"
	VersionUnknown       = "unknown"
	VersionNeverReported = "never_reported"
)

// Machine is one machine's evaluated state, for the page.
type Machine struct {
	Email      string
	DeviceID   string
	Hostname   string
	Build      string
	Channel    string
	Schema     int
	Revoked    bool
	Reported   bool
	LastReport time.Time
	Silent     bool
	SilentFor  time.Duration
	Worst      string
	Paused     bool
	Pending    int
	Quarantine int
	// Parked and EmptyStarts24h are nil where the report predates them.
	Parked         *int
	EmptyStarts24h *int
	Upgrade        *Upgrade
	Conditions     []Condition
	VersionState   string
	HoursBehind    float64
	// AlsoRunning is the other build the machine reported in the build
	// window while it also reported the published one: the old daemon still
	// draining a session beside the new binary. Empty once it settles.
	AlsoRunning string
}

// Summary is the fleet summary line.
type Summary struct {
	Enrolled          int
	Reporting         int
	Silent            int
	NeverReported     int
	CaptureBlocked    int
	QuarantineDevices int
	ParkedDevices     int
	Current           int
	Behind            int
	Unmanaged         int
	VersionUnknown    int
	Drops             int64
	Empties           int
	Sessions          int
	EmptyRate         float64
	AnswerTurns       int
	Answered          int
	MissingAnswerRate float64
	PublishedBuild    string
	PublishedAt       time.Time
	ServerBuild       string
	ManifestPresent   bool
}

// ConditionLine is one machine's condition, for the census metric.
type ConditionLine struct {
	Email        string
	DeviceID     string
	Kind         string
	Level        string
	Detail       string
	Since        time.Time
	AgentVersion string
}

// SilentLine is one machine that reported once and stopped.
type SilentLine struct {
	Email     string
	DeviceID  string
	Since     time.Time
	Hours     float64
	LastWorst string
}

// LagLine is one managed machine on a build other than the published one.
type LagLine struct {
	Email          string
	DeviceID       string
	AgentVersion   string
	PublishedBuild string
	HoursBehind    float64
}

// EmptyStartLine is one device over the empty-start rule.
type EmptyStartLine struct {
	Email    string
	DeviceID string
	Empties  int
	Total    int
	Rate     float64
	Cwds     []string
	Recipe   string
}

// MissingAnswerLine is one cohort's answer rate for one day.
type MissingAnswerLine struct {
	Day          time.Time
	AgentVersion string
	Entrypoint   string
	Turns        int
	Answered     int
	Rate         float64
}

// CTA is one operator action, one row per (person, kind), ranked worst
// first. Command is the real CLI verb when one exists; Action says it in
// words, naming the person when they have to run it.
type CTA struct {
	Rank     int
	Email    string
	DeviceID string
	Hostname string
	Kind     string
	Level    string
	Detail   string
	Since    time.Time
	Action   string
	Command  string
	Anchor   string
	Machines int
	// Muted is the mute that silences this row, nil when none does.
	Muted *Mute
}

// Evaluation is what one evaluation produced.
type Evaluation struct {
	At             time.Time
	Summary        Summary
	Machines       []Machine
	Conditions     []ConditionLine
	Drops          []DropLine
	Silent         []SilentLine
	Lag            []LagLine
	EmptyStarts    []EmptyStartLine
	MissingAnswers []MissingAnswerLine
	CTAs           []CTA
	Muted          []CTA
	// Mutes are the mutes in force at evaluation time, for the page.
	Mutes    []Mute
	Warnings []string
}

// DropLine is one machine's drops since the last tick, by reason.
type DropLine struct {
	Email    string
	DeviceID string
	Reason   string
	Delta    int64
}

// ctaSpec is the CTA table entry for one kind: the verb, the runbook, and
// where it sorts. Rank order is what an operator should do first; the
// quarantine row leads because the fleet baseline when the quarantine fix was promoted
// had a large share of devices critical from historical quarantine, and a
// list that buried them under version lag would hide the backlog the fix
// exists to burn down.
type ctaSpec struct {
	rank    int
	level   string
	action  string
	command string
	anchor  string
}

var ctaTable = map[string]ctaSpec{
	"quarantine_nonempty": {0, "critical", "Replay one quarantined item to see the server's verdict, then redrive the rest once the server accepts it", "loop-sessions doctor --replay-quarantine; loop-sessions doctor --redrive", "runbook-quarantine"},
	"capture_blocked":     {1, "critical", "Free disk on the machine: the spool refuses writes below its floor and events are being dropped right now; an older client refuses below 5% of the disk", "loop-sessions status", "runbook-capture-blocked"},
	"never_delivered":     {2, "critical", "Re-enrol the machine; check the device token was not revoked", "loop-sessions install", "runbook-enrol"},
	"backlog_stalled":     {3, "critical", "Check the laptop's network or VPN and the ingest 5xx alert; if the server is healthy, restart the daemon", "loop-sessions status", "runbook-backlog"},
	"drops_recorded":      {4, "error", "Read the reason: disk_full is the disk floor, queue_overflow the outbox cap, hook_abandoned a hook that timed out, rejected:<reason> the server refusing an item", "loop-sessions status", "runbook-drops"},
	"silent":              {5, "error", "Ping the person: the laptop is asleep, off, or its daemon died; reinstall past 72 hours", "loop-sessions install", "runbook-silent"},
	"parked":              {6, "error", "Items the server never gave a verdict on are parked; redrive them", "loop-sessions doctor --redrive", "runbook-parked"},
	"capture_stale":       {7, "degraded", "The harness is in use and nothing is being captured: put the hooks back", "loop-sessions install --hooks-only", "runbook-hooks"},
	"backlog_growing":     {8, "degraded", "Arrivals are outrunning deliveries while delivery still works; check the network and the ingest 5xx alert", "loop-sessions status", "runbook-backlog"},
	"spool_near_cap":      {9, "degraded", "The outbox is near its ceiling and will evict oldest-first; same as a backlog, act before it does", "loop-sessions status", "runbook-backlog"},
	"missing_answer":      {10, "error", "Turns are being captured without their answers: the fleet is still on a client without the last_assistant_message fix; upgrade and watch version lag", "loop-sessions daemon --upgrade-now", "runbook-missing-answers"},
	"empty_start":         {11, "warning", "A script on this machine runs claude non-interactively (a GUI host spawning a CLI per project, or claude -p / -c / --resume with stdin closed); the sessions are tagged empty and hidden; share the launcher recipe", "", "runbook-empty-sessions"},
	"version_lag":         {12, "warning", "The daemon self-upgrades at its next check; past a day, run the upgrade by hand", "loop-sessions daemon --upgrade-now", "runbook-upgrade"},
}

// CTAFor returns the table entry for a kind: the action, the command and
// the runbook anchor. ok is false for a kind the table does not know.
func CTAFor(kind string) (action, command, anchor string, ok bool) {
	spec, ok := ctaTable[kind]
	return spec.action, spec.command, spec.anchor, ok
}

// IsSHA reports whether a version string is a git sha (7 to 40 hex digits),
// which is what the release step stamps. Anything else (dev, e2e-1, a
// -dirty suffix) is a build nobody published, counted as unmanaged and
// never alerted as lag.
func IsSHA(v string) bool {
	v = strings.TrimSpace(v)
	if len(v) < 7 || len(v) > 40 {
		return false
	}
	for i := range len(v) {
		c := v[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// sameBuild compares a machine's build with the manifest's by prefix, in
// either direction: the manifest carries a full commit and the client a
// seven-digit one, or the reverse. Both sides have to be shas first, so a
// build of "7" is never on the commit that happens to start with it.
func sameBuild(build, commit string) bool {
	b, c := strings.ToLower(strings.TrimSpace(build)), strings.ToLower(strings.TrimSpace(commit))
	if !IsSHA(b) || !IsSHA(c) {
		return false
	}
	return strings.HasPrefix(b, c) || strings.HasPrefix(c, b)
}

type machineKey struct{ email, device string }

// Evaluate computes the fleet's state from the rows it is handed.
func Evaluate(in Inputs) Evaluation {
	now := in.Now
	ev := Evaluation{At: now}
	ev.Summary.ServerBuild = in.ServerBuild
	if in.Manifest != nil {
		ev.Summary.ManifestPresent = true
		ev.Summary.PublishedBuild = in.Manifest.Commit
		ev.Summary.PublishedAt = in.Manifest.BuildDate
	} else {
		ev.Warnings = append(ev.Warnings, "release manifest missing")
	}

	disabled := map[string]bool{}
	for _, p := range in.People {
		if p.Disabled {
			disabled[p.Email] = true
		}
	}
	reports := map[machineKey]Report{}
	for _, r := range in.Reports {
		reports[machineKey{r.Email, r.DeviceID}] = r
	}
	hostnames := map[machineKey]string{}
	mutes := map[machineKey]Mute{}
	for _, m := range in.Mutes {
		if m.Active(now) {
			mutes[machineKey{m.Email, m.Kind}] = m
			ev.Mutes = append(ev.Mutes, m)
		}
	}
	sort.Slice(ev.Mutes, func(i, j int) bool {
		if ev.Mutes[i].Email != ev.Mutes[j].Email {
			return ev.Mutes[i].Email < ev.Mutes[j].Email
		}
		return ev.Mutes[i].Kind < ev.Mutes[j].Kind
	})
	muteFor := func(email, kind string) *Mute {
		if m, ok := mutes[machineKey{email, kind}]; ok {
			c := m
			return &c
		}
		if m, ok := mutes[machineKey{Everyone, kind}]; ok {
			c := m
			return &c
		}
		return nil
	}
	// muted says whether a (person, kind) is silenced. A mute holds back the
	// alert line and the machine's count in the summary gauge, because the
	// line-count and gauge policies are what page; the CTA row still exists,
	// under muted, so the page shows what is being held back and until when.
	muted := func(email, kind string) bool { return muteFor(email, kind) != nil }
	// The builds each machine reported inside the window, most recently seen
	// first, so the other build a machine runs alongside the published one
	// is the one it reported last.
	recent := map[machineKey][]BuildSighting{}
	for _, b := range in.Builds {
		if b.Build == "" {
			continue
		}
		k := machineKey{b.Email, b.DeviceID}
		recent[k] = append(recent[k], b)
	}
	for _, seen := range recent {
		sort.SliceStable(seen, func(i, j int) bool { return seen[i].LastSeen.After(seen[j].LastSeen) })
	}

	// Machines: every enrolled device, plus any report from a device the
	// devices table does not know (a report is proof the machine exists).
	seen := map[machineKey]bool{}
	for _, d := range in.Devices {
		if disabled[d.Email] {
			continue
		}
		k := machineKey{d.Email, d.ID}
		seen[k] = true
		r, reported := reports[k]
		if !reported && d.Revoked {
			// Revoked and quiet is the intended end state of a revocation.
			continue
		}
		m := Machine{Email: d.Email, DeviceID: d.ID, Hostname: d.Hostname, Build: d.AgentVersion, Revoked: d.Revoked}
		if reported {
			fill(&m, r)
		}
		hostnames[k] = m.Hostname
		ev.Machines = append(ev.Machines, m)
	}
	for k, r := range reports {
		if seen[k] || disabled[k.email] {
			continue
		}
		m := Machine{Email: r.Email, DeviceID: r.DeviceID}
		fill(&m, r)
		hostnames[k] = m.Hostname
		ev.Machines = append(ev.Machines, m)
	}
	sort.Slice(ev.Machines, func(i, j int) bool {
		if ev.Machines[i].Email != ev.Machines[j].Email {
			return ev.Machines[i].Email < ev.Machines[j].Email
		}
		return ev.Machines[i].DeviceID < ev.Machines[j].DeviceID
	})
	judgeSilence(&ev, now)

	type ctaKey struct{ email, kind string }
	ctas := map[ctaKey]*CTA{}
	addCTA := func(email, device, kind, level, detail string, since time.Time) {
		spec, ok := ctaTable[kind]
		if !ok {
			return
		}
		if level == "" {
			level = spec.level
		}
		k := ctaKey{email, kind}
		c, exists := ctas[k]
		if !exists {
			c = &CTA{Rank: spec.rank, Email: email, DeviceID: device, Hostname: hostnames[machineKey{email, device}],
				Kind: kind, Level: level, Detail: detail, Since: since, Anchor: spec.anchor, Command: spec.command, Muted: muteFor(email, kind)}
			c.Action = spec.action
			if spec.command != "" && email != "" {
				c.Action = "Ask " + shortEmail(email) + " to run `" + spec.command + "`. " + spec.action
			}
			ctas[k] = c
		}
		c.Machines++
		if c.Machines > 1 {
			c.Detail = fmt.Sprintf("%d machines; %s", c.Machines, detail)
			if since.Before(c.Since) || c.Since.IsZero() {
				c.Since = since
			}
		}
	}

	// Per machine: reporting state, conditions, version.
	for i := range ev.Machines {
		m := &ev.Machines[i]
		ev.Summary.Enrolled++
		switch {
		case !m.Reported:
			ev.Summary.NeverReported++
			m.VersionState = VersionNeverReported
			if !m.Revoked && m.Build != "" {
				// The enrollment-day build is all the fleet knows; judged the
				// same way and tallied (an unmanaged build is unmanaged whether
				// or not it reports) but never a lag line: a machine that
				// never reported is a coverage hole first.
				m.VersionState, m.HoursBehind = versionState(m.Build, in.Manifest, now)
				switch m.VersionState {
				case VersionCurrent:
					ev.Summary.Current++
				case VersionUnmanaged:
					ev.Summary.Unmanaged++
				case VersionUnknown:
					ev.Summary.VersionUnknown++
				}
			}
			continue
		case m.Silent:
			if !muted(m.Email, "silent") {
				ev.Summary.Silent++
				ev.Silent = append(ev.Silent, SilentLine{Email: m.Email, DeviceID: m.DeviceID, Since: m.LastReport, Hours: m.SilentFor.Hours(), LastWorst: m.Worst})
			}
			addCTA(m.Email, m.DeviceID, "silent", "error",
				fmt.Sprintf("no report for %s; the last one was %s", roundHours(m.SilentFor), m.Worst), m.LastReport)
		default:
			ev.Summary.Reporting++
			for _, c := range m.Conditions {
				// The census line is the record of what the machine said and
				// is never muted; the gauge is what pages, and is.
				ev.Conditions = append(ev.Conditions, ConditionLine{Email: m.Email, DeviceID: m.DeviceID, Kind: c.Kind, Level: c.Level, Detail: c.Detail, Since: c.Since, AgentVersion: m.Build})
				switch c.Kind {
				case "capture_blocked":
					if !muted(m.Email, c.Kind) {
						ev.Summary.CaptureBlocked++
					}
				}
				if _, ok := ctaTable[c.Kind]; ok {
					addCTA(m.Email, m.DeviceID, c.Kind, c.Level, c.Detail, c.Since)
				}
			}
			if m.Quarantine > 0 {
				if !muted(m.Email, "quarantine_nonempty") {
					ev.Summary.QuarantineDevices++
				}
				if !hasKind(m.Conditions, "quarantine_nonempty") {
					addCTA(m.Email, m.DeviceID, "quarantine_nonempty", "critical", fmt.Sprintf("%d items permanently undeliverable", m.Quarantine), m.LastReport)
				}
			}
			if m.Parked != nil && *m.Parked > 0 {
				if !muted(m.Email, "parked") {
					ev.Summary.ParkedDevices++
				}
				addCTA(m.Email, m.DeviceID, "parked", "error", fmt.Sprintf("%d items parked without a server verdict", *m.Parked), m.LastReport)
			}
		}
		m.VersionState, m.HoursBehind, m.Build, m.AlsoRunning = judgeBuild(m.Build, recent[machineKey{m.Email, m.DeviceID}], in.Manifest, now)
		switch m.VersionState {
		case VersionCurrent:
			ev.Summary.Current++
		case VersionUnmanaged:
			ev.Summary.Unmanaged++
		case VersionUnknown:
			ev.Summary.VersionUnknown++
		case VersionLagging:
			if m.Silent {
				// An off machine cannot upgrade: it is the silent row's
				// problem, and the state stays on the row for the page.
				break
			}
			lagMuted := muted(m.Email, "version_lag")
			if !lagMuted {
				ev.Lag = append(ev.Lag, LagLine{Email: m.Email, DeviceID: m.DeviceID, AgentVersion: m.Build, PublishedBuild: in.Manifest.Commit, HoursBehind: m.HoursBehind})
			}
			if m.HoursBehind >= LagAfter.Hours() {
				if !lagMuted {
					ev.Summary.Behind++
				}
				addCTA(m.Email, m.DeviceID, "version_lag", "warning",
					fmt.Sprintf("on %s, published build %s is %s old", m.Build, shortSHA(in.Manifest.Commit), roundHours(time.Duration(m.HoursBehind*float64(time.Hour)))), in.Manifest.BuildDate)
			}
		}
	}

	// Drops since the last tick, by reason.
	for _, d := range in.Drops {
		if d.Delta <= 0 {
			continue
		}
		if !muted(d.Email, "drops_recorded") {
			ev.Summary.Drops += d.Delta
			ev.Drops = append(ev.Drops, DropLine(d))
		}
		addCTA(d.Email, d.DeviceID, "drops_recorded", "error", fmt.Sprintf("%d events dropped (%s) since the last evaluation", d.Delta, d.Reason), now)
	}
	sort.Slice(ev.Drops, func(i, j int) bool {
		a, b := ev.Drops[i], ev.Drops[j]
		if a.Email != b.Email {
			return a.Email < b.Email
		}
		if a.DeviceID != b.DeviceID {
			return a.DeviceID < b.DeviceID
		}
		return a.Reason < b.Reason
	})

	// Empty starts: research/r8's per-device rule. A muted device leaves
	// the fleet rate as well as the line: its sessions are a known cause,
	// and the rate exists to catch an unknown one.
	for _, e := range in.EmptyStarts {
		if disabled[e.Email] {
			continue
		}
		emptyMuted := muted(e.Email, "empty_start")
		if !emptyMuted {
			ev.Summary.Empties += e.Aborted
			ev.Summary.Sessions += e.Total
		}
		if e.Aborted >= EmptyStartMin && e.Total > 0 && float64(e.Aborted)/float64(e.Total) >= EmptyStartRatio {
			rate := float64(e.Aborted) / float64(e.Total)
			if !emptyMuted {
				ev.EmptyStarts = append(ev.EmptyStarts, EmptyStartLine{Email: e.Email, DeviceID: e.DeviceID, Empties: e.Aborted, Total: e.Total, Rate: rate, Cwds: e.Cwds, Recipe: EmptyStartRecipe})
			}
			addCTA(e.Email, e.DeviceID, "empty_start", "warning",
				fmt.Sprintf("%d of %d sessions in 24 h were lifecycle-only spawns (cwd: %s); recipe: %s", e.Aborted, e.Total, strings.Join(e.Cwds, ", "), EmptyStartRecipe), now.Add(-24*time.Hour))
		}
	}
	if ev.Summary.Sessions > 0 {
		ev.Summary.EmptyRate = float64(ev.Summary.Empties) / float64(ev.Summary.Sessions)
	}

	// Missing answers per day, build and entrypoint; the summary rate is the
	// latest day's, over every cohort.
	var latest time.Time
	for _, c := range in.Answers {
		if c.Turns == 0 {
			continue
		}
		rate := 1 - float64(c.Answered)/float64(c.Turns)
		ev.MissingAnswers = append(ev.MissingAnswers, MissingAnswerLine{Day: c.Day, AgentVersion: c.AgentVersion, Entrypoint: c.Entrypoint, Turns: c.Turns, Answered: c.Answered, Rate: rate})
		if c.Day.After(latest) {
			latest = c.Day
		}
	}
	for _, c := range in.Answers {
		if c.Day.Equal(latest) {
			ev.Summary.AnswerTurns += c.Turns
			ev.Summary.Answered += c.Answered
		}
	}
	if ev.Summary.AnswerTurns > 0 {
		ev.Summary.MissingAnswerRate = 1 - float64(ev.Summary.Answered)/float64(ev.Summary.AnswerTurns)
		if ev.Summary.MissingAnswerRate > MissingAnswerAlertRate {
			addCTA("", "", "missing_answer", "error",
				fmt.Sprintf("%d of %d hook-captured turns on %s have no answer (%.0f%%)", ev.Summary.AnswerTurns-ev.Summary.Answered, ev.Summary.AnswerTurns, latest.Format("2006-01-02"), 100*ev.Summary.MissingAnswerRate), latest)
		}
	}

	// The CTA list: worst first by the table's rank, then longest standing.
	for _, c := range ctas {
		if c.Muted != nil {
			ev.Muted = append(ev.Muted, *c)
		} else {
			ev.CTAs = append(ev.CTAs, *c)
		}
	}
	byRank := func(cs []CTA) {
		sort.Slice(cs, func(i, j int) bool {
			if cs[i].Rank != cs[j].Rank {
				return cs[i].Rank < cs[j].Rank
			}
			if !cs[i].Since.Equal(cs[j].Since) {
				return cs[i].Since.Before(cs[j].Since)
			}
			return cs[i].Email < cs[j].Email
		})
	}
	byRank(ev.CTAs)
	byRank(ev.Muted)
	sort.Slice(ev.Conditions, func(i, j int) bool {
		a, b := ev.Conditions[i], ev.Conditions[j]
		if a.Email != b.Email {
			return a.Email < b.Email
		}
		if a.DeviceID != b.DeviceID {
			return a.DeviceID < b.DeviceID
		}
		return a.Kind < b.Kind
	})
	return ev
}

// fill copies a report onto a machine and judges its silence from arrival,
// never from the report's own clock.
func fill(m *Machine, r Report) {
	m.Reported = true
	m.LastReport = r.ReceivedAt
	if r.Hostname != "" {
		m.Hostname = r.Hostname
	}
	if b := r.Build(); b != "" {
		m.Build = b
	}
	if r.Channel != nil {
		m.Channel = *r.Channel
	}
	m.Schema = r.SchemaVersion
	m.Worst = r.Worst
	m.Paused = r.Paused
	m.Pending = r.Pending
	m.Quarantine = r.Quarantine
	m.Parked = r.Parked
	m.EmptyStarts24h = r.EmptyStarts24h
	m.Upgrade = r.Upgrade
	m.Conditions = r.Conditions
}

// judgeSilence sets the silent flag from arrival time; split from fill so the
// caller decides the clock.
func judgeSilence(ev *Evaluation, now time.Time) {
	for i := range ev.Machines {
		m := &ev.Machines[i]
		if m.Reported && now.Sub(m.LastReport) > SilentAfter {
			m.Silent = true
			m.SilentFor = now.Sub(m.LastReport)
		}
	}
}

// judgeBuild judges one machine's build against the manifest over the build
// window. The published build seen in any report of the window makes the
// machine current, whatever its newest report says, and the other build it
// reported meanwhile is what it is still running alongside; with nothing in
// the window (a machine quiet for over an hour) the newest report's build is
// judged as it always was. It returns the state, the hours behind, the build
// the row should name, and the build run alongside it.
func judgeBuild(build string, seen []BuildSighting, manifest *Manifest, now time.Time) (state string, hours float64, current, also string) {
	if manifest != nil && manifest.Commit != "" {
		for _, s := range seen {
			if !sameBuild(s.Build, manifest.Commit) {
				continue
			}
			for _, o := range seen {
				if !sameBuild(o.Build, manifest.Commit) {
					also = o.Build
					break
				}
			}
			return VersionCurrent, 0, s.Build, also
		}
	}
	state, hours = versionState(build, manifest, now)
	return state, hours, build, ""
}

// versionState judges a build against the manifest.
func versionState(build string, manifest *Manifest, now time.Time) (string, float64) {
	if build == "" {
		return VersionUnknown, 0
	}
	if !IsSHA(build) {
		return VersionUnmanaged, 0
	}
	if manifest == nil || manifest.Commit == "" {
		return VersionUnknown, 0
	}
	if sameBuild(build, manifest.Commit) {
		return VersionCurrent, 0
	}
	hours := 0.0
	if !manifest.BuildDate.IsZero() && now.After(manifest.BuildDate) {
		hours = now.Sub(manifest.BuildDate).Hours()
	}
	return VersionLagging, hours
}

func hasKind(cs []Condition, kind string) bool {
	for _, c := range cs {
		if c.Kind == kind {
			return true
		}
	}
	return false
}

func shortEmail(email string) string {
	if i := strings.IndexByte(email, '@'); i > 0 {
		return email[:i]
	}
	return email
}

func shortSHA(s string) string {
	if len(s) > 7 {
		return s[:7]
	}
	return s
}

func roundHours(d time.Duration) string {
	if d < time.Hour {
		return fmt.Sprintf("%d min", int(d.Minutes()))
	}
	if d < 48*time.Hour {
		return fmt.Sprintf("%.0f h", d.Hours())
	}
	return fmt.Sprintf("%.0f d", d.Hours()/24)
}
