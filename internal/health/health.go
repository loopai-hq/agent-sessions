// Package health builds the client's self-telemetry.
//
// Employee laptops have no cloud credentials and cannot write metrics
// directly, so everything the fleet ever learns about a machine arrives inside
// this one struct. That constraint drives three properties.
//
// It is unconditional. The server alarms on the ABSENCE of these reports, so a
// Report must be produced on a fixed cadence whether or not anything happened
// and whether or not the machine is healthy. Build therefore returns no error:
// it takes whatever inputs it was handed, and anything it could not read becomes
// a condition rather than a failure. A health system that errors when the
// machine is unhealthy reports nothing exactly when it matters.
//
// It is conditions, not counters. A pending count of 4000 does not page anyone
// and does not tell an operator whether the machine is busy or broken. The
// derivation from raw state to a ranked, named problem happens here, on the
// client, because this is where the context to make that call exists.
//
// It is safe to ship fleet-wide. No session identifiers, project paths or
// prompt text appear anywhere in a Report. These values land in metric labels,
// where anything high-cardinality is both a cost problem and a leak; Redacted
// goes further and removes the machine identity itself for dashboards that are
// shared more widely than the fleet operators.
package health

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/discovery"
	"github.com/loopai-hq/loop-sessions/internal/spool"
)

// SchemaVersion is bumped when the wire shape of Report changes. It is present
// on every report so a server can decode reports from agents that predate its
// own deployment, which on a laptop fleet is the normal case rather than the
// exception.
//
// Version 2 adds the build identity (commit, build date, dirty flag), the
// capture schema and release channel the machine runs, the outcome of the last
// upgrade check, the parked count, the new drop reasons and the count of
// sessions that started and ended without a prompt. Every field is additive;
// a version 1 reader ignores them and a version 2 reader defaults them.
const SchemaVersion = 2

// Level ranks a condition. The set is deliberately three-wide: anything finer
// invites arguments about which shade a problem is, and anything coarser cannot
// separate "someone turned it off" from "this machine is losing data".
type Level string

const (
	// LevelInfo is something a person chose, such as a pause: nothing to fix.
	LevelInfo Level = "info"
	// LevelDegraded means data is delayed or at risk.
	LevelDegraded Level = "degraded"
	// LevelCritical means data is being lost.
	LevelCritical Level = "critical"
)

// rank orders levels for sorting and for Worst.
func (l Level) rank() int {
	switch l {
	case LevelCritical:
		return 3
	case LevelDegraded:
		return 2
	case LevelInfo:
		return 1
	default:
		return 0
	}
}

// Condition kinds. Stable, low-cardinality strings: they are the natural metric
// label, so specifics belong in Detail rather than in new Kind values.
const (
	// KindCaptureBlocked means the spool is refusing writes because the disk is
	// too full. Events are being dropped to a counter right now.
	KindCaptureBlocked = "capture_blocked"
	// KindBacklogStalled means delivery worked before and has stopped, with an
	// aging backlog behind it.
	KindBacklogStalled = "backlog_stalled"
	// KindBacklogGrowing means the queue is large but delivery still succeeds:
	// arrivals are outrunning departures, which is a warning, not an outage.
	KindBacklogGrowing = "backlog_growing"
	// KindQuarantineNonEmpty means items are permanently undeliverable and an
	// operator has to look at them.
	KindQuarantineNonEmpty = "quarantine_nonempty"
	// KindParkedNonEmpty means the server keeps answering without deciding
	// about some items. They retry on their own after the next upgrade and
	// daily, so this is degraded rather than critical: it is a server-side
	// inconsistency to chase, not data an operator must rescue by hand.
	KindParkedNonEmpty = "parked_nonempty"
	// KindNeverDelivered means this agent has never successfully shipped
	// anything. Absence-alerting cannot catch this: a client that never reported
	// once produces no series to be absent from.
	KindNeverDelivered = "never_delivered"
	// KindCaptureStale means the harness is being used and we are recording none
	// of it: hooks that were never registered, were rewritten by a harness
	// upgrade, or that point at a binary which has since moved.
	//
	// This is the one install failure that is otherwise completely invisible.
	// never_delivered covers "events exist but cannot reach the server"; this
	// covers "no events are being produced at all", where the agent is healthy,
	// online, and reporting a clean zero for everything. From the fleet's point
	// of view such a machine is indistinguishable from one whose owner is on
	// holiday, which is exactly why it needs a name.
	KindCaptureStale = "capture_stale"
	// KindSpoolNearCap means the outbox is approaching its byte ceiling. The
	// spool evicts OLDEST-first on overflow, so this is the last moment at which
	// an operator can act before data begins disappearing; drops_recorded is the
	// same event observed too late to prevent.
	KindSpoolNearCap = "spool_near_cap"
	// KindToolNeedsPath means a harness is installed but its sessions were not
	// found: a silent coverage hole. Informational: a binary sitting unused on
	// a laptop is a fact about the laptop, not a malfunction of this agent, and
	// degraded is reserved for conditions where something is actively failing.
	KindToolNeedsPath = "tool_needs_path"
	// KindDropsRecorded means the spool discarded data, with the reason. Only
	// a reason that is a loss raises it; see NotLosses.
	KindDropsRecorded = "drops_recorded"
	// KindPaused means the user turned capture off. Informational by design.
	KindPaused = "paused"
	// KindSpoolUnreadable means spool statistics could not be read, so the
	// backlog conditions above cannot be evaluated at all.
	KindSpoolUnreadable = "spool_unreadable"
	// KindDiskUnknown means free space could not be measured, so capture_blocked
	// cannot be evaluated.
	KindDiskUnknown = "disk_unknown"
	// KindClockSkew means the machine's clock moved backwards relative to
	// recorded timestamps, which silently corrupts every duration in this report.
	KindClockSkew = "clock_skew"
)

// Report is one health sample. Everything the fleet knows about a machine.
type Report struct {
	// Identity and provenance.
	SchemaVersion int    `json:"schema_version"`
	AgentVersion  string `json:"agent_version,omitempty"`
	// AgentCommit is the full VCS revision the binary was built from, read
	// off the build info the Go toolchain stamps; AgentBuildDate is the commit
	// time or the ldflags build date. Together they are what lets the fleet
	// compare a machine with the manifest the release host publishes: the
	// short version string alone ("dev", "e2e-1", "23713ea-dirty") cannot be
	// ordered or matched.
	AgentCommit    string `json:"agent_commit,omitempty"`
	AgentBuildDate string `json:"agent_build_date,omitempty"`
	// AgentDirty reports a build from a modified tree. Such a machine is
	// "unmanaged": it never counts as lagging, because no published build is
	// its target.
	AgentDirty bool `json:"agent_dirty,omitempty"`
	// CaptureSchemaVersion is the extraction version this machine's history was
	// last walked under, from its config.
	CaptureSchemaVersion int `json:"capture_schema_version,omitempty"`
	// Channel is the release channel the self-upgrade follows.
	Channel string `json:"channel,omitempty"`
	// Upgrade is the outcome of the most recent upgrade check, so the fleet
	// can say "cannot reach the release host since Tuesday" instead of
	// "stale".
	Upgrade *UpgradeStatus `json:"upgrade,omitempty"`

	Hostname  string    `json:"hostname,omitempty"`
	OS        string    `json:"os,omitempty"`
	Arch      string    `json:"arch,omitempty"`
	EmittedAt time.Time `json:"emitted_at"`

	// Liveness.
	StartedAt   time.Time     `json:"started_at,omitempty"`
	Uptime      time.Duration `json:"uptime"`
	LastSuccess time.Time     `json:"last_success,omitempty"`
	LastError   string        `json:"last_error,omitempty"`

	// Work state.
	Spool     SpoolHealth      `json:"spool"`
	Sessions  SessionHealth    `json:"sessions"`
	Discovery DiscoverySummary `json:"discovery"`
	// EmptyStarts24h counts sessions on this machine that started and ended in
	// the last day without a single prompt: the signature of a script running
	// `claude` non-interactively with no input. Per machine because the fix is
	// per machine: somebody's automation, not the fleet's.
	EmptyStarts24h int `json:"empty_starts_24h,omitempty"`

	// Environment.
	Disk            DiskHealth `json:"disk"`
	HarnessVersions []string   `json:"harness_versions,omitempty"`

	// Posture the user chose.
	Paused       bool      `json:"paused,omitempty"`
	PausedSince  time.Time `json:"paused_since,omitempty"`
	SkippedTools []string  `json:"skipped_tools,omitempty"`

	// IsRedacted marks a report that has been through Redacted. Without it a
	// redacted and an unredacted report are indistinguishable on the wire, which
	// is how a dashboard shared beyond the fleet operators ends up silently
	// rendering real hostnames.
	//
	// Named IsRedacted rather than Redacted because a field and a method cannot
	// share a name on the same type; the JSON key is the one that matters.
	IsRedacted bool `json:"redacted,omitempty"`

	// Problems, ranked worst-first.
	Conditions []Condition `json:"conditions,omitempty"`
}

// UpgradeStatus is what the last upgrade check concluded. Result is one of
// ok (already on the published build), stale (published differs; not
// replaced), upgraded (replaced), skipped (a guard stopped it; SkippedReason
// says which), error (the check itself failed; Error says how), or "" when no
// check has run on this machine yet.
type UpgradeStatus struct {
	LastCheckAt   time.Time `json:"last_check_at,omitempty"`
	Result        string    `json:"result,omitempty"`
	SkippedReason string    `json:"skipped_reason,omitempty"`
	PublishedSHA  string    `json:"published_sha,omitempty"`
	Error         string    `json:"error,omitempty"`
}

// Condition is one derived problem.
type Condition struct {
	Level  Level     `json:"level"`
	Kind   string    `json:"kind"`
	Detail string    `json:"detail,omitempty"`
	Since  time.Time `json:"since,omitempty"`
}

// SpoolHealth mirrors the spool's own statistics.
type SpoolHealth struct {
	Pending    int            `json:"pending"`
	Quarantine int            `json:"quarantine"`
	Parked     int            `json:"parked,omitempty"`
	Bytes      int64          `json:"bytes"`
	OldestAge  time.Duration  `json:"oldest_age"`
	Dropped    map[string]int `json:"dropped,omitempty"`
}

// SessionHealth counts sessions rather than naming them: a session id here would
// be both high-cardinality and a direct link to someone's work.
type SessionHealth struct {
	Active             int `json:"active"`
	AbandonedRecovered int `json:"abandoned_recovered"`
}

// DiskHealth describes the filesystem holding the spool. ThresholdBytes is the
// free-space floor the spool refuses writes under on this disk, so the fleet
// can show "9.2 GB free, floor 4 GB" rather than a ratio nobody can act on.
type DiskHealth struct {
	FreeBytes      uint64  `json:"free_bytes"`
	TotalBytes     uint64  `json:"total_bytes"`
	FreeRatio      float64 `json:"free_ratio"`
	ThresholdBytes uint64  `json:"threshold_bytes,omitempty"`
}

// DiscoverySummary is counts only. The discovery findings themselves carry
// filesystem paths, which must not travel.
type DiscoverySummary struct {
	ToolsFound       int `json:"tools_found"`
	ToolsNeedingPath int `json:"tools_needing_path"`
	SessionsSeen     int `json:"sessions_seen"`
}

// Thresholds tune condition derivation. Zero values take the defaults below.
type Thresholds struct {
	// DiskGuard is the spool's write policy. The zero value is the spool's
	// default policy; a caller with a spool handle passes spool.Guard() so a
	// per-machine ratio override is judged the way the spool judges it. It is
	// the spool's own type on purpose: the three copies of a ratio that used
	// to live here, in the spool and in the config could disagree, and did.
	DiskGuard spool.Guard
	// BacklogStalled is how old the oldest pending item must be, alongside a
	// dead delivery path, before the backlog counts as stalled.
	BacklogStalled time.Duration
	// DeliveryStale is how long since the last successful delivery before
	// delivery is considered dead.
	DeliveryStale time.Duration
	// EnrollmentGrace is how long a fresh agent gets to complete its first
	// delivery before never_delivered fires.
	EnrollmentGrace time.Duration
	// BacklogWarnItems is the pending count above which a backlog is called
	// growing, even while delivery still succeeds.
	BacklogWarnItems int
	// CaptureStaleGrace is how long an agent runs before it can be accused of
	// capturing nothing. It exists so a freshly started agent, which has had no
	// opportunity to spool anything yet, is not reported as broken.
	CaptureStaleGrace time.Duration
	// SpoolMaxBytes is the spool's byte ceiling, needed to say how close to it
	// we are. Defaults to the spool's own default.
	SpoolMaxBytes int64
	// SpoolNearCapRatio is the fraction of SpoolMaxBytes at which the outbox is
	// called nearly full.
	SpoolNearCapRatio float64
}

// Defaults, each chosen against a real constant elsewhere in the client rather
// than picked to look round.
const (
	// The drain's backoff ceiling is 5 minutes, so a backlog whose oldest item
	// is an hour old has survived roughly twelve maximum-length retry waits.
	// That is no longer a slow network.
	defaultBacklogStalled = time.Hour
	// The drain polls every 5 seconds. Fifteen minutes without a single success
	// is ~180 consecutive failed cycles.
	defaultDeliveryStale = 15 * time.Minute
	// Long enough that a machine mid-boot is not accused of a broken
	// enrollment, short enough that a genuinely broken install surfaces within
	// the same working session.
	defaultEnrollmentGrace = 15 * time.Minute
	// An idle laptop sits near zero pending. Four figures means arrivals are
	// outrunning deliveries.
	defaultBacklogWarnItems = 1000
	// Two hours spans a long meeting plus lunch. The inferential signal already
	// requires session activity to have RISEN, so a genuinely idle afternoon
	// produces no rise and therefore no condition; this grace only protects the
	// startup window, before there is any baseline to compare against.
	defaultCaptureStaleGrace = 2 * time.Hour
	// Matches spool.defaultMaxBytes.
	defaultSpoolMaxBytes = 2 << 30
	// Four fifths full. Early enough to act on a laptop that may be offline for
	// days, late enough not to nag about a routine backlog.
	defaultSpoolNearCapRatio = 0.8
)

// DefaultThresholds returns the values Build uses when a caller supplies none.
//
// It exists so that a caller who needs to phrase one of these judgements for a
// person ("nothing has uploaded for fifteen minutes") reads the same number
// this package derives its conditions from. A duplicated constant elsewhere in
// the client would drift, and the two would then disagree about whether the same
// machine is broken.
func DefaultThresholds() Thresholds { return Thresholds{}.withDefaults() }

func (t Thresholds) withDefaults() Thresholds {
	if t.DiskGuard == (spool.Guard{}) {
		t.DiskGuard = spool.DefaultGuard()
	}
	if t.BacklogStalled <= 0 {
		t.BacklogStalled = defaultBacklogStalled
	}
	if t.DeliveryStale <= 0 {
		t.DeliveryStale = defaultDeliveryStale
	}
	if t.EnrollmentGrace <= 0 {
		t.EnrollmentGrace = defaultEnrollmentGrace
	}
	if t.BacklogWarnItems <= 0 {
		t.BacklogWarnItems = defaultBacklogWarnItems
	}
	if t.CaptureStaleGrace <= 0 {
		t.CaptureStaleGrace = defaultCaptureStaleGrace
	}
	if t.SpoolMaxBytes <= 0 {
		t.SpoolMaxBytes = defaultSpoolMaxBytes
	}
	if t.SpoolNearCapRatio <= 0 {
		t.SpoolNearCapRatio = defaultSpoolNearCapRatio
	}
	return t
}

// Inputs is everything Build can use. Every field is optional: a caller that
// cannot supply something leaves it zero and the corresponding gap is reported
// as a condition.
type Inputs struct {
	AgentVersion string
	// Build identity; see Report. BuildInfo reads them off the binary.
	AgentCommit    string
	AgentBuildDate time.Time
	AgentDirty     bool
	// CaptureSchemaVersion and Channel come from the config.
	CaptureSchemaVersion int
	Channel              string
	// Upgrade is the persisted outcome of the last upgrade check, if any.
	Upgrade *UpgradeStatus
	// Hostname overrides the OS hostname, which is otherwise looked up.
	Hostname string
	// StartedAt is when this agent process began.
	StartedAt time.Time

	// Spool statistics. SpoolErr set means they could not be read and the
	// numbers must not be trusted.
	Spool    spool.Stats
	SpoolErr error

	// LastSuccess and LastError come from the drain. They are taken as plain
	// values rather than as a drain.Stats so this package does not depend on the
	// uploader; health is read by everything and should pull in as little as
	// possible.
	LastSuccess time.Time
	LastError   error

	// Discovery findings. Only counts are copied out; the paths inside stay here.
	Discovery []discovery.Finding

	ActiveSessions     int
	AbandonedRecovered int
	EmptyStarts24h     int
	HarnessVersions    []string

	Paused       bool
	PausedSince  time.Time
	SkippedTools []string

	// PrevPending, when set, is the pending count from the previous report. It
	// lets backlog_growing be a genuine rate rather than a level.
	//
	// A pointer because zero is a legitimate previous value: an int whose zero
	// value meant "not supplied" would read every fresh caller as "the queue was
	// empty last time" and flag any backlog at all as growing.
	PrevPending *int

	// PrevDropped, when set, is the spool's Dropped map as the previous report
	// carried it. The counters in that map are cumulative for the life of the
	// spool directory, so a level says only that this machine ever lost
	// something: a laptop that filled its disk once in August still reads the
	// same total today, and drops_recorded, raised off the level, degraded it
	// for as long as the directory lives. A rise says data is being lost now,
	// which is what the condition is for.
	//
	// Nil means no previous report is known: a first report, or the first from
	// a new daemon. Nothing is raised then, deliberately. The server compares
	// reports itself for the fleet drop metric and its alert, across processes
	// and across restarts (the rollup's delta), so a rise that straddles a
	// daemon's start is still alerted on; what this condition adds is the
	// machine's own answer to "is capture failing right now", and re-reading a
	// lifetime total as news at every daemon start is the opposite of that.
	PrevDropped map[string]int

	// EventsSpooledSinceLastReport is how many items capture wrote since the
	// previous report. Zero while the harness is in use is the symptom of hooks
	// that are not firing.
	EventsSpooledSinceLastReport int
	// PrevSessionsSeen, when set, is Discovery.SessionsSeen from the previous
	// report. A pointer for the same reason as PrevPending: zero is a legitimate
	// previous value, and treating "unset" as zero would read every first report
	// as evidence that session activity had risen.
	PrevSessionsSeen *int
	// HooksRegistered reports whether the harness settings still contain our
	// hook commands. Nil means it could not be determined, which is common (not
	// every harness exposes its configuration in a form we can read), so its
	// absence must never itself be treated as a failure.
	HooksRegistered *bool

	// SpoolDir is the directory whose filesystem is measured.
	SpoolDir string
	// DiskFree matches the spool's own injectable probe. Nil means unmeasurable,
	// which is itself reported.
	DiskFree func(dir string) (free, total uint64, err error)
	// Now is injectable for tests.
	Now func() time.Time

	Thresholds Thresholds
}

// Build derives a Report. It never fails and never panics, including on a
// completely zero Inputs.
func Build(in Inputs) Report {
	now := time.Now
	if in.Now != nil {
		now = in.Now
	}
	at := now()
	th := in.Thresholds.withDefaults()

	host := in.Hostname
	if host == "" {
		// Errors ignored on purpose: an unknown hostname is a cosmetic gap, and
		// failing the report over it would lose everything else in it.
		host, _ = os.Hostname()
	}

	r := Report{
		SchemaVersion:        SchemaVersion,
		AgentVersion:         in.AgentVersion,
		AgentCommit:          in.AgentCommit,
		AgentDirty:           in.AgentDirty,
		CaptureSchemaVersion: in.CaptureSchemaVersion,
		Channel:              in.Channel,
		Hostname:             host,
		OS:                   runtime.GOOS,
		Arch:                 runtime.GOARCH,
		EmittedAt:            at,
		StartedAt:            in.StartedAt,
		LastSuccess:          in.LastSuccess,
		Sessions: SessionHealth{
			Active:             in.ActiveSessions,
			AbandonedRecovered: in.AbandonedRecovered,
		},
		EmptyStarts24h:  in.EmptyStarts24h,
		HarnessVersions: append([]string(nil), in.HarnessVersions...),
		Paused:          in.Paused,
		PausedSince:     in.PausedSince,
		SkippedTools:    append([]string(nil), in.SkippedTools...),
	}
	if !in.AgentBuildDate.IsZero() {
		r.AgentBuildDate = in.AgentBuildDate.UTC().Format(time.RFC3339)
	}
	if in.Upgrade != nil {
		u := *in.Upgrade
		r.Upgrade = &u
	}
	if in.LastError != nil {
		r.LastError = in.LastError.Error()
	}
	if !in.StartedAt.IsZero() {
		if up := at.Sub(in.StartedAt); up > 0 {
			r.Uptime = up
		}
	}

	if in.SpoolErr == nil {
		r.Spool = SpoolHealth{
			Pending:    in.Spool.Pending,
			Quarantine: in.Spool.Quarantine,
			Parked:     in.Spool.Parked,
			Bytes:      in.Spool.Bytes,
			OldestAge:  in.Spool.OldestAge,
			Dropped:    copyCounts(in.Spool.Dropped),
		}
	}

	r.Discovery = summarize(in.Discovery)
	// Probed once and the outcome threaded through: DiskFree is a syscall the
	// caller supplied, and a report should not invoke it twice.
	disk, diskErr := measureDisk(in, th)
	r.Disk = disk
	r.Conditions = derive(in, r, th, at, diskErr)
	return r
}

// Worst returns the highest level present. A report with nothing to say returns
// LevelInfo rather than an empty string, so callers can compare levels without
// special-casing the healthy machine.
func (r Report) Worst() Level {
	worst := LevelInfo
	for _, c := range r.Conditions {
		if c.Level.rank() > worst.rank() {
			worst = c.Level
		}
	}
	return worst
}

// Redacted returns a copy safe for a dashboard shared beyond the fleet
// operators.
//
// The hostname becomes a stable digest rather than being dropped: a dashboard
// still needs to count distinct machines and follow one machine over time, and
// deleting the field would make both impossible. Error text is scrubbed of paths
// and URLs, which is where infrastructure detail and occasionally a home
// directory leak in.
func (r Report) Redacted() Report {
	out := r
	out.IsRedacted = true
	out.Hostname = hashHost(r.Hostname)
	out.LastError = scrubText(r.LastError)

	out.HarnessVersions = append([]string(nil), r.HarnessVersions...)
	out.SkippedTools = append([]string(nil), r.SkippedTools...)
	out.Spool.Dropped = copyCounts(r.Spool.Dropped)
	if r.Upgrade != nil {
		u := *r.Upgrade
		u.Error = scrubText(u.Error)
		out.Upgrade = &u
	}

	if r.Conditions != nil {
		out.Conditions = make([]Condition, len(r.Conditions))
		copy(out.Conditions, r.Conditions)
		for i := range out.Conditions {
			out.Conditions[i].Detail = scrubText(out.Conditions[i].Detail)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Derivation
// ---------------------------------------------------------------------------

func derive(in Inputs, r Report, th Thresholds, at time.Time, diskErr error) []Condition {
	var cs []Condition
	add := func(level Level, kind, detail string, since time.Time) {
		cs = append(cs, Condition{Level: level, Kind: kind, Detail: detail, Since: since})
	}

	// Capture blocked. The most urgent condition in the system: the spool is
	// refusing writes, so events are being lost at the source, where no retry
	// can recover them. The decision is the spool's own, byte for byte.
	switch {
	case diskErr != nil:
		add(LevelDegraded, KindDiskUnknown,
			"free space could not be measured, so capture_blocked cannot be evaluated", at)
	default:
		if ok, threshold := th.DiskGuard.Allowed(r.Disk.FreeBytes, r.Disk.TotalBytes); !ok {
			detail := fmt.Sprintf("%d bytes free is below the %d byte spool write floor; capture is dropping events",
				r.Disk.FreeBytes, threshold)
			add(LevelCritical, KindCaptureBlocked, detail, at)
		}
	}

	if in.SpoolErr != nil {
		add(LevelDegraded, KindSpoolUnreadable,
			"spool statistics unavailable: "+in.SpoolErr.Error(), at)
	} else {
		cs = append(cs, spoolConditions(in, r, th, at)...)
	}

	// Never delivered. Distinct from a stalled backlog because the remedy is
	// different: this is an enrollment or configuration failure, not an outage.
	if in.LastSuccess.IsZero() && r.Uptime > th.EnrollmentGrace {
		since := in.StartedAt
		if since.IsZero() {
			since = at
		}
		detail := fmt.Sprintf("no successful delivery in %s of uptime", r.Uptime.Round(time.Second))
		level := LevelDegraded
		// Escalation. A machine that has never delivered AND is sitting on an
		// aging backlog is not merely misconfigured: the spool evicts
		// oldest-first once it hits its cap, so this is a countdown to data
		// loss rather than a queue waiting for a fix. Requires trustworthy
		// spool numbers; an unreadable spool cannot justify a page.
		if in.SpoolErr == nil && r.Spool.Pending > 0 && r.Spool.OldestAge > th.BacklogStalled {
			level = LevelCritical
			detail = fmt.Sprintf(
				"no successful delivery in %s of uptime, with %d items queued and the oldest %s old; the spool evicts oldest-first at its cap",
				r.Uptime.Round(time.Second), r.Spool.Pending, r.Spool.OldestAge.Round(time.Second))
		}
		add(level, KindNeverDelivered, detail, since)
	}

	// Capture stale. Deliberately degraded rather than critical: a critical that
	// fires on every machine whose owner simply did not code today would teach
	// people to ignore the whole system, and a signal nobody reads is worse than
	// no signal.
	if kind, detail, ok := captureStale(in, r, th); ok {
		add(LevelDegraded, kind, detail, at)
	}

	// Coverage hole: a harness is installed but we could not find its sessions.
	// Info, not degraded. Half the fleet has a gemini_cli or windsurf binary it
	// never opens, and painting every such machine yellow made the fleet page
	// read as two-thirds broken while nothing was failing anywhere. The hole is
	// still worth naming (it is the difference between "capturing everything"
	// and "capturing everything they use") but degraded means an error is
	// happening now, and no error is happening here.
	if n := r.Discovery.ToolsNeedingPath; n > 0 {
		add(LevelInfo, KindToolNeedsPath,
			fmt.Sprintf("%s installed but no sessions located", strings.Join(needPathTools(in.Discovery), ", ")), at)
	}

	// The user turned it off. Deliberately info: a user exercising a control we
	// advertised is not a fault, and a monitoring system that pages on it
	// teaches people to ignore it.
	if in.Paused {
		since := in.PausedSince
		if since.IsZero() {
			since = at
		}
		add(LevelInfo, KindPaused, "capture paused by the user", since)
	}

	// A backwards clock silently corrupts every duration above.
	if !in.StartedAt.IsZero() && in.StartedAt.After(at) {
		add(LevelDegraded, KindClockSkew,
			fmt.Sprintf("start time is %s in the future; durations in this report are unreliable",
				in.StartedAt.Sub(at).Round(time.Second)), at)
	}

	sortConditions(cs)
	return cs
}

// NotLosses names the counters the spool carries in its Dropped map that are
// not losses. That map is the durable place for "this many of X happened,
// across every process that ever wrote to this directory", and recovery
// reuses it so its count survives the hook that produced it; the comment on
// spool.State.Dropped says as much. Nothing read the distinction, so every
// counter in the map raised drops_recorded and a machine whose recovery pass
// worked read as degraded for it. Measured on a real fleet: more than half
// of the reporting machines were degraded on this condition, and on the
// worst of them nearly every drop in a 48-hour window was a recovery, which
// buried the handful of rows that really were rejected.
//
// The client's own doctor output has classified these three correctly since
// it was written (cmd/loop-sessions/main.go reads this map), so the two
// halves of the same binary disagreed about what a drop is; they now read
// one list.
//
// A counter listed here still travels in the report and still reaches the
// fleet page. It is not a drop, not an alert and not a degradation.
var NotLosses = map[string]bool{
	// Events the recovery pass read back out of a transcript after a hook
	// was abandoned. The data reached the server; the counter says the live
	// path needed help, which is worth knowing and is not a loss.
	"recovered_from_transcript": true,
	// An item the server has left undecided too many times, moved to the
	// parked directory rather than deleted. It is redriven when the binary
	// changes and once a day besides, and it has its own count, its own
	// condition (parked_nonempty) and its own policy, so counting it here
	// too was a double count of something that is still on the machine.
	"parked": true,
	// A stale temp file swept by housekeeping: a write that never completed,
	// so no item was ever queued to lose. The hook whose write it was is the
	// recovery pass's business.
	"tmp_swept": true,
}

// IsLoss reports whether a reason in the spool's Dropped map means data was
// discarded.
func IsLoss(reason string) bool { return !NotLosses[reason] }

// NotLossReasons lists NotLosses in a stable order, for a caller that has to
// name them outside Go. The server's hourly rollup passes them into its SQL
// rather than repeating the list, so the two cannot come to disagree about
// which counters are losses.
func NotLossReasons() []string {
	out := make([]string, 0, len(NotLosses))
	for r := range NotLosses {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// spoolConditions derives everything that depends on trustworthy spool numbers.
func spoolConditions(in Inputs, r Report, th Thresholds, at time.Time) []Condition {
	var cs []Condition
	add := func(level Level, kind, detail string, since time.Time) {
		cs = append(cs, Condition{Level: level, Kind: kind, Detail: detail, Since: since})
	}

	// Quarantine means an operator decision is required: these items will never
	// leave on their own, and redriving them is a deliberate act.
	if q := r.Spool.Quarantine; q > 0 {
		add(LevelCritical, KindQuarantineNonEmpty,
			fmt.Sprintf("%d items permanently undeliverable; needs an operator", q), at)
	}

	// Parked means the server keeps not deciding. The items retry on their own
	// after an upgrade and daily; the condition is for whoever owns the server.
	if p := r.Spool.Parked; p > 0 {
		add(LevelDegraded, KindParkedNonEmpty,
			fmt.Sprintf("%d items parked after repeated undecided verdicts; they redrive automatically", p), at)
	}

	deliveryDead := !in.LastSuccess.IsZero() && at.Sub(in.LastSuccess) > th.DeliveryStale
	backlogOld := r.Spool.OldestAge > th.BacklogStalled

	switch {
	case r.Spool.Pending > 0 && backlogOld && deliveryDead:
		// Stalled requires that delivery once worked. A machine that never
		// delivered is reported as never_delivered instead, so a broken
		// enrollment does not also page as an outage.
		since := at.Add(-r.Spool.OldestAge)
		add(LevelCritical, KindBacklogStalled,
			fmt.Sprintf("%d pending, oldest %s, no delivery for %s",
				r.Spool.Pending, r.Spool.OldestAge.Round(time.Second),
				at.Sub(in.LastSuccess).Round(time.Second)), since)

	case r.Spool.Pending > 0 && backlogGrowing(in, r, th):
		// Growing, not stalled: deliveries are still landing, the queue is just
		// filling faster than it drains.
		detail := fmt.Sprintf("%d pending and rising while delivery still succeeds", r.Spool.Pending)
		if in.PrevPending != nil && r.Spool.Pending > *in.PrevPending {
			detail = fmt.Sprintf("%d pending, up from %d, while delivery still succeeds",
				r.Spool.Pending, *in.PrevPending)
		}
		add(LevelDegraded, KindBacklogGrowing, detail, at)
	}

	// Approaching the cap. Predictive by design: once the spool starts evicting,
	// the data it discarded is gone, and drops_recorded can only report the
	// funeral.
	if th.SpoolMaxBytes > 0 && r.Spool.Bytes > 0 {
		limit := int64(float64(th.SpoolMaxBytes) * th.SpoolNearCapRatio)
		if r.Spool.Bytes >= limit {
			add(LevelDegraded, KindSpoolNearCap, fmt.Sprintf(
				"outbox at %d of %d bytes (%.0f%% of cap); the spool evicts oldest-first beyond it",
				r.Spool.Bytes, th.SpoolMaxBytes,
				100*float64(r.Spool.Bytes)/float64(th.SpoolMaxBytes)), at)
		}
	}

	// Drops are reported per reason, sorted so the output is stable. The reason
	// stays in Detail rather than becoming part of Kind: Kind is the metric
	// label, and it should not grow a new value every time the spool learns a
	// new way to discard something.
	for _, reason := range sortedKeys(r.Spool.Dropped) {
		n := r.Spool.Dropped[reason]
		if n <= 0 || !IsLoss(reason) || in.PrevDropped == nil {
			continue
		}
		prev := in.PrevDropped[reason]
		if n <= prev {
			continue
		}
		add(LevelDegraded, KindDropsRecorded,
			fmt.Sprintf("reason=%s count=%d rose=%d", reason, n, n-prev), at)
	}

	return cs
}

// backlogGrowing is true when the queue is large or measurably rising while
// delivery is still working.
func backlogGrowing(in Inputs, r Report, th Thresholds) bool {
	// A dead delivery path is stalled, not growing; that case is handled above.
	if in.LastSuccess.IsZero() {
		return false
	}
	if in.PrevPending != nil && r.Spool.Pending > *in.PrevPending && r.Spool.Pending > 0 {
		return true
	}
	return r.Spool.Pending >= th.BacklogWarnItems
}

// captureStale decides whether the harness is in use while we record nothing.
//
// Two signals, and neither is level-based. A level test ("sessions exist on
// disk but no events were spooled") would fire on any quiet afternoon, because
// last week's session files are still there. Only evidence of CHANGE, or an
// explicit statement from the harness configuration, can separate "not working
// today" from "not capturing at all".
func captureStale(in Inputs, r Report, th Thresholds) (kind, detail string, ok bool) {
	// Before the grace period there has been no opportunity to capture anything,
	// so silence proves nothing.
	if r.Uptime <= th.CaptureStaleGrace {
		return "", "", false
	}

	// Definitive: the harness configuration no longer contains our hooks. Nil
	// means undeterminable, which is not the same as false and must not fire.
	if in.HooksRegistered != nil && !*in.HooksRegistered {
		return KindCaptureStale,
			"hooks are not registered in the harness configuration; no events can be produced", true
	}

	// Inferential: session activity rose while nothing at all was spooled.
	if in.PrevSessionsSeen != nil && in.EventsSpooledSinceLastReport == 0 {
		if grew := r.Discovery.SessionsSeen - *in.PrevSessionsSeen; grew > 0 {
			return KindCaptureStale, fmt.Sprintf(
				"%d new harness sessions since the last report but nothing captured; hooks are likely not firing",
				grew), true
		}
	}
	return "", "", false
}

func needPathTools(fs []discovery.Finding) []string {
	var out []string
	for _, f := range fs {
		if f.State == discovery.Installed && !f.Skipped {
			out = append(out, string(f.Tool))
		}
	}
	sort.Strings(out)
	return out
}

func summarize(fs []discovery.Finding) DiscoverySummary {
	var s DiscoverySummary
	for _, f := range fs {
		switch {
		case f.State == discovery.Found:
			s.ToolsFound++
		case f.State == discovery.Installed && !f.Skipped:
			s.ToolsNeedingPath++
		}
		s.SessionsSeen += f.Sessions
	}
	return s
}

// measureDisk probes the spool filesystem. A nil probe or an error yields a zero
// DiskHealth and a non-nil error, which derive turns into disk_unknown.
func measureDisk(in Inputs, th Thresholds) (DiskHealth, error) {
	if in.DiskFree == nil {
		return DiskHealth{}, errNoDiskProbe
	}
	free, total, err := in.DiskFree(in.SpoolDir)
	if err != nil {
		return DiskHealth{}, err
	}
	if total == 0 {
		return DiskHealth{}, errNoDiskProbe
	}
	return DiskHealth{
		FreeBytes:      free,
		TotalBytes:     total,
		FreeRatio:      float64(free) / float64(total),
		ThresholdBytes: th.DiskGuard.Threshold(total),
	}, nil
}

var errNoDiskProbe = fmt.Errorf("health: no usable disk probe")

// sortConditions ranks worst-first, then by kind so equal-severity output is
// stable across reports and JSON round-trips.
func sortConditions(cs []Condition) {
	sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].Level.rank() != cs[j].Level.rank() {
			return cs[i].Level.rank() > cs[j].Level.rank()
		}
		if cs[i].Kind != cs[j].Kind {
			return cs[i].Kind < cs[j].Kind
		}
		return cs[i].Detail < cs[j].Detail
	})
}

// ---------------------------------------------------------------------------
// Redaction helpers
// ---------------------------------------------------------------------------

// hashHost turns a hostname into a stable, non-reversible identifier. The digest
// is truncated to 12 hex characters: enough to keep a fleet of thousands
// collision-free, short enough to read in a dashboard legend.
func hashHost(h string) string {
	if h == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(h))
	return "h-" + hex.EncodeToString(sum[:])[:12]
}

// scrubText removes path- and URL-shaped tokens from free text.
//
// Error strings are the one place in this report where arbitrary text survives,
// and they routinely carry a URL, an internal IP or a home directory. Replacing
// whole tokens keeps the useful part (the error class) legible.
func scrubText(s string) string {
	if s == "" {
		return s
	}
	fields := strings.Fields(s)
	for i, f := range fields {
		if looksIdentifying(f) {
			fields[i] = "[redacted]"
		}
	}
	return strings.Join(fields, " ")
}

func looksIdentifying(tok string) bool {
	trimmed := strings.Trim(tok, `"'`+"`,;:()[]{}")
	switch {
	case trimmed == "":
		return false
	case strings.Contains(trimmed, "://"):
		return true
	case strings.HasPrefix(trimmed, "/") || strings.HasPrefix(trimmed, "~/"):
		return true
	case strings.HasPrefix(trimmed, `\\`) || (len(trimmed) > 2 && trimmed[1] == ':' && trimmed[2] == '\\'):
		return true
	}
	// A bare host:port or address, as connection errors report them.
	if host, _, err := net.SplitHostPort(trimmed); err == nil && net.ParseIP(host) != nil {
		return true
	}
	return net.ParseIP(trimmed) != nil
}

func copyCounts(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sortedKeys(m map[string]int) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
