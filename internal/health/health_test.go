package health

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/discovery"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

var now = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return now.Add(d) }

func clock() func() time.Time { return func() time.Time { return now } }

// healthyDisk reports a filesystem with plenty of room.
func healthyDisk() func(string) (uint64, uint64, error) {
	return func(string) (uint64, uint64, error) { return 500 << 30, 1000 << 30, nil }
}

// fullDisk is a laptop with 300 MiB free of 460 GiB: under the 512 MiB floor the
// spool refuses writes at. (9.5 GiB free of 460, the state this file was first
// written on, is now allowed: that machine dropped 17,340 events to a 5% ratio
// while its owner had tens of gigabytes to spare.)
func fullDisk() func(string) (uint64, uint64, error) {
	return func(string) (uint64, uint64, error) { return 300 << 20, 460 << 30, nil }
}

func brokenDisk() func(string) (uint64, uint64, error) {
	return func(string) (uint64, uint64, error) { return 0, 0, errors.New("statfs: permission denied") }
}

// base is a healthy machine: delivering, empty spool, roomy disk.
func base() Inputs {
	return Inputs{
		AgentVersion: "0.1.0",
		Hostname:     "laptop-01",
		StartedAt:    at(-2 * time.Hour),
		LastSuccess:  at(-30 * time.Second),
		SpoolDir:     "/spool",
		DiskFree:     healthyDisk(),
		Now:          clock(),
	}
}

func intp(n int) *int { return &n }

func boolp(b bool) *bool { return &b }

// kinds returns the condition kinds present, sorted.
func kinds(r Report) []string {
	out := make([]string, 0, len(r.Conditions))
	for _, c := range r.Conditions {
		out = append(out, c.Kind)
	}
	sort.Strings(out)
	return out
}

func hasKind(r Report, kind string) bool {
	for _, c := range r.Conditions {
		if c.Kind == kind {
			return true
		}
	}
	return false
}

func condition(r Report, kind string) (Condition, bool) {
	for _, c := range r.Conditions {
		if c.Kind == kind {
			return c, true
		}
	}
	return Condition{}, false
}

// ---------------------------------------------------------------------------
// Condition derivation
// ---------------------------------------------------------------------------

func TestConditionDerivation(t *testing.T) {
	cases := []struct {
		name string
		in   func(Inputs) Inputs
		// want lists kinds that MUST be present.
		want []string
		// absent lists kinds that must NOT be present.
		absent []string
		// level, when set, is the expected Worst().
		level Level
	}{
		{
			name:   "healthy machine reports nothing",
			in:     func(i Inputs) Inputs { return i },
			want:   nil,
			absent: []string{KindCaptureBlocked, KindBacklogStalled, KindBacklogGrowing, KindQuarantineNonEmpty, KindNeverDelivered, KindToolNeedsPath, KindDropsRecorded, KindPaused, KindDiskUnknown, KindSpoolUnreadable, KindClockSkew},
			level:  LevelInfo,
		},
		{
			// The live state of the machine this was written on.
			name: "disk below the spool write threshold is critical",
			in: func(i Inputs) Inputs {
				i.DiskFree = fullDisk()
				return i
			},
			want:  []string{KindCaptureBlocked},
			level: LevelCritical,
		},
		{
			name: "disk just above the threshold is fine",
			in: func(i Inputs) Inputs {
				// One byte over the 4 GiB cap that a 460 GiB disk resolves to.
				i.DiskFree = func(string) (uint64, uint64, error) { return 4<<30 + 1, 460 << 30, nil }
				return i
			},
			absent: []string{KindCaptureBlocked},
			level:  LevelInfo,
		},
		{
			name: "unmeasurable disk is degraded, not silent",
			in: func(i Inputs) Inputs {
				i.DiskFree = brokenDisk()
				return i
			},
			want:   []string{KindDiskUnknown},
			absent: []string{KindCaptureBlocked},
			level:  LevelDegraded,
		},
		{
			name: "missing disk probe is also reported",
			in: func(i Inputs) Inputs {
				i.DiskFree = nil
				return i
			},
			want:  []string{KindDiskUnknown},
			level: LevelDegraded,
		},
		{
			name: "old backlog with dead delivery is stalled",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Pending: 42, OldestAge: 3 * time.Hour}
				i.LastSuccess = at(-2 * time.Hour)
				return i
			},
			want:   []string{KindBacklogStalled},
			absent: []string{KindBacklogGrowing, KindNeverDelivered},
			level:  LevelCritical,
		},
		{
			name: "old backlog but delivery still working is not stalled",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Pending: 42, OldestAge: 3 * time.Hour}
				i.LastSuccess = at(-10 * time.Second)
				return i
			},
			absent: []string{KindBacklogStalled},
		},
		{
			name: "dead delivery but young backlog is not stalled",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Pending: 42, OldestAge: time.Minute}
				i.LastSuccess = at(-2 * time.Hour)
				return i
			},
			absent: []string{KindBacklogStalled},
		},
		{
			name: "large backlog with working delivery is growing, degraded",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Pending: 5000, OldestAge: time.Minute}
				return i
			},
			want:   []string{KindBacklogGrowing},
			absent: []string{KindBacklogStalled},
			level:  LevelDegraded,
		},
		{
			name: "rising backlog is growing even below the absolute floor",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Pending: 20}
				i.PrevPending = intp(5)
				return i
			},
			want:  []string{KindBacklogGrowing},
			level: LevelDegraded,
		},
		{
			name: "shrinking backlog is not growing",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Pending: 5}
				i.PrevPending = intp(50)
				return i
			},
			absent: []string{KindBacklogGrowing},
		},
		{
			name: "quarantine needs an operator",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Quarantine: 3}
				return i
			},
			want:  []string{KindQuarantineNonEmpty},
			level: LevelCritical,
		},
		{
			name: "never delivered past the grace period",
			in: func(i Inputs) Inputs {
				i.LastSuccess = time.Time{}
				i.StartedAt = at(-time.Hour)
				return i
			},
			want:   []string{KindNeverDelivered},
			absent: []string{KindBacklogStalled},
			level:  LevelDegraded,
		},
		{
			name: "never delivered inside the grace period stays quiet",
			in: func(i Inputs) Inputs {
				i.LastSuccess = time.Time{}
				i.StartedAt = at(-time.Minute)
				return i
			},
			absent: []string{KindNeverDelivered},
			level:  LevelInfo,
		},
		{
			// A broken enrollment must not also page as a separate outage, but an
			// undelivered backlog against oldest-first eviction is a countdown to
			// data loss, so the one condition escalates rather than two firing.
			name: "never delivered with an old backlog escalates to critical",
			in: func(i Inputs) Inputs {
				i.LastSuccess = time.Time{}
				i.StartedAt = at(-24 * time.Hour)
				i.Spool = spool.Stats{Pending: 900, OldestAge: 20 * time.Hour}
				return i
			},
			want:   []string{KindNeverDelivered},
			absent: []string{KindBacklogStalled, KindBacklogGrowing},
			level:  LevelCritical,
		},
		{
			name: "never delivered with no backlog stays degraded",
			in: func(i Inputs) Inputs {
				i.LastSuccess = time.Time{}
				i.StartedAt = at(-24 * time.Hour)
				return i
			},
			want:  []string{KindNeverDelivered},
			level: LevelDegraded,
		},
		{
			name: "never delivered with a young backlog stays degraded",
			in: func(i Inputs) Inputs {
				i.LastSuccess = time.Time{}
				i.StartedAt = at(-24 * time.Hour)
				i.Spool = spool.Stats{Pending: 900, OldestAge: time.Minute}
				return i
			},
			want:  []string{KindNeverDelivered},
			level: LevelDegraded,
		},
		{
			name: "never delivered does not escalate on spool numbers we cannot read",
			in: func(i Inputs) Inputs {
				i.LastSuccess = time.Time{}
				i.StartedAt = at(-24 * time.Hour)
				i.SpoolErr = errors.New("readdir: permission denied")
				i.Spool = spool.Stats{Pending: 900, OldestAge: 20 * time.Hour}
				return i
			},
			want:  []string{KindNeverDelivered, KindSpoolUnreadable},
			level: LevelDegraded,
		},

		// --- capture_stale -------------------------------------------------
		{
			name: "harness used but nothing captured is capture_stale",
			in: func(i Inputs) Inputs {
				i.StartedAt = at(-6 * time.Hour)
				i.Discovery = []discovery.Finding{{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 40}}
				i.PrevSessionsSeen = intp(31)
				i.EventsSpooledSinceLastReport = 0
				return i
			},
			want:  []string{KindCaptureStale},
			level: LevelDegraded,
		},
		{
			name: "hooks explicitly absent is capture_stale on its own",
			in: func(i Inputs) Inputs {
				i.StartedAt = at(-6 * time.Hour)
				i.HooksRegistered = boolp(false)
				return i
			},
			want:  []string{KindCaptureStale},
			level: LevelDegraded,
		},
		{
			name: "hooks present is not capture_stale",
			in: func(i Inputs) Inputs {
				i.StartedAt = at(-6 * time.Hour)
				i.HooksRegistered = boolp(true)
				i.Discovery = []discovery.Finding{{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 40}}
				i.PrevSessionsSeen = intp(40)
				return i
			},
			absent: []string{KindCaptureStale},
			level:  LevelInfo,
		},
		{
			// The whole point: an idle afternoon must not look like a broken install.
			name: "quiet period with no new sessions is not capture_stale",
			in: func(i Inputs) Inputs {
				i.StartedAt = at(-6 * time.Hour)
				i.Discovery = []discovery.Finding{{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 40}}
				i.PrevSessionsSeen = intp(40)
				i.EventsSpooledSinceLastReport = 0
				return i
			},
			absent: []string{KindCaptureStale},
			level:  LevelInfo,
		},
		{
			name: "sessions rising while events are captured is healthy",
			in: func(i Inputs) Inputs {
				i.StartedAt = at(-6 * time.Hour)
				i.Discovery = []discovery.Finding{{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 40}}
				i.PrevSessionsSeen = intp(31)
				i.EventsSpooledSinceLastReport = 512
				return i
			},
			absent: []string{KindCaptureStale},
			level:  LevelInfo,
		},
		{
			// Undeterminable is not the same as false.
			name: "unknown hook state alone never fires",
			in: func(i Inputs) Inputs {
				i.StartedAt = at(-6 * time.Hour)
				i.HooksRegistered = nil
				i.Discovery = []discovery.Finding{{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 40}}
				return i
			},
			absent: []string{KindCaptureStale},
			level:  LevelInfo,
		},
		{
			// Without a baseline there is no evidence of change, and a level test
			// would fire on last week's session files.
			name: "no previous sample means no inference",
			in: func(i Inputs) Inputs {
				i.StartedAt = at(-6 * time.Hour)
				i.Discovery = []discovery.Finding{{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 40}}
				i.PrevSessionsSeen = nil
				i.EventsSpooledSinceLastReport = 0
				return i
			},
			absent: []string{KindCaptureStale},
			level:  LevelInfo,
		},

		// --- spool_near_cap ------------------------------------------------
		{
			name: "outbox approaching its cap is reported before data is lost",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Bytes: 1_800_000_000}
				return i
			},
			want:   []string{KindSpoolNearCap},
			absent: []string{KindDropsRecorded},
			level:  LevelDegraded,
		},
		{
			name: "a modest outbox is not near the cap",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Bytes: 10 << 20}
				return i
			},
			absent: []string{KindSpoolNearCap},
			level:  LevelInfo,
		},
		{
			name: "a custom cap is honoured",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Bytes: 90 << 20}
				i.Thresholds = Thresholds{SpoolMaxBytes: 100 << 20}
				return i
			},
			want:  []string{KindSpoolNearCap},
			level: LevelDegraded,
		},
		{
			name: "near-cap is suppressed when spool numbers are unreadable",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Bytes: 1_800_000_000}
				i.SpoolErr = errors.New("readdir: permission denied")
				return i
			},
			absent: []string{KindSpoolNearCap},
			want:   []string{KindSpoolUnreadable},
		},
		{
			name: "installed tool with no sessions found is a coverage hole, not a fault",
			in: func(i Inputs) Inputs {
				i.Discovery = []discovery.Finding{
					{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 10, Root: "/home/u/.claude/projects"},
					{Tool: discovery.Codex, State: discovery.Installed},
				}
				return i
			},
			want: []string{KindToolNeedsPath},
			// Info, never degraded: an unused binary on the laptop is not an
			// error happening now, and it must not turn the machine yellow on
			// the fleet page.
			level: LevelInfo,
		},
		{
			name: "a tool the user skipped is not a coverage hole",
			in: func(i Inputs) Inputs {
				i.Discovery = []discovery.Finding{
					{Tool: discovery.Codex, State: discovery.Installed, Skipped: true},
				}
				return i
			},
			absent: []string{KindToolNeedsPath},
			level:  LevelInfo,
		},
		{
			name: "an absent tool is not a coverage hole",
			in: func(i Inputs) Inputs {
				i.Discovery = []discovery.Finding{
					{Tool: discovery.Aider, State: discovery.Absent},
				}
				return i
			},
			absent: []string{KindToolNeedsPath},
		},
		{
			name: "drops are reported",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Dropped: map[string]int{"disk_full": 12}}
				i.PrevDropped = map[string]int{}
				return i
			},
			want:  []string{KindDropsRecorded},
			level: LevelDegraded,
		},
		{
			name: "zero-valued drop counters do not fire",
			in: func(i Inputs) Inputs {
				i.Spool = spool.Stats{Dropped: map[string]int{"disk_full": 0, "corrupt": 0}}
				i.PrevDropped = map[string]int{}
				return i
			},
			absent: []string{KindDropsRecorded},
			level:  LevelInfo,
		},
		{
			// A user exercising a control we advertised is not a fault.
			name: "paused is info, never degraded",
			in: func(i Inputs) Inputs {
				i.Paused = true
				i.PausedSince = at(-time.Hour)
				return i
			},
			want:  []string{KindPaused},
			level: LevelInfo,
		},
		{
			name: "unreadable spool is degraded and suppresses backlog claims",
			in: func(i Inputs) Inputs {
				i.SpoolErr = errors.New("readdir: permission denied")
				i.Spool = spool.Stats{Pending: 99, Quarantine: 5}
				return i
			},
			want:   []string{KindSpoolUnreadable},
			absent: []string{KindQuarantineNonEmpty, KindBacklogGrowing, KindBacklogStalled, KindDropsRecorded},
			level:  LevelDegraded,
		},
		{
			name: "a clock in the future is reported",
			in: func(i Inputs) Inputs {
				i.StartedAt = at(time.Hour)
				return i
			},
			want:  []string{KindClockSkew},
			level: LevelDegraded,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := Build(tc.in(base()))

			for _, k := range tc.want {
				if !hasKind(r, k) {
					t.Errorf("missing condition %q; got %v", k, kinds(r))
				}
			}
			for _, k := range tc.absent {
				if hasKind(r, k) {
					c, _ := condition(r, k)
					t.Errorf("unexpected condition %q (%s); got %v", k, c.Detail, kinds(r))
				}
			}
			if tc.level != "" && r.Worst() != tc.level {
				t.Errorf("Worst() = %q, want %q; conditions %v", r.Worst(), tc.level, kinds(r))
			}
		})
	}
}

// The grace period must suppress a short quiet period outright, whatever the
// other signals say. A freshly started agent has had no chance to capture
// anything, so its silence is not evidence of a broken install.
func TestCaptureStaleGraceSuppressesShortQuietPeriods(t *testing.T) {
	cases := []struct {
		name   string
		uptime time.Duration
		want   bool
	}{
		{"just started", time.Minute, false},
		{"an hour in", time.Hour, false},
		{"just inside the grace period", defaultCaptureStaleGrace - time.Minute, false},
		{"just past the grace period", defaultCaptureStaleGrace + time.Minute, true},
		{"a full day", 24 * time.Hour, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i := base()
			i.StartedAt = at(-tc.uptime)
			// Both signals present and screaming; only the grace decides.
			i.HooksRegistered = boolp(false)
			i.Discovery = []discovery.Finding{{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 40}}
			i.PrevSessionsSeen = intp(10)
			i.EventsSpooledSinceLastReport = 0

			if got := hasKind(Build(i), KindCaptureStale); got != tc.want {
				t.Errorf("capture_stale = %v at %s uptime, want %v", got, tc.uptime, tc.want)
			}
		})
	}
}

// A custom grace must be honoured, so a fleet with a different working rhythm
// can tune it without forking the package.
func TestCaptureStaleGraceConfigurable(t *testing.T) {
	i := base()
	i.StartedAt = at(-10 * time.Minute)
	i.HooksRegistered = boolp(false)
	i.Thresholds = Thresholds{CaptureStaleGrace: time.Minute}

	if !hasKind(Build(i), KindCaptureStale) {
		t.Error("a custom CaptureStaleGrace was not honoured")
	}
}

// Degraded is reserved for active errors. A machine whose only finding is an
// installed-but-unused harness is healthy, and the fleet page must not paint it
// yellow: when this fired at LevelDegraded, two of the six machines in the
// fleet showed degraded over binaries their owners had simply never opened.
func TestUnusedToolNeverDegradesTheMachine(t *testing.T) {
	i := base()
	i.Discovery = []discovery.Finding{
		{Tool: discovery.GeminiCLI, State: discovery.Installed},
		{Tool: discovery.Windsurf, State: discovery.Installed},
	}
	r := Build(i)
	c, ok := condition(r, KindToolNeedsPath)
	if !ok {
		t.Fatal("the coverage hole must still be named")
	}
	if c.Level != LevelInfo {
		t.Errorf("tool_needs_path carries %s; an unused binary is not an active error", c.Level)
	}
	if r.Worst() != LevelInfo {
		t.Errorf("Worst() = %s; an unused binary must not change the machine's badge", r.Worst())
	}
}

// Every declared kind must be reachable. A condition that no input can produce
// is a condition that will never fire in production either.
func TestEveryKindIsReachable(t *testing.T) {
	all := []string{
		KindCaptureBlocked, KindBacklogStalled, KindBacklogGrowing,
		KindQuarantineNonEmpty, KindNeverDelivered, KindToolNeedsPath,
		KindDropsRecorded, KindPaused, KindSpoolUnreadable, KindDiskUnknown,
		KindClockSkew, KindCaptureStale, KindSpoolNearCap,
	}

	seen := map[string]bool{}
	record := func(r Report) {
		for _, c := range r.Conditions {
			seen[c.Kind] = true
		}
	}

	record(Build(func() Inputs { i := base(); i.DiskFree = fullDisk(); return i }()))
	record(Build(func() Inputs {
		i := base()
		i.Spool = spool.Stats{Pending: 5, OldestAge: 3 * time.Hour, Quarantine: 1, Dropped: map[string]int{"corrupt": 1}}
		i.PrevDropped = map[string]int{}
		i.LastSuccess = at(-2 * time.Hour)
		return i
	}()))
	record(Build(func() Inputs { i := base(); i.Spool = spool.Stats{Pending: 5000}; return i }()))
	record(Build(func() Inputs { i := base(); i.LastSuccess = time.Time{}; return i }()))
	record(Build(func() Inputs {
		i := base()
		i.Discovery = []discovery.Finding{{Tool: discovery.Codex, State: discovery.Installed}}
		return i
	}()))
	record(Build(func() Inputs { i := base(); i.Paused = true; return i }()))
	record(Build(func() Inputs { i := base(); i.SpoolErr = errors.New("x"); return i }()))
	record(Build(func() Inputs { i := base(); i.DiskFree = nil; return i }()))
	record(Build(func() Inputs { i := base(); i.StartedAt = at(time.Hour); return i }()))
	record(Build(func() Inputs {
		i := base()
		i.StartedAt = at(-6 * time.Hour)
		i.HooksRegistered = boolp(false)
		return i
	}()))
	record(Build(func() Inputs { i := base(); i.Spool = spool.Stats{Bytes: 1_900_000_000}; return i }()))

	for _, k := range all {
		if !seen[k] {
			t.Errorf("kind %q is declared but no input produced it", k)
		}
	}
}

// ---------------------------------------------------------------------------
// Ranking
// ---------------------------------------------------------------------------

func TestWorstRanking(t *testing.T) {
	cases := []struct {
		name string
		cs   []Condition
		want Level
	}{
		{"no conditions is info", nil, LevelInfo},
		{"info only", []Condition{{Level: LevelInfo, Kind: KindPaused}}, LevelInfo},
		{"degraded beats info", []Condition{
			{Level: LevelInfo, Kind: KindPaused},
			{Level: LevelDegraded, Kind: KindDropsRecorded},
		}, LevelDegraded},
		{"critical beats degraded", []Condition{
			{Level: LevelDegraded, Kind: KindDropsRecorded},
			{Level: LevelCritical, Kind: KindCaptureBlocked},
		}, LevelCritical},
		{"order does not matter", []Condition{
			{Level: LevelCritical, Kind: KindCaptureBlocked},
			{Level: LevelInfo, Kind: KindPaused},
		}, LevelCritical},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := (Report{Conditions: tc.cs}).Worst(); got != tc.want {
				t.Errorf("Worst() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Conditions are ranked worst-first so a reader sees the page-worthy item first.
func TestConditionsSortedWorstFirst(t *testing.T) {
	i := base()
	i.DiskFree = fullDisk()
	i.Paused = true
	i.Spool = spool.Stats{Quarantine: 2, Dropped: map[string]int{"disk_full": 3}}
	i.PrevDropped = map[string]int{}

	r := Build(i)
	if len(r.Conditions) < 3 {
		t.Fatalf("expected several conditions, got %v", kinds(r))
	}
	for idx := 1; idx < len(r.Conditions); idx++ {
		prev, cur := r.Conditions[idx-1].Level.rank(), r.Conditions[idx].Level.rank()
		if cur > prev {
			t.Errorf("condition %d (%s) outranks its predecessor (%s); order %v",
				idx, r.Conditions[idx].Level, r.Conditions[idx-1].Level, kinds(r))
		}
	}
	if r.Conditions[0].Level != LevelCritical {
		t.Errorf("first condition is %q, want critical first", r.Conditions[0].Level)
	}
}

// One condition per drop reason, in a stable order.
func TestDropsReportedPerReasonAndStable(t *testing.T) {
	i := base()
	i.Spool = spool.Stats{Dropped: map[string]int{
		"queue_overflow": 2, "disk_full": 7, "corrupt": 1, "max_attempts": 4,
	}}
	i.PrevDropped = map[string]int{}

	first := Build(i)
	var details []string
	for _, c := range first.Conditions {
		if c.Kind == KindDropsRecorded {
			details = append(details, c.Detail)
		}
	}
	if len(details) != 4 {
		t.Fatalf("got %d drop conditions, want one per reason: %v", len(details), details)
	}
	if !strings.Contains(details[0], "corrupt") {
		t.Errorf("drop conditions not sorted by reason: %v", details)
	}

	// Repeated builds over the same map must not reorder.
	for n := 0; n < 20; n++ {
		var again []string
		for _, c := range Build(i).Conditions {
			if c.Kind == KindDropsRecorded {
				again = append(again, c.Detail)
			}
		}
		if !reflect.DeepEqual(details, again) {
			t.Fatalf("drop condition order is unstable:\n %v\n %v", details, again)
		}
	}
}

// ---------------------------------------------------------------------------
// Robustness
// ---------------------------------------------------------------------------

// Build must produce a usable report from nothing at all. A health system that
// fails when the machine is broken reports nothing exactly when it matters.
func TestBuildNeverPanicsOnEmptyInputs(t *testing.T) {
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("Build panicked on zero Inputs: %v", p)
		}
	}()

	r := Build(Inputs{})

	if r.SchemaVersion != SchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", r.SchemaVersion, SchemaVersion)
	}
	if r.EmittedAt.IsZero() {
		t.Error("EmittedAt is zero; a report must always be timestamped")
	}
	if r.OS == "" || r.Arch == "" {
		t.Errorf("OS/Arch not populated: %q/%q", r.OS, r.Arch)
	}
	if r.Worst() == "" {
		t.Error("Worst() returned an empty level")
	}
	// The one thing an empty input can still tell us is that it could not see
	// the disk.
	if !hasKind(r, KindDiskUnknown) {
		t.Errorf("expected disk_unknown from an empty Inputs; got %v", kinds(r))
	}
}

func TestBuildTolerantOfPartialInputs(t *testing.T) {
	cases := []struct {
		name string
		in   Inputs
	}{
		{"no clock", Inputs{DiskFree: healthyDisk()}},
		{"no disk probe", Inputs{Now: clock()}},
		{"nil dropped map", Inputs{Now: clock(), DiskFree: healthyDisk(), Spool: spool.Stats{Dropped: nil}}},
		{"nil discovery", Inputs{Now: clock(), DiskFree: healthyDisk(), Discovery: nil}},
		{"nil prev pending", Inputs{Now: clock(), DiskFree: healthyDisk(), Spool: spool.Stats{Pending: 3}}},
		{"nil last error", Inputs{Now: clock(), DiskFree: healthyDisk(), LastError: nil}},
		{"zero total bytes", Inputs{Now: clock(), DiskFree: func(string) (uint64, uint64, error) { return 0, 0, nil }}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := Build(tc.in)
			if r.SchemaVersion != SchemaVersion || r.EmittedAt.IsZero() {
				t.Errorf("unusable report: %+v", r)
			}
		})
	}
}

// A backwards clock must not produce a negative uptime.
func TestUptimeNeverNegative(t *testing.T) {
	i := base()
	i.StartedAt = at(time.Hour)
	if got := Build(i).Uptime; got < 0 {
		t.Errorf("Uptime = %s, want >= 0", got)
	}
}

// The caller's disk probe is a syscall; a report must not invoke it twice.
func TestDiskProbeCalledOnce(t *testing.T) {
	var calls int
	i := base()
	i.DiskFree = func(string) (uint64, uint64, error) {
		calls++
		return 500 << 30, 1000 << 30, nil
	}
	Build(i)
	if calls != 1 {
		t.Errorf("DiskFree called %d times, want 1", calls)
	}
}

func TestBuildDoesNotAliasCallerSlicesOrMaps(t *testing.T) {
	dropped := map[string]int{"corrupt": 1}
	tools := []string{"codex"}
	versions := []string{"2.1.221"}

	i := base()
	i.Spool = spool.Stats{Dropped: dropped}
	i.SkippedTools = tools
	i.HarnessVersions = versions

	r := Build(i)
	dropped["corrupt"] = 99
	tools[0] = "mutated"
	versions[0] = "mutated"

	if r.Spool.Dropped["corrupt"] != 1 {
		t.Error("Report aliases the caller's Dropped map")
	}
	if r.SkippedTools[0] != "codex" {
		t.Error("Report aliases the caller's SkippedTools slice")
	}
	if r.HarnessVersions[0] != "2.1.221" {
		t.Error("Report aliases the caller's HarnessVersions slice")
	}
}

// ---------------------------------------------------------------------------
// Privacy
// ---------------------------------------------------------------------------

// Nothing identifying may reach a Report, because these land in metric labels.
func TestReportCarriesNoPathsOrSessionIDs(t *testing.T) {
	i := base()
	i.SpoolDir = "/home/someone/Library/loop-sessions/spool"
	i.Discovery = []discovery.Finding{
		{
			Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 12,
			Root: "/home/someone/.claude/projects",
			Candidates: []discovery.Candidate{
				{Path: "/home/someone/.claude/projects", Exists: true, Sessions: 12},
			},
			InstallEvidence: []string{"/usr/local/bin/claude"},
		},
		{Tool: discovery.Codex, State: discovery.Installed, Root: "/home/someone/.codex"},
	}

	b, err := json.Marshal(Build(i))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(b)

	for _, leak := range []string{"/home/someone", ".claude/projects", "/usr/local/bin", "Library"} {
		if strings.Contains(got, leak) {
			t.Errorf("report leaked %q into the wire form", leak)
		}
	}
}

func TestRedactedRemovesWhatItClaims(t *testing.T) {
	i := base()
	i.Hostname = "alex-laptop.corp.internal"
	i.LastError = errors.New(`Post "https://ingest.example.com/v1/events": dial tcp 10.4.2.19:443: connect: connection refused`)

	r := Build(i)
	red := r.Redacted()

	if red.Hostname == r.Hostname {
		t.Error("Redacted did not change the hostname")
	}
	if strings.Contains(red.Hostname, "alex") || strings.Contains(red.Hostname, "corp") {
		t.Errorf("redacted hostname still identifying: %q", red.Hostname)
	}
	if !strings.HasPrefix(red.Hostname, "h-") {
		t.Errorf("redacted hostname = %q, want an h- prefixed digest", red.Hostname)
	}

	blob, err := json.Marshal(red)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, leak := range []string{"ingest.example.com", "10.4.2.19", "https://"} {
		if strings.Contains(string(blob), leak) {
			t.Errorf("redacted report still contains %q", leak)
		}
	}
	// The useful part of the error must survive, or redaction has destroyed the
	// diagnostic value it was meant to preserve.
	if !strings.Contains(red.LastError, "connection refused") {
		t.Errorf("redaction removed the error class too: %q", red.LastError)
	}
}

// The same hostname must always redact to the same value, or a dashboard cannot
// follow one machine over time.
func TestRedactedHostnameIsStableAndDistinct(t *testing.T) {
	a1 := Report{Hostname: "host-a"}.Redacted().Hostname
	a2 := Report{Hostname: "host-a"}.Redacted().Hostname
	b := Report{Hostname: "host-b"}.Redacted().Hostname

	if a1 != a2 {
		t.Errorf("same host redacted differently: %q vs %q", a1, a2)
	}
	if a1 == b {
		t.Errorf("different hosts collided: %q", a1)
	}
	if got := (Report{Hostname: ""}).Redacted().Hostname; got != "" {
		t.Errorf("empty hostname became %q, want empty", got)
	}
}

// Redacted must not mutate the receiver — a caller that ships both forms would
// otherwise silently lose the unredacted one.
func TestRedactedDoesNotMutateOriginal(t *testing.T) {
	i := base()
	i.Hostname = "laptop-01"
	i.LastError = errors.New("dial https://ingest.example.com failed")
	i.Spool = spool.Stats{Dropped: map[string]int{"corrupt": 1}}
	i.PrevDropped = map[string]int{}

	r := Build(i)
	before, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	_ = r.Redacted()

	after, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(before) != string(after) {
		t.Errorf("Redacted mutated the original:\n before %s\n after  %s", before, after)
	}
}

// A redacted report must say so, or a dashboard shared beyond the fleet
// operators cannot tell which form it is rendering.
func TestRedactedIsMarkedOnTheWire(t *testing.T) {
	r := Build(base())
	if r.IsRedacted {
		t.Error("a freshly built report claims to be redacted")
	}

	red := r.Redacted()
	if !red.IsRedacted {
		t.Error("Redacted() did not mark the report")
	}
	if r.IsRedacted {
		t.Error("Redacted() mutated the original's marker")
	}

	// The unredacted form omits the key; the redacted form carries it.
	plain, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(plain, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["redacted"]; ok {
		t.Error("unredacted report carries a redacted key")
	}

	blob, err := json.Marshal(red)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(blob, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v, ok := m["redacted"]; !ok || v != true {
		t.Errorf("redacted report missing the marker: %v", m["redacted"])
	}

	// Redacting twice must stay redacted rather than flapping.
	if !red.Redacted().IsRedacted {
		t.Error("re-redacting cleared the marker")
	}
}

// The whole justification for spool_near_cap: it fires while drops_recorded is
// still silent, which is the only window in which an operator can prevent loss.
func TestNearCapFiresBeforeAnythingIsDropped(t *testing.T) {
	i := base()
	i.Spool = spool.Stats{Bytes: 1_800_000_000} // 84% of the 2 GiB default cap
	r := Build(i)

	if !hasKind(r, KindSpoolNearCap) {
		t.Fatalf("near-cap did not fire; conditions %v", kinds(r))
	}
	if hasKind(r, KindDropsRecorded) {
		t.Error("drops already recorded; the warning came too late to be useful")
	}

	// And once eviction starts, both are true: the warning does not disappear
	// the moment it is vindicated. The previous report carried no drops, so
	// this is a rise, which is what the condition reports.
	i.PrevDropped = map[string]int{}
	i.Spool.Dropped = map[string]int{"queue_overflow": 4}
	r = Build(i)
	if !hasKind(r, KindSpoolNearCap) || !hasKind(r, KindDropsRecorded) {
		t.Errorf("expected both conditions once eviction begins; got %v", kinds(r))
	}
}

func TestScrubText(t *testing.T) {
	cases := []struct {
		in   string
		keep []string
		gone []string
	}{
		{
			in:   `Post "https://ingest.example.com/v1" failed`,
			keep: []string{"Post", "failed"},
			gone: []string{"ingest.example.com"},
		},
		{
			in:   "open /home/x/.claude/projects: no such file",
			keep: []string{"no such file"},
			gone: []string{"/home/x"},
		},
		{
			in:   "dial tcp 10.4.2.19:443: connect: connection refused",
			keep: []string{"connection refused"},
			gone: []string{"10.4.2.19"},
		},
		{
			in:   "context deadline exceeded",
			keep: []string{"context deadline exceeded"},
		},
		{in: "", keep: nil},
	}
	for _, tc := range cases {
		got := scrubText(tc.in)
		for _, k := range tc.keep {
			if !strings.Contains(got, k) {
				t.Errorf("scrubText(%q) = %q, lost %q", tc.in, got, k)
			}
		}
		for _, g := range tc.gone {
			if strings.Contains(got, g) {
				t.Errorf("scrubText(%q) = %q, still contains %q", tc.in, got, g)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Wire form
// ---------------------------------------------------------------------------

func TestJSONRoundTripStable(t *testing.T) {
	i := base()
	i.DiskFree = fullDisk()
	i.Paused = true
	i.PausedSince = at(-time.Hour)
	i.SkippedTools = []string{"cursor"}
	i.HarnessVersions = []string{"2.1.220", "2.1.221"}
	i.LastError = errors.New("connection refused")
	i.Spool = spool.Stats{
		Pending: 4, Quarantine: 1, Bytes: 9999,
		OldestAge: 90 * time.Minute,
		Dropped:   map[string]int{"disk_full": 3, "corrupt": 1},
	}
	i.Discovery = []discovery.Finding{
		{Tool: discovery.ClaudeCode, State: discovery.Found, Sessions: 12},
		{Tool: discovery.Codex, State: discovery.Installed},
	}
	i.ActiveSessions = 2
	i.AbandonedRecovered = 1

	r := Build(i)

	first, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Report
	if err := json.Unmarshal(first, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	second, err := json.Marshal(back)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if string(first) != string(second) {
		t.Errorf("round trip not stable:\n first  %s\n second %s", first, second)
	}
	if !reflect.DeepEqual(r, back) {
		t.Errorf("decoded report differs from the original\n got  %+v\n want %+v", back, r)
	}

	// Marshalling the same report repeatedly must be byte-identical, or the
	// server cannot dedup or diff consecutive reports.
	for n := 0; n < 20; n++ {
		again, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if string(again) != string(first) {
			t.Fatalf("marshalling is not deterministic:\n %s\n %s", first, again)
		}
	}
}

func TestSchemaVersionAlwaysPresent(t *testing.T) {
	for _, in := range []Inputs{{}, base()} {
		b, err := json.Marshal(Build(in))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		v, ok := m["schema_version"]
		if !ok {
			t.Fatal("schema_version missing from the wire form")
		}
		if int(v.(float64)) != SchemaVersion {
			t.Errorf("schema_version = %v, want %d", v, SchemaVersion)
		}
	}
}

// The report is emitted on a fixed cadence even when nothing happened, so the
// quiet case must still be a complete, decodable document.
func TestQuietMachineStillProducesAFullReport(t *testing.T) {
	r := Build(base())

	if len(r.Conditions) != 0 {
		t.Errorf("healthy machine reported conditions: %v", kinds(r))
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, required := range []string{"schema_version", "emitted_at", "uptime", "spool", "sessions", "discovery", "disk"} {
		if _, ok := m[required]; !ok {
			t.Errorf("quiet report is missing %q", required)
		}
	}
}

func TestThresholdDefaultsApplied(t *testing.T) {
	got := Thresholds{}.withDefaults()
	want := Thresholds{
		DiskGuard:         spool.DefaultGuard(),
		BacklogStalled:    defaultBacklogStalled,
		DeliveryStale:     defaultDeliveryStale,
		EnrollmentGrace:   defaultEnrollmentGrace,
		BacklogWarnItems:  defaultBacklogWarnItems,
		CaptureStaleGrace: defaultCaptureStaleGrace,
		SpoolMaxBytes:     defaultSpoolMaxBytes,
		SpoolNearCapRatio: defaultSpoolNearCapRatio,
	}
	if got != want {
		t.Errorf("defaults = %+v, want %+v", got, want)
	}

	custom := Thresholds{DiskGuard: spool.Guard{MinFreeRatio: 0.2}}.withDefaults()
	if custom.DiskGuard.MinFreeRatio != 0.2 {
		t.Errorf("explicit disk guard overwritten: %+v", custom.DiskGuard)
	}
	if custom.BacklogStalled != defaultBacklogStalled {
		t.Errorf("unset field not defaulted: %v", custom.BacklogStalled)
	}
}

// capture_blocked flips at exactly the byte the spool refuses at, on a large
// disk and a small one. There is no second copy of the threshold to drift:
// health asks the spool's own Guard, so a machine cannot be reported healthy
// at the moment capture is dropping events.
func TestCaptureBlockedUsesSpoolThreshold(t *testing.T) {
	disks := map[string]uint64{"large (1 TiB)": 1 << 40, "small (32 GiB)": 32 << 30}
	for name, total := range disks {
		t.Run(name, func(t *testing.T) {
			_, threshold := spool.WriteAllowed(0, total)
			at := func(free uint64) Report {
				i := base()
				i.DiskFree = func(string) (uint64, uint64, error) { return free, total, nil }
				return Build(i)
			}
			if hasKind(at(threshold), KindCaptureBlocked) {
				t.Errorf("free == threshold (%d) reported capture_blocked; the spool allows that write", threshold)
			}
			if !hasKind(at(threshold-1), KindCaptureBlocked) {
				t.Errorf("free == threshold-1 (%d) not reported; the spool refuses that write", threshold-1)
			}
			if got := at(threshold).Disk.ThresholdBytes; got != threshold {
				t.Errorf("report carries threshold %d, want %d", got, threshold)
			}
		})
	}

	// A per-machine ratio override is judged the way the spool judges it.
	i := base()
	i.Thresholds = Thresholds{DiskGuard: spool.Guard{MinFreeRatio: 0.10}}
	i.DiskFree = func(string) (uint64, uint64, error) { return 1 << 30, 20 << 30, nil } // 5% of 20 GiB
	if !hasKind(Build(i), KindCaptureBlocked) {
		t.Error("a custom disk guard was not honoured")
	}
}

// Parked items are a degraded condition, not a critical one: they retry by
// themselves, and paging a person for a server-side inconsistency that will be
// retried after the next deploy teaches people to ignore the page.
func TestParkedItemsAreDegradedNotCritical(t *testing.T) {
	i := base()
	i.Spool = spool.Stats{Parked: 3}
	r := Build(i)
	c, ok := condition(r, KindParkedNonEmpty)
	if !ok {
		t.Fatalf("parked items produced no condition; kinds: %v", kinds(r))
	}
	if c.Level != LevelDegraded {
		t.Errorf("parked_nonempty is %q, want degraded", c.Level)
	}
	if r.Spool.Parked != 3 {
		t.Errorf("report parked = %d, want 3", r.Spool.Parked)
	}
}

// The version 2 identity fields ride the wire under the names the fleet
// evaluator compares against latest.json.
func TestVersionTwoFieldsAreOnTheWire(t *testing.T) {
	i := base()
	i.AgentCommit = "5b8bdd7897dae7f8d455e44f211dc0e62059dd2c"
	i.AgentBuildDate = time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	i.AgentDirty = true
	i.CaptureSchemaVersion = 4
	i.Channel = "canary"
	i.Upgrade = &UpgradeStatus{LastCheckAt: now, Result: "error", Error: "dial tcp 10.0.0.1:443: no such host"}
	i.EmptyStarts24h = 7
	i.Spool = spool.Stats{Parked: 1, Dropped: map[string]int{"hook_abandoned": 2, "parked": 1, "rejected:too_large": 1}}

	b, err := json.Marshal(Build(i))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"schema_version":2`, `"agent_commit":"5b8bdd7897dae7f8d455e44f211dc0e62059dd2c"`,
		`"agent_build_date":"2026-09-10T12:00:00Z"`, `"agent_dirty":true`, `"capture_schema_version":4`,
		`"channel":"canary"`, `"upgrade":{`, `"result":"error"`, `"empty_starts_24h":7`, `"parked":1`,
		`"hook_abandoned":2`, `"rejected:too_large":1`, `"threshold_bytes":`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("report lacks %s:\n%s", want, b)
		}
	}
	// Redaction scrubs the upgrade error's address like every other error.
	red := Build(i).Redacted()
	if strings.Contains(red.Upgrade.Error, "10.0.0.1") {
		t.Errorf("redacted upgrade error still carries an address: %q", red.Upgrade.Error)
	}
	if Build(i).Upgrade.Error == red.Upgrade.Error {
		t.Error("Redacted mutated the original's upgrade status or did not scrub it")
	}
}

// A binary that was never stamped reports an empty commit rather than failing,
// and the ldflags date fills in when the stamp has none.
func TestBuildInfoDegradesToTheFallbackDate(t *testing.T) {
	b := BuildInfo("2026-09-10T10:00:00Z")
	if b.Date.IsZero() {
		t.Error("the fallback build date was not read")
	}
	if BuildInfo("not a date").Commit != b.Commit {
		t.Error("the commit must not depend on the fallback date")
	}
}

// DefaultThresholds is read by the client to phrase these same judgements for a
// person ("nothing has uploaded for fifteen minutes"). If it returned anything
// other than the numbers Build derives from, a machine could be described as
// working by one command and broken by another, which is the failure this whole
// package exists to prevent.
func TestDefaultThresholdsAreTheOnesBuildDerivesFrom(t *testing.T) {
	th := DefaultThresholds()

	cases := []struct {
		name string
		in   func() Inputs
		kind string
		want bool
	}{
		{
			name: "a delivery one second inside DeliveryStale is not a stalled backlog",
			in: func() Inputs {
				i := base()
				i.LastSuccess = at(-th.DeliveryStale + time.Second)
				i.Spool = spool.Stats{Pending: 5, OldestAge: th.BacklogStalled + time.Minute}
				return i
			},
			kind: KindBacklogStalled,
			want: false,
		},
		{
			name: "a delivery one second past DeliveryStale is a stalled backlog",
			in: func() Inputs {
				i := base()
				i.LastSuccess = at(-th.DeliveryStale - time.Second)
				i.Spool = spool.Stats{Pending: 5, OldestAge: th.BacklogStalled + time.Minute}
				return i
			},
			kind: KindBacklogStalled,
			want: true,
		},
		{
			name: "an agent one second inside EnrollmentGrace is not yet accused of never delivering",
			in: func() Inputs {
				i := base()
				i.LastSuccess = time.Time{}
				i.StartedAt = at(-th.EnrollmentGrace + time.Second)
				return i
			},
			kind: KindNeverDelivered,
			want: false,
		},
		{
			name: "an agent one second past EnrollmentGrace has never delivered",
			in: func() Inputs {
				i := base()
				i.LastSuccess = time.Time{}
				i.StartedAt = at(-th.EnrollmentGrace - time.Second)
				return i
			},
			kind: KindNeverDelivered,
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasKind(Build(tc.in()), tc.kind); got != tc.want {
				t.Errorf("%s present = %v, want %v (kinds: %v)", tc.kind, got, tc.want, kinds(Build(tc.in())))
			}
		})
	}
}

// TestARecoveryIsNotADrop: the spool's Dropped map is the durable place for
// counters that must outlive the process that observed them, and recovery
// uses it for "this many events came back out of a transcript". Every
// counter in it used to raise drops_recorded, so a machine whose recovery
// pass worked read as degraded and its recoveries were summed into the
// fleet's drop total, burying the reasons that really were losses.
func TestARecoveryIsNotADrop(t *testing.T) {
	if !IsLoss("spool_full") || !IsLoss("rejected:batch_exceeds_the_server_s_size_limit") {
		t.Error("a real discard is not counted as a loss")
	}
	for _, notALoss := range []string{"recovered_from_transcript", "parked", "tmp_swept"} {
		if IsLoss(notALoss) {
			t.Errorf("%q is counted as a loss; it is not one", notALoss)
		}
	}
	for _, r := range NotLossReasons() {
		if IsLoss(r) {
			t.Errorf("NotLossReasons lists %q, which IsLoss calls a loss", r)
		}
	}
	if got := NotLossReasons(); len(got) != len(NotLosses) {
		t.Errorf("NotLossReasons returned %d of %d entries", len(got), len(NotLosses))
	}

	in := Inputs{}
	in.PrevDropped = map[string]int{}
	in.Spool.Dropped = map[string]int{
		"recovered_from_transcript": 8643,
		"parked":                    7,
		"tmp_swept":                 3,
		"spool_full":                2,
	}
	r := Build(in)
	var drops []Condition
	for _, c := range r.Conditions {
		if c.Kind == KindDropsRecorded {
			drops = append(drops, c)
		}
	}
	if len(drops) != 1 {
		t.Fatalf("drops_recorded raised %d time(s), want once for spool_full only: %+v", len(drops), drops)
	}
	if !strings.Contains(drops[0].Detail, "reason=spool_full") || !strings.Contains(drops[0].Detail, "count=2") {
		t.Errorf("the condition names %q, want the real discard", drops[0].Detail)
	}
	// The counter still travels: the fleet can still see how often the live
	// path needed the transcript.
	if r.Spool.Dropped["recovered_from_transcript"] != 8643 {
		t.Errorf("the recovery counter was dropped from the report: %v", r.Spool.Dropped)
	}
}

// TestAReportOfOnlyRecoveriesIsNotDegraded: the shape more than half of a
// real fleet's machines were in when this was measured.
func TestAReportOfOnlyRecoveriesIsNotDegraded(t *testing.T) {
	in := Inputs{}
	in.PrevDropped = map[string]int{}
	in.Spool.Dropped = map[string]int{"recovered_from_transcript": 512}
	r := Build(in)
	for _, c := range r.Conditions {
		if c.Kind == KindDropsRecorded {
			t.Errorf("a machine whose recovery pass worked raised %s: %s", c.Kind, c.Detail)
		}
	}
}

// TestDropsRecordedIsARiseNotALifetimeTotal: the counters in the spool's
// Dropped map live as long as the spool directory, so a level says only
// that this machine ever lost something. A laptop that filled its disk once
// read degraded from then on, and on the fleet on 2026-09-23 one of the two
// machines showing disk_full had 127 GiB free and had been carrying its
// 3,010 from an earlier day. The condition is for "capture is failing now",
// so it fires on the rise.
func TestDropsRecordedIsARiseNotALifetimeTotal(t *testing.T) {
	// A total carried over from an earlier day, unchanged since the previous
	// report: nothing is being lost now.
	i := base()
	i.PrevDropped = map[string]int{"disk_full": 3010}
	i.Spool = spool.Stats{Dropped: map[string]int{"disk_full": 3010}}
	if r := Build(i); hasKind(r, KindDropsRecorded) {
		c, _ := condition(r, KindDropsRecorded)
		t.Errorf("an unchanged lifetime total raised the condition: %s", c.Detail)
	}

	// The same total rising by two is data being lost now, and the detail
	// says by how much rather than only how many there have ever been.
	i.Spool = spool.Stats{Dropped: map[string]int{"disk_full": 3012}}
	r := Build(i)
	c, ok := condition(r, KindDropsRecorded)
	if !ok {
		t.Fatalf("a rise did not raise the condition; conditions %v", kinds(r))
	}
	if !strings.Contains(c.Detail, "rose=2") || !strings.Contains(c.Detail, "count=3012") {
		t.Errorf("the condition says %q, want the rise and the total", c.Detail)
	}

	// No previous report known (a first report, or the first from a new
	// daemon): nothing is raised, because re-reading a lifetime total as news
	// at every daemon start is what made the condition permanent. The
	// server's own delta across reports still alerts on a rise that straddles
	// a restart.
	i.PrevDropped = nil
	if r := Build(i); hasKind(r, KindDropsRecorded) {
		c, _ := condition(r, KindDropsRecorded)
		t.Errorf("a first report raised the condition from a lifetime total: %s", c.Detail)
	}
}
