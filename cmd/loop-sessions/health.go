package main

// Self-telemetry: the half of the client that tells the fleet this machine
// exists.
//
// The fleet page was empty and the health_reports table held zero rows, on a
// server that had already ingested seventy thousand events. Nothing was broken
// in either half: internal/health could build a Report and server/ingest could
// store one, and both were tested. No code on any laptop had ever sent one. This
// file is that join, and the tests beside it assert that a running daemon posts
// a report rather than that a report can be constructed.
//
// Reporting is deliberately not serialised across the machine the way delivery
// is. The delivery lock exists because two daemons leasing one spool would send
// every item twice; two daemons describing one machine produce two nearly
// identical rows, and the fleet keeps the newest per device. Suppressing a
// report because a sibling holds a lock would trade a duplicate for a silence,
// and silence is the one thing the fleet page alarms on.
//
// The rule that shapes everything below is that a report must never matter. It
// is telemetry ABOUT the delivery pipeline, so a server answering 500 here has
// to leave the pipeline exactly as it found it: nothing acked, nothing
// quarantined, and in particular no failure recorded against the spool, because
// that record is what `status` reads to explain a stuck queue and a health
// outage must not make a perfectly healthy laptop print NOT UPLOADING.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/discovery"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/hooks"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// healthPath is the server's health route. Duplicated here for the same reason
// eventsPath is: the two halves are separately deployable, and a client that
// imported a server package would have to be rebuilt in lockstep with it.
const healthPath = "/v1/health"

// healthTimeout bounds one report. Much shorter than uploadTimeout because a
// report is a few kilobytes rather than a 4 MiB batch, and because the daemon
// waits for an in-flight report when the session ends: this is the longest a
// server that has stopped answering can keep a dying daemon alive.
const healthTimeout = 20 * time.Second

// healthMinInterval is the least time between two reports from one machine,
// whichever daemon sends them. Every session start used to send one, and a
// machine starting fifty promptless sessions a day added fifty rows that
// said what the previous row said. The cadence inside a session is the
// daemon's ReportInterval, which is this same five minutes.
const healthMinInterval = 5 * time.Minute

// discoveryTTL is how long one discovery scan is reused across reports.
//
// The scan walks every harness's session tree, which is far too much work to
// repeat on the report cadence of a machine running on battery. What it measures
// — how many tools were found, and how many are installed with their sessions
// missing — changes when somebody installs an editor, not between two reports
// five minutes apart.
const discoveryTTL = 30 * time.Minute

// healthReporter takes one reading of this machine and posts it.
//
// One reporter is driven by one goroutine — the daemon's — because it carries
// the previous reading forward across reports. Two goroutines sharing one would
// race on that memory and, worse, compare a queue depth against a value from a
// report that was never sent.
type healthReporter struct {
	paths config.Paths
	cfg   config.Config
	// sp is the delivery path's own spool handle rather than a second one. Part
	// of the drop tally lives in a handle's memory — the durable copy is best
	// effort by design — and the report is exactly where those counts are
	// supposed to be complete.
	sp *spool.Spool

	url    string
	token  string
	client *http.Client

	// prevPending is the queue depth the previous report carried, which is what
	// lets health call a backlog growing rather than merely large. It lives here
	// because only something that reports repeatedly has a previous value.
	prevPending *int
	// prevDropped is the drop counters the previous report carried, which is
	// what makes drops_recorded a rise rather than a lifetime total.
	prevDropped map[string]int

	// findings and scannedAt cache the discovery scan; see discoveryTTL.
	findings  []discovery.Finding
	scannedAt time.Time

	// minInterval is the machine-wide rate limit; zero disables it (tests).
	minInterval time.Duration

	now func() time.Time
}

// newHealthReporter builds the reporter the daemon drives.
//
// It authenticates the same way delivery does, through the same device token on
// disk, because a second credential path is a second thing to expire: a report
// that authenticated differently from an upload would let the fleet believe a
// machine is fine while everything it captures is being rejected.
func newHealthReporter(p config.Paths, cfg config.Config, sp *spool.Spool) (*healthReporter, error) {
	token, err := loadDeviceToken(p)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("health: no endpoint is configured; run `loop-sessions install`")
	}
	if sp == nil {
		return nil, errors.New("health: a spool is required to report anything about one")
	}
	return &healthReporter{
		paths:       p,
		cfg:         cfg,
		sp:          sp,
		url:         strings.TrimSuffix(cfg.Endpoint, "/") + healthPath,
		token:       token,
		client:      &http.Client{Timeout: healthTimeout},
		minInterval: healthMinInterval,
		now:         time.Now,
	}, nil
}

// Report takes one reading and posts it.
//
// Every path that is not a 2xx returns an error, and no path touches the spool.
// The report carries its own emission time and the server deduplicates on it, so
// a sample lost to a 500 is not redelivered: the next one, five minutes later,
// carries strictly newer facts and is worth more than a retry of stale ones.
func (r *healthReporter) Report(ctx context.Context) error {
	now := r.now()
	if r.minInterval > 0 && !stampDue(r.paths, stampHealthReport, r.minInterval, now) {
		// Another daemon on this machine reported within the interval; the
		// fleet has a fresh row and a second one would say the same thing.
		return nil
	}
	body, err := json.Marshal(r.build())
	if err != nil {
		return r.failed(fmt.Errorf("health: encode report: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return r.failed(fmt.Errorf("health: build request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("User-Agent", "loop-sessions/"+Version)

	res, err := r.client.Do(req)
	if err != nil {
		return r.failed(fmt.Errorf("health: post %s: %w", r.url, err))
	}
	defer res.Body.Close()
	// The verdict is a status code; the body is drained only so the connection
	// can be reused rather than reopened on the next report.
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4<<10))

	if res.StatusCode/100 != 2 {
		return r.failed(fmt.Errorf("health: %s answered http %d", r.url, res.StatusCode))
	}
	if err := writeStamp(r.paths, stampHealthReport, now); err != nil {
		logf("health: could not record the report time: %v", err)
	}
	return nil
}

// build takes the reading.
//
// Two of health's inputs are deliberately absent. PrevSessionsSeen and
// EventsSpooledSinceLastReport drive the inferential half of capture_stale —
// "sessions appeared and we captured none of them" — and this client cannot
// count events spooled between two reports without reading the drain's private
// counters, which several daemons on one machine would each see only a share of.
// A wrong zero there accuses a working laptop of capturing nothing, and a false
// critical on the fleet page costs more than a missing inferential signal. The
// definitive half, HooksRegistered, is supplied below and catches the same
// failure from the other side.
//
// Session counts and harness versions are absent rather than guessed for the
// same reason: the honest source for both is a walk of every daemon's state
// file, and a count of the one session this daemon shadows would be read in the
// fleet view as a machine-wide total.
func (r *healthReporter) build() health.Report {
	now := r.now()
	r.refresh()

	st, statsErr := r.sp.Stats()
	sd, stateErr := r.sp.State()
	stamp := health.BuildInfo(BuildDate)

	in := health.Inputs{
		AgentVersion:         Version,
		AgentCommit:          stamp.Commit,
		AgentBuildDate:       stamp.Date,
		AgentDirty:           stamp.Dirty,
		CaptureSchemaVersion: r.cfg.CaptureSchemaVersion,
		Channel:              r.cfg.Channel,
		Upgrade:              readUpgradeStatus(r.paths),
		EmptyStarts24h:       capture.Ledger{Dir: r.paths.StateDir()}.EmptyStarts(now, 24*time.Hour),
		Thresholds:           health.Thresholds{DiskGuard: r.sp.Guard(), SpoolMaxBytes: r.cfg.SpoolMaxBytes},
		// Process uptime would say nothing here: a daemon is born with each
		// session. The clock that makes "has never delivered" meaningful is how
		// long this machine has been enrolled, which is the same clock `status`
		// derives its verdict from, so the two cannot disagree about a laptop.
		StartedAt:       r.cfg.InstalledAt,
		Spool:           st,
		SpoolErr:        statsErr,
		LastSuccess:     sd.LastSuccess,
		LastError:       r.lastError(sd, stateErr),
		Discovery:       r.discover(now),
		Paused:          r.cfg.Paused,
		PausedSince:     r.cfg.PausedSince,
		SkippedTools:    r.cfg.SkippedTools,
		PrevPending:     r.prevPending,
		PrevDropped:     r.prevDropped,
		HooksRegistered: hooksRegistered(),
		SpoolDir:        r.paths.SpoolDir(),
		DiskFree:        spool.DiskFree,
		Now:             func() time.Time { return now },
	}

	// Advanced on every reading rather than on every accepted one. The count is
	// a fact about this machine at this moment; whether the server took the
	// sample changes nothing about what the next one should be compared against.
	if statsErr == nil {
		pending := st.Pending
		r.prevPending = &pending
		r.prevDropped = make(map[string]int, len(st.Dropped))
		for k, v := range st.Dropped {
			r.prevDropped[k] = v
		}
	}
	return health.Build(in)
}

// refresh re-reads the settings this report describes.
//
// A session outlives the decisions made during it: somebody pauses capture, or
// tells discovery where a harness keeps its sessions, in the middle of one. A
// reporter holding the config it was built with would keep describing the
// machine as it was at SessionStart, and the fleet page would show a paused
// laptop as a running one for as long as the session lasts.
//
// A config that cannot be read leaves the previous one in place, deliberately.
// The last posture we know about is a better answer than a default, which would
// report a machine the user paused as capturing.
func (r *healthReporter) refresh() {
	if cfg, err := config.Load(r.paths); err == nil {
		r.cfg = cfg
	}
}

// lastError is what the fleet should be told is wrong with delivery.
//
// A delivery history that could not be read is reported through the same field,
// because it is the only channel this report has for saying so — and it matters:
// never_delivered is derived from LastSuccess, and an unreadable state file
// produces a zero LastSuccess that is indistinguishable from a machine which
// genuinely never delivered.
func (r *healthReporter) lastError(sd spool.State, stateErr error) error {
	if stateErr != nil {
		return fmt.Errorf("delivery history unreadable, so upload times in this report are not measurements: %w", stateErr)
	}
	return currentFailure(sd)
}

// discover returns the cached harness scan, refreshing it when it ages out.
func (r *healthReporter) discover(now time.Time) []discovery.Finding {
	if r.findings != nil && now.Sub(r.scannedAt) < discoveryTTL {
		return r.findings
	}
	skip := map[discovery.Tool]bool{}
	for _, s := range r.cfg.SkippedTools {
		skip[discovery.Tool(s)] = true
	}
	roots := map[discovery.Tool]string{}
	for k, v := range r.cfg.Roots {
		roots[discovery.Tool(k)] = v
	}
	r.findings = discovery.Run(discovery.Options{Skip: skip, UserPaths: roots})
	r.scannedAt = now
	return r.findings
}

// hooksRegistered answers whether this machine can still produce events at all.
//
// Nil rather than false when the question cannot be answered: health treats an
// undeterminable answer as no evidence and a false one as proof that capture is
// dead, and a harness whose settings we cannot parse is not proof of anything.
//
// The predicate is "any of our hooks survive in the harness settings", not "all
// of them". A partial set still produces events, whereas a binary that moved or
// an upgrade that rewrote the file leaves none — and that, a machine that is
// online, healthy and silently recording nothing, is the install failure this
// signal exists to catch.
func hooksRegistered() *bool {
	self, err := os.Executable()
	if err != nil {
		return nil
	}
	plan, err := hooks.Preview(hooks.Options{Binary: self})
	if err != nil {
		return nil
	}
	registered := len(plan.AlreadySet) > 0
	return &registered
}

// failed records a report that did not land and hands the error back.
//
// It writes to the agent log and nowhere else. It must not call
// RecordDeliveryFailure: that record is what `status` prints as the reason
// uploads are stuck, and a machine whose events are being delivered perfectly
// would then explain its health outage as a delivery outage.
func (r *healthReporter) failed(err error) error {
	logf("health: this machine's report was not sent, so the fleet view will call it silent: %v", err)
	return err
}
