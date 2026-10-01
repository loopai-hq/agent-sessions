package main

// Delivery: the half of the client that turns a spool file into a row in the
// server's database.
//
// Everything in here exists because of a gap that was invisible for a day. The
// hooks captured, the spool filled, and nothing ever moved: internal/drain knew
// how to deliver a batch and internal/daemon knew how to shadow a session, but
// no code path constructed either of them, so a laptop that had captured ten
// items reported ten items pending forever and said nothing about it. Two rules
// follow, and they are why this file looks the way it does.
//
// Delivery is composed, not merely written. The daemon takes a Flusher, the
// drain takes a Transport, and neither has a default that reaches the network,
// which is correct design and exactly how a component can pass every test it has
// and never run. This file is the composition root for the client's outbound
// path, and the tests below assert composition rather than the pieces.
//
// Delivery is loud. Every upload, every rejection and every failure is written
// to the agent log, because the log recorded thousands of captures and not one
// word about delivery, and that silence is the only reason the failure looked
// like "nothing was captured" instead of "nothing was uploaded".

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/capture"
	"github.com/loopai-hq/loop-sessions/internal/config"
	"github.com/loopai-hq/loop-sessions/internal/daemon"
	"github.com/loopai-hq/loop-sessions/internal/drain"
	"github.com/loopai-hq/loop-sessions/internal/spool"
)

// eventsPath is the server's ingest route. It is duplicated here rather than
// imported from server/ingest on purpose: the two halves are separately
// deployable, and a client that imported a server package would have to be
// rebuilt in lockstep with it. Renaming this route is a fleet-wide outage, which
// is why it is a constant on both sides.
const eventsPath = "/v1/events"

// deliveryLockName guards the spool against two daemons draining it at once.
//
// One daemon per session is right — a daemon shadows a harness — but a person
// with three editor windows has three harnesses and therefore three daemons, all
// pointed at one spool directory. Leasing does not hide an item from another
// reader, so without this they would each send every item and each pay for the
// uplink. It lives beside the spool rather than beside the session state because
// what it protects is the spool.
const deliveryLockName = ".delivery.lock"

// uploadTimeout bounds one batch. Generous because the cap on a batch is 4 MiB
// and the machine may be on hotel wifi; a batch that cannot finish in this is
// not going to finish, and the spool keeps the data either way.
const uploadTimeout = 90 * time.Second

// ---------------------------------------------------------------- transport

// httpTransport delivers a batch to the server's ingest endpoint.
type httpTransport struct {
	url    string
	token  string
	client *http.Client
}

// wireResponse is the server's per-item verdict as it arrives. Modelled locally
// for the same reason the route is: the client must be able to read a response
// from a server it was not built alongside.
type wireResponse struct {
	Accepted     []string     `json:"accepted"`
	Rejected     []wireReject `json:"rejected"`
	RetryAfterMS int64        `json:"retry_after_ms"`
}

type wireReject struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// Send posts one batch and translates the answer.
//
// Nothing may be acked on anything other than an explicit acceptance, so every
// path that is not a parsed 2xx verdict returns an error and leaves the whole
// batch pending. That includes a 200 whose body does not parse: a response we
// cannot read is not permission to delete anything.
func (t *httpTransport) Send(ctx context.Context, batch []spool.Leased) (drain.Response, error) {
	items := make([]spool.Item, len(batch))
	for i, l := range batch {
		items[i] = l.Item
	}
	body, err := json.Marshal(items)
	if err != nil {
		return drain.Response{}, fmt.Errorf("delivery: encode batch: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return drain.Response{}, fmt.Errorf("delivery: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+t.token)
	req.Header.Set("User-Agent", "loop-sessions/"+Version)

	res, err := t.client.Do(req)
	if err != nil {
		return drain.Response{}, fmt.Errorf("delivery: post %s: %w", t.url, err)
	}
	defer res.Body.Close()

	// Bounded read: the verdict is a list of ids and a server that answered with
	// something else must not be able to make this process allocate for it.
	raw, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))

	var wire wireResponse
	parsed := readErr == nil && json.Unmarshal(raw, &wire) == nil

	out := drain.Response{}
	if parsed && wire.RetryAfterMS > 0 {
		out.RetryAfter = time.Duration(wire.RetryAfterMS) * time.Millisecond
	}
	if hdr := retryAfterHeader(res); hdr > out.RetryAfter {
		out.RetryAfter = hdr
	}

	if res.StatusCode/100 != 2 {
		// The pacing instruction is carried out with the error because the drain
		// reads it from the response even on a failure; that is how a 429 slows
		// the whole fleet down rather than only the requests that succeed.
		return out, statusError(res.StatusCode, t.url)
	}
	if readErr != nil {
		return out, fmt.Errorf("delivery: read verdict from %s: %w", t.url, readErr)
	}
	if !parsed {
		return out, fmt.Errorf("delivery: %s answered %d with a body that is not a verdict", t.url, res.StatusCode)
	}

	out.Accepted = wire.Accepted
	for _, r := range wire.Rejected {
		out.Rejected = append(out.Rejected, drain.Reject{ID: r.ID, Reason: r.Reason})
	}
	return out, nil
}

// statusError names what a non-2xx means for the person who has to fix it.
//
// A 401 is the one status a laptop cannot recover from on its own, and it is
// indistinguishable from an outage in the logs unless it is called out: the
// backlog grows, delivery fails forever, and the reason is that the device
// credential was revoked or expired. The message is what somebody reading
// agent.log at midnight needs.
func statusError(status int, url string) error {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("delivery: %s rejected this device's credential (http %d); "+
			"nothing will upload until you run `loop-sessions install` to sign in again", url, status)
	default:
		return fmt.Errorf("delivery: %s answered http %d", url, status)
	}
}

// retryAfterHeader reads the server's pacing instruction. Only the seconds form
// is honoured; the HTTP-date form is legal and no server we talk to sends it, so
// guessing at a parse would invent a delay rather than read one.
func retryAfterHeader(res *http.Response) time.Duration {
	v := strings.TrimSpace(res.Header.Get("Retry-After"))
	if v == "" {
		return 0
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs <= 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

// ---------------------------------------------------------------- flusher

// spoolFlusher adapts a drain to the daemon's Flusher, serialises delivery
// across the daemons on one machine, and writes what happened to the agent log.
type spoolFlusher struct {
	d        *drain.Drain
	sp       *spool.Spool
	lockPath string

	// resolved and refused track the drain's cumulative counters so a cycle can
	// report its own delta. The drain reports totals since it was constructed,
	// and "how many moved just now" is what both the caller and the log need.
	resolved int
	refused  int
}

// RunOnce performs one delivery cycle.
//
// The count it returns is items the server RESOLVED — accepted or permanently
// rejected — not items handed to the transport. The daemon's shutdown drain
// loops until this reaches zero, so counting attempts instead would spin against
// an unreachable server for the whole grace period and then exit having
// delivered nothing.
func (f *spoolFlusher) RunOnce(ctx context.Context) (int, error) {
	lock, err := daemon.TryLock(f.lockPath)
	if errors.Is(err, daemon.ErrLocked) {
		// Another session's daemon is delivering right now. Returning zero ends a
		// shutdown drain early, which is the right trade: the holder is draining
		// the same spool, and waiting behind its HTTP request would delay this
		// process's exit for no gain.
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer lock.Close()

	st, runErr := f.d.RunOnce(ctx)

	sent := (st.Accepted + st.Rejected) - f.resolved
	refused := st.Rejected - f.refused
	f.resolved = st.Accepted + st.Rejected
	f.refused = st.Rejected

	f.report(sent, refused, runErr)
	return sent, runErr
}

// report writes the delivery line the agent log never had.
//
// An idle cycle says nothing: on a quiet laptop this runs every few seconds and
// a heartbeat in the log would bury the lines that matter. Everything else is
// recorded, including the failures, because a silent failure here is what turned
// one broken pipeline into a day of believing capture was broken.
func (f *spoolFlusher) report(sent, refused int, err error) {
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// The daemon is shutting down, not failing to deliver. Logging this
			// as a failure would put a scary line in the log at the end of every
			// session and teach whoever reads it to ignore the ones that matter.
			return
		}
		logf("drain: upload failed, %d item(s) still queued: %v", f.pending(), err)
		// Recorded where `loop-sessions status` can read it, because the log
		// file is not where somebody looks when they want to know why nothing
		// is arriving. A transport failure never reaches the items themselves,
		// by design, so the uploader is the only thing that ever holds this
		// error: if it does not write it down here, nobody else can. The
		// difference this makes is a person seeing a 401 and knowing to re-run
		// install, rather than filing a ticket against a silent queue.
		f.sp.RecordDeliveryFailure(err)
		return
	}
	if refused > 0 {
		// Permanent. These are now in quarantine and will never retry, so this is
		// the only notice anybody gets that data was thrown away.
		logf("drain: the server permanently rejected %d item(s); they are quarantined and will not retry", refused)
	}
	if sent > 0 {
		logf("drain: uploaded %d item(s), %d still queued", sent, f.pending())
	}
}

// pending reports the queue depth for a log line. A failure to read it is not
// worth reporting on top of whatever is already being reported, so it degrades
// to a value that cannot be mistaken for a real count.
func (f *spoolFlusher) pending() int {
	st, err := f.sp.Stats()
	if err != nil {
		return -1
	}
	return st.Pending
}

// newDelivery builds the outbound path: spool, transport, drain.
//
// Every failure here is a machine that captures and never delivers, so none of
// them is defaulted away. A missing token, an unset endpoint and an unreadable
// spool are all reported to the caller, which logs them where `doctor` and the
// user can find them.
func newDelivery(p config.Paths, cfg config.Config) (*spoolFlusher, error) {
	transport, err := newTransport(p, cfg)
	if err != nil {
		return nil, err
	}

	sp, err := spool.Open(spool.Options{
		Dir:          p.SpoolDir(),
		MaxBytes:     cfg.SpoolMaxBytes,
		MinFreeRatio: cfg.MinFreeRatio,
	})
	if err != nil {
		return nil, fmt.Errorf("delivery: open spool: %w", err)
	}

	d, err := drain.New(drain.Options{
		Spool:     sp,
		Transport: transport,
		Enrich:    newStopAnchorer(p.StateDir()).enrich,
		// Full jitter, because a fleet knocked offline by one outage otherwise
		// comes back in step and knocks the server over again.
		Backoff: drain.BackoffPolicy{Jitter: true},
	})
	if err != nil {
		return nil, fmt.Errorf("delivery: %w", err)
	}

	return &spoolFlusher{d: d, sp: sp, lockPath: filepath.Join(p.SpoolDir(), deliveryLockName)}, nil
}

// newTransport builds the ingest transport from the device credential and the
// configured endpoint. It is its own function so `doctor --replay-quarantine`
// can post one item through exactly the path the daemon uses.
func newTransport(p config.Paths, cfg config.Config) (*httpTransport, error) {
	token, err := loadDeviceToken(p)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("delivery: no endpoint is configured; run `loop-sessions install`")
	}
	return &httpTransport{
		url:    strings.TrimSuffix(cfg.Endpoint, "/") + eventsPath,
		token:  token,
		client: &http.Client{Timeout: uploadTimeout},
	}, nil
}

// loadDeviceToken reads the credential enrollment wrote.
func loadDeviceToken(p config.Paths) (string, error) {
	path := filepath.Join(p.Root(), "device.token")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("delivery: this machine has no device credential at %s; "+
				"run `loop-sessions install` to sign in", path)
		}
		return "", fmt.Errorf("delivery: read device token: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("delivery: the device credential at %s is empty; "+
			"run `loop-sessions install` to sign in again", path)
	}
	return token, nil
}

// ---------------------------------------------------------------- starting it

// spawnDaemon is the seam the hook calls and the tests replace. It is a variable
// rather than a direct call because the property worth testing is WHICH hook
// starts a daemon, and that must be assertable without spawning a process.
var spawnDaemon = launchDaemon

// launchDaemon starts the delivery daemon for a session, detached, and returns
// immediately.
//
// Three details here are each the difference between working and not.
//
// The child gets no stdio. A backgrounded child that inherits the hook's stdout
// pipe holds the harness's output reader open until it exits, which on this
// codepath would mean forever; the measured cost of getting that wrong is two
// seconds per hook against twenty milliseconds.
//
// The child gets its own session (setsid). Without it the daemon shares the
// harness's process group and a closed terminal window SIGHUPs both — losing
// exactly the final drain that the "terminal closed outright" case exists to
// perform.
//
// The child is never waited on. The hook exits within milliseconds and the
// daemon is reparented to init, which is the whole point: the daemon has to
// outlive the process that started it.
func launchDaemon(p config.Paths, h capture.HookEvent) error {
	// Cheap pre-check. The authoritative gate is the lock the daemon itself
	// takes, but a resume fires SessionStart again while the first daemon is
	// still running, and there is no reason to pay for a fork to find that out.
	if daemon.AlreadyRunning(p.StateDir(), h.SessionID, nil, time.Now(), 0) {
		return nil
	}

	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("daemon: cannot determine my own path: %w", err)
	}

	// The harness is this hook's parent: the harness spawns the hook directly,
	// and where a shell is interposed it execs the command rather than forking,
	// so the pid is the same either way. Passing it explicitly matters because
	// the daemon is a grandchild and its own getppid is the hook, which is about
	// to exit.
	cmd := exec.Command(self, "daemon", //nolint:noctx // the daemon is started detached and must outlive this hook; a context would end it with the hook
		"--session", h.SessionID,
		"--owner-pid", strconv.Itoa(os.Getppid()),
		"--cwd", h.Cwd,
		"--transcript", h.TranscriptPath,
	)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	cmd.SysProcAttr = detachedProcAttr()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("daemon: start: %w", err)
	}
	// Deliberately not Wait()ed: this process is about to exit and the daemon
	// must survive it.
	_ = cmd.Process.Release()
	return nil
}
