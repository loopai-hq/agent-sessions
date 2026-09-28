// Package skillusage receives skill invocations from the emitters that are
// not an enrolled laptop: the Claude Code hook on a cloud or unenrolled
// machine, the beacon line in a mirrored SKILL.md, and the reconcilers that
// replay a vendor's own activation log.
//
// One row per call, and the row is a claim. The credential proves the
// platform (a source token, lss_) or the machine (a device token, lsd_),
// never the skill, the ids or the outcome; those are what the payload said.
// The store keeps that distinction as trust = claimed or device, and the
// derived copy of the same moment, built from events the laptop delivered,
// replaces whatever an API copy put on the key. So nothing here decides
// what counts: it decides what is well-formed, who sent it, and whether
// they may.
//
// Three bounds sit ahead of the database, because the verify touches one
// hot row and shares the pool with /v1/events, and a token readable by a
// whole vendor environment will eventually be flooded. A fixed map of
// per-address buckets refuses a flood before the verify; a semaphore of
// four connections per instance keeps the route from queuing ingest; and
// both transactions run under a short lock bound, so a post on a key a
// derive batch holds parks a connection for a tenth of a second and
// answers 429 rather than holding the semaphore for the statement timeout.
// The verify's touch commits in a transaction of its own before the row is
// written, so no request holds the token row past that one statement and
// a burst under one shared token queues on it for microseconds, not for
// the length of a request. The row's own counter, shared by every
// instance, stays the authority.
//
// Nothing in a payload reaches a log line: ids and the skill pass through
// the ingest package's shape gate, enums are validated before they are
// logged, and the bearer is hashed before it is looked up and never
// logged at all.
package skillusage

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/loopai-hq/agent-sessions/server/ingest"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// InvocationsPath is the route the emitters have compiled in (the hook
// script, the beacon line): a rename is a fleet-wide outage.
const InvocationsPath = "/v1/skill-invocations"

// Scope is the source-token scope this route accepts. A catalog
// publisher's token is dead here, and answers 401 with its state logged.
const Scope = store.ScopeSkillInvocations

// Tx is the store inside one request's bounded transaction: the reads the
// row needs and the row write, and nothing that touches the token row,
// which the verify committed on its own before this transaction opened.
type Tx interface {
	// SessionOwner returns who a session is attributed to, or false when
	// the store has not seen it.
	SessionOwner(ctx context.Context, sessionRef string) (email string, found bool, err error)
	// ActorKnown reports whether an email is a live principal.
	ActorKnown(ctx context.Context, email string) (bool, error)
	// Upsert writes one row through the store's merge.
	Upsert(ctx context.Context, row store.SkillRow) (store.UpsertOutcome, error)
}

// Store is the two transactions a source request makes, in order: the
// verify's touch, committed on its own so the token row is held for one
// statement and a burst under a shared token never queues behind another
// post's row write; then the bounded transaction the row lands in.
// Implementations set the lock bound inside both and hand back the error
// untouched, since the handler reads the sqlstate off it to tell a lock
// wait from a failure.
type Store interface {
	// AuthenticateSource resolves a token hash to its row and state,
	// touching and counting the row when it is live for scope, and
	// commits that touch before returning. No row is a state, not an
	// error. A request refused after this call has still been counted,
	// which is the point: the counter is a rate limit, not a ledger.
	AuthenticateSource(ctx context.Context, tokenHash []byte, scope string) (store.SourceToken, error)
	InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}

// Devices verifies an lsd_ bearer. It is the upload route's own port on
// purpose: one implementation decides which laptops are live, so this
// route and /v1/events can never disagree about a revoked one.
type Devices = ingest.Devices

// Options configure a Handler. Store, Devices and Logger are required in
// production; the bounds default to the design's values.
type Options struct {
	Store   Store
	Devices Devices
	Logger  *slog.Logger
	// Now is injectable so the clamp and the rate windows can be tested
	// without waiting.
	Now func() time.Time

	// MaxBodyBytes is the body cap; a body over it is 413. Default 4 KiB.
	MaxBodyBytes int64
	// Connections is the per-instance semaphore over the pool. Default 4.
	Connections int
	// ShedBuckets is the size of the per-address and per-token map; the
	// oldest bucket is evicted when it is full. Default 4096.
	ShedBuckets int
	// ShedPerMinute is each bucket's allowance. Default the soft-revoked
	// ceiling, so the shed never refuses what the row would have allowed.
	ShedPerMinute int
	// DevicePerMinute is the in-process bucket per device id. Default 60.
	DevicePerMinute int
}

const (
	defaultMaxBodyBytes    = 4096
	defaultConnections     = 4
	defaultShedBuckets     = 4096
	softRevokedCeiling     = store.SoftRevokedCeiling
	defaultDevicePerMinute = 60
	// retryAfterCap is the Retry-After of a token over its cap, most of
	// the window; retryAfterLock is the one of a condition that clears in
	// milliseconds: a lock wait (the other holder commits), a full
	// semaphore (a request finishes) or an address bucket over its
	// allowance, all of which a reconciler honouring the header should
	// retry at once rather than idle 30 s on.
	retryAfterCap  = 30 * time.Second
	retryAfterLock = time.Second
	// clampPast and clampFuture bound a claimed occurred_at: outside them
	// the row takes received_at and says so, so a leaked token can
	// inflate the present but never rewrite last quarter.
	clampPast   = 7 * 24 * time.Hour
	clampFuture = 5 * time.Minute
	// maxArgsBytes is the args_bytes ceiling.
	maxArgsBytes = 1_000_000
)

// Handler serves the route.
type Handler struct {
	store   Store
	devices Devices
	log     *slog.Logger
	now     func() time.Time
	maxBody int64
	sem     semaphore
	shed    *shed
	perDev  *bucketLimiter
	mux     *http.ServeMux
}

// New builds a Handler. It fails rather than defaulting when a port is
// missing: a nil verifier would make the route accept anything, which is
// not a condition to discover in production.
func New(o Options) (*Handler, error) {
	if o.Store == nil {
		return nil, errors.New("skillusage: Store is required")
	}
	if o.Devices == nil {
		return nil, errors.New("skillusage: Devices is required")
	}
	h := &Handler{
		store:   o.Store,
		devices: o.Devices,
		log:     o.Logger,
		now:     o.Now,
		maxBody: o.MaxBodyBytes,
	}
	if h.log == nil {
		h.log = slog.Default()
	}
	if h.now == nil {
		h.now = time.Now
	}
	if h.maxBody <= 0 {
		h.maxBody = defaultMaxBodyBytes
	}
	conns := o.Connections
	if conns <= 0 {
		conns = defaultConnections
	}
	h.sem = make(semaphore, conns)
	buckets, perMin := o.ShedBuckets, o.ShedPerMinute
	if buckets <= 0 {
		buckets = defaultShedBuckets
	}
	if perMin <= 0 {
		perMin = softRevokedCeiling
	}
	h.shed = newShed(buckets, perMin)
	perDev := o.DevicePerMinute
	if perDev <= 0 {
		perDev = defaultDevicePerMinute
	}
	h.perDev = newBucketLimiter(float64(perDev), time.Minute/time.Duration(perDev))
	h.mux = http.NewServeMux()
	h.Register(h.mux)
	return h, nil
}

// Route is one of the handler's mountings, in the form a mux takes.
type Route struct {
	Pattern string
	Handler http.Handler
}

// Routes lists every route the handler serves, so a server that mounts
// them on its own mux mounts exactly what Register mounts and the parity
// test has nothing to catch. The reconciler-runs route joins this list
// with its table.
func (h *Handler) Routes() []Route {
	return []Route{
		{Pattern: "POST " + InvocationsPath, Handler: http.HandlerFunc(h.handleInvocation)},
		{Pattern: "POST " + ReconcilerRunsPath, Handler: http.HandlerFunc(h.handleReconcilerRun)},
	}
}

// Register adds the routes to a mux owned by the caller.
func (h *Handler) Register(mux *http.ServeMux) {
	for _, r := range h.Routes() {
		mux.Handle(r.Pattern, r.Handler)
	}
}

// ServeHTTP lets the Handler stand alone.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }
