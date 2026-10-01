package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/loopai-hq/loop-sessions/internal/health"
	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// ---------------------------------------------------------------------------
// The write path
// ---------------------------------------------------------------------------

// ingestStore binds the upload endpoint to the database.
type ingestStore struct{ s *store.Store }

// NewIngestStore adapts the storage layer to the port the upload endpoint
// declares.
//
// A nil store is refused here rather than left to be discovered later. ingest.New
// checks its Store for nil, but what it is handed is an interface value, and a nil
// *store.Store inside a non-nil interface passes that check and becomes a nil
// dereference on the first upload a laptop attempts. Failing at composition time
// turns a fleet-wide 500 into a process that does not start.
func NewIngestStore(s *store.Store) ingest.Store {
	if s == nil {
		panic("app: NewIngestStore requires a store")
	}
	return ingestStore{s: s}
}

// UpsertEvents hands a batch to the storage layer and reports back exactly what
// the storage layer said became of it.
//
// The translation is mechanical on purpose, because both of the layers that make
// a redelivery free live below this line and neither of them is visible from
// here. Events deduplicate on the event id. Token usage deduplicates separately,
// through a ledger keyed on the identity of the model call, its message id
// (store.usageKey), because one call reaches the server as two events with two
// ids whenever it appears in two transcript records. Anything in this method that
// merged, reordered or dropped records would fold the second key into the first,
// and the symptom of that is not an error anybody sees: it is every token count
// and every cost figure in the product reading high, forever, with no way to tell
// from the data that it happened.
func (a ingestStore) UpsertEvents(ctx context.Context, records []ingest.Record) (ingest.Result, error) {
	version := agentVersionOf(ctx)
	items := make([]store.Ingest, len(records))
	for i, r := range records {
		items[i] = store.Ingest{
			Event: r.Event,
			// Attribution is carried across unchanged. It was derived from the
			// device credential rather than read out of the payload, and an event
			// that arrives at the database without an owning principal is invisible
			// to every permission check that runs over it afterwards.
			Email:    r.Email,
			DeviceID: r.DeviceID,
			Repo:     r.Repo,
			// The delivered bytes, not a re-marshalling of the parsed event. Storing
			// what actually arrived is what lets a parser change be replayed later
			// against the real input.
			Body: r.Body,
			// The client build that delivered the batch, from the request. It is
			// the cohort key for "did the new client capture the answer", and it
			// rides the context because the upload endpoint hands this port its
			// records and nothing about the request they came in.
			AgentVersion: version,
		}
	}

	res, err := a.s.UpsertEvents(ctx, items)
	if err != nil {
		// The count is safe to name and the records are not: a failure worth
		// diagnosing here is a failure of size.
		err = fmt.Errorf("app: upsert %d events: %w", len(items), err)
	}
	return ingestVerdict(res, err)
}

// ingestVerdict restates the storage layer's answer in the endpoint's vocabulary.
//
// The error is taken as an argument rather than checked at the call site so that
// the one rule that matters cannot be bypassed by a future caller: no acceptance
// travels with a failure. A failed transaction rolled back, so the whole batch is
// still the agent's, and any part of a Result returned beside the error would
// tell the laptop to delete spool items that were never stored. The spool is the
// only other copy of them. The storage layer already zeroes its result on the way
// out, but this port's guarantee must not rest on the other side continuing to.
//
// Inserted and Duplicate stay apart. The endpoint answers the agent with their
// union, because an item the server already holds must be deleted from the spool
// or it is redelivered forever; anything counting activity may only count
// Inserted, because a network flap otherwise looks like a burst of work.
//
// Rejected carries the storage layer's own reason text through unchanged. It
// reaches a human, next to the quarantined item in a directory on somebody's
// laptop, and a reason reworded here would be a second wording to keep in step
// with the one the database logged.
//
// UpsertResult.Sessions is dropped because this port has nowhere to put it: the
// upload endpoint holds no cache to invalidate. It is deliberately not smuggled
// into one of the other lists, where it would read as a verdict on an item.
func ingestVerdict(res store.UpsertResult, err error) (ingest.Result, error) {
	if err != nil {
		return ingest.Result{}, err
	}
	out := ingest.Result{Inserted: res.Inserted, Duplicate: res.Duplicate}
	if len(res.Rejected) > 0 {
		out.Rejected = make([]ingest.Rejection, len(res.Rejected))
		for i, r := range res.Rejected {
			out.Rejected[i] = ingest.Rejection{ID: r.ID, Reason: r.Reason}
		}
	}
	return out, nil
}

// agentVersionKey is the context key the delivering client's build travels
// under, from the upload request to the store adapter.
type agentVersionKey struct{}

// WithAgentVersion returns a context carrying the build of the client that
// sent the request, as the upload endpoint's User-Agent names it.
//
// The value crosses on the context because the ingest port receives records
// and a context and nothing else: the request that carried the header is
// already gone by the time the adapter runs. The request middleware is the
// caller; it reads the header once and puts it here, and a request that never
// went through it delivers with no version rather than a wrong one. Only the
// build is carried, never the whole header, because the header reaches a log
// line and the database row alike and the build is the one part of it either
// needs.
func WithAgentVersion(ctx context.Context, version string) context.Context {
	version = strings.TrimSpace(version)
	if version == "" {
		return ctx
	}
	return context.WithValue(ctx, agentVersionKey{}, version)
}

func agentVersionOf(ctx context.Context) string {
	v, _ := ctx.Value(agentVersionKey{}).(string)
	return v
}

// RegisterIngest mounts the upload routes on the server's mux, behind the one
// piece of the request the store adapter needs and cannot reach for itself:
// the build of the client that delivered the batch.
//
// The ingest handler hands its port records and a context, never the request,
// so the User-Agent has to be read here, where the request and the mux are
// both in hand, and put on the context WithAgentVersion defines. The routes
// are the handler's own list (ingest.Handler.Routes), each wrapped, so a
// route the handler grows later is mounted here the moment it exists and
// the parity test has nothing to catch.
func RegisterIngest(mux *http.ServeMux, h *ingest.Handler) {
	for _, r := range h.Routes() {
		mux.Handle(r.Pattern, withRequestAgentVersion(r.Handler))
	}
}

// withRequestAgentVersion stamps the delivering client's build onto the
// request context, through the ingest package's own parser of the header, so
// the row and the "ingest batch" line read the same build from the same
// bytes. The agent identifies every upload as "loop-sessions/<build>";
// anything else (a curl during an incident, a browser) carries no version
// rather than a made-up one, which is what lets the two be told apart in the
// data afterwards.
func withRequestAgentVersion(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := ingest.AgentBuild(r.UserAgent()); v != "" {
			r = r.WithContext(WithAgentVersion(r.Context(), v))
		}
		next.ServeHTTP(w, r)
	})
}

// PutHealthReport stores one self-telemetry sample.
//
// The failure travels up rather than being absorbed, because the fleet view
// alerts on the ABSENCE of these samples. A report quietly dropped here would
// make a machine that is reporting indistinguishable from one that has gone
// quiet, in the direction that hides the problem.
func (a ingestStore) PutHealthReport(ctx context.Context, email, deviceID string, report health.Report) error {
	if err := a.s.PutHealthReport(ctx, email, deviceID, report); err != nil {
		// Neither the address nor the report is named: this error reaches a log
		// line, and the log must not become a second copy of what the report says
		// about somebody's machine.
		return fmt.Errorf("app: put health report: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Device credentials
// ---------------------------------------------------------------------------

// ingestDevices binds credential verification to the port the upload endpoint
// declares.
type ingestDevices struct{ d *auth.Devices }

// NewIngestDevices adapts device-token verification to the shape the upload
// endpoint expects. A nil verifier is refused for the reason given on
// NewIngestStore, with the additional one that an upload endpoint whose verifier
// is missing is an anonymous upload endpoint.
func NewIngestDevices(d *auth.Devices) ingest.Devices {
	if d == nil {
		panic("app: NewIngestDevices requires a device verifier")
	}
	return ingestDevices{d: d}
}

// credentialFailures are the conditions under which the presented credential is
// unusable and the caller must be told so.
//
// They are listed rather than inferred because the classification runs the other
// way round: anything not on this list is treated as our failure, not theirs. Get
// that backwards and a database that is briefly unreachable answers a fleet of
// working laptops with 401, every one of them concludes its credential is dead,
// and the enrollment endpoint is the next thing to fall over.
//
// store.ErrNotFound is on the list because it is what the storage layer returns
// when a device-token lookup matches no row, and "no row" is precisely an
// unrecognised credential. A database that is down does not produce it.
var credentialFailures = []error{
	auth.ErrDeviceTokenMalformed,
	auth.ErrDeviceTokenUnknown,
	auth.ErrDeviceTokenRevoked,
	auth.ErrDeviceTokenExpired,
	auth.ErrDeviceRevoked,
	auth.ErrPrincipalDisabled,
	store.ErrNotFound,
}

// Verify authenticates a presented device credential and classifies the failure.
//
// The classification is the whole of this adapter's job. The endpoint answers 401
// for a credential failure and 503 for anything else, and those two answers mean
// opposite things to an agent: the first sends a laptop to re-enrol, the second
// makes it keep everything it has captured and try again later.
func (a ingestDevices) Verify(ctx context.Context, presented string) (ingest.Identity, error) {
	id, err := a.d.Verify(ctx, presented)
	if err != nil {
		for _, bad := range credentialFailures {
			if errors.Is(err, bad) {
				// The specific condition stays wrapped underneath. The endpoint
				// deliberately does not act on it — an attacker who learns that a
				// token was real but revoked has learned something — but discarding
				// it here would make the distinction unrecoverable for anyone who
				// later needs to tell a revocation from a guess.
				return ingest.Identity{}, fmt.Errorf("%w: %w", ingest.ErrUnauthenticated, err)
			}
		}
		return ingest.Identity{}, fmt.Errorf("app: verify device credential: %w", err)
	}
	// The role is not carried over. The upload path makes no permission decision:
	// every row it writes is attributed to the principal that owns the delivering
	// device, and a role passed into it would be a second place authorization could
	// be decided from.
	return ingest.Identity{Email: id.Email, DeviceID: id.DeviceID}, nil
}
