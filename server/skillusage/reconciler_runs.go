package skillusage

// POST /v1/skill-invocations/reconciler-runs: a reconciler's run record
// (design 3.7, 5.4, 5.7). One row per run, keyed on the platform and the
// run's own key, so the compliance report can tell a day with no run from
// a day with no activations and the platform summary can read the newest
// run. Source tokens only, and only those minted with reconciler among
// their origins: a beacon token, which every Devin session can read, is
// 401 here, so it can neither pre-claim a run date nor blank a real run.
//
// A retry with the same counts is a duplicate; one with different counts is
// a conflict the caller is told about, never a silent no-op. The platform
// is the token row's: a body may repeat it and may not contradict it.

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// ReconcilerRunsPath is the route the two reconcilers have compiled in.
const ReconcilerRunsPath = InvocationsPath + "/reconciler-runs"

// reasonRunConflict is the seventh 7.1 rejection reason: a retried run
// whose counts differ from the stored row's (design 3.7).
const reasonRunConflict = "run_conflict"

// ReconcilerStore is the ledger behind the route. The handler's Store is
// LS-2's transactional port; this one is asserted at request time and
// satisfied by the same adapter (server/app), which the compile-time check
// there pins, so a server that mounts the route always has the ledger.
type ReconcilerStore interface {
	// RecordReconcilerRun inserts the run or reads back the stored one:
	// duplicate when the counts match, conflict when they do not.
	RecordReconcilerRun(ctx context.Context, tokenID string, r store.ReconcilerRun) (duplicate bool, conflict bool, err error)
	// LogSoftRevokedReconcilerRun writes the run's line for a limit-0
	// token, which records nothing: the post is answered a duplicate, and
	// the line is the only trace an operator has of a reconciler still
	// posting on a rotated token (review-1 finding 3).
	LogSoftRevokedReconcilerRun(ctx context.Context, tokenID string, r store.ReconcilerRun)
}

// errNoReconcilerStore is the server's own mistake, answered 503: the store
// behind the route does not carry the ledger.
var errNoReconcilerStore = errors.New("skillusage: the store carries no reconciler run ledger")

// reconcilerRunRequest is the wire body: the ReconcilerRun fields in
// snake_case, every one a pointer so an absent key is told from a zero.
// agent_platform is optional and, when present, must equal the token's.
type reconcilerRunRequest struct {
	AgentPlatform      *string `json:"agent_platform"`
	IdempotencyKey     *string `json:"idempotency_key"`
	WindowStart        *string `json:"window_start"`
	WindowEnd          *string `json:"window_end"`
	SessionsScanned    *int    `json:"sessions_scanned"`
	SessionsWithEvents *int    `json:"sessions_with_events"`
	RowsPosted         *int    `json:"rows_posted"`
	Truncated          *int    `json:"truncated"`
}

// handleReconcilerRun is POST /v1/skill-invocations/reconciler-runs.
func (h *Handler) handleReconcilerRun(w http.ResponseWriter, r *http.Request) {
	now := h.now().UTC()
	ctx := r.Context()
	line := lineFields{shape: shapeUnknown, state: store.TokenStateNone}

	// Source tokens only: a device token names a laptop, and a laptop is
	// never a reconciler. Refused by prefix before any lookup, like the
	// invocation route's gate.
	bearer := auth.BearerToken(r)
	if !strings.HasPrefix(bearer, store.SourceTokenPrefix) {
		if strings.HasPrefix(bearer, auth.TokenPrefix) {
			line.shape = shapeDevice
		}
		h.reject(ctx, w, line, verdict{status: http.StatusUnauthorized, reason: reasonUnauthenticated})
		return
	}
	line.shape = shapeSource
	if !h.shed.allow(clientAddr(r), now) {
		h.reject(ctx, w, line, verdict{status: http.StatusTooManyRequests, reason: reasonRateLimited, retry: retryAfterLock})
		return
	}
	var req reconcilerRunRequest
	size, v := decodeBody(w, r, h.maxBody, &req)
	line.bytes = size
	if v != nil {
		h.reject(ctx, w, line, *v)
		return
	}
	if !h.sem.tryAcquire() {
		h.reject(ctx, w, line, verdict{status: http.StatusTooManyRequests, reason: reasonRateLimited, retry: retryAfterLock})
		return
	}
	defer h.sem.release()

	out, err := h.serveRun(ctx, bearer, req, now)
	if err != nil {
		if store.IsLockWait(err) {
			h.reject(ctx, w, line, verdict{status: http.StatusTooManyRequests, reason: reasonRateLimited, retry: retryAfterLock, token: out.token})
			return
		}
		op := out.op
		if op == "" {
			op = "record reconciler run"
		}
		h.storeFailed(ctx, r, op, err, out.token)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "unavailable"})
		return
	}
	if out.status != http.StatusOK {
		h.reject(ctx, w, line, out)
		return
	}
	writeJSON(w, http.StatusOK, response{Duplicate: out.dup})
}

// serveRun is the verify, the refusals it decides, the body rules, then
// the ledger. The verify's touch commits on its own, as on the invocation
// route, so a refused body has still used a window slot.
func (h *Handler) serveRun(ctx context.Context, bearer string, req reconcilerRunRequest, now time.Time) (verdict, error) {
	hash := auth.HashToken(bearer)
	out := verdict{op: "verify source token"}
	tok, err := h.store.AuthenticateSource(ctx, hash, Scope)
	if err != nil {
		return out, err
	}
	if tok.State == store.TokenStateNone {
		return verdict{status: http.StatusUnauthorized, reason: reasonUnauthenticated}, nil
	}
	out.token = &tok
	// A live token minted without the reconciler origin is a beacon or a
	// hook credential: 401, the same as a dead one, since what it lacks is
	// the authority (design 3.7 DEV-c, SECURITY1-2).
	if tok.State != store.TokenStateLive || !tok.Allows(store.OriginReconciler) {
		out.status, out.reason = http.StatusUnauthorized, reasonUnauthenticated
		return out, nil
	}
	if !h.shed.allow(hex.EncodeToString(hash), now) || tok.OverCap() {
		out.status, out.reason, out.retry = http.StatusTooManyRequests, reasonRateLimited, retryAfterCap
		return out, nil
	}
	run, ferr := assembleRun(req, tok, now)
	if ferr != nil {
		out.status, out.reason, out.field = http.StatusBadRequest, ferr.reason, ferr.field
		return out, nil
	}
	ledger, ok := h.store.(ReconcilerStore)
	if !ok {
		out.op = "record reconciler run"
		return out, errNoReconcilerStore
	}
	out.status = http.StatusOK
	if tok.SoftRevoked() {
		// Limit 0: touched and counted, nothing recorded, the caller told
		// its run was a duplicate, as the invocation route answers. The
		// run's line still goes out, carrying soft_revoked, so the post is
		// visible rather than silent (review-1 finding 3).
		out.dup = true
		ledger.LogSoftRevokedReconcilerRun(ctx, tok.ID, run)
		return out, nil
	}
	out.op = "record reconciler run"
	dup, conflict, err := ledger.RecordReconcilerRun(ctx, tok.ID, run)
	if err != nil {
		return out, err
	}
	if conflict {
		out.status, out.reason = http.StatusConflict, reasonRunConflict
		return out, nil
	}
	out.dup = dup
	return out, nil
}

// assembleRun applies the body rules in order and builds the run. The
// platform is the token's; a body naming another is refused with the
// mismatch reason on the line (CB-11). The window is required, ordered,
// and may not end in the future beyond the clamp the row route allows a
// reconciler's window, since a future window_end is how a holder would
// pre-claim a run date (SECURITY3-1). Counts are whole numbers and fit the
// INTEGER columns of 0023: a count above 2^31-1 used to reach Postgres,
// come back "integer out of range" and be answered 503, so the reconciler
// retried a permanently invalid body forever while the operator saw a store
// failure rather than a client one (adversarial finding 5).
func assembleRun(req reconcilerRunRequest, tok store.SourceToken, now time.Time) (store.ReconcilerRun, *fieldError) {
	var run store.ReconcilerRun
	if !in(store.ReconcilerPlatforms, tok.Platform) {
		return run, invalid("agent_platform")
	}
	if req.AgentPlatform != nil && *req.AgentPlatform != tok.Platform {
		return run, &fieldError{field: "agent_platform", reason: reasonPlatformMismatch}
	}
	run.AgentPlatform = tok.Platform
	if req.IdempotencyKey == nil || !ingest.IDShape.MatchString(*req.IdempotencyKey) {
		return run, invalid("idempotency_key")
	}
	run.IdempotencyKey = *req.IdempotencyKey
	start, ok := parseTime(req.WindowStart)
	if !ok {
		return run, invalid("window_start")
	}
	end, ok := parseTime(req.WindowEnd)
	if !ok {
		return run, invalid("window_end")
	}
	if end.After(now.Add(clampFuture)) {
		return run, invalid("window_end")
	}
	if start.After(end) {
		return run, invalid("window_start")
	}
	run.WindowStart, run.WindowEnd = start, end
	for _, c := range []struct {
		name string
		v    *int
		dst  *int
	}{
		{"sessions_scanned", req.SessionsScanned, &run.SessionsScanned},
		{"sessions_with_events", req.SessionsWithEvents, &run.SessionsWithEvents},
		{"rows_posted", req.RowsPosted, &run.RowsPosted},
		{"truncated", req.Truncated, &run.Truncated},
	} {
		if c.v == nil {
			continue
		}
		if *c.v < 0 || *c.v > math.MaxInt32 {
			return run, invalid(c.name)
		}
		*c.dst = *c.v
	}
	return run, nil
}

// decodeBody reads one JSON document into dst under the cap, refusing
// unknown keys and a second document, measuring the bytes read rather than
// the caller's Content-Length. The invocation route's decoder is bound to
// its own request type, so this one takes a destination.
func decodeBody(w http.ResponseWriter, r *http.Request, limit int64, dst any) (int64, *verdict) {
	counted := &countingReader{r: http.MaxBytesReader(w, r.Body, limit)}
	dec := json.NewDecoder(counted)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &tooLarge):
			return counted.n, &verdict{status: http.StatusRequestEntityTooLarge, reason: reasonBodyTooLarge}
		case errors.As(err, &typeErr):
			return counted.n, &verdict{status: http.StatusBadRequest, reason: reasonInvalidPayload, field: typeErr.Field}
		}
		field := "body"
		if m := unknownField.FindStringSubmatch(err.Error()); m != nil {
			field = m[1]
		}
		return counted.n, &verdict{status: http.StatusBadRequest, reason: reasonInvalidPayload, field: field}
	}
	if _, err := dec.Token(); err != io.EOF {
		return counted.n, &verdict{status: http.StatusBadRequest, reason: reasonInvalidPayload, field: "body"}
	}
	return counted.n, nil
}
