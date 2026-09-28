package skillusage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/skilllog"
	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/ingest"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// The line messages and the rejection reasons (design 7.1). The strings
// are the filters in examples/deploy-gcp/monitoring/metrics, so a rename is a
// metric that silently reads zero. They come from internal/skilllog
// because the catalog PUT in server/api emits the refusal line too and
// must read the same literal without importing this package, which would
// pull the store behind server/api's port boundary (LS-3 review-3).
const (
	lineAccepted    = skilllog.Accepted
	lineRejected    = skilllog.Rejected
	lineStoreFailed = skilllog.StoreFailed

	reasonUnauthenticated  = "unauthenticated"
	reasonRateLimited      = "rate_limited"
	reasonInvalidPayload   = "invalid_payload"
	reasonBodyTooLarge     = "body_too_large"
	reasonPlatformMismatch = "platform_mismatch"
	reasonForbidden        = "forbidden"
)

// The token shapes the rejected line names. The bearer itself is never
// bound to a line.
const (
	shapeDevice  = "device"
	shapeSource  = "source"
	shapeUnknown = "unknown"
)

// verdict is what one request came to. The verify's touch has already
// committed by the time one is decided, so a flood of bad payloads on a
// live token is counted against it, and the cap answers before the body
// is read a second time.
type verdict struct {
	status int
	reason string
	field  string
	retry  time.Duration
	// op names the statement that failed when the store returned an
	// error, for the store failed line; set before each statement so the
	// runbook reads which one.
	op      string
	dup     bool
	soft    bool
	clamped bool
	known   bool
	// The row as written, for the accepted line.
	row store.SkillRow
	// The token row when one verified, for the labels on either line.
	token *store.SourceToken
}

type response struct {
	Duplicate bool `json:"duplicate"`
}

type errorResponse struct {
	Error string `json:"error"`
	Field string `json:"field,omitempty"`
}

// handleInvocation is POST /v1/skill-invocations.
func (h *Handler) handleInvocation(w http.ResponseWriter, r *http.Request) {
	now := h.now().UTC()
	ctx := r.Context()
	line := lineFields{shape: shapeUnknown, state: store.TokenStateNone}

	// The prefix gate: anything that is not one of our two shapes is
	// refused before the body is read and before any lookup, so a stream
	// of guesses is a stream of string comparisons.
	bearer := auth.BearerToken(r)
	switch {
	case strings.HasPrefix(bearer, auth.TokenPrefix):
		line.shape = shapeDevice
	case strings.HasPrefix(bearer, store.SourceTokenPrefix):
		line.shape = shapeSource
	default:
		h.reject(ctx, w, line, verdict{status: http.StatusUnauthorized, reason: reasonUnauthenticated})
		return
	}

	// The shed on the forwarded address, ahead of everything the database
	// would have to do for this request. The bucket refills within the
	// minute, so the header says try again soon, not after the window.
	if !h.shed.allow(clientAddr(r), now) {
		h.reject(ctx, w, line, verdict{status: http.StatusTooManyRequests, reason: reasonRateLimited, retry: retryAfterLock})
		return
	}

	req, size, v := h.decode(w, r)
	line.bytes = size
	if v != nil {
		h.reject(ctx, w, line, *v)
		return
	}

	// The semaphore, held across the request's statements and nothing
	// else; a full one clears as soon as any of the four finishes.
	if !h.sem.tryAcquire() {
		h.reject(ctx, w, line, verdict{status: http.StatusTooManyRequests, reason: reasonRateLimited, retry: retryAfterLock})
		return
	}
	defer h.sem.release()

	var out verdict
	var err error
	if line.shape == shapeDevice {
		out, err = h.serveDevice(ctx, bearer, req, now)
	} else {
		out, err = h.serveSource(ctx, bearer, req, now)
	}
	if err != nil {
		if store.IsLockWait(err) {
			// The lock bound fired: on the verify, the token row is held
			// by another post's touch or an admin limit or revoke; on the
			// upsert, a skill row a derive batch holds. Either clears as
			// soon as the holder commits, so the emitter retries in a
			// second. The platform is the token row's when the verify
			// returned one, which a verify that timed out did not.
			h.reject(ctx, w, line, verdict{status: http.StatusTooManyRequests, reason: reasonRateLimited, retry: retryAfterLock, token: out.token})
			return
		}
		op := out.op
		if op == "" {
			op = "write skill invocation"
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
	h.accepted(ctx, line, out)
}

// serveDevice is the lsd_ path: the upload route's verifier, the
// per-device bucket, the owner check on a known session, then the row in
// the bounded transaction.
func (h *Handler) serveDevice(ctx context.Context, bearer string, req request, now time.Time) (verdict, error) {
	id, err := h.devices.Verify(ctx, bearer)
	if err != nil {
		if errors.Is(err, ingest.ErrUnauthenticated) {
			return verdict{status: http.StatusUnauthorized, reason: reasonUnauthenticated}, nil
		}
		// The credential may be fine; the lookup was not. 503, never 401:
		// a 401 would send a working laptop to re-enrol over a blip.
		return verdict{op: "verify device"}, err
	}
	if !h.perDev.allow(id.DeviceID, now) {
		return verdict{status: http.StatusTooManyRequests, reason: reasonRateLimited, retry: retryAfterCap}, nil
	}
	row, ferr := assemble(req, credential{device: &id}, now)
	if ferr != nil {
		return verdict{status: http.StatusBadRequest, reason: ferr.reason, field: ferr.field}, nil
	}
	if _, err := store.DedupeKey(row); err != nil {
		return verdict{status: http.StatusBadRequest, reason: reasonInvalidPayload, field: "idempotency_key"}, nil
	}
	out := verdict{status: http.StatusOK, row: row, known: true}
	err = h.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		if row.SessionRef != "" {
			out.op = "read session owner"
			owner, found, err := tx.SessionOwner(ctx, row.SessionRef)
			if err != nil {
				return err
			}
			// A device may post only on sessions its owner ran. A session
			// the store has not seen is not refused: the derived copy
			// replaces the API row on its key when it arrives.
			if found && !strings.EqualFold(owner, id.Email) {
				out = verdict{status: http.StatusForbidden, reason: reasonForbidden}
				return nil
			}
		}
		out.op = "write skill invocation"
		res, err := tx.Upsert(ctx, row)
		if err != nil {
			return err
		}
		out.dup = !(res.Inserted || res.Changed)
		out.clamped = row.TimeClamped
		return nil
	})
	return out, err
}

// serveSource is the lss_ path: the verify's touch, committed on its own,
// then the refusals it decides, then the row in the bounded transaction.
// The touch is committed whatever follows: a refused payload or a failed
// write has still used a window slot, since the counter bounds a flood
// and does not account for rows.
func (h *Handler) serveSource(ctx context.Context, bearer string, req request, now time.Time) (verdict, error) {
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
	if tok.State != store.TokenStateLive {
		out.status, out.reason = http.StatusUnauthorized, reasonUnauthenticated
		return out, nil
	}
	// The shed on the hash, only now that the verify says it is live: a
	// bucket per junk bearer would grow the map without bound.
	if !h.shed.allow(hex.EncodeToString(hash), now) || tok.OverCap() {
		out.status, out.reason, out.retry = http.StatusTooManyRequests, reasonRateLimited, retryAfterCap
		return out, nil
	}
	row, ferr := assemble(req, credential{source: &tok}, now)
	if ferr != nil {
		out.status, out.reason, out.field = http.StatusBadRequest, ferr.reason, ferr.field
		return out, nil
	}
	if _, err := store.DedupeKey(row); err != nil {
		out.status, out.reason, out.field = http.StatusBadRequest, reasonInvalidPayload, "idempotency_key"
		return out, nil
	}
	out.status, out.row, out.clamped = http.StatusOK, row, row.TimeClamped
	if tok.SoftRevoked() {
		// The soft revoke: touched and counted, nothing written, the
		// caller told its post was a duplicate.
		out.dup, out.soft = true, true
		return out, nil
	}
	err = h.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		if row.ActorEmail != nil {
			out.op = "read roster"
			known, err := tx.ActorKnown(ctx, *row.ActorEmail)
			if err != nil {
				return err
			}
			row.ActorKnown, out.known = known, known
			out.row = row
		}
		out.op = "write skill invocation"
		res, err := tx.Upsert(ctx, row)
		if err != nil {
			return err
		}
		out.dup = !(res.Inserted || res.Changed)
		return nil
	})
	return out, err
}

// ---------------------------------------------------------------------------
// The body
// ---------------------------------------------------------------------------

var unknownField = regexp.MustCompile(`json: unknown field "([^"]*)"`)

// decode reads the body under the cap and refuses unknown keys. The size
// is measured from the bytes read, not from Content-Length, which the
// caller chose.
func (h *Handler) decode(w http.ResponseWriter, r *http.Request) (request, int64, *verdict) {
	counted := &countingReader{r: http.MaxBytesReader(w, r.Body, h.maxBody)}
	dec := json.NewDecoder(counted)
	dec.DisallowUnknownFields()
	var req request
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &tooLarge):
			return req, counted.n, &verdict{status: http.StatusRequestEntityTooLarge, reason: reasonBodyTooLarge}
		case errors.As(err, &typeErr):
			return req, counted.n, &verdict{status: http.StatusBadRequest, reason: reasonInvalidPayload, field: typeErr.Field}
		}
		field := "body"
		if m := unknownField.FindStringSubmatch(err.Error()); m != nil {
			field = m[1]
		}
		return req, counted.n, &verdict{status: http.StatusBadRequest, reason: reasonInvalidPayload, field: field}
	}
	// One document only: a second value after the first is not a
	// request, and reading it would let a body hide fields past the one
	// the decoder validated.
	if _, err := dec.Token(); err != io.EOF {
		return req, counted.n, &verdict{status: http.StatusBadRequest, reason: reasonInvalidPayload, field: "body"}
	}
	return req, counted.n, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// ---------------------------------------------------------------------------
// Responses and lines
// ---------------------------------------------------------------------------

// lineFields is what a request contributes to its line before any
// verdict: the bearer's shape, the token's state when one was read, and
// the size.
type lineFields struct {
	shape, state string
	bytes        int64
}

func (h *Handler) reject(ctx context.Context, w http.ResponseWriter, line lineFields, v verdict) {
	if v.retry > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(v.retry.Seconds())))
	}
	// The wire code of every 400 is invalid_payload; the line's reason
	// tells a platform mismatch apart, since the runbook reads the line.
	code := v.reason
	if v.status == http.StatusBadRequest {
		code = reasonInvalidPayload
	}
	writeJSON(w, v.status, errorResponse{Error: code, Field: v.field})
	tokenID, platform := "", ""
	if v.token != nil {
		line.state = v.token.State
		tokenID, platform = v.token.ID, v.token.Platform
	}
	// platform is the token row's or nothing: a body's value would let a
	// prober grow the label set. field is gated the way ids are: an
	// unknown key is the one place a body's own text could name itself
	// onto a line, and the runbook groups 17b by this field.
	h.log.WarnContext(ctx, lineRejected,
		slog.String("reason", v.reason),
		slog.String("field", logField(v.field)),
		slog.String("token_shape", line.shape),
		slog.String("token_state", line.state),
		slog.String("source_token_id", tokenID),
		slog.String("platform", platform),
		slog.Int("status", v.status))
}

func (h *Handler) accepted(ctx context.Context, line lineFields, v verdict) {
	row := v.row
	attrs := []any{
		slog.String("trust", row.Trust),
		slog.String("platform", row.AgentPlatform),
		slog.String("environment", row.TokenEnvironment),
		slog.String("origin", row.Origin),
	}
	if v.token != nil {
		attrs = append(attrs, slog.String("source_token_id", v.token.ID))
	} else {
		attrs = append(attrs, slog.String("device_id", deref(row.DeviceID)), slog.String("email", deref(row.ActorEmail)))
	}
	attrs = append(attrs,
		slog.String("skill", logSkill(row)),
		slog.String("trigger", row.Trigger),
		slog.String("outcome", row.Outcome),
		slog.Bool("duplicate", v.dup),
		slog.Bool("soft_revoked", v.soft),
		slog.Bool("time_clamped", v.clamped),
		slog.Bool("actor_known", v.known),
		slog.Int64("bytes", line.bytes),
		slog.Int("status", http.StatusOK))
	h.log.InfoContext(ctx, lineAccepted, attrs...)
}

// logSkill is the skill as a line carries it: the normalised slug through
// the id gate, never the raw name, which an emitter chooses.
func logSkill(row store.SkillRow) string {
	if row.Skill == nil {
		return "other"
	}
	name := *row.Skill
	if row.Plugin != "" {
		name = row.Plugin + ":" + name
	}
	return ingest.LogID(name)
}

// storeFailed logs the detail behind a 503. The platform is the token
// row's when one verified.
func (h *Handler) storeFailed(ctx context.Context, r *http.Request, op string, err error, tok *store.SourceToken) {
	platform := ""
	if tok != nil {
		platform = tok.Platform
	}
	h.log.ErrorContext(ctx, lineStoreFailed,
		slog.String("op", op),
		slog.String("error", err.Error()),
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("platform", platform))
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// writeJSON encodes into a buffer before touching the writer, so a
// marshal failure cannot follow a committed status line.
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
