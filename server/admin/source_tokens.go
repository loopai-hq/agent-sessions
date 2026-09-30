package admin

// The source token routes (design 4c): mint a token for a platform's
// environment, change its per-minute limit, revoke it. Cookie POSTs by an
// admin, each refused unless the browser marked it same-origin, each
// writing its audit row in the transaction that performs the change, with
// the caller's role re-read inside it.
//
// The plaintext is returned once and exists nowhere else: the store keeps
// a hash, the audit row keeps the request minus it, and no line carries
// it. The laptop environment is refused here on purpose. A per-person
// token is minted by the person, self-service, so no admin ever holds a
// credential that writes rows in a colleague's name.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/server/auth"
)

// The audit row actions, the routes' names as the runbook reads them.
const (
	sourceTokenMintAction   = "source_token.mint"
	sourceTokenLimitAction  = "source_token.limit"
	sourceTokenRevokeAction = "source_token.revoke"
)

// laptopEnvironment is the one environment this route refuses.
const laptopEnvironment = "laptop"

// maxTokenLifetimeDays is the mandatory expiry's ceiling; a token in
// hostile hands never goes quiet on its own.
const maxTokenLifetimeDays = 180

// SourceTokenMint is the mint request as the store port takes it. Every
// field is the caller's except the actor, which is the verified admin.
type SourceTokenMint struct {
	Platform, Environment, Scope, Label string
	ExpiresInDays                       int
	AllowedOrigins                      []string
}

// SourceTokenIssued is a minted token: the plaintext, once, and the row.
type SourceTokenIssued struct {
	ID, Token                           string
	Platform, Environment, Scope, Label string
	ExpiresAt                           time.Time
	RateLimitPerMin                     int
	AllowedOrigins                      []string
}

// FieldError is a mint the store refused on one field, which the route
// answers 400 with the field named.
type FieldError struct {
	Field, Reason string
}

func (e *FieldError) Error() string { return "admin: " + e.Field + " " + e.Reason }

// ErrTokenNotFound means the id names no token. The route answers 404.
var ErrTokenNotFound = errors.New("admin: no such source token")

// mintRequest is the wire body. expires_in_days is a pointer because it is
// required, and a zero from an absent key would be indistinguishable from
// a caller asking for zero days.
type mintRequest struct {
	Platform        string   `json:"platform"`
	Environment     string   `json:"environment"`
	Scope           string   `json:"scope"`
	Label           string   `json:"label"`
	ExpiresInDays   *int     `json:"expires_in_days"`
	AllowedOrigins  []string `json:"allowed_origins"`
	BoundActorEmail *string  `json:"bound_actor_email"`
}

// auditDetail is the body as the audit row records it: every field, none
// a secret.
func (m mintRequest) auditDetail() map[string]any {
	d := map[string]any{
		"platform": m.Platform, "environment": m.Environment, "scope": m.Scope, "label": m.Label,
		"allowed_origins": m.AllowedOrigins,
	}
	if m.ExpiresInDays != nil {
		d["expires_in_days"] = *m.ExpiresInDays
	}
	return d
}

type mintResponse struct {
	ID              string    `json:"id"`
	Token           string    `json:"token"`
	Platform        string    `json:"platform"`
	Environment     string    `json:"environment"`
	Scope           string    `json:"scope"`
	Label           string    `json:"label,omitempty"`
	ExpiresAt       time.Time `json:"expires_at"`
	RateLimitPerMin int       `json:"rate_limit_per_min"`
	AllowedOrigins  []string  `json:"allowed_origins"`
}

type limitRequest struct {
	RateLimitPerMin *int `json:"rate_limit_per_min"`
}

type limitResponse struct {
	ID              string `json:"id"`
	RateLimitPerMin int    `json:"rate_limit_per_min"`
}

type revokeResponse struct {
	ID      string `json:"id"`
	Revoked bool   `json:"revoked"`
}

// tokenCaller is the part of every token route that is the same: the
// caller identified and authorized (a member sees the admin routes' 404),
// then the same-origin check, since a cross-site form carrying the
// dashboard cookie is exactly the request the check exists to stop and
// /revoke has no body for anything else to catch.
func (h *Handler) tokenCaller(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	id, ok := h.identify(w, r)
	if !ok {
		return Identity{}, false
	}
	if _, err := authorizeAdmin(r.Context(), h.store, id.Email); err != nil {
		h.writeAuthzFailure(w, r, err)
		return Identity{}, false
	}
	if !auth.SameOriginPOST(r) {
		writeError(w, http.StatusForbidden, "forbidden", "same-origin POST required")
		return Identity{}, false
	}
	return id, true
}

// handleMintSourceToken is POST /v1/admin/source-tokens.
func (h *Handler) handleMintSourceToken(w http.ResponseWriter, r *http.Request) {
	id, ok := h.tokenCaller(w, r)
	if !ok {
		return
	}
	var req mintRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "malformed body: "+err.Error())
		return
	}
	if req.ExpiresInDays == nil || *req.ExpiresInDays < 1 || *req.ExpiresInDays > maxTokenLifetimeDays {
		writeFieldError(w, "expires_in_days", "is required, between 1 and 180")
		return
	}
	if strings.TrimSpace(req.Environment) == laptopEnvironment {
		// Not a 400: the request is well-formed and the answer is no. A
		// bound token is minted by its holder, never by an admin.
		writeError(w, http.StatusForbidden, "forbidden", "laptop tokens are minted self-service by their holder")
		return
	}
	if req.BoundActorEmail != nil {
		writeFieldError(w, "bound_actor_email", "is set by the self-service laptop mint only")
		return
	}
	if len(req.AllowedOrigins) == 0 {
		req.AllowedOrigins = []string{"beacon"}
	}

	var issued SourceTokenIssued
	err := h.store.InTx(r.Context(), func(ctx context.Context, tx Tx) error {
		issued = SourceTokenIssued{}
		if _, err := authorizeAdmin(ctx, tx, id.Email); err != nil {
			return err
		}
		out, err := tx.MintSourceToken(ctx, id.Email, SourceTokenMint{
			Platform: req.Platform, Environment: req.Environment, Scope: req.Scope, Label: req.Label,
			ExpiresInDays: *req.ExpiresInDays, AllowedOrigins: req.AllowedOrigins,
		})
		if err != nil {
			return err
		}
		issued = out
		return tx.RecordAdminAction(ctx, id.Email, sourceTokenMintAction, out.ID, req.auditDetail())
	})
	if err != nil {
		h.writeTokenFailure(w, r, "mint source token", err)
		return
	}
	h.log.Info("source token minted",
		"actor", id.Email, "source_token_id", issued.ID, "platform", issued.Platform,
		"environment", issued.Environment, "scope", issued.Scope, "expires_at", issued.ExpiresAt)
	writeJSON(w, http.StatusOK, mintResponse(issued))
}

// handleSetSourceTokenLimit is POST /v1/admin/source-tokens/{id}/limit. A
// limit of 0 is the soft revoke of design 4c.
func (h *Handler) handleSetSourceTokenLimit(w http.ResponseWriter, r *http.Request) {
	id, ok := h.tokenCaller(w, r)
	if !ok {
		return
	}
	target := strings.TrimSpace(r.PathValue("id"))
	var req limitRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "malformed body: "+err.Error())
		return
	}
	if req.RateLimitPerMin == nil || *req.RateLimitPerMin < 0 || *req.RateLimitPerMin > 100000 {
		writeFieldError(w, "rate_limit_per_min", "is required, between 0 and 100000")
		return
	}
	err := h.store.InTx(r.Context(), func(ctx context.Context, tx Tx) error {
		if _, err := authorizeAdmin(ctx, tx, id.Email); err != nil {
			return err
		}
		if err := tx.SetSourceTokenLimit(ctx, target, *req.RateLimitPerMin); err != nil {
			return err
		}
		return tx.RecordAdminAction(ctx, id.Email, sourceTokenLimitAction, target, map[string]any{"rate_limit_per_min": *req.RateLimitPerMin})
	})
	if err != nil {
		h.writeTokenFailure(w, r, "set source token limit", err)
		return
	}
	h.log.Info("source token limit set", "actor", id.Email, "source_token_id", target, "rate_limit_per_min", *req.RateLimitPerMin)
	writeJSON(w, http.StatusOK, limitResponse{ID: target, RateLimitPerMin: *req.RateLimitPerMin})
}

// handleRevokeSourceToken is POST /v1/admin/source-tokens/{id}/revoke: the
// hard revoke, which frees the hash slot for the rotation's successor.
func (h *Handler) handleRevokeSourceToken(w http.ResponseWriter, r *http.Request) {
	id, ok := h.tokenCaller(w, r)
	if !ok {
		return
	}
	target := strings.TrimSpace(r.PathValue("id"))
	// No body is the normal request; a body is read to the cap and ignored
	// rather than refused, since a client that sends {} is not wrong.
	_, _ = io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, maxBodyBytes))
	err := h.store.InTx(r.Context(), func(ctx context.Context, tx Tx) error {
		if _, err := authorizeAdmin(ctx, tx, id.Email); err != nil {
			return err
		}
		if err := tx.RevokeSourceToken(ctx, target, id.Email); err != nil {
			return err
		}
		return tx.RecordAdminAction(ctx, id.Email, sourceTokenRevokeAction, target, nil)
	})
	if err != nil {
		h.writeTokenFailure(w, r, "revoke source token", err)
		return
	}
	h.log.Info("source token revoked", "actor", id.Email, "source_token_id", target)
	writeJSON(w, http.StatusOK, revokeResponse{ID: target, Revoked: true})
}

// writeTokenFailure maps a token transaction's error: a demotion inside
// it is the admin routes' 404, an unknown id is 404, a refused field is
// 400 with the field, and anything else is a 500 whose detail goes to the
// log.
func (h *Handler) writeTokenFailure(w http.ResponseWriter, r *http.Request, op string, err error) {
	var fe *FieldError
	switch {
	case errors.Is(err, errNotAdmin), errors.Is(err, ErrTokenNotFound):
		writeNotFound(w)
	case errors.As(err, &fe):
		writeFieldError(w, fe.Field, fe.Reason)
	default:
		h.fail(r, op, err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func writeFieldError(w http.ResponseWriter, field, reason string) {
	writeJSON(w, http.StatusBadRequest, struct {
		Error   string `json:"error"`
		Field   string `json:"field"`
		Message string `json:"message"`
	}{Error: "invalid_request", Field: field, Message: field + " " + reason})
}
