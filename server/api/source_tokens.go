package api

// POST /v1/source-tokens/laptop: a member mints the per-person token an
// unenrolled laptop's skill hook posts under (design 4c, C1).
//
// Self-service on purpose. The token writes claimed rows in one person's
// name, so the person is the only one who may mint it: an admin route
// taking a bound_actor_email would leave every admin holding a credential
// that attributes rows to a colleague. The server sets everything but the
// label and the lifetime, and the body may carry nothing else, so a caller
// cannot widen the token's origins or point it at another environment.
//
// A cookie POST that changes state, so it carries the same-origin check
// every such route does: the browser sets Sec-Fetch-Site and a page's
// script cannot, and a cross-site form carrying the dashboard cookie is
// refused before the body is read. The audit row lands in the mint's own
// transaction, in the store.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/loopai-hq/loop-sessions/server/auth"
)

// The values the server fixes on a laptop token. The origin is hook
// because that is what the hook script posts; the default of beacon would
// answer its first post with 400 on origin.
const (
	laptopPlatform      = "claude_code"
	laptopEnvironment   = "laptop"
	laptopScope         = "skill-invocations"
	laptopOrigin        = "hook"
	laptopDefaultDays   = 180
	laptopMaxDays       = 180
	laptopTokenBodyByte = 1 << 10
)

// SourceTokenMint is the mint as the port takes it: every column the row
// needs, set here so the test can read what the handler decided.
type SourceTokenMint struct {
	Platform, Environment, Scope, Label string
	ExpiresInDays                       int
	AllowedOrigins                      []string
	BoundActorEmail                     string
}

// MintedSourceToken is the minted row: the plaintext, once, and what a
// caller needs to know about it.
type MintedSourceToken struct {
	ID, Token string
	ExpiresAt time.Time
}

// SourceTokenMinter mints a token on the viewer's behalf, writing its audit
// row in the same transaction. The app adapter binds it to the store.
type SourceTokenMinter interface {
	MintSourceToken(ctx context.Context, v Viewer, m SourceTokenMint) (MintedSourceToken, error)
}

// laptopMintRequest is the whole wire body: any other key is refused.
type laptopMintRequest struct {
	Label         string `json:"label"`
	ExpiresInDays *int   `json:"expires_in_days"`
}

type laptopMintResponse struct {
	ID             string    `json:"id"`
	Token          string    `json:"token"`
	Platform       string    `json:"platform"`
	Environment    string    `json:"environment"`
	Scope          string    `json:"scope"`
	AllowedOrigins []string  `json:"allowed_origins"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// handleMintLaptopToken serves the route.
func (h *Handler) handleMintLaptopToken(w http.ResponseWriter, r *http.Request) {
	v, ok := h.viewer(w, r)
	if !ok {
		return
	}
	if !auth.SameOriginPOST(r) {
		writeError(w, http.StatusForbidden, "forbidden", "same-origin POST required")
		return
	}
	var body laptopMintRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, laptopTokenBodyByte))
	dec.DisallowUnknownFields()
	// No body is the same request as {}: every field has a default, and
	// the derive rerun route reads an empty body the same way.
	if err := dec.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		badRequest(w, "body", errors.New("malformed: "+err.Error()))
		return
	}
	days := laptopDefaultDays
	if body.ExpiresInDays != nil {
		days = *body.ExpiresInDays
	}
	if days < 1 || days > laptopMaxDays {
		badRequest(w, "expires_in_days", errors.New("must be between 1 and 180"))
		return
	}
	if len(body.Label) > 80 {
		badRequest(w, "label", errors.New("must be at most 80 characters"))
		return
	}
	minted, err := h.tokens.MintSourceToken(r.Context(), v, SourceTokenMint{
		Platform:        laptopPlatform,
		Environment:     laptopEnvironment,
		Scope:           laptopScope,
		Label:           body.Label,
		ExpiresInDays:   days,
		AllowedOrigins:  []string{laptopOrigin},
		BoundActorEmail: v.Email,
	})
	if err != nil {
		h.fail(r, "mint laptop token", err)
		writeInternal(w)
		return
	}
	h.log.Info("laptop token minted", "email", v.Email, "source_token_id", minted.ID, "expires_at", minted.ExpiresAt)
	// 200 with the token in the body, once: it is the whole credential
	// and is never put in a URL this server logs.
	writeJSON(w, http.StatusOK, laptopMintResponse{
		ID: minted.ID, Token: minted.Token, Platform: laptopPlatform, Environment: laptopEnvironment,
		Scope: laptopScope, AllowedOrigins: []string{laptopOrigin}, ExpiresAt: minted.ExpiresAt,
	})
}
