package app

// This file binds *store.Store to the two ports the skill-usage routes
// declare, and nothing else: the transactional store the POST route writes
// through, and the minter behind the read API's self-service laptop token.
// No policy lives here. The route decides what is well-formed and who may
// post; the store decides the key and the merge; this file translates.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/loopai-hq/agent-sessions/server/api"
	"github.com/loopai-hq/agent-sessions/server/skillusage"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// newSkillUsage builds the skill-invocation route over the store and the
// same device verifier the upload route uses, so the two can never
// disagree about which laptop is live.
func newSkillUsage(st *store.Store, log *slog.Logger) (*skillusage.Handler, error) {
	return skillusage.New(skillusage.Options{
		Store:   NewSkillUsageStore(st),
		Devices: newStoreDevices(st),
		Logger:  log.With("component", "skillusage"),
	})
}

// RegisterSkillUsage mounts the skill-invocation routes on the server's
// mux, from the handler's own list, so a route the handler grows later is
// mounted the moment it exists and the parity test has nothing to catch.
// No wrapper: the route reads nothing off the request the handler cannot
// read itself.
func RegisterSkillUsage(mux *http.ServeMux, h *skillusage.Handler) {
	for _, r := range h.Routes() {
		mux.Handle(r.Pattern, r.Handler)
	}
}

// skillUsageStore adapts the store to the route's transactional port.
type skillUsageStore struct{ s *store.Store }

// NewSkillUsageStore returns the persistence behind the skill-invocation
// route. A nil store is refused at composition time for the reason
// newStoreDevices gives.
func NewSkillUsageStore(s *store.Store) skillusage.Store {
	if s == nil {
		panic("app: NewSkillUsageStore requires a store")
	}
	return skillUsageStore{s: s}
}

// AuthenticateSource runs the verify's touch in a bounded transaction of
// its own and commits it before returning, so the token row is held for
// that one statement and never for the length of a request: the row is
// shared by every post under the token, and a burst under one (parallel
// cloud-hook sessions, a reconciler posting with a pool) would otherwise
// queue on it past the lock bound and be answered 429, rows lost rather
// than delayed. The error comes back untouched for the same reason InTx
// gives.
func (a skillUsageStore) AuthenticateSource(ctx context.Context, hash []byte, scope string) (store.SourceToken, error) {
	var tok store.SourceToken
	err := a.s.InSourceTx(ctx, func(ctx context.Context, q store.Queryer) error {
		var err error
		tok, err = a.s.AuthenticateSourceToken(ctx, q, hash, scope)
		return err
	})
	if err != nil {
		return store.SourceToken{}, err
	}
	return tok, nil
}

// InTx opens the store's bounded transaction (the lock bound is set
// inside) and hands fn's error back untouched: the route reads the
// sqlstate off it to tell a lock wait from a failure.
func (a skillUsageStore) InTx(ctx context.Context, fn func(context.Context, skillusage.Tx) error) error {
	return a.s.InSourceTx(ctx, func(ctx context.Context, q store.Queryer) error {
		return fn(ctx, skillUsageTx{s: a.s, q: q})
	})
}

// skillUsageTx is the same store bound to one open transaction.
type skillUsageTx struct {
	s *store.Store
	q store.Queryer
}

func (t skillUsageTx) SessionOwner(ctx context.Context, sessionRef string) (string, bool, error) {
	return t.s.SessionOwner(ctx, t.q, sessionRef)
}

func (t skillUsageTx) ActorKnown(ctx context.Context, email string) (bool, error) {
	return t.s.ActorKnown(ctx, t.q, email)
}

func (t skillUsageTx) Upsert(ctx context.Context, row store.SkillRow) (store.UpsertOutcome, error) {
	return t.s.UpsertSkillInvocation(ctx, t.q, row)
}

// ---------------------------------------------------------------------------
// The laptop mint
// ---------------------------------------------------------------------------

// apiSourceTokens adapts the store's mint to the read API's port.
type apiSourceTokens struct{ s *store.Store }

// NewAPISourceTokens returns the minter behind POST /v1/source-tokens/laptop.
func NewAPISourceTokens(s *store.Store) api.SourceTokenMinter {
	if s == nil {
		panic("app: NewAPISourceTokens requires a store")
	}
	return apiSourceTokens{s: s}
}

// MintSourceToken mints on the viewer's behalf. The route fixed every
// column but the label and the lifetime; the store writes the row and its
// audit row in one transaction. A field the store refuses is an error the
// route answers 500 with, since the route validated the two fields it lets
// through and anything else here is the server's own mistake.
func (a apiSourceTokens) MintSourceToken(ctx context.Context, v api.Viewer, m api.SourceTokenMint) (api.MintedSourceToken, error) {
	bound := m.BoundActorEmail
	req := store.MintRequest{
		Platform:        m.Platform,
		Environment:     m.Environment,
		Scope:           m.Scope,
		Label:           m.Label,
		ExpiresInDays:   m.ExpiresInDays,
		AllowedOrigins:  m.AllowedOrigins,
		BoundActorEmail: &bound,
	}
	plaintext, row, err := a.s.MintSourceToken(ctx, store.Viewer{Email: v.Email, Role: store.Role(v.Role)}, req)
	if err != nil {
		var me *store.MintError
		if errors.As(err, &me) {
			return api.MintedSourceToken{}, fmt.Errorf("app: the laptop mint's %s was refused: %w", me.Field, err)
		}
		return api.MintedSourceToken{}, err
	}
	out := api.MintedSourceToken{ID: row.ID, Token: plaintext}
	if row.ExpiresAt != nil {
		out.ExpiresAt = *row.ExpiresAt
	}
	return out, nil
}
