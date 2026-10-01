package app

// This file binds *store.Store to the port server/admin declares, and nothing
// else. It holds no policy: the admin package re-reads the caller's roster entry
// on every request and, for an edit, re-reads it inside the same transaction as
// the write. A permission decision made here as well would be a second copy of
// that rule, and of two copies of a permission rule the one that drifts is
// always the more permissive one.
//
// What this file does own is translation, and two translations here are
// load-bearing rather than mechanical. A health snapshot keeps its device id, so
// that coverage stays a per-machine question; a person with a working laptop and
// a dead one is not covered, and a snapshot that arrives without its device key
// collapses the two machines into one row that answers for both. An access event
// keeps its row id, which is the only thing the access log can page on, because
// two reads landing in the same millisecond are ordinary and a timestamp cursor
// would either repeat one of them or drop it.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/loopai-hq/loop-sessions/server/admin"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// adminStore adapts the store to the admin package's persistence port.
type adminStore struct{ s *store.Store }

// NewAdminStore builds the admin surface's view of the store.
func NewAdminStore(s *store.Store) admin.Store { return adminStore{s: s} }

// adminRosterReader is the identity the store's own admin gate is handed for
// the one admin read that still asks for one.
//
// admin.Store.ListPrincipals carries no viewer, deliberately: by the time it is
// called the admin package has already re-read the caller's roster entry for
// this request, and re-deriving that predicate here is exactly what the contract
// forbids. store.ListPrincipals predates that port and still gates on
// Viewer.IsAdmin, so this is what satisfies it. That makes the store's gate
// decorative on this one path and this adapter reachable only from a surface
// that has already established admin authority; routing it anywhere else would
// hand the whole roster to a member.
//
// It carries no email because a roster read writes no audit row. Inventing an
// address nobody holds would put a fiction into any log line that later grows
// one, and an audit trail naming a principal who does not exist is worse than
// one naming nobody.
var adminRosterReader = store.Viewer{Role: store.RoleAdmin}

// Principal resolves one roster entry, reporting absence as false rather than as
// an error. The boolean is the whole point of the newer store spelling: a
// zero-valued row reads as a member, and a member is somebody this package would
// answer authorization questions about.
func (a adminStore) Principal(ctx context.Context, email string) (admin.Principal, bool, error) {
	p, found, err := a.s.PrincipalLookup(ctx, email)
	if err != nil {
		return admin.Principal{}, false, err
	}
	if !found {
		return admin.Principal{}, false, nil
	}
	return toAdminPrincipal(p), true, nil
}

// ListPrincipals returns the whole roster.
//
// This and the three reads below allocate their result even when there is
// nothing to put in it, so an empty answer reaches the handler as an empty list
// rather than a nil slice. The admin routes marshal these straight into a JSON
// body, and a nil slice marshals as null: an operator's page would then have to
// tell "no rows" from "field absent" for a difference that does not exist, and a
// client iterating the field would fault on the empty case only.
func (a adminStore) ListPrincipals(ctx context.Context) ([]admin.Principal, error) {
	ps, err := a.s.ListPrincipals(ctx, adminRosterReader)
	if err != nil {
		if errors.Is(err, store.ErrNotAdmin) {
			// Unreachable unless the reader above stops satisfying the store's
			// gate. Saying which viewer was refused beats logging "caller is not
			// an admin" on a request whose caller has already been proven to be
			// one, which reads as an authorization bug in the admin package and
			// is not one.
			return nil, fmt.Errorf("app: the admin adapter's roster reader was refused by the store: %w", err)
		}
		return nil, err
	}
	out := make([]admin.Principal, 0, len(ps))
	for _, p := range ps {
		out = append(out, toAdminPrincipal(p))
	}
	return out, nil
}

func (a adminStore) LatestHealth(ctx context.Context) ([]admin.HealthSnapshot, error) {
	snaps, err := a.s.LatestHealth(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]admin.HealthSnapshot, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, toAdminSnapshot(s))
	}
	return out, nil
}

func (a adminStore) EnrolledDevices(ctx context.Context) ([]admin.Device, error) {
	devices, err := a.s.EnrolledDevices(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]admin.Device, 0, len(devices))
	for _, d := range devices {
		out = append(out, toAdminDevice(d))
	}
	return out, nil
}

func (a adminStore) AccessEvents(ctx context.Context, q admin.AccessQuery) ([]admin.AccessEvent, error) {
	rows, err := a.s.AccessEvents(ctx, toStoreAccessFilter(q))
	if err != nil {
		return nil, err
	}
	out := make([]admin.AccessEvent, 0, len(rows))
	for _, r := range rows {
		out = append(out, toAdminAccessEvent(r))
	}
	return out, nil
}

// InTx runs fn inside the store's transaction, so that the lockout guard, the
// roster write and the audit row that describes it are one atomic unit.
//
// fn's error travels back to the caller untouched. The admin handler decides
// between a 404, a 409 and a 500 by matching its own sentinels on this value,
// and this adapter re-wrapping an error the handler produced would put a second
// layer between a sentinel and the switch that reads it, for no context the
// handler did not already have. Failures the store raises on its own account are
// already wrapped where they happen.
func (a adminStore) InTx(ctx context.Context, fn func(context.Context, admin.Tx) error) error {
	return a.s.InTx(ctx, func(ctx context.Context, tx store.AdminTx) error {
		return fn(ctx, adminTx{tx: tx})
	})
}

// adminTx is the same translation bound to one open transaction, so the roster
// entry the guard reads inside the write is converted by exactly the code that
// converted the one the request was authorized against. A divergence there would
// let somebody be an admin to the check outside the transaction and not to the
// re-check inside it, which is the check that exists to survive a demotion
// landing mid-request.
type adminTx struct{ tx store.AdminTx }

func (t adminTx) Principal(ctx context.Context, email string) (admin.Principal, bool, error) {
	p, found, err := t.tx.Principal(ctx, email)
	if err != nil {
		return admin.Principal{}, false, err
	}
	if !found {
		return admin.Principal{}, false, nil
	}
	return toAdminPrincipal(p), true, nil
}

// CountActiveAdminsExcept passes straight through. It is the point at which two
// demotions racing each other are serialised, and the lock it takes belongs to
// the transaction the store opened, so there is nothing here to translate and
// nothing here that may retry.
func (t adminTx) CountActiveAdminsExcept(ctx context.Context, email string) (int, error) {
	return t.tx.CountActiveAdminsExcept(ctx, email)
}

func (t adminTx) SavePrincipal(ctx context.Context, p admin.Principal) error {
	return t.tx.SavePrincipal(ctx, toStorePrincipal(p))
}

func (t adminTx) RecordPrincipalChange(ctx context.Context, c admin.PrincipalChange) error {
	return t.tx.RecordPrincipalChange(ctx, toStorePrincipalChange(c))
}

// The skill derive rerun passes straight through: the audit row's shape
// and the two statements are the store's, and nothing here translates.
func (t adminTx) RecordAdminAction(ctx context.Context, actor, action, target string, detail any) error {
	return t.tx.RecordAdminAction(ctx, actor, action, target, detail)
}

func (t adminTx) EnqueueSkillRederiveSince(ctx context.Context, since time.Time) (int64, error) {
	return t.tx.EnqueueSkillRederiveSince(ctx, since)
}

func (t adminTx) ResetSkillDeriveStep(ctx context.Context) error {
	return t.tx.ResetSkillDeriveStep(ctx)
}

// The source token routes translate their request and their two
// sentinels: a field the store's normaliser refuses becomes the admin
// package's FieldError (a 400 naming it), an unknown id its ErrTokenNotFound
// (a 404). The plaintext passes through once and is held nowhere.
func (t adminTx) MintSourceToken(ctx context.Context, actor string, req admin.SourceTokenMint) (admin.SourceTokenIssued, error) {
	plaintext, row, err := t.tx.MintSourceToken(ctx, actor, store.MintRequest{
		Platform:       req.Platform,
		Environment:    req.Environment,
		Scope:          req.Scope,
		Label:          req.Label,
		ExpiresInDays:  req.ExpiresInDays,
		AllowedOrigins: req.AllowedOrigins,
	})
	if err != nil {
		return admin.SourceTokenIssued{}, tokenFailure(err)
	}
	out := admin.SourceTokenIssued{
		ID: row.ID, Token: plaintext, Platform: row.Platform, Environment: row.Environment, Scope: row.Scope,
		Label: row.Label, RateLimitPerMin: row.RateLimitPerMin, AllowedOrigins: row.AllowedOrigins,
	}
	if row.ExpiresAt != nil {
		out.ExpiresAt = *row.ExpiresAt
	}
	return out, nil
}

func (t adminTx) SetSourceTokenLimit(ctx context.Context, id string, perMin int) error {
	return tokenFailure(t.tx.SetSourceTokenLimit(ctx, id, perMin))
}

func (t adminTx) RevokeSourceToken(ctx context.Context, id, actor string) error {
	return tokenFailure(t.tx.RevokeSourceToken(ctx, id, actor))
}

// tokenFailure maps the store's token errors onto the admin package's.
// Anything else passes through untouched, for the reason InTx gives.
func tokenFailure(err error) error {
	if err == nil {
		return nil
	}
	var me *store.MintError
	switch {
	case errors.As(err, &me):
		return &admin.FieldError{Field: me.Field, Reason: me.Reason}
	case errors.Is(err, store.ErrNotFound):
		return admin.ErrTokenNotFound
	}
	return err
}

// ---------------------------------------------------------------------------
// Translation
// ---------------------------------------------------------------------------

// The two Role vocabularies are the same four bytes on both sides and the casts
// below rely on it. If either side renames a constant the cast still compiles
// and the failure surfaces as a CHECK constraint violation inside a transaction
// in production, so the equality is asserted by a test rather than left to be
// noticed.

func toAdminPrincipal(p store.Principal) admin.Principal {
	return admin.Principal{
		Email:       p.Email,
		Role:        admin.Role(p.Role),
		DisplayName: p.DisplayName,
		// AddedBy and AddedAt cross the boundary because they are the only record
		// of how somebody got access in the first place, and the admin page shows
		// them next to the role that record explains.
		AddedBy:    p.AddedBy,
		AddedAt:    p.AddedAt,
		DisabledAt: p.DisabledAt,
	}
}

func toStorePrincipal(p admin.Principal) store.Principal {
	return store.Principal{
		Email:       p.Email,
		Role:        store.Role(p.Role),
		DisplayName: p.DisplayName,
		AddedBy:     p.AddedBy,
		AddedAt:     p.AddedAt,
		DisabledAt:  p.DisabledAt,
	}
}

func toStorePrincipalChange(c admin.PrincipalChange) store.PrincipalChange {
	return store.PrincipalChange{
		Actor:        c.Actor,
		Target:       c.Target,
		FromRole:     store.Role(c.FromRole),
		ToRole:       store.Role(c.ToRole),
		FromDisabled: c.FromDisabled,
		ToDisabled:   c.ToDisabled,
		// Created is carried rather than derived from an empty FromRole. The store
		// writes it as a NULL from_role, and NULL from_role is the only thing that
		// tells a creation from a promotion once the change is a year old and the
		// person asking is trying to work out how somebody got in.
		Created: c.Created,
		At:      c.At,
	}
}

// toAdminDevice drops the columns the fleet page does not read. Revocation is not
// one of them: a revoked laptop that is still sending health reports is the
// finding the coverage report exists to surface, and a device that arrives here
// without its revocation stamp is indistinguishable from a live one.
func toAdminDevice(d store.Device) admin.Device {
	return admin.Device{
		ID:         d.ID,
		Email:      d.Email,
		Hostname:   d.Hostname,
		EnrolledAt: d.EnrolledAt,
		LastSeenAt: d.LastSeenAt,
		RevokedAt:  d.RevokedAt,
	}
}

// toAdminSnapshot keeps the device id and both clocks.
//
// The device id is what makes coverage a per-machine question; without it two
// laptops belonging to one person become one row and the working machine answers
// for the dead one. EmittedAt is the laptop's clock and ReceivedAt is ours, and
// both are carried because their difference is the only evidence of a machine
// whose clock has drifted: coverage measures silence from ReceivedAt precisely
// because emitted_at is a number the reporting machine chooses.
func toAdminSnapshot(s store.HealthSnapshot) admin.HealthSnapshot {
	return admin.HealthSnapshot{
		Email:      s.Email,
		DeviceID:   s.DeviceID,
		EmittedAt:  s.EmittedAt,
		ReceivedAt: s.ReceivedAt,
		Worst:      s.Worst,
		Report:     s.Report,
	}
}

// toAdminAccessEvent keeps the row id. The handler builds its next_before cursor
// from the last event's id, and a zero there produces a cursor its own validation
// rejects, which ends the access log at page one with no error to explain it.
func toAdminAccessEvent(a store.Access) admin.AccessEvent {
	return admin.AccessEvent{
		ID:        a.ID,
		Viewer:    a.Viewer,
		SessionID: a.SessionID,
		Owner:     a.Owner,
		Via:       a.Via,
		At:        a.At,
	}
}

// toStoreAccessFilter carries every field the admin UI offers. A filter that is
// accepted here and dropped on the way to the SQL is worse than one that does not
// exist: the operator reads the narrowed page as the whole answer, and the read
// they were looking for is the one that is missing.
//
// From and To become Since and Until, which are inclusive and exclusive
// respectively, so that adjacent windows tile without counting the row on the
// boundary twice.
func toStoreAccessFilter(q admin.AccessQuery) store.AccessLogFilter {
	return store.AccessLogFilter{
		SessionID: q.SessionID,
		Viewer:    q.Viewer,
		Owner:     q.Owner,
		Via:       q.Via,
		Since:     q.From,
		Until:     q.To,
		Before:    q.Before,
		Limit:     q.Limit,
	}
}
