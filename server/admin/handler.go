// Package admin serves the operator surface of the session platform: who is
// allowed in and at what level, whether the agents on their laptops are
// actually reporting, and who has been reading whose transcripts.
//
// Three properties shape everything here, and each defends against a specific
// way this kind of page goes wrong.
//
// Authority is re-read from the store on every request. The credential layer
// proves who is calling; it does not decide what they may do. A role handed out
// at sign-in and cached in a session cookie outlives the revocation that was
// supposed to end it, so the only moment that counts is the moment of the
// request. Mutations go further and re-read the caller's role inside the same
// transaction that performs the change, because a demotion that lands between
// the check and the write would otherwise be executed by an ex-admin.
//
// A caller without admin rights gets 404, never 403. The principals list is a
// roster of everyone enrolled in a system that records their work, and the
// access log names the people who have read colleagues' transcripts. A 403
// confirms that both exist and, on a per-email route, that a particular
// colleague is enrolled — the same leak the session rules guard against
// forbid, arriving through a different door.
//
// The system refuses to end up with zero admins. Every change that would remove
// the last active admin is rejected, including the plausible one where an admin
// demotes themselves to see what the member view looks like. Recovering an empty
// admin table takes a hand on the production database, which is a far larger
// event than the mistake that caused it.
package admin

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/server/auth"
)

// Role is the authorization level. Two values, per DEC-7: the manager hierarchy
// it replaced needed a directory sync, a closure table and a recursive join, and
// bought nothing this pair does not cover.
type Role string

const (
	// RoleAdmin may read every session and edit the roster.
	RoleAdmin Role = "admin"
	// RoleMember may read only their own sessions plus whatever is shared with
	// them explicitly.
	RoleMember Role = "member"
)

// Valid reports whether r is a role the store is allowed to hold. The column has
// a CHECK constraint for the same reason, but rejecting the value here turns a
// constraint violation deep in a transaction into a 400 with a useful message.
func (r Role) Valid() bool { return r == RoleAdmin || r == RoleMember }

// Principal is one person's entry in the roster.
type Principal struct {
	Email       string `json:"email"`
	Role        Role   `json:"role"`
	DisplayName string `json:"display_name,omitempty"`
	AddedBy     string `json:"added_by,omitempty"`

	AddedAt time.Time `json:"added_at"`
	// DisabledAt is a pointer because "not disabled" and "disabled at the zero
	// time" have to be distinguishable on the wire as well as in the column. A
	// plain time.Time would render as 0001-01-01 for every active person, and an
	// admin page that shows a disable date for everyone is a page nobody trusts.
	DisabledAt *time.Time `json:"disabled_at,omitempty"`
}

// Disabled reports whether the principal has been switched off. Disabled people
// keep their row so their sessions stay attributable and the audit trail keeps
// its foreign keys; they simply cannot authenticate.
func (p Principal) Disabled() bool { return p.DisabledAt != nil }

// ActiveAdmin is the only definition of admin authority in this package. A
// disabled admin is not an admin: they cannot sign in, so counting them as one
// of the survivors when someone demotes the last active admin would permit
// exactly the lockout the guard exists to prevent.
func (p Principal) ActiveAdmin() bool { return p.Role == RoleAdmin && !p.Disabled() }

// PrincipalChange is the audit record for a role or status edit. Granting
// yourself visibility over colleagues' transcripts has to leave a trace, and the
// trace has to say who did it and what it changed from, because "Alice is an
// admin" answers none of the questions an investigation asks.
//
// The frozen schema has no table for this and access_log
// cannot carry it: access_log rows are session-scoped and a role change has no
// session. Storage implementations need something of this shape:
//
//	CREATE TABLE principal_changes (
//	  id            BIGSERIAL PRIMARY KEY,
//	  actor         TEXT NOT NULL,
//	  target        TEXT NOT NULL,
//	  from_role     TEXT,                  -- NULL when the principal was created
//	  to_role       TEXT NOT NULL,
//	  from_disabled BOOLEAN NOT NULL,
//	  to_disabled   BOOLEAN NOT NULL,
//	  at            TIMESTAMPTZ NOT NULL DEFAULT now()
//	);
//	CREATE INDEX ON principal_changes (target, at DESC);
//	CREATE INDEX ON principal_changes (actor, at DESC);
type PrincipalChange struct {
	Actor  string `json:"actor"`
	Target string `json:"target"`

	// FromRole is empty when the principal did not exist before the change, which
	// is how a creation is told apart from a promotion after the fact.
	FromRole     Role `json:"from_role,omitempty"`
	ToRole       Role `json:"to_role"`
	FromDisabled bool `json:"from_disabled"`
	ToDisabled   bool `json:"to_disabled"`

	Created bool      `json:"created,omitempty"`
	At      time.Time `json:"at"`
}

// AccessQuery filters the access log. Every field is optional; the zero query
// returns the most recent page across the whole fleet.
type AccessQuery struct {
	Viewer    string
	SessionID string
	Owner     string
	Via       string

	// From and To bound `at`, which is server time. The access log is the one
	// place where ingest time is the right clock: it records when a read
	// happened here, not when anything happened on a laptop.
	From time.Time
	To   time.Time

	// Before pages backwards by primary key rather than by timestamp. Two reads
	// in the same millisecond are ordinary, and a timestamp cursor would either
	// skip one of them or return it twice.
	Before int64
	Limit  int
}

// AccessEvent is one recorded read of somebody's session.
type AccessEvent struct {
	ID        int64     `json:"id"`
	Viewer    string    `json:"viewer"`
	SessionID string    `json:"session_id"`
	Owner     string    `json:"owner"`
	Via       string    `json:"via"`
	At        time.Time `json:"at"`
}

// Store is the persistence this package needs, expressed as a port so the policy
// above can be tested without a database and so the SQL lives with whoever owns
// the schema.
type Store interface {
	// Principal returns the roster entry for an email. The boolean distinguishes
	// "no such person" from a zero-valued row, which matters because a zero row
	// would read as a member and silently answer authorization questions.
	Principal(ctx context.Context, email string) (Principal, bool, error)
	ListPrincipals(ctx context.Context) ([]Principal, error)

	// LatestHealth returns the most recent health report per (email, device).
	// Coverage needs one row per device rather than one per person: somebody with
	// a working laptop and a dead one is not healthy, and a per-person max would
	// report them as such.
	LatestHealth(ctx context.Context) ([]HealthSnapshot, error)

	// EnrolledDevices returns every device row, including revoked ones. Coverage
	// decides what to do with a revoked device; hiding them here would hide the
	// case where a revoked device is still sending reports.
	EnrolledDevices(ctx context.Context) ([]Device, error)

	AccessEvents(ctx context.Context, q AccessQuery) ([]AccessEvent, error)

	// InTx runs fn inside a transaction, rolling back if fn returns an error. It
	// exists so the lockout guard, the write and the audit record are one atomic
	// unit: an audit row that can fail independently of the change it describes
	// is not an audit trail.
	//
	// Implementations must serialize concurrent principal changes, by taking the
	// candidate admin rows FOR UPDATE or by running at SERIALIZABLE. Two
	// simultaneous demotions that each observe the other as the surviving admin
	// would both be permitted, and the roster would end up with none.
	InTx(ctx context.Context, fn func(context.Context, Tx) error) error
}

// Tx is the transactional subset of Store.
type Tx interface {
	Principal(ctx context.Context, email string) (Principal, bool, error)

	// CountActiveAdminsExcept counts admins who are neither disabled nor the
	// given email. Excluding the target is what makes the answer mean "who would
	// be left", which is the only question the lockout guard is asking.
	CountActiveAdminsExcept(ctx context.Context, email string) (int, error)

	// SavePrincipal inserts or updates the row. Insert is reachable because PUT
	// is how somebody joins the roster; see handleUpdatePrincipal.
	SavePrincipal(ctx context.Context, p Principal) error

	RecordPrincipalChange(ctx context.Context, c PrincipalChange) error

	// The skill derive rerun (handleSkillDeriveRerun). RecordAdminAction is
	// the audit row every mutation outside the roster writes, in the same
	// transaction as the change it describes, for the same reason
	// RecordPrincipalChange is here. EnqueueSkillRederiveSince queues every
	// session updated at or after since for the dirty tick's re-derive
	// drain and reports how many; ResetSkillDeriveStep clears the
	// skill_invocations step's ledger row and lowers the derived stamp so
	// the next versioned pass runs that step alone.
	RecordAdminAction(ctx context.Context, actor, action, target string, detail any) error
	EnqueueSkillRederiveSince(ctx context.Context, since time.Time) (int64, error)
	ResetSkillDeriveStep(ctx context.Context) error

	// The source token routes (source_tokens.go): a mint, a limit change
	// and a revoke, each in the transaction that re-reads the caller's
	// role and writes the audit row. A mint the store refuses on a field
	// comes back as *FieldError; an unknown id as ErrTokenNotFound.
	MintSourceToken(ctx context.Context, actor string, req SourceTokenMint) (SourceTokenIssued, error)
	SetSourceTokenLimit(ctx context.Context, id string, perMin int) error
	RevokeSourceToken(ctx context.Context, id, actor string) error
}

// Identity is who the credential layer says is calling. It carries no role on
// purpose: a role that travelled with the credential would be a role this
// package could not re-check, and re-checking is the point.
type Identity struct {
	Email string
}

// Authenticator verifies the caller's credential. Implementations return
// ErrNoIdentity for a missing or invalid one and a real error for an
// infrastructure failure, because the first is the caller's problem and the
// second is ours.
type Authenticator interface {
	Authenticate(r *http.Request) (Identity, error)
}

// ErrNoIdentity means the request carried no usable credential.
var ErrNoIdentity = errors.New("admin: no verified identity")

// Options configure a Handler.
type Options struct {
	Store Store
	Auth  Authenticator

	// Fleet tunes coverage. Zero values take the documented defaults.
	Fleet FleetOptions

	// Now is injectable so tests can place a fleet in a fixed moment rather than
	// racing the wall clock.
	Now func() time.Time

	// Logger receives the detail that 500 responses deliberately withhold. An
	// opaque error to the caller and nothing in the logs is an outage nobody can
	// diagnose.
	Logger *slog.Logger

	// MaxPageSize caps the access log page. Defaults to defaultMaxPageSize.
	MaxPageSize int
}

const (
	defaultPageSize = 100
	// Must not exceed the ceiling the store applies to a single query, which is
	// 500. This handler offers a cursor only when a page comes back full, and
	// "full" is judged by comparing the row count to the limit that was asked
	// for. If the store silently returns fewer rows than were requested because
	// its own ceiling is lower, that comparison is never true, no cursor is
	// ever emitted, and the access log ends at page one with no error to
	// explain why. The two ceilings have to agree; the store cannot import this
	// package to say so, so the coupling is stated here.
	defaultMaxPageSize = 500
	// Request bodies here are a handful of fields. The cap exists so a
	// mistargeted upload cannot be buffered into memory before being rejected.
	maxBodyBytes = 64 << 10
)

// Handler serves the admin API.
type Handler struct {
	store       Store
	auth        Authenticator
	fleet       FleetOptions
	now         func() time.Time
	log         *slog.Logger
	maxPageSize int
	mux         *http.ServeMux
}

// New builds a Handler. It fails rather than defaulting when the store or the
// authenticator is missing: a nil authenticator would make every route public,
// and that is not a condition to discover in production.
func New(opts Options) (*Handler, error) {
	if opts.Store == nil {
		return nil, errors.New("admin: Store is required")
	}
	if opts.Auth == nil {
		return nil, errors.New("admin: Auth is required")
	}
	h := &Handler{
		store:       opts.Store,
		auth:        opts.Auth,
		fleet:       opts.Fleet.withDefaults(),
		now:         opts.Now,
		log:         opts.Logger,
		maxPageSize: opts.MaxPageSize,
	}
	if h.now == nil {
		h.now = time.Now
	}
	if h.log == nil {
		h.log = slog.Default()
	}
	if h.maxPageSize <= 0 {
		h.maxPageSize = defaultMaxPageSize
	}
	h.mux = http.NewServeMux()
	h.Register(h.mux)
	return h, nil
}

// Register adds the admin routes to a mux owned by the caller, for servers that
// mount everything on one tree. The patterns are absolute, so this and ServeHTTP
// route identically and a route cannot be reachable through one and not the
// other.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/admin/principals", h.handleListPrincipals)
	mux.HandleFunc("PUT /v1/admin/principals/{email}", h.handleUpdatePrincipal)
	mux.HandleFunc("GET /v1/admin/fleet", h.handleFleet)
	mux.HandleFunc("GET /v1/admin/access-log", h.handleAccessLog)
	mux.HandleFunc("POST /v1/admin/derive/skill-invocations/rerun", h.handleSkillDeriveRerun)
	mux.HandleFunc("POST /v1/admin/source-tokens", h.handleMintSourceToken)
	mux.HandleFunc("POST /v1/admin/source-tokens/{id}/limit", h.handleSetSourceTokenLimit)
	mux.HandleFunc("POST /v1/admin/source-tokens/{id}/revoke", h.handleRevokeSourceToken)
}

// ServeHTTP lets the Handler stand alone.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

// errNotAdmin is answered as 404. It is a distinct sentinel from a genuine
// absence so the code paths stay readable, but the two must produce byte-identical
// responses; see writeNotFound.
var errNotAdmin = errors.New("admin: caller is not an active admin")

// identify resolves the caller or writes the failure. A missing credential is
// 401 rather than 404: telling somebody they are not signed in reveals nothing
// about anyone else, and answering 404 there would send a browser with an
// expired cookie into a dead end with no prompt to sign in again.
func (h *Handler) identify(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	id, err := h.auth.Authenticate(r)
	if err != nil {
		if errors.Is(err, ErrNoIdentity) {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "sign in to continue")
			return Identity{}, false
		}
		h.fail(r, "authenticate", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return Identity{}, false
	}
	email := normalizeEmail(id.Email)
	if email == "" {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "sign in to continue")
		return Identity{}, false
	}
	id.Email = email
	return id, true
}

// authorizeAdmin re-reads the caller's roster entry. Every request pays for this
// lookup because the alternative is trusting a decision made at sign-in, which
// may have been reversed since, possibly by the very page this is protecting.
func authorizeAdmin(ctx context.Context, p principalReader, email string) (Principal, error) {
	actor, found, err := p.Principal(ctx, email)
	if err != nil {
		return Principal{}, err
	}
	if !found || !actor.ActiveAdmin() {
		return Principal{}, errNotAdmin
	}
	return actor, nil
}

// principalReader is what authorizeAdmin needs, satisfied by both Store and Tx
// so the check is literally the same code inside and outside a transaction.
type principalReader interface {
	Principal(ctx context.Context, email string) (Principal, bool, error)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListPrincipals(w http.ResponseWriter, r *http.Request) {
	id, ok := h.identify(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	if _, err := authorizeAdmin(ctx, h.store, id.Email); err != nil {
		h.writeAuthzFailure(w, r, err)
		return
	}
	ps, err := h.store.ListPrincipals(ctx)
	if err != nil {
		h.fail(r, "list principals", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	sortPrincipals(ps)

	var admins int
	for _, p := range ps {
		if p.ActiveAdmin() {
			admins++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"principals": ps,
		// active_admins is here so the UI can grey out the control that would be
		// refused, instead of offering an action and then explaining the 409.
		"active_admins": admins,
	})
}

// updateRequest is a patch: an absent field means "leave it alone". Pointers
// rather than values because false and "" are both meaningful settings, and a
// value type cannot say whether the caller sent them or simply omitted them.
type updateRequest struct {
	Role        *Role   `json:"role,omitempty"`
	Disabled    *bool   `json:"disabled,omitempty"`
	DisplayName *string `json:"display_name,omitempty"`
}

var (
	// errLastAdmin is the lockout guard firing.
	errLastAdmin = errors.New("admin: refusing to remove the last active admin")
	// errRoleRequired means a patch arrived for somebody who is not on the roster
	// yet, so there is no existing role to patch.
	errRoleRequired = errors.New("admin: role is required when adding a principal")
)

// handleUpdatePrincipal sets a role, disables or re-enables somebody, and adds
// them if they were not on the roster.
//
// Creation lives on PUT because the frozen contract lists no other way to add a
// person, and a system whose roster can only ever shrink cannot onboard the
// second employee. PUT already means "make this resource have this state", so
// the semantics fit; the deviation is noted rather than hidden.
//
// The whole operation is one transaction. The caller's authority is re-checked
// inside it, the lockout guard reads the surviving admin count inside it, and
// the audit row is written inside it, so there is no interleaving in which the
// roster changes without a record of who changed it.
func (h *Handler) handleUpdatePrincipal(w http.ResponseWriter, r *http.Request) {
	id, ok := h.identify(w, r)
	if !ok {
		return
	}
	// Authorization runs before the body is looked at, so that a member probing
	// this route with a malformed body gets the same 404 a nonexistent route
	// would give them rather than a 400 that confirms something parses here. This
	// check is not the authoritative one: the transaction below repeats it, and
	// that repetition is what survives a demotion landing mid-request.
	if _, err := authorizeAdmin(r.Context(), h.store, id.Email); err != nil {
		h.writeAuthzFailure(w, r, err)
		return
	}

	target := normalizeEmail(r.PathValue("email"))
	if target == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "email is required")
		return
	}

	var req updateRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "malformed body: "+err.Error())
		return
	}
	if req.Role == nil && req.Disabled == nil && req.DisplayName == nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "no changes requested")
		return
	}
	if req.Role != nil && !req.Role.Valid() {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("role must be %q or %q", RoleAdmin, RoleMember))
		return
	}

	now := h.now().UTC()
	var (
		saved  Principal
		change *PrincipalChange
	)
	err := h.store.InTx(r.Context(), func(ctx context.Context, tx Tx) error {
		// Reset per attempt: an implementation that retries a serialization
		// failure must not leave the previous attempt's result behind.
		saved, change = Principal{}, nil

		actor, err := authorizeAdmin(ctx, tx, id.Email)
		if err != nil {
			return err
		}
		current, found, err := tx.Principal(ctx, target)
		if err != nil {
			return err
		}
		next, err := applyUpdate(target, current, found, req, actor.Email, now)
		if err != nil {
			return err
		}

		// The guard runs only when admin authority is actually being removed.
		// Phrasing it as "would this leave zero admins" instead would also refuse
		// unrelated edits made while the roster happens to be empty, which helps
		// nobody and blocks the repair.
		if found && current.ActiveAdmin() && !next.ActiveAdmin() {
			remaining, err := tx.CountActiveAdminsExcept(ctx, target)
			if err != nil {
				return err
			}
			if remaining == 0 {
				return errLastAdmin
			}
		}

		if found && samePrincipal(current, next) {
			// Nothing moved, so there is nothing to audit. Recording a change
			// that did not happen would make the trail harder to read at exactly
			// the moment somebody is trying to read it carefully.
			saved = current
			return nil
		}
		if err := tx.SavePrincipal(ctx, next); err != nil {
			return err
		}
		c := PrincipalChange{
			Actor:        actor.Email,
			Target:       target,
			ToRole:       next.Role,
			ToDisabled:   next.Disabled(),
			FromDisabled: found && current.Disabled(),
			Created:      !found,
			At:           now,
		}
		if found {
			c.FromRole = current.Role
		}
		// The audit write is inside the transaction and its error is returned, so
		// a failure here rolls the role change back. An unaudited grant of
		// visibility over colleagues' transcripts is worse than a failed edit.
		if err := tx.RecordPrincipalChange(ctx, c); err != nil {
			return err
		}
		saved, change = next, &c
		return nil
	})

	switch {
	case err == nil:
	case errors.Is(err, errNotAdmin):
		h.writeAuthzFailure(w, r, err)
		return
	case errors.Is(err, errLastAdmin):
		writeError(w, http.StatusConflict, "last_admin",
			"refusing to remove the last active admin; promote someone else first")
		return
	case errors.Is(err, errRoleRequired):
		writeError(w, http.StatusBadRequest, "invalid_request",
			"role is required when adding a principal")
		return
	default:
		h.fail(r, "update principal", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"principal": saved,
		"change":    change,
	})
}

// applyUpdate folds the patch onto the current row. It is separate from the
// handler so the transition rules can be read, and tested, without a request.
//
// The target email is passed rather than read from current, because on a
// creation there is no current row to read it from.
func applyUpdate(target string, current Principal, found bool, req updateRequest, actor string, now time.Time) (Principal, error) {
	next := current
	if !found {
		if req.Role == nil {
			return Principal{}, errRoleRequired
		}
		next = Principal{Email: target, AddedBy: actor, AddedAt: now}
	}
	if req.Role != nil {
		next.Role = *req.Role
	}
	if req.DisplayName != nil {
		next.DisplayName = strings.TrimSpace(*req.DisplayName)
	}
	if req.Disabled != nil {
		switch {
		case *req.Disabled && !next.Disabled():
			at := now
			next.DisabledAt = &at
		case !*req.Disabled:
			next.DisabledAt = nil
		}
	}
	return next, nil
}

// samePrincipal compares the fields a change is about. The disable timestamp is
// compared as a boolean because re-disabling somebody who is already disabled is
// not a change worth a second audit row.
func samePrincipal(a, b Principal) bool {
	return a.Role == b.Role &&
		a.DisplayName == b.DisplayName &&
		a.Disabled() == b.Disabled()
}

func (h *Handler) handleFleet(w http.ResponseWriter, r *http.Request) {
	id, ok := h.identify(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	if _, err := authorizeAdmin(ctx, h.store, id.Email); err != nil {
		h.writeAuthzFailure(w, r, err)
		return
	}

	opts := h.fleet
	q := r.URL.Query()
	if v := q.Get("stale_after"); v != "" {
		d, err := parsePositiveDuration(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "stale_after: "+err.Error())
			return
		}
		opts.StaleAfter = d
	}
	if v := q.Get("enrollment_grace"); v != "" {
		d, err := parsePositiveDuration(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "enrollment_grace: "+err.Error())
			return
		}
		opts.EnrollmentGrace = d
	}

	ps, err := h.store.ListPrincipals(ctx)
	if err != nil {
		h.fail(r, "list principals", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	devices, err := h.store.EnrolledDevices(ctx)
	if err != nil {
		h.fail(r, "list devices", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	snaps, err := h.store.LatestHealth(ctx)
	if err != nil {
		h.fail(r, "latest health", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}

	writeJSON(w, http.StatusOK, Coverage(h.now().UTC(), FleetInput{
		Principals: ps,
		Devices:    devices,
		Health:     snaps,
	}, opts))
}

func (h *Handler) handleAccessLog(w http.ResponseWriter, r *http.Request) {
	id, ok := h.identify(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	if _, err := authorizeAdmin(ctx, h.store, id.Email); err != nil {
		h.writeAuthzFailure(w, r, err)
		return
	}

	q := r.URL.Query()
	query := AccessQuery{
		Viewer:    normalizeEmail(q.Get("viewer")),
		Owner:     normalizeEmail(q.Get("owner")),
		SessionID: strings.TrimSpace(q.Get("session_id")),
		Via:       strings.TrimSpace(q.Get("via")),
		Limit:     defaultPageSize,
	}
	var err error
	if query.From, err = parseTime(q.Get("from")); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "from: "+err.Error())
		return
	}
	if query.To, err = parseTime(q.Get("to")); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "to: "+err.Error())
		return
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_request", "limit must be a positive integer")
			return
		}
		// Clamped rather than rejected: an over-large limit is a caller being
		// optimistic, and refusing the page helps nobody.
		query.Limit = min(n, h.maxPageSize)
	}
	if v := q.Get("before"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_request", "before must be a positive integer")
			return
		}
		query.Before = n
	}

	events, err := h.store.AccessEvents(ctx, query)
	if err != nil {
		h.fail(r, "access log", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	body := map[string]any{"entries": events}
	// A cursor is offered only when the page came back full. Emitting one for a
	// short page invites a second round trip that is guaranteed to be empty.
	if len(events) == query.Limit && len(events) > 0 {
		body["next_before"] = events[len(events)-1].ID
	}
	writeJSON(w, http.StatusOK, body)
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

// writeAuthzFailure answers an authorization failure. A caller who is not an
// admin and a route that does not exist must be indistinguishable, so this
// deliberately produces the same status, code and message a missing route would,
// and says nothing about roles.
func (h *Handler) writeAuthzFailure(w http.ResponseWriter, r *http.Request, err error) {
	if !errors.Is(err, errNotAdmin) {
		h.fail(r, "authorize", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	writeNotFound(w)
}

func writeNotFound(w http.ResponseWriter) {
	writeError(w, http.StatusNotFound, "not_found", "not found")
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg}})
}

// writeJSON encodes into a buffer before touching the ResponseWriter. Encoding
// straight to the writer commits the status line first, so a marshal failure
// halfway through would append error text to a body already declared 200.
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal","message":"internal error"}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// fail logs the detail that the caller is not given. The request path and method
// go with it because an admin 500 with no route is an unanswerable page.
func (h *Handler) fail(r *http.Request, op string, err error) {
	h.log.Error("admin request failed",
		slog.String("op", op),
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("error", err.Error()))
}

// ---------------------------------------------------------------------------
// Skill derive rerun
// ---------------------------------------------------------------------------

// skillDeriveRerunAction is the audit row's action name for the rerun, the
// route's name as the runbook reads it back.
const skillDeriveRerunAction = "derive.skill_invocations.rerun"

// rerunRequest is the optional body. since narrows the rerun to the
// sessions a rolled-back revision served, which nothing else revisits; no
// body is the whole step again.
type rerunRequest struct {
	Since *string `json:"since"`
}

type rerunResponse struct {
	// Queued is how many sessions the since form put in the re-derive
	// queue; StepReset says the full form ran instead.
	Queued    int64 `json:"queued"`
	StepReset bool  `json:"step_reset"`
}

// handleSkillDeriveRerun is the targeted repair of the skill_invocations
// derivation (design 9.5), the lever the runbook names instead of hand SQL
// on production. Same-origin only: the browser sets Sec-Fetch-Site and a
// page's script cannot, so a cross-site form carrying the dashboard cookie
// is refused before the body is read; the check runs after authorization
// so a member probing the route still sees the 404 every admin route
// answers them with. The audit row, the queue insert or the step reset are
// one transaction, and the caller's role is re-read inside it.
func (h *Handler) handleSkillDeriveRerun(w http.ResponseWriter, r *http.Request) {
	id, ok := h.identify(w, r)
	if !ok {
		return
	}
	if _, err := authorizeAdmin(r.Context(), h.store, id.Email); err != nil {
		h.writeAuthzFailure(w, r, err)
		return
	}
	if !auth.SameOriginPOST(r) {
		writeError(w, http.StatusForbidden, "forbidden", "same-origin POST required")
		return
	}

	var req rerunRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "malformed body: "+err.Error())
		return
	}
	var since *time.Time
	if req.Since != nil {
		t, err := time.Parse(time.RFC3339, *req.Since)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "since must be an RFC 3339 timestamp")
			return
		}
		t = t.UTC()
		since = &t
	}

	var res rerunResponse
	err := h.store.InTx(r.Context(), func(ctx context.Context, tx Tx) error {
		res = rerunResponse{}
		if _, err := authorizeAdmin(ctx, tx, id.Email); err != nil {
			return err
		}
		target := ""
		detail := map[string]any{"since": nil}
		if since != nil {
			target = since.Format(time.RFC3339)
			detail["since"] = target
			n, err := tx.EnqueueSkillRederiveSince(ctx, *since)
			if err != nil {
				return err
			}
			res.Queued = n
		} else {
			if err := tx.ResetSkillDeriveStep(ctx); err != nil {
				return err
			}
			res.StepReset = true
		}
		return tx.RecordAdminAction(ctx, id.Email, skillDeriveRerunAction, target, detail)
	})
	if err != nil {
		if errors.Is(err, errNotAdmin) {
			writeNotFound(w)
			return
		}
		h.fail(r, "skill derive rerun", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	h.log.Info("skill derive rerun requested",
		slog.String("actor", id.Email),
		slog.Bool("step_reset", res.StepReset),
		slog.Int64("queued", res.Queued))
	writeJSON(w, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

// normalizeEmail lower-cases and trims. Workspace addresses are case-insensitive
// in practice, so without this a roster can hold two rows for one person, each
// with its own role: the authorization check would consult one of them and the
// admin page would edit the other.
func normalizeEmail(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

func parseTime(s string) (time.Time, error) {
	if strings.TrimSpace(s) == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, errors.New("must be RFC3339")
	}
	return t, nil
}

func parsePositiveDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, errors.New("must be a duration such as 24h")
	}
	if d <= 0 {
		return 0, errors.New("must be positive")
	}
	return d, nil
}

// sortPrincipals gives the roster a stable order. Email rather than added_at:
// the page is read to find a person, not to see who joined last.
func sortPrincipals(ps []Principal) {
	slices.SortFunc(ps, func(a, b Principal) int { return cmp.Compare(a.Email, b.Email) })
}
