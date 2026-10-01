package store

// Source tokens: the credential behind every claimed skill row.
//
// A source token proves a platform, never a person: a Devin organisation,
// a Capy project, a Codex or Claude Code cloud environment, a Vorflux
// harness, or one laptop that has not enrolled. One statement verifies it
// and counts it, because the counter lives on the row and is therefore
// shared by every Cloud Run instance without a Redis this service does not
// otherwise run. The verify returns a dead token's state too, so a leaked
// token used after a revoke is logged and alerted rather than answered
// with a bare 401 nobody sees.
//
// Limit 0 is the soft revoke: the row is still touched and counted, nothing
// is inserted, and the caller is told its post was a duplicate, so a
// platform still on an old token stays visible in the metrics while the
// rotation finishes. The hard revoke is revoked_at.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/server/auth"
)

// SourceTokenPrefix marks a source token, four characters like the device
// prefix so /v1/events refuses one by shape before any lookup and a secret
// scanner can tell the two apart.
const SourceTokenPrefix = "lss_"

// SoftRevokedCeiling is the cap a token at rate_limit_per_min = 0 is still
// held to: a flood on a soft-revoked token is bounded and counted rather
// than free. softRevokedCeiling is the design's spelling of the same value.
const SoftRevokedCeiling = 1200

const softRevokedCeiling = SoftRevokedCeiling

// sourceWindow is the rate window the verify statement counts within.
const sourceWindow = time.Minute

// The two scopes a token can hold (0022): the emitters' and the catalog
// publisher's. A token of one scope is dead on the other route.
const (
	ScopeSkillInvocations = "skill-invocations"
	ScopeSkillCatalog     = "skill-catalog"
)

// Scopes is the scope CHECK in its order, for the closed-enum guard.
var Scopes = []string{ScopeSkillInvocations, ScopeSkillCatalog}

// EnvironmentLaptop is the environment of a per-person token minted
// self-service by an unenrolled laptop; it is the one environment whose
// tokens carry a bound actor, and the one the admin route refuses.
const EnvironmentLaptop = "laptop"

// The states a presented source token can be in, as the verify reports
// them and the "skill invocation rejected" line carries them. Every state
// but live is a 401.
const (
	TokenStateNone       = "none"
	TokenStateRevoked    = "revoked"
	TokenStateExpired    = "expired"
	TokenStateWrongScope = "wrong_scope"
	TokenStateLive       = "live"
)

// The audit row actions the token routes write (DEV-i), named after the
// routes so the runbook can read them back.
const (
	ActionSourceTokenMint   = "source_token.mint"
	ActionSourceTokenLimit  = "source_token.limit"
	ActionSourceTokenRevoke = "source_token.revoke"
)

// Mint bounds. The expiry is mandatory and short because a token in
// hostile hands never goes quiet on its own (design 4c); the limit ceiling
// is the column CHECK.
const (
	MaxTokenLifetimeDays = 180
	MaxRateLimitPerMin   = 100000
)

var (
	// environmentShape is the environment CHECK of 0022.
	environmentShape = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,39}$`)
	// tokenOrigins are the origins a token may be allowed to post: the
	// allowed_origins CHECK, which excludes derived on purpose since that
	// value is the server's own.
	tokenOrigins = []string{OriginHook, OriginBeacon, OriginReconciler}
)

// SourceToken is a source_tokens row as the verify returns it, plus the
// state the verify decided. ID is a string holding a uuid, as every id in
// this package is (SkillRow says why).
type SourceToken struct {
	ID                           string
	Platform, Environment, Scope string
	// Label is known at mint only; the verify does not read it.
	Label                string
	IssuedAt             time.Time
	RevokedAt, ExpiresAt *time.Time
	RateLimitPerMin      int
	AllowedOrigins       []string
	BoundActorEmail      *string
	// WindowCount is the row's count after this call's touch, meaningful
	// only when State is live.
	WindowCount int
	// State is live, or why the token is not: none (no such hash),
	// revoked, expired, wrong_scope.
	State string
}

// Cap is the per-minute count the token may reach: its own limit, or the
// soft-revoked ceiling when the limit is 0.
func (t SourceToken) Cap() int {
	if t.RateLimitPerMin <= 0 {
		return softRevokedCeiling
	}
	return t.RateLimitPerMin
}

// SoftRevoked reports whether the token is at limit 0: touched and
// counted, never written through.
func (t SourceToken) SoftRevoked() bool { return t.RateLimitPerMin == 0 }

// OverCap reports whether this call's touch passed the cap.
func (t SourceToken) OverCap() bool { return t.WindowCount > t.Cap() }

// Allows reports whether the token may post rows of origin.
func (t SourceToken) Allows(origin string) bool {
	for _, o := range t.AllowedOrigins {
		if o == origin {
			return true
		}
	}
	return false
}

// sourceTokenVerifySQL is the design 4c statement, verbatim: one read of
// the newest live row for the hash, one touch of it when it is live for
// this scope, and the row's columns back either way so the caller can
// name the state of a dead token. The ORDER BY prefers the one live row a
// rotation leaves beside revoked ones; the unique index on token_hash is
// partial over live rows.
const sourceTokenVerifySQL = `WITH t AS (
  SELECT id, platform, environment, scope, revoked_at, expires_at, rate_limit_per_min, allowed_origins, bound_actor_email
  FROM source_tokens WHERE token_hash = $1 ORDER BY (revoked_at IS NULL) DESC LIMIT 1),
u AS (
  UPDATE source_tokens s SET last_used_at = now(),
    window_started = CASE WHEN s.window_started IS NULL OR s.window_started < now() - $2::interval THEN now() ELSE s.window_started END,
    window_count = CASE WHEN s.window_started IS NULL OR s.window_started < now() - $2::interval THEN 1 ELSE s.window_count + 1 END
  FROM t WHERE s.id = t.id AND t.revoked_at IS NULL AND (t.expires_at IS NULL OR t.expires_at > now()) AND t.scope = $3
  RETURNING s.window_count)
SELECT t.id, t.platform, t.environment, t.scope, t.revoked_at, t.expires_at, t.rate_limit_per_min, t.allowed_origins, t.bound_actor_email, u.window_count
FROM t LEFT JOIN u ON true;`

// AuthenticateSourceToken resolves a presented token's hash to its row and
// state, touching and counting the row when it is live for scope. No row
// is State none, not an error: a junk bearer is the caller's problem and
// costs one indexed read. It is meant to run on a transaction InSourceTx
// opens, where the lock bound is already set, and to be the only
// statement of it: the route commits the touch before it opens the
// transaction the row is written in, so the token row, which every post
// under the token contends for, is held for one statement and not for a
// request.
func (s *Store) AuthenticateSourceToken(ctx context.Context, q Queryer, tokenHash []byte, scope string) (SourceToken, error) {
	var (
		t     SourceToken
		count *int
	)
	err := q.QueryRow(ctx, sourceTokenVerifySQL, tokenHash, intervalString(sourceWindow), scope).Scan(
		&t.ID, &t.Platform, &t.Environment, &t.Scope, &t.RevokedAt, &t.ExpiresAt,
		&t.RateLimitPerMin, &t.AllowedOrigins, &t.BoundActorEmail, &count)
	if err != nil {
		if noRows(err) {
			return SourceToken{State: TokenStateNone}, nil
		}
		return SourceToken{}, fmt.Errorf("store: authenticate source token: %w", err)
	}
	t.State = sourceTokenState(t, count, scope, time.Now())
	if count != nil {
		t.WindowCount = *count
	}
	return t, nil
}

// sourceTokenState maps the verify's row to a state: a touched row is
// live, and an untouched one is revoked, expired or of the wrong scope,
// tested in that order. The expiry is read against the caller's clock
// where the statement read the database's; the one row that can fall
// between the two is an expiry on the boundary, which the last arm reads
// as expired rather than as a scope the token does have.
func sourceTokenState(t SourceToken, count *int, scope string, now time.Time) string {
	switch {
	case count != nil:
		return TokenStateLive
	case t.RevokedAt != nil:
		return TokenStateRevoked
	case t.ExpiresAt != nil && !t.ExpiresAt.After(now):
		return TokenStateExpired
	case t.Scope != scope:
		return TokenStateWrongScope
	default:
		return TokenStateExpired
	}
}

// sourceLockTimeout is the lock bound of the route's transaction (design
// 4c, SECURITY3-6): an insider spraying keys a derive batch holds parks a
// connection for this long, not for the statement timeout that would let
// four such posts hold an instance's whole semaphore.
const sourceLockTimeout = 100 * time.Millisecond

// InSourceTx runs fn inside one transaction under the route's lock bound,
// committing when fn returns nil. The route opens it twice per source
// request, the verify's touch alone in the first so the token row is
// released before the second, and the roster read with the row write in
// the second, so a 55P03 on either surfaces as the same retryable answer.
// SET LOCAL outside a transaction is a no-op with a warning, which is why
// the bound is set here and not by the statement.
func (s *Store) InSourceTx(ctx context.Context, fn func(ctx context.Context, q Queryer) error) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin source transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", sourceLockTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("store: set the source lock bound: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		// Returned as fn produced it: the route reads the sqlstate off it
		// to tell a lock wait (429) from a failure (503).
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit source transaction: %w", err)
	}
	return nil
}

// IsLockWait reports whether err is the lock bound firing (55P03) or a
// deadlock Postgres broke (40P01), the two the route answers 429 with
// Retry-After: 1 rather than 503.
func IsLockWait(err error) bool { return isLockWait(err) }

// SessionOwner returns the email a session row is attributed to, or false
// when the store has never seen the session. The route reads it for a
// device row: a device may post only on sessions its owner ran
// (SECURITY1-3), and a session the store has not seen yet is not refused,
// since the derived copy replaces the API row on its key when it arrives.
func (s *Store) SessionOwner(ctx context.Context, q Queryer, sessionRef string) (string, bool, error) {
	var email string
	err := q.QueryRow(ctx, `SELECT email FROM sessions WHERE session_id = $1`, sessionRef).Scan(&email)
	if err != nil {
		if noRows(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("store: read session owner: %w", err)
	}
	return email, true, nil
}

// ActorKnown reports whether an email is a live principal, which is what a
// claimed row's actor_known means: the token's bound actor matched the
// roster at ingest. Never echoed to a caller.
func (s *Store) ActorKnown(ctx context.Context, q Queryer, email string) (bool, error) {
	p, found, err := principalLookup(ctx, q, email)
	if err != nil {
		return false, err
	}
	return found && p.DisabledAt == nil, nil
}

// ---------------------------------------------------------------- minting

// MintRequest is what a mint needs. ExpiresInDays is mandatory (1 to
// MaxTokenLifetimeDays); AllowedOrigins defaults to beacon when nil;
// BoundActorEmail is required for the laptop environment and refused for
// every other, so a bound token never sits in a shared environment
// attributing everyone's rows to one person (SECURITY3-7).
type MintRequest struct {
	Platform, Environment, Scope, Label string
	ExpiresInDays                       int
	AllowedOrigins                      []string
	BoundActorEmail                     *string
}

// MintError names the field a mint request failed on, so a route can
// answer 400 with it.
type MintError struct {
	Field, Reason string
}

func (e *MintError) Error() string { return "store: mint source token: " + e.Field + " " + e.Reason }

func mintError(field, reason string) error { return &MintError{Field: field, Reason: reason} }

// Normalized returns the request with its defaults applied and its
// strings trimmed, or the MintError the first bad field produces. The
// checks mirror the 0022 CHECKs so a bad request is a 400 and never a
// statement error, and add the two rules the schema cannot hold: the
// mandatory expiry and the laptop binding. domains is the deployment's
// ALLOWED_DOMAINS, which a bound actor must sit in; the Store passes its
// own (SetDeployment), and an empty list qualifies nobody.
func (r MintRequest) Normalized(domains []string) (MintRequest, error) {
	r.Platform = strings.TrimSpace(r.Platform)
	r.Environment = strings.TrimSpace(r.Environment)
	r.Scope = strings.TrimSpace(r.Scope)
	r.Label = strings.TrimSpace(r.Label)
	if !contains(Platforms, r.Platform) {
		return r, mintError("platform", "must be one of "+strings.Join(Platforms, ", "))
	}
	if !environmentShape.MatchString(r.Environment) {
		return r, mintError("environment", "must match "+environmentShape.String())
	}
	if r.Scope == "" {
		r.Scope = ScopeSkillInvocations
	}
	if !contains(Scopes, r.Scope) {
		return r, mintError("scope", "must be one of "+strings.Join(Scopes, ", "))
	}
	if len(r.Label) > 80 {
		return r, mintError("label", "must be at most 80 characters")
	}
	if r.ExpiresInDays < 1 || r.ExpiresInDays > MaxTokenLifetimeDays {
		return r, mintError("expires_in_days", fmt.Sprintf("must be between 1 and %d", MaxTokenLifetimeDays))
	}
	if len(r.AllowedOrigins) == 0 {
		r.AllowedOrigins = []string{OriginBeacon}
	}
	seen := map[string]bool{}
	for _, o := range r.AllowedOrigins {
		if !contains(tokenOrigins, o) {
			return r, mintError("allowed_origins", "must be a subset of "+strings.Join(tokenOrigins, ", "))
		}
		if seen[o] {
			return r, mintError("allowed_origins", "must not repeat an origin")
		}
		seen[o] = true
	}
	laptop := r.Environment == EnvironmentLaptop
	switch {
	case laptop && (r.BoundActorEmail == nil || strings.TrimSpace(*r.BoundActorEmail) == ""):
		return r, mintError("bound_actor_email", "is required for the laptop environment")
	case !laptop && r.BoundActorEmail != nil:
		return r, mintError("bound_actor_email", "is allowed only for the laptop environment")
	case laptop:
		email := auth.Normalize(*r.BoundActorEmail)
		if !inDomains(email, domains) {
			return r, mintError("bound_actor_email", "must be an address in an allowed domain")
		}
		r.BoundActorEmail = &email
	}
	return r, nil
}

// AuditDetail is the request as the admin_actions row records it: every
// field, none of them a secret. The plaintext and the hash are never here.
func (r MintRequest) AuditDetail() map[string]any {
	d := map[string]any{
		"platform":        r.Platform,
		"environment":     r.Environment,
		"scope":           r.Scope,
		"label":           r.Label,
		"expires_in_days": r.ExpiresInDays,
		"allowed_origins": r.AllowedOrigins,
	}
	if r.BoundActorEmail != nil {
		d["bound_actor_email"] = *r.BoundActorEmail
	}
	return d
}

// inDomains reports whether email sits in one of the deployment's allowed
// domains, which is the whole of what a bound actor may be: the roster
// decides whether the person is still there, at ingest, through actor_known.
func inDomains(email string, domains []string) bool {
	at := strings.LastIndex(email, "@")
	if at <= 0 || at == len(email)-1 {
		return false
	}
	return contains(domains, strings.ToLower(email[at+1:]))
}

func contains(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

// mintSourceToken writes the row on q and returns the plaintext once. Only
// the sha256 reaches the table, through auth.HashToken so the verify and
// the mint can never disagree about what is hashed. The expiry is
// computed by the database from the same clock that stamps issued_at, so
// the token summary's issued_days is exact; in whole 24-hour spans rather
// than calendar days, since a day interval added to a timestamptz in a
// zone with daylight saving is an hour long or short across the change.
func mintSourceToken(ctx context.Context, q Queryer, actor string, req MintRequest, domains []string) (string, SourceToken, error) {
	req, err := req.Normalized(domains)
	if err != nil {
		return "", SourceToken{}, err
	}
	actor = auth.Normalize(actor)
	if actor == "" {
		return "", SourceToken{}, errors.New("store: mint source token: an actor is required")
	}
	plaintext := SourceTokenPrefix + newToken()
	row := SourceToken{
		ID:              newUUID(),
		Platform:        req.Platform,
		Environment:     req.Environment,
		Scope:           req.Scope,
		Label:           req.Label,
		RateLimitPerMin: softRevokedCeiling,
		AllowedOrigins:  req.AllowedOrigins,
		BoundActorEmail: req.BoundActorEmail,
		State:           TokenStateLive,
	}
	var label *string
	if req.Label != "" {
		label = &req.Label
	}
	var expires time.Time
	if err := q.QueryRow(ctx, `
		INSERT INTO source_tokens (id, platform, environment, scope, label, token_hash, issued_by, expires_at, allowed_origins, bound_actor_email)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, now() + ($8::int * interval '24 hours'), $9, $10)
		RETURNING issued_at, expires_at, rate_limit_per_min`,
		row.ID, row.Platform, row.Environment, row.Scope, label, auth.HashToken(plaintext), actor,
		req.ExpiresInDays, row.AllowedOrigins, row.BoundActorEmail,
	).Scan(&row.IssuedAt, &expires, &row.RateLimitPerMin); err != nil {
		return "", SourceToken{}, fmt.Errorf("store: mint source token: %w", err)
	}
	row.ExpiresAt = &expires
	return plaintext, row, nil
}

// MintSourceToken mints a token in one transaction with its audit row, on
// behalf of v. The plaintext is returned once and exists nowhere else. The
// admin route does not use this: it mints on its own transaction (AdminTx)
// so the caller's role is re-read inside it; the self-service laptop mint
// does, since a member's own cookie is the whole authority there.
func (s *Store) MintSourceToken(ctx context.Context, v Viewer, req MintRequest) (string, SourceToken, error) {
	if auth.Normalize(v.Email) == "" {
		return "", SourceToken{}, errors.New("store: mint source token: the viewer has no email")
	}
	req, err := req.Normalized(s.deployment.AllowedDomains)
	if err != nil {
		return "", SourceToken{}, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", SourceToken{}, fmt.Errorf("store: begin mint: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	plaintext, row, err := mintSourceToken(ctx, tx, v.Email, req, s.deployment.AllowedDomains)
	if err != nil {
		return "", SourceToken{}, err
	}
	if err := recordAdminAction(ctx, tx, auth.Normalize(v.Email), ActionSourceTokenMint, row.ID, req.AuditDetail()); err != nil {
		return "", SourceToken{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", SourceToken{}, fmt.Errorf("store: commit mint: %w", err)
	}
	return plaintext, row, nil
}

// setSourceTokenLimit writes the per-minute limit. 0 is the soft revoke.
func setSourceTokenLimit(ctx context.Context, q Queryer, id string, perMin int) error {
	if perMin < 0 || perMin > MaxRateLimitPerMin {
		return mintError("rate_limit_per_min", fmt.Sprintf("must be between 0 and %d", MaxRateLimitPerMin))
	}
	if !isUUID(id) {
		return ErrNotFound
	}
	n, err := q.Exec(ctx, `UPDATE source_tokens SET rate_limit_per_min = $2 WHERE id = $1::uuid`, id, perMin)
	if err != nil {
		return fmt.Errorf("store: set source token limit: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// revokeSourceToken stamps revoked_at and revoked_by, once: a second revoke
// of the same token is a no-op that still answers found, since the
// operator's intent is met either way. The partial unique index frees the
// hash slot with revoked_at.
func revokeSourceToken(ctx context.Context, q Queryer, id, actor string) error {
	if !isUUID(id) {
		return ErrNotFound
	}
	n, err := q.Exec(ctx, `
		UPDATE source_tokens SET revoked_at = COALESCE(revoked_at, now()), revoked_by = COALESCE(revoked_by, $2)
		WHERE id = $1::uuid`, id, auth.Normalize(actor))
	if err != nil {
		return fmt.Errorf("store: revoke source token: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// SetSourceTokenLimit changes a token's per-minute limit in one
// transaction with its audit row. Admin only; ErrNotFound for an unknown
// id. The admin route uses the AdminTx form so the role is re-read inside
// the transaction; this form serves callers that already hold authority.
func (s *Store) SetSourceTokenLimit(ctx context.Context, v Viewer, id string, perMin int) error {
	if !v.IsAdmin() {
		return ErrNotAdmin
	}
	return s.inAuditedTx(ctx, "set source token limit", func(ctx context.Context, tx Tx) error {
		if err := setSourceTokenLimit(ctx, tx, id, perMin); err != nil {
			return err
		}
		return recordAdminAction(ctx, tx, auth.Normalize(v.Email), ActionSourceTokenLimit, id, map[string]any{"rate_limit_per_min": perMin})
	})
}

// RevokeSourceToken hard-revokes a token in one transaction with its audit
// row. Admin only; ErrNotFound for an unknown id.
func (s *Store) RevokeSourceToken(ctx context.Context, v Viewer, id string) error {
	if !v.IsAdmin() {
		return ErrNotAdmin
	}
	return s.inAuditedTx(ctx, "revoke source token", func(ctx context.Context, tx Tx) error {
		if err := revokeSourceToken(ctx, tx, id, v.Email); err != nil {
			return err
		}
		return recordAdminAction(ctx, tx, auth.Normalize(v.Email), ActionSourceTokenRevoke, id, nil)
	})
}

// inAuditedTx runs fn in one transaction, so the change and the audit row
// fn writes land together or not at all. fn's error comes back untouched
// for the caller's sentinels.
func (s *Store) inAuditedTx(ctx context.Context, op string, fn func(context.Context, Tx) error) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin %s: %w", op, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit %s: %w", op, err)
	}
	return nil
}

// RevokeAndUnbindLaptopTokens is the token half of a person's erasure
// (SECURITY3-3): every laptop token bound to the address is revoked, then
// every binding to the address is cleared, in the caller's transaction, so
// no anonymised row can be joined back to the person through a token that
// still names them. Two statements in that order, which is how the README
// purge recipe runs them by hand (its step 8): the revoke reads the
// binding the unbind clears. The unbind is wider than the revoke on
// purpose: only a laptop token is minted bound, but a row bound by hand
// under another environment would otherwise keep the join the erasure
// exists to cut. Idempotent; a person with no bound token is a no-op.
// revoked_by stays NULL: the signature carries no operator (the caller is
// an erasure, not a person), and the column references principals, so
// there is nothing true to put there. The hand-run recipe, which has an
// operator at the keyboard, adds revoked_by to the same statement.
func (s *Store) RevokeAndUnbindLaptopTokens(ctx context.Context, q Queryer, email string) error {
	email = auth.Normalize(email)
	if email == "" {
		return errors.New("store: revoke laptop tokens: an email is required")
	}
	if _, err := q.Exec(ctx, `
		UPDATE source_tokens SET revoked_at = now()
		WHERE bound_actor_email = $1 AND environment = $2 AND revoked_at IS NULL`, email, EnvironmentLaptop); err != nil {
		return fmt.Errorf("store: revoke laptop tokens: %w", err)
	}
	if _, err := q.Exec(ctx, `UPDATE source_tokens SET bound_actor_email = NULL WHERE bound_actor_email = $1`, email); err != nil {
		return fmt.Errorf("store: unbind laptop tokens: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- the admin transaction

// The token routes' statements bound to the admin transaction, so the
// caller's role re-read, the change and the audit row are one unit (design
// 4c; the rerun route's shape in skills.go).

func (t adminTx) MintSourceToken(ctx context.Context, actor string, req MintRequest) (string, SourceToken, error) {
	return mintSourceToken(ctx, t.tx, actor, req, t.domains)
}

func (t adminTx) SetSourceTokenLimit(ctx context.Context, id string, perMin int) error {
	return setSourceTokenLimit(ctx, t.tx, id, perMin)
}

func (t adminTx) RevokeSourceToken(ctx context.Context, id, actor string) error {
	return revokeSourceToken(ctx, t.tx, id, actor)
}

// ---------------------------------------------------------------- the token summary

// tokenNeverUsed is what last_used_minutes_ago reads for a token nobody
// has presented, the silence sentinel's value so a token minted and never
// wired shows as the most silent rather than the freshest.
const tokenNeverUsed = skillSilenceSentinel

// tokenSummaryRow is one live token as the summary reads it.
type tokenSummaryRow struct {
	ID, Platform, Environment, Scope string
	IssuedAt                         time.Time
	ExpiresAt, LastUsedAt            *time.Time
	WindowCount, RateLimitPerMin     int
	AllowedOrigins                   []string
}

// tokenSeries is one expected (platform, environment, origin) series the
// live emitter tokens open (design 7.1, CB-16), with the newest row of
// the table under that platform and environment for that origin.
type tokenSeries struct {
	Platform, Environment, Origin string
	Rows24h                       int64
	LastRow                       *time.Time
}

// LogSkillTokenSummary writes the "skill token summary" line per live
// token and the "skill platform summary" line per series those tokens
// make expected, once per fleet tick from the instance holding the lock
// (called from the AfterTick closure after LogSkillPlatformSummary, which
// writes the derived series). The 17d gauge reads expires_in_days off the
// first; the 18b gauge reads minutes_since_last_row off the second.
//
// Per token, never per series, for the first line, since the alert is
// about a credential; per (platform, environment, origin) from the newest
// row under that platform and environment, never per token, for the
// second, so two tokens sharing labels during a rotation cannot make the
// most silent holder the series' value (OPS3-3). A live token opens a
// series for each of hook and beacon it may post; a reconciler series is
// counted from the runs table, which lands with the reconciler route, so
// none is opened here. A hand-inserted NULL expires_at reads
// expires_in_days = 0 with issued_days at the sentinel, which is what
// makes 17d fire on it.
func (s *Store) LogSkillTokenSummary(ctx context.Context, now time.Time) error {
	tokens, err := s.liveTokens(ctx)
	if err != nil {
		return err
	}
	for _, t := range tokens {
		expiresIn, issued := 0, skillSilenceSentinel
		if t.ExpiresAt != nil {
			if d := t.ExpiresAt.Sub(now); d > 0 {
				expiresIn = int(d.Hours() / 24)
			}
			issued = int(t.ExpiresAt.Sub(t.IssuedAt).Hours() / 24)
		}
		// A post that landed between the tick's clock capture and this
		// read, or a database clock a little ahead of the instance's,
		// puts last_used_at after now; that is a token used just now,
		// not one never used, so it reads 0 rather than the sentinel.
		lastUsed := float64(tokenNeverUsed)
		if t.LastUsedAt != nil {
			lastUsed = 0
			if now.After(*t.LastUsedAt) {
				lastUsed = now.Sub(*t.LastUsedAt).Minutes()
			}
		}
		s.logger().InfoContext(ctx, "skill token summary",
			slog.String("platform", t.Platform),
			slog.String("environment", t.Environment),
			slog.String("source_token_id", t.ID),
			slog.String("scope", t.Scope),
			slog.Int("expires_in_days", expiresIn),
			slog.Int("issued_days", issued),
			slog.Float64("last_used_minutes_ago", lastUsed),
			slog.Int("window_count", t.WindowCount),
			slog.Int("rate_limit_per_min", t.RateLimitPerMin),
			slog.Bool("soft_revoked", t.RateLimitPerMin == 0))
	}
	series, err := s.tokenSeries(ctx, tokens, now)
	if err != nil {
		return err
	}
	for _, sr := range series {
		s.logger().InfoContext(ctx, "skill platform summary",
			slog.String("platform", sr.Platform),
			slog.String("environment", sr.Environment),
			slog.String("origin", sr.Origin),
			// Not split by trigger: 18b watches the channel, not the
			// path, and a hook's typed and model paths are one channel.
			slog.String("trigger", ""),
			slog.Int64("rows_24h", sr.Rows24h),
			slog.Float64("minutes_since_last_row", skillSilenceMinutes(sr.LastRow, now, time.Time{}, false)),
			slog.Int64("rederive_queue", 0),
			slog.Float64("rederive_oldest_minutes", 0))
	}
	return nil
}

// liveTokens reads every unrevoked token, oldest first within a platform.
func (s *Store) liveTokens(ctx context.Context) ([]tokenSummaryRow, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, platform, environment, scope, issued_at, expires_at, last_used_at,
		       window_count, rate_limit_per_min, allowed_origins
		FROM source_tokens WHERE revoked_at IS NULL
		ORDER BY platform, environment, issued_at`)
	if err != nil {
		return nil, fmt.Errorf("store: read live source tokens: %w", err)
	}
	defer rows.Close()
	var out []tokenSummaryRow
	for rows.Next() {
		var t tokenSummaryRow
		if err := rows.Scan(&t.ID, &t.Platform, &t.Environment, &t.Scope, &t.IssuedAt, &t.ExpiresAt, &t.LastUsedAt,
			&t.WindowCount, &t.RateLimitPerMin, &t.AllowedOrigins); err != nil {
			return nil, fmt.Errorf("store: scan live source token: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read live source tokens: %w", err)
	}
	return out, nil
}

// tokenSeriesLookback bounds the series read: rows older than this are not
// scanned, and a series with none younger reads the silence sentinel. It
// sits well past the longest 18b threshold (a week, with the weekend mask
// on top), so the alert reads the same either side of it, and it keeps a
// per-tick aggregate from walking the whole table as the rows accrue.
const tokenSeriesLookback = 30 * 24 * time.Hour

// tokenSeries lists the series the live emitter tokens make expected and
// reads each one's newest row inside the lookback: one per (platform,
// environment, origin) for every hook or beacon origin a live
// skill-invocations token may post, whatever its environment (the canary
// environment of design 9.3 included, which is what lets a
// mint-and-post-nothing token prove the gauge). The join is on the
// token's platform and environment, so the rows of every token of the
// pair count, rotations included.
func (s *Store) tokenSeries(ctx context.Context, tokens []tokenSummaryRow, now time.Time) ([]tokenSeries, error) {
	byKey := map[string]*tokenSeries{}
	for _, t := range tokens {
		if t.Scope != ScopeSkillInvocations {
			continue
		}
		for _, o := range t.AllowedOrigins {
			if o != OriginHook && o != OriginBeacon {
				continue
			}
			k := t.Platform + "/" + t.Environment + "/" + o
			if _, ok := byKey[k]; !ok {
				byKey[k] = &tokenSeries{Platform: t.Platform, Environment: t.Environment, Origin: o}
			}
		}
	}
	if len(byKey) == 0 {
		return nil, nil
	}
	rows, err := s.db.Query(ctx, `
		SELECT st.platform, st.environment, s.origin, max(s.occurred_at),
		       count(*) FILTER (WHERE s.occurred_at >= $1)
		FROM skill_invocations s JOIN source_tokens st ON st.id = s.source_token_id
		WHERE s.origin IN ('hook', 'beacon') AND s.occurred_at >= $2
		GROUP BY st.platform, st.environment, s.origin`, now.Add(-24*time.Hour), now.Add(-tokenSeriesLookback))
	if err != nil {
		return nil, fmt.Errorf("store: read skill token series: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			platform, environment, origin string
			last                          time.Time
			n                             int64
		)
		if err := rows.Scan(&platform, &environment, &origin, &last, &n); err != nil {
			return nil, fmt.Errorf("store: scan skill token series: %w", err)
		}
		if sr, ok := byKey[platform+"/"+environment+"/"+origin]; ok {
			l := last
			sr.LastRow, sr.Rows24h = &l, n
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read skill token series: %w", err)
	}
	var out []tokenSeries
	for _, k := range sortedKeys(byKey) {
		out = append(out, *byKey[k])
	}
	return out, nil
}
