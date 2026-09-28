package store

// The admin package declares its own Store port and this file is what satisfies
// it. The shapes here are dictated by that port rather than by this package's
// own habits, which is why none of these methods take a Viewer: the admin
// package re-derives admin authority from the roster on every request, and for a
// principal edit it does so inside the same transaction as the write. A second
// copy of that predicate here would be a second thing to keep correct, and of
// two copies of a permission rule the one that drifts is always the more
// permissive one. The consequence is that everything in this file is an
// unaudited, fleet-wide read of metadata, and it must only be reached from the
// admin surface.
//
// Nothing here reads a transcript, so none of it writes an access_log row. The
// audit obligation in this package attaches to reading somebody's work, not to
// counting their laptops.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/health"
)

// HealthSnapshot is the newest health report from one machine.
//
// EmittedAt is the laptop's clock and ReceivedAt is ours, and both are carried
// because the difference between them is the only evidence of a machine whose
// clock has drifted. Coverage measures silence from ReceivedAt: emitted_at is a
// value the reporting machine chooses, so a clock that has jumped forward would
// otherwise look permanently fresh and one that has jumped back permanently
// silent.
type HealthSnapshot struct {
	Email    string `json:"email"`
	DeviceID string `json:"device_id,omitempty"`

	EmittedAt  time.Time `json:"emitted_at"`
	ReceivedAt time.Time `json:"received_at"`

	// Worst is the level the agent assigned to itself, denormalised at ingest.
	// It is stored rather than recomputed so the fleet page agrees with what the
	// machine said about itself even for a report from an agent newer than this
	// server.
	Worst  health.Level  `json:"worst,omitempty"`
	Report health.Report `json:"report"`
}

// PrincipalChange is the audit record for a role or status edit.
//
// access_log cannot carry these: its rows are session-scoped and a role change
// has no session. The trail is separate but the obligation is the same one, and
// stronger if anything, because granting somebody visibility over colleagues'
// transcripts is the change that makes every later access legitimate.
//
// FromRole is empty when the principal did not exist before the change, which is
// how a creation is told apart from a promotion after the fact. That is also how
// the column is stored: NULL from_role means the row was created, so Created is
// derived rather than persisted and the two cannot disagree.
type PrincipalChange struct {
	Actor  string `json:"actor"`
	Target string `json:"target"`

	FromRole     Role `json:"from_role,omitempty"`
	ToRole       Role `json:"to_role"`
	FromDisabled bool `json:"from_disabled"`
	ToDisabled   bool `json:"to_disabled"`

	Created bool      `json:"created,omitempty"`
	At      time.Time `json:"at"`
}

// AdminTx is the roster half of the store, scoped to one transaction.
//
// It exists so that the lockout guard, the write it guards and the audit row
// that describes the write are one atomic unit. An audit row that can fail
// independently of the change it describes is not an audit trail, and a guard
// that reads outside the transaction it protects is not a guard.
type AdminTx interface {
	// Principal returns the roster entry for an email. The boolean is what
	// distinguishes "no such person" from a zero-valued row; a zero row reads as
	// a member and would silently answer authorization questions.
	Principal(ctx context.Context, email string) (Principal, bool, error)

	// CountActiveAdminsExcept counts the admins who are neither disabled nor the
	// given email, which is the only question the lockout guard asks: who would
	// be left. It is also the point at which concurrent principal changes
	// serialise; see the implementation.
	CountActiveAdminsExcept(ctx context.Context, email string) (int, error)

	// SavePrincipal inserts or updates the row. Insert is reachable because
	// joining the roster is the same operation as editing it.
	SavePrincipal(ctx context.Context, p Principal) error

	RecordPrincipalChange(ctx context.Context, c PrincipalChange) error

	// The skill derive rerun (design 9.5): its audit row (DEV-i) and the
	// queue insert or the step reset are one atomic unit, like a roster
	// change and its record. Implemented in skills.go.
	RecordAdminAction(ctx context.Context, actor, action, target string, detail any) error
	EnqueueSkillRederiveSince(ctx context.Context, since time.Time) (int64, error)
	ResetSkillDeriveStep(ctx context.Context) error

	// The source token routes (design 4c): a mint, a limit change and a
	// revoke each land with the caller's role re-read and its audit row in
	// one transaction. Implemented in source_tokens.go.
	MintSourceToken(ctx context.Context, actor string, req MintRequest) (plaintext string, row SourceToken, err error)
	SetSourceTokenLimit(ctx context.Context, id string, perMin int) error
	RevokeSourceToken(ctx context.Context, id, actor string) error
}

// PrincipalLookup loads one access-list row, reporting absence as a false rather
// than as an error.
//
// Principal is the older spelling and stays as it is: callers inside this
// package treat a missing row as ErrNotFound and are correct to. The port the
// admin package declares needs the two cases separated, because it distinguishes
// "not enrolled" from "enrolled and switched off" and a sentinel error forces
// that distinction through an errors.Is at every call site.
func (s *Store) PrincipalLookup(ctx context.Context, email string) (Principal, bool, error) {
	return principalLookup(ctx, s.db, email)
}

// principalLookup is shared by the pool-backed and transaction-backed readers so
// that a role read inside a guarded transaction is literally the same statement
// as one outside it.
func principalLookup(ctx context.Context, q Queryer, email string) (Principal, bool, error) {
	p, err := scanPrincipal(q.QueryRow(ctx,
		`SELECT `+principalColumns+` FROM principals WHERE email = $1`, email))
	if err != nil {
		if noRows(err) {
			return Principal{}, false, nil
		}
		return Principal{}, false, fmt.Errorf("store: look up principal: %w", err)
	}
	return p, true, nil
}

// LatestHealth returns the newest report from every machine that has ever sent
// one, as one row per (email, device_id).
//
// Per device rather than per person is the whole point. Somebody with a working
// laptop and a dead one is not covered, and a per-person maximum would report
// them as healthy because the working machine's report answers for both. A null
// device id is a legitimate key and groups on its own: reports that predate
// enrolment still prove the person is alive.
//
// The pick is by received_at, not emitted_at. emitted_at is the reporting
// machine's clock, so ordering by it lets a laptop whose clock jumped forward
// pin a stale sample at the top of its own history forever, and one whose clock
// jumped back look silent while it is still reporting every minute. Ordering by
// arrival makes ReceivedAt the true maximum for the device, which is the number
// coverage subtracts from now.
func (s *Store) LatestHealth(ctx context.Context) ([]HealthSnapshot, error) {
	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT ON (email, device_id)
		       email, device_id::text, emitted_at, received_at, worst, report::text
		FROM health_reports
		ORDER BY email, device_id, received_at DESC, id DESC`)
	if err != nil {
		return nil, fmt.Errorf("store: latest health: %w", err)
	}
	defer rows.Close()

	var out []HealthSnapshot
	for rows.Next() {
		var (
			snap     HealthSnapshot
			deviceID *string
			body     string
		)
		if err := rows.Scan(&snap.Email, &deviceID, &snap.EmittedAt, &snap.ReceivedAt,
			&snap.Worst, &body); err != nil {
			return nil, fmt.Errorf("store: scan health snapshot: %w", err)
		}
		snap.DeviceID = deref(deviceID)
		// A report we cannot decode fails the whole call rather than being
		// dropped. Coverage alerts on absence, so a snapshot quietly missing from
		// this slice is a machine that reads as silent when it is reporting
		// perfectly well, which is the one wrong answer this page must not give.
		if err := json.Unmarshal([]byte(body), &snap.Report); err != nil {
			return nil, fmt.Errorf("store: decode health report for %s/%s: %w",
				snap.Email, snap.DeviceID, err)
		}
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read health snapshots: %w", err)
	}
	return out, nil
}

// EnrolledDevices returns every enrolled machine, revoked ones included.
//
// Filtering revoked devices out here would hide the case the coverage report
// exists to catch: a laptop whose credential was withdrawn and which is still
// sending reports. The caller decides what a revoked device means for coverage;
// it cannot decide anything about a row it never sees.
func (s *Store) EnrolledDevices(ctx context.Context) ([]Device, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, email, hostname, os, arch, agent_version,
		       enrolled_at, last_seen_at, revoked_at
		FROM devices
		ORDER BY email, enrolled_at, id`)
	if err != nil {
		return nil, fmt.Errorf("store: enrolled devices: %w", err)
	}
	defer rows.Close()

	var out []Device
	for rows.Next() {
		var (
			d                            Device
			hostname, os, arch, agentVer *string
		)
		if err := rows.Scan(&d.ID, &d.Email, &hostname, &os, &arch, &agentVer,
			&d.EnrolledAt, &d.LastSeenAt, &d.RevokedAt); err != nil {
			return nil, fmt.Errorf("store: scan device: %w", err)
		}
		d.Hostname = deref(hostname)
		d.OS = deref(os)
		d.Arch = deref(arch)
		d.AgentVersion = deref(agentVer)
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read devices: %w", err)
	}
	return out, nil
}

// AccessEvents returns audit rows newest first.
//
// It is the viewerless twin of ListAccessLog, for the admin package's port, and
// it has to honour every field of the filter for the same reason ListAccessLog
// does: a filter that is accepted and then ignored produces a narrowed page the
// operator reads as the whole answer.
//
// The secondary ordering on id is not decoration. Two reads land in the same
// millisecond routinely, and it is also what the Before cursor pages on: a page
// boundary that falls between two rows sharing a timestamp either repeats one or
// drops one, and this table grows while somebody is reading it.
func (s *Store) AccessEvents(ctx context.Context, f AccessLogFilter) ([]Access, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, viewer, session_id, owner, via, at
		FROM access_log
		WHERE ($1 = '' OR session_id = $1)
		  AND ($2 = '' OR viewer = $2)
		  AND ($3 = '' OR owner = $3)
		  AND ($4::timestamptz IS NULL OR at >= $4)
		  AND ($5::timestamptz IS NULL OR at < $5)
		  AND ($6 = '' OR via = $6)
		  AND ($7 = 0 OR id < $7)
		ORDER BY at DESC, id DESC
		LIMIT $8`,
		f.SessionID, f.Viewer, f.Owner, nullTime(f.Since), nullTime(f.Until),
		f.Via, f.Before, clampLimit(f.Limit))
	if err != nil {
		return nil, fmt.Errorf("store: access events: %w", err)
	}
	defer rows.Close()

	var out []Access
	for rows.Next() {
		var a Access
		if err := rows.Scan(&a.ID, &a.Viewer, &a.SessionID, &a.Owner, &a.Via, &a.At); err != nil {
			return nil, fmt.Errorf("store: scan access event: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read access events: %w", err)
	}
	return out, nil
}

// InTx runs fn inside one transaction, committing when it returns nil and
// rolling back otherwise.
//
// Serialisation of concurrent principal changes happens inside
// CountActiveAdminsExcept, which takes a row lock on every active admin before
// answering. That placement is deliberate: it is exactly the transactions that
// are about to remove admin authority which have to queue behind one another,
// and locking on every roster edit would serialise renames and additions for no
// benefit.
func (s *Store) InTx(ctx context.Context, fn func(context.Context, AdminTx) error) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin admin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(ctx, adminTx{tx: tx, domains: s.deployment.AllowedDomains}); err != nil {
		// Returned exactly as fn produced it. The caller matches its own
		// sentinels on this value to decide between a 404, a 409 and a 500, and
		// re-wrapping an error that already carries the caller's context adds a
		// second place for that matching to have to keep working.
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit admin transaction: %w", err)
	}
	return nil
}

// adminTx binds the roster methods to one open transaction. domains is the
// deployment's allowlist, carried so a mint inside the transaction can check a
// bound actor without reaching back to the Store.
type adminTx struct {
	tx      Tx
	domains []string
}

func (t adminTx) Principal(ctx context.Context, email string) (Principal, bool, error) {
	return principalLookup(ctx, t.tx, email)
}

// CountActiveAdminsExcept answers "who would be left", and is the point at which
// two demotions racing each other are made to happen one after the other.
//
// The lock covers every active admin rather than only the target, because the
// answer is about the others: two admins demoting each other at the same instant
// each read the other as the surviving admin, both pass the guard, and the
// roster ends with nobody able to administer it and no way back without database
// access. Holding the rows means the second transaction blocks, and when it
// resumes Postgres re-evaluates role = 'admin' against the committed row, so the
// admin the first transaction just demoted is no longer counted as a survivor.
//
// The ordering inside the lock is what stops the two transactions taking the
// same rows in opposite orders and deadlocking instead of queueing.
func (t adminTx) CountActiveAdminsExcept(ctx context.Context, email string) (int, error) {
	var n int
	if err := t.tx.QueryRow(ctx, `
		WITH locked AS (
			SELECT email FROM principals
			WHERE role = 'admin' AND disabled_at IS NULL
			ORDER BY email
			FOR UPDATE
		)
		SELECT count(*) FROM locked WHERE email <> $1`, email).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: count active admins: %w", err)
	}
	return n, nil
}

// SavePrincipal writes the roster row as given.
//
// The caller has already folded its patch onto the current row, so this is a
// whole-row write and an empty display name really does mean "clear it". The one
// thing an update does not touch is provenance: added_by and added_at record who
// first put this person on the roster, and a later edit rewriting them would
// erase the only record of how they got access in the first place.
func (t adminTx) SavePrincipal(ctx context.Context, p Principal) error {
	if p.Email == "" {
		return fmt.Errorf("store: principal needs an email")
	}
	if p.Role != RoleAdmin && p.Role != RoleMember {
		return fmt.Errorf("store: unknown role %q", p.Role)
	}
	if _, err := t.tx.Exec(ctx, `
		INSERT INTO principals (email, role, display_name, added_by, added_at, disabled_at)
		VALUES ($1, $2, nullif($3,''), nullif($4,''), COALESCE($5::timestamptz, now()), $6)
		ON CONFLICT (email) DO UPDATE SET
			role         = EXCLUDED.role,
			display_name = EXCLUDED.display_name,
			disabled_at  = EXCLUDED.disabled_at`,
		p.Email, string(p.Role), p.DisplayName, p.AddedBy,
		nullTime(p.AddedAt), p.DisabledAt); err != nil {
		return fmt.Errorf("store: save principal %s: %w", p.Email, err)
	}
	return nil
}

// RecordPrincipalChange appends one row to the role-change trail.
//
// It runs on the caller's transaction, so its failure takes the change it
// describes down with it. That is the intended direction: a role change nobody
// can account for later is worse than a role change that did not happen.
func (t adminTx) RecordPrincipalChange(ctx context.Context, c PrincipalChange) error {
	if c.Target == "" {
		return fmt.Errorf("store: principal change needs a target")
	}
	// Created is not stored. A NULL from_role already means the row did not
	// exist, and a second column saying the same thing is a second column that
	// can end up saying something else.
	from := string(c.FromRole)
	if c.Created {
		from = ""
	}
	if _, err := t.tx.Exec(ctx, `
		INSERT INTO principal_changes
			(actor, target, from_role, to_role, from_disabled, to_disabled, at)
		VALUES ($1, $2, nullif($3,''), $4, $5, $6, COALESCE($7::timestamptz, now()))`,
		c.Actor, c.Target, from, string(c.ToRole),
		c.FromDisabled, c.ToDisabled, nullTime(c.At)); err != nil {
		return fmt.Errorf("store: record principal change for %s: %w", c.Target, err)
	}
	return nil
}

// EnrolSelf creates a member row for somebody the sign-in path has already
// verified, and reports whether it created one.
//
// This is the only write in the system that is not made by an admin, so its
// bounds matter more than its body.
//
// It can only ever ADD, never modify. ON CONFLICT DO NOTHING is doing the
// security work: an address that already has a row keeps it exactly as it is,
// which is what makes disabling somebody meaningful. Without that, a disabled
// colleague signs in once and re-enrols themselves, and the admin page's only
// destructive action silently stops working. The caller must therefore still
// refuse a disabled row after calling this; creating and authorising are
// separate decisions and this makes only the first.
//
// The role is always member. Nothing here can mint an admin, so the worst a new
// arrival from a company domain can do is see their own sessions, which is what
// DEC-7 says a personal direct user is.
//
// The audit row is written in the same transaction, with the person as their
// own actor. A roster that grows without a trail is one nobody can reconstruct
// later, and self-enrolment is exactly the growth an admin did not watch happen.
func (s *Store) EnrolSelf(ctx context.Context, email string) (Principal, bool, error) {
	email = strings.TrimSpace(strings.ToLower(email))
	if email == "" {
		return Principal{}, false, errors.New("store: self-enrolment needs an email")
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Principal{}, false, fmt.Errorf("store: begin self-enrolment: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var created bool
	if err := tx.QueryRow(ctx, `
		WITH ins AS (
			INSERT INTO principals (email, role, added_by)
			VALUES ($1, 'member', 'self')
			ON CONFLICT (email) DO NOTHING
			RETURNING email
		)
		SELECT EXISTS (SELECT 1 FROM ins)`, email).Scan(&created); err != nil {
		return Principal{}, false, fmt.Errorf("store: self-enrol %q: %w", email, err)
	}

	if created {
		// from_role NULL marks a creation rather than a promotion, the same way
		// the admin path records one.
		if _, err := tx.Exec(ctx, `
			INSERT INTO principal_changes (actor, target, from_role, to_role, from_disabled, to_disabled)
			VALUES ($1, $1, NULL, 'member', false, false)`, email); err != nil {
			return Principal{}, false, fmt.Errorf("store: record self-enrolment of %q: %w", email, err)
		}
	}

	p, found, err := principalLookup(ctx, tx, email)
	if err != nil {
		return Principal{}, false, err
	}
	if !found {
		// Only reachable if the row vanished between the insert and the read,
		// which means somebody deleted it mid-transaction. Reporting it is
		// better than returning a zero-valued Principal that reads as a member.
		return Principal{}, false, fmt.Errorf("store: self-enrolled %q but no row followed", email)
	}
	if err := tx.Commit(ctx); err != nil {
		return Principal{}, false, fmt.Errorf("store: commit self-enrolment: %w", err)
	}
	return p, created, nil
}
