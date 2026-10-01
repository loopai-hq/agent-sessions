//go:build integration

package store

// The half of the source tokens only Postgres can answer: that the verify
// statement's window arithmetic holds across connections and past the cap,
// that a dead token comes back with its state rather than nothing, that
// the partial unique index lets two live rows verify during a rotation,
// that a row held by another transaction answers inside the lock bound,
// that the merge on Postgres lets a device copy beat a claimed one in
// either order, and that a limit change rolls back with its audit row.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/server/auth"
)

const (
	intAdmin   = "boss@example.com"
	intDeviceA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	intDeviceB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

func intAdminViewer(t *testing.T, s *Store) Viewer {
	t.Helper()
	mustPrincipal(t, s, intAdmin, RoleAdmin)
	return Viewer{Email: intAdmin, Role: RoleAdmin}
}

func intMint(t *testing.T, s *Store, v Viewer, req MintRequest) (string, SourceToken) {
	t.Helper()
	plaintext, row, err := s.MintSourceToken(context.Background(), v, req)
	if err != nil {
		t.Fatalf("mint %+v: %v", req, err)
	}
	return plaintext, row
}

func intVerify(t *testing.T, s *Store, q Queryer, plaintext, scope string) SourceToken {
	t.Helper()
	tok, err := s.AuthenticateSourceToken(context.Background(), q, auth.HashToken(plaintext), scope)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	return tok
}

// TestIntegrationSourceTokenVerifyAcrossConnectionsAndStates is design
// 10.2 "Source tokens": call 1,201 in a minute across two connections is
// over the cap; limit 0 inserts nothing, answers live and still advances
// last_used_at, and is over the ceiling; revoked, expired and wrong-scope
// tokens answer their state; two live rows per platform both verify; a
// mint without an expiry or at 400 days is refused.
func TestIntegrationSourceTokenVerifyAcrossConnectionsAndStates(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	plaintext, row := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", Label: "devin", ExpiresInDays: 30})
	if row.RateLimitPerMin != 1200 || row.ExpiresAt == nil || row.IssuedAt.IsZero() {
		t.Fatalf("minted row = %+v", row)
	}

	c1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Release()
	c2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Release()
	conns := []Queryer{poolConn{conn: c1}, poolConn{conn: c2}}
	var last SourceToken
	for i := 1; i <= 1201; i++ {
		last = intVerify(t, s, conns[i%2], plaintext, ScopeSkillInvocations)
		if last.State != TokenStateLive || last.WindowCount != i {
			t.Fatalf("call %d: state %s count %d", i, last.State, last.WindowCount)
		}
		if over := last.OverCap(); over != (i > 1200) {
			t.Fatalf("call %d: over cap %v", i, over)
		}
	}
	if last.ID != row.ID || last.Platform != PlatformDevin || last.Environment != "default" || len(last.AllowedOrigins) != 1 || last.AllowedOrigins[0] != OriginBeacon || last.BoundActorEmail != nil {
		t.Errorf("the verify's row = %+v", last)
	}

	// Limit 0: still verified and counted, last_used_at moving, and over
	// the ceiling since the window already passed it.
	if err := s.SetSourceTokenLimit(ctx, admin, row.ID, 0); err != nil {
		t.Fatalf("limit 0: %v", err)
	}
	var before time.Time
	if err := pool.QueryRow(ctx, `SELECT last_used_at FROM source_tokens WHERE id = $1::uuid`, row.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	soft := intVerify(t, s, conns[0], plaintext, ScopeSkillInvocations)
	if soft.State != TokenStateLive || !soft.SoftRevoked() || !soft.OverCap() || soft.WindowCount != 1202 {
		t.Errorf("soft-revoked verify = %+v", soft)
	}
	var after time.Time
	if err := pool.QueryRow(ctx, `SELECT last_used_at FROM source_tokens WHERE id = $1::uuid`, row.ID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.After(before) {
		t.Errorf("last_used_at did not advance on a soft-revoked token: %v then %v", before, after)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM skill_invocations`).Scan(&rows); err != nil || rows != 0 {
		t.Errorf("rows = %d err %v; the verify writes no invocation", rows, err)
	}
	// The window resets once it is a minute old.
	if _, err := pool.Exec(ctx, `UPDATE source_tokens SET window_started = now() - interval '61 seconds' WHERE id = $1::uuid`, row.ID); err != nil {
		t.Fatal(err)
	}
	if fresh := intVerify(t, s, conns[1], plaintext, ScopeSkillInvocations); fresh.WindowCount != 1 || fresh.OverCap() {
		t.Errorf("a new window = %+v", fresh)
	}

	// Two live rows per (platform, environment) both verify: a rotation.
	plaintext2, row2 := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 60})
	if v := intVerify(t, s, conns[0], plaintext2, ScopeSkillInvocations); v.State != TokenStateLive || v.ID != row2.ID {
		t.Errorf("the successor = %+v", v)
	}
	if v := intVerify(t, s, conns[0], plaintext, ScopeSkillInvocations); v.State != TokenStateLive || v.ID != row.ID {
		t.Errorf("the predecessor = %+v", v)
	}

	// Revoked: the row, its id and platform come back with the state, and
	// nothing is touched.
	if err := s.RevokeSourceToken(ctx, admin, row.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := s.RevokeSourceToken(ctx, admin, row.ID); err != nil {
		t.Errorf("a second revoke: %v", err)
	}
	if err := s.RevokeSourceToken(ctx, admin, "99999999-9999-4999-8999-999999999999"); !errors.Is(err, ErrNotFound) {
		t.Errorf("revoking an unknown id: %v", err)
	}
	revoked := intVerify(t, s, conns[0], plaintext, ScopeSkillInvocations)
	if revoked.State != TokenStateRevoked || revoked.ID != row.ID || revoked.Platform != PlatformDevin || revoked.WindowCount != 0 {
		t.Errorf("revoked verify = %+v", revoked)
	}
	// The window held two touches before the revoke (the reset check and
	// the rotation check) and holds two after it.
	var count int
	if err := pool.QueryRow(ctx, `SELECT window_count FROM source_tokens WHERE id = $1::uuid`, row.ID).Scan(&count); err != nil || count != 2 {
		t.Errorf("a revoked token was touched: count %d err %v", count, err)
	}

	// Expired.
	plaintext3, row3 := intMint(t, s, admin, MintRequest{Platform: PlatformCapy, Environment: "backend", ExpiresInDays: 1})
	if _, err := pool.Exec(ctx, `UPDATE source_tokens SET expires_at = now() - interval '1 hour' WHERE id = $1::uuid`, row3.ID); err != nil {
		t.Fatal(err)
	}
	if v := intVerify(t, s, conns[0], plaintext3, ScopeSkillInvocations); v.State != TokenStateExpired || v.ID != row3.ID {
		t.Errorf("expired verify = %+v", v)
	}

	// Wrong scope: the catalog publisher's token on the invocation route
	// (T10).
	plaintext4, row4 := intMint(t, s, admin, MintRequest{Platform: PlatformClaudeCode, Environment: "catalog-backend", Scope: ScopeSkillCatalog, ExpiresInDays: 30})
	if v := intVerify(t, s, conns[0], plaintext4, ScopeSkillInvocations); v.State != TokenStateWrongScope || v.ID != row4.ID {
		t.Errorf("catalog token on the invocation scope = %+v", v)
	}
	if v := intVerify(t, s, conns[0], plaintext4, ScopeSkillCatalog); v.State != TokenStateLive {
		t.Errorf("catalog token on its own scope = %+v", v)
	}

	// Unknown: no row, no error.
	if v := intVerify(t, s, conns[0], "lss_never-minted", ScopeSkillInvocations); v.State != TokenStateNone {
		t.Errorf("unknown = %+v", v)
	}

	// Mint refusals the schema and the normaliser hold.
	for _, req := range []MintRequest{
		{Platform: PlatformDevin, Environment: "default"},
		{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 400},
	} {
		var me *MintError
		if _, _, err := s.MintSourceToken(ctx, admin, req); !errors.As(err, &me) || me.Field != "expires_in_days" {
			t.Errorf("mint %+v: %v, want a MintError on expires_in_days", req, err)
		}
	}
	var audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM admin_actions WHERE actor = $1 AND action IN ($2, $3, $4)`, intAdmin, ActionSourceTokenMint, ActionSourceTokenLimit, ActionSourceTokenRevoke).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	// Four mints, one limit, two revokes (the no-op second one is audited
	// too: the operator asked), no row for the unknown id.
	if audits != 7 {
		t.Errorf("%d audit rows, want 7", audits)
	}
}

// TestIntegrationAPostOnAHeldKeyAnswersInsideTheLockBound is SECURITY3-6:
// with a row on the key open in another transaction, the bounded
// transaction's merge fails 55P03 inside the bound rather than waiting
// for the statement timeout.
func TestIntegrationAPostOnAHeldKeyAnswersInsideTheLockBound(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	_, tok := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 30})
	row := SkillRow{Origin: OriginBeacon, AgentPlatform: PlatformDevin, Trust: TrustClaimed, SourceTokenID: strPtr(tok.ID),
		RawName: "git", Skill: strPtr("git"), SkillSource: SkillSourceUnknown, Trigger: TriggerUnknown, Outcome: OutcomeStarted,
		IdempotencyKey: strPtr("held-1"), OccurredAt: time.Now()}

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := s.UpsertSkillInvocation(ctx, pgxTx{tx: holder}, row); err != nil {
		t.Fatalf("the holder's insert: %v", err)
	}

	started := time.Now()
	err = s.InSourceTx(ctx, func(ctx context.Context, q Queryer) error {
		_, err := s.UpsertSkillInvocation(ctx, q, row)
		return err
	})
	elapsed := time.Since(started)
	if err == nil || !IsLockWait(err) {
		t.Fatalf("a post on a held key: %v, want a lock wait", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("the post took %v, want inside 200 ms", elapsed)
	}
}

// TestIntegrationAVerifyNeverWaitsOnAnotherPostsRowTransaction is the
// shape the route runs a source post in, on Postgres: the verify's touch
// in a transaction of its own, committed, then the row in a second. With
// the first post's row transaction still open, a second verify under the
// same token answers live inside the bound, because nothing it needs is
// held. The control is the shape the route no longer runs: a verify held
// open in a transaction makes the next verify under that token wait on
// the row and fail the bound, which is what a burst under a shared token
// would have hit on every post.
func TestIntegrationAVerifyNeverWaitsOnAnotherPostsRowTransaction(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	plaintext, tok := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 30})
	hash := auth.HashToken(plaintext)
	verify := func() (SourceToken, error) {
		var out SourceToken
		err := s.InSourceTx(ctx, func(ctx context.Context, q Queryer) error {
			var err error
			out, err = s.AuthenticateSourceToken(ctx, q, hash, ScopeSkillInvocations)
			return err
		})
		return out, err
	}
	row := func(key string) SkillRow {
		return SkillRow{Origin: OriginBeacon, AgentPlatform: PlatformDevin, Trust: TrustClaimed, SourceTokenID: strPtr(tok.ID),
			RawName: "git", Skill: strPtr("git"), SkillSource: SkillSourceUnknown, Trigger: TriggerUnknown, Outcome: OutcomeStarted,
			IdempotencyKey: strPtr(key), OccurredAt: time.Now(), TokenPlatform: PlatformDevin, TokenEnvironment: "default"}
	}

	// The first post: touch committed, then its row transaction held open
	// after the write.
	if first, err := verify(); err != nil || first.State != TokenStateLive || first.WindowCount != 1 {
		t.Fatalf("the first post's verify: %+v, %v", first, err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- s.InSourceTx(ctx, func(ctx context.Context, q Queryer) error {
			if _, err := s.UpsertSkillInvocation(ctx, q, row("burst-1")); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	// The second post under the same token, while the first is held.
	started := time.Now()
	second, err := verify()
	elapsed := time.Since(started)
	if err != nil || second.State != TokenStateLive || second.WindowCount != 2 {
		t.Fatalf("the second verify with the first post's transaction open: %+v, %v (after %v); want live, count 2", second, err, elapsed)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("the second verify took %v, want inside 200 ms", elapsed)
	}
	if err := s.InSourceTx(ctx, func(ctx context.Context, q Queryer) error {
		_, err := s.UpsertSkillInvocation(ctx, q, row("burst-2"))
		return err
	}); err != nil {
		t.Fatalf("the second post's row with the first held: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("the first post's transaction: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM skill_invocations`).Scan(&n); err != nil || n != 2 {
		t.Errorf("rows = %d err %v, want both posts' rows", n, err)
	}

	// The control: a verify left open in a transaction holds the row, and
	// the next verify under the token fails the bound rather than waiting.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := s.AuthenticateSourceToken(ctx, pgxTx{tx: holder}, hash, ScopeSkillInvocations); err != nil {
		t.Fatalf("the holder's verify: %v", err)
	}
	started = time.Now()
	_, err = verify()
	elapsed = time.Since(started)
	if err == nil || !IsLockWait(err) {
		t.Fatalf("a verify behind an open verify: %v after %v, want a lock wait", err, elapsed)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("the refused verify took %v, want inside 200 ms", elapsed)
	}
}

// TestIntegrationMergeOnPostgresDeviceBeatsClaimed is design 10.2 "Merge
// on Postgres" and the T4 probes (criterion g): claimed and device in
// either order end trust = device with the device copy's payload and the
// displaced credential recorded; device A first on B's unseen session
// still ends with B's derived row; a claimed error plus evil:git onto a
// derived row is a duplicate; started and success in either order end
// success.
func TestIntegrationMergeOnPostgresDeviceBeatsClaimed(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	_, tok := intMint(t, s, admin, MintRequest{Platform: PlatformClaudeCode, Environment: "cloud", ExpiresInDays: 30, AllowedOrigins: []string{OriginHook}})

	claimed := func(key, skill, outcome string) SkillRow {
		rawName, plugin, slug, _ := NormalizeSkillName(skill)
		return SkillRow{Origin: OriginHook, AgentPlatform: PlatformClaudeCode, Trust: TrustClaimed, SourceTokenID: strPtr(tok.ID),
			RawName: rawName, Plugin: plugin, Skill: slug, SkillSource: SkillSourceUnknown, Trigger: TriggerAgent, Outcome: outcome,
			SessionRef: "sess-1", ToolUseID: strPtr(key), OccurredAt: time.Now(), TokenPlatform: PlatformClaudeCode, TokenEnvironment: "cloud"}
	}
	derived := func(key, skill, device, email string) SkillRow {
		rawName, plugin, slug, _ := NormalizeSkillName(skill)
		return SkillRow{Origin: OriginDerived, AgentPlatform: PlatformClaudeCode, Trust: TrustDevice, DeviceID: strPtr(device),
			RawName: rawName, Plugin: plugin, Skill: slug, SkillSource: SkillSourcePlugin, Trigger: TriggerAgent, Outcome: OutcomeStarted,
			ActorEmail: strPtr(email), ActorKnown: true, SessionRef: "sess-1", ToolUseID: strPtr(key), EventID: strPtr("evt-" + key), OccurredAt: time.Now()}
	}
	lsdAPI := func(key, skill, device, email string) SkillRow {
		r := derived(key, skill, device, email)
		r.Origin, r.EventID, r.SkillSource = OriginHook, nil, SkillSourceUnknown
		return r
	}
	type stored struct {
		Trust, Origin, Skill, Outcome                      string
		Token, PreemptedBy, Device, PreemptedDevice, Actor *string
	}
	read := func(key string) stored {
		var st stored
		if err := pool.QueryRow(ctx, `
			SELECT trust, origin, skill, outcome, source_token_id::text, preempted_by::text, device_id::text, preempted_device::text, actor_email
			FROM skill_invocations WHERE dedupe_key = $1`, "claude_code:sess-1:t:"+key).Scan(
			&st.Trust, &st.Origin, &st.Skill, &st.Outcome, &st.Token, &st.PreemptedBy, &st.Device, &st.PreemptedDevice, &st.Actor); err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		return st
	}
	upsert := func(r SkillRow) UpsertOutcome {
		t.Helper()
		out, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, r)
		if err != nil {
			t.Fatalf("upsert %s: %v", deref(r.ToolUseID), err)
		}
		return out
	}

	// 1. Claimed zz-fake first, then the derived copy: the derived skill,
	// trust device, the token in preempted_by.
	upsert(claimed("k1", "zz-fake", OutcomeStarted))
	out := upsert(derived("k1", "engg:git", intDeviceA, "dev@example.com"))
	if !out.Changed || !out.Preempted || out.PreemptedBy == nil || *out.PreemptedBy != tok.ID {
		t.Errorf("derived onto claimed: %+v", out)
	}
	if st := read("k1"); st.Trust != TrustDevice || st.Origin != OriginDerived || st.Skill != "git" || st.Token != nil || st.PreemptedBy == nil || *st.PreemptedBy != tok.ID || st.Actor == nil || *st.Actor != "dev@example.com" {
		t.Errorf("k1 = %+v", st)
	}

	// 2. Derived first, then a claimed error with evil:git: a duplicate,
	// the row untouched.
	upsert(derived("k2", "engg:git", intDeviceA, "dev@example.com"))
	if out := upsert(claimed("k2", "evil:git", OutcomeError)); out.Inserted || out.Changed || out.Preempted {
		t.Errorf("claimed onto derived: %+v, want a duplicate", out)
	}
	if st := read("k2"); st.Trust != TrustDevice || st.Skill != "git" || st.Outcome != OutcomeStarted || st.PreemptedBy != nil {
		t.Errorf("k2 = %+v", st)
	}

	// 3. An lsd_ API row on device A, then the derived copy on the same
	// device: origin derived, the device in preempted_device.
	upsert(lsdAPI("k3", "zz-fake", intDeviceA, "dev@example.com"))
	out = upsert(derived("k3", "engg:git", intDeviceA, "dev@example.com"))
	if !out.Preempted || out.PreemptedDevice == nil || *out.PreemptedDevice != intDeviceA || out.PreemptedBy != nil {
		t.Errorf("derived onto an lsd_ row: %+v", out)
	}
	if st := read("k3"); st.Origin != OriginDerived || st.Skill != "git" || st.PreemptedDevice == nil || *st.PreemptedDevice != intDeviceA {
		t.Errorf("k3 = %+v", st)
	}

	// 4. Device A first on B's unseen session, then B's derived row: B's
	// row wins, A recorded as displaced.
	upsert(lsdAPI("k4", "zz-fake", intDeviceA, "a@example.com"))
	upsert(derived("k4", "engg:git", intDeviceB, "b@example.com"))
	if st := read("k4"); st.Device == nil || *st.Device != intDeviceB || st.Actor == nil || *st.Actor != "b@example.com" || st.Skill != "git" || st.PreemptedDevice == nil || *st.PreemptedDevice != intDeviceA {
		t.Errorf("k4 = %+v", st)
	}

	// 5. started then success, and success then started, under one token:
	// success both ways.
	upsert(claimed("k5", "engg:git", OutcomeStarted))
	upsert(claimed("k5", "engg:git", OutcomeSuccess))
	upsert(claimed("k6", "engg:git", OutcomeSuccess))
	if out := upsert(claimed("k6", "engg:git", OutcomeStarted)); out.Inserted || out.Changed {
		t.Errorf("started onto success: %+v, want a duplicate", out)
	}
	if a, b := read("k5"), read("k6"); a.Outcome != OutcomeSuccess || b.Outcome != OutcomeSuccess {
		t.Errorf("k5 = %s, k6 = %s, want success", a.Outcome, b.Outcome)
	}

	// The canary (T5, 10.3): an off-shape skill with customer text in it
	// is stored as off-shape, and no substring of it is in the row.
	const canary = "value-summary acme jane@example.net"
	r := claimed("k7", canary, OutcomeStarted)
	upsert(r)
	var body string
	if err := pool.QueryRow(ctx, `SELECT to_jsonb(s)::text FROM skill_invocations s WHERE dedupe_key = $1`, "claude_code:sess-1:t:k7").Scan(&body); err != nil {
		t.Fatal(err)
	}
	for _, frag := range []string{"acme", "jane@example.net", "value-summary"} {
		if strings.Contains(body, frag) {
			t.Errorf("the row carries %q: %s", frag, body)
		}
	}
	if !strings.Contains(body, `"raw_name": "off-shape"`) {
		t.Errorf("raw_name is not off-shape: %s", body)
	}
}

// TestIntegrationALimitChangeRollsBackWithItsAuditRow is criterion j's
// integration half: the audit insert failing takes the limit change with
// it, since both run in one transaction.
func TestIntegrationALimitChangeRollsBackWithItsAuditRow(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	_, tok := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 30})

	if _, err := pool.Exec(ctx, `ALTER TABLE admin_actions RENAME TO admin_actions_gone`); err != nil {
		t.Fatal(err)
	}
	restore := func() {
		_, _ = pool.Exec(ctx, `ALTER TABLE admin_actions_gone RENAME TO admin_actions`)
	}
	defer restore()
	if err := s.SetSourceTokenLimit(ctx, admin, tok.ID, 0); err == nil {
		t.Fatal("a limit change without its audit row succeeded")
	}
	if err := s.RevokeSourceToken(ctx, admin, tok.ID); err == nil {
		t.Fatal("a revoke without its audit row succeeded")
	}
	restore()
	var limit int
	var revoked *time.Time
	if err := pool.QueryRow(ctx, `SELECT rate_limit_per_min, revoked_at FROM source_tokens WHERE id = $1::uuid`, tok.ID).Scan(&limit, &revoked); err != nil {
		t.Fatal(err)
	}
	if limit != 1200 || revoked != nil {
		t.Errorf("the change survived the failed audit write: limit %d revoked %v", limit, revoked)
	}
}

// TestIntegrationLaptopTokensAreRevokedAndUnbound is SECURITY3-3: after
// the erasure call no row of the person's laptop tokens joins back to
// them through bound_actor_email, and the tokens are dead.
func TestIntegrationLaptopTokensAreRevokedAndUnbound(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	mustPrincipal(t, s, "dev@example.com", RoleMember)
	member := Viewer{Email: "dev@example.com", Role: RoleMember}
	email := "dev@example.com"
	// A bound actor must sit in the deployment's allowlist.
	s.SetDeployment(Deployment{AllowedDomains: []string{testDomainOf(email)}})
	plaintext, laptop := intMint(t, s, member, MintRequest{Platform: PlatformClaudeCode, Environment: EnvironmentLaptop, ExpiresInDays: 180,
		AllowedOrigins: []string{OriginHook}, BoundActorEmail: &email})
	if laptop.BoundActorEmail == nil || *laptop.BoundActorEmail != email {
		t.Fatalf("laptop row = %+v", laptop)
	}
	row := SkillRow{Origin: OriginHook, AgentPlatform: PlatformClaudeCode, Trust: TrustClaimed, SourceTokenID: strPtr(laptop.ID),
		RawName: "engg:git", Plugin: "engg", Skill: strPtr("git"), SkillSource: SkillSourceUnknown, Trigger: TriggerUser, Outcome: OutcomeStarted,
		ActorEmail: &email, ActorKnown: true, SessionRef: "sess-l", PromptID: strPtr("p1"), OccurredAt: time.Now()}
	if _, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, row); err != nil {
		t.Fatal(err)
	}
	// The tokens the erasure must leave alone: a colleague's laptop token,
	// still bound to them, and a cloud token, bound to nobody.
	peerEmail := "peer@example.com"
	mustPrincipal(t, s, peerEmail, RoleMember)
	peerPlain, peer := intMint(t, s, Viewer{Email: peerEmail, Role: RoleMember}, MintRequest{Platform: PlatformClaudeCode, Environment: EnvironmentLaptop, ExpiresInDays: 90,
		AllowedOrigins: []string{OriginHook}, BoundActorEmail: &peerEmail})
	cloudPlain, cloud := intMint(t, s, intAdminViewer(t, s), MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 30})

	if err := s.RevokeAndUnbindLaptopTokens(ctx, poolDB{pool: pool}, "Dev@Example.com"); err != nil {
		t.Fatalf("RevokeAndUnbindLaptopTokens: %v", err)
	}
	var joined int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM skill_invocations s JOIN source_tokens t ON t.id = s.source_token_id
		WHERE t.bound_actor_email IS NOT NULL`).Scan(&joined); err != nil || joined != 0 {
		t.Errorf("%d rows still join a bound token (err %v)", joined, err)
	}
	if v := intVerify(t, s, poolDB{pool: pool}, plaintext, ScopeSkillInvocations); v.State != TokenStateRevoked || v.BoundActorEmail != nil {
		t.Errorf("the laptop token after erasure = %+v", v)
	}
	if v := intVerify(t, s, poolDB{pool: pool}, peerPlain, ScopeSkillInvocations); v.ID != peer.ID || v.State != TokenStateLive || v.BoundActorEmail == nil || *v.BoundActorEmail != peerEmail {
		t.Errorf("the colleague's laptop token after somebody else's erasure = %+v", v)
	}
	if v := intVerify(t, s, poolDB{pool: pool}, cloudPlain, ScopeSkillInvocations); v.ID != cloud.ID || v.State != TokenStateLive || v.RevokedAt != nil {
		t.Errorf("the cloud token after the erasure = %+v", v)
	}
	var bound int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_tokens WHERE bound_actor_email = $1`, email).Scan(&bound); err != nil || bound != 0 {
		t.Errorf("%d tokens still bound to the erased person (err %v)", bound, err)
	}
	// Idempotent, and a stranger is a no-op.
	if err := s.RevokeAndUnbindLaptopTokens(ctx, poolDB{pool: pool}, email); err != nil {
		t.Errorf("second call: %v", err)
	}
	if err := s.RevokeAndUnbindLaptopTokens(ctx, poolDB{pool: pool}, "nobody@example.com"); err != nil {
		t.Errorf("a stranger: %v", err)
	}
}

// TestIntegrationTokenSummaryReadsLiveTokensAndTheirSeries runs the
// summary's two statements on Postgres: the token read with its array and
// nullable columns, and the series join, with a rotation's two tokens
// counted as one series.
func TestIntegrationTokenSummaryReadsLiveTokensAndTheirSeries(t *testing.T) {
	s := newStore(t, nil)
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	_, old := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 20})
	_, current := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 170})
	_, catalog := intMint(t, s, admin, MintRequest{Platform: PlatformClaudeCode, Environment: "catalog-backend", Scope: ScopeSkillCatalog, ExpiresInDays: 30})
	_, canary := intMint(t, s, admin, MintRequest{Platform: PlatformCapy, Environment: "canary", ExpiresInDays: 30})
	_, stale := intMint(t, s, admin, MintRequest{Platform: PlatformCodex, Environment: "default", ExpiresInDays: 30})
	_ = catalog
	_ = canary
	now := time.Now()
	for i, id := range []string{old.ID, current.ID} {
		row := SkillRow{Origin: OriginBeacon, AgentPlatform: PlatformDevin, Trust: TrustClaimed, SourceTokenID: strPtr(id),
			RawName: "git", Skill: strPtr("git"), SkillSource: SkillSourceUnknown, Trigger: TriggerUnknown, Outcome: OutcomeStarted,
			IdempotencyKey: strPtr("sum-" + string(rune('a'+i))), OccurredAt: now.Add(-time.Duration(3-i) * time.Hour),
			TokenPlatform: PlatformDevin, TokenEnvironment: "default"}
		if _, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, row); err != nil {
			t.Fatal(err)
		}
	}
	// A row older than the lookback is outside the series read: the
	// codex series reads the sentinel, as it would with no row at all.
	// The occurred_at is written directly, since the merge clamps a
	// claimed moment older than a week to received_at.
	staleRow := SkillRow{Origin: OriginBeacon, AgentPlatform: PlatformCodex, Trust: TrustClaimed, SourceTokenID: strPtr(stale.ID),
		RawName: "git", Skill: strPtr("git"), SkillSource: SkillSourceUnknown, Trigger: TriggerUnknown, Outcome: OutcomeStarted,
		IdempotencyKey: strPtr("sum-stale"), OccurredAt: now, TokenPlatform: PlatformCodex, TokenEnvironment: "default"}
	if _, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, staleRow); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE skill_invocations SET occurred_at = $1 WHERE source_token_id = $2::uuid`,
		now.Add(-tokenSeriesLookback-24*time.Hour), stale.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.LogSkillTokenSummary(ctx, now); err != nil {
		t.Fatalf("LogSkillTokenSummary: %v", err)
	}
	var tokens, series []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %v (%s)", err, line)
		}
		switch entry["msg"] {
		case "skill token summary":
			tokens = append(tokens, entry)
		case "skill platform summary":
			series = append(series, entry)
		}
	}
	if len(tokens) != 5 {
		t.Fatalf("got %d token lines, want 5:\n%s", len(tokens), buf.String())
	}
	byID := map[string]map[string]any{}
	for _, l := range tokens {
		byID[l["source_token_id"].(string)] = l
	}
	if l := byID[old.ID]; l["expires_in_days"] != float64(19) || l["issued_days"] != float64(20) || l["platform"] != PlatformDevin || l["soft_revoked"] != false || l["scope"] != ScopeSkillInvocations {
		t.Errorf("old token line = %v", l)
	}
	if l := byID[current.ID]; l["expires_in_days"] != float64(169) || l["last_used_minutes_ago"] != float64(100000) {
		t.Errorf("current token line = %v", l)
	}
	// devin/default/beacon once for the two tokens, with the newer row's
	// age; capy/canary/beacon with the sentinel; codex/default/beacon
	// with the sentinel, its one row being older than the lookback;
	// nothing for the catalog token.
	if len(series) != 3 {
		t.Fatalf("got %d series lines, want 3:\n%s", len(series), buf.String())
	}
	for _, l := range series {
		switch l["platform"] {
		case PlatformDevin:
			if l["environment"] != "default" || l["origin"] != OriginBeacon || l["rows_24h"] != float64(2) || l["trigger"] != "" {
				t.Errorf("devin series = %v", l)
			}
			if m := l["minutes_since_last_row"].(float64); m < 100 || m > 140 {
				t.Errorf("devin series minutes = %v, want about 120 (the newer of the two tokens' rows)", m)
			}
		case PlatformCapy:
			if l["environment"] != "canary" || l["minutes_since_last_row"] != float64(100000) || l["rows_24h"] != float64(0) {
				t.Errorf("canary series = %v", l)
			}
		case PlatformCodex:
			if l["environment"] != "default" || l["minutes_since_last_row"] != float64(100000) || l["rows_24h"] != float64(0) {
				t.Errorf("stale series = %v, want the sentinel: its row is older than the lookback", l)
			}
		default:
			t.Errorf("unexpected series %v", l)
		}
	}
}
