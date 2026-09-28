package store

// The source token statements and their arguments, pinned on the fake
// connection: the verify is the design 4c CTE and nothing else, the state
// mapping reads the row the way the design says, a mint and its audit row
// share a transaction, and the token summary carries the fields the 17d
// metric reads and no secret. What only Postgres can answer (the window
// arithmetic, the partial index, the lock bound) is in
// source_tokens_integration_test.go.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/server/auth"
)

const tokenTestID = "12345678-1234-4123-8123-123456789abc"

func TestAuthenticateSourceTokenIsThe4cStatementWithItsArguments(t *testing.T) {
	db := &fakeDB{}
	s, _ := skillStore(db)
	hash := auth.HashToken("lss_test")

	tok, err := s.AuthenticateSourceToken(context.Background(), db, hash, ScopeSkillInvocations)
	if err != nil {
		t.Fatalf("AuthenticateSourceToken: %v", err)
	}
	if tok.State != TokenStateNone {
		t.Errorf("no row: state = %q, want none", tok.State)
	}
	if len(db.calls) != 1 || db.calls[0].kind != "queryrow" {
		t.Fatalf("issued %d statements (%v), want one QueryRow", len(db.calls), db.calls)
	}
	if got := db.calls[0].sql; got != sourceTokenVerifySQL {
		t.Errorf("statement is not the 4c CTE:\n%s", got)
	}
	for _, want := range []string{"ORDER BY (revoked_at IS NULL) DESC LIMIT 1", "LEFT JOIN u ON true", "RETURNING s.window_count", "t.scope = $3"} {
		if !strings.Contains(db.calls[0].sql, want) {
			t.Errorf("statement lacks %q", want)
		}
	}
	args := db.calls[0].args
	if len(args) != 3 {
		t.Fatalf("args = %v, want hash, window, scope", args)
	}
	if got, ok := args[0].([]byte); !ok || string(got) != string(hash) {
		t.Errorf("arg 1 = %v, want the token hash", args[0])
	}
	if args[1] != "60.000 seconds" {
		t.Errorf("arg 2 = %v, want the 60 s window as an interval", args[1])
	}
	if args[2] != ScopeSkillInvocations {
		t.Errorf("arg 3 = %v, want the route's scope", args[2])
	}
}

// TestSourceTokenStateMapping is the design 4c mapping: a touched row is
// live; an untouched one is revoked, expired or wrong_scope in that order.
func TestSourceTokenStateMapping(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	past, future := now.Add(-time.Hour), now.Add(time.Hour)
	one := 1
	cases := []struct {
		name  string
		row   SourceToken
		count *int
		want  string
	}{
		{"touched is live", SourceToken{Scope: ScopeSkillInvocations}, &one, TokenStateLive},
		{"touched stays live even when the row says revoked", SourceToken{Scope: ScopeSkillInvocations, RevokedAt: &past}, &one, TokenStateLive},
		{"revoked before expired", SourceToken{Scope: ScopeSkillInvocations, RevokedAt: &past, ExpiresAt: &past}, nil, TokenStateRevoked},
		{"revoked before wrong scope", SourceToken{Scope: ScopeSkillCatalog, RevokedAt: &past}, nil, TokenStateRevoked},
		{"expired before wrong scope", SourceToken{Scope: ScopeSkillCatalog, ExpiresAt: &past}, nil, TokenStateExpired},
		{"wrong scope", SourceToken{Scope: ScopeSkillCatalog, ExpiresAt: &future}, nil, TokenStateWrongScope},
		{"an untouched live-looking row is the expiry boundary", SourceToken{Scope: ScopeSkillInvocations, ExpiresAt: &future}, nil, TokenStateExpired},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := sourceTokenState(c.row, c.count, ScopeSkillInvocations, now); got != c.want {
				t.Errorf("state = %q, want %q", got, c.want)
			}
		})
	}
}

func TestAuthenticateSourceTokenReadsTheRowAndTheCount(t *testing.T) {
	db := &fakeDB{}
	expires := time.Now().Add(24 * time.Hour)
	email := "dev@example.com"
	seven := 7
	db.stubs = []*stub{{match: "WITH t AS", rows: [][]any{{
		tokenTestID, PlatformDevin, "default", ScopeSkillInvocations, (*time.Time)(nil), &expires, 0, []string{OriginBeacon, OriginReconciler}, &email, &seven,
	}}}}
	s, _ := skillStore(db)
	tok, err := s.AuthenticateSourceToken(context.Background(), db, auth.HashToken("lss_x"), ScopeSkillInvocations)
	if err != nil {
		t.Fatalf("AuthenticateSourceToken: %v", err)
	}
	if tok.State != TokenStateLive || tok.WindowCount != 7 || tok.ID != tokenTestID || tok.Platform != PlatformDevin {
		t.Errorf("token = %+v", tok)
	}
	if !tok.SoftRevoked() || tok.Cap() != softRevokedCeiling || tok.OverCap() {
		t.Errorf("limit 0: soft revoked with the ceiling as its cap; got soft=%v cap=%d over=%v", tok.SoftRevoked(), tok.Cap(), tok.OverCap())
	}
	if !tok.Allows(OriginBeacon) || tok.Allows(OriginHook) {
		t.Errorf("allowed origins not read: %v", tok.AllowedOrigins)
	}
	if tok.BoundActorEmail == nil || *tok.BoundActorEmail != email {
		t.Errorf("bound actor not read: %v", tok.BoundActorEmail)
	}
	over := SourceToken{RateLimitPerMin: 10, WindowCount: 11}
	if !over.OverCap() || over.Cap() != 10 {
		t.Errorf("a limit of 10 with count 11 is over the cap")
	}
}

func TestAuthenticateSourceTokenReportsAStoreFailure(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "WITH t AS", err: errors.New("connection refused")}}}
	s, _ := skillStore(db)
	if _, err := s.AuthenticateSourceToken(context.Background(), db, auth.HashToken("lss_x"), ScopeSkillInvocations); err == nil {
		t.Fatal("a failed verify was answered as a token state")
	}
}

func TestInSourceTxSetsTheLockBoundAndCommits(t *testing.T) {
	db := &fakeDB{}
	s, _ := skillStore(db)
	err := s.InSourceTx(context.Background(), func(ctx context.Context, q Queryer) error {
		_, err := q.Exec(ctx, "SELECT 1")
		return err
	})
	if err != nil {
		t.Fatalf("InSourceTx: %v", err)
	}
	if len(db.calls) != 2 || db.calls[0].sql != "SET LOCAL lock_timeout = '100ms'" || !db.calls[0].inTx || !db.calls[1].inTx {
		t.Errorf("calls = %+v, want the lock bound then the statement, both in the transaction", db.calls)
	}
	if db.committed != 1 || db.rolled != 0 {
		t.Errorf("committed %d, rolled back %d", db.committed, db.rolled)
	}
	db = &fakeDB{}
	s, _ = skillStore(db)
	boom := errors.New("boom")
	if err := s.InSourceTx(context.Background(), func(context.Context, Queryer) error { return boom }); !errors.Is(err, boom) {
		t.Errorf("fn's error came back as %v", err)
	}
	if db.committed != 0 || db.rolled != 1 {
		t.Errorf("a failing fn committed %d, rolled back %d", db.committed, db.rolled)
	}
}

func TestMintRequestNormalized(t *testing.T) {
	email := "Dev@example.com"
	other := "x@example.net"
	// The deployment's allowlist, derived from the fixture so the two agree.
	domains := []string{testDomainOf(strings.ToLower(email))}
	cases := []struct {
		name  string
		req   MintRequest
		field string
	}{
		{"platform", MintRequest{Platform: "github", Environment: "default", ExpiresInDays: 30}, "platform"},
		{"environment shape", MintRequest{Platform: PlatformDevin, Environment: "Prod Env", ExpiresInDays: 30}, "environment"},
		{"scope", MintRequest{Platform: PlatformDevin, Environment: "default", Scope: "admin", ExpiresInDays: 30}, "scope"},
		{"label length", MintRequest{Platform: PlatformDevin, Environment: "default", Label: strings.Repeat("x", 81), ExpiresInDays: 30}, "label"},
		{"expiry required", MintRequest{Platform: PlatformDevin, Environment: "default"}, "expires_in_days"},
		{"expiry over 180", MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 400}, "expires_in_days"},
		{"origin outside the set", MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 30, AllowedOrigins: []string{OriginDerived}}, "allowed_origins"},
		{"origin repeated", MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 30, AllowedOrigins: []string{OriginHook, OriginHook}}, "allowed_origins"},
		{"laptop without an actor", MintRequest{Platform: PlatformClaudeCode, Environment: EnvironmentLaptop, ExpiresInDays: 30, AllowedOrigins: []string{OriginHook}}, "bound_actor_email"},
		{"cloud with an actor", MintRequest{Platform: PlatformClaudeCode, Environment: "cloud", ExpiresInDays: 30, BoundActorEmail: &email}, "bound_actor_email"},
		{"laptop with a personal address", MintRequest{Platform: PlatformClaudeCode, Environment: EnvironmentLaptop, ExpiresInDays: 30, AllowedOrigins: []string{OriginHook}, BoundActorEmail: &other}, "bound_actor_email"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.req.Normalized(domains)
			var me *MintError
			if !errors.As(err, &me) || me.Field != c.field {
				t.Errorf("error = %v, want a MintError on %s", err, c.field)
			}
		})
	}
	got, err := MintRequest{Platform: " devin ", Environment: "default", ExpiresInDays: 180}.Normalized(domains)
	if err != nil {
		t.Fatalf("a good request failed: %v", err)
	}
	if got.Platform != PlatformDevin || got.Scope != ScopeSkillInvocations || len(got.AllowedOrigins) != 1 || got.AllowedOrigins[0] != OriginBeacon {
		t.Errorf("defaults not applied: %+v", got)
	}
	laptop, err := MintRequest{Platform: PlatformClaudeCode, Environment: EnvironmentLaptop, ExpiresInDays: 30, AllowedOrigins: []string{OriginHook}, BoundActorEmail: &email}.Normalized(domains)
	if err != nil {
		t.Fatalf("a laptop request failed: %v", err)
	}
	if *laptop.BoundActorEmail != "dev@example.com" {
		t.Errorf("bound actor not normalised: %q", *laptop.BoundActorEmail)
	}
	if d := laptop.AuditDetail(); d["bound_actor_email"] != "dev@example.com" || d["expires_in_days"] != 30 {
		t.Errorf("audit detail = %v", d)
	}
}

// TestMintSourceTokenWritesTheRowAndItsAuditRowInOneTransaction: the hash,
// never the plaintext, reaches the statement; the audit row lands in the
// same transaction; the plaintext carries the prefix and 32 random bytes.
func TestMintSourceTokenWritesTheRowAndItsAuditRowInOneTransaction(t *testing.T) {
	db := &fakeDB{}
	issued := time.Now()
	expires := issued.Add(30 * 24 * time.Hour)
	db.stubs = []*stub{{match: "INSERT INTO source_tokens", rows: [][]any{{issued, expires, 1200}}}}
	s, _ := skillStore(db)

	plaintext, row, err := s.MintSourceToken(context.Background(), Viewer{Email: "Boss@example.com", Role: RoleAdmin},
		MintRequest{Platform: PlatformCapy, Environment: "backend", Label: "capy backend", ExpiresInDays: 30})
	if err != nil {
		t.Fatalf("MintSourceToken: %v", err)
	}
	if !strings.HasPrefix(plaintext, SourceTokenPrefix) || len(plaintext) != len(SourceTokenPrefix)+43 {
		t.Errorf("plaintext %q is not lss_ plus 32 base64url bytes", plaintext)
	}
	if !isUUID(row.ID) || row.Platform != PlatformCapy || row.RateLimitPerMin != 1200 || row.ExpiresAt == nil || !row.ExpiresAt.Equal(expires) || row.State != TokenStateLive {
		t.Errorf("row = %+v", row)
	}
	if len(db.calls) != 2 {
		t.Fatalf("issued %d statements, want the insert and the audit row", len(db.calls))
	}
	ins, audit := db.calls[0], db.calls[1]
	if !ins.inTx || !audit.inTx || db.committed != 1 {
		t.Errorf("not one committed transaction: insert in tx %v, audit in tx %v, committed %d", ins.inTx, audit.inTx, db.committed)
	}
	if !strings.Contains(ins.sql, "now() + ($8::int * interval '24 hours')") {
		t.Errorf("the expiry is not computed by the database: %s", ins.sql)
	}
	for _, a := range ins.args {
		if strings.Contains(argText(a), plaintext) {
			t.Fatalf("the plaintext reached the statement: %v", ins.args)
		}
	}
	if got, ok := ins.args[5].([]byte); !ok || string(got) != string(auth.HashToken(plaintext)) {
		t.Errorf("arg 6 is not auth.HashToken(plaintext)")
	}
	if ins.args[6] != "boss@example.com" {
		t.Errorf("issued_by = %v, want the normalised caller", ins.args[6])
	}
	if !strings.Contains(audit.sql, "INSERT INTO admin_actions") || audit.args[1] != ActionSourceTokenMint || audit.args[2] != row.ID {
		t.Errorf("audit row = %v %v", audit.sql, audit.args)
	}
	if detail := audit.args[3].(string); strings.Contains(detail, plaintext) || !strings.Contains(detail, `"platform":"capy"`) {
		t.Errorf("audit detail = %s", detail)
	}
}

func TestMintSourceTokenRollsBackWhenTheAuditRowFails(t *testing.T) {
	db := &fakeDB{stubs: []*stub{
		{match: "INSERT INTO source_tokens", rows: [][]any{{time.Now(), time.Now(), 1200}}},
		{match: "INSERT INTO admin_actions", err: errors.New("disk full")},
	}}
	s, _ := skillStore(db)
	if _, _, err := s.MintSourceToken(context.Background(), Viewer{Email: "boss@example.com", Role: RoleAdmin},
		MintRequest{Platform: PlatformCapy, Environment: "backend", ExpiresInDays: 30}); err == nil {
		t.Fatal("a failed audit row minted a token")
	}
	if db.committed != 0 || db.rolled != 1 {
		t.Errorf("committed %d, rolled back %d", db.committed, db.rolled)
	}
}

func TestSetLimitAndRevokeAreAuditedAndReportAnUnknownID(t *testing.T) {
	admin := Viewer{Email: "boss@example.com", Role: RoleAdmin}

	db := &fakeDB{}
	s, _ := skillStore(db)
	if err := s.SetSourceTokenLimit(context.Background(), admin, tokenTestID, 0); err != nil {
		t.Fatalf("SetSourceTokenLimit: %v", err)
	}
	if len(db.calls) != 2 || !strings.Contains(db.calls[0].sql, "SET rate_limit_per_min = $2") || db.calls[0].args[1] != 0 ||
		db.calls[1].args[1] != ActionSourceTokenLimit || db.calls[1].args[2] != tokenTestID || db.committed != 1 {
		t.Errorf("calls = %+v, committed %d", db.calls, db.committed)
	}
	if err := s.SetSourceTokenLimit(context.Background(), admin, tokenTestID, 100001); err == nil {
		t.Error("a limit over the CHECK was accepted")
	}
	if err := s.SetSourceTokenLimit(context.Background(), Viewer{Email: "dev@example.com", Role: RoleMember}, tokenTestID, 5); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("a member set a limit: %v", err)
	}

	db = &fakeDB{stubs: []*stub{{match: "UPDATE source_tokens SET rate_limit_per_min", affected: 0}}}
	s, _ = skillStore(db)
	if err := s.SetSourceTokenLimit(context.Background(), admin, tokenTestID, 5); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown id = %v, want ErrNotFound", err)
	}
	if db.rolled != 1 {
		t.Errorf("the miss did not roll back: rolled %d", db.rolled)
	}
	if err := s.SetSourceTokenLimit(context.Background(), admin, "not-a-uuid", 5); !errors.Is(err, ErrNotFound) {
		t.Errorf("a malformed id = %v, want ErrNotFound before any statement", err)
	}

	db = &fakeDB{stubs: []*stub{{match: "INSERT INTO admin_actions", err: errors.New("disk full")}}}
	s, _ = skillStore(db)
	if err := s.SetSourceTokenLimit(context.Background(), admin, tokenTestID, 5); err == nil || db.committed != 0 || db.rolled != 1 {
		t.Errorf("a failed audit row did not roll the limit change back: err=%v committed=%d rolled=%d", err, db.committed, db.rolled)
	}

	db = &fakeDB{}
	s, _ = skillStore(db)
	if err := s.RevokeSourceToken(context.Background(), admin, tokenTestID); err != nil {
		t.Fatalf("RevokeSourceToken: %v", err)
	}
	if len(db.calls) != 2 || !strings.Contains(db.calls[0].sql, "revoked_at = COALESCE(revoked_at, now())") || db.calls[0].args[1] != "boss@example.com" ||
		db.calls[1].args[1] != ActionSourceTokenRevoke || db.committed != 1 {
		t.Errorf("calls = %+v, committed %d", db.calls, db.committed)
	}
}

func TestRevokeAndUnbindLaptopTokensStatement(t *testing.T) {
	db := &fakeDB{}
	s, _ := skillStore(db)
	if err := s.RevokeAndUnbindLaptopTokens(context.Background(), db, " Dev@Example.com "); err != nil {
		t.Fatalf("RevokeAndUnbindLaptopTokens: %v", err)
	}
	// Two statements, revoke then unbind: the revoke's WHERE reads the
	// binding the unbind clears, and the README's purge step 8 runs the
	// same pair in the same order.
	if len(db.calls) != 2 {
		t.Fatalf("issued %d statements, want two", len(db.calls))
	}
	revoke, unbind := db.calls[0], db.calls[1]
	for _, want := range []string{"revoked_at = now()", "revoked_at IS NULL", "environment = $2", "bound_actor_email = $1"} {
		if !strings.Contains(revoke.sql, want) {
			t.Errorf("revoke lacks %q: %s", want, revoke.sql)
		}
	}
	if strings.Contains(revoke.sql, "bound_actor_email = NULL") || len(revoke.args) != 2 || revoke.args[0] != "dev@example.com" || revoke.args[1] != EnvironmentLaptop {
		t.Errorf("revoke = %s %v", revoke.sql, revoke.args)
	}
	if !strings.Contains(unbind.sql, "bound_actor_email = NULL") || strings.Contains(unbind.sql, "environment") || strings.Contains(unbind.sql, "revoked_at") ||
		len(unbind.args) != 1 || unbind.args[0] != "dev@example.com" {
		t.Errorf("unbind = %s %v", unbind.sql, unbind.args)
	}
	if err := s.RevokeAndUnbindLaptopTokens(context.Background(), db, ""); err == nil {
		t.Error("an empty email was accepted")
	}
}

func TestSessionOwnerAndActorKnown(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "SELECT email FROM sessions", rows: [][]any{{"owner@example.com"}}}}}
	s, _ := skillStore(db)
	email, found, err := s.SessionOwner(context.Background(), db, "sess-1")
	if err != nil || !found || email != "owner@example.com" {
		t.Errorf("SessionOwner = %q %v %v", email, found, err)
	}
	db = &fakeDB{}
	s, _ = skillStore(db)
	if _, found, err := s.SessionOwner(context.Background(), db, "sess-2"); err != nil || found {
		t.Errorf("an unseen session = found %v err %v", found, err)
	}
	known, err := s.ActorKnown(context.Background(), db, "nobody@example.com")
	if err != nil || known {
		t.Errorf("an unknown actor = %v %v", known, err)
	}
}

// TestLogSkillTokenSummaryCarriesTheContractFields: one line per live
// token with the 7.1 fields, the expiry arithmetic 17d reads, and no hash
// on any line; then one "skill platform summary" line per series the
// tokens open, never per token, trigger empty.
func TestLogSkillTokenSummaryCarriesTheContractFields(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	issued := now.Add(-10 * 24 * time.Hour)
	expiresSoon := now.Add(5*24*time.Hour + time.Hour)
	used := now.Add(-30 * time.Minute)
	other := "22222222-2222-4222-8222-222222222222"
	catalog := "33333333-3333-4333-8333-333333333333"
	justUsed := "44444444-4444-4444-8444-444444444444"
	// A post landed between the tick's clock capture and the read, or
	// the database clock runs a little ahead: last_used_at after now.
	usedAhead := now.Add(2 * time.Second)
	db := &fakeDB{stubs: []*stub{
		{match: "FROM source_tokens WHERE revoked_at IS NULL", rows: [][]any{
			{tokenTestID, PlatformDevin, "default", ScopeSkillInvocations, issued, &expiresSoon, &used, 40, 1200, []string{OriginBeacon, OriginReconciler}},
			{other, PlatformDevin, "default", ScopeSkillInvocations, issued, (*time.Time)(nil), (*time.Time)(nil), 0, 0, []string{OriginBeacon}},
			{catalog, PlatformClaudeCode, "catalog-backend", ScopeSkillCatalog, issued, &expiresSoon, (*time.Time)(nil), 0, 1200, []string{OriginBeacon}},
			{justUsed, PlatformCapy, "default", ScopeSkillInvocations, issued, &expiresSoon, &usedAhead, 1, 1200, []string{OriginBeacon}},
		}},
		{match: "JOIN source_tokens st ON st.id = s.source_token_id", rows: [][]any{
			{PlatformDevin, "default", OriginBeacon, now.Add(-2 * time.Hour), int64(3)},
		}},
	}}
	s, buf := skillStore(db)
	if err := s.LogSkillTokenSummary(context.Background(), now); err != nil {
		t.Fatalf("LogSkillTokenSummary: %v", err)
	}
	lines := skillLines(t, buf)
	summaries := linesWithMessage(lines, "skill token summary")
	if len(summaries) != 4 {
		t.Fatalf("got %d token summary lines, want 4:\n%s", len(summaries), buf.String())
	}
	if l := summaries[3]; l["source_token_id"] != justUsed || l["last_used_minutes_ago"] != float64(0) {
		t.Errorf("a token used after the tick's clock capture: %v, want last_used_minutes_ago 0, not the never-used sentinel", l)
	}
	first := summaries[0]
	want := map[string]any{
		"platform": PlatformDevin, "environment": "default", "source_token_id": tokenTestID, "scope": ScopeSkillInvocations,
		"expires_in_days": float64(5), "issued_days": float64(15), "last_used_minutes_ago": float64(30),
		"window_count": float64(40), "rate_limit_per_min": float64(1200), "soft_revoked": false,
	}
	for k, v := range want {
		if first[k] != v {
			t.Errorf("line %s = %v, want %v", k, first[k], v)
		}
	}
	second := summaries[1]
	if second["expires_in_days"] != float64(0) || second["issued_days"] != float64(skillSilenceSentinel) ||
		second["last_used_minutes_ago"] != float64(tokenNeverUsed) || second["soft_revoked"] != true {
		t.Errorf("a NULL expiry, never used, limit 0 token: %v", second)
	}
	for _, l := range lines {
		for k := range l {
			if strings.Contains(k, "hash") || strings.Contains(k, "token_hash") {
				t.Errorf("a line carries a hash field: %v", l)
			}
		}
	}
	series := linesWithMessage(lines, "skill platform summary")
	if len(series) != 2 {
		t.Fatalf("got %d series lines, want 2 (devin/default/beacon and capy/default/beacon; the catalog token opens none, the reconciler origin opens none):\n%s", len(series), buf.String())
	}
	sr := series[1]
	if sr["platform"] != PlatformDevin || sr["environment"] != "default" || sr["origin"] != OriginBeacon || sr["trigger"] != "" ||
		sr["rows_24h"] != float64(3) || sr["minutes_since_last_row"] != float64(120) {
		t.Errorf("series line = %v", sr)
	}
	if len(db.calls) != 2 {
		t.Errorf("issued %d statements, want the token read and one series read", len(db.calls))
	}
	// The series read is bounded: its second argument is the lookback's
	// start, so the aggregate never walks rows older than the sentinel
	// would report anyway.
	read := db.calls[1]
	if !strings.Contains(read.sql, "s.occurred_at >= $2") || len(read.args) != 2 || read.args[1] != now.Add(-tokenSeriesLookback) {
		t.Errorf("series read = %s with %v, want the lookback bound as its second argument", read.sql, read.args)
	}
	if tokenSeriesLookback < 8*24*time.Hour {
		t.Errorf("the lookback %v is inside 18b's longest threshold (a week plus the weekend mask)", tokenSeriesLookback)
	}
}

func TestLogSkillTokenSummaryOpensASeriesWithTheSentinelForATokenThatNeverPosted(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	db := &fakeDB{stubs: []*stub{
		{match: "FROM source_tokens WHERE revoked_at IS NULL", rows: [][]any{
			{tokenTestID, PlatformClaudeCode, "canary", ScopeSkillInvocations, now, &now, (*time.Time)(nil), 0, 1200, []string{OriginHook, OriginBeacon}},
		}},
	}}
	s, buf := skillStore(db)
	if err := s.LogSkillTokenSummary(context.Background(), now); err != nil {
		t.Fatalf("LogSkillTokenSummary: %v", err)
	}
	series := linesWithMessage(skillLines(t, buf), "skill platform summary")
	if len(series) != 2 {
		t.Fatalf("got %d series lines, want hook and beacon:\n%s", len(series), buf.String())
	}
	for _, sr := range series {
		if sr["minutes_since_last_row"] != float64(skillSilenceSentinel) || sr["environment"] != "canary" {
			t.Errorf("series line = %v", sr)
		}
	}
}

// TestPreemptedLineNamesTheDisplacedCredential: when the merge reports
// that a derived copy displaced an API row, the derivation logs the 7.1
// line with whichever of the token or the device is set, ids and the slug
// only.
func TestPreemptedLineNamesTheDisplacedCredential(t *testing.T) {
	db := &fakeDB{stubs: []*stub{{match: "INSERT INTO skill_invocations", rows: [][]any{{false, true, tokenTestID, nil}}}}}
	s, buf := skillStore(db)
	call := skillCall("evt-call", "sess-skill", "p1", "toolu_1", `{"skill":"engg:git","args":"customer acme"}`, 1)
	if _, err := s.insertSkillInvocations(context.Background(), db, []Ingest{call}); err != nil {
		t.Fatalf("insertSkillInvocations: %v", err)
	}
	lines := linesWithMessage(skillLines(t, buf), "skill invocation preempted")
	if len(lines) != 1 {
		t.Fatalf("got %d preempted lines, want 1:\n%s", len(lines), buf.String())
	}
	l := lines[0]
	if l["level"] != "WARN" || l["platform"] != PlatformClaudeCode || l["preempted_by"] != tokenTestID || l["preempted_device"] != "" ||
		l["session_ref"] != "sess-skill" || l["skill"] != "git" {
		t.Errorf("preempted line = %v", l)
	}
	if strings.Contains(buf.String(), "customer acme") {
		t.Errorf("the arguments reached a line: %s", buf.String())
	}

	// The device form: an lsd_ API row displaced.
	db = &fakeDB{stubs: []*stub{{match: "INSERT INTO skill_invocations", rows: [][]any{{false, true, nil, skillTestDevice}}}}}
	s, buf = skillStore(db)
	if _, err := s.insertSkillInvocations(context.Background(), db, []Ingest{call}); err != nil {
		t.Fatalf("insertSkillInvocations: %v", err)
	}
	lines = linesWithMessage(skillLines(t, buf), "skill invocation preempted")
	if len(lines) != 1 || lines[0]["preempted_device"] != skillTestDevice || lines[0]["preempted_by"] != "" {
		t.Errorf("device form = %v", lines)
	}
	// Not preempted: no line.
	db = &fakeDB{stubs: []*stub{insertedRow()}}
	s, buf = skillStore(db)
	if _, err := s.insertSkillInvocations(context.Background(), db, []Ingest{call}); err != nil {
		t.Fatalf("insertSkillInvocations: %v", err)
	}
	if n := len(linesWithMessage(skillLines(t, buf), "skill invocation preempted")); n != 0 {
		t.Errorf("an inserted row logged %d preempted lines", n)
	}
}
