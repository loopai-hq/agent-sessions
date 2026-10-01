package app

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/server/admin"
	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/skillusage"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// RegisterSkillUsage restates the handler's route list on the server's
// mux, as RegisterIngest does; this is the same parity check, so a route
// the handler grows (the reconciler-runs route) is mounted on the server
// the moment it exists.
func TestRegisterSkillUsageMountsEveryRouteTheHandlerRegisters(t *testing.T) {
	paths := skillUsageRoutePaths(t)
	if len(paths) < 1 {
		t.Fatalf("read %v from the skillusage package; expected at least the invocations path", paths)
	}
	h, err := skillusage.New(skillusage.Options{
		Store:   nopSkillStore{},
		Devices: uaDevices{},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("skillusage.New: %v", err)
	}
	own := http.NewServeMux()
	h.Register(own)
	ours := http.NewServeMux()
	RegisterSkillUsage(ours, h)
	for _, path := range paths {
		for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			r := httptest.NewRequest(method, path, nil)
			_, want := own.Handler(r)
			_, got := ours.Handler(r)
			if got != want {
				t.Errorf("%s %s: the handler's own mounting resolves to %q, RegisterSkillUsage to %q", method, path, want, got)
			}
		}
	}
	found := false
	for _, p := range paths {
		if p == skillusage.InvocationsPath {
			found = true
		}
	}
	if !found {
		t.Errorf("the skillusage package's routes %v do not include %s", paths, skillusage.InvocationsPath)
	}
}

type nopSkillStore struct{}

func (nopSkillStore) AuthenticateSource(context.Context, []byte, string) (store.SourceToken, error) {
	return store.SourceToken{State: store.TokenStateNone}, nil
}

func (nopSkillStore) InTx(context.Context, func(context.Context, skillusage.Tx) error) error {
	return nil
}

// The adapter's verify is a transaction of its own: begun, bounded,
// the one statement, committed, before the row's transaction begins, so
// the token row is never held across a request. Observed on the boot fake
// as the statement sequence the store issued.
func TestTheVerifyCommitsBeforeTheRowTransactionOpens(t *testing.T) {
	db := &bootDB{}
	st := NewSkillUsageStore(store.NewWithDB(db, ingest.NewPricer(nil, bootLogger())))
	ctx := context.Background()
	tok, err := st.AuthenticateSource(ctx, auth.HashToken(store.SourceTokenPrefix+strings.Repeat("a", 43)), store.ScopeSkillInvocations)
	if err != nil {
		t.Fatalf("AuthenticateSource: %v", err)
	}
	if tok.State != store.TokenStateNone {
		t.Errorf("no row: state %q, want none", tok.State)
	}
	afterVerify := len(db.snapshot())
	if err := st.InTx(ctx, func(ctx context.Context, tx skillusage.Tx) error {
		_, _, err := tx.SessionOwner(ctx, "sess-1")
		return err
	}); err != nil {
		t.Fatalf("InTx: %v", err)
	}
	calls := db.snapshot()
	var got []string
	for _, c := range calls {
		switch {
		case strings.HasPrefix(c.sql, "SET LOCAL lock_timeout"):
			got = append(got, "lock bound")
		case strings.HasPrefix(c.sql, "WITH t AS"):
			got = append(got, "verify")
		case strings.Contains(c.sql, "FROM sessions WHERE session_id"):
			got = append(got, "session owner")
		default:
			got = append(got, c.sql)
		}
	}
	// SET LOCAL is issued once per transaction InSourceTx opens, right
	// after its Begin, so two bounds are two transactions: the verify's
	// own, then the row's.
	want := []string{"lock bound", "verify", "lock bound", "session owner"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("statements = %v, want %v: the verify and the row each under their own bound", got, want)
	}
	if afterVerify != 2 {
		t.Errorf("the verify issued %d statements before returning, want the bound and the CTE only", afterVerify)
	}
}

// skillUsageRoutePaths reads the skillusage package's non-test source for
// the paths it could route, the way ingestRoutePaths reads the ingest
// package's.
func skillUsageRoutePaths(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join("..", "skillusage")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	seen := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if p := routePath(n); p != "" {
				seen[p] = true
			}
			return true
		})
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// The upload route keeps refusing a source token by prefix, with no
// lookup: the two credentials are told apart before the database sees
// either.
func TestTheUploadVerifierRefusesASourceTokenWithoutALookup(t *testing.T) {
	db := &bootDB{}
	devices := newStoreDevices(store.NewWithDB(db, ingest.NewPricer(nil, bootLogger())))
	_, err := devices.Verify(context.Background(), store.SourceTokenPrefix+strings.Repeat("a", 43))
	if !errors.Is(err, ingest.ErrUnauthenticated) {
		t.Fatalf("an lss_ bearer on the upload verifier: %v, want ErrUnauthenticated", err)
	}
	if !errors.Is(err, auth.ErrDeviceTokenMalformed) {
		t.Errorf("the cause was not the prefix: %v", err)
	}
	if got := len(db.snapshot()); got != 0 {
		t.Errorf("issued %d statements, want none", got)
	}
}

// The admin adapter's translation of the token routes' two sentinels.
func TestAdminTokenAdapterTranslatesTheStoreSentinels(t *testing.T) {
	fx := &fakeTokenTx{}
	tx := adminTx{tx: fx}

	fx.err = &store.MintError{Field: "platform", Reason: "must be one of"}
	_, err := tx.MintSourceToken(context.Background(), "boss@example.com", admin.SourceTokenMint{Platform: "github"})
	var fe *admin.FieldError
	if !errors.As(err, &fe) || fe.Field != "platform" {
		t.Errorf("a MintError became %v, want a FieldError on platform", err)
	}
	fx.err = store.ErrNotFound
	if err := tx.SetSourceTokenLimit(context.Background(), "x", 5); !errors.Is(err, admin.ErrTokenNotFound) {
		t.Errorf("ErrNotFound became %v", err)
	}
	if err := tx.RevokeSourceToken(context.Background(), "x", "boss@example.com"); !errors.Is(err, admin.ErrTokenNotFound) {
		t.Errorf("ErrNotFound became %v", err)
	}
	other := errors.New("disk full")
	fx.err = other
	if err := tx.RevokeSourceToken(context.Background(), "x", "boss@example.com"); !errors.Is(err, other) {
		t.Errorf("an unrelated error became %v", err)
	}
	fx.err = nil
	expires := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	fx.row = store.SourceToken{ID: "id-1", Platform: "devin", Environment: "default", Scope: "skill-invocations", Label: "l",
		RateLimitPerMin: 1200, AllowedOrigins: []string{"beacon"}, ExpiresAt: &expires}
	out, err := tx.MintSourceToken(context.Background(), "boss@example.com", admin.SourceTokenMint{Platform: "devin", Environment: "default", ExpiresInDays: 30})
	if err != nil {
		t.Fatalf("MintSourceToken: %v", err)
	}
	if out.ID != "id-1" || out.Token != "lss_plain" || out.Platform != "devin" || !out.ExpiresAt.Equal(expires) || out.RateLimitPerMin != 1200 {
		t.Errorf("issued = %+v", out)
	}
	if fx.req.ExpiresInDays != 30 || fx.req.BoundActorEmail != nil || fx.actor != "boss@example.com" {
		t.Errorf("the store saw %+v as %q; the admin mint never binds an actor", fx.req, fx.actor)
	}
}

// fakeTokenTx is a store.AdminTx whose token methods answer as told; the
// roster methods are never reached here.
type fakeTokenTx struct {
	err   error
	row   store.SourceToken
	req   store.MintRequest
	actor string
}

func (f *fakeTokenTx) Principal(context.Context, string) (store.Principal, bool, error) {
	return store.Principal{}, false, nil
}
func (f *fakeTokenTx) CountActiveAdminsExcept(context.Context, string) (int, error) { return 0, nil }
func (f *fakeTokenTx) SavePrincipal(context.Context, store.Principal) error         { return nil }
func (f *fakeTokenTx) RecordPrincipalChange(context.Context, store.PrincipalChange) error {
	return nil
}
func (f *fakeTokenTx) RecordAdminAction(context.Context, string, string, string, any) error {
	return nil
}
func (f *fakeTokenTx) EnqueueSkillRederiveSince(context.Context, time.Time) (int64, error) {
	return 0, nil
}
func (f *fakeTokenTx) ResetSkillDeriveStep(context.Context) error { return nil }
func (f *fakeTokenTx) MintSourceToken(_ context.Context, actor string, req store.MintRequest) (string, store.SourceToken, error) {
	f.actor, f.req = actor, req
	if f.err != nil {
		return "", store.SourceToken{}, f.err
	}
	return "lss_plain", f.row, nil
}
func (f *fakeTokenTx) SetSourceTokenLimit(context.Context, string, int) error  { return f.err }
func (f *fakeTokenTx) RevokeSourceToken(context.Context, string, string) error { return f.err }
