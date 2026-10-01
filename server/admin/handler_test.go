package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/health"
)

var testNow = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeStore stands in for Postgres. InTx serialises and rolls back, which is the
// behaviour the real store is required to provide, so the guards under test are
// exercised against the same contract the SQL implementation must honour.
type fakeStore struct {
	mu         sync.Mutex
	principals map[string]Principal
	devices    []Device
	health     []HealthSnapshot
	access     []AccessEvent
	changes    []PrincipalChange
	saves      []Principal
	lastQuery  AccessQuery

	errPrincipal error
	errList      error
	errDevices   error
	errHealth    error
	errAccess    error
	errSave      error
	errRecord    error

	// The skill derive rerun's three statements, as the fake saw them.
	actions     []adminAction
	enqueued    []time.Time
	resets      int
	errAction   error
	errRerun    error
	queuedCount int64

	// The source token routes' rows, as the fake holds them; rolled back
	// with the roster when a transaction fails.
	tokens   map[string]fakeToken
	errToken error

	// onTx runs at the start of every transaction, so a test can simulate a
	// change that lands between the request arriving and the transaction reading.
	onTx func(*fakeStore)
}

func newStore(ps ...Principal) *fakeStore {
	s := &fakeStore{principals: map[string]Principal{}}
	for _, p := range ps {
		s.principals[p.Email] = p
	}
	return s
}

func admin(email string) Principal {
	return Principal{Email: email, Role: RoleAdmin, AddedAt: testNow.Add(-90 * 24 * time.Hour)}
}

func member(email string) Principal {
	return Principal{Email: email, Role: RoleMember, AddedAt: testNow.Add(-90 * 24 * time.Hour)}
}

func disabled(p Principal) Principal {
	at := testNow.Add(-time.Hour)
	p.DisabledAt = &at
	return p
}

func (s *fakeStore) Principal(_ context.Context, email string) (Principal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.principal(email)
}

func (s *fakeStore) principal(email string) (Principal, bool, error) {
	if s.errPrincipal != nil {
		return Principal{}, false, s.errPrincipal
	}
	p, ok := s.principals[email]
	return p, ok, nil
}

func (s *fakeStore) ListPrincipals(context.Context) ([]Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.errList != nil {
		return nil, s.errList
	}
	out := make([]Principal, 0, len(s.principals))
	for _, p := range s.principals {
		out = append(out, p)
	}
	return out, nil
}

func (s *fakeStore) EnrolledDevices(context.Context) ([]Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.devices, s.errDevices
}

func (s *fakeStore) LatestHealth(context.Context) ([]HealthSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health, s.errHealth
}

func (s *fakeStore) AccessEvents(_ context.Context, q AccessQuery) ([]AccessEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastQuery = q
	return s.access, s.errAccess
}

func (s *fakeStore) InTx(ctx context.Context, fn func(context.Context, Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.onTx != nil {
		s.onTx(s)
	}
	before := make(map[string]Principal, len(s.principals))
	for k, v := range s.principals {
		before[k] = v
	}
	changes := len(s.changes)
	saves := len(s.saves)
	tokens := cloneTokens(s.tokens)

	if err := fn(ctx, fakeTx{s}); err != nil {
		s.principals = before
		s.changes = s.changes[:changes]
		s.saves = s.saves[:saves]
		s.tokens = tokens
		return err
	}
	return nil
}

func (s *fakeStore) activeAdmins() int {
	n := 0
	for _, p := range s.principals {
		if p.ActiveAdmin() {
			n++
		}
	}
	return n
}

// fakeTx deliberately does not lock: InTx holds the mutex for the whole
// transaction, which is how the real store is required to serialise.
type fakeTx struct{ s *fakeStore }

func (t fakeTx) Principal(_ context.Context, email string) (Principal, bool, error) {
	return t.s.principal(email)
}

func (t fakeTx) CountActiveAdminsExcept(_ context.Context, email string) (int, error) {
	n := 0
	for _, p := range t.s.principals {
		if p.Email != email && p.ActiveAdmin() {
			n++
		}
	}
	return n, nil
}

func (t fakeTx) SavePrincipal(_ context.Context, p Principal) error {
	if t.s.errSave != nil {
		return t.s.errSave
	}
	t.s.principals[p.Email] = p
	t.s.saves = append(t.s.saves, p)
	return nil
}

func (t fakeTx) RecordPrincipalChange(_ context.Context, c PrincipalChange) error {
	if t.s.errRecord != nil {
		return t.s.errRecord
	}
	t.s.changes = append(t.s.changes, c)
	return nil
}

// adminAction is one audit row the rerun route wrote.
type adminAction struct {
	Actor, Action, Target string
	Detail                any
}

func (t fakeTx) RecordAdminAction(_ context.Context, actor, action, target string, detail any) error {
	if t.s.errAction != nil {
		return t.s.errAction
	}
	t.s.actions = append(t.s.actions, adminAction{Actor: actor, Action: action, Target: target, Detail: detail})
	return nil
}

func (t fakeTx) EnqueueSkillRederiveSince(_ context.Context, since time.Time) (int64, error) {
	if t.s.errRerun != nil {
		return 0, t.s.errRerun
	}
	t.s.enqueued = append(t.s.enqueued, since)
	return t.s.queuedCount, nil
}

func (t fakeTx) ResetSkillDeriveStep(_ context.Context) error {
	if t.s.errRerun != nil {
		return t.s.errRerun
	}
	t.s.resets++
	return nil
}

// headerAuth reads the caller from a header so one handler can serve several
// identities, and carries no role: role is the store's business.
type headerAuth struct{}

func (headerAuth) Authenticate(r *http.Request) (Identity, error) {
	switch e := r.Header.Get("X-Test-User"); e {
	case "":
		return Identity{}, ErrNoIdentity
	case "broken":
		return Identity{}, errors.New("identity provider unreachable")
	default:
		return Identity{Email: e}, nil
	}
}

func newHandler(t *testing.T, s *fakeStore) *Handler {
	t.Helper()
	h, err := New(Options{
		Store:  s,
		Auth:   headerAuth{},
		Now:    func() time.Time { return testNow },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return h
}

func do(t *testing.T, h *Handler, method, path, user, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if user != "" {
		r.Header.Set("X-Test-User", user)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func decode[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %s: %v", w.Body.String(), err)
	}
	return v
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

var adminRoutes = []struct {
	method, path, body string
}{
	{http.MethodGet, "/v1/admin/principals", ""},
	{http.MethodGet, "/v1/admin/fleet", ""},
	{http.MethodGet, "/v1/admin/access-log", ""},
	{http.MethodPut, "/v1/admin/principals/someone@example.com", `{"role":"admin"}`},
	{http.MethodPost, "/v1/admin/derive/skill-invocations/rerun", ""},
	{http.MethodPost, "/v1/admin/source-tokens", `{"platform":"devin","environment":"default","expires_in_days":30}`},
	{http.MethodPost, "/v1/admin/source-tokens/12345678-1234-4123-8123-123456789abc/limit", `{"rate_limit_per_min":0}`},
	{http.MethodPost, "/v1/admin/source-tokens/12345678-1234-4123-8123-123456789abc/revoke", ""},
}

func TestMemberGets404NotForbidden(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	h := newHandler(t, s)

	for _, rt := range adminRoutes {
		w := do(t, h, rt.method, rt.path, "dev@example.com", rt.body)
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s %s: status = %d, want 404", rt.method, rt.path, w.Code)
		}
		// A body that explains the caller is not an admin confirms the endpoint
		// exists and that there is a roster behind it, which is the leak the 404
		// is there to prevent.
		if b := strings.ToLower(w.Body.String()); strings.Contains(b, "admin") || strings.Contains(b, "role") {
			t.Fatalf("%s %s: body leaks the reason: %s", rt.method, rt.path, w.Body.String())
		}
	}
	if len(s.changes) != 0 {
		t.Fatalf("a member's request produced %d changes", len(s.changes))
	}

	// A 400 here would tell a member that something on this path parses bodies,
	// which is the existence of the admin surface leaking through the back door.
	w := do(t, h, http.MethodPut, "/v1/admin/principals/someone@example.com", "dev@example.com", `{`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("malformed body from a member: status = %d, want 404", w.Code)
	}
}

func TestUnknownCallerGets404(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	h := newHandler(t, s)
	w := do(t, h, http.MethodGet, "/v1/admin/principals", "stranger@example.com", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestDisabledAdminGets404(t *testing.T) {
	s := newStore(disabled(admin("boss@example.com")), admin("other@example.com"))
	h := newHandler(t, s)
	w := do(t, h, http.MethodGet, "/v1/admin/principals", "boss@example.com", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestNoCredentialGets401(t *testing.T) {
	h := newHandler(t, newStore(admin("boss@example.com")))
	w := do(t, h, http.MethodGet, "/v1/admin/principals", "", "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestAuthenticatorFailureIs500(t *testing.T) {
	h := newHandler(t, newStore(admin("boss@example.com")))
	w := do(t, h, http.MethodGet, "/v1/admin/principals", "broken", "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

// TestAuthorityIsRereadPerRequest is the whole reason the role is not carried on
// the credential: a demotion has to take effect on the next request, not on the
// next sign-in.
func TestAuthorityIsRereadPerRequest(t *testing.T) {
	s := newStore(admin("boss@example.com"), admin("second@example.com"))
	h := newHandler(t, s)

	if w := do(t, h, http.MethodGet, "/v1/admin/principals", "boss@example.com", ""); w.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", w.Code)
	}
	s.mu.Lock()
	s.principals["boss@example.com"] = member("boss@example.com")
	s.mu.Unlock()

	if w := do(t, h, http.MethodGet, "/v1/admin/principals", "boss@example.com", ""); w.Code != http.StatusNotFound {
		t.Fatalf("after demotion status = %d, want 404", w.Code)
	}
}

// TestActorDemotedInsideTransaction covers the narrower race: the caller is an
// admin when the request arrives and is not one by the time the transaction
// opens. The check has to live inside the transaction for this to be caught.
func TestActorDemotedInsideTransaction(t *testing.T) {
	s := newStore(admin("boss@example.com"), admin("second@example.com"), member("dev@example.com"))
	s.onTx = func(st *fakeStore) { st.principals["boss@example.com"] = member("boss@example.com") }
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/dev@example.com", "boss@example.com", `{"role":"admin"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if got := s.principals["dev@example.com"].Role; got != RoleMember {
		t.Fatalf("role = %q, want unchanged member", got)
	}
	if len(s.changes) != 0 {
		t.Fatalf("recorded %d changes for a request that was refused", len(s.changes))
	}
}

// ---------------------------------------------------------------------------
// Principals: listing
// ---------------------------------------------------------------------------

func TestListPrincipalsSortedWithAdminCount(t *testing.T) {
	s := newStore(
		member("zoe@example.com"),
		admin("boss@example.com"),
		disabled(admin("gone@example.com")),
	)
	h := newHandler(t, s)
	w := do(t, h, http.MethodGet, "/v1/admin/principals", "boss@example.com", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	got := decode[struct {
		Principals   []Principal `json:"principals"`
		ActiveAdmins int         `json:"active_admins"`
	}](t, w)

	want := []string{"boss@example.com", "gone@example.com", "zoe@example.com"}
	if len(got.Principals) != len(want) {
		t.Fatalf("got %d principals, want %d", len(got.Principals), len(want))
	}
	for i, e := range want {
		if got.Principals[i].Email != e {
			t.Fatalf("principal %d = %q, want %q", i, got.Principals[i].Email, e)
		}
	}
	// The disabled admin must not be counted: they cannot sign in, so they are
	// not one of the admins the lockout guard would leave behind.
	if got.ActiveAdmins != 1 {
		t.Fatalf("active_admins = %d, want 1", got.ActiveAdmins)
	}
}

func TestListPrincipalsStoreFailureIs500(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	s.errList = errors.New("connection refused")
	h := newHandler(t, s)
	w := do(t, h, http.MethodGet, "/v1/admin/principals", "boss@example.com", "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	if strings.Contains(w.Body.String(), "connection refused") {
		t.Fatalf("internal error text reached the caller: %s", w.Body)
	}
}

// ---------------------------------------------------------------------------
// Principals: the lockout guard
// ---------------------------------------------------------------------------

func TestDemotionSucceedsWhenAnotherAdminRemains(t *testing.T) {
	s := newStore(admin("boss@example.com"), admin("second@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/second@example.com", "boss@example.com", `{"role":"member"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	if got := s.principals["second@example.com"].Role; got != RoleMember {
		t.Fatalf("role = %q, want member", got)
	}
	if len(s.changes) != 1 {
		t.Fatalf("recorded %d changes, want 1", len(s.changes))
	}
	c := s.changes[0]
	if c.Actor != "boss@example.com" || c.Target != "second@example.com" ||
		c.FromRole != RoleAdmin || c.ToRole != RoleMember || !c.At.Equal(testNow) {
		t.Fatalf("audit record = %+v", c)
	}
}

func TestRefusesToDemoteTheLastAdmin(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/boss@example.com", "boss@example.com", `{"role":"member"}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", w.Code, w.Body)
	}
	if got := s.principals["boss@example.com"].Role; got != RoleAdmin {
		t.Fatalf("role = %q, want admin", got)
	}
	if s.activeAdmins() != 1 {
		t.Fatalf("active admins = %d, want 1", s.activeAdmins())
	}
	if len(s.changes) != 0 || len(s.saves) != 0 {
		t.Fatalf("a refused change wrote %d audit rows and %d saves", len(s.changes), len(s.saves))
	}
}

func TestRefusesToDisableTheLastAdmin(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/boss@example.com", "boss@example.com", `{"disabled":true}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", w.Code, w.Body)
	}
	if s.principals["boss@example.com"].Disabled() {
		t.Fatal("the last admin was disabled")
	}
}

// TestDisabledAdminDoesNotCountAsASurvivor: a disabled admin cannot sign in, so
// counting them as one of the admins left behind would permit exactly the
// lockout the guard exists to prevent. Self-demotion is the only shape this case
// can take, because any other actor would themselves be a surviving admin.
func TestDisabledAdminDoesNotCountAsASurvivor(t *testing.T) {
	s := newStore(admin("boss@example.com"), disabled(admin("onleave@example.com")))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/boss@example.com", "boss@example.com", `{"role":"member"}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", w.Code, w.Body)
	}
	if s.activeAdmins() != 1 {
		t.Fatalf("active admins = %d, want 1", s.activeAdmins())
	}
}

// TestConcurrentDemotionsLeaveOneAdmin is the reason InTx must serialise. Two
// demotions that each read "one other admin survives" would both be allowed, and
// the roster would end with none.
func TestConcurrentDemotionsLeaveOneAdmin(t *testing.T) {
	s := newStore(admin("boss@example.com"), admin("second@example.com"))
	h := newHandler(t, s)

	codes := make([]int, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	for i, target := range []string{"boss@example.com", "second@example.com"} {
		go func() {
			defer wg.Done()
			w := do(t, h, http.MethodPut, "/v1/admin/principals/"+target, "boss@example.com", `{"role":"member"}`)
			codes[i] = w.Code
		}()
	}
	wg.Wait()

	ok := 0
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			ok++
		case http.StatusConflict, http.StatusNotFound:
		default:
			t.Fatalf("unexpected status %d (codes %v)", c, codes)
		}
	}
	if ok != 1 {
		t.Fatalf("%d requests succeeded, want exactly 1 (codes %v)", ok, codes)
	}
	if s.activeAdmins() < 1 {
		t.Fatal("both demotions landed and the roster has no admins left")
	}
}

func TestDisablingANonAdminIsAllowed(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/dev@example.com", "boss@example.com", `{"disabled":true}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	if !s.principals["dev@example.com"].Disabled() {
		t.Fatal("principal was not disabled")
	}
	if got := s.changes[0]; !got.ToDisabled || got.FromDisabled {
		t.Fatalf("audit record = %+v", got)
	}
}

func TestReEnablingClearsDisabledAt(t *testing.T) {
	s := newStore(admin("boss@example.com"), disabled(member("dev@example.com")))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/dev@example.com", "boss@example.com", `{"disabled":false}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	if s.principals["dev@example.com"].Disabled() {
		t.Fatal("principal is still disabled")
	}
}

// ---------------------------------------------------------------------------
// Principals: auditing
// ---------------------------------------------------------------------------

// TestAuditFailureRollsBackTheChange is the same rule the session read path
// follows: an unaudited grant of visibility over colleagues' transcripts is
// worse than a failed edit.
func TestAuditFailureRollsBackTheChange(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	s.errRecord = errors.New("audit table is read only")
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/dev@example.com", "boss@example.com", `{"role":"admin"}`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: %s", w.Code, w.Body)
	}
	if got := s.principals["dev@example.com"].Role; got != RoleMember {
		t.Fatalf("role = %q, want the change rolled back to member", got)
	}
}

func TestSelfPromotionIsAudited(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/dev@example.com", "boss@example.com", `{"role":"admin"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	got := decode[struct {
		Change *PrincipalChange `json:"change"`
	}](t, w)
	if got.Change == nil {
		t.Fatal("response carried no change record")
	}
	if got.Change.FromRole != RoleMember || got.Change.ToRole != RoleAdmin {
		t.Fatalf("change = %+v", got.Change)
	}
}

func TestNoOpChangeIsNotAudited(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/dev@example.com", "boss@example.com", `{"role":"member"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	if len(s.changes) != 0 {
		t.Fatalf("a no-op wrote %d audit rows", len(s.changes))
	}
	if len(s.saves) != 0 {
		t.Fatalf("a no-op wrote %d rows", len(s.saves))
	}
}

// ---------------------------------------------------------------------------
// Principals: request handling
// ---------------------------------------------------------------------------

func TestPutAddsAnUnknownPrincipal(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/new@example.com", "boss@example.com",
		`{"role":"member","display_name":"New Person"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	p := s.principals["new@example.com"]
	if p.Role != RoleMember || p.AddedBy != "boss@example.com" || !p.AddedAt.Equal(testNow) {
		t.Fatalf("principal = %+v", p)
	}
	if len(s.changes) != 1 || !s.changes[0].Created || s.changes[0].FromRole != "" {
		t.Fatalf("audit record = %+v", s.changes)
	}
}

func TestPutWithoutRoleForUnknownPrincipalIs400(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/new@example.com", "boss@example.com", `{"disabled":true}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body)
	}
	if _, ok := s.principals["new@example.com"]; ok {
		t.Fatal("a principal was created without a role")
	}
}

func TestInvalidRequests(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	h := newHandler(t, s)

	for name, body := range map[string]string{
		"unknown role":   `{"role":"owner"}`,
		"empty patch":    `{}`,
		"unknown field":  `{"rôle":"admin"}`,
		"malformed json": `{`,
	} {
		w := do(t, h, http.MethodPut, "/v1/admin/principals/dev@example.com", "boss@example.com", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400: %s", name, w.Code, w.Body)
		}
	}
}

// TestEmailIsNormalized keeps one person from holding two rows. Two rows means
// two roles, and authorization would consult one while the admin page edits the
// other.
func TestEmailIsNormalized(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	h := newHandler(t, s)

	w := do(t, h, http.MethodPut, "/v1/admin/principals/Dev@Example.com", "BOSS@example.com", `{"role":"admin"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	if len(s.principals) != 2 {
		t.Fatalf("roster grew to %d rows", len(s.principals))
	}
	if got := s.principals["dev@example.com"].Role; got != RoleAdmin {
		t.Fatalf("role = %q, want admin", got)
	}
	if got := s.changes[0].Actor; got != "boss@example.com" {
		t.Fatalf("audit actor = %q", got)
	}
}

// ---------------------------------------------------------------------------
// Access log
// ---------------------------------------------------------------------------

func TestAccessLogFiltersAndPaging(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	s.access = []AccessEvent{
		{ID: 9, Viewer: "boss@example.com", Owner: "dev@example.com", SessionID: "s1", Via: "admin", At: testNow},
		{ID: 8, Viewer: "boss@example.com", Owner: "dev@example.com", SessionID: "s2", Via: "admin", At: testNow},
	}
	h := newHandler(t, s)

	w := do(t, h, http.MethodGet,
		"/v1/admin/access-log?viewer=BOSS@example.com&owner=Dev@example.com&session_id=s1&via=admin"+
			"&from=2026-08-01T00:00:00Z&to=2026-08-05T00:00:00Z&limit=2&before=20", "boss@example.com", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	q := s.lastQuery
	if q.Viewer != "boss@example.com" || q.Owner != "dev@example.com" || q.SessionID != "s1" || q.Via != "admin" {
		t.Fatalf("query = %+v", q)
	}
	if q.Limit != 2 || q.Before != 20 {
		t.Fatalf("paging = limit %d before %d", q.Limit, q.Before)
	}
	if q.From.IsZero() || q.To.IsZero() {
		t.Fatalf("time bounds = %v..%v", q.From, q.To)
	}

	got := decode[struct {
		Entries    []AccessEvent `json:"entries"`
		NextBefore int64         `json:"next_before"`
	}](t, w)
	if len(got.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(got.Entries))
	}
	// The page came back full, so there may be more behind it.
	if got.NextBefore != 8 {
		t.Fatalf("next_before = %d, want 8", got.NextBefore)
	}
}

func TestAccessLogShortPageOffersNoCursor(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	s.access = []AccessEvent{{ID: 3, Viewer: "boss@example.com", Owner: "dev@example.com", At: testNow}}
	h := newHandler(t, s)

	w := do(t, h, http.MethodGet, "/v1/admin/access-log?limit=10", "boss@example.com", "")
	if strings.Contains(w.Body.String(), "next_before") {
		t.Fatalf("short page offered a cursor: %s", w.Body)
	}
}

func TestAccessLogLimitIsClamped(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	h := newHandler(t, s)

	if w := do(t, h, http.MethodGet, "/v1/admin/access-log?limit=99999", "boss@example.com", ""); w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	if s.lastQuery.Limit != defaultMaxPageSize {
		t.Fatalf("limit = %d, want %d", s.lastQuery.Limit, defaultMaxPageSize)
	}
	for _, bad := range []string{"limit=0", "limit=-1", "limit=x", "before=0", "before=abc", "from=yesterday"} {
		w := do(t, h, http.MethodGet, "/v1/admin/access-log?"+bad, "boss@example.com", "")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", bad, w.Code)
		}
	}
}

// ---------------------------------------------------------------------------
// Fleet
// ---------------------------------------------------------------------------

func snapshot(email string, device string, received time.Time, cs ...health.Condition) HealthSnapshot {
	r := health.Report{
		SchemaVersion: health.SchemaVersion,
		Hostname:      device + "-host",
		EmittedAt:     received,
		Conditions:    cs,
	}
	return HealthSnapshot{
		Email:      email,
		DeviceID:   device,
		EmittedAt:  received,
		ReceivedAt: received,
		Worst:      r.Worst(),
		Report:     r,
	}
}

func rowFor(t *testing.T, f Fleet, email string) PrincipalHealth {
	t.Helper()
	for _, r := range f.Principals {
		if r.Email == email {
			return r
		}
	}
	t.Fatalf("no row for %s", email)
	return PrincipalHealth{}
}

// TestCoverageCountsSomeoneWhoNeverReported is the case the whole file exists
// for. A person who enrolled and never reported once produces no series, so
// nothing built on the absence of a series can ever notice them. They have to
// come from the roster.
func TestCoverageCountsSomeoneWhoNeverReported(t *testing.T) {
	ps := []Principal{
		{Email: "live@example.com", Role: RoleMember, AddedAt: testNow.Add(-30 * 24 * time.Hour)},
		{Email: "ghost@example.com", Role: RoleMember, AddedAt: testNow.Add(-30 * 24 * time.Hour)},
	}
	f := Coverage(testNow, FleetInput{
		Principals: ps,
		Health:     []HealthSnapshot{snapshot("live@example.com", "d1", testNow.Add(-time.Minute))},
	}, FleetOptions{})

	if f.Summary.Enrolled != 2 {
		t.Fatalf("enrolled = %d, want 2", f.Summary.Enrolled)
	}
	if f.Summary.NeverReported != 1 || f.Summary.Reporting != 1 {
		t.Fatalf("summary = %+v", f.Summary)
	}
	if f.Summary.CoverageRatio != 0.5 {
		t.Fatalf("coverage = %v, want 0.5", f.Summary.CoverageRatio)
	}

	ghost := rowFor(t, f, "ghost@example.com")
	if ghost.Status != StatusNeverReported {
		t.Fatalf("status = %q", ghost.Status)
	}
	if ghost.Level != health.LevelCritical {
		t.Fatalf("level = %q, want critical past the enrollment grace", ghost.Level)
	}
	if ghost.For != 30*24*time.Hour {
		t.Fatalf("for = %s, want 30 days measured from when they were added", ghost.For)
	}
	// Worst first: the invisible person outranks the healthy one.
	if f.Principals[0].Email != "ghost@example.com" {
		t.Fatalf("rows are not ranked worst first: %+v", f.Principals)
	}
}

func TestNewlyAddedPrincipalIsNotYetAProblem(t *testing.T) {
	f := Coverage(testNow, FleetInput{
		Principals: []Principal{{Email: "new@example.com", Role: RoleMember, AddedAt: testNow.Add(-time.Hour)}},
	}, FleetOptions{})

	row := rowFor(t, f, "new@example.com")
	if row.Status != StatusNeverReported {
		t.Fatalf("status = %q, want never_reported", row.Status)
	}
	if row.Level != health.LevelInfo {
		t.Fatalf("level = %q, want info inside the enrollment grace", row.Level)
	}
	// Still counted as uncovered: the grace changes the urgency, not the fact.
	if f.Summary.NeverReported != 1 || f.Summary.Reporting != 0 {
		t.Fatalf("summary = %+v", f.Summary)
	}
}

func TestSummaryPartitionsTheEnrolledAndExcludesDisabled(t *testing.T) {
	at := testNow.Add(-2 * time.Hour)
	ps := []Principal{
		{Email: "a@example.com", Role: RoleMember, AddedAt: testNow.Add(-time.Hour * 100)},
		{Email: "b@example.com", Role: RoleMember, AddedAt: testNow.Add(-time.Hour * 100)},
		{Email: "c@example.com", Role: RoleMember, AddedAt: testNow.Add(-time.Hour * 100)},
		{Email: "d@example.com", Role: RoleMember, AddedAt: testNow.Add(-time.Hour * 100), DisabledAt: &at},
	}
	f := Coverage(testNow, FleetInput{
		Principals: ps,
		Health: []HealthSnapshot{
			snapshot("a@example.com", "d1", testNow.Add(-time.Minute)),
			snapshot("b@example.com", "d2", testNow.Add(-72*time.Hour)),
			// A disabled person still reporting must not inflate coverage.
			snapshot("d@example.com", "d4", testNow.Add(-time.Minute)),
		},
	}, FleetOptions{})

	s := f.Summary
	if s.Enrolled != 3 || s.Disabled != 1 {
		t.Fatalf("enrolled = %d disabled = %d", s.Enrolled, s.Disabled)
	}
	if s.Reporting+s.Silent+s.NeverReported != s.Enrolled {
		t.Fatalf("reporting+silent+never_reported = %d, want %d: %+v",
			s.Reporting+s.Silent+s.NeverReported, s.Enrolled, s)
	}
	if s.Reporting != 1 || s.Silent != 1 || s.NeverReported != 1 {
		t.Fatalf("summary = %+v", s)
	}
	if got := rowFor(t, f, "d@example.com"); got.Status != StatusDisabled {
		t.Fatalf("disabled row status = %q", got.Status)
	}
}

// TestSilenceIsMeasuredFromReceivedTime keeps a laptop with a skewed clock from
// looking permanently fresh. emitted_at is a value the reporting machine picks.
func TestSilenceIsMeasuredFromReceivedTime(t *testing.T) {
	s := snapshot("skew@example.com", "d1", testNow)
	s.ReceivedAt = testNow.Add(-72 * time.Hour)
	s.EmittedAt = testNow.Add(48 * time.Hour)
	s.Report.EmittedAt = s.EmittedAt

	f := Coverage(testNow, FleetInput{
		Principals: []Principal{{Email: "skew@example.com", Role: RoleMember, AddedAt: testNow.Add(-100 * time.Hour)}},
		Health:     []HealthSnapshot{s},
	}, FleetOptions{})

	row := rowFor(t, f, "skew@example.com")
	if row.Status != StatusSilent {
		t.Fatalf("status = %q, want silent", row.Status)
	}
	if row.For != 72*time.Hour {
		t.Fatalf("for = %s, want 72h", row.For)
	}
}

// TestQuietIsNotBroken is the distinction a laptop fleet needs: absence looks
// the same for a closed lid and a dead agent, so the last report's conditions
// are what separate them.
func TestQuietIsNotBroken(t *testing.T) {
	old := testNow.Add(-48 * time.Hour)
	ps := []Principal{
		{Email: "asleep@example.com", Role: RoleMember, AddedAt: old},
		{Email: "dying@example.com", Role: RoleMember, AddedAt: old},
		{Email: "paused@example.com", Role: RoleMember, AddedAt: old},
	}
	dying := snapshot("dying@example.com", "d2", old, health.Condition{
		Level: health.LevelCritical, Kind: health.KindCaptureBlocked, Detail: "free ratio 0.02",
	})
	pausedSnap := snapshot("paused@example.com", "d3", old, health.Condition{
		Level: health.LevelInfo, Kind: health.KindPaused, Detail: "capture paused by the user",
	})
	pausedSnap.Report.Paused = true

	f := Coverage(testNow, FleetInput{
		Principals: ps,
		Health: []HealthSnapshot{
			snapshot("asleep@example.com", "d1", old),
			dying,
			pausedSnap,
		},
	}, FleetOptions{})

	asleep := rowFor(t, f, "asleep@example.com")
	if asleep.Status != StatusSilent || asleep.Level != health.LevelDegraded {
		t.Fatalf("asleep = %q/%q, want silent/degraded", asleep.Status, asleep.Level)
	}
	broken := rowFor(t, f, "dying@example.com")
	if broken.Status != StatusSilent || broken.Level != health.LevelCritical {
		t.Fatalf("dying = %q/%q, want silent/critical", broken.Status, broken.Level)
	}
	if !strings.Contains(broken.Detail, health.KindCaptureBlocked) {
		t.Fatalf("detail does not name the condition: %q", broken.Detail)
	}
	quiet := rowFor(t, f, "paused@example.com")
	if quiet.Level != health.LevelInfo || !quiet.Paused {
		t.Fatalf("paused = %q paused=%v, want info/true", quiet.Level, quiet.Paused)
	}
	if f.Summary.Paused != 1 {
		t.Fatalf("paused count = %d, want 1", f.Summary.Paused)
	}
}

// TestConditionsAreSurfacedVerbatim: the agent already decided what its counters
// mean, and a second derivation here would disagree with what its owner sees.
func TestConditionsAreSurfacedVerbatim(t *testing.T) {
	cond := health.Condition{
		Level:  health.LevelDegraded,
		Kind:   health.KindNeverDelivered,
		Detail: "no successful delivery in 3h of uptime",
		Since:  testNow.Add(-3 * time.Hour),
	}
	f := Coverage(testNow, FleetInput{
		Principals: []Principal{{Email: "a@example.com", Role: RoleMember, AddedAt: testNow.Add(-100 * time.Hour)}},
		Health:     []HealthSnapshot{snapshot("a@example.com", "d1", testNow.Add(-time.Minute), cond)},
	}, FleetOptions{})

	row := rowFor(t, f, "a@example.com")
	if row.Status != StatusReporting {
		t.Fatalf("status = %q, want reporting", row.Status)
	}
	if row.Level != health.LevelDegraded {
		t.Fatalf("level = %q, want the agent's own worst", row.Level)
	}
	if len(row.Conditions) != 1 || row.Conditions[0] != cond {
		t.Fatalf("conditions = %+v, want the agent's verbatim", row.Conditions)
	}
	if f.Summary.Degraded != 1 {
		t.Fatalf("degraded = %d, want 1", f.Summary.Degraded)
	}
}

// TestEnrolledDeviceThatNeverReported is the same blindness one level down: the
// person is fine because their other laptop works, and the dead one would be
// invisible without the devices table as the denominator.
func TestEnrolledDeviceThatNeverReported(t *testing.T) {
	f := Coverage(testNow, FleetInput{
		Principals: []Principal{{Email: "two@example.com", Role: RoleMember, AddedAt: testNow.Add(-100 * time.Hour)}},
		Devices: []Device{
			{ID: "d1", Email: "two@example.com", Hostname: "work", EnrolledAt: testNow.Add(-100 * time.Hour)},
			{ID: "d2", Email: "two@example.com", Hostname: "spare", EnrolledAt: testNow.Add(-100 * time.Hour)},
		},
		Health: []HealthSnapshot{snapshot("two@example.com", "d1", testNow.Add(-time.Minute))},
	}, FleetOptions{})

	row := rowFor(t, f, "two@example.com")
	if row.Status != StatusReporting {
		t.Fatalf("person status = %q, want reporting", row.Status)
	}
	if len(row.Devices) != 2 {
		t.Fatalf("devices = %d, want 2", len(row.Devices))
	}
	if f.Summary.DevicesNeverReported != 1 {
		t.Fatalf("devices_never_reported = %d, want 1", f.Summary.DevicesNeverReported)
	}
	var spare DeviceHealth
	for _, d := range row.Devices {
		if d.DeviceID == "d2" {
			spare = d
		}
	}
	if spare.Status != StatusNeverReported || spare.Level != health.LevelCritical {
		t.Fatalf("spare device = %q/%q", spare.Status, spare.Level)
	}
}

// TestRevokedDeviceCannotSatisfyCoverage: a retired laptop that is still
// chattering must not make its owner look covered, and it is itself a finding.
func TestRevokedDeviceCannotSatisfyCoverage(t *testing.T) {
	revokedAt := testNow.Add(-10 * time.Hour)
	f := Coverage(testNow, FleetInput{
		Principals: []Principal{{Email: "old@example.com", Role: RoleMember, AddedAt: testNow.Add(-100 * time.Hour)}},
		Devices: []Device{
			{ID: "d1", Email: "old@example.com", EnrolledAt: testNow.Add(-100 * time.Hour), RevokedAt: &revokedAt},
		},
		Health: []HealthSnapshot{snapshot("old@example.com", "d1", testNow.Add(-time.Minute))},
	}, FleetOptions{})

	row := rowFor(t, f, "old@example.com")
	if row.Status != StatusNeverReported {
		t.Fatalf("status = %q: a revoked device satisfied coverage", row.Status)
	}
	if row.Level != health.LevelCritical {
		t.Fatalf("level = %q, want critical", row.Level)
	}
	if !strings.Contains(row.Detail, "revoked") {
		t.Fatalf("detail = %q, want the revoked device named", row.Detail)
	}
}

// TestReportFromUnknownDeviceStillCounts: enrolment rows and health reports come
// from different paths, and a report is proof the machine exists whatever the
// devices table says.
func TestReportFromUnknownDeviceStillCounts(t *testing.T) {
	f := Coverage(testNow, FleetInput{
		Principals: []Principal{{Email: "a@example.com", Role: RoleMember, AddedAt: testNow.Add(-100 * time.Hour)}},
		Health:     []HealthSnapshot{snapshot("a@example.com", "", testNow.Add(-time.Minute))},
	}, FleetOptions{})

	row := rowFor(t, f, "a@example.com")
	if row.Status != StatusReporting {
		t.Fatalf("status = %q, want reporting", row.Status)
	}
	if len(row.Devices) != 1 || !strings.Contains(row.Devices[0].Detail, "no device id") {
		t.Fatalf("device rows = %+v", row.Devices)
	}
}

func TestCoverageKeepsTheNewestSnapshotPerDevice(t *testing.T) {
	f := Coverage(testNow, FleetInput{
		Principals: []Principal{{Email: "a@example.com", Role: RoleMember, AddedAt: testNow.Add(-100 * time.Hour)}},
		Health: []HealthSnapshot{
			snapshot("a@example.com", "d1", testNow.Add(-time.Minute)),
			snapshot("a@example.com", "d1", testNow.Add(-90*time.Hour)),
		},
	}, FleetOptions{})

	row := rowFor(t, f, "a@example.com")
	if row.Status != StatusReporting {
		t.Fatalf("status = %q, want reporting from the newest snapshot", row.Status)
	}
	if len(row.Devices) != 1 {
		t.Fatalf("devices = %d, want 1", len(row.Devices))
	}
}

func TestFleetEndpointHonoursWindowOverrides(t *testing.T) {
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	s.health = []HealthSnapshot{snapshot("dev@example.com", "d1", testNow.Add(-2*time.Hour))}
	h := newHandler(t, s)

	w := do(t, h, http.MethodGet, "/v1/admin/fleet?stale_after=1h", "boss@example.com", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body)
	}
	f := decode[Fleet](t, w)
	if f.StaleAfter != time.Hour {
		t.Fatalf("stale_after = %s, want 1h", f.StaleAfter)
	}
	if got := rowFor(t, f, "dev@example.com"); got.Status != StatusSilent {
		t.Fatalf("status = %q, want silent under a 1h window", got.Status)
	}
	if !f.GeneratedAt.Equal(testNow) {
		t.Fatalf("generated_at = %s", f.GeneratedAt)
	}

	for _, bad := range []string{"stale_after=soon", "stale_after=-1h", "enrollment_grace=0s"} {
		if w := do(t, h, http.MethodGet, "/v1/admin/fleet?"+bad, "boss@example.com", ""); w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", bad, w.Code)
		}
	}
}

func TestFleetStoreFailureIs500(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	s.errHealth = errors.New("statement timeout")
	h := newHandler(t, s)
	if w := do(t, h, http.MethodGet, "/v1/admin/fleet", "boss@example.com", ""); w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}

func TestCoverageOfAnEmptyRoster(t *testing.T) {
	f := Coverage(testNow, FleetInput{}, FleetOptions{})
	if f.Summary.Enrolled != 0 || f.Summary.CoverageRatio != 0 {
		t.Fatalf("summary = %+v", f.Summary)
	}
	if f.StaleAfter != defaultStaleAfter || f.EnrollmentGrace != defaultEnrollmentGrace {
		t.Fatalf("defaults were not applied: %+v", f)
	}
}

func TestNewRejectsMissingDependencies(t *testing.T) {
	if _, err := New(Options{Auth: headerAuth{}}); err == nil {
		t.Fatal("New accepted a nil store")
	}
	if _, err := New(Options{Store: newStore()}); err == nil {
		t.Fatal("New accepted a nil authenticator")
	}
}

// ---------------------------------------------------------------------------
// Skill derive rerun
// ---------------------------------------------------------------------------

// rerun posts as the dashboard would: same-origin unless a test says
// otherwise.
func rerun(t *testing.T, h *Handler, user, body, fetchSite string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(http.MethodPost, "/v1/admin/derive/skill-invocations/rerun", nil)
	} else {
		r = httptest.NewRequest(http.MethodPost, "/v1/admin/derive/skill-invocations/rerun", strings.NewReader(body))
	}
	if user != "" {
		r.Header.Set("X-Test-User", user)
	}
	if fetchSite != "" {
		r.Header.Set("Sec-Fetch-Site", fetchSite)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// A POST the browser did not mark same-origin is refused before anything
// is read or written, whatever cookie it carries: the dashboard cookie on
// a cross-site form is exactly the request this exists to stop.
func TestRerunRefusesACrossSitePOST(t *testing.T) {
	for _, site := range []string{"cross-site", "same-site", "none", ""} {
		s := newStore(admin("boss@example.com"))
		h := newHandler(t, s)
		w := rerun(t, h, "boss@example.com", "", site)
		if w.Code != http.StatusForbidden {
			t.Errorf("Sec-Fetch-Site %q: status = %d, want 403: %s", site, w.Code, w.Body.String())
		}
		if len(s.actions) != 0 || s.resets != 0 || len(s.enqueued) != 0 {
			t.Errorf("Sec-Fetch-Site %q: a refused POST reached the store", site)
		}
	}
	// A member on a cross-site POST still gets the admin routes' 404, not
	// a 403 that confirms the route.
	s := newStore(admin("boss@example.com"), member("dev@example.com"))
	if w := rerun(t, newHandler(t, s), "dev@example.com", "", "cross-site"); w.Code != http.StatusNotFound {
		t.Errorf("member cross-site: status = %d, want 404", w.Code)
	}
}

// No body: the step is reset and the audit row names the actor, the
// action and an empty target.
func TestRerunWithoutSinceResetsTheStepAndAudits(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	h := newHandler(t, s)
	w := rerun(t, h, "boss@example.com", "", "same-origin")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	res := decode[rerunResponse](t, w)
	if !res.StepReset || res.Queued != 0 {
		t.Errorf("response = %+v, want the step reset", res)
	}
	if s.resets != 1 || len(s.enqueued) != 0 {
		t.Errorf("resets %d, enqueued %v", s.resets, s.enqueued)
	}
	if len(s.actions) != 1 {
		t.Fatalf("%d audit rows, want 1", len(s.actions))
	}
	a := s.actions[0]
	if a.Actor != "boss@example.com" || a.Action != "derive.skill_invocations.rerun" || a.Target != "" {
		t.Errorf("audit row = %+v", a)
	}
	if d, _ := a.Detail.(map[string]any); d == nil || d["since"] != nil {
		t.Errorf("audit detail = %v, want since null", a.Detail)
	}
	// An empty JSON object is the same request.
	s = newStore(admin("boss@example.com"))
	if w := rerun(t, newHandler(t, s), "boss@example.com", "{}", "same-origin"); w.Code != http.StatusOK || s.resets != 1 {
		t.Errorf("{} body: status = %d, resets %d", w.Code, s.resets)
	}
}

// With since: the sessions of the window are queued, nothing is reset,
// and the audit row's target is the since value.
func TestRerunWithSinceQueuesTheWindowAndAudits(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	s.queuedCount = 412
	h := newHandler(t, s)
	w := rerun(t, h, "boss@example.com", `{"since":"2026-09-17T02:00:00+05:30"}`, "same-origin")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	res := decode[rerunResponse](t, w)
	if res.StepReset || res.Queued != 412 {
		t.Errorf("response = %+v, want 412 queued and no reset", res)
	}
	want := time.Date(2026, 9, 16, 20, 30, 0, 0, time.UTC)
	if s.resets != 0 || len(s.enqueued) != 1 || !s.enqueued[0].Equal(want) {
		t.Errorf("resets %d, enqueued %v, want [%v]", s.resets, s.enqueued, want)
	}
	if len(s.actions) != 1 || s.actions[0].Target != "2026-09-16T20:30:00Z" || s.actions[0].Action != "derive.skill_invocations.rerun" {
		t.Errorf("audit rows = %+v", s.actions)
	}
	if d, _ := s.actions[0].Detail.(map[string]any); d["since"] != "2026-09-16T20:30:00Z" {
		t.Errorf("audit detail = %v", s.actions[0].Detail)
	}
}

func TestRerunRejectsABadBody(t *testing.T) {
	for _, body := range []string{`{"since":"yesterday"}`, `{"since":""}`, `{"until":"2026-09-17T00:00:00Z"}`, `{"since":`} {
		s := newStore(admin("boss@example.com"))
		w := rerun(t, newHandler(t, s), "boss@example.com", body, "same-origin")
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", body, w.Code)
		}
		if len(s.actions) != 0 || s.resets != 0 || len(s.enqueued) != 0 {
			t.Errorf("%s: a refused body reached the store", body)
		}
	}
}

// The audit row and the change are one transaction: an audit failure
// rolls the reset back, and a demotion landing inside the transaction
// stops the rerun.
func TestRerunAuditFailureAndMidRequestDemotionAreRefused(t *testing.T) {
	s := newStore(admin("boss@example.com"))
	s.errAction = errors.New("audit table gone")
	if w := rerun(t, newHandler(t, s), "boss@example.com", "", "same-origin"); w.Code != http.StatusInternalServerError {
		t.Errorf("audit failure: status = %d, want 500", w.Code)
	}

	s = newStore(admin("boss@example.com"), admin("other@example.com"))
	s.onTx = func(s *fakeStore) {
		p := s.principals["boss@example.com"]
		p.Role = RoleMember
		s.principals["boss@example.com"] = p
	}
	if w := rerun(t, newHandler(t, s), "boss@example.com", "", "same-origin"); w.Code != http.StatusNotFound {
		t.Errorf("demoted mid-request: status = %d, want 404", w.Code)
	}
	if s.resets != 0 || len(s.actions) != 0 {
		t.Error("a demoted caller's rerun ran")
	}
}
