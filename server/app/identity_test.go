package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/server/admin"
	"github.com/loopai-hq/loop-sessions/server/api"
	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/store"
	"github.com/loopai-hq/loop-sessions/server/web"
)

// w7Roster is a roster that answers from a map, so the identity adapters can be
// exercised without a database.
type w7Roster struct {
	rows map[string]store.Principal
	err  error
	// calls counts lookups, which is how the tests assert that the roster is
	// re-read per request rather than trusted from the cookie.
	calls int
	// enrolled records every address self-enrolment created, so a test can
	// assert that a disabled or existing row produced no creation at all.
	enrolled []string
}

func (f *w7Roster) PrincipalLookup(_ context.Context, email string) (store.Principal, bool, error) {
	f.calls++
	if f.err != nil {
		return store.Principal{}, false, f.err
	}
	p, ok := f.rows[auth.Normalize(email)]
	return p, ok, nil
}

// EnrolSelf mirrors the store's bound exactly: it may add a member row and it
// may never touch an existing one. A fake that "helpfully" revived a disabled
// row would make the disabled-account tests pass against a store that does the
// wrong thing.
func (f *w7Roster) EnrolSelf(_ context.Context, email string) (store.Principal, bool, error) {
	if f.err != nil {
		return store.Principal{}, false, f.err
	}
	key := auth.Normalize(email)
	if p, ok := f.rows[key]; ok {
		return p, false, nil
	}
	if f.rows == nil {
		f.rows = map[string]store.Principal{}
	}
	p := store.Principal{Email: key, Role: store.RoleMember}
	f.rows[key] = p
	f.enrolled = append(f.enrolled, key)
	return p, true, nil
}

var w7SigningKey = []byte("0123456789abcdef0123456789abcdef")

func w7Cookies(t *testing.T, now func() time.Time) *auth.Cookies {
	t.Helper()
	c, err := auth.NewCookies(auth.CookieOptions{
		Keys:     [][]byte{w7SigningKey},
		Insecure: true,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("new cookies: %v", err)
	}
	return c
}

var w7Epoch = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// w7Request builds a GET carrying whatever cookie value is given, or none.
func w7Request(name, value string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/sessions", nil)
	if value != "" {
		r.AddCookie(&http.Cookie{Name: name, Value: value})
	}
	return r
}

// w7IssueCookie mints a real session cookie and returns its value.
func w7IssueCookie(t *testing.T, c *auth.Cookies, email string, role auth.Role, authAt time.Time) string {
	t.Helper()
	rec := httptest.NewRecorder()
	if _, err := c.Issue(rec, email, role, authAt); err != nil {
		t.Fatalf("issue: %v", err)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("issue set %d cookies, want 1", len(cookies))
	}
	return cookies[0].Value
}

func w7Enabled(email string, role store.Role) store.Principal {
	return store.Principal{Email: email, Role: role, DisplayName: "Dev", AddedAt: w7Epoch}
}

func w7Disabled(email string, role store.Role) store.Principal {
	at := w7Epoch
	p := w7Enabled(email, role)
	p.DisabledAt = &at
	return p
}

// credentialCase is one way a request can fail to carry a usable identity.
type credentialCase struct {
	name   string
	cookie func(t *testing.T, live *auth.Cookies) string
	rows   map[string]store.Principal
}

// w7CredentialFailures enumerates every credential failure the contract names.
// The five have nothing in common mechanically — one never reaches the store,
// one fails a MAC, one fails a clock, one finds no row and one finds a row that
// is switched off — which is exactly why they have to be checked against each
// other rather than each on its own.
func w7CredentialFailures() []credentialCase {
	live := map[string]store.Principal{"dev@example.com": w7Enabled("dev@example.com", store.RoleMember)}
	return []credentialCase{
		{
			name:   "no cookie at all",
			cookie: func(*testing.T, *auth.Cookies) string { return "" },
			rows:   live,
		},
		{
			name: "cookie whose payload was edited",
			cookie: func(t *testing.T, c *auth.Cookies) string {
				v := w7IssueCookie(t, c, "dev@example.com", auth.RoleMember, w7Epoch)
				parts := strings.Split(v, ".")
				// Flip a byte of the payload and keep the original MAC, which is
				// what somebody promoting themselves to admin would try.
				parts[1] = "A" + parts[1][1:]
				return strings.Join(parts, ".")
			},
			rows: live,
		},
		{
			name: "cookie that has expired",
			cookie: func(t *testing.T, c *auth.Cookies) string {
				old := w7Cookies(t, func() time.Time { return w7Epoch.Add(-48 * time.Hour) })
				return w7IssueCookie(t, old, "dev@example.com", auth.RoleMember, w7Epoch.Add(-48*time.Hour))
			},
			rows: live,
		},
		{
			name: "valid cookie for an address with no roster row",
			cookie: func(t *testing.T, c *auth.Cookies) string {
				return w7IssueCookie(t, c, "stranger@example.com", auth.RoleMember, w7Epoch)
			},
			rows: live,
		},
		{
			name: "valid cookie for a principal that was disabled",
			cookie: func(t *testing.T, c *auth.Cookies) string {
				return w7IssueCookie(t, c, "gone@example.com", auth.RoleAdmin, w7Epoch)
			},
			rows: map[string]store.Principal{"gone@example.com": w7Disabled("gone@example.com", store.RoleAdmin)},
		},
	}
}

// Property: a missing, forged, expired, unenrolled or disabled credential all
// fail closed, and every one of them is the same value to the caller. A caller
// that can tell them apart can tell a colleague's account was switched off,
// and a caller that can tell "expired" from "forged" has a probe.
func TestIdentityFailuresAreIndistinguishable(t *testing.T) {
	for _, tc := range w7CredentialFailures() {
		t.Run(tc.name, func(t *testing.T) {
			c := w7Cookies(t, func() time.Time { return w7Epoch })
			roster := &w7Roster{rows: tc.rows}
			r := w7Request(c.Name(), tc.cookie(t, c))

			gotAPI, errAPI := apiAuth{cookies: c, roster: roster}.Authenticate(r)
			if gotAPI != (api.Identity{}) {
				t.Errorf("api identity = %+v, want zero", gotAPI)
			}
			if errAPI != api.ErrNoIdentity { //nolint:errorlint // identity is the assertion: exactly the sentinel, nothing wrapped around it
				t.Errorf("api error = %v (%T), want exactly api.ErrNoIdentity", errAPI, errAPI)
			}

			gotAdmin, errAdmin := adminAuth{cookies: c, roster: roster}.Authenticate(r)
			if gotAdmin != (admin.Identity{}) {
				t.Errorf("admin identity = %+v, want zero", gotAdmin)
			}
			if errAdmin != admin.ErrNoIdentity { //nolint:errorlint // identity is the assertion: exactly the sentinel, nothing wrapped around it
				t.Errorf("admin error = %v (%T), want exactly admin.ErrNoIdentity", errAdmin, errAdmin)
			}

			gotWeb, ok := webViewer{cookies: c, roster: roster}.viewer(r)
			if ok {
				t.Errorf("web viewer resolved to %+v, want not ok", gotWeb)
			}
			if gotWeb != (web.Viewer{}) {
				t.Errorf("web viewer = %+v, want zero", gotWeb)
			}
		})
	}
}

// Property: a disabled principal authenticates as nobody. Stated separately
// from the table above because it is the one failure whose input is a perfectly
// valid credential: the cookie verifies, the roster row exists, and the answer
// still has to be no. A revoked colleague who kept reading until their cookie
// expired is the failure this defends.
func TestDisabledPrincipalAuthenticatesAsNobody(t *testing.T) {
	c := w7Cookies(t, func() time.Time { return w7Epoch })
	roster := &w7Roster{rows: map[string]store.Principal{
		"gone@example.com": w7Disabled("gone@example.com", store.RoleAdmin),
	}}
	// A cookie minted while they were still an enabled admin.
	r := w7Request(c.Name(), w7IssueCookie(t, c, "gone@example.com", auth.RoleAdmin, w7Epoch))

	if _, err := (apiAuth{cookies: c, roster: roster}).Authenticate(r); err != api.ErrNoIdentity { //nolint:errorlint // identity is the assertion: exactly the sentinel
		t.Errorf("api error = %v, want api.ErrNoIdentity", err)
	}
	if _, err := (adminAuth{cookies: c, roster: roster}).Authenticate(r); err != admin.ErrNoIdentity { //nolint:errorlint // identity is the assertion: exactly the sentinel
		t.Errorf("admin error = %v, want admin.ErrNoIdentity", err)
	}
	if v, ok := (webViewer{cookies: c, roster: roster}).viewer(r); ok {
		t.Errorf("web viewer resolved a disabled principal to %+v", v)
	}
	if roster.calls == 0 {
		t.Error("roster was never read; the cookie was trusted on its own")
	}
}

// Property: a roster this server could not read is not an authentication
// failure. Reporting a database outage as "you are not signed in" sends
// everybody to a sign-in page that cannot help, and hides the outage behind it.
func TestRosterOutageIsNotAnAuthenticationFailure(t *testing.T) {
	down := errors.New("dial postgres: connection refused")
	c := w7Cookies(t, func() time.Time { return w7Epoch })
	roster := &w7Roster{err: down}
	r := w7Request(c.Name(), w7IssueCookie(t, c, "dev@example.com", auth.RoleMember, w7Epoch))

	_, err := apiAuth{cookies: c, roster: roster}.Authenticate(r)
	if errors.Is(err, api.ErrNoIdentity) {
		t.Error("api reported a store outage as a missing identity")
	}
	if !errors.Is(err, down) {
		t.Errorf("api error = %v, want it to wrap the store failure", err)
	}

	_, err = adminAuth{cookies: c, roster: roster}.Authenticate(r)
	if errors.Is(err, admin.ErrNoIdentity) {
		t.Error("admin reported a store outage as a missing identity")
	}
	if !errors.Is(err, down) {
		t.Errorf("admin error = %v, want it to wrap the store failure", err)
	}

	// The web port has no error channel, so the only safe answer is "no viewer".
	if v, ok := (webViewer{cookies: c, roster: roster}).viewer(r); ok {
		t.Errorf("web viewer resolved %+v while the roster was unreadable", v)
	}
}

// Property: the role comes from the roster on this request, and only the one
// value that means administrator grants it. A cookie minted for an admin whose
// row now says member must not render an admin page, and a role this server
// cannot interpret must not be read as the privileged one.
func TestWebViewerTakesTheRoleFromTheRosterAndNeverGuesses(t *testing.T) {
	cases := []struct {
		name      string
		cookie    auth.Role
		row       store.Role
		wantAdmin bool
	}{
		{name: "admin row is an admin", cookie: auth.RoleAdmin, row: store.RoleAdmin, wantAdmin: true},
		{name: "member row is not", cookie: auth.RoleMember, row: store.RoleMember, wantAdmin: false},
		{name: "admin cookie, demoted row", cookie: auth.RoleAdmin, row: store.RoleMember, wantAdmin: false},
		{name: "member cookie, promoted row", cookie: auth.RoleMember, row: store.RoleAdmin, wantAdmin: true},
		{name: "role the roster holds that we cannot read", cookie: auth.RoleAdmin, row: store.Role("owner"), wantAdmin: false},
		{name: "empty role", cookie: auth.RoleAdmin, row: store.Role(""), wantAdmin: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := w7Cookies(t, func() time.Time { return w7Epoch })
			roster := &w7Roster{rows: map[string]store.Principal{
				"dev@example.com": w7Enabled("dev@example.com", tc.row),
			}}
			r := w7Request(c.Name(), w7IssueCookie(t, c, "dev@example.com", tc.cookie, w7Epoch))

			v, ok := webViewer{cookies: c, roster: roster}.viewer(r)
			if !ok {
				t.Fatal("viewer did not resolve")
			}
			if v.Admin != tc.wantAdmin {
				t.Errorf("Admin = %v, want %v", v.Admin, tc.wantAdmin)
			}
			if v.Email != "dev@example.com" {
				t.Errorf("Email = %q", v.Email)
			}
			if v.Name != "Dev" {
				t.Errorf("Name = %q, want the roster display name", v.Name)
			}
		})
	}
}

// Property: the address the adapters hand downstream is the normalised one from
// the roster row, because every permission predicate in the store joins on it
// and two spellings of one person are two owners.
func TestIdentityNormalisesTheAddressFromTheRoster(t *testing.T) {
	c := w7Cookies(t, func() time.Time { return w7Epoch })
	roster := &w7Roster{rows: map[string]store.Principal{
		"dev@example.com": {Email: "Dev@Example.com", Role: store.RoleMember},
	}}
	r := w7Request(c.Name(), w7IssueCookie(t, c, "Dev@Example.com", auth.RoleMember, w7Epoch))

	got, err := apiAuth{cookies: c, roster: roster}.Authenticate(r)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if got.Email != "dev@example.com" {
		t.Errorf("api identity email = %q, want the normalised address", got.Email)
	}
	gotAdmin, err := adminAuth{cookies: c, roster: roster}.Authenticate(r)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if gotAdmin.Email != "dev@example.com" {
		t.Errorf("admin identity email = %q, want the normalised address", gotAdmin.Email)
	}
}

// Property: an address counts as ours when its domain is in the allowlist, not
// when it equals one string. The Workspace aliases two domains and half the
// roster, including two of the four admins, sits on the one that is not the
// primary.
func TestEmailDomainIsCheckedAsSetMembership(t *testing.T) {
	domains := []string{"example.com", "example.org"}
	cases := []struct {
		email string
		want  bool
	}{
		{"dev@example.com", true},
		{"dev@example.org", true},
		{"Dev@Example.com", true},
		{"dev@example.net", false},
		{"dev@evil.example", false},
		// The suffix trick a naive HasSuffix check accepts.
		{"dev@not-example.com", false},
		{"dev@example.com.evil.example", false},
		{"example.com", false},
		{"dev@", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := emailInDomains(auth.Normalize(tc.email), domains); got != tc.want {
			t.Errorf("emailInDomains(%q) = %v, want %v", tc.email, got, tc.want)
		}
	}
}

// The constructors are exercised for their contract-pinned shapes so a change to
// one is a failing test rather than a compile error in somebody else's file.
func TestIdentityConstructorsSatisfyTheConsumerPorts(t *testing.T) {
	c := w7Cookies(t, func() time.Time { return w7Epoch })
	var _ = NewAPIAuth(c, nil)
	var _ = NewAdminAuth(c, nil)
	var _ = NewWebViewer(c, nil)
}
