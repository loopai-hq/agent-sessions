package auth

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var (
	cookieNow = time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC)
	keyA      = []byte("0123456789abcdef0123456789abcdef")
	keyB      = []byte("fedcba9876543210fedcba9876543210")
)

func newTestCookies(t *testing.T, tweak func(*CookieOptions)) (*Cookies, *time.Time) {
	t.Helper()
	clock := cookieNow
	o := CookieOptions{Keys: [][]byte{keyA}, Now: func() time.Time { return clock }}
	if tweak != nil {
		tweak(&o)
	}
	c, err := NewCookies(o)
	if err != nil {
		t.Fatal(err)
	}
	return c, &clock
}

// carry moves the cookie a handler set onto the next request, which is the
// browser's half of the exchange.
func carry(t *testing.T, rec *httptest.ResponseRecorder, name string) *http.Request {
	t.Helper()
	r := httptest.NewRequest("GET", "/v1/sessions", nil)
	for _, ck := range (&http.Response{Header: rec.Header()}).Cookies() {
		if ck.Name == name {
			r.AddCookie(ck)
		}
	}
	return r
}

func setCookie(t *testing.T, rec *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, ck := range (&http.Response{Header: rec.Header()}).Cookies() {
		if ck.Name == name {
			return ck
		}
	}
	t.Fatalf("no %s cookie was set", name)
	return nil
}

func TestIssueSetsASafeCookie(t *testing.T) {
	c, _ := newTestCookies(t, nil)
	rec := httptest.NewRecorder()

	if _, err := c.Issue(rec, "Dev@Example.com", RoleMember, cookieNow); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	ck := setCookie(t, rec, DefaultCookieName)

	if !ck.HttpOnly {
		t.Error("cookie is readable by script")
	}
	if !ck.Secure {
		t.Error("cookie would travel over plain HTTP")
	}
	if ck.SameSite != http.SameSiteLaxMode {
		t.Errorf("SameSite = %v, want Lax", ck.SameSite)
	}
	if ck.Path != "/" {
		t.Errorf("Path = %q, want /", ck.Path)
	}
	if ck.Domain != "" {
		t.Errorf("Domain = %q; a __Host- cookie must not carry one", ck.Domain)
	}
	if want := int((12 * time.Hour).Seconds()); ck.MaxAge != want {
		t.Errorf("MaxAge = %d, want %d", ck.MaxAge, want)
	}
	if strings.Contains(ck.Value, "dev@example.com") {
		t.Error("the cookie value should be encoded, not plain text")
	}
}

func TestIssueAndVerifyRoundTripSession(t *testing.T) {
	c, _ := newTestCookies(t, nil)
	rec := httptest.NewRecorder()

	issued, err := c.Issue(rec, "Dev@Example.com", RoleAdmin, cookieNow.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Verify(carry(t, rec, c.Name()))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.Email != "dev@example.com" {
		t.Errorf("Email = %q, want the normalised address", got.Email)
	}
	if got.Role != RoleAdmin {
		t.Errorf("Role = %q", got.Role)
	}
	if got.ID != issued.ID || got.ID == "" {
		t.Errorf("ID = %q, want the issued session id", got.ID)
	}
	if !got.AuthAt.Equal(cookieNow.Add(-time.Hour)) {
		t.Errorf("AuthAt = %v, want the moment of the Google sign-in", got.AuthAt)
	}
	if got.Viewer() != (Viewer{Email: "dev@example.com", Role: RoleAdmin}) {
		t.Errorf("Viewer() = %+v", got.Viewer())
	}
}

func TestIssueRejectsAnIncompleteSession(t *testing.T) {
	c, _ := newTestCookies(t, nil)
	if _, err := c.Issue(httptest.NewRecorder(), "", RoleMember, cookieNow); err == nil {
		t.Error("a session with no email authenticates as nobody")
	}
	if _, err := c.Issue(httptest.NewRecorder(), "dev@example.com", Role("root"), cookieNow); err == nil {
		t.Error("an unrecognised role must not be minted into a cookie")
	}
}

func TestVerifyRejectsATamperedCookie(t *testing.T) {
	c, _ := newTestCookies(t, nil)
	rec := httptest.NewRecorder()
	if _, err := c.Issue(rec, "dev@example.com", RoleMember, cookieNow); err != nil {
		t.Fatal(err)
	}
	original := setCookie(t, rec, c.Name())

	// Promote yourself to admin and re-encode the payload, leaving the MAC.
	parts := strings.Split(original.Value, ".")
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var s Session
	if err := json.Unmarshal(body, &s); err != nil {
		t.Fatal(err)
	}
	s.Role = RoleAdmin
	s.Email = "ceo@example.com"
	edited, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	parts[1] = base64.RawURLEncoding.EncodeToString(edited)

	forged := []string{
		strings.Join(parts, "."),               // edited payload
		original.Value[:len(original.Value)-1], // truncated MAC
		"v2." + parts[1] + "." + parts[2],      // a version we do not speak
		parts[1] + "." + parts[2],              // missing a segment
		"v1.!!!." + parts[2],                   // payload is not base64
		"v1." + parts[1] + ".!!!",              // MAC is not base64
		"",                                     // nothing at all
	}
	for _, v := range forged {
		r := httptest.NewRequest("GET", "/", nil)
		r.AddCookie(&http.Cookie{Name: c.Name(), Value: v})
		if _, err := c.Verify(r); !errors.Is(err, ErrCookieInvalid) {
			t.Errorf("%.30q: err = %v, want ErrCookieInvalid", v, err)
		}
	}
}

func TestVerifyRejectsACookieSignedWithAnotherKey(t *testing.T) {
	mine, _ := newTestCookies(t, nil)
	theirs, _ := newTestCookies(t, func(o *CookieOptions) { o.Keys = [][]byte{keyB} })

	rec := httptest.NewRecorder()
	if _, err := theirs.Issue(rec, "dev@example.com", RoleAdmin, cookieNow); err != nil {
		t.Fatal(err)
	}
	if _, err := mine.Verify(carry(t, rec, mine.Name())); !errors.Is(err, ErrCookieInvalid) {
		t.Fatalf("err = %v, want ErrCookieInvalid", err)
	}
}

func TestKeyRotationDoesNotSignEverybodyOut(t *testing.T) {
	old, _ := newTestCookies(t, nil)
	rec := httptest.NewRecorder()
	if _, err := old.Issue(rec, "dev@example.com", RoleMember, cookieNow); err != nil {
		t.Fatal(err)
	}

	// The new key signs; the old one still verifies for one more lifetime.
	rotated, _ := newTestCookies(t, func(o *CookieOptions) { o.Keys = [][]byte{keyB, keyA} })
	if _, err := rotated.Verify(carry(t, rec, rotated.Name())); err != nil {
		t.Fatalf("a cookie from before the rotation should still verify: %v", err)
	}

	fresh := httptest.NewRecorder()
	if _, err := rotated.Issue(fresh, "dev@example.com", RoleMember, cookieNow); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Verify(carry(t, fresh, old.Name())); !errors.Is(err, ErrCookieInvalid) {
		t.Error("the retired key should not verify a cookie signed with the new one")
	}
}

func TestVerifyRejectsAnExpiredCookie(t *testing.T) {
	c, clock := newTestCookies(t, func(o *CookieOptions) { o.Lifetime = time.Hour })
	rec := httptest.NewRecorder()
	if _, err := c.Issue(rec, "dev@example.com", RoleMember, cookieNow); err != nil {
		t.Fatal(err)
	}
	r := carry(t, rec, c.Name())

	*clock = clock.Add(time.Hour)
	if _, err := c.Verify(r); !errors.Is(err, ErrCookieExpired) {
		t.Fatalf("err = %v, want ErrCookieExpired at the expiry instant", err)
	}
}

func TestNoCookieIsItsOwnAnswer(t *testing.T) {
	c, _ := newTestCookies(t, nil)
	if _, err := c.Verify(httptest.NewRequest("GET", "/", nil)); !errors.Is(err, ErrNoCookie) {
		t.Fatalf("err = %v, want ErrNoCookie", err)
	}
}

func TestRenewRollsAnActiveSessionForward(t *testing.T) {
	c, clock := newTestCookies(t, func(o *CookieOptions) {
		o.Lifetime = 12 * time.Hour
		o.RenewAfter = 6 * time.Hour
	})
	rec := httptest.NewRecorder()
	s, err := c.Issue(rec, "dev@example.com", RoleMember, cookieNow)
	if err != nil {
		t.Fatal(err)
	}

	// Too early: a Set-Cookie on every request is noise.
	if _, renewed := c.Renew(httptest.NewRecorder(), s); renewed {
		t.Error("renewed before the renewal point")
	}

	*clock = clock.Add(7 * time.Hour)
	out := httptest.NewRecorder()
	next, renewed := c.Renew(out, s)
	if !renewed {
		t.Fatal("did not renew past the renewal point")
	}
	if !next.ExpiresAt.After(s.ExpiresAt) {
		t.Errorf("renewal did not extend the session: %v to %v", s.ExpiresAt, next.ExpiresAt)
	}
	if next.ID != s.ID || !next.AuthAt.Equal(s.AuthAt) {
		t.Error("renewal should keep the session identity and the original sign-in time")
	}
	if _, err := c.Verify(carry(t, out, c.Name())); err != nil {
		t.Fatalf("the renewed cookie does not verify: %v", err)
	}
}

func TestRenewalCannotOutrunTheAbsoluteLifetime(t *testing.T) {
	c, clock := newTestCookies(t, func(o *CookieOptions) {
		o.Lifetime = 12 * time.Hour
		o.RenewAfter = time.Hour
		o.AbsoluteLifetime = 24 * time.Hour
	})
	rec := httptest.NewRecorder()
	s, err := c.Issue(rec, "dev@example.com", RoleMember, cookieNow)
	if err != nil {
		t.Fatal(err)
	}

	// Renewing repeatedly must not walk a session past the cap: the point of
	// the cap is that being signed in stops being self-sustaining.
	for range 40 {
		*clock = clock.Add(time.Hour)
		s, _ = c.Renew(httptest.NewRecorder(), s)
	}
	if deadline := cookieNow.Add(24 * time.Hour); s.ExpiresAt.After(deadline) {
		t.Fatalf("ExpiresAt = %v, past the absolute deadline %v", s.ExpiresAt, deadline)
	}

	*clock = cookieNow.Add(25 * time.Hour)
	if _, renewed := c.Renew(httptest.NewRecorder(), s); renewed {
		t.Error("renewed a session that is past its absolute deadline")
	}
}

func TestIssueClampsToTheAbsoluteDeadline(t *testing.T) {
	c, _ := newTestCookies(t, func(o *CookieOptions) {
		o.Lifetime = 12 * time.Hour
		o.AbsoluteLifetime = 24 * time.Hour
	})
	// Signed in to Google 23 hours ago: this cookie gets one hour, not twelve.
	s, err := c.Issue(httptest.NewRecorder(), "dev@example.com", RoleMember, cookieNow.Add(-23*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if want := cookieNow.Add(time.Hour); !s.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", s.ExpiresAt, want)
	}

	if _, err := c.Issue(httptest.NewRecorder(), "dev@example.com", RoleMember, cookieNow.Add(-25*time.Hour)); !errors.Is(err, ErrCookieExpired) {
		t.Error("a sign-in older than the absolute lifetime must not produce a cookie")
	}
}

func TestVerifyEnforcesTheAbsoluteLifetimeOnEveryRead(t *testing.T) {
	c, clock := newTestCookies(t, func(o *CookieOptions) {
		o.Lifetime = 30 * 24 * time.Hour // deliberately longer than the cap
		o.AbsoluteLifetime = 24 * time.Hour
	})
	rec := httptest.NewRecorder()
	if _, err := c.Issue(rec, "dev@example.com", RoleMember, cookieNow); err != nil {
		t.Fatal(err)
	}
	r := carry(t, rec, c.Name())

	*clock = clock.Add(25 * time.Hour)
	if _, err := c.Verify(r); !errors.Is(err, ErrCookieExpired) {
		t.Fatalf("err = %v, want the absolute cap to bite", err)
	}
}

func TestClearSignsOut(t *testing.T) {
	c, _ := newTestCookies(t, nil)
	rec := httptest.NewRecorder()
	c.Clear(rec)

	ck := setCookie(t, rec, c.Name())
	if ck.MaxAge >= 0 {
		t.Errorf("MaxAge = %d, want a negative value so the browser deletes it", ck.MaxAge)
	}
	if ck.Value != "" {
		t.Errorf("Value = %q, want it emptied", ck.Value)
	}
	if !ck.HttpOnly || !ck.Secure {
		t.Error("the clearing cookie should carry the same attributes as the real one")
	}
}

func TestInsecureModeIsOnlyForLocalDevelopment(t *testing.T) {
	c, err := NewCookies(CookieOptions{Keys: [][]byte{keyA}, Insecure: true, Now: func() time.Time { return cookieNow }})
	if err != nil {
		t.Fatal(err)
	}
	if c.Name() == DefaultCookieName {
		// A __Host- cookie without Secure is silently dropped by the browser,
		// which looks like a broken sign-in rather than a missing flag.
		t.Error("insecure mode must not use the __Host- name")
	}
	rec := httptest.NewRecorder()
	if _, err := c.Issue(rec, "dev@example.com", RoleMember, cookieNow); err != nil {
		t.Fatal(err)
	}
	if setCookie(t, rec, c.Name()).Secure {
		t.Error("insecure mode should not claim Secure")
	}

	if _, err := NewCookies(CookieOptions{Keys: [][]byte{keyA}, Insecure: true, Name: "__Host-x"}); err == nil {
		t.Error("a __Host- name without Secure should be refused at construction")
	}
}

func TestNewCookiesRejectsWeakConfiguration(t *testing.T) {
	if _, err := NewCookies(CookieOptions{}); err == nil {
		t.Error("a cookie signer with no key would accept anything")
	}
	if _, err := NewCookies(CookieOptions{Keys: [][]byte{[]byte("short")}}); err == nil {
		t.Error("a five-byte signing key should be refused")
	}
	if _, err := NewCookies(CookieOptions{Keys: [][]byte{keyA, []byte("short")}}); err == nil {
		t.Error("every key has to be strong, not just the first")
	}
}

func TestSigningKeysAreCopied(t *testing.T) {
	key := append([]byte(nil), keyA...)
	c, err := NewCookies(CookieOptions{Keys: [][]byte{key}, Now: func() time.Time { return cookieNow }})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	if _, err := c.Issue(rec, "dev@example.com", RoleMember, cookieNow); err != nil {
		t.Fatal(err)
	}
	r := carry(t, rec, c.Name())

	// A caller that reuses or zeroes its key buffer must not invalidate every
	// live session.
	for i := range key {
		key[i] = 0
	}
	if _, err := c.Verify(r); err != nil {
		t.Fatalf("Verify after the caller wiped its buffer: %v", err)
	}
}

func TestTheMACCoversTheCookieName(t *testing.T) {
	// Two deployments sharing a signing key but naming their cookies
	// differently must not accept each other's values.
	a, err := NewCookies(CookieOptions{Keys: [][]byte{keyA}, Name: "__Host-one", Now: func() time.Time { return cookieNow }})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewCookies(CookieOptions{Keys: [][]byte{keyA}, Name: "__Host-two", Now: func() time.Time { return cookieNow }})
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	if _, err := a.Issue(rec, "dev@example.com", RoleAdmin, cookieNow); err != nil {
		t.Fatal(err)
	}
	value := setCookie(t, rec, a.Name()).Value

	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: b.Name(), Value: value})
	if _, err := b.Verify(r); !errors.Is(err, ErrCookieInvalid) {
		t.Fatalf("err = %v, want ErrCookieInvalid", err)
	}
}

func TestNewKey(t *testing.T) {
	k, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(k) != 32 {
		t.Fatalf("key is %d bytes", len(k))
	}
	if _, err := NewCookies(CookieOptions{Keys: [][]byte{k}}); err != nil {
		t.Fatalf("a generated key should satisfy the constructor: %v", err)
	}
}

func TestVerifyRejectsAValidlySignedButEmptySession(t *testing.T) {
	c, _ := newTestCookies(t, nil)

	// Signed by us and unexpired, but carrying no identity: the shape a cookie
	// from an older format, or from a bug upstream, would have. It must not
	// authenticate a request as nobody with no role.
	for _, s := range []Session{
		{ID: "s", Email: "", Role: RoleMember, IssuedAt: cookieNow, ExpiresAt: cookieNow.Add(time.Hour), AuthAt: cookieNow},
		{ID: "s", Email: "dev@example.com", Role: "", IssuedAt: cookieNow, ExpiresAt: cookieNow.Add(time.Hour), AuthAt: cookieNow},
		{ID: "s", Email: "dev@example.com", Role: "root", IssuedAt: cookieNow, ExpiresAt: cookieNow.Add(time.Hour), AuthAt: cookieNow},
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.AddCookie(&http.Cookie{Name: c.Name(), Value: c.encode(s)})
		if _, err := c.Verify(r); !errors.Is(err, ErrCookieInvalid) {
			t.Errorf("%+v: err = %v, want ErrCookieInvalid", s, err)
		}
	}
}
