package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/enroll"
	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/store"
	"github.com/loopai-hq/agent-sessions/server/web"
)

// w7Verifier stands in for auth.Verifier. The real one needs a Firebase-signed
// token, and a test that needs a live third party is a test that gets skipped.
type w7Verifier struct {
	id  auth.Identity
	err error
	// saw is the last token handed over, so a test can assert that what was
	// verified is what the browser posted.
	saw string
}

func (v *w7Verifier) Verify(_ context.Context, idToken string) (auth.Identity, error) {
	v.saw = idToken
	if v.err != nil {
		return auth.Identity{}, v.err
	}
	return v.id, nil
}

// The credential strings the tests hunt for in logs and responses. Distinctive
// so a substring search cannot match them by accident.
const (
	w7IDToken = "IDTOKEN-6f0a58d2-must-never-be-logged"
	w7Project = "example-project-12345"
	w7APIKey  = "AIzaSyTEST-public-by-design"
)

// w7Domains is the allowlist the sign-in fixtures sit in.
func w7Domains() []string { return testDomainsOf("dev@example.com", "dev@example.org") }

// w7SignIn builds a SignIn wired to a fake verifier and a fake roster.
func w7SignIn(t *testing.T, v *w7Verifier, roster *w7Roster, log *slog.Logger) *SignIn {
	t.Helper()
	verifier, err := auth.NewVerifier(auth.VerifierOptions{ProjectID: w7Project, Domains: w7Domains()})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	s, err := NewSignIn(SignInOptions{
		Cookies:           w7Cookies(t, func() time.Time { return w7Epoch }),
		Verifier:          verifier,
		Store:             &store.Store{},
		FirebaseAPIKey:    w7APIKey,
		FirebaseProjectID: w7Project,
		Domains:           w7Domains(),
		Insecure:          true,
		Logger:            log,
	})
	if err != nil {
		t.Fatalf("new sign-in: %v", err)
	}
	s.verify = v
	s.roster = roster
	return s
}

func w7Log() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

func w7Identity(email string) auth.Identity {
	at := strings.LastIndex(email, "@")
	return auth.Identity{
		Email:     email,
		Subject:   "1234567890",
		Domain:    email[at+1:],
		Name:      "Dev",
		IssuedAt:  w7Epoch.Add(-time.Minute),
		ExpiresAt: w7Epoch.Add(time.Hour),
	}
}

func w7LiveRoster(email string, role store.Role) *w7Roster {
	return &w7Roster{rows: map[string]store.Principal{
		auth.Normalize(email): w7Enabled(auth.Normalize(email), role),
	}}
}

// w7Page runs GET /auth/signin and returns the rendered response.
func w7Page(t *testing.T, s *SignIn, next string) *http.Response {
	t.Helper()
	target := SignInPath
	if next != "" {
		target += "?next=" + url.QueryEscape(next)
	}
	rec := httptest.NewRecorder()
	s.handleSignIn(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec.Result()
}

// w7hidden pulls the hidden inputs out of the rendered page.
var w7hidden = regexp.MustCompile(`name="([a-z_]+)" value="([^"]*)"`)

// w7Attempt renders the sign-in page and returns the cookie it set together
// with the form that page would post.
//
// The form is read back out of the HTML rather than assembled from the cookie,
// so every test that posts it is also asserting that the page and the cookie
// carry the same token. A page that rendered a token the cookie does not hold
// would refuse every real sign-in while a test built from the cookie passed.
func w7Attempt(t *testing.T, s *SignIn, next string) (*http.Cookie, url.Values) {
	t.Helper()
	res := w7Page(t, s, next)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("sign-in page status = %d, want 200", res.StatusCode)
	}
	cookies := res.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("the sign-in page set %d cookies, want 1", len(cookies))
	}
	form := url.Values{}
	for _, m := range w7hidden.FindAllStringSubmatch(w7Body(t, res), -1) {
		form.Set(m[1], html.UnescapeString(m[2]))
	}
	form.Set("id_token", w7IDToken)
	return cookies[0], form
}

// w7Session posts the form the page would submit.
func w7Session(s *SignIn, ck *http.Cookie, form url.Values) *http.Response {
	r := httptest.NewRequest(http.MethodPost, SessionPath, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if ck != nil {
		r.AddCookie(ck)
	}
	rec := httptest.NewRecorder()
	s.handleSession(rec, r)
	return rec.Result()
}

// w7SignedIn runs a whole successful sign-in and returns the final response.
func w7SignedIn(t *testing.T, s *SignIn, next string) *http.Response {
	t.Helper()
	ck, form := w7Attempt(t, s, next)
	return w7Session(s, ck, form)
}

func w7Body(t *testing.T, res *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

// w7ScriptSources returns the sources a policy permits script from: script-src
// when the policy names one, and default-src otherwise, which is how a browser
// resolves it. A test that grepped for the literal "script-src 'none'" would
// pass on a policy that named neither directive and therefore restricted
// nothing.
func w7ScriptSources(csp string) string {
	var fallback string
	for _, directive := range strings.Split(csp, ";") {
		directive = strings.TrimSpace(directive)
		if v, ok := strings.CutPrefix(directive, "script-src "); ok {
			return strings.TrimSpace(v)
		}
		if v, ok := strings.CutPrefix(directive, "default-src "); ok {
			fallback = strings.TrimSpace(v)
		}
	}
	return fallback
}

// Property: the sign-in page's own origins belong to the sign-in page, and no
// page that prints a transcript may run script at all.
//
// script-src 'none' on the transcript pages is what keeps an escaping mistake
// there from becoming code execution, and the cheapest way to make any scripted
// page work is to relax the policy for everybody. This test is the whole defence
// against that.
//
// It pins three tiers rather than two since 2026-08-13, when the filter pages
// took a same-origin script (combo.js) so their dropdowns could prune as
// somebody types. Those pages carry 'self' and name no foreign origin; the
// transcript pages carry 'none'; the sign-in page is alone in naming Google's.
// A page moving between tiers should have to edit this table to do it.
func TestOnlyTheSignInPageMayRunScript(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")},
		w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	mux := http.NewServeMux()
	s.Register(mux)
	// Mounted the way the composition root mounts it: the dashboard is the
	// catch-all, and web.Secure is the single place its headers are decided, so
	// asserting on what comes out of it is asserting on every HTML page it
	// serves.
	mux.Handle("/", web.Secure(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `<p>a transcript quoting <script>alert(1)</script> back</p>`)
	})))

	cases := []struct {
		name    string
		method  string
		target  string
		sources string
	}{
		{"the sign-in page, the one page that names an outside origin", http.MethodGet, SignInPath,
			"'self' https://www.gstatic.com https://apis.google.com"},

		// Transcript bodies. Nothing here may run script, because everything
		// here is text an agent read back from somewhere else.
		{"a transcript", http.MethodGet, "/sessions/2f4c", "'none'"},
		{"one event of a transcript in full", http.MethodGet, "/sessions/2f4c/events/91", "'none'"},
		{"an artifact a session wrote", http.MethodGet, "/artifacts/9c21", "'none'"},
		{"a shared transcript, which needs no session at all", http.MethodGet, "/shared/abc", "'none'"},
		// /search redirects onto the list, and a redirect body is not a filter
		// page. The header it carries should be the strict one.
		{"search results, which echo the query", http.MethodGet, "/search?q=%3Cscript%3E", "'none'"},
		// The exchange's own refusal page carries no script either: it is the
		// nearest response to the relaxed one and the easiest to widen by
		// accident.
		{"a refused session exchange", http.MethodPost, SessionPath, "'none'"},
		// A POST to a filter page's own address stays strict: these answer with
		// a redirect, and a handler that starts rendering HTML on POST should
		// not inherit the looser header by having picked the right path.
		{"a write to a filter page's address", http.MethodPost, "/admin/principals/x@example.com", "'none'"},

		// Filter pages. Same-origin script, no foreign origins, and the
		// assertion below still refuses to let Google's appear here.
		{"the session list, which runs the combo script", http.MethodGet, "/sessions", "'self'"},
		{"the analytics page", http.MethodGet, "/analytics", "'self'"},
		{"the admin roster", http.MethodGet, "/admin/principals", "'self'"},
		{"the fleet page", http.MethodGet, "/admin/fleet", "'self'"},
		{"the access log", http.MethodGet, "/admin/access", "'self'"},
		{"the notification settings page", http.MethodGet, "/settings/notifications", "'self'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, nil))
			csp := rec.Header().Get("Content-Security-Policy")
			if csp == "" {
				t.Fatal("the response carries no Content-Security-Policy at all")
			}
			if got := w7ScriptSources(csp); got != tc.sources {
				t.Errorf("script sources = %q, want %q\npolicy: %s", got, tc.sources, csp)
			}
			if tc.target == SignInPath {
				return
			}
			// Nothing but the sign-in page may name the origins the sign-in page
			// needs, whichever directive somebody put them in.
			for _, origin := range []string{"gstatic.com", "identitytoolkit.googleapis.com",
				"securetoken.googleapis.com", "firebaseapp.com",
				// Added with the popup flow's own requirements. A page that
				// renders a transcript has no business naming an origin that
				// exists to host somebody else's sign-in iframe.
				"apis.google.com", "accounts.google.com"} {
				if strings.Contains(csp, origin) {
					t.Errorf("policy names %s outside the sign-in page: %s", origin, csp)
				}
			}
		})
	}
}

// Property: the sign-in page's policy names exactly what the SDK needs, and the
// script it loads asks for nothing the policy does not allow. The two are
// written in different files and in different languages, so nothing but a test
// keeps them honest; a mismatch presents as a sign-in page that silently does
// nothing.
func TestTheSignInPolicyAndTheScriptAgreeOnWhereCodeComesFrom(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	csp := w7Page(t, s, "").Header.Get("Content-Security-Policy")

	for _, want := range []string{
		"script-src 'self' https://www.gstatic.com",
		"connect-src https://identitytoolkit.googleapis.com https://securetoken.googleapis.com",
		"frame-src https://" + w7Project + ".firebaseapp.com",
		"form-action 'self'",
		"frame-ancestors 'none'",
		"base-uri 'none'",
	} {
		if !strings.Contains(csp, want) {
			t.Errorf("policy is missing %q\npolicy: %s", want, csp)
		}
	}

	// Every module the script imports has to come from an origin the policy
	// allows. A CDN added here would be code that handles a live credential
	// arriving from somebody else's release process, and the policy would refuse
	// to load it.
	imports := regexp.MustCompile(`from\s*"(https?://[^/"]+)`).FindAllStringSubmatch(web.SignInScript(), -1)
	if len(imports) == 0 {
		t.Fatal("the sign-in script imports nothing; it cannot be loading the Firebase SDK")
	}
	for _, m := range imports {
		if m[1] != "https://www.gstatic.com" {
			t.Errorf("the script imports from %s, which the policy does not allow", m[1])
		}
	}
}

// Property: signInWithPopup completes by posting a message to the window that
// opened it, and a strict Cross-Origin-Opener-Policy severs that link — the
// popup succeeds, the opener never hears, and the page waits forever with no
// error. The dashboard sets the strict value; this page must not.
func TestTheSignInPageDoesNotSeverThePopupItOpens(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	if got := w7Page(t, s, "").Header.Get("Cross-Origin-Opener-Policy"); got != "same-origin-allow-popups" {
		t.Errorf("Cross-Origin-Opener-Policy = %q, want same-origin-allow-popups", got)
	}
}

// Property: the page carries the public Firebase web config the SDK needs, and
// carries no trace of the OAuth client this rehaul deleted. The second half
// matters because a leftover authorization URL still compiles and still looks
// like a working sign-in.
func TestTheSignInPageCarriesTheFirebaseConfigAndNoOAuthClient(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	res := w7Page(t, s, "")
	body := w7Body(t, res)

	for _, want := range []string{w7APIKey, w7Project, w7Project + ".firebaseapp.com", SignInScriptPath, SessionPath} {
		if !strings.Contains(body, want) {
			t.Errorf("the page does not carry %q", want)
		}
	}
	for _, gone := range []string{"accounts.google.com", "oauth2.googleapis.com", "client_secret", "response_type"} {
		if strings.Contains(body, gone) {
			t.Errorf("the page still carries %q from the OAuth flow", gone)
		}
	}
	if got := res.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("Content-Type = %q", got)
	}
}

// w7NextValue pulls the rendered destination back out of the page.
var w7NextValue = regexp.MustCompile(`name="next" value="([^"]*)"`)

// Property: the one caller-supplied value the sign-in page renders cannot become
// markup. This is the only page in the service that may run script, so a `next`
// that closed the attribute it sits in would be the one place an injected tag
// would actually execute.
func TestTheOneCallerSuppliedValueOnTheSignInPageCannotBecomeMarkup(t *testing.T) {
	hostile := []string{
		`/sessions"><script>alert(1)</script>`,
		`/sessions" onfocus="alert(1)`,
		`/sessions'><img src=x onerror=alert(1)>`,
		`javascript:alert(1)`,
		`//evil.example/`,
	}
	for _, next := range hostile {
		t.Run(next, func(t *testing.T) {
			s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
			body := w7Body(t, w7Page(t, s, next))

			// The page carries exactly the one script tag it is entitled to, its
			// own module. Anything that escaped the attribute would be a second.
			if got := strings.Count(body, "<script"); got != 1 {
				t.Errorf("the page carries %d script tags, want 1:\n%s", got, body)
			}
			if strings.Contains(body, "<img") {
				t.Errorf("`next` opened a tag:\n%s", body)
			}
			// Escaped rather than dropped: the value still round-trips to exactly
			// what localPath reduced it to, so a legitimate destination carrying a
			// quote is not silently mangled on the way to the redirect.
			m := w7NextValue.FindStringSubmatch(body)
			if m == nil {
				t.Fatalf("the page renders no next field:\n%s", body)
			}
			if got := html.UnescapeString(m[1]); got != localPath(next) {
				t.Errorf("next rendered as %q, want %q", got, localPath(next))
			}
		})
	}
}

// Property: `next` is somewhere on this site or it is the root.
//
// An absolute URL accepted here is an open redirect on the sign-in path of an
// internal tool, which is the ideal phishing primitive: the link arrives on the
// domain colleagues have been told to trust and lands them on the attacker's
// page already in the habit of signing in.
func TestNextIsRejectedUnlessItIsALocalPath(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", "/"},
		{"/", "/"},
		{"/sessions", "/sessions"},
		{"/sessions/abc?highlight=3#seq-9", "/sessions/abc?highlight=3#seq-9"},

		// Absolute, in every spelling.
		{"https://evil.example/", "/"},
		{"http://evil.example/", "/"},
		{"//evil.example/", "/"},
		{"//evil.example", "/"},
		// Browsers normalise a backslash to a slash, so this is protocol-relative
		// to anything reading it as a URL even though it is not to a prefix check.
		{`/\evil.example`, "/"},
		{`/\/evil.example`, "/"},
		{"javascript:alert(1)", "/"},
		{"data:text/html,<script>", "/"},
		{"HTTPS://evil.example", "/"},
		// url.Parse rejects control characters outright, so anything smuggling
		// one falls back rather than being passed through half-parsed.
		{"/\thttps://evil.example", "/"},

		// Not a path at all.
		{"sessions", "/"},
		{"../admin", "/"},

		// Header injection.
		{"/ok\r\nSet-Cookie: a=b", "/"},
		{"/ok\nLocation: https://evil.example", "/"},
		{"/ok\x00", "/"},
	}
	for _, tc := range cases {
		if got := localPath(tc.in); got != tc.want {
			t.Errorf("localPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// Property: whatever destination is asked for, the browser is sent somewhere on
// this site. Checked end to end because localPath being right is not the same as
// the handler calling it — and `next` now arrives in a form field, which a
// caller can set to anything without ever loading the page that renders it.
func TestSignInNeverRedirectsOffSite(t *testing.T) {
	hostile := []string{
		"https://evil.example/",
		"//evil.example/",
		`/\evil.example`,
		"javascript:alert(1)",
	}
	for _, next := range hostile {
		t.Run(next, func(t *testing.T) {
			s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")},
				w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

			ck, form := w7Attempt(t, s, next)
			// Also set it directly on the post: the value has been out of this
			// server's hands, so the check that counts is the one immediately
			// before the Location header is written.
			form.Set("next", next)

			res := w7Session(s, ck, form)
			if res.StatusCode != http.StatusSeeOther {
				t.Fatalf("status = %d, want 303; body %s", res.StatusCode, w7Body(t, res))
			}
			// The property is that the browser stays on this site, not that it
			// lands on any particular page: a hostile value may reduce to the root
			// or to a harmless path, and either is fine. What must never appear is
			// a Location a browser resolves against another host.
			got := res.Header.Get("Location")
			u, err := url.Parse(got)
			if err != nil || u.Scheme != "" || u.Host != "" || u.Opaque != "" {
				t.Fatalf("Location = %q, want a path on this site", got)
			}
			if strings.HasPrefix(got, "//") || strings.HasPrefix(got, `/\`) {
				t.Errorf("Location = %q resolves against another host", got)
			}
		})
	}
}

// Property: a legitimate destination survives the round trip, which is the
// reason `next` exists at all — a link to a transcript should still open the
// transcript after signing in.
func TestNextSurvivesTheRoundTrip(t *testing.T) {
	const want = "/sessions/2f4c?highlight=88"
	s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")},
		w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	res := w7SignedIn(t, s, want)
	if got := res.Header.Get("Location"); got != want {
		t.Errorf("Location = %q, want %q", got, want)
	}
}

// Property: the exchange only completes when it carries the token this server
// minted into this browser's page. Without the comparison anybody can make a
// colleague's browser post an ID token for an account of the attacker's
// choosing, leaving that browser holding a session the attacker is on the other
// end of — a session fixation dressed as a sign-in.
func TestTheSessionExchangeRequiresTheTokenItMinted(t *testing.T) {
	cases := []struct {
		name   string
		mangle func(ck *http.Cookie, form url.Values) *http.Cookie
		wantOK bool
	}{
		{
			name:   "the pair this server issued",
			mangle: func(ck *http.Cookie, _ url.Values) *http.Cookie { return ck },
			wantOK: true,
		},
		{
			name: "no cookie, as when the attacker rendered the page",
			mangle: func(_ *http.Cookie, _ url.Values) *http.Cookie {
				return nil
			},
		},
		{
			name: "no token in the form",
			mangle: func(ck *http.Cookie, form url.Values) *http.Cookie {
				form.Del("csrf")
				return ck
			},
		},
		{
			name: "a token from a different sign-in",
			mangle: func(ck *http.Cookie, form url.Values) *http.Cookie {
				form.Set("csrf", "a-token-this-browser-never-received")
				return ck
			},
		},
		{
			name: "a cookie edited to match an attacker-chosen token",
			mangle: func(ck *http.Cookie, form url.Values) *http.Cookie {
				form.Set("csrf", "chosen")
				ck.Value = "not-chosen"
				return ck
			},
		},
		{
			name: "empty on both sides",
			mangle: func(ck *http.Cookie, form url.Values) *http.Cookie {
				form.Set("csrf", "")
				ck.Value = ""
				return ck
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := &w7Verifier{id: w7Identity("dev@example.com")}
			s := w7SignIn(t, v, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

			ck, form := w7Attempt(t, s, "")
			ck = tc.mangle(ck, form)

			res := w7Session(s, ck, form)
			if tc.wantOK {
				if res.StatusCode != http.StatusSeeOther {
					t.Fatalf("status = %d, want 303; body %s", res.StatusCode, w7Body(t, res))
				}
				return
			}
			if res.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", res.StatusCode)
			}
			if v.saw != "" {
				t.Error("the id token was verified before the csrf token was checked")
			}
			if len(res.Cookies()) == 0 {
				t.Error("no Set-Cookie on the refusal; the csrf cookie was left behind")
			}
		})
	}
}

// Property: a minted token is good for exactly one exchange. A token that
// survives its first use is a token that can be replayed.
func TestTheCSRFTokenIsSingleUse(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")},
		w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	ck, form := w7Attempt(t, s, "")
	first := w7Session(s, ck, form)
	if first.StatusCode != http.StatusSeeOther {
		t.Fatalf("first exchange status = %d, want 303", first.StatusCode)
	}
	var cleared bool
	for _, c := range first.Cookies() {
		if c.Name == s.csrfName() && c.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("the exchange did not expire the csrf cookie")
	}
	// The browser obeys the clearing Set-Cookie, so the replay carries none.
	if second := w7Session(s, nil, form); second.StatusCode != http.StatusForbidden {
		t.Errorf("replay status = %d, want 403", second.StatusCode)
	}
}

// Property: a post from another origin is refused even if it somehow carries a
// matching pair. The Origin header and the token pair cover different failures,
// and this one still holds if a token ever leaks.
func TestTheSessionExchangeRefusesAPostFromAnotherOrigin(t *testing.T) {
	cases := map[string]struct {
		origin string
		wantOK bool
	}{
		"no Origin at all, as a form post may send": {origin: "", wantOK: true},
		"our own origin":               {origin: "http://example.com", wantOK: true},
		"somebody else's page":         {origin: "https://evil.example", wantOK: false},
		"our host under a hostile one": {origin: "https://example.com.evil.example", wantOK: false},
		"an Origin that is not a URL":  {origin: "://", wantOK: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")},
				w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
			ck, form := w7Attempt(t, s, "")

			r := httptest.NewRequest(http.MethodPost, SessionPath, strings.NewReader(form.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			r.AddCookie(ck)
			rec := httptest.NewRecorder()
			s.handleSession(rec, r)

			want := http.StatusForbidden
			if tc.wantOK {
				want = http.StatusSeeOther
			}
			if rec.Code != want {
				t.Errorf("status = %d, want %d", rec.Code, want)
			}
		})
	}
}

// w7RefusalCases enumerates every way the exchange can fail to establish an
// identity. They are mechanically unrelated — a CSRF check, a missing token, a
// bad signature, an account that signed in with a password, a wrong domain —
// which is why the responses have to be compared with each other.
func w7RefusalCases() []struct {
	name  string
	setup func(v *w7Verifier, form url.Values, ck *http.Cookie) *http.Cookie
} {
	return []struct {
		name  string
		setup func(v *w7Verifier, form url.Values, ck *http.Cookie) *http.Cookie
	}{
		{
			name: "the csrf token did not match",
			setup: func(_ *w7Verifier, form url.Values, ck *http.Cookie) *http.Cookie {
				form.Set("csrf", "wrong")
				return ck
			},
		},
		{
			name: "the post carried no id token",
			setup: func(_ *w7Verifier, form url.Values, ck *http.Cookie) *http.Cookie {
				form.Del("id_token")
				return ck
			},
		},
		{
			name: "the id token did not verify",
			setup: func(v *w7Verifier, _ url.Values, ck *http.Cookie) *http.Cookie {
				v.err = auth.ErrTokenSignature
				return ck
			},
		},
		{
			name: "the id token had expired",
			setup: func(v *w7Verifier, _ url.Values, ck *http.Cookie) *http.Cookie {
				v.err = auth.ErrTokenExpired
				return ck
			},
		},
		{
			name: "the id token was minted for another Firebase project",
			setup: func(v *w7Verifier, _ url.Values, ck *http.Cookie) *http.Cookie {
				v.err = auth.ErrTokenAudience
				return ck
			},
		},
		{
			name: "the token came from another project's issuer",
			setup: func(v *w7Verifier, _ url.Values, ck *http.Cookie) *http.Cookie {
				v.err = auth.ErrTokenIssuer
				return ck
			},
		},
		{
			name: "the account signed in with something other than Google",
			setup: func(v *w7Verifier, _ url.Values, ck *http.Cookie) *http.Cookie {
				v.err = auth.ErrSignInProvider
				return ck
			},
		},
		{
			name: "the address was never verified",
			setup: func(v *w7Verifier, _ url.Values, ck *http.Cookie) *http.Cookie {
				v.err = auth.ErrEmailUnverified
				return ck
			},
		},
		{
			name: "the account is outside the allowed domains",
			setup: func(v *w7Verifier, _ url.Values, ck *http.Cookie) *http.Cookie {
				v.err = auth.ErrDomainNotAllowed
				return ck
			},
		},
		{
			name: "the verifier allowed a domain this handler does not",
			setup: func(v *w7Verifier, _ url.Values, ck *http.Cookie) *http.Cookie {
				v.id = auth.Identity{Email: "dev@evil.example", Domain: "evil.example"}
				return ck
			},
		},
	}
}

// Property: every authentication failure is one response. auth/firebase.go keeps
// the causes apart so this server can record which check fired; telling the
// caller which one fired helps nobody but the caller who is guessing.
func TestSignInRefusalsAreIndistinguishable(t *testing.T) {
	type answer struct {
		status int
		body   string
		ctype  string
	}
	var seen []answer

	for _, tc := range w7RefusalCases() {
		t.Run(tc.name, func(t *testing.T) {
			v := &w7Verifier{id: w7Identity("dev@example.com")}
			s := w7SignIn(t, v, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

			ck, form := w7Attempt(t, s, "")
			ck = tc.setup(v, form, ck)

			res := w7Session(s, ck, form)
			seen = append(seen, answer{res.StatusCode, w7Body(t, res), res.Header.Get("Content-Type")})
		})
	}

	if len(seen) < 2 {
		t.Fatal("nothing to compare")
	}
	for i, got := range seen {
		if got != seen[0] {
			t.Errorf("refusal %d differs from the first:\n got %+v\nwant %+v", i, got, seen[0])
		}
	}
	if seen[0].status != http.StatusForbidden {
		t.Errorf("refusal status = %d, want 403", seen[0].status)
	}
	if !strings.Contains(seen[0].body, signInRefusedMessage) {
		t.Errorf("refusal body = %q", seen[0].body)
	}
}

// Property: "Google verified them but the roster did not" is one response too,
// and specifically it does not say whether the person was never enrolled, was
// switched off, or holds a role this server cannot read. It is allowed to differ
// from a verification failure because by then the caller has proved the account
// is theirs, and somebody genuinely not on the roster has to be told rather than
// sent round the sign-in loop forever.
func TestRosterRefusalsAreIndistinguishableFromEachOther(t *testing.T) {
	// "never enrolled" is deliberately absent: an address from a company domain
	// with no row is now created as a member and signed in, so it is no longer a
	// refusal at all. What remains must still be indistinguishable, because
	// telling somebody they were switched off rather than holding an unreadable
	// role says something about a decision an admin made about them.
	rosters := map[string]*w7Roster{
		"switched off": {rows: map[string]store.Principal{
			"dev@example.com": w7Disabled("dev@example.com", store.RoleMember),
		}},
		"disabled admin": {rows: map[string]store.Principal{
			"dev@example.com": w7Disabled("dev@example.com", store.RoleAdmin),
		}},
		"role we cannot read": {rows: map[string]store.Principal{
			"dev@example.com": w7Enabled("dev@example.com", store.Role("owner")),
		}},
	}
	var first string
	for name, roster := range rosters {
		t.Run(name, func(t *testing.T) {
			s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")}, roster, slog.New(slog.DiscardHandler))
			res := w7SignedIn(t, s, "")

			if res.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", res.StatusCode)
			}
			body := w7Body(t, res)
			if !strings.Contains(body, signInNotEnrolledMessage) {
				t.Fatalf("body = %q, want the not-enrolled page", body)
			}
			for _, c := range res.Cookies() {
				if c.Name == s.cookies.Name() && c.Value != "" {
					t.Error("a session cookie was issued to somebody with no roster row")
				}
			}
			if first == "" {
				first = body
			} else if body != first {
				t.Errorf("roster refusals differ:\n got %q\nwant %q", body, first)
			}
		})
	}
}

// Property: something broken on our side is not reported as a refusal. A person
// told their account is not authorised will go and ask an admin to fix a roster
// that is already correct, while the database stays down.
func TestSignInAnswersUnavailableWhenSomethingOfOursIsDown(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")},
		&w7Roster{err: errors.New("dial postgres: connection refused")}, slog.New(slog.DiscardHandler))

	res := w7SignedIn(t, s, "")
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.StatusCode)
	}
	if body := w7Body(t, res); !strings.Contains(body, signInUnavailableMessage) {
		t.Errorf("body = %q", body)
	}
}

// Property: no ID token, CSRF token or session cookie value reaches a log or a
// response body. The log is a second, less guarded copy of whatever is written
// to it, and a credential in it is a credential in every downstream log sink.
//
// What this can defend is this handler's own formatting, which is the only place
// the token is in scope. The refusal path logs the verifier's error verbatim, and
// that is safe because auth's sentinels name the check rather than the credential
// — a guarantee that lives in server/auth/firebase.go and is tested there.
func TestSignInNeverLogsACredential(t *testing.T) {
	run := func(t *testing.T, prepare func(v *w7Verifier, roster *w7Roster)) (string, []string) {
		t.Helper()
		log, buf := w7Log()
		v := &w7Verifier{id: w7Identity("dev@example.com")}
		roster := w7LiveRoster("dev@example.com", store.RoleMember)
		prepare(v, roster)
		s := w7SignIn(t, v, roster, log)

		ck, form := w7Attempt(t, s, "")
		res := w7Session(s, ck, form)

		secrets := []string{w7IDToken, ck.Value}
		// The session cookie is a bearer credential for the whole dashboard, so
		// it is checked too, using whatever value this run happened to mint
		// rather than a fixture.
		for _, c := range res.Cookies() {
			if c.Name == s.cookies.Name() && c.Value != "" {
				secrets = append(secrets, c.Value)
			}
		}
		return buf.String() + "\n" + w7Body(t, res), secrets
	}

	cases := map[string]func(v *w7Verifier, roster *w7Roster){
		"successful sign-in": func(*w7Verifier, *w7Roster) {},
		"the token did not verify": func(v *w7Verifier, _ *w7Roster) {
			v.err = auth.ErrTokenSignature
		},
		"the account signed in with a password": func(v *w7Verifier, _ *w7Roster) {
			v.err = auth.ErrSignInProvider
		},
		"the roster is down": func(_ *w7Verifier, roster *w7Roster) {
			roster.err = errors.New("dial postgres: connection refused")
		},
	}
	for name, prepare := range cases {
		t.Run(name, func(t *testing.T) {
			out, secrets := run(t, prepare)
			for _, secret := range secrets {
				if strings.Contains(out, secret) {
					t.Errorf("a credential reached the log or the response:\n%s", out)
				}
			}
		})
	}
}

// Property: the session is issued against the roster row, not against anything
// the caller supplied, and it is dated from when the token was minted so the
// absolute cap in auth.Cookies is a bound rather than a suggestion.
func TestSignInIssuesTheSessionFromTheRosterRow(t *testing.T) {
	id := w7Identity("Dev@Example.com")
	id.IssuedAt = w7Epoch.Add(-30 * time.Minute)
	v := &w7Verifier{id: id}
	roster := &w7Roster{rows: map[string]store.Principal{
		"dev@example.com": w7Enabled("dev@example.com", store.RoleAdmin),
	}}
	s := w7SignIn(t, v, roster, slog.New(slog.DiscardHandler))

	res := w7SignedIn(t, s, "")
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d; body %s", res.StatusCode, w7Body(t, res))
	}
	if v.saw != w7IDToken {
		t.Errorf("verified %q, want the token the page posted", v.saw)
	}

	var value string
	for _, c := range res.Cookies() {
		if c.Name == s.cookies.Name() && c.Value != "" {
			value = c.Value
		}
	}
	if value == "" {
		t.Fatal("no session cookie was issued")
	}
	sess, err := s.cookies.Verify(w7Request(s.cookies.Name(), value))
	if err != nil {
		t.Fatalf("the issued cookie does not verify: %v", err)
	}
	if sess.Email != "dev@example.com" {
		t.Errorf("session email = %q, want the normalised roster address", sess.Email)
	}
	if sess.Role != auth.RoleAdmin {
		t.Errorf("session role = %q, want the role from the roster", sess.Role)
	}
	if !sess.AuthAt.Equal(id.IssuedAt) {
		t.Errorf("AuthAt = %s, want the moment the token was minted (%s)", sess.AuthAt, id.IssuedAt)
	}
}

// Property: neither the page that mints a CSRF token nor the exchange that mints
// a session is cacheable, and neither hands its URL onward. A cached sign-in page
// is a cached token, and one token in two browsers is no token at all.
func TestTheSignInSurfaceIsNeverCached(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")},
		w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	ck, form := w7Attempt(t, s, "")
	page := w7Page(t, s, "")
	exchange := w7Session(s, ck, form)

	for name, res := range map[string]*http.Response{"the page": page, "the exchange": exchange} {
		if got := res.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("%s: Cache-Control = %q, want no-store", name, got)
		}
		if got := res.Header.Get("Referrer-Policy"); got != "same-origin" {
			t.Errorf("%s: Referrer-Policy = %q, want same-origin", name, got)
		}
	}
}

// Property: a body far larger than any ID token is refused rather than read into
// memory, and it is refused as an ordinary sign-in failure so it tells a caller
// nothing the other refusals do not.
func TestAnOversizedSessionBodyIsRefused(t *testing.T) {
	v := &w7Verifier{id: w7Identity("dev@example.com")}
	s := w7SignIn(t, v, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	ck, form := w7Attempt(t, s, "")
	form.Set("id_token", strings.Repeat("A", maxSessionBodyBytes+1))

	res := w7Session(s, ck, form)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", res.StatusCode)
	}
	if !strings.Contains(w7Body(t, res), signInRefusedMessage) {
		t.Error("an oversized body answered differently from every other refusal")
	}
	if v.saw != "" {
		t.Error("an oversized body reached the verifier")
	}
}

// Property: the script is served as something a browser will execute. It is
// served from here rather than from the dashboard's /static/ handler, which has
// no Content-Type for .js and sends nosniff alongside — a combination that
// refuses the file and reads as a broken script rather than as a missing case.
func TestTheSignInScriptIsServedAsExecutableJavaScript(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	rec := httptest.NewRecorder()
	s.handleScript(rec, httptest.NewRequest(http.MethodGet, SignInScriptPath, nil))
	res := rec.Result()

	if got := res.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/javascript") {
		t.Errorf("Content-Type = %q, want text/javascript", got)
	}
	if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", got)
	}
	if body := w7Body(t, res); body != web.SignInScript() || body == "" {
		t.Error("the route did not serve the embedded module")
	}
}

// Property: the four routes this surface owns are mounted where the rest of the
// service expects them, and the two that change state are POST only. A GET
// sign-out can be triggered by any page that can make this browser fetch an
// image; a GET session exchange would put an ID token in a URL.
func TestTheSignInRoutesAreMountedAndTheStateChangingOnesArePostOnly(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	s.Register(mux)

	for _, tc := range []struct {
		method string
		target string
		want   int
	}{
		{http.MethodGet, SignInPath, http.StatusOK},
		{http.MethodGet, SignInScriptPath, http.StatusOK},
		{http.MethodPost, SignOutPath, http.StatusSeeOther},
		{http.MethodGet, SignOutPath, http.StatusMethodNotAllowed},
		{http.MethodGet, SessionPath, http.StatusMethodNotAllowed},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.target, nil))
		if rec.Code != tc.want {
			t.Errorf("%s %s = %d, want %d", tc.method, tc.target, rec.Code, tc.want)
		}
	}
}

// Property: signing out clears the cookie for real, with a negative MaxAge,
// which is what tells a browser to delete it now rather than keep it for the
// rest of the browsing session.
func TestSignOutClearsTheSessionCookie(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	rec := httptest.NewRecorder()
	s.handleSignOut(rec, httptest.NewRequest(http.MethodPost, SignOutPath, nil))
	res := rec.Result()

	if res.StatusCode != http.StatusSeeOther {
		t.Errorf("status = %d, want 303", res.StatusCode)
	}
	var cleared bool
	for _, c := range res.Cookies() {
		if c.Name == s.cookies.Name() {
			cleared = c.Value == "" && c.MaxAge < 0
		}
	}
	if !cleared {
		t.Errorf("session cookie was not deleted: %v", res.Cookies())
	}
}

// Property: the constructor names every missing requirement at once and refuses
// a configuration whose failure mode would be silent. A server that starts with
// no Firebase configuration and discovers it on the first sign-in is an outage
// found by a user.
func TestNewSignInRefusesAnIncompleteConfiguration(t *testing.T) {
	full := func() SignInOptions {
		ver, err := auth.NewVerifier(auth.VerifierOptions{ProjectID: w7Project, Domains: w7Domains()})
		if err != nil {
			t.Fatalf("verifier: %v", err)
		}
		return SignInOptions{
			Cookies:           w7Cookies(t, func() time.Time { return w7Epoch }),
			Verifier:          ver,
			Store:             &store.Store{},
			FirebaseAPIKey:    w7APIKey,
			FirebaseProjectID: w7Project,
			Domains:           w7Domains(),
		}
	}
	if _, err := NewSignIn(full()); err != nil {
		t.Fatalf("a complete configuration was refused: %v", err)
	}

	cases := map[string]func(o *SignInOptions){
		"Cookies":           func(o *SignInOptions) { o.Cookies = nil },
		"Verifier":          func(o *SignInOptions) { o.Verifier = nil },
		"Store":             func(o *SignInOptions) { o.Store = nil },
		"FirebaseAPIKey":    func(o *SignInOptions) { o.FirebaseAPIKey = " " },
		"FirebaseProjectID": func(o *SignInOptions) { o.FirebaseProjectID = "" },
		// No default allowlist: a sign-in that accepted every domain would
		// admit anyone with a Google account.
		"Domains": func(o *SignInOptions) { o.Domains = nil },
	}
	for name, drop := range cases {
		t.Run("missing "+name, func(t *testing.T) {
			o := full()
			drop(&o)
			_, err := NewSignIn(o)
			if err == nil {
				t.Fatal("err = nil")
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("err = %v, want it to name %s", err, name)
			}
		})
	}

	t.Run("names every missing field at once", func(t *testing.T) {
		o := full()
		o.FirebaseAPIKey = ""
		o.FirebaseProjectID = ""
		o.Store = nil
		_, err := NewSignIn(o)
		if err == nil {
			t.Fatal("err = nil")
		}
		for _, want := range []string{"Store", "FirebaseAPIKey", "FirebaseProjectID"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("err = %v, want it to name %s", err, want)
			}
		}
	})

	// The auth domain is interpolated into a Content-Security-Policy header, so a
	// value carrying a space, a semicolon or a wildcard does not name a different
	// host: it adds a directive, ends one, or opens the policy to everybody.
	t.Run("refuses an auth domain that would rewrite the policy", func(t *testing.T) {
		for _, bad := range []string{
			"example-project.firebaseapp.com; script-src *",
			"example-project.firebaseapp.com evil.example",
			"*.firebaseapp.com",
			"*",
			"https://example-project.firebaseapp.com",
			"example-project.firebaseapp.com/path",
			"example-project.firebaseapp.com\nX-Evil: 1",
		} {
			o := full()
			o.FirebaseAuthDomain = bad
			s, err := NewSignIn(o)
			if err == nil {
				t.Errorf("FirebaseAuthDomain %q was accepted, producing policy: %s", bad, s.csp)
			}
		}
	})

	t.Run("defaults the auth domain to the project's own", func(t *testing.T) {
		s, err := NewSignIn(full())
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if got := s.firebase.AuthDomain; got != w7Project+".firebaseapp.com" {
			t.Errorf("AuthDomain = %q", got)
		}
		if !strings.Contains(s.csp, "frame-src https://"+w7Project+".firebaseapp.com") {
			t.Errorf("the default auth domain did not reach the policy: %s", s.csp)
		}
	})
}

// ---------------------------------------------------------------------------
// The page the laptop agent opens: /auth/cli
// ---------------------------------------------------------------------------

// A state the shape internal/enroll mints: base64url of 32 random bytes, which
// is 43 characters.
const w7CLIState = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFG"

const w7CLIPort = "54321"

func w7CLIQuery() url.Values {
	return url.Values{"port": {w7CLIPort}, "state": {w7CLIState}}
}

// w7StripComments removes whole-line // comments and /* */ blocks.
//
// Only lines that begin with the marker, so the // inside an https:// import
// specifier is left alone.
func w7StripComments(src string) string {
	var out strings.Builder
	inBlock := false
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		if inBlock {
			if strings.Contains(trimmed, "*/") {
				inBlock = false
			}
			continue
		}
		if strings.HasPrefix(trimmed, "/*") {
			if !strings.Contains(trimmed, "*/") {
				inBlock = true
			}
			continue
		}
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		out.WriteString(line)
		out.WriteString("\n")
	}
	return out.String()
}

func w7CLIPage(t *testing.T, s *SignIn, q url.Values) *http.Response {
	t.Helper()
	rec := httptest.NewRecorder()
	s.handleCLI(rec, httptest.NewRequest(http.MethodGet, CLIAuthPath+"?"+q.Encode(), nil))
	return rec.Result()
}

// w7Directives splits a policy into its directives, keyed by name.
func w7Directives(csp string) map[string]string {
	out := map[string]string{}
	for _, d := range strings.Split(csp, ";") {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}
		name, value, _ := strings.Cut(d, " ")
		out[name] = value
	}
	return out
}

// Property: a query this server will not relay a token to renders no page at
// all — no API key, no project, no state, no script tag, no loopback address.
//
// This is the property the whole flow rests on. The page it refuses to render
// is one that obtains a live ID token, and the only thing that then stops the
// token being posted somewhere of the caller's choosing is that this server
// decided where it goes. A page rendered from a refused query would be an
// exfiltration primitive aimed by a link.
func TestCLIPageRendersNothingWhenTheCallbackIsRefused(t *testing.T) {
	cases := map[string]url.Values{
		"no query at all":            {},
		"no port":                    {"state": {w7CLIState}},
		"no state":                   {"port": {w7CLIPort}},
		"port below the range":       {"port": {"80"}, "state": {w7CLIState}},
		"port zero":                  {"port": {"0"}, "state": {w7CLIState}},
		"port above the range":       {"port": {"65536"}, "state": {w7CLIState}},
		"negative port":              {"port": {"-1"}, "state": {w7CLIState}},
		"port that is not a number":  {"port": {"evil.example"}, "state": {w7CLIState}},
		"port with a trailing host":  {"port": {"54321@evil.example"}, "state": {w7CLIState}},
		"port in hex":                {"port": {"0xd431"}, "state": {w7CLIState}},
		"state too short":            {"port": {w7CLIPort}, "state": {"short"}},
		"state too long":             {"port": {w7CLIPort}, "state": {strings.Repeat("a", 129)}},
		"state outside the alphabet": {"port": {w7CLIPort}, "state": {`"><script>alert(1)</script>`}},
		"state with a quote":         {"port": {w7CLIPort}, "state": {strings.Repeat("a", 20) + `"`}},
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			log, buf := w7Log()
			s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), log)

			res := w7CLIPage(t, s, q)
			if res.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", res.StatusCode)
			}
			body := w7Body(t, res)

			// Nothing that would let a browser run the SDK, and nothing naming
			// where a token could be sent.
			for _, leak := range []string{
				w7APIKey, w7Project, CLIScriptPath, "127.0.0.1", "<script", "firebase",
			} {
				if strings.Contains(body, leak) {
					t.Errorf("a refused page carried %q:\n%s", leak, body)
				}
			}
			// And the policy on it is the restrictive one, not the page's.
			if got := w7ScriptSources(res.Header.Get("Content-Security-Policy")); got != "'none'" {
				t.Errorf("script sources = %q, want 'none' on a refusal", got)
			}
			// The rejected value is caller input; it must not be reflected back
			// into the page, and the log line must stay bounded.
			if strings.Contains(body, "script>") {
				t.Errorf("the refusal reflected the query into the page:\n%s", body)
			}
			if len(buf.String()) > 4096 {
				t.Errorf("the refusal logged %d bytes of caller input", len(buf.String()))
			}
		})
	}
}

// Property: the destination is the one this server assembled from a port it
// parsed, and no other value in the query has any effect on it. A page that
// took its destination from its own query string is an exfiltration primitive.
func TestCLIPagePostsOnlyWhereTheServerDecided(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	q := w7CLIQuery()
	// Everything an attacker would try alongside a legitimate port.
	q.Set("host", "evil.example")
	q.Set("callback", "https://evil.example/steal")
	q.Set("redirect_uri", "https://evil.example/steal")
	q.Set("origin", "https://evil.example")
	q.Set("url", "https://evil.example")

	res := w7CLIPage(t, s, q)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	body := w7Body(t, res)

	if want := `data-callback-url="http://127.0.0.1:` + w7CLIPort + `/callback"`; !strings.Contains(body, want) {
		t.Errorf("page does not carry %s:\n%s", want, body)
	}
	if strings.Contains(body, "evil.example") {
		t.Errorf("a query value reached the page:\n%s", body)
	}
	if strings.Contains(res.Header.Get("Content-Security-Policy"), "evil.example") {
		t.Errorf("a query value reached the policy: %s", res.Header.Get("Content-Security-Policy"))
	}
	// And "localhost" is never a spelling of the loopback literal in this flow.
	if strings.Contains(body, "localhost") {
		t.Errorf("the page names localhost rather than the loopback literal:\n%s", body)
	}
}

// Property: the agent's nonce is relayed back exactly as it arrived. It is the
// only thing that lets the agent tell its own sign-in from another local
// process's, and this server neither stores nor compares it.
func TestCLIPageRelaysTheStateUntouched(t *testing.T) {
	for _, state := range []string{
		w7CLIState,
		strings.Repeat("a", 16),
		strings.Repeat("z", 128),
		"has-dashes_and_underscores-0123456",
	} {
		s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
		res := w7CLIPage(t, s, url.Values{"port": {w7CLIPort}, "state": {state}})
		if res.StatusCode != http.StatusOK {
			t.Fatalf("state %q: status = %d, want 200", state, res.StatusCode)
		}
		if want := `data-callback-state="` + state + `"`; !strings.Contains(w7Body(t, res), want) {
			t.Errorf("state %q was not relayed verbatim", state)
		}
	}
}

// Property: the policy names the one loopback origin and never a wildcard.
// http://127.0.0.1:* would let a page served on this route reach every port on
// the laptop, which is a port scanner holding a live credential; dropping the
// directive would let it reach anywhere at all.
func TestCLIPolicyAllowsExactlyTheOneLoopbackOrigin(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	res := w7CLIPage(t, s, w7CLIQuery())
	csp := res.Header.Get("Content-Security-Policy")
	d := w7Directives(csp)

	origin := "http://127.0.0.1:" + w7CLIPort
	if !strings.Contains(d["connect-src"], origin) {
		t.Errorf("connect-src = %q, want it to name %s", d["connect-src"], origin)
	}
	if strings.Contains(csp, "*") {
		t.Errorf("the policy carries a wildcard: %s", csp)
	}
	// The page has no form, so no navigation can carry the token.
	if d["form-action"] != "'none'" {
		t.Errorf("form-action = %q, want 'none'", d["form-action"])
	}
	if d["frame-ancestors"] != "'none'" {
		t.Errorf("frame-ancestors = %q, want 'none'", d["frame-ancestors"])
	}
	if d["default-src"] != "'none'" {
		t.Errorf("default-src = %q, want 'none'", d["default-src"])
	}
	// The loopback origin is allowed as a fetch destination and nothing else.
	for _, name := range []string{"script-src", "frame-src", "img-src", "style-src"} {
		if strings.Contains(d[name], "127.0.0.1") {
			t.Errorf("%s names the loopback origin: %q", name, d[name])
		}
	}
}

// Property: the two pages that may run script carry the same policy apart from
// the two directives that differ on purpose. Two hand-maintained policies for
// two pages using the same SDK drift, and the one that drifts is the page
// nobody opens by hand.
func TestCLIPolicyMatchesTheSignInPolicyExceptWhereItMust(t *testing.T) {
	authDomain := w7Project + ".firebaseapp.com"
	origin := "http://127.0.0.1:54321"

	signIn := w7Directives(newSignInCSP(authDomain))
	cli := w7Directives(newCLICSP(authDomain, origin))

	if len(signIn) != len(cli) {
		t.Fatalf("the two policies have different directives: %v vs %v", signIn, cli)
	}
	for name, want := range signIn {
		got, ok := cli[name]
		if !ok {
			t.Errorf("the cli policy is missing %s", name)
			continue
		}
		switch name {
		case "connect-src":
			if got != want+" "+origin {
				t.Errorf("connect-src = %q, want the sign-in value plus the callback origin", got)
			}
		case "form-action":
			if want != "'self'" || got != "'none'" {
				t.Errorf("form-action: sign-in %q, cli %q; want 'self' and 'none'", want, got)
			}
		default:
			if got != want {
				t.Errorf("%s differs unintentionally: sign-in %q, cli %q", name, want, got)
			}
		}
	}
}

// Property: the response headers this page needs to work and not to leak. The
// popup cannot report back without the opener policy, and the URL carries the
// agent's state so it is worth telling nobody where the page came from.
func TestCLIPageSetsThePopupAndPrivacyHeaders(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	res := w7CLIPage(t, s, w7CLIQuery())

	want := map[string]string{
		"Cross-Origin-Opener-Policy": "same-origin-allow-popups",
		"Referrer-Policy":            "no-referrer",
		"Cache-Control":              "no-store",
		"X-Content-Type-Options":     "nosniff",
		"Content-Type":               "text/html; charset=utf-8",
	}
	for k, v := range want {
		if got := res.Header.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	// No CSRF cookie on this route: the binding is the agent's state, and the
	// agent is the one that checks it.
	if len(res.Cookies()) != 0 {
		t.Errorf("the cli page set cookies: %v", res.Cookies())
	}
}

// Property: the module is served so a browser will execute it. The dashboard's
// static handler answers application/octet-stream with nosniff alongside, which
// a browser obeying both refuses to run.
func TestCLIScriptIsServedExecutable(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	rec := httptest.NewRecorder()
	s.handleCLIScript(rec, httptest.NewRequest(http.MethodGet, CLIScriptPath, nil))
	res := rec.Result()

	if got := res.Header.Get("Content-Type"); got != "text/javascript; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", got)
	}
	if body := w7Body(t, res); !strings.Contains(body, "signInWithPopup") {
		t.Error("the served module is not the sign-in module")
	}
}

// Property: the two modules load the same Firebase SDK. They are separate files
// so that each page's destination is fixed at render time, and the cost of that
// is a version pinned in two places; this is the test that makes the cost a
// failing build rather than one page sitting on an old SDK for a year.
func TestCLIAndSignInScriptsPinTheSameSDKVersion(t *testing.T) {
	version := regexp.MustCompile(`gstatic\.com/firebasejs/([0-9.]+)/`)

	collect := func(src string) []string {
		var out []string
		for _, m := range version.FindAllStringSubmatch(src, -1) {
			out = append(out, m[1])
		}
		return out
	}
	signIn := collect(web.SignInScript())
	cli := collect(web.CLIScript())

	if len(signIn) == 0 || len(cli) == 0 {
		t.Fatalf("no pinned SDK version found: sign-in %v, cli %v", signIn, cli)
	}
	for _, got := range append(signIn, cli...) {
		if got != signIn[0] {
			t.Errorf("the two modules pin different SDK versions: %v and %v", signIn, cli)
			break
		}
	}
}

// Property: the module takes its destination from what the server rendered and
// never from the URL. Reading location.search would put the destination back
// under the caller's control on the client side, undoing the whole reason the
// server parses the port.
func TestCLIScriptTakesItsDestinationFromTheDocument(t *testing.T) {
	// Comments stripped first. This asserts about what the module does, and a
	// comment explaining why it does not read the URL names the very API the
	// assertion forbids; a test that cannot tell those apart is a test that
	// punishes the next person for documenting the reasoning.
	src := w7StripComments(web.CLIScript())

	for _, forbidden := range []string{
		"location.search", "URLSearchParams", "window.location", "document.location",
		"location.href", "referrer",
	} {
		if strings.Contains(src, forbidden) {
			t.Errorf("the module reads %s; the destination must come from the rendered page", forbidden)
		}
	}
	if !strings.Contains(src, "dataset") {
		t.Error("the module does not read its configuration from the document")
	}
	// The token must not be written anywhere a later script or an extension can
	// read it off the page.
	for _, forbidden := range []string{"innerHTML", "document.write", "localStorage", "sessionStorage"} {
		if strings.Contains(src, forbidden) {
			t.Errorf("the module uses %s, which would expose the token", forbidden)
		}
	}
	// The post must force a preflight, which is what the agent answers only for
	// this origin. A form content type would skip it.
	if !strings.Contains(src, `"Content-Type": "application/json"`) {
		t.Error("the module does not post application/json, so the agent's preflight never happens")
	}
	if !strings.Contains(src, `mode: "cors"`) {
		t.Error("the module does not post in cors mode")
	}
}

// Property: the page carries no form. The token leaves by fetch, so there is no
// navigation that could carry it and form-action 'none' is honest.
func TestCLIPageHasNoForm(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	body := w7Body(t, w7CLIPage(t, s, w7CLIQuery()))

	for _, forbidden := range []string{"<form", "<input", "action="} {
		if strings.Contains(body, forbidden) {
			t.Errorf("the cli page carries %q:\n%s", forbidden, body)
		}
	}
	// And it tells somebody who did not start an installer that they should not
	// be here, which is the only thing this page can tell them that they could
	// not work out for themselves.
	if !strings.Contains(body, "did not just") {
		t.Error("the page does not warn somebody who did not start the enrollment")
	}
}

// Property: bareOrigin refuses anything that could add or end a directive in the
// policy header it is interpolated into.
func TestBareOriginRefusesAnythingThatCouldExtendAPolicy(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"http://127.0.0.1:54321", true},
		{"https://sessions.example.com", true},

		{"", false},
		{"127.0.0.1:54321", false},
		{"http://", false},
		{"ftp://127.0.0.1:1", false},
		{"javascript://127.0.0.1", false},
		{"http://127.0.0.1:1 https://evil.example", false},
		{"http://127.0.0.1:1; script-src *", false},
		{"http://127.0.0.1:1,https://evil.example", false},
		{"http://127.0.0.1:*", false},
		{"http://127.0.0.1:1/callback", false},
		{"http://127.0.0.1:1\nX-Evil: 1", false},
		{"http://127.0.0.1:1'", false},
	}
	for _, tc := range cases {
		if got := bareOrigin(tc.in); got != tc.want {
			t.Errorf("bareOrigin(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// Property: the CLI routes mount where the agent opens them, alongside the
// dashboard's, on one mux.
func TestCLIRoutesRegisterWhereTheAgentOpensThem(t *testing.T) {
	if CLIAuthPath != "/auth/cli" {
		t.Fatalf("the cli page moved to %q; internal/enroll opens /auth/cli", CLIAuthPath)
	}
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	s.Register(mux)

	for _, path := range []string{CLIAuthPath, CLIScriptPath, SignInPath, SignInScriptPath} {
		if _, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil)); pattern == "" {
			t.Errorf("%s is not routed", path)
		}
	}
	// The page is a GET. A POST to it must not render anything.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, CLIAuthPath+"?"+w7CLIQuery().Encode(), nil))
	if rec.Code == http.StatusOK {
		t.Error("POST /auth/cli rendered the page")
	}
}

// w7Attr pulls a data attribute's value out of the rendered page, which is how
// the module reads its configuration.
var w7Attr = func(body, name string) string {
	m := regexp.MustCompile(name + `="([^"]*)"`).FindStringSubmatch(body)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

// Property: the page this server actually renders posts where the agent is
// listening, with the payload the agent parses.
//
// The contract test in enroll_test.go simulates the browser by calling
// ParseCLICallback itself, which proves the agent agrees with that function.
// This one renders the real page for the real query and replays exactly what
// its module sends, taking the destination and the state out of the rendered
// HTML the way the module does. A mismatch between what this server renders and
// what the agent accepts — a renamed attribute, a payload key, a state that
// arrives altered — fails here rather than on somebody's laptop, where the only
// symptom is an installer that waits forever.
func TestTheRenderedCLIPagePostsWhatTheAgentAccepts(t *testing.T) {
	s := w7SignIn(t, &w7Verifier{}, w7LiveRoster("dev@example.com", store.RoleMember), slog.New(slog.DiscardHandler))

	v := &enrollVerifier{id: enrollIdentity("dev@example.com")}
	devices := &enrollDevices{}
	e := newTestEnroll(t, v, devices, enrollQuiet())
	mux := http.NewServeMux()
	e.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// The browser's half: fetch the page this server renders, then do what its
	// module does with it and nothing more.
	browser := func(raw string) error {
		u, err := url.Parse(raw)
		if err != nil {
			return err
		}
		rec := httptest.NewRecorder()
		s.handleCLI(rec, httptest.NewRequest(http.MethodGet, u.RequestURI(), nil))
		res := rec.Result()
		if res.StatusCode != http.StatusOK {
			t.Errorf("the agent's own URL rendered %d, not a page", res.StatusCode)
			return errors.New("no page")
		}
		body := w7Body(t, res)

		callback := w7Attr(body, "data-callback-url")
		state := w7Attr(body, "data-callback-state")
		if callback == "" || state == "" {
			t.Errorf("the page carries no destination or no state:\n%s", body)
			return errors.New("no destination")
		}
		// The policy on that response has to permit this very request, or the
		// browser refuses it before the agent ever sees it.
		if !strings.Contains(w7Directives(res.Header.Get("Content-Security-Policy"))["connect-src"], callback[:strings.LastIndex(callback, "/")]) {
			t.Errorf("the page's own policy forbids the post it exists to make: %s",
				res.Header.Get("Content-Security-Policy"))
		}

		payload, err := json.Marshal(map[string]string{"id_token": enrollIDToken, "state": state})
		if err != nil {
			return err
		}
		req, err := http.NewRequest(http.MethodPost, callback, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", srv.URL)
		post, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer post.Body.Close()
		if post.StatusCode != http.StatusOK {
			t.Errorf("the agent refused what the page posted: %d", post.StatusCode)
		}
		return nil
	}

	flow, err := enroll.New(enroll.Options{Endpoint: srv.URL, Open: browser, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	got, err := flow.Run(context.Background(), enroll.Machine{
		Hostname: "dev-mbp", OS: "darwin", Arch: "arm64", AgentVersion: "0.4.1",
	})
	if err != nil {
		t.Fatalf("a laptop could not enroll through the rendered page: %v", err)
	}
	if got.Email != "dev@example.com" {
		t.Errorf("email = %q", got.Email)
	}
	if !strings.HasPrefix(got.DeviceToken, auth.TokenPrefix) {
		t.Errorf("device token = %q, want one carrying the scanner prefix", got.DeviceToken)
	}
	if v.saw != enrollIDToken {
		t.Errorf("verified %q, want the token the rendered page posted", v.saw)
	}
}

// The bound that makes self-enrolment safe: it may create a row and it may
// never revive one. Without this, disabling somebody is undone the next time
// they sign in, and the admin page's only destructive action silently stops
// working — with no error anywhere, because the sign-in would look ordinary.
func TestSelfEnrolmentNeverRevivesADisabledAccount(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rows       map[string]store.Principal
		wantStatus int
		wantRole   store.Role
		wantCreate bool
	}{
		{
			name:       "an unknown colleague from a company domain is enrolled as a member",
			rows:       map[string]store.Principal{},
			wantStatus: http.StatusSeeOther,
			wantRole:   store.RoleMember,
			wantCreate: true,
		},
		{
			name:       "a disabled member stays refused and is not recreated",
			rows:       map[string]store.Principal{"dev@example.com": w7Disabled("dev@example.com", store.RoleMember)},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "a disabled admin stays refused and is not silently demoted to an enabled member",
			rows:       map[string]store.Principal{"dev@example.com": w7Disabled("dev@example.com", store.RoleAdmin)},
			wantStatus: http.StatusForbidden,
		},
		{
			// The one that would be worst to get wrong: self-enrolment must not
			// overwrite an existing admin with a member row.
			name:       "an existing admin keeps their role",
			rows:       map[string]store.Principal{"dev@example.com": w7Enabled("dev@example.com", store.RoleAdmin)},
			wantStatus: http.StatusSeeOther,
			wantRole:   store.RoleAdmin,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roster := &w7Roster{rows: tc.rows}
			s := w7SignIn(t, &w7Verifier{id: w7Identity("dev@example.com")}, roster, slog.New(slog.DiscardHandler))
			res := w7SignedIn(t, s, "")

			if res.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.wantStatus)
			}
			if got := roster.rows["dev@example.com"]; tc.wantRole != "" && got.Role != tc.wantRole {
				t.Errorf("role = %q, want %q", got.Role, tc.wantRole)
			}
			if tc.wantStatus == http.StatusForbidden {
				if len(roster.enrolled) != 0 {
					t.Errorf("a refused sign-in created %v", roster.enrolled)
				}
				if got := roster.rows["dev@example.com"]; got.DisabledAt == nil {
					t.Error("the disabled stamp was cleared by a sign-in attempt")
				}
			}
			if tc.wantCreate && len(roster.enrolled) != 1 {
				t.Errorf("enrolled %v, want exactly one creation", roster.enrolled)
			}
		})
	}
}
