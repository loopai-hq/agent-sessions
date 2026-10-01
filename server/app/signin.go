package app

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/store"
	"github.com/loopai-hq/loop-sessions/server/web"
)

const (
	// SignInPath renders the page that signs somebody in. web.Options.SignInPath
	// is set from it and every unauthenticated dashboard page redirects here.
	SignInPath = "/auth/signin"

	// SignInScriptPath serves that page's module from this origin, which is what
	// lets its script-src be 'self' plus Google's CDN and nothing else.
	//
	// The dashboard's /static/ handler cannot serve it. That handler has no
	// Content-Type for .js and answers application/octet-stream, and it sends
	// X-Content-Type-Options: nosniff alongside, so a browser obeying both will
	// refuse to execute the file — a failure that looks like a broken script
	// rather than like a missing case in a switch.
	SignInScriptPath = "/auth/signin.js"

	// SessionPath takes the Firebase ID token that page obtained and mints the
	// dashboard session. A POST because it changes state and because a token in
	// a URL is a token in every proxy log between here and the browser.
	SessionPath = "/auth/session"

	// SignOutPath is a POST because a GET sign-out can be triggered by any page
	// that can make this browser fetch an image.
	SignOutPath = "/auth/signout"

	// CLIScriptPath serves the module for CLIAuthPath, for the same reason
	// SignInScriptPath exists: the dashboard's /static/ handler answers
	// application/octet-stream with nosniff alongside, so a browser obeying both
	// refuses to execute it.
	//
	// A second module rather than the sign-in page's. That one ends in a form
	// submit to this origin and this one ends in a fetch to a loopback port, and
	// a single module deciding between them at run time would be a module that
	// can be made to choose wrongly.
	CLIScriptPath = "/auth/cli.js"
)

const (
	// The CSRF cookie carries the __Host- prefix for the same reason the session
	// cookie does: without it any host under the parent domain can set a cookie
	// this server would read as its own, and a forgeable half of a double-submit
	// pair is not a check at all.
	csrfCookieName = "__Host-loop_signin_csrf"
	// Browsers drop a __Host- cookie that is not Secure, and the resulting
	// failure looks like a bug in the sign-in code rather than a missing flag.
	insecureCSRFCookieName = "loop_signin_csrf"

	// Long enough to leave the tab open through a meeting and still finish,
	// short enough that an abandoned attempt is not still submittable an hour
	// later.
	csrfLifetime = 30 * time.Minute

	// A Firebase ID token is around a kilobyte and the other three fields are
	// short. The cap is here so a body this endpoint would refuse anyway is
	// refused before it is read into memory.
	maxSessionBodyBytes = 16 << 10

	// The Firebase auth domain defaults to this suffix under the project id,
	// which is what the console provisions and what the organisation's other
	// frontends use.
	firebaseAuthDomainSuffix = ".firebaseapp.com"
)

// Every way a sign-in can fail to establish an identity renders this, byte for
// byte: a CSRF token that did not match the cookie, a post carrying no token, a
// signature that did not verify, an expired or wrong-project token, an account
// that signed in with anything other than Google, and an address outside the
// allowed domains.
//
// The uniformity is the property. auth/firebase.go returns those causes as
// separate values so this server can record which check fired; telling the
// caller which one fired helps nobody except the caller who is guessing.
const signInRefusedMessage = "We could not complete that sign-in. Please start again from the dashboard. " +
	"If you chose a personal Google account, start again and pick your work Google account."

// The one distinguishable outcome, and only because it is the caller's own
// account either way. Somebody Google verified, in one of our domains, who has
// no enabled roster row has to be told that rather than sent round the sign-in
// loop forever. It says nothing about anybody else, and "no row" and "row
// switched off" render identically so it says nothing about whether they were
// ever enrolled either.
const signInNotEnrolledMessage = "This Google account cannot use loop-sessions. Ask an admin to add you."

const signInUnavailableMessage = "Sign-in is temporarily unavailable. Please try again in a moment."

// SignInOptions configure a SignIn.
type SignInOptions struct {
	// Cookies mints the dashboard session. Required.
	Cookies *auth.Cookies
	// Verifier checks the Firebase ID token the page posts back. Required.
	Verifier *auth.Verifier
	// Store resolves a verified address to its roster row. Required.
	Store *store.Store

	// FirebaseAPIKey is the web API key, rendered into the page.
	//
	// It is public by design, like every Firebase web config: it names the
	// project so the SDK knows which one to talk to and it authorises nothing on
	// its own. Storing it as a secret or redacting it in a log would imply
	// otherwise, and the next person to handle it would treat a leak as an
	// incident instead of as a page-source view. Required, because a page
	// rendered without it fails in the browser with an SDK error rather than
	// here with a configuration one.
	FirebaseAPIKey string
	// FirebaseProjectID is the Firebase project, and must be the same one the
	// Verifier pins its audience and issuer to. Required.
	FirebaseProjectID string
	// FirebaseAuthDomain is where the SDK's auth helper is hosted. Empty takes
	// <project>.firebaseapp.com.
	FirebaseAuthDomain string

	// Domains are the email domains an address may sit in. Required: at
	// least one, with no default. It is a list because a Workspace can carry
	// more than one domain; see emailInDomains.
	Domains []string

	// Insecure must match auth.CookieOptions.Insecure. The CSRF cookie is set by
	// this package rather than by auth, so it needs the same answer about
	// whether a browser will accept a __Host- name.
	Insecure bool

	Logger *slog.Logger
}

// SignIn is the dashboard's Firebase sign-in surface: a page that runs the
// Firebase SDK, the module that page loads, the exchange that turns the
// resulting ID token into this server's own session, and sign-out.
type SignIn struct {
	cookies *auth.Cookies
	verify  idTokenVerifier
	roster  authPrincipals
	page    *template.Template
	script  string
	// csp is this route's Content-Security-Policy, assembled once because it
	// interpolates the configured auth domain. See newSignInCSP for why it is
	// this route's and not the service's.
	csp      string
	firebase signInData
	domains  []string
	secure   bool
	log      *slog.Logger

	// The CLI enrollment page and its module. Its policy cannot be assembled
	// once alongside csp: it names the loopback origin from the request's own
	// query, so it is built per request from a port this server has parsed.
	cliPage   *template.Template
	cliScript string
	// authDomain is kept so that per-request policy can be built from it.
	authDomain string
}

// NewSignIn builds a SignIn, failing rather than defaulting on anything whose
// absence would make the flow insecure instead of broken.
//
// A missing API key produces a page the SDK cannot initialise, which is merely
// broken; a missing verifier or roster would produce a sign-in that succeeds for
// anybody. Both are refused here, because the point of failing at construction
// is that an operator watching a deploy sees it rather than a colleague seeing
// it at the moment they try to get in.
func NewSignIn(o SignInOptions) (*SignIn, error) {
	var missing []string
	if o.Cookies == nil {
		missing = append(missing, "Cookies")
	}
	if o.Verifier == nil {
		missing = append(missing, "Verifier")
	}
	if o.Store == nil {
		missing = append(missing, "Store")
	}
	if strings.TrimSpace(o.FirebaseAPIKey) == "" {
		missing = append(missing, "FirebaseAPIKey")
	}
	if strings.TrimSpace(o.FirebaseProjectID) == "" {
		missing = append(missing, "FirebaseProjectID")
	}
	if len(cleanDomains(o.Domains)) == 0 {
		missing = append(missing, "Domains")
	}
	if len(missing) > 0 {
		// Everything at once. A server that reports one missing field per restart
		// costs an operator one deploy cycle per mistake.
		return nil, fmt.Errorf("app: sign-in needs %s", strings.Join(missing, ", "))
	}

	project := strings.TrimSpace(o.FirebaseProjectID)
	authDomain := strings.TrimSpace(o.FirebaseAuthDomain)
	if authDomain == "" {
		authDomain = project + firebaseAuthDomainSuffix
	}
	if !bareHost(authDomain) {
		return nil, fmt.Errorf("app: FirebaseAuthDomain must be a bare hostname, got %q", o.FirebaseAuthDomain)
	}

	page, err := template.New("signin.html").Parse(web.SignInPageTemplate())
	if err != nil {
		// A template that does not parse is a failed deploy rather than a 500 on
		// the one page every signed-out person lands on.
		return nil, fmt.Errorf("app: parse the sign-in page: %w", err)
	}
	cliPage, err := template.New("cli.html").Parse(web.CLIPageTemplate())
	if err != nil {
		// Same reasoning, and it matters more here: nobody opens this page by
		// browsing, so a template failure would first be seen by whoever is
		// installing the agent rather than by anybody watching the deploy.
		return nil, fmt.Errorf("app: parse the cli sign-in page: %w", err)
	}

	s := &SignIn{
		cookies:    o.Cookies,
		verify:     o.Verifier,
		roster:     o.Store,
		page:       page,
		script:     web.SignInScript(),
		csp:        newSignInCSP(authDomain),
		cliPage:    cliPage,
		cliScript:  web.CLIScript(),
		authDomain: authDomain,
		firebase: signInData{
			APIKey:      strings.TrimSpace(o.FirebaseAPIKey),
			AuthDomain:  authDomain,
			ProjectID:   project,
			ScriptPath:  SignInScriptPath,
			SessionPath: SessionPath,
		},
		domains: cleanDomains(o.Domains),
		secure:  !o.Insecure,
		log:     o.Logger,
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	return s, nil
}

// bareHost reports whether v is a hostname and nothing else.
//
// The auth domain is interpolated into a Content-Security-Policy header, where a
// space, a semicolon or a comma does not name a different host: it adds or ends
// a directive. A wildcard is the other half of the same problem, because it is
// the first thing anybody reaches for to make a stubborn deployment work. A
// hostname contains none of these, so refusing them costs nothing real and
// removes the whole class.
func bareHost(v string) bool {
	return v != "" && !strings.ContainsAny(v, " \t\r\n;,'\"/\\*?#")
}

// newSignInCSP assembles the policy for the dashboard's sign-in page.
//
// The dashboard's script-src 'none' exists because it renders transcripts, and a
// transcript contains whatever an agent read: script tags, HTML entities, error
// text pasted back. That policy is what makes an escaping mistake there inert,
// and it must not be weakened for everybody to let one page run one script.
//
// This page can afford the opposite because it renders nothing anybody else
// controls. There is no transcript on it, no session title, no prompt text, and
// the single caller-supplied value it carries — `next` — has already been
// reduced to a path on this site by localPath and is escaped into an attribute
// by html/template. The policy is therefore set on this route's own response and
// on no other, which is why it lives here rather than in web.Secure.
func newSignInCSP(authDomain string) string {
	// The form posts to SessionPath on this origin and nowhere else.
	return scriptPageCSP(authDomain, "", "form-action 'self'")
}

// newCLICSP assembles the policy for the page the laptop agent opens.
//
// It differs from the sign-in page's in exactly two directives, and both
// differences exist because of where the token goes.
//
// connect-src gains the loopback origin, because without it this page's own
// policy forbids the single request it exists to make. The origin is the exact
// one, not http://127.0.0.1:* — a wildcard would let a page served on this
// route reach every port on the laptop, which is a port scanner with a live
// credential in hand, and dropping the directive would let it reach anywhere at
// all. The value is assembled by ParseCLICallback from an integer it parsed and
// range-checked, so no string a caller supplied is interpolated into a header.
//
// form-action becomes 'none' because there is no form on that page. The token
// leaves by fetch, so no navigation can carry it, and saying so in the policy
// means a form added later fails loudly rather than quietly working.
func newCLICSP(authDomain, callbackOrigin string) string {
	return scriptPageCSP(authDomain, callbackOrigin, "form-action 'none'")
}

// scriptPageCSP is the shared body of the two relaxed policies in this service.
//
// Both are built here rather than written out twice so that a directive added
// for one page is a directive somebody has to consider for the other. Two
// hand-maintained policies for two pages doing the same thing with the same SDK
// drift, and the one that drifts is the page nobody opens by hand.
func scriptPageCSP(authDomain, extraConnect, formAction string) string {
	// What the Firebase Auth SDK talks to: identitytoolkit to complete the
	// sign-in, securetoken to mint the ID token.
	connect := "connect-src https://identitytoolkit.googleapis.com https://securetoken.googleapis.com"
	if extraConnect != "" {
		connect += " " + extraConnect
	}
	return strings.Join([]string{
		"default-src 'none'",
		// 'self' is this route's own module. gstatic is Google's own origin and
		// is where the organisation's other frontends take the Firebase SDK from; a
		// third-party CDN would put the code handling a live credential on
		// somebody else's release process.
		//
		// apis.google.com is not optional and is not obvious from the import
		// list: nothing in this page's source names it. signInWithPopup loads
		// https://apis.google.com/js/api.js at run time to host the gapi iframe
		// the flow is built on, so a policy covering only the modules the page
		// imports blocks it. The symptom is the SDK reporting
		// auth/internal-error, which reads like a fault at Google rather than a
		// header we sent, and the only evidence is a blocked request in the
		// browser console that no server log records.
		"script-src 'self' https://www.gstatic.com https://apis.google.com",
		connect,
		// Three frames, for three different steps. The project's auth domain
		// hosts the SDK's auth helper, apis.google.com hosts the gapi iframe,
		// and accounts.google.com is the account chooser the popup lands on.
		// Omitting any one of them fails the flow at a different point, all
		// with the same opaque auth/internal-error.
		"frame-src https://" + authDomain + " https://apis.google.com https://accounts.google.com",
		// The page's styling is a <style> block. Nothing a remote party controls
		// is interpolated into either page, which is exactly the condition that
		// makes inline style safe here and unacceptable on a transcript page.
		"style-src 'unsafe-inline'",
		"img-src data:",
		formAction,
		// Nobody frames a sign-in button.
		"frame-ancestors 'none'",
		"base-uri 'none'",
	}, "; ")
}

// Register adds the sign-in routes to a mux owned by the caller.
func (s *SignIn) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET "+SignInPath, s.handleSignIn)
	mux.HandleFunc("GET "+SignInScriptPath, s.handleScript)
	mux.HandleFunc("GET "+CLIAuthPath, s.handleCLI)
	mux.HandleFunc("GET "+CLIScriptPath, s.handleCLIScript)
	mux.HandleFunc("POST "+SessionPath, s.handleSession)
	mux.HandleFunc("POST "+SignOutPath, s.handleSignOut)
}

// signInData is everything the sign-in page renders.
//
// Every field is configuration or a compile-time path except CSRF, which this
// server minted a moment ago, and Next, which localPath has already reduced to
// somewhere on this site.
type signInData struct {
	APIKey      string
	AuthDomain  string
	ProjectID   string
	ScriptPath  string
	SessionPath string
	CSRF        string
	Next        string
}

// cliData is everything the agent's sign-in page renders.
//
// It carries no CSRF token, which is the one place this page is deliberately
// weaker than the dashboard's and is not a gap. The dashboard's pair binds a
// post to a browser this server rendered a page for, because this server is the
// one being posted to. Here the post goes to the agent, and the value that
// binds it is State, which the agent minted and which the agent compares — in
// constant time, in internal/enroll. A token minted here would bind the page to
// this browser and tell the agent nothing, so it would be ceremony rather than
// a check.
type cliData struct {
	APIKey     string
	AuthDomain string
	ProjectID  string
	ScriptPath string
	// CallbackURL is the one address this page may post the token to. It comes
	// from ParseCLICallback, which builds it from a parsed integer rather than
	// from any string in the query.
	CallbackURL string
	// State is the agent's nonce, relayed back untouched. This server neither
	// stores nor compares it.
	State string
}

// handleSignIn renders the page that runs the Firebase SDK.
func (s *SignIn) handleSignIn(w http.ResponseWriter, r *http.Request) {
	token, err := randomToken()
	if err != nil {
		s.fail(r, "mint a sign-in csrf token", err)
		signInPage(w, http.StatusServiceUnavailable, "Sign-in unavailable", signInUnavailableMessage)
		return
	}

	data := s.firebase
	data.CSRF = token
	data.Next = localPath(r.URL.Query().Get("next"))

	// Rendered into a buffer before a single header is written, so a template
	// failure produces the unavailable page rather than a half-written document
	// with a 200 and a relaxed policy on it.
	var buf bytes.Buffer
	if err := s.page.Execute(&buf, data); err != nil {
		s.fail(r, "render the sign-in page", err)
		signInPage(w, http.StatusServiceUnavailable, "Sign-in unavailable", signInUnavailableMessage)
		return
	}

	s.setCSRF(w, token)
	noStore(w)
	w.Header().Set("Content-Security-Policy", s.csp)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// same-origin, so the destination somebody was heading for — which names a
	// session id — is not handed to Google along with the popup.
	w.Header().Set("Referrer-Policy", "same-origin")
	// same-origin-allow-popups, not same-origin. signInWithPopup completes by
	// posting a message back to the window that opened it, and a strict
	// Cross-Origin-Opener-Policy severs that link: the popup succeeds, the
	// opener never hears about it, and the page waits forever with no error.
	// This is the one header the dashboard sets that this page must not.
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin-allow-popups")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = buf.WriteTo(w)
}

// handleScript serves the page's module.
func (s *SignIn) handleScript(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// The same short window the dashboard gives an asset whose URL is not
	// fingerprinted: long enough to survive a reload, short enough that a deploy
	// is picked up without anybody clearing a cache.
	w.Header().Set("Cache-Control", "public, max-age=60")
	_, _ = io.WriteString(w, s.script)
}

// The page the agent opens renders this and nothing else when its query does not
// describe a callback this server will relay a token to. One message for every
// cause, because the causes are a bad port, a port outside the ephemeral range,
// and a state that is not the shape this client mints — and a caller sweeping
// ports learns nothing from being told which.
const cliRefusedMessage = "This link is not a valid sign-in for the loop-sessions installer. " +
	"Run the installer again and use the page it opens."

// handleCLI renders the page the laptop agent opens in a browser.
//
// The security of the whole enrollment flow rests on one property of this
// handler: the page is rendered from ParseCLICallback's answer or it is not
// rendered at all. A refusal must emit no API key, no state and no script,
// because a page that runs the SDK is a page that obtains a live ID token, and
// the only thing that then stops the token going somewhere else is that this
// server decided where it goes.
func (s *SignIn) handleCLI(w http.ResponseWriter, r *http.Request) {
	cb, err := ParseCLICallback(r.URL.Query())
	if err != nil {
		// The error carries only clipped, bounded values; see ParseCLICallback.
		s.log.Warn("cli sign-in refused", "err", err, "remote", r.RemoteAddr)
		noStore(w)
		signInPage(w, http.StatusBadRequest, "Sign-in failed", cliRefusedMessage)
		return
	}
	// Defence in depth, not doubt about today's ParseCLICallback: cb.Origin is
	// interpolated into a Content-Security-Policy header, where a space or a
	// semicolon does not name a different host but adds or ends a directive.
	// This is the same guard bareHost applies to the configured auth domain, and
	// it is here so that a later change to how the origin is built cannot turn
	// into header injection without a test failing first.
	if !bareOrigin(cb.Origin) {
		s.fail(r, "assemble the cli policy", fmt.Errorf("callback origin is not a bare origin: %q", cb.Origin))
		noStore(w)
		signInPage(w, http.StatusBadRequest, "Sign-in failed", cliRefusedMessage)
		return
	}

	var buf bytes.Buffer
	if err := s.cliPage.Execute(&buf, cliData{
		APIKey:      s.firebase.APIKey,
		AuthDomain:  s.firebase.AuthDomain,
		ProjectID:   s.firebase.ProjectID,
		ScriptPath:  CLIScriptPath,
		CallbackURL: cb.URL,
		State:       cb.State,
	}); err != nil {
		// Rendered into a buffer before a single header is written, so a template
		// failure produces the unavailable page rather than a half-written document
		// with a 200 and a relaxed policy on it.
		s.fail(r, "render the cli sign-in page", err)
		signInPage(w, http.StatusServiceUnavailable, "Sign-in unavailable", signInUnavailableMessage)
		return
	}

	noStore(w)
	w.Header().Set("Content-Security-Policy", newCLICSP(s.authDomain, cb.Origin))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// no-referrer rather than the sign-in page's same-origin. This URL carries
	// the agent's state in its query, and there is no navigation on this page
	// whose destination is worth telling where it came from.
	w.Header().Set("Referrer-Policy", "no-referrer")
	// signInWithPopup completes by posting a message back to the window that
	// opened it, and a strict Cross-Origin-Opener-Policy severs that link: the
	// popup succeeds, the opener never hears about it, and the page waits forever
	// with no error.
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin-allow-popups")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = buf.WriteTo(w)
}

// handleCLIScript serves that page's module.
func (s *SignIn) handleCLIScript(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_, _ = io.WriteString(w, s.cliScript)
}

// bareOrigin reports whether v is a scheme and host and nothing else, with none
// of the characters that would let it add or end a directive in a policy header.
func bareOrigin(v string) bool {
	scheme, rest, ok := strings.Cut(v, "://")
	if !ok || (scheme != "http" && scheme != "https") || rest == "" {
		return false
	}
	return !strings.ContainsAny(rest, " \t\r\n;,'\"/\\*?#")
}

// handleSession exchanges a verified Firebase ID token for this server's session
// cookie.
func (s *SignIn) handleSession(w http.ResponseWriter, r *http.Request) {
	noStore(w)
	w.Header().Set("Referrer-Policy", "same-origin")

	// Always consumed, whatever happens next: a token that survives one attempt
	// is a token that can be replayed against a second.
	minted, haveCSRF := s.takeCSRF(w, r)

	// Two independent checks, because each covers what the other misses. The
	// Origin header proves the post came from a page on this origin. The token
	// pair proves it came from a page this server rendered for this browser, and
	// it still holds against a client that sends no Origin at all.
	if origin := r.Header.Get("Origin"); origin != "" {
		if u, err := url.Parse(origin); err != nil || u.Host != r.Host {
			s.refuse(w, r, "origin", errors.New("session exchange posted from another origin"))
			return
		}
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxSessionBodyBytes)
	if err := r.ParseForm(); err != nil {
		s.refuse(w, r, "body", fmt.Errorf("read the session exchange: %w", err))
		return
	}

	// Compared, not merely present. Without this anybody can make a colleague's
	// browser post an ID token for an account of the attacker's choosing and
	// leave that browser holding a session for it, which is a session fixation
	// with the attacker on the other end of every transcript the colleague opens.
	if !haveCSRF || subtle.ConstantTimeCompare([]byte(minted), []byte(r.PostFormValue("csrf"))) != 1 {
		s.refuse(w, r, "csrf", errors.New("the posted csrf token did not match the cookie"))
		return
	}

	// Validated here rather than trusted from the page. It was validated on the
	// way in, and it has been out of this server's hands since; the check that
	// matters is the one immediately before the Location header is written.
	next := localPath(r.PostFormValue("next"))

	idToken := r.PostFormValue("id_token")
	if idToken == "" {
		s.refuse(w, r, "token", errors.New("session exchange carried no id token"))
		return
	}

	id, err := s.verify.Verify(r.Context(), idToken)
	if err != nil {
		s.refuse(w, r, "verify", err)
		return
	}
	// Checked again here, against this handler's own list. The Verifier has
	// already refused an address outside the domains it was configured with, but
	// the two lists are configured separately and a deployment that widens one
	// and not the other should widen nothing: a Firebase token carries no `hd`
	// claim, so the address is the entire domain control and it is worth two
	// independent statements of what the domains are.
	if !emailInDomains(id.Email, s.domains) {
		s.refuse(w, r, "domain", errors.New("address is outside the allowed domains"))
		return
	}

	p, found, err := s.roster.PrincipalLookup(r.Context(), auth.Normalize(id.Email))
	if err != nil {
		s.fail(r, "read roster", err)
		signInPage(w, http.StatusServiceUnavailable, "Sign-in unavailable", signInUnavailableMessage)
		return
	}
	// An address from a company domain with no row yet is a colleague who has
	// not signed in before, not an intruder. Everything that decides they may be
	// here has already run: Firebase minted the token, the signature and project
	// verified, the provider was Google, and the domain is ours. Refusing them
	// would mean every one of fifty people needs an admin to type their address
	// before they can use the tool, which is how a rollout stalls.
	//
	// They are created as a member and nothing more. A member sees only their
	// own sessions, so the worst outcome of a wrong new row is somebody with a
	// company Google account seeing their own work.
	if !found {
		var created bool
		p, created, err = s.roster.EnrolSelf(r.Context(), auth.Normalize(id.Email))
		if err != nil {
			s.fail(r, "self-enrol", err)
			signInPage(w, http.StatusServiceUnavailable, "Sign-in unavailable", signInUnavailableMessage)
			return
		}
		found = true
		if created {
			s.log.Info("enrolled a new member on first sign-in", "email", p.Email)
		}
	}

	role, roleErr := auth.ParseRole(string(p.Role))
	if p.DisabledAt != nil || roleErr != nil {
		// Deliberately AFTER the creation above and deliberately not merged with
		// it. EnrolSelf can only add, so a disabled row arrives here unchanged
		// and is refused: if self-enrolment could revive one, disabling somebody
		// would be undone by them simply signing in again, and the admin page's
		// only destructive action would silently stop working.
		//
		// A role the roster holds that this server cannot interpret is refused
		// rather than defaulted, because the default chosen under time pressure
		// is the permissive one.
		s.log.Info("sign-in refused: no enabled roster row",
			"email", auth.Normalize(id.Email), "found", found, "disabled", p.DisabledAt != nil)
		signInPage(w, http.StatusForbidden, "Not enrolled", signInNotEnrolledMessage)
		return
	}

	// The token's `iat`, which is when Firebase minted it. auth.Cookies measures
	// its absolute cap from this and never resets it on renewal, so it is what
	// makes the cap a bound rather than a suggestion.
	//
	// It stands for the moment the person authenticated only because the sign-in
	// page keeps no Firebase session in the browser: with in-memory persistence
	// every token is minted by a fresh popup. A page that let the SDK persist a
	// session would hand back tokens refreshed hourly from an authentication days
	// old, and the cap would restart on each refresh. The `auth_time` claim would
	// say so directly; auth.Identity does not carry it.
	if _, err := s.cookies.Issue(w, p.Email, role, id.IssuedAt); err != nil {
		s.fail(r, "issue session", err)
		signInPage(w, http.StatusServiceUnavailable, "Sign-in unavailable", signInUnavailableMessage)
		return
	}
	s.log.Info("signed in", "email", p.Email, "role", string(role))
	// See Other so the browser follows with a GET; a 302 after a POST leaves
	// some clients repeating the post to the new location.
	http.Redirect(w, r, next, http.StatusSeeOther)
}

// handleSignOut clears the session cookie.
//
// It has nothing to say to Firebase. The browser holds no Firebase session to
// end — the sign-in page keeps none — so this cookie is the whole session and
// deleting it is the whole sign-out.
func (s *SignIn) handleSignOut(w http.ResponseWriter, r *http.Request) {
	s.cookies.Clear(w)
	noStore(w)
	// See Other so the browser follows with a GET; a 302 after a POST leaves
	// some clients repeating the post to the new location.
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// refuse renders the one indistinguishable failure and records which check
// actually fired.
func (s *SignIn) refuse(w http.ResponseWriter, r *http.Request, stage string, err error) {
	// The error text is safe to log: auth's sentinels name the check, not the
	// credential, and nothing here formats the ID token into a message.
	s.log.Warn("sign-in refused", "stage", stage, "err", err, "remote", r.RemoteAddr)
	signInPage(w, http.StatusForbidden, "Sign-in failed", signInRefusedMessage)
}

// fail records something that is this server's fault rather than the caller's.
func (s *SignIn) fail(r *http.Request, what string, err error) {
	s.log.Error("sign-in", "op", what, "err", err, "path", r.URL.Path)
}

// ---------------------------------------------------------------------------
// CSRF
// ---------------------------------------------------------------------------

func (s *SignIn) csrfName() string {
	if s.secure {
		return csrfCookieName
	}
	return insecureCSRFCookieName
}

// setCSRF stores one half of the pair the exchange compares.
//
// The token in the page alone proves nothing: the page is unauthenticated, so an
// attacker can fetch one for themselves. It is the cookie that binds the pair to
// a browser, because an attacker can neither read this browser's cookies nor set
// one this server will read as its own.
func (s *SignIn) setCSRF(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:  s.csrfName(),
		Value: token,
		Path:  "/",
		// The whole exchange is bounded by this. A token that outlives the
		// sign-in it belongs to is a token somebody can come back to.
		MaxAge:   int(csrfLifetime.Seconds()),
		HttpOnly: true,
		Secure:   s.secure,
		// Strict, which the OAuth state cookie this replaces could not be: that
		// one had to survive a top-level navigation back from accounts.google.com,
		// and this one accompanies a form post from a page on this origin. Strict
		// therefore costs nothing and is the outer half of the defence — a
		// cross-site post arrives carrying no cookie at all and is refused before
		// the comparison happens.
		SameSite: http.SameSiteStrictMode,
	})
}

// takeCSRF reads and always clears the CSRF cookie.
func (s *SignIn) takeCSRF(w http.ResponseWriter, r *http.Request) (string, bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     s.csrfName(),
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   s.secure,
		SameSite: http.SameSiteStrictMode,
	})
	c, err := r.Cookie(s.csrfName())
	if err != nil || c.Value == "" {
		return "", false
	}
	return c.Value, true
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("app: no entropy available for a sign-in token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// ---------------------------------------------------------------------------
// next
// ---------------------------------------------------------------------------

// localPath reduces a caller-supplied destination to somewhere on this site,
// falling back to the root rather than reporting an error.
//
// An absolute URL accepted here is an open redirect, and an open redirect on
// the sign-in path of an internal tool is the ideal phishing primitive: the
// link a colleague receives is on the domain they have been told to trust, and
// it lands them on the attacker's page already in the habit of signing in.
//
// The awkward cases are the ones that do not look absolute. "//evil.example" is
// protocol-relative and resolves to another host. "/\evil.example" is treated
// as the same thing by browsers that normalise a backslash to a slash. Both
// begin with the slash a naive prefix check is satisfied by.
func localPath(raw string) string {
	const fallback = "/"
	if raw == "" || raw[0] != '/' {
		return fallback
	}
	if len(raw) > 1 && (raw[1] == '/' || raw[1] == '\\') {
		return fallback
	}
	// A newline or a NUL in a Location header is a response-splitting attempt.
	// Go's own writer rejects the header rather than sending it, which would turn
	// this into a blank page; refusing here keeps it a redirect to the root.
	if strings.ContainsAny(raw, "\x00\r\n") {
		return fallback
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "" || u.Host != "" || u.Opaque != "" || u.User != nil {
		return fallback
	}
	return raw
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

// noStore keeps a response that carries or mints a credential out of every cache
// between here and the browser.
func noStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}

// signInPage renders the browser-facing result of a sign-in attempt.
//
// This is what somebody sees at the moment they are deciding whether the tool
// works, so it says what happened in words rather than echoing a status code.
// Title and message are compile-time constants: nothing a remote party controls
// is interpolated, which is what makes the inline style safe to allow.
//
// Its policy is the strict one, deliberately. This page runs no script, and the
// relaxed policy belongs to the one route that does — see newSignInCSP.
func signInPage(w http.ResponseWriter, status int, title, msg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<!doctype html><meta charset="utf-8">
<title>loop-sessions</title>
<style>
 body{font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      display:grid;place-items:center;min-height:100vh;margin:0;
      background:#fafafa;color:#1a1a1a}
 @media (prefers-color-scheme:dark){body{background:#141414;color:#eaeaea}}
 .card{max-width:32rem;padding:2.5rem;text-align:center}
 h1{font-size:1.375rem;margin:0 0 .75rem;font-weight:600}
 p{margin:0;opacity:.75}
</style>
<div class="card"><h1>%s</h1><p>%s</p></div>`, title, msg)
}
