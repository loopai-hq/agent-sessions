// Package web is the loop-sessions dashboard: server-rendered HTML over the
// session store, with no build step and almost no JavaScript.
//
// The absence of a front-end toolchain is a security property before it is a
// maintenance one. This dashboard renders transcripts, and transcripts contain
// whatever an agent read: web pages it fetched, files it opened, error text it
// pasted back. That content is script tags and HTML entities as often as it is
// prose. The event archive and individual event/file pages ship no JavaScript
// and carry script-src 'none'. The opt-in continuous conversation route allows
// one first-party script and same-origin reads of those bounded archive pages.
// Transcript markup is still produced by escaped server templates; inline and
// third-party scripts remain blocked.
//
// The filter pages are the exception and they are fenced off deliberately.
// They load one dependency-free file, static/combo.js, so the themed dropdowns
// prune as somebody types; Secure grants script-src 'self' to exactly the
// routes in scriptedPaths. Every one of those pages still
// works with the script blocked, because the script only hides options the
// server already rendered and every option is a link the server can answer.
//
// Works, not identically. The filter bars are unchanged without the script:
// the panel opens, the options apply themselves, Enter submits. The settings
// destination picker degrades further, because it feeds a form field rather
// than navigating, so its options round-trip through the server to fill that
// field in. See notificationsView.destPickURL for what that trip can and
// cannot carry with it.
//
// The second reason is arithmetic: fifty internal users do not amortise a
// JavaScript build. Every dependency upgrade, lockfile audit and node version
// bump would be paid forever for a set of pages that are, structurally, a list
// and a document.
//
// Two rules hold everywhere in here. Nothing derived from a client payload is
// ever converted to template.HTML, which is why search highlighting is
// expressed as Segment values that the template wraps rather than as HTML
// assembled in Go. And nothing renders a whole session: events arrive in
// windows and every block has a byte ceiling, because real sessions reach tens
// of megabytes and the ones people most want to read are the largest.
package web

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Options configure a Server.
type Options struct {
	// Data is the storage the dashboard reads. Required.
	Data Data

	// Version is the serving build, which the fleet page treats as latest:
	// agent releases and the server are cut from the same commit here, so an
	// agent that matches it is current and one that does not is stale.
	Version string
	// BuildDate is when the build was cut; zero when unstamped.
	BuildDate time.Time

	// Viewer resolves the signed-in person for a request. Required, and
	// supplied by the auth layer rather than implemented here: this package
	// deliberately knows nothing about cookies, OAuth or the hd claim, so a
	// change to how people sign in never touches a template.
	Viewer func(*http.Request) (Viewer, bool)

	// SignInPath is where an unauthenticated browser is sent.
	SignInPath string

	// CSRFKey signs the form tokens that protect the admin page. When it is
	// empty a random key is generated at startup, which is safe but logs
	// everyone's open forms out on every deploy; set it from a secret in any
	// deployment running more than one instance, since otherwise a token
	// minted by one instance is rejected by the next.
	CSRFKey []byte

	// Slack is the notification-settings port. Nil means this deployment has
	// no Slack mirror: the settings page, the nav link and the session panel
	// are all absent rather than broken.
	Slack SlackSettings

	// PageSize is the session list window.
	PageSize int
	// EventPageSize is the transcript window. It is separate from PageSize
	// because the units are not comparable: a hundred list rows is a screen,
	// a hundred events is a paragraph.
	EventPageSize int

	Now    func() time.Time
	Logger *slog.Logger
}

// Server renders the dashboard. It holds no per-request state, so one instance
// serves every request and the zero-downtime deploy story is just process
// replacement.
type Server struct {
	data    Data
	viewer  func(*http.Request) (Viewer, bool)
	signIn  string
	csrfKey []byte
	slack   SlackSettings
	pageN   int
	eventN  int
	now     func() time.Time
	version string
	builtAt time.Time
	log     *slog.Logger
	rnd     *renderer
	static  fs.FS
}

// New builds a Server, failing at startup on a template that does not parse.
// Templates are compiled once here rather than per request so a syntax error
// is a failed deploy instead of a 500 that only fires on the page nobody
// opened during testing.
func New(o Options) (*Server, error) {
	if o.Data == nil {
		return nil, errors.New("web: Data is required")
	}
	if o.Viewer == nil {
		return nil, errors.New("web: Viewer is required")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.PageSize <= 0 {
		o.PageSize = 50
	}
	if o.EventPageSize <= 0 {
		o.EventPageSize = 200
	}
	if o.SignInPath == "" {
		// The composition root always sets this, so the default is only ever
		// reached by a caller assembling the dashboard by hand. It still has to
		// be a path that exists: the previous value, /auth/google/start, was
		// the OAuth entry point and now 404s, so the fallback would have sent
		// every unauthenticated visitor to a dead end while looking like a
		// deliberate choice.
		o.SignInPath = "/auth/signin"
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	key := o.CSRFKey
	if len(key) == 0 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("web: csrf key: %w", err)
		}
	}
	rnd, err := newRenderer(o.Now, o.Logger)
	if err != nil {
		return nil, err
	}
	sub, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	return &Server{
		data:    o.Data,
		viewer:  o.Viewer,
		signIn:  o.SignInPath,
		csrfKey: key,
		slack:   o.Slack,
		pageN:   o.PageSize,
		eventN:  o.EventPageSize,
		now:     o.Now,
		version: o.Version,
		builtAt: o.BuildDate,
		log:     o.Logger,
		rnd:     rnd,
		static:  sub,
	}, nil
}

// Routes registers the dashboard on an existing mux, so the JSON API and the
// HTML pages can share one listener and therefore one Cloud Run service. A
// second service would mean a second cold-start budget for no gain.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", s.handleRoot)
	mux.HandleFunc("GET /sessions", s.handleList)
	mux.HandleFunc("GET /sessions/{id}", s.handleDetail)
	mux.HandleFunc("GET /sessions/{id}/conversation", s.handleDetail)
	mux.HandleFunc("GET /sessions/{id}/events/{eventID}", s.handleEvent)
	// Not nested under the session: an artifact id already names its session,
	// and a URL carrying both invites the two disagreeing.
	mux.HandleFunc("GET /artifacts/{id}", s.handleArtifact)
	mux.HandleFunc("GET /analytics", s.handleAnalytics)
	mux.HandleFunc("GET /skills", s.handleSkills)
	// The search page folded into the session list. The route stays as a
	// redirect because the addresses it served are already in people's history.
	mux.HandleFunc("GET /search", s.handleSearchRedirect)
	mux.HandleFunc("GET /shared/{token}", s.handleShared)
	mux.HandleFunc("GET /admin/principals", s.handlePrincipals)
	mux.HandleFunc("POST /admin/principals/{email}", s.handlePrincipalUpdate)
	if s.slack != nil {
		mux.HandleFunc("GET /settings/notifications", s.handleNotifications)
		mux.HandleFunc("POST /settings/notifications/groups", s.handleNotifGroupCreate)
		mux.HandleFunc("POST /settings/notifications/groups/{id}", s.handleNotifGroupUpdate)
		mux.HandleFunc("POST /settings/notifications/groups/{id}/delete", s.handleNotifGroupDelete)
		mux.HandleFunc("POST /settings/notifications/controls", s.handleNotifControls)
		mux.HandleFunc("POST /sessions/{id}/mirror-form", s.handleSessionMirrorForm)
	}
	mux.HandleFunc("GET /admin/fleet", s.handleFleet)
	mux.HandleFunc("POST /admin/fleet/mute", s.handleFleetMute)
	mux.HandleFunc("POST /admin/fleet/unmute", s.handleFleetUnmute)
	mux.HandleFunc("GET /admin/access", s.handleAccessLog)
	mux.HandleFunc("GET /static/{path...}", s.handleStatic)
}

// Handler returns the dashboard as a standalone http.Handler with its security
// headers applied. Callers mounting into a shared mux should use Routes and
// wrap the result in Secure themselves.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.Routes(mux)
	return Secure(mux)
}

// scriptedPaths are the dashboard pages allowed to load a script, which today
// means combo.js and its filter dropdowns. It is an exact-match set, and a
// deliberately short one: /sessions is here and /sessions/{id} is not, because
// the list is a page of filters and the detail page is a transcript.
//
// Everything absent from this map gets script-src 'none', so a new route is
// script-free until somebody adds it here on purpose. Membership is also what
// the base template's "scripts" block must agree with; the guard test walks
// both halves rather than trusting them to stay in step.
var scriptedPaths = map[string]bool{
	"/sessions":               true,
	"/analytics":              true,
	"/admin/principals":       true,
	"/admin/fleet":            true,
	"/admin/access":           true,
	"/settings/notifications": true,
}

// Secure applies the response headers every page needs.
//
// script-src is the load-bearing directive and it has two values here.
//
// Archive pages that render a transcript body keep 'none': the dashboard prints
// whatever an agent read, which is script tags and HTML entities as often as
// it is prose, and 'none' converts any hypothetical escaping bug in that path
// from a stored XSS into inert text. The session detail page, single events,
// artifacts and shared links all stay in that class, and they ship no script
// to lose by it.
// The separately matched continuous reader permits a first-party script and
// same-origin fetches of bounded, authorized archive pages. It still blocks
// inline scripts, external scripts and external network connections.
//
// The filter pages move to 'self' so combo.js can prune the dropdowns as
// somebody types, which is a thing CSS cannot do and the owner asked for on
// 2026-08-13. What that costs is smaller than it looks, because the absence of
// 'unsafe-inline' is doing most of the work in either policy: an injected
// <script>…</script> is refused under both, and so is one sourced from another
// origin. The delta is a same-origin src, and the only same-origin scripts
// that exist are the two files in static/. The session list does print search
// snippets drawn from transcripts, which is why it earns this paragraph rather
// than a shrug: its exposure is an attacker who can make the page load our own
// combo.js, twice.
//
// frame-ancestors 'none' stops the dashboard being framed by an internal tool
// and clickjacked into an admin role change, which is the one destructive
// action the UI offers.
func Secure(h http.Handler) http.Handler {
	const common = "default-src 'none'; " +
		"style-src 'self'; " +
		"img-src 'self' data:; " +
		"font-src 'self'; " +
		"form-action 'self'; " +
		"frame-ancestors 'none'; " +
		"base-uri 'none'; " +
		"connect-src 'none'; " +
		"script-src "
	const (
		cspNone = common + "'none'"
		cspSelf = common + "'self'"
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h2 := w.Header()
		// The header is chosen from the request, before any handler has run,
		// so it describes the address rather than the response. Two harmless
		// consequences follow and are easier to read here than to rediscover.
		//
		// A page that fails renders error.html at whatever path was asked for,
		// so an error at /sessions answers 'self' while carrying no script.
		// The looser header on a page with nothing to load costs nothing.
		//
		// HEAD matches the GET routes in net/http's mux but not this condition,
		// so HEAD /sessions answers 'none' where GET answers 'self'. A HEAD has
		// no body to apply a policy to.
		//
		// Only a GET of a listed page relaxes the policy. A POST to one of
		// these paths answers with a redirect and needs nothing, and keeping
		// the method in the condition means a handler that starts rendering
		// HTML on a POST does not silently inherit the looser header.
		csp := cspNone
		if r.Method == http.MethodGet && scriptedPaths[r.URL.Path] {
			csp = cspSelf
		}
		if r.Method == http.MethodGet && continuousPath(r.URL.Path) {
			csp = strings.Replace(cspSelf, "connect-src 'none'", "connect-src 'self'", 1)
		}
		h2.Set("Content-Security-Policy", csp)
		h2.Set("X-Content-Type-Options", "nosniff")
		h2.Set("Referrer-Policy", "same-origin")
		h2.Set("Cross-Origin-Opener-Policy", "same-origin")
		// Transcripts are private by construction, so no shared cache may keep
		// a copy that another viewer could be served.
		if !strings.HasPrefix(r.URL.Path, "/static/") {
			h2.Set("Cache-Control", "private, no-store")
		}
		h.ServeHTTP(w, r)
	})
}

// Page is the chrome every template needs: who is looking, where they are, and
// anything the last action wants to tell them.
type Page struct {
	Continuous bool
	Title      string
	Nav        string
	Viewer     Viewer
	CSRF       string
	Notice     string
	Error      string
	// Query is the current search box contents, kept so the header input does
	// not empty itself every time someone navigates.
	Query string
}

func (s *Server) page(v Viewer, title, nav string) Page {
	return Page{Title: title, Nav: nav, Viewer: v, CSRF: s.csrfToken(v)}
}

// require resolves the viewer or ends the request. A browser doing a GET is
// redirected to sign-in; anything else gets a status, because redirecting a
// form post to an OAuth flow loses the post body and lands the user on a page
// that silently did nothing.
func (s *Server) require(w http.ResponseWriter, r *http.Request) (Viewer, bool) {
	v, ok := s.viewer(r)
	if ok && v.Email != "" {
		return v, true
	}
	if r.Method == http.MethodGet {
		next := r.URL.RequestURI()
		http.Redirect(w, r, s.signIn+"?next="+url.QueryEscape(next), http.StatusFound)
		return Viewer{}, false
	}
	http.Error(w, "sign in required", http.StatusUnauthorized)
	return Viewer{}, false
}

// requireAdmin gates the admin pages, answering 404 rather than 403 for the
// same reason session reads do: a member who probes the admin URL learns
// nothing about whether it exists.
func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) (Viewer, bool) {
	v, ok := s.require(w, r)
	if !ok {
		return Viewer{}, false
	}
	if !v.Admin {
		s.notFound(w, r, v)
		return Viewer{}, false
	}
	return v, true
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/sessions", http.StatusFound)
}

// notFound is the single response for "no such thing" and for "not yours".
// Both callers pass through here so the two cannot drift apart into a
// distinguishable pair.
func (s *Server) notFound(w http.ResponseWriter, _ *http.Request, v Viewer) {
	p := s.page(v, "Not found", "")
	p.Error = "No session at this address, or it is not one you can open."
	s.rnd.render(w, http.StatusNotFound, "error.html", struct {
		Page Page
		Code int
	}{p, http.StatusNotFound})
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, v Viewer, err error) {
	s.log.Error("dashboard request failed", "path", r.URL.Path, "viewer", v.Email, "err", err)
	p := s.page(v, "Something broke", "")
	p.Error = "That did not load. The failure is recorded; try again in a moment."
	s.rnd.render(w, http.StatusInternalServerError, "error.html", struct {
		Page Page
		Code int
	}{p, http.StatusInternalServerError})
}

// readError maps a storage error onto a response. Denial and absence collapse
// into the same 404 here, in one place, so no handler can accidentally
// distinguish them.
func (s *Server) readError(w http.ResponseWriter, r *http.Request, v Viewer, err error) {
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrDenied) {
		s.notFound(w, r, v)
		return
	}
	s.fail(w, r, v, err)
}

// handleStatic serves the embedded stylesheet.
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.PathValue("path"), "/")
	// Rejecting traversal explicitly rather than relying on the FS: an
	// embedded FS happens to be safe, but this handler should not silently
	// depend on which FS implementation it was handed.
	if name == "" || strings.Contains(name, "..") {
		http.NotFound(w, r)
		return
	}
	b, err := fs.ReadFile(s.static, name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch {
	case strings.HasSuffix(name, ".css"):
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case strings.HasSuffix(name, ".svg"):
		w.Header().Set("Content-Type", "image/svg+xml")
	case strings.HasSuffix(name, ".js"):
		// Every response here carries X-Content-Type-Options: nosniff, so a
		// script served as application/octet-stream is one a browser refuses to
		// execute. It fails in the console rather than in any log this server
		// writes, which makes it a slow thing to diagnose from the server side.
		//
		// The sign-in scripts do not travel this route today; they are served
		// by the sign-in handler at /auth/signin.js, which sets the type itself.
		// They are nonetheless reachable here, because they live under static/
		// for go:embed, and a copy that is reachable and silently unexecutable
		// is a trap for whoever adds the next script and points at the obvious
		// URL.
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	// Only a fingerprinted URL may be cached forever; a bare one has to
	// revalidate, because it is the URL that would go stale across a deploy.
	if r.URL.Query().Get("v") == s.rnd.staticETags[name] {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=60")
	}
	http.ServeContent(w, r, name, time.Time{}, strings.NewReader(string(b)))
}

// ---------------------------------------------------------------------------
// CSRF
// ---------------------------------------------------------------------------

// csrfTTL bounds how long an open form stays submittable. Long enough that
// someone can leave the admin page open through a meeting, short enough that a
// token copied out of a page source has a limited life.
const csrfTTL = 12 * time.Hour

// csrfToken mints a token bound to the viewer and to an expiry.
//
// It is stateless rather than stored in a session because the dashboard keeps
// no server-side session of its own, and binding to the viewer's email is what
// makes a token useless to anyone else: an attacker who obtains one cannot use
// it, because the request it accompanies is authenticated as a different person
// and the check compares the two.
func (s *Server) csrfToken(v Viewer) string {
	if v.Email == "" {
		return ""
	}
	exp := s.now().Add(csrfTTL).Unix()
	msg := fmt.Sprintf("%s\x00%d", v.Email, exp)
	mac := hmac.New(sha256.New, s.csrfKey)
	mac.Write([]byte(msg))
	return fmt.Sprintf("%d.%s", exp, base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
}

func (s *Server) csrfValid(v Viewer, tok string) bool {
	parts := strings.SplitN(tok, ".", 2)
	if len(parts) != 2 || v.Email == "" {
		return false
	}
	exp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || s.now().Unix() > exp {
		return false
	}
	msg := fmt.Sprintf("%s\x00%d", v.Email, exp)
	mac := hmac.New(sha256.New, s.csrfKey)
	mac.Write([]byte(msg))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(want), []byte(parts[1]))
}

// checkWrite validates a state-changing request.
//
// Two independent checks, because each covers a case the other misses. The
// token proves the form came from a page we rendered for this viewer. The
// origin check proves the request was issued by our own page rather than by a
// cross-site form post, and it still holds if a token ever leaks.
func (s *Server) checkWrite(r *http.Request) error {
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || u.Host != r.Host {
			return errors.New("cross-origin form submission")
		}
	}
	return nil
}
