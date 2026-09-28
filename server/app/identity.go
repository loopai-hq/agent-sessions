package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/loopai-hq/agent-sessions/server/admin"
	"github.com/loopai-hq/agent-sessions/server/api"
	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/store"
	"github.com/loopai-hq/agent-sessions/server/web"
)

// authPrincipals is the roster read that stands behind every authenticated
// request.
//
// PrincipalLookup rather than Principal because the two outcomes this layer has
// to separate are "nobody by that name" and "a row that says something we could
// not read". A sentinel error collapses the first into the same channel as a
// database failure, and the call site that gets that wrong fails open.
type authPrincipals interface {
	PrincipalLookup(ctx context.Context, email string) (store.Principal, bool, error)
	// EnrolSelf adds a member row for a verified address that has none, and
	// reports whether it created one. It can only add: an existing row, enabled
	// or not, is returned untouched. See store.EnrolSelf for why that bound is
	// the whole security argument.
	EnrolSelf(ctx context.Context, email string) (store.Principal, bool, error)
}

// idTokenVerifier is the ID-token check both sign-in paths perform. It is an
// interface so the dashboard callback and the enrollment exchange can be
// exercised without a Google-signed token; *auth.Verifier satisfies it.
type idTokenVerifier interface {
	Verify(ctx context.Context, idToken string) (auth.Identity, error)
}

// errNoSession is the single answer to every way a request can fail to carry a
// usable identity: no cookie, a forged one, an expired one, an address with no
// roster row, and a roster row that has been switched off.
//
// It is returned bare rather than wrapped with the reason. A wrapped variant
// would be a different error value per cause, and the first caller to render an
// error string turns "your cookie expired" and "your account was disabled" into
// two observably different answers. The reason is worth logging and is worth
// nothing to the person holding the credential.
var errNoSession = errors.New("app: request carries no usable session")

// resolvePrincipal turns the session cookie on a request into the roster row
// behind it, re-reading the roster on every request.
//
// Re-reading is the point. The cookie carries a role because the cookie has to
// carry something, but a role decided at sign-in is a role this server cannot
// re-check, and somebody demoted at lunchtime would keep administrator reads
// until their cookie expired that evening.
//
// The two failure classes stay apart because they have to answer differently at
// the edge: a credential this server will not accept is the caller's problem
// and answers 401, and a roster it could not read is ours and answers 500.
// Reporting the second as the first sends a signed-in person to sign in again,
// which cannot help, and hides a database outage behind a login page.
func resolvePrincipal(r *http.Request, c *auth.Cookies, p authPrincipals) (store.Principal, error) {
	s, err := c.Verify(r)
	if err != nil {
		return store.Principal{}, errNoSession
	}
	email := auth.Normalize(s.Email)
	if email == "" {
		return store.Principal{}, errNoSession
	}
	row, found, err := p.PrincipalLookup(r.Context(), email)
	if err != nil {
		return store.Principal{}, fmt.Errorf("app: read the roster for a session cookie: %w", err)
	}
	if !found {
		return store.Principal{}, errNoSession
	}
	// Any disabled_at at all, rather than one already in the past. The column is
	// written with the server's own clock, so a future value means either a clock
	// problem or a deliberate scheduled removal, and treating either as "still
	// enabled" keeps a revoked colleague reading transcripts.
	if row.DisabledAt != nil {
		return store.Principal{}, errNoSession
	}
	row.Email = auth.Normalize(row.Email)
	if row.Email == "" {
		// A roster row with no address cannot own or authorise anything: every
		// permission predicate downstream joins on it.
		return store.Principal{}, errNoSession
	}
	return row, nil
}

// emailInDomains reports whether an address sits in one of the email domains
// this deployment allows (ALLOWED_DOMAINS).
//
// Set membership, never equality against one string. A Google Workspace can
// carry more than one domain, and which of them a person's canonical address
// is on depends on how their account was created rather than on anything this
// server decides; a single-string comparison would lock out everyone whose
// account happens to sit on the other one.
//
// The check is against the verified address alone. A Firebase ID token
// carries no `hd` claim (see server/auth), so this list and the principals
// roster are the whole of the domain control: the address is what the roster
// keys on, and an account whose address is outside the list is one this
// server cannot attribute anything to.
func emailInDomains(email string, domains []string) bool {
	at := strings.LastIndex(email, "@")
	if at < 0 || at == len(email)-1 {
		return false
	}
	got := strings.ToLower(strings.TrimSpace(email[at+1:]))
	for _, want := range domains {
		if strings.EqualFold(strings.TrimSpace(want), got) {
			return true
		}
	}
	return false
}

// cleanDomains is the allowlist with each entry lower-cased and trimmed and
// the blanks dropped, so a constructor can tell "no domains" from "a list of
// empty strings" and the comparison in emailInDomains sees one spelling.
func cleanDomains(domains []string) []string {
	out := make([]string, 0, len(domains))
	for _, d := range domains {
		if d = strings.ToLower(strings.TrimSpace(d)); d != "" {
			out = append(out, d)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// api
// ---------------------------------------------------------------------------

// NewAPIAuth adapts the dashboard session cookie to the read API's
// authenticator port.
func NewAPIAuth(c *auth.Cookies, s *store.Store) api.Authenticator {
	return apiAuth{cookies: c, roster: s}
}

type apiAuth struct {
	cookies *auth.Cookies
	roster  authPrincipals
}

func (a apiAuth) Authenticate(r *http.Request) (api.Identity, error) {
	p, err := resolvePrincipal(r, a.cookies, a.roster)
	if err != nil {
		if errors.Is(err, errNoSession) {
			// Bare, not wrapped: see errNoSession.
			return api.Identity{}, api.ErrNoIdentity
		}
		return api.Identity{}, err
	}
	return api.Identity{Email: p.Email}, nil
}

// ---------------------------------------------------------------------------
// admin
// ---------------------------------------------------------------------------

// NewAdminAuth adapts the dashboard session cookie to the admin API's
// authenticator port.
//
// It carries no role even though the roster row it just read has one. The admin
// package re-reads the caller's role inside the transaction that performs a
// change, because a demotion landing between the check and the write would
// otherwise be executed by an ex-admin, and a role handed over from here would
// be the copy that goes stale.
func NewAdminAuth(c *auth.Cookies, s *store.Store) admin.Authenticator {
	return adminAuth{cookies: c, roster: s}
}

type adminAuth struct {
	cookies *auth.Cookies
	roster  authPrincipals
}

func (a adminAuth) Authenticate(r *http.Request) (admin.Identity, error) {
	p, err := resolvePrincipal(r, a.cookies, a.roster)
	if err != nil {
		if errors.Is(err, errNoSession) {
			return admin.Identity{}, admin.ErrNoIdentity
		}
		return admin.Identity{}, err
	}
	return admin.Identity{Email: p.Email}, nil
}

// ---------------------------------------------------------------------------
// web
// ---------------------------------------------------------------------------

// NewWebViewer adapts the dashboard session cookie to the HTML server's viewer
// function.
//
// The port has no error channel, so a roster that cannot be read renders as a
// signed-out browser and the person is sent to sign in again, which will not
// fix a database outage. That is the safe direction — the alternative is
// rendering pages for a viewer this server could not confirm — but it is
// invisible from the outside, so the failure is logged here rather than
// swallowed.
func NewWebViewer(c *auth.Cookies, s *store.Store) func(*http.Request) (web.Viewer, bool) {
	v := webViewer{cookies: c, roster: s}
	return v.viewer
}

type webViewer struct {
	cookies *auth.Cookies
	roster  authPrincipals
	// log is nil in production and set by tests that assert on what was
	// recorded; slog.Default() is what the rest of the process writes to.
	log *slog.Logger
}

func (v webViewer) viewer(r *http.Request) (web.Viewer, bool) {
	p, err := resolvePrincipal(r, v.cookies, v.roster)
	if err != nil {
		if !errors.Is(err, errNoSession) {
			v.logger().Error("resolve viewer", "err", err, "path", r.URL.Path)
		}
		return web.Viewer{}, false
	}
	return web.Viewer{
		Email: p.Email,
		Name:  p.DisplayName,
		// Compared against the one role that grants it rather than derived by
		// excluding the other. A role this server does not recognise means the
		// roster says something it cannot interpret, and the only safe reading of
		// that is "not an administrator".
		Admin: p.Role == store.RoleAdmin,
	}, true
}

func (v webViewer) logger() *slog.Logger {
	if v.log != nil {
		return v.log
	}
	return slog.Default()
}
