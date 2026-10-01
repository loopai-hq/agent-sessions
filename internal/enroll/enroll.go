// Package enroll signs a person in and exchanges that for a device credential.
//
// The constraint that shaped this is "no cloud permissions or login" on the
// employee's machine: no gcloud, no cloud console, no IAM grant per person.
// That rules out anything requiring an administrator to provision the
// individual before they can start, but it does not rule out the Google
// account they are already signed into in their browser. So enrollment opens a
// browser, the person clicks the account they already use, and the flow
// completes on a loopback listener only this process can receive on.
//
// There is no OAuth client here, and that absence is the shape of the whole
// file: the browser half signs in with Firebase Auth on a page the server
// serves, and this process never speaks to Google at all: it binds a
// port, opens <endpoint>/auth/cli?port=&state=, and waits for that page to
// post the Firebase ID token back. What used to be an authorization code
// redeemed server-side is now an ID token relayed through the browser, which
// is why the two defences below carry the weight PKCE used to.
//
// First, the page may post only to the 127.0.0.1 literal, and it takes that
// literal from its own code rather than from any host in its query string: a
// page that posts a live ID token to a host somebody handed it is an
// exfiltration primitive. RFC 8252 §8.3 is also why "localhost" is not an
// accepted spelling on either side, here or on the server — the name can
// resolve to an address off the loopback interface, which would put the token
// on whatever network this laptop is attached to.
//
// Second, the state minted here travels to the page and back, and a callback
// whose state does not match is refused, compared in constant time. Any process
// on this machine can reach the listener; the state is what stops one of them
// feeding it a token of its choosing and enrolling a device in somebody else's
// name.
//
// Why not the emailed one-time code originally asked for: NIST SP 800-63B-4
// §3.1.3.1 states plainly that email must not be used as an out-of-band
// authentication channel, and an emailed code proves only mailbox access.
// Signing in with the Google account proves control of an account the company
// administers, and it is fewer steps for the person.
//
// The Firebase token is never persisted. It is spent once against our own
// server, which verifies it and mints our own opaque device credential, so
// revocation is ours to perform instantly rather than something mediated by
// Google's consent and token-limit semantics.
package enroll

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Result is what a successful enrollment yields.
type Result struct {
	Email       string    `json:"email"`
	DeviceID    string    `json:"device_id"`
	DeviceToken string    `json:"device_token"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

// Machine is what this laptop says about itself when it exchanges the ID token.
//
// DeviceID is the only field that is not a fact about the hardware: it is the id
// a previous enrolment on this machine returned, and sending it is what stops a
// second enrolment minting a second device row for one laptop. The server
// updates the row it names instead — and the row is what the fleet page counts,
// so an abandoned one reads as a machine that enrolled and never reported again.
//
// It is empty on a first install and on a machine whose config was deleted, and
// both are ordinary: the server has to be able to enrol a laptop that cannot
// name itself, so this is an optimisation of accuracy, never a requirement.
type Machine struct {
	DeviceID     string
	Hostname     string
	OS           string
	Arch         string
	AgentVersion string
}

// Options configure a flow.
type Options struct {
	// Endpoint is the server base URL. It is the only address this flow talks
	// to: the sign-in page, the callback's permitted origin and the enrollment
	// post all derive from it, so there is one value to get right rather than
	// three that can disagree.
	Endpoint string
	// Open launches a URL in the browser. Injected for tests.
	Open func(string) error
	// HTTP is the client used to post the ID token to our server.
	HTTP *http.Client
	// Timeout bounds the wait for sign-in, so a person who wanders off does
	// not leave a listener bound forever.
	Timeout time.Duration
}

const (
	// signInPath is our own server's page, not Google's. The path is written
	// out rather than imported from server/app because the two halves deploy
	// separately: a laptop running last month's binary has to keep working
	// against today's server, and a shared constant would hide the moment that
	// stops being true behind a compile that still succeeds.
	signInPath = "/auth/cli"
	// completePath is where the verified ID token is exchanged for a device
	// credential, and is fixed for the same reason.
	completePath = "/v1/enroll/complete"
	// callbackPath is the one path this process serves.
	callbackPath = "/callback"

	// Long enough to sign in with a password manager and a second factor,
	// short enough that an abandoned run releases the port the same afternoon.
	defaultTimeout = 5 * time.Minute
	// The exchange gets its own budget rather than whatever is left of the
	// sign-in deadline, so somebody who spends four minutes finding their
	// phone does not then get one second to deliver the token.
	exchangeTimeout = 30 * time.Second

	// A Firebase ID token is around a kilobyte. The cap is here so anything
	// else that finds this port cannot be read into memory unbounded.
	maxCallbackBytes = 16 << 10

	// 256 bits. The state has to be unguessable by another process on this
	// machine that can see the port but not this memory.
	stateBytes = 32
)

// Flow is one enrollment attempt.
type Flow struct {
	opts     Options
	base     string
	origin   string
	state    string
	listener net.Listener
	callback string
}

// New prepares a flow and binds the loopback listener.
//
// The listener is bound before the browser opens so the port is known and
// cannot race: an ephemeral port chosen by the OS means no fixed port to
// collide with another tool, and RFC 8252 sanctions exactly this pattern for
// native apps.
func New(o Options) (*Flow, error) {
	base := strings.TrimSuffix(strings.TrimSpace(o.Endpoint), "/")
	if base == "" {
		return nil, errors.New("enroll: Endpoint is required")
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("enroll: Endpoint must be an absolute http or https URL, got %q", o.Endpoint)
	}
	if o.Open == nil {
		o.Open = openBrowser
	}
	if o.HTTP == nil {
		o.HTTP = &http.Client{Timeout: exchangeTimeout}
	}
	if o.Timeout <= 0 {
		o.Timeout = defaultTimeout
	}

	// The IP literal, never the hostname: see the package comment.
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("enroll: cannot bind a local port for sign-in: %w", err)
	}
	state, err := randomString(stateBytes)
	if err != nil {
		ln.Close()
		return nil, err
	}
	return &Flow{
		opts:  o,
		base:  base,
		state: state,
		// Scheme and host only. This is the Origin a browser will send on the
		// page's post, and it carries no path, so it is compared as a whole
		// string rather than by prefix.
		origin:   u.Scheme + "://" + u.Host,
		listener: ln,
		callback: fmt.Sprintf("http://127.0.0.1:%d%s", ln.Addr().(*net.TCPAddr).Port, callbackPath),
	}, nil
}

// SignInURL is the page this flow opens in the browser.
//
// The port is written as an integer taken from the bound listener, and the
// server re-derives 127.0.0.1:<port> from it rather than trusting any address
// in this URL. Neither half ever accepts a host from the other.
func (f *Flow) SignInURL() string {
	q := url.Values{}
	q.Set("port", strconv.Itoa(f.listener.Addr().(*net.TCPAddr).Port))
	q.Set("state", f.state)
	return f.base + signInPath + "?" + q.Encode()
}

// Run opens the browser, waits for the sign-in page to post an ID token back,
// and exchanges it with our server for a device credential.
func (f *Flow) Run(ctx context.Context, m Machine) (Result, error) {
	defer f.listener.Close()

	waitCtx, cancel := context.WithTimeout(ctx, f.opts.Timeout)
	defer cancel()

	got := make(chan string, 1)
	srv := &http.Server{
		Handler: f.handler(got),
		// A connection that opens and then dribbles would otherwise hold this
		// flow's only listener for as long as it liked.
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() { _ = srv.Serve(f.listener) }()
	defer func() {
		shutCtx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = srv.Shutdown(shutCtx)
	}()

	if err := f.opts.Open(f.SignInURL()); err != nil {
		// A machine with no browser, or a remote shell. Printing the URL keeps
		// the flow usable rather than dead-ending, and the listener is still
		// bound while the person carries it to a browser.
		return Result{}, fmt.Errorf("enroll: could not open a browser (%w); "+
			"open this URL manually to continue:\n\n%s", err, f.SignInURL())
	}

	select {
	case <-waitCtx.Done():
		return Result{}, fmt.Errorf("enroll: timed out waiting for sign-in after %s", f.opts.Timeout)
	case idToken := <-got:
		exchangeCtx, c := context.WithTimeout(ctx, exchangeTimeout)
		defer c()
		return f.complete(exchangeCtx, idToken, m)
	}
}

// handler serves the one path this process exposes to the browser.
//
// A refused callback is answered and then forgotten rather than ending the
// flow. Any process on this machine can post here, so treating the first bad
// post as fatal would let one of them cancel somebody's enrollment at will; the
// timeout is what bounds the wait instead, and a stale tab replaying an old
// state costs nothing.
func (f *Flow) handler(got chan<- string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != callbackPath {
			http.NotFound(w, r)
			return
		}

		// The page is served from our own endpoint, so its post is cross-origin
		// and a browser makes it only if this listener names the origin it may
		// come from. Naming the endpoint rather than "*" is what stops any other
		// page the person has open from reading the answer, and refusing a
		// mismatched Origin outright stops it making the request at all.
		//
		// This is a browser control and nothing more: a native process can send
		// any Origin it likes, or none. The state below is the check that binds
		// a callback to this run.
		origin := r.Header.Get("Origin")
		if origin != "" && origin != f.origin {
			http.Error(w, "unexpected origin", http.StatusForbidden)
			return
		}
		w.Header().Set("Vary", "Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}

		switch r.Method {
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
			// A public https page reaching a loopback address is Private Network
			// Access as far as Chrome is concerned, and it asks the local side to
			// agree before it will send the post at all. Without this the flow
			// fails in one browser only, at the last step, with a console message
			// nobody running an installer will see.
			w.Header().Set("Access-Control-Allow-Private-Network", "true")
			w.WriteHeader(http.StatusNoContent)
			return
		case http.MethodPost:
		default:
			w.Header().Set("Allow", "POST, OPTIONS")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// application/json is deliberately not one of the content types CORS
		// treats as simple, so a browser cannot deliver this post without first
		// making the preflight above — which only our own origin gets an answer
		// to. Accepting text/plain here would open the listener to a form post
		// from any page in the browser.
		if !isJSON(r.Header.Get("Content-Type")) {
			http.Error(w, "expected application/json", http.StatusUnsupportedMediaType)
			return
		}

		var body struct {
			IDToken string `json:"id_token"`
			State   string `json:"state"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, maxCallbackBytes)).Decode(&body); err != nil {
			http.Error(w, "malformed callback", http.StatusBadRequest)
			return
		}
		// Constant time, and never a prefix comparison: the state is the only
		// thing separating this run's callback from one another local process
		// invented, and a comparison that returns early leaks how much of it was
		// already right.
		if subtle.ConstantTimeCompare([]byte(body.State), []byte(f.state)) != 1 {
			http.Error(w, "this sign-in did not match the request that started it", http.StatusForbidden)
			return
		}
		if body.IDToken == "" {
			http.Error(w, "no id token in the callback", http.StatusBadRequest)
			return
		}

		// Non-blocking, because a second valid post must not park a browser
		// connection forever waiting for a receiver that has already moved on.
		select {
		case got <- body.IDToken:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
}

// complete hands the ID token to our server, which verifies it and mints a
// device credential.
//
// The token is spent here and kept nowhere. It is a Google-issued statement
// about a person with an hour to live; the credential that comes back is ours,
// belongs to this machine alone, and can be revoked from the dashboard without
// asking Google anything.
func (f *Flow) complete(ctx context.Context, idToken string, m Machine) (Result, error) {
	fields := map[string]string{
		"id_token":      idToken,
		"hostname":      m.Hostname,
		"os":            m.OS,
		"arch":          m.Arch,
		"agent_version": m.AgentVersion,
	}
	// Left out rather than sent empty when this machine has no id to offer. The
	// field is absent from every request a binary already on a laptop makes, so
	// the server must read absent as "no claim" regardless; sending an empty
	// string would be a second spelling of the same thing for it to get right.
	if m.DeviceID != "" {
		fields["device_id"] = m.DeviceID
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return Result{}, fmt.Errorf("enroll: build the enrollment request: %w", err)
	}
	u := f.base + completePath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(string(body)))
	if err != nil {
		return Result{}, fmt.Errorf("enroll: build the enrollment request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.opts.HTTP.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("enroll: cannot reach %s: %w", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		// The most likely real-world failure: someone signed in with an account
		// that is not in the company directory, or is not yet a principal. Say
		// so plainly rather than surfacing a status code.
		return Result{}, errors.New("enroll: that account is not authorised for loop-sessions. " +
			"Sign in with a Google account on one of your organisation's allowed domains, or ask an admin to add you")
	}
	if resp.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("enroll: server refused enrollment (%s)", resp.Status)
	}

	var out Result
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxCallbackBytes)).Decode(&out); err != nil {
		return Result{}, fmt.Errorf("enroll: unreadable response from server: %w", err)
	}
	if out.Email == "" || out.DeviceToken == "" {
		// The credential is returned exactly once and is stored only as a hash,
		// so there is nothing to fetch again later. A response that parses but
		// carries no credential has to fail here, or the agent persists an empty
		// token and every upload fails afterwards with no visible cause.
		return Result{}, errors.New("enroll: server response was missing the identity or credential")
	}
	return out, nil
}

// Close releases the listener when a flow is abandoned before Run.
func (f *Flow) Close() error { return f.listener.Close() }

// CallbackURL is the loopback address the sign-in page posts to.
func (f *Flow) CallbackURL() string { return f.callback }

// isJSON reports whether a Content-Type names JSON, ignoring any parameters
// such as the charset a fetch() adds of its own accord.
func isJSON(header string) bool {
	media, _, _ := strings.Cut(header, ";")
	return strings.EqualFold(strings.TrimSpace(media), "application/json")
}

func randomString(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("enroll: no entropy available: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// openBrowser launches the default browser.
func openBrowser(u string) error {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd = "open"
	case "windows":
		cmd, args = "rundll32", []string{"url.dll,FileProtocolHandler"}
	default:
		cmd = "xdg-open"
	}
	return exec.CommandContext(context.Background(), cmd, append(args, u)...).Start()
}
