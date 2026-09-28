package app

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// EnrollCompletePath is where internal/enroll sends the Firebase ID token its
// browser half obtained. The path is the client's, not ours to choose: binaries
// already on laptops post here.
const EnrollCompletePath = "/v1/enroll/complete"

// CLIAuthPath is the page the agent opens in a browser. It is the same Firebase
// sign-in the dashboard performs, differing only in where the resulting ID
// token goes: the dashboard exchanges it for a session cookie, and this page
// posts it to the loopback listener the agent named in ?port=.
//
// The page's whole configuration comes from ParseCLICallback, and it must
// render nothing at all when that fails.
const CLIAuthPath = "/auth/cli"

const (
	// The request is one ID token and four short strings. The cap is here so a
	// mistargeted upload cannot be buffered into memory before being rejected.
	maxEnrollBodyBytes = 8 << 10

	// Enrollment is a once-per-laptop event. Five in a burst covers somebody
	// re-running the installer while they work out which account to use, and one
	// every ten minutes after that is far more than any honest client needs.
	defaultEnrollBurst    = 5
	defaultEnrollInterval = 10 * time.Minute

	// Above this many tracked addresses the limiter sweeps entries that have
	// fully refilled. The key space is the size of the company, so this is not a
	// memory bound so much as a way of not keeping a row for somebody who left.
	enrollLimiterSweepAt = 256
)

const (
	// The agent binds an ephemeral port, so anything below 1024 is a port it
	// could not have bound without privileges it does not have — and is
	// therefore somebody asking this page to post a live ID token at a service
	// listening on a well-known port instead.
	minCallbackPort = 1024
	maxCallbackPort = 65535

	// Bounds on the state relayed through the page. internal/enroll mints 32
	// random bytes, which is 43 base64url characters; a value shorter than this
	// floor is not a nonce that resists guessing by another process on the
	// laptop, and one past the ceiling is not this client's at all.
	minCLIStateLen = 16
	maxCLIStateLen = 128

	// The shortest value scrubToken will treat as a credential.
	minScrubbableToken = 16

	// How much of a rejected query value is worth putting in a log line. The
	// value is caller-controlled and unbounded, and a log is the one place
	// unbounded caller input reliably ends up being read.
	maxLoggedQueryValue = 24
)

// Every way enrollment can fail to establish an identity answers with this, and
// with the same status. The client renders its own sentence for 403; see
// internal/enroll.complete, which says "that account is not authorised" and
// covers all of them.
const enrollRefusedMessage = "not authorised"

// The two ways /auth/cli can be asked for a page it must not render. They are
// separate values so the handler can log which rule fired while answering both
// identically: a caller probing the port range learns nothing from the answer.
var (
	errCLIPort  = errors.New("app: /auth/cli was given a port outside the loopback range")
	errCLIState = errors.New("app: /auth/cli was given a state this server will not relay")
)

// EnrollOptions configure an Enroll.
type EnrollOptions struct {
	// Store records the device and the hash of its credential. Required.
	Store *store.Store
	// Verifier checks the Firebase ID token the browser obtained. Required, and
	// the same check the dashboard performs: enrollment mints a credential that
	// outlives any browser session, so it cannot be the laxer of the two paths.
	Verifier *auth.Verifier

	// Domains are the email domains an address may sit in. Required: at
	// least one, with no default.
	Domains []string

	// TokenLifetime expires issued device credentials. Zero means they live until
	// revoked, which is the intended default: a laptop that has been offline for
	// two months must still be able to deliver what it captured, and an expiry
	// that silently strands data is worse than one revocation an admin performs
	// deliberately.
	TokenLifetime time.Duration

	// Burst and Interval tune the per-address rate limit. Zero takes the
	// defaults above.
	Burst    int
	Interval time.Duration

	Now    func() time.Time
	Logger *slog.Logger
}

// deviceEnroller is the store method enrollment needs.
//
// EnrollDevice writes the device row and the token row in one transaction and
// refuses when the principal is absent or disabled, so the roster rule is
// applied once, in the database, inside the same transaction as the write it
// governs. Re-checking it here would be a second copy of a permission rule, and
// the copy that drifts is the permissive one.
type deviceEnroller interface {
	EnrollDevice(ctx context.Context, d store.Device, tokenHash []byte, expiresAt time.Time) (store.Device, error)
}

// Enroll is the device enrollment surface.
type Enroll struct {
	store   deviceEnroller
	verify  idTokenVerifier
	domains []string
	life    time.Duration
	limit   *enrollLimiter
	now     func() time.Time
	log     *slog.Logger
}

// NewEnroll builds an Enroll.
func NewEnroll(o EnrollOptions) (*Enroll, error) {
	var missing []string
	if o.Store == nil {
		missing = append(missing, "Store")
	}
	if o.Verifier == nil {
		missing = append(missing, "Verifier")
	}
	if len(cleanDomains(o.Domains)) == 0 {
		missing = append(missing, "Domains")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("app: enrollment needs %s", strings.Join(missing, ", "))
	}

	e := &Enroll{
		store:   o.Store,
		verify:  o.Verifier,
		domains: cleanDomains(o.Domains),
		life:    o.TokenLifetime,
		now:     o.Now,
		log:     o.Logger,
	}
	if e.now == nil {
		e.now = time.Now
	}
	if e.log == nil {
		e.log = slog.Default()
	}
	burst := o.Burst
	if burst <= 0 {
		burst = defaultEnrollBurst
	}
	interval := o.Interval
	if interval <= 0 {
		interval = defaultEnrollInterval
	}
	e.limit = newEnrollLimiter(float64(burst), interval)
	return e, nil
}

// Register adds the enrollment route to a mux owned by the caller.
func (e *Enroll) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST "+EnrollCompletePath, e.handleComplete)
}

// enrollRequest is the client's payload. The field names are internal/enroll's
// and are reproduced rather than shared: the two halves deploy separately, and a
// struct they both imported would force them to be upgraded together.
type enrollRequest struct {
	IDToken string `json:"id_token"`
	// DeviceID is the id a previous enrolment on that machine returned, and is
	// absent from every binary that shipped before it existed — so absent and
	// empty have to mean the same thing here, which is "this laptop has no row
	// to point at". It is forwarded to the store unexamined: it is a claim about
	// which machine this is, the store is what decides whether to honour it, and
	// a second opinion here would be the copy that drifts.
	DeviceID     string `json:"device_id"`
	Hostname     string `json:"hostname"`
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	AgentVersion string `json:"agent_version"`
}

// enrollResponse mirrors enroll.Result. ExpiresAt is a pointer so that "does not
// expire" is an absent field rather than the year 1.
type enrollResponse struct {
	Email       string     `json:"email"`
	DeviceID    string     `json:"device_id"`
	DeviceToken string     `json:"device_token"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
}

// handleComplete verifies the ID token and mints the device credential.
func (e *Enroll) handleComplete(w http.ResponseWriter, r *http.Request) {
	noStore(w)

	var req enrollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEnrollBodyBytes)).Decode(&req); err != nil {
		enrollError(w, http.StatusBadRequest, "malformed request")
		return
	}
	if strings.TrimSpace(req.IDToken) == "" {
		enrollError(w, http.StatusBadRequest, "malformed request")
		return
	}

	// Everything this endpoint knows about the caller comes from here. There is
	// no longer a code exchange in front of it: the browser obtained the token
	// from Firebase directly, so the signature, the project, the expiry, the
	// verified address and the google.com sign-in provider are the entirety of
	// the proof, and auth.Verifier checks all of them.
	id, err := e.verify.Verify(r.Context(), req.IDToken)
	if err != nil {
		e.refuse(w, r, "verify", req.IDToken, err)
		return
	}
	email := auth.Normalize(id.Email)
	// The Verifier has already matched the address against its own allowed
	// domains. This is enrollment's list, which EnrollOptions lets a caller set
	// separately, and enrollment mints the longer-lived credential of the two
	// paths, so it must not be the laxer one if the two lists ever diverge.
	if !emailInDomains(email, e.domains) {
		e.refuse(w, r, "domain", req.IDToken, errors.New("address is outside the allowed domains"))
		return
	}

	// Limited after verification, because the limit is per identity and there is
	// no identity before the token is checked. Reaching the limiter now costs a
	// live Firebase ID token for an allowed-domain address, which is a far higher bar than
	// the single-use authorization code it used to cost; what is worth bounding
	// is how many long-lived credentials one identity can mint, and that is
	// exactly what this bounds.
	if !e.limit.allow(email, e.now()) {
		e.log.Warn("enrollment rate limited", "email", email)
		w.Header().Set("Retry-After", "600")
		enrollError(w, http.StatusTooManyRequests, "too many enrollment attempts")
		return
	}

	token, err := mintDeviceToken()
	if err != nil {
		e.fail(r, "mint device token", err)
		enrollError(w, http.StatusServiceUnavailable, "temporarily unavailable")
		return
	}
	var expires time.Time
	if e.life > 0 {
		expires = e.now().Add(e.life)
	}

	// Only the hash is handed to the store. The plaintext exists in this process
	// and in the response, and nowhere else ever: a dump of device_tokens is not
	// a set of working credentials, and a lost response means the laptop
	// re-enrolls rather than somebody recovering a secret.
	dev, err := e.store.EnrollDevice(r.Context(), store.Device{
		ID:           req.DeviceID,
		Email:        email,
		Hostname:     req.Hostname,
		OS:           req.OS,
		Arch:         req.Arch,
		AgentVersion: req.AgentVersion,
	}, auth.HashToken(token), expires)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// The roster refusal: no principal by that address, or one that has
			// been switched off. Answered exactly as a failed verification is.
			e.refuse(w, r, "roster", req.IDToken, err)
			return
		}
		e.fail(r, "enroll device", err)
		enrollError(w, http.StatusServiceUnavailable, "temporarily unavailable")
		return
	}

	// reused_row says whether the laptop's claim to an existing device was
	// honoured. It is recorded because a fleet that has quietly stopped reusing
	// rows is exactly what this defect looked like from the outside, and there
	// was nothing in any log that would have said so.
	e.log.Info("device enrolled", "email", email, "device_id", dev.ID,
		"reused_row", req.DeviceID != "" && dev.ID == req.DeviceID,
		"hostname", req.Hostname, "os", req.OS, "arch", req.Arch, "agent_version", req.AgentVersion)

	out := enrollResponse{Email: email, DeviceID: dev.ID, DeviceToken: token}
	if !expires.IsZero() {
		out.ExpiresAt = &expires
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		// The credential is already in the database and is unrecoverable from
		// here. Nothing can be done for this request, but the operator should see
		// that a device exists whose owner never received its token.
		e.fail(r, "write enrollment response", err)
	}
}

// refuse renders the one indistinguishable failure and records which check
// fired.
//
// The cause is scrubbed of the token that was presented rather than trusted not
// to contain it. auth's sentinels name the check and not the credential, but
// that is a property of another package, and this handler is the one place a
// live ID token and an error string are in scope together: an error that ever
// quoted its input would put an hour of somebody's Google identity into every
// downstream log sink, where it would sit unnoticed because the sign-in it came
// from failed.
func (e *Enroll) refuse(w http.ResponseWriter, r *http.Request, stage, presented string, err error) {
	e.log.Warn("enrollment refused", "stage", stage,
		"err", scrubToken(err.Error(), presented), "remote", r.RemoteAddr)
	enrollError(w, http.StatusForbidden, enrollRefusedMessage)
}

// scrubToken removes a presented credential from text bound for a log.
//
// Short values are left alone. Nothing that brief is an ID token, and replacing
// a two-character string would corrupt the message it was meant to protect.
func scrubToken(text, presented string) string {
	if len(presented) < minScrubbableToken {
		return text
	}
	return strings.ReplaceAll(text, presented, "[redacted]")
}

func (e *Enroll) fail(r *http.Request, what string, err error) {
	e.log.Error("enrollment", "op", what, "err", err, "path", r.URL.Path)
}

func enrollError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// mintDeviceToken produces the credential the laptop will present.
//
// The format belongs to auth: the prefix so a secret scanner can recognise a
// token pasted into a repository, and 256 bits behind it so guessing is not a
// strategy. It is assembled here rather than by auth.Devices.Issue because
// store.EnrollDevice writes the device row and the token row in one
// transaction, and Issue would insert a second token row of its own through the
// DeviceStore port. The hash still goes through auth.HashToken, so the value
// stored at enrollment and the value compared at every upload can never be
// computed two different ways.
func mintDeviceToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		// Never fall back to a weaker source. A degraded random source is a reason
		// to refuse enrollment, not to continue with a token somebody can predict.
		return "", fmt.Errorf("app: no entropy available to mint a device token: %w", err)
	}
	return auth.TokenPrefix + base64.RawURLEncoding.EncodeToString(raw), nil
}

// ---------------------------------------------------------------------------
// The /auth/cli contract
// ---------------------------------------------------------------------------

// CLICallback is everything /auth/cli may learn from its own query string.
type CLICallback struct {
	// URL is the one address the page may post the ID token to. It is always
	// http://127.0.0.1:<port>/callback, assembled here from the parsed integer,
	// so no string a caller supplied is ever interpolated into it and the page
	// has no decision left to make about where the token goes.
	URL string
	// Origin is URL's scheme and host, and is the only value the page's
	// Content-Security-Policy may add to connect-src.
	//
	// It is returned rather than left for the handler to derive because the
	// policy on that route otherwise forbids the very post the page exists to
	// make, and the two ways to escape that are both worse than one field:
	// widening connect-src to all of http://127.0.0.1 lets any page served on
	// this route reach every port on the laptop, and dropping the directive
	// lets it reach anywhere at all.
	Origin string
	// State is the agent's nonce, echoed back in the post so the agent can tell
	// its own sign-in from another local process's. This server neither stores
	// nor compares it: the check belongs to whoever minted it, and
	// internal/enroll makes it in constant time.
	State string
}

// ParseCLICallback validates the query a CLI opened /auth/cli with. A page
// configured from anything this refuses must not be rendered at all.
//
// The rules exist because that page holds a live Firebase ID token for the
// person signing in. A page that posts its token to a host taken from its own
// query string is an exfiltration primitive — a link is all it takes — so the
// destination is not taken from the query at all: only a port number is, and
// the host is the 127.0.0.1 literal written above. "localhost" is not accepted
// as a spelling of it anywhere in this flow, per RFC 8252 §8.3, because the
// name can resolve to an address off the loopback interface and put the token
// on the network the laptop is attached to.
func ParseCLICallback(q url.Values) (CLICallback, error) {
	raw := q.Get("port")
	port, err := strconv.Atoi(raw)
	if err != nil || port < minCallbackPort || port > maxCallbackPort {
		return CLICallback{}, fmt.Errorf("%w: %q", errCLIPort, clipValue(raw))
	}
	state := q.Get("state")
	if len(state) < minCLIStateLen || len(state) > maxCLIStateLen || !isBase64URL(state) {
		return CLICallback{}, fmt.Errorf("%w: %d characters", errCLIState, len(state))
	}
	origin := fmt.Sprintf("http://127.0.0.1:%d", port)
	return CLICallback{
		URL:    origin + "/callback",
		Origin: origin,
		State:  state,
	}, nil
}

// isBase64URL reports whether every character could have come out of
// base64.RawURLEncoding.
//
// The state is relayed into a page as data for a script to post back, so the
// alphabet is pinned rather than trusting an escaper downstream to be correct
// forever. A quote, an angle bracket or a backslash cannot appear in the state
// this client mints, so refusing them costs nothing and removes the class of
// bug where a template change makes the relay injectable.
func isBase64URL(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// clipValue bounds a caller-supplied value on its way into a log line. The
// query is unbounded and the log is where unbounded input is actually read.
func clipValue(s string) string {
	if len(s) > maxLoggedQueryValue {
		return s[:maxLoggedQueryValue] + "..."
	}
	return s
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

// enrollLimiter bounds how often one address may mint a device credential.
//
// Keyed on the verified address rather than on the source IP. A laptop enrolls
// from home, from the office and from a phone hotspot, so an IP is not a person;
// the quantity worth bounding is how many long-lived credentials can be minted
// for one identity, and that is the address.
//
// It is per-process. Cloud Run runs several instances, so the fleet-wide rate is
// this multiplied by the instance count, which is a weaker bound than a shared
// counter would give. That is deliberate: the alternative is a Redis this
// service does not otherwise need, added to the one path where a new dependency
// failing means nobody can enroll. Enrollment is a once-per-laptop event, so the
// bound only has to be right to within an order of magnitude, and a handful per
// instance per ten minutes is.
type enrollLimiter struct {
	mu       sync.Mutex
	burst    float64
	interval time.Duration
	seen     map[string]*enrollBucket
}

// enrollBucket is a token bucket measured against an injected clock rather than
// a timer, so a test can exhaust and refill it without sleeping.
type enrollBucket struct {
	tokens float64
	last   time.Time
}

func newEnrollLimiter(burst float64, interval time.Duration) *enrollLimiter {
	return &enrollLimiter{burst: burst, interval: interval, seen: map[string]*enrollBucket{}}
}

func (l *enrollLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.seen[key]
	if !ok {
		l.sweep(now)
		b = &enrollBucket{tokens: l.burst, last: now}
		l.seen[key] = b
	}
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens += float64(elapsed) / float64(l.interval)
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweep drops entries that have refilled to full, which are indistinguishable
// from an address that has never been seen. Called only when a new key arrives,
// so a steady fleet pays nothing for it.
func (l *enrollLimiter) sweep(now time.Time) {
	if len(l.seen) < enrollLimiterSweepAt {
		return
	}
	full := time.Duration(l.burst * float64(l.interval))
	for k, b := range l.seen {
		if now.Sub(b.last) >= full {
			delete(l.seen, k)
		}
	}
}
