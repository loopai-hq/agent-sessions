// Firebase ID tokens replace the Google OAuth ID tokens this package used to
// verify, and the thing a reader will go looking for is deliberately absent:
// there is no hosted-domain (`hd`) check here, because a Firebase token carries
// no `hd` claim to check. The old verifier could ask Google "is this account
// administered by one of the deployment's Workspace domains" and get a signed
// answer covering tenancy, suspension and the impossibility of a consumer
// account holding the claim. Firebase asserts no such thing. The strongest
// statements available are the `email` claim, `email_verified`, and
// `firebase.sign_in_provider`, so the domain suffix on the address plus the
// roster row read after verification are now the whole control rather than a
// second opinion behind `hd`. Anything that loosens either one loosens the
// only fence there is.
package auth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// There is no package-level list of domains. Every consumer of a domain
// allowlist (VerifierOptions here, SignInOptions and EnrollOptions in
// server/app, the store's source-token binding) is handed the deployment's
// ALLOWED_DOMAINS explicitly and refuses to be built without one, so a
// deployment cannot inherit a default that names somebody else's company.
//
// The list says a domain belongs to the deployment. It does not say the person
// still works there, which is what the principals roster says, so nothing may
// treat a domain match as sufficient on its own.

const (
	// FirebaseCertsURL serves the public halves of the keys Firebase signs ID
	// tokens with. It is not a JWKS: the body is a flat JSON object mapping key
	// id to an x509 certificate in PEM, and a JWKS parser pointed at it parses
	// successfully into nothing, which presents as every sign-in failing with an
	// unknown key id rather than as a parse error.
	FirebaseCertsURL = "https://www.googleapis.com/service_accounts/v1/metadata/x509/securetoken@system.gserviceaccount.com"

	// A Firebase ID token names the project in `iss` as well as in `aud`, so
	// both are pinned from one configured project id. Accepting the issuer
	// without the project suffix would accept a token minted by Firebase for any
	// other project on the platform.
	firebaseIssuerPrefix = "https://securetoken.google.com/"

	// The only sign-in method that proves what this server needs. Firebase Auth
	// on this project can have other providers enabled, and every one of them
	// can end up holding an address in an allowed domain: email/password and email
	// link both take the address from whoever is typing, and an account created
	// that way is indistinguishable from a Workspace one by the time it reaches
	// the `email` claim. Requiring google.com is what makes the address mean the
	// Workspace account rather than a string somebody chose.
	signInProviderGoogle = "google.com"

	// Firebase signs ID tokens with RS256. Accepting a second algorithm would
	// mean accepting whatever the token's own header asks for, which is the
	// classic JWT confusion bug: a token that names "none", or names HS256 and
	// gets verified against a public key used as an HMAC secret.
	signingAlg = "RS256"

	// The certificate document is a few kilobytes. The cap is here so a wrong
	// URL or a hostile response cannot make the server read an unbounded body
	// into memory.
	maxCertsBytes = 1 << 20

	// A Firebase ID token is around a kilobyte. Anything an order of magnitude
	// past that is not a token, and rejecting it before the base64 decode keeps
	// a stream of oversized bodies from becoming a stream of large allocations.
	maxTokenBytes = 8 << 10

	// Anything below this is not a key Google would publish, and accepting a
	// small modulus from a source we did not expect makes forgery cheap.
	minRSAModulusBits = 2048
)

// Verification failures. They are separate values so the server can log which
// check failed, but the HTTP layer must answer all of them identically: telling
// a caller whether the signature or the domain was wrong helps nobody but the
// caller who is guessing.
var (
	ErrTokenMalformed   = errors.New("auth: id token is malformed")
	ErrTokenSignature   = errors.New("auth: id token signature does not verify")
	ErrTokenExpired     = errors.New("auth: id token has expired")
	ErrTokenIssuer      = errors.New("auth: id token was not issued by Firebase for this project")
	ErrTokenAudience    = errors.New("auth: id token was issued for another project")
	ErrEmailUnverified  = errors.New("auth: id token carries no verified email")
	ErrSignInProvider   = errors.New("auth: account did not sign in with Google")
	ErrDomainNotAllowed = errors.New("auth: account is not in an allowed domain")
	ErrUnknownKeyID     = errors.New("auth: id token was signed by an unknown key")
	ErrKeysUnavailable  = errors.New("auth: signing keys could not be fetched")
)

// Identity is what a verified Firebase ID token establishes about a person.
type Identity struct {
	Email string
	// Subject is the Firebase account identifier. It survives a rename, which
	// the email does not, so anything that must follow a person through a change
	// of address should key on this rather than on Email.
	Subject string
	// Domain is the domain of Email, already checked against the allowed list.
	// It is derived from the address rather than asserted by Google: unlike the
	// `hd` claim it used to stand in for, it proves nothing on its own beyond
	// what the address already says.
	Domain    string
	Name      string
	Picture   string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// KeySet is a snapshot of the token-signing keys, indexed by the `kid` value
// that appears in a token header, together with how long the source says the
// snapshot may be reused.
type KeySet struct {
	Keys map[string]*rsa.PublicKey
	// MaxAge is the freshness the source claims, read from Cache-Control on the
	// response. Zero means the source expressed no opinion and the verifier's
	// own RefreshAfter applies.
	MaxAge time.Duration
}

// KeySource supplies Firebase's current token-signing keys.
//
// It is an interface so the verifier can be exercised against a key server the
// test controls: the whole of Verify is otherwise untestable without either
// reaching the public internet or holding a real Firebase-signed token, and a
// test that needs a live third party is a test that gets skipped.
type KeySource interface {
	Keys(ctx context.Context) (KeySet, error)
}

// HTTPKeySource fetches the x509 certificate document over HTTP.
type HTTPKeySource struct {
	// URL defaults to FirebaseCertsURL.
	URL string
	// Client defaults to a client with a short timeout: this fetch sits in the
	// path of an interactive sign-in, and a hung connection to Google should
	// fail the sign-in rather than hold the request open.
	Client *http.Client
}

// Keys implements KeySource.
func (s HTTPKeySource) Keys(ctx context.Context) (KeySet, error) {
	u := s.URL
	if u == "" {
		u = FirebaseCertsURL
	}
	c := s.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return KeySet{}, fmt.Errorf("key source %s: %w", u, err)
	}
	resp, err := c.Do(req)
	if err != nil {
		return KeySet{}, fmt.Errorf("key source %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return KeySet{}, fmt.Errorf("key source %s: %s", u, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCertsBytes))
	if err != nil {
		return KeySet{}, fmt.Errorf("key source %s: %w", u, err)
	}
	keys, err := parseCertificates(body)
	if err != nil {
		return KeySet{}, fmt.Errorf("key source %s: %w", u, err)
	}
	return KeySet{Keys: keys, MaxAge: parseMaxAge(resp.Header.Get("Cache-Control"))}, nil
}

// VerifierOptions configure a Verifier.
type VerifierOptions struct {
	// ProjectID is the Firebase project. It fixes both accepted values at once:
	// `aud` must equal it exactly and `iss` must be the securetoken issuer
	// carrying it, so there is one setting to get right rather than two that can
	// disagree.
	ProjectID string
	// Domains are the email domains a verified address may sit in. Required:
	// at least one, and there is no default, because a verifier that accepts
	// every domain accepts every Google account in the world.
	Domains []string
	// Keys defaults to fetching the certificate document over HTTP.
	Keys KeySource

	// Leeway absorbs clock skew between this server and Google. Zero takes
	// defaultLeeway.
	Leeway time.Duration
	// RefreshAfter is how old the cached key set may be before the next
	// verification refreshes it, used only when the key source names no
	// max-age of its own. Zero takes defaultKeyRefresh.
	RefreshAfter time.Duration
	// RefetchInterval bounds how often an unknown key id may trigger a fetch.
	// Zero takes defaultRefetchInterval.
	RefetchInterval time.Duration
	// RefetchBurst is how many fetches may happen back to back before the
	// interval starts throttling. Zero takes defaultRefetchBurst.
	RefetchBurst int

	// Now is injectable for tests.
	Now func() time.Time
}

const (
	// A minute is generous for skew between two NTP-disciplined machines and
	// far short of the hour an ID token lives, so it cannot meaningfully extend
	// a token's life.
	defaultLeeway = time.Minute
	// The fallback cadence for a key source that names no max-age. Firebase
	// rotates its signing certificates daily and publishes the replacement well
	// before it uses it, so an hour picks a rotation up long before the old
	// certificate disappears, without polling in the request path.
	defaultKeyRefresh = time.Hour
	// The floor on unknown-key-id fetches. A real rotation needs one fetch; a
	// caller inventing key ids gets one fetch a minute no matter how fast they
	// send them.
	defaultRefetchInterval = time.Minute
	// Two, so a genuine rotation that arrives immediately after an unrelated
	// refresh is not delayed by a whole interval.
	defaultRefetchBurst = 2

	// Bounds on the max-age a key source is allowed to impose. A response that
	// says zero would put a fetch in front of every verification, and one that
	// says a year would pin a retired certificate for a year; neither is a
	// number Google sends, so both mean something upstream is wrong and neither
	// should be obeyed literally.
	minKeyTTL = time.Minute
	maxKeyTTL = 24 * time.Hour
)

// Verifier checks Firebase ID tokens.
//
// It is safe for concurrent use and expects to be long-lived: the key cache and
// the refetch budget only mean anything if every request shares one.
type Verifier struct {
	src      KeySource
	audience string
	issuer   string
	domains  []string
	leeway   time.Duration
	refresh  time.Duration
	now      func() time.Time

	// mu guards the cache, which is read on every verification.
	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
	// keyTTL is how long the current snapshot stays usable, taken from the key
	// source's max-age when it names one and from refresh when it does not.
	keyTTL time.Duration

	// fetchMu serialises fetches so that a burst of tokens naming unknown key
	// ids produces one request to Google rather than one per request to us. The
	// budget lives under it because it is only ever consulted here.
	fetchMu sync.Mutex
	budget  bucket
}

// NewVerifier builds a Verifier. It performs no network I/O: the first
// verification populates the cache, so a server whose start-up races Google
// being briefly unreachable still starts.
func NewVerifier(o VerifierOptions) (*Verifier, error) {
	project := strings.TrimSpace(o.ProjectID)
	if project == "" {
		return nil, errors.New("auth: a Firebase project id is required")
	}
	// The project id is interpolated into the issuer URL, so a value carrying a
	// slash or whitespace would change which issuer this server accepts rather
	// than just naming a different project.
	if strings.ContainsAny(project, "/ \t\r\n") {
		return nil, errors.New("auth: Firebase project id contains a character that would change the issuer")
	}
	domains := make([]string, 0, len(o.Domains))
	for _, d := range o.Domains {
		if d = strings.ToLower(strings.TrimSpace(d)); d != "" {
			domains = append(domains, d)
		}
	}
	if len(domains) == 0 {
		return nil, errors.New("auth: at least one allowed email domain is required (Domains)")
	}
	v := &Verifier{
		src:      o.Keys,
		audience: project,
		issuer:   firebaseIssuerPrefix + project,
		domains:  domains,
		leeway:   o.Leeway,
		refresh:  o.RefreshAfter,
		now:      o.Now,
		keys:     map[string]*rsa.PublicKey{},
	}
	if v.src == nil {
		v.src = HTTPKeySource{}
	}
	if v.leeway <= 0 {
		v.leeway = defaultLeeway
	}
	if v.refresh <= 0 {
		v.refresh = defaultKeyRefresh
	}
	v.keyTTL = v.refresh
	if v.now == nil {
		v.now = time.Now
	}
	interval := o.RefetchInterval
	if interval <= 0 {
		interval = defaultRefetchInterval
	}
	burst := o.RefetchBurst
	if burst <= 0 {
		burst = defaultRefetchBurst
	}
	v.budget = newBucket(float64(burst), interval)
	return v, nil
}

// Verify checks a Firebase ID token and returns the identity it establishes.
//
// The order is signature first, then claims. Reading a claim out of a token
// whose signature has not been checked is reading attacker-supplied JSON, and
// the fact that it is base64 encoded fools nobody but the person writing the
// code.
func (v *Verifier) Verify(ctx context.Context, idToken string) (Identity, error) {
	if len(idToken) > maxTokenBytes {
		return Identity{}, fmt.Errorf("%w: %d bytes is not an id token", ErrTokenMalformed, len(idToken))
	}
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return Identity{}, fmt.Errorf("%w: expected three segments", ErrTokenMalformed)
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Identity{}, fmt.Errorf("%w: header is not base64url", ErrTokenMalformed)
	}
	var hdr struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerJSON, &hdr); err != nil {
		return Identity{}, fmt.Errorf("%w: header is not JSON", ErrTokenMalformed)
	}
	// The algorithm comes from configuration, never from the token. A token is
	// not allowed to nominate how it should be checked.
	if hdr.Alg != signingAlg {
		return Identity{}, fmt.Errorf("%w: alg %q is not accepted", ErrTokenMalformed, hdr.Alg)
	}
	if hdr.Kid == "" {
		return Identity{}, fmt.Errorf("%w: header carries no key id", ErrTokenMalformed)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Identity{}, fmt.Errorf("%w: signature is not base64url", ErrTokenMalformed)
	}
	key, err := v.key(ctx, hdr.Kid)
	if err != nil {
		return Identity{}, err
	}
	signed := idToken[:len(parts[0])+1+len(parts[1])]
	digest := sha256.Sum256([]byte(signed))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], sig); err != nil {
		return Identity{}, ErrTokenSignature
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Identity{}, fmt.Errorf("%w: payload is not base64url", ErrTokenMalformed)
	}
	var c claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Identity{}, fmt.Errorf("%w: payload is not JSON", ErrTokenMalformed)
	}
	return v.checkClaims(c)
}

// claims is the subset of a Firebase ID token this server relies on.
type claims struct {
	Iss string `json:"iss"`
	// Aud is a plain string rather than the string-or-list the JWT spec allows.
	// securetoken mints exactly one audience, the project id, so a list here is
	// not a token Firebase produced and being lenient about the shape would only
	// widen what this server will consider.
	Aud           string `json:"aud"`
	Sub           string `json:"sub"`
	Exp           int64  `json:"exp"`
	Iat           int64  `json:"iat"`
	Nbf           int64  `json:"nbf"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Name          string `json:"name"`
	Picture       string `json:"picture"`
	Firebase      struct {
		SignInProvider string `json:"sign_in_provider"`
	} `json:"firebase"`
}

func (v *Verifier) checkClaims(c claims) (Identity, error) {
	if c.Iss != v.issuer {
		return Identity{}, fmt.Errorf("%w: iss=%q", ErrTokenIssuer, c.Iss)
	}
	if c.Aud != v.audience {
		return Identity{}, fmt.Errorf("%w: aud=%q", ErrTokenAudience, c.Aud)
	}

	now := v.now()
	if c.Exp == 0 {
		return Identity{}, fmt.Errorf("%w: no expiry claim", ErrTokenMalformed)
	}
	exp := time.Unix(c.Exp, 0)
	if !now.Before(exp.Add(v.leeway)) {
		return Identity{}, fmt.Errorf("%w at %s", ErrTokenExpired, exp.UTC().Format(time.RFC3339))
	}
	// A token stamped in the future is either a clock problem or a forgery
	// attempt against a verifier that only looks at expiry.
	if c.Iat != 0 && time.Unix(c.Iat, 0).After(now.Add(v.leeway)) {
		return Identity{}, fmt.Errorf("%w: issued in the future", ErrTokenMalformed)
	}
	if c.Nbf != 0 && now.Before(time.Unix(c.Nbf, 0).Add(-v.leeway)) {
		return Identity{}, fmt.Errorf("%w: not valid yet", ErrTokenMalformed)
	}

	// The subject is the only identifier that survives a change of address, and
	// an empty one would let two different people share a principal key in any
	// table that stores it. Firebase always sets it, so an empty value means the
	// token is not one.
	if strings.TrimSpace(c.Sub) == "" {
		return Identity{}, fmt.Errorf("%w: token carries no subject", ErrTokenMalformed)
	}

	email := Normalize(c.Email)
	if email == "" || !c.EmailVerified {
		return Identity{}, ErrEmailUnverified
	}

	// Which provider signed the person in is the check that makes the address
	// mean anything. Firebase will happily mint a token whose `email` is
	// somebody's work address for an account created through password sign-up,
	// an email link or a federated provider we do not administer, and every one
	// of those tokens is issued by our project, signed by our keys and verified
	// by everything above this line. Only google.com routes through the
	// Workspace that owns the domain.
	if c.Firebase.SignInProvider != signInProviderGoogle {
		return Identity{}, fmt.Errorf("%w: sign_in_provider=%q", ErrSignInProvider, c.Firebase.SignInProvider)
	}

	domain, err := emailDomain(email)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: %w", ErrDomainNotAllowed, err)
	}
	if !containsFold(v.domains, domain) {
		return Identity{}, fmt.Errorf("%w: domain=%q", ErrDomainNotAllowed, domain)
	}

	id := Identity{
		Email:     email,
		Subject:   c.Sub,
		Domain:    domain,
		Name:      c.Name,
		Picture:   c.Picture,
		ExpiresAt: exp,
	}
	if c.Iat != 0 {
		id.IssuedAt = time.Unix(c.Iat, 0)
	}
	return id, nil
}

// emailDomain returns the part after the single @ in an address.
//
// It insists on exactly one @ rather than taking the last one. An address with
// two is not something Google issues, and either choice of separator is a guess
// about which half an attacker controls: "victim@example.com@evil.example" reads
// as ours under one rule and theirs under the other.
func emailDomain(email string) (string, error) {
	at := strings.IndexByte(email, '@')
	if at <= 0 || at != strings.LastIndexByte(email, '@') || at == len(email)-1 {
		return "", fmt.Errorf("address %q has no single domain part", email)
	}
	return email[at+1:], nil
}

// key returns the public key for a key id, refreshing the cache when the id is
// unknown or the cache has aged out.
//
// Two things bound the traffic this can generate. Fetches are serialised, so a
// thousand concurrent tokens naming a thousand invented key ids queue behind
// one fetch rather than starting a thousand. And each fetch spends from a token
// bucket, so once the budget is gone an unknown key id is simply rejected until
// the bucket refills. Without the second bound, a caller who can send garbage
// faster than Google can answer turns this server into a load generator aimed
// at Google, and gets our address rate-limited for everyone.
func (v *Verifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	if k, ok := v.cached(kid); ok {
		return k, nil
	}

	v.fetchMu.Lock()
	defer v.fetchMu.Unlock()

	// Another caller may have refreshed while this one waited for the lock.
	if k, ok := v.cached(kid); ok {
		return k, nil
	}

	now := v.now()
	stale, present := v.held(kid)
	if !v.budget.allow(now) {
		// Out of budget. A key we hold but consider stale is still a key Google
		// published, and serving it beats failing every sign-in for a minute.
		if present {
			return stale, nil
		}
		return nil, ErrUnknownKeyID
	}

	set, err := v.src.Keys(ctx)
	if err != nil {
		if present {
			return stale, nil
		}
		return nil, fmt.Errorf("%w: %w", ErrKeysUnavailable, err)
	}
	if len(set.Keys) == 0 {
		// A source that reports success with nothing in it would otherwise
		// replace a working cache with an empty one and sign everybody out.
		if present {
			return stale, nil
		}
		return nil, fmt.Errorf("%w: key source returned no keys", ErrKeysUnavailable)
	}

	v.mu.Lock()
	v.keys = set.Keys
	v.fetchedAt = v.now()
	v.keyTTL = clampKeyTTL(set.MaxAge, v.refresh)
	v.mu.Unlock()

	if k, ok := set.Keys[kid]; ok {
		return k, nil
	}
	return nil, ErrUnknownKeyID
}

// clampKeyTTL turns a source's stated max-age into how long the snapshot may be
// reused. Google's own Cache-Control wins over the configured cadence because
// it is the publisher saying when its answer stops being good: refetching more
// often than that is waste, and refetching less often risks missing a rotation.
func clampKeyTTL(maxAge, fallback time.Duration) time.Duration {
	if maxAge <= 0 {
		return fallback
	}
	if maxAge < minKeyTTL {
		return minKeyTTL
	}
	if maxAge > maxKeyTTL {
		return maxKeyTTL
	}
	return maxAge
}

// cached returns a key only when it is both present and fresh.
func (v *Verifier) cached(kid string) (*rsa.PublicKey, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	k, ok := v.keys[kid]
	if !ok || v.fetchedAt.IsZero() {
		return nil, false
	}
	if v.now().Sub(v.fetchedAt) > v.keyTTL {
		return nil, false
	}
	return k, true
}

// held reports what the cache holds for a key id regardless of freshness.
func (v *Verifier) held(kid string) (*rsa.PublicKey, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	k, ok := v.keys[kid]
	return k, ok
}

func containsFold(haystack []string, needle string) bool {
	for _, h := range haystack {
		if strings.EqualFold(h, needle) {
			return true
		}
	}
	return false
}

// parseCertificates turns Firebase's certificate document into usable RSA keys.
//
// Entries this server cannot use are skipped rather than failing the document,
// because Google adding an entry of a shape we did not anticipate must not
// break every sign-in. A document with nothing usable left is an error: an
// empty key set returned as a success reads downstream as "no key by that id"
// and turns an upstream format change into a silent, total sign-in outage with
// no log line saying why.
func parseCertificates(body []byte) (map[string]*rsa.PublicKey, error) {
	var doc map[string]string
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("certificates: %w", err)
	}
	out := make(map[string]*rsa.PublicKey, len(doc))
	for kid, encoded := range doc {
		if kid == "" {
			continue
		}
		block, _ := pem.Decode([]byte(encoded))
		if block == nil || block.Type != "CERTIFICATE" {
			continue
		}
		// The certificate is self-signed by Google and arrives over TLS from
		// Google, so the transport is what makes it trustworthy; there is no
		// chain here to build. Its NotAfter is deliberately not enforced: it
		// tracks Google's own rotation, Google drops a retired key from the
		// document anyway, and rejecting on it would make a few minutes of
		// clock skew look like a signing-key outage.
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		pub, ok := cert.PublicKey.(*rsa.PublicKey)
		if !ok || pub.N == nil || pub.N.BitLen() < minRSAModulusBits {
			continue
		}
		if pub.E < 3 {
			continue
		}
		out[kid] = pub
	}
	if len(out) == 0 {
		return nil, errors.New("certificates: document contained no usable RSA signing certificates")
	}
	return out, nil
}

// parseMaxAge reads the max-age directive out of a Cache-Control header,
// returning zero when there is none to read. Anything unparseable is treated as
// absent so the verifier falls back to its configured cadence rather than
// inheriting a nonsense freshness from a header.
func parseMaxAge(header string) time.Duration {
	for _, directive := range strings.Split(header, ",") {
		directive = strings.ToLower(strings.TrimSpace(directive))
		value, ok := strings.CutPrefix(directive, "max-age=")
		if !ok {
			continue
		}
		seconds, err := strconv.Atoi(strings.Trim(value, `"`))
		if err != nil || seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	return 0
}

// bucket is a token bucket measured against the injected clock rather than a
// timer, so a test can exhaust and refill it without sleeping. It is not
// internally synchronised: the Verifier only touches it while holding fetchMu.
type bucket struct {
	tokens   float64
	capacity float64
	interval time.Duration
	last     time.Time
}

func newBucket(capacity float64, interval time.Duration) bucket {
	return bucket{tokens: capacity, capacity: capacity, interval: interval}
}

func (b *bucket) allow(now time.Time) bool {
	if b.last.IsZero() {
		b.last = now
	}
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens += float64(elapsed) / float64(b.interval)
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
