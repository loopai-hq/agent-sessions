package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Key generation is the slowest thing in this package's tests, so the pair is
// generated once and shared. Nothing here mutates it.
var testKeys = sync.OnceValue(func() []*rsa.PrivateKey {
	var out []*rsa.PrivateKey
	for range 2 {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		out = append(out, k)
	}
	return out
})

const (
	testProject = "example-project-12345"
	testIssuer  = firebaseIssuerPrefix + testProject
	testKid     = "kid-a"
	testKidB    = "kid-b"
	// testEmail is the address every fixture token carries.
	testEmail = "dev@example.com"
)

// testDomains is the allowlist every test verifier is built with: the fixture
// address's own domain, derived from it so the two cannot drift apart, plus a
// second one so the tests that walk the list exercise more than one entry.
func testDomains() []string { return []string{domainOf(testEmail), "second.example"} }

func domainOf(email string) string { return email[strings.LastIndex(email, "@")+1:] }

var testNow = time.Date(2026, 8, 4, 9, 30, 0, 0, time.UTC)

// certPEM produces what Firebase actually publishes for a signing key: a
// self-signed x509 certificate in PEM, not a JWKS entry.
func certPEM(t *testing.T, pub any, signer crypto.Signer) string {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "securetoken.google.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, signer)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func rsaCertPEM(t *testing.T, key *rsa.PrivateKey) string {
	t.Helper()
	return certPEM(t, &key.PublicKey, key)
}

func certsJSON(t *testing.T, certs map[string]string) []byte {
	t.Helper()
	b, err := json.Marshal(certs)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// certServer stands in for Google's x509 endpoint. It counts requests, so a
// test can assert on how much traffic a verifier generates, and its document
// can be swapped to simulate a rotation.
type certServer struct {
	*httptest.Server
	mu       sync.Mutex
	body     []byte
	cacheCtl string
	hits     atomic.Int64
	failing  atomic.Bool
}

func newCertServer(t *testing.T, certs map[string]string) *certServer {
	t.Helper()
	cs := &certServer{body: certsJSON(t, certs)}
	cs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cs.hits.Add(1)
		if cs.failing.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		cs.mu.Lock()
		body, cacheCtl := cs.body, cs.cacheCtl
		cs.mu.Unlock()
		if cacheCtl != "" {
			w.Header().Set("Cache-Control", cacheCtl)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(cs.Close)
	return cs
}

func (cs *certServer) rotate(t *testing.T, certs map[string]string) {
	t.Helper()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.body = certsJSON(t, certs)
}

func (cs *certServer) serveRaw(body string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.body = []byte(body)
}

// fakeKeySource is used where a test needs to state a max-age directly rather
// than through a header, or to hand back a key set no real endpoint would.
type fakeKeySource struct {
	mu   sync.Mutex
	set  KeySet
	err  error
	hits atomic.Int64
}

func (f *fakeKeySource) Keys(context.Context) (KeySet, error) {
	f.hits.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.set, f.err
}

// signToken builds a signed JWT from a claim set, so each test can state only
// the claim it cares about.
func signToken(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	return signTokenAlg(t, key, kid, "RS256", claims)
}

func signTokenAlg(t *testing.T, key *rsa.PrivateKey, kid, alg string, claims map[string]any) string {
	t.Helper()
	hdr := map[string]any{"alg": alg, "typ": "JWT"}
	if kid != "" {
		hdr["kid"] = kid
	}
	h, err := json.Marshal(hdr)
	if err != nil {
		t.Fatal(err)
	}
	p, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signing := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// validClaims is the shape a real Firebase ID token carries after a Google
// popup sign-in, down to the nested firebase object.
func validClaims() map[string]any {
	return claimsAt(testNow)
}

func claimsAt(now time.Time) map[string]any {
	return map[string]any{
		"iss":            testIssuer,
		"aud":            testProject,
		"sub":            "yH3kQb2s9ZTt0mVn4xJqL8pR1ce2",
		"email":          "Dev@Example.com",
		"email_verified": true,
		"name":           "Dev Person",
		"picture":        "https://lh3.googleusercontent.com/a/x",
		"auth_time":      now.Add(-5 * time.Minute).Unix(),
		"iat":            now.Add(-time.Minute).Unix(),
		"exp":            now.Add(time.Hour).Unix(),
		"firebase": map[string]any{
			"identities": map[string]any{
				"google.com": []string{"108166012345678901234"},
				"email":      []string{"dev@example.com"},
			},
			"sign_in_provider": "google.com",
		},
	}
}

func newTestVerifier(t *testing.T, cs *certServer, tweak func(*VerifierOptions)) (*Verifier, *time.Time) {
	t.Helper()
	clock := testNow
	o := VerifierOptions{
		ProjectID: testProject,
		Domains:   testDomains(),
		Now:       func() time.Time { return clock },
	}
	if cs != nil {
		o.Keys = HTTPKeySource{URL: cs.URL}
	}
	if tweak != nil {
		tweak(&o)
	}
	v, err := NewVerifier(o)
	if err != nil {
		t.Fatal(err)
	}
	return v, &clock
}

func oneCertServer(t *testing.T) (*certServer, []*rsa.PrivateKey) {
	t.Helper()
	keys := testKeys()
	return newCertServer(t, map[string]string{testKid: rsaCertPEM(t, keys[0])}), keys
}

func TestVerifyAcceptsAGoogleSignInFromFirebase(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, _ := newTestVerifier(t, cs, nil)

	id, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, validClaims()))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if id.Email != "dev@example.com" {
		t.Errorf("Email = %q, want the normalised address", id.Email)
	}
	if id.Subject != "yH3kQb2s9ZTt0mVn4xJqL8pR1ce2" {
		t.Errorf("Subject = %q", id.Subject)
	}
	if id.Domain != domainOf(testEmail) {
		t.Errorf("Domain = %q, want the domain of the verified address", id.Domain)
	}
	if id.Name != "Dev Person" {
		t.Errorf("Name = %q", id.Name)
	}
	if !id.ExpiresAt.Equal(time.Unix(testNow.Add(time.Hour).Unix(), 0)) {
		t.Errorf("ExpiresAt = %v", id.ExpiresAt)
	}
	if !id.IssuedAt.Equal(time.Unix(testNow.Add(-time.Minute).Unix(), 0)) {
		t.Errorf("IssuedAt = %v", id.IssuedAt)
	}
}

func TestVerifyAcceptsEveryConfiguredDomain(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, _ := newTestVerifier(t, cs, nil)

	for _, d := range testDomains() {
		c := validClaims()
		c["email"] = "dev@" + d
		id, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, c))
		if err != nil {
			t.Errorf("domain %s rejected: %v", d, err)
			continue
		}
		if id.Domain != d {
			t.Errorf("Domain = %q, want %q", id.Domain, d)
		}
	}
}

func TestVerifyRejects(t *testing.T) {
	cs, keys := oneCertServer(t)

	cases := []struct {
		name  string
		claim func(map[string]any)
		want  error
	}{
		{"a token Firebase minted for another project", func(c map[string]any) {
			c["iss"] = firebaseIssuerPrefix + "someone-elses-project"
		}, ErrTokenIssuer},
		{"a Google OAuth id token rather than a Firebase one", func(c map[string]any) {
			c["iss"] = "https://accounts.google.com"
		}, ErrTokenIssuer},
		{"an issuer that merely starts the same way", func(c map[string]any) {
			c["iss"] = testIssuer + ".evil.example"
		}, ErrTokenIssuer},
		{"a token minted for another project", func(c map[string]any) { c["aud"] = "some-other-project" }, ErrTokenAudience},
		{"an expired token", func(c map[string]any) { c["exp"] = testNow.Add(-2 * time.Hour).Unix() }, ErrTokenExpired},
		{"a token with no expiry at all", func(c map[string]any) { delete(c, "exp") }, ErrTokenMalformed},
		{"a token issued in the future", func(c map[string]any) { c["iat"] = testNow.Add(time.Hour).Unix() }, ErrTokenMalformed},
		{"a token not yet valid", func(c map[string]any) { c["nbf"] = testNow.Add(time.Hour).Unix() }, ErrTokenMalformed},
		{"an unverified email", func(c map[string]any) { c["email_verified"] = false }, ErrEmailUnverified},
		{"no email at all", func(c map[string]any) { delete(c, "email") }, ErrEmailUnverified},
		// Every table that stores a principal keys on the subject somewhere, and
		// an empty one collapses two people onto one row.
		{"an empty subject", func(c map[string]any) { c["sub"] = "" }, ErrTokenMalformed},
		{"no subject at all", func(c map[string]any) { delete(c, "sub") }, ErrTokenMalformed},
		// The whole reason sign_in_provider is checked: each of these is a token
		// our own project signed, carrying a real allowed-domain address, for an account
		// nobody in the Workspace administers.
		{"an account created with a password", func(c map[string]any) {
			c["firebase"] = map[string]any{"sign_in_provider": "password"}
		}, ErrSignInProvider},
		{"an account created with an email link", func(c map[string]any) {
			c["firebase"] = map[string]any{"sign_in_provider": "emailLink"}
		}, ErrSignInProvider},
		{"an anonymous account", func(c map[string]any) {
			c["firebase"] = map[string]any{"sign_in_provider": "anonymous"}
		}, ErrSignInProvider},
		{"a provider that merely looks like Google", func(c map[string]any) {
			c["firebase"] = map[string]any{"sign_in_provider": "google.com.evil.example"}
		}, ErrSignInProvider},
		{"no firebase claim at all", func(c map[string]any) { delete(c, "firebase") }, ErrSignInProvider},
		{"another company's address", func(c map[string]any) { c["email"] = "dev@not-example.com" }, ErrDomainNotAllowed},
		{"a domain that merely contains ours", func(c map[string]any) {
			c["email"] = "dev@example.com.attacker.example"
		}, ErrDomainNotAllowed},
		{"a domain ours is a suffix of", func(c map[string]any) { c["email"] = "dev@evil" + domainOf(testEmail) }, ErrDomainNotAllowed},
		{"an address with no domain part", func(c map[string]any) { c["email"] = "dev" }, ErrDomainNotAllowed},
		{"an address with two domain parts", func(c map[string]any) {
			c["email"] = "dev@example.com@attacker.example"
		}, ErrDomainNotAllowed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := newTestVerifier(t, cs, nil)
			c := validClaims()
			tc.claim(c)
			_, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, c))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestVerifyRejectsForgedTokens(t *testing.T) {
	cs, keys := oneCertServer(t)

	t.Run("signed by a key Google does not publish", func(t *testing.T) {
		v, _ := newTestVerifier(t, cs, nil)
		// Same key id, different key: the attacker copies the header of a real
		// token and signs the body themselves.
		tok := signToken(t, keys[1], testKid, validClaims())
		if _, err := v.Verify(context.Background(), tok); !errors.Is(err, ErrTokenSignature) {
			t.Fatalf("err = %v, want ErrTokenSignature", err)
		}
	})

	t.Run("payload edited after signing", func(t *testing.T) {
		v, _ := newTestVerifier(t, cs, nil)
		tok := signToken(t, keys[0], testKid, validClaims())
		parts := strings.Split(tok, ".")
		tampered := validClaims()
		tampered["email"] = "ceo@example.com"
		b, err := json.Marshal(tampered)
		if err != nil {
			t.Fatal(err)
		}
		parts[1] = base64.RawURLEncoding.EncodeToString(b)
		if _, err := v.Verify(context.Background(), strings.Join(parts, ".")); !errors.Is(err, ErrTokenSignature) {
			t.Fatalf("err = %v, want ErrTokenSignature", err)
		}
	})

	t.Run("alg none", func(t *testing.T) {
		v, _ := newTestVerifier(t, cs, nil)
		b, err := json.Marshal(validClaims())
		if err != nil {
			t.Fatal(err)
		}
		h, err := json.Marshal(map[string]any{"alg": "none", "kid": testKid})
		if err != nil {
			t.Fatal(err)
		}
		tok := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(b) + "."
		if _, err := v.Verify(context.Background(), tok); !errors.Is(err, ErrTokenMalformed) {
			t.Fatalf("err = %v, want ErrTokenMalformed", err)
		}
	})

	t.Run("alg swapped to a symmetric one", func(t *testing.T) {
		v, _ := newTestVerifier(t, cs, nil)
		tok := signTokenAlg(t, keys[0], testKid, "HS256", validClaims())
		if _, err := v.Verify(context.Background(), tok); !errors.Is(err, ErrTokenMalformed) {
			t.Fatalf("err = %v, want ErrTokenMalformed", err)
		}
	})

	t.Run("no key id in the header", func(t *testing.T) {
		v, _ := newTestVerifier(t, cs, nil)
		if _, err := v.Verify(context.Background(), signToken(t, keys[0], "", validClaims())); !errors.Is(err, ErrTokenMalformed) {
			t.Fatalf("err = %v, want ErrTokenMalformed", err)
		}
	})

	t.Run("structurally broken values", func(t *testing.T) {
		v, _ := newTestVerifier(t, cs, nil)
		for _, tok := range []string{"", "a", "a.b", "a.b.c.d", "!!.!!.!!", strings.Repeat("x", 5000)} {
			if _, err := v.Verify(context.Background(), tok); !errors.Is(err, ErrTokenMalformed) {
				t.Errorf("%.20q: err = %v, want ErrTokenMalformed", tok, err)
			}
		}
	})

	t.Run("a body far larger than any id token", func(t *testing.T) {
		v, _ := newTestVerifier(t, cs, nil)
		huge := strings.Repeat("x", maxTokenBytes+1)
		if _, err := v.Verify(context.Background(), huge); !errors.Is(err, ErrTokenMalformed) {
			t.Fatalf("err = %v, want ErrTokenMalformed", err)
		}
	})
}

func TestVerifyRejectsAnUnknownKeyID(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, _ := newTestVerifier(t, cs, nil)

	if _, err := v.Verify(context.Background(), signToken(t, keys[0], "kid-nobody-published", validClaims())); !errors.Is(err, ErrUnknownKeyID) {
		t.Fatalf("err = %v, want ErrUnknownKeyID", err)
	}
}

func TestVerifyRejectsASignedButUndecodablePayload(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, _ := newTestVerifier(t, cs, nil)

	// The signature covers whatever bytes sit between the dots, so a payload
	// that is not base64 at all can still carry a valid signature. The decode
	// therefore has to be checked after the signature rather than assumed.
	h, err := json.Marshal(map[string]any{"alg": "RS256", "kid": testKid})
	if err != nil {
		t.Fatal(err)
	}
	signing := base64.RawURLEncoding.EncodeToString(h) + ".!!!not-base64!!!"
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, keys[0], crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	tok := signing + "." + base64.RawURLEncoding.EncodeToString(sig)
	if _, err := v.Verify(context.Background(), tok); !errors.Is(err, ErrTokenMalformed) {
		t.Fatalf("err = %v, want ErrTokenMalformed", err)
	}
}

func TestVerifyRejectsAnAudienceThatIsNotOneString(t *testing.T) {
	cs, keys := oneCertServer(t)

	// securetoken mints a single string audience. Anything else is not a token
	// it produced, and accepting a list containing the project id would accept a
	// token minted for somebody else that happens to name us as well.
	for _, aud := range []any{[]string{"someone.else", testProject}, 1234, nil} {
		v, _ := newTestVerifier(t, cs, nil)
		c := validClaims()
		c["aud"] = aud
		_, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, c))
		if !errors.Is(err, ErrTokenMalformed) && !errors.Is(err, ErrTokenAudience) {
			t.Errorf("aud=%v: err = %v, want a rejection", aud, err)
		}
	}
}

func TestHTTPKeySourceReadsX509CertificatesRatherThanAJWKS(t *testing.T) {
	keys := testKeys()
	cs := newCertServer(t, map[string]string{
		testKid:  rsaCertPEM(t, keys[0]),
		testKidB: rsaCertPEM(t, keys[1]),
	})

	set, err := (HTTPKeySource{URL: cs.URL}).Keys(context.Background())
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(set.Keys) != 2 {
		t.Fatalf("parsed %d keys, want 2", len(set.Keys))
	}
	if set.Keys[testKid].N.Cmp(keys[0].N) != 0 || set.Keys[testKid].E != keys[0].E {
		t.Error("the certificate did not round trip to the key that signs tokens")
	}
}

func TestHTTPKeySourceReportsTheCacheControlMaxAge(t *testing.T) {
	keys := testKeys()
	cs := newCertServer(t, map[string]string{testKid: rsaCertPEM(t, keys[0])})
	cs.cacheCtl = "public, max-age=19335, must-revalidate, no-transform"

	set, err := (HTTPKeySource{URL: cs.URL}).Keys(context.Background())
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if set.MaxAge != 19335*time.Second {
		t.Fatalf("MaxAge = %v, want the header's 19335s", set.MaxAge)
	}
}

func TestParseMaxAge(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"the directive Google actually sends", "public, max-age=19335, must-revalidate, no-transform", 19335 * time.Second},
		{"a header of only the directive", "max-age=60", time.Minute},
		{"a quoted value", `max-age="60"`, time.Minute},
		{"an upper case directive", "MAX-AGE=60", time.Minute},
		{"no max-age at all", "no-cache, no-store", 0},
		{"an empty header", "", 0},
		{"a value that is not a number", "max-age=soon", 0},
		{"a zero which would put a fetch in front of every request", "max-age=0", 0},
		{"a negative value", "max-age=-5", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseMaxAge(tc.header); got != tc.want {
				t.Errorf("parseMaxAge(%q) = %v, want %v", tc.header, got, tc.want)
			}
		})
	}
}

func TestHTTPKeySourceRejectsABadResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	if _, err := (HTTPKeySource{URL: srv.URL}).Keys(context.Background()); err == nil {
		t.Error("a 500 from the key endpoint must be an error")
	}
}

func TestACertificateThatDoesNotParseIsAnErrorNotAnEmptyKeySet(t *testing.T) {
	keys := testKeys()
	cs := newCertServer(t, map[string]string{testKid: rsaCertPEM(t, keys[0])})
	cs.serveRaw(`{"` + testKid + `": "-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----\n"}`)

	if _, err := (HTTPKeySource{URL: cs.URL}).Keys(context.Background()); err == nil {
		t.Fatal("an unparseable certificate must not come back as a usable key set")
	}

	// And the verifier reports the outage rather than an unknown key id, which
	// is what an empty key set would look like from here.
	v, _ := newTestVerifier(t, cs, nil)
	_, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, validClaims()))
	if !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("err = %v, want ErrKeysUnavailable", err)
	}
}

func TestParseCertificatesSkipsEntriesItCannotUse(t *testing.T) {
	keys := testKeys()
	good := rsaCertPEM(t, keys[0])

	small, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	unusable := map[string]string{
		"an empty value":                 "",
		"a value that is not PEM":        "MIIDdTCCAl2gAwIBAgIJ",
		"a PEM block of another type":    string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("x")})),
		"a PEM body that is not x509":    string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not a certificate")})),
		"a modulus too small to trust":   certPEM(t, &small.PublicKey, small),
		"a certificate for another algo": certPEM(t, &ec.PublicKey, ec),
	}
	for name, body := range unusable {
		t.Run(name, func(t *testing.T) {
			if _, err := parseCertificates(certsJSON(t, map[string]string{"x": body})); err == nil {
				t.Error("parsed as a usable key set")
			}
		})
	}

	t.Run("documents that are not the shape Firebase publishes", func(t *testing.T) {
		for _, body := range []string{`{}`, `not json`, `[]`, `{"keys":[{"kid":"x"}]}`} {
			if _, err := parseCertificates([]byte(body)); err == nil {
				t.Errorf("%q parsed as a key set", body)
			}
		}
	})

	t.Run("one unusable entry does not discard the usable ones", func(t *testing.T) {
		parsed, err := parseCertificates(certsJSON(t, map[string]string{
			"broken": "not a certificate",
			testKid:  good,
		}))
		if err != nil {
			t.Fatalf("parseCertificates: %v", err)
		}
		if _, ok := parsed[testKid]; !ok || len(parsed) != 1 {
			t.Errorf("parsed %d keys, want just the usable one", len(parsed))
		}
	})
}

func TestKeysAreCachedByKeyID(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, _ := newTestVerifier(t, cs, nil)

	for i := range 20 {
		if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, validClaims())); err != nil {
			t.Fatalf("Verify %d: %v", i, err)
		}
	}
	if got := cs.hits.Load(); got != 1 {
		t.Fatalf("fetched the certificate document %d times, want 1", got)
	}
}

func TestUnknownKeyIDRefetchesOnce(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, _ := newTestVerifier(t, cs, nil)

	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, validClaims())); err != nil {
		t.Fatalf("first Verify: %v", err)
	}

	// Firebase rotates: a new key id appears, signed by a key we have never seen.
	cs.rotate(t, map[string]string{testKid: rsaCertPEM(t, keys[0]), testKidB: rsaCertPEM(t, keys[1])})
	if _, err := v.Verify(context.Background(), signToken(t, keys[1], testKidB, validClaims())); err != nil {
		t.Fatalf("after rotation: %v", err)
	}
	if got := cs.hits.Load(); got != 2 {
		t.Fatalf("fetched %d times, want 2 (cold start plus the rotation)", got)
	}
	// The rotated key is now cached like any other.
	if _, err := v.Verify(context.Background(), signToken(t, keys[1], testKidB, validClaims())); err != nil {
		t.Fatalf("second use of the rotated key: %v", err)
	}
	if got := cs.hits.Load(); got != 2 {
		t.Fatalf("fetched %d times, want the rotated key to have been cached", got)
	}
}

func TestNonsenseKeyIDsCannotFloodGoogle(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, clock := newTestVerifier(t, cs, func(o *VerifierOptions) {
		o.RefetchInterval = time.Minute
		o.RefetchBurst = 2
	})

	// One thousand tokens, each naming a key id that has never existed.
	for i := range 1000 {
		tok := signToken(t, keys[0], fmt.Sprintf("garbage-%d", i), validClaims())
		if _, err := v.Verify(context.Background(), tok); !errors.Is(err, ErrUnknownKeyID) {
			t.Fatalf("token %d: err = %v, want ErrUnknownKeyID", i, err)
		}
	}
	if got := cs.hits.Load(); got != 2 {
		t.Fatalf("fetched %d times for 1000 unknown key ids, want the burst of 2", got)
	}

	// The budget refills with time, so a genuine rotation later still works.
	*clock = clock.Add(2 * time.Minute)
	cs.rotate(t, map[string]string{testKidB: rsaCertPEM(t, keys[1])})
	if _, err := v.Verify(context.Background(), signToken(t, keys[1], testKidB, claimsAt(*clock))); err != nil {
		t.Fatalf("after the budget refilled: %v", err)
	}
}

func TestStaleKeysSurviveAnOutage(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, clock := newTestVerifier(t, cs, func(o *VerifierOptions) {
		o.RefreshAfter = time.Minute
		o.RefetchInterval = time.Second
	})

	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, validClaims())); err != nil {
		t.Fatalf("first Verify: %v", err)
	}

	// The cache ages out and Google is unreachable. Signing people out for the
	// duration of somebody else's outage is the wrong answer when we still hold
	// the key that verifies their token.
	*clock = clock.Add(10 * time.Minute)
	cs.failing.Store(true)
	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, claimsAt(*clock))); err != nil {
		t.Fatalf("stale cache should still verify: %v", err)
	}

	// An unknown key id during the outage has nothing to fall back on.
	if _, err := v.Verify(context.Background(), signToken(t, keys[1], testKidB, claimsAt(*clock))); err == nil {
		t.Fatal("an unknown key id during an outage must not verify")
	}
}

func TestASourceReportingNoKeysDoesNotDiscardTheWorkingCache(t *testing.T) {
	keys := testKeys()
	src := &fakeKeySource{set: KeySet{Keys: map[string]*rsa.PublicKey{testKid: &keys[0].PublicKey}}}
	v, clock := newTestVerifier(t, nil, func(o *VerifierOptions) {
		o.Keys = src
		o.RefreshAfter = time.Minute
	})

	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, validClaims())); err != nil {
		t.Fatalf("first Verify: %v", err)
	}

	// A key source that answers with an empty set is a bug somewhere upstream,
	// not an instruction to forget every key we hold.
	src.mu.Lock()
	src.set = KeySet{Keys: map[string]*rsa.PublicKey{}}
	src.mu.Unlock()
	*clock = clock.Add(10 * time.Minute)
	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, claimsAt(*clock))); err != nil {
		t.Fatalf("an empty key set must not sign everybody out: %v", err)
	}
}

func TestTheSourcesMaxAgeDecidesWhenKeysAreRefetched(t *testing.T) {
	keys := testKeys()
	src := &fakeKeySource{set: KeySet{
		Keys:   map[string]*rsa.PublicKey{testKid: &keys[0].PublicKey},
		MaxAge: 2 * time.Hour,
	}}
	v, clock := newTestVerifier(t, nil, func(o *VerifierOptions) {
		o.Keys = src
		// Deliberately far shorter than the max-age: what Google says about its
		// own document wins over our fallback cadence.
		o.RefreshAfter = time.Minute
	})

	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, validClaims())); err != nil {
		t.Fatalf("first Verify: %v", err)
	}

	*clock = clock.Add(90 * time.Minute)
	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, claimsAt(*clock))); err != nil {
		t.Fatalf("within the stated max-age: %v", err)
	}
	if got := src.hits.Load(); got != 1 {
		t.Fatalf("fetched %d times inside the stated max-age, want 1", got)
	}

	*clock = clock.Add(45 * time.Minute)
	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, claimsAt(*clock))); err != nil {
		t.Fatalf("past the stated max-age: %v", err)
	}
	if got := src.hits.Load(); got != 2 {
		t.Fatalf("fetched %d times past the stated max-age, want 2", got)
	}
}

func TestAnAbsurdMaxAgeCannotPinAKeyForever(t *testing.T) {
	keys := testKeys()
	src := &fakeKeySource{set: KeySet{
		Keys:   map[string]*rsa.PublicKey{testKid: &keys[0].PublicKey},
		MaxAge: 100000 * time.Hour,
	}}
	v, clock := newTestVerifier(t, nil, func(o *VerifierOptions) { o.Keys = src })

	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, validClaims())); err != nil {
		t.Fatalf("first Verify: %v", err)
	}
	*clock = clock.Add(maxKeyTTL + time.Hour)
	if _, err := v.Verify(context.Background(), signToken(t, keys[0], testKid, claimsAt(*clock))); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got := src.hits.Load(); got != 2 {
		t.Fatalf("fetched %d times, want a refetch once the ceiling passed", got)
	}
}

func TestClampKeyTTL(t *testing.T) {
	cases := []struct {
		name     string
		maxAge   time.Duration
		fallback time.Duration
		want     time.Duration
	}{
		{"no opinion from the source falls back to the configured cadence", 0, time.Hour, time.Hour},
		{"a stated max-age is honoured as given", 6 * time.Hour, time.Hour, 6 * time.Hour},
		{"a max-age below the floor cannot put a fetch on every request", time.Second, time.Hour, minKeyTTL},
		{"a max-age above the ceiling cannot pin a retired key", 365 * 24 * time.Hour, time.Hour, maxKeyTTL},
		{"a negative max-age is treated as no opinion", -time.Hour, time.Hour, time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampKeyTTL(tc.maxAge, tc.fallback); got != tc.want {
				t.Errorf("clampKeyTTL(%v, %v) = %v, want %v", tc.maxAge, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestConcurrentVerifyFetchesOnce(t *testing.T) {
	cs, keys := oneCertServer(t)
	v, _ := newTestVerifier(t, cs, nil)

	tok := signToken(t, keys[0], testKid, validClaims())
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := v.Verify(context.Background(), tok); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Verify: %v", err)
	}
	if got := cs.hits.Load(); got != 1 {
		t.Fatalf("fetched %d times from 50 concurrent verifications, want 1", got)
	}
}

func TestNewVerifierRequiresAUsableProjectID(t *testing.T) {
	cases := []struct {
		name    string
		project string
	}{
		{"no project id accepts a token minted by any Firebase project", ""},
		{"a blank project id is not a project id", "   "},
		// The id is interpolated into the issuer, so these would silently change
		// which issuer the server trusts.
		{"a project id carrying a path separator", testProject + "/../evil"},
		{"a project id carrying whitespace", "example project"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewVerifier(VerifierOptions{ProjectID: tc.project, Domains: testDomains()}); err == nil {
				t.Error("NewVerifier accepted it")
			}
		})
	}

	v, err := NewVerifier(VerifierOptions{ProjectID: " " + testProject + " ", Domains: testDomains()})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if v.issuer != testIssuer || v.audience != testProject {
		t.Errorf("issuer = %q, audience = %q, want the trimmed project", v.issuer, v.audience)
	}
}

// TestNewVerifierRequiresAnAllowlist pins the absence of a default domain
// list. A verifier built with no domains would accept every Google account in
// the world, and a package-level fallback naming one organisation's domains
// would ship that organisation's allowlist to every other deployment; both
// are refused at construction, where an operator sees it.
func TestNewVerifierRequiresAnAllowlist(t *testing.T) {
	for name, domains := range map[string][]string{
		"nil":           nil,
		"empty":         {},
		"only blanks":   {"", "  ", "\t"},
		"blank and nil": {""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewVerifier(VerifierOptions{ProjectID: testProject, Domains: domains})
			if err == nil {
				t.Fatal("NewVerifier accepted a configuration with no allowed domain")
			}
			if !strings.Contains(err.Error(), "Domains") {
				t.Errorf("err = %v, want it to name Domains", err)
			}
		})
	}
	v, err := NewVerifier(VerifierOptions{ProjectID: testProject, Domains: []string{" Second.EXAMPLE ", ""}})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if len(v.domains) != 1 || v.domains[0] != "second.example" {
		t.Errorf("domains = %q, want the one entry trimmed and lower-cased", v.domains)
	}
}
