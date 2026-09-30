package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/enroll"
	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// The Firebase project every test verifier is built against, and the ID token
// the client half posts. The token is distinctive so a substring search for it
// in a log or a response body cannot match by accident.
const (
	enrollProjectID = "example-project-12345"
	enrollIDToken   = "IDTOKEN-6f0a58d2-must-never-be-logged"
)

var enrollEpoch = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// enrollVerifier stands in for auth.Verifier. The real one needs a
// Firebase-signed token, and a test that needs a live third party is a test
// that gets skipped.
type enrollVerifier struct {
	id  auth.Identity
	err error
	// saw is the last token handed over, so a test can assert that what was
	// verified is what the client posted rather than anything else.
	saw string
}

func (v *enrollVerifier) Verify(_ context.Context, idToken string) (auth.Identity, error) {
	v.saw = idToken
	if v.err != nil {
		return auth.Identity{}, v.err
	}
	return v.id, nil
}

// The id enrollDevices hands back. A test that wants the store to have honoured
// the laptop's claim asks for this one; anything else stands in for a store that
// declined the claim and minted a row of its own.
const enrollStoreDeviceID = "11111111-2222-3333-4444-555555555555"

// enrollDevices records what enrollment asked the store to write.
type enrollDevices struct {
	err   error
	calls int
	// got is the last device row handed over, and hash the last token hash.
	got     store.Device
	hash    []byte
	expires time.Time
}

func (d *enrollDevices) EnrollDevice(_ context.Context, dev store.Device, hash []byte, expires time.Time) (store.Device, error) {
	d.calls++
	d.got, d.hash, d.expires = dev, hash, expires
	if d.err != nil {
		return store.Device{}, d.err
	}
	dev.ID = enrollStoreDeviceID
	dev.EnrolledAt = enrollEpoch
	return dev, nil
}

func enrollIdentity(email string) auth.Identity {
	at := strings.LastIndex(email, "@")
	return auth.Identity{
		Email:     email,
		Subject:   "1234567890",
		Domain:    email[at+1:],
		Name:      "Dev",
		IssuedAt:  enrollEpoch.Add(-time.Minute),
		ExpiresAt: enrollEpoch.Add(time.Hour),
	}
}

// enrollDomains is the allowlist the enrollment fixtures sit in.
func enrollDomains() []string { return testDomainsOf("dev@example.com", "dev@example.org") }

// newTestEnroll builds an Enroll over a fake verifier and store.
func newTestEnroll(t *testing.T, v *enrollVerifier, devices *enrollDevices, log *slog.Logger) *Enroll {
	t.Helper()
	real, err := auth.NewVerifier(auth.VerifierOptions{ProjectID: enrollProjectID, Domains: enrollDomains()})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	e, err := NewEnroll(EnrollOptions{
		Store:    &store.Store{},
		Verifier: real,
		Domains:  enrollDomains(),
		Now:      func() time.Time { return enrollEpoch },
		Logger:   log,
	})
	if err != nil {
		t.Fatalf("new enroll: %v", err)
	}
	e.verify = v
	e.store = devices
	return e
}

// enrollBody is the payload internal/enroll.complete posts, field for field.
func enrollBody() map[string]string {
	return map[string]string{
		"id_token":      enrollIDToken,
		"hostname":      "dev-mbp",
		"os":            "darwin",
		"arch":          "arm64",
		"agent_version": "0.4.1",
	}
}

func postEnroll(e *Enroll, body any) *http.Response {
	b, _ := json.Marshal(body)
	r := httptest.NewRequest(http.MethodPost, EnrollCompletePath, bytes.NewReader(b))
	r.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.handleComplete(rec, r)
	return rec.Result()
}

func enrollLog() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

func enrollReadBody(t *testing.T, res *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

// enrollQuiet is the logger for tests that assert on behaviour rather than on
// what was recorded.
func enrollQuiet() *slog.Logger { return slog.New(slog.DiscardHandler) }

// Property: the payload this endpoint accepts is the one the client already
// sends, and the response is one the client can read. The two halves ship
// separately, so a field renamed on either side is a fleet that cannot enroll.
func TestEnrollSpeaksTheClientsPayloadAndResponse(t *testing.T) {
	v := &enrollVerifier{id: enrollIdentity("dev@example.com")}
	devices := &enrollDevices{}
	e := newTestEnroll(t, v, devices, enrollQuiet())

	res := postEnroll(e, enrollBody())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", res.StatusCode, enrollReadBody(t, res))
	}

	// Decoded into the client's own type rather than into ours: that is the
	// contract, and a struct that only round-trips through itself proves nothing.
	var out enroll.Result
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("the client cannot read this response: %v", err)
	}
	if out.Email != "dev@example.com" {
		t.Errorf("email = %q", out.Email)
	}
	if out.DeviceID == "" {
		t.Error("no device id, so the client cannot name this machine again")
	}
	if !strings.HasPrefix(out.DeviceToken, auth.TokenPrefix) {
		t.Errorf("token %q does not carry the prefix a secret scanner matches on", out.DeviceToken)
	}
	if !out.ExpiresAt.IsZero() {
		t.Errorf("ExpiresAt = %s, want absent when the credential does not expire", out.ExpiresAt)
	}

	// The token that was verified is the one the client posted, and the device
	// metadata it sent is what was stored.
	if v.saw != enrollIDToken {
		t.Errorf("verified %q, want the token in the request", v.saw)
	}
	if devices.got.Hostname != "dev-mbp" || devices.got.OS != "darwin" ||
		devices.got.Arch != "arm64" || devices.got.AgentVersion != "0.4.1" {
		t.Errorf("device row = %+v, want the fields the client sent", devices.got)
	}
	// The address comes from the verified token, never from the request: the
	// request has no say in whose device this becomes.
	if devices.got.Email != "dev@example.com" {
		t.Errorf("device email = %q, want the address from the verified token", devices.got.Email)
	}
}

// Property: the two halves and the /auth/cli contract agree end to end. The
// client opens the sign-in page, this server's own ParseCLICallback decides
// where that page may post, the page posts there, and the token that comes out
// enrolls a device through this endpoint. Nothing in the chain is a fake except
// the browser and the Firebase signature.
func TestTheAgentEnrollsThroughTheCLIPageContract(t *testing.T) {
	v := &enrollVerifier{id: enrollIdentity("dev@example.com")}
	devices := &enrollDevices{}
	e := newTestEnroll(t, v, devices, enrollQuiet())

	mux := http.NewServeMux()
	e.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// The browser's half, driven by the same rules the real page must follow:
	// it learns where to post from ParseCLICallback and from nowhere else.
	page := func(raw string) error {
		u, err := url.Parse(raw)
		if err != nil {
			return err
		}
		if u.Path != CLIAuthPath {
			t.Errorf("the agent opened %q, want %q", u.Path, CLIAuthPath)
		}
		cb, err := ParseCLICallback(u.Query())
		if err != nil {
			return err
		}
		body, _ := json.Marshal(map[string]string{"id_token": enrollIDToken, "state": cb.State})
		req, err := http.NewRequest(http.MethodPost, cb.URL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", srv.URL)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Errorf("the agent refused the page's callback: %d", res.StatusCode)
		}
		return nil
	}

	flow, err := enroll.New(enroll.Options{Endpoint: srv.URL, Open: page, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// The id this laptop was given the last time it enrolled. It is carried by
	// the client, relayed by this handler and honoured by the store, and this is
	// the only test that runs all three in one process — which is what it takes
	// to notice that one of the three dropped it and started minting a second
	// device row per reinstall.
	const priorDevice = "11111111-2222-4333-8444-555555555555"

	got, err := flow.Run(context.Background(), enroll.Machine{
		DeviceID:     priorDevice,
		Hostname:     "dev-mbp",
		OS:           "darwin",
		Arch:         "arm64",
		AgentVersion: "0.4.1",
	})
	if err != nil {
		t.Fatalf("the agent could not enroll: %v", err)
	}
	if got.Email != "dev@example.com" || !strings.HasPrefix(got.DeviceToken, auth.TokenPrefix) {
		t.Fatalf("result = %+v", got)
	}
	if v.saw != enrollIDToken {
		t.Errorf("verified %q, want the token the page posted", v.saw)
	}
	if devices.got.Hostname != "dev-mbp" || devices.got.AgentVersion != "0.4.1" {
		t.Errorf("device row = %+v", devices.got)
	}
	if devices.got.ID != priorDevice {
		t.Errorf("the store was asked to enroll device %q, want the id the laptop already had (%q)",
			devices.got.ID, priorDevice)
	}
}

// Property: the device id is relayed to the store exactly as the client sent it,
// and its absence is relayed as absence. This handler does not judge the claim —
// only the store can, because only the store can see whose row it names and
// whether it is still live — so a second opinion here would be the copy that
// drifts permissive.
func TestEnrollRelaysTheLaptopsDeviceClaimToTheStore(t *testing.T) {
	cases := map[string]struct {
		claim  string
		want   string
		reused bool
	}{
		"a machine whose claim the store honours": {
			claim: enrollStoreDeviceID, want: enrollStoreDeviceID, reused: true,
		},
		"a first install": {claim: "", want: ""},
		// Nothing here rejects it, because rejecting an enrolment would strand a
		// laptop over a value the store is free to ignore.
		"a claim that is not an id at all": {claim: "not-a-uuid", want: "not-a-uuid"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			devices := &enrollDevices{}
			log, buf := enrollLog()
			e := newTestEnroll(t, &enrollVerifier{id: enrollIdentity("dev@example.com")}, devices, log)

			body := enrollBody()
			if tc.claim != "" {
				body["device_id"] = tc.claim
			}
			res := postEnroll(e, body)
			if res.StatusCode != http.StatusOK {
				t.Fatalf("status = %d: %s", res.StatusCode, enrollReadBody(t, res))
			}
			if devices.got.ID != tc.want {
				t.Errorf("store saw device id %q, want %q", devices.got.ID, tc.want)
			}
			// enrollDevices answers with a fixed id of its own, standing in for a
			// store that declined the claim, so this says whether the log tells a
			// reused row from a fresh one rather than restating the fixture.
			if want := fmt.Sprintf("\"reused_row\":%v", tc.reused); !strings.Contains(buf.String(), want) {
				t.Errorf("enrolment log does not record %s:\n%s", want, buf.String())
			}
		})
	}
}

// Property: the plaintext credential leaves this process exactly once, in the
// response, and only its hash is handed to the store. A dump of device_tokens is
// then not a set of working credentials, and a lost response means the laptop
// re-enrolls rather than somebody recovering a secret.
func TestEnrollStoresOnlyTheHashOfTheToken(t *testing.T) {
	devices := &enrollDevices{}
	e := newTestEnroll(t, &enrollVerifier{id: enrollIdentity("dev@example.com")}, devices, enrollQuiet())

	res := postEnroll(e, enrollBody())
	var out enroll.Result
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if bytes.Contains(devices.hash, []byte(out.DeviceToken)) {
		t.Fatal("the plaintext token was handed to the store")
	}
	// Hashed by auth.HashToken specifically, which is what auth.Devices.Verify
	// will hash the presented credential with. Two spellings of the hash is every
	// credential in the table being dead on arrival.
	if want := auth.HashToken(out.DeviceToken); !bytes.Equal(devices.hash, want) {
		t.Errorf("hash = %x, want auth.HashToken's answer %x", devices.hash, want)
	}
	if len(devices.hash) != sha256.Size {
		t.Errorf("hash is %d bytes, want %d", len(devices.hash), sha256.Size)
	}
}

// Property: two enrollments never produce the same credential.
func TestEnrollMintsADistinctTokenEachTime(t *testing.T) {
	e := newTestEnroll(t, &enrollVerifier{id: enrollIdentity("dev@example.com")}, &enrollDevices{}, enrollQuiet())
	// The limiter is per address; raise the burst so this test measures entropy
	// rather than the rate limit.
	e.limit = newEnrollLimiter(64, time.Minute)

	seen := map[string]bool{}
	for range 16 {
		var out enroll.Result
		res := postEnroll(e, enrollBody())
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if seen[out.DeviceToken] {
			t.Fatalf("token %q was issued twice", out.DeviceToken)
		}
		seen[out.DeviceToken] = true
	}
}

// Property: every way enrollment can fail to establish an identity is one
// answer. The client renders a single sentence for it, and an attacker probing
// this endpoint learns nothing about which check they failed — including, now
// that the token arrives straight from the browser, which of the token's own
// claims was wrong.
func TestEnrollRefusalsAreIndistinguishable(t *testing.T) {
	type answer struct {
		status int
		body   string
		ctype  string
	}
	cases := map[string]func(v *enrollVerifier, d *enrollDevices){
		"the id token did not verify": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = auth.ErrTokenSignature
		},
		"the id token was minted for another project": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = auth.ErrTokenAudience
		},
		"the id token came from another issuer": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = auth.ErrTokenIssuer
		},
		"the id token has expired": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = auth.ErrTokenExpired
		},
		"the account did not sign in with Google": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = auth.ErrSignInProvider
		},
		"the address is not verified": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = auth.ErrEmailUnverified
		},
		"the account is outside our domains": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = auth.ErrDomainNotAllowed
		},
		"the address is outside this endpoint's domains": func(v *enrollVerifier, _ *enrollDevices) {
			// A verifier configured more widely than enrollment is: the token
			// verifies and the address is still refused here.
			v.id = enrollIdentity("dev@evil.example")
		},
		"nobody by that address is enrolled": func(_ *enrollVerifier, d *enrollDevices) {
			d.err = store.ErrNotFound
		},
		"the principal was switched off": func(_ *enrollVerifier, d *enrollDevices) {
			// EnrollDevice's INSERT ... SELECT matches no row for a disabled
			// principal, which is the same ErrNotFound as an absent one.
			d.err = store.ErrNotFound
		},
	}

	var seen []answer
	for name, prepare := range cases {
		t.Run(name, func(t *testing.T) {
			v := &enrollVerifier{id: enrollIdentity("dev@example.com")}
			d := &enrollDevices{}
			prepare(v, d)
			e := newTestEnroll(t, v, d, enrollQuiet())

			res := postEnroll(e, enrollBody())
			seen = append(seen, answer{res.StatusCode, enrollReadBody(t, res), res.Header.Get("Content-Type")})
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
	// 403 specifically: internal/enroll.complete turns exactly this status into
	// "that account is not authorised for loop-sessions", which is the sentence
	// somebody who signed in with the wrong account needs to see.
	if seen[0].status != http.StatusForbidden {
		t.Errorf("refusal status = %d, want 403", seen[0].status)
	}
}

// Property: something broken on our side is not reported as a refusal. The
// client turns 403 into "ask an admin to add you", and sending somebody to an
// admin because Postgres is unreachable wastes two people's afternoon and hides
// the outage.
func TestEnrollAnswersUnavailableWhenSomethingOfOursIsDown(t *testing.T) {
	cases := map[string]error{
		"the store is unreachable":   errors.New("dial postgres: connection refused"),
		"the store failed unusually": errors.New("pq: deadlock detected"),
	}
	for name, storeErr := range cases {
		t.Run(name, func(t *testing.T) {
			d := &enrollDevices{err: storeErr}
			e := newTestEnroll(t, &enrollVerifier{id: enrollIdentity("dev@example.com")}, d, enrollQuiet())

			res := postEnroll(e, enrollBody())
			if res.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503", res.StatusCode)
			}
		})
	}
}

// Property: a malformed request is refused before anything is verified or
// written, and the refusal says nothing about identity.
func TestEnrollRefusesAMalformedRequestWithoutTouchingTheStore(t *testing.T) {
	cases := map[string]any{
		"not an object":       []string{"id_token"},
		"no token":            map[string]string{"hostname": "dev-mbp"},
		"an empty token":      map[string]string{"id_token": "", "hostname": "dev-mbp"},
		"a token of spaces":   map[string]string{"id_token": "   "},
		"empty object":        map[string]string{},
		"the old code fields": map[string]string{"code": "c", "code_verifier": "v", "redirect_uri": "http://127.0.0.1:1/callback"},
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			v := &enrollVerifier{id: enrollIdentity("dev@example.com")}
			d := &enrollDevices{}
			e := newTestEnroll(t, v, d, enrollQuiet())

			res := postEnroll(e, body)
			if res.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", res.StatusCode)
			}
			if v.saw != "" {
				t.Errorf("a malformed request reached the verifier as %q", v.saw)
			}
			if d.calls != 0 {
				t.Error("a malformed request reached the store")
			}
		})
	}
}

// Property: a body larger than the cap is refused rather than buffered. A
// mistargeted upload — this path shares a prefix with /v1/events — must not be
// read into memory before being rejected.
func TestEnrollCapsTheRequestBody(t *testing.T) {
	d := &enrollDevices{}
	e := newTestEnroll(t, &enrollVerifier{id: enrollIdentity("dev@example.com")}, d, enrollQuiet())

	body := enrollBody()
	body["hostname"] = strings.Repeat("a", maxEnrollBodyBytes+1)
	res := postEnroll(e, body)
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", res.StatusCode)
	}
	if d.calls != 0 {
		t.Error("an oversized body reached the store")
	}
}

// Property: no ID token or device token reaches a log. This server holds
// colleagues' transcripts and the log is a second, less guarded copy of whatever
// is written to it; an ID token in it is an hour of somebody's identity, and a
// device token is an upload credential in every downstream log sink.
func TestEnrollNeverLogsACredential(t *testing.T) {
	cases := map[string]func(v *enrollVerifier, d *enrollDevices){
		"successful enrollment": func(*enrollVerifier, *enrollDevices) {},
		"the token did not verify": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = auth.ErrTokenSignature
		},
		"the verifier quoted the token back in its error": func(v *enrollVerifier, _ *enrollDevices) {
			v.err = errors.New("auth: id token " + enrollIDToken + " does not verify")
		},
		"the store is unreachable": func(_ *enrollVerifier, d *enrollDevices) {
			d.err = errors.New("dial postgres: connection refused")
		},
		"nobody by that address is enrolled": func(_ *enrollVerifier, d *enrollDevices) {
			d.err = store.ErrNotFound
		},
	}
	for name, prepare := range cases {
		t.Run(name, func(t *testing.T) {
			log, buf := enrollLog()
			v := &enrollVerifier{id: enrollIdentity("dev@example.com")}
			d := &enrollDevices{}
			prepare(v, d)
			e := newTestEnroll(t, v, d, log)

			res := postEnroll(e, enrollBody())
			body := enrollReadBody(t, res)
			logged := buf.String()

			if strings.Contains(logged, enrollIDToken) || strings.Contains(body, enrollIDToken) {
				t.Errorf("the id token reached the log or the response:\nlog: %s\nbody: %s", logged, body)
			}
			// The issued token is the one secret that legitimately appears in the
			// response, and it must appear nowhere else.
			var out enroll.Result
			if json.Unmarshal([]byte(body), &out) == nil && out.DeviceToken != "" {
				if strings.Contains(logged, out.DeviceToken) {
					t.Errorf("the issued device token was logged:\n%s", logged)
				}
			}
		})
	}
}

// Property: one address cannot mint credentials without limit. This endpoint
// turns a one-hour ID token into a credential that lives until it is revoked,
// and it is the highest-value unauthenticated surface on the service.
func TestEnrollRateLimitsByEmail(t *testing.T) {
	v := &enrollVerifier{id: enrollIdentity("dev@example.com")}
	devices := &enrollDevices{}
	e := newTestEnroll(t, v, devices, enrollQuiet())

	now := enrollEpoch
	e.now = func() time.Time { return now }
	e.limit = newEnrollLimiter(3, time.Hour)

	for i := range 3 {
		if res := postEnroll(e, enrollBody()); res.StatusCode != http.StatusOK {
			t.Fatalf("enrollment %d: status = %d, want 200", i, res.StatusCode)
		}
	}
	res := postEnroll(e, enrollBody())
	if res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", res.StatusCode)
	}
	if devices.calls != 3 {
		t.Errorf("the store was written %d times, want 3", devices.calls)
	}
	if res.Header.Get("Retry-After") == "" {
		t.Error("no Retry-After, so the client has nothing to pace itself by")
	}

	// A different person is unaffected: the limit is per identity, not global,
	// or one noisy laptop stops everybody else enrolling.
	v.id = enrollIdentity("other@example.org")
	if res := postEnroll(e, enrollBody()); res.StatusCode != http.StatusOK {
		t.Errorf("a second address was limited by the first: status %d", res.StatusCode)
	}

	// And the bucket refills.
	v.id = enrollIdentity("dev@example.com")
	now = now.Add(time.Hour)
	if res := postEnroll(e, enrollBody()); res.StatusCode != http.StatusOK {
		t.Errorf("the limit did not refill: status %d", res.StatusCode)
	}
}

// Property: an expiring credential is asked for explicitly, and the default is
// a credential that lives until it is revoked. A laptop offline for two months
// must still be able to deliver what it captured, and an expiry that silently
// strands data is worse than one revocation an admin performs deliberately.
func TestEnrollTokenLifetimeIsOptInAndReachesTheStore(t *testing.T) {
	d := &enrollDevices{}
	e := newTestEnroll(t, &enrollVerifier{id: enrollIdentity("dev@example.com")}, d, enrollQuiet())

	if res := postEnroll(e, enrollBody()); res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if !d.expires.IsZero() {
		t.Errorf("expiry = %s, want none by default", d.expires)
	}

	e.life = 90 * 24 * time.Hour
	res := postEnroll(e, enrollBody())
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	want := enrollEpoch.Add(90 * 24 * time.Hour)
	if !d.expires.Equal(want) {
		t.Errorf("expiry = %s, want %s", d.expires, want)
	}
	var out enroll.Result
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.ExpiresAt.Equal(want) {
		t.Errorf("response ExpiresAt = %s, want %s", out.ExpiresAt, want)
	}
}

// Property: the constructor names every missing requirement at once, and it no
// longer asks for an OAuth client, because there is not one to ask for. A
// server that starts without a verifier and finds out on the first enrollment
// is an outage discovered by whoever is installing the agent.
func TestNewEnrollRefusesAnIncompleteConfiguration(t *testing.T) {
	full := func() EnrollOptions {
		ver, err := auth.NewVerifier(auth.VerifierOptions{ProjectID: enrollProjectID, Domains: enrollDomains()})
		if err != nil {
			t.Fatalf("verifier: %v", err)
		}
		return EnrollOptions{Store: &store.Store{}, Verifier: ver, Domains: enrollDomains()}
	}
	if _, err := NewEnroll(full()); err != nil {
		t.Fatalf("a complete configuration was refused: %v", err)
	}
	cases := map[string]func(o *EnrollOptions){
		"Store":    func(o *EnrollOptions) { o.Store = nil },
		"Verifier": func(o *EnrollOptions) { o.Verifier = nil },
		// No default allowlist: an enrollment that accepted every domain
		// would mint device credentials for anyone with a Google account.
		"Domains": func(o *EnrollOptions) { o.Domains = []string{"", " "} },
	}
	for name, drop := range cases {
		t.Run("missing "+name, func(t *testing.T) {
			o := full()
			drop(&o)
			_, err := NewEnroll(o)
			if err == nil {
				t.Fatal("err = nil")
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("err = %v, want it to name %s", err, name)
			}
		})
	}
	// Both at once, in one message: an operator who fixes one field per deploy
	// cycle pays a deploy per mistake.
	o := full()
	o.Store, o.Verifier = nil, nil
	_, err := NewEnroll(o)
	if err == nil {
		t.Fatal("an empty configuration was accepted")
	}
	if !strings.Contains(err.Error(), "Store") || !strings.Contains(err.Error(), "Verifier") {
		t.Errorf("err = %q, want both missing fields named", err)
	}
}

// Property: the limiter does not accumulate a row per address forever, and the
// sweep never drops somebody who is currently being limited.
func TestEnrollLimiterSweepsOnlyRefilledEntries(t *testing.T) {
	l := newEnrollLimiter(2, time.Minute)
	now := enrollEpoch

	// Somebody who is out of budget right now.
	if !l.allow("busy@example.com", now) || !l.allow("busy@example.com", now) {
		t.Fatal("the first two attempts were refused")
	}
	if l.allow("busy@example.com", now) {
		t.Fatal("a third attempt was allowed")
	}
	// Enough distinct addresses to trip the sweep.
	for i := range enrollLimiterSweepAt + 1 {
		l.allow(string(rune('a'+i%26))+"-"+time.Duration(i).String()+"@example.com", now)
	}
	if l.allow("busy@example.com", now) {
		t.Error("the sweep reset an address that was being limited")
	}
	if len(l.seen) > enrollLimiterSweepAt+2 {
		t.Errorf("the limiter holds %d entries and never sweeps", len(l.seen))
	}
}

// Property: registering the route puts it exactly where the client posts.
// Binaries already on laptops post to this path; it is not ours to move.
func TestEnrollRegistersTheClientsPath(t *testing.T) {
	e := newTestEnroll(t, &enrollVerifier{id: enrollIdentity("dev@example.com")}, &enrollDevices{}, enrollQuiet())
	mux := http.NewServeMux()
	e.Register(mux)

	if EnrollCompletePath != "/v1/enroll/complete" {
		t.Fatalf("the enrollment path moved to %q", EnrollCompletePath)
	}
	b, _ := json.Marshal(enrollBody())
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, EnrollCompletePath, bytes.NewReader(b))
	mux.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200; body %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, EnrollCompletePath, nil))
	if rec.Code == http.StatusOK {
		t.Error("GET enrolled a device")
	}
}

// ---------------------------------------------------------------------------
// The /auth/cli contract
// ---------------------------------------------------------------------------

// Property: the page is only ever configured to post at a port the agent could
// have bound. Below 1024 is a privileged service the agent could not be
// listening on, so a page pointed there is somebody using this endpoint to
// deliver a live ID token to something else on the machine, and anything that
// is not a plain decimal port is not a port at all.
func TestParseCLICallbackRefusesAPortOutsideTheEphemeralRange(t *testing.T) {
	const state = "Zm9vYmFyYmF6cXV4LTEyMzQ1Njc4OQ"
	cases := map[string]struct {
		port string
		ok   bool
	}{
		"the bottom of the range":  {"1024", true},
		"the top of the range":     {"65535", true},
		"a typical ephemeral port": {"54321", true},

		"zero":                   {"0", false},
		"http":                   {"80", false},
		"the last reserved port": {"1023", false},
		"one past the top":       {"65536", false},
		"far past the top":       {"99999", false},
		"negative":               {"-1", false},
		"absent":                 {"", false},
		"not a number":           {"http", false},
		"a port with a comment":  {"54321;evil", false},
		"a port with a path":     {"54321/../callback", false},
		"a port with a space":    {"54321 ", false},
		"a port with a newline":  {"54321\n", false},
		"hexadecimal":            {"0xd431", false},
		"an unsigned overflow":   {"18446744073709551617", false},
		"a float":                {"54321.0", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			q := url.Values{"port": {tc.port}, "state": {state}}
			cb, err := ParseCLICallback(q)
			switch {
			case tc.ok && err != nil:
				t.Fatalf("ParseCLICallback(port=%q) = %v, want a page", tc.port, err)
			case !tc.ok && err == nil:
				t.Fatalf("ParseCLICallback(port=%q) rendered a page posting to %q", tc.port, cb.URL)
			case !tc.ok && !errors.Is(err, errCLIPort):
				t.Fatalf("err = %v, want it to name the port rule", err)
			}
		})
	}
}

// Property: the page posts to the 127.0.0.1 literal and to nothing else. The
// destination is assembled from the parsed integer, so no string a caller
// supplied is ever interpolated into it, and no host, redirect or callback
// parameter in the query has any effect at all. A page that posts a live ID
// token to a host from its own URL is an exfiltration primitive, and "localhost"
// is not an accepted spelling of the literal because it can resolve elsewhere.
func TestParseCLICallbackAlwaysPostsToTheLoopbackLiteral(t *testing.T) {
	const state = "Zm9vYmFyYmF6cXV4LTEyMzQ1Njc4OQ"
	cases := map[string]struct {
		query url.Values
		want  string
	}{
		"a plain port": {
			url.Values{"port": {"54321"}, "state": {state}},
			"http://127.0.0.1:54321/callback",
		},
		"a host somebody added to the query": {
			url.Values{"port": {"54321"}, "state": {state}, "host": {"evil.example"}},
			"http://127.0.0.1:54321/callback",
		},
		"a callback somebody added to the query": {
			url.Values{"port": {"54321"}, "state": {state},
				"callback": {"https://evil.example/steal"}, "redirect_uri": {"https://evil.example"}},
			"http://127.0.0.1:54321/callback",
		},
		"a port written with a leading zero": {
			// Re-serialised from the integer, so the page never carries the
			// caller's spelling of it.
			url.Values{"port": {"01024"}, "state": {state}},
			"http://127.0.0.1:1024/callback",
		},
		"a port written with a sign": {
			url.Values{"port": {"+54321"}, "state": {state}},
			"http://127.0.0.1:54321/callback",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cb, err := ParseCLICallback(tc.query)
			if err != nil {
				t.Fatalf("ParseCLICallback: %v", err)
			}
			if cb.URL != tc.want {
				t.Errorf("URL = %q, want %q", cb.URL, tc.want)
			}
			if cb.State != state {
				t.Errorf("state = %q, want it relayed unchanged", cb.State)
			}
		})
	}

	// And the literal is the only host that can come out of it, whatever is
	// asked for.
	for _, port := range []string{"1024", "8080", "54321", "65535"} {
		cb, err := ParseCLICallback(url.Values{"port": {port}, "state": {state}})
		if err != nil {
			t.Fatalf("port %s: %v", port, err)
		}
		u, err := url.Parse(cb.URL)
		if err != nil {
			t.Fatalf("the URL this page would post to does not parse: %v", err)
		}
		if u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.Path != "/callback" {
			t.Errorf("URL = %q, want the loopback literal", cb.URL)
		}
		if strings.Contains(cb.URL, "localhost") {
			t.Errorf("URL = %q; localhost can resolve off-loopback", cb.URL)
		}
	}
}

// Property: the state is relayed into the page only when it is the shape this
// client mints. It is data a script will post back, so pinning the alphabet
// removes the class of bug where a later template change makes the relay
// injectable, and pinning the length keeps a nonce short enough to guess from
// ever reaching the agent as though it were one.
func TestParseCLICallbackRefusesAStateItWillNotRelay(t *testing.T) {
	cases := map[string]struct {
		state string
		ok    bool
	}{
		"what the client mints": {"UTNuLW9RRlVRc0dGYnl6TWtGWkE0ZlJPTFFDTGtEcHc", true},
		"the shortest allowed":  {strings.Repeat("a", minCLIStateLen), true},
		"the longest allowed":   {strings.Repeat("a", maxCLIStateLen), true},
		"base64url punctuation": {"abcdefghijklmnop-_", true},

		"absent":              {"", false},
		"one character short": {strings.Repeat("a", minCLIStateLen-1), false},
		"one character long":  {strings.Repeat("a", maxCLIStateLen+1), false},
		"a quote":             {`abcdefghijklmnop"`, false},
		"an angle bracket":    {"abcdefghijklmnop<script>", false},
		"a backslash":         {`abcdefghijklmnop\rest`, false},
		"a space":             {"abcdefghijklmnop rest", false},
		"a newline":           {"abcdefghijklmnop\nrest", false},
		"base64 padding":      {"abcdefghijklmnop==", false},
		"a semicolon":         {"abcdefghijklmnop;alert(1)", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			q := url.Values{"port": {"54321"}, "state": {tc.state}}
			cb, err := ParseCLICallback(q)
			switch {
			case tc.ok && err != nil:
				t.Fatalf("ParseCLICallback(state=%q) = %v, want a page", tc.state, err)
			case !tc.ok && err == nil:
				t.Fatalf("ParseCLICallback(state=%q) rendered a page relaying it", tc.state)
			case !tc.ok && !errors.Is(err, errCLIState):
				t.Fatalf("err = %v, want it to name the state rule", err)
			case tc.ok && cb.State != tc.state:
				t.Fatalf("state = %q, want it relayed unchanged", cb.State)
			}
		})
	}
}

// Property: a rejected query value does not put unbounded caller input into a
// log line. The log is the one place unbounded input is reliably read again.
func TestParseCLICallbackDoesNotLogAnUnboundedValue(t *testing.T) {
	_, err := ParseCLICallback(url.Values{"port": {strings.Repeat("9", 4096)}, "state": {"x"}})
	if err == nil {
		t.Fatal("a four-kilobyte port was accepted")
	}
	if len(err.Error()) > 200 {
		t.Errorf("the error is %d bytes long: %.120s...", len(err.Error()), err.Error())
	}
}
