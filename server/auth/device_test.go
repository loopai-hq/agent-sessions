package auth

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDeviceStore stands in for the Postgres tables. It is deliberately dumb:
// the interesting behaviour under test is what Devices decides, not what SQL
// does, and a fake that enforces its own rules would hide the ones this package
// is responsible for.
type fakeDeviceStore struct {
	mu      sync.Mutex
	rows    map[string]TokenRow // keyed by hex of the token hash
	written []TokenRecord
	touched []string
	revoked []string

	insertErr error
	lookupErr error
	touchErr  error
	revokeErr error
}

func newFakeStore() *fakeDeviceStore {
	return &fakeDeviceStore{rows: map[string]TokenRow{}}
}

func (f *fakeDeviceStore) InsertDeviceToken(_ context.Context, rec TokenRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertErr != nil {
		return f.insertErr
	}
	f.written = append(f.written, rec)
	f.rows[hex.EncodeToString(rec.TokenHash)] = TokenRow{
		TokenID:   rec.ID,
		DeviceID:  rec.DeviceID,
		Email:     rec.Email,
		Role:      RoleMember,
		TokenHash: rec.TokenHash,
		ExpiresAt: rec.ExpiresAt,
	}
	return nil
}

func (f *fakeDeviceStore) DeviceTokenByHash(_ context.Context, hash []byte) (TokenRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return TokenRow{}, f.lookupErr
	}
	row, ok := f.rows[hex.EncodeToString(hash)]
	if !ok {
		return TokenRow{}, fmt.Errorf("select device_tokens: %w", ErrDeviceTokenUnknown)
	}
	return row, nil
}

func (f *fakeDeviceStore) RevokeDeviceToken(_ context.Context, tokenID string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.revokeErr != nil {
		return f.revokeErr
	}
	f.revoked = append(f.revoked, tokenID)
	for k, row := range f.rows {
		if row.TokenID == tokenID {
			row.RevokedAt = at
			f.rows[k] = row
		}
	}
	return nil
}

func (f *fakeDeviceStore) TouchDeviceToken(_ context.Context, tokenID string, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.touchErr != nil {
		return f.touchErr
	}
	f.touched = append(f.touched, tokenID)
	for k, row := range f.rows {
		if row.TokenID == tokenID {
			row.LastUsedAt = at
			f.rows[k] = row
		}
	}
	return nil
}

// put installs a row directly, for the states that only the database can be in.
func (f *fakeDeviceStore) put(row TokenRow) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[hex.EncodeToString(row.TokenHash)] = row
}

func (f *fakeDeviceStore) touchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.touched)
}

var deviceNow = time.Date(2026, 8, 4, 14, 0, 0, 0, time.UTC)

func newTestDevices(t *testing.T, store DeviceStore, tweak func(*DeviceOptions)) (*Devices, *time.Time) {
	t.Helper()
	clock := deviceNow
	o := DeviceOptions{Store: store, Now: func() time.Time { return clock }}
	if tweak != nil {
		tweak(&o)
	}
	d, err := NewDevices(o)
	if err != nil {
		t.Fatal(err)
	}
	return d, &clock
}

func TestIssueStoresOnlyAHash(t *testing.T) {
	store := newFakeStore()
	d, _ := newTestDevices(t, store, nil)

	got, err := d.Issue(context.Background(), "Dev@Example.com", "device-1")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if !strings.HasPrefix(got.Token, TokenPrefix) {
		t.Errorf("token %q lacks the scannable prefix", got.Token)
	}
	if got.Email != "dev@example.com" {
		t.Errorf("Email = %q, want the normalised address", got.Email)
	}
	if len(store.written) != 1 {
		t.Fatalf("wrote %d rows, want 1", len(store.written))
	}
	rec := store.written[0]
	if strings.Contains(string(rec.TokenHash), got.Token) {
		t.Error("the token itself reached the database")
	}
	if hex.EncodeToString(rec.TokenHash) != hex.EncodeToString(HashToken(got.Token)) {
		t.Error("stored hash does not match the issued token")
	}
	if len(rec.TokenHash) != 32 {
		t.Errorf("stored hash is %d bytes, want a sha256", len(rec.TokenHash))
	}
	if !rec.ExpiresAt.IsZero() {
		t.Error("credentials should not expire by default; an offline laptop must still deliver")
	}
	if rec.ID != got.TokenID || rec.DeviceID != "device-1" {
		t.Errorf("record %+v does not describe the issued credential", rec)
	}
}

func TestIssuedTokensAreUnique(t *testing.T) {
	store := newFakeStore()
	d, _ := newTestDevices(t, store, nil)

	seen := map[string]bool{}
	for i := range 100 {
		got, err := d.Issue(context.Background(), "dev@example.com", fmt.Sprintf("device-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if seen[got.Token] {
			t.Fatal("issued the same credential twice")
		}
		seen[got.Token] = true
	}
}

func TestIssueRejectsIncompleteInput(t *testing.T) {
	d, _ := newTestDevices(t, newFakeStore(), nil)
	if _, err := d.Issue(context.Background(), "", "device-1"); err == nil {
		t.Error("a credential with no owner authenticates as nobody")
	}
	if _, err := d.Issue(context.Background(), "dev@example.com", " "); err == nil {
		t.Error("a credential with no device cannot be revoked from the device list")
	}
}

func TestIssueAndVerifyRoundTrip(t *testing.T) {
	store := newFakeStore()
	d, _ := newTestDevices(t, store, nil)

	issued, err := d.Issue(context.Background(), "dev@example.com", "device-1")
	if err != nil {
		t.Fatal(err)
	}
	id, err := d.Verify(context.Background(), issued.Token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if id.Email != "dev@example.com" || id.DeviceID != "device-1" || id.TokenID != issued.TokenID {
		t.Errorf("identity = %+v", id)
	}
	if id.Viewer() != (Viewer{Email: "dev@example.com", Role: RoleMember}) {
		t.Errorf("Viewer() = %+v", id.Viewer())
	}
}

func TestVerifyRefusesTheseCredentials(t *testing.T) {
	hash := HashToken(TokenPrefix + "whatever")

	cases := []struct {
		name string
		row  TokenRow
		want error
	}{
		{
			name: "a revoked token",
			row:  TokenRow{TokenID: "t", DeviceID: "d", Email: "dev@example.com", TokenHash: hash, RevokedAt: deviceNow.Add(-time.Minute)},
			want: ErrDeviceTokenRevoked,
		},
		{
			name: "an expired token",
			row:  TokenRow{TokenID: "t", DeviceID: "d", Email: "dev@example.com", TokenHash: hash, ExpiresAt: deviceNow.Add(-time.Second)},
			want: ErrDeviceTokenExpired,
		},
		{
			// The laptop was wiped or lost. Its credential dies with the device
			// row even though the token itself is untouched.
			name: "a revoked device",
			row:  TokenRow{TokenID: "t", DeviceID: "d", Email: "dev@example.com", TokenHash: hash, DeviceRevokedAt: deviceNow.Add(-time.Hour)},
			want: ErrDeviceRevoked,
		},
		{
			// Offboarding. One disabled principals row has to stop every
			// machine that person ever enrolled, in the same instant.
			name: "a disabled principal",
			row:  TokenRow{TokenID: "t", DeviceID: "d", Email: "dev@example.com", TokenHash: hash, PrincipalDisabledAt: deviceNow.Add(-time.Hour)},
			want: ErrPrincipalDisabled,
		},
		{
			name: "a row whose hash does not actually match",
			row:  TokenRow{TokenID: "t", DeviceID: "d", Email: "dev@example.com", TokenHash: HashToken("something else")},
			want: ErrDeviceTokenUnknown,
		},
		{
			name: "a row with no owner",
			row:  TokenRow{TokenID: "t", DeviceID: "d", TokenHash: hash},
			want: ErrDeviceTokenUnknown,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			// Installed under the presented token's hash so the lookup finds it
			// and the decision has to come from the row's own state.
			store.rows[hex.EncodeToString(hash)] = tc.row
			d, _ := newTestDevices(t, store, nil)

			_, err := d.Verify(context.Background(), TokenPrefix+"whatever")
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRevocationTakesEffectAtItsTimestamp(t *testing.T) {
	store := newFakeStore()
	hash := HashToken(TokenPrefix + "scheduled")
	store.put(TokenRow{
		TokenID: "t", DeviceID: "d", Email: "dev@example.com", TokenHash: hash,
		RevokedAt: deviceNow.Add(time.Hour),
	})
	d, clock := newTestDevices(t, store, nil)

	if _, err := d.Verify(context.Background(), TokenPrefix+"scheduled"); err != nil {
		t.Fatalf("a revocation an hour from now should not bite yet: %v", err)
	}
	*clock = clock.Add(time.Hour)
	if _, err := d.Verify(context.Background(), TokenPrefix+"scheduled"); !errors.Is(err, ErrDeviceTokenRevoked) {
		t.Fatalf("err = %v, want ErrDeviceTokenRevoked at the revocation instant", err)
	}
}

func TestVerifyRejectsMalformedCredentialsWithoutTouchingTheStore(t *testing.T) {
	store := newFakeStore()
	store.lookupErr = errors.New("the store must not have been consulted")
	d, _ := newTestDevices(t, store, nil)

	for _, tok := range []string{"", "   ", "not-a-loop-token", "Bearer lsd_x", strings.Repeat("x", 4096)} {
		if _, err := d.Verify(context.Background(), tok); !errors.Is(err, ErrDeviceTokenMalformed) {
			t.Errorf("%.20q: err = %v, want ErrDeviceTokenMalformed", tok, err)
		}
	}
}

func TestVerifyReportsAnUnknownCredential(t *testing.T) {
	d, _ := newTestDevices(t, newFakeStore(), nil)
	if _, err := d.Verify(context.Background(), TokenPrefix+"never-issued"); !errors.Is(err, ErrDeviceTokenUnknown) {
		t.Fatalf("err = %v, want ErrDeviceTokenUnknown", err)
	}
}

func TestVerifyPassesTheStoredRole(t *testing.T) {
	store := newFakeStore()
	hash := HashToken(TokenPrefix + "admin")
	store.put(TokenRow{TokenID: "t", DeviceID: "d", Email: "boss@example.com", Role: RoleAdmin, TokenHash: hash})
	d, _ := newTestDevices(t, store, nil)

	id, err := d.Verify(context.Background(), TokenPrefix+"admin")
	if err != nil {
		t.Fatal(err)
	}
	if id.Role != RoleAdmin {
		t.Errorf("Role = %q, want admin", id.Role)
	}
}

func TestAnUnreadableRoleDegradesToMember(t *testing.T) {
	store := newFakeStore()
	hash := HashToken(TokenPrefix + "odd")
	store.put(TokenRow{TokenID: "t", DeviceID: "d", Email: "dev@example.com", Role: Role("superuser"), TokenHash: hash})
	d, _ := newTestDevices(t, store, nil)

	id, err := d.Verify(context.Background(), TokenPrefix+"odd")
	if err != nil {
		t.Fatal(err)
	}
	if id.Role != RoleMember {
		t.Errorf("Role = %q, want the least privileged role", id.Role)
	}
}

func TestLastUsedIsWrittenOnAThrottle(t *testing.T) {
	store := newFakeStore()
	d, clock := newTestDevices(t, store, func(o *DeviceOptions) { o.TouchInterval = 5 * time.Minute })

	issued, err := d.Issue(context.Background(), "dev@example.com", "device-1")
	if err != nil {
		t.Fatal(err)
	}

	// The first upload records the use.
	if _, err := d.Verify(context.Background(), issued.Token); err != nil {
		t.Fatal(err)
	}
	if got := store.touchCount(); got != 1 {
		t.Fatalf("touches = %d, want 1", got)
	}

	// An agent uploading every few seconds must not turn authentication into a
	// write on every request.
	for range 50 {
		*clock = clock.Add(2 * time.Second)
		if _, err := d.Verify(context.Background(), issued.Token); err != nil {
			t.Fatal(err)
		}
	}
	if got := store.touchCount(); got != 1 {
		t.Fatalf("touches = %d after a burst inside the throttle window, want 1", got)
	}

	*clock = clock.Add(5 * time.Minute)
	if _, err := d.Verify(context.Background(), issued.Token); err != nil {
		t.Fatal(err)
	}
	if got := store.touchCount(); got != 2 {
		t.Fatalf("touches = %d after the window elapsed, want 2", got)
	}
}

func TestAFailedTouchDoesNotFailTheUpload(t *testing.T) {
	store := newFakeStore()
	store.touchErr = errors.New("statement timeout")
	d, _ := newTestDevices(t, store, nil)
	hash := HashToken(TokenPrefix + "t")
	store.put(TokenRow{TokenID: "t", DeviceID: "d", Email: "dev@example.com", TokenHash: hash})

	if _, err := d.Verify(context.Background(), TokenPrefix+"t"); err != nil {
		t.Fatalf("a telemetry column must not cost us the data: %v", err)
	}
}

func TestExpiryIsSetWhenALifetimeIsConfigured(t *testing.T) {
	store := newFakeStore()
	d, _ := newTestDevices(t, store, func(o *DeviceOptions) { o.Lifetime = 90 * 24 * time.Hour })

	issued, err := d.Issue(context.Background(), "dev@example.com", "device-1")
	if err != nil {
		t.Fatal(err)
	}
	if want := deviceNow.Add(90 * 24 * time.Hour); !issued.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", issued.ExpiresAt, want)
	}
}

func TestRevoke(t *testing.T) {
	store := newFakeStore()
	d, _ := newTestDevices(t, store, nil)

	issued, err := d.Issue(context.Background(), "dev@example.com", "device-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Revoke(context.Background(), issued.TokenID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := d.Verify(context.Background(), issued.Token); !errors.Is(err, ErrDeviceTokenRevoked) {
		t.Fatalf("err = %v, want the credential to be dead", err)
	}
	if err := d.Revoke(context.Background(), " "); err == nil {
		t.Error("revoking nothing should be an error, not a silent no-op")
	}
}

func TestRevokeByToken(t *testing.T) {
	store := newFakeStore()
	d, _ := newTestDevices(t, store, nil)

	issued, err := d.Issue(context.Background(), "dev@example.com", "device-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.RevokeByToken(context.Background(), issued.Token); err != nil {
		t.Fatalf("RevokeByToken: %v", err)
	}
	if len(store.revoked) != 1 || store.revoked[0] != issued.TokenID {
		t.Errorf("revoked %v, want the issued credential", store.revoked)
	}
	if err := d.RevokeByToken(context.Background(), TokenPrefix+"never-issued"); !errors.Is(err, ErrDeviceTokenUnknown) {
		t.Errorf("err = %v, want ErrDeviceTokenUnknown", err)
	}
}

func TestStoreFailuresSurface(t *testing.T) {
	boom := errors.New("connection refused")

	store := newFakeStore()
	store.insertErr = boom
	d, _ := newTestDevices(t, store, nil)
	if _, err := d.Issue(context.Background(), "dev@example.com", "device-1"); !errors.Is(err, boom) {
		t.Errorf("Issue err = %v, want the store error", err)
	}

	store2 := newFakeStore()
	store2.lookupErr = boom
	d2, _ := newTestDevices(t, store2, nil)
	// A transient database failure must not read as a rejected credential: the
	// agent quarantines anything it is told is permanent.
	if _, err := d2.Verify(context.Background(), TokenPrefix+"x"); !errors.Is(err, boom) {
		t.Errorf("Verify err = %v, want the store error", err)
	}
	if errors.Is(store2.lookupErr, ErrDeviceTokenUnknown) {
		t.Error("the fake is not exercising the case this test claims")
	}
}

func TestNewDevicesRequiresAStore(t *testing.T) {
	if _, err := NewDevices(DeviceOptions{}); err == nil {
		t.Error("a Devices with no store would verify nothing and say yes")
	}
}

func TestConcurrentVerify(t *testing.T) {
	store := newFakeStore()
	d, _ := newTestDevices(t, store, nil)
	issued, err := d.Issue(context.Background(), "dev@example.com", "device-1")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := d.Verify(context.Background(), issued.Token); err != nil {
				t.Errorf("Verify: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestBearerToken(t *testing.T) {
	cases := map[string]string{
		"Bearer lsd_abc":   "lsd_abc",
		"bearer lsd_abc":   "lsd_abc",
		"BEARER  lsd_abc ": "lsd_abc",
		"Basic lsd_abc":    "",
		"lsd_abc":          "",
		"":                 "",
		"Bearer":           "",
	}
	for header, want := range cases {
		r := httptest.NewRequest("POST", "/v1/events", nil)
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		if got := BearerToken(r); got != want {
			t.Errorf("BearerToken(%q) = %q, want %q", header, got, want)
		}
	}
}

func TestNewUUID(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		u, err := NewUUID()
		if err != nil {
			t.Fatal(err)
		}
		if len(u) != 36 || u[8] != '-' || u[13] != '-' || u[14] != '4' || u[18] != '-' || u[23] != '-' {
			t.Fatalf("%q is not a v4 UUID", u)
		}
		if !strings.ContainsRune("89ab", rune(u[19])) {
			t.Fatalf("%q has the wrong variant bits", u)
		}
		if seen[u] {
			t.Fatal("generated the same UUID twice")
		}
		seen[u] = true
	}
}
