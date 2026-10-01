package config

import (
	"os"
	"testing"

	"github.com/loopai-hq/loop-sessions/internal/spool"
)

// Every config written before the disk guard was rewritten carries
// "min_free_ratio": 0.05, because Defaults() wrote it. That number is what
// dropped 17,340 events on this machine; it must read as "no override" so the
// new floor-and-cap policy applies to the fleet without anybody editing forty
// config files.
func TestStoredLegacyRatioIsTreatedAsUnset(t *testing.T) {
	p := paths(t)
	if err := os.MkdirAll(p.Root(), 0o700); err != nil {
		t.Fatal(err)
	}
	cases := map[string]float64{
		`{"email":"dev@example.com","endpoint":"https://x.example.com","min_free_ratio":0.05}`: 0,
		`{"email":"dev@example.com","endpoint":"https://x.example.com","min_free_ratio":0.10}`: 0.10,
		`{"email":"dev@example.com","endpoint":"https://x.example.com"}`:                       0,
	}
	for body, want := range cases {
		if err := os.WriteFile(p.ConfigFile(), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		c, err := Load(p)
		if err != nil {
			t.Fatal(err)
		}
		if c.MinFreeRatio != want {
			t.Errorf("%s -> MinFreeRatio %v, want %v", body, c.MinFreeRatio, want)
		}
	}
	if Defaults().MinFreeRatio != 0 {
		t.Errorf("Defaults writes a ratio (%v); that is how the old default came to look explicit", Defaults().MinFreeRatio)
	}
}

// The loader's idea of the legacy value and the spool's must be one number.
func TestLegacyRatioMatchesTheSpoolsConstant(t *testing.T) {
	if legacyMinFreeRatio != spool.LegacyMinFreeRatio {
		t.Fatalf("config legacy ratio %v != spool.LegacyMinFreeRatio %v", legacyMinFreeRatio, spool.LegacyMinFreeRatio)
	}
}

func TestChannelDefaultsToLatestAndIsValidated(t *testing.T) {
	if Defaults().Channel != DefaultChannel || DefaultChannel != "latest" {
		t.Fatalf("default channel = %q, want latest", Defaults().Channel)
	}
	c := Defaults()
	c.Email, c.Endpoint = "dev@example.com", "https://x.example.com"
	c.Channel = "canary"
	if err := c.Validate(); err != nil {
		t.Fatalf("canary is a real channel: %v", err)
	}
	c.Channel = "nightly"
	if err := c.Validate(); err == nil {
		t.Fatal("an unknown channel must be refused, or a typo pins a machine to a 404 forever")
	}
	c.Channel = ""
	if err := c.Validate(); err != nil {
		t.Fatalf("an empty channel reads as latest: %v", err)
	}
}
