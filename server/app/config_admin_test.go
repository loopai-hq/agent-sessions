package app

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/loopai-hq/agent-sessions/server/store"
)

// adminEnv is bootEnv with an allowlist the ADMIN_EMAILS and DOMAIN_ALIASES
// fixtures below sit in.
func adminEnv() map[string]string {
	env := bootEnv()
	env["ALLOWED_DOMAINS"] = "example.com,example.org,third.example"
	return env
}

// TestAdminEmailsAreReadAndHeldToTheAllowlist pins the shape of ADMIN_EMAILS:
// comma-separated, trimmed and lower-cased, empties dropped, and every
// address inside ALLOWED_DOMAINS, because an admin who cannot sign in is a
// deployment nobody can administer and the boot is where to say so.
func TestAdminEmailsAreReadAndHeldToTheAllowlist(t *testing.T) {
	env := adminEnv()
	env["ADMIN_EMAILS"] = " Root@Example.com , ops@example.org,, "
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.AdminEmails; len(got) != 2 || got[0] != "root@example.com" || got[1] != "ops@example.org" {
		t.Errorf("AdminEmails = %q", got)
	}

	for name, value := range map[string]string{
		"outside the allowlist": "root@example.com,intruder@example.net",
		"not an address":        "root",
		"no local part":         "@example.com",
	} {
		t.Run(name, func(t *testing.T) {
			env := adminEnv()
			env["ADMIN_EMAILS"] = value
			_, err := Load(bootGetenv(env))
			if err == nil {
				t.Fatalf("ADMIN_EMAILS=%q was accepted", value)
			}
			if !strings.Contains(err.Error(), "ADMIN_EMAILS") {
				t.Errorf("err = %v, want it to name ADMIN_EMAILS", err)
			}
		})
	}

	// Unset is the ordinary case and means nothing is bootstrapped.
	cfg, err = Load(bootGetenv(adminEnv()))
	if err != nil || len(cfg.AdminEmails) != 0 {
		t.Errorf("unset: AdminEmails = %v, err = %v", cfg.AdminEmails, err)
	}
}

// TestDomainAliasesAreReadAsPairsInsideTheAllowlist pins DOMAIN_ALIASES:
// comma-separated a:b pairs of distinct domains in the hostname alphabet,
// both inside ALLOWED_DOMAINS, and nothing else. The domains reach the
// export's SQL as literals, which is why the alphabet is enforced here.
func TestDomainAliasesAreReadAsPairsInsideTheAllowlist(t *testing.T) {
	env := adminEnv()
	env["DOMAIN_ALIASES"] = " Example.com:EXAMPLE.org , example.org:third.example,"
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []store.DomainAlias{{A: "example.com", B: "example.org"}, {A: "example.org", B: "third.example"}}
	if len(cfg.DomainAliases) != len(want) || cfg.DomainAliases[0] != want[0] || cfg.DomainAliases[1] != want[1] {
		t.Errorf("DomainAliases = %+v, want %+v", cfg.DomainAliases, want)
	}

	for name, value := range map[string]string{
		"not a pair":              "example.com",
		"three parts":             "example.com:example.org:third.example",
		"a domain with itself":    "example.com:example.com",
		"outside the allowlist":   "example.com:gmail.com",
		"a quote in a domain":     "example.com:evil'.example",
		"a space in a domain":     "example.com:evil example",
		"an empty side":           "example.com:",
		"a wildcard":              "example.com:*.example",
		"a label starting with -": "example.com:-bad.example",
	} {
		t.Run(name, func(t *testing.T) {
			env := adminEnv()
			env["DOMAIN_ALIASES"] = value
			_, err := Load(bootGetenv(env))
			if err == nil {
				t.Fatalf("DOMAIN_ALIASES=%q was accepted", value)
			}
			if !strings.Contains(err.Error(), "DOMAIN_ALIASES") {
				t.Errorf("err = %v, want it to name DOMAIN_ALIASES", err)
			}
		})
	}

	cfg, err = Load(bootGetenv(adminEnv()))
	if err != nil || len(cfg.DomainAliases) != 0 {
		t.Errorf("unset: DomainAliases = %v, err = %v", cfg.DomainAliases, err)
	}
}

// TestTheBootLogNamesTheAdminsAndAliases: both are addresses and domain names
// rather than secrets, and the log line is how an operator confirms what the
// boot is about to write.
func TestTheBootLogNamesTheAdminsAndAliases(t *testing.T) {
	env := adminEnv()
	env["ADMIN_EMAILS"] = "root@example.com"
	env["DOMAIN_ALIASES"] = "example.com:example.org"
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var buf bytes.Buffer
	NewLogger(slog.LevelInfo, &buf).Info("config", "config", cfg)
	for _, want := range []string{`"admin_emails":["root@example.com"]`, `"domain_aliases":["example.com:example.org"]`} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log lacks %s:\n%s", want, buf.String())
		}
	}
}
