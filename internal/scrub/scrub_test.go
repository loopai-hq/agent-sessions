// Every credential-shaped value in this file is synthetic. This is the
// scrubber's test corpus, and a scrubber whose tests contain nothing that
// looks like a secret tests nothing, so the fixtures are shaped exactly like
// the real thing: the AWS documentation example key pair
// (AKIAIOSFODNN7EXAMPLE and its secret), the jwt.io sample token, PEM bodies
// cut to a line, AbCdEf/1234567890 filler behind every vendor prefix, a
// Postgres DSN with a made-up password on a host that does not exist. None of
// them has ever been a live credential anywhere. .gitleaks.toml and
// .github/secret_scanning.yml allowlist this file by path for the same
// reason: a scanner that reports it is reporting the test.

package scrub

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// True positives — one case per Kind
// ---------------------------------------------------------------------------

func TestScrubDetects(t *testing.T) {
	cases := []struct {
		name string
		kind Kind
		in   string
		// leak is a fragment that must NOT survive scrubbing.
		leak string
		// keep, when set, must survive verbatim (prefix preservation).
		keep string
	}{
		{
			name: "anthropic key",
			kind: KindAnthropicKey,
			in:   `ANTHROPIC_API_KEY=sk-ant-api03-AbCdEf1234567890AbCdEf1234567890AbCdEf12`,
			leak: "sk-ant-api03-AbCdEf",
		},
		{
			name: "openai legacy key",
			kind: KindOpenAIKey,
			in:   `export OPENAI_KEY=sk-AbCdEf1234567890AbCdEf1234567890AbCdEf1234567890`,
			leak: "sk-AbCdEf1234567890",
		},
		{
			name: "openai project key",
			kind: KindOpenAIKey,
			in:   `sk-proj-AbCdEf1234567890AbCdEf1234567890`,
			leak: "sk-proj-AbCdEf",
		},
		{
			name: "stripe live secret key",
			kind: KindStripeSecretKey,
			in:   `stripe.api_key = "sk_live_51AbCdEf1234567890AbCdEf"`,
			leak: "sk_live_51AbCdEf",
		},
		{
			name: "stripe restricted key",
			kind: KindStripeRestrictedKey,
			in:   `rk_live_51AbCdEf1234567890AbCdEf`,
			leak: "rk_live_51AbCdEf",
		},
		{
			name: "github token",
			kind: KindGitHubToken,
			in:   `gh auth login --with-token ghp_AbCdEf1234567890AbCdEf1234567890AbCd`,
			leak: "ghp_AbCdEf",
		},
		{
			name: "github fine-grained pat",
			kind: KindGitHubPAT,
			in:   `github_pat_11ABCDEFG0AbCdEfGhIjKl_AbCdEf1234567890AbCdEf1234567890AbCdEf1234567890AbCdEf12345`,
			leak: "github_pat_11ABCDEFG0",
		},
		{
			name: "aws access key id",
			kind: KindAWSAccessKeyID,
			in:   `AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE`,
			leak: "AKIAIOSFODNN7EXAMPLE",
		},
		{
			name: "aws sts session key",
			kind: KindAWSAccessKeyID,
			in:   `ASIAY34FZKBOKMUTVV7A`,
			leak: "ASIAY34FZKBOKMUTVV7A",
		},
		{
			name: "aws secret access key",
			kind: KindAWSSecretAccessKey,
			in:   `aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"`,
			leak: "wJalrXUtnFEMI/K7MDENG",
			keep: `aws_secret_access_key = "`,
		},
		{
			name: "google api key",
			kind: KindGoogleAPIKey,
			in:   `https://maps.googleapis.com/maps/api/js?key=AIzaSyA1234567890abcdefghijklmnopqrstuv`,
			leak: "AIzaSyA1234567890",
		},
		{
			name: "google oauth token",
			kind: KindGoogleOAuthToken,
			in:   `{"access_token":"ya29.A0ARrdaM-abcdefghijklmnop1234567890"}`,
			leak: "ya29.A0ARrdaM",
		},
		{
			name: "slack bot token",
			kind: KindSlackToken,
			in:   `SLACK_BOT_TOKEN=xoxb-123456789012-1234567890123-AbCdEfGhIjKlMnOpQrStUvWx`,
			leak: "xoxb-123456789012",
		},
		{
			name: "slack webhook url",
			kind: KindSlackWebhook,
			in:   `curl -X POST https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX`,
			leak: "hooks.slack.com/services/T00000000",
		},
		{
			name: "jwt",
			kind: KindJWT,
			in:   `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV`,
			leak: "eyJhbGciOiJIUzI1NiIs",
		},
		{
			name: "pem private key block",
			kind: KindPrivateKeyPEM,
			in:   "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAxGZm9Q\nc3JldA==\n-----END RSA PRIVATE KEY-----",
			leak: "MIIEowIBAAKCAQEA",
		},
		{
			name: "openssh private key block",
			kind: KindSSHPrivateKey,
			in:   "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAA\n-----END OPENSSH PRIVATE KEY-----",
			leak: "b3BlbnNzaC1rZXktdjEA",
		},
		{
			name: "authorization bearer",
			kind: KindAuthBearer,
			in:   `Authorization: Bearer AbCdEf1234567890AbCdEf1234567890`,
			leak: "AbCdEf1234567890",
			keep: "Authorization: Bearer ",
		},
		{
			name: "authorization basic",
			kind: KindAuthBasic,
			in:   `Authorization: Basic dXNlcjpzdXBlcnNlY3JldA==`,
			leak: "dXNlcjpzdXBlcnNlY3JldA",
			keep: "Authorization: Basic ",
		},
		{
			name: "postgres dsn password",
			kind: KindDBPassword,
			in:   `postgres://svcuser:S3cr3tP4ssw0rd@db.internal:5432/loop`,
			leak: "S3cr3tP4ssw0rd",
			keep: "postgres://svcuser:",
		},
		{
			name: "mysql dsn password",
			kind: KindDBPassword,
			in:   `mysql://root:R00tP4ssw0rdX@127.0.0.1:3306/app`,
			leak: "R00tP4ssw0rdX",
		},
		{
			name: "mongodb srv dsn password",
			kind: KindDBPassword,
			in:   `mongodb+srv://admin:Zx9Qw8Er7Ty6@cluster0.mongodb.net/db`,
			leak: "Zx9Qw8Er7Ty6",
		},
		{
			name: "redis dsn password",
			kind: KindDBPassword,
			in:   `redis://default:Hs82jdKw01Lm@cache.internal:6379/0`,
			leak: "Hs82jdKw01Lm",
		},
		{
			name: "generic secret behind key name",
			kind: KindGenericSecret,
			in:   `{"api_key": "A1b2C3d4E5f6G7h8I9j0"}`,
			leak: "A1b2C3d4E5f6G7h8I9j0",
			keep: `{"api_key": "`,
		},
		{
			name: "generic secret env assignment",
			kind: KindGenericSecret,
			in:   `SESSION_SECRET=k9Vx2Qm7Zp4Lr8Ts1Wy6`,
			leak: "k9Vx2Qm7Zp4Lr8Ts1Wy6",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Scrub(tc.in)

			if strings.Contains(got.Text, tc.leak) {
				t.Errorf("secret survived scrubbing\n  input:  %s\n  output: %s\n  leaked: %s", tc.in, got.Text, tc.leak)
			}
			if got.Counts[tc.kind] == 0 {
				t.Errorf("expected kind %q to fire, got counts %v\n  output: %s", tc.kind, got.Counts, got.Text)
			}
			if !strings.Contains(got.Text, redaction(tc.kind)) {
				t.Errorf("expected marker %s in output, got: %s", redaction(tc.kind), got.Text)
			}
			if tc.keep != "" && !strings.Contains(got.Text, tc.keep) {
				t.Errorf("prefix not preserved\n  want prefix: %s\n  output:      %s", tc.keep, got.Text)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// False positives — over-redaction destroys the transcript's value as a record
// ---------------------------------------------------------------------------

// taskDescription is the brief that specified this package. It names almost every
// credential prefix the package detects, so scrubbing it is the sharpest possible
// test that bare prefixes discussed in prose are left alone.
const taskDescription = `
BUILD: a Go package scrub that redacts secrets from Claude Code transcript text.

RULES TO IMPLEMENT — port from peteromallet/dataclaw (MIT) and gitleaks' rule set.
Minimum coverage, each its own Kind: anthropic key (sk-ant-), openai key, stripe
live secret+restricted (sk_live_/rk_live_), github token (ghp_/gho_/ghu_/ghs_/ghr_)
and github_pat_, AWS access key (AKIA...), AWS secret access key, google api key
(AIza), google oauth token (ya29.), slack tokens (xox[baprs]-) and slack webhook
URLs, JWTs, PEM private key blocks, Authorization: Bearer/Basic headers, database
connection strings with inline passwords (postgres://, mysql://, mongodb://,
redis://), private SSH keys, generic high-entropy strings ONLY when adjacent to a
secret-ish key name (password/secret/token/api_key/apikey/passwd/pwd) — never bare
entropy, it false-positives on base64 payloads and hashes.

WHY THIS MATTERS — on this laptop's real transcripts: 30 files contain
postgres://user:password@ DSNs, 26 contain JWTs, 18 contain Google API keys, 5
contain AWS AKIA keys, 3 contain PEM private keys, 3 Slack tokens, 2 Anthropic keys.
`

func TestScrubDoesNotOverRedact(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"the task description itself", taskDescription},
		{"bare stripe prefix in prose", "we grep for rk_live_ prefixes in the audit"},
		{"bare slack prefix in prose", "the xoxb- format is documented upstream"},
		{"bare anthropic prefix in prose", "keys start with sk-ant- and are 108 chars"},
		{"bare aws prefix in prose", "look for AKIA... at the start of the line"},
		{"bare github prefixes in prose", "one of ghp_ gho_ ghu_ ghs_ ghr_ or github_pat_"},
		{"git sha", "commit 00c37b591c1e4f5a9b8d2c3e4f5a6b7c8d9e0f1a landed on main"},
		{"short git sha list", "090b8165 fc98982b 9c367460 e6b583ab 350d179d"},
		{"uuid", "session 22f6577a-25d3-4955-826e-a76d94c1e219 finished"},
		{"uuid in json", `{"session_id": "22f6577a-25d3-4955-826e-a76d94c1e219"}`},
		{
			"base64 encoded image",
			`{"type":"image","data":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="}`,
		},
		{
			"long file path",
			"/home/dev/src/project/docs/research/30-platform-integration.md",
		},
		{"sha256 hex hash", "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"sha512 hex hash", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"},
		{"token counts in usage payload", `{"usage":{"input_tokens":1234567890123456,"output_tokens":9876543210987654}}`},
		{"non-secret key suffixes", `{"cache_key":"a1b2c3d4e5f6g7h8i9j0","idempotency_key":"z9y8x7w6v5u4t3s2r1q0"}`},
		{"basic english after auth scheme", "Basic internationalization support is required"},
		{"bearer english after auth scheme", "Bearer authentication_is_required_here for the API"},
		{"documentation dsn", "connect with postgres://user:password@localhost:5432/db"},
		{"documentation dsn username variant", "postgres://username:password@host/db is the template"},
		{"placeholder api key", `{"api_key": "YOUR_API_KEY_HERE_XXXX"}`},
		{"env var reference", `api_key = process.env.ANTHROPIC_API_KEY_VALUE`},
		{"stripe test key is not sensitive", "sk_test_51AbCdEf1234567890AbCdEf"},
		{"prose about passwords", "the password/secret/token/api_key/apikey/passwd/pwd names all count"},
		{"semver and versions", "upgraded to 1.24.0-beta.20260801.buildmetadata"},

		// The cases below are the highest-frequency false positives found by
		// scanning 2.81 GB of real transcripts with an entropy-only generic rule.
		// They are all source code, and together they accounted for the bulk of
		// 7519 spurious hits across 1259 files. See isHighEntropySecret.
		{"attribute access as value", `secret_name=self.credential_secret_name`},
		{"attribute access on token", `token: self._access_token_cache`},
		{"snake_case identifier as value", `secret = synapse_ingestion_service`},
		{"snake_case identifier on token", `token = handler_registry_lookup`},
		{"private identifier as value", `api_key = _extract_credentials_for`},
		{"class attribute as value", `password = cls.default_password_field`},
		{"go struct field as value", `Token: interval_seconds_maximum`},
		{"module path as value", `secret: loop.credential.resolver`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Scrub(tc.in)
			if len(got.Counts) != 0 {
				t.Errorf("false positive: %v\n  input:  %s\n  output: %s", got.Counts, tc.in, got.Text)
			}
			if got.Text != tc.in {
				t.Errorf("input was modified\n  input:  %s\n  output: %s", tc.in, got.Text)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Structural cases
// ---------------------------------------------------------------------------

func TestScrubEmptyInput(t *testing.T) {
	got := Scrub("")
	if got.Text != "" {
		t.Errorf("Text = %q, want empty", got.Text)
	}
	if got.Counts == nil {
		t.Error("Counts is nil; want non-nil empty map")
	}
	if len(got.Counts) != 0 {
		t.Errorf("Counts = %v, want empty", got.Counts)
	}
}

func TestScrubCleanInputUnchanged(t *testing.T) {
	in := "func main() {\n\tfmt.Println(\"hello\")\n}\n// nothing sensitive here at all"
	got := Scrub(in)
	if got.Text != in {
		t.Errorf("clean input modified\n  in:  %q\n  out: %q", in, got.Text)
	}
	if len(got.Counts) != 0 {
		t.Errorf("Counts = %v, want empty", got.Counts)
	}
}

func TestScrubMultipleSecretsOnOneLine(t *testing.T) {
	in := `ANTHROPIC=sk-ant-api03-AbCdEf1234567890AbCdEf1234567890AbCdEf12 AWS=AKIAIOSFODNN7EXAMPLE GH=ghp_AbCdEf1234567890AbCdEf1234567890AbCd`
	got := Scrub(in)

	for _, k := range []Kind{KindAnthropicKey, KindAWSAccessKeyID, KindGitHubToken} {
		if got.Counts[k] != 1 {
			t.Errorf("Counts[%s] = %d, want 1 (counts: %v)", k, got.Counts[k], got.Counts)
		}
	}
	for _, leak := range []string{"sk-ant-api03", "AKIAIOSFODNN7EXAMPLE", "ghp_AbCdEf"} {
		if strings.Contains(got.Text, leak) {
			t.Errorf("leaked %q in %s", leak, got.Text)
		}
	}
}

func TestScrubRepeatedSecretCountsEachOccurrence(t *testing.T) {
	key := "AKIAIOSFODNN7EXAMPLE"
	in := key + " and again " + key + " and once more " + key
	got := Scrub(in)
	if got.Counts[KindAWSAccessKeyID] != 3 {
		t.Errorf("Counts[%s] = %d, want 3", KindAWSAccessKeyID, got.Counts[KindAWSAccessKeyID])
	}
}

// A transcript is JSONL, so a multi-line secret arrives with its newlines encoded
// as the two characters backslash and n rather than as real line breaks.
func TestScrubSecretAcrossJSONEscapedBoundary(t *testing.T) {
	in := `{"role":"user","content":"here is the deploy key\n-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAxGZm9QsecretbodyAAAA\n-----END RSA PRIVATE KEY-----\nplease rotate it"}`
	got := Scrub(in)

	if got.Counts[KindPrivateKeyPEM] != 1 {
		t.Fatalf("Counts[%s] = %d, want 1 (counts: %v)", KindPrivateKeyPEM, got.Counts[KindPrivateKeyPEM], got.Counts)
	}
	if strings.Contains(got.Text, "MIIEowIBAAKCAQEA") {
		t.Errorf("key body survived: %s", got.Text)
	}
	// The surrounding JSON envelope must be intact.
	if !strings.HasPrefix(got.Text, `{"role":"user","content":"`) || !strings.HasSuffix(got.Text, `please rotate it"}`) {
		t.Errorf("JSON envelope damaged: %s", got.Text)
	}
}

func TestScrubBearerInsideEscapedJSON(t *testing.T) {
	in := `{"headers":"{\"Authorization\": \"Bearer AbCdEf1234567890AbCdEf1234567890\"}"}`
	got := Scrub(in)

	if got.Counts[KindAuthBearer] != 1 {
		t.Fatalf("Counts[%s] = %d, want 1 (counts: %v)", KindAuthBearer, got.Counts[KindAuthBearer], got.Counts)
	}
	if !strings.Contains(got.Text, `Bearer [REDACTED:auth_bearer]`) {
		t.Errorf("prefix not preserved: %s", got.Text)
	}
	if !strings.HasSuffix(got.Text, `\"}"}`) {
		t.Errorf("escaped JSON tail damaged: %s", got.Text)
	}
}

// A JWT behind a Bearer prefix should be reported as a JWT, once, with the
// scheme keyword still readable.
func TestScrubBearerJWTCountsOnce(t *testing.T) {
	in := `Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV`
	got := Scrub(in)

	if got.Counts[KindJWT] != 1 {
		t.Errorf("Counts[%s] = %d, want 1", KindJWT, got.Counts[KindJWT])
	}
	if total := totalCount(got.Counts); total != 1 {
		t.Errorf("total redactions = %d, want 1 (counts: %v)", total, got.Counts)
	}
	if !strings.Contains(got.Text, "Authorization: Bearer [REDACTED:jwt]") {
		t.Errorf("unexpected output: %s", got.Text)
	}
}

// Prefix preservation exists so a scrubbed transcript line is still a transcript
// line. A redaction that swallowed the closing quote or the @ of a DSN would turn
// valid JSONL into a parse error downstream.
func TestScrubbedJSONStaysParseable(t *testing.T) {
	lines := []string{
		`{"role":"user","content":"DATABASE_URL=postgres://svcuser:S3cr3tP4ssw0rd@db.internal:5432/loop"}`,
		`{"headers":{"Authorization":"Bearer AbCdEf1234567890AbCdEf1234567890"}}`,
		`{"headers":{"Authorization":"Basic dXNlcjpzdXBlcnNlY3JldA=="}}`,
		`{"env":{"ANTHROPIC_API_KEY":"sk-ant-api03-AbCdEf1234567890AbCdEf1234567890AbCdEf12"}}`,
		`{"env":{"AWS_ACCESS_KEY_ID":"AKIAIOSFODNN7EXAMPLE","aws_secret_access_key":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}}`,
		`{"config":{"api_key":"A1b2C3d4E5f6G7h8I9j0","slack":"xoxb-123456789012-1234567890123-AbCdEfGhIjKlMnOpQrStUvWx"}}`,
	}

	for _, line := range lines {
		var before any
		if err := json.Unmarshal([]byte(line), &before); err != nil {
			t.Fatalf("fixture is not valid JSON: %v\n  %s", err, line)
		}

		got := Scrub(line)
		if len(got.Counts) == 0 {
			t.Errorf("fixture produced no redactions: %s", line)
		}

		var after any
		if err := json.Unmarshal([]byte(got.Text), &after); err != nil {
			t.Errorf("scrubbed output is not valid JSON: %v\n  in:  %s\n  out: %s", err, line, got.Text)
		}
	}
}

func TestScrubIsIdempotent(t *testing.T) {
	in := `postgres://svcuser:S3cr3tP4ssw0rd@db/loop key=sk-ant-api03-AbCdEf1234567890AbCdEf1234567890AbCdEf12`
	once := Scrub(in)
	twice := Scrub(once.Text)

	if twice.Text != once.Text {
		t.Errorf("second pass changed the text\n  once:  %s\n  twice: %s", once.Text, twice.Text)
	}
	if len(twice.Counts) != 0 {
		t.Errorf("second pass found %v, want nothing left to redact", twice.Counts)
	}
}

func TestScrubBytes(t *testing.T) {
	t.Run("redacts and reports", func(t *testing.T) {
		in := []byte(`AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE`)
		out, counts := ScrubBytes(in)
		if counts[KindAWSAccessKeyID] != 1 {
			t.Errorf("counts = %v, want one aws_access_key_id", counts)
		}
		if strings.Contains(string(out), "AKIAIOSFODNN7EXAMPLE") {
			t.Errorf("secret survived: %s", out)
		}
	})

	t.Run("clean input returned unmodified", func(t *testing.T) {
		in := []byte("plain transcript text with nothing to hide")
		out, counts := ScrubBytes(in)
		if len(counts) != 0 {
			t.Errorf("counts = %v, want empty", counts)
		}
		if string(out) != string(in) {
			t.Errorf("out = %q, want %q", out, in)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		out, counts := ScrubBytes(nil)
		if len(out) != 0 {
			t.Errorf("out = %q, want empty", out)
		}
		if counts == nil || len(counts) != 0 {
			t.Errorf("counts = %v, want non-nil empty", counts)
		}
	})
}

// ---------------------------------------------------------------------------
// Package invariants
// ---------------------------------------------------------------------------

// Kinds is what callers register metric series from, so it must exactly match
// the set of kinds the rule table can actually emit.
func TestKindsMatchesRuleTable(t *testing.T) {
	inKinds := make(map[Kind]bool, len(Kinds))
	for _, k := range Kinds {
		if inKinds[k] {
			t.Errorf("Kinds contains %q twice", k)
		}
		inKinds[k] = true
	}

	inRules := make(map[Kind]bool, len(rules))
	for _, r := range rules {
		inRules[r.kind] = true
		if !inKinds[r.kind] {
			t.Errorf("rule kind %q is missing from Kinds", r.kind)
		}
	}
	for _, k := range Kinds {
		if !inRules[k] {
			t.Errorf("Kinds lists %q but no rule emits it", k)
		}
	}
}

// Every rule must be reachable: its own gates have to fire on text that the rule
// itself matches, or the pre-filter would silently disable it.
func TestEveryRuleHasGates(t *testing.T) {
	for _, r := range rules {
		if len(r.gates) == 0 && len(r.foldGates) == 0 {
			t.Errorf("rule %q has no gates and would run on every input", r.kind)
		}
	}
}

func TestShannonEntropy(t *testing.T) {
	cases := []struct {
		in      string
		wantMin float64
		wantMax float64
	}{
		{"", 0, 0},
		{"aaaaaaaaaaaaaaaa", 0, 0.001},
		{"A1b2C3d4E5f6G7h8I9j0", 4.0, 4.4},
	}
	for _, tc := range cases {
		got := shannonEntropy(tc.in)
		if got < tc.wantMin || got > tc.wantMax {
			t.Errorf("shannonEntropy(%q) = %v, want in [%v, %v]", tc.in, got, tc.wantMin, tc.wantMax)
		}
	}
}

func TestContainsFold(t *testing.T) {
	cases := []struct {
		s, needle string
		want      bool
	}{
		{"the PassWord is set", "password", true},
		{"the PASSWORD is set", "password", true},
		{"the password is set", "password", true},
		{"nothing here", "password", false},
		{"pass", "password", false},
		{"", "password", false},
		{"anything", "", true},
		{"ends with passworD", "password", true},
	}
	for _, tc := range cases {
		if got := containsFold(tc.s, tc.needle); got != tc.want {
			t.Errorf("containsFold(%q, %q) = %v, want %v", tc.s, tc.needle, got, tc.want)
		}
	}
}

func totalCount(counts map[Kind]int) int {
	n := 0
	for _, c := range counts {
		n += c
	}
	return n
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// cleanChunk is representative transcript traffic: JSONL assistant turns, tool
// results, code and prose. It contains the words "token" and "://", so it does
// NOT short-circuit — this is the realistic steady-state cost.
const cleanChunk = `{"type":"assistant","uuid":"3f9a1c22-77bd-4e11-9c0a-2b8e5d6f7a01","timestamp":"2026-08-04T09:12:44.912Z","message":{"role":"assistant","model":"claude-fable-5","content":[{"type":"text","text":"I read internal/spool/spool.go and the writer already fsyncs on rotate, so the missing piece is the manifest checksum."},{"type":"tool_use","id":"toolu_01A","name":"Read","input":{"file_path":"/home/dev/src/loop-sessions/internal/spool/spool.go","limit":200}}],"usage":{"input_tokens":18432,"output_tokens":512,"cache_read_input_tokens":150994,"cache_creation_input_tokens":2048}}}
{"type":"user","uuid":"7c1d0e83-5a44-4f90-b3e2-9d1f6c4a8b55","timestamp":"2026-08-04T09:12:45.303Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01A","content":"package spool\n\nimport (\n\t\"bufio\"\n\t\"encoding/json\"\n\t\"os\"\n\t\"sync\"\n)\n\n// Writer appends records to a rotating on-disk spool.\ntype Writer struct {\n\tmu   sync.Mutex\n\tf    *os.File\n\tbw   *bufio.Writer\n\tsize int64\n}\n\nfunc (w *Writer) Append(rec []byte) error {\n\tw.mu.Lock()\n\tdefer w.mu.Unlock()\n\tif _, err := w.bw.Write(rec); err != nil {\n\t\treturn err\n\t}\n\treturn w.bw.WriteByte('\\n')\n}"}]}}
{"type":"assistant","uuid":"91b4f7de-2c35-4a68-8e07-1f3d5b9c2e44","timestamp":"2026-08-04T09:12:51.118Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"The rotate path writes the manifest before the data file is fsynced. If the process dies between those two operations the manifest references a file whose tail is missing. I should reorder: fsync data, then write manifest, then fsync the directory."},{"type":"text","text":"See https://www.sqlite.org/atomiccommit.html for the ordering argument. The fix is three lines in rotate()."}],"usage":{"input_tokens":21004,"output_tokens":389}}}
`

// dirtyChunk is the same shape with roughly one credential per 400 bytes, which
// is far denser than any real transcript.
const dirtyChunk = cleanChunk + `{"type":"user","message":{"role":"user","content":"deploy env:\nANTHROPIC_API_KEY=sk-ant-api03-AbCdEf1234567890AbCdEf1234567890AbCdEf12\nAWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE\naws_secret_access_key = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"\nDATABASE_URL=postgres://svcuser:S3cr3tP4ssw0rd@db.internal:5432/loop\nSLACK_BOT_TOKEN=xoxb-123456789012-1234567890123-AbCdEfGhIjKlMnOpQrStUvWx\nAuthorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV"}}
`

// pureProse short-circuits at the pre-filter: no credential markers at all.
const pureProse = `The rotate path writes the manifest before the data file is flushed to disk. If
the process dies between those two operations, the manifest references a file
whose tail is missing. Reorder so the data file is flushed first, then the
manifest is written, then the containing directory is flushed. This is the
standard ordering argument from the SQLite atomic commit documentation, and it
costs one extra syscall per rotation, which happens once every few minutes.
`

func buildInput(chunk string, targetBytes int) string {
	var b strings.Builder
	b.Grow(targetBytes + len(chunk))
	for b.Len() < targetBytes {
		b.WriteString(chunk)
	}
	return b.String()
}

func BenchmarkScrubClean(b *testing.B) {
	in := buildInput(cleanChunk, 4<<20)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if r := Scrub(in); len(r.Counts) != 0 {
			b.Fatalf("clean corpus produced redactions: %v", r.Counts)
		}
	}
}

func BenchmarkScrubDirty(b *testing.B) {
	in := buildInput(dirtyChunk, 4<<20)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if r := Scrub(in); len(r.Counts) == 0 {
			b.Fatal("dirty corpus produced no redactions")
		}
	}
}

// BenchmarkScrubPrefiltered measures the short-circuit path — text with no
// credential markers at all should never reach the regex engine.
func BenchmarkScrubPrefiltered(b *testing.B) {
	in := buildInput(pureProse, 4<<20)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if r := Scrub(in); len(r.Counts) != 0 {
			b.Fatalf("prose corpus produced redactions: %v", r.Counts)
		}
	}
}

func ExampleScrub() {
	r := Scrub(`DATABASE_URL=postgres://svcuser:S3cr3tP4ssw0rd@db.internal:5432/loop`)
	fmt.Println(r.Text)
	fmt.Println(r.Counts[KindDBPassword])
	// Output:
	// DATABASE_URL=postgres://svcuser:[REDACTED:db_password]@db.internal:5432/loop
	// 1
}

// ---------------------------------------------------------------------------
// loop-sessions tokens (skill-usage telemetry, design 10.1 "Scrub")
// ---------------------------------------------------------------------------

// The service's own bearers redact in plain text and inside a JSON-escaped
// transcript line, the prefix is what decides (a same-length hex run is a
// git SHA), and the kind is registered so a counter exists for it.
func TestScrubLoopSessionsTokens(t *testing.T) {
	const device = "lsd_AbCdEf0123456789AbCdEf0123456789"
	const source = "lss_9zYxWvUtSrQpOnMlKjIhGfEdCbA_-12"
	cases := []struct {
		name string
		in   string
		leak string
		keep string
	}{
		{name: "device token in an env line", in: "LOOP_SESSIONS_TOKEN=" + device, leak: device[:12], keep: "LOOP_SESSIONS_TOKEN="},
		{name: "source token in a curl config", in: `header = "Authorization: Bearer ` + source + `"`, leak: source[:12], keep: "Authorization: Bearer "},
		{name: "source token inside escaped JSON", in: `{"text":"config:\n-H \"Authorization: Bearer ` + source + `\"\n"}`, leak: source[:12], keep: `{"text":"config:\n-H \"Authorization: Bearer `},
		{name: "device token in prose", in: "the laptop enrolled with " + device + " yesterday", leak: device[:12], keep: " yesterday"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Scrub(tc.in)
			if strings.Contains(got.Text, tc.leak) {
				t.Errorf("leaked %q in %q", tc.leak, got.Text)
			}
			if !strings.Contains(got.Text, redaction(KindLoopSessionsToken)) {
				t.Errorf("no %s marker in %q", KindLoopSessionsToken, got.Text)
			}
			if tc.keep != "" && !strings.Contains(got.Text, tc.keep) {
				t.Errorf("prefix %q did not survive in %q", tc.keep, got.Text)
			}
			if got.Counts[KindLoopSessionsToken] != 1 {
				t.Errorf("counted %d, want 1", got.Counts[KindLoopSessionsToken])
			}
			for k, n := range got.Counts {
				if k != KindLoopSessionsToken && n != 0 {
					t.Errorf("a second kind %s fired %d times on the same token", k, n)
				}
			}
		})
	}

	// Not hits: the prefix without a long enough tail, a hex run of the
	// same length with no prefix, and the prefix glued to a word.
	for _, clean := range []string{
		"lsd_short", "lss_",
		"AbCdEf0123456789AbCdEf0123456789",
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		"xlsd_AbCdEf0123456789AbCdEf0123456789",
	} {
		if got := Scrub(clean); got.Text != clean || got.Counts[KindLoopSessionsToken] != 0 {
			t.Errorf("%q was touched: %q %v", clean, got.Text, got.Counts)
		}
	}

	found := false
	for _, k := range Kinds {
		if k == KindLoopSessionsToken {
			found = true
		}
	}
	if !found {
		t.Error("Kinds does not list loop_sessions_token; no counter would exist for it")
	}
}
