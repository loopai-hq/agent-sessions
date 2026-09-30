// Package scrub redacts credentials from Claude Code transcript text before it
// leaves the machine that produced it.
//
// The rule set is ported from peteromallet/dataclaw (MIT) and gitleaks, with two
// deliberate departures:
//
//   - Every hit is replaced with [REDACTED:<kind>] rather than a bare marker, so
//     downstream consumers can tell a leaked Stripe key from a leaked JWT without
//     re-scanning.
//   - Rules that sit behind a meaningful prefix (Authorization headers, DSN
//     userinfo, key=value assignments) preserve the prefix and redact only the
//     value. A transcript line stays parseable as JSON after scrubbing.
//
// Over-redaction is treated as a defect, not a safe default: a transcript whose
// every long string has been blanked is worthless as a record. Rules that cannot
// be anchored to a vendor prefix are therefore gated on an adjacent secret-ish key
// name plus an entropy floor, never on entropy alone.
package scrub

import (
	"math"
	"regexp"
	"strings"
)

// Kind identifies the class of credential a redaction replaced. The value is
// what the [REDACTED:<kind>] marker names, so a reader of a scrubbed
// transcript can tell which rule fired without seeing what it removed.
type Kind string

// The kinds, one per rule in Rules.
const (
	KindAnthropicKey        Kind = "anthropic_key"
	KindOpenAIKey           Kind = "openai_key"
	KindStripeSecretKey     Kind = "stripe_secret_key"
	KindStripeRestrictedKey Kind = "stripe_restricted_key"
	KindGitHubToken         Kind = "github_token"
	KindLoopSessionsToken   Kind = "loop_sessions_token"
	KindGitHubPAT           Kind = "github_pat"
	KindAWSAccessKeyID      Kind = "aws_access_key_id"
	KindAWSSecretAccessKey  Kind = "aws_secret_access_key"
	KindGoogleAPIKey        Kind = "google_api_key"
	KindGoogleOAuthToken    Kind = "google_oauth_token"
	KindSlackToken          Kind = "slack_token"
	KindSlackWebhook        Kind = "slack_webhook"
	KindJWT                 Kind = "jwt"
	KindPrivateKeyPEM       Kind = "private_key_pem"
	KindSSHPrivateKey       Kind = "ssh_private_key"
	KindAuthBearer          Kind = "auth_bearer"
	KindAuthBasic           Kind = "auth_basic"
	KindDBPassword          Kind = "db_password"
	KindGenericSecret       Kind = "generic_secret"
)

// Kinds lists every Kind the package can emit, in rule-application order. Callers
// register metric series from this so a counter exists before its first hit.
var Kinds = []Kind{
	KindSSHPrivateKey,
	KindPrivateKeyPEM,
	KindDBPassword,
	KindSlackWebhook,
	KindAnthropicKey,
	KindOpenAIKey,
	KindStripeSecretKey,
	KindStripeRestrictedKey,
	KindGitHubPAT,
	KindGitHubToken,
	KindLoopSessionsToken,
	KindAWSAccessKeyID,
	KindGoogleAPIKey,
	KindGoogleOAuthToken,
	KindSlackToken,
	KindJWT,
	KindAWSSecretAccessKey,
	KindAuthBearer,
	KindAuthBasic,
	KindGenericSecret,
}

// Result is the output of Scrub.
type Result struct {
	// Text is the redacted input. It is the input verbatim when nothing matched.
	Text string
	// Counts holds one entry per Kind that fired, with the number of redactions.
	// Always non-nil; empty when the input was clean.
	Counts map[Kind]int
}

// redaction builds the replacement token for a kind.
func redaction(k Kind) string { return "[REDACTED:" + string(k) + "]" }

// rule is one detector. A rule runs only when at least one of its literal gates
// is present in the input, which keeps clean text off the regex engine entirely.
type rule struct {
	kind Kind
	re   *regexp.Regexp
	// gates are case-sensitive literals; foldGates are ASCII-case-insensitive.
	// The rule runs if any gate from either list is present.
	gates     []string
	foldGates []string
	// repl renders the replacement from the regex submatches. A nil repl means
	// the whole match is replaced.
	repl func(sub []string) string
	// accept vets a candidate match. A nil accept means every match is a hit.
	// Returning false leaves the text untouched and does not count.
	accept func(sub []string) bool
}

// Compiled once at package load. Never build these inside Scrub.
var rules = []rule{
	// ---- Key blocks. Multi-line, matched first so their base64 body is never
	// picked apart by the narrower rules below. `(?s)` lets `.` cross newlines;
	// it also crosses the literal backslash-n of a JSON-escaped transcript line.
	{
		kind:  KindSSHPrivateKey,
		re:    regexp.MustCompile(`(?s)-----BEGIN OPENSSH PRIVATE KEY-----.*?-----END OPENSSH PRIVATE KEY-----`),
		gates: []string{"OPENSSH PRIVATE KEY"},
	},
	{
		kind:  KindPrivateKeyPEM,
		re:    regexp.MustCompile(`(?s)-----BEGIN (?:RSA |DSA |EC |PGP |ENCRYPTED )?PRIVATE KEY-----.*?-----END (?:RSA |DSA |EC |PGP |ENCRYPTED )?PRIVATE KEY-----`),
		gates: []string{"PRIVATE KEY"},
	},

	// ---- Connection strings. Prefix-preserving: scheme and username survive so
	// the URL still parses and still says which database was involved.
	{
		kind: KindDBPassword,
		// Square brackets are excluded from the userinfo classes so a DSN that has
		// already been scrubbed cannot match its own [REDACTED:...] marker and be
		// counted twice. Real passwords are percent-encoded, so nothing is lost.
		re:        regexp.MustCompile(`((?i:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|rediss?|amqps?))://([^:@/\s"'\[\]]+):([^@\s"'\[\]]+)@`),
		foldGates: []string{"://"},
		accept:    func(sub []string) bool { return !isPlaceholderPassword(sub[3]) },
		repl:      func(sub []string) string { return sub[1] + "://" + sub[2] + ":" + redaction(KindDBPassword) + "@" },
	},

	// ---- Vendor-prefixed credentials. Each carries enough trailing entropy that
	// a bare prefix quoted in prose ("we grep for rk_live_") cannot match.
	{
		kind:  KindSlackWebhook,
		re:    regexp.MustCompile(`https://hooks\.slack\.com/services/T[A-Za-z0-9_]+/B[A-Za-z0-9_]+/[A-Za-z0-9]{20,}`),
		gates: []string{"hooks.slack.com"},
	},
	{
		kind:  KindAnthropicKey,
		re:    regexp.MustCompile(`sk-ant-[A-Za-z0-9_\-]{20,}`),
		gates: []string{"sk-ant-"},
	},
	{
		// Modern project/service keys carry an infix; legacy keys are sk- plus a
		// long alnum run. Neither can swallow sk-ant- (the hyphen stops the
		// legacy class, and the infix alternation does not list "ant").
		kind:  KindOpenAIKey,
		re:    regexp.MustCompile(`sk-(?:proj|svcacct|admin)-[A-Za-z0-9_\-]{20,}|sk-[A-Za-z0-9]{32,}`),
		gates: []string{"sk-"},
	},
	{
		// Test-mode keys (sk_test_/rk_test_) are deliberately not matched: they
		// are not sensitive and redacting them hides useful transcript detail.
		kind:  KindStripeSecretKey,
		re:    regexp.MustCompile(`sk_live_[A-Za-z0-9]{20,}`),
		gates: []string{"sk_live_"},
	},
	{
		kind:  KindStripeRestrictedKey,
		re:    regexp.MustCompile(`rk_live_[A-Za-z0-9]{20,}`),
		gates: []string{"rk_live_"},
	},
	{
		kind:  KindGitHubPAT,
		re:    regexp.MustCompile(`github_pat_[A-Za-z0-9]{22,}_[A-Za-z0-9]{59,}`),
		gates: []string{"github_pat_"},
	},
	{
		kind:  KindGitHubToken,
		re:    regexp.MustCompile(`\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{30,}\b`),
		gates: []string{"ghp_", "gho_", "ghu_", "ghs_", "ghr_"},
	},
	{
		// This service's own bearers: lsd_ device tokens and lss_ source
		// tokens (skill-usage emitters). A transcript that shows an enrol
		// or a hook's curl config carries one, and the server that stores
		// the transcript is the one the token opens. The tail alphabet is
		// what the minter emits, so a same-length hex run without the
		// prefix, a git SHA say, is not a hit. The characters need no JSON
		// escaping, so the same rule reads a token inside an escaped line.
		kind:  KindLoopSessionsToken,
		re:    regexp.MustCompile(`\bls[sd]_[A-Za-z0-9_-]{20,}`),
		gates: []string{"lss_", "lsd_"},
	},
	{
		// ASIA covers STS session credentials, which are as sensitive as AKIA and
		// carry no extra false-positive risk behind the 16-char uppercase tail.
		kind:  KindAWSAccessKeyID,
		re:    regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`),
		gates: []string{"AKIA", "ASIA"},
	},
	{
		kind:  KindGoogleAPIKey,
		re:    regexp.MustCompile(`\bAIza[A-Za-z0-9_\-]{35}\b`),
		gates: []string{"AIza"},
	},
	{
		kind:  KindGoogleOAuthToken,
		re:    regexp.MustCompile(`\bya29\.[A-Za-z0-9_\-]{20,}`),
		gates: []string{"ya29."},
	},
	{
		kind:  KindSlackToken,
		re:    regexp.MustCompile(`\bxox[baprse]-[A-Za-z0-9\-]{20,}`),
		gates: []string{"xox"},
	},
	{
		// Full three-segment form only. A bare `eyJ...` run is base64 of `{"` and
		// shows up in plenty of non-secret payloads, so partial JWTs are skipped.
		kind:  KindJWT,
		re:    regexp.MustCompile(`\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{5,}`),
		gates: []string{"eyJ"},
	},

	// ---- Name-anchored rules. The 40-char AWS secret alphabet also matches a
	// git SHA, so this only fires next to an explicit AWS key name.
	{
		kind:      KindAWSSecretAccessKey,
		re:        regexp.MustCompile(`((?i:aws_secret_access_key|aws_secret_key|secret_?access_?key))(["']?\s*[:=]\s*["']?)([A-Za-z0-9/+=]{40})`),
		foldGates: []string{"secret"},
		repl:      func(sub []string) string { return sub[1] + sub[2] + redaction(KindAWSSecretAccessKey) },
	},

	// ---- Authorization headers. Prefix-preserving so the scheme stays visible.
	{
		kind:      KindAuthBearer,
		re:        regexp.MustCompile(`\b((?i:bearer)\s+)([A-Za-z0-9_\-.=+/]{20,})`),
		foldGates: []string{"bearer"},
		accept:    func(sub []string) bool { return looksRandom(sub[2]) },
		repl:      func(sub []string) string { return sub[1] + redaction(KindAuthBearer) },
	},
	{
		kind:      KindAuthBasic,
		re:        regexp.MustCompile(`\b((?i:basic)\s+)([A-Za-z0-9+/]{16,}={0,2})`),
		foldGates: []string{"basic "},
		accept:    func(sub []string) bool { return looksRandom(sub[2]) },
		repl:      func(sub []string) string { return sub[1] + redaction(KindAuthBasic) },
	},

	// ---- Generic catch-all, last. Entropy alone is never enough: this requires
	// a secret-ish key name immediately followed by `=` or `:`. Note the name
	// alternation takes api_key/apikey but NOT a bare "key", which is what keeps
	// cache_key, idempotency_key and image_key out of the results.
	//
	// The pattern deliberately starts at the keyword rather than at the whole
	// identifier: an anchoring `[A-Za-z0-9_-]*` prefix would make every position
	// in the input a candidate start and costs an order of magnitude in scan
	// time. Any owning prefix (the SESSION_ of SESSION_SECRET=) simply falls
	// outside the match and survives untouched, which is the same output.
	{
		kind:      KindGenericSecret,
		re:        regexp.MustCompile(`((?i:password|passwd|pwd|secret|token|api[_\-]?key))(["']?\s*[:=]\s*["']?)([A-Za-z0-9_/+=.\-]{16,})`),
		foldGates: []string{"password", "passwd", "pwd", "secret", "token", "api_key", "apikey", "api-key"},
		accept:    func(sub []string) bool { return isHighEntropySecret(sub[3]) },
		repl:      func(sub []string) string { return sub[1] + sub[2] + redaction(KindGenericSecret) },
	},
}

// Scrub redacts every credential it recognises in s.
//
// Clean input is returned verbatim with an empty Counts map, having touched the
// regex engine zero times.
func Scrub(s string) Result {
	counts := make(map[Kind]int)
	if s == "" {
		return Result{Text: s, Counts: counts}
	}

	// Pre-filter: collect the rules whose literal gates appear at all. On text
	// with no credential-shaped markers this leaves `active` empty and we return
	// without compiling a single match.
	active := make([]*rule, 0, len(rules))
	for i := range rules {
		if gated(s, &rules[i]) {
			active = append(active, &rules[i])
		}
	}
	if len(active) == 0 {
		return Result{Text: s, Counts: counts}
	}

	out := s
	for _, r := range active {
		out = apply(out, r, counts)
	}
	return Result{Text: out, Counts: counts}
}

// ScrubBytes is the []byte form of Scrub. The input slice is returned unmodified
// and uncopied when nothing matched.
func ScrubBytes(b []byte) ([]byte, map[Kind]int) {
	res := Scrub(string(b))
	if len(res.Counts) == 0 {
		return b, res.Counts
	}
	return []byte(res.Text), res.Counts
}

// gated reports whether any of the rule's literal markers appear in s.
func gated(s string, r *rule) bool {
	for _, g := range r.gates {
		if strings.Contains(s, g) {
			return true
		}
	}
	for _, g := range r.foldGates {
		if containsFold(s, g) {
			return true
		}
	}
	return false
}

// apply runs one rule over s, counting each accepted hit.
//
// It uses FindAllStringSubmatchIndex rather than ReplaceAllStringFunc: the latter
// hands the callback only the whole match, forcing a second regex pass per hit to
// recover the capture groups that prefix preservation needs, and it builds an
// output buffer even when every candidate is rejected. Indices give both the
// groups and a cheap no-op path.
func apply(s string, r *rule, counts map[Kind]int) string {
	locs := r.re.FindAllStringSubmatchIndex(s, -1)
	if locs == nil {
		return s
	}

	var b strings.Builder
	last, hits := 0, 0
	for _, loc := range locs {
		sub := submatches(s, loc)
		if r.accept != nil && !r.accept(sub) {
			continue
		}
		if hits == 0 {
			b.Grow(len(s))
		}
		b.WriteString(s[last:loc[0]])
		if r.repl == nil {
			b.WriteString(redaction(r.kind))
		} else {
			b.WriteString(r.repl(sub))
		}
		last = loc[1]
		hits++
	}
	if hits == 0 {
		return s
	}

	b.WriteString(s[last:])
	counts[r.kind] += hits
	return b.String()
}

// submatches expands a FindAllStringSubmatchIndex entry into group strings.
// Group 0 is the whole match; a group that did not participate yields "".
func submatches(s string, loc []int) []string {
	sub := make([]string, len(loc)/2)
	for i := range sub {
		if start := loc[2*i]; start >= 0 {
			sub[i] = s[start:loc[2*i+1]]
		}
	}
	return sub
}

// ---------------------------------------------------------------------------
// Candidate vetting
// ---------------------------------------------------------------------------

// placeholderPrefixes are the openings of values that document a secret rather
// than being one. Matched against the lowercased value's prefix so that a real
// key which merely happens to contain "test" is still redacted.
var placeholderPrefixes = []string{
	"your", "example", "placeholder", "changeme", "change_me", "change-me",
	"dummy", "sample", "insert", "todo", "fixme", "redacted", "xxxx", "....",
	"process.env", "env.", "os.environ", "null", "none", "undefined",
}

func looksLikePlaceholder(v string) bool {
	l := strings.ToLower(v)
	for _, p := range placeholderPrefixes {
		if strings.HasPrefix(l, p) {
			return true
		}
	}
	// A shell or template interpolation is a reference, not a credential.
	return strings.Contains(l, "process.env") || strings.Contains(l, "os.environ")
}

// placeholderPasswords are the userinfo values that appear in documentation DSNs.
// Without these, the sentence "30 files contain postgres://user:password@ DSNs"
// would itself be redacted.
var placeholderPasswords = map[string]bool{
	"password": true, "pass": true, "passwd": true, "pwd": true, "secret": true,
	"mypassword": true, "yourpassword": true, "changeme": true, "hunter2": true,
	"xxxx": true, "xxxxx": true, "xxxxxx": true, "redacted": true, "example": true,
}

func isPlaceholderPassword(v string) bool {
	return placeholderPasswords[strings.ToLower(v)] || looksLikePlaceholder(v)
}

// looksRandom rejects ordinary English that happens to sit behind an auth scheme
// keyword — neither "Basic internationalization support" nor
// "Bearer authentication_is_required_here" is a credential. Real base64 and real
// opaque tokens carry digits, mixed case, or base64 padding.
//
// Only +, / and = count as evidence. Underscore, hyphen and dot are deliberately
// excluded: they are the punctuation of ordinary identifiers, and counting them
// would readmit every snake_case English phrase.
func looksRandom(v string) bool {
	var hasUpper, hasLower, hasDigit, hasBase64Punct bool
	for i := range len(v) {
		switch c := v[i]; {
		case c >= 'A' && c <= 'Z':
			hasUpper = true
		case c >= 'a' && c <= 'z':
			hasLower = true
		case c >= '0' && c <= '9':
			hasDigit = true
		case c == '+' || c == '/' || c == '=':
			hasBase64Punct = true
		}
	}
	return hasDigit || hasBase64Punct || (hasUpper && hasLower)
}

// minSecretEntropy is the Shannon floor, in bits per character, for a value to
// count as a generic secret. It clears degenerate runs ("aaaaaaaaaaaaaaaa" = 0)
// while staying under a real passphrase (~3.4) and well under base64 (~5.0).
const minSecretEntropy = 3.0

// codeExpressionPrefixes open a value that is source code, not a credential.
var codeExpressionPrefixes = []string{"self.", "cls.", "this.", "obj.", "ctx.", "cfg."}

// isHighEntropySecret vets the value of a `<secret-ish name> = <value>` pair.
//
// Entropy alone is not sufficient, and measurement says so. Scanning 2.81 GB of
// real transcripts with an entropy-only rule produced 7519 hits across 1259
// files, and the most frequent "secrets" were ordinary source code —
// `secret_name=self.credential_secret_name`, `token: synapse_ingestion_service`.
// Identifiers score 3.5-3.7 bits, comfortably above any floor that still admits
// a 16-character key, so entropy cannot separate them.
//
// The discriminator that does work is character-class shape: real credentials
// carry digits, mixed case, or base64 padding, whereas snake_case identifiers are
// uniformly lower-case letters and underscores.
func isHighEntropySecret(v string) bool {
	if looksLikePlaceholder(v) || looksLikeCodeExpression(v) {
		return false
	}
	if !looksRandom(v) {
		return false
	}
	return shannonEntropy(v) >= minSecretEntropy
}

func looksLikeCodeExpression(v string) bool {
	// A leading underscore is a private identifier, never a credential.
	if strings.HasPrefix(v, "_") {
		return true
	}
	l := strings.ToLower(v)
	for _, p := range codeExpressionPrefixes {
		if strings.HasPrefix(l, p) {
			return true
		}
	}
	return false
}

// shannonEntropy returns the per-byte Shannon entropy of s.
func shannonEntropy(s string) float64 {
	if s == "" {
		return 0
	}
	var freq [256]int
	for i := range len(s) {
		freq[s[i]]++
	}
	n := float64(len(s))
	var h float64
	for _, c := range freq {
		if c == 0 {
			continue
		}
		p := float64(c) / n
		h -= p * math.Log2(p)
	}
	return h
}

// ---------------------------------------------------------------------------
// Case-insensitive literal search
// ---------------------------------------------------------------------------

// containsFold reports whether needle occurs in s under ASCII case folding.
//
// It exists so the pre-filter can stay allocation-free: lowercasing a multi-MB
// transcript to run strings.Contains would cost a full copy per call. Candidate
// positions come from strings.IndexByte, which is vectorised, and only those get
// a window comparison.
//
// The two casings of the first byte are tracked with independent cursors. The
// obvious formulation — search both from the current offset on every candidate —
// is quadratic whenever one casing is common and the other is absent: every 'p'
// in the text triggers a fresh full-length scan for a 'P' that is never there.
// Advancing only the cursor that was consumed keeps this linear.
func containsFold(s, needle string) bool {
	n := len(needle)
	if n == 0 {
		return true
	}
	if len(s) < n {
		return false
	}
	limit := len(s) - n
	lo, up := lowerASCII(needle[0]), upperASCII(needle[0])

	if lo == up {
		// Non-alphabetic first byte: a single vectorised scan suffices.
		for i := 0; i <= limit; i++ {
			j := indexByteFrom(s, lo, i, limit)
			if j < 0 {
				return false
			}
			if strings.EqualFold(s[j:j+n], needle) {
				return true
			}
			i = j
		}
		return false
	}

	nextLo := indexByteFrom(s, lo, 0, limit)
	nextUp := indexByteFrom(s, up, 0, limit)
	for {
		i := nextLo
		if i < 0 || (nextUp >= 0 && nextUp < i) {
			i = nextUp
		}
		if i < 0 {
			return false
		}
		if strings.EqualFold(s[i:i+n], needle) {
			return true
		}
		if nextLo == i {
			nextLo = indexByteFrom(s, lo, i+1, limit)
		}
		if nextUp == i {
			nextUp = indexByteFrom(s, up, i+1, limit)
		}
	}
}

// indexByteFrom returns the index of the first c in s at or after from and at or
// before limit, or -1.
func indexByteFrom(s string, c byte, from, limit int) int {
	if from > limit {
		return -1
	}
	j := strings.IndexByte(s[from:limit+1], c)
	if j < 0 {
		return -1
	}
	return from + j
}

func lowerASCII(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

func upperASCII(c byte) byte {
	if c >= 'a' && c <= 'z' {
		return c - ('a' - 'A')
	}
	return c
}

// Func returns Scrub with the plain-string count map that every consumer
// actually wants.
//
// Scrub's own Counts are keyed by Kind, which is right for this package's
// internals and wrong for everyone else: capture, drain and the ingest server
// each need map[string]int, and each was independently writing the same
// three-line conversion. A conversion that three unrelated packages have to
// rediscover belongs here, once, next to the type it converts.
func Func(s string) (string, map[string]int) {
	r := Scrub(s)
	if len(r.Counts) == 0 {
		return r.Text, nil
	}
	out := make(map[string]int, len(r.Counts))
	for k, v := range r.Counts {
		out[string(k)] = v
	}
	return r.Text, out
}
