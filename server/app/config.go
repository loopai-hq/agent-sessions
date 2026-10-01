package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/loop-sessions/server/store"
	"github.com/loopai-hq/loop-sessions/server/store/derive"
)

// Config is this service's entire deployment surface.
//
// Everything comes from the environment because the process is built to run
// as a container (Cloud Run, or anything that hands a container an
// environment), where the environment IS the deployment surface. A flag would
// have nowhere to be set from, and a config file would have to be baked into
// an image that is otherwise byte-identical across revisions, which is how two
// environments end up running the same tag with different behaviour.
//
// The variable names below are the contract with whatever deploys this
// server (examples/deploy-gcp/ declares them for Cloud Run). Changing one here
// without changing it there produces a server that starts, reports nothing
// wrong, and is missing a value it needed.
//
// Required: DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD,
// SESSION_KEY, FIREBASE_PROJECT_ID, FIREBASE_API_KEY, ALLOWED_DOMAINS,
// PUBLIC_URL. Everything else is optional and documented on its field.
type Config struct {
	// Version is this build's stamp, plumbed from the binary rather than read
	// from anywhere, so the fleet page can call an agent latest or stale
	// against the build that is actually serving it. In this deployment the
	// agent release and the server are cut from the same commit, which is what
	// makes the comparison honest.
	Version string
	// BuildDate is when this build was cut; zero when unstamped.
	BuildDate time.Time

	// Port is where the listener binds. Cloud Run sets PORT and expects the
	// container to honour it rather than assume 8080.
	Port int

	// DatabaseHost is either a hostname (local development, TCP) or a directory
	// beginning with "/" (Cloud Run, where the Cloud SQL connector mounts a unix
	// socket into the container). See poolConfig for how the two are told apart.
	DatabaseHost string
	DatabasePort int
	DatabaseName string
	DatabaseUser string
	// DatabasePassword arrives as a discrete Secret Manager value and stays one.
	// It is never folded into a DSN: a URL carrying the password would be a
	// second copy of it, the two copies rotate independently, and that is
	// precisely how a credential rotation half-lands.
	DatabasePassword string

	// SessionKey signs the dashboard session cookie and the admin form tokens.
	// One key for both because cloudrun.yaml provisions one secret for both, and
	// because a per-instance key would reject a form posted to a different
	// instance than the one that rendered it.
	SessionKey []byte

	// FirebaseProjectID is the Firebase project this service authenticates
	// against, and it is the entire configuration of the token verifier: `aud`
	// on an ID token must equal it and `iss` must be the securetoken issuer
	// carrying it. One value rather than a pair of client ids, so there is
	// nothing that can disagree with anything else.
	//
	// Any Firebase project with the Google sign-in provider enabled will do;
	// a person's Firebase account lives in one project, so every deployment
	// an organisation runs should point at the same one.
	FirebaseProjectID string

	// ReleaseBucket holds the installer and the agent binaries, served through
	// this service rather than from a public bucket. Empty disables the
	// download routes entirely, which is what a deployment that distributes the
	// agent some other way wants; it is not an error.
	ReleaseBucket string

	// FirebaseAPIKey is the Firebase web API key the sign-in page hands the SDK.
	//
	// It is public by design, exactly like the config block every Firebase web
	// app ships in its bundle, and it authorises nothing on its own: it
	// names the project so the SDK knows which one to talk to. That is why it is
	// a plain environment value here rather than a Secret Manager binding, and
	// why LogValue prints it verbatim rather than as "set". Reporting it the way
	// a password is reported would tell the next reader it is a secret, and they
	// would treat a rotation, or a screenshot of the page source, as an
	// incident.
	FirebaseAPIKey string

	// FirebaseAuthDomain is where the SDK's sign-in helper is hosted. Optional:
	// empty leaves NewSignIn to apply <project>.firebaseapp.com, which is what
	// the Firebase console provisions. The default lives there, at the one place
	// that renders it into a page and a CSP, rather than being applied twice.
	FirebaseAuthDomain string

	// AllowedDomains are the email domains an identity may sit in
	// (ALLOWED_DOMAINS, comma-separated, required). An account outside them
	// cannot sign in, enrol a device, or be named as a bound actor on a
	// source token.
	//
	// Checked against the verified address alone, because a Firebase ID token
	// carries no `hd` claim: the Workspace-membership assertion the OAuth design
	// leaned on does not exist here. That makes this list and the principals
	// roster the whole of the domain control rather than a second opinion behind
	// Google's. A list rather than a string because a Workspace can carry more
	// than one domain; see emailInDomains.
	AllowedDomains []string

	// AdminEmails are the addresses that are made administrators at boot
	// (ADMIN_EMAILS, comma-separated, optional). It is how a fresh deployment
	// gets its first admin: after migrations, each address that has no
	// principals row gets an active admin row, ON CONFLICT DO NOTHING. A row
	// that already exists is left exactly as it is, whatever its role and
	// whether it is disabled, so the variable can grant a first admin but can
	// never re-grant, promote or re-enable anyone the admin page has changed,
	// and removing an address from it removes nothing. Each address must sit
	// in AllowedDomains, since one that does not could never sign in. See
	// store.BootstrapAdmins.
	AdminEmails []string

	// DomainAliases are pairs of AllowedDomains that one Google Workspace
	// serves as the same accounts (DOMAIN_ALIASES, comma-separated pairs
	// written a.example:b.example, optional). On such a pair the same local
	// part is the same person, so the export's viewer_emails lets a person
	// read their sessions under either address. Both domains of a pair must
	// appear in AllowedDomains. Empty, the default, means no aliasing: an
	// address is its own only viewer. Declare a pair only when the two
	// domains really are one Workspace; on any other pair a same-local-part
	// address could be somebody else. See store.Deployment.
	DomainAliases []store.DomainAlias

	// PublicURL is the absolute base this service is reached at. See Load for
	// why it is required here even though the contract lists it as optional.
	PublicURL string

	// SlackBotToken authorises the mirror, and its presence is what switches the
	// whole feature on.
	//
	// Optional, and empty is the shipped default. A deployment without it builds
	// no poster and mounts no preference route, so there is no way to record an
	// intention nothing will act on; see newSlackMirror. Nothing else is needed
	// to enable the mirror and nothing else can: the token is the connection to
	// a workspace, and every individual decision about whether to post is a
	// person's own row in slack_prefs rather than a variable here.
	//
	// A secret, bound from Secret Manager exactly as the database password is,
	// and never logged: see LogValue, which reports it as set or unset.
	SlackBotToken string
	// SlackSigningSecret verifies interactive payloads; absent, the buttons
	// and their endpoint are simply off.
	SlackSigningSecret string

	// Retention is how long this deployment keeps what it captured, and it is
	// the only configuration here that destroys anything.
	//
	// It is a policy rather than two integers because the rules that make a pair
	// of windows coherent — the floor, and a body window that cannot outlive the
	// session that carries it — belong beside the sweep that acts on them, not
	// beside the code that read the environment. See store.RetentionPolicy.
	//
	// Defaulted to keeping everything, which is a decision rather than an
	// omission: see defaultRetentionBodyDays for the arithmetic it was made
	// against. The consequence is that an unset variable means unbounded growth,
	// so the boot log says so in as many words every time this server starts,
	// and prices what the recommended windows would remove if somebody changed
	// their mind.
	Retention store.RetentionPolicy

	// Derive is what the derive runner is told: the daily window in which its
	// body-reading steps may run, and the rows-per-second cap on them. Both
	// default in the store to the figures the clone rehearsal was measured
	// at (02-06 in the server's TZ, 1,500 rows/s), so an unset variable is
	// the rehearsed configuration rather than an unbounded one.
	// See store.DeriveConfig.
	Derive store.DeriveConfig

	LogLevel slog.Level
}

// Environment variable names, in one place so the loader and the operator
// documentation cannot disagree about them.
const (
	envPort             = "PORT"
	envDatabaseHost     = "DATABASE_HOST"
	envDatabasePort     = "DATABASE_PORT"
	envDatabaseName     = "DATABASE_NAME"
	envDatabaseUser     = "DATABASE_USER"
	envDatabasePassword = "DATABASE_PASSWORD"
	envSessionKey       = "SESSION_KEY"
	// The Firebase project both halves of sign-in verify against. The web API
	// key is public and is set as a plain value; see Config.FirebaseAPIKey.
	envFirebaseProjectID = "FIREBASE_PROJECT_ID"
	envFirebaseAPIKey    = "FIREBASE_API_KEY"
	envReleaseBucket     = "RELEASE_BUCKET"
	// Optional; NewSignIn defaults it to <project>.firebaseapp.com.
	envFirebaseAuthDomain = "FIREBASE_AUTH_DOMAIN"
	envAllowedDomains     = "ALLOWED_DOMAINS"
	// Optional. The first admins, applied at boot (Config.AdminEmails), and
	// the domain pairs the export treats as one Workspace (Config.DomainAliases).
	envAdminEmails   = "ADMIN_EMAILS"
	envDomainAliases = "DOMAIN_ALIASES"
	envPublicURL     = "PUBLIC_URL"
	envLogLevel      = "LOG_LEVEL"
	// The two retention windows, in days. Separate variables because they trade
	// differently: the first reclaims 65% of the database and leaves every
	// derived row intact, the second is irreversible. Either accepts 0, which
	// means keep forever and is warned about at boot.
	envRetentionBodyDays    = "RETENTION_BODY_DAYS"
	envRetentionSessionDays = "RETENTION_SESSION_DAYS"
	// The derive runner's knobs. Optional: unset takes the store's
	// defaults. The window is "HH-HH" in the server's TZ or "always"; the cap is whole
	// rows per second; the batch is the rows one event_keys sub-batch
	// reads, clamped to the store's floor and cap (250..5,000), so an
	// operator can shrink it for a corpus of heavy bodies without a deploy.
	envDeriveWindow       = "DERIVE_WINDOW"
	envDeriveRowsPerSec   = "DERIVE_ROWS_PER_SEC"
	envDeriveRowsPerBatch = "DERIVE_ROWS_PER_BATCH"
	// The Slack bot token, xoxb-. Optional: unset means no mirror at all, which
	// is the shipped default and is not an error.
	envSlackBotToken      = "SLACK_BOT_TOKEN"
	envSlackSigningSecret = "LOOP_SESSIONS_SLACK_SIGNING_SECRET"
)

const (
	// Cloud Run's own default, restated so a local run without PORT set behaves
	// the way the deployed one does.
	defaultPort = 8080
	// Postgres' default, and the number the Cloud SQL socket file is named
	// after; see poolConfig.
	defaultDatabasePort = 5432
	// The cookie MAC is HMAC-SHA256 and auth.NewCookies refuses anything
	// shorter, so a key that would be rejected there is rejected here instead,
	// where the message can name the variable.
	minSessionKeyBytes = 32

	// The default retention windows: both off, so this deployment deletes
	// nothing until somebody says otherwise.
	//
	// That is an owner's decision rather than an oversight, and it was made
	// against the arithmetic. Measured, one heavy user is 58 MB a day and 65% of
	// it is the TOAST behind events.body; at fifty people that is 2.9 GB a day
	// and about a terabyte a year. Three options were priced — 90/365 settling
	// near 535 GB, 30/180 near 240 GB, and keeping everything at roughly a
	// terabyte a year, unbounded. The last was chosen: the corpus is the asset,
	// and paying for storage is preferable to losing history.
	//
	// So the sweep ships built, tested and dormant. What that buys is that
	// turning it on later is two environment variables rather than a project,
	// and that the schema support arrives now — see migration 0005, which adds
	// an index to events while that table is small enough for the statement to
	// take milliseconds.
	//
	// Nothing about zero is a special case in the code below it: zero means the
	// half is switched off, checked before any cutoff is computed, and a window
	// of zero can never become a cutoff of "now". That property is what stands
	// between this default and deleting the entire corpus on the first boot, and
	// it is pinned by its own tests in both packages.
	defaultRetentionBodyDays    = 0
	defaultRetentionSessionDays = 0

	// The windows a deployment that wants retention should start from, and the
	// numbers behind the 535 GB above. They are not applied; they are what the
	// boot log prices when retention is off, so the cost of the standing
	// decision stays visible and reversing it is a change to two variables
	// rather than a fresh analysis.
	//
	// Ninety days of bodies is a quarter of readable transcripts — long enough
	// for every reason anybody has actually opened one — and it is the window
	// that holds the expensive 65%. A year of sessions keeps what survives
	// expiry: the rollup, the cost, the search index and the shape of the work,
	// at a twentieth of the size, which is why it can afford to be four times
	// longer.
	recommendedRetentionBodyDays    = 90
	recommendedRetentionSessionDays = 365
)

// recommendedRetention is the priced-out policy the boot log reports against
// when this deployment has chosen to keep everything. It is never applied.
func recommendedRetention() store.RetentionPolicy {
	return store.RetentionPolicy{
		BodyAfter:    recommendedRetentionBodyDays * 24 * time.Hour,
		SessionAfter: recommendedRetentionSessionDays * 24 * time.Hour,
	}
}

// Load reads the configuration from the environment.
//
// getenv is a parameter so the loader can be exercised without mutating the
// process environment, which is what makes these tests safe to run in parallel
// with every other test in the package. Nil takes os.Getenv.
//
// Every problem is reported, not the first one found. A server that names one
// missing variable per restart costs an operator a deploy cycle per mistake,
// and the mistakes arrive in groups: a new environment is stood up with none of
// the secrets bound, not with one of them missing.
//
// PUBLIC_URL is required here although the contract lists it as optional. It is
// this service's only statement of its own origin, and insecureCookies derives
// the cookie's Secure attribute from its scheme. The alternative is reading the
// origin off the request, where an X-Forwarded-Proto of "http" would talk this
// server out of Secure cookies and a Host header would decide what any absolute
// link it prints points at. Deriving it safely means validating the host against
// an allowlist, at which point the allowlist is the configuration this variable
// already is. The contract sanctions exactly this trade — "a required variable
// is a worse operator experience and a much better failure mode".
//
// ADMIN_EMAILS is read here and applied by the server after migrations
// (store.BootstrapAdmins). It can only add rows that are missing, never change
// one that exists, so it is safe to leave set: the roster stays the admin
// page's once the first admin is in. It is validated against ALLOWED_DOMAINS
// because an admin address outside the allowlist could never sign in, and an
// operator who set both and can still not get in should be told at boot.
func Load(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	get := func(name string) string { return strings.TrimSpace(getenv(name)) }

	var c Config
	var bad []string
	require := func(name string) string {
		v := get(name)
		if v == "" {
			bad = append(bad, name+" is required")
		}
		return v
	}
	port := func(name string, def int) int {
		raw := get(name)
		if raw == "" {
			return def
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 65535 {
			bad = append(bad, fmt.Sprintf("%s must be a TCP port between 1 and 65535, got %q", name, raw))
			return def
		}
		return n
	}
	// days reads a retention window, in days, into a duration.
	//
	// A value that will not parse is refused rather than defaulted. Every other
	// optional variable here can afford to fall back, because the fallback is
	// merely a behaviour; this one decides what gets deleted, and a typo that
	// silently became the default would be discovered by noticing that
	// transcripts are missing.
	//
	// The ceiling is not arithmetic pedantry: a duration is nanoseconds in an
	// int64, so a pasted-in value of a few billion days overflows into a
	// negative window, and a negative window is a cutoff in the future — which
	// is to say, everything.
	days := func(name string, def int) time.Duration {
		raw := get(name)
		if raw == "" {
			return time.Duration(def) * 24 * time.Hour
		}
		const maxDays = 36500
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > maxDays {
			bad = append(bad, fmt.Sprintf(
				"%s must be a whole number of days between 0 and %d, where 0 keeps everything forever, got %q",
				name, maxDays, raw))
			return time.Duration(def) * 24 * time.Hour
		}
		return time.Duration(n) * 24 * time.Hour
	}

	c.Port = port(envPort, defaultPort)
	c.DatabasePort = port(envDatabasePort, defaultDatabasePort)

	c.DatabaseHost = require(envDatabaseHost)
	c.DatabaseName = require(envDatabaseName)
	c.DatabaseUser = require(envDatabaseUser)
	c.DatabasePassword = require(envDatabasePassword)

	if raw := require(envSessionKey); raw != "" {
		key, err := decodeKey(raw)
		switch {
		case err != nil:
			bad = append(bad, envSessionKey+" must be base64")
		case len(key) < minSessionKeyBytes:
			bad = append(bad, fmt.Sprintf("%s must decode to at least %d bytes, got %d",
				envSessionKey, minSessionKeyBytes, len(key)))
		default:
			c.SessionKey = key
		}
	}

	c.ReleaseBucket = get(envReleaseBucket)
	// Optional, and deliberately not validated for the xoxb- shape. A token this
	// server rejected at boot would be a deploy that fails on a credential the
	// workspace may well have changed the format of; slack.NewClient refuses an
	// empty one, and Slack itself is the authority on the rest.
	c.SlackBotToken = get(envSlackBotToken)
	// Optional like the token, and gating a different surface: the interactive
	// endpoint and the Stop buttons on live-thread roots. This line is the one
	// that was missing when the secret sat wired in the deploy template while
	// the route stayed absent — the declaration test now names this variable.
	c.SlackSigningSecret = get(envSlackSigningSecret)
	c.FirebaseProjectID = require(envFirebaseProjectID)
	c.FirebaseAPIKey = require(envFirebaseAPIKey)
	// Through get, not os.Getenv: Load takes its environment as a parameter so
	// these tests can run in parallel with everything else in the package
	// without mutating the process environment out from under them.
	//
	// Not validated here. NewSignIn interpolates it into a Content-Security-Policy
	// header and refuses a value that would break out of one, and that check has
	// to live beside the header it protects rather than be duplicated into a
	// second, laxer copy here.
	c.FirebaseAuthDomain = get(envFirebaseAuthDomain)

	if raw := require(envAllowedDomains); raw != "" {
		c.AllowedDomains = splitDomains(raw)
		if len(c.AllowedDomains) == 0 {
			bad = append(bad, envAllowedDomains+" must name at least one domain")
		}
	}
	inAllowed := func(domain string) bool {
		for _, d := range c.AllowedDomains {
			if d == domain {
				return true
			}
		}
		return false
	}
	// Both are validated against the allowlist only when there is one; with
	// ALLOWED_DOMAINS missing that error is already on the list and a second
	// complaint per address would only bury it.
	if raw := get(envAdminEmails); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			email := strings.ToLower(strings.TrimSpace(part))
			if email == "" {
				continue
			}
			at := strings.LastIndex(email, "@")
			if at <= 0 || at == len(email)-1 {
				bad = append(bad, fmt.Sprintf("%s: %q is not an email address", envAdminEmails, part))
				continue
			}
			if len(c.AllowedDomains) > 0 && !inAllowed(email[at+1:]) {
				bad = append(bad, fmt.Sprintf("%s: %s is not in %s, so it could never sign in", envAdminEmails, email, envAllowedDomains))
				continue
			}
			c.AdminEmails = append(c.AdminEmails, email)
		}
	}
	if raw := get(envDomainAliases); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			pair := strings.TrimSpace(part)
			if pair == "" {
				continue
			}
			a, b, ok := strings.Cut(pair, ":")
			a, b = strings.ToLower(strings.TrimSpace(a)), strings.ToLower(strings.TrimSpace(b))
			if !ok || a == "" || b == "" {
				bad = append(bad, fmt.Sprintf("%s: %q must be a pair written a.example:b.example", envDomainAliases, part))
				continue
			}
			if a == b {
				bad = append(bad, fmt.Sprintf("%s: %q pairs a domain with itself", envDomainAliases, part))
				continue
			}
			// The alias domains are spliced into the export SQL as literals
			// (store.exportViewerEmails), so they are held to the hostname
			// alphabet here, where the message can name the variable.
			if !domainShape.MatchString(a) || !domainShape.MatchString(b) {
				bad = append(bad, fmt.Sprintf("%s: %q is not a pair of domain names", envDomainAliases, part))
				continue
			}
			if len(c.AllowedDomains) > 0 && (!inAllowed(a) || !inAllowed(b)) {
				bad = append(bad, fmt.Sprintf("%s: both domains of %q must be in %s", envDomainAliases, part, envAllowedDomains))
				continue
			}
			c.DomainAliases = append(c.DomainAliases, store.DomainAlias{A: a, B: b})
		}
	}

	if raw := require(envPublicURL); raw != "" {
		base, err := publicBase(raw)
		if err != nil {
			bad = append(bad, envPublicURL+": "+err.Error())
		} else {
			c.PublicURL = base
		}
	}

	if raw := get(envLogLevel); raw != "" {
		lvl, err := parseLevel(raw)
		if err != nil {
			bad = append(bad, err.Error())
		} else {
			c.LogLevel = lvl
		}
	}

	// Both windows are read before either is judged, so a deployment that got
	// both wrong is told about both. Validate is the store's, deliberately: the
	// floor and the body-outlives-session rule describe what the sweep does, and
	// a second copy of them here is a second thing to keep correct.
	c.Retention = store.RetentionPolicy{
		BodyAfter:    days(envRetentionBodyDays, defaultRetentionBodyDays),
		SessionAfter: days(envRetentionSessionDays, defaultRetentionSessionDays),
	}
	if err := c.Retention.Validate(); err != nil {
		bad = append(bad, err.Error())
	}

	// The runner's knobs are refused rather than defaulted when they will not
	// parse, for the retention windows' reason: a mistyped window would run
	// the body-reading steps at every hour of the day, or at none, and either
	// would be discovered by noticing the database slow down or the rebuild
	// never finish.
	if raw := get(envDeriveWindow); raw != "" {
		w, err := derive.ParseWindow(raw)
		if err != nil {
			bad = append(bad, envDeriveWindow+": "+err.Error())
		} else {
			c.Derive.Window = w
		}
	}
	if raw := get(envDeriveRowsPerSec); raw != "" {
		const maxRowsPerSec = 1_000_000
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxRowsPerSec {
			bad = append(bad, fmt.Sprintf(
				"%s must be a whole number of rows per second between 1 and %d, got %q",
				envDeriveRowsPerSec, maxRowsPerSec, raw))
		} else {
			c.Derive.RowsPerSec = n
		}
	}
	if raw := get(envDeriveRowsPerBatch); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			bad = append(bad, fmt.Sprintf("%s must be a whole number of rows per batch, got %q", envDeriveRowsPerBatch, raw))
		} else {
			// Clamped rather than refused: a value outside the floor and
			// cap is a wish the runner can honour in part, and either bound
			// is a rehearsed figure the operator need not know by heart.
			c.Derive.EventRowsPerBatch = min(max(n, store.DeriveEventRowsFloor), store.DeriveEventRowsPerBatch)
		}
	}

	// TZ is in the deployment's variable table and is deliberately not read
	// here: the Go runtime consumes it when it initialises time.Local, so a
	// second reading in this package would be a second answer to what the
	// server's timezone is.

	if len(bad) > 0 {
		// The zero Config rather than a half-populated one. A caller that logs
		// the error and carries on would otherwise be carrying on with a
		// database host and no password.
		return Config{}, fmt.Errorf(
			"app: the environment does not configure this server (the variables are documented on Config in server/app/config.go): %s",
			strings.Join(bad, "; "))
	}
	return c, nil
}

// decodeKey accepts every spelling of base64 a person might paste.
//
// A key is generated once and copied through a terminal, a secret manager and
// possibly a chat window on the way to the deployment; whether it arrives
// padded, or in the URL-safe alphabet, says nothing about whether it is the
// right key. Refusing one spelling would produce a server that will not start
// and an operator who is certain they set the variable.
func decodeKey(s string) ([]byte, error) {
	for _, enc := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if b, err := enc.DecodeString(s); err == nil {
			return b, nil
		}
	}
	return nil, errors.New("not base64")
}

// domainShape is the alphabet a domain in DOMAIN_ALIASES may use: labels of
// letters, digits and hyphens joined by dots. Anything else cannot be a
// hostname and must not reach a SQL literal.
var domainShape = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$`)

// splitDomains parses the comma-separated allowlist, dropping empties so a
// trailing comma is a typo rather than a domain named "".
func splitDomains(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if d := strings.ToLower(strings.TrimSpace(p)); d != "" {
			out = append(out, d)
		}
	}
	return out
}

// publicBase validates the absolute base and returns it without a trailing
// slash, which is the form a path can be appended to without producing a double
// separator.
func publicBase(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSuffix(raw, "/"))
	if err != nil {
		return "", errors.New("must be an absolute URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("must begin with http:// or https://, got %q", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("must carry a host, got %q", raw)
	}
	// A query or fragment on the base would be silently discarded the moment a
	// path is appended, so the value an operator set and the value the service
	// uses would differ with nothing to say so.
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("must not carry a query or a fragment")
	}
	return u.String(), nil
}

// parseLevel maps the slog level names. An unrecognised value is refused rather
// than defaulted: LOG_LEVEL=warning silently serving info is the class of
// mistake that is only noticed during the incident it hid.
func parseLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(raw) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("%s must be one of debug, info, warn, error, got %q", envLogLevel, raw)
}

// insecureCookies reports whether cookies must drop the Secure attribute.
//
// Derived from the scheme of PUBLIC_URL rather than from a variable of its own,
// because the two answers have to agree and a second variable is a second thing
// to get wrong. A browser drops a __Host- cookie that is not Secure and refuses
// to send a Secure one over plain HTTP, so a deployment whose base is http://
// and whose cookies are Secure presents as "sign-in silently does nothing".
// NewSignIn refuses that combination outright, which is what makes this
// derivation load-bearing rather than a convenience.
func (c Config) insecureCookies() bool {
	return strings.HasPrefix(c.PublicURL, "http://")
}

// LogValue is what a Config looks like in a log line.
//
// It exists so that logging the configuration — at startup, or from a debug
// line somebody adds during an incident — cannot put the database password or
// the session signing key into Cloud Logging, which retains it for thirty days
// and shows it to everyone with project read access. Each is reported as
// present or absent, which is the only thing a person reading a log line needs
// to know about a secret.
//
// This server holds three secrets. Two of them are its own — the database
// password and the session signing key — and after the Firebase rehaul neither
// is an authentication credential: there is no OAuth client secret any more,
// because there is no OAuth client. The third is the Slack bot token, which is
// the one credential here that authorises this service to act somewhere else,
// and is therefore the one whose exposure is not bounded by this database.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("port", c.Port),
		slog.String("database_host", c.DatabaseHost),
		slog.Int("database_port", c.DatabasePort),
		slog.String("database_name", c.DatabaseName),
		slog.String("database_user", c.DatabaseUser),
		slog.String("database_password", secretState(c.DatabasePassword != "")),
		slog.String("session_key", secretState(len(c.SessionKey) > 0)),
		slog.String("firebase_project_id", c.FirebaseProjectID),
		// Verbatim, deliberately, and not through secretState. A Firebase web
		// API key is public by design and is already in the page source of the
		// sign-in page; reporting it as "set" alongside the password above
		// would say it is the same kind of thing, and the next person to read
		// this line would treat a rotation as an incident. Printing the value is
		// also what makes "the deploy is pointed at the wrong project" a
		// one-line diagnosis.
		slog.String("firebase_api_key", c.FirebaseAPIKey),
		// Empty here means NewSignIn's <project>.firebaseapp.com default is in
		// force; it is not defaulted before logging, so the line reports what
		// was configured rather than what was derived.
		slog.String("firebase_auth_domain", c.FirebaseAuthDomain),
		slog.Any("allowed_domains", c.AllowedDomains),
		// Addresses and domain names, not secrets: the line is how an
		// operator confirms the first admin the boot will write.
		slog.Any("admin_emails", c.AdminEmails),
		slog.Any("domain_aliases", aliasStrings(c.DomainAliases)),
		slog.String("public_url", c.PublicURL),
		// Present or absent, never the value. It is a workspace credential, and
		// whether it is set is also the whole answer to "why is nothing being
		// mirrored" — which is the question this line exists to shorten.
		slog.String("slack_bot_token", secretState(c.SlackBotToken != "")),
		// Same shape for the same reason: present-or-absent answers "why do the
		// thread roots carry no Stop button" without exposing the credential.
		slog.String("slack_signing_secret", secretState(c.SlackSigningSecret != "")),
		// The one setting here that destroys data, reported in days through the
		// policy's own LogValue. It is repeated by announceRetention at a level
		// of its own, because a value buried in the configuration group is not
		// the same as a line somebody grepping for "retention" will find.
		slog.Any("retention", c.Retention),
		// The effective values, defaults applied, through the runner's own
		// LogValue: what the operator needs to know is when the body-reading
		// steps will run, not whether the variable was set.
		slog.Any("derive", c.Derive),
		slog.String("log_level", c.LogLevel.String()),
	)
}

// aliasStrings renders the alias pairs the way DOMAIN_ALIASES spells them.
func aliasStrings(aliases []store.DomainAlias) []string {
	out := make([]string, 0, len(aliases))
	for _, a := range aliases {
		out = append(out, a.A+":"+a.B)
	}
	return out
}

func secretState(present bool) string {
	if present {
		return "set"
	}
	return "unset"
}

// NewLogger builds a JSON logger with slog's own key names: time, level, msg.
//
// It is the logger this package's tests read, and the one a developer's
// terminal shows. It is NOT what the deployed process installs, and the
// difference is the whole reason NewCloudLogger exists: Cloud Logging lifts a
// JSON line's severity, message and timestamp out of the payload only when they
// arrive under those exact keys, and slog's defaults are none of them. For a
// year every line this server wrote reached Cloud Logging with no severity at
// all, so a filter on severity>=ERROR matched nothing and no alert could be
// written against the "ingest request failed" line that already existed.
//
// Nothing in this server logs a session's content, a prompt, a device token or
// a cookie. The log is a second, less guarded copy of whatever reaches it, and
// this one holds colleagues' transcripts.
func NewLogger(level slog.Level, w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// The keys Cloud Logging reads out of a structured JSON line and moves onto
// the LogEntry itself. Everything else stays inside jsonPayload, where a
// severity filter cannot see it.
const (
	cloudSeverityKey  = "severity"
	cloudMessageKey   = "message"
	cloudTimestampKey = "timestamp"
	// Set from the request's X-Cloud-Trace-Context by the trace middleware, so
	// the Logs Explorer groups a server line with the request log entry Cloud
	// Run wrote for the same request. Without them an "ingest request failed"
	// line and the 503 it explains are two entries with nothing in common.
	cloudTraceKey        = "logging.googleapis.com/trace"
	cloudSpanKey         = "logging.googleapis.com/spanId"
	cloudTraceSampledKey = "logging.googleapis.com/trace_sampled"
)

// NewCloudLogger builds the logger the deployed process installs.
//
// Same JSON stream as NewLogger, with three keys renamed to the ones Cloud
// Logging promotes to LogEntry fields (severity, message, timestamp), WARN
// spelled the way Cloud's severity enum spells it (WARNING; there is no WARN),
// the build stamped on every line as "version", and the request's trace
// attached to any line logged with a context the trace middleware has seen.
//
// The version is an attribute rather than a once-at-boot line because two
// revisions serve side by side during a rollout, and a line that does not say
// which one wrote it cannot be used to decide whether the new one is healthy.
func NewCloudLogger(level slog.Level, w io.Writer, version string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: cloudAttrs})
	return slog.New(newCloudHandler(h)).With("version", version)
}

// cloudAttrs renames slog's built-in keys to Cloud Logging's. Only at the top
// level: a user attribute called "level" inside a group is that user's own.
func cloudAttrs(groups []string, a slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return a
	}
	switch a.Key {
	case slog.TimeKey:
		a.Key = cloudTimestampKey
	case slog.MessageKey:
		a.Key = cloudMessageKey
	case slog.LevelKey:
		a.Key = cloudSeverityKey
		if lvl, ok := a.Value.Any().(slog.Level); ok {
			a.Value = slog.StringValue(cloudSeverity(lvl))
		}
	}
	return a
}

// cloudSeverity maps a slog level onto the four Cloud severities this server
// uses. Custom levels between two named ones round down, matching how slog
// itself names them.
func cloudSeverity(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "DEBUG"
	case l < slog.LevelWarn:
		return "INFO"
	case l < slog.LevelError:
		return "WARNING"
	default:
		return "ERROR"
	}
}

// cloudHandler attaches the request trace carried in the context to the record.
//
// A wrapper rather than a change to every call site, so a line logged from
// deep inside a handler with log.ErrorContext(ctx, ...) is correlated without
// that handler knowing what a trace is. Lines logged without a context, or
// with one no request produced, are written exactly as before.
//
// Cloud Logging reads the trace keys at the top level of the line and nowhere
// else. Attributes added to a record land under whatever group the logger was
// built with, so for a logger derived through WithGroup the trace would be
// written inside the group, present and correlated with nothing, and the
// failure would be silent. The handler therefore keeps the root it was built
// from and the With and WithGroup calls made since; a grouped logger's line
// is written by a copy of the root that takes the trace first and replays
// those calls after it. That rebuild costs allocations per line and is reached
// by no logger this server builds today (none uses WithGroup), so the
// ungrouped path stays the cheap one.
type cloudHandler struct {
	// root is the handler as built, before any With or WithGroup: the only
	// handler on which WithAttrs is guaranteed to write at the top level.
	root slog.Handler
	// cur is root with every derivation applied; it writes the ordinary line.
	cur slog.Handler
	// ops are the derivations in order, for the grouped rebuild.
	ops []func(slog.Handler) slog.Handler
	// grouped is whether any op is a WithGroup, which is the only case the
	// rebuild is needed for.
	grouped bool
}

func newCloudHandler(root slog.Handler) cloudHandler {
	return cloudHandler{root: root, cur: root}
}

func (h cloudHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.cur.Enabled(ctx, level)
}

func (h cloudHandler) Handle(ctx context.Context, r slog.Record) error {
	t, ok := traceFromContext(ctx)
	if !ok {
		return h.cur.Handle(ctx, r)
	}
	attrs := []slog.Attr{slog.String(cloudTraceKey, t.trace), slog.String(cloudSpanKey, t.span)}
	if t.sampled {
		attrs = append(attrs, slog.Bool(cloudTraceSampledKey, true))
	}
	if h.grouped {
		out := h.root.WithAttrs(attrs)
		for _, op := range h.ops {
			out = op(out)
		}
		return out.Handle(ctx, r)
	}
	// Clone before adding: the record's attribute slice may be shared with
	// the caller, and appending to a shared slice is how one log line's
	// attributes turn up on another's.
	r = r.Clone()
	r.AddAttrs(attrs...)
	return h.cur.Handle(ctx, r)
}

// WithAttrs and WithGroup re-wrap, because slog.Logger.With returns whatever
// the handler returns and a bare inner handler would drop the trace from every
// component logger built with log.With("component", ...).
func (h cloudHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	return h.derive(h.cur.WithAttrs(attrs), func(x slog.Handler) slog.Handler { return x.WithAttrs(attrs) }, h.grouped)
}

func (h cloudHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.derive(h.cur.WithGroup(name), func(x slog.Handler) slog.Handler { return x.WithGroup(name) }, true)
}

// derive is the re-wrap both derivations share. The ops slice is copied
// rather than appended to in place: two loggers derived from one parent must
// not share a backing array, or the second derivation's op overwrites the
// first's and its lines are written with the wrong attributes.
func (h cloudHandler) derive(cur slog.Handler, op func(slog.Handler) slog.Handler, grouped bool) cloudHandler {
	ops := make([]func(slog.Handler) slog.Handler, len(h.ops)+1)
	copy(ops, h.ops)
	ops[len(h.ops)] = op
	return cloudHandler{root: h.root, cur: cur, ops: ops, grouped: grouped}
}

// Pool sizing, which is coupled to the Cloud Run manifest (examples/deploy-gcp/cloudrun.yaml).
const (
	// The connection budget, stated once. The Cloud SQL instance runs with
	// max_connections 400 and superuser_reserved_connections 3, so 397 are
	// usable. maxScale is 10, so the service may hold 10 x 36 = 360; the
	// hourly export job (a second process built from this same binary, on a
	// pool of 2) takes 2; that leaves 35, of which 5 are reserved for the
	// proxy sessions an operator opens during an incident or a rehearsal.
	// 360 + 2 + 5 = 367 of 397. The previous value, 45, put the service alone
	// at 450, above the instance's ceiling, and the comment beside it said the
	// tier would "start refusing them under load" as if that were somebody
	// else's problem.
	//
	// containerConcurrency is 40, so at full concurrency four requests on an
	// instance queue inside pgx rather than inside Cloud Run. That is the
	// trade: a visible queue of four against a fleet-wide connection refusal,
	// and at the measured p99 concurrency of 3 the queue never forms.
	poolMaxConns = 36
	// Two warm connections on an instance that Cloud Run keeps alive at
	// minScale 1, so the first request after an idle period does not pay for a
	// TLS handshake and a Postgres backend fork. CPU is not throttled between
	// requests here, so the pool's own maintenance actually runs.
	poolMinConns = 2
	// Cloud SQL recycles backends and a connection held for hours accumulates
	// server-side memory. Half an hour is far longer than any request and far
	// shorter than a deploy interval.
	poolMaxConnLifetime = 30 * time.Minute
	poolMaxConnIdleTime = 5 * time.Minute
	poolHealthCheck     = time.Minute
	// A connection that cannot be established in ten seconds is not going to be
	// established; failing lets the request answer 503 while the deadline is
	// still inside Cloud Run's 120s request timeout.
	poolConnectTimeout = 10 * time.Second

	// poolStatementTimeout is the ceiling on any single statement issued
	// through the pool, applied to each connection as it is opened.
	//
	// Until this existed no statement anywhere had a bound: the role, the
	// database and the instance all ran with statement_timeout off. Cloud
	// Run's 120 s request timeout cancels the HTTP context, but a background
	// loop (the retention sweep, the derive runner) and a job process have no
	// such context, and a runaway statement in one of them holds a pool
	// connection and a snapshot that keeps autovacuum from reclaiming dead
	// tuples for as long as it runs. Thirty seconds is an order of magnitude
	// above any request path's p99 and a quarter of the request timeout; the
	// passes that legitimately need longer take it explicitly with
	// store.WithStatementTimeout inside their own transaction, where the
	// override ends with the transaction instead of leaking to the next
	// borrower of the connection.
	poolStatementTimeout = 30 * time.Second
)

// setStatementTimeout is the pool's AfterConnect hook: a session-level SET on
// every new connection, so no borrower can inherit an unbounded statement.
//
// A session-level SET rather than a per-request SET LOCAL, because the point
// is a floor that holds for callers who never thought about it, and a hook
// that runs once per connection cannot be forgotten by a call site. The
// duration is rendered in whole milliseconds; SET does not take a parameter,
// so the literal is built here from a constant rather than from anything a
// request supplied.
func setStatementTimeout(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%dms'", poolStatementTimeout.Milliseconds()))
	if err != nil {
		return fmt.Errorf("app: set statement_timeout on a new connection: %w", err)
	}
	return nil
}

// poolConfig assembles the pgx configuration from the discrete parts.
//
// The connection is built from a keyword/value string carrying only the
// non-secret parts, with the password set on the parsed config afterwards. That
// ordering is the point: the password never appears in a string that could be
// logged, put in an error message by the driver, or read out of /proc.
//
// A DATABASE_HOST beginning with "/" is a directory rather than a hostname,
// which is how the Cloud SQL connector exposes the instance, and pgx dials a
// unix socket when it sees one. The port still matters in that case because the
// socket file is named after it (.s.PGSQL.5432), which is why it is always sent
// rather than omitted for the socket form.
func (c Config) poolConfig() (*pgxpool.Config, error) {
	kv := fmt.Sprintf("host=%s port=%d dbname=%s user=%s",
		quoteConnValue(c.DatabaseHost),
		c.DatabasePort,
		quoteConnValue(c.DatabaseName),
		quoteConnValue(c.DatabaseUser))

	pc, err := pgxpool.ParseConfig(kv)
	if err != nil {
		// The string carries no secret, so it is safe to let the driver's own
		// message through with it.
		return nil, fmt.Errorf("app: database connection parameters: %w", err)
	}
	pc.ConnConfig.Password = c.DatabasePassword
	pc.ConnConfig.ConnectTimeout = poolConnectTimeout

	pc.MaxConns = poolMaxConns
	pc.MinConns = poolMinConns
	pc.MaxConnLifetime = poolMaxConnLifetime
	pc.MaxConnIdleTime = poolMaxConnIdleTime
	pc.HealthCheckPeriod = poolHealthCheck
	pc.AfterConnect = setStatementTimeout
	return pc, nil
}

// quoteConnValue renders one keyword/value parameter the way libpq specifies:
// single quoted, with backslashes and quotes escaped. A host path or a database
// name is unlikely to need it, but a value that did would otherwise be parsed
// as two parameters and connect somewhere nobody named.
func quoteConnValue(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + r.Replace(v) + "'"
}
