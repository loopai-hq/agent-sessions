package app

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Helpers here carry a boot prefix. Package app holds every adapter and every
// writer's fixtures, so a bare fakeDB or newConfig would collide with the next
// file that needs one.

// bootSessionKey is a valid 32-byte key in the padded standard alphabet, which
// is what `openssl rand -base64 32` produces.
const bootSessionKey = "c2Vzc2lvbi1rZXktMzItYnl0ZXMtZm9yLXRlc3RzISE="

// bootEnv is a complete, valid environment, matching the shape
// examples/deploy-gcp/cloudrun.yaml sets.
func bootEnv() map[string]string {
	return map[string]string{
		"PORT":                "8080",
		"DATABASE_HOST":       "/cloudsql/proj:asia-south1:sessions",
		"DATABASE_NAME":       "loop_sessions",
		"DATABASE_USER":       "loop_sessions",
		"DATABASE_PASSWORD":   "s3cr3t-from-secret-manager",
		"SESSION_KEY":         bootSessionKey,
		"FIREBASE_PROJECT_ID": "example-project-12345",
		// Shaped like a real Firebase web API key rather than a placeholder,
		// because these tests assert it is printed rather than redacted and a
		// value that did not look like a key would make that assertion read as a
		// mistake.
		"FIREBASE_API_KEY": "AIzaSyTestKeyNotARealCredential000000000",
		// Derived from the fixture addresses the package's tests sign in
		// with, so the allowlist and the fixtures cannot drift apart.
		"ALLOWED_DOMAINS": strings.Join(testDomainsOf("dev@example.com", "dev@example.org"), ","),
		"PUBLIC_URL":      "https://sessions.example.com",
	}
}

// bootGetenv turns a map into the lookup Load takes, so a test never mutates the
// process environment and every test in this package stays safe to run in
// parallel with every other.
func bootGetenv(env map[string]string) func(string) string {
	return func(k string) string { return env[k] }
}

// bootRecordingGetenv also records which names were looked up, in order.
func bootRecordingGetenv(env map[string]string, seen *[]string) func(string) string {
	return func(k string) string {
		*seen = append(*seen, k)
		return env[k]
	}
}

// TestConfigNamesEveryMissingVariableAtOnce defends the contract's fail-fast
// rule. A server that reports one missing variable per restart costs an
// operator a deploy cycle per mistake, and the mistakes arrive in groups: a new
// environment is stood up with none of the secrets bound rather than with one
// of them missing.
func TestConfigNamesEveryMissingVariableAtOnce(t *testing.T) {
	allRequired := []string{
		"DATABASE_HOST", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD",
		"SESSION_KEY", "FIREBASE_PROJECT_ID", "FIREBASE_API_KEY",
		"ALLOWED_DOMAINS", "PUBLIC_URL",
	}

	tests := []struct {
		name    string
		env     map[string]string
		want    []string
		notWant []string
	}{
		{
			name: "an empty environment names all nine required variables in one failure",
			env:  map[string]string{},
			want: allRequired,
		},
		{
			name: "the four database parts are named separately so a half-bound secret is visible",
			env: func() map[string]string {
				e := bootEnv()
				delete(e, "DATABASE_PASSWORD")
				delete(e, "DATABASE_USER")
				return e
			}(),
			want:    []string{"DATABASE_PASSWORD", "DATABASE_USER"},
			notWant: []string{"DATABASE_HOST", "DATABASE_NAME", "SESSION_KEY"},
		},
		{
			name: "one missing variable names that one and accuses no other",
			env: func() map[string]string {
				e := bootEnv()
				delete(e, "SESSION_KEY")
				return e
			}(),
			want:    []string{"SESSION_KEY"},
			notWant: []string{"DATABASE_HOST", "PUBLIC_URL", "FIREBASE_PROJECT_ID"},
		},
		{
			name: "a missing variable and an unusable one are reported together",
			env: func() map[string]string {
				e := bootEnv()
				delete(e, "ALLOWED_DOMAINS")
				e["PORT"] = "http"
				e["LOG_LEVEL"] = "verbose"
				return e
			}(),
			want: []string{"ALLOWED_DOMAINS", "PORT", "LOG_LEVEL"},
		},
		{
			name: "whitespace is not a value",
			env: func() map[string]string {
				e := bootEnv()
				e["FIREBASE_API_KEY"] = "   "
				return e
			}(),
			want: []string{"FIREBASE_API_KEY"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(bootGetenv(tc.env))
			if err == nil {
				t.Fatalf("Load succeeded on an environment missing %v", tc.want)
			}
			msg := err.Error()
			for _, name := range tc.want {
				if !strings.Contains(msg, name) {
					t.Errorf("failure does not name %s: %s", name, msg)
				}
			}
			for _, name := range tc.notWant {
				if strings.Contains(msg, name) {
					t.Errorf("failure names %s, which was set: %s", name, msg)
				}
			}
			// A half-populated Config is worse than none: a caller that logged
			// the error and carried on would be carrying on with a database host
			// and no password.
			if !reflect.DeepEqual(cfg, Config{}) {
				t.Errorf("Load returned a populated Config alongside its failure: %+v", cfg)
			}
		})
	}
}

// TestConfigRefusesAValueItCannotUse defends the other half of fail-fast: a
// value that is present and wrong has to fail at boot, where a deploy shows it,
// rather than at the first request that needs it.
func TestConfigRefusesAValueItCannotUse(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
		want string
	}{
		{"a port that is not a number", "PORT", "http", "PORT"},
		{"a port below the range", "PORT", "0", "PORT"},
		{"a port above the range", "PORT", "70000", "PORT"},
		{"a database port that is not a number", "DATABASE_PORT", "postgres", "DATABASE_PORT"},
		{"a session key that is not base64", "SESSION_KEY", "not base64 at all!", "SESSION_KEY"},
		{"a session key too short to sign with", "SESSION_KEY", base64.StdEncoding.EncodeToString(make([]byte, 16)), "at least 32 bytes"},
		{"an allowlist of nothing but separators", "ALLOWED_DOMAINS", " , , ", "ALLOWED_DOMAINS"},
		{"a public url with no scheme", "PUBLIC_URL", "sessions.example.com", "PUBLIC_URL"},
		{"a public url with no host", "PUBLIC_URL", "https://", "PUBLIC_URL"},
		{"a public url carrying a query", "PUBLIC_URL", "https://sessions.example.com/?next=/", "PUBLIC_URL"},
		{"a log level nobody defined", "LOG_LEVEL", "verbose", "LOG_LEVEL"},
		{"a derive batch that is not a number", "DERIVE_ROWS_PER_BATCH", "many", "DERIVE_ROWS_PER_BATCH"},
		{"a derive batch of nothing", "DERIVE_ROWS_PER_BATCH", "0", "DERIVE_ROWS_PER_BATCH"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := bootEnv()
			env[tc.key] = tc.val
			_, err := Load(bootGetenv(env))
			if err == nil {
				t.Fatalf("Load accepted %s=%q", tc.key, tc.val)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("failure does not mention %q: %s", tc.want, err)
			}
		})
	}
}

// TestConfigDefaultsAreTheOnesTheDeploymentAssumes pins the three optional
// variables to the values examples/deploy-gcp/cloudrun.yaml expects when they are
// absent. A default that drifted from the deployment would be found by a
// listener bound to the wrong port on a platform that health-checks the right
// one.
func TestConfigDefaultsAreTheOnesTheDeploymentAssumes(t *testing.T) {
	env := bootEnv()
	delete(env, "PORT")

	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Port != 8080 {
		t.Errorf("PORT default = %d, want 8080 (Cloud Run's own default)", cfg.Port)
	}
	if cfg.DatabasePort != 5432 {
		t.Errorf("DATABASE_PORT default = %d, want 5432", cfg.DatabasePort)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LOG_LEVEL default = %v, want info", cfg.LogLevel)
	}

	t.Run("a port the platform names is honoured", func(t *testing.T) {
		env := bootEnv()
		env["PORT"] = "9090"
		cfg, err := Load(bootGetenv(env))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Port != 9090 {
			t.Errorf("Port = %d, want 9090", cfg.Port)
		}
	})

	t.Run("the domain allowlist is a set, never one string", func(t *testing.T) {
		env := bootEnv()
		env["ALLOWED_DOMAINS"] = " Example.COM , example.org ,"
		cfg, err := Load(bootGetenv(env))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		// Two domains alias one another on this Workspace and two of the four
		// named admins sit on each, so a loader that kept only the first value
		// locks half the admins out of the tool they administer. The trailing
		// separator is a typo, not a third domain named "".
		want := []string{"example.com", "example.org"}
		if !reflect.DeepEqual(cfg.AllowedDomains, want) {
			t.Errorf("AllowedDomains = %q, want %q", cfg.AllowedDomains, want)
		}
	})

	t.Run("a session key is accepted in every spelling of base64", func(t *testing.T) {
		raw := make([]byte, 32)
		for i := range raw {
			raw[i] = byte(i) + 200 // forces bytes that differ between the two alphabets
		}
		for _, enc := range []*base64.Encoding{
			base64.StdEncoding, base64.RawStdEncoding,
			base64.URLEncoding, base64.RawURLEncoding,
		} {
			env := bootEnv()
			env["SESSION_KEY"] = enc.EncodeToString(raw)
			cfg, err := Load(bootGetenv(env))
			if err != nil {
				t.Fatalf("Load with %T-encoded key: %v", enc, err)
			}
			if !bytes.Equal(cfg.SessionKey, raw) {
				t.Errorf("key decoded to %x, want %x", cfg.SessionKey, raw)
			}
		}
	})
}

// TestDeriveBatchIsClampedToTheRunnersBounds pins the one knob that is
// clamped rather than refused: DERIVE_ROWS_PER_BATCH lets an operator shrink
// the event_keys sub-batch for a corpus of heavy bodies without a deploy
// (review-2 finding 22), and a value past either bound lands on the bound.
func TestDeriveBatchIsClampedToTheRunnersBounds(t *testing.T) {
	for raw, want := range map[string]int{"": 0, "1000": 1000, "250": 250, "5000": 5000, "100": 250, "90000": 5000} {
		env := bootEnv()
		if raw != "" {
			env["DERIVE_ROWS_PER_BATCH"] = raw
		}
		cfg, err := Load(bootGetenv(env))
		if err != nil {
			t.Fatalf("Load with DERIVE_ROWS_PER_BATCH=%q: %v", raw, err)
		}
		if cfg.Derive.EventRowsPerBatch != want {
			t.Errorf("DERIVE_ROWS_PER_BATCH=%q -> %d, want %d", raw, cfg.Derive.EventRowsPerBatch, want)
		}
	}
}

// TestConfigReadsExactlyTheVariablesTheDeploymentDeclares is the tripwire on
// dead configuration in both directions. A variable read here and absent from
// the deployment manifest is a value nobody can set; a variable set there and
// never read is a setting that looks live and does nothing.
//
// SEED_ADMINS is the named case: a withdrawn spelling of what ADMIN_EMAILS now
// does, and it must not come back under the old name, because a variable that
// is read, respected and can never take effect sends the next person looking
// for a bug somewhere real.
//
// The GOOGLE_OAUTH_ names are the second named case, and the reason they are
// checked by prefix rather than one by one. Sign-in goes through Firebase and
// there is no OAuth client of this server's own; reading one of those names
// again would be the first step back towards a flow that needs one.
func TestConfigReadsExactlyTheVariablesTheDeploymentDeclares(t *testing.T) {
	env := bootEnv()
	env["LOG_LEVEL"] = "debug"
	env["TZ"] = "Asia/Kolkata"
	env["SEED_ADMINS"] = "attacker@example.com"
	env["GOOGLE_OAUTH_CLIENT_ID"] = "web-client.apps.googleusercontent.com"
	env["GOOGLE_OAUTH_CLIENT_SECRET"] = "GOCSPX-should-never-be-read"
	env["GOOGLE_OAUTH_DESKTOP_CLIENT_ID"] = "desktop-client.apps.googleusercontent.com"
	env["GOOGLE_OAUTH_DESKTOP_CLIENT_SECRET"] = "GOCSPX-should-never-be-read-either"

	var seen []string
	if _, err := Load(bootRecordingGetenv(env, &seen)); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// TZ is absent on purpose: the Go runtime consumes it when it initialises
	// time.Local, and a second reading here would be a second answer to what the
	// server's timezone is.
	want := []string{
		// Optional: the first admins, applied at boot and never re-applied
		// to a row that exists.
		"ADMIN_EMAILS",
		"ALLOWED_DOMAINS",
		"DATABASE_HOST",
		"DATABASE_NAME",
		"DATABASE_PASSWORD",
		"DATABASE_PORT",
		"DATABASE_USER",
		// The derive runner's knobs. Optional: unset takes the store's
		// rehearsed defaults (02-06 in the server's TZ, 1,500 rows/s, 5,000 rows per
		// batch), which is why the deployment need not set them and why a
		// set value is validated.
		"DERIVE_ROWS_PER_BATCH",
		"DERIVE_ROWS_PER_SEC",
		"DERIVE_WINDOW",
		// Optional: the domain pairs the export's viewer_emails treats as
		// one Workspace. Empty means no aliasing.
		"DOMAIN_ALIASES",
		"FIREBASE_API_KEY",
		// Read but not required. NewSignIn defaults it to
		// <project>.firebaseapp.com, which is what the console provisions, so
		// the deployment sets it only for a custom auth domain.
		"FIREBASE_AUTH_DOMAIN",
		"FIREBASE_PROJECT_ID",
		"LOG_LEVEL",
		// Optional, and the Slack-side control surface: with it, the
		// interactivity endpoint mounts and live-thread roots carry a Stop
		// button; without it, the route is absent and the roots are plain.
		// This entry exists because the field once sat declared and wired
		// while nothing loaded it, and every deploy shipped buttonless.
		"LOOP_SESSIONS_SLACK_SIGNING_SECRET",
		"PORT",
		"PUBLIC_URL",
		// Optional. Empty leaves the download routes unmounted, which is what a
		// deployment that ships the agent some other way wants.
		"RELEASE_BUCKET",
		// The two retention windows. Optional, and defaulted to windows that
		// delete: unset must not mean "keep everything forever", because that
		// is the behaviour this service already had and the reason it was on
		// course for a terabyte a year.
		"RETENTION_BODY_DAYS",
		"RETENTION_SESSION_DAYS",
		"SESSION_KEY",
		// Optional, and the whole of the Slack mirror's deployment surface.
		// Unset means no poster and no preference route, which is the shipped
		// default: these are messages about somebody's work in a shared
		// workspace, so the capability is bound deliberately and each person
		// still opts in individually.
		"SLACK_BOT_TOKEN",
	}
	got := append([]string(nil), seen...)
	sort.Strings(got)
	got = bootUnique(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Load read %v\nwant %v", got, want)
	}
	for _, name := range seen {
		if name == "SEED_ADMINS" {
			t.Fatal("Load read SEED_ADMINS; the variable is ADMIN_EMAILS, and a second name for it is one that can silently do nothing")
		}
		if strings.HasPrefix(name, "GOOGLE_OAUTH_") {
			t.Fatalf("Load read %s; sign-in goes through Firebase and this server has no OAuth client of its own", name)
		}
	}
}

// TestConfigIgnoresTheWithdrawnSeedVariable is the structural half of the same
// rule: a value set under the withdrawn name reaches no field at all, by name
// or by content. The variable that does carry the first admins is ADMIN_EMAILS
// (config_admin_test.go), and its whole defence against re-running on every
// instance of every revision is that it can only add.
func TestConfigIgnoresTheWithdrawnSeedVariable(t *testing.T) {
	env := bootEnv()
	env["SEED_ADMINS"] = "attacker@example.com,someone@example.com"
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.AdminEmails) != 0 {
		t.Errorf("AdminEmails = %v with only SEED_ADMINS set", cfg.AdminEmails)
	}
	rt := reflect.TypeOf(cfg)
	rv := reflect.ValueOf(cfg)
	for i := range rt.NumField() {
		name := strings.ToLower(rt.Field(i).Name)
		if strings.Contains(name, "seed") || strings.Contains(name, "roster") {
			t.Errorf("Config carries a field named %s", rt.Field(i).Name)
		}
		if strings.Contains(bootRender(rv.Field(i)), "attacker@example.com") {
			t.Errorf("Config field %s carries the withdrawn SEED_ADMINS value", rt.Field(i).Name)
		}
	}
}

// TestPoolConfigKeepsThePasswordOutOfEveryString defends the reason the
// connection is assembled from parts. A DSN carrying the password would be a
// second copy of it inside a longer string, and the two copies rotate
// independently, which is how a rotation half-lands. It is also the string a
// driver quotes back in an error and a process lists in /proc.
func TestPoolConfigKeepsThePasswordOutOfEveryString(t *testing.T) {
	env := bootEnv()
	env["DATABASE_PASSWORD"] = "correct-horse-battery-staple"
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	pc, err := cfg.poolConfig()
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	if pc.ConnConfig.Password != "correct-horse-battery-staple" {
		t.Fatalf("Password = %q, want the configured one", pc.ConnConfig.Password)
	}
	for _, s := range []string{pc.ConnString(), pc.ConnConfig.ConnString()} {
		if strings.Contains(s, "correct-horse-battery-staple") {
			t.Errorf("the password appears in a connection string: %s", s)
		}
	}
}

// TestPoolConfigHandlesASocketDirectoryAndAHostnameAlike defends the two forms
// DATABASE_HOST takes. On Cloud Run the Cloud SQL connector mounts a unix
// socket directory and there is no hostname to resolve; locally it is TCP.
// Sending the port in both cases is deliberate: the socket file is named after
// it, so dropping it for the socket form looks for .s.PGSQL.0.
func TestPoolConfigHandlesASocketDirectoryAndAHostnameAlike(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		port     string
		wantHost string
		wantPort uint16
	}{
		{
			name:     "the cloud sql socket directory is passed through as the host",
			host:     "/cloudsql/proj:asia-south1:sessions",
			port:     "",
			wantHost: "/cloudsql/proj:asia-south1:sessions",
			wantPort: 5432,
		},
		{
			name:     "a hostname and an explicit port are used as given",
			host:     "127.0.0.1",
			port:     "5433",
			wantHost: "127.0.0.1",
			wantPort: 5433,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := bootEnv()
			env["DATABASE_HOST"] = tc.host
			if tc.port != "" {
				env["DATABASE_PORT"] = tc.port
			}
			cfg, err := Load(bootGetenv(env))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			pc, err := cfg.poolConfig()
			if err != nil {
				t.Fatalf("poolConfig: %v", err)
			}
			if pc.ConnConfig.Host != tc.wantHost {
				t.Errorf("Host = %q, want %q", pc.ConnConfig.Host, tc.wantHost)
			}
			if pc.ConnConfig.Port != tc.wantPort {
				t.Errorf("Port = %d, want %d", pc.ConnConfig.Port, tc.wantPort)
			}
			if pc.ConnConfig.Database != "loop_sessions" || pc.ConnConfig.User != "loop_sessions" {
				t.Errorf("dbname/user = %q/%q, want loop_sessions/loop_sessions",
					pc.ConnConfig.Database, pc.ConnConfig.User)
			}
			// The connection budget, per the comment on poolMaxConns: the
			// instance allows 397 usable connections, maxScale is 10, the
			// export job takes 2 and 5 are kept for an operator. A pool that
			// breaks that arithmetic is a fleet-wide connection refusal under
			// load, which is worse than the four requests that queue inside
			// pgx at containerConcurrency 40.
			if pc.MaxConns != poolMaxConns {
				t.Errorf("MaxConns = %d, want poolMaxConns (%d)", pc.MaxConns, poolMaxConns)
			}
			if budget := 10*pc.MaxConns + 2 + 5; budget > 397 {
				t.Errorf("10 x MaxConns + 2 (export job) + 5 (admin) = %d, over the instance's 397 usable connections", budget)
			}
			if pc.AfterConnect == nil {
				t.Error("no AfterConnect hook, so pooled connections carry no statement_timeout")
			}
		})
	}

	t.Run("a value needing quoting reaches the driver whole", func(t *testing.T) {
		env := bootEnv()
		env["DATABASE_NAME"] = `loop's sessions`
		cfg, err := Load(bootGetenv(env))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		pc, err := cfg.poolConfig()
		if err != nil {
			t.Fatalf("poolConfig: %v", err)
		}
		// Unquoted, this would parse as two parameters and connect to a
		// database nobody named.
		if pc.ConnConfig.Database != `loop's sessions` {
			t.Errorf("Database = %q, want %q", pc.ConnConfig.Database, `loop's sessions`)
		}
	})
}

// TestCookieSecurityFollowsThePublicURLScheme defends the one derived setting.
// A browser refuses to send a Secure cookie over plain HTTP and drops a __Host-
// cookie that is not Secure, so a deployment whose base is http:// and whose
// cookies are Secure presents as "sign-in silently does nothing".
func TestCookieSecurityFollowsThePublicURLScheme(t *testing.T) {
	tests := []struct {
		name         string
		publicURL    string
		wantInsecure bool
	}{
		{"a deployed https base keeps cookies Secure", "https://sessions.example.com", false},
		{"a local http base drops Secure so the browser will send the cookie", "http://localhost:8080", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := bootEnv()
			env["PUBLIC_URL"] = tc.publicURL
			cfg, err := Load(bootGetenv(env))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := cfg.insecureCookies(); got != tc.wantInsecure {
				t.Errorf("insecureCookies() = %v, want %v", got, tc.wantInsecure)
			}
		})
	}
}

// TestConfigRedactsItsSecretsWhenLogged defends the log against becoming the
// place the credentials leak. Cloud Logging keeps entries for thirty days and
// shows them to everyone with project read access, so a debug line somebody
// adds during an incident must not be able to put the database password or the
// cookie signing key there.
func TestConfigRedactsItsSecretsWhenLogged(t *testing.T) {
	env := bootEnv()
	env["DATABASE_PASSWORD"] = "pw-must-not-appear"
	env["SESSION_KEY"] = base64.StdEncoding.EncodeToString([]byte("key-must-not-appear-0123456789ab"))
	env["LOOP_SESSIONS_SLACK_SIGNING_SECRET"] = "signing-must-not-appear"
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	var buf bytes.Buffer
	NewLogger(slog.LevelInfo, &buf).Info("starting", "config", cfg)
	line := buf.String()

	if cfg.SlackSigningSecret != "signing-must-not-appear" {
		t.Errorf("Load did not carry the signing secret into the config: %q", cfg.SlackSigningSecret)
	}
	for _, secret := range []string{"pw-must-not-appear", "key-must-not-appear", "signing-must-not-appear"} {
		if strings.Contains(line, secret) {
			t.Errorf("a secret reached the log: %s", line)
		}
	}
	// Present-or-absent is what an operator actually needs from a log line
	// about a secret, and it is what turns "the deploy forgot the secret" into
	// a one-line diagnosis.
	for _, want := range []string{`"database_password":"set"`, `"session_key":"set"`,
		`"slack_signing_secret":"set"`} {
		if !strings.Contains(line, want) {
			t.Errorf("log line does not report %s: %s", want, line)
		}
	}
	// The host is not a secret and is the field most likely to be wrong.
	if !strings.Contains(line, "/cloudsql/proj:asia-south1:sessions") {
		t.Errorf("log line does not carry the database host: %s", line)
	}
}

// TestTheFirebaseAPIKeyIsLoggedAsAValueAndNotAsASecret defends the one thing
// about this variable that is easy to get wrong in a way nothing else catches.
//
// A Firebase web API key is public by design. It is in the page source of every
// frontend on the project, it authorises nothing on its own, and the controls are the
// project's Firebase configuration, the ID token verification and the roster.
// Printing it as "set" beside the database password would say it is the same
// kind of thing, and the next person to read that line would treat a rotation,
// or a screenshot of the sign-in page, as an incident. Printing the value is
// also what makes "this revision is pointed at the wrong project" legible from
// one log line.
func TestTheFirebaseAPIKeyIsLoggedAsAValueAndNotAsASecret(t *testing.T) {
	t.Parallel()
	env := bootEnv()
	env["FIREBASE_API_KEY"] = "AIzaSyPublicByDesign00000000000000000000"
	env["FIREBASE_PROJECT_ID"] = "example-project-12345"
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	var buf bytes.Buffer
	NewLogger(slog.LevelInfo, &buf).Info("starting", "config", cfg)
	line := buf.String()

	for _, want := range []string{
		`"firebase_api_key":"AIzaSyPublicByDesign00000000000000000000"`,
		`"firebase_project_id":"example-project-12345"`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("log line does not carry %s: %s", want, line)
		}
	}
	// The exact wording secretState produces. Reaching it means somebody routed
	// this value through the redaction helper.
	if strings.Contains(line, `"firebase_api_key":"set"`) {
		t.Errorf("the api key is reported as a redacted secret, which tells the next reader it is one: %s", line)
	}
}

// TestAnUnsetAuthDomainIsLeftForTheSignInPageToDefault pins where the
// <project>.firebaseapp.com default lives. Applying it here as well would be a
// second copy of the same rule, and NewSignIn is where the value is interpolated
// into the page and into that page's frame-src, so the copy that matters is the
// one beside the header it appears in.
func TestAnUnsetAuthDomainIsLeftForTheSignInPageToDefault(t *testing.T) {
	t.Parallel()
	env := bootEnv()
	delete(env, "FIREBASE_AUTH_DOMAIN")
	cfg, err := Load(bootGetenv(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.FirebaseAuthDomain != "" {
		t.Errorf("FirebaseAuthDomain = %q, want empty so NewSignIn applies its documented default",
			cfg.FirebaseAuthDomain)
	}

	t.Run("a configured auth domain is carried through unchanged", func(t *testing.T) {
		env := bootEnv()
		env["FIREBASE_AUTH_DOMAIN"] = "auth.example.com"
		cfg, err := Load(bootGetenv(env))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.FirebaseAuthDomain != "auth.example.com" {
			t.Errorf("FirebaseAuthDomain = %q, want auth.example.com", cfg.FirebaseAuthDomain)
		}
	})
}

// TestTheConfiguredFirebaseProjectReachesBothHalvesOfSignIn defends the one
// wiring mistake in the composition root that nothing else would catch.
//
// routes() hands FIREBASE_PROJECT_ID to two places: auth.NewVerifier, which
// pins the audience and the issuer, and NewSignIn, which renders it into the
// page the browser hands to the Firebase SDK. If those two ever name different
// projects the service is broken in the least legible way available — the SDK
// signs somebody in perfectly against project B, the verifier refuses the
// resulting token as a wrong audience, and the caller gets the same
// indistinguishable refusal every other authentication failure produces. There
// is no log line that says "these two disagree", because nothing at that point
// knows they were supposed to agree.
//
// It lives in this file rather than beside the routing tests because the
// property is about a configured value reaching the surfaces that consume it,
// which is what this file exists to pin.
func TestTheConfiguredFirebaseProjectReachesBothHalvesOfSignIn(t *testing.T) {
	cfg := bootConfig(t)
	a := bootApp(t, &bootDB{}, nil)

	rec := bootDo(t, a.handler, "GET", SignInPath, nil)
	if rec.Code != 200 {
		t.Fatalf("GET %s = %d, want 200", SignInPath, rec.Code)
	}
	body := rec.Body.String()

	// Not merely "some project id is present". The page has to carry the same
	// one the verifier was constructed with, which is the only reading of this
	// that catches a second literal creeping into either call site.
	if !strings.Contains(body, cfg.FirebaseProjectID) {
		t.Errorf("the sign-in page does not name the configured project %q; the SDK would sign people in "+
			"against a project the verifier does not accept tokens from", cfg.FirebaseProjectID)
	}
	// The key travels the same path and is just as silent when it is wrong: a
	// page rendered with another project's key fails in the browser, not here.
	if !strings.Contains(body, cfg.FirebaseAPIKey) {
		t.Errorf("the sign-in page does not carry the configured api key")
	}
}

// TestNewLoggerWritesJSONAtTheConfiguredLevel defends what Cloud Logging can
// parse. Anything but JSON on stdout arrives as a wall of text with no
// severity, which makes a production filter impossible to write.
func TestNewLoggerWritesJSONAtTheConfiguredLevel(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(slog.LevelWarn, &buf)
	log.Info("suppressed")
	log.Warn("emitted", "k", "v")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1 (info must be below the configured level): %q", len(lines), buf.String())
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("log line is not JSON: %v (%s)", err, lines[0])
	}
	if entry["msg"] != "emitted" || entry["level"] != "WARN" || entry["k"] != "v" {
		t.Errorf("log line = %v, want msg=emitted level=WARN k=v", entry)
	}
}

func bootUnique(in []string) []string {
	out := in[:0:0]
	for i, s := range in {
		if i == 0 || s != in[i-1] {
			out = append(out, s)
		}
	}
	return out
}

// bootRender is a value's text for the containment check above, whatever kind
// of field it is.
func bootRender(v reflect.Value) string {
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Slice:
		var b strings.Builder
		for i := range v.Len() {
			b.WriteString(bootRender(v.Index(i)))
			b.WriteByte(' ')
		}
		return b.String()
	default:
		return ""
	}
}
