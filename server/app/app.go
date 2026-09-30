package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/agent-sessions/server/admin"
	"github.com/loopai-hq/agent-sessions/server/api"
	"github.com/loopai-hq/agent-sessions/server/auth"
	"github.com/loopai-hq/agent-sessions/server/fleet"
	"github.com/loopai-hq/agent-sessions/server/ingest"
	"github.com/loopai-hq/agent-sessions/server/slack"
	"github.com/loopai-hq/agent-sessions/server/store"
	"github.com/loopai-hq/agent-sessions/server/web"
)

const (
	// HealthzPath answers liveness and must never touch the database. Cloud Run
	// restarts an instance whose liveness check fails, so a probe that reads
	// Postgres turns a degraded read path into a rolling restart of the whole
	// service: every instance is killed, every one of them starts, every one of
	// them runs migrations and fails the same probe. The failure the database is
	// having is not one a restart can fix.
	HealthzPath = "/healthz"
	// LivezPath is the same handler at a second path, and it is the one to
	// actually use on Cloud Run.
	//
	// Google's front end swallows /healthz on a run.app URL and answers its own
	// 404 HTML: the request never reaches the container, which is verifiable in
	// the request log, where /healthz is absent entirely while /healthz/ with a
	// trailing slash appears as a 404 the container itself served. Nothing in
	// the response says Google produced it, so the obvious reading is that the
	// route was never registered, and the obvious next move is to go looking for
	// the bug in this file.
	//
	// Cloud Run's own startup probe is a TCP check rather than an HTTP one, so
	// this costs nothing today. It costs something the moment somebody points an
	// uptime check at the conventional path and gets a permanent, inexplicable
	// 404 back.
	LivezPath = "/livez"
	// ReadyzPath reports whether this instance can serve, which does mean
	// reaching the database.
	ReadyzPath = "/readyz"
)

const (
	// Cloud Run sends SIGTERM and allows ten seconds before SIGKILL. The drain
	// budget sits below that with room left for closing the pool and exiting,
	// because a process still draining when the ten seconds elapse is killed
	// mid-response, which is the outcome the drain exists to avoid.
	drainTimeout = 7 * time.Second

	// A readiness check that hangs is a readiness check that says nothing. Two
	// seconds is longer than a healthy round trip to Cloud SQL over a unix
	// socket by an order of magnitude.
	readyTimeout = 2 * time.Second

	// Startup reaches the database once before doing anything else, so an
	// unreachable database is a failed start with a clear message rather than a
	// migration error that reads like a schema problem.
	startupPingTimeout = 10 * time.Second

	// Headers must arrive promptly; a connection that opens and then dribbles
	// is a slowloris, and holding one costs a pooled goroutine.
	readHeaderTimeout = 10 * time.Second
	// Matched to cloudrun.yaml's timeoutSeconds. A ceiling below the platform's
	// would truncate a large event batch that the platform was still willing to
	// wait for; a ceiling above it would never be reached.
	requestTimeout = 120 * time.Second
	idleTimeout    = 2 * time.Minute
)

const (
	// retentionFirstSweep delays the first sweep after this instance starts.
	//
	// Not zero, because a rolling deploy starts every instance at once and each
	// one boots, migrates, warms its pool and takes its first traffic in the
	// same few seconds; a sweep in that window competes with the work the
	// deploy exists to keep serving. Not long either, and that is deliberate:
	// an instance that only swept on a six-hour tick would never sweep at all
	// if instances are recycled more often than that, which is exactly how a
	// scheduled job quietly stops running and nobody notices for a quarter.
	// Sweeping shortly after every boot means the schedule survives any
	// instance lifetime, and a sweep with nothing to do is one indexed lookup.
	retentionFirstSweep = 2 * time.Minute

	// retentionInterval is the gap between sweeps on a long-lived instance.
	//
	// Four times a day against a policy measured in months. The work per pass
	// is bounded by the store's own budget rather than by this number, so a
	// shorter interval would only mean more passes that find nothing.
	retentionInterval = 6 * time.Hour
)

const (
	// deriveFirstPass delays the derive runner after boot for the reason
	// retentionFirstSweep gives: the deploy's first minutes belong to serving.
	deriveFirstPass = 2 * time.Minute

	// deriveDirtyInterval is the cadence of the dirty-set pass, the one fold
	// there is: ingest marks the sessions it touched and this pass folds them
	// into turns. Thirty seconds is how stale the reader's turns page and the
	// Slack mirror's answer slot may be for a live session, and a pass with
	// nothing dirty is one indexed lookup.
	deriveDirtyInterval = 30 * time.Second

	// deriveDeferredRetry is how long the versioned pass waits when another
	// instance held the lock or a pass failed on its own account (a
	// connection error, not a step's): long enough not to contend with the
	// instance doing the work, short enough that a rolling deploy's survivor
	// picks the work up within minutes of the other instance going away.
	deriveDeferredRetry = 5 * time.Minute

	// deriveFailedRetry is the wait after a step reached its retry cap. The
	// step stays failed until an operator resets its attempts (the "derive
	// step failed" line prints the statement), so a shorter wait would only
	// repeat the same line; an hour keeps the alert fed without a loop.
	deriveFailedRetry = time.Hour

	// deriveDoneRecheck is the gap between checks once the version is
	// stamped. The code's version is a constant, so nothing changes until the
	// next deploy; the check exists so an operator who forces a rebuild does
	// not have to restart the service too. RunDerive skips every step
	// derive_jobs records as finished and re-stamps when none is pending,
	// so lowering the stamp alone does nothing. Each step names the version
	// that introduced it (deriveStep.since), and the ledger seeds a step
	// finished when the stored version is at or past it: a stamp of N - 1
	// with the version-N rows deleted re-runs the steps new in N alone,
	// while a stamp of 0 with every row deleted re-runs all of them. The
	// targeted repair of one step is the admin rerun route (server/admin,
	// POST /v1/admin/derive/skill-invocations/rerun); the statements are
	// in docs/UPGRADES.md.
	deriveDoneRecheck = 6 * time.Hour
)

// App is a built server: routes assembled, migrations applied, database open.
//
// Construction and serving are separate calls because the order matters and the
// type is what enforces it. Everything that can fail — configuration, the
// database, a template that does not parse, a Firebase project id the verifier
// will not accept — fails in New, before a listener exists. A process that binds
// the port first and then discovers it cannot serve is a process Cloud Run has
// already started routing traffic to.
type App struct {
	cfg     Config
	log     *slog.Logger
	handler http.Handler

	// st outlives assembly because one thing in this server is not a request:
	// the retention sweeper, which is the only code here that deletes anything.
	// It is nil in the tests that construct an App literally to exercise the
	// drain, and a nil store must therefore mean "no sweeper" rather than a
	// panic on the first tick.
	st *store.Store
	// The sweep schedule, as fields rather than as the constants directly, so a
	// test can drive the loop it would otherwise have to wait six hours to
	// observe. Nothing outside this package sets them.
	retentionFirst time.Duration
	retentionEvery time.Duration
	// The derive runner's schedule, fields for the same reason: the shipped
	// cadence is a two-minute delay and a thirty-second dirty pass, and a
	// versioned pass that sleeps toward a four-hour window, none of which a
	// test can wait for.
	deriveFirst      time.Duration
	deriveDirtyEvery time.Duration

	// mirror is the second thing here that is not a request: the Slack poster.
	// Nil when this deployment has no bot token, which is the shipped default,
	// and nil must therefore mean "nothing to run" rather than a panic on the
	// first tick. See newSlackMirror.
	mirror *slack.Mirror

	// fleet is the evaluator: every five minutes it reads what the laptops
	// last said, judges them against the release manifest, and logs the
	// lines the fleet alerts are built from. Nil when there is no store.
	fleet *fleet.Runner
	// fleetFirst is the delay before the evaluator's first tick, a field so a
	// test can drive it.
	fleetFirst time.Duration

	// closePool is nil in tests, which supply their own connection source.
	closePool func()
}

// New opens the database, applies migrations and assembles the routes.
//
// The order is deliberate and is the reason this is one function rather than a
// set of exported pieces the caller sequences: migrations run before anything
// can serve, and the pool is closed again on every failure path so a server
// that fails to start does not leave connections held against a Cloud SQL
// instance it is about to be restarted onto.
func New(ctx context.Context, cfg Config, log *slog.Logger) (*App, error) {
	if log == nil {
		log = slog.Default()
	}

	pc, err := cfg.poolConfig()
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("app: open the database pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, startupPingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		// The host is named because it is the thing most likely to be wrong and
		// it is not a secret; the password is not in this message and is not in
		// the connection string the driver would quote back either.
		return nil, fmt.Errorf("app: reach the database at %s: %w", cfg.DatabaseHost, err)
	}

	// The Pricer starts empty and is loaded from the database below, after the
	// migrations that carry the rates have run. It cannot be loaded here: the
	// rows arrive in migration 0004 and build() is what applies it.
	pricer := ingest.NewPricer(nil, log)

	st := store.New(pool, pricer)
	// The pool is handed over twice, and the second is not redundant. The store
	// wraps it behind its own narrowed interface; the Slack mirror owns two
	// tables nothing else reads and talks pgx directly, so it needs the pool
	// itself. This is the only place in the process that holds one.
	a, err := build(ctx, cfg, log, st, pool.Ping, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}

	// Load the rate table. Until this call existed, store.ModelPrices had no
	// caller anywhere in the repository and every model call priced at zero
	// forever, because a cost is computed once at ingest and stored rather than
	// derived at read time. Seeding rates without this line changes nothing at
	// all, which is exactly the shape of the six other components in this
	// codebase that were written, tested and never invoked.
	//
	// After build() on purpose: the rates arrive in a migration, so reading them
	// before build() would read a table that does not exist yet on a fresh
	// database. Before the listener opens, so no request is ever priced against
	// a table that is only half-loaded.
	prices, err := st.ModelPrices(ctx, time.Now())
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("app: read model prices: %w", err)
	}
	rates := make([]ingest.Rate, 0, len(prices))
	for _, p := range prices {
		rates = append(rates, ingest.Rate{
			Model:                  p.Model,
			EffectiveFrom:          p.EffectiveFrom,
			InputPerMTok:           p.InputPerMTok,
			OutputPerMTok:          p.OutputPerMTok,
			CacheReadMultiplier:    p.CacheReadMultiplier,
			CacheWrite5mMultiplier: p.CacheWrite5mMultiplier,
			CacheWrite1hMultiplier: p.CacheWrite1hMultiplier,
		})
	}
	pricer.Load(rates)
	if len(rates) == 0 {
		// Keyed off the loaded count rather than stated unconditionally. A
		// warning that fires even when rates ARE present is one everybody learns
		// to ignore, and this is the line that has to still mean something on
		// the day somebody empties the table.
		log.Warn("no model rates are loaded; token usage will be stored with a zero cost")
	} else {
		log.Info("model rates loaded", "models", len(rates))
	}
	a.closePool = pool.Close
	return a, nil
}

// build applies migrations and assembles the routes over an already-open store.
//
// Split from New so that everything below the pool can be exercised over a fake
// connection source. What this function decides — which routes exist, which
// adapter each handler is given, what a request with no cookie sees — is
// observable without a Postgres, and none of it should need one to be tested.
// mirrorDB is variadic for one reason worth stating: the Slack mirror needs a
// raw pgx connection source, only New has one, and every existing caller of
// build assembles a server over a fake store that has none. A required
// parameter would make each of those pass a nil that means nothing to them.
// Absent, or nil, and the mirror is not built and its route is not mounted.
func build(ctx context.Context, cfg Config, log *slog.Logger, st *store.Store, ping func(context.Context) error, mirrorDB ...slack.DB) (*App, error) {
	// Before the listener opens, never after. Two instances of a rolling deploy
	// start at once and both run this; store.Migrate holds a Postgres advisory
	// lock on a connection of its own for the whole run and applies each file
	// in a transaction of its own, so the second instance waits rather than
	// applying the same file twice, and a file that cannot take its table lock
	// under live ingest is retried on its own rather than rolling the others
	// back with it. The wait and the retries are bounded well inside the
	// startup probe's budget (cloudrun.yaml, failureThreshold), which is the
	// number that has to move if the budget here ever does.
	// The store writes lines of its own (the migration runner's per-file
	// line, the derive runner's step lines) and they belong in the same
	// stream as everything else the server says, under the same severity
	// mapping; before Migrate, so the first migration line is already there.
	st.SetLogger(log)
	// The allowlist and the alias pairs the store's own checks read: a bound
	// actor on a source token must sit in ALLOWED_DOMAINS, and the export's
	// viewer_emails follows DOMAIN_ALIASES. Before Migrate so the store is
	// whole before anything runs against it.
	st.SetDeployment(store.Deployment{AllowedDomains: cfg.AllowedDomains, DomainAliases: cfg.DomainAliases})
	if err := st.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("app: apply migrations: %w", err)
	}
	// The first admins, after migrations because the roster table has to
	// exist, and before the listener opens so the admin page is never served
	// to a deployment whose roster is still empty. Adds only: see
	// store.BootstrapAdmins for why a row that exists is reported and left.
	outcomes, err := st.BootstrapAdmins(ctx, cfg.AdminEmails)
	if err != nil {
		return nil, fmt.Errorf("app: bootstrap admins from %s: %w", envAdminEmails, err)
	}
	for _, o := range outcomes {
		switch {
		case o.Created:
			log.Info("admin bootstrapped", "email", o.Email, "source", envAdminEmails)
		case o.Disabled:
			log.Warn("admin in "+envAdminEmails+" is disabled in the roster and was left disabled; re-enable it in the admin page", "email", o.Email, "role", string(o.Role))
		case o.Role != store.RoleAdmin:
			log.Warn("address in "+envAdminEmails+" already has a roster row with another role and was left as it is; promote it in the admin page", "email", o.Email, "role", string(o.Role))
		default:
			log.Info("admin in "+envAdminEmails+" already on the roster", "email", o.Email)
		}
	}

	var mdb slack.DB
	if len(mirrorDB) > 0 {
		mdb = mirrorDB[0]
	}
	// One evaluator serves the ticks and the admin page, so the page judges
	// versions against the same manifest the alerts do.
	runner := &fleet.Runner{
		Store:       fleetStore{s: st},
		Manifest:    newManifestSource(cfg, log),
		ServerBuild: cfg.Version,
		Log:         log.With("component", "fleet"),
		// The skill summaries ride the fleet tick: one line per expected
		// series per five minutes, from the instance holding the lock,
		// which is what the silence alert reads. The token summary
		// (source_tokens.go) is logged one line after the platform
		// summary once the token routes land.
		AfterTick: func(ctx context.Context, now time.Time) {
			st.LogSkillPlatformSummary(ctx, now)
			if err := st.LogSkillTokenSummary(ctx, now); err != nil {
				log.Error("skill token summary failed", "err", err)
			}
		},
	}
	handler, mirror, err := routes(cfg, log, st, ping, mdb, runner)
	if err != nil {
		return nil, err
	}
	a := &App{
		cfg:              cfg,
		log:              log,
		handler:          handler,
		st:               st,
		mirror:           mirror,
		retentionFirst:   retentionFirstSweep,
		retentionEvery:   retentionInterval,
		deriveFirst:      deriveFirstPass,
		deriveDirtyEvery: deriveDirtyInterval,
		fleetFirst:       fleet.Interval,
		fleet:            runner,
	}
	announceRetention(ctx, log, st, cfg.Retention)
	return a, nil
}

// announceRetention states the deletion policy at boot, before anything can
// delete under it.
//
// This is the only moment at which a policy that removes colleagues' transcripts
// is visible without reading the source or the deployment. It runs after
// Migrate, which is what makes the backlog query answerable at all — the column
// and the index it reads arrive in migration 0005 — and before the listener
// opens, so the line is in the log above the first request rather than
// interleaved with traffic.
//
// A backlog that cannot be counted is logged and does not stop the boot. The
// server's job is to serve; refusing to start because a housekeeping figure was
// unavailable would turn a retention problem into an outage. The sweeper itself
// reports what it actually removed, so nothing depends on this number being
// present.
func announceRetention(ctx context.Context, log *slog.Logger, st *store.Store, p store.RetentionPolicy) {
	if !p.Enabled() {
		// Warn rather than Info, and stated as an unbounded cost rather than as
		// a neutral setting. This is the shipped default and it is a deliberate
		// choice — the corpus is the asset — but it is also the configuration
		// that grows a terabyte a year, and the two facts have to arrive
		// together. Somebody who chose it can read a warning; somebody who
		// arrived at it by deleting a variable needs to.
		//
		// The policy is logged with it, because "forever" spelled out is what
		// distinguishes a decision from a value that failed to load.
		log.Warn("retention is switched off, so this database grows without bound and never releases anything",
			"retention", p)

		// And what the decision costs, priced against windows nobody has
		// applied. Without this the standing choice is only reversible by
		// redoing the analysis that produced it; with it, every boot states the
		// size of the thing being kept. Two capped counts, once per instance
		// start.
		rec := recommendedRetention()
		due, err := st.RetentionDueNow(ctx, rec, time.Now())
		if err != nil {
			log.Warn("cannot report what retention would remove", "err", err)
			return
		}
		log.Info("retention is off; this is what the recommended windows would remove today",
			"recommended", rec, "backlog", due)
		return
	}

	log.Info("retention policy", "retention", p)
	due, err := st.RetentionDueNow(ctx, p, time.Now())
	if err != nil {
		log.Warn("cannot report what retention would remove", "err", err)
		return
	}
	log.Info("retention backlog at boot", "backlog", due)
}

// routes builds every handler and mounts them on one mux.
//
// One mux and therefore one listener and one Cloud Run service. Splitting the
// ingest API from the dashboard would double the min-instance bill and give the
// upload path its own set of deploy mistakes to make, for an isolation that does
// not matter at fifty laptops.
// It reports the mirror alongside the handler because the mirror is two things
// at once — a route on this mux and a loop Serve has to start — and returning
// only the handler is exactly how the second half gets forgotten.
func routes(cfg Config, log *slog.Logger, st *store.Store, ping func(context.Context) error, mirrorDB slack.DB, runner *fleet.Runner) (http.Handler, *slack.Mirror, error) {
	insecure := cfg.insecureCookies()
	if insecure {
		log.Warn("PUBLIC_URL is http, so session cookies will not carry Secure; this is safe only on a developer's machine",
			"public_url", cfg.PublicURL)
	}

	cookies, err := auth.NewCookies(auth.CookieOptions{
		Keys:     [][]byte{cfg.SessionKey},
		Insecure: insecure,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: session cookies: %w", err)
	}

	// One Verifier for both sign-in paths, because it holds the key cache and
	// the refetch budget: two of them would double the traffic a stream of
	// tokens naming unknown key ids can aim at Google, and halve the cache hit
	// rate for no gain.
	//
	// Sharing it is also what makes the two paths provably identical. Enrollment
	// mints a credential that outlives any browser session, so it must not be
	// the laxer of the two, and one verifier is a stronger guarantee of that
	// than two constructed from the same configuration.
	verifier, err := auth.NewVerifier(auth.VerifierOptions{
		ProjectID: cfg.FirebaseProjectID,
		Domains:   cfg.AllowedDomains,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: firebase id token verifier: %w", err)
	}

	ingestH, err := ingest.New(ingest.Options{
		Store:   NewIngestStore(st),
		Devices: newStoreDevices(st),
		Logger:  log.With("component", "ingest"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: ingest routes: %w", err)
	}

	apiH, err := api.New(api.Options{
		Store: NewAPIStore(st),
		Auth:  NewAPIAuth(cookies, st),
		// The repair route: the same device credential the upload path
		// verifies, and the store's list of what a laptop's re-walk would
		// complete, so a daemon learns which transcripts to walk again.
		Devices: NewAPIDevices(st),
		Repair:  NewAPIRepair(st),
		// The self-service laptop token (design 4c, C1): a member's own
		// cookie mints the per-person credential their skill hook posts
		// under.
		SourceTokens: NewAPISourceTokens(st),
		// PageSize and MaxPageSize are left at the package defaults, which are
		// already the store's own ceiling. Naming a larger number here would
		// produce pages the store silently truncates.
		Logger: log.With("component", "api"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: read api routes: %w", err)
	}

	adminH, err := admin.New(admin.Options{
		Store: NewAdminStore(st),
		Auth:  NewAdminAuth(cookies, st),
		// MaxPageSize stays zero deliberately. admin's default is 500, which is
		// exactly the ceiling store.clampLimit applies; a larger value here
		// would make the access log stop after page one with no error, because
		// the handler only emits a cursor when a page comes back as full as the
		// limit it asked for.
		Logger: log.With("component", "admin"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: admin routes: %w", err)
	}

	skillH, err := newSkillUsage(st, log)
	if err != nil {
		return nil, nil, fmt.Errorf("app: skill usage routes: %w", err)
	}

	// The Slack mirror, which is a route and a loop. This mounts the route; the
	// loop is started by Serve, and neither half is any use without the other —
	// see newSlackMirror for why a deployment gets both or gets neither.
	//
	// Not mounted when there is no mirror, so a server with no bot token answers
	// the preference path with the read API's 404 rather than accepting a
	// setting nothing will ever act on.
	mirror, err := newSlackMirror(cfg, log, mirrorDB, NewSlackViewer(cookies, st), deviceEmailResolver(st))
	if err != nil {
		return nil, nil, fmt.Errorf("app: slack mirror: %w", err)
	}
	if mirror == nil {
		// Said once at boot rather than inferred from the absence of messages.
		// "I turned the mirror on and nothing happened" is otherwise a question
		// that can only be answered by reading this function.
		log.Info("the slack mirror is not configured, so sessions are not mirrored and the preference route is not served",
			"reason", slackDisabledReason(cfg, mirrorDB))
	}

	dashboard, err := web.New(web.Options{
		Data:       newWebData(st, runner),
		Viewer:     NewWebViewer(cookies, st),
		Slack:      NewSlackWeb(mirror),
		SignInPath: SignInPath,
		// The same key that signs the session cookie, per cloudrun.yaml, which
		// provisions one secret for both. It has to be shared across instances:
		// a per-instance key rejects a form posted to a different instance than
		// the one that rendered it, which presents as an admin page that works
		// intermittently.
		CSRFKey:   cfg.SessionKey,
		Logger:    log.With("component", "web"),
		Version:   cfg.Version,
		BuildDate: cfg.BuildDate,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: dashboard: %w", err)
	}

	// The three Firebase values go only here, to the one surface that renders a
	// page. Nothing else in the server needs them: the verifier is told the
	// project id separately and takes its keys from Google directly, so the API
	// key and the auth domain never reach a code path that decides anything.
	signIn, err := NewSignIn(SignInOptions{
		Cookies:  cookies,
		Verifier: verifier,
		Store:    st,
		// Passed through as configured. An empty auth domain is NewSignIn's to
		// default to <project>.firebaseapp.com, because that is where it is
		// interpolated into the page and into the page's frame-src.
		FirebaseAPIKey:     cfg.FirebaseAPIKey,
		FirebaseProjectID:  cfg.FirebaseProjectID,
		FirebaseAuthDomain: cfg.FirebaseAuthDomain,
		Domains:            cfg.AllowedDomains,
		Insecure:           insecure,
		Logger:             log.With("component", "signin"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: sign-in routes: %w", err)
	}

	// No client credentials of any kind. The agent's browser performs the same
	// Firebase sign-in the dashboard does and posts the resulting ID token here,
	// so enrollment holds nothing that could be a second, weaker way in.
	enroll, err := NewEnroll(EnrollOptions{
		Store:    st,
		Verifier: verifier,
		Domains:  cfg.AllowedDomains,
		Logger:   log.With("component", "enroll"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("app: enrollment route: %w", err)
	}

	mux := http.NewServeMux()
	if mirror != nil {
		mirror.Register(mux)
		mirror.RegisterGroups(mux)
		mirror.RegisterInteractive(mux)
	}
	RegisterIngest(mux, ingestH)
	RegisterSkillUsage(mux, skillH)
	apiH.Register(mux)
	adminH.Register(mux)
	signIn.Register(mux)
	enroll.Register(mux)

	// Unauthenticated on purpose, and the only routes here that are. A laptop
	// following the published one-liner has no cookie and no device token yet,
	// which is the whole point of a one-liner. Absent a bucket the routes are
	// simply not mounted, so a deployment that distributes the agent another way
	// does not answer for a path it cannot serve.
	if cfg.ReleaseBucket != "" {
		downloads, err := NewDownloads(DownloadOptions{
			Bucket:    cfg.ReleaseBucket,
			PublicURL: cfg.PublicURL,
			Logger:    log.With("component", "downloads"),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("app: download routes: %w", err)
		}
		downloads.Register(mux)
	}

	live := livezHandler(cfg.Version)
	mux.HandleFunc("GET "+HealthzPath, live)
	mux.HandleFunc("GET "+LivezPath, live)
	mux.HandleFunc("GET "+ReadyzPath, readyHandler(ping, log))

	// Anything under /v1/ that matched no route answers with the read API's own
	// 404, including a verb a route does not serve. The alternative is the
	// standard library's plain-text 404, which would differ from the JSON 404 a
	// forbidden session produces and would therefore let a caller tell "there is
	// no such route" from "there is a session here you may not see" — the one
	// distinction every layer of this service is built to deny them.
	mux.HandleFunc("/v1/", api.NotFound)

	// The dashboard is mounted last and as the catch-all, which is what makes a
	// bare path like /sessions land on HTML while /v1/sessions lands on JSON.
	// Handler() rather than Routes() so the dashboard's own security headers
	// wrap every HTML response: script-src 'none' is what keeps an escaping
	// mistake in the transcript path from becoming code execution, and it must
	// not depend on this file remembering to apply it.
	mux.Handle("/", dashboard.Handler())
	// Outermost, so every handler below sees the trace in its context. The
	// project is the Firebase project because in this deployment they are the
	// same string (cloudrun.yaml sets both from __PROJECT__), and a trace name
	// under the wrong project is one the Logs Explorer never matches.
	return withTrace(cfg.FirebaseProjectID, renewSessions(mux, cookies)), mirror, nil
}

// ---------------------------------------------------------------------------
// Request tracing
// ---------------------------------------------------------------------------

// requestTrace is what the trace middleware read from one request's headers,
// already in the form a log line carries: the trace as a full resource name
// and the span as sixteen hex digits, which is what Cloud Logging's spanId
// field takes even though the X-Cloud-Trace-Context header spells the same
// span in decimal.
type requestTrace struct {
	trace   string
	span    string
	sampled bool
}

// traceContextKey is the context key the middleware stores under. An unexported
// struct type so nothing outside this package can collide with it.
type traceContextKey struct{}

// traceFromContext reports the request trace, if the context descends from a
// request the middleware saw. cloudHandler calls this on every record.
func traceFromContext(ctx context.Context) (requestTrace, bool) {
	if ctx == nil {
		return requestTrace{}, false
	}
	t, ok := ctx.Value(traceContextKey{}).(requestTrace)
	return t, ok && t.trace != ""
}

// withTrace puts the request's trace into its context.
//
// Cloud Run writes a request log entry for every request with a trace id it
// minted, and sends that id to the container in X-Cloud-Trace-Context. A
// container line carrying the same id under logging.googleapis.com/trace is
// grouped with that entry in the Logs Explorer; one without it is an orphan
// that has to be matched to its request by eye, by timestamp. traceparent is
// read as a fallback for a caller that speaks W3C instead, which today is no
// caller of this service and tomorrow is whichever load balancer is put in
// front of it.
//
// Twenty lines here rather than OpenTelemetry, because correlation is the
// only thing wanted and a tracing SDK brings an exporter, a sampler and a
// dependency tree along with it.
func withTrace(project string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if t, ok := parseTrace(project, r.Header); ok {
			r = r.WithContext(context.WithValue(r.Context(), traceContextKey{}, t))
		}
		next.ServeHTTP(w, r)
	})
}

// parseTrace reads whichever trace header the request carries. Google's own
// header wins when both are present, because it is the one whose id matches
// the request log entry.
func parseTrace(project string, h http.Header) (requestTrace, bool) {
	if project == "" {
		// A trace name needs a project; without one it could only be wrong.
		return requestTrace{}, false
	}
	var (
		t  requestTrace
		ok bool
	)
	if v := h.Get("X-Cloud-Trace-Context"); v != "" {
		t, ok = parseCloudTrace(v)
	}
	if !ok {
		if v := h.Get("traceparent"); v != "" {
			t, ok = parseTraceparent(v)
		}
	}
	if !ok {
		return requestTrace{}, false
	}
	t.trace = "projects/" + project + "/traces/" + t.trace
	return t, true
}

// parseCloudTrace reads "TRACE_ID/SPAN_ID;o=TRACE_TRUE": a 32-hex trace id, a
// decimal 64-bit span id, and an optional sampling flag. Anything else is not
// a trace, and a malformed header is dropped rather than half-used, because a
// trace name that does not match any request entry is noise in every query.
func parseCloudTrace(v string) (requestTrace, bool) {
	v, opts, _ := strings.Cut(strings.TrimSpace(v), ";")
	traceID, spanDec, ok := strings.Cut(v, "/")
	if !ok || !isHex(traceID, 32) {
		return requestTrace{}, false
	}
	span, err := strconv.ParseUint(spanDec, 10, 64)
	if err != nil {
		return requestTrace{}, false
	}
	t := requestTrace{
		trace: strings.ToLower(traceID),
		span:  fmt.Sprintf("%016x", span),
	}
	for _, opt := range strings.Split(opts, ";") {
		if strings.TrimSpace(opt) == "o=1" {
			t.sampled = true
		}
	}
	return t, true
}

// parseTraceparent reads the W3C form "00-<32 hex trace>-<16 hex span>-<2 hex
// flags>". The span is already hex, so it is carried through as is.
func parseTraceparent(v string) (requestTrace, bool) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 || !isHex(parts[0], 2) || !isHex(parts[1], 32) || !isHex(parts[2], 16) || !isHex(parts[3], 2) {
		return requestTrace{}, false
	}
	flags, err := strconv.ParseUint(parts[3], 16, 8)
	if err != nil {
		return requestTrace{}, false
	}
	return requestTrace{
		trace:   strings.ToLower(parts[1]),
		span:    strings.ToLower(parts[2]),
		sampled: flags&1 == 1,
	}, true
}

// isHex reports whether s is exactly n hexadecimal digits.
func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// renewSessions rolls a live session's cookie forward before the request runs.
//
// auth.Cookies knows how to renew and nothing was calling it. Every consumer
// port that reads an identity — api.Authenticator, admin.Authenticator, the
// dashboard's viewer function — is handed only an *http.Request, deliberately,
// because deciding who somebody is should not carry the power to write a
// response. That leaves nowhere below this point that can set a cookie, so
// renewal has to happen here or not at all.
//
// Not at all was the behaviour: everybody was signed out a fixed period after
// signing in, mid-task, no matter how continuously they had been using the
// dashboard, and the fix for a session that ends while you are reading a
// transcript is a full trip back through the sign-in page and a Google popup.
//
// Renew is a no-op until a session passes its renewal point, so this costs a
// signature check on requests that carry a cookie and nothing at all on those
// that do not. It deliberately does not gate on the route: a renewal that only
// fired on some pages would make session lifetime depend on which page somebody
// happened to be reading. auth.Cookies enforces the absolute cap, so an
// indefinitely active tab still gets sent back to Google eventually.
//
// What it does gate on is the response, and it has to wait for the handler to
// decide it. See renewingWriter for the two responses a session cookie must
// never be attached to and why neither is knowable before the handler runs.
func renewSessions(next http.Handler, c *auth.Cookies) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, err := c.Verify(r)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		rw := &renewingWriter{ResponseWriter: w, cookies: c, session: s}
		next.ServeHTTP(rw, r)
		// A handler that returns without writing anything still gets its
		// renewal: net/http commits the header after this returns, so the map is
		// still open here and closed everywhere after.
		rw.renew()
	})
}

// renewingWriter defers the renewal to the moment the response's own headers
// are settled.
//
// Two things are only knowable then, and each is a way a renewal written
// up-front goes wrong.
//
// A response marked for a shared cache must not carry a session cookie. The
// stylesheet answers "public, max-age=31536000, immutable" and the sign-in
// script answers "public, max-age=60", and a browser fetches both with the
// session cookie attached exactly as it does a page; a cache that stored one
// stored somebody's live session and hands it to whoever asks next. Skipping
// them costs nothing, because every publicly cacheable response this server
// produces is a subresource of a page that was renewed a moment earlier.
//
// And a handler that decides this cookie itself has the last word. Sign-out
// clears it and sign-in issues a new one, both before writing their status, so
// a renewal appended afterwards would be the value the browser keeps — which
// turns signing out into staying signed in.
type renewingWriter struct {
	http.ResponseWriter
	cookies *auth.Cookies
	session auth.Session
	done    bool
}

func (w *renewingWriter) WriteHeader(code int) {
	w.renew()
	w.ResponseWriter.WriteHeader(code)
}

// Write covers the implicit 200: net/http commits the header on the first
// write, so a cookie added after one is a cookie nobody receives.
func (w *renewingWriter) Write(b []byte) (int, error) {
	w.renew()
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the writer underneath, so wrapping
// here does not silently take a capability away from a handler below.
func (w *renewingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *renewingWriter) renew() {
	if w.done {
		return
	}
	w.done = true
	h := w.Header()
	if publiclyCacheable(h.Get("Cache-Control")) || setsCookie(h, w.cookies.Name()) {
		return
	}
	// Renew reports whether it reissued; the answer is not needed here because
	// the response goes out unchanged either way.
	_, _ = w.cookies.Renew(w.ResponseWriter, w.session)
}

// publiclyCacheable reports whether a response invites a shared cache to keep a
// copy.
//
// Only an explicit `public` counts. This server spells the directive out on
// every route that means it, and reading cacheability out of a bare max-age
// instead would withhold renewal from responses that are private by default and
// make session lifetime depend on a header the dashboard does not set.
func publiclyCacheable(cacheControl string) bool {
	for _, d := range strings.Split(cacheControl, ",") {
		if strings.EqualFold(strings.TrimSpace(d), "public") {
			return true
		}
	}
	return false
}

// setsCookie reports whether the response already decides the named cookie.
func setsCookie(h http.Header, name string) bool {
	for _, v := range h.Values("Set-Cookie") {
		if got, _, ok := strings.Cut(v, "="); ok && strings.TrimSpace(got) == name {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Device credentials
// ---------------------------------------------------------------------------

// storeDevices verifies the credential a laptop presents, against the store.
//
// The contract pins NewIngestDevices(*auth.Devices), and an *auth.Devices cannot
// be constructed in this repository: auth.NewDevices requires an
// auth.DeviceStore, and nothing implements that interface — the store's device
// methods are a different shape, and no writer owns a file where the bridge
// could live. Rather than leave the upload endpoint unwireable, the composition
// root binds it to store.AuthenticateDevice, which answers the same question.
//
// It answers it in one statement, which is stronger than the port it replaces:
// the token's own revocation and expiry, the device's revocation and the
// principal's disablement are all predicates in a single query, so there is no
// window in which a token verifies against a device that was revoked between two
// reads. That window is exactly the moment somebody is being offboarded.
//
// Two things are lost and both are recorded here rather than papered over.
// last_used_at is written on every verification instead of on auth.Devices'
// five-minute throttle, so an upload's authentication is a write on the hottest
// row in device_tokens. And the six auth sentinels are collapsed into
// store.ErrNotFound, so a log line can no longer say which condition fired.
// Neither changes what a caller is told, because every credential failure is one
// indistinguishable 401 by design.
type storeDevices struct{ s *store.Store }

// newStoreDevices adapts the store to the upload endpoint's credential port. A
// nil store is refused at composition time: ingest.New's own nil check inspects
// an interface value, and a nil *store.Store inside a non-nil interface passes
// it and becomes a nil dereference on the first upload a laptop attempts.
func newStoreDevices(s *store.Store) ingest.Devices {
	if s == nil {
		panic("app: newStoreDevices requires a store")
	}
	return storeDevices{s: s}
}

// Verify authenticates a presented device credential and classifies the failure.
//
// The classification is the whole job. The endpoint answers 401 for a credential
// failure and 503 for anything else, and the two mean opposite things to an
// agent: the first sends a laptop to re-enrol, the second makes it keep
// everything it has captured and try again later. Get it backwards and a
// database blip tells a fleet of working laptops their credentials are dead.
//
// Only two conditions are read as the caller's problem — a token that is not
// ours by shape, and a lookup that matched no row. Everything else, including a
// failure this build has never seen, is ours.
func (d storeDevices) Verify(ctx context.Context, presented string) (ingest.Identity, error) {
	presented = strings.TrimSpace(presented)
	if presented == "" || !strings.HasPrefix(presented, auth.TokenPrefix) {
		// Refused without a lookup, so a stream of guesses is a stream of string
		// comparisons rather than a stream of index probes.
		return ingest.Identity{}, fmt.Errorf("%w: %w", ingest.ErrUnauthenticated, auth.ErrDeviceTokenMalformed)
	}

	// auth.HashToken is the one place a token becomes a stored value, and the
	// enrollment path hashes through it too. Two spellings of "what is hashed"
	// would turn every credential in the table into a dead one.
	id, err := d.s.AuthenticateDevice(ctx, auth.HashToken(presented))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// The cause stays wrapped underneath. The endpoint deliberately does
			// not act on it, but discarding it would make a revocation and a
			// guess unrecoverable from each other for anyone who later has to
			// tell them apart.
			return ingest.Identity{}, fmt.Errorf("%w: %w", ingest.ErrUnauthenticated, err)
		}
		return ingest.Identity{}, fmt.Errorf("app: verify device credential: %w", err)
	}
	// The role is not carried across. The upload path makes no permission
	// decision: every row it writes is attributed to the principal owning the
	// delivering device, and a role here would be a second place authorization
	// could be decided from.
	return ingest.Identity{Email: id.Email, DeviceID: id.DeviceID}, nil
}

// ---------------------------------------------------------------------------
// Liveness and readiness
// ---------------------------------------------------------------------------

// livezHandler answers that this process is alive and names the build that is
// answering, and reads nothing to decide either. See HealthzPath for what a
// database call here would cost.
//
// The body is "ok <version>": the build was logged once at boot and exposed
// nowhere else, so the only way an external check could learn what was
// deployed was `gcloud run services describe`. The rollout gate compares
// three shas (origin/main, the serving build, the published agent release),
// and this is where it reads the middle one. An unstamped build says "ok"
// alone rather than inventing a version.
func livezHandler(version string) http.HandlerFunc {
	body := "ok\n"
	if version != "" {
		body = "ok " + version + "\n"
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
	}
}

// readyHandler answers whether this instance can serve a request that needs the
// database, which every route except liveness does.
//
// The check is bounded separately from the request: a readiness probe that
// inherits a caller's deadline reports whatever that caller was willing to wait
// for, and one that inherits no deadline at all hangs for as long as the pool
// does.
func readyHandler(ping func(context.Context) error, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")

		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := ping(ctx); err != nil {
			// Logged because the body deliberately says nothing: this endpoint
			// is reachable from the internet and a driver's error text names
			// hosts, users and constraints. With the request's context, so the
			// line carries the probe's trace and the alert on it can be opened
			// next to the request entry that failed.
			log.WarnContext(r.Context(), "readiness check failed", "err", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "not ready\n")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Serve binds the configured port and serves until ctx is cancelled.
func (a *App) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", a.cfg.Port))
	if err != nil {
		return fmt.Errorf("app: listen on port %d: %w", a.cfg.Port, err)
	}
	return a.serve(ctx, ln)
}

// serve runs the HTTP server on an existing listener and drains on cancellation.
//
// Cloud Run sends SIGTERM before it removes an instance from the load balancer,
// so requests are still arriving and still in flight at the moment the signal
// lands. A server that exits on the signal drops them: an upload that was
// accepted and not acknowledged goes back on the laptop's spool and is
// redelivered, which is survivable, and a dashboard read becomes a 502 in
// somebody's browser, which is not what a deploy should look like.
func (a *App) serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           a.handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       requestTimeout,
		WriteTimeout:      requestTimeout,
		IdleTimeout:       idleTimeout,
		// Request contexts are rooted at Background rather than at ctx on
		// purpose. ctx is cancelled by SIGTERM, and deriving requests from it
		// would cancel every in-flight query at the instant the drain starts,
		// which turns a graceful shutdown into the abrupt one it replaced.
		BaseContext: func(net.Listener) context.Context { return context.Background() },
		// The standard library writes connection-level faults through a
		// *log.Logger; without this they go to stderr as unstructured text that
		// Cloud Logging cannot filter on.
		ErrorLog: slog.NewLogLogger(a.log.Handler(), slog.LevelWarn),
	}

	// Both background loops are bounded by this function's lifetime, which is
	// what keeps them from outliving the pool: main
	// defers App.Close immediately after New, so a goroutine still issuing
	// statements after serve returns would be issuing them against a closed
	// pool. Derived from ctx rather than being ctx so that the other way out of
	// this function — Serve returning an error, with no signal and therefore no
	// cancellation — also stops them.
	bgCtx, stopBackground := context.WithCancel(ctx)
	sweeperDone := a.startRetention(bgCtx)

	// The derive runner stands where the boot-time rebuild used to start: a
	// goroutine that walked every event with text into memory, on every
	// instance of a rolling deploy at once, and started over on the next
	// deploy because the version was stamped only at the end. The runner is
	// on the sweeper's schedule and pattern instead (see startDerive), and
	// its versioned pass is recorded in derive_jobs so a deploy mid-way
	// resumes rather than restarts.
	deriveDone := a.startDerive(bgCtx)
	mirrorDone := a.startMirror(bgCtx)
	fleetDone := a.startFleet(bgCtx)
	defer func() {
		stopBackground()
		<-sweeperDone
		<-deriveDone
		<-mirrorDone
		<-fleetDone
	}()

	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	a.log.Info("serving", "addr", ln.Addr().String())

	select {
	case err := <-served:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("app: serve: %w", err)
	case <-ctx.Done():
	}

	a.log.Info("draining", "budget", drainTimeout.String())
	drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		// The budget is spent. Closing is what keeps the process from being
		// SIGKILLed with the pool still open, and a request cut off here was
		// going to be cut off by the platform a moment later anyway.
		_ = srv.Close()
		<-served
		return fmt.Errorf("app: drain within %s: %w", drainTimeout, err)
	}
	<-served
	return nil
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

// startRetention runs the sweep on a schedule for as long as ctx lives, and
// reports a channel that closes once it has stopped.
//
// This is the caller store.SweepRetention needs to have. A sweeper that exists
// and is never invoked is the same defect as a composition root nobody
// constructs: it passes its own tests, it deletes nothing, and the database goes
// on growing exactly as it did before anybody wrote it.
//
// In-process on the serving instance rather than a Cloud Scheduler job or a
// Cloud Run job of its own, for three reasons that are specific to this
// deployment. cloudrun.yaml sets cpu-throttling to false and minScale to 1, so
// there is always one instance with CPU allocated between requests and a
// background timer actually fires — on a throttled service it would not. A
// separate job would need its own image, its own service account binding to
// Cloud SQL and its own deploy, all of which drift from this one. And the
// sweep's own advisory lock already makes duplicate runners harmless, which is
// the property a scheduler would otherwise have to provide.
//
// What stops two instances from both sweeping: nothing stops them from trying,
// and that is deliberate. Each batch takes a transaction-scoped advisory lock
// before it writes, so two instances can never be inside a batch at once, and
// the one that finds the lock held ends its pass and waits for the next tick.
// The alternative — electing a leader — would be a second distributed system to
// keep correct, in exchange for saving a few statements that already cost
// nothing.
func (a *App) startRetention(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	// A nil store is the App the drain tests construct literally; a disabled
	// policy is a deployment that has chosen to keep everything. Neither should
	// start a goroutine, and neither is an error.
	if a.st == nil || !a.cfg.Retention.Enabled() {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		a.runRetention(ctx)
	}()
	return done
}

// runRetention sweeps shortly after boot and then on the interval.
//
// A timer rather than a ticker: a ticker that fires while a pass is still
// running queues the tick and the next pass starts the instant this one ends,
// which is how a slow sweep turns into a continuous one. Resetting after each
// pass makes the interval a gap between passes rather than a period.
func (a *App) runRetention(ctx context.Context) {
	first, every := a.retentionFirst, a.retentionEvery
	if first <= 0 {
		first = retentionFirstSweep
	}
	if every <= 0 {
		every = retentionInterval
	}

	timer := time.NewTimer(first)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		sweep, err := a.st.SweepRetention(ctx, a.cfg.Retention, time.Now())
		switch {
		case err != nil && ctx.Err() != nil:
			// The shutdown cancelled the statement in flight. The batch rolled
			// back, nothing was half-removed, and the next instance to sweep
			// continues from the same cutoff — so this is the drain working,
			// not a failure. Reported at info with what the pass had already
			// committed, because a line saying nothing at all here would make a
			// deploy look like a lost sweep.
			a.log.Info("retention sweep stopped by shutdown", "sweep", sweep)
			return
		case err != nil:
			// The counts are logged with the error: a pass that removed forty
			// thousand rows and then failed is a different event from one that
			// failed on its first statement, and only the counts tell them
			// apart.
			a.log.Error("retention sweep failed", "err", err, "sweep", sweep)
		default:
			// Logged unconditionally, including the pass that removed nothing.
			// That line is the only standing evidence that the sweeper is still
			// running, and its absence is what an operator should be able to
			// notice.
			a.log.Info("retention sweep", "sweep", sweep)
		}
		// The health ledger rides the same schedule: raw health reports older
		// than a week are rolled up into health_hourly and deleted in bounded
		// batches. The fleet evaluator sweeps it once an hour as well, so a
		// deployment that keeps every session (the shipped default, which
		// starts no sweeper here) still bounds health_reports; both paths are
		// idempotent and serialise on the retention lock.
		if ctx.Err() == nil {
			if hs, err := a.st.SweepHealth(ctx, time.Now()); err != nil && ctx.Err() == nil {
				a.log.Error("health sweep failed", "err", err, "sweep", hs)
			} else if err == nil {
				a.log.Info("health sweep", "sweep", hs)
			}
		}
		timer.Reset(every)
	}
}

// Close releases the database pool. It is separate from Serve so that a caller
// can drain first and disconnect after: closing the pool while requests are
// still finishing would fail exactly the requests the drain was protecting.
func (a *App) Close() {
	if a.closePool != nil {
		a.closePool()
	}
}

// startDerive runs the derive runner for as long as ctx lives, and reports a
// channel that closes when it has stopped, on the sweeper's pattern
// (startRetention) and for the sweeper's reasons: in-process on the serving
// instance, one goroutine per instance, standing down when another instance
// holds the work. A nil store is the App the drain tests construct
// literally; nothing to derive, no goroutine.
func (a *App) startDerive(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if a.st == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		a.runDerive(ctx)
	}()
	return done
}

// runDerive waits out the boot delay and then runs the two passes side by
// side until ctx ends: the dirty-set pass on its short cadence, and the
// versioned pass, which sleeps for whatever the store says it is waiting on.
func (a *App) runDerive(ctx context.Context) {
	first, every := a.deriveFirst, a.deriveDirtyEvery
	if first <= 0 {
		first = deriveFirstPass
	}
	if every <= 0 {
		every = deriveDirtyInterval
	}
	if !sleepFor(ctx, first) {
		return
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		a.runDeriveDirty(ctx, every)
	}()
	go func() {
		defer wg.Done()
		a.runDeriveVersioned(ctx)
	}()
	wg.Wait()
}

// runDeriveDirty folds the sessions ingest has touched, every interval.
func (a *App) runDeriveDirty(ctx context.Context, every time.Duration) {
	for {
		res, err := a.st.DeriveDirty(ctx, a.cfg.Derive)
		switch {
		case err != nil && ctx.Err() != nil:
			// The shutdown cancelled the statement in flight; the session
			// that was mid-fold stays dirty and the next instance folds it.
			a.log.Info("derive dirty pass stopped by shutdown", "dirty", res)
			return
		case err != nil:
			a.log.Error("derive dirty pass failed", "err", err, "dirty", res)
		case res.Folded > 0 || res.Skipped > 0 || res.Failed > 0 || res.Parked > 0:
			// A line only when there was something to fold, or something
			// that would not fold: at this cadence an unconditional line
			// would be most of the log, and the versioned pass line below is
			// the standing evidence that the runner is alive.
			a.log.Info("derive dirty pass", "dirty", res)
		}
		if !sleepFor(ctx, every) {
			return
		}
	}
}

// runDeriveVersioned drives the versioned pass: run it, sleep for what it
// reported, run it again. The store decides how far one pass goes (to the
// end, to the window's edge, to another instance's lock, to a failed step);
// this loop only decides how long to wait before asking again.
func (a *App) runDeriveVersioned(ctx context.Context) {
	for {
		pass, err := a.st.RunDerive(ctx, a.cfg.Derive)
		var wait time.Duration
		switch {
		case err != nil && ctx.Err() != nil:
			a.log.Info("derive pass stopped by shutdown", "pass", pass)
			return
		case err != nil:
			a.log.Error("derive pass failed", "err", err, "pass", pass)
			wait = deriveDeferredRetry
		case pass.Failed != "":
			// The step's own line ("derive step failed") already said what
			// and how to recover; this one says the pass is parked on it.
			a.log.Warn("derive pass parked on a failed step", "pass", pass)
			wait = deriveFailedRetry
		case pass.Waiting:
			a.log.Info("derive pass", "pass", pass)
			wait = pass.Until
		case pass.Deferred:
			a.log.Info("derive pass", "pass", pass)
			wait = deriveDeferredRetry
		case pass.Done:
			// Logged even when nothing ran: this is the line whose absence
			// says the runner is gone.
			a.log.Info("derive pass", "pass", pass)
			wait = deriveDoneRecheck
		default:
			a.log.Info("derive pass", "pass", pass)
			wait = deriveDeferredRetry
		}
		if !sleepFor(ctx, wait) {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Fleet evaluator
// ---------------------------------------------------------------------------

// startFleet runs the fleet evaluator for as long as ctx lives, on the
// sweeper's pattern and for the sweeper's reasons: in-process on the serving
// instance (cpu-throttling is off, so the ticker fires), one goroutine per
// instance, the store's advisory lock making one instance's tick the
// fleet's. Nothing to evaluate without a store.
func (a *App) startFleet(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	if a.st == nil || a.fleet == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		if !sleepFor(ctx, a.fleetFirst) {
			return
		}
		a.fleet.Run(ctx)
	}()
	return done
}

// fleetStore adapts the store to the evaluator's port. Translation only:
// every method is one of the store's bounded reads with the rows reshaped.
type fleetStore struct{ s *store.Store }

func (f fleetStore) Lock(ctx context.Context) (func(), bool, error) {
	tx, held, err := f.s.FleetLock(ctx)
	if err != nil || !held {
		return nil, false, err
	}
	// The lock is transaction-scoped; ending the transaction is the release,
	// and a rollback is the honest end of a transaction that wrote nothing.
	return func() { _ = tx.Rollback(ctx) }, true, nil
}

func (f fleetStore) TickDue(ctx context.Context, minAge time.Duration) (bool, error) {
	due, err := f.s.FleetTickDue(ctx, minAge)
	if err != nil {
		return false, fmt.Errorf("app: fleet tick: %w", err)
	}
	return due, nil
}

func (f fleetStore) People(ctx context.Context) ([]fleet.Person, error) {
	ps, err := f.s.ListPrincipals(ctx, adminRosterReader)
	if err != nil {
		return nil, fmt.Errorf("app: fleet roster: %w", err)
	}
	out := make([]fleet.Person, 0, len(ps))
	for _, p := range ps {
		out = append(out, fleet.Person{Email: p.Email, Disabled: p.DisabledAt != nil})
	}
	return out, nil
}

func (f fleetStore) Devices(ctx context.Context) ([]fleet.Device, error) {
	ds, err := f.s.EnrolledDevices(ctx)
	if err != nil {
		return nil, fmt.Errorf("app: fleet devices: %w", err)
	}
	out := make([]fleet.Device, 0, len(ds))
	for _, d := range ds {
		fd := fleet.Device{ID: d.ID, Email: d.Email, Hostname: d.Hostname, AgentVersion: d.AgentVersion, EnrolledAt: d.EnrolledAt, Revoked: d.RevokedAt != nil}
		if d.LastSeenAt != nil {
			fd.LastSeenAt = *d.LastSeenAt
		}
		out = append(out, fd)
	}
	return out, nil
}

func (f fleetStore) Reports(ctx context.Context) ([]fleet.Report, error) {
	rows, err := f.s.HealthLatestRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("app: fleet reports: %w", err)
	}
	out := make([]fleet.Report, 0, len(rows))
	for _, r := range rows {
		rep, err := fleet.DecodeReport(r.Email, r.DeviceID, r.EmittedAt, r.ReceivedAt, r.Worst, r.AgentVersion, r.Report)
		if err != nil {
			return nil, err
		}
		out = append(out, rep)
	}
	return out, nil
}

func (f fleetStore) RecentBuilds(ctx context.Context, since time.Time) ([]fleet.BuildSighting, error) {
	bs, err := f.s.HealthRecentBuilds(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("app: fleet recent builds: %w", err)
	}
	out := make([]fleet.BuildSighting, 0, len(bs))
	for _, b := range bs {
		out = append(out, fleet.BuildSighting{Email: b.Email, DeviceID: b.DeviceID, Build: b.Build, LastSeen: b.LastSeen})
	}
	return out, nil
}

func (f fleetStore) Hours(ctx context.Context, since time.Time) ([]fleet.Hour, error) {
	hs, err := f.s.HealthHours(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("app: fleet hours: %w", err)
	}
	out := make([]fleet.Hour, 0, len(hs))
	for _, h := range hs {
		out = append(out, fleet.Hour{Email: h.Email, DeviceID: h.DeviceID, Hour: h.Hour, Dropped: h.DroppedByReason()})
	}
	return out, nil
}

func (f fleetStore) Rollup(ctx context.Context, from, to time.Time) (bool, error) {
	_, locked, err := f.s.RollupHealth(ctx, from, to)
	return !locked, err
}

func (f fleetStore) Mutes(ctx context.Context) ([]fleet.Mute, error) {
	ms, err := f.s.FleetMutes(ctx)
	if err != nil {
		return nil, fmt.Errorf("app: fleet mutes: %w", err)
	}
	out := make([]fleet.Mute, 0, len(ms))
	for _, m := range ms {
		out = append(out, fleet.Mute{Email: m.Email, Kind: m.Kind, Until: m.Until, Note: m.Note, CreatedBy: m.CreatedBy})
	}
	return out, nil
}

func (f fleetStore) EmptyStarts(ctx context.Context, since time.Time) ([]fleet.EmptyStart, error) {
	es, err := f.s.FleetEmptyStarts(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("app: fleet empty starts: %w", err)
	}
	out := make([]fleet.EmptyStart, 0, len(es))
	for _, e := range es {
		out = append(out, fleet.EmptyStart{Email: e.Email, DeviceID: e.DeviceID, Aborted: e.Aborted, Total: e.Total, Cwds: e.Cwds})
	}
	return out, nil
}

func (f fleetStore) MissingAnswers(ctx context.Context, since time.Time) ([]fleet.AnswerCohort, error) {
	cs, err := f.s.FleetMissingAnswers(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("app: fleet missing answers: %w", err)
	}
	out := make([]fleet.AnswerCohort, 0, len(cs))
	for _, c := range cs {
		out = append(out, fleet.AnswerCohort{Day: c.Day, AgentVersion: c.AgentVersion, Entrypoint: c.Entrypoint, Turns: c.Turns, Answered: c.Answered})
	}
	return out, nil
}

func (f fleetStore) SweepHealth(ctx context.Context, now time.Time) (slog.Value, error) {
	sweep, err := f.s.SweepHealth(ctx, now)
	return sweep.LogValue(), err
}

// newManifestSource reads latest.json from the release bucket, through the
// same credential and API the download routes use, so the evaluator judges
// the fleet against exactly what a laptop would download. Without a bucket
// there is no manifest, and the evaluator says so once.
func newManifestSource(cfg Config, log *slog.Logger) fleet.ManifestSource {
	if cfg.ReleaseBucket == "" {
		return noManifest{}
	}
	d, err := NewDownloads(DownloadOptions{Bucket: cfg.ReleaseBucket, PublicURL: cfg.PublicURL, Logger: log.With("component", "fleet")})
	if err != nil {
		return noManifest{}
	}
	return bucketManifest{d: d}
}

type noManifest struct{}

func (noManifest) Latest(context.Context) (fleet.Manifest, error) {
	return fleet.Manifest{}, fleet.ErrManifestMissing
}

type bucketManifest struct{ d *Downloads }

func (b bucketManifest) Latest(ctx context.Context) (fleet.Manifest, error) {
	tok, err := b.d.accessToken()
	if err != nil {
		return fleet.Manifest{}, fmt.Errorf("app: token for the release bucket: %w", err)
	}
	api := fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s?alt=media",
		url.PathEscape(b.d.bucket), url.PathEscape("latest/latest.json"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, api, nil)
	if err != nil {
		return fleet.Manifest{}, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := b.d.http.Do(req)
	if err != nil {
		return fleet.Manifest{}, fmt.Errorf("app: read latest.json: %w", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return fleet.Manifest{}, fleet.ErrManifestMissing
	case resp.StatusCode != http.StatusOK:
		return fleet.Manifest{}, fmt.Errorf("app: read latest.json: bucket answered %d", resp.StatusCode)
	}
	var m fleet.Manifest
	// A manifest is a few hundred bytes; the cap is here so a wrong object
	// cannot be read into memory unbounded.
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&m); err != nil {
		return fleet.Manifest{}, fmt.Errorf("app: decode latest.json: %w", err)
	}
	return m, nil
}

// sleepFor waits d or until ctx ends, and reports whether it was the timer.
func sleepFor(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
