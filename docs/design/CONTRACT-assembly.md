# Server assembly contract

> **Historical design note.** Written while the server was being assembled and kept as it was; moved from `server/` to `docs/design/` on 2026-09-30. It is not maintained and may no longer match the code. The current description of the system is [docs/ARCHITECTURE.md](../ARCHITECTURE.md); the source is the truth where the two differ.

The six server packages were each built against their own consumer-defined port
interfaces and each tested against their own fakes. Nothing binds them to the
store, and there is no process entrypoint. This document pins the boundary so
the remaining work can be written in parallel and still compile as one program.

Read the real declarations rather than trusting a paraphrase here. This file
pins names, files and signatures; the field-level truth lives in the source.

## Package layout

- `server/app` — new package. Owns every adapter from `*store.Store` to a
  consumer port, plus the wiring that builds an `http.Handler`. This is the only
  package allowed to import more than one of api/admin/web/ingest/auth.
- `server/cmd/loop-sessions-server` — new package `main`. Thin: read config,
  call `app.New`, serve, shut down. No business logic. The Dockerfile already
  points `MAIN_PKG` here.

Direction of dependency: `app` imports `store`, `api`, `admin`, `web`,
`ingest`, `auth`. None of those may import `app`. `store` must not import any
consumer package; where a consumer needs a shape the store does not have, the
store declares its own type and `app` translates.

## File ownership (disjoint, one writer each)

| File | Owner | Contents |
|---|---|---|
| `server/store/admin_queries.go` | W1 | New store methods listed below |
| `server/store/migrations/0002_admin_queries.sql` | W1 | Only if a new index is needed |
| `server/app/api_adapter.go` | W2 | `store` to `api` |
| `server/app/web_adapter.go` | W3 | `store` to `web` |
| `server/app/admin_adapter.go` | W4 | `store` to `admin` |
| `server/app/ingest_adapter.go` | W5 | `store` to `ingest`, `auth` to `ingest.Devices` |
| `server/app/app.go`, `server/app/config.go` | W6 | Composition root |
| `server/cmd/loop-sessions-server/main.go` | W6 | Entrypoint |
| `server/app/signin.go` | W7 | Google sign-in HTTP surface |
| `server/app/enroll.go` | W7 | Device enrollment endpoint |
| `server/app/identity.go` | W7 | Cookie-to-viewer adapters |

Do not edit a file you do not own. If you need a change in someone else's file,
say so in your result instead of making it.

## W1: store methods the admin package requires

`server/admin/handler.go` declares a `Store` port with four methods the store
does not implement. Add them to `*store.Store` using store-owned types.

```go
func (s *Store) LatestHealth(ctx context.Context) ([]HealthSnapshot, error)
func (s *Store) EnrolledDevices(ctx context.Context) ([]Device, error)
func (s *Store) AccessEvents(ctx context.Context, f AccessLogFilter) ([]Access, error)
func (s *Store) InTx(ctx context.Context, fn func(context.Context, AdminTx) error) error
```

`AdminTx` is a new store-owned interface mirroring `admin.Tx` in store types:

```go
type AdminTx interface {
    Principal(ctx context.Context, email string) (Principal, bool, error)
    CountActiveAdminsExcept(ctx context.Context, email string) (int, error)
    SavePrincipal(ctx context.Context, p Principal) error
    RecordPrincipalChange(ctx context.Context, c PrincipalChange) error
}
```

`HealthSnapshot` must carry one row per `(email, device_id)`, not per person:
`admin/fleet.go` treats a person with one working and one dead laptop as not
covered, and a per-person max would report them as healthy.

`EnrolledDevices` must include revoked devices. Hiding them hides the case the
coverage report exists to catch, a revoked device still sending reports.

`InTx` must serialize concurrent principal changes, by selecting the candidate
admin rows `FOR UPDATE` or running at `SERIALIZABLE`. Two simultaneous
demotions that each observe the other as the surviving admin would both be
permitted and the roster would end up with zero admins. Write the test that
demonstrates the guard holds under concurrency.

Existing `Principal(ctx, email) (Principal, error)` returns two values while
every consumer port wants three. Do not change the existing signature; the
zero-row case is what the boolean carries, so add
`PrincipalLookup(ctx, email) (Principal, bool, error)` and leave the old method
alone. Callers inside the store keep working, and `app` uses the new one.

## W2 to W5: adapter shape

Every adapter follows the same shape. One unexported struct wrapping
`*store.Store`, one exported constructor, methods that translate types and
errors. No business logic and no permission decisions: the store owns the
permission predicate and writes the audit row inside the same transaction as
the read, and a second copy of a permission rule in an adapter is a second
thing to keep correct. The copy that drifts is always the more permissive one.

```go
// in package app
type apiStore struct{ s *store.Store }

func NewAPIStore(s *store.Store) api.Store { return apiStore{s: s} }
```

Constructors, all in package `app`:

| Constructor | Returns |
|---|---|
| `NewAPIStore(*store.Store)` | `api.Store` |
| `NewWebData(*store.Store)` | `web.Data` |
| `NewAdminStore(*store.Store)` | `admin.Store` |
| `NewIngestStore(*store.Store)` | `ingest.Store` |
| `NewIngestDevices(*auth.Devices)` | `ingest.Devices` |
| `NewAPIAuth(*auth.Cookies, *store.Store)` | `api.Authenticator` |
| `NewAdminAuth(*auth.Cookies, *store.Store)` | `admin.Authenticator` |
| `NewWebViewer(*auth.Cookies, *store.Store)` | `func(*http.Request) (web.Viewer, bool)` |
| `NewSignIn(SignInOptions) (*SignIn, error)` | has `Register(*http.ServeMux)` |
| `NewEnroll(EnrollOptions) (*Enroll, error)` | has `Register(*http.ServeMux)` |

## W7: SUPERSEDED — see CONTRACT-firebase-auth.md

Everything below this line describes the Google OAuth design: two OAuth
clients, an authorization-code exchange, PKCE, and a
`{code, code_verifier, redirect_uri, ...}` enrollment payload. None of it is
what the server does. It was replaced wholesale by Firebase
Auth, which needs no OAuth client at all; `CONTRACT-firebase-auth.md` is the
current contract and this section is kept only so the reasoning behind the
change is legible next to what it changed.

Do not implement anything below. The environment-variable table in W6 is
likewise superseded: there are no `GOOGLE_OAUTH_` variables, and
`server/app/config_test.go` fails if one is ever read again.

## W7: the missing HTTP surface for authentication

`server/auth` provides every primitive and not one handler. `Verifier` checks a
Google ID token, `Cookies` signs and reads the dashboard session, `Devices`
issues and verifies device bearer tokens, `CanRead` decides access. Nothing
turns them into routes, so today the dashboard redirects to a sign-in path that
404s and the CLI enrolls against an endpoint that does not exist. Neither the
dashboard nor a single laptop can authenticate.

Three route groups, all in package `app`.

### Dashboard sign-in, `signin.go`

- `GET /auth/google/start` builds the Google authorization URL and redirects.
- `GET /auth/google/callback` exchanges the code, verifies the identity,
  resolves the principal, sets the session cookie, and redirects onward.
- `POST /auth/signout` clears the cookie.

`web.Options.SignInPath` is already `/auth/google/start`, so that path is
fixed. Preserve the `next` parameter through the round trip so a link to a
transcript survives sign-in, and reject any `next` that is not a local path.
An absolute URL accepted there is an open redirect that phishes a colleague
with a link on your own trusted domain.

CSRF on the callback is not optional. Generate `state`, store it in a
short-lived cookie, and compare on return. Without it anyone can trigger a
sign-in on somebody else's browser as an account of their choosing.

The `hd` claim and the email domain are both checked against the allowlist. The
claim proves the Workspace; the address is what the roster keys on. Note that
a Workspace can carry two domains that alias each other, so this is a set
membership test and never an equality test against one string.

### Device enrollment, `enroll.go`

`POST /v1/enroll/complete` receives, as JSON,
`{code, code_verifier, redirect_uri, hostname, os, arch, agent_version}`.
`internal/enroll/enroll.go` is the client half; read it and match it exactly
rather than inventing a shape.

The client holds no OAuth client secret, deliberately, because it ships to
laptops. The server therefore performs the PKCE code exchange on its behalf,
sending `code`, `code_verifier` and `redirect_uri` to Google's token endpoint
with the server's own client credentials. Verify the returned ID token with
`auth.Verifier` exactly as the dashboard path does, then call
`store.EnrollDevice` and return the issued device token.

`store.EnrollDevice` stores only a hash of the token, so the plaintext is
returned to the client once here and is unrecoverable afterwards. If the
response is lost the device re-enrolls; there is no recovery path and there
should not be one.

Rate-limit by email. This endpoint mints long-lived credentials from a code and
is the highest-value unauthenticated surface on the service.

Never log the code, the verifier, the ID token or the issued device token.

### Identity adapters, `identity.go`

`api`, `admin` and `web` each declare their own identity shape. Each adapter
reads the session cookie, loads the principal, and returns that package's type.
A disabled principal authenticates as nobody: return the package's
no-identity error rather than a viewer with a role, or a revoked colleague
keeps reading transcripts until their cookie expires.

Test that a missing cookie, a tampered cookie, an expired cookie, an unknown
email and a disabled principal all fail closed, and that each failure is
indistinguishable from the others to the caller.

### Error translation is load-bearing

`api.ErrNotFound` and `web`'s equivalent must be returned for BOTH "no such
session" and "not allowed to see it". `server/api/api.go` documents why: a 403
on the second confirms the session exists, and that a named colleague ran
something at a particular time is exactly the fact the viewer was not allowed
to learn. If the store returns distinguishable errors, collapse them in the
adapter. Never widen: an unknown store error maps to a generic failure, never
to not-found, because a database outage rendered as an empty list looks like a
person with no sessions.

Write a test per adapter that asserts a permission denial and a missing row
produce byte-identical error values.

## W6: composition root

Config comes from the environment only. No flags, no config file: the process
runs on Cloud Run where the environment is the deployment surface.

These names are not negotiable: `examples/deploy-gcp/cloudrun.yaml` already sets them
and it is the deployment surface. Read that file before writing the loader.

| Variable | Required | Meaning |
|---|---|---|
| `PORT` | no, default 8080 | Cloud Run sets this |
| `DATABASE_HOST` | yes | `/cloudsql/PROJECT:REGION:INSTANCE` socket dir on Cloud Run, or a hostname locally |
| `DATABASE_NAME` | yes | `loop_sessions` |
| `DATABASE_USER` | yes | `loop_sessions` |
| `DATABASE_PASSWORD` | yes | from Secret Manager |
| `DATABASE_PORT` | no, default 5432 | ignored when `DATABASE_HOST` is a socket path |
| `SESSION_KEY` | yes | 32+ bytes base64, cookie signing |
| `GOOGLE_OAUTH_CLIENT_ID` | yes | web sign-in |
| `GOOGLE_OAUTH_CLIENT_SECRET` | yes | web sign-in |
| `ALLOWED_DOMAINS` | yes | comma separated, checked against the `hd` claim |
| `PUBLIC_URL` | no | absolute base for OAuth redirect and share links |
| ~~`SEED_ADMINS`~~ | REMOVED | do not implement, see below |
| `LOG_LEVEL` | no, default info | slog level |
| `TZ` | no | set to Etc/UTC by cloudrun.yaml; a named zone needs `time/tzdata` |

The connection is assembled from the parts rather than taken as one DSN so the
password stays a discrete Secret Manager value. Folding it into a URL would
mean storing a second copy of the password inside a longer string, and the two
copies rotate independently, which is how a rotation half-lands.

A `DATABASE_HOST` beginning with `/` is a unix socket directory, which is how
the Cloud SQL connector exposes the instance. pgx takes that as the `host`
parameter with no port. Handle both forms; local development uses TCP.

`PUBLIC_URL` is optional because a Cloud Run URL is not known until the service
first exists. When it is unset, derive the base from the incoming request but
ONLY from a host you have already validated against an allowlist, never from a
raw `Host` or `X-Forwarded-Host` header. An attacker-controlled host reflected
into an OAuth redirect is an account takeover, and share links built from it
point victims at the attacker's domain. If you cannot make that safe, require
the variable instead and fail fast; a required variable is a worse operator
experience and a much better failure mode.

Fail fast and loudly on a missing required variable, naming every missing one
at once rather than one per restart. A server that starts with no OAuth client
and discovers it on the first sign-in is an outage found by a user.

**Do not implement `SEED_ADMINS`.** It was specified here earlier and has been
withdrawn. The first admins come from `ADMIN_EMAILS` (`store.BootstrapAdmins`,
run after migrations): each listed address with no `principals` row gets an
admin row and an existing row is never changed. A "seed only when empty"
variable would be dead configuration that looks live once the roster has a row.
An environment variable that is read, respected in code, and can never take
effect is worse than no variable: the next person sets it, observes nothing,
and goes looking for the bug somewhere real.

Roster seeding belongs in a migration rather than in boot code for the same
reason the rest of the schema does. It is versioned, applied exactly once under
the advisory lock, and auditable after the fact. Boot-time seeding runs on
every instance of every revision and has to defend itself against re-running.

Routes, mounted on one `http.ServeMux`:

- `ingest.Handler.Register` — `/v1/...` device-token authenticated
- `api.Handler.Register` — dashboard read API, cookie authenticated
- `admin.Handler.Register` — admin routes, cookie authenticated
- `web.Server` — HTML, mounted last as the catch-all
- `GET /healthz` — process liveness, no database call
- `GET /readyz` — pings the database, non-200 when it cannot serve

Liveness must not touch the database. A liveness probe that fails on database
trouble makes Cloud Run kill and restart every instance during an incident,
turning a degraded read path into a total outage.

Migrations run at boot via `store.Migrate` before the listener opens. Concurrent
instances must not race: `Migrate` needs a Postgres advisory lock so the second
instance waits rather than applying the same migration twice.

Shutdown: on SIGTERM stop accepting, drain in-flight requests with a timeout
below Cloud Run's 10s grace, then close the pool. Cloud Run sends SIGTERM
before it removes an instance from the load balancer, so a server that exits
immediately drops requests already in flight.

Logging is `slog` JSON to stdout, which is what Cloud Logging parses. Never log
a session's content, a prompt, a device token or a cookie. This server holds
colleagues' transcripts and the log is a second, less guarded copy of them.

## Definition of done, all writers

- `go build ./...` and `go vet ./...` clean at the repository root
- `go test ./... -race` green, including the tests you add
- `gofmt -l` empty for the files you own
- No `TODO`, no stub returning a zero value, no panic in a non-init path
- Every claim in your result that says something works names the test that
  proves it
