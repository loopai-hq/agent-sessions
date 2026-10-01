# Operating the server

Platform-neutral notes for whoever runs `loop-sessions-server`: the
endpoints, every environment variable, the log contract, backup and restore,
retention, upgrade and rollback, and what to alert on. The Cloud Run and
Cloud SQL specifics, the log-based metrics and the twenty-odd runbooks that
go with them are in [examples/deploy-gcp/README.md](../examples/deploy-gcp/README.md);
a one-command local stack is in
[examples/docker-compose/README.md](../examples/docker-compose/README.md).

## Endpoints

The server listens on one port (`PORT`, default 8080) and mounts:

| Path | Who calls it | What it does |
|---|---|---|
| `GET /healthz`, `GET /livez` | your orchestrator | liveness: `ok <version>`, no database touched, so a dead database does not restart a healthy process |
| `GET /readyz` | your load balancer | readiness: pings the database with a 2 s ceiling; `ready` or `503 not ready`, the reason only in the log |
| `POST /v1/events`, `POST /v1/health` | enrolled agents, with a device token | ingest; every item answered individually |
| `POST /v1/enroll/complete` | the agent's enrolment flow, from the browser | exchanges a Firebase ID token for a device token; rate-limited per address |
| `GET /auth/signin`, `GET /auth/cli`, `POST /auth/session`, `POST /auth/signout` | people, in a browser | Firebase sign-in for the dashboard and for CLI enrolment; the server mints its own HMAC cookie |
| `GET /`, `/sessions`, `/sessions/{id}`, `/search`, `/analytics`, `/skills`, `/settings/notifications`, `/shared/{token}` | signed-in people | the dashboard |
| `GET /admin/fleet`, `/admin/principals`, `/admin/access` | admins | the fleet page, the roster, the access log |
| `GET /v1/sessions`, `/v1/sessions/{id}`, `/v1/sessions/{id}/events`, `/v1/search`, `/v1/skills/...`, `POST /v1/sessions/{id}/share`, `GET /v1/shared/{token}` | the same cookie | the JSON read API |
| `GET /v1/admin/principals`, `PUT /v1/admin/principals/{email}`, `GET /v1/admin/fleet`, `GET /v1/admin/access-log`, `POST /v1/admin/derive/skill-invocations/rerun`, `POST /v1/admin/source-tokens...` | admins | the operator API |
| `POST /v1/skill-invocations`, `POST /v1/skill-invocations/reconciler-runs`, `PUT /v1/skill-catalog/{source_repo}` | emitters that are not a laptop, with a source token | skill usage from other platforms |
| `GET /install.sh`, `GET /dl/{channel}/{asset}` | installers and the agents' self-upgrade | mounted only when `RELEASE_BUCKET` is set |

Anything else under `/v1/` answers the API's own 404. There is no metrics
endpoint; everything an operator can see is the log stream below (a portable
metrics endpoint is on [ROADMAP.md](../ROADMAP.md)).

## Configuration

The server reads everything from the environment (`server/app/config.go`)
and reports every missing required variable in one error at boot. Required:

| Variable | Meaning |
|---|---|
| `DATABASE_HOST` | hostname for TCP, or a directory starting with `/` for a unix socket |
| `DATABASE_NAME`, `DATABASE_USER`, `DATABASE_PASSWORD` | the Postgres role and database |
| `SESSION_KEY` | base64, decoding to at least 32 bytes; signs the dashboard cookie. Logged as `set`, never verbatim |
| `FIREBASE_PROJECT_ID` | the Firebase project both sign-in surfaces verify against |
| `FIREBASE_API_KEY` | the project's web API key; public by design (it is rendered into the sign-in page) and logged verbatim |
| `ALLOWED_DOMAINS` | comma-separated email domains permitted to sign in; a Google account on a listed domain is enrolled as a member on first sign-in |
| `PUBLIC_URL` | the origin people reach the dashboard at; an `http://` value switches cookies to insecure, for local use only, and is warned about at boot |

Optional:

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | listener port |
| `DATABASE_PORT` | `5432` | sent for a unix socket too, because the socket file is named after it |
| `FIREBASE_AUTH_DOMAIN` | `<project>.firebaseapp.com` | the auth domain the sign-in page loads |
| `ADMIN_EMAILS` | unset | comma-separated addresses, each in `ALLOWED_DOMAINS`, given an admin row at boot when they have none; an existing row is never changed, so it grants a first admin but never re-grants, promotes or re-enables (the People page does that) |
| `DOMAIN_ALIASES` | unset | comma-separated `a.example:b.example` pairs of `ALLOWED_DOMAINS` entries that one Workspace serves as the same accounts; leave unset with one domain |
| `RELEASE_BUCKET` | unset | Cloud Storage bucket to serve `/install.sh` and `/dl/` from; unset means those routes are not mounted and agents log an upgrade error per check |
| `SLACK_BOT_TOKEN` | unset | enables the Slack mirror |
| `LOOP_SESSIONS_SLACK_SIGNING_SECRET` | unset | enables Slack interactive replies |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` (or `warning`), `error`; anything else is refused at boot |
| `RETENTION_BODY_DAYS`, `RETENTION_SESSION_DAYS` | `0` | days before event bodies, then whole sessions, are deleted; `0` keeps forever and is warned about at boot; see [Retention](#retention) |
| `DERIVE_WINDOW`, `DERIVE_ROWS_PER_SEC`, `DERIVE_ROWS_PER_BATCH` | `02-06`, `1500`, `5000` | bounds on the background derive runner; the window is `HH-HH` in the server's `TZ` (or `always`) and gates only the body-reading steps |
| `TZ` | the container's, `Etc/UTC` in the examples | read by the Go runtime, not by the server: it sets the zone every dashboard timestamp is rendered in (the dashboard never uses the viewer's clock: the archive pages ship no JavaScript) and the zone `DERIVE_WINDOW` is interpreted in. A named zone needs a zone database at runtime; the distroless image carries one |

The `export` subcommand (`server/export`) reads the same `DATABASE_*`
variables plus `EXPORT_PROJECT`, `EXPORT_BUCKET`, `EXPORT_DATASET`,
`EXPORT_LOCATION`, `EXPORT_MAX_SESSIONS`, `EXPORT_MAX_SESSION_DAYS` and
`EXPORT_MAX_EVENT_DAYS`; it is Google-Cloud-specific and documented with the
example deployment.

## The log contract

One JSON line per event on stdout, written by `app.NewCloudLogger`. Every
line carries `severity` (`DEBUG`, `INFO`, `WARNING`, `ERROR`), `message`,
`timestamp` and `version` (the build the line came from, so two revisions
serving side by side during a rollout stay distinguishable); lines logged
inside a request also carry `logging.googleapis.com/trace` and `spanId`,
read from `X-Cloud-Trace-Context` or `traceparent`. The key names are Cloud
Logging's; any JSON-aware log stack reads them (a switch to `trace_id` and
`span_id` naming for other stacks is on [ROADMAP.md](../ROADMAP.md)).

No line carries a session's content, a prompt, a device token or a cookie;
the ingest tests grep the log for payload text to keep it that way. The
`message` strings are exact and are what alerts filter on. The ones that
matter on any platform:

| Message | Severity | What it means |
|---|---|---|
| `starting`, `serving`, `stopped` | INFO | boot with the whole configuration (secrets as `set`), the listener address, and exit |
| `migration applied` | INFO | one per file `store.Migrate` applied at boot |
| `admin bootstrapped` | INFO | `ADMIN_EMAILS` created an admin row |
| `retention is switched off, so this database grows without bound and never releases anything` | WARNING | both retention windows are 0; the next line prices what the recommended windows would remove |
| `retention sweep`, `retention sweep failed` | INFO, ERROR | one per six-hourly sweep |
| `ingest batch` | INFO | one per `POST /v1/events`: `email`, `device_id`, `agent_version`, `items`, `accepted`, `duplicate`, `rejected`, `undecided`, `bytes`, `status` |
| `ingest item rejected` | WARNING | an item refused for good; the agent quarantines it |
| `store returned no verdict` | ERROR | an admitted item the store answered for neither way: a server bug |
| `ingest request failed` | ERROR | the detail behind every 5xx the ingest routes answer |
| `ingest scrubbed a credential the agent missed` | WARNING | the server-side scrub caught something the laptop's did not: `kinds` says what |
| `readiness check failed` | WARNING | `/readyz` answered 503 |
| `derive pass`, `derive step`, `derive step failed`, `derive session failed` | INFO, INFO, ERROR, ERROR | the derive runner; `derive step failed` carries the `UPDATE derive_jobs ...` statement that lets the step retry |
| `fleet summary` | INFO | one per five-minute evaluator tick: `enrolled`, `reporting`, `silent`, `never_reported`, `capture_blocked`, `drops`, `behind`, `ctas`, ... |
| `fleet silent`, `fleet drops`, `fleet empty_start` | WARNING | one machine that stopped reporting for a day, dropped events, or is over the empty-start rule |
| `fleet condition`, `fleet version_lag` | INFO | per-machine conditions from the latest health reports |
| `release manifest missing` | WARNING | `latest/latest.json` is absent from the release bucket |
| `sign-in refused` | WARNING | a sign-in refused before the roster check; `stage` names the check that fired (origin, body, csrf, token, verify or domain) |
| `sign-in refused: no enabled roster row` | INFO | a verified token on an allowed domain whose roster row is missing or disabled |
| `slack mirror pass failed`, `slack live pass failed` | ERROR | the Slack mirror, when configured |
| `export run` | INFO on success, ERROR on failure | one per `export` run, `status` `ok`, `failed` or `timeout` |

The full field list for every line, and the GCP alert policy built on each,
is in the example deployment's "The log contract" section.

## What to alert on

Expressed as filters on the lines above, so any log stack can implement
them. The first four are the ones that page.

| Alert | Filter | Why |
|---|---|---|
| Ingest failing | `message="ingest request failed"` more than a handful in 5 min | agents retry forever, so the fleet's spools grow silently until this is fixed |
| Store bug | any `message="store returned no verdict"` | an admitted item was neither stored nor refused; the agent will park it after eight tries |
| Not ready | `message="readiness check failed"` sustained for 2 min | the database is unreachable or slow; `/readyz` is already 503 |
| Derive stalled | `message="derive step failed"` or `derive pass` with `stalled_minutes` above 30 | new sessions stop appearing as turns; the line carries the recovery statement |
| Retention off | `message="retention is switched off, so this database grows without bound and never releases anything"` at boot | a decision, not a fault, but one to make deliberately: the next line prices it |
| Retention failing | `message="retention sweep failed"` twice in a row | disk will fill on the schedule the boot line predicted |
| Agent scrubber behind | `message="ingest scrubbed a credential the agent missed"` | a laptop is running rules older than the server's; look at its `agent_version` |
| Fleet silent | `message="fleet silent"` for a machine you expect to see | the laptop stopped reporting a day ago; the fleet page has the CTA |
| Dropped events | `message="fleet drops"` | a laptop's dropped-event counter rose since the last tick |
| Release manifest missing | `message="release manifest missing"` | with `RELEASE_BUCKET` set, nothing can upgrade until `latest/latest.json` exists |
| Export lag | `message="export run"` with `status!="ok"` twice, or `lag_seconds` above 3 h | only with the export job |

Alongside the log filters: an external HTTP check on `/livez` (answers with
the build), a Postgres disk-free alert (retention's whole purpose), and, on a
platform with request logs, the 5xx rate on `/v1/events`.

## Backup

Back up the whole database, `pg_dump -Fc` or your platform's point-in-time
recovery. Everything derived (`sessions`, `turns`, `messages`, `links`,
`artifacts`, `artifact_versions`) is a pure function of the `events` table
and can be rebuilt while the bodies are still there, so in an emergency
`events` plus the tables that are not derived from it is a complete backup,
including: `principals` (the roster) and `principal_changes`, `devices` and
`device_tokens` (the fleet's credentials), `access_log` (who read what) and
`admin_actions`, `shares`, `usage_ledger`, `model_prices`, `health_reports`,
`fleet_mutes`, `source_tokens`, `session_mirrors`, `reconciler_runs`, the
`skill_*` and `slack_*` tables, and the ledgers `schema_migrations`,
`derive_jobs`, `derived_schema`, `fleet_ticks` and `export_watermarks`. That
shortcut stops holding once `RETENTION_BODY_DAYS` is set: body retention
deletes `events.body` but keeps `messages`, `turns` and the search index,
and the runner cannot re-derive from an expired body, so from then on the
derived tables hold the only copy of expired transcripts and must be backed
up too. `events.body` is roughly 65% of the database, so a dump is dominated
by it; a nightly dump plus WAL archiving is enough for a service whose
writers retry until acknowledged.

## Restore

1. Restore the dump into an empty database, `pg_restore` or your
   platform's equivalent. `schema_migrations` travels with it.
2. Start the server. `store.Migrate` runs at boot under an advisory lock and
   applies any file newer than the dump before the listener opens.
3. If the dump predates a derived-schema bump, or a derived table was
   restored inconsistently, force a rebuild from `events` (next section).
4. Agents notice nothing: every laptop keeps its spool until the server
   acknowledges each item, and event ids are deterministic, so anything
   captured between the dump and the restore is delivered again and stored
   once.

## Rebuilding the derived tables

The derive runner (`server/store/runner.go`) records every finished step in
`derive_jobs` and stamps `derived_schema` when no step of the current version
is pending. To re-derive everything from `events`, with `N` the value of
`store.DerivedSchema` in the running build:

```sql
DELETE FROM derive_jobs WHERE version = N;
UPDATE derived_schema SET version = 0 WHERE only_row;
```

The running instance notices at its next check (six hours after it last saw
the stamp) or at its next boot and runs the versioned pass again, inside
`DERIVE_WINDOW`, at `DERIVE_ROWS_PER_SEC`. Setting the stamp to `N - 1`
instead runs only the steps new in `N`. [UPGRADES.md](UPGRADES.md) explains
both, what a rebuild costs, and the one step that has its own admin route
(`POST /v1/admin/derive/skill-invocations/rerun`).

## Retention

Both windows default to 0, which keeps everything forever, and the server
says so at boot and prices the alternative. Measured, one heavy user is
58 MB a day and 65% of it is `events.body`; at fifty people that is 2.9 GB a
day and about a terabyte a year unbounded. The recommended starting point,
priced in the boot log, is `RETENTION_BODY_DAYS=90` and
`RETENTION_SESSION_DAYS=365`: a quarter of readable transcripts, then a year
of the rollup, cost, search index and shape of the work at a twentieth of
the size.

Rules the store enforces at boot (`server/store/retention.go`): a non-zero
window below seven days is refused, because `RETENTION_SESSION_DAYS=1` where
100 was meant deletes nearly everything on the next tick with no undo; a
body window longer than the session window is refused, because it could
never take effect; zero is never a cutoff. The sweep runs every six hours in
bounded batches and logs `retention sweep`. Retention is also the legal
control: [DATA-PROTECTION.md](DATA-PROTECTION.md) covers what it means for
the people whose sessions these are.

## Upgrade

Run the new build; migrations apply at boot before the listener opens, one
advisory lock across instances so a rolling deploy applies each file once
and a second instance waits rather than races. Every migration is additive
(new tables, new columns with defaults, idempotent statements), which is what
makes the previous build keep working against the new schema during and
after the rollout.

- **Server image.** `ghcr.io/loopai-hq/loop-sessions-server:vX.Y.Z` (or
  `sha-<full commit>`); `latest` follows the newest non-prerelease. Verify
  the image's provenance with `gh attestation verify` as
  [RELEASE.md](RELEASE.md#verifying-a-release) describes.
- **Agents.** With `RELEASE_BUCKET` set, publish `dist/` to
  `<bucket>/latest/` and every daemon replaces itself within six hours,
  comparing digests, not versions. Without a bucket, laptops re-run the
  installer from GitHub Releases ([RELEASE.md](RELEASE.md#github-releases)).
- **Compatibility.** The wire is `internal/event` at `CaptureSchema`; the
  server keeps the highest `capture_version` it has seen for an event and
  never rejects a lower one, so an older agent keeps delivering to a newer
  server and a newer agent's re-walk replaces what an older one stored.
  Upgrade the server first, then the fleet.

## Rollback

Start the previous build. Migrations are forward-only and additive, so the
old build runs against the new schema; the one thing a downgrade cannot do
is un-derive: rows a newer `DerivedSchema` wrote stay until the next bump
rebuilds them, which is harmless because they are a function of `events`.
If a release corrupted derived data rather than schema, roll the build back
and force a rebuild (above) instead of restoring a dump: `events` is intact.
Rolling the agents back is publishing the older `dist/` to `latest/`; the
digest comparison propagates it like any release.

## Purging one person

There is no admin route or CLI for per-person erasure yet (it is on
[ROADMAP.md](../ROADMAP.md), with export). The recipe is one transaction an
operator runs by hand, keyed on the address so no statement is hand-edited,
in foreign-key order. `principals` and `access_log` are kept on purpose: the
roster row is what the sign-in check reads, and the access log is the record
of who viewed whose sessions, which outlives its subject. Record that record
first, because it is the last readable answer to the question:

```sql
SELECT viewer, session_id, via, at FROM access_log WHERE owner = :'who' ORDER BY at;
```

Then, with `psql -v ON_ERROR_STOP=1 -v who='<email>' -v me='<your email>' -f purge.sql`:

```sql
BEGIN;
UPDATE skill_invocations SET
  session_type = COALESCE(NULLIF(session_type, ''), (SELECT s.session_type FROM sessions s WHERE s.session_id = skill_invocations.session_ref), ''),
  actor_email = NULL, device_id = NULL, source_token_id = NULL, preempted_by = NULL, preempted_device = NULL,
  session_ref = '', event_id = NULL, prompt_id = NULL, tool_use_id = NULL, dedupe_key = 'anon:' || id
WHERE actor_email = :'who';
DELETE FROM usage_ledger WHERE email = :'who';
DELETE FROM events WHERE email = :'who';        -- cascades messages and artifact_versions
DELETE FROM sessions WHERE email = :'who';      -- cascades artifacts, links, shares and session-keyed slack_posts
DELETE FROM slack_posts WHERE email = :'who';
DELETE FROM slack_prefs WHERE email = :'who';
DELETE FROM health_reports WHERE email = :'who';
DELETE FROM devices WHERE email = :'who';       -- cascades device_tokens
UPDATE source_tokens SET revoked_at = now(), revoked_by = :'me'
  WHERE bound_actor_email = :'who' AND environment = 'laptop' AND revoked_at IS NULL;
UPDATE source_tokens SET bound_actor_email = NULL WHERE bound_actor_email = :'who';
COMMIT;
```

Afterwards `SELECT count(*) FROM sessions WHERE email = :'who'` is zero. A
reinstall for the same person on the same laptop must pass `--skip-backfill`,
or the history still on that machine is imported again under the ids the
purge removed. The Cloud SQL version of this recipe, with the proxy and
Secret Manager steps, is "Runbook: purge a person" in the example deployment.

## Revoking a device

A stolen or retired laptop: the device's rows carry `revoked_at`, and a
revoked device that keeps sending health reports is shown on the fleet page
rather than filtered out, because that is a finding. There is no route for
the revocation yet (the store method exists; the route is on the roadmap
with the erasure route), so until then:

```sql
UPDATE device_tokens SET revoked_at = now() WHERE device_id = '<id>' AND revoked_at IS NULL;
UPDATE devices SET revoked_at = now() WHERE id = '<id>' AND revoked_at IS NULL;
```

Device tokens do not expire on their own: a laptop that has been offline for
two months must still be able to deliver what it captured, so revocation is
deliberate rather than silent.
