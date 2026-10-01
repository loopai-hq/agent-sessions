# Architecture

This is the map of the country, not an atlas of its states: it names the
important packages, files and types and says why the system is shaped the
way it is, without line numbers, so it stays true across refactors. Read the
package doc comments for the field-level truth; every package has one, and
most of the rules below were written there first.

## Bird's-eye view

A session's transcript is the only record of what an AI coding agent did,
and that record otherwise lives in one directory on one machine until it is
deleted. loop-sessions moves it, scrubbed, to a server an organisation runs,
and shows it back: the session list, the transcript with tool calls and
diffs, search, cost, per-skill usage, and a fleet page that says which
laptops are reporting and which are not.

Two binaries, one Go module, no shared state except the server's database.
The **client** (`cmd/loop-sessions`, `internal/`) runs on every laptop:
it registers Claude Code lifecycle hooks, turns them into canonical events,
scrubs the events, spools them to disk and delivers them; it also imports the
transcripts that were on disk before it arrived (Claude Code and Codex). The
**server** (`server/`) receives events, stores them in Postgres as the
durable record, derives everything a reader sees from them, and serves the
dashboard, the JSON API and the operator surface.

## Why it is shaped this way

Three facts drove most of the design.

**A killed harness fires no end-of-session hook.** `SessionEnd` runs on a
clean exit and on `SIGTERM`, but not on `SIGKILL`, a closed terminal window
or a laptop losing power, and those are disproportionately the long, messy
sessions worth capturing. So a daemon spawned at session start watches the
harness process directly and finalises the session when it disappears, by
any means. Recovery for a daemon that itself dies is handled at the next
session start rather than by a background timer. A daemon whose binary is
replaced by an upgrade restarts in place shortly after (same pid, new build,
once the new file has run its `version` verb), so a long session never keeps
an old daemon.

**Transcripts contain live credentials.** Measured, not assumed: on one
laptop's real transcripts, 30 files contained Postgres connection strings
with passwords, 26 JWTs, 18 Google API keys, 5 AWS keys, 3 PEM private keys.
Scrubbing runs before any egress, and over-redaction is treated as a defect
too, since a transcript with every long string blanked is worthless as a
record.

**One logical task spans several files.** The compaction marker
(`logicalParentUuid`) names a record inside the same file, never a parent
session; reading it as lineage once produced hundreds of parents of which none
resolved, so it is carried as a record reference and nothing else. The only
lineage the harness makes provable is a fork: a new file that begins with the origin's records, uuids
and timestamps intact. Forks are found by indexing the first content uuid of
every file in a project, and the child's start event names its parent and
how much it inherited. Reaching the end of a file is not evidence of an
ending, so the backfill never emits a session-ended event: a transcript that
stops could equally be a finished session or a crashed one, and mislabelling
the crashed ones would lose exactly the sessions the system exists to keep.

## Codemap

### The client: `loop-sessions`

The client is stdlib-only. It has to install on a machine with no module
cache and no network, so every avoidable dependency is one more way an
install fails. `cmd/loop-sessions` is the CLI and the agent in one binary:
`install`, `uninstall`, `status`, `discover`, `backfill`, `pause`, `resume`,
`doctor`, `mirror`, `version`, and the two verbs the harness and the
installer invoke, `hook` and `daemon`.

| Package | What it owns |
|---|---|
| `internal/event` | the canonical, tool-agnostic record everything speaks; `CaptureSchema` lives here |
| `internal/hooks` | registering and removing this agent's hooks in Claude Code's settings file, tagged so they can be removed exactly |
| `internal/capture` | the live path: one lifecycle hook payload in, canonical events out, scrubbed, handed to the spool; no network |
| `internal/scrub` | credential redaction; every hit becomes `[REDACTED:<kind>]` so a leaked Stripe key and a leaked JWT stay distinguishable downstream; `Kinds` is the list |
| `internal/spool` | the durable outbox: immutable payload files published by atomic rename under `pending/`, `quarantine/` for what the server explicitly refused, `parked/` for what it neither accepted nor refused eight times running |
| `internal/drain` | delivery, the only path session content leaves by (`POST /v1/events`, through the transport in `cmd/loop-sessions/deliver.go`): acknowledges exactly what the server said it stored, retries transport failures forever with full-jitter backoff, treats only an explicit rejection as final |
| `internal/daemon` | a session's lifetime: spawned at session start, watches the harness pid, flushes on a cadence, finalises when the process goes away, reconciles sessions an earlier daemon left open |
| `internal/backfill` | historical transcripts to events, timestamps intact: Claude Code (including subagent and workflow files) and Codex rollouts; forks and lineage |
| `internal/discovery` | where each harness keeps its sessions (the probe table covers macOS, Linux and Windows layouts; a path that does not apply simply does not exist), honouring `CLAUDE_CONFIG_DIR` and `CODEX_HOME`; three states per tool: found, installed with no sessions where it looked, absent |
| `internal/normalize` | the one classifier for which "user" records a person actually typed, shared by every consumer that used to keep its own list |
| `internal/links` | find and classify the URLs a session mentioned (pull requests, issues, runbooks) |
| `internal/health` | the self-telemetry report the fleet page is drawn from: conditions rather than counters, on a fixed cadence, with no session ids, paths or prompt text |
| `internal/upgrade` | replace the agent binary when the published digest differs from the running file's |
| `internal/enroll` | browser sign-in exchanged for a device credential over a loopback listener; the Firebase token is spent once and never persisted |
| `internal/config` | the client's on-disk settings under `~/.loop/sessions` and the record of what a person consented to, including `paused_until` |
| `internal/pipeline` | the two adapters that wire capture to the spool and the spool to delivery, written once |
| `internal/receiver` | a reference implementation of the server side of the drain's contract; the client's end-to-end test fixture |
| `internal/skilllog` | the skill-usage log strings two server packages share |

### The server: `loop-sessions-server`

| Package | What it owns |
|---|---|
| `server/cmd/loop-sessions-server` | the entrypoint and the `export` subcommand; the Dockerfile at the repository root builds it |
| `server/app` | configuration from the environment (`config.go`), wiring, the route table, health endpoints, sign-in and enrolment handlers, the `/dl/` release proxy, drain policy, the retention and derive loops |
| `server/ingest` | `POST /v1/events` and `POST /v1/health`: every item answered individually, redelivery a no-op through the session rollup and the cost arithmetic, every payload scrubbed a second time |
| `server/store` | Postgres: the migrations under `migrations/`, the events table, the derive runner (`runner.go`), retention, authorization as a predicate inside the read, the access log, device tokens, shares; `DerivedSchema` lives here |
| `server/store/derive` | the pure fold from stored events to turns and threads |
| `server/auth` | Firebase ID-token verification (Google provider only, domain-checked, then looked up in the principals roster), device tokens (`lsd_`), the HMAC session cookie, the permission rule |
| `server/web` | the dashboard: server-rendered `html/template`, no build step, `script-src 'none'` on every page except the few that opt in to one first-party script |
| `server/api` | the JSON read API behind the same cookie: sessions, transcripts, search, share links, skill usage, the resume bundle |
| `server/admin`, `server/fleet` | the operator surface: the principals roster, the fleet evaluation (a pure function over what every laptop last said about itself), the access log; the system refuses to end up with zero admins |
| `server/skillusage` | skill invocations from emitters that are not an enrolled laptop |
| `server/slack` | the Slack mirror, one message per finished session, off for everyone until a person turns it on |
| `server/export` | the derived tables to Cloud Storage as gzip JSONL and into BigQuery, for analysts who should not be reading the primary |

None of the three optional integrations (release bucket, Slack, export) is
needed to run the server.

### Around the binaries

| Path | What it is |
|---|---|
| `install/` | `install.sh`, `uninstall.sh` and the page that explains them |
| `Makefile` | `build`, `test`, `lint`, `release`, `verify`, `notices`, `screenshots` |
| `.github/workflows/` | `ci`, `identifier-gate`, `scorecard`, `release` |
| `examples/deploy-gcp/` | a complete Cloud Run and Cloud SQL deployment with placeholders, monitoring and runbooks |
| `examples/docker-compose/` | Postgres plus the server, one command |
| `docs/` | operations, data protection, the threat model, upgrades, releases, maintaining; `docs/design/` holds dated historical design notes |

## Invariants

Each of these is an absence, and each is enforced somewhere you can point at.

- **Nothing on the hook path touches the network.** A hook runs inline with a
  person's turn on a budget of tens of milliseconds; `internal/capture`
  writes a file and returns. Session content leaves only through the drain's
  transport (`POST /v1/events`); the agent's other requests (the health
  report, enrolment, the self-upgrade check against `/dl`, `mirror`, the
  repair walk's `GET /v1/repair`) go to the same enrolled server and carry
  no transcript content. The hook verb returns zero unconditionally and never
  writes to stdout, because a gap in telemetry is better than a degraded
  editor.
- **Scrub runs before any byte reaches the spool.** Redaction happens in
  `internal/capture` and `internal/backfill` before the spool write, not at
  upload time. The server scrubs again on ingest and counts what the agent
  missed, so an agent whose rules have fallen behind is visible rather than
  silently leaking.
- **Staleness is a digest, not a version.** `internal/upgrade` hashes the
  running binary and compares it with the published `SHA256SUMS`; different
  bytes mean stale, whichever way the version moved, so a deliberate rollback
  propagates like a release. Builds are reproducible so that two builds of
  one commit are one digest.
- **The client imports only the standard library.** `cmd/` and `internal/`
  have no third-party import; `depguard` in `.golangci.yml` fails the build
  on one. A new `require` for the client is a design discussion.
- **Events are the source of truth.** Everything in `sessions`, `turns`,
  `messages`, `links`, `artifacts` and their versions is a pure function of
  the `events` table and can be discarded and recomputed by the derive
  runner. Backup the events; the rest is a rebuild.
- **Authorization is inside the read.** `server/store` applies the
  owner/admin/share predicate in the query, and a read of a colleague's
  session writes its audit row in the same transaction. A session the viewer
  may not see and one that does not exist produce byte-identical responses.
- **The health report names no session.** No session ids, project paths or
  prompt text appear anywhere in `internal/health`'s report, so it can be
  shipped fleet-wide and land in metric labels.
- **The dashboard renders transcripts as data, never as markup or script.**
  Escaped server templates, `script-src 'none'` on the archive pages, one
  first-party script on the pages that opt in, and every one of those pages
  works with the script blocked.
- **Only one credential on the laptop.** The device token minted at
  enrolment. The Firebase token is spent once against the server and is never
  written to disk.

## Boundaries and layers

- **Laptop to server**: the wire is `POST /v1/events` and `POST /v1/health`
  with a device token; the payload is `internal/event` at `CaptureSchema`.
  The two halves deploy separately, so the server accepts what older agents
  send and the agent never assumes the server it enrolled with is current.
  `docs/UPGRADES.md` says when to bump `CaptureSchema` and when to bump
  `DerivedSchema` instead, and what each costs.
- **Store to readers**: `server/web`, `server/api` and `server/admin` are
  each built against their own consumer-defined port interfaces and tested
  against their own fakes; `server/app` binds them to `server/store`. The dashboard may not import `server/store` (the pure
  `server/store/derive` fold is the one exception), and `server/fleet`
  imports the standard library only.
- **Browser to server**: sign-in is Firebase Authentication with the Google
  provider; the page loads the SDK from `www.gstatic.com` and `apis.google.com`
  under a Content-Security-Policy naming exactly those origins, hands one ID
  token to `POST /auth/session`, and the server mints its own HMAC cookie. The
  server holds no OAuth client and, for sign-in, calls no Google API other than
  fetching the published certificates.
- **Release host to fleet**: the layout under `<base>/<channel>/` is one
  contract between the `Makefile`, `install/install.sh`, `internal/upgrade`
  and `server/app/download.go`; `docs/RELEASE.md` names every consumer.

## Cross-cutting concerns

- **Idempotency.** Event ids are deterministic, so a redelivery is a no-op
  and a re-walk with a higher `capture_version` replaces what is stored
  (`docs/UPGRADES.md`).
- **Logging.** One JSON line per event on stdout, `severity` and `message`
  as the contract, no session content in any line; the message strings are
  what alerts filter on (`docs/OPERATIONS.md`).
- **Migrations.** Applied at boot under a Postgres advisory lock, each file in
  its own transaction, additive only, so two instances of a rolling deploy
  cannot apply one twice and the previous revision keeps working against the
  new schema.
- **Testing.** Every package with behaviour has unit tests against fakes
  (`internal/skilllog` is constants only); `*_integration_test.go`
  files need a database and the `integration` build tag and run in CI against
  Postgres 15 and 17; `server/web` can serve the whole dashboard against
  fixtures (`TestServeDemo`) without a database or a Firebase project, which
  is also where the screenshots come from.
- **Identifiers.** Nothing in the tree names a real organisation's
  infrastructure or people; `.github/scripts/identifier-gate.sh` enforces the
  mechanical part and the placeholders are `__PROJECT__`, `sessions.example.com`
  and `you@example.com`.
