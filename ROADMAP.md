# Roadmap

What is planned, with enough of the recipe that someone could pick it up,
and what is deliberately not planned. Nothing here is a promise or a date;
the order is roughly the order of demand. Open an issue to argue for moving
something up.

## Status

Version 0.x. The wire contract (`event.CaptureSchema`) and the derived
schema are versioned and upgrades are documented
([docs/UPGRADES.md](docs/UPGRADES.md)), but the JSON read API, the admin
API and the configuration surface may still change between minor versions;
the CHANGELOG says when they do. In production at Loop AI since before the
public release.

## Planned

### Sign-in beyond Firebase (OIDC)
Sign-in is Firebase Authentication with the Google provider only, which is
the largest self-hosting objection. Recipe: a second verifier in
`server/auth` for a generic OIDC issuer (discovery document, JWKS, `iss` and
`aud` pinned from configuration, an `email` claim, `email_verified`), chosen
by environment variables, with the same domain gate and roster lookup
afterwards; the sign-in page loads no third-party SDK on that path. The
CLI-enrolment page needs the same. Firebase stays the default until the
second path has run in production somewhere.

### Per-person export and an admin erasure route
Erasure exists as a hand-run SQL recipe ([docs/OPERATIONS.md](docs/OPERATIONS.md#purging-one-person));
what is missing is an admin route and a CLI verb that run the same
transaction, write an `admin_actions` row, and refuse to run for the last
admin, plus an export: everything held about one address (events, sessions,
usage, health reports, devices, access-log rows where they are the viewer)
as a zip of JSONL, streamed. Both belong on the People page.

### A route to revoke a device
`store.RevokeDevice` exists; a button on the fleet page and a
`POST /v1/admin/devices/{id}/revoke` route do not.

### A portable metrics endpoint
Everything an operator sees today is the log stream; every metric in the
GCP example is a log-based metric. Cheapest honest option with no new
dependency: `expvar` on a separate `METRICS_PORT`, exporting the counters
the log-based metrics extract (ingest accepted/rejected/undecided/5xx,
readiness failures, derive stall minutes, the fleet gauges). A Prometheus
client is a new `require` and therefore a design issue first.

### `LOG_FORMAT` switch
The log line's trace keys are Cloud Logging's (`logging.googleapis.com/trace`).
A `LOG_FORMAT=cloud|json` variable selecting `trace_id`/`span_id` naming
for other stacks; same lines, same messages.

### A GitHub-Releases-backed `/dl` redirector
The agent's self-upgrade reads `<endpoint>/dl/<channel>/` from the server,
whose `/dl/` is a proxy to a bucket. A second backend, selected by an
environment variable, answers `latest/<asset>` with a redirect to
`releases/latest/download/<asset>` and `canary/<asset>` with the newest
pre-release's asset, resolved through the Releases API and cached for five
minutes. Both clients already follow redirects; no client change.
[docs/RELEASE.md](docs/RELEASE.md#github-releases) has the design.

### A migrate-from-previous-tag test
Once two tags exist: a CI job that restores a database at the previous
tag's schema (apply that tag's migrations, seed a few sessions), starts the
new build, and asserts the migrations apply under the advisory lock and the
derive runner rebuilds cleanly. Proves the upgrade path rather than
asserting it.

### SBOM
An SPDX or CycloneDX SBOM for each release's binaries and the image,
generated in `release.yml` and attached as assets; `THIRD_PARTY_NOTICES.md`
already lists the licences.

### Harden-Runner in CI
StepSecurity's harden-runner in audit mode on every job, then block mode
once the egress list is known.

### Playwright regression in CI
`server/web/conversation.browser.cjs` and `screenshots.cjs` already drive
the demo server with Chromium; run the browser tests in CI and diff the
screenshots against the committed ones so a template change that alters a
page is visible in the pull request.

### An accessibility job
axe-core through the same Playwright harness against the demo server, as a
CI job; fix the `label for=`/`id` pairs it will find first. State the
non-goals explicitly: English only, times in the server's `TZ`.

### Windows client
Not supported today: `cmd/loop-sessions/reap.go` uses `syscall.Kill`, the
daemon detaches with `Setsid` (`cmd/loop-sessions/detach_unix.go`), the
per-OS launcher files (`internal/capture/launcher_*.go`) identify the harness
that launched the hook (`parentComm`, via `kern.procargs2` on macOS and
`/proc/<pid>/comm` on Linux), and the installer refuses anything but macOS
and Linux. Recipe: build-tag the process watching, detaching and launcher
lookup, a Task Scheduler or service equivalent for the daemon, `%APPDATA%`
discovery paths (already present for Claude Desktop), and a PowerShell
installer. A port without live capture (backfill only) is a smaller first
step.

### Evals
A repeatable measurement of the scrubber's precision and recall on a
synthetic corpus, and of the derive runner's turn classification against
labelled transcripts, run in CI and reported as numbers in the CHANGELOG.

### Smaller items
- `discover --set <tool>=<path>` as a flag over the `roots` map.
- A monthly test that fails when the pinned Firebase SDK version is older
  than N months.
- Multi-architecture server image (`linux/arm64`); today the published
  image is `linux/amd64`.
- Coverage published from CI.
- A `.github` organisation repository for the shared community files.
- A `conduct@` contact distinct from `security@`.

## Not planned

- **Homebrew or goreleaser.** The Makefile, `install.sh` and the release
  layout are one contract with the in-app upgrade; a second packaging path
  would fork it. Revisit if demand appears.
- **Go Report Card badge.** The service shut down in 2026.
- **CITATION.cff.** This is an operations tool, not research software.
- **A DCO or CLA.** Inbound contributions are under the same MIT licence as
  the project; see [CONTRIBUTING.md](CONTRIBUTING.md#licensing).
- **A ban on AI-assisted contributions.** The project is built with them;
  the policy is accountability and disclosure, not prohibition.
- **Capturing harnesses that do not write transcripts to disk or expose
  hooks.** Discovery reports them so the gap is visible; capture needs a
  source.
