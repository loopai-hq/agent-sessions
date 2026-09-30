# Threat model

What loop-sessions trusts, what it does not, what an attacker in each
position can do, and how it is noticed. It is written against the code, so
each claim names the package that enforces it. Report anything that
contradicts it as described in [SECURITY.md](../SECURITY.md).

## Trust boundaries

```
 laptop                                   server                       elsewhere
 ┌──────────────────────────────┐        ┌──────────────────────┐    ┌───────────────┐
 │ Claude Code ── hook ─▶ spool │─drain─▶│ ingest ─▶ Postgres   │    │ Google certs  │
 │ (user's account)   daemon    │  lsd_  │ auth  ◀── cookie ────│◀───│ Firebase      │
 │ device.token (mode 0600)     │        │ web / api / admin    │    │ (browser only)│
 └──────────────────────────────┘        │ /dl  ◀── bucket      │    │ release host  │
                                         └──────────────────────┘    └───────────────┘
```

| Boundary | Credential | Who is trusted |
|---|---|---|
| Laptop to server | the device token (`lsd_…`), minted at enrolment, stored in `~/.loop/sessions/device.token` (mode 0600, a separate file from the config) | the person whose account the agent runs under; the server trusts the token to speak for that person's device and nothing more |
| Browser to server | a Firebase ID token, spent once for an HMAC session cookie (`__Host-loop_session`, HttpOnly, SameSite=Lax, Secure unless `PUBLIC_URL` is `http://`) | the person; Google, to have authenticated them |
| Server to database | the `DATABASE_*` credentials | the operator's platform |
| Server to Google | none outbound beyond fetching the public certificates | Google's key publication |
| Server to release host | the bucket's service account, only when `RELEASE_BUCKET` is set | the operator |
| Admin to everyone | the roster row with `role = admin` | the operator's admins, who can read every session and leave an audit row each time |

## The laptop

**The agent runs as the person, with the person's rights.** It reads the
harness's transcript directories, writes under `~/.loop/sessions`, registers
hooks in `~/.claude/settings.json`, and installs to `~/.local/bin` without
`sudo`. Anyone who can write to that home directory can replace the binary
or edit the hook entries, which is the same position as being able to
replace Claude Code itself; the model does not defend against a compromised
user account.

**The hook path is inline with the person's turn.** Each hook reads a
payload on stdin, scrubs, writes a spool file and returns zero
unconditionally without writing to stdout, because a hook that errors or
chatters degrades the editor. Nothing on that path opens a connection
(`internal/capture`). Session content leaves only through delivery
(`POST /v1/events`); the agent's other requests (the health report,
enrolment, the self-upgrade check against `/dl`, `mirror`, the repair walk's
`GET /v1/repair`) go to the same enrolled server and carry no transcript
content. A hook cannot be made to exfiltrate to a third party by anything in
a transcript: it has no network and the destination is the enrolled endpoint
in the config, which the transcript cannot change.

**Transcript content is untrusted.** A page the agent fetched or a file it
read can contain anything, including text that looks like a credential or a
hook payload. The scrubber treats it all as text; the server renders it as
escaped data under `script-src 'none'` (below); the classifier in
`internal/normalize` decides which "user" records a person typed so
injected records are not credited to them.

**The Firebase token is never on disk.** Enrolment (`internal/enroll`) opens
a loopback listener, the browser posts the ID token to `127.0.0.1` only, the
token is spent once against `POST /v1/enroll/complete`, and the device token
that comes back is the only credential persisted. Plain `http://` endpoints
are accepted only for the loopback literal.

**Paused means paused.** `pause` (with or without `--for`) is honoured by
capture, backfill, the repair walk, delivery and the upgrade check; the
health report says so, and the fleet page shows it.

## A stolen device token

What it can do: `POST /v1/events` and `POST /v1/health` as that device, and
the few device-authenticated reads the client itself uses (`GET /v1/repair`,
the Slack mirror preference routes). It cannot read anyone's sessions: the
dashboard and the JSON read API take the session cookie, not a device
token, and the token authenticates a device, never a person's browser. The
worst outcomes are fabricated or duplicated events attributed to that
person, and misleading health reports.

How it is noticed: every ingest batch logs `email`, `device_id` and
`agent_version`; the fleet page shows each device's hostname, last-seen time
and conditions, so a second machine reporting under one device id, an agent
version that does not match the fleet, or a revoked device that keeps
reporting all surface there (a revoked device is carried, not filtered, for
exactly this reason: `server/fleet` and `server/admin/fleet.go`). Event ids are deterministic and
idempotent, so replaying captured batches changes nothing.

How it is revoked: set `revoked_at` on the device and its tokens
([OPERATIONS.md](OPERATIONS.md#revoking-a-device)); a revoked token is
refused on the next request. Device tokens do not expire on their own, by
design (a laptop offline for two months must still deliver what it
captured), so revocation is the control, and it is an operator action until
the admin route on the roadmap exists. The token is stored hashed; a
database read does not yield usable tokens.

## The server

**Sign-in.** `server/auth` verifies the Firebase ID token's signature
against Google's published certificates, pins `iss` and `aud` to the
configured project, requires the `google.com` provider, checks the address's
domain against `ALLOWED_DOMAINS`, and then looks the address up in the
roster. A verified account on an allowed domain with no row is enrolled as a
member; a disabled row is refused. The server holds no OAuth client secret;
the web API key is public by design. The cookie is HMAC-signed with
`SESSION_KEY` (at least 32 bytes, refused otherwise) and carries no
credential itself. Sign-in failures log the check that failed, never the
token.

**Authorization.** The owner/admin/share predicate is inside the query
(`server/store`), a forbidden and a missing session are byte-identical
from outside (`server/api`), and a read of someone else's session writes its
`access_log` row in the same transaction. Admin routes require the admin
role; the roster refuses to remove its last admin.

**Ingest.** Bodies are bounded: `MaxBatchBytes` (4 MiB by default) is the
advertised cap, answered with a per-item rejection when exceeded, and
`HardBodyLimit` (eight times that) is the real memory bound. Enrolment is
rate-limited per address (a burst of five, then one per ten minutes). There
is no per-device rate limit on `/v1/events` beyond the body bounds and the
pool's 30 s statement timeout; an enrolled device can send as fast as the
database accepts, which is a capacity concern rather than a data one.

**The dashboard.** Server-rendered `html/template` with no build step;
transcript markup is produced by escaped templates. Event archive and
per-event pages ship no JavaScript and carry `script-src 'none'`; the
filter pages and the opt-in continuous view load one first-party script
each and work with it blocked. Cookies are `__Host-` prefixed and HttpOnly.

**Third-party script on the sign-in and CLI-enrolment pages.** These two
pages, and only these, load the Firebase SDK from `https://www.gstatic.com`
at a pinned version (`server/web/static/signin.js`, `cli.js`); the SDK in
turn needs `https://apis.google.com`. The Content-Security-Policy for those
routes names exactly those origins for `script-src`, the auth domain and
Google's accounts pages for `frame-src`, and Google's identity endpoints for
`connect-src` (`server/app/signin.go`). The trust placed here is that Google
does not serve a malicious SDK from its own origin, which is the same trust
already placed in Google authenticating the person. The consequence of a
compromised SDK would be a stolen ID token for one sign-in, not a session's
content. The pinned version is checked monthly
([MAINTAINING.md](MAINTAINING.md)); Dependabot has no ecosystem for a URL
import. No other page loads anything from a third party.

**The database.** Nothing in a log line carries a session's content, a
prompt, a device token or a cookie; the ingest tests grep the log for
payload text. Device and source tokens are stored hashed. Retention and the
purge recipe are the controls over what a database compromise exposes over
time ([DATA-PROTECTION.md](DATA-PROTECTION.md)).

## The release path and `curl | sh`

The published one-liner pipes a script from GitHub Releases into `sh`.
Read it first: fetch `install.sh` and `install.sh.sha256` from the same
release, check one against the other, read the script, then run it:

```sh
base=https://github.com/loopai-hq/agent-sessions/releases/latest/download
curl -fsSLO "$base/install.sh" && curl -fsSLO "$base/install.sh.sha256"
sha256sum -c install.sh.sha256      # shasum -a 256 -c on macOS
less install.sh
LOOP_SESSIONS_BASE_URL=https://github.com/loopai-hq/agent-sessions/releases/latest \
  LOOP_SESSIONS_VERSION=download sh install.sh
```

Both scripts are attested by the release workflow (`gh attestation verify
install.sh --repo loopai-hq/agent-sessions`). The script itself is POSIX
`sh` with `set -eu`, defines every function before the calls at the
end (a download truncated before that block executes nothing), fetches with
`--proto '=https' --tlsv1.2`, refuses to install without a SHA-256 tool,
verifies the binary against `SHA256SUMS` before it is made executable,
deletes it on a mismatch, never uses `sudo`, and runs the new binary once
before replacing an old one. Why a shell pipe rather than a downloadable
installer is a measured macOS Gatekeeper fact, explained in
[install/README.md](../install/README.md).

The binaries are reproducible from a clean clone at the tag, attested with
build provenance and signed with a keyless cosign bundle over `SHA256SUMS`;
[RELEASE.md](RELEASE.md#verifying-a-release) has the three checks. The
agent's self-upgrade reads `<endpoint>/dl/<channel>/SHA256SUMS` from the
server it is enrolled with, compares digests, fsyncs and verifies the
download before making it executable, runs it once, and only then renames
it over itself; the server's `/dl/` is a proxy to a bucket the operator
controls. A compromised release host can therefore ship a malicious agent to
the fleet, which is why the bucket is the operator's, why `latest.json`
names the commit, and why builds are reproducible: a third party can rebuild
the tag and compare.

## Out of scope

- A compromised user account or a root compromise of the laptop.
- A compromised Google account: Firebase sign-in is the identity provider;
  its second factor and session policies are yours to set.
- A hostile operator: admins can read every session by design, and the
  access log is the control, not a prevention.
- Side channels in the harness itself: the agent captures what Claude Code
  reports through its hooks and what the transcripts contain; it does not
  sandbox the harness.
