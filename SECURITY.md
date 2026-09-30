# Security policy

loop-sessions reads coding-session transcripts on people's laptops and uploads
them to a server. A vulnerability here can expose a colleague's work, so
reports are taken seriously and handled privately until a fix is released.
The trust boundaries and what each position can do are written down in
[docs/THREAT-MODEL.md](docs/THREAT-MODEL.md); a report that contradicts that
document is a report we want.

## Reporting a vulnerability

Do not open a public issue for a security problem.

1. Preferred: use GitHub's private vulnerability reporting. Open the
   repository's **Security** tab and choose **Report a vulnerability**
   (<https://github.com/loopai-hq/agent-sessions/security/advisories/new>).
   The report is visible only to the maintainers until it is published.
2. Otherwise, email `security@loopai.com`.

Include what you found, how to reproduce it, and which component it is in
(client agent, installer, server, dashboard, an example deployment). A
proof-of-concept helps; a working exploit against someone else's deployment
does not, so please test only against a server you run.

**If an AI tool found the issue, say so in the report,** and reproduce it
yourself before you send it: we need to be told what the tool concluded and
what you confirmed. A report that turns out to be fabricated closes the
door on further reports from its sender.

## What to expect

- An acknowledgement within **5 business days** of the report.
- A status update within **14 days**: confirmed and being fixed, needs more
  information, or not a vulnerability, with the reasoning.
- A fix on `main` and in the next release, and a published advisory, as
  soon as the fix is ready; we will agree a disclosure date with you and
  aim for 90 days from the report at the latest.
- Credit in the fix's commit message, the advisory and [AUTHORS.md](AUTHORS.md)
  unless you ask us not to.

## Supported versions

| Version | Supported |
|---|---|
| `v0.1.0` and later `v0.x` releases | yes: fixes land on `main` and ship in the next release; the newest release is the one to run |
| `main` | yes, for self-hosters who build from source |
| anything older than the newest release | no: upgrade; there are no maintained release branches |

Releases are on [GitHub Releases](https://github.com/loopai-hq/agent-sessions/releases)
and can be verified by digest, build provenance and signature as
[docs/RELEASE.md](docs/RELEASE.md#verifying-a-release) describes. The server
image is `ghcr.io/loopai-hq/loop-sessions-server`.

## Bug bounty

There is no bug bounty programme.

## What is in scope

- The client agent (`cmd/loop-sessions`, `internal/`): anything that lets a
  hook affect the harness it runs in, lets a local process obtain the device
  credential, or gets credential material past the scrubber
  (`internal/scrub`) and onto the wire.
- The installer (`install/`): anything that weakens checksum verification or
  the HTTPS-only fetch.
- The server (`server/`): authentication and authorization (a read of a
  session the viewer may not see, an admin route reachable by a member, a
  device token accepted for the wrong person), the ingest endpoints, and
  anything that renders transcript content as live HTML or script in the
  dashboard.
- The release path (`.github/workflows/release.yml`, the attestations and
  the signature): anything that lets an asset be published that the
  workflow did not build from the tag.

Findings in the example deployments under `examples/` are welcome too, with
the caveat that they are examples rather than a supported product.

## Out of scope

A compromised user account or root on the laptop; a compromised Google
account; an operator or admin who is hostile by design (admins can read
every session, and the access log is the control); denial of service that
needs an enrolled device token, unless it crosses into another person's
data.
