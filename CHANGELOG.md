# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each
released section's heading is `## [X.Y.Z] - YYYY-MM-DD` exactly, because the
release workflow extracts it for the release notes.

## [Unreleased]

## [0.1.0] - 2026-09-30

The first public release: the client agent, the server, the dashboard and
the example deployments, with releases people can download and verify.

### Added
- GitHub Releases for every `vX.Y.Z` tag, with the four agent binaries
  (`loop-sessions_{darwin,linux}_{amd64,arm64}`), `SHA256SUMS` and its
  keyless cosign signature `SHA256SUMS.sigstore.json`, `latest.json`,
  `install.sh` and `uninstall.sh` with their digests, and
  `THIRD_PARTY_NOTICES.md`; every asset attested with build provenance
  (`gh attestation verify <asset> --repo loopai-hq/agent-sessions`).
- The server image `ghcr.io/loopai-hq/loop-sessions-server`, tagged
  `vX.Y.Z`, `sha-<full commit>` and `latest`, with OCI labels, attested.
- The installer works from GitHub Releases without a channel host
  (`LOOP_SESSIONS_BASE_URL=.../releases/latest LOOP_SESSIONS_VERSION=download`).
- `pause --for <duration>`: capture and delivery pause for the period and
  resume on their own; `resume` ends it early; `status` and the health
  report show the deadline. New config field `paused_until`.
- `install` and `status` print who can read captured sessions (the owner,
  the server's admins, a colleague through a shared link), that every read
  by anyone but the owner is logged, and that retention is set by the
  operator.
- `mirror` is listed in `loop-sessions help`.
- CI: golangci-lint v2 with a curated configuration (`.golangci.yml`,
  depguard holding the client to the standard library), a macOS test leg,
  the integration suite on Postgres 15 and 17, govulncheck, gitleaks,
  dependency review, a weekly schedule, and OpenSSF Scorecard.
- Reproducible builds: `BUILD_DATE` defaults to the commit time
  (`SOURCE_DATE_EPOCH` honoured), CI builds every commit twice and diffs
  `SHA256SUMS`.
- `make notices` and `THIRD_PARTY_NOTICES.md`, shipped in releases and in
  the image; `make screenshots` and the four dashboard screenshots in
  `docs/images/`.
- `examples/docker-compose/`: Postgres 16 plus the server in one command.
- Documentation: `docs/ARCHITECTURE.md`, `docs/OPERATIONS.md`,
  `docs/DATA-PROTECTION.md`, `docs/THREAT-MODEL.md`, `docs/MAINTAINING.md`,
  `SUPPORT.md`, `GOVERNANCE.md`, `MAINTAINERS.md`, `ROADMAP.md`,
  `AGENTS.md` and `CLAUDE.md`, YAML issue forms, and the AI-assisted
  contribution policy in `CONTRIBUTING.md`.
- `.editorconfig`, `.gitattributes`, `.github/release.yml` categories for
  generated release notes.

### Changed
- **The server Dockerfile moved from `examples/deploy-gcp/Dockerfile` to the
  repository root** (with a root `.dockerignore`). Cloud Build triggers or
  scripts that pass `-f examples/deploy-gcp/Dockerfile` must drop the flag;
  the build context stays the repository root. New build arg `REVISION`
  (the full commit) for the OCI label; `BUILD_DATE` should be the commit
  time.
- Dependencies: pgx v5.11.0, goldmark v1.8.6, `golang.org/x/sync` v0.23.0,
  `golang.org/x/text` v0.42.0; `go 1.26.0` with `toolchain go1.26.8`; the
  Dockerfile base is `golang:1.26.8-bookworm`. These supersede the four
  Dependabot pull requests open since the org move. Dependabot now groups
  Go minor and patch updates, groups Actions updates, and tracks the
  Dockerfile's base image.
- The README is rewritten in the exemplar shape: product proof first, a
  five-minute path that needs no Firebase, a supported-harnesses table, a
  data-inventory table, and honest platform claims (macOS and Linux only;
  Codex is imported and repaired from disk, not captured live). The design
  rationale moved to `docs/ARCHITECTURE.md`.
- `server/CONTRACT-*.md` moved to `docs/design/` as dated historical notes.
- The `discover` hint names the real mechanism for unfound session
  directories (`"roots"` in `config.json`) instead of a `--set` flag that
  did not exist.
- Every exported identifier has a doc comment; builtin identifiers are no
  longer shadowed; `%w` wrapping and `errors.Is` where the auto-fixer could
  prove it; one fewer trailing newline in an enrolment error message.
- `discovery.json` in the client's state directory is written `0600`
  (was `0644`), matching every other file there.
- `SECURITY.md` states a response SLA and a supported-versions table;
  issue templates are YAML forms; the pull request template asks for an AI
  assistance line.

### Fixed
- `pause --for` was accepted and silently ignored, leaving a machine paused
  until someone ran `resume`.
- `loop-sessions help` did not list `mirror`.
- The README claimed Windows support and live Codex capture; neither was
  true.
- `reflect.Ptr` in a test fake (deprecated) and the pgx `TypeMap` the new
  driver version requires of row fakes.
- `docs/RELEASE.md` claimed reproducible builds while `BUILD_DATE` was the
  wall clock; the claim is now true and proven in CI.

### Removed
- `LaunchAgentLabel`, an unused constant carrying the previous organisation
  name.
- `examples/deploy-gcp/Dockerfile` and its `.dockerignore` (see Changed).
- The Markdown issue templates, replaced by YAML forms.

### Security
- `gitleaks` runs in CI over the tree and the history with the repository
  configuration; the three known fixture false positives are allowlisted by
  path and exact string, never by widening the paths.
- Every GitHub Action is pinned to a full commit SHA; `persist-credentials:
  false` on every checkout; least-privilege `GITHUB_TOKEN` with job-level
  elevation only where the release publishes.
- `docs/THREAT-MODEL.md` documents the trust boundaries, what a stolen
  device token can do, the third-party script on the sign-in page, and the
  read-then-run form of the installer.

[Unreleased]: https://github.com/loopai-hq/agent-sessions/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/loopai-hq/agent-sessions/releases/tag/v0.1.0
