# Maintaining the repository

Repository operations for the people listed in [MAINTAINERS.md](../MAINTAINERS.md):
what runs on a schedule, where alerts go, the settings that live outside the
checkout, labels, releases, dependencies, screenshots, and the checklists
before and after a tag. Code-level rules are in
[CONTRIBUTING.md](../CONTRIBUTING.md).

## What runs, and when

| Workflow | Trigger | Jobs |
|---|---|---|
| `ci` (`.github/workflows/ci.yml`) | every pull request, every push to `main`, Mondays 06:17 UTC, manual | `lint` (make lint plus golangci-lint v2.14.0 with `.golangci.yml`), `test` (Ubuntu and macOS, so the darwin-only files execute), `integration` (Postgres 15 and 17, `go test -race -tags integration ./server/...`), `build` (`make release ENDPOINT=`, `make verify`, a second build diffed against the first to prove reproducibility, `make notices` diffed against the committed file, a `docker build` of the root Dockerfile), `govulncheck`, `gitleaks` (`dir` and `git` with `.gitleaks.toml`), `dependency-review` (pull requests only) |
| `identifier-gate` | every pull request, every push to `main`, manual | `.github/scripts/identifier-gate.sh` over the tree; in this repository's own CI it also uses the `IDENTIFIER_GATE_PATTERNS` secret; forks run the generic pattern only, so a maintainer re-runs the full gate before merging a fork's pull request |
| `scorecard` | pushes to `main`, Tuesdays 06:31 UTC | OpenSSF Scorecard with SARIF upload to code scanning; `publish_results` needs a public repository |
| `release` | tags `v*`; pull requests that touch the release path (dry run); manual with `dry_run` | `build` (the dry run: build, verify, twice-build diff, govulncheck on the binary, image build) and `publish` (GitHub Release, GHCR image, attestations, cosign) |

The weekly schedules exist so `main` cannot rot between pull requests: a
dependency advisory, a base-image change or a toolchain patch shows up on
Monday rather than in the next contributor's PR. **GitHub disables a
scheduled workflow after 60 days without repository activity**; if the repo
goes quiet, expect the `ci` and `scorecard` schedules to need re-enabling
from the Actions tab, and do that before trusting a green badge.

## Where alerts go

Route these to a team address or a channel, never to one login:

- Workflow failures on `main` and on the schedules (Actions notification
  settings for the organisation).
- Dependabot alerts, code-scanning alerts (Scorecard's SARIF, and CodeQL
  once default setup is on) and secret-scanning alerts: Settings → Code
  security, "notification" recipients; the default is the repository admins.
- Private vulnerability reports: they arrive as draft advisories under the
  Security tab and email the maintainers; [SECURITY.md](../SECURITY.md)
  promises an acknowledgement within five business days and a status update
  within fourteen days, so someone has to be watching.

## Settings that live outside the checkout

None of these can be set from a pull request. The pre-tag checklist below
lists the order; this is the what.

- **Ruleset on `main`**, created once the rename pull request's merge
  commit is in (its CI jobs are the ones required): require these checks
  to pass, under the names the matrix jobs report, which are not the job
  ids: `lint`, `test (ubuntu-latest)`, `test (macos-latest)`,
  `integration (15)`, `integration (17)`, `build`, `govulncheck`,
  `gitleaks`, and `identifier-gate`'s `gate`. Require a pull request, no
  force-push, no deletion. Do not require linear history: a long-lived
  branch such as the rename lands as a merge commit by design, and the
  requirement would refuse it. Add `dependency-review` for pull requests.
- **Ruleset on tags `v*`**: restrict creation, update and deletion to the
  maintainers (the ruleset's bypass list and nobody else). A `v*` tag is
  what runs `publish`, with the signing identity and GHCR credentials, so
  without this anyone with write access can cut a signed release.
- **Code scanning**: enable CodeQL **default setup** (not an advanced
  workflow; the two conflict and this repository ships none). Scorecard's
  SARIF lands in the same tab.
- **Secret scanning and push protection**: on. `.github/secret_scanning.yml`
  and `.gitleaks.toml` list the same three fixture paths; keep them in
  parity.
- **Private vulnerability reporting**: on (SECURITY.md relies on it).
- **Dependency graph**: on (`dependency-review` needs it).
- **Discussions**: on, then switch the questions link in
  `.github/ISSUE_TEMPLATE/config.yml` and [SUPPORT.md](../SUPPORT.md) from
  SUPPORT.md to Discussions.
- **Immutable releases**: on, so a published release's assets and tag cannot
  be changed afterwards; the release workflow's `publish` job refuses to
  continue when a release for the tag already exists, before anything is
  pushed, signed or published (the `build` job has already run by then).
- **GHCR package `loop-sessions-server`**: after the first push, make it
  public (packages inherit the repository's permissions but not its
  visibility) and verify with an anonymous `docker pull`.
- **Repository metadata**: homepage, description, topics, social preview
  (1280×640; no API for it), Wiki and Projects off unless used.

## Labels

`.github/release.yml` groups auto-generated release notes by label, so use
these: `enhancement` or `feature` (Features), `bug` or `fix` (Fixes),
`documentation` or `docs` (Documentation), `dependencies`, `ci` or `build`
(CI and tooling), and `ignore-for-release` to keep a change out of the
notes. Triage labels: `needs-triage` on every new issue, `question` for
things that belong in SUPPORT.md's channels, `good first issue` and
`help wanted` when the work is genuinely bounded. Area labels
(`area/client`, `area/server`, `area/installer`, `area/dashboard`,
`area/deploy-gcp`) mirror the component dropdown in the bug form. Aim to
answer a new issue within five business days.

## Releases

The procedure is [RELEASE.md, "Cutting a release"](RELEASE.md#cutting-a-release).
The parts that need a maintainer's judgement:

1. Move the `Unreleased` entries in [CHANGELOG.md](../CHANGELOG.md) under a
   `## [X.Y.Z] - YYYY-MM-DD` heading, in that exact shape:
   `.github/scripts/changelog-section.sh` extracts it for the release notes
   and falls back to GitHub's generated notes when it is missing.
2. Dry-run the workflow from the Actions tab (`release`, "Run workflow",
   `dry_run` ticked) on the commit you will tag; it builds, verifies,
   proves reproducibility and builds the image without publishing.
3. Tag `vX.Y.Z` on `main` and push the tag. The workflow creates the release
   as a draft, uploads every asset, pushes and attests the image, and
   undrafts it; a `-rc` suffix makes it a pre-release, which GitHub's
   `latest` alias and the image's `latest` tag both skip. For the first
   release, and after any change to the release path, cut `vX.Y.Z-rc.1`
   first and verify it as the checklist below says; a failed tag run is
   recovered as [RELEASE.md](RELEASE.md#cutting-a-release) step 4 describes.
4. Update the supported-versions table in [SECURITY.md](../SECURITY.md).

Public binaries are built with `ENDPOINT` empty; an organisation that stamps
its own endpoint publishes to its own channel host as RELEASE.md describes.

## Dependencies

Dependabot (`.github/dependabot.yml`) opens weekly grouped pull requests:
Go modules (minor and patch in one PR), GitHub Actions (one PR), and the
Dockerfile's base image (major and minor Go bumps ignored, because the Go
version is pinned by `go.mod`'s `toolchain` line and moves deliberately).
Every action is pinned to a full commit SHA with a `# vX.Y.Z` comment;
when Dependabot bumps the SHA, check that the version comment moved with it.

Policy: merge a green Dependabot PR within a week; merge a security bump
within two business days of the alert; a Go major or a Postgres driver bump
gets a human read of the changelog first. Regenerate
`THIRD_PARTY_NOTICES.md` with `make notices` (needs
`go install github.com/google/go-licenses/v2@latest`) whenever `go.mod`
changes; CI fails if the committed file is stale.

**Monthly: the Firebase SDK pin.** `server/web/static/signin.js` and
`cli.js` import the Firebase JS SDK from `www.gstatic.com` at a pinned
version, which no update tool tracks. Once a month, check the pinned
version against the current release, bump both files together, and test
sign-in against a Firebase project. (A test that fails when the pin is
older than N months is on the roadmap.)

## Screenshots

`docs/images/{sessions,session,fleet,analytics}.png` are taken from the
fixture-backed demo server by `server/web/screenshots.cjs` (`make
screenshots`), 1280×800, light theme, under 500 KB each. Regenerate them in
the same change that alters a template, `app.css` or the demo fixtures. The
script needs Node with `playwright` resolvable (`node_modules`, `NODE_PATH`
or `npm root -g`) and Playwright's cached Chromium or `CHROMIUM_PATH`; it
picks a free port, waits for the server, fails on any page that is not 200
or that contains a non-example address, and stops the server afterwards.
The fixture clock drifts, so two of the images change by a few hundred
bytes on every run; that is harmless.

## Checklists

**Before the first tag (and before undrafting the first pull request):**

- [ ] Rename the repository to `loopai-hq/loop-sessions` in Settings
      before merging the rename pull request: the tree, the module path,
      the badges and the image's `source` label already assume the new
      name, and GitHub redirects the old name to the new, never the other
      way. Re-run CI on the renamed repository, then merge.
- [ ] Create the `main` ruleset (the check names above, pull request
      required, no force-push, no linear-history requirement) and the `v*`
      tag ruleset, once the merge commit is in.
- [ ] Enable CodeQL default setup, secret scanning with push protection,
      private vulnerability reporting and the dependency graph.
- [ ] Enable Discussions and switch the questions links.
- [ ] Enable immutable releases.
- [ ] Route Dependabot, code-scanning, secret-scanning and workflow-failure
      alerts to a team address.
- [ ] Add the `IDENTIFIER_GATE_PATTERNS` secret and confirm the gate log
      says it is using the extended pattern on a maintainer-authored PR.
- [ ] Dry-run the release workflow.
- [ ] Cut `v0.1.0-rc.1` first. A hyphenated tag takes the prerelease path
      (no `latest` release, no `latest` image tag), so it exercises
      `publish` end to end, signing and GHCR included, without moving what
      installers resolve. From a laptop with no account, run the three
      verification commands in RELEASE.md against every asset and
      `gh attestation verify oci://ghcr.io/loopai-hq/loop-sessions-server@<digest>`
      against the image, and run both installer one-liners from README
      against the release. Only when all of that passes, tag `v0.1.0`.
- [ ] Set homepage, description, topics and the social preview.

**After a release:**

- [ ] Make the GHCR package public and verify with an anonymous
      `docker pull ghcr.io/loopai-hq/loop-sessions-server:vX.Y.Z`.
- [ ] Run the three verification commands from RELEASE.md against the
      published assets from a laptop with no account.
- [ ] Re-run the two installer one-liners from README against the real
      release.
- [ ] After v0.1.0, apply for the OpenSSF Best Practices badge (passing
      level) and add it to the README badge row.
- [ ] Update SECURITY.md's supported versions.

**Every month:**

- [ ] The Firebase SDK pin (above).
- [ ] The scheduled workflows are still enabled and green.
- [ ] Open Dependabot PRs are merged or closed with a reason.
