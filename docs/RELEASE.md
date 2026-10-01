# Releases: what is published, where it can live, and who reads it

A loop-sessions release is a directory of static files. Nothing about it
needs a particular host: the installer, the agent's self-upgrade and the
server all fetch the same handful of names from `<base>/<channel>/`, and
anything that answers HTTPS for those names is a release host. This page
describes the layout, how `make release` produces it, and exactly how each
consumer reads it, so that hosting it somewhere new is a matter of copying
files rather than reading Go. The public copy of every release lives on
[GitHub Releases](#github-releases), with provenance and a signature you can
[verify](#verifying-a-release); the bytes are
[reproducible](#reproducible-builds); and [cutting one](#cutting-a-release)
is a tag push.

The names are a contract between four places: the `Makefile` release
target, `install/install.sh`, `internal/upgrade` and `server/app/download.go`.
`internal/upgrade/upgrade_test.go`
(`TestTheAssetNameMatchesWhatTheReleaseTargetPublishes`) reads the Makefile
and the installer and fails when the binary name drifts between them, which
is the one drift that breaks upgrades silently while installs keep working.

## The files

`make release` writes everything into `dist/`:

| File | What it is |
|---|---|
| `loop-sessions_darwin_arm64` | the agent binary, one per platform |
| `loop-sessions_darwin_amd64` | |
| `loop-sessions_linux_amd64` | |
| `loop-sessions_linux_arm64` | |
| `SHA256SUMS` | `sha256sum` output for the four binaries, one `<hex>  <name>` per line |
| `latest.json` | the build's identity and the digest of every asset |

The platform list is `PLATFORMS` in the Makefile: `darwin/arm64 darwin/amd64
linux/amd64 linux/arm64`. Binaries are named `loop-sessions_<GOOS>_<GOARCH>`
with no extension and no version in the name; the version lives inside the
binary (`loop-sessions version` prints it) and in `latest.json`.

`SHA256SUMS` is written from inside `dist/` (`cd $(DIST) && sha256sum
loop-sessions_* > SHA256SUMS`), so the names it carries have no directory
part. Both readers tolerate the `*` binary-mode marker before a name and
compare by base name, but the file as published never has either.

`latest.json` is generated from `SHA256SUMS` rather than from the build loop,
so the two cannot disagree about a digest:

```json
{
  "version": "<VERSION>",
  "commit": "<full 40-character sha>",
  "build_date": "<RFC 3339, UTC>",
  "capture_schema": 4,
  "assets": {
    "loop-sessions_darwin_amd64": "<sha256 hex>",
    "loop-sessions_darwin_arm64": "<sha256 hex>",
    "loop-sessions_linux_amd64": "<sha256 hex>",
    "loop-sessions_linux_arm64": "<sha256 hex>"
  }
}
```

`commit` is the full sha on purpose: the server's fleet page compares it with
the VCS stamp each machine reports, and a 7-character prefix is enough for a
person and not for a machine. `capture_schema` is read from the constant
`CaptureSchema` in `internal/event/event.go` at build time, so the manifest
cannot claim an extraction version the code does not have.

## Building a release

```sh
make release ENDPOINT=https://sessions.example.com
make verify
```

What `make release` does, in order (`Makefile`, target `release`):

1. `make clean`, then refuses to run when `git status --porcelain` is not
   empty. `ALLOW_DIRTY=1` skips this and the stamp check below, for scratch
   builds only.
2. Reads `CAPTURE_SCHEMA` from `internal/event/event.go` and stops if it
   cannot.
3. Cross-compiles the four platforms with `CGO_ENABLED=0 go build -trimpath
   -ldflags '-s -w -X main.Version=$(VERSION) -X main.BuildDate=$(BUILD_DATE)
   -X main.defaultEndpoint=$(ENDPOINT)'`. Static, path-stripped and
   symbol-stripped, with `BUILD_DATE` taken from the commit rather than the
   clock, so two people building the same commit with the same toolchain get
   the same bytes and no home directory leaks into the binary (see
   [Reproducible builds](#reproducible-builds)).
4. Runs `go version -m` on the linux/amd64 binary and refuses to write a
   manifest when its `vcs.revision` is not `COMMIT` or it carries
   `vcs.modified=true`. A build from a linked git worktree is stamped with
   the primary checkout's HEAD, which is the case this catches: the fleet
   page could never match such a binary to `latest.json`.
5. Writes `SHA256SUMS`, then `latest.json` (`make manifest`), then prints
   both and the stamp the fleet will see.

The variables that shape a build:

| Variable | Default | Meaning |
|---|---|---|
| `VERSION` | `git describe --tags --always --dirty` | stamped into the binary and `latest.json` |
| `COMMIT` | `git rev-parse HEAD` | the full sha in `latest.json` |
| `BUILD_DATE` | the commit time of `HEAD` in UTC (`TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd`); `SOURCE_DATE_EPOCH` when set; the wall clock only without a `.git`; read once and exported | stamped into the binary and `latest.json`; the `BuildDate` the health report carries when the toolchain has no `vcs.time` (a `git archive` build) |
| `ENDPOINT` | see the Makefile | the server URL stamped into the agent as `defaultEndpoint`; `loop-sessions install --endpoint` overrides it per machine |
| `ALLOW_DIRTY` | unset | skip the clean-tree and stamp checks |

`ENDPOINT` is the one that makes a release yours. The agent talks to exactly
one server, and a release built for one deployment points at it by default;
build with `ENDPOINT=https://<your server>` or tell every machine to pass
`--endpoint` at install.

`make verify` re-checks `dist/` against its own output: `sha256sum -c
SHA256SUMS` (or `shasum -a 256 -c`), then, when `python3` is on the PATH, an
assertion that `latest.json` carries `version`, `commit` and a non-empty
`assets` map.

`make release` does not copy the installer into `dist/`. If the published
one-liner should work, publish `install/install.sh` beside the channel's files
as well (see below). `install/uninstall.sh` may be published at the base too,
for people without the binary; the installer itself points at
`loop-sessions uninstall`, which needs no download.

## Hosting: the layout

```
<base>/
  latest/
    SHA256SUMS
    latest.json
    loop-sessions_darwin_arm64
    loop-sessions_darwin_amd64
    loop-sessions_linux_amd64
    loop-sessions_linux_arm64
    install.sh            # optional; what the server's /install.sh serves
  canary/
    (the same seven names)
  uninstall.sh            # optional; for people who no longer have the binary
```

`<base>` is any HTTPS URL. A directory on a static web host, an object
bucket behind a CDN, or the loop-sessions server's own `/dl/` proxy all
satisfy it; the installer requires only that `GET <base>/<channel>/<name>`
answers with the bytes. Plain `http://` is accepted only for `127.0.0.1`,
`localhost` and `[::1]`, which exists so the installer can be tested against
a local release host (`install/install.sh`, `is_loopback`).

Two channels exist, `latest` and `canary`. The server's download proxy
answers 404 for any other name (`server/app/download.go`,
`downloadableChannels`), and the agent's config rejects any other value for
`channel` (`internal/config/config.go`, `Validate`). `latest` is what every
installed agent follows and what the installer fetches by default; `canary`
is for the machine or two that take a build first. A host that publishes
only `latest/` serves every machine on the default channel.

Two rules for publishing into a channel:

- **Binaries first, manifests last.** Upload the four `loop-sessions_*`
  files, then `SHA256SUMS` and `latest.json`. A reader fetches the manifest
  and then the binary it names; a manifest published ahead of its binaries
  sends that reader to a 404 or, worse, to yesterday's bytes under today's
  digest, which the installer reports as a checksum mismatch.
- **Never cacheable.** Serve the channel with `Cache-Control: no-store` (the
  server proxy sets it on every response; the GCP example uploads with the
  same header). A `latest/` that a proxy holds for a day is a fleet installing
  yesterday's agent with no way to tell from the server.

A pattern the GCP example in `examples/deploy-gcp/` follows and any host can
copy: keep an immutable `builds/<sha>/` copy of every release beside the
channels, publish each merge to `canary/`, and promote to `latest/` by
copying a `builds/<sha>/` directory across (binaries first). Rollback is then
promoting an older directory, and the self-upgrade treats it exactly like a
forward move, because it compares bytes rather than version numbers.

## How `install.sh` consumes it

`install/install.sh` is POSIX `sh` with no dependency beyond `curl` or
`wget`, `awk`, `sed` and `shasum` or `sha256sum`. It reads four environment
variables. `LOOP_SESSIONS_BASE_URL` has no default: the script stops with a
message when it is unset, so a copy of it cannot quietly download from a host
nobody chose.

| Variable | Default | Purpose |
|---|---|---|
| `LOOP_SESSIONS_BASE_URL` | none; required (the script stops with a message when it is unset) | `<base>` above |
| `LOOP_SESSIONS_VERSION` | `latest` | the channel (`latest` or `canary`); the script uses it as a path segment and validates nothing about it, which is what lets a GitHub tag or `download` stand in for a channel (see [GitHub Releases](#github-releases)) |
| `LOOP_SESSIONS_BIN_DIR` | `~/.local/bin` | where the binary lands |
| `LOOP_SESSIONS_NO_ENROLL` | unset | `1` skips the sign-in hand-off |
| `LOOP_SESSIONS_ENDPOINT` | unset | not read by the script; inherited by the `loop-sessions install` it hands off to, which needs it (or `--endpoint`) when the binary carries no stamped endpoint, as a GitHub release binary does not |

and then, in order (`download_and_verify`, `install_binary`):

1. `GET <base>/<channel>/SHA256SUMS`. A failure here is "no such channel or
   no network" and stops everything.
2. Finds the line for `loop-sessions_<os>_<arch>` in it. No line means the
   release has no build for this platform, and the script says which.
3. `GET <base>/<channel>/loop-sessions_<os>_<arch>` into a temporary
   directory; an empty file is treated as a truncated transfer.
4. Computes the SHA-256 and compares it with the manifest's. On a mismatch
   the download is deleted, nothing is installed, and the exit is non-zero.
5. `GET <base>/<channel>/latest.json`, optionally. Failure is ignored; when
   it is there, `version` and `commit` are read with `sed` and printed after
   the install, so the line a person pastes into a bug report names a commit.
6. Stages the binary inside the target directory, `chmod 0755`, runs it once
   (`loop-sessions version`) and only then renames it over the old one. A
   build that passes its checksum and still does not start leaves the
   previous install untouched.

Every non-loopback fetch is HTTPS with `--proto '=https' --tlsv1.2` (curl) or
`--https-only` (wget), so a redirect cannot downgrade the connection.

The script ends by handing off to `loop-sessions install` for sign-in, which
is where the server address comes in: the binary uses its stamped
`defaultEndpoint` unless `--endpoint` is given. The release host and the
server are independent; only the binary's own stamp ties them.

To verify a download by hand instead, fetch `SHA256SUMS` and the binary for
your platform from `<base>/latest/` and run `shasum -a 256 -c SHA256SUMS
--ignore-missing` (macOS) or `sha256sum -c SHA256SUMS --ignore-missing`
(Linux). Because the build is [reproducible](#reproducible-builds), `make
release` at the same commit with the same toolchain gives the same digest,
and a GitHub release carries provenance and a signature over that file as
well ([Verifying a release](#verifying-a-release)).

## How the in-app upgrade consumes it

The agent replaces its own binary from the daemon (`internal/upgrade`,
called from `cmd/loop-sessions/main.go`). What it reads, and from where:

- The base is **the server endpoint**, not the installer's
  `LOOP_SESSIONS_BASE_URL`: `upgrade.Run` is given `BaseURL: cfg.Endpoint`
  and reads `<endpoint>/dl/<channel>/SHA256SUMS`, then
  `<endpoint>/dl/<channel>/loop-sessions_<os>_<arch>`. It never reads
  `latest.json`. So auto-upgrade works only when the server itself answers
  `/dl/` (below); the installer works against any base.
- Staleness is a digest, not a version. The daemon hashes its own executable
  and compares it with the manifest's entry for its platform. Different
  bytes mean stale, whichever direction the version moved, so a rollback
  published to `latest/` propagates like any other release.
- The download is written to a temporary file in the same directory,
  `fsync`ed, checked against the digest, made executable, run once
  (`version`), and only then renamed over the running binary. Every failure
  leaves the old binary in place and is logged; none is fatal to capture.

The daemon checks at most once every six hours per machine
(`upgradeRecheck`), with a ten-minute budget per attempt (`upgradeBudget`).
`loop-sessions daemon --upgrade-now` runs one check immediately. The check
is skipped, and says why in the log and the health report, when:

- the binary is a development build (`Version` is `dev` or empty);
- `LOOP_SESSIONS_NO_UPGRADE` is set in the environment;
- `disable_auto_upgrade` is `true` in `~/.loop/sessions/config.json`;
- capture is paused (`--upgrade-now` only);
- the binary is outside the home directory, or its directory is not
  writable, on the reasoning that whatever installed it there owns it;
- the manifest lists no asset for this OS/architecture.

The channel comes from `channel` in the config (`latest` when unset).

## How the server serves and uses it

The server has two relationships with a release: it can serve one, and it
judges the fleet against one. Both are switched on by the `RELEASE_BUCKET`
environment variable and both currently assume Google Cloud Storage.

**Serving** (`server/app/download.go`). With `RELEASE_BUCKET` set, three
unauthenticated routes are mounted:

| Route | Serves |
|---|---|
| `GET /dl/{channel}/{asset}` | the object `<channel>/<asset>` from the bucket |
| `GET /install.sh` | the object `latest/install.sh` |
| `GET /install` | an HTML page with the one-liner `curl -fsSL <PUBLIC_URL>/install.sh \| sh`, a `/dl/latest/<binary>` link per platform and one to `/dl/latest/SHA256SUMS` |

`asset` must be one of the seven names in `downloadableAssets`
(`SHA256SUMS`, `install.sh`, `latest.json` and the four binaries) and
`channel` one of `latest` or `canary`; anything else is a plain 404, so the
route cannot be used to reach an arbitrary object. Objects are read with a
token from the instance metadata server, which is why this works on Cloud
Run or GCE and nowhere else, and every response carries `Cache-Control:
no-store`. With `RELEASE_BUCKET` unset the routes are not mounted at all, and
a deployment that distributes the agent another way answers 404 for `/dl/`.
The consequence for that deployment is the one stated above: installs work
from any base, and every laptop's auto-upgrade logs an error each check and
stays on its build.

**Judging** (`server/app/app.go`, `newManifestSource`; `server/fleet`). The
fleet page reads `latest/latest.json` from the same bucket, with the same
credential, decodes `version`, `commit`, `build_date` and `capture_schema`,
and compares the `commit` with the VCS stamp every machine sends in its
health report. That is how a machine is shown as behind, and how far. The
manifest is cached for five minutes. Without a bucket the evaluator reports
the manifest as missing once and judges nothing about versions.

## GitHub Releases

Every tag `vX.Y.Z` is published as a GitHub Release by
`.github/workflows/release.yml`, and that is where the public downloads the
agent. A release carries these assets:

| Asset | What it is |
|---|---|
| `loop-sessions_darwin_arm64`, `loop-sessions_darwin_amd64`, `loop-sessions_linux_amd64`, `loop-sessions_linux_arm64` | the agent, exactly as `make release` names them |
| `SHA256SUMS` | the digests of the four binaries |
| `SHA256SUMS.sigstore.json` | a keyless cosign signature over `SHA256SUMS` |
| `latest.json` | the build's identity, as above |
| `install.sh`, `install.sh.sha256` | the installer and its digest |
| `uninstall.sh`, `uninstall.sh.sha256` | the uninstaller and its digest |
| `THIRD_PARTY_NOTICES.md` | licence notices for what the server links (the agent is standard library only) |

The binaries on GitHub are built with `ENDPOINT` empty: a public download
carries no organisation's server address, so `loop-sessions install` asks for
`--endpoint` (or `LOOP_SESSIONS_ENDPOINT`). An organisation that stamps its
own builds into its channel keeps doing that; the two builds of one tag
differ only in that stamp.

The server image for the same tag is `ghcr.io/loopai-hq/loop-sessions-server`,
tagged `vX.Y.Z`, `sha-<full commit>` and, for a non-prerelease, `latest`.
It is linux/amd64, labelled with `org.opencontainers.image.{source,version,
revision,created}`, ships `/THIRD_PARTY_NOTICES.md` beside `/server`, and
its digest is attested like the binaries.

GitHub serves release assets at two permanent URL shapes:

```
https://github.com/loopai-hq/loop-sessions/releases/download/vX.Y.Z/<asset>
https://github.com/loopai-hq/loop-sessions/releases/latest/download/<asset>
```

Both are `<something>/<segment>/<asset>`, and `install.sh` builds its URLs as
`<LOOP_SESSIONS_BASE_URL>/<LOOP_SESSIONS_VERSION>/<asset>` without checking
what the version segment is. So the installer works from GitHub Releases
today, with no code that knows about GitHub, in either of two forms:

```sh
# a specific release
curl -fsSL https://github.com/loopai-hq/loop-sessions/releases/download/vX.Y.Z/install.sh |
  LOOP_SESSIONS_BASE_URL=https://github.com/loopai-hq/loop-sessions/releases/download \
  LOOP_SESSIONS_VERSION=vX.Y.Z sh

# whatever GitHub marks as the latest release
curl -fsSL https://github.com/loopai-hq/loop-sessions/releases/latest/download/install.sh |
  LOOP_SESSIONS_BASE_URL=https://github.com/loopai-hq/loop-sessions/releases/latest \
  LOOP_SESSIONS_VERSION=download sh
```

The second form leans on GitHub's `latest` alias, which excludes
pre-releases, so it never installs an `-rc`. Both forms fetch `SHA256SUMS`
first and verify the binary against it before anything is made executable,
as the installer always does. (Both URL shapes were exercised end to end
against a local static server laid out exactly like the two paths above
before this was written down.) To read the script before running it, fetch
`install.sh` and `install.sh.sha256` from the same release and check one
against the other.

What GitHub Releases does not give you, and why:

- **The agent's self-upgrade.** The daemon never reads
  `LOOP_SESSIONS_BASE_URL`; it reads `<endpoint>/dl/<channel>/` from the
  server it enrolled with (previous section). The server's `/dl/` proxy is
  bound to a bucket (`RELEASE_BUCKET`) and cannot front GitHub today, so a
  deployment without a bucket installs fine from GitHub and then logs an
  upgrade error per check, as described above. The fix on the roadmap is a
  server-side redirector: a `/dl/` backend selected by a new environment
  variable that answers `latest/<asset>` with a redirect to
  `releases/latest/download/<asset>` and `canary/<asset>` with a redirect to
  the newest pre-release's asset, resolved through the Releases API and
  cached for five minutes like the manifest. Both clients already follow
  HTTPS redirects, so no client change is involved.
- **A `canary` channel.** GitHub has a `latest` alias and no `canary` one;
  until the redirector exists, canary is a bucket-only concept.
- **The fleet page's version column**, which reads `latest/latest.json` from
  the bucket for the same reason.

## Verifying a release

Three checks, from cheapest to strongest. All of them work on a laptop with
no account; the second and third need the `gh` or `cosign` binaries.

**1. The digest.** Download `SHA256SUMS` and the binary for your platform
into one directory and check the one against the other:

```sh
sha256sum -c SHA256SUMS --ignore-missing      # Linux
shasum -a 256 -c SHA256SUMS --ignore-missing  # macOS
```

`--ignore-missing` makes the tool report only the files you downloaded
rather than complaining about the three platforms you did not. This is what
`install.sh` does for you.

**2. Build provenance.** Every binary named in `SHA256SUMS`, both scripts and
the image digest are attested by the release workflow with `actions/attest`:
a signed statement, stored by GitHub, that this exact digest was produced by
`release.yml` in this repository from this tag. Verify a downloaded file with

```sh
gh attestation verify loop-sessions_linux_amd64 --repo loopai-hq/loop-sessions
```

`gh` computes the file's digest, fetches the attestations GitHub holds for
it, and checks the Sigstore signature and that the signing workflow belongs
to the repository named by `--repo`. To pin it to the workflow file as well,
add `--signer-workflow loopai-hq/loop-sessions/.github/workflows/release.yml`.
The image is verified the same way with `oci://ghcr.io/loopai-hq/loop-sessions-server:vX.Y.Z`
in place of the file.

**3. The cosign signature on `SHA256SUMS`.** The workflow also signs the
manifest with `cosign sign-blob --bundle`, keyless, using the workflow's own
OIDC identity. The bundle is the `SHA256SUMS.sigstore.json` asset:

```sh
cosign verify-blob \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-identity https://github.com/loopai-hq/loop-sessions/.github/workflows/release.yml@refs/tags/vX.Y.Z \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
```

The identity is the workflow file at the tag, and the issuer is GitHub
Actions' token service (not the interactive `github.com/login/oauth`
issuer that some examples show, which is what a person signing from a
laptop would get). A valid signature plus check 1 means the binary you hold
is the one the workflow hashed. The workflow runs both `gh attestation
verify` and `cosign verify-blob` itself, before it creates the release, so
a release that exists is one these commands passed on.

## Reproducible builds

Two builds of one commit produce byte-identical binaries and an identical
`SHA256SUMS`, provided the toolchain is the same: a clean clone at the tag,
built with the `toolchain go1.26.8` line in `go.mod` (the go command
downloads that exact release when the local one differs). What makes it
true:

- `CGO_ENABLED=0` removes the host C toolchain from the inputs and `-trimpath`
  removes the working directory; the Go toolchain is otherwise deterministic.
- `BUILD_DATE`, the one input that used to vary, defaults to the commit time
  of `HEAD` in UTC. The Makefile's rule, in order: an explicit `BUILD_DATE`
  on the command line or in the environment; else `SOURCE_DATE_EPOCH`
  (converted with `date -u -d @N` on GNU or `date -u -r N` on BSD, tried in
  that order); else the committer time when the tree has a `.git`; else,
  for a `git archive` export that has no commit to name, the wall clock.
  `latest.json`'s `build_date` and the binary's `BuildDate` therefore say
  when the code was finished, not when someone ran `make`.
- The VCS stamp the toolchain embeds (`vcs.revision`, `vcs.time`,
  `vcs.modified`) is a property of the commit as well, which is why `make
  release` refuses a dirty tree and a linked worktree: both would stamp
  something a rebuild cannot recreate.

CI proves it rather than asserting it: the `build` job of `ci.yml` and the
`build` job of `release.yml` run `make release` twice from a fresh clone,
copying `SHA256SUMS` out between the runs (because `make release` cleans
`dist/`), and `diff` the two; they also check that `build_date` equals the
commit time. The `publish` job of `release.yml` builds once, from the same
commit, and runs only after `build` has passed (`needs: build`), so nothing
is published that was not first shown to rebuild identically. To reproduce
a published release yourself, clone the repository at the tag and build:

```sh
git clone --branch vX.Y.Z https://github.com/loopai-hq/loop-sessions.git
cd loop-sessions
make release ENDPOINT=
diff dist/SHA256SUMS <(curl -fsSL https://github.com/loopai-hq/loop-sessions/releases/download/vX.Y.Z/SHA256SUMS)
```

An empty diff means your machine produced the same four binaries GitHub is
serving. The self-upgrade design depends on this too: it compares bytes, so
a rebuild of the same commit must not look like a new release.

## Cutting a release

A release is a tag. The workflow does everything else, and a dry run of it
is available at any time.

1. **Update `CHANGELOG.md`.** Move the `Unreleased` entries under a new
   `## [X.Y.Z] - YYYY-MM-DD` heading (Keep a Changelog form). The workflow
   uses that section as the release notes
   (`.github/scripts/changelog-section.sh`); when it finds none it falls
   back to GitHub's generated notes, categorised by `.github/release.yml`.
   Note any `CaptureSchema` or migration change in an upgrade paragraph.
2. **Dry-run the release path.** Every pull request that touches
   `release.yml`, the `Makefile`, the `Dockerfile`, `install/` or
   `internal/upgrade/` already runs the `build` job of `release.yml`:
   `make release` twice with a diff, `make verify`, govulncheck on the
   linux/amd64 binary, and the image build. A maintainer can also start the
   workflow by hand (Actions, `release`, "Run workflow") with `dry_run`
   ticked, on any branch or tag; nothing is published.
3. **Tag and push.** On the merged commit:

   ```sh
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

   A tag containing a hyphen (`v0.2.0-rc.1`) is published as a pre-release:
   it does not become GitHub's `latest`, and the image does not get the
   `latest` tag.
4. **What the workflow does on the tag**, in order, in `.github/workflows/release.yml`:
   the `build` job as in step 2; then `publish`, which builds again from the
   tag (`VERSION` is the tag by name, not `git describe`), checks the
   binary's VCS stamp and `latest.json` name this commit, writes
   `install.sh.sha256` and `uninstall.sh.sha256`, attests the four
   binaries (`subject-checksums: dist/SHA256SUMS`) and the two scripts,
   signs `SHA256SUMS` with cosign, verifies both, creates the GitHub
   Release as a **draft** and uploads every asset, then builds and pushes
   the image to GHCR, attests its digest and verifies that attestation
   (`gh attestation verify oci://…@sha256:…`), and only then flips the
   release to published (`gh release edit --draft=false`). The
   draft-then-publish order is the binaries-first rule in GitHub's terms:
   nobody can observe a release whose assets are still uploading, and the
   image is pushed only once the draft holds every asset. The job's first
   step refuses to run if a release for the tag already exists, before
   anything is built, pushed or signed. Both jobs run only in this
   repository's owner's copy (`if: github.repository_owner == 'loopai-hq'`);
   in a fork the workflow is skipped entirely, dry run included.

   **If a tag run fails before the last step**, it leaves a draft release
   and, if the image step had run, image tags in GHCR, but nothing
   published. Delete the draft (the Releases page, or
   `gh release delete vX.Y.Z --yes`; the tag stays), fix the cause, and
   re-run the workflow from the Actions tab. The first step refuses to
   start while the draft exists, because `gh release view` finds drafts
   too, so the delete is not optional. Immutable releases bind a release's
   assets and tag only once it is undrafted, which is why a draft can be
   deleted and remade. A re-run rebuilds the image, and its digest may
   differ (layer mtimes come from the checkout); the image tags are
   re-pointed to the new digest, and the earlier attestation names an
   image nothing refers to.
5. **Repository settings this relies on** (one-time, in Settings):
   *immutable releases* enabled, so that once a release is published its
   assets and its tag cannot be changed, which is exactly why the workflow
   attaches everything before publishing; and the GHCR package
   `loop-sessions-server` set to public after the first push (a package
   linked to the repository through the `org.opencontainers.image.source`
   label, which the Dockerfile sets, inherits the repository's access
   permissions but not its visibility; a public repository does not make
   the package public by itself. Make it public in the package settings and
   check with an anonymous `docker pull`).
6. **Afterwards.** An organisation that runs its own channel copies the
   release's assets into `builds/<sha>/` and `canary/` (or builds its own
   stamped binaries with `ENDPOINT` set), and promotes to `latest/` as its
   own process dictates; the GitHub release and the bucket then serve the
   same commit.

## Hosting somewhere else

Everything above reduces to one URL shape:

```
<base>/<channel>/SHA256SUMS
<base>/<channel>/latest.json
<base>/<channel>/loop-sessions_<os>_<arch>
```

Any static host that serves that shape over HTTPS is a complete release host
for the installer, with the two publishing rules (binaries first; no caching)
observed. GitHub Releases satisfies it for the installer, as shown above.
What a plain static host does not give you is the server's `/dl/` proxy, and
therefore not auto-upgrade or the fleet page's version column; those are
bound to `RELEASE_BUCKET` and the GCS API today, and the redirector described
under GitHub Releases is the planned way to point them at an HTTPS base. If
you take that on, keep the asset names and the two manifests exactly as they
are: every consumer, and the drift test, depends on them.
