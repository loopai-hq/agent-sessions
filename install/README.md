# Installing loop-sessions

This page is written for someone deciding whether to trust this, not for
someone who already has. It describes exactly what the installer does, what
leaves your machine, who can read it, and how to remove it.

## Where the binary comes from

**From GitHub Releases**, which is where the public downloads it:

```sh
curl -fsSL https://github.com/loopai-hq/loop-sessions/releases/latest/download/install.sh |
  LOOP_SESSIONS_BASE_URL=https://github.com/loopai-hq/loop-sessions/releases/latest \
  LOOP_SESSIONS_VERSION=download sh
loop-sessions install --endpoint https://sessions.example.com
```

A release's binaries carry no server address, so `install` needs
`--endpoint` (or `LOOP_SESSIONS_ENDPOINT`): the base URL of the server your
organisation runs. The pinned-version form, and how a release is verified
(checksums, build provenance, a signature), are in
[docs/RELEASE.md](../docs/RELEASE.md#github-releases).

**From your organisation's server**, when it is set up as a release host
(`RELEASE_BUCKET`), in which case the binary is stamped with the server's
address and `--endpoint` is not needed:

```sh
curl -fsSL https://sessions.example.com/install.sh | LOOP_SESSIONS_BASE_URL=https://sessions.example.com/dl sh
```

Either way `LOOP_SESSIONS_BASE_URL` is required: it names where the release
files live (`<base>/<version>/SHA256SUMS`, `<base>/<version>/latest.json`
and `<base>/<version>/loop-sessions_<os>_<arch>`, the layout `make release`
produces), and the script refuses to download anything until it is set.

You can read the script before running it. It is about 400 lines of POSIX
shell with no dependencies beyond `curl` or `wget`, `awk`, `sed`, and
`shasum` or `sha256sum`. Fetch it and its digest from the same release,
check one against the other, and read it:

```sh
base=https://github.com/loopai-hq/loop-sessions/releases/latest/download
curl -fsSLO "$base/install.sh" && curl -fsSLO "$base/install.sh.sha256"
sha256sum -c install.sh.sha256      # shasum -a 256 -c install.sh.sha256 on macOS
less install.sh
```

## Supported platforms

macOS (Apple Silicon and Intel) and 64-bit Linux (Intel and ARM). Windows
is not supported: the agent relies on POSIX process control (signals and a
detached process group: `syscall.Kill`, `Setsid`), the client does not
compile for it, and the installer stops with a message rather than
downloading something that will not run.

## Why a shell pipe rather than a downloadable installer

This is the counter-intuitive part, and it is a measured fact rather than a
preference.

A file fetched with `curl` carries no `com.apple.quarantine` attribute, so
Gatekeeper never engages and no Apple Developer certificate is involved. A
file downloaded through a **browser** does get quarantined, and macOS Sequoia
removed the Control-click override, so an unsigned browser-downloaded
installer now dead-ends in System Settings with no way for a non-technical
person to continue.

So the shell pipe is both the cheaper lane and the one that actually works.
Claude Code itself ships exactly this way, so most people installing this
have already performed this motion at least once. The read-then-run form
above is there for anyone who would rather not pipe.

## What the installer does, in order

1. **Checks your machine.** Reads `uname` to decide which build you need. If
   it is neither macOS nor Linux, it stops and says so.
2. **Checks its own tools.** Confirms `curl` (or `wget`) and `shasum` (or
   `sha256sum`) are present. If it cannot verify a download, it refuses to
   install at all rather than continuing unverified.
3. **Prints the plan.** Every path it will touch, before it touches any of
   them.
4. **Downloads the checksum manifest** from `<base>/<version>/SHA256SUMS`.
5. **Downloads the binary** for your platform.
6. **Verifies the SHA-256** against the manifest. On a mismatch the download
   is deleted, nothing is installed, and the script exits non-zero telling
   you not to run the file.
7. **Installs to `~/.local/bin/loop-sessions`.** No `sudo`, ever. The file
   is staged inside the same directory and moved into place with an atomic
   rename, so an interrupted install cannot leave a half-written executable
   on your PATH.
8. **Runs the new binary once** (`loop-sessions version`) before it replaces
   any existing copy, so a broken build cannot take out a working install.
9. **Checks your PATH** and, if `~/.local/bin` is missing from it, prints
   the exact line to add and the exact file to add it to, for the shell you
   actually use.
10. **Hands off to `loop-sessions install`** for sign-in. The installer does
    no authentication itself.

When the script is piped to `sh`, step 10 is skipped and the command is
printed instead. Sign-in is interactive, and a piped script has no terminal
to ask you anything with.

## What `loop-sessions install` does

It shows what it found on the machine (which harnesses, how many sessions),
prints who will be able to read your sessions (below), opens the browser to
sign in with your Google account, registers the capture hooks in
`~/.claude/settings.json` (tagged `#loop-sessions` so they can be removed
exactly), and imports the history already on disk (`--backfill-since 30d`
to limit it, `--skip-backfill` to skip it; `--skip-hooks`, `--skip-signin`).
The sign-in token is spent once against the server and never written to
disk; the only credential kept is a device token minted for this machine.

## What gets installed, and where

| Path | What it is |
|---|---|
| `~/.local/bin/loop-sessions` | the agent binary, the only thing installed |
| `~/.loop/sessions/config.json` | your settings, written at sign-in, including what you chose to exclude |
| `~/.loop/sessions/device.token` | the device token, the one credential the agent keeps; mode `0600` |
| `~/.loop/sessions/discovery.json` | what `discover` found: where each harness keeps its sessions; mode `0600` |
| `~/.loop/sessions/spool/` | captured events waiting to upload |
| `~/.loop/sessions/state/`, `seq/` | bookkeeping |
| `~/.loop/sessions/logs/agent.log` | the agent's own log |
| `~/.claude/settings.json` | gains hook entries marked `#loop-sessions` |

Nothing is installed outside your home directory. Nothing runs as root.
Nothing is added to a system launch path without your sign-in step.
`LOOP_SESSIONS_HOME` moves the state directory somewhere else.

## Verifying the download by hand

If you would rather not trust the script's own check, do it yourself:

```sh
# 1. fetch the manifest and the binary for your platform
base=https://github.com/loopai-hq/loop-sessions/releases/latest/download
curl -fsSLO "$base/SHA256SUMS"
curl -fsSLO "$base/loop-sessions_darwin_arm64"

# 2. verify
shasum -a 256 -c SHA256SUMS --ignore-missing     # macOS
sha256sum -c SHA256SUMS --ignore-missing         # Linux

# 3. install it yourself
chmod +x loop-sessions_darwin_arm64
mv loop-sessions_darwin_arm64 ~/.local/bin/loop-sessions
```

Replace `darwin_arm64` with `darwin_amd64` (Intel Mac), `linux_amd64` or
`linux_arm64` as appropriate; from an organisation's host, replace the base
with `https://sessions.example.com/dl/latest`.

The published binaries are reproducible: a clean clone at the tag, built
with the toolchain named in `go.mod` (`toolchain go1.26.8`), produces the
same bytes and the same `SHA256SUMS`, and CI proves it on every commit by
building twice. You can rebuild and compare:

```sh
git clone --branch vX.Y.Z https://github.com/loopai-hq/loop-sessions && cd loop-sessions
make release ENDPOINT=
shasum -a 256 dist/loop-sessions_darwin_arm64
```

`docs/RELEASE.md` also describes the build provenance (`gh attestation
verify`) and the cosign signature over `SHA256SUMS`.

## What leaves your machine

Being direct about this is the point of the page, so: **the content of your
coding sessions leaves your machine.** That is what the tool is for.
Concretely, each captured event can include

- your prompts and the assistant's replies,
- the tools that ran, their inputs, and their output,
- file paths, the working directory, the git branch, and diffs of files
  changed,
- token counts, model names, and the harness version.

Before anything is written to the spool (not at upload time, but before it
touches your disk) every text field is passed through a credential scrubber
covering twenty kinds: API keys, tokens, private keys, database connection
strings and `Authorization` headers. Each hit becomes `[REDACTED:<kind>]`,
and a redaction count travels with the event so a machine whose scrubber is
firing constantly is visible rather than quietly shipping near-misses.

Separately, a periodic health report leaves the machine containing your
hostname, OS and architecture, the agent's version, uptime, queue and disk
statistics, which harnesses were found, and whether capture is paused. It
deliberately contains **no** session identifiers, project paths or prompt
text.

Everything goes to the one server you enrolled with and nowhere else; the
agent has no other network destination. What never leaves: files the agent
was not shown, anything in a project you excluded (`exclude_paths` in
`config.json`, or only the projects in `capture_only_paths`), harnesses you
chose to skip, and anything captured while the agent is paused.

## Who can read it

`install` prints this before you sign in, and `status` repeats it:

> Your captured sessions can be read by you and by the server's admins, who
> also see the first prompt and metadata of every session in the list;
> opening someone else's full session is recorded in the access log the
> admins can see. A colleague can read one of your sessions only through a
> share link that you or an admin create. How long sessions are kept, and
> whether anything is exported, is decided by the server operator.

Colleagues cannot browse your sessions: a member of the server sees their
own work and what has been shared with them, and nothing else. Admins see
every session in the list, first prompt included, and that list is not
audited; opening one is. What the operator holds, and the controls they
have, are in [docs/DATA-PROTECTION.md](../docs/DATA-PROTECTION.md).

You can see the current state at any time:

```sh
loop-sessions status          # what is being captured, and whether upload is working
loop-sessions doctor          # diagnose problems and how to fix them
loop-sessions pause --for 2h  # stop capturing for two hours, then resume on its own
loop-sessions pause           # stop capturing until you run resume
```

## Upgrading

Re-run the installer. It replaces the binary and leaves your configuration,
your spool and your registered hooks alone, and reports the version it
replaced and the version it installed. Machines enrolled with a server that
is a release host also upgrade themselves: the agent compares its own
digest with the published one and replaces itself when they differ;
`LOOP_SESSIONS_NO_UPGRADE=1` or `disable_auto_upgrade` in `config.json`
turns that off.

## Uninstalling

```sh
loop-sessions uninstall
```

`install/uninstall.sh` does the same without the binary. It is a GitHub
Release asset (`uninstall.sh`, with `uninstall.sh.sha256`) and is in a
checkout of this repository; a server's `/dl` proxy does not serve it.

It prints every path it deletes. By default it removes the binary and any
background service, and **keeps** your captured data and configuration,
printing the one command that removes those too.

| Flag | Effect |
|---|---|
| `--dry-run` | show exactly what would be removed, change nothing |
| `--purge` | also delete `~/.loop/sessions` |
| `--force` | remove the binary even if harness hooks still reference it |

One safeguard worth knowing about: if hook entries are still registered in
`~/.claude/settings.json`, the uninstaller **refuses to delete the binary**
and tells you how to remove the hooks first. Deleting the binary while a
hook still points at it would make the harness run a missing command on
every event, which would turn removing a telemetry tool into a broken
editor. `--force` overrides this if you want to clean up by hand.

The uninstaller never edits `settings.json` itself. A settings file left
unparseable could stop the harness from starting, and that is a worse
outcome than a hook entry that lingers for a minute.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `LOOP_SESSIONS_BASE_URL` | none, **required** | where releases are fetched from: `https://github.com/loopai-hq/loop-sessions/releases/latest` or `https://sessions.example.com/dl` |
| `LOOP_SESSIONS_VERSION` | `latest` | the path segment under the base: a channel (`latest`, `canary`) on an organisation's host, `download` under GitHub's `releases/latest`, or a tag under `releases/download`; the script validates nothing about it |
| `LOOP_SESSIONS_BIN_DIR` | `~/.local/bin` | install somewhere else |
| `LOOP_SESSIONS_NO_ENROLL` | unset | set to `1` to skip sign-in |

Downloads are HTTPS-only, including across redirects, so a server cannot
downgrade the connection partway through. Plain HTTP is permitted only for
`127.0.0.1`, `localhost` and `[::1]`, which exists so the installer can be
tested against a local release host.

## If something goes wrong

Every failure exits non-zero and prints what happened and what to do about
it. The cases the script handles explicitly:

| What happened | What it does |
|---|---|
| unsupported OS or CPU | names what it detected, downloads nothing |
| no `curl`/`wget` | tells you what to install |
| no SHA-256 tool | refuses to install rather than skipping verification |
| manifest or binary 404 | prints the URL it tried |
| your platform missing from the release | says which platform is missing |
| download truncated or empty | stops before installing |
| checksum mismatch | deletes the file, tells you not to run it |
| `~/.local/bin` not writable | explains, and never escalates to `sudo` |
| the new binary will not run | keeps your existing install untouched |
| sign-in fails | tells you the binary is installed and how to retry |

If you are stuck, `loop-sessions doctor` explains the current state in plain
language, and whoever operates your server can help from there
([SUPPORT.md](../SUPPORT.md)).
