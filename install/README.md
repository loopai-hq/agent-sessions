# Installing loop-sessions

This page is written for someone deciding whether to trust this, not for someone
who already has. It describes exactly what the installer does, what leaves your
machine, and how to remove it.

```sh
curl -fsSL https://sessions.example.com/install.sh | LOOP_SESSIONS_BASE_URL=https://sessions.example.com/dl sh
```

Replace `https://sessions.example.com` with the base URL of the server your
organisation runs. `LOOP_SESSIONS_BASE_URL` is required: it names where the
release files live (`<base>/latest/SHA256SUMS`, `<base>/latest/latest.json`
and `<base>/latest/loop-sessions_<os>_<arch>`, the layout `make release`
produces), and the script refuses to download anything until it is set. A
server started with `RELEASE_BUCKET` serves that layout under its own `/dl`
path, which is what the example above points at.

You can read the script before running it. It is about 400 lines of POSIX shell
and has no dependencies beyond `curl`, `awk` and `shasum`:

```sh
curl -fsSL https://sessions.example.com/install.sh | less
```

## Why a shell pipe rather than a downloadable installer

This is the counter-intuitive part, and it is a measured fact rather than a
preference.

A file fetched with `curl` carries no `com.apple.quarantine` attribute, so
Gatekeeper never engages and no Apple Developer certificate is involved. A file
downloaded through a **browser** does get quarantined, and macOS Sequoia removed
the Control-click override, so an unsigned browser-downloaded installer now
dead-ends in System Settings with no way for a non-technical person to continue.

So the shell pipe is both the cheaper lane and the one that actually works.
Claude Code itself ships exactly this way, so most people installing this have
already performed this motion at least once.

## What the installer does, in order

1. **Checks your machine.** Reads `uname` to decide which build you need
   (macOS or Linux, Apple Silicon/ARM or Intel). If it is neither, it stops and
   says so rather than downloading something that will not run.
2. **Checks its own tools.** Confirms `curl` (or `wget`) and a SHA-256 program
   are present. If it cannot verify a download, it refuses to install at all
   rather than continuing unverified.
3. **Prints the plan.** Every path it will touch, before it touches any of them.
4. **Downloads the checksum manifest** from `<host>/latest/SHA256SUMS`.
5. **Downloads the binary** for your platform.
6. **Verifies the SHA-256** against the manifest. On a mismatch the download is
   deleted, nothing is installed, and the script exits non-zero telling you not
   to run the file.
7. **Installs to `~/.local/bin/loop-sessions`.** No `sudo`, ever. The file is
   staged inside the same directory and moved into place with an atomic rename,
   so an interrupted install cannot leave a half-written executable on your PATH.
8. **Runs the new binary once** (`loop-sessions version`) before it replaces any
   existing copy, so a broken build cannot take out a working install.
9. **Checks your PATH** and, if `~/.local/bin` is missing from it, prints the
   exact line to add and the exact file to add it to, for the shell you actually
   use.
10. **Hands off to `loop-sessions install`** for sign-in. The installer does no
    authentication itself.

When the script is piped to `sh`, step 10 is skipped and the command is printed
instead. Sign-in is interactive, and a piped script has no terminal to ask you
anything with.

## What gets installed, and where

| Path | What it is |
|---|---|
| `~/.local/bin/loop-sessions` | the agent binary, the only thing installed |
| `~/.loop/sessions/config.json` | your settings, written at sign-in |
| `~/.loop/sessions/spool/` | captured events waiting to upload |
| `~/.loop/sessions/state/`, `seq/` | bookkeeping |
| `~/.loop/sessions/logs/agent.log` | the agent's own log |
| `~/.claude/settings.json` | gains hook entries marked `#loop-sessions` |

Nothing is installed outside your home directory. Nothing runs as root. Nothing
is added to a system launch path without your sign-in step.

## Verifying the download by hand

If you would rather not trust the script's own check, do it yourself:

```sh
# 1. fetch the manifest and the binary for your platform
curl -fsSLO https://sessions.example.com/dl/latest/SHA256SUMS
curl -fsSLO https://sessions.example.com/dl/latest/loop-sessions_darwin_arm64

# 2. verify
shasum -a 256 -c SHA256SUMS --ignore-missing     # macOS
sha256sum -c SHA256SUMS --ignore-missing         # Linux

# 3. install it yourself
chmod +x loop-sessions_darwin_arm64
mv loop-sessions_darwin_arm64 ~/.local/bin/loop-sessions
```

Replace `darwin_arm64` with `darwin_amd64` (Intel Mac), `linux_amd64` or
`linux_arm64` as appropriate.

The published binaries are built with `-trimpath` and `CGO_ENABLED=0`, so the
same commit produces byte-identical output on any machine. You can rebuild from
source and compare hashes:

```sh
git clone https://github.com/loopai-hq/agent-sessions && cd agent-sessions
make release
shasum -a 256 dist/loop-sessions_darwin_arm64
```

## What leaves your machine

Being direct about this is the point of the page, so: **the content of your
coding sessions leaves your machine.** That is what the tool is for. Concretely,
each captured event can include

- your prompts and the assistant's replies,
- the tools that ran, their inputs, and their output,
- file paths, the working directory, the git branch, and diffs of files changed,
- token counts, model names, and the harness version.

Before anything is written to the spool (not at upload time, but before it
touches your disk) every text field is passed through a credential scrubber
covering API keys, tokens, private keys, database connection strings and
`Authorization` headers. A redaction count travels with each event so a machine
whose scrubber is firing constantly is visible rather than quietly shipping
near-misses.

Separately, a periodic health report leaves the machine containing your
hostname, OS and architecture, uptime, queue and disk statistics, and counts of
sessions and tools found. It deliberately contains **no** session identifiers,
project paths or prompt text.

What never leaves: files the agent was not pointed at, anything in a project you
excluded, and anything captured while the agent is paused.

You can see the current state at any time:

```sh
loop-sessions status     # what is being captured, and whether upload is working
loop-sessions doctor     # diagnose problems and how to fix them
loop-sessions pause      # stop capturing until you resume
```

## Upgrading

Re-run the installer. It replaces the binary and leaves your configuration,
your spool, and your registered hooks alone. It reports the version it replaced
and the version it installed.

## Uninstalling

```sh
loop-sessions uninstall
```

`install/uninstall.sh` does the same without the binary. The server's `/dl`
proxy does not serve it; it is only reachable on a static release host that
publishes it at the base, or from a checkout of this repository.

It prints every path it deletes. By default it removes the binary and any
background service, and **keeps** your captured data and configuration, printing
the one command that removes those too.

Useful flags:

| Flag | Effect |
|---|---|
| `--dry-run` | show exactly what would be removed, change nothing |
| `--purge` | also delete `~/.loop/sessions` |
| `--force` | remove the binary even if harness hooks still reference it |

One safeguard worth knowing about: if hook entries are still registered in
`~/.claude/settings.json`, the uninstaller **refuses to delete the binary** and
tells you how to remove the hooks first. Deleting the binary while a hook still
points at it would make the harness run a missing command on every event, which
would turn removing a telemetry tool into a broken editor. `--force` overrides
this if you want to clean up by hand.

The uninstaller never edits `settings.json` itself. A settings file left
unparseable could stop the harness from starting, and that is a worse outcome
than a hook entry that lingers for a minute.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `LOOP_SESSIONS_BASE_URL` | none, **required** | where releases are fetched from, e.g. `https://sessions.example.com/dl` |
| `LOOP_SESSIONS_VERSION` | `latest` | the channel to follow: `latest` or `canary` |
| `LOOP_SESSIONS_BIN_DIR` | `~/.local/bin` | install somewhere else |
| `LOOP_SESSIONS_NO_ENROLL` | unset | set to `1` to skip sign-in |

Downloads are HTTPS-only, including across redirects, so a server cannot
downgrade the connection partway through. Plain HTTP is permitted only for
`127.0.0.1`, `localhost` and `[::1]`, which exists so the installer can be tested against
a local release host.

## If something goes wrong

Every failure exits non-zero and prints what happened and what to do about it.
The cases the script handles explicitly:

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
language, and whoever operates your server can help from there.
