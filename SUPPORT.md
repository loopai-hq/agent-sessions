# Getting help

## Before you ask

- **On a laptop:** run `loop-sessions doctor`. It explains the current state
  in plain language and says how to fix what it finds; `loop-sessions status`
  says what is being captured and whether upload is working. Most problems
  end there.
- **On a server:** read the boot log. Every missing or malformed environment
  variable is reported in one error; `/readyz` answering 503 means the
  database ping failed and the reason is in the log line
  `readiness check failed`. [docs/OPERATIONS.md](docs/OPERATIONS.md) lists
  every log line worth knowing and what to alert on.
- **Deploying:** [examples/docker-compose/README.md](examples/docker-compose/README.md)
  for a local stack, [examples/deploy-gcp/README.md](examples/deploy-gcp/README.md)
  for Cloud Run with its runbooks, [docs/UPGRADES.md](docs/UPGRADES.md) for
  schema bumps.

## Where to ask

- **Questions and how-do-I:** open an issue and label it `question`.
  GitHub Discussions will replace this once enabled; this page and the
  issue chooser will point there when it is.
- **Bugs:** the bug form asks for the component, what happened, how to
  reproduce it and the `doctor` output.
- **Feature requests:** the feature form asks for the problem before the
  proposal.
- **Security:** never in an issue. Use private vulnerability reporting or
  `security@loopai.com`, as [SECURITY.md](SECURITY.md) describes.

Answers come from the maintainers in [MAINTAINERS.md](MAINTAINERS.md); the
aim is a first response within five business days. There is no paid support
and no SLA beyond that.

## What to redact before you paste anything

This tool captures coding sessions, so its logs and outputs can carry the
very things you should not post in public:

- transcript content: prompts, tool output, file contents, diffs;
- device tokens (`lsd_…`), session cookies, the `SESSION_KEY`, database
  passwords, and anything the scrubber turned into `[REDACTED:<kind>]`;
- your organisation's hostnames, project ids, email addresses and people's
  names (use `sessions.example.com`, `__PROJECT__`, `you@example.com`).

`loop-sessions doctor` and `status` output is designed to be safe to paste;
the server's log lines carry ids, counts and reasons, never content, but do
check the `email` and `device_id` fields before posting.

## If you operate a server for your organisation

Your engineers' first stop is you, not this repository: you hold the roster,
the access log, the retention settings and the fleet page, and the agent
prints your server's address in `status`. Point them at
[install/README.md](install/README.md) for what the agent does on their
machine and at [docs/DATA-PROTECTION.md](docs/DATA-PROTECTION.md) for who
can read what.
