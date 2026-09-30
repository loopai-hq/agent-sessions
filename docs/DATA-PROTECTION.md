# Data protection

loop-sessions copies the content of people's coding sessions off their
laptops and keeps it where their colleagues and their employer's admins can
read it. That is the product, and it is also personal data about the people
who ran the sessions. This page is for the organisation that runs a server
(the controller of that data), the engineer whose laptop is enrolled, and
anyone assessing whether to deploy it. It says what is collected, who can
read it, how that reading is recorded, which controls exist, and what is not
implemented yet. Legal conclusions are yours to draw for your jurisdiction;
the facts below are what the code does.

## What the engineer is told

`loop-sessions install` prints, before sign-in, exactly this sentence, and
`loop-sessions status` repeats it:

> Your captured sessions can be read by you and by the server's admins, and
> by a colleague only through a link you share; every read by anyone but you
> is logged in the admin access log, and how long sessions are kept is set by
> the server operator.

It is printed before sign-in because it is part of what the person is
agreeing to. `install` also prints every path it will touch, what it found
on the machine, and which identity it is signing in as. The installer page
([install/README.md](../install/README.md)) is written for someone deciding
whether to trust it and lists the same inventory.

## Data inventory

| Data | Where it is collected | Where it is stored | Contains |
|---|---|---|---|
| Captured events | Claude Code lifecycle hooks on the laptop, live; the backfill walk over Claude Code and Codex transcripts already on disk | `events` (the durable record), then derived into `sessions`, `turns`, `messages`, `links`, `artifacts`, `artifact_versions` | prompts, the assistant's replies, tool calls with inputs and output, file paths, the working directory, the git branch, diffs of changed files, token counts, model names, the harness version, timestamps, the person's email address and device id |
| Credentials in transcripts | the scrubber on the laptop, before the spool; again on the server at ingest | nowhere: each becomes `[REDACTED:<kind>]`; only the kind and the count are kept | which of the twenty kinds fired, how many times |
| Health reports | the agent, on a fixed cadence | `health_reports`, hourly rollups, `health_latest` | hostname, OS and architecture, agent version and build, uptime, spool and disk counts, which harnesses were found and their versions, whether capture is paused, the agent's conditions. No session ids, project paths or prompt text, by construction (`internal/health`) |
| Enrolment | the sign-in exchange at `install` | `principals`, `devices`, `device_tokens` | email, role, display name, who added the row and when; per device the hostname, OS, architecture, agent version, enrolled and last-seen times; a hash of the device token, never the token |
| Usage and cost | ingest, from the events' token counts and `model_prices` | `usage_ledger` | per person, per model, tokens and derived cost |
| Reads of other people's sessions | every read whose viewer is not the owner | `access_log` | viewer, session id, owner, how (`admin` or `share`), when |
| Shares | a person sharing their own session | `shares` | who shared which session with which colleague (or with any signed-in employee), the link token, expiry, revocation |
| Skill usage | ingest, and emitters that are not a laptop | `skill_invocations` and the catalog tables | which skill a person or platform invoked, when, with what outcome |
| Slack mirror | only when a person turns it on | `slack_prefs`, `slack_posts`, `slack_groups` | the person's mirror preference and which sessions were posted where |
| Admin actions | the roster and fleet pages | `admin_actions`, `principal_changes`, `fleet_mutes` | who promoted, disabled, muted whom, and when |

What is never collected: files the harness did not show the agent, projects
in the person's `exclude_paths` (or outside `capture_only_paths`), anything
while capture is paused, and harnesses the person chose to skip at install
(`skipped_tools`). Those three choices are recorded in the client's own
`config.json` as the record of what the person consented to.

Nothing goes to the maintainers of this software. The client talks only to
the endpoint it enrolled with; the server talks to Google to fetch the
certificates it verifies sign-in tokens against, and, only when configured,
to a Cloud Storage bucket, Slack and BigQuery. There is no usage telemetry,
crash reporting or update check that reaches anyone but the operator.

## Who can read what, and the audit row

Authorization is a predicate inside the read (`server/store`): a session is
returned to its owner, to an admin, or to the holder of a live share, and to
nobody else. A session the viewer may not see and a session that does not
exist produce byte-identical responses, so the dashboard cannot be used to
learn that a colleague ran something.

| Role | Sees | Recorded |
|---|---|---|
| Member (the default for any allowed-domain account on first sign-in) | their own sessions, and sessions shared with them or with everyone | a read through a share writes an `access_log` row (`via = share`) |
| Admin | every session, the fleet page, the roster, the access log | every read of someone else's session writes an `access_log` row (`via = admin`) in the same transaction as the read, so an admin cannot read without leaving the row |
| Owner | their own sessions | no row: reading your own work is not an access event |

The access log is visible to admins at `/admin/access`. Admin actions on the
roster (promote, disable, re-enable) and on fleet mutes are recorded
separately. The system refuses to end up with zero admins.

Members cannot read each other's sessions by default. The `ALLOWED_DOMAINS`
gate decides who may sign in at all; the roster decides what each person
may do; a disabled row is refused even though its domain is allowed.

## Controls the operator has

- **Retention** (`RETENTION_BODY_DAYS`, `RETENTION_SESSION_DAYS`): event
  bodies, then whole sessions, are deleted after the windows you set. The
  default keeps everything and the server says so at boot, pricing the
  recommended 90-day and 365-day windows. A window below seven days is
  refused, so a typo cannot delete a corpus. This is the storage-limitation
  control: [OPERATIONS.md](OPERATIONS.md#retention).
- **Per-person erasure**: a one-transaction SQL recipe, keyed on the
  address, that removes a person's events, sessions, usage, health reports,
  devices and tokens while keeping the roster row and the access log (the
  record of who read their sessions outlives its subject):
  [OPERATIONS.md](OPERATIONS.md#purging-one-person), and the Cloud SQL
  version in the example deployment's "Runbook: purge a person".
- **Revoking a device**: a stolen or retired laptop's credential is
  withdrawn by setting `revoked_at`; a revoked device that keeps reporting
  is shown on the fleet page as a finding
  ([OPERATIONS.md](OPERATIONS.md#revoking-a-device)).
- **Disabling a person**: the People page (`/admin/principals`) disables a
  roster row; sign-in is refused from then on and their devices' deliveries
  are refused with it.
- **Scope of capture**: the person's own `exclude_paths`,
  `capture_only_paths`, `skipped_tools` and `pause`, all honoured by every
  path (live capture, backfill, the repair walk, delivery).
- **Scrubbing**: twenty credential kinds on the laptop, the same rules again
  at ingest, and a count of what the agent missed so a stale agent is
  visible.

## Controls the engineer has

`loop-sessions pause [--for 2h]` stops capture and delivery (and resumes on
its own after the period); `status` shows what is being captured and where
it goes; `loop-sessions uninstall` removes the hooks, after which the binary
can be removed (`install/uninstall.sh` does that); `--purge` also deletes
`~/.loop/sessions`, including anything captured that had not yet uploaded.
What has already been delivered is the operator's to delete.

## Controller and processor

The organisation that runs the server decides why the data is collected,
who may read it and how long it is kept: it is the controller. The software
is self-hosted and sends nothing to its authors, so the maintainers of this
repository process none of your data and are not a processor. Your hosting
provider (the database, the container platform) and, if configured, Slack
and Google Cloud (BigQuery export, the release bucket) are processors under
whatever terms you have with them. Firebase Authentication sees the sign-in
event (the Google account, the project) and not the sessions.

## Before you deploy: a checklist

Written as questions a data-protection impact assessment asks; the answers
are yours, the facts are above.

1. **Purpose and basis.** Why are transcripts kept? Who decided? Is the
   basis you rely on (contract, legitimate interest, consent, a works-council
   agreement) documented, and does it cover reading by admins?
2. **Notice.** Have the people whose laptops are enrolled been told, beyond
   the sentence `install` prints? Where is your notice, and does it name the
   retention windows you chose?
3. **Minimisation.** Which projects should never be captured? Tell people
   about `exclude_paths` and `capture_only_paths`. Should Codex history be
   imported, or only new sessions (`--backfill-since`, `--skip-backfill`)?
4. **Retention.** Have you set `RETENTION_BODY_DAYS` and
   `RETENTION_SESSION_DAYS`, or decided in writing to keep everything?
5. **Access.** Who are the admins? Are they few? Do you review the access
   log, and do people know it exists?
6. **Rights.** How does a person ask what is held about them (an admin can
   read it), ask for it to be deleted (the purge recipe), or ask for a copy
   (see below)? Who runs the recipe, and how quickly?
7. **Security.** Is `PUBLIC_URL` HTTPS? Is the database reachable only from
   the server? Are backups encrypted and retained no longer than the data?
   Have you read [THREAT-MODEL.md](THREAT-MODEL.md)?
8. **Third parties.** Are you comfortable with Google (Firebase sign-in, and
   the SDK the browser loads from `www.gstatic.com`) and, if enabled, Slack
   and BigQuery?
9. **Leavers.** When someone leaves, do you disable the roster row, revoke
   the devices, and run the purge, or keep the sessions under the retention
   policy? Decide before the first departure.

## What is not implemented

- **An admin route or CLI for erasure, and an export.** Erasure today is the
  SQL recipe an operator runs by hand; there is no button, no CLI and no
  audit row for it beyond your own change record, and there is no per-person
  export (a copy of everything held about one person in a portable form).
  Both are on [ROADMAP.md](../ROADMAP.md).
- **A route to revoke a device.** The store method exists; the route does
  not. Revocation is SQL until then.
- **Notice inside the dashboard.** The sentence above is printed by the
  client; the dashboard itself shows no privacy notice to a signed-in
  member.
- **Pseudonymisation.** Sessions are keyed by email address; there is no
  pseudonymous mode.
- **Consent per session.** The person consents at install and can pause or
  exclude paths; there is no per-session prompt.
