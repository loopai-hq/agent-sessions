# Deploying loop-sessions

> This directory is one example of hosting loop-sessions: a single Cloud Run
> service on GCP with Cloud SQL, plus optional monitoring and a BigQuery export.
> It is not the only way to run it: the server needs Postgres and a Firebase project.

The whole server is one Cloud Run service backed by one Cloud SQL Postgres
instance. Follow the steps in order; each one is idempotent, so a re-run after
a failure is safe.

Written to be followed by someone who did not build this. Where a step makes a
choice that has a plausible-looking alternative, the reason is stated inline,
because the alternative is what the next person will try.

## 0. What you need before you start

- `gcloud` authenticated as someone with Owner or the equivalent set of roles
  on the target project.
- `docker` with BuildKit (any current Docker Desktop or Engine).
- The repository checked out. **Every command below runs from the repository
  root**, not from this directory.

Set the variables once. They are referenced by every step.

```bash
export PROJECT=my-project
export REGION=us-central1
export INSTANCE=loop-sessions
export SERVICE=loop-sessions
# Cloud Run's URL for the service. It is not known until the service exists;
# step 7 says how to read the real one back and correct it.
export PUBLIC_URL=https://loop-sessions-<project-number>.${REGION}.run.app
# Every email domain your people sign in from, comma-separated. Listing one
# domain locks out everybody whose Google account sits on another.
export DOMAINS=example.com
# The bucket the installer and the agent binaries are served from
# (RELEASE_BUCKET in cloudrun.yaml; the release workflow example publishes to it).
export RELEASE_BUCKET=my-project-loop-sessions-releases
# The Firebase web API key, from the Firebase console (step 6).
export FIREBASE_API_KEY=<web API key>
export TAG=$(git rev-parse --short HEAD)

gcloud config set project "$PROJECT"
```

`PROJECT` is doing double duty: it is the GCP project and it is also the
**Firebase** project this service authenticates against. That is not a
coincidence to be tidied up later: a person's Firebase account exists in
exactly one project, and pointing this elsewhere signs nobody in.

Enable the APIs. This is slow the first time and instant afterwards.

```bash
gcloud services enable \
  run.googleapis.com \
  sqladmin.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com
```

## 1. Artifact Registry

```bash
gcloud artifacts repositories create loop-sessions \
  --repository-format=docker \
  --location="$REGION" \
  --description="loop-sessions server images"

gcloud auth configure-docker "${REGION}-docker.pkg.dev"
```

## 2. Cloud SQL

Postgres 16, because the schema uses generated `tsvector` columns and
`BIGSERIAL` identity semantics that changed across versions.

**Postgres 15 is a hard floor**, here and on any machine running the tests.
`0001_init.sql` builds the health-report idempotency index with
`NULLS NOT DISTINCT`, which does not exist before 15. On 14 the migration stops
at `syntax error at or near "NULLS"`, which reads like a typo in the migration
rather than a version problem. A stock `brew install postgresql` is currently
14, so this is the first thing to check when the integration tests fail on a
laptop but pass in CI.

```bash
gcloud sql instances create "$INSTANCE" \
  --database-version=POSTGRES_16 \
  --tier=db-custom-2-7680 \
  --region="$REGION" \
  --storage-size=100GB \
  --storage-auto-increase \
  --backup-start-time=19:00 \
  --enable-point-in-time-recovery \
  --maintenance-window-day=SUN \
  --maintenance-window-hour=20
```

Point-in-time recovery is on because this database is the only copy of the
corpus. The laptops delete their spool as soon as the server acknowledges an
upload, so a restore from a nightly backup would lose a working day of
sessions that no longer exist anywhere else.

Create the database and its user. Generate the password rather than choosing
one; it is only ever read by Secret Manager.

```bash
DB_PASSWORD=$(openssl rand -base64 32)

gcloud sql databases create loop_sessions --instance="$INSTANCE"
gcloud sql users create loop_sessions --instance="$INSTANCE" --password="$DB_PASSWORD"
```

## 3. Secrets

Two secrets, and neither is an authentication credential. Create them before
the service account, so the grant in step 4 has something to bind to.

```bash
printf '%s' "$DB_PASSWORD" | \
  gcloud secrets create loop-sessions-db-password --data-file=- --replication-policy=automatic

# 32 random bytes, used to sign the dashboard cookie and the admin form tokens.
openssl rand -base64 32 | tr -d '\n' | \
  gcloud secrets create loop-sessions-session-key --data-file=- --replication-policy=automatic
```

There is no third. This server holds no OAuth client secret, because it has no
OAuth client (see step 6). The one Firebase value the sign-in page needs is the
web API key, which is public by design and is set as a plain environment value
in `cloudrun.yaml`. Do not "fix" that by moving it into Secret Manager: binding
it as a secret tells the next operator it is a credential, and they will treat
reading it out of the page source as an incident and a rotation as an emergency.
It authorises nothing on its own.

## 4. Service account

The runtime identity gets exactly three capabilities: reach the database, read
its own secrets, write logs. It gets no Cloud SQL admin role, so a compromised
container cannot drop the instance it is reading.

```bash
SA="loop-sessions-run@${PROJECT}.iam.gserviceaccount.com"

gcloud iam service-accounts create loop-sessions-run \
  --display-name="loop-sessions Cloud Run runtime"

for role in roles/cloudsql.client roles/logging.logWriter; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:${SA}" --role="$role"
done

for s in db-password session-key; do
  gcloud secrets add-iam-policy-binding "loop-sessions-${s}" \
    --member="serviceAccount:${SA}" \
    --role=roles/secretmanager.secretAccessor
done
```

It gets no Firebase or Identity Platform role either. The server verifies ID
tokens against Google's published signing certificates, which are fetched over
plain HTTPS with no credential, so the runtime identity needs nothing from
Firebase at all.

## 5. Migrations

Run them from your laptop through the Cloud SQL Auth Proxy. The proxy
authenticates with your own gcloud credentials and needs no IP allowlist, which
is why this does not require opening the instance to the internet even
temporarily.

```bash
# Terminal 1
cloud-sql-proxy "${PROJECT}:${REGION}:${INSTANCE}" --port 5433

# Terminal 2
export PGPASSWORD="$DB_PASSWORD"
for f in server/store/migrations/*.sql; do
  echo "applying $f"
  psql -h 127.0.0.1 -p 5433 -U loop_sessions -d loop_sessions -v ON_ERROR_STOP=1 -f "$f"
done
```

`ON_ERROR_STOP=1` is not optional. Without it psql prints the error, carries
on with the rest of the file, and exits zero, which is how a half-applied
schema reaches production looking like a successful migration.

Confirm the tables landed before going further. A deploy against an empty
schema starts cleanly and then fails on the first request, which is a much
more confusing failure than this one.

```bash
psql -h 127.0.0.1 -p 5433 -U loop_sessions -d loop_sessions -c '\dt'
# expect: principals, devices, device_tokens, sessions, events, messages,
#         shares, access_log, health_reports
```

Do not seed admins with an `INSERT` typed here: a guessed address is how
somebody ends up locked out of the page that fixes lockouts. The repository
README says how the first admin is bootstrapped. Confirm the roster instead:

```bash
psql -h 127.0.0.1 -p 5433 -U loop_sessions -d loop_sessions \
  -c "SELECT email, role FROM principals ORDER BY email;"
# expect one row per configured admin, role admin
```

The server also runs these migrations itself at boot, under a Postgres advisory
lock, so several Cloud Run instances starting together apply them exactly once.
Running them from here first is still worth doing: a schema failure at your
terminal is a legible error, and the same failure at boot is a crashloop.

Everyone else is added from the People page after they first try to sign in.

## 6. Firebase sign-in

There is no consent screen to configure and no OAuth client to create. The
browser signs in with the Firebase Auth SDK against `__PROJECT__`, and the
server verifies the resulting ID token.

That is a deliberate change from the original design, and the reason is worth
knowing before somebody tries to put the OAuth flow back. Firebase Auth needs
no OAuth client of its own, so a project that already sits at Google's
hard ceiling of OAuth clients, or one that shares Firebase Auth with other
apps, can adopt this server without touching those credentials. A dedicated
project was considered and rejected as ceremony for one internal tool; a
Firebase project that is already carrying the organisation's accounts removes
the dependency rather than negotiating with it.

What the server needs is already in `cloudrun.yaml`:

| Variable | Value |
|---|---|
| `FIREBASE_PROJECT_ID` | `__PROJECT__`, substituted from `__PROJECT__` |
| `FIREBASE_API_KEY` | The web API key. Public, a plain value, not a secret |
| `FIREBASE_AUTH_DOMAIN` | Unset. Defaults to `<project>.firebaseapp.com` |

Read the API key from the Firebase console under Project settings.

### Two things to check in the Firebase console

Both are project-wide and both are almost certainly already true when other
apps on the project depend on them. Check rather than assume, because either one being
wrong presents as sign-in silently failing in the browser with nothing in this
service's logs.

1. **Google is enabled as a sign-in provider.** Authentication, Sign-in method.
   The server refuses any token whose `firebase.sign_in_provider` is not
   `google.com`, so a person who somehow obtains a Firebase account bearing a
   company address by email link or password cannot use it here.
2. **This service's domain is in the authorized-domains list.** This is the one
   thing that is genuinely new for this deployment. See below.

### The authorized-domains list

Firebase refuses to complete a sign-in popup opened from an origin that is not
in the project's authorized-domains list, so `__PUBLIC_URL__`
has to be in it. The failure without it is `auth/unauthorized-domain` in the
browser console and nothing at all server-side, because the request never
reaches this service.

Manage it through the **Identity Toolkit admin API**, not by hand in the
console. The console's Authorized domains screen writes the same field, but it
gives no record of what changed, and this list is shared with every other app
on the project.

```bash
TOKEN=$(gcloud auth print-access-token)

# Read the current list FIRST. This is not optional; see the warning below.
curl -sS -H "Authorization: Bearer ${TOKEN}" \
  "https://identitytoolkit.googleapis.com/admin/v2/projects/${PROJECT}/config" \
  | jq '.authorizedDomains'

# Then send the whole list back with ours appended. The body below is a
# TEMPLATE, not a value to run as it stands: build it from what the read above
# returned. This field is replaced, not merged, so anything you leave out is
# removed from the project.
curl -sS -X PATCH \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  "https://identitytoolkit.googleapis.com/admin/v2/projects/${PROJECT}/config?updateMask=authorizedDomains" \
  -d '{"authorizedDomains":[
        "<every domain the read returned, verbatim>",
        "__PUBLIC_URL__"
      ]}' | jq '.authorizedDomains'
```

**`updateMask=authorizedDomains` replaces the field; it does not merge into
it.** Sending only this service's domain removes every other app's domain
from the project and breaks their sign-in, everywhere, immediately, with no
deploy to roll back and no revision to point at. That is why the read comes
first and why the body above is left as a placeholder rather than a plausible
list somebody could run without looking. The read returns `localhost` and the
project's own `firebaseapp.com` and `web.app` domains at minimum, plus whatever
other apps on the project have added since.

The caller needs `firebaseauth.configs.update` on the project: Firebase Admin
or Owner. Your own gcloud credentials, not the runtime service account, which
deliberately has no Firebase role at all.

### The CLI

Nothing to configure. The agent opens `$PUBLIC_URL/auth/cli` in a browser, that
page performs the same Firebase sign-in, and it posts the resulting ID token
back to a loopback listener the CLI is holding open. There is no Desktop client,
no `--client-id` flag and no client id stamped into a release build, because
there is no client credential anywhere in the flow.

## 7. Build, push, deploy

```bash
docker build \
  --build-arg VERSION="$TAG" \
  --build-arg BUILD_DATE="$(TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --build-arg REVISION="$(git rev-parse HEAD 2>/dev/null || true)" \
  -t "${REGION}-docker.pkg.dev/${PROJECT}/loop-sessions/server:${TAG}" \
  .

docker push "${REGION}-docker.pkg.dev/${PROJECT}/loop-sessions/server:${TAG}"
```

On an Apple Silicon laptop add `--platform linux/amd64`. Cloud Run will accept
an arm64 image and then fail to start it, with an error that does not mention
the architecture.

```bash
sed -e "s|__PROJECT__|${PROJECT}|g" \
    -e "s|__REGION__|${REGION}|g" \
    -e "s|__INSTANCE__|${INSTANCE}|g" \
    -e "s|__TAG__|${TAG}|g" \
    -e "s|__DOMAINS__|${DOMAINS}|g" \
    -e "s|__ADMIN_EMAILS__|${ADMIN_EMAILS}|g" \
    -e "s|__PUBLIC_URL__|${PUBLIC_URL}|g" \
    -e "s|__RELEASE_BUCKET__|${RELEASE_BUCKET}|g" \
    -e "s|__FIREBASE_API_KEY__|${FIREBASE_API_KEY}|g" \
    examples/deploy-gcp/cloudrun.yaml > /tmp/cloudrun.rendered.yaml

# Nothing should be left. An unsubstituted placeholder reaches the container as
# a literal, and PUBLIC_URL is the one that then fails at boot with a message
# about a URL nobody typed.
grep -n '__[A-Z_]*__' /tmp/cloudrun.rendered.yaml && exit 1

gcloud run services replace /tmp/cloudrun.rendered.yaml --region="$REGION"
```

Allow unauthenticated invocations:

```bash
gcloud run services add-iam-policy-binding "$SERVICE" \
  --region="$REGION" --member=allUsers --role=roles/run.invoker
```

That flag looks alarming and is correct. Cloud Run's own IAM check only
understands Google identities. The laptops authenticate with device bearer
tokens, which are not Google identities, so a service that required an IAM
identity would reject every upload. Authentication happens in the process
instead: a device token for `/v1/*` client endpoints, a session cookie minted
from a verified Firebase ID token for the dashboard, and nothing is served to an
unauthenticated request except the sign-in page.

## 7b. Publish the matching agent release (required)

The fleet page judges every machine against THIS build, because agent releases
and the server are cut from the same commit. That invariant is only true if you
keep it true: a server deployed without a matching agent release makes every
machine in the fleet read stale, including ones that self-upgraded minutes ago.
It happened on the day the staleness badges shipped, twice.

```bash
make release
make verify
gsutil -m -h "Cache-Control:no-store" cp \
  dist/loop-sessions_darwin_arm64 dist/loop-sessions_darwin_amd64 \
  dist/loop-sessions_linux_amd64 dist/loop-sessions_linux_arm64 \
  dist/SHA256SUMS gs://__RELEASE_BUCKET__/latest/
```

The fleet converges by itself from there: agents self-upgrade at their next
session start.

## 8. Verify

```bash
URL=$(gcloud run services describe "$SERVICE" --region="$REGION" --format='value(status.url)')

# An unauthenticated dashboard request redirects to sign-in rather than
# rendering anything.
curl -sS -o /dev/null -w '%{http_code} %{redirect_url}\n' "$URL/sessions"
# expect: 302 .../auth/signin?next=%2Fsessions

# The sign-in page renders. It does NOT redirect anywhere: the browser talks to
# Firebase from this page, so a 302 here means an OAuth flow survived somewhere.
curl -sS -o /dev/null -w '%{http_code}\n' "$URL/auth/signin"
# expect: 200

# The page carries the project it will sign in against. If this is empty or
# names another project, FIREBASE_PROJECT_ID did not reach the revision.
curl -sS "$URL/auth/signin" | grep -o '__PROJECT__' | head -1

# The relaxed script policy is scoped to the sign-in page alone.
curl -sSI "$URL/auth/signin" | grep -i content-security-policy
# expect: script-src 'self' https://www.gstatic.com
curl -sSI "$URL/sessions" | grep -i content-security-policy
# expect: script-src 'none' -- the dashboard renders untrusted transcript text

# An unauthenticated ingest request is refused, and refused as 401 rather than
# as a redirect: a client that followed a redirect would post a batch of
# events at an HTML page.
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "$URL/v1/events" -d '[]'
# expect: 401
```

Then in a browser, signed in with an account on `ALLOWED_DOMAINS`:

1. `$URL/sessions` renders the list, empty until a laptop uploads something.
2. `$URL/admin/principals` lists the addresses you put in `ADMIN_EMAILS`, role
   admin.
3. `$URL/admin/fleet` shows nothing enrolled yet. That page is where you will
   watch the rollout.

Finally, enrol one laptop and confirm a session appears end to end. Until that
has happened once, nothing above has been tested against a real client.

## A note on timestamps

The dashboard ships no JavaScript, so it cannot read the viewer's clock and
renders every time in the server's zone. `TZ` in `cloudrun.yaml` sets that
zone, and it is a company-wide choice: change it there and everyone's
"yesterday afternoon" moves together.

A named zone needs a zone database at runtime. The distroless static base
carries one, but the guarantee that matters is in the binary: the server's main
package should `import _ "time/tzdata"`, which embeds the database and makes
the container's contents irrelevant. Without it, a base image change turns
a named `TZ` such as `Europe/Berlin` into a silent fallback to UTC, and every
timestamp on the dashboard shifts by hours with nothing in the logs to say why.

## 9. Rolling back

Revisions are immutable, so a rollback is a traffic change and takes seconds.

```bash
gcloud run revisions list --service="$SERVICE" --region="$REGION"
gcloud run services update-traffic "$SERVICE" \
  --region="$REGION" --to-revisions=loop-sessions-00007-abc=100
```

A rollback does not undo a migration. Migrations here must be additive for
exactly that reason: the previous revision has to keep working against the new
schema.

## A note on IAP

The dashboard is Workspace-only, and the obvious way to enforce that is
Identity-Aware Proxy. This deployment does not use it, deliberately.

IAP sits in front of an external HTTPS load balancer with a serverless network
endpoint group, and it authenticates **every** request to the backend as a
Google identity. The laptop clients do not have one: they present a device
bearer token issued at enrolment, exactly so that installing the agent needs no
gcloud and no cloud login. IAP has no per-path exemption, so putting it in
front of this service would reject every upload.

The alternatives, in the order they should be considered:

1. **What is deployed.** In-process Firebase Auth: an ID token verified on
   every sign-in against this project's issuer, audience and signing
   certificates, with `firebase.sign_in_provider` required to be `google.com`,
   the verified email checked against `ALLOWED_DOMAINS`, and the `principals`
   table checked after that.

   Read the second control carefully, because it is what changed. A Firebase ID
   token carries **no `hd` claim**, so there is no Workspace-membership
   assertion from Google to lean on. The email domain and the roster are the
   whole of it. That makes `ALLOWED_DOMAINS` and the `principals` table
   load-bearing in a way they were not under OAuth, and it is why the roster
   check is the primary control here rather than a second opinion.
2. **IAP on a second, dashboard-only service** sharing this database, with
   ingest left on the public service. This buys defence in depth for the
   dashboard at the cost of a second service, a load balancer, a certificate
   and a second deploy path. Worth doing if the corpus later holds something
   that changes the risk calculation; not worth it for the first release.
3. **IAP over everything**, with the clients rewritten to obtain Google
   credentials. This contradicts the install constraint that started the
   project, and should not be proposed without revisiting that decision first.

If you take option 2, set `run.googleapis.com/ingress` to
`internal-and-cloud-load-balancing` on the dashboard service only. Leaving it
as `all` while IAP fronts the load balancer leaves the direct `run.app` URL
open, and that URL bypasses IAP entirely.

## Runbook: live Slack threads

The mirror's live pass runs every minute inside the server. Signals and
levers, in the order an operator reaches for them:

- Health: log lines `slack live` (per-pass counts) and `slack live pass
  failed`. The log-based metric `loop_sessions/slack_live_pass_failed`
  (`monitoring/metrics/slack_live_pass_failed.yaml`) counts the failures and
  the policy `monitoring/policies/slack-live-pass-failed.json` alerts on it
  being nonzero for 15 minutes; both exist once `monitoring/apply.sh` has
  been run (see "Observability" below). Until then the metric is a file in
  this repository and nothing more.
- Message-level failures: `SELECT key, last_error, attempts FROM
  slack_posts WHERE last_error IS NOT NULL ORDER BY claimed_at DESC`:
  Slack's own error word per undelivered message. `invalid_auth` means
  the bot token (secret `loop-sessions-slack-bot-token`) needs rotating;
  `not_in_channel` self-heals via join; `channel_not_found` is a typo'd
  or archived destination and spends its attempts, by design.
- Unstick one message: `DELETE FROM slack_posts WHERE key='...' AND
  posted_at IS NULL` (the next pass re-claims it fresh).
- Silence one person immediately: `UPDATE slack_prefs SET mode='off'
  WHERE email='...'`: refuses every live request and digest for them.
- Kill the whole surface: null the SLACK_BOT_TOKEN env on the service and
  redeploy; every pass fails visibly rather than half-working.
- Caps: 60 messages per thread (then one capped notice), 200 per person
  per rolling day, 10 replies per session per pass, one post per second
  globally. A thread that seems to lag is usually pacing, not failure.

## Runbook: notification groups

Groups extend the live-thread runbook above; the same failure ledger and
levers apply. Additions:

- Per-group kill: Disable on the settings page, or `UPDATE slack_groups
  SET disabled = TRUE WHERE id = ...`: running threads close, nothing
  new opens for that destination.
- Per-person kill: the master switch on /settings/notifications, or
  `UPDATE slack_prefs SET live_disabled = TRUE WHERE email = ...`.
- One pair stuck: `SELECT * FROM session_mirrors WHERE detached_at IS
  NULL` names every active narration; detach with `UPDATE ... SET
  detached_at = now()`.
- Slack-side buttons: mount by pasting the app's signing secret into the
  LOOP_SESSIONS_SLACK_SIGNING_SECRET env (Cloud Run secret) and setting
  the app's Interactivity URL to POST {PublicURL}/v1/slack/interactive.
  Without it the endpoint is absent and roots carry no buttons; that is
  the off state, not a fault.
- Destination picker: the channel search on /settings/notifications calls
  conversations.list, which needs the `channels:read` bot scope (add under
  OAuth & Permissions, then Reinstall to Workspace; if the xoxb value
  changes, update secret `loop-sessions-slack-bot-token`). Without it the
  search logs `channel search ... missing_scope` and shows no matches;
  the DM and by-id destinations keep working.

## Observability

Everything an operator can see about this service comes from one JSON line
per event on stdout, the request log Cloud Run writes on its own, and a
handful of Cloud SQL and Cloud Run metrics. The pieces that turn those into
alerts live in `examples/deploy-gcp/monitoring/` and are applied by
`monitoring/apply.sh`; nothing is configured by hand in the console except
the notification channels (Slack channels can only be authorised there).

### The log contract

The process logger is `app.NewCloudLogger` (`server/app/config.go`). Every
line carries:

| Key | Meaning |
|---|---|
| `severity` | `DEBUG`, `INFO`, `WARNING` or `ERROR`. Cloud Logging promotes it to the LogEntry, so `severity>=ERROR` filters work. slog's default key is `level` and its spelling is `WARN`, neither of which Cloud Logging reads; every line this service wrote before this contract arrived with no severity at all. |
| `message` | The line's text. Alerts filter on it exactly, so the strings below are part of the contract. |
| `timestamp` | Event time, RFC 3339. |
| `version` | The build (`main.version`, the git sha the image was built from). Two revisions serve side by side during a rollout; this is how a line is attributed to one of them. |
| `logging.googleapis.com/trace`, `logging.googleapis.com/spanId` | Present on lines logged inside a request with the request's context. Read from `X-Cloud-Trace-Context` (fallback `traceparent`) by the middleware in `server/app/app.go`; the Logs Explorer groups the line with Cloud Run's request entry for the same request. |

No line carries a session's content, a prompt, a device token or a cookie.
The lines below carry ids, counts and reasons only, and the ingest tests
grep the log for payload text to keep it that way.

The lines alerts are built from, by `message`:

| Message | Severity | Fields | Source |
|---|---|---|---|
| `ingest batch` | INFO | `email`, `device_id`, `agent_version`, `items`, `accepted`, `duplicate`, `rejected`, `undecided`, `bytes`, `status` | one per `/v1/events` request, `server/ingest` |
| `ingest item rejected` | WARNING | `reason`, `event_type`, `email`, `device_id` | one per item refused for good; the agent quarantines it |
| `store returned no verdict` | ERROR | `event_id`, `event_type`, `email`, `device_id` | an admitted item the store answered for neither way: a server bug |
| `ingest request failed` | ERROR | `op`, `error`, `method`, `path` | the detail behind every 5xx the ingest routes answer |
| `ingest scrubbed a credential the agent missed` | WARNING | `email`, `device_id`, `session_id`, `event_id`, `event_type`, `kinds` | the server-side scrub caught something the laptop's did not |
| `readiness check failed` | WARNING | `err` | `/readyz` answered 503 |
| `slack live pass failed` | ERROR | `err`, `pass` | the Slack mirror's minute pass failed |
| `derive step` | INFO | `step`, `derive_version`, `rows`, `batches`, `seconds`, `rows_per_sec`, `finished` | one per finished derive step; `rows_per_sec` on the `event_keys` step is the rebuild's pace |
| `derive pass` | INFO | `pass{derive_version, done, deferred, waiting, until, failed, stalled_minutes, steps, elapsed}` | one per derive runner pass; `stalled_minutes` is how long the first unfinished step has gone without committing a batch |
| `derive step failed` | ERROR | `step`, `derive_version`, `attempts`, `rows`, `error`, `recover` | a derive step reached its retry cap; `recover` is the statement that lets it retry |
| `derive session failed` | ERROR | `session_id`, `err`, `derive_version` | the dirty pass could not fold one session and parked it |
| `skill invocations derived` | INFO | `email`, `device_id`, `agent_platform`, `candidates`, `rows_user`, `rows_agent`, `outcome_updates`, `duplicates`, `skipped_builtin`, `skipped_shape`, `unconfirmed`, `oracle_errors`, `touched` | one per `/v1/events` batch holding a `Skill` call, its result or a slash prompt (`server/store/skills.go`); `touched` when a row was written or deduplicated; `skipped_shape` counts the rows dropped for an id outside the shape or an overlong key and the names stored as `off-shape` (design 4a), so beside rows written it is not by itself a drop; `unconfirmed` is a name the catalog does not hold, dropped for good, while `oracle_errors` is a catalog read that FAILED, which ends the derivation and takes the savepoint path, so this line always carries it as 0 and the `skill invocation store failed` line is what you will see instead |
| `skill invocation store failed` | ERROR | `op`, `error`, `method`, `path`, `platform` | the store could not write a skill row; `op=derive` at ingest, where the events still committed and the sessions were queued |
| `skill invocation deferred` | WARNING | `email`, `device_id`, `sqlstate`, `sessions` | the ingest-time derivation hit a lock a derive batch holds (55P03, 40P01); rolled back and queued, under no policy |
| `skill platform summary` | INFO | `platform`, `environment`, `origin`, `trigger`, `rows_24h`, `minutes_since_last_row`, `rederive_queue`, `rederive_oldest_minutes` | one per expected series per fleet tick, from the newest row of `skill_invocations`, never per token; the weekend in the mask zone (America/New_York, `skillMaskZone` in `server/store/skills.go`) is not counted; a series with no row reads 100000, the derived series read 0 for the first day after 0022 |
| `skill invocation accepted` | INFO | `trust`, `platform`, `environment`, `origin`, `source_token_id` or `device_id`, `email` (device rows), `skill`, `trigger`, `outcome`, `duplicate`, `soft_revoked`, `time_clamped`, `actor_known`, `bytes`, `status` | one per 200 on `POST /v1/skill-invocations` (`server/skillusage`); a call on a limit-0 token logs `duplicate=true, soft_revoked=true` and writes nothing; `skill` is the normalised slug through the id gate, never the raw name |
| `skill invocation rejected` | WARNING | `reason` (`unauthenticated`, `rate_limited`, `invalid_payload`, `body_too_large`, `platform_mismatch`, `forbidden`, `run_conflict`), `field`, `token_shape` (`device`, `source`, `unknown`), `token_state` (`none`, `revoked`, `expired`, `wrong_scope`, `live`), `source_token_id`, `platform`, `status` | one per 4xx on `POST /v1/skill-invocations` and on `POST /v1/skill-invocations/reconciler-runs`, whose 409 is the `run_conflict` reason (both `server/skillusage`), and one per 401 or 403 on `PUT /v1/skill-catalog/{source_repo}` (`server/api/skills_catalog.go`), which shares the line rather than opening a third of its own and takes the message string from `internal/skilllog` so the two cannot drift and `server/api` keeps no import behind its ports; the PUT's other refusals stay off it, since a malformed catalog body is the publisher's bug and not a credential's. `token_state`, the id and `platform` come from the token row the verify read, never from the body or the path, so a leaked token used after a revoke is seen, a catalog token used after its revoke reaches policy 17's `revoked`/`expired` condition (intended: that is the alarm), and a prober cannot grow the label set |
| `skill invocation preempted` | WARNING | `platform`, `preempted_by`, `preempted_device`, `session_ref`, `skill` | a derived copy overwrote an API row on its key (`server/store/skills.go`); whichever of the token or the `lsd_` device is set is carried, the other empty |
| `skill token summary` | INFO | `platform`, `environment`, `source_token_id`, `scope`, `expires_in_days`, `issued_days`, `last_used_minutes_ago`, `window_count`, `rate_limit_per_min`, `soft_revoked` | one per live source token per fleet tick (`server/store/source_tokens.go`), after the platform summary; a never-used token reads `last_used_minutes_ago` 100000 and one used after the tick's clock was captured reads 0, a NULL expiry reads `expires_in_days` 0 with `issued_days` 100000; the same tick also writes the `skill platform summary` line for each (platform, environment, hook or beacon) series a live emitter token makes expected, `trigger` empty on those, from the rows of the last 30 days (a series with none younger reads 100000, the same as never) |
| `skill reconciler run` | INFO, WARNING when `mismatch` | `platform`, `source_token_id`, `idempotency_key`, `mismatch`, `rows_posted`, `rows_seen`, `sessions_scanned`, `truncated`, `duplicate`, `soft_revoked` | one per run recorded on `POST /v1/skill-invocations/reconciler-runs` (`server/store/skill_catalog.go`), the retried duplicate included, never a `409 run_conflict`; a post on a limit-0 token records nothing and logs `duplicate=true, soft_revoked=true, mismatch=false` with `rows_seen` 0, so a reconciler still posting on a rotated token is visible; `rows_seen` is the platform's reconciler rows whose `occurred_at` falls inside the run's own `[window_start, window_end]`, whatever token posted them (never the caller's token, so a run retried under a rotated token counts the same rows, and never the rows that ARRIVED since the previous run, so a re-scan of a period already reconciled counts its own rows rather than zero), and `mismatch` is that count differing from `rows_posted` on a FIRST record only: a `duplicate` stored nothing, so it never mismatches. `skill_reconciler_mismatch` counts the `mismatch=true` lines for policy 17's fourth condition |
| `skill catalog published` | INFO; WARNING when `stale_entries` > 0, when `skills` < `present_before` on an accepted publish, when `refused` is set, or on a limit-0 token | `source_repo`, `source_token_id`, `commit`, `skills`, `aliases`, `stale_entries`, `present_before`, `duplicate`, `soft_revoked`, `refused` | one per `PUT /v1/skill-catalog/{source_repo}` accepted, refused `409 catalog_shrink` (`refused = catalog_shrink`, nothing written) or answered on a limit-0 token (`soft_revoked = true`, `skills` 0, nothing written, so a publisher on a rotated token is visible rather than silent); `commit` is the sha or `other`; `stale_entries` counts the repository's present entries whose tree hash differs from their lineage head's or whose latest publish trails the head repository's by 14 days, so a WARNING there is the backend mirror behind the marketplace; `present_before` is what the repository held present when the transaction opened, so a catalog that shrank under the gate's half is a WARNING to look at (Runbook: skill catalog). The route's 401 and 403 ride `skill invocation rejected` instead; a malformed body is the publisher's bug and rides neither |
| `fleet summary` | INFO | `enrolled`, `reporting`, `silent`, `never_reported`, `capture_blocked`, `quarantine_devices`, `parked_devices`, `current`, `behind`, `unmanaged`, `version_unknown`, `drops`, `empties`, `sessions`, `empty_rate`, `answer_turns`, `answered`, `missing_answer_rate`, `published_build`, `server_build`, `manifest_present`, `ctas`, `muted` | one per five-minute evaluator tick, from the instance holding the lock whose tick found no other instance's tick younger than the interval on `fleet_ticks` (`server/fleet`), so several instances log one fleet; a machine whose (person, kind) is muted on `/admin/fleet` is left out of the gauge for that kind (`silent`, `behind`, `drops`, `capture_blocked`, `quarantine_devices`, `parked_devices`, `empties` and `sessions`) and `muted` counts the rows held back, so `enrolled` can exceed `reporting + silent + never_reported` by the muted silent machines |
| `fleet condition` | INFO | `email`, `device_id`, `kind`, `level`, `since`, `agent_version`, `detail` | one per machine condition in the latest reports; the census is never muted |
| `fleet drops` | WARNING | `email`, `device_id`, `reason`, `delta` | a machine's dropped-event counter rose since the last tick; held back while `drops_recorded` is muted for the person |
| `fleet silent` | WARNING | `email`, `device_id`, `since`, `hours`, `last_worst` | a machine that reported once and has not for a day; held back while `silent` is muted for the person |
| `fleet version_lag` | INFO | `email`, `device_id`, `agent_version`, `published_build`, `hours_behind` | a managed, reporting machine none of whose reports in the last hour named the manifest's commit; held back while `version_lag` is muted for the person |
| `fleet empty_start` | WARNING | `email`, `device_id`, `empties`, `total`, `rate`, `cwds`, `recipe` | a machine over the empty-start rule, with the launcher-probe recipe; held back while `empty_start` is muted for the person |
| `fleet missing_answer` | INFO | `day`, `agent_version`, `entrypoint`, `turns`, `answered`, `rate` | one per day, client build and entrypoint over the last two days |
| `release manifest missing` | WARNING | `channel` | said once when `latest/latest.json` is absent; no version lag is judged until it exists |

`/livez` answers `ok <version>` so an external check can read which build is
serving without `gcloud run services describe`; `/readyz` answers `ready`
only after a database ping and says nothing else on failure, on purpose.

### Metrics, policies and the uptime check

`monitoring/metrics/*.yaml` define log-based metrics under
`logging.googleapis.com/user/loop_sessions/`. The counters: `ingest_batches`,
`ingest_rejected_items`, `ingest_undecided`, `ingest_5xx` and `health_posts`
(both from the request log), `readyz_failed`, `server_catch`,
`slack_live_pass_failed`, `fleet_drops`, `fleet_empty_start`,
`derive_step_failed`, `derive_session_failed`. Counters count lines, labelled by the fields the file
extracts; the counts inside `ingest batch` are not summed by a metric and are
read from the lines themselves. The evaluator's figures are gauges sampled
once per tick, and a counter would count the line rather than the value, so
they go through DISTRIBUTION metrics with a `valueExtractor`:
`fleet_capture_blocked`, `fleet_quarantine_devices`, `fleet_parked_devices`,
`fleet_silent`, `fleet_empty_rate`, `fleet_behind`, `fleet_unmanaged` (all
from `fleet summary`), `fleet_missing_answer_rate` (from `fleet
missing_answer`, labelled by `agent_version` and `entrypoint`),
`derive_stalled_minutes` (from `derive pass`) and `derive_rows_per_sec` (from
the `event_keys` step's `derive step` line). A policy on one of them reads
the sample back with `ALIGN_PERCENTILE_99` over one alignment period; Cloud
Monitoring refuses `ALIGN_DELTA` on a gauge, which is why nothing here is
one.

`monitoring/policies/*.json` are the alert policies, one file per alert,
each labelled `app=loop-sessions` and a `tier` (`infra`, `fleet` or `data`).
Every policy carries a markdown runbook in `documentation.content` with the
commands to run, and a link to the matching heading in this file. The infra
tier as shipped:

| File | Fires when |
|---|---|
| `01-ingest-5xx-ratio` | 5xx share of all requests above 2% for 5 min |
| `01b-ingest-5xx-absolute` | 5 or more 5xx on `/v1/events` or `/v1/health` in 5 min (the low-traffic guard) |
| `03-ingest-undecided` | any `store returned no verdict` line |
| `06b-health-heartbeat-absent` | no accepted health report for 20 min (the fleet posts about 336 an hour) |
| `10-cloudsql-disk-warning`, `10-cloudsql-disk-error` | disk above 80% / 90% for 30 min |
| `10b-cloudsql-disk-growth` | more than 5 GB of growth in a day (baseline 0.7) |
| `11-cloudsql-connections-warning`, `11-cloudsql-connections-error` | backends above 300 / 360 for 5 min against a budget of 367 of 397 |
| `11b-cloudsql-resources` | CPU above 80% or memory above 90% for 15 min, txid space above half for 1 h, any deadlock |
| `12-cloudrun-instances` | 8 or more active instances of maxScale 10 for 15 min |
| `12b-cloudrun-memory` | container memory p99 above 80% of 512Mi for 10 min |
| `12c-cloudrun-saturation` | concurrency p99 above 32 of 40, or latency p99 above 5 s, for 10 min |
| `13-readyz-failed` | 3 or more failed readiness checks in 5 min |
| `13b-uptime-readyz` | the external `/readyz` check failing from 3 regions |
| `17-skill-auth-rejected` | 20 or more `unauthenticated` skill posts in 15 min, any post on a `revoked` or `expired` token in an hour, or more than 5 derived rows displacing API rows in an hour (WARNING) |
| `17c-skill-store-error` | any `skill invocation store failed` line in 15 min, or 5 or more 5xx on the skill routes in 5 min (ERROR) |
| `17d-skill-token-expiring` | the nearest live source token expiry under 14 days for an hour (WARNING) |
| `slack-live-pass-failed` | the mirror's live pass failing for 15 min |

The fleet tier notifies the fleet Slack channel
(`SLACK_FLEET`) and the owner; every row's runbook is a heading below and
the same CTA on `/admin/fleet`:

| File | Fires when |
|---|---|
| `02-ingest-rejection-rate` | batches carrying a rejected item above 10% of batches for 15 min |
| `04-fleet-capture-blocked` | any machine with `capture_blocked` for 15 min (CRITICAL: events are being dropped as it fires) |
| `04b-fleet-quarantine` | any machine with a non-empty quarantine for 30 min (open on day one: the historical backlog) |
| `04c-fleet-parked` | any machine holding parked items for 30 min |
| `05-fleet-drops` | any `fleet drops` line in an hour |
| `06a-fleet-silent` | 3 or more machines silent for a day, for 30 min |
| `07-fleet-empty-start` | fleet empty-start rate above 0.6 for an hour, or any machine over the per-device rule |
| `08-fleet-missing-answer` | missing-answer rate per client build above 10% for 6 h. The number is `fleet.MissingAnswerAlertRate`, shared with the page's CTA and pinned by a test; it is 10 and not 5 because the fleet's chronic residual differs by entrypoint while a build that has stopped capturing reads 100% |
| `09-fleet-behind-warning` | any managed machine behind the published build for a day |
| `09b-fleet-behind-error` | 7 or more machines (a quarter of the 25-machine baseline) behind for two days |
| `15-server-catch` | 10 or more server-side scrub catches in an hour |
| `17e-skill-rate-limited` | a platform's skill posts rate limited for 10 min, one condition per platform |
| `18b-skill-beacon-silent` | a live emitter token's series silent above 1440 counted minutes (`devin`, `capy`, `codex` beacons), 10080 (the `claude_code` cloud hook) or 2880 (a reconciler), each for 30 min, `canary` and `laptop` excluded |

The data tier (`INFRA` channel, since it is the server's own pipeline):

| File | Fires when |
|---|---|
| `16-derive-step-failed` | any `derive step failed` line in 15 min (WARNING) |
| `16b-derive-stalled` | `pass.stalled_minutes` above 120 for 30 min (WARNING) |
| `18-skill-derive-silent` | either derived skill series (`agent`, `user`) silent above 1440 counted minutes, the re-derive queue above 100, or its oldest row above 1440 minutes, each for 30 min (WARNING, notifies the fleet channel) |
| `17b-skill-invalid-payload` | more than 50 skill posts refused on a field rule in an hour (WARNING) |

`monitoring/uptime/readyz.json` describes the external check on `/readyz`
from three regions every minute. `/readyz`, not `/healthz`: Google's front
end swallows `/healthz` on a `run.app` URL and serves its own 404, so a check
on it would fail forever. `monitoring/uptime/readyz.json` names the host as
`__PUBLIC_HOST__`, which `apply.sh` renders from `PUBLIC_URL`; `apply.sh`
also needs `RELEASE_BUCKET` and `ANALYTICS_BUCKET` exported, for the
policies' documentation.

`monitoring/channels.env` holds the notification channel ids the policies
are rendered with: `SLACK_INFRA` (the infra-tier Slack channel),
`SLACK_FLEET` (the fleet-tier Slack channel, posting as the
server's own Slack bot; `apply.sh` refuses to run while it is empty,
naming it) and `EMAIL_OWNER`. `monitoring/assets_test.go` checks every policy parses, carries
the labels, the documentation and a channel, names only metrics that have a
yaml, and links only to headings that exist in this file.

### Applying it

See the runbook "apply monitoring" below. In short: fill `channels.env`, run
`monitoring/apply.sh --dry-run`, read what it would create, run it without
the flag, then confirm in the console that the policies list under
`app=loop-sessions`.

## Runbook: apply monitoring

`examples/deploy-gcp/monitoring/apply.sh` is update-or-create for everything it
touches, so it is safe to run again after any edit and safe to run twice.

Before the first run, once, by a human:

1. Create the fleet tier's Slack notification channel (`SLACK_FLEET`)
   through the API, not the console: the Slack channel type accepts any
   token that can post, so it carries the server's own bot token
   (Secret Manager `loop-sessions-slack-bot-token`; the bot must be a member
   of the channel, `conversations.join`). To recreate it, read the token
   into a file and POST `{type: "slack", labels: {channel_name, auth_token}}`
   to `projects.notificationChannels.create`, then paste the numeric id into
   `channels.env` (`gcloud beta monitoring channels list --filter='type=slack'
   --format='value(name,displayName)'` shows it). The fleet tier uses it; the
   infra tier uses a second channel, `SLACK_INFRA`. While either is empty
   `apply.sh` refuses to start and names it.
2. Create the owner's email channel and paste its numeric id into
   `channels.env` as `EMAIL_OWNER`:
   `gcloud beta monitoring channels create --type=email --channel-labels=email_address=you@example.com --display-name=Owner --format='value(name)'`

Then, from the repository root, with `gcloud` authenticated as someone
holding `roles/monitoring.editor` and `roles/logging.configWriter`:

```bash
examples/deploy-gcp/monitoring/apply.sh --dry-run   # reads only, descriptor GETs included; prints create/update per asset, present/absent per descriptor
examples/deploy-gcp/monitoring/apply.sh             # applies
examples/deploy-gcp/monitoring/apply.sh --dry-run   # expect: every metric and policy reported as update, the uptime check as exists, every descriptor present
```

What it does, in order: metrics (describe, then update or create), the uptime
check (list by display name, create if absent, read its id), a wait of up to
60 s for every metric descriptor a policy names to propagate, then policies
(render the placeholders, refuse the run if any is left, list by
`userLabels.app` and `displayName`, update or create). It refuses to start
while any channel id in `channels.env` is empty or is not a bare resource id
(letters, digits, `.`, `_`, `-`), because the ids are spliced into the
policies with `sed` and a stray `|` or `&` would corrupt one silently.

The descriptor wait is a GET on
`monitoring.googleapis.com/v3/projects/<project>/metricDescriptors/<metric type>`
with the type unescaped, slashes and all: the API answers 400 "Invalid metric
name" to the `%2F` form, and a first version of the script sent that, so
every real apply died in the wait after the metrics and the uptime check had
already gone in. The dry run makes the same GET (it is a read) and reports an
absent descriptor instead of waiting for it, since absent is the expected
state before the first apply. Any status other than 2xx or 404 (a bad token,
a missing role) fails the run at once rather than being waited on for 60 s
and reported as propagation.

After the first apply, check the two things only a real project can
confirm. The uptime check's matcher:
`gcloud monitoring uptime describe <id>` shows
`contentMatchers[0].matcher: CONTAINS_STRING` (apply.sh prints the id on
its `uptime ... exists (<id>)` line the next time it runs). The metric labels read from
numeric fields (`status` and `had_rejects` on `ingest_batches`, `status` on
`ingest_5xx`): the LogMetric reference says an extracted value is converted
to the label's type, so after a few minutes of traffic a Metrics Explorer
look at `ingest_batches` grouped by `status` shows values such as `200`. A
label that is empty on every point means the conversion did not happen and
the yaml has to read the field another way.

If it fails: a metric that "already exists" on create means somebody created
it by hand without the label; delete it in the console and re-run. A policy
create rejected with a metric type "not found" is the propagation wait
running out; re-run. Two policies with the same display name is a console
experiment; delete the extra and re-run.

## Runbook: ingest 5xx

Alerts: `01-ingest-5xx-ratio`, `01b-ingest-5xx-absolute`, and the Cloud Run
policies (`12-*`) and the heartbeat (`06b`) point here too.

Every 5xx on `/v1/events` keeps a batch on a laptop and the fleet retries it
against the same instance, so a 5xx rate is the leading edge of a capture
backlog. Read the request log first, then the cause:

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND httpRequest.status>=500' --limit=20 --freshness=1h
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="ingest request failed"' --limit=20 --freshness=1h --format='value(jsonPayload.op,jsonPayload.error)'
```

`op` says which half failed: `upsert events` and `verify device` are the
database (see the cloudsql runbook), `read body` is a batch over the hard
limit (8 x 4 MiB; the agent sends an over-limit item alone and would retry
forever, which is why the advertised cap rejects per item instead), `put
health report` is a report the store refused (answered 5xx on purpose so the
fleet page never shows a machine as reporting when it is not).

If a deploy landed in the last hour, roll back first and diagnose second:

```bash
gcloud run revisions list --service=loop-sessions --region="$REGION"
gcloud run services update-traffic loop-sessions --region="$REGION" --to-revisions=<previous>=100
```

Which build is serving is on `/livez` (`ok <version>`); which build each
laptop runs is the `agent_version` label on `ingest_batches` and `ingest_5xx`.

## Runbook: undecided

Alert: `03-ingest-undecided`, on the `store returned no verdict` line.

The handler admitted an item, handed it to the store, and the store's answer
named it in neither the inserted, duplicate nor rejected list. The handler
claims nothing about it, so the agent keeps the item and redelivers it every
cycle; an agent on an older client
gives up after eight such answers and quarantines it under `max_attempts`, a
new one parks it and retries daily. Either way the item is not stored and
this line is the only evidence.

It is a server bug (the two halves disagree about what a batch contains),
never the laptop's:

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="store returned no verdict"' --limit=20 --freshness=6h --format='value(jsonPayload.event_id,jsonPayload.event_type,jsonPayload.email,jsonPayload.device_id)'
```

Take the `event_id` to the store's own lines for the same request (they
share the trace: click the request in the Logs Explorer and expand its
entries) and to `SELECT id, type, session_id FROM events WHERE id = '<event_id>'`
through the proxy. A row that exists means `UpsertEvents` stored it and did
not report it (a bug in the result assembly); no row means it was dropped
between `prepare` and the insert. The device is on `/admin/fleet`; once the
bug is fixed the next delivery of the same item stores it, and on a new
client `loop-sessions doctor --redrive` pulls the parked copy forward.

## Runbook: cloudsql

Alerts: `10-*`, `10b`, `11-*`, `11b`, `12c-cloudrun-saturation`,
`13-readyz-failed`.

The instance is `loop-sessions` (Postgres 16, `db-custom-2-7680`, 100 GB
PD_SSD with autoresize and no cap, zonal, PITR on). Connection budget:
`max_connections` 400, 3 reserved, so 397 usable; the service holds up to
10 instances x 36 = 360, the export job 2, and 5 are kept for the proxy
sessions an operator opens. Statement timeout is 30 s on every pooled
connection (`server/app/config.go`, `setStatementTimeout`); the passes that
need longer take it with `store.WithStatementTimeout` inside their own
transaction.

```bash
gcloud sql instances describe loop-sessions --format='value(state,settings.dataDiskSizeGb,settings.storageAutoResizeLimit,settings.tier)'
# Through the proxy (section 5):
psql -h 127.0.0.1 -p 5433 -U loop_sessions -d loop_sessions -c "SELECT state, count(*) FROM pg_stat_activity GROUP BY 1"
psql -h 127.0.0.1 -p 5433 -U loop_sessions -d loop_sessions -c "SELECT relname, n_dead_tup, last_autovacuum FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 5"
```

- Backends above 300: something is fanning out. `idle in transaction` rows
  are a leaked transaction, with one exception: during a deploy, a single
  row whose query is `SELECT pg_advisory_xact_lock($1)` is the migration
  runner holding its lock (runbook: migrations) and goes away when the run
  ends. pgx binds the key as a parameter, so the number is never in
  `pg_stat_activity.query`; match the `$1` text as written, or look the
  holder up by key through `pg_locks` (runbook: migrations, step 1).
  The instance-count alert says whether Cloud Run scaled
  up to produce the rest. Above 360, shed load first
  (`gcloud run services update loop-sessions --region="$REGION" --max-instances=4`)
  and restore 10 afterwards.
- Disk above 80%: autoresize will handle the space; the number to act on is
  the growth rate. Above 5 GB a day, find the device from the `ingest batch`
  lines (`jsonPayload.bytes` summed by `device_id`); a re-walk that
  duplicated a corpus shows as one device sending far more than the rest.
- CPU or memory: enable Query Insights before chasing it
  (`gcloud sql instances patch loop-sessions --insights-config-query-insights-enabled`).
  Statements the 30 s timeout cut off appear in `ingest request failed`
  lines with `SQLSTATE 57014`.
- txid utilization above half: autovacuum is not keeping up (it has never
  run on `events`); schedule `VACUUM (ANALYZE) events` in the derive window.
- Readiness failing: the instance cannot reach the socket. Check the
  instance state above and that the Cloud Run revision still carries the
  `run.googleapis.com/cloudsql-instances` annotation for it.

## Runbook: migrations

The server runs `server/store/migrations/*.sql` at boot, before it listens
(`store.Migrate`, `server/store/store.go`). What happens, so a boot failure
reads correctly:

1. One advisory lock (`pg_advisory_xact_lock`, key `migrationLockKey`) is
   taken in a transaction of its own and held for the whole run. A second
   instance of a rolling deploy waits on it for up to 60 s, then fails its
   boot with `migration lock: another server has held it for longer than
   1m0s`. That message means the first instance's run is stuck or slow, not
   that the second is broken; look at the first instance's log.
   For as long as the run lasts the holder is one `idle in transaction` row
   in `pg_stat_activity` whose query is `SELECT pg_advisory_xact_lock($1)`.
   The key (`migrationLockKey`, 7266794526548561) is bound as a parameter,
   so it is not in the query text and a filter on the number finds nothing.
   The lookup that does not depend on the text goes through `pg_locks`,
   which shows a 64-bit advisory key as `classid` (high 32 bits) and
   `objid` (low 32 bits):

   ```sql
   SELECT l.pid, a.state, a.query
     FROM pg_locks l JOIN pg_stat_activity a USING (pid)
    WHERE l.locktype = 'advisory'
      AND (l.classid::bigint << 32) + l.objid::bigint = 7266794526548561;
   ```

   That row is the lock, not a leaked transaction, and it is the one row the
   cloudsql runbook's `idle in transaction` rule exempts. The instance sets no
   `idle_in_transaction_session_timeout` (the flag is 0) and must not: the
   timeout would terminate the holder mid-run, which drops the lock without
   failing the run, and a second instance could then apply files alongside
   the first.
2. The ledger table `schema_migrations` is created (committed on its own).
3. Each file runs in its own transaction with `SET LOCAL lock_timeout = '5s'`
   and `SET LOCAL statement_timeout = '60s'`, and its ledger row commits with
   it. A file whose `ALTER TABLE` cannot take its table lock inside 5 s (an
   ingest transaction holds the table) is rolled back and retried, up to four
   times with a jittered pause, inside a 25 s lock budget for that file. Past
   the budget the boot fails with `migration <file> could not take its lock
   within 25s (<n> attempts); ingest is holding the table it alters`. The
   budget bounds the lock wait and nothing else: a file whose own SQL runs
   longer than 25 s is not cut off by it. The 60 s is the ceiling on any one
   statement of the file (it replaces the pool's 30 s request default for
   the transaction), and a statement that runs past it fails the boot with
   `migration <file> ran past the 60s statement ceiling`, which is the file
   and not ingest: split it, or move the work to a runner step.

The startup probe allows 120 s (`cloudrun.yaml`, `failureThreshold: 60` at
2 s), which covers the 10 s ping, the 60 s lock wait and one file's lock
budget with room to spare. Both budgets are per file, so a boot that ships
several files which all lose a lock race, or one slow file after a full lock
wait, can be killed by the probe. That is not a crash loop: every file
commits on its own, so the next boot skips what landed and resumes at the
first file the ledger lacks. Move the probe number if any of these budgets
moves.

Rules for a migration file, and why: catalog-only statements (ALTER TABLE
ADD COLUMN, CREATE TABLE, CREATE FUNCTION); no `CREATE INDEX` on a large
table (build it `CONCURRENTLY` from the derive runner, outside any
transaction); no statement that reads `events` or `messages` (each is
gigabytes; the one bounded exception is documented in its own file); every
statement inside the 60 s ceiling on production sizes, measured cold and
written in the file's comment (an `UPDATE` over all of `sessions` counts,
and work that needs longer is a runner step, not a migration); every
statement idempotent (`IF NOT EXISTS`, `ON CONFLICT DO NOTHING`), because
the ledger is an audit trail and not the thing that makes re-running safe.

The rollback floor. A rollback is a traffic change (section 9) and does not
undo a migration, so every file must leave the previous revision working:
additive columns with defaults, constraints the old code's writes satisfy,
functions the old code does not need. Each file states its own floor in a
comment: the earliest revision that still runs correctly against the schema
it leaves behind. A file that lowers the floor (a `CHECK` the old code's
writes would violate, say) has to say so and be deployed as its own step,
with the revision before it retired first.

If a boot fails on a migration:

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="cannot start"' --limit=5 --freshness=1h --format='value(jsonPayload.err)'
# Through the proxy: what the ledger says landed
psql -h 127.0.0.1 -p 5433 -U loop_sessions -d loop_sessions -c "SELECT name, applied_at FROM schema_migrations ORDER BY name"
```

A lock-budget failure clears itself on the next boot once ingest lets go of
the table; Cloud Run retries the revision on its own. A file that fails on
its SQL, or that ran past the statement ceiling, fails the same way every
boot, and the previous revision keeps serving because the new one never
passes its probe: fix the file (for the ceiling, split it or move the work
to a runner step), or apply it by hand through the proxy (section 5) and
insert its ledger row, then redeploy.

## Runbook: capture blocked

Alert: `04-fleet-capture-blocked` (CRITICAL). CTA row `capture_blocked` on
`/admin/fleet`.

A laptop's spool is refusing writes because its disk is below the client's
floor, and every hook that fires on that machine is dropping its event now.
This is the one fleet condition where the loss is in progress while you
read the alert, which is why it is the only CRITICAL in the fleet tier.

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="fleet condition" AND jsonPayload.kind="capture_blocked"' --freshness=15m --format='value(jsonPayload.email,jsonPayload.device_id,jsonPayload.detail,jsonPayload.agent_version)'
```

Ask the person to free disk. On their machine `loop-sessions status` says
how far below the floor the volume is and what the spool holds; the current
client keeps an absolute floor, while anything older refuses below 5% of
the disk and needs `loop-sessions daemon --upgrade-now` as much as it needs
space. Once the floor clears, capture resumes on the next hook and the
condition leaves the next report; the sessions that ran while it was
blocked carry a capture-loss banner in the reader and stay incomplete.

## Runbook: quarantine

Alerts: `04b-fleet-quarantine` (ERROR), `02-ingest-rejection-rate`
(WARNING). CTA row `quarantine_nonempty` on `/admin/fleet`, ranked first.

Quarantined items are ones the server refused for good (the reason is in
the file name on the laptop), or, on a client older than the quarantine fix, items it
gave up on after eight undecided verdicts. Nothing retries them by itself.
When the quarantine fix is first promoted, every machine that hit the old
client's attempt cap still holds quarantine, so this alert can open on day
one and the CTA list keeps these rows above version lag on purpose: they
are the backlog the fix exists to burn down.

On the person's machine, in this order:

```bash
loop-sessions doctor --replay-quarantine   # posts one quarantined item on its own and prints the server's verdict
loop-sessions doctor --redrive             # if the verdict was accepted: queue the rest again
```

Accepted means the server has since been fixed for that shape, and the
redrive stores the rest. Rejected with a reason means the items are bad:
`no_session_id` and `no_id` are a client bug on that build (upgrade, then
redrive), `batch_too_big` is a cap mismatch (upgrade), a scrub reason is
content the server will never take, and the person deletes the quarantine
directory the doctor names. For the rejection-rate alert, group the reasons
first:

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="ingest item rejected"' --freshness=1h --format='value(jsonPayload.reason,jsonPayload.event_type,jsonPayload.email,jsonPayload.device_id)' | sort | uniq -c | sort -rn
```

While a person is away, mute the row on `/admin/fleet` with an until and a
note. A mute on a (person, kind) moves the row to the muted list on the
page, holds back that machine's alert line and leaves it out of the summary
gauge for that kind until the mute lapses, so the alert on the line or the
gauge stays quiet for exactly what was silenced; the `fleet condition`
census line still names the machine, and the muted list shows what is held
back and until when. The person `*` is the fleet: a mute filed under it
silences the kind for every machine, and it is how a row that names nobody
(the missing-answer rate) is muted; the page files that row's mute under
`*` by itself.

## Runbook: parked

Alert: `04c-fleet-parked` (ERROR). CTA row `parked` on `/admin/fleet`.

Parked items are ones the server answered for neither way eight deliveries
in a row. The current client parks them instead of quarantining them and
redrives them when its binary changes and once a day, so they are not lost.
Eight undecided verdicts for one item is a server bug (the `undecided`
runbook), and until it is fixed the daily redrive parks them again.

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="store returned no verdict"' --freshness=1d --limit=20 --format='value(jsonPayload.event_id,jsonPayload.event_type,jsonPayload.email,jsonPayload.device_id)'
```

Fix and deploy the server first. Then, on the person's machine,
`loop-sessions doctor --redrive` pulls the parked items forward now rather
than at the next daily redrive. Who holds parked items is the `parked` row
on `/admin/fleet`; a client older than the quarantine fix cannot report the count and
its row reads unknown, never zero, while the current client leaves the
field out of its report when the count is zero and the server reads that
absence as zero (the same rule holds for `empty_starts_24h` and the `Empty
starts (24 h)` column).

## Runbook: drops

Alert: `05-fleet-drops` (ERROR). CTA row `drops_recorded` on `/admin/fleet`;
the reader shows a capture-loss banner on sessions the machine ran around
the drop.

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="fleet drops"' --freshness=1h --format='value(jsonPayload.email,jsonPayload.device_id,jsonPayload.reason,jsonPayload.delta)'
```

The reason decides the path. `disk_full` is the disk floor: the capture
blocked runbook. `queue_overflow` is the outbox at its cap evicting
oldest-first, which means the machine captures faster than it delivers: the
backlog runbook. `hook_abandoned` is a hook that hit its self-timeout; the
the current client client raised it and made the write ledger-first, so on an older
build the fix is `loop-sessions daemon --upgrade-now`. `parked` is the
eighth undecided verdict: the parked runbook. `rejected:<reason>` is the
server refusing an item: the quarantine runbook. The evaluator reports
drops as deltas between ticks from the hourly ledger (`health_hourly`), so
a counter that was already high when a machine enrolled is never reported
as new loss and a restart never counts the same drop twice.

Three counters in the spool's `dropped` map are not losses. The map is the
durable place for a count that has to outlive the process that observed it,
so it also carries: `recovered_from_transcript`, events the recovery pass
read back out of a transcript after a hook was abandoned, which reached the
server; `parked`, an item the server left undecided too many times, moved
to the parked directory and redriven later, which has its own count, its
own condition and `04c-fleet-parked`; and `tmp_swept`, a stale temp file
from a write that never completed, so nothing was ever queued to lose.

`internal/health.NotLosses` names them, the `drops_recorded` condition
skips them, the client's own `doctor` output reads the same list, and the
rollup is passed it as a parameter rather than repeating it, so none of the
four can come to disagree. They still travel in the report and still reach
the fleet page: a machine leaning on its recovery pass is worth seeing, it
is just not degraded and not alerting for it.

The condition is also a rise, not a level. These counters live as long as
the spool directory, so a level says only that the machine ever lost
something: one of those two machines was carrying 3,010 `disk_full` from an
earlier day with 127 GiB free, and had read degraded ever since. A report
is compared with the previous one from the same daemon; with no previous
report (a first report, or a new daemon's first) nothing is raised, because
re-reading a lifetime total as news at every daemon start is what made the
condition permanent. Nothing is lost by that: the fleet drop metric and
`05-fleet-drops` are built on the server's own delta between reports, which
spans processes and restarts.

## Runbook: backlog

Conditions `backlog_growing` (degraded), `backlog_stalled` (critical) and
`spool_near_cap` (degraded), as CTA rows on `/admin/fleet`; there is no
policy of their own, because a fleet-wide delivery failure shows first as
ingest 5xx (`01-`, `01b-`) and a single machine's shows as drops (`05-`)
once the outbox evicts.

The spool on a laptop is arriving faster than it delivers (`growing`), has
not delivered anything for a while (`stalled`), or is near the cap past
which it evicts oldest-first (`near_cap`). Check the server before the
laptop:

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND httpRequest.status>=500' --limit=20 --freshness=1h
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="ingest batch" AND jsonPayload.email="<email>"' --limit=10 --freshness=1h --format='value(timestamp,jsonPayload.items,jsonPayload.accepted,jsonPayload.status)'
```

Batches arriving and being accepted means the laptop is catching up on its
own; give it the hour. No batches from that device while others deliver
means the laptop's network or VPN: ask the person to run
`loop-sessions status` (it prints the last delivery attempt and its error)
and, if the daemon is not running, `loop-sessions install` puts it back. A
server that is healthy and a daemon that is running with a stalled outbox
is a bug: collect `~/.loop/sessions/logs/` from the machine.

## Runbook: enrol

Condition `never_delivered` (critical) as a CTA row on `/admin/fleet`, and
the `never_reported` count in `fleet summary`.

A device that enrolled and never delivered a batch, or a machine on the
roster that never sent a health report, is a failed install: the device
token was revoked, the daemon never started, or the person signed in and
closed the terminal before the setup finished.

```bash
# Through the proxy: the device rows for the person
psql -h 127.0.0.1 -p 5433 -U loop_sessions -d loop_sessions -c "SELECT id, hostname, agent_version, enrolled_at, last_seen_at, revoked_at FROM devices WHERE email = '<email>' ORDER BY enrolled_at DESC"
```

A `revoked_at` that is set means somebody revoked it (the People page or a
re-enrol); a `last_seen_at` that never moved means the daemon never came up.
Either way the person runs `loop-sessions install` on the machine, which
signs in again, enrols a device and starts the daemon; the old device row
stays for the audit trail and stops counting once revoked.

## Runbook: hooks

Condition `capture_stale` (degraded) as a CTA row on `/admin/fleet`.

The harness is in use on the machine (its transcripts change) and nothing
is being captured: a harness upgrade rewrote the settings file and the
capture hooks went with it, or somebody removed them by hand. Sessions run
in that state are only recoverable by a backfill from the transcript.

On the person's machine:

```bash
loop-sessions install --hooks-only   # puts the capture hooks back without signing in again
loop-sessions status                 # confirms the hooks are registered and the daemon is up
```

Once the hooks are back, `loop-sessions backfill` walks the transcripts the
hooks missed (`loop-sessions backfill --session <id>` for one session; the
reader's head-state banner carries that command for a session whose start
was not captured).

## Runbook: silent

Alerts: `06a-fleet-silent` (WARNING), `06b-health-heartbeat-absent`
(CRITICAL, the fleet-wide case). CTA row `silent` on `/admin/fleet`.

A machine that reported once and has not for a day is asleep, off, or its
daemon died. One or two on any given day is a weekend; three at once is the
pattern this alert exists for, and the pattern that matters is a release
whose daemon dies on start.

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="fleet silent"' --freshness=15m --format='value(jsonPayload.email,jsonPayload.device_id,jsonPayload.since,jsonPayload.hours,jsonPayload.last_worst)'
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="fleet condition" AND jsonPayload.email="<email>"' --freshness=3d --limit=5 --format='value(timestamp,jsonPayload.agent_version,jsonPayload.kind)'
```

If the silent machines share an `agent_version` on their last lines, that
build is the suspect: check the release (section 7b) and the upgrade
runbook, and republish the previous build if it is the daemon. Otherwise
ping the people. Past 72 hours, `loop-sessions install` on the machine puts
the daemon back; `loop-sessions status` says whether it is running at all.
A silent machine is never counted behind the published build and gets no
`version_lag` row: a machine that is off cannot upgrade, and its version
state stays on its row for when it comes back. A `silent` mute holds back
the line and the `silent` gauge for that person; the row stays on the page.

## Runbook: upgrade

Alerts: `09-fleet-behind-warning` (WARNING), `09b-fleet-behind-error`
(ERROR), `15-server-catch` (WARNING, when the devices are on an old build).
CTA row `version_lag` on `/admin/fleet`, with the lag in hours.

The evaluator compares each machine's build (`agent_commit` in a version 2
health report, `agent_version` in a version 1 one) with the commit in the
release manifest `latest/latest.json`: the same commit is current, another
sha is behind, and anything that is not a sha (`dev`, `e2e-1`, a `-dirty`
suffix) is unmanaged, counted in its own column and never alerted. No
manifest published means one `release manifest missing` WARNING and no lag
judged at all. The daemon self-upgrades at its next check, so a machine
still behind a day after the publish has not run one.

The judgement reads the last hour of a machine's reports, not only its
newest one. A laptop keeps running the old daemon until the session that
started it ends, while the new binary is already on disk and every new
session spawns a daemon from it, so for hours or days the machine's reports
alternate between the two builds. A machine that named the published build in any report of
the last hour is current, and the fleet page says which build it is still
running alongside it ("running 23713ea alongside the current client"); it is behind
only when no report in that hour named the published build. A silent
machine is not judged behind at all (the silent runbook); a `version_lag`
mute holds back the line and the `behind` gauge for that person.

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="fleet version_lag"' --freshness=15m --format='value(jsonPayload.email,jsonPayload.agent_version,jsonPayload.published_build,jsonPayload.hours_behind)'
curl -sS __PUBLIC_URL__/dl/latest/latest.json
gsutil ls -L gs://__RELEASE_BUCKET__/latest/
```

One machine behind: ask the person to run `loop-sessions daemon
--upgrade-now`, which checks the release host now, ignoring the throttle,
and replaces the binary; the `upgrade` block of its next health report says
what happened (`result`, `skipped_reason`, `error`). A quarter of the fleet
behind is the upgrade path itself: the manifest and the artefacts it names
disagree, the checksum fails, or the new binary dies on start and the
daemon rolls back. Check the manifest against the bucket listing above and
republish (section 7b), from `builds/<sha>/` if the previous build has to
come back. A manifest older than the server build (the fleet page's Latest
build against its Server build line) means section 7b was skipped.

## Runbook: empty sessions

Alert: `07-fleet-empty-start` (WARNING). CTA row `empty_start` on
`/admin/fleet`; the list hides these sessions by default (filter `empty`
shows them).

A lifecycle-only session is a `claude` process that started and ended
before a first prompt: a GUI host spawning one per open project, or a
script running `claude -p`, `claude -c` or `claude --resume` with stdin
closed. The server classifies them `empty`/`aborted` and hides them, so
nothing is broken server-side; the rule that names a person is twenty or
more in a day and at least half of that machine's sessions, and the people
generating them usually do not know.

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="fleet empty_start"' --freshness=1h --format='value(jsonPayload.email,jsonPayload.empties,jsonPayload.total,jsonPayload.rate,jsonPayload.cwds)'
```

Send the person the launcher probe, which the line carries as `recipe`: a
second `SessionStart` hook in `~/.claude/settings.json` that appends the
parent process, bundle id, terminal and entrypoint to
`~/.loop/sessions/logs/launcher.log`:

```bash
printf '%s %s ppid=%s %s bundle=%s term=%s ep=%s\n' "$(date -u +%FT%TZ)" "$PWD" "$PPID" "$(ps -o comm= -p $PPID)" "$__CFBundleIdentifier" "$TERM_PROGRAM" "$CLAUDE_CODE_ENTRYPOINT" >> ~/.loop/sessions/logs/launcher.log
```

A day of that log names the launcher (a `com.anthropic.claudefordesktop`
bundle id is the Desktop app's Code tab with a project open on the folder
in `cwds`; a script shows as its interpreter). Then mute the row on
`/admin/fleet` with the date in the note, so the same name does not page
every day while the person changes the script: the mute holds back the
`fleet empty_start` line and leaves that machine's sessions out of the
fleet's `empties` and `sessions` gauges, so the rate alert reads the rest
of the fleet.

## Runbook: missing answers

Alert: `08-fleet-missing-answer` (ERROR, ships disabled). The reader shows
"no answer captured" in the answer slot of an affected turn, and
`/v1/repair` lists the affected sessions to the device that owns them.

A hook-captured human turn with no captured answer is the defect the
the current client client fixes (it reads `last_assistant_message` at the stop hook);
every older client produces them for every turn, so before the rollout the
rate is a known state, and the policy ships `enabled: false`.

Enable it once two things are true: the fleet is on a client that captures
the answer, and the server counts the turns that could have answered and
only those.

What the cohort leaves out, and why each one had no answer to lose: a turn
still in progress; a turn the person interrupted; and a turn whose only
captured event is its prompt, when the session answered something else.
That last kind is a prompt the harness folded into the next turn. It is
excused only in a session that answered something, because the same shape
is what a client produces when its hooks stop firing after the prompt, and
excusing it unconditionally would let such a build emit no cohort row at
all, where a threshold cannot fire.

Cohorts under twenty turns do not reach the alert at all: the floor is in the metric's filter (`jsonPayload.turns>=20`), so the page and the log keep every cohort and only the alert is spared the small ones. Read `turns` in an incident anyway.
Overnight a build can have two or three
turns, where one unanswered turn reads as 33%, so under twenty turns treat
the rate as a hint and open the sessions rather than paging anyone.

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="fleet missing_answer"' --freshness=6h --format='value(jsonPayload.day,jsonPayload.agent_version,jsonPayload.entrypoint,jsonPayload.turns,jsonPayload.answered,jsonPayload.rate)'
```

The line is per day, client build and entrypoint. The CTA row is the
fleet's rather than a person's, so it names nobody to ask and its mute is
filed under the person `*`. A high rate on an old build is version lag
(the upgrade runbook: `loop-sessions daemon --upgrade-now`). A high rate on the published build is a regression in the
client: open a session from that cohort in the reader, read the answer slot,
and compare the transcript (`loop-sessions backfill --session <id>` on the
machine imports the transcript's copy of the answer, which the reader then
shows from the transcript origin). The turns already captured without an
answer are repaired by the client's own repair pass, which asks
`/v1/repair` for the list and backfills from the transcripts.

## Runbook: derive

Alerts: `16-derive-step-failed` (WARNING), `16b-derive-stalled` (WARNING).
The runner's own version attribute on every line is `derive_version`
(`version` on a line is the server build, as on every other line).

The derive runner (`server/store/runner.go`) rebuilds the derived tables
(`turns`, `turn_events`, session facets) in steps, each in bounded batches
under an advisory lock, and writes a `derive pass` line per pass. A step
that fails as many times as the runner allows stops advancing and writes
`derive step failed` with the statement that lets it retry; a step that is
alive but not committing shows as `pass.stalled_minutes`.

```bash
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="derive step failed"' --freshness=1h --format='value(jsonPayload.step,jsonPayload.attempts,jsonPayload.error,jsonPayload.recover)'
gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="derive pass"' --freshness=1h --limit=5 --format='value(timestamp,jsonPayload.pass.deferred,jsonPayload.pass.waiting,jsonPayload.pass.until,jsonPayload.pass.failed,jsonPayload.pass.stalled_minutes)'
```

Read the error first. A statement timeout or a deadlock is transient: run
the `recover` statement through the proxy (`UPDATE derive_jobs SET attempts
= 0 WHERE version = <n> AND step = '<step>'`) and the runner tries again on
its next pass. A constraint violation or a decode error is a bug in the
step: fix it, deploy, then reset attempts. A pass that is `deferred` on
every instance is an advisory lock left behind by a connection that died:

```sql
SELECT l.pid, a.state, a.query FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory';
SELECT pg_terminate_backend(<pid>);
```

`waiting` with a long `until` is the body window (bodies are read only in
the window the runner is configured for) and clears on its own. The
derived tables are rebuilt, never hand-edited: a wrong row is fixed by
fixing the step and letting it run.

Until the runner's `messages_kind` step has visited a session, a user-role
row stored before migration 0014 carries the kind `''`, and the reader
shows it as the person's bubble (the fold makes the same choice, so the
two agree). A harness wrapper rendered as a bubble on a session whose
`derive pass` has not yet reached it is this transitional state, not a
classification bug; it ends when the pass completes.

The tick's two-hour rollup of `health_hourly` and the sweep's day batches
take the retention advisory lock in turn, so a tick that logs `health
rollup deferred` found a sweep batch in flight and counts its drop deltas
on the next tick; `health_hourly.conditions` keeps each hour's closing
counters (`totals`, `last_at`) so a rollup over the oldest retained hour
seeds from the ledger once the raw rows before it are gone.

## Runbook: skill invocations

Alerts: `17c-skill-store-error` (ERROR), `18-skill-derive-silent`
(WARNING). The lines are in the log contract above.

Every Skill tool call and typed slash command an enrolled laptop delivers
becomes a `skill_invocations` row in the same transaction as its events
(`server/store/skills.go`, called after the links), under a savepoint: a
skill row never fails an events batch. History is rebuilt by the
`skill_invocations` step of the derive runner (version 4), and a session
whose skill rows were lost to a rollback, a lock wait or a skipped session,
or whose typed command's hook copy arrived a batch after its transcript
copy (`reason = 'twin'`, so the two fold at the next tick rather than the
next rebuild), waits in `skill_rederive_queue` for the dirty tick, which
rebuilds twenty per pass, counts no attempt for a lock wait, and parks a
session after five failed attempts.

| Symptom | Check | Lever |
|---|---|---|
| 18 fired, or history not rebuilt | replay one session's `Skill` calls and slash prompts against its `origin = 'derived'` rows; `SELECT * FROM derive_jobs WHERE version = 4 ORDER BY ordinal` | a renamed `tool_input.skill` or an over-wide built-in list (`internal/normalize/skills.go`), then the targeted rerun below; a parked step's `recover` statement (Runbook: derive) |
| 17c fired | `gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="skill invocation store failed"' --freshness=30m --format='value(jsonPayload.op,jsonPayload.platform,jsonPayload.error)'` | a constraint violation is a shape the derivation let through: fix, deploy; the queued sessions drain on their own once the statement succeeds |
| queue growing, or `derive session failed` with `step = skill_invocations` | `SELECT attempts, last_error, count(*) FROM skill_rederive_queue GROUP BY 1, 2 ORDER BY 1` | a parked row (`attempts = 5`) is unparked by the rerun route, whose `since` re-queues it with attempts reset; a `last_error` that names a body is a decode bug, fix and deploy first |
| a rollback served a window without skill rows, or the first catalog publish landed | the rollback's start from the canary file and the audit row (`SELECT * FROM admin_actions WHERE action = 'derive.skill_invocations.rerun' ORDER BY at DESC`) | the rerun route, below |

The targeted rerun is `POST /v1/admin/derive/skill-invocations/rerun` from
the dashboard session (an admin, same-origin; a cross-site POST is 403),
never hand SQL on prod. With a body `{"since": "<RFC 3339>"}` it queues
every session updated at or after that moment into `skill_rederive_queue`
with `reason = 'rollback'` and changes no stamp; the dirty tick drains the
queue at about 2,400 sessions an hour. Without a body it resets the one
step (`derive_jobs` row cleared, `started_at` included so the stalled
alert does not read the old start) and lowers `derived_schema` to 3 (a
stamp already below 3, a fresh database's 0, is left where it is: raised,
it would seed the version-3 steps finished on a database that never ran
them), so the next versioned pass re-runs `skill_invocations` alone inside
the derive window; an instance notices at its six-hourly recheck or at a boot, and a
same-night bounce is a new revision of the serving image. Every call writes
an `admin_actions` row naming the caller and the `since` value. The full
rebuild of every step is reserved for logic changes and is in
`docs/UPGRADES.md`.

The derived rows are rebuilt, never hand-edited: a wrong row is fixed by
fixing the derivation and running the step. The dirty pass folds and
classifies only, so a lost skill row comes back through the queue or the
step and nowhere else.

## Runbook: source tokens

Alerts: `17-skill-auth-rejected` (WARNING), `17b-skill-invalid-payload`
(WARNING), `17d-skill-token-expiring` (WARNING), `17e-skill-rate-limited`
(WARNING), `18b-skill-beacon-silent` (WARNING). The lines are in the log
contract above.

A source token (`lss_`) proves a platform's environment, never a person:
the Devin organisation, a Capy project, the Claude Code cloud environment,
a Codex environment, the Vorflux harness, or one unenrolled laptop. Every
row posted under one is `trust = 'claimed'`; the derived copy built from
an enrolled laptop's own events replaces it on the same key. One row of
`source_tokens` per `(platform, environment)`, two during a rotation; the
row carries the per-minute counter every instance shares, the origins the
token may post (`allowed_origins`, fixed at mint), and, for a laptop
token only, the person it writes rows for (`bound_actor_email`). Tokens
are minted, limited and revoked from the dashboard session on the admin
routes below (same-origin cookie POSTs; a cross-site POST is 403; every
call writes an `admin_actions` row), never by hand SQL. The plaintext is
shown once. GSM names keep the `loop-sessions-*` prefix:
`loop-sessions-skill-usage-token-<platform>-<environment>` and
`loop-sessions-skill-catalog-token-<repo>`; the vendor-side variable is
`LOOP_SKILL_USAGE_TOKEN`.

| Symptom | Check | Lever |
|---|---|---|
| 17 fired | `gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="skill invocation rejected"' --freshness=30m --format='value(timestamp,jsonPayload.reason,jsonPayload.token_state,jsonPayload.source_token_id,jsonPayload.platform)'`, grouped by `reason`, `token_state`, `source_token_id`; for the third condition the `skill invocation preempted` lines, grouped by `preempted_by` and `preempted_device` | `token_state` `revoked` or `expired`: that token is still on a vendor (finish the rotation below) or a leaked copy is in use (rotate the platform now); `none` at volume is a probe unless 5xx correlates; `wrong_scope` is a catalog token on the invocation route (the publisher's secret is in the wrong repository variable); a `preempted_by` token or `preempted_device` that keeps appearing is pre-inserting rows and is revoked, with one benign shape to rule out first: a person who posted under a laptop token (or an `lsd_` API row) and then enrolled, whose backfill's derived rows displace every hook row of those sessions in one burst; that `preempted_by` is a `laptop` token bound to the person who just enrolled, it fires once, and it is a laptop token that has done its job (revoke it, nothing else) |
| 17b fired | the same command with `jsonPayload.reason="invalid_payload"`, grouped by `field` and `platform` | one field on one platform is that emitter's bug (the hook script and the beacon line in your skills repository, the reconcilers and the Vorflux monitor); `field=origin` on a freshly minted token is a mint with the wrong `allowed_origins`, mint again; `platform_mismatch` on the line is a body naming another platform than its token's |
| 17d fired | `gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="skill token summary"' --freshness=10m --format='value(jsonPayload.platform,jsonPayload.environment,jsonPayload.source_token_id,jsonPayload.expires_in_days,jsonPayload.last_used_minutes_ago,jsonPayload.rate_limit_per_min)'`, grouped by `source_token_id`, because one `REDUCE_MIN` incident hides every other expiring token | rotate every token under 14 days (below); `expires_in_days` 0 with `issued_days` 100000 is a row hand-inserted with no expiry: revoke and mint properly; `expires_in_days` 0 with a real `issued_days` is a minted token in its last 24 h, rotate it now |
| 17e fired | the rejected line with `reason=rate_limited`, then the token summary's `window_count` and `rate_limit_per_min` for that platform | a `window_count` far above what the platform's sessions could produce is a flood from one holder of the shared token: rotate the platform; a count just over a small limit is a canary limit never raised: `POST /v1/admin/source-tokens/{id}/limit`; `Retry-After: 1` answers are lock waits that clear on their own: on the verify's touch of the token row, which every post under the token contends for (each holds it for that one statement, committed before the row is written; a limit or revoke on the admin route holds it for its transaction), or on a skill row a derive batch holds; a verify that timed out logs `platform` and `source_token_id` empty, since the statement that would have read the row is the one that failed; count those lines against the platform whose emitters report 429s |
| 18b fired, or a platform reads zero | `gcloud logging read 'resource.labels.service_name="loop-sessions" AND jsonPayload.message="skill platform summary"' --freshness=10m --format='value(jsonPayload.platform,jsonPayload.environment,jsonPayload.origin,jsonPayload.minutes_since_last_row,jsonPayload.rows_24h)'`, then `last_used_minutes_ago` on that platform's token summary line | vendor secret current and pushed, mirror carries the beacon line, `rate_limit_per_min` not 0 from an unfinished rotation, egress from the platform; a silent reconciler series is the automation stopped, re-enable it or restart the monitor; a platform that stopped using skills for good is not an incident, revoke its token so its series is no longer expected; the `claude_code` condition reads `environment="cloud"` and nothing else, so the cloud hook's token is minted with `environment = cloud` exactly (a `cloud-1` or `cloud-eu` token opens a series 18b never reads: rows land, the alert cannot fire, and a reading of zero for `claude_code` at `cloud` is the mint's name, not the hook) |

The routes, all from the dashboard session (an admin, same-origin):

- `POST /v1/admin/source-tokens` with `{"platform", "environment", "scope", "label", "expires_in_days", "allowed_origins"}`: `expires_in_days` is required, 1 to 180; `scope` defaults to `skill-invocations` (`skill-catalog` for a catalog publisher, whose `environment` is `catalog-<source_repo>`); `allowed_origins` is a non-empty subset of `hook`, `beacon`, `reconciler` and defaults to `{beacon}`; `environment = laptop` is 403 and `bound_actor_email` is 400, since a per-person token is minted by the person. The `environment` is also the label 18b watches: the Claude Code cloud hook's token is `environment = cloud` (with `allowed_origins = {hook}`), since that is the one name its condition reads. The answer carries `token` once.
- `POST /v1/admin/source-tokens/{id}/limit` with `{"rate_limit_per_min": n}`, 0 to 100000. 0 is the soft revoke: the token is still verified and counted, nothing is written, the emitter is told its post was a duplicate, and `skill_soft_revoked` counts it, so a platform still on the old token stays visible.
- `POST /v1/admin/source-tokens/{id}/revoke`: the hard revoke (`revoked_at`, `revoked_by`), which frees the hash slot. Idempotent.
- `POST /v1/source-tokens/laptop` from a member's own cookie, body at most `{"label", "expires_in_days"}` (default 180): the server sets `platform = claude_code`, `environment = laptop`, `scope = skill-invocations`, `allowed_origins = {hook}` and `bound_actor_email` = the caller, so an unenrolled laptop's hook posts rows in its owner's name and nobody else can mint one for them. The hook reads it from `LOOP_SKILL_USAGE_TOKEN`; an enrolled laptop needs nothing, its hook exits on the device token.

Rotation, per platform: mint the successor for the same `(platform,
environment)` (expiries staggered 150 to 180 days so they never share a
week), add a GSM version, push it to the vendor, wait for the old row's
`last_used_at` to stop moving or 24 h after the push, whichever comes
first (a token in hostile hands never goes quiet), set the old limit to 0
for a day, then revoke. The old token answers `200 {"duplicate": true}`
throughout the limit-0 day while `skill_soft_revoked` counts it. Two live
rows for one pair both verify in the meantime; a reconciler's re-post
under the successor lands on the same key, since its key is the platform
and environment rather than the token.

Rollback of a token is the same two steps in the same order: limit 0, then
revoke. Rollback of the route is the revision rollback in "Rolling back";
the tokens stay valid and the emitters drop their 404s until the
roll-forward.

## Runbook: skill catalog

No alert of its own; policy 17's fourth condition (`skill_reconciler_mismatch`)
and the `skill catalog published` WARNING are the two signals.

Each source repository's CI PUTs its whole catalog (`catalog/skills.json`)
on every push to main and weekly, under a `skill-catalog` token whose
`environment` is `catalog-<source_repo>` (`<your-skills-repo>`, `<your-app-repo>`,
`devin-builtin`): entries upsert with `first_seen_at` kept, entries the body
no longer carries flip `present = false`, the repository's aliases are
replaced by the body's union of `aliases[]` with `<plugin>:<dir>` and
`<dir>`, and a re-PUT of a recorded `(source_repo, commit)` answers
`duplicate: true` and changes nothing, unless its stored row recorded
`skills = 0` (see the recovery below). The aliases are also the 4a oracle:
a typed slash command the shape rule read (the hook copy, or a Codex
`$skill` mention) becomes a row only once its name is in
`skill_catalog_aliases`, so until the first publish those rows are counted
`unconfirmed` on the `skill invocations derived` line and dropped.

| Symptom | Check | Lever |
|---|---|---|
| the first marketplace publish landed | `SELECT source_repo, commit, generated_at, received_at, skills FROM skill_catalog_publishes ORDER BY received_at DESC` shows `<your-skills-repo>` | an admin runs `POST /v1/admin/derive/skill-invocations/rerun` once, with no body, from the dashboard session (Runbook: skill invocations): the typed rows the oracle refused before the aliases existed are re-derived by the step |
| the Devin built-ins are missing from `/skills` | `SELECT count(*) FROM skill_catalog_entries WHERE source_repo = 'devin-builtin'` is 0 | mint a `skill-catalog` token with `environment = catalog-devin-builtin` and `expires_in_days = 1`, PUT the lead's `devin-builtin.json` (`plugin = ''`, `authored_by = vendor`), then revoke the token, all inside the hour: the 17d gauge skips a token issued for under 14 days, but a live catalog token is still a credential |
| `skill catalog published` at WARNING, or `stale_mirror` on `/skills` | `SELECT e.source_repo, e.plugin, e.skill, e.sha256_tree, e.lineage_of FROM skill_catalog_entries e WHERE e.present AND e.lineage_of IS NOT NULL`, against the head's rows; `max(generated_at)` per `source_repo` on `skill_catalog_publishes` | the app repository's mirror is behind the marketplace: run your mirror-sync workflow (or merge its open PR) so the mirror and `catalog/skills.json` move together, then the mirror's publish clears the flag |
| `PUT` answers 401 | the token's state on the server's `skill invocation rejected` line, which this route shares with the invocation one (`reason = unauthenticated`, `status = 401`, `token_state`) | `wrong_scope` is an emitter token in the publisher's secret; `revoked` or `expired` is a rotation unfinished: mint under `POST /v1/admin/source-tokens` with `scope = skill-catalog` and `environment = catalog-<source_repo>`, update the repository's `LOOP_SKILL_CATALOG_TOKEN` in the `catalog-publish` Environment |
| `PUT` answers 403 | the same line at `reason = forbidden`, then the path's `source_repo` against the token's `environment` | the token is another repository's; every publisher has its own token, never a shared one |
| `PUT` answers 409 `alias_conflict` | the `message` names the alias and the holder | two repositories claim one name with no lineage between them: the backend generator emits `lineage_of` naming the marketplace entry, and the marketplace publishes first (its entries must exist before a `lineage_of` can name them, else the whole PUT is 400 on `lineage_of`) |
| `/skills` says "history rebuilding" | `SELECT processed, finished_at FROM derive_jobs WHERE version = 4 AND step = 'skill_invocations'` | nothing to do: the banner clears when the step finishes; a step that never finishes is Runbook: derive |
| unknown-skill report grows | `GET /v1/skills/unknown?range=7d`, or the panel on `/skills` (admins see the names) | add the alias to the skill's frontmatter (the generator folds `name` and `aliases` into `aliases[]`) or the built-in to the 3.5 list, then push main; the next publish resolves the rows on their next read, no rerun needed |
| `PUT` answers 409 `catalog_shrink` | the body's `present` and `incoming` counts, against `SELECT count(*) FROM skill_catalog_entries WHERE source_repo = '<repo>' AND present` | the generator produced an empty or nearly empty catalog, usually a checkout it globbed before the skills were there: fix the generator or the workflow and push again. Nothing was written, so the catalog, the aliases and the oracle are untouched. Do NOT set `allow_shrink` to get past it (see below) |
| the unknown panel fills with every real name at once, or `/skills` reads empty | `SELECT count(*) FROM skill_catalog_aliases WHERE source_repo = '<repo>'` is 0, and the newest `skill_catalog_publishes` row for the repository has `skills = 0` | recovery from a wipe: re-run the CI job of the last good commit. A publish row recording `skills = 0` is not treated as published, so the SAME sha is accepted and restores the entries and aliases; the confirmation oracle answers again on the next read and no `rerun` is needed. If the wipe predates the shrink gate and the stored row carries entries, publish the same catalog under a fresh sha instead |

A publish may not retire most of a catalog (design 4d): a body with zero
entries, or one that would flip more than half of the repository's present
entries to `present = false`, is refused `409 catalog_shrink` and logged at
WARNING, and an accepted publish whose count merely FALLS is logged at
WARNING too, with `skills` and `present_before` on the line. The only way
past the refusal is an explicit `"allow_shrink": true` in the body. The
publish script never sets it and no workflow may: it is a two-person step,
one engineer running the PUT by hand while a second reads the `present` and
`incoming` counts of the 409 and agrees the repository really did delete
those skills. The reason is that the aliases are the 4a oracle, so an
accepted wipe does not merely empty a page: every derived invocation of
every skill becomes `unconfirmed` and is dropped, which is silent.

The gate is per publish, so it does not stop a staircase: seven accepted
publishes can take a hundred present entries to one (50, 25, 13, 7, 4, 2,
1), each under the half and each logged only as the `skills` <
`present_before` WARNING on `skill catalog published`, which no metric
reads. A generator intermittently emitting partial output over a week of
scheduled runs empties a catalog through the gate unseen. Until a metric on
that WARNING exists, the check is the trend and not one publish:
`SELECT commit, received_at, skills FROM skill_catalog_publishes WHERE
source_repo = '<repo>' ORDER BY received_at DESC LIMIT 10` must not fall
across consecutive rows without a matching deletion in the repository, and
`gcloud logging read 'jsonPayload.message="skill catalog published" AND
severity>=WARNING' --freshness=7d` lists every fall with both counts.
Recovery is a fresh publish of the whole catalog: push an empty commit to
main so CI PUTs under a new sha, since a re-PUT of the last good sha answers
`duplicate: true` and restores nothing (its row recorded entries, so the
`skills = 0` repair above does not apply), and growth never meets the gate.

The canaries after the first publish (design 9.3): an enrolled laptop types
`/engg:git` and one `user` row keyed `:p:<prompt_id>` appears, created by
the transcript copy before the publish and by the hook copy from the first
publish on, never a second row; the Devin reconciler's first re-enabled run
leaves one `reconciler_runs` row and joins its `link_ref` to a beacon row's
`session_ref`; and the T10 negative paths answer as above: a
`skill-invocations` token 401, another repository's token 403, a body
naming an entry that does not exist 400 on `lineage_of`.

## Runbook: purge a person

No alert; this is the one-transaction erasure run after a person's request.
Nothing in the service does it, since a mass DELETE in the production
binary was judged more blast radius than a recipe an operator reads twice,
and it is run by hand, in the order below. Keep the
table names and the order: it is the FK order (`sessions.device_id`
references `devices`, so the sessions go before the devices; `messages` and
`artifact_versions` hang off `events`, so those two cascade). `principals`
and `access_log` are kept on purpose: the roster row is what the sign-in
check reads, and the access log is the record of who viewed whose sessions,
which outlives its subject.

Connect as section 5 does, through the proxy
(`cloud-sql-proxy "${PROJECT}:${REGION}:${INSTANCE}" --port 5433`),
as user and database `loop_sessions`, with the password from Secret Manager
`loop-sessions-db-password`. Run the file with `-v ON_ERROR_STOP=1`, so a
refused statement aborts the whole transaction rather than leaving half a
person behind, and pass the address and your own as psql variables, so no
statement is hand-edited:

```bash
psql -h 127.0.0.1 -p 5433 -U loop_sessions -d loop_sessions -v ON_ERROR_STOP=1 \
  -v who='<email>' -v me='<your email>' -f purge.sql
```

Before the deletes, record who viewed that person's sessions. The
`access_log` rows stay, but the sessions they name will not, so the copy
taken now is the last readable answer to that question:

```sql
SELECT viewer, session_id, via, at FROM access_log WHERE owner = :'who' ORDER BY at;
```

Then `purge.sql`, one transaction, every statement keyed on the address:

```sql
BEGIN;
-- 0. The person's skill rows, while their session rows still exist: the
--    same SET the retention sweep runs (server/store/retention.go,
--    deleteSessionBatch), keyed on the person instead of a session
--    (design 3.4, SECURITY3-3). It runs before step 3 because the
--    session_type term reads the session row that step deletes; after it,
--    an automation session's rows would return to the default views.
UPDATE skill_invocations SET
  session_type = COALESCE(NULLIF(session_type, ''), (SELECT s.session_type FROM sessions s WHERE s.session_id = skill_invocations.session_ref), ''),
  actor_email = NULL, device_id = NULL, source_token_id = NULL, preempted_by = NULL, preempted_device = NULL,
  session_ref = '', event_id = NULL, prompt_id = NULL, tool_use_id = NULL, dedupe_key = 'anon:' || id
WHERE actor_email = :'who';
-- 1
DELETE FROM usage_ledger WHERE email = :'who';
-- 2 (cascades messages and artifact_versions)
DELETE FROM events WHERE email = :'who';
-- 3 (cascades artifacts, links, shares and the session-keyed slack_posts)
DELETE FROM sessions WHERE email = :'who';
-- 4 the cap notices, which carry the address and no session
DELETE FROM slack_posts WHERE email = :'who';
-- 5
DELETE FROM slack_prefs WHERE email = :'who';
-- 6
DELETE FROM health_reports WHERE email = :'who';
-- 7 (cascades device_tokens)
DELETE FROM devices WHERE email = :'who';
-- 8. The person's laptop tokens: revoke, then unbind, the pair
--    RevokeAndUnbindLaptopTokens runs (server/store/source_tokens.go), in
--    that order because the revoke reads the binding the unbind clears.
--    The one difference from the method: it has no operator in its
--    signature and leaves revoked_by NULL; you have one, so the hand-run
--    revoke records you. revoked_by references principals, so :'me' is
--    your roster address.
UPDATE source_tokens SET revoked_at = now(), revoked_by = :'me'
  WHERE bound_actor_email = :'who' AND environment = 'laptop' AND revoked_at IS NULL;
UPDATE source_tokens SET bound_actor_email = NULL WHERE bound_actor_email = :'who';
COMMIT;
```

Afterwards both counts are zero, which is what the integration suite
asserts of the store method
(`TestIntegrationLaptopTokensAreRevokedAndUnbound`):

```sql
SELECT count(*) FROM source_tokens WHERE bound_actor_email = :'who';
SELECT count(*) FROM skill_invocations WHERE actor_email = :'who';
```

A reinstall for the same person on the same machine needs the client's
`--skip-backfill` flag (`cmd/loop-sessions/setup.go`): the dedupe keys are
the primary keys (`events.id`, `sessions.session_id`, the `usage_ledger`
pair), so the history still on that laptop would be imported again under
the ids the purge removed, and the person would be back in full.

## Runbook: export

Alerts: `20-export-lag` (WARNING, data tier: lag above 3 h, or no run line
for 3 h), `20b-export-load-failed` (WARNING: any failed load job) and
`20c-export-run-failing` (WARNING: a run line with `status` other than `ok`
in each of two consecutive hours; this is the one that sees a run failing
before it has anything to measure, and a first run that has no watermark
yet).

What the job writes, one line per run, `message="export run"`
(`server/export`, `LogRun`): `partitions`, `bundles`, `rows`, `bytes`,
`seconds`, `lag_seconds`, `status` (`ok`, `failed`, `timeout`), `capped`,
`bootstrap`, `failed_loads`, and `error` on failure. `lag_seconds` is the
age of the oldest watermark in force after the run: the new positions when
the run advanced them, the old ones when it failed at any point, and zero
only on a first run with no watermark at all (`bootstrap=true`). One
`export partition` line per file (`table`, `day`, `rows`, `bytes`, `job`),
one `export load failed` line per failed load (`table`, `day`, `job`,
`error`), and `export tie split` if more than the cap shared one
`updated_at`. No line carries content.

Expect `20-export-lag` to fire while the corpus is first loaded, and after
any outage long enough to leave more than one run's worth of partitions
behind. Each run is capped, so `lag_seconds` is then the age of the oldest
day not yet loaded rather than a fault. A first run over a corpus that
reaches back months reports `capped=true` with a `lag_seconds` of weeks. The
hourly schedule works the backlog forward and
the alert clears itself. What is a fault is `capped=true` with the lag not
falling between runs, or a `status` other than `ok`, which is `20c`'s job.

The job's logs are under the Cloud Run job resource, not the service:

```bash
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="loop-sessions-export" AND jsonPayload.message="export run"' \
  --limit=6 --freshness=6h \
  --format='value(timestamp,jsonPayload.status,jsonPayload.lag_seconds,jsonPayload.partitions,jsonPayload.bundles,jsonPayload.capped,jsonPayload.error)'
gcloud run jobs executions list --job=loop-sessions-export --region="$REGION" --limit=5
```

Before the first run: the derive runner's `index:events_ingested_at_idx`
step must have finished (the runner's step line, or through the proxy:
`SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('events_ingested_at_idx')`
must return `t`). Without the index the first run's planning read and each
events-day COPY are sequential scans of the whole `events` table, about a
minute each at today's size; they fit the export ceiling, so the run
succeeds, but slowly and against the instance's I/O, and the index is what
keeps every later hourly run cheap.

Provisioning, once, by the owner, and again after any edit under
`examples/deploy-gcp/analytics/`, any change to `--admin-emails`, or any deploy
of a new image (the job runs the same image as the service, so it is
re-pointed with `--tag`):

```bash
examples/deploy-gcp/analytics/provision.sh --dry-run --tag="$TAG"   # reads only; prints every write, keeps the rendered DDL, access lists and job.yaml
examples/deploy-gcp/analytics/provision.sh --tag="$TAG" --admin-emails='you@example.com,you@example.com'
examples/deploy-gcp/analytics/scheduler.sh --run-now                # one execution now, rather than at :07
```

`--dry-run` describes the bucket, the service account, the datasets, the
taxonomy and the jobs, and prints, in order, every `gcloud` and `bq`
command a real run would make; read it before the real apply. The real run
is update-or-create throughout. Then check that rows landed in the raw
tables, that the views read them, and that the two things a
`WRITE_TRUNCATE` load is documented to remove are still there (the view
authorizations and the tags; repeat these two after the first successful
run, and whenever in doubt):

```bash
bq query --location="$REGION" --use_legacy_sql=false \
  'SELECT table_name, partition_id, total_rows FROM `__PROJECT__.loop_sessions_raw.INFORMATION_SCHEMA.PARTITIONS` ORDER BY table_name, partition_id DESC LIMIT 40'
bq query --location="$REGION" --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `__PROJECT__.loop_sessions.sessions_latest`'   # as an admin: every row; as a member: their own
bq query --location="$REGION" --use_legacy_sql=false \
  'SELECT email, started_at FROM `__PROJECT__.loop_sessions.sessions` LIMIT 1'   # as a member WITHOUT Fine-Grained Reader: their own row and no error (untagged columns read through the SELECT * views)
# the admins list is exactly --admin-emails and nothing more (the export identity is WRITER on the raw dataset, this table included)
bq query --location="$REGION" --use_legacy_sql=false \
  'SELECT email FROM `__PROJECT__.loop_sessions_raw.admins` ORDER BY email'
# the views are still authorized on the raw dataset (nine "view" entries; more than nine is a duplicate, see below)
bq show --format=json __PROJECT__:loop_sessions_raw | jq '[.access[] | select(.view)] | length'
# the raw dataset carries no project group but projectOwners (the script removes projectReaders and projectWriters; expect one line)
bq show --format=json __PROJECT__:loop_sessions_raw | jq -c '.access[] | select(.specialGroup)'
# the governed dataset carries no projectWriters (the script removes it; expect projectReaders and projectOwners, two lines)
bq show --format=json __PROJECT__:loop_sessions | jq -c '.access[] | select(.specialGroup)'
# every governed view still carries its rule (two SESSION_USER() clauses each; fewer means it was redefined, see below)
for v in turns sessions messages events health_hourly events_latest sessions_latest turns_latest messages_latest; do
  printf '%s ' "$v"; bq show --format=json "__PROJECT__:loop_sessions.$v" | jq -r '.view.query' | grep -c 'SESSION_USER()'
done
# the tags are still on the four text columns
bq show --schema --format=json __PROJECT__:loop_sessions_raw.messages | jq '.[] | select(.name == "text") | .policyTags'
bq show --schema --format=json __PROJECT__:loop_sessions_raw.events | jq '.[] | select(.name == "body") | .policyTags'
# through the proxy (section 5): where the export stands
psql ... -c 'SELECT name, watermark, updated_at FROM export_watermarks'
```

If any of those comes back empty, re-run `provision.sh` (it re-asserts
both) and record what the run before it had done. A `view` count above
nine, or two entries for one principal, means `bq show` rendered an entry
with a field the script's presence check did not expect and a run appended
a second copy; the script compares only the fields it wants, so that is
not expected, but the first real run's `bq show --format=json` is what
settles it. A `projectReaders` or `projectWriters` line on
`loop_sessions_raw`, or a `projectWriters` line on `loop_sessions`, means
someone put the group back by hand since the last run; the next run removes
it again. A view reporting fewer than two `SESSION_USER()` clauses was
redefined since the last run, by a principal whose write reach on
`loop_sessions` comes from project IAM (the access model says who): re-run
`provision.sh`, which re-creates every view from `views.sql`, then find out
who and why, because every reader of `loop_sessions` read every raw row
through that view in the meantime.
An admins row you did not list means something other than `provision.sh`
wrote the table: re-run the script (the table becomes the flag again) and
find out what.

If the lag alert fires: read the last run lines. `capped=true` with a
falling `lag_seconds` is a catch-up (the first runs after provisioning, or
after an outage); nothing to do. `status=failed` with `failed_loads` above
zero: the `export load failed` line names the table, the day and BigQuery's
error; a schema mismatch (`Provided Schema does not match`, or a field the
table lacks) means `server/store/export_reads.go` gained a column that
`bigquery/tables.sql` does not have, so add it there with
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and re-run `provision.sh` (which
also re-creates the views, whose column lists are fixed when they are
created). `status=failed` with `error` starting `export: plan:` is a
planning read; on a `bootstrap=true` run check the index step above.
`status=timeout`: a run that did not finish in 28 minutes; the partition
lines before it show which table was slow (an events day is the usual
suspect), and a smaller `EXPORT_MAX_EVENT_DAYS` in `job.yaml` is the knob.
No run line at all: the schedule is paused or the job cannot start;
`gcloud scheduler jobs describe loop-sessions-export-hourly --location="$REGION"`
and the executions list above.

`health_hourly` before PR E: the run reports `partitions` without any
`health_hourly` file and the `health_hourly` row is absent from
`export_watermarks`; that is the job waiting for the table, not a failure.
Once PR E's sweeper creates it, the next run exports every rolled-up day
and the row appears. Do not create the row by hand.

Pausing and resuming (an incident on the primary, a DDL change in
progress): `scheduler.sh --pause`, `scheduler.sh --resume`.

Forcing a full re-export (a projection changed for every row, a partition
was damaged, or old-day copies left behind by sessions whose start day
moved should go): through the proxy,
`DELETE FROM export_watermarks WHERE name IN ('sessions','events','health_hourly')`,
then `scheduler.sh --run-now`. The runs that follow rewrite every partition,
capped, and the lag alert fires until they are done. Deleting one name
re-exports one stream.

Do not grant the export service account `roles/datacatalog.categoryFineGrainedReader`
or anything on the `loop_sessions` dataset; do not grant anyone but the
the admin grantees read access on `loop_sessions_raw` (every row of every table
is visible there; the `projectReaders` and `projectWriters` entries
BigQuery attaches at creation are removed by every `provision.sh` run, and
one put back by hand goes on the next); do not grant anyone `WRITER` on
`loop_sessions` (a writer there can redefine a view without its rule, which
is why every run removes the `projectWriters` entry); do not run the job
with more than one task
(`taskCount: 1`; two runs would race for the watermark); do not add
`EXPORT_ACCESS_TOKEN` to `job.yaml` (it is for a laptop run with
`gcloud auth print-access-token`, and only that).
