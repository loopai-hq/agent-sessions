# loop-sessions with Docker Compose

The server and its Postgres on one machine, started with one command. This
is the quickest way to run the real thing (as opposed to the fixture-backed
demo server in the repository README) and a reasonable shape for a small
team's self-hosted deployment once TLS is in front of it.

It is not the only way to run the server. The server needs a Postgres 15 or
newer and a Firebase project for sign-in, and nothing else; anything that
hands a container an environment works. `examples/deploy-gcp/` is the same
server on Cloud Run with Cloud SQL.

## What you need

- Docker with the Compose plugin (`docker compose version` prints v2 or
  later).
- A Firebase project with Google enabled as a sign-in provider. Sign-in is
  Google accounts through Firebase only; there is no local password login,
  so this cannot be skipped. Creating a project is free and takes a few
  minutes; the three values it gives you are below.
- To build the image locally instead of pulling it: nothing beyond Docker.
  The build runs inside the `golang` image.

## Start it

```sh
cd examples/docker-compose
cp .env.example .env
$EDITOR .env              # SESSION_KEY, the Firebase values, your domain, your address
docker compose up -d --build
curl -sf http://127.0.0.1:8080/readyz && echo ready
```

`up` starts Postgres, waits for its healthcheck, then starts the server,
which pings the database, applies the schema migrations under an advisory
lock and begins serving. `/readyz` answers 200 as soon as that is done,
usually within a few seconds of Postgres reporting healthy. On the first run
the image build (a Go compile inside Docker) is most of the wait.

Then open <http://127.0.0.1:8080> and sign in with a Google account on one
of the `ALLOWED_DOMAINS`.

### Pull the published image instead of building

The service names the published image,
`ghcr.io/loopai-hq/loop-sessions-server`, and also carries a `build:` section
pointing at the repository root, so:

- `docker compose up -d --build` always builds from the checkout you are
  standing in. This works before any release exists and on any architecture
  Docker can build for.
- `docker compose pull server && docker compose up -d` runs the released
  image (tag `latest`, or set `SERVER_IMAGE_TAG=v0.1.0` in `.env`). The
  published image is linux/amd64; on an arm64 host either build locally or
  run it under emulation.
- Plain `docker compose up -d` leaves the choice to Compose: an image of
  that name already on the machine is used as it is; otherwise Compose
  tries to pull the published one and, when that is not available, builds
  from the checkout. The two commands above say which you meant.

## The three Firebase values

All three are in the Firebase console under **Project settings, General**.

| Variable | Where it comes from | Secret? |
|---|---|---|
| `FIREBASE_PROJECT_ID` | The project id (not the display name and not the project number). | No |
| `FIREBASE_API_KEY` | "Web API Key" on the same page. | No: it is served to every browser on the sign-in page. It is still project-specific, which is why `.env.example` leaves it blank rather than inventing one. |
| `FIREBASE_AUTH_DOMAIN` | Optional. Defaults to `<project id>.firebaseapp.com`; set it only if the project uses a custom auth domain. | No |

Two console settings decide whether sign-in works at all, and both fail
silently in the browser with nothing in the server's logs:

1. **Google is a sign-in provider.** Authentication, Sign-in method. The
   server refuses any token whose provider is not `google.com`, so nothing
   else needs enabling.
2. **`127.0.0.1` is an authorised domain.** Authentication, Settings,
   Authorized domains. Firebase completes the sign-in popup only for origins
   on that list, so make sure `127.0.0.1` is on it (the domain alone,
   without a port or scheme) and add it if it is not. The failure without
   it is `auth/unauthorized-domain` in the browser console. When you put the
   server behind a real hostname, add that hostname the same way and change
   `PUBLIC_URL` to match.

## The first admin

`ADMIN_EMAILS` in `.env` names the addresses that are created as admins at
boot if they do not already exist. It never changes a row that is already
there, so the variable can stay set. Nobody else needs an admin's action to
get in: anyone with a Google account on `ALLOWED_DOMAINS` is enrolled as a
member automatically the first time they sign in (a member sees only their
own sessions), so that list is the access gate. The **People** page
(`/admin/principals`) is where admins promote, disable or re-enable people;
a disabled address stays refused even though its domain is allowed. Each
address in `ADMIN_EMAILS` must be on `ALLOWED_DOMAINS`, or the server refuses
to start rather than create an admin who could never sign in.

## Ports and addresses

The server listens on 8080 inside the container (`compose.yaml` fixes
`PORT` for it), and `compose.yaml` publishes that as `127.0.0.1:8080` on the
host: loopback only, plain HTTP. `PUBLIC_URL` must be what a browser types to
reach it, so the default `http://127.0.0.1:8080` is right for a laptop; the
`http://` scheme is also what tells the server to set its cookie without the
`Secure` attribute, which a loopback deployment needs. Set `HOST_PORT` in
`.env` to publish a different host port and change `PUBLIC_URL` with it;
`PORT` in `.env` is not a knob here (it would be ignored).

Postgres is reachable only on the Compose network, as `db:5432`. `compose.yaml`
fixes `DATABASE_HOST` and `DATABASE_PORT` for the server; nothing publishes
the port on the host. To reach it with `psql` for a backup or a query:

```sh
docker compose exec db psql -U loop_sessions -d loop_sessions
```

## Data and backups

Everything the server stores is in the `pgdata` volume, mounted at
`/var/lib/postgresql/data` in the `db` container: sessions, events (the
transcripts; the source of truth), the search index, the roster, the access
log. `docker compose down` keeps it; `docker compose down -v` deletes it.

Back up the database, not the containers. A plain dump is enough for this
size of deployment:

```sh
docker compose exec -T db pg_dump -U loop_sessions -Fc loop_sessions > loop-sessions-$(date +%F).dump
```

Restore with `pg_restore` into an empty database before starting the server,
which then applies any migrations the dump predates. What to back up, how
often, how to restore, how derived rows are rebuilt from events and how to
size retention are in [docs/OPERATIONS.md](../../docs/OPERATIONS.md).

## Enrol a client

The client captures Claude Code sessions on a laptop and sends them to the
server. Point it at this deployment with `--endpoint`; public binaries are
not stamped with a server, so this flag (or `LOOP_SESSIONS_ENDPOINT`) is
required:

```sh
# from a checkout
make build
./loop-sessions install --endpoint http://127.0.0.1:8080

# or the released binary, verified against SHA256SUMS by the installer
curl -fsSL https://github.com/loopai-hq/agent-sessions/releases/latest/download/install.sh |
  LOOP_SESSIONS_BASE_URL=https://github.com/loopai-hq/agent-sessions/releases/latest \
  LOOP_SESSIONS_VERSION=download sh
loop-sessions install --endpoint http://127.0.0.1:8080
```

`install` opens the browser to sign in (the same Google account rules apply),
registers the capture hooks and imports the history already on disk. A client
on another machine needs a `PUBLIC_URL` that machine can reach, which means
the TLS step below.

## Upgrading

```sh
git pull                       # or set SERVER_IMAGE_TAG to the new tag
docker compose up -d --build   # or: docker compose pull server && docker compose up -d
```

The new server applies any new migrations at boot; the database is shared
with nothing else, so there is nothing to coordinate. Take a dump first.

## Stopping and removing

```sh
docker compose down            # stop; the data volume stays
docker compose down -v         # stop and delete the database
```

## Going further

- **TLS and a hostname.** Put a reverse proxy that terminates TLS (Caddy,
  nginx, Traefik) in front of `127.0.0.1:8080`, set
  `PUBLIC_URL=https://sessions.example.com`, and add that hostname to the
  Firebase authorised domains. With an `https://` `PUBLIC_URL` the cookie is
  `Secure` again. Do not publish the plain-HTTP port beyond loopback.
- **Health.** `/livez` answers without touching the database and is the
  probe for "is the process up"; `/readyz` reaches Postgres and is the one
  for "can it serve". The image is distroless (no shell, no curl), so probe
  from the host or the proxy rather than with a container healthcheck.
- **Retention, log format, the alert catalogue, rebuilding derived rows,
  upgrade and rollback rules**: [docs/OPERATIONS.md](../../docs/OPERATIONS.md).
- **Slack mirroring, the release bucket for client self-upgrade, domain
  aliases**: the commented-out variables at the end of `.env.example`, each
  with what it does.
