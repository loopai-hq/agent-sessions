# Working in this repository with a coding agent

Instructions for AI coding agents (and the humans driving them). Everything
here is verifiable: run the command, read the file. The policy for
contributions made with an agent is in
[CONTRIBUTING.md](CONTRIBUTING.md#ai-assisted-contributions); the
architecture and its invariants are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## What this is

Two binaries in one Go module (`go 1.26.0`, `toolchain go1.26.8`):
`cmd/loop-sessions` (the laptop agent, standard library only) and
`server/cmd/loop-sessions-server` (Postgres, dashboard, API). Events are
the source of truth; everything else is derived from them.

## Commands

```sh
make build                      # ./loop-sessions for this machine
make lint                       # gofmt, go vet, shellcheck, golangci-lint (when installed)
go test -race ./...             # the unit suites
bash .github/scripts/identifier-gate.sh   # must print "identifier-gate: OK"
```

Integration tests need a database and the build tag; they skip themselves
otherwise:

```sh
docker run -d --name loop-sessions-test -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=loop_sessions_test -p 55433:5432 postgres:16
LOOP_SESSIONS_TEST_DSN=postgres://postgres:test@127.0.0.1:55433/loop_sessions_test \
  go test -race -tags integration ./server/...
```

The dashboard without a database or a Firebase project:

```sh
LOOP_WEB_DEMO=:8099 go test ./server/web -run TestServeDemo -timeout 0
```

golangci-lint must be v2.14.0 built with the toolchain in `go.mod`
(`go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0`);
an older build refuses a go1.26 target. Local secret scan:
`gitleaks dir --config .gitleaks.toml --exit-code 1 .` and
`gitleaks git --config .gitleaks.toml --exit-code 1 .`.

## Rules the checks enforce

- `cmd/**` and `internal/**` import the standard library and this module
  only (`depguard` in `.golangci.yml`). A new `require` for the client is a
  design issue, not a change.
- Every exported identifier has a doc comment; no builtin is shadowed; every
  `//nolint` names the linter and a reason (`nolintlint`).
- Scrub fixtures (`internal/scrub`) are synthetic: a value that matches a
  real key's shape but was never issued.
- No real identifiers anywhere: no cloud project ids, hostnames, bucket
  names, chat ids, personal addresses or transcript content. Use
  `__PROJECT__`, `sessions.example.com`, `you@example.com`. The gate script
  is the mechanical check; the reviewer is the rest.
- British spelling in prose and comments (`misspell` runs with the neutral
  locale; do not "fix" `honoured`).
- Conventional commit subjects (`feat(scope):`, `fix(scope):`, `docs:`,
  `refactor(scope):`, `test(scope):`, `chore:`); the body says why. Say
  which of `event.CaptureSchema` and `store.DerivedSchema` you considered.

## Where things are

| Change | Look in |
|---|---|
| a CLI verb or flag | `cmd/loop-sessions/main.go` (`usage()` must match the README's command block) |
| what an event carries | `internal/event/event.go` (`CaptureSchema`; read `docs/UPGRADES.md` first) |
| what the server derives | `server/store/runner.go`, `server/store/derive/` (`DerivedSchema`) |
| a new environment variable | `server/app/config.go`, then `docs/OPERATIONS.md` and `examples/docker-compose/.env.example` |
| a route | `server/app/app.go` (`routes`), then the package that owns it; `docs/OPERATIONS.md` lists them |
| the release layout | `Makefile`, `install/install.sh`, `internal/upgrade`, `server/app/download.go`: one contract, change all four |
| a log message | it is part of the log contract in `docs/OPERATIONS.md` and the GCP alert policies; do not rename one casually |
| a dashboard page | `server/web/templates/`, `server/web/static/app.css`; rerun `make screenshots` if a screenshotted page changed |

## What an agent must not do here

- Never commit secrets, real identifiers, or paths from the machine you run
  on. Agents paste what they see.
- Never merge, never push to `main`, never create a release or a tag.
- Never weaken the identifier gate, a scrub rule, a lint rule or a test to
  get green. Exclude a lint finding only with a written reason at the
  exclusion.
- Never add a `Signed-off-by` line: there is no DCO, and only a person can
  certify one.
- Never leave the first review to the maintainers: run the commands above,
  read your own diff, and remove anything you cannot justify.
- Never answer a review comment on the author's behalf; the person replies.
