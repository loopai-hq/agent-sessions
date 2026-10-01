# Contributing

Thanks for looking. This page says what a change needs before it can merge,
and why. Where to ask for help is in [SUPPORT.md](SUPPORT.md); how decisions
are made is in [GOVERNANCE.md](GOVERNANCE.md).

## Before you start

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and the package doc
comment of whatever you are touching. The doc comments say why a package is
shaped the way it is, and most of the rules below exist because a plausible
change broke one of those reasons.

Small fixes can go straight to a pull request. For anything that changes
behaviour, what an event carries, or the release layout, open an issue first
so the design can be discussed before the code.

## Building and testing

```sh
make lint && go test -race ./...
```

That is what CI runs (`.github/workflows/ci.yml`), plus the integration
suite against Postgres 15 and 17, a macOS leg so the darwin-only files
execute, a cross-compile of the client built twice to prove the bytes are
reproducible, a build of the server image, govulncheck, gitleaks and a
dependency review. `make lint` is `gofmt -l`, `go vet`, `shellcheck` over
every shell script, and `golangci-lint` with the repository's `.golangci.yml`
when the binary is installed. Install the version CI runs:

```sh
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
```

A golangci-lint built with a Go older than `go.mod`'s toolchain refuses to
run, so use `go install`, not a package manager's older build. `make lint`
is expected to pass on macOS and FreeBSD checkouts too.

Integration tests (`*_integration_test.go`, build tag `integration`) need a
Postgres 15+ database and skip themselves without one:

```sh
LOOP_SESSIONS_TEST_DSN=postgres://postgres:test@127.0.0.1:55433/loop_sessions_test \
  go test -race -tags integration ./server/...
```

Each run works inside its own schema and drops it afterwards. The dashboard
can be looked at without a database or a Firebase project:
`LOOP_WEB_DEMO=:8099 go test ./server/web -run TestServeDemo -timeout 0`.

Before opening a pull request, also run the two local scans CI runs:

```sh
bash .github/scripts/identifier-gate.sh
gitleaks dir --config .gitleaks.toml --exit-code 1 . && gitleaks git --config .gitleaks.toml --exit-code 1 .
```

New behaviour comes with a test. A change to `internal/scrub` comes with a
fixture that shows the case, and the fixture must be synthetic: a value that
matches a real key's shape but was never issued. `.editorconfig` and
`.gitattributes` carry the formatting defaults; most editors read them.

## Code style

The linter enforces most of this; the rest is what a reviewer looks for.

- **Errors wrap with `%w` and are checked with `errors.Is` and `errors.As`.**
  Sentinels are package-level `ErrSomething = errors.New(...)` values that
  callers can match; a returned error names the operation
  (`fmt.Errorf("store: apply migrations: %w", err)`) and is wrapped once per
  layer, never formatted with `%v` into a string that loses the chain.
  Compare errors by identity (`!=`) only where identity is the assertion,
  and say so in a `//nolint:errorlint // ...` reason.
- **Every package has a package doc comment, including the two `main`
  packages.** It says what the package is for and why it is shaped that way,
  in prose; `internal/scrub/scrub.go` and `internal/drain/drain.go` are the
  model. The comment is the design note; there is no separate one.
- **Every exported identifier has a doc comment** that starts with its name
  (`revive`'s `exported` rule, without the stuttering check).
- **No builtin is shadowed** (`revive`'s `redefines-builtin-id`): not `max`,
  `min`, `cap`, `copy`, `close`, `any`, `clear`, `real`, `recover`.
- **Tests are table-driven** where there is more than one case: a
  `cases := []struct{...}` with a `name`, run with `t.Run`. Test helpers
  are exported for the package's own tests only; test fakes implement
  interfaces and may ignore arguments, which is why `_test.go` files are
  exempt from a few rules in `.golangci.yml`.
- **Comments explain why, in full sentences,** and quote the measurement
  when there was one ("two seconds on every tool call is the difference
  between a tool nobody notices and one everybody uninstalls"). A comment
  that restates the code is deleted in review.
- **British spelling** in prose and comments (`honoured`, `behaviour`,
  `organisation`); `misspell` runs with the neutral locale so it does not
  fight this.
- **Every `//nolint` names the linter and gives a reason**
  (`//nolint:gosec // G306: settings.json is Claude Code's file`); an
  unused directive is itself a lint failure.
- **The client is standard library only.** `depguard` fails the build on a
  third-party import under `cmd/**` or `internal/**` (the server's
  `server/cmd/**` is not covered). A new `require` in `go.mod` for the
  client is a design discussion, not a routine change.
- The one GOOS-dependent file (`internal/spool/diskfree_unix.go`) is covered
  by a path exclusion in `.golangci.yml` rather than inline directives, so
  the lint passes on every platform it is run on.

[Effective Go](https://go.dev/doc/effective_go) and
[Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) cover
everything not listed.

## Two constants to think about

- `event.CaptureSchema` (`internal/event/event.go`): bump it when the agent
  will extract something better or different from the same transcript.
- `store.DerivedSchema` (`server/store`): bump it when the server will derive
  something better or different from events already stored.

`docs/UPGRADES.md` explains both, what a bump costs, and when not to bump.
Say in the commit message which one you considered and why you left it
alone if you did.

## Commits

Use conventional commit subjects: `feat(scope): ...`, `fix(scope): ...`,
`docs: ...`, `refactor(scope): ...`, `test(scope): ...`, `chore: ...`. The
scope is a package or directory (`drain`, `server/ingest`, `install`).

The body explains why. The diff already says what. User-visible changes get
a line under `Unreleased` in [CHANGELOG.md](CHANGELOG.md).

## Pull requests

- One change per pull request. A refactor and the behaviour change it
  enables are two pull requests.
- The description says what problem the change solves and how you verified
  it. Paste the relevant test output when the change is not obviously
  covered by CI.
- CI must be green. A failing cross-compile or a Docker build that no longer
  builds is a failing PR.
- Keep the client dependency-free (above).
- Changes to the release layout (`Makefile` `release` target,
  `install/install.sh`, `internal/upgrade`, `server/app/download.go`) must
  update all four together. They are one contract.
- A change to a dashboard page that is screenshotted in the README
  (`/sessions`, `/sessions/{id}`, `/admin/fleet`, `/analytics`) comes with
  `make screenshots`.

## AI-assisted contributions

This project is built with AI coding agents and welcomes contributions made
the same way. The rules are about accountability, not tooling:

1. **You are the author.** You must understand every line you submit and be
   able to explain, without the tool, what it does and why. A reviewer may
   ask; if the answer is "the agent did it", the pull request is closed.
2. **Disclose non-trivial use** in the pull request description, on the
   `AI assistance:` line the template provides: the tool, and roughly what
   it produced (code, tests, docs). One sentence is enough ("Written with
   Claude Code; I rewrote the tests by hand."). Autocomplete, spell-checking
   and translation need no disclosure. A `Co-Authored-By:` or `Assisted-by:`
   commit trailer added by your tool is fine and does not replace the
   sentence.
3. **Review before you ask for review.** Run the checks yourself (`make lint
   && go test -race ./...`, the identifier gate and gitleaks above), read
   the diff, and remove anything you cannot justify. Do not leave the first
   review to us.
4. **Reply to review comments yourself.** We want to talk to you, not to a
   model.
5. **No secrets, no identifiers, no transcript content.** Agents paste what
   they see; the identifier gate catches the mechanical part and you check
   the rest (see below).
6. **Licensing.** You certify you have the right to contribute the content
   under MIT. If your tool's terms or the provenance of its output make you
   unsure, do not submit it.
7. **Security reports** must say whether an AI tool found the issue, and you
   must have reproduced it yourself before reporting (see
   [SECURITY.md](SECURITY.md)).

Maintainers use the same tools and hold themselves to the same rules.
[AGENTS.md](AGENTS.md) is the instruction file the agents read; keep it
under two hundred lines and keep every instruction in it verifiable.

## Licensing

Contributions are accepted under the same terms as the project: by opening
a pull request you agree that your contribution is licensed under the
[MIT License](LICENSE) (inbound = outbound). There is no contributor
licence agreement and no Developer Certificate of Origin sign-off; do not
add `Signed-off-by` lines.

## No company-specific identifiers or customer data

This repository is the public cut of an internal tool. Nothing in it may name
a specific organisation's infrastructure or people:

- no cloud project ids, project numbers, service-account addresses, bucket
  names, or production hostnames;
- no employee names, personal email addresses, or chat workspace, channel or
  user ids;
- no customer names and no transcript content, real or "lightly edited";
- test fixtures use `@example.com` addresses and `example.com` domains.

CI enforces the mechanical part with the identifier gate workflow
(`.github/workflows/identifier-gate.yml`), which runs
`.github/scripts/identifier-gate.sh` over the tree; a hit fails the build.
The script greps for the shape of internal identifiers (chat ids, home
directory paths, cloud hostnames and project ids, mail addresses outside the
example domains). In this repository's own CI it also runs an extended,
non-public pattern supplied through the `IDENTIFIER_GATE_PATTERNS` repository
secret; pull requests from forks run the generic pattern only, and a
maintainer re-runs the full gate before merging.

The gate cannot recognise every identifier, so the reviewer checks the rest.
When you need an example value, use the placeholders the example deployment
already uses (`__PROJECT__`, `sessions.example.com`, `you@example.com`).

## Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).

## Security

Do not report vulnerabilities in a pull request or issue. See
[SECURITY.md](SECURITY.md).
