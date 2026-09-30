# loop-sessions build and release.
#
# The release target produces exactly what install/install.sh and the agent's
# self-upgrade expect to find on the release host: one binary per platform,
# named loop-sessions_<os>_<arch>, a SHA256SUMS manifest covering all of them,
# and latest.json, which names the build (version, commit, date, capture
# schema) and the digest of every asset. Those names are a contract between
# this file, that script, internal/upgrade and server/app/download.go;
# changing one without the others breaks installs silently, since the
# installer would simply report that no build exists for the platform.

BIN        := loop-sessions
PKG        := ./cmd/loop-sessions
DIST       := dist

# Version comes from git when available. A tagged build gets the tag; anything
# else gets a describe string that names the commit, so a binary in the wild can
# always be traced back to a tree. COMMIT is the full sha for latest.json, which
# is what the fleet evaluator compares health reports against; a 7-character
# prefix is enough for a person and not enough for a machine.
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)

# BUILD_DATE is the one input that used to make two builds of one commit
# differ: it was the wall clock, so every `make release` stamped a fresh
# instant into the binary through LDFLAGS and every SHA256SUMS came out
# different. The rule now is, in order of precedence:
#
#   1. an explicit BUILD_DATE on the command line or in the environment is
#      honoured as before;
#   2. SOURCE_DATE_EPOCH (the reproducible-builds.org convention, seconds
#      since the epoch) is converted to the same RFC 3339 UTC form; GNU date
#      spells the conversion `-d @N`, BSD/macOS date spells it `-r N`, and
#      both are tried in that order;
#   3. with a .git in the tree, the committer time of HEAD in UTC. That is a
#      property of the commit, so anyone building the same commit with the
#      same toolchain gets the same bytes, and latest.json's build_date names
#      when the code was finished rather than when someone ran make;
#   4. with no .git (a `git archive` export) the wall clock, which is the
#      only time such a tree has. Those builds are not expected to be
#      reproducible and the binary carries no vcs stamp to compare anyway.
#
# It is evaluated exactly once here and exported, so the manifest recipe
# (a sub-make) sees the same value the binaries were stamped with; with `?=`
# it was a recursive variable and the two readings disagreed by seconds.
ifndef BUILD_DATE
ifdef SOURCE_DATE_EPOCH
BUILD_DATE := $(shell date -u -d @$(SOURCE_DATE_EPOCH) +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r $(SOURCE_DATE_EPOCH) +%Y-%m-%dT%H:%M:%SZ)
else
BUILD_DATE := $(shell test -e .git && TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd 2>/dev/null)
endif
endif
ifeq ($(strip $(BUILD_DATE)),)
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
endif
export BUILD_DATE

# The capture schema the binary emits, read from the constant rather than
# typed here so latest.json cannot claim a version the code does not.
CAPTURE_SCHEMA := $(shell sed -n 's/^const CaptureSchema = \([0-9][0-9]*\)$$/\1/p' internal/event/event.go)

# Reproducibility, and the reasons:
#   CGO_ENABLED=0  a static binary that runs on a machine without a toolchain,
#                  and removes the host C library from the output.
#   -trimpath      strips local filesystem paths, so two people building the same
#                  commit get byte-identical output and no home directory leaks
#                  into the shipped binary.
#   -s -w          drops the symbol table and DWARF; smaller download.
# Together with the BUILD_DATE rule above and the toolchain line in go.mod,
# these make `make release` at one commit produce one SHA256SUMS, whoever
# runs it; CI builds twice from a fresh clone and diffs the two to prove it.
GOFLAGS    := -trimpath

# Stamped into the binary so an installed agent works with no flags. It is not
# a secret: it is a URL people paste into a browser anyway. There is no default:
# set ENDPOINT to the base URL of the server you run, and a binary built
# without one asks for --endpoint or LOOP_SESSIONS_ENDPOINT at install time
# (cmd/loop-sessions/setup.go, resolveEndpoint) rather than silently enrolling
# against a placeholder host:
#
#   make release ENDPOINT=https://sessions.example.com
#
# The binaries on GitHub Releases are built with ENDPOINT empty for exactly
# that reason: a public download must not carry one organisation's server.
#
# No OAuth client id is stamped alongside it. Enrollment signs in through the
# server's own page against the server's Firebase project, so the agent holds
# no client credential at all.
#
# BuildDate is stamped for the builds the toolchain cannot stamp itself (a git
# archive has no .git and gets no vcs.time); the health report carries
# whichever the binary has.
ENDPOINT   ?=
LDFLAGS    := -s -w -X main.Version=$(VERSION) \
              -X main.BuildDate=$(BUILD_DATE) \
              -X main.defaultEndpoint=$(ENDPOINT)
BUILDENV   := CGO_ENABLED=0

PLATFORMS  := darwin/arm64 darwin/amd64 linux/amd64 linux/arm64

# macOS ships shasum; most Linux images ship sha256sum.
SHA256 := $(shell command -v sha256sum 2>/dev/null || echo "shasum -a 256")

# Shell scripts that must parse and pass shellcheck, split by dialect so each
# is checked as what its shebang says it is: the installer and uninstaller
# are POSIX sh because people pipe them to /bin/sh, the GCP helpers are bash.
SCRIPTS_SH   := install/install.sh install/uninstall.sh examples/deploy-gcp/ci-setup.sh
SCRIPTS_BASH := .github/scripts/identifier-gate.sh \
                .github/scripts/changelog-section.sh \
                examples/deploy-gcp/monitoring/apply.sh \
                examples/deploy-gcp/analytics/provision.sh \
                examples/deploy-gcp/analytics/scheduler.sh \
                examples/deploy-gcp/analytics/import-dv-sessions.sh

# The third-party notices file shipped with every binary and image, and the
# template that renders it (see the notices target).
NOTICES          := THIRD_PARTY_NOTICES.md
NOTICES_TEMPLATE := .github/scripts/third-party-notices.tmpl

.DEFAULT_GOAL := help
.PHONY: help build test lint fmt release verify clean notices screenshots

help: ## show this help
	@printf 'loop-sessions %s\n\n' '$(VERSION)'
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-12s\033[0m %s\n", $$1, $$2}'
	@printf '\n'

build: ## build for this machine into ./loop-sessions
	$(BUILDENV) go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN) $(PKG)
	@printf 'built %s %s\n' '$(BIN)' '$(VERSION)'

test: ## run the full suite with the race detector
	go test -race ./...

# gofmt -l prints the names of unformatted files and exits zero, so the exit
# status has to be derived from whether it printed anything. Without this the
# target passes on a badly formatted tree.
#
# golangci-lint runs only when both the binary and .golangci.yml are present:
# a contributor without the binary still gets gofmt, vet and shellcheck, and a
# tree without the config is not silently linted with somebody's defaults. CI
# installs the pinned version, so there it always runs.
lint: ## fail if anything is unformatted, vet-unclean or fails shellcheck/golangci-lint
	@out="$$(gofmt -l . 2>/dev/null)"; \
	if [ -n "$$out" ]; then \
		printf 'gofmt: these files need formatting:\n%s\n' "$$out" >&2; \
		exit 1; \
	fi
	@printf 'gofmt: clean\n'
	go vet ./...
	@printf 'go vet: clean\n'
	@if command -v shellcheck >/dev/null 2>&1; then \
		shellcheck -s sh $(SCRIPTS_SH) && shellcheck -s bash $(SCRIPTS_BASH) && printf 'shellcheck: clean\n'; \
	else \
		printf 'shellcheck: not installed, skipping\n'; \
	fi
	@for s in $(SCRIPTS_SH); do sh -n "$$s" || exit 1; done && printf 'sh -n: clean\n'
	@for s in $(SCRIPTS_BASH); do bash -n "$$s" || exit 1; done && printf 'bash -n: clean\n'
	@if command -v golangci-lint >/dev/null 2>&1 && [ -f .golangci.yml ]; then \
		golangci-lint run ./... && printf 'golangci-lint: clean\n'; \
	else \
		printf 'golangci-lint: binary or .golangci.yml missing, skipping\n'; \
	fi

fmt: ## format everything
	gofmt -w .

# A release is cut from a clean checkout, or not at all. A build that a fleet
# once ran for weeks carried vcs.modified=true and a vcs.revision that names
# no commit in this repository, so nobody could reproduce its bytes; refusing
# a dirty tree is how that stops recurring. The tree check alone is not
# enough: the toolchain stamps a linked worktree with its primary checkout's
# HEAD (it looks for a .git directory, and a worktree has a .git file), so a
# clean worktree yields a binary whose agent_commit the fleet evaluator can
# never match to latest.json. The stamp is therefore compared with COMMIT
# after the build, before any manifest is written. ALLOW_DIRTY=1 skips both
# checks for a scratch build against a scratch endpoint, never for anything
# that reaches a bucket.
release: clean ## cross-compile all platforms, write dist/SHA256SUMS and dist/latest.json
	@if [ -z "$(ALLOW_DIRTY)" ] && [ -n "$$(git status --porcelain 2>/dev/null)" ]; then \
		printf 'release: the working tree is dirty; commit or stash first (ALLOW_DIRTY=1 overrides for scratch builds):\n' >&2; \
		git status --porcelain >&2; \
		exit 1; \
	fi
	@test -n "$(CAPTURE_SCHEMA)" || { printf 'release: cannot read CaptureSchema from internal/event/event.go\n' >&2; exit 1; }
	@mkdir -p $(DIST)
	@for p in $(PLATFORMS); do \
		os="$${p%/*}"; arch="$${p#*/}"; \
		out="$(DIST)/$(BIN)_$${os}_$${arch}"; \
		printf 'building %s\n' "$$out"; \
		$(BUILDENV) GOOS=$$os GOARCH=$$arch \
			go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o "$$out" $(PKG) || exit 1; \
	done
	@if [ -z "$(ALLOW_DIRTY)" ]; then \
		stamp="$$(go version -m $(DIST)/$(BIN)_linux_amd64)"; \
		rev="$$(printf '%s\n' "$$stamp" | sed -n 's/.*vcs\.revision=\([0-9a-f]*\).*/\1/p')"; \
		if [ "$$rev" != "$(COMMIT)" ]; then \
			printf 'release: the binary is stamped vcs.revision=%s but latest.json would say %s; build from the checkout that owns .git (a linked worktree is stamped with its primary checkout) or pass ALLOW_DIRTY=1 for a scratch build\n' "$${rev:-none}" '$(COMMIT)' >&2; \
			exit 1; \
		fi; \
		if printf '%s\n' "$$stamp" | grep -q 'vcs.modified=true'; then \
			printf 'release: the binary carries vcs.modified=true; refusing to write a manifest for it\n' >&2; \
			exit 1; \
		fi; \
	fi
	@cd $(DIST) && $(SHA256) $(BIN)_* > SHA256SUMS
	@printf '\n%s\n' 'SHA256SUMS:'
	@cat $(DIST)/SHA256SUMS
	@$(MAKE) --no-print-directory manifest
	@printf '\n%s\n' 'go version -m (the stamp the fleet will report):'
	@go version -m $(DIST)/$(BIN)_linux_amd64 | grep -E '^\s+(build\s+(vcs|-ldflags|CGO_ENABLED|-trimpath)|path|mod)' || true
	@printf '\nUpload the contents of %s/ to the release host.\n' '$(DIST)'
	@printf 'install.sh fetches <base>/<channel>/SHA256SUMS, <base>/<channel>/latest.json and <base>/<channel>/$(BIN)_<os>_<arch>.\n'

# latest.json names the build and its assets. Written from SHA256SUMS rather
# than from the build loop so the two cannot disagree about a digest.
manifest:
	@test -f $(DIST)/SHA256SUMS || { printf 'no %s/SHA256SUMS; run make release first\n' '$(DIST)' >&2; exit 1; }
	@{ \
		printf '{\n  "version": "%s",\n  "commit": "%s",\n  "build_date": "%s",\n  "capture_schema": %s,\n  "assets": {' \
			'$(VERSION)' '$(COMMIT)' '$(BUILD_DATE)' '$(CAPTURE_SCHEMA)'; \
		awk '{ name = $$2; sub(/^\*/, "", name); printf "%s\n    \"%s\": \"%s\"", (NR > 1 ? "," : ""), name, $$1 } END { printf "\n  }\n}\n" }' $(DIST)/SHA256SUMS; \
	} > $(DIST)/latest.json
	@printf '\n%s\n' 'latest.json:'
	@cat $(DIST)/latest.json

verify: ## re-check dist/ against its own manifest
	@test -f $(DIST)/SHA256SUMS || { printf 'no %s/SHA256SUMS; run make release first\n' '$(DIST)' >&2; exit 1; }
	@cd $(DIST) && if command -v sha256sum >/dev/null 2>&1; then \
		sha256sum -c SHA256SUMS; \
	else \
		shasum -a 256 -c SHA256SUMS; \
	fi
	@test -f $(DIST)/latest.json || { printf 'no %s/latest.json; run make release first\n' '$(DIST)' >&2; exit 1; }
	@if command -v python3 >/dev/null 2>&1; then \
		python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["version"] and d["commit"] and d["assets"], d' $(DIST)/latest.json && printf 'latest.json: valid\n'; \
	fi

# The binaries and the server image redistribute the server's dependencies,
# and MIT and BSD both require their notices to travel with the copy. This
# renders every module linked into any package in the tree (the client is
# stdlib-only, so the list is the server's) through the template, then
# appends the Go distribution's own licence, which covers the standard
# library every binary is built from. go-licenses is installed with
# `go install github.com/google/go-licenses/v2@latest`. GOROOT is passed
# explicitly because go-licenses tells the standard library apart from
# dependencies by the GOROOT compiled into itself, which is the wrong one
# when the go.mod toolchain line has switched the build to a downloaded Go.
# CI regenerates the file and fails when it differs from the committed copy.
notices: ## regenerate THIRD_PARTY_NOTICES.md with go-licenses
	@command -v go-licenses >/dev/null 2>&1 || { printf 'notices: go-licenses not found; go install github.com/google/go-licenses/v2@latest\n' >&2; exit 1; }
	@GOROOT="$$(go env GOROOT)" go-licenses report ./... \
		--ignore "$$(go list -m)" \
		--template $(NOTICES_TEMPLATE) > $(NOTICES).tmp
	@{ printf '## The Go standard library\n\nEvery binary and the server image are built from the Go standard library, whose licence follows.\n\n```\n'; \
	   cat "$$(go env GOROOT)/LICENSE"; printf '```\n'; } >> $(NOTICES).tmp
	@mv $(NOTICES).tmp $(NOTICES)
	@printf 'wrote %s\n' '$(NOTICES)'

# Screenshots for the README, taken from the fixture-backed demo server with
# the in-tree Playwright script (server/web/screenshots.cjs).
screenshots: ## regenerate docs/images/*.png from the demo server
	node server/web/screenshots.cjs

clean: ## remove build output
	rm -rf $(DIST) $(BIN)
