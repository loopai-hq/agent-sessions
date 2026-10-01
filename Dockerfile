# The server image. Build context is the repository root:
#
#   docker build --build-arg VERSION=v0.1.0 --build-arg REVISION=<sha> .
#
# The server imports internal/event and internal/health, which the client also
# uses, so the module has to be present whole; that is why this file sits at
# the root rather than beside the GCP example that deploys it
# (examples/deploy-gcp/cloudbuild.yaml and release.yml.example both build it
# from here). The published image is
# ghcr.io/loopai-hq/loop-sessions-server, built by .github/workflows/release.yml
# on every tag; the same file builds on every pull request so a change that
# breaks the image is found before it is merged.
#
# The base is pinned to the patch release named by go.mod's toolchain line.
# Dependabot's docker entry moves this line on its own (a minor bump is
# ignored there so the Go major/minor only ever changes through go.mod) and
# nothing moves go.mod's toolchain with it, so ci.yml's build job fails
# when the two disagree; bump both in one change.
FROM golang:1.26.8-bookworm AS build

WORKDIR /src

# Dependencies are copied and downloaded before the source so an edit to a
# handler does not invalidate the module cache layer. go.sum is optional
# because the client is dependency-free and the file may not exist yet.
COPY go.mod ./
COPY go.su[m] ./
RUN go mod download

COPY . .

# MAIN_PKG is an argument rather than a constant because the server entrypoint
# is owned by a different part of the tree and may move. Overriding it here is
# cheaper than rewriting this file when it does.
ARG MAIN_PKG=./server/cmd/loop-sessions-server
ARG VERSION=dev
# BUILD_DATE is RFC 3339 UTC, the commit time by the Makefile's rule. There
# is no fallback here because a Dockerfile cannot compute one that the LABEL
# below would also see; every caller (ci.yml, release.yml, cloudbuild.yaml,
# the deploy README) computes it from git with a wall-clock fallback for a
# tree without .git. Empty is tolerated, not wanted: the server then shows
# no build time on the fleet page and the `created` label is empty.
ARG BUILD_DATE=

# CGO off produces a static binary, which is the only kind the distroless
# static base can run. -trimpath keeps build-machine paths out of panics, which
# would otherwise leak a developer's home directory into production logs.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.buildDate=${BUILD_DATE}" \
      -o /out/server ${MAIN_PKG}

# The final image carries no shell, no package manager and no libc. There is
# nothing for an attacker who reaches code execution to pivot into, and nothing
# for a CVE scanner to find, because the only files are our binary and the
# licence notices for what is linked into it. Templates and CSS are compiled
# into it by go:embed, so there is no asset directory to mount or to get wrong.
FROM gcr.io/distroless/static-debian12:nonroot

# The args are re-declared because a build stage does not inherit them. The
# same three values are stamped into the binary above and into the labels
# here, so `docker inspect` and the server's startup log line agree.
ARG VERSION=dev
ARG BUILD_DATE=
ARG REVISION=

# OCI annotations. `source` is what links the package to this repository on
# GHCR (and what makes the package inherit the repository's visibility);
# `revision` is the full commit so it can be matched against the VCS stamp
# the fleet page compares; `version`/`created` mirror the -X flags.
LABEL org.opencontainers.image.source="https://github.com/loopai-hq/loop-sessions" \
      org.opencontainers.image.title="loop-sessions-server" \
      org.opencontainers.image.description="The loop-sessions server: ingests, stores and serves AI coding session transcripts" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${BUILD_DATE}"

COPY --from=build /out/server /server
COPY THIRD_PARTY_NOTICES.md /THIRD_PARTY_NOTICES.md

# nonroot is uid 65532. Cloud Run does not require it, but a container that
# cannot write to its own filesystem cannot be persuaded to.
USER nonroot:nonroot

EXPOSE 8080
ENTRYPOINT ["/server"]
