#!/usr/bin/env bash
# Identifier gate. Exits non-zero when a file in the tree contains a
# company-internal identifier: a cloud project id or number, an employee's
# home directory, a chat workspace, channel or user id, a private hostname,
# a mail address outside the example domains.
#
# This repository is the public cut of a tree that ran one operator's private
# deployment, and that tree named the operator everywhere. None of it may
# come back, and a reviewer cannot be relied on to spot one identifier in a
# long diff, so CI greps for them. The concrete list of forbidden strings is
# deliberately not committed here, because publishing it would publish exactly
# what it guards. It arrives through an environment variable:
#
#   IDENTIFIER_GATE_PATTERNS  optional extended regex with the concrete
#                             strings that must never appear. In CI it is a
#                             repository secret, combined with the generic
#                             pattern below and never echoed. Absent (a fork
#                             pull request, a contributor's checkout) the
#                             generic pattern runs alone.
#
# The generic pattern matches the shape of such identifiers: Slack ids, macOS
# home paths, a Firebase web API key, a Compute Engine default service
# account, a Cloud Run hostname, a Google Cloud project id with its numeric
# suffix, and any mail address whose domain is not one of the reserved
# example domains. Credential-shaped strings (keys, tokens, PEM blocks) are
# gitleaks' job, configured in .gitleaks.toml.
#
# Allowed strings are removed from a line before it is re-tested, so a line
# that mixes an allowed string with a forbidden one is still caught: the two
# published contact addresses, the maintainers' GitHub handles, links to this
# organisation's public repositories, addresses on example.com, example.org,
# example.net (and the not-example.com near-misses the tests use), *.example,
# *.test, *.invalid and *.localhost, Google's public token-signing certificate
# address, and a web API key whose tail is a run of zeros (a placeholder).
#
# Carve-outs: examples/ is a deployment example built around placeholder
# values, so the project and hostname shapes would fire on every placeholder
# and are not run over it; the mail-address rule alone is, because a real
# address in an example comment is exactly the leak the carve-out would
# otherwise hide. LICENSE names the copyright holder, dist/ is build output.
#
# Usage, from the repository root:
#   .github/scripts/identifier-gate.sh                # whole tree
#   .github/scripts/identifier-gate.sh <path> [...]   # only these files or directories
# Output lines are file:line:text.
set -uo pipefail

# Single-character bracket classes ([U], [L], [b], [s]) keep this file from
# matching its own patterns when the gate runs over the repository.
DEFAULT_PATTERNS='(^|[^A-Za-z0-9])(C0|U0|S0|G0)[A-Z0-9]{8,}([^A-Za-z0-9]|$)|/[U]sers/[a-z]|~/[L]oop/|AIza[0-9A-Za-z_-]{30,}|[0-9]{12}-compute@|[a-z0-9-]+\.[a-z]+[0-9]?\.run\.app|(^|[^a-z0-9-])[a-z]+(-[a-z]+)+-[0-9]{6}([^0-9]|$)|[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[a-z]{2,}'

ALLOWED='engineering@tryloop\.ai|security@loopai\.com|github\.com/loopai-hq/(agent-sessions|loop-claude-plugins)|@[b]havathi-loop|@[s]undar-loop|[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+\.)*([A-Za-z0-9-]*[Ee]xample\.(com|org|net)|[A-Za-z0-9-]+\.(example|test|invalid|localhost))|[A-Za-z0-9._%+-]+@users\.noreply\.github\.com|noreply@[A-Za-z0-9.-]+|securetoken@system\.gserviceaccount\.com|AIza[A-Za-z]+0{8,}'

if [ -n "${IDENTIFIER_GATE_PATTERNS:-}" ]; then
  PATTERNS="${IDENTIFIER_GATE_PATTERNS}|${DEFAULT_PATTERNS}"
  echo "identifier-gate: using repository patterns + generic patterns"
else
  PATTERNS="${DEFAULT_PATTERNS}"
  echo "identifier-gate: using generic patterns only (IDENTIFIER_GATE_PATTERNS unset)"
fi

# The three files whose whole point is credential-shaped synthetic data (the
# scrubber's fixtures: a placeholder web API key, made-up DSN passwords). They
# are the same three .gitleaks.toml allowlists, for the same reason.
FIXTURES='^(\./)?(internal/scrub/scrub_test\.go|internal/receiver/receiver_test\.go|server/ingest/scrub_test\.go):'

raw_hits() {
  # Plain grep rather than git grep on purpose: it behaves the same on a CI
  # checkout, also covers untracked files in a local run, and cannot be fooled
  # by an exported tree that happens to sit inside some other git work tree.
  if [ "$#" -gt 0 ]; then
    grep -rnIE "$PATTERNS" --exclude-dir=.git --exclude-dir=examples --exclude-dir=dist --exclude=LICENSE "$@"
  else
    grep -rnIE "$PATTERNS" --exclude-dir=.git --exclude-dir=examples --exclude-dir=dist --exclude=LICENSE .
  fi | grep -vE "$FIXTURES"
}

# Strip allowed substrings, then re-test so a line that mixes an allowed string
# with a forbidden one is still caught.
hits=$(raw_hits "$@" 2>/dev/null | sed -E "s#${ALLOWED}##g" | grep -E "$PATTERNS" || true)

# The mail-address rule over examples/. Placeholder service accounts
# (<name>@<project>.iam.gserviceaccount.com) are the one extra allowed shape.
MAIL_PATTERN='[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[a-z]{2,}'
EXAMPLES_ALLOWED="${ALLOWED}|[A-Za-z0-9._-]+@[A-Za-z0-9._-]+\.iam\.gserviceaccount\.com"
example_paths=()
if [ "$#" -eq 0 ]; then
  [ -d examples ] && example_paths=(examples)
else
  for p in "$@"; do
    case "$p" in examples|examples/*|./examples|./examples/*) example_paths+=("$p") ;; esac
  done
fi
if [ "${#example_paths[@]}" -gt 0 ]; then
  example_hits=$(grep -rnIE "$MAIL_PATTERN" --exclude-dir=.git "${example_paths[@]}" 2>/dev/null | sed -E "s#${EXAMPLES_ALLOWED}##g" | grep -E "$MAIL_PATTERN" || true)
  if [ -n "$example_hits" ]; then
    hits="${hits:+$hits
}${example_hits}"
  fi
fi

if [ -n "$hits" ]; then
  echo "identifier-gate: FAILED: internal identifiers found:"
  printf '%s\n' "$hits" | while IFS= read -r line; do
    printf '::error::%s\n' "$line"
  done
  exit 1
fi
echo "identifier-gate: OK"
