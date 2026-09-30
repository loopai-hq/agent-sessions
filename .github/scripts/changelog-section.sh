#!/usr/bin/env bash
# Print the CHANGELOG.md section for one version, for use as release notes.
#
#   .github/scripts/changelog-section.sh v0.1.0 [CHANGELOG.md]
#
# CHANGELOG.md follows Keep a Changelog: one `## [X.Y.Z] - date` heading per
# release. This prints the body under the heading for the given tag (with or
# without the leading `v`) up to the next `## ` heading, with the link
# reference lines at the bottom of the file left out. Nothing is printed and
# the exit is 0 when the file or the section is missing, so the caller can
# fall back to GitHub's generated notes; only a malformed argument is an error.
set -euo pipefail

tag="${1:-}"
file="${2:-CHANGELOG.md}"

case "$tag" in
v[0-9]*) version="${tag#v}" ;;
[0-9]*) version="$tag" ;;
*)
  echo "changelog-section: expected a version like v0.1.0, got '${tag}'" >&2
  exit 2
  ;;
esac

[ -f "$file" ] || exit 0

awk -v want="$version" '
  /^## / {
    if (printing) exit
    # "## [0.1.0] - 2026-10-01" or "## 0.1.0 - 2026-10-01"
    line = $0
    sub(/^## +\[?/, "", line)
    sub(/[]\] ].*$/, "", line)
    if (line == want) { printing = 1; next }
  }
  printing && /^\[[^]]+\]: / { next }
  printing && !started && /^[[:space:]]*$/ { next }
  printing { started = 1; print }
' "$file" | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}'
