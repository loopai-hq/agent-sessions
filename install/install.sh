#!/bin/sh
#
# loop-sessions installer.
#
#   curl -fsSL https://sessions.example.com/install.sh | sh
#
# (replace https://sessions.example.com with the base URL of the server your
# organisation runs; the script needs LOOP_SESSIONS_BASE_URL to name where
# releases live, see below.)
#
# This installs a binary that reads your local AI coding session transcripts, so
# it is written to be read before it is trusted. Every step prints what it is
# about to do. Nothing needs root. Nothing is installed outside your home
# directory. The download is checksum-verified before it is ever made
# executable, and if verification fails the file is deleted rather than kept.
#
# Environment:
#   LOOP_SESSIONS_BASE_URL   where releases live      (REQUIRED, no default;
#                            e.g. https://sessions.example.com/dl)
#   LOOP_SESSIONS_VERSION    version or "latest"      (default latest)
#   LOOP_SESSIONS_BIN_DIR    install location         (default ~/.local/bin)
#   LOOP_SESSIONS_NO_ENROLL  set to 1 to skip sign-in
#   LOOP_SESSIONS_ENDPOINT   not read here; inherited by `loop-sessions install`
#                            when this script runs it, which a binary with no
#                            stamped endpoint (a GitHub release) needs
#
# POSIX sh only: people pipe this to /bin/sh, which on macOS is not bash.

set -eu

# There is deliberately no default release host. This script is published
# alongside a server anyone can run, and a URL written here would be one
# deployment's address baked into everybody else's installer, failing at the
# first fetch for anyone who did not edit it. The base must serve
# <base>/<channel>/SHA256SUMS, <base>/<channel>/latest.json and
# <base>/<channel>/loop-sessions_<os>_<arch> (the layout `make release`
# produces); the server's own /dl proxy is one such host. It is checked in
# preflight so a missing value is reported before anything is downloaded.
BASE_URL="${LOOP_SESSIONS_BASE_URL:-}"
CHANNEL="${LOOP_SESSIONS_VERSION:-latest}"
BIN_DIR="${LOOP_SESSIONS_BIN_DIR:-$HOME/.local/bin}"
BIN_NAME="loop-sessions"
DATA_DIR="$HOME/.loop/sessions"

TMP_DIR=""
DL_TOOL=""
SUM_TOOL=""
OS=""
ARCH=""
ASSET=""
# What the channel's latest.json says about the build, when the release host
# publishes one. Older releases have none, and the install proceeds without.
MANIFEST_VERSION=""
MANIFEST_COMMIT=""

# ---------------------------------------------------------------- output

if [ -t 1 ]; then
	B="$(printf '\033[1m')"
	R="$(printf '\033[0m')"
else
	B=""
	R=""
fi

say() { printf '%s\n' "$*"; }
step() { printf '%s==>%s %s\n' "$B" "$R" "$*"; }

# die prints to stderr and exits non-zero. Every failure path ends here, so
# every failure is both visible and detectable by a caller.
die() {
	printf '\n%serror:%s %s\n' "$B" "$R" "$1" >&2
	shift
	for line in "$@"; do
		printf '  %s\n' "$line" >&2
	done
	exit 1
}

cleanup() {
	if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
		rm -rf "$TMP_DIR"
	fi
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------- preflight

# preflight checks everything needed BEFORE anything is downloaded, so an
# unsupported machine is told so immediately rather than after a 20 MB transfer.
preflight() {
	if [ -z "$BASE_URL" ]; then
		die "LOOP_SESSIONS_BASE_URL is not set, so there is nowhere to download from." \
			"Set it to the base URL your release files are served from, for example:" \
			"  LOOP_SESSIONS_BASE_URL=https://sessions.example.com/dl sh install.sh" \
			"That base must serve <base>/latest/SHA256SUMS (see \`make release\`)."
	fi
	BASE_URL="${BASE_URL%/}"

	uname_s="$(uname -s 2>/dev/null || echo unknown)"
	case "$uname_s" in
	Darwin) OS="darwin" ;;
	Linux) OS="linux" ;;
	*)
		die "loop-sessions runs on macOS and Linux; this machine reports \"$uname_s\"." \
			"If that looks wrong, report what you are running to whoever operates your server."
		;;
	esac

	uname_m="$(uname -m 2>/dev/null || echo unknown)"
	case "$uname_m" in
	arm64 | aarch64) ARCH="arm64" ;;
	x86_64 | amd64) ARCH="amd64" ;;
	*)
		die "No loop-sessions build exists for this processor (\"$uname_m\")." \
			"Supported: Apple Silicon and Intel Macs, and 64-bit Linux on Intel or ARM."
		;;
	esac

	ASSET="${BIN_NAME}_${OS}_${ARCH}"

	if command -v curl >/dev/null 2>&1; then
		DL_TOOL="curl"
	elif command -v wget >/dev/null 2>&1; then
		DL_TOOL="wget"
	else
		die "Neither curl nor wget is available, so nothing can be downloaded." \
			"On Linux: sudo apt install curl   (or the equivalent for your distribution)"
	fi

	# Checked up front and treated as fatal. Installing without verifying the
	# download would be worse than not installing: this is a pipe-to-shell
	# installer fetching a binary that reads your transcripts.
	if command -v shasum >/dev/null 2>&1; then
		SUM_TOOL="shasum"
	elif command -v sha256sum >/dev/null 2>&1; then
		SUM_TOOL="sha256sum"
	else
		die "No SHA-256 tool found, so the download cannot be verified." \
			"Refusing to install an unverified binary." \
			"Install coreutils (Linux) or use a machine with shasum (macOS ships it)."
	fi
}

# ---------------------------------------------------------------- download

# is_loopback reports whether a URL points at this machine. Plain HTTP is
# permitted only there: a loopback request cannot be intercepted by a network
# attacker, and without it this script could never be tested end to end against
# a local release host. Everything else is HTTPS-only, including redirects, so a
# downgrade cannot be forced by the server.
is_loopback() {
	case "$1" in
	http://127.0.0.1[:/]* | http://localhost[:/]* | http://\[::1\][:/]*) return 0 ;;
	*) return 1 ;;
	esac
}

fetch() {
	# fetch <url> <destination>
	if is_loopback "$1"; then
		if [ "$DL_TOOL" = "curl" ]; then
			curl -fsSL -o "$2" "$1"
		else
			wget -q -O "$2" "$1"
		fi
		return
	fi
	if [ "$DL_TOOL" = "curl" ]; then
		# -f fails on HTTP errors instead of saving the error page as the binary.
		# --proto '=https' applies to redirects too, so an https URL cannot be
		# bounced to http partway through.
		curl -fsSL --proto '=https' --tlsv1.2 -o "$2" "$1"
	else
		wget -q --https-only -O "$2" "$1"
	fi
}

sha256_of() {
	if [ "$SUM_TOOL" = "shasum" ]; then
		shasum -a 256 "$1" | awk '{print $1}'
	else
		sha256sum "$1" | awk '{print $1}'
	fi
}

# ---------------------------------------------------------------- steps

announce() {
	say ""
	say "${B}loop-sessions installer${R}"
	say ""
	say "This will:"
	say "  1. download ${ASSET} from ${BASE_URL}/${CHANNEL}"
	say "  2. check it against the published SHA-256 checksum"
	say "  3. install it to ${BIN_DIR}/${BIN_NAME}"
	say "  4. hand off to \`${BIN_NAME} install\` to sign you in"
	say ""
	say "It will not use sudo, and will not touch anything outside your home"
	say "directory. Your existing settings and captured data in ${DATA_DIR}"
	say "are left alone."
	say ""
}

download_and_verify() {
	TMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t loop-sessions)" ||
		die "Could not create a temporary directory."

	step "Fetching checksums"
	if ! fetch "${BASE_URL}/${CHANNEL}/SHA256SUMS" "${TMP_DIR}/SHA256SUMS"; then
		die "Could not download the release manifest." \
			"URL: ${BASE_URL}/${CHANNEL}/SHA256SUMS" \
			"Check your network, or that the version \"${CHANNEL}\" exists."
	fi

	# The manifest is sha256sum output: "<hash>  <filename>", optionally with a
	# "*" before the name for binary mode. Both forms are matched.
	expected="$(awk -v want="$ASSET" '
		{ name = $2; sub(/^\*/, "", name); if (name == want) { print $1; exit } }
	' "${TMP_DIR}/SHA256SUMS")"

	if [ -z "$expected" ]; then
		die "This release has no build for ${OS}/${ARCH}." \
			"The manifest at ${BASE_URL}/${CHANNEL}/SHA256SUMS does not list ${ASSET}."
	fi

	step "Downloading ${ASSET}"
	if ! fetch "${BASE_URL}/${CHANNEL}/${ASSET}" "${TMP_DIR}/${ASSET}"; then
		die "Download failed." \
			"URL: ${BASE_URL}/${CHANNEL}/${ASSET}"
	fi

	# A truncated transfer is the failure this catches that curl does not: an
	# interrupted body can still exit zero.
	if [ ! -s "${TMP_DIR}/${ASSET}" ]; then
		die "The downloaded file is empty, so the transfer did not complete."
	fi

	step "Verifying checksum"
	actual="$(sha256_of "${TMP_DIR}/${ASSET}")"
	if [ "$actual" != "$expected" ]; then
		rm -f "${TMP_DIR}/${ASSET}"
		die "Checksum mismatch. The download has been deleted and NOT installed." \
			"expected: ${expected}" \
			"actual:   ${actual}" \
			"" \
			"This means the file you received is not the file that was published." \
			"Do not run it. Report this to whoever operates your release host."
	fi
	say "    ok: ${actual}"

	# latest.json names the build (version, full commit, build date). It is
	# optional: a channel published before it existed has none, and the
	# binary's own version is still printed below. Read with sed rather than a
	# JSON tool so the installer keeps needing nothing but a shell.
	if fetch "${BASE_URL}/${CHANNEL}/latest.json" "${TMP_DIR}/latest.json" 2>/dev/null; then
		MANIFEST_VERSION="$(sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${TMP_DIR}/latest.json" | head -n 1)"
		MANIFEST_COMMIT="$(sed -n 's/.*"commit"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${TMP_DIR}/latest.json" | head -n 1)"
	fi
}

install_binary() {
	target="${BIN_DIR}/${BIN_NAME}"

	previous=""
	if [ -x "$target" ]; then
		previous="$("$target" version 2>/dev/null || true)"
	fi

	if ! mkdir -p "$BIN_DIR" 2>/dev/null; then
		die "Could not create ${BIN_DIR}." \
			"Check the permissions on your home directory."
	fi
	if [ ! -w "$BIN_DIR" ]; then
		die "${BIN_DIR} is not writable by you." \
			"This installer never uses sudo. Either fix the permissions, or set" \
			"LOOP_SESSIONS_BIN_DIR to somewhere you own and re-run."
	fi

	# Staged inside BIN_DIR rather than in the temp directory so the final move
	# is a same-filesystem rename, which is atomic. A cross-device mv would copy,
	# and an interrupted copy would leave a half-written executable on PATH.
	staged="${BIN_DIR}/.${BIN_NAME}.new.$$"
	rm -f "$staged"
	cp "${TMP_DIR}/${ASSET}" "$staged" || die "Could not write to ${BIN_DIR}."
	chmod 0755 "$staged" || die "Could not make the binary executable."

	# Verified before it replaces anything, so a broken build cannot take out a
	# working install.
	if ! "$staged" version >/dev/null 2>&1; then
		rm -f "$staged"
		die "The downloaded binary did not run on this machine." \
			"It passed its checksum, so this is a build problem, not a corrupt download." \
			"Nothing has been changed. Report this to whoever published the release."
	fi

	# rename(2) over a running executable is safe: a process already running keeps
	# its own inode, and the next invocation picks up the new file.
	mv -f "$staged" "$target" || die "Could not install to ${target}."

	installed="$("$target" version 2>/dev/null || echo unknown)"
	if [ -n "$previous" ] && [ "$previous" != "$installed" ]; then
		step "Upgraded ${BIN_NAME} ${previous} -> ${installed}"
	elif [ -n "$previous" ]; then
		step "Reinstalled ${BIN_NAME} ${installed} (unchanged)"
	else
		step "Installed ${BIN_NAME} ${installed}"
	fi
	if [ -n "$MANIFEST_VERSION" ]; then
		# The channel's own statement of what was just installed, so the line
		# a person pastes into a ticket names the commit and not only a tag.
		say "    release ${MANIFEST_VERSION} (commit ${MANIFEST_COMMIT:-unknown}) from ${CHANNEL}"
	fi
	say "    ${target}"
}

# rc_file names the startup file for the user's actual shell, rather than
# assuming bash. Telling a zsh user to edit .bashrc is how a PATH instruction
# silently does nothing.
rc_file() {
	case "$(basename "${SHELL:-/bin/sh}")" in
	zsh) printf '%s\n' "$HOME/.zshrc" ;;
	bash)
		if [ "$OS" = "darwin" ]; then
			printf '%s\n' "$HOME/.bash_profile"
		else
			printf '%s\n' "$HOME/.bashrc"
		fi
		;;
	fish) printf '%s\n' "$HOME/.config/fish/config.fish" ;;
	*) printf '%s\n' "$HOME/.profile" ;;
	esac
}

check_path() {
	case ":${PATH}:" in
	*":${BIN_DIR}:"*)
		return 0
		;;
	esac

	rc="$(rc_file)"
	say ""
	say "${B}One more step:${R} ${BIN_DIR} is not on your PATH."
	say ""
	if [ "$(basename "${SHELL:-/bin/sh}")" = "fish" ]; then
		say "  echo 'fish_add_path ${BIN_DIR}' >> ${rc}"
	else
		say "  echo 'export PATH=\"${BIN_DIR}:\$PATH\"' >> ${rc}"
	fi
	say ""
	say "Then open a new terminal, or run: . ${rc}"
	say ""
	PATH_NEEDS_FIX=1
}

enroll() {
	target="${BIN_DIR}/${BIN_NAME}"

	if [ "${LOOP_SESSIONS_NO_ENROLL:-0}" = "1" ]; then
		say ""
		step "Skipping sign-in (LOOP_SESSIONS_NO_ENROLL=1)"
		say "    Run \`${BIN_NAME} install\` when you are ready."
		return 0
	fi

	# When this script is piped to sh, stdin is the script itself, not a
	# terminal, so an interactive sign-in would either hang or read the rest of
	# the script as answers. In that case we stop and print the command instead.
	if [ ! -t 0 ]; then
		say ""
		step "Ready to sign in"
		say "    Run this to finish:"
		say ""
		say "      ${target} install"
		say ""
		say "    (Sign-in is interactive, and this installer was piped to sh,"
		say "     so it cannot ask you anything right now.)"
		return 0
	fi

	say ""
	step "Signing you in"
	if ! "$target" install; then
		die "Sign-in did not complete." \
			"The binary IS installed at ${target}." \
			"Run \`${target} install\` again when ready, or \`${target} doctor\` to diagnose."
	fi
}

farewell() {
	say ""
	say "${B}Done.${R}"
	say ""
	say "  binary:   ${BIN_DIR}/${BIN_NAME}"
	say "  data:     ${DATA_DIR}"
	say "  status:   ${BIN_NAME} status"
	say "  problems: ${BIN_NAME} doctor"
	say "  pause:    ${BIN_NAME} pause"
	say "  remove:   ${BIN_NAME} uninstall"
	say ""
	if [ "${PATH_NEEDS_FIX:-0}" = "1" ]; then
		say "Remember the PATH line above, or use the full path ${BIN_DIR}/${BIN_NAME}."
		say ""
	fi
}

# ---------------------------------------------------------------- main

PATH_NEEDS_FIX=0

preflight
announce
download_and_verify
install_binary
check_path
enroll
farewell
