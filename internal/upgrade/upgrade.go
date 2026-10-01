// Package upgrade replaces this machine's agent binary when the published one
// differs from it.
//
// Why staleness is a checksum and not a version number: the release host already
// publishes SHA256SUMS for every platform, and the same hash that answers "am I
// running what is published" also answers "did the bytes I just downloaded
// arrive intact". A version string would need a new endpoint, an ordering
// convention, and a decision about what to do when the server publishes an older
// build on purpose. Comparing bytes has none of those questions: the release
// host is the statement of what should be running, and anything else is stale,
// including a deliberate rollback.
//
// Why this runs in the daemon and not in a hook: a hook is a short-lived
// process per lifecycle event, PostToolUse fires tens of thousands of times in
// a working week, and the harness kills async hooks at the teardown of a
// `claude -p` run, so a download started there is a download that may never
// finish. The daemon is started detached at a session's first prompt, has no
// terminal, already talks to the server on a cadence, and checks at most once
// per interval per machine, so a slow download costs nobody anything.
//
// The failure posture throughout is "log it and leave the machine as it was".
// An agent that cannot upgrade is an inconvenience; an agent that half-replaces
// its own binary is a machine that no longer captures anything and cannot be
// fixed remotely, because the thing that would fix it is the binary that broke.
package upgrade

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

// DevVersion is the stamp a tree-local `go build` leaves. A binary carrying it
// was built by somebody sitting at the machine, and replacing that with a
// release build would delete work they have not committed yet.
const DevVersion = "dev"

// binaryPrefix is the published asset naming contract, shared with the Makefile
// release target and install/install.sh. All three have to agree; this is the
// copy that has a test.
const binaryPrefix = "loop-sessions"

// downloadLimit bounds what will be read from the release host. The real
// binaries are around 7 MB; this leaves room for growth while refusing to fill
// somebody's disk if that URL ever starts answering with something else.
const downloadLimit = 128 << 20

// busyTries and busyPause bound how long sanityCheck waits for a staging file
// the kernel still counts as open for writing. sanityCheck explains the race;
// the budget only has to outlast another fork reaching its exec, which takes
// microseconds, and 200 ms of patience against that is cheap next to throwing
// away a 7 MB download that was correct.
const (
	busyTries = 10
	busyPause = 20 * time.Millisecond
)

// Options configures one upgrade attempt. Every field that names the outside
// world has a test seam, because the interesting cases here are the ones that
// are painful to produce for real: a truncated download, a manifest that omits
// this platform, a binary somebody else owns.
type Options struct {
	// BaseURL is the service root. Releases are read from
	// <BaseURL>/dl/<Channel>/. Required.
	BaseURL string
	// Channel is the release channel: "latest" for the fleet, "canary" for
	// the machines that take a build first. Comes from the config.
	Channel string
	// ExecPath is the binary to replace. Defaults to the running executable
	// with symlinks resolved, which is what must be replaced — rewriting a
	// symlink would leave the real file stale and every later check confused.
	ExecPath string
	// Version is this build's stamp. DevVersion never upgrades.
	Version string
	// GOOS and GOARCH select the published asset. Default to this build's.
	GOOS, GOARCH string
	// HomeDir bounds what may be replaced. Defaults to os.UserHomeDir.
	HomeDir string
	// HTTPClient is used for both the manifest and the download.
	HTTPClient *http.Client
	// Logf receives one line per decision. Never nil after defaults.
	Logf func(string, ...any)
}

// Result describes what happened, in enough detail that a log line can say why
// nothing did. Skipped is empty when a decision was actually reached.
type Result struct {
	// Skipped names the guard that stopped this before any network call.
	Skipped string
	// Stale reports that the published bytes differ from the local ones.
	Stale bool
	// Upgraded reports that the binary on disk was replaced.
	Upgraded bool
	// LocalSHA and PublishedSHA are the compared digests, for the log.
	LocalSHA, PublishedSHA string
}

func (o *Options) applyDefaults() error {
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	if o.Channel == "" {
		o.Channel = "latest"
	}
	if o.GOOS == "" {
		o.GOOS = runtime.GOOS
	}
	if o.GOARCH == "" {
		o.GOARCH = runtime.GOARCH
	}
	if o.HTTPClient == nil {
		// A bounded client rather than http.DefaultClient: this runs unattended
		// in a background process, and a request with no timeout there is a
		// goroutine waiting for a server that is never coming back.
		o.HTTPClient = &http.Client{Timeout: 5 * time.Minute}
	}
	if o.BaseURL == "" {
		return errors.New("upgrade: no base URL, so there is nowhere to check")
	}
	if o.ExecPath == "" {
		exe, err := os.Executable()
		if err != nil {
			return fmt.Errorf("upgrade: cannot find my own binary: %w", err)
		}
		o.ExecPath = exe
	}
	// Resolved whether it was derived or supplied, and resolved on both sides of
	// the later home-directory comparison. Two reasons, and the second is the
	// one that bites: the installer may have placed a symlink, and the file that
	// has to change is its target. But also, a path can be spelled two ways on a
	// perfectly ordinary machine — on macOS /var is a symlink to /private/var —
	// and comparing an unresolved binary path against a resolved home directory
	// decides they are unrelated. That reads as "somebody else owns this
	// binary", so the machine quietly never upgrades and says so only in a log
	// nobody is watching.
	resolved, err := filepath.EvalSymlinks(o.ExecPath)
	if err != nil {
		return fmt.Errorf("upgrade: cannot resolve %s: %w", o.ExecPath, err)
	}
	o.ExecPath = resolved
	if o.HomeDir == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("upgrade: cannot find the home directory: %w", err)
		}
		o.HomeDir = h
	}
	return nil
}

// Run checks the published manifest and replaces the local binary when it
// differs.
//
// It returns an error only for conditions a caller might want to count. Every
// ordinary reason not to upgrade — a development build, a binary somebody else
// owns, a platform with no published asset — comes back as a Result with
// Skipped set, because those are the normal state of somebody's machine and
// reporting them as failures would teach people to ignore the log.
func Run(ctx context.Context, o Options) (Result, error) {
	if err := o.applyDefaults(); err != nil {
		return Result{}, err
	}

	if o.Version == DevVersion || o.Version == "" {
		o.Logf("upgrade: this is a %s build, so it is left alone", DevVersion)
		return Result{Skipped: "development build"}, nil
	}
	if os.Getenv("LOOP_SESSIONS_NO_UPGRADE") != "" {
		o.Logf("upgrade: LOOP_SESSIONS_NO_UPGRADE is set, so nothing is replaced")
		return Result{Skipped: "disabled by environment"}, nil
	}
	if skip := o.ownership(); skip != "" {
		return Result{Skipped: skip}, nil
	}

	asset := fmt.Sprintf("%s_%s_%s", binaryPrefix, o.GOOS, o.GOARCH)
	base := strings.TrimSuffix(o.BaseURL, "/") + "/dl/" + o.Channel

	published, err := o.publishedSHA(ctx, base, asset)
	if err != nil {
		return Result{}, err
	}
	if published == "" {
		// Not an error: a platform nobody publishes for is a platform this
		// machine simply keeps its current binary on.
		o.Logf("upgrade: the manifest lists no %s, so there is nothing to move to", asset)
		return Result{Skipped: "no published build for " + o.GOOS + "/" + o.GOARCH}, nil
	}

	local, err := fileSHA(o.ExecPath)
	if err != nil {
		return Result{}, fmt.Errorf("upgrade: cannot hash the running binary: %w", err)
	}
	res := Result{LocalSHA: local, PublishedSHA: published}
	if local == published {
		o.Logf("upgrade: already running the published build (%s)", short(local))
		return res, nil
	}
	res.Stale = true
	o.Logf("upgrade: local build %s differs from published %s; replacing", short(local), short(published))

	if err := o.replace(ctx, base+"/"+asset, published); err != nil {
		return res, err
	}
	res.Upgraded = true
	o.Logf("upgrade: replaced %s with the published build (%s); running daemons on this build restart in place shortly, new sessions start on it", o.ExecPath, short(published))
	return res, nil
}

// ownership reports why this binary must not be touched, or "".
//
// Two conditions, both about not fighting whoever else manages this file. A
// binary outside the home directory was put there by something with more
// authority than an unattended background process — a package manager, an
// administrator, an image build — and replacing it would be both a surprise and
// a thing their tooling undoes. A directory this process cannot write is the
// same statement made by the filesystem, and it is checked separately because
// the rename needs the DIRECTORY writable, not the file.
func (o *Options) ownership() string {
	home, err := filepath.EvalSymlinks(o.HomeDir)
	if err != nil {
		home = o.HomeDir
	}
	rel, err := filepath.Rel(home, o.ExecPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		o.Logf("upgrade: %s is outside %s, so whatever installed it owns it", o.ExecPath, home)
		return "binary is not under the home directory"
	}
	dir := filepath.Dir(o.ExecPath)
	probe, err := os.CreateTemp(dir, ".loop-sessions-writable-*")
	if err != nil {
		o.Logf("upgrade: %s is not writable, so the binary cannot be replaced here: %v", dir, err)
		return "install directory is not writable"
	}
	name := probe.Name()
	_ = probe.Close()
	_ = os.Remove(name)
	return ""
}

// publishedSHA fetches the manifest and returns the digest recorded for asset,
// or "" when the manifest does not mention it.
func (o *Options) publishedSHA(ctx context.Context, base, asset string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/SHA256SUMS", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "loop-sessions/"+o.Version)
	resp, err := o.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("upgrade: cannot read the release manifest: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("upgrade: the release manifest answered %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("upgrade: cannot read the release manifest: %w", err)
	}
	return parseManifest(string(body), asset), nil
}

// parseManifest reads sha256sum output: one "<hex>  <name>" per line. Names are
// compared by base name because the manifest is written from inside dist/, and
// one written from elsewhere later would carry a path.
func parseManifest(body, asset string) string {
	for _, line := range strings.Split(body, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) != 2 {
			continue
		}
		// The "*" marks binary mode in sha256sum output and is not part of the
		// name.
		sum, name := strings.ToLower(fields[0]), strings.TrimPrefix(fields[1], "*")
		if filepath.Base(name) != asset {
			continue
		}
		if len(sum) != sha256.Size*2 {
			continue
		}
		if _, err := hex.DecodeString(sum); err != nil {
			continue
		}
		return sum
	}
	return ""
}

// replace downloads the asset and swaps it in.
//
// The order is load-bearing. The download lands in a temporary file in the
// destination directory — the same directory, so that the final step is a
// rename within one filesystem, which is atomic; anywhere else risks a
// cross-device rename that degrades to copy-then-delete and can be observed
// half-done. Nothing is made executable until its digest matches, and nothing is
// renamed into place until it has been run once, because a binary that cannot
// start is worse than a stale one: the stale one still captures.
func (o *Options) replace(ctx context.Context, url, want string) error {
	dir := filepath.Dir(o.ExecPath)
	tmp, err := os.CreateTemp(dir, ".loop-sessions-upgrade-*")
	if err != nil {
		return fmt.Errorf("upgrade: cannot stage a download in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	// Removed on every path that does not end in a successful rename, so a
	// failed upgrade does not leave the install directory filling with
	// half-downloaded binaries nobody will ever look at.
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "loop-sessions/"+o.Version)
	resp, err := o.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("upgrade: cannot download the new build: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upgrade: downloading the new build answered %s", resp.Status)
	}

	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, sum), io.LimitReader(resp.Body, downloadLimit))
	if err != nil {
		return fmt.Errorf("upgrade: the download stopped after %d byte(s): %w", n, err)
	}
	// fsync before the rename. Without it, a crash between the two can leave the
	// binary's name pointing at a file whose contents never reached the disk,
	// which is precisely the unrecoverable state this package exists to avoid.
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("upgrade: cannot flush the download to disk: %w", err)
	}
	// Closed here, before the sanity check, and not merely before the rename:
	// Linux refuses to exec a file that any process holds open for writing, so
	// a staging file still open in this process could never pass its own check.
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("upgrade: cannot close the download: %w", err)
	}

	if got := hex.EncodeToString(sum.Sum(nil)); got != want {
		// The one case worth being loud about: the bytes served did not match
		// the manifest served alongside them. Truncation explains it innocently;
		// so does something on the path rewriting the download.
		return fmt.Errorf("upgrade: refusing a download whose checksum is %s, not the published %s", short(got), short(want))
	}
	if err := os.Chmod(tmpName, 0o755); err != nil { //nolint:gosec // G302: the downloaded build has to be executable
		return fmt.Errorf("upgrade: cannot make the new build executable: %w", err)
	}
	if err := o.sanityCheck(ctx, tmpName); err != nil {
		return err
	}
	if err := os.Rename(tmpName, o.ExecPath); err != nil {
		return fmt.Errorf("upgrade: cannot move the new build into place: %w", err)
	}
	return nil
}

// sanityCheck runs the downloaded binary once before trusting it with the name
// every hook on this machine invokes.
//
// The checksum already proves the bytes are the ones that were published, so
// this is not an integrity check. It catches a published build that is intact
// and still cannot start here: the wrong architecture recorded in the manifest,
// a dynamically linked binary on a machine without the library, a release
// nobody verified. Skipped when cross-checking another platform's asset, where
// failing to execute would prove nothing.
//
// The exec is retried on ETXTBSY and on nothing else. Linux refuses to execute
// a file that any process holds open for writing (that is what "text file
// busy" means), and a fork carries every descriptor the parent has open into
// the child until the child's own exec closes them (golang/go#22315; cmd/go
// retries its own tool execs for exactly this reason). So two upgrades in one
// process race: A forks its check while B's staging file is still open for
// writing, and B's exec of a file B has since closed is refused until A's
// child reaches its exec. In production each daemon is its own process and
// inherits nothing from its neighbours, so the window there is whatever else
// this process forks in the same few microseconds; in the concurrent test,
// eight upgrades share one process and hit it routinely. Either way the
// download is good and the refusal is momentary, and giving up on it would
// discard a build that runs. The wait is bounded and sits inside the caller's
// context, because a file that stays busy is not this package's problem to
// outlast.
func (o *Options) sanityCheck(ctx context.Context, path string) error {
	if o.GOOS != runtime.GOOS || o.GOARCH != runtime.GOARCH {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var (
		out []byte
		err error
	)
	for try := 1; ; try++ {
		out, err = runVersion(ctx, path)
		if !errors.Is(err, syscall.ETXTBSY) || try == busyTries {
			break
		}
		o.Logf("upgrade: the new build is still open for writing somewhere (try %d of %d); waiting %s before running it again", try, busyTries, busyPause)
		select {
		case <-ctx.Done():
			return fmt.Errorf("upgrade: the downloaded build was still busy when the check ran out of time (%w), so it is discarded: %w", ctx.Err(), err)
		case <-time.After(busyPause):
		}
	}
	if err != nil {
		return fmt.Errorf("upgrade: the downloaded build does not run, so it is discarded: %w", err)
	}
	if len(strings.TrimSpace(string(out))) == 0 {
		return errors.New("upgrade: the downloaded build printed no version, so it is discarded")
	}
	return nil
}

// runVersion is the seam sanityCheck execs through and the tests replace. It
// is a variable rather than a direct call because the one exec failure this
// package handles deliberately, ETXTBSY, cannot be produced on demand: it
// needs a fork in one goroutine to land between another goroutine's close and
// exec, which no test can schedule. Injecting the errno is the only way the
// retry gets a test that is red without it.
var runVersion = execVersion

// execVersion runs the downloaded build once, the way a hook would.
func execVersion(ctx context.Context, path string) ([]byte, error) {
	return exec.CommandContext(ctx, path, "version").Output()
}

// Digest is the sha256 of the file at path, hex encoded: the identity the
// manifest names a build by, and what a running daemon compares its own
// binary against to notice an upgrade.
func Digest(path string) (string, error) { return fileSHA(path) }

// Probe runs the binary at path once (its version verb) the way the upgrade
// checks a download before renaming it into place: a file that is still
// being written, truncated, or not executable fails here instead of in an
// exec that would replace a working process image with nothing.
func Probe(ctx context.Context, path string) error {
	o := &Options{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Logf: func(string, ...any) {}}
	return o.sanityCheck(ctx, path)
}

func fileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// short is what goes in a log line. A full digest wraps in a terminal and buries
// the sentence it was meant to qualify.
func short(sum string) string {
	if len(sum) > 12 {
		return sum[:12]
	}
	return sum
}
