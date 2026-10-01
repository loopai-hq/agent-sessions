package upgrade

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// The tests use shell scripts as stand-in binaries. That is not a shortcut
// around the hard part: a script with a shebang is executed by the same exec
// path a real binary is, so the sanity check that runs the download before
// trusting it is genuinely exercised rather than stubbed out.

const (
	oldVersion = "0000001"
	newVersion = "9999999"
)

func script(version string) []byte {
	return []byte("#!/bin/sh\necho " + version + "\n")
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func assetName() string {
	return fmt.Sprintf("%s_%s_%s", binaryPrefix, runtime.GOOS, runtime.GOARCH)
}

// machine is a fake install: a home directory with a binary in it, and a release
// host that serves some manifest and some bytes.
type machine struct {
	t        *testing.T
	home     string
	binDir   string
	execPath string
	srv      *httptest.Server

	// What the release host serves. Set per test.
	manifest string
	payload  []byte
	// status, when non-zero, is returned for the asset download.
	status int
	// truncate serves only the first n bytes of payload.
	truncate int
}

func newMachine(t *testing.T) *machine {
	t.Helper()
	home := t.TempDir()
	binDir := filepath.Join(home, ".local", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	execPath := filepath.Join(binDir, binaryPrefix)
	if err := os.WriteFile(execPath, script(oldVersion), 0o755); err != nil {
		t.Fatal(err)
	}

	m := &machine{t: t, home: home, binDir: binDir, execPath: execPath}
	m.payload = script(newVersion)
	m.manifest = fmt.Sprintf("%s  %s\n", sum(m.payload), assetName())

	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/SHA256SUMS"):
			_, _ = w.Write([]byte(m.manifest))
		case strings.HasSuffix(r.URL.Path, assetName()):
			if m.status != 0 {
				w.WriteHeader(m.status)
				return
			}
			body := m.payload
			if m.truncate > 0 && m.truncate < len(body) {
				body = body[:m.truncate]
			}
			_, _ = w.Write(body)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *machine) opts() Options {
	return Options{
		BaseURL:  m.srv.URL,
		ExecPath: m.execPath,
		Version:  oldVersion,
		HomeDir:  m.home,
		Logf:     func(f string, a ...any) { m.t.Logf(f, a...) },
	}
}

// onDisk is what the binary's name points at now.
func (m *machine) onDisk() []byte {
	m.t.Helper()
	b, err := os.ReadFile(m.execPath)
	if err != nil {
		m.t.Fatalf("read the binary: %v", err)
	}
	return b
}

// leftovers reports staging files the install directory should never keep.
func (m *machine) leftovers() []string {
	m.t.Helper()
	entries, err := os.ReadDir(m.binDir)
	if err != nil {
		m.t.Fatal(err)
	}
	var found []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".loop-sessions-") {
			found = append(found, e.Name())
		}
	}
	return found
}

// The point of the feature: a machine running something other than what is
// published ends up running what is published, and the replacement is the exact
// bytes the manifest named.
func TestAStaleBinaryIsReplacedByThePublishedOne(t *testing.T) {
	m := newMachine(t)

	res, err := Run(context.Background(), m.opts())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Stale || !res.Upgraded {
		t.Fatalf("stale=%v upgraded=%v, want both true", res.Stale, res.Upgraded)
	}
	if got := sum(m.onDisk()); got != sum(m.payload) {
		t.Errorf("the binary on disk is %s, want the published %s", short(got), short(sum(m.payload)))
	}
	if l := m.leftovers(); len(l) > 0 {
		t.Errorf("staging files were left behind: %v", l)
	}
}

// The replaced file has to remain executable. A correct download that lands
// without its mode is a machine where every hook fails with "permission
// denied", which looks like a broken harness rather than a broken upgrade.
func TestTheReplacedBinaryIsStillExecutable(t *testing.T) {
	m := newMachine(t)
	if _, err := Run(context.Background(), m.opts()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	fi, err := os.Stat(m.execPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o111 == 0 {
		t.Fatalf("mode is %v, which nothing can execute", fi.Mode().Perm())
	}
	out, err := runIt(t, m.execPath)
	if err != nil {
		t.Fatalf("the installed binary does not run: %v", err)
	}
	if strings.TrimSpace(out) != newVersion {
		t.Errorf("the installed binary reports %q, want %q", strings.TrimSpace(out), newVersion)
	}
}

// A machine already running the published build must not download it again.
// Every SessionStart on every machine reaches this path, so a needless download
// here is the whole fleet re-fetching the same 7 MB forever.
func TestAnUpToDateBinaryIsNeitherDownloadedNorTouched(t *testing.T) {
	m := newMachine(t)
	// Publish exactly what is already installed.
	m.payload = script(oldVersion)
	m.manifest = fmt.Sprintf("%s  %s\n", sum(m.payload), assetName())

	var assetHits int
	m.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/SHA256SUMS") {
			_, _ = w.Write([]byte(m.manifest))
			return
		}
		assetHits++
		_, _ = w.Write(m.payload)
	})

	before := m.onDisk()
	res, err := Run(context.Background(), m.opts())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Stale || res.Upgraded {
		t.Errorf("stale=%v upgraded=%v, want both false", res.Stale, res.Upgraded)
	}
	if assetHits != 0 {
		t.Errorf("the binary was downloaded %d time(s) despite being current", assetHits)
	}
	if string(m.onDisk()) != string(before) {
		t.Error("the binary was rewritten despite being current")
	}
}

// Bytes that do not match the manifest served beside them are refused, and the
// working binary is left exactly as it was.
//
// This is the case that decides whether this package is safe to run unattended:
// the alternative to refusing is a machine whose agent has been replaced with
// something nobody published.
func TestADownloadThatDoesNotMatchItsChecksumIsRefused(t *testing.T) {
	m := newMachine(t)
	// The manifest promises the new script; the host serves something else.
	m.payload = script(newVersion)
	m.manifest = fmt.Sprintf("%s  %s\n", sum(m.payload), assetName())
	before := m.onDisk()
	m.payload = []byte("#!/bin/sh\necho tampered\n")

	res, err := Run(context.Background(), m.opts())
	if err == nil {
		t.Fatal("a mismatched download was accepted")
	}
	if !strings.Contains(err.Error(), "checksum") {
		t.Errorf("the error does not name the checksum: %v", err)
	}
	if res.Upgraded {
		t.Error("Upgraded is set despite the refusal")
	}
	if string(m.onDisk()) != string(before) {
		t.Fatal("the working binary was replaced by bytes that failed verification")
	}
	if l := m.leftovers(); len(l) > 0 {
		t.Errorf("the rejected download was left in the install directory: %v", l)
	}
}

// A truncated transfer is the innocent version of the same thing and must reach
// the same outcome, because a half-written agent is one that never runs again.
func TestATruncatedDownloadLeavesTheWorkingBinaryInPlace(t *testing.T) {
	m := newMachine(t)
	m.truncate = 4
	before := m.onDisk()

	if _, err := Run(context.Background(), m.opts()); err == nil {
		t.Fatal("a truncated download was accepted")
	}
	if string(m.onDisk()) != string(before) {
		t.Fatal("a truncated download replaced the working binary")
	}
	if l := m.leftovers(); len(l) > 0 {
		t.Errorf("the partial download was left behind: %v", l)
	}
}

// A download whose checksum is right and which still cannot run is discarded.
// The stale binary captures; a binary that exits non-zero on every hook does
// not, and cannot be fixed remotely because fixing it is its own job.
func TestADownloadThatCannotRunIsDiscarded(t *testing.T) {
	m := newMachine(t)
	// Correctly published, correctly hashed, and not executable by any loader.
	m.payload = []byte("\x7fELF this is not a real binary")
	m.manifest = fmt.Sprintf("%s  %s\n", sum(m.payload), assetName())
	before := m.onDisk()

	res, err := Run(context.Background(), m.opts())
	if err == nil {
		t.Fatal("a build that cannot run was installed")
	}
	if !strings.Contains(err.Error(), "does not run") && !strings.Contains(err.Error(), "printed no version") {
		t.Errorf("the error does not say the build failed to run: %v", err)
	}
	if res.Upgraded {
		t.Error("Upgraded is set despite discarding the build")
	}
	if string(m.onDisk()) != string(before) {
		t.Fatal("a build that cannot run replaced the working binary")
	}
}

// A tree-local build belongs to whoever is sitting at the machine. Replacing it
// with a release build would silently delete work in progress, and it would do
// it to the one person able to notice — which is why the guard is first, before
// any network call.
func TestADevelopmentBuildIsNeverReplaced(t *testing.T) {
	m := newMachine(t)
	var hits int
	m.srv.Config.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ })

	o := m.opts()
	o.Version = DevVersion
	before := m.onDisk()

	res, err := Run(context.Background(), o)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Skipped == "" {
		t.Error("a dev build was not reported as skipped")
	}
	if hits != 0 {
		t.Errorf("a dev build still made %d request(s) to the release host", hits)
	}
	if string(m.onDisk()) != string(before) {
		t.Fatal("a development build was replaced")
	}
}

// Somebody who has switched this off must stay switched off. An auto-updater
// with no working opt-out is one people delete rather than configure.
func TestTheEnvironmentCanTurnItOff(t *testing.T) {
	m := newMachine(t)
	t.Setenv("LOOP_SESSIONS_NO_UPGRADE", "1")
	before := m.onDisk()

	res, err := Run(context.Background(), m.opts())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Skipped != "disabled by environment" {
		t.Errorf("Skipped = %q, want the environment reason", res.Skipped)
	}
	if string(m.onDisk()) != string(before) {
		t.Fatal("the binary was replaced despite the opt-out")
	}
}

// A binary outside the home directory was installed by something with more
// authority than this process — a package manager, an image build, an admin —
// and fighting that tooling is how an agent becomes the thing people uninstall.
func TestABinaryOutsideTheHomeDirectoryIsLeftAlone(t *testing.T) {
	m := newMachine(t)
	elsewhere := t.TempDir() // deliberately not under m.home
	other := filepath.Join(elsewhere, binaryPrefix)
	if err := os.WriteFile(other, script(oldVersion), 0o755); err != nil {
		t.Fatal(err)
	}

	o := m.opts()
	o.ExecPath = other

	res, err := Run(context.Background(), o)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Skipped != "binary is not under the home directory" {
		t.Errorf("Skipped = %q, want the ownership reason", res.Skipped)
	}
	if string(mustRead(t, other)) != string(script(oldVersion)) {
		t.Fatal("a binary outside the home directory was replaced")
	}
}

// A platform the release does not build for keeps what it has, and says so
// rather than erroring. Reporting this as a failure would put a red line in
// every log on a machine that is working perfectly well.
func TestAPlatformWithNoPublishedBuildIsNotAFailure(t *testing.T) {
	m := newMachine(t)
	m.manifest = fmt.Sprintf("%s  %s_plan9_mips\n", sum(m.payload), binaryPrefix)
	before := m.onDisk()

	res, err := Run(context.Background(), m.opts())
	if err != nil {
		t.Fatalf("a platform with no build reported an error: %v", err)
	}
	if !strings.Contains(res.Skipped, "no published build") {
		t.Errorf("Skipped = %q, want the missing-asset reason", res.Skipped)
	}
	if string(m.onDisk()) != string(before) {
		t.Fatal("the binary changed despite no published build for this platform")
	}
}

// Several editor windows opening at once fire several SessionStart hooks, each
// starting a daemon, each reaching this code against the same file. Whatever
// order they interleave in, the binary at the end must be one whole published
// build and never a mixture of two writers.
func TestConcurrentUpgradesLeaveOneWholeBinary(t *testing.T) {
	m := newMachine(t)

	const racers = 8
	var wg sync.WaitGroup
	errs := make([]error, racers)
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = Run(context.Background(), m.opts())
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("racer %d: %v", i, err)
		}
	}
	if got := sum(m.onDisk()); got != sum(m.payload) {
		t.Fatalf("the binary is %s, which is neither the old nor the published build", short(got))
	}
	if l := m.leftovers(); len(l) > 0 {
		t.Errorf("concurrent upgrades left staging files behind: %v", l)
	}
	out, err := runIt(t, m.execPath)
	if err != nil || strings.TrimSpace(out) != newVersion {
		t.Fatalf("after the race the binary runs as %q (%v), want %q", strings.TrimSpace(out), err, newVersion)
	}
}

// busy is the error os.StartProcess returns when Linux refuses to execute a
// file some process still holds open for writing. It is injected rather than
// produced: producing it needs a fork in one goroutine to land between
// another goroutine's close and exec, which no test can schedule.
func busy(path string) error {
	return &fs.PathError{Op: "fork/exec", Path: path, Err: syscall.ETXTBSY}
}

// stubVersionRun replaces the sanity check's exec for one test and restores it.
func stubVersionRun(t *testing.T, f func(ctx context.Context, path string) ([]byte, error)) {
	t.Helper()
	saved := runVersion
	runVersion = f
	t.Cleanup(func() { runVersion = saved })
}

// A staging file the kernel still counts as open for writing is not a bad
// build; it is a good build whose exec came a few microseconds early. The
// check has to wait that out rather than discard a download that was correct.
func TestASanityCheckWaitsOutATextFileBusyExec(t *testing.T) {
	m := newMachine(t)
	var calls int
	stubVersionRun(t, func(ctx context.Context, path string) ([]byte, error) {
		calls++
		if calls <= 3 {
			return nil, busy(path)
		}
		return execVersion(ctx, path)
	})

	res, err := Run(context.Background(), m.opts())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.Upgraded {
		t.Fatal("the build was not installed")
	}
	if calls != 4 {
		t.Errorf("the sanity check ran %d time(s), want 4: three busy answers and one real run", calls)
	}
	if got := sum(m.onDisk()); got != sum(m.payload) {
		t.Errorf("the binary on disk is %s, want the published %s", short(got), short(sum(m.payload)))
	}
	if l := m.leftovers(); len(l) > 0 {
		t.Errorf("staging files were left behind: %v", l)
	}
}

// The wait is a budget, not a promise. A file that stays busy is discarded
// with the errno intact, and the working binary is untouched.
func TestASanityCheckGivesUpOnAFileThatStaysBusy(t *testing.T) {
	m := newMachine(t)
	before := m.onDisk()
	var calls int
	stubVersionRun(t, func(_ context.Context, path string) ([]byte, error) {
		calls++
		return nil, busy(path)
	})

	res, err := Run(context.Background(), m.opts())
	if err == nil {
		t.Fatal("a build that never became runnable was installed")
	}
	if !errors.Is(err, syscall.ETXTBSY) {
		t.Errorf("the error lost the errno: %v", err)
	}
	if calls != busyTries {
		t.Errorf("the sanity check ran %d time(s), want the budget of %d", calls, busyTries)
	}
	if res.Upgraded {
		t.Error("Upgraded is set despite discarding the build")
	}
	if string(m.onDisk()) != string(before) {
		t.Fatal("a build that never ran replaced the working binary")
	}
	if l := m.leftovers(); len(l) > 0 {
		t.Errorf("staging files were left behind: %v", l)
	}
}

// The pauses between tries sit inside the caller's context. An upgrade that is
// cancelled mid-wait returns then, not after the whole budget.
func TestASanityCheckStopsWaitingWhenTheUpgradeIsCancelled(t *testing.T) {
	m := newMachine(t)
	before := m.onDisk()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	stubVersionRun(t, func(_ context.Context, path string) ([]byte, error) {
		calls++
		cancel()
		return nil, busy(path)
	})

	_, err := Run(ctx, m.opts())
	if err == nil {
		t.Fatal("a cancelled upgrade installed a build")
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.ETXTBSY) {
		t.Errorf("the error should carry both the cancellation and the errno: %v", err)
	}
	if calls != 1 {
		t.Errorf("the sanity check ran %d time(s) after cancellation, want 1", calls)
	}
	if string(m.onDisk()) != string(before) {
		t.Fatal("a cancelled upgrade replaced the working binary")
	}
	if l := m.leftovers(); len(l) > 0 {
		t.Errorf("staging files were left behind: %v", l)
	}
}

// A release host that is down, or answering with something else, must not be
// able to break a working install.
func TestAReleaseHostThatFailsCannotBreakTheInstall(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*machine)
	}{
		{"the asset 404s", func(m *machine) { m.status = http.StatusNotFound }},
		{"the asset 500s", func(m *machine) { m.status = http.StatusInternalServerError }},
		{"the manifest is empty", func(m *machine) { m.manifest = "" }},
		{"the manifest is garbage", func(m *machine) { m.manifest = "<html>404 not found</html>" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMachine(t)
			tc.setup(m)
			before := m.onDisk()

			// Errors are allowed here; damage is not.
			_, _ = Run(context.Background(), m.opts())

			if string(m.onDisk()) != string(before) {
				t.Fatal("a failing release host changed the installed binary")
			}
			if l := m.leftovers(); len(l) > 0 {
				t.Errorf("staging files were left behind: %v", l)
			}
		})
	}
}

// The manifest is the one thing parsed from the network, so the shapes it can
// arrive in are pinned here rather than discovered on somebody's laptop.
func TestParseManifest(t *testing.T) {
	good := strings.Repeat("a", 64)
	asset := "loop-sessions_darwin_arm64"
	for _, tc := range []struct {
		name, body, want string
	}{
		{"the shape make release writes", good + "  " + asset + "\n", good},
		{"binary mode marks the name with a star", good + " *" + asset + "\n", good},
		{"a manifest listing several platforms", "b" + strings.Repeat("c", 63) + "  loop-sessions_linux_amd64\n" + good + "  " + asset + "\n", good},
		{"uppercase hex is still hex", strings.ToUpper(good) + "  " + asset + "\n", good},
		{"a path in the name still matches on the base", good + "  dist/" + asset + "\n", good},
		{"an absent asset", good + "  loop-sessions_linux_arm64\n", ""},
		{"a digest of the wrong length", strings.Repeat("a", 40) + "  " + asset + "\n", ""},
		{"a digest that is not hex", strings.Repeat("z", 64) + "  " + asset + "\n", ""},
		{"an html error page", "<html><body>nope</body></html>", ""},
		{"nothing at all", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseManifest(tc.body, asset); got != tc.want {
				t.Errorf("parseManifest = %q, want %q", got, tc.want)
			}
		})
	}
}

// The asset name is a contract with the Makefile release target and with
// install.sh. If it drifts, installs keep working and upgrades silently stop,
// which is the worst of both: nobody notices until a fleet is months stale.
func TestTheAssetNameMatchesWhatTheReleaseTargetPublishes(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatalf("read the Makefile: %v", err)
	}
	if !strings.Contains(string(makefile), "BIN        := "+binaryPrefix) {
		t.Errorf("the Makefile does not build %q; the published asset names and this package have drifted", binaryPrefix)
	}
	installer, err := os.ReadFile(filepath.Join("..", "..", "install", "install.sh"))
	if err != nil {
		t.Fatalf("read install.sh: %v", err)
	}
	if !strings.Contains(string(installer), `BIN_NAME="`+binaryPrefix+`"`) {
		t.Errorf("install.sh does not install %q; the published asset names and this package have drifted", binaryPrefix)
	}
}

// runIt executes the installed file the way a hook would, which is the only
// way to prove the swap produced something that actually works.
func runIt(t *testing.T, path string) (string, error) {
	t.Helper()
	out, err := exec.Command(path, "version").Output()
	return string(out), err
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
