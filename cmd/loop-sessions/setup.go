package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/config"
	"github.com/loopai-hq/loop-sessions/internal/discovery"
	"github.com/loopai-hq/loop-sessions/internal/enroll"
	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/hooks"
)

// defaultEndpoint may be stamped at build time (make build ENDPOINT=...). It
// is not a secret: it is a URL people paste into a browser anyway.
//
// The source default is empty on purpose. This binary is built for whichever
// server an organisation runs, and a URL baked in here would be one
// deployment's address shipped to everybody else's; a build that stamps
// nothing must therefore be told where its server is at install time, either
// with --endpoint or through LOOP_SESSIONS_ENDPOINT (see resolveEndpoint).
//
// There is no client id here. Enrollment signs in through the server's own
// page against the server's Firebase project, so the only thing this binary
// needs to know is where that server is.
var defaultEndpoint = ""

// endpointEnv is the environment variable an installer or a fleet tool can set
// instead of passing --endpoint. The flag wins when both are given.
const endpointEnv = "LOOP_SESSIONS_ENDPOINT"

// resolveEndpoint decides which server this install talks to, in the order a
// person would expect: the --endpoint flag, then LOOP_SESSIONS_ENDPOINT, then
// whatever the build stamped, and finally the endpoint an earlier install on
// this machine already recorded (so re-running install to sign in again does
// not require repeating the address). With nothing to go on it returns an
// error that names both ways of supplying one, because a machine configured
// against no server would capture and never deliver.
func resolveEndpoint(flagValue string, p config.Paths) (string, error) {
	if v := strings.TrimSpace(flagValue); v != "" {
		return v, nil
	}
	if v := strings.TrimSpace(os.Getenv(endpointEnv)); v != "" {
		return v, nil
	}
	if v := strings.TrimSpace(defaultEndpoint); v != "" {
		return v, nil
	}
	if cfg, err := config.Load(p); err == nil && strings.TrimSpace(cfg.Endpoint) != "" {
		return strings.TrimSpace(cfg.Endpoint), nil
	}
	return "", fmt.Errorf("install: no server endpoint is configured; pass --endpoint https://sessions.example.com or set %s", endpointEnv)
}

// runInstall is the guided setup a person runs once.
//
// The shape is dictated by something the installer script discovered: when the
// one-liner is piped to sh, this program's stdin is the remainder of the script
// rather than a terminal, so an interactive prompt would either hang or consume
// the script as its own answers. The install script therefore stops after
// placing the binary and tells the person to run this separately. That makes
// the advertised one-liner honestly two steps, and this command has to work
// standalone rather than assuming it was called from the installer.
func runInstall(args []string) error {
	fs := flag.NewFlagSet("install", flag.ExitOnError)
	endpointFlag := fs.String("endpoint", "", "server base URL (or set "+endpointEnv+")")
	skipHooks := fs.Bool("skip-hooks", false, "do not register capture hooks")
	hooksOnly := fs.Bool("hooks-only", false, "re-register the capture hooks and change nothing else")
	skipEnroll := fs.Bool("skip-signin", false, "configure without signing in")
	skipBackfill := fs.Bool("skip-backfill", false, "do not import the history already on this machine")
	backfillSince := fs.String("backfill-since", defaultInstallWindow, "how much history to import at install; the default is everything (7d, 30d, a date)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	p := config.Paths{}
	if *hooksOnly {
		// The fleet CTA for a machine whose hooks a harness upgrade rewrote
		// away: put them back without signing in again or importing anything.
		if _, err := config.Load(p); err != nil {
			return fmt.Errorf("install --hooks-only: %w", err)
		}
		return registerHooks()
	}
	// Decided before anything is printed or scanned, so a machine with no
	// endpoint is told exactly that and nothing else changes.
	endpointURL, err := resolveEndpoint(*endpointFlag, p)
	if err != nil {
		return err
	}
	endpoint := &endpointURL
	fmt.Println("Setting up loop-sessions.")
	fmt.Println()

	// Step 1: look at the machine before changing anything, and show the person
	// what was found. Someone installing a tool that reads their coding sessions
	// is entitled to see what it located before it starts sending anything.
	fmt.Println("Looking for AI coding sessions on this machine...")
	findings := discovery.Run(discovery.Options{})
	sum := discovery.Summarize(findings, time.Now())
	fmt.Println()
	printDiscovery(sum)
	fmt.Println()
	// Said before sign-in, because it is part of what the person is agreeing to.
	fmt.Println(whoCanRead)
	fmt.Println()

	// Step 2: identity. Everything server-side joins on this, because a
	// transcript carries no identity of its own.
	cfg, err := config.Load(p)
	if err != nil && !errors.Is(err, config.ErrNotConfigured) {
		return err
	}
	if errors.Is(err, config.ErrNotConfigured) {
		cfg = config.Defaults()
	}
	cfg.Endpoint = *endpoint
	cfg.AgentVersion = Version
	if cfg.InstalledAt.IsZero() {
		cfg.InstalledAt = time.Now()
	}

	switch {
	case *skipEnroll:
		fmt.Println("Skipping sign-in. Run `loop-sessions install` again when ready.")
	case cfg.Email != "" && cfg.DeviceID != "" && hasDeviceToken(p):
		fmt.Printf("Already signed in as %s.\n", cfg.Email)
	default:
		// The credential is a separate file from the config, so it can be gone
		// while the config still names this machine. That used to read as
		// "already signed in" and left a laptop that captures and can never
		// deliver, with the only cure being to know that the fix is to delete a
		// file. Signing in again is the fix, and cfg.DeviceID is what makes it
		// re-credential the machine already on file rather than enrol a second.
		if cfg.Email != "" {
			fmt.Printf("Signing in again as %s to replace this machine's credential.\n", cfg.Email)
		}
		res, err := signIn(*endpoint, cfg.DeviceID)
		if err != nil {
			return err
		}
		cfg.Email = res.Email
		cfg.DeviceID = res.DeviceID
		if err := saveToken(p, res.DeviceToken); err != nil {
			return err
		}
		fmt.Printf("Signed in as %s.\n", res.Email)
	}

	// Step 3: remember where sessions actually live on THIS machine, rather
	// than re-deriving defaults later. A person with a non-standard layout
	// should not silently stop being captured when a tool moves its default.
	if cfg.Roots == nil {
		cfg.Roots = map[string]string{}
	}
	for _, f := range findings {
		if f.State == discovery.Found {
			cfg.Roots[string(f.Tool)] = f.Root
		}
	}

	// Without an identity there is nothing coherent to persist: every captured
	// event is attributed server-side by the enrolled account, so a config
	// without one would describe a machine that can capture but can never
	// deliver. Stopping here leaves the machine exactly as it was found rather
	// than half-configured in a state the person would have to reason about.
	if cfg.Email == "" {
		fmt.Println()
		fmt.Println("Nothing has been changed on this machine yet.")
		fmt.Println("Capture starts once you sign in:  loop-sessions install")
		return nil
	}

	if err := config.Save(p, cfg); err != nil {
		return err
	}

	// Persist what the scan found. Backfill walks discovery's result rather than
	// re-deriving where sessions live, and until now only `loop-sessions
	// discover` ever wrote this file — so a machine that had only ever run
	// install had nothing on disk for the import to read. Written after the
	// config, so the "nothing has been changed on this machine yet" path above
	// stays true.
	if err := discovery.WriteSummary(p.DiscoveryFile(), sum); err != nil {
		fmt.Printf("Could not record what was found (%v); run `loop-sessions discover` before importing history.\n", err)
	}

	// Step 4: register the hooks. This is the step that actually starts
	// capture, so it is last: everything before it is reversible by deleting a
	// directory, and this one edits a file the person owns.
	if *skipHooks {
		fmt.Println("Skipping hook registration; nothing will be captured yet.")
	} else if err := registerHooks(); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("Done. Check anytime with:  loop-sessions status")
	fmt.Println("Pause whenever you like:   loop-sessions pause")

	if len(sum.NeedsAsk) > 0 {
		fmt.Println()
		fmt.Println("One thing: these tools look installed but I could not find their sessions.")
		for _, t := range sum.NeedsAsk {
			fmt.Printf("  %s\n", t)
		}
		fmt.Println("If you use them, run: loop-sessions discover")
	}

	// Step 5: import the history that was already here. It runs last because
	// everything above it is what makes the machine work and takes seconds,
	// while this is the one step that can take minutes — so an interrupted
	// import leaves a fully working install behind rather than a half-configured
	// one. A failure here is reported and not returned for the same reason: the
	// machine is set up, capture is running, and the import is resumable.
	if *skipBackfill {
		fmt.Println()
		fmt.Println("Skipping the history already on this machine.")
		fmt.Println("Import it whenever you like:  loop-sessions backfill")
	} else if err := importAtInstall(p, cfg, *backfillSince, os.Stdout); err != nil {
		fmt.Println()
		fmt.Printf("The history import stopped: %v\n", err)
		fmt.Println("Capture is running regardless. Pick the import back up with:  loop-sessions backfill")
		logf("install: history import stopped: %v", err)
	} else {
		// This machine's history has now been read by this build's extraction,
		// so record which one that was. Without it, the first daemon to start
		// would see a machine at capture v0 and immediately re-read everything
		// that was just imported.
		//
		// Only on success: a failed or interrupted import leaves the version
		// where it was, and the re-walk that a later session performs is exactly
		// the right recovery for it.
		cfg.CaptureSchemaVersion = event.CaptureSchema
		if err := config.Save(p, cfg); err != nil {
			logf("install: could not record the capture version: %v", err)
		}
	}
	return nil
}

// signIn runs the browser half and exchanges the result for a device
// credential. deviceID is this machine's own id from a previous enrolment, or
// empty on a machine that has none to offer.
func signIn(endpoint, deviceID string) (enroll.Result, error) {
	flow, err := enroll.New(enroll.Options{Endpoint: endpoint})
	if err != nil {
		return enroll.Result{}, err
	}
	host, _ := os.Hostname()
	fmt.Println("Opening your browser to sign in with your Google account...")
	fmt.Println("(If it does not open, the URL will be printed here.)")
	return flow.Run(context.Background(), enroll.Machine{
		DeviceID:     deviceID,
		Hostname:     host,
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		AgentVersion: Version,
	})
}

// hasDeviceToken reports whether this machine still holds a usable credential.
// It reads through the same loader delivery uses, so "install thinks I am signed
// in" and "delivery says I have no credential" cannot disagree.
func hasDeviceToken(p config.Paths) bool {
	_, err := loadDeviceToken(p)
	return err == nil
}

// saveToken writes the device credential.
//
// The OS keychain is the right home for this and is the intended destination;
// until that is wired, the token lives in a 0600 file inside the client's own
// directory. That is a deliberate, stated interim rather than an oversight: the
// token grants upload rights for one device and is revocable server-side, so the
// exposure is bounded, but it is still weaker than the keychain and should not
// stay this way.
func saveToken(p config.Paths, token string) error {
	if token == "" {
		return errors.New("install: server returned no device token")
	}
	path := filepath.Join(p.Root(), "device.token")
	if err := os.MkdirAll(p.Root(), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(token), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func registerHooks() error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("install: cannot determine my own path: %w", err)
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		return fmt.Errorf("install: cannot resolve my own path: %w", err)
	}

	plan, err := hooks.Preview(hooks.Options{Binary: self})
	if err != nil {
		return err
	}
	if plan.Foreign > 0 {
		// Say this out loud. The settings file often contains hooks somebody
		// built themselves, and the reasonable fear when a tool edits it is
		// that their own work will be clobbered.
		fmt.Printf("Your settings file has %d hook(s) of your own. They will be left alone.\n", plan.Foreign)
	}
	if len(plan.Add) == 0 && len(plan.Update) == 0 {
		fmt.Println("Capture hooks are already registered.")
		return nil
	}
	if _, err := hooks.Register(hooks.Options{Binary: self}); err != nil {
		return err
	}
	if len(plan.Add) > 0 {
		fmt.Printf("Registered %d capture hooks in %s\n", len(plan.Add), plan.Path)
	}
	if len(plan.Update) > 0 {
		// An entry an earlier release wrote, brought up to this one's
		// defaults (the SessionEnd timeout, most often). Said separately so
		// a fleet operator running --hooks-only for exactly this sees it.
		fmt.Printf("Updated %d capture hook(s) in %s to this release's defaults: %s\n", len(plan.Update), plan.Path, strings.Join(plan.Update, ", "))
	}
	return nil
}

// runUninstall removes what this tool put on the machine.
//
// The shell uninstaller deliberately refuses to delete the binary while hooks
// still reference it, because a hook pointing at a missing command runs on every
// event and a non-zero hook exit is meaningful to the harness — removing a
// telemetry agent must not degrade the editor it was watching. This command is
// what makes that path clean: it unregisters first, so the binary is safe to
// remove afterwards.
func runUninstall(args []string) error {
	fs := flag.NewFlagSet("uninstall", flag.ExitOnError)
	purge := fs.Bool("purge", false, "also delete captured data waiting to upload")
	if err := fs.Parse(args); err != nil {
		return err
	}

	removed, err := hooks.Unregister(hooks.Options{})
	if err != nil {
		return fmt.Errorf("uninstall: could not clean up hooks: %w", err)
	}
	if removed > 0 {
		fmt.Printf("Removed %d capture hook(s); your own hooks were left alone.\n", removed)
	} else {
		fmt.Println("No capture hooks were registered.")
	}

	p := config.Paths{}
	if *purge {
		// Report what is being destroyed before destroying it. Anything still
		// spooled has not reached the server, so this is the one irreversible
		// action the tool can take.
		if n, bytes := spoolSize(p); n > 0 {
			fmt.Printf("Deleting %d item(s) (%s) that had not finished uploading.\n", n, humanBytes(bytes))
		}
		if err := os.RemoveAll(p.Root()); err != nil {
			return err
		}
		fmt.Printf("Deleted %s\n", p.Root())
	} else {
		fmt.Printf("Your data is still at %s\n", p.Root())
		fmt.Printf("Remove it with: rm -rf %s\n", p.Root())
	}

	fmt.Println()
	fmt.Println("Capture has stopped. The binary itself can now be removed safely.")
	return nil
}

func spoolSize(p config.Paths) (int, int64) {
	var n int
	var bytes int64
	dir := filepath.Join(p.SpoolDir(), "pending")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0
	}
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		if fi, err := e.Info(); err == nil {
			n++
			bytes += fi.Size()
		}
	}
	return n, bytes
}
