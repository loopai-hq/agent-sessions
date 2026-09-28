package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/health"
)

// Stamps: the per-machine "when did this last happen" files in the state
// directory.
//
// Each daemon is born with a session and dies with it, so nothing a daemon
// remembers survives to the next one. Without these files every session start
// fetched the release manifest, sent a health report and re-checked the
// parked items, and on a machine that starts fifty promptless sessions a day
// that was fifty manifest reads and fifty health rows to learn nothing the
// previous one had not said. A stamp is a timestamp in a file; reading one is
// cheaper than any of the work it gates.
const (
	stampUpgradeCheck = "upgrade.last-check"
	stampHealthReport = "health.last-report"
	stampRedriveLast  = "redrive.last"
	stampRedriveVer   = "redrive.version"
	stampRepairLast   = "repair.last"
	// stampHooksVer is the binary version that last brought the registered
	// hooks up to its defaults, so a fleet that self-upgrades converges on a
	// new SessionEnd budget without anyone running install --hooks-only.
	stampHooksVer = "hooks.version"
	// upgradeStatusFile holds the outcome of the last upgrade check for the
	// health report, so the fleet can say "cannot reach the release host
	// since Tuesday" rather than "stale".
	upgradeStatusFile = "upgrade.json"
)

func stampPath(p config.Paths, name string) string {
	return filepath.Join(p.StateDir(), name)
}

// readStamp reads a stamp, reporting false when there is none or it is
// unreadable; both mean "never, as far as this machine knows".
func readStamp(p config.Paths, name string) (time.Time, bool) {
	b, err := os.ReadFile(stampPath(p, name))
	if err != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b)))
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func writeStamp(p config.Paths, name string, at time.Time) error {
	if err := os.MkdirAll(p.StateDir(), 0o700); err != nil {
		return err
	}
	return os.WriteFile(stampPath(p, name), []byte(at.UTC().Format(time.RFC3339Nano)+"\n"), 0o600)
}

// stampDue reports whether the stamped action is older than every, or never
// happened. A stamp from the future (a clock that stepped back) is treated as
// due rather than as a reason to wait until the clock catches up.
func stampDue(p config.Paths, name string, every time.Duration, now time.Time) bool {
	last, ok := readStamp(p, name)
	if !ok {
		return true
	}
	if last.After(now) {
		return true
	}
	return now.Sub(last) >= every
}

// readText reads a small text stamp verbatim (the binary version the parked
// items were last redriven under).
func readText(p config.Paths, name string) string {
	b, err := os.ReadFile(stampPath(p, name))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func writeText(p config.Paths, name, value string) error {
	if err := os.MkdirAll(p.StateDir(), 0o700); err != nil {
		return err
	}
	return os.WriteFile(stampPath(p, name), []byte(value+"\n"), 0o600)
}

// writeUpgradeStatus records what the last upgrade check concluded.
func writeUpgradeStatus(p config.Paths, st health.UpgradeStatus) {
	b, err := json.Marshal(st)
	if err != nil {
		return
	}
	_ = os.MkdirAll(p.StateDir(), 0o700)
	tmp := stampPath(p, upgradeStatusFile) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, stampPath(p, upgradeStatusFile))
}

// readUpgradeStatus returns the last recorded outcome, or nil when no check
// has ever run on this machine, which the report carries as an absence.
func readUpgradeStatus(p config.Paths) *health.UpgradeStatus {
	b, err := os.ReadFile(stampPath(p, upgradeStatusFile))
	if err != nil {
		return nil
	}
	var st health.UpgradeStatus
	if json.Unmarshal(b, &st) != nil {
		return nil
	}
	return &st
}
