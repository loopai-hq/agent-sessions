// Package config is the client's on-disk settings and paths.
//
// Two properties drive the design. First, the config is written by an installer
// that a non-technical person runs and then never thinks about again, so
// defaults have to be right rather than merely present, and a missing or
// half-written file must degrade to something usable rather than to an error
// the person cannot act on. Second, the controls a user is promised (pause,
// skip a tool, exclude a path) live here, so this file is also the record of
// what someone consented to. Losing it silently would mean silently resuming
// capture somebody had turned off.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SchemaVersion is bumped when the on-disk shape changes incompatibly. It is
// written on every save so a future client can tell what it is reading rather
// than guessing from which fields happen to be present.
const SchemaVersion = 1

// DefaultChannel is the release channel a machine follows unless told
// otherwise. "canary" is the other one: the release engineer's machine and a
// volunteer or two run it so a build is exercised before the fleet moves.
const DefaultChannel = "latest"

// legacyMinFreeRatio is the disk ratio the old Defaults() wrote into every
// config file, so it arrives in Load looking like an explicit choice. It was
// the default, not a choice, and it is what dropped 17,340 events on this
// machine; the loader treats it as unset. It must equal
// spool.LegacyMinFreeRatio, which a test in this package pins without this
// package importing the spool.
const legacyMinFreeRatio = 0.05

// Config is the client's settings.
type Config struct {
	SchemaVersion int `json:"schema_version"`

	// Email identifies the person. It is set once at enrollment and is the
	// join key for everything server-side, because transcripts carry no
	// identity of their own.
	Email string `json:"email"`
	// DeviceID distinguishes this machine from the same person's other
	// machines, so a credential can be revoked per device.
	DeviceID string `json:"device_id"`
	// Endpoint is the ingest base URL.
	Endpoint string `json:"endpoint"`

	// Roots maps a tool to the session directory discovery settled on. Stored
	// rather than re-derived so a machine with a non-default layout keeps
	// working after the tool moves its default.
	Roots map[string]string `json:"roots,omitempty"`
	// SkippedTools are harnesses the user chose to exclude. Recorded so the
	// gap is visible in the fleet view rather than looking like a failure.
	SkippedTools []string `json:"skipped_tools,omitempty"`
	// ExcludePaths are project directories never to capture, including their
	// Git worktrees. This is how "do not record my personal work" is expressed.
	ExcludePaths []string `json:"exclude_paths,omitempty"`
	// CaptureOnlyPaths, when non-empty, inverts the policy to an allowlist:
	// nothing outside these directories or their Git worktrees is captured at
	// all. An allowlist is the safer shape for a machine that mixes work and
	// personal projects, and it is the setting a repo-scoped rollout would use.
	CaptureOnlyPaths []string `json:"capture_only_paths,omitempty"`

	// Paused suspends all capture and delivery. Honoured everywhere and
	// surfaced in the fleet view, so a paused machine reads as a deliberate
	// choice rather than as a broken agent.
	Paused      bool      `json:"paused,omitempty"`
	PausedSince time.Time `json:"paused_since,omitempty"`
	// PausedUntil, when set, ends the pause on its own: IsPaused reports false
	// from that moment, without a write to the config, so `pause --for 2h` on
	// a laptop closed for the day does not stay paused until somebody remembers
	// it. Zero means paused until `resume`.
	PausedUntil time.Time `json:"paused_until,omitempty"`

	// Slack holds per-user mirroring preferences. Off by default: a tool that
	// starts posting someone's work into a channel without being asked is one
	// they uninstall.
	Slack SlackPrefs `json:"slack"`

	// See the comment above SlackPrefs' constants.
	MirrorDefault string `json:"mirror_default,omitempty"`
	MirrorOnce    bool   `json:"mirror_once,omitempty"`

	// SpoolMaxBytes bounds the client's own disk footprint.
	SpoolMaxBytes int64 `json:"spool_max_bytes,omitempty"`
	// MinFreeRatio overrides the fraction of the disk the spool keeps free.
	// Zero means the spool's default policy (1%, floored at 512 MiB and capped
	// at 4 GiB). A stored 0.05 is the old default, not a choice, and is read
	// as zero; any other value is honoured within the same floor and cap.
	MinFreeRatio float64 `json:"min_free_ratio,omitempty"`

	// Channel is the release channel the self-upgrade follows: "latest" for
	// the fleet, "canary" for the machines that take a build first. Empty
	// reads as latest, so a config written before channels existed keeps
	// following the fleet.
	Channel string `json:"channel,omitempty"`

	// DisableAutoUpgrade stops the agent replacing its own binary when the
	// release host publishes a different one. Default off, so a fleet does not
	// need a rollout to receive a fix. It exists because software that updates
	// itself with no way to say no is software people delete rather than
	// configure, and because somebody pinned to a build while chasing a bug
	// needs that pin to survive their next session starting.
	DisableAutoUpgrade bool `json:"disable_auto_upgrade,omitempty"`

	// CaptureSchemaVersion is the extraction version the last full import of
	// this machine's history ran under.
	//
	// It is how the agent knows its own history is stale: when the running build
	// extracts more than the build that last walked these transcripts did, the
	// difference is visible here and nowhere else. Zero means a machine that was
	// set up before this was recorded, whose history therefore predates every
	// versioned extraction and is re-walked once.
	CaptureSchemaVersion int `json:"capture_schema_version,omitempty"`

	// InstalledAt and AgentVersion are provenance for support.
	InstalledAt  time.Time `json:"installed_at,omitempty"`
	AgentVersion string    `json:"agent_version,omitempty"`
}

// SlackPrefs controls conversation mirroring.
type SlackPrefs struct {
	// Mode is "off", "dm", or "channel". Default off.
	Mode string `json:"mode,omitempty"`
	// Channel is used when Mode is "channel". Per-user rather than global so
	// nobody can accidentally flood a shared channel for the whole company.
	Channel string `json:"channel,omitempty"`
}

// The values SlackPrefs.Mode takes.
const (
	// SlackOff is the shipped default: nothing is mirrored.
	SlackOff = "off"
	// SlackDM mirrors to the person as direct messages.
	SlackDM = "dm"
	// SlackChannel mirrors into SlackPrefs.Channel.
	SlackChannel = "channel"
)

// MirrorDefault is the standing per-machine answer to "mirror my sessions
// live?", set by `loop-sessions mirror use`. A group name or id the server
// resolves, "on" for the owner's server-side default group, empty for off.
// MirrorOnce marks the default as single-use: the next SessionStart consumes
// it, which is the cure for the stale-exported-env-var disease: a one-shot
// cannot be forgotten, because it removes itself.

// Defaults returns a Config with the settings a fresh install should have.
//
// The defaults are deliberately conservative where a wrong guess is
// irreversible (Slack off, no allowlist) and generous where it is not (spool
// size). Someone who never opens this file should still end up with a client
// that behaves the way the data policy says it does. The disk ratio is left
// unset on purpose: writing a number here is how the old 5% came to look like
// forty people's explicit choice.
func Defaults() Config {
	return Config{
		SchemaVersion: SchemaVersion,
		Roots:         map[string]string{},
		Slack:         SlackPrefs{Mode: SlackOff},
		SpoolMaxBytes: 2 << 30, // 2 GiB
		Channel:       DefaultChannel,
	}
}

// Paths resolves where the client keeps its state.
type Paths struct{ Home string }

// Root is the client's private directory.
func (p Paths) Root() string {
	if v := os.Getenv("LOOP_SESSIONS_HOME"); v != "" {
		return v
	}
	home := p.Home
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	return filepath.Join(home, ".loop", "sessions")
}

// ConfigFile is the JSON file Load reads and Save writes.
func (p Paths) ConfigFile() string { return filepath.Join(p.Root(), "config.json") }

// SpoolDir is the outbox: captured events waiting to be delivered.
func (p Paths) SpoolDir() string { return filepath.Join(p.Root(), "spool") }

// StateDir holds the daemon's markers, stamps and ledgers.
func (p Paths) StateDir() string { return filepath.Join(p.Root(), "state") }

// SeqDir holds the per-session sequence counters the hook path advances.
func (p Paths) SeqDir() string { return filepath.Join(p.Root(), "seq") }

// LogFile is the agent log every subcommand appends to.
func (p Paths) LogFile() string { return filepath.Join(p.Root(), "logs", "agent.log") }

// DiscoveryFile is where `discover` records what it found on this machine.
func (p Paths) DiscoveryFile() string { return filepath.Join(p.Root(), "discovery.json") }

// Load reads the config.
//
// A missing file is not an error: it means "not enrolled yet", which callers
// need to distinguish from "enrolled but broken". A corrupt file IS an error,
// because silently replacing it with defaults would re-enable capture that the
// user may have paused, and quietly overriding someone's stated preference is
// the worst failure this package can have.
func Load(p Paths) (Config, error) {
	b, err := os.ReadFile(p.ConfigFile())
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, ErrNotConfigured
		}
		return Config{}, fmt.Errorf("config: read: %w", err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return Config{}, fmt.Errorf("config: %s is unreadable (%w); refusing to overwrite it, "+
			"move it aside to re-enroll", p.ConfigFile(), err)
	}
	return c.withDefaults(), nil
}

// ErrNotConfigured means no config exists yet.
var ErrNotConfigured = errors.New("config: not configured; run `loop-sessions install`")

// withDefaults fills zero values from Defaults so an older config file gains
// new settings without a migration step.
func (c Config) withDefaults() Config {
	d := Defaults()
	if c.SchemaVersion == 0 {
		c.SchemaVersion = d.SchemaVersion
	}
	if c.Roots == nil {
		c.Roots = map[string]string{}
	}
	if c.Slack.Mode == "" {
		c.Slack.Mode = d.Slack.Mode
	}
	if c.SpoolMaxBytes <= 0 {
		c.SpoolMaxBytes = d.SpoolMaxBytes
	}
	if c.MinFreeRatio < 0 || c.MinFreeRatio == legacyMinFreeRatio {
		c.MinFreeRatio = 0
	}
	if c.Channel == "" {
		c.Channel = d.Channel
	}
	return c
}

// Save writes the config atomically with owner-only permissions.
//
// Atomic because a half-written config that fails to parse would, on the next
// run, look like corruption and block the client. Owner-only because it names
// the person and their endpoint.
func Save(p Paths, c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	c.SchemaVersion = SchemaVersion
	if err := os.MkdirAll(p.Root(), 0o700); err != nil {
		return fmt.Errorf("config: mkdir: %w", err)
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := p.ConfigFile() + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("config: write: %w", err)
	}
	return os.Rename(tmp, p.ConfigFile())
}

// Validate reports why a config cannot be used. Messages name the fix, because
// the person reading them may not be an engineer.
func (c Config) Validate() error {
	var problems []string
	if c.Email == "" {
		problems = append(problems, "email is missing (re-run `loop-sessions install`)")
	} else if !strings.Contains(c.Email, "@") {
		problems = append(problems, fmt.Sprintf("email %q does not look like an address", c.Email))
	}
	if c.Endpoint == "" {
		problems = append(problems, "endpoint is missing")
	} else if !strings.HasPrefix(c.Endpoint, "https://") && !strings.HasPrefix(c.Endpoint, "http://127.0.0.1") {
		// Plain http is allowed only to loopback, which is how local testing
		// works; anything else would ship transcripts in the clear.
		problems = append(problems, "endpoint must be https (or http on 127.0.0.1 for local testing)")
	}
	switch c.Slack.Mode {
	case "", SlackOff, SlackDM:
	case SlackChannel:
		if c.Slack.Channel == "" {
			problems = append(problems, "slack mode is \"channel\" but no channel is set")
		}
	default:
		problems = append(problems, fmt.Sprintf("slack mode %q is not one of off, dm, channel", c.Slack.Mode))
	}
	if ch := c.Channel; ch != "" && ch != DefaultChannel && ch != "canary" {
		problems = append(problems, fmt.Sprintf("channel %q is not one of latest, canary", ch))
	}
	if len(problems) > 0 {
		return fmt.Errorf("config is not usable: %s", strings.Join(problems, "; "))
	}
	return nil
}

// ShouldCapture reports whether a session in cwd may be captured, and why not
// when the answer is no.
//
// The reason string exists so a user can ask "why is this session missing" and
// get a real answer. Silent exclusion is indistinguishable from a bug, and a
// capture tool that cannot explain its own gaps does not get trusted.
func (c Config) ShouldCapture(cwd string) (bool, string) {
	if c.IsPaused(time.Now()) {
		return false, "capture is paused"
	}
	clean := filepath.Clean(cwd)
	var currentRepo string
	var repoResolved bool
	matches := func(base string) bool {
		if underPath(clean, base) {
			return true
		}
		if !repoResolved {
			currentRepo, _ = gitCommonDir(clean)
			repoResolved = true
		}
		baseRepo, ok := gitCommonDir(base)
		return ok && currentRepo != "" && currentRepo == baseRepo
	}
	for _, ex := range c.ExcludePaths {
		if matches(ex) {
			return false, fmt.Sprintf("path is excluded by %q", ex)
		}
	}
	if len(c.CaptureOnlyPaths) > 0 {
		for _, inc := range c.CaptureOnlyPaths {
			if matches(inc) {
				return true, ""
			}
		}
		return false, "path is outside the configured capture allowlist"
	}
	return true, ""
}

// underPath reports whether p is at or below base. Comparison is on cleaned
// path segments so that "/repo-secret" is not treated as being inside "/repo",
// which a naive prefix check would get wrong.
func underPath(p, base string) bool {
	base = filepath.Clean(base)
	if base == "" || base == "." {
		return false
	}
	if p == base {
		return true
	}
	return strings.HasPrefix(p, base+string(filepath.Separator))
}

func gitCommonDir(path string) (string, bool) {
	if !filepath.IsAbs(path) {
		return "", false
	}
	for root := filepath.Clean(path); ; root = filepath.Dir(root) {
		dotGit := filepath.Join(root, ".git")
		info, err := os.Stat(dotGit)
		if err == nil {
			gitDir := dotGit
			if !info.IsDir() {
				contents, err := os.ReadFile(dotGit)
				if err != nil {
					return "", false
				}
				value, ok := strings.CutPrefix(strings.TrimSpace(string(contents)), "gitdir:")
				if !ok || strings.TrimSpace(value) == "" {
					return "", false
				}
				gitDir = strings.TrimSpace(value)
				if !filepath.IsAbs(gitDir) {
					gitDir = filepath.Join(root, gitDir)
				}
			}
			return resolveGitCommonDir(gitDir)
		}
		if !os.IsNotExist(err) || filepath.Dir(root) == root {
			return "", false
		}
	}
}

func resolveGitCommonDir(gitDir string) (string, bool) {
	commonDir := gitDir
	contents, err := os.ReadFile(filepath.Join(gitDir, "commondir"))
	if err == nil {
		commonDir = strings.TrimSpace(string(contents))
		if commonDir == "" {
			return "", false
		}
		if !filepath.IsAbs(commonDir) {
			commonDir = filepath.Join(gitDir, commonDir)
		}
	} else if !os.IsNotExist(err) {
		return "", false
	}
	resolved, err := filepath.EvalSymlinks(commonDir)
	if err != nil {
		return "", false
	}
	info, err := os.Stat(resolved)
	return filepath.Clean(resolved), err == nil && info.IsDir()
}

// IsSkipped reports whether a tool was excluded by the user.
func (c Config) IsSkipped(tool string) bool {
	for _, s := range c.SkippedTools {
		if s == tool {
			return true
		}
	}
	return false
}

// IsPaused reports whether capture and delivery are suspended at now. It is
// the one reading every path consults (the hook, the daemon, backfill, repair,
// status, doctor and the health report), so a timed pause ends everywhere at
// once: a config that says Paused whose PausedUntil has passed is not paused.
func (c Config) IsPaused(now time.Time) bool {
	if !c.Paused {
		return false
	}
	return c.PausedUntil.IsZero() || now.Before(c.PausedUntil)
}

// Pause records the user's choice with a timestamp, so the fleet view can show
// how long a machine has been paused rather than just that it is. A positive
// d ends the pause at now+d without a further command; zero pauses until
// Resume.
func (c Config) Pause(now time.Time, d time.Duration) Config {
	c.Paused = true
	c.PausedSince = now
	c.PausedUntil = time.Time{}
	if d > 0 {
		c.PausedUntil = now.Add(d)
	}
	return c
}

// Resume clears the pause, its timestamp and its deadline.
func (c Config) Resume() Config {
	c.Paused = false
	c.PausedSince = time.Time{}
	c.PausedUntil = time.Time{}
	return c
}
