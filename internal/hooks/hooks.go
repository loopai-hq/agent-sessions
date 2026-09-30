// Package hooks registers and removes this agent's hooks in a harness's own
// settings file.
//
// This is the most invasive thing the client does: it edits a file the user
// owns, that their editor depends on, and that they may have hand-tuned. Three
// rules follow from that, and they shape everything below.
//
// Never destroy what we did not write. The settings file frequently contains
// hooks the person built themselves — on the reference machine it holds four
// unrelated hook events implementing personal enforcement gates. Registration
// merges into that, and removal takes out only entries carrying our marker.
//
// Never leave the file unparseable. A settings file that fails to load can stop
// the harness from starting, which would turn a telemetry install into an outage
// on someone's laptop. Writes are atomic, and a file that was already invalid is
// left strictly alone rather than "fixed".
//
// Always be removable. Every entry we add is tagged, so uninstall is exact and a
// user can see precisely what we put there by reading their own config.
package hooks

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DefaultSessionEndTimeout is the per-hook timeout registered on SessionEnd,
// in seconds. SessionEnd hooks share a 1.5-second budget by default, and the
// harness raises that budget to the largest per-hook timeout in the settings,
// up to 60. Thirty leaves room for the transcript-exists check, the title
// scan and a spool write on a laptop that is busy shutting down, and stays
// under the ceiling so the harness honours it rather than clamping it.
const DefaultSessionEndTimeout = 30

// Marker identifies entries this agent owns. It lives inside the command string
// because the harness's hook schema has no field for provenance, and a comment
// would not survive a JSON round-trip.
const Marker = "#loop-sessions"

// Events are the hooks we register, and the reason for each.
//
// The set is deliberately smaller than everything available. Each entry costs a
// process spawn on the user's machine during their turn, so an event earns its
// place by carrying information nothing else does.
var Events = []Event{
	{Name: "SessionStart", Why: "session envelope, and the moment we reconcile anything a previous crash left open"},
	{Name: "UserPromptSubmit", Why: "the prompt, verbatim, before any client-side rewriting"},
	{Name: "PreToolUse", Why: "what the agent intended to run"},
	{Name: "PostToolUse", Why: "what actually happened, including real file diffs and full tool output"},
	{Name: "PostToolUseFailure", Why: "failures, which are the most analytically interesting turns"},
	{Name: "Stop", Why: "turn boundaries"},
	{Name: "SubagentStop", Why: "attaches subagent work to its parent instead of orphaning it"},
	{Name: "PreCompact", Why: "marks where a continuation seam will fall"},
	{Name: "SessionEnd", Why: "a hint that the session ended; never relied on, since it does not fire on a kill"},
}

// Event is one hook registration.
type Event struct {
	Name string
	Why  string
}

// settings is a partial view of the harness settings file.
//
// Everything we do not model is preserved through Extra, because a user's
// settings contain far more than hooks and dropping any of it on a merge would
// be a serious breach of trust in a file we were only meant to add one thing to.
type settings struct {
	Hooks map[string][]matcherGroup  `json:"hooks,omitempty"`
	Extra map[string]json.RawMessage `json:"-"`
}

type matcherGroup struct {
	Matcher string                     `json:"matcher,omitempty"`
	Hooks   []hookEntry                `json:"hooks"`
	Extra   map[string]json.RawMessage `json:"-"`
}

type hookEntry struct {
	Type    string `json:"type"`
	Command string `json:"command"`
	// Timeout matters on SessionEnd, where all hooks share a budget of about
	// 1.5 seconds by default and an untimed handler is killed mid-work. It is
	// not set on the other events: they are async, and the harness enforces
	// no timeout on an async hook at all.
	Timeout int  `json:"timeout,omitempty"`
	Async   bool `json:"async,omitempty"`
}

// Plan describes what registration would change, so an installer can show a
// user the effect before touching their file.
type Plan struct {
	Path string
	Add  []string
	// AlreadySet names the events that carry our entry, whether or not it is
	// current; Update is the subset whose entry an earlier release wrote with
	// a different timeout or async flag and that registration rewrites in
	// place. Kept apart because "registered" is what the health report asks,
	// and a machine that merely needs a timeout raised is not unregistered.
	AlreadySet []string
	Update     []string
	Foreign    int // hooks belonging to someone else that we will leave alone
	Created    bool
}

// Options configure registration.
type Options struct {
	// SettingsPath is the harness settings file. Defaults to
	// ~/.claude/settings.json.
	SettingsPath string
	// Binary is the absolute path to this agent. Absolute on purpose: a hook
	// resolved through PATH breaks the moment the harness runs with a different
	// environment, which is a failure that looks like "capture silently stopped".
	Binary string
	// Timeout for hook entries, in seconds.
	Timeout int
}

// DefaultSettingsPath returns the harness settings file for this user,
// respecting the same override the harness itself honours.
func DefaultSettingsPath() string {
	if v := os.Getenv("CLAUDE_CONFIG_DIR"); v != "" {
		return filepath.Join(v, "settings.json")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".claude", "settings.json")
}

// Command builds the hook command line for an event.
func Command(binary string) string {
	return fmt.Sprintf("%s hook %s", binary, Marker)
}

// Preview reports what Register would do without changing anything.
func Preview(o Options) (Plan, error) {
	o = o.withDefaults()
	p := Plan{Path: o.SettingsPath}
	s, existed, err := load(o.SettingsPath)
	if err != nil {
		return p, err
	}
	p.Created = !existed
	for _, ev := range Events {
		want := wanted(o, ev)
		have, ok := ours(s.Hooks[ev.Name], want.Command)
		switch {
		case !ok:
			p.Add = append(p.Add, ev.Name)
		case stale(*have, want):
			p.AlreadySet = append(p.AlreadySet, ev.Name)
			p.Update = append(p.Update, ev.Name)
		default:
			p.AlreadySet = append(p.AlreadySet, ev.Name)
		}
	}
	for _, groups := range s.Hooks {
		for _, g := range groups {
			for _, h := range g.Hooks {
				if !isOurs(h.Command) {
					p.Foreign++
				}
			}
		}
	}
	sort.Strings(p.Add)
	sort.Strings(p.AlreadySet)
	sort.Strings(p.Update)
	return p, nil
}

// Register merges our hooks into the settings file, preserving everything else.
// An entry that is already there is brought up to the current defaults rather
// than left as written: the timeout on SessionEnd is what an old entry most
// often gets wrong, and a machine that self-upgrades its binary never runs
// the installer again, so if registration did not rewrite it the whole fleet
// would keep whatever budget the first release wrote.
func Register(o Options) (Plan, error) {
	o = o.withDefaults()
	if o.Binary == "" {
		return Plan{}, errors.New("hooks: Binary is required and must be an absolute path")
	}
	if !filepath.IsAbs(o.Binary) {
		return Plan{}, fmt.Errorf("hooks: Binary %q must be absolute; a PATH-resolved hook "+
			"breaks whenever the harness runs with a different environment", o.Binary)
	}

	plan, err := Preview(o)
	if err != nil {
		return plan, err
	}
	if len(plan.Add) == 0 && len(plan.Update) == 0 {
		return plan, nil // registered and current; nothing to write
	}

	s, _, err := load(o.SettingsPath)
	if err != nil {
		return plan, err
	}
	if s.Hooks == nil {
		s.Hooks = map[string][]matcherGroup{}
	}
	for _, ev := range Events {
		want := wanted(o, ev)
		if have, ok := ours(s.Hooks[ev.Name], want.Command); ok {
			have.Timeout, have.Async = want.Timeout, want.Async
			continue
		}
		s.Hooks[ev.Name] = appendToGroup(s.Hooks[ev.Name], want)
	}
	if err := save(o.SettingsPath, s); err != nil {
		return plan, err
	}
	return plan, nil
}

// Refresh brings the entries Register already wrote up to the current
// defaults and adds nothing. It reports the events it rewrote.
//
// The daemon calls it once per binary version, which is how a fleet that
// self-upgrades converges on a new SessionEnd budget without anyone running
// `install --hooks-only`. Adding is left to the installer on purpose: a daemon
// started from a binary the hooks do not name (a development build run by
// hand) must not register a second set of hooks pointing at itself, and only
// an exact command match is treated as ours here.
func Refresh(o Options) ([]string, error) {
	o = o.withDefaults()
	if o.Binary == "" || !filepath.IsAbs(o.Binary) {
		return nil, fmt.Errorf("hooks: Binary %q must be absolute", o.Binary)
	}
	plan, err := Preview(o)
	if err != nil {
		return nil, err
	}
	if len(plan.Update) == 0 {
		return nil, nil
	}
	s, _, err := load(o.SettingsPath)
	if err != nil {
		return nil, err
	}
	for _, ev := range Events {
		want := wanted(o, ev)
		if have, ok := ours(s.Hooks[ev.Name], want.Command); ok {
			have.Timeout, have.Async = want.Timeout, want.Async
		}
	}
	if err := save(o.SettingsPath, s); err != nil {
		return nil, err
	}
	return plan.Update, nil
}

// Unregister removes only the entries we own.
func Unregister(o Options) (removed int, err error) {
	o = o.withDefaults()
	s, existed, err := load(o.SettingsPath)
	if err != nil || !existed {
		return 0, err
	}
	for name, groups := range s.Hooks {
		var keptGroups []matcherGroup
		for _, g := range groups {
			var kept []hookEntry
			for _, h := range g.Hooks {
				if isOurs(h.Command) {
					removed++
					continue
				}
				kept = append(kept, h)
			}
			// A group emptied by our removal is ours to drop; a group that
			// still holds someone else's hooks stays exactly as it was.
			if len(kept) > 0 {
				g.Hooks = kept
				keptGroups = append(keptGroups, g)
			}
		}
		if len(keptGroups) > 0 {
			s.Hooks[name] = keptGroups
		} else {
			delete(s.Hooks, name)
		}
	}
	if removed == 0 {
		return 0, nil
	}
	return removed, save(o.SettingsPath, s)
}

func (o Options) withDefaults() Options {
	if o.SettingsPath == "" {
		o.SettingsPath = DefaultSettingsPath()
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultSessionEndTimeout
	}
	return o
}

func isOurs(cmd string) bool { return strings.Contains(cmd, Marker) }

// wanted is the entry an event should carry under these options. Every hook
// is async; only SessionEnd carries a timeout, because SessionEnd hooks share
// a ~1.5s budget by default and an untimed handler is killed halfway through
// its work, while the harness enforces no timeout on an async hook at all.
func wanted(o Options, ev Event) hookEntry {
	e := hookEntry{Type: "command", Command: Command(o.Binary), Async: true}
	if ev.Name == "SessionEnd" {
		e.Timeout = o.Timeout
	}
	return e
}

// ours finds our entry for an event by its exact command, addressable so a
// caller can rewrite it in place.
func ours(groups []matcherGroup, want string) (*hookEntry, bool) {
	for gi := range groups {
		for hi := range groups[gi].Hooks {
			if groups[gi].Hooks[hi].Command == want {
				return &groups[gi].Hooks[hi], true
			}
		}
	}
	return nil, false
}

// stale reports whether an entry of ours was written under different
// defaults than the ones in force now.
func stale(have, want hookEntry) bool {
	return have.Timeout != want.Timeout || have.Async != want.Async
}

// appendToGroup adds an entry, reusing an existing unmatched group so we do not
// accumulate a new group per install.
func appendToGroup(groups []matcherGroup, e hookEntry) []matcherGroup {
	for i, g := range groups {
		if g.Matcher == "" {
			groups[i].Hooks = append(groups[i].Hooks, e)
			return groups
		}
	}
	return append(groups, matcherGroup{Hooks: []hookEntry{e}})
}

// load reads the settings file, preserving unmodelled keys.
//
// A file that does not parse is an error, never something to overwrite: it is
// the user's configuration, it may be mid-edit, and replacing it would destroy
// work that has nothing to do with us.
func load(path string) (settings, bool, error) {
	var s settings
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return settings{Extra: map[string]json.RawMessage{}}, false, nil
		}
		return s, false, fmt.Errorf("hooks: read %s: %w", path, err)
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(b, &all); err != nil {
		return s, true, fmt.Errorf("hooks: %s is not valid JSON (%w); refusing to modify it", path, err)
	}
	s.Extra = map[string]json.RawMessage{}
	for k, v := range all {
		if k == "hooks" {
			if err := json.Unmarshal(v, &s.Hooks); err != nil {
				return s, true, fmt.Errorf("hooks: %s has a hooks section we cannot parse (%w); "+
					"refusing to modify it", path, err)
			}
			continue
		}
		s.Extra[k] = v
	}
	return s, true, nil
}

// save writes the settings back atomically, with our hooks merged in and every
// other key restored byte-for-byte.
func save(path string, s settings) error {
	out := map[string]json.RawMessage{}
	for k, v := range s.Extra {
		out[k] = v
	}
	if len(s.Hooks) > 0 {
		b, err := json.Marshal(s.Hooks)
		if err != nil {
			return err
		}
		out["hooks"] = b
	} else {
		delete(out, "hooks")
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { //nolint:gosec // G301: this is Claude Code's own directory, created the way the harness creates it
		return err
	}
	// Atomic: a half-written settings file can stop the harness from starting,
	// which would make installing telemetry an outage on someone's laptop.
	tmp := path + ".loop-tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil { //nolint:gosec // G306: settings.json is Claude Code's file; it keeps the mode the harness and an editor give it
		return err
	}
	return os.Rename(tmp, path)
}
