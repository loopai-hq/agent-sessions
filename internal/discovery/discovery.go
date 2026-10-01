// Package discovery locates local AI-agent session stores on a machine.
//
// The premise is that no two laptops are laid out the same way. People move
// their home directory, set CLAUDE_CONFIG_DIR or CODEX_HOME, run tools we have
// not heard of, or have a tool installed with its history somewhere we do not
// expect. So discovery never assumes: it probes every location it knows about
// for every tool it knows about, reports what it actually found with evidence
// (file counts, bytes, newest mtime), and hands the caller a list of Findings
// that distinguishes three states which matter operationally:
//
//	Found      - sessions are here, we can read them
//	Installed  - the tool is on this machine but its sessions are not where we
//	             looked, so the user should be asked where they are
//	Absent     - no sign of the tool at all, say nothing and move on
//
// The Installed-but-empty case is the one that matters. Reporting "no Codex
// sessions" when the person uses Codex daily is how a fleet ends up with silent
// coverage holes, so we surface it as a question rather than a conclusion.
package discovery

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/backfill"
)

// Tool identifies a supported agent harness.
type Tool string

// The harnesses discovery knows where to look for. Claude Code is captured
// live and imported; Codex is imported and repaired from its rollouts; the
// rest are located and reported so a fleet can see what it is not covering.
const (
	// ClaudeCode is Anthropic's Claude Code CLI.
	ClaudeCode Tool = "claude_code"
	// ClaudeDesktop is the Claude desktop application.
	ClaudeDesktop Tool = "claude_desktop"
	// Codex is OpenAI's Codex CLI.
	Codex Tool = "codex"
	// Cursor is the Cursor editor.
	Cursor Tool = "cursor"
	// Windsurf is the Windsurf editor.
	Windsurf Tool = "windsurf"
	// Aider is the aider CLI.
	Aider Tool = "aider"
	// Continue is the Continue extension.
	Continue Tool = "continue"
	// GeminiCLI is Google's Gemini CLI.
	GeminiCLI Tool = "gemini_cli"
)

// State is what we concluded about a tool on this machine.
type State string

const (
	// Found means we located a session store with at least one session file.
	Found State = "found"
	// Installed means the tool is present but we found no sessions where we
	// looked. The caller should ask the user for the path rather than assume
	// the tool is unused.
	Installed State = "installed_no_sessions"
	// Absent means we saw no evidence of the tool.
	Absent State = "absent"
)

// Finding is the result for one tool.
type Finding struct {
	Tool  Tool  `json:"tool"`
	State State `json:"state"`

	// Root is the session directory we settled on, empty unless State==Found.
	Root string `json:"root,omitempty"`
	// Candidates are every path we probed, in probe order, so a user who
	// disagrees with the outcome can see what we looked at.
	Candidates []Candidate `json:"candidates"`
	// Evidence of installation independent of sessions: a binary on PATH, a
	// config file, an app bundle. Populated even when State==Absent so support
	// can tell "not installed" from "we could not tell".
	InstallEvidence []string `json:"install_evidence,omitempty"`

	Sessions int       `json:"sessions"`
	Bytes    int64     `json:"bytes"`
	Newest   time.Time `json:"newest,omitempty"`

	// Skipped is set when the user chose to exclude this tool. A skip is
	// remembered and reported rather than silently dropped, because an
	// unexplained coverage gap is worse than a declared one.
	Skipped bool `json:"skipped,omitempty"`
	// UserPath records that the root came from the user rather than a probe.
	UserPath bool `json:"user_path,omitempty"`
}

// Candidate is one probed location and what we saw there.
type Candidate struct {
	Path     string `json:"path"`
	Exists   bool   `json:"exists"`
	Sessions int    `json:"sessions"`
	Note     string `json:"note,omitempty"`
}

// spec describes how to find one tool. Keeping this declarative means adding a
// tool is a data change, not a code change, which is the extensibility bar the
// requirements set (EXT-1).
type spec struct {
	tool Tool
	// envRoots are environment variables that, when set, override the default
	// locations entirely. Claude Code (CLAUDE_CONFIG_DIR) and Codex
	// (CODEX_HOME) both support this and people do use it.
	envRoots []string
	// sub is appended to an env root to reach the session dir.
	sub string
	// roots are the default locations relative to $HOME, in probe order.
	roots []string
	// glob matches session files under a root, evaluated recursively.
	exts []string
	// match identifies primary session files when a store also contains
	// subagent transcripts or unrelated JSON artifacts.
	match func(root, path string) bool
	// binaries on PATH that prove the tool is installed.
	binaries []string
	// configs relative to $HOME that prove the tool is installed.
	configs []string
}

// specs is the probe table. Paths cover macOS, Linux and Windows layouts; a
// path that does not apply to the current OS simply will not exist, so we probe
// them all rather than branching per platform and risking a missed layout.
var specs = []spec{
	{
		tool:     ClaudeCode,
		envRoots: []string{"CLAUDE_CONFIG_DIR"},
		sub:      "projects",
		roots: []string{
			".claude/projects",
			".config/claude/projects",
		},
		exts:     []string{".jsonl"},
		match:    claudeCodeSession,
		binaries: []string{"claude"},
		configs:  []string{".claude.json", ".claude/settings.json"},
	},
	{
		tool: ClaudeDesktop,
		roots: []string{
			"Library/Application Support/Claude/local-agent-mode-sessions",
			".config/Claude/local-agent-mode-sessions",
			"AppData/Roaming/Claude/local-agent-mode-sessions",
		},
		exts:  []string{".jsonl", ".json"},
		match: claudeDesktopSession,
		configs: []string{
			"Library/Application Support/Claude/claude_desktop_config.json",
			".config/Claude/claude_desktop_config.json",
			"AppData/Roaming/Claude/claude_desktop_config.json",
		},
	},
	{
		tool:     Codex,
		envRoots: []string{"CODEX_HOME"},
		sub:      "sessions",
		roots: []string{
			".codex/sessions",
			".config/codex/sessions",
		},
		exts:     []string{".jsonl"},
		match:    func(_, path string) bool { return backfill.IsPrimaryCodex(path) },
		binaries: []string{"codex"},
		configs:  []string{".codex/config.toml", ".codex/history.jsonl"},
	},
	{
		tool: Cursor,
		roots: []string{
			"Library/Application Support/Cursor/User/workspaceStorage",
			".config/Cursor/User/workspaceStorage",
			"AppData/Roaming/Cursor/User/workspaceStorage",
		},
		exts:     []string{".json", ".vscdb"},
		binaries: []string{"cursor"},
		configs:  []string{".cursor"},
	},
	{
		tool: Windsurf,
		roots: []string{
			".codeium/windsurf",
			"Library/Application Support/Windsurf/User/workspaceStorage",
			"AppData/Roaming/Windsurf/User/workspaceStorage",
		},
		exts:     []string{".json", ".jsonl"},
		binaries: []string{"windsurf"},
		configs:  []string{".codeium"},
	},
	{
		tool:     Aider,
		roots:    []string{".aider"},
		exts:     []string{".md", ".json"},
		binaries: []string{"aider"},
		configs:  []string{".aider.conf.yml", ".aider.input.history"},
	},
	{
		tool:     Continue,
		roots:    []string{".continue/sessions"},
		exts:     []string{".json", ".jsonl"},
		binaries: []string{"cont"},
		configs:  []string{".continue/config.json"},
	},
	{
		tool:     GeminiCLI,
		roots:    []string{".gemini/tmp", ".gemini/sessions"},
		exts:     []string{".json", ".jsonl"},
		binaries: []string{"gemini"},
		configs:  []string{".gemini/settings.json"},
	},
}

// Options tunes a discovery run.
type Options struct {
	// Home overrides the home directory. Tests set this; production leaves it
	// empty and we resolve it.
	Home string
	// Getenv overrides environment lookup. Tests set this.
	Getenv func(string) string
	// LookPath overrides PATH resolution. Tests set this.
	LookPath func(string) (string, error)
	// Skip lists tools the user has excluded.
	Skip map[Tool]bool
	// UserPaths maps a tool to a path the user supplied, which wins over every
	// probe.
	UserPaths map[Tool]string
	// MaxWalk caps how many directory entries we will visit per root. Discovery
	// runs during install on someone's laptop, so it must stay fast and must
	// never hang on a pathological tree.
	MaxWalk int
}

func (o *Options) home() string {
	if o.Home != "" {
		return o.Home
	}
	h, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return h
}

func (o *Options) getenv(k string) string {
	if o.Getenv != nil {
		return o.Getenv(k)
	}
	return os.Getenv(k)
}

func (o *Options) lookPath(b string) (string, error) {
	if o.LookPath != nil {
		return o.LookPath(b)
	}
	return exec.LookPath(b)
}

const defaultMaxWalk = 200_000

// Run probes every known tool and returns one Finding each, in a stable order.
func Run(opts Options) []Finding {
	if opts.MaxWalk <= 0 {
		opts.MaxWalk = defaultMaxWalk
	}
	home := opts.home()
	out := make([]Finding, 0, len(specs))
	for _, sp := range specs {
		out = append(out, probe(sp, home, opts))
	}
	return out
}

func probe(sp spec, home string, opts Options) Finding {
	f := Finding{Tool: sp.tool, State: Absent}

	if opts.Skip[sp.tool] {
		f.Skipped = true
		return f
	}

	f.InstallEvidence = installEvidence(sp, home, opts)

	// A user-supplied path outranks every probe. We still validate it, because
	// a path that contains no sessions is a typo we should catch during install
	// rather than a silent hole discovered weeks later.
	if p := opts.UserPaths[sp.tool]; p != "" {
		n, b, newest := scan(p, sp.exts, sp.match, opts.MaxWalk)
		c := Candidate{Path: p, Exists: exists(p), Sessions: n, Note: "user-supplied"}
		f.Candidates = append(f.Candidates, c)
		f.UserPath = true
		if n > 0 {
			f.State, f.Root, f.Sessions, f.Bytes, f.Newest = Found, p, n, b, newest
		} else {
			f.State = Installed
		}
		return f
	}

	for _, p := range candidatePaths(sp, home, opts) {
		n, b, newest := scan(p, sp.exts, sp.match, opts.MaxWalk)
		f.Candidates = append(f.Candidates, Candidate{Path: p, Exists: exists(p), Sessions: n})
		if n > 0 && n > f.Sessions {
			f.State, f.Root, f.Sessions, f.Bytes, f.Newest = Found, p, n, b, newest
		}
	}

	// Present but empty is a question, not a conclusion.
	if f.State != Found && len(f.InstallEvidence) > 0 {
		f.State = Installed
	}
	return f
}

// candidatePaths returns probe locations in priority order: env overrides
// first, then defaults under home.
func candidatePaths(sp spec, home string, opts Options) []string {
	var out []string
	seen := map[string]bool{}
	add := func(p string) {
		if p == "" || seen[p] {
			return
		}
		seen[p] = true
		out = append(out, p)
	}
	for _, ev := range sp.envRoots {
		if v := opts.getenv(ev); v != "" {
			if sp.sub != "" {
				add(filepath.Join(v, sp.sub))
			}
			add(v)
		}
	}
	for _, r := range sp.roots {
		add(filepath.Join(home, filepath.FromSlash(r)))
	}
	return out
}

func installEvidence(sp spec, home string, opts Options) []string {
	var ev []string
	for _, b := range sp.binaries {
		if p, err := opts.lookPath(b); err == nil && p != "" {
			ev = append(ev, "binary:"+p)
		}
	}
	for _, c := range sp.configs {
		p := filepath.Join(home, filepath.FromSlash(c))
		if exists(p) {
			ev = append(ev, "config:"+p)
		}
	}
	return ev
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// scan walks a root counting session files, their bytes, and the newest mtime.
//
// It walks recursively because stores such as Codex and Claude Desktop place
// primary sessions below dated or generated directories. A tool-specific
// matcher distinguishes those sessions from other artifacts in the same tree.
func scan(root string, exts []string, match func(root, path string) bool, maxWalk int) (count int, bytes int64, newest time.Time) {
	st, err := os.Stat(root)
	if err != nil || !st.IsDir() {
		return 0, 0, time.Time{}
	}
	visited := 0
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// Unreadable subtree: skip it rather than abandoning the whole
			// scan. A single permission-denied directory should not make us
			// report a machine as having no sessions.
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		visited++
		if visited > maxWalk {
			return errors.New("walk cap reached")
		}
		if d.IsDir() {
			return nil
		}
		if !matchesExt(p, exts) {
			return nil
		}
		if match != nil && !match(root, p) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.Size() == 0 {
			return nil
		}
		count++
		bytes += info.Size()
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		return nil
	})
	return count, bytes, newest
}

func claudeCodeSession(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	return len(parts) == 1 || len(parts) == 2
}

func claudeDesktopSession(_ string, path string) bool {
	return claudeDesktopSessionName.MatchString(filepath.Base(path))
}

var claudeDesktopSessionName = regexp.MustCompile(`(?i)^local_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$`)

func matchesExt(p string, exts []string) bool {
	lower := strings.ToLower(p)
	for _, e := range exts {
		if strings.HasSuffix(lower, e) {
			return true
		}
	}
	return false
}

// NeedsPrompt returns the tools the installer should ask the user about: those
// that look installed but whose sessions we could not locate. Everything else
// is either working or genuinely absent, and asking about those is noise that
// makes a non-technical person abandon the install.
func NeedsPrompt(fs []Finding) []Finding {
	var out []Finding
	for _, f := range fs {
		if f.State == Installed && !f.Skipped {
			out = append(out, f)
		}
	}
	return out
}

// Summary is a compact, user-facing rollup written at install time and
// refreshed by `loop-sessions discover`.
type Summary struct {
	Host      string    `json:"host"`
	OS        string    `json:"os"`
	Arch      string    `json:"arch"`
	Scanned   time.Time `json:"scanned"`
	Findings  []Finding `json:"findings"`
	Sessions  int       `json:"total_sessions"`
	Bytes     int64     `json:"total_bytes"`
	NeedsAsk  []Tool    `json:"needs_ask,omitempty"`
	SkipCount int       `json:"skipped,omitempty"`
}

// Summarize rolls findings into a Summary, sorted so output is stable.
func Summarize(findings []Finding, now time.Time) Summary {
	host, _ := os.Hostname()
	s := Summary{Host: host, OS: runtime.GOOS, Arch: runtime.GOARCH, Scanned: now, Findings: findings}
	for _, f := range findings {
		s.Sessions += f.Sessions
		s.Bytes += f.Bytes
		if f.Skipped {
			s.SkipCount++
		}
		if f.State == Installed && !f.Skipped {
			s.NeedsAsk = append(s.NeedsAsk, f.Tool)
		}
	}
	sort.Slice(s.Findings, func(i, j int) bool { return s.Findings[i].Tool < s.Findings[j].Tool })
	return s
}

// WriteSummary persists a Summary atomically. Discovery output is the input to
// the install UI and to fleet coverage reporting, so a half-written file would
// be worse than none.
func WriteSummary(path string, s Summary) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
