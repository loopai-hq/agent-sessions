package discovery

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeHome builds a synthetic home directory. Every test drives discovery
// against one of these rather than the real machine, so the suite is
// deterministic and proves the "not everyone is laid out like you" property.
func fakeHome(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func write(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = 'x'
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeText(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func findingFor(fs []Finding, tool Tool) Finding {
	for _, f := range fs {
		if f.Tool == tool {
			return f
		}
	}
	return Finding{}
}

func noEnv(string) string           { return "" }
func noPath(string) (string, error) { return "", errors.New("not found") }

func TestFindsClaudeCodeAtDefaultLocation(t *testing.T) {
	home := fakeHome(t)
	write(t, filepath.Join(home, ".claude/projects/-home-x-repo/sess1.jsonl"), 100)
	write(t, filepath.Join(home, ".claude/projects/-home-x-repo/sess2.jsonl"), 200)

	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), ClaudeCode)
	if f.State != Found {
		t.Fatalf("state = %q, want found", f.State)
	}
	if f.Sessions != 2 || f.Bytes != 300 {
		t.Fatalf("sessions=%d bytes=%d, want 2/300", f.Sessions, f.Bytes)
	}
}

func TestClaudeCodeCountsPrimarySessionsNotTranscriptArtifacts(t *testing.T) {
	home := fakeHome(t)
	base := filepath.Join(home, ".claude/projects/-home-x-repo")
	write(t, filepath.Join(base, "sess1.jsonl"), 10)
	write(t, filepath.Join(base, "sess1/subagents/agent-a.jsonl"), 10)
	write(t, filepath.Join(base, "sess1/subagents/workflows/wf_1/agent-b.jsonl"), 10)
	write(t, filepath.Join(base, "sess1/subagents/workflows/wf_1/journal.jsonl"), 10)
	write(t, filepath.Join(home, ".claude/projects/-home-x-other/sess2.jsonl"), 20)

	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), ClaudeCode)
	if f.Sessions != 2 || f.Bytes != 30 {
		t.Fatalf("sessions=%d bytes=%d, want 2/30", f.Sessions, f.Bytes)
	}
}

func TestClaudeDesktopCountsSessionMetadataNotJSONArtifacts(t *testing.T) {
	home := fakeHome(t)
	base := filepath.Join(home, "Library/Application Support/Claude/local-agent-mode-sessions/account/task")
	write(t, filepath.Join(base, "local_2f538a45-e65b-40f2-aeb0-32fddf763564.json"), 10)
	write(t, filepath.Join(base, "cowork_settings.json"), 20)
	write(t, filepath.Join(base, "audit.jsonl"), 30)
	write(t, filepath.Join(base, "workspace/.claude/projects/p/sess.jsonl"), 40)

	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), ClaudeDesktop)
	if f.Sessions != 1 || f.Bytes != 10 {
		t.Fatalf("sessions=%d bytes=%d, want 1/10", f.Sessions, f.Bytes)
	}
}

func TestCodexCountsPrimaryRolloutsNotSubagents(t *testing.T) {
	home := fakeHome(t)
	base := filepath.Join(home, ".codex/sessions/2026/08/06")
	writeText(t, filepath.Join(base, "primary.jsonl"), `{"type":"session_meta","payload":{"id":"root","source":"cli"}}`)
	writeText(t, filepath.Join(base, "subagent.jsonl"), `{"type":"session_meta","payload":{"id":"child","source":{"subagent":{"thread_spawn":{"parent_thread_id":"root"}}}}}`)

	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), Codex)
	if f.Sessions != 1 {
		t.Fatalf("sessions=%d, want 1", f.Sessions)
	}
}

// A machine that sets CLAUDE_CONFIG_DIR does not look like the default at all.
func TestEnvOverrideBeatsDefaultLocation(t *testing.T) {
	home := fakeHome(t)
	custom := t.TempDir()
	write(t, filepath.Join(home, ".claude/projects/p/default.jsonl"), 10)
	write(t, filepath.Join(custom, "projects/p/a.jsonl"), 10)
	write(t, filepath.Join(custom, "projects/p/b.jsonl"), 10)

	env := func(k string) string {
		if k == "CLAUDE_CONFIG_DIR" {
			return custom
		}
		return ""
	}
	f := findingFor(Run(Options{Home: home, Getenv: env, LookPath: noPath}), ClaudeCode)
	if f.Root != filepath.Join(custom, "projects") {
		t.Fatalf("root = %q, want the env-derived path", f.Root)
	}
	if f.Sessions != 2 {
		t.Fatalf("sessions = %d, want 2", f.Sessions)
	}
}

// The case that matters most: tool is installed, sessions are not where we
// looked. We must say "ask the user", never "absent".
func TestInstalledButNoSessionsAsksRatherThanConcludes(t *testing.T) {
	home := fakeHome(t)
	write(t, filepath.Join(home, ".codex/config.toml"), 10) // install evidence only

	fs := Run(Options{Home: home, Getenv: noEnv, LookPath: noPath})
	f := findingFor(fs, Codex)
	if f.State != Installed {
		t.Fatalf("state = %q, want installed_no_sessions", f.State)
	}
	if len(f.InstallEvidence) == 0 {
		t.Fatal("expected install evidence to be recorded")
	}
	prompts := NeedsPrompt(fs)
	var sawCodex bool
	for _, p := range prompts {
		if p.Tool == Codex {
			sawCodex = true
		}
	}
	if !sawCodex {
		t.Fatal("codex should be in the ask-the-user list")
	}
}

func TestBinaryOnPathCountsAsInstallEvidence(t *testing.T) {
	home := fakeHome(t)
	lp := func(b string) (string, error) {
		if b == "claude" {
			return "/usr/local/bin/claude", nil
		}
		return "", errors.New("nope")
	}
	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: lp}), ClaudeCode)
	if f.State != Installed {
		t.Fatalf("state = %q, want installed_no_sessions", f.State)
	}
	if len(f.InstallEvidence) != 1 || f.InstallEvidence[0] != "binary:/usr/local/bin/claude" {
		t.Fatalf("evidence = %v", f.InstallEvidence)
	}
}

func TestTrulyAbsentToolStaysSilent(t *testing.T) {
	home := fakeHome(t)
	fs := Run(Options{Home: home, Getenv: noEnv, LookPath: noPath})
	f := findingFor(fs, Aider)
	if f.State != Absent {
		t.Fatalf("state = %q, want absent", f.State)
	}
	for _, p := range NeedsPrompt(fs) {
		if p.Tool == Aider {
			t.Fatal("absent tool must not generate a prompt")
		}
	}
}

func TestUserSuppliedPathWinsAndIsValidated(t *testing.T) {
	home := fakeHome(t)
	custom := t.TempDir()
	write(t, filepath.Join(custom, "a.jsonl"), 42)

	f := findingFor(Run(Options{
		Home: home, Getenv: noEnv, LookPath: noPath,
		UserPaths: map[Tool]string{ClaudeCode: custom},
	}), ClaudeCode)

	if f.State != Found || f.Root != custom {
		t.Fatalf("state=%q root=%q, want found at %q", f.State, f.Root, custom)
	}
	if !f.UserPath {
		t.Fatal("UserPath flag should be set")
	}
}

// A user-supplied path that contains nothing is a typo. Catch it at install
// time instead of producing a silent coverage hole.
func TestUserSuppliedEmptyPathIsFlaggedNotAccepted(t *testing.T) {
	home := fakeHome(t)
	empty := t.TempDir()
	f := findingFor(Run(Options{
		Home: home, Getenv: noEnv, LookPath: noPath,
		UserPaths: map[Tool]string{ClaudeCode: empty},
	}), ClaudeCode)

	if f.State != Installed {
		t.Fatalf("state = %q, want installed_no_sessions for an empty user path", f.State)
	}
}

func TestSkipIsRememberedAndReported(t *testing.T) {
	home := fakeHome(t)
	write(t, filepath.Join(home, ".claude/projects/p/a.jsonl"), 10)

	fs := Run(Options{Home: home, Getenv: noEnv, LookPath: noPath, Skip: map[Tool]bool{ClaudeCode: true}})
	f := findingFor(fs, ClaudeCode)
	if !f.Skipped {
		t.Fatal("skip flag not recorded")
	}
	if f.Sessions != 0 {
		t.Fatal("a skipped tool must not be scanned")
	}
	if got := Summarize(fs, time.Now()).SkipCount; got != 1 {
		t.Fatalf("skip count = %d, want 1", got)
	}
}

func TestEmptyFilesAreNotCountedAsSessions(t *testing.T) {
	home := fakeHome(t)
	write(t, filepath.Join(home, ".claude/projects/p/empty.jsonl"), 0)
	write(t, filepath.Join(home, ".claude/projects/p/real.jsonl"), 5)

	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), ClaudeCode)
	if f.Sessions != 1 {
		t.Fatalf("sessions = %d, want 1 (zero-byte file excluded)", f.Sessions)
	}
}

func TestNonSessionExtensionsIgnored(t *testing.T) {
	home := fakeHome(t)
	write(t, filepath.Join(home, ".claude/projects/p/a.jsonl"), 10)
	write(t, filepath.Join(home, ".claude/projects/p/notes.md"), 10)
	write(t, filepath.Join(home, ".claude/projects/p/tool-results/out.txt"), 10)

	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), ClaudeCode)
	if f.Sessions != 1 {
		t.Fatalf("sessions = %d, want 1", f.Sessions)
	}
}

// An unreadable subtree must not make us declare the machine empty.
func TestUnreadableSubtreeDoesNotAbortScan(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root defeats permission checks")
	}
	home := fakeHome(t)
	base := filepath.Join(home, ".claude/projects/p")
	write(t, filepath.Join(base, "good.jsonl"), 10)
	locked := filepath.Join(base, "locked")
	write(t, filepath.Join(locked, "hidden.jsonl"), 10)
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Skip("cannot chmod on this filesystem")
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })

	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), ClaudeCode)
	if f.State != Found || f.Sessions < 1 {
		t.Fatalf("state=%q sessions=%d, want the readable file still found", f.State, f.Sessions)
	}
}

func TestWalkCapPreventsRunaway(t *testing.T) {
	home := fakeHome(t)
	for i := range 50 {
		write(t, filepath.Join(home, ".claude/projects/p", "s"+string(rune('a'+i%26))+string(rune('a'+i/26))+".jsonl"), 10)
	}
	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath, MaxWalk: 5}), ClaudeCode)
	if f.Sessions >= 50 {
		t.Fatalf("sessions = %d, walk cap did not apply", f.Sessions)
	}
}

func TestSummarizeAggregatesAndSortsStably(t *testing.T) {
	home := fakeHome(t)
	write(t, filepath.Join(home, ".claude/projects/p/a.jsonl"), 100)
	codex := `{"type":"session_meta","payload":{"id":"session","source":"cli"}}`
	writeText(t, filepath.Join(home, ".codex/sessions/2026/08/04/rollout-x.jsonl"), codex)

	s := Summarize(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), time.Now())
	if s.Sessions != 2 || s.Bytes != 100+int64(len(codex)) {
		t.Fatalf("sessions=%d bytes=%d, want 2/%d", s.Sessions, s.Bytes, 100+len(codex))
	}
	for i := 1; i < len(s.Findings); i++ {
		if s.Findings[i-1].Tool > s.Findings[i].Tool {
			t.Fatal("findings not sorted stably")
		}
	}
}

func TestWriteSummaryIsAtomicAndReadable(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "nested", "summary.json")
	s := Summarize(nil, time.Unix(0, 0).UTC())
	if err := WriteSummary(p, s); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 || b[len(b)-1] != '\n' {
		t.Fatal("summary should be newline-terminated json")
	}
	if _, err := os.Stat(p + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file should not survive a successful write")
	}
}

func TestCandidatesRecordedEvenWhenNothingFound(t *testing.T) {
	home := fakeHome(t)
	f := findingFor(Run(Options{Home: home, Getenv: noEnv, LookPath: noPath}), ClaudeCode)
	if len(f.Candidates) == 0 {
		t.Fatal("probed paths must be recorded so a user can see what we checked")
	}
	for _, c := range f.Candidates {
		if c.Exists {
			t.Fatalf("candidate %q should not exist in an empty home", c.Path)
		}
	}
}
