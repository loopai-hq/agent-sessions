package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/discovery"
)

// writeCodexRollout lays down a rollout whose session_meta names id, in the
// dated directory Codex uses.
func writeCodexRollout(t *testing.T, root, day, name, id string) string {
	t.Helper()
	path := filepath.Join(root, day, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	meta := `{"timestamp":"2026-08-06T10:00:00.000Z","type":"session_meta","payload":{"id":"` + id +
		`","timestamp":"2026-08-06T10:00:00.000Z","cwd":"/repo","cli_version":"0.147.0","parent_thread_id":""}}`
	if err := os.WriteFile(path, []byte(meta+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestACodexRepairEntryIsResolvedBySessionID: the server names a Codex
// session and no file, because a rollout's name says nothing about which
// session it holds and the server has never seen the rollout. Before this,
// the walk skipped every such entry and a Codex session's missing answers
// were never repaired.
func TestACodexRepairEntryIsResolvedBySessionID(t *testing.T) {
	home := t.TempDir()
	t.Setenv("LOOP_SESSIONS_HOME", filepath.Join(home, "state"))
	codexRoot := filepath.Join(home, ".codex", "sessions")
	const want = "019fd644-3a15-7c60-a3d4-01d8df6849a0"
	const other = "11111111-1111-4111-8111-111111111111"
	writeCodexRollout(t, codexRoot, "2026/08/06", "rollout-a.jsonl", other)
	mine := writeCodexRollout(t, codexRoot, "2026/08/07", "rollout-b.jsonl", want)

	p := config.Paths{Home: home}
	if err := os.MkdirAll(p.Root(), 0o755); err != nil {
		t.Fatal(err)
	}
	sum := discovery.Summary{Findings: []discovery.Finding{
		{Tool: discovery.ClaudeCode, Root: filepath.Join(home, ".claude", "projects")},
		{Tool: discovery.Codex, Root: codexRoot},
	}}
	raw, err := json.Marshal(sum)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.DiscoveryFile(), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Config{}
	roots := codexRepairRoots(p, cfg)
	if len(roots) != 1 || roots[0] != codexRoot {
		t.Fatalf("codexRepairRoots = %v, want just the Codex root %s (the Claude root is another tool's)", roots, codexRoot)
	}

	got, err := findCodexRollout(roots, want)
	if err != nil {
		t.Fatalf("findCodexRollout: %v", err)
	}
	if got != mine {
		t.Errorf("findCodexRollout = %q, want %q", got, mine)
	}
	// A session this machine does not hold is not an error: the rollout may
	// be compressed, deleted, or on another of this person's laptops.
	got, err = findCodexRollout(roots, "99999999-9999-4999-8999-999999999999")
	if err != nil || got != "" {
		t.Errorf("findCodexRollout(absent) = %q (%v), want an empty path and no error", got, err)
	}
	// A root that is not there is stepped over rather than failing the walk.
	if got, err := findCodexRollout([]string{filepath.Join(home, "gone"), codexRoot}, want); err != nil || got != mine {
		t.Errorf("findCodexRollout over a missing root = %q (%v), want %q", got, err, mine)
	}
}

// TestCodexRootsComeFromDiscoveryAndTheConfig: a machine whose discovery
// summary is gone still has somewhere to look, and a Claude root is never
// searched for a Codex rollout.
func TestCodexRootsComeFromDiscoveryAndTheConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("LOOP_SESSIONS_HOME", filepath.Join(home, "state"))
	p := config.Paths{Home: home}
	claude := filepath.Join(home, ".claude", "projects")
	codex := filepath.Join(home, ".codex", "sessions")

	// No summary on disk: the config's roots are all there is, and the
	// Claude one is never searched for a Codex rollout.
	roots := codexRepairRoots(p, config.Config{Roots: map[string]string{
		string(discovery.ClaudeCode): claude,
		string(discovery.Codex):      codex,
	}})
	if len(roots) != 1 || roots[0] != codex {
		t.Fatalf("with no summary, codexRepairRoots = %v, want just %s", roots, codex)
	}

	// A summary that names the same root discovery settled on and probed
	// yields it once, so it is walked once.
	if err := os.MkdirAll(p.Root(), 0o755); err != nil {
		t.Fatal(err)
	}
	sum := discovery.Summary{Findings: []discovery.Finding{{
		Tool: discovery.Codex, Root: codex,
		Candidates: []discovery.Candidate{{Path: codex}, {Path: filepath.Join(home, ".config", "codex", "sessions")}},
	}}}
	raw, err := json.Marshal(sum)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.DiscoveryFile(), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	roots = codexRepairRoots(p, config.Config{})
	if len(roots) != 2 || roots[0] != codex {
		t.Fatalf("codexRepairRoots = %v, want the settled root once and the other candidate", roots)
	}
	for i := range roots {
		for j := range roots {
			if i != j && roots[i] == roots[j] {
				t.Errorf("a root appears twice: %v", roots)
			}
		}
	}
}

// TestACodexEntryIsRecognisedByItsSourceOrItsPath: the source is the
// server's word; the path is the fallback for a server that predates the
// field. A Claude entry is neither.
func TestACodexEntryIsRecognisedByItsSourceOrItsPath(t *testing.T) {
	for _, c := range []struct {
		name string
		in   repairSession
		want bool
	}{
		{"the server said so", repairSession{SessionID: "s", Source: "codex"}, true},
		{"an old server, a codex path", repairSession{SessionID: "s", TranscriptPath: "/home/x/.codex/sessions/2026/rollout.jsonl"}, true},
		{"a claude entry", repairSession{SessionID: "s", TranscriptPath: "/home/x/.claude/projects/p/s.jsonl", Source: "claude_code"}, false},
		{"neither", repairSession{SessionID: "s"}, false},
	} {
		if got := isCodexEntry(c.in); got != c.want {
			t.Errorf("%s: isCodexEntry = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestTheServersCodexHintDoesNotNameAFilenamePattern: the hint used to tell
// a reader to look for rollout-*-<id>.jsonl, a naming rule nothing in this
// repository relies on; the client resolves the file by the session_meta
// line instead. A hint that teaches the wrong rule is worse than none.
func TestTheServersCodexHintDoesNotNameAFilenamePattern(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "server", "app", "api_adapter.go"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "rollout-*-") {
		t.Error("the Codex repair hint still names a rollout filename pattern")
	}
}
