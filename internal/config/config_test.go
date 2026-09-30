package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func paths(t *testing.T) Paths {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("LOOP_SESSIONS_HOME", dir)
	return Paths{Home: dir}
}

func valid() Config {
	c := Defaults()
	c.Email = "dev@example.com"
	c.Endpoint = "https://sessions.example.com"
	return c
}

func TestLoadOnFreshMachineSaysNotConfigured(t *testing.T) {
	p := paths(t)
	_, err := Load(p)
	// "Never installed" and "installed but broken" need different responses
	// from the caller, so they must be distinguishable.
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("err = %v, want ErrNotConfigured", err)
	}
}

func TestSaveThenLoadRoundTrips(t *testing.T) {
	p := paths(t)
	in := valid()
	in.Roots = map[string]string{"claude_code": "/custom/projects"}
	in.SkippedTools = []string{"codex"}
	if err := Save(p, in); err != nil {
		t.Fatal(err)
	}
	out, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if out.Email != in.Email || out.Endpoint != in.Endpoint {
		t.Fatalf("identity lost: %+v", out)
	}
	if out.Roots["claude_code"] != "/custom/projects" {
		t.Fatal("a non-default session root must survive; re-deriving it would break that machine")
	}
	if !out.IsSkipped("codex") {
		t.Fatal("skip choice lost")
	}
}

func TestConfigFileIsOwnerOnly(t *testing.T) {
	p := paths(t)
	if err := Save(p, valid()); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perms = %o, want 600; the file names the user and their endpoint", perm)
	}
}

func TestSaveIsAtomic(t *testing.T) {
	p := paths(t)
	if err := Save(p, valid()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p.ConfigFile() + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file survived a successful save")
	}
}

// Silently replacing a corrupt config with defaults could re-enable capture the
// user had paused. Refusing is the only safe behaviour.
func TestCorruptConfigIsAnErrorNotSilentlyReplaced(t *testing.T) {
	p := paths(t)
	if err := os.MkdirAll(p.Root(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.ConfigFile(), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(p)
	if err == nil {
		t.Fatal("corrupt config must not load")
	}
	if errors.Is(err, ErrNotConfigured) {
		t.Fatal("corrupt must be distinguishable from absent")
	}
	// The original bytes must still be there for a human to inspect.
	b, _ := os.ReadFile(p.ConfigFile())
	if string(b) != "{not json" {
		t.Fatal("a corrupt config was overwritten")
	}
}

func TestValidateRejectsUnusableConfigs(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"no email", func(c *Config) { c.Email = "" }, "email is missing"},
		{"bad email", func(c *Config) { c.Email = "notanaddress" }, "does not look like"},
		{"no endpoint", func(c *Config) { c.Endpoint = "" }, "endpoint is missing"},
		{"plaintext endpoint", func(c *Config) { c.Endpoint = "http://sessions.example.com" }, "must be https"},
		{"channel mode without channel", func(c *Config) { c.Slack.Mode = SlackChannel }, "no channel is set"},
		{"unknown slack mode", func(c *Config) { c.Slack.Mode = "carrier-pigeon" }, "not one of"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := valid()
			tc.mut(&c)
			err := c.Validate()
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if !contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// Local testing has to work without TLS, but only against loopback.
func TestLoopbackHTTPEndpointIsAllowed(t *testing.T) {
	c := valid()
	c.Endpoint = "http://127.0.0.1:8080"
	if err := c.Validate(); err != nil {
		t.Fatalf("loopback http should be allowed for local testing: %v", err)
	}
}

func TestSaveRefusesInvalidConfig(t *testing.T) {
	p := paths(t)
	c := valid()
	c.Email = ""
	if err := Save(p, c); err == nil {
		t.Fatal("an unusable config must not be persisted")
	}
	if _, err := os.Stat(p.ConfigFile()); !os.IsNotExist(err) {
		t.Fatal("nothing should have been written")
	}
}

func TestPausedBlocksCaptureWithAReason(t *testing.T) {
	c := valid().Pause(time.Now(), 0)
	ok, why := c.ShouldCapture("/repo")
	if ok {
		t.Fatal("paused config must not capture")
	}
	if why == "" {
		t.Fatal("a refusal needs a reason a user can act on")
	}
	if c.PausedSince.IsZero() {
		t.Fatal("pause time must be recorded so the fleet view can show duration")
	}
	if r := c.Resume(); r.Paused || !r.PausedSince.IsZero() {
		t.Fatal("resume must clear both fields")
	}
}

// A timed pause ends on its own: IsPaused, which every path consults, reads
// the deadline, and ShouldCapture lets a session through once it has passed.
// Nothing rewrites the config for that; resume clears all three fields.
func TestTimedPauseEndsOnItsOwn(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	c := valid().Pause(now, 2*time.Hour)
	if !c.Paused || !c.PausedSince.Equal(now) || !c.PausedUntil.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("pause --for did not record the deadline: %+v", c)
	}
	if !c.IsPaused(now) || !c.IsPaused(now.Add(2*time.Hour-time.Second)) {
		t.Fatal("not paused inside the window")
	}
	if c.IsPaused(now.Add(2 * time.Hour)) {
		t.Fatal("still paused at the deadline")
	}
	if !valid().Pause(now, 0).IsPaused(now.Add(24 * 365 * time.Hour)) {
		t.Fatal("an untimed pause must hold until resume")
	}

	// ShouldCapture reads the same deadline against the real clock.
	if ok, why := valid().Pause(time.Now(), 2*time.Hour).ShouldCapture("/repo"); ok || why == "" {
		t.Fatal("captured during a timed pause")
	}
	expired := valid().Pause(time.Now().Add(-3*time.Hour), 2*time.Hour)
	if ok, _ := expired.ShouldCapture("/repo"); !ok {
		t.Fatal("capture did not resume after the deadline")
	}

	r := c.Resume()
	if r.Paused || !r.PausedSince.IsZero() || !r.PausedUntil.IsZero() {
		t.Fatalf("resume must clear the pause, its timestamp and its deadline: %+v", r)
	}

	// The deadline survives the config file, or the daemon would never see it.
	p := paths(t)
	if err := Save(p, c); err != nil {
		t.Fatal(err)
	}
	back, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if !back.PausedUntil.Equal(c.PausedUntil) {
		t.Fatalf("paused_until did not round-trip: %v != %v", back.PausedUntil, c.PausedUntil)
	}
}

func TestExcludedPathsAreNotCaptured(t *testing.T) {
	c := valid()
	c.ExcludePaths = []string{"/home/x/personal"}
	if ok, why := c.ShouldCapture("/home/x/personal/taxes"); ok || why == "" {
		t.Fatalf("excluded path captured (ok=%v why=%q)", ok, why)
	}
	if ok, _ := c.ShouldCapture("/home/x/work/repo"); !ok {
		t.Fatal("unrelated path was wrongly excluded")
	}
}

// A prefix check on raw strings would treat /repo-secret as inside /repo.
func TestExclusionMatchesPathSegmentsNotStringPrefixes(t *testing.T) {
	c := valid()
	c.ExcludePaths = []string{"/repo"}
	if ok, _ := c.ShouldCapture("/repo-secret/project"); !ok {
		t.Fatal("/repo-secret is not inside /repo and must still be captured")
	}
	if ok, _ := c.ShouldCapture("/repo/project"); ok {
		t.Fatal("/repo/project is inside /repo and must be excluded")
	}
}

func TestAllowlistInvertsThePolicy(t *testing.T) {
	c := valid()
	c.CaptureOnlyPaths = []string{"/home/x/work"}
	if ok, _ := c.ShouldCapture("/home/x/work/repo"); !ok {
		t.Fatal("path inside the allowlist must be captured")
	}
	if ok, why := c.ShouldCapture("/home/x/personal/thing"); ok || why == "" {
		t.Fatalf("path outside the allowlist must not be captured (ok=%v why=%q)", ok, why)
	}
}

func TestExclusionWinsOverAllowlist(t *testing.T) {
	// A user who both allows a tree and excludes a subtree means the exclusion.
	c := valid()
	c.CaptureOnlyPaths = []string{"/work"}
	c.ExcludePaths = []string{"/work/secret"}
	if ok, _ := c.ShouldCapture("/work/secret/thing"); ok {
		t.Fatal("an explicit exclusion must beat the allowlist")
	}
}

func TestAllowlistIncludesGitWorktrees(t *testing.T) {
	repo, worktree := gitWorktree(t, "project")
	c := valid()
	c.CaptureOnlyPaths = []string{repo}

	if ok, why := c.ShouldCapture(filepath.Join(worktree, "services", "api")); !ok {
		t.Fatalf("worktree of allowed repository was excluded: %s", why)
	}
}

func TestAllowlistDoesNotMatchUnrelatedRepoWithSameName(t *testing.T) {
	repo, _ := gitWorktree(t, "project")
	unrelated := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(filepath.Join(unrelated, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	c := valid()
	c.CaptureOnlyPaths = []string{repo}

	if ok, _ := c.ShouldCapture(unrelated); ok {
		t.Fatal("an unrelated repository with the same name must not be captured")
	}
}

func TestExclusionIncludesGitWorktrees(t *testing.T) {
	repo, worktree := gitWorktree(t, "personal")
	c := valid()
	c.ExcludePaths = []string{repo}

	if ok, _ := c.ShouldCapture(worktree); ok {
		t.Fatal("worktree of excluded repository was captured")
	}
}

func gitWorktree(t *testing.T, name string) (string, string) {
	t.Helper()
	root := t.TempDir()
	repo := filepath.Join(root, "repos", name)
	gitDir := filepath.Join(repo, ".git")
	worktree := filepath.Join(root, "worktrees", "abc", name)
	worktreeGitDir := filepath.Join(gitDir, "worktrees", "abc")
	for _, dir := range []string{gitDir, worktree, worktreeGitDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(worktree, ".git"), []byte("gitdir: "+worktreeGitDir+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktreeGitDir, "commondir"), []byte("../..\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return repo, worktree
}

func TestSlackDefaultsToOff(t *testing.T) {
	// A tool that starts posting someone's work without being asked gets
	// uninstalled, so off is the only defensible default.
	if Defaults().Slack.Mode != SlackOff {
		t.Fatalf("default slack mode = %q, want off", Defaults().Slack.Mode)
	}
}

func TestOlderConfigGainsNewDefaultsWithoutMigration(t *testing.T) {
	p := paths(t)
	if err := os.MkdirAll(p.Root(), 0o700); err != nil {
		t.Fatal(err)
	}
	// A file written by an earlier version: only the fields that existed then.
	old := `{"email":"dev@example.com","endpoint":"https://x.example.com"}`
	if err := os.WriteFile(p.ConfigFile(), []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.SpoolMaxBytes <= 0 || c.Slack.Mode == "" || c.Roots == nil || c.Channel != DefaultChannel {
		t.Fatalf("defaults not backfilled into an older config: %+v", c)
	}
	if c.SchemaVersion != SchemaVersion {
		t.Fatalf("schema version = %d, want %d", c.SchemaVersion, SchemaVersion)
	}
}

func TestPathsAreUnderTheClientRoot(t *testing.T) {
	p := paths(t)
	root := p.Root()
	for name, got := range map[string]string{
		"config":    p.ConfigFile(),
		"spool":     p.SpoolDir(),
		"state":     p.StateDir(),
		"seq":       p.SeqDir(),
		"log":       p.LogFile(),
		"discovery": p.DiscoveryFile(),
	} {
		if !contains(got, root) {
			t.Fatalf("%s path %q escapes the client root %q", name, got, root)
		}
	}
}

func TestEnvOverrideRelocatesEverything(t *testing.T) {
	custom := t.TempDir()
	t.Setenv("LOOP_SESSIONS_HOME", custom)
	p := Paths{Home: "/should/be/ignored"}
	if p.Root() != custom {
		t.Fatalf("root = %q, want the env override %q", p.Root(), custom)
	}
	if filepath.Dir(p.ConfigFile()) != custom {
		t.Fatal("config did not follow the override")
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
