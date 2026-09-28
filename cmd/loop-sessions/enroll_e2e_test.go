package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/loopai-hq/agent-sessions/internal/config"
)

// TestReinstallingKeepsTheSameDeviceRow is the test that would have caught the
// phantom device row.
//
// Every half of enrolment had tests and passed them. What nothing asserted was
// the whole chain in one process tree: the real binary, run twice, against a
// real server, with the config that survives between the runs. The client posted
// four facts about the machine and no id, the server built a device with no id,
// the store minted a fresh one, and each of those three was correct on its own
// terms — so a laptop that enrolled twice left a row behind that could never
// report again, and admin/fleet.go counted it against the person forever.
//
// The properties, in the order they cost when they break: a machine that has
// enrolled before names its own row; the server is told which row that is; and
// an install that finds a working credential does not sign in at all, because
// re-enrolling on every install would mint a credential per run.
func TestReinstallingKeepsTheSameDeviceRow(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the agent and runs it against a live server")
	}
	if runtime.GOOS == "windows" {
		t.Skip("the browser is opened through rundll32 on Windows, which this fixture does not stand in for")
	}

	srv := newEnrollServer(t)
	defer srv.Close()

	bin := buildAgent(t)
	browser := buildFakeBrowser(t)
	home := t.TempDir()

	install := func() (string, error) {
		cmd := exec.Command(bin, "install",
			"--endpoint", srv.URL, "--skip-hooks", "--skip-backfill")
		cmd.Env = installEnv(home, browser)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	out, err := install()
	if err != nil {
		t.Fatalf("first install: %v\n%s", err, out)
	}
	if n := srv.count(); n != 1 {
		t.Fatalf("first install made %d enrolment requests, want 1:\n%s", n, out)
	}
	if got := srv.request(0)["device_id"]; got != "" {
		t.Errorf("a machine that had never enrolled claimed device %q", got)
	}
	minted := srv.reply(0)
	if got := readConfig(t, home).DeviceID; got != minted {
		t.Fatalf("config kept device %q, want the one the server minted (%q)", got, minted)
	}

	// An install on a machine that is already signed in and holds its credential
	// must not enrol again. Doing so would leave a token row per run even now
	// that the device row is reused.
	if out, err := install(); err != nil {
		t.Fatalf("second install: %v\n%s", err, out)
	} else if n := srv.count(); n != 1 {
		t.Fatalf("an install on a working machine enrolled again (%d requests):\n%s", n, out)
	}

	// The credential is a separate file from the config, and losing it is the
	// ordinary way a machine that is still on file has to sign in again: a
	// restore, a purge of one file, a rotation. Before this change that read as
	// "already signed in" and the machine stayed mute; now it re-credentials the
	// row it already has.
	if err := os.Remove(filepath.Join(config.Paths{Home: home}.Root(), "device.token")); err != nil {
		t.Fatalf("remove the device credential: %v", err)
	}
	out, err = install()
	if err != nil {
		t.Fatalf("install after losing the credential: %v\n%s", err, out)
	}
	if n := srv.count(); n != 2 {
		t.Fatalf("a machine with no credential made %d enrolment requests, want 2:\n%s", n, out)
	}
	if got := srv.request(1)["device_id"]; got != minted {
		t.Fatalf("the re-enrolment claimed device %q, want the row this machine already had (%q)", got, minted)
	}
	if got := srv.request(1)["hostname"]; got == "" {
		t.Error("the re-enrolment carried no hostname, which is the server's only fallback")
	}
	if got := readConfig(t, home).DeviceID; got != minted {
		t.Errorf("config now names device %q, want %q", got, minted)
	}
	token, err := os.ReadFile(filepath.Join(config.Paths{Home: home}.Root(), "device.token"))
	if err != nil || len(token) == 0 {
		t.Errorf("the machine did not end up with a credential: %v", err)
	}
}

// ---------------------------------------------------------------- fixtures

// enrollServer is the enrolment contract reduced to what the client needs: it
// honours the id the laptop claims, which is what a store that recognises the
// machine does, and records every request so the test can read what crossed the
// wire rather than inferring it.
type enrollServer struct {
	*httptest.Server

	mu       sync.Mutex
	requests []map[string]string
	replies  []string
}

func newEnrollServer(t *testing.T) *enrollServer {
	t.Helper()
	s := &enrollServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/enroll/complete" {
			http.NotFound(w, r)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "malformed", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		device := body["device_id"]
		if device == "" {
			device = fmt.Sprintf("00000000-0000-4000-8000-%012d", len(s.requests)+1)
		}
		s.requests = append(s.requests, body)
		s.replies = append(s.replies, device)
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"email":        "dev@example.com",
			"device_id":    device,
			"device_token": "loops_v1_" + device,
		})
	}))
	return s
}

func (s *enrollServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *enrollServer) request(i int) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[i]
}

func (s *enrollServer) reply(i int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replies[i]
}

// buildFakeBrowser compiles the browser half of the sign-in as a program named
// the way this platform's opener is named, so the agent reaches it through the
// same exec.Command lookup a person's real browser is reached through. Anything
// that stubbed the opener inside the process would test a call the installed
// binary does not make.
func buildFakeBrowser(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	write("go.mod", "module fakebrowser\n\ngo 1.26\n")
	write("main.go", fakeBrowserSource)

	name := "xdg-open"
	if runtime.GOOS == "darwin" {
		name = "open"
	}
	cmd := exec.Command("go", "build", "-o", filepath.Join(dir, name), ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the fake browser: %v\n%s", err, out)
	}
	return dir
}

// fakeBrowserSource is the page's half of the flow: it takes the port and state
// out of the URL it was opened with and posts an ID token to the loopback
// listener, exactly as the served sign-in page does.
const fakeBrowserSource = `package main

import (
	"net/http"
	"net/url"
	"os"
	"strings"
)

func main() {
	u, err := url.Parse(os.Args[len(os.Args)-1])
	if err != nil {
		os.Exit(1)
	}
	q := u.Query()
	body := ` + "`" + `{"id_token":"IDTOKEN-e2e","state":"` + "`" + ` + q.Get("state") + ` + "`" + `"}` + "`" + `
	res, err := http.Post("http://127.0.0.1:"+q.Get("port")+"/callback",
		"application/json", strings.NewReader(body))
	if err != nil {
		os.Exit(1)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
`

// installEnv runs the agent against a private home and a private browser. HOME
// is redirected as well as LOOP_SESSIONS_HOME because install scans the home
// directory for sessions before it does anything else.
func installEnv(home, browser string) []string {
	env := []string{
		"HOME=" + home,
		"LOOP_SESSIONS_HOME=" + config.Paths{Home: home}.Root(),
		"PATH=" + browser + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	for _, keep := range []string{"GOPATH", "GOCACHE", "GOMODCACHE", "TMPDIR"} {
		if v := os.Getenv(keep); v != "" {
			env = append(env, keep+"="+v)
		}
	}
	return env
}

func readConfig(t *testing.T, home string) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Paths{Home: home})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !strings.Contains(cfg.Email, "@") {
		t.Fatalf("config has no identity: %+v", cfg)
	}
	return cfg
}
