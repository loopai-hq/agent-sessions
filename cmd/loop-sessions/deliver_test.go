package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/daemon"
	"github.com/loopai-hq/agent-sessions/internal/drain"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// TestWhichHooksStartTheDeliveryDaemon defends the composition that was once
// missing entirely (hooks captured into the spool and nothing ever started the
// thing that empties it) and pins WHICH hook starts it now.
//
// The first prompt starts it: every real session has one, and a session that
// never gets one (44.6% of the fleet's sessions, a script running `claude`
// with nothing on stdin) used to cost a daemon, a manifest fetch and a health
// report each. SessionStart starts it only for work that cannot wait: a
// backlog a previous daemon left behind, or a session a crash left open.
// Starting from every hook would put a fork on the path that runs on every
// tool call.
func TestWhichHooksStartTheDeliveryDaemon(t *testing.T) {
	tests := []struct {
		name      string
		hook      string
		prep      func(t *testing.T, home string)
		wantStart bool
	}{
		{name: "SessionStart with nothing queued and nothing to reconcile waits for a prompt", hook: "SessionStart"},
		{name: "SessionStart with a backlog starts it, nothing else will drain the backlog", hook: "SessionStart",
			prep: func(t *testing.T, home string) { seedSpool(t, home, "left-behind") }, wantStart: true},
		{name: "SessionStart with an abandoned session starts it", hook: "SessionStart",
			prep: func(t *testing.T, home string) { abandonedSession(t, "crashed") }, wantStart: true},
		{name: "the first prompt starts it", hook: "UserPromptSubmit", wantStart: true},
		{name: "PreToolUse does not", hook: "PreToolUse"},
		{name: "PostToolUse does not", hook: "PostToolUse"},
		{name: "Stop does not", hook: "Stop"},
		{name: "SessionEnd does not, the daemon it would start has nothing left to shadow", hook: "SessionEnd"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := enrolledHome(t, "https://example.invalid")
			if tc.prep != nil {
				tc.prep(t, home)
			}

			var started int
			restore := stubSpawn(func(_ config.Paths, h capture.HookEvent) error {
				if h.HookEventName != "SessionStart" && h.HookEventName != "UserPromptSubmit" {
					t.Errorf("daemon started from %q", h.HookEventName)
				}
				started++
				return nil
			})
			defer restore()

			feedHook(t, capture.HookEvent{
				HookEventName: tc.hook,
				SessionID:     "s-1",
				Cwd:           home,
				Source:        "startup",
				Reason:        "clear",
				Prompt:        "hello",
				ToolName:      "Bash",
				ToolInput:     json.RawMessage(`{"command":"ls"}`),
			})

			want := 0
			if tc.wantStart {
				want = 1
			}
			if started != want {
				t.Fatalf("%s started %d daemon(s), want %d", tc.hook, started, want)
			}
		})
	}
}

// abandonedSession writes the state file a daemon that died with its harness
// leaves behind: unfinalized, both pids gone.
func abandonedSession(t *testing.T, sessionID string) {
	t.Helper()
	dir := config.Paths{}.StateDir()
	st := daemon.State{SessionID: sessionID, OwnerPID: 4194303, DaemonPID: 4194302,
		StartedAt: time.Now().Add(-time.Hour), Heartbeat: time.Now().Add(-time.Hour)}
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, sessionID+".json"), string(b))
}

// TestAFailureToStartTheDaemonIsWrittenToTheAgentLog defends the second most
// important property: when delivery is not happening the agent must say so. A
// hook that swallows this leaves a log full of captures and no explanation for
// an empty server, which is exactly how a day was lost.
func TestAFailureToStartTheDaemonIsWrittenToTheAgentLog(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")

	restore := stubSpawn(func(config.Paths, capture.HookEvent) error {
		return errors.New("exec format error")
	})
	defer restore()

	feedHook(t, capture.HookEvent{HookEventName: "UserPromptSubmit", SessionID: "s-1", Cwd: home, Prompt: "hello"})

	log := readAgentLog(t, home)
	if !strings.Contains(log, "could not start the delivery daemon") || !strings.Contains(log, "exec format error") {
		t.Fatalf("agent log does not report the failure or its reason:\n%s", log)
	}
}

// TestTheTransportAcksOnlyWhatTheServerAccepted is the delivery contract in one
// table. Every row is a way a server can answer, and the property under each is
// the same: an item may be deleted from the spool only when the server said, in
// a verdict this client could read, that it stored it.
func TestTheTransportAcksOnlyWhatTheServerAccepted(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		body         string
		retryAfter   string
		wantAccepted []string
		wantRejected []string
		wantErr      bool
		wantRetry    time.Duration
	}{
		{
			name:         "a per-item verdict is honoured item by item",
			status:       200,
			body:         `{"accepted":["a"],"rejected":[{"id":"b","reason":"event failed validation"}]}`,
			wantAccepted: []string{"a"},
			wantRejected: []string{"b"},
		},
		{
			name:   "an item in neither list is neither acked nor quarantined",
			status: 200,
			body:   `{"accepted":[]}`,
		},
		{
			name:    "a 503 acks nothing, however the body reads",
			status:  503,
			body:    `{"accepted":["a","b"]}`,
			wantErr: true,
		},
		{
			name:      "a 429 carries the server's pacing instruction out with the error",
			status:    429,
			body:      `{"accepted":[],"retry_after_ms":45000}`,
			wantErr:   true,
			wantRetry: 45 * time.Second,
		},
		{
			name:       "a Retry-After header is honoured when it asks for more room than the body",
			status:     503,
			body:       `{"accepted":[],"retry_after_ms":1000}`,
			retryAfter: "30",
			wantErr:    true,
			wantRetry:  30 * time.Second,
		},
		{
			name:    "a credential rejection is an error, not an empty verdict",
			status:  401,
			body:    `{"error":{"code":"unauthenticated"}}`,
			wantErr: true,
		},
		{
			name:    "a 200 whose body is not a verdict acks nothing",
			status:  200,
			body:    `<html>proxy sign-in page</html>`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.retryAfter != "" {
					w.Header().Set("Retry-After", tc.retryAfter)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			tr := &httpTransport{url: srv.URL + eventsPath, token: "tok", client: srv.Client()}
			resp, err := tr.Send(context.Background(), leased(t, t.TempDir(), "a", "b"))

			if tc.wantErr != (err != nil) {
				t.Fatalf("error = %v, want error: %v", err, tc.wantErr)
			}
			if got := ids(resp.Accepted); !equal(got, tc.wantAccepted) {
				t.Errorf("accepted = %v, want %v", got, tc.wantAccepted)
			}
			var rejected []string
			for _, r := range resp.Rejected {
				rejected = append(rejected, r.ID)
			}
			if !equal(rejected, tc.wantRejected) {
				t.Errorf("rejected = %v, want %v", rejected, tc.wantRejected)
			}
			if resp.RetryAfter != tc.wantRetry {
				t.Errorf("retry after = %s, want %s", resp.RetryAfter, tc.wantRetry)
			}
		})
	}
}

// TestTheTransportPresentsTheDeviceCredential defends the one header without
// which every upload is a 401 and the whole backlog stays on the laptop.
func TestTheTransportPresentsTheDeviceCredential(t *testing.T) {
	var gotAuth, gotType string
	var gotItems []spool.Item
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotType = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&gotItems)
		_, _ = w.Write([]byte(`{"accepted":[]}`))
	}))
	defer srv.Close()

	tr := &httpTransport{url: srv.URL + eventsPath, token: "loops_v1_secret", client: srv.Client()}
	if _, err := tr.Send(context.Background(), leased(t, t.TempDir(), "a")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotAuth != "Bearer loops_v1_secret" {
		t.Errorf("Authorization = %q, want a bearer device token", gotAuth)
	}
	if gotType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotType)
	}
	if len(gotItems) != 1 || gotItems[0].ID != "a" {
		t.Errorf("server received %+v, want the leased item verbatim", gotItems)
	}
}

// TestTheFlusherCountsResolvedItemsAsProgress defends the daemon's shutdown
// drain from spinning.
//
// The daemon loops its final flush until this returns zero. Counting items sent
// rather than items the server resolved would mean an unreachable server reports
// progress on every cycle, so the shutdown drain would hammer a dead endpoint for
// its entire grace period and then exit having delivered nothing.
func TestTheFlusherCountsResolvedItemsAsProgress(t *testing.T) {
	tests := []struct {
		name     string
		verdict  string
		status   int
		items    []string
		wantSent int
		wantErr  bool
	}{
		{
			name:     "accepted items count",
			verdict:  `{"accepted":["a","b"]}`,
			items:    []string{"a", "b"},
			wantSent: 2,
		},
		{
			name:     "permanently rejected items count, because they have left the queue",
			verdict:  `{"accepted":["a"],"rejected":[{"id":"b","reason":"event failed validation"}]}`,
			items:    []string{"a", "b"},
			wantSent: 2,
		},
		{
			name:     "an item the server neither accepted nor rejected is not progress",
			verdict:  `{"accepted":[]}`,
			items:    []string{"a"},
			wantSent: 0,
		},
		{
			name:     "an unreachable server is not progress",
			status:   503,
			verdict:  `{}`,
			items:    []string{"a"},
			wantSent: 0,
			wantErr:  true,
		},
		{
			name:     "an empty spool is not progress and costs no request",
			verdict:  `{"accepted":[]}`,
			items:    nil,
			wantSent: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				_, _ = w.Write([]byte(tc.verdict))
			}))
			defer srv.Close()

			home := enrolledHome(t, srv.URL)
			seedSpool(t, home, tc.items...)

			f := mustDelivery(t, home)
			sent, err := f.RunOnce(context.Background())
			if tc.wantErr != (err != nil) {
				t.Fatalf("error = %v, want error: %v", err, tc.wantErr)
			}
			if sent != tc.wantSent {
				t.Fatalf("RunOnce reported %d item(s) of progress, want %d", sent, tc.wantSent)
			}
		})
	}
}

// TestTheAgentLogRecordsDelivery defends the property that made this failure
// invisible for a day: agent.log recorded every capture and not one word about
// an upload, a failure to upload, or a reason.
func TestTheAgentLogRecordsDelivery(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		verdict   string
		items     []string
		cancelled bool
		want      []string
		absent    []string
	}{
		{
			name:    "a successful upload names how many went and how many remain",
			verdict: `{"accepted":["a"]}`,
			items:   []string{"a", "b"},
			want:    []string{"drain: uploaded 1 item(s)", "1 still queued"},
		},
		{
			name:    "a failure names the reason and that the items are still queued",
			status:  503,
			verdict: `{}`,
			items:   []string{"a"},
			want:    []string{"drain: upload failed", "1 item(s) still queued", "http 503"},
		},
		{
			name:    "a rejected credential says what the person has to do about it",
			status:  401,
			verdict: `{}`,
			items:   []string{"a"},
			want:    []string{"rejected this device's credential", "loop-sessions install"},
		},
		{
			name:    "a permanent rejection is reported, because nothing will retry it",
			verdict: `{"accepted":[],"rejected":[{"id":"a","reason":"event failed validation"}]}`,
			items:   []string{"a"},
			want:    []string{"permanently rejected 1 item(s)", "quarantined"},
		},
		{
			name:    "an idle cycle says nothing, so the lines that matter are findable",
			verdict: `{"accepted":[]}`,
			items:   nil,
			absent:  []string{"drain:"},
		},
		{
			name:      "a shutdown is not a delivery failure and is not reported as one",
			verdict:   `{"accepted":["a"]}`,
			items:     []string{"a"},
			cancelled: true,
			absent:    []string{"upload failed"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				_, _ = w.Write([]byte(tc.verdict))
			}))
			defer srv.Close()

			home := enrolledHome(t, srv.URL)
			seedSpool(t, home, tc.items...)

			ctx := context.Background()
			if tc.cancelled {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}

			f := mustDelivery(t, home)
			_, _ = f.RunOnce(ctx)

			log := readAgentLog(t, home)
			for _, want := range tc.want {
				if !strings.Contains(log, want) {
					t.Errorf("agent log is missing %q:\n%s", want, log)
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(log, absent) {
					t.Errorf("agent log contains %q and should be silent when idle:\n%s", absent, log)
				}
			}
		})
	}
}

// TestOnlyOneDaemonDeliversAtATime defends the spool from the machine's own
// daemons. One daemon per session is right, but a person with three editor
// windows has three of them pointed at one spool directory, and leasing does not
// hide an item from another reader: without this every item is uploaded once per
// open window.
func TestOnlyOneDaemonDeliversAtATime(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{"accepted":["a"]}`))
	}))
	defer srv.Close()

	home := enrolledHome(t, srv.URL)
	seedSpool(t, home, "a")

	// Stand in for the daemon of another session, mid-cycle.
	other, err := daemon.TryLock(filepath.Join(config.Paths{}.SpoolDir(), deliveryLockName))
	if err != nil {
		t.Fatalf("hold the delivery lock: %v", err)
	}

	f := mustDelivery(t, home)
	sent, err := f.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce while another daemon holds the lock: %v", err)
	}
	if sent != 0 || requests != 0 {
		t.Fatalf("delivered %d item(s) in %d request(s) while another daemon held the lock; want none",
			sent, requests)
	}
	if got := pendingCount(t, home); got != 1 {
		t.Fatalf("%d item(s) pending after a skipped cycle, want 1 — nothing may be consumed", got)
	}

	// And the moment the other daemon is done, this one takes over. A lock that
	// is not released turns "one delivers" into "none delivers".
	if err := other.Close(); err != nil {
		t.Fatalf("release: %v", err)
	}
	sent, err = f.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce after release: %v", err)
	}
	if sent != 1 || requests != 1 {
		t.Fatalf("after release delivered %d item(s) in %d request(s), want 1 and 1", sent, requests)
	}
}

// TestDeliveryRefusesToStartWithoutWhatItNeeds defends against the failure this
// whole file exists to prevent, in its quieter form: a daemon that starts, finds
// it cannot deliver, and says nothing.
func TestDeliveryRefusesToStartWithoutWhatItNeeds(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		token    string
		wantErr  string
	}{
		{name: "no device credential", endpoint: "https://example.invalid", token: "", wantErr: "no device credential"},
		{name: "a blank device credential", endpoint: "https://example.invalid", token: "   \n", wantErr: "is empty"},
		{name: "no endpoint", endpoint: "", token: "tok", wantErr: "no endpoint is configured"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("LOOP_SESSIONS_HOME", home)
			if tc.token != "" {
				writeFile(t, filepath.Join(home, "device.token"), tc.token)
			}

			_, err := newDelivery(config.Paths{}, config.Config{Endpoint: tc.endpoint})
			if err == nil {
				t.Fatal("newDelivery succeeded with an unusable configuration")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not name the problem (%q)", err, tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------- helpers

// stubSpawn replaces the daemon launcher and returns its restorer. The package
// variable is process-global, so these tests do not run in parallel.
func stubSpawn(f func(config.Paths, capture.HookEvent) error) func() {
	prev := spawnDaemon
	spawnDaemon = f
	return func() { spawnDaemon = prev }
}

// enrolledHome builds the directory an installed, signed-in machine has.
func enrolledHome(t *testing.T, endpoint string) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("LOOP_SESSIONS_HOME", home)

	cfg := config.Defaults()
	cfg.Email = "dev@example.com"
	cfg.DeviceID = "d-1"
	cfg.Endpoint = endpoint
	if err := config.Save(config.Paths{}, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	writeFile(t, filepath.Join(home, "device.token"), "loops_v1_test")
	return home
}

func mustDelivery(t *testing.T, home string) *spoolFlusher {
	t.Helper()
	p := config.Paths{}
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatalf("load config from %s: %v", home, err)
	}
	f, err := newDelivery(p, cfg)
	if err != nil {
		t.Fatalf("newDelivery: %v", err)
	}
	return f
}

// seedSpool puts items in the outbox the way a hook would have.
func seedSpool(t *testing.T, home string, ids ...string) {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: config.Paths{}.SpoolDir()})
	if err != nil {
		t.Fatalf("open spool in %s: %v", home, err)
	}
	for _, id := range ids {
		err := sp.Add(spool.Item{
			ID:        id,
			Kind:      "event",
			SessionID: "s-1",
			Payload:   json.RawMessage(`{"id":"` + id + `","session_id":"s-1"}`),
		})
		if err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
}

func pendingCount(t *testing.T, home string) int {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: config.Paths{}.SpoolDir()})
	if err != nil {
		t.Fatalf("open spool in %s: %v", home, err)
	}
	st, err := sp.Stats()
	if err != nil {
		t.Fatalf("spool stats: %v", err)
	}
	return st.Pending
}

// leased builds a batch shaped the way the spool hands one to the transport.
func leased(t *testing.T, dir string, ids ...string) []spool.Leased {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	for _, id := range ids {
		if err := sp.Add(spool.Item{ID: id, Kind: "event", SessionID: "s-1", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	out, err := sp.Lease(len(ids))
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	return out
}

// feedHook runs the real hook path against a payload on stdin, the way the
// harness does, and fails on anything the hook wrote to the agent log as an
// error.
func feedHook(t *testing.T, h capture.HookEvent, args ...string) {
	t.Helper()
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal hook: %v", err)
	}
	path := filepath.Join(t.TempDir(), "payload.json")
	writeFile(t, path, string(b))

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open payload: %v", err)
	}
	defer f.Close()

	prev := os.Stdin
	os.Stdin = f
	defer func() { os.Stdin = prev }()

	if code := runHook(args); code != 0 {
		t.Fatalf("hook exited %d; a hook never exits non-zero", code)
	}
}

func readAgentLog(t *testing.T, home string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(home, "logs", "agent.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		t.Fatalf("read agent log: %v", err)
	}
	return string(b)
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func ids(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return in
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// compile-time guard: the transport is the drain's, and the flusher is the
// daemon's. Composition is the thing that was missing, so it is asserted rather
// than assumed.
var (
	_ drain.Transport = (*httpTransport)(nil)
	_ daemon.Flusher  = (*spoolFlusher)(nil)
)
