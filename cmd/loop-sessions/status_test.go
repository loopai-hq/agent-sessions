package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// The whole of this file defends one property: `status` is run when something is
// already wrong, so it must be able to say which wrong thing. A machine that
// captured nothing, a machine holding a queue it cannot deliver, and a machine
// that has delivered everything are three different situations with three
// different remedies, and until this file existed all three printed
// "Waiting to upload: 0 item(s), 0 B" or its equally uninformative sibling.

// laptop is a machine in a known state: a config, a spool, and whatever delivery
// history the case under test needs. Built on a temp home so nothing here can
// read or write the real ~/.loop.
type laptop struct {
	home string
	sp   *spool.Spool
}

func newLaptop(t *testing.T, installedAgo time.Duration) *laptop {
	t.Helper()
	home := t.TempDir()
	t.Setenv("LOOP_SESSIONS_HOME", home)

	p := config.Paths{}
	cfg := config.Defaults()
	cfg.Email = "someone@example.com"
	cfg.Endpoint = "https://sessions.example.com"
	cfg.DeviceID = "device-1"
	cfg.InstalledAt = time.Now().Add(-installedAgo)
	if err := config.Save(p, cfg); err != nil {
		t.Fatal(err)
	}

	sp, err := spool.Open(spool.Options{Dir: p.SpoolDir()})
	if err != nil {
		t.Fatal(err)
	}
	return &laptop{home: home, sp: sp}
}

// capture writes n items and backdates them, because the age of a queue is the
// evidence that separates "waiting" from "not moving".
func (l *laptop) capture(t *testing.T, n int, queuedAgo time.Duration) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := l.sp.Add(spool.Item{Kind: "event", SessionID: "s1", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
			t.Fatal(err)
		}
	}
	dir := filepath.Join(config.Paths{}.SpoolDir(), "pending")
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().Add(-queuedAgo)
	for _, e := range ents {
		if err := os.Chtimes(filepath.Join(dir, e.Name()), when, when); err != nil {
			t.Fatal(err)
		}
	}
}

// deliverAll drains the spool the way a real delivery does: lease, then ack.
func (l *laptop) deliverAll(t *testing.T) {
	t.Helper()
	leased, err := l.sp.Lease(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) == 0 {
		t.Fatal("nothing to deliver; the fixture captured nothing")
	}
	for _, it := range leased {
		if err := l.sp.Ack(it); err != nil {
			t.Fatal(err)
		}
	}
}

// runStatusOutput runs the command a person runs and returns what they see.
func runStatusOutput(t *testing.T, args ...string) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	runErr := runStatus(args)
	os.Stdout = saved
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if runErr != nil {
		t.Fatalf("status returned an error: %v", runErr)
	}
	return string(out)
}

func statusJSON(t *testing.T) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(runStatusOutput(t, "--json")), &v); err != nil {
		t.Fatalf("status --json is not JSON: %v", err)
	}
	return v
}

// TestStatusNamesWhichSituationTheMachineIsIn is the defect in one table. Each
// row is a real laptop somebody has had, and the property is that no two rows
// produce the same reading.
func TestStatusNamesWhichSituationTheMachineIsIn(t *testing.T) {
	stale := 20 * time.Minute

	cases := []struct {
		name      string
		setup     func(t *testing.T, l *laptop)
		wantState string
		wantSays  []string
		wantNever []string
	}{
		{
			name:      "nothing captured is not the same reading as everything delivered",
			setup:     func(t *testing.T, l *laptop) {},
			wantState: "nothing_captured",
			wantSays:  []string{"nothing has been captured"},
			wantNever: []string{"everything captured has been uploaded"},
		},
		{
			name: "a fresh queue is waiting, and says how much and how old",
			setup: func(t *testing.T, l *laptop) {
				l.capture(t, 3, time.Minute)
			},
			wantState: "waiting",
			wantSays:  []string{"3 item(s)", "waiting to upload"},
			wantNever: []string{"nothing has been captured"},
		},
		{
			name: "a queue that has never moved is reported as not uploading",
			setup: func(t *testing.T, l *laptop) {
				l.capture(t, 10, stale)
			},
			wantState: "not_uploading",
			wantSays:  []string{"NOT UPLOADING", "10 item(s)", "nothing has ever been uploaded"},
			wantNever: []string{"nothing has been captured"},
		},
		{
			name: "everything delivered says so, rather than printing a bare zero",
			setup: func(t *testing.T, l *laptop) {
				l.capture(t, 2, time.Minute)
				l.deliverAll(t)
			},
			wantState: "up_to_date",
			wantSays:  []string{"everything captured has been uploaded"},
			wantNever: []string{"nothing has been captured", "NOT UPLOADING"},
		},
		{
			name: "delivery that worked and then stopped is not reported as healthy",
			setup: func(t *testing.T, l *laptop) {
				l.capture(t, 1, time.Minute)
				l.deliverAll(t)
				l.capture(t, 4, stale)
			},
			wantState: "not_uploading",
			wantSays:  []string{"NOT UPLOADING", "4 item(s)"},
			wantNever: []string{"everything captured has been uploaded"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newLaptop(t, time.Hour)
			tc.setup(t, l)

			out := runStatusOutput(t)
			for _, want := range tc.wantSays {
				if !strings.Contains(out, want) {
					t.Errorf("status never said %q; it said:\n%s", want, out)
				}
			}
			for _, never := range tc.wantNever {
				if strings.Contains(out, never) {
					t.Errorf("status said %q, which is not true of this machine:\n%s", never, out)
				}
			}
			if got := statusJSON(t)["state"]; got != tc.wantState {
				t.Errorf("state = %v, want %q", got, tc.wantState)
			}
		})
	}
}

// TestStatusReportsWhyUploadsAreFailing defends the second half of a useful
// verdict: a person told that nothing is uploading still cannot act unless they
// are told what the server said.
func TestStatusReportsWhyUploadsAreFailing(t *testing.T) {
	l := newLaptop(t, time.Hour)
	l.capture(t, 5, 20*time.Minute)
	l.sp.RecordDeliveryFailure(errors.New(
		"delivery: https://sessions.example.com/v1/events rejected this device's credential (http 401)"))

	out := runStatusOutput(t)
	for _, want := range []string{"http 401", "rejected this device's credential"} {
		if !strings.Contains(out, want) {
			t.Errorf("status hid the reason %q; it said:\n%s", want, out)
		}
	}
	if got := statusJSON(t)["last_error"]; got == nil || !strings.Contains(got.(string), "http 401") {
		t.Errorf("last_error = %v, want the recorded transport error", got)
	}
}

// TestStatusSaysWhenTheQueueIsFailingWithNoRecordedReason defends the honest
// absence: an uploader that never recorded why must produce "not recorded",
// never a blank that reads as "no problem".
func TestStatusSaysWhenTheQueueIsFailingWithNoRecordedReason(t *testing.T) {
	l := newLaptop(t, time.Hour)
	l.capture(t, 2, 20*time.Minute)

	out := runStatusOutput(t)
	if !strings.Contains(out, "no reason was recorded") {
		t.Errorf("a stuck queue with no recorded error must say so; it said:\n%s", out)
	}
	if !strings.Contains(out, "agent.log") {
		t.Errorf("status must point at the log when it has no reason of its own:\n%s", out)
	}
}

// TestStatusRefusesToPrintZeroForASpoolItCannotRead is defect 4 in its purest
// form: an unreadable outbox produced the same "0 item(s)" as an empty one, so a
// broken machine and an idle one were indistinguishable.
func TestStatusRefusesToPrintZeroForASpoolItCannotRead(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read a directory with no permissions, so the failure cannot be staged")
	}
	l := newLaptop(t, time.Hour)
	l.capture(t, 4, time.Minute)

	pending := filepath.Join(config.Paths{}.SpoolDir(), "pending")
	if err := os.Chmod(pending, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(pending, 0o700) })

	out := runStatusOutput(t)
	if strings.Contains(out, "0 item(s)") {
		t.Errorf("status invented a count of zero for an unreadable spool:\n%s", out)
	}
	if !strings.Contains(out, "could not be read") {
		t.Errorf("status must say the outbox is unreadable; it said:\n%s", out)
	}
	v := statusJSON(t)
	if v["state"] != "unknown" {
		t.Errorf("state = %v, want \"unknown\" when the spool cannot be read", v["state"])
	}
	if v["spool_error"] == nil {
		t.Error("spool_error must carry the read failure so a script can see it too")
	}
}

// TestStatusDoesNotCallAMachineActiveOnTheStrengthOfASettingAlone defends the
// word. "active" was printed by a machine that had captured nothing and uploaded
// nothing, because it was derived from the paused flag and nothing else; a word
// that asserts liveness has to be backed by evidence of liveness.
func TestStatusDoesNotCallAMachineActiveOnTheStrengthOfASettingAlone(t *testing.T) {
	l := newLaptop(t, time.Hour)
	l.capture(t, 3, 30*time.Minute)

	out := runStatusOutput(t)
	if strings.Contains(out, "Status:  active") {
		t.Errorf("a machine that has never uploaded must not be summarised as active:\n%s", out)
	}
}

// TestStatusStillReportsTheUsersPause keeps the one verdict that was already
// honest: paused is a choice, and it must not be dressed up as a fault.
func TestStatusStillReportsTheUsersPause(t *testing.T) {
	newLaptop(t, time.Hour)
	p := config.Paths{}
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Save(p, cfg.Pause(time.Now())); err != nil {
		t.Fatal(err)
	}

	out := runStatusOutput(t)
	if !strings.Contains(strings.ToLower(out), "paused") {
		t.Errorf("a paused machine must say it is paused:\n%s", out)
	}
	if statusJSON(t)["paused"] != true {
		t.Error("paused must survive into the machine-readable view")
	}
}

// TestStatusSurfacesDropsRecordedByAnotherProcess defends the cross-process
// half of honesty. Capture drops events inside a hook that exits immediately;
// unless the count outlives it, `status` reports a clean machine at the exact
// moment data is being thrown away.
func TestStatusSurfacesDropsRecordedByAnotherProcess(t *testing.T) {
	l := newLaptop(t, time.Hour)

	// A second Spool over the same directory stands in for the hook process
	// that observed the full disk and exited.
	hook, err := spool.Open(spool.Options{
		Dir:      config.Paths{}.SpoolDir(),
		DiskFree: func(string) (uint64, uint64, error) { return 1, 100, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.Add(spool.Item{Kind: "event", Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("a nearly-full disk must refuse the write")
	}
	_ = l

	out := runStatusOutput(t)
	if !strings.Contains(out, "disk is nearly full") {
		t.Errorf("status must report drops recorded by another process:\n%s", out)
	}
}
