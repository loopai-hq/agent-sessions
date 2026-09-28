package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/capture"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// The hook path is ledger-first. These tests hold a hook at a chosen point
// and look at what is on disk, which is what a recovery pass would see if
// the process had died there.

// gate is a scrubber that blocks every call until released.
type gate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGate() *gate {
	return &gate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) scrub(s string) (string, map[string]int) {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return s, nil
}

// wroteSink wraps the real sink and reports when a write completes.
type wroteSink struct {
	inner capture.Sink
	delay time.Duration
	wrote chan struct{}
	once  sync.Once
}

func (w *wroteSink) Put(e event.Event) error {
	time.Sleep(w.delay)
	err := w.inner.Put(e)
	w.once.Do(func() { close(w.wrote) })
	return err
}

// withHookSeams installs test seams and returns a channel that is closed when
// the hook's capture goroutine has finished, so a test that abandons a hook can
// wait for the abandoned goroutine before the seams and the temp home go away.
func withHookSeams(t *testing.T, scrub capture.Scrubber, sink func(*spool.Spool) (capture.Sink, error)) <-chan struct{} {
	t.Helper()
	prevScrub, prevSink, prevDone := hookScrub, hookSink, onHookDone
	if scrub != nil {
		hookScrub = scrub
	}
	if sink != nil {
		hookSink = sink
	}
	finished := make(chan struct{})
	var once sync.Once
	onHookDone = func() { once.Do(func() { close(finished) }) }
	restoreSpawn := stubSpawn(func(config.Paths, capture.HookEvent) error { return nil })
	t.Cleanup(func() {
		hookScrub, hookSink, onHookDone = prevScrub, prevSink, prevDone
		restoreSpawn()
	})
	return finished
}

func stdinFrom(t *testing.T, h capture.HookEvent) {
	t.Helper()
	path := writePayload(t, h)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	prev := os.Stdin
	os.Stdin = f
	t.Cleanup(func() { os.Stdin = prev; _ = f.Close() })
}

func TestHookWritesInflightMarkerBeforeScrub(t *testing.T) {
	enrolledHome(t, "https://example.invalid")
	g := newGate()
	written := &wroteSink{wrote: make(chan struct{})}
	withHookSeams(t, g.scrub, func(sp *spool.Spool) (capture.Sink, error) {
		inner, err := hookSinkDefault(sp)
		written.inner = inner
		return written, err
	})
	stdinFrom(t, capture.HookEvent{HookEventName: "UserPromptSubmit", SessionID: "s-marker", PromptID: "p-1", Prompt: "hello"})

	done := make(chan int, 1)
	go func() { done <- runHook(nil) }()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the scrubber was never entered")
	}

	ledger := capture.Ledger{Dir: config.Paths{}.StateDir()}
	if !ledger.HasInflight("s-marker") {
		t.Fatal("no inflight marker while the hook is inside the scrubber; a death here would lose the event untraceably")
	}
	c, _ := ledger.Read("s-marker")
	if len(c.Inflight) != 1 || c.Inflight[0].HookEvent != "UserPromptSubmit" || c.Inflight[0].PromptID != "p-1" {
		t.Fatalf("marker = %+v, want the hook and its prompt", c.Inflight)
	}

	close(g.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the hook did not return after the scrubber was released")
	}
	if ledger.HasInflight("s-marker") {
		t.Fatal("the marker was not removed after the spool write")
	}
	c, _ = ledger.Read("s-marker")
	if !c.PromptIDs["p-1"] || c.Types[event.UserPrompt] != 1 {
		t.Fatalf("the captured ledger does not name the prompt: %+v", c)
	}
	if n := pendingCount(t, ""); n != 1 {
		t.Fatalf("pending = %d, want the one event", n)
	}
}

func TestAbandonedHookLeavesMarkerAndDurableCounter(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	g := newGate()
	written := &wroteSink{wrote: make(chan struct{})}
	finished := withHookSeams(t, g.scrub, func(sp *spool.Spool) (capture.Sink, error) {
		inner, err := hookSinkDefault(sp)
		written.inner = inner
		return written, err
	})
	// The blocked goroutine is released at cleanup and allowed to finish, so
	// it does not outlive the seams or the temp directory.
	t.Cleanup(func() {
		close(g.release)
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
		}
	})
	stdinFrom(t, capture.HookEvent{HookEventName: "UserPromptSubmit", SessionID: "s-lost", PromptID: "p-9", Prompt: "hello"})

	start := time.Now()
	if code := runHook([]string{"--timeout", "50ms"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("the abandon path did not return promptly")
	}

	ledger := capture.Ledger{Dir: config.Paths{}.StateDir()}
	if !ledger.HasInflight("s-lost") {
		t.Fatal("an abandoned hook must leave its marker for the recovery pass")
	}
	sp, err := spool.Open(spool.Options{Dir: config.Paths{}.SpoolDir()})
	if err != nil {
		t.Fatal(err)
	}
	st, _ := sp.Stats()
	if st.Dropped["hook_abandoned"] != 1 {
		t.Fatalf("hook_abandoned = %d, want 1: %v", st.Dropped["hook_abandoned"], st.Dropped)
	}
	log := readAgentLog(t, home)
	for _, want := range []string{"abandoning event=UserPromptSubmit", "session=s-lost", "phase=translate", "bytes="} {
		if !strings.Contains(log, want) {
			t.Errorf("the abandon line does not say %q:\n%s", want, log)
		}
	}
}

// A timeout that fires during the spool write waits for the write. The
// alternative is a temp file in pending/ and a lost event, to save nobody any
// time: nothing waits on an async hook.
func TestHookDoesNotExitBeforeSpoolWrite(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	written := &wroteSink{wrote: make(chan struct{}), delay: 300 * time.Millisecond}
	withHookSeams(t, nil, func(sp *spool.Spool) (capture.Sink, error) {
		inner, err := hookSinkDefault(sp)
		written.inner = inner
		return written, err
	})
	stdinFrom(t, capture.HookEvent{HookEventName: "UserPromptSubmit", SessionID: "s-slow", Prompt: "hello"})

	if code := runHook([]string{"--timeout", "50ms"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	select {
	case <-written.wrote:
	default:
		t.Fatal("runHook returned before the spool write finished")
	}
	if n := pendingCount(t, home); n != 1 {
		t.Fatalf("pending = %d, want the event to have landed", n)
	}
	sp, _ := spool.Open(spool.Options{Dir: config.Paths{}.SpoolDir()})
	if st, _ := sp.Stats(); st.Dropped["hook_abandoned"] != 0 {
		t.Fatalf("a completed write was counted as abandoned: %v", st.Dropped)
	}
	if !strings.Contains(readAgentLog(t, home), "waiting for the spool write") {
		t.Error("the log does not say the hook waited")
	}
	if capture.Ledger.HasInflight(capture.Ledger{Dir: config.Paths{}.StateDir()}, "s-slow") {
		t.Fatal("marker left behind after a completed write")
	}
}

// A hook opening the spool sweeps the temp file a dead hook left behind.
func TestStaleTmpFilesAreSwept(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	withHookSeams(t, nil, nil)
	pending := filepath.Join(config.Paths{}.SpoolDir(), "pending")
	if err := os.MkdirAll(pending, 0o700); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(pending, ".0000000000001-dead.json.tmp")
	writeFile(t, stale, `{"kind":"event"`)
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	feedHook(t, capture.HookEvent{HookEventName: "SessionStart", SessionID: "s-sweep", Cwd: home, Source: "startup"})

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("the stale temp file survived a hook")
	}
}

// Tool events record their tool_use_id in the ledger, which is the key the
// recovery pass subtracts from the transcript.
func TestHookRecordsToolAnchorsInTheLedger(t *testing.T) {
	home := enrolledHome(t, "https://example.invalid")
	withHookSeams(t, nil, nil)
	feedHook(t, capture.HookEvent{HookEventName: "PreToolUse", SessionID: "s-tool", Cwd: home,
		PromptID: "p-3", ToolUseID: "toolu_41", ToolName: "Bash", ToolInput: []byte(`{"command":"ls"}`)})
	feedHook(t, capture.HookEvent{HookEventName: "PostToolUse", SessionID: "s-tool", Cwd: home,
		PromptID: "p-3", ToolUseID: "toolu_42", ToolName: "Bash", ToolResponse: []byte(`{"stdout":"ok"}`)})
	c, err := capture.Ledger{Dir: config.Paths{}.StateDir()}.Read("s-tool")
	if err != nil {
		t.Fatal(err)
	}
	// The prompt id set is fed by user_prompt entries alone; a tool event
	// carries the turn's id but does not prove the prompt itself landed.
	if c.PromptIDs["p-3"] || c.Types[event.ToolResult] != 1 || c.Types[event.ToolCall] != 1 {
		t.Fatalf("ledger = %+v", c)
	}
	// Each hook vouches for itself: a PreToolUse lands in the call set and a
	// PostToolUse in the result set, never the other way round, so a landed
	// call cannot hide its own lost result from the recovery pass.
	if !c.ToolCallIDs["toolu_41"] || c.ToolResultIDs["toolu_41"] {
		t.Fatalf("PreToolUse is not in the call set alone: %+v", c)
	}
	if !c.ToolResultIDs["toolu_42"] || c.ToolCallIDs["toolu_42"] {
		t.Fatalf("PostToolUse is not in the result set alone: %+v", c)
	}
}

// hookSinkDefault is the production sink, reachable after a test replaced
// the seam.
func hookSinkDefault(sp *spool.Spool) (capture.Sink, error) {
	return pipelineSink(sp)
}
