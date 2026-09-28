package capture

import (
	"os"
	"path/filepath"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// launcherFromEnv reads what started the harness off the environment the hook
// inherits from it. Nothing is forked to find out: a hook runs on every
// session start and a process spawn there is paid by every person every time.
//
// The three variables are the ones the harness and macOS set for free.
// CLAUDE_CODE_ENTRYPOINT is the harness's own word; __CFBundleIdentifier is
// the launching app on macOS (a terminal, an editor, a GUI host that spawns
// and discards CLI processes); TERM_PROGRAM is the terminal emulator when
// there is one. A GUI host that sets none of them is, by that absence, told
// apart from a person's terminal, which is the class behind lifecycle-only
// sessions.
func launcherFromEnv() *event.Launcher {
	l := &event.Launcher{
		Entrypoint: os.Getenv("CLAUDE_CODE_ENTRYPOINT"),
		BundleID:   os.Getenv("__CFBundleIdentifier"),
		Term:       os.Getenv("TERM_PROGRAM"),
	}
	if comm, ok := parentComm(os.Getppid()); ok {
		l.ParentComm = filepath.Base(comm)
	}
	return l
}
