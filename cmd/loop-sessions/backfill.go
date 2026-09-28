package main

// Backfill: the history that was already on the laptop when the agent arrived.
//
// internal/backfill has parsed transcripts correctly, and been tested for it,
// since before the first install. Nothing in cmd/loop-sessions referenced the
// package, so "no backfilled sessions were uploaded" was not a bug in the
// importer; the importer had never been called. This file is the call.
//
// # Why it is a command, and why install runs a bounded one
//
// The corpus on the machine this shipped to is 6,403 transcripts and 2.8 GB. An
// install that imported all of it before returning would look hung for minutes
// and, on a tethered connection, would spend somebody's data allowance without
// asking. Asking is not available either: the installer is piped to sh, so this
// program's stdin is the remainder of the script and a prompt would consume the
// script as its own answer.
//
// So both. `loop-sessions backfill` imports everything and is the thing a person
// runs deliberately. `install` imports a recent window — long enough that the
// dashboard has something in it the same evening, short enough that it finishes
// while somebody is still watching the terminal — announces the window, prints
// progress as it goes, and says how to import the rest. Either can be stopped
// with Ctrl-C and resumed, and `--skip-backfill` opts out entirely.
//
// # Why it uploads as it walks
//
// The spool drops its OLDEST pending items when it reaches its byte cap. An
// import that only wrote would therefore delete the events it had just written —
// and the live session's events with them — somewhere around the 2 GiB mark, and
// would report success while doing it. So the walk paces itself against the
// outbox: when the queue grows past a high-water mark the import delivers until
// it is small again, and if delivery is not working the import stops and says so
// rather than filling a queue that eats itself.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/backfill"
	"github.com/loopai-hq/agent-sessions/internal/config"
	"github.com/loopai-hq/agent-sessions/internal/discovery"
	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/pipeline"
	"github.com/loopai-hq/agent-sessions/internal/scrub"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

const (
	// defaultInstallWindow is how much history `install` imports on its own.
	//
	// Measured on the reference machine — 6,403 transcripts, 2.8 GB, an unusually
	// heavy user — rather than guessed:
	//
	//	--since all   607,975 events   8m49s
	//	--since 30d   419,711 events   4m16s
	//	--since 7d     70,640 events   1m23s
	//
	// The whole corpus, because a history tool that defaults to a week is not
	// the thing it claims to be. Claude Code deletes local transcripts after
	// cleanupPeriodDays (30 by default), so what somebody has on disk at install
	// is the only copy that will ever exist — a window that skips it does not
	// defer that history, it loses it. The nine minutes buys a corpus nobody can
	// reconstruct later; the eight it saves buys nothing.
	//
	// That is affordable here only because of where the import sits: install
	// runs it last, after the machine is already capturing, reports a failure
	// rather than returning it, and journals every file it sends. So the cost of
	// the longer default is a progress counter somebody can walk away from —
	// Ctrl-C leaves a working install and `loop-sessions backfill` resumes from
	// the journal. Anyone who wants less passes --backfill-since 7d or
	// --skip-backfill, and the flag says so.
	//
	// The walk itself is a fixed cost either way: every transcript is read
	// whichever window is chosen, because the sequence numbers that make ids
	// stable are assigned in walk order and skipping files would renumber the
	// ones that follow. Only redaction is skipped for what falls outside, which
	// is where the difference between 8m49s and 1m23s comes from.
	defaultInstallWindow = "all"

	// journalName is the resume record, one line per imported file.
	//
	// Append-only rather than a rewritten map: at 6,403 files a rewrite per file
	// is quadratic, and a torn final line after a crash costs one file being
	// imported twice — which the server dedups on the event id — where a torn
	// map would cost the whole record.
	journalName = "backfill.jsonl"

	// The outbox bounds. A walk stops feeding the spool once either is crossed
	// and resumes when delivery has brought the queue back under the low marks.
	// Bytes matter as much as counts because a single transcript line can be
	// over a megabyte.
	outboxHighItems = 500
	outboxLowItems  = 100
	outboxHighBytes = 64 << 20
	outboxLowBytes  = 8 << 20

	// paceEvery is how many queued events pass between outbox checks. Reading
	// the queue costs a directory listing plus a stat per item, which is cheap
	// occasionally and not cheap per event.
	paceEvery = 250

	// settleTimeout bounds how long the import waits for the queue to come down
	// before declaring delivery broken. Generous enough for a slow uplink to
	// clear a full high-water queue, short enough that somebody watching a
	// terminal learns the truth rather than watching a stalled counter.
	settleTimeout = 5 * time.Minute
	settlePoll    = 500 * time.Millisecond

	// progressEvery paces the progress lines. They are written as whole lines
	// rather than redrawn in place so that piping the output to a file, which is
	// what a person does when it goes wrong, keeps the history.
	progressEvery = 2 * time.Second
)

// ---------------------------------------------------------------- entry points

// runBackfill imports history the person asked for.
func runBackfill(args []string) error {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	since := fs.String("since", "", "how much to import: all, 30d, 720h, or a date like 2026-07-01")
	session := fs.String("session", "", "import one session by id, whole, whether or not it was sent before")
	dryRun := fs.Bool("dry-run", false, "report what would be imported without sending anything")
	if err := fs.Parse(args); err != nil {
		return err
	}

	p := config.Paths{}
	cfg, err := config.Load(p)
	if err != nil {
		return err
	}
	cutoff, err := parseSince(*since, time.Now())
	if err != nil {
		return err
	}

	// Ctrl-C stops the walk between events rather than killing the process
	// mid-write, so the journal on disk describes exactly what was queued.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = importHistory(ctx, p, cfg, request{Since: cutoff, Session: strings.TrimSpace(*session), DryRun: *dryRun, Out: os.Stdout})
	if errors.Is(err, context.Canceled) {
		stopped(os.Stdout)
		return nil
	}
	return err
}

// stopped is what an interrupted import says. Interruption is expected — this
// is a command somebody stops when they need their uplink back — so it reads as
// a pause rather than as a failure, and it names the way back.
func stopped(out io.Writer) {
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Stopped. Nothing already queued is lost, and nothing sent will be sent twice.")
	fmt.Fprintln(out, "Carry on where this left off with:  loop-sessions backfill")
}

// importAtInstall runs the bounded import `install` performs as its last step.
//
// It is a separate entry point rather than a flag on runBackfill because the two
// have different defaults and different voices: this one is one step of a setup
// somebody is watching, and it must never be the reason an otherwise finished
// install reports failure.
func importAtInstall(p config.Paths, cfg config.Config, since string, out io.Writer) error {
	cutoff, err := parseSince(since, time.Now())
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = importHistory(ctx, p, cfg, request{Since: cutoff, Out: out, Install: true})
	if errors.Is(err, context.Canceled) {
		stopped(out)
		return nil
	}
	return err
}

// rewalkForCaptureSchema re-imports this machine's whole history because the
// agent now extracts something the version that last walked it did not.
//
// This is the client half of the upgrade rule. An agent that learns to read a
// transcript better has to apply that to the transcripts it has already read, or
// the dashboard shows one interpretation for sessions captured before the
// upgrade and another for sessions captured after, with nothing on the page to
// say why.
//
// The journal is rotated first, and that is the whole trick: it records which
// files have been delivered, and a walk that consults it emits nothing at all
// for a corpus that has already been imported. Rotating rather than ignoring it
// keeps resume working — an interrupted re-walk picks up where it stopped
// instead of starting over.
//
// Re-sending is safe because event ids are derived from the record's identity
// rather than from when it was read, so the server recognises each event as one
// it already has and keeps whichever copy carries the higher capture version.
func rewalkForCaptureSchema(p config.Paths, cfg config.Config, from, to int, out io.Writer) error {
	journal := filepath.Join(p.Root(), journalName)
	// Renamed rather than removed: if the re-walk fails immediately, the record
	// of what had already been delivered is still on disk to look at.
	if err := os.Rename(journal, journal+".pre-v"+strconv.Itoa(to)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("backfill: cannot rotate the resume record: %w", err)
	}
	fmt.Fprintf(out, "Re-reading this machine's history: capture improved from v%d to v%d.\n", from, to)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	err := importHistory(ctx, p, cfg, request{Out: out})
	if errors.Is(err, context.Canceled) {
		// Not a failure and not finished. The version is only recorded by the
		// caller on success, so the next session tries again.
		stopped(out)
		return context.Canceled
	}
	return err
}

// request is one import.
type request struct {
	// Since drops everything older. Zero imports the whole corpus.
	Since time.Time
	// Session names one session to import whole, ignoring the window and the
	// resume record. It is the CTA the reader prints under a session whose
	// head was never imported, and what the repair pass uses.
	Session string
	// DryRun walks and reports without queueing or uploading anything, which is
	// how somebody on a metered connection finds out what this would cost.
	DryRun bool
	// Install marks the bounded run install performs, which words its output as
	// one step of a setup rather than as a command's whole output.
	Install bool
	Out     io.Writer
}

// ---------------------------------------------------------------- the import

func importHistory(ctx context.Context, p config.Paths, cfg config.Config, req request) error {
	if req.Out == nil {
		req.Out = io.Discard
	}
	// A paused agent is paused for history too. Importing into a machine
	// somebody has switched off would be the same betrayal as capturing on it.
	if cfg.Paused {
		return errors.New("backfill: capture is paused, so nothing is being imported; run `loop-sessions resume` first")
	}

	sources, others, err := historySources(p, cfg)
	if errors.Is(err, errNoHistory) {
		// A machine with nothing to import is not a machine with a problem, and
		// wording it as one during an install would teach the person that this
		// tool reports failures it does not have.
		fmt.Fprintln(req.Out)
		fmt.Fprintln(req.Out, "No supported sessions were found on this machine, so there is no history to import.")
		return nil
	}
	if err != nil {
		return err
	}

	im := &importer{paths: p, cfg: cfg, out: req.Out, now: time.Now, since: req.Since, session: req.Session, dryRun: req.DryRun, sessions: map[string]struct{}{}}
	if err := im.open(); err != nil {
		return err
	}
	defer im.close()

	im.announce(sources, others, req.Install)

	var result backfill.Result
	for _, source := range sources {
		res, walkErr := source.walk(backfill.Options{
			Root: source.root, Since: req.Since, Scrub: scrub.Func,
			Done: im.journal.delivered, OnSession: im.onFile, Session: req.Session,
		}, im.emit(ctx))
		result.Sessions += res.Sessions
		result.Events += res.Events
		result.Files += res.Files
		result.Bytes += res.Bytes
		result.Resumed += res.Resumed
		result.Skipped = append(result.Skipped, res.Skipped...)
		if walkErr != nil {
			logf("backfill: stopped after %d event(s): %v", im.imported, walkErr)
			return walkErr
		}
	}

	// The last queued events still have to reach the server. Returning before
	// they do would report an import that had not happened, which is the exact
	// shape of the failure this whole file exists to end.
	if err := im.settle(ctx, 0); err != nil {
		logf("backfill: %d event(s) imported but not all delivered: %v", im.imported, err)
		return err
	}

	im.summarize(result)
	logf("backfill: imported %d event(s) from %d file(s)", im.imported, result.Files-result.Resumed)
	return nil
}

// importer holds one run's state: where it is writing, what it has counted, and
// how loudly it has said so.
type importer struct {
	paths   config.Paths
	cfg     config.Config
	out     io.Writer
	now     func() time.Time
	since   time.Time
	session string
	dryRun  bool

	outbox  *spool.Spool
	sink    *pipeline.Sink
	flusher *spoolFlusher
	journal *journal

	imported  int
	excluded  int
	files     int
	sinceLast int
	lastPrint time.Time
	sessions  map[string]struct{}
}

func (im *importer) open() error {
	j, err := openJournal(filepath.Join(im.paths.Root(), journalName), im.dryRun)
	if err != nil {
		return err
	}
	im.journal = j

	if im.dryRun {
		return nil
	}

	sp, err := spool.Open(spool.Options{
		Dir:          im.paths.SpoolDir(),
		MaxBytes:     im.cfg.SpoolMaxBytes,
		MinFreeRatio: im.cfg.MinFreeRatio,
	})
	if err != nil {
		return fmt.Errorf("backfill: open the outbox: %w", err)
	}
	sink, err := pipeline.NewSink(sp)
	if err != nil {
		return err
	}
	// Delivery is built from the same composition root the daemon uses. A second
	// one here would be a second place for the wiring to be wrong, and being
	// wired in exactly one place is the property this codebase keeps losing.
	flusher, err := newDelivery(im.paths, im.cfg)
	if err != nil {
		return err
	}
	im.outbox, im.sink, im.flusher = sp, sink, flusher
	return nil
}

func (im *importer) close() {
	if err := im.journal.close(); err != nil {
		logf("backfill: could not close the resume record: %v", err)
	}
}

// emit is the walk's consumer: policy, then the outbox, then pacing.
func (im *importer) emit(ctx context.Context) func(event.Event) error {
	return func(e event.Event) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// The same exclusions that govern live capture govern history. Somebody
		// who told this tool to stay out of a directory did not mean "except for
		// everything that happened in it before you arrived".
		if ok, _ := im.cfg.ShouldCapture(e.Cwd); !ok {
			im.excluded++
			return nil
		}
		im.imported++
		im.sessions[e.SessionID] = struct{}{}
		if im.dryRun {
			return nil
		}
		// Through the sink, never straight to the spool: the sink is what carries
		// OccurredAt into the item's EventTime, and a spool item left without one
		// is stamped with now(). That single omission would date this entire
		// import to the moment it ran and destroy the only reason to run it.
		if err := im.sink.Put(e); err != nil {
			return err
		}
		im.sinceLast++
		if im.sinceLast >= paceEvery {
			im.sinceLast = 0
			return im.pace(ctx)
		}
		return nil
	}
}

// onFile records a file as delivered and paces the progress output.
func (im *importer) onFile(p backfill.Progress) {
	im.files++
	// Recorded once the file's events are in the outbox rather than once the
	// server has them. The outbox is durable, the import keeps it small enough
	// that it never has to drop anything, and anything still in it when this
	// process ends is delivered by the next session's daemon.
	if !p.Done && p.Err == nil {
		if err := im.journal.record(p.File); err != nil {
			// Losing the resume record costs a re-import, which the server
			// dedups. Losing the import over it would cost the history.
			logf("backfill: could not record %s as imported: %v", p.File, err)
		}
	}
	im.progress(false)
}

// ---------------------------------------------------------------- pacing

// pace keeps the outbox small, and stops the import when it cannot.
func (im *importer) pace(ctx context.Context) error {
	if im.flusher == nil {
		return nil
	}
	st, err := im.outbox.Stats()
	if err != nil {
		return fmt.Errorf("backfill: read the outbox: %w", err)
	}
	if st.Pending < outboxHighItems && st.Bytes < outboxHighBytes {
		return nil
	}
	return im.settle(ctx, outboxLowItems)
}

// settle delivers until the queue is down to target items, or reports why it is
// not going to be.
//
// target 0 means "everything", which is what the end of an import needs: the
// person is told the import finished, and that has to mean the server has it.
func (im *importer) settle(ctx context.Context, target int) error {
	if im.flusher == nil {
		return nil
	}
	// A byte floor as well as an item floor, for the same reason the high-water
	// marks come in pairs: a hundred items can be a hundred megabytes. Draining
	// to nothing means exactly that, bytes included.
	targetBytes := int64(outboxLowBytes)
	if target == 0 {
		targetBytes = 0
	}

	deadline := im.now().Add(settleTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		st, err := im.outbox.Stats()
		if err != nil {
			return fmt.Errorf("backfill: read the outbox: %w", err)
		}
		if st.Pending <= target && st.Bytes <= targetBytes {
			return nil
		}
		if im.now().After(deadline) {
			return fmt.Errorf("backfill: %s item(s) have been waiting %s to upload and the queue is not moving; "+
				"stopping the import rather than filling a queue that drops its oldest items. "+
				"Nothing already queued is lost — run `loop-sessions backfill` again once uploads work",
				humanCount(st.Pending), settleTimeout)
		}

		n, err := im.flusher.RunOnce(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("backfill: nothing is uploading, so the import stopped with %s item(s) still queued: %w",
				humanCount(st.Pending), err)
		}
		if n == 0 {
			// Either this machine's session daemon holds the delivery lock and is
			// draining the same queue, or the server accepted nothing this cycle.
			// Both resolve by waiting; the deadline above is what keeps "waiting"
			// from becoming "forever".
			if err := sleepFor(ctx, settlePoll); err != nil {
				return err
			}
		}
	}
}

func sleepFor(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ---------------------------------------------------------------- what it says

func (im *importer) announce(sources []historySource, others []string, install bool) {
	if install {
		fmt.Fprintln(im.out)
		// Not "your recent history": the default window is the whole corpus, and
		// the scope lines immediately below say which it actually is.
		fmt.Fprintln(im.out, "Importing your local agent history")
	} else {
		fmt.Fprintln(im.out, "Importing local agent history")
	}
	for _, source := range sources {
		fmt.Fprintf(im.out, "  %s: %s session(s), %s on disk from %s\n", source.tool, humanCount(source.finding.Sessions), humanBytes(source.finding.Bytes), source.root)
	}
	switch {
	case im.session != "":
		fmt.Fprintf(im.out, "  only session %s, whole, whether or not it was sent before\n", im.session)
	case im.since.IsZero():
		fmt.Fprintln(im.out, "  everything, however old")
	default:
		fmt.Fprintf(im.out, "  sessions since %s\n", im.since.Format("2 Jan 2006"))
	}
	if n := im.journal.count(); n > 0 {
		fmt.Fprintf(im.out, "  %s file(s) an earlier run already sent; those are skipped\n", humanCount(n))
	}
	if im.dryRun {
		fmt.Fprintln(im.out, "  dry run: nothing will be queued or uploaded")
	} else {
		fmt.Fprintln(im.out, "  uploading as it goes; Ctrl-C stops it and `loop-sessions backfill` resumes it")
	}
	for _, o := range others {
		// A declared gap. Silently importing nothing for a tool somebody uses
		// daily is how a fleet ends up believing it has coverage it does not.
		fmt.Fprintf(im.out, "  note: %s sessions were found but cannot be imported yet\n", o)
	}
	fmt.Fprintln(im.out)
}

// progress prints at most one line per progressEvery, plus a final one.
func (im *importer) progress(final bool) {
	now := im.now()
	if !final && now.Sub(im.lastPrint) < progressEvery {
		return
	}
	im.lastPrint = now

	line := fmt.Sprintf("  %s history files read", humanCount(im.files))
	line += fmt.Sprintf("   %s events", humanCount(im.imported))
	if im.outbox != nil {
		if st, err := im.outbox.Stats(); err == nil {
			line += fmt.Sprintf("   %s waiting to upload", humanCount(st.Pending))
		}
	}
	fmt.Fprintln(im.out, line)
}

func (im *importer) summarize(res backfill.Result) {
	im.progress(true)
	fmt.Fprintln(im.out)

	switch {
	case im.dryRun:
		fmt.Fprintf(im.out, "Would import %s event(s) across %s session(s) from %s history file(s).\n",
			humanCount(im.imported), humanCount(len(im.sessions)), humanCount(res.Files-res.Resumed))
	case im.imported == 0 && res.Resumed > 0:
		fmt.Fprintln(im.out, "Nothing new to import; every history file in this window had already been sent.")
	case im.imported == 0:
		fmt.Fprintln(im.out, "Nothing to import: no sessions were found in this window.")
	default:
		fmt.Fprintf(im.out, "Imported %s event(s) from %s session(s). All of it reached the server.\n",
			humanCount(im.imported), humanCount(len(im.sessions)))
	}
	if im.excluded > 0 {
		fmt.Fprintf(im.out, "%s event(s) were left out because their project is excluded from capture.\n", humanCount(im.excluded))
	}
	if n := skippedFileCount(res.Skipped); n > 0 {
		// Advisories are a count plus a pointer, not a wall of text: on a real
		// corpus this list is hundreds of lines and would bury the result.
		fmt.Fprintf(im.out, "%s file(s) had something odd about them (empty, malformed, or a foreign schema).\n", humanCount(n))
	}
	if im.since.IsZero() || im.session != "" {
		return
	}
	fmt.Fprintln(im.out, "Older sessions were not imported. Import everything with:  loop-sessions backfill --since all")
}

func skippedFileCount(skipped []backfill.Skip) int {
	paths := make(map[string]struct{}, len(skipped))
	for _, skip := range skipped {
		paths[skip.Path] = struct{}{}
	}
	return len(paths)
}

// ---------------------------------------------------------------- where history is

// errNoHistory means the machine was scanned and has no supported sessions.
// That is an ordinary state — a new laptop, a person who uses something else —
// and not a failure to report as one.
var errNoHistory = errors.New("backfill: no supported sessions were found on this machine")

type historySource struct {
	tool    discovery.Tool
	root    string
	finding discovery.Finding
	walk    historyWalk
}

type historyWalk func(backfill.Options, func(event.Event) error) (backfill.Result, error)

var historyWalkers = map[discovery.Tool]historyWalk{
	discovery.ClaudeCode: backfill.Walk,
	discovery.Codex:      backfill.WalkCodex,
}

// historySources reports the supported session stores on this machine, and what
// else was found that cannot be imported yet.
//
// It reads what discovery already wrote rather than probing again. Discovery is
// the component that knows how to find session stores on a machine that has been
// customised, and a second implementation of "where do transcripts live" would
// drift from it — which is how a person with CLAUDE_CONFIG_DIR set ends up
// silently importing nothing.
func historySources(p config.Paths, cfg config.Config) (sources []historySource, others []string, err error) {
	scanned := false
	if sum, err := readDiscovery(p.DiscoveryFile()); err == nil {
		scanned = true
		for _, f := range sum.Findings {
			if f.State != discovery.Found || f.Skipped {
				continue
			}
			if walk := historyWalkers[f.Tool]; walk != nil {
				sources = append(sources, historySource{tool: f.Tool, root: f.Root, finding: f, walk: walk})
				continue
			}
			others = append(others, string(f.Tool))
		}
	}
	if len(sources) == 0 {
		// The config records the same roots install settled on, so a machine
		// whose discovery.json predates this feature still imports.
		for _, tool := range []discovery.Tool{discovery.ClaudeCode, discovery.Codex} {
			if root := cfg.Roots[string(tool)]; root != "" {
				sources = append(sources, historySource{tool: tool, root: root, walk: historyWalkers[tool]})
			}
		}
	}
	if len(sources) == 0 && scanned {
		// The scan ran and there was nothing. Distinguished from the case below
		// because the two need opposite answers: this one is a machine with no
		// history, that one is a machine nobody has looked at yet.
		return nil, nil, errNoHistory
	}
	if len(sources) == 0 {
		return nil, nil, fmt.Errorf("backfill: I do not know where this machine keeps its sessions yet; "+
			"run `loop-sessions discover` and then `loop-sessions backfill` (looked in %s)", p.DiscoveryFile())
	}
	for _, source := range sources {
		if fi, statErr := os.Stat(source.root); statErr != nil || !fi.IsDir() {
			return nil, nil, fmt.Errorf("backfill: %s is where this machine's sessions were found and it is not there now; "+
				"run `loop-sessions discover` to look again", source.root)
		}
	}
	return sources, others, nil
}

func readDiscovery(path string) (discovery.Summary, error) {
	var s discovery.Summary
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("backfill: %s is unreadable: %w", path, err)
	}
	return s, nil
}

// ---------------------------------------------------------------- resume record

// journal is the list of files an earlier import already queued.
type journal struct {
	path string
	done map[string]journalEntry
	f    *os.File
}

// journalEntry identifies a file by content rather than by name. A transcript
// grows while its session runs, so a file imported yesterday and appended to
// today is not done: re-walking it re-emits what was already sent under the same
// ids, which the server dedups, and picks up what is new.
type journalEntry struct {
	Path  string    `json:"path"`
	Size  int64     `json:"size"`
	ModNS int64     `json:"mtime_ns"`
	At    time.Time `json:"at"`
}

func openJournal(path string, readOnly bool) (*journal, error) {
	j := &journal{path: path, done: map[string]journalEntry{}}

	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("backfill: read the resume record %s: %w", path, err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var e journalEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			// A torn last line is what a crash mid-append leaves. Dropping it
			// costs one file being imported twice and dedup'd server-side;
			// refusing to start over it would cost the whole resume.
			continue
		}
		j.done[e.Path] = e
	}
	if readOnly {
		return j, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("backfill: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("backfill: open the resume record %s: %w", path, err)
	}
	j.f = f
	return j, nil
}

// delivered answers backfill's Options.Done.
func (j *journal) delivered(path string) bool {
	e, ok := j.done[path]
	if !ok {
		return false
	}
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Size() == e.Size && fi.ModTime().UnixNano() == e.ModNS
}

func (j *journal) record(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	e := journalEntry{Path: path, Size: fi.Size(), ModNS: fi.ModTime().UnixNano(), At: time.Now().UTC()}
	j.done[path] = e
	if j.f == nil {
		return nil
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = j.f.Write(append(b, '\n'))
	return err
}

func (j *journal) count() int { return len(j.done) }

func (j *journal) close() error {
	if j == nil || j.f == nil {
		return nil
	}
	return j.f.Close()
}

// ---------------------------------------------------------------- helpers

// parseSince turns the flag into an instant.
//
// It takes four forms because the people who run this write all four: "all",
// "30d" because that is how anybody says a month, "720h" because that is what Go
// parses, and a date because "since I joined" is a real question.
func parseSince(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	switch strings.ToLower(s) {
	case "", "all", "everything":
		return time.Time{}, nil
	}
	if days, ok := strings.CutSuffix(strings.ToLower(s), "d"); ok {
		if n, err := strconv.Atoi(days); err == nil && n > 0 {
			return now.AddDate(0, 0, -n), nil
		}
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return now.Add(-d), nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("backfill: cannot read %q as an amount of history; "+
		"use all, 30d, 720h, or a date like 2026-07-01", s)
}

// humanCount groups digits, because six thousand and six hundred thousand are
// indistinguishable at a glance without it and this output is read at a glance.
func humanCount(n int) string {
	s := strconv.Itoa(n)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
