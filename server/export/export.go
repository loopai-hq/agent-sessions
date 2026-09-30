// Package export copies the derived tables out of the production Postgres
// into Cloud Storage as gzip JSONL and loads them into BigQuery, one run per
// hour, as the Cloud Run job loop-sessions-export (the same binary as the
// server, subcommand "export").
//
// Why a copy at all: the primary is a 2-vCPU instance whose events table is
// 27 GB of mostly TOASTed JSON; an analyst's whole-corpus query on it times
// out at two minutes and competes with ingest while it runs, and a psql role
// for analysts would see every colleague's transcript (research/r2, F3). The
// copy is bounded, partitioned, row-filtered per viewer, and queryable with
// SQL, DuckDB or jq without touching the primary.
//
// The shape of a run, and why:
//
//   - Everything is keyed on a watermark that lives in the database
//     (export_watermarks, one row per stream). A run reads the watermarks,
//     computes which partitions changed since, rewrites those partitions
//     whole, loads each with WRITE_TRUNCATE into its BigQuery partition, and
//     advances the watermarks only when every load succeeded. A failed run
//     therefore repeats its work next hour instead of leaving a partition
//     half old and half new, and repeating is safe because a partition is
//     always written whole.
//
//   - The session-keyed tables (turns, sessions, messages) are partitioned by
//     the SESSION's start day, not the row's own time. A session's turns span
//     midnight, and a turn is re-folded hours or days after the session
//     started when a later batch supersedes a hook copy; with per-row
//     partitions, "re-export what changed" would either truncate unchanged
//     turns out of an old partition or append duplicates into it (ops review
//     F8). With the session's day as the key, "the partitions touched by a
//     changed session" is exactly one partition per session, and rewriting it
//     whole is both complete and idempotent.
//
//   - events are partitioned by ingested_at day, which never changes for a
//     stored row, and rewritten per day; events_latest in BigQuery dedupes
//     on (id, capture_version) as a belt-and-braces view.
//
//   - A run is bounded (Options.MaxSessions, MaxSessionDays, MaxEventDays) so the
//     first run over the whole corpus, or a run after a long outage, finishes
//     inside the job's 30-minute task timeout and advances the watermark to
//     where it stopped rather than never advancing at all. The alert on
//     export lag fires during such a catch-up; that is the truth.
//
//   - Nothing here imports a Google client library. The GCS upload and the
//     BigQuery load API are two REST calls each, and the module has no cloud
//     dependency today (go.mod: pgx and goldmark); adding the SDKs would pull
//     in gRPC and protobuf for the server image too, for two endpoints. See
//     gcs.go and bigquery.go.
//
// No line this package logs carries a session's content: ids, counts, day
// names and error strings only, matching the server's log contract.
package export

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// Table names an exported table. The value is both the Postgres projection
// the source copies and the BigQuery table the file is loaded into, and the
// first path segment of the object in the bucket.
type Table string

const (
	TableTurns        Table = "turns"
	TableSessions     Table = "sessions"
	TableMessages     Table = "messages"
	TableEvents       Table = "events"
	TableHealthHourly Table = "health_hourly"
)

// sessionTables are the tables partitioned by the session's start day and
// re-exported together for every touched day.
var sessionTables = []Table{TableTurns, TableSessions, TableMessages}

// Day is a UTC calendar day in 2006-01-02 form: the partition unit on both
// sides. UTC because BigQuery's DATE(timestamp) partitioning is UTC, and a
// partition computed in any other zone would be loaded into the wrong
// decorator for the hours around midnight.
type Day string

// DayOf is the UTC day t falls in.
func DayOf(t time.Time) Day { return Day(t.UTC().Format("2006-01-02")) }

// Start is midnight UTC at the start of the day.
func (d Day) Start() time.Time {
	t, _ := time.Parse("2006-01-02", string(d))
	return t
}

// Next is the following day.
func (d Day) Next() Day { return DayOf(d.Start().Add(24 * time.Hour)) }

// Decorator is the day in BigQuery's partition-decorator form (YYYYMMDD).
func (d Day) Decorator() string { return strings.ReplaceAll(string(d), "-", "") }

// Valid reports whether d is a real date in the form DayOf produces.
func (d Day) Valid() bool {
	t, err := time.Parse("2006-01-02", string(d))
	return err == nil && DayOf(t) == d
}

// Watermarks are the positions the three streams were exported up to. A
// zero time means never exported, which is "from the beginning".
type Watermarks struct {
	// Sessions is sessions.updated_at; it covers turns, sessions and
	// messages, which are exported by the session's start day.
	Sessions time.Time
	// Events is events.ingested_at.
	Events time.Time
	// Health is health_hourly.hour.
	Health time.Time
}

// TouchedSession is one session whose row changed since the watermark.
type TouchedSession struct {
	SessionID string
	Email     string
	StartedAt time.Time
	UpdatedAt time.Time
}

// PlanRequest is what a run asks the source before it writes anything. The
// limits are one more than the run will take, so it can tell a cut set from
// a complete one.
type PlanRequest struct {
	// SessionsSince and SessionsLimit select sessions with updated_at >
	// since, ordered by (updated_at, session_id).
	SessionsSince time.Time
	SessionsLimit int
	// EventsSince and EventsLimit select the distinct UTC days of events
	// with ingested_at > since, ascending.
	EventsSince time.Time
	EventsLimit int
	// HealthSince selects the distinct UTC days of health_hourly rows with
	// hour > since, ascending.
	HealthSince time.Time
}

// PlanReads is the source's answer to a PlanRequest.
type PlanReads struct {
	Sessions   []TouchedSession
	EventDays  []Day
	HealthDays []Day
	// HealthTableExists is false while health_hourly does not exist yet (it
	// arrives with PR E). The run then leaves the health watermark where it
	// is: advancing it would put the rollups PR E writes for the retained
	// week behind a position they were never exported from.
	HealthTableExists bool
}

// Source is the Postgres side of a run. The store implements it
// (server/store/export_reads.go); the tests fake it.
type Source interface {
	// Now is the database's clock, which is the one the watermarks and
	// updated_at are stamped from. The job's own clock is not used for the
	// watermark, because two clocks a few seconds apart would either skip
	// rows or re-export them, and only the database's is consistent with
	// the timestamps it compares against.
	Now(ctx context.Context) (time.Time, error)
	// Watermarks reads the current positions; a missing row is zero.
	Watermarks(ctx context.Context) (Watermarks, error)
	// Plan issues every read that decides what the run does, together, in
	// one read-only transaction under the export ceiling (the store's
	// ExportPlan says why the ceiling matters: the first run's events-day
	// read is a scan of the whole table).
	Plan(ctx context.Context, req PlanRequest) (PlanReads, error)
	// CopyPartition writes one table's rows for one day to w as JSON lines
	// and reports how many.
	CopyPartition(ctx context.Context, table Table, day Day, w io.Writer) (rows int64, err error)
	// CopyBundle writes one session's bundle (session line, turn lines,
	// event lines, raw excluded) to w as JSON lines.
	CopyBundle(ctx context.Context, sessionID string, w io.Writer) (rows int64, err error)
	// AdvanceWatermarks records the positions a successful run reached.
	AdvanceWatermarks(ctx context.Context, w Watermarks) error
}

// ObjectStore is the bucket. Put streams one object: write is called once
// with the writer the body goes to, so a partition never has to be held in
// memory, and an error from write is the error Put returns.
type ObjectStore interface {
	Put(ctx context.Context, object string, write func(w io.Writer) error) (bytes int64, err error)
}

// LoadJob is one BigQuery load: the object just written, into one partition.
type LoadJob struct {
	// ID is the job's own id, deterministic per run and partition so a
	// retried insert is recognised as the same job rather than started twice.
	ID     string
	Table  Table
	Day    Day
	Object string
}

// Loader starts load jobs and waits for them. Start and Wait are separate
// so a run can have every partition's load in flight before it waits for
// any; BigQuery runs them in parallel and a run that waited on each in turn
// would spend most of its budget idle.
type Loader interface {
	Start(ctx context.Context, job LoadJob) error
	Wait(ctx context.Context, job LoadJob) error
}

// Options bound and name a run.
type Options struct {
	// MaxSessions caps how many touched sessions one run takes (their
	// partitions, and their bundles). Zero takes DefaultMaxSessions.
	MaxSessions int
	// MaxSessionDays caps how many distinct session start days one run
	// rewrites (three partition files each). Zero takes
	// DefaultMaxSessionDays.
	MaxSessionDays int
	// MaxEventDays caps how many event days one run rewrites. Zero takes
	// DefaultMaxEventDays.
	MaxEventDays int
	// Margin is how far behind the database clock the watermark is set when
	// a run took everything: a row committed by a transaction that began
	// before the run read the clock carries an updated_at earlier than that
	// clock, and would be skipped by a watermark set at the clock exactly.
	// Zero takes DefaultMargin.
	Margin time.Duration
	// EventCap is the byte cap on each large text field of an event in a
	// bundle. Zero takes DefaultEventCap.
	EventCap int
	// BundleWorkers is how many bundles are copied concurrently. Bounded by
	// the job's pool of two connections; zero takes DefaultBundleWorkers.
	BundleWorkers int
	// RunID names the run in load job ids; empty takes the start time.
	RunID string
}

// The defaults, and the arithmetic behind them.
const (
	// A session's bundle is one COPY and one upload, tens of milliseconds
	// each on a small session; two thousand is a few minutes of a run, and
	// the corpus of about 12,700 sessions is exported in seven runs.
	DefaultMaxSessions = 2000
	// A session day is three partition files (turns, sessions, messages),
	// each a COPY, an upload and a load; forty days is 120 files, a few
	// minutes, and a production history of some 260 start days is exported
	// in seven runs. The sessions of one hour normally fall on one
	// or two days, so the cap is felt only during a catch-up.
	DefaultMaxSessionDays = 40
	// An event day is about 125,000 rows whose bodies have to be detoasted;
	// the clone rehearsal (design.md section 11) read about 1,700 rows/s, so
	// a day is about a minute and six days are well inside the budget with
	// the session partitions beside them.
	DefaultMaxEventDays = 6
	// Longer than any ingest transaction, which is what the margin has to
	// cover. Every partition inside the margin is rewritten again next hour,
	// which is cheap and idempotent.
	DefaultMargin = 5 * time.Minute
	// Sixteen KiB per large field keeps a bundle in the low megabytes for
	// LLM batch input; the full text stays in events.
	DefaultEventCap = 16 * 1024
	// The job's pool has two connections (server/app/config.go, the
	// connection budget); a third worker would only queue.
	DefaultBundleWorkers = 2
	// RunBudget is the wall clock a run may take, below the Cloud Run task
	// timeout of 30 minutes (deploy/analytics/job.yaml) so the run ends with
	// its own log line rather than a SIGKILL that writes nothing.
	RunBudget = 28 * time.Minute
	// partitionAttempts is how many times one partition's copy-and-upload
	// is tried before the run gives up on it. The second attempt repeats the
	// COPY; the failure that motivates it is a dropped upload, not the query.
	partitionAttempts = 2
)

func (o Options) withDefaults() Options {
	if o.MaxSessions <= 0 {
		o.MaxSessions = DefaultMaxSessions
	}
	if o.MaxSessionDays <= 0 {
		o.MaxSessionDays = DefaultMaxSessionDays
	}
	if o.MaxEventDays <= 0 {
		o.MaxEventDays = DefaultMaxEventDays
	}
	if o.Margin <= 0 {
		o.Margin = DefaultMargin
	}
	if o.EventCap <= 0 {
		o.EventCap = DefaultEventCap
	}
	if o.BundleWorkers <= 0 {
		o.BundleWorkers = DefaultBundleWorkers
	}
	return o
}

// Result is what a run did, and what the "export run" line reports.
type Result struct {
	// Partitions is how many (table, day) files were written and loaded.
	Partitions int
	// Bundles is how many session bundles were rewritten.
	Bundles int
	// Rows counts the JSON lines written across partitions and bundles.
	Rows int64
	// Bytes counts compressed bytes uploaded.
	Bytes int64
	// Seconds is the run's wall clock.
	Seconds float64
	// LagSeconds is how far behind the database clock the oldest watermark
	// in force after the run is: the new positions when the run advanced
	// them, the old ones when it did not, however early it failed. It is set
	// from the old positions the moment they are read, so a run that dies in
	// its planning read still reports the true lag rather than zero, which
	// the lag alert would read as "caught up" (review-1 I1). Zero only on a
	// first run with no watermark to measure against (Bootstrap says so).
	LagSeconds float64
	// Bootstrap is set when no stream had a watermark before the run.
	Bootstrap bool
	// Capped is set when a cap cut the run short; the next run continues.
	Capped bool
	// FailedLoads is how many load jobs ended in error.
	FailedLoads int
	// Status is "ok", "failed" or "timeout".
	Status string
	// Watermarks are the positions in force after the run.
	Watermarks Watermarks
}

// Run performs one export. It returns the result together with the error
// that stopped it, if any; the result is filled in either way so the caller
// can log it.
func Run(ctx context.Context, src Source, objs ObjectStore, loader Loader, opt Options, log *slog.Logger) (Result, error) {
	opt = opt.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	started := time.Now()
	var res Result
	res.Status = "failed"
	defer func() { res.Seconds = time.Since(started).Seconds() }()

	now, err := src.Now(ctx)
	if err != nil {
		return res, fmt.Errorf("export: read the database clock: %w", err)
	}
	old, err := src.Watermarks(ctx)
	if err != nil {
		return res, fmt.Errorf("export: read watermarks: %w", err)
	}
	res.Watermarks = old
	res.Bootstrap = old.Sessions.IsZero() && old.Events.IsZero() && old.Health.IsZero()
	// The lag the line reports if nothing below succeeds: against the old
	// positions, which stay in force. Overwritten only when the run advanced
	// them.
	res.LagSeconds = lag(now, old)
	if opt.RunID == "" {
		opt.RunID = now.UTC().Format("20060102t150405")
	}
	floor := now.Add(-opt.Margin)

	// Plan. Every read that decides what the run does happens before any
	// write, in one transaction under the export ceiling, so the set of
	// partitions is fixed and the watermark that will be recorded is known
	// up front; a run never discovers half-way that it should have taken
	// more. The rollup for an hour is rewritten as late reports for it land,
	// so the trailing health day is always taken again; a day of health rows
	// is a few hundred lines.
	reads, err := src.Plan(ctx, PlanRequest{
		SessionsSince: old.Sessions, SessionsLimit: opt.MaxSessions + 1,
		EventsSince: old.Events, EventsLimit: opt.MaxEventDays + 1,
		HealthSince: old.Health.Add(-24 * time.Hour),
	})
	if err != nil {
		return res, timeoutOr(ctx, &res, fmt.Errorf("export: plan: %w", err))
	}
	sessions := planSessions(reads.Sessions, opt.MaxSessions, opt.MaxSessionDays, old.Sessions, floor)
	if sessions.tieSplit > 0 {
		// See planSessions. Loud, because the sessions left behind are
		// exported only when something touches them again.
		log.Warn("export tie split", "sessions_left", sessions.tieSplit, "updated_at", sessions.watermark)
	}
	events := planDays(reads.EventDays, opt.MaxEventDays, old.Events, floor)
	next := Watermarks{
		Sessions: sessions.watermark,
		Events:   events.watermark,
		// No table, no position: the health stream starts where PR E's
		// first rollups do, not at a clock floor they are already behind.
		Health: old.Health,
	}
	if reads.HealthTableExists {
		next.Health = laterOf(old.Health, floor)
	}
	healthDays := reads.HealthDays
	res.Capped = sessions.capped || events.capped

	var partitions []LoadJob
	for _, day := range sessions.days {
		for _, t := range sessionTables {
			partitions = append(partitions, LoadJob{ID: jobID(opt.RunID, t, day), Table: t, Day: day, Object: partitionObject(t, day)})
		}
	}
	for _, day := range events.days {
		partitions = append(partitions, LoadJob{ID: jobID(opt.RunID, TableEvents, day), Table: TableEvents, Day: day, Object: partitionObject(TableEvents, day)})
	}
	for _, day := range healthDays {
		partitions = append(partitions, LoadJob{ID: jobID(opt.RunID, TableHealthHourly, day), Table: TableHealthHourly, Day: day, Object: partitionObject(TableHealthHourly, day)})
	}

	// Partitions: copy, upload, start the load, move on. The loads run in
	// BigQuery while the next partition is being copied.
	var inFlight []LoadJob
	for _, p := range partitions {
		rows, bytes, err := exportPartition(ctx, src, objs, p)
		if err != nil {
			return res, timeoutOr(ctx, &res, fmt.Errorf("export: partition %s/%s: %w", p.Table, p.Day, err))
		}
		if err := loader.Start(ctx, p); err != nil {
			return res, timeoutOr(ctx, &res, fmt.Errorf("export: start load %s: %w", p.ID, err))
		}
		inFlight = append(inFlight, p)
		res.Partitions++
		res.Rows += rows
		res.Bytes += bytes
		log.Info("export partition", "table", string(p.Table), "day", string(p.Day), "rows", rows, "bytes", bytes, "job", p.ID)
	}

	// Bundles, a bounded number at a time. A bundle that cannot be written
	// fails the run: its session is inside the watermark the run would
	// record, and a bundle missing from the bucket with no run to redo it
	// would be missing for good.
	bundleRows, bundleBytes, err := exportBundles(ctx, src, objs, sessions.included, opt)
	res.Rows += bundleRows
	res.Bytes += bundleBytes
	if err != nil {
		return res, timeoutOr(ctx, &res, err)
	}
	res.Bundles = len(sessions.included)

	// Loads. Every failure is logged on its own line so the metric counts
	// each, and the run fails as a whole: a watermark advanced past a
	// partition BigQuery does not hold would hide the gap for good.
	for _, p := range inFlight {
		if err := loader.Wait(ctx, p); err != nil {
			res.FailedLoads++
			log.Error("export load failed", "table", string(p.Table), "day", string(p.Day), "job", p.ID, "error", err.Error())
		}
	}
	if res.FailedLoads > 0 {
		return res, timeoutOr(ctx, &res, fmt.Errorf("export: %d of %d load jobs failed", res.FailedLoads, len(inFlight)))
	}

	if err := src.AdvanceWatermarks(ctx, next); err != nil {
		return res, fmt.Errorf("export: advance watermarks: %w", err)
	}
	res.Watermarks = next
	res.LagSeconds = lag(now, next)
	res.Status = "ok"
	return res, nil
}

// timeoutOr names a failure that was really the run budget expiring, so the
// line reads "timeout" rather than whatever the cancelled call reported.
func timeoutOr(ctx context.Context, res *Result, err error) error {
	if ctx.Err() != nil {
		res.Status = "timeout"
		return fmt.Errorf("export: run budget exhausted: %w", err)
	}
	return err
}

// exportPartition copies one (table, day) to its object and reports the rows
// and compressed bytes. A failed attempt is retried once from the COPY; see
// partitionAttempts.
func exportPartition(ctx context.Context, src Source, objs ObjectStore, p LoadJob) (rows, bytes int64, err error) {
	for attempt := 1; ; attempt++ {
		rows = 0
		bytes, err = objs.Put(ctx, p.Object, func(w io.Writer) error {
			gz := gzip.NewWriter(w)
			n, err := src.CopyPartition(ctx, p.Table, p.Day, gz)
			rows = n
			if err != nil {
				return err
			}
			return gz.Close()
		})
		if err == nil || attempt >= partitionAttempts || ctx.Err() != nil {
			return rows, bytes, err
		}
	}
}

// exportBundles rewrites one bundle per session with a bounded number of
// workers, stopping at the first failure.
func exportBundles(ctx context.Context, src Source, objs ObjectStore, sessions []TouchedSession, opt Options) (rows, bytes int64, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	work := make(chan TouchedSession)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	fail := func(e error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = e
			cancel()
		}
		mu.Unlock()
	}
	for range opt.BundleWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range work {
				var n int64
				b, err := objs.Put(ctx, BundleObject(s.Email, s.SessionID), func(w io.Writer) error {
					gz := gzip.NewWriter(w)
					capped := newCapWriter(gz, opt.EventCap)
					var err error
					n, err = src.CopyBundle(ctx, s.SessionID, capped)
					if err != nil {
						return err
					}
					if err := capped.Flush(); err != nil {
						return err
					}
					return gz.Close()
				})
				if err != nil {
					fail(fmt.Errorf("export: bundle %s: %w", s.SessionID, err))
					continue
				}
				mu.Lock()
				rows += n
				bytes += b
				mu.Unlock()
			}
		}()
	}
	for _, s := range sessions {
		if ctx.Err() != nil {
			break
		}
		select {
		case work <- s:
		case <-ctx.Done():
		}
	}
	close(work)
	wg.Wait()
	return rows, bytes, firstErr
}

// partitionObject is the object a (table, day) is written to. The dt= form
// is what DuckDB and Hive-style readers take as a partition column.
func partitionObject(t Table, d Day) string {
	return fmt.Sprintf("%s/dt=%s/part-0000.jsonl.gz", t, d)
}

// BundleObject is the object a session's bundle is written to.
func BundleObject(email, sessionID string) string {
	return fmt.Sprintf("bundles/%s/%s.jsonl.gz", email, sessionID)
}

// jobID is the BigQuery job id for one partition of one run: letters,
// digits, underscores and dashes only, which is all the API allows.
func jobID(runID string, t Table, d Day) string {
	return fmt.Sprintf("loop_sessions_export_%s_%s_%s", safeID(runID), t, d.Decorator())
}

func safeID(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// lag is the age of the oldest watermark in force against the database
// clock; zero when no stream has one yet.
func lag(now time.Time, w Watermarks) float64 {
	var oldest time.Time
	for _, t := range []time.Time{w.Sessions, w.Events, w.Health} {
		if t.IsZero() {
			continue
		}
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return now.Sub(oldest).Seconds()
}

func laterOf(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// ErrNoConnection is returned by a Source that cannot pin a database
// connection for COPY, which the fakes in this package's own tests cannot
// and a pool can.
var ErrNoConnection = errors.New("export: the source cannot pin a connection for COPY")

// Tables lists every table the run exports, in a fixed order, for the
// tests that check the DDL and the metrics name each of them.
func Tables() []Table {
	out := append([]Table{}, sessionTables...)
	out = append(out, TableEvents, TableHealthHourly)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
