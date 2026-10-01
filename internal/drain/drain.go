// Package drain moves items from the spool to the server.
//
// It is the only component that touches the network, which is why the rest of
// the client can be synchronous and fast: a hook writes a file and returns, and
// everything about connectivity, retries and backpressure lives here.
//
// Three rules shape the implementation.
//
// Acknowledge only what the server said it stored. An item that is not in the
// accepted set stays pending. Acking optimistically — on a 200, on a partial
// response, on anything other than an explicit per-item acceptance — is the
// mechanism by which clients lose data the server never wrote.
//
// Never give up on a transient failure. A laptop offline for a week must still
// deliver when it reconnects, so transport errors retry forever. Nor is
// silence a verdict: an item the server names in neither list stays pending,
// and after eight such deliveries it is parked to be retried after the next
// upgrade and daily, never quarantined. Only an explicit rejection is
// permanent, and it keeps the server's reason. Nine machines carried
// quarantines that turned out to be eight silent 200s counted as eight
// failures; nothing on the server had ever refused the items.
//
// Back off with full jitter. Fifty laptops recovering from the same outage will
// otherwise synchronise into a thundering herd and knock the server over again
// the moment it comes back.
package drain

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/spool"
)

// Transport delivers a batch and reports what the server did with it.
//
// It is declared here, by the consumer, rather than imported from a client
// package: authentication is still undecided, and a drain that depended on a
// concrete client would have to change when that decision lands. Everything the
// drain needs from the network is these three lines.
type Transport interface {
	Send(ctx context.Context, batch []spool.Leased) (Response, error)
}

// Response is the server's per-item verdict.
//
// It is deliberately not a single status code. A batch endpoint that could only
// say "200" would force the client to choose between acking items the server
// dropped and re-sending items it already has; per-item accounting is what makes
// at-least-once delivery converge.
type Response struct {
	// Accepted lists item IDs the server durably stored. Only these are removed
	// from the spool.
	Accepted []string
	// Rejected lists items that are permanently unacceptable — a schema
	// violation, an oversized payload. Retrying these can never succeed, so they
	// go straight to quarantine.
	Rejected []Reject
	// RetryAfter carries the server's own pacing instruction, as sent on 429 and
	// 503. Zero means use the computed backoff.
	RetryAfter time.Duration
}

// Reject is one permanently undeliverable item and the server's reason.
type Reject struct {
	ID     string
	Reason string
}

// Options configure a Drain.
type Options struct {
	Spool     *spool.Spool
	Transport Transport
	// Enrich, when set, sees each batch after it is leased and before it is
	// sent, and may rewrite an item's payload in place. The daemon uses it
	// to anchor a Stop copy that the hook spooled before the harness had
	// flushed the turn's answer (cmd/loop-sessions, stopAnchorer). It works
	// on the in-memory copy: a retry after a failed send leases the file
	// again and enriches it again. It runs before limitBytes, so what it
	// adds counts against MaxBytes like any other byte of the payload.
	Enrich func(ctx context.Context, batch []spool.Leased)

	// BatchSize caps items per request. Default 100.
	BatchSize int
	// MaxBytes caps the encoded size of one request. Default 4 MiB. A batch is
	// limited by whichever of the two binds first.
	MaxBytes int
	// Interval is the idle poll period, used when the spool was empty. Default
	// 5s. A cycle that filled its batch does not wait, so a large backlog drains
	// at network speed rather than one batch per interval.
	Interval time.Duration

	Backoff BackoffPolicy
	// OnCycle, if set, is called with a Stats snapshot after every cycle.
	OnCycle func(Stats)
	// Now is injectable for tests.
	Now func() time.Time
}

// BackoffPolicy describes the retry curve.
type BackoffPolicy struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	// Jitter selects full jitter: sleep is uniform over [0, ceiling) rather than
	// the ceiling itself. Randomising the whole interval, not a fraction of it,
	// is what actually decorrelates a fleet; it is also why tests that assert on
	// the curve set this false.
	Jitter bool
}

// Stats is a cumulative snapshot. Safe to read while Run is executing.
type Stats struct {
	Cycles   int
	Sent     int
	Accepted int
	Rejected int
	// Undecided counts items the server neither accepted nor rejected. They
	// stay pending with their attempt counters untouched; the spool's own
	// undecided counter advances and parks the item at its cutoff.
	Undecided int
	// Parked counts items that reached that cutoff this drain's lifetime.
	Parked      int
	LastErr     error
	LastSuccess time.Time
	// Backoff is the delay Run will wait before the next cycle.
	Backoff time.Duration
}

const (
	defaultBatchSize  = 100
	defaultMaxBytes   = 4 << 20
	defaultInterval   = 5 * time.Second
	defaultInitial    = time.Second
	defaultMax        = 5 * time.Minute
	defaultMultiplier = 2.0

	// envelopeBytes approximates the JSON overhead of an Item's non-payload
	// fields. The byte cap only has to keep a request under a server limit, so a
	// small fixed allowance beats marshalling every item twice to measure it —
	// which on a 70 MB item would cost more than the request.
	envelopeBytes = 512
)

// Drain is safe for concurrent use: Stats may be called while Run is executing.
type Drain struct {
	opts Options

	// cycleMu serialises cycles. The spool deliberately does not hide leased
	// items from other readers, so two overlapping cycles would lease the same
	// files and send them twice. The server's idempotency key absorbs that, but
	// on a laptop it is wasted uplink — and the case is not hypothetical: a
	// shutdown flush calling RunOnce while Run is still looping is exactly how
	// the daemon is expected to be used.
	//
	// Separate from mu so Stats never blocks behind an in-flight request.
	cycleMu sync.Mutex

	mu    sync.Mutex
	stats Stats
	// attempt counts consecutive transport failures and drives the curve.
	attempt int
	// retryAfter is a server-supplied floor for the next wait.
	retryAfter time.Duration
	// batchFull records whether the last cycle filled its batch, meaning more
	// work is almost certainly waiting and Run should not idle.
	batchFull bool
}

// New validates options and applies defaults.
func New(opts Options) (*Drain, error) {
	if opts.Spool == nil {
		return nil, errors.New("drain: Spool is required")
	}
	if opts.Transport == nil {
		return nil, errors.New("drain: Transport is required")
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = defaultMaxBytes
	}
	if opts.Interval <= 0 {
		opts.Interval = defaultInterval
	}
	if opts.Backoff.Initial <= 0 {
		opts.Backoff.Initial = defaultInitial
	}
	if opts.Backoff.Max <= 0 {
		opts.Backoff.Max = defaultMax
	}
	if opts.Backoff.Multiplier < 1 {
		opts.Backoff.Multiplier = defaultMultiplier
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Drain{opts: opts}, nil
}

// Stats returns a cumulative snapshot.
func (d *Drain) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stats
}

// RunOnce performs exactly one cycle: lease a batch, send it, apply the
// server's verdict.
//
// One cycle is one batch, not "drain everything". Looping internally until the
// spool empties would spin forever the first time the server declines an item
// without rejecting it, because that item legitimately stays pending. A caller
// flushing on shutdown should loop RunOnce under its own deadline.
//
// The returned error is the transport's, when delivery failed. Items remain
// pending in that case and the next cycle retries them; the error is surfaced so
// a shutdown flush can tell "delivered" from "gave up because the network was
// down".
func (d *Drain) RunOnce(ctx context.Context) (Stats, error) {
	if err := ctx.Err(); err != nil {
		return d.Stats(), err
	}

	// Wait for any cycle already in flight. Bounded: an in-flight cycle honours
	// the same cancellation, so a shutdown flush is never blocked for longer
	// than the request it is waiting on.
	d.cycleMu.Lock()
	defer d.cycleMu.Unlock()
	if err := ctx.Err(); err != nil {
		return d.Stats(), err
	}

	d.mu.Lock()
	d.stats.Cycles++
	d.batchFull = false
	d.mu.Unlock()

	leased, err := d.opts.Spool.Lease(d.opts.BatchSize)
	if err != nil {
		d.recordErr(fmt.Errorf("drain: lease: %w", err))
		d.notify()
		return d.Stats(), err
	}
	if len(leased) == 0 {
		// An empty spool must not cost a request. This is the common case on an
		// idle laptop and it should be a directory listing and nothing else.
		d.notify()
		return d.Stats(), nil
	}

	if d.opts.Enrich != nil {
		d.opts.Enrich(ctx, leased)
	}
	batch := d.limitBytes(leased)
	if err := ctx.Err(); err != nil {
		return d.Stats(), err
	}

	// Sent counts what was handed to the transport, credited before the call so
	// it is accurate whether or not the request comes back.
	d.mu.Lock()
	d.stats.Sent += len(batch)
	d.mu.Unlock()

	resp, sendErr := d.opts.Transport.Send(ctx, batch)

	if sendErr != nil {
		// A transport failure is not the items' fault, so their attempt counters
		// are left alone. Advancing them here would mean a week-long outage
		// quarantines the entire backlog, which is precisely the data loss the
		// spool exists to prevent.
		if ctxErr := ctx.Err(); ctxErr != nil {
			d.notify()
			return d.Stats(), ctxErr
		}
		d.recordTransportFailure(sendErr, resp.RetryAfter)
		d.notify()
		return d.Stats(), sendErr
	}

	// The send succeeded, so the verdict is applied even if the context has since
	// been cancelled: these are local file operations, and skipping them would
	// re-send items the server has already durably stored.
	d.applyVerdict(batch, resp)
	d.recordSuccess(resp.RetryAfter, len(batch) == d.opts.BatchSize)
	d.notify()

	if err := ctx.Err(); err != nil {
		return d.Stats(), err
	}
	return d.Stats(), nil
}

// Run cycles until ctx is done, waiting between cycles according to the backoff
// curve, the server's RetryAfter, or the idle interval.
func (d *Drain) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		_, err := d.RunOnce(ctx)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		wait := d.nextWait(err != nil)
		if wait <= 0 {
			continue
		}
		if err := sleepCtx(ctx, wait); err != nil {
			return err
		}
	}
}

// nextWait decides how long Run pauses before the next cycle.
func (d *Drain) nextWait(failed bool) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()

	var wait time.Duration
	switch {
	case failed:
		wait = d.stats.Backoff
	case d.batchFull:
		// More work is waiting. Idling here would drain a large backlog one
		// batch per interval, which on a laptop returning from a long offline
		// stretch is the difference between minutes and hours.
		wait = 0
	default:
		wait = d.opts.Interval
	}
	// The server's own pacing wins whenever it asks for more room than we chose.
	if d.retryAfter > wait {
		wait = d.retryAfter
	}
	return wait
}

// limitBytes trims a leased batch to the byte cap.
//
// The first item is always included, however large it is. A single item over the
// cap would otherwise be skipped on every cycle and block everything behind it
// forever; sending it alone at least lets the server reject it, which routes it
// to quarantine and clears the queue.
func (d *Drain) limitBytes(in []spool.Leased) []spool.Leased {
	total := 0
	for i, l := range in {
		n := itemBytes(l)
		if i > 0 && total+n > d.opts.MaxBytes {
			return in[:i]
		}
		total += n
	}
	return in
}

func itemBytes(l spool.Leased) int {
	return len(l.Item.Payload) + envelopeBytes
}

// applyVerdict acks, quarantines or defers each item in the batch.
func (d *Drain) applyVerdict(batch []spool.Leased, resp Response) {
	accepted := make(map[string]bool, len(resp.Accepted))
	for _, id := range resp.Accepted {
		accepted[id] = true
	}
	// Membership, not the reason string: a server that rejects an item without
	// filling in a reason still rejected it, and testing the reason for
	// emptiness would quietly route it back onto the retry path.
	rejected := make(map[string]bool, len(resp.Rejected))
	for _, r := range resp.Rejected {
		rejected[r.ID] = true
	}

	// The reason travels with the item into quarantine, where it becomes the
	// filename suffix and the drop counter's key.
	reasons := make(map[string]string, len(resp.Rejected))
	for _, r := range resp.Rejected {
		reasons[r.ID] = r.Reason
	}

	var nAcc, nRej, nUnd, nPark int
	for _, l := range batch {
		id := l.Item.ID
		switch {
		case accepted[id]:
			if err := d.opts.Spool.Ack(l); err != nil {
				// The server has it; a failed unlink means we may deliver it
				// again, which the idempotency key absorbs.
				d.recordErr(fmt.Errorf("drain: ack %s: %w", id, err))
				continue
			}
			nAcc++
		case rejected[id]:
			// Permanent, immediately, with the server's reason kept: an item
			// the server refused is evidence, and redriving it after a
			// server-side fix is a supported operation.
			if err := d.opts.Spool.Quarantine(l, reasons[id]); err != nil {
				d.recordErr(fmt.Errorf("drain: quarantine %s: %w", id, err))
				continue
			}
			nRej++
		default:
			// Neither accepted nor rejected. Not a failed attempt: the spool
			// counts it separately and parks the item at its cutoff.
			parked, err := d.opts.Spool.Undecided(l)
			if err != nil {
				d.recordErr(fmt.Errorf("drain: undecided %s: %w", id, err))
				continue
			}
			nUnd++
			if parked {
				nPark++
			}
		}
	}

	d.mu.Lock()
	d.stats.Accepted += nAcc
	d.stats.Rejected += nRej
	d.stats.Undecided += nUnd
	d.stats.Parked += nPark
	d.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Bookkeeping
// ---------------------------------------------------------------------------

func (d *Drain) recordTransportFailure(err error, retryAfter time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.LastErr = err
	d.retryAfter = retryAfter
	d.stats.Backoff = d.computeBackoffLocked(d.attempt)
	d.attempt++
}

func (d *Drain) recordSuccess(retryAfter time.Duration, batchFull bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.LastErr = nil
	d.stats.LastSuccess = d.opts.Now()
	d.retryAfter = retryAfter
	d.batchFull = batchFull
	// A reachable server resets the curve, so the next outage starts from the
	// bottom rather than from wherever the previous one ended.
	d.attempt = 0
	d.stats.Backoff = 0
}

func (d *Drain) recordErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stats.LastErr = err
}

func (d *Drain) notify() {
	if d.opts.OnCycle == nil {
		return
	}
	d.opts.OnCycle(d.Stats())
}

// computeBackoffLocked returns the delay for a given consecutive-failure count.
//
// Full jitter, per the AWS retry guidance: sleep is uniform over [0, ceiling)
// where ceiling = min(max, initial * multiplier^attempt). Sleeping the ceiling
// itself, or jittering only a slice of it, leaves a fleet that failed together
// still retrying together.
func (d *Drain) computeBackoffLocked(attempt int) time.Duration {
	ceiling := float64(d.opts.Backoff.Initial) * math.Pow(d.opts.Backoff.Multiplier, float64(attempt))
	if maxF := float64(d.opts.Backoff.Max); ceiling > maxF || math.IsInf(ceiling, 0) {
		ceiling = maxF
	}
	if ceiling <= 0 {
		return 0
	}
	if !d.opts.Backoff.Jitter {
		return time.Duration(ceiling)
	}
	return time.Duration(rand.Int64N(int64(ceiling)) + 1)
}

// sleepCtx waits for d, returning early if ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
