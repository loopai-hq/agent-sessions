package drain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/spool"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// fakeTransport is a scripted server. Each Send consumes the next scripted
// reply, or repeats the last one once the script runs out.
type fakeTransport struct {
	mu sync.Mutex

	// replies are consumed in order.
	replies []reply
	// calls records every batch handed over, for assertions on batching.
	calls [][]string
	// hook, if set, runs at the start of Send. Used to cancel mid-flight.
	hook func(batch []spool.Leased)
}

type reply struct {
	resp Response
	err  error
	// acceptAll is a convenience: accept every ID in the batch.
	acceptAll bool
}

func (f *fakeTransport) Send(ctx context.Context, batch []spool.Leased) (Response, error) {
	f.mu.Lock()
	ids := make([]string, 0, len(batch))
	for _, l := range batch {
		ids = append(ids, l.Item.ID)
	}
	f.calls = append(f.calls, ids)

	var r reply
	switch {
	case len(f.replies) == 0:
		r = reply{acceptAll: true}
	case len(f.replies) == 1:
		r = f.replies[0]
	default:
		r = f.replies[0]
		f.replies = f.replies[1:]
	}
	hook := f.hook
	f.mu.Unlock()

	if hook != nil {
		hook(batch)
	}
	if r.err != nil {
		return r.resp, r.err
	}
	if r.acceptAll {
		return Response{Accepted: ids}, nil
	}
	return r.resp, nil
}

func (f *fakeTransport) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeTransport) lastBatch() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return nil
	}
	return f.calls[len(f.calls)-1]
}

// newSpool creates a spool in a temp dir. maxAttempts is exposed so tests can
// verify quarantine behaviour without waiting for the default of 8.
//
// DiskFree is stubbed. The spool refuses to write when the real filesystem drops
// below MinFreeRatio, which is correct behaviour on a laptop and fatal for a
// test suite: on a developer machine that happens to be low on space every test
// here fails with "refusing to write" and none of them are testing that.
func newSpool(t *testing.T, maxAttempts int) (*spool.Spool, string) {
	t.Helper()
	dir := t.TempDir()
	sp, err := spool.Open(spool.Options{
		Dir:          dir,
		MaxAttempts:  maxAttempts,
		MaxUndecided: maxAttempts,
		DiskFree: func(string) (free, total uint64, err error) {
			return 1 << 40, 1 << 40, nil
		},
	})
	if err != nil {
		t.Fatalf("spool.Open: %v", err)
	}
	return sp, dir
}

// seed adds n items and returns their IDs in insertion order.
func seed(t *testing.T, sp *spool.Spool, n int) []string {
	t.Helper()
	return seedSized(t, sp, n, 16)
}

func seedSized(t *testing.T, sp *spool.Spool, n, payloadBytes int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := range n {
		id := fmt.Sprintf("item-%03d", i)
		body, err := json.Marshal(strings.Repeat("x", payloadBytes))
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		it := spool.Item{
			ID:        id,
			Kind:      "event",
			SessionID: "sess",
			Seq:       int64(i),
			EventTime: time.Unix(1700000000+int64(i), 0).UTC(),
			Payload:   body,
		}
		if err := sp.Add(it); err != nil {
			t.Fatalf("spool.Add: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

func pendingCount(t *testing.T, sp *spool.Spool) int {
	t.Helper()
	st, err := sp.Stats()
	if err != nil {
		t.Fatalf("spool.Stats: %v", err)
	}
	return st.Pending
}

func quarantineCount(t *testing.T, sp *spool.Spool) int {
	t.Helper()
	st, err := sp.Stats()
	if err != nil {
		t.Fatalf("spool.Stats: %v", err)
	}
	return st.Quarantine
}

// pendingIDs reads the pending directory directly so a test can assert exactly
// which item survived, not merely how many.
func pendingIDs(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(dir, "pending"))
	if err != nil {
		t.Fatalf("read pending: %v", err)
	}
	var ids []string
	for _, e := range ents {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, "pending", e.Name()))
		if err != nil {
			continue
		}
		var it spool.Item
		if err := json.Unmarshal(b, &it); err != nil {
			continue
		}
		ids = append(ids, it.ID)
	}
	return ids
}

func newDrain(t *testing.T, sp *spool.Spool, tr Transport, mutate func(*Options)) *Drain {
	t.Helper()
	opts := Options{
		Spool:     sp,
		Transport: tr,
		BatchSize: 10,
		Interval:  time.Millisecond,
		Backoff:   BackoffPolicy{Initial: time.Millisecond, Max: 8 * time.Millisecond, Multiplier: 2},
	}
	if mutate != nil {
		mutate(&opts)
	}
	d, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

// ---------------------------------------------------------------------------
// Acknowledgement is the load-bearing property
// ---------------------------------------------------------------------------

// Only what the server said it stored may leave the spool.
func TestPartialAcceptanceLeavesRemainderPending(t *testing.T) {
	sp, dir := newSpool(t, 8)
	ids := seed(t, sp, 3)

	tr := &fakeTransport{replies: []reply{{
		resp: Response{Accepted: []string{ids[0], ids[2]}},
	}}}
	d := newDrain(t, sp, tr, nil)

	st, err := d.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if st.Sent != 3 {
		t.Errorf("Sent = %d, want 3", st.Sent)
	}
	if st.Accepted != 2 {
		t.Errorf("Accepted = %d, want 2", st.Accepted)
	}
	if n := pendingCount(t, sp); n != 1 {
		t.Fatalf("pending = %d, want exactly 1", n)
	}
	if got := pendingIDs(t, dir); len(got) != 1 || got[0] != ids[1] {
		t.Errorf("pending = %v, want [%s] — the un-acked item specifically", got, ids[1])
	}
}

// An empty accepted list must remove nothing, even on a successful response.
func TestSuccessfulResponseAcceptingNothingAcksNothing(t *testing.T) {
	sp, _ := newSpool(t, 8)
	seed(t, sp, 3)

	tr := &fakeTransport{replies: []reply{{resp: Response{}}}}
	d := newDrain(t, sp, tr, nil)

	if _, err := d.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n := pendingCount(t, sp); n != 3 {
		t.Errorf("pending = %d, want 3; a 200 with no accepted ids must not delete anything", n)
	}
	if st := d.Stats(); st.Accepted != 0 || st.Undecided != 3 {
		t.Errorf("Accepted=%d Undecided=%d, want 0 and 3", st.Accepted, st.Undecided)
	}
}

// An item the server names in neither list is not a failed delivery. Its
// attempt counter must not move: nine machines carried quarantines that were
// nothing but eight silent 200s counted as eight failures.
func TestUndecidedDoesNotAdvanceAttempts(t *testing.T) {
	sp, dir := newSpool(t, 8)
	seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{resp: Response{}}}}
	d := newDrain(t, sp, tr, nil)

	for i := range 5 {
		if _, err := d.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce %d: %v", i, err)
		}
	}
	leased, err := sp.Lease(1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("item left pending: %v %d", err, len(leased))
	}
	if leased[0].Item.Attempts != 0 {
		t.Errorf("Attempts = %d after five undecided verdicts, want 0", leased[0].Item.Attempts)
	}
	if leased[0].Item.Undecided != 5 {
		t.Errorf("Undecided = %d, want 5", leased[0].Item.Undecided)
	}
	if got := pendingIDs(t, dir); len(got) != 1 {
		t.Errorf("pending = %v, want the one item still there", got)
	}
	if n := quarantineCount(t, sp); n != 0 {
		t.Errorf("quarantine = %d, want 0", n)
	}
}

// At the cutoff the item is parked, never quarantined, and the counters say so.
func TestUndecidedParksAfterNAndNeverQuarantines(t *testing.T) {
	sp, _ := newSpool(t, 3)
	seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{resp: Response{}}}}
	d := newDrain(t, sp, tr, nil)

	for i := range 5 {
		if _, err := d.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce %d: %v", i, err)
		}
	}
	st, err := sp.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Pending != 0 || st.Parked != 1 || st.Quarantine != 0 {
		t.Errorf("pending=%d parked=%d quarantine=%d, want 0/1/0", st.Pending, st.Parked, st.Quarantine)
	}
	if st.Dropped["parked"] != 1 || st.Dropped["max_attempts"] != 0 {
		t.Errorf("counters = %v; the item was parked, not exhausted", st.Dropped)
	}
	if ds := d.Stats(); ds.Parked != 1 || ds.Undecided != 3 {
		t.Errorf("drain stats parked=%d undecided=%d, want 1 and 3", ds.Parked, ds.Undecided)
	}
	// Nothing further is sent for a parked item.
	if tr.callCount() != 3 {
		t.Errorf("transport called %d times, want 3: a parked item leaves the queue", tr.callCount())
	}
}

// A parked item redriven after an upgrade delivers like any other.
func TestParkedItemsDeliverAfterRedrive(t *testing.T) {
	sp, _ := newSpool(t, 2)
	ids := seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{resp: Response{}}, {resp: Response{}}, {acceptAll: true}}}
	d := newDrain(t, sp, tr, nil)
	ctx := context.Background()
	for range 2 {
		_, _ = d.RunOnce(ctx)
	}
	if st, _ := sp.Stats(); st.Parked != 1 {
		t.Fatalf("parked = %d, want 1", st.Parked)
	}
	if n, err := sp.RedriveParked(); err != nil || n != 1 {
		t.Fatalf("redrive: n=%d err=%v", n, err)
	}
	if _, err := d.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if n := pendingCount(t, sp); n != 0 {
		t.Errorf("pending = %d, want 0 after the redriven item was accepted", n)
	}
	if last := tr.lastBatch(); len(last) != 1 || last[0] != ids[0] {
		t.Errorf("last batch = %v, want the redriven item", last)
	}
}

// ---------------------------------------------------------------------------
// Rejection
// ---------------------------------------------------------------------------

// A rejected item is permanently undeliverable. It must leave pending, and it
// must land in quarantine rather than being deleted.
func TestRejectionQuarantinesRatherThanDropping(t *testing.T) {
	sp, _ := newSpool(t, 8)
	ids := seed(t, sp, 2)

	tr := &fakeTransport{replies: []reply{{
		resp: Response{
			Accepted: []string{ids[0]},
			Rejected: []Reject{{ID: ids[1], Reason: "schema violation"}},
		},
	}}}
	d := newDrain(t, sp, tr, nil)

	st, err := d.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if st.Rejected != 1 {
		t.Errorf("Rejected = %d, want 1", st.Rejected)
	}
	if n := pendingCount(t, sp); n != 0 {
		t.Errorf("pending = %d, want 0", n)
	}
	if n := quarantineCount(t, sp); n != 1 {
		t.Fatalf("quarantine = %d, want 1; a rejected item must be evidence, not a silent drop", n)
	}
}

// Quarantine must happen on the first rejection regardless of how the spool's
// attempt cutoff is configured, and must not require N cycles to take effect.
func TestRejectionQuarantinesImmediatelyAtAnyCutoff(t *testing.T) {
	for _, maxAttempts := range []int{1, 3, 8, 100} {
		t.Run(fmt.Sprintf("maxAttempts=%d", maxAttempts), func(t *testing.T) {
			sp, _ := newSpool(t, maxAttempts)
			ids := seed(t, sp, 1)

			tr := &fakeTransport{replies: []reply{{
				resp: Response{Rejected: []Reject{{ID: ids[0], Reason: "too large"}}},
			}}}
			d := newDrain(t, sp, tr, nil)

			if _, err := d.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if n := quarantineCount(t, sp); n != 1 {
				t.Errorf("quarantine = %d after one cycle, want 1", n)
			}
			if n := pendingCount(t, sp); n != 0 {
				t.Errorf("pending = %d, want 0", n)
			}
		})
	}
}

// A server that rejects an item without filling in a reason has still rejected
// it. Keying on the reason string would route it back onto the retry path.
func TestRejectionWithEmptyReasonStillQuarantines(t *testing.T) {
	sp, _ := newSpool(t, 100)
	ids := seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{
		resp: Response{Rejected: []Reject{{ID: ids[0]}}},
	}}}
	d := newDrain(t, sp, tr, nil)

	if _, err := d.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if st := d.Stats(); st.Rejected != 1 || st.Undecided != 0 {
		t.Errorf("Rejected=%d Undecided=%d, want 1 and 0", st.Rejected, st.Undecided)
	}
	if n := quarantineCount(t, sp); n != 1 {
		t.Errorf("quarantine = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Transport failure
// ---------------------------------------------------------------------------

// A network failure is not the items' fault. Everything stays pending, and
// crucially the attempt counters must NOT advance — otherwise a long outage
// quarantines the whole backlog.
func TestTransportErrorLeavesEverythingPendingAndRetries(t *testing.T) {
	sp, _ := newSpool(t, 3)
	seed(t, sp, 3)

	boom := errors.New("dial tcp: network is unreachable")
	tr := &fakeTransport{replies: []reply{{err: boom}}}
	d := newDrain(t, sp, tr, nil)

	// Far more cycles than the spool's attempt cutoff.
	for i := range 10 {
		_, err := d.RunOnce(context.Background())
		if !errors.Is(err, boom) {
			t.Fatalf("cycle %d: err = %v, want %v", i, err, boom)
		}
	}

	if n := pendingCount(t, sp); n != 3 {
		t.Errorf("pending = %d, want 3; a network outage must not consume delivery attempts", n)
	}
	if n := quarantineCount(t, sp); n != 0 {
		t.Errorf("quarantine = %d, want 0; an unreachable server is not a poison payload", n)
	}
	if st := d.Stats(); st.LastErr == nil {
		t.Error("LastErr not recorded")
	}
}

// Once the network comes back, the backlog delivers.
func TestRecoversAfterOutage(t *testing.T) {
	sp, _ := newSpool(t, 8)
	seed(t, sp, 3)

	tr := &fakeTransport{replies: []reply{
		{err: errors.New("offline")},
		{err: errors.New("offline")},
		{acceptAll: true},
	}}
	d := newDrain(t, sp, tr, nil)

	ctx := context.Background()
	for range 3 {
		_, _ = d.RunOnce(ctx)
	}

	if n := pendingCount(t, sp); n != 0 {
		t.Errorf("pending = %d, want 0 after the server returned", n)
	}
	if st := d.Stats(); st.LastErr != nil {
		t.Errorf("LastErr = %v, want nil after a success", st.LastErr)
	}
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

func TestBackoffGrowsAndResetsOnSuccess(t *testing.T) {
	sp, _ := newSpool(t, 100)
	seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{
		{err: errors.New("down")},
		{err: errors.New("down")},
		{err: errors.New("down")},
		{acceptAll: true},
	}}
	// Jitter off so the curve is assertable; the jitter itself is covered below.
	d := newDrain(t, sp, tr, func(o *Options) {
		o.Backoff = BackoffPolicy{Initial: 10 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: false}
	})

	ctx := context.Background()
	var seen []time.Duration
	for range 3 {
		st, _ := d.RunOnce(ctx)
		seen = append(seen, st.Backoff)
	}

	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for i := range want {
		if seen[i] != want[i] {
			t.Errorf("backoff[%d] = %s, want %s (curve: %v)", i, seen[i], want[i], seen)
		}
	}

	st, _ := d.RunOnce(ctx)
	if st.Backoff != 0 {
		t.Errorf("Backoff = %s after success, want 0; a reachable server must reset the curve", st.Backoff)
	}
}

func TestBackoffCappedAtMax(t *testing.T) {
	sp, _ := newSpool(t, 1000)
	seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{err: errors.New("down")}}}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.Backoff = BackoffPolicy{Initial: time.Second, Max: 5 * time.Second, Multiplier: 2, Jitter: false}
	})

	ctx := context.Background()
	for i := range 30 {
		st, _ := d.RunOnce(ctx)
		if st.Backoff > 5*time.Second {
			t.Fatalf("cycle %d: backoff %s exceeded Max", i, st.Backoff)
		}
	}
	if st := d.Stats(); st.Backoff != 5*time.Second {
		t.Errorf("Backoff = %s, want the 5s ceiling", st.Backoff)
	}
}

// Full jitter means uniform over the whole interval, not a fraction of it. A
// fleet that fails together must not retry together.
func TestFullJitterSpreadsWithinCeiling(t *testing.T) {
	sp, _ := newSpool(t, 100000)
	seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{err: errors.New("down")}}}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.Backoff = BackoffPolicy{Initial: time.Second, Max: time.Second, Multiplier: 1, Jitter: true}
	})

	ctx := context.Background()
	seen := map[time.Duration]bool{}
	var belowHalf int
	const n = 200
	for range n {
		st, _ := d.RunOnce(ctx)
		if st.Backoff <= 0 || st.Backoff > time.Second {
			t.Fatalf("backoff %s outside (0, 1s]", st.Backoff)
		}
		seen[st.Backoff] = true
		if st.Backoff < 500*time.Millisecond {
			belowHalf++
		}
	}

	if len(seen) < n/2 {
		t.Errorf("only %d distinct delays out of %d — this does not look like jitter", len(seen), n)
	}
	// With full jitter roughly half the samples fall in the lower half. Fixed
	// backoff or a narrow jitter band would put essentially none there.
	if belowHalf < n/5 {
		t.Errorf("%d/%d samples below the midpoint; full jitter should be ~half", belowHalf, n)
	}
}

func TestRetryAfterHonouredWhenLargerThanBackoff(t *testing.T) {
	sp, _ := newSpool(t, 100)
	seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{
		resp: Response{RetryAfter: 750 * time.Millisecond},
		err:  errors.New("503 service unavailable"),
	}}}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.Backoff = BackoffPolicy{Initial: time.Millisecond, Max: 2 * time.Millisecond, Multiplier: 2, Jitter: false}
	})

	if _, err := d.RunOnce(context.Background()); err == nil {
		t.Fatal("expected the transport error")
	}
	if got := d.nextWait(true); got != 750*time.Millisecond {
		t.Errorf("wait = %s, want the server's 750ms RetryAfter to win over the 1ms backoff", got)
	}
}

func TestBackoffWinsWhenLargerThanRetryAfter(t *testing.T) {
	sp, _ := newSpool(t, 100)
	seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{
		resp: Response{RetryAfter: time.Millisecond},
		err:  errors.New("503"),
	}}}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.Backoff = BackoffPolicy{Initial: time.Second, Max: time.Minute, Multiplier: 2, Jitter: false}
	})

	if _, err := d.RunOnce(context.Background()); err == nil {
		t.Fatal("expected the transport error")
	}
	if got := d.nextWait(true); got != time.Second {
		t.Errorf("wait = %s, want the 1s backoff to win over a 1ms RetryAfter", got)
	}
}

// A server can pace a client without failing the request.
func TestRetryAfterHonouredOnSuccessfulResponse(t *testing.T) {
	sp, _ := newSpool(t, 100)
	ids := seed(t, sp, 1)

	tr := &fakeTransport{replies: []reply{{
		resp: Response{Accepted: ids, RetryAfter: 300 * time.Millisecond},
	}}}
	d := newDrain(t, sp, tr, nil)

	if _, err := d.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := d.nextWait(false); got != 300*time.Millisecond {
		t.Errorf("wait = %s, want 300ms; RetryAfter applies to throttled successes too", got)
	}
}

// ---------------------------------------------------------------------------
// Batching
// ---------------------------------------------------------------------------

func TestBatchSizeCapsItemsPerRequest(t *testing.T) {
	sp, _ := newSpool(t, 100)
	seed(t, sp, 25)

	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, func(o *Options) { o.BatchSize = 10 })

	if _, err := d.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n := len(tr.lastBatch()); n != 10 {
		t.Errorf("batch = %d items, want 10", n)
	}
	if n := pendingCount(t, sp); n != 15 {
		t.Errorf("pending = %d, want 15", n)
	}
}

func TestByteCapSplitsBatch(t *testing.T) {
	sp, _ := newSpool(t, 100)
	// Each item is ~1 KiB of payload plus the envelope allowance.
	seedSized(t, sp, 10, 1024)

	tr := &fakeTransport{}
	// Room for roughly two items, well under the 10-item count cap.
	d := newDrain(t, sp, tr, func(o *Options) {
		o.BatchSize = 10
		o.MaxBytes = 2 * (1024 + envelopeBytes)
	})

	if _, err := d.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	n := len(tr.lastBatch())
	if n == 0 || n >= 10 {
		t.Fatalf("batch = %d items; want the byte cap to bind before the count cap", n)
	}
	if n > 2 {
		t.Errorf("batch = %d items, want at most 2 within the byte budget", n)
	}
}

// A single item larger than the whole budget must still be sent, alone. Skipping
// it would block every item behind it forever.
func TestOversizedSingleItemIsSentAlone(t *testing.T) {
	sp, _ := newSpool(t, 100)
	seedSized(t, sp, 1, 64*1024)
	seedSized(t, sp, 1, 16)

	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.BatchSize = 10
		o.MaxBytes = 1024 // smaller than the first item
	})

	if _, err := d.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n := len(tr.lastBatch()); n != 1 {
		t.Fatalf("batch = %d items, want exactly 1 — the oversized item on its own", n)
	}
	if st := d.Stats(); st.Sent != 1 {
		t.Errorf("Sent = %d, want 1", st.Sent)
	}
}

// The queue must not wedge behind an item the server will never take.
func TestOversizedItemRejectedThenQueueDrains(t *testing.T) {
	sp, _ := newSpool(t, 8)
	big := seedSized(t, sp, 1, 64*1024)
	seedSized(t, sp, 1, 16)

	tr := &fakeTransport{replies: []reply{
		{resp: Response{Rejected: []Reject{{ID: big[0], Reason: "payload too large"}}}},
		{acceptAll: true},
	}}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.BatchSize = 10
		o.MaxBytes = 1024
	})

	ctx := context.Background()
	for i := range 2 {
		if _, err := d.RunOnce(ctx); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
	}

	if n := pendingCount(t, sp); n != 0 {
		t.Errorf("pending = %d, want 0; the queue must drain once the poison item is quarantined", n)
	}
	if n := quarantineCount(t, sp); n != 1 {
		t.Errorf("quarantine = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Idle and lifecycle
// ---------------------------------------------------------------------------

func TestEmptySpoolIsACheapNoOp(t *testing.T) {
	sp, _ := newSpool(t, 8)

	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, nil)

	st, err := d.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if tr.callCount() != 0 {
		t.Error("an empty spool must not produce a request")
	}
	if st.Cycles != 1 {
		t.Errorf("Cycles = %d, want 1", st.Cycles)
	}
	if st.Sent != 0 {
		t.Errorf("Sent = %d, want 0", st.Sent)
	}
}

func TestNewValidatesOptions(t *testing.T) {
	sp, _ := newSpool(t, 8)
	if _, err := New(Options{Transport: &fakeTransport{}}); err == nil {
		t.Error("expected an error with no Spool")
	}
	if _, err := New(Options{Spool: sp}); err == nil {
		t.Error("expected an error with no Transport")
	}

	d, err := New(Options{Spool: sp, Transport: &fakeTransport{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.opts.BatchSize != defaultBatchSize || d.opts.MaxBytes != defaultMaxBytes ||
		d.opts.Interval != defaultInterval || d.opts.Backoff.Multiplier != defaultMultiplier {
		t.Errorf("defaults not applied: %+v", d.opts)
	}
}

func TestStatsAccumulateAcrossCycles(t *testing.T) {
	sp, _ := newSpool(t, 100)
	ids := seed(t, sp, 4)

	tr := &fakeTransport{replies: []reply{
		{resp: Response{
			Accepted: []string{ids[0], ids[1]},
			Rejected: []Reject{{ID: ids[2], Reason: "bad"}},
		}},
		{resp: Response{Accepted: []string{ids[3]}}},
	}}
	d := newDrain(t, sp, tr, nil)

	ctx := context.Background()
	for i := range 2 {
		if _, err := d.RunOnce(ctx); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
	}

	st := d.Stats()
	if st.Cycles != 2 {
		t.Errorf("Cycles = %d, want 2", st.Cycles)
	}
	if st.Sent != 5 {
		t.Errorf("Sent = %d, want 5 (4 then the 1 left over)", st.Sent)
	}
	if st.Accepted != 3 {
		t.Errorf("Accepted = %d, want 3", st.Accepted)
	}
	if st.Rejected != 1 {
		t.Errorf("Rejected = %d, want 1", st.Rejected)
	}
	if st.LastSuccess.IsZero() {
		t.Error("LastSuccess not set")
	}
	if n := pendingCount(t, sp); n != 0 {
		t.Errorf("pending = %d, want 0", n)
	}
}

func TestOnCycleCalledEveryCycle(t *testing.T) {
	sp, _ := newSpool(t, 8)
	seed(t, sp, 1)

	var mu sync.Mutex
	var calls int
	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.OnCycle = func(Stats) {
			mu.Lock()
			calls++
			mu.Unlock()
		}
	})

	ctx := context.Background()
	// One cycle with work, one on an empty spool.
	_, _ = d.RunOnce(ctx)
	_, _ = d.RunOnce(ctx)

	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Errorf("OnCycle called %d times, want 2 (idle cycles count too)", calls)
	}
}

// A cycle that filled its batch must not idle, or a large backlog would drain
// one batch per interval.
func TestFullBatchDoesNotWaitTheIdleInterval(t *testing.T) {
	sp, _ := newSpool(t, 100)
	// 25, not a multiple of the batch size, so the final batch is partial and
	// the idle wait is actually exercised.
	seed(t, sp, 25)

	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.BatchSize = 10
		o.Interval = time.Hour // would stall the test if it were honoured
	})

	if _, err := d.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := d.nextWait(false); got != 0 {
		t.Errorf("wait = %s after a full batch, want 0", got)
	}

	// Drain the rest; the final partial batch should then request the idle wait.
	for pendingCount(t, sp) > 0 {
		if _, err := d.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
	}
	if got := d.nextWait(false); got != time.Hour {
		t.Errorf("wait = %s after a partial batch, want the idle interval", got)
	}
}

func TestRunDrainsAndStopsOnContextCancel(t *testing.T) {
	sp, _ := newSpool(t, 100)
	seed(t, sp, 25)

	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.BatchSize = 10
		o.Interval = time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for pendingCount(t, sp) > 0 {
		select {
		case <-deadline:
			t.Fatalf("Run did not drain the spool; %d pending", pendingCount(t, sp))
		default:
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after cancel")
	}
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

// Cancelling mid-request must return promptly and must not consume items.
func TestCancelMidBatchLeavesItemsIntact(t *testing.T) {
	sp, _ := newSpool(t, 100)
	seed(t, sp, 5)

	ctx, cancel := context.WithCancel(context.Background())
	tr := &fakeTransport{
		replies: []reply{{err: context.Canceled}},
		hook:    func([]spool.Leased) { cancel() },
	}
	d := newDrain(t, sp, tr, nil)

	start := time.Now()
	_, err := d.RunOnce(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("RunOnce took %s after cancel, want prompt return", elapsed)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if n := pendingCount(t, sp); n != 5 {
		t.Errorf("pending = %d, want all 5 intact", n)
	}
	if n := quarantineCount(t, sp); n != 0 {
		t.Errorf("quarantine = %d, want 0; cancellation is not a delivery failure", n)
	}
}

// If the server did store the batch, a cancellation arriving afterwards must not
// cost us the ack — otherwise shutdown re-sends data the server already has.
func TestCancelAfterSuccessfulSendStillAcks(t *testing.T) {
	sp, _ := newSpool(t, 100)
	seed(t, sp, 3)

	ctx, cancel := context.WithCancel(context.Background())
	tr := &fakeTransport{hook: func([]spool.Leased) { cancel() }}
	d := newDrain(t, sp, tr, nil)

	_, err := d.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if n := pendingCount(t, sp); n != 0 {
		t.Errorf("pending = %d, want 0; the server stored these, so they must be acked", n)
	}
}

func TestRunOnceReturnsImmediatelyOnCancelledContext(t *testing.T) {
	sp, _ := newSpool(t, 8)
	seed(t, sp, 3)

	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := d.RunOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if tr.callCount() != 0 {
		t.Error("a cancelled context must not produce a request")
	}
	if n := pendingCount(t, sp); n != 3 {
		t.Errorf("pending = %d, want 3", n)
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// Run and Stats must not race. Meaningful under -race.
func TestRunAndStatsDoNotRace(t *testing.T) {
	sp, _ := newSpool(t, 1000)
	seed(t, sp, 200)

	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.BatchSize = 5
		o.Interval = time.Microsecond
		o.OnCycle = func(s Stats) { _ = s.Cycles }
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = d.Run(ctx)
	}()

	// Hammer Stats from several readers while Run mutates it.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				st := d.Stats()
				_ = st.Cycles + st.Sent + st.Accepted + st.Rejected + st.Undecided + st.Parked
				_ = st.Backoff
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	if st := d.Stats(); st.Cycles == 0 {
		t.Error("Run performed no cycles")
	}
}

// Concurrent RunOnce callers must not corrupt the counters. The daemon is not
// expected to do this, but a shutdown flush racing the loop is plausible.
func TestConcurrentRunOnceIsSafe(t *testing.T) {
	sp, _ := newSpool(t, 1000)
	seed(t, sp, 100)

	tr := &fakeTransport{}
	d := newDrain(t, sp, tr, func(o *Options) { o.BatchSize = 5 })

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				_, _ = d.RunOnce(context.Background())
			}
		}()
	}
	wg.Wait()

	st := d.Stats()
	if st.Cycles != 80 {
		t.Errorf("Cycles = %d, want 80", st.Cycles)
	}
	// Cycles are serialised, so no item is leased by two overlapping cycles and
	// nothing is delivered — or counted — twice.
	if st.Accepted != 100 {
		t.Errorf("Accepted = %d, want exactly 100; overlapping cycles must not double-send", st.Accepted)
	}
	if st.Sent != 100 {
		t.Errorf("Sent = %d, want exactly 100", st.Sent)
	}
	if n := pendingCount(t, sp); n != 0 {
		t.Errorf("pending = %d, want 0", n)
	}
}

// Enrich sees the leased batch before it is sent and its rewrite of a payload
// is what the transport receives.
func TestEnrichRewritesThePayloadTheTransportSends(t *testing.T) {
	sp, err := spool.Open(spool.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.Add(spool.Item{ID: "e1", Kind: "event", SessionID: "s1", Seq: 1, Payload: json.RawMessage(`{"text":"before"}`)}); err != nil {
		t.Fatal(err)
	}
	var sent string
	tr := &fakeTransport{hook: func(batch []spool.Leased) {
		if len(batch) == 1 {
			sent = string(batch[0].Item.Payload)
		}
	}}
	d := newDrain(t, sp, tr, func(o *Options) {
		o.Enrich = func(_ context.Context, batch []spool.Leased) {
			for i := range batch {
				batch[i].Item.Payload = json.RawMessage(`{"text":"after"}`)
			}
		}
	})
	if _, err := d.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sent != `{"text":"after"}` {
		t.Errorf("transport received %q, want the enriched payload", sent)
	}
}
