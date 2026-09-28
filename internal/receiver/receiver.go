// Package receiver is a reference implementation of the server side of the
// drain's contract, and the fixture the client's end-to-end tests run against.
//
// It exists for two reasons. The obvious one is testing: until now the drain
// had only ever spoken to a fake defined in its own test file, so nothing had
// proven hook -> spool -> drain -> server as a single chain. The less obvious
// one matters more — this is what the real server gets written against, so
// where it is sloppy the production implementation will be sloppy in the same
// place.
//
// Two behaviours are therefore not negotiable.
//
// Idempotency by event id. The client guarantees at-least-once delivery by
// design: a crash between the server's commit and the client's ack, a
// truncation re-read, a reconciliation pass revisiting a session — all of them
// legitimately redeliver. A server that appends on redelivery double-counts
// every metric derived from the stream, and does so silently.
//
// Rejection is a judgement only the server can make. The client quarantines
// whatever comes back rejected and never retries it, so rejecting is
// irreversible from the client's side. Anything malformed must be rejected
// rather than accepted, because an accepted-but-unstorable item is data the
// client has already deleted.
package receiver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// Path is the ingest endpoint.
const Path = "/v1/events"

const (
	defaultMaxBatchBytes = 4 << 20
	// defaultHardBodyMultiple bounds what is read off the wire at all, as a
	// multiple of MaxBatchBytes. The gap between the two is deliberate: a body
	// in that window is over the advertised cap but still small enough to parse,
	// which is what lets an over-cap batch be answered per item instead of with
	// a 413. Past the hard limit there is no per-item answer to give, because
	// the items were never read.
	defaultHardBodyMultiple = 8
)

// Response is the wire form of the server's verdict. It mirrors drain.Response
// without importing it: the client and server are separately deployable and
// should not share a struct that would force them to be upgraded together.
type Response struct {
	Accepted     []string `json:"accepted"`
	Rejected     []Reject `json:"rejected,omitempty"`
	RetryAfterMS int64    `json:"retry_after_ms,omitempty"`
}

// Reject is one permanently unacceptable item.
type Reject struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// Rejection reasons. Stable strings so a test — or an operator reading a
// quarantine directory — can tell why something was refused.
const (
	ReasonNoID        = "missing id"
	ReasonNoSessionID = "missing session_id"
	ReasonBadPayload  = "payload is not valid json"
	ReasonInvalid     = "event failed validation"
	ReasonBatchTooBig = "batch exceeds the server's size limit"
)

// FailMode selects an injected failure.
type FailMode string

const (
	// FailNone is the healthy path.
	FailNone FailMode = ""
	// Fail503 is an unavailable server: a transport error to the client, which
	// must retry forever rather than dropping anything.
	Fail503 FailMode = "503"
	// Fail429 is throttling, carrying Retry-After.
	Fail429 FailMode = "429"
	// FailDrop hijacks and closes the connection without a response, which is
	// what a dropped network link looks like from the client's side. It is the
	// nastiest case because the client cannot know whether the server committed.
	FailDrop FailMode = "drop"
)

// Options configure a Receiver.
type Options struct {
	// MaxBatchBytes caps one request body. Zero takes 4 MiB.
	MaxBatchBytes int
	// HardBodyLimit is the absolute ceiling on bytes read. Zero takes eight
	// times MaxBatchBytes.
	HardBodyLimit int64
	// Now is injectable so a test can pin ingest timestamps.
	Now func() time.Time
}

// Receiver accumulates delivered events. It holds no global state, so tests may
// run in parallel.
type Receiver struct {
	maxBatchBytes int
	hardBodyLimit int64
	now           func() time.Time

	mu sync.Mutex
	// stored is keyed by event id. The map IS the idempotency: a redelivered id
	// finds itself already present and nothing is appended.
	stored map[string]event.Event
	// arrival preserves the order ids were first accepted, which is what lets a
	// test tell "stored once" from "stored twice and deduped on read".
	arrival []string
	// ingestedAt records when each id first landed, which is deliberately
	// separate from the event's own OccurredAt.
	ingestedAt map[string]time.Time

	requests  int
	rejected  int
	redundant int

	failMode   FailMode
	failTimes  int
	retryAfter time.Duration
}

// New builds a Receiver.
func New(opts Options) *Receiver {
	if opts.MaxBatchBytes <= 0 {
		opts.MaxBatchBytes = defaultMaxBatchBytes
	}
	if opts.HardBodyLimit <= 0 {
		opts.HardBodyLimit = int64(opts.MaxBatchBytes) * defaultHardBodyMultiple
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Receiver{
		maxBatchBytes: opts.MaxBatchBytes,
		hardBodyLimit: opts.HardBodyLimit,
		now:           opts.Now,
		stored:        map[string]event.Event{},
		ingestedAt:    map[string]time.Time{},
	}
}

// ServeHTTP implements the ingest endpoint.
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != Path {
		http.NotFound(w, req)
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if mode, retryAfter, ok := r.takeFailure(); ok {
		r.serveFailure(w, req, mode, retryAfter)
		return
	}

	r.mu.Lock()
	r.requests++
	r.mu.Unlock()

	items, tooBig, err := r.decode(req)
	if err != nil {
		// A body we cannot parse at all is the one case with no per-item answer
		// to give, so it is a request-level error.
		http.Error(w, "malformed request body", http.StatusBadRequest)
		return
	}

	resp := r.ingest(items, tooBig)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// decode reads the batch.
//
// An over-cap batch is reported rather than refused. Returning 413 would be the
// conventional answer and is the wrong one here: to the client a 413 is a
// transport error, so it would retry the same oversized batch forever, and the
// drain deliberately sends a single over-limit item alone rather than skipping
// it. Parsing anyway and rejecting each item gives the client something it can
// act on — quarantine — instead of an unbreakable loop.
func (r *Receiver) decode(req *http.Request) (items []spool.Item, tooBig bool, err error) {
	counted := &countingReader{r: http.MaxBytesReader(nil, req.Body, r.hardBodyLimit)}

	dec := json.NewDecoder(counted)
	if err := dec.Decode(&items); err != nil {
		return nil, false, err
	}

	// Measured from what was actually read, not from Content-Length: the header
	// is supplied by the client and a server must not size-check on a number the
	// other side chose.
	return items, counted.n > int64(r.maxBatchBytes), nil
}

// countingReader records how many bytes were consumed.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// ingest applies the per-item verdict.
func (r *Receiver) ingest(items []spool.Item, tooBig bool) Response {
	resp := Response{Accepted: []string{}}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, it := range items {
		if tooBig {
			resp.Rejected = append(resp.Rejected, Reject{ID: it.ID, Reason: ReasonBatchTooBig})
			r.rejected++
			continue
		}
		if reason, ok := validate(it); !ok {
			resp.Rejected = append(resp.Rejected, Reject{ID: it.ID, Reason: reason})
			r.rejected++
			continue
		}

		var e event.Event
		// validate already proved this parses.
		_ = json.Unmarshal(it.Payload, &e)

		if _, seen := r.stored[it.ID]; seen {
			// Redelivery. Acknowledged so the client can free it, but nothing is
			// appended: at-least-once delivery is guaranteed, so this is the
			// normal path after any crash or truncation re-read, not an anomaly.
			r.redundant++
			resp.Accepted = append(resp.Accepted, it.ID)
			continue
		}

		r.stored[it.ID] = e
		r.arrival = append(r.arrival, it.ID)
		r.ingestedAt[it.ID] = r.now()
		resp.Accepted = append(resp.Accepted, it.ID)
	}
	return resp
}

// validate decides whether an item is storable. This is the only place the
// judgement is made, and it is irreversible from the client's side.
func validate(it spool.Item) (reason string, ok bool) {
	if it.ID == "" {
		return ReasonNoID, false
	}
	if it.SessionID == "" {
		return ReasonNoSessionID, false
	}
	// A nil payload marshals to JSON null and unmarshals back into a zero event
	// without error, so an emptiness check alone lets it through as a validation
	// failure rather than the missing body it is.
	trimmed := bytes.TrimSpace(it.Payload)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return ReasonBadPayload, false
	}
	var e event.Event
	if json.Unmarshal(trimmed, &e) != nil {
		return ReasonBadPayload, false
	}
	if err := e.Validate(); err != nil {
		return ReasonInvalid, false
	}
	return "", true
}

// serveFailure emits an injected failure.
func (r *Receiver) serveFailure(w http.ResponseWriter, req *http.Request, mode FailMode, retryAfter time.Duration) {
	switch mode {
	case FailDrop:
		// Hijack and close without writing anything. The client sees a broken
		// connection and cannot tell whether the batch was committed, which is
		// exactly the ambiguity idempotency exists to resolve.
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijack unsupported", http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			http.Error(w, "hijack failed", http.StatusInternalServerError)
			return
		}
		_ = conn.Close()

	case Fail429:
		if retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(Response{
			Accepted:     []string{},
			RetryAfterMS: retryAfter.Milliseconds(),
		})

	default: // Fail503
		if retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(Response{
			Accepted:     []string{},
			RetryAfterMS: retryAfter.Milliseconds(),
		})
	}
}

// ---------------------------------------------------------------------------
// Failure injection
// ---------------------------------------------------------------------------

// FailNext makes the next n requests fail with mode. A negative n fails every
// request until Heal is called.
func (r *Receiver) FailNext(n int, mode FailMode, retryAfter time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failMode = mode
	r.failTimes = n
	r.retryAfter = retryAfter
}

// Heal clears any injected failure.
func (r *Receiver) Heal() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failMode = FailNone
	r.failTimes = 0
	r.retryAfter = 0
}

// takeFailure consumes one injected failure if any remain.
func (r *Receiver) takeFailure() (FailMode, time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failMode == FailNone || r.failTimes == 0 {
		return FailNone, 0, false
	}
	if r.failTimes > 0 {
		r.failTimes--
	}
	return r.failMode, r.retryAfter, true
}

// ---------------------------------------------------------------------------
// Accumulated state, for assertions
// ---------------------------------------------------------------------------

// Sessions returns everything stored, grouped by session and ordered by Seq.
//
// Ordering on Seq rather than on arrival is the whole point: retries and
// partial acceptance reorder the wire, so a server that presented events in
// arrival order would show a shuffled conversation whenever delivery was
// anything other than perfect.
func (r *Receiver) Sessions() map[string][]event.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := map[string][]event.Event{}
	for _, id := range r.arrival {
		e := r.stored[id]
		out[e.SessionID] = append(out[e.SessionID], e)
	}
	for _, evs := range out {
		sort.SliceStable(evs, func(i, j int) bool { return evs[i].Seq < evs[j].Seq })
	}
	return out
}

// Count is the number of distinct events stored.
func (r *Receiver) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.stored)
}

// Requests is the number of well-formed requests served, excluding injected
// failures.
func (r *Receiver) Requests() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.requests
}

// RejectedCount is the number of items refused.
func (r *Receiver) RejectedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rejected
}

// RedundantCount is the number of redeliveries absorbed. Non-zero is healthy:
// it means at-least-once delivery did its job and idempotency caught it.
func (r *Receiver) RedundantCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.redundant
}

// Has reports whether an id was stored.
func (r *Receiver) Has(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.stored[id]
	return ok
}

// Event returns a stored event by id.
func (r *Receiver) Event(id string) (event.Event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.stored[id]
	return e, ok
}

// IngestedAt reports when an id first landed. Separate from the event's own
// OccurredAt, which is when the thing happened.
func (r *Receiver) IngestedAt(id string) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.ingestedAt[id]
	return t, ok
}

// Raw returns the JSON of everything stored, for leak assertions that should
// not have to know the struct's shape.
func (r *Receiver) Raw() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	evs := make([]event.Event, 0, len(r.arrival))
	for _, id := range r.arrival {
		evs = append(evs, r.stored[id])
	}
	return json.Marshal(evs)
}

// ErrHTTP is returned by a Transport for a non-2xx response.
type ErrHTTP struct {
	Status     int
	RetryAfter time.Duration
}

func (e *ErrHTTP) Error() string {
	return fmt.Sprintf("receiver: http %d", e.Status)
}

// IsUnavailable reports whether err is a retryable server-side status.
func IsUnavailable(err error) bool {
	var he *ErrHTTP
	if !errors.As(err, &he) {
		return false
	}
	return he.Status == http.StatusServiceUnavailable || he.Status == http.StatusTooManyRequests
}
