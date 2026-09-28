package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
)

// memStore is a store that behaves the way the real one does, in memory.
//
// The properties this package's tests are about — a redelivery moving no
// counter, tokens credited once for a call that arrives as two events — are
// properties of the whole path rather than of the handler alone, so a fake that
// merely records what it was handed would leave them untested. This therefore
// reproduces the three mechanisms the storage layer uses: idempotency on the
// event id, credit through a ledger keyed on the identity of the model call, and
// a rollup folded forward from the rows a call actually inserted.
type memStore struct {
	pricer *Pricer

	mu       sync.Mutex
	events   map[string]Record
	ledger   map[string]bool
	owners   map[string]string
	sessions map[string]*rollup
	reports  []storedReport

	// upsertErr and healthErr stand in for a database that is unreachable,
	// deadlocked or out of connections: a transient condition with no per-item
	// meaning.
	upsertErr error
	healthErr error

	upserts int
}

// rollup is the derived per-session record, holding only what these tests
// assert on.
type rollup struct {
	Email      string
	Repo       string
	StartedAt  time.Time
	EndedAt    time.Time
	Ended      bool
	UserTurns  int
	ToolCalls  int
	Subagents  int
	Errors     int
	Input      int64
	Output     int64
	CacheRead  int64
	CacheWrite int64
	CostUSD    float64
	Redactions map[string]int
}

type storedReport struct {
	Email    string
	DeviceID string
	Report   health.Report
}

func newMemStore(rates []Rate) *memStore {
	return &memStore{
		pricer:   NewPricer(rates, discardLogger()),
		events:   map[string]Record{},
		ledger:   map[string]bool{},
		owners:   map[string]string{},
		sessions: map[string]*rollup{},
	}
}

func (m *memStore) UpsertEvents(_ context.Context, records []Record) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.upserts++
	if m.upsertErr != nil {
		// A failed transaction leaves nothing behind and reports no per-item
		// verdicts, which is what makes the handler's 5xx path meaningful.
		return Result{}, m.upsertErr
	}

	var res Result
	batch := map[string]bool{}
	for _, rec := range records {
		sid := rec.Event.SessionID
		owner, claimed := m.owners[sid]
		if !claimed {
			m.owners[sid] = rec.Email
			owner = rec.Email
		}
		if owner != rec.Email {
			res.Rejected = append(res.Rejected, Rejection{
				ID:     rec.Event.ID,
				Reason: "session belongs to another principal",
			})
			continue
		}
		if batch[rec.Event.ID] {
			res.Duplicate = append(res.Duplicate, rec.Event.ID)
			continue
		}
		batch[rec.Event.ID] = true
		if _, seen := m.events[rec.Event.ID]; seen {
			res.Duplicate = append(res.Duplicate, rec.Event.ID)
			continue
		}
		m.events[rec.Event.ID] = rec
		res.Inserted = append(res.Inserted, rec.Event.ID)
		m.fold(rec)
	}
	return res, nil
}

// fold applies one newly stored event to its session's rollup.
func (m *memStore) fold(rec Record) {
	s := m.sessions[rec.Event.SessionID]
	if s == nil {
		s = &rollup{Email: rec.Email, StartedAt: rec.Event.OccurredAt}
		m.sessions[rec.Event.SessionID] = s
	}
	if rec.Repo != "" {
		s.Repo = rec.Repo
	}
	if rec.Event.OccurredAt.Before(s.StartedAt) {
		s.StartedAt = rec.Event.OccurredAt
	}
	if rec.Event.OccurredAt.After(s.EndedAt) {
		s.EndedAt = rec.Event.OccurredAt
	}
	switch rec.Event.Type {
	case event.UserPrompt:
		s.UserTurns++
	case event.ToolCall:
		s.ToolCalls++
	case event.ToolFailed:
		s.Errors++
	case event.SubagentStart:
		s.Subagents++
	case event.SessionEnded:
		s.Ended = true
	}
	for k, v := range rec.Event.Redactions {
		if s.Redactions == nil {
			s.Redactions = map[string]int{}
		}
		s.Redactions[k] += v
	}

	u := rec.Event.Usage
	if u == nil || rec.Event.Model == SyntheticModel {
		return
	}
	// The ledger key is the identity of the model call, not of the event that
	// carried it. Two transcript records reporting one call are two events with
	// two ids, and event idempotency alone would count their tokens twice. The
	// key is the message id alone, as store.usageKey has it: a hook copy from
	// a client that did not stamp the request id and a walked copy that does
	// are one call.
	key := u.MessageID
	if u.MessageID == "" {
		key = "event:" + rec.Event.ID
	}
	if m.ledger[key] {
		return
	}
	m.ledger[key] = true

	s.Input += u.InputTokens
	s.Output += u.OutputTokens
	s.CacheRead += u.CacheReadTokens
	if write := u.Ephemeral5m + u.Ephemeral1h; write > 0 {
		s.CacheWrite += write
	} else {
		s.CacheWrite += u.CacheCreationTokens
	}
	s.CostUSD += m.pricer.CostUSD(rec.Event.Model, *u, rec.Event.OccurredAt)
}

func (m *memStore) PutHealthReport(_ context.Context, email, deviceID string, r health.Report) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.healthErr != nil {
		return m.healthErr
	}
	// Idempotent on (email, device, emitted_at), as the schema is: a redelivered
	// sample must not read as a second machine checking in.
	for _, existing := range m.reports {
		if existing.Email == email && existing.DeviceID == deviceID &&
			existing.Report.EmittedAt.Equal(r.EmittedAt) {
			return nil
		}
	}
	m.reports = append(m.reports, storedReport{Email: email, DeviceID: deviceID, Report: r})
	return nil
}

func (m *memStore) snapshot(sessionID string) rollup {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[sessionID]
	if s == nil {
		return rollup{}
	}
	out := *s
	out.Redactions = map[string]int{}
	for k, v := range s.Redactions {
		out.Redactions[k] += v
	}
	return out
}

func (m *memStore) stored(id string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.events[id]
	return rec, ok
}

func (m *memStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func (m *memStore) reportCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.reports)
}
