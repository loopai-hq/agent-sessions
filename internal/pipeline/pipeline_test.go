package pipeline

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// Real byte counts: the spool guard has an absolute floor, so a toy 90-of-100
// would be refused as 90 bytes free.
func healthyDisk(string) (free, total uint64, err error) { return 500 << 30, 1 << 40, nil }

func newSink(t *testing.T) (*Sink, *spool.Spool) {
	t.Helper()
	sp, err := spool.Open(spool.Options{Dir: t.TempDir(), DiskFree: healthyDisk})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSink(sp)
	if err != nil {
		t.Fatal(err)
	}
	return s, sp
}

func ev(id string, at time.Time) event.Event {
	return event.Event{
		ID:         id,
		Source:     event.SourceClaudeCode,
		Origin:     event.OriginTranscript,
		Type:       event.UserPrompt,
		SessionID:  "s1",
		Seq:        1,
		OccurredAt: at,
		Text:       "hello",
	}
}

// The reason this package exists. A backfilled event carries the timestamp from
// the original transcript; if the sink drops it, the spool stamps import time
// instead and every historical event silently claims to have happened today.
func TestHistoricalEventTimeSurvivesTheSpool(t *testing.T) {
	s, sp := newSink(t)
	historical := time.Date(2026, 6, 1, 9, 30, 0, 0, time.UTC)

	if err := s.Put(ev("e1", historical)); err != nil {
		t.Fatal(err)
	}
	leased, err := sp.Lease(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("leased %d items, want 1", len(leased))
	}
	if !leased[0].Item.EventTime.Equal(historical) {
		t.Fatalf("spool item EventTime = %v, want the original %v; the adapter dropped it "+
			"and backfill's temporal accuracy is gone", leased[0].Item.EventTime, historical)
	}
}

func TestEventTimeIsNotOverwrittenForLiveCaptureEither(t *testing.T) {
	s, sp := newSink(t)
	captured := time.Now().Add(-3 * time.Minute).UTC().Truncate(time.Second)

	if err := s.Put(ev("e1", captured)); err != nil {
		t.Fatal(err)
	}
	leased, _ := sp.Lease(1)
	if got := leased[0].Item.EventTime.UTC().Truncate(time.Second); !got.Equal(captured) {
		t.Fatalf("EventTime = %v, want %v", got, captured)
	}
}

func TestIdentityFieldsReachTheSpool(t *testing.T) {
	s, sp := newSink(t)
	e := ev("e-abc", time.Now())
	e.Seq = 42
	if err := s.Put(e); err != nil {
		t.Fatal(err)
	}
	leased, _ := sp.Lease(1)
	it := leased[0].Item
	if it.ID != "e-abc" {
		t.Fatalf("id = %q; the server dedups on this, so losing it means duplicates", it.ID)
	}
	if it.SessionID != "s1" || it.Seq != 42 || it.Kind != "event" {
		t.Fatalf("item fields wrong: %+v", it)
	}
}

func TestPayloadRoundTripsToTheOriginalEvent(t *testing.T) {
	s, sp := newSink(t)
	orig := ev("e1", time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC))
	orig.Redactions = map[string]int{"anthropic_key": 2}
	if err := s.Put(orig); err != nil {
		t.Fatal(err)
	}
	leased, _ := sp.Lease(1)
	var got event.Event
	if err := json.Unmarshal(leased[0].Item.Payload, &got); err != nil {
		t.Fatalf("payload is not a valid event: %v", err)
	}
	if got.ID != orig.ID || got.Type != orig.Type || !got.OccurredAt.Equal(orig.OccurredAt) {
		t.Fatalf("event did not round-trip: %+v", got)
	}
	if got.Redactions["anthropic_key"] != 2 {
		t.Fatal("redaction counts lost in transit; a machine whose scrubber is firing " +
			"constantly would look clean")
	}
	if got.Origin != event.OriginTranscript {
		t.Fatalf("origin = %q; backfilled and live events must stay distinguishable", got.Origin)
	}
}

// An item with no idempotency key is redelivered after any crash and counted
// twice, so this fails loudly rather than inventing one.
func TestEventWithoutIDIsRejected(t *testing.T) {
	s, _ := newSink(t)
	e := ev("", time.Now())
	err := s.Put(e)
	if err == nil {
		t.Fatal("expected an event with no id to be rejected")
	}
}

func TestNilSpoolIsRejectedAtConstruction(t *testing.T) {
	if _, err := NewSink(nil); err == nil {
		t.Fatal("expected construction to fail without a spool")
	}
}

func TestEmitReturnsAUsableFunction(t *testing.T) {
	s, sp := newSink(t)
	emit := s.Emit()
	if err := emit(ev("e1", time.Now())); err != nil {
		t.Fatal(err)
	}
	if leased, _ := sp.Lease(1); len(leased) != 1 {
		t.Fatal("Emit did not reach the spool")
	}
}

func TestSpoolErrorsPropagate(t *testing.T) {
	// A full disk must surface to the caller, not be swallowed: the capture
	// layer decides what to do about it and the health report needs the count.
	sp, err := spool.Open(spool.Options{
		Dir:          t.TempDir(),
		MinFreeRatio: 0.5,
		DiskFree:     func(string) (uint64, uint64, error) { return 1, 100, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := NewSink(sp)
	if err := s.Put(ev("e1", time.Now())); err == nil {
		t.Fatal("expected the spool's disk refusal to reach the caller")
	}
}
