// Package pipeline wires the client's components into the chain they are
// always used in.
//
// Every deployment needs the same two adapters to connect capture to the spool
// and the spool to delivery. Both were being written independently at each call
// site, which is how a subtle bug spreads: the sink below has one line that must
// not be omitted, and omitting it silently destroys a property three packages
// worked to preserve.
//
// That line is EventTime. The spool stamps an item's EventTime with the current
// time when it is left zero, which is the right default for something being
// captured live. But a backfilled event carries the timestamp from the original
// transcript, and if the sink does not forward it, every historical event
// arrives stamped with the moment it was imported. The temporal accuracy that
// backfill goes to some trouble to preserve is then lost one layer below, in a
// three-line adapter nobody thinks to test.
package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/internal/spool"
)

// Sink adapts a spool to the interface capture and backfill emit into.
type Sink struct{ Spool *spool.Spool }

// NewSink builds a Sink.
func NewSink(s *spool.Spool) (*Sink, error) {
	if s == nil {
		return nil, fmt.Errorf("pipeline: spool is required")
	}
	return &Sink{Spool: s}, nil
}

// Put marshals an event and queues it for delivery.
func (s *Sink) Put(e event.Event) error {
	if e.ID == "" {
		// A spooled item without an idempotency key would be delivered again
		// after any crash and counted twice, so this is a hard failure rather
		// than something to paper over with a generated id.
		return fmt.Errorf("pipeline: event for session %q has no id", e.SessionID)
	}
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("pipeline: marshal event %s: %w", e.ID, err)
	}
	return s.Spool.Add(spool.Item{
		ID:        e.ID,
		Kind:      "event",
		SessionID: e.SessionID,
		Seq:       e.Seq,
		// Forwarding this is the whole reason the package exists. See the
		// package comment.
		EventTime: e.OccurredAt,
		Payload:   body,
	})
}

// Emit returns Put as a plain function, for callers that take one.
func (s *Sink) Emit() func(event.Event) error { return s.Put }
