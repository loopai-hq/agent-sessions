package export

import (
	"context"
	"io"
	"time"

	"github.com/loopai-hq/agent-sessions/server/store"
)

// storeSource adapts the store's plain-typed export reads to Source. The
// store cannot name this package's types (it would be an import cycle), so
// the conversion lives on this side.
type storeSource struct {
	st *store.Store
}

// NewStoreSource wraps a store as a Source.
func NewStoreSource(st *store.Store) Source { return storeSource{st: st} }

func (s storeSource) Now(ctx context.Context) (time.Time, error) { return s.st.ExportNow(ctx) }

func (s storeSource) Watermarks(ctx context.Context) (Watermarks, error) {
	m, err := s.st.ExportWatermarks(ctx)
	if err != nil {
		return Watermarks{}, err
	}
	return Watermarks{
		Sessions: m[store.ExportWatermarkSessions],
		Events:   m[store.ExportWatermarkEvents],
		Health:   m[store.ExportWatermarkHealth],
	}, nil
}

func (s storeSource) Plan(ctx context.Context, req PlanRequest) (PlanReads, error) {
	reads, err := s.st.ExportPlan(ctx, store.ExportPlanRequest{
		SessionsSince: req.SessionsSince,
		SessionsLimit: req.SessionsLimit,
		EventsSince:   req.EventsSince,
		EventsLimit:   req.EventsLimit,
		HealthSince:   req.HealthSince,
	})
	if err != nil {
		return PlanReads{}, err
	}
	out := PlanReads{
		Sessions:          make([]TouchedSession, len(reads.Sessions)),
		EventDays:         toDays(reads.EventDays),
		HealthDays:        toDays(reads.HealthDays),
		HealthTableExists: reads.HealthTableExists,
	}
	for i, r := range reads.Sessions {
		out.Sessions[i] = TouchedSession{SessionID: r.SessionID, Email: r.Email, StartedAt: r.StartedAt, UpdatedAt: r.UpdatedAt}
	}
	return out, nil
}

func toDays(in []string) []Day {
	if in == nil {
		return nil
	}
	out := make([]Day, len(in))
	for i, d := range in {
		out[i] = Day(d)
	}
	return out
}

func (s storeSource) CopyPartition(ctx context.Context, table Table, day Day, w io.Writer) (int64, error) {
	return s.st.ExportCopyPartition(ctx, string(table), string(day), w)
}

func (s storeSource) CopyBundle(ctx context.Context, sessionID string, w io.Writer) (int64, error) {
	return s.st.ExportCopyBundle(ctx, sessionID, w)
}

func (s storeSource) AdvanceWatermarks(ctx context.Context, w Watermarks) error {
	// A zero stream (health before its table exists) writes no row; the
	// store skips it, so a missing position stays missing rather than
	// becoming the year 1.
	return s.st.ExportAdvanceWatermarks(ctx, map[string]time.Time{
		store.ExportWatermarkSessions: w.Sessions,
		store.ExportWatermarkEvents:   w.Events,
		store.ExportWatermarkHealth:   w.Health,
	})
}
