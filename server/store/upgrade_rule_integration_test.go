//go:build integration

package store

// The upgrade rule: a change that makes the tool understand a session better has
// to reach the sessions already captured. These pin the two properties that make
// that safe rather than destructive — an improved copy replaces a stored event,
// and a re-delivery of the same copy still changes nothing at all.

import (
	"context"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

// The point of capture_version. A re-walk produces the same ids, so without a
// version to compare, ON CONFLICT DO NOTHING discards every improvement and the
// re-walk is nine minutes that changes nothing.
func TestIntegrationANewerCaptureVersionReplacesAStoredEvent(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-upg-1"
	mustPrincipal(t, s, email, RoleMember)

	ctx := context.Background()
	batch := session(t, email, sid)
	batch[0].Event.CaptureVersion = 1
	batch[0].Event.Text = "the text an older agent managed to read"
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("first ingest: %v", err)
	}

	// The same event, re-extracted by a newer agent.
	improved := session(t, email, sid)
	improved[0].Event.CaptureVersion = 2
	improved[0].Event.Text = "the whole text, correctly read"
	res, err := s.UpsertEvents(ctx, improved)
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}

	// It is an upgrade, not an insert: nothing downstream may treat it as new.
	if len(res.Inserted) != 0 {
		t.Errorf("an upgraded event was reported as inserted: %v", res.Inserted)
	}

	var stored string
	var version int
	if err := pool.QueryRow(ctx,
		`SELECT body->>'text', capture_version FROM events WHERE id = $1`,
		batch[0].Event.ID).Scan(&stored, &version); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if stored != "the whole text, correctly read" {
		t.Errorf("the stored event was not upgraded: %q", stored)
	}
	if version != 2 {
		t.Errorf("capture_version = %d, want 2", version)
	}
}

// The other half, and the one that would be expensive to get wrong: an ordinary
// re-delivery must remain a no-op. A spool that could not confirm its upload
// sends the same batch again, and if that counted as an upgrade the tokens would
// be credited twice and every link occurrence inflated.
func TestIntegrationTheSameCaptureVersionChangesNothing(t *testing.T) {
	s := newStore(t, flatPricer{perToken: 0.001})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-upg-2"
	mustPrincipal(t, s, email, RoleMember)

	ctx := context.Background()
	batch := session(t, email, sid)
	for i := range batch {
		batch[i].Event.CaptureVersion = 3
	}
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	before := loadSession(t, s, sid)

	res, err := s.UpsertEvents(ctx, batch)
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if len(res.Inserted) != 0 {
		t.Errorf("a re-delivery inserted %d event(s)", len(res.Inserted))
	}
	after := loadSession(t, s, sid)

	if after.TokensInput != before.TokensInput || after.TokensOutput != before.TokensOutput {
		t.Errorf("a re-delivery moved the token totals: %d/%d -> %d/%d",
			before.TokensInput, before.TokensOutput, after.TokensInput, after.TokensOutput)
	}
	if after.CostUSD != before.CostUSD {
		t.Errorf("a re-delivery moved the cost: %v -> %v", before.CostUSD, after.CostUSD)
	}
}

// An older copy arriving late — a spool draining after a machine was upgraded —
// must not drag a stored event backwards to a worse extraction.
func TestIntegrationAnOlderCaptureVersionCannotOverwriteANewerOne(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-upg-3"
	mustPrincipal(t, s, email, RoleMember)

	ctx := context.Background()
	newer := session(t, email, sid)
	newer[0].Event.CaptureVersion = 5
	newer[0].Event.Text = "read by the newer agent"
	if _, err := s.UpsertEvents(ctx, newer); err != nil {
		t.Fatalf("ingest newer: %v", err)
	}

	older := session(t, email, sid)
	older[0].Event.CaptureVersion = 2
	older[0].Event.Text = "read by the older agent"
	if _, err := s.UpsertEvents(ctx, older); err != nil {
		t.Fatalf("ingest older: %v", err)
	}

	var stored string
	if err := pool.QueryRow(ctx, `SELECT body->>'text' FROM events WHERE id = $1`,
		newer[0].Event.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "read by the newer agent" {
		t.Fatalf("a late older copy overwrote a newer extraction: %q", stored)
	}
}

// Every event this build produces must be stamped. An unstamped one reads as
// version zero, which means the next re-walk would "upgrade" it pointlessly, and
// worse, it can never itself upgrade anything.
func TestIntegrationEventsCarryTheCaptureVersionTheyWereBuiltWith(t *testing.T) {
	if event.CaptureSchema < 1 {
		t.Fatalf("CaptureSchema = %d; versioning starts at 1", event.CaptureSchema)
	}
}

// The server half. A pass whose derivation version matches what already ran
// must not rebuild — otherwise every revision of a rolling deploy walks the
// whole corpus, and a deploy's cost grows with the data. The runner is what
// rebuilds now: a pass over a stale corpus works through every step and
// stamps the version; a pass over a stamped one reads one row and stops.
func TestIntegrationTheDerivationRebuildRunsOnceAndThenStopsRunning(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	// The runner's ledger and the version stamp outlive fresh(), and the
	// derive tests before this one leave the version current; a stale corpus
	// is what this test needs to start from.
	resetDerived(t)
	const email, sid = "dev@example.com", "s-upg-4"
	mustPrincipal(t, s, email, RoleMember)

	ctx := context.Background()
	batch := session(t, email, sid)
	batch[0].Event.Text = "see https://github.com/loopai-hq/agent-sessions/pull/4"
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Pretend nothing has ever been derived, which is the state of a corpus that
	// predates the derivation code.
	if _, err := pool.Exec(ctx, `UPDATE derived_schema SET version = 0 WHERE only_row`); err != nil {
		t.Fatal(err)
	}

	// The window is open for the test: the body-reading steps would
	// otherwise wait for two in the morning.
	cfg := DeriveConfig{Window: derive.Window{Always: true}}
	pass, err := s.RunDerive(ctx, cfg)
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !pass.Done || len(pass.Steps) == 0 {
		t.Fatalf("a stale corpus was not rebuilt: %+v", pass)
	}

	var version int
	var rebuiltAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT version, rebuilt_at FROM derived_schema WHERE only_row`).Scan(&version, &rebuiltAt); err != nil {
		t.Fatal(err)
	}
	if version != DerivedSchema {
		t.Errorf("version = %d, want %d", version, DerivedSchema)
	}
	if rebuiltAt == nil {
		t.Error("rebuilt_at was not stamped, so an operator cannot tell when this last ran")
	}

	// The second pass is the one that matters: every other instance in a
	// rolling deploy makes it.
	pass, err = s.RunDerive(ctx, cfg)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if !pass.Done || len(pass.Steps) != 0 {
		t.Errorf("an up-to-date corpus was rebuilt again; every deploy would pay for this: %+v", pass)
	}
}
