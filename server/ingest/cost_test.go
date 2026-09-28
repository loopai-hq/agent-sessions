package ingest

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

func closeEnough(got, want float64) bool {
	diff := got - want
	return diff < 1e-9 && diff > -1e-9
}

var jan2026 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestCostFollowsTheContractFormula checks the arithmetic in section 4 against
// numbers worked out by hand. Cache traffic dominates a real session, so the
// three multipliers are the part that decides whether a bill is right.
func TestCostFollowsTheContractFormula(t *testing.T) {
	p := NewPricer([]Rate{{
		Model:         "m",
		EffectiveFrom: jan2026,
		InputPerMTok:  15,
		OutputPerMTok: 75,
	}}, discardLogger())

	got := p.CostUSD("m", event.Usage{
		InputTokens:     1_000_000, // 15.00
		OutputTokens:    200_000,   // 15.00
		CacheReadTokens: 2_000_000, // 2 * 15 * 0.1  =  3.00
		Ephemeral5m:     1_000_000, // 1 * 15 * 1.25 = 18.75
		Ephemeral1h:     500_000,   // 0.5 * 15 * 2  = 15.00
	}, jan2026.AddDate(0, 1, 0))

	if want := 66.75; !closeEnough(got, want) {
		t.Fatalf("cost %v, want %v", got, want)
	}
}

// TestSyntheticModelCostsNothing keeps records that never became an API call out
// of the money. They are numerous, so including them would not look like an
// error; it would look like a larger bill.
func TestSyntheticModelCostsNothing(t *testing.T) {
	p := NewPricer([]Rate{{Model: SyntheticModel, EffectiveFrom: jan2026, InputPerMTok: 15}}, discardLogger())
	if got := p.CostUSD(SyntheticModel, event.Usage{InputTokens: 5_000_000}, jan2026); got != 0 {
		t.Fatalf("cost %v, want 0 even with a rate on file", got)
	}
	if got := p.CostUSD("", event.Usage{InputTokens: 5_000_000}, jan2026); got != 0 {
		t.Fatalf("unnamed model cost %v, want 0", got)
	}
}

// TestUnknownModelCostsNothingAndSaysSo prefers a visible zero to a guess: a
// fabricated rate would produce a figure that looks exactly as authoritative as
// a real one.
func TestUnknownModelCostsNothingAndSaysSo(t *testing.T) {
	log, logs := capturingLogger()
	p := NewPricer(nil, log)

	for range 3 {
		if got := p.CostUSD("nobody-priced-this", event.Usage{InputTokens: 1000}, jan2026); got != 0 {
			t.Fatalf("cost %v, want 0", got)
		}
	}
	if n := strings.Count(logs.String(), "no rate on file"); n != 1 {
		t.Fatalf("logged %d times, want exactly one line per model", n)
	}
}

// TestRatesAreEffectiveDated is why the rates are a table rather than constants:
// a price change must not rewrite what last quarter's work cost.
func TestRatesAreEffectiveDated(t *testing.T) {
	june := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	p := NewPricer([]Rate{
		{Model: "m", EffectiveFrom: june, InputPerMTok: 30},
		{Model: "m", EffectiveFrom: jan2026, InputPerMTok: 15},
	}, discardLogger())

	usage := event.Usage{InputTokens: 1_000_000}
	if got := p.CostUSD("m", usage, june.AddDate(0, -1, 0)); !closeEnough(got, 15) {
		t.Fatalf("call before the change cost %v, want 15", got)
	}
	if got := p.CostUSD("m", usage, june); !closeEnough(got, 30) {
		t.Fatalf("call at the change cost %v, want 30", got)
	}
	if got := p.CostUSD("m", usage, june.AddDate(0, 1, 0)); !closeEnough(got, 30) {
		t.Fatalf("call after the change cost %v, want 30", got)
	}
}

// TestCallOlderThanEveryRateTakesTheEarliest covers a backfill of history into a
// rate table seeded today. Reporting a quarter of real work as free is a worse
// answer than reporting it at the oldest price on file.
func TestCallOlderThanEveryRateTakesTheEarliest(t *testing.T) {
	p := NewPricer([]Rate{{Model: "m", EffectiveFrom: jan2026, InputPerMTok: 15}}, discardLogger())
	got := p.CostUSD("m", event.Usage{InputTokens: 1_000_000}, jan2026.AddDate(-1, 0, 0))
	if !closeEnough(got, 15) {
		t.Fatalf("cost %v, want 15", got)
	}
}

// TestUnsetMultipliersTakeTheContractDefaults reads a zero as an omission. Cache
// traffic is most of what a session costs, so the alternative reading
// under-reports by roughly an order of magnitude and does it silently.
func TestUnsetMultipliersTakeTheContractDefaults(t *testing.T) {
	p := NewPricer([]Rate{{Model: "m", EffectiveFrom: jan2026, InputPerMTok: 10}}, discardLogger())

	read := p.CostUSD("m", event.Usage{CacheReadTokens: 1_000_000}, jan2026)
	if !closeEnough(read, 10*DefaultCacheReadMultiplier) {
		t.Fatalf("cache read %v, want %v", read, 10*DefaultCacheReadMultiplier)
	}
	w5 := p.CostUSD("m", event.Usage{Ephemeral5m: 1_000_000}, jan2026)
	if !closeEnough(w5, 10*DefaultCacheWrite5mMultiplier) {
		t.Fatalf("5m write %v, want %v", w5, 10*DefaultCacheWrite5mMultiplier)
	}
	w1 := p.CostUSD("m", event.Usage{Ephemeral1h: 1_000_000}, jan2026)
	if !closeEnough(w1, 10*DefaultCacheWrite1hMultiplier) {
		t.Fatalf("1h write %v, want %v", w1, 10*DefaultCacheWrite1hMultiplier)
	}
}

// TestCacheCreationAggregateIsAFallback checks the two reports of the same
// tokens are never both counted.
func TestCacheCreationAggregateIsAFallback(t *testing.T) {
	p := NewPricer([]Rate{{Model: "m", EffectiveFrom: jan2026, InputPerMTok: 10}}, discardLogger())

	// No split available: the aggregate is priced, at the shorter TTL.
	only := p.CostUSD("m", event.Usage{CacheCreationTokens: 1_000_000}, jan2026)
	if !closeEnough(only, 10*DefaultCacheWrite5mMultiplier) {
		t.Fatalf("aggregate-only %v, want %v", only, 10*DefaultCacheWrite5mMultiplier)
	}

	// Split available: the aggregate is the same tokens again and is ignored.
	both := p.CostUSD("m", event.Usage{
		CacheCreationTokens: 1_000_000,
		Ephemeral5m:         1_000_000,
	}, jan2026)
	if !closeEnough(both, only) {
		t.Fatalf("cache creation counted twice: %v vs %v", both, only)
	}
}

// TestLoadReplacesRatesUnderConcurrentPricing is the shape the server runs in: a
// periodic refresh while uploads are being priced.
func TestLoadReplacesRatesUnderConcurrentPricing(t *testing.T) {
	p := NewPricer([]Rate{{Model: "m", EffectiveFrom: jan2026, InputPerMTok: 15}}, discardLogger())

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				p.CostUSD("m", event.Usage{InputTokens: 1_000_000}, jan2026)
			}
		}()
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				p.Load([]Rate{{Model: "m", EffectiveFrom: jan2026, InputPerMTok: 30}})
			}
		}()
	}
	wg.Wait()

	if got := p.CostUSD("m", event.Usage{InputTokens: 1_000_000}, jan2026); !closeEnough(got, 30) {
		t.Fatalf("cost after reload %v, want 30", got)
	}
}
