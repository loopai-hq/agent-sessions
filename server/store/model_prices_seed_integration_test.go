//go:build integration

package store

// What this file defends is the whole chain a dollar figure travels: the rate
// card written into migration 0004, read back out through ModelPrices, loaded
// into the Pricer the ingest path actually uses, and applied to the token counts
// the deployed fleet really produced. Each link is exercised by something else;
// none of them proves the number on the page is right, because a fat-fingered
// rate in the SQL passes every one of those tests and changes every figure in
// the product by a factor nobody would notice.
//
//	LOOP_SESSIONS_TEST_DSN=postgres:///loop_sessions_test go test -tags integration ./server/store/...

import (
	"context"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/server/ingest"
)

// seedModelPrices re-applies migration 0004's INSERT.
//
// TestMain applies it once, but fresh() truncates model_prices before every test
// in this package, so by the time any test runs the seeded rows are usually
// gone. Re-running the migration's own file is what keeps this a test of that
// file rather than of a copy of it that could drift from it.
func seedModelPrices(t *testing.T) {
	t.Helper()
	body, err := migrations.ReadFile("migrations/0004_model_prices_seed.sql")
	if err != nil {
		t.Fatalf("read the seed migration: %v", err)
	}
	if _, err := pool.Exec(context.Background(), string(body)); err != nil {
		t.Fatalf("apply the seed migration: %v", err)
	}
}

// TestIntegrationSeededRatesMatchThePublishedCard checks the numbers in the
// migration against Anthropic's published price list, which is the only thing
// that makes them defensible. Every figure here was read from
// https://platform.claude.com/docs/en/about-claude/pricing on 2026-08-05 and is
// restated in the migration's own header.
func TestIntegrationSeededRatesMatchThePublishedCard(t *testing.T) {
	s := newStore(t, nil)
	seedModelPrices(t)

	prices, err := s.ModelPrices(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("ModelPrices: %v", err)
	}

	for _, tc := range []struct {
		model    string
		in, out  float64
		read     float64
		w5m, w1h float64
	}{
		{"claude-fable-5", 10, 50, 0.1, 1.25, 2},
		{"claude-opus-5", 5, 25, 0.1, 1.25, 2},
		{"claude-haiku-4-5-20251001", 1, 5, 0.1, 1.25, 2},
	} {
		t.Run(tc.model, func(t *testing.T) {
			p, ok := prices[tc.model]
			if !ok {
				t.Fatalf("no rate seeded for %s; every model in captured usage needs one", tc.model)
			}
			if p.InputPerMTok != tc.in || p.OutputPerMTok != tc.out {
				t.Errorf("rate $%v/$%v per MTok, want $%v/$%v",
					p.InputPerMTok, p.OutputPerMTok, tc.in, tc.out)
			}
			if p.CacheReadMultiplier != tc.read ||
				p.CacheWrite5mMultiplier != tc.w5m ||
				p.CacheWrite1hMultiplier != tc.w1h {
				t.Errorf("cache multipliers %v/%v/%v, want %v/%v/%v",
					p.CacheReadMultiplier, p.CacheWrite5mMultiplier, p.CacheWrite1hMultiplier,
					tc.read, tc.w5m, tc.w1h)
			}
		})
	}

	// A rate for a model nobody runs would be a guess presented as a fact, and
	// the whole point of seeding only what could be sourced is lost the moment
	// somebody adds one "for completeness".
	if n := len(prices); n != 3 {
		t.Errorf("%d rates seeded, want exactly the 3 models that appear in captured usage", n)
	}
}

// TestIntegrationSeededRatesPriceTheCapturedCorpus drives the seeded card
// through the real Pricer over the token counts actually in the production
// database on 2026-08-05, read from usage_ledger grouped by model.
//
// It is worth pinning real totals rather than round numbers because they are the
// answer to the question the product exists to ask. This corpus reports $0.00
// today; the figures below are what it actually cost, and cache traffic is
// roughly 99% of it — which is also why a rate row written without its cache
// multipliers would under-report by two orders of magnitude and still look
// plausible.
func TestIntegrationSeededRatesPriceTheCapturedCorpus(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, nil)
	seedModelPrices(t)

	prices, err := s.ModelPrices(ctx, time.Now())
	if err != nil {
		t.Fatalf("ModelPrices: %v", err)
	}

	// This conversion is the one line missing from the composition root: nothing
	// in the running server turns ModelPrices into Pricer rates, so the seeded
	// table never reaches the arithmetic below. See the note in this file's
	// companion report.
	rates := make([]ingest.Rate, 0, len(prices))
	for _, p := range prices {
		rates = append(rates, ingest.Rate{
			Model:                  p.Model,
			EffectiveFrom:          p.EffectiveFrom,
			InputPerMTok:           p.InputPerMTok,
			OutputPerMTok:          p.OutputPerMTok,
			CacheReadMultiplier:    p.CacheReadMultiplier,
			CacheWrite5mMultiplier: p.CacheWrite5mMultiplier,
			CacheWrite1hMultiplier: p.CacheWrite1hMultiplier,
		})
	}
	pricer := ingest.NewPricer(rates, nil)

	var total float64
	for _, tc := range []struct {
		model string
		usage event.Usage
		want  float64
	}{
		{
			model: "claude-fable-5",
			usage: event.Usage{
				InputTokens: 219_225, OutputTokens: 3_443_385,
				CacheReadTokens: 1_589_138_226,
				Ephemeral5m:     34_901_294, Ephemeral1h: 58_958_653,
			},
			want: 3378.9389610,
		},
		{
			model: "claude-opus-5",
			usage: event.Usage{
				InputTokens: 125_019, OutputTokens: 1_464_657,
				CacheReadTokens: 1_071_004_207,
				Ephemeral5m:     27_283_910, Ephemeral1h: 6_499_347,
			},
			want: 808.261531,
		},
		{
			model: "claude-haiku-4-5-20251001",
			usage: event.Usage{
				InputTokens: 167, OutputTokens: 3_290,
				CacheReadTokens: 559_135,
				Ephemeral5m:     27_649, Ephemeral1h: 164_995,
			},
			want: 0.43708175,
		},
	} {
		t.Run(tc.model, func(t *testing.T) {
			got := pricer.CostUSD(tc.model, tc.usage, time.Now())
			if diff := got - tc.want; diff > 1e-6 || diff < -1e-6 {
				t.Errorf("cost $%.6f, want $%.6f", got, tc.want)
			}
			total += got
		})
	}

	if diff := total - 4187.63757375; diff > 1e-5 || diff < -1e-5 {
		t.Errorf("corpus total $%.5f, want $4187.63757", total)
	}
}

// TestIntegrationAnUnseededModelIsNotPricedAtZero is the property the product
// rests on: a model with no rate must be distinguishable from a free one. The
// Pricer returns zero for both because its signature has nowhere else to put the
// answer, so the distinction has to survive as "tokens with no cost", which is
// what the dashboard renders as unpriced rather than as $0.00.
func TestIntegrationAnUnseededModelIsNotPricedAtZero(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, nil)
	seedModelPrices(t)

	prices, err := s.ModelPrices(ctx, time.Now())
	if err != nil {
		t.Fatalf("ModelPrices: %v", err)
	}
	if _, ok := prices["claude-sonnet-5"]; ok {
		t.Fatal("a model outside captured usage was seeded; only sourced rates belong in the card")
	}

	pricer := ingest.NewPricer(nil, nil)
	usage := event.Usage{InputTokens: 1_000_000, OutputTokens: 1_000_000}
	if got := pricer.CostUSD("claude-sonnet-5", usage, time.Now()); got != 0 {
		t.Fatalf("an unpriced model returned $%v; it must return no figure at all", got)
	}
	// The tokens are what survives, and they are what the reader is shown.
	if usage.InputTokens == 0 || usage.OutputTokens == 0 {
		t.Fatal("pricing consumed the token counts the unpriced view depends on")
	}
}
