package ingest

import (
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/loopai-hq/loop-sessions/internal/event"
)

// SyntheticModel is the model name a harness stamps on a transcript record that
// never became an API call. The record carries usage-shaped numbers that were
// never billed, so pricing it would inflate every cost figure in the product by
// an amount nobody could account for.
const SyntheticModel = "<synthetic>"

// The multipliers from section 4 of the contract, applied when a rate row leaves
// them unset.
//
// They are also the vendor's published figures, not a house convention:
// Anthropic quotes cache reads at 0.1x, five-minute cache writes at 1.25x and
// one-hour cache writes at 2x the base input rate
// (https://platform.claude.com/docs/en/about-claude/pricing, read 2026-08-05).
// Every rate seeded so far follows them, which is why the defaults exist at all;
// a vendor that ever quotes different ratios sets them explicitly on its rows.
//
// A zero multiplier is read as "not specified" rather than as "free". Cache
// traffic is the majority of what a real session costs, so a rate row written
// without its multipliers would under-report by roughly an order of magnitude,
// and it would do it silently. The price of that reading is that a genuinely
// zero-rated cache tier cannot be expressed; if one ever exists, it belongs in
// the rate row as an explicit flag rather than as a zero that looks like an
// omission.
const (
	DefaultCacheReadMultiplier    = 0.1
	DefaultCacheWrite5mMultiplier = 1.25
	DefaultCacheWrite1hMultiplier = 2.0
)

// Rate is one model's price from an effective moment onwards.
//
// Rates are dated rows rather than constants because a stored cost has to stay
// reproducible: a price change must not retroactively rewrite what last
// quarter's work cost, and a figure on a dashboard has to be defensible months
// after the fact.
type Rate struct {
	Model         string
	EffectiveFrom time.Time

	InputPerMTok  float64
	OutputPerMTok float64

	CacheReadMultiplier    float64
	CacheWrite5mMultiplier float64
	CacheWrite1hMultiplier float64
}

func (r Rate) withDefaults() Rate {
	if r.CacheReadMultiplier == 0 {
		r.CacheReadMultiplier = DefaultCacheReadMultiplier
	}
	if r.CacheWrite5mMultiplier == 0 {
		r.CacheWrite5mMultiplier = DefaultCacheWrite5mMultiplier
	}
	if r.CacheWrite1hMultiplier == 0 {
		r.CacheWrite1hMultiplier = DefaultCacheWrite1hMultiplier
	}
	return r
}

// Pricer turns token usage into dollars at the rates in force when the call
// happened.
//
// It is a value the storage layer is given rather than something it owns,
// because the two halves of "price every call exactly once" divide cleanly: the
// store decides which model calls are new, since only it can see what the usage
// ledger accepted, and this decides what a call costs. Keeping the rates here
// also means a price correction is a reload rather than a migration.
//
// Safe for concurrent use. Every upload prices through the same instance.
type Pricer struct {
	log *slog.Logger

	mu    sync.RWMutex
	rates map[string][]Rate
	// warned suppresses repeat logging for a model with no rate. Without it a
	// backfill of a fleet's history produces one line per event.
	warned map[string]bool
}

// NewPricer builds a Pricer over a rate table. The table may be empty; see
// CostUSD for what an unpriced model does.
func NewPricer(rates []Rate, log *slog.Logger) *Pricer {
	if log == nil {
		log = slog.Default()
	}
	p := &Pricer{log: log, warned: map[string]bool{}}
	p.Load(rates)
	return p
}

// Load replaces the rate table.
//
// A long-running server has to be able to pick up a price change without a
// deploy, and the replacement is wholesale rather than incremental so that a
// deleted row actually disappears. Costs already stored are untouched: they were
// computed at the rates in force at the time, which is the entire point of
// storing them.
func (p *Pricer) Load(rates []Rate) {
	byModel := make(map[string][]Rate, len(rates))
	for _, r := range rates {
		if r.Model == "" {
			continue
		}
		byModel[r.Model] = append(byModel[r.Model], r.withDefaults())
	}
	for _, rs := range byModel {
		// Ascending, so the applicable rate is the last one at or before the call.
		sort.Slice(rs, func(i, j int) bool { return rs[i].EffectiveFrom.Before(rs[j].EffectiveFrom) })
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.rates = byModel
	// Forget the warnings too: a model that was unpriced a moment ago may be
	// priced now, and if it is still not, the operator should hear about it again.
	p.warned = map[string]bool{}
}

// CostUSD prices one model call, following section 4 of the contract:
//
//	cost = input*rate_in + output*rate_out
//	     + cache_read*rate_in*0.1
//	     + cache_write_5m*rate_in*1.25
//	     + cache_write_1h*rate_in*2.0
//
// The synthetic model prices at zero. Those records carry usage-shaped numbers
// for calls that never happened, and they are numerous enough that including
// them would not look like an error, just like a larger bill.
//
// A model with no rate also returns zero, and says so in the log. The
// alternatives are worse: refusing the event would discard real work over a
// missing configuration row, and guessing a rate would produce a figure that
// looks exactly as authoritative as a real one.
//
// That zero is not a price, and this is the one thing a caller has to carry
// forward. It is stored in sessions.cost_usd next to zeroes that mean "nothing
// was billed", so by the time it reaches a reader the two are the same number
// and only the token counts beside it can tell them apart: a row with tokens and
// a zero cost was never priced, and a row with no tokens never spent anything.
// Rendering the first as $0.00 states a figure nobody can check, which is worse
// than stating none — see costOf in server/web/sessions.go, which is where the
// product makes that distinction and the only place that should.
func (p *Pricer) CostUSD(model string, u event.Usage, at time.Time) float64 {
	if model == "" || model == SyntheticModel {
		return 0
	}
	rate, ok := p.rateAt(model, at)
	if !ok {
		p.warnUnpriced(model)
		return 0
	}

	// Rates are quoted per million tokens, which is how every vendor publishes
	// them, so the conversion happens once here rather than in the table.
	in := rate.InputPerMTok / 1e6
	out := rate.OutputPerMTok / 1e6

	cost := float64(u.InputTokens)*in + float64(u.OutputTokens)*out
	cost += float64(u.CacheReadTokens) * in * rate.CacheReadMultiplier

	// The per-TTL split and the aggregate are two reports of the same tokens, so
	// taking both would double them. The split is preferred because it is what
	// prices correctly; the aggregate is the fallback for a harness that does not
	// break it down, and it is priced at the shorter TTL because that is the
	// cheaper of the two and over-charging somebody's team for a number we had to
	// guess at is the worse mistake.
	write5m, write1h := u.Ephemeral5m, u.Ephemeral1h
	if write5m == 0 && write1h == 0 {
		write5m = u.CacheCreationTokens
	}
	cost += float64(write5m) * in * rate.CacheWrite5mMultiplier
	cost += float64(write1h) * in * rate.CacheWrite1hMultiplier
	return cost
}

// rateAt returns the rate in force for a model at a moment.
//
// A call older than every rate on file takes the earliest one rather than
// nothing. That happens on any backfill of history into a rate table seeded
// today, and reporting a quarter of real work as costing zero dollars is a
// worse answer than reporting it at the oldest price we know about.
func (p *Pricer) rateAt(model string, at time.Time) (Rate, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	rates := p.rates[model]
	if len(rates) == 0 {
		return Rate{}, false
	}
	if at.IsZero() {
		at = time.Now()
	}
	// Ascending order, so the last row at or before the call is the one in force.
	idx := sort.Search(len(rates), func(i int) bool { return rates[i].EffectiveFrom.After(at) }) - 1
	if idx < 0 {
		return rates[0], true
	}
	return rates[idx], true
}

// warnUnpriced logs a model with no rate, once.
//
// The message names the fix rather than the symptom because this line is the
// only notice anyone gets before the gap becomes permanent: the cost is computed
// once at ingest and stored, so every session that lands while the rate is
// missing keeps reporting no cost even after the rate arrives.
func (p *Pricer) warnUnpriced(model string) {
	p.mu.Lock()
	if p.warned[model] {
		p.mu.Unlock()
		return
	}
	p.warned[model] = true
	p.mu.Unlock()

	p.log.Warn("no rate on file for model; its sessions will report tokens with no cost until one is added to model_prices",
		slog.String("model", model))
}
