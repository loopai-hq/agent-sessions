package web

// Everything here is driven through the real assembled Server and the real
// templates, because the defect these tests exist for is a rendering defect: the
// numbers were always correct in storage and always wrong on the page.
//
// The fixtures are rows copied out of the deployed database on 2026-08-05, token
// counts and all. That matters more than it usually would. The production corpus
// is 891 sessions of which 351 carry tokens and a zero cost and 540 carry
// neither, and not one carries a cost above zero — so the case this product
// spent its whole life rendering is the case a made-up fixture would not have.

import (
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// Real rows, from `SELECT session_id, repo, tokens_*, cost_usd FROM sessions`
// against loop-sessions on 2026-08-05.
var (
	realUnpriced = Session{
		ID: "22f6577a-25d3-4955-826e-a76d94c1e219", Repo: "main",
		TokensInput: 186_042, TokensOutput: 1_445_787,
		TokensCacheRead: 886_928_221, TokensCacheWrite: 25_602_099,
		CostUSD: 0,
	}
	realUnpricedSmaller = Session{
		ID: "0d67bf89-82a5-44f6-93af-1ccab1149ecc", Repo: "ai_ingestion",
		TokensInput: 7_276, TokensOutput: 252_729,
		TokensCacheRead: 265_313_779, TokensCacheWrite: 11_062_713,
		CostUSD: 0,
	}
	realNoModelCalls = Session{
		ID:      "52d370b3-c1f3-4db5-b2f8-03d52265b52a",
		CostUSD: 0,
	}
)

// seedRow installs one production row under a viewer, with the timestamps the
// list needs to render at all.
func seedRow(f *fakeData, s Session, email string) {
	s.Email = email
	s.Source = "claude_code"
	s.StartedAt = fixedNow.Add(-time.Hour)
	s.EndedAt = fixedNow.Add(-30 * time.Minute)
	s.Ended = true
	f.sessions[s.ID] = SessionDetail{Session: s, Via: "own"}
}

// TestCostOfSeparatesTheThreeMeaningsOfZero is the rule the whole feature rests
// on. cost_usd = 0 is stored for a session nobody could price and for a session
// that made no model calls, and the figure alone cannot tell them apart.
func TestCostOfSeparatesTheThreeMeaningsOfZero(t *testing.T) {
	for _, tc := range []struct {
		name             string
		in               Session
		priced, billable bool
		unpriced         bool
	}{
		{
			name: "real row with tokens and no rate on file is unpriced, not free",
			in:   realUnpriced,
			// The state that covers 351 of the 891 deployed sessions.
			priced: false, billable: true, unpriced: true,
		},
		{
			name: "real row with no model calls spent nothing and is not a gap",
			in:   realNoModelCalls,
			// The state that covers the other 540.
			priced: false, billable: false, unpriced: false,
		},
		{
			name:   "a stored cost above zero is a price and renders as one",
			in:     Session{TokensInput: 186_042, CostUSD: 3378.938961},
			priced: true, billable: true, unpriced: false,
		},
		{
			name:   "cache traffic alone is billable, because it is most of a real bill",
			in:     Session{TokensCacheRead: 886_928_221},
			priced: false, billable: true, unpriced: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := costOf(tc.in)
			if got.Priced != tc.priced {
				t.Errorf("Priced = %v, want %v", got.Priced, tc.priced)
			}
			if got.Billable != tc.billable {
				t.Errorf("Billable = %v, want %v", got.Billable, tc.billable)
			}
			if got.Unpriced() != tc.unpriced {
				t.Errorf("Unpriced() = %v, want %v", got.Unpriced(), tc.unpriced)
			}
		})
	}
}

// TestListNeverPrintsMoneyForASessionItCannotPrice is the defect itself, driven
// through the mounted handler and the real template. A dollar figure on one of
// these rows is a number the reader has no way to know is fabricated.
func TestListNeverPrintsMoneyForASessionItCannotPrice(t *testing.T) {
	f := newFake()
	seedRow(f, realUnpriced, owner.Email)
	seedRow(f, realNoModelCalls, owner.Email)
	s := newServer(t, f, owner)

	body := get(t, s, "/sessions").Body.String()

	for _, forbidden := range []string{"$0.00", "$0.0000", "$0"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("the list rendered %q for a session with no rate on file", forbidden)
		}
	}
	if !strings.Contains(body, "unpriced") {
		t.Error("the list does not say the cost is unknown")
	}
}

// TestListShowsTokensForAnUnpricedSession keeps the honest half of the trade.
// Withholding the dollar figure is only an improvement if what is true survives:
// "1.2M in / 340k out" with no dollar column is useful, a blank row is not.
func TestListShowsTokensForAnUnpricedSession(t *testing.T) {
	f := newFake()
	seedRow(f, realUnpriced, owner.Email)
	s := newServer(t, f, owner)

	body := get(t, s, "/sessions").Body.String()

	// tokenLabel's compressed forms for this row's real counts.
	for _, want := range []string{"186.0k", "1.4M"} {
		if !strings.Contains(body, want) {
			t.Errorf("the list does not show %s; the tokens are what is left to show", want)
		}
	}
	// The exact counts stay reachable without a second page load.
	if !strings.Contains(body, "886,928,221 cache read") {
		t.Error("the exact token counts are not available on the row")
	}
}

// TestASessionThatMadeNoModelCallsIsNotReportedAsAGap keeps the notice
// proportionate. Most of the deployed corpus never called a model, and marking
// 540 sessions as missing a price would bury the 351 that are.
func TestASessionThatMadeNoModelCallsIsNotReportedAsAGap(t *testing.T) {
	f := newFake()
	seedRow(f, realNoModelCalls, owner.Email)
	s := newServer(t, f, owner)

	body := get(t, s, "/sessions").Body.String()
	if strings.Contains(body, "unpriced") {
		t.Error("a session that spent nothing was reported as one we could not price")
	}
}

// TestListStillPrintsARealPrice guards the other direction: the fix must not
// suppress the figure it was built to produce. This is the state the whole
// corpus moves into once the rate table reaches the ingest path.
func TestListStillPrintsARealPrice(t *testing.T) {
	f := newFake()
	priced := realUnpriced
	priced.CostUSD = 3378.938961 // what this corpus's fable-5 usage really cost
	seedRow(f, priced, owner.Email)
	s := newServer(t, f, owner)

	body := get(t, s, "/sessions").Body.String()
	if !strings.Contains(body, "$3,379") {
		t.Error("a priced session did not render its cost")
	}
	if strings.Contains(body, "unpriced") {
		t.Error("a priced session was reported as unpriced")
	}
}

// TestPageSummaryDoesNotPresentALowerBoundAsATotal is why the strip counts
// unpriced sessions out loud. Summing stored costs over a set that is mostly
// unpriced produces a number that is arithmetically correct and materially
// false, and the reader cannot see the difference unless it is stated.
func TestPageSummaryDoesNotPresentALowerBoundAsATotal(t *testing.T) {
	f := newFake()
	seedRow(f, realUnpriced, owner.Email)
	seedRow(f, realUnpricedSmaller, owner.Email)
	seedRow(f, realNoModelCalls, owner.Email)
	s := newServer(t, f, owner)

	body := get(t, s, "/sessions").Body.String()

	if !strings.Contains(body, "2</b> unpriced") {
		t.Error("the summary does not say how many of its sessions have no price")
	}
	if strings.Contains(body, "priced") && strings.Contains(body, "across 0 priced") {
		t.Error("the summary claimed a dollar total over sessions it could not price")
	}
	// Tokens are summed across every session on the page, priced or not:
	// 1,445,787 + 252,729 output, and 886,928,221 + 265,313,779 cache read.
	if !strings.Contains(body, "1.7M") {
		t.Error("the summary does not total the page's output tokens")
	}
	if !strings.Contains(body, "1.15B") {
		t.Error("the summary does not total the page's cache reads, which are most of the bill")
	}
}

// TestTokenLabelStaysReadableAtTheScaleThisCorpusReaches. Cache reads on the
// deployed fleet run to billions, and the compressed form exists so a reader
// does not have to count digits.
func TestTokenLabelStaysReadableAtTheScaleThisCorpusReaches(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int64
		want string
	}{
		{"nothing spent reads as nothing, not as an abbreviation", 0, "0"},
		{"small counts stay exact", 942, "942"},
		{"thousands", 186_042, "186.0k"},
		{"millions", 1_445_787, "1.4M"},
		{"one real session's cache reads", 886_928_221, "886.9M"},
		{"the whole corpus's cache reads", 2_660_701_568, "2.66B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tokenLabel(tc.in); got != tc.want {
				t.Errorf("tokenLabel(%d) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestOnlyAnAdminIsToldTheRateTableHasAGap. The notice asks somebody to add a
// row to model_prices; a member cannot, and telling them makes their own
// sessions look broken.
func TestOnlyAnAdminIsToldTheRateTableHasAGap(t *testing.T) {
	const notice = "missing a rate"
	for _, tc := range []struct {
		name string
		v    Viewer
		want bool
	}{
		{"an admin is shown the gap, because an admin can close it", admin, true},
		{"a member is not, because there is nothing they can do about it", owner, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			seedRow(f, realUnpriced, owner.Email)
			s := newServer(t, f, tc.v)

			body := get(t, s, "/sessions").Body.String()
			if got := strings.Contains(body, notice); got != tc.want {
				t.Errorf("gap notice shown = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDetailNamesTheModelItCouldNotPrice is what makes the gap closeable rather
// than permanent. "unpriced" with no model name tells an admin that something is
// missing and nothing about what to add.
func TestDetailNamesTheModelItCouldNotPrice(t *testing.T) {
	f := newFake()
	seedRow(f, realUnpriced, owner.Email)
	f.events[realUnpriced.ID] = []event.Event{
		{
			ID: "e1", SessionID: realUnpriced.ID, Seq: 1, Type: event.AssistantTurn,
			Source: event.SourceClaudeCode, Origin: event.OriginTranscript,
			OccurredAt: fixedNow, Model: "claude-fable-5",
			Usage: &event.Usage{InputTokens: 186_042, OutputTokens: 1_445_787},
		},
		{
			// A record that never became an API call. Naming it would send an
			// admin off to invent a price for something that is never charged.
			ID: "e2", SessionID: realUnpriced.ID, Seq: 2, Type: event.AssistantTurn,
			Source: event.SourceClaudeCode, Origin: event.OriginTranscript,
			OccurredAt: fixedNow, Model: syntheticModel,
			Usage: &event.Usage{InputTokens: 900},
		},
	}
	s := newServer(t, f, owner)

	body := get(t, s, "/sessions/"+realUnpriced.ID).Body.String()

	if !strings.Contains(body, "claude-fable-5") {
		t.Error("the detail page does not name the model that has no rate")
	}
	if strings.Contains(body, syntheticModel) {
		t.Error("a record that was never an API call was listed as awaiting a rate")
	}
	if !strings.Contains(body, "model_prices") {
		t.Error("the page does not say where the missing rate goes")
	}
	if strings.Contains(body, "$0.00") {
		t.Error("the detail page rendered a fabricated cost")
	}
}

// TestModelsUsedSkipsWhatWasNeverBilled keeps the caption to models that could
// actually need a rate.
func TestModelsUsedSkipsWhatWasNeverBilled(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []event.Event
		want []string
	}{
		{
			name: "a model call with usage is what needs a rate",
			in:   []event.Event{{Model: "claude-opus-5", Usage: &event.Usage{InputTokens: 1}}},
			want: []string{"claude-opus-5"},
		},
		{
			name: "an event with no usage was never billed and needs nothing",
			in:   []event.Event{{Model: "claude-opus-5"}},
			want: nil,
		},
		{
			name: "a synthetic record is never charged, so it is never missing a rate",
			in:   []event.Event{{Model: syntheticModel, Usage: &event.Usage{InputTokens: 1}}},
			want: nil,
		},
		{
			name: "each model is named once however many calls it made",
			in: []event.Event{
				{Model: "claude-fable-5", Usage: &event.Usage{InputTokens: 1}},
				{Model: "claude-fable-5", Usage: &event.Usage{InputTokens: 2}},
				{Model: "claude-opus-5", Usage: &event.Usage{InputTokens: 3}},
			},
			want: []string{"claude-fable-5", "claude-opus-5"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := modelsUsed(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("models %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("models %v, want %v", got, tc.want)
				}
			}
		})
	}
}
