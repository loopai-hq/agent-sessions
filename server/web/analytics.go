package web

import (
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The analytics page: what this corpus cost and who spent it.
//
// The layout follows the shape every usage dashboard converges on, because the
// questions converge: a time range, a small row of totals, activity over time,
// and a per-person breakdown. Anything cleverer than that answers questions
// nobody asked with pixels somebody has to scan past.
//
// Visibility is the store's: an admin aggregates the fleet, a member sees
// exactly their own numbers on the same page. The page never has to know which
// it is showing.

type analyticsView struct {
	Page  Page
	Range string
	From  time.Time
	To    time.Time

	// Totals over the whole range.
	Sessions   int64
	People     int64
	TokensIn   int64
	TokensOut  int64
	CacheRead  int64
	CostUSD    float64
	ToolCalls  int64
	Errors     int64
	AvgTokens  int64
	AnyUnknown bool

	// Days backs the charts; each entry is one bucket. The bucket width is
	// chosen per range so the count stays small and fixed — at most 24 — and
	// the chart fills the page at every range instead of scrolling: hours for a
	// day, days for a week, two-day chunks for a month, weeks for a quarter.
	Days          []DayUsage
	BucketKind    string
	BucketNoun    string
	SessionsChart ChartView
	TokensChart   ChartView
	CostChart     ChartView

	// TokenTypes and PersonTypes are the session-type selections for the
	// tokens chart and the per-person component, resolved and never empty; the
	// full set means "all", which is these panels' default. The sessions and
	// cost charts take no selection: they draw every type as a stack, and a
	// filtered stack is a shorter bar with a missing explanation.
	TokenTypes  []string
	PersonTypes []string

	// The per-person chart, drawn for the table's top rows. It is part of the
	// same component as the table: row checkboxes show and hide its lines with
	// no round trip, which under script-src 'none' is done by CSS :has() over
	// pre-rendered lines rather than by script. The server therefore always
	// renders every line; the stylesheet decides visibility.
	PersonChart  ChartView
	ChartPeople  []PersonUsage
	PersonMetric string

	// The per-person table.
	Rows          []PersonUsage
	Sort          string
	Top           int
	Q             string
	PeopleOptions []string
}

// analyticsRanges are the offered windows, in display order. Fixed rather than
// arbitrary dates because the questions these answer are comparative — "was this
// week normal" — and comparability needs everyone looking at the same windows.
var analyticsRanges = []rangeDef{
	{"1d", "Today", 1, "hour", time.Hour, 24, "hour"},
	{"7d", "7 days", 7, "day", 24 * time.Hour, 7, "day"},
	{"30d", "30 days", 30, "day", 48 * time.Hour, 15, "2 days"},
	// 91 days, not 90: thirteen weekly buckets tile 91 exactly, and a window
	// one day short of its buckets left the newest week permanently one day
	// underweight — a ~14%% terminal dip that read as a real decline.
	{"90d", "90 days", 91, "day", 7 * 24 * time.Hour, 13, "week"},
}

// rangeDef is one offered window and the bucketing that keeps its chart
// readable: the segment count is fixed per range, and the segment width is the
// range divided by it, so every range fills the same page width.
type rangeDef struct {
	Key   string
	Label string
	Days  int
	// Unit is what the store truncates to; Step and Count shape the buckets
	// client-side (a "2 days" bucket is two day-rows folded together).
	Unit  string
	Step  time.Duration
	Count int
	// Noun is what the captions call one bucket.
	Noun string
}

// Kind names the bucket for chart captions.
func (r rangeDef) Kind() string {
	switch {
	case r.Unit == "hour":
		return "hour"
	case r.Step == 7*24*time.Hour:
		return "week"
	case r.Step == 48*time.Hour:
		return "2d"
	default:
		return "day"
	}
}

// Ranges exposes the range definitions to the template.
func (analyticsView) Ranges() []rangeDef { return analyticsRanges }

// RangeURL rebuilds the page URL with a different range, keeping every other
// filter, so switching windows does not silently drop the person somebody had
// searched for.
func (v analyticsView) RangeURL(key string) string {
	return analyticsURL(key, v.Sort, v.Q, v.Top, v.PersonMetric, v.TokenTypes, v.PersonTypes)
}

// TokenTypeURL toggles one session type on the tokens chart. The fragment
// brings the reload back to the panel the menu is in.
func (v analyticsView) TokenTypeURL(t string) string {
	tt := toggleType(v.TokenTypes, t, sessionTypeOptions)
	return analyticsURL(v.Range, v.Sort, v.Q, v.Top, v.PersonMetric, tt, v.PersonTypes) + "#tokens"
}

// PersonTypeURL toggles one session type on the per-person component.
func (v analyticsView) PersonTypeURL(t string) string {
	pt := toggleType(v.PersonTypes, t, sessionTypeOptions)
	return analyticsURL(v.Range, v.Sort, v.Q, v.Top, v.PersonMetric, v.TokenTypes, pt) + "#viewing"
}

// TokenTypeChecked and PersonTypeChecked report a type's place in each panel's
// selection, for the menu checkmarks.
func (v analyticsView) TokenTypeChecked(t string) bool  { return containsType(v.TokenTypes, t) }
func (v analyticsView) PersonTypeChecked(t string) bool { return containsType(v.PersonTypes, t) }

// TypeOptions exposes the offered session types to the template.
func (analyticsView) TypeOptions() []string { return sessionTypeOptions }

// TypeName labels a session type for legends and menus.
func (analyticsView) TypeName(t string) string { return SessionTypeLabel(t) }

// TypeMenuLabel names a selection for a closed menu.
func typeMenuLabel(sel []string) string {
	if len(sel) == len(sessionTypeOptions) {
		return "all types"
	}
	var names []string
	for _, t := range sel {
		names = append(names, SessionTypeLabel(t))
	}
	return strings.Join(names, ", ")
}

// TokenTypeLabel and PersonTypeLabel are the closed-menu captions.
func (v analyticsView) TokenTypeLabel() string  { return typeMenuLabel(v.TokenTypes) }
func (v analyticsView) PersonTypeLabel() string { return typeMenuLabel(v.PersonTypes) }

// SortURL rebuilds the URL sorted by key. The fragment brings the reload back
// to the component the click was in: a sort click that lands the reader at the
// top of the page costs them the very table they were sorting.
func (v analyticsView) SortURL(key string) string {
	return analyticsURL(v.Range, key, v.Q, v.Top, v.PersonMetric, v.TokenTypes, v.PersonTypes) + "#viewing"
}

// MetricURL rebuilds the URL with the person chart drawn by a different metric.
func (v analyticsView) MetricURL(key string) string {
	return analyticsURL(v.Range, v.Sort, v.Q, v.Top, key, v.TokenTypes, v.PersonTypes) + "#viewing"
}

// TopURL rebuilds the URL listing n people, applied on click like every other
// selection on the page.
func (v analyticsView) TopURL(n int) string {
	return analyticsURL(v.Range, v.Sort, v.Q, n, v.PersonMetric, v.TokenTypes, v.PersonTypes) + "#viewing"
}

// ClearURL drops the person filter and nothing else. It exists because the
// first Clear rendered RangeURL, which preserves Q — a Clear that keeps the
// very filter it clears is a button-shaped no-op — and because it lives inside
// the Viewing panel, so it carries the fragment back there.
func (v analyticsView) ClearURL() string {
	return analyticsURL(v.Range, v.Sort, "", v.Top, v.PersonMetric, v.TokenTypes, v.PersonTypes) + "#viewing"
}

// PersonURL applies a person filter from the combo panel, keeping every other
// knob — including a non-default top, which the hand-built href silently reset.
func (v analyticsView) PersonURL(q string) string {
	return analyticsURL(v.Range, v.Sort, q, v.Top, v.PersonMetric, v.TokenTypes, v.PersonTypes) + "#viewing"
}

// TopChoices are the offered table sizes.
func (analyticsView) TopChoices() []int { return []int{10, 25, 50, 100} }

// FailurePct renders the tool failure rate over the range.
func (v analyticsView) FailurePct() string {
	if v.ToolCalls == 0 {
		return "0%"
	}
	return strconv.FormatFloat(float64(v.Errors)/float64(v.ToolCalls)*100, 'f', 1, 64) + "%"
}

func analyticsURL(rng, sortKey, q string, top int, metric string, tt, pt []string) string {
	var parts []string
	if rng != "" && rng != "7d" {
		parts = append(parts, "range="+rng)
	}
	if sortKey != "" && sortKey != "tokens" {
		parts = append(parts, "sort="+sortKey)
	}
	if q != "" {
		parts = append(parts, "q="+template.URLQueryEscaper(q))
	}
	if top != 0 && top != 25 {
		parts = append(parts, "top="+strconv.Itoa(top))
	}
	if metric != "" && metric != "tokens" {
		parts = append(parts, "metric="+metric)
	}
	// The full set is these panels' default and stays out of the URL.
	if len(tt) > 0 && !typesEqual(tt, sessionTypeOptions) {
		parts = append(parts, "tt="+strings.Join(tt, ","))
	}
	if len(pt) > 0 && !typesEqual(pt, sessionTypeOptions) {
		parts = append(parts, "pt="+strings.Join(pt, ","))
	}
	if len(parts) == 0 {
		return "/analytics"
	}
	return "/analytics?" + strings.Join(parts, "&")
}

// personTypesParam turns a resolved selection into a store parameter. The
// selection is sent as it stands, the full set included: an empty list means
// the store's own default, which is narrower than "all types", and a panel
// whose menu says all types must not quietly show fewer.
func personTypesParam(sel []string) []string {
	return append([]string(nil), sel...)
}

// chartTypeColor maps a session-type index to the cost chart's palette. The
// sessions chart uses type order directly; cost keeps its historical brown
// for the dominant series and its historical second colour for automation,
// so the page's colour language survives the split, and the two classes the
// lattice added take palette entries of their own rather than sharing one,
// which a stacked bar cannot read.
func chartTypeColor(ti int) int {
	switch ti {
	case 0:
		return 2
	case 1:
		return 3
	case 2:
		return 1
	case 3:
		return 0
	}
	return ti
}

func (s *Server) handleAnalytics(w http.ResponseWriter, r *http.Request) {
	v, ok := s.require(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()

	view := analyticsView{
		Page:         s.page(v, "Analytics", "analytics"),
		Range:        "7d",
		Sort:         "tokens",
		Top:          25,
		Q:            strings.TrimSpace(q.Get("q")),
		PersonMetric: "tokens",
	}
	for _, rd := range analyticsRanges {
		if q.Get("range") == rd.Key {
			view.Range = rd.Key
		}
	}
	if _, ok := map[string]bool{"sessions": true, "tokens": true, "cost": true, "recent": true}[q.Get("sort")]; ok {
		view.Sort = q.Get("sort")
	}
	if n, err := strconv.Atoi(q.Get("top")); err == nil && n > 0 && n <= 200 {
		view.Top = n
	}
	if m := q.Get("metric"); m == "sessions" || m == "cost" {
		view.PersonMetric = m
	}
	// Both panels default to every type: analytics answers "what did the fleet
	// spend", and automation is half that answer. The list page's user-only
	// default is about reading sessions, not counting them.
	view.TokenTypes = parseTypes(q.Get("tt"), sessionTypeOptions)
	view.PersonTypes = parseTypes(q.Get("pt"), sessionTypeOptions)

	// The window: [midnight local (days-1) ago, tomorrow midnight), so "today"
	// is a full calendar day and the newest bucket is the one in progress.
	plan := analyticsRanges[1]
	for _, rd := range analyticsRanges {
		if rd.Key == view.Range {
			plan = rd
		}
	}
	now := s.now()
	y, m, d := now.Date()
	midnight := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
	view.From = midnight.AddDate(0, 0, -(plan.Days - 1))
	view.To = midnight.AddDate(0, 0, 1)
	view.BucketKind, view.BucketNoun = plan.Kind(), plan.Noun

	ctx := r.Context()
	// Every type, named: a nil list is the store's own default, which hides
	// the empty and internal classes, while the stacked charts below count
	// all four. Headline totals that disagreed with the stacks under them
	// would be the page contradicting itself.
	daily, err := s.data.DailyUsage(ctx, v, view.From, view.To, plan.Unit, sessionTypeOptions)
	if err != nil {
		s.readError(w, r, v, err)
		return
	}
	view.Days = fillBuckets(daily, view.From, plan.Count, plan.Step)

	for _, day := range view.Days {
		view.Sessions += day.Sessions
		view.TokensIn += day.TokensIn
		view.TokensOut += day.TokensOut
		view.CacheRead += day.CacheRead
		view.CostUSD += day.CostUSD
		view.ToolCalls += day.ToolCalls
		view.Errors += day.Errors
	}
	if view.Sessions > 0 {
		view.AvgTokens = (view.TokensIn + view.TokensOut) / view.Sessions
	}

	rows, err := s.data.PersonUsage(ctx, v, view.From, view.To, view.Sort, view.Q, view.Top, personTypesParam(view.PersonTypes))
	if err != nil {
		s.readError(w, r, v, err)
		return
	}
	view.Rows = rows
	// Active people over the range comes from the breakdown rather than a
	// separate count: the table is already per-person and unfiltered when Q is
	// empty. Under a prefix filter the count follows the filter, which is what
	// the caption says.
	view.People = int64(len(rows))

	if people, _, err := s.data.FilterOptions(ctx, v); err != nil {
		s.log.Error("filter options for analytics", "err", err)
	} else {
		view.PeopleOptions = people
	}

	// The charts, all drawn from Days so the tables and the pictures cannot
	// disagree.
	chDays := make([]time.Time, len(view.Days))
	sessionsPts := make([]float64, len(view.Days))
	inPts := make([]float64, len(view.Days))
	outPts := make([]float64, len(view.Days))
	costPts := make([]float64, len(view.Days))
	for i, day := range view.Days {
		chDays[i] = day.Day
		sessionsPts[i] = float64(day.Sessions)
		inPts[i] = float64(day.TokensIn)
		outPts[i] = float64(day.TokensOut)
		costPts[i] = day.CostUSD
	}
	// Sessions and cost are stacked by session type: the totals stay readable
	// as bar heights while the split answers "how much of this was robots"
	// without a click. Sparse per-type rows are bucketed independently and
	// aligned by construction, since fillBuckets anchors on the range start.
	byType, err := s.data.DailyUsageByType(ctx, v, view.From, view.To, plan.Unit)
	if err != nil {
		s.readError(w, r, v, err)
		return
	}
	var sessionsSer, costSer []series
	for ti, t := range sessionTypeOptions {
		var rows []DayUsage
		for _, r := range byType {
			if r.SessionType == t {
				rows = append(rows, r)
			}
		}
		buckets := fillBuckets(rows, view.From, plan.Count, plan.Step)
		sPts := make([]float64, len(buckets))
		cPts := make([]float64, len(buckets))
		for i, b := range buckets {
			sPts[i] = float64(b.Sessions)
			cPts[i] = b.CostUSD
		}
		sessionsSer = append(sessionsSer, series{Label: SessionTypeLabel(t), Points: sPts, Color: ti})
		costSer = append(costSer, series{Label: SessionTypeLabel(t), Points: cPts, Color: chartTypeColor(ti)})
	}
	view.SessionsChart = layoutChart(chartSpec{
		Days: chDays, Kind: view.BucketKind, Bars: true, Stacked: true,
		Fmt: func(f float64) string { return numLabel(int64(f)) },
		Ser: sessionsSer,
	})

	// The tokens chart follows its own type selection; when it is narrowed the
	// whole-range Days series cannot back it, so it re-reads with the filter.
	tokIn, tokOut := inPts, outPts
	if !typesEqual(view.TokenTypes, sessionTypeOptions) {
		filtered, err := s.data.DailyUsage(ctx, v, view.From, view.To, plan.Unit, view.TokenTypes)
		if err != nil {
			s.readError(w, r, v, err)
			return
		}
		fb := fillBuckets(filtered, view.From, plan.Count, plan.Step)
		tokIn = make([]float64, len(fb))
		tokOut = make([]float64, len(fb))
		for i, b := range fb {
			tokIn[i] = float64(b.TokensIn)
			tokOut[i] = float64(b.TokensOut)
		}
	}
	view.TokensChart = layoutChart(chartSpec{
		Days: chDays, Kind: view.BucketKind,
		Fmt: func(f float64) string { return tokenLabel(int64(f)) },
		Ser: []series{
			{Label: "in", Points: tokIn, Color: 0},
			{Label: "out", Points: tokOut, Color: 1},
		},
	})
	view.CostChart = layoutChart(chartSpec{
		Days: chDays, Kind: view.BucketKind, Bars: true, Stacked: true,
		Fmt: costLabel,
		Ser: costSer,
	})

	// The per-person chart draws the table's top rows — up to eight, which is
	// where a line chart stops being readable — in table order, so row N and
	// line N share a colour by construction. Visibility of each line follows
	// its row's checkbox in CSS; the top five arrive checked.
	if n := min(len(rows), 8); n > 0 {
		emails := make([]string, n)
		for i := range n {
			emails[i] = rows[i].Email
		}
		view.ChartPeople = rows[:n]
		perDay, err := s.data.DailyUsageByPerson(ctx, v, view.From, view.To, plan.Unit, emails, personTypesParam(view.PersonTypes))
		if err != nil {
			s.log.Error("per-person series", "err", err)
		} else {
			view.PersonChart = personChart(perDay, emails, view.From, plan, view.PersonMetric)
		}
	}

	s.rnd.render(w, http.StatusOK, "analytics.html", view)
}

// fillBuckets expands sparse store rows into exactly n step-wide buckets
// anchored on the range start, so a quiet bucket draws as zero rather than
// silently not existing — a chart that omits quiet days makes every gap look
// like weekend arithmetic.
//
// People counts do not survive multi-day rollup honestly: summing daily
// distinct-counts counts a person once per active day. The bucket figure is the
// max across it — "the busiest day had N people" — which is true, merely
// blunter than a distinct count over the bucket would be.
func fillBuckets(rows []DayUsage, from time.Time, n int, step time.Duration) []DayUsage {
	// Boundaries are built by calendar arithmetic, never by multiplying the
	// step: a "day" is 23 or 25 wall-clock hours twice a year in any zone with
	// DST, so from.Add(i*24h) drifts an hour off midnight after a transition,
	// and dividing a wall-clock difference by the step drops the last hour of
	// a fall-back day entirely and folds two days into one bucket after a
	// spring-forward. The deployment zone (IST) has no DST, which is the only
	// reason the arithmetic version never misplaced a row in production.
	bounds := make([]time.Time, n+1)
	stepDays := int(step / (24 * time.Hour))
	stepHours := int(step / time.Hour)
	for i := 0; i <= n; i++ {
		if stepDays > 0 {
			bounds[i] = from.AddDate(0, 0, i*stepDays)
		} else {
			// Clock-face hours, normalised by time.Date, so hour 24 is the
			// next midnight whatever DST did in between. Adding durations
			// instead leaves the last hour of a 25-hour day past the final
			// bound, and rows in it were dropped.
			bounds[i] = time.Date(from.Year(), from.Month(), from.Day(),
				from.Hour()+i*stepHours, 0, 0, 0, from.Location())
		}
	}
	out := make([]DayUsage, n)
	for i := range n {
		out[i] = DayUsage{Day: bounds[i]}
	}
	for _, r := range rows {
		// The store's buckets come back as clock-face values with no zone (the
		// price of AT TIME ZONE), so they are re-anchored in the range's own
		// zone before comparison.
		local := time.Date(r.Day.Year(), r.Day.Month(), r.Day.Day(),
			r.Day.Hour(), 0, 0, 0, from.Location())
		// Linear over at most 24 boundaries; correctness over cleverness.
		i := -1
		for j := range n {
			if !local.Before(bounds[j]) && local.Before(bounds[j+1]) {
				i = j
				break
			}
		}
		if i < 0 {
			continue
		}
		b := &out[i]
		b.Sessions += r.Sessions
		b.TokensIn += r.TokensIn
		b.TokensOut += r.TokensOut
		b.CacheRead += r.CacheRead
		b.CacheWrite += r.CacheWrite
		b.CostUSD += r.CostUSD
		b.ToolCalls += r.ToolCalls
		b.Errors += r.Errors
		if r.People > b.People {
			b.People = r.People
		}
	}
	return out
}

// personChart lays out one line per person for the chosen metric, in the order
// given — which is the table's order, so row N and line N share a colour.
func personChart(rows []DayUsage, people []string, from time.Time, plan rangeDef, metric string) ChartView {
	perPerson := map[string][]DayUsage{}
	for _, r := range rows {
		perPerson[r.Email] = append(perPerson[r.Email], r)
	}

	value := func(d DayUsage) float64 {
		switch metric {
		case "sessions":
			return float64(d.Sessions)
		case "cost":
			return d.CostUSD
		default:
			return float64(d.TokensIn + d.TokensOut)
		}
	}
	fmt := func(f float64) string { return tokenLabel(int64(f)) }
	switch metric {
	case "sessions":
		fmt = func(f float64) string { return numLabel(int64(f)) }
	case "cost":
		fmt = costLabel
	}

	var chDays []time.Time
	sp := chartSpec{Kind: plan.Kind(), Fmt: fmt}
	for i, p := range people {
		buckets := fillBuckets(perPerson[p], from, plan.Count, plan.Step)
		if chDays == nil {
			chDays = make([]time.Time, len(buckets))
			for j, b := range buckets {
				chDays[j] = b.Day
			}
			sp.Days = chDays
		}
		pts := make([]float64, len(buckets))
		for j, b := range buckets {
			pts[j] = value(b)
		}
		sp.Ser = append(sp.Ser, series{Label: p, Points: pts, Color: i})
	}
	return layoutChart(sp)
}
