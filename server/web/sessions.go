package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

// filters is the list form's state, kept as the raw strings the user typed so
// the form redisplays exactly what they asked for rather than a normalised
// rewrite of it.
type filters struct {
	Q      string
	Email  string
	Source string
	Repo   string
	// Types is the session-type selection. Nil is the store's default view
	// (everything but the empty and internal classes, plus any row whose
	// device reported capture loss), which the list adopts rather than
	// narrowing further: the store's default is the one answer to "which
	// sessions" that the JSON API, the mirror and this page all share. An
	// explicit selection is what a chip toggle produces.
	Types []string
	From  string
	To    string
	// Harness widens a text search to the harness's own messages.
	Harness bool
}

// sessionTypeOptions is every offered session type, in display order. It must
// equal store.SessionTypes, which is the single authority; this package cannot
// import the store, so the equality is a test in the composition root rather
// than an alias.
var sessionTypeOptions = []string{"user", "internal", "automation", "empty"}

// SessionTypeOptions exposes the offered types for that test.
func SessionTypeOptions() []string { return append([]string(nil), sessionTypeOptions...) }

// defaultSessionTypes is what the store's default view shows, spelled out for
// the chips: a person's sessions and the automations. The page sends no list
// for it (nil reaches the store as its default, which also keeps a
// capture-loss row of a hidden class visible); this is only how the menu
// marks the state.
var defaultSessionTypes = []string{"user", "automation"}

// SessionTypeLabel names a type the way the page says it.
func SessionTypeLabel(t string) string {
	switch t {
	case "automation":
		return "automation"
	case "internal":
		return "internal"
	case "empty":
		return "empty"
	}
	return "user-initiated"
}

// parseTypes resolves the types parameter: unknown values vanish, absence
// means the given default, and the full set round-trips as the full set. A
// nil default is the store's own view, returned as nil so the store applies
// it rather than a list this package spelled out.
func parseTypes(raw string, def []string) []string {
	if strings.TrimSpace(raw) == "" {
		if def == nil {
			return nil
		}
		return append([]string(nil), def...)
	}
	seen := map[string]bool{}
	for _, t := range strings.Split(raw, ",") {
		seen[strings.TrimSpace(t)] = true
	}
	var out []string
	for _, t := range sessionTypeOptions {
		if seen[t] {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		if def == nil {
			return nil
		}
		return append([]string(nil), def...)
	}
	return out
}

// toggleType returns the selection with one type flipped. It never returns an
// empty selection: deselecting the last type flips to the others, because a
// filter that can select nothing renders a page that can only explain itself
// with an empty table.
func toggleType(current []string, t string, all []string) []string {
	has := false
	var out []string
	for _, c := range current {
		if c == t {
			has = true
			continue
		}
		out = append(out, c)
	}
	if !has {
		out = nil
		for _, a := range all {
			if a == t || containsType(current, a) {
				out = append(out, a)
			}
		}
		return out
	}
	if len(out) == 0 {
		for _, a := range all {
			if a != t {
				out = append(out, a)
			}
		}
	}
	return out
}

func containsType(ts []string, t string) bool {
	for _, c := range ts {
		if c == t {
			return true
		}
	}
	return false
}

func typesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// With returns this filter set re-serialised with one key replaced, as a page
// URL. It is what makes every dropdown option a plain link: clicking an option
// navigates with that value applied and everything else kept, which is the
// whole of "selections apply immediately" on a page with no script.
func (f filters) With(base, key, value string) string {
	v := f.values()
	if value == "" {
		v.Del(key)
	} else {
		v.Set(key, value)
	}
	if enc := v.Encode(); enc != "" {
		return base + "?" + enc
	}
	return base
}

// WithDates replaces both date bounds at once, for the preset ranges.
func (f filters) WithDates(base, from, to string) string {
	v := f.values()
	v.Del("from")
	v.Del("to")
	if from != "" {
		v.Set("from", from)
	}
	if to != "" {
		v.Set("to", to)
	}
	if enc := v.Encode(); enc != "" {
		return base + "?" + enc
	}
	return base
}

func (f filters) values() url.Values {
	v := url.Values{}
	set := func(k, s string) {
		if s != "" {
			v.Set(k, s)
		}
	}
	set("q", f.Q)
	set("email", f.Email)
	set("source", f.Source)
	set("repo", f.Repo)
	// The default selection stays out of the URL, so an unfiltered page keeps
	// its clean address and Clear means what it says.
	if f.Types != nil && !typesEqual(f.Types, defaultSessionTypes) {
		set("types", strings.Join(f.Types, ","))
	}
	set("from", f.From)
	set("to", f.To)
	if f.Harness {
		set("harness", "1")
	}
	return v
}

// effectiveTypes is the selection as the chips show it: the default spelled
// out when nothing was chosen.
func (f filters) effectiveTypes() []string {
	if f.Types == nil {
		return defaultSessionTypes
	}
	return f.Types
}

// WithTypes returns the page URL with one session type toggled. A toggle
// that lands back on the default clears the parameter, so the default view
// is always the store's own and never a list this page spelled out.
func (f filters) WithTypes(base, t string) string {
	g := f
	g.Types = toggleType(f.effectiveTypes(), t, sessionTypeOptions)
	if typesEqual(g.Types, defaultSessionTypes) {
		g.Types = nil
	}
	return g.With(base, "", "")
}

// WithHarness returns the page URL with the harness-messages toggle flipped.
func (f filters) WithHarness(base string) string {
	g := f
	g.Harness = !f.Harness
	return g.With(base, "", "")
}

// TypeChecked reports whether a type is in the current selection.
func (f filters) TypeChecked(t string) bool { return containsType(f.effectiveTypes(), t) }

// TypeOptions exposes the offered types to the template.
func (f filters) TypeOptions() []string { return sessionTypeOptions }

// TypeLabel names the current selection for the closed menu: the default by
// what it shows, one type by its name, everything as "all types".
func (f filters) TypeLabel() string {
	if f.Types == nil {
		return "people and automations"
	}
	if len(f.Types) == len(sessionTypeOptions) {
		return "all types"
	}
	var names []string
	for _, t := range f.Types {
		names = append(names, SessionTypeLabel(t))
	}
	return strings.Join(names, ", ")
}

// Active reports whether anything is filtered, which is what decides between
// showing a "clear" affordance and showing nothing.
func (f filters) Active() bool { return len(f.values()) > 0 }

func parseFilters(r *http.Request) filters {
	q := r.URL.Query()
	return filters{
		Q:       strings.TrimSpace(q.Get("q")),
		Email:   strings.TrimSpace(q.Get("email")),
		Source:  strings.TrimSpace(q.Get("source")),
		Repo:    strings.TrimSpace(q.Get("repo")),
		Types:   parseTypes(q.Get("types"), nil),
		From:    strings.TrimSpace(q.Get("from")),
		To:      strings.TrimSpace(q.Get("to")),
		Harness: q.Get("harness") == "1",
	}
}

// parseDay accepts the date input's own format and a full timestamp, and
// returns the zero time for anything else. A malformed date silently widening
// the window is better than an error page: the filter bar is a browsing tool,
// not a form submission with consequences.
func parseDay(s string, endOfDay bool) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.ParseInLocation("2006-01-02", s, time.Local); err == nil {
		if endOfDay {
			return t.Add(24*time.Hour - time.Nanosecond)
		}
		return t
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}

func (f filters) query(cursor string, limit int) SessionQuery {
	return SessionQuery{
		Q:      f.Q,
		Email:  f.Email,
		Source: f.Source,
		Repo:   f.Repo,
		Types:  f.Types,
		From:   parseDay(f.From, false),
		To:     parseDay(f.To, true),
		Cursor: cursor,
		Limit:  limit,
	}
}

// sourceOptions is the set of harnesses the filter bars offer, shared by the
// list and by search so the two pages cannot come to disagree about what a
// corpus can contain. It is a fixed list rather than a SELECT DISTINCT over the
// column: on a deployment where nobody has uploaded anything yet the query
// returns nothing, and a filter that renders as an empty dropdown reads as a
// broken control rather than as an empty corpus. A harness added to
// event.Source has to be added here to become filterable.
var sourceOptions = []string{
	string(event.SourceClaudeCode),
	string(event.SourceCodex),
}

// syntheticModel is the model name a harness stamps on a transcript record that
// never became an API call.
//
// Copied rather than imported: this package talks to storage through Data and
// must not take a dependency on the ingest or store packages to render a page.
// The same constant already exists in server/ingest, server/store and
// internal/backfill for the same reason, and all four have to agree — it is the
// harness's spelling, not ours, so it changes only when the harness changes.
const syntheticModel = "<synthetic>"

// spend is what a session's stored cost figure is actually worth to a reader.
//
// It exists because cost_usd = 0 means three different things and the number
// cannot say which. The cost is computed once at ingest, so a session whose
// model had no rate on file at that moment stores a zero that is not a price;
// so does a session that made no model calls at all; and so, legitimately, would
// a session that spent nothing. Only the token counts beside the figure separate
// them. A template handed a bare float renders all three as money, which is how
// a fleet that has spent thousands of dollars comes to report $0.00 on every
// row — and a plausible zero is worse than a blank, because nobody checks it.
type spend struct {
	USD float64

	// Priced reports that USD is a price. When it is false the figure is not a
	// smaller number, it is not a number at all, and the reader is shown tokens.
	//
	// The test is USD > 0 rather than a flag from storage, because storage has
	// no flag: the rate table is consulted at ingest and nothing about the
	// outcome is recorded except the figure. That makes this an inference, and
	// it is wrong in exactly one direction — a real cost small enough to round
	// to zero at the column's six decimal places (a handful of Haiku cache-read
	// tokens and nothing else) reads as unpriced. Erring towards "we do not
	// know" is the safe half of that trade.
	Priced bool

	// Billable reports that somebody was charged for this session. It is what
	// separates the two zeroes: an unpriced session has tokens, a session that
	// made no model calls has none, and only the second is honestly a dash.
	Billable bool

	In, Out, CacheRead, CacheWrite int64
}

// Unpriced is the state the template has to treat specially: real work, no
// figure. Everything else renders as either a price or a dash.
func (s spend) Unpriced() bool { return s.Billable && !s.Priced }

// costOf reads a session's spend off the rollup.
//
// Cache traffic is counted as billable because it is: reads are charged at a
// tenth of the input rate and writes at more than it, and on a real session they
// are the overwhelming majority of the bill. Treating them as free here would
// make a session that spent most of its money look like one that spent nothing.
//
// A session that mixes a priced model with an unpriced one shows a price that
// undercounts, because the rollup does not carry a per-model breakdown and this
// layer cannot see one. That gap is currently empty rather than closed: the rate
// table covers every model in captured usage, so a session can only mix if a
// fourth model appears before somebody prices it. If that becomes routine, the
// fix belongs in the rollup, not here.
func costOf(s Session) spend {
	sp := spend{
		USD:        s.CostUSD,
		Priced:     s.CostUSD > 0,
		In:         s.TokensInput,
		Out:        s.TokensOutput,
		CacheRead:  s.TokensCacheRead,
		CacheWrite: s.TokensCacheWrite,
	}
	sp.Billable = sp.In+sp.Out+sp.CacheRead+sp.CacheWrite > 0
	return sp
}

// pageSpend totals the rows a reader is actually looking at.
//
// Scoped to the page and labelled as such, because the store computes no
// aggregate over the whole filtered set and the port renders no summary strip
// when it is absent. A page-scoped total that says so is useful; the same
// numbers presented as the total for a filter that matched nine hundred sessions
// would be a second wrong number sitting above the first one.
//
// Unpriced sessions contribute their tokens and not their zero, and the count of
// them is carried out so the strip can say how much of itself is missing. A
// dollar total that silently absorbs three hundred unpriced sessions is a lower
// bound presented as a sum.
type pageSpend struct {
	Sessions int
	Priced   int
	Unpriced int

	USD                            float64
	In, Out, CacheRead, CacheWrite int64
}

func summarise(sessions []Session) *pageSpend {
	if len(sessions) == 0 {
		return nil
	}
	p := &pageSpend{Sessions: len(sessions)}
	for _, s := range sessions {
		sp := costOf(s)
		p.In += sp.In
		p.Out += sp.Out
		p.CacheRead += sp.CacheRead
		p.CacheWrite += sp.CacheWrite
		switch {
		case sp.Priced:
			p.Priced++
			p.USD += sp.USD
		case sp.Billable:
			p.Unpriced++
		}
	}
	return p
}

// modelsUsed lists the distinct models a window of events called, oldest first.
//
// It is how the detail page names what it could not price. Without a name the
// gap is a permanent property of the product — an admin reading "unpriced" has
// nothing to act on — and with one it is a row in model_prices.
//
// Events with no usage are skipped: a model that was never billed for is not
// what is missing from the rate table. Synthetic records are skipped for the
// same reason the pricer refuses to price them; they were never API calls, and
// listing one as a model awaiting a rate would send somebody to invent a price
// for something that is never charged.
func modelsUsed(events []event.Event) []string {
	seen := map[string]bool{}
	var out []string
	for _, e := range events {
		if e.Usage == nil || e.Model == "" || e.Model == syntheticModel || seen[e.Model] {
			continue
		}
		seen[e.Model] = true
		out = append(out, e.Model)
	}
	return out
}

type sessionRow struct {
	S      Session
	Spend  spend
	Prompt []Segment
	URL    string
	// Facet is the row's lattice, provenance and lineage state, read once for
	// the whole page; the badges and the Turns column come from it.
	Facet SessionFacet
	// Answer opens the first turn's final answer, when the first turn got one.
	Answer []Segment
	// Match is the strongest message in this session that matched the query, or
	// nil when the row is here because its own metadata matched. It is what
	// keeps the merged list at least as useful as the search page it replaced:
	// without it a text filter narrows the list and never says which words it
	// narrowed on.
	Match *searchHit
}

// Turns is the Turns column: the main-thread turns a person opened, which
// the runner recomputed from turns. The additive counter is the fallback for
// a row whose facet did not arrive.
func (r sessionRow) Turns() int {
	if r.Facet.Type != "" {
		return r.Facet.HumanTurns
	}
	return r.S.UserTurns
}

// HeadTruncated reports a session whose beginning was not imported.
func (r sessionRow) HeadTruncated() bool {
	return r.Facet.HeadState == "truncated_window" || r.Facet.HeadState == "start_lost"
}

// TitleBadge names where a title came from when a person did not type it.
func (r sessionRow) TitleBadge() string {
	switch r.Facet.TitleSource {
	case "command":
		return "command"
	case "automation_template":
		return "template"
	}
	return ""
}

type listView struct {
	Page   Page
	Filter filters
	Rows   []sessionRow
	Totals *Totals
	// Spend totals this page's rows. Nil when the page is empty, which is what
	// keeps the strip off a "nothing here yet" screen rather than printing a
	// row of zeroes above it.
	Spend   *pageSpend
	NextURL string
	// ShowOwner is true for admins, whose list spans people and is unreadable
	// without a name column. A member's list is all their own sessions, where
	// the column would repeat their own address on every row.
	ShowOwner bool
	Sources   []string

	// Elsewhere are matches in sessions this page is not listing, which is the
	// normal case for a query that appears in tool output rather than in a
	// prompt: nothing about the session's own row says the word, so the list
	// query cannot find it.
	Elsewhere []searchHit
	// Capped reports that the store stopped collecting candidates before
	// ranking them. Surfaced rather than hidden, for the same reason the search
	// page surfaced it: a reader whose query matched half the corpus should be
	// told to narrow it instead of believing they saw everything.
	Capped bool
	// MatchesWithheld reports that the message search did not run because the
	// reader also asked for a repository. See handleList.
	MatchesWithheld bool

	// People and Repos feed the filter dropdown panels, so the person box
	// completes to addresses that exist rather than being a plain text field
	// somebody has to type a whole email into correctly. Empty on a lookup
	// failure, which degrades the inputs to plain text rather than failing
	// the page.
	People []string
	Repos  []string

	// Today anchors the date-preset links, in the server's zone.
	Today time.Time
}

// DatePreset is one quick range, Grafana-style: the picker is presets plus
// typed absolute dates, not a calendar widget — a calendar without script is a
// month of links and three states of hover nobody asked for.
type DatePreset struct {
	Label    string
	From, To string
}

// DatePresets are the quick ranges the date menu offers.
func (l listView) DatePresets() []DatePreset {
	day := func(d int) string { return l.Today.AddDate(0, 0, d).Format("2006-01-02") }
	return []DatePreset{
		{"Any time", "", ""},
		{"Today", day(0), day(0)},
		{"Last 7 days", day(-6), day(0)},
		{"Last 30 days", day(-29), day(0)},
	}
}

// DateLabel names the current date selection for the menu button.
func (l listView) DateLabel() string {
	switch {
	case l.Filter.From == "" && l.Filter.To == "":
		return "Any time"
	case l.Filter.From != "" && l.Filter.To != "":
		return l.Filter.From + " \u2192 " + l.Filter.To
	case l.Filter.From != "":
		return "since " + l.Filter.From
	default:
		return "until " + l.Filter.To
	}
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	v, ok := s.require(w, r)
	if !ok {
		return
	}
	f := parseFilters(r)
	q := f.query(r.URL.Query().Get("cursor"), s.pageN)

	page, err := s.data.ListSessions(r.Context(), v, q)
	if err != nil {
		s.fail(w, r, v, err)
		return
	}

	// One facets read for the whole page, never one per row: the badges and
	// the Turns column come from the lattice columns the session projection
	// does not carry. A failure costs the badges, not the list.
	var facets map[string]SessionFacet
	if len(page.Sessions) > 0 {
		ids := make([]string, 0, len(page.Sessions))
		for _, sess := range page.Sessions {
			ids = append(ids, sess.ID)
		}
		if fc, err := s.data.Facets(r.Context(), v, ids); err != nil {
			s.log.Error("read session facets for the list", "err", err)
		} else {
			facets = fc
		}
	}

	terms := QueryTerms(f.Q)
	rows := make([]sessionRow, 0, len(page.Sessions))
	for _, sess := range page.Sessions {
		row := sessionRow{
			S:      sess,
			Spend:  costOf(sess),
			Prompt: Highlight(Snippet(sess.FirstPrompt, terms, 240), terms),
			URL:    sessionURL(sess.ID, f.Q),
			Facet:  facets[sess.ID],
		}
		if row.Facet.FirstAnswer != "" {
			row.Answer = Highlight(Snippet(row.Facet.FirstAnswer, terms, 200), terms)
		}
		rows = append(rows, row)
	}

	p := s.page(v, "Sessions", "sessions")
	p.Query = f.Q
	view := listView{
		Page:   p,
		Filter: f,
		Rows:   rows,
		Totals: page.Totals,
		// Summarised from what the store returned rather than from view.Rows,
		// which the message-match fold-in below rewrites. The strip describes
		// the sessions on this page, and that set is decided here.
		Spend:     summarise(page.Sessions),
		ShowOwner: v.Admin,
		Sources:   sourceOptions,
	}
	if page.NextCursor != "" {
		vals := f.values()
		vals.Set("cursor", page.NextCursor)
		view.NextURL = "/sessions?" + vals.Encode()
	}

	// The query box narrows the list on what a session row records and, through
	// the search below, on what was said inside the transcript. Both halves have
	// to run for the same words, or the same box answers the same question two
	// different ways depending on where the match happened to be.
	switch {
	case f.Q == "":
		// Nobody typed anything, so nothing is ranked. The list is the page
		// everyone lands on and a full-text query on every visit would be a tax
		// on the common case for no answer.
	case f.Repo != "":
		// Messages carry no repository, so the store cannot narrow a search to
		// one: the adapter refuses the query rather than returning every repo's
		// hits under a heading that says otherwise. Asking anyway would fail the
		// whole page, and quietly asking without the repo would list matches
		// from repositories the reader has filtered out. Withholding them and
		// saying so is the only one of the three that is true.
		view.MatchesWithheld = true
	default:
		hits, capped, err := s.messageMatches(r.Context(), v, f)
		if err != nil {
			s.fail(w, r, v, err)
			return
		}
		view.Rows, view.Elsewhere = attachMatches(view.Rows, hits)
		view.Capped = capped
	}

	if people, repos, err := s.data.FilterOptions(r.Context(), v); err != nil {
		s.log.Error("filter options for the session list", "err", err)
	} else {
		view.People, view.Repos = people, repos
	}
	view.Today = s.now()
	s.rnd.render(w, http.StatusOK, "sessions.html", view)
}

func sessionURL(id, q string) string {
	u := "/sessions/" + url.PathEscape(id)
	if q != "" {
		u += "?hl=" + url.QueryEscape(q)
	}
	return u
}

type detailView struct {
	Inspector      *artifactView
	InspectorQuery url.Values
	InspectorError string
	Page           Page
	Session        SessionDetail
	// Facet is the session's lattice, provenance and lineage state: the
	// title badge, the head banner and the continued link read it.
	Facet SessionFacet
	Spend spend
	// Unpriced names the models seen in the turns this page loaded, and is set
	// only when the session reports tokens with no cost. It is what turns
	// "unpriced" from a dead end into an action: these are the strings that need
	// a row in model_prices.
	//
	// It covers the loaded page rather than the whole session, because that is
	// what this page has in hand and a second query for the sake of a caption is
	// not worth what it costs on a twelve-thousand-event transcript. The caption
	// says so; naming one real model is enough to close the gap, and the pricer
	// names every one of them in the log the first time it meets it.
	Unpriced []string
	// Turns is the page: the runner's exchanges, in order, each with its
	// prompt, its collapsed work, its answer slot and its subagents.
	Turns []TurnView
	// Head is the banner above a session whose beginning is not on the page,
	// nil when the head is complete.
	Head *headBanner
	// EventsCapped reports that the store cut this page's events at its
	// ceiling, so the later turns on it are incomplete.
	EventsCapped bool
	// The Slack mirroring panel: present only when the deployment has a
	// mirror and the viewer owns the session; attaching posts the owner's
	// work, so nobody else gets the control.
	ShowMirror   bool
	Mirrors      []SlackSessionMirror
	MirrorGroups []SlackGroup

	// Artifacts and Links are what the session produced as opposed to what it
	// said. Both are empty for any session captured by a transcript walk rather
	// than by live hooks, which is most historical sessions, so the template
	// renders each panel only when it has something — an empty box on every old
	// session reads as a broken feature rather than an absent one.
	Artifacts []Artifact
	Links     []Link

	// Skills is the strip under the head: the skills this session's own
	// events show it ran, in order. Derived rows only (design 6.3): a row a
	// shared cloud token posted under a colleague's session id never
	// renders on their page, and the template keeps to that origin too.
	Skills []SkillInvocation

	PrevURL string
	NextURL string
	// AgentURL indexes the subagent filter links by agent id so the header can
	// offer a jump into one thread of a long session.
	AgentURL map[string]string
	AllURL   string
	Agent    string
}

func (s *Server) handleDetail(w http.ResponseWriter, r *http.Request) {
	v, ok := s.require(w, r)
	if !ok {
		return
	}
	s.renderSession(w, r, v, r.PathValue("id"))
}

// handleShared resolves a share link. The token is exchanged for a session by
// storage, which is also what records the read as via="share"; this handler
// never learns whether the token was invalid or merely expired, and neither
// does the person holding it.
func (s *Server) handleShared(w http.ResponseWriter, r *http.Request) {
	v, ok := s.require(w, r)
	if !ok {
		return
	}
	det, err := s.data.ResolveShare(r.Context(), v, r.PathValue("token"))
	if err != nil {
		s.readError(w, r, v, err)
		return
	}
	http.Redirect(w, r, sessionURL(det.ID, ""), http.StatusFound)
}

// renderSession draws one session's detail page.
//
// Everything it shows is fetched through Data with the viewer attached, which
// is also why the page never links out to object storage. A signed URL is
// fetched without an identity, so a read through one would not appear in the
// access log, and the promise that reading a colleague's transcript is
// recorded would be quietly false for anyone who knew to use it.
func (s *Server) renderSession(w http.ResponseWriter, r *http.Request, v Viewer, id string) {
	ctx := r.Context()
	det, err := s.data.Session(ctx, v, id)
	if err != nil {
		s.readError(w, r, v, err)
		return
	}

	q := r.URL.Query()
	agent := strings.TrimSpace(q.Get("agent"))
	tq := TurnQuery{
		Thread: derive.CanonicalThread(agent),
		After:  parseTurnCursor(q.Get("after")),
		Limit:  turnsPerPage,
	}
	tp, err := s.data.Turns(ctx, v, id, tq)
	if err != nil {
		s.readError(w, r, v, err)
		return
	}
	// The facets read is one query for the one id; the banner, the badge and
	// the continued link come from it. A failure costs those and not the
	// transcript.
	var facet SessionFacet
	if facets, err := s.data.Facets(ctx, v, []string{id}); err != nil {
		s.log.Error("read session facets", "session", id, "err", err)
	} else {
		facet = facets[id]
	}

	hl := strings.TrimSpace(q.Get("hl"))
	turns := buildTurnViews(tp, TranscriptOptions{
		Start:             det.StartedAt,
		Terms:             QueryTerms(hl),
		Labels:            AgentLabels(det.Agents),
		SessionID:         id,
		FullAssistantText: continuousPath(r.URL.Path) || q.Get("reader") == "1",
	}, facet.ParentSessionID)

	p := s.page(v, sessionTitle(det.Session), "sessions")
	p.Continuous = continuousPath(r.URL.Path)
	p.Query = hl
	view := detailView{
		InspectorQuery: q,
		Page:           p,
		Session:        det,
		Facet:          facet,
		Spend:          costOf(det.Session),
		Turns:          turns,
		EventsCapped:   tp.EventsCapped,
		Agent:          agent,
		AgentURL:       map[string]string{},
		AllURL:         sessionPageURL(id, hl, "", ""),
	}
	// The head banner belongs to the first page only: a reader who paged
	// forward has already seen where the capture begins.
	if tq.After == nil {
		first := det.StartedAt
		if len(turns) > 0 {
			first = turns[0].StartedAt
		}
		view.Head = headBannerFor(id, facet, first)
	}
	if view.Spend.Unpriced() {
		view.Unpriced = modelsUsed(turnEvents(tp))
	}
	if s.slack != nil && det.Session.Email == v.Email {
		view.ShowMirror = true
		if ms, err := s.slack.SessionMirrors(ctx, id); err == nil {
			view.Mirrors = ms
		} else {
			s.log.Error("read session mirrors", "session", id, "err", err)
		}
		if sv, err := s.slack.Settings(ctx, v.Email); err == nil {
			for _, g := range sv.Groups {
				if !g.Disabled {
					view.MirrorGroups = append(view.MirrorGroups, g)
				}
			}
		} else {
			s.log.Error("read groups for session panel", "err", err)
		}
	}
	if tp.HasMore && tp.NextAfter != nil {
		view.NextURL = sessionPageURL(id, hl, agent, strconv.Itoa(*tp.NextAfter))
	}
	// Deeper pages link back to the start rather than one page up: the cursor
	// chain is forward-only, and "back one page" is the browser's job.
	if tq.After != nil {
		view.PrevURL = sessionPageURL(id, hl, agent, "")
	}
	for _, a := range det.Agents {
		view.AgentURL[a.AgentID] = sessionPageURL(id, hl, a.AgentID, "")
	}

	// Artifacts and links are supplementary: the transcript is the page, and a
	// store that cannot answer these must not cost somebody the session they
	// came to read. Both failures are logged and the panels simply do not
	// render.
	if arts, err := s.data.Artifacts(ctx, v, id); err != nil {
		s.log.Error("read session artifacts", "session", id, "err", err)
	} else {
		view.Artifacts = arts
	}
	if ls, err := s.data.Links(ctx, v, id); err != nil {
		s.log.Error("read session links", "session", id, "err", err)
	} else {
		view.Links = ls
	}
	// The skills strip is supplementary like the panels above: a failure is
	// logged and the strip is not drawn. The session read above already
	// settled who may see this page, so a not-found here is a race and is
	// treated as the same absence.
	if sk, err := s.data.SessionSkills(ctx, v, id); err != nil {
		if !errors.Is(err, ErrNotFound) {
			s.log.Error("read session skills", "session", id, "err", err)
		}
	} else {
		view.Skills = sk
	}
	if file := q.Get("file"); file != "" {
		artifactID, err := strconv.ParseInt(file, 10, 64)
		if err != nil {
			s.notFound(w, r, v)
			return
		}
		art, versions, err := s.data.Artifact(ctx, v, artifactID)
		if err != nil {
			s.readError(w, r, v, err)
			return
		}
		if art.SessionID != id {
			s.notFound(w, r, v)
			return
		}
		preview := &artifactView{Artifact: art, Versions: versions}
		if len(versions) > 0 {
			preview.Selected = versions[0]
			if version := q.Get("version"); version != "" {
				found := false
				for _, candidate := range versions {
					if candidate.EventID == version {
						preview.Selected = candidate
						found = true
						break
					}
				}
				if !found {
					s.notFound(w, r, v)
					return
				}
			}
			body, err := s.data.ArtifactContent(ctx, v, artifactID, preview.Selected.EventID)
			if err != nil {
				view.InspectorError = "This captured version could not be loaded. Try opening the full file history."
			} else {
				if len(body) > maxRendered {
					cut := maxRendered
					for cut > 0 && !utf8Start(body[cut]) {
						cut--
					}
					body, preview.Truncated = body[:cut], true
				}
				preview.Content = body
			}
		}
		view.Inspector = preview
	}

	s.rnd.render(w, http.StatusOK, "session.html", view)
}

// turnsPerPage is the reader's page. Twenty-five exchanges is a screen or
// two of a real session and a turn is never split, so a page is a whole
// number of questions with their answers.
const turnsPerPage = 25

// turnEvents flattens a page's events, for the caption that names unpriced
// models.
func turnEvents(tp TurnPage) []event.Event {
	var out []event.Event
	for _, t := range tp.Turns {
		for _, e := range t.Events {
			out = append(out, e.Event)
		}
	}
	for _, t := range tp.Agents {
		for _, e := range t.Events {
			out = append(out, e.Event)
		}
	}
	return out
}

func (v detailView) FileURL(id int64, version string) string {
	q := url.Values{}
	for _, key := range []string{"after", "agent", "hl"} {
		if value := v.InspectorQuery.Get(key); value != "" {
			q.Set(key, value)
		}
	}
	if id != 0 {
		q.Set("file", strconv.FormatInt(id, 10))
	}
	if version != "" {
		q.Set("version", version)
	}
	u := "/sessions/" + url.PathEscape(v.Session.ID)
	if v.Page.Continuous {
		u += "/conversation"
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u + "#inspector-h"
}

func sessionPageURL(id, hl, agent, after string) string {
	vals := url.Values{}
	if hl != "" {
		vals.Set("hl", hl)
	}
	if agent != "" {
		vals.Set("agent", agent)
	}
	if after != "" {
		vals.Set("after", after)
	}
	u := "/sessions/" + url.PathEscape(id)
	if len(vals) > 0 {
		u += "?" + vals.Encode()
	}
	return u
}

func sessionTitle(s Session) string {
	switch {
	case s.Repo != "":
		return s.Repo
	case s.FirstPrompt != "":
		return firstLine(s.FirstPrompt, 60)
	default:
		return "Session " + shortID(s.ID)
	}
}

type eventView struct {
	Page    Page
	Session Session
	Event   event.Event
	Back    string

	Text      []Segment
	Input     []Segment
	Output    []Segment
	Raw       []Segment
	Diffs     []FileDiff
	Truncated bool
}

// rawEventLimit is how much of one event's original payload the page will
// render. Generous compared with the transcript's per-block ceilings, because
// this page exists precisely for the case where those ceilings got in the way,
// but still bounded: a single event can be tens of megabytes and this is a
// browser.
const rawEventLimit = 512 << 10

// handleEvent renders one event in full. Everything on this page still goes
// through the same escaping as the transcript; "raw" describes how little we
// reshape the payload, not a relaxation of how it is written to the response.
func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	v, ok := s.require(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	det, err := s.data.Session(r.Context(), v, id)
	if err != nil {
		s.readError(w, r, v, err)
		return
	}
	e, err := s.data.Event(r.Context(), v, id, r.PathValue("eventID"))
	if err != nil {
		s.readError(w, r, v, err)
		return
	}

	view := eventView{
		Page:    s.page(v, "Event "+shortID(e.ID), "sessions"),
		Session: det.Session,
		Event:   e,
		Back:    sessionPageURL(id, "", "", pageBefore(e.OccurredAt)),
	}
	var t bool
	view.Text, t = textSegments(e.Text, rawEventLimit, nil)
	view.Truncated = view.Truncated || t
	if e.Tool != nil {
		view.Input, t = textSegments(prettyJSON(e.Tool.Input), rawEventLimit, nil)
		view.Truncated = view.Truncated || t
		view.Output, t = textSegments(e.Tool.Output, rawEventLimit, nil)
		view.Truncated = view.Truncated || t
		if d := e.Tool.Diff; d != nil {
			view.Diffs = []FileDiff{BuildFileDiff(d.Path, d.Before, d.After, d.Created)}
		}
	}
	if len(e.Raw) > 0 {
		view.Raw, t = textSegments(prettyJSON(json.RawMessage(e.Raw)), rawEventLimit, nil)
		view.Truncated = view.Truncated || t
	}
	s.rnd.render(w, http.StatusOK, "event.html", view)
}

// pageBefore addresses the window containing a given moment, for the back
// link from an event to its context: on a twelve-thousand-event transcript,
// "back to the top" is a different thing entirely. A millisecond of slack
// keeps the event itself inside the window whatever thread it belongs to.
func pageBefore(at time.Time) string {
	c := EventCursor{At: at.Add(-time.Millisecond)}
	if c.At.IsZero() || at.IsZero() {
		return ""
	}
	return c.Encode()
}
