package web

// The skills page: which skills the fleet runs, on which platforms,
// by whom, and what the catalog holds that nobody runs. Seven panels over
// one window, modelled on the analytics page: the KPI row, the per-bucket
// chart stacked by platform, the top skills with their per-copy drill-down,
// the platform and repository tables, the person table, the unknown names
// beside the never-used entries, and, for an admin, the pruning and
// compliance reports.
//
// Visibility is the store's: fleet aggregates go to every member, the
// person panel is the member's own row, unknown names are counts without
// names for a member, and the two admin panels are not drawn for anyone
// else. The page never has to know which it is showing beyond what it
// asks for.

import (
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SkillQuery is the page's filter as the port takes it: the 6.2 common
// parameters, the catalog reports' window and the top-skills panel's sort
// and prefix. Types nil is the store's default, which hides internal and
// automation rows.
type SkillQuery struct {
	Range          string
	Platform, Repo string
	Types          []string
	Trigger, Trust string
	IncludeClaimed bool
	TZ             string
	Since          string
	Sort, Q        string
}

// The port's rows, one per 6.2 document row; the store's shapes as the
// page reads them.

// SkillTotals is the summary's KPI row: counts over the window, as the page reads it.
type SkillTotals struct {
	Invocations, Skills, People, Sessions         int64
	User, Agent, Success, Error, Started, Claimed int64
}

// SkillBucketRow is the invocations in one time bucket for one platform, trigger and trust, as the page reads it.
type SkillBucketRow struct {
	Bucket                   time.Time
	Platform, Trigger, Trust string
	Invocations              int64
}

// SkillCopy is one catalog copy of a lineage, for the drill-down.
type SkillCopy struct {
	SourceRepo, Plugin, Skill string
	Invocations               int64
}

// SkillSkillRow is one skill lineage and its use over the window, as the page reads it.
type SkillSkillRow struct {
	Lineage, SourceRepo, Plugin, Skill              string
	Invocations, People, Sessions, User, Agent, Err int64
	Platforms, Repos                                []string
	LastUsedAt                                      *time.Time
	Copies                                          []SkillCopy
}

// Name is the skill as the page prints it: plugin:skill, or the bare
// skill, or the folded row's name.
func (r SkillSkillRow) Name() string {
	if r.Plugin != "" {
		return r.Plugin + ":" + r.Skill
	}
	return r.Skill
}

// Folded reports the member's other row, which carries no drill-down and
// no last use.
func (r SkillSkillRow) Folded() bool { return r.Lineage == "other" }

// SkillPlatformRow is the invocations on one platform for one origin and trust, as the page reads it.
type SkillPlatformRow struct {
	Platform, Origin, Trust     string
	Invocations, Skills, People int64
}

// SkillPersonRow is one person's skill use over the window, as the page reads it.
type SkillPersonRow struct {
	Email               string
	Invocations, Skills int64
	TopSkill            string
	Platforms           []string
}

// SkillRepoRow is the invocations of one lineage in one repository, as the page reads it.
type SkillRepoRow struct {
	Repo, Lineage string
	Invocations   int64
}

// SkillSummary is the summary document: the window, the totals and every breakdown, as the page reads it.
type SkillSummary struct {
	From, To   time.Time
	Totals     SkillTotals
	ByBucket   []SkillBucketRow
	BySkill    []SkillSkillRow
	ByPlatform []SkillPlatformRow
	ByPerson   []SkillPersonRow
	ByRepo     []SkillRepoRow
}

// SkillUnusedRow is a catalog entry nobody ran in the window, as the page reads it.
type SkillUnusedRow struct {
	SourceRepo, Plugin, Skill, Lineage, AuthoredBy string
	Mirrored, Installable                          bool
	FirstSeenAt                                    time.Time
	DaysInCatalog                                  int
	LastUsedAt                                     *time.Time
	StaleMirror                                    bool
}

// SkillSuggestion is the catalog entry an unknown name most likely meant, as the page reads it.
type SkillSuggestion struct {
	SourceRepo, Plugin, Skill string
}

// SkillUnknownRow is a skill name no catalog entry resolves, with how often it was seen, as the page reads it.
type SkillUnknownRow struct {
	RawName, Platform, Origin string
	Count                     int64
	FirstSeen, LastSeen       time.Time
	Suggested                 *SkillSuggestion
}

// SkillPruningRow is the pruning report's row: a catalog entry and the action proposed for it, as the page reads it.
type SkillPruningRow struct {
	SourceRepo, Plugin, Skill, AuthoredBy, AuthorEvidence string
	LastUsedAt                                            *time.Time
	DaysUnused, DaysInCatalog                             int
	ProposedAction, ExemptReason                          string
	StaleMirror                                           bool
}

// SkillComplianceRow is the compliance report's row: beacon rows against reconciler rows for one day, platform and lineage, as the page reads it.
type SkillComplianceRow struct {
	Day               time.Time
	Platform, Lineage string
	BeaconRows        int64
	ReconcilerRows    *int64
	ExactJoins        int64
	CompliancePct     *float64
	LastRunAt         *time.Time
}

// SkillRebuild is the versioned step's progress: rebuilding while the
// skill_invocations step has started and not finished.
type SkillRebuild struct {
	Rebuilding       bool
	Processed, Total int64
}

// SkillInvocation is one row of the session strip.
type SkillInvocation struct {
	OccurredAt                   time.Time
	Origin, AgentPlatform, Trust string
	RawName, Plugin, Skill       string
	Trigger, Outcome             string
	SessionRef                   string
	Lineage                      string
}

// Name is the row as the strip prints it.
func (r SkillInvocation) Name() string {
	switch {
	case r.Skill == "":
		return r.RawName
	case r.Plugin != "":
		return r.Plugin + ":" + r.Skill
	}
	return r.Skill
}

// skillsView is everything the template draws.
type skillsView struct {
	Page    Page
	Range   string
	Sort    string
	Q       string
	Trigger string
	Claimed bool
	// Types is the resolved type selection for the menu; nil is the store
	// default, which the page names as "internal and automation hidden".
	Types []string
	From  time.Time
	To    time.Time

	Summary    SkillSummary
	Chart      ChartView
	Platforms  []string
	Unused     []SkillUnusedRow
	Unknown    []SkillUnknownRow
	Pruning    []SkillPruningRow
	Compliance []SkillComplianceRow
	Rebuild    SkillRebuild
	// The four supplementary panels each carry whether their read failed.
	// A failure costs the panel and not the page, but the panel must then
	// say so: without these flags a statement timeout rendered "Every name
	// in this window resolves." and "No beacon or reconciler rows in this
	// window.", which is an admin being told the catalog is clean and the
	// reconcilers have nothing to reconcile (adversarial finding 3).
	UnusedFailed, UnknownFailed, PruningFailed, ComplianceFailed bool
	// BucketNoun is what the chart's caption calls one bucket.
	BucketNoun string
}

// skillRanges are the offered windows (design 6.2), in display order.
var skillRanges = []struct{ Key, Label, Noun string }{
	{"1d", "Today", "hour"},
	{"7d", "7 days", "day"},
	{"30d", "30 days", "day"},
	{"90d", "90 days", "day"},
}

// skillSortKeys are the top-skills orderings (design 6.3).
var skillSortKeys = []string{"invocations", "people", "sessions", "user", "recent"}

const (
	defaultSkillRange = "30d"
	defaultSkillSort  = "invocations"
)

// Ranges, SortKeys and TypeOptions expose the offered values to the
// template.
func (skillsView) Ranges() []struct{ Key, Label, Noun string } { return skillRanges }
func (skillsView) SortKeys() []string                          { return skillSortKeys }
func (skillsView) TypeOptions() []string                       { return sessionTypeOptions }
func (skillsView) TypeName(t string) string                    { return SessionTypeLabel(t) }

// TypeChecked reports a type's place in the selection: the store default
// shows user rows and the untyped ones, so with no selection the user
// type reads checked and the two hidden classes do not.
func (v skillsView) TypeChecked(t string) bool {
	if v.Types == nil {
		return t == "user" || t == "empty"
	}
	return containsType(v.Types, t)
}

// TypeLabel names the selection for the closed menu.
func (v skillsView) TypeLabel() string {
	if v.Types == nil {
		return "internal and automation hidden"
	}
	return typeMenuLabel(v.Types)
}

// The URL builders keep every other knob when one changes, so a range
// click does not drop the prefix somebody typed.
func (v skillsView) RangeURL(key string) string {
	return skillsURL(key, v.Sort, v.Q, v.Trigger, v.Claimed, v.Types)
}
func (v skillsView) SortURL(key string) string {
	return skillsURL(v.Range, key, v.Q, v.Trigger, v.Claimed, v.Types) + "#top"
}
func (v skillsView) TriggerURL(t string) string {
	return skillsURL(v.Range, v.Sort, v.Q, t, v.Claimed, v.Types) + "#buckets"
}
func (v skillsView) ClaimedURL(on bool) string {
	return skillsURL(v.Range, v.Sort, v.Q, v.Trigger, on, v.Types)
}
func (v skillsView) TypeURL(t string) string {
	current := v.Types
	if current == nil {
		current = []string{"user", "empty"}
	}
	return skillsURL(v.Range, v.Sort, v.Q, v.Trigger, v.Claimed, toggleType(current, t, sessionTypeOptions))
}
func (v skillsView) ClearURL() string {
	return skillsURL(v.Range, v.Sort, "", v.Trigger, v.Claimed, v.Types) + "#top"
}

// UserPct and ErrorPct are the KPI shares.
func (v skillsView) UserPct() string {
	return pctLabel(v.Summary.Totals.User, v.Summary.Totals.Invocations)
}
func (v skillsView) ErrorPct() string {
	return pctLabel(v.Summary.Totals.Error, v.Summary.Totals.Invocations)
}

func pctLabel(part, whole int64) string {
	if whole == 0 {
		return "0%"
	}
	return strconv.FormatFloat(float64(part)/float64(whole)*100, 'f', 1, 64) + "%"
}

// PlatformColor is the chart palette index of a platform, in the order the
// chart drew them.
func (v skillsView) PlatformColor(p string) int {
	for i, q := range v.Platforms {
		if q == p {
			return chartTypeColor(i)
		}
	}
	return 0
}

// Pct renders a compliance ratio.
func (skillsView) Pct(p *float64) string {
	if p == nil {
		return "no run"
	}
	return strconv.FormatFloat(*p*100, 'f', 0, 64) + "%"
}

// Rows renders an optional count. It takes *int64 rather than going
// through the num function, which takes any: a pointer is assignable to
// any, so the template never indirects it, no case in numLabel matches and
// its default printed the heap address into the page (adversarial finding
// 2). A typed parameter is what makes the template indirect, so the wrong
// call cannot compile away into a plausible-looking cell.
func (skillsView) Rows(n *int64) string {
	if n == nil {
		return "none"
	}
	return numLabel(*n)
}

func skillsURL(rng, sortKey, q, trigger string, claimed bool, types []string) string {
	var parts []string
	if rng != "" && rng != defaultSkillRange {
		parts = append(parts, "range="+rng)
	}
	if sortKey != "" && sortKey != defaultSkillSort {
		parts = append(parts, "sort="+sortKey)
	}
	if q != "" {
		parts = append(parts, "q="+template.URLQueryEscaper(q))
	}
	if trigger != "" {
		parts = append(parts, "trigger="+trigger)
	}
	if claimed {
		parts = append(parts, "include_claimed=1")
	}
	if types != nil {
		parts = append(parts, "types="+strings.Join(types, ","))
	}
	if len(parts) == 0 {
		return "/skills"
	}
	return "/skills?" + strings.Join(parts, "&")
}

func (s *Server) handleSkills(w http.ResponseWriter, r *http.Request) {
	v, ok := s.require(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	view := skillsView{
		Page:       s.page(v, "Skills", "skills"),
		Range:      defaultSkillRange,
		Sort:       defaultSkillSort,
		Q:          strings.TrimSpace(q.Get("q")),
		Claimed:    q.Get("include_claimed") == "1",
		BucketNoun: "day",
	}
	for _, rd := range skillRanges {
		if q.Get("range") == rd.Key {
			view.Range, view.BucketNoun = rd.Key, rd.Noun
		}
	}
	for _, k := range skillSortKeys {
		if q.Get("sort") == k {
			view.Sort = k
		}
	}
	if t := q.Get("trigger"); t == "user" || t == "agent" {
		view.Trigger = t
	}
	// No selection is the store's default (internal and automation
	// hidden); a selection is sent as named.
	view.Types = parseTypes(q.Get("types"), nil)

	query := SkillQuery{Range: view.Range, Types: view.Types, Trigger: view.Trigger, IncludeClaimed: view.Claimed,
		TZ: analyticsTZ(), Sort: view.Sort, Q: view.Q}
	ctx := r.Context()
	sum, err := s.data.SkillSummary(ctx, v, query)
	if err != nil {
		s.readError(w, r, v, err)
		return
	}
	view.Summary, view.From, view.To = sum, sum.From, sum.To
	view.Chart, view.Platforms = skillChart(sum.ByBucket, view.BucketNoun)

	// The catalog and unknown panels are supplementary: a failure costs the
	// panel, not the page.
	if rows, err := s.data.SkillUnused(ctx, v, query); err != nil {
		s.log.Error("unused skills", "err", err)
		view.UnusedFailed = true
	} else {
		view.Unused = rows
	}
	if rows, err := s.data.SkillUnknown(ctx, v, query); err != nil {
		s.log.Error("unknown skills", "err", err)
		view.UnknownFailed = true
	} else {
		view.Unknown = rows
	}
	if v.Admin {
		if rows, err := s.data.SkillPruning(ctx, v, query); err != nil {
			s.log.Error("skill pruning", "err", err)
			view.PruningFailed = true
		} else {
			view.Pruning = rows
		}
		if rows, err := s.data.SkillCompliance(ctx, v, query); err != nil {
			s.log.Error("skill compliance", "err", err)
			view.ComplianceFailed = true
		} else {
			view.Compliance = rows
		}
	}
	if rb, err := s.data.SkillRebuild(ctx); err != nil {
		s.log.Error("skill rebuild progress", "err", err)
	} else {
		view.Rebuild = rb
	}
	s.rnd.render(w, http.StatusOK, "skills.html", view)
}

// analyticsTZ is the zone the page buckets in: the server's own local
// zone, pinned to the office's by the deployment, UTC when unpinned.
func analyticsTZ() string {
	tz := time.Local.String()
	if tz == "" || tz == "Local" {
		return "UTC"
	}
	return tz
}

// skillChart stacks the per-bucket rows by platform: one series per
// platform in first-seen order, every bucket present in every series so
// the stacks line up, and the platforms returned in the order they were
// coloured for the legend.
func skillChart(rows []SkillBucketRow, noun string) (ChartView, []string) {
	if len(rows) == 0 {
		return ChartView{}, nil
	}
	bucketSet := map[time.Time]bool{}
	var platforms []string
	seen := map[string]bool{}
	for _, r := range rows {
		bucketSet[r.Bucket] = true
		if !seen[r.Platform] {
			seen[r.Platform] = true
			platforms = append(platforms, r.Platform)
		}
	}
	buckets := make([]time.Time, 0, len(bucketSet))
	for b := range bucketSet {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Before(buckets[j]) })
	index := map[time.Time]int{}
	for i, b := range buckets {
		index[b] = i
	}
	sp := chartSpec{Days: buckets, Kind: noun, Bars: true, Stacked: true, Fmt: func(f float64) string { return numLabel(int64(f)) }}
	for i, p := range platforms {
		pts := make([]float64, len(buckets))
		for _, r := range rows {
			if r.Platform == p {
				pts[index[r.Bucket]] += float64(r.Invocations)
			}
		}
		sp.Ser = append(sp.Ser, series{Label: p, Points: pts, Color: chartTypeColor(i)})
	}
	return layoutChart(sp), platforms
}
