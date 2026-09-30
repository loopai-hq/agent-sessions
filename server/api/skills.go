package api

// The skill read routes: six GETs beside the session routes, cookie
// authenticated through viewer, with the common filter parsed here and the
// scoping done in the store. A member's compliance read is 404, the one
// admin-only route, and a member's unknown-name read comes back nameless
// from the store; the handlers add no rule of their own.

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

// SkillFilter is the common filter every skill read takes, plus each
// route's own: the listing's skill, person, session and cursor, the
// catalog reports' since and source_repo. The JSON API sets Limit through
// parseLimit; the store validates types and falls back on an unknown
// range, but the routes refuse one first so a typo is not silently thirty
// days.
type SkillFilter struct {
	Range          string
	Platform, Repo string
	Types          []string
	Trigger, Trust string
	IncludeClaimed bool
	TZ             string

	Skill, Email, SessionRef string
	Cursor                   string
	Limit                    int

	Since, SourceRepo string
}

// The response documents, tagged here because the tags are the wire
// contract (design 6.2).

// SkillTotals is the summary's KPI row: counts over the window (wire form).
type SkillTotals struct {
	Invocations int64 `json:"invocations"`
	Skills      int64 `json:"skills"`
	People      int64 `json:"people"`
	Sessions    int64 `json:"sessions"`
	User        int64 `json:"user"`
	Agent       int64 `json:"agent"`
	Success     int64 `json:"success"`
	Error       int64 `json:"error"`
	Started     int64 `json:"started"`
	Claimed     int64 `json:"claimed"`
}

// SkillBucketRow is the invocations in one time bucket for one platform, trigger and trust (wire form).
type SkillBucketRow struct {
	Bucket      time.Time `json:"bucket"`
	Platform    string    `json:"platform"`
	Trigger     string    `json:"trigger"`
	Trust       string    `json:"trust"`
	Invocations int64     `json:"invocations"`
}

// SkillSkillRow is one skill lineage and its use over the window (wire form).
type SkillSkillRow struct {
	Lineage     string     `json:"lineage"`
	SourceRepo  string     `json:"source_repo"`
	Plugin      string     `json:"plugin"`
	Skill       string     `json:"skill"`
	Invocations int64      `json:"invocations"`
	People      int64      `json:"people"`
	Sessions    int64      `json:"sessions"`
	User        int64      `json:"user"`
	Agent       int64      `json:"agent"`
	Error       int64      `json:"error"`
	Platforms   []string   `json:"platforms"`
	Repos       []string   `json:"repos"`
	LastUsedAt  *time.Time `json:"last_used_at"`
}

// SkillPlatformRow is the invocations on one platform for one origin and trust (wire form).
type SkillPlatformRow struct {
	Platform    string `json:"platform"`
	Origin      string `json:"origin"`
	Trust       string `json:"trust"`
	Invocations int64  `json:"invocations"`
	Skills      int64  `json:"skills"`
	People      int64  `json:"people"`
}

// SkillPersonRow is one person's skill use over the window (wire form).
type SkillPersonRow struct {
	Email       string   `json:"email"`
	Invocations int64    `json:"invocations"`
	Skills      int64    `json:"skills"`
	TopSkill    string   `json:"top_skill"`
	Platforms   []string `json:"platforms"`
}

// SkillRepoRow is the invocations of one lineage in one repository (wire form).
type SkillRepoRow struct {
	Repo        string `json:"repo"`
	Lineage     string `json:"lineage"`
	Invocations int64  `json:"invocations"`
}

// SkillSummary is the summary document: the window, the totals and every breakdown (wire form).
type SkillSummary struct {
	From       time.Time          `json:"from"`
	To         time.Time          `json:"to"`
	Totals     SkillTotals        `json:"totals"`
	ByBucket   []SkillBucketRow   `json:"by_bucket"`
	BySkill    []SkillSkillRow    `json:"by_skill"`
	ByPlatform []SkillPlatformRow `json:"by_platform"`
	ByPerson   []SkillPersonRow   `json:"by_person"`
	ByRepo     []SkillRepoRow     `json:"by_repo"`
}

// SkillInvocation is one listed row: the table's columns plus lineage,
// minus the credential columns.
type SkillInvocation struct {
	ID             int64     `json:"id"`
	OccurredAt     time.Time `json:"occurred_at"`
	ReceivedAt     time.Time `json:"received_at"`
	TimeClamped    bool      `json:"time_clamped"`
	Origin         string    `json:"origin"`
	AgentPlatform  string    `json:"agent_platform"`
	Trust          string    `json:"trust"`
	RawName        string    `json:"raw_name"`
	Plugin         string    `json:"plugin"`
	Skill          *string   `json:"skill"`
	SkillSource    string    `json:"skill_source"`
	Trigger        string    `json:"trigger"`
	Outcome        string    `json:"outcome"`
	ErrorClass     *string   `json:"error_class"`
	ActorEmail     *string   `json:"actor_email"`
	ActorKnown     bool      `json:"actor_known"`
	Repo           string    `json:"repo"`
	SessionRef     string    `json:"session_ref"`
	LinkRef        *string   `json:"link_ref"`
	SessionType    string    `json:"session_type"`
	EventID        *string   `json:"event_id"`
	PromptID       *string   `json:"prompt_id"`
	ToolUseID      *string   `json:"tool_use_id"`
	IdempotencyKey *string   `json:"idempotency_key"`
	ArgsPresent    bool      `json:"args_present"`
	ArgsBytes      int       `json:"args_bytes"`
	HarnessVersion string    `json:"harness_version"`
	Lineage        string    `json:"lineage"`
}

// SkillInvocationPage is one page of the invocation listing and the cursor for the next (wire form).
type SkillInvocationPage struct {
	Invocations []SkillInvocation
	NextCursor  string
}

// SkillUnusedRow is a catalog entry nobody ran in the window (wire form).
type SkillUnusedRow struct {
	SourceRepo    string     `json:"source_repo"`
	Plugin        string     `json:"plugin"`
	Skill         string     `json:"skill"`
	Lineage       string     `json:"lineage"`
	AuthoredBy    string     `json:"authored_by"`
	Mirrored      bool       `json:"mirrored"`
	Installable   bool       `json:"installable"`
	FirstSeenAt   time.Time  `json:"first_seen_at"`
	DaysInCatalog int        `json:"days_in_catalog"`
	LastUsedAt    *time.Time `json:"last_used_at"`
	StaleMirror   bool       `json:"stale_mirror"`
}

// SkillSuggestion is the catalog entry an unknown name most likely meant (wire form).
type SkillSuggestion struct {
	SourceRepo string `json:"source_repo"`
	Plugin     string `json:"plugin"`
	Skill      string `json:"skill"`
}

// SkillUnknownRow is a skill name no catalog entry resolves, with how often it was seen (wire form).
type SkillUnknownRow struct {
	RawName   string           `json:"raw_name"`
	Platform  string           `json:"platform"`
	Origin    string           `json:"origin"`
	Count     int64            `json:"count"`
	FirstSeen time.Time        `json:"first_seen"`
	LastSeen  time.Time        `json:"last_seen"`
	Suggested *SkillSuggestion `json:"suggested"`
}

// SkillPruningRow is the pruning report's row: a catalog entry and the action proposed for it (wire form).
type SkillPruningRow struct {
	SourceRepo     string     `json:"source_repo"`
	Plugin         string     `json:"plugin"`
	Skill          string     `json:"skill"`
	AuthoredBy     string     `json:"authored_by"`
	AuthorEvidence string     `json:"author_evidence"`
	LastUsedAt     *time.Time `json:"last_used_at"`
	DaysUnused     int        `json:"days_unused"`
	DaysInCatalog  int        `json:"days_in_catalog"`
	ProposedAction string     `json:"proposed_action"`
	ExemptReason   string     `json:"exempt_reason"`
	StaleMirror    bool       `json:"stale_mirror"`
}

// SkillComplianceRow is the compliance report's row: beacon rows against reconciler rows for one day, platform and lineage (wire form).
type SkillComplianceRow struct {
	Day            time.Time  `json:"day"`
	Platform       string     `json:"platform"`
	Lineage        string     `json:"lineage"`
	BeaconRows     int64      `json:"beacon_rows"`
	ReconcilerRows *int64     `json:"reconciler_rows"`
	ExactJoins     int64      `json:"exact_joins"`
	CompliancePct  *float64   `json:"compliance_pct"`
	LastRunAt      *time.Time `json:"last_run_at"`
}

// The route documents.

type skillInvocationsResponse struct {
	Invocations []SkillInvocation `json:"invocations"`
	NextCursor  string            `json:"next_cursor,omitempty"`
}

type skillUnusedResponse struct {
	Entries []SkillUnusedRow `json:"entries"`
}

type skillUnknownResponse struct {
	Names []SkillUnknownRow `json:"names"`
}

type skillPruningResponse struct {
	Candidates []SkillPruningRow `json:"candidates"`
}

type skillComplianceResponse struct {
	Days []SkillComplianceRow `json:"days"`
}

// The offered windows, refused rather than defaulted at the edge.
var skillRanges = map[string]bool{"": true, "1d": true, "7d": true, "30d": true, "90d": true}

// cursorSkills is the listing's cursor kind.
const cursorSkills cursorKind = "skills"

// parseSkillFilter reads the common parameters. Every value the store
// would fall back on is refused here instead, since a caller that typed
// range=1y should be told rather than handed thirty days.
func parseSkillFilter(r *http.Request) (SkillFilter, string, error) {
	q := r.URL.Query()
	f := SkillFilter{
		Range:      strings.TrimSpace(q.Get("range")),
		Platform:   strings.TrimSpace(q.Get("platform")),
		Repo:       strings.TrimSpace(q.Get("repo")),
		Types:      parseTypes(q.Get("types")),
		Trigger:    strings.TrimSpace(q.Get("trigger")),
		Trust:      strings.TrimSpace(q.Get("trust")),
		TZ:         strings.TrimSpace(q.Get("tz")),
		Skill:      strings.TrimSpace(q.Get("skill")),
		Email:      normalizeEmail(q.Get("email")),
		SessionRef: strings.TrimSpace(q.Get("session_ref")),
		Since:      strings.TrimSpace(q.Get("since")),
		SourceRepo: strings.TrimSpace(q.Get("source_repo")),
	}
	if !skillRanges[f.Range] {
		return f, "range", errors.New("must be one of 1d, 7d, 30d, 90d")
	}
	if !skillRanges[f.Since] {
		return f, "since", errors.New("must be one of 1d, 7d, 30d, 90d")
	}
	switch q.Get("include_claimed") {
	case "", "0":
	case "1":
		f.IncludeClaimed = true
	default:
		return f, "include_claimed", errors.New("must be 0 or 1")
	}
	if f.TZ != "" {
		if _, err := time.LoadLocation(f.TZ); err != nil {
			return f, "tz", errors.New("must be an IANA zone name")
		}
	}
	return f, "", nil
}

// skillRead is the shape every skill route shares: the viewer, the
// filter, then the store.
func (h *Handler) skillRead(w http.ResponseWriter, r *http.Request) (Viewer, SkillFilter, bool) {
	v, ok := h.viewer(w, r)
	if !ok {
		return Viewer{}, SkillFilter{}, false
	}
	f, field, err := parseSkillFilter(r)
	if err != nil {
		badRequest(w, field, err)
		return Viewer{}, SkillFilter{}, false
	}
	return v, f, true
}

func (h *Handler) handleSkillSummary(w http.ResponseWriter, r *http.Request) {
	v, f, ok := h.skillRead(w, r)
	if !ok {
		return
	}
	sum, err := h.store.SkillSummary(r.Context(), v, f)
	if err != nil {
		h.storeFailure(w, r, "skill summary", err)
		return
	}
	// Arrays, never null: a client that handles both handles one of them
	// wrong, and the empty case is a new deployment's first visit.
	if sum.ByBucket == nil {
		sum.ByBucket = []SkillBucketRow{}
	}
	if sum.BySkill == nil {
		sum.BySkill = []SkillSkillRow{}
	}
	if sum.ByPlatform == nil {
		sum.ByPlatform = []SkillPlatformRow{}
	}
	if sum.ByPerson == nil {
		sum.ByPerson = []SkillPersonRow{}
	}
	if sum.ByRepo == nil {
		sum.ByRepo = []SkillRepoRow{}
	}
	writeJSON(w, http.StatusOK, sum)
}

func (h *Handler) handleSkillInvocations(w http.ResponseWriter, r *http.Request) {
	v, f, ok := h.skillRead(w, r)
	if !ok {
		return
	}
	var err error
	if f.Limit, err = h.parseLimit(r.URL.Query().Get("limit")); err != nil {
		badRequest(w, "limit", err)
		return
	}
	// The viewer is part of the fingerprint, as on the session list: the
	// same position under two identities is two pages, and a cursor
	// pasted from a colleague's URL is refused rather than reinterpreted.
	scope := scopeOf(string(cursorSkills), v.Email, f.Range, f.Platform, f.Repo, strings.Join(f.Types, ","), f.Trigger, f.Trust,
		boolParam(f.IncludeClaimed), f.TZ, f.Skill, f.Email, f.SessionRef)
	cur, err := decodeCursor(r.URL.Query().Get("cursor"), cursorSkills, scope)
	if err != nil {
		h.storeFailure(w, r, "list skill invocations", err)
		return
	}
	f.Cursor = cur.Store
	page, err := h.store.SkillInvocations(r.Context(), v, f)
	if err != nil {
		h.storeFailure(w, r, "list skill invocations", err)
		return
	}
	body := skillInvocationsResponse{Invocations: page.Invocations}
	if body.Invocations == nil {
		body.Invocations = []SkillInvocation{}
	}
	if page.NextCursor != "" {
		body.NextCursor = encodeCursor(cursor{Kind: cursorSkills, Scope: scope, Store: page.NextCursor})
	}
	writeJSON(w, http.StatusOK, body)
}

func boolParam(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func (h *Handler) handleSkillUnused(w http.ResponseWriter, r *http.Request) {
	v, f, ok := h.skillRead(w, r)
	if !ok {
		return
	}
	rows, err := h.store.SkillUnused(r.Context(), v, f)
	if err != nil {
		h.storeFailure(w, r, "unused skills", err)
		return
	}
	if rows == nil {
		rows = []SkillUnusedRow{}
	}
	writeJSON(w, http.StatusOK, skillUnusedResponse{Entries: rows})
}

// handleSkillUnknown serves the unresolved names. The store blanks the
// names for a member (raw_name is text an emitter chose); the route adds
// nothing, so the two surfaces cannot disagree.
func (h *Handler) handleSkillUnknown(w http.ResponseWriter, r *http.Request) {
	v, f, ok := h.skillRead(w, r)
	if !ok {
		return
	}
	rows, err := h.store.SkillUnknown(r.Context(), v, f)
	if err != nil {
		h.storeFailure(w, r, "unknown skills", err)
		return
	}
	if rows == nil {
		rows = []SkillUnknownRow{}
	}
	writeJSON(w, http.StatusOK, skillUnknownResponse{Names: rows})
}

func (h *Handler) handleSkillPruning(w http.ResponseWriter, r *http.Request) {
	v, f, ok := h.skillRead(w, r)
	if !ok {
		return
	}
	rows, err := h.store.SkillPruning(r.Context(), v, f)
	if err != nil {
		h.storeFailure(w, r, "skill pruning", err)
		return
	}
	if rows == nil {
		rows = []SkillPruningRow{}
	}
	writeJSON(w, http.StatusOK, skillPruningResponse{Candidates: rows})
}

// handleSkillCompliance is the one admin-only read: a member gets the
// package's 404, so the route's existence tells them nothing, and the
// store refuses a member too should the check here ever be lost.
func (h *Handler) handleSkillCompliance(w http.ResponseWriter, r *http.Request) {
	v, f, ok := h.skillRead(w, r)
	if !ok {
		return
	}
	if !v.IsAdmin() {
		writeNotFound(w)
		return
	}
	rows, err := h.store.SkillCompliance(r.Context(), v, f)
	if err != nil {
		h.storeFailure(w, r, "skill compliance", err)
		return
	}
	if rows == nil {
		rows = []SkillComplianceRow{}
	}
	writeJSON(w, http.StatusOK, skillComplianceResponse{Days: rows})
}
