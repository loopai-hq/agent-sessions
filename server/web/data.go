package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/server/fleet"
)

// Data is everything the dashboard needs from storage.
//
// It is an interface rather than a concrete store handle for one reason that
// matters more than testability: authorization and audit are storage
// decisions, not rendering decisions. Every method takes the Viewer, so the
// implementation can apply the canRead rules and write the access_log row in
// the same transaction as the read it is auditing. A web layer that fetched a
// session first and asked about permission afterwards would have already done
// the unaudited read by the time it found out.
//
// The corollary is that nothing here returns "you may not see this" as a
// distinguishable outcome. Denial and absence both arrive as ErrNotFound, and
// the handlers render them identically, because a response that separates the
// two tells the viewer that a colleague ran something.
type Data interface {
	// ListSessions returns the sessions this viewer may see, newest first.
	ListSessions(ctx context.Context, v Viewer, q SessionQuery) (SessionPage, error)

	// Session returns one session's rollup. Audited.
	Session(ctx context.Context, v Viewer, id string) (SessionDetail, error)

	// Events returns a window of a session's events in seq order. The window
	// exists because sessions reach tens of megabytes and a page that loads a
	// whole one is a page that times out on exactly the sessions people most
	// want to read. Audited.
	//
	// One page view calls Session and then Events, so a naive implementation
	// records two audit rows for one read. That is not wrong, and it matches
	// the JSON API where the two are separate endpoints, but if the access log
	// is to stay readable the implementation should collapse repeats of the
	// same (viewer, session, via) within a short window.
	Events(ctx context.Context, v Viewer, id string, q EventQuery) (EventPage, error)

	// Event returns one event in full, for the page that exists because the
	// transcript view truncates: a reader who needs the untruncated payload
	// has to be able to get to it without an operator running a query.
	// Audited.
	Event(ctx context.Context, v Viewer, sessionID, eventID string) (event.Event, error)

	// Search runs message-granularity full-text search scoped to what the
	// viewer may read.
	Search(ctx context.Context, v Viewer, q SearchQuery) (SearchResults, error)

	// ResolveShare exchanges a share link token for the session it grants.
	// Audited as via="share".
	ResolveShare(ctx context.Context, v Viewer, token string) (SessionDetail, error)

	// Principals lists everyone who may use the system. Admin only.
	Principals(ctx context.Context, v Viewer) ([]Principal, error)

	// SetPrincipal changes a role or disables an account. The implementation
	// owns the last-admin guard; the handler's own check is a fast, friendly
	// duplicate of it and not a substitute.
	// The boolean reports whether the roster actually moved. A submission whose
	// values already match the row is not an error and is not a change, and
	// telling somebody "Saved." for it teaches them that the button lies —
	// which is exactly how a real failed save later goes unnoticed.
	SetPrincipal(ctx context.Context, v Viewer, u PrincipalUpdate) (bool, error)

	// Fleet reports client coverage derived from health reports. Admin only.
	Fleet(ctx context.Context, v Viewer) (Fleet, error)

	// AccessLog reports who read whose sessions. Admin only. The audit trail
	// is only a control if someone can actually look at it.
	AccessLog(ctx context.Context, v Viewer, q AccessQuery) ([]AccessEntry, error)

	// Artifacts lists the files a session created or changed, most recently
	// touched first. Empty for any session whose events came from a transcript
	// walk rather than live hooks, because only the live path carries diffs.
	Artifacts(ctx context.Context, v Viewer, sessionID string) ([]Artifact, error)

	// Links lists the URLs a session mentioned, classified and ordered by how
	// likely somebody is to want them: pull requests first.
	Links(ctx context.Context, v Viewer, sessionID string) ([]Link, error)

	// Artifact returns one file and every content state it passed through,
	// newest first. Authorized against the artifact's session, so it is visible
	// exactly when the transcript it came from is.
	Artifact(ctx context.Context, v Viewer, id int64) (Artifact, []ArtifactVersion, error)

	// ArtifactContent returns the file as it stood at one version.
	ArtifactContent(ctx context.Context, v Viewer, id int64, eventID string) (string, error)

	// DailyUsage returns whole-scope totals over [from, to), bucketed by unit
	// ("hour" or "day"). Scoped by the implementation: an admin aggregates
	// everyone, a member only themselves. types narrows to the given session
	// types; empty means every type.
	DailyUsage(ctx context.Context, v Viewer, from, to time.Time, unit string, types []string) ([]DayUsage, error)

	// DailyUsageByType returns per-bucket rows split by session type, for the
	// stacked charts. Unfiltered by design: a stack shows every part of the
	// whole.
	DailyUsageByType(ctx context.Context, v Viewer, from, to time.Time, unit string) ([]DayUsage, error)

	// DailyUsageByPerson returns per-bucket rows for a small set of people, for
	// the person chart. The implementation caps the set. types narrows to the
	// given session types; empty means every type.
	DailyUsageByPerson(ctx context.Context, v Viewer, from, to time.Time, unit string, emails, types []string) ([]DayUsage, error)

	// PersonUsage returns per-person totals ordered by sortKey ("sessions",
	// "tokens", "cost", "recent"), filtered by a case-insensitive prefix on
	// email or display name, at most top rows. types narrows to the given
	// session types; empty means every type.
	PersonUsage(ctx context.Context, v Viewer, from, to time.Time, sortKey, prefix string, top int, types []string) ([]PersonUsage, error)

	// FilterOptions returns the people and repositories worth offering in filter
	// dropdowns, scoped to what the viewer may see.
	FilterOptions(ctx context.Context, v Viewer) (people, repos []string, err error)

	// Turns returns one page of a session's turns, the derived exchanges the
	// runner folded (server/store/derive), each with the canonical copies of
	// its events. The reader renders these and nothing else: a page that
	// re-derived turns from events would be a second fold. Audited like
	// Events.
	Turns(ctx context.Context, v Viewer, id string, q TurnQuery) (TurnPage, error)

	// Facets returns the lattice, provenance and lineage state of the given
	// sessions, keyed by id, in one read for the whole page. A session the
	// viewer may not see is simply absent from the map.
	Facets(ctx context.Context, v Viewer, ids []string) (map[string]SessionFacet, error)

	// FleetEvaluation is the fleet evaluator's view (server/fleet): the
	// ranked CTA list, every machine's version state against the release
	// manifest, the mutes in force. Admin only. It is the same computation
	// the evaluator logs from, so the page and the alerts say one thing.
	FleetEvaluation(ctx context.Context, v Viewer) (*fleet.Evaluation, error)

	// MuteFleet silences one (person, kind) until a moment; UnmuteFleet
	// lifts it. Admin only.
	MuteFleet(ctx context.Context, v Viewer, m FleetMute) error
	UnmuteFleet(ctx context.Context, v Viewer, email, kind string) error

	// The /skills page and the session strip (skills.go): every read takes
	// the Viewer, and the implementation scopes the person panels the way
	// the store scopes every other read.
	SkillData
}

// SkillData is the skill-usage half of Data (design 6.3): the summary the
// page's first five panels draw, the two catalog reports, the unknown-name
// report, the admin-only compliance report, the rebuild banner's progress
// and the session page's strip.
type SkillData interface {
	SkillSummary(ctx context.Context, v Viewer, q SkillQuery) (SkillSummary, error)
	SkillUnused(ctx context.Context, v Viewer, q SkillQuery) ([]SkillUnusedRow, error)
	SkillUnknown(ctx context.Context, v Viewer, q SkillQuery) ([]SkillUnknownRow, error)
	SkillPruning(ctx context.Context, v Viewer, q SkillQuery) ([]SkillPruningRow, error)
	SkillCompliance(ctx context.Context, v Viewer, q SkillQuery) ([]SkillComplianceRow, error)
	SkillRebuild(ctx context.Context) (SkillRebuild, error)
	// SessionSkills is the strip: one session's derived rows, for a viewer
	// the session's own predicate admits. Denial and absence are one
	// ErrNotFound.
	SessionSkills(ctx context.Context, v Viewer, sessionID string) ([]SkillInvocation, error)
}

// FleetMute is one mute as the page submits it.
type FleetMute struct {
	Email string
	Kind  string
	Until time.Time
	Note  string
}

// TurnQuery is a window into one session's turns.
type TurnQuery struct {
	// Thread is "" for the main conversation; a canonical agent id reads one
	// subagent's own turns top-level.
	Thread string
	// After is the exclusive lower bound on the turn index, nil for the
	// first page. A turn is never split across pages.
	After *int
	Limit int
}

// TurnPage is one page of turns plus the subagent turns inside its span.
type TurnPage struct {
	Turns  []Turn
	Agents []Turn
	// HasMore and NextAfter page forward by turn index.
	HasMore   bool
	NextAfter *int
	// EventsCapped reports that the page's events were cut at the store's
	// ceiling and the later turns on it are shown incomplete.
	EventsCapped bool
}

// Turn is one exchange as the runner folded it: what was asked, what the
// agent did, what it finally said, and how long that took.
type Turn struct {
	Thread string
	Index  int
	// Kind is the opening prompt's kind (human, slash_command, subagent_task,
	// or "" for a head turn made of rows before the first captured prompt).
	Kind string
	// Outcome is answered, interrupted, no_answer_captured, no_work or
	// in_progress.
	Outcome       string
	Inherited     bool
	PromptEventID string
	FinalEventID  string

	StartedAt       time.Time
	FirstActivityAt time.Time
	LastActivityAt  time.Time
	AnsweredAt      time.Time
	Wall            time.Duration
	Active          time.Duration
	Idle            time.Duration
	WaitingForHuman time.Duration

	Origins      []string
	Merged       int
	Prompts      int
	ToolCalls    int
	Errors       int
	Subagents    int
	FilesChanged int

	TokensInput      int64
	TokensOutput     int64
	TokensCacheRead  int64
	TokensCacheWrite int64
	CostUSD          float64
	Model            string

	Events []TurnEvent
}

// TurnEvent is one event inside a turn: the record, the role the fold gave
// it (prompt, final, work) and the message kind the normalizer assigned at
// ingest. Kind is "" for rows that have no message row or were stored before
// kinds existed, which a reader treats as a person's rather than as noise.
type TurnEvent struct {
	event.Event
	Role string
	Kind string
}

// SessionFacet is the lattice, provenance and lineage state of a session:
// the columns the list badges, the head banner and the title badge read.
type SessionFacet struct {
	Type            string
	EmptyKind       string
	HeadState       string
	LineageSource   string
	ParentSessionID string
	TitleSource     string
	HumanTurns      int
	HarnessTitle    string
	ContentEvents   int
	Entrypoint      string
	// CaptureLossDrops is how many events the device reported dropping
	// around a session whose head state is capture_loss.
	CaptureLossDrops int64
	// FirstAnswer opens the first turn's final answer, for the list row.
	FirstAnswer string
}

// DayUsage is one day's totals, optionally for one person.
type DayUsage struct {
	Day         time.Time
	Email       string
	SessionType string
	Sessions    int64
	People      int64
	TokensIn    int64
	TokensOut   int64
	CacheRead   int64
	CacheWrite  int64
	CostUSD     float64
	ToolCalls   int64
	Errors      int64
}

// PersonUsage is one person's totals over a range.
type PersonUsage struct {
	Email       string
	DisplayName string
	Sessions    int64
	TokensIn    int64
	TokensOut   int64
	CacheRead   int64
	CostUSD     float64
	ToolCalls   int64
	Errors      int64
	LastActive  time.Time
}

// Display is the name the page shows: the display name when the roster has one,
// the email otherwise.
func (p PersonUsage) Display() string {
	if p.DisplayName != "" {
		return p.DisplayName
	}
	return p.Email
}

// AvgTokens is tokens per session, the number that separates "many small
// sessions" from "few enormous ones" when two people's totals look alike.
func (p PersonUsage) AvgTokens() int64 {
	if p.Sessions == 0 {
		return 0
	}
	return (p.TokensIn + p.TokensOut) / p.Sessions
}

// Artifact is a file a session created or changed.
type Artifact struct {
	ID           int64
	SessionID    string
	Email        string
	Path         string
	Created      bool
	FirstSeen    time.Time
	LastSeen     time.Time
	VersionCount int
	LatestSHA    string
	LatestBytes  int64
}

// Name is the last path element, which is what a list of twenty files in one
// repository is actually distinguished by. The full path is still rendered, but
// as the subtitle rather than the thing the eye has to scan.
func (a Artifact) Name() string {
	if i := strings.LastIndexByte(a.Path, '/'); i >= 0 && i+1 < len(a.Path) {
		return a.Path[i+1:]
	}
	return a.Path
}

// ShortSHA is the checksum at the length people actually compare by eye.
func (a Artifact) ShortSHA() string {
	if len(a.LatestSHA) > 12 {
		return a.LatestSHA[:12]
	}
	return a.LatestSHA
}

// ArtifactVersion is one content state of a file.
type ArtifactVersion struct {
	ArtifactID int64
	EventID    string
	SHA256     string
	Bytes      int64
	Created    bool
	OccurredAt time.Time
}

// ShortSHA is the checksum at the length people compare by eye.
func (v ArtifactVersion) ShortSHA() string {
	if len(v.SHA256) > 12 {
		return v.SHA256[:12]
	}
	return v.SHA256
}

// Link is a URL a session mentioned.
type Link struct {
	SessionID   string
	URL         string
	Kind        string
	Host        string
	Ref         string
	FirstSeen   time.Time
	LastSeen    time.Time
	Occurrences int
}

// Label is what the link renders as. The classified short form when there is
// one — "loopai-hq/agent-sessions#4" reads as a pull request in a way the URL
// does not — and otherwise the URL with its scheme stripped, because "https://"
// repeated down a column carries no information.
func (l Link) Label() string {
	if l.Ref != "" {
		return l.Ref
	}
	s := strings.TrimPrefix(l.URL, "https://")
	s = strings.TrimPrefix(s, "http://")
	if len(s) > 72 {
		return s[:69] + "..."
	}
	return s
}

// ErrNotFound is the single answer to "no such session" and to "not yours".
//
// Handlers render both as a 404 with identical wording. Storage should return
// this for a denied read rather than a distinct permission error, but the
// handlers also map ErrDenied here for a store that reports denial explicitly,
// so the leak cannot happen by accident on either side of the interface.
var ErrNotFound = errors.New("web: not found")

// ErrDenied exists so a store that wants to be explicit about refusing an
// action has a way to say so. For reads it is indistinguishable from
// ErrNotFound at the HTTP boundary by design; for writes, where the actor
// already knows the target exists, it renders as a plain refusal.
var ErrDenied = errors.New("web: denied")

// UserError carries a message that is safe and useful to show to the person
// who triggered it, such as the last-admin guard refusing a demotion. Any
// other error renders as a generic failure, because storage error text is
// written for operators and routinely names hosts, columns and constraints.
type UserError struct{ Message string }

func (e UserError) Error() string { return e.Message }

// Viewer is the signed-in person, resolved by the auth layer and handed to
// every storage call so reads can be attributed.
type Viewer struct {
	Email string
	Name  string
	// Admin grants read access to everyone's sessions and to the admin pages.
	// It is a property of the principals row, re-read per request rather than
	// baked into the cookie, so revoking someone takes effect on their next
	// click instead of on their next sign-in.
	Admin bool
}

// Display prefers the human name and falls back to the email, so a principal
// added before anyone filled in a display name still renders as something.
func (v Viewer) Display() string {
	if v.Name != "" {
		return v.Name
	}
	return v.Email
}

// Session is the rollup shown in the list and at the top of the detail page.
// It intentionally carries enough to make a list row useful without a second
// query: the list is the page people live in, and a row that needs a click to
// tell you what it cost is a row that gets clicked for no reason.
type Session struct {
	ID       string
	ParentID string
	Email    string
	Name     string
	Source   string
	// Type is "user" for a session a person started and "automation" for one a
	// machine started (headless -p, SDK, cron).
	Type   string
	Repo   string
	Cwd    string
	Branch string

	// StartedAt and EndedAt are event times. IngestedAt is when we received
	// it, and is shown only where the distinction matters, so a backfilled
	// session reads exactly like a live one everywhere else.
	StartedAt  time.Time
	EndedAt    time.Time
	IngestedAt time.Time
	Ended      bool

	UserTurns int
	ToolCalls int
	Subagents int
	Errors    int
	Events    int64

	FirstPrompt     string
	HarnessVersions []string

	TokensInput      int64
	TokensOutput     int64
	TokensCacheRead  int64
	TokensCacheWrite int64

	// CostUSD is the figure computed and stored at ingest. It is a float here
	// because this value is only ever formatted for display; the authoritative
	// NUMERIC stays in the database and is never recomputed from this copy.
	CostUSD float64

	Redactions map[string]int
}

// Duration is the session's wall-clock span. Sessions that never ended are
// measured to their last event rather than to now, because a session that
// stopped producing events three days ago did not run for three days.
func (s Session) Duration() time.Duration {
	if s.StartedAt.IsZero() || s.EndedAt.Before(s.StartedAt) {
		return 0
	}
	return s.EndedAt.Sub(s.StartedAt)
}

// SessionDetail adds what only the detail page needs, so the list query stays
// a single table scan.
type SessionDetail struct {
	Session

	// Via is how the viewer got here: own, admin or share. Rendered as a
	// banner for anything that is not "own", because someone reading a
	// colleague's transcript should see that the read was recorded rather
	// than discover it later in an audit report.
	Via string

	// Agents summarises the subagents this session spawned, which is the only
	// affordance that works when the transcript is paginated: the agent that
	// did the interesting work may be on page nine.
	Agents []Agent

	// Continuations are sessions that resumed from this one. A single logical
	// task spread over several session files reads as several unrelated
	// sessions without this link.
	Continuations []Session

	// Shares are the live grants on this session, shown to the owner and to
	// admins so a forgotten link is visible rather than merely revocable.
	Shares []Share
}

// Agent is one subagent's footprint within a session.
type Agent struct {
	AgentID    string
	WorkflowID string
	Name       string
	FirstSeq   int64
	LastSeq    int64
	ToolCalls  int
	Errors     int
	StartedAt  time.Time
	EndedAt    time.Time
}

// Share is a live grant on one session.
type Share struct {
	ID        string
	CreatedBy string
	Grantee   string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// SessionQuery is the list filter. Every field is optional and an empty query
// means "everything this viewer may see, newest first".
type SessionQuery struct {
	Q      string
	Email  string
	Source string
	Repo   string
	// Types narrows to the given session types; empty means every type.
	Types []string
	From  time.Time
	To    time.Time

	// Cursor is opaque to the web layer and comes from a previous
	// SessionPage. Keyset pagination rather than OFFSET because the list is
	// ordered by started_at on a table that grows at the head: with OFFSET,
	// sessions arriving between two page loads shift rows across the page
	// boundary and the reader silently skips them.
	Cursor string
	Limit  int
}

// SessionPage is one screen of the list.
type SessionPage struct {
	Sessions   []Session
	NextCursor string

	// Totals summarise the whole filtered set rather than this page. Optional:
	// a store that does not want to pay for the aggregate leaves it nil and
	// the summary strip is simply not rendered.
	Totals *Totals
}

// Totals is the aggregate over a filtered set.
type Totals struct {
	Sessions int
	CostUSD  float64
	Duration time.Duration
	Errors   int
}

// EventQuery is a window into one session's event stream, in time order.
// After is an opaque cursor from a previous page's NextCursor; empty means the
// beginning. Time order rather than seq order because seq restarts per
// subagent stream, and a page of "everyone's first fifty events" reads as a
// shuffle of every thread at once.
type EventQuery struct {
	After string
	Limit int

	// AgentID, when set, restricts the window to one subagent's events. This
	// is how a reader gets to the interesting part of a long session without
	// paging through the parts they do not care about.
	AgentID string
}

// EventCursor is a position in the session timeline: (occurred_at, agent
// thread, seq). Encoded opaquely for URLs; the zero Agent/Seq with a time
// addresses "just before this moment", which is what an event page's back
// link needs.
type EventCursor struct {
	At    time.Time `json:"at"`
	Agent string    `json:"agent,omitempty"`
	Seq   int64     `json:"seq,omitempty"`
}

// Encode round-trips through an opaque URL token, so the address bar does not
// become an API for the ordering internals.
func (c EventCursor) Encode() string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeEventCursor is the inverse; garbage decodes to the zero cursor (the
// beginning), because cursors live in URLs and URLs get truncated in chat.
func DecodeEventCursor(s string) EventCursor {
	var c EventCursor
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		_ = json.Unmarshal(b, &c)
	}
	return c
}

// EventPage is one window of a session's events, in time order.
type EventPage struct {
	Events []event.Event

	// NextCursor addresses the window after this one; empty when HasMore is
	// false. Opaque to this layer.
	NextCursor string

	// HasMore reports whether events exist beyond this window in the
	// direction of travel. It is a flag rather than a total because counting
	// the remainder of a 70 MB session on every page view costs more than the
	// information is worth.
	HasMore bool

	// Total is the session's full event count when the store already knows it
	// cheaply, and zero otherwise. Used only to render "showing 200 of
	// 12,984", which is omitted when unknown.
	Total int64
}

// SearchQuery is a message-granularity full-text search.
type SearchQuery struct {
	Q      string
	Email  string
	Source string
	Repo   string
	// Types mirrors the list filter, so a type the list suppresses cannot
	// resurface as a transcript hit on the same page.
	Types []string
	From  time.Time
	To    time.Time
	Limit int
	// IncludeHarness widens the corpus to the harness's own user-role
	// messages (caveats, notifications, reminders, summaries), which the
	// default leaves out so a word the harness injects everywhere does not
	// outrank the person who typed it.
	IncludeHarness bool
}

// SearchResults are ranked hits plus the fact of capping.
type SearchResults struct {
	Hits []SearchHit

	// Capped reports that the candidate set hit its ceiling before ranking.
	// Surfaced rather than hidden: a user whose query matched half the corpus
	// should be told to narrow it, not left believing they saw everything.
	Capped bool
}

// SearchHit is one matching message. It carries the session summary inline so
// the results page can show what a hit belongs to without a lookup per row.
type SearchHit struct {
	Session Session
	EventID string
	Seq     int64
	Role    string
	// Kind is messages.kind, what the matching message is: a person's
	// prompt, a command, assistant text, tool output or a harness wrapper.
	Kind       string
	OccurredAt time.Time
	Text       string
}

// Principal is a row of the admin page.
type Principal struct {
	Email       string
	DisplayName string
	Role        string
	AddedBy     string
	AddedAt     time.Time
	DisabledAt  time.Time

	// LastSeen and Sessions give the admin page a reason to exist beyond role
	// editing: they are how you notice that someone enrolled two months ago
	// and has never sent anything.
	LastSeen time.Time
	Sessions int
	Devices  int
}

// Disabled is a method rather than a field so the zero DisabledAt cannot be
// misread as "disabled at the epoch" by a template.
func (p Principal) Disabled() bool { return !p.DisabledAt.IsZero() }

// PrincipalUpdate is one edit from the admin page. Both fields are always
// sent, so a form submission fully describes the intended end state rather
// than a delta against whatever the row happened to say when the page loaded.
type PrincipalUpdate struct {
	Email    string
	Role     string
	Disabled bool
}

// Roles, kept as constants because they are compared in the last-admin guard
// and a typo there fails open.
const (
	RoleAdmin  = "admin"
	RoleMember = "member"
)

// Fleet is client coverage: who is enrolled, who is reporting, who has gone
// quiet. Derived entirely from health reports, because a laptop with no cloud
// credentials has no other way to tell us anything.
type Fleet struct {
	Machines []Machine

	Enrolled  int
	Reporting int
	Degraded  int
	Critical  int

	// Silent counts machines that reported once and stopped. This is the
	// number that matters: a machine that never reported at all is a failed
	// install someone knows about, while a machine that went quiet is a
	// coverage hole nobody has noticed.
	Silent int

	// VersionFirstSeen is when each agent version first appeared in a health
	// report, which is what turns a bare hash into an age on the page.
	VersionFirstSeen map[string]time.Time
}

// Machine is one enrolled device's last known state. The health report is
// embedded verbatim rather than flattened so the page can show new conditions
// the moment a client starts emitting them, without a server change.
type Machine struct {
	Email      string
	Name       string
	DeviceID   string
	ReceivedAt time.Time
	Silent     bool
	Report     health.Report
}

// Age is how long ago this machine last reported, which is the fleet page's
// primary sort key: staleness is the failure this page exists to catch.
func (m Machine) Age(now time.Time) time.Duration {
	if m.ReceivedAt.IsZero() {
		return 0
	}
	return now.Sub(m.ReceivedAt)
}

// AccessQuery filters the audit view.
type AccessQuery struct {
	Viewer    string
	SessionID string
	Owner     string
	From      time.Time
	To        time.Time
	Limit     int
}

// AccessEntry is one recorded read of somebody else's session.
type AccessEntry struct {
	Viewer    string
	SessionID string
	Owner     string
	Via       string
	At        time.Time
}
