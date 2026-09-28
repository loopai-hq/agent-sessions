package app

// This file binds *store.Store to the port server/web declares. It is the widest
// of the adapters because the dashboard carries the richest shapes in the server,
// and most of what follows is field-for-field translation. Four things in it are
// not, and each is here because the store has no column for the answer.
//
// Via is re-derived. The store decides how a read was justified inside the
// transaction that audits it and does not hand that decision back, so this file
// works it out again with the same precedence and names it with the store's own
// constants. The banner on a session page tells a reader their read was recorded;
// if it named a different justification from the audit row it would be describing
// a log entry that does not exist.
//
// Shares are narrowed to the live ones. store.ListShares returns revoked and
// expired grants as well, because the API has to be able to explain a link that
// used to work, and web.Share has no field to say so. A dead grant listed under
// "active shares" reads to an owner as a live link on their session.
//
// Fleet coverage is counted per machine. Somebody with a working laptop and a
// dead one is not covered, and a per-person roll-up reports them as healthy
// because the working machine answers for both.
//
// And where the store cannot answer at all, the call fails rather than returning
// a shape the page will render as a fact. A filter accepted and then dropped is
// worse than one that does not exist: the reader takes the narrowed page for the
// whole answer.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/server/fleet"
	"github.com/loopai-hq/agent-sessions/server/store"
	"github.com/loopai-hq/agent-sessions/server/web"
)

// webData adapts the store to the dashboard's persistence port.
type webData struct {
	s *store.Store

	// now is the clock coverage is measured against. It is a field rather than a
	// call to time.Now so the fleet page's silence threshold can be tested at a
	// fixed instant instead of by waiting a day for one to be crossed.
	now func() time.Time

	// fleet is the evaluator the admin page reads its view from, the same
	// one the ticks run on so the page and the alerts share a manifest and a
	// server build. Nil builds one over the store with no manifest, which
	// is what a test's construction gets.
	fleet *fleet.Runner
}

// NewWebData builds the dashboard's view of the store.
func NewWebData(s *store.Store) web.Data { return webData{s: s, now: time.Now} }

// newWebData is NewWebData with the fleet evaluator the server runs.
func newWebData(s *store.Store, r *fleet.Runner) web.Data {
	return webData{s: s, now: time.Now, fleet: r}
}

// errWebUnsupported marks a query the dashboard can express and the store cannot
// answer.
//
// It is returned instead of the best available approximation on purpose. Every
// alternative is a page that states something untrue: an unfiltered result set
// under a filter the user typed, or a window of the wrong events under a "page
// back" link. A failure is visible, gets fixed, and cannot be mistaken for data.
var errWebUnsupported = errors.New("app: the store cannot answer this dashboard query")

// fleetStaleAfter is how long a machine may stay quiet before it counts as
// silent. A day, because these are laptops: they are shut overnight, over
// weekends and through holidays, and a shorter window fills the page with people
// who are asleep. It is the same figure server/admin uses, since the two pages
// answer the same question and a fleet that reads healthy on one and silent on
// the other is a pair of pages nobody trusts.
const fleetStaleAfter = 24 * time.Hour

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

// ListSessions returns the sessions this viewer may see. The store's predicate
// does the narrowing; this method only reshapes the rows.
func (d webData) ListSessions(ctx context.Context, v web.Viewer, q web.SessionQuery) (web.SessionPage, error) {
	page, err := d.s.ListSessions(ctx, webStoreViewer(v), store.SessionFilter{
		Query:  q.Q,
		Email:  q.Email,
		Source: q.Source,
		Repo:   q.Repo,
		Types:  q.Types,
		From:   q.From,
		To:     q.To,
		Cursor: q.Cursor,
		Limit:  q.Limit,
	})
	if err != nil {
		return web.SessionPage{}, webReadError("list sessions", err)
	}

	out := web.SessionPage{
		Sessions:   make([]web.Session, 0, len(page.Sessions)),
		NextCursor: page.NextCursor,
		// Totals stays nil, which the port defines as "not rendered". The store
		// has no aggregate over a filtered set, and the honest way to say that on
		// a summary strip is to leave the strip off. A zeroed Totals would put
		// "0 sessions, $0.00" above a page that is visibly showing sessions.
	}
	for _, s := range page.Sessions {
		// The owner's display name is left empty here rather than looked up per
		// row: the list is fifty rows wide and a name lookup each is fifty
		// queries for a column the template already falls back to the address
		// for. The detail page, where it is one query, does fill it.
		out.Sessions = append(out.Sessions, toWebSession(s, ""))
	}
	return out, nil
}

// Session returns one session's rollup. The store authorises and audits the read
// in one transaction; everything after that is assembly.
func (d webData) Session(ctx context.Context, v web.Viewer, id string) (web.SessionDetail, error) {
	sv := webStoreViewer(v)
	sess, err := d.s.GetSession(ctx, sv, id)
	if err != nil {
		return web.SessionDetail{}, webReadError("read session", err)
	}
	return d.sessionDetail(ctx, sv, v, sess)
}

// ResolveShare exchanges a link secret for the session it grants.
//
// The resolved grant itself is discarded. The detail page's share list is the set
// of grants the store decided this viewer may administer, which for somebody
// arriving on a link is none, and adding the link they came in on would make that
// list mean two different things depending on how the page was reached.
func (d webData) ResolveShare(ctx context.Context, v web.Viewer, token string) (web.SessionDetail, error) {
	sv := webStoreViewer(v)
	sess, _, err := d.s.ResolveShare(ctx, sv, token)
	if err != nil {
		return web.SessionDetail{}, webReadError("resolve share link", err)
	}
	return d.sessionDetail(ctx, sv, v, sess)
}

// sessionDetail assembles the detail page from an already-authorised session, so
// the share path and the direct path cannot drift into showing different things
// about the same session.
func (d webData) sessionDetail(ctx context.Context, sv store.Viewer, v web.Viewer, sess store.Session) (web.SessionDetail, error) {
	name, err := d.ownerName(ctx, sess.Email)
	if err != nil {
		return web.SessionDetail{}, err
	}
	shares, err := d.s.ListShares(ctx, sv, sess.SessionID)
	if err != nil {
		return web.SessionDetail{}, webReadError("list shares on session", err)
	}
	return web.SessionDetail{
		Session: toWebSession(sess, name),
		Via:     webVia(v, sess.Email),
		Shares:  liveShares(shares, d.now()),
		// Agents and Continuations stay nil because the store cannot answer
		// either question: there is no per-session agent roll-up and no way to
		// select sessions by parent id. Both are guarded by {{if}} in the
		// template, so an empty one omits its section rather than asserting that
		// a session with three subagents had none. Filling them needs a store
		// method, not a scan of the transcript from here.
	}, nil
}

// ownerName resolves the display name shown beside a session.
//
// A principal who has been removed from the roster is not an error: their
// sessions outlive their access, and the page falls back to the address. A
// failure to ask, on the other hand, is returned. An empty name substituted for a
// failed read is indistinguishable from a person who never set one, so the page
// would silently degrade for everybody the moment the query started failing.
func (d webData) ownerName(ctx context.Context, email string) (string, error) {
	p, found, err := d.s.PrincipalLookup(ctx, email)
	if err != nil {
		return "", fmt.Errorf("app: display name for session owner: %w", err)
	}
	if !found {
		return "", nil
	}
	return p.DisplayName, nil
}

// Events returns a window of a session's events, in time order, optionally
// narrowed to one subagent thread.
func (d webData) Events(ctx context.Context, v web.Viewer, id string, q web.EventQuery) (web.EventPage, error) {
	tr := store.TimelineRange{AgentID: q.AgentID, Limit: q.Limit}
	if q.After != "" {
		if c := web.DecodeEventCursor(q.After); !c.At.IsZero() {
			tr.After = &store.TimelineCursor{At: c.At, Agent: c.Agent, Seq: c.Seq}
		}
	}
	page, err := d.s.GetTimeline(ctx, webStoreViewer(v), id, tr)
	if err != nil {
		return web.EventPage{}, webReadError("read transcript window", err)
	}

	out := web.EventPage{
		Events:  make([]event.Event, 0, len(page.Events)),
		HasMore: page.HasMore,
		// Total stays zero, which the port defines as "unknown" and the template
		// renders by omitting the "of 12,984" suffix. The store does not count a
		// session's events, and a count of the window presented as the count of
		// the session would tell a reader they had seen all of it.
	}
	for _, se := range page.Events {
		e, err := toEvent(se)
		if err != nil {
			return web.EventPage{}, err
		}
		out.Events = append(out.Events, e)
	}
	if page.HasMore && len(page.Events) > 0 {
		last := page.Events[len(page.Events)-1]
		out.NextCursor = web.EventCursor{
			At: last.OccurredAt, Agent: last.AgentID, Seq: last.Seq,
		}.Encode()
	}
	return out, nil
}

// Event returns one event in full.
//
// The store cannot do this. Every read it offers pages a session by seq, and an
// event id carries no seq, so finding one from here means walking the session
// from its first event: tens of megabytes of transfer and one audit row per page
// for a single record. Reporting the event as absent instead would be worse than
// the failure, because the transcript page linked to it a moment ago and the
// reader would be told something they can see does not exist.
func (d webData) Event(ctx context.Context, v web.Viewer, sessionID, eventID string) (event.Event, error) {
	return event.Event{}, fmt.Errorf(
		"%w: one event by id needs a store read keyed on (session_id, id), authorised and audited like store.GetSession",
		errWebUnsupported)
}

// Search runs message-granularity search scoped to what the viewer may read.
func (d webData) Search(ctx context.Context, v web.Viewer, q web.SearchQuery) (web.SearchResults, error) {
	if q.Repo != "" {
		// The search form offers this box, so this is reachable by typing. It
		// still refuses: store.SearchFilter has no repo, and messages carry no
		// repo column, so honouring it would mean returning every repo's hits
		// under a heading that says otherwise.
		return web.SearchResults{}, fmt.Errorf(
			"%w: narrowing search to a repository needs store.SearchFilter to carry one, resolved against sessions the way Source already is",
			errWebUnsupported)
	}

	res, err := d.s.SearchMessages(ctx, webStoreViewer(v), store.SearchFilter{
		Query:          q.Q,
		Email:          q.Email,
		Source:         q.Source,
		Types:          q.Types,
		From:           q.From,
		To:             q.To,
		Limit:          q.Limit,
		IncludeHarness: q.IncludeHarness,
	})
	if err != nil {
		return web.SearchResults{}, webReadError("search messages", err)
	}

	out := web.SearchResults{
		Hits:   make([]web.SearchHit, 0, len(res.Hits)),
		Capped: res.Capped,
	}
	for _, h := range res.Hits {
		out.Hits = append(out.Hits, web.SearchHit{
			// The store's search returns the message row and joins nothing, so
			// the session summary is only as complete as that row: an id and an
			// owner. The results template falls back to the short id when there
			// is no repo and to the address when there is no name, so the page
			// says less rather than something untrue. Filling the rest would be a
			// session read per hit, each writing a second audit row for a read
			// SearchMessages has already recorded.
			Session:    web.Session{ID: h.SessionID, Email: h.Email},
			EventID:    h.EventID,
			Seq:        h.Seq,
			Role:       h.Role,
			Kind:       h.Kind,
			OccurredAt: h.OccurredAt,
			Text:       h.Snippet,
		})
	}
	return out, nil
}

// Turns returns one page of a session's turns as the runner folded them.
// The store authorises and audits the read; this method reshapes rows and
// decodes each event's body the way Events does, so a body that will not
// decode fails the page rather than leaving a hole in a turn.
func (d webData) Turns(ctx context.Context, v web.Viewer, id string, q web.TurnQuery) (web.TurnPage, error) {
	page, err := d.s.GetTurns(ctx, webStoreViewer(v), id, store.TurnRange{
		Thread: q.Thread,
		After:  q.After,
		Limit:  q.Limit,
	})
	if err != nil {
		return web.TurnPage{}, webReadError("read turns", err)
	}
	out := web.TurnPage{HasMore: page.HasMore, NextAfter: page.NextAfter, EventsCapped: page.EventsCapped}
	for _, t := range page.Turns {
		wt, err := toWebTurn(t)
		if err != nil {
			return web.TurnPage{}, err
		}
		out.Turns = append(out.Turns, wt)
	}
	for _, t := range page.Agents {
		wt, err := toWebTurn(t)
		if err != nil {
			return web.TurnPage{}, err
		}
		out.Agents = append(out.Agents, wt)
	}
	return out, nil
}

func toWebTurn(t store.TurnRow) (web.Turn, error) {
	out := web.Turn{
		Thread:           t.Thread,
		Index:            t.Index,
		Kind:             t.Kind,
		Outcome:          t.Outcome,
		Inherited:        t.Inherited,
		PromptEventID:    t.PromptEventID,
		FinalEventID:     t.FinalEventID,
		StartedAt:        t.StartedAt,
		LastActivityAt:   t.LastActivityAt,
		Wall:             time.Duration(t.WallMS) * time.Millisecond,
		Active:           time.Duration(t.ActiveMS) * time.Millisecond,
		Idle:             time.Duration(t.IdleMS) * time.Millisecond,
		WaitingForHuman:  time.Duration(t.WaitingForHumanMS) * time.Millisecond,
		Origins:          t.Origins,
		Merged:           t.Merged,
		Prompts:          t.Prompts,
		ToolCalls:        t.ToolCalls,
		Errors:           t.Errors,
		Subagents:        t.Subagents,
		FilesChanged:     t.FilesChanged,
		TokensInput:      t.TokensInput,
		TokensOutput:     t.TokensOutput,
		TokensCacheRead:  t.TokensCacheRead,
		TokensCacheWrite: t.TokensCacheWrite,
		CostUSD:          t.CostUSD,
		Model:            t.Model,
	}
	if t.FirstActivityAt != nil {
		out.FirstActivityAt = *t.FirstActivityAt
	}
	if t.AnsweredAt != nil {
		out.AnsweredAt = *t.AnsweredAt
	}
	for _, te := range t.Events {
		e, err := toEvent(te.StoredEvent)
		if err != nil {
			return web.Turn{}, err
		}
		out.Events = append(out.Events, web.TurnEvent{Event: e, Role: te.Role, Kind: te.Kind})
	}
	return out, nil
}

// Facets reads the lattice and lineage state of a page of sessions in one
// query, scoped by the store to what the viewer may see.
func (d webData) Facets(ctx context.Context, v web.Viewer, ids []string) (map[string]web.SessionFacet, error) {
	facets, err := d.s.SessionFacets(ctx, webStoreViewer(v), ids)
	if err != nil {
		return nil, webReadError("read session facets", err)
	}
	out := make(map[string]web.SessionFacet, len(facets))
	for id, f := range facets {
		out[id] = web.SessionFacet{
			Type:             f.Type,
			EmptyKind:        f.EmptyKind,
			HeadState:        f.HeadState,
			LineageSource:    f.LineageSource,
			ParentSessionID:  f.ParentSessionID,
			TitleSource:      f.TitleSource,
			HumanTurns:       f.HumanTurns,
			HarnessTitle:     f.HarnessTitle,
			ContentEvents:    f.ContentEvents,
			Entrypoint:       f.Entrypoint,
			CaptureLossDrops: f.CaptureLossDrops,
			FirstAnswer:      f.FirstAnswer,
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Admin surfaces
// ---------------------------------------------------------------------------

// Principals lists everyone who may use the system.
//
// The roster read is issued first because it is the only one of the three the
// store gates. The other two are viewerless fleet-wide reads by construction, and
// running them behind a call the store has already refused is what keeps a second
// copy of the admin predicate out of this file.
func (d webData) Principals(ctx context.Context, v web.Viewer) ([]web.Principal, error) {
	people, err := d.s.ListPrincipals(ctx, webStoreViewer(v))
	if err != nil {
		return nil, webReadError("list principals", err)
	}
	devices, err := d.s.EnrolledDevices(ctx)
	if err != nil {
		return nil, fmt.Errorf("app: read devices for the roster page: %w", err)
	}
	snaps, err := d.s.LatestHealth(ctx)
	if err != nil {
		return nil, fmt.Errorf("app: read health for the roster page: %w", err)
	}
	counts, err := d.s.SessionCounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("app: count sessions for the roster page: %w", err)
	}
	return webPrincipals(people, devices, snaps, counts), nil
}

// SetPrincipal changes a role or disables an account.
//
// The last-admin guard lives in the store, inside the transaction that performs
// the write, and arrives here as a sentinel. It is translated into a UserError
// because it is the one refusal on this page that is the operator's own doing and
// that they can act on; everything else renders as a generic failure, since
// storage error text names hosts, columns and constraints.
func (d webData) SetPrincipal(ctx context.Context, v web.Viewer, u web.PrincipalUpdate) (bool, error) {
	// Both fields are always sent by the form, so both are non-nil here. A nil
	// would mean "leave alone", which would turn a submission that clears the
	// disabled box into one that says nothing about it.
	role := store.Role(u.Role)
	disabled := u.Disabled

	saved, err := d.s.PutPrincipal(ctx, webStoreViewer(v), store.PrincipalUpdate{
		Email:    u.Email,
		Role:     &role,
		Disabled: &disabled,
		// DisplayName is deliberately nil. The admin form does not carry a name,
		// and a non-nil empty string is a whole-field write: every edit made from
		// this page would erase the name of the person it edited.
	})
	switch {
	case err == nil:
		// Change is nil when the submitted values already matched the row, in
		// which case the store deliberately wrote nothing at all.
		return saved.Change != nil, nil
	case errors.Is(err, store.ErrLastAdmin):
		return false, web.UserError{Message: "That would leave the system with no active admin. Promote somebody else first."}
	case errors.Is(err, store.ErrNotAdmin):
		// A write, not a read, so this does not collapse into not-found. The
		// actor is looking at the roster and already knows the person exists;
		// hiding the row from them here would answer a refusal with a lie about
		// what is on the page in front of them.
		return false, web.ErrDenied
	default:
		// Wrapped rather than returned bare so the dashboard's log line names the
		// operation. The handler matches web.UserError with errors.As and
		// web.ErrNotFound with errors.Is, and both still match through a wrap.
		return false, fmt.Errorf("app: save principal %q: %w", u.Email, err)
	}
}

// Fleet reports client coverage.
//
// Same ordering as Principals and for the same reason: the gated read runs first,
// so the two ungated fleet-wide reads are only reached by a caller the store has
// already accepted as an admin.
func (d webData) Fleet(ctx context.Context, v web.Viewer) (web.Fleet, error) {
	people, err := d.s.ListPrincipals(ctx, webStoreViewer(v))
	if err != nil {
		return web.Fleet{}, webReadError("read the roster for fleet coverage", err)
	}
	devices, err := d.s.EnrolledDevices(ctx)
	if err != nil {
		return web.Fleet{}, fmt.Errorf("app: read devices for fleet coverage: %w", err)
	}
	snaps, err := d.s.LatestHealth(ctx)
	if err != nil {
		return web.Fleet{}, fmt.Errorf("app: read health for fleet coverage: %w", err)
	}
	f := webFleet(d.now(), people, devices, snaps)
	// Best-effort: a page that cannot date the versions still names them.
	if seen, err := d.s.VersionFirstSeen(ctx); err == nil {
		f.VersionFirstSeen = seen
	}
	return f, nil
}

// FleetEvaluation is the evaluator's view for the admin page. The admin
// check is here because the evaluator's store port has none: it is a
// background component that reads the whole fleet by design.
func (d webData) FleetEvaluation(ctx context.Context, v web.Viewer) (*fleet.Evaluation, error) {
	if !webStoreViewer(v).IsAdmin() {
		return nil, web.ErrNotFound
	}
	r := d.fleet
	if r == nil {
		r = &fleet.Runner{Store: fleetStore{s: d.s}, Now: d.now}
	}
	ev, err := r.View(ctx)
	if err != nil {
		return nil, fmt.Errorf("app: evaluate the fleet for the page: %w", err)
	}
	return &ev, nil
}

// MuteFleet writes one mute; the store enforces the admin role.
func (d webData) MuteFleet(ctx context.Context, v web.Viewer, m web.FleetMute) error {
	err := d.s.PutFleetMute(ctx, webStoreViewer(v), store.FleetMute{Email: m.Email, Kind: m.Kind, Until: m.Until, Note: m.Note})
	return webWriteError(fmt.Sprintf("mute %s for %s", m.Kind, m.Email), err)
}

// UnmuteFleet lifts one mute; the store enforces the admin role.
func (d webData) UnmuteFleet(ctx context.Context, v web.Viewer, email, kind string) error {
	return webWriteError(fmt.Sprintf("unmute %s for %s", kind, email), d.s.DeleteFleetMute(ctx, webStoreViewer(v), email, kind))
}

// webWriteError maps a store refusal on a write to the dashboard's explicit
// denial (a write is never collapsed into not-found: the actor is looking
// at the row) and wraps anything else with the operation's name.
func webWriteError(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotAdmin):
		return web.ErrDenied
	default:
		return fmt.Errorf("app: %s: %w", op, err)
	}
}

// AccessLog reports who read whose sessions. The store gates it on the admin
// role; this method only reshapes the rows.
func (d webData) AccessLog(ctx context.Context, v web.Viewer, q web.AccessQuery) ([]web.AccessEntry, error) {
	rows, err := d.s.ListAccessLog(ctx, webStoreViewer(v), store.AccessLogFilter{
		SessionID: q.SessionID,
		Viewer:    q.Viewer,
		Owner:     q.Owner,
		// Since is inclusive and Until exclusive, so two adjacent windows tile
		// without counting the row on the boundary twice.
		Since: q.From,
		Until: q.To,
		Limit: q.Limit,
	})
	if err != nil {
		return nil, webReadError("read access log", err)
	}
	out := make([]web.AccessEntry, 0, len(rows))
	for _, a := range rows {
		// The row id is dropped because web.AccessEntry has nowhere to put it and
		// this page shows one window with no cursor. Paging the audit trail lives
		// on the JSON API, which does carry the id.
		out = append(out, web.AccessEntry{
			Viewer:    a.Viewer,
			SessionID: a.SessionID,
			Owner:     a.Owner,
			Via:       a.Via,
			At:        a.At,
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Artifacts and links
// ---------------------------------------------------------------------------

// Artifacts lists the files a session created or changed.
//
// The store authorizes against the session, so this method only reshapes rows.
// It returns an empty slice rather than nil for a session with none, because the
// template distinguishes "no artifacts" from "not asked" and a nil that renders
// identically to an empty one would hide the difference.
func (d webData) Artifacts(ctx context.Context, v web.Viewer, sessionID string) ([]web.Artifact, error) {
	rows, err := d.s.SessionArtifacts(ctx, webStoreViewer(v), sessionID)
	if err != nil {
		return nil, webReadError("read artifacts", err)
	}
	out := make([]web.Artifact, 0, len(rows))
	for _, a := range rows {
		out = append(out, webArtifact(a))
	}
	return out, nil
}

// Links lists the URLs a session mentioned.
func (d webData) Links(ctx context.Context, v web.Viewer, sessionID string) ([]web.Link, error) {
	rows, err := d.s.SessionLinks(ctx, webStoreViewer(v), sessionID)
	if err != nil {
		return nil, webReadError("read links", err)
	}
	out := make([]web.Link, 0, len(rows))
	for _, l := range rows {
		out = append(out, web.Link{
			SessionID:   l.SessionID,
			URL:         l.URL,
			Kind:        l.Kind,
			Host:        l.Host,
			Ref:         l.Ref,
			FirstSeen:   l.FirstSeen,
			LastSeen:    l.LastSeen,
			Occurrences: l.Occurrences,
		})
	}
	return out, nil
}

// Artifact returns one file and its whole version history.
func (d webData) Artifact(ctx context.Context, v web.Viewer, id int64) (web.Artifact, []web.ArtifactVersion, error) {
	a, versions, err := d.s.Artifact(ctx, webStoreViewer(v), id)
	if err != nil {
		return web.Artifact{}, nil, webReadError("read artifact", err)
	}
	out := make([]web.ArtifactVersion, 0, len(versions))
	for _, av := range versions {
		out = append(out, web.ArtifactVersion{
			ArtifactID: av.ArtifactID,
			EventID:    av.EventID,
			SHA256:     av.SHA256,
			Bytes:      av.Bytes,
			Created:    av.Created,
			OccurredAt: av.OccurredAt,
		})
	}
	return webArtifact(a), out, nil
}

// ArtifactContent returns the file as it stood at one version.
func (d webData) ArtifactContent(ctx context.Context, v web.Viewer, id int64, eventID string) (string, error) {
	body, err := d.s.ArtifactContent(ctx, webStoreViewer(v), id, eventID)
	if err != nil {
		return "", webReadError("read artifact content", err)
	}
	return body, nil
}

func webArtifact(a store.Artifact) web.Artifact {
	return web.Artifact{
		ID:           a.ID,
		SessionID:    a.SessionID,
		Email:        a.Email,
		Path:         a.Path,
		Created:      a.Created,
		FirstSeen:    a.FirstSeen,
		LastSeen:     a.LastSeen,
		VersionCount: a.VersionCount,
		LatestSHA:    a.LatestSHA,
		LatestBytes:  a.LatestBytes,
	}
}

// ---------------------------------------------------------------------------
// Analytics
// ---------------------------------------------------------------------------

// analyticsZone is the zone daily buckets are drawn in: the server's own local
// zone, which the deployment pins to the office's (TZ in cloudrun.yaml). The
// fallback matters because Postgres does not know Go's "Local" — an unpinned
// deployment buckets in UTC rather than failing every chart.
func analyticsZone() string {
	tz := time.Local.String()
	if tz == "" || tz == "Local" {
		return "UTC"
	}
	return tz
}

// DailyUsage returns whole-scope daily totals.
func (d webData) DailyUsage(ctx context.Context, v web.Viewer, from, to time.Time, unit string, types []string) ([]web.DayUsage, error) {
	rows, err := d.s.DailyUsage(ctx, webStoreViewer(v), from, to, analyticsZone(), unit, nil, types)
	if err != nil {
		return nil, webReadError("daily usage", err)
	}
	return webDayUsage(rows), nil
}

// DailyUsageByType returns per-bucket rows split by session type, for the
// stacked charts.
func (d webData) DailyUsageByType(ctx context.Context, v web.Viewer, from, to time.Time, unit string) ([]web.DayUsage, error) {
	rows, err := d.s.DailyUsageByType(ctx, webStoreViewer(v), from, to, analyticsZone(), unit, nil)
	if err != nil {
		return nil, webReadError("daily usage by type", err)
	}
	return webDayUsage(rows), nil
}

// DailyUsageByPerson returns per-day rows for the comparison chart.
func (d webData) DailyUsageByPerson(ctx context.Context, v web.Viewer, from, to time.Time, unit string, emails, types []string) ([]web.DayUsage, error) {
	rows, err := d.s.DailyUsageByPerson(ctx, webStoreViewer(v), from, to, analyticsZone(), unit, emails, types)
	if err != nil {
		return nil, webReadError("daily usage by person", err)
	}
	return webDayUsage(rows), nil
}

// PersonUsage returns per-person totals over the range.
func (d webData) PersonUsage(ctx context.Context, v web.Viewer, from, to time.Time, sortKey, prefix string, top int, types []string) ([]web.PersonUsage, error) {
	rows, err := d.s.PersonUsage(ctx, webStoreViewer(v), from, to, sortKey, prefix, top, types)
	if err != nil {
		return nil, webReadError("person usage", err)
	}
	out := make([]web.PersonUsage, 0, len(rows))
	for _, p := range rows {
		out = append(out, web.PersonUsage{
			Email:       p.Email,
			DisplayName: p.DisplayName,
			Sessions:    p.Sessions,
			TokensIn:    p.TokensIn,
			TokensOut:   p.TokensOut,
			CacheRead:   p.CacheRead,
			CostUSD:     p.CostUSD,
			ToolCalls:   p.ToolCalls,
			Errors:      p.Errors,
			LastActive:  p.LastActive,
		})
	}
	return out, nil
}

// FilterOptions returns the dropdown values the viewer may see.
func (d webData) FilterOptions(ctx context.Context, v web.Viewer) (people, repos []string, err error) {
	people, repos, err = d.s.FilterOptions(ctx, webStoreViewer(v))
	if err != nil {
		return nil, nil, webReadError("filter options", err)
	}
	return people, repos, nil
}

func webDayUsage(rows []store.DayUsage) []web.DayUsage {
	out := make([]web.DayUsage, 0, len(rows))
	for _, r := range rows {
		out = append(out, web.DayUsage{
			Day: r.Day, Email: r.Email, SessionType: r.SessionType, Sessions: r.Sessions, People: r.People,
			TokensIn: r.TokensIn, TokensOut: r.TokensOut,
			CacheRead: r.CacheRead, CacheWrite: r.CacheWrite,
			CostUSD: r.CostUSD, ToolCalls: r.ToolCalls, Errors: r.Errors,
		})
	}
	return out
}

// ---------------------------------------------------------------------------
// Identity and errors
// ---------------------------------------------------------------------------

// webStoreViewer turns the dashboard's identity into the store's.
//
// The admin bit is carried rather than re-derived: the auth layer re-reads it
// from the principals row on every request, so a revoked admin loses everything
// on their next click, and a second lookup here would answer the same question
// from the same table one round trip later.
func webStoreViewer(v web.Viewer) store.Viewer {
	role := store.RoleMember
	if v.Admin {
		role = store.RoleAdmin
	}
	return store.Viewer{Email: v.Email, Role: role}
}

// webVia names how an allowed read was justified, in the store's own vocabulary
// and with the store's own precedence: your own work first, then the admin role,
// then an explicit share.
//
// It is derived because the store settles the question inside the transaction it
// audits in and does not return the answer. The page uses it for the banner that
// tells a reader their read was recorded, so a disagreement between the two would
// have the banner describe an audit row that says something else.
func webVia(v web.Viewer, owner string) string {
	switch {
	case owner == v.Email:
		return store.AccessViaOwn
	case v.Admin:
		return store.AccessViaAdmin
	default:
		return store.AccessViaShare
	}
}

// webReadError collapses everything the store can say about a read it would not
// perform into the single answer the dashboard is allowed to give.
//
// Absence and denial arrive as one value, and it is the bare sentinel rather than
// a wrapped one: a wrapper carries text, text differs between the two cases, and
// a caller comparing errors or a log line quoting one would separate what this
// exists to keep together. Confirming that a session exists is confirming that a
// named colleague ran something at a particular time. That is also why op is not
// applied to this branch: an operation name is exactly the sort of detail that
// would make the two refusals distinguishable again.
//
// Nothing else is widened. An unknown failure keeps its cause and gains the name
// of the read that failed, because a database outage rendered as "no such
// session" looks exactly like a person who has never run anything, and because
// the dashboard's own log line for a failure says only that the page did not
// load. store.ErrInvalidCursor lands in that branch deliberately: the only
// cursors the dashboard issues are ones it was handed, so a rejected one is
// either tampering or a deploy that changed the cursor format, and both are worth
// a loud failure rather than a page that silently restarts from the top.
func webReadError(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrNotAdmin):
		return web.ErrNotFound
	default:
		return fmt.Errorf("app: %s: %w", op, err)
	}
}

// ---------------------------------------------------------------------------
// Translation
// ---------------------------------------------------------------------------

// toWebSession reshapes the stored rollup.
//
// EndedAt is a pointer in the store and a value here, and the nil case is the
// interesting one. The column holds the greatest event time seen so far, not the
// end marker, which is what Ended records separately; a session that never ended
// is therefore measured to its last event and not to now, and one that has no
// events yet gets the zero time, which the port's Duration reads as zero rather
// than as fifty-six years.
func toWebSession(s store.Session, ownerName string) web.Session {
	var endedAt time.Time
	if s.EndedAt != nil {
		endedAt = *s.EndedAt
	}
	return web.Session{
		ID:       s.SessionID,
		ParentID: s.ParentSessionID,
		Email:    s.Email,
		Name:     ownerName,
		Source:   s.Source,
		Type:     s.Type,
		Repo:     s.Repo,
		Cwd:      s.Cwd,
		Branch:   s.GitBranch,

		StartedAt:  s.StartedAt,
		EndedAt:    endedAt,
		IngestedAt: s.IngestedAt,
		Ended:      s.Ended,

		UserTurns: s.UserTurns,
		ToolCalls: s.ToolCalls,
		Subagents: s.Subagents,
		Errors:    s.Errors,
		// Events stays zero. The rollup counts turns, tools, subagents and
		// errors; it does not count events, and no template renders this field,
		// so leaving it unset states nothing rather than stating nought.

		FirstPrompt:     s.FirstPrompt,
		HarnessVersions: s.HarnessVersions,

		TokensInput:      s.TokensInput,
		TokensOutput:     s.TokensOutput,
		TokensCacheRead:  s.TokensCacheRead,
		TokensCacheWrite: s.TokensCacheWrite,
		CostUSD:          s.CostUSD,
		Redactions:       s.Redactions,
	}
}

// liveShares keeps the grants that would still let somebody in.
//
// The two exclusions match the store's own read predicate exactly, including the
// boundary: a share expiring at this instant is refused by "expires_at > now()"
// there and dropped here. A grant this list showed and that predicate would
// refuse is a link an owner believes is live, which is the wrong half of the pair
// to be wrong about.
//
// The link secret is not carried across. web.Share has no field for it and the
// page has no use for it, and a token in a rendered page is a credential in
// somebody's browser history.
func liveShares(shares []store.Share, now time.Time) []web.Share {
	var out []web.Share
	for _, sh := range shares {
		if sh.RevokedAt != nil {
			continue
		}
		if sh.ExpiresAt != nil && !sh.ExpiresAt.After(now) {
			continue
		}
		var expires time.Time
		if sh.ExpiresAt != nil {
			expires = *sh.ExpiresAt
		}
		out = append(out, web.Share{
			ID:        sh.ID,
			CreatedBy: sh.CreatedBy,
			Grantee:   sh.Grantee,
			CreatedAt: sh.CreatedAt,
			ExpiresAt: expires,
		})
	}
	return out
}

// toEvent rebuilds the delivered record and then lets the indexed columns
// overrule it.
//
// The body is the bytes the laptop sent, kept verbatim so a later parser change
// can be replayed against what actually arrived. The columns beside it are what
// the server indexed, ordered and authorised on. Where the two disagree the
// columns win: the transcript is ordered by the stored seq and attributed to the
// stored session, so a body claiming another seq would render one event inside
// another's ordering, and one claiming another session id would put a row from
// somebody else's transcript on this page.
//
// A body that will not decode fails the window. Dropping it instead would leave a
// page that looks complete, in seq order, with an event missing from the middle
// and nothing to say so.
func toEvent(se store.StoredEvent) (event.Event, error) {
	var e event.Event
	if err := json.Unmarshal(se.Body, &e); err != nil {
		return event.Event{}, fmt.Errorf("app: decode stored event %s: %w", se.ID, err)
	}
	e.ID = se.ID
	e.SessionID = se.SessionID
	e.Seq = se.Seq
	e.Type = event.Type(se.Type)
	e.Origin = event.Origin(se.Origin)
	e.OccurredAt = se.OccurredAt
	e.AgentID = se.AgentID
	e.WorkflowID = se.WorkflowID
	e.Model = se.Model
	return e, nil
}

// webPrincipals joins the roster to what the fleet knows about each person.
//
// The columns this page shows beyond the role are not on the roster row: device
// and last-seen come from the same fleet-wide reads the coverage page uses, and
// the session count from a GROUP BY over the sessions rollup. The count shipped
// as a hard-coded zero once, with a comment admitting it; the store method
// exists so that cannot quietly happen again.
func webPrincipals(people []store.Principal, devices []store.Device, snaps []store.HealthSnapshot, sessions map[string]int64) []web.Principal {
	// Revoked machines are not counted. The column answers "how many laptops does
	// this person have enrolled", and a withdrawn credential is one they no longer
	// have; counting it would overstate coverage on the page where somebody
	// decides whether a person is covered. The fleet page is where a revoked
	// machine still has to be visible, and it keeps them.
	live := map[string]int{}
	for _, dev := range devices {
		if dev.RevokedAt == nil {
			live[dev.Email]++
		}
	}
	// Last seen is measured by arrival, not by the emitted stamp. emitted_at is
	// the reporting machine's own clock, so a laptop whose clock jumped forward
	// would sit at the top of this column forever and one whose clock jumped back
	// would read as missing while it reports every minute.
	lastSeen := map[string]time.Time{}
	for _, s := range snaps {
		if s.ReceivedAt.After(lastSeen[s.Email]) {
			lastSeen[s.Email] = s.ReceivedAt
		}
	}

	out := make([]web.Principal, 0, len(people))
	for _, p := range people {
		var disabledAt time.Time
		if p.DisabledAt != nil {
			disabledAt = *p.DisabledAt
		}
		out = append(out, web.Principal{
			Email:       p.Email,
			DisplayName: p.DisplayName,
			Role:        string(p.Role),
			AddedBy:     p.AddedBy,
			AddedAt:     p.AddedAt,
			DisabledAt:  disabledAt,
			LastSeen:    lastSeen[p.Email],
			Sessions:    int(sessions[p.Email]),
			Devices:     live[p.Email],
		})
	}
	return out
}

// webMachineKey identifies one machine. Keyed by person and device rather than by
// device alone because an empty device id is legitimate: reports that predate
// enrolment still prove their sender is alive, and they must not all collapse
// into one row shared across the fleet.
type webMachineKey struct {
	email    string
	deviceID string
}

// webFleet derives client coverage from the roster, the devices table and the
// newest report from each machine.
//
// It is a pure function of its inputs and the supplied clock so the states that
// matter can be tested without waiting a day for one to occur.
//
// The counters are per machine and they add up: every machine listed is counted
// once in Enrolled, and Reporting, Silent and the never-reported remainder
// partition it. Degraded and Critical are levels rather than states and overlap
// with Reporting by design.
func webFleet(now time.Time, people []store.Principal, devices []store.Device, snaps []store.HealthSnapshot) web.Fleet {
	names := make(map[string]string, len(people))
	for _, p := range people {
		if p.DisplayName != "" {
			names[p.Email] = p.DisplayName
		}
	}

	newest := make(map[webMachineKey]store.HealthSnapshot, len(snaps))
	for _, s := range snaps {
		k := webMachineKey{email: s.Email, deviceID: s.DeviceID}
		if prev, ok := newest[k]; ok && prev.ReceivedAt.After(s.ReceivedAt) {
			continue
		}
		newest[k] = s
	}

	var f web.Fleet
	seen := make(map[webMachineKey]bool, len(devices))
	for _, dev := range devices {
		k := webMachineKey{email: dev.Email, deviceID: dev.ID}
		seen[k] = true
		snap, reported := newest[k]
		if !reported && dev.RevokedAt != nil {
			// Revoked and quiet is the intended end state of a revocation rather
			// than a coverage gap. A revoked machine that is still reporting is
			// the opposite and stays on the page.
			continue
		}
		f.Machines = append(f.Machines, webMachine(now, dev, snap, reported, names[dev.Email]))
	}
	for k, snap := range newest {
		if seen[k] {
			continue
		}
		// A report is proof the machine exists whatever the devices table says.
		// Dropping it would hide data arriving from a machine nobody has a record
		// of, which is the finding, not the noise.
		f.Machines = append(f.Machines, web.Machine{
			Email:      snap.Email,
			Name:       names[snap.Email],
			DeviceID:   snap.DeviceID,
			ReceivedAt: snap.ReceivedAt,
			Silent:     now.Sub(snap.ReceivedAt) > fleetStaleAfter,
			Report:     snap.Report,
		})
	}

	// Sorted so an otherwise unchanged fleet does not shuffle between refreshes.
	// The handler re-sorts by severity with a stable sort, so this order is what
	// survives inside each severity band, and the map walk above has no order of
	// its own at all.
	sort.SliceStable(f.Machines, func(i, j int) bool {
		if f.Machines[i].Email != f.Machines[j].Email {
			return f.Machines[i].Email < f.Machines[j].Email
		}
		return f.Machines[i].DeviceID < f.Machines[j].DeviceID
	})

	for _, m := range f.Machines {
		f.Enrolled++
		switch {
		case m.ReceivedAt.IsZero():
			// Enrolled and never heard from. Not silent: the port reserves that
			// count for machines that reported once and stopped, which is the
			// coverage hole nobody has noticed, as against a failed install
			// somebody already knows about.
		case m.Silent:
			f.Silent++
		default:
			f.Reporting++
			// Levels are counted only for machines that are reporting, because
			// the page renders a silent machine's pill as "silent" rather than as
			// its last level. Counting a silent machine as critical as well would
			// put a number on a card with no row under it to explain the number.
			//
			// The level comes from the report's own conditions, which is what the
			// page derives the pill from. The stored, agent-assigned level would
			// be the better source, and there is nowhere on web.Machine to put it.
			switch m.Report.Worst() {
			case health.LevelCritical:
				f.Critical++
			case health.LevelDegraded:
				f.Degraded++
			}
		}
	}
	return f
}

// webMachine describes one enrolled machine's last known state.
func webMachine(now time.Time, dev store.Device, snap store.HealthSnapshot, reported bool, name string) web.Machine {
	m := web.Machine{
		Email:    dev.Email,
		Name:     name,
		DeviceID: dev.ID,
	}
	if reported {
		m.ReceivedAt = snap.ReceivedAt
		m.Report = snap.Report
		// Silence is measured from arrival for the same reason last-seen is: a
		// machine cannot be trusted to say when it last spoke.
		m.Silent = now.Sub(snap.ReceivedAt) > fleetStaleAfter
		return m
	}
	// A machine that has never reported still gets a row, carrying what
	// enrolment recorded about it so the line names a host instead of rendering
	// blank. Nothing here is invented: these four values are what the laptop sent
	// when it enrolled. The conditions list stays empty because conditions are the
	// agent's own judgement of itself, and a server-side guess at one would
	// contradict the machine's own report the moment it arrives.
	m.Report = health.Report{
		Hostname:     dev.Hostname,
		OS:           dev.OS,
		Arch:         dev.Arch,
		AgentVersion: dev.AgentVersion,
	}
	return m
}

// ---------------------------------------------------------------------------
// Skills
// ---------------------------------------------------------------------------

// The skills page's port (server/web/skills.go): field-for-field
// translation of the store's rows. The scoping is the store's; the one
// error mapping is the read one, so a member's compliance read, which the
// page never issues, would arrive as not-found rather than a failure.

func webSkillQuery(q web.SkillQuery) store.SkillFilter {
	return store.SkillFilter{Range: q.Range, Platform: q.Platform, Repo: q.Repo, Types: q.Types, Trigger: q.Trigger, Trust: q.Trust,
		IncludeClaimed: q.IncludeClaimed, TZ: q.TZ, Since: q.Since, Sort: q.Sort, Q: q.Q}
}

func (d webData) SkillSummary(ctx context.Context, v web.Viewer, q web.SkillQuery) (web.SkillSummary, error) {
	sum, err := d.s.SkillSummary(ctx, webStoreViewer(v), webSkillQuery(q))
	if err != nil {
		return web.SkillSummary{}, webReadError("skill summary", err)
	}
	out := web.SkillSummary{From: sum.From, To: sum.To, Totals: web.SkillTotals{
		Invocations: sum.Totals.Invocations, Skills: sum.Totals.Skills, People: sum.Totals.People, Sessions: sum.Totals.Sessions,
		User: sum.Totals.User, Agent: sum.Totals.Agent, Success: sum.Totals.Success, Error: sum.Totals.Error, Started: sum.Totals.Started, Claimed: sum.Totals.Claimed,
	}}
	for _, b := range sum.ByBucket {
		out.ByBucket = append(out.ByBucket, web.SkillBucketRow{Bucket: b.Bucket, Platform: b.Platform, Trigger: b.Trigger, Trust: b.Trust, Invocations: b.Invocations})
	}
	for _, r := range sum.BySkill {
		row := web.SkillSkillRow{Lineage: r.Lineage, SourceRepo: r.SourceRepo, Plugin: r.Plugin, Skill: r.Skill, Invocations: r.Invocations, People: r.People,
			Sessions: r.Sessions, User: r.User, Agent: r.Agent, Err: r.Err, Platforms: r.Platforms, Repos: r.Repos, LastUsedAt: r.LastUsedAt}
		for _, c := range r.Copies {
			row.Copies = append(row.Copies, web.SkillCopy{SourceRepo: c.SourceRepo, Plugin: c.Plugin, Skill: c.Skill, Invocations: c.Invocations})
		}
		out.BySkill = append(out.BySkill, row)
	}
	for _, r := range sum.ByPlatform {
		out.ByPlatform = append(out.ByPlatform, web.SkillPlatformRow{Platform: r.Platform, Origin: r.Origin, Trust: r.Trust, Invocations: r.Invocations, Skills: r.Skills, People: r.People})
	}
	for _, r := range sum.ByPerson {
		out.ByPerson = append(out.ByPerson, web.SkillPersonRow{Email: r.Email, Invocations: r.Invocations, Skills: r.Skills, TopSkill: r.TopSkill, Platforms: r.Platforms})
	}
	for _, r := range sum.ByRepo {
		out.ByRepo = append(out.ByRepo, web.SkillRepoRow{Repo: r.Repo, Lineage: r.Lineage, Invocations: r.Invocations})
	}
	return out, nil
}

func (d webData) SkillUnused(ctx context.Context, v web.Viewer, q web.SkillQuery) ([]web.SkillUnusedRow, error) {
	rows, err := d.s.SkillUnused(ctx, webStoreViewer(v), webSkillQuery(q))
	if err != nil {
		return nil, webReadError("unused skills", err)
	}
	out := make([]web.SkillUnusedRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, web.SkillUnusedRow{SourceRepo: r.SourceRepo, Plugin: r.Plugin, Skill: r.Skill, Lineage: r.Lineage, AuthoredBy: r.AuthoredBy,
			Mirrored: r.Mirrored, Installable: r.Installable, FirstSeenAt: r.FirstSeenAt, DaysInCatalog: r.DaysInCatalog, LastUsedAt: r.LastUsedAt, StaleMirror: r.StaleMirror})
	}
	return out, nil
}

func (d webData) SkillUnknown(ctx context.Context, v web.Viewer, q web.SkillQuery) ([]web.SkillUnknownRow, error) {
	rows, err := d.s.SkillUnknown(ctx, webStoreViewer(v), webSkillQuery(q))
	if err != nil {
		return nil, webReadError("unknown skills", err)
	}
	out := make([]web.SkillUnknownRow, 0, len(rows))
	for _, r := range rows {
		row := web.SkillUnknownRow{RawName: r.RawName, Platform: r.Platform, Origin: r.Origin, Count: r.Count, FirstSeen: r.FirstSeen, LastSeen: r.LastSeen}
		if r.Suggested != nil {
			row.Suggested = &web.SkillSuggestion{SourceRepo: r.Suggested.SourceRepo, Plugin: r.Suggested.Plugin, Skill: r.Suggested.Skill}
		}
		out = append(out, row)
	}
	return out, nil
}

func (d webData) SkillPruning(ctx context.Context, v web.Viewer, q web.SkillQuery) ([]web.SkillPruningRow, error) {
	rows, err := d.s.SkillPruning(ctx, webStoreViewer(v), webSkillQuery(q))
	if err != nil {
		return nil, webReadError("skill pruning", err)
	}
	out := make([]web.SkillPruningRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, web.SkillPruningRow{SourceRepo: r.SourceRepo, Plugin: r.Plugin, Skill: r.Skill, AuthoredBy: r.AuthoredBy, AuthorEvidence: r.AuthorEvidence,
			LastUsedAt: r.LastUsedAt, DaysUnused: r.DaysUnused, DaysInCatalog: r.DaysInCatalog, ProposedAction: r.ProposedAction, ExemptReason: r.ExemptReason, StaleMirror: r.StaleMirror})
	}
	return out, nil
}

func (d webData) SkillCompliance(ctx context.Context, v web.Viewer, q web.SkillQuery) ([]web.SkillComplianceRow, error) {
	rows, err := d.s.SkillCompliance(ctx, webStoreViewer(v), webSkillQuery(q))
	if err != nil {
		return nil, webReadError("skill compliance", err)
	}
	out := make([]web.SkillComplianceRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, web.SkillComplianceRow{Day: r.Day, Platform: r.Platform, Lineage: r.Lineage, BeaconRows: r.BeaconRows, ReconcilerRows: r.ReconcilerRows,
			ExactJoins: r.ExactJoins, CompliancePct: r.CompliancePct, LastRunAt: r.LastRunAt})
	}
	return out, nil
}

func (d webData) SkillRebuild(ctx context.Context) (web.SkillRebuild, error) {
	rb, err := d.s.SkillRebuildProgress(ctx)
	if err != nil {
		return web.SkillRebuild{}, fmt.Errorf("app: skill rebuild progress: %w", err)
	}
	return web.SkillRebuild{Rebuilding: rb.Rebuilding, Processed: rb.Processed, Total: rb.Total}, nil
}

func (d webData) SessionSkills(ctx context.Context, v web.Viewer, sessionID string) ([]web.SkillInvocation, error) {
	rows, err := d.s.SessionSkills(ctx, webStoreViewer(v), sessionID)
	if err != nil {
		return nil, webReadError("read session skills", err)
	}
	out := make([]web.SkillInvocation, 0, len(rows))
	for _, r := range rows {
		skill := ""
		if r.Skill != nil {
			skill = *r.Skill
		}
		out = append(out, web.SkillInvocation{OccurredAt: r.OccurredAt, Origin: r.Origin, AgentPlatform: r.AgentPlatform, Trust: r.Trust,
			RawName: r.RawName, Plugin: r.Plugin, Skill: skill, Trigger: r.Trigger, Outcome: r.Outcome, SessionRef: r.SessionRef, Lineage: r.Lineage})
	}
	return out, nil
}
