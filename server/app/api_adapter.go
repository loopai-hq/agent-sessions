// Package app is where the store meets the packages that serve it.
//
// Every other server package was built against a port it declared itself and
// tested against its own fakes, which is why none of them import one another:
// api asks for an api.Store, web asks for a web.Data, admin asks for an
// admin.Store, and the store has never heard of any of them. That leaves
// exactly one place where the concrete types have to be introduced, and this is
// it. app imports store, api, admin, web, ingest and auth; nothing imports app.
//
// The adapters here translate types and errors and do nothing else. They hold
// no state, cache nothing, and make no permission decisions. The store applies
// the authorization predicate inside the SELECT and writes the audit row in the
// same transaction as the read it records, so a rule re-derived here would be a
// second copy of it, free to drift, and of two copies of a permission rule the
// one that drifts is always the more permissive one.
package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/server/api"
	"github.com/loopai-hq/agent-sessions/server/ingest"
	"github.com/loopai-hq/agent-sessions/server/skillusage"
	"github.com/loopai-hq/agent-sessions/server/store"
)

// ---------------------------------------------------------------------------
// Reconciler runs
// ---------------------------------------------------------------------------

// The reconciler-runs route (server/skillusage/reconciler_runs.go) asserts
// its ledger port on the same store the invocation route was built over,
// so the one adapter serves both routes and a server that mounts the
// second always carries the ledger; the assertion below makes that a
// compile-time fact rather than a 503 discovered in production.
var _ skillusage.ReconcilerStore = skillUsageStore{}

// RecordReconcilerRun binds the route's ledger port to the store's run
// table. No transaction wrapper: the store's insert is one statement under
// its UNIQUE, and the read-back on a repeat is a second, so the token row
// is never held.
func (a skillUsageStore) RecordReconcilerRun(ctx context.Context, tokenID string, r store.ReconcilerRun) (bool, bool, error) {
	return a.s.RecordReconcilerRun(ctx, tokenID, r)
}

// LogSoftRevokedReconcilerRun binds the line the route writes when the
// token's limit is 0 and nothing is recorded, so the run's own line stays
// single-homed in the store beside the recorded one.
func (a skillUsageStore) LogSoftRevokedReconcilerRun(ctx context.Context, tokenID string, r store.ReconcilerRun) {
	a.s.LogSoftRevokedReconcilerRun(ctx, tokenID, r)
}

// apiStore adapts *store.Store to the port the dashboard read API declares.
//
// It carries the handle and nothing else. Any other field would be state the
// api package cannot see and cannot invalidate, and the first thing anybody
// would reach for it to hold is a resolved role, which is precisely the value
// that must be re-read on every request so that a demotion at lunchtime ends
// the demoted person's admin reads at lunchtime.
type apiStore struct{ s *store.Store }

// NewAPIStore returns the persistence behind the dashboard's read routes.
func NewAPIStore(s *store.Store) api.Store { return apiStore{s: s} }

// Principal resolves an address to its roster row.
//
// The error is wrapped plainly rather than routed through apiFailure, because
// absence is not an error on this method: the boolean carries it. Sending a
// roster read through the session translator would let a future store.ErrNotFound
// out of here as api.ErrNotFound, and the routes would answer a roster problem
// with a 404 about a session nobody asked for.
func (a apiStore) Principal(ctx context.Context, email string) (api.Principal, bool, error) {
	p, found, err := a.s.PrincipalLookup(ctx, email)
	if err != nil {
		return api.Principal{}, false, fmt.Errorf("app: look up principal %q: %w", email, err)
	}
	if !found {
		return api.Principal{}, false, nil
	}
	return apiPrincipal(p), true, nil
}

func (a apiStore) ListSessions(ctx context.Context, v api.Viewer, f api.SessionFilter) (api.SessionPage, error) {
	page, err := a.s.ListSessions(ctx, apiToStoreViewer(v), store.SessionFilter{
		Query:  f.Query,
		Email:  f.Email,
		Source: f.Source,
		Repo:   f.Repo,
		Types:  f.Types,
		From:   f.From,
		To:     f.To,
		Limit:  f.Limit,
		Cursor: f.Cursor,
	})
	if err != nil {
		return api.SessionPage{}, apiFailure("list sessions", err)
	}

	out := api.SessionPage{NextCursor: page.NextCursor}
	// A nil slice is carried through as nil. The route turns it into an empty
	// JSON array itself, and allocating one here would put a second answer to
	// "what does no sessions look like" in a second package.
	if len(page.Sessions) > 0 {
		out.Sessions = make([]api.Session, len(page.Sessions))
		for i := range page.Sessions {
			out.Sessions[i] = apiSession(page.Sessions[i])
		}
	}
	return out, nil
}

func (a apiStore) GetSession(ctx context.Context, v api.Viewer, sessionID string) (api.Session, error) {
	sess, err := a.s.GetSession(ctx, apiToStoreViewer(v), sessionID)
	if err != nil {
		return api.Session{}, apiFailure("get session", err)
	}
	return apiSession(sess), nil
}

func (a apiStore) GetEvents(ctx context.Context, v api.Viewer, sessionID string, r api.EventRange) (api.EventPage, error) {
	page, err := a.s.GetEvents(ctx, apiToStoreViewer(v), sessionID, store.EventRange{
		// The pointer is handed over rather than dereferenced into a value. Both
		// packages document nil as "start at the beginning" and a zero bound as
		// "everything after event zero", and the backfill walker numbers a
		// session's events from zero, so flattening the two here would hide the
		// first event of every imported session with nothing about the result
		// looking wrong.
		AfterSeq:          r.AfterSeq,
		IncludeSuperseded: r.IncludeSuperseded,
		Limit:             r.Limit,
	})
	if err != nil {
		return api.EventPage{}, apiFailure("get events", err)
	}

	out := api.EventPage{HasMore: page.HasMore, NextAfter: page.NextAfter}
	if len(page.Events) > 0 {
		out.Events = make([]api.StoredEvent, len(page.Events))
		for i := range page.Events {
			out.Events[i] = apiEvent(page.Events[i])
		}
	}
	return out, nil
}

func (a apiStore) SearchMessages(ctx context.Context, v api.Viewer, f api.SearchFilter) (api.SearchResult, error) {
	// CandidateCap is left at zero, which the store reads as its own
	// SearchCandidateCap. The api port does not expose the knob, and inventing a
	// value here would put the ranking budget in a package that cannot see what
	// it costs.
	res, err := a.s.SearchMessages(ctx, apiToStoreViewer(v), store.SearchFilter{
		Query:  f.Query,
		Email:  f.Email,
		Source: f.Source,
		Types:  f.Types,
		From:   f.From,
		To:     f.To,
		Limit:  f.Limit,
		Offset: f.Offset,
	})
	if err != nil {
		return api.SearchResult{}, apiFailure("search messages", err)
	}

	out := api.SearchResult{Candidates: res.Candidates, Capped: res.Capped}
	if len(res.Hits) > 0 {
		out.Hits = make([]api.Hit, len(res.Hits))
		for i := range res.Hits {
			out.Hits[i] = apiHit(res.Hits[i])
		}
	}
	return out, nil
}

func (a apiStore) ListShares(ctx context.Context, v api.Viewer, sessionID string) ([]api.Share, error) {
	shares, err := a.s.ListShares(ctx, apiToStoreViewer(v), sessionID)
	if err != nil {
		return nil, apiFailure("list shares", err)
	}
	if len(shares) == 0 {
		return nil, nil
	}
	out := make([]api.Share, len(shares))
	for i := range shares {
		out[i] = apiShare(shares[i])
	}
	return out, nil
}

func (a apiStore) CreateShare(ctx context.Context, v api.Viewer, req api.ShareRequest) (api.Share, error) {
	sh, err := a.s.CreateShare(ctx, apiToStoreViewer(v), store.ShareRequest{
		SessionID: req.SessionID,
		Grantee:   req.Grantee,
		ExpiresAt: req.ExpiresAt,
	})
	if err != nil {
		return api.Share{}, apiFailure("create share", err)
	}
	return apiShare(sh), nil
}

func (a apiStore) RevokeShare(ctx context.Context, v api.Viewer, shareID string) error {
	if err := a.s.RevokeShare(ctx, apiToStoreViewer(v), shareID); err != nil {
		return apiFailure("revoke share", err)
	}
	return nil
}

func (a apiStore) ResolveShare(ctx context.Context, v api.Viewer, token string) (api.Session, api.Share, error) {
	sess, sh, err := a.s.ResolveShare(ctx, apiToStoreViewer(v), token)
	if err != nil {
		return api.Session{}, api.Share{}, apiFailure("resolve share", err)
	}
	return apiSession(sess), apiShare(sh), nil
}

// ---------------------------------------------------------------------------
// Error translation
// ---------------------------------------------------------------------------

// apiFailure turns a storage error into one the routes know how to answer.
//
// The collapse is the whole point of this function. store.ErrNotFound already
// covers both "no such session" and "you are not allowed to know whether there
// is such a session", because the permission predicate lives inside the SELECT
// and an unauthorized read comes back as no rows. Nothing here may reintroduce
// the distinction, so the sentinel is returned bare rather than wrapped: a
// wrapped chain would carry whatever the store said about the read, and the day
// the store's two messages differ the difference reaches the caller. A 403 on a
// colleague's session, or a 404 whose body differs from another 404, confirms
// that a named person ran something at a particular time, which is exactly the
// fact the viewer was not allowed to learn.
//
// Nothing is widened in the other direction either. An unrecognised error keeps
// its context and stays unrecognised, so that the routes answer 500. Mapping it
// to not-found would render a database outage as an empty result, and a member
// whose colleagues' sessions all disappeared for an hour would have no way to
// tell that from a quiet week. store.ErrNotAdmin lands here too: no method
// reachable through this port returns it today, and if one ever does, a loud
// failure is the right answer rather than a 404 that reads as absence.
func apiFailure(op string, err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return api.ErrNotFound
	case errors.Is(err, store.ErrInvalidCursor):
		return api.ErrInvalidCursor
	default:
		return fmt.Errorf("app: %s: %w", op, err)
	}
}

// ---------------------------------------------------------------------------
// Type translation
// ---------------------------------------------------------------------------

// apiToStoreViewer carries the caller's identity across the boundary.
//
// The role is converted rather than mapped through a switch. Both packages spell
// the two roles with the same strings, and a switch with a default would have to
// choose what an unrecognised role becomes: member grants a stranger a
// colleague's shared sessions, admin grants them everything. The api package
// already refuses a roster row it cannot interpret before a viewer is built, so
// the honest translation here is the literal one.
func apiToStoreViewer(v api.Viewer) store.Viewer {
	return store.Viewer{Email: v.Email, Role: store.Role(v.Role)}
}

// apiPrincipal narrows a roster row to the parts the read routes act on.
//
// Disabled is a boolean here and a timestamp in the store because the two
// packages ask different questions of it. This one asks only whether the person
// may read anything at all; when they were switched off is the admin page's
// business and stays where the admin page reads it.
func apiPrincipal(p store.Principal) api.Principal {
	return api.Principal{
		Email:       p.Email,
		Role:        api.Role(p.Role),
		DisplayName: p.DisplayName,
		Disabled:    p.DisabledAt != nil,
	}
}

func apiSession(s store.Session) api.Session {
	return api.Session{
		SessionID:        s.SessionID,
		Email:            s.Email,
		DeviceID:         s.DeviceID,
		Source:           s.Source,
		ParentSessionID:  s.ParentSessionID,
		Cwd:              s.Cwd,
		Repo:             s.Repo,
		GitBranch:        s.GitBranch,
		StartedAt:        s.StartedAt,
		EndedAt:          s.EndedAt,
		Ended:            s.Ended,
		UserTurns:        s.UserTurns,
		ToolCalls:        s.ToolCalls,
		Subagents:        s.Subagents,
		Errors:           s.Errors,
		FirstPrompt:      s.FirstPrompt,
		HarnessVersions:  s.HarnessVersions,
		TokensInput:      s.TokensInput,
		TokensOutput:     s.TokensOutput,
		TokensCacheRead:  s.TokensCacheRead,
		TokensCacheWrite: s.TokensCacheWrite,
		CostUSD:          s.CostUSD,
		Redactions:       s.Redactions,
		IngestedAt:       s.IngestedAt,
		UpdatedAt:        s.UpdatedAt,
	}
}

// apiEvent carries one stored event across.
//
// Body is passed as it stands rather than re-marshalled. It is the scrubbed
// event exactly as it was delivered, and a round trip through this package's
// idea of the shape would show the reader our re-rendering of what arrived
// instead of what arrived.
func apiEvent(e store.StoredEvent) api.StoredEvent {
	return api.StoredEvent{
		ID:         e.ID,
		SessionID:  e.SessionID,
		Email:      e.Email,
		Seq:        e.Seq,
		Type:       e.Type,
		Origin:     e.Origin,
		OccurredAt: e.OccurredAt,
		IngestedAt: e.IngestedAt,
		AgentID:    e.AgentID,
		WorkflowID: e.WorkflowID,
		Model:      e.Model,
		ToolName:   e.ToolName,
		Body:       e.Body,
	}
}

func apiHit(h store.Hit) api.Hit {
	return api.Hit{
		EventID:    h.EventID,
		SessionID:  h.SessionID,
		Email:      h.Email,
		Seq:        h.Seq,
		Role:       h.Role,
		OccurredAt: h.OccurredAt,
		Snippet:    h.Snippet,
		Rank:       h.Rank,
	}
}

func apiShare(sh store.Share) api.Share {
	return api.Share{
		ID:        sh.ID,
		SessionID: sh.SessionID,
		CreatedBy: sh.CreatedBy,
		Grantee:   sh.Grantee,
		Token:     sh.Token,
		CreatedAt: sh.CreatedAt,
		ExpiresAt: sh.ExpiresAt,
		RevokedAt: sh.RevokedAt,
	}
}

// ---------------------------------------------------------------------------
// Repair
// ---------------------------------------------------------------------------

// apiDevices adapts the device credential check the upload path uses to the
// read API's device port, so GET /v1/repair accepts exactly the bearer the
// laptop uploads with and nothing else.
type apiDevices struct{ d storeDevices }

// NewAPIDevices builds the repair route's device verifier.
func NewAPIDevices(s *store.Store) api.DeviceAuthenticator {
	return apiDevices{d: storeDevices{s: s}}
}

// VerifyDevice classifies the failure the way the upload path does: a
// credential that is not ours or matches no row is the caller's problem and
// becomes ErrNoIdentity; anything else is ours and stays an error, so a
// database blip is a 500 the daemon retries tomorrow and never a 401 that
// sends a working laptop to re-enrol.
func (a apiDevices) VerifyDevice(ctx context.Context, token string) (api.DeviceIdentity, error) {
	id, err := a.d.Verify(ctx, token)
	if err != nil {
		if errors.Is(err, ingest.ErrUnauthenticated) {
			return api.DeviceIdentity{}, api.ErrNoIdentity
		}
		return api.DeviceIdentity{}, err
	}
	return api.DeviceIdentity{DeviceID: id.DeviceID, Email: id.Email}, nil
}

// apiRepair adapts the store's repair list to the read API's port and builds
// the transcript path the client needs from what the server knows.
type apiRepair struct{ s *store.Store }

// NewAPIRepair builds the repair route's store.
func NewAPIRepair(s *store.Store) api.RepairStore { return apiRepair{s: s} }

// RepairList names the sessions the device should re-walk. A Claude Code
// transcript's path follows from the session's cwd by the harness's own
// rule (store.ClaudeTranscriptPath); a cwd the rule cannot place leaves the
// path empty and says so in the hint, because the client skips an entry with
// no path rather than guess. A Codex rollout's name says nothing about which
// session it holds, so those entries carry no path either; the client finds
// the file under its own Codex roots by reading each rollout's session_meta
// line, which is how every other part of the Codex walker decides too.
func (a apiRepair) RepairList(ctx context.Context, deviceID string, now time.Time, limit int) ([]api.RepairEntry, error) {
	rows, err := a.s.RepairList(ctx, deviceID, now, limit)
	if err != nil {
		return nil, fmt.Errorf("app: repair list for device %s: %w", deviceID, err)
	}
	out := make([]api.RepairEntry, 0, len(rows))
	for _, r := range rows {
		e := api.RepairEntry{SessionID: r.SessionID, Reason: r.Reason, Source: r.Source}
		switch {
		case r.Source == string(event.SourceCodex):
			e.Hint = "re-walk credits the token counts the walker did not read; the rollout path is not known to the server, and the client finds the file by the session id in its session_meta line"
		case r.Reason == "missing_answers":
			e.TranscriptPath = store.ClaudeTranscriptPath(r.Cwd, r.SessionID)
			e.Hint = fmt.Sprintf("re-walk recovers %d answerless turn(s) from the transcript while it is still on the laptop", r.Turns)
		default:
			e.TranscriptPath = store.ClaudeTranscriptPath(r.Cwd, r.SessionID)
			e.Hint = "re-walk completes the session from the transcript"
		}
		if e.TranscriptPath == "" && r.Source != string(event.SourceCodex) {
			e.Hint = fmt.Sprintf("the transcript path could not be built from cwd %q and session id %q (a clean absolute path under /Users or /home and a harness uuid are needed); run loop-sessions backfill --session %s on the laptop", r.Cwd, r.SessionID, r.SessionID)
		}
		out = append(out, e)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Skill reads and the catalog
// ---------------------------------------------------------------------------

// The skill half of the read API's port (server/api/skills.go,
// skills_catalog.go): field-for-field translation of the store's rows, and
// the one error mapping the routes rely on. A member's compliance read is
// the store's ErrNotAdmin, which arrives here as api.ErrNotFound so the
// route answers the package 404 rather than a 500 that says the read was
// refused; the route checks the role first and this is the second line.

func apiSkillFilter(f api.SkillFilter) store.SkillFilter {
	return store.SkillFilter{
		Range: f.Range, Platform: f.Platform, Repo: f.Repo, Types: f.Types, Trigger: f.Trigger, Trust: f.Trust,
		IncludeClaimed: f.IncludeClaimed, TZ: f.TZ, Skill: f.Skill, Email: f.Email, SessionRef: f.SessionRef,
		Cursor: f.Cursor, Limit: f.Limit, Since: f.Since, SourceRepo: f.SourceRepo,
	}
}

// apiSkillFailure is apiFailure plus the admin refusal: on these reads it
// is the store saying "not for you", which the port collapses into 404.
func apiSkillFailure(op string, err error) error {
	if errors.Is(err, store.ErrNotAdmin) {
		return api.ErrNotFound
	}
	return apiFailure(op, err)
}

func (a apiStore) SkillSummary(ctx context.Context, v api.Viewer, f api.SkillFilter) (api.SkillSummary, error) {
	sum, err := a.s.SkillSummary(ctx, apiToStoreViewer(v), apiSkillFilter(f))
	if err != nil {
		return api.SkillSummary{}, apiSkillFailure("skill summary", err)
	}
	out := api.SkillSummary{From: sum.From, To: sum.To, Totals: api.SkillTotals{
		Invocations: sum.Totals.Invocations, Skills: sum.Totals.Skills, People: sum.Totals.People, Sessions: sum.Totals.Sessions,
		User: sum.Totals.User, Agent: sum.Totals.Agent, Success: sum.Totals.Success, Error: sum.Totals.Error, Started: sum.Totals.Started, Claimed: sum.Totals.Claimed,
	}}
	for _, b := range sum.ByBucket {
		out.ByBucket = append(out.ByBucket, api.SkillBucketRow{Bucket: b.Bucket, Platform: b.Platform, Trigger: b.Trigger, Trust: b.Trust, Invocations: b.Invocations})
	}
	for _, r := range sum.BySkill {
		// Copies stay behind: the 6.2 row has no such key; the page reads
		// them through its own port.
		out.BySkill = append(out.BySkill, api.SkillSkillRow{Lineage: r.Lineage, SourceRepo: r.SourceRepo, Plugin: r.Plugin, Skill: r.Skill,
			Invocations: r.Invocations, People: r.People, Sessions: r.Sessions, User: r.User, Agent: r.Agent, Error: r.Err,
			Platforms: nonNil(r.Platforms), Repos: nonNil(r.Repos), LastUsedAt: r.LastUsedAt})
	}
	for _, r := range sum.ByPlatform {
		out.ByPlatform = append(out.ByPlatform, api.SkillPlatformRow{Platform: r.Platform, Origin: r.Origin, Trust: r.Trust, Invocations: r.Invocations, Skills: r.Skills, People: r.People})
	}
	for _, r := range sum.ByPerson {
		out.ByPerson = append(out.ByPerson, api.SkillPersonRow{Email: r.Email, Invocations: r.Invocations, Skills: r.Skills, TopSkill: r.TopSkill, Platforms: nonNil(r.Platforms)})
	}
	for _, r := range sum.ByRepo {
		out.ByRepo = append(out.ByRepo, api.SkillRepoRow{Repo: r.Repo, Lineage: r.Lineage, Invocations: r.Invocations})
	}
	return out, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (a apiStore) SkillInvocations(ctx context.Context, v api.Viewer, f api.SkillFilter) (api.SkillInvocationPage, error) {
	page, err := a.s.SkillInvocations(ctx, apiToStoreViewer(v), apiSkillFilter(f))
	if err != nil {
		return api.SkillInvocationPage{}, apiSkillFailure("list skill invocations", err)
	}
	out := api.SkillInvocationPage{NextCursor: page.NextCursor}
	for _, r := range page.Invocations {
		out.Invocations = append(out.Invocations, apiSkillInvocation(r))
	}
	return out, nil
}

func apiSkillInvocation(r store.SkillInvocationRow) api.SkillInvocation {
	return api.SkillInvocation{
		ID: r.ID, OccurredAt: r.OccurredAt, ReceivedAt: r.ReceivedAt, TimeClamped: r.TimeClamped,
		Origin: r.Origin, AgentPlatform: r.AgentPlatform, Trust: r.Trust, RawName: r.RawName, Plugin: r.Plugin, Skill: r.Skill,
		SkillSource: r.SkillSource, Trigger: r.Trigger, Outcome: r.Outcome, ErrorClass: r.ErrorClass, ActorEmail: r.ActorEmail, ActorKnown: r.ActorKnown,
		Repo: r.Repo, SessionRef: r.SessionRef, LinkRef: r.LinkRef, SessionType: r.SessionType,
		EventID: r.EventID, PromptID: r.PromptID, ToolUseID: r.ToolUseID, IdempotencyKey: r.IdempotencyKey,
		ArgsPresent: r.ArgsPresent, ArgsBytes: r.ArgsBytes, HarnessVersion: r.HarnessVersion, Lineage: r.Lineage,
	}
}

func (a apiStore) SkillUnused(ctx context.Context, v api.Viewer, f api.SkillFilter) ([]api.SkillUnusedRow, error) {
	rows, err := a.s.SkillUnused(ctx, apiToStoreViewer(v), apiSkillFilter(f))
	if err != nil {
		return nil, apiSkillFailure("unused skills", err)
	}
	out := make([]api.SkillUnusedRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, api.SkillUnusedRow{SourceRepo: r.SourceRepo, Plugin: r.Plugin, Skill: r.Skill, Lineage: r.Lineage, AuthoredBy: r.AuthoredBy,
			Mirrored: r.Mirrored, Installable: r.Installable, FirstSeenAt: r.FirstSeenAt, DaysInCatalog: r.DaysInCatalog, LastUsedAt: r.LastUsedAt, StaleMirror: r.StaleMirror})
	}
	return out, nil
}

func (a apiStore) SkillUnknown(ctx context.Context, v api.Viewer, f api.SkillFilter) ([]api.SkillUnknownRow, error) {
	rows, err := a.s.SkillUnknown(ctx, apiToStoreViewer(v), apiSkillFilter(f))
	if err != nil {
		return nil, apiSkillFailure("unknown skills", err)
	}
	out := make([]api.SkillUnknownRow, 0, len(rows))
	for _, r := range rows {
		row := api.SkillUnknownRow{RawName: r.RawName, Platform: r.Platform, Origin: r.Origin, Count: r.Count, FirstSeen: r.FirstSeen, LastSeen: r.LastSeen}
		if r.Suggested != nil {
			row.Suggested = &api.SkillSuggestion{SourceRepo: r.Suggested.SourceRepo, Plugin: r.Suggested.Plugin, Skill: r.Suggested.Skill}
		}
		out = append(out, row)
	}
	return out, nil
}

func (a apiStore) SkillPruning(ctx context.Context, v api.Viewer, f api.SkillFilter) ([]api.SkillPruningRow, error) {
	rows, err := a.s.SkillPruning(ctx, apiToStoreViewer(v), apiSkillFilter(f))
	if err != nil {
		return nil, apiSkillFailure("skill pruning", err)
	}
	out := make([]api.SkillPruningRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, api.SkillPruningRow{SourceRepo: r.SourceRepo, Plugin: r.Plugin, Skill: r.Skill, AuthoredBy: r.AuthoredBy, AuthorEvidence: r.AuthorEvidence,
			LastUsedAt: r.LastUsedAt, DaysUnused: r.DaysUnused, DaysInCatalog: r.DaysInCatalog, ProposedAction: r.ProposedAction, ExemptReason: r.ExemptReason, StaleMirror: r.StaleMirror})
	}
	return out, nil
}

func (a apiStore) SkillCompliance(ctx context.Context, v api.Viewer, f api.SkillFilter) ([]api.SkillComplianceRow, error) {
	rows, err := a.s.SkillCompliance(ctx, apiToStoreViewer(v), apiSkillFilter(f))
	if err != nil {
		return nil, apiSkillFailure("skill compliance", err)
	}
	out := make([]api.SkillComplianceRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, api.SkillComplianceRow{Day: r.Day, Platform: r.Platform, Lineage: r.Lineage, BeaconRows: r.BeaconRows, ReconcilerRows: r.ReconcilerRows,
			ExactJoins: r.ExactJoins, CompliancePct: r.CompliancePct, LastRunAt: r.LastRunAt})
	}
	return out, nil
}

// PublishSkillCatalog hands the body to the store's one transaction and
// maps its four caller errors onto the port's, so the route can name the
// field, the lineage, the alias or the shrink it refused without knowing
// the store.
func (a apiStore) PublishSkillCatalog(ctx context.Context, tokenID, sourceRepo string, body api.CatalogBody) (api.PublishResult, error) {
	sb := store.CatalogBody{Schema: body.Schema, GeneratedAt: body.GeneratedAt, SourceRepo: body.SourceRepo, Commit: body.Commit,
		AllowShrink: body.AllowShrink}
	for _, sk := range body.Skills {
		sb.Skills = append(sb.Skills, store.CatalogSkill{Slug: sk.Slug, Dir: sk.Dir, Name: sk.Name, Plugin: sk.Plugin, Path: sk.Path, Sha256Tree: sk.Sha256Tree,
			Installable: sk.Installable, Mirrored: sk.Mirrored, SkipReason: sk.SkipReason, AuthoredBy: sk.AuthoredBy, AuthorEvidence: sk.AuthorEvidence,
			LineageOf: sk.LineageOf, Aliases: sk.Aliases})
	}
	res, err := a.s.PublishSkillCatalog(ctx, tokenID, sourceRepo, sb)
	if err != nil {
		var fe *store.CatalogFieldError
		var le *store.LineageError
		var ce *store.AliasConflictError
		var se *store.CatalogShrinkError
		switch {
		case errors.As(err, &fe):
			return api.PublishResult{}, &api.CatalogFieldError{Field: fe.Field, Reason: fe.Reason}
		case errors.As(err, &le):
			return api.PublishResult{}, &api.LineageError{Lineage: le.Lineage}
		case errors.As(err, &ce):
			return api.PublishResult{}, &api.AliasConflictError{Alias: ce.Alias, HeldBy: ce.HeldBy}
		case errors.As(err, &se):
			return api.PublishResult{}, &api.CatalogShrinkError{Present: se.Present, Incoming: se.Incoming}
		}
		return api.PublishResult{}, fmt.Errorf("app: publish skill catalog %s: %w", sourceRepo, err)
	}
	return api.PublishResult{Duplicate: res.Duplicate, Skills: res.Skills, Aliases: res.Aliases, StaleEntries: res.StaleEntries,
		PresentBefore: res.PresentBefore}, nil
}

// AuthenticateSource is the catalog route's verifier, which the read API
// picks up from its store when no separate one is given: the verify's
// touch in a bounded transaction of its own, committed before the answer,
// exactly as the invocation route's adapter does it. The store's token
// becomes the port's identity; the hash and the scope pass through.
func (a apiStore) AuthenticateSource(ctx context.Context, hash []byte, scope string) (api.SourceIdentity, error) {
	var tok store.SourceToken
	err := a.s.InSourceTx(ctx, func(ctx context.Context, q store.Queryer) error {
		var err error
		tok, err = a.s.AuthenticateSourceToken(ctx, q, hash, scope)
		return err
	})
	if err != nil {
		return api.SourceIdentity{}, fmt.Errorf("app: verify source token: %w", err)
	}
	return api.SourceIdentity{ID: tok.ID, Platform: tok.Platform, Environment: tok.Environment, Scope: tok.Scope, State: tok.State,
		OverCap: tok.State == store.TokenStateLive && tok.OverCap(), SoftRevoked: tok.State == store.TokenStateLive && tok.SoftRevoked()}, nil
}

// The read API mounts the catalog route when its store verifies source
// tokens; pinned so the mount cannot be lost to a refactor of either side.
var _ api.SourceAuthenticator = apiStore{}
