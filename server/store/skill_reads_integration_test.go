//go:build integration

package store

// The reads' half only Postgres can answer: the shared CTEs run, the
// alias join resolves derived rows to their lineage, the member scope and
// the folding hold on real rows (the design 6.1 probe), an lsd_ API row
// leaves a skill in the pruning report (SECURITY3-4), the session strip is
// derived rows only, the unknown report keeps names for admins alone, and
// the compliance report reads the runs.

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// readOwner is the person the skill fixtures (skillCall, skillResult)
// ingest under, so the owner viewer is theirs.
const (
	readOwner    = skillTestEmail
	readOther    = "other@example.com"
	readDeviceID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

// seedCatalog publishes engg:git and engg:temporal under the marketplace.
func seedCatalog(t *testing.T, s *Store) {
	t.Helper()
	admin := intAdminViewer(t, s)
	_, tok := intCatalogToken(t, s, admin, "example-skills")
	if _, err := s.PublishSkillCatalog(context.Background(), tok.ID, "example-skills", catalogBody("example-skills", "seed",
		catalogSkill("engg", "git", "engg:git", "git"), catalogSkill("engg", "temporal"))); err != nil {
		t.Fatalf("seed catalog: %v", err)
	}
}

// claimedRow inserts one API row under a token.
func claimedRow(t *testing.T, s *Store, tok SourceToken, origin, platform, skill, key string, at time.Time, linkRef *string) {
	t.Helper()
	raw, plugin, bare, _ := NormalizeSkillName(skill)
	row := SkillRow{Origin: origin, AgentPlatform: platform, Trust: TrustClaimed, SourceTokenID: &tok.ID, RawName: raw, Plugin: plugin, Skill: bare,
		SkillSource: SkillSourceUnknown, Trigger: TriggerUnknown, Outcome: OutcomeUnknown, SessionRef: "vendor-" + key, IdempotencyKey: strPtr(key),
		LinkRef: linkRef, OccurredAt: at, TokenPlatform: platform, TokenEnvironment: tok.Environment}
	if _, err := s.UpsertSkillInvocation(context.Background(), poolDB{pool: pool}, row); err != nil {
		t.Fatalf("claimed row %s: %v", key, err)
	}
}

// TestIntegrationSkillSummaryScopesAndFoldsOnRealRows is the design 6.1
// probe and the D5 rule on Postgres: the owner's derived rows resolve to
// their lineage, a member sees the fleet aggregates and only their own
// person row, an admin sees everyone, and a member on
// platform=vorflux&range=1d gets a folded platform row and no bucket rows.
func TestIntegrationSkillSummaryScopesAndFoldsOnRealRows(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	seedCatalog(t, s)
	skillSeedPerson(t, s, readOwner, readDeviceID)
	mustPrincipal(t, s, readOther, RoleMember)
	admin := intAdminViewer(t, s)
	// Half past the previous UTC hour, not two hours ago: the range=1d
	// probe below asserts ONE hour bucket over two rows a minute apart, and
	// a clock-relative fixture put them either side of an hour boundary
	// whenever the suite ran in the first minutes of an hour. Same class as
	// the compliance fixture's day boundary (LS-3 review-3 finding 1): a
	// bucket is a local-time computation, so anything the fixture leaves to
	// the wall clock is a test that is red for a few minutes an hour.
	at := time.Now().UTC().Truncate(time.Hour).Add(-30 * time.Minute)
	skillIngest(t, s, asSession([]Ingest{
		skillCall("rd-call", "sess-read", "p1", "toolu_rd1", `{"skill":"engg:git"}`, 1),
		skillResult("rd-res", "sess-read", "toolu_rd1", event.ToolResult, 2),
		skillCall("rd-call2", "sess-read", "p2", "toolu_rd2", `{"skill":"probe-skill"}`, 3),
	}, readDeviceID, at))
	// Vorflux reconciler rows, person-less, claimed.
	_, vorflux := intMint(t, s, admin, MintRequest{Platform: PlatformVorflux, Environment: "reconciler", ExpiresInDays: 30, AllowedOrigins: []string{OriginReconciler}})
	claimedRow(t, s, vorflux, OriginReconciler, PlatformVorflux, "git", "vt-1", at, nil)
	claimedRow(t, s, vorflux, OriginReconciler, PlatformVorflux, "temporal", "vt-2", at.Add(time.Minute), nil)

	owner := Viewer{Email: readOwner, Role: RoleMember}
	other := Viewer{Email: readOther, Role: RoleMember}

	sum, err := s.SkillSummary(ctx, owner, SkillFilter{})
	if err != nil {
		t.Fatalf("owner summary: %v", err)
	}
	if sum.Totals.Invocations != 2 || sum.Totals.Claimed != 2 || sum.Totals.Agent != 2 || sum.Totals.Success != 1 || sum.Totals.Started != 1 || sum.Totals.People != 1 || sum.Totals.Sessions != 1 {
		t.Errorf("owner totals = %+v, want the two derived rows proven and the two vorflux rows claimed", sum.Totals)
	}
	// One person: every lineage cell folds for a member.
	if len(sum.BySkill) != 1 || sum.BySkill[0].Lineage != otherCell || sum.BySkill[0].Invocations != 1 {
		t.Errorf("a member's skills = %+v, want git folded into other (probe-skill is unresolved)", sum.BySkill)
	}
	// git and probe-skill tie at one row each, so either is the top skill.
	if len(sum.ByPerson) != 1 || sum.ByPerson[0].Email != readOwner || sum.ByPerson[0].Invocations != 2 || sum.ByPerson[0].Skills != 2 || sum.ByPerson[0].TopSkill == "" {
		t.Errorf("owner's person row = %+v", sum.ByPerson)
	}
	if len(sum.ByBucket) != 0 {
		t.Errorf("a member's buckets = %+v, want none for one-person cells", sum.ByBucket)
	}
	// One row per (repo, lineage): the git lineage and the unresolved probe.
	if len(sum.ByRepo) != 2 || sum.ByRepo[0].Repo != "loop-sessions" || sum.ByRepo[1].Repo != "loop-sessions" {
		t.Errorf("owner's repos = %+v", sum.ByRepo)
	}

	// Another member: the same fleet aggregates, no person row, no repos.
	sum, err = s.SkillSummary(ctx, other, SkillFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Totals.Invocations != 2 || len(sum.ByPerson) != 0 || len(sum.ByRepo) != 0 {
		t.Errorf("another member sees totals %+v, people %+v, repos %+v", sum.Totals, sum.ByPerson, sum.ByRepo)
	}

	// The admin: nothing folded, the lineage resolved with its copy, the
	// person-less rows as one blank person when claimed rows count.
	sum, err = s.SkillSummary(ctx, admin, SkillFilter{IncludeClaimed: true})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Totals.Invocations != 4 || sum.Totals.Claimed != 2 {
		t.Errorf("admin totals = %+v", sum.Totals)
	}
	var git *SkillSkillRow
	for i := range sum.BySkill {
		if sum.BySkill[i].Lineage == "example-skills/engg:git" {
			git = &sum.BySkill[i]
		}
	}
	if git == nil || git.Invocations != 2 || git.SourceRepo != "example-skills" || git.Skill != "git" || len(git.Copies) != 1 || git.Copies[0].Invocations != 2 || len(git.Platforms) != 2 {
		t.Errorf("admin git lineage = %+v", git)
	}
	if len(sum.ByBucket) == 0 || len(sum.ByPerson) != 2 {
		t.Errorf("admin buckets %d, people %+v", len(sum.ByBucket), sum.ByPerson)
	}

	// The probe: a member on platform=vorflux&range=1d gets one folded
	// platform row and no bucket rows, whether or not claimed rows count.
	for _, ic := range []bool{false, true} {
		sum, err = s.SkillSummary(ctx, other, SkillFilter{Platform: PlatformVorflux, Range: "1d", IncludeClaimed: ic})
		if err != nil {
			t.Fatal(err)
		}
		if len(sum.ByPlatform) != 1 || sum.ByPlatform[0].Platform != otherCell || sum.ByPlatform[0].Invocations != 2 || len(sum.ByBucket) != 0 {
			t.Errorf("the vorflux probe (include_claimed=%v) = platforms %+v, buckets %+v", ic, sum.ByPlatform, sum.ByBucket)
		}
	}
	sum, err = s.SkillSummary(ctx, admin, SkillFilter{Platform: PlatformVorflux, Range: "1d"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sum.ByPlatform) != 1 || sum.ByPlatform[0].Platform != PlatformVorflux || sum.ByPlatform[0].Trust != TrustClaimed || len(sum.ByBucket) != 1 {
		t.Errorf("the admin's vorflux view = %+v %+v", sum.ByPlatform, sum.ByBucket)
	}

	// The listing: the owner's rows with their lineage, the other member's
	// nothing, the admin's everything, and the session filter derived only.
	page, err := s.SkillInvocations(ctx, owner, SkillFilter{SessionRef: "sess-read"})
	if err != nil || len(page.Invocations) != 2 || page.Invocations[0].Lineage != "" && page.Invocations[1].Lineage != "" && page.Invocations[0].Lineage == page.Invocations[1].Lineage {
		t.Errorf("owner listing = %+v, %v", page, err)
	}
	if page, _ := s.SkillInvocations(ctx, other, SkillFilter{}); len(page.Invocations) != 0 {
		t.Errorf("another member lists %d rows", len(page.Invocations))
	}
	page, err = s.SkillInvocations(ctx, admin, SkillFilter{IncludeClaimed: true, Limit: 3})
	if err != nil || len(page.Invocations) != 3 || page.NextCursor == "" {
		t.Fatalf("admin listing = %d rows, cursor %q, %v", len(page.Invocations), page.NextCursor, err)
	}
	rest, err := s.SkillInvocations(ctx, admin, SkillFilter{IncludeClaimed: true, Limit: 3, Cursor: page.NextCursor})
	if err != nil || len(rest.Invocations) != 1 || rest.NextCursor != "" {
		t.Errorf("the second page = %+v, %v", rest, err)
	}

	// The strip: derived rows for the owner and the admin, not found for
	// the other member and for an unknown session.
	strip, err := s.SessionSkills(ctx, owner, "sess-read")
	if err != nil || len(strip) != 2 || strip[0].Origin != OriginDerived {
		t.Errorf("owner strip = %+v, %v", strip, err)
	}
	if _, err := s.SessionSkills(ctx, other, "sess-read"); !errors.Is(err, ErrNotFound) {
		t.Errorf("another member's strip = %v, want not found", err)
	}
	if _, err := s.SessionSkills(ctx, admin, "no-such-session"); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown session's strip = %v, want not found", err)
	}
	if strip, _ := s.SessionSkills(ctx, admin, "sess-read"); len(strip) != 2 {
		t.Errorf("admin strip = %d rows", len(strip))
	}

	// Unknown: probe-skill for the admin by name, nameless for a member.
	unknown, err := s.SkillUnknown(ctx, admin, SkillFilter{})
	if err != nil || len(unknown) != 1 || unknown[0].RawName != "probe-skill" || unknown[0].Suggested != nil {
		t.Errorf("admin unknown = %+v, %v", unknown, err)
	}
	unknown, err = s.SkillUnknown(ctx, other, SkillFilter{})
	if err != nil || len(unknown) != 1 || unknown[0].RawName != "" || unknown[0].Count != 1 || unknown[0].Platform != PlatformClaudeCode {
		t.Errorf("member unknown = %+v, %v", unknown, err)
	}
}

// TestIntegrationPruningKeepsASkillAnAPIRowTouched is SECURITY3-4: an
// lsd_ API row (device trust, hook origin, caller-chosen name) for a skill
// unused for ninety days leaves it in the pruning and zero-use reports; a
// derived row clears it; with include_claimed the API row counts.
func TestIntegrationPruningKeepsASkillAnAPIRowTouched(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	seedCatalog(t, s)
	skillSeedPerson(t, s, readOwner, readDeviceID)
	admin := intAdminViewer(t, s)
	if _, err := pool.Exec(ctx, `UPDATE skill_catalog_entries SET first_seen_at = now() - interval '100 days'`); err != nil {
		t.Fatal(err)
	}
	inPruning := func(skill string) (SkillPruningRow, bool) {
		rows, err := s.SkillPruning(ctx, admin, SkillFilter{})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if r.Skill == skill {
				return r, true
			}
		}
		return SkillPruningRow{}, false
	}
	if r, ok := inPruning("git"); !ok || r.DaysInCatalog != 100 || r.ExemptReason != "" || r.ProposedAction != PruneFlagOnly {
		t.Fatalf("an untouched entry is not a candidate: %+v %v", r, ok)
	}
	// The lsd_ API row: trust device, origin hook, no source token.
	device := readDeviceID
	row := SkillRow{Origin: OriginHook, AgentPlatform: PlatformClaudeCode, Trust: TrustDevice, DeviceID: &device, RawName: "engg:git", Plugin: "engg", Skill: strPtr("git"),
		SkillSource: SkillSourcePlugin, Trigger: TriggerAgent, Outcome: OutcomeStarted, ActorEmail: strPtr(readOwner), ActorKnown: true,
		SessionRef: "sess-api", ToolUseID: strPtr("toolu_api"), OccurredAt: time.Now().UTC().Add(-time.Hour)}
	if _, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, row); err != nil {
		t.Fatal(err)
	}
	if _, ok := inPruning("git"); !ok {
		t.Error("an lsd_ API row immunised a skill from pruning")
	}
	unused, err := s.SkillUnused(ctx, admin, SkillFilter{})
	if err != nil || len(unused) != 2 {
		t.Errorf("unused = %+v, %v, want both entries", unused, err)
	}
	unused, err = s.SkillUnused(ctx, admin, SkillFilter{IncludeClaimed: true})
	if err != nil || len(unused) != 1 || unused[0].Skill != "temporal" {
		t.Errorf("unused with claimed rows = %+v, %v, want temporal alone", unused, err)
	}
	// A derived row is proven and clears the skill.
	skillIngest(t, s, asSession([]Ingest{skillCall("pr-call", "sess-pr", "p1", "toolu_pr", `{"skill":"engg:git"}`, 1)}, readDeviceID, time.Now().UTC().Add(-time.Hour)))
	if _, ok := inPruning("git"); ok {
		t.Error("a derived row did not clear the skill from pruning")
	}
	if r, ok := inPruning("temporal"); !ok || r.DaysUnused != 100 {
		t.Errorf("temporal = %+v %v", r, ok)
	}

	// The D5 fold on these two reports, on real rows. One proven row for
	// temporal, ninety-five days back and one person's, leaves the entry
	// unused in the ninety-day window while giving it a last use: an admin
	// reads the timestamp, a member does not, and a member's days_unused
	// falls back to the days in the catalog rather than naming the day
	// (adversarial finding 7).
	mustPrincipal(t, s, readOther, RoleMember)
	member := Viewer{Email: readOther, Role: RoleMember}
	longAgo := time.Now().UTC().Add(-95 * 24 * time.Hour)
	skillIngest(t, s, asSession([]Ingest{skillCall("tm-call", "sess-tm", "p9", "toolu_tm", `{"skill":"engg:temporal"}`, 1)}, readDeviceID, longAgo))
	adminRow, ok := inPruning("temporal")
	if !ok || adminRow.LastUsedAt == nil {
		t.Fatalf("an admin's temporal = %+v %v, want its last use ninety-five days back", adminRow, ok)
	}
	if adminRow.DaysUnused != 95 {
		t.Errorf("an admin reads temporal's days_unused = %d, want 95", adminRow.DaysUnused)
	}
	var seen bool
	for _, r := range inPruningFor(t, s, member) {
		if r.Skill != "temporal" {
			continue
		}
		seen = true
		if r.LastUsedAt != nil {
			t.Errorf("a member reads temporal's fleet last_used_at %v", r.LastUsedAt)
		}
		if r.DaysUnused != r.DaysInCatalog {
			t.Errorf("a member reads temporal's days_unused = %d against %d in catalog", r.DaysUnused, r.DaysInCatalog)
		}
	}
	if !seen {
		t.Fatal("temporal is not in a member's pruning report")
	}
	// The same on the zero-use report, which every member sees, and the
	// fold drops no row: both viewers list the same entries.
	adminUnused, err := s.SkillUnused(ctx, admin, SkillFilter{})
	if err != nil {
		t.Fatal(err)
	}
	memberUnused, err := s.SkillUnused(ctx, member, SkillFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(adminUnused) != len(memberUnused) || len(memberUnused) == 0 {
		t.Fatalf("admin sees %d unused entries, member %d", len(adminUnused), len(memberUnused))
	}
	for i := range memberUnused {
		if adminUnused[i].Skill != memberUnused[i].Skill {
			t.Fatalf("the reports disagree on their rows: %q vs %q", adminUnused[i].Skill, memberUnused[i].Skill)
		}
		if memberUnused[i].Skill == "temporal" && (adminUnused[i].LastUsedAt == nil || memberUnused[i].LastUsedAt != nil) {
			t.Errorf("temporal's last use on the zero-use report: admin %v, member %v", adminUnused[i].LastUsedAt, memberUnused[i].LastUsedAt)
		}
	}
}

// inPruningFor is the pruning report as one viewer reads it.
func inPruningFor(t *testing.T, s *Store, v Viewer) []SkillPruningRow {
	t.Helper()
	rows, err := s.SkillPruning(context.Background(), v, SkillFilter{})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestIntegrationComplianceReadsBeaconsReconcilersAndRuns: per (day,
// platform, lineage) the beacon and reconciler rows join on link_ref and
// every local day the run's window COVERS carries it; a member is refused.
//
// The fixture pins its instants to yesterday noon rather than to now minus
// three hours, and the run's window deliberately crosses UTC midnight.
// Both are review-3 finding 1: the day attribution is a local-date
// computation, so a clock-relative fixture moved the run's day with the
// hour the suite happened to run and this test was red only between 02:00
// and 03:00 UTC, while the defect it was hiding is permanent (an hourly
// reconciler crossing midnight left the day it reconciled reading "no run"
// forever).
func TestIntegrationComplianceReadsBeaconsReconcilersAndRuns(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	seedCatalog(t, s)
	admin := intAdminViewer(t, s)
	mustPrincipal(t, s, readOther, RoleMember)
	_, beacon := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 30})
	_, recon := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "reconciler", ExpiresInDays: 30, AllowedOrigins: []string{OriginReconciler}})
	// Midnight UTC today, so every instant below sits on a known local date
	// whatever the hour: the rows land on yesterday, 22:30 onwards.
	midnight := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := midnight.AddDate(0, 0, -1)
	at := midnight.Add(-90 * time.Minute)
	link := "lsref-1"
	// Two beacon rows, one of them the reconciler's link; two reconciler rows.
	beaconRow := SkillRow{Origin: OriginBeacon, AgentPlatform: PlatformDevin, Trust: TrustClaimed, SourceTokenID: &beacon.ID, RawName: "engg:git", Plugin: "engg", Skill: strPtr("git"),
		SkillSource: SkillSourceMirror, Trigger: TriggerAgent, Outcome: OutcomeStarted, SessionRef: link, IdempotencyKey: strPtr("b-1"), OccurredAt: at, TokenPlatform: PlatformDevin, TokenEnvironment: "default"}
	if _, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, beaconRow); err != nil {
		t.Fatal(err)
	}
	beaconRow.SessionRef, beaconRow.IdempotencyKey = "other-session", strPtr("b-2")
	if _, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, beaconRow); err != nil {
		t.Fatal(err)
	}
	// A third beacon row four days back, on a day no run covers: its
	// last_run_at must stay nil, which is what "no run" means.
	uncovered := midnight.AddDate(0, 0, -4).Add(12 * time.Hour)
	beaconRow.SessionRef, beaconRow.IdempotencyKey, beaconRow.OccurredAt = "older-session", strPtr("b-3"), uncovered
	if _, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, beaconRow); err != nil {
		t.Fatal(err)
	}
	claimedRow(t, s, recon, OriginReconciler, PlatformDevin, "git", "r-1", at.Add(time.Minute), &link)
	claimedRow(t, s, recon, OriginReconciler, PlatformDevin, "git", "r-2", at.Add(2*time.Minute), nil)
	// The window opens on yesterday evening and closes after midnight, so
	// it covers two local days and ends on neither row's day.
	windowEnd := midnight.Add(30 * time.Minute)
	if _, _, err := s.RecordReconcilerRun(ctx, recon.ID, ReconcilerRun{AgentPlatform: PlatformDevin, IdempotencyKey: "run-1", WindowStart: at.Add(-time.Hour), WindowEnd: windowEnd, RowsPosted: 2}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SkillCompliance(ctx, Viewer{Email: readOther, Role: RoleMember}, SkillFilter{}); !errors.Is(err, ErrNotAdmin) {
		t.Errorf("a member's compliance = %v", err)
	}
	rows, err := s.SkillCompliance(ctx, admin, SkillFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want the covered day and the uncovered one", rows)
	}
	byDay := map[string]SkillComplianceRow{}
	for _, r := range rows {
		byDay[r.Day.Format("2006-01-02")] = r
	}
	r, ok := byDay[yesterday.Format("2006-01-02")]
	if !ok {
		t.Fatalf("no row for %s: %+v", yesterday.Format("2006-01-02"), rows)
	}
	if r.Platform != PlatformDevin || r.Lineage != "example-skills/engg:git" || r.BeaconRows != 2 || r.ReconcilerRows == nil || *r.ReconcilerRows != 2 || r.ExactJoins != 1 {
		t.Errorf("row = %+v", r)
	}
	if r.CompliancePct == nil || *r.CompliancePct != 1 {
		t.Errorf("pct = %s, want 1", pctText(r.CompliancePct))
	}
	// The run's window ended after midnight, so keying it on window_end
	// alone left this day with no run at all; the covering rule attaches it
	// here, and the stamp is the window's end, not the moment of the POST.
	if r.LastRunAt == nil {
		t.Errorf("the day the run reconciled reads no run: %+v", r)
	} else if !r.LastRunAt.Equal(windowEnd) {
		t.Errorf("last run = %v, want the covering run's window_end %v", r.LastRunAt.UTC(), windowEnd)
	}
	older, ok := byDay[uncovered.Format("2006-01-02")]
	if !ok {
		t.Fatalf("no row for the uncovered day: %+v", rows)
	}
	if older.LastRunAt != nil {
		t.Errorf("a day no run covers reads last run %v", older.LastRunAt)
	}
}

// pctText prints a compliance ratio by value: a %v on the pointer prints a
// heap address, which is what made a failure here unreadable (review-3
// finding 1, secondary).
func pctText(p *float64) string {
	if p == nil {
		return "no run"
	}
	return strconv.FormatFloat(*p, 'f', -1, 64)
}

// TestIntegrationRebuildBannerReadsTheStep: the banner reads the version's
// step row and the sessions count while the step is open.
func TestIntegrationRebuildBannerReadsTheStep(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	rb, err := s.SkillRebuildProgress(ctx)
	if err != nil || rb.Rebuilding {
		t.Fatalf("before any step: %+v, %v", rb, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO derive_jobs (version, step, ordinal, processed) VALUES ($1, 'skill_invocations', 14, 3)
		ON CONFLICT (version, step) DO UPDATE SET finished_at = NULL, processed = 3`, DerivedSchema); err != nil {
		t.Fatal(err)
	}
	rb, err = s.SkillRebuildProgress(ctx)
	if err != nil || !rb.Rebuilding || rb.Processed != 3 {
		t.Errorf("an open step: %+v, %v", rb, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE derive_jobs SET finished_at = now() WHERE version = $1 AND step = 'skill_invocations'`, DerivedSchema); err != nil {
		t.Fatal(err)
	}
	if rb, err := s.SkillRebuildProgress(ctx); err != nil || rb.Rebuilding {
		t.Errorf("a finished step: %+v, %v", rb, err)
	}
}
