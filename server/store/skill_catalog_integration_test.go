//go:build integration

package store

// The half of the skill catalog only Postgres can answer: that 0023 applies
// cold inside one transaction without touching the largest table, and again
// on a migrated schema without changing anything; that a publish keeps
// first_seen_at, flips present both ways, absorbs a repeated commit and
// refuses the two SECURITY2-9 bodies; that the alias oracle reads what a
// publish wrote; and that a retried reconciler run is one row.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/server/auth"
)

const catalogMigration = "0023_skill_catalog.sql"

var catalogTables = []string{"skill_catalog_entries", "skill_catalog_aliases", "skill_catalog_publishes", "reconciler_runs"}

// TestIntegrationCatalogMigrationAppliesColdAndTwice is design 10.2
// "Migrations" for 0023: the file's body applied cold inside one
// transaction, the way the migrator applies it, takes no lock on events
// (pg_locks is read from inside that transaction before it commits), and a
// second Migrate on the migrated schema records the file once and leaves
// each table existing once.
func TestIntegrationCatalogMigrationAppliesColdAndTwice(t *testing.T) {
	newStore(t, nil)
	ctx := context.Background()
	body, err := migrations.ReadFile("migrations/" + catalogMigration)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Cold: the four tables are gone before the body runs, so every CREATE
	// in it does its work rather than reading IF NOT EXISTS as a no-op.
	if _, err := tx.Exec(ctx, `DROP TABLE `+strings.Join(catalogTables, ", ")+` CASCADE`); err != nil {
		t.Fatalf("drop the catalog tables: %v", err)
	}
	if _, err := tx.Exec(ctx, string(body)); err != nil {
		t.Fatalf("apply %s cold: %v", catalogMigration, err)
	}
	var onEvents int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
		WHERE l.pid = pg_backend_pid() AND c.relname IN ('events', 'sessions', 'messages')`).Scan(&onEvents); err != nil {
		t.Fatal(err)
	}
	if onEvents != 0 {
		t.Errorf("the cold apply holds %d locks on events, sessions or messages", onEvents)
	}
	var created int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
		WHERE l.pid = pg_backend_pid() AND c.relname = ANY($1::text[])`, catalogTables).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if created == 0 {
		t.Error("the cold apply holds no lock on the tables it created; the body did not run")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Twice: the ledger already names the file, and the IF NOT EXISTS forms
	// are inert on the tables the cold apply just made.
	if err := New(pool, nil).Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if n := countRows(t, `SELECT count(*) FROM schema_migrations WHERE name = $1`, catalogMigration); n != 1 {
		t.Errorf("%s recorded %d times, want 1", catalogMigration, n)
	}
	for _, table := range catalogTables {
		if n := countRows(t, `SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1`, table); n != 1 {
			t.Errorf("%s exists %d times after two applies", table, n)
		}
	}
	// The foreign keys reach source_tokens and the catalog's own entries
	// table, and nothing else.
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT ccu.table_name FROM information_schema.table_constraints tc
		JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = current_schema() AND tc.table_name = ANY($1::text[])`, catalogTables)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var referenced string
		if err := rows.Scan(&referenced); err != nil {
			t.Fatal(err)
		}
		if referenced != "source_tokens" && referenced != "skill_catalog_entries" {
			t.Errorf("a catalog foreign key references %s", referenced)
		}
	}
}

// ---------------------------------------------------------------- publishes

func catalogSkill(plugin, dir string, aliases ...string) CatalogSkill {
	return CatalogSkill{Slug: dir, Dir: dir, Name: dir, Plugin: plugin, Path: "plugins/" + plugin + "/skills/" + dir + "/SKILL.md",
		Sha256Tree: "sha-" + dir, Installable: true, Mirrored: true, AuthoredBy: AuthoredByUnknown, Aliases: aliases}
}

func catalogBody(repo, commit string, skills ...CatalogSkill) CatalogBody {
	return CatalogBody{Schema: 1, GeneratedAt: time.Date(2026, 9, 21, 6, 0, 0, 0, time.UTC), SourceRepo: repo, Commit: commit, Skills: skills}
}

// intCatalogToken mints the publisher's token for a repository.
func intCatalogToken(t *testing.T, s *Store, v Viewer, repo string) (string, SourceToken) {
	t.Helper()
	return intMint(t, s, v, MintRequest{Platform: PlatformClaudeCode, Environment: "catalog-" + repo, Scope: ScopeSkillCatalog, Label: repo, ExpiresInDays: 30})
}

// TestIntegrationCatalogPublishKeepsFirstSeenAndFlipsPresent is design 10.2
// "catalog": first_seen_at is kept across publishes while last_seen_at and
// commit move; an entry a later body drops flips present = false and comes
// back present when re-added; a repeated (source_repo, commit) is a
// duplicate that changes nothing; the aliases are the body's union with the
// composite and bare forms; and a skill-invocations token verifies as
// wrong_scope on the catalog scope.
func TestIntegrationCatalogPublishKeepsFirstSeenAndFlipsPresent(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	plaintext, tok := intCatalogToken(t, s, admin, "example-skills")

	res, err := s.PublishSkillCatalog(ctx, tok.ID, "example-skills", catalogBody("example-skills", "c1",
		catalogSkill("engg", "git", "engg:git", "git"), catalogSkill("engg", "temporal")))
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if res.Duplicate || res.Skills != 2 || res.Aliases != 4 || res.StaleEntries != 0 {
		t.Errorf("first publish = %+v, want 2 skills and 4 aliases (engg:git, git, engg:temporal, temporal)", res)
	}
	var firstSeen, lastSeen time.Time
	if err := pool.QueryRow(ctx, `SELECT first_seen_at, last_seen_at FROM skill_catalog_entries WHERE source_repo = 'example-skills' AND plugin = 'engg' AND skill = 'git'`).Scan(&firstSeen, &lastSeen); err != nil {
		t.Fatal(err)
	}
	if !firstSeen.Equal(lastSeen) {
		t.Errorf("a fresh entry's first_seen_at %v differs from last_seen_at %v", firstSeen, lastSeen)
	}
	// The oracle reads what the publish wrote, on both forms.
	for _, c := range []struct {
		plugin, skill string
		want          bool
	}{{"engg", "git", true}, {"", "git", true}, {"", "temporal", true}, {"", "standup", false}, {"engg", "standup", false}} {
		if got, err := s.ConfirmSkillAlias(ctx, s.db, c.plugin, c.skill); err != nil || got != c.want {
			t.Errorf("ConfirmSkillAlias(%q, %q) = %v, %v, want %v", c.plugin, c.skill, got, err, c.want)
		}
	}
	if s.AliasOracle == nil {
		t.Fatal("the store's oracle is nil; New did not wire ConfirmSkillAlias")
	}
	if ok, _ := s.AliasOracle(ctx, s.db, "", "git"); !ok {
		t.Error("the wired oracle does not confirm a published alias")
	}

	// The same commit again: nothing moves, not even the publish row's
	// received_at, and the result says duplicate.
	var received time.Time
	if err := pool.QueryRow(ctx, `SELECT received_at FROM skill_catalog_publishes WHERE source_repo = 'example-skills' AND commit = 'c1'`).Scan(&received); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	res, err = s.PublishSkillCatalog(ctx, tok.ID, "example-skills", catalogBody("example-skills", "c1", catalogSkill("engg", "git")))
	if err != nil || !res.Duplicate || res.Skills != 0 {
		t.Fatalf("repeated commit = %+v, %v, want duplicate and nothing else", res, err)
	}
	var again time.Time
	if err := pool.QueryRow(ctx, `SELECT received_at FROM skill_catalog_publishes WHERE source_repo = 'example-skills' AND commit = 'c1'`).Scan(&again); err != nil {
		t.Fatal(err)
	}
	if !again.Equal(received) || countRows(t, `SELECT count(*) FROM skill_catalog_publishes`) != 1 || countRows(t, `SELECT count(*) FROM skill_catalog_aliases`) != 4 {
		t.Error("a duplicate commit changed the publish row or the aliases")
	}

	// A later commit drops temporal: it flips present = false and keeps
	// its row; git keeps first_seen_at while last_seen_at and commit move.
	time.Sleep(5 * time.Millisecond)
	res, err = s.PublishSkillCatalog(ctx, tok.ID, "example-skills", catalogBody("example-skills", "c2", catalogSkill("engg", "git", "engg:git", "git")))
	if err != nil || res.Duplicate || res.Skills != 1 || res.Aliases != 2 {
		t.Fatalf("second publish = %+v, %v", res, err)
	}
	var present bool
	var commit string
	if err := pool.QueryRow(ctx, `SELECT present, commit FROM skill_catalog_entries WHERE plugin = 'engg' AND skill = 'temporal'`).Scan(&present, &commit); err != nil {
		t.Fatal(err)
	}
	if present || commit != "c1" {
		t.Errorf("the dropped entry reads present=%v commit=%q, want absent with its last commit", present, commit)
	}
	var firstSeen2, lastSeen2 time.Time
	if err := pool.QueryRow(ctx, `SELECT first_seen_at, last_seen_at, commit FROM skill_catalog_entries WHERE plugin = 'engg' AND skill = 'git'`).Scan(&firstSeen2, &lastSeen2, &commit); err != nil {
		t.Fatal(err)
	}
	if !firstSeen2.Equal(firstSeen) || !lastSeen2.After(lastSeen) || commit != "c2" {
		t.Errorf("git after the second publish: first_seen %v (was %v), last_seen %v (was %v), commit %q", firstSeen2, firstSeen, lastSeen2, lastSeen, commit)
	}
	if ok, _ := s.ConfirmSkillAlias(ctx, s.db, "", "temporal"); ok {
		t.Error("a dropped entry's alias survived the replacement")
	}
	// And back: re-added, it is present again with its first_seen_at kept.
	res, err = s.PublishSkillCatalog(ctx, tok.ID, "example-skills", catalogBody("example-skills", "c3", catalogSkill("engg", "git"), catalogSkill("engg", "temporal")))
	if err != nil || res.Skills != 2 {
		t.Fatalf("third publish = %+v, %v", res, err)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_catalog_entries WHERE present AND source_repo = 'example-skills'`); n != 2 {
		t.Errorf("%d present entries after re-adding, want 2", n)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_catalog_entries WHERE plugin = 'engg' AND skill = 'temporal' AND present AND commit = 'c3'`); n != 1 {
		t.Error("the re-added entry is not present under the new commit")
	}

	// The publisher's token on the invocation route's scope is wrong_scope,
	// and an invocation token on the catalog scope likewise (T10).
	if st := intVerify(t, s, poolDB{pool: pool}, plaintext, ScopeSkillInvocations).State; st != TokenStateWrongScope {
		t.Errorf("a catalog token on the invocation scope verifies %q, want wrong_scope", st)
	}
	emitter, _ := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "default", ExpiresInDays: 30})
	if st := intVerify(t, s, poolDB{pool: pool}, emitter, ScopeSkillCatalog).State; st != TokenStateWrongScope {
		t.Errorf("an emitter token on the catalog scope verifies %q, want wrong_scope", st)
	}
	_ = auth.HashToken
}

// TestIntegrationCatalogPublishRefusesForeignAliasesAndUnknownLineage is
// SECURITY2-9: an alias another repository holds is refused unless one
// entry's lineage_of names the other, and a lineage_of that names no entry
// is refused; each refusal rolls the whole publish back.
func TestIntegrationCatalogPublishRefusesForeignAliasesAndUnknownLineage(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	_, head := intCatalogToken(t, s, admin, "example-skills")
	_, mirror := intCatalogToken(t, s, admin, "backend")

	if _, err := s.PublishSkillCatalog(ctx, head.ID, "example-skills", catalogBody("example-skills", "h1", catalogSkill("engg", "git", "engg:git", "git"))); err != nil {
		t.Fatal(err)
	}
	// backend claims git with no lineage: 409.
	_, err := s.PublishSkillCatalog(ctx, mirror.ID, "backend", catalogBody("backend", "b1", catalogSkill("engg", "git")))
	var conflict *AliasConflictError
	// The bare form sorts first, so the refusal names git rather than
	// engg:git; both are the head's.
	if !errors.As(err, &conflict) || conflict.HeldBy != "example-skills" || conflict.Alias != "git" {
		t.Fatalf("a foreign alias without lineage: err = %v, want AliasConflictError on git held by example-skills", err)
	}
	if countRows(t, `SELECT count(*) FROM skill_catalog_publishes WHERE source_repo = 'backend'`) != 0 || countRows(t, `SELECT count(*) FROM skill_catalog_entries WHERE source_repo = 'backend'`) != 0 {
		t.Error("the refused publish left rows behind")
	}
	// A lineage naming no entry: 400, rolled back.
	nowhere := "example-skills/engg:nowhere"
	sk := catalogSkill("engg", "git")
	sk.LineageOf = &nowhere
	_, err = s.PublishSkillCatalog(ctx, mirror.ID, "backend", catalogBody("backend", "b2", sk))
	var lineage *LineageError
	if !errors.As(err, &lineage) || lineage.Lineage != nowhere {
		t.Fatalf("an unknown lineage: err = %v, want LineageError", err)
	}
	if countRows(t, `SELECT count(*) FROM skill_catalog_entries WHERE source_repo = 'backend'`) != 0 {
		t.Error("the refused publish left an entry behind")
	}
	// With the lineage naming the head, the same alias is allowed, and the
	// mirror's tree hash matching the head's reads no stale entry.
	headKey := "example-skills/engg:git"
	sk.LineageOf = &headKey
	res, err := s.PublishSkillCatalog(ctx, mirror.ID, "backend", catalogBody("backend", "b3", sk))
	if err != nil || res.Aliases != 2 || res.StaleEntries != 0 {
		t.Fatalf("a lineage publish = %+v, %v", res, err)
	}
	// The head republished with a new tree: the mirror's entry is stale.
	changed := catalogSkill("engg", "git", "engg:git", "git")
	changed.Sha256Tree = "sha-git-2"
	if _, err := s.PublishSkillCatalog(ctx, head.ID, "example-skills", catalogBody("example-skills", "h2", changed)); err != nil {
		t.Fatal(err)
	}
	res, err = s.PublishSkillCatalog(ctx, mirror.ID, "backend", catalogBody("backend", "b4", sk))
	if err != nil || res.StaleEntries != 1 {
		t.Fatalf("the mirror behind the head = %+v, %v, want one stale entry", res, err)
	}
	// The head's aliases are untouched by the mirror's publishes.
	if n := countRows(t, `SELECT count(*) FROM skill_catalog_aliases WHERE source_repo = 'example-skills'`); n != 2 {
		t.Errorf("the head holds %d aliases after the mirror published, want 2", n)
	}
}

// TestIntegrationCatalogPublishRaceLeavesOneAliasOwner is the race the
// review named as uncovered: two repositories publishing the same alias at
// the same time. The conflict check is a SELECT inside each transaction, so
// neither would see the other's uncommitted rows, and the aliases' primary
// key carries source_repo, so it lets two repositories hold one name and
// blocks nothing across them. What holds the line is the advisory lock
// PublishSkillCatalog takes first (skill_catalog.go, catalogPublishLockKey):
// it serialises the two publishes, so the second reads the first's
// committed rows and is refused. Exactly one publish writes the aliases,
// the other gets AliasConflictError naming the holder, and no alias row is
// left pointing at an entry of a repository that does not hold it. Remove
// the lock and this test fails on both counts, which is the point of
// asserting the outcomes rather than a range of them.
func TestIntegrationCatalogPublishRaceLeavesOneAliasOwner(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	_, head := intCatalogToken(t, s, admin, "example-skills")
	_, mirror := intCatalogToken(t, s, admin, "backend")

	type outcome struct {
		res PublishResult
		err error
	}
	out := make([]outcome, 2)
	repos := []struct {
		name, commit, token string
	}{
		{"example-skills", "h1", head.ID},
		{"backend", "b1", mirror.ID},
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, repo := range repos {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// The same skill in both bodies, so both publishes claim the
			// aliases engg:git and git in the same order: identical order
			// is what keeps the two INSERTs off a deadlock.
			body := catalogBody(repo.name, repo.commit, catalogSkill("engg", "git", "engg:git", "git"))
			out[i].res, out[i].err = s.PublishSkillCatalog(ctx, repo.token, repo.name, body)
		}()
	}
	close(start)
	wg.Wait()

	var winners, refused int
	for i, o := range out {
		var conflict *AliasConflictError
		switch {
		case o.err == nil && o.res.Aliases == 2:
			winners++
		case errors.As(o.err, &conflict):
			refused++
			if conflict.HeldBy == repos[i].name {
				t.Errorf("%s was refused its own alias", repos[i].name)
			}
		default:
			t.Fatalf("%s = %+v, %v", repos[i].name, o.res, o.err)
		}
	}
	if winners != 1 || refused != 1 {
		t.Fatalf("%d publishes wrote the aliases and %d were refused, want 1 and 1", winners, refused)
	}
	if n := countRows(t, `SELECT count(DISTINCT source_repo) FROM skill_catalog_aliases`); n != 1 {
		t.Errorf("%d repositories hold alias rows, want 1", n)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_catalog_aliases`); n != 2 {
		t.Errorf("%d alias rows, want the winner's 2", n)
	}
	// Every alias row still points at an entry its own repository holds,
	// which is what the conflict check's join reads.
	if n := countRows(t, `
		SELECT count(*) FROM skill_catalog_aliases a
		WHERE NOT EXISTS (SELECT 1 FROM skill_catalog_entries e
		  WHERE (e.source_repo, e.plugin, e.skill) = (a.source_repo, a.plugin, a.skill))`); n != 0 {
		t.Errorf("%d alias rows point at no entry of their repository", n)
	}
}

// ---------------------------------------------------------------- reconciler runs

// TestIntegrationReconcilerRunRetryIsOneRow is design 10.2 "Reconcilers": a
// retried run with equal counts is one row and a duplicate, one with
// different counts is a conflict and still one row, and the run line counts
// the rows the token posted since the previous run.
func TestIntegrationReconcilerRunRetryIsOneRow(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	admin := intAdminViewer(t, s)
	_, tok := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "reconciler", ExpiresInDays: 30, AllowedOrigins: []string{OriginReconciler}})
	at := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	key := "k-1"
	for i, id := range []string{"r-1", "r-2"} {
		row := SkillRow{Origin: OriginReconciler, AgentPlatform: PlatformDevin, Trust: TrustClaimed, SourceTokenID: &tok.ID,
			RawName: "engg:git", Plugin: "engg", Skill: strPtr("git"), SkillSource: SkillSourcePlugin, Trigger: TriggerUnknown, Outcome: OutcomeUnknown,
			SessionRef: "devin-" + id, IdempotencyKey: strPtr(key + "-" + id), OccurredAt: at.Add(time.Duration(i) * time.Minute),
			TokenPlatform: PlatformDevin, TokenEnvironment: "reconciler"}
		if _, err := s.UpsertSkillInvocation(ctx, poolDB{pool: pool}, row); err != nil {
			t.Fatal(err)
		}
	}
	run := ReconcilerRun{AgentPlatform: PlatformDevin, IdempotencyKey: "2026-09-21", WindowStart: at.Add(-24 * time.Hour), WindowEnd: at.Add(time.Hour),
		SessionsScanned: 5, SessionsWithEvents: 2, RowsPosted: 2}
	dup, conflict, err := s.RecordReconcilerRun(ctx, tok.ID, run)
	if err != nil || dup || conflict {
		t.Fatalf("first run = dup %v conflict %v err %v", dup, conflict, err)
	}
	dup, conflict, err = s.RecordReconcilerRun(ctx, tok.ID, run)
	if err != nil || !dup || conflict {
		t.Fatalf("retried run = dup %v conflict %v err %v, want a duplicate", dup, conflict, err)
	}
	differing := run
	differing.RowsPosted = 3
	dup, conflict, err = s.RecordReconcilerRun(ctx, tok.ID, differing)
	if err != nil || dup || !conflict {
		t.Fatalf("a retry with other counts = dup %v conflict %v err %v, want a conflict", dup, conflict, err)
	}
	if n := countRows(t, `SELECT count(*) FROM reconciler_runs`); n != 1 {
		t.Errorf("%d run rows after three posts, want 1", n)
	}
	lines := linesWithMessage(skillLines(t, &buf), lineReconcilerRun)
	if len(lines) != 2 {
		t.Fatalf("%d run lines, want 2 (the insert and the duplicate; a conflict records no run)", len(lines))
	}
	if lines[0]["mismatch"] != false || lines[0]["rows_seen"] != float64(2) || lines[0]["rows_posted"] != float64(2) || lines[0]["level"] != "INFO" || lines[0]["platform"] != PlatformDevin || lines[0]["source_token_id"] != tok.ID {
		t.Errorf("first run line = %v", lines[0])
	}
	if lines[1]["duplicate"] != true {
		t.Errorf("the retried run's line does not say duplicate: %v", lines[1])
	}
	// A second run with a posted count the rows do not bear: WARNING.
	next := ReconcilerRun{AgentPlatform: PlatformDevin, IdempotencyKey: "2026-09-22", WindowStart: at.Add(time.Hour), WindowEnd: at.Add(2 * time.Hour), RowsPosted: 4}
	if _, _, err := s.RecordReconcilerRun(ctx, tok.ID, next); err != nil {
		t.Fatal(err)
	}
	lines = linesWithMessage(skillLines(t, &buf), lineReconcilerRun)
	last := lines[len(lines)-1]
	if last["mismatch"] != true || last["level"] != "WARN" || last["rows_seen"] != float64(0) {
		t.Errorf("mismatched run line = %v", last)
	}
}

// TestIntegrationCatalogPublishRefusesAWipeAndRepairsOne is design 4d as
// amended 2026-09-22 (LS-3's adversarial finding 1), on real rows: an empty
// body is refused with both counts and leaves every entry, alias and oracle
// answer exactly as they were; a body past the half is refused the same way
// and one at the half is accepted; the counts an accepted publish reports
// are what the route's WARNING reads; allow_shrink lets the deliberate case
// through; and re-running the last good CI job on the SAME commit repairs
// the wipe, which the plain duplicate gate made impossible.
func TestIntegrationCatalogPublishRefusesAWipeAndRepairsOne(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	admin := intAdminViewer(t, s)
	_, tok := intCatalogToken(t, s, admin, "example-skills")
	full := func(commit string) CatalogBody {
		return catalogBody("example-skills", commit,
			catalogSkill("engg", "git", "engg:git", "git"), catalogSkill("engg", "temporal"),
			catalogSkill("engg", "plan"), catalogSkill("engg", "doc"))
	}
	if res, err := s.PublishSkillCatalog(ctx, tok.ID, "example-skills", full("good-1")); err != nil || res.Skills != 4 || res.PresentBefore != 0 {
		t.Fatalf("the first publish = %+v, %v", res, err)
	}
	aliasesBefore := countRows(t, `SELECT count(*) FROM skill_catalog_aliases WHERE source_repo = 'example-skills'`)
	presentBefore := countRows(t, `SELECT count(*) FROM skill_catalog_entries WHERE source_repo = 'example-skills' AND present`)
	if presentBefore != 4 || aliasesBefore == 0 {
		t.Fatalf("seeded %d present entries and %d aliases", presentBefore, aliasesBefore)
	}

	// The generator globbed an empty checkout. Refused, with both counts,
	// and nothing moved: the oracle still answers for every skill, which is
	// what keeps every derived invocation from turning unconfirmed.
	var se *CatalogShrinkError
	_, err := s.PublishSkillCatalog(ctx, tok.ID, "example-skills", catalogBody("example-skills", "empty-1"))
	if !errors.As(err, &se) || se.Present != 4 || se.Incoming != 0 {
		t.Fatalf("an empty publish = %v, want a shrink refusal with 4 present and 0 incoming", err)
	}
	// Past the half: three of four retired.
	_, err = s.PublishSkillCatalog(ctx, tok.ID, "example-skills", catalogBody("example-skills", "shrink-1", catalogSkill("engg", "git")))
	if !errors.As(err, &se) || se.Present != 4 || se.Incoming != 1 {
		t.Fatalf("a publish past the half = %v", err)
	}
	for _, q := range []struct {
		sql  string
		want int
	}{
		{`SELECT count(*) FROM skill_catalog_entries WHERE source_repo = 'example-skills' AND present`, 4},
		{`SELECT count(*) FROM skill_catalog_aliases WHERE source_repo = 'example-skills'`, aliasesBefore},
		{`SELECT count(*) FROM skill_catalog_publishes WHERE source_repo = 'example-skills'`, 1},
	} {
		if n := countRows(t, q.sql); n != q.want {
			t.Errorf("after two refusals %s = %d, want %d", q.sql, n, q.want)
		}
	}
	for _, name := range []string{"git", "temporal", "plan", "doc"} {
		if ok, err := s.ConfirmSkillAlias(ctx, s.db, "", name); err != nil || !ok {
			t.Errorf("after the refusals the oracle answers %v for %q", ok, name)
		}
	}

	// At the half exactly: accepted, and the counts the route's WARNING
	// reads say the catalog fell from four to two.
	res, err := s.PublishSkillCatalog(ctx, tok.ID, "example-skills", catalogBody("example-skills", "half-1",
		catalogSkill("engg", "git", "engg:git", "git"), catalogSkill("engg", "temporal")))
	if err != nil {
		t.Fatalf("a publish at the half: %v", err)
	}
	if res.Skills != 2 || res.PresentBefore != 4 {
		t.Errorf("the half publish = %+v, want 2 incoming against 4 present", res)
	}

	// The deliberate case: allow_shrink empties the catalog, which is the
	// wipe the gate refuses without it.
	wipe := catalogBody("example-skills", "empty-2")
	wipe.AllowShrink = true
	if res, err := s.PublishSkillCatalog(ctx, tok.ID, "example-skills", wipe); err != nil || res.Skills != 0 {
		t.Fatalf("allow_shrink = %+v, %v", res, err)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_catalog_entries WHERE source_repo = 'example-skills' AND present`); n != 0 {
		t.Errorf("%d present entries after the override, want the wipe it asked for", n)
	}
	if ok, _ := s.ConfirmSkillAlias(ctx, s.db, "", "git"); ok {
		t.Error("the override left the aliases in place")
	}

	// The repair: re-run the CI job that produced empty-2 with the real
	// checkout, the SAME commit. The stored row recorded skills = 0, so it
	// is not a publish worth keeping and the body is let through.
	res, err = s.PublishSkillCatalog(ctx, tok.ID, "example-skills", full("empty-2"))
	if err != nil {
		t.Fatalf("the repair publish: %v", err)
	}
	if res.Duplicate {
		t.Fatal("re-running the last good job on the same commit was answered duplicate, so a wiped catalog cannot be repaired without a fresh sha")
	}
	if res.Skills != 4 || res.PresentBefore != 0 {
		t.Errorf("the repair = %+v", res)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_catalog_entries WHERE source_repo = 'example-skills' AND present`); n != 4 {
		t.Errorf("%d present entries after the repair, want 4", n)
	}
	for _, name := range []string{"git", "temporal", "plan", "doc"} {
		if ok, err := s.ConfirmSkillAlias(ctx, s.db, "", name); err != nil || !ok {
			t.Errorf("after the repair the oracle answers %v for %q", ok, name)
		}
	}
	// The repaired row carries the entries now, so a further re-PUT of it
	// is an ordinary duplicate again.
	if res, err := s.PublishSkillCatalog(ctx, tok.ID, "example-skills", full("empty-2")); err != nil || !res.Duplicate {
		t.Errorf("a re-PUT of the repaired commit = %+v, %v, want duplicate", res, err)
	}
	if n := countRows(t, `SELECT count(*) FROM skill_catalog_publishes WHERE source_repo = 'example-skills' AND commit = 'empty-2'`); n != 1 {
		t.Errorf("%d publish rows for empty-2, want the one the repair overwrote", n)
	}
}

// A run counts the rows of its OWN window by occurred_at, not the rows
// that ARRIVED since the run before it. received_at does not move on the
// idempotent upsert, so a row the platform re-posts arrived once, before
// the earlier run: an arrival window read a real re-post as zero rows
// against a real rows_posted, so mismatch went true and policy 17's fourth
// condition paged the on-call for a re-scan that changed nothing
// (adversarial iteration 5, finding 5).
func TestIntegrationAReReconciledWindowCountsItsOwnRowsAndDoesNotAlarm(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	admin := intAdminViewer(t, s)
	_, tok := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "reconciler", ExpiresInDays: 30, AllowedOrigins: []string{OriginReconciler}})
	at := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	window := ReconcilerRun{AgentPlatform: PlatformDevin, WindowStart: at.Add(-time.Hour), WindowEnd: at.Add(time.Hour), RowsPosted: 1}

	claimedRow(t, s, tok, OriginReconciler, PlatformDevin, "git", "rescan-1", at, nil)
	first := window
	first.IdempotencyKey = "2026-09-21-a"
	if _, _, err := s.RecordReconcilerRun(ctx, tok.ID, first); err != nil {
		t.Fatal(err)
	}

	// The platform re-scans the same period under a new run key and
	// re-posts the same row. The upsert stores nothing new, so received_at
	// stays where it was, before the first run.
	buf.Reset()
	claimedRow(t, s, tok, OriginReconciler, PlatformDevin, "git", "rescan-1", at, nil)
	if n := countRows(t, `SELECT count(*) FROM skill_invocations WHERE origin = 'reconciler'`); n != 1 {
		t.Fatalf("%d rows after the re-post, want the one idempotent row", n)
	}
	second := window
	second.IdempotencyKey = "2026-09-21-b"
	if _, _, err := s.RecordReconcilerRun(ctx, tok.ID, second); err != nil {
		t.Fatal(err)
	}
	lines := linesWithMessage(skillLines(t, &buf), lineReconcilerRun)
	if len(lines) != 1 {
		t.Fatalf("%d lines for the re-scan", len(lines))
	}
	l := lines[0]
	if l["rows_seen"] != float64(1) {
		t.Errorf("rows_seen = %v for a re-reconciled window, want the 1 row it covers", l["rows_seen"])
	}
	if l["mismatch"] != false || l["level"] != "INFO" {
		t.Errorf("a re-scan that changed nothing pages the on-call: %v", l)
	}

	// A window holding none of the platform's rows still mismatches when
	// the run claims to have posted some: the alarm keeps its purpose.
	buf.Reset()
	empty := ReconcilerRun{AgentPlatform: PlatformDevin, IdempotencyKey: "2026-09-22", WindowStart: at.Add(24 * time.Hour), WindowEnd: at.Add(25 * time.Hour), RowsPosted: 3}
	if _, _, err := s.RecordReconcilerRun(ctx, tok.ID, empty); err != nil {
		t.Fatal(err)
	}
	last := linesWithMessage(skillLines(t, &buf), lineReconcilerRun)
	if n := len(last); n != 1 || last[0]["rows_seen"] != float64(0) || last[0]["mismatch"] != true || last[0]["level"] != "WARN" {
		t.Errorf("an empty window claiming three rows = %v", last)
	}
}

// TestIntegrationReconcilerRunUnderARotatedTokenDoesNotAlarm is the case
// 0023's own comment advertises ("a run retried under a rotated token lands
// on the row the compliance report ... read"), which used to page the
// on-call: rows_seen was scoped to the CALLER's token, so the retry
// counted zero against a real rows_posted, mismatch went true and
// skill_reconciler_mismatch with policy 17's fourth condition turned a
// routine rotation into an alarm (adversarial finding 4).
func TestIntegrationReconcilerRunUnderARotatedTokenDoesNotAlarm(t *testing.T) {
	s := newStore(t, nil)
	ctx := context.Background()
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewJSONHandler(&buf, nil)))
	admin := intAdminViewer(t, s)
	_, old := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "reconciler", ExpiresInDays: 30, AllowedOrigins: []string{OriginReconciler}})
	_, fresh := intMint(t, s, admin, MintRequest{Platform: PlatformDevin, Environment: "reconciler", ExpiresInDays: 30, Label: "rotated", AllowedOrigins: []string{OriginReconciler}})
	at := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	// The rows go out under the old token, as they did before the rotation.
	for i, id := range []string{"rot-1", "rot-2"} {
		claimedRow(t, s, old, OriginReconciler, PlatformDevin, "git", id, at.Add(time.Duration(i)*time.Minute), nil)
	}
	run := ReconcilerRun{AgentPlatform: PlatformDevin, IdempotencyKey: "2026-09-21", WindowStart: at.Add(-24 * time.Hour), WindowEnd: at.Add(time.Hour), RowsPosted: 2}
	if _, _, err := s.RecordReconcilerRun(ctx, old.ID, run); err != nil {
		t.Fatal(err)
	}
	first := linesWithMessage(skillLines(t, &buf), lineReconcilerRun)
	if n := len(first); n != 1 || first[0]["mismatch"] != false || first[0]["rows_seen"] != float64(2) {
		t.Fatalf("the first run's line = %v", first)
	}

	// The secret rotates and the reconciler retries the same run key.
	buf.Reset()
	dup, conflict, err := s.RecordReconcilerRun(ctx, fresh.ID, run)
	if err != nil || !dup || conflict {
		t.Fatalf("the rotated retry = dup %v conflict %v err %v, want a duplicate", dup, conflict, err)
	}
	lines := linesWithMessage(skillLines(t, &buf), lineReconcilerRun)
	if len(lines) != 1 {
		t.Fatalf("%d lines for the rotated retry", len(lines))
	}
	l := lines[0]
	if l["mismatch"] != false || l["level"] != "WARN" && l["level"] != "INFO" {
		t.Fatalf("the rotated retry's line = %v", l)
	}
	if l["level"] != "INFO" {
		t.Errorf("a rotation pages the on-call: the retry's line is %v", l["level"])
	}
	if l["rows_seen"] != float64(2) {
		t.Errorf("rows_seen = %v under the rotated token, want the platform's 2", l["rows_seen"])
	}
	if l["source_token_id"] != fresh.ID || l["duplicate"] != true {
		t.Errorf("the retry's line = %v", l)
	}
	if n := countRows(t, `SELECT count(*) FROM reconciler_runs`); n != 1 {
		t.Errorf("%d run rows after the rotated retry, want 1", n)
	}
	if n := countRows(t, `SELECT count(*) FROM reconciler_runs WHERE source_token_id = $1::uuid`, old.ID); n != 1 {
		t.Error("the retry moved the run onto the new token's row")
	}
}
