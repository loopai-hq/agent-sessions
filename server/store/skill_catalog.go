package store

// The skill catalog: what skills exist under which names, published
// by each source repository's CI, and the alias table that resolves an
// invocation row to an entry and confirms a typed command the shape rule
// read. The reconciler-run ledger sits beside it because it ships in
// the same migration and the compliance report reads both.
//
// One transaction per publish, keyed on (source_repo, commit): a re-PUT of a
// commit already recorded changes nothing and says so. Entries a body no
// longer carries flip present = false rather than being deleted, so
// first_seen_at survives and the pruning report keeps its clock. Aliases
// are replaced wholesale for the repository, since they are a function of
// the body and nothing else.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/skilllog"
)

// ---------------------------------------------------------------- closed enums

// The closed sets of 0023, as Go constants, one slice per column in the
// CHECK's order; the closed-enum guard compares them with the migration text
// the way it does 0022's.
const (
	AuthoredByHuman   = "human"
	AuthoredByAgent   = "agent"
	AuthoredByVendor  = "vendor"
	AuthoredByUnknown = "unknown"

	AuthorEvidenceNone           = ""
	AuthorEvidenceFrontmatter    = "frontmatter"
	AuthorEvidenceGitFirstCommit = "git_first_commit"
)

// The same sets as slices, in the CHECK's order, for the closed-enum guard
// and for the UI's option lists.
var (
	// AuthoredBys is skill_catalog.authored_by's accepted values.
	AuthoredBys = []string{AuthoredByHuman, AuthoredByAgent, AuthoredByVendor, AuthoredByUnknown}
	// AuthorEvidences is skill_catalog.author_evidence's accepted values.
	AuthorEvidences = []string{AuthorEvidenceNone, AuthorEvidenceFrontmatter, AuthorEvidenceGitFirstCommit}
	// ReconcilerPlatforms is the reconciler_runs.agent_platform CHECK: the
	// platforms whose activation log a reconciler replays. The two the
	// store derives from transcripts are not among them.
	ReconcilerPlatforms = []string{PlatformDevin, PlatformCapy, PlatformCodex, PlatformVorflux}
)

// ---------------------------------------------------------------- the alias oracle

// confirmAliasSQL is one indexed read of the aliases' primary key prefix.
const confirmAliasSQL = `SELECT EXISTS (SELECT 1 FROM skill_catalog_aliases WHERE alias_plugin = $1 AND alias_skill = $2)`

// ConfirmSkillAlias is the 4a confirmation oracle: true iff the catalog
// holds an alias (plugin, skill), whichever repository published it. It
// reads on the Queryer the caller passes, which at ingest is the events
// transaction itself: catalog rows are committed by the publish route, so
// reading them from inside the derivation's savepoint answers the same
// thing a pool read would, and it does so without taking a second pool
// connection while the first one is held (adversarial iteration 5). New
// and NewWithDB assign it to AliasOracle, so the derivation consults it
// for every name the shape rule read; a name from a transcript envelope
// never reaches it.
func (s *Store) ConfirmSkillAlias(ctx context.Context, q Queryer, plugin, skill string) (bool, error) {
	var found bool
	if err := q.QueryRow(ctx, confirmAliasSQL, plugin, skill).Scan(&found); err != nil {
		return false, fmt.Errorf("store: confirm skill alias: %w", err)
	}
	return found, nil
}

// ---------------------------------------------------------------- the catalog body

// CatalogBody is a catalog as the publisher PUTs it (design 4d; the sample
// at research/r5-requirements-catalog-prior-art.md), with its keys already
// checked by the route: the store re-checks the shapes the CHECKs would
// refuse, so a bad value is a field error and never a statement error.
type CatalogBody struct {
	Schema      int
	GeneratedAt time.Time
	SourceRepo  string
	Commit      string
	Skills      []CatalogSkill
	// AllowShrink overrides the shrink refusal below. The publish script
	// never sets it; a human does, once, with a second pair of eyes (the
	// runbook's recovery step).
	AllowShrink bool
}

// CatalogSkill is one entry of the body: the thirteen per-skill keys. Dir is
// the identity (the entries' skill column); Slug and Name are carried for
// the aliases the route already folded into Aliases. LineageOf is the
// '<source_repo>/<plugin>:<skill>' of the lineage head, nil on a head.
type CatalogSkill struct {
	Slug, Dir, Name, Plugin, Path, Sha256Tree string
	Installable, Mirrored                     bool
	SkipReason                                *string
	AuthoredBy, AuthorEvidence                string
	LineageOf                                 *string
	Aliases                                   []string
}

// PublishResult is what a publish came to. Duplicate means the commit was
// already recorded and nothing else was touched; the counts are the entries
// upserted, the alias rows written and the present entries of the
// repository that read stale against their lineage head after the write.
// PresentBefore is what the repository held present when the transaction
// opened, so the route can tell a catalog that grew from one that shrank.
type PublishResult struct {
	Duplicate       bool
	Skills, Aliases int
	StaleEntries    int
	PresentBefore   int
}

// CatalogFieldError names the field of a body the store refused, for a 400
// on that field.
type CatalogFieldError struct {
	Field, Reason string
}

func (e *CatalogFieldError) Error() string {
	return "store: skill catalog: " + e.Field + " " + e.Reason
}

// LineageError is a lineage_of that names no entry (design 4d): the head
// must be published first, which is why the backend catalog publishes
// after the marketplace's.
type LineageError struct{ Lineage string }

func (e *LineageError) Error() string {
	return "store: skill catalog: lineage_of names no entry: " + e.Lineage
}

// CatalogShrinkError is a body that would retire most of a repository's
// catalog (design 4d as amended 2026-09-22 from LS-3's adversarial finding
// 1): an accepted publish may never flip more than half of a repository's
// present entries to absent, and an empty body is refused outright. One
// generator run over an empty checkout would otherwise leave every alias
// deleted, the confirmation oracle answering false for every skill, and the
// unknown panel full of real names, while the publish answered 200 and
// logged INFO. Overridable by allow_shrink for the one case that is
// deliberate: a repository that really did delete its skills.
type CatalogShrinkError struct{ Present, Incoming int }

func (e *CatalogShrinkError) Error() string {
	return fmt.Sprintf("store: skill catalog: %d present entries, %d incoming: a publish may not retire most of a catalog", e.Present, e.Incoming)
}

// AliasConflictError is an alias another repository already holds, with no
// lineage between the two entries (design 4d, SECURITY2-9): one repository's
// publish cannot claim another's names.
type AliasConflictError struct{ Alias, HeldBy string }

func (e *AliasConflictError) Error() string {
	return "store: skill catalog: alias " + e.Alias + " is held by " + e.HeldBy
}

var (
	// sourceRepoShape is the source_repo CHECK, the same regex the route
	// applies to its path.
	sourceRepoShape = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,31}$`)
	// lineageShape is '<source_repo>/<plugin>:<skill>', the plugin possibly
	// empty.
	lineageShape = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,31}/([a-z0-9][a-z0-9_-]{0,62}[a-z0-9])?:[a-z0-9][a-z0-9_-]{0,62}[a-z0-9]$`)
)

// catalogTextMax is the text cap on every string field of the body.
const catalogTextMax = 200

// lineageKey is the string an entry's lineage_of names, as SQL renders it.
func lineageKey(alias string) string {
	return alias + ".source_repo || '/' || " + alias + ".plugin || ':' || " + alias + ".skill"
}

// LineageOf is the lineage key of an entry, as lineage_of names it.
func LineageOf(sourceRepo, plugin, skill string) string {
	return sourceRepo + "/" + plugin + ":" + skill
}

// SplitAlias splits an alias on its first colon into (alias_plugin,
// alias_skill); a bare name leaves alias_plugin empty.
func SplitAlias(alias string) (plugin, skill string) {
	if i := strings.Index(alias, ":"); i >= 0 {
		return alias[:i], alias[i+1:]
	}
	return "", alias
}

// checkCatalog re-applies the CHECKs to the body so a value the route let
// through is a field error rather than a statement error, and dedupes the
// entries on their key so the batch upsert never touches a row twice.
func checkCatalog(sourceRepo string, body CatalogBody) ([]CatalogSkill, error) {
	if !sourceRepoShape.MatchString(sourceRepo) || body.SourceRepo != sourceRepo {
		return nil, &CatalogFieldError{Field: "source_repo", Reason: "must match the path"}
	}
	if body.Commit == "" || len(body.Commit) > catalogTextMax {
		return nil, &CatalogFieldError{Field: "commit", Reason: "is required"}
	}
	if body.GeneratedAt.IsZero() {
		return nil, &CatalogFieldError{Field: "generated_at", Reason: "is required"}
	}
	seen := map[string]bool{}
	out := make([]CatalogSkill, 0, len(body.Skills))
	for i, sk := range body.Skills {
		field := func(k string) string { return fmt.Sprintf("skills[%d].%s", i, k) }
		if !slugShape.MatchString(sk.Dir) {
			return nil, &CatalogFieldError{Field: field("dir"), Reason: "must be a skill slug"}
		}
		if sk.Plugin != "" && !slugShape.MatchString(sk.Plugin) {
			return nil, &CatalogFieldError{Field: field("plugin"), Reason: "must be a plugin slug or empty"}
		}
		for _, t := range []struct{ k, v string }{{"slug", sk.Slug}, {"name", sk.Name}, {"path", sk.Path}, {"sha256_tree", sk.Sha256Tree}} {
			if len(t.v) > catalogTextMax {
				return nil, &CatalogFieldError{Field: field(t.k), Reason: "is over 200 characters"}
			}
		}
		if sk.SkipReason != nil && len(*sk.SkipReason) > catalogTextMax {
			return nil, &CatalogFieldError{Field: field("skip_reason"), Reason: "is over 200 characters"}
		}
		if !contains(AuthoredBys, sk.AuthoredBy) {
			return nil, &CatalogFieldError{Field: field("authored_by"), Reason: "must be one of " + strings.Join(AuthoredBys, ", ")}
		}
		if !contains(AuthorEvidences, sk.AuthorEvidence) {
			return nil, &CatalogFieldError{Field: field("author_evidence"), Reason: "must be one of '', frontmatter, git_first_commit"}
		}
		if sk.LineageOf != nil && (len(*sk.LineageOf) > catalogTextMax || !lineageShape.MatchString(*sk.LineageOf)) {
			return nil, &CatalogFieldError{Field: field("lineage_of"), Reason: "must be <source_repo>/<plugin>:<skill>"}
		}
		for _, a := range sk.Aliases {
			p, s := SplitAlias(a)
			if len(a) > catalogTextMax || !slugShape.MatchString(s) || (p != "" && !slugShape.MatchString(p)) {
				return nil, &CatalogFieldError{Field: field("aliases"), Reason: "must be plugin:skill or skill slugs"}
			}
		}
		key := sk.Plugin + ":" + sk.Dir
		if seen[key] {
			return nil, &CatalogFieldError{Field: field("dir"), Reason: "repeats an entry"}
		}
		seen[key] = true
		out = append(out, sk)
	}
	return out, nil
}

// catalogAlias is one alias row about to be written.
type catalogAlias struct {
	aliasPlugin, aliasSkill, plugin, skill string
}

// catalogAliases is the deduped union of every entry's aliases[] with its
// composite and bare forms, each split on the first colon, first entry
// wins: the generator refuses a bare name two plugins provide, so a repeat
// here is a body the generator did not write, and the insert's ON CONFLICT
// DO NOTHING is the second guard.
func catalogAliases(skills []CatalogSkill) []catalogAlias {
	seen := map[string]bool{}
	var out []catalogAlias
	add := func(name string, sk CatalogSkill) {
		p, s := SplitAlias(name)
		k := p + ":" + s
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, catalogAlias{aliasPlugin: p, aliasSkill: s, plugin: sk.Plugin, skill: sk.Dir})
	}
	for _, sk := range skills {
		if sk.Plugin != "" {
			add(sk.Plugin+":"+sk.Dir, sk)
		}
		add(sk.Dir, sk)
		for _, a := range sk.Aliases {
			add(a, sk)
		}
	}
	return out
}

// staleMirrorSQL is the stale_mirror expression for an entry aliased e
// (design 6.5, OPS3-6): true when the entry has a lineage head whose tree
// hash differs, or whose repository's latest publish is fourteen days ahead
// of this entry's repository's. A head is never stale.
func staleMirrorSQL(e string) string {
	return fmt.Sprintf(`EXISTS (SELECT 1 FROM skill_catalog_entries h
		WHERE %[1]s.lineage_of = %[2]s
		  AND (%[1]s.sha256_tree <> h.sha256_tree
		       OR coalesce((SELECT max(p.generated_at) FROM skill_catalog_publishes p WHERE p.source_repo = %[1]s.source_repo), '-infinity'::timestamptz)
		          < coalesce((SELECT max(p.generated_at) FROM skill_catalog_publishes p WHERE p.source_repo = h.source_repo), '-infinity'::timestamptz) - interval '14 days'))`,
		e, lineageKey("h"))
}

// catalogPublishLockKey serialises catalog publishes across instances, so
// the alias-conflict check and the insert it guards are one step. Distinct
// from the migration, retention and derive keys beside it.
const catalogPublishLockKey int64 = 7266794526548564

// PublishSkillCatalog records one publish in a transaction (design 4d): the
// (source_repo, commit) row first, and a commit already there is answered
// Duplicate with nothing else touched; then the entries upserted with
// first_seen_at kept, the repository's absent entries flipped present =
// false, every lineage_of checked against the table, the repository's
// aliases replaced by the body's, refused when another repository holds one
// with no lineage between the two entries, and the stale count read back.
// tokenID is the publisher's token, recorded on the publish row.
func (s *Store) PublishSkillCatalog(ctx context.Context, tokenID string, sourceRepo string, body CatalogBody) (PublishResult, error) {
	skills, err := checkCatalog(sourceRepo, body)
	if err != nil {
		return PublishResult{}, err
	}
	if tokenID != "" && !isUUID(tokenID) {
		return PublishResult{}, errors.New("store: publish skill catalog: the token id is not a uuid")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return PublishResult{}, fmt.Errorf("store: begin skill catalog publish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// One publish at a time across the fleet. The alias rule is read then
	// written: the conflict check below is a SELECT, and the aliases' key
	// carries source_repo, since a head and its mirror legitimately hold
	// one name, so nothing in the schema stops two repositories that
	// publish the same alias at the same moment from both passing the
	// check and both writing the row. That leaves an alias with two
	// unrelated holders, which doubles it in the 6.1 catalog join and
	// wedges both repositories' next publish on a 409 with no lineage to
	// resolve it. Publishes are a handful a day and the lock is held for
	// this transaction only, so serialising them costs nothing and no
	// reader ever waits on it.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, catalogPublishLockKey); err != nil {
		return PublishResult{}, fmt.Errorf("store: take the skill catalog publish lock: %w", err)
	}

	// The duplicate gate, with the one exception that makes a wipe
	// repairable: a stored row recording skills = 0 is not a publish worth
	// keeping, so re-running the last good CI job on the same sha is let
	// through and overwrites it. Without that, the obvious recovery from an
	// empty publish was answered duplicate: true and restored nothing, and
	// the only ways left were a fresh sha or DB surgery (adversarial
	// finding 1). The conditional DO UPDATE returns no row when the stored
	// row carries entries, which is the ordinary duplicate.
	var recorded bool
	if err := tx.QueryRow(ctx, `
		INSERT INTO skill_catalog_publishes AS p (source_repo, commit, generated_at, source_token_id, skills)
		VALUES ($1, $2, $3, $4::uuid, $5)
		ON CONFLICT (source_repo, commit) DO UPDATE SET
		  generated_at = EXCLUDED.generated_at, source_token_id = EXCLUDED.source_token_id, skills = EXCLUDED.skills
		WHERE p.skills = 0
		RETURNING true`, sourceRepo, body.Commit, body.GeneratedAt, nilEmpty(&tokenID), len(skills)).Scan(&recorded); err != nil {
		if noRows(err) {
			// The commit is already recorded: the transaction is rolled back
			// by the deferred call, so even the touch above never lands.
			return PublishResult{Duplicate: true}, nil
		}
		return PublishResult{}, fmt.Errorf("store: record skill catalog publish: %w", err)
	}

	n := len(skills)
	present, retiring, err := catalogShrink(ctx, tx, sourceRepo, skills)
	if err != nil {
		return PublishResult{}, err
	}
	// The shrink gate, read before anything is written and rolled back with
	// the rest on a refusal.
	if !body.AllowShrink && (n == 0 || retiring*2 > present) {
		return PublishResult{}, &CatalogShrinkError{Present: present, Incoming: n}
	}
	plugins, dirs, paths, hashes := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	installables, mirroreds := make([]bool, n), make([]bool, n)
	skips, lineages := make([]*string, n), make([]*string, n)
	authors, evidences := make([]string, n), make([]string, n)
	for i, sk := range skills {
		plugins[i], dirs[i], paths[i], hashes[i] = sk.Plugin, sk.Dir, sk.Path, sk.Sha256Tree
		installables[i], mirroreds[i] = sk.Installable, sk.Mirrored
		skips[i], lineages[i] = sk.SkipReason, sk.LineageOf
		authors[i], evidences[i] = sk.AuthoredBy, sk.AuthorEvidence
	}
	if n > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO skill_catalog_entries AS e
			  (source_repo, plugin, skill, path, sha256_tree, installable, mirrored, skip_reason, authored_by, author_evidence, lineage_of, present, last_seen_at, commit)
			SELECT $1, u.plugin, u.skill, u.path, u.sha256_tree, u.installable, u.mirrored, u.skip_reason, u.authored_by, u.author_evidence, u.lineage_of, true, now(), $2
			FROM unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::bool[], $8::bool[], $9::text[], $10::text[], $11::text[], $12::text[])
			  AS u(plugin, skill, path, sha256_tree, installable, mirrored, skip_reason, authored_by, author_evidence, lineage_of)
			ON CONFLICT (source_repo, plugin, skill) DO UPDATE SET
			  path = EXCLUDED.path, sha256_tree = EXCLUDED.sha256_tree, installable = EXCLUDED.installable,
			  mirrored = EXCLUDED.mirrored, skip_reason = EXCLUDED.skip_reason, authored_by = EXCLUDED.authored_by,
			  author_evidence = EXCLUDED.author_evidence, lineage_of = EXCLUDED.lineage_of,
			  present = true, last_seen_at = now(), commit = EXCLUDED.commit`,
			sourceRepo, body.Commit, plugins, dirs, paths, hashes, installables, mirroreds, skips, authors, evidences, lineages); err != nil {
			return PublishResult{}, fmt.Errorf("store: upsert skill catalog entries: %w", err)
		}
	}
	// Every entry the body carries now bears this commit, so the ones that
	// do not are the ones it left out.
	if _, err := tx.Exec(ctx, `
		UPDATE skill_catalog_entries SET present = false WHERE source_repo = $1 AND commit <> $2 AND present`,
		sourceRepo, body.Commit); err != nil {
		return PublishResult{}, fmt.Errorf("store: retire absent skill catalog entries: %w", err)
	}

	// Lineage: every head named must exist, in this body or before it.
	var named []string
	for _, l := range lineages {
		if l != nil {
			named = append(named, *l)
		}
	}
	if len(named) > 0 {
		var missing *string
		err := tx.QueryRow(ctx, `
			SELECT l FROM unnest($1::text[]) AS u(l)
			WHERE NOT EXISTS (SELECT 1 FROM skill_catalog_entries e WHERE `+lineageKey("e")+` = u.l)
			LIMIT 1`, named).Scan(&missing)
		switch {
		case err == nil && missing != nil:
			return PublishResult{}, &LineageError{Lineage: *missing}
		case err != nil && !noRows(err):
			return PublishResult{}, fmt.Errorf("store: check skill catalog lineage: %w", err)
		}
	}

	// Aliases: the repository's own are replaced; another repository's on
	// the same name is a conflict unless one entry's lineage names the other.
	aliases := catalogAliases(skills)
	if _, err := tx.Exec(ctx, `DELETE FROM skill_catalog_aliases WHERE source_repo = $1`, sourceRepo); err != nil {
		return PublishResult{}, fmt.Errorf("store: clear skill catalog aliases: %w", err)
	}
	if err := checkAliasConflicts(ctx, tx, sourceRepo, skills, aliases); err != nil {
		return PublishResult{}, err
	}
	var written int64
	if len(aliases) > 0 {
		ap, as, ep, es := make([]string, len(aliases)), make([]string, len(aliases)), make([]string, len(aliases)), make([]string, len(aliases))
		for i, a := range aliases {
			ap[i], as[i], ep[i], es[i] = a.aliasPlugin, a.aliasSkill, a.plugin, a.skill
		}
		written, err = tx.Exec(ctx, `
			INSERT INTO skill_catalog_aliases (alias_plugin, alias_skill, source_repo, plugin, skill)
			SELECT u.alias_plugin, u.alias_skill, $1, u.plugin, u.skill
			FROM unnest($2::text[], $3::text[], $4::text[], $5::text[]) AS u(alias_plugin, alias_skill, plugin, skill)
			ON CONFLICT DO NOTHING`, sourceRepo, ap, as, ep, es)
		if err != nil {
			return PublishResult{}, fmt.Errorf("store: write skill catalog aliases: %w", err)
		}
	}

	var stale int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM skill_catalog_entries e
		WHERE e.source_repo = $1 AND e.present AND `+staleMirrorSQL("e"), sourceRepo).Scan(&stale); err != nil {
		return PublishResult{}, fmt.Errorf("store: count stale skill catalog entries: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return PublishResult{}, fmt.Errorf("store: commit skill catalog publish: %w", err)
	}
	return PublishResult{Skills: n, Aliases: int(written), StaleEntries: stale, PresentBefore: present}, nil
}

// catalogShrink reads what this publish would cost the repository before a
// row moves: how many entries it holds present, and how many of those the
// body leaves out and would therefore retire. Both come from one statement
// per count on the entries' primary key, inside the publish transaction and
// under its lock, so the numbers the refusal reports are the ones the write
// would have acted on.
func catalogShrink(ctx context.Context, q Queryer, sourceRepo string, skills []CatalogSkill) (present, retiring int, err error) {
	plugins, dirs := make([]string, len(skills)), make([]string, len(skills))
	for i, sk := range skills {
		plugins[i], dirs[i] = sk.Plugin, sk.Dir
	}
	if err := q.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE NOT EXISTS (
		  SELECT 1 FROM unnest($2::text[], $3::text[]) AS u(plugin, skill)
		  WHERE (u.plugin, u.skill) = (e.plugin, e.skill)))
		FROM skill_catalog_entries e WHERE e.source_repo = $1 AND e.present`,
		sourceRepo, plugins, dirs).Scan(&present, &retiring); err != nil {
		return 0, 0, fmt.Errorf("store: measure the skill catalog publish: %w", err)
	}
	return present, retiring, nil
}

// checkAliasConflicts reads every alias of the body that another repository
// holds, with the lineage of the entry it points at, and refuses the first
// whose two entries are not in one lineage.
func checkAliasConflicts(ctx context.Context, q Queryer, sourceRepo string, skills []CatalogSkill, aliases []catalogAlias) error {
	if len(aliases) == 0 {
		return nil
	}
	ownLineage := map[string]*string{}
	for _, sk := range skills {
		ownLineage[sk.Plugin+":"+sk.Dir] = sk.LineageOf
	}
	target := map[string]catalogAlias{}
	ap, as := make([]string, len(aliases)), make([]string, len(aliases))
	for i, a := range aliases {
		ap[i], as[i] = a.aliasPlugin, a.aliasSkill
		target[a.aliasPlugin+":"+a.aliasSkill] = a
	}
	rows, err := q.Query(ctx, `
		SELECT a.alias_plugin, a.alias_skill, a.source_repo, a.plugin, a.skill, e.lineage_of
		FROM skill_catalog_aliases a
		JOIN skill_catalog_entries e ON (e.source_repo, e.plugin, e.skill) = (a.source_repo, a.plugin, a.skill)
		WHERE a.source_repo <> $1 AND (a.alias_plugin, a.alias_skill) IN (SELECT * FROM unnest($2::text[], $3::text[]))
		ORDER BY a.alias_plugin, a.alias_skill, a.source_repo`,
		sourceRepo, ap, as)
	if err != nil {
		return fmt.Errorf("store: read skill catalog alias holders: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			aliasPlugin, aliasSkill, repo, plugin, skill string
			theirLineage                                 *string
		)
		if err := rows.Scan(&aliasPlugin, &aliasSkill, &repo, &plugin, &skill, &theirLineage); err != nil {
			return fmt.Errorf("store: scan skill catalog alias holder: %w", err)
		}
		ours := target[aliasPlugin+":"+aliasSkill]
		ourKey := LineageOf(sourceRepo, ours.plugin, ours.skill)
		theirKey := LineageOf(repo, plugin, skill)
		ourLineage := ownLineage[ours.plugin+":"+ours.skill]
		related := (ourLineage != nil && *ourLineage == theirKey) || (theirLineage != nil && *theirLineage == ourKey)
		if !related {
			alias := aliasSkill
			if aliasPlugin != "" {
				alias = aliasPlugin + ":" + aliasSkill
			}
			return &AliasConflictError{Alias: alias, HeldBy: repo}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: read skill catalog alias holders: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- reconciler runs

// ReconcilerRun is one run as the route hands it to the store: the platform
// is the token row's, never the body's (CB-11), and the key is the run's own
// (a Devin run date, a Vorflux tick id).
type ReconcilerRun struct {
	AgentPlatform, IdempotencyKey string
	WindowStart, WindowEnd        time.Time
	SessionsScanned               int
	SessionsWithEvents            int
	RowsPosted, Truncated         int
}

// lineReconcilerRun is the 7.1 line the run writes; the mismatch metric
// filters on it exactly, so the string comes from internal/skilllog rather
// than a second literal here.
const lineReconcilerRun = skilllog.ReconcilerRun

// RecordReconcilerRun inserts the run under UNIQUE (agent_platform,
// idempotency_key) (design 3.7). A retry whose counts equal the stored
// row's is a duplicate; one whose counts differ is a conflict, which the
// route answers 409 and never absorbs silently, since a hostile holder
// could otherwise blank a real run. Every recorded run, first or retried,
// writes the "skill reconciler run" line: rows_seen is what the PLATFORM
// posted FOR the run's own window, and mismatch, a WARNING, is that count
// differing from rows_posted on a first record (5.4).
func (s *Store) RecordReconcilerRun(ctx context.Context, tokenID string, r ReconcilerRun) (duplicate bool, conflict bool, err error) {
	if !isUUID(tokenID) {
		return false, false, errors.New("store: record reconciler run: the token id is not a uuid")
	}
	if !contains(ReconcilerPlatforms, r.AgentPlatform) {
		return false, false, fmt.Errorf("store: record reconciler run: %q is not a reconciler platform", r.AgentPlatform)
	}
	var id int64
	err = s.db.QueryRow(ctx, `
		INSERT INTO reconciler_runs (source_token_id, agent_platform, idempotency_key, window_start, window_end, sessions_scanned, sessions_with_events, rows_posted, truncated)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (agent_platform, idempotency_key) DO NOTHING
		RETURNING id`,
		tokenID, r.AgentPlatform, r.IdempotencyKey, r.WindowStart, r.WindowEnd, r.SessionsScanned, r.SessionsWithEvents, r.RowsPosted, r.Truncated,
	).Scan(&id)
	switch {
	case err == nil:
	case noRows(err):
		var stored ReconcilerRun
		if err := s.db.QueryRow(ctx, `
			SELECT id, sessions_scanned, sessions_with_events, rows_posted, truncated
			FROM reconciler_runs WHERE agent_platform = $1 AND idempotency_key = $2`, r.AgentPlatform, r.IdempotencyKey,
		).Scan(&id, &stored.SessionsScanned, &stored.SessionsWithEvents, &stored.RowsPosted, &stored.Truncated); err != nil {
			return false, false, fmt.Errorf("store: read the stored reconciler run: %w", err)
		}
		if stored.SessionsScanned != r.SessionsScanned || stored.SessionsWithEvents != r.SessionsWithEvents ||
			stored.RowsPosted != r.RowsPosted || stored.Truncated != r.Truncated {
			return false, true, nil
		}
		duplicate = true
	default:
		return false, false, fmt.Errorf("store: record reconciler run: %w", err)
	}

	// rows_seen is scoped by the run's PLATFORM, not by the caller's token.
	// 0023 advertises the case that made the difference: "a run retried
	// under a rotated token lands on the row the compliance report ...
	// read". Counting by the caller's token answered that retry 0 rows
	// against a real rows_posted, so mismatch went true, and
	// skill_reconciler_mismatch with policy 17's fourth condition paged the
	// on-call for a routine token rotation (adversarial finding 4).
	//
	// It is counted over the run's OWN window by occurred_at, which is what
	// the reconciler counted, and not by arrival between the previous run
	// and this one. received_at never moves on the idempotent upsert, so a
	// row the platform re-posted arrived once, long before the window it
	// belongs to was reconciled; an arrival window therefore read a real
	// re-post as zero rows against a real rows_posted and paged for it
	// (adversarial iteration 5, finding 5). The bounds are inclusive at
	// both ends, the closed interval the run reports and the route already
	// refuses to invert.
	var seen int64
	if err := s.db.QueryRow(ctx, `
		SELECT count(*) FROM skill_invocations
		WHERE agent_platform = $1 AND origin = 'reconciler'
		  AND occurred_at >= $2 AND occurred_at <= $3`, r.AgentPlatform, r.WindowStart, r.WindowEnd).Scan(&seen); err != nil {
		return false, false, fmt.Errorf("store: count the rows a reconciler run posted: %w", err)
	}
	// A duplicate stored nothing, so there is nothing to disagree about:
	// only a first record can mismatch. Without this a retry of a run whose
	// rows had since been superseded alarmed on a post that changed no
	// state.
	mismatch := !duplicate && seen != int64(r.RowsPosted)
	attrs := reconcilerRunAttrs(tokenID, r, seen, mismatch, duplicate, false)
	if mismatch {
		s.logger().WarnContext(ctx, lineReconcilerRun, attrs...)
	} else {
		s.logger().InfoContext(ctx, lineReconcilerRun, attrs...)
	}
	return duplicate, false, nil
}

// LogSoftRevokedReconcilerRun writes the run's 7.1 line for a post on a
// limit-0 token, which records nothing: rows_seen stays 0 and mismatch
// false, because no run was stored and no window of rows belongs to it, so
// the line can never trip policy 17's fourth condition. Without it a
// reconciler still posting on a rotated token would be answered a duplicate
// in silence, with no line for an operator to find it by (review-1 finding
// 3); the invocation route's accepted line carries soft_revoked for the
// same reason.
func (s *Store) LogSoftRevokedReconcilerRun(ctx context.Context, tokenID string, r ReconcilerRun) {
	s.logger().InfoContext(ctx, lineReconcilerRun, reconcilerRunAttrs(tokenID, r, 0, false, true, true)...)
}

// reconcilerRunAttrs is the line's field set, shared so the recorded run
// and the soft-revoked one cannot drift apart: a filter on soft_revoked
// only works if every line carries the key.
func reconcilerRunAttrs(tokenID string, r ReconcilerRun, seen int64, mismatch, duplicate, softRevoked bool) []any {
	return []any{
		slog.String("platform", r.AgentPlatform),
		slog.String("source_token_id", tokenID),
		slog.String("idempotency_key", r.IdempotencyKey),
		slog.Bool("mismatch", mismatch),
		slog.Int("rows_posted", r.RowsPosted),
		slog.Int64("rows_seen", seen),
		slog.Int("sessions_scanned", r.SessionsScanned),
		slog.Int("truncated", r.Truncated),
		slog.Bool("duplicate", duplicate),
		slog.Bool("soft_revoked", softRevoked),
	}
}
