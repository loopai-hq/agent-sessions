package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/links"
)

// Artifacts and links are derived from events already stored, in the same
// transaction that stores them.
//
// The alternative — deriving at read time by scanning event bodies — was
// rejected for one reason: the questions worth asking are cross-session ("every
// session that touched this file", "every PR opened this week"), and those
// cannot be answered by a scan scoped to the session you are already looking at.
// Per-session panels would have been fine either way.

// Artifact is a file a session created or changed.
type Artifact struct {
	ID           int64     `json:"id"`
	SessionID    string    `json:"session_id"`
	Email        string    `json:"email"`
	Path         string    `json:"path"`
	Created      bool      `json:"created"`
	FirstSeen    time.Time `json:"first_seen_at"`
	LastSeen     time.Time `json:"last_seen_at"`
	VersionCount int       `json:"version_count"`
	LatestSHA    string    `json:"latest_sha256"`
	LatestBytes  int64     `json:"latest_bytes"`
}

// ArtifactVersion is one content state a file passed through.
type ArtifactVersion struct {
	ArtifactID int64     `json:"artifact_id"`
	EventID    string    `json:"event_id"`
	SHA256     string    `json:"sha256"`
	Bytes      int64     `json:"bytes"`
	Created    bool      `json:"created"`
	OccurredAt time.Time `json:"occurred_at"`
}

// Link is a URL a session mentioned, classified.
type Link struct {
	SessionID   string    `json:"session_id"`
	URL         string    `json:"url"`
	Kind        string    `json:"kind"`
	Host        string    `json:"host"`
	Ref         string    `json:"ref,omitempty"`
	FirstSeen   time.Time `json:"first_seen_at"`
	LastSeen    time.Time `json:"last_seen_at"`
	Occurrences int       `json:"occurrences"`
}

// insertArtifacts records the file changes in this batch.
//
// Ordering is by event time, not by batch order: a batch is whatever the client
// happened to have queued, and applying a stale edit after a fresh one would
// leave latest_sha256 describing a version that is not the latest. Within the
// same instant, seq breaks the tie, which is the same order the transcript had.
func insertArtifacts(ctx context.Context, q Queryer, items []Ingest) error {
	changes := make([]Ingest, 0, len(items))
	for _, it := range items {
		if it.Event.Type != event.FileChanged {
			continue
		}
		if it.Event.Tool == nil || it.Event.Tool.Diff == nil {
			continue
		}
		if strings.TrimSpace(it.Event.Tool.Diff.Path) == "" {
			continue
		}
		changes = append(changes, it)
	}
	if len(changes) == 0 {
		return nil
	}
	sort.SliceStable(changes, func(i, j int) bool {
		a, b := changes[i].Event, changes[j].Event
		if !a.OccurredAt.Equal(b.OccurredAt) {
			return a.OccurredAt.Before(b.OccurredAt)
		}
		return a.Seq < b.Seq
	})

	for _, it := range changes {
		if err := applyArtifact(ctx, q, it); err != nil {
			return err
		}
	}
	return nil
}

// snapshot returns the complete file text a diff carries, and reports whether it
// found one.
//
// Which field holds it depends on the tool, and getting this wrong is silent:
//
//	Write/create   Before is empty,          After is the whole new file
//	Edit           Before is the whole file, After is only the replacement text
//
// So After is the full file exactly when there is no Before. Taking After
// unconditionally — which is the obvious reading of a field called "after" —
// yields the size of an edit fragment presented as a file size, a viewer that
// shows a snippet while claiming to show the file, and a checksum computed over
// the replacement text, which would collapse two identical replacements made in
// different places into one version.
//
// The states this produces line up chronologically: a create gives the file just
// after it was written, and each later edit gives the file just before it. The
// state before edit N+1 is the state after edit N, so the sequence is the file's
// history with one gap — the state after the FINAL edit is not recoverable from
// what is stored, because nothing later recorded it.
func snapshot(d *event.Diff) (string, bool) {
	if d.Before != "" {
		return d.Before, true
	}
	if d.After != "" {
		return d.After, true
	}
	return "", false
}

func applyArtifact(ctx context.Context, q Queryer, it Ingest) error {
	d := it.Event.Tool.Diff
	text, ok := snapshot(d)
	if !ok {
		// A change that recorded neither side of itself says nothing about the
		// file's contents. It is still a real event; it is just not a version.
		return nil
	}
	sum := sha256.Sum256([]byte(text))
	digest := hex.EncodeToString(sum[:])
	bytes := int64(len(text))

	var (
		id       int64
		prevSHA  string
		lastSeen time.Time
	)
	err := q.QueryRow(ctx, `
		INSERT INTO artifacts (session_id, email, path, created,
		                       first_seen_at, last_seen_at, version_count, latest_sha256, latest_bytes)
		VALUES ($1, $2, $3, $4, $5, $5, 0, '', 0)
		ON CONFLICT (session_id, path) DO UPDATE SET
			first_seen_at = LEAST(artifacts.first_seen_at, EXCLUDED.first_seen_at),
			-- Created is sticky: a session that made the file made it, whatever
			-- later edits say about themselves.
			created = artifacts.created OR EXCLUDED.created
		RETURNING id, latest_sha256, last_seen_at`,
		it.Event.SessionID, it.Email, d.Path, d.Created, it.Event.OccurredAt,
	).Scan(&id, &prevSHA, &lastSeen)
	if err != nil {
		return fmt.Errorf("store: record artifact %q: %w", d.Path, err)
	}

	// The checksum is what makes this a history of changes rather than a log of
	// writes. A tool that rewrites a file with identical content — a formatter
	// that changed nothing, a retried edit — is a real event and stays in the
	// event log, but it is not a new version of the file and must not appear as
	// one in a version list somebody is reading to understand what happened.
	if digest != prevSHA {
		if _, err := q.Exec(ctx, `
			INSERT INTO artifact_versions (artifact_id, event_id, sha256, bytes, created, occurred_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (artifact_id, event_id) DO NOTHING`,
			id, it.Event.ID, digest, bytes, d.Created, it.Event.OccurredAt,
		); err != nil {
			return fmt.Errorf("store: record artifact version for %q: %w", d.Path, err)
		}
	}

	// version_count is recounted rather than incremented so it is self-healing:
	// an incremented counter that ever diverges stays wrong forever, and this
	// runs at most once per file change.
	//
	// The CASE guards are what make out-of-order arrival safe. An event older
	// than what we already have advances neither the latest checksum nor
	// last_seen_at, so a late-delivered edit from a dead session cannot make the
	// page claim the file's current contents are last week's.
	if _, err := q.Exec(ctx, `
		UPDATE artifacts SET
			version_count = (SELECT count(*) FROM artifact_versions WHERE artifact_id = $1),
			latest_sha256 = CASE WHEN $2 >= last_seen_at THEN $3 ELSE latest_sha256 END,
			latest_bytes  = CASE WHEN $2 >= last_seen_at THEN $4 ELSE latest_bytes  END,
			last_seen_at  = GREATEST(last_seen_at, $2)
		WHERE id = $1`,
		id, it.Event.OccurredAt, digest, bytes,
	); err != nil {
		return fmt.Errorf("store: update artifact %q: %w", d.Path, err)
	}
	return nil
}

// insertLinks records the URLs this batch mentioned.
//
// Every event with text is scanned, not just the ones that become messages: a
// tool result that printed a PR URL is exactly as good a record of that PR as an
// assistant turn describing it, and restricting this to message roles would drop
// the `gh pr create` output that names the URL in the first place.
func insertLinks(ctx context.Context, q Queryer, items []Ingest) error {
	type agg struct {
		l           links.Link
		email       string
		first, last time.Time
		n           int
	}
	// Keyed by session and URL so a batch mentioning one PR forty times is one
	// row with occurrences=40 rather than forty round trips.
	found := map[string]*agg{}
	var order []string

	for _, it := range items {
		if it.Event.Text == "" {
			continue
		}
		for _, l := range links.Extract(it.Event.Text) {
			key := it.Event.SessionID + "\x00" + l.URL
			a, ok := found[key]
			if !ok {
				a = &agg{l: l, email: it.Email, first: it.Event.OccurredAt, last: it.Event.OccurredAt}
				found[key] = a
				order = append(order, key)
			}
			a.n++
			if it.Event.OccurredAt.Before(a.first) {
				a.first = it.Event.OccurredAt
			}
			if it.Event.OccurredAt.After(a.last) {
				a.last = it.Event.OccurredAt
			}
		}
	}
	if len(order) == 0 {
		return nil
	}

	var (
		sessionIDs, emails, urls, kinds, hosts, refs []string
		firsts, lasts                                []time.Time
		counts                                       []int32
	)
	for _, key := range order {
		a := found[key]
		sessionIDs = append(sessionIDs, key[:strings.IndexByte(key, 0)])
		emails = append(emails, a.email)
		urls = append(urls, a.l.URL)
		kinds = append(kinds, string(a.l.Kind))
		hosts = append(hosts, a.l.Host)
		refs = append(refs, a.l.Ref)
		firsts = append(firsts, a.first)
		lasts = append(lasts, a.last)
		counts = append(counts, int32(a.n))
	}

	// Occurrences accumulate across batches because this only ever runs on
	// freshly inserted events. A re-uploaded batch inserts no events, so it
	// reaches here with nothing and cannot inflate the count — which is the
	// property that makes the number mean "times mentioned" rather than "times
	// the client retried".
	_, err := q.Exec(ctx, `
		INSERT INTO links (session_id, email, url, kind, host, ref,
		                   first_seen_at, last_seen_at, occurrences)
		SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[],
		                     $6::text[], $7::timestamptz[], $8::timestamptz[], $9::int[])
		ON CONFLICT (session_id, url) DO UPDATE SET
			first_seen_at = LEAST(links.first_seen_at, EXCLUDED.first_seen_at),
			last_seen_at  = GREATEST(links.last_seen_at, EXCLUDED.last_seen_at),
			occurrences   = links.occurrences + EXCLUDED.occurrences`,
		sessionIDs, emails, urls, kinds, hosts, refs, firsts, lasts, counts)
	if err != nil {
		return fmt.Errorf("store: insert links: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- derivation version

// DerivedSchema is the version of the server-side derivation.
//
// Bump it when a change makes the server derive something BETTER or DIFFERENT
// from events that are already stored — a new derived table, a changed
// classification rule, a corrected checksum. The derive runner (RunDerive,
// runner.go) notices the gap and works through every step of the new version
// in cursor-committed batches, so the rebuild survives a deploy mid-way, runs
// on one instance at a time, and never reads the corpus into memory.
//
// Do NOT bump it for read-path, template, routing or auth changes. Those render
// what is already derived; they do not change it, and a rebuild would be minutes
// of work to produce byte-identical rows.
//
// Version history:
//
//	1  artifacts, links, and messages, with a version's text taken from the
//	   field that holds the whole file
//	2  session_type reclassification joins the rebuild, so a capture re-walk
//	   that lands entrypoint evidence on old events also corrects the
//	   sessions those events had already misclassified
//	3  the runner replaces the boot-time rebuild: the key columns 0017 added
//	   are filled from stored bodies, message kinds and titles are
//	   recomputed, turns and turn_events are folded and the hook copies they
//	   elect out are marked, the five counters are rolled up from turns, the
//	   session lattice, head state and lineage are applied to history, and
//	   artifacts and links are rebuilt one session at a time
//	4  skill invocations: every Skill tool_call and typed slash command in
//	   the stored events becomes a skill_invocations row (skills.go), so the
//	   usage history the /skills reports read starts with the corpus rather
//	   than with the deploy. The step carries since 4, so a stored 3 runs it
//	   alone rather than the thirteen steps before it again (runner.go)
const DerivedSchema = 4

// ---------------------------------------------------------------- reads

// SessionArtifacts lists the files a session changed, most recently touched
// first. Authorization goes through the same gate as reading the session, so an
// artifact can never be more visible than the transcript it came from.
func (s *Store) SessionArtifacts(ctx context.Context, v Viewer, sessionID string) ([]Artifact, error) {
	if _, err := authorizeSession(ctx, s.db, v, sessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, `
		SELECT id, session_id, email, path, created, first_seen_at, last_seen_at,
		       version_count, latest_sha256, latest_bytes
		FROM artifacts WHERE session_id = $1
		ORDER BY last_seen_at DESC, path`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("store: list artifacts: %w", err)
	}
	defer rows.Close()

	out := []Artifact{}
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.ID, &a.SessionID, &a.Email, &a.Path, &a.Created,
			&a.FirstSeen, &a.LastSeen, &a.VersionCount, &a.LatestSHA, &a.LatestBytes); err != nil {
			return nil, fmt.Errorf("store: scan artifact: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// SessionLinks lists the URLs a session mentioned. PRs first, then issues and
// commits, then everything else: the ordering is by how likely somebody is to
// want it, not alphabetical.
func (s *Store) SessionLinks(ctx context.Context, v Viewer, sessionID string) ([]Link, error) {
	if _, err := authorizeSession(ctx, s.db, v, sessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, `
		SELECT session_id, url, kind, host, ref, first_seen_at, last_seen_at, occurrences
		FROM links WHERE session_id = $1
		ORDER BY CASE kind
			WHEN 'pr' THEN 0 WHEN 'issue' THEN 1 WHEN 'commit' THEN 2
			WHEN 'repo' THEN 3 WHEN 'doc' THEN 4 WHEN 'slack' THEN 5 ELSE 6 END,
			first_seen_at, url`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("store: list links: %w", err)
	}
	defer rows.Close()

	out := []Link{}
	for rows.Next() {
		var l Link
		if err := rows.Scan(&l.SessionID, &l.URL, &l.Kind, &l.Host, &l.Ref,
			&l.FirstSeen, &l.LastSeen, &l.Occurrences); err != nil {
			return nil, fmt.Errorf("store: scan link: %w", err)
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// Artifact returns one artifact and its whole version history, newest first.
func (s *Store) Artifact(ctx context.Context, v Viewer, id int64) (Artifact, []ArtifactVersion, error) {
	var a Artifact
	err := s.db.QueryRow(ctx, `
		SELECT id, session_id, email, path, created, first_seen_at, last_seen_at,
		       version_count, latest_sha256, latest_bytes
		FROM artifacts WHERE id = $1`, id).
		Scan(&a.ID, &a.SessionID, &a.Email, &a.Path, &a.Created, &a.FirstSeen, &a.LastSeen,
			&a.VersionCount, &a.LatestSHA, &a.LatestBytes)
	if err != nil {
		return Artifact{}, nil, ErrNotFound
	}
	// Authorized against the artifact's session rather than its email, so a share
	// grants the artifact exactly when it grants the transcript.
	if _, err := authorizeSession(ctx, s.db, v, a.SessionID); err != nil {
		return Artifact{}, nil, err
	}

	rows, err := s.db.Query(ctx, `
		SELECT artifact_id, event_id, sha256, bytes, created, occurred_at
		FROM artifact_versions WHERE artifact_id = $1
		ORDER BY occurred_at DESC, event_id DESC`, id)
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("store: list artifact versions: %w", err)
	}
	defer rows.Close()

	versions := []ArtifactVersion{}
	for rows.Next() {
		var av ArtifactVersion
		if err := rows.Scan(&av.ArtifactID, &av.EventID, &av.SHA256, &av.Bytes,
			&av.Created, &av.OccurredAt); err != nil {
			return Artifact{}, nil, fmt.Errorf("store: scan artifact version: %w", err)
		}
		versions = append(versions, av)
	}
	return a, versions, rows.Err()
}

// ArtifactContent returns the file as it stood at one version.
//
// The text is read back out of the event body rather than copied into
// artifact_versions. File contents are the largest thing in this database and
// storing them twice to save a join would be the single most expensive decision
// available here.
func (s *Store) ArtifactContent(ctx context.Context, v Viewer, id int64, eventID string) (string, error) {
	var sessionID string
	if err := s.db.QueryRow(ctx,
		`SELECT session_id FROM artifacts WHERE id = $1`, id).Scan(&sessionID); err != nil {
		return "", ErrNotFound
	}
	if _, err := authorizeSession(ctx, s.db, v, sessionID); err != nil {
		return "", err
	}

	// The same rule snapshot() applies at write time, expressed in SQL: the full
	// file is Before when there is one, and After only when there is not. Reading
	// After unconditionally here would render an edit's replacement fragment as
	// though it were the file.
	var body string
	err := s.db.QueryRow(ctx, `
		SELECT CASE
		         WHEN coalesce(e.body->'tool'->'diff'->>'before', '') <> ''
		           THEN e.body->'tool'->'diff'->>'before'
		         ELSE coalesce(e.body->'tool'->'diff'->>'after', '')
		       END
		FROM artifact_versions av JOIN events e ON e.id = av.event_id
		WHERE av.artifact_id = $1 AND av.event_id = $2`, id, eventID).Scan(&body)
	if err != nil {
		return "", ErrNotFound
	}
	return body, nil
}

// PathHistory lists every session that changed a given path, newest first. This
// is the cross-session question the per-session rows exist to make answerable.
func (s *Store) PathHistory(ctx context.Context, v Viewer, path string, limit int) ([]Artifact, error) {
	rows, err := s.db.Query(ctx, `
		SELECT a.id, a.session_id, a.email, a.path, a.created, a.first_seen_at, a.last_seen_at,
		       a.version_count, a.latest_sha256, a.latest_bytes
		FROM artifacts a JOIN sessions s ON s.session_id = a.session_id
		WHERE a.path = $1 AND `+canRead("s", "$2::bool", "$3")+`
		ORDER BY a.last_seen_at DESC
		LIMIT $4`, path, v.IsAdmin(), v.Email, clampLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("store: path history: %w", err)
	}
	defer rows.Close()

	out := []Artifact{}
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.ID, &a.SessionID, &a.Email, &a.Path, &a.Created,
			&a.FirstSeen, &a.LastSeen, &a.VersionCount, &a.LatestSHA, &a.LatestBytes); err != nil {
			return nil, fmt.Errorf("store: scan path history: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- per-session rebuild

// rebuildSessionArtifacts derives one session's artifacts again from its
// stored file changes, inside the caller's transaction: the session's rows
// are deleted (artifact_versions goes by cascade) and rebuilt through the
// same insertArtifacts the live path calls, so the two cannot drift. The
// table is empty for this one session and for the length of the batch, never
// for anyone else, which is what the runner's per-session step exists to
// guarantee; the old rebuild emptied the whole table and read the whole
// corpus into memory first.
//
// A session with an expired body among its file changes is left as it is.
// The derived rows describe what those bodies said before retention replaced
// them, and a rebuild from the stubs would only erase that. It reports the
// file changes it replayed.
func rebuildSessionArtifacts(ctx context.Context, q Queryer, sessionID string) (int, error) {
	var expired bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM events
		               WHERE session_id = $1 AND type = 'file_changed' AND body_expired_at IS NOT NULL)`,
		sessionID).Scan(&expired); err != nil {
		return 0, fmt.Errorf("store: probe expired file changes of %s: %w", sessionID, err)
	}
	if expired {
		return 0, nil
	}
	// Both sides are read, because which one holds the complete file depends
	// on the tool. See snapshot().
	rows, err := q.Query(ctx, `
		SELECT id, email, seq, occurred_at,
		       coalesce(body->'tool'->'diff'->>'path', ''),
		       coalesce(body->'tool'->'diff'->>'before', ''),
		       coalesce(body->'tool'->'diff'->>'after', ''),
		       coalesce((body->'tool'->'diff'->>'created')::bool, false)
		FROM events
		WHERE session_id = $1 AND type = 'file_changed' AND body->'tool'->'diff'->>'path' IS NOT NULL
		ORDER BY occurred_at, seq`, sessionID)
	if err != nil {
		return 0, fmt.Errorf("store: scan file changes of %s: %w", sessionID, err)
	}
	var changes []Ingest
	for rows.Next() {
		var (
			it                  Ingest
			path, before, after string
			created             bool
			id, eml             string
			seq                 int64
			at                  time.Time
		)
		if err := rows.Scan(&id, &eml, &seq, &at, &path, &before, &after, &created); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: scan file change: %w", err)
		}
		it.Email = eml
		it.Event.ID, it.Event.SessionID, it.Event.Seq = id, sessionID, seq
		it.Event.Type, it.Event.OccurredAt = event.FileChanged, at
		it.Event.Tool = &event.Tool{Diff: &event.Diff{
			Path: path, Before: before, After: after, Created: created,
		}}
		changes = append(changes, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: scan file changes of %s: %w", sessionID, err)
	}
	if _, err := q.Exec(ctx, `DELETE FROM artifacts WHERE session_id = $1`, sessionID); err != nil {
		return 0, fmt.Errorf("store: clear artifacts of %s: %w", sessionID, err)
	}
	if err := insertArtifacts(ctx, q, changes); err != nil {
		return 0, err
	}
	return len(changes), nil
}

// rebuildSessionLinks derives one session's links again from every stored
// event of it that carries text, inside the caller's transaction, through
// the same insertLinks the live path calls. The whole session goes to one
// call, so the occurrence counts are recomputed from every mention the
// session holds rather than accumulated batch by batch. Left as it is when
// any text-bearing body has expired, for the reason rebuildSessionArtifacts
// gives. It reports the text rows it scanned.
func rebuildSessionLinks(ctx context.Context, q Queryer, sessionID string) (int, error) {
	var expired bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM events WHERE session_id = $1 AND body_expired_at IS NOT NULL)`,
		sessionID).Scan(&expired); err != nil {
		return 0, fmt.Errorf("store: probe expired bodies of %s: %w", sessionID, err)
	}
	if expired {
		return 0, nil
	}
	rows, err := q.Query(ctx, `
		SELECT id, email, seq, occurred_at, body->>'text'
		FROM events
		WHERE session_id = $1 AND body ? 'text' AND body->>'text' <> ''
		ORDER BY occurred_at, seq`, sessionID)
	if err != nil {
		return 0, fmt.Errorf("store: scan event text of %s: %w", sessionID, err)
	}
	var texts []Ingest
	for rows.Next() {
		var (
			it           Ingest
			id, eml, txt string
			seq          int64
			at           time.Time
		)
		if err := rows.Scan(&id, &eml, &seq, &at, &txt); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: scan event text: %w", err)
		}
		it.Email = eml
		it.Event.ID, it.Event.SessionID, it.Event.Seq = id, sessionID, seq
		it.Event.OccurredAt, it.Event.Text = at, txt
		texts = append(texts, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: scan event text of %s: %w", sessionID, err)
	}
	if _, err := q.Exec(ctx, `DELETE FROM links WHERE session_id = $1`, sessionID); err != nil {
		return 0, fmt.Errorf("store: clear links of %s: %w", sessionID, err)
	}
	if err := insertLinks(ctx, q, texts); err != nil {
		return 0, err
	}
	return len(texts), nil
}
