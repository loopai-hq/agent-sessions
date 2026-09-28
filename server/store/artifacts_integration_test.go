//go:build integration

package store

// Artifact and link derivation is mostly SQL — an upsert with LEAST/GREATEST
// merge expressions, a conditional that guards against out-of-order arrival, and
// a unique index doing deduplication. None of that is exercised by a fake, so it
// is tested here against a real Postgres.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
)

// fileChange builds a file_changed event carrying a diff, which is the only
// shape artifacts are derived from.
func fileChange(id, sessionID, email, path, after string, seq int64, at time.Time, created bool) Ingest {
	it := ingestOf(id, sessionID, email, event.FileChanged, seq)
	it.Event.OccurredAt = at
	it.Event.Tool = &event.Tool{
		Name: "Edit",
		Diff: &event.Diff{Path: path, After: after, Created: created},
	}
	return it
}

func artifactsOf(t *testing.T, s *Store, email, sessionID string) []Artifact {
	t.Helper()
	arts, err := s.SessionArtifacts(context.Background(), Viewer{Email: email, Role: RoleMember}, sessionID)
	if err != nil {
		t.Fatalf("SessionArtifacts: %v", err)
	}
	return arts
}

// The basic claim: a file edited twice is one artifact with two versions, and
// the artifact reports the newest content.
func TestIntegrationAFileEditedTwiceIsOneArtifactWithTwoVersions(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-1"
	mustPrincipal(t, s, email, RoleMember)

	base := time.Now().UTC().Truncate(time.Second)
	batch := append(session(t, email, sid),
		fileChange("fc-1", sid, email, "app/main.go", "one", 10, base, true),
		fileChange("fc-2", sid, email, "app/main.go", "two", 11, base.Add(time.Minute), false),
	)
	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	arts := artifactsOf(t, s, email, sid)
	if len(arts) != 1 {
		t.Fatalf("got %d artifacts, want 1: %+v", len(arts), arts)
	}
	a := arts[0]
	if a.Path != "app/main.go" {
		t.Errorf("path = %q", a.Path)
	}
	if a.VersionCount != 2 {
		t.Errorf("version_count = %d, want 2", a.VersionCount)
	}
	if !a.Created {
		t.Error("created is false, but this session brought the file into existence")
	}
	if a.LatestBytes != int64(len("two")) {
		t.Errorf("latest_bytes = %d, want %d", a.LatestBytes, len("two"))
	}

	_, versions, err := s.Artifact(context.Background(), Viewer{Email: email, Role: RoleMember}, a.ID)
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("got %d versions, want 2", len(versions))
	}
	// Newest first, which is the order the page renders.
	if !versions[0].OccurredAt.After(versions[1].OccurredAt) {
		t.Error("versions are not newest-first")
	}

	body, err := s.ArtifactContent(context.Background(), Viewer{Email: email, Role: RoleMember}, a.ID, versions[0].EventID)
	if err != nil {
		t.Fatalf("ArtifactContent: %v", err)
	}
	if body != "two" {
		t.Errorf("newest content = %q, want %q", body, "two")
	}
}

// The checksum is what makes this a history of changes rather than a log of
// writes. A formatter that rewrote a file without changing it is a real event
// and must stay in the event log, but it is not a version.
func TestIntegrationRewritingAFileWithIdenticalContentIsNotANewVersion(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-2"
	mustPrincipal(t, s, email, RoleMember)

	base := time.Now().UTC().Truncate(time.Second)
	batch := append(session(t, email, sid),
		fileChange("fc-1", sid, email, "a.go", "same", 10, base, true),
		fileChange("fc-2", sid, email, "a.go", "same", 11, base.Add(time.Minute), false),
		fileChange("fc-3", sid, email, "a.go", "different", 12, base.Add(2*time.Minute), false),
	)
	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	arts := artifactsOf(t, s, email, sid)
	if len(arts) != 1 {
		t.Fatalf("got %d artifacts, want 1", len(arts))
	}
	if arts[0].VersionCount != 2 {
		t.Errorf("version_count = %d, want 2 (three writes, two contents)", arts[0].VersionCount)
	}
}

// A batch is whatever the client had queued, and a spool drains in whatever
// order recovery produced. An older edit arriving after a newer one must not
// make the page claim the file's current contents are the stale ones.
func TestIntegrationAStaleEditArrivingLateDoesNotBecomeTheLatest(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-3"
	mustPrincipal(t, s, email, RoleMember)

	base := time.Now().UTC().Truncate(time.Second)
	ctx := context.Background()

	// The newer edit lands first.
	if _, err := s.UpsertEvents(ctx, append(session(t, email, sid),
		fileChange("fc-new", sid, email, "a.go", "newer", 11, base.Add(time.Hour), false),
	)); err != nil {
		t.Fatalf("ingest newer: %v", err)
	}
	// Then a stale one from earlier in the session.
	if _, err := s.UpsertEvents(ctx, []Ingest{
		fileChange("fc-old", sid, email, "a.go", "older", 10, base, true),
	}); err != nil {
		t.Fatalf("ingest older: %v", err)
	}

	arts := artifactsOf(t, s, email, sid)
	if len(arts) != 1 {
		t.Fatalf("got %d artifacts, want 1", len(arts))
	}
	a := arts[0]
	if a.VersionCount != 2 {
		t.Errorf("version_count = %d, want 2 (both edits are history)", a.VersionCount)
	}
	if a.LatestBytes != int64(len("newer")) {
		t.Errorf("latest_bytes = %d, want %d: the stale edit overwrote the latest",
			a.LatestBytes, len("newer"))
	}
	if !a.LastSeen.Equal(base.Add(time.Hour)) {
		t.Errorf("last_seen = %s, want the newer edit's time %s", a.LastSeen, base.Add(time.Hour))
	}
}

// Re-delivery is normal: a spool that could not confirm its upload sends again.
// It must not manufacture versions or inflate how many times a link was
// mentioned, because the whole point of those numbers is that they describe the
// session rather than the network.
func TestIntegrationRedeliveringABatchChangesNothing(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-4"
	mustPrincipal(t, s, email, RoleMember)

	base := time.Now().UTC().Truncate(time.Second)
	batch := append(session(t, email, sid),
		fileChange("fc-1", sid, email, "a.go", "one", 10, base, true),
	)
	batch[0].Event.Text = "opened https://github.com/loopai-hq/agent-sessions/pull/4"

	ctx := context.Background()
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	before := artifactsOf(t, s, email, sid)
	linksBefore, err := s.SessionLinks(ctx, Viewer{Email: email, Role: RoleMember}, sid)
	if err != nil {
		t.Fatalf("SessionLinks: %v", err)
	}

	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	after := artifactsOf(t, s, email, sid)
	linksAfter, err := s.SessionLinks(ctx, Viewer{Email: email, Role: RoleMember}, sid)
	if err != nil {
		t.Fatalf("SessionLinks: %v", err)
	}

	if len(before) != len(after) || before[0].VersionCount != after[0].VersionCount {
		t.Errorf("re-delivery changed artifacts: %+v -> %+v", before, after)
	}
	if len(linksBefore) != len(linksAfter) {
		t.Fatalf("re-delivery changed the link count: %d -> %d", len(linksBefore), len(linksAfter))
	}
	if linksAfter[0].Occurrences != linksBefore[0].Occurrences {
		t.Errorf("re-delivery inflated occurrences: %d -> %d",
			linksBefore[0].Occurrences, linksAfter[0].Occurrences)
	}
}

// Links are the other half of the feature, and the classification is what makes
// them worth more than the raw text they came from.
func TestIntegrationLinksAreExtractedAndClassified(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-5"
	mustPrincipal(t, s, email, RoleMember)

	batch := session(t, email, sid)
	batch[0].Event.Text = "see https://github.com/loopai-hq/agent-sessions/pull/4 and https://notion.so/example/runbook"
	batch[1].Event.Text = "again https://github.com/loopai-hq/agent-sessions/pull/4"

	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	got, err := s.SessionLinks(context.Background(), Viewer{Email: email, Role: RoleMember}, sid)
	if err != nil {
		t.Fatalf("SessionLinks: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d links, want 2: %+v", len(got), got)
	}
	// Pull requests sort first because that is what people come looking for.
	if got[0].Kind != "pr" {
		t.Errorf("first link kind = %q, want pr", got[0].Kind)
	}
	if got[0].Ref != "loopai-hq/agent-sessions#4" {
		t.Errorf("ref = %q", got[0].Ref)
	}
	if got[0].Occurrences != 2 {
		t.Errorf("occurrences = %d, want 2", got[0].Occurrences)
	}
	if got[1].Kind != "doc" {
		t.Errorf("second link kind = %q, want doc", got[1].Kind)
	}
}

// An artifact must be exactly as visible as the transcript it came from. This is
// the check that stops the feature becoming a way to read a colleague's files.
func TestIntegrationAnotherPrincipalsArtifactsAreRefused(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const owner, other, sid = "owner@example.com", "other@example.com", "s-art-6"
	mustPrincipal(t, s, owner, RoleMember)
	mustPrincipal(t, s, other, RoleMember)

	base := time.Now().UTC().Truncate(time.Second)
	batch := append(session(t, owner, sid),
		fileChange("fc-1", sid, owner, "secret.go", "confidential", 10, base, true),
	)
	ctx := context.Background()
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	mine := artifactsOf(t, s, owner, sid)
	if len(mine) != 1 {
		t.Fatalf("the owner cannot see their own artifact")
	}

	stranger := Viewer{Email: other, Role: RoleMember}
	if _, err := s.SessionArtifacts(ctx, stranger, sid); err == nil {
		t.Error("another principal listed the artifacts of a session they cannot read")
	}
	if _, _, err := s.Artifact(ctx, stranger, mine[0].ID); err == nil {
		t.Error("another principal opened an artifact they cannot read")
	}
	if _, err := s.ArtifactContent(ctx, stranger, mine[0].ID, "fc-1"); err == nil {
		t.Error("another principal read the CONTENTS of a file they cannot read")
	}
	if _, err := s.SessionLinks(ctx, stranger, sid); err == nil {
		t.Error("another principal listed the links of a session they cannot read")
	}
}

// The per-session rebuild the derive runner's artifacts and links steps call
// claims to reproduce what the live path produces. That claim is the whole
// reason it replays stored events through the same insertArtifacts and
// insertLinks the live path calls instead of reimplementing the derivation
// in SQL, so it is tested rather than asserted in a comment: derive live,
// throw the derived rows away, rebuild, and require the result to match.
// This test outlived BackfillDerived, which made the same claim for the
// whole corpus at once and is gone.
func TestIntegrationThePerSessionRebuildReproducesTheLivePath(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-8"
	mustPrincipal(t, s, email, RoleMember)

	base := time.Now().UTC().Truncate(time.Second)
	batch := append(session(t, email, sid),
		fileChange("fc-1", sid, email, "a.go", "one", 10, base, true),
		fileChange("fc-2", sid, email, "a.go", "two", 11, base.Add(time.Minute), false),
		fileChange("fc-3", sid, email, "b.go", "bee", 12, base.Add(2*time.Minute), true),
	)
	batch[0].Event.Text = "opened https://github.com/loopai-hq/agent-sessions/pull/4 and https://notion.so/x"
	batch[1].Event.Text = "again https://github.com/loopai-hq/agent-sessions/pull/4"

	ctx := context.Background()
	if _, err := s.UpsertEvents(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	v := Viewer{Email: email, Role: RoleMember}
	liveArts := artifactsOf(t, s, email, sid)
	liveLinks, err := s.SessionLinks(ctx, v, sid)
	if err != nil {
		t.Fatalf("SessionLinks: %v", err)
	}
	if len(liveArts) != 2 || len(liveLinks) != 2 {
		t.Fatalf("fixture is wrong: %d artifacts, %d links", len(liveArts), len(liveLinks))
	}

	// Throw away everything derived, leaving the events untouched — exactly the
	// state every event ingested before this feature existed is in.
	if _, err := pool.Exec(ctx, `DELETE FROM artifact_versions`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM artifacts`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM links`); err != nil {
		t.Fatal(err)
	}
	if arts := artifactsOf(t, s, email, sid); len(arts) != 0 {
		t.Fatalf("the teardown did not clear artifacts")
	}

	rebuild := func(what string) {
		t.Helper()
		files, err := rebuildSessionArtifacts(ctx, s.db, sid)
		if err != nil {
			t.Fatalf("%s rebuildSessionArtifacts: %v", what, err)
		}
		texts, err := rebuildSessionLinks(ctx, s.db, sid)
		if err != nil {
			t.Fatalf("%s rebuildSessionLinks: %v", what, err)
		}
		if files == 0 || texts == 0 {
			t.Errorf("%s rebuild replayed %d file changes and %d text rows, want both non-zero", what, files, texts)
		}
	}
	rebuild("the")

	rebuiltArts := artifactsOf(t, s, email, sid)
	rebuiltLinks, err := s.SessionLinks(ctx, v, sid)
	if err != nil {
		t.Fatalf("SessionLinks after rebuild: %v", err)
	}

	if len(rebuiltArts) != len(liveArts) {
		t.Fatalf("rebuild produced %d artifacts, live produced %d", len(rebuiltArts), len(liveArts))
	}
	for i := range liveArts {
		a, b := liveArts[i], rebuiltArts[i]
		// Ids are new rows, so they are expected to differ; everything that
		// describes the file must not.
		if a.Path != b.Path || a.VersionCount != b.VersionCount ||
			a.LatestSHA != b.LatestSHA || a.LatestBytes != b.LatestBytes || a.Created != b.Created {
			t.Errorf("artifact %d differs after rebuild:\n live    %+v\n rebuilt %+v", i, a, b)
		}
	}
	if len(rebuiltLinks) != len(liveLinks) {
		t.Fatalf("rebuild produced %d links, live produced %d", len(rebuiltLinks), len(liveLinks))
	}
	for i := range liveLinks {
		a, b := liveLinks[i], rebuiltLinks[i]
		if a.URL != b.URL || a.Kind != b.Kind || a.Ref != b.Ref || a.Occurrences != b.Occurrences {
			t.Errorf("link %d differs after rebuild:\n live    %+v\n rebuilt %+v", i, a, b)
		}
	}

	// And running it twice must not double the occurrence counts, which is the
	// one number an upsert-on-conflict would happily inflate.
	rebuild("a second")
	twice, err := s.SessionLinks(ctx, v, sid)
	if err != nil {
		t.Fatal(err)
	}
	for i := range rebuiltLinks {
		if twice[i].Occurrences != rebuiltLinks[i].Occurrences {
			t.Errorf("a second rebuild inflated %s: %d -> %d",
				twice[i].URL, rebuiltLinks[i].Occurrences, twice[i].Occurrences)
		}
	}
}

// An Edit's After field is the replacement text, not the file. Only Before holds
// the whole file, and only for an edit; a create puts the whole file in After.
//
// This shipped wrong once: sizes were the size of an edit fragment presented as
// a file size, the viewer showed a snippet while claiming to show the file, and
// the checksum was computed over replacement text — so two identical
// replacements made in different parts of a file collapsed into one version. The
// last of those is the reason this test asserts on the checksum and not only on
// the rendered size.
func TestIntegrationAVersionIsTheWholeFileAndNotAnEditFragment(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-9"
	mustPrincipal(t, s, email, RoleMember)

	whole := "package main\n" + strings.Repeat("// a long file\n", 500)
	fragment := "// just this line\n"

	base := time.Now().UTC().Truncate(time.Second)
	create := fileChange("fc-1", sid, email, "big.go", whole, 10, base, true)

	// An edit: the whole file in Before, only the replacement in After. This is
	// the shape capture produces for the Edit tool.
	edit := ingestOf("fc-2", sid, email, event.FileChanged, 11)
	edit.Event.OccurredAt = base.Add(time.Minute)
	edit.Event.Tool = &event.Tool{Name: "Edit", Diff: &event.Diff{
		Path: "big.go", Before: whole + "// grown\n", After: fragment,
	}}

	ctx := context.Background()
	if _, err := s.UpsertEvents(ctx, append(session(t, email, sid), create, edit)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	arts := artifactsOf(t, s, email, sid)
	if len(arts) != 1 {
		t.Fatalf("got %d artifacts, want 1", len(arts))
	}
	a := arts[0]

	// The size must describe the file, not the fragment that replaced part of it.
	if a.LatestBytes == int64(len(fragment)) {
		t.Fatalf("latest_bytes is the edit fragment's size (%d); it must be the file's", a.LatestBytes)
	}
	if a.LatestBytes != int64(len(whole)+len("// grown\n")) {
		t.Errorf("latest_bytes = %d, want the whole file's %d",
			a.LatestBytes, len(whole)+len("// grown\n"))
	}

	v := Viewer{Email: email, Role: RoleMember}
	_, versions, err := s.Artifact(ctx, v, a.ID)
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("got %d versions, want 2", len(versions))
	}
	body, err := s.ArtifactContent(ctx, v, a.ID, versions[0].EventID)
	if err != nil {
		t.Fatalf("ArtifactContent: %v", err)
	}
	if body == fragment {
		t.Fatal("the viewer returned the edit fragment instead of the file")
	}
	if !strings.HasPrefix(body, "package main\n") || len(body) < len(whole) {
		t.Errorf("the newest version is not a whole file (%d bytes)", len(body))
	}
}

// The failure that hides behind a fragment checksum: the same replacement text
// applied twice, to a file that differs between the two edits, is two distinct
// states of the file and must be two versions.
func TestIntegrationIdenticalEditsToADifferingFileAreTwoVersions(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-10"
	mustPrincipal(t, s, email, RoleMember)

	const same = "x = 1\n" // the identical replacement, made twice
	base := time.Now().UTC().Truncate(time.Second)

	mk := func(id, before string, seq int64, at time.Time) Ingest {
		it := ingestOf(id, sid, email, event.FileChanged, seq)
		it.Event.OccurredAt = at
		it.Event.Tool = &event.Tool{Name: "Edit", Diff: &event.Diff{
			Path: "a.go", Before: before, After: same,
		}}
		return it
	}

	batch := append(session(t, email, sid),
		mk("fc-1", "file state one\n", 10, base),
		mk("fc-2", "file state two\n", 11, base.Add(time.Minute)),
	)
	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	arts := artifactsOf(t, s, email, sid)
	if len(arts) != 1 {
		t.Fatalf("got %d artifacts, want 1", len(arts))
	}
	if arts[0].VersionCount != 2 {
		t.Errorf("version_count = %d, want 2: identical replacement text collapsed "+
			"two different file states into one version", arts[0].VersionCount)
	}
}

// An event that changed nothing on disk carries no diff, and a diff with no path
// names no file. Neither may create an artifact, or the panel fills with rows
// that point at nothing.
func TestIntegrationEventsWithoutAUsableDiffCreateNoArtifact(t *testing.T) {
	s := newStore(t, flatPricer{})
	fresh(t, s)
	const email, sid = "dev@example.com", "s-art-7"
	mustPrincipal(t, s, email, RoleMember)

	noDiff := ingestOf("fc-nodiff", sid, email, event.FileChanged, 10)
	noDiff.Event.Tool = &event.Tool{Name: "Edit"}

	noPath := ingestOf("fc-nopath", sid, email, event.FileChanged, 11)
	noPath.Event.Tool = &event.Tool{Name: "Edit", Diff: &event.Diff{Path: "  ", After: "x"}}

	batch := append(session(t, email, sid), noDiff, noPath)
	if _, err := s.UpsertEvents(context.Background(), batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if arts := artifactsOf(t, s, email, sid); len(arts) != 0 {
		t.Errorf("got %d artifacts from events with no usable diff: %+v", len(arts), arts)
	}
}
