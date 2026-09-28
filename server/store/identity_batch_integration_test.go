//go:build integration

package store

// One record identity arriving more than once in a single batch, and the
// shapes around it that PR C's adversarial review found on the ingest path:
// the walker's documented "rare double" re-walked under a newer extraction
// with new text, two aliases of one stored row at different versions in
// either order, a re-walked copy beside the id it re-walked, an upgrade-only
// batch landing while the dirty pass is mid-fold, and a client-controlled
// key value the btree cannot hold. Each was red before its fix.

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/server/store/derive"
)

func sessionTokens(t *testing.T, sid string) (int64, float64) {
	t.Helper()
	var in int64
	var cost float64
	if err := pool.QueryRow(context.Background(),
		`SELECT tokens_input, cost_usd::float8 FROM sessions WHERE session_id = $1`, sid).Scan(&in, &cost); err != nil {
		t.Fatalf("read tokens of %s: %v", sid, err)
	}
	return in, cost
}

// storedText reads what the events row and its messages row hold, so a test
// can say which extraction won and that the search corpus agrees with it.
func storedText(t *testing.T, id string) (body, message string, version int) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `
		SELECT e.body->>'text', coalesce(m.text, ''), e.capture_version
		FROM events e LEFT JOIN messages m ON m.event_id = e.id
		WHERE e.id = $1`, id).Scan(&body, &message, &version); err != nil {
		t.Fatalf("read %s: %v", id, err)
	}
	return body, message, version
}

func sortedCopy(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

// incompressible is n bytes a btree cannot squeeze: it compresses an index
// tuple, so a run of one character 4,000 long fits where this does not.
func incompressible(n int) string {
	big := make([]byte, 0, n)
	x := uint32(2463534242)
	for len(big) < n {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		big = append(big, "0123456789abcdef"[x%16])
	}
	return string(big)
}

// The walker's rare double stores one record as two rows on first sight
// (nothing to match yet). The next walk under a newer extraction delivers
// both copies under new ids and both alias both stored rows. Each stored
// row is upgraded once, from the same elected copy, both ids are
// acknowledged as duplicates, and the batch lands: before the fix the
// messages refresh named one row twice in one ON CONFLICT DO UPDATE and
// Postgres refused the statement (SQLSTATE 21000), so the client retried
// the same batch on every drain, forever.
func TestIntegrationARareDoubleReWalkedWithNewTextUpgradesOnce(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-rare-double"

	a1 := transcriptAt("g-1", sid, event.AssistantTurn, 1, dv0, "old text", assistantRecord("u-g1", "msg_g1", "old text"))
	a2 := transcriptAt("g-2", sid, event.AssistantTurn, 2, dv0, "old text", assistantRecord("u-g1", "msg_g1", "old text"))
	ingestBatch(t, s, transcriptAt("g-0", sid, event.UserPrompt, 0, dv0.Add(-time.Second), "do", userRecord("u-g0", "", "do")), a1, a2)
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); n != 3 {
		t.Fatalf("events = %d, want 3: the double is stored twice on first sight", n)
	}
	deriveAll(t, s, dv0.Add(time.Hour))

	r1 := transcriptAt("g-1-rw", sid, event.AssistantTurn, 2, dv0, "new text", assistantRecord("u-g1", "msg_g1", "new text"))
	r2 := transcriptAt("g-2-rw", sid, event.AssistantTurn, 3, dv0, "new text", assistantRecord("u-g1", "msg_g1", "new text"))
	r1.Event.CaptureVersion, r2.Event.CaptureVersion = 4, 4
	res, err := s.UpsertEvents(ctx, []Ingest{r1, r2})
	if err != nil {
		t.Fatalf("the re-walk of a doubled record failed the whole batch: %v", err)
	}
	if len(res.Inserted) != 0 || strings.Join(sortedCopy(res.Duplicate), ",") != "g-1-rw,g-2-rw" {
		t.Errorf("verdict inserted=%v duplicate=%v, want both copies acknowledged as duplicates", res.Inserted, res.Duplicate)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); n != 3 {
		t.Errorf("events = %d after the re-walk, want 3", n)
	}
	if body, msg, v := storedText(t, "g-1"); body != "new text" || msg != "new text" || v != 4 {
		t.Errorf("g-1 = %q / messages %q at %d, want the new extraction at 4 in both", body, msg, v)
	}
	if body, msg, v := storedText(t, "g-2"); body != "new text" || msg != "new text" || v != 4 {
		t.Errorf("g-2 = %q / messages %q at %d, want the second copy of the double carrying the newest extraction too", body, msg, v)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'g-2' AND seq = 2`); n != 1 {
		t.Errorf("the upgrade moved g-2's seq; the stored rows keep their positions")
	}
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = $1 AND derive_dirty`, sid); n != 1 {
		t.Errorf("the upgrade did not mark the session for the fold")
	}
}

// The rare double, re-walked and upgraded, then folded: the fold elects the
// turn's final answer among the stored copies by position, so the final's
// text has to be the newest extraction whichever copy it picks, or the
// reader and the export render the pre-upgrade text as the answer while
// the timeline shows both. The two fixtures put each copy last in turn.
// Before the fix only the lower-id row was upgraded and the fold's final
// was the other one, at the old text (review-4 finding N2). The re-walk
// also brings the usage the first sight lacked: the ledger keys the two
// rows by the one model call and the session is credited once.
func TestIntegrationARareDoubleUpgradedFoldsToTheNewestExtraction(t *testing.T) {
	buildIdentityIndex(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		seq1, seq2 int64
	}{
		{"the second copy is last", 1, 2},
		{"the first copy is last", 2, 1},
	} {
		s := newStore(t, flatPricer{perToken: 0.001})
		mustPrincipal(t, s, dvEmail, RoleMember)
		const sid = "s-rare-double-final"

		a1 := transcriptAt("g-1", sid, event.AssistantTurn, tc.seq1, dv0, "old text", assistantRecord("u-g1", "msg_g1", "old text"))
		a2 := transcriptAt("g-2", sid, event.AssistantTurn, tc.seq2, dv0, "old text", assistantRecord("u-g1", "msg_g1", "old text"))
		a1.Event.Model, a2.Event.Model = "claude-opus-5", "claude-opus-5"
		ingestBatch(t, s, transcriptAt("g-0", sid, event.UserPrompt, 0, dv0.Add(-time.Second), "do", userRecord("u-g0", "", "do")), a1, a2)
		deriveAll(t, s, dv0.Add(time.Hour))
		if in, _ := sessionTokens(t, sid); in != 0 {
			t.Fatalf("%s: tokens before the re-walk = %d", tc.name, in)
		}

		r1 := transcriptAt("g-1-rw", sid, event.AssistantTurn, 3, dv0, "new text", assistantRecord("u-g1", "msg_g1", "new text"))
		r2 := transcriptAt("g-2-rw", sid, event.AssistantTurn, 4, dv0, "new text", assistantRecord("u-g1", "msg_g1", "new text"))
		for _, it := range []*Ingest{&r1, &r2} {
			it.Event.CaptureVersion = 4
			it.Event.Model = "claude-opus-5"
			it.Event.Usage = &event.Usage{InputTokens: 300, OutputTokens: 30, MessageID: "msg_g1"}
		}
		if _, err := s.UpsertEvents(ctx, []Ingest{r1, r2}); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		deriveAll(t, s, dv0.Add(2*time.Hour))

		if n := countRows(t, `SELECT count(*) FROM turns WHERE session_id = $1`, sid); n != 1 {
			t.Errorf("%s: turns = %d, want the double counted once", tc.name, n)
		}
		var finalID, finalText string
		if err := pool.QueryRow(ctx, `
			SELECT t.final_event_id, e.body->>'text' FROM turns t JOIN events e ON e.id = t.final_event_id
			WHERE t.session_id = $1 ORDER BY t.turn_index LIMIT 1`, sid).Scan(&finalID, &finalText); err != nil {
			t.Fatalf("%s: read the turn's final: %v", tc.name, err)
		}
		if finalText != "new text" {
			t.Errorf("%s: the turn's final answer is %s with %q, want the newest extraction whichever copy the fold picks", tc.name, finalID, finalText)
		}
		if _, msg, v := storedText(t, finalID); msg != "new text" || v != 4 {
			t.Errorf("%s: the final %s reads %q at %d in messages, want the newest extraction at 4", tc.name, finalID, msg, v)
		}
		if n := countRows(t, `SELECT count(*) FROM usage_ledger WHERE session_id = $1`, sid); n != 1 {
			t.Errorf("%s: ledger rows = %d, want the one model call priced once across both rows", tc.name, n)
		}
		if in, _ := sessionTokens(t, sid); in != 300 {
			t.Errorf("%s: tokens_input = %d, want 300", tc.name, in)
		}
	}
}

// The rare double's own ids re-delivered: the stored id of either twin,
// alone or beside an alias, is one of the identity's rows, so it goes
// through the insert's version guard for its own row and the identity
// upgrade carries the same copy to the twin. Before the fix the second
// twin's own id matched the first row only and was treated as an alias
// of it, so its body landed on the wrong row and its own row kept the old
// extraction.
func TestIntegrationTheStoredIDOfEitherTwinUpgradesBoth(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-twin-own-id"

	mk := func(id string, seq int64, v int, text string) Ingest {
		it := transcriptAt(id, sid, event.AssistantTurn, seq, dv0, text, assistantRecord("u-w1", "msg_w1", text))
		it.Event.CaptureVersion = v
		return it
	}
	ingestBatch(t, s, transcriptAt("w-0", sid, event.UserPrompt, 0, dv0.Add(-time.Second), "do", userRecord("u-w0", "", "do")), mk("w-1", 1, 3, "old"), mk("w-2", 2, 3, "old"))

	for _, tc := range []struct {
		name  string
		batch []Ingest
		want  string
	}{
		{"second twin's own id alone", []Ingest{mk("w-2", 2, 4, "own2")}, "own2"},
		{"first twin's own id alone", []Ingest{mk("w-1", 1, 4, "own1")}, "own1"},
		{"own id at 5 beside an alias at 4", []Ingest{mk("w-2", 2, 5, "own5"), mk("w-1-rw", 3, 4, "alias4")}, "own5"},
		{"alias at 5 beside an own id at 4", []Ingest{mk("w-1", 1, 4, "own4"), mk("w-2-rw", 3, 5, "alias5")}, "alias5"},
	} {
		if _, err := pool.Exec(ctx, `UPDATE events SET capture_version = 3, body = jsonb_set(body, '{text}', '"old"') WHERE session_id = $1 AND type = 'assistant_turn'`, sid); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE messages SET text = 'old' WHERE session_id = $1 AND role = 'assistant'`, sid); err != nil {
			t.Fatal(err)
		}
		res, err := s.UpsertEvents(ctx, tc.batch)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(res.Inserted) != 0 || len(res.Duplicate) != len(tc.batch) {
			t.Errorf("%s: verdict inserted=%v duplicate=%v, want every copy acknowledged as a stored row", tc.name, res.Inserted, res.Duplicate)
		}
		for _, id := range []string{"w-1", "w-2"} {
			if body, msg, v := storedText(t, id); body != tc.want || msg != tc.want || v != 4+len(tc.batch)-1 {
				t.Errorf("%s: %s = %q / messages %q at %d, want %q on both twins", tc.name, id, body, msg, v, tc.want)
			}
		}
		if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); n != 3 {
			t.Errorf("%s: events = %d, want 3", tc.name, n)
		}
	}
}

// An upgraded row that had no messages row (its first extraction carried
// no text) gets one from the refresh, and that row carries the stored
// row's seq and occurred_at, never the incoming copy's: the alias arrives
// at its new walk position, and the stored row's neighbours were numbered
// against the old one, which is why the identity upgrade leaves seq alone
// too. Before the fix the insert path of the refresh wrote the copy's
// position (review-4 finding N4).
func TestIntegrationTheMessagesRefreshKeepsTheStoredRowsPosition(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-refresh-position"

	ingestBatch(t, s,
		transcriptAt("q-0", sid, event.UserPrompt, 0, dv0, "do", userRecord("u-q0", "pid-q", "do")),
		transcriptAt("q-1", sid, event.AssistantTurn, 1, dv0.Add(time.Second), "", assistantRecord("u-q1", "msg_q1", "")))
	if n := countRows(t, `SELECT count(*) FROM messages WHERE event_id = 'q-1'`); n != 0 {
		t.Fatalf("messages rows for the empty extraction = %d, want none", n)
	}

	rw := transcriptAt("q-1-rw", sid, event.AssistantTurn, 7, dv0.Add(time.Minute), "now extracted", assistantRecord("u-q1", "msg_q1", "now extracted"))
	rw.Event.CaptureVersion = 4
	res, err := s.UpsertEvents(ctx, []Ingest{rw})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 0 || len(res.Duplicate) != 1 {
		t.Fatalf("verdict inserted=%v duplicate=%v, want the copy acknowledged as the stored row", res.Inserted, res.Duplicate)
	}
	var seq int64
	var at time.Time
	var text string
	if err := pool.QueryRow(ctx, `SELECT seq, occurred_at, text FROM messages WHERE event_id = 'q-1'`).Scan(&seq, &at, &text); err != nil {
		t.Fatalf("the refresh wrote no messages row for the upgraded row: %v", err)
	}
	if text != "now extracted" {
		t.Errorf("messages text = %q, want the new extraction", text)
	}
	if seq != 1 || !at.Equal(dv0.Add(time.Second)) {
		t.Errorf("messages row at seq %d / %s, want the stored row's seq 1 / %s, not the incoming copy's", seq, at.UTC(), dv0.Add(time.Second))
	}
}

// A Usage.MessageID over derive.KeyMaxBytes takes the event id as the
// ledger's key, exactly as a missing message id does, so the batch is
// admitted and the call is priced once under the stand-in. Before the fix
// the value reached usage_ledger's primary key uncapped, and with a
// 4,000-byte value the whole batch failed on the btree entry ceiling
// (SQLSTATE 54000) and the good prompt beside it never landed (review-4
// finding N1, the residual of adversarial finding 4).
func TestIntegrationAnOversizedUsageMessageIDIsCreditedUnderTheEventKey(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, flatPricer{perToken: 0.001})
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-big-message-id"

	good := transcriptAt("m-0", sid, event.UserPrompt, 0, dv0.Add(-time.Second), "hello", userRecord("u-m0", "pid-m", "hello"))
	bad := transcriptAt("m-1", sid, event.AssistantTurn, 1, dv0, "hi", assistantRecord("u-m1", "msg_m1", "hi"))
	bad.Event.Model = "claude-opus-5"
	bad.Event.Usage = &event.Usage{InputTokens: 300, OutputTokens: 30, MessageID: incompressible(4000)}
	res, err := s.UpsertEvents(ctx, []Ingest{good, bad})
	if err != nil {
		t.Fatalf("one oversized usage message id failed the whole batch: %v", err)
	}
	if strings.Join(sortedCopy(res.Inserted), ",") != "m-0,m-1" {
		t.Errorf("inserted = %v, want both rows", res.Inserted)
	}
	if n := countRows(t, `SELECT count(*) FROM usage_ledger WHERE session_id = $1`, sid); n != 1 {
		t.Errorf("ledger rows = %d, want 1", n)
	}
	if n := countRows(t, `SELECT count(*) FROM usage_ledger WHERE event_id = 'm-1' AND message_id = 'event:m-1'`); n != 1 {
		t.Errorf("the ledger row is not under the event-id stand-in")
	}
	if in, _ := sessionTokens(t, sid); in != 300 {
		t.Errorf("tokens_input = %d, want 300", in)
	}
}

// An event id or session id over the key cap is refused per item with a
// stable reason, and the rows beside it land. Before this a 4,000-byte id
// reached events_pkey uncapped and failed the whole batch (SQLSTATE 54000),
// which the client re-sent on every drain (review-4 observation).
func TestIntegrationAnOversizedIDIsRejectedAloneAndTheBatchLands(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	big := incompressible(4000)

	good := transcriptAt("n-0", "s-big-id", event.UserPrompt, 0, dv0.Add(-time.Second), "hello", userRecord("u-n0", "pid-n", "hello"))
	badID := transcriptAt(big, "s-big-id", event.AssistantTurn, 1, dv0, "hi", assistantRecord("u-n1", "msg_n1", "hi"))
	badSession := transcriptAt("n-2", big, event.AssistantTurn, 1, dv0, "hi", assistantRecord("u-n2", "msg_n2", "hi"))
	res, err := s.UpsertEvents(ctx, []Ingest{good, badID, badSession})
	if err != nil {
		t.Fatalf("one oversized id failed the whole batch: %v", err)
	}
	if strings.Join(res.Inserted, ",") != "n-0" {
		t.Errorf("inserted = %v, want the good row alone", res.Inserted)
	}
	want := map[string]string{big: "event id too long", "n-2": "session id too long"}
	if len(res.Rejected) != len(want) {
		t.Fatalf("rejected %d items, want %d", len(res.Rejected), len(want))
	}
	for _, r := range res.Rejected {
		if want[r.ID] != r.Reason {
			t.Errorf("rejected id of %d bytes with %q, want %q", len(r.ID), r.Reason, want[r.ID])
		}
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = 's-big-id'`); n != 1 {
		t.Errorf("events = %d, want the good row alone", n)
	}
}

// Two aliases of one stored row at different versions in one batch: the
// stored row ends at the highest version with that copy's text, whichever
// order the copies arrived in, and the messages row agrees. Before the fix
// the UPDATE ... FROM unnest named the row twice (an arbitrary copy won) and
// the messages refresh then failed the batch.
func TestIntegrationTwoAliasesOfOneRowEndAtTheHighestVersionInEitherOrder(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-two-aliases"

	a := transcriptAt("f-1", sid, event.AssistantTurn, 1, dv0, "v3", assistantRecord("u-f1", "msg_f1", "v3"))
	ingestBatch(t, s, transcriptAt("f-0", sid, event.UserPrompt, 0, dv0.Add(-time.Second), "do", userRecord("u-f0", "", "do")), a)
	r4 := transcriptAt("f-1-v4", sid, event.AssistantTurn, 1, dv0, "v4", assistantRecord("u-f1", "msg_f1", "v4"))
	r4.Event.CaptureVersion = 4
	r5 := transcriptAt("f-1-v5", sid, event.AssistantTurn, 1, dv0, "v5", assistantRecord("u-f1", "msg_f1", "v5"))
	r5.Event.CaptureVersion = 5

	for _, order := range [][]Ingest{{r4, r5}, {r5, r4}} {
		name := order[0].Event.ID + "," + order[1].Event.ID
		if _, err := pool.Exec(ctx, `UPDATE events SET capture_version = 3, body = jsonb_set(body, '{text}', '"v3"') WHERE id = 'f-1'`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE messages SET text = 'v3' WHERE event_id = 'f-1'`); err != nil {
			t.Fatal(err)
		}
		res, err := s.UpsertEvents(ctx, order)
		if err != nil {
			t.Fatalf("order %s: %v", name, err)
		}
		if len(res.Inserted) != 0 || strings.Join(sortedCopy(res.Duplicate), ",") != "f-1-v4,f-1-v5" {
			t.Errorf("order %s: verdict inserted=%v duplicate=%v, want both acknowledged as duplicates", name, res.Inserted, res.Duplicate)
		}
		if body, msg, v := storedText(t, "f-1"); body != "v5" || msg != "v5" || v != 5 {
			t.Errorf("order %s: stored row is %q / messages %q at %d, want v5 at 5 in both", name, body, msg, v)
		}
		if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); n != 2 {
			t.Errorf("order %s: events = %d, want 2", name, n)
		}
	}
}

// The same record identity twice in one batch, both copies carrying the
// usage the stored row lacked: the ledger takes one row, and the session's
// tokens and cost move by that one row. Before the fix the rollup loop ran
// over the batch as delivered and added the one credit once per copy.
func TestIntegrationOneIdentityTwiceInABatchCreditsTheRollupOnce(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, flatPricer{perToken: 0.001})
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-identity-twice"

	a := transcriptAt("a-1", sid, event.AssistantTurn, 1, dv0, "sure", assistantRecord("u-a1", "msg_a1", "sure"))
	a.Event.Model = "claude-opus-5"
	ingestBatch(t, s, transcriptAt("a-0", sid, event.UserPrompt, 0, dv0.Add(-time.Second), "do", userRecord("u-p0", "", "do")), a)
	if in, _ := sessionTokens(t, sid); in != 0 {
		t.Fatalf("tokens before = %d", in)
	}

	r1 := a
	r1.Event.ID = "a-1-rw1"
	r1.Event.CaptureVersion = 4
	r1.Event.Usage = &event.Usage{InputTokens: 300, OutputTokens: 30, MessageID: "msg_a1"}
	r2 := r1
	r2.Event.ID = "a-1-rw2"
	res, err := s.UpsertEvents(ctx, []Ingest{r1, r2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 0 || len(res.Duplicate) != 2 {
		t.Errorf("verdict inserted=%v duplicate=%v", res.Inserted, res.Duplicate)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); n != 2 {
		t.Errorf("events = %d, want 2", n)
	}
	if n := countRows(t, `SELECT count(*) FROM usage_ledger WHERE session_id = $1`, sid); n != 1 {
		t.Errorf("ledger rows = %d, want 1", n)
	}
	if in, cost := sessionTokens(t, sid); in != 300 || cost < 0.32 || cost > 0.34 {
		t.Errorf("tokens_input = %d cost = %v, want 300 and 0.33: the one ledger row was credited to the rollup more than once", in, cost)
	}
}

// The re-walked copy under a new id and the stored id itself in one batch
// (both spool files pending): one ledger row, one credit.
func TestIntegrationAnAliasBesideItsStoredIDCreditsTheRollupOnce(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, flatPricer{perToken: 0.001})
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-alias-beside-id"

	a := transcriptAt("b-1", sid, event.AssistantTurn, 1, dv0, "sure", assistantRecord("u-b1", "msg_b1", "sure"))
	a.Event.Model = "claude-opus-5"
	ingestBatch(t, s, transcriptAt("b-0", sid, event.UserPrompt, 0, dv0.Add(-time.Second), "do", userRecord("u-q0", "", "do")), a)

	same := a
	same.Event.CaptureVersion = 4
	same.Event.Usage = &event.Usage{InputTokens: 300, OutputTokens: 30, MessageID: "msg_b1"}
	alias := same
	alias.Event.ID = "b-1-rw"
	res, err := s.UpsertEvents(ctx, []Ingest{alias, same})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 0 || len(res.Duplicate) != 2 {
		t.Errorf("verdict inserted=%v duplicate=%v", res.Inserted, res.Duplicate)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE session_id = $1`, sid); n != 2 {
		t.Errorf("events = %d, want 2", n)
	}
	if n := countRows(t, `SELECT count(*) FROM usage_ledger WHERE session_id = $1`, sid); n != 1 {
		t.Errorf("ledger rows = %d, want 1", n)
	}
	if in, _ := sessionTokens(t, sid); in != 300 {
		t.Errorf("tokens_input = %d, want 300", in)
	}
	if _, _, v := storedText(t, "b-1"); v != 4 {
		t.Errorf("b-1 at %d, want 4", v)
	}
}

// An upgrade-only batch that lands while the dirty pass is folding the same
// session. deriveOne reads the session's updated_at before the fold and
// clears derive_dirty only when the row still carries that value at the end;
// the mark an upgrade-only batch sets has to move updated_at too, or the
// fold in flight clears it and the new extraction waits for the next
// versioned pass. The interleaving is replayed by hand around the batch,
// with deriveOne's two statements verbatim, because the fold has no hook
// between its facts read and its lock.
func TestIntegrationAnUpgradeOnlyBatchDuringAFoldKeepsItsDirtyMark(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()
	const sid = "s-mid-fold-upgrade"

	ingestBatch(t, s,
		transcriptAt("d-0", sid, event.UserPrompt, 0, dv0, "hello", userRecord("u-d0", "pid-d", "hello")),
		transcriptAt("d-1", sid, event.AssistantTurn, 1, dv0.Add(time.Second), "hi", assistantRecord("u-d1", "msg_d1", "hi")))
	deriveAll(t, s, dv0.Add(time.Hour))
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = $1 AND derive_dirty`, sid); n != 0 {
		t.Fatal("dirty before the probe")
	}

	// deriveOne, the head: the facts read in the fold's own transaction.
	tx, err := s.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	facts, err := readSessionFacts(ctx, tx, sid)
	if err != nil {
		t.Fatal(err)
	}

	// Ingest, concurrently: an upgrade-only batch for the same session.
	up := transcriptAt("d-1-rw", sid, event.AssistantTurn, 1, dv0.Add(time.Second), "hi (new extraction)", assistantRecord("u-d1", "msg_d1", "hi (new extraction)"))
	up.Event.CaptureVersion = 5
	res, err := s.UpsertEvents(ctx, []Ingest{up})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inserted) != 0 || len(res.Duplicate) != 1 {
		t.Fatalf("upgrade-only verdict %+v", res)
	}
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = $1 AND derive_dirty`, sid); n != 1 {
		t.Fatalf("the upgrade-only batch did not mark the session dirty")
	}

	// deriveOne, the tail: the touched test and the flag write.
	var touched bool
	if err := tx.QueryRow(ctx, `SELECT updated_at <> $2 FROM sessions WHERE session_id = $1 FOR UPDATE`, sid, facts.UpdatedAt).Scan(&touched); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET derive_dirty = $2 WHERE session_id = $1`, sid, touched); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !touched {
		t.Errorf("the fold read the session as untouched after an upgrade-only batch committed against it")
	}
	if n := countRows(t, `SELECT count(*) FROM sessions WHERE session_id = $1 AND derive_dirty`, sid); n != 1 {
		t.Errorf("the fold cleared the dirty mark the upgrade-only batch set; the new extraction is not folded until the next versioned pass")
	}
}

// A key value longer than derive.KeyMaxBytes is dropped at ingest and by the
// runner's event_keys step, so the row lands by id with the key NULL and
// the identity index can be built over it. Before the fix, with the index
// in place, one incompressible value of 4,000 bytes failed the whole batch
// (btree entry ceiling, SQLSTATE 54000) and the good prompt beside it was
// lost with it; stored before the index existed, the same row failed the
// runner's index step on every attempt and parked the versioned pass.
func TestIntegrationAnOversizedKeyIsDroppedAndTheRowLandsByID(t *testing.T) {
	buildIdentityIndex(t)
	s := newStore(t, nil)
	mustPrincipal(t, s, dvEmail, RoleMember)
	ctx := context.Background()

	bigUUID := incompressible(4000)
	if len(bigUUID) <= derive.KeyMaxBytes {
		t.Fatalf("probe value is %d bytes, not over the cap", len(bigUUID))
	}

	// With the index in place (production after the runner's first window).
	good := transcriptAt("h-0", "s-big-key", event.UserPrompt, 0, dv0.Add(-time.Second), "hello", userRecord("u-h0", "pid-h", "hello"))
	bad := transcriptAt("h-1", "s-big-key", event.AssistantTurn, 1, dv0, "hi", assistantRecord(bigUUID, "msg_h1", "hi"))
	res, err := s.UpsertEvents(ctx, []Ingest{good, bad})
	if err != nil {
		t.Fatalf("one oversized key failed the whole batch: %v", err)
	}
	if strings.Join(sortedCopy(res.Inserted), ",") != "h-0,h-1" {
		t.Errorf("inserted = %v, want both rows", res.Inserted)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'h-1' AND record_uuid IS NULL AND message_id = 'msg_h1'`); n != 1 {
		t.Errorf("the oversized record_uuid was stored, or its neighbours were dropped with it")
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'h-0' AND record_uuid = 'u-h0' AND prompt_id = 'pid-h'`); n != 1 {
		t.Errorf("the good row beside it lost its keys")
	}

	// The hook path's named prompt id reaches events_session_prompt_idx the
	// same way.
	hook := hookAt("h-hook", "s-big-key-hook", dvEmail, event.UserPrompt, 1, dv0, "hello")
	hook.Event.PromptID = bigUUID
	if _, err := s.UpsertEvents(ctx, []Ingest{hook}); err != nil {
		t.Fatalf("an oversized prompt_id on a hook row failed the batch: %v", err)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'h-hook' AND prompt_id IS NULL`); n != 1 {
		t.Errorf("the oversized prompt_id was stored")
	}

	// Without the index (production until the runner's first window): the
	// row lands by id, and the runner's index step and its event_keys pass
	// over the stored body both succeed.
	if _, err := pool.Exec(ctx, `DROP INDEX events_record_identity_idx`); err != nil {
		t.Fatal(err)
	}
	s2 := New(pool, nil)
	good.Event.SessionID, bad.Event.SessionID = "s-big-key-2", "s-big-key-2"
	good.Event.ID, bad.Event.ID = "h-0b", "h-1b"
	if _, err := s2.UpsertEvents(ctx, []Ingest{good, bad}); err != nil {
		t.Fatalf("without the index: %v", err)
	}
	// What the old server's rows look like: keys never written.
	if _, err := pool.Exec(ctx, `UPDATE events SET record_uuid = NULL, message_id = NULL, request_id = NULL WHERE id = 'h-1b'`); err != nil {
		t.Fatal(err)
	}
	resetDerived(t)
	pass, err := s2.RunDerive(ctx, DeriveConfig{Window: derive.Window{Always: true}, RowsPerSec: 1_000_000, Sleep: noSleep})
	if err != nil {
		t.Fatalf("RunDerive: %v", err)
	}
	if !pass.Done || pass.Failed != "" {
		t.Fatalf("pass = %+v, want every step finished over the stored oversized key", pass)
	}
	var valid bool
	if err := pool.QueryRow(ctx, `SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('events_record_identity_idx')`).Scan(&valid); err != nil || !valid {
		t.Errorf("identity index after the pass: valid=%v err=%v", valid, err)
	}
	if n := countRows(t, `SELECT count(*) FROM events WHERE id = 'h-1b' AND record_uuid IS NULL AND message_id = 'msg_h1'`); n != 1 {
		t.Errorf("event_keys wrote the oversized record_uuid, or dropped the message id beside it")
	}
}
