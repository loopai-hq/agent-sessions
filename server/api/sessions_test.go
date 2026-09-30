package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestListSessionsIsScopedAndAlwaysAnArray(t *testing.T) {
	f := seeded()
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	w := do(t, h, http.MethodGet, "/v1/sessions", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body)
	}
	page := decodeBody[sessionListResponse](t, w)
	if len(page.Sessions) != 1 || page.Sessions[0].SessionID != "own-1" {
		t.Fatalf("sessions = %+v, want only own-1", page.Sessions)
	}

	// A viewer with nothing yet must get an empty array. A client that has to
	// handle both null and [] handles one of them wrong, and the one it gets
	// wrong is the case a new employee sees on their first visit.
	f.addPrincipal(Principal{Email: "new@example.com", Role: RoleMember})
	h2 := newHandler(t, f, &fakeAuth{email: "new@example.com"})
	w2 := do(t, h2, http.MethodGet, "/v1/sessions", "")
	if got := w2.Body.String(); got != `{"sessions":[]}` {
		t.Fatalf("empty listing = %s, want an empty array", got)
	}
}

func TestListSessionsPassesFiltersToTheStore(t *testing.T) {
	f := seeded()
	h := newHandler(t, f, &fakeAuth{email: "admin@example.com"})

	q := url.Values{}
	q.Set("q", "migration")
	q.Set("email", "Other@Example.com")
	q.Set("source", "claude_code")
	q.Set("repo", "loop-sessions")
	q.Set("from", "2026-08-01T00:00:00Z")
	q.Set("to", "2026-08-05T00:00:00Z")
	q.Set("limit", "3")
	if w := do(t, h, http.MethodGet, "/v1/sessions?"+q.Encode(), ""); w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body)
	}

	got := f.lastSessionFilter
	if got.Query != "migration" || got.Source != "claude_code" || got.Repo != "loop-sessions" {
		t.Fatalf("filter = %+v", got)
	}
	// The address is normalised before it reaches the store, because a roster
	// row written as other@example.com must match a filter typed with capitals.
	if got.Email != "other@example.com" {
		t.Fatalf("email filter = %q, want normalised", got.Email)
	}
	if !got.From.Equal(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("from = %v", got.From)
	}
	if got.Limit != 3 {
		t.Fatalf("limit = %d, want 3", got.Limit)
	}
}

func TestListSessionsRejectsUnusableParameters(t *testing.T) {
	f := seeded()
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	for _, target := range []string{
		"/v1/sessions?from=yesterday",
		"/v1/sessions?to=2026-08-05",
		"/v1/sessions?limit=0",
		"/v1/sessions?limit=-4",
		"/v1/sessions?limit=lots",
	} {
		w := do(t, h, http.MethodGet, target, "")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", target, w.Code)
		}
	}
}

func TestListSessionsClampsAnOverLargePage(t *testing.T) {
	f := seeded()
	h, err := New(Options{Store: f, Auth: &fakeAuth{email: "member@example.com"}, MaxPageSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if w := do(t, h, http.MethodGet, "/v1/sessions?limit=5000", ""); w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if f.lastSessionFilter.Limit != 10 {
		t.Fatalf("limit = %d, want the ceiling of 10", f.lastSessionFilter.Limit)
	}
}

// TestListSessionsCursorSurvivesConcurrentIngest is the reason pagination is a
// position rather than an offset. Sessions arrive continuously, including while
// somebody is reading the list, and under an offset every arrival above the
// window makes the next page repeat one row and skip another.
func TestListSessionsCursorSurvivesConcurrentIngest(t *testing.T) {
	f := newFakeStore()
	f.addPrincipal(Principal{Email: "member@example.com", Role: RoleMember})
	for i := 1; i <= 5; i++ {
		f.sessions = append(f.sessions, Session{
			SessionID: fmt.Sprintf("s%d", i),
			Email:     "member@example.com",
			Source:    "claude_code",
			StartedAt: testNow.Add(time.Duration(-i) * time.Hour),
		})
	}
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	var seen []string
	cursor := ""
	for page := range 5 {
		target := "/v1/sessions?limit=2"
		if cursor != "" {
			target += "&cursor=" + url.QueryEscape(cursor)
		}
		w := do(t, h, http.MethodGet, target, "")
		if w.Code != http.StatusOK {
			t.Fatalf("page %d status = %d (%s)", page, w.Code, w.Body)
		}
		body := decodeBody[sessionListResponse](t, w)
		for _, s := range body.Sessions {
			seen = append(seen, s.SessionID)
		}
		cursor = body.NextCursor

		// Between pages, two newer sessions land, which is what a fleet of
		// laptops uploading does all day.
		f.sessions = append(f.sessions, Session{
			SessionID: fmt.Sprintf("fresh-%d", page),
			Email:     "member@example.com",
			Source:    "claude_code",
			StartedAt: testNow.Add(time.Duration(page) * time.Minute),
		})
		if cursor == "" {
			break
		}
	}

	want := []string{"s1", "s2", "s3", "s4", "s5"}
	if len(seen) != len(want) {
		t.Fatalf("saw %v, want exactly %v with no repeats and nothing skipped", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("saw %v, want %v", seen, want)
		}
	}
}

func TestCursorIsRejectedWhenTheQueryChanges(t *testing.T) {
	f := newFakeStore()
	f.addPrincipal(Principal{Email: "member@example.com", Role: RoleMember})
	for i := 1; i <= 4; i++ {
		f.sessions = append(f.sessions, Session{
			SessionID: fmt.Sprintf("s%d", i),
			Email:     "member@example.com",
			Source:    "claude_code",
			Repo:      "loop-sessions",
			StartedAt: testNow.Add(time.Duration(-i) * time.Hour),
		})
	}
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	first := decodeBody[sessionListResponse](t, do(t, h, http.MethodGet, "/v1/sessions?limit=2", ""))
	if first.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}
	// Same cursor, different filter: the position it names belongs to another
	// ordering, and honouring it would hand back a page that is neither the
	// first nor the next one.
	w := do(t, h, http.MethodGet, "/v1/sessions?limit=2&repo=other&cursor="+url.QueryEscape(first.NextCursor), "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if got := decodeBody[errorBody](t, w).Error.Code; got != "invalid_cursor" {
		t.Fatalf("code = %q, want invalid_cursor", got)
	}

	// The page size is not part of the identity of a query, so a reader who
	// switches from two rows to four keeps their place.
	if w := do(t, h, http.MethodGet, "/v1/sessions?limit=4&cursor="+url.QueryEscape(first.NextCursor), ""); w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body)
	}
}

func TestSessionDenialIsIndistinguishableFromAbsence(t *testing.T) {
	f := seeded()
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	forbidden := do(t, h, http.MethodGet, "/v1/sessions/other-1", "")
	absent := do(t, h, http.MethodGet, "/v1/sessions/no-such-session", "")

	if forbidden.Code != http.StatusNotFound {
		t.Fatalf("forbidden status = %d, want 404", forbidden.Code)
	}
	if absent.Code != http.StatusNotFound {
		t.Fatalf("absent status = %d, want 404", absent.Code)
	}
	// Byte-identical, not merely both 404: a difference in the message would be
	// enough to tell the caller that a colleague ran something.
	if forbidden.Body.String() != absent.Body.String() {
		t.Fatalf("bodies differ:\n forbidden %s\n absent    %s", forbidden.Body, absent.Body)
	}
	if forbidden.Header().Get("Content-Type") != absent.Header().Get("Content-Type") {
		t.Fatal("content types differ between denial and absence")
	}
	// The same must hold for every route that names a session.
	for _, suffix := range []string{"/events", "/resume"} {
		a := do(t, h, http.MethodGet, "/v1/sessions/other-1"+suffix, "")
		b := do(t, h, http.MethodGet, "/v1/sessions/no-such-session"+suffix, "")
		if a.Code != http.StatusNotFound || a.Body.String() != b.Body.String() {
			t.Fatalf("%s: %d %s vs %d %s", suffix, a.Code, a.Body, b.Code, b.Body)
		}
	}
}

func TestSessionDetailCarriesShares(t *testing.T) {
	f := seeded()
	f.shares = []Share{{
		ID: "share-1", SessionID: "own-1", CreatedBy: "member@example.com",
		Grantee: "other@example.com", Token: "tok-1", CreatedAt: testNow,
	}}
	owner := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	got := decodeBody[sessionDetailResponse](t, do(t, owner, http.MethodGet, "/v1/sessions/own-1", ""))
	if got.Session.SessionID != "own-1" {
		t.Fatalf("session = %+v", got.Session)
	}
	if len(got.Shares) != 1 || got.Shares[0].ID != "share-1" {
		t.Fatalf("shares = %+v, want the owner's own grant", got.Shares)
	}

	// The recipient of a share sees the session and not the guest list: being
	// given a session does not come with the right to see who else was given it.
	guest := newHandler(t, f, &fakeAuth{email: "other@example.com"})
	w := do(t, guest, http.MethodGet, "/v1/sessions/own-1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("guest status = %d, want 200 (%s)", w.Code, w.Body)
	}
	if n := len(decodeBody[sessionDetailResponse](t, w).Shares); n != 0 {
		t.Fatalf("guest saw %d shares, want none", n)
	}
}

// TestAuditFailureFailsTheRead is the property that makes the audit trail worth
// having. An unaudited read of a colleague's transcript is worse than a failed
// page load, so a read whose audit row cannot be written must not answer.
func TestAuditFailureFailsTheRead(t *testing.T) {
	routes := []struct {
		name   string
		method string
		target string
		body   string
	}{
		{"detail", http.MethodGet, "/v1/sessions/other-1", ""},
		{"events", http.MethodGet, "/v1/sessions/other-1/events", ""},
		{"resume", http.MethodGet, "/v1/sessions/other-1/resume", ""},
		{"search", http.MethodGet, "/v1/search?q=hello", ""},
	}
	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			f := seeded()
			f.hits = []Hit{{
				EventID: "e1", SessionID: "other-1", Email: "other@example.com",
				Seq: 1, Role: "user", OccurredAt: testNow, Snippet: "hello there",
			}}
			f.auditErr = errors.New("access_log write failed")
			// An admin, so the read is permitted and the only thing standing
			// between the viewer and the transcript is the audit row.
			h := newHandler(t, f, &fakeAuth{email: "admin@example.com"})

			w := do(t, h, tc.method, tc.target, tc.body)
			if w.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 (%s)", w.Code, w.Body)
			}
			if got := decodeBody[errorBody](t, w).Error.Code; got != "internal" {
				t.Fatalf("code = %q, want internal", got)
			}
			// Nothing of the session may appear in a response that failed to
			// record the read.
			if body := w.Body.String(); strings.Contains(body, "other-1") || strings.Contains(body, "hello") {
				t.Fatalf("failed read leaked content: %s", body)
			}
		})
	}
}

func TestOwnReadsSurviveAnAuditOutage(t *testing.T) {
	f := seeded()
	f.auditErr = errors.New("access_log write failed")
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	// Reading your own work writes no audit row, so it must not be taken down
	// by one failing. Coupling them would make every engineer's own dashboard
	// depend on a table that exists to record other people's reads.
	if w := do(t, h, http.MethodGet, "/v1/sessions/own-1", ""); w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", w.Code, w.Body)
	}
}

func TestEventsPageThroughTheTranscriptInSequenceOrder(t *testing.T) {
	f := seeded()
	for i := int64(1); i <= 5; i++ {
		f.events["own-1"] = append(f.events["own-1"], StoredEvent{
			ID: fmt.Sprintf("e%d", i), SessionID: "own-1", Email: "member@example.com",
			Seq: i, Type: "user_prompt", Origin: "hook", OccurredAt: testNow,
			Body: json.RawMessage(`{"id":"e"}`),
		})
	}
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	first := decodeBody[eventsResponse](t, do(t, h, http.MethodGet, "/v1/sessions/own-1/events?limit=2", ""))
	if len(first.Events) != 2 || first.Events[0].Seq != 1 || !first.HasMore {
		t.Fatalf("first page = %+v", first)
	}
	if first.NextCursor == "" {
		t.Fatal("expected a next cursor while more events remain")
	}

	second := decodeBody[eventsResponse](t, do(t, h, http.MethodGet,
		"/v1/sessions/own-1/events?limit=2&cursor="+url.QueryEscape(first.NextCursor), ""))
	if len(second.Events) != 2 || second.Events[0].Seq != 3 {
		t.Fatalf("second page = %+v", second)
	}
	if f.lastEventRange.AfterSeq == nil || *f.lastEventRange.AfterSeq != 2 {
		t.Fatalf("after seq = %v, want an exclusive bound of 2", f.lastEventRange.AfterSeq)
	}

	last := decodeBody[eventsResponse](t, do(t, h, http.MethodGet,
		"/v1/sessions/own-1/events?limit=2&cursor="+url.QueryEscape(second.NextCursor), ""))
	if len(last.Events) != 1 || last.HasMore || last.NextCursor != "" {
		t.Fatalf("last page = %+v, want one event and no further cursor", last)
	}
}

func TestEventsCursorCannotBeReplayedAgainstAnotherSession(t *testing.T) {
	f := seeded()
	f.events["own-1"] = []StoredEvent{
		{ID: "a", SessionID: "own-1", Seq: 1, Body: json.RawMessage(`{}`)},
		{ID: "b", SessionID: "own-1", Seq: 2, Body: json.RawMessage(`{}`)},
	}
	f.sessions = append(f.sessions, Session{
		SessionID: "own-2", Email: "member@example.com", Source: "claude_code", StartedAt: testNow,
	})
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	first := decodeBody[eventsResponse](t, do(t, h, http.MethodGet, "/v1/sessions/own-1/events?limit=1", ""))
	w := do(t, h, http.MethodGet, "/v1/sessions/own-2/events?cursor="+url.QueryEscape(first.NextCursor), "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestGarbageCursorIsAClientError(t *testing.T) {
	f := seeded()
	h := newHandler(t, f, &fakeAuth{email: "member@example.com"})

	w := do(t, h, http.MethodGet, "/v1/sessions?cursor=not-a-cursor", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 rather than a silent restart", w.Code)
	}
}
