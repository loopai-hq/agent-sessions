package web

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// fakeSlackSettings is the minimum port the pages need.
type fakeSlackSettings struct {
	view     SlackSettingsView
	created  []string
	deleted  []int64
	killed   []bool
	resolved []string
}

func (f *fakeSlackSettings) Settings(_ context.Context, string1 string) (SlackSettingsView, error) {
	return f.view, nil
}
func (f *fakeSlackSettings) Channels(_ context.Context, prefix string, _ int) ([]SlackChannel, error) {
	all := []SlackChannel{{ID: "C1ENG12345", Name: "eng-pod"}, {ID: "C1OPS54321", Name: "ops"}}
	var out []SlackChannel
	for _, c := range all {
		if prefix == "" || strings.HasPrefix(c.Name, prefix) {
			out = append(out, c)
		}
	}
	return out, nil
}
func (f *fakeSlackSettings) Users(context.Context) ([]SlackUser, error) {
	return []SlackUser{{ID: "U1MORGAN23", Handle: "morgan", RealName: "Morgan Kim"}}, nil
}
func (f *fakeSlackSettings) ResolveDestination(_ context.Context, kind, raw string) (string, error) {
	f.resolved = append(f.resolved, kind+"|"+raw)
	switch {
	case kind == "dm" || kind == "":
		return "dm", nil
	case kind == "channel" && strings.EqualFold(strings.TrimPrefix(raw, "#"), "eng-pod"):
		return "C1ENG12345", nil
	case kind == "person" && strings.HasPrefix(raw, "@morgan"):
		return "U1MORGAN23", nil
	}
	return "", fmt.Errorf("%w: nothing named %q", ErrNoSuchDestination, raw)
}

func (f *fakeSlackSettings) CreateGroup(_ context.Context, _, name, _, dest string) error {
	f.created = append(f.created, name+"->"+dest)
	return nil
}
func (f *fakeSlackSettings) UpdateGroup(context.Context, string, SlackGroup) error { return nil }
func (f *fakeSlackSettings) DeleteGroup(_ context.Context, _ string, id int64) error {
	f.deleted = append(f.deleted, id)
	return nil
}
func (f *fakeSlackSettings) SetLiveKill(_ context.Context, _ string, disabled bool) error {
	f.killed = append(f.killed, disabled)
	return nil
}
func (f *fakeSlackSettings) SessionMirrors(context.Context, string) ([]SlackSessionMirror, error) {
	return nil, nil
}
func (f *fakeSlackSettings) Attach(context.Context, string, string, string) error { return nil }
func (f *fakeSlackSettings) Detach(context.Context, string, string, string) error { return nil }

// fltPost submits a form through the full handler stack.
func fltPost(t *testing.T, s *Server, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func newSettingsServer(t *testing.T, fs *fakeSlackSettings) *Server {
	t.Helper()
	f := newFake()
	s2, err := New(Options{
		Data:    f,
		Viewer:  func(r *http.Request) (Viewer, bool) { return Viewer{Email: "ana@example.org"}, true },
		CSRFKey: []byte("test-key-for-signing-form-tokens"),
		Slack:   fs,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s2
}

// The settings page must actually RENDER, end to end through the template
// registry. The page first shipped with its handler wired and its template
// missing from the renderer's explicit list, and every unit test passed while
// production answered "template not found" — only a test that fetches the
// page catches that class.
func TestSettingsPageRenders(t *testing.T) {
	fs := &fakeSlackSettings{view: SlackSettingsView{
		Groups: []SlackGroup{{ID: 1, Name: "eng", OwnerEmail: "ana@example.org",
			Visibility: "private", Destination: "C1ENG12345"}},
		DestLabels: map[string]string{"C1ENG12345": "#eng-pod"},
		DigestMode: "off",
	}}
	s2 := newSettingsServer(t, fs)
	rec := fltGet(t, s2, "/settings/notifications")
	if rec.Code != 200 {
		t.Fatalf("settings page answered %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		"Notifications", "eng", "C1ENG12345", "Create a group", "csrf",
		// The whole directory rides in the two picker panels, which combo.js
		// prunes as the person types. data-fill is what tells it these feed a
		// form field instead of navigating, since this is a POST form.
		`class="combo" data-fill`,
		`>#eng-pod</a>`, `>#ops</a>`,
		// The person option says the real name so it is searchable, and carries
		// the handle alone in data-value so that is what lands in the field.
		`data-value="@morgan"`, "Morgan Kim",
		// Every option is still a real link: with the script blocked the click
		// round-trips here and the field arrives filled. Keys are sorted
		// because the href is built with url.Values.Encode.
		`href="/settings/notifications?channel_pick=%23eng-pod&amp;dest_mode=channel`,
		`href="/settings/notifications?dest_mode=person&amp;person_pick=%40morgan`,
		// Groups decorate their raw ids with the human spelling.
		"#eng-pod <code>C1ENG12345</code>",
		// One-click controls, no separate Save.
		"Pause all live mirroring", "Delete", "Share org-wide",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("settings page is missing %q", want)
		}
	}
	for _, gone := range []string{"default_group", "Save</button>", "chq",
		// The native popup this picker used between 2026-08-12 and 08-13. It
		// pruned but could not be themed; the combo above does both.
		"<datalist", "list="} {
		if strings.Contains(body, gone) {
			t.Errorf("settings page still carries removed control %q", gone)
		}
	}

	// The scripted half of the 2026-08-13 policy split. This page is a filter
	// page, so it loads combo.js and says so in its own header.
	if csp := rec.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "script-src 'self'") {
		t.Errorf("settings CSP = %q, want same-origin script", csp)
	}
	if !strings.Contains(body, "/static/combo.js") {
		t.Error("the settings page carries 'self' but loads no combo.js")
	}
}

// TestDestinationPickerWorksWithoutTheScript is the fallback the picker's
// option links exist for.
//
// The two searches feed a POST field rather than navigating, so a click has to
// put text in a box, and putting text in a box is the one thing an <a href>
// cannot do on its own. The answer is that each option addresses this page with
// its own value: combo.js intercepts the click and fills the field directly,
// and a browser with the script blocked follows the link instead and gets the
// field filled by the server. Both routes end with the same string in the same
// input, which is what this test holds.
func TestDestinationPickerWorksWithoutTheScript(t *testing.T) {
	s2 := newSettingsServer(t, &fakeSlackSettings{view: SlackSettingsView{DigestMode: "off"}})

	for _, tc := range []struct {
		name   string
		target string
		want   []string
	}{
		{"a channel picked by link", "/settings/notifications?dest_mode=channel&channel_pick=%23eng-pod",
			[]string{`value="channel" checked`, `name="channel_pick" value="#eng-pod"`}},
		{"a person picked by link", "/settings/notifications?dest_mode=person&person_pick=%40morgan",
			[]string{`value="person" checked`, `name="person_pick" value="@morgan"`}},
		// No mode named, and an unknown one, both land on the DM. It is the
		// only destination that needs no directory lookup, so it is the only
		// one that cannot be wrong.
		{"no mode named", "/settings/notifications", []string{`value="dm" checked`}},
		{"an unknown mode", "/settings/notifications?dest_mode=carrier-pigeon", []string{`value="dm" checked`}},
		// The rest of the card rides along. Without this the pick answered by
		// clearing the form it was meant to fill in: the typed name went
		// blank and org visibility fell back to private, silently narrowing
		// what somebody had already chosen.
		{"the name survives the pick", "/settings/notifications?dest_mode=channel&channel_pick=%23ops&name=eng-sessions",
			[]string{`name="name" value="eng-sessions"`}},
		{"org visibility survives the pick", "/settings/notifications?dest_mode=channel&channel_pick=%23ops&visibility=org",
			[]string{`value="org" checked`}},
		// And an unreadable visibility narrows rather than widens.
		{"a mangled visibility falls back to private", "/settings/notifications?visibility=everyone",
			[]string{`value="private" checked`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := fltGet(t, s2, tc.target)
			if rec.Code != http.StatusOK {
				t.Fatalf("answered %d", rec.Code)
			}
			for _, want := range tc.want {
				if !strings.Contains(rec.Body.String(), want) {
					t.Errorf("the picker did not come back showing %q", want)
				}
			}
		})
	}

	// The option links carry the card forward too, or the next pick loses what
	// the last one preserved.
	body := fltGet(t, s2, "/settings/notifications?name=eng-sessions&visibility=org").Body.String()
	for _, want := range []string{"name=eng-sessions", "visibility=org"} {
		if !strings.Contains(body, want) {
			t.Errorf("the picker's own option links dropped %q", want)
		}
	}
}

// TestAMisspeltDestinationSendsTheCardBack is the producer for all of the
// above.
//
// Everything that reads these parameters was landed before anything wrote
// them, so the fallback machinery type-checked, passed its own tests against
// hand-written URLs, and could not be reached by a person using the page.
// Every settings form POSTs and every other redirect goes to the bare path, so
// this is the one response that carries the card: a create whose destination
// did not resolve, which is exactly the case where somebody has to fill the
// form in again and should not have to retype it.
func TestAMisspeltDestinationSendsTheCardBack(t *testing.T) {
	fs := &fakeSlackSettings{view: SlackSettingsView{DigestMode: "off"}}
	s2 := newSettingsServer(t, fs)

	rec := fltPost(t, s2, "/settings/notifications/groups", url.Values{
		"csrf":         {s2.csrfToken(Viewer{Email: "ana@example.org"})},
		"name":         {"eng-sessions"},
		"visibility":   {"org"},
		"dest_mode":    {"channel"},
		"channel_pick": {"#eng-pdo"},
	})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("answered %d, want a redirect back to the form", rec.Code)
	}
	loc := rec.Header().Get("Location")
	back, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("Location %q does not parse: %v", loc, err)
	}
	q := back.Query()
	for key, want := range map[string]string{
		"name":         "eng-sessions",
		"visibility":   "org",
		"dest_mode":    "channel",
		"channel_pick": "#eng-pdo",
	} {
		if got := q.Get(key); got != want {
			t.Errorf("the redirect dropped %s: got %q, want %q", key, got, want)
		}
	}
	// And the flash still arrives, which is what the extra parameters must not
	// have displaced: redirectOutcome appends rather than assuming it is first.
	if q.Get("err") == "" {
		t.Errorf("the redirect carries no failure message: %s", loc)
	}
	if !strings.Contains(q.Get("err"), "eng-pdo") {
		t.Errorf("the message does not name the spelling that failed: %q", q.Get("err"))
	}

	// Following it renders the card as it was submitted.
	body := fltGet(t, s2, loc).Body.String()
	for _, want := range []string{
		`name="name" value="eng-sessions"`,
		`value="org" checked`,
		`value="channel" checked`,
		`name="channel_pick" value="#eng-pdo"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the returned card is missing %q", want)
		}
	}
}

// A picked value is echoed into a form field, so it is the one place on this
// page where something from the query string reaches the HTML. It must arrive
// as text.
func TestDestinationPickEchoIsEscaped(t *testing.T) {
	s2 := newSettingsServer(t, &fakeSlackSettings{view: SlackSettingsView{DigestMode: "off"}})

	rec := fltGet(t, s2, "/settings/notifications?dest_mode=channel&channel_pick="+
		url.QueryEscape(`"><script>alert(1)</script>`))
	body := rec.Body.String()
	if strings.Contains(body, "<script>alert(1)</script>") {
		t.Error("a query value reached the page as live markup")
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Error("the escaped value is not present at all")
	}
}

// The create form resolves picker submissions server-side: a picked channel
// name becomes its id, a person their user id, and an unknown spelling comes
// back as a flash naming it rather than a stored garbage destination.
func TestCreateGroupResolvesDestinations(t *testing.T) {
	fs := &fakeSlackSettings{view: SlackSettingsView{DigestMode: "off"}}
	s2 := newSettingsServer(t, fs)
	csrf := s2.csrfToken(Viewer{Email: "ana@example.org"})

	for _, tc := range []struct {
		form url.Values
		want string
	}{
		{url.Values{"name": {"a"}, "visibility": {"private"}, "dest_mode": {"dm"}}, "a->dm"},
		{url.Values{"name": {"b"}, "visibility": {"private"}, "dest_mode": {"channel"}, "channel_pick": {"#eng-pod"}}, "b->C1ENG12345"},
		{url.Values{"name": {"c"}, "visibility": {"org"}, "dest_mode": {"person"}, "person_pick": {"@morgan"}}, "c->U1MORGAN23"},
	} {
		tc.form.Set("csrf", csrf)
		rec := fltPost(t, s2, "/settings/notifications/groups", tc.form)
		if rec.Code != http.StatusSeeOther {
			t.Fatalf("create answered %d", rec.Code)
		}
	}
	if got := strings.Join(fs.created, " "); got != "a->dm b->C1ENG12345 c->U1MORGAN23" {
		t.Errorf("created = %s", got)
	}

	form := url.Values{"name": {"d"}, "visibility": {"private"}, "dest_mode": {"channel"},
		"channel_pick": {"#nope"}, "csrf": {csrf}}
	rec := fltPost(t, s2, "/settings/notifications/groups", form)
	loc := rec.Header().Get("Location")
	if !strings.Contains(loc, "err=") || !strings.Contains(loc, "nope") {
		t.Errorf("an unknown channel did not flash its spelling back: %s", loc)
	}
	if len(fs.created) != 3 {
		t.Errorf("an unresolvable destination still created a group")
	}
}

// Delete and the one-click kill land on the port with no ceremony.
func TestGroupDeleteAndLiveToggle(t *testing.T) {
	fs := &fakeSlackSettings{view: SlackSettingsView{DigestMode: "off"}}
	s2 := newSettingsServer(t, fs)
	csrf := s2.csrfToken(Viewer{Email: "ana@example.org"})

	rec := fltPost(t, s2, "/settings/notifications/groups/7/delete", url.Values{"csrf": {csrf}})
	if rec.Code != http.StatusSeeOther || len(fs.deleted) != 1 || fs.deleted[0] != 7 {
		t.Fatalf("delete: code %d, deleted %v", rec.Code, fs.deleted)
	}
	rec = fltPost(t, s2, "/settings/notifications/controls", url.Values{"csrf": {csrf}, "live": {"pause"}})
	if rec.Code != http.StatusSeeOther || len(fs.killed) != 1 || !fs.killed[0] {
		t.Fatalf("pause: code %d, killed %v", rec.Code, fs.killed)
	}
	rec = fltPost(t, s2, "/settings/notifications/controls", url.Values{"csrf": {csrf}, "live": {"resume"}})
	if rec.Code != http.StatusSeeOther || len(fs.killed) != 2 || fs.killed[1] {
		t.Fatalf("resume: code %d, killed %v", rec.Code, fs.killed)
	}
}
