package slack

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestRunPostsWithoutAnybodyCallingPass is the property this package exists to
// have and the one its whole design can satisfy on paper while failing in
// production: the loop is a caller.
//
// Six components in this repository were written, unit-tested and never
// invoked. Every one of them passed a test that called it directly. This test
// deliberately does not call pass: it starts Run, which is what the composition
// root starts, and waits for a message to arrive at Slack. If Run's timer, its
// select or its call into pass were wrong, every other test in this file would
// still pass and nothing would ever be posted.
func TestRunPostsWithoutAnybodyCallingPass(t *testing.T) {
	fs := newFakeSlack(t)
	db := newFakeDB()
	db.due = []candidate{sampleCandidate("s-run", "ana@example.org", ModeChannel)}

	m := testMirror(t, db, fs.client(t), func(o *Options) {
		o.Interval = time.Millisecond
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Run(ctx)
	}()

	waitFor(t, func() bool { return len(fs.posts()) > 0 }, "Run never posted anything")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	posts := fs.posts()
	if posts[0].Channel != "C123" {
		t.Errorf("posted to %q, want the channel the preference names", posts[0].Channel)
	}
	if !strings.Contains(posts[0].Text, "s-run") {
		t.Errorf("the message does not link to the session it is about: %q", posts[0].Text)
	}
}

// TestRunStopsWhenItsContextIsCancelled defends the drain. Serve derives the
// mirror's context from its own and waits for this goroutine before returning,
// so a Run that ignored cancellation would hold a shutdown open past Cloud
// Run's ten seconds and be killed with the pool still in use.
func TestRunStopsWhenItsContextIsCancelled(t *testing.T) {
	fs := newFakeSlack(t)
	m := testMirror(t, newFakeDB(), fs.client(t), func(o *Options) {
		o.Interval = time.Hour // Never fires; only cancellation can end this.
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Run(ctx)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run outlived its context; a drain would hang here")
	}
}

// TestASessionIsPostedOnceHoweverManyPassesRun is the idempotency guarantee.
// chat.postMessage has no idempotency key, so a retry that reposts is a
// duplicate in somebody's channel with no way to take it back.
func TestASessionIsPostedOnceHoweverManyPassesRun(t *testing.T) {
	fs := newFakeSlack(t)
	db := newFakeDB()
	db.due = []candidate{sampleCandidate("s-once", "ana@example.org", ModeChannel)}

	m := testMirror(t, db, fs.client(t), nil)
	for i := range 4 {
		if _, err := m.pass(context.Background()); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}

	if got := len(fs.posts()); got != 1 {
		t.Fatalf("four passes over the same due session posted %d messages, want exactly 1", got)
	}
	// The second pass must report the session as skipped rather than posted, so
	// the log distinguishes "already said" from "said again".
	p, err := m.pass(context.Background())
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if p.Posted != 0 || p.Skipped != 1 {
		t.Errorf("a pass over an already-posted session reported %+v, want 0 posted and 1 skipped", p)
	}
}

// TestTheDailyCeilingHoldsAndSaysSoOnce defends the two halves of the cap
// together. A feed that simply stops is indistinguishable from one that broke,
// so the ceiling has to be visible; and a notice that repeated per held-back
// session would be the firehose the ceiling exists to prevent.
func TestTheDailyCeilingHoldsAndSaysSoOnce(t *testing.T) {
	fs := newFakeSlack(t)
	db := newFakeDB()
	db.due = []candidate{
		sampleCandidate("s-1", "ana@example.org", ModeChannel),
		sampleCandidate("s-2", "ana@example.org", ModeChannel),
		sampleCandidate("s-3", "ana@example.org", ModeChannel),
	}

	m := testMirror(t, db, fs.client(t), func(o *Options) { o.DailyCap = 1 })
	p, err := m.pass(context.Background())
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if p.Posted != 1 {
		t.Errorf("posted %d sessions against a ceiling of 1", p.Posted)
	}
	if p.Capped != 2 {
		t.Errorf("held back %d sessions, want the 2 above the ceiling", p.Capped)
	}

	var notices int
	for _, c := range fs.posts() {
		if strings.Contains(c.Text, "ceiling") {
			notices++
		}
	}
	if notices != 1 {
		t.Errorf("sent %d ceiling notices for one person on one day, want exactly 1", notices)
	}
	if got := len(fs.posts()); got != 2 {
		t.Errorf("sent %d messages, want 1 summary and 1 notice", got)
	}
}

// TestFailuresAreClassifiedByWhetherRetryingCouldHelp is the distinction that
// keeps a small mistake from becoming the reason the bot is rate-limited. A
// channel that does not exist will never accept a message; a 503 will.
func TestFailuresAreClassifiedByWhetherRetryingCouldHelp(t *testing.T) {
	tests := []struct {
		name string
		code string
		// wantAttempts is what the outbox should hold afterwards: a permanent
		// failure spends the whole budget at once rather than waiting three
		// passes to discover the same answer.
		wantSpent bool
	}{
		{"a channel that does not exist is never retried", "channel_not_found", true},
		{"a bot that was removed from the workspace is never retried", "account_inactive", true},
		{"a message too long to accept is never retried", "msg_too_long", true},
		{"an overloaded Slack is retried", "service_unavailable", false},
		{"an internal Slack error is retried", "internal_error", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeSlack(t)
			fs.set(func(f *fakeSlack) { f.postErr = tc.code })
			db := newFakeDB()
			db.due = []candidate{sampleCandidate("s-fail", "ana@example.org", ModeChannel)}

			m := testMirror(t, db, fs.client(t), func(o *Options) { o.MaxAttempts = 3 })
			p, err := m.pass(context.Background())
			if err != nil {
				t.Fatalf("pass: %v", err)
			}
			if p.Failed != 1 {
				t.Fatalf("pass reported %+v, want one failure", p)
			}

			db.mu.Lock()
			attempts := db.attempts[sessionKey("s-fail")]
			db.mu.Unlock()
			if spent := attempts >= 3; spent != tc.wantSpent {
				t.Errorf("after one failure the key holds %d attempts (budget spent: %v), want spent=%v",
					attempts, spent, tc.wantSpent)
			}
		})
	}
}

// TestAChannelModePreferenceWithNoChannelIsRefusedRatherThanRedirected. A
// preference that named a channel and posts somewhere else is worse than one
// that posts nowhere: the person believes their work is going to one audience
// and it is going to another.
func TestAChannelModePreferenceWithNoChannelIsRefusedRatherThanRedirected(t *testing.T) {
	fs := newFakeSlack(t)
	db := newFakeDB()
	c := sampleCandidate("s-nochan", "ana@example.org", ModeChannel)
	c.Channel = ""
	db.due = []candidate{c}

	m := testMirror(t, db, fs.client(t), nil)
	p, err := m.pass(context.Background())
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if p.Failed != 1 {
		t.Errorf("pass reported %+v, want one failure", p)
	}
	if got := len(fs.posts()); got != 0 {
		t.Fatalf("posted %d messages for a preference with no destination, want none", got)
	}
}

// TestAPublicChannelIsJoinedOnlyWhenSlackSaysItMustBe. Joining is visible to
// everybody in the channel, so the bot appears where somebody has actually
// pointed their mirror rather than everywhere at startup.
func TestAPublicChannelIsJoinedOnlyWhenSlackSaysItMustBe(t *testing.T) {
	fs := newFakeSlack(t)
	fs.set(func(f *fakeSlack) { f.needJoin["C123"] = true })
	db := newFakeDB()
	db.due = []candidate{sampleCandidate("s-join", "ana@example.org", ModeChannel)}

	m := testMirror(t, db, fs.client(t), nil)
	if _, err := m.pass(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}

	fs.mu.Lock()
	joined := append([]string(nil), fs.joined...)
	fs.mu.Unlock()
	if len(joined) != 1 || joined[0] != "C123" {
		t.Fatalf("joined %v, want exactly the channel that refused the message", joined)
	}
	if got := len(fs.posts()); got != 2 {
		t.Errorf("made %d post attempts, want the refusal and the retry after joining", got)
	}
	if fs.posts()[1].Channel != "C123" {
		t.Error("the retry after joining went somewhere else")
	}
}

// TestADMResolvesTheSlackAccountAndCachesIt. The lookup is the one call here
// that can fail for a reason the person can act on, and repeating it for every
// session would be a rate limit of its own.
func TestADMResolvesTheSlackAccountAndCachesIt(t *testing.T) {
	fs := newFakeSlack(t)
	db := newFakeDB()
	db.prefs["ana@example.org"] = Prefs{Email: "ana@example.org", Mode: ModeDM}
	db.due = []candidate{sampleCandidate("s-dm", "ana@example.org", ModeDM)}

	m := testMirror(t, db, fs.client(t), nil)
	if _, err := m.pass(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}

	posts := fs.posts()
	if len(posts) != 1 {
		t.Fatalf("posted %d messages, want 1", len(posts))
	}
	if !strings.HasPrefix(posts[0].Channel, "D") {
		t.Errorf("a DM went to %q, which is not a direct-message channel", posts[0].Channel)
	}
	// A DM is addressed to the person whose session it is, so it must not open
	// by naming them: that reads as a form letter about your own work.
	if strings.Contains(posts[0].Text, "ana@example.org") {
		t.Errorf("a DM names its own recipient: %q", posts[0].Text)
	}
	if db.issued("UPDATE slack_prefs SET slack_user_id") != 1 {
		t.Error("the resolved Slack id was not cached, so every future session pays for the lookup again")
	}
}

// TestOnePersonsFailureDoesNotStopAnotherPersonsMessage. The pass is a shared
// drain: stopping at the first bad destination would let one misconfigured
// preference hold up everybody's.
func TestOnePersonsFailureDoesNotStopAnotherPersonsMessage(t *testing.T) {
	fs := newFakeSlack(t)
	db := newFakeDB()
	broken := sampleCandidate("s-broken", "bo@example.org", ModeChannel)
	broken.Channel = "" // Refused before Slack is reached.
	db.due = []candidate{broken, sampleCandidate("s-fine", "ana@example.org", ModeChannel)}

	m := testMirror(t, db, fs.client(t), nil)
	p, err := m.pass(context.Background())
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if p.Failed != 1 || p.Posted != 1 {
		t.Errorf("pass reported %+v, want one failure and one post", p)
	}
	if got := len(fs.posts()); got != 1 || !strings.Contains(fs.posts()[0].Text, "s-fine") {
		t.Error("the working preference did not get its message")
	}
}

// TestAPassThatCannotReadTheDatabaseReportsItRatherThanPostingNothingQuietly.
// A mirror that swallowed this would be indistinguishable from one with nothing
// to do, which is the failure mode this whole feature is most likely to have.
func TestAPassThatCannotReadTheDatabaseReportsItRatherThanPostingNothingQuietly(t *testing.T) {
	fs := newFakeSlack(t)
	db := newFakeDB()
	db.failOn = "FROM sessions s"

	m := testMirror(t, db, fs.client(t), nil)
	_, err := m.pass(context.Background())
	if err == nil {
		t.Fatal("a pass whose eligibility query failed reported success")
	}
	if !strings.Contains(err.Error(), "slack:") {
		t.Errorf("the failure does not name this package: %v", err)
	}
}

// TestNewRefusesEveryDependencyItWouldOtherwiseDiscoverLate. The first pass
// happens a minute after a deploy with nobody watching, and its failure is one
// log line in the middle of ordinary traffic.
func TestNewRefusesEveryDependencyItWouldOtherwiseDiscoverLate(t *testing.T) {
	full := func() Options {
		return Options{
			DB:        newFakeDB(),
			Slack:     &Client{},
			PublicURL: "https://sessions.example.com",
			Viewer:    func(*http.Request) (string, bool) { return "", false },
		}
	}
	tests := []struct {
		name string
		drop func(*Options)
		want string
	}{
		{"no connection source", func(o *Options) { o.DB = nil }, "DB is required"},
		{"no slack client", func(o *Options) { o.Slack = nil }, "Slack client is required"},
		{"no dashboard url to link back to", func(o *Options) { o.PublicURL = "" }, "PublicURL is required"},
		{"no viewer, which would leave the preference route open", func(o *Options) { o.Viewer = nil }, "Viewer is required"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := full()
			tc.drop(&o)
			m, err := New(o)
			if err == nil {
				t.Fatal("New accepted a Mirror it could not run")
			}
			if m != nil {
				t.Error("New returned a Mirror alongside its failure")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name what was missing (%q)", err, tc.want)
			}
		})
	}
}

// TestNewClientRefusesAnEmptyToken. A mirror that started, polled, claimed a
// session and then discovered it had no credential has already consumed the
// claim; the session is then never posted at all.
func TestNewClientRefusesAnEmptyToken(t *testing.T) {
	for _, token := range []string{"", "   "} {
		c, err := NewClient(ClientOptions{Token: token})
		if err == nil {
			t.Fatalf("NewClient accepted %q", token)
		}
		if c != nil {
			t.Error("NewClient returned a Client alongside its failure")
		}
	}
}

// TestSummaryCarriesNoFieldThatCouldHoldTranscript is a structural guard rather
// than a behavioural one, and it is the most important test in this file.
//
// The rule this package enforces is that a Slack message carries metadata and
// never content, and the rule is enforced by Summary having nowhere to put
// content. sessions carries first_prompt and cwd; a first prompt is the obvious
// thing to title a message with, it is what the dashboard's own list shows, and
// adding it here is a one-line change that no behavioural test would catch,
// because the message would still render, still link and still be under the
// length limit.
//
// It would also be a permission bypass. The dashboard decides who may read a
// transcript and writes an audit row for every read of somebody else's; a Slack
// message is readable by whoever is in the channel now, whoever joins later,
// and every export of the workspace, with no record that anybody read it.
//
// So the field set is pinned. A failure here is not a broken test: it is the
// moment to re-read Summary's comment and decide whether the field really may
// leave the dashboard.
func TestSummaryCarriesNoFieldThatCouldHoldTranscript(t *testing.T) {
	want := []string{
		"Branch", "CostUSD", "Email", "Ended", "EndedAt", "Errors",
		"Repo", "SessionID", "StartedAt", "Subagents", "ToolCalls", "UserTurns",
	}
	var got []string
	ty := reflect.TypeOf(Summary{})
	for i := range ty.NumField() {
		got = append(got, ty.Field(i).Name)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Summary's fields are %v, want %v.\n"+
			"A new field here reaches a shared Slack channel, where the dashboard's "+
			"permission rules and audit log do not apply. Read Summary's comment before changing this list.", got, want)
	}
}

// TestPermanentIsTheConservativeDefaultForAnUnknownRefusal. Mistaking a
// transient failure for a permanent one drops one message; mistaking a
// permanent one for transient aims a retry loop at a service that said no.
func TestPermanentIsTheConservativeDefaultForAnUnknownRefusal(t *testing.T) {
	tests := []struct {
		name      string
		err       *APIError
		permanent bool
	}{
		{"a refusal Slack has never documented", &APIError{Code: "some_new_error"}, true},
		{"a channel that does not exist", &APIError{Code: "channel_not_found"}, true},
		{"an explicit rate limit", &APIError{Code: "ratelimited"}, false},
		{"a 429 with no body", &APIError{Status: http.StatusTooManyRequests}, false},
		{"a server error", &APIError{Status: http.StatusBadGateway}, false},
		{"a gateway timeout", &APIError{Status: http.StatusGatewayTimeout}, false},
		{"a bad request", &APIError{Status: http.StatusBadRequest, Code: "invalid_arguments"}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := errors.Is(tc.err, ErrPermanent); got != tc.permanent {
				t.Errorf("errors.Is(%v, ErrPermanent) = %v, want %v", tc.err, got, tc.permanent)
			}
		})
	}
}

// waitFor polls until cond holds or the test has waited long enough to be sure
// it never will.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

// TestAMessageThatWasSentIsCountedAsSentEvenIfRecordingItFailed.
//
// The narrow window this design accepts: Slack accepted the message and the row
// could not be settled, so the lease will expire and it will be posted a second
// time. Classifying it as a failure would send whoever reads the log looking
// for a message that did arrive, and would hide the duplicate that is coming.
func TestAMessageThatWasSentIsCountedAsSentEvenIfRecordingItFailed(t *testing.T) {
	fs := newFakeSlack(t)
	db := newFakeDB()
	db.due = []candidate{sampleCandidate("s-unrecorded", "ana@example.org", ModeChannel)}
	// Fails only the settle, which runs after Slack has already accepted.
	db.failOn = "SET posted_at"

	m := testMirror(t, db, fs.client(t), nil)
	p, err := m.pass(context.Background())
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if got := len(fs.posts()); got != 1 {
		t.Fatalf("posted %d messages, want 1", got)
	}
	if p.Posted != 1 || p.Failed != 0 {
		t.Errorf("pass reported %+v, want the message counted as posted", p)
	}
}
