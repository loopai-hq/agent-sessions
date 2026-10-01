package slack

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestAFailureInsideATwoHundredIsAFailure is the single most important property
// of this client.
//
// Slack reports refusals inside a 200 body as a matter of course. A client that
// only checked the status code would report every one of these as a success,
// the mirror would settle the outbox row, and the message would be recorded as
// posted having never been sent — silently, permanently, and with no way to
// find out afterwards which sessions were lost.
func TestAFailureInsideATwoHundredIsAFailure(t *testing.T) {
	fs := newFakeSlack(t)
	fs.set(func(f *fakeSlack) { f.postErr = "channel_not_found" })

	_, err := fs.client(t).PostMessage(context.Background(), "C123", "hello")
	if err == nil {
		t.Fatal("a body of ok:false inside HTTP 200 was reported as a success")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is not an *APIError: %v", err)
	}
	// Slack's own string, verbatim: rewording it would make the log
	// un-greppable against Slack's documentation.
	if apiErr.Code != "channel_not_found" {
		t.Errorf("code = %q, want Slack's own word for it", apiErr.Code)
	}
}

// TestARateLimitIsWaitedOutRatherThanDropped. Slack answers 429 with a
// Retry-After; a client that gave up would lose the message, and one that
// retried immediately would earn a longer ban.
func TestARateLimitIsWaitedOutRatherThanDropped(t *testing.T) {
	fs := newFakeSlack(t)
	var attempts int
	var mu sync.Mutex
	fs.set(func(f *fakeSlack) {
		f.reply["chat.postMessage"] = func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			attempts++
			n := attempts
			mu.Unlock()
			if n == 1 {
				w.Header().Set("Retry-After", "3")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			writeSlackJSON(w, map[string]any{"ok": true, "ts": "1.000200"})
		}
	})

	var slept []time.Duration
	c, err := NewClient(ClientOptions{
		Token:   "xoxb-fake",
		BaseURL: fs.srv.URL,
		Now:     func() time.Time { return time.Time{} },
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ts, err := c.PostMessage(context.Background(), "C123", "hello")
	if err != nil {
		t.Fatalf("a rate-limited post was not retried: %v", err)
	}
	if ts != "1.000200" {
		t.Errorf("ts = %q, want the id from the successful attempt", ts)
	}
	if len(slept) != 1 || slept[0] != 3*time.Second {
		t.Errorf("waited %v, want exactly the 3s Slack asked for", slept)
	}
}

// TestRetryAfterIsBoundedAndNeverZero. Zero would mean "not a rate limit" to
// the caller, turning the one error that must be retried into the one that is
// not; and an unbounded wait would hold the whole pass, which is the only thing
// draining the outbox.
func TestRetryAfterIsBoundedAndNeverZero(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{"the value Slack sent", "5", 5 * time.Second},
		{"an absent header still means retry", "", retryAfterDefault},
		{"a header that is not a number still means retry", "soon", retryAfterDefault},
		{"a negative value still means retry", "-1", retryAfterDefault},
		{"a zero value still means retry", "0", retryAfterDefault},
		{"an hour is clipped to the cap", "3600", retryAfterCap},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryAfter(tc.header); got != tc.want {
				t.Errorf("retryAfter(%q) = %v, want %v", tc.header, got, tc.want)
			}
			if got := retryAfter(tc.header); got <= 0 {
				t.Error("a zero wait would be read as 'not a rate limit'")
			}
		})
	}
}

// TestARateLimitReportedInsideATwoHundredIsAlsoWaitedOut. That shape carries no
// header and no number, so the wait is ours to choose; treating it as a
// permanent failure would drop the message.
func TestARateLimitReportedInsideATwoHundredIsAlsoWaitedOut(t *testing.T) {
	fs := newFakeSlack(t)
	var attempts int
	var mu sync.Mutex
	fs.set(func(f *fakeSlack) {
		f.reply["chat.postMessage"] = func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			attempts++
			n := attempts
			mu.Unlock()
			if n == 1 {
				writeSlackJSON(w, map[string]any{"ok": false, "error": "ratelimited"})
				return
			}
			writeSlackJSON(w, map[string]any{"ok": true, "ts": "1.000300"})
		}
	})

	var slept []time.Duration
	c, _ := NewClient(ClientOptions{
		Token: "xoxb-fake", BaseURL: fs.srv.URL,
		Now:   func() time.Time { return time.Time{} },
		Sleep: func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil },
	})
	if _, err := c.PostMessage(context.Background(), "C123", "hello"); err != nil {
		t.Fatalf("an ok:false ratelimited body was not retried: %v", err)
	}
	if len(slept) != 1 {
		t.Errorf("waited %v times, want once", len(slept))
	}
}

// TestAPersistentRateLimitGivesUpAndSaysWhy. The outbox retries on a later
// pass, which is a slower retry against a service that just said it wants less
// traffic; spinning here would be the opposite.
func TestAPersistentRateLimitGivesUpAndSaysWhy(t *testing.T) {
	fs := newFakeSlack(t)
	var attempts int
	var mu sync.Mutex
	fs.set(func(f *fakeSlack) {
		f.reply["chat.postMessage"] = func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			attempts++
			mu.Unlock()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		}
	})

	c, _ := NewClient(ClientOptions{
		Token: "xoxb-fake", BaseURL: fs.srv.URL,
		Now:   func() time.Time { return time.Time{} },
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
	_, err := c.PostMessage(context.Background(), "C123", "hello")
	if err == nil {
		t.Fatal("a permanently rate-limited post reported success")
	}
	if !strings.Contains(err.Error(), "rate limited") {
		t.Errorf("the failure does not say it was a rate limit: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != maxAttempts {
		t.Errorf("made %d attempts, want the bounded %d", attempts, maxAttempts)
	}
}

// TestAShutdownEndsAWaitRatherThanOutlivingIt. A drain that has to wait out a
// Retry-After is a drain that overruns Cloud Run's ten-second grace.
func TestAShutdownEndsAWaitRatherThanOutlivingIt(t *testing.T) {
	fs := newFakeSlack(t)
	fs.set(func(f *fakeSlack) {
		f.reply["chat.postMessage"] = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(http.StatusTooManyRequests)
		}
	})

	// The real sleep, so cancellation is what actually ends this.
	c, _ := NewClient(ClientOptions{Token: "xoxb-fake", BaseURL: fs.srv.URL})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	if _, err := c.PostMessage(ctx, "C123", "hello"); err == nil {
		t.Fatal("a cancelled post reported success")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the wait outlived its context by %v", elapsed)
	}
}

// TestTheTokenTravelsInAHeaderAndTheMessageAsAForm. The token is not in the
// body, where a proxy log would keep it.
func TestTheTokenTravelsInAHeaderAndTheMessageAsAForm(t *testing.T) {
	fs := newFakeSlack(t)
	if _, err := fs.client(t).PostMessage(context.Background(), "C123", "hello"); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	calls := fs.snapshot()
	if len(calls) != 1 {
		t.Fatalf("made %d calls, want 1", len(calls))
	}
	if calls[0].Token != "xoxb-fake-token" {
		t.Errorf("Authorization carried %q", calls[0].Token)
	}
	if calls[0].Text != "hello" || calls[0].Channel != "C123" {
		t.Errorf("the message did not arrive as sent: %+v", calls[0])
	}
}

// TestALinkInAMessageIsNotUnfurled. An unfurl would put the dashboard's own
// page title into the channel, which is content this package never posts.
func TestALinkInAMessageIsNotUnfurled(t *testing.T) {
	fs := newFakeSlack(t)
	var form url.Values
	fs.set(func(f *fakeSlack) {
		f.reply["chat.postMessage"] = func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			form = r.PostForm
			writeSlackJSON(w, map[string]any{"ok": true, "ts": "1.1"})
		}
	})
	if _, err := fs.client(t).PostMessage(context.Background(), "C1", "x"); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	for _, k := range []string{"unfurl_links", "unfurl_media"} {
		if got := form.Get(k); got != "false" {
			t.Errorf("%s = %q, want false", k, got)
		}
	}
	// The link is the whole point of the message, and Slack only renders one
	// inside a text field when mrkdwn is on.
	if got := form.Get("mrkdwn"); got != "true" {
		t.Errorf("mrkdwn = %q, want true", got)
	}
}

// TestAnAddressSlackDoesNotKnowIsItsOwnKindOfFailure. It is the only failure
// here a person can act on themselves, and the preference route reports it to
// them at the moment they ask rather than into a log nobody reads.
func TestAnAddressSlackDoesNotKnowIsItsOwnKindOfFailure(t *testing.T) {
	for _, code := range []string{"users_not_found", "user_not_found"} {
		t.Run(code, func(t *testing.T) {
			fs := newFakeSlack(t)
			fs.set(func(f *fakeSlack) { f.lookupErr = code })

			_, err := fs.client(t).LookupUserByEmail(context.Background(), "ghost@example.org")
			if !errors.Is(err, ErrUserNotFound) {
				t.Fatalf("error is not ErrUserNotFound: %v", err)
			}
			// Permanent too, so the mirror stops retrying a DM to somebody Slack
			// has never heard of every minute forever.
			if !errors.Is(err, ErrUserNotFound) {
				t.Error("a missing account should not be retried indefinitely")
			}
			if !strings.Contains(err.Error(), "ghost@example.org") {
				t.Errorf("the failure does not name the address it looked for: %v", err)
			}
		})
	}
}

// TestAnEmptyUserIsNotAUser. Slack answering ok:true with no id would otherwise
// become a DM opened against an empty string.
func TestAnEmptyUserIsNotAUser(t *testing.T) {
	fs := newFakeSlack(t)
	fs.set(func(f *fakeSlack) {
		f.reply["users.lookupByEmail"] = func(w http.ResponseWriter, r *http.Request) {
			writeSlackJSON(w, map[string]any{"ok": true, "user": map[string]any{"id": ""}})
		}
	})
	if _, err := fs.client(t).LookupUserByEmail(context.Background(), "a@example.org"); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("an ok:true with no id was accepted: %v", err)
	}
}

// TestANonTwoHundredIsAFailureWithItsStatusKept. A 500 from Slack's edge
// carries no JSON body at all, so the status is the only thing to report.
func TestANonTwoHundredIsAFailureWithItsStatusKept(t *testing.T) {
	fs := newFakeSlack(t)
	fs.set(func(f *fakeSlack) {
		f.reply["chat.postMessage"] = func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("<html>upstream</html>"))
		}
	})

	_, err := fs.client(t).PostMessage(context.Background(), "C1", "x")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is not an *APIError: %v", err)
	}
	if apiErr.Status != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", apiErr.Status)
	}
	// A 502 is transient: the same message will be accepted shortly.
	if errors.Is(err, ErrPermanent) {
		t.Error("a 502 was classified as permanent, which would drop the message")
	}
}

// TestPostsArePacedSoThisPosterIsNotWhyTheLimitIsReached. Slack allows roughly
// one message per second per channel; the pacing is what keeps this client from
// being the traffic that trips it.
func TestPostsArePacedSoThisPosterIsNotWhyTheLimitIsReached(t *testing.T) {
	fs := newFakeSlack(t)
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	var slept []time.Duration
	c, _ := NewClient(ClientOptions{
		Token: "xoxb-fake", BaseURL: fs.srv.URL,
		Now: func() time.Time { return now },
		Sleep: func(_ context.Context, d time.Duration) error {
			slept = append(slept, d)
			return nil
		},
	})

	for i := range 3 {
		if _, err := c.PostMessage(context.Background(), "C1", "x"); err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}
	// The clock never advances, so every gap after the first must be waited
	// out in full.
	if len(slept) != 2 {
		t.Fatalf("waited %d times for 3 posts, want 2", len(slept))
	}
	for i, d := range slept {
		if d != postInterval {
			t.Errorf("gap %d was %v, want %v", i, d, postInterval)
		}
	}
}

// TestTheFirstPostIsNotDelayed. A mirror that slept before its very first
// message would add a second to every pass that had one thing to say.
func TestTheFirstPostIsNotDelayed(t *testing.T) {
	fs := newFakeSlack(t)
	var slept int
	c, _ := NewClient(ClientOptions{
		Token: "xoxb-fake", BaseURL: fs.srv.URL,
		Now:   time.Now,
		Sleep: func(context.Context, time.Duration) error { slept++; return nil },
	})
	if _, err := c.PostMessage(context.Background(), "C1", "x"); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if slept != 0 {
		t.Errorf("the first post waited %d times", slept)
	}
}

// TestAnUnreachableSlackIsATransportFailureRatherThanARefusal. It must not be
// permanent: the outbox has to retry it on a later pass.
func TestAnUnreachableSlackIsATransportFailureRatherThanARefusal(t *testing.T) {
	c, _ := NewClient(ClientOptions{
		Token: "xoxb-fake",
		// A port nothing is listening on.
		BaseURL: "http://127.0.0.1:1",
		Sleep:   func(context.Context, time.Duration) error { return nil },
	})
	_, err := c.PostMessage(context.Background(), "C1", "x")
	if err == nil {
		t.Fatal("a post to an unreachable host reported success")
	}
	if errors.Is(err, ErrPermanent) {
		t.Error("a transport failure was classified as permanent, which would drop the message")
	}
}
