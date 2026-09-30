package slack

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A payload signed with the wrong secret, or an aged one, is refused with one
// indistinguishable answer; a good one for somebody else's session detaches
// nothing.
func TestInteractiveVerifiesAndScopes(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	body := "payload=" + url.QueryEscape(`{"type":"block_actions","user":{"id":"U1"},"actions":[{"action_id":"stop_mirror","value":"s-1|3"}]}`)
	ts := strconv.FormatInt(now.Unix(), 10)

	sign := func(secret string) string {
		mac := hmac.New(sha256.New, []byte(secret))
		fmt.Fprintf(mac, "v0:%s:%s", ts, body)
		return "v0=" + hex.EncodeToString(mac.Sum(nil))
	}

	req := httptest.NewRequest("POST", InteractivePath, strings.NewReader(body))
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", sign("right-secret"))
	if !verifySlackSignature("right-secret", req, []byte(body), now) {
		t.Fatal("a correctly signed payload was refused")
	}
	if verifySlackSignature("other-secret", req, []byte(body), now) {
		t.Fatal("a payload signed with the wrong secret verified")
	}
	if verifySlackSignature("right-secret", req, []byte(body), now.Add(10*time.Minute)) {
		t.Fatal("a replayed payload outside the window verified")
	}
}
