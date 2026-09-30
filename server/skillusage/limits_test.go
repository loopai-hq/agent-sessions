package skillusage

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestShedEvictsTheLeastRecentlyTouchedBucketAndResetsPerMinute(t *testing.T) {
	s := newShed(2, 2)
	now := testNow
	if !s.allow("a", now) || !s.allow("b", now) || s.size() != 2 {
		t.Fatal("two keys fit")
	}
	// Touching a makes b the oldest; c evicts b.
	s.allow("a", now)
	s.allow("c", now)
	if s.size() != 2 {
		t.Errorf("size = %d, want 2", s.size())
	}
	if _, ok := s.seen["b"]; ok {
		t.Error("b survived the eviction; a, touched later, should have")
	}
	if _, ok := s.seen["a"]; !ok {
		t.Error("a was evicted")
	}
	// a has been counted twice: over the allowance of 2 on the third.
	if s.allow("a", now) {
		t.Error("a third call in the minute was allowed")
	}
	if !s.allow("a", now.Add(time.Minute)) {
		t.Error("the next minute was refused")
	}
}

func TestBucketLimiterRefillsAndSweeps(t *testing.T) {
	l := newBucketLimiter(2, time.Second)
	now := testNow
	if !l.allow("d", now) || !l.allow("d", now) || l.allow("d", now) {
		t.Fatal("burst of two, then refused")
	}
	if !l.allow("d", now.Add(time.Second)) {
		t.Error("one second refills one")
	}
	for i := range bucketSweepAt {
		l.allow(string(rune('A'+i%26))+string(rune(i)), now)
	}
	before := len(l.seen)
	l.allow("new", now.Add(time.Hour))
	if len(l.seen) >= before {
		t.Errorf("a new key at the sweep size did not drop full buckets: %d then %d", before, len(l.seen))
	}
}

// TestClientAddrPrefersTheForwardedHop: the hop the front end appended is
// the last one, so a client-supplied prefix (the spoof that would give
// every request its own bucket) is skipped, as are empty hops and lines.
func TestClientAddrPrefersTheForwardedHop(t *testing.T) {
	r := httptest.NewRequest("POST", InvocationsPath, nil)
	r.RemoteAddr = "10.1.1.1:4444"
	if got := clientAddr(r); got != "10.1.1.1" {
		t.Errorf("no header: %q", got)
	}
	r.Header.Set("X-Forwarded-For", " 203.0.113.7 , 169.254.1.1")
	if got := clientAddr(r); got != "169.254.1.1" {
		t.Errorf("forwarded: %q", got)
	}
	r.Header.Set("X-Forwarded-For", "spoofed-1, spoofed-2, 198.51.100.9, ")
	if got := clientAddr(r); got != "198.51.100.9" {
		t.Errorf("a client prefix and a trailing empty hop: %q", got)
	}
	r.Header.Add("X-Forwarded-For", "198.51.100.10")
	if got := clientAddr(r); got != "198.51.100.10" {
		t.Errorf("a second header line: %q", got)
	}
	r.Header.Set("X-Forwarded-For", " , ")
	if got := clientAddr(r); got != "10.1.1.1" {
		t.Errorf("an empty header falls back to the connection: %q", got)
	}
	r.RemoteAddr = "bare"
	r.Header.Del("X-Forwarded-For")
	if got := clientAddr(r); got != "bare" {
		t.Errorf("unparseable remote: %q", got)
	}
}
