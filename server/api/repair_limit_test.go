package api

import (
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestSimultaneousRepairAsksAreAnsweredOnce is the adversarial pass's F2.
// The hour was charged after the answer, so twenty requests that all passed
// the check before the first answer landed were all served and all ran the
// store query. The slot is reserved before the work and given back on every
// path that is not an answer, so exactly one of them is answered.
func TestSimultaneousRepairAsksAreAnsweredOnce(t *testing.T) {
	now := testNow
	devices := &fakeDevices{tokens: map[string]DeviceIdentity{"lsd_a": {DeviceID: "d1", Email: "a@example.com"}}}
	repair := &fakeRepair{lists: map[string][]RepairEntry{}}
	h := newRepairHandler(t, devices, repair, &now)

	const n = 20
	codes := make([]int, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			codes[i] = repairGet(h, "lsd_a", "").Code
		}(i)
	}
	close(start)
	wg.Wait()
	ok, limited := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			limited++
		}
	}
	if ok != 1 || limited != n-1 {
		t.Errorf("served 200 to %d of %d simultaneous requests and 429 to %d; want one answer and the rest limited", ok, n, limited)
	}
	if repair.calls != 1 {
		t.Errorf("the store list was called %d times, want once", repair.calls)
	}
	// The one answer spent the hour; the next ask is limited, and the one
	// after the hour is answered.
	if w := repairGet(h, "lsd_a", ""); w.Code != http.StatusTooManyRequests {
		t.Errorf("the ask after the burst answered %d, want 429", w.Code)
	}
	now = now.Add(repairEvery + time.Second)
	if w := repairGet(h, "lsd_a", ""); w.Code != http.StatusOK {
		t.Errorf("the ask an hour later answered %d, want 200", w.Code)
	}
}
