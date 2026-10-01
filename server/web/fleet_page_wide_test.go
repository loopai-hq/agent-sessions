package web

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/server/fleet"
)

// TestFleetPageRendersAFleetWideRowWithoutAPersonAndMutesItUnderTheSentinel
// is the adversarial pass's F7. The missing-answer row is the fleet's: the
// page said "ask  to run" with nobody named, and its mute form posted an
// empty person the route refused. The row names the fleet, its command
// stands alone, and it mutes under the sentinel person "*".
func TestFleetPageRendersAFleetWideRowWithoutAPersonAndMutesItUnderTheSentinel(t *testing.T) {
	f := fleetPageFixture()
	f.eval.CTAs = append(f.eval.CTAs, fleet.CTA{
		Rank: 10, Kind: "missing_answer", Level: "error",
		Detail: "40 of 100 hook-captured turns on 2026-09-11 have no answer (40%)", Since: fixedNow.Add(-24 * time.Hour),
		Action:  "Turns are being captured without their answers: the fleet is still on a client without the last_assistant_message fix; upgrade and watch version lag",
		Command: "loop-sessions daemon --upgrade-now", Anchor: "runbook-missing-answers",
	})
	s := newServer(t, f, admin)
	body := get(t, s, "/admin/fleet").Body.String()
	if !strings.Contains(body, "cta-missing_answer") {
		t.Fatal("the fleet-wide row is missing from the page")
	}
	if strings.Contains(body, "ask  to run") || strings.Contains(body, "ask <") {
		t.Error("the fleet-wide row asks nobody to run its command")
	}
	if !strings.Contains(body, `name="email" value="*"`) {
		t.Error("the fleet-wide row's mute form does not post the sentinel person")
	}

	rec := postFleet(t, s, "/admin/fleet/mute", url.Values{
		"csrf": {s.csrfToken(admin)}, "email": {"*"}, "kind": {"missing_answer"}, "hours": {"72"}, "note": {"fleet converging on 788dcb3"},
	})
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), "ok=muted") {
		t.Fatalf("a sentinel mute answered %d %q", rec.Code, rec.Header().Get("Location"))
	}
	if len(f.muted) != 1 || f.muted[0].Email != "*" || f.muted[0].Kind != "missing_answer" {
		t.Fatalf("storage received %+v, want one mute under the sentinel", f.muted)
	}

	// Listed under muted as the fleet's, and lifted under the sentinel.
	f.eval.CTAs = f.eval.CTAs[:len(f.eval.CTAs)-1]
	f.eval.Muted = append(f.eval.Muted, fleet.CTA{
		Rank: 10, Kind: "missing_answer", Level: "error", Detail: "40 of 100 hook-captured turns have no answer",
		Command: "loop-sessions daemon --upgrade-now", Anchor: "runbook-missing-answers",
		Muted: &fleet.Mute{Email: "*", Kind: "missing_answer", Until: fixedNow.Add(72 * time.Hour), Note: "fleet converging on 788dcb3", CreatedBy: "admin@example.com"},
	})
	body = get(t, s, "/admin/fleet").Body.String()
	if !strings.Contains(body, "2 muted") || !strings.Contains(body, "fleet converging on 788dcb3") {
		t.Errorf("the muted list does not show the fleet-wide mute")
	}
	if strings.Count(body, `name="email" value="*"`) != 1 {
		t.Errorf("the muted list's unmute form does not post the sentinel: %d sentinel inputs", strings.Count(body, `name="email" value="*"`))
	}
	rec = postFleet(t, s, "/admin/fleet/unmute", url.Values{"csrf": {s.csrfToken(admin)}, "email": {"*"}, "kind": {"missing_answer"}})
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), "ok=unmuted") {
		t.Fatalf("a sentinel unmute answered %d %q", rec.Code, rec.Header().Get("Location"))
	}
	if len(f.unmuted) != 1 || f.unmuted[0] != "*/missing_answer" {
		t.Errorf("unmuted = %v", f.unmuted)
	}
}
