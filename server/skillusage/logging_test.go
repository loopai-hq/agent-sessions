package skillusage

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/loopai-hq/loop-sessions/server/store"
)

// secretFragment is the canary no line may carry (the ingest package's
// pattern, server/ingest/logging_test.go): put in every string field of a
// request, in a fake args key, in the bearer and in the prompt-shaped
// fields, and then asserted absent from every line the request produced.
const secretFragment = "SECRET-PROMPT-CONTENT-do-not-log"

// TestNoLineCarriesAPayloadStringOrTheBearer is the 7.1 hygiene contract:
// ids and the skill pass through the id gate, enums are validated before
// they are logged, and the bearer is hashed before anything reads it.
func TestNoLineCarriesAPayloadStringOrTheBearer(t *testing.T) {
	st := newFakeStore()
	st.addToken(sourceToken, beaconToken())
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	canary := func(kind string) string { return secretFragment + "-" + kind }
	everyField := map[string]any{
		"origin": canary("origin"), "agent_platform": canary("platform"), "skill": canary("skill"),
		"skill_source": canary("source"), "trigger": canary("trigger"), "outcome": canary("outcome"),
		"error_class": canary("class"), "repo": canary("repo"), "session_ref": canary("session"),
		"prompt_id": canary("prompt"), "tool_use_id": canary("tool"), "idempotency_key": canary("key"),
		"link_ref": canary("link"), "session_type": canary("type"), "window_start": canary("start"),
		"window_end": canary("end"), "occurred_at": canary("at"), "harness_version": canary("build"),
		"actor_email": canary("actor"),
	}
	// One request per field, so each rule's own refusal path is walked
	// with the canary in the field it names; then the whole set at once,
	// an args key, and a bearer made of the canary.
	for field, v := range everyField {
		b := beacon()
		b[field] = v
		post(t, h, sourceToken, body(b))
		b = hook()
		b[field] = v
		post(t, h, deviceToken, body(b))
	}
	post(t, h, sourceToken, body(everyField))
	withArgs := beacon()
	withArgs["args"] = canary("args")
	post(t, h, sourceToken, body(withArgs))
	// The canary as a KEY: the decoder names an unknown key in its error,
	// the wire 400 may name it back to the sender, but the line logs
	// other, under a live token and under a junk bearer (which is refused
	// on the body before any verify, so no credential is needed to put
	// text on the line this way).
	for _, bearer := range []string{sourceToken, "lss_junk"} {
		w := post(t, h, bearer, body(map[string]any{canary("key"): 1}))
		wantRejected(t, w, http.StatusBadRequest, "invalid_payload", canary("key"))
	}
	post(t, h, "lss_"+canary("bearer"), body(beacon()))
	post(t, h, "lsd_"+canary("bearer"), body(hook()))
	post(t, h, canary("bearer"), body(beacon()))
	// A skill that fits the id shape but is not a slug reaches the line as
	// other; a well-formed one reaches it as its slug.
	odd := beacon()
	odd["skill"] = "/" + canary("slash")
	post(t, h, sourceToken, body(odd))
	// The accepted path with the canary where the shape rules let it
	// through: an off-shape skill is stored as off-shape and logged as
	// other, and a session ref that fits the shape is an id.
	ok := beacon()
	ok["skill"] = canary("skill with spaces")
	ok["session_ref"] = "sess-1"
	ok["prompt_id"] = "p-1"
	wantDuplicate(t, post(t, h, sourceToken, body(ok)), false)

	lines := logLines(t, buf)
	if len(lines) < 40 {
		t.Fatalf("only %d lines; the requests above should each have logged one", len(lines))
	}
	for _, l := range lines {
		raw, _ := json.Marshal(l)
		if strings.Contains(string(raw), secretFragment) {
			t.Fatalf("a log line carries payload text: %s", raw)
		}
		if strings.Contains(string(raw), sourceToken) || strings.Contains(string(raw), deviceToken) {
			t.Fatalf("a log line carries a bearer: %s", raw)
		}
	}
	accepted := linesNamed(lines, lineAccepted)
	if len(accepted) == 0 {
		t.Fatal("no accepted line")
	}
	last := accepted[len(accepted)-1]
	if last["skill"] != "other" {
		t.Errorf("an off-shape skill logged as %v, want other", last["skill"])
	}
	others := 0
	for _, l := range linesNamed(lines, lineRejected) {
		if l["field"] == "other" {
			others++
		}
	}
	if others != 2 {
		t.Errorf("%d rejected lines log field=other, want the two unknown-key requests", others)
	}
}

// TestLogFieldPassesOurNamesAndNothingElse pins the gate: every request
// key, args and body pass; an empty field stays empty; anything else is
// other.
func TestLogFieldPassesOurNamesAndNothingElse(t *testing.T) {
	for _, name := range []string{"origin", "agent_platform", "skill", "skill_source", "trigger", "outcome", "error_class",
		"repo", "session_ref", "prompt_id", "tool_use_id", "idempotency_key", "link_ref", "session_type", "window_start",
		"window_end", "occurred_at", "args_present", "args_bytes", "harness_version", "actor_email", "args", "body", ""} {
		if got := logField(name); got != name {
			t.Errorf("logField(%q) = %q", name, got)
		}
	}
	for _, name := range []string{"trust", "Origin", "skill ", secretFragment, "origin.x", "-"} {
		if got := logField(name); got != "other" {
			t.Errorf("logField(%q) = %q, want other", name, got)
		}
	}
}

// TestTheLinesCarryTheContractFields pins the 7.1 field sets and the
// message strings the metric filters name.
func TestTheLinesCarryTheContractFields(t *testing.T) {
	if lineAccepted != "skill invocation accepted" || lineRejected != "skill invocation rejected" || lineStoreFailed != "skill invocation store failed" {
		t.Fatal("a line message drifted from the metric filters")
	}
	st := newFakeStore()
	tok := beaconToken()
	tok.Environment = "eu"
	st.addToken(sourceToken, tok)
	log, buf := jsonLogger()
	h := newHandler(t, st, goodDevices(), func(o *Options) { o.Logger = log })

	b := beacon()
	b["trigger"], b["outcome"] = "agent", "success"
	wantDuplicate(t, post(t, h, sourceToken, body(b)), false)
	wantDuplicate(t, post(t, h, deviceToken, body(hook())), false)
	wantRejected(t, post(t, h, sourceToken, body(map[string]any{"origin": "beacon"})), http.StatusBadRequest, "invalid_payload", "skill")

	lines := logLines(t, buf)
	accepted := linesNamed(lines, lineAccepted)
	if len(accepted) != 2 {
		t.Fatalf("got %d accepted lines, want 2", len(accepted))
	}
	claimed := accepted[0]
	want := map[string]any{
		"level": "INFO", "trust": store.TrustClaimed, "platform": store.PlatformDevin, "environment": "eu", "origin": store.OriginBeacon,
		"source_token_id": sourceTokenID, "skill": "engg:git", "trigger": "agent", "outcome": "success",
		"duplicate": false, "soft_revoked": false, "time_clamped": false, "actor_known": false, "status": float64(200),
	}
	for k, v := range want {
		if claimed[k] != v {
			t.Errorf("claimed line %s = %v, want %v", k, claimed[k], v)
		}
	}
	if _, has := claimed["bytes"]; !has {
		t.Error("the claimed line carries no bytes")
	}
	if _, has := claimed["device_id"]; has {
		t.Error("a claimed line carries device_id")
	}
	device := accepted[1]
	if device["trust"] != store.TrustDevice || device["device_id"] != deviceID || device["email"] != deviceEmail || device["actor_known"] != true || device["environment"] != "" {
		t.Errorf("device line = %v", device)
	}
	if _, has := device["source_token_id"]; has {
		t.Error("a device line carries source_token_id")
	}
	rejected := linesNamed(lines, lineRejected)
	if len(rejected) != 1 {
		t.Fatalf("got %d rejected lines, want 1", len(rejected))
	}
	rj := rejected[0]
	for _, k := range []string{"reason", "field", "token_shape", "token_state", "source_token_id", "platform", "status"} {
		if _, has := rj[k]; !has {
			t.Errorf("the rejected line lacks %s: %v", k, rj)
		}
	}
	if rj["level"] != "WARN" || rj["reason"] != reasonInvalidPayload || rj["field"] != "skill" || rj["token_state"] != store.TokenStateLive || rj["platform"] != store.PlatformDevin {
		t.Errorf("rejected line = %v", rj)
	}
}
