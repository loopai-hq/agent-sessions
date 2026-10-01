package skillusage

import (
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// request is the wire body (design 4b). Every field is a pointer so an
// absent key can be told from an empty one, since several rules turn on
// presence. There is no args field, and the decoder refuses unknown keys,
// so an emitter that sends the arguments is answered 400 on that key
// rather than having them dropped on the floor here and stored by a
// later edit. actor_email is declared only so it is refused by name under
// a source token and ignored under a device token; nothing reads it into
// a row.
type request struct {
	Origin         *string `json:"origin"`
	AgentPlatform  *string `json:"agent_platform"`
	Skill          *string `json:"skill"`
	SkillSource    *string `json:"skill_source"`
	Trigger        *string `json:"trigger"`
	Outcome        *string `json:"outcome"`
	ErrorClass     *string `json:"error_class"`
	Repo           *string `json:"repo"`
	SessionRef     *string `json:"session_ref"`
	PromptID       *string `json:"prompt_id"`
	ToolUseID      *string `json:"tool_use_id"`
	IdempotencyKey *string `json:"idempotency_key"`
	LinkRef        *string `json:"link_ref"`
	SessionType    *string `json:"session_type"`
	WindowStart    *string `json:"window_start"`
	WindowEnd      *string `json:"window_end"`
	OccurredAt     *string `json:"occurred_at"`
	ArgsPresent    *bool   `json:"args_present"`
	ArgsBytes      *int    `json:"args_bytes"`
	HarnessVersion *string `json:"harness_version"`
	ActorEmail     *string `json:"actor_email"`
}

// loggableFields are the field names a rejected line may carry: the
// request's own keys, read off the struct so the two cannot drift, plus
// the two names the decoder produces on its own (args, the refused key
// the design names; body, a document that is not a request). Any other
// name came from the body and logs as other, the way an id outside the
// shape does through ingest.LogID: a client can put up to the body cap of
// its own text in a key, and a line is not where it belongs. The wire 400
// still names the key, since that goes back to whoever sent it.
var loggableFields = func() map[string]bool {
	fields := map[string]bool{"args": true, "body": true}
	rt := reflect.TypeOf(request{})
	for i := range rt.NumField() {
		if tag, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ","); tag != "" {
			fields[tag] = true
		}
	}
	return fields
}()

// logField is the field as a line carries it: itself when it is one of
// ours, other when the body chose it.
func logField(field string) string {
	if field == "" || loggableFields[field] {
		return field
	}
	return "other"
}

// credential is what verified the request: exactly one of the two.
type credential struct {
	device *ingest.Identity
	source *store.SourceToken
}

func (c credential) isDevice() bool { return c.device != nil }

// fieldError is one 4b rule failing: the field the 400 names, and the
// reason the rejected line carries (invalid_payload, or platform_mismatch
// for the one rule the runbook wants told apart).
type fieldError struct {
	field, reason string
}

func invalid(field string) *fieldError {
	return &fieldError{field: field, reason: reasonInvalidPayload}
}

// assemble applies the 4b rules in the table's order and builds the row
// the store writes. Every miss names its field. The trust, the actor and
// the token columns come from the credential alone, never from the body.
func assemble(req request, cred credential, now time.Time) (store.SkillRow, *fieldError) {
	var row store.SkillRow

	// origin: required; a device may only say hook; a source token may
	// only say what it was minted for; derived is the server's own value.
	origin := value(req.Origin)
	switch {
	case origin == "" || !in(store.Origins, origin) || origin == store.OriginDerived:
		return row, invalid("origin")
	case cred.isDevice() && origin != store.OriginHook:
		return row, invalid("origin")
	case !cred.isDevice() && !cred.source.Allows(origin):
		return row, invalid("origin")
	}
	row.Origin = origin

	// agent_platform: a device names it (the two platforms a laptop runs);
	// a source token's is the row's, and a body that says otherwise is
	// refused, with its own reason on the line.
	platform := value(req.AgentPlatform)
	if cred.isDevice() {
		if platform != store.PlatformClaudeCode && platform != store.PlatformCodex {
			return row, invalid("agent_platform")
		}
		row.Trust = store.TrustDevice
		row.DeviceID = ptr(cred.device.DeviceID)
		row.ActorEmail = ptr(cred.device.Email)
		row.ActorKnown = true
	} else {
		if platform == "" {
			platform = cred.source.Platform
		}
		if platform != cred.source.Platform {
			return row, &fieldError{field: "agent_platform", reason: reasonPlatformMismatch}
		}
		row.Trust = store.TrustClaimed
		row.SourceTokenID = ptr(cred.source.ID)
		row.TokenPlatform = cred.source.Platform
		row.TokenEnvironment = cred.source.Environment
		row.ActorEmail = cred.source.BoundActorEmail
	}
	row.AgentPlatform = platform

	// skill: required; the store's normaliser decides the rest, keeping an
	// off-shape name as the literal off-shape and still counting the call.
	if req.Skill == nil || strings.TrimSpace(*req.Skill) == "" {
		return row, invalid("skill")
	}
	row.RawName, row.Plugin, row.Skill, _ = store.NormalizeSkillName(*req.Skill)

	// The enums. An off-enum skill_source is coerced to unknown, since the
	// emitters read it off a harness field whose vocabulary may grow; the
	// other three are refused, since a wrong outcome would miscount.
	row.SkillSource = store.SkillSourceUnknown
	if v := value(req.SkillSource); in(store.SkillSources, v) {
		row.SkillSource = v
	}
	row.Trigger = store.TriggerUnknown
	if req.Trigger != nil {
		if !in(store.Triggers, *req.Trigger) {
			return row, invalid("trigger")
		}
		row.Trigger = *req.Trigger
	}
	row.Outcome = store.OutcomeStarted
	if req.Outcome != nil {
		if !in(store.Outcomes, *req.Outcome) {
			return row, invalid("outcome")
		}
		row.Outcome = *req.Outcome
	}
	if req.ErrorClass != nil {
		if !in(store.ErrorClasses, *req.ErrorClass) || row.Outcome != store.OutcomeError {
			return row, invalid("error_class")
		}
		row.ErrorClass = ptr(*req.ErrorClass)
	}

	// actor_email: a device's is proven and the field is ignored; a source
	// token's is the one bound at mint, and a body naming anyone is
	// refused, since the token is readable by everyone in its environment.
	if !cred.isDevice() && req.ActorEmail != nil {
		return row, invalid("actor_email")
	}

	if req.Repo != nil {
		if !repoShape.MatchString(*req.Repo) {
			return row, invalid("repo")
		}
		row.Repo = *req.Repo
	}

	// The ids, each in the shape every id the fleet stores has. A harness
	// id without its session names no key the derived copy could land on,
	// so session_ref is required beside either; with neither, the
	// emitter's own key is the row's.
	for _, f := range []struct {
		name string
		v    *string
		dst  **string
	}{
		{"prompt_id", req.PromptID, &row.PromptID},
		{"tool_use_id", req.ToolUseID, &row.ToolUseID},
		{"idempotency_key", req.IdempotencyKey, &row.IdempotencyKey},
	} {
		if f.v == nil {
			continue
		}
		if !ingest.IDShape.MatchString(*f.v) {
			return row, invalid(f.name)
		}
		*f.dst = ptr(*f.v)
	}
	if req.SessionRef != nil {
		if !ingest.IDShape.MatchString(*req.SessionRef) {
			return row, invalid("session_ref")
		}
		row.SessionRef = *req.SessionRef
	}
	harnessID := row.PromptID != nil || row.ToolUseID != nil
	if harnessID && row.SessionRef == "" {
		return row, invalid("session_ref")
	}
	if !harnessID && row.IdempotencyKey == nil {
		return row, invalid("idempotency_key")
	}

	// The reconciler-only fields, and the reconciler's own time window.
	reconciler := origin == store.OriginReconciler
	if req.LinkRef != nil {
		if !reconciler || !ingest.IDShape.MatchString(*req.LinkRef) {
			return row, invalid("link_ref")
		}
		row.LinkRef = ptr(*req.LinkRef)
	}
	if req.SessionType != nil {
		if !reconciler || *req.SessionType == "" || !in(store.SkillSessionTypes, *req.SessionType) {
			return row, invalid("session_type")
		}
		row.SessionType = *req.SessionType
	}
	var windowStart, windowEnd time.Time
	if reconciler {
		var ok bool
		if windowStart, ok = parseTime(req.WindowStart); !ok {
			return row, invalid("window_start")
		}
		if windowEnd, ok = parseTime(req.WindowEnd); !ok {
			return row, invalid("window_end")
		}
		if windowEnd.Before(windowStart) {
			return row, invalid("window_start")
		}
		// A window ending in the future would let the one credential whose
		// rows are never clamped date rows ahead of now.
		if windowEnd.After(now.Add(clampFuture)) {
			return row, invalid("window_end")
		}
	} else if req.WindowStart != nil || req.WindowEnd != nil {
		return row, invalid("window_start")
	}

	// occurred_at: a claim, clamped into the recent past for every API
	// copy, device rows included; a reconciler's must sit inside its own
	// window and is never moved, so a catch-up after a stop keeps old
	// activations on their day.
	row.OccurredAt = now
	if req.OccurredAt != nil {
		at, ok := parseTime(req.OccurredAt)
		if !ok {
			return row, invalid("occurred_at")
		}
		switch {
		case reconciler && (at.Before(windowStart) || at.After(windowEnd)):
			return row, invalid("occurred_at")
		case reconciler:
			row.OccurredAt = at
		case at.Before(now.Add(-clampPast)) || at.After(now.Add(clampFuture)):
			row.OccurredAt, row.TimeClamped = now, true
		default:
			row.OccurredAt = at
		}
	} else if reconciler {
		// A reconciler names the moment; the default of now is what the
		// window exists to prevent.
		return row, invalid("occurred_at")
	}

	if req.ArgsPresent != nil {
		row.ArgsPresent = *req.ArgsPresent
	}
	if req.ArgsBytes != nil {
		if *req.ArgsBytes < 0 || *req.ArgsBytes > maxArgsBytes {
			return row, invalid("args_bytes")
		}
		row.ArgsBytes = *req.ArgsBytes
	}
	if req.HarnessVersion != nil {
		if !ingest.AgentBuildShape.MatchString(*req.HarnessVersion) {
			return row, invalid("harness_version")
		}
		row.HarnessVersion = *req.HarnessVersion
	}
	return row, nil
}

// repoShape is the repo CHECK of 0022: the last path segment a hook
// sends, never a path. A copy of the store's own bound, since the store
// coerces a derived row's repo to ” where this route refuses it.
var repoShape = regexp.MustCompile(`^[0-9A-Za-z._-]{1,64}$`)

func parseTime(s *string) (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, *s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

func value(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func ptr(s string) *string { return &s }

func in(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}
