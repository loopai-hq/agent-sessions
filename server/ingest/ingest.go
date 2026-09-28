// Package ingest receives what the agents on people's laptops captured.
//
// Two endpoints, and one property that decides almost everything about them.
// The agent delivers at-least-once: it holds every item on disk until this
// server acknowledges it and resends after any interruption, so a redelivery is
// the ordinary shape of a healthy system recovering rather than an anomaly to be
// defended against. Four consequences follow, and each of them is a way this
// endpoint can quietly destroy data if it is written the obvious way instead.
//
// A repeated item must leave the database exactly as a single delivery would,
// followed through the session rollup and the cost arithmetic and not merely
// through the insert. Idempotency on the event id is not sufficient for tokens:
// the same model call reaches us as two events with two different ids because it
// appears in two transcript records, so usage is credited through a ledger keyed
// by the identity of the call rather than by the event that carried it. Both
// layers belong in the same transaction as the insert, so both live in the
// storage layer behind Store; what this package owns is handing that layer
// well-formed, attributed, twice-scrubbed records, and pricing each model call
// at the rates in force when it happened (see Pricer).
//
// The answer is per item. A batch cannot be answered with a single status code
// because the agent deletes exactly what it is told was accepted and
// permanently quarantines exactly what it is told was rejected. One verdict for
// a mixed batch forces a choice between deleting items that were never stored
// and resending items that were.
//
// Rejection is final, which is what makes it dangerous. It is used only for
// conditions that can never become true: a missing required field, a payload
// that does not parse, an event that fails validation, a body over the
// advertised cap. A database timeout or an exhausted connection pool fails the
// whole request with a 5xx instead, so the agent retries all of it. Reporting a
// transient failure as a per-item rejection tells the agent to quarantine data
// the server could have stored a second later, and it does so silently.
//
// Size is bounded twice, for the reason set out in section 4a of the contract.
// MaxBatchBytes is the advertised cap and an over-cap batch is parsed anyway and
// answered with a rejection per item; HardBodyLimit is the real memory bound and
// the only one that produces a transport-level failure. Collapsing the two into
// one limit defeats the policy, because reading stops at exactly the size the
// policy needs to read past. A 413 in its place is worse than useless: the agent
// treats a transport error as transient and retries forever, and it deliberately
// sends a single over-limit item alone, so the request can never succeed and
// never stops being made.
//
// Everything is scrubbed again here. The agent scrubs before uploading, but
// pattern matching on a laptop is best-effort, and this pass is what supports
// the claim that no live credential is retained centrally. A hit on this side is
// recorded rather than silently fixed: it is the only signal that the agent's
// pattern set has fallen behind.
package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/spool"
)

// The client-facing routes. They are constants because the agent has them
// compiled in and a rename is a fleet-wide outage rather than a refactor.
const (
	EventsPath = "/v1/events"
	HealthPath = "/v1/health"
)

// Record is one event ready to be stored, with the attribution the server
// derived rather than the attribution the payload claimed.
//
// Email never comes from the body. A device token proves which principal is
// uploading; the payload is whatever the machine chose to send, and taking the
// owner from it would let any enrolled laptop file events under a colleague's
// name.
type Record struct {
	Event event.Event

	// Email is the principal the delivering device belongs to.
	Email string
	// DeviceID is the machine that delivered it, empty when unknown.
	DeviceID string
	// Repo is the repository the session's working directory resolves to.
	Repo string
	// Body is the record as it will be persisted: the delivered bytes verbatim
	// when the second scrub pass found nothing, and the re-encoded scrubbed
	// document when it did. Keeping the delivered bytes where possible means a
	// parser change later can be replayed against what actually arrived.
	Body json.RawMessage
}

// Rejection is one event the store refused permanently.
type Rejection struct {
	ID     string
	Reason string
}

// Result is what the store made of a batch.
//
// Inserted and Duplicate are separate because the two audiences differ: the
// agent must treat both as accepted or a retried batch is retried forever, while
// anything counting activity must count only Inserted or a network flap looks
// like a burst of work.
type Result struct {
	Inserted  []string
	Duplicate []string
	Rejected  []Rejection
}

// Store is the persistence this package needs, declared here rather than
// imported so the endpoint's policy can be exercised against a fake and so the
// SQL stays with whoever owns the schema.
//
// UpsertEvents must be atomic and idempotent: a redelivered event must move no
// counter in the session rollup and no figure in the cost total, and a failure
// must leave nothing behind. It must return an error for every transient
// condition and use Result.Rejected only for permanent ones, because this
// package turns the first into a 5xx and the second into a client-side
// quarantine, and there is no way back from the quarantine.
//
// The storage package satisfies this through a field-for-field adapter: its
// Ingest is this Record and its UpsertResult is this Result. The adapter must
// stay mechanical. Anything clever in it — collapsing a returned error into a
// rejection to salvage part of a batch, say — undoes the distinction the two
// halves of this interface exist to preserve.
type Store interface {
	UpsertEvents(ctx context.Context, records []Record) (Result, error)
	PutHealthReport(ctx context.Context, email, deviceID string, report health.Report) error
}

// Identity is who a verified device credential says is calling.
type Identity struct {
	Email    string
	DeviceID string
}

// Devices verifies the credential a laptop presents.
//
// Implementations return an error wrapping ErrUnauthenticated when the
// credential is missing, malformed, unknown, revoked, expired or belongs to a
// disabled principal, and a plain error for an infrastructure failure. The
// distinction is the whole reason the interface is shaped this way: the first is
// the caller's problem and answers 401, the second is ours and answers 503, and
// a lookup that failed because the database was unreachable must never be
// reported to a laptop as "your credential is bad".
type Devices interface {
	Verify(ctx context.Context, presented string) (Identity, error)
}

// ErrUnauthenticated means the request carried no usable device credential.
var ErrUnauthenticated = errors.New("ingest: no verified device identity")

// Reasons a delivered item was refused. They are stable strings because they
// reach a human: the agent stores the reason next to the quarantined item, and
// somebody eventually reads a quarantine directory to find out what went wrong.
const (
	ReasonNoID        = "missing id"
	ReasonNoSessionID = "missing session_id"
	ReasonBadPayload  = "payload is not valid json"
	ReasonInvalid     = "event failed validation"
	ReasonBatchTooBig = "batch exceeds the server's size limit"
)

// healthReportType is the event_type the server-catch line carries for a
// credential found in a health report, which is not an event and has no type
// of its own.
const healthReportType = "health_report"

// knownEventTypes is every value the event_type field of a log line may take
// besides "" and "other"; see logEventType.
var knownEventTypes = map[string]bool{
	string(event.SessionStarted): true,
	string(event.SessionEnded):   true,
	string(event.UserPrompt):     true,
	string(event.AssistantTurn):  true,
	string(event.ToolCall):       true,
	string(event.ToolResult):     true,
	string(event.ToolFailed):     true,
	string(event.FileChanged):    true,
	string(event.SubagentStart):  true,
	string(event.SubagentEnd):    true,
	string(event.Compaction):     true,
	string(event.Artifact):       true,
	healthReportType:             true,
}

// logEventType bounds the event_type field on every line this package writes
// and, through it, the metric labels built from those lines.
//
// The type in a payload is whatever the client wrote there: Validate requires
// only that it is non-empty, so a buggy or hostile enrolled device can put a
// transcript line, or four kilobytes of anything, in it, and until this
// existed both went verbatim into an ERROR line and into a metric label with
// no bound on its cardinality. A known type passes through; anything else
// is "other". Nothing is lost by that: the store keeps the type the payload
// declared, and "other" in the metric is itself the signal that a client is
// sending a type this build does not know. A type added to internal/event is
// added here to get a label of its own.
func logEventType(t string) string {
	if t == "" || knownEventTypes[t] {
		return t
	}
	return "other"
}

// IDShape is what an event id or a session id the agent sends looks like:
// the client's 32-character event ids, the harness's 36-character session
// uuids, and the Codex rollout ids, all letters, digits and a little
// punctuation. Nothing the fleet has ever stored falls outside it. Exported
// because the skill-invocation route bounds its session_ref, prompt_id and
// tool_use_id by the same rule, so one shape decides on both routes.
var IDShape = regexp.MustCompile(`^[0-9A-Za-z._:-]{1,64}$`)

// idShape is IDShape under the name this file grew up with.
var idShape = IDShape

// LogID bounds an id field on a log line the way logEventType bounds the
// type: Validate only requires the ids to be non-empty, so they are the last
// payload strings that could carry content into a line, and the line's
// promise is that no payload text reaches the logs whatever the client sends.
// An id of the expected shape passes through; anything else is "other", and
// the store's own rejection reason still names the item.
func LogID(id string) string {
	if id == "" || IDShape.MatchString(id) {
		return id
	}
	return "other"
}

// logID is LogID under the name this file grew up with.
func logID(id string) string { return LogID(id) }

// Response is the wire form of the verdict.
//
// It mirrors the client's expectation without sharing a type with it: the two
// halves are separately deployable, and a struct they both imported would force
// them to be upgraded together.
type Response struct {
	// Accepted lists the item ids the server durably stored, including the ones
	// it already had. Only these may be deleted from the spool.
	Accepted []string `json:"accepted"`
	// Rejected lists items that can never be accepted, with the reason.
	Rejected []Reject `json:"rejected,omitempty"`
	// RetryAfterMS is the server's own pacing instruction, sent when it is
	// refusing work rather than when it is doing it.
	RetryAfterMS int64 `json:"retry_after_ms,omitempty"`
}

// Reject is one permanently unacceptable item.
type Reject struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// Options configure a Handler.
type Options struct {
	Store   Store
	Devices Devices

	// MaxBatchBytes is the advertised cap on one batch. A body over it is still
	// parsed, and every item in it is rejected individually. Zero takes
	// defaultMaxBatchBytes.
	MaxBatchBytes int
	// HardBodyLimit is the absolute ceiling on bytes read from a request, and the
	// only limit that produces a transport-level failure. Zero takes
	// hardBodyMultiple times MaxBatchBytes. It must be comfortably larger than
	// MaxBatchBytes; see the package comment.
	HardBodyLimit int64
	// MaxHealthBytes caps a health report. Zero takes defaultMaxHealthBytes.
	MaxHealthBytes int64

	// Repo resolves a session's working directory to a repository name. It is
	// injected because the answer depends on how checkouts are laid out on the
	// machines being captured, which is deployment knowledge rather than
	// something derivable here. Nil takes RepoFromCwd.
	Repo func(cwd string) string

	// RetryAfter is what a 5xx asks the agent to wait. Zero sends no instruction
	// and leaves the agent on its own backoff curve, which is already correct;
	// setting it is for shedding load deliberately.
	RetryAfter time.Duration

	// Logger receives what the responses deliberately withhold, including every
	// credential this server caught that the agent did not.
	Logger *slog.Logger
}

const (
	defaultMaxBatchBytes = 4 << 20
	// hardBodyMultiple is the gap between the advertised cap and the real memory
	// bound. A body in that window is over the cap but still small enough to
	// parse, which is the window in which a per-item rejection is possible at
	// all.
	hardBodyMultiple = 8

	defaultMaxHealthBytes = 256 << 10
)

// Handler serves the client-facing ingest API.
type Handler struct {
	store   Store
	devices Devices

	maxBatchBytes  int
	hardBodyLimit  int64
	maxHealthBytes int64

	repo       func(string) string
	retryAfter time.Duration
	log        *slog.Logger

	mux *http.ServeMux
}

// New builds a Handler. A missing store or verifier is refused rather than
// defaulted: a nil verifier would make the upload endpoint anonymous, and that
// is not a condition to discover from the data.
func New(opts Options) (*Handler, error) {
	if opts.Store == nil {
		return nil, errors.New("ingest: Store is required")
	}
	if opts.Devices == nil {
		return nil, errors.New("ingest: Devices is required")
	}
	h := &Handler{
		store:          opts.Store,
		devices:        opts.Devices,
		maxBatchBytes:  opts.MaxBatchBytes,
		hardBodyLimit:  opts.HardBodyLimit,
		maxHealthBytes: opts.MaxHealthBytes,
		repo:           opts.Repo,
		retryAfter:     opts.RetryAfter,
		log:            opts.Logger,
	}
	if h.maxBatchBytes <= 0 {
		h.maxBatchBytes = defaultMaxBatchBytes
	}
	if h.hardBodyLimit <= 0 {
		h.hardBodyLimit = int64(h.maxBatchBytes) * hardBodyMultiple
	}
	if h.hardBodyLimit <= int64(h.maxBatchBytes) {
		// Equal limits are the failure section 4a documents: reading stops at the
		// size the policy needs to read past, an over-cap batch becomes
		// unparseable, and the per-item rejection it was supposed to get turns
		// into a 400 for the whole request.
		return nil, fmt.Errorf("ingest: HardBodyLimit (%d) must exceed MaxBatchBytes (%d)",
			h.hardBodyLimit, h.maxBatchBytes)
	}
	if h.maxHealthBytes <= 0 {
		h.maxHealthBytes = defaultMaxHealthBytes
	}
	if h.repo == nil {
		h.repo = RepoFromCwd
	}
	if h.log == nil {
		h.log = slog.Default()
	}
	h.mux = http.NewServeMux()
	h.Register(h.mux)
	return h, nil
}

// Route is one of the handler's mountings: the pattern a mux takes, method
// included, and the handler behind it.
type Route struct {
	Pattern string
	Handler http.Handler
}

// Routes lists every route the handler serves, in the form Register mounts
// them. It is exported so a caller that has to wrap each route (the server
// stamps the delivering client's build onto the request context before the
// upload reaches the store, and its mux is not this package's) mounts
// exactly what Register mounts: a route added here reaches the handler's
// own mux, the server's, and the test that compares the two, with no list
// restated anywhere.
func (h *Handler) Routes() []Route {
	return []Route{
		{Pattern: "POST " + EventsPath, Handler: http.HandlerFunc(h.handleEvents)},
		{Pattern: "POST " + HealthPath, Handler: http.HandlerFunc(h.handleHealth)},
	}
}

// Register adds the ingest routes to a mux owned by the caller. The patterns are
// absolute and carry their method, so this and ServeHTTP route identically and a
// route cannot be reachable through one and not the other.
func (h *Handler) Register(mux *http.ServeMux) {
	for _, r := range h.Routes() {
		mux.Handle(r.Pattern, r.Handler)
	}
}

// ServeHTTP lets the Handler stand alone.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

func (h *Handler) handleEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := h.identify(w, r)
	if !ok {
		return
	}

	items, size, overCap, err := h.decodeBatch(w, r)
	if err != nil {
		status := h.writeBodyFailure(w, r, err)
		h.logBatch(r, id, batchOutcome{bytes: size, status: status})
		return
	}

	verdicts, records := h.prepare(r, id, items, overCap)

	var res Result
	if len(records) > 0 {
		res, err = h.store.UpsertEvents(r.Context(), records)
		if err != nil {
			// Everything in the batch stays the agent's, including the items this
			// server had already judged unacceptable. A per-item answer alongside a
			// failed write would be a verdict on a transaction that did not happen.
			h.fail(r, "upsert events", err)
			h.writeUnavailable(w)
			h.logBatch(r, id, batchOutcome{items: len(items), bytes: size, status: http.StatusServiceUnavailable})
			return
		}
	}

	resp, outcome := h.respond(r, id, verdicts, res)
	outcome.items, outcome.bytes, outcome.status = len(items), size, http.StatusOK
	writeJSON(w, http.StatusOK, resp)
	h.logBatch(r, id, outcome)
}

// batchOutcome is what one /v1/events request came to, in counts. It is the
// content of the "ingest batch" line and nothing else: every field is a
// number or a status, so the line can be emitted for every request without
// the privacy rule in the package comment having to be re-checked per field.
type batchOutcome struct {
	items     int
	accepted  int
	duplicate int
	rejected  int
	undecided int
	bytes     int64
	status    int
}

// logBatch writes the one line per request that the ingest metrics are built
// from. INFO, because a batch is the ordinary event here; the rejection and
// undecided lines are the ones that carry a higher severity.
//
// The agent version comes from the User-Agent because the fleet is never on
// one build at once: a rejection rate that is high on one build and zero on
// the rest is a client bug, and a rate that is the same on every build is a
// server one, and that split is the first question anybody asks.
func (h *Handler) logBatch(r *http.Request, id Identity, o batchOutcome) {
	h.log.InfoContext(r.Context(), "ingest batch",
		slog.String("email", id.Email),
		slog.String("device_id", id.DeviceID),
		slog.String("agent_version", agentVersion(r)),
		slog.Int("items", o.items),
		slog.Int("accepted", o.accepted),
		slog.Int("duplicate", o.duplicate),
		slog.Int("rejected", o.rejected),
		slog.Int("undecided", o.undecided),
		slog.Int64("bytes", o.bytes),
		slog.Int("status", o.status))
}

// AgentBuildShape is what a build string the agent sends looks like: a
// commit hash, "dev", or a tag. The value becomes a metric label and a
// per-session array column, so the shape is a bound on its length and
// alphabet rather than a validation of the build: a User-Agent is a header
// any caller can set, and without the bound one device could put anything,
// at any length, into the label and grow the array with every request.
// Exported because a skill invocation's harness_version is bounded by the
// same rule.
var AgentBuildShape = regexp.MustCompile(`^[0-9A-Za-z._-]{1,40}$`)

// agentBuildShape is AgentBuildShape under the name this file grew up with.
var agentBuildShape = AgentBuildShape

// AgentBuild reads the build out of the agent's User-Agent, which every
// client path sends as "loop-sessions/<build>", followed by platform detail
// after a space that never reaches a column grouped and counted by build.
// Empty for anything else, which is what a curl during an incident looks
// like and is worth being able to tell apart; a "loop-sessions/" prefix on a
// build that does not fit the shape is treated the same way rather than
// clipped into one, since no agent ever sent one and a clipped token would
// be a build that never existed.
//
// It is the one parser of that header: the "ingest batch" line reads it
// through agentVersion, and the server's route wrapper reads it to stamp the
// build onto the request context for the store, so the log and the row
// cannot disagree about which builds exist.
func AgentBuild(userAgent string) string {
	v, ok := strings.CutPrefix(userAgent, "loop-sessions/")
	if !ok {
		return ""
	}
	v, _, _ = strings.Cut(v, " ")
	if !agentBuildShape.MatchString(v) {
		return ""
	}
	return v
}

// agentVersion is AgentBuild over the request, for the batch line.
func agentVersion(r *http.Request) string {
	return AgentBuild(r.UserAgent())
}

// verdict is one delivered item and what became of it. The item id and the event
// id are tracked separately because they are two different keys: the agent acks
// on the item id, and the store dedups on the id inside the payload. They are
// equal in every payload the current agent produces, and relying on that would
// mean an agent that ever changed it would silently have its acks misrouted.
type verdict struct {
	itemID  string
	eventID string
	// reason, when set, is a permanent rejection decided before the store was
	// asked.
	reason string
	// eventType is the payload's declared type when it parsed far enough to
	// have one. It travels with the verdict for the rejection log line only:
	// a rejection rate is diagnosable by type (every tool_result too big, say)
	// and meaningless without it.
	eventType string
}

// prepare turns delivered items into records to store, deciding the permanent
// rejections on the way.
//
// Order is preserved so the response reads in the same order as the request,
// which is what makes a mixed batch diagnosable by eye.
//
// An item's Kind is not consulted. It is the agent's routing hint, and a kind
// this build has never heard of is not a permanent condition — a later build
// would store it — so rejecting on it would quarantine deliverable data. What
// decides is whether the payload is an event, which is the only thing this
// endpoint can store either way.
func (h *Handler) prepare(r *http.Request, id Identity, items []spool.Item, overCap bool) ([]verdict, []Record) {
	verdicts := make([]verdict, 0, len(items))
	records := make([]Record, 0, len(items))

	for _, it := range items {
		if overCap {
			// Not a judgement on the item: the batch it arrived in was too large.
			// It is still a rejection, because the agent sends an over-limit item
			// alone and a transport failure would loop on it forever.
			verdicts = append(verdicts, verdict{itemID: it.ID, reason: ReasonBatchTooBig})
			continue
		}
		if it.ID == "" {
			verdicts = append(verdicts, verdict{reason: ReasonNoID})
			continue
		}
		if it.SessionID == "" {
			verdicts = append(verdicts, verdict{itemID: it.ID, reason: ReasonNoSessionID})
			continue
		}

		ev, body, reason := h.decodeItem(it)
		if reason != "" {
			verdicts = append(verdicts, verdict{itemID: it.ID, reason: reason, eventType: string(ev.Type)})
			continue
		}

		counts, err := rescan(&ev, &body)
		if err != nil {
			// Only reachable if a document that just parsed will not re-encode,
			// which is not a state the agent can reach by sending different bytes.
			// Treated as ours rather than theirs: a verdict carrying no event id
			// is claimed by neither list, so the item stays on the laptop and is
			// delivered again once the bug is fixed.
			h.fail(r, "rescan payload", err)
			verdicts = append(verdicts, verdict{itemID: it.ID, eventType: string(ev.Type)})
			continue
		}
		if len(counts) > 0 {
			h.recordServerCatch(r.Context(), id, ev.SessionID, ev.ID, string(ev.Type), counts)
		}

		verdicts = append(verdicts, verdict{itemID: it.ID, eventID: ev.ID, eventType: string(ev.Type)})
		records = append(records, Record{
			Event:    ev,
			Email:    id.Email,
			DeviceID: id.DeviceID,
			Repo:     h.repo(ev.Cwd),
			Body:     body,
		})
	}
	return verdicts, records
}

// decodeItem parses a delivered payload into an event, or names the permanent
// reason it cannot be one. On a validation failure the parsed event is still
// returned so the rejection line can name its type; the caller must not store
// it.
func (h *Handler) decodeItem(it spool.Item) (event.Event, json.RawMessage, string) {
	// A nil payload marshals to JSON null and unmarshals back into a zero event
	// without error, so testing for emptiness alone would let it through as a
	// validation failure rather than as the missing body it is. The trim is on
	// bytes rather than through a string so a multi-megabyte payload is not
	// copied to find out whether it is blank.
	body := json.RawMessage(bytes.TrimSpace(it.Payload))
	if len(body) == 0 || string(body) == "null" {
		return event.Event{}, nil, ReasonBadPayload
	}
	var ev event.Event
	if err := json.Unmarshal(body, &ev); err != nil {
		return event.Event{}, nil, ReasonBadPayload
	}
	if err := ev.Validate(); err != nil {
		return event.Event{Type: ev.Type}, nil, ReasonInvalid
	}
	return ev, body, ""
}

// respond assembles the per-item answer and counts what it decided.
//
// An item that appears in neither list is deliberate. The agent keeps such an
// item pending and advances its own attempt counter, which is the right outcome
// for the one case that produces it: something this server could not complete
// and has no permanent judgement about.
//
// Every rejection is logged here, one WARNING line per item, because a
// rejection is final on the laptop: the agent quarantines the item with the
// reason and nothing retries it. Until this line existed the reason reached
// only the JSON response, and a fleet-wide rejection rate could not be built
// from anything the server kept. The line carries the reason, the type, the
// principal and the device, and never the payload: the reason enum and
// logEventType bound its cardinality and the privacy rule bounds its content.
func (h *Handler) respond(r *http.Request, id Identity, verdicts []verdict, res Result) (Response, batchOutcome) {
	// eid, not id: the principal is the id parameter, and a loop variable of
	// the same name would shadow it for anyone who later reads id.Email
	// inside one of these loops.
	inserted := make(map[string]bool, len(res.Inserted))
	for _, eid := range res.Inserted {
		inserted[eid] = true
	}
	stored := make(map[string]bool, len(res.Inserted)+len(res.Duplicate))
	for _, eid := range res.Inserted {
		stored[eid] = true
	}
	for _, eid := range res.Duplicate {
		stored[eid] = true
	}
	refused := make(map[string]string, len(res.Rejected))
	for _, rj := range res.Rejected {
		refused[rj.ID] = rj.Reason
	}

	var o batchOutcome
	// An event id the store inserted is counted as accepted for the first
	// item that carries it and as a duplicate for any further item in the
	// same batch that carries it too; the store reports the id once, so the
	// counts have to be kept per item here.
	claimed := make(map[string]bool, len(res.Inserted))
	resp := Response{Accepted: []string{}}
	for _, v := range verdicts {
		// Membership decides a refusal, not the reason text. A store that refuses
		// an event without filling in a reason has still refused it, and testing
		// the reason for emptiness would quietly route the item back onto the
		// retry path it can never leave.
		reason, wasRefused := refused[v.eventID]
		switch {
		case v.reason != "":
			resp.Rejected = append(resp.Rejected, Reject{ID: v.itemID, Reason: v.reason})
			o.rejected++
			h.logRejected(r, id, v.reason, v.eventType)
		case stored[v.eventID]:
			resp.Accepted = append(resp.Accepted, v.itemID)
			if inserted[v.eventID] && !claimed[v.eventID] {
				claimed[v.eventID] = true
				o.accepted++
			} else {
				o.duplicate++
			}
		case wasRefused:
			resp.Rejected = append(resp.Rejected, Reject{ID: v.itemID, Reason: reason})
			o.rejected++
			h.logRejected(r, id, reason, v.eventType)
		case v.eventID != "":
			// The store answered without mentioning an event it was handed. Nothing
			// is claimed about it, and the omission is logged at ERROR because it
			// means the two sides disagree about what a batch contains: the item
			// stays on the laptop and is redelivered, and an old agent gives up on
			// it after eight such answers. The event id is what a person needs to
			// find the item in the store's own logs.
			o.undecided++
			h.log.ErrorContext(r.Context(), "store returned no verdict",
				slog.String("event_id", logID(v.eventID)),
				slog.String("event_type", logEventType(v.eventType)),
				slog.String("email", id.Email),
				slog.String("device_id", id.DeviceID))
		default:
			// A verdict with neither an event id nor a reason: the rescan failure
			// prepare logged. Undecided from the agent's side too.
			o.undecided++
		}
	}
	return resp, o
}

// logRejected is the per-item rejection line. See respond.
func (h *Handler) logRejected(r *http.Request, id Identity, reason, eventType string) {
	h.log.WarnContext(r.Context(), "ingest item rejected",
		slog.String("reason", reason),
		slog.String("event_type", logEventType(eventType)),
		slog.String("email", id.Email),
		slog.String("device_id", id.DeviceID))
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

// handleHealth stores one self-telemetry sample.
//
// The fleet view alerts on the ABSENCE of these, so the one thing this must not
// do is answer 202 for a report it did not store: a machine that is reporting
// would then be indistinguishable from one that has gone quiet, in the direction
// that hides the problem. A report the store refused is therefore a 5xx and the
// agent redelivers it, which is safe because the report carries its own emission
// time and is deduplicated on it.
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	id, ok := h.identify(w, r)
	if !ok {
		return
	}

	var report health.Report
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.maxHealthBytes))
	if err := dec.Decode(&report); err != nil {
		h.writeBodyFailure(w, r, err)
		return
	}

	// Health text is scrubbed for the same reason event text is. The report is
	// mostly counters, but LastError carries whatever the delivery path failed
	// with, and a failed request URL is a place credentials turn up.
	if counts := rescanReport(&report); len(counts) > 0 {
		h.recordServerCatch(r.Context(), id, "", "", healthReportType, counts)
	}

	if err := h.store.PutHealthReport(r.Context(), id.Email, id.DeviceID, report); err != nil {
		h.fail(r, "put health report", err)
		h.writeUnavailable(w)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// ---------------------------------------------------------------------------
// Reading the request
// ---------------------------------------------------------------------------

// decodeBatch reads a batch and reports how many bytes it read and whether
// that was over the advertised cap.
//
// The size is measured from the bytes actually read rather than from
// Content-Length. That header is supplied by the caller, and a server that
// size-checks on a number the other side chose is not size-checking. The
// count is returned even on failure, because a body that could not be parsed
// still has a size worth logging: the hard-limit failure is the one case
// where the size is the whole diagnosis.
func (h *Handler) decodeBatch(w http.ResponseWriter, r *http.Request) ([]spool.Item, int64, bool, error) {
	counted := &countingReader{r: http.MaxBytesReader(w, r.Body, h.hardBodyLimit)}
	var items []spool.Item
	if err := json.NewDecoder(counted).Decode(&items); err != nil {
		return nil, counted.n, false, err
	}
	return items, counted.n, counted.n > int64(h.maxBatchBytes), nil
}

// countingReader records how many bytes were consumed.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// identify resolves the calling device or writes the failure.
func (h *Handler) identify(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	token := bearerToken(r)
	if token == "" {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "device token required")
		return Identity{}, false
	}
	id, err := h.devices.Verify(r.Context(), token)
	if err != nil {
		if errors.Is(err, ErrUnauthenticated) {
			// No detail. The agent cannot act differently on any of the ways a
			// credential can be bad, and an attacker learning that a token was real
			// but revoked has learned something.
			writeError(w, http.StatusUnauthorized, "unauthenticated", "device token rejected")
			return Identity{}, false
		}
		// The credential may well be valid; we could not tell. Answering 401 would
		// send a working laptop to re-enrol over a database blip.
		h.fail(r, "verify device", err)
		h.writeUnavailable(w)
		return Identity{}, false
	}
	id.Email = normalizeEmail(id.Email)
	if id.Email == "" {
		// A verified credential with no owner cannot attribute anything, and an
		// event attributed to nobody is invisible to every permission check.
		h.fail(r, "verify device", errors.New("verifier returned an identity with no email"))
		writeError(w, http.StatusUnauthorized, "unauthenticated", "device token rejected")
		return Identity{}, false
	}
	return id, true
}

// bearerToken extracts the credential from an Authorization header. The scheme
// is compared case-insensitively because RFC 7235 says it is, and an agent that
// sends "bearer" is not wrong.
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const scheme = "bearer "
	if len(h) < len(scheme) || !strings.EqualFold(h[:len(scheme)], scheme) {
		return ""
	}
	return strings.TrimSpace(h[len(scheme):])
}

// normalizeEmail lower-cases and trims. The address is the ownership key on
// every row this endpoint writes, and two spellings of one person are two
// owners.
func normalizeEmail(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// RepoFromCwd is the default resolution of a working directory to a repository.
//
// It takes the last path segment, which is right for the common case because a
// session is started at the root of a checkout and the harness records that
// directory. It is wrong for a session started in a subdirectory, which reports
// the subdirectory's name. The server cannot do better on its own: it has no
// access to the laptop's filesystem, so it cannot look for a checkout marker,
// and the path is all there is. A deployment that knows how its machines are laid
// out should supply Options.Repo instead of living with the approximation.
func RepoFromCwd(cwd string) string {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return ""
	}
	base := path.Base(strings.ReplaceAll(cwd, `\`, "/"))
	if base == "." || base == "/" {
		return ""
	}
	return strings.TrimSuffix(base, ".git")
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

// writeBodyFailure answers a request whose body could not be read, and
// reports the status it chose so the caller can log it.
//
// A body over the hard limit is the one case in this package that gets a
// transport-level failure, because the items were never read and there is no
// per-item answer to give. Everything else is a 400: the bytes arrived and were
// not a batch.
func (h *Handler) writeBodyFailure(w http.ResponseWriter, r *http.Request, err error) int {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		h.fail(r, "read body", err)
		writeError(w, http.StatusRequestEntityTooLarge, "body_too_large",
			"request body exceeds the hard limit")
		return http.StatusRequestEntityTooLarge
	}
	writeError(w, http.StatusBadRequest, "invalid_request", "malformed request body")
	return http.StatusBadRequest
}

// writeUnavailable answers a transient failure.
//
// The shape is the verdict shape with nothing accepted and nothing rejected,
// because the agent parses one response type and must not read a 503 as a batch
// of permanent refusals. The status is what matters: it keeps every item on the
// laptop.
func (h *Handler) writeUnavailable(w http.ResponseWriter) {
	if h.retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(h.retryAfter.Round(time.Second).Seconds())))
	}
	writeJSON(w, http.StatusServiceUnavailable, Response{
		Accepted:     []string{},
		RetryAfterMS: h.retryAfter.Milliseconds(),
	})
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg}})
}

// writeJSON encodes into a buffer before touching the ResponseWriter. Encoding
// straight to the writer commits the status line first, so a marshal failure
// halfway through would append error text to a body already declared 200.
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal","message":"internal error"}}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// recordServerCatch logs a credential this server removed and the agent did not.
//
// The counts also travel with the event into the session's redaction tally,
// under a prefix that keeps them apart from the agent's own; see
// ServerCaughtPrefix. This log line is the operational half of the same signal,
// and it names the machine so somebody can find out which agent version is
// behind. The value is never logged, for the obvious reason.
func (h *Handler) recordServerCatch(ctx context.Context, id Identity, sessionID, eventID, kind string, counts map[string]int) {
	kinds := make([]string, 0, len(counts))
	for k := range counts {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	h.log.WarnContext(ctx, "ingest scrubbed a credential the agent missed",
		slog.String("email", id.Email),
		slog.String("device_id", id.DeviceID),
		slog.String("session_id", logID(sessionID)),
		slog.String("event_id", logID(eventID)),
		slog.String("event_type", logEventType(kind)),
		slog.Any("kinds", kinds))
}

// fail logs the detail the caller is not given. A 5xx with nothing in the logs
// is an outage nobody can diagnose. The request may be nil for a failure that
// is not attributable to one.
//
// Logged with the request's context, as every line in this package is, so
// the trace the server's middleware put there reaches the line and the Logs
// Explorer can show this failure next to the 503 it explains.
func (h *Handler) fail(r *http.Request, op string, err error) {
	ctx := context.Background()
	attrs := []any{slog.String("op", op), slog.String("error", err.Error())}
	if r != nil {
		ctx = r.Context()
		attrs = append(attrs,
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path))
	}
	h.log.ErrorContext(ctx, "ingest request failed", attrs...)
}
