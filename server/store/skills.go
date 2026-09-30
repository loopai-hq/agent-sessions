package store

// Skill invocations: one row per time a skill ran on any agent platform,
// derived here from the events an enrolled laptop already delivers (a
// Skill tool_call, or a typed slash command on a user_prompt) and posted by
// the hook, beacon and reconciler emitters through the API route. This file
// is the store half of the design: the closed enums every other PR shares, the dedupe key, the
// one merge statement, the derivation at ingest and over history, the
// re-derive queue, the admin audit row and the silence summary.
//
// Two invariants shape the code. A skill row never fails an events batch:
// the derivation runs under a savepoint, and a failure rolls the skill
// work back, commits the events and queues the sessions for the dirty
// tick. And the rebuild is the live path: the versioned step and the queue
// drain call the same insertSkillInvocations over events read back from
// the store, so history and ingest cannot disagree.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/normalize"
)

// ---------------------------------------------------------------- closed enums

// The closed sets of 0022, as Go constants, one slice per column in the
// CHECK's order. The guard test compares each slice with the migration
// text, so a value added on one side and not the other fails the build
// rather than a statement in production.
const (
	OriginDerived    = "derived"
	OriginHook       = "hook"
	OriginBeacon     = "beacon"
	OriginReconciler = "reconciler"

	PlatformClaudeCode = "claude_code"
	PlatformDevin      = "devin"
	PlatformCapy       = "capy"
	PlatformCodex      = "codex"
	PlatformVorflux    = "vorflux"

	TrustDevice  = "device"
	TrustClaimed = "claimed"

	SkillSourcePlugin  = "plugin"
	SkillSourceProject = "project"
	SkillSourceUser    = "user"
	SkillSourceMirror  = "mirror"
	SkillSourceBuiltin = "builtin"
	SkillSourceUnknown = "unknown"

	TriggerUser    = "user"
	TriggerAgent   = "agent"
	TriggerNested  = "nested"
	TriggerPreload = "preload"
	TriggerUnknown = "unknown"

	OutcomeStarted = "started"
	OutcomeSuccess = "success"
	OutcomeError   = "error"
	OutcomeUnknown = "unknown"

	ErrorClassNotFound = "not_found"
	ErrorClassTimeout  = "timeout"
	ErrorClassRuntime  = "runtime_error"
	ErrorClassUnknown  = "unknown"

	SessionTypeNone       = ""
	SessionTypeEmpty      = "empty"
	SessionTypeUser       = "user"
	SessionTypeInternal   = "internal"
	SessionTypeAutomation = "automation"
)

var (
	Origins      = []string{OriginDerived, OriginHook, OriginBeacon, OriginReconciler}
	Platforms    = []string{PlatformClaudeCode, PlatformDevin, PlatformCapy, PlatformCodex, PlatformVorflux}
	Trusts       = []string{TrustDevice, TrustClaimed}
	SkillSources = []string{SkillSourcePlugin, SkillSourceProject, SkillSourceUser, SkillSourceMirror, SkillSourceBuiltin, SkillSourceUnknown}
	Triggers     = []string{TriggerUser, TriggerAgent, TriggerNested, TriggerPreload, TriggerUnknown}
	Outcomes     = []string{OutcomeStarted, OutcomeSuccess, OutcomeError, OutcomeUnknown}
	ErrorClasses = []string{ErrorClassNotFound, ErrorClassTimeout, ErrorClassRuntime, ErrorClassUnknown}
	// SkillSessionTypes is the skill_invocations.session_type CHECK: the
	// sessions lattice (store.SessionTypes, the display authority) plus ''
	// for a derived row whose session has not been copied yet.
	SkillSessionTypes = []string{SessionTypeNone, SessionTypeEmpty, SessionTypeUser, SessionTypeInternal, SessionTypeAutomation}
)

// ---------------------------------------------------------------- rows and keys

// SkillRow is one skill invocation as the store writes it: every 0022
// column but the ones the database fills (id, received_at) and the two the
// merge alone sets (preempted_by, preempted_device).
//
// Ids are strings holding uuids, as device ids are everywhere else in this
// package (Ingest.DeviceID, sessions.device_id): the module carries no uuid
// package and the pool casts with ::uuid, so isUUID is the check before a
// statement and the column type is the check inside it.
type SkillRow struct {
	Origin, AgentPlatform, Trust                          string
	DeviceID                                              *string
	SourceTokenID                                         *string
	RawName, Plugin                                       string
	Skill                                                 *string
	SkillSource, Trigger, Outcome                         string
	ErrorClass                                            *string
	ActorEmail                                            *string
	ActorKnown                                            bool
	Repo, SessionRef, SessionType                         string
	EventID, PromptID, ToolUseID, IdempotencyKey, LinkRef *string
	OccurredAt                                            time.Time
	TimeClamped, ArgsPresent                              bool
	ArgsBytes                                             int
	HarnessVersion                                        string
	// TokenPlatform and TokenEnvironment are the source token's, set by the
	// API route on a claimed row; the reconciler form of the dedupe key
	// reads them so a re-post under a rotated token lands on the same key.
	TokenPlatform, TokenEnvironment string
}

// skillDeriveStats are the counters of the "skill invocations derived"
// line, per batch.
type skillDeriveStats struct {
	Candidates, RowsUser, RowsAgent, OutcomeUpdates, Duplicates, SkippedBuiltin, SkippedShape, Unconfirmed int
	// OracleErrors counts the names whose alias lookup FAILED, which is
	// not the same thing as a name the catalog does not know: the first
	// gives up the whole derivation for the dirty tick to retry, the
	// second drops one candidate for good. Any non-zero value therefore
	// ends the batch, so the "derived" line carries it as a zero and the
	// "store failed" line is what an operator sees (design 7.1).
	OracleErrors int
	Touched      bool
}

// UpsertOutcome is what the merge statement came to for one row. No row
// back from the statement is a duplicate: neither Inserted nor Changed.
type UpsertOutcome struct {
	Inserted, Changed, Preempted bool
	PreemptedBy                  *string
	PreemptedDevice              *string
}

// ErrDedupeKey is form 4 of the dedupe key: a row with no harness id, no
// event and no idempotency key names no moment and cannot be stored. The
// API route answers it as 400 invalid_payload on idempotency_key.
var ErrDedupeKey = errors.New("store: skill row carries no id to key on")

// dedupeKeyMax is the dedupe_key CHECK's ceiling.
const dedupeKeyMax = 200

// DedupeKey composes the key every copy of one invocation lands on (design
// 3.3), in precedence order: the tool-use id (form 1), the prompt id (form
// 2), the event id for a derived copy with neither (2b for a Codex $skill
// mention, which carries the skill so two mentions in one prompt are two
// rows; 2c for a transcript slash prompt), then the emitter's idempotency
// key (form 3), keyed on the source token, on the device for an lsd_ row,
// or on the token's platform and environment for a reconciler so a re-post
// under a rotated token is the same row. Anything else is form 4.
func DedupeKey(r SkillRow) (string, error) {
	switch {
	case r.ToolUseID != nil && *r.ToolUseID != "":
		return r.AgentPlatform + ":" + r.SessionRef + ":t:" + *r.ToolUseID, nil
	case r.PromptID != nil && *r.PromptID != "":
		return r.AgentPlatform + ":" + r.SessionRef + ":p:" + *r.PromptID, nil
	case r.Origin == OriginDerived && r.EventID != nil && *r.EventID != "":
		if r.AgentPlatform == PlatformCodex && r.Skill != nil && *r.Skill != "" {
			return PlatformCodex + ":" + r.SessionRef + ":e:" + *r.EventID + ":" + *r.Skill, nil
		}
		return r.AgentPlatform + ":" + r.SessionRef + ":e:" + *r.EventID, nil
	case r.IdempotencyKey != nil && *r.IdempotencyKey != "":
		switch {
		case r.Origin == OriginReconciler:
			return "k:" + r.TokenPlatform + ":" + r.TokenEnvironment + ":" + *r.IdempotencyKey, nil
		case r.SourceTokenID != nil && *r.SourceTokenID != "":
			return "k:" + *r.SourceTokenID + ":" + *r.IdempotencyKey, nil
		case r.DeviceID != nil && *r.DeviceID != "":
			return "k:d:" + *r.DeviceID + ":" + *r.IdempotencyKey, nil
		}
		return "", fmt.Errorf("%w: idempotency key with no credential", ErrDedupeKey)
	}
	return "", ErrDedupeKey
}

var (
	// rawNameShape is the 4b rule for what an emitter may call a skill:
	// no whitespace, no control bytes, bounded. Skill text can carry
	// customer identifiers (SECURITY2-2), so a name outside it is stored as
	// the literal off-shape and the invocation still counts.
	rawNameShape = regexp.MustCompile(`^[/$]?[A-Za-z0-9][A-Za-z0-9_.:/-]{0,199}$`)
	// slugShape is the plugin and skill CHECK of 0022.
	slugShape = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}[a-z0-9]$`)
	// skillIDShape is ingest.IDShape, the bound on session_ref, prompt_id
	// and tool_use_id; a copy rather than an import so the store keeps
	// its dependence on the route layer at zero (a test pins equality).
	skillIDShape = regexp.MustCompile(`^[0-9A-Za-z._:-]{1,64}$`)
	// skillRepoShape and skillBuildShape are the repo and harness_version
	// CHECKs; a value outside either is stored '' rather than refused.
	skillRepoShape  = regexp.MustCompile(`^[0-9A-Za-z._-]{1,64}$`)
	skillBuildShape = regexp.MustCompile(`^[0-9A-Za-z._-]{1,40}$`)
	// codexMention is the $skill form a Codex prompt names a mirrored
	// skill by (design 4a), on a person's own text only.
	codexMention = regexp.MustCompile(`(?:^|\s)\$([a-z][a-z0-9-]{1,63})`)
)

// OffShapeName is what raw_name holds when the emitter's name failed the
// shape rule.
const OffShapeName = "off-shape"

// NormalizeSkillName applies the 4a/4b rule: control bytes stripped and
// the text bounded, kept as raw_name only when it fits the shape (else the
// literal off-shape and no skill); then one leading slash or dollar
// dropped, lowercased and split on the last colon into plugin and skill,
// each of which must fit the slug CHECK or the pair is (”, nil). The
// catalog joins on (plugin, skill), never on raw_name, so a name the
// normaliser could not place lands in the unknown report rather than
// being refused.
func NormalizeSkillName(raw string) (rawName, plugin string, skill *string, offShape bool) {
	cleaned := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, raw)
	if len(cleaned) > 200 {
		cleaned = cleaned[:200]
	}
	if !rawNameShape.MatchString(cleaned) {
		return OffShapeName, "", nil, true
	}
	name := strings.ToLower(cleaned)
	if strings.HasPrefix(name, "/") || strings.HasPrefix(name, "$") {
		name = name[1:]
	}
	plugin, bare := "", name
	if i := strings.LastIndex(name, ":"); i >= 0 {
		plugin, bare = name[:i], name[i+1:]
	}
	if !slugShape.MatchString(bare) || (plugin != "" && !slugShape.MatchString(plugin)) {
		return cleaned, "", nil, false
	}
	return cleaned, plugin, &bare, false
}

// ---------------------------------------------------------------- the merge

// The predicates of the design 3.3 merge, spelled once. U: the derived copy
// onto any API row, whose every payload column then becomes the derived
// copy's, keyed on origin rather than trust because an lsd_ API row is
// itself a payload claim. M: two copies under one authority (two derived
// copies, which claimSession binds to one owner, or one API credential),
// which may only upgrade the row. N: a composite name beating a bare one
// for the same skill.
const (
	skillMergeU = `EXCLUDED.origin = 'derived' AND s.origin <> 'derived'`
	skillMergeM = `EXCLUDED.origin = s.origin AND (s.origin = 'derived' OR (EXCLUDED.device_id IS NOT DISTINCT FROM s.device_id AND EXCLUDED.source_token_id IS NOT DISTINCT FROM s.source_token_id))`
	skillMergeN = `s.plugin = '' AND EXCLUDED.plugin <> '' AND EXCLUDED.skill = s.skill`
)

// skillUpsertSQL is the one statement every copy of an invocation goes
// through. Built at package load from the predicates above; the fake
// connection test pins the rendered text.
var skillUpsertSQL = func() string {
	u, m, n := "("+skillMergeU+")", "("+skillMergeM+")", "("+skillMergeN+")"
	uArm := func(c string) string {
		return fmt.Sprintf("%s = CASE WHEN %s THEN EXCLUDED.%s ELSE s.%s END", c, u, c, c)
	}
	mArms := map[string]string{
		"outcome":      "CASE WHEN EXCLUDED.outcome IN ('success', 'error') THEN EXCLUDED.outcome ELSE s.outcome END",
		"plugin":       fmt.Sprintf("CASE WHEN %s THEN EXCLUDED.plugin ELSE s.plugin END", n),
		"skill_source": "CASE WHEN s.skill_source = 'unknown' THEN EXCLUDED.skill_source ELSE s.skill_source END",
		"event_id":     "COALESCE(s.event_id, EXCLUDED.event_id)",
		"link_ref":     "COALESCE(s.link_ref, EXCLUDED.link_ref)",
		"session_type": "CASE WHEN s.session_type = '' THEN EXCLUDED.session_type ELSE s.session_type END",
	}
	var b strings.Builder
	b.WriteString(`INSERT INTO skill_invocations AS s (
  occurred_at, time_clamped, origin, agent_platform, trust, device_id, source_token_id,
  raw_name, plugin, skill, skill_source, trigger, outcome, error_class,
  actor_email, actor_known, repo, session_ref, link_ref, session_type,
  event_id, prompt_id, tool_use_id, idempotency_key, dedupe_key,
  args_present, args_bytes, harness_version)
VALUES ($1, $2, $3, $4, $5, $6::uuid, $7::uuid, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)
ON CONFLICT (dedupe_key) DO UPDATE SET
  preempted_by     = CASE WHEN ` + u + ` THEN s.source_token_id ELSE s.preempted_by END,
  preempted_device = CASE WHEN ` + u + ` AND s.source_token_id IS NULL THEN s.device_id ELSE s.preempted_device END,
  source_token_id  = CASE WHEN ` + u + ` THEN NULL ELSE s.source_token_id END,
`)
	for _, c := range []string{"trust", "origin", "device_id", "actor_email", "actor_known", "skill", "trigger", "occurred_at",
		"time_clamped", "repo", "prompt_id", "tool_use_id", "args_present", "args_bytes", "harness_version"} {
		b.WriteString("  " + uArm(c) + ",\n")
	}
	fmt.Fprintf(&b, "  raw_name     = CASE WHEN %s OR (%s AND %s) THEN EXCLUDED.raw_name ELSE s.raw_name END,\n", u, m, n)
	fmt.Fprintf(&b, "  plugin       = CASE WHEN %s OR (%s AND %s) THEN EXCLUDED.plugin ELSE s.plugin END,\n", u, m, n)
	fmt.Fprintf(&b, "  skill_source = CASE WHEN %s OR (%s AND s.skill_source = 'unknown') THEN EXCLUDED.skill_source ELSE s.skill_source END,\n", u, m)
	// The outcome arms (design 3.3, ADV-LS1 F1): a derived copy always
	// arrives started, its result being the separate UPDATE of query 2,
	// so under U it takes the outcome only when it carries one. A hook's
	// PostToolUse that already stored success or error keeps it while
	// the rest of the row flips to the derived copy; the plain U would
	// have read success as started until the result's batch, or for good
	// when that result never pairs.
	fmt.Fprintf(&b, "  outcome      = CASE WHEN (%s AND EXCLUDED.outcome <> 'started') OR (%s AND EXCLUDED.outcome IN ('success', 'error')) THEN EXCLUDED.outcome ELSE s.outcome END,\n", u, m)
	fmt.Fprintf(&b, "  error_class  = CASE WHEN (%s AND EXCLUDED.outcome <> 'started') OR (%s AND EXCLUDED.outcome IN ('success', 'error')) THEN EXCLUDED.error_class ELSE s.error_class END,\n", u, m)
	fmt.Fprintf(&b, "  event_id     = CASE WHEN %s THEN EXCLUDED.event_id WHEN %s THEN COALESCE(s.event_id, EXCLUDED.event_id) ELSE s.event_id END,\n", u, m)
	fmt.Fprintf(&b, "  link_ref     = CASE WHEN %s THEN EXCLUDED.link_ref WHEN %s THEN COALESCE(s.link_ref, EXCLUDED.link_ref) ELSE s.link_ref END,\n", u, m)
	fmt.Fprintf(&b, "  session_type = CASE WHEN %s OR (%s AND s.session_type = '') THEN EXCLUDED.session_type ELSE s.session_type END\n", u, m)
	fmt.Fprintf(&b, "WHERE %s OR (%s AND (s.outcome, s.plugin, s.skill_source, s.event_id, s.link_ref, s.session_type)\n  IS DISTINCT FROM (%s, %s, %s, %s, %s, %s))\n",
		u, m, mArms["outcome"], mArms["plugin"], mArms["skill_source"], mArms["event_id"], mArms["link_ref"], mArms["session_type"])
	b.WriteString(`RETURNING (xmax = 0) AS inserted, (s.preempted_by IS NOT NULL OR s.preempted_device IS NOT NULL) AS preempted, s.preempted_by::text, s.preempted_device::text`)
	return b.String()
}()

// UpsertSkillInvocation writes one row through the merge. The key is
// computed here and nowhere else; a row whose ids fail their shape is a
// statement error the caller sees, since the derivation checks shapes
// before it gets here and the route answers 400 before it does.
func (s *Store) UpsertSkillInvocation(ctx context.Context, q Queryer, r SkillRow) (UpsertOutcome, error) {
	key, err := DedupeKey(r)
	if err != nil {
		return UpsertOutcome{}, err
	}
	if len(key) > dedupeKeyMax {
		return UpsertOutcome{}, fmt.Errorf("store: skill dedupe key of %d bytes passes the %d ceiling", len(key), dedupeKeyMax)
	}
	for _, id := range []*string{r.DeviceID, r.SourceTokenID} {
		if id != nil && *id != "" && !isUUID(*id) {
			return UpsertOutcome{}, fmt.Errorf("store: skill row id %q is not a uuid", *id)
		}
	}
	var out UpsertOutcome
	err = q.QueryRow(ctx, skillUpsertSQL,
		r.OccurredAt, r.TimeClamped, r.Origin, r.AgentPlatform, r.Trust, nilEmpty(r.DeviceID), nilEmpty(r.SourceTokenID),
		r.RawName, r.Plugin, r.Skill, r.SkillSource, r.Trigger, r.Outcome, r.ErrorClass,
		r.ActorEmail, r.ActorKnown, r.Repo, r.SessionRef, r.LinkRef, r.SessionType,
		nilEmpty(r.EventID), nilEmpty(r.PromptID), nilEmpty(r.ToolUseID), nilEmpty(r.IdempotencyKey), key,
		r.ArgsPresent, r.ArgsBytes, r.HarnessVersion,
	).Scan(&out.Inserted, &out.Preempted, &out.PreemptedBy, &out.PreemptedDevice)
	if err != nil {
		if noRows(err) {
			return UpsertOutcome{}, nil
		}
		return UpsertOutcome{}, fmt.Errorf("store: upsert skill invocation: %w", err)
	}
	out.Changed = !out.Inserted
	return out, nil
}

// nilEmpty is a NULL for an absent or empty id, so an empty string never
// reaches a ::uuid cast or a CHECK that wants a shape.
func nilEmpty(p *string) *string {
	if p == nil || *p == "" {
		return nil
	}
	return p
}

// ---------------------------------------------------------------- derivation

// skillCandidate is one row the derivation is about to write, with what it
// needs to fold and pair before the key is computed.
type skillCandidate struct {
	row      SkillRow
	origin   event.Origin
	envelope bool
	name     string
	when     time.Time
}

// skillInput is the Skill tool's input as the harness records it.
type skillInput struct {
	Skill string          `json:"skill"`
	Args  json.RawMessage `json:"args"`
}

// pairWindow is how far apart the hook copy and the transcript copy of one
// typed command may sit and still be one prompt (design 4a, CA-12).
const pairWindow = 120 * time.Second

// insertSkillInvocations derives the skill rows of a batch: Skill tool
// calls (form 1), their results (an UPDATE of outcome, never an insert,
// since a transcript result carries no tool name and an insert would have
// no raw_name), typed slash commands on both capture paths (form 2, or 2c
// for a transcript copy no hook copy pairs with) and Codex $skill mentions
// (2b). Rows are folded per key in Go and written sorted by key, so both
// copies of one prompt never raise 21000 inside one batch and two batches
// take their conflicts in one order.
//
// The recover exists because no other recover does under server/ or
// internal/: a panic while parsing a Raw envelope inside a request would
// be swallowed by net/http with the events uncommitted, and the agent
// retries that batch forever (OPS3-1). Under the recover the savepoint and
// the queue engage instead.
func (s *Store) insertSkillInvocations(ctx context.Context, q Queryer, fresh []Ingest) (skillDeriveStats, error) {
	st, _, err := s.deriveSkillRows(ctx, q, fresh, false)
	return st, err
}

// deriveSkillRows is insertSkillInvocations with one more answer: the
// form-2c keys this derivation produced, which the rebuild's (3b) DELETE
// keeps and nothing else reads. A transcript copy that paired keys form 2,
// so the set is what came out of the pairing, never what went in. With
// settled set, the rebuild's case, keys whose stored row the merge would
// leave as it is are not sent to it (settledSkillKeys).
func (s *Store) deriveSkillRows(ctx context.Context, q Queryer, fresh []Ingest, settled bool) (st skillDeriveStats, keys2c []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("skill derive panic: %v", r)
		}
	}()

	var (
		cands    []skillCandidate
		results  []Ingest // tool_result and tool_failed rows carrying a tool-use id
		callKeys = map[string]bool{}
		slashes  []skillCandidate // slash prompts, both copies, before pairing
	)
	for _, it := range fresh {
		e := it.Event
		switch e.Type {
		case event.ToolCall:
			if e.Tool == nil || e.Tool.Name != "Skill" || !derivedPlatform(e.Source) {
				continue
			}
			k := keysFor(it)
			if k.toolUse == "" {
				if e.ToolUseID != "" {
					// The harness named an id the key bound dropped: a
					// call that cannot be keyed, counted rather than lost.
					st.Candidates++
					st.SkippedShape++
				}
				continue
			}
			st.Candidates++
			var in skillInput
			if len(e.Tool.Input) > 0 {
				_ = json.Unmarshal(e.Tool.Input, &in)
			}
			row := s.derivedRow(it, TriggerAgent)
			row.ToolUseID = strPtr(k.toolUse)
			row.PromptID = strPtr(k.prompt)
			row.ArgsPresent = len(in.Args) > 0 && string(in.Args) != "null"
			row.ArgsBytes = argsBytes(in.Args)
			c := skillCandidate{row: row, origin: e.Origin, when: e.OccurredAt}
			keep, nerr := s.nameCandidate(ctx, q, &c, in.Skill, true, &st)
			if nerr != nil {
				return st, nil, nerr
			}
			if !keep {
				continue
			}
			callKeys[string(e.Source)+":"+e.SessionID+":t:"+k.toolUse] = true
			cands = append(cands, c)
		case event.ToolResult, event.ToolFailed:
			if !derivedPlatform(e.Source) {
				continue
			}
			if k := keysFor(it); k.toolUse != "" {
				results = append(results, it)
				if callKeys[string(e.Source)+":"+e.SessionID+":t:"+k.toolUse] {
					st.Candidates++
				}
			}
		case event.UserPrompt:
			// A subagent's prompt is the harness handing it its task, not a
			// person typing a command, so agent_id keeps only user_prompt
			// rows out (design 4a query 3, 3.6 step (2)). A subagent's
			// Skill call above is a row like the main thread's, keyed on
			// its tool-use id (R2's nested-skill trigger), and its result
			// is an outcome; dropping every subagent event undercounted
			// them on both paths alike (review-1 F2).
			if e.AgentID != "" || !derivedPlatform(e.Source) {
				continue
			}
			// The hook copy's text is the typed line. A Claude Code
			// transcript copy's is its record's message.content, never
			// Event.Text, which the walker sets to the arguments when the
			// command carried any. A Codex record has no message.content
			// and its Text is the person's words (design 4a).
			var text string
			var raw []byte
			switch {
			case e.Source == event.SourceCodex:
				raw, text = e.Raw, e.Text
			case e.Origin == event.OriginTranscript:
				raw = e.Raw
				text = normalize.ContentText(raw)
			default:
				text = e.Text
			}
			if name, envelope, ok := normalize.SlashCommand(text, raw); ok {
				st.Candidates++
				row := s.derivedRow(it, TriggerUser)
				row.PromptID = strPtr(keysFor(it).prompt)
				args := normalize.ClassifyUser(text, raw, false, false).Args
				row.ArgsPresent, row.ArgsBytes = args != "", len(args)
				slashes = append(slashes, skillCandidate{row: row, origin: e.Origin, envelope: envelope, name: name, when: e.OccurredAt})
				continue
			}
			if e.Source == event.SourceCodex {
				mentions, merr := s.codexMentions(ctx, q, it, text, raw, &st)
				if merr != nil {
					return st, nil, merr
				}
				cands = append(cands, mentions...)
			}
		}
	}

	slashes, err = s.pairSlashCopies(ctx, q, slashes)
	if err != nil {
		return st, nil, err
	}
	// The live path only: the rebuild's batch is the whole session, so
	// its pairing is the final word and nothing is left for the queue.
	var twins []string
	if !settled {
		if twins, err = storedTwinsOf(ctx, q, slashes); err != nil {
			return st, nil, err
		}
	}
	for i := range slashes {
		c := slashes[i]
		if c.origin == event.OriginHook && c.row.PromptID == nil {
			// A hook copy without prompt_id is never a row: it stores no
			// Raw for event_keys to fill from, so nothing would ever key it.
			continue
		}
		keep, nerr := s.nameCandidate(ctx, q, &c, c.name, c.envelope, &st)
		if nerr != nil {
			return st, nil, nerr
		}
		if !keep {
			continue
		}
		cands = append(cands, c)
	}

	// Fold per key. A composite name beats a bare one for the same skill
	// (G8) and the envelope copy beats the typed one, so the transcript's
	// canonical plugin:skill is what the row carries when both copies are
	// in one batch.
	byKey := map[string]*skillCandidate{}
	var keys []string
	for i := range cands {
		c := &cands[i]
		if !s.shapeOK(c, &st) {
			continue
		}
		key, err := DedupeKey(c.row)
		if err != nil || len(key) > dedupeKeyMax {
			st.SkippedShape++
			continue
		}
		if have, ok := byKey[key]; ok {
			// Both copies of one prompt in one batch: one row, and the
			// 7.1 line counts the copy it absorbed as a duplicate.
			foldCandidate(have, c)
			st.Duplicates++
			continue
		}
		byKey[key] = c
		keys = append(keys, key)
		if c.row.ToolUseID == nil && c.row.PromptID == nil && c.row.AgentPlatform == PlatformClaudeCode {
			keys2c = append(keys2c, key)
		}
	}
	sort.Strings(keys)
	if settled && len(keys) > 0 {
		done, err := settledSkillKeys(ctx, q, keys, byKey)
		if err != nil {
			return st, nil, err
		}
		// A new slice, not keys[:0]: the read above was handed keys.
		kept := make([]string, 0, len(keys))
		for _, key := range keys {
			if done[key] {
				st.Duplicates++
				continue
			}
			kept = append(kept, key)
		}
		keys = kept
	}
	for _, key := range keys {
		c := byKey[key]
		out, err := s.UpsertSkillInvocation(ctx, q, c.row)
		if err != nil {
			return st, nil, err
		}
		// out.Preempted is observed here and nowhere else: the derived
		// copy displaced an API row on its key (predicate U), and the line
		// names the token or the device that had put it there, which is
		// the one trace of a pre-insert campaign (design 7.1; skill_preempted
		// and the third condition of policy 17 read it).
		if out.Preempted {
			s.logger().WarnContext(ctx, "skill invocation preempted",
				slog.String("platform", c.row.AgentPlatform),
				slog.String("preempted_by", deref(out.PreemptedBy)),
				slog.String("preempted_device", deref(out.PreemptedDevice)),
				slog.String("session_ref", c.row.SessionRef),
				slog.String("skill", deref(c.row.Skill)))
		}
		switch {
		case out.Inserted || out.Changed:
			if c.row.Trigger == TriggerAgent {
				st.RowsAgent++
			} else {
				st.RowsUser++
			}
		default:
			st.Duplicates++
		}
	}

	if len(results) > 0 {
		n, err := updateSkillOutcomes(ctx, q, results)
		if err != nil {
			return st, nil, err
		}
		st.OutcomeUpdates += n
	}
	if len(twins) > 0 {
		if err := s.EnqueueSkillRederive(ctx, q, twins, "twin"); err != nil {
			return st, nil, err
		}
	}
	st.Touched = st.RowsUser+st.RowsAgent+st.OutcomeUpdates+st.Duplicates > 0
	return st, keys2c, nil
}

// storedTwinsOf finds, for each hook copy of the batch with no transcript
// twin in it, a stored 2c row of the same session and skill within the
// pairing window: the twin arrived in an earlier batch and keyed 2c there,
// the hook copy keys form 2 here, and only a rebuild folds the two (design
// 3.6, 3b). It reports those sessions, which the caller queues with reason
// twin, so the fold happens at the next dirty tick rather than the next
// DerivedSchema bump (ADV-LS1 F7). A hook copy is a twin's when a
// transcript copy of the batch carries its prompt id, adopted in the
// pairing or written by the harness; a built-in's twin is never a row, so
// its copies are not looked for. The read is by session over the 2c
// shape (derived, claude_code, neither harness id), so a stored twin that
// paired at its own ingest, whose row keys form 2, is not one.
func storedTwinsOf(ctx context.Context, q Queryer, slashes []skillCandidate) ([]string, error) {
	twinned := map[string]bool{}
	for _, c := range slashes {
		if c.origin == event.OriginTranscript && c.row.PromptID != nil {
			twinned[c.row.SessionRef+"\x00"+*c.row.PromptID] = true
		}
	}
	var (
		lonely   []skillCandidate
		sessions = map[string]bool{}
		lo, hi   time.Time
	)
	for _, c := range slashes {
		if c.origin != event.OriginHook || c.row.PromptID == nil || twinned[c.row.SessionRef+"\x00"+*c.row.PromptID] {
			continue
		}
		_, plugin, skill, offShape := NormalizeSkillName(c.name)
		if offShape || (plugin == "" && normalize.IsBuiltinCommand(*skill)) {
			continue
		}
		lonely = append(lonely, c)
		sessions[c.row.SessionRef] = true
		if lo.IsZero() || c.when.Before(lo) {
			lo = c.when
		}
		if hi.IsZero() || c.when.After(hi) {
			hi = c.when
		}
	}
	if len(lonely) == 0 {
		return nil, nil
	}
	rows, err := q.Query(ctx, `
		SELECT session_ref, skill, occurred_at FROM skill_invocations
		WHERE session_ref = ANY($1::text[]) AND origin = 'derived' AND agent_platform = 'claude_code'
		  AND prompt_id IS NULL AND tool_use_id IS NULL AND skill IS NOT NULL
		  AND occurred_at BETWEEN $2 AND $3`, sortedKeys(sessions), lo.Add(-pairWindow), hi.Add(pairWindow))
	if err != nil {
		return nil, fmt.Errorf("store: read stored twins for the queue: %w", err)
	}
	defer rows.Close()
	twins := map[string]bool{}
	for rows.Next() {
		var (
			sid, skill string
			when       time.Time
		)
		if err := rows.Scan(&sid, &skill, &when); err != nil {
			return nil, fmt.Errorf("store: scan stored twin: %w", err)
		}
		for _, h := range lonely {
			if h.row.SessionRef == sid && bareCommand(h.name) == skill && absDuration(when.Sub(h.when)) <= pairWindow {
				twins[sid] = true
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read stored twins for the queue: %w", err)
	}
	return sortedKeys(twins), nil
}

// settledSkillKeys is the step's prefilter (design 4a): the keys whose
// stored derived row the merge's M arm would leave as it is, read in one
// query and compared in Go on the six columns the merge's WHERE compares.
// ON CONFLICT DO UPDATE locks the conflicting row for the rest of the
// transaction even when its WHERE is false (design 3.6), and the step's
// transaction spans fifty sessions, so a row the step would not change is
// a row it must not touch: ingest of a live session would wait on it, up
// to its 2 s bound, and defer its skill rows to the queue for nothing.
// A stored row of another origin is never settled, since the U arm
// rewrites it. The live path skips the read: its rows are fresh.
func settledSkillKeys(ctx context.Context, q Queryer, keys []string, cands map[string]*skillCandidate) (map[string]bool, error) {
	rows, err := q.Query(ctx, `
		SELECT dedupe_key, outcome, plugin, skill, skill_source, event_id, link_ref, session_type
		FROM skill_invocations WHERE dedupe_key = ANY($1::text[]) AND origin = 'derived'`, keys)
	if err != nil {
		return nil, fmt.Errorf("store: read stored skill rows: %w", err)
	}
	defer rows.Close()
	done := map[string]bool{}
	for rows.Next() {
		var (
			key, outcome, plugin, source, sessionType string
			skill, eventID, linkRef                   *string
		)
		if err := rows.Scan(&key, &outcome, &plugin, &skill, &source, &eventID, &linkRef, &sessionType); err != nil {
			return nil, fmt.Errorf("store: scan stored skill row: %w", err)
		}
		c, ok := cands[key]
		if !ok {
			continue
		}
		r := c.row
		// One case per M arm, in the order of the merge's WHERE tuple; a
		// true is a column the arm would move.
		switch {
		case (r.Outcome == OutcomeSuccess || r.Outcome == OutcomeError) && r.Outcome != outcome:
		case plugin == "" && r.Plugin != "" && skill != nil && r.Skill != nil && *skill == *r.Skill:
		case source == SkillSourceUnknown && r.SkillSource != source:
		case eventID == nil && nilEmpty(r.EventID) != nil:
		case linkRef == nil && nilEmpty(r.LinkRef) != nil:
		case sessionType == "" && r.SessionType != "":
		default:
			done[key] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read stored skill rows: %w", err)
	}
	return done, nil
}

// derivedPlatform reports whether events from a source become derived
// rows: the two harnesses whose transcripts the store holds.
func derivedPlatform(src event.Source) bool {
	return src == event.SourceClaudeCode || src == event.SourceCodex
}

// derivedRow is the column map every derived row shares (design 4a): the
// same-named Event and Ingest fields, trust device because a device token
// delivered the events, repo and harness_version coerced to ” outside
// their CHECKs rather than refused.
func (s *Store) derivedRow(it Ingest, trigger string) SkillRow {
	e := it.Event
	row := SkillRow{
		Origin:        OriginDerived,
		AgentPlatform: string(e.Source),
		Trust:         TrustDevice,
		Trigger:       trigger,
		Outcome:       OutcomeStarted,
		SkillSource:   SkillSourceUnknown,
		ActorEmail:    strPtr(it.Email),
		ActorKnown:    true,
		SessionRef:    e.SessionID,
		EventID:       strPtr(e.ID),
		OccurredAt:    e.OccurredAt,
	}
	if it.DeviceID != "" && isUUID(it.DeviceID) {
		row.DeviceID = strPtr(it.DeviceID)
	}
	if skillRepoShape.MatchString(it.Repo) {
		row.Repo = it.Repo
	}
	if skillBuildShape.MatchString(e.HarnessVersion) {
		row.HarnessVersion = e.HarnessVersion
	}
	return row
}

// nameCandidate fills raw_name, plugin, skill and skill_source from a
// name, drops a built-in, and for a name the shape rule read rather than
// the harness wrote asks the alias oracle, which under LS-1 is nil and
// reads as unconfirmed. It reports whether the candidate survives.
//
// A FAILED lookup is returned, never folded into "unconfirmed": the two
// are opposite facts about the same name, and treating an error as a
// non-confirmation dropped the row for good, with nothing to bring it
// back short of a DerivedSchema bump. Returning it gives the derivation's
// savepoint the failure instead, which rolls the skill block back, leaves
// the events committed and queues the session for the dirty tick.
func (s *Store) nameCandidate(ctx context.Context, q Queryer, c *skillCandidate, name string, envelope bool, st *skillDeriveStats) (bool, error) {
	rawName, plugin, skill, offShape := NormalizeSkillName(name)
	if offShape {
		st.SkippedShape++
	}
	c.row.RawName, c.row.Plugin, c.row.Skill = rawName, plugin, skill
	if plugin != "" {
		c.row.SkillSource = SkillSourcePlugin
	}
	if c.row.Trigger == TriggerUser && plugin == "" {
		bare := name
		if skill != nil {
			bare = *skill
		}
		if normalize.IsBuiltinCommand(bare) {
			st.SkippedBuiltin++
			return false, nil
		}
	}
	if c.row.Trigger == TriggerUser && !envelope {
		ok, err := s.confirmAlias(ctx, q, plugin, skill)
		if err != nil {
			st.OracleErrors++
			return false, err
		}
		if !ok {
			st.Unconfirmed++
			return false, nil
		}
	}
	return true, nil
}

// confirmAlias reads the alias oracle on the derivation's own Queryer:
// nil, as LS-1 ships it, is no confirmation for any name.
func (s *Store) confirmAlias(ctx context.Context, q Queryer, plugin string, skill *string) (bool, error) {
	if s.AliasOracle == nil || skill == nil {
		return false, nil
	}
	return s.AliasOracle(ctx, q, plugin, *skill)
}

// shapeOK applies the id coercions of design 4a: a session_ref, prompt_id
// or tool_use_id outside the id shape is a row the CHECK would refuse, so
// it is skipped and counted rather than failing the batch.
func (s *Store) shapeOK(c *skillCandidate, st *skillDeriveStats) bool {
	if !skillIDShape.MatchString(c.row.SessionRef) {
		st.SkippedShape++
		return false
	}
	for _, id := range []*string{c.row.PromptID, c.row.ToolUseID} {
		if id != nil && *id != "" && !skillIDShape.MatchString(*id) {
			st.SkippedShape++
			return false
		}
	}
	return true
}

// foldCandidate merges a second copy of one key into the first: the
// composite name for the same skill wins, the envelope copy's name wins,
// and args presence is the union.
func foldCandidate(have, other *skillCandidate) {
	sameSkill := have.row.Skill != nil && other.row.Skill != nil && *have.row.Skill == *other.row.Skill
	if (have.row.Plugin == "" && other.row.Plugin != "" && sameSkill) || (other.envelope && !have.envelope) {
		have.row.RawName, have.row.Plugin, have.row.Skill, have.row.SkillSource = other.row.RawName, other.row.Plugin, other.row.Skill, other.row.SkillSource
		have.envelope = have.envelope || other.envelope
	}
	if other.row.ArgsPresent && !have.row.ArgsPresent {
		have.row.ArgsPresent, have.row.ArgsBytes = true, other.row.ArgsBytes
	}
	if have.row.PromptID == nil && other.row.PromptID != nil {
		have.row.PromptID = other.row.PromptID
	}
}

// pairSlashCopies applies the 4a pairing rule (CA-12): a transcript copy
// without a prompt_id adopts the prompt_id of the nearest same-name hook
// copy of its session within the window, first among the batch, then
// among stored hook rows, each hook copy pairing at most once. A hook copy
// stores no Raw for the event_keys step to fill from, so a transcript copy
// that pairs here keys form 2 and one that does not keys 2c; the rebuild
// removes a 2c row whose twin arrived in a later batch (design 3.6, 3b).
func (s *Store) pairSlashCopies(ctx context.Context, q Queryer, slashes []skillCandidate) ([]skillCandidate, error) {
	var orphans []int
	for i, c := range slashes {
		if c.origin == event.OriginTranscript && c.row.PromptID == nil {
			orphans = append(orphans, i)
		}
	}
	if len(orphans) == 0 {
		return slashes, nil
	}
	var pairs []nearPair
	for _, oi := range orphans {
		o := slashes[oi]
		for hi, h := range slashes {
			if h.origin != event.OriginHook || h.row.PromptID == nil || h.row.SessionRef != o.row.SessionRef || bareCommand(h.name) != bareCommand(o.name) {
				continue
			}
			if gap := absDuration(h.when.Sub(o.when)); gap <= pairWindow {
				pairs = append(pairs, nearPair{orphan: oi, hook: hi, gap: gap})
			}
		}
	}
	usedHooks := map[string]bool{}
	matchNearest(pairs, func(oi, hi int) {
		slashes[oi].row.PromptID = slashes[hi].row.PromptID
		if id := slashes[hi].row.EventID; id != nil {
			usedHooks[*id] = true
		}
	})

	// The stored copies of the batch's sessions: the hook copies inside
	// the window of the orphans still unpaired, and the transcript copies
	// without a prompt_id inside the window of those hook copies, which
	// compete for them. A hook copy that an earlier batch's orphan sits
	// nearer to is that orphan's (the rebuild pairs them, design 3.6), so
	// an orphan of this batch must not take it; without the competitors
	// every later batch could adopt the same stored hook copy once more.
	// The batch's own rows are in events already, under this
	// transaction, and are skipped by id: the pass above offered every
	// hook copy among them. events has no text column, so a hook copy's
	// name is read from its stored body the way the hook wrote it, and a
	// transcript copy's from its stored record, as the live path reads it.
	var still []int
	sessions := map[string]bool{}
	batchIDs := map[string]bool{}
	var lo, hi time.Time
	for _, c := range slashes {
		if c.row.EventID != nil {
			batchIDs[*c.row.EventID] = true
		}
	}
	for _, oi := range orphans {
		o := slashes[oi]
		if o.row.PromptID != nil {
			continue
		}
		still = append(still, oi)
		sessions[o.row.SessionRef] = true
		if lo.IsZero() || o.when.Before(lo) {
			lo = o.when
		}
		if hi.IsZero() || o.when.After(hi) {
			hi = o.when
		}
	}
	if len(still) == 0 {
		return slashes, nil
	}
	rows, err := q.Query(ctx, `
		SELECT id, origin, session_id, coalesce(prompt_id, ''), occurred_at,
		       CASE WHEN origin = 'hook' THEN coalesce(body->>'text', '') ELSE coalesce(body->>'raw', '') END
		FROM events
		WHERE session_id = ANY($1::text[]) AND type = 'user_prompt' AND agent_id IS NULL
		  AND ((origin = 'hook' AND prompt_id IS NOT NULL AND prompt_id <> '')
		    OR (origin = 'transcript' AND coalesce(prompt_id, '') = ''))
		  AND occurred_at BETWEEN $2 AND $3`, sortedKeys(sessions), lo.Add(-2*pairWindow), hi.Add(2*pairWindow))
	if err != nil {
		return nil, fmt.Errorf("store: read stored copies for pairing: %w", err)
	}
	type storedCopy struct {
		id, sid, pid, name string
		at                 time.Time
	}
	var hooks, competitors []storedCopy
	for rows.Next() {
		var (
			c            storedCopy
			origin, text string
		)
		if err := rows.Scan(&c.id, &origin, &c.sid, &c.pid, &c.at, &text); err != nil {
			rows.Close()
			return nil, fmt.Errorf("store: scan stored copy: %w", err)
		}
		if batchIDs[c.id] || usedHooks[c.id] {
			continue
		}
		if origin == string(event.OriginHook) {
			if name, _, ok := normalize.SlashCommand(text, nil); ok {
				c.name = name
				hooks = append(hooks, c)
			}
			continue
		}
		raw := []byte(text)
		if name, _, ok := normalize.SlashCommand(normalize.ContentText(raw), raw); ok {
			c.name = name
			competitors = append(competitors, c)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: read stored copies for pairing: %w", err)
	}
	// Orphans of this batch first, then the stored competitors, so on an
	// equal gap the batch's copy keeps the hook copy.
	pairs = pairs[:0]
	orphanAt := func(oi int) (sid, name string, at time.Time) {
		if oi < len(still) {
			o := slashes[still[oi]]
			return o.row.SessionRef, o.name, o.when
		}
		c := competitors[oi-len(still)]
		return c.sid, c.name, c.at
	}
	for oi := range len(still) + len(competitors) {
		sid, name, at := orphanAt(oi)
		for hi, h := range hooks {
			if h.sid != sid || bareCommand(h.name) != bareCommand(name) {
				continue
			}
			if gap := absDuration(h.at.Sub(at)); gap <= pairWindow {
				pairs = append(pairs, nearPair{orphan: oi, hook: hi, gap: gap})
			}
		}
	}
	matchNearest(pairs, func(oi, hi int) {
		if oi < len(still) {
			slashes[still[oi]].row.PromptID = strPtr(hooks[hi].pid)
		}
	})
	return slashes, nil
}

// bareCommand is the name both copies of one typed command share: the
// hook copy is the short form a person typed and the transcript envelope
// is the canonical plugin:skill (G8), so the pairing compares the bare
// skill, the term the merge's N predicate compares too.
func bareCommand(name string) string {
	name = strings.ToLower(name)
	if i := strings.LastIndex(name, ":"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// nearPair is one (transcript copy, hook copy) pair inside the window.
type nearPair struct {
	orphan, hook int
	gap          time.Duration
}

// matchNearest gives each orphan the nearest hook copy, nearest pairs
// first across the whole set and each side taken once, so two copies
// typed a minute apart pair with their own hook copies whatever order the
// batch arrived in. Ties keep the pairs' order.
func matchNearest(pairs []nearPair, adopt func(orphan, hook int)) {
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].gap < pairs[j].gap })
	usedOrphan, usedHook := map[int]bool{}, map[int]bool{}
	for _, p := range pairs {
		if usedOrphan[p.orphan] || usedHook[p.hook] {
			continue
		}
		usedOrphan[p.orphan], usedHook[p.hook] = true, true
		adopt(p.orphan, p.hook)
	}
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// codexMentions reads the $skill mentions of a Codex prompt, on a
// person's own text only: Codex files environment and instruction blocks
// under the user role, and shell text such as $path would match there.
// Each kept mention is its own row keyed on the event and the skill, and
// is kept only when the alias oracle confirms it.
func (s *Store) codexMentions(ctx context.Context, q Queryer, it Ingest, text string, raw []byte, st *skillDeriveStats) ([]skillCandidate, error) {
	if normalize.ClassifyUser(text, raw, false, false).Kind != normalize.KindHuman {
		return nil, nil
	}
	var out []skillCandidate
	seen := map[string]bool{}
	for _, m := range codexMention.FindAllStringSubmatch(text, -1) {
		name := m[1]
		if seen[name] {
			continue
		}
		seen[name] = true
		st.Candidates++
		row := s.derivedRow(it, TriggerUser)
		c := skillCandidate{row: row, origin: it.Event.Origin, name: name, when: it.Event.OccurredAt}
		keep, err := s.nameCandidate(ctx, q, &c, name, false, st)
		if err != nil {
			return nil, err
		}
		if !keep {
			continue
		}
		c.row.SkillSource = SkillSourceMirror
		out = append(out, c)
	}
	return out, nil
}

// updateSkillOutcomes is the second query of design 4a: the outcome of a
// started row, keyed on the form-1 key of every result in the batch. Never
// an insert. A result that lands before its call leaves the row started;
// the rebuild repairs it.
func updateSkillOutcomes(ctx context.Context, q Queryer, results []Ingest) (int, error) {
	keys := make([]string, 0, len(results))
	outcomes := make([]string, 0, len(results))
	for _, it := range results {
		k := keysFor(it)
		if !skillIDShape.MatchString(k.toolUse) || !skillIDShape.MatchString(it.Event.SessionID) {
			continue
		}
		keys = append(keys, string(it.Event.Source)+":"+it.Event.SessionID+":t:"+k.toolUse)
		if it.Event.Type == event.ToolFailed {
			outcomes = append(outcomes, OutcomeError)
		} else {
			outcomes = append(outcomes, OutcomeSuccess)
		}
	}
	if len(keys) == 0 {
		return 0, nil
	}
	n, err := q.Exec(ctx, `
		UPDATE skill_invocations si
		SET outcome = u.outcome, error_class = CASE WHEN u.outcome = 'error' THEN 'runtime_error' END
		FROM unnest($1::text[], $2::text[]) AS u(dedupe_key, outcome)
		WHERE si.dedupe_key = u.dedupe_key AND si.outcome IN ('started', 'unknown')`, keys, outcomes)
	if err != nil {
		return 0, fmt.Errorf("store: update skill outcomes: %w", err)
	}
	return int(n), nil
}

func argsBytes(raw json.RawMessage) int {
	if len(raw) == 0 || string(raw) == "null" {
		return 0
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return len(s)
	}
	return len(raw)
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ---------------------------------------------------------------- ingest block

// isLockWait reports the two states a row held by a derive batch produces
// under a lock_timeout: the wait itself, and a deadlock Postgres broke.
func isLockWait(err error) bool {
	st := sqlState(err)
	return st == "55P03" || st == "40P01"
}

// deriveSkillsAtIngest is the block UpsertEvents runs after insertLinks
// over the fresh rows (design 4a). Under a savepoint with its own lock
// bound, so a skill row never fails an events batch: a failure rolls the
// skill work back, logs it under the alert or as a deferral, and queues
// the sessions for the dirty tick. The lock bound is reset on both paths
// because a SET LOCAL survives RELEASE SAVEPOINT until commit, and the
// derive_dirty UPDATE that follows would otherwise fail 55P03 behind a
// fold's row lock where today it waits under the statement timeout.
func (s *Store) deriveSkillsAtIngest(ctx context.Context, tx Queryer, fresh []Ingest) {
	_, _ = tx.Exec(ctx, "SAVEPOINT skill_inv")
	_, _ = tx.Exec(ctx, "SET LOCAL lock_timeout = '2s'")
	st, err := s.insertSkillInvocations(ctx, tx, fresh)
	if err != nil {
		_, _ = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT skill_inv")
		sessions := sessionsOf(fresh)
		if isLockWait(err) {
			s.logSkillDeriveDeferred(ctx, fresh, err, len(sessions))
		} else {
			s.logSkillDeriveFailed(ctx, fresh, err)
		}
		s.queueSkillRederive(ctx, tx, sessions, "ingest")
	} else {
		_, _ = tx.Exec(ctx, "RELEASE SAVEPOINT skill_inv")
		if st.Candidates > 0 || st.OutcomeUpdates > 0 {
			s.logSkillDerived(ctx, fresh, st)
		}
	}
	_, _ = tx.Exec(ctx, "SET LOCAL lock_timeout = 0")
}

// queueSkillRederive enqueues the batch's sessions under a savepoint of
// its own, best effort: the queue is how a lost skill row comes back, and
// a queue write that failed the events batch would lose more than it
// saves.
func (s *Store) queueSkillRederive(ctx context.Context, tx Queryer, sessions []string, reason string) {
	_, _ = tx.Exec(ctx, "SAVEPOINT skill_q")
	if err := s.EnqueueSkillRederive(ctx, tx, sessions, reason); err != nil {
		_, _ = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT skill_q")
		return
	}
	_, _ = tx.Exec(ctx, "RELEASE SAVEPOINT skill_q")
}

func sessionsOf(items []Ingest) []string {
	seen := map[string]bool{}
	for _, it := range items {
		seen[it.Event.SessionID] = true
	}
	return sortedKeys(seen)
}

// EnqueueSkillRederive records sessions whose skill rows must be derived
// again. A session already queued keeps its place in line and is unparked:
// a new reason to re-derive it is a new chance for a rebuild that had
// given up.
func (s *Store) EnqueueSkillRederive(ctx context.Context, q Queryer, sessionIDs []string, reason string) error {
	if len(sessionIDs) == 0 {
		return nil
	}
	if _, err := q.Exec(ctx, `
		INSERT INTO skill_rederive_queue (session_id, reason)
		SELECT u.session_id, $2 FROM unnest($1::text[]) AS u(session_id)
		ON CONFLICT (session_id) DO UPDATE SET reason = EXCLUDED.reason, attempts = 0, last_error = ''`,
		sessionIDs, reason); err != nil {
		return fmt.Errorf("store: enqueue skill rederive: %w", err)
	}
	return nil
}

// EnqueueSkillRederiveSince queues every session touched at or after a
// moment, with the rollback reason: the window a version-3 revision served
// got no skill rows and nothing else revisits it (design 9.5). It reports
// how many sessions were queued.
func (s *Store) EnqueueSkillRederiveSince(ctx context.Context, q Queryer, since time.Time) (int64, error) {
	return enqueueSkillRederiveSince(ctx, q, since)
}

func enqueueSkillRederiveSince(ctx context.Context, q Queryer, since time.Time) (int64, error) {
	n, err := q.Exec(ctx, `
		INSERT INTO skill_rederive_queue (session_id, reason)
		SELECT session_id, 'rollback' FROM sessions WHERE updated_at >= $1
		ON CONFLICT (session_id) DO UPDATE SET reason = EXCLUDED.reason, attempts = 0, last_error = ''`, since)
	if err != nil {
		return 0, fmt.Errorf("store: enqueue skill rederive since %s: %w", since.Format(time.RFC3339), err)
	}
	return n, nil
}

// ResetSkillDeriveStep is the targeted repair of design 9.5: the one step
// re-runs, nothing else does. started_at is cleared with the rest because
// a kept one would make stalledMinutes read days and fire the stalled
// alert at once. The stamp is lowered to the version before this one and
// never raised: a database still at 0 or 2 (fresh, or before its
// version-3 pass) has no version-4 ledger rows yet, and a stamp set to 3
// there would make ensureDeriveJobs seed the thirteen version-3 steps
// finished on a database that never ran them (ADV-LS1 F4).
func (s *Store) ResetSkillDeriveStep(ctx context.Context, q Queryer) error {
	return resetSkillDeriveStep(ctx, q)
}

func resetSkillDeriveStep(ctx context.Context, q Queryer) error {
	if _, err := q.Exec(ctx, `
		UPDATE derive_jobs SET finished_at = NULL, started_at = NULL, updated_at = now(), last_error = '', cursor = '', attempts = 0, processed = 0
		WHERE version = $1 AND step = 'skill_invocations'`, DerivedSchema); err != nil {
		return fmt.Errorf("store: reset the skill_invocations step: %w", err)
	}
	if _, err := q.Exec(ctx, `UPDATE derived_schema SET version = LEAST(version, $1) WHERE only_row`, DerivedSchema-1); err != nil {
		return fmt.Errorf("store: lower the derived stamp: %w", err)
	}
	return nil
}

// RecordAdminAction writes the audit row behind an admin mutation (DEV-i),
// inside the caller's transaction so the row and the change land together
// or not at all. detail is the request minus its secrets; nil is an empty
// object.
func (s *Store) RecordAdminAction(ctx context.Context, q Queryer, actor, action, target string, detail any) error {
	return recordAdminAction(ctx, q, actor, action, target, detail)
}

func recordAdminAction(ctx context.Context, q Queryer, actor, action, target string, detail any) error {
	body := []byte("{}")
	if detail != nil {
		b, err := json.Marshal(detail)
		if err != nil {
			return fmt.Errorf("store: marshal admin action detail: %w", err)
		}
		body = b
	}
	if _, err := q.Exec(ctx, `INSERT INTO admin_actions (actor, action, target, detail) VALUES ($1, $2, $3, $4::jsonb)`,
		actor, action, target, string(body)); err != nil {
		return fmt.Errorf("store: record admin action %s: %w", action, err)
	}
	return nil
}

// ---------------------------------------------------------------- log lines

// The 7.1 lines. Ids and counts only: the skill name, its arguments and
// the prompt never reach a line, and the tests grep the log for them.

func (s *Store) logSkillDerived(ctx context.Context, fresh []Ingest, st skillDeriveStats) {
	email, device, platform := batchIdentity(fresh)
	s.logger().InfoContext(ctx, "skill invocations derived",
		slog.String("email", email),
		slog.String("device_id", device),
		slog.String("agent_platform", platform),
		slog.Int("candidates", st.Candidates),
		slog.Int("rows_user", st.RowsUser),
		slog.Int("rows_agent", st.RowsAgent),
		slog.Int("outcome_updates", st.OutcomeUpdates),
		slog.Int("duplicates", st.Duplicates),
		slog.Int("skipped_builtin", st.SkippedBuiltin),
		slog.Int("skipped_shape", st.SkippedShape),
		slog.Int("unconfirmed", st.Unconfirmed),
		slog.Int("oracle_errors", st.OracleErrors),
		slog.Bool("touched", st.Touched))
}

func (s *Store) logSkillDeriveFailed(ctx context.Context, fresh []Ingest, err error) {
	_, _, platform := batchIdentity(fresh)
	s.logger().ErrorContext(ctx, "skill invocation store failed",
		slog.String("op", "derive"),
		slog.String("error", err.Error()),
		slog.String("method", "POST"),
		slog.String("path", "/v1/events"),
		slog.String("platform", platform))
}

func (s *Store) logSkillDeriveDeferred(ctx context.Context, fresh []Ingest, err error, sessions int) {
	email, device, _ := batchIdentity(fresh)
	s.logger().WarnContext(ctx, "skill invocation deferred",
		slog.String("email", email),
		slog.String("device_id", device),
		slog.String("sqlstate", sqlState(err)),
		slog.Int("sessions", sessions))
}

func batchIdentity(fresh []Ingest) (email, device, platform string) {
	if len(fresh) == 0 {
		return "", "", ""
	}
	return fresh[0].Email, fresh[0].DeviceID, string(fresh[0].Event.Source)
}

// ---------------------------------------------------------------- rebuild

// errSkillLockWait is the sentinel the skill_invocations step returns when
// a row it needs is held by ingest: perSession maps it to a rollback, a
// pause and the same batch again, never a skipped session.
var errSkillLockWait = errors.New("store: skill rebuild waited on a lock")

// rebuildSessionSkillInvocations derives one session's skill rows again
// from its stored events, inside the caller's transaction and through the
// same insertSkillInvocations the live path calls (design 3.6). Left as it
// is when any body has expired, as the links rebuild is. No DELETE of the
// API or form 1, 2, 2b rows, since the upsert is idempotent; the one
// DELETE (3b) is of the derived 2c rows this rebuild did not regenerate,
// which are ingest-time rows whose hook twin arrived in a later batch. It
// reports the rows it wrote or updated.
func (s *Store) rebuildSessionSkillInvocations(ctx context.Context, q Queryer, sessionID string) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("skill derive panic: %v", r)
		}
	}()
	var expired bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM events WHERE session_id = $1 AND body_expired_at IS NOT NULL)`,
		sessionID).Scan(&expired); err != nil {
		return 0, fmt.Errorf("store: probe expired bodies of %s: %w", sessionID, err)
	}
	if expired {
		return 0, nil
	}
	var (
		source, repo string
		deviceID     *string
	)
	if err := q.QueryRow(ctx, `SELECT source, coalesce(repo, ''), device_id::text FROM sessions WHERE session_id = $1`, sessionID).Scan(&source, &repo, &deviceID); err != nil {
		if noRows(err) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("store: read session %s for the skill rebuild: %w", sessionID, err)
	}
	rows, err := q.Query(ctx, `
		SELECT id, email, seq, occurred_at, body
		FROM events
		WHERE session_id = $1 AND (
		     (type = 'tool_call' AND tool_name = 'Skill')
		  OR (type IN ('tool_result', 'tool_failed') AND coalesce(tool_use_id, body->>'tool_use_id') IN (
		        SELECT coalesce(tool_use_id, body->>'tool_use_id') FROM events
		        WHERE session_id = $1 AND type = 'tool_call' AND tool_name = 'Skill'))
		  OR (type = 'user_prompt' AND agent_id IS NULL))
		ORDER BY occurred_at, seq`, sessionID)
	if err != nil {
		return 0, fmt.Errorf("store: read skill events of %s: %w", sessionID, err)
	}
	var items []Ingest
	for rows.Next() {
		var (
			it      Ingest
			id, eml string
			seq     int64
			at      time.Time
			body    []byte
		)
		if err := rows.Scan(&id, &eml, &seq, &at, &body); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: scan skill event: %w", err)
		}
		if err := json.Unmarshal(body, &it.Event); err != nil {
			// A body this store wrote and cannot read back is a bug worth a
			// parked session, not a silent gap.
			rows.Close()
			return 0, fmt.Errorf("store: decode event %s: %w", id, err)
		}
		it.Event.ID, it.Event.SessionID, it.Event.Seq, it.Event.OccurredAt = id, sessionID, seq, at
		it.Event.Source = event.Source(source)
		it.Email, it.Repo = eml, repo
		if deviceID != nil {
			it.DeviceID = *deviceID
		}
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: read skill events of %s: %w", sessionID, err)
	}

	st, keep, err := s.deriveSkillRows(ctx, q, items, true)
	if err != nil {
		if isLockWait(err) {
			return 0, fmt.Errorf("%w: %w", errSkillLockWait, err)
		}
		return 0, err
	}
	// (3b): the 2c keys this rebuild produced are the ones that may stay.
	// A transcript copy whose hook twin arrived in a later batch keyed 2c
	// at ingest and form 2 here, so its 2c row is what the DELETE finds.
	// Only a row whose event is still stored: retention deletes a long
	// session's events in batches and commits each, so a rebuild between
	// two batches reads a partial session, and the 2c rows of the events
	// already gone are the sweep's last batch's to anonymise for the
	// 90-day count, not this rebuild's to remove (ADV-LS1 F6).
	if keep == nil {
		keep = []string{}
	}
	if _, err := q.Exec(ctx, `
		DELETE FROM skill_invocations si
		WHERE si.session_ref = $1 AND si.origin = 'derived' AND si.dedupe_key LIKE 'claude_code:' || $1 || ':e:%' AND si.dedupe_key <> ALL($2::text[])
		  AND EXISTS (SELECT 1 FROM events e WHERE e.session_id = $1 AND e.id = si.event_id)`,
		sessionID, keep); err != nil {
		if isLockWait(err) {
			return 0, fmt.Errorf("%w: %w", errSkillLockWait, err)
		}
		return 0, fmt.Errorf("store: prune orphan skill rows of %s: %w", sessionID, err)
	}
	if _, err := q.Exec(ctx, `
		UPDATE skill_invocations si SET session_type = s.session_type
		FROM sessions s WHERE s.session_id = $1 AND si.session_ref = $1 AND si.origin = 'derived' AND si.session_type = ''`,
		sessionID); err != nil {
		return 0, fmt.Errorf("store: copy session type onto skill rows of %s: %w", sessionID, err)
	}
	return st.RowsUser + st.RowsAgent + st.OutcomeUpdates, nil
}

// skillRederiveParkAt is how many failed attempts leave a queue row where
// it is: the drain skips it, the "derive session failed" line names it,
// and a later enqueue unparks it.
const skillRederiveParkAt = 5

// DrainSkillRederive rebuilds up to limit queued sessions, oldest first,
// each in its own transaction under the per-session lock, deleting the
// queue row with the rebuild's commit; a failure counts an attempt and
// keeps the error, a lock wait counts nothing, and the fifth failure logs
// the session as parked. It reports the sessions done and the sessions
// that parked in this drain. Called from the dirty tick.
func (s *Store) DrainSkillRederive(ctx context.Context, limit int) (done, parked int, err error) {
	rows, err := s.db.Query(ctx, `
		SELECT session_id FROM skill_rederive_queue WHERE attempts < $2 ORDER BY queued_at LIMIT $1`, limit, skillRederiveParkAt)
	if err != nil {
		return 0, 0, fmt.Errorf("store: read skill rederive queue: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, 0, fmt.Errorf("store: scan skill rederive queue: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("store: read skill rederive queue: %w", err)
	}
	for _, sid := range ids {
		if ctx.Err() != nil {
			return done, parked, ctx.Err()
		}
		ok, err := s.rederiveOne(ctx, sid)
		if err == nil {
			if ok {
				done++
			}
			continue
		}
		if ctx.Err() != nil {
			return done, parked, ctx.Err()
		}
		if isLockWait(err) || errors.Is(err, errSkillLockWait) {
			// A row held by ingest or a derive batch is the next tick's
			// business, as the step treats it (the same batch again,
			// never a skip): a wait is not an attempt, or five busy ticks
			// would park a live session until something re-queued it
			// (ADV-LS1 F5).
			continue
		}
		var attempts int
		if qerr := s.db.QueryRow(ctx, `
			UPDATE skill_rederive_queue SET attempts = attempts + 1, last_error = $2
			WHERE session_id = $1 RETURNING attempts`, sid, clipError(err.Error())).Scan(&attempts); qerr != nil {
			return done, parked, fmt.Errorf("store: count a skill rederive failure: %w", qerr)
		}
		if attempts >= skillRederiveParkAt {
			parked++
			s.logSessionFailed(ctx, "dirty", "skill_invocations", sid, attempts, err)
		}
	}
	return done, parked, nil
}

// rederiveOne rebuilds one queued session and deletes its queue row in
// the same transaction. It reports false, nil when the session is held by
// another fold or its row is already gone, which is the next tick's
// business rather than a failure.
func (s *Store) rederiveOne(ctx context.Context, sessionID string) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("store: begin skill rederive of %s: %w", sessionID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := WithStatementTimeout(ctx, tx, deriveStatementTimeout); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '5s'"); err != nil {
		return false, fmt.Errorf("store: bound the skill rederive lock wait: %w", err)
	}
	var queued bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM skill_rederive_queue WHERE session_id = $1 FOR UPDATE SKIP LOCKED)`, sessionID).Scan(&queued); err != nil {
		return false, fmt.Errorf("store: lock skill rederive row of %s: %w", sessionID, err)
	}
	if !queued {
		return false, nil
	}
	locked, err := sessionLocked(ctx, tx, sessionID)
	if err != nil {
		return false, err
	}
	if !locked {
		return false, nil
	}
	if _, err := s.rebuildSessionSkillInvocations(ctx, tx, sessionID); err != nil && !errors.Is(err, ErrNotFound) {
		return false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM skill_rederive_queue WHERE session_id = $1`, sessionID); err != nil {
		return false, fmt.Errorf("store: dequeue %s: %w", sessionID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("store: commit skill rederive of %s: %w", sessionID, err)
	}
	return true, nil
}

// ---------------------------------------------------------------- platform summary

// skillSilenceSentinel is what a series with no row ever reports, so a
// channel that never worked alerts rather than reading as fresh.
const skillSilenceSentinel = 100000

// skillSummaryGrace is how long after 0022 landed the derived series
// withhold the sentinel: a deploy outside US hours would otherwise log it
// on both until the first Skill batch and fire the silence alert.
const skillSummaryGrace = 24 * time.Hour

// skillMaskZone is the zone the weekend mask is taken in. Saturday 00:00
// to Monday 00:00 there is not counted as silence. It is a fixed business
// zone rather than the server's TZ so the mask does not move with the
// container; a deployment whose people work elsewhere changes it here. The
// fallback is for a container with no zone database; the distroless image
// carries one.
var skillMaskZone = func() *time.Location {
	if loc, err := time.LoadLocation("America/New_York"); err == nil {
		return loc
	}
	return time.FixedZone("EST", -5*3600)
}()

// skillSeries is one (platform, environment, origin, trigger) line of the
// "skill platform summary".
type skillSeries struct {
	Platform, Environment, Origin, Trigger string
	Rows24h                                int64
	LastRow                                *time.Time
}

// skillSilenceMinutes is the silence a series reports: the minutes since
// its last row with the weekend mask applied, the sentinel when it has no
// row, or zero for a derived series inside the post-migration grace.
func skillSilenceMinutes(last *time.Time, now, migratedAt time.Time, derived bool) float64 {
	if last == nil {
		if derived && !migratedAt.IsZero() && now.Sub(migratedAt) < skillSummaryGrace {
			return 0
		}
		return skillSilenceSentinel
	}
	return countedMinutes(*last, now)
}

// countedMinutes is now minus last, less every weekend in between.
func countedMinutes(last, now time.Time) float64 {
	if !now.After(last) {
		return 0
	}
	total := now.Sub(last)
	// Walk the Saturdays from the week of last to the week of now.
	l := last.In(skillMaskZone)
	sat := time.Date(l.Year(), l.Month(), l.Day(), 0, 0, 0, 0, skillMaskZone)
	for sat.Weekday() != time.Saturday {
		sat = sat.AddDate(0, 0, -1)
	}
	for sat.Before(now) {
		mon := sat.AddDate(0, 0, 2)
		start, end := sat, mon
		if start.Before(last) {
			start = last
		}
		if end.After(now) {
			end = now
		}
		if end.After(start) {
			total -= end.Sub(start)
		}
		sat = sat.AddDate(0, 0, 7)
	}
	return total.Minutes()
}

// SkillPlatformSummary reads the derived series and the queue for the
// summary line: one row per (platform, origin, trigger) among the derived
// rows, the two expected claude_code series present whether or not they
// have rows, and the unparked queue depth and age.
func (s *Store) SkillPlatformSummary(ctx context.Context, now time.Time) ([]skillSeries, int64, float64, time.Time, error) {
	rows, err := s.db.Query(ctx, `
		SELECT agent_platform, origin, trigger, max(occurred_at),
		       count(*) FILTER (WHERE occurred_at >= $1)
		FROM skill_invocations WHERE origin = 'derived'
		GROUP BY agent_platform, origin, trigger`, now.Add(-24*time.Hour))
	if err != nil {
		return nil, 0, 0, time.Time{}, fmt.Errorf("store: read skill platform summary: %w", err)
	}
	byKey := map[string]*skillSeries{}
	for rows.Next() {
		var sr skillSeries
		var last time.Time
		if err := rows.Scan(&sr.Platform, &sr.Origin, &sr.Trigger, &last, &sr.Rows24h); err != nil {
			rows.Close()
			return nil, 0, 0, time.Time{}, fmt.Errorf("store: scan skill platform summary: %w", err)
		}
		sr.LastRow = &last
		byKey[sr.Platform+"/"+sr.Origin+"/"+sr.Trigger] = &sr
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, 0, time.Time{}, fmt.Errorf("store: read skill platform summary: %w", err)
	}
	for _, trig := range []string{TriggerAgent, TriggerUser} {
		k := PlatformClaudeCode + "/" + OriginDerived + "/" + trig
		if _, ok := byKey[k]; !ok {
			byKey[k] = &skillSeries{Platform: PlatformClaudeCode, Origin: OriginDerived, Trigger: trig}
		}
	}
	var series []skillSeries
	for _, k := range sortedKeys(byKey) {
		series = append(series, *byKey[k])
	}
	var depth int64
	var oldest *time.Time
	if err := s.db.QueryRow(ctx, `
		SELECT count(*), min(queued_at) FROM skill_rederive_queue WHERE attempts < $1`, skillRederiveParkAt).Scan(&depth, &oldest); err != nil {
		return nil, 0, 0, time.Time{}, fmt.Errorf("store: read skill rederive depth: %w", err)
	}
	oldestMinutes := 0.0
	if oldest != nil && now.After(*oldest) {
		oldestMinutes = now.Sub(*oldest).Minutes()
	}
	var migratedAt *time.Time
	if err := s.db.QueryRow(ctx, `SELECT applied_at FROM schema_migrations WHERE name = '0022_skill_invocations.sql'`).Scan(&migratedAt); err != nil && !noRows(err) {
		return nil, 0, 0, time.Time{}, fmt.Errorf("store: read the 0022 apply time: %w", err)
	}
	var migrated time.Time
	if migratedAt != nil {
		migrated = *migratedAt
	}
	return series, depth, oldestMinutes, migrated, nil
}

// LogSkillPlatformSummary writes the "skill platform summary" lines for
// the derived series, one per five-minute fleet tick from the instance
// holding the fleet lock. The queue figures ride the derived series.
func (s *Store) LogSkillPlatformSummary(ctx context.Context, now time.Time) {
	series, depth, oldest, migrated, err := s.SkillPlatformSummary(ctx, now)
	if err != nil {
		s.logger().ErrorContext(ctx, "skill platform summary failed", slog.String("error", err.Error()))
		return
	}
	for _, sr := range series {
		s.logger().InfoContext(ctx, "skill platform summary",
			slog.String("platform", sr.Platform),
			slog.String("environment", sr.Environment),
			slog.String("origin", sr.Origin),
			slog.String("trigger", sr.Trigger),
			slog.Int64("rows_24h", sr.Rows24h),
			slog.Float64("minutes_since_last_row", skillSilenceMinutes(sr.LastRow, now, migrated, sr.Origin == OriginDerived)),
			slog.Int64("rederive_queue", depth),
			slog.Float64("rederive_oldest_minutes", oldest))
	}
}

// ---------------------------------------------------------------- admin transaction

// The rerun route's statements bound to the admin transaction (AdminTx),
// so the audit row and the change land together or not at all (design
// 9.5, DEV-i). None of them reads the store; the transaction is the only
// state, which is why the statements above are package functions.

func (t adminTx) RecordAdminAction(ctx context.Context, actor, action, target string, detail any) error {
	return recordAdminAction(ctx, t.tx, actor, action, target, detail)
}

func (t adminTx) EnqueueSkillRederiveSince(ctx context.Context, since time.Time) (int64, error) {
	return enqueueSkillRederiveSince(ctx, t.tx, since)
}

func (t adminTx) ResetSkillDeriveStep(ctx context.Context) error {
	return resetSkillDeriveStep(ctx, t.tx)
}
