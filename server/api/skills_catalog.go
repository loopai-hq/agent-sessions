package api

// PUT /v1/skill-catalog/{source_repo}: a repository's CI publishes its
// skill catalog (the body is catalog/skills.json, with the short source_repo
// slug).
//
// The credential is a source token of scope skill-catalog whose
// environment is catalog-<source_repo>: one repository's token cannot
// rewrite another's entries, present flags or authorship, so the check is
// on the row the verify returned and never on the body. The body's keys
// are exactly the thirteen per skill, the five at the top and the
// allow_shrink override below; anything else is refused by name, since a generator that grew a key would
// otherwise have it dropped on the floor here and stored by a later edit.
// One transaction in the store: a commit already recorded is a duplicate
// and nothing else moves, unless its stored row recorded no entries, which
// is the repair path out of a wipe.
//
// A publish may not retire most of a catalog: an empty body, or one that
// would flip more than half of the repository's present entries to absent,
// is 409 catalog_shrink unless the body carries allow_shrink, and an
// accepted publish whose count falls is a WARNING rather than an INFO
// (adversarial finding 1, ruled 2026-09-22).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/loopai-hq/agent-sessions/internal/skilllog"
	"github.com/loopai-hq/agent-sessions/server/auth"
)

// CatalogScope is the token scope the route accepts.
const CatalogScope = "skill-catalog"

// catalogBodyMax is the body cap (design 4d): a catalog of three hundred
// skills is under a megabyte.
const catalogBodyMax = 4 << 20

// catalogTextMax is the cap on every string field of the body.
const catalogTextMax = 200

// sourceTokenPrefix is the lss_ shape, gated before any lookup.
const sourceTokenPrefix = "lss_"

// lineCatalogPublished is the 7.1 line; ids and counts only.
const lineCatalogPublished = skilllog.CatalogPublished

// lineInvocationRejected is 7.1's rejection line, which this route shares
// with POST /v1/skill-invocations rather than opening a third LS-3 line:
// the contract section 3 pins LS-3's lines at "skill reconciler run" and
// "skill catalog published", and a refused catalog credential is the same
// class as a refused emitter one, so it belongs under the same
// skill_rejected metric and the same two conditions of policy 17 that see a
// token used after its revoke (review-1 finding 4, confirmed by the ruling
// of 2026-09-22 on review-2).
//
// The string is taken from internal/skilllog rather than written again
// here: the metric's filter reads that one literal, and two declarations
// of it would let a rename in one package leave this route's refusals
// under no metric with nothing failing to say so (review-2 finding 1).
// The leaf package, not server/skillusage, because this package reaches
// the store only through ports.go and importing the emitter for a string
// would pull the store, the derive package and six internal packages
// behind that boundary (review-3 finding 3, ruled 2026-09-22).
const lineInvocationRejected = skilllog.Rejected

var (
	// sourceRepoShape is the entries' source_repo CHECK, applied to the
	// path so the body, the path and the token's environment agree.
	sourceRepoShape = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,31}$`)
	// slugShape is the plugin and skill CHECK.
	slugShape = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}[a-z0-9]$`)
	// lineageShape is '<source_repo>/<plugin>:<skill>', the plugin possibly
	// empty.
	lineageShape = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,31}/([a-z0-9][a-z0-9_-]{0,62}[a-z0-9])?:[a-z0-9][a-z0-9_-]{0,62}[a-z0-9]$`)
)

// The closed sets of the entries table, refused here before the store's
// CHECK would.
var (
	authoredBys     = map[string]bool{"human": true, "agent": true, "vendor": true, "unknown": true}
	authorEvidences = map[string]bool{"": true, "frontmatter": true, "git_first_commit": true}
)

// CatalogBody is the body as the port takes it.
type CatalogBody struct {
	Schema      int
	GeneratedAt time.Time
	SourceRepo  string
	Commit      string
	Skills      []CatalogSkill
	// AllowShrink is the explicit override on the shrink refusal below.
	AllowShrink bool
}

// CatalogSkill is one entry: the thirteen per-skill keys.
type CatalogSkill struct {
	Slug, Dir, Name, Plugin, Path, Sha256Tree string
	Installable, Mirrored                     bool
	SkipReason                                *string
	AuthoredBy, AuthorEvidence                string
	LineageOf                                 *string
	Aliases                                   []string
}

// PublishResult is what the store came to.
type PublishResult struct {
	Duplicate       bool
	Skills, Aliases int
	StaleEntries    int
	PresentBefore   int
}

// CatalogFieldError names a field the store refused: 400 on that field.
type CatalogFieldError struct{ Field, Reason string }

func (e *CatalogFieldError) Error() string { return "api: skill catalog: " + e.Field + " " + e.Reason }

// LineageError is a lineage_of that names no entry: 400 on lineage_of.
type LineageError struct{ Lineage string }

func (e *LineageError) Error() string {
	return "api: skill catalog: lineage_of names no entry: " + e.Lineage
}

// CatalogShrinkError is a body that would retire most of a repository's
// catalog, or an empty one: 409 catalog_shrink, carrying both counts so the
// publisher's log says what it was about to lose.
type CatalogShrinkError struct{ Present, Incoming int }

func (e *CatalogShrinkError) Error() string {
	return "api: skill catalog: a publish may not retire most of a catalog"
}

// AliasConflictError is an alias another repository holds with no lineage
// between the two entries: 409.
type AliasConflictError struct{ Alias, HeldBy string }

func (e *AliasConflictError) Error() string {
	return "api: skill catalog: alias " + e.Alias + " is held by " + e.HeldBy
}

// The wire body: every field a pointer so an absent key is told from a
// zero, and the decoder refusing unknown keys at both levels.
type catalogRequest struct {
	Schema      *int                   `json:"schema"`
	GeneratedAt *string                `json:"generated_at"`
	SourceRepo  *string                `json:"source_repo"`
	Commit      *string                `json:"commit"`
	Skills      *[]catalogSkillRequest `json:"skills"`
	AllowShrink *bool                  `json:"allow_shrink"`
}

type catalogSkillRequest struct {
	Slug           *string   `json:"slug"`
	Dir            *string   `json:"dir"`
	Name           *string   `json:"name"`
	Plugin         *string   `json:"plugin"`
	Path           *string   `json:"path"`
	Sha256Tree     *string   `json:"sha256_tree"`
	Installable    *bool     `json:"installable"`
	Mirrored       *bool     `json:"mirrored"`
	SkipReason     *string   `json:"skip_reason"`
	AuthoredBy     *string   `json:"authored_by"`
	AuthorEvidence *string   `json:"author_evidence"`
	LineageOf      *string   `json:"lineage_of"`
	Aliases        *[]string `json:"aliases"`
}

type catalogResponse struct {
	Duplicate    bool `json:"duplicate"`
	Skills       int  `json:"skills"`
	Aliases      int  `json:"aliases"`
	StaleEntries int  `json:"stale_entries"`
}

// catalogShrinkResponse is the 409 a refused shrink answers. It carries
// numbers rather than a message, so the publish script's log says exactly
// what it was about to lose and the runbook's recovery step can be read off
// it (adversarial finding 1).
type catalogShrinkResponse struct {
	Error    string `json:"error"`
	Present  int    `json:"present"`
	Incoming int    `json:"incoming"`
}

// catalogError is the flat error shape the emitter family reads
// (server/skillusage), not the read API's nested one: the publisher is a
// CI script beside the hook and the beacon.
type catalogError struct {
	Error   string `json:"error"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message,omitempty"`
}

func writeCatalogError(w http.ResponseWriter, status int, code, field, msg string) {
	writeJSON(w, status, catalogError{Error: code, Field: field, Message: msg})
}

// handlePublishSkillCatalog is PUT /v1/skill-catalog/{source_repo}.
func (h *Handler) handlePublishSkillCatalog(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// The prefix gate: anything but a source token is refused before the
	// path is read and before any lookup.
	bearer := auth.BearerToken(r)
	if !strings.HasPrefix(bearer, sourceTokenPrefix) {
		writeCatalogError(w, http.StatusUnauthorized, "unauthenticated", "", "a source token is required")
		return
	}
	sourceRepo := r.PathValue("source_repo")
	if !sourceRepoShape.MatchString(sourceRepo) {
		writeCatalogError(w, http.StatusBadRequest, "invalid_payload", "source_repo", "must match ^[a-z0-9][a-z0-9_.-]{0,31}$")
		return
	}
	tok, err := h.sources.AuthenticateSource(ctx, auth.HashToken(bearer), CatalogScope)
	if err != nil {
		h.fail(r, "verify catalog token", err)
		writeCatalogError(w, http.StatusServiceUnavailable, "unavailable", "", "")
		return
	}
	// Dead, wrong-scope or unknown: one 401, the state on our own log.
	if tok.State != SourceStateLive {
		h.rejectedCatalog(ctx, "unauthenticated", http.StatusUnauthorized, tok)
		writeCatalogError(w, http.StatusUnauthorized, "unauthenticated", "", "")
		return
	}
	// The token names its repository: another repository's token is 403,
	// which is what keeps one publisher off another's entries.
	if tok.Environment != "catalog-"+sourceRepo {
		h.rejectedCatalog(ctx, "forbidden", http.StatusForbidden, tok)
		writeCatalogError(w, http.StatusForbidden, "forbidden", "", "the token is not this repository's")
		return
	}
	if tok.OverCap {
		w.Header().Set("Retry-After", "30")
		writeCatalogError(w, http.StatusTooManyRequests, "rate_limited", "", "")
		return
	}
	body, status, field, msg := decodeCatalog(w, r, sourceRepo)
	if status != 0 {
		writeCatalogError(w, status, catalogCode(status), field, msg)
		return
	}
	if tok.SoftRevoked {
		// Limit 0: verified and counted, nothing written, the publisher
		// told its commit was a duplicate, as every emitter is. The line
		// still goes out, carrying soft_revoked and skills: 0, because a
		// publisher still pushing on a rotated token would otherwise read
		// success while the catalog aged with no line to find it by, which
		// is the slow form of the wipe above (adversarial finding 6; the
		// runs route was given the same treatment for the same reason in
		// review-1 finding 3).
		h.log.WarnContext(ctx, lineCatalogPublished, catalogAttrs(sourceRepo, tok.ID, body.Commit, PublishResult{Duplicate: true}, true, "")...)
		writeJSON(w, http.StatusOK, catalogResponse{Duplicate: true})
		return
	}
	res, err := h.store.PublishSkillCatalog(ctx, tok.ID, sourceRepo, body)
	if err != nil {
		var fe *CatalogFieldError
		var le *LineageError
		var ce *AliasConflictError
		var se *CatalogShrinkError
		switch {
		case errors.As(err, &fe):
			writeCatalogError(w, http.StatusBadRequest, "invalid_payload", fe.Field, fe.Reason)
		case errors.As(err, &le):
			writeCatalogError(w, http.StatusBadRequest, "invalid_payload", "lineage_of", "names no entry: "+le.Lineage)
		case errors.As(err, &se):
			// Nothing was written, so the line reports what the refusal
			// saved rather than what a publish did.
			h.log.WarnContext(ctx, lineCatalogPublished,
				catalogAttrs(sourceRepo, tok.ID, body.Commit, PublishResult{Skills: se.Incoming, PresentBefore: se.Present}, false, "catalog_shrink")...)
			writeJSON(w, http.StatusConflict, catalogShrinkResponse{Error: "catalog_shrink", Present: se.Present, Incoming: se.Incoming})
		case errors.As(err, &ce):
			writeCatalogError(w, http.StatusConflict, "alias_conflict", "aliases", "alias "+ce.Alias+" is held by "+ce.HeldBy)
		default:
			h.fail(r, "publish skill catalog", err)
			writeCatalogError(w, http.StatusServiceUnavailable, "unavailable", "", "")
		}
		return
	}
	// The 7.1 line: ids and counts only. A WARNING when entries read stale
	// against their lineage head, which is the fourteen-day signal the
	// runbook acts on (OPS3-6), and a WARNING when the count FELL even
	// though the publish was accepted: a catalog that loses entries under
	// the shrink gate's half is legitimate often enough not to refuse and
	// suspicious often enough to look at, and it is the shape a wipe takes
	// on its way to being one (adversarial finding 1).
	attrs := catalogAttrs(sourceRepo, tok.ID, body.Commit, res, false, "")
	if res.StaleEntries > 0 || (!res.Duplicate && res.Skills < res.PresentBefore) {
		h.log.WarnContext(ctx, lineCatalogPublished, attrs...)
	} else {
		h.log.InfoContext(ctx, lineCatalogPublished, attrs...)
	}
	writeJSON(w, http.StatusOK, catalogResponse{Duplicate: res.Duplicate, Skills: res.Skills, Aliases: res.Aliases, StaleEntries: res.StaleEntries})
}

// catalogAttrs is the published line's field set, shared by the accepted
// publish, the refused shrink and the soft-revoked post so a filter on any
// one key works on all of them: a line that carried present_before only
// sometimes would make "the catalog shrank" unqueryable.
func catalogAttrs(sourceRepo, tokenID, commit string, res PublishResult, softRevoked bool, refused string) []any {
	return []any{
		slog.String("source_repo", sourceRepo),
		slog.String("source_token_id", tokenID),
		slog.String("commit", commitLabel(commit)),
		slog.Int("skills", res.Skills),
		slog.Int("aliases", res.Aliases),
		slog.Int("stale_entries", res.StaleEntries),
		slog.Int("present_before", res.PresentBefore),
		slog.Bool("duplicate", res.Duplicate),
		slog.Bool("soft_revoked", softRevoked),
		slog.String("refused", refused),
	}
}

// rejectedCatalog writes 7.1's rejection line for a refused PUT. The field
// set is that line's exactly, so one metric reads both routes, and every
// value comes from the token row the verify returned rather than from the
// path or the body: an unauthenticated caller can name neither a platform
// nor a token id, so it cannot grow the metric's label set (SECURITY1-8).
func (h *Handler) rejectedCatalog(ctx context.Context, reason string, status int, tok SourceIdentity) {
	h.log.WarnContext(ctx, lineInvocationRejected,
		slog.String("reason", reason),
		slog.String("field", ""),
		slog.String("token_shape", "source"),
		slog.String("token_state", tok.State),
		slog.String("source_token_id", tok.ID),
		slog.String("platform", tok.Platform),
		slog.Int("status", status))
}

// commitLabel is a commit as a line carries it: a hex sha, or other.
var commitShape = regexp.MustCompile(`^[0-9a-f]{7,64}$`)

func commitLabel(commit string) string {
	if commitShape.MatchString(commit) {
		return commit
	}
	return "other"
}

func catalogCode(status int) string {
	switch status {
	case http.StatusRequestEntityTooLarge:
		return "body_too_large"
	default:
		return "invalid_payload"
	}
}

// decodeCatalog reads the body under the cap, refusing unknown keys at
// every level and a second document, and applies the field rules: the
// path and body agree on source_repo, commit and generated_at are set, and
// every skill carries its keys in their shapes. It reports the status and
// the field of the first refusal, or zero.
func decodeCatalog(w http.ResponseWriter, r *http.Request, sourceRepo string) (CatalogBody, int, string, string) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, catalogBodyMax))
	dec.DisallowUnknownFields()
	var req catalogRequest
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &tooLarge):
			return CatalogBody{}, http.StatusRequestEntityTooLarge, "", ""
		case errors.As(err, &typeErr):
			return CatalogBody{}, http.StatusBadRequest, typeErr.Field, "has the wrong type"
		}
		field := "body"
		if m := unknownKey.FindStringSubmatch(err.Error()); m != nil {
			field = m[1]
		}
		return CatalogBody{}, http.StatusBadRequest, field, "malformed"
	}
	if _, err := dec.Token(); err != io.EOF {
		return CatalogBody{}, http.StatusBadRequest, "body", "one document only"
	}
	if req.Schema == nil || *req.Schema < 1 {
		return CatalogBody{}, http.StatusBadRequest, "schema", "is required"
	}
	if req.SourceRepo == nil || *req.SourceRepo != sourceRepo {
		return CatalogBody{}, http.StatusBadRequest, "source_repo", "must equal the path"
	}
	if req.Commit == nil || *req.Commit == "" || len(*req.Commit) > catalogTextMax {
		return CatalogBody{}, http.StatusBadRequest, "commit", "is required; the publish script stamps GITHUB_SHA"
	}
	if req.GeneratedAt == nil {
		return CatalogBody{}, http.StatusBadRequest, "generated_at", "is required; the publish script stamps the commit date"
	}
	generated, err := time.Parse(time.RFC3339, *req.GeneratedAt)
	if err != nil {
		return CatalogBody{}, http.StatusBadRequest, "generated_at", "must be RFC 3339"
	}
	if req.Skills == nil {
		return CatalogBody{}, http.StatusBadRequest, "skills", "is required"
	}
	body := CatalogBody{Schema: *req.Schema, GeneratedAt: generated.UTC(), SourceRepo: sourceRepo, Commit: *req.Commit,
		AllowShrink: req.AllowShrink != nil && *req.AllowShrink}
	seen := map[string]bool{}
	for i, sk := range *req.Skills {
		at := func(k string) string { return "skills[" + strconv.Itoa(i) + "]." + k }
		for _, f := range []struct {
			k string
			v *string
		}{{"slug", sk.Slug}, {"dir", sk.Dir}, {"name", sk.Name}, {"plugin", sk.Plugin}, {"path", sk.Path}, {"sha256_tree", sk.Sha256Tree}, {"authored_by", sk.AuthoredBy}, {"author_evidence", sk.AuthorEvidence}} {
			if f.v == nil {
				return CatalogBody{}, http.StatusBadRequest, at(f.k), "is required"
			}
			if len(*f.v) > catalogTextMax {
				return CatalogBody{}, http.StatusBadRequest, at(f.k), "is over 200 characters"
			}
		}
		if sk.Installable == nil || sk.Mirrored == nil || sk.Aliases == nil {
			return CatalogBody{}, http.StatusBadRequest, at("aliases"), "installable, mirrored and aliases are required"
		}
		if !slugShape.MatchString(*sk.Dir) {
			return CatalogBody{}, http.StatusBadRequest, at("dir"), "must be a skill slug"
		}
		if *sk.Plugin != "" && !slugShape.MatchString(*sk.Plugin) {
			return CatalogBody{}, http.StatusBadRequest, at("plugin"), "must be a plugin slug or empty"
		}
		if !authoredBys[*sk.AuthoredBy] {
			return CatalogBody{}, http.StatusBadRequest, at("authored_by"), "must be human, agent, vendor or unknown"
		}
		if !authorEvidences[*sk.AuthorEvidence] {
			return CatalogBody{}, http.StatusBadRequest, at("author_evidence"), "must be '', frontmatter or git_first_commit"
		}
		if sk.SkipReason != nil && len(*sk.SkipReason) > catalogTextMax {
			return CatalogBody{}, http.StatusBadRequest, at("skip_reason"), "is over 200 characters"
		}
		if sk.LineageOf != nil && (len(*sk.LineageOf) > catalogTextMax || !lineageShape.MatchString(*sk.LineageOf)) {
			return CatalogBody{}, http.StatusBadRequest, at("lineage_of"), "must be <source_repo>/<plugin>:<skill>"
		}
		for _, a := range *sk.Aliases {
			plugin, skill := "", a
			if j := strings.Index(a, ":"); j >= 0 {
				plugin, skill = a[:j], a[j+1:]
			}
			if len(a) > catalogTextMax || !slugShape.MatchString(skill) || (plugin != "" && !slugShape.MatchString(plugin)) {
				return CatalogBody{}, http.StatusBadRequest, at("aliases"), "must be plugin:skill or skill slugs"
			}
		}
		key := *sk.Plugin + ":" + *sk.Dir
		if seen[key] {
			return CatalogBody{}, http.StatusBadRequest, at("dir"), "repeats an entry"
		}
		seen[key] = true
		body.Skills = append(body.Skills, CatalogSkill{
			Slug: *sk.Slug, Dir: *sk.Dir, Name: *sk.Name, Plugin: *sk.Plugin, Path: *sk.Path, Sha256Tree: *sk.Sha256Tree,
			Installable: *sk.Installable, Mirrored: *sk.Mirrored, SkipReason: sk.SkipReason,
			AuthoredBy: *sk.AuthoredBy, AuthorEvidence: *sk.AuthorEvidence, LineageOf: sk.LineageOf, Aliases: *sk.Aliases,
		})
	}
	return body, 0, "", ""
}

var unknownKey = regexp.MustCompile(`json: unknown field "([^"]*)"`)
