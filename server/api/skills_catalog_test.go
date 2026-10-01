package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/loopai-hq/loop-sessions/server/auth"
)

// The catalog route's properties, observed without a database: which
// credentials reach the store (a live skill-catalog token whose
// environment names the path's repository, nothing else), which body rule
// a request fails and the field it names, what the store is handed, which
// store answer becomes which status, and what the line carries.

const (
	catalogToken   = "lss_catalog-token-for-tests"
	catalogTokenID = "44444444-4444-4444-8444-444444444444"
)

// fakeSources is the token verifier, keyed by the hex of the hash as the
// route presents it.
type fakeSources struct {
	tokens map[string]SourceIdentity
	err    error
	calls  int
}

func (f *fakeSources) AuthenticateSource(_ context.Context, hash []byte, scope string) (SourceIdentity, error) {
	f.calls++
	if f.err != nil {
		return SourceIdentity{}, f.err
	}
	tok, ok := f.tokens[hex.EncodeToString(hash)]
	if !ok {
		return SourceIdentity{State: SourceStateNone}, nil
	}
	if tok.State == "" {
		tok.State = SourceStateLive
	}
	if tok.State == SourceStateLive && tok.Scope != scope {
		tok.State = "wrong_scope"
	}
	return tok, nil
}

func newSources(plaintext string, tok SourceIdentity) *fakeSources {
	return &fakeSources{tokens: map[string]SourceIdentity{hex.EncodeToString(auth.HashToken(plaintext)): tok}}
}

func catalogTokenFor(repo string) SourceIdentity {
	return SourceIdentity{ID: catalogTokenID, Platform: "claude_code", Environment: "catalog-" + repo, Scope: CatalogScope}
}

func catalogHandler(t *testing.T, f *fakeStore, src SourceAuthenticator, log *slog.Logger) *Handler {
	t.Helper()
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h, err := New(Options{Store: f, Auth: &fakeAuth{email: "nobody@example.com"}, Sources: src, Now: func() time.Time { return testNow }, Logger: log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return h
}

func catalogBody() map[string]any {
	return map[string]any{
		"schema": 1, "generated_at": "2026-09-21T06:00:00Z", "source_repo": "example-skills", "commit": "abc1234",
		"skills": []map[string]any{catalogEntry("engg", "git", "engg:git", "git")},
	}
}

func catalogEntry(plugin, dir string, aliases ...string) map[string]any {
	if aliases == nil {
		aliases = []string{}
	}
	return map[string]any{
		"slug": dir, "dir": dir, "name": dir, "plugin": plugin, "path": "plugins/" + plugin + "/skills/" + dir + "/SKILL.md",
		"sha256_tree": "h1", "installable": true, "mirrored": true, "skip_reason": nil, "authored_by": "unknown", "author_evidence": "",
		"lineage_of": nil, "aliases": aliases,
	}
}

func putCatalog(t *testing.T, h *Handler, bearer, repo string, b []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/v1/skill-catalog/"+repo, bytes.NewReader(b))
	r.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		r.Header.Set("Authorization", "Bearer "+bearer)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type catalogErr struct {
	Error   string `json:"error"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

func wantCatalogError(t *testing.T, w *httptest.ResponseRecorder, status int, code, field string) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status = %d, want %d: %s", w.Code, status, w.Body.String())
	}
	var e catalogErr
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	if e.Error != code || e.Field != field {
		t.Fatalf("body = %+v, want %s on %q", e, code, field)
	}
}

// The route is mounted with a verifier and not without one; the store's
// own verifier is picked up when the option is left out.
func TestCatalogRouteMountsWithAVerifier(t *testing.T) {
	f := skillSeeded()
	h := newHandler(t, f, &fakeAuth{email: "admin@example.com"})
	w := putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, catalogBody()))
	if w.Code != http.StatusNotFound {
		t.Errorf("without a verifier the route answers %d, want the package 404", w.Code)
	}
	h = catalogHandler(t, f, newSources(catalogToken, catalogTokenFor("example-skills")), nil)
	w = putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, catalogBody()))
	if w.Code != http.StatusOK {
		t.Errorf("with a verifier the route answers %d: %s", w.Code, w.Body.String())
	}
}

// T10 and the environment rule: no bearer, a device or junk bearer, an
// unknown, dead or wrong-scope token are 401; a live token of another
// repository is 403; and the verify is never reached by a bearer that is
// not a source token.
func TestCatalogRouteRefusesEveryCredentialButTheRepositorysToken(t *testing.T) {
	f := skillSeeded()
	src := newSources(catalogToken, catalogTokenFor("example-skills"))
	revoked := catalogTokenFor("example-skills")
	revoked.State = "revoked"
	src.tokens[hex.EncodeToString(auth.HashToken("lss_revoked-token-for-tests"))] = revoked
	emitter := SourceIdentity{ID: "55555555-5555-4555-8555-555555555555", Platform: "devin", Environment: "default", Scope: "skill-invocations"}
	src.tokens[hex.EncodeToString(auth.HashToken("lss_emitter-token-for-tests"))] = emitter
	var buf bytes.Buffer
	h := catalogHandler(t, f, src, slog.New(slog.NewJSONHandler(&buf, nil)))
	body := mustJSON(t, catalogBody())

	for _, bearer := range []string{"", "lsd_device-token-for-tests", "abc_" + strings.Repeat("x", 40)} {
		wantCatalogError(t, putCatalog(t, h, bearer, "example-skills", body), http.StatusUnauthorized, "unauthenticated", "")
	}
	if src.calls != 0 {
		t.Errorf("a bearer that is not a source token reached the verify %d times", src.calls)
	}
	for _, bearer := range []string{"lss_" + strings.Repeat("y", 40), "lss_revoked-token-for-tests", "lss_emitter-token-for-tests"} {
		wantCatalogError(t, putCatalog(t, h, bearer, "example-skills", body), http.StatusUnauthorized, "unauthenticated", "")
	}
	// The marketplace's token on the backend path: 403, and the store is
	// not called.
	other := catalogBody()
	other["source_repo"] = "backend"
	wantCatalogError(t, putCatalog(t, h, catalogToken, "backend", mustJSON(t, other)), http.StatusForbidden, "forbidden", "")
	if len(f.skills().published) != 0 {
		t.Error("a refused publish reached the store")
	}
	// The refusals ride 7.1's rejection line, not a third LS-3 line, with
	// that line's field set: the state, id and platform of the token row
	// the verify read, so skill_rejected and policy 17 see a catalog
	// credential used after its revoke (review-1 finding 4).
	if line := lastLine(t, &buf, lineInvocationRejected); line["level"] != "WARN" || line["reason"] != "forbidden" ||
		line["token_shape"] != "source" || line["token_state"] != "live" || line["source_token_id"] != catalogTokenID ||
		line["platform"] != "claude_code" || line["status"] != float64(403) || line["field"] != "" {
		t.Errorf("the 403 line = %v", line)
	}
	if strings.Contains(buf.String(), "skill catalog rejected") {
		t.Error("the route opened a third LS-3 log line")
	}
	// The wrong-scope 401 of the loop above: an emitter
	// token in the publisher's secret is the runbook's first case.
	lines := allLines(t, &buf, lineInvocationRejected)
	if n := len(lines); n != 4 {
		t.Fatalf("%d rejection lines for the three 401s that reached the verify and the 403", n)
	}
	if line := lines[2]; line["reason"] != "unauthenticated" || line["token_state"] != "wrong_scope" ||
		line["source_token_id"] != emitter.ID || line["platform"] != "devin" || line["status"] != float64(401) {
		t.Errorf("the wrong-scope line = %v", line)
	}
}

// The path and the body: a path off the slug shape is 400, and so is a
// body whose source_repo differs, whose commit or generated_at is
// missing, or that carries a key the contract does not name.
func TestCatalogRouteBodyRulesNameTheirField(t *testing.T) {
	f := skillSeeded()
	h := catalogHandler(t, f, newSources(catalogToken, catalogTokenFor("example-skills")), nil)
	wantCatalogError(t, putCatalog(t, h, catalogToken, "Example-Skills", mustJSON(t, catalogBody())), http.StatusBadRequest, "invalid_payload", "source_repo")

	long := strings.Repeat("x", 201)
	cases := []struct {
		name, field string
		mutate      func(map[string]any)
	}{
		{"source_repo differs", "source_repo", func(b map[string]any) { b["source_repo"] = "backend" }},
		{"no commit", "commit", func(b map[string]any) { delete(b, "commit") }},
		{"null commit", "commit", func(b map[string]any) { b["commit"] = nil }},
		{"no generated_at", "generated_at", func(b map[string]any) { delete(b, "generated_at") }},
		{"null generated_at", "generated_at", func(b map[string]any) { b["generated_at"] = nil }},
		{"generated_at not a time", "generated_at", func(b map[string]any) { b["generated_at"] = "Monday" }},
		{"no schema", "schema", func(b map[string]any) { delete(b, "schema") }},
		{"no skills", "skills", func(b map[string]any) { delete(b, "skills") }},
		{"unknown top key", "mirror_source", func(b map[string]any) { b["mirror_source"] = "abc" }},
		{"unknown skill key", "description", func(b map[string]any) { b["skills"].([]map[string]any)[0]["description"] = "x" }},
		{"missing skill key", "skills[0].sha256_tree", func(b map[string]any) { delete(b["skills"].([]map[string]any)[0], "sha256_tree") }},
		{"missing aliases", "skills[0].aliases", func(b map[string]any) { delete(b["skills"].([]map[string]any)[0], "aliases") }},
		{"dir off shape", "skills[0].dir", func(b map[string]any) { b["skills"].([]map[string]any)[0]["dir"] = "Git" }},
		{"plugin off shape", "skills[0].plugin", func(b map[string]any) { b["skills"].([]map[string]any)[0]["plugin"] = "en gg" }},
		{"long path", "skills[0].path", func(b map[string]any) { b["skills"].([]map[string]any)[0]["path"] = long }},
		{"long skip_reason", "skills[0].skip_reason", func(b map[string]any) { b["skills"].([]map[string]any)[0]["skip_reason"] = long }},
		{"authored_by off enum", "skills[0].authored_by", func(b map[string]any) { b["skills"].([]map[string]any)[0]["authored_by"] = "robot" }},
		{"author_evidence off enum", "skills[0].author_evidence", func(b map[string]any) { b["skills"].([]map[string]any)[0]["author_evidence"] = "guess" }},
		{"lineage off shape", "skills[0].lineage_of", func(b map[string]any) { b["skills"].([]map[string]any)[0]["lineage_of"] = "Bad Name" }},
		{"alias off shape", "skills[0].aliases", func(b map[string]any) { b["skills"].([]map[string]any)[0]["aliases"] = []string{"engg:Git"} }},
		{"installable not a bool", "skills.installable", func(b map[string]any) { b["skills"].([]map[string]any)[0]["installable"] = "yes" }},
		{"repeated entry", "skills[1].dir", func(b map[string]any) {
			b["skills"] = append(b["skills"].([]map[string]any), catalogEntry("engg", "git"))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := catalogBody()
			c.mutate(b)
			wantCatalogError(t, putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, b)), http.StatusBadRequest, "invalid_payload", c.field)
		})
	}
	wantCatalogError(t, putCatalog(t, h, catalogToken, "example-skills", []byte(`{} {}`)), http.StatusBadRequest, "invalid_payload", "body")
	wantCatalogError(t, putCatalog(t, h, catalogToken, "example-skills", []byte(`not json`)), http.StatusBadRequest, "invalid_payload", "body")
	if len(f.skills().published) != 0 {
		t.Error("a refused body reached the store")
	}
	// Over the 4 MiB cap: 413.
	huge := catalogBody()
	huge["commit"] = strings.Repeat("a", 5<<20)
	if w := putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, huge)); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("a 5 MiB body = %d", w.Code)
	}
}

// A well-formed body reaches the store as the thirteen keys under the
// token's id, the answer carries the counts, and the line carries ids and
// counts only; a stale count makes the line a WARNING.
func TestCatalogRoutePublishesAndLogs(t *testing.T) {
	f := skillSeeded()
	f.skills().publish = PublishResult{Skills: 2, Aliases: 5, StaleEntries: 0}
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	h := catalogHandler(t, f, newSources(catalogToken, catalogTokenFor("example-skills")), log)
	skip := "skip_skills"
	b := catalogBody()
	second := catalogEntry("gtm", "customer-call-analyzer", "gtm:customer-call-analyzer", "customer-call-analyzer", "gtm:call-analyzer", "call-analyzer")
	second["name"] = "call-analyzer"
	second["skip_reason"] = skip
	second["lineage_of"] = "example-skills/engg:git"
	second["authored_by"] = "human"
	second["author_evidence"] = "git_first_commit"
	b["skills"] = append(b["skills"].([]map[string]any), second)
	w := putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, b))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var resp catalogResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Duplicate || resp.Skills != 2 || resp.Aliases != 5 {
		t.Errorf("response = %+v, %v", resp, err)
	}
	if f.skills().publishTok != catalogTokenID || len(f.skills().published) != 1 {
		t.Fatalf("published under %q, %d bodies", f.skills().publishTok, len(f.skills().published))
	}
	got := f.skills().published[0]
	if got.SourceRepo != "example-skills" || got.Commit != "abc1234" || got.Schema != 1 || !got.GeneratedAt.Equal(time.Date(2026, 9, 21, 6, 0, 0, 0, time.UTC)) || len(got.Skills) != 2 {
		t.Errorf("body = %+v", got)
	}
	sk := got.Skills[1]
	if sk.Dir != "customer-call-analyzer" || sk.Name != "call-analyzer" || sk.Plugin != "gtm" || sk.SkipReason == nil || *sk.SkipReason != skip || sk.LineageOf == nil || *sk.LineageOf != "example-skills/engg:git" || sk.AuthoredBy != "human" || sk.AuthorEvidence != "git_first_commit" || len(sk.Aliases) != 4 {
		t.Errorf("second entry = %+v", sk)
	}
	if got.Skills[0].SkipReason != nil || got.Skills[0].LineageOf != nil || !got.Skills[0].Installable {
		t.Errorf("first entry = %+v", got.Skills[0])
	}
	line := lastLine(t, &buf, lineCatalogPublished)
	if line["level"] != "INFO" || line["source_repo"] != "example-skills" || line["source_token_id"] != catalogTokenID || line["commit"] != "abc1234" || line["skills"] != float64(2) || line["aliases"] != float64(5) || line["stale_entries"] != float64(0) || line["duplicate"] != false {
		t.Errorf("line = %v", line)
	}
	if strings.Contains(buf.String(), catalogToken) || strings.Contains(buf.String(), "SKILL.md") {
		t.Error("a line carries the bearer or a path")
	}

	buf.Reset()
	f.skills().publish = PublishResult{Skills: 2, Aliases: 5, StaleEntries: 1}
	putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, b))
	if line := lastLine(t, &buf, lineCatalogPublished); line["level"] != "WARN" || line["stale_entries"] != float64(1) {
		t.Errorf("a stale publish's line = %v", line)
	}
	// A commit that is not a sha logs as other.
	buf.Reset()
	b["commit"] = "hand-1"
	putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, b))
	if line := lastLine(t, &buf, lineCatalogPublished); line["commit"] != "other" {
		t.Errorf("a non-sha commit logged as %v", line["commit"])
	}
	// The lead's devin-builtin body: plugin '', vendor, one bare alias.
	builtin := catalogBody()
	builtin["source_repo"] = "devin-builtin"
	entry := catalogEntry("", "ask-devin", "ask-devin")
	entry["authored_by"] = "vendor"
	entry["mirrored"], entry["installable"] = false, false
	builtin["skills"] = []map[string]any{entry}
	h = catalogHandler(t, f, newSources(catalogToken, catalogTokenFor("devin-builtin")), nil)
	if w := putCatalog(t, h, catalogToken, "devin-builtin", mustJSON(t, builtin)); w.Code != http.StatusOK {
		t.Errorf("the devin-builtin body = %d: %s", w.Code, w.Body.String())
	}
}

// The store's answers: a duplicate is 200 with duplicate true, a field
// error is 400 on its field, an unknown lineage is 400 on lineage_of, an
// alias another repository holds is 409, a failure is 503 with nothing of
// it on the wire; the soft-revoked token is told duplicate without a
// store call.
func TestCatalogRouteTranslatesStoreAnswers(t *testing.T) {
	f := skillSeeded()
	h := catalogHandler(t, f, newSources(catalogToken, catalogTokenFor("example-skills")), nil)
	body := mustJSON(t, catalogBody())

	f.skills().publish = PublishResult{Duplicate: true}
	w := putCatalog(t, h, catalogToken, "example-skills", body)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"duplicate":true`) {
		t.Errorf("a duplicate = %d %s", w.Code, w.Body.String())
	}
	f.skills().publishErr = &CatalogFieldError{Field: "skills[0].dir", Reason: "must be a skill slug"}
	wantCatalogError(t, putCatalog(t, h, catalogToken, "example-skills", body), http.StatusBadRequest, "invalid_payload", "skills[0].dir")
	f.skills().publishErr = &LineageError{Lineage: "example-skills/engg:nowhere"}
	wantCatalogError(t, putCatalog(t, h, catalogToken, "example-skills", body), http.StatusBadRequest, "invalid_payload", "lineage_of")
	f.skills().publishErr = &AliasConflictError{Alias: "git", HeldBy: "example-skills"}
	w = putCatalog(t, h, catalogToken, "example-skills", body)
	wantCatalogError(t, w, http.StatusConflict, "alias_conflict", "aliases")
	if !strings.Contains(w.Body.String(), "held by example-skills") {
		t.Errorf("the conflict does not name the holder: %s", w.Body.String())
	}
	f.skills().publishErr = errors.New("connection refused")
	w = putCatalog(t, h, catalogToken, "example-skills", body)
	wantCatalogError(t, w, http.StatusServiceUnavailable, "unavailable", "")
	if strings.Contains(w.Body.String(), "refused") {
		t.Error("the store's error text reached the wire")
	}
	// A verify failure is 503 too, never 401.
	src := newSources(catalogToken, catalogTokenFor("example-skills"))
	src.err = errors.New("pool exhausted")
	h = catalogHandler(t, f, src, nil)
	wantCatalogError(t, putCatalog(t, h, catalogToken, "example-skills", body), http.StatusServiceUnavailable, "unavailable", "")

	// Soft revoked: duplicate, no store call; over the cap: 429.
	f.skills().publishErr = nil
	before := len(f.skills().published)
	soft := catalogTokenFor("example-skills")
	soft.SoftRevoked = true
	h = catalogHandler(t, f, newSources(catalogToken, soft), nil)
	w = putCatalog(t, h, catalogToken, "example-skills", body)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"duplicate":true`) || len(f.skills().published) != before {
		t.Errorf("a soft-revoked token = %d %s, %d publishes", w.Code, w.Body.String(), len(f.skills().published)-before)
	}
	capped := catalogTokenFor("example-skills")
	capped.OverCap = true
	h = catalogHandler(t, f, newSources(catalogToken, capped), nil)
	w = putCatalog(t, h, catalogToken, "example-skills", body)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "30" {
		t.Errorf("over the cap = %d %q", w.Code, w.Header().Get("Retry-After"))
	}
}

// The shrink refusal (design 4d as amended 2026-09-22): the store's
// CatalogShrinkError becomes 409 catalog_shrink carrying both counts, the
// line is a WARNING naming the refusal, and nothing of the body reaches
// the wire. An accepted publish whose count merely falls is a WARNING too,
// not an INFO, and allow_shrink travels to the store as the publisher set
// it (adversarial finding 1).
func TestCatalogRouteRefusesAShrinkAndWarnsOnAFall(t *testing.T) {
	f := skillSeeded()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	h := catalogHandler(t, f, newSources(catalogToken, catalogTokenFor("example-skills")), log)
	body := mustJSON(t, catalogBody())

	f.skills().publishErr = &CatalogShrinkError{Present: 41, Incoming: 0}
	w := putCatalog(t, h, catalogToken, "example-skills", body)
	if w.Code != http.StatusConflict {
		t.Fatalf("a refused shrink = %d: %s", w.Code, w.Body.String())
	}
	var shrink struct {
		Error    string `json:"error"`
		Present  int    `json:"present"`
		Incoming int    `json:"incoming"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &shrink); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	if shrink.Error != "catalog_shrink" || shrink.Present != 41 || shrink.Incoming != 0 {
		t.Errorf("the 409 body = %+v, want catalog_shrink with 41 present and 0 incoming", shrink)
	}
	line := lastLine(t, &buf, lineCatalogPublished)
	if line["level"] != "WARN" || line["refused"] != "catalog_shrink" || line["present_before"] != float64(41) || line["skills"] != float64(0) {
		t.Errorf("the refusal line = %v", line)
	}

	// An accepted publish that shrinks under the gate's half: 200, and a
	// WARNING carrying both counts, since that is the shape a wipe takes on
	// its way to being one.
	buf.Reset()
	f.skills().publishErr = nil
	f.skills().publish = PublishResult{Skills: 30, Aliases: 60, PresentBefore: 41}
	if w := putCatalog(t, h, catalogToken, "example-skills", body); w.Code != http.StatusOK {
		t.Fatalf("a falling publish = %d: %s", w.Code, w.Body.String())
	}
	if line := lastLine(t, &buf, lineCatalogPublished); line["level"] != "WARN" || line["skills"] != float64(30) || line["present_before"] != float64(41) || line["refused"] != "" {
		t.Errorf("a falling publish's line = %v", line)
	}
	// The same counts the other way round are an ordinary INFO.
	buf.Reset()
	f.skills().publish = PublishResult{Skills: 41, Aliases: 60, PresentBefore: 30}
	putCatalog(t, h, catalogToken, "example-skills", body)
	if line := lastLine(t, &buf, lineCatalogPublished); line["level"] != "INFO" {
		t.Errorf("a growing publish's line = %v", line)
	}

	// allow_shrink is the publisher's, absent by default and never inferred.
	if got := f.skills().published[len(f.skills().published)-1]; got.AllowShrink {
		t.Error("a body without allow_shrink reached the store with the override set")
	}
	b := catalogBody()
	b["allow_shrink"] = true
	if w := putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, b)); w.Code != http.StatusOK {
		t.Fatalf("allow_shrink = %d: %s", w.Code, w.Body.String())
	}
	if got := f.skills().published[len(f.skills().published)-1]; !got.AllowShrink {
		t.Error("allow_shrink did not reach the store")
	}
}

// A publish on a limit-0 token is answered duplicate and writes nothing,
// but it writes its line: a publisher still pushing on a rotated token
// would otherwise read success while the catalog aged, with nothing to
// find it by (adversarial finding 6).
func TestCatalogRouteLogsASoftRevokedPublish(t *testing.T) {
	f := skillSeeded()
	var buf bytes.Buffer
	soft := catalogTokenFor("example-skills")
	soft.SoftRevoked = true
	h := catalogHandler(t, f, newSources(catalogToken, soft), slog.New(slog.NewJSONHandler(&buf, nil)))
	before := len(f.skills().published)
	w := putCatalog(t, h, catalogToken, "example-skills", mustJSON(t, catalogBody()))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"duplicate":true`) || len(f.skills().published) != before {
		t.Fatalf("a soft-revoked publish = %d %s, %d new bodies", w.Code, w.Body.String(), len(f.skills().published)-before)
	}
	line := lastLine(t, &buf, lineCatalogPublished)
	if line["level"] != "WARN" || line["soft_revoked"] != true || line["skills"] != float64(0) || line["duplicate"] != true ||
		line["source_token_id"] != catalogTokenID || line["source_repo"] != "example-skills" {
		t.Errorf("the soft-revoked line = %v", line)
	}
	// Still ids and counts only.
	if strings.Contains(buf.String(), catalogToken) || strings.Contains(buf.String(), "SKILL.md") {
		t.Error("the soft-revoked line carries the bearer or a path")
	}
}

func lastLine(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	lines := allLines(t, buf, msg)
	if len(lines) == 0 {
		t.Fatalf("no %q line in:\n%s", msg, buf.String())
	}
	return lines[len(lines)-1]
}

// allLines is every line of one message in order, which a route that logs
// several refusals in a row needs to read.
func allLines(t *testing.T, buf *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %v (%s)", err, line)
		}
		if entry["msg"] == msg {
			out = append(out, entry)
		}
	}
	return out
}
