//go:build integration

package app

// The repair route end to end over a real Postgres: a device's bearer is
// verified against the store, the list names the answerless hook-only
// session with the transcript path built from its cwd, and a second ask
// inside the hour is refused.
//
//	LOOP_SESSIONS_TEST_DSN=postgres:///loop_sessions_test go test -tags integration ./server/app/...

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/loopai-hq/loop-sessions/internal/event"
	"github.com/loopai-hq/loop-sessions/server/api"
	"github.com/loopai-hq/loop-sessions/server/auth"
	"github.com/loopai-hq/loop-sessions/server/store"
)

func TestIntegrationRepairRouteNamesTheDevicesAnswerlessSessions(t *testing.T) {
	dsn := os.Getenv("LOOP_SESSIONS_TEST_DSN")
	if dsn == "" {
		t.Skip("LOOP_SESSIONS_TEST_DSN is unset")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("loop_sessions_repair_%d", time.Now().UnixNano())
	bootstrap, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	bootstrap.Close()
	t.Cleanup(func() {
		if cleanup, err := pgxpool.New(ctx, dsn); err == nil {
			_, _ = cleanup.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
			cleanup.Close()
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	st := store.New(pool, nil)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	const email = "repair@example.com"
	if _, err := pool.Exec(ctx, `INSERT INTO principals (email, role, added_by) VALUES ($1, 'member', 'test')`, email); err != nil {
		t.Fatal(err)
	}
	token := auth.TokenPrefix + "repairtoken000000000000000000000"
	dev, err := st.EnrollDevice(ctx, store.Device{Email: email, Hostname: "mbp"}, auth.HashToken(token), time.Time{})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}

	// An answerless hook-only session, ended, from a cwd the slug rule can
	// place, under the uuid the harness gives a session (the path is built
	// for a uuid only).
	const sid = "0f6d1b2e-3c4d-4e5f-8a9b-0c1d2e3f4a5b"
	now := time.Now().UTC().Add(-2 * time.Hour)
	mk := func(id string, seq int64, typ event.Type, at time.Time, text string) store.Ingest {
		return store.Ingest{Email: email, DeviceID: dev.ID, Event: event.Event{
			ID: id, SessionID: sid, Seq: seq, Type: typ, Source: event.SourceClaudeCode,
			Origin: event.OriginHook, OccurredAt: at, Text: text, Cwd: "/home/dev/work/api",
		}}
	}
	p := mk("s-rep-p", 2, event.UserPrompt, now, "fix it")
	p.Event.PromptID = "pid-1"
	a := mk("s-rep-a", 3, event.AssistantTurn, now.Add(10*time.Second), "")
	a.Event.PromptID = "pid-1"
	if _, err := st.UpsertEvents(ctx, []store.Ingest{
		mk("s-rep-start", 1, event.SessionStarted, now.Add(-time.Second), "startup"),
		p, a,
		mk("s-rep-end", 4, event.SessionEnded, now.Add(time.Minute), ""),
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := st.DeriveDirty(ctx, store.DeriveConfig{Now: time.Now}); err != nil {
		t.Fatalf("fold: %v", err)
	}

	h, err := api.New(api.Options{
		Store:   NewAPIStore(st),
		Auth:    &noCookie{},
		Devices: NewAPIDevices(st),
		Repair:  NewAPIRepair(st),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	get := func(bearer string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/v1/repair", nil)
		if bearer != "" {
			r.Header.Set("Authorization", "Bearer "+bearer)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	if w := get(auth.TokenPrefix + "wrong"); w.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: %d", w.Code)
	}
	w := get(token)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Sessions []api.RepairEntry `json:"sessions"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Sessions) != 1 {
		t.Fatalf("list = %+v, want the one answerless session", body.Sessions)
	}
	e := body.Sessions[0]
	if e.SessionID != sid || e.Reason != "missing_answers" || e.Source != "claude_code" {
		t.Errorf("entry = %+v", e)
	}
	if e.TranscriptPath != "/home/dev/.claude/projects/-home-dev-work-api/"+sid+".jsonl" {
		t.Errorf("transcript_path = %q", e.TranscriptPath)
	}
	if !strings.Contains(e.Hint, "1 answerless turn") {
		t.Errorf("hint = %q", e.Hint)
	}
	if w := get(token); w.Code != http.StatusTooManyRequests {
		t.Errorf("second ask inside the hour: %d", w.Code)
	}
}

// noCookie is the read API's cookie authenticator for a test that only uses
// the device route.
type noCookie struct{}

func (noCookie) Authenticate(*http.Request) (api.Identity, error) {
	return api.Identity{}, api.ErrNoIdentity
}
