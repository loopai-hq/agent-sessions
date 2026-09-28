package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/loopai-hq/agent-sessions/internal/event"
	"github.com/loopai-hq/agent-sessions/internal/health"
	"github.com/loopai-hq/agent-sessions/internal/spool"
	"github.com/loopai-hq/agent-sessions/server/ingest"
)

// uaStore is the ingest port as the adapter's callers see it, recording the
// one thing this file is about: the build the request context carried when
// the port was called. It stands in for the adapter deliberately, because
// the adapter's own translation of that value into store.Ingest is covered
// next to it (TestAgentVersionRidesTheContext); what is unproven without an
// HTTP request is that anything ever puts the value on the context.
type uaStore struct {
	mu       sync.Mutex
	versions []string
}

func (s *uaStore) UpsertEvents(ctx context.Context, records []ingest.Record) (ingest.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions = append(s.versions, agentVersionOf(ctx))
	var res ingest.Result
	for _, r := range records {
		res.Inserted = append(res.Inserted, r.Event.ID)
	}
	return res, nil
}

func (s *uaStore) PutHealthReport(ctx context.Context, _, _ string, _ health.Report) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions = append(s.versions, agentVersionOf(ctx))
	return nil
}

func (s *uaStore) last(t *testing.T) string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.versions) == 0 {
		t.Fatal("the store was never called")
	}
	return s.versions[len(s.versions)-1]
}

// uaDevices accepts exactly one credential.
type uaDevices struct{}

func (uaDevices) Verify(_ context.Context, presented string) (ingest.Identity, error) {
	if presented != "device-token" {
		return ingest.Identity{}, ingest.ErrUnauthenticated
	}
	return ingest.Identity{Email: ingAlice, DeviceID: ingDevice}, nil
}

// The build that delivered a batch reaches the store from the request's
// User-Agent and from nowhere else. The ingest handler hands its port records
// and a context, so the header has to be read where the routes are mounted;
// this is the proof that the mounting reads it, and that a request from
// anything but the agent delivers no version rather than a wrong one.
func TestRegisterIngestStampsTheDeliveringBuild(t *testing.T) {
	st := &uaStore{}
	h, err := ingest.New(ingest.Options{
		Store:   st,
		Devices: uaDevices{},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	mux := http.NewServeMux()
	RegisterIngest(mux, h)

	ev := event.Event{
		ID:         "e1",
		SessionID:  "sess-1",
		Source:     event.SourceClaudeCode,
		Origin:     event.OriginHook,
		Type:       event.UserPrompt,
		Seq:        1,
		OccurredAt: ingClock,
		Cwd:        "/home/dev/src/loop-sessions",
		Text:       "hello",
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal([]spool.Item{{
		ID: ev.ID, Kind: "event", SessionID: ev.SessionID, Seq: ev.Seq, EventTime: ev.OccurredAt, Payload: payload,
	}})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ userAgent, want string }{
		{"loop-sessions/23713ea (darwin; arm64)", "23713ea"},
		{"loop-sessions/1.4.0", "1.4.0"},
		{"curl/8.7.1", ""},
		{"", ""},
		// The header is any caller's to set. A token outside the build shape
		// delivers no version rather than reaching a column that grows by
		// one element per distinct value: too long, a control byte, a SQL
		// fragment, a quote.
		{"loop-sessions/" + strings.Repeat("x", 41), ""},
		{"loop-sessions/abc\tdef", ""},
		{"loop-sessions/'; DROP TABLE sessions; --", ""},
		{"loop-sessions/1.4.0\x01", ""},
		{"loop-sessions/" + strings.Repeat("x", 40), strings.Repeat("x", 40)},
	} {
		r := httptest.NewRequest(http.MethodPost, ingest.EventsPath, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer device-token")
		if tc.userAgent != "" {
			r.Header.Set("User-Agent", tc.userAgent)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("User-Agent %q: status %d (%s)", tc.userAgent, w.Code, w.Body)
		}
		if got := st.last(t); got != tc.want {
			t.Errorf("User-Agent %q delivered version %q, want %q", tc.userAgent, got, tc.want)
		}
	}

	// Both upload routes are mounted: a request with no credential reaches
	// the handler's own 401, not the mux's 404.
	for _, path := range []string{ingest.EventsPath, ingest.HealthPath} {
		r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(nil))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("POST %s without a credential answered %d, want the handler's 401", path, w.Code)
		}
	}
}
