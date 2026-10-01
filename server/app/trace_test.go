package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/loopai-hq/loop-sessions/server/ingest"
	"github.com/loopai-hq/loop-sessions/server/store"
)

// traceServe runs one request through the middleware and a handler that logs
// from inside it, and returns the lines the logger wrote.
func traceServe(t *testing.T, project string, header http.Header) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	log := NewCloudLogger(slog.LevelInfo, &buf, "v")
	h := withTrace(project, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The component logger built with With, because that is how every
		// handler in this server holds its logger, and the wrapper has to
		// survive it.
		log.With("component", "test").InfoContext(r.Context(), "inside the request")
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	return cloudLines(t, &buf)
}

// TestTraceMiddlewareCorrelatesLinesWithTheRequestLog is the correlation
// contract. Cloud Run writes a request entry carrying the trace it minted and
// sends the same id in X-Cloud-Trace-Context; a container line under
// logging.googleapis.com/trace with the full resource name is grouped with
// that entry. The span arrives in decimal and leaves in sixteen hex digits,
// because that is the form the LogEntry field takes.
func TestTraceMiddlewareCorrelatesLinesWithTheRequestLog(t *testing.T) {
	t.Parallel()
	lines := traceServe(t, "example-project-12345", http.Header{
		"X-Cloud-Trace-Context": {"105445aa7843bc8bf206b12000100000/1234567890;o=1"},
	})
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	entry := lines[0]
	if got, want := entry[cloudTraceKey], "projects/example-project-12345/traces/105445aa7843bc8bf206b12000100000"; got != want {
		t.Errorf("trace = %v, want %s", got, want)
	}
	// 1234567890 in hex is 499602d2, left-padded to sixteen digits.
	if got, want := entry[cloudSpanKey], "00000000499602d2"; got != want {
		t.Errorf("spanId = %v, want %s", got, want)
	}
	if entry[cloudTraceSampledKey] != true {
		t.Errorf("o=1 did not become trace_sampled: %v", entry)
	}
	if entry["component"] != "test" || entry["version"] != "v" {
		t.Errorf("the request line lost its ordinary attributes: %v", entry)
	}
}

// TestTraceMiddlewareFallsBackToTraceparent covers a caller that speaks W3C:
// nothing today, and whichever load balancer is put in front of this service
// tomorrow.
func TestTraceMiddlewareFallsBackToTraceparent(t *testing.T) {
	t.Parallel()
	lines := traceServe(t, "p", http.Header{
		"Traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	})
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	if got, want := lines[0][cloudTraceKey], "projects/p/traces/4bf92f3577b34da6a3ce929d0e0e4736"; got != want {
		t.Errorf("trace = %v, want %s", got, want)
	}
	if got, want := lines[0][cloudSpanKey], "00f067aa0ba902b7"; got != want {
		t.Errorf("spanId = %v, want %s", got, want)
	}
	if lines[0][cloudTraceSampledKey] != true {
		t.Errorf("flags 01 did not become trace_sampled: %v", lines[0])
	}
}

// TestTraceMiddlewareIgnoresWhatItCannotUse: a malformed header, or a
// deployment with no project, produces a line with no trace at all rather
// than a trace name that matches no request entry. Noise in every query is
// worse than an orphan line.
func TestTraceMiddlewareIgnoresWhatItCannotUse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		project string
		header  http.Header
	}{
		{"no header", "p", http.Header{}},
		{"no project", "", http.Header{"X-Cloud-Trace-Context": {"105445aa7843bc8bf206b12000100000/1;o=1"}}},
		{"short trace id", "p", http.Header{"X-Cloud-Trace-Context": {"105445aa/1;o=1"}}},
		{"non-hex trace id", "p", http.Header{"X-Cloud-Trace-Context": {"zz5445aa7843bc8bf206b12000100000/1"}}},
		{"span is not a number", "p", http.Header{"X-Cloud-Trace-Context": {"105445aa7843bc8bf206b12000100000/abc"}}},
		{"no span", "p", http.Header{"X-Cloud-Trace-Context": {"105445aa7843bc8bf206b12000100000"}}},
		{"traceparent with the wrong shape", "p", http.Header{"Traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-01"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lines := traceServe(t, tc.project, tc.header)
			if len(lines) != 1 {
				t.Fatalf("got %d lines, want 1", len(lines))
			}
			for _, key := range []string{cloudTraceKey, cloudSpanKey, cloudTraceSampledKey} {
				if _, ok := lines[0][key]; ok {
					t.Errorf("%s: line carries %q from a header it should not have used: %v", tc.name, key, lines[0])
				}
			}
		})
	}
}

// TestTraceMiddlewareIsMountedOnTheAssembledServer: the middleware exists only
// if routes() wraps the mux with it. This drives the assembled handler and
// checks the readiness line, which is logged with the request context, carries
// the trace. A middleware nobody mounted is the same defect as a sweeper nobody
// calls.
func TestTraceMiddlewareIsMountedOnTheAssembledServer(t *testing.T) {
	var buf bytes.Buffer
	log := NewCloudLogger(slog.LevelInfo, &buf, "v")
	st := store.NewWithDB(&bootDB{}, ingest.NewPricer(nil, log))
	a, err := build(context.Background(), bootConfig(t), log, st,
		func(context.Context) error { return errors.New("the database is gone") })
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, ReadyzPath, nil)
	req.Header.Set("X-Cloud-Trace-Context", "105445aa7843bc8bf206b12000100000/7;o=1")
	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503 with a failing ping", rec.Code)
	}
	var found bool
	for _, entry := range cloudLines(t, &buf) {
		if entry["message"] != "readiness check failed" {
			continue
		}
		found = true
		if _, ok := entry[cloudTraceKey]; !ok {
			t.Errorf("the readiness line was logged inside a traced request and carries no trace: %v", entry)
		}
	}
	if !found {
		t.Fatalf("no readiness line was logged:\n%s", buf.String())
	}
}

// TestLivezNamesTheBuild: the rollout gate reads the /livez body to learn
// which build is serving without a `gcloud run services describe`, so the body
// is a contract and not a courtesy. The pre-existing liveness test checks the
// status only, and both paths, because they are meant to be one handler.
func TestLivezNamesTheBuild(t *testing.T) {
	var buf bytes.Buffer
	log := NewCloudLogger(slog.LevelInfo, &buf, "abc")
	cfg := bootConfig(t)
	cfg.Version = "abc"
	st := store.NewWithDB(&bootDB{}, ingest.NewPricer(nil, log))
	a, err := build(context.Background(), cfg, log, st, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, path := range []string{LivezPath, HealthzPath} {
		rec := httptest.NewRecorder()
		a.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", path, rec.Code)
		}
		if got, want := rec.Body.String(), "ok abc\n"; got != want {
			t.Errorf("%s body = %q, want %q", path, got, want)
		}
	}
}
