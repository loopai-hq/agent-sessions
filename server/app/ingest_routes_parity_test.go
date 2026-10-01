package app

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/loopai-hq/loop-sessions/server/ingest"
)

// RegisterIngest restates the ingest handler's route list, because the
// handler's Register takes a mux and no middleware and this package does not
// own it. A route the handler grows later is reachable through its ServeHTTP
// and, until it is added to RegisterIngest as well, answers 404 on the
// server. This test makes that failure loud instead of silent: every path
// the ingest package names is probed under every method on both mountings,
// and the two must resolve to the same pattern. The paths are read from the
// package's own source, because it exports no list of them and a list typed
// here would be the third copy of the thing this test exists to catch.
func TestRegisterIngestMountsEveryRouteTheHandlerRegisters(t *testing.T) {
	paths := ingestRoutePaths(t)
	if len(paths) < 2 {
		t.Fatalf("read %v from the ingest package; expected at least the upload and health paths", paths)
	}

	h, err := ingest.New(ingest.Options{
		Store:   &uaStore{},
		Devices: uaDevices{},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	own := http.NewServeMux()
	h.Register(own)
	ours := http.NewServeMux()
	RegisterIngest(ours, h)

	for _, path := range paths {
		for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			r := httptest.NewRequest(method, path, nil)
			_, want := own.Handler(r)
			_, got := ours.Handler(r)
			if got != want {
				t.Errorf("%s %s: the handler's own mounting resolves to %q, RegisterIngest to %q", method, path, want, got)
			}
		}
	}
}

// ingestRoutePaths reads the ingest package's non-test source for the paths
// it could route: every string constant that starts with "/" and every
// pattern literal inside a Register method. A path the package names but
// does not route is harmless here, since both mountings then answer with no
// pattern; one it routes and this misses would need a route registered from
// a value that is neither a constant nor a literal, which nothing in the
// package does.
func ingestRoutePaths(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join("..", "ingest")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	seen := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.ValueSpec:
				for _, v := range n.Values {
					if p := routePath(v); p != "" {
						seen[p] = true
					}
				}
			case *ast.FuncDecl:
				if n.Recv == nil || n.Name.Name != "Register" || n.Body == nil {
					return true
				}
				ast.Inspect(n.Body, func(m ast.Node) bool {
					if p := routePath(m); p != "" {
						seen[p] = true
					}
					return true
				})
			}
			return true
		})
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// routePath returns the path inside a string literal node: the literal
// itself when it starts with "/", or what follows the method when it is a
// "METHOD /path" pattern. Anything else is not a route.
func routePath(n ast.Node) string {
	lit, ok := n.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return ""
	}
	if strings.HasPrefix(s, "/") {
		return s
	}
	if _, rest, ok := strings.Cut(s, " "); ok && strings.HasPrefix(rest, "/") {
		return rest
	}
	return ""
}
