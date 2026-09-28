package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// modulePath is this module, so an import can be told from a standard or
// third-party one.
const modulePath = "github.com/loopai-hq/agent-sessions"

// portBoundary is every package of this module that server/api may import.
// ports.go exists so this package reaches the store through interfaces it
// declares itself; an import of the store, the derive package or an
// emitter would put the concrete types back behind that boundary and make
// a store change a read-API change. server/auth carries the cookie and
// token helpers the routes are built on; internal/skilllog carries the
// refusal line's message, which the catalog PUT must spell exactly as
// server/skillusage does (ruled 2026-09-22 from LS-3 review-3) and which
// is a string, not a dependency on anything.
var portBoundary = map[string]bool{
	modulePath + "/server/auth":       true,
	modulePath + "/internal/skilllog": true,
}

// TestServerAPIImportsNothingBehindItsPorts walks this package's non-test
// files and refuses any module import outside portBoundary. It is the
// cheap, offline form of `go list -deps ./server/api`: the allowlist's two
// members are leaves (pinned below), so their transitive closure adds
// nothing, and a new edge here is a compile-time list to justify rather
// than a graph nobody looks at.
func TestServerAPIImportsNothingBehindItsPorts(t *testing.T) {
	for path, f := range parseGoDir(t, ".") {
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			if !strings.HasPrefix(p, modulePath) || portBoundary[p] {
				continue
			}
			t.Errorf("%s imports %s, which is behind server/api's ports; declare what you need in ports.go or move the value to a leaf package", path, p)
		}
	}
}

// TestSkillLogIsALeaf pins the other half: the package the refusal line
// lives in imports nothing of ours, so importing it can never widen this
// package's graph.
func TestSkillLogIsALeaf(t *testing.T) {
	dir := filepath.Join("..", "..", "internal", "skilllog")
	files := parseGoDir(t, dir)
	if len(files) != 1 {
		t.Errorf("internal/skilllog has %d files, want the one holding the message strings", len(files))
	}
	for path, f := range files {
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			if strings.HasPrefix(p, modulePath) {
				t.Errorf("%s imports %s; internal/skilllog is a leaf on purpose", path, p)
			}
		}
		for _, d := range f.Decls {
			if _, ok := d.(*ast.FuncDecl); ok {
				t.Errorf("%s declares a function; internal/skilllog holds message strings only", path)
			}
		}
	}
}

// parseGoDir parses every non-test Go file in dir.
func parseGoDir(t *testing.T, dir string) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	out := map[string]*ast.File{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		out[filepath.Join(dir, name)] = f
	}
	return out
}
