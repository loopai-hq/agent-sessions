package app

import (
	"context"
	"reflect"
	"testing"

	"github.com/loopai-hq/loop-sessions/server/store"
	"github.com/loopai-hq/loop-sessions/server/web"
)

// The store is the single authority on which values session_type may hold.
// The web cannot import the store, so it keeps a copy for its menus, and this
// is the test that makes the copy honest: the two lists are equal, in order,
// or the page offers a type the store will silently drop before SQL.
func TestWebSessionTypesEqualTheStoreAuthority(t *testing.T) {
	if got, want := web.SessionTypeOptions(), store.SessionTypes; !reflect.DeepEqual(got, want) {
		t.Errorf("web offers %v, store holds %v", got, want)
	}
}

// The client build rides the context from the request to the store adapter,
// and an absent or blank one delivers nothing rather than a wrong value.
func TestAgentVersionRidesTheContext(t *testing.T) {
	ctx := context.Background()
	if got := agentVersionOf(ctx); got != "" {
		t.Errorf("a bare context carried version %q", got)
	}
	if got := agentVersionOf(WithAgentVersion(ctx, "  ")); got != "" {
		t.Errorf("a blank version was carried as %q", got)
	}
	if got := agentVersionOf(WithAgentVersion(ctx, " 23713ea ")); got != "23713ea" {
		t.Errorf("version carried as %q, want the trimmed build", got)
	}
}
