package registry

import (
	"context"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// TestToolsCacheInvalidatedOnCatalogMutation pins the tools-catalog cache:
// Tools() serves the same cached slice while the catalog is unchanged, and
// every catalog mutation (merging a new upstream, dropping an existing one —
// the mergeLocked/dropLocked paths installLocked is also built from)
// invalidates it, so the next read reflects the new state.
func TestToolsCacheInvalidatedOnCatalogMutation(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "a", Enabled: true}}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"a": {name: "a", tools: []string{"x"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	catalog := func() string {
		var names []string
		for _, d := range r.Tools() {
			names = append(names, d.Name)
		}
		return strings.Join(names, ",") // Tools() is sorted, so this is canonical
	}

	if got := catalog(); got != "a__x" {
		t.Fatalf("initial catalog = %q, want %q", got, "a__x")
	}
	// A second read with no mutation in between must serve the cache — the very
	// same backing slice, not a freshly rebuilt copy.
	first, again := r.Tools(), r.Tools()
	if &first[0] != &again[0] {
		t.Error("Tools() rebuilt the catalog without a mutation in between; want the cached slice back")
	}

	// Merging another upstream must invalidate the cache: the next read includes
	// both upstreams' tools.
	b := &fakeUpstream{name: "b", tools: []string{"y"}}
	tools, err := b.ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	r.merge("b", b, tools)
	if got := catalog(); got != "a__x,b__y" {
		t.Fatalf("catalog after merge = %q, want %q (stale cache?)", got, "a__x,b__y")
	}

	// Dropping an upstream must invalidate it too.
	r.dropUpstream("a")
	if got := catalog(); got != "b__y" {
		t.Fatalf("catalog after drop = %q, want %q (stale cache?)", got, "b__y")
	}
	if got := r.ToolCount(); got != 1 {
		t.Errorf("ToolCount() = %d, want 1", got)
	}
}
