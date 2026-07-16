package registry

import (
	"context"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// TestFilterAndRenameTools drives the pure Stage 9 projection: allow
// (intersection when non-empty), then deny (subtraction), then rename
// (client-facing name; default "<upstream>__<original>" otherwise).
func TestFilterAndRenameTools(t *testing.T) {
	toolList := func(names ...string) []mcp.Tool {
		out := make([]mcp.Tool, 0, len(names))
		for _, n := range names {
			out = append(out, mcp.Tool{Name: n})
		}
		return out
	}
	tests := []struct {
		name   string
		tools  []mcp.Tool
		filter config.ToolFilter
		want   map[string]string // client-facing name → original name
	}{
		{
			name:   "empty filter passes all with default namespacing",
			tools:  toolList("a", "b"),
			filter: config.ToolFilter{},
			want:   map[string]string{"up__a": "a", "up__b": "b"},
		},
		{
			name:   "allow only keeps the intersection",
			tools:  toolList("a", "b", "c"),
			filter: config.ToolFilter{Allow: []string{"a", "c", "phantom"}},
			want:   map[string]string{"up__a": "a", "up__c": "c"},
		},
		{
			name:   "deny only subtracts",
			tools:  toolList("a", "b", "c"),
			filter: config.ToolFilter{Deny: []string{"b"}},
			want:   map[string]string{"up__a": "a", "up__c": "c"},
		},
		{
			name:   "deny wins over allow",
			tools:  toolList("a", "b"),
			filter: config.ToolFilter{Allow: []string{"a", "b"}, Deny: []string{"b"}},
			want:   map[string]string{"up__a": "a"},
		},
		{
			name:   "rename without allow or deny",
			tools:  toolList("a", "b"),
			filter: config.ToolFilter{Rename: map[string]string{"a": "short_a"}},
			want:   map[string]string{"short_a": "a", "up__b": "b"},
		},
		{
			name:  "rename applies after allow and deny",
			tools: toolList("a", "b", "c"),
			filter: config.ToolFilter{
				Allow:  []string{"a", "b"},
				Deny:   []string{"b"},
				Rename: map[string]string{"a": "short_a", "b": "never_used"},
			},
			want: map[string]string{"short_a": "a"},
		},
		{
			name:   "no tools advertised yields empty projection",
			tools:  nil,
			filter: config.ToolFilter{Allow: []string{"a"}},
			want:   map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := map[string]string{}
			for _, e := range filterAndRenameTools("up", tt.tools, tt.filter) {
				got[e.name] = e.tool.Name
			}
			if len(got) != len(tt.want) {
				t.Fatalf("projection = %v, want %v", got, tt.want)
			}
			for name, orig := range tt.want {
				if got[name] != orig {
					t.Errorf("entry %q → original %q, want %q", name, got[name], orig)
				}
			}
		})
	}
}

// TestRegistryAppliesToolFilter checks the filter reaches the live catalog via
// mergeLocked on Start, and that a RENAMED tool is callable under its
// client-facing name and routed to the upstream under the ORIGINAL name.
func TestRegistryAppliesToolFilter(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "gh", Enabled: true, Tools: config.ToolFilter{
			Deny:   []string{"delete_repo"},
			Rename: map[string]string{"search": "gh_search"},
		}},
		{Name: "web", Enabled: true}, // no filter: everything passes through
	}}
	gh := &fakeUpstream{name: "gh", tools: []string{"search", "create_issue", "delete_repo"}}
	web := &fakeUpstream{name: "web", tools: []string{"fetch"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"gh": gh, "web": web})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	got := map[string]bool{}
	for _, d := range r.Tools() {
		got[d.Name] = true
	}
	for _, want := range []string{"gh_search", "gh__create_issue", "web__fetch"} {
		if !got[want] {
			t.Errorf("catalog missing %q: %v", want, got)
		}
	}
	for _, absent := range []string{"gh__search", "gh__delete_repo"} {
		if got[absent] {
			t.Errorf("catalog must not contain %q (filtered/renamed): %v", absent, got)
		}
	}

	// The renamed tool routes back to the upstream's ORIGINAL name.
	if _, err := r.CallTool(context.Background(), "gh_search", nil); err != nil {
		t.Fatalf("CallTool renamed tool: %v", err)
	}
	if gh.lastNamed != "search" {
		t.Errorf("upstream received name %q, want original %q", gh.lastNamed, "search")
	}
	// The denied tool is not callable even by its would-be namespaced name.
	if _, err := r.CallTool(context.Background(), "gh__delete_repo", nil); err == nil {
		t.Error("denied tool must not be callable")
	}
}
