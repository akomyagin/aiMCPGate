package registry

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// TestStartReportCountsFilteredTools is the regression test for the doctor
// tool count: an upstream advertising more tools than its allow-filter lets
// through must be reported (UpstreamStatus.Tools → doctor's TOOLS column) with
// the PROJECTED count — what the client actually sees — not the raw
// advertisement.
func TestStartReportCountsFilteredTools(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "gh", Enabled: true, Tools: config.ToolFilter{
			Allow: []string{"search"},
		}},
	}}
	gh := &fakeUpstream{name: "gh", tools: []string{"search", "create_issue", "delete_repo"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"gh": gh})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	report := r.StartReport()
	if len(report) != 1 {
		t.Fatalf("report has %d entries, want 1: %+v", len(report), report)
	}
	if got := report[0]; !got.OK || got.Tools != 1 {
		t.Errorf("report entry = %+v, want OK=true Tools=1 (3 advertised, allow keeps 1)", got)
	}
}

// TestReloadFilterOnlyDoesNotRelaunch is the Stage 9 race-guard (run under
// -race): a reload that changes ONLY the tools filter (launch fields identical)
// must apply the new projection WITHOUT closing, relaunching, or even
// re-listing the upstream process. Two independent proofs:
//
//  1. connection identity — the registry must hold the very same conn before
//     and after the reload (a relaunch, including one via a wrongly-triggered
//     supervisor, would install a fresh one);
//  2. the fakeserver's tools file is rewritten before the reload — any
//     relaunch or network re-list would pick up the new list, so the catalog
//     still reflecting the ORIGINAL raw list proves no tools/list RPC ran.
func TestReloadFilterOnlyDoesNotRelaunch(t *testing.T) {
	bin := buildFakeServer(t)
	dir := t.TempDir()
	toolsFile := filepath.Join(dir, "tools")
	if err := os.WriteFile(toolsFile, []byte("alpha,beta"), 0o600); err != nil {
		t.Fatalf("seed tools file: %v", err)
	}
	base := config.Upstream{Name: "svc", Command: bin, Enabled: true, Env: map[string]string{
		"FAKE_TOOLS_FILE": toolsFile,
		"FAKE_ECHO":       "1",
	}}
	cfg := &config.Config{
		// Auto-restart ENABLED with a tiny backoff: if the filter-only path
		// wrongly closed the process, the supervisor would resurrect it with a
		// NEW connection — caught by the identity check below.
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
		},
		Upstreams: []config.Upstream{base},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "svc__alpha", 2*time.Second)
	waitForTool(t, r, "svc__beta", 2*time.Second)

	r.mu.RLock()
	connBefore := r.conns["svc"]
	r.mu.RUnlock()

	// Change what the fakeserver WOULD advertise on a fresh tools/list: if the
	// reload relaunches or re-lists, "gamma" shows up and alpha/beta vanish.
	if err := os.WriteFile(toolsFile, []byte("gamma"), 0o600); err != nil {
		t.Fatalf("update tools file: %v", err)
	}

	changed := base
	changed.Env = map[string]string{"FAKE_TOOLS_FILE": toolsFile, "FAKE_ECHO": "1"} // identical launch
	changed.Tools = config.ToolFilter{Deny: []string{"beta"}}
	newCfg := &config.Config{Restart: cfg.Restart, Upstreams: []config.Upstream{changed}}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// The new deny takes effect immediately (the filter-only branch is
	// synchronous inside Reload)...
	if hasTool(r, "svc__beta") {
		t.Error("denied tool svc__beta still in the catalog after filter-only reload")
	}
	if !hasTool(r, "svc__alpha") {
		t.Error("svc__alpha disappeared across a filter-only reload")
	}

	// ...and the process was neither relaunched nor re-listed. Give a wrongly
	// surviving/triggered supervisor several backoff cycles to act first.
	time.Sleep(300 * time.Millisecond)
	if hasTool(r, "svc__gamma") {
		t.Fatal("catalog picked up a fresh tools/list — filter-only reload must reuse the stored raw list")
	}
	r.mu.RLock()
	connAfter := r.conns["svc"]
	r.mu.RUnlock()
	if connBefore != connAfter {
		t.Fatal("upstream connection was replaced — filter-only reload must not relaunch the process")
	}
	// Still callable on the original, never-interrupted connection.
	if _, err := r.CallTool(context.Background(), "svc__alpha", []byte(`{}`)); err != nil {
		t.Fatalf("upstream not callable after filter-only reload: %v", err)
	}
}

// TestReloadAllowExpansionRestoresFilteredTool proves the stored raw tool list
// (rawTools) really is the source of a filter-only re-merge: a tool filtered
// out by the INITIAL allow-list must come back when a reload widens the list —
// even though its mcp.Tool was never in the client-facing catalog. The tools
// file is rewritten before the reload, so the restored tool can only have come
// from the raw list captured at Start, not from a fresh tools/list.
func TestReloadAllowExpansionRestoresFilteredTool(t *testing.T) {
	bin := buildFakeServer(t)
	dir := t.TempDir()
	toolsFile := filepath.Join(dir, "tools")
	if err := os.WriteFile(toolsFile, []byte("alpha,beta"), 0o600); err != nil {
		t.Fatalf("seed tools file: %v", err)
	}
	base := config.Upstream{Name: "svc", Command: bin, Enabled: true,
		Env:   map[string]string{"FAKE_TOOLS_FILE": toolsFile},
		Tools: config.ToolFilter{Allow: []string{"alpha"}},
	}
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{base},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "svc__alpha", 2*time.Second)
	if hasTool(r, "svc__beta") {
		t.Fatal("svc__beta must be filtered out by the initial allow-list")
	}

	// A fresh tools/list would now say "gamma" — the reload below must NOT do one.
	if err := os.WriteFile(toolsFile, []byte("gamma"), 0o600); err != nil {
		t.Fatalf("update tools file: %v", err)
	}

	widened := base
	widened.Env = map[string]string{"FAKE_TOOLS_FILE": toolsFile} // identical launch
	widened.Tools = config.ToolFilter{Allow: []string{"alpha", "beta"}}
	newCfg := &config.Config{Restart: cfg.Restart, Upstreams: []config.Upstream{widened}}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if !hasTool(r, "svc__beta") {
		t.Fatal("widening the allow-list did not restore the previously filtered tool — raw tool list lost")
	}
	if !hasTool(r, "svc__alpha") {
		t.Error("svc__alpha disappeared across the allow-widening reload")
	}
	if hasTool(r, "svc__gamma") {
		t.Error("catalog picked up a fresh tools/list — restoration must come from the stored raw list")
	}
}
