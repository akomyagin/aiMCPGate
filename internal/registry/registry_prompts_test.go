package registry

// Round 4 tests: prompt aggregation, namespacing, routing and lifecycle —
// mirrors of the tools tests in registry_test.go, exercised through the same
// fakeUpstream (whose ListPrompts/GetPrompt the registry may only touch when
// the fake's caps declare "prompts").

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// promptsCaps is the capabilities object a prompt-advertising fake reports.
var promptsCaps = json.RawMessage(`{"tools":{},"prompts":{}}`)

// promptCatalog flattens Prompts() into "name→upstream" for easy assertions.
func promptCatalog(r *Registry) map[string]string {
	got := map[string]string{}
	for _, d := range r.Prompts() {
		got[d.Name] = d.Upstream
	}
	return got
}

// TestRegistryAggregatesAndNamespacesPrompts: prompts from a capability-
// declaring upstream land in the catalog namespaced "<upstream>__<prompt>";
// an upstream without the capability contributes none. A filter-only
// re-projection (remergeUpstream, which fetches no fresh prompts) must carry
// the recorded prompts over instead of losing them.
func TestRegistryAggregatesAndNamespacesPrompts(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "docs", Enabled: boolPtr(true)},
		{Name: "bare", Enabled: boolPtr(true)},
	}}
	fakes := map[string]*fakeUpstream{
		"docs": {name: "docs", tools: []string{"search"}, caps: promptsCaps, prompts: []string{"summarize", "review"}},
		"bare": {name: "bare", tools: []string{"fetch"}}, // no prompts capability
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	want := map[string]string{
		"docs__summarize": "docs",
		"docs__review":    "docs",
	}
	got := promptCatalog(r)
	if len(got) != len(want) {
		t.Fatalf("prompt catalog = %v, want %v", got, want)
	}
	for name, up := range want {
		if got[name] != up {
			t.Errorf("prompt %q owned by %q, want %q", name, got[name], up)
		}
	}
	// The verbatim prompt payload survives aggregation.
	for _, d := range r.Prompts() {
		if d.Name == "docs__summarize" && string(d.Prompt.Description) != `"prompt summarize"` {
			t.Errorf("description not verbatim: %s", d.Prompt.Description)
		}
	}

	// A re-projection without a fresh prompts fetch keeps the recorded prompts.
	r.remergeUpstream("docs")
	if got := promptCatalog(r); len(got) != len(want) {
		t.Errorf("prompt catalog after remerge = %v, want %v (prompts lost by carry-over)", got, want)
	}
}

// TestRegistryPromptCollisionKeepFirst: prompts are always namespaced
// "<upstream>__<original>", so a client-facing collision needs a name that
// smuggles the separator — upstream "alpha" advertising prompt "x__p" and
// upstream "alpha__x" advertising prompt "p" both project "alpha__x__p". The
// winner must be the upstream listed FIRST in the config (Start merges
// sequentially in config order), the loser skipped — same keep-first policy
// as tools.
func TestRegistryPromptCollisionKeepFirst(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "alpha", Enabled: boolPtr(true)},
		{Name: "alpha__x", Enabled: boolPtr(true)},
	}}
	fakes := map[string]*fakeUpstream{
		"alpha":    {name: "alpha", tools: []string{"a"}, caps: promptsCaps, prompts: []string{"x__p"}},
		"alpha__x": {name: "alpha__x", tools: []string{"b"}, caps: promptsCaps, prompts: []string{"p"}},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	descs := r.Prompts()
	if len(descs) != 1 {
		t.Fatalf("prompt catalog has %d entries, want 1 (keep-first dedup): %+v", len(descs), descs)
	}
	if descs[0].Name != "alpha__x__p" || descs[0].Upstream != "alpha" {
		t.Errorf("collided prompt = %q owned by %q, want %q owned by %q (config order wins)",
			descs[0].Name, descs[0].Upstream, "alpha__x__p", "alpha")
	}

	// The route must agree with the surviving descriptor: prompts/get reaches
	// the keep-first winner with ITS original name.
	if _, err := r.GetPrompt(context.Background(), "alpha__x__p", nil); err != nil {
		t.Fatalf("GetPrompt: %v", err)
	}
	if got := fakes["alpha"].lastPromptName; got != "x__p" {
		t.Errorf("winner received prompt name %q, want its original %q", got, "x__p")
	}
	if got := fakes["alpha__x"].lastPromptName; got != "" {
		t.Errorf("loser was called (name=%q), want untouched", got)
	}
}

// TestRegistryGetPromptRoutesAndRewritesName: prompts/get resolves the owning
// upstream and forwards the ORIGINAL name plus the arguments verbatim; the
// other upstream is never touched.
func TestRegistryGetPromptRoutesAndRewritesName(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "docs", Enabled: boolPtr(true)},
		{Name: "web", Enabled: boolPtr(true)},
	}}
	docs := &fakeUpstream{name: "docs", tools: []string{"a"}, caps: promptsCaps, prompts: []string{"greet"}}
	web := &fakeUpstream{name: "web", tools: []string{"b"}, caps: promptsCaps, prompts: []string{"greet"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"docs": docs, "web": web})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	args := json.RawMessage(`{"style":"formal"}`)
	resp, err := r.GetPrompt(context.Background(), "web__greet", args)
	if err != nil {
		t.Fatalf("GetPrompt: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("rpc error: %v", resp.Error)
	}
	if web.lastPromptName != "greet" {
		t.Errorf("web received prompt name %q, want original %q", web.lastPromptName, "greet")
	}
	if docs.lastPromptName != "" {
		t.Errorf("docs was called but should not have been (name=%q)", docs.lastPromptName)
	}
	if string(web.lastPromptArgs) != string(args) {
		t.Errorf("arguments not forwarded verbatim: %s", web.lastPromptArgs)
	}
	if !strings.Contains(string(resp.Result), "prompt greet") {
		t.Errorf("unexpected result: %s", resp.Result)
	}
}

// TestRegistryGetPromptUnknownErrors: an unrouted name yields a clean error,
// naming only the prompt the client itself asked for.
func TestRegistryGetPromptUnknownErrors(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "docs", Enabled: boolPtr(true)}}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"docs": {name: "docs", tools: []string{"a"}, caps: promptsCaps, prompts: []string{"greet"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	_, err := r.GetPrompt(context.Background(), "nope__nope", nil)
	if err == nil {
		t.Fatal("expected error for unknown prompt")
	}
	if want := `unknown prompt "nope__nope"`; err.Error() != want {
		t.Errorf("error = %q, want %q", err.Error(), want)
	}
}

// panicListPromptsUpstream fails the test loudly if ListPrompts is ever called
// — the probe for "no capability, no call".
type panicListPromptsUpstream struct {
	*fakeUpstream
}

func (p *panicListPromptsUpstream) ListPrompts(context.Context) ([]mcp.Prompt, error) {
	panic("ListPrompts called on an upstream that never declared the prompts capability")
}

// TestRegistryNoListPromptsWithoutCapability: an upstream whose initialize
// declares no "prompts" capability must never receive prompts/list at all —
// not merely have its (error) result discarded.
func TestRegistryNoListPromptsWithoutCapability(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "bare", Enabled: boolPtr(true)}}}
	bare := &panicListPromptsUpstream{
		fakeUpstream: &fakeUpstream{name: "bare", tools: []string{"a"}, caps: json.RawMessage(`{"tools":{}}`)},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) { return bare, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if got := r.Prompts(); len(got) != 0 {
		t.Errorf("prompt catalog = %+v, want empty", got)
	}
}

// TestRegistryListPromptsErrorDoesNotFailLaunch: an upstream that DECLARES the
// prompts capability but errors on prompts/list is degraded to "no prompts" —
// its tools still come up, Start does not fail, the catalog just has no
// prompts from it (best-effort, unlike the fatal tools/list).
func TestRegistryListPromptsErrorDoesNotFailLaunch(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "flaky", Enabled: boolPtr(true)}}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"flaky": {name: "flaky", tools: []string{"a"}, caps: promptsCaps,
			prompts: []string{"greet"}, promptsErr: errors.New("prompts exploded")},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start must tolerate a failing prompts/list: %v", err)
	}
	defer r.Close()

	tools := r.Tools()
	if len(tools) != 1 || tools[0].Name != "flaky__a" {
		t.Fatalf("tools = %+v, want flaky__a (tools must survive a prompts/list failure)", tools)
	}
	if got := r.Prompts(); len(got) != 0 {
		t.Errorf("prompt catalog = %+v, want empty after a failed prompts/list", got)
	}
}

// TestRegistryDropUpstreamClearsPrompts: dropping an upstream removes its
// prompts and routes from the catalog, leaving the other upstream's intact —
// and prompts/get against the dropped name fails cleanly.
func TestRegistryDropUpstreamClearsPrompts(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "docs", Enabled: boolPtr(true)},
		{Name: "web", Enabled: boolPtr(true)},
	}}
	fakes := map[string]*fakeUpstream{
		"docs": {name: "docs", tools: []string{"a"}, caps: promptsCaps, prompts: []string{"summarize"}},
		"web":  {name: "web", tools: []string{"b"}, caps: promptsCaps, prompts: []string{"greet"}},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	r.dropUpstream("docs")

	got := promptCatalog(r)
	if len(got) != 1 || got["web__greet"] != "web" {
		t.Fatalf("prompt catalog after drop = %v, want only web__greet", got)
	}
	if _, err := r.GetPrompt(context.Background(), "docs__summarize", nil); err == nil {
		t.Error("GetPrompt on a dropped upstream's prompt should error")
	}
}
