package registry

// Round 5 tests: resource aggregation (URI-addressed, no namespacing),
// resource templates with {var} matching, resources/read routing and
// completion/complete routing — mirrors of the prompt tests in
// registry_prompts_test.go, exercised through the same fakeUpstream (whose
// ListResources/ListResourceTemplates the registry may only touch when the
// fake's caps declare "resources").

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// resourcesCaps is the capabilities object a resource-advertising fake reports.
var resourcesCaps = json.RawMessage(`{"tools":{},"resources":{}}`)

// resourceCatalog flattens Resources() into "uri→upstream" for easy assertions.
func resourceCatalog(r *Registry) map[string]string {
	got := map[string]string{}
	for _, d := range r.Resources() {
		got[d.URI] = d.Upstream
	}
	return got
}

// TestRegistryAggregatesResources: resources from a capability-declaring
// upstream land in the catalog under their ORIGINAL URIs (no namespacing);
// an upstream without the capability contributes none. Templates aggregate
// alongside. A filter-only re-projection (remergeUpstream, which fetches
// nothing fresh) must carry both over instead of losing them.
func TestRegistryAggregatesResources(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "files", Enabled: boolPtr(true)},
		{Name: "bare", Enabled: boolPtr(true)},
	}}
	fakes := map[string]*fakeUpstream{
		"files": {name: "files", tools: []string{"read"}, caps: resourcesCaps,
			resources: []string{"file:///a.txt", "file:///b.txt"},
			templates: []string{"file:///logs/{name}.log"}},
		"bare": {name: "bare", tools: []string{"fetch"}}, // no resources capability
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	want := map[string]string{
		"file:///a.txt": "files",
		"file:///b.txt": "files",
	}
	got := resourceCatalog(r)
	if len(got) != len(want) {
		t.Fatalf("resource catalog = %v, want %v", got, want)
	}
	for uri, up := range want {
		if got[uri] != up {
			t.Errorf("resource %q owned by %q, want %q", uri, got[uri], up)
		}
	}
	// The verbatim resource payload survives aggregation.
	for _, d := range r.Resources() {
		if d.URI == "file:///a.txt" && string(d.Resource.Description) != `"resource file:///a.txt"` {
			t.Errorf("description not verbatim: %s", d.Resource.Description)
		}
	}
	tpls := r.ResourceTemplates()
	if len(tpls) != 1 || tpls[0].URITemplate != "file:///logs/{name}.log" || tpls[0].Upstream != "files" {
		t.Fatalf("templates = %+v, want the one files template", tpls)
	}

	// A re-projection without a fresh fetch keeps resources AND templates.
	r.remergeUpstream("files")
	if got := resourceCatalog(r); len(got) != len(want) {
		t.Errorf("resource catalog after remerge = %v, want %v (lost by carry-over)", got, want)
	}
	if tpls := r.ResourceTemplates(); len(tpls) != 1 {
		t.Errorf("templates after remerge = %+v, want 1 (lost by carry-over)", tpls)
	}
}

// TestRegistryResourceURICollisionKeepFirst: resources are NOT namespaced, so
// two upstreams advertising the same URI collide directly. The winner must be
// the upstream listed FIRST in the config (Start merges sequentially in config
// order), the loser skipped — and resources/read must reach the winner.
func TestRegistryResourceURICollisionKeepFirst(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "alpha", Enabled: boolPtr(true)},
		{Name: "beta", Enabled: boolPtr(true)},
	}}
	alpha := &fakeUpstream{name: "alpha", tools: []string{"a"}, caps: resourcesCaps, resources: []string{"file:///shared.txt"}}
	beta := &fakeUpstream{name: "beta", tools: []string{"b"}, caps: resourcesCaps, resources: []string{"file:///shared.txt"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"alpha": alpha, "beta": beta})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	descs := r.Resources()
	if len(descs) != 1 {
		t.Fatalf("resource catalog has %d entries, want 1 (keep-first dedup): %+v", len(descs), descs)
	}
	if descs[0].URI != "file:///shared.txt" || descs[0].Upstream != "alpha" {
		t.Errorf("collided resource = %q owned by %q, want owned by %q (config order wins)",
			descs[0].URI, descs[0].Upstream, "alpha")
	}

	if _, err := r.ReadResource(context.Background(), "file:///shared.txt"); err != nil {
		t.Fatalf("ReadResource: %v", err)
	}
	if alpha.lastReadURI != "file:///shared.txt" {
		t.Errorf("winner received uri %q, want %q", alpha.lastReadURI, "file:///shared.txt")
	}
	if beta.lastReadURI != "" {
		t.Errorf("loser was called (uri=%q), want untouched", beta.lastReadURI)
	}
}

// TestRegistryReadResourceRoutesByURI: resources/read resolves the owning
// upstream by exact URI and forwards it untouched; the other upstream is never
// touched.
func TestRegistryReadResourceRoutesByURI(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "docs", Enabled: boolPtr(true)},
		{Name: "web", Enabled: boolPtr(true)},
	}}
	docs := &fakeUpstream{name: "docs", tools: []string{"a"}, caps: resourcesCaps, resources: []string{"file:///docs/readme.md"}}
	web := &fakeUpstream{name: "web", tools: []string{"b"}, caps: resourcesCaps, resources: []string{"https://example.com/page"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"docs": docs, "web": web})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	resp, err := r.ReadResource(context.Background(), "https://example.com/page")
	if err != nil {
		t.Fatalf("ReadResource: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("rpc error: %v", resp.Error)
	}
	if web.lastReadURI != "https://example.com/page" {
		t.Errorf("web received uri %q, want it verbatim", web.lastReadURI)
	}
	if docs.lastReadURI != "" {
		t.Errorf("docs was called but should not have been (uri=%q)", docs.lastReadURI)
	}
	if !strings.Contains(string(resp.Result), "read by web") {
		t.Errorf("unexpected result: %s", resp.Result)
	}
}

// TestRegistryReadResourceUnknownErrors: an unowned uri yields a clean error
// wrapping ErrUnknownResource (the dispatcher's Invalid-params signal), naming
// only the uri the client itself asked for.
func TestRegistryReadResourceUnknownErrors(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "docs", Enabled: boolPtr(true)}}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"docs": {name: "docs", tools: []string{"a"}, caps: resourcesCaps, resources: []string{"file:///a.txt"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	_, err := r.ReadResource(context.Background(), "file:///nope.txt")
	if err == nil {
		t.Fatal("expected error for unknown resource")
	}
	if !errors.Is(err, ErrUnknownResource) {
		t.Errorf("error %v does not wrap ErrUnknownResource", err)
	}
	if want := `unknown resource "file:///nope.txt"`; err.Error() != want {
		t.Errorf("error = %q, want %q", err.Error(), want)
	}
}

// TestTemplateRegexp: the {var}→regexp translation must treat literal parts
// literally (dots, pluses and other regexp metacharacters), match one
// non-empty path segment per variable, and anchor both ends.
func TestTemplateRegexp(t *testing.T) {
	tests := []struct {
		tmpl  string
		uri   string
		match bool
	}{
		{"file:///logs/{name}.log", "file:///logs/app.log", true},
		{"file:///logs/{name}.log", "file:///logs/appXlog", false},     // "." is literal, not any-char
		{"file:///logs/{name}.log", "file:///logs/a/b.log", false},     // {var} never crosses "/"
		{"file:///logs/{name}.log", "file:///logs/.log", false},        // variable must be non-empty
		{"file:///logs/{name}.log", "file:///logs/app.log.bak", false}, // anchored at the end
		{"file:///logs/{name}.log", "xfile:///logs/app.log", false},    // anchored at the start
		{"c++:///{mod}/doc", "c++:///vector/doc", true},                // "+" in the literal part is literal
		{"c++:///{mod}/doc", "cxx:///vector/doc", false},               //
		{"db://{schema}/{table}", "db://public/users", true},           // several variables
		{"db://{schema}/{table}", "db://public", false},                //
		{"file:///plain.txt", "file:///plain.txt", true},               // no variables at all
		{"file:///weird{unclosed", "file:///weird{unclosed", true},     // unclosed brace stays literal
		{"file:///weird{unclosed", "file:///weirdXunclosed", false},    //
		{"greet://{a}-{b}", "greet://hello-world", true},               // adjacent vars with a literal
		{"greet://{a}-{b}", "greet://helloworld", false},               //
	}
	for _, tc := range tests {
		re, err := templateRegexp(tc.tmpl)
		if err != nil {
			t.Fatalf("templateRegexp(%q): %v", tc.tmpl, err)
		}
		if got := re.MatchString(tc.uri); got != tc.match {
			t.Errorf("template %q vs uri %q: match=%v, want %v (regexp %s)", tc.tmpl, tc.uri, got, tc.match, re)
		}
	}
}

// TestRegistryReadResourceTemplateMatch: a uri with no exact catalog entry is
// matched against the aggregated templates — the FIRST matching template in
// merge (config) order wins, and the read is forwarded to its owner with the
// uri untouched. A uri matching nothing stays unknown.
func TestRegistryReadResourceTemplateMatch(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "logs", Enabled: boolPtr(true)},
		{Name: "catchall", Enabled: boolPtr(true)},
	}}
	logs := &fakeUpstream{name: "logs", tools: []string{"a"}, caps: resourcesCaps,
		templates: []string{"file:///logs/{name}.log"}}
	catchall := &fakeUpstream{name: "catchall", tools: []string{"b"}, caps: resourcesCaps,
		templates: []string{"file:///logs/{name}.log", "file:///{any}"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"logs": logs, "catchall": catchall})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	// Overlapping template: the upstream merged first (config order) wins.
	resp, err := r.ReadResource(context.Background(), "file:///logs/app.log")
	if err != nil {
		t.Fatalf("ReadResource via template: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("rpc error: %v", resp.Error)
	}
	if logs.lastReadURI != "file:///logs/app.log" {
		t.Errorf("template owner received uri %q, want the client's verbatim", logs.lastReadURI)
	}
	if catchall.lastReadURI != "" {
		t.Errorf("second template owner was called (uri=%q), want first-match-wins", catchall.lastReadURI)
	}

	// A uri only the catchall matches routes there.
	if _, err := r.ReadResource(context.Background(), "file:///top.txt"); err != nil {
		t.Fatalf("ReadResource via catchall template: %v", err)
	}
	if catchall.lastReadURI != "file:///top.txt" {
		t.Errorf("catchall received uri %q, want file:///top.txt", catchall.lastReadURI)
	}

	// No template matches (variable would have to span a "/"): unknown.
	if _, err := r.ReadResource(context.Background(), "file:///a/b/c.log"); !errors.Is(err, ErrUnknownResource) {
		t.Errorf("uri matching no template must wrap ErrUnknownResource, got %v", err)
	}
}

// TestRegistryCompleteRefPrompt: completion/complete with ref/prompt resolves
// the owner via the prompt routing table, rewrites ref.name from the
// client-facing namespaced form back to the upstream's original, and forwards
// argument/context verbatim.
func TestRegistryCompleteRefPrompt(t *testing.T) {
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

	params := mcp.CompletionCompleteParams{
		Argument: json.RawMessage(`{"name":"style","value":"for"}`),
		Context:  json.RawMessage(`{"arguments":{"lang":"ru"}}`),
	}
	params.Ref.Type = mcp.CompletionRefPrompt
	params.Ref.Name = "web__greet"

	resp, err := r.Complete(context.Background(), params)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("rpc error: %v", resp.Error)
	}
	if docs.lastCompleteParams != nil {
		t.Errorf("docs was called but should not have been: %s", docs.lastCompleteParams)
	}
	var sent mcp.CompletionCompleteParams
	if err := json.Unmarshal(web.lastCompleteParams, &sent); err != nil {
		t.Fatalf("decode forwarded params: %v", err)
	}
	if sent.Ref.Name != "greet" {
		t.Errorf("forwarded ref.name = %q, want the upstream's original %q", sent.Ref.Name, "greet")
	}
	if string(sent.Argument) != `{"name":"style","value":"for"}` {
		t.Errorf("argument not forwarded verbatim: %s", sent.Argument)
	}
	if string(sent.Context) != `{"arguments":{"lang":"ru"}}` {
		t.Errorf("context not forwarded verbatim: %s", sent.Context)
	}
}

// TestRegistryCompleteRefResource: ref/resource resolves via the exact uri
// table or the template matcher (the same resolveResourceOwner ReadResource
// uses), forwarding the uri untouched; unknown refs and unsupported ref types
// wrap ErrUnknownCompletionRef.
func TestRegistryCompleteRefResource(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "files", Enabled: boolPtr(true)},
	}}
	files := &fakeUpstream{name: "files", tools: []string{"a"}, caps: resourcesCaps,
		resources: []string{"file:///a.txt"},
		templates: []string{"file:///logs/{name}.log"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"files": files})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	complete := func(uri string) (*mcp.Message, error) {
		var p mcp.CompletionCompleteParams
		p.Ref.Type = mcp.CompletionRefResource
		p.Ref.URI = uri
		p.Argument = json.RawMessage(`{"name":"name","value":"ap"}`)
		return r.Complete(context.Background(), p)
	}

	// Exact catalog uri.
	if _, err := complete("file:///a.txt"); err != nil {
		t.Fatalf("Complete exact uri: %v", err)
	}
	var sent mcp.CompletionCompleteParams
	if err := json.Unmarshal(files.lastCompleteParams, &sent); err != nil {
		t.Fatalf("decode forwarded params: %v", err)
	}
	if sent.Ref.URI != "file:///a.txt" {
		t.Errorf("forwarded ref.uri = %q, want verbatim", sent.Ref.URI)
	}

	// Template uri (how the spec expects resource-variable completion: the
	// client sends the TEMPLATE uri itself, which the {var} matcher also
	// accepts as a literal-free match... it does not — a template uri with
	// braces matches no template pattern, so ownership comes from the template
	// LIST match below).
	if _, err := complete("file:///logs/app.log"); err != nil {
		t.Fatalf("Complete template-matched uri: %v", err)
	}

	// Unknown uri.
	_, err := complete("ftp://nowhere")
	if !errors.Is(err, ErrUnknownCompletionRef) {
		t.Errorf("unknown resource ref must wrap ErrUnknownCompletionRef, got %v", err)
	}

	// Unsupported ref type.
	var bad mcp.CompletionCompleteParams
	bad.Ref.Type = "ref/tool"
	if _, err := r.Complete(context.Background(), bad); !errors.Is(err, ErrUnknownCompletionRef) {
		t.Errorf("unsupported ref type must wrap ErrUnknownCompletionRef, got %v", err)
	}

	// Unknown prompt ref.
	var noPrompt mcp.CompletionCompleteParams
	noPrompt.Ref.Type = mcp.CompletionRefPrompt
	noPrompt.Ref.Name = "files__nope"
	if _, err := r.Complete(context.Background(), noPrompt); !errors.Is(err, ErrUnknownCompletionRef) {
		t.Errorf("unknown prompt ref must wrap ErrUnknownCompletionRef, got %v", err)
	}
}

// TestRegistryCompleteRefResourceTemplateURI: when completing a template
// variable the client sends the TEMPLATE's own uriTemplate string as ref.uri
// (that is the uri it saw in resources/templates/list). The resolver finds the
// owner because the literal "{name}" text is itself a slash-free segment, so
// the template's compiled [^/]+ matches it — the template uri effectively
// matches its own pattern. Guarded by a test so a future "smarter" matcher
// does not silently break template-variable completion.
func TestRegistryCompleteRefResourceTemplateURI(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "files", Enabled: boolPtr(true)}}}
	files := &fakeUpstream{name: "files", tools: []string{"a"}, caps: resourcesCaps,
		templates: []string{"file:///logs/{name}.log"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"files": files})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	var p mcp.CompletionCompleteParams
	p.Ref.Type = mcp.CompletionRefResource
	p.Ref.URI = "file:///logs/{name}.log" // the template itself, as clients send it
	if _, err := r.Complete(context.Background(), p); err != nil {
		t.Fatalf("Complete with the template uri itself must resolve its owner: %v", err)
	}
	if files.lastCompleteParams == nil {
		t.Fatal("owner never received the completion")
	}
}

// panicListResourcesUpstream fails the test loudly if the resources methods
// are ever called — the probe for "no capability, no call".
type panicListResourcesUpstream struct {
	*fakeUpstream
}

func (p *panicListResourcesUpstream) ListResources(context.Context) ([]mcp.Resource, error) {
	panic("ListResources called on an upstream that never declared the resources capability")
}

func (p *panicListResourcesUpstream) ListResourceTemplates(context.Context) ([]mcp.ResourceTemplate, error) {
	panic("ListResourceTemplates called on an upstream that never declared the resources capability")
}

// TestRegistryNoListResourcesWithoutCapability: an upstream whose initialize
// declares no "resources" capability must never receive resources/list or
// resources/templates/list at all.
func TestRegistryNoListResourcesWithoutCapability(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "bare", Enabled: boolPtr(true)}}}
	bare := &panicListResourcesUpstream{
		fakeUpstream: &fakeUpstream{name: "bare", tools: []string{"a"}, caps: json.RawMessage(`{"tools":{}}`)},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) { return bare, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if got := r.Resources(); len(got) != 0 {
		t.Errorf("resource catalog = %+v, want empty", got)
	}
}

// TestRegistryListResourcesErrorDoesNotFailLaunch: an upstream that DECLARES
// the resources capability but errors on resources/list (or templates/list) is
// degraded — its tools still come up, Start does not fail. A templates-only
// failure keeps the plain resources.
func TestRegistryListResourcesErrorDoesNotFailLaunch(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "flaky", Enabled: boolPtr(true)}}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"flaky": {name: "flaky", tools: []string{"a"}, caps: resourcesCaps,
			resources: []string{"file:///x"}, resourcesErr: errors.New("resources exploded")},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start must tolerate a failing resources/list: %v", err)
	}
	defer r.Close()
	if tools := r.Tools(); len(tools) != 1 || tools[0].Name != "flaky__a" {
		t.Fatalf("tools = %+v, want flaky__a (tools must survive a resources/list failure)", tools)
	}
	if got := r.Resources(); len(got) != 0 {
		t.Errorf("resource catalog = %+v, want empty after a failed resources/list", got)
	}

	cfg2 := &config.Config{Upstreams: []config.Upstream{{Name: "half", Enabled: boolPtr(true)}}}
	r2 := newTestRegistry(t, cfg2, nil, map[string]*fakeUpstream{
		"half": {name: "half", tools: []string{"b"}, caps: resourcesCaps,
			resources: []string{"file:///y"}, templates: []string{"file:///{z}"},
			templatesErr: errors.New("templates exploded")},
	})
	if err := r2.Start(context.Background()); err != nil {
		t.Fatalf("Start must tolerate a failing templates/list: %v", err)
	}
	defer r2.Close()
	if got := resourceCatalog(r2); len(got) != 1 || got["file:///y"] != "half" {
		t.Errorf("resources = %v, want file:///y kept when only templates/list failed", got)
	}
	if tpls := r2.ResourceTemplates(); len(tpls) != 0 {
		t.Errorf("templates = %+v, want empty after a failed templates/list", tpls)
	}
}

// TestRegistryDropUpstreamClearsResources: dropping an upstream removes its
// resources, templates and routes, leaving the other upstream's intact — and
// both resources/read and template matching against the dropped upstream fail
// cleanly afterwards.
func TestRegistryDropUpstreamClearsResources(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "docs", Enabled: boolPtr(true)},
		{Name: "web", Enabled: boolPtr(true)},
	}}
	fakes := map[string]*fakeUpstream{
		"docs": {name: "docs", tools: []string{"a"}, caps: resourcesCaps,
			resources: []string{"file:///docs/readme.md"}, templates: []string{"file:///docs/{page}.md"}},
		"web": {name: "web", tools: []string{"b"}, caps: resourcesCaps,
			resources: []string{"https://example.com/page"}, templates: []string{"https://example.com/{path}"}},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	r.dropUpstream("docs")

	got := resourceCatalog(r)
	if len(got) != 1 || got["https://example.com/page"] != "web" {
		t.Fatalf("resource catalog after drop = %v, want only web's", got)
	}
	tpls := r.ResourceTemplates()
	if len(tpls) != 1 || tpls[0].Upstream != "web" {
		t.Fatalf("templates after drop = %+v, want only web's", tpls)
	}
	if _, err := r.ReadResource(context.Background(), "file:///docs/readme.md"); !errors.Is(err, ErrUnknownResource) {
		t.Error("read of a dropped upstream's exact resource should be unknown")
	}
	if _, err := r.ReadResource(context.Background(), "file:///docs/intro.md"); !errors.Is(err, ErrUnknownResource) {
		t.Error("read matching only a dropped upstream's template should be unknown")
	}
	// The survivor still routes.
	if _, err := r.ReadResource(context.Background(), "https://example.com/anything"); err != nil {
		t.Errorf("survivor's template must still match: %v", err)
	}
}
