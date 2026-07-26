package transport

// Round 5 end-to-end tests: resource aggregation, resource templates and
// completion/complete over the real stdio stack — fakeserver child processes,
// registry, dispatcher, framing. The fakeserver advertises resources via
// FAKE_RESOURCES / FAKE_RESOURCE_TEMPLATES (which also make it declare the
// resources capability in initialize) and completions via FAKE_COMPLETIONS.

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// startResourceServer brings up the gateway over three fakeserver upstreams:
// "files" with resources+templates+completions+prompts, "web" with one
// resource that COLLIDES with files' first uri, and "bare" with none of it.
func startResourceServer(t *testing.T) (*fakeClient, func()) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "files", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":               "files",
			"FAKE_TOOLS":              "read",
			"FAKE_PROMPTS":            "summarize",
			"FAKE_RESOURCES":          "file:///a.txt,file:///b.txt",
			"FAKE_RESOURCE_TEMPLATES": "file:///logs/{name}.log",
			"FAKE_COMPLETIONS":        "1",
		}},
		{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":      "web",
			"FAKE_TOOLS":     "fetch",
			"FAKE_RESOURCES": "file:///a.txt,https://example.com/page",
		}},
		{Name: "bare", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":  "bare",
			"FAKE_TOOLS": "ping",
		}},
	}}
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	return c, func() { cancel(); <-done }
}

// TestStdioResourcesCapabilityConditional: with resource/completion-capable
// upstreams in the mix the gateway advertises resources (honestly with
// subscribe/listChanged false) and completions; with none, neither capability
// appears at all.
func TestStdioResourcesCapabilityConditional(t *testing.T) {
	c, stop := startResourceServer(t)
	defer stop()
	res := c.initialize()
	if !strings.Contains(string(res.Capabilities), `"resources":{"listChanged":false,"subscribe":false}`) {
		t.Errorf("capabilities = %s, want resources with subscribe/listChanged false", res.Capabilities)
	}
	if !strings.Contains(string(res.Capabilities), `"completions":{}`) {
		t.Errorf("capabilities = %s, want completions declared", res.Capabilities)
	}
}

func TestStdioNoResourcesCapabilityWithoutResourceUpstream(t *testing.T) {
	c, cancel, done := startServer(t, false) // github upstream, no FAKE_RESOURCES
	defer func() { cancel(); <-done }()
	res := c.initialize()
	if strings.Contains(string(res.Capabilities), `"resources"`) {
		t.Errorf("capabilities = %s, want NO resources capability (no upstream declares it)", res.Capabilities)
	}
	if strings.Contains(string(res.Capabilities), `"completions"`) {
		t.Errorf("capabilities = %s, want NO completions capability", res.Capabilities)
	}
}

// TestStdioResourcesListReadAndTemplates: the aggregated resources/list
// carries every upstream's resources under their ORIGINAL uris with the
// cross-upstream collision resolved keep-first (config order);
// resources/templates/list carries the aggregated templates; resources/read
// routes by exact uri (to the collision winner) and by template match, and an
// unknown uri is Invalid params.
func TestStdioResourcesListReadAndTemplates(t *testing.T) {
	c, stop := startResourceServer(t)
	defer stop()
	c.initialize()

	// resources/list: files' two + web's non-colliding one; file:///a.txt owned
	// by files (first in config) exactly once.
	id := c.request(mcp.MethodResourceList, nil)
	resp := c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("resources/list response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error != nil {
		t.Fatalf("resources/list error: %v", resp.Error)
	}
	var list mcp.ResourceListResult
	if err := json.Unmarshal(resp.Result, &list); err != nil {
		t.Fatalf("decode resources/list result: %v", err)
	}
	var uris []string
	for _, r := range list.Resources {
		uris = append(uris, r.URI)
	}
	want := "file:///a.txt,file:///b.txt,https://example.com/page"
	if got := strings.Join(uris, ","); got != want {
		t.Fatalf("resource catalog = %q, want %q (aggregated, sorted, keep-first dedup)", got, want)
	}

	// resources/templates/list.
	c.request(mcp.MethodResourceTemplatesList, nil)
	resp = c.readResponse()
	if resp.Error != nil {
		t.Fatalf("resources/templates/list error: %v", resp.Error)
	}
	var tpls mcp.ResourceTemplatesListResult
	if err := json.Unmarshal(resp.Result, &tpls); err != nil {
		t.Fatalf("decode templates result: %v", err)
	}
	if len(tpls.ResourceTemplates) != 1 || tpls.ResourceTemplates[0].URITemplate != "file:///logs/{name}.log" {
		t.Fatalf("templates = %+v, want the one files template", tpls.ResourceTemplates)
	}

	// resources/read of the COLLIDED uri: the keep-first winner ("files")
	// serves it — the fakeserver echoes its own name in the contents.
	read := func(uri string) *mcp.Message {
		c.request(mcp.MethodResourceRead, mcp.MustParams(mcp.ResourceReadParams{URI: uri}))
		return c.readResponse()
	}
	resp = read("file:///a.txt")
	if resp.Error != nil {
		t.Fatalf("resources/read error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "read file:///a.txt on files") {
		t.Errorf("collided uri served by the wrong upstream: %s", resp.Result)
	}

	// web's own resource routes to web.
	resp = read("https://example.com/page")
	if resp.Error != nil {
		t.Fatalf("resources/read error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "on web") {
		t.Errorf("web's resource served by the wrong upstream: %s", resp.Result)
	}

	// Template match: no exact entry, the files template matches.
	resp = read("file:///logs/app.log")
	if resp.Error != nil {
		t.Fatalf("resources/read via template error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "read file:///logs/app.log on files") {
		t.Errorf("template-matched read misrouted: %s", resp.Result)
	}

	// Unknown uri: Invalid params naming only the requested uri.
	resp = read("file:///nope.txt")
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Fatalf("resources/read of an unknown uri = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}
	if !strings.Contains(resp.Error.Message, `"file:///nope.txt"`) {
		t.Errorf("error message = %q, want it to name the requested uri", resp.Error.Message)
	}

	// Missing uri: Invalid params.
	c.request(mcp.MethodResourceRead, json.RawMessage(`{}`))
	resp = c.readResponse()
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("resources/read without a uri = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}
}

// TestStdioCompletionComplete: completion/complete with ref/prompt is routed
// to the prompt's owner with ref.name rewritten back to the upstream's
// original (the fakeserver echoes the raw params it received); ref/resource is
// routed by uri/template with everything verbatim; a ref naming nothing and a
// bogus ref.type are Invalid params.
func TestStdioCompletionComplete(t *testing.T) {
	c, stop := startResourceServer(t)
	defer stop()
	c.initialize()

	// echoedParams decodes the fakeserver's completion result — a single
	// completion value carrying the RAW params the upstream received.
	echoedParams := func(result json.RawMessage) string {
		var res struct {
			Completion struct {
				Values []string `json:"values"`
			} `json:"completion"`
		}
		if err := json.Unmarshal(result, &res); err != nil || len(res.Completion.Values) != 1 {
			t.Fatalf("unexpected completion result %s (err=%v)", result, err)
		}
		return res.Completion.Values[0]
	}

	// ref/prompt with the client-facing namespaced name.
	params := json.RawMessage(`{"ref":{"type":"ref/prompt","name":"files__summarize"},"argument":{"name":"text","value":"he"}}`)
	id := c.request(mcp.MethodCompletionComplete, params)
	resp := c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("completion response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error != nil {
		t.Fatalf("completion/complete (prompt) error: %v", resp.Error)
	}
	echoed := echoedParams(resp.Result)
	if !strings.Contains(echoed, `"name":"summarize"`) {
		t.Errorf("upstream received ref.name != original: %s", echoed)
	}
	if strings.Contains(echoed, "files__summarize") {
		t.Errorf("namespaced name leaked to the upstream: %s", echoed)
	}
	if !strings.Contains(echoed, `"value":"he"`) {
		t.Errorf("argument not forwarded verbatim: %s", echoed)
	}

	// ref/resource routed via the template match, uri verbatim.
	params = json.RawMessage(`{"ref":{"type":"ref/resource","uri":"file:///logs/{name}.log"},"argument":{"name":"name","value":"ap"}}`)
	c.request(mcp.MethodCompletionComplete, params)
	resp = c.readResponse()
	if resp.Error != nil {
		t.Fatalf("completion/complete (resource) error: %v", resp.Error)
	}
	if echoed := echoedParams(resp.Result); !strings.Contains(echoed, `"uri":"file:///logs/{name}.log"`) {
		t.Errorf("resource ref not forwarded verbatim: %s", echoed)
	}

	// Unknown prompt ref → Invalid params.
	params = json.RawMessage(`{"ref":{"type":"ref/prompt","name":"files__nope"},"argument":{"name":"a","value":""}}`)
	c.request(mcp.MethodCompletionComplete, params)
	resp = c.readResponse()
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("completion for an unknown prompt = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}

	// Bogus ref.type → Invalid params.
	params = json.RawMessage(`{"ref":{"type":"ref/tool","name":"x"},"argument":{"name":"a","value":""}}`)
	c.request(mcp.MethodCompletionComplete, params)
	resp = c.readResponse()
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("completion with a bogus ref.type = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}
}
