package transport

// Round 4 end-to-end tests: prompt aggregation over the real stdio stack —
// fakeserver child processes, registry, dispatcher, framing. The fakeserver
// advertises prompts via FAKE_PROMPTS (which also makes it declare the
// prompts capability in initialize).

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// startPromptServer brings up the gateway over two fakeserver upstreams: "docs"
// with prompts (and the capability), "bare" without.
func startPromptServer(t *testing.T) (*fakeClient, context.CancelFunc, <-chan error) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "docs", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":    "docs",
			"FAKE_TOOLS":   "search",
			"FAKE_PROMPTS": "summarize,review",
		}},
		{Name: "bare", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":  "bare",
			"FAKE_TOOLS": "fetch",
		}},
	}}
	return startServerWithConfig(t, cfg, nil)
}

// initialize runs the client handshake and returns the decoded result.
func (c *fakeClient) initialize() mcp.InitializeResult {
	c.t.Helper()
	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	resp := c.readResponse()
	if resp.Error != nil {
		c.t.Fatalf("initialize returned error: %v", resp.Error)
	}
	var res mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		c.t.Fatalf("decode initialize result: %v", err)
	}
	return res
}

// TestStdioPromptsCapabilityConditional: with a prompt-capable upstream in the
// mix the gateway advertises prompts (honestly with listChanged:false); with
// none, no prompts capability appears at all.
func TestStdioPromptsCapabilityConditional(t *testing.T) {
	c, cancel, done := startPromptServer(t)
	defer func() { cancel(); <-done }()
	res := c.initialize()
	if !strings.Contains(string(res.Capabilities), `"prompts":{"listChanged":false}`) {
		t.Errorf("capabilities = %s, want prompts.listChanged:false declared", res.Capabilities)
	}
}

func TestStdioNoPromptsCapabilityWithoutPromptUpstream(t *testing.T) {
	c, cancel, done := startServer(t, false) // github upstream, no FAKE_PROMPTS
	defer func() { cancel(); <-done }()
	res := c.initialize()
	if strings.Contains(string(res.Capabilities), `"prompts"`) {
		t.Errorf("capabilities = %s, want NO prompts capability (no upstream declares it)", res.Capabilities)
	}
}

// TestStdioPromptsListAndGet: the aggregated prompts/list carries the
// namespaced catalog (only from the declaring upstream), and prompts/get is
// routed to the owner with the name rewritten back to the original and the
// arguments forwarded verbatim — the upstream's result comes back under the
// client's id.
func TestStdioPromptsListAndGet(t *testing.T) {
	c, cancel, done := startPromptServer(t)
	defer func() { cancel(); <-done }()
	c.initialize()

	id := c.request(mcp.MethodPromptsList, nil)
	resp := c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("prompts/list response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error != nil {
		t.Fatalf("prompts/list error: %v", resp.Error)
	}
	var list mcp.PromptsListResult
	if err := json.Unmarshal(resp.Result, &list); err != nil {
		t.Fatalf("decode prompts/list result: %v", err)
	}
	var names []string
	for _, p := range list.Prompts {
		names = append(names, p.Name)
	}
	if got, want := strings.Join(names, ","), "docs__review,docs__summarize"; got != want {
		t.Fatalf("prompt catalog = %q, want %q (namespaced, sorted, declaring upstream only)", got, want)
	}

	args := json.RawMessage(`{"text":"hello"}`)
	id = c.request(mcp.MethodPromptsGet, mcp.MustParams(mcp.PromptsGetParams{Name: "docs__summarize", Arguments: args}))
	resp = c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("prompts/get response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error != nil {
		t.Fatalf("prompts/get error: %v", resp.Error)
	}
	// The fakeserver echoes the received name and raw arguments: proves the
	// namespace was rewritten to the original and the arguments passed verbatim.
	if !strings.Contains(string(resp.Result), `prompt summarize|args={\"text\":\"hello\"}`) &&
		!strings.Contains(string(resp.Result), `prompt summarize|args={"text":"hello"}`) {
		t.Errorf("prompts/get result = %s, want the original name and verbatim args echoed", resp.Result)
	}

	// Unknown prompt: a clean JSON-RPC error naming only the requested prompt.
	c.request(mcp.MethodPromptsGet, mcp.MustParams(mcp.PromptsGetParams{Name: "docs__nope"}))
	resp = c.readResponse()
	if resp.Error == nil {
		t.Fatal("prompts/get for an unknown prompt must error")
	}
	if !strings.Contains(resp.Error.Message, `"docs__nope"`) {
		t.Errorf("error message = %q, want it to name the requested prompt", resp.Error.Message)
	}

	// Missing name: invalid params.
	c.request(mcp.MethodPromptsGet, json.RawMessage(`{}`))
	resp = c.readResponse()
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("prompts/get without a name = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}
}
