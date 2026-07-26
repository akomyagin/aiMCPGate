package transport

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// elicitTestConfig builds the single-upstream config the elicitation e2e tests
// share: one fakeserver whose every tools/call first sends an
// elicitation/create and waits for the answer (FAKE_ELICIT).
func elicitTestConfig(t *testing.T) *config.Config {
	t.Helper()
	bin := buildFakeServer(t)
	return &config.Config{Upstreams: []config.Upstream{
		{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":   "web",
			"FAKE_TOOLS":  "ask",
			"FAKE_ELICIT": "1",
		}},
	}}
}

// TestStdioElicitationRoundTrip (Round 14) drives the full upstream→client→
// upstream cycle: the upstream's elicitation/create arrives at the client with
// its params verbatim but under a gateway-minted string id (never the
// upstream's own); the client's answer is routed back to the upstream, whose
// tools/call then completes with the answer embedded in its result.
func TestStdioElicitationRoundTrip(t *testing.T) {
	c, cancel, done := startServerWithConfig(t, elicitTestConfig(t), nil)
	defer func() { cancel(); <-done }()

	// The client DECLARES the elicitation capability — presence of the key is
	// what the gateway must detect, even as an empty object.
	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{"elicitation":{}}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}

	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))

	// The next server→client message must be the proxied elicitation/create
	// REQUEST — before the call's own reply, which is blocked on the answer.
	elicitReq := c.readResponse()
	if !elicitReq.IsRequest() || elicitReq.Method != mcp.MethodElicitationCreate {
		t.Fatalf("expected elicitation/create request, got method=%q id=%s err=%+v",
			elicitReq.Method, elicitReq.ID, elicitReq.Error)
	}
	var gatewayID string
	if err := json.Unmarshal(elicitReq.ID, &gatewayID); err != nil || !strings.HasPrefix(gatewayID, "elicit-") {
		t.Fatalf("elicitation request id = %s, want gateway-minted string id with \"elicit-\" prefix (upstream id leaked?)", elicitReq.ID)
	}
	// Params travel verbatim: the fakeserver's fixed message and schema.
	if !strings.Contains(string(elicitReq.Params), `"need input"`) ||
		!strings.Contains(string(elicitReq.Params), `"requestedSchema"`) {
		t.Errorf("elicitation params not proxied verbatim: %s", elicitReq.Params)
	}

	// Answer as the human operator would, echoing the gateway's id.
	answer := json.RawMessage(`{"action":"accept","content":{"answer":"42"}}`)
	if err := c.w.Write(mcp.NewResult(elicitReq.ID, answer)); err != nil {
		t.Fatalf("client write elicitation answer: %v", err)
	}

	// The tools/call now completes, its result embedding the answer the
	// upstream received (fakeserver appends "|elicited=<raw result>").
	resp := c.readResponse()
	if string(resp.ID) != string(callID) {
		t.Fatalf("reply id = %s, want the call's %s", resp.ID, callID)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "elicited=") ||
		!strings.Contains(string(resp.Result), "42") {
		t.Errorf("tool result did not embed the elicitation answer: %s", resp.Result)
	}
}

// TestStdioElicitationRefusedWithoutClientCapability (Round 14): when the
// client's initialize did NOT declare the elicitation capability, the gateway
// must answer the upstream's elicitation/create immediately with a JSON-RPC
// method-not-found error — the upstream must not hang until its own timeout,
// and nothing may be pushed to the client. The fakeserver embeds the error it
// received into the tool result ("|elicit-error=<code> <message>"), so the
// call completing promptly with that marker proves both halves.
func TestStdioElicitationRefusedWithoutClientCapability(t *testing.T) {
	c, cancel, done := startServerWithConfig(t, elicitTestConfig(t), nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`), // no elicitation key
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}

	start := time.Now()
	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	resp := c.readResponse()
	elapsed := time.Since(start)

	// The FIRST message must be the call's reply — no elicitation/create may
	// have been pushed to a client that cannot answer it.
	if string(resp.ID) != string(callID) {
		t.Fatalf("first message id=%s method=%q, want the call reply %s (elicitation pushed to an incapable client?)",
			resp.ID, resp.Method, callID)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "elicit-error=-32601") {
		t.Errorf("upstream did not receive an immediate method-not-found refusal: %s", resp.Result)
	}
	// Well before the fakeserver's 10s elicitation timeout: the refusal was
	// immediate, not a hang.
	if elapsed > 5*time.Second {
		t.Errorf("refusal took %v — the upstream waited instead of being answered immediately", elapsed)
	}
}

// TestClientSupportsElicitation covers the tolerant capability sniffing on the
// raw initialize params (Round 14): only the PRESENCE of the "elicitation" key
// counts, and any malformed shape degrades to false, never an error.
func TestClientSupportsElicitation(t *testing.T) {
	tests := []struct {
		name   string
		params string // "" means absent params
		want   bool
	}{
		{"declared empty object", `{"capabilities":{"elicitation":{}}}`, true},
		{"declared with content", `{"capabilities":{"elicitation":{"form":true},"roots":{}}}`, true},
		{"other capabilities only", `{"capabilities":{"roots":{},"sampling":{}}}`, false},
		{"empty capabilities", `{"capabilities":{}}`, false},
		{"no capabilities field", `{"clientInfo":{"name":"x","version":"1"}}`, false},
		{"absent params", "", false},
		{"malformed params", `[42]`, false},
		{"capabilities not an object", `{"capabilities":["elicitation"]}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var params json.RawMessage
			if tt.params != "" {
				params = json.RawMessage(tt.params)
			}
			if got := clientSupportsElicitation(params); got != tt.want {
				t.Errorf("clientSupportsElicitation(%s) = %v, want %v", tt.params, got, tt.want)
			}
		})
	}
}
