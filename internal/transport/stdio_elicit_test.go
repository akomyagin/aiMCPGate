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
		{Name: "web", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
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

// TestStdioElicitationNotAskedWithoutClientCapability (Round 14 test,
// REWRITTEN in Stage 15). It used to assert that a client without the
// elicitation capability makes the gateway answer the upstream's
// elicitation/create with {"action":"decline"}. That assertion became
// physically unreachable: since Stage 15 the gateway declares only what its
// client actually declared, so a client with no elicitation capability means
// the upstream's handshake carries no elicitation either — and a
// negotiation-respecting server (which the fakeserver is, like every
// spec-abiding one) never asks at all. The stronger property is asserted
// instead: no request is made, the call succeeds immediately, and nothing is
// pushed to the client. The decline path itself did not disappear; it is
// covered against a deliberately rude upstream in
// TestStdioElicitationFromRudeUpstreamDeclined (stdio_caps_test.go).
func TestStdioElicitationNotAskedWithoutClientCapability(t *testing.T) {
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
	if strings.Contains(string(resp.Result), "elicit-error") ||
		strings.Contains(string(resp.Result), `"isError":true`) {
		t.Fatalf("the call failed instead of proceeding without elicitation: %s", resp.Result)
	}
	// The upstream honoured the negotiation: its distinct marker, not an
	// answered elicitation and not a timeout.
	if !strings.Contains(string(resp.Result), "elicit-skipped") {
		t.Errorf("upstream asked (or the hook broke) despite an undeclared capability: %s", resp.Result)
	}
	// Well before the fakeserver's 10s elicitation timeout: nothing waited.
	if elapsed > 5*time.Second {
		t.Errorf("call took %v — something waited instead of skipping outright", elapsed)
	}
}

// TestClientDeclaresCapability covers the tolerant capability sniffing on the
// raw initialize params (Round 14, generalized in Stage 15 to all three
// server→client capabilities): only the PRESENCE of the key counts, and any
// malformed shape degrades to false, never an error.
func TestClientDeclaresCapability(t *testing.T) {
	tests := []struct {
		name   string
		params string // "" means absent params
		cap    string
		want   bool
	}{
		{"declared empty object", `{"capabilities":{"elicitation":{}}}`, "elicitation", true},
		{"declared with content", `{"capabilities":{"elicitation":{"form":true},"roots":{}}}`, "elicitation", true},
		{"other capabilities only", `{"capabilities":{"roots":{},"sampling":{}}}`, "elicitation", false},
		{"sampling declared", `{"capabilities":{"sampling":{}}}`, "sampling", true},
		{"sampling absent", `{"capabilities":{"elicitation":{}}}`, "sampling", false},
		{"roots declared with listChanged", `{"capabilities":{"roots":{"listChanged":true}}}`, "roots", true},
		{"roots absent", `{"capabilities":{"sampling":{}}}`, "roots", false},
		{"empty capabilities", `{"capabilities":{}}`, "elicitation", false},
		{"no capabilities field", `{"clientInfo":{"name":"x","version":"1"}}`, "elicitation", false},
		{"absent params", "", "elicitation", false},
		{"malformed params", `[42]`, "sampling", false},
		{"capabilities not an object", `{"capabilities":["elicitation"]}`, "elicitation", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var params json.RawMessage
			if tt.params != "" {
				params = json.RawMessage(tt.params)
			}
			if got := clientDeclaresCapability(params, tt.cap); got != tt.want {
				t.Errorf("clientDeclaresCapability(%s, %q) = %v, want %v", tt.params, tt.cap, got, tt.want)
			}
		})
	}
}

// TestClientServerRequestCapsCollectsWholeSet pins the whole-set helper the
// stdio transport feeds the registry with: exactly the declared names, nothing
// invented, an unreadable initialize declaring nothing at all — and each name's
// RAW VALUE carried through untouched, because the registry renders its own
// upstream declaration from it (roots' listChanged must be mirrored, not
// asserted — found by review).
func TestClientServerRequestCapsCollectsWholeSet(t *testing.T) {
	tests := []struct {
		name   string
		params string
		want   map[string]string // capability → the raw value it must carry
	}{
		{"all three", `{"capabilities":{"elicitation":{},"sampling":{},"roots":{"listChanged":true}}}`,
			map[string]string{"elicitation": "{}", "sampling": "{}", "roots": `{"listChanged":true}`}},
		{"only elicitation", `{"capabilities":{"elicitation":{}}}`,
			map[string]string{"elicitation": "{}"}},
		{"roots without listChanged keeps its bare value", `{"capabilities":{"roots":{}}}`,
			map[string]string{"roots": "{}"}},
		{"unknown capability ignored", `{"capabilities":{"experimental":{}}}`, nil},
		{"malformed params", `[42]`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clientServerRequestCaps(json.RawMessage(tt.params))
			if len(got) != len(tt.want) {
				t.Fatalf("caps = %v, want exactly %v", got, tt.want)
			}
			for name, want := range tt.want {
				value, ok := got[name]
				if !ok {
					t.Errorf("caps missing %q: %v", name, got)
					continue
				}
				if string(value) != want {
					t.Errorf("caps[%q] = %s, want the client's own %s", name, value, want)
				}
			}
		})
	}
}
