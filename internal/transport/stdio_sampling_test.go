package transport

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// TestStdioSamplingRoundTrip (C6) drives the full upstream→client→upstream
// cycle for sampling/createMessage: the request arrives at the client with its
// params verbatim under a gateway-minted "sampling-" id, the client's answer is
// routed back, and the upstream's tools/call completes with it embedded.
//
// This is the incident test of v0.3.0 in its STRONG form. The fakeserver
// respects capability negotiation, so it only asks when the gateway really
// declared sampling to it — which, since Stage 15, can only happen if the
// gateway derived that declaration from THIS client's initialize. A gateway
// that declares nothing (the v0.3.0 bug) or that declares from a transport
// flag alone fails here, not silently.
func TestStdioSamplingRoundTrip(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	cfg := capsUpstream(t, capsFile, map[string]string{"FAKE_SAMPLING": "1"})
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{"sampling":{}}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}

	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))

	// The next server→client message must be the proxied sampling request —
	// before the call's own reply, which is blocked on the answer.
	req := c.readResponse()
	if !req.IsRequest() || req.Method != mcp.MethodSamplingCreateMessage {
		t.Fatalf("expected sampling/createMessage request, got method=%q id=%s err=%+v result=%s (upstream skipped — was sampling declared?)",
			req.Method, req.ID, req.Error, req.Result)
	}
	var gatewayID string
	if err := json.Unmarshal(req.ID, &gatewayID); err != nil || !strings.HasPrefix(gatewayID, "sampling-") {
		t.Fatalf("sampling request id = %s, want a gateway-minted string id with the \"sampling-\" prefix (upstream id leaked?)", req.ID)
	}
	if !strings.Contains(string(req.Params), `"say hi"`) || !strings.Contains(string(req.Params), `"maxTokens"`) {
		t.Errorf("sampling params not proxied verbatim: %s", req.Params)
	}

	answer := json.RawMessage(`{"role":"assistant","content":{"type":"text","text":"hi"},"model":"test-model"}`)
	if err := c.w.Write(mcp.NewResult(req.ID, answer)); err != nil {
		t.Fatalf("client write sampling answer: %v", err)
	}

	resp := c.readResponse()
	if string(resp.ID) != string(callID) {
		t.Fatalf("reply id = %s, want the call's %s", resp.ID, callID)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "sampled=") || !strings.Contains(string(resp.Result), "hi") {
		t.Errorf("tool result did not embed the sampling answer: %s", resp.Result)
	}
}

// TestStdioClientWithoutSamplingKeepsCallWorking (C7) is the direct check of
// the motive for withdrawing the optimistic declaration: a client that declares
// elicitation but NOT sampling must see its tools/call succeed untouched. The
// gateway declares no sampling, so a negotiation-respecting upstream never asks
// (|sampling-skipped) and the -32601 that server SDKs turn into a failed call
// never happens. Declaring sampling optimistically would break exactly this.
func TestStdioClientWithoutSamplingKeepsCallWorking(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	cfg := capsUpstream(t, capsFile, map[string]string{"FAKE_SAMPLING": "1"})
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{"elicitation":{}}`), // no sampling
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}
	got := lastCaps(t, capsFile)
	if _, ok := got["sampling"]; ok {
		t.Fatalf("gateway declared sampling to the upstream for a client that never did: %v", got)
	}

	start := time.Now()
	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	resp := c.readResponse()
	elapsed := time.Since(start)

	// The FIRST frame is the call's reply: nothing was pushed to the client.
	if string(resp.ID) != string(callID) {
		t.Fatalf("first message id=%s method=%q, want the call reply %s (sampling pushed to a client that cannot sample?)",
			resp.ID, resp.Method, callID)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	// The call SUCCEEDS — no -32601 anywhere in the chain.
	if strings.Contains(string(resp.Result), "sampling-error") ||
		strings.Contains(string(resp.Result), `"isError":true`) {
		t.Fatalf("the call failed on a sampling refusal that should never have arisen: %s", resp.Result)
	}
	if !strings.Contains(string(resp.Result), "sampling-skipped") {
		t.Errorf("upstream asked for sampling (or the hook broke) despite no declaration: %s", resp.Result)
	}
	if elapsed > 5*time.Second {
		t.Errorf("call took %v — something waited instead of skipping outright", elapsed)
	}
}

// TestStdioSamplingFromRudeUpstreamRefused keeps the sampling refusal path
// covered, the way TestStdioElicitationFromRudeUpstreamDeclined does for
// elicitation: with honest declaration a negotiation-respecting upstream can
// never reach it. FAKE_SAMPLING_FORCE asks anyway; the gateway must answer
// -32601 IMMEDIATELY rather than leave the upstream hanging to its own
// timeout, and nothing may be pushed to a client that cannot sample. The
// fakeserver mirrors real SDKs by failing its tool call on that error — which
// is exactly the cost that made the optimistic declaration unaffordable.
func TestStdioSamplingFromRudeUpstreamRefused(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	cfg := capsUpstream(t, capsFile, map[string]string{"FAKE_SAMPLING_FORCE": "1"})
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`), // declares nothing
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}

	start := time.Now()
	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	resp := c.readResponse()
	elapsed := time.Since(start)

	// Nothing was pushed to the client: the first frame is the call's reply.
	if string(resp.ID) != string(callID) {
		t.Fatalf("first message id=%s method=%q, want the call reply %s (sampling pushed to an incapable client?)",
			resp.ID, resp.Method, callID)
	}
	// The refusal is a JSON-RPC -32601, NOT an elicitation-style decline
	// result: the fakeserver surfaces the code it received verbatim.
	if !strings.Contains(string(resp.Result), "sampling-error=-32601") {
		t.Errorf("upstream did not receive an immediate -32601 refusal: %s", resp.Result)
	}
	if elapsed > 5*time.Second {
		t.Errorf("refusal took %v — the upstream waited instead of being answered immediately", elapsed)
	}
}
