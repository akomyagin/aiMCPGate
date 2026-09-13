// Stage 19c: the MRTR bridge over HTTP. A modern (2026-07-28) client POSTs a
// tools/call whose upstream elicits mid-call; the gateway answers the POST with
// an InputRequiredResult (never a counter-request), and a follow-up POST
// carrying inputResponses + requestState resumes the parked call. The point
// these tests defend beyond the registry unit tests is REQUEST SCOPE: the first
// POST's http.Request context is cancelled the moment its response is written,
// yet the parked CallTool — running on the registry's process context — is
// still alive to accept the retry.

package transport

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// startModernMRTRGateway brings up an HTTP gateway whose one upstream elicits on
// every tools/call (FAKE_ELICIT) — the counter-request the MRTR bridge converts.
func startModernMRTRGateway(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME":   "web",
				"FAKE_TOOLS":  "ask",
				"FAKE_ECHO":   "1",
				"FAKE_ELICIT": "1",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", hs.handleMCP)
	srv := httptest.NewServer(mux)
	return srv, func() { srv.Close(); _ = reg.Close() }
}

// elicitCaps is the raw _meta.clientCapabilities a modern client declaring
// elicitation sends.
const elicitCaps = `{"elicitation":{}}`

// modernCallBody builds a modern tools/call body carrying elicitation caps and
// the given extra params (name/arguments, plus MRTR retry fields).
func modernCallBody(t *testing.T, id int64, extra map[string]json.RawMessage) *mcp.Message {
	t.Helper()
	obj := map[string]json.RawMessage{
		mcp.MetaProtocolVersion:    mcp.MustParams(mcp.ProtocolVersionModern),
		mcp.MetaClientInfo:         mcp.MustParams(mcp.Implementation{Name: "modern-client", Version: "1.0"}),
		mcp.MetaClientCapabilities: json.RawMessage(elicitCaps),
	}
	params := map[string]json.RawMessage{"_meta": mcp.MustParams(obj)}
	for k, v := range extra {
		params[k] = v
	}
	return mcp.NewRequest(mcp.IntID(id), mcp.MethodToolsCall, mcp.MustParams(params))
}

// decodeInputRequired decodes an HTTP response as an InputRequiredResult,
// failing if it is not one (an error, or a plain complete result).
func decodeInputRequired(t *testing.T, resp *http.Response) mcp.InputRequiredResult {
	t.Helper()
	var msg mcp.Message
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	resp.Body.Close()
	if msg.Error != nil {
		t.Fatalf("response carried an error, want input_required: %+v", msg.Error)
	}
	var out mcp.InputRequiredResult
	if err := json.Unmarshal(msg.Result, &out); err != nil {
		t.Fatalf("result is not an InputRequiredResult: %v (%s)", err, msg.Result)
	}
	if out.ResultType != mcp.ResultTypeInputRequired {
		t.Fatalf("resultType = %q, want %q (result: %s)", out.ResultType, mcp.ResultTypeInputRequired, msg.Result)
	}
	return out
}

// TestMRTRSurvivesRequestScope is the sharp test: the first POST returns
// input_required and its request context is DONE, but the parked CallTool lives
// on the registry's process context, so the retry POST completes the round trip.
// A mutation running the parked CallTool on the request context (rather than
// procCtx) makes the upstream's tool call abort the instant the first POST
// returns, and the retry then finds no parked call → this test goes red.
func TestMRTRSurvivesRequestScope(t *testing.T) {
	srv, cleanup := startModernMRTRGateway(t)
	defer cleanup()

	// First call: the upstream elicits, so the gateway answers input_required.
	callExtra := map[string]json.RawMessage{
		"name":      mcp.MustParams("web__ask"),
		"arguments": json.RawMessage(`{}`),
	}
	resp := postModern(t, srv, modernCallBody(t, 1, callExtra),
		modernHeaders(mcp.ProtocolVersionModern, mcp.MethodToolsCall, "web__ask"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first POST status = %d, want 200", resp.StatusCode)
	}
	ir := decodeInputRequired(t, resp)
	if ir.RequestState == "" {
		t.Fatal("input_required carried an empty requestState")
	}
	if len(ir.InputRequests) != 1 {
		t.Fatalf("got %d input requests, want 1", len(ir.InputRequests))
	}
	var gatewayID string
	var env mcp.InputRequestEnvelope
	for id, raw := range ir.InputRequests {
		gatewayID = id
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("input request %q not an envelope: %v", id, err)
		}
	}
	if env.Method != mcp.MethodElicitationCreate {
		t.Errorf("envelope method = %q, want elicitation/create", env.Method)
	}
	// At this point the first HTTP request is fully served and its context is
	// done. The parked CallTool must still be alive.

	// Retry: answer the elicitation. requestState + inputResponses ride the same
	// params._meta modern envelope; the answer is keyed by the gateway id.
	retryExtra := map[string]json.RawMessage{
		"name":           mcp.MustParams("web__ask"),
		"arguments":      json.RawMessage(`{}`),
		"requestState":   mcp.MustParams(ir.RequestState),
		"inputResponses": mcp.MustParams(map[string]json.RawMessage{gatewayID: json.RawMessage(`{"action":"accept","content":{"answer":"yes"}}`)}),
	}
	resp2 := postModern(t, srv, modernCallBody(t, 2, retryExtra),
		modernHeaders(mcp.ProtocolVersionModern, mcp.MethodToolsCall, "web__ask"))
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("retry POST status = %d, want 200", resp2.StatusCode)
	}
	var final mcp.Message
	if err := json.NewDecoder(resp2.Body).Decode(&final); err != nil {
		t.Fatalf("decode retry response: %v", err)
	}
	resp2.Body.Close()
	if final.Error != nil {
		t.Fatalf("retry returned an error, want the final tool result: %+v", final.Error)
	}
	// The final result carries the tool text with the elicited marker, plus the
	// modern resultType decoration.
	var res struct {
		ResultType string          `json:"resultType"`
		Content    json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(final.Result, &res); err != nil {
		t.Fatalf("final result not an object: %v (%s)", err, final.Result)
	}
	if res.ResultType != mcp.ResultTypeComplete {
		t.Errorf("final resultType = %q, want %q", res.ResultType, mcp.ResultTypeComplete)
	}
	if len(res.Content) == 0 {
		t.Errorf("final result had no content: %s", final.Result)
	}
}

// TestMRTRHTTPUnknownState: a retry whose requestState matches no parked call is
// answered -32602 Invalid params (HTTP 200 — an application-level error on this
// transport), and no upstream is disturbed.
func TestMRTRHTTPUnknownState(t *testing.T) {
	srv, cleanup := startModernMRTRGateway(t)
	defer cleanup()

	retryExtra := map[string]json.RawMessage{
		"name":           mcp.MustParams("web__ask"),
		"arguments":      json.RawMessage(`{}`),
		"requestState":   mcp.MustParams("not-a-real-token"),
		"inputResponses": mcp.MustParams(map[string]json.RawMessage{"mrtr-1": json.RawMessage(`{"action":"accept"}`)}),
	}
	resp := postModern(t, srv, modernCallBody(t, 9, retryExtra),
		modernHeaders(mcp.ProtocolVersionModern, mcp.MethodToolsCall, "web__ask"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unknown-state retry status = %d, want 200", resp.StatusCode)
	}
	var msg mcp.Message
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if msg.Error == nil || msg.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("unknown state answered %+v, want a -32602 Invalid params error", msg.Error)
	}
}
