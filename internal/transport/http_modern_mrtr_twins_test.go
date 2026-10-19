// fix/stage-19-debt p.1: direct HTTP tests for the MRTR twins —
// handlePromptsGetModern and handleResourcesReadModern (review finding 7's
// drift risk). The twins are mechanical copies of dispatchToolsCallModern
// (method constant, params field, resume identity), and before this file only
// the tools/call twin had transport-level coverage (TestMRTRSurvivesRequestScope).
// These tests pin the copy-paste delta over the real HTTP path: correct final
// results and modern decoration per method, the ErrUnknownResource mapping, and
// — the main drift guard — a retry token bound to its creating method across
// twins (TestMRTRHTTPRetryBindsMethodAcrossTwins).

package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// startModernMRTRTwinsGateway is startModernMRTRGateway with the prompts and
// resources catalog enabled on the same fakeserver upstream, so all three MRTR
// methods have a route: tool web__ask (eliciting), prompt web__p, resource
// mcp://web/doc (resources are NOT namespaced — the client URI equals the
// upstream URI, keep-first routing).
func startModernMRTRTwinsGateway(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME":      "web",
				"FAKE_TOOLS":     "ask",
				"FAKE_ECHO":      "1",
				"FAKE_ELICIT":    "1",
				"FAKE_PROMPTS":   "p",
				"FAKE_RESOURCES": "mcp://web/doc",
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

// modernMethodBody generalizes modernCallBody (which hardcodes tools/call):
// the same _meta envelope (protocolVersion + clientInfo + elicitation caps),
// but the method and the extra params are given.
func modernMethodBody(t *testing.T, id int64, method string, extra map[string]json.RawMessage) *mcp.Message {
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
	return mcp.NewRequest(mcp.IntID(id), method, mcp.MustParams(params))
}

// decodeModernReply decodes one JSON-RPC message from an HTTP response body.
func decodeModernReply(t *testing.T, resp *http.Response) mcp.Message {
	t.Helper()
	var msg mcp.Message
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	resp.Body.Close()
	return msg
}

// TestMRTRHTTPPromptsGetFinal: a modern prompts/get through the MRTR bridge
// returns the upstream's prompt as a COMPLETE result, and — prompts/get is not
// in modernCacheable — carries NO ttlMs/cacheScope. Catches a
// GetPromptModern→CallToolModern substitution (web__p does not resolve as a
// tool → error) and decoration drift on the twin.
func TestMRTRHTTPPromptsGetFinal(t *testing.T) {
	srv, cleanup := startModernMRTRTwinsGateway(t)
	defer cleanup()

	extra := map[string]json.RawMessage{
		"name":      mcp.MustParams("web__p"),
		"arguments": json.RawMessage(`{}`),
	}
	resp := postModern(t, srv, modernMethodBody(t, 1, mcp.MethodPromptsGet, extra),
		modernHeaders(mcp.ProtocolVersionModern, mcp.MethodPromptsGet, "web__p"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("prompts/get status = %d, want 200", resp.StatusCode)
	}
	msg := decodeModernReply(t, resp)
	if msg.Error != nil {
		t.Fatalf("prompts/get returned an error, want the final prompt: %+v", msg.Error)
	}
	obj := resultKeys(t, msg.Result)
	if got := string(obj["resultType"]); got != string(mcp.MustParams(mcp.ResultTypeComplete)) {
		t.Errorf("resultType = %s, want %q", got, mcp.ResultTypeComplete)
	}
	if _, ok := obj["ttlMs"]; ok {
		t.Errorf("prompts/get result carries ttlMs, but the method is not cacheable: %s", msg.Result)
	}
	if _, ok := obj["cacheScope"]; ok {
		t.Errorf("prompts/get result carries cacheScope, but the method is not cacheable: %s", msg.Result)
	}
}

// TestMRTRHTTPResourcesReadFinal: a modern resources/read through the MRTR
// bridge returns a COMPLETE result, and — resources/read IS cacheable — carries
// the gateway ttlMs/cacheScope. Catches modernCacheable/decoration drift on the
// resources twin.
func TestMRTRHTTPResourcesReadFinal(t *testing.T) {
	srv, cleanup := startModernMRTRTwinsGateway(t)
	defer cleanup()

	const uri = "mcp://web/doc"
	extra := map[string]json.RawMessage{"uri": mcp.MustParams(uri)}
	resp := postModern(t, srv, modernMethodBody(t, 1, mcp.MethodResourceRead, extra),
		modernHeaders(mcp.ProtocolVersionModern, mcp.MethodResourceRead, uri))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resources/read status = %d, want 200", resp.StatusCode)
	}
	msg := decodeModernReply(t, resp)
	if msg.Error != nil {
		t.Fatalf("resources/read returned an error, want the final contents: %+v", msg.Error)
	}
	obj := resultKeys(t, msg.Result)
	if got := string(obj["resultType"]); got != string(mcp.MustParams(mcp.ResultTypeComplete)) {
		t.Errorf("resultType = %s, want %q", got, mcp.ResultTypeComplete)
	}
	if got, want := string(obj["ttlMs"]), string(mcp.MustParams(mcp.GatewayTTLMs)); got != want {
		t.Errorf("ttlMs = %s, want %s", got, want)
	}
	if got, want := string(obj["cacheScope"]), string(mcp.MustParams(mcp.GatewayCacheScope)); got != want {
		t.Errorf("cacheScope = %s, want %s", got, want)
	}
}

// TestMRTRHTTPResourcesReadUnknownURI: a URI no upstream owns runs the
// callModern !ownerOK branch → ReadResource's ErrUnknownResource → mrtrReply
// maps it to -32602 Invalid params (HTTP 200, application-level error). Covers
// the ErrUnknownResource arm of mrtrReply at the transport level.
func TestMRTRHTTPResourcesReadUnknownURI(t *testing.T) {
	srv, cleanup := startModernMRTRTwinsGateway(t)
	defer cleanup()

	const uri = "mcp://nobody/x"
	extra := map[string]json.RawMessage{"uri": mcp.MustParams(uri)}
	resp := postModern(t, srv, modernMethodBody(t, 1, mcp.MethodResourceRead, extra),
		modernHeaders(mcp.ProtocolVersionModern, mcp.MethodResourceRead, uri))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unknown-URI resources/read status = %d, want 200", resp.StatusCode)
	}
	msg := decodeModernReply(t, resp)
	if msg.Error == nil || msg.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("unknown URI answered %+v, want a -32602 Invalid params error", msg.Error)
	}
}

// TestMRTRHTTPRetryBindsMethodAcrossTwins is the main drift guard for the
// twins' method constants and resume identities: a park created by tools/call
// must NOT be resumable through a twin handler. Each twin retry carries the
// VALID requestState of a live tools/call park but goes through prompts/get /
// resources/read → mrtrParkMatches mismatches on method → ErrUnknownRequestState
// → -32602, and the mismatch is a PEEK (not a take): the correct tools/call
// retry afterwards still resumes and completes.
//
// Mutation-acceptance (run by the main session): in handlePromptsGetModern pass
// mcp.MethodToolsCall instead of mcp.MethodPromptsGet to ResumeToolModern
// (dispatch_modern.go:219) → the prompts/get row's retry MATCHES and consumes
// the park (200 + a final result instead of -32602) → this test goes RED.
func TestMRTRHTTPRetryBindsMethodAcrossTwins(t *testing.T) {
	cases := []struct {
		name   string
		method string
		extra  map[string]json.RawMessage
	}{
		{
			"prompts/get retry of a tools/call park",
			mcp.MethodPromptsGet,
			map[string]json.RawMessage{"name": mcp.MustParams("web__ask")},
		},
		{
			"resources/read retry of a tools/call park",
			mcp.MethodResourceRead,
			map[string]json.RawMessage{"uri": mcp.MustParams("web__ask")},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Parks are single-use, so every row brings up its own gateway.
			srv, cleanup := startModernMRTRTwinsGateway(t)
			defer cleanup()

			// (a) First call: the upstream elicits → input_required + a live park.
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
			gatewayID := ""
			for id := range ir.InputRequests {
				gatewayID = id
			}
			if gatewayID == "" {
				t.Fatalf("input_required carried %d input requests, want 1", len(ir.InputRequests))
			}

			// (b) The twin retry with the VALID token: refused -32602, park untouched.
			twinExtra := map[string]json.RawMessage{
				"requestState":   mcp.MustParams(ir.RequestState),
				"inputResponses": mcp.MustParams(map[string]json.RawMessage{gatewayID: json.RawMessage(`{"action":"accept","content":{"answer":"yes"}}`)}),
			}
			for k, v := range tc.extra {
				twinExtra[k] = v
			}
			twinResp := postModern(t, srv, modernMethodBody(t, 2, tc.method, twinExtra),
				modernHeaders(mcp.ProtocolVersionModern, tc.method, "web__ask"))
			if twinResp.StatusCode != http.StatusOK {
				t.Fatalf("twin retry status = %d, want 200", twinResp.StatusCode)
			}
			twinMsg := decodeModernReply(t, twinResp)
			if twinMsg.Error == nil || twinMsg.Error.Code != mcp.CodeInvalidParams {
				t.Fatalf("twin retry answered %+v / %s, want a -32602 error (park must not resume under a foreign method)",
					twinMsg.Error, twinMsg.Result)
			}

			// (c) The mismatched retry was a peek, not a take: the CORRECT
			// tools/call retry still resumes and completes.
			retryExtra := map[string]json.RawMessage{
				"name":           mcp.MustParams("web__ask"),
				"arguments":      json.RawMessage(`{}`),
				"requestState":   mcp.MustParams(ir.RequestState),
				"inputResponses": mcp.MustParams(map[string]json.RawMessage{gatewayID: json.RawMessage(`{"action":"accept","content":{"answer":"yes"}}`)}),
			}
			finalResp := postModern(t, srv, modernCallBody(t, 3, retryExtra),
				modernHeaders(mcp.ProtocolVersionModern, mcp.MethodToolsCall, "web__ask"))
			if finalResp.StatusCode != http.StatusOK {
				t.Fatalf("correct retry status = %d, want 200", finalResp.StatusCode)
			}
			final := decodeModernReply(t, finalResp)
			if final.Error != nil {
				t.Fatalf("correct retry returned an error, want the final result: %+v", final.Error)
			}
			obj := resultKeys(t, final.Result)
			if got := string(obj["resultType"]); got != string(mcp.MustParams(mcp.ResultTypeComplete)) {
				t.Errorf("final resultType = %s, want %q", got, mcp.ResultTypeComplete)
			}
			if len(obj["content"]) == 0 {
				t.Errorf("final result had no content: %s", final.Result)
			}
		})
	}
}

// TestMRTRTwinsInvalidParams pins the twins' params-parsing branches
// (dispatch_modern.go:208–213, :231–236) at the DISPATCHER level: the HTTP
// header gate is already covered by TestModernPostHeaderValidation, and a
// non-object params cannot carry the _meta that routes a request to the modern
// branch at all — so dispatchModern is called directly with a modern meta.
func TestMRTRTwinsInvalidParams(t *testing.T) {
	d := liveDispatcher(t, "search")
	meta := mcp.RequestMeta{ProtocolVersion: mcp.ProtocolVersionModern}

	cases := []struct {
		name     string
		method   string
		params   string
		wantFrag string
	}{
		{"prompts/get without name", mcp.MethodPromptsGet, `{}`, "missing prompt name"},
		{"resources/read without uri", mcp.MethodResourceRead, `{}`, "missing uri"},
		{"prompts/get non-object params", mcp.MethodPromptsGet, `[1,2,3]`, "invalid prompts/get params"},
		{"resources/read non-object params", mcp.MethodResourceRead, `"not-an-object"`, "invalid resources/read params"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := mcp.NewRequest(mcp.IntID(1), tc.method, json.RawMessage(tc.params))
			reply := d.dispatchModern(context.Background(), req, meta)
			if reply.Error == nil {
				t.Fatalf("%s with params %s = result %s, want a -32602 error", tc.method, tc.params, reply.Result)
			}
			if reply.Error.Code != mcp.CodeInvalidParams {
				t.Errorf("error code = %d, want %d (%s)", reply.Error.Code, mcp.CodeInvalidParams, tc.method)
			}
			if !strings.Contains(reply.Error.Message, tc.wantFrag) {
				t.Errorf("error message = %q, want it to contain %q", reply.Error.Message, tc.wantFrag)
			}
		})
	}
}
