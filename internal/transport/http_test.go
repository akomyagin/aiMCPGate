package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// startHTTPGateway builds a gateway with one stdio fakeserver upstream and wires
// its httpServer.handleMCP handler into an httptest.Server, so tests drive the
// client-facing HTTP transport with REAL HTTP round-trips. The registry is
// started once up front (as Serve would) and torn down via the returned cleanup.
//
// It deliberately exercises the handler directly rather than httpServer.Serve so
// the test needs no ephemeral-port bookkeeping; Serve's own bind/shutdown path
// is thin plumbing over this handler.
func startHTTPGateway(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "github", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":  "github",
				"FAKE_TOOLS": "search,create_issue",
				"FAKE_ECHO":  "1",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry Start: %v", err)
	}

	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", hs.handleMCP)
	srv := httptest.NewServer(mux)

	return srv, func() { srv.Close(); _ = reg.Close() }
}

// post sends one JSON-RPC message to the gateway's /mcp endpoint and returns the
// HTTP response for the caller to inspect (status + body).
func post(t *testing.T, srv *httptest.Server, msg *mcp.Message) *http.Response {
	t.Helper()
	body, err := mcp.Encode(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return resp
}

// decodeBody reads and JSON-decodes a single MCP message from an HTTP response.
func decodeBody(t *testing.T, resp *http.Response) *mcp.Message {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var msg mcp.Message
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	return &msg
}

func TestHTTPServerInitialize(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	id := mcp.IntID(1)
	resp := post(t, srv, mcp.NewRequest(id, mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	})))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initialize HTTP status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	msg := decodeBody(t, resp)
	if string(msg.ID) != string(id) {
		t.Fatalf("response id = %s, want client id %s", msg.ID, id)
	}
	var res mcp.InitializeResult
	if err := json.Unmarshal(msg.Result, &res); err != nil {
		t.Fatalf("decode initialize result: %v", err)
	}
	if res.ServerInfo.Name != "aiMCPGate" || res.ServerInfo.Version != "test-1.2.3" {
		t.Errorf("serverInfo = %+v, want aiMCPGate/test-1.2.3", res.ServerInfo)
	}
}

func TestHTTPServerNotificationReturns202(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	resp := post(t, srv, mcp.NewNotification(mcp.NotifInitialized, nil))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("notification HTTP status = %d, want 202", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(bytes.TrimSpace(body)) != 0 {
		t.Errorf("notification response should have no body, got %q", body)
	}
}

func TestHTTPServerToolsList(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	resp := post(t, srv, mcp.NewRequest(mcp.IntID(2), mcp.MethodToolsList, nil))
	msg := decodeBody(t, resp)
	if msg.Error != nil {
		t.Fatalf("tools/list error: %v", msg.Error)
	}
	var res mcp.ToolsListResult
	if err := json.Unmarshal(msg.Result, &res); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}
	got := map[string]bool{}
	for _, tl := range res.Tools {
		got[tl.Name] = true
	}
	for _, want := range []string{"github__search", "github__create_issue"} {
		if !got[want] {
			t.Errorf("catalog missing %q (got %v)", want, got)
		}
	}
}

func TestHTTPServerToolsCallKeepsClientID(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	id := mcp.IntID(3)
	resp := post(t, srv, mcp.NewRequest(id, mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{
		Name:      "github__search",
		Arguments: json.RawMessage(`{"q":"golang"}`),
	})))
	msg := decodeBody(t, resp)
	if string(msg.ID) != string(id) {
		t.Fatalf("tools/call response id = %s, want client id %s (upstream id leaked?)", msg.ID, id)
	}
	if msg.Error != nil {
		t.Fatalf("tools/call error: %v", msg.Error)
	}
	if !strings.Contains(string(msg.Result), "golang") {
		t.Errorf("tools/call did not echo arguments through: %s", msg.Result)
	}
}

func TestHTTPServerUnknownMethod(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	resp := post(t, srv, mcp.NewRequest(mcp.IntID(4), "does/not/exist", nil))
	msg := decodeBody(t, resp)
	if msg.Error == nil || msg.Error.Code != mcp.CodeMethodNotFound {
		t.Fatalf("want method-not-found error, got %+v", msg.Error)
	}
}

func TestHTTPServerParseErrorReturns400(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader("{not json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	msg := decodeBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed body HTTP status = %d, want 400", resp.StatusCode)
	}
	if msg.Error == nil || msg.Error.Code != mcp.CodeParseError {
		t.Fatalf("want parse error, got %+v", msg.Error)
	}
}

// TestHTTPServerHybridRequestResponseRejected pins the malformed-hybrid case:
// a message carrying a method AND a result looks like both a request and a
// response; instead of being silently dropped (202) it must be answered with
// an explicit -32600 invalid-request error under its own id — INCLUDING when
// that id is null or absent: such a hybrid used to slip past the gate entirely
// (IsNotification looks only at the id, so it counted as a "notification" and
// was silently accepted — found by review).
func TestHTTPServerHybridRequestResponseRejected(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	cases := []struct {
		name   string
		raw    string
		wantID string // raw id echoed in the error response ("" = omitted)
	}{
		{"int id", `{"jsonrpc":"2.0","id":1,"method":"tools/list","result":{}}`, "1"},
		{"null id", `{"jsonrpc":"2.0","id":null,"method":"tools/list","result":{}}`, "null"},
		{"absent id", `{"jsonrpc":"2.0","method":"tools/list","result":{}}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(tc.raw))
			req.Header.Set("Content-Type", "application/json")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("hybrid message HTTP status = %d, want 200 with a JSON-RPC error (not a silent 202)", resp.StatusCode)
			}
			msg := decodeBody(t, resp)
			if msg.Error == nil || msg.Error.Code != mcp.CodeInvalidRequest {
				t.Fatalf("want invalid-request error (-32600), got %+v", msg.Error)
			}
			if string(msg.ID) != tc.wantID {
				t.Fatalf("error response id = %q, want %q (echo the hybrid's own id, null/omitted when it had none)", msg.ID, tc.wantID)
			}
		})
	}
}

// startHTTPGatewayWithAuth is like startHTTPGateway but configures an auth token.
func startHTTPGatewayWithAuth(t *testing.T, token string) (*httptest.Server, func()) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		AuthToken: token,
		Upstreams: []config.Upstream{
			{Name: "github", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":  "github",
				"FAKE_TOOLS": "search",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry Start: %v", err)
	}
	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	mux := http.NewServeMux()
	mux.Handle("/mcp", hs.authMiddleware(http.HandlerFunc(hs.handleMCP)))
	srv := httptest.NewServer(mux)
	return srv, func() { srv.Close(); _ = reg.Close() }
}

// postWithAuth sends one JSON-RPC message with an Authorization header.
func postWithAuth(t *testing.T, srv *httptest.Server, msg *mcp.Message, token string) *http.Response {
	t.Helper()
	body, err := mcp.Encode(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return resp
}

func TestHTTPServerAuthTokenRequired(t *testing.T) {
	const token = "test-secret-token"
	srv, cleanup := startHTTPGatewayWithAuth(t, token)
	defer cleanup()

	msg := mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil)

	// No header → 401.
	resp := postWithAuth(t, srv, msg, "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no auth header: status = %d, want 401", resp.StatusCode)
	}

	// Wrong token → 401.
	resp = postWithAuth(t, srv, msg, "wrong-token")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d, want 401", resp.StatusCode)
	}

	// Correct token → 200.
	resp = postWithAuth(t, srv, msg, token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("correct token: status = %d, want 200", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestHTTPServerNoAuthTokenAllowsAll(t *testing.T) {
	srv, cleanup := startHTTPGateway(t) // no token configured
	defer cleanup()

	// Any request (even without Authorization header) must succeed.
	resp := post(t, srv, mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("no-auth gateway: status = %d, want 200", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestHTTPServerGETNotAllowed(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	resp, err := srv.Client().Get(srv.URL + "/mcp")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /mcp status = %d, want 405 (no SSE server-stream in MVP)", resp.StatusCode)
	}
}

// TestHTTPServerOriginCheck verifies the DNS-rebinding defence: a request whose
// Origin header names a non-local page is rejected 403 before any dispatch,
// while requests with no Origin (regular MCP clients) or a localhost Origin
// (local browser tooling) are served as before.
func TestHTTPServerOriginCheck(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	postWithOrigin := func(origin string) *http.Response {
		t.Helper()
		body, err := mcp.Encode(mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil))
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		return resp
	}

	// Foreign origins → 403, before any JSON-RPC handling.
	for _, origin := range []string{"http://evil.example.com", "https://evil.example.com:8080", "null"} {
		resp := postWithOrigin(origin)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("Origin %q: status = %d, want 403", origin, resp.StatusCode)
		}
	}

	// No Origin (non-browser MCP client) and localhost origins → served
	// normally. 127.0.0.2 is loopback too (127.0.0.0/8): originAllowed shares
	// config.IsLoopbackHost with the listen_addr validation, so the whole
	// range is accepted, not just the literal 127.0.0.1.
	for _, origin := range []string{"", "http://localhost:3000", "http://127.0.0.1:8080", "https://localhost", "http://127.0.0.2:3000"} {
		resp := postWithOrigin(origin)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Origin %q: status = %d, want 200", origin, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}
}

// TestHTTPServerTimeoutsConfigured is a regression test: the *http.Server
// used to set only ReadHeaderTimeout, leaving the body-read phase and idle
// keep-alive connections unbounded — a slow-body/slowloris-style DoS vector
// once listen_addr is widened past loopback (found by code review).
func TestHTTPServerTimeoutsConfigured(t *testing.T) {
	cfg := &config.Config{Transport: config.TransportHTTP, CallTimeout: 45 * time.Second}
	hs := newHTTPServer(cfg, registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true), quietLogger(), "test")
	srv := hs.buildServer(http.NewServeMux())

	if srv.ReadHeaderTimeout <= 0 {
		t.Error("ReadHeaderTimeout must be set")
	}
	if srv.ReadTimeout <= 0 {
		t.Error("ReadTimeout must be set")
	}
	if srv.IdleTimeout <= 0 {
		t.Error("IdleTimeout must be set")
	}
	// WriteTimeout covers the whole handler (net/http docs), not just the
	// network write, so it must comfortably exceed the configured
	// call_timeout — otherwise a legitimate slow upstream call gets cut off
	// before EffectiveCallTimeout ever has a chance to fire.
	wantWrite := cfg.CallTimeout + httpWriteTimeoutSlack
	if srv.WriteTimeout != wantWrite {
		t.Errorf("WriteTimeout = %v, want %v (call_timeout + slack)", srv.WriteTimeout, wantWrite)
	}
	if srv.WriteTimeout <= cfg.CallTimeout {
		t.Errorf("WriteTimeout (%v) must exceed call_timeout (%v), or legitimate slow calls get cut off first", srv.WriteTimeout, cfg.CallTimeout)
	}
}
