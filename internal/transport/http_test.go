package transport

import (
	"bytes"
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
// client-facing HTTP transport with REAL HTTP round-trips. The registry starts
// itself on the first request that needs it (Stage 17a) and is torn down via the
// returned cleanup — Close is correct for a registry that never started, so a
// test failing before its first POST cleans up fine.
//
// It deliberately exercises the handler directly rather than httpServer.Serve so
// the test needs no ephemeral-port bookkeeping; Serve's own bind/shutdown path
// is thin plumbing over this handler. Note what that costs: a handler mounted
// this way has no server→client request router (Serve's prologue subscribes it),
// which is exactly today's behaviour for these tests — the ones that need the
// router run the real Serve, in http_serverreq_test.go.
func startHTTPGateway(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "github", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME":  "github",
				"FAKE_TOOLS": "search,create_issue",
				"FAKE_ECHO":  "1",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	// No reg.Start here (Stage 17a): the HTTP transport starts the registry
	// lazily, on the first request that needs it, so that the capabilities its
	// client declares can still be declared to the upstreams. Starting it here
	// too would launch every upstream twice.

	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", hs.handleMCP)
	srv := httptest.NewServer(mux)

	return srv, func() { srv.Close(); _ = reg.Close() }
}

// postHeaders sends one JSON-RPC message to the gateway's /mcp endpoint with
// arbitrary extra headers and returns the HTTP response for the caller to
// inspect (status + body). It is the single place tests build a POST, so the
// Stage 16 session header is added the same way everywhere.
func postHeaders(t *testing.T, srv *httptest.Server, msg *mcp.Message, headers map[string]string) *http.Response {
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
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return resp
}

// post sends one JSON-RPC message with NO session header. Since Stage 16 that
// is only legal for initialize (and for the malformed-body cases, which never
// reach the session gate) — everything else must use postSession.
func post(t *testing.T, srv *httptest.Server, msg *mcp.Message) *http.Response {
	t.Helper()
	return postHeaders(t, srv, msg, nil)
}

// postSession sends one JSON-RPC message carrying the session id issued by
// initialize — the normal shape of every post-handshake HTTP request.
func postSession(t *testing.T, srv *httptest.Server, msg *mcp.Message, sid string) *http.Response {
	t.Helper()
	return postHeaders(t, srv, msg, map[string]string{sessionHeader: sid})
}

// withSession returns a copy of headers plus the session header — copy, not
// mutation, so a shared table-driven header map is never altered in place.
func withSession(headers map[string]string, sid string) map[string]string {
	out := make(map[string]string, len(headers)+1)
	for k, v := range headers {
		out[k] = v
	}
	out[sessionHeader] = sid
	return out
}

// sessionHeaders is withSession for the common case of no other headers.
func sessionHeaders(sid string) map[string]string { return withSession(nil, sid) }

// initSessionAt runs the initialize handshake against baseURL and returns the
// Mcp-Session-Id the gateway issued (Stage 16). headers carry whatever else
// the endpoint demands (Authorization, Origin). It fails the test if no id
// comes back — every later request depends on it.
func initSessionAt(t *testing.T, client *http.Client, baseURL string, headers map[string]string) string {
	t.Helper()
	body, err := mcp.Encode(mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	})))
	if err != nil {
		t.Fatalf("encode initialize: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build initialize request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("initialize POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initialize status = %d, want 200", resp.StatusCode)
	}
	sid := resp.Header.Get(sessionHeader)
	if sid == "" {
		t.Fatalf("initialize response carries no %s header", sessionHeader)
	}
	return sid
}

// initSession is initSessionAt against an httptest.Server.
func initSession(t *testing.T, srv *httptest.Server, headers map[string]string) string {
	t.Helper()
	return initSessionAt(t, srv.Client(), srv.URL, headers)
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

	sid := initSession(t, srv, nil)
	resp := postSession(t, srv, mcp.NewNotification(mcp.NotifInitialized, nil), sid)
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

	sid := initSession(t, srv, nil)
	resp := postSession(t, srv, mcp.NewRequest(mcp.IntID(2), mcp.MethodToolsList, nil), sid)
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

	sid := initSession(t, srv, nil)
	id := mcp.IntID(3)
	resp := postSession(t, srv, mcp.NewRequest(id, mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{
		Name:      "github__search",
		Arguments: json.RawMessage(`{"q":"golang"}`),
	})), sid)
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

	sid := initSession(t, srv, nil)
	resp := postSession(t, srv, mcp.NewRequest(mcp.IntID(4), "does/not/exist", nil), sid)
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
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed body HTTP status = %d, want 400", resp.StatusCode)
	}
	var msg mcp.Message
	if err := json.Unmarshal(body, &msg); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if msg.Error == nil || msg.Error.Code != mcp.CodeParseError {
		t.Fatalf("want parse error, got %+v", msg.Error)
	}
	// JSON-RPC 2.0: when the request id could not be determined the error
	// response MUST carry the literal "id":null — not omit the field.
	if !strings.Contains(string(body), `"id":null`) {
		t.Fatalf("parse-error response must contain %q, got: %s", `"id":null`, body)
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

	// A hybrid is not initialize, so it needs a session like any other POST
	// (Stage 16); the hybrid gate itself is what is under test here.
	sid := initSession(t, srv, nil)

	cases := []struct {
		name   string
		raw    string
		wantID string // raw id echoed in the error response (null when it had none)
	}{
		{"int id", `{"jsonrpc":"2.0","id":1,"method":"tools/list","result":{}}`, "1"},
		{"null id", `{"jsonrpc":"2.0","id":null,"method":"tools/list","result":{}}`, "null"},
		{"absent id", `{"jsonrpc":"2.0","method":"tools/list","result":{}}`, "null"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(tc.raw))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(sessionHeader, sid)
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
				t.Fatalf("error response id = %q, want %q (echo the hybrid's own id, null when it had none)", msg.ID, tc.wantID)
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
			{Name: "github", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME":  "github",
				"FAKE_TOOLS": "search",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	// No reg.Start here (Stage 17a): the HTTP transport starts the registry
	// lazily, on the first request that needs it, so that the capabilities its
	// client declares can still be declared to the upstreams. Starting it here
	// too would launch every upstream twice.
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

	// No header → 401. Auth runs before the session gate, so this stays 401
	// (not the 400 an unauthenticated-but-sessionless request would get if the
	// order were reversed) — that ordering is the point of these two cases.
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

	// Correct token → served: handshake, then a real request over the session
	// it issued. (tools/list with the right token but NO session would now be
	// 400 — Stage 16 — so the success case carries both credentials.)
	auth := map[string]string{"Authorization": "Bearer " + token}
	sid := initSession(t, srv, auth)
	resp = postHeaders(t, srv, msg, map[string]string{
		"Authorization": "Bearer " + token,
		sessionHeader:   sid,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("correct token: status = %d, want 200", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestHTTPServerNoAuthTokenAllowsAll(t *testing.T) {
	srv, cleanup := startHTTPGateway(t) // no token configured
	defer cleanup()

	// Any request (even without Authorization header) must succeed.
	sid := initSession(t, srv, nil)
	resp := postSession(t, srv, mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil), sid)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("no-auth gateway: status = %d, want 200", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

// TestHTTPServerMethodNotAllowed pins the method gate for everything that is
// none of the three legitimate verbs: POST (one JSON-RPC message), GET (the
// server→client SSE stream, Round 12 — exercised in http_sse_test.go) and
// DELETE (session termination, Stage 16 — exercised in
// http_session_test.go). GET was the 405 case originally and DELETE was the
// 405 case until Stage 16; PUT is the one that is still nobody's method.
func TestHTTPServerMethodNotAllowed(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	req, err := http.NewRequest(http.MethodPut, srv.URL+"/mcp", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PUT /mcp status = %d, want 405", resp.StatusCode)
	}
}

// TestHTTPServerOriginCheck verifies the DNS-rebinding defence: a request whose
// Origin header names a non-local page is rejected 403 before any dispatch,
// while requests with no Origin (regular MCP clients) or a localhost Origin
// (local browser tooling) are served as before.
func TestHTTPServerOriginCheck(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	// The payload is initialize on purpose: the Origin gate runs BEFORE the
	// Stage 16 session gate, so the accepted cases must be served 200 without
	// any session header — which is exactly what this pins.
	postWithOrigin := func(origin string) *http.Response {
		t.Helper()
		body, err := mcp.Encode(mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
			ProtocolVersion: mcp.ProtocolVersion,
			Capabilities:    json.RawMessage(`{}`),
			ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
		})))
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
//
// WriteTimeout is checked in TestBuildServerHasNoStaticWriteTimeout: it used
// to be call_timeout + slack computed once at startup, but that could never
// follow a SIGHUP reload of call_timeout, so slow-client protection moved to
// a live per-request write deadline in handlePost.
func TestHTTPServerTimeoutsConfigured(t *testing.T) {
	cfg := &config.Config{Transport: config.TransportHTTP, CallTimeout: 45 * time.Second}
	hs := newHTTPServer(cfg, registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test"), quietLogger(), "test")
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
}

// TestBuildServerHasNoStaticWriteTimeout pins the reload-aware timeout
// design: buildServer must NOT bake call_timeout into the static
// Server.WriteTimeout (it is computed once and would ignore SIGHUP reloads).
// The per-request deadline handlePost sets from the live config replaces it.
func TestBuildServerHasNoStaticWriteTimeout(t *testing.T) {
	cfg := &config.Config{Transport: config.TransportHTTP, CallTimeout: 45 * time.Second}
	hs := newHTTPServer(cfg, registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test"), quietLogger(), "test")
	srv := hs.buildServer(http.NewServeMux())

	if srv.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0 (slow-client protection is the live per-request deadline in handlePost, not a static field)", srv.WriteTimeout)
	}
}

// TestAuthMiddlewareReadsLiveConfig verifies the SIGHUP-reload fix for token
// rotation: authMiddleware must read auth_token from the config function on
// every request, so swapping the config (as Registry.Reload does behind its
// atomic pointer) immediately invalidates the old token and accepts the new
// one — without rebuilding the transport.
func TestAuthMiddlewareReadsLiveConfig(t *testing.T) {
	current := &config.Config{AuthToken: "A"}
	hs := &httpServer{
		log: quietLogger(),
		cfg: func() *config.Config { return current },
	}
	handler := hs.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	do := func(token string) int {
		req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	// Config says token A: A passes, B is rejected.
	if got := do("A"); got != http.StatusOK {
		t.Fatalf("token A before rotation: status = %d, want 200", got)
	}
	if got := do("B"); got != http.StatusUnauthorized {
		t.Fatalf("token B before rotation: status = %d, want 401", got)
	}

	// Rotate the token by swapping what cfg() returns — no server rebuild,
	// exactly what a SIGHUP reload does to the Registry's config pointer.
	current = &config.Config{AuthToken: "B"}

	if got := do("A"); got != http.StatusUnauthorized {
		t.Fatalf("token A after rotation: status = %d, want 401 (old token must stop working)", got)
	}
	if got := do("B"); got != http.StatusOK {
		t.Fatalf("token B after rotation: status = %d, want 200 (new token must work without restart)", got)
	}
}
