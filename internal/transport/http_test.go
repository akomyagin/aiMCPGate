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
	reg := registry.New(cfg, quietLogger(), nil)
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
