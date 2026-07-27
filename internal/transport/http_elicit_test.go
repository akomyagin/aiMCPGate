package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// TestHTTPGatewayDoesNotDeclareElicitationToUpstream pins the negative half of
// the truthful upstream handshake (the fix after Round 14): the HTTP
// client-facing transport has no channel for gateway→client requests, never
// calls SetElicitationProxySupported, and so the gateway must NOT declare the
// elicitation capability to its stdio upstreams — declaring would promise a
// proxy chain that does not exist. Asserted from both ends: the fakeserver's
// FAKE_CAPS_FILE records the raw capabilities the handshake actually carried,
// and — because the fakeserver respects negotiation — its armed FAKE_ELICIT
// hook answers the call directly with the distinct |elicit-skipped marker
// instead of asking. The client here even declares elicitation itself: that
// must change nothing, since the upstream handshakes completed before any
// client connected and the transport still cannot relay the request.
func TestHTTPGatewayDoesNotDeclareElicitationToUpstream(t *testing.T) {
	bin := buildFakeServer(t)
	capsFile := filepath.Join(t.TempDir(), "caps")
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":      "web",
				"FAKE_TOOLS":     "ask",
				"FAKE_ELICIT":    "1",
				"FAKE_CAPS_FILE": capsFile,
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry Start: %v", err)
	}
	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", hs.handleMCP)
	srv := httptest.NewServer(mux)
	defer func() { srv.Close(); _ = reg.Close() }()

	resp := post(t, srv, mcp.NewRequest(mcp.IntID(1), mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{"elicitation":{}}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	})))
	if msg := decodeBody(t, resp); msg.Error != nil {
		t.Fatalf("initialize error: %v", msg.Error)
	}

	callID := mcp.IntID(2)
	resp = post(t, srv, mcp.NewRequest(callID, mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"})))
	msg := decodeBody(t, resp)
	if string(msg.ID) != string(callID) {
		t.Fatalf("tools/call response id = %s, want %s", msg.ID, callID)
	}
	if msg.Error != nil {
		t.Fatalf("tools/call error: %v", msg.Error)
	}
	if !strings.Contains(string(msg.Result), "elicit-skipped") {
		t.Errorf("upstream should have skipped elicitation against an undeclaring gateway, got: %s", msg.Result)
	}

	// The direct record: the handshake's capabilities object must not carry
	// the elicitation key at all (today it is exactly {}).
	data, err := os.ReadFile(capsFile)
	if err != nil {
		t.Fatalf("read caps file: %v", err)
	}
	if strings.Contains(string(data), "elicitation") {
		t.Errorf("gateway declared elicitation to a stdio upstream over an HTTP client transport: %s", data)
	}
}
