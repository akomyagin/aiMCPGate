package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// fakeHTTPUpstream is a minimal Streamable-HTTP MCP server for the registry
// integration test: it proves the REAL startHTTP path (default r.start dispatch
// on ResolveKind == http) launches, handshakes, aggregates, and routes a call
// to an HTTP upstream — the HTTP analogue of the stdio regression test in
// registry_stdio_test.go.
func fakeHTTPUpstream() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Message
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.IsNotification() {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		switch req.Method {
		case mcp.MethodInitialize:
			_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(
				fmt.Sprintf(`{"protocolVersion":%q,"capabilities":{"tools":{}},"serverInfo":{"name":"remote","version":"1.0.0"}}`, mcp.ProtocolVersion))))
		case mcp.MethodToolsList:
			_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(
				`{"tools":[{"name":"search","description":"remote search","inputSchema":{"type":"object"}}]}`)))
		case mcp.MethodResourceList:
			_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(`{"resources":[]}`)))
		case mcp.MethodToolsCall:
			var p mcp.ToolsCallParams
			_ = json.Unmarshal(req.Params, &p)
			b, _ := json.Marshal(string(p.Arguments))
			_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(
				fmt.Sprintf(`{"content":[{"type":"text","text":%s}],"isError":false}`, b))))
		default:
			_ = enc.Encode(mcp.NewError(req.ID, mcp.CodeMethodNotFound, "method not found", nil))
		}
	})
}

func TestRegistryAggregatesHTTPUpstream(t *testing.T) {
	srv := httptest.NewServer(fakeHTTPUpstream())
	defer srv.Close()

	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "remote", URL: srv.URL, Enabled: true}, // kind inferred http from url
	}}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	tools := r.Tools()
	if len(tools) != 1 || tools[0].Name != "remote__search" {
		t.Fatalf("unexpected catalog from HTTP upstream: %+v", tools)
	}

	msg, err := r.CallTool(ctx, "remote__search", json.RawMessage(`{"q":"http-routing"}`))
	if err != nil {
		t.Fatalf("CallTool to HTTP upstream: %v", err)
	}
	if msg.Error != nil {
		t.Fatalf("HTTP upstream returned error: %+v", msg.Error)
	}
	if !strings.Contains(string(msg.Result), "http-routing") {
		t.Errorf("call to HTTP upstream did not route arguments through: %s", msg.Result)
	}
}
