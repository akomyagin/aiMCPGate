package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")

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

	msg, err := r.CallTool(ctx, "remote__search", json.RawMessage(`{"q":"http-routing"}`), nil)
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

// sseHTTPUpstream extends fakeHTTPUpstream with the two things the Round 13
// e2e test needs: a MUTABLE tool catalog and the optional GET SSE stream on
// the same path (as a real Streamable-HTTP server serves it), through which
// the test pushes server-initiated notifications to the gateway.
type sseHTTPUpstream struct {
	mu    sync.Mutex
	tools string // JSON array for tools/list results

	events    chan string   // event payloads to emit on the connected stream
	connected chan struct{} // closed when the first GET stream attaches
	once      sync.Once
}

func newSSEHTTPUpstream(tools string) *sseHTTPUpstream {
	return &sseHTTPUpstream{
		tools:     tools,
		events:    make(chan string, 4),
		connected: make(chan struct{}),
	}
}

func (s *sseHTTPUpstream) setTools(tools string) {
	s.mu.Lock()
	s.tools = tools
	s.mu.Unlock()
}

func (s *sseHTTPUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fl.Flush()
		s.once.Do(func() { close(s.connected) })
		for {
			select {
			case ev := <-s.events:
				fmt.Fprintf(w, "data: %s\n\n", ev)
				fl.Flush()
			case <-r.Context().Done():
				return
			}
		}
	}

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
			fmt.Sprintf(`{"protocolVersion":%q,"capabilities":{"tools":{"listChanged":true}},"serverInfo":{"name":"remote","version":"1.0.0"}}`, mcp.ProtocolVersion))))
	case mcp.MethodToolsList:
		s.mu.Lock()
		tools := s.tools
		s.mu.Unlock()
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(fmt.Sprintf(`{"tools":%s}`, tools))))
	case mcp.MethodResourceList:
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(`{"resources":[]}`)))
	default:
		_ = enc.Encode(mcp.NewError(req.ID, mcp.CodeMethodNotFound, "method not found", nil))
	}
}

// TestRegistryHTTPUpstreamRelistsOnSSEListChanged is the Round 13 e2e test:
// an HTTP upstream pushes notifications/tools/list_changed over its GET SSE
// stream, and the registry — wired to the SAME onUpstreamNotification path
// stdio uses — debounces, re-lists, and the aggregated client-facing catalog
// actually changes. Registry teardown (Close) must also stop the stream
// goroutine, or the -race run of this test leaks it.
func TestRegistryHTTPUpstreamRelistsOnSSEListChanged(t *testing.T) {
	up := newSSEHTTPUpstream(`[{"name":"alpha","description":"a","inputSchema":{"type":"object"}}]`)
	srv := httptest.NewServer(up)
	defer srv.Close()

	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "remote", URL: srv.URL, Enabled: true},
	}}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	if tools := r.Tools(); len(tools) != 1 || tools[0].Name != "remote__alpha" {
		t.Fatalf("initial catalog: %+v", tools)
	}

	// The gateway opens the stream during Start (after the handshake), but
	// asynchronously — wait for the upstream to see it before emitting.
	select {
	case <-up.connected:
	case <-time.After(5 * time.Second):
		t.Fatal("gateway never opened the GET SSE stream to the HTTP upstream")
	}

	up.setTools(`[{"name":"alpha","description":"a","inputSchema":{"type":"object"}},` +
		`{"name":"beta","description":"b","inputSchema":{"type":"object"}}]`)
	up.events <- `{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}`

	// Debounce (200ms) + re-list happen on background goroutines: poll.
	deadline := time.Now().Add(10 * time.Second)
	for {
		tools := r.Tools()
		if len(tools) == 2 {
			names := map[string]bool{}
			for _, tool := range tools {
				names[tool.Name] = true
			}
			if !names["remote__alpha"] || !names["remote__beta"] {
				t.Fatalf("re-listed catalog has wrong names: %+v", tools)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("catalog never re-listed after SSE list_changed, still: %+v", tools)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
