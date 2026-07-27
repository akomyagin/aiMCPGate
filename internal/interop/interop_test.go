// Package interop holds black-box interoperability tests: the gateway (and the
// demo stub server) driven by the OFFICIAL MCP Go SDK acting as the client, so
// aiMCPGate's hand-rolled JSON-RPC layer (internal/mcp, docs/MCP_NOTES.md §1)
// is checked against an independent protocol implementation, not just against
// itself.
//
// The SDK dependency lives ONLY in this test package — no prod package
// (internal/mcp, internal/registry, internal/transport, ...) imports it; the
// gateway stays a thin hand-rolled proxy by design.
//
// Everything runs in-process: no built binary, no exec.Command. The gateway's
// upstream is an httptest server speaking Streamable HTTP (same pattern as
// internal/registry/registry_http_test.go), the gateway itself is served by
// transport.NewServer on a loopback port, and the stdio-framing test wires the
// SDK client to internal/demo's echo server over io.Pipe.
package interop

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/demo"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
	"github.com/akomyagin/aiMCPGate/internal/transport"
)

const gatewayVersion = "0.0.0-interop"

// echoUpstream is a minimal in-process Streamable-HTTP MCP upstream exposing
// the same two tools as the __demo-echo stub (echo, ping). It exists so the
// gateway has a real upstream to aggregate without launching any subprocess
// (the demo stub itself speaks stdio, which would need os/exec here).
func echoUpstream() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Message
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.IsNotification() {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		text := func(s string) json.RawMessage {
			b, _ := json.Marshal(s)
			return json.RawMessage(fmt.Sprintf(`{"content":[{"type":"text","text":%s}],"isError":false}`, b))
		}
		switch req.Method {
		case mcp.MethodInitialize:
			_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(fmt.Sprintf(
				`{"protocolVersion":%q,"capabilities":{"tools":{}},"serverInfo":{"name":"demo-echo-http","version":"1.0.0"}}`,
				mcp.ProtocolVersion))))
		case mcp.MethodToolsList:
			_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(
				`{"tools":[`+
					`{"name":"echo","description":"Echo the given text back verbatim.","inputSchema":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}},`+
					`{"name":"ping","description":"Health check: always returns \"pong\".","inputSchema":{"type":"object","properties":{}}}`+
					`]}`)))
		case mcp.MethodResourceList:
			_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(`{"resources":[]}`)))
		case mcp.MethodToolsCall:
			var p mcp.ToolsCallParams
			_ = json.Unmarshal(req.Params, &p)
			switch p.Name {
			case "echo":
				var args struct {
					Text string `json:"text"`
				}
				_ = json.Unmarshal(p.Arguments, &args)
				_ = enc.Encode(mcp.NewResult(req.ID, text(args.Text)))
			case "ping":
				_ = enc.Encode(mcp.NewResult(req.ID, text("pong")))
			default:
				_ = enc.Encode(mcp.NewError(req.ID, mcp.CodeInvalidParams, "unknown tool: "+p.Name, nil))
			}
		default:
			_ = enc.Encode(mcp.NewError(req.ID, mcp.CodeMethodNotFound, "method not found", nil))
		}
	})
}

// addrCapture is a slog.Handler that reports the first "addr" attribute it
// sees. The HTTP transport's Serve binds its listener internally (there is no
// exported handler or bound-address accessor, and this round deliberately
// changes no prod code), so with listen_addr "127.0.0.1:0" the only way to
// learn the kernel-chosen port is the "http transport ready" log record —
// which doubles as the readiness signal.
type addrCapture struct {
	ch chan string
}

func (h *addrCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *addrCapture) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *addrCapture) WithGroup(string) slog.Handler            { return h }
func (h *addrCapture) Handle(_ context.Context, rec slog.Record) error {
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == "addr" {
			select {
			case h.ch <- a.Value.String():
			default: // already reported
			}
			return false
		}
		return true
	})
	return nil
}

// boolPtr is a tiny helper for config.Upstream.Enabled (*bool: nil = key
// absent in YAML = enabled).
func boolPtr(b bool) *bool { return &b }

func noopPayloadLog(t *testing.T) logging.PayloadLog {
	t.Helper()
	p, err := logging.NewPayloadLog("")
	if err != nil {
		t.Fatalf("NewPayloadLog: %v", err)
	}
	return p
}

// startHTTPGateway brings up the full gateway stack in-process — registry with
// one HTTP upstream, HTTP transport bound to an ephemeral loopback port — and
// returns the gateway's /mcp endpoint URL. Cleanup (registered on t) cancels
// Serve and waits for it to return so upstream connections are torn down
// before the httptest upstream closes.
func startHTTPGateway(t *testing.T) string {
	t.Helper()

	upstream := httptest.NewServer(echoUpstream())
	t.Cleanup(upstream.Close)

	cfg := &config.Config{
		Transport:  config.TransportHTTP,
		ListenAddr: "127.0.0.1:0", // ephemeral port; discovered via addrCapture
		Upstreams: []config.Upstream{
			{Name: "demo", URL: upstream.URL, Enabled: boolPtr(true)}, // kind inferred http
		},
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := registry.New(cfg, quiet, nil, noopPayloadLog(t), true, gatewayVersion)

	capture := &addrCapture{ch: make(chan string, 1)}
	srv := transport.NewServer(cfg, reg, slog.New(capture), gatewayVersion)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("gateway Serve returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("gateway Serve did not stop within 10s of cancel")
		}
	})

	select {
	case addr := <-capture.ch:
		return "http://" + addr + "/mcp"
	case err := <-done:
		t.Fatalf("gateway Serve exited before becoming ready: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("gateway did not report a listen address within 15s")
	}
	return "" // unreachable
}

// connect dials the gateway with the official SDK client over Streamable HTTP
// and returns an initialized session.
//
// DisableStandaloneSSE: the gateway answers GET /mcp with 405 — it has no
// server-initiated messages in the MVP, which the spec explicitly allows
// (docs/MCP_NOTES.md §8) — so the SDK's optional standalone SSE stream is
// switched off rather than left to retry against a permanent 405.
func connect(t *testing.T, ctx context.Context, endpoint string) *sdk.ClientSession {
	t.Helper()
	client := sdk.NewClient(&sdk.Implementation{Name: "interop-test-client", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{
		Endpoint:             endpoint,
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("SDK client Connect (initialize handshake): %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// textOf extracts the single text content of a tool result.
func textOf(t *testing.T, res *sdk.CallToolResult) string {
	t.Helper()
	if res.IsError {
		t.Fatalf("tool result isError=true: %+v", res.Content)
	}
	if len(res.Content) != 1 {
		t.Fatalf("tool result content length = %d, want 1 (%+v)", len(res.Content), res.Content)
	}
	tc, ok := res.Content[0].(*sdk.TextContent)
	if !ok {
		t.Fatalf("tool result content is %T, want *sdk.TextContent", res.Content[0])
	}
	return tc.Text
}

// TestSDKClientOverHTTP drives the whole gateway through the official Go SDK:
// initialize handshake, aggregated (namespaced) tool catalog, and tool calls
// routed to the upstream and back.
func TestSDKClientOverHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("interop test brings up a full in-process gateway; skipped with -short")
	}

	endpoint := startHTTPGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	session := connect(t, ctx, endpoint)

	// Initialize result: the SDK accepted the gateway's handshake; serverInfo
	// must identify the gateway (not any upstream).
	init := session.InitializeResult()
	if init.ServerInfo == nil || init.ServerInfo.Name != "aiMCPGate" || init.ServerInfo.Version != gatewayVersion {
		t.Errorf("serverInfo = %+v, want aiMCPGate/%s", init.ServerInfo, gatewayVersion)
	}
	if init.ProtocolVersion != mcp.ProtocolVersion {
		t.Errorf("negotiated protocolVersion = %q, want %q", init.ProtocolVersion, mcp.ProtocolVersion)
	}

	// tools/list: the upstream's tools appear under the aggregated namespace.
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	got := map[string]bool{}
	for _, tool := range tools.Tools {
		got[tool.Name] = true
	}
	for _, want := range []string{"demo__echo", "demo__ping"} {
		if !got[want] {
			t.Errorf("catalog missing %q (got %v)", want, got)
		}
	}

	// tools/call: arguments round-trip through the gateway to the upstream.
	echoRes, err := session.CallTool(ctx, &sdk.CallToolParams{
		Name:      "demo__echo",
		Arguments: map[string]any{"text": "interop-echo"},
	})
	if err != nil {
		t.Fatalf("CallTool demo__echo: %v", err)
	}
	if text := textOf(t, echoRes); text != "interop-echo" {
		t.Errorf("echo returned %q, want %q", text, "interop-echo")
	}

	pingRes, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "demo__ping"})
	if err != nil {
		t.Fatalf("CallTool demo__ping: %v", err)
	}
	if text := textOf(t, pingRes); text != "pong" {
		t.Errorf("ping returned %q, want %q", text, "pong")
	}
}

// TestSDKClientOverStdioFraming checks the stdio side of the codec: the SDK
// client speaks newline-delimited JSON (its IOTransport) straight into
// internal/demo's echo server, which is built on the same internal/mcp
// Reader/Writer the gateway's own stdio transport uses. This pins the framing
// (one message per line, no embedded newlines) against an independent
// implementation without any subprocess.
//
// The gateway's OWN stdio transport cannot be wired here in-process: its
// constructor is unexported and transport.NewServer hardwires os.Stdin/
// os.Stdout, and this round deliberately changes no prod code. Its dispatch
// logic is shared with the HTTP transport exercised above; only the pipe
// plumbing goes untested, which the demo server covers at the codec level.
func TestSDKClientOverStdioFraming(t *testing.T) {
	if testing.Short() {
		t.Skip("interop test; skipped with -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// client → demo stdin, demo stdout → client.
	demoIn, clientOut := io.Pipe()
	clientIn, demoOut := io.Pipe()
	go func() { _ = demo.Run(ctx, demoIn, demoOut, gatewayVersion) }()

	client := sdk.NewClient(&sdk.Implementation{Name: "interop-test-client", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &sdk.IOTransport{Reader: clientIn, Writer: clientOut}, nil)
	if err != nil {
		t.Fatalf("SDK client Connect to demo server over pipes: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	init := session.InitializeResult()
	if init.ServerInfo == nil || init.ServerInfo.Name != "aimcpgate-demo-echo" {
		t.Errorf("serverInfo = %+v, want aimcpgate-demo-echo", init.ServerInfo)
	}

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools.Tools) != 2 {
		t.Fatalf("demo tools = %d, want 2 (echo, ping)", len(tools.Tools))
	}

	res, err := session.CallTool(ctx, &sdk.CallToolParams{
		Name: "echo",
		// Escaped newlines inside a string value are legal within one frame;
		// the demo must echo them back intact through the SDK's own framing.
		Arguments: map[string]any{"text": "line1\nline2"},
	})
	if err != nil {
		t.Fatalf("CallTool echo: %v", err)
	}
	if text := textOf(t, res); text != "line1\nline2" {
		t.Errorf("echo returned %q, want %q", text, "line1\nline2")
	}
}
