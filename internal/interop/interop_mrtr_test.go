package interop

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
	"github.com/akomyagin/aiMCPGate/internal/transport"
)

// Stage 19c interop: the WHOLE MRTR round trip through the OFFICIAL SDK. A
// modern SDK client with an ElicitationHandler calls a gateway tool whose
// (legacy) HTTP upstream elicits mid-call. The gateway converts the upstream's
// counter-request into an InputRequiredResult; the SDK's multi-round-trip shim
// (SEP-2322) transparently invokes the handler and retries with inputResponses.
// A green run proves our InputRequiredResult wire shape is exactly what the real
// client's shim consumes — the independent oracle for the whole bridge.

// elicitUpstream is a legacy (2025-06-18) Streamable-HTTP MCP upstream that
// elicits on every tools/call: it keeps a live GET-SSE stream, pushes an
// elicitation/create request onto it when a call arrives, waits for the client's
// answer to be POSTed back, then answers the call echoing what it elicited. This
// is the counter-request leg the MRTR bridge sits in front of (the upstream side
// is unchanged legacy, plan §2.3).
type elicitUpstream struct {
	mu      sync.Mutex
	stream  http.ResponseWriter
	flush   http.Flusher
	answers map[string]chan json.RawMessage // elicit id → the answer routed back
	seq     int
}

func newElicitUpstream() *elicitUpstream {
	return &elicitUpstream{answers: map[string]chan json.RawMessage{}}
}

func (u *elicitUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		// The long-lived server→client stream. Hold it open; tools/call pushes
		// the elicitation/create frame here.
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		fl.Flush()
		u.mu.Lock()
		u.stream = w
		u.flush = fl
		u.mu.Unlock()
		<-r.Context().Done()
		return
	}

	var req mcp.Message
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// A RESPONSE POSTed back is the gateway answering our elicitation/create.
	if req.IsResponse() {
		u.mu.Lock()
		ch := u.answers[string(req.ID)]
		u.mu.Unlock()
		if ch != nil {
			ch <- req.Result
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if req.IsNotification() {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	switch req.Method {
	case mcp.MethodInitialize:
		// Declare tools; the elicitation is server-initiated, gated only by what
		// the gateway declared to US (it will declare elicitation because the SDK
		// client did). We just push the request when a call comes in.
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(fmt.Sprintf(
			`{"protocolVersion":%q,"capabilities":{"tools":{}},"serverInfo":{"name":"elicit-upstream","version":"1.0.0"}}`,
			mcp.ProtocolVersion))))
	case mcp.MethodToolsList:
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(
			`{"tools":[{"name":"ask","description":"asks for input","inputSchema":{"type":"object"}}]}`)))
	case mcp.MethodResourceList:
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(`{"resources":[]}`)))
	case mcp.MethodToolsCall:
		// Elicit, wait for the answer, then finish the call. On its own goroutine
		// so this POST handler can return the final result only after the answer
		// arrives via a SEPARATE POST (the gateway's response to our push).
		u.mu.Lock()
		u.seq++
		elicitID := fmt.Sprintf("elicit-up-%d", u.seq)
		idRaw, _ := json.Marshal(elicitID)
		ch := make(chan json.RawMessage, 1)
		// Key by the RAW JSON id (with quotes) — that is what string(req.ID) is
		// when the answer POST comes back.
		u.answers[string(idRaw)] = ch
		stream, flush := u.stream, u.flush
		u.mu.Unlock()
		if stream == nil {
			_ = enc.Encode(mcp.NewError(req.ID, mcp.CodeInternalError, "no server→client stream yet", nil))
			return
		}
		frame := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"method":%q,"params":%s}`,
			idRaw, mcp.MethodElicitationCreate,
			`{"message":"need input","requestedSchema":{"type":"object","properties":{"answer":{"type":"string"}}}}`)
		u.mu.Lock()
		fmt.Fprintf(stream, "data: %s\n\n", frame)
		flush.Flush()
		u.mu.Unlock()

		var answer json.RawMessage
		select {
		case answer = <-ch:
		case <-time.After(15 * time.Second):
			_ = enc.Encode(mcp.NewError(req.ID, mcp.CodeInternalError, "elicit timed out", nil))
			return
		}
		b, _ := json.Marshal("asked and got " + string(answer))
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(
			fmt.Sprintf(`{"content":[{"type":"text","text":%s}],"isError":false}`, b))))
	default:
		_ = enc.Encode(mcp.NewError(req.ID, mcp.CodeMethodNotFound, "method not found", nil))
	}
}

// startGatewayWithUpstream is startHTTPGateway parameterized on the upstream
// handler, so an MRTR test can supply an elicit-capable one.
func startGatewayWithUpstream(t *testing.T, h http.Handler) string {
	t.Helper()
	upstream := httptest.NewServer(h)
	t.Cleanup(upstream.Close)

	cfg := &config.Config{
		Transport:  config.TransportHTTP,
		ListenAddr: "127.0.0.1:0",
		Upstreams: []config.Upstream{
			{Name: "demo", URL: upstream.URL, Enabled: boolPtr(true)},
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
		case <-done:
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
	return ""
}

// TestSDKModernElicitation drives the full MRTR round trip through the official
// SDK client: it declares an ElicitationHandler (so the gateway declares
// elicitation to the upstream and the SDK's multi-round-trip shim is armed),
// calls the tool, and expects the shim to transparently answer the elicitation
// and return the upstream's final result — no InputRequiredResult ever surfaces
// to the caller.
func TestSDKModernElicitation(t *testing.T) {
	if testing.Short() {
		t.Skip("interop test brings up a full in-process gateway; skipped with -short")
	}

	endpoint := startGatewayWithUpstream(t, newElicitUpstream())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var elicited bool
	client := sdk.NewClient(&sdk.Implementation{Name: "interop-mrtr-client", Version: "1.0.0"},
		&sdk.ClientOptions{
			ElicitationHandler: func(_ context.Context, _ *sdk.ElicitRequest) (*sdk.ElicitResult, error) {
				elicited = true
				return &sdk.ElicitResult{Action: "accept", Content: map[string]any{"answer": "yes"}}, nil
			},
		})
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: endpoint}, nil)
	if err != nil {
		t.Fatalf("SDK client Connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	if session.InitializeResult().ProtocolVersion != mcp.ProtocolVersionModern {
		t.Fatalf("negotiated version = %q, want modern", session.InitializeResult().ProtocolVersion)
	}

	res, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "demo__ask", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool demo__ask (MRTR round trip): %v", err)
	}
	if !elicited {
		t.Error("the SDK never invoked the ElicitationHandler — the MRTR round trip did not reach the client")
	}
	if res.IsError {
		t.Fatalf("tool result isError=true: %+v", res.Content)
	}
	if len(res.Content) != 1 {
		t.Fatalf("content length = %d, want 1: %+v", len(res.Content), res.Content)
	}
	tc, ok := res.Content[0].(*sdk.TextContent)
	if !ok {
		t.Fatalf("content is %T, want *sdk.TextContent", res.Content[0])
	}
	if wantSub := "asked and got"; !strings.Contains(tc.Text, wantSub) {
		t.Errorf("final tool text = %q, want it to contain %q", tc.Text, wantSub)
	}
}
