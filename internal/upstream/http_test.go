package upstream_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/upstream"
)

// quietLogger is declared in this package's stdio_test.go; reused here.

// fakeHTTPServer is an httptest-backed MCP server speaking the Streamable HTTP
// transport (MCP 2025-06-18). It is the HTTP analogue of the stdio
// testdata/fakeserver: a deterministic integration peer that runs the REAL
// net/http round-trip, not a mock. Behaviour is toggled by fields so one server
// plays several roles.
type fakeHTTPServer struct {
	tools []string // tool names advertised in tools/list
	echo  bool     // if true, tools/call echoes arguments back as text content
	sse   bool     // if true, request responses are delivered as an SSE stream

	sessionID   string // if non-empty, handed back on initialize and required after
	requireAuth string // if non-empty, the exact Authorization header required

	mu           sync.Mutex
	sawAuth      string // last Authorization header seen (for assertions)
	initialized  bool
	sessionEchos int // count of requests that echoed the session id back
}

func (f *fakeHTTPServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "only POST", http.StatusMethodNotAllowed)
			return
		}
		if f.requireAuth != "" && r.Header.Get("Authorization") != f.requireAuth {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		f.mu.Lock()
		f.sawAuth = r.Header.Get("Authorization")
		if f.sessionID != "" && r.Header.Get("Mcp-Session-Id") == f.sessionID {
			f.sessionEchos++
		}
		f.mu.Unlock()

		var req mcp.Message
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}

		// Notifications: 202 with no body.
		if req.IsNotification() {
			if req.Method == mcp.NotifInitialized {
				f.mu.Lock()
				f.initialized = true
				f.mu.Unlock()
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}

		resp := f.reply(&req)
		if req.Method == mcp.MethodInitialize && f.sessionID != "" {
			w.Header().Set("Mcp-Session-Id", f.sessionID)
		}
		if f.sse {
			f.writeSSE(w, resp)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}

// reply builds the JSON-RPC response for a request.
func (f *fakeHTTPServer) reply(req *mcp.Message) *mcp.Message {
	switch req.Method {
	case mcp.MethodInitialize:
		res := fmt.Sprintf(`{"protocolVersion":%q,"capabilities":{"tools":{}},"serverInfo":{"name":"fakehttp","version":"1.0.0"}}`, mcp.ProtocolVersion)
		return mcp.NewResult(req.ID, json.RawMessage(res))
	case mcp.MethodToolsList:
		return mcp.NewResult(req.ID, json.RawMessage(toolsListJSON(f.tools)))
	case mcp.MethodResourceList:
		return mcp.NewResult(req.ID, json.RawMessage(`{"resources":[]}`))
	case mcp.MethodToolsCall:
		var p mcp.ToolsCallParams
		_ = json.Unmarshal(req.Params, &p)
		text := "called " + p.Name
		if f.echo && len(p.Arguments) > 0 {
			text = string(p.Arguments)
		}
		b, _ := json.Marshal(text)
		return mcp.NewResult(req.ID, json.RawMessage(fmt.Sprintf(`{"content":[{"type":"text","text":%s}],"isError":false}`, b)))
	default:
		return mcp.NewError(req.ID, mcp.CodeMethodNotFound, "method not found: "+req.Method, nil)
	}
}

// writeSSE delivers resp as an SSE stream. To exercise the client's ability to
// skip interleaved server messages before the response (spec §Streamable HTTP),
// it first emits an unrelated notification event, then the real response.
func (f *fakeHTTPServer) writeSSE(w http.ResponseWriter, resp *mcp.Message) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)

	interleaved, _ := mcp.Encode(mcp.NewNotification("notifications/message", json.RawMessage(`{"level":"info"}`)))
	fmt.Fprintf(w, "event: message\ndata: %s\n", strings.TrimSpace(string(interleaved)))
	fmt.Fprint(w, "\n")
	if fl != nil {
		fl.Flush()
	}

	body, _ := mcp.Encode(resp)
	fmt.Fprintf(w, "event: message\ndata: %s\n\n", strings.TrimSpace(string(body)))
	if fl != nil {
		fl.Flush()
	}
}

func toolsListJSON(tools []string) string {
	var b strings.Builder
	b.WriteString(`{"tools":[`)
	for i, name := range tools {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":%q,"description":"fake tool %s","inputSchema":{"type":"object"}}`, name, name)
	}
	b.WriteString(`]}`)
	return b.String()
}

// TestHTTPEndpointCredentialsRedactedInErrors pins the audit-log invariant: a
// URL-embedded password (the one config field env-expansion never touches) must
// not leak through a transport error's text, which reaches the metadata-only
// audit log via err.Error(). The password is replaced by url.URL.Redacted's
// "xxxxx" marker.
func TestHTTPEndpointCredentialsRedactedInErrors(t *testing.T) {
	// Port 1 on loopback: connection refused, immediately and deterministically.
	conn := upstream.StartHTTP(quietLogger(), "leaky", "http://user:sup3rsecret@127.0.0.1:1/mcp", nil, nil)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := conn.ListTools(ctx)
	if err == nil {
		t.Fatal("expected a connection error against a closed port")
	}
	if strings.Contains(err.Error(), "sup3rsecret") {
		t.Fatalf("error text leaks the URL-embedded password: %v", err)
	}
	if !strings.Contains(err.Error(), "xxxxx") {
		t.Fatalf("error text should carry the redacted endpoint (user:xxxxx@...), got: %v", err)
	}
}

func newConn(t *testing.T, f *fakeHTTPServer, headers map[string]string) (*upstream.Conn, func()) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	conn := upstream.StartHTTP(quietLogger(), "fakehttp", srv.URL, headers, srv.Client())
	return conn, func() { _ = conn.Close(); srv.Close() }
}

func TestHTTPInitializeAndCatalog(t *testing.T) {
	f := &fakeHTTPServer{tools: []string{"search", "fetch"}}
	conn, cleanup := newConn(t, f, nil)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := conn.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if info.ServerInfo.Name != "fakehttp" {
		t.Errorf("serverInfo.name = %q, want fakehttp", info.ServerInfo.Name)
	}

	f.mu.Lock()
	inited := f.initialized
	f.mu.Unlock()
	if !inited {
		t.Error("server never received notifications/initialized")
	}

	tools, err := conn.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	got := map[string]bool{}
	for _, tl := range tools {
		got[tl.Name] = true
		if tl.InputSchema == nil {
			t.Errorf("tool %q lost inputSchema", tl.Name)
		}
	}
	if !got["search"] || !got["fetch"] {
		t.Errorf("catalog missing tools, got %v", got)
	}
}

func TestHTTPCallToolJSON(t *testing.T) {
	f := &fakeHTTPServer{tools: []string{"fetch"}, echo: true}
	conn, cleanup := newConn(t, f, nil)
	defer cleanup()

	ctx := context.Background()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	resp, err := conn.CallTool(ctx, "fetch", json.RawMessage(`{"q":"golang"}`))
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("CallTool returned error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "golang") {
		t.Errorf("call did not echo arguments through: %s", resp.Result)
	}
}

// TestHTTPCallToolSSE proves the client reads a response out of an SSE stream,
// skipping the interleaved server notification the fake emits first.
func TestHTTPCallToolSSE(t *testing.T) {
	f := &fakeHTTPServer{tools: []string{"fetch"}, echo: true, sse: true}
	conn, cleanup := newConn(t, f, nil)
	defer cleanup()

	ctx := context.Background()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize over SSE: %v", err)
	}
	resp, err := conn.CallTool(ctx, "fetch", json.RawMessage(`{"q":"sse-works"}`))
	if err != nil {
		t.Fatalf("CallTool over SSE: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("CallTool over SSE returned error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "sse-works") {
		t.Errorf("SSE call did not deliver the right response: %s", resp.Result)
	}
}

// TestHTTPSessionIDEchoed checks the client captures Mcp-Session-Id from
// initialize and echoes it on every subsequent request.
func TestHTTPSessionIDEchoed(t *testing.T) {
	f := &fakeHTTPServer{tools: []string{"fetch"}, sessionID: "sess-abc-123"}
	conn, cleanup := newConn(t, f, nil)
	defer cleanup()

	ctx := context.Background()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if _, err := conn.ListTools(ctx); err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if _, err := conn.CallTool(ctx, "fetch", nil); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	f.mu.Lock()
	echos := f.sessionEchos
	f.mu.Unlock()
	// tools/list + tools/call carried the session id (initialize itself does not
	// yet know it), so at least 2 echoes.
	if echos < 2 {
		t.Errorf("session id echoed on %d requests, want >= 2", echos)
	}
}

// TestHTTPAuthHeaderSentNotLogged proves a configured auth header reaches the
// upstream (so the request is authorized) and never appears in the operational
// log — secrets must not leak (SKILL §6).
func TestHTTPAuthHeaderSentNotLogged(t *testing.T) {
	const secret = "Bearer SUPER_SECRET_TOKEN_xyz789"
	f := &fakeHTTPServer{tools: []string{"fetch"}, requireAuth: secret}

	var logBuf strings.Builder
	logger := slog.New(slog.NewJSONHandler(&stringWriter{&logBuf}, &slog.HandlerOptions{Level: slog.LevelDebug}))

	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	conn := upstream.StartHTTP(logger, "fakehttp", srv.URL, map[string]string{"Authorization": secret}, srv.Client())
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize (auth should have succeeded): %v", err)
	}
	if _, err := conn.ListTools(ctx); err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	f.mu.Lock()
	sawAuth := f.sawAuth
	f.mu.Unlock()
	if sawAuth != secret {
		t.Fatalf("upstream did not receive the auth header; got %q", sawAuth)
	}
	if strings.Contains(logBuf.String(), "SUPER_SECRET_TOKEN") {
		t.Fatalf("secret leaked into operational log:\n%s", logBuf.String())
	}
}

// TestHTTPCallOversizedJSONResponseIsRejected is a regression test: the
// application/json branch of call() used to decode with no size limit at all
// (found by code review), unlike the SSE branch, which was already capped.
// A misbehaving/malicious upstream sending a huge body must make the client
// error out instead of buffering it without bound.
func TestHTTPCallOversizedJSONResponseIsRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"padding":"`)
		chunk := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB
		for i := 0; i < 34; i++ {                 // 34 MiB, over the 32 MiB cap
			_, _ = w.Write(chunk)
		}
		// Deliberately never closes the JSON string/object — irrelevant,
		// the reader is cut off well before reaching here either way.
	}))
	defer srv.Close()

	conn := upstream.StartHTTP(quietLogger(), "oversized", srv.URL, nil, srv.Client())
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.ListTools(ctx); err == nil {
		t.Fatal("ListTools with a 34 MiB response body: want an error, got nil (unbounded read?)")
	}
}

// stringWriter adapts a strings.Builder to io.Writer with a mutex, since the
// slog handler may be written from the HTTP client's connection goroutines.
type stringWriter struct{ b *strings.Builder }

var swMu sync.Mutex

func (s *stringWriter) Write(p []byte) (int, error) {
	swMu.Lock()
	defer swMu.Unlock()
	return s.b.Write(p)
}
