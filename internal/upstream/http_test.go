package upstream_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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
	sawInitCaps  string // raw capabilities object of the last initialize received
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
		// Record the CLIENT's declared capabilities verbatim, so tests can
		// assert what the gateway put in its handshake (in particular that an
		// HTTP upstream — no channel for server-initiated requests — is never
		// promised elicitation).
		var p mcp.InitializeParams
		_ = json.Unmarshal(req.Params, &p)
		f.mu.Lock()
		f.sawInitCaps = string(p.Capabilities)
		f.mu.Unlock()
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
	conn := upstream.StartHTTP(quietLogger(), "leaky", "http://user:sup3rsecret@127.0.0.1:1/mcp", nil, nil, "0.0.0-test", nil)
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
	conn := upstream.StartHTTP(quietLogger(), "fakehttp", srv.URL, headers, srv.Client(), "0.0.0-test", nil)
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

// TestHTTPConnDefaultDeclaresNoClientCapabilities pins the DEFAULT of a bare
// HTTP Conn over the real net/http round-trip: nothing declared means exactly
// {} on the wire — byte-identical to the pre-declaration handshake, not null,
// not an absent field. Deliberately narrow: this package cannot see the
// registry's policy (startHTTP never calling DeclareClientCapabilities is
// what actually keeps an HTTP upstream undeclared — RespondUpstreamRequest
// refuses the transport, so declaring would be an unkeepable promise); that
// guarantee is pinned end-to-end by
// registry.TestRegistryDoesNotDeclareElicitationToHTTPUpstream.
func TestHTTPConnDefaultDeclaresNoClientCapabilities(t *testing.T) {
	f := &fakeHTTPServer{tools: []string{"search"}}
	conn, cleanup := newConn(t, f, nil)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	f.mu.Lock()
	caps := f.sawInitCaps
	f.mu.Unlock()
	if caps != "{}" {
		t.Errorf("HTTP upstream received capabilities %s, want exactly {} (nothing to declare)", caps)
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
	resp, err := conn.CallTool(ctx, "fetch", json.RawMessage(`{"q":"golang"}`), nil)
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
	resp, err := conn.CallTool(ctx, "fetch", json.RawMessage(`{"q":"sse-works"}`), nil)
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
	if _, err := conn.CallTool(ctx, "fetch", nil, nil); err != nil {
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
	conn := upstream.StartHTTP(logger, "fakehttp", srv.URL, map[string]string{"Authorization": secret}, srv.Client(), "0.0.0-test", nil)
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

	conn := upstream.StartHTTP(quietLogger(), "oversized", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.ListTools(ctx); err == nil {
		t.Fatal("ListTools with a 34 MiB response body: want an error, got nil (unbounded read?)")
	}
}

// TestCallRejectsNonResponseBody is a regression test: the application/json
// branch of call() used to return whatever decoded as a Message, even a body
// with neither result nor error (not a response at all) — unlike the SSE
// branch, which always checked IsResponse(). Such a body must be an error.
func TestCallRejectsNonResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0"}`)
	}))
	defer srv.Close()

	conn := upstream.StartHTTP(quietLogger(), "nonresponse", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	if _, err := conn.CallTool(context.Background(), "fetch", nil, nil); err == nil {
		t.Fatal("body without result/error: want an error, got success")
	}
}

// TestCallRejectsMismatchedID: a JSON body answering a DIFFERENT request id
// must not be accepted as our response (the SSE branch already skipped such
// frames; the JSON branch used to take them verbatim).
func TestCallRejectsMismatchedID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":424242,"result":{}}`)
	}))
	defer srv.Close()

	conn := upstream.StartHTTP(quietLogger(), "wrongid", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	_, err := conn.CallTool(context.Background(), "fetch", nil, nil)
	if err == nil {
		t.Fatal("response with a mismatched id: want an error, got success")
	}
	if !strings.Contains(err.Error(), "424242") {
		t.Errorf("error should mention the mismatched id, got: %v", err)
	}
}

// TestCallAcceptsNullResult is the control case for the two tests above: an
// explicit "result": null is a perfectly valid JSON-RPC response (RawMessage
// keeps the null bytes, so IsResponse() holds) and must NOT be rejected.
func TestCallAcceptsNullResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Message
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":null}`, req.ID)
	}))
	defer srv.Close()

	conn := upstream.StartHTTP(quietLogger(), "nullresult", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	resp, err := conn.CallTool(context.Background(), "fetch", nil, nil)
	if err != nil {
		t.Fatalf(`"result": null is a valid response, got error: %v`, err)
	}
	if got := strings.TrimSpace(string(resp.Result)); got != "null" {
		t.Errorf("result passed through wrong: got %q, want %q", got, "null")
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

// sessionExpiringServer is a Streamable-HTTP fake for the session-expiry
// scenario (spec: the server MAY drop a session at any time; a request
// carrying the stale Mcp-Session-Id then gets HTTP 404 and the client MUST
// start a new session with a fresh initialize).
type sessionExpiringServer struct {
	alwaysExpire bool // if true, every fresh session is dead on arrival (never recovers)

	mu        sync.Mutex
	session   string   // current valid session id
	expired   bool     // when true, requests carrying ANY session id get 404
	initCount int      // number of initialize requests served
	callSIDs  []string // session ids seen on successfully served tools/call
}

// expire simulates the server forgetting the current session (e.g. restart).
func (s *sessionExpiringServer) expire() {
	s.mu.Lock()
	s.expired = true
	s.mu.Unlock()
}

func (s *sessionExpiringServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Message
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()

		if req.IsNotification() {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if req.Method == mcp.MethodInitialize {
			s.initCount++
			s.session = fmt.Sprintf("sess-%d", s.initCount)
			s.expired = s.alwaysExpire
			w.Header().Set("Mcp-Session-Id", s.session)
			w.Header().Set("Content-Type", "application/json")
			res := fmt.Sprintf(`{"protocolVersion":%q,"capabilities":{},"serverInfo":{"name":"expiring","version":"1.0.0"}}`, mcp.ProtocolVersion)
			_ = json.NewEncoder(w).Encode(mcp.NewResult(req.ID, json.RawMessage(res)))
			return
		}
		if sid := r.Header.Get("Mcp-Session-Id"); sid != s.session || s.expired {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if req.Method == mcp.MethodToolsCall {
			s.callSIDs = append(s.callSIDs, r.Header.Get("Mcp-Session-Id"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mcp.NewResult(req.ID, json.RawMessage(`{"content":[{"type":"text","text":"ok"}]}`)))
	})
}

// TestHTTPSessionExpiry404ReinitializesAndRetries pins the fix for the
// never-invalidated session id: a 404 on a request carrying Mcp-Session-Id
// must clear the stale id, re-run the initialize handshake and retry the call
// once — previously the dead id was echoed forever, bricking the upstream
// until a gateway restart.
func TestHTTPSessionExpiry404ReinitializesAndRetries(t *testing.T) {
	f := &sessionExpiringServer{}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	conn := upstream.StartHTTP(quietLogger(), "expiring", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	f.expire() // the server forgets sess-1 behind our back

	resp, err := conn.CallTool(ctx, "fetch", nil, nil)
	if err != nil {
		t.Fatalf("CallTool after session expiry: want transparent recovery, got %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("CallTool returned rpc error: %v", resp.Error)
	}

	f.mu.Lock()
	inits, sids := f.initCount, append([]string(nil), f.callSIDs...)
	f.mu.Unlock()
	if inits != 2 {
		t.Errorf("server saw %d initialize requests, want 2 (original + recovery)", inits)
	}
	if len(sids) != 1 || sids[0] != "sess-2" {
		t.Errorf("served calls carried session ids %v, want exactly [sess-2] (the FRESH session)", sids)
	}
}

// TestHTTPSessionExpiryRetriesOnlyOnce: when even the retried call comes back
// 404 (the upstream is genuinely broken, not merely restarted), CallTool must
// give up with an error after ONE re-initialize — never loop.
func TestHTTPSessionExpiryRetriesOnlyOnce(t *testing.T) {
	f := &sessionExpiringServer{alwaysExpire: true}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	conn := upstream.StartHTTP(quietLogger(), "expiring", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	_, err := conn.CallTool(ctx, "fetch", nil, nil)
	if err == nil {
		t.Fatal("upstream 404s every session-carrying request: want an error, got success")
	}
	if !strings.Contains(err.Error(), "session expired") {
		t.Errorf("error should name the expired session, got: %v", err)
	}
	f.mu.Lock()
	inits := f.initCount
	f.mu.Unlock()
	if inits != 2 {
		t.Errorf("server saw %d initialize requests, want exactly 2 (one retry, no loop)", inits)
	}
}

// TestHTTPSSEMultiLineDataConcatenated pins WHATWG SSE framing: consecutive
// data: lines of ONE event are one payload (joined with "\n"), parsed as a
// single JSON-RPC message — the previous parser treated every data: line as
// its own message and broke any upstream that splits a response across lines.
func TestHTTPSSEMultiLineDataConcatenated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Message
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		// One JSON-RPC response split across three data: lines of one event —
		// only their concatenation is valid JSON.
		fmt.Fprint(w, "event: message\n")
		fmt.Fprint(w, "data: {\"jsonrpc\":\"2.0\",\n")
		fmt.Fprintf(w, "data: \"id\":%s,\n", req.ID)
		fmt.Fprint(w, "data: \"result\":{\"content\":[{\"type\":\"text\",\"text\":\"multi-line-ok\"}]}}\n")
		fmt.Fprint(w, "\n")
	}))
	defer srv.Close()

	conn := upstream.StartHTTP(quietLogger(), "multiline", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	resp, err := conn.CallTool(context.Background(), "fetch", nil, nil)
	if err != nil {
		t.Fatalf("CallTool over multi-line SSE: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("CallTool returned rpc error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "multi-line-ok") {
		t.Errorf("split payload not reassembled into one message: %s", resp.Result)
	}
}

// TestHTTPJSONBodyDrainedForKeepAlive pins the keep-alive fix: the JSON branch
// of call() must drain the response body to EOF before the deferred Close, or
// net/http cannot return the connection to the pool and every call dials a
// fresh connection. Several sequential calls over one *Conn must therefore
// reuse a connection at least once — before the fix each call deterministically
// opened its own (as many new conns as calls).
func TestHTTPJSONBodyDrainedForKeepAlive(t *testing.T) {
	var newConns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Message
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{}}`, req.ID)
	}))
	srv.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	conn := upstream.StartHTTP(quietLogger(), "reuse", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	const calls = 4
	for i := 0; i < calls; i++ {
		if _, err := conn.CallTool(context.Background(), "fetch", nil, nil); err != nil {
			t.Fatalf("CallTool %d: %v", i, err)
		}
	}
	// Not asserting ==1: connection return to the idle pool races the next
	// request by a hair in net/http, so an occasional second dial is legal.
	// What may NOT happen is the no-reuse-ever signature of an undrained body:
	// one new connection per call.
	if got := newConns.Load(); got >= calls {
		t.Errorf("server saw %d new connections for %d sequential calls — bodies are not drained, keep-alive is dead", got, calls)
	}
}

// TestHTTPNegotiatedProtocolVersionEchoed pins the header fix: after the
// upstream answers initialize with SOME protocol version, every subsequent
// request must advertise THAT version in MCP-Protocol-Version — not the
// package constant the gateway merely proposed.
func TestHTTPNegotiatedProtocolVersionEchoed(t *testing.T) {
	const negotiated = "2025-03-26" // deliberately != mcp.ProtocolVersion
	var mu sync.Mutex
	var afterInit []string // MCP-Protocol-Version values seen after initialize

	initialized := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req mcp.Message
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		mu.Lock()
		if initialized {
			afterInit = append(afterInit, r.Header.Get("MCP-Protocol-Version"))
		}
		mu.Unlock()
		if req.IsNotification() {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if req.Method == mcp.MethodInitialize {
			mu.Lock()
			initialized = true
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			res := fmt.Sprintf(`{"protocolVersion":%q,"capabilities":{},"serverInfo":{"name":"older","version":"1.0.0"}}`, negotiated)
			_ = json.NewEncoder(w).Encode(mcp.NewResult(req.ID, json.RawMessage(res)))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mcp.NewResult(req.ID, json.RawMessage(`{}`)))
	}))
	defer srv.Close()

	conn := upstream.StartHTTP(quietLogger(), "older", srv.URL, nil, srv.Client(), "0.0.0-test", nil)
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if _, err := conn.CallTool(ctx, "fetch", nil, nil); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(afterInit) == 0 {
		t.Fatal("no post-initialize requests observed")
	}
	for i, v := range afterInit {
		if v != negotiated {
			t.Errorf("post-initialize request %d sent MCP-Protocol-Version %q, want the negotiated %q", i, v, negotiated)
		}
	}
}

// --- Round 13: long-lived GET SSE stream (server-initiated notifications) ---

// TestHTTPSSEStreamNotificationsAndReconnect covers the happy path of the
// Round 13 GET stream end to end against a real httptest server: after the
// handshake the connection opens a GET on the same endpoint, receives a
// notification through onNotify, survives the server DROPPING the stream
// (reconnects with Last-Event-ID carrying the last id seen), receives another
// notification on the reconnected stream, and Close stops the goroutine
// promptly even while the stream is held open.
func TestHTTPSSEStreamNotificationsAndReconnect(t *testing.T) {
	f := &fakeHTTPServer{tools: []string{"t"}}
	notifs := make(chan string, 8)

	var mu sync.Mutex
	var lastIDs []string              // Last-Event-ID header of each GET, in order
	streamHeld := make(chan struct{}) // closed when the SECOND stream is up and held

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			f.handler().ServeHTTP(w, r)
			return
		}
		mu.Lock()
		lastIDs = append(lastIDs, r.Header.Get("Last-Event-ID"))
		n := len(lastIDs)
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		if n == 1 {
			// One notification with an event id, then DROP the stream: the
			// client must reconnect and resume via Last-Event-ID: 7.
			fmt.Fprint(w, "id: 7\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n")
			fl.Flush()
			return
		}
		fmt.Fprint(w, "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":1}}\n\n")
		fl.Flush()
		if n == 2 {
			close(streamHeld)
		}
		<-r.Context().Done() // hold the stream open until the client tears it down
	})

	srv := httptest.NewServer(handler)
	defer srv.Close()

	onNotify := func(method string, _ json.RawMessage) { notifs <- method }
	conn := upstream.StartHTTP(quietLogger(), "sse", srv.URL, nil, srv.Client(), "0.0.0-test", onNotify)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	waitNotif := func(want string) {
		t.Helper()
		select {
		case got := <-notifs:
			if got != want {
				t.Fatalf("notification = %q, want %q", got, want)
			}
		case <-time.After(10 * time.Second): // reconnect includes a 1s backoff
			t.Fatalf("timed out waiting for %q from the SSE stream", want)
		}
	}
	waitNotif(mcp.NotifToolsListChanged)
	waitNotif(mcp.NotifProgress) // arrived on the SECOND stream, post-reconnect

	// Close must stop the stream goroutine even though the second stream is
	// being held open by the server — bounded wait, not a leak.
	select {
	case <-streamHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("second stream never reported held")
	}
	closed := make(chan struct{})
	go func() { _ = conn.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not stop the SSE stream goroutine")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lastIDs) < 2 {
		t.Fatalf("expected a reconnect (>= 2 GETs), saw %d", len(lastIDs))
	}
	if lastIDs[0] != "" {
		t.Errorf("first GET carried Last-Event-ID %q, want none", lastIDs[0])
	}
	if lastIDs[1] != "7" {
		t.Errorf("reconnect GET carried Last-Event-ID %q, want %q (the last id received)", lastIDs[1], "7")
	}
}

// TestHTTPSSEStreamNotOffered pins the no-retry-storm guarantee: an upstream
// answering the optional GET with 405 (the spec's "I do not offer the stream")
// gets exactly ONE attempt — no reconnect loop — and normal RPC keeps working.
func TestHTTPSSEStreamNotOffered(t *testing.T) {
	f := &fakeHTTPServer{tools: []string{"t"}}
	var gets atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
			http.Error(w, "no stream here", http.StatusMethodNotAllowed)
			return
		}
		f.handler().ServeHTTP(w, r)
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	conn := upstream.StartHTTP(quietLogger(), "nostream", srv.URL, nil, srv.Client(), "0.0.0-test",
		func(string, json.RawMessage) {})
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	// Wait for the single attempt, then past the would-be first reconnect
	// backoff (1s): a retry loop would show a second GET by then.
	deadline := time.Now().Add(5 * time.Second)
	for gets.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the gateway never attempted the GET SSE stream")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond)
	if n := gets.Load(); n != 1 {
		t.Fatalf("refused SSE stream was retried: %d GET attempts, want exactly 1", n)
	}

	// The connection itself is unimpaired.
	if _, err := conn.ListTools(ctx); err != nil {
		t.Fatalf("ListTools after refused SSE stream: %v", err)
	}
}

// TestHTTPSSEStreamRetriesTransientFirstAttempt (review fix): a TRANSIENT
// failure on the very first GET (here a 503) must NOT permanently disable the
// stream — only an explicit refusal (405/404, or a non-SSE 200) may. The
// client must retry with backoff and receive notifications once the server
// recovers.
func TestHTTPSSEStreamRetriesTransientFirstAttempt(t *testing.T) {
	f := &fakeHTTPServer{tools: []string{"t"}}
	notifs := make(chan string, 4)
	var gets atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			f.handler().ServeHTTP(w, r)
			return
		}
		if gets.Add(1) == 1 {
			// The upstream is briefly unwell — says nothing about SSE support.
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	onNotify := func(method string, _ json.RawMessage) { notifs <- method }
	conn := upstream.StartHTTP(quietLogger(), "transient", srv.URL, nil, srv.Client(), "0.0.0-test", onNotify)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	select {
	case got := <-notifs:
		if got != mcp.NotifToolsListChanged {
			t.Fatalf("notification = %q, want %q", got, mcp.NotifToolsListChanged)
		}
	case <-time.After(10 * time.Second): // the retry includes a 1s backoff
		t.Fatalf("no notification after a transient first-attempt failure: the stream was never retried (%d GETs)", gets.Load())
	}
	if gets.Load() < 2 {
		t.Fatalf("expected a retry after the 503, saw %d GET attempts", gets.Load())
	}
}

// TestHTTPCloseBeforeSSEStreamStarted pins the pre-closed streamDone design:
// Close on a connection whose stream goroutine never launched (no Initialize
// ever ran) must not block waiting for a goroutine that does not exist.
func TestHTTPCloseBeforeSSEStreamStarted(t *testing.T) {
	conn := upstream.StartHTTP(quietLogger(), "never", "http://127.0.0.1:1/mcp", nil, nil, "0.0.0-test",
		func(string, json.RawMessage) {})
	closed := make(chan struct{})
	go func() { _ = conn.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked though the SSE stream never started")
	}
	// And a Close-then-Initialize race resolves to "no stream": a second
	// Close (idempotent) still returns promptly.
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
