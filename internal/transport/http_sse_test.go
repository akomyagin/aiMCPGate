// Round 12 — the GET /mcp server→client SSE stream. These tests drive
// handleSSE through REAL HTTP round-trips (httptest.NewServer + a streaming
// client; httptest.NewRecorder cannot stream), wired exactly as production
// Serve wires it: authMiddleware wrapping handleMCP — the write-deadline
// behaviour under test spans BOTH layers (the middleware arms a per-request
// deadline sized for one POST, handleSSE must neutralize it for the stream's
// lifetime).
package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// startSSEGateway builds a gateway from cfg and mounts the PRODUCTION handler
// chain — hs.authMiddleware(handleMCP) — into an httptest.Server. extraMW,
// when non-nil, is spliced between the middleware and the handler (used by the
// expired-deadline test to pre-arm a hostile write deadline exactly where
// authMiddleware arms its own).
func startSSEGateway(t *testing.T, cfg *config.Config, extraMW func(http.Handler) http.Handler) (*httptest.Server, func()) {
	t.Helper()
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry Start: %v", err)
	}
	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	var h http.Handler = http.HandlerFunc(hs.handleMCP)
	if extraMW != nil {
		h = extraMW(h)
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", hs.authMiddleware(h))
	srv := httptest.NewServer(mux)
	return srv, func() { srv.Close(); _ = reg.Close() }
}

// notifyingUpstreamCfg returns a config with one fakeserver upstream whose
// catalog can be mutated at runtime: write toolsFile (FAKE_TOOLS_FILE) with a
// new tool list, then create notifyFile (FAKE_NOTIFY_FILE) to make the
// upstream send notifications/tools/list_changed — the registry re-lists
// (debounced) and signals its catalog subscribers, which is what the SSE
// stream must relay.
func notifyingUpstreamCfg(t *testing.T) (cfg *config.Config, toolsFile, notifyFile string) {
	t.Helper()
	bin := buildFakeServer(t)
	dir := t.TempDir()
	toolsFile = filepath.Join(dir, "tools")
	notifyFile = filepath.Join(dir, "notify")
	cfg = &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":        "web",
				"FAKE_TOOLS":       "fetch",
				"FAKE_TOOLS_FILE":  toolsFile,
				"FAKE_NOTIFY_FILE": notifyFile,
			}},
		},
	}
	return cfg, toolsFile, notifyFile
}

// triggerCatalogChange mutates the upstream catalog and fires its
// list_changed (see notifyingUpstreamCfg).
func triggerCatalogChange(t *testing.T, toolsFile, notifyFile string) {
	t.Helper()
	if err := os.WriteFile(toolsFile, []byte("fetch,scrape\n"), 0o644); err != nil {
		t.Fatalf("write tools file: %v", err)
	}
	if err := os.WriteFile(notifyFile, []byte("go\n"), 0o644); err != nil {
		t.Fatalf("write notify file: %v", err)
	}
}

// openSSE issues GET /mcp with the given extra headers and returns the live
// (possibly streaming) response. Status assertions are the caller's.
func openSSE(t *testing.T, url string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+"/mcp", nil)
	if err != nil {
		t.Fatalf("build GET request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	// A fresh client per stream: the default shared transport would pool the
	// half-open SSE connection across tests.
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatalf("GET /mcp: %v", err)
	}
	return resp
}

// requireSSEOpen asserts the GET was answered as an open event stream.
func requireSSEOpen(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /mcp status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
}

// sseEvent is one decoded SSE frame: the id line and the JSON-RPC message in
// its data line.
type sseEvent struct {
	id  string
	msg *mcp.Message
}

// sseEvents parses SSE frames off body on its own goroutine, delivering each
// complete event on the returned channel; the channel closes when the stream
// ends (server shutdown, closed body). Reading on a goroutine keeps every
// test's waits bounded — bufio.Scanner.Scan itself blocks indefinitely.
func sseEvents(body io.Reader) <-chan sseEvent {
	ch := make(chan sseEvent, 16)
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(body)
		var ev sseEvent
		var data []byte
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "": // blank line terminates the event
				if len(data) > 0 {
					var m mcp.Message
					if json.Unmarshal(data, &m) == nil {
						ev.msg = &m
						ch <- ev
					}
				}
				ev, data = sseEvent{}, nil
			case strings.HasPrefix(line, "id: "):
				ev.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "data: "):
				data = append(data, strings.TrimPrefix(line, "data: ")...)
			}
		}
	}()
	return ch
}

// waitEvent receives one event within timeout or fails the test.
func waitEvent(t *testing.T, ch <-chan sseEvent, timeout time.Duration, what string) sseEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatalf("%s: SSE stream closed before the event arrived", what)
		}
		return ev
	case <-time.After(timeout):
		t.Fatalf("%s: no SSE event within %v", what, timeout)
	}
	return sseEvent{} // unreachable
}

// TestHTTPSSEPushesListChangedOnCatalogChange proves the core Round 12 path:
// a runtime catalog mutation (upstream sends tools/list_changed, registry
// re-lists and signals its subscribers) reaches an open GET SSE stream as a
// framed notifications/tools/list_changed event.
func TestHTTPSSEPushesListChangedOnCatalogChange(t *testing.T) {
	cfg, toolsFile, notifyFile := notifyingUpstreamCfg(t)
	srv, cleanup := startSSEGateway(t, cfg, nil)
	defer cleanup()

	resp := openSSE(t, srv.URL, nil)
	defer func() { _ = resp.Body.Close() }()
	requireSSEOpen(t, resp)
	events := sseEvents(resp.Body)

	// The stream is open (headers received ⇒ handleSSE subscribed before
	// writing them), so a change from here on cannot be missed.
	triggerCatalogChange(t, toolsFile, notifyFile)

	ev := waitEvent(t, events, 15*time.Second, "list_changed")
	if ev.msg.Method != mcp.NotifToolsListChanged || !ev.msg.IsNotification() {
		t.Fatalf("event = %+v, want notifications/tools/list_changed notification", ev.msg)
	}
	if ev.msg.JSONRPC != mcp.Version {
		t.Errorf("event jsonrpc = %q, want %q (full JSON-RPC frame)", ev.msg.JSONRPC, mcp.Version)
	}
	if ev.id != "1" {
		t.Errorf("first SSE event id = %q, want \"1\" (monotonic per-connection counter)", ev.id)
	}
}

// TestHTTPSSEForwardsProgressDuringToolsCall proves the second event source:
// an upstream emitting notifications/progress mid-tools/call (FAKE_PROGRESS
// echoes the client's progressToken) has it forwarded to the concurrent GET
// SSE stream verbatim — same method, same params, token untouched.
func TestHTTPSSEForwardsProgressDuringToolsCall(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":     "web",
				"FAKE_TOOLS":    "fetch",
				"FAKE_ECHO":     "1",
				"FAKE_PROGRESS": "1",
			}},
		},
	}
	srv, cleanup := startSSEGateway(t, cfg, nil)
	defer cleanup()

	resp := openSSE(t, srv.URL, nil)
	defer func() { _ = resp.Body.Close() }()
	requireSSEOpen(t, resp)
	events := sseEvents(resp.Body)

	// POST the call while the stream is open; the upstream emits progress
	// before answering, and the two travel independent paths (SSE stream vs
	// POST response).
	id := mcp.IntID(7)
	callResp := post(t, srv, mcp.NewRequest(id, mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{
		Name: "web__fetch",
		Meta: json.RawMessage(`{"progressToken":42}`),
	})))
	reply := decodeBody(t, callResp)
	if reply.Error != nil {
		t.Fatalf("tools/call error: %v", reply.Error)
	}

	ev := waitEvent(t, events, 10*time.Second, "progress")
	if ev.msg.Method != mcp.NotifProgress {
		t.Fatalf("event method = %q, want %q", ev.msg.Method, mcp.NotifProgress)
	}
	if !strings.Contains(string(ev.msg.Params), `"progressToken":42`) {
		t.Errorf("progress params = %s, want the client's token 42 verbatim", ev.msg.Params)
	}
	if ev.msg.JSONRPC != mcp.Version {
		t.Errorf("event jsonrpc = %q, want %q (message forwarded whole)", ev.msg.JSONRPC, mcp.Version)
	}
}

// TestHTTPSSEAuthAndOriginEnforced confirms GET SSE sits behind exactly the
// same gates as POST: authMiddleware (401 without/with a wrong bearer token)
// and handleMCP's Origin check (403 for a foreign browser origin) both run
// before handleSSE ever opens a stream.
func TestHTTPSSEAuthAndOriginEnforced(t *testing.T) {
	const token = "sse-secret-token"
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		AuthToken: token,
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":  "web",
				"FAKE_TOOLS": "fetch",
			}},
		},
	}
	srv, cleanup := startSSEGateway(t, cfg, nil)
	defer cleanup()

	cases := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"no token", nil, http.StatusUnauthorized},
		{"wrong token", map[string]string{"Authorization": "Bearer wrong"}, http.StatusUnauthorized},
		{"valid token, foreign origin", map[string]string{
			"Authorization": "Bearer " + token,
			"Origin":        "http://evil.example.com",
		}, http.StatusForbidden},
		{"valid token", map[string]string{"Authorization": "Bearer " + token}, http.StatusOK},
		{"valid token, localhost origin", map[string]string{
			"Authorization": "Bearer " + token,
			"Origin":        "http://localhost:3000",
		}, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := openSSE(t, srv.URL, tc.headers)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.want {
				t.Fatalf("GET /mcp status = %d, want %d", resp.StatusCode, tc.want)
			}
			if tc.want == http.StatusOK {
				requireSSEOpen(t, resp)
			}
		})
	}
}

// TestHTTPSSEStreamOutlivesCallTimeout pins the user-visible Round 12
// contract at PRODUCTION wiring: with an artificially small call_timeout
// (200ms) the stream stays open through an idle period several times longer,
// and an event arriving after it is still delivered. (With production
// constants the middleware's deadline is call_timeout + a 10s slack, so the
// deadline itself cannot expire inside this test's bounded runtime — the
// actual expiry scenario is proven by TestHTTPSSESurvivesExpiredWriteDeadline
// below.)
func TestHTTPSSEStreamOutlivesCallTimeout(t *testing.T) {
	cfg, toolsFile, notifyFile := notifyingUpstreamCfg(t)
	cfg.CallTimeout = 200 * time.Millisecond
	srv, cleanup := startSSEGateway(t, cfg, nil)
	defer cleanup()

	resp := openSSE(t, srv.URL, nil)
	defer func() { _ = resp.Body.Close() }()
	requireSSEOpen(t, resp)
	events := sseEvents(resp.Body)

	// Idle well past call_timeout with NO events on the stream.
	time.Sleep(700 * time.Millisecond)
	select {
	case ev, ok := <-events:
		if !ok {
			t.Fatal("SSE stream died during an idle period longer than call_timeout")
		}
		t.Fatalf("unexpected event during idle: %+v", ev.msg)
	default: // stream is quiet and alive — expected
	}

	triggerCatalogChange(t, toolsFile, notifyFile)
	ev := waitEvent(t, events, 15*time.Second, "post-idle list_changed")
	if ev.msg.Method != mcp.NotifToolsListChanged {
		t.Fatalf("post-idle event method = %q, want %q", ev.msg.Method, mcp.NotifToolsListChanged)
	}
}

// TestHTTPSSESurvivesExpiredWriteDeadline is the REAL proof for the Round 12
// deadline bug: authMiddleware arms a per-request write deadline on EVERY
// request (call_timeout + httpWriteTimeoutSlack), and per
// http.ResponseController's documented contract a write deadline that has
// already been exceeded can no longer be extended — so a handleSSE that left
// it armed (or only extended it lazily before each write) would have its
// first post-expiry write fail and the stream die silently, with the client
// alive and reading. Reproducing that with production constants would need a
// >10s idle (the slack dominates), so the test emulates the middleware's role
// with a far tighter deadline: a wrapper arms a 150ms write deadline exactly
// where authMiddleware arms its own, the stream then idles 500ms (deadline
// long exceeded), and an event must STILL be delivered — possible only
// because handleSSE disarms the pre-set deadline while it is fresh and
// re-arms a finite one around each individual write.
func TestHTTPSSESurvivesExpiredWriteDeadline(t *testing.T) {
	cfg, toolsFile, notifyFile := notifyingUpstreamCfg(t)
	shortDeadline := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
				t.Errorf("pre-arm write deadline: %v", err)
			}
			next.ServeHTTP(w, r)
		})
	}
	srv, cleanup := startSSEGateway(t, cfg, shortDeadline)
	defer cleanup()

	resp := openSSE(t, srv.URL, nil)
	defer func() { _ = resp.Body.Close() }()
	requireSSEOpen(t, resp)
	events := sseEvents(resp.Body)

	// Let the pre-armed 150ms deadline expire — with no deadline handling in
	// handleSSE, every write from here on would fail.
	time.Sleep(500 * time.Millisecond)

	triggerCatalogChange(t, toolsFile, notifyFile)
	ev := waitEvent(t, events, 15*time.Second, "post-expiry list_changed")
	if ev.msg.Method != mcp.NotifToolsListChanged {
		t.Fatalf("post-expiry event method = %q, want %q", ev.msg.Method, mcp.NotifToolsListChanged)
	}
}

// TestHTTPSSEGracefulShutdownClosesStream drives the REAL httpServer.Serve
// (bind, BaseContext wiring, Shutdown) end to end: with an SSE stream open,
// cancelling Serve's context (SIGTERM path) must both complete the graceful
// shutdown promptly — the SSE handler returns on r.Context() cancellation, it
// is NOT waited out for the whole 5s shutdown budget — and end the client's
// stream, with every wait bounded.
func TestHTTPSSEGracefulShutdownClosesStream(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport:  config.TransportHTTP,
		ListenAddr: "127.0.0.1:0", // ephemeral port, learned via onListen
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":  "web",
				"FAKE_TOOLS": "fetch",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")

	addrCh := make(chan net.Addr, 1)
	hs.onListen = func(a net.Addr) { addrCh <- a }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- hs.Serve(ctx) }()

	var addr net.Addr
	select {
	case addr = <-addrCh:
	case err := <-done:
		t.Fatalf("Serve exited before binding: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Serve never bound its listener")
	}

	resp := openSSE(t, "http://"+addr.String(), nil)
	defer func() { _ = resp.Body.Close() }()
	requireSSEOpen(t, resp)
	events := sseEvents(resp.Body)

	cancel() // SIGTERM equivalent: Serve runs srv.Shutdown

	// Serve must return well inside its own 5s shutdown budget: BaseContext
	// cancellation reaches the SSE handler's r.Context(), the handler exits,
	// the connection goes idle and Shutdown completes.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error on shutdown: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Serve did not shut down with an open SSE stream (handler stuck?)")
	}

	// The client's stream must reach EOF, not hang.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return // stream closed cleanly — pass
			}
			// A stray buffered event is fine; keep draining until close.
		case <-deadline:
			t.Fatal("SSE stream did not close after server shutdown")
		}
	}
}

// TestHTTPAdvertisesToolsListChanged pins the capability flip that comes with
// the SSE stream: the HTTP transport can now push
// notifications/tools/list_changed, so initialize must advertise
// tools.listChanged:true (it was false while HTTP was POST-only).
func TestHTTPAdvertisesToolsListChanged(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	resp := post(t, srv, mcp.NewRequest(mcp.IntID(1), mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	})))
	msg := decodeBody(t, resp)
	if msg.Error != nil {
		t.Fatalf("initialize error: %v", msg.Error)
	}
	var res mcp.InitializeResult
	if err := json.Unmarshal(msg.Result, &res); err != nil {
		t.Fatalf("decode initialize result: %v", err)
	}
	var caps struct {
		Tools struct {
			ListChanged bool `json:"listChanged"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(res.Capabilities, &caps); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	if !caps.Tools.ListChanged {
		t.Fatal("HTTP initialize advertises tools.listChanged=false, want true (GET SSE stream exists since Round 12)")
	}
}
