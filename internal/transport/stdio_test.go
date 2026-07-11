package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// quietLogger discards operational logs so test output stays clean.
func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// buildFakeServer compiles the shared internal/upstream/testdata/fakeserver
// binary once and returns its path. The fakeserver speaks MCP over stdio and is
// configured via env (FAKE_NAME/FAKE_TOOLS/FAKE_ECHO). Same helper pattern as
// internal/registry/registry_stdio_test.go — testdata dirs are not importable
// across packages, so it is duplicated rather than shared.
func buildFakeServer(t *testing.T) string {
	t.Helper()
	src := filepath.Join("..", "upstream", "testdata", "fakeserver")
	bin := filepath.Join(t.TempDir(), "fakeserver")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = src
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fakeserver: %v\n%s", err, out)
	}
	return bin
}

// fakeClient drives the stdioServer as an MCP client would: it writes framed
// JSON-RPC requests into the server's input pipe and reads framed responses
// from the server's output pipe. It mints its OWN request-id space so the test
// can assert responses come back with the client's id, not an upstream id.
type fakeClient struct {
	t       *testing.T
	toSrv   *io.PipeWriter // client → server (server reads this)
	fromSrv *mcp.Reader    // server → client (client reads this)
	w       *mcp.Writer
	nextID  int64
}

// startServer wires a stdioServer to a pair of pipes and runs Serve in the
// background, returning a fakeClient bound to the other ends. The returned
// cancel stops the server; done is closed when Serve returns.
func startServer(t *testing.T, twoUpstreams bool) (*fakeClient, context.CancelFunc, <-chan error) {
	t.Helper()
	bin := buildFakeServer(t)

	ups := []config.Upstream{
		{Name: "github", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":  "github",
			"FAKE_TOOLS": "search,create_issue",
			"FAKE_ECHO":  "1",
		}},
	}
	if twoUpstreams {
		ups = append(ups, config.Upstream{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":  "web",
			"FAKE_TOOLS": "search,fetch", // "search" collides with github__search
			"FAKE_ECHO":  "1",
		}})
	}
	cfg := &config.Config{Upstreams: ups}

	return startServerWithConfig(t, cfg, nil)
}

// startServerWithConfig is the lower-level variant used when a test needs a
// custom config or a call log.
func startServerWithConfig(t *testing.T, cfg *config.Config, callLog logging.CallLog) (*fakeClient, context.CancelFunc, <-chan error) {
	t.Helper()
	reg := registry.New(cfg, quietLogger(), callLog)

	clientToSrv, srvIn := io.Pipe() // client writes to srvIn side... (see below)
	srvOut, clientFromSrv := io.Pipe()

	// Pipe wiring: the server reads its requests from clientToSrv and writes its
	// responses to clientFromSrv; the client writes to srvIn and reads from
	// srvOut.
	srv := newStdioServer(cfg, reg, quietLogger(), "test-1.2.3", clientToSrv, clientFromSrv)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()

	fc := &fakeClient{
		t:       t,
		toSrv:   srvIn,
		fromSrv: mcp.NewReader(srvOut),
		w:       mcp.NewWriter(srvIn),
	}
	return fc, cancel, done
}

// request sends a request with a fresh client id and returns that id (raw).
func (c *fakeClient) request(method string, params json.RawMessage) json.RawMessage {
	c.t.Helper()
	c.nextID++
	id := mcp.IntID(c.nextID)
	if err := c.w.Write(mcp.NewRequest(id, method, params)); err != nil {
		c.t.Fatalf("client write %s: %v", method, err)
	}
	return id
}

// notify sends a notification (no id, no response).
func (c *fakeClient) notify(method string, params json.RawMessage) {
	c.t.Helper()
	if err := c.w.Write(mcp.NewNotification(method, params)); err != nil {
		c.t.Fatalf("client notify %s: %v", method, err)
	}
}

// readResponse reads the next framed response from the server.
func (c *fakeClient) readResponse() *mcp.Message {
	c.t.Helper()
	msg, err := c.fromSrv.Read()
	if err != nil {
		c.t.Fatalf("client read response: %v", err)
	}
	return msg
}

// closeInput closes the client→server pipe, simulating client disconnect.
func (c *fakeClient) closeInput() { _ = c.toSrv.Close() }

func TestStdioInitializeHandshake(t *testing.T) {
	c, cancel, done := startServer(t, false)
	defer func() { cancel(); <-done }()

	id := c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	resp := c.readResponse()

	if string(resp.ID) != string(id) {
		t.Fatalf("initialize response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error != nil {
		t.Fatalf("initialize returned error: %v", resp.Error)
	}
	var res mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode initialize result: %v", err)
	}
	if res.ServerInfo.Name != "aiMCPGate" {
		t.Errorf("serverInfo.name = %q, want aiMCPGate", res.ServerInfo.Name)
	}
	if res.ServerInfo.Version != "test-1.2.3" {
		t.Errorf("serverInfo.version = %q, want threaded version test-1.2.3", res.ServerInfo.Version)
	}
	if res.ProtocolVersion != mcp.ProtocolVersion {
		t.Errorf("protocolVersion = %q, want %q", res.ProtocolVersion, mcp.ProtocolVersion)
	}
}

// TestStdioAdvertisesListChanged verifies the Stage 7c capability change: over
// stdio the gateway declares tools.listChanged=true, because it CAN push a
// server→client notification over the same pipe when the live catalog changes.
func TestStdioAdvertisesListChanged(t *testing.T) {
	c, cancel, done := startServer(t, false)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	resp := c.readResponse()
	var res mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode initialize result: %v", err)
	}
	if !strings.Contains(string(res.Capabilities), `"listChanged":true`) {
		t.Errorf("stdio capabilities = %s, want tools.listChanged:true", res.Capabilities)
	}
}

// TestStdioPushesListChangedOnCatalogChange proves the server→client path: when
// an upstream crashes and is auto-restarted (Stage 7a), the registry signals a
// catalog change and the stdio transport pushes notifications/tools/list_changed
// to the client (Stage 7c). The fakeserver crashes after one call, so triggering
// the restart is a single tools/call.
func TestStdioPushesListChangedOnCatalogChange(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			MaxAttempts:    5,
		},
		Upstreams: []config.Upstream{
			{Name: "crasher", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	// One call crashes the upstream; its reply comes back first.
	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "crasher__ping"}))
	callResp := c.readResponse()
	if string(callResp.ID) != string(callID) {
		t.Fatalf("first message was not the call reply (id=%s want %s)", callResp.ID, callID)
	}

	// The next server→client message must be the list_changed notification the
	// auto-restart triggered. Read until we see it (there should be nothing else
	// on the stream, but be robust to ordering).
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("did not receive notifications/tools/list_changed after auto-restart")
		}
		msg := c.readResponse()
		if msg.IsNotification() && msg.Method == mcp.NotifToolsListChanged {
			return // success
		}
	}
}

func boolPtr(b bool) *bool { return &b }

func TestStdioToolsListAggregatesNamespaced(t *testing.T) {
	c, cancel, done := startServer(t, true)
	defer func() { cancel(); <-done }()

	c.notify(mcp.NotifInitialized, nil) // must not produce a response

	c.request(mcp.MethodToolsList, nil)
	resp := c.readResponse()
	if resp.Error != nil {
		t.Fatalf("tools/list error: %v", resp.Error)
	}
	var res mcp.ToolsListResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}

	got := map[string]bool{}
	for _, tl := range res.Tools {
		got[tl.Name] = true
		if tl.InputSchema == nil {
			t.Errorf("tool %q lost its inputSchema in aggregation", tl.Name)
		}
	}
	for _, want := range []string{"github__search", "github__create_issue", "web__search", "web__fetch"} {
		if !got[want] {
			t.Errorf("aggregated catalog missing namespaced tool %q (got %v)", want, keys(got))
		}
	}
}

func TestStdioToolsCallRoutesAndKeepsClientID(t *testing.T) {
	c, cancel, done := startServer(t, true)
	defer func() { cancel(); <-done }()

	args := json.RawMessage(`{"q":"golang"}`)
	id := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{
		Name:      "web__search",
		Arguments: args,
	}))
	resp := c.readResponse()

	// The response MUST carry the client's id, never the upstream-side id the
	// registry minted internally.
	if string(resp.ID) != string(id) {
		t.Fatalf("tools/call response id = %s, want client id %s (upstream id leaked?)", resp.ID, id)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	// FAKE_ECHO=1 makes the fakeserver echo the arguments back as text content,
	// proving the call reached the right upstream with the arguments intact.
	if !strings.Contains(string(resp.Result), "golang") {
		t.Errorf("tools/call result did not echo arguments through: %s", resp.Result)
	}
}

func TestStdioUnknownToolReturnsError(t *testing.T) {
	c, cancel, done := startServer(t, false)
	defer func() { cancel(); <-done }()

	id := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "nope__nope"}))
	resp := c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("error response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error == nil {
		t.Fatal("expected JSON-RPC error for unknown tool")
	}
}

func TestStdioUnknownMethodReturnsMethodNotFound(t *testing.T) {
	c, cancel, done := startServer(t, false)
	defer func() { cancel(); <-done }()

	c.request("does/not/exist", nil)
	resp := c.readResponse()
	if resp.Error == nil || resp.Error.Code != mcp.CodeMethodNotFound {
		t.Fatalf("want method-not-found error, got %+v", resp.Error)
	}
}

func TestStdioResourcesListIsEmpty(t *testing.T) {
	c, cancel, done := startServer(t, false)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodResourceList, nil)
	resp := c.readResponse()
	if resp.Error != nil {
		t.Fatalf("resources/list error: %v", resp.Error)
	}
	var res mcp.ResourceListResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode resources/list: %v", err)
	}
	if len(res.Resources) != 0 {
		t.Errorf("resources/list should be empty in Phase 1, got %d", len(res.Resources))
	}
}

// The call log records call metadata but never the arguments, which may carry
// secrets. This exercises the full transport → registry → audit path.
func TestStdioCallLogHasNoSecrets(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":  "web",
			"FAKE_TOOLS": "fetch",
			"FAKE_ECHO":  "1",
		}},
	}}
	var logBuf bytes.Buffer
	callLog := logging.NewCallLogWriter(&logBuf)

	c, cancel, done := startServerWithConfig(t, cfg, callLog)
	defer func() { cancel(); <-done }()

	const secret = "SUPER_SECRET_TOKEN_abc123"
	args := json.RawMessage(`{"authorization":"Bearer ` + secret + `"}`)
	c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__fetch", Arguments: args}))
	resp := c.readResponse()
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}

	logged := logBuf.String()
	if strings.Contains(logged, secret) {
		t.Fatalf("call log leaked secret:\n%s", logged)
	}
	if !strings.Contains(logged, `"tool":"web__fetch"`) || !strings.Contains(logged, `"upstream":"web"`) {
		t.Fatalf("call log missing expected metadata:\n%s", logged)
	}
}

// A call that outlives the configured timeout must be cancelled and surfaced as
// an error to the client rather than hanging. The upstream delays only its
// tools/call reply (FAKE_CALL_DELAY), so the handshake still completes well
// within the timeout and the upstream registers normally — it is the call
// itself that trips reg.CallTool's EffectiveCallTimeout.
func TestStdioCallTimeoutSurfacesError(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		CallTimeout: 300 * time.Millisecond,
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":       "web",
				"FAKE_TOOLS":      "fetch",
				"FAKE_CALL_DELAY": "3s",
			}},
		},
	}
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	id := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__fetch"}))
	resp := c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("timeout error response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error == nil {
		t.Fatalf("expected an error from the timed-out call, got result: %s", resp.Result)
	}
}

func TestStdioClientDisconnectEndsServe(t *testing.T) {
	c, cancel, done := startServer(t, false)
	defer cancel()

	c.closeInput() // EOF on the server's input pipe
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error on clean disconnect: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after client disconnect")
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
