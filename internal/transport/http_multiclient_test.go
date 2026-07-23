// Stage 11 — multi-client verification (test, not feature; POST_MVP_TECHNICAL_PLAN
// "Этап 11"). These tests add ZERO production code: they exist to prove under
// -race that the HTTP transport is already safe for concurrent clients — the
// dispatcher holds no per-connection state, Registry.CallTool only RLocks for
// the route lookup, and the stdio transport's call multiplexes concurrent calls over one
// upstream pipe via atomic ids + mutex-guarded writes.
//
// Concurrency-test conventions used here:
//   - Worker goroutines NEVER call t.Fatal/t.Error (Fatal must run on the test
//     goroutine; runtime.Goexit from a worker would hang the WaitGroup). Workers
//     return errors; the main test goroutine reports them after Wait().
//   - Every request across ALL goroutines carries a globally unique JSON-RPC id
//     and every tools/call carries a globally unique fixed-width echo marker in
//     its arguments, both minted from one process-wide atomic counter (testIDs),
//     so any cross-client response mix-up is caught by content, not just by
//     absence of a race report.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// testIDs mints globally unique JSON-RPC ids and echo markers for every
// goroutine in every test of this file. One process-wide atomic counter makes
// uniqueness a structural guarantee instead of arithmetic over test constants
// (the old client*1000+seq scheme silently broke if a client ever issued 1000+
// requests).
var testIDs atomic.Int64

// nextTestID returns the next globally unique id.
func nextTestID() int64 { return testIDs.Add(1) }

// uniqueMarker formats the fixed-width echo marker for a nextTestID value. The
// fixed width matters: checkEchoCall matches via strings.Contains, and two
// distinct equal-length markers with the same prefix can never be substrings
// of one another (unlike e.g. "marker-1" vs "marker-10").
func uniqueMarker(id int64) string { return fmt.Sprintf("marker-%08d", id) }

// startHTTPGatewayMulti is startHTTPGateway's multi-upstream sibling: it wires
// n stdio fakeserver upstreams (named "up0".."up<n-1>", each advertising
// "search" and "fetch" with FAKE_ECHO=1) behind one httpServer.handleMCP
// handler in an httptest.Server. One fakeserver binary is built and reused for
// every upstream; only the env differs. It lives in this file (not http_test.go)
// so the single-upstream helper there stays untouched.
func startHTTPGatewayMulti(t *testing.T, n int) (*httptest.Server, func()) {
	t.Helper()
	bin := buildFakeServer(t)
	ups := make([]config.Upstream, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("up%d", i)
		ups = append(ups, config.Upstream{Name: name, Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_NAME":  name,
			"FAKE_TOOLS": "search,fetch",
			"FAKE_ECHO":  "1",
		}})
	}
	cfg := &config.Config{Transport: config.TransportHTTP, Upstreams: ups}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry Start: %v", err)
	}

	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", hs.handleMCP)
	srv := httptest.NewServer(mux)

	return srv, func() { srv.Close(); _ = reg.Close() }
}

// postErr is a goroutine-safe variant of post: it returns errors instead of
// calling t.Fatalf, because Fatal is only legal on the test goroutine and these
// helpers run inside worker goroutines.
func postErr(srv *httptest.Server, msg *mcp.Message) (*http.Response, error) {
	body, err := mcp.Encode(msg)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := srv.Client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST: %w", err)
	}
	return resp, nil
}

// roundTrip POSTs one request and decodes the single JSON-RPC reply, verifying
// the HTTP status and — crucially for multi-client correctness — that the reply
// carries EXACTLY the id this caller sent (any other id means the gateway mixed
// up responses between concurrent clients).
func roundTrip(srv *httptest.Server, msg *mcp.Message) (*mcp.Message, error) {
	resp, err := postErr(srv, msg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP status = %d, want 200", msg.Method, resp.StatusCode)
	}
	var out mcp.Message
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: decode response: %w", msg.Method, err)
	}
	if string(out.ID) != string(msg.ID) {
		return nil, fmt.Errorf("%s: response id = %s, want %s (responses mixed up between clients?)", msg.Method, out.ID, msg.ID)
	}
	return &out, nil
}

// clientCycle runs one full independent MCP client session against the shared
// gateway: initialize → notifications/initialized → tools/list → callsPerClient
// tools/call spread across upstreams. client is this session's index, used
// only for upstream rotation and error messages; JSON-RPC ids and echo markers
// come from the process-wide testIDs counter.
func clientCycle(srv *httptest.Server, client, numUpstreams, callsPerClient int) error {
	nextID := func() json.RawMessage { return mcp.IntID(nextTestID()) }

	// initialize — each "client" performs its own handshake with its own id.
	msg, err := roundTrip(srv, mcp.NewRequest(nextID(), mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      mcp.Implementation{Name: fmt.Sprintf("client-%d", client), Version: "1.0.0"},
	})))
	if err != nil {
		return fmt.Errorf("client %d: %w", client, err)
	}
	if msg.Error != nil {
		return fmt.Errorf("client %d: initialize error: %v", client, msg.Error)
	}
	var initRes mcp.InitializeResult
	if err := json.Unmarshal(msg.Result, &initRes); err != nil {
		return fmt.Errorf("client %d: decode initialize result: %w", client, err)
	}
	if initRes.ServerInfo.Name != "aiMCPGate" {
		return fmt.Errorf("client %d: serverInfo.Name = %q, want aiMCPGate", client, initRes.ServerInfo.Name)
	}

	// notifications/initialized — a notification, so the gateway must answer
	// 202 Accepted with no body (per the HTTP transport contract).
	resp, err := postErr(srv, mcp.NewNotification(mcp.NotifInitialized, nil))
	if err != nil {
		return fmt.Errorf("client %d: %w", client, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("client %d: notification status = %d, want 202", client, resp.StatusCode)
	}

	// tools/list — the aggregated catalog must be complete and consistent no
	// matter how many clients are reading it concurrently.
	msg, err = roundTrip(srv, mcp.NewRequest(nextID(), mcp.MethodToolsList, nil))
	if err != nil {
		return fmt.Errorf("client %d: %w", client, err)
	}
	if msg.Error != nil {
		return fmt.Errorf("client %d: tools/list error: %v", client, msg.Error)
	}
	var listRes mcp.ToolsListResult
	if err := json.Unmarshal(msg.Result, &listRes); err != nil {
		return fmt.Errorf("client %d: decode tools/list: %w", client, err)
	}
	got := make(map[string]bool, len(listRes.Tools))
	for _, tl := range listRes.Tools {
		got[tl.Name] = true
	}
	for i := 0; i < numUpstreams; i++ {
		for _, tool := range []string{"search", "fetch"} {
			want := fmt.Sprintf("up%d__%s", i, tool)
			if !got[want] {
				return fmt.Errorf("client %d: catalog missing %q (got %d tools)", client, want, len(listRes.Tools))
			}
		}
	}

	// tools/call — rotate over upstreams and tools so concurrent clients hit
	// different AND the same upstream connections; the echoed arguments must be
	// exactly THIS client's marker, never another goroutine's.
	tools := []string{"search", "fetch"}
	for j := 0; j < callsPerClient; j++ {
		name := fmt.Sprintf("up%d__%s", (client+j)%numUpstreams, tools[j%len(tools)])
		uid := nextTestID()
		if err := checkEchoCall(srv, mcp.IntID(uid), name, uniqueMarker(uid)); err != nil {
			return fmt.Errorf("client %d call %d: %w", client, j, err)
		}
	}
	return nil
}

// checkEchoCall performs one tools/call with a unique marker in the arguments
// and verifies the FAKE_ECHO=1 upstream echoed back exactly that marker — proof
// that the result belongs to this request, not to a concurrent one.
func checkEchoCall(srv *httptest.Server, id json.RawMessage, tool, marker string) error {
	msg, err := roundTrip(srv, mcp.NewRequest(id, mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{
		Name:      tool,
		Arguments: json.RawMessage(fmt.Sprintf(`{"marker":%q}`, marker)),
	})))
	if err != nil {
		return err
	}
	if msg.Error != nil {
		return fmt.Errorf("tools/call %s error: %v", tool, msg.Error)
	}
	if !strings.Contains(string(msg.Result), marker) {
		return fmt.Errorf("tools/call %s: result does not echo marker %q (cross-client mix-up?): %s", tool, marker, msg.Result)
	}
	return nil
}

// TestHTTPConcurrentClientsFullCycle is Stage 11's main verification: N
// independent "clients" (goroutines) each run a full MCP session concurrently
// against ONE shared gateway (one dispatcher, one registry, shared upstream
// connections). Every response must match its request by id and by echoed
// content. Its main value is running under -race: a data race anywhere in the
// shared dispatcher/registry/stdio transport path fails the run.
func TestHTTPConcurrentClientsFullCycle(t *testing.T) {
	const (
		numUpstreams   = 3
		numClients     = 24
		callsPerClient = 4
	)
	srv, cleanup := startHTTPGatewayMulti(t, numUpstreams)
	defer cleanup()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			if err := clientCycle(srv, client, numUpstreams, callsPerClient); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	for _, err := range errs {
		t.Error(err)
	}
}

// TestHTTPConcurrentCallsToSameUpstream stresses the single-upstream
// multiplexing path specifically: many concurrent tools/call from different
// "clients" all funnel into ONE stdio transport (one stdin pipe, one reader
// goroutine), exercising its atomic id minting, mutex-serialized writes and the
// waiters demux map under real contention. Each call must get back its own
// echoed marker with its own id.
func TestHTTPConcurrentCallsToSameUpstream(t *testing.T) {
	const (
		numClients     = 50
		callsPerClient = 4
	)
	srv, cleanup := startHTTPGatewayMulti(t, 1)
	defer cleanup()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			for j := 0; j < callsPerClient; j++ {
				uid := nextTestID()
				if err := checkEchoCall(srv, mcp.IntID(uid), "up0__search", uniqueMarker(uid)); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("client %d call %d: %w", client, j, err))
					mu.Unlock()
					return
				}
			}
		}(c)
	}
	wg.Wait()

	for _, err := range errs {
		t.Error(err)
	}
}
