package transport

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// rootsClientCaps is what a roots-capable client declares: the sub-flag is
// what the gateway must mirror to its upstreams.
const rootsClientCaps = `{"roots":{"listChanged":true}}`

// answerRoots replies to one proxied roots/list request read from the server,
// returning the gateway id it carried.
func answerRoots(t *testing.T, c *fakeClient, msg *mcp.Message) string {
	t.Helper()
	if !msg.IsRequest() || msg.Method != mcp.MethodRootsList {
		t.Fatalf("expected a roots/list request, got method=%q id=%s err=%+v result=%s (upstream skipped — was roots declared?)",
			msg.Method, msg.ID, msg.Error, msg.Result)
	}
	var gatewayID string
	if err := json.Unmarshal(msg.ID, &gatewayID); err != nil || !strings.HasPrefix(gatewayID, "roots-") {
		t.Fatalf("roots/list id = %s, want a gateway-minted string id with the \"roots-\" prefix", msg.ID)
	}
	if err := c.w.Write(mcp.NewResult(msg.ID, json.RawMessage(`{"roots":[{"uri":"file:///w","name":"w"}]}`))); err != nil {
		t.Fatalf("client write roots answer: %v", err)
	}
	return gatewayID
}

// TestStdioRootsRoundTrip (D9) drives the full upstream→client→upstream cycle
// for roots/list against a real child process.
func TestStdioRootsRoundTrip(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	cfg := capsUpstream(t, capsFile, map[string]string{"FAKE_ROOTS": "1"})
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(rootsClientCaps),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}
	// The declaration must carry the sub-flag: the gateway really does relay
	// the client's list_changed (asserted by D11).
	if got := lastCaps(t, capsFile); string(got["roots"]) != `{"listChanged":true}` {
		t.Errorf("declared roots = %s, want {\"listChanged\":true}", got["roots"])
	}

	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	answerRoots(t, c, c.readResponse())

	resp := c.readResponse()
	if string(resp.ID) != string(callID) {
		t.Fatalf("reply id = %s, want the call's %s", resp.ID, callID)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "roots=") ||
		!strings.Contains(string(resp.Result), "file:///w") {
		t.Errorf("tool result did not embed the roots answer: %s", resp.Result)
	}
}

// TestStdioRootsSingleFlightAndCacheAcrossUpstreams (D10) is the criterion a
// multiplexer lives or dies by: N upstreams asking the same question must cost
// the client ONE. Three tools/call across two upstreams, and the client must
// see exactly one roots/list for all of them.
//
// The first two calls are fired CONCURRENTLY, before anything is answered, so
// their roots questions overlap in flight — that is single-flight, and it is
// what the earlier strictly-sequential version of this test could not
// distinguish from plain caching (found by review). The third goes out after
// the answer landed, which is the cache. Both are asserted by the same count.
func TestStdioRootsSingleFlightAndCacheAcrossUpstreams(t *testing.T) {
	binPath := buildFakeServer(t)
	env := func(name string) map[string]string {
		return map[string]string{"FAKE_NAME": name, "FAKE_TOOLS": "ask", "FAKE_ROOTS": "1"}
	}
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "one", Command: binPath, Enabled: boolPtr(true), Env: env("one")},
		{Name: "two", Command: binPath, Enabled: boolPtr(true), Env: env("two")},
	}}
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(rootsClientCaps),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}

	// Both calls go out before either is answered: each upstream asks for roots
	// while the other's question is still in flight.
	pending := map[string]bool{
		string(c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "one__ask"}))): true,
		string(c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "two__ask"}))): true,
	}

	rootsAsked := 0
	drain := func(what string) {
		for len(pending) > 0 {
			msg := c.readResponse()
			if msg.IsRequest() {
				rootsAsked++
				answerRoots(t, c, msg)
				continue
			}
			if msg.Error != nil {
				t.Fatalf("%s: id=%s error=%v", what, msg.ID, msg.Error)
			}
			if !strings.Contains(string(msg.Result), "roots=") ||
				!strings.Contains(string(msg.Result), "file:///w") {
				t.Errorf("%s (id=%s) did not embed the roots answer: %s", what, msg.ID, msg.Result)
			}
			delete(pending, string(msg.ID))
		}
	}
	drain("concurrent call")
	if rootsAsked != 1 {
		t.Fatalf("the client was asked for roots %d times by two concurrent calls, want exactly 1 — single-flight is broken",
			rootsAsked)
	}

	// A later call is served from the cache: still no new question.
	pending[string(c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "one__ask"})))] = true
	drain("cached call")
	if rootsAsked != 1 {
		t.Errorf("the client was asked for roots %d times in total, want exactly 1 — the cache did nothing",
			rootsAsked)
	}
}

// TestStdioRootsListChangedFanOutAndRefetch (D11): the client's
// notifications/roots/list_changed (a) reaches the upstream, which the gateway
// declared roots to, and (b) drops the cache, so the next call asks again.
func TestStdioRootsListChangedFanOutAndRefetch(t *testing.T) {
	dir := t.TempDir()
	capsFile := filepath.Join(dir, "caps")
	changedFile := filepath.Join(dir, "roots-changed")
	cfg := capsUpstream(t, capsFile, map[string]string{
		"FAKE_ROOTS":              "1",
		"FAKE_ROOTS_CHANGED_FILE": changedFile,
	})
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(rootsClientCaps),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}

	// Prime the cache.
	c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	firstGatewayID := answerRoots(t, c, c.readResponse())
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("first call error: %v", resp.Error)
	}

	c.notify(mcp.NotifRootsListChanged, nil)

	// (a) the fan-out reached the upstream.
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(changedFile)
		if err == nil && strings.TrimSpace(string(data)) != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("upstream never received the relayed notifications/roots/list_changed")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// (b) the cache was dropped: the next call asks the client again.
	c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	secondGatewayID := answerRoots(t, c, c.readResponse())
	if secondGatewayID == firstGatewayID {
		t.Errorf("the refetch reused the gateway id %q", secondGatewayID)
	}
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("second call error: %v", resp.Error)
	}
}

// TestStdioRootsNotDeclaredSoUpstreamNeverAsks (D12): a client that never
// declared roots means the gateway declares none, so a negotiation-respecting
// upstream never asks — and its tools/call succeeds untouched. The REFUSAL a
// rude upstream would get is a different assertion, pinned at the registry
// level by TestRootsRefusedWithoutClientCapability.
func TestStdioRootsNotDeclaredSoUpstreamNeverAsks(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	cfg := capsUpstream(t, capsFile, map[string]string{"FAKE_ROOTS": "1"})
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{"elicitation":{}}`), // no roots
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}
	if got := lastCaps(t, capsFile); len(got) != 1 || got["roots"] != nil {
		t.Fatalf("declared %v, want exactly the client's {elicitation}", got)
	}

	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	resp := c.readResponse()
	if string(resp.ID) != string(callID) {
		t.Fatalf("first message id=%s method=%q, want the call reply %s (roots pushed to a client without the capability?)",
			resp.ID, resp.Method, callID)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "roots-skipped") {
		t.Errorf("upstream asked for roots (or the hook broke) despite no declaration: %s", resp.Result)
	}
}
