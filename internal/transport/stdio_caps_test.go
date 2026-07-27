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
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// Stage 15, part B: the gateway declares to its stdio upstreams exactly the
// server→client capabilities its OWN client declared, which is only possible
// because the registry now starts LAZILY — after the client's initialize has
// been parsed. Both halves are pinned here from the outside, through the raw
// record of what each upstream's handshake actually carried (FAKE_CAPS_FILE).

// capsUpstream builds a one-upstream config whose fakeserver records the raw
// capabilities object of every initialize it receives into capsFile. extraEnv
// is merged in (FAKE_ELICIT & co.).
func capsUpstream(t *testing.T, capsFile string, extraEnv map[string]string) *config.Config {
	t.Helper()
	env := map[string]string{
		"FAKE_NAME":      "web",
		"FAKE_TOOLS":     "ask",
		"FAKE_CAPS_FILE": capsFile,
	}
	for k, v := range extraEnv {
		env[k] = v
	}
	return &config.Config{Upstreams: []config.Upstream{
		{Name: "web", Command: bin(t), Enabled: boolPtr(true), Env: env},
	}}
}

// bin compiles the fakeserver once per test (buildFakeServer already caches
// nothing, but the call is cheap enough and keeps capsUpstream readable).
func bin(t *testing.T) string {
	t.Helper()
	return buildFakeServer(t)
}

// capsLines reads the handshake records the fakeserver appended: one raw
// capabilities object per initialize it served. A missing file means no
// handshake happened at all, which is a legitimate expectation here (the
// registry may not have started yet), so it is reported as zero lines, not a
// failure.
func capsLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read caps file: %v", err)
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, strings.TrimSpace(line))
		}
	}
	return out
}

// lastCaps returns the capabilities object of the most recent handshake,
// decoded. It fails the test when no handshake was recorded.
func lastCaps(t *testing.T, path string) map[string]json.RawMessage {
	t.Helper()
	lines := capsLines(t, path)
	if len(lines) == 0 {
		t.Fatalf("no upstream handshake recorded in %s", path)
	}
	var caps map[string]json.RawMessage
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &caps); err != nil {
		t.Fatalf("decode declared capabilities %q: %v", lines[len(lines)-1], err)
	}
	return caps
}

// TestStdioDeclaresOnlyClientDeclaredCaps (B1) is the honesty criterion: what
// reaches an upstream equals what the client declared — not "all of them", not
// "none", and, for the one capability with a sub-flag, not a stronger claim
// than the client's own.
//
// roots is that capability. The gateway may promise listChanged only when the
// client promised it, because the fan-out is a RELAY of the client's
// notifications/roots/list_changed — the two bottom cases are the same client
// support with and without that promise, and they must not produce the same
// handshake (review И-3).
func TestStdioDeclaresOnlyClientDeclaredCaps(t *testing.T) {
	tests := []struct {
		name         string
		clientCaps   string
		wantDeclared map[string]string // capability → exact rendered value
	}{
		{
			"all three declared",
			`{"elicitation":{},"sampling":{},"roots":{"listChanged":true}}`,
			map[string]string{"elicitation": "{}", "sampling": "{}", "roots": `{"listChanged":true}`},
		},
		{
			"only elicitation declared",
			`{"elicitation":{}}`,
			map[string]string{"elicitation": "{}"},
		},
		{
			"nothing declared",
			`{}`,
			map[string]string{},
		},
		{
			"roots WITH listChanged is mirrored",
			`{"roots":{"listChanged":true}}`,
			map[string]string{"roots": `{"listChanged":true}`},
		},
		{
			"roots WITHOUT listChanged is not upgraded",
			`{"roots":{}}`,
			map[string]string{"roots": "{}"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capsFile := filepath.Join(t.TempDir(), "caps")
			c, cancel, done := startServerWithConfig(t, capsUpstream(t, capsFile, nil), nil)
			defer func() { cancel(); <-done }()

			c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
				ProtocolVersion: mcp.ProtocolVersion,
				Capabilities:    json.RawMessage(tt.clientCaps),
				ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
			}))
			if resp := c.readResponse(); resp.Error != nil {
				t.Fatalf("initialize error: %v", resp.Error)
			}

			got := lastCaps(t, capsFile)
			if len(got) != len(tt.wantDeclared) {
				t.Fatalf("declared %v, want exactly %v", got, tt.wantDeclared)
			}
			for name, want := range tt.wantDeclared {
				v, ok := got[name]
				if !ok {
					t.Errorf("capability %q missing from the handshake: %v", name, got)
					continue
				}
				if string(v) != want {
					t.Errorf("capability %q = %s, want %s", name, v, want)
				}
			}
		})
	}
}

// TestStdioRegistryStartsOnlyAfterInitializeParsed (B2) is the direct proof of
// the new order: no upstream handshake exists until the client has spoken,
// because the gateway cannot know what to declare before then.
func TestStdioRegistryStartsOnlyAfterInitializeParsed(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	c, cancel, done := startServerWithConfig(t, capsUpstream(t, capsFile, nil), nil)
	defer func() { cancel(); <-done }()

	// Generous window: a started registry would have handshaked long before.
	time.Sleep(500 * time.Millisecond)
	if lines := capsLines(t, capsFile); len(lines) != 0 {
		t.Fatalf("upstream handshaked before the client's initialize: %v", lines)
	}

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{"sampling":{}}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}

	got := lastCaps(t, capsFile)
	if _, ok := got["sampling"]; !ok || len(got) != 1 {
		t.Errorf("declared %v, want exactly the client's {sampling}", got)
	}
}

// TestStdioPingBeforeInitializeDoesNotFreezeCaps (B3): ping is the one request
// the spec allows before the handshake, and it consults nothing — so it must
// NOT start the registry. Were it to, a client that pings first would freeze
// the declaration set at empty for the whole session and the feature would die
// silently (Stage 15 §4.2).
func TestStdioPingBeforeInitializeDoesNotFreezeCaps(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	c, cancel, done := startServerWithConfig(t, capsUpstream(t, capsFile, nil), nil)
	defer func() { cancel(); <-done }()

	pingID := c.request(mcp.MethodPing, nil)
	resp := c.readResponse()
	if string(resp.ID) != string(pingID) || resp.Error != nil || string(resp.Result) != "{}" {
		t.Fatalf("ping before initialize: id=%s err=%+v result=%s", resp.ID, resp.Error, resp.Result)
	}
	if lines := capsLines(t, capsFile); len(lines) != 0 {
		t.Fatalf("ping started the registry: %v", lines)
	}

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{"elicitation":{}}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}
	if _, ok := lastCaps(t, capsFile)["elicitation"]; !ok {
		t.Errorf("a ping-first client lost its elicitation declaration: %v", lastCaps(t, capsFile))
	}
}

// TestStdioRequestBeforeInitializeDeclaresNothing (B4): a non-conformant client
// that asks for the catalog before handshaking still gets the full aggregated
// catalog (the pre-Stage-15 behaviour), and the upstreams simply see the
// historical empty declaration — nothing was known to declare.
func TestStdioRequestBeforeInitializeDeclaresNothing(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	c, cancel, done := startServerWithConfig(t, capsUpstream(t, capsFile, nil), nil)
	defer func() { cancel(); <-done }()

	listID := c.request(mcp.MethodToolsList, nil)
	resp := c.readResponse()
	if string(resp.ID) != string(listID) || resp.Error != nil {
		t.Fatalf("tools/list before initialize: id=%s err=%+v", resp.ID, resp.Error)
	}
	var res mcp.ToolsListResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode tools/list result: %v", err)
	}
	if len(res.Tools) != 1 || res.Tools[0].Name != "web__ask" {
		t.Errorf("catalog = %+v, want the aggregated web__ask", res.Tools)
	}
	if got := lastCaps(t, capsFile); len(got) != 0 {
		t.Errorf("declared %v to an upstream started before any handshake, want exactly {}", got)
	}
}

// TestStdioStartFailureAnsweredOnInitialize (B5): when the lazy bring-up fails
// (here: the only upstream's command does not exist, so zero live connections
// remain — a misconfiguration, not a degraded mode), the client gets a
// diagnosable JSON-RPC -32603 under its own initialize id instead of an abrupt
// EOF, and Serve still returns the error so the process exits with it.
func TestStdioStartFailureAnsweredOnInitialize(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "web", Command: "definitely-not-a-real-binary-xyz", Enabled: boolPtr(true)},
	}}
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer cancel()

	id := c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{"elicitation":{}}`),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	resp := c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("reply id = %s, want the initialize id %s", resp.ID, id)
	}
	if resp.Error == nil {
		t.Fatalf("initialize succeeded despite a failed bring-up: %s", resp.Result)
	}
	if resp.Error.Code != mcp.CodeInternalError {
		t.Errorf("error code = %d, want %d", resp.Error.Code, mcp.CodeInternalError)
	}
	// Sanitized: no upstream name, no internal error text (same policy as
	// handleToolsCall). The details belong in the operational log.
	if strings.Contains(resp.Error.Message, "definitely-not-a-real-binary-xyz") {
		t.Errorf("error message leaks upstream internals: %q", resp.Error.Message)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Error("Serve returned nil after a failed registry start, want the error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after a failed registry start")
	}
}

// TestStdioServerRequestBeforeHandshakeIsRefused (review И-2) drives the one
// branch that used to drop a proxied server→client request silently: it arrives
// while the client has not initialized. A drop there would leave the registry's
// pending entry parked forever — for roots/list that single entry wedges the
// single-flight for the life of the process — so the transport must ROLL IT
// BACK and refuse the upstream instead.
//
// The situation cannot arise through the client pipe alone: the runtime
// capability gate is fed from an initialize, and the registry does not even
// start before one. So the gate is opened out of band, after a non-conformant
// tools/list has brought the registry up, and the upstream is the "rude" kind
// that asks without seeing a declaration. What proves the refusal landed is the
// upstream's own tool result: it carries the decline it received, where a
// silent drop would leave it waiting out its own 10s timeout and answering
// |elicit-timeout instead.
func TestStdioServerRequestBeforeHandshakeIsRefused(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	cfg := capsUpstream(t, capsFile, map[string]string{"FAKE_ELICIT_FORCE": "1"})
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	c, cancel, done := startServerWithRegistry(t, cfg, reg)
	defer func() { cancel(); <-done }()

	// A request that needs the catalog but is NOT an initialize: it brings the
	// registry up (declaring exactly {}, since nothing is known) and leaves the
	// client uninitialized — the state this test needs.
	listID := c.request(mcp.MethodToolsList, nil)
	if resp := c.readResponse(); string(resp.ID) != string(listID) || resp.Error != nil {
		t.Fatalf("tools/list before initialize: id=%s err=%+v", resp.ID, resp.Error)
	}

	// Open the runtime gate the way an initialize would have, so the registry
	// publishes the rude upstream's request instead of refusing it at the gate.
	reg.SetClientServerRequestCaps(map[string]json.RawMessage{mcp.CapElicitation: json.RawMessage(`{}`)})

	start := time.Now()
	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	resp := c.readResponse()
	elapsed := time.Since(start)

	// Nothing was pushed to the un-initialized client: the first frame back is
	// the call's own reply.
	if string(resp.ID) != string(callID) {
		t.Fatalf("first message id=%s method=%q, want the call reply %s — a request was pushed before the handshake",
			resp.ID, resp.Method, callID)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), `\"action\":\"decline\"`) {
		t.Errorf("upstream was not refused, it got %s — a silently dropped request leaves it hanging (and its entry leaked)",
			resp.Result)
	}
	if elapsed > 5*time.Second {
		t.Errorf("the call took %v — the upstream waited out its own timeout instead of being refused", elapsed)
	}
}

// TestStdioElicitationFromRudeUpstreamDeclined (B6) keeps the refusal path
// covered now that the honest declaration made it unreachable through a
// well-behaved upstream. FAKE_ELICIT_FORCE models a server that ignores the
// negotiation and asks anyway: the gateway must answer {"action":"decline"}
// immediately — a RESULT, so the tool call still succeeds — and must push
// nothing to a client that never declared the capability.
func TestStdioElicitationFromRudeUpstreamDeclined(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	cfg := capsUpstream(t, capsFile, map[string]string{"FAKE_ELICIT_FORCE": "1"})
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`), // declares nothing
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	}))
	if resp := c.readResponse(); resp.Error != nil {
		t.Fatalf("initialize error: %v", resp.Error)
	}
	if got := lastCaps(t, capsFile); len(got) != 0 {
		t.Fatalf("declared %v to the upstream, want exactly {} — the rude case needs no declaration", got)
	}

	start := time.Now()
	callID := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: "web__ask"}))
	resp := c.readResponse()
	elapsed := time.Since(start)

	// Nothing was pushed to the client: the first frame is the call's reply.
	if string(resp.ID) != string(callID) {
		t.Fatalf("first message id=%s method=%q, want the call reply %s (request pushed to an incapable client?)",
			resp.ID, resp.Method, callID)
	}
	if resp.Error != nil {
		t.Fatalf("tools/call error: %v", resp.Error)
	}
	if strings.Contains(string(resp.Result), "elicit-error") ||
		strings.Contains(string(resp.Result), `"isError":true`) {
		t.Fatalf("the gateway answered with an error, failing a call that must survive: %s", resp.Result)
	}
	// The decline reached the upstream: the marker rides inside the result's
	// text string, so the embedded ElicitResult arrives JSON-escaped.
	if !strings.Contains(string(resp.Result), "elicited=") ||
		!strings.Contains(string(resp.Result), `\"action\":\"decline\"`) {
		t.Errorf("upstream did not receive an immediate decline: %s", resp.Result)
	}
	if elapsed > 5*time.Second {
		t.Errorf("decline took %v — the upstream waited instead of being answered immediately", elapsed)
	}
}
