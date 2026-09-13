// Stage 19b: the stdio modern (2026-07-28) path — the gateway declares to its
// upstreams the clientCapabilities carried in the FIRST modern request's _meta
// (the modern counterpart of reading them from initialize), and it pushes no
// unsolicited notifications to a modern client (the initialized gate stays
// false for a client that never sends notifications/initialized).

package transport

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// TestModernCapsDeclaredUpstream (stdio): a modern tools/list is the first
// request; the upstream's recorded handshake must show exactly the
// clientCapabilities the request's _meta declared. Mutation: pass nil caps into
// the lazy start on the modern path → the upstream sees {} → red.
func TestModernCapsDeclaredUpstream(t *testing.T) {
	capsFile := filepath.Join(t.TempDir(), "caps")
	c, cancel, done := startServerWithConfig(t, capsUpstream(t, capsFile, nil), nil)
	defer func() { cancel(); <-done }()

	// A modern request whose _meta declares elicitation+sampling. It is NOT an
	// initialize — the modern era has none — so this is the only place those
	// capabilities can come from.
	params := modernParams(t, mcp.ProtocolVersionModern, `{"elicitation":{},"sampling":{}}`, nil)
	id := c.request(mcp.MethodToolsList, params)
	resp := c.readResponse()
	if string(resp.ID) != string(id) || resp.Error != nil {
		t.Fatalf("modern tools/list: id=%s err=%+v", resp.ID, resp.Error)
	}

	got := lastCaps(t, capsFile)
	if _, ok := got["elicitation"]; !ok {
		t.Errorf("upstream handshake missing elicitation: %v", got)
	}
	if _, ok := got["sampling"]; !ok {
		t.Errorf("upstream handshake missing sampling: %v", got)
	}
	if len(got) != 2 {
		t.Errorf("upstream declared %v, want exactly the modern client's {elicitation, sampling}", got)
	}
}

// TestModernNoUnsolicitedNotifications (stdio): a modern client that sends a
// modern request but never notifications/initialized must NOT be pushed an
// unsolicited notifications/tools/list_changed when the catalog changes — the
// modern spec delivers list_changed only over a subscription (19d), and the
// initialized gate correctly stays false for such a client. Mutation: drop the
// initialized gate → the push escapes → the next frame is a notification → red.
func TestModernNoUnsolicitedNotifications(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			MaxAttempts:    5,
		},
		Upstreams: []config.Upstream{
			{Name: "crasher", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	// A modern tools/call crashes the upstream (FAKE_EXIT_AFTER=1), triggering an
	// auto-restart and a catalog change. The client never sent
	// notifications/initialized (modern has none), so it must receive ONLY the
	// call reply — never a pushed list_changed.
	callID := c.request(mcp.MethodToolsCall, modernParams(t, mcp.ProtocolVersionModern, "",
		map[string]json.RawMessage{"name": mcp.MustParams("crasher__ping")}))
	callResp := c.readResponse()
	if string(callResp.ID) != string(callID) {
		t.Fatalf("first frame id=%s method=%q, want the call reply %s", callResp.ID, callResp.Method, callID)
	}
	if callResp.Error != nil {
		t.Fatalf("modern tools/call error: %+v", callResp.Error)
	}

	// Give the auto-restart time to fire its catalog change; then a follow-up
	// modern request's reply must be the very NEXT frame — nothing pushed ahead
	// of it. If the initialized gate had leaked, a list_changed notification
	// would sit in front of this reply.
	time.Sleep(300 * time.Millisecond)
	pingID := c.request(mcp.MethodServerDiscover, modernParams(t, mcp.ProtocolVersionModern, "", nil))
	next := c.readResponse()
	if next.IsNotification() {
		t.Fatalf("an unsolicited notification (%q) was pushed to a modern client", next.Method)
	}
	if string(next.ID) != string(pingID) {
		t.Fatalf("next frame id=%s, want the discover reply %s — something was pushed", next.ID, pingID)
	}
}
