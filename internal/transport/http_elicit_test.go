package transport

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// Stage 17a REPLACED the test that used to live here.
//
// TestHTTPGatewayDoesNotDeclareElicitationToUpstream pinned the Round 14
// limitation: the HTTP transport had no channel for gateway→client requests,
// so it never called SetClientServerRequestCaps and the gateway declared
// nothing to its upstreams even when the HTTP client itself declared
// elicitation. That behaviour is exactly what this stage removes, so the test
// is gone rather than weakened — see http_serverreq_test.go for the round trip
// that replaces it (E1) and TestHTTPLazyStartPinsFirstClientCaps for the
// handshake bytes it used to assert the negative of.
//
// What remains here is the half of that assertion which is still TRUE and
// still worth pinning: a client that declares NOTHING must still see no
// elicitation declared upstream, and the upstream must skip asking. It is the
// negative control for the whole stage — the pinning in ensureRegistry takes
// the first client's set, whatever that set is, including empty.

// TestHTTPUndeclaringClientGetsNoElicitation drives a full gateway whose first
// (and only) client declares no capabilities at all: the upstream handshake
// must carry an empty capabilities object, and the negotiation-respecting
// fakeserver must answer its tools/call with the |elicit-skipped marker rather
// than asking anyone.
func TestHTTPUndeclaringClientGetsNoElicitation(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	sid := gw.initialize(t, `{}`) // no elicitation key
	open := gw.openStream(t, sid)
	defer open.close()

	msg := gw.call(t, sid, "web__ask")
	if msg.Error != nil {
		t.Fatalf("tools/call error: %v", msg.Error)
	}
	if !strings.Contains(string(msg.Result), "elicit-skipped") {
		t.Errorf("upstream asked (or the hook broke) although the client declared nothing: %s", msg.Result)
	}

	// The direct record: the handshake's capabilities object must not carry the
	// elicitation key at all (today it is exactly {}).
	data, err := os.ReadFile(gw.capsFile)
	if err != nil {
		t.Fatalf("read caps file: %v", err)
	}
	if strings.Contains(string(data), "elicitation") {
		t.Errorf("gateway declared elicitation on behalf of a client that declared none: %s", data)
	}
}

// TestHTTPRootsListChangedReachesUpstreams pins the side effect of turning the
// dispatcher's serverRequests flag on for HTTP: a client's
// notifications/roots/list_changed is now relayed to every upstream the gateway
// declared roots to, exactly as over stdio. Before Stage 17a the HTTP
// dispatcher dropped it on the floor.
func TestHTTPRootsListChangedReachesUpstreams(t *testing.T) {
	cfg, changedFile := rootsChangedCfg(t)
	gw := startServerReqGateway(t, cfg)
	defer gw.stop()

	sid := gw.initialize(t, `{"roots":{"listChanged":true}}`)
	resp := gw.post(t, mcp.NewNotification(mcp.NotifRootsListChanged, nil), sid)
	_ = resp.Body.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(changedFile)
		if err == nil && strings.Contains(string(data), "roots-changed") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("upstream never received the client's notifications/roots/list_changed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
