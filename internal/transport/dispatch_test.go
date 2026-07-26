package transport

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// mustDecode parses one raw JSON-RPC line into a message, failing the test on
// any decode error — these tests exercise dispatch classification, not framing.
func mustDecode(t *testing.T, line string) *mcp.Message {
	t.Helper()
	var m mcp.Message
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("decode %s: %v", line, err)
	}
	return &m
}

// The classification tests below never reach a method handler, so the
// dispatcher needs no live registry — nil is safe here.

// TestDispatchIDWithoutMethodIsInvalidRequest: a message with an id but no
// method is not a notification (has an id), not a response (no result/error)
// and not a request (no method). JSON-RPC 2.0 requires answering such a
// request-shaped message with -32600 Invalid Request under that id, not
// dropping it silently.
func TestDispatchIDWithoutMethodIsInvalidRequest(t *testing.T) {
	d := newDispatcher(nil, quietLogger(), "test", true, false)
	reply := d.dispatch(context.Background(), mustDecode(t, `{"jsonrpc":"2.0","id":7}`))
	if reply == nil {
		t.Fatal("message with id but no method was silently dropped, want -32600 error")
	}
	if string(reply.ID) != "7" {
		t.Errorf("reply id = %s, want 7", reply.ID)
	}
	if reply.Error == nil || reply.Error.Code != mcp.CodeInvalidRequest {
		t.Fatalf("reply error = %+v, want code %d", reply.Error, mcp.CodeInvalidRequest)
	}
}

// TestDispatchClientResponseIsIgnored: a genuine response from the client
// (result present, no method) is unexpected in the server role but legitimate
// on the wire — it must be ignored (nil), never answered with an error.
func TestDispatchClientResponseIsIgnored(t *testing.T) {
	d := newDispatcher(nil, quietLogger(), "test", true, false)
	if reply := d.dispatch(context.Background(), mustDecode(t, `{"jsonrpc":"2.0","id":7,"result":{}}`)); reply != nil {
		t.Fatalf("client response must be ignored, got reply %+v", reply)
	}
}

// TestDispatchNotificationNeedsNoReply: a notification (no id) produces no
// reply — unchanged by the missing-method check above.
func TestDispatchNotificationNeedsNoReply(t *testing.T) {
	d := newDispatcher(nil, quietLogger(), "test", true, false)
	if reply := d.dispatch(context.Background(), mustDecode(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)); reply != nil {
		t.Fatalf("notification must produce no reply, got %+v", reply)
	}
}

// TestDispatchPingAnswersEmptyResult: ping is the one request allowed at ANY
// time — including before initialize — so the dispatcher answers it with an
// empty result without consulting the registry at all (nil here) and never
// with method-not-found.
func TestDispatchPingAnswersEmptyResult(t *testing.T) {
	d := newDispatcher(nil, quietLogger(), "test", true, false)
	reply := d.dispatch(context.Background(), mustDecode(t, `{"jsonrpc":"2.0","id":11,"method":"ping"}`))
	if reply == nil {
		t.Fatal("ping was not answered")
	}
	if reply.Error != nil {
		t.Fatalf("ping answered with error %+v, want empty result", reply.Error)
	}
	if string(reply.ID) != "11" {
		t.Errorf("ping reply id = %s, want 11", reply.ID)
	}
	if string(reply.Result) != "{}" {
		t.Errorf("ping result = %s, want {}", reply.Result)
	}
}

// TestBuildCapabilitiesMatchesFormerLiteral pins the byte-level compatibility
// of the structural capability builder with the raw literals it replaced: this
// round it must produce exactly the tools-only object, per listChanged flavour.
func TestBuildCapabilitiesMatchesFormerLiteral(t *testing.T) {
	if got := string(buildCapabilities(nil, true)); got != `{"tools":{"listChanged":true}}` {
		t.Errorf("buildCapabilities(true) = %s", got)
	}
	if got := string(buildCapabilities(nil, false)); got != `{"tools":{"listChanged":false}}` {
		t.Errorf("buildCapabilities(false) = %s", got)
	}
}

// TestClientStringTolerantParse covers the identity extraction the transports
// attach to contexts: happy path, empty clientInfo, malformed params — the
// last two must yield "" (never an error, never a panic).
func TestClientStringTolerantParse(t *testing.T) {
	cases := []struct {
		name   string
		params string
		want   string
	}{
		{"full", `{"protocolVersion":"x","clientInfo":{"name":"claude-code","version":"2.1"}}`, "claude-code/2.1"},
		{"empty clientInfo", `{"protocolVersion":"x","clientInfo":{}}`, ""},
		{"no params", ``, ""},
		{"malformed", `[42]`, ""},
		{"name only", `{"clientInfo":{"name":"solo"}}`, "solo/"},
	}
	for _, tc := range cases {
		var raw json.RawMessage
		if tc.params != "" {
			raw = json.RawMessage(tc.params)
		}
		if got := clientString(raw); got != tc.want {
			t.Errorf("%s: clientString(%s) = %q, want %q", tc.name, tc.params, got, tc.want)
		}
	}
}
