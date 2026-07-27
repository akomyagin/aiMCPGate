package registry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// elicitRecordingUpstream records every RespondUpstreamRequest into a channel
// so tests can await the (asynchronous) write-back without sleeping.
type elicitRecordingUpstream struct {
	fakeUpstreamBase
	responses chan *mcp.Message
}

func (f *elicitRecordingUpstream) RespondUpstreamRequest(msg *mcp.Message) error {
	f.responses <- msg
	return nil
}

// declaredCaps builds a client-capability set the way the client-facing
// transport does — name → the RAW value the client sent — for the many tests
// that only care THAT a capability was declared. The value is the commonest
// real one, {}. Tests that assert on the value itself (roots' listChanged
// sub-flag) spell the map out instead.
func declaredCaps(names ...string) map[string]json.RawMessage {
	caps := map[string]json.RawMessage{}
	for _, name := range names {
		caps[name] = json.RawMessage(`{}`)
	}
	return caps
}

// newElicitTestRegistry builds a registry with one recording fake conn wired
// in directly — the elicitation plumbing touches no catalog or lifecycle
// state, so no Start is needed (same rationale as newNotifTestRegistry).
func newElicitTestRegistry(name string) (*Registry, *elicitRecordingUpstream) {
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
	fake := &elicitRecordingUpstream{responses: make(chan *mcp.Message, 4)}
	r.mu.Lock()
	r.conns[name] = fake
	r.mu.Unlock()
	return r, fake
}

// TestElicitationRoundTripThroughRegistry (Round 14): an upstream's
// elicitation/create reaches the subscriber with params verbatim under a
// fresh gateway id, and the client's answer is routed back to that upstream
// with the id rewritten to the upstream's ORIGINAL — the two id spaces never
// leak into each other.
func TestElicitationRoundTripThroughRegistry(t *testing.T) {
	r, fake := newElicitTestRegistry("web")
	r.SetClientServerRequestCaps(declaredCaps("elicitation"))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(7)
	params := json.RawMessage(`{"message":"need input"}`)
	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, originalID, params)

	var req UpstreamRequest
	select {
	case req = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the elicitation request")
	}
	if req.GatewayID == "" || req.GatewayID == string(originalID) {
		t.Errorf("GatewayID = %q, want a fresh gateway-minted id distinct from the upstream's", req.GatewayID)
	}
	if string(req.Params) != string(params) {
		t.Errorf("params = %s, want verbatim %s", req.Params, params)
	}

	answer := mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(`{"action":"decline"}`))
	if !r.RouteUpstreamResponse(req.GatewayID, answer) {
		t.Fatal("RouteUpstreamResponse = false for a pending gateway id")
	}
	select {
	case got := <-fake.responses:
		if string(got.ID) != string(originalID) {
			t.Errorf("upstream received id %s, want the original %s", got.ID, originalID)
		}
		if string(got.Result) != `{"action":"decline"}` {
			t.Errorf("upstream received result %s, want the client's verbatim", got.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the answer never reached the upstream")
	}

	// A second answer under the same id is stale: the pending entry is gone.
	if r.RouteUpstreamResponse(req.GatewayID, answer) {
		t.Error("RouteUpstreamResponse = true for an already-consumed id, want false")
	}
}

// TestElicitationDeclinedWithoutClientCapability (Round 14, semantics fixed
// after review): with the client flag unset (the default — HTTP transports
// never set it), an upstream's elicitation/create is answered immediately
// with the spec's {"action":"decline"} RESULT under the upstream's ORIGINAL
// id — never a JSON-RPC error, which contradicts the declared capability and
// which SDKs turn into a failed tools/call — and nothing reaches the
// subscribers.
func TestElicitationDeclinedWithoutClientCapability(t *testing.T) {
	r, fake := newElicitTestRegistry("web")
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(3)
	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, originalID, json.RawMessage(`{"message":"need input"}`))

	select {
	case got := <-fake.responses:
		if string(got.ID) != string(originalID) {
			t.Errorf("decline id = %s, want the upstream's original %s", got.ID, originalID)
		}
		if got.Error != nil {
			t.Errorf("upstream got a JSON-RPC error %+v, want the decline result", got.Error)
		}
		if string(got.Result) != `{"action":"decline"}` {
			t.Errorf("decline result = %s, want exactly {\"action\":\"decline\"}", got.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never received the immediate decline")
	}
	select {
	case req := <-ch:
		t.Fatalf("elicitation reached the subscriber despite an incapable client: %+v", req)
	default:
	}
}

// TestElicitationUndeliveredAnsweredAndNotLeaked (review fix): when the
// publish reaches NO subscriber (here: none registered at all), the parked
// pendingServerReqs entry must be rolled back — RouteUpstreamResponse is its
// only other remover, so it would leak forever — and the upstream must get an
// immediate {"action":"decline"} under its ORIGINAL id instead of hanging to
// its timeout: nobody can ask the human, which is a decline, not an error
// (same user decision as the incapable-client path).
func TestElicitationUndeliveredAnsweredAndNotLeaked(t *testing.T) {
	r, fake := newElicitTestRegistry("web")
	r.SetClientServerRequestCaps(declaredCaps("elicitation")) // capable client, but nobody subscribed

	originalID := mcp.IntID(9)
	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, originalID, json.RawMessage(`{"message":"need input"}`))

	select {
	case got := <-fake.responses:
		if string(got.ID) != string(originalID) {
			t.Errorf("decline id = %s, want the upstream's original %s", got.ID, originalID)
		}
		if got.Error != nil {
			t.Errorf("upstream got a JSON-RPC error %+v, want the decline result", got.Error)
		}
		if string(got.Result) != `{"action":"decline"}` {
			t.Errorf("decline result = %s, want exactly {\"action\":\"decline\"}", got.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never received an answer for the undeliverable elicitation")
	}

	r.serverReqMu.Lock()
	pending := len(r.pendingServerReqs)
	r.serverReqMu.Unlock()
	if pending != 0 {
		t.Errorf("pendingServerReqs holds %d entries after an undelivered publish, want 0 (leak)", pending)
	}
}

// TestDropUpstreamClearsPendingElicits (review fix): removing an upstream from
// the registry must discard its parked elicitations — the answer could never
// be delivered anyway, and without cleanup the entries would accumulate
// forever across reloads/restarts.
func TestDropUpstreamClearsPendingElicits(t *testing.T) {
	r, _ := newElicitTestRegistry("web")
	r.SetClientServerRequestCaps(declaredCaps("elicitation"))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(1), json.RawMessage(`{}`))
	var req UpstreamRequest
	select {
	case req = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the elicitation request")
	}

	r.dropUpstream("web")

	r.serverReqMu.Lock()
	pending := len(r.pendingServerReqs)
	r.serverReqMu.Unlock()
	if pending != 0 {
		t.Errorf("pendingServerReqs holds %d entries after dropUpstream, want 0", pending)
	}
	if r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(`{}`))) {
		t.Error("RouteUpstreamResponse = true for a dropped upstream's elicitation, want false")
	}
}

// TestRouteUpstreamResponseUnknownID (Round 14): an answer whose id matches
// no pending elicitation (stale, duplicate, or plain garbage) is reported
// false — the caller falls back to its normal unexpected-response handling —
// and nothing panics.
func TestRouteUpstreamResponseUnknownID(t *testing.T) {
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
	msg := mcp.NewResult(mcp.StringID("elicit-999"), json.RawMessage(`{}`))
	if r.RouteUpstreamResponse("elicit-999", msg) {
		t.Error("RouteUpstreamResponse = true for an unknown gateway id, want false")
	}
}
