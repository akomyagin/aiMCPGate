package registry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// Stage 15, part C: sampling/createMessage rides the SAME pipeline
// elicitation does — one pending map, one subscription, one router — and
// differs only in the spec-table entry. These tests pin the two things that
// entry decides: the id prefix and the shape of the refusal (-32601, never a
// decline result).

// addRecordingUpstream wires one more recording fake conn into an existing
// registry, so a test can watch two upstreams' answers apart.
func addRecordingUpstream(r *Registry, name string) *elicitRecordingUpstream {
	fake := &elicitRecordingUpstream{responses: make(chan *mcp.Message, 4)}
	r.mu.Lock()
	r.conns[name] = fake
	r.mu.Unlock()
	return fake
}

// TestSamplingRoundTripThroughRegistry (C1): an upstream's
// sampling/createMessage reaches the subscriber with its params verbatim under
// a "sampling-" prefixed gateway id, and the client's answer goes back to that
// upstream under the upstream's ORIGINAL id. A second answer under the same id
// is stale.
func TestSamplingRoundTripThroughRegistry(t *testing.T) {
	r, fake := newElicitTestRegistry("web")
	r.SetClientServerRequestCaps(declaredCaps("sampling"))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(11)
	params := json.RawMessage(`{"messages":[{"role":"user","content":{"type":"text","text":"say hi"}}],"maxTokens":16}`)
	r.onUpstreamRequest("web", mcp.MethodSamplingCreateMessage, originalID, params)

	var req UpstreamRequest
	select {
	case req = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the sampling request")
	}
	if req.Method != mcp.MethodSamplingCreateMessage {
		t.Errorf("method = %q, want %q", req.Method, mcp.MethodSamplingCreateMessage)
	}
	if len(req.GatewayID) < len("sampling-") || req.GatewayID[:len("sampling-")] != "sampling-" {
		t.Errorf("GatewayID = %q, want the \"sampling-\" prefix", req.GatewayID)
	}
	if req.GatewayID == string(originalID) {
		t.Errorf("GatewayID = %q leaked the upstream's own id", req.GatewayID)
	}
	if string(req.Params) != string(params) {
		t.Errorf("params = %s, want verbatim %s", req.Params, params)
	}

	answer := mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(`{"role":"assistant","content":{"type":"text","text":"hi"},"model":"m"}`))
	if !r.RouteUpstreamResponse(req.GatewayID, answer) {
		t.Fatal("RouteUpstreamResponse = false for a pending gateway id")
	}
	select {
	case got := <-fake.responses:
		if string(got.ID) != string(originalID) {
			t.Errorf("upstream received id %s, want the original %s", got.ID, originalID)
		}
		if got.Error != nil {
			t.Errorf("upstream received an error %+v, want the client's result", got.Error)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the answer never reached the upstream")
	}

	if r.RouteUpstreamResponse(req.GatewayID, answer) {
		t.Error("RouteUpstreamResponse = true for an already-consumed id, want false")
	}
}

// TestSamplingRefusedWithoutClientCapability (C2): with no "sampling" in the
// client's set, the upstream gets JSON-RPC -32601 — NOT the elicitation-style
// decline result, which has no counterpart for sampling in the spec — and
// nothing reaches the subscribers.
func TestSamplingRefusedWithoutClientCapability(t *testing.T) {
	r, fake := newElicitTestRegistry("web")
	r.SetClientServerRequestCaps(declaredCaps("elicitation")) // sampling deliberately absent
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(5)
	r.onUpstreamRequest("web", mcp.MethodSamplingCreateMessage, originalID, json.RawMessage(`{"maxTokens":1}`))

	select {
	case got := <-fake.responses:
		if string(got.ID) != string(originalID) {
			t.Errorf("refusal id = %s, want the upstream's original %s", got.ID, originalID)
		}
		if got.Error == nil {
			t.Fatalf("upstream got result %s, want a JSON-RPC error", got.Result)
		}
		if got.Error.Code != mcp.CodeMethodNotFound {
			t.Errorf("refusal code = %d, want %d", got.Error.Code, mcp.CodeMethodNotFound)
		}
		if got.Result != nil {
			t.Errorf("refusal also carried a result %s, want none", got.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never received the immediate refusal")
	}
	select {
	case req := <-ch:
		t.Fatalf("sampling reached the subscriber despite an incapable client: %+v", req)
	default:
	}
}

// TestSamplingUndeliveredRefusedAndNotLeaked (C3): a publish that reaches NO
// subscriber must roll the parked entry back — RouteUpstreamResponse is its
// only other remover — and refuse the upstream immediately instead of leaving
// it to time out.
func TestSamplingUndeliveredRefusedAndNotLeaked(t *testing.T) {
	r, fake := newElicitTestRegistry("web")
	r.SetClientServerRequestCaps(declaredCaps("sampling")) // capable client, nobody subscribed

	originalID := mcp.IntID(9)
	r.onUpstreamRequest("web", mcp.MethodSamplingCreateMessage, originalID, json.RawMessage(`{}`))

	select {
	case got := <-fake.responses:
		if got.Error == nil || got.Error.Code != mcp.CodeMethodNotFound {
			t.Errorf("upstream got %+v / %s, want a -32601 error", got.Error, got.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upstream never received an answer for the undeliverable sampling request")
	}

	r.serverReqMu.Lock()
	pending := len(r.pendingServerReqs)
	r.serverReqMu.Unlock()
	if pending != 0 {
		t.Errorf("pendingServerReqs holds %d entries after an undelivered publish, want 0 (leak)", pending)
	}
}

// TestServerReqIDsDistinctAcrossMethods (C4) pins the id scheme: ONE counter
// serves every method and every upstream, so two requests of DIFFERENT methods
// from DIFFERENT upstreams get distinct gateway ids, and each answer finds its
// own upstream under its own original id. The prefixes are cosmetic — the
// uniqueness must not depend on them.
func TestServerReqIDsDistinctAcrossMethods(t *testing.T) {
	r, elicitFake := newElicitTestRegistry("web")
	samplingFake := addRecordingUpstream(r, "llm")
	r.SetClientServerRequestCaps(declaredCaps("elicitation", "sampling"))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	elicitOriginal := mcp.IntID(1)
	samplingOriginal := mcp.StringID("s-1")
	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, elicitOriginal, json.RawMessage(`{"message":"?"}`))
	r.onUpstreamRequest("llm", mcp.MethodSamplingCreateMessage, samplingOriginal, json.RawMessage(`{"maxTokens":1}`))

	byMethod := map[string]UpstreamRequest{}
	for i := 0; i < 2; i++ {
		select {
		case req := <-ch:
			byMethod[req.Method] = req
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of 2 requests reached the subscriber", i)
		}
	}
	elicitReq, ok1 := byMethod[mcp.MethodElicitationCreate]
	samplingReq, ok2 := byMethod[mcp.MethodSamplingCreateMessage]
	if !ok1 || !ok2 {
		t.Fatalf("subscriber saw methods %v, want both", byMethod)
	}
	if elicitReq.GatewayID == samplingReq.GatewayID {
		t.Fatalf("both requests got the same gateway id %q", elicitReq.GatewayID)
	}

	if !r.RouteUpstreamResponse(samplingReq.GatewayID, mcp.NewResult(mcp.StringID(samplingReq.GatewayID), json.RawMessage(`{"ok":true}`))) {
		t.Fatal("RouteUpstreamResponse = false for the pending sampling id")
	}
	if !r.RouteUpstreamResponse(elicitReq.GatewayID, mcp.NewResult(mcp.StringID(elicitReq.GatewayID), json.RawMessage(`{"action":"accept"}`))) {
		t.Fatal("RouteUpstreamResponse = false for the pending elicitation id")
	}

	select {
	case got := <-samplingFake.responses:
		if string(got.ID) != string(samplingOriginal) {
			t.Errorf("llm received id %s, want its original %s", got.ID, samplingOriginal)
		}
		if string(got.Result) != `{"ok":true}` {
			t.Errorf("llm received result %s, want the sampling answer", got.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the sampling answer never reached its upstream")
	}
	select {
	case got := <-elicitFake.responses:
		if string(got.ID) != string(elicitOriginal) {
			t.Errorf("web received id %s, want its original %s", got.ID, elicitOriginal)
		}
		if string(got.Result) != `{"action":"accept"}` {
			t.Errorf("web received result %s, want the elicitation answer", got.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the elicitation answer never reached its upstream")
	}
}

// TestDeclaredClientCapsFollowClientSet (B8, white-box): the set that goes on
// the wire is exactly the client's, rendered per the spec table — and the
// setter drives the runtime refusal gate as well, so there is genuinely ONE
// source of truth rather than the two flags Round 14 kept.
func TestDeclaredClientCapsFollowClientSet(t *testing.T) {
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")

	if got := r.declaredClientCaps(); len(got) != 0 {
		t.Errorf("a registry nobody told anything declares %v, want exactly {}", got)
	}

	r.SetClientServerRequestCaps(declaredCaps("elicitation"))
	got := r.declaredClientCaps()
	if len(got) != 1 || string(got["elicitation"]) != "{}" {
		t.Errorf("declared %v, want exactly {elicitation:{}}", got)
	}
	if !r.clientDeclared("elicitation") || r.clientDeclared("sampling") {
		t.Error("the runtime gate disagrees with the declared set")
	}

	r.SetClientServerRequestCaps(declaredCaps("elicitation", "sampling"))
	got = r.declaredClientCaps()
	if len(got) != 2 || string(got["sampling"]) != "{}" {
		t.Errorf("declared %v, want elicitation and sampling", got)
	}
	if !r.clientDeclared("sampling") {
		t.Error("a second setter call did not move the runtime gate")
	}

	// A caller's map must not be aliased: mutating it afterwards changes
	// nothing inside the registry.
	caller := declaredCaps("elicitation")
	r.SetClientServerRequestCaps(caller)
	caller["sampling"] = json.RawMessage(`{}`)
	if r.clientDeclared("sampling") {
		t.Error("the registry aliased the caller's map instead of copying it")
	}

	r.SetClientServerRequestCaps(nil)
	if got := r.declaredClientCaps(); len(got) != 0 {
		t.Errorf("declared %v after a nil set, want exactly {}", got)
	}
	if r.clientDeclared("elicitation") {
		t.Error("a nil set left the runtime gate open")
	}
}
