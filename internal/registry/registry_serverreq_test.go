package registry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// Stage 15, review follow-up: a proxied server→client request must not be able
// to stay pending forever. Every test here shrinks Registry.serverReqTimeout —
// the production value is five minutes, which is the point (a human answers an
// elicitation form), and is exactly why it is a field and not the constant.

// testServerReqTimeout is short enough to keep the suite fast and long enough
// that the publish and the assertions before it are never racing the deadline.
const testServerReqTimeout = 80 * time.Millisecond

// awaitPublished takes the one request the pipeline published, failing the test
// if none arrives.
func awaitPublished(t *testing.T, ch <-chan UpstreamRequest, what string) UpstreamRequest {
	t.Helper()
	select {
	case req := <-ch:
		return req
	case <-time.After(2 * time.Second):
		t.Fatalf("subscriber never received %s", what)
		return UpstreamRequest{}
	}
}

// TestServerReqDeadlineRefusesInTheSpecShape (review И-1): a client that never
// answers must not leave the upstream hanging and the entry leaking. When the
// deadline passes the upstream is refused in the shape its spec entry
// prescribes — a soft {"action":"decline"} RESULT for elicitation, -32601 for
// sampling — under its OWN id, and the pending entry is gone.
func TestServerReqDeadlineRefusesInTheSpecShape(t *testing.T) {
	tests := []struct {
		name       string
		capability string
		method     string
		check      func(t *testing.T, msg *mcp.Message)
	}{
		{
			"elicitation declines", mcp.CapElicitation, mcp.MethodElicitationCreate,
			func(t *testing.T, msg *mcp.Message) {
				t.Helper()
				if msg.Error != nil || string(msg.Result) != `{"action":"decline"}` {
					t.Errorf("timed-out elicitation answered %+v / %s, want the decline RESULT",
						msg.Error, msg.Result)
				}
			},
		},
		{
			"sampling gets method-not-found", mcp.CapSampling, mcp.MethodSamplingCreateMessage,
			func(t *testing.T, msg *mcp.Message) {
				t.Helper()
				if msg.Error == nil || msg.Error.Code != mcp.CodeMethodNotFound {
					t.Errorf("timed-out sampling answered %+v / %s, want a -32601 error",
						msg.Error, msg.Result)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, fake := newElicitTestRegistry("web")
			r.serverReqTimeout = testServerReqTimeout
			r.SetClientServerRequestCaps(declaredCaps(tt.capability))
			ch, unsubscribe := r.SubscribeUpstreamRequests()
			defer unsubscribe()

			originalID := mcp.IntID(7)
			r.onUpstreamRequest("web", tt.method, originalID, json.RawMessage(`{"x":1}`))
			req := awaitPublished(t, ch, tt.method)

			got := awaitResponse(t, fake, "the deadline refusal")
			if string(got.ID) != string(originalID) {
				t.Errorf("refusal id = %s, want the upstream's own %s", got.ID, originalID)
			}
			tt.check(t, got)

			// The entry is gone, so a client answering LATE cannot produce a
			// second response to the same upstream request: the pending map is
			// the arbiter, and the deadline already won.
			late := mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(`{"action":"accept"}`))
			if r.RouteUpstreamResponse(req.GatewayID, late) {
				t.Error("a late client answer was still routed — the upstream would get two responses")
			}
			select {
			case extra := <-fake.responses:
				t.Errorf("the upstream received a second response: %+v", extra)
			case <-time.After(100 * time.Millisecond):
			}

			r.serverReqMu.Lock()
			pending := len(r.pendingServerReqs)
			r.serverReqMu.Unlock()
			if pending != 0 {
				t.Errorf("%d pending server→client request(s) left after the deadline, want 0", pending)
			}
		})
	}
}

// TestServerReqAnsweredInTimeIsNotRefused is the positive half: the deadline
// must not touch a request the client answers normally. The answer reaches the
// upstream verbatim and NOTHING follows it once the deadline has passed.
func TestServerReqAnsweredInTimeIsNotRefused(t *testing.T) {
	r, fake := newElicitTestRegistry("web")
	r.serverReqTimeout = testServerReqTimeout
	r.SetClientServerRequestCaps(declaredCaps(mcp.CapElicitation))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(7)
	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, originalID, json.RawMessage(`{"message":"?"}`))
	req := awaitPublished(t, ch, "the elicitation request")

	answer := json.RawMessage(`{"action":"accept","content":{"answer":"yes"}}`)
	if !r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), answer)) {
		t.Fatal("RouteUpstreamResponse = false for a pending gateway id")
	}
	got := awaitResponse(t, fake, "the client's answer")
	if string(got.Result) != string(answer) {
		t.Errorf("upstream got %s, want the client's verbatim %s", got.Result, answer)
	}

	select {
	case extra := <-fake.responses:
		t.Errorf("a refusal followed the answered request: %+v — the deadline fired anyway", extra)
	case <-time.After(3 * testServerReqTimeout):
	}
}

// TestRootsDeadlineUnwedgesSingleFlight (review И-1, the sharp case): roots is
// the one method where losing ONE answer used to break the feature for the
// whole process — rootsFetchID was cleared only on delivery, so every later
// asker parked behind a question nobody would ever answer and rootsWaiters grew
// without bound. The deadline must refuse everyone parked, wipe the
// single-flight state, and let the NEXT asker start a fresh question.
func TestRootsDeadlineUnwedgesSingleFlight(t *testing.T) {
	r, alpha, beta := rootsCapableRegistry(t)
	r.serverReqTimeout = testServerReqTimeout
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	alphaID, betaID := mcp.IntID(1), mcp.StringID("b-1")
	r.onUpstreamRequest("alpha", mcp.MethodRootsList, alphaID, nil)
	r.onUpstreamRequest("beta", mcp.MethodRootsList, betaID, nil)
	first := awaitPublished(t, ch, "the roots/list request")

	// Both parked upstreams are refused, each under its own original id.
	gotAlpha := awaitResponse(t, alpha, "alpha's roots refusal")
	if string(gotAlpha.ID) != string(alphaID) || gotAlpha.Error == nil ||
		gotAlpha.Error.Code != mcp.CodeMethodNotFound {
		t.Errorf("alpha got id=%s err=%+v, want %s / -32601", gotAlpha.ID, gotAlpha.Error, alphaID)
	}
	gotBeta := awaitResponse(t, beta, "beta's roots refusal")
	if string(gotBeta.ID) != string(betaID) || gotBeta.Error == nil ||
		gotBeta.Error.Code != mcp.CodeMethodNotFound {
		t.Errorf("beta got id=%s err=%+v, want %s / -32601", gotBeta.ID, gotBeta.Error, betaID)
	}

	r.rootsMu.Lock()
	fetchID, waiters, stale, cached := r.rootsFetchID, len(r.rootsWaiters), r.rootsStale, r.rootsCache
	r.rootsMu.Unlock()
	if fetchID != "" || waiters != 0 || stale || cached != nil {
		t.Errorf("after the deadline: rootsFetchID=%q waiters=%d stale=%v cache=%s, want all empty",
			fetchID, waiters, stale, cached)
	}

	// The wedge is gone: a later asker gets a NEW question rather than parking
	// behind the dead one.
	r.onUpstreamRequest("alpha", mcp.MethodRootsList, mcp.IntID(2), nil)
	second := awaitPublished(t, ch, "a fresh roots/list after the deadline")
	if second.GatewayID == first.GatewayID {
		t.Errorf("the fresh fetch reused the abandoned id %q", second.GatewayID)
	}
}

// TestRefusePendingServerReqBeforeHandshake (review И-2): the exported
// give-up a client-facing transport calls when it cannot deliver a request
// after all. It must roll the pending entry back AND refuse the upstream — a
// silent drop is what leaves the wedge above.
func TestRefusePendingServerReqBeforeHandshake(t *testing.T) {
	r, fake := newElicitTestRegistry("web")
	r.SetClientServerRequestCaps(declaredCaps(mcp.CapElicitation))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(7)
	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, originalID, json.RawMessage(`{"message":"?"}`))
	req := awaitPublished(t, ch, "the elicitation request")

	if !r.RefusePendingServerReq(req.GatewayID, "the client has not completed its handshake") {
		t.Fatal("RefusePendingServerReq = false for a pending gateway id")
	}
	got := awaitResponse(t, fake, "the refusal")
	if string(got.ID) != string(originalID) || string(got.Result) != `{"action":"decline"}` {
		t.Errorf("refusal id=%s result=%s err=%+v, want %s / the decline RESULT",
			got.ID, got.Result, got.Error, originalID)
	}

	// Idempotent and safe: the entry is gone, so a second giver-upper and a
	// late client answer both find nothing.
	if r.RefusePendingServerReq(req.GatewayID, "again") {
		t.Error("RefusePendingServerReq = true twice for the same id")
	}
	if r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(`{}`))) {
		t.Error("a late client answer was still routed after the request was refused")
	}
	if r.RefusePendingServerReq("elicit-never-existed", "unknown") {
		t.Error("RefusePendingServerReq = true for an id that was never pending")
	}
}

// TestRefusePendingServerReqUnwedgesRoots is the same give-up on the roots
// fetch: one call must unwind the whole single-flight, not just the map entry.
func TestRefusePendingServerReqUnwedgesRoots(t *testing.T) {
	r, alpha, _ := rootsCapableRegistry(t)
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(1)
	r.onUpstreamRequest("alpha", mcp.MethodRootsList, originalID, nil)
	req := awaitPublished(t, ch, "the roots/list request")

	if !r.RefusePendingServerReq(req.GatewayID, "the client has not completed its handshake") {
		t.Fatal("RefusePendingServerReq = false for the pending roots fetch")
	}
	got := awaitResponse(t, alpha, "the roots refusal")
	if string(got.ID) != string(originalID) || got.Error == nil ||
		got.Error.Code != mcp.CodeMethodNotFound {
		t.Errorf("refusal id=%s err=%+v, want %s / -32601", got.ID, got.Error, originalID)
	}

	r.rootsMu.Lock()
	fetchID, waiters := r.rootsFetchID, len(r.rootsWaiters)
	r.rootsMu.Unlock()
	if fetchID != "" || waiters != 0 {
		t.Errorf("rootsFetchID=%q waiters=%d after the give-up, want both empty", fetchID, waiters)
	}
}

// TestCloseDropsPendingServerReqs pins that no deadline outlives the registry:
// Close forgets every pending request (and stops its timer with it), so nothing
// can fire a refusal into a gateway whose upstreams are already gone.
func TestCloseDropsPendingServerReqs(t *testing.T) {
	r, _ := newElicitTestRegistry("web")
	r.serverReqTimeout = testServerReqTimeout
	r.SetClientServerRequestCaps(declaredCaps(mcp.CapElicitation))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(7), json.RawMessage(`{"message":"?"}`))
	awaitPublished(t, ch, "the elicitation request")

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r.serverReqMu.Lock()
	pending := len(r.pendingServerReqs)
	r.serverReqMu.Unlock()
	if pending != 0 {
		t.Errorf("%d pending server→client request(s) survived Close, want 0", pending)
	}

	// Nothing panics or resurrects after the deadline would have passed.
	time.Sleep(3 * testServerReqTimeout)
}

// TestNoServerReqTimeoutInConfig guards the decision that this deadline is NOT
// a public knob (Stage 15 deliberately leaves the config contract alone): a
// fresh registry gets the constant, and only a test may move it.
func TestNoServerReqTimeoutInConfig(t *testing.T) {
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
	if r.serverReqTimeout != defaultServerReqTimeout {
		t.Errorf("serverReqTimeout = %s, want the %s default", r.serverReqTimeout, defaultServerReqTimeout)
	}
}

// TestCapabilityForMethod (Stage 17a, U3) pins the exported method→capability
// mapping the HTTP transport picks its delivery target with. The point is that
// it READS THE SPEC TABLE rather than restating it: a hand-written copy would
// answer "elicitation" for everything (or drift the moment a fourth method is
// added), which is exactly what the sampling/roots rows below catch.
func TestCapabilityForMethod(t *testing.T) {
	tests := []struct {
		method string
		want   string
		wantOK bool
	}{
		{mcp.MethodElicitationCreate, mcp.CapElicitation, true},
		{mcp.MethodSamplingCreateMessage, mcp.CapSampling, true},
		{mcp.MethodRootsList, mcp.CapRoots, true},
		{mcp.MethodToolsCall, "", false}, // a real method, but client→server
		{"ping", "", false},              // never proxied server→client
		{"", "", false},                  // degenerate input must not match
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			got, ok := CapabilityForMethod(tt.method)
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("CapabilityForMethod(%q) = (%q, %v), want (%q, %v)",
					tt.method, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

// TestCapabilityForMethodCoversEveryProxiedMethod is the drift guard the table
// above cannot be: it walks serverReqSpecs itself, so a method added there
// without a working lookup fails here rather than silently becoming
// undeliverable over HTTP (the transport refuses whatever it cannot map).
func TestCapabilityForMethodCoversEveryProxiedMethod(t *testing.T) {
	for i := range serverReqSpecs {
		spec := &serverReqSpecs[i]
		got, ok := CapabilityForMethod(spec.method)
		if !ok {
			t.Errorf("CapabilityForMethod(%q) reports the method is not proxied, but it has a spec entry", spec.method)
			continue
		}
		if got != spec.capability {
			t.Errorf("CapabilityForMethod(%q) = %q, want the spec's %q", spec.method, got, spec.capability)
		}
	}
}

// TestServerReqPendingTracksTheParkedEntry (Stage 17a) pins the read-only
// probe a client transport sweeps its own gatewayID bookkeeping with: true
// exactly while the request is parked, false before it is published, after the
// answer is routed, and after the registry's deadline gave up on it — which is
// the case the transport actually needs (the registry drops the entry on its
// own timer and nothing else would ever tell the transport).
func TestServerReqPendingTracksTheParkedEntry(t *testing.T) {
	r, _ := newElicitTestRegistry("web")
	r.serverReqTimeout = testServerReqTimeout
	r.SetClientServerRequestCaps(declaredCaps(mcp.CapElicitation))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	if r.ServerReqPending("elicit-nope") {
		t.Error("ServerReqPending reports an id that was never minted as pending")
	}

	r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(7), json.RawMessage(`{"message":"?"}`))
	req := awaitPublished(t, ch, "the elicitation request")
	if !r.ServerReqPending(req.GatewayID) {
		t.Fatalf("ServerReqPending(%q) = false while the request is parked", req.GatewayID)
	}

	// The deadline is the whole reason this exists: nobody answers, the registry
	// gives up on its own, and the transport must be able to notice.
	deadline := time.Now().Add(2 * time.Second)
	for r.ServerReqPending(req.GatewayID) {
		if time.Now().After(deadline) {
			t.Fatalf("ServerReqPending(%q) still true long after the %s deadline", req.GatewayID, r.serverReqTimeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
