package registry

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// Stage 15, part D: roots/list is the one proxied method with a handler of its
// own — N upstreams must cost the client ONE question, and the client's
// list_changed must both drop the cache and reach the upstreams that were told
// the gateway relays it.

// rootsCapableRegistry builds a registry whose client declared roots, with two
// recording upstreams wired in directly (no catalog, no Start — the roots
// plumbing touches neither).
func rootsCapableRegistry(t *testing.T) (*Registry, *elicitRecordingUpstream, *elicitRecordingUpstream) {
	t.Helper()
	r, first := newElicitTestRegistry("alpha")
	second := addRecordingUpstream(r, "beta")
	r.SetClientServerRequestCaps(declaredCaps("roots"))
	return r, first, second
}

func awaitResponse(t *testing.T, fake *elicitRecordingUpstream, what string) *mcp.Message {
	t.Helper()
	select {
	case msg := <-fake.responses:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatalf("upstream never received %s", what)
		return nil
	}
}

// TestRootsSingleFlightAndCache (D1): two upstreams asking concurrently
// produce exactly ONE client request; the single answer reaches both under
// their own original ids; a third request afterwards is served from the cache
// without asking the client again.
func TestRootsSingleFlightAndCache(t *testing.T) {
	r, alpha, beta := rootsCapableRegistry(t)
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	alphaID, betaID := mcp.IntID(1), mcp.StringID("b-1")
	r.onUpstreamRequest("alpha", mcp.MethodRootsList, alphaID, nil)
	r.onUpstreamRequest("beta", mcp.MethodRootsList, betaID, nil)

	var req UpstreamRequest
	select {
	case req = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the roots/list request")
	}
	if req.Method != mcp.MethodRootsList {
		t.Errorf("method = %q, want %q", req.Method, mcp.MethodRootsList)
	}
	if len(req.Params) != 0 {
		t.Errorf("params = %s, want none — roots/list takes none and one answer serves many askers", req.Params)
	}
	select {
	case extra := <-ch:
		t.Fatalf("a second roots/list reached the client: %+v — single-flight broken", extra)
	case <-time.After(200 * time.Millisecond):
	}

	result := json.RawMessage(`{"roots":[{"uri":"file:///w","name":"w"}]}`)
	if !r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), result)) {
		t.Fatal("RouteUpstreamResponse = false for the pending roots id")
	}

	gotAlpha := awaitResponse(t, alpha, "the roots answer")
	if string(gotAlpha.ID) != string(alphaID) || string(gotAlpha.Result) != string(result) {
		t.Errorf("alpha got id=%s result=%s, want %s / %s", gotAlpha.ID, gotAlpha.Result, alphaID, result)
	}
	gotBeta := awaitResponse(t, beta, "the roots answer")
	if string(gotBeta.ID) != string(betaID) || string(gotBeta.Result) != string(result) {
		t.Errorf("beta got id=%s result=%s, want %s / %s", gotBeta.ID, gotBeta.Result, betaID, result)
	}

	// Third ask: served from the cache, no new client request.
	thirdID := mcp.IntID(3)
	r.onUpstreamRequest("alpha", mcp.MethodRootsList, thirdID, nil)
	gotThird := awaitResponse(t, alpha, "the cached roots answer")
	if string(gotThird.ID) != string(thirdID) || string(gotThird.Result) != string(result) {
		t.Errorf("cached answer id=%s result=%s, want %s / %s", gotThird.ID, gotThird.Result, thirdID, result)
	}
	select {
	case extra := <-ch:
		t.Fatalf("the cached request still reached the client: %+v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestRootsRefusedWithoutClientCapability (D2): no "roots" in the client's set
// means an immediate -32601 — like sampling, roots has no soft refusal.
func TestRootsRefusedWithoutClientCapability(t *testing.T) {
	r, fake := newElicitTestRegistry("alpha")
	r.SetClientServerRequestCaps(declaredCaps("elicitation"))
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(7)
	r.onUpstreamRequest("alpha", mcp.MethodRootsList, originalID, nil)

	got := awaitResponse(t, fake, "the roots refusal")
	if string(got.ID) != string(originalID) {
		t.Errorf("refusal id = %s, want the original %s", got.ID, originalID)
	}
	if got.Error == nil || got.Error.Code != mcp.CodeMethodNotFound {
		t.Errorf("refusal = %+v / %s, want a -32601 error", got.Error, got.Result)
	}
	select {
	case req := <-ch:
		t.Fatalf("roots/list reached the subscriber despite an incapable client: %+v", req)
	default:
	}
}

// TestRootsListChangedInvalidatesCache (D3): after the client says its roots
// changed, the next upstream question goes to the client again instead of
// being answered from a stale cache.
func TestRootsListChangedInvalidatesCache(t *testing.T) {
	r, alpha, _ := rootsCapableRegistry(t)
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	r.onUpstreamRequest("alpha", mcp.MethodRootsList, mcp.IntID(1), nil)
	first := <-ch
	r.RouteUpstreamResponse(first.GatewayID, mcp.NewResult(mcp.StringID(first.GatewayID), json.RawMessage(`{"roots":[]}`)))
	awaitResponse(t, alpha, "the first roots answer")

	r.OnClientRootsListChanged()

	r.onUpstreamRequest("alpha", mcp.MethodRootsList, mcp.IntID(2), nil)
	select {
	case second := <-ch:
		if second.GatewayID == first.GatewayID {
			t.Errorf("the second fetch reused the id %q", second.GatewayID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no fresh roots/list after the cache was invalidated")
	}
}

// TestRootsStaleWindowNotCached (D4): an invalidation arriving WHILE the
// question is in flight still lets the answer reach whoever asked (they asked
// before the change), but that answer must not be cached — the next asker
// starts a fresh question.
func TestRootsStaleWindowNotCached(t *testing.T) {
	r, alpha, _ := rootsCapableRegistry(t)
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	r.onUpstreamRequest("alpha", mcp.MethodRootsList, mcp.IntID(1), nil)
	first := <-ch

	r.InvalidateRootsCache() // lands mid-flight

	result := json.RawMessage(`{"roots":[{"uri":"file:///old"}]}`)
	r.RouteUpstreamResponse(first.GatewayID, mcp.NewResult(mcp.StringID(first.GatewayID), result))
	got := awaitResponse(t, alpha, "the in-flight roots answer")
	if string(got.Result) != string(result) {
		t.Errorf("waiter got %s, want the answer it was waiting for", got.Result)
	}

	r.rootsMu.Lock()
	cached := r.rootsCache
	r.rootsMu.Unlock()
	if cached != nil {
		t.Errorf("cache holds %s after an invalidation mid-flight, want empty", cached)
	}

	r.onUpstreamRequest("alpha", mcp.MethodRootsList, mcp.IntID(2), nil)
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("the next ask was served from a cache that should have stayed empty")
	}
}

// TestRootsErrorResponseNotCached (D5): a client error is relayed verbatim to
// the waiter and never cached — otherwise one transient failure would be
// re-served to every later asker.
func TestRootsErrorResponseNotCached(t *testing.T) {
	r, alpha, _ := rootsCapableRegistry(t)
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	originalID := mcp.IntID(1)
	r.onUpstreamRequest("alpha", mcp.MethodRootsList, originalID, nil)
	first := <-ch
	r.RouteUpstreamResponse(first.GatewayID,
		mcp.NewError(mcp.StringID(first.GatewayID), mcp.CodeInternalError, "no roots for you", nil))

	got := awaitResponse(t, alpha, "the roots error")
	if string(got.ID) != string(originalID) {
		t.Errorf("error id = %s, want the original %s", got.ID, originalID)
	}
	if got.Error == nil || got.Error.Code != mcp.CodeInternalError || got.Error.Message != "no roots for you" {
		t.Errorf("error = %+v, want the client's verbatim", got.Error)
	}
	if got.Result != nil {
		t.Errorf("the error response also carried a result %s", got.Result)
	}

	r.rootsMu.Lock()
	cached := r.rootsCache
	r.rootsMu.Unlock()
	if cached != nil {
		t.Errorf("an error answer was cached: %s", cached)
	}
}

// TestDeclaredRootsMirrorsClientListChanged (review И-3) is the honesty
// criterion for the one capability value that carries a sub-flag: what the
// gateway declares to its upstreams is derived from what its OWN client
// declared, never asserted.
//
// A client may legally support roots without notifying about changes
// ("roots":{} per the 2025-06-18 spec). The gateway does not originate
// notifications/roots/list_changed — it only relays the client's — so promising
// listChanged on such a client's behalf would leave a trusting upstream serving
// a stale root list for the rest of the session, with nothing ever arriving to
// correct it.
func TestDeclaredRootsMirrorsClientListChanged(t *testing.T) {
	tests := []struct {
		name        string
		clientValue string
		want        string
	}{
		{"client notifies", `{"listChanged":true}`, `{"listChanged":true}`},
		{"client supports roots but does not notify", `{}`, `{}`},
		{"client says so explicitly", `{"listChanged":false}`, `{}`},
		{"unreadable value degrades to the weaker claim", `"yes"`, `{}`},
		{"null value degrades too", `null`, `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
			r.SetClientServerRequestCaps(map[string]json.RawMessage{
				mcp.CapRoots: json.RawMessage(tt.clientValue),
			})

			got := r.declaredClientCaps()
			if len(got) != 1 || string(got[mcp.CapRoots]) != tt.want {
				t.Errorf("client declared roots:%s, gateway declares %v — want exactly {roots:%s}",
					tt.clientValue, got, tt.want)
			}
			// Whatever the value, the key's presence alone opens the runtime
			// gate: that is what the spec makes meaningful.
			if !r.clientDeclared(mcp.CapRoots) {
				t.Error("the runtime gate refuses roots the client did declare")
			}
		})
	}
}

// TestDeclaredFlaglessCapsRenderEmpty is the counterpart: elicitation and
// sampling have no sub-flags in the spec, so the gateway declares {} whatever
// the client put in the value — the renderer is per-capability, not a blind
// passthrough of the client's object.
func TestDeclaredFlaglessCapsRenderEmpty(t *testing.T) {
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
	r.SetClientServerRequestCaps(map[string]json.RawMessage{
		mcp.CapElicitation: json.RawMessage(`{"listChanged":true,"nonsense":1}`),
		mcp.CapSampling:    json.RawMessage(`{"models":["x"]}`),
	})

	got := r.declaredClientCaps()
	if len(got) != 2 ||
		string(got[mcp.CapElicitation]) != "{}" || string(got[mcp.CapSampling]) != "{}" {
		t.Errorf("declared %v, want exactly {elicitation:{}, sampling:{}}", got)
	}
}

// rootsFanOutUpstream counts ForwardRootsListChanged calls and can fail them,
// so a test can assert the fan-out reaches everyone and survives one failure.
type rootsFanOutUpstream struct {
	fakeUpstreamBase
	mu    sync.Mutex
	calls int
	err   error
}

func (f *rootsFanOutUpstream) ForwardRootsListChanged(context.Context) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.err
}

func (f *rootsFanOutUpstream) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestRootsFanOutReachesEveryConnection (D6): OnClientRootsListChanged calls
// ForwardRootsListChanged on EVERY live connection — the per-upstream "was
// roots declared to me" gate lives INSIDE the connection
// (TestForwardRootsListChangedHonoursDeclaration in internal/upstream covers
// that half), so from here the assertion is reach, not selectivity — and one
// failing upstream does not stop the others.
func TestRootsFanOutReachesEveryConnection(t *testing.T) {
	r, _ := newElicitTestRegistry("alpha")
	good := &rootsFanOutUpstream{}
	bad := &rootsFanOutUpstream{err: errors.New("stdin is stuck")}
	r.mu.Lock()
	r.conns["good"] = good
	r.conns["bad"] = bad
	r.mu.Unlock()

	r.OnClientRootsListChanged()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if good.count() == 1 && bad.count() == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if good.count() != 1 {
		t.Errorf("healthy upstream got %d notifications, want 1", good.count())
	}
	if bad.count() != 1 {
		t.Errorf("failing upstream got %d notifications, want 1", bad.count())
	}
}

// TestDropUpstreamClearsRootsWaiters (D7): a departing upstream's roots
// waiters are discarded — the answer could never reach the process that asked
// — and the client's late answer is delivered to nobody without panicking.
func TestDropUpstreamClearsRootsWaiters(t *testing.T) {
	r, _, _ := rootsCapableRegistry(t)
	ch, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	r.onUpstreamRequest("alpha", mcp.MethodRootsList, mcp.IntID(1), nil)
	req := <-ch

	r.dropUpstream("alpha")

	r.rootsMu.Lock()
	waiters := len(r.rootsWaiters)
	r.rootsMu.Unlock()
	if waiters != 0 {
		t.Errorf("rootsWaiters holds %d entries after dropUpstream, want 0", waiters)
	}

	// The late answer still matches the pending fetch (it fills the cache for
	// the next asker) and must not panic with nobody left to deliver to.
	if !r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(`{"roots":[]}`))) {
		t.Error("RouteUpstreamResponse = false for the pending roots fetch, want true")
	}
}

// deadlineRecordingUpstream captures whether the context its
// ForwardRootsListChanged received carries a deadline. It blocks until that
// context is done, standing in for an upstream that has stopped reading.
type deadlineRecordingUpstream struct {
	fakeUpstreamBase
	mu       sync.Mutex
	hadLimit bool
	returned bool
}

func (f *deadlineRecordingUpstream) ForwardRootsListChanged(ctx context.Context) error {
	_, ok := ctx.Deadline()
	f.mu.Lock()
	f.hadLimit = ok
	f.mu.Unlock()
	<-ctx.Done() // a hung upstream: only the deadline can free this goroutine
	f.mu.Lock()
	f.returned = true
	f.mu.Unlock()
	return ctx.Err()
}

func (f *deadlineRecordingUpstream) state() (hadLimit, returned bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hadLimit, f.returned
}

// TestRootsFanOutIsBounded pins the fix for the leak Stage 17b would otherwise
// have introduced: OnClientRootsListChanged used to hand ForwardRootsListChanged
// the registry's bare process context. That was harmless while roots was never
// declared to an HTTP upstream (the connection's own gate short-circuited
// before any I/O), but Stage 17b declares to HTTP upstreams too — so a POST to
// an upstream that has stopped reading would hold its goroutine and connection
// until the gateway process stopped, once per client roots/list_changed.
//
// The assertion is on the CONTEXT, not on wall-clock time: the real bound is 30
// seconds and no test should wait for it. A hung upstream must (a) be given a
// context with a deadline and (b) actually be released by it — here forced by
// cancelling the registry's process context, which the derived context must
// inherit.
func TestRootsFanOutIsBounded(t *testing.T) {
	r, _ := newElicitTestRegistry("alpha")
	hung := &deadlineRecordingUpstream{}
	r.mu.Lock()
	r.conns["hung"] = hung
	r.mu.Unlock()

	r.OnClientRootsListChanged()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hadLimit, _ := hung.state(); hadLimit {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if hadLimit, _ := hung.state(); !hadLimit {
		t.Fatal("ForwardRootsListChanged got a context with no deadline: a hung upstream would pin this goroutine for the life of the process")
	}

	// And the derived context must still unwind with the registry, not outlive it.
	r.procCancel()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, returned := hung.state(); returned {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the fan-out goroutine did not unwind after the registry's process context was cancelled")
}
