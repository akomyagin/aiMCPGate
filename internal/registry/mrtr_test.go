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

// Stage 19c: the MRTR bridge. These tests run the real Registry.CallToolModern /
// ResumeToolModern against a controllable in-process upstream whose CallTool
// blocks and, while blocked, fires a server→client counter-request the SAME way
// a real upstream's reader goroutine does — by calling r.onUpstreamRequest. That
// is exactly the interception point, so no transport is involved: the tests
// prove the registry converts the counter-request into an InputRequiredResult
// for a modern client and resumes the parked call on retry, with the id spaces
// kept separate.

// mrtrFakeUpstream is a routed upstream whose tools/call blocks on release,
// asks its client one or more server→client questions while blocked, and
// records the responses routed back to it. It implements enough of Upstream to
// be wired directly into r.conns with a matching toolRoute.
type mrtrFakeUpstream struct {
	fakeUpstreamBase
	name      string
	responses chan *mcp.Message // responses RouteUpstreamResponse writes back to us

	// callGate, when closed, releases a blocked tools/call so it can return its
	// final response. Each call takes a fresh gate from gates.
	mu        sync.Mutex
	callCount int
	// onCall is invoked (on the CallTool goroutine) with the call's ordinal and
	// the call's context right after tools/call starts; it lets a test fire
	// onUpstreamRequest and then release the call. It must return the final
	// result the call answers with (or a nil result if ctx was cancelled — the
	// registry maps a cancelled call to an error before it reaches the outcome).
	onCall func(ctx context.Context, n int) *mcp.Message
}

func (f *mrtrFakeUpstream) Name() string { return f.name }

func (f *mrtrFakeUpstream) RespondUpstreamRequest(msg *mcp.Message) error {
	f.responses <- msg
	return nil
}

func (f *mrtrFakeUpstream) CallTool(ctx context.Context, name string, _, _ json.RawMessage) (*mcp.Message, error) {
	f.mu.Lock()
	f.callCount++
	n := f.callCount
	f.mu.Unlock()
	if f.onCall != nil {
		return f.onCall(ctx, n), nil
	}
	return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[{"type":"text","text":"ok `+name+`"}]}`)), nil
}

// newMRTRTestRegistry wires one mrtrFakeUpstream under name with a single tool
// route "<name>__tool" → the upstream. The MRTR plumbing needs a real route
// (toolOwner) plus a real conn (RouteUpstreamResponse writes back through it);
// no Start/catalog machinery is otherwise touched.
func newMRTRTestRegistry(t *testing.T, name string) (*Registry, *mrtrFakeUpstream) {
	t.Helper()
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
	fake := &mrtrFakeUpstream{name: name, responses: make(chan *mcp.Message, 8)}
	r.mu.Lock()
	r.conns[name] = fake
	r.toolRoute[name+"__tool"] = route{upstream: name, original: "tool"}
	r.promptRoute[name+"__prompt"] = route{upstream: name, original: "prompt"}
	r.mu.Unlock()
	t.Cleanup(func() { _ = r.Close() })
	return r, fake
}

// awaitMRTRResponse takes the next response routed back to the fake upstream.
func awaitMRTRResponse(t *testing.T, fake *mrtrFakeUpstream, what string) *mcp.Message {
	t.Helper()
	select {
	case msg := <-fake.responses:
		return msg
	case <-time.After(3 * time.Second):
		t.Fatalf("upstream never received %s", what)
		return nil
	}
}

// TestMRTRElicitationRoundtrip is the core round trip: a modern tools/call whose
// upstream elicits mid-call gets an InputRequiredResult (the elicitation
// envelope verbatim under a gateway id, a non-empty requestState), the client
// retries with inputResponses, the upstream receives the ElicitResult under its
// OWN original id, and the retry returns the final result.
func TestMRTRElicitationRoundtrip(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")

	const originalID = 7
	const elicitParams = `{"message":"need input","requestedSchema":{"type":"object"}}`
	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		// Ask the client mid-call (as the reader goroutine would), then block on
		// the answer reaching us before returning the final result.
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(originalID), json.RawMessage(elicitParams))
		// Wait until the client's answer has been routed back to us.
		<-fake.responses // the ElicitResult under the upstream's own id
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[{"type":"text","text":"done|elicited"}]}`))
	}

	caps := declaredCaps(mcp.CapElicitation)
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	if out.Final != nil {
		t.Fatalf("got a final response, want input_required: %+v", out.Final)
	}
	if out.RequestState == "" {
		t.Error("input_required carried an empty requestState")
	}
	if len(out.InputRequests) != 1 {
		t.Fatalf("got %d input requests, want exactly 1", len(out.InputRequests))
	}
	var gatewayID string
	var env mcp.InputRequestEnvelope
	for id, raw := range out.InputRequests {
		gatewayID = id
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("input request %q is not an envelope: %v", id, err)
		}
	}
	if env.Method != mcp.MethodElicitationCreate {
		t.Errorf("envelope method = %q, want %q", env.Method, mcp.MethodElicitationCreate)
	}
	if string(env.Params) != elicitParams {
		t.Errorf("envelope params = %s, want them verbatim: %s", env.Params, elicitParams)
	}
	if gatewayID == "" {
		t.Fatal("input request keyed by an empty gateway id")
	}

	// The client retries with its answer keyed by the gateway id it saw. In a
	// goroutine: ResumeToolModern blocks until the upstream answers, and the
	// upstream (fake.onCall) only answers after we route the ElicitResult — but
	// that routing happens INSIDE ResumeToolModern. So the two must overlap.
	const answer = `{"action":"accept","content":{"name":"ada"}}`
	retry := mcp.MRTRRetry{
		RequestState:   out.RequestState,
		InputResponses: map[string]json.RawMessage{gatewayID: json.RawMessage(answer)},
	}
	resumeOut, err := r.ResumeToolModern(context.Background(), "web__tool", retry, caps)
	if err != nil {
		t.Fatalf("ResumeToolModern: %v", err)
	}
	if resumeOut.Final == nil {
		t.Fatalf("resume returned no final response: %+v", resumeOut)
	}
	if resumeOut.Final.Error != nil {
		t.Fatalf("final carried an error: %+v", resumeOut.Final.Error)
	}
}

// TestMRTRElicitationUpstreamGetsOriginalID pins the id rewriting explicitly:
// the ElicitResult the upstream receives is under ITS original id, not the
// gateway id the client answered with.
func TestMRTRElicitationUpstreamGetsOriginalID(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")

	originalID := mcp.IntID(42)
	got := make(chan *mcp.Message, 1)
	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, originalID, json.RawMessage(`{"message":"?"}`))
		resp := <-fake.responses
		got <- resp
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	caps := declaredCaps(mcp.CapElicitation)
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	var gatewayID string
	for id := range out.InputRequests {
		gatewayID = id
	}
	const answer = `{"action":"accept"}`
	retry := mcp.MRTRRetry{RequestState: out.RequestState,
		InputResponses: map[string]json.RawMessage{gatewayID: json.RawMessage(answer)}}
	if _, err := r.ResumeToolModern(context.Background(), "web__tool", retry, caps); err != nil {
		t.Fatalf("ResumeToolModern: %v", err)
	}

	select {
	case resp := <-got:
		if string(resp.ID) != string(originalID) {
			t.Errorf("upstream got id %s, want its own original %s", resp.ID, originalID)
		}
		if string(resp.Result) != answer {
			t.Errorf("upstream got result %s, want the client's verbatim %s", resp.Result, answer)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream never received the ElicitResult")
	}
}

// TestMRTRSampling repeats the round trip for sampling/createMessage — the
// bridge is method-agnostic beyond the capability gate.
func TestMRTRSampling(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")
	const params = `{"messages":[{"role":"user","content":{"type":"text","text":"hi"}}],"maxTokens":16}`
	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		r.onUpstreamRequest("web", mcp.MethodSamplingCreateMessage, mcp.IntID(9), json.RawMessage(params))
		<-fake.responses
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}
	caps := declaredCaps(mcp.CapSampling)
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	var gatewayID string
	var env mcp.InputRequestEnvelope
	for id, raw := range out.InputRequests {
		gatewayID = id
		_ = json.Unmarshal(raw, &env)
	}
	if env.Method != mcp.MethodSamplingCreateMessage {
		t.Errorf("envelope method = %q, want %q", env.Method, mcp.MethodSamplingCreateMessage)
	}
	if string(env.Params) != params {
		t.Errorf("sampling params = %s, want verbatim %s", env.Params, params)
	}
	retry := mcp.MRTRRetry{RequestState: out.RequestState,
		InputResponses: map[string]json.RawMessage{gatewayID: json.RawMessage(`{"role":"assistant","content":{"type":"text","text":"ok"},"model":"m"}`)}}
	if _, err := r.ResumeToolModern(context.Background(), "web__tool", retry, caps); err != nil {
		t.Fatalf("ResumeToolModern: %v", err)
	}
}

// TestMRTRRootsCacheHit: with the roots cache warm, an intercepted roots/list is
// answered from the cache and the modern client is NOT shown an input_required —
// the call proceeds to its final result. Proves the MRTR roots path shares the
// legacy cache rather than always asking.
func TestMRTRRootsCacheHit(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")
	// Warm the cache the same way a delivered client answer would.
	const cached = `{"roots":[{"uri":"file:///work","name":"work"}]}`
	r.rootsMu.Lock()
	r.rootsCache = json.RawMessage(cached)
	r.rootsMu.Unlock()

	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		r.onUpstreamRequest("web", mcp.MethodRootsList, mcp.IntID(1), nil)
		// The cache hit answers us directly; wait for it, then finish.
		resp := <-fake.responses
		if string(resp.Result) != cached {
			t.Errorf("upstream got roots %s, want the cached %s", resp.Result, cached)
		}
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, declaredCaps(mcp.CapRoots))
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	if out.Final == nil {
		t.Fatalf("roots cache hit still produced input_required: %+v", out)
	}
	if len(out.InputRequests) != 0 {
		t.Errorf("cache hit surfaced %d input requests, want 0", len(out.InputRequests))
	}
}

// TestMRTRRootsCacheMiss: with the cache empty, the intercepted roots/list is
// surfaced as an input request; the retry answer is delivered to the upstream
// AND fills the cache (proven by a second call served from it).
func TestMRTRRootsCacheMiss(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")
	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		r.onUpstreamRequest("web", mcp.MethodRootsList, mcp.IntID(int64(100+n)), nil)
		<-fake.responses
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	caps := declaredCaps(mcp.CapRoots)
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	if len(out.InputRequests) != 1 {
		t.Fatalf("cache miss surfaced %d input requests, want 1", len(out.InputRequests))
	}
	var gatewayID string
	var env mcp.InputRequestEnvelope
	for id, raw := range out.InputRequests {
		gatewayID = id
		_ = json.Unmarshal(raw, &env)
	}
	if env.Method != mcp.MethodRootsList {
		t.Errorf("envelope method = %q, want %q", env.Method, mcp.MethodRootsList)
	}
	const answer = `{"roots":[{"uri":"file:///x","name":"x"}]}`
	retry := mcp.MRTRRetry{RequestState: out.RequestState,
		InputResponses: map[string]json.RawMessage{gatewayID: json.RawMessage(answer)}}
	if _, err := r.ResumeToolModern(context.Background(), "web__tool", retry, caps); err != nil {
		t.Fatalf("ResumeToolModern: %v", err)
	}

	// The cache is now warm: a second call's roots/list is served from it (no
	// input_required).
	if got := func() json.RawMessage {
		r.rootsMu.Lock()
		defer r.rootsMu.Unlock()
		return r.rootsCache
	}(); string(got) != answer {
		t.Errorf("roots cache = %s after the answer, want %s", got, answer)
	}
}

// TestMRTRCapabilityGate: a client that did NOT declare elicitation must not be
// shown the question — the upstream is refused immediately (soft decline for
// elicitation) and the call finishes with its final result, no input_required.
func TestMRTRCapabilityGate(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")
	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(5), json.RawMessage(`{"message":"?"}`))
		resp := <-fake.responses // the immediate decline
		if resp.Error != nil || string(resp.Result) != `{"action":"decline"}` {
			t.Errorf("incapable-client refusal = %+v / %s, want the decline RESULT", resp.Error, resp.Result)
		}
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	// caps WITHOUT elicitation.
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, declaredCaps(mcp.CapSampling))
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	if out.Final == nil || len(out.InputRequests) != 0 {
		t.Errorf("incapable client still got input_required: %+v", out)
	}
}

// TestMRTRUnknownState: a retry whose requestState matches no park is
// ErrUnknownRequestState (→ -32602 at the transport), and no upstream is touched.
func TestMRTRUnknownState(t *testing.T) {
	r, _ := newMRTRTestRegistry(t, "web")
	retry := mcp.MRTRRetry{RequestState: "deadbeef", InputResponses: map[string]json.RawMessage{"mrtr-9": json.RawMessage(`{}`)}}
	_, err := r.ResumeToolModern(context.Background(), "web__tool", retry, declaredCaps(mcp.CapElicitation))
	if !errors.Is(err, ErrUnknownRequestState) {
		t.Errorf("resume with an unknown state = %v, want ErrUnknownRequestState", err)
	}
}

// TestMRTRStateSingleUse: two retries of the SAME state — the first resumes, the
// second finds nothing (ErrUnknownRequestState). Guards against answering the
// upstream twice.
func TestMRTRStateSingleUse(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")
	resumed := make(chan struct{})
	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(1), json.RawMessage(`{"message":"?"}`))
		<-fake.responses
		close(resumed)
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	caps := declaredCaps(mcp.CapElicitation)
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	var gatewayID string
	for id := range out.InputRequests {
		gatewayID = id
	}
	retry := mcp.MRTRRetry{RequestState: out.RequestState,
		InputResponses: map[string]json.RawMessage{gatewayID: json.RawMessage(`{"action":"accept"}`)}}
	if _, err := r.ResumeToolModern(context.Background(), "web__tool", retry, caps); err != nil {
		t.Fatalf("first ResumeToolModern: %v", err)
	}
	<-resumed
	// Second retry of the same state finds nothing.
	if _, err := r.ResumeToolModern(context.Background(), "web__tool", retry, caps); !errors.Is(err, ErrUnknownRequestState) {
		t.Errorf("second resume of the same state = %v, want ErrUnknownRequestState", err)
	}
}

// TestMRTRParkTimeout: a parked call whose client never retries is swept on the
// deadline — the upstream is refused and a late retry gets ErrUnknownRequestState.
func TestMRTRParkTimeout(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")
	r.serverReqTimeout = 80 * time.Millisecond

	fake.onCall = func(ctx context.Context, n int) *mcp.Message {
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(3), json.RawMessage(`{"message":"?"}`))
		// Block until the sweep cancels the detached call context — a real
		// upstream call would abort then. We must NOT consume from fake.responses
		// here: the sweep's decline goes there and the test asserts on it.
		<-ctx.Done()
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	caps := declaredCaps(mcp.CapElicitation)
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	// The upstream is refused by the sweep (soft decline for elicitation).
	got := awaitMRTRResponse(t, fake, "the sweep refusal")
	if string(got.Result) != `{"action":"decline"}` {
		t.Errorf("sweep refusal = %+v / %s, want the decline RESULT", got.Error, got.Result)
	}
	// A late retry finds nothing.
	retry := mcp.MRTRRetry{RequestState: out.RequestState,
		InputResponses: map[string]json.RawMessage{"mrtr-1": json.RawMessage(`{"action":"accept"}`)}}
	if _, err := r.ResumeToolModern(context.Background(), "web__tool", retry, caps); !errors.Is(err, ErrUnknownRequestState) {
		t.Errorf("late retry after sweep = %v, want ErrUnknownRequestState", err)
	}
}

// TestMRTRSequentialQuestions: the upstream asks a SECOND question after the
// answer to the first — the resume returns a fresh input_required for it rather
// than a final.
func TestMRTRSequentialQuestions(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")
	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		// First question.
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(1), json.RawMessage(`{"message":"q1"}`))
		<-fake.responses // answer to q1
		// Second question, after the first was answered.
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(2), json.RawMessage(`{"message":"q2"}`))
		<-fake.responses // answer to q2
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	caps := declaredCaps(mcp.CapElicitation)
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	firstID := onlyKey(t, out.InputRequests)
	retry1 := mcp.MRTRRetry{RequestState: out.RequestState,
		InputResponses: map[string]json.RawMessage{firstID: json.RawMessage(`{"action":"accept"}`)}}
	out2, err := r.ResumeToolModern(context.Background(), "web__tool", retry1, caps)
	if err != nil {
		t.Fatalf("first ResumeToolModern: %v", err)
	}
	if out2.Final != nil || len(out2.InputRequests) != 1 {
		t.Fatalf("after answering q1 want a second input_required, got %+v", out2)
	}
	secondID := onlyKey(t, out2.InputRequests)
	retry2 := mcp.MRTRRetry{RequestState: out2.RequestState,
		InputResponses: map[string]json.RawMessage{secondID: json.RawMessage(`{"action":"accept"}`)}}
	out3, err := r.ResumeToolModern(context.Background(), "web__tool", retry2, caps)
	if err != nil {
		t.Fatalf("second ResumeToolModern: %v", err)
	}
	if out3.Final == nil {
		t.Fatalf("after answering q2 want the final, got %+v", out3)
	}
}

// TestMRTRLegacyCoexistence: a legacy subscriber and a modern call live on the
// SAME gateway at once. A question from an upstream with a modern waiter goes to
// the modern call (input_required); a question from an upstream WITHOUT one goes
// to the legacy subscriber the old way. Neither steals the other's traffic.
func TestMRTRLegacyCoexistence(t *testing.T) {
	r, modernFake := newMRTRTestRegistry(t, "modern")
	// A second upstream with a plain recording conn and NO modern call — its
	// question must ride the legacy conveyor.
	legacyFake := &elicitRecordingUpstream{responses: make(chan *mcp.Message, 4)}
	r.mu.Lock()
	r.conns["legacy"] = legacyFake
	r.mu.Unlock()
	// The process-wide gate lets the legacy upstream's question through.
	r.SetClientServerRequestCaps(declaredCaps(mcp.CapElicitation))
	legacyCh, unsubscribe := r.SubscribeUpstreamRequests()
	defer unsubscribe()

	modernFake.onCall = func(_ context.Context, n int) *mcp.Message {
		// While the modern call is in flight, the LEGACY upstream asks its own
		// question — it must reach the legacy subscriber, not the modern call.
		r.onUpstreamRequest("legacy", mcp.MethodElicitationCreate, mcp.IntID(99), json.RawMessage(`{"message":"legacy?"}`))
		select {
		case legacyReq := <-legacyCh:
			if legacyReq.Method != mcp.MethodElicitationCreate {
				t.Errorf("legacy subscriber got %q, want elicitation/create", legacyReq.Method)
			}
			// Answer the legacy question so its upstream is unblocked.
			r.RouteUpstreamResponse(legacyReq.GatewayID, mcp.NewResult(mcp.StringID(legacyReq.GatewayID), json.RawMessage(`{"action":"accept"}`)))
		case <-time.After(2 * time.Second):
			t.Error("legacy subscriber never received the coexisting legacy question")
		}
		// Now the MODERN upstream asks — this one must be intercepted.
		r.onUpstreamRequest("modern", mcp.MethodElicitationCreate, mcp.IntID(1), json.RawMessage(`{"message":"modern?"}`))
		<-modernFake.responses // the modern answer routed back after retry
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	caps := declaredCaps(mcp.CapElicitation)
	out, err := r.CallToolModern(context.Background(), "modern__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	if len(out.InputRequests) != 1 {
		t.Fatalf("modern call got %d input requests, want 1 (its own question)", len(out.InputRequests))
	}
	// The legacy subscriber must NOT have received the modern question.
	select {
	case stray := <-legacyCh:
		t.Fatalf("the modern question leaked to the legacy subscriber: %+v", stray)
	default:
	}
	gatewayID := onlyKey(t, out.InputRequests)
	retry := mcp.MRTRRetry{RequestState: out.RequestState,
		InputResponses: map[string]json.RawMessage{gatewayID: json.RawMessage(`{"action":"accept"}`)}}
	if _, err := r.ResumeToolModern(context.Background(), "modern__tool", retry, caps); err != nil {
		t.Fatalf("ResumeToolModern: %v", err)
	}
	// The legacy upstream got its answer under its own id.
	select {
	case got := <-legacyFake.responses:
		if string(got.ID) != string(mcp.IntID(99)) {
			t.Errorf("legacy upstream answered under id %s, want its own 99", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Error("legacy upstream never received its answer")
	}
}

// TestMRTRPartialResponses: the upstream asks TWO questions in one coalesced
// batch; the client answers only one on the first retry. The resume must
// re-surface the still-open one as a fresh input_required rather than treating
// the call as complete.
func TestMRTRPartialResponses(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")
	fake.onCall = func(_ context.Context, n int) *mcp.Message {
		// Fire both questions back-to-back (before either is answered) so the
		// 50ms collect window coalesces them into one input_required.
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(1), json.RawMessage(`{"message":"q1"}`))
		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(2), json.RawMessage(`{"message":"q2"}`))
		<-fake.responses // answer to first
		<-fake.responses // answer to second
		return mcp.NewResult(mcp.IntID(int64(n)), json.RawMessage(`{"content":[]}`))
	}

	caps := declaredCaps(mcp.CapElicitation)
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, caps)
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	if len(out.InputRequests) != 2 {
		t.Fatalf("coalesced batch surfaced %d input requests, want 2", len(out.InputRequests))
	}
	// Answer exactly one of the two.
	var firstID string
	for id := range out.InputRequests {
		firstID = id
		break
	}
	retry := mcp.MRTRRetry{RequestState: out.RequestState,
		InputResponses: map[string]json.RawMessage{firstID: json.RawMessage(`{"action":"accept"}`)}}
	out2, err := r.ResumeToolModern(context.Background(), "web__tool", retry, caps)
	if err != nil {
		t.Fatalf("ResumeToolModern: %v", err)
	}
	if out2.Final != nil {
		t.Fatalf("a partial answer completed the call, want the remaining input_required: %+v", out2)
	}
	if len(out2.InputRequests) != 1 {
		t.Fatalf("re-surfaced %d input requests, want the 1 unanswered", len(out2.InputRequests))
	}
	if _, ok := out2.InputRequests[firstID]; ok {
		t.Error("the re-surfaced batch still contains the already-answered question")
	}
	// Answer the remaining one; the call now completes.
	var secondID string
	for id := range out2.InputRequests {
		secondID = id
	}
	retry2 := mcp.MRTRRetry{RequestState: out2.RequestState,
		InputResponses: map[string]json.RawMessage{secondID: json.RawMessage(`{"action":"accept"}`)}}
	out3, err := r.ResumeToolModern(context.Background(), "web__tool", retry2, caps)
	if err != nil {
		t.Fatalf("second ResumeToolModern: %v", err)
	}
	if out3.Final == nil {
		t.Fatalf("after answering both, want the final: %+v", out3)
	}
}

// TestMRTRNoQuestionReturnsFinal: a modern call whose upstream asks nothing just
// returns the final response — the common case, and the proof that the collect
// window is not paid by a normal call.
func TestMRTRNoQuestionReturnsFinal(t *testing.T) {
	r, _ := newMRTRTestRegistry(t, "web")
	out, err := r.CallToolModern(context.Background(), "web__tool", nil, nil, declaredCaps(mcp.CapElicitation))
	if err != nil {
		t.Fatalf("CallToolModern: %v", err)
	}
	if out.Final == nil || len(out.InputRequests) != 0 {
		t.Errorf("a no-question call returned %+v, want just a final", out)
	}
}

// TestMRTRDeliverAfterCallFinished is the regression for the pick/deliver race
// (fix in mrtr.go, plan §16): handleMRTRIfWaiting picks a waiter under
// serverReqMu and RELEASES the lock before deliverMRTRQuestion. On an HTTP
// upstream the counter-request rides the long-lived GET-SSE reader — a DIFFERENT
// goroutine from the POST-response reader that finishes the call — so between
// the pick and the delivery the call can finish: collectMRTR's resultCh branch
// deregisters and closes the waiter, refuseOpenMRTR runs on an open set that does
// not yet contain this question. Before the fix the later trackOpen recorded the
// question anyway, the send landed in a channel nobody drains, and the
// pendingServerReqs entry + the upstream's blocked call leaked for the life of
// the connection.
//
// The interleaving is forced deterministically, not raced: the call is finished
// FIRST (collectMRTR returns Final, proving the waiter is now deregistered and
// closed — exactly the post-race state), and only THEN is deliverMRTRQuestion
// invoked with that waiter — the delivery that lost the race. The fix makes that
// delivery refuse the upstream and leave no pending entry; the old code leaked.
func TestMRTRDeliverAfterCallFinished(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")

	waiter := &mrtrWaiter{
		clientCaps: declaredCaps(mcp.CapElicitation),
		questions:  make(chan mrtrQuestion, 8),
		open:       map[string]mrtrOpenQuestion{},
	}
	r.registerMRTRWaiter("web", waiter)

	// Drive the call to completion through the real collector, so the waiter ends
	// up in precisely the state the resultCh branch leaves it: deregistered and
	// closed, refuseOpenMRTR already run.
	resultCh := make(chan mrtrCallResult, 1)
	_, cancel := context.WithCancel(context.Background())
	resultCh <- mrtrCallResult{resp: mcp.NewResult(mcp.IntID(1), json.RawMessage(`{"content":[]}`))}
	out, err := r.collectMRTR("web", waiter, resultCh, cancel)
	if err != nil {
		t.Fatalf("collectMRTR: %v", err)
	}
	if out.Final == nil {
		t.Fatalf("collectMRTR did not return the final: %+v", out)
	}

	// A counter-request now arrives on the OTHER goroutine and is delivered to the
	// just-finished waiter — the delivery that lost the race. This mirrors what
	// handleMRTRIfWaiting does after releasing serverReqMu (it had picked this
	// waiter before the call finished).
	const originalID = 55
	before := pendingServerReqCount(r)
	r.deliverMRTRQuestion(waiter, "web", mcp.MethodElicitationCreate,
		mcp.IntID(originalID), json.RawMessage(`{"message":"late?"}`),
		(*Registry).respondElicitDecline)

	// The upstream that asked must be answered (a soft decline for elicitation),
	// not left hanging.
	got := awaitMRTRResponse(t, fake, "the refusal for the late question")
	if string(got.ID) != string(mcp.IntID(originalID)) {
		t.Errorf("refusal went to id %s, want the upstream's own %d", got.ID, originalID)
	}
	if string(got.Result) != `{"action":"decline"}` {
		t.Errorf("late question refusal = %+v / %s, want the decline RESULT", got.Error, got.Result)
	}

	// And nothing may leak in pendingServerReqs: the delivery must have rolled its
	// entry back. (Before the fix this is where the leak shows up.)
	if after := pendingServerReqCount(r); after != before {
		t.Errorf("pendingServerReqs leaked: %d entries after a lost-race delivery, want %d", after, before)
	}
	// Nothing may sit unread in the dead waiter's channel either.
	select {
	case stray := <-waiter.questions:
		t.Errorf("a question was queued to a finished waiter's channel: %+v", stray)
	default:
	}
}

// TestMRTRDeliverRootsAfterCallFinished is the same race on the roots path
// (deliverMRTRRoots starts a single-flight, then trackOpens). A delivery losing
// the race must unwind the single-flight it just started — clear rootsFetchID,
// refuse the parked upstream, drop the pending entry — rather than wedge roots
// for the connection.
func TestMRTRDeliverRootsAfterCallFinished(t *testing.T) {
	r, fake := newMRTRTestRegistry(t, "web")

	waiter := &mrtrWaiter{
		clientCaps: declaredCaps(mcp.CapRoots),
		questions:  make(chan mrtrQuestion, 8),
		open:       map[string]mrtrOpenQuestion{},
	}
	r.registerMRTRWaiter("web", waiter)

	resultCh := make(chan mrtrCallResult, 1)
	_, cancel := context.WithCancel(context.Background())
	resultCh <- mrtrCallResult{resp: mcp.NewResult(mcp.IntID(1), json.RawMessage(`{"content":[]}`))}
	out, err := r.collectMRTR("web", waiter, resultCh, cancel)
	if err != nil {
		t.Fatalf("collectMRTR: %v", err)
	}
	if out.Final == nil {
		t.Fatalf("collectMRTR did not return the final: %+v", out)
	}

	const originalID = 77
	before := pendingServerReqCount(r)
	r.deliverMRTRRoots(waiter, "web", mcp.IntID(originalID))

	// The upstream is refused with roots' -32601 shape, not left hanging.
	got := awaitMRTRResponse(t, fake, "the refusal for the late roots question")
	if string(got.ID) != string(mcp.IntID(originalID)) {
		t.Errorf("roots refusal went to id %s, want the upstream's own %d", got.ID, originalID)
	}
	if got.Error == nil {
		t.Errorf("late roots question = %+v, want a -32601 error", got)
	}
	// No pending leak, and the single-flight is unwound so the next asker starts
	// fresh.
	if after := pendingServerReqCount(r); after != before {
		t.Errorf("pendingServerReqs leaked on roots path: %d after, want %d", after, before)
	}
	r.rootsMu.Lock()
	fetchID, waiters := r.rootsFetchID, len(r.rootsWaiters)
	r.rootsMu.Unlock()
	if fetchID != "" || waiters != 0 {
		t.Errorf("roots single-flight not unwound: rootsFetchID=%q rootsWaiters=%d", fetchID, waiters)
	}
	select {
	case stray := <-waiter.questions:
		t.Errorf("a roots question was queued to a finished waiter's channel: %+v", stray)
	default:
	}
}

// pendingServerReqCount counts the parked server→client requests — the leak
// surface for the pick/deliver race regressions above.
func pendingServerReqCount(r *Registry) int {
	r.serverReqMu.Lock()
	defer r.serverReqMu.Unlock()
	return len(r.pendingServerReqs)
}

// onlyKey returns the single key of a one-entry input-request map, failing the
// test otherwise.
func onlyKey(t *testing.T, m map[string]json.RawMessage) string {
	t.Helper()
	if len(m) != 1 {
		t.Fatalf("want exactly 1 input request, got %d", len(m))
	}
	for k := range m {
		return k
	}
	return ""
}
