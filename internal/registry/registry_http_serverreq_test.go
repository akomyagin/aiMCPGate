package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// Stage 17b end-to-end: a REMOTE MCP server reached over Streamable HTTP asks
// the gateway something mid-session (elicitation/create, sampling/createMessage,
// roots/list) by pushing a request frame into its SSE stream; the gateway proxies
// it through the ONE server→client pipeline (serverreq.go) and POSTs the client's
// answer back under the upstream's ORIGINAL id. Everything here runs the real
// registry against a real httptest server — no transport fakes.

// serverReqHTTPUpstream is that server. Beyond the catalog basics it does two
// things the earlier HTTP stands do not:
//
//   - it RESPECTS CAPABILITY NEGOTIATION. ask() refuses to send a question the
//     gateway never declared support for, exactly as a spec-abiding server must
//     ("both parties MUST only use negotiated capabilities"). That is what makes
//     these tests able to prove the declaration half of the stage: drop the
//     DeclareClientCapabilities call from startHTTP and the stand simply never
//     asks, so the round-trip times out. A mock that asked regardless would have
//     stayed green while the feature was dead against every real server — the
//     v0.3.0 elicitation incident, verbatim;
//   - it records every JSON-RPC RESPONSE POSTed back to it, which is where the
//     tracker's literal criterion is checked: the id must be the server's OWN.
type serverReqHTTPUpstream struct {
	// rude sends questions even when nothing was declared — the impolite
	// server the registry's runtime gate (clientDeclared) has to refuse.
	rude bool
	// dropFirstStream closes the first GET stream immediately, forcing the
	// gateway's reconnect loop to prove itself.
	dropFirstStream bool

	events    chan string       // frames to emit on the live stream
	streams   chan int          // ordinal of each GET stream as it attaches
	responses chan *mcp.Message // response bodies POSTed back to us

	mu      sync.Mutex
	rawCaps string // capabilities object of the initialize we received, verbatim
	gets    int
}

func newServerReqHTTPUpstream() *serverReqHTTPUpstream {
	return &serverReqHTTPUpstream{
		events:    make(chan string, 4),
		streams:   make(chan int, 8),
		responses: make(chan *mcp.Message, 8),
	}
}

// sawCaps returns the raw capabilities object the gateway declared in its
// initialize ("{}" when it declared nothing).
func (s *serverReqHTTPUpstream) sawCaps() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rawCaps
}

// declared reports whether the gateway's initialize carried the named client
// capability — presence of the KEY is what the spec makes meaningful.
func (s *serverReqHTTPUpstream) declared(capability string) bool {
	var caps map[string]json.RawMessage
	if json.Unmarshal([]byte(s.sawCaps()), &caps) != nil {
		return false
	}
	_, ok := caps[capability]
	return ok
}

// ask pushes one server→client request into the live SSE stream, under the
// server's OWN id. Unless rude, it stays silent when the matching capability was
// never declared to it — the negotiation this stage is really about. The skip is
// logged, because the resulting failure is a timeout further down the test and
// the reason must be visible in the output.
func (s *serverReqHTTPUpstream) ask(t *testing.T, method, id, params string) {
	t.Helper()
	capability, ok := CapabilityForMethod(method)
	if !ok {
		t.Fatalf("test bug: %s is not a proxied server→client method", method)
	}
	if !s.rude && !s.declared(capability) {
		t.Logf("upstream NOT asking %s: capability %q was never declared to it (initialize carried %s)",
			method, capability, s.sawCaps())
		return
	}
	frame := fmt.Sprintf(`{"jsonrpc":"2.0","id":%q,"method":%q`, id, method)
	if params != "" {
		frame += `,"params":` + params
	}
	s.events <- frame + `}`
}

// awaitStream waits until the n-th GET stream has attached.
func (s *serverReqHTTPUpstream) awaitStream(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case got := <-s.streams:
			if got >= n {
				return
			}
		case <-deadline:
			t.Fatalf("the gateway never opened GET SSE stream #%d", n)
		}
	}
}

// awaitResponse takes the next JSON-RPC response POSTed back to the server.
func (s *serverReqHTTPUpstream) awaitResponse(t *testing.T, what string) *mcp.Message {
	t.Helper()
	select {
	case msg := <-s.responses:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatalf("the upstream never received %s", what)
		return nil
	}
}

func (s *serverReqHTTPUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		s.mu.Lock()
		s.gets++
		n := s.gets
		s.mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fl.Flush()
		s.streams <- n
		if n == 1 && s.dropFirstStream {
			return // drop it: the gateway must reconnect and keep listening
		}
		for {
			select {
			case ev := <-s.events:
				fmt.Fprintf(w, "data: %s\n\n", ev)
				fl.Flush()
			case <-r.Context().Done():
				return
			}
		}
	}

	var req mcp.Message
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// A RESPONSE body is the gateway answering a question WE asked. Per the
	// spec the server acknowledges it with 202 and no content.
	if req.IsResponse() {
		s.responses <- &req
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if req.IsNotification() {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	switch req.Method {
	case mcp.MethodInitialize:
		var p mcp.InitializeParams
		_ = json.Unmarshal(req.Params, &p)
		s.mu.Lock()
		s.rawCaps = string(p.Capabilities)
		s.mu.Unlock()
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(
			fmt.Sprintf(`{"protocolVersion":%q,"capabilities":{"tools":{}},"serverInfo":{"name":"remote","version":"1.0.0"}}`, mcp.ProtocolVersion))))
	case mcp.MethodToolsList:
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(
			`{"tools":[{"name":"search","description":"remote search","inputSchema":{"type":"object"}}]}`)))
	case mcp.MethodResourceList:
		_ = enc.Encode(mcp.NewResult(req.ID, json.RawMessage(`{"resources":[]}`)))
	default:
		_ = enc.Encode(mcp.NewError(req.ID, mcp.CodeMethodNotFound, "method not found", nil))
	}
}

// startServerReqRegistry brings up a registry against up over the REAL startHTTP
// path and returns it with the client's subscription already in place (the
// gateway's own "client", as in the Stage 15 tests). caps are the server→client
// capabilities that client declares — set before Start, because a handshake that
// already happened cannot be re-negotiated.
func startServerReqRegistry(t *testing.T, up *serverReqHTTPUpstream, caps ...string) (*Registry, <-chan UpstreamRequest) {
	t.Helper()
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)

	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "remote", URL: srv.URL, Enabled: boolPtr(true)},
	}}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	if len(caps) > 0 {
		r.SetClientServerRequestCaps(declaredCaps(caps...))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	ch, unsubscribe := r.SubscribeUpstreamRequests()
	t.Cleanup(unsubscribe)
	return r, ch
}

// TestHTTPUpstreamElicitationRoundTrip is the stage's literal acceptance
// criterion: an httptest upstream that pushes elicitation/create into its SSE
// stream gets the client's answer back under ITS OWN id, not the gateway-minted
// one the client sees.
func TestHTTPUpstreamElicitationRoundTrip(t *testing.T) {
	up := newServerReqHTTPUpstream()
	r, ch := startServerReqRegistry(t, up, mcp.CapElicitation)
	up.awaitStream(t, 1)

	const params = `{"message":"need input","requestedSchema":{"type":"object"}}`
	up.ask(t, mcp.MethodElicitationCreate, "ask-1", params)

	req := awaitPublished(t, ch, "the HTTP upstream's elicitation/create")
	if !strings.HasPrefix(req.GatewayID, "elicit-") {
		t.Errorf("client-facing id = %q, want an elicit- prefixed gateway id", req.GatewayID)
	}
	if req.Method != mcp.MethodElicitationCreate {
		t.Errorf("published method = %q, want %q", req.Method, mcp.MethodElicitationCreate)
	}
	if string(req.Params) != params {
		t.Errorf("published params = %s, want them verbatim: %s", req.Params, params)
	}

	const answer = `{"action":"accept","content":{"name":"ada"}}`
	if !r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(answer))) {
		t.Fatal("the client's answer was not routed to a pending request")
	}

	got := up.awaitResponse(t, "the client's answer")
	if string(got.ID) != `"ask-1"` {
		t.Errorf("answer POSTed under id %s, want the upstream's OWN %q", got.ID, `"ask-1"`)
	}
	if string(got.Result) != answer {
		t.Errorf("answer result = %s, want it verbatim: %s", got.Result, answer)
	}
}

// TestHTTPUpstreamSamplingRoundTrip repeats the round trip for
// sampling/createMessage — the transport must stay method-agnostic (which
// methods are proxied is the registry's spec table), and the conversation
// content in params must survive verbatim.
func TestHTTPUpstreamSamplingRoundTrip(t *testing.T) {
	up := newServerReqHTTPUpstream()
	r, ch := startServerReqRegistry(t, up, mcp.CapSampling)
	up.awaitStream(t, 1)

	const params = `{"messages":[{"role":"user","content":{"type":"text","text":"summarize"}}],"maxTokens":64}`
	up.ask(t, mcp.MethodSamplingCreateMessage, "sample-9", params)

	req := awaitPublished(t, ch, "the HTTP upstream's sampling/createMessage")
	if !strings.HasPrefix(req.GatewayID, "sampling-") {
		t.Errorf("client-facing id = %q, want a sampling- prefixed gateway id", req.GatewayID)
	}
	if string(req.Params) != params {
		t.Errorf("published params = %s, want them verbatim: %s", req.Params, params)
	}

	const answer = `{"role":"assistant","content":{"type":"text","text":"done"},"model":"test"}`
	if !r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(answer))) {
		t.Fatal("the client's answer was not routed to a pending request")
	}

	got := up.awaitResponse(t, "the client's sampling answer")
	if string(got.ID) != `"sample-9"` {
		t.Errorf("answer POSTed under id %s, want the upstream's OWN %q", got.ID, `"sample-9"`)
	}
	if string(got.Result) != answer {
		t.Errorf("answer result = %s, want it verbatim: %s", got.Result, answer)
	}
}

// TestHTTPUpstreamRootsListRoundTrip covers the third method, which takes the
// pipeline's roots/list special case (one client question serves N upstreams)
// rather than the generic park-and-publish. It carries no params.
func TestHTTPUpstreamRootsListRoundTrip(t *testing.T) {
	up := newServerReqHTTPUpstream()
	r, ch := startServerReqRegistry(t, up, mcp.CapRoots)
	up.awaitStream(t, 1)

	up.ask(t, mcp.MethodRootsList, "roots-req-3", "")

	req := awaitPublished(t, ch, "the HTTP upstream's roots/list")
	if !strings.HasPrefix(req.GatewayID, "roots-") {
		t.Errorf("client-facing id = %q, want a roots- prefixed gateway id", req.GatewayID)
	}
	if len(req.Params) != 0 {
		t.Errorf("roots/list published with params %s, want none", req.Params)
	}

	const answer = `{"roots":[{"uri":"file:///work","name":"work"}]}`
	if !r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(answer))) {
		t.Fatal("the client's answer was not routed to a pending request")
	}

	got := up.awaitResponse(t, "the client's roots list")
	if string(got.ID) != `"roots-req-3"` {
		t.Errorf("answer POSTed under id %s, want the upstream's OWN %q", got.ID, `"roots-req-3"`)
	}
	if string(got.Result) != answer {
		t.Errorf("answer result = %s, want it verbatim: %s", got.Result, answer)
	}
}

// TestRegistryDeclaresClientCapsToHTTPUpstreamBytes replaces the former
// TestRegistryDoesNotDeclareElicitationToHTTPUpstream, which pinned exactly the
// behaviour Stage 17b removes ("an HTTP upstream always receives {}"). That was
// right while the HTTP transport had no way to carry an answer back; respond
// gave it one, so an HTTP upstream is now told the same honest set a stdio one
// is. Asserted on the BYTES of the initialize the upstream received — a flag or
// a mock would not prove a spec-abiding server ever sees the capability.
func TestRegistryDeclaresClientCapsToHTTPUpstreamBytes(t *testing.T) {
	up := newServerReqHTTPUpstream()
	startServerReqRegistry(t, up, mcp.CapElicitation)

	caps := up.sawCaps()
	var got map[string]json.RawMessage
	if err := json.Unmarshal([]byte(caps), &got); err != nil {
		t.Fatalf("initialize capabilities %q are not an object: %v", caps, err)
	}
	value, ok := got[mcp.CapElicitation]
	if !ok {
		t.Fatalf("HTTP upstream received capabilities %s, want them to carry %q", caps, mcp.CapElicitation)
	}
	if string(value) != "{}" {
		t.Errorf("elicitation declared as %s, want {} (it has no sub-flags)", value)
	}
	if _, ok := got[mcp.CapSampling]; ok {
		t.Errorf("HTTP upstream was promised sampling the client never declared: %s", caps)
	}
}

// TestRegistryHTTPUpstreamUndeclaredWithoutClientCaps is the other half of that
// replacement and the guard for doctor/call/catalog: with no client behind the
// gateway nothing is declared, and the handshake carries exactly {} — the same
// bytes as before this stage, strictly compared.
func TestRegistryHTTPUpstreamUndeclaredWithoutClientCaps(t *testing.T) {
	up := newServerReqHTTPUpstream()
	startServerReqRegistry(t, up)

	if caps := up.sawCaps(); caps != "{}" {
		t.Errorf("HTTP upstream received capabilities %s, want exactly {} when the client declared nothing", caps)
	}
}

// TestHTTPUpstreamRudeAskRefusedByPost pins that REFUSALS travel over HTTP too:
// an upstream that ignores the negotiation and asks anyway is answered
// immediately by the registry's runtime gate — with the spec's soft decline,
// under its own id, over the same response POST.
func TestHTTPUpstreamRudeAskRefusedByPost(t *testing.T) {
	up := newServerReqHTTPUpstream()
	up.rude = true
	_, _ = startServerReqRegistry(t, up) // no client capabilities at all
	up.awaitStream(t, 1)

	up.ask(t, mcp.MethodElicitationCreate, "rude-1", `{"message":"?"}`)

	got := up.awaitResponse(t, "the refusal of its undeclared question")
	if string(got.ID) != `"rude-1"` {
		t.Errorf("refusal POSTed under id %s, want the upstream's OWN %q", got.ID, `"rude-1"`)
	}
	if got.Error != nil || string(got.Result) != `{"action":"decline"}` {
		t.Errorf("refusal = %+v / %s, want the spec's decline RESULT", got.Error, got.Result)
	}
}

// TestHTTPUpstreamAskAfterReconnectServed pins that the ability to RECEIVE
// questions survives the stream dropping: the upstream kills the first GET, the
// gateway reconnects (1s backoff), and a question asked on the second stream
// still completes the round trip.
func TestHTTPUpstreamAskAfterReconnectServed(t *testing.T) {
	up := newServerReqHTTPUpstream()
	up.dropFirstStream = true
	r, ch := startServerReqRegistry(t, up, mcp.CapElicitation)
	up.awaitStream(t, 2)

	up.ask(t, mcp.MethodElicitationCreate, "after-reconnect", `{"message":"still there?"}`)

	req := awaitPublished(t, ch, "the elicitation asked on the reconnected stream")
	const answer = `{"action":"accept","content":{"ok":true}}`
	if !r.RouteUpstreamResponse(req.GatewayID, mcp.NewResult(mcp.StringID(req.GatewayID), json.RawMessage(answer))) {
		t.Fatal("the client's answer was not routed to a pending request")
	}

	got := up.awaitResponse(t, "the client's answer after the reconnect")
	if string(got.ID) != `"after-reconnect"` {
		t.Errorf("answer POSTed under id %s, want the upstream's OWN %q", got.ID, `"after-reconnect"`)
	}
	if string(got.Result) != answer {
		t.Errorf("answer result = %s, want it verbatim: %s", got.Result, answer)
	}
}
