// Stage 17a — end-to-end tests for server→client requests over the
// client-facing HTTP transport: an upstream asks (elicitation/create,
// sampling/createMessage, roots/list), the question travels as an SSE event on
// ONE session's GET /mcp stream, the answer comes back on an ordinary POST
// carrying that session's Mcp-Session-Id, and the upstream's tools/call
// completes with it.
//
// Unlike every other HTTP test file, these run the REAL httpServer.Serve
// (bound on an ephemeral port, learned through the onListen hook): the router
// that decides WHICH stream a request goes to is subscribed in Serve's
// prologue, so a bare handleMCP mounted in an httptest.Server — which is what
// the older files do — has no router at all.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// serverReqCfg builds the single-upstream config these tests share: one
// fakeserver named "web" with an "ask" tool, plus whatever hook the caller arms
// (FAKE_ELICIT / FAKE_SAMPLING / FAKE_ROOTS). FAKE_CAPS_FILE is always set —
// several tests assert the BYTES of the upstream handshake, which is the only
// direct evidence of what the gateway declared on its client's behalf.
func serverReqCfg(t *testing.T, env map[string]string) *config.Config {
	t.Helper()
	bin := buildFakeServer(t)
	full := map[string]string{
		"FAKE_NAME":      "web",
		"FAKE_TOOLS":     "ask",
		"FAKE_CAPS_FILE": filepath.Join(t.TempDir(), "caps"),
	}
	for k, v := range env {
		full[k] = v
	}
	return &config.Config{
		Transport:  config.TransportHTTP,
		ListenAddr: "127.0.0.1:0", // ephemeral port, learned via onListen
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: boolPtr(true), Env: full},
		},
	}
}

// rootsChangedCfg is serverReqCfg plus the record of every
// notifications/roots/list_changed the upstream received.
func rootsChangedCfg(t *testing.T) (*config.Config, string) {
	t.Helper()
	changed := filepath.Join(t.TempDir(), "roots-changed")
	return serverReqCfg(t, map[string]string{"FAKE_ROOTS_CHANGED_FILE": changed}), changed
}

// serverReqGateway is a gateway running the real Serve, with the helpers these
// tests need to act as one (or several) MCP clients over HTTP.
type serverReqGateway struct {
	base     string
	client   *http.Client
	hs       *httpServer
	capsFile string

	cancel    context.CancelFunc
	serveDone chan error
	serveRes  error
	serveGot  bool
}

// errServeRunning is what waitServe reports when Serve is still going — a
// distinct sentinel so a test can tell "returned nil" from "did not return".
var errServeRunning = fmt.Errorf("Serve is still running")

// startServerReqGateway launches the gateway and waits for its listener. The
// registry is NOT started here: the whole point of Stage 17a is that it comes
// up on the first request, with the first client's capabilities in hand.
func startServerReqGateway(t *testing.T, cfg *config.Config) *serverReqGateway {
	t.Helper()
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")

	addrCh := make(chan net.Addr, 1)
	hs.onListen = func(a net.Addr) { addrCh <- a }

	ctx, cancel := context.WithCancel(context.Background())
	g := &serverReqGateway{
		client:    &http.Client{},
		hs:        hs,
		capsFile:  cfg.Upstreams[0].Env["FAKE_CAPS_FILE"],
		cancel:    cancel,
		serveDone: make(chan error, 1),
	}
	go func() { g.serveDone <- hs.Serve(ctx) }()

	select {
	case addr := <-addrCh:
		g.base = "http://" + addr.String()
	case err := <-g.serveDone:
		cancel()
		t.Fatalf("Serve exited before binding: %v", err)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("Serve never bound its listener")
	}
	return g
}

// stop cancels Serve and waits for it to tear the registry down.
func (g *serverReqGateway) stop() {
	g.cancel()
	g.waitServe(10 * time.Second)
}

// waitServe returns the error Serve exited with, or errServeRunning if it has
// not exited within timeout. The result is remembered, so stop() may be called
// after a test has already collected it.
func (g *serverReqGateway) waitServe(timeout time.Duration) error {
	if g.serveGot {
		return g.serveRes
	}
	select {
	case err := <-g.serveDone:
		g.serveRes, g.serveGot = err, true
		return err
	case <-time.After(timeout):
		return errServeRunning
	}
}

// send posts one JSON-RPC message, with the session header when sid is
// non-empty. It returns errors instead of failing, so it is safe from a
// goroutine (a blocking tools/call always runs on one).
func (g *serverReqGateway) send(msg *mcp.Message, sid string) (*http.Response, error) {
	body, err := mcp.Encode(msg)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, g.base+"/mcp", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if sid != "" {
		req.Header.Set(sessionHeader, sid)
	}
	return g.client.Do(req)
}

// post is send for the test goroutine, where a failure is fatal.
func (g *serverReqGateway) post(t *testing.T, msg *mcp.Message, sid string) *http.Response {
	t.Helper()
	resp, err := g.send(msg, sid)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return resp
}

// initializeResp runs one initialize declaring exactly rawCaps (a raw
// capabilities object) and hands back the whole response — E10 needs the body,
// everyone else just the id.
func (g *serverReqGateway) initializeResp(t *testing.T, rawCaps string, id json.RawMessage) *http.Response {
	t.Helper()
	return g.post(t, mcp.NewRequest(id, mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(rawCaps),
		ClientInfo:      mcp.Implementation{Name: "test-client", Version: "9.9.9"},
	})), "")
}

// initialize handshakes as a fresh client declaring rawCaps and returns the
// session id the gateway issued.
func (g *serverReqGateway) initialize(t *testing.T, rawCaps string) string {
	t.Helper()
	resp := g.initializeResp(t, rawCaps, mcp.IntID(nextTestID()))
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		t.Fatalf("initialize status = %d, want 200", resp.StatusCode)
	}
	sid := resp.Header.Get(sessionHeader)
	if msg := decodeBody(t, resp); msg.Error != nil {
		t.Fatalf("initialize error: %v", msg.Error)
	}
	if sid == "" {
		t.Fatalf("initialize response carries no %s header", sessionHeader)
	}
	return sid
}

// sseStream is one open GET /mcp stream plus its decoded events.
type sseStream struct {
	resp   *http.Response
	events <-chan sseEvent
}

func (s *sseStream) close() { _ = s.resp.Body.Close() }

// openStream opens the session's server→client SSE stream.
func (g *serverReqGateway) openStream(t *testing.T, sid string) *sseStream {
	t.Helper()
	resp := openSSE(t, g.base, sessionHeaders(sid))
	requireSSEOpen(t, resp)
	return &sseStream{resp: resp, events: sseEvents(resp.Body)}
}

// toolCall carries the outcome of a tools/call made from a goroutine.
type toolCall struct {
	msg *mcp.Message
	err error
}

// callAsync issues tools/call on its own goroutine: with a server→client
// request in flight the POST does not answer until the whole round trip is
// done, so the test goroutine has to stay free to read the SSE stream and post
// the answer.
func (g *serverReqGateway) callAsync(sid, tool string) <-chan toolCall {
	out := make(chan toolCall, 1)
	go func() {
		resp, err := g.send(mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodToolsCall,
			mcp.MustParams(mcp.ToolsCallParams{Name: tool})), sid)
		if err != nil {
			out <- toolCall{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			out <- toolCall{err: fmt.Errorf("tools/call status = %d, want 200", resp.StatusCode)}
			return
		}
		var msg mcp.Message
		if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
			out <- toolCall{err: fmt.Errorf("decode tools/call response: %w", err)}
			return
		}
		out <- toolCall{msg: &msg}
	}()
	return out
}

// awaitCall waits for a tools/call started with callAsync.
func awaitCall(t *testing.T, ch <-chan toolCall, timeout time.Duration) *mcp.Message {
	t.Helper()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("tools/call: %v", res.err)
		}
		return res.msg
	case <-time.After(timeout):
		t.Fatalf("tools/call did not complete within %v", timeout)
		return nil
	}
}

// call is the simple blocking form, for calls that need no round trip.
func (g *serverReqGateway) call(t *testing.T, sid, tool string) *mcp.Message {
	t.Helper()
	return awaitCall(t, g.callAsync(sid, tool), 20*time.Second)
}

// answer posts the client's answer to a proxied server→client request, echoing
// the gateway-minted id, and asserts the transport accepted it.
func (g *serverReqGateway) answer(t *testing.T, sid string, id json.RawMessage, result string) {
	t.Helper()
	resp := g.post(t, mcp.NewResult(id, json.RawMessage(result)), sid)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("answer POST status = %d, want 202", resp.StatusCode)
	}
}

// awaitServerReq takes the next SSE event and asserts it is the expected
// server→client request: the right method, a gateway-minted STRING id with the
// spec table's prefix (never the upstream's own "fake-…" id).
func awaitServerReq(t *testing.T, s *sseStream, method, idPrefix string) *mcp.Message {
	t.Helper()
	ev := waitEvent(t, s.events, 15*time.Second, method)
	if !ev.msg.IsRequest() || ev.msg.Method != method {
		t.Fatalf("SSE event = method %q id %s err %+v, want a %s request",
			ev.msg.Method, ev.msg.ID, ev.msg.Error, method)
	}
	var gatewayID string
	if err := json.Unmarshal(ev.msg.ID, &gatewayID); err != nil || !strings.HasPrefix(gatewayID, idPrefix) {
		t.Fatalf("request id = %s, want a gateway-minted string id prefixed %q (upstream id leaked?)",
			ev.msg.ID, idPrefix)
	}
	return ev.msg
}

// declined reports whether the tool result carries the gateway's
// {"action":"decline"} refusal. The check goes through the ESCAPED form on
// purpose: the fakeserver embeds the raw elicitation result inside the tool's
// text, so what lands in the response is "|elicited={\"action\":\"decline\"}".
func declined(result json.RawMessage) bool {
	return strings.Contains(string(result), "elicited=") &&
		strings.Contains(string(result), `\"action\":\"decline\"`)
}

// E1. TestHTTPElicitationRoundTrip is the whole feature in one test: the
// upstream's elicitation/create reaches the client as an SSE event on its own
// session's stream, params verbatim and under a gateway-minted id; the client
// answers with a plain POST; the upstream receives that answer under its OWN
// id (if the id were not rewritten back the fakeserver would never match it and
// would sit out its 10s timeout) and finishes its tools/call with it.
func TestHTTPElicitationRoundTrip(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	sid := gw.initialize(t, `{"elicitation":{}}`)
	stream := gw.openStream(t, sid)
	defer stream.close()

	call := gw.callAsync(sid, "web__ask")

	req := awaitServerReq(t, stream, mcp.MethodElicitationCreate, "elicit-")
	if !strings.Contains(string(req.Params), `"need input"`) ||
		!strings.Contains(string(req.Params), `"requestedSchema"`) {
		t.Errorf("elicitation params not proxied verbatim: %s", req.Params)
	}

	gw.answer(t, sid, req.ID, `{"action":"accept","content":{"answer":"42"}}`)

	msg := awaitCall(t, call, 20*time.Second)
	if msg.Error != nil {
		t.Fatalf("tools/call error: %v", msg.Error)
	}
	if !strings.Contains(string(msg.Result), "elicited=") || !strings.Contains(string(msg.Result), "42") {
		t.Errorf("tool result did not embed the client's answer: %s", msg.Result)
	}
}

// E12. TestHTTPSamplingRoundTrip repeats E1 for sampling/createMessage. Its
// real subject is that nothing in the transport is elicitation-specific: the
// method travels in the event, and the capability that gates delivery comes
// from the registry's spec table (registry.CapabilityForMethod), so a
// hand-written "method → capability" copy in this package would fail here.
func TestHTTPSamplingRoundTrip(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_SAMPLING": "1"}))
	defer gw.stop()

	sid := gw.initialize(t, `{"sampling":{}}`)
	stream := gw.openStream(t, sid)
	defer stream.close()

	call := gw.callAsync(sid, "web__ask")

	req := awaitServerReq(t, stream, mcp.MethodSamplingCreateMessage, "sampling-")
	if !strings.Contains(string(req.Params), `"maxTokens"`) {
		t.Errorf("sampling params not proxied verbatim: %s", req.Params)
	}

	gw.answer(t, sid, req.ID,
		`{"role":"assistant","content":{"type":"text","text":"sure"},"model":"test-model"}`)

	msg := awaitCall(t, call, 20*time.Second)
	if msg.Error != nil {
		t.Fatalf("tools/call error: %v", msg.Error)
	}
	if !strings.Contains(string(msg.Result), "sampled=") || !strings.Contains(string(msg.Result), "test-model") {
		t.Errorf("tool result did not embed the sampling answer: %s", msg.Result)
	}
}

// E13. TestHTTPRootsListServedAndCached covers the third method plus the one
// piece of per-method machinery the registry keeps: one client answer serves
// every later asker from the cache, so a SECOND tools/call must complete
// WITHOUT a second question on the stream.
func TestHTTPRootsListServedAndCached(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ROOTS": "1"}))
	defer gw.stop()

	sid := gw.initialize(t, `{"roots":{}}`)
	stream := gw.openStream(t, sid)
	defer stream.close()

	call := gw.callAsync(sid, "web__ask")

	req := awaitServerReq(t, stream, mcp.MethodRootsList, "roots-")
	if len(req.Params) != 0 {
		t.Errorf("roots/list carried params %s, want none", req.Params)
	}

	gw.answer(t, sid, req.ID, `{"roots":[{"uri":"file:///work","name":"work"}]}`)

	msg := awaitCall(t, call, 20*time.Second)
	if msg.Error != nil {
		t.Fatalf("tools/call error: %v", msg.Error)
	}
	if !strings.Contains(string(msg.Result), "roots=") || !strings.Contains(string(msg.Result), "file:///work") {
		t.Errorf("tool result did not embed the roots answer: %s", msg.Result)
	}

	// Second call: answered from the registry's cache. Positive signal first
	// (the call completed), then the bounded check that nothing was asked.
	second := awaitCall(t, gw.callAsync(sid, "web__ask"), 20*time.Second)
	if !strings.Contains(string(second.Result), "file:///work") {
		t.Errorf("second call was not served from the roots cache: %s", second.Result)
	}
	assertNoServerReq(t, stream, mcp.MethodRootsList, 500*time.Millisecond)
}

// assertNoServerReq drains a stream for the given window and fails if a
// server→client request of that method shows up. Always used AFTER a positive
// signal (the round trip finished), so the window bounds a settled system
// rather than racing one.
func assertNoServerReq(t *testing.T, s *sseStream, method string, window time.Duration) {
	t.Helper()
	deadline := time.After(window)
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				return // stream ended: certainly no further request
			}
			if ev.msg.Method == method {
				t.Fatalf("unexpected %s on this stream: %s", method, ev.msg.ID)
			}
		case <-deadline:
			return
		}
	}
}

// E2. TestHTTPLazyStartPinsFirstClientCaps checks the two halves of the lazy
// start by the only evidence that cannot be faked — the bytes of the upstream
// handshake. Before any POST the upstream has not been launched at all (no caps
// file); the first initialize brings it up, exactly once, declaring what THAT
// client declared. A SetClientServerRequestCaps that ran after Start (or a
// Start left in Serve's prologue) shows up here as a handshake without
// elicitation.
func TestHTTPLazyStartPinsFirstClientCaps(t *testing.T) {
	cfg := serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"})
	gw := startServerReqGateway(t, cfg)
	defer gw.stop()

	if _, err := os.Stat(gw.capsFile); err == nil {
		t.Fatalf("upstream handshake happened before the first client request: %s", capsLines(t, gw.capsFile))
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat caps file: %v", err)
	}

	gw.initialize(t, `{"elicitation":{}}`)

	lines := capsLines(t, gw.capsFile)
	if len(lines) != 1 {
		t.Fatalf("upstream received %d handshakes, want exactly 1: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "elicitation") {
		t.Errorf("handshake capabilities = %s, want the first client's elicitation declared", lines[0])
	}
}

// E3. TestHTTPSecondSessionDoesNotWidenPinnedCaps pins the deliberate
// limitation: the set declared upstream belongs to the FIRST client and cannot
// be widened later, because MCP 2025-06-18 has no re-negotiation and the
// handshake has already happened. A second client declaring elicitation
// therefore gets an upstream that never heard of it — and, being
// negotiation-respecting, does not ask.
func TestHTTPSecondSessionDoesNotWidenPinnedCaps(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	gw.initialize(t, `{}`) // first client: declares nothing — this is what gets pinned

	second := gw.initialize(t, `{"elicitation":{}}`)
	stream := gw.openStream(t, second)
	defer stream.close()

	msg := gw.call(t, second, "web__ask")
	if msg.Error != nil {
		t.Fatalf("tools/call error: %v", msg.Error)
	}
	if !strings.Contains(string(msg.Result), "elicit-skipped") {
		t.Errorf("upstream asked although the pinned handshake declared nothing: %s", msg.Result)
	}

	lines := capsLines(t, gw.capsFile)
	if len(lines) != 1 {
		t.Fatalf("upstream received %d handshakes, want exactly 1 (the registry starts once): %v", len(lines), lines)
	}
	if strings.Contains(lines[0], "elicitation") {
		t.Errorf("a later client widened what was declared upstream: %s", lines[0])
	}
}

// E4. TestHTTPOtherSessionInitializeKeepsRegistryGate guards the recordCaps
// split: handleInitialize runs for EVERY session's handshake, and if the HTTP
// dispatcher wrote the client's capabilities into the registry there, the
// second (capability-less) client would silently revoke the first one's — the
// registry's runtime gate would then refuse the elicitation the first client
// can perfectly well answer.
func TestHTTPOtherSessionInitializeKeepsRegistryGate(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	capable := gw.initialize(t, `{"elicitation":{}}`)
	stream := gw.openStream(t, capable)
	defer stream.close()

	gw.initialize(t, `{}`) // a second client handshakes with nothing declared

	call := gw.callAsync(capable, "web__ask")
	req := awaitServerReq(t, stream, mcp.MethodElicitationCreate, "elicit-")
	gw.answer(t, capable, req.ID, `{"action":"accept","content":{"answer":"42"}}`)

	msg := awaitCall(t, call, 20*time.Second)
	if !strings.Contains(string(msg.Result), "42") {
		t.Errorf("round trip broke after another session initialized: %s", msg.Result)
	}
}

// E5. TestHTTPServerReqDeliveredOnlyToCapableSession is the unicast contract:
// with two clients streaming, the question goes to the one that declared the
// capability and to nobody else. A broadcast (the way catalog NOTIFICATIONS
// work) would put the same form in front of two humans for one answer.
//
// The tools/call is deliberately made by the INCAPABLE session, and it is also
// the most recently active one when the question is minted. Two things follow,
// both intended: routing is decided by the declared capability alone, never by
// "whoever called" or "whoever spoke last" (drop the capability gate and this
// request lands on the wrong stream); and the answer legitimately comes from a
// different session than the caller — with the registry's gate being
// process-wide, the gateway has ONE client conceptually, spread over sessions.
func TestHTTPServerReqDeliveredOnlyToCapableSession(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	capable := gw.initialize(t, `{"elicitation":{}}`)
	capableStream := gw.openStream(t, capable)
	defer capableStream.close()

	other := gw.initialize(t, `{}`)
	otherStream := gw.openStream(t, other)
	defer otherStream.close()

	call := gw.callAsync(other, "web__ask")
	req := awaitServerReq(t, capableStream, mcp.MethodElicitationCreate, "elicit-")
	gw.answer(t, capable, req.ID, `{"action":"accept","content":{"answer":"42"}}`)

	msg := awaitCall(t, call, 20*time.Second)
	if !strings.Contains(string(msg.Result), "42") {
		t.Fatalf("round trip through the capable session failed: %s", msg.Result)
	}
	// Positive signal is in: the second stream can now be checked for silence.
	assertNoServerReq(t, otherStream, mcp.MethodElicitationCreate, 500*time.Millisecond)
}

// E6. TestHTTPServerReqNoStreamRefusedDecline: the client declared the
// capability (so the upstream was told, and asks) but has no GET stream open,
// leaving nowhere to deliver. The upstream must be refused at once in the
// spec's soft shape rather than waiting out its own timeout.
func TestHTTPServerReqNoStreamRefusedDecline(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	sid := gw.initialize(t, `{"elicitation":{}}`) // capable, but never opens GET /mcp

	start := time.Now()
	msg := gw.call(t, sid, "web__ask")
	elapsed := time.Since(start)

	if msg.Error != nil {
		t.Fatalf("tools/call error: %v", msg.Error)
	}
	// The same 5s threshold as TestStdioElicitationNotAskedWithoutClientCapability:
	// well under the fakeserver's 10s elicitation timeout, so "nobody refused
	// it" surfaces as a failure rather than a slow pass.
	if elapsed > 5*time.Second {
		t.Errorf("tools/call took %v — the undeliverable elicitation was not refused promptly", elapsed)
	}
	if strings.Contains(string(msg.Result), "elicit-timeout") {
		t.Fatalf("the upstream waited out its own timeout: nothing refused the request: %s", msg.Result)
	}
	if !declined(msg.Result) {
		t.Errorf("upstream did not receive the spec's decline result: %s", msg.Result)
	}
	if strings.Contains(string(msg.Result), `"isError":true`) {
		t.Errorf("the refusal failed the whole tools/call, which the decline shape exists to avoid: %s", msg.Result)
	}
}

// E7. TestHTTPResponseFromForeignSessionDropped: only the session the question
// was put to may answer it. Without the ownership check, anyone holding a valid
// session could answer a stranger's elicitation by guessing the short,
// predictable gateway id — and the upstream would act on it.
func TestHTTPResponseFromForeignSessionDropped(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	owner := gw.initialize(t, `{"elicitation":{}}`)
	stream := gw.openStream(t, owner)
	defer stream.close()

	stranger := gw.initialize(t, `{"elicitation":{}}`) // valid session, never asked

	call := gw.callAsync(owner, "web__ask")
	req := awaitServerReq(t, stream, mcp.MethodElicitationCreate, "elicit-")

	// The stranger answers first, with a distinguishable value.
	resp := gw.post(t, mcp.NewResult(req.ID, json.RawMessage(`{"action":"accept","content":{"answer":"66"}}`)), stranger)
	if resp.StatusCode != http.StatusAccepted {
		_ = resp.Body.Close()
		t.Fatalf("foreign answer status = %d, want 202 (the same status a legitimate one gets: "+
			"a 4xx would tell a stranger the id exists)", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// The call must still be waiting: the stranger's answer went nowhere.
	select {
	case res := <-call:
		t.Fatalf("tools/call completed on a foreign session's answer: %+v", res.msg)
	case <-time.After(500 * time.Millisecond):
	}

	gw.answer(t, owner, req.ID, `{"action":"accept","content":{"answer":"42"}}`)
	msg := awaitCall(t, call, 20*time.Second)
	if !strings.Contains(string(msg.Result), "42") {
		t.Errorf("upstream did not get the owner's answer: %s", msg.Result)
	}
	if strings.Contains(string(msg.Result), "66") {
		t.Errorf("upstream acted on a foreign session's answer: %s", msg.Result)
	}
}

// E8. TestHTTPSessionTerminationRefusesDeliveredRequest: the client is asked,
// never answers, and then terminates its session (DELETE /mcp). The upstream is
// inside tools/call, so waiting out the registry's five-minute deadline would
// hang it for minutes — the session's death must refuse the request at once.
func TestHTTPSessionTerminationRefusesDeliveredRequest(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	sid := gw.initialize(t, `{"elicitation":{}}`)
	stream := gw.openStream(t, sid)
	defer stream.close()

	call := gw.callAsync(sid, "web__ask")
	awaitServerReq(t, stream, mcp.MethodElicitationCreate, "elicit-") // delivered, unanswered

	req, err := http.NewRequest(http.MethodDelete, gw.base+"/mcp", nil)
	if err != nil {
		t.Fatalf("build DELETE: %v", err)
	}
	req.Header.Set(sessionHeader, sid)
	resp, err := gw.client.Do(req)
	if err != nil {
		t.Fatalf("DELETE /mcp: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}

	start := time.Now()
	msg := awaitCall(t, call, 20*time.Second)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("tools/call took %v after the session was terminated — nothing refused the pending request", elapsed)
	}
	if strings.Contains(string(msg.Result), "elicit-timeout") {
		t.Fatalf("the upstream waited out its own timeout after the session died: %s", msg.Result)
	}
	if !declined(msg.Result) {
		t.Errorf("upstream did not receive the decline result: %s", msg.Result)
	}
}

// E9. TestHTTPClosedStreamStopsDelivery: the session stays alive but its only
// stream is gone. The channel must be deregistered when the handler returns —
// otherwise the router would hand the question to a channel nobody reads, and
// the upstream would wait out its full timeout.
func TestHTTPClosedStreamStopsDelivery(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	sid := gw.initialize(t, `{"elicitation":{}}`)
	stream := gw.openStream(t, sid)
	stream.close()

	// The handler's deferred cleanup runs some time after the client hangs up,
	// so poll the session's stream tally rather than assuming (H17's pattern).
	sess, _, ok := gw.hs.sessions.touch(sid)
	if !ok {
		t.Fatal("session vanished right after its stream was opened")
	}
	deadline := time.Now().Add(10 * time.Second)
	for gw.hs.sessions.streamCount(sess) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the closed SSE stream was never deregistered")
		}
		time.Sleep(20 * time.Millisecond)
	}

	start := time.Now()
	msg := gw.call(t, sid, "web__ask")
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("tools/call took %v — the request went to a dead stream instead of being refused", elapsed)
	}
	if strings.Contains(string(msg.Result), "elicit-timeout") {
		t.Fatalf("the upstream waited out its own timeout: %s", msg.Result)
	}
	if !declined(msg.Result) {
		t.Errorf("upstream did not receive the decline result: %s", msg.Result)
	}
}

// E10. TestHTTPRegistryStartFailureAnswersClientAndStopsServe pins the
// operational contract the lazy start must not quietly change: a gateway whose
// upstreams cannot start is a misconfiguration, so the triggering client gets a
// diagnosable JSON-RPC -32603 (sanitized — no upstream names, no paths) and
// Serve still ends with the real error, exactly as it did when Start ran in the
// prologue. A process that stayed up answering -32603 forever would hide the
// failure from whatever supervises it.
func TestHTTPRegistryStartFailureAnswersClientAndStopsServe(t *testing.T) {
	cfg := serverReqCfg(t, nil)
	cfg.Upstreams[0].Command = filepath.Join(t.TempDir(), "no-such-binary")
	gw := startServerReqGateway(t, cfg)
	defer gw.stop()

	id := mcp.IntID(nextTestID())
	resp := gw.initializeResp(t, `{"elicitation":{}}`, id)
	// The failure is protocol-level, so it rides a 200 carrying a JSON-RPC
	// error — like every other protocol error on this transport. Pinned
	// explicitly because decodeBody ignores the status: without this the choice
	// was documented (plan §14.3) but unenforced (found by review).
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d with a JSON-RPC error body", resp.StatusCode, http.StatusOK)
	}
	msg := decodeBody(t, resp)

	if msg.Error == nil {
		t.Fatalf("initialize succeeded although no upstream could start: %s", msg.Result)
	}
	if string(msg.ID) != string(id) {
		t.Errorf("error id = %s, want the client's own %s", msg.ID, id)
	}
	if msg.Error.Code != mcp.CodeInternalError {
		t.Errorf("error code = %d, want %d", msg.Error.Code, mcp.CodeInternalError)
	}
	if msg.Error.Message != "gateway failed to start its upstreams" {
		t.Errorf("error message = %q, want the sanitized stdio wording", msg.Error.Message)
	}
	// Sanitized: nothing about the upstream or the filesystem may travel.
	if strings.Contains(msg.Error.Message, "web") || strings.Contains(msg.Error.Message, "no-such-binary") {
		t.Errorf("error message leaks internals: %q", msg.Error.Message)
	}

	if err := gw.waitServe(15 * time.Second); err == nil {
		t.Fatal("Serve returned nil after a failed registry start, want the start error")
	} else if err == errServeRunning {
		t.Fatal("Serve kept running after a failed registry start (the process would never exit non-zero)")
	}
}

// E11. TestHTTPConcurrentInitializesStartRegistryOnce: unlike stdio's single
// read loop, HTTP handlers run in parallel, so the bring-up must be guarded.
// Eight simultaneous initializes must all succeed and the upstream must see
// exactly ONE handshake — several would mean several launched child processes.
// Run under -race this also checks that startErr is published safely.
func TestHTTPConcurrentInitializesStartRegistryOnce(t *testing.T) {
	gw := startServerReqGateway(t, serverReqCfg(t, map[string]string{"FAKE_ELICIT": "1"}))
	defer gw.stop()

	const clients = 8
	type initResult struct {
		sid string
		err error
	}
	results := make(chan initResult, clients)
	start := make(chan struct{})
	for i := 0; i < clients; i++ {
		go func() {
			<-start // release them together, into the same bring-up window
			resp, err := gw.send(mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodInitialize,
				mcp.MustParams(mcp.InitializeParams{
					ProtocolVersion: mcp.ProtocolVersion,
					Capabilities:    json.RawMessage(`{"elicitation":{}}`),
					ClientInfo:      mcp.Implementation{Name: "racer", Version: "1.0.0"},
				})), "")
			if err != nil {
				results <- initResult{err: err}
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				results <- initResult{err: fmt.Errorf("status = %d, want 200", resp.StatusCode)}
				return
			}
			results <- initResult{sid: resp.Header.Get(sessionHeader)}
		}()
	}
	close(start)

	seen := map[string]bool{}
	for i := 0; i < clients; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("concurrent initialize: %v", res.err)
			}
			if res.sid == "" {
				t.Fatal("concurrent initialize returned no session id")
			}
			if seen[res.sid] {
				t.Fatalf("two concurrent clients were given the same session id %q", res.sid)
			}
			seen[res.sid] = true
		case <-time.After(30 * time.Second):
			t.Fatal("a concurrent initialize never came back")
		}
	}

	lines := capsLines(t, gw.capsFile)
	if len(lines) != 1 {
		t.Fatalf("upstream received %d handshakes under %d concurrent initializes, want exactly 1: %v",
			len(lines), clients, lines)
	}
}
