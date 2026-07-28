package transport

// Stage 16 — end-to-end tests for server-side Mcp-Session-Id over the
// client-facing HTTP transport: issuing an id on initialize, the strict gate on
// everything else (400 without a header, 404 with an unknown or expired one),
// DELETE /mcp termination, the per-session client identity that reaches the
// call log, and the cap/expiry bounds of the store. The store's own unit tests
// live in session_test.go; here everything goes through REAL HTTP round-trips.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// startSessionGateway is startHTTPGateway's Stage 16 sibling: it also hands
// back the *httpServer, so a test can shrink the session store's TTL or cap
// (both unexported fields, adjusted BEFORE any request is served), and it wires
// a REAL call log into callLogPath when one is given — the only way to assert
// what CallRecord.Client actually recorded.
func startSessionGateway(t *testing.T, callLogPath string) (*httptest.Server, *httpServer, func()) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "github", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME":  "github",
				"FAKE_TOOLS": "search",
				"FAKE_ECHO":  "1",
			}},
		},
	}
	var callLog logging.CallLog
	if callLogPath != "" {
		cl, err := logging.NewCallLog(callLogPath)
		if err != nil {
			t.Fatalf("open call log: %v", err)
		}
		callLog = cl
	}
	reg := registry.New(cfg, quietLogger(), callLog, noopPayloadLog(), true, "0.0.0-test")
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry Start: %v", err)
	}
	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", hs.handleMCP)
	srv := httptest.NewServer(mux)

	return srv, hs, func() {
		srv.Close()
		_ = reg.Close()
		if callLog != nil {
			_ = callLog.Close()
		}
	}
}

// initClient performs initialize with a specific clientInfo and returns the
// issued session id together with the raw response, so a caller can also check
// the header on a RE-initialize.
func initClient(t *testing.T, srv *httptest.Server, name, version, sid string) (string, *http.Response) {
	t.Helper()
	msg := mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodInitialize, mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      mcp.Implementation{Name: name, Version: version},
	}))
	var resp *http.Response
	if sid == "" {
		resp = post(t, srv, msg)
	} else {
		resp = postSession(t, srv, msg, sid)
	}
	return resp.Header.Get(sessionHeader), resp
}

// sendMethod is a bare DELETE/GET/POST with an explicit session header value
// ("" = no header at all), for the cases where only the status matters.
func sendMethod(t *testing.T, srv *httptest.Server, method, sid string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+"/mcp", nil)
	if err != nil {
		t.Fatalf("build %s request: %v", method, err)
	}
	if sid != "" {
		req.Header.Set(sessionHeader, sid)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s /mcp: %v", method, err)
	}
	return resp
}

// callLogRecords reads every CallRecord written to path so far.
func callLogRecords(t *testing.T, path string) []logging.CallRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read call log: %v", err)
	}
	var out []logging.CallRecord
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var rec logging.CallRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode call log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// H1. TestHTTPInitializeIssuesSessionID: the reply to initialize carries a
// non-empty Mcp-Session-Id, and two handshakes get two DIFFERENT ids — the
// header is minted per session, not a constant.
func TestHTTPInitializeIssuesSessionID(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	first, resp1 := initClient(t, srv, "client-a", "1.0.0", "")
	_ = resp1.Body.Close()
	if first == "" {
		t.Fatalf("initialize response carries no %s header", sessionHeader)
	}
	second, resp2 := initClient(t, srv, "client-b", "2.0.0", "")
	_ = resp2.Body.Close()
	if second == "" {
		t.Fatalf("second initialize response carries no %s header", sessionHeader)
	}
	if first == second {
		t.Fatalf("both initializes got session id %q, want two distinct sessions", first)
	}
}

// H2. TestHTTPRequestWithoutSessionRejected400: everything but initialize is
// 400 without the header — a request, a notification AND ping. ping is the
// deliberate policy call recorded in handlePost's comment: the lifecycle spec
// would allow it before the handshake, but the transport rule is stated at
// server level and a second exemption would only blur it.
func TestHTTPRequestWithoutSessionRejected400(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	cases := []struct {
		name string
		msg  *mcp.Message
	}{
		{"request", mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil)},
		{"notification", mcp.NewNotification(mcp.NotifInitialized, nil)},
		{"ping", mcp.NewRequest(mcp.IntID(2), mcp.MethodPing, nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := post(t, srv, tc.msg)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s without a session: status = %d, want 400", tc.name, resp.StatusCode)
			}
		})
	}
}

// H3. TestHTTPRequestWithUnknownSessionRejected404: an id the gateway never
// issued is 404 on POST and on GET alike — the spec's "session is gone,
// re-initialize" signal, the mirror of how the gateway itself reacts to a 404
// from an HTTP upstream (internal/upstream/http.go).
func TestHTTPRequestWithUnknownSessionRejected404(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	const bogus = "deadbeefdeadbeefdeadbeefdeadbeef"

	resp := postSession(t, srv, mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil), bogus)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST with an unknown session: status = %d, want 404", resp.StatusCode)
	}

	resp = sendMethod(t, srv, http.MethodGet, bogus)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET with an unknown session: status = %d, want 404", resp.StatusCode)
	}
}

// H4. TestHTTPDeleteTerminatesSession: DELETE with a valid id frees the session
// (204); afterwards the same id is unknown to POST (404) and a repeated DELETE
// is 404 too.
func TestHTTPDeleteTerminatesSession(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	sid := initSession(t, srv, nil)

	resp := sendMethod(t, srv, http.MethodDelete, sid)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE with a valid session: status = %d, want 204", resp.StatusCode)
	}

	resp = postSession(t, srv, mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil), sid)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST after DELETE: status = %d, want 404 (session state must be gone)", resp.StatusCode)
	}

	resp = sendMethod(t, srv, http.MethodDelete, sid)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("second DELETE of the same session: status = %d, want 404", resp.StatusCode)
	}
}

// H5. TestHTTPDeleteWithoutSession400: DELETE has nothing to terminate without
// the header.
func TestHTTPDeleteWithoutSession400(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	resp := sendMethod(t, srv, http.MethodDelete, "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("DELETE without a session header: status = %d, want 400", resp.StatusCode)
	}
}

// H6. TestHTTPSSERequiresSession: the GET SSE stream sits behind the same gate
// — 400 with no header, 404 with a bogus one, an open stream with a valid one.
func TestHTTPSSERequiresSession(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	resp := openSSE(t, srv.URL, nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET without a session header: status = %d, want 400", resp.StatusCode)
	}

	resp = openSSE(t, srv.URL, sessionHeaders("0123456789abcdef0123456789abcdef"))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET with an unknown session: status = %d, want 404", resp.StatusCode)
	}

	sid := initSession(t, srv, nil)
	resp = openSSE(t, srv.URL, sessionHeaders(sid))
	defer func() { _ = resp.Body.Close() }()
	requireSSEOpen(t, resp)
}

// H7. TestHTTPDeleteClosesSSEStream: terminating a session must end its open
// SSE stream, not leave the handler parked in its event loop until the client
// or the process goes away. Modelled on
// TestHTTPSSEGracefulShutdownClosesStream's bounded wait.
func TestHTTPDeleteClosesSSEStream(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	sid := initSession(t, srv, nil)
	resp := openSSE(t, srv.URL, sessionHeaders(sid))
	defer func() { _ = resp.Body.Close() }()
	requireSSEOpen(t, resp)
	events := sseEvents(resp.Body)

	del := sendMethod(t, srv, http.MethodDelete, sid)
	_ = del.Body.Close()
	if del.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: status = %d, want 204", del.StatusCode)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-events:
			if !ok {
				return // stream closed — pass
			}
			// A stray buffered event is fine; keep draining until close.
		case <-deadline:
			t.Fatal("SSE stream stayed open after its session was terminated")
		}
	}
}

// H8. TestHTTPCallRecordClientWholeSession is the tracker's own criterion: the
// audit log must attribute EVERY request of a session to the client that
// initialized it, and two clients must show up as two distinct signatures.
// Before Stage 16 the HTTP transport could only attribute the initialize
// request itself, so a tools/call landed in calls.jsonl with an empty client.
func TestHTTPCallRecordClientWholeSession(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "calls.jsonl")
	srv, _, cleanup := startSessionGateway(t, logPath)
	defer cleanup()

	callFor := func(sid, marker string) {
		t.Helper()
		resp := postSession(t, srv, mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{
			Name:      "github__search",
			Arguments: json.RawMessage(`{"marker":"` + marker + `"}`),
		})), sid)
		msg := decodeBody(t, resp)
		if msg.Error != nil {
			t.Fatalf("tools/call error: %v", msg.Error)
		}
	}

	sidA, respA := initClient(t, srv, "test-client", "9.9.9", "")
	_ = respA.Body.Close()
	sidB, respB := initClient(t, srv, "other-client", "0.0.1", "")
	_ = respB.Body.Close()
	if sidA == "" || sidB == "" || sidA == sidB {
		t.Fatalf("want two distinct session ids, got %q and %q", sidA, sidB)
	}

	callFor(sidA, "a")
	callFor(sidB, "b")

	var clients []string
	for _, rec := range callLogRecords(t, logPath) {
		if rec.Method == mcp.MethodToolsCall {
			clients = append(clients, rec.Client)
		}
	}
	if len(clients) != 2 {
		t.Fatalf("call log holds %d tools/call records, want 2: %v", len(clients), clients)
	}
	if clients[0] != "test-client/9.9.9" {
		t.Errorf("first tools/call recorded client = %q, want %q (identity must survive the whole session, not just initialize)", clients[0], "test-client/9.9.9")
	}
	if clients[1] != "other-client/0.0.1" {
		t.Errorf("second tools/call recorded client = %q, want %q (each session must be audited under ITS OWN client)", clients[1], "other-client/0.0.1")
	}
}

// H9. TestHTTPSessionExpiresAndClientReinitializes: with a shortened idle TTL a
// quiet session is 404 on its next request, and the client simply initializes
// again — the same degradation path the gateway itself takes when an HTTP
// upstream drops its session (internal/upstream/http.go resets sessionID on
// 404).
func TestHTTPSessionExpiresAndClientReinitializes(t *testing.T) {
	srv, hs, cleanup := startSessionGateway(t, "")
	defer cleanup()

	// Shrunk before the first request is served, so no handler ever reads this
	// field concurrently with the write.
	hs.sessions.idleTTL = 50 * time.Millisecond

	sid := initSession(t, srv, nil)
	time.Sleep(120 * time.Millisecond) // idle well past the TTL

	resp := postSession(t, srv, mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil), sid)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST on an expired session: status = %d, want 404", resp.StatusCode)
	}

	// Restore a generous TTL before creating the fresh session: the point of
	// this test is that a NEW session works after the OLD one expired, not
	// that a brand-new session must survive initSession's own HTTP round-trip
	// inside 50ms under a full -race build. Leaving idleTTL shortened here
	// would make the assertions below flaky for a reason unrelated to what
	// the test is supposed to prove.
	hs.sessions.idleTTL = time.Minute

	// Re-initialize: a brand-new session, immediately usable.
	fresh := initSession(t, srv, nil)
	if fresh == sid {
		t.Fatalf("re-initialize reused the expired session id %q", sid)
	}
	resp = postSession(t, srv, mcp.NewRequest(mcp.IntID(2), mcp.MethodToolsList, nil), fresh)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST on the fresh session: status = %d, want 200", resp.StatusCode)
	}
}

// H10. TestHTTPReinitializeKeepsSessionID: initialize sent WITH a valid session
// id re-initializes that session in place — same id echoed back, session still
// alive — and the refreshed clientInfo is what the next call is audited under.
func TestHTTPReinitializeKeepsSessionID(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "calls.jsonl")
	srv, _, cleanup := startSessionGateway(t, logPath)
	defer cleanup()

	sid, resp := initClient(t, srv, "first-name", "1.0.0", "")
	_ = resp.Body.Close()
	if sid == "" {
		t.Fatal("initialize issued no session id")
	}

	again, resp := initClient(t, srv, "second-name", "2.0.0", sid)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("re-initialize status = %d, want 200", resp.StatusCode)
	}
	if msg := decodeBody(t, resp); msg.Error != nil {
		t.Fatalf("re-initialize error: %v", msg.Error)
	}
	if again != sid {
		t.Fatalf("re-initialize returned session id %q, want the original %q", again, sid)
	}

	// The session is still usable, and the call is attributed to the NEW
	// clientInfo — proof reinit updated the stored identity.
	callResp := postSession(t, srv, mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{
		Name:      "github__search",
		Arguments: json.RawMessage(`{"marker":"reinit"}`),
	})), sid)
	if msg := decodeBody(t, callResp); msg.Error != nil {
		t.Fatalf("tools/call after re-initialize: %v", msg.Error)
	}

	var got []string
	for _, rec := range callLogRecords(t, logPath) {
		if rec.Method == mcp.MethodToolsCall {
			got = append(got, rec.Client)
		}
	}
	if len(got) != 1 || got[0] != "second-name/2.0.0" {
		t.Fatalf("tools/call clients = %v, want [second-name/2.0.0] (re-initialize must refresh the session's identity)", got)
	}
}

// H11. TestHTTPSessionCap503: with the store full a new initialize is refused
// 503 — an honest refusal instead of evicting somebody's live session — and
// freeing a slot with DELETE lets the next client in. Plan §2.2 is explicit
// that overflow must answer 503 "rather than by evicting the oldest session":
// the assertion right after the 503 — that the ALREADY-issued sid still works
// — is the part that actually distinguishes "honest refusal" from "silently
// evicted to make room". Without it this test would equally pass a mutant
// that frees a slot by evicting sid's session instead of refusing the new one.
func TestHTTPSessionCap503(t *testing.T) {
	srv, hs, cleanup := startSessionGateway(t, "")
	defer cleanup()

	hs.sessions.max = 1 // set before any request is served

	sid := initSession(t, srv, nil)

	_, resp := initClient(t, srv, "second", "1.0.0", "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("initialize with a full session store: status = %d, want 503", resp.StatusCode)
	}

	// The pre-existing session must be untouched by the refused initialize:
	// the cap rejects the NEWCOMER, it does not evict the incumbent.
	stillGood := postSession(t, srv, mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodToolsList, nil), sid)
	_ = stillGood.Body.Close()
	if stillGood.StatusCode != http.StatusOK {
		t.Fatalf("previously issued session after a 503: status = %d, want 200 (503 must refuse the new session, not evict an existing one)", stillGood.StatusCode)
	}

	resp = sendMethod(t, srv, http.MethodDelete, sid)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: status = %d, want 204", resp.StatusCode)
	}

	freed, resp := initClient(t, srv, "third", "1.0.0", "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initialize after freeing a slot: status = %d, want 200", resp.StatusCode)
	}
	if freed == "" {
		t.Fatal("initialize after freeing a slot issued no session id")
	}
}

// H12. TestHTTPReinitializeOfTerminatedSessionRejected404: re-initializing a
// session that has already been terminated must be a 404, not a resurrection.
// Note this exercises handlePost's ORDINARY gate (touch fails once the id is
// gone from the map) — it does not by itself single out the reinit fix, since
// touch already 404s before reinit would ever run. It is still worth pinning
// as an end-to-end regression test; the race-specific coverage for reinit's
// pointer-identity check lives in session_test.go
// (TestSessionStoreReinitOfStaleSessionRejected), which calls reinit directly
// on a session pointer that predates a terminate — the one scenario where
// touch's ordinary gate would NOT catch a bad reinit.
func TestHTTPReinitializeOfTerminatedSessionRejected404(t *testing.T) {
	srv, hs, cleanup := startSessionGateway(t, "")
	defer cleanup()

	sid := initSession(t, srv, nil)
	if !hs.sessions.terminate(sid) {
		t.Fatalf("terminate(%q): want true (session must exist before this call)", sid)
	}

	_, resp := initClient(t, srv, "reinit-after-terminate", "1.0.0", sid)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("re-initialize of a terminated session: status = %d, want 404", resp.StatusCode)
	}
	if got := resp.Header.Get(sessionHeader); got != "" {
		t.Fatalf("re-initialize of a terminated session set %s = %q, want no header at all", sessionHeader, got)
	}
}

// H13. TestHTTPInitializeWithUnknownSessionRejected404 pins plan §2.1's
// explicit decision: an initialize that carries a session id the gateway
// never issued is 404, exactly like any other request with an unknown id —
// it is NOT treated as "no header" (which would create a fresh session under
// a client-chosen id, letting a client pick its own Mcp-Session-Id).
func TestHTTPInitializeWithUnknownSessionRejected404(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	const bogus = "0123456789abcdef0123456789abcdef"
	_, resp := initClient(t, srv, "client", "1.0.0", bogus)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("initialize with an unknown session id: status = %d, want 404", resp.StatusCode)
	}
	if got := resp.Header.Get(sessionHeader); got != "" {
		t.Fatalf("initialize with an unknown session id set %s = %q, want no header at all", sessionHeader, got)
	}
}

// H14. TestHTTPUnknownSessionBeforeParseErrorReturns404: the session gate is
// checked BEFORE the request body is even read, so an unknown/invalid session
// id together with a garbled JSON body must still answer 404 — not the parse
// error's 400. Plan §2.1's stated ordering ("touch before decode") has real
// consequences: a client with a dead session should be told to re-initialize
// even if it also happened to send garbage, and the transport should not
// waste a MaxBytesReader pass on a body it's going to refuse anyway.
func TestHTTPUnknownSessionBeforeParseErrorReturns404(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	const bogus = "abcdefabcdefabcdefabcdefabcdefab"
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader("{not valid json"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(sessionHeader, bogus)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown session + malformed body: status = %d, want 404 (session must be checked before the body is read)", resp.StatusCode)
	}
}

// H16. TestHTTPResponseWithoutSessionRejected400: plan §2.1 puts JSON-RPC
// RESPONSES under the exact same gate as requests and notifications — "no
// header at all is 400" holds for every message shape POSTed to /mcp, not
// just msg.IsRequest(). This matters beyond Stage 16 itself: Stage 17 accepts
// a client's reply to a server→client request as a bare POST body that is a
// response (has an id and a result/error, no method), and without the gate
// applying to that shape there would be no session to route it against.
// TestHTTPRequestWithoutSessionRejected400 only covers a request, a
// notification and ping — none of those are IsResponse() — so this is
// deliberately a distinct case.
func TestHTTPResponseWithoutSessionRejected400(t *testing.T) {
	srv, _, cleanup := startSessionGateway(t, "")
	defer cleanup()

	resp := post(t, srv, mcp.NewResult(mcp.StringID("srv-req-1"), json.RawMessage(`{}`)))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("response body without a session header: status = %d, want 400", resp.StatusCode)
	}
}

// H15. TestHTTPStreamEndedRefreshesLastSeen: an open SSE stream pins a session
// alive (streams > 0 short-circuits expiry), which means the ordinary "does a
// stale lastSeen expire the session" path never fires while streaming — a
// stream that ran for hours would otherwise leave lastSeen exactly where
// streamStarted set it. Closing the stream must therefore stamp lastSeen
// fresh (as of the moment it closed), or a session with a long-lived stream
// would die the instant the stream ends, having accumulated no fresh
// activity in the store's eyes. This test shortens idleTTL, holds a stream
// open past it (proving streams>0 pins it), closes the stream, and expects
// the session to still answer normally right after — which only holds if
// streamEnded refreshed lastSeen.
func TestHTTPStreamEndedRefreshesLastSeen(t *testing.T) {
	srv, hs, cleanup := startSessionGateway(t, "")
	defer cleanup()

	// Hundreds of milliseconds, not microseconds: this must be comfortably
	// clear of scheduling noise under -race, since the test holds the stream
	// open for longer than the TTL on purpose.
	hs.sessions.idleTTL = 200 * time.Millisecond

	sid := initSession(t, srv, nil)
	resp := openSSE(t, srv.URL, sessionHeaders(sid))
	requireSSEOpen(t, resp)
	events := sseEvents(resp.Body)

	// Outlive the TTL while the stream is open: streams>0 must keep the
	// session alive regardless of how old lastSeen is.
	time.Sleep(300 * time.Millisecond)
	_ = resp.Body.Close()
	// Drain until the handler observes the close and streamEnded runs.
	for range events {
	}

	// If streamEnded had NOT refreshed lastSeen, lastSeen would still date
	// from streamStarted — already older than idleTTL — and this POST would
	// see an immediately-expired session (404). A fresh lastSeen means the
	// session gets a full new TTL window starting now.
	callResp := postSession(t, srv, mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodToolsList, nil), sid)
	defer func() { _ = callResp.Body.Close() }()
	if callResp.StatusCode != http.StatusOK {
		t.Fatalf("POST right after the stream closed: status = %d, want 200 (streamEnded must refresh lastSeen)", callResp.StatusCode)
	}
}

// H17. TestHTTPClosedStreamLetsSessionExpireAgain covers the OTHER half of the
// stream tally, the one H15 cannot see: that closing a stream actually
// DECREMENTS it, making the session mortal again.
//
// Why H15 misses it: with a broken streamEnded (no decrement, or the handler's
// defer dropped altogether) the tally stays above zero, expiredLocked
// short-circuits on streams > 0 forever, and the session becomes IMMORTAL — so
// H15's "POST right after close returns 200" still passes, for the wrong
// reason. An immortal session never expires and never frees its slot out of
// maxSessions: exactly the unbounded-map leak the idle TTL exists to prevent.
//
// This is the END-TO-END half of that coverage, and it is NOT redundant with
// session_test.go's TestSessionStoreStreamEndedRestoresMortality: that one
// drives the store directly and so can only catch a broken decrement INSIDE
// streamEnded. It cannot catch handleSSE forgetting to CALL streamEnded at all
// (a dropped `defer`), because it never goes through the handler. This test
// does, which is why it asserts on the tally observed via the store after a
// real client hangs up.
//
// So: assert the tally returns to zero after the client hangs up (polled — the
// handler's defer runs asynchronously in its own goroutine) and then that the
// session really does expire on idle, 404-ing its next request.
func TestHTTPClosedStreamLetsSessionExpireAgain(t *testing.T) {
	srv, hs, cleanup := startSessionGateway(t, "")
	defer cleanup()

	// Hundreds of milliseconds, not microseconds: the test deliberately waits
	// out this TTL, so it must be clear of scheduling noise under -race.
	const ttl = 300 * time.Millisecond
	hs.sessions.idleTTL = ttl

	sid := initSession(t, srv, nil)
	resp := openSSE(t, srv.URL, sessionHeaders(sid))
	requireSSEOpen(t, resp)
	events := sseEvents(resp.Body)

	// Grab the session pointer while the stream pins it alive, so the tally can
	// be observed under the store lock afterwards.
	sess, _, ok := hs.sessions.touch(sid)
	if !ok {
		t.Fatal("session vanished while its SSE stream was open")
	}

	_ = resp.Body.Close()
	for range events { // drain until the SSE reader sees the close
	}

	// The handler's deferred streamEnded runs in the handler goroutine, so poll
	// instead of assuming it has already happened.
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := hs.sessions.streamCount(sess)
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream tally stuck at %d after the stream closed: the session can never expire again and holds its slot out of maxSessions forever", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Mortal again: with no stream pinning it, plain idle must kill it.
	time.Sleep(ttl * 2)
	late := postSession(t, srv, mcp.NewRequest(mcp.IntID(nextTestID()), mcp.MethodToolsList, nil), sid)
	defer func() { _ = late.Body.Close() }()
	if late.StatusCode != http.StatusNotFound {
		t.Fatalf("POST on a session idle past the TTL after its stream closed: status = %d, want 404 (a closed stream must stop pinning the session alive)", late.StatusCode)
	}
}
