// fix/stage-19-debt p.3: stdio coverage for the MRTR bridge. The dispatch core
// is shared with HTTP (dispatchModern), but stdio has its own stakes:
//
//   - a modern client must NEVER receive the upstream's server→client
//     counter-request as a separate REQUEST frame on stdout — MRTR replaces it
//     with an input_required RESPONSE (TestMRTRStdioRoundtrip asserts the very
//     first frame read is the response, so a leaked elicitation/create frame
//     goes red on resp.Method != "");
//   - tools/call dispatches on its own goroutine (stdio.go:283–307) so a parked
//     call cannot wedge the read loop — a frame sent WHILE the call is in
//     flight must be answered, and answered FIRST, because the parked call's
//     input_required needs at least the 50ms collect window
//     (TestMRTRStdioParkDoesNotBlockLoop);
//   - the retry's framing: the final result comes back under the CLIENT's own
//     request id.
//
// Deliberately NOT covered here (plan p.3.3, recorded in the plan's
// «Расхождения»): stdio MRTR round trips for prompts/get and resources/read —
// the fakeserver only elicits on tools/call and extending it is out of bounds;
// the twins' bridge is covered by the registry smokes
// (TestMRTRGetPromptRoundtrip/TestMRTRReadResourceRoundtrip) and the HTTP tests
// (http_modern_mrtr_twins_test.go), and the dispatch itself is shared.

package transport

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// mrtrStdioCaps is the raw _meta.clientCapabilities a modern stdio client
// declares so the gateway announces elicitation to the upstream and the MRTR
// bridge may intercept the question.
const mrtrStdioCaps = `{"elicitation":{}}`

// startMRTRStdioGateway brings up a stdio gateway over one fakeserver upstream
// that elicits on every tools/call — the same stand the HTTP MRTR tests use.
func startMRTRStdioGateway(t *testing.T) (*fakeClient, context.CancelFunc, <-chan error) {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Upstreams: []config.Upstream{
			{Name: "web", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME":   "web",
				"FAKE_TOOLS":  "ask",
				"FAKE_ECHO":   "1",
				"FAKE_ELICIT": "1",
			}},
		},
	}
	return startServerWithConfig(t, cfg, nil)
}

// decodeStdioInputRequired fails the test unless msg is a response (no method)
// carrying an InputRequiredResult, and returns it.
func decodeStdioInputRequired(t *testing.T, msg *mcp.Message) mcp.InputRequiredResult {
	t.Helper()
	if msg.Method != "" {
		t.Fatalf("server sent a REQUEST frame %q to a modern client, want only responses (MRTR must intercept counter-requests)", msg.Method)
	}
	if msg.Error != nil {
		t.Fatalf("response carried an error, want input_required: %+v", msg.Error)
	}
	var ir mcp.InputRequiredResult
	if err := json.Unmarshal(msg.Result, &ir); err != nil {
		t.Fatalf("result is not an InputRequiredResult: %v (%s)", err, msg.Result)
	}
	if ir.ResultType != mcp.ResultTypeInputRequired {
		t.Fatalf("resultType = %q, want %q (result: %s)", ir.ResultType, mcp.ResultTypeInputRequired, msg.Result)
	}
	return ir
}

// TestMRTRStdioRoundtrip is the full stdio MRTR round trip: a modern tools/call
// whose upstream elicits mid-call is answered with an input_required RESPONSE
// (the first frame the client reads — never an elicitation/create request
// frame), and the retry with requestState + inputResponses completes the call
// under the client's own id.
func TestMRTRStdioRoundtrip(t *testing.T) {
	c, cancel, done := startMRTRStdioGateway(t)
	defer func() { cancel(); <-done }()

	callExtra := map[string]json.RawMessage{
		"name":      mcp.MustParams("web__ask"),
		"arguments": json.RawMessage(`{}`),
	}
	id := c.request(mcp.MethodToolsCall, modernParams(t, mcp.ProtocolVersionModern, mrtrStdioCaps, callExtra))

	// The FIRST frame back must be the response itself: were the bridge broken
	// and the elicitation proxied as a counter-request, readResponse would
	// return a message with Method == "elicitation/create" and this fails.
	resp := c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("input_required came under id %s, want the client's own %s", resp.ID, id)
	}
	ir := decodeStdioInputRequired(t, resp)
	if ir.RequestState == "" {
		t.Fatal("input_required carried an empty requestState")
	}
	if len(ir.InputRequests) != 1 {
		t.Fatalf("got %d input requests, want 1", len(ir.InputRequests))
	}
	var gatewayID string
	var env mcp.InputRequestEnvelope
	for gid, raw := range ir.InputRequests {
		gatewayID = gid
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("input request %q is not an envelope: %v", gid, err)
		}
	}
	if env.Method != mcp.MethodElicitationCreate {
		t.Errorf("envelope method = %q, want %q", env.Method, mcp.MethodElicitationCreate)
	}

	// Retry with the answer keyed by the gateway id; the final result must come
	// back under the RETRY's own client id.
	retryExtra := map[string]json.RawMessage{
		"name":           mcp.MustParams("web__ask"),
		"arguments":      json.RawMessage(`{}`),
		"requestState":   mcp.MustParams(ir.RequestState),
		"inputResponses": mcp.MustParams(map[string]json.RawMessage{gatewayID: json.RawMessage(`{"action":"accept","content":{"answer":"yes"}}`)}),
	}
	retryID := c.request(mcp.MethodToolsCall, modernParams(t, mcp.ProtocolVersionModern, mrtrStdioCaps, retryExtra))
	final := c.readResponse()
	if final.Method != "" {
		t.Fatalf("retry produced a REQUEST frame %q, want the final response", final.Method)
	}
	if string(final.ID) != string(retryID) {
		t.Fatalf("final came under id %s, want the retry's own %s", final.ID, retryID)
	}
	if final.Error != nil {
		t.Fatalf("retry returned an error, want the final result: %+v", final.Error)
	}
	var res struct {
		ResultType string          `json:"resultType"`
		Content    json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(final.Result, &res); err != nil {
		t.Fatalf("final result not an object: %v (%s)", err, final.Result)
	}
	if res.ResultType != mcp.ResultTypeComplete {
		t.Errorf("final resultType = %q, want %q", res.ResultType, mcp.ResultTypeComplete)
	}
	if len(res.Content) == 0 {
		t.Errorf("final result had no content: %s", final.Result)
	}
}

// TestMRTRStdioParkDoesNotBlockLoop pins the read-loop liveness while a modern
// call is in flight: the client fires tools/call and, WITHOUT waiting for its
// reply, immediately sends server/discover. Because tools/call dispatches on
// its own goroutine (stdio.go:283–307) and the eliciting call parks only after
// the 50ms collect window, the loop stays free and the discover RESPONSE must
// arrive FIRST, ahead of the input_required.
//
// Mutation-acceptance (run by the main session): dispatch tools/call inline
// (drop the goroutine branch in stdio.go) → the loop blocks in the call's
// dispatch until the park returns, input_required is written BEFORE the
// discover reply → the frame-order assert below goes RED.
func TestMRTRStdioParkDoesNotBlockLoop(t *testing.T) {
	c, cancel, done := startMRTRStdioGateway(t)
	defer func() { cancel(); <-done }()

	callExtra := map[string]json.RawMessage{
		"name":      mcp.MustParams("web__ask"),
		"arguments": json.RawMessage(`{}`),
	}
	callID := c.request(mcp.MethodToolsCall, modernParams(t, mcp.ProtocolVersionModern, mrtrStdioCaps, callExtra))
	// Sent while the call is still in flight (pre-park): the loop must serve it
	// without waiting for the call.
	discID := c.request(mcp.MethodServerDiscover, modernParams(t, mcp.ProtocolVersionModern, "", nil))

	// Frame order: the discover response first (inline, immediate), the
	// input_required second (≥ the 50ms collect window).
	disc := c.readResponse()
	if disc.Method != "" {
		t.Fatalf("server sent a REQUEST frame %q while a modern call was in flight", disc.Method)
	}
	if string(disc.ID) != string(discID) {
		t.Fatalf("first frame back is id %s, want the discover response %s — the in-flight tools/call is holding the read loop", disc.ID, discID)
	}
	if disc.Error != nil {
		t.Fatalf("server/discover returned an error: %+v", disc.Error)
	}
	if got := string(resultKeys(t, disc.Result)["resultType"]); got != string(mcp.MustParams(mcp.ResultTypeComplete)) {
		t.Errorf("discover resultType = %s, want %q", got, mcp.ResultTypeComplete)
	}

	park := c.readResponse()
	if string(park.ID) != string(callID) {
		t.Fatalf("second frame is id %s, want the tools/call input_required %s", park.ID, callID)
	}
	ir := decodeStdioInputRequired(t, park)
	var gatewayID string
	for gid := range ir.InputRequests {
		gatewayID = gid
	}
	if gatewayID == "" {
		t.Fatalf("input_required carried %d input requests, want 1", len(ir.InputRequests))
	}

	// The parked call still resumes normally afterwards: the retry frame was
	// readable (the loop never wedged) and the final arrives under its own id.
	retryExtra := map[string]json.RawMessage{
		"name":           mcp.MustParams("web__ask"),
		"arguments":      json.RawMessage(`{}`),
		"requestState":   mcp.MustParams(ir.RequestState),
		"inputResponses": mcp.MustParams(map[string]json.RawMessage{gatewayID: json.RawMessage(`{"action":"accept","content":{"answer":"yes"}}`)}),
	}
	retryID := c.request(mcp.MethodToolsCall, modernParams(t, mcp.ProtocolVersionModern, mrtrStdioCaps, retryExtra))
	final := c.readResponse()
	if final.Method != "" {
		t.Fatalf("retry produced a REQUEST frame %q, want the final response", final.Method)
	}
	if string(final.ID) != string(retryID) {
		t.Fatalf("final came under id %s, want the retry's own %s", final.ID, retryID)
	}
	if final.Error != nil {
		t.Fatalf("retry returned an error, want the final result: %+v", final.Error)
	}
	var res struct {
		ResultType string          `json:"resultType"`
		Content    json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(final.Result, &res); err != nil {
		t.Fatalf("final result not an object: %v (%s)", err, final.Result)
	}
	if res.ResultType != mcp.ResultTypeComplete {
		t.Errorf("final resultType = %q, want %q", res.ResultType, mcp.ResultTypeComplete)
	}
	if len(res.Content) == 0 {
		t.Errorf("final result had no content: %s", final.Result)
	}
}
