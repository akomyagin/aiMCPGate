package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// fakeUpstream is an in-process Upstream used to test the multiplexer without
// spawning processes. Each fake mints its own call-side ids from a private
// counter, so the test asserts the registry keeps id spaces separated.
type fakeUpstream struct {
	name      string
	tools     []string
	initErr   error
	listErr   error
	callErr   error
	callResp  *mcp.Message // if set, CallTool returns this response verbatim
	nextID    atomic.Int64
	lastArgs  json.RawMessage
	lastNamed string
}

func (f *fakeUpstream) Name() string { return f.name }

func (f *fakeUpstream) Initialize(context.Context) (*mcp.InitializeResult, error) {
	if f.initErr != nil {
		return nil, f.initErr
	}
	return &mcp.InitializeResult{ProtocolVersion: mcp.ProtocolVersion, ServerInfo: mcp.Implementation{Name: f.name}}, nil
}

func (f *fakeUpstream) ListTools(context.Context) ([]mcp.Tool, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]mcp.Tool, 0, len(f.tools))
	for _, t := range f.tools {
		out = append(out, mcp.Tool{Name: t, Description: "desc " + t, InputSchema: json.RawMessage(`{"type":"object"}`)})
	}
	return out, nil
}

func (f *fakeUpstream) ListResources(context.Context) ([]mcp.Resource, error) { return nil, nil }

func (f *fakeUpstream) CallTool(_ context.Context, name string, arguments json.RawMessage) (*mcp.Message, error) {
	f.lastNamed = name
	f.lastArgs = arguments
	if f.callErr != nil {
		return nil, f.callErr
	}
	if f.callResp != nil {
		return f.callResp, nil
	}
	// Use a private id counter to prove id spaces are separate from any other
	// upstream / the registry.
	id := mcp.IntID(f.nextID.Add(1))
	result := json.RawMessage(`{"content":[{"type":"text","text":"ok ` + name + `"}],"isError":false}`)
	return mcp.NewResult(id, result), nil
}

func (f *fakeUpstream) Close() error { return nil }

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// noopPayloadLog returns the disabled (no-op) payload log used by tests that do
// not exercise Stage 10 payload logging (empty path never errors).
func noopPayloadLog() logging.PayloadLog {
	p, _ := logging.NewPayloadLog("")
	return p
}

// newTestRegistry builds a registry whose starter returns the provided fakes by
// upstream name.
func newTestRegistry(t *testing.T, cfg *config.Config, callLog logging.CallLog, fakes map[string]*fakeUpstream) *Registry {
	t.Helper()
	return newTestRegistryWithPayload(t, cfg, callLog, noopPayloadLog(), fakes)
}

// newTestRegistryWithPayload is newTestRegistry plus an explicit payload log,
// for the Stage 10 opt-in payload tests.
func newTestRegistryWithPayload(t *testing.T, cfg *config.Config, callLog logging.CallLog, payloadLog logging.PayloadLog, fakes map[string]*fakeUpstream) *Registry {
	t.Helper()
	r := New(cfg, quietLogger(), callLog, payloadLog, true)
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) {
		f, ok := fakes[u.Name]
		if !ok {
			return nil, errors.New("no fake for " + u.Name)
		}
		return f, nil
	}
	return r
}

func TestRegistryAggregatesAndNamespaces(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "github", Enabled: true},
		{Name: "web", Enabled: true},
	}}
	fakes := map[string]*fakeUpstream{
		"github": {name: "github", tools: []string{"search", "create_issue"}},
		"web":    {name: "web", tools: []string{"search", "fetch"}}, // "search" collides
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	got := map[string]string{} // namespaced name → upstream
	for _, d := range r.Tools() {
		got[d.Name] = d.Upstream
	}
	want := map[string]string{
		"github__search":       "github",
		"github__create_issue": "github",
		"web__search":          "web",
		"web__fetch":           "web",
	}
	if len(got) != len(want) {
		t.Fatalf("catalog size %d want %d: %v", len(got), len(want), got)
	}
	for name, up := range want {
		if got[name] != up {
			t.Errorf("tool %q owned by %q want %q", name, got[name], up)
		}
	}
	// The colliding original name "search" must be disambiguated by namespace —
	// both survive, no clobbering.
	if _, ok := got["github__search"]; !ok {
		t.Error("github__search missing after collision")
	}
	if _, ok := got["web__search"]; !ok {
		t.Error("web__search missing after collision")
	}
}

func TestRegistryRoutesCallToOwner(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "github", Enabled: true},
		{Name: "web", Enabled: true},
	}}
	gh := &fakeUpstream{name: "github", tools: []string{"search"}}
	web := &fakeUpstream{name: "web", tools: []string{"search"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"github": gh, "web": web})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	args := json.RawMessage(`{"q":"golang"}`)
	resp, err := r.CallTool(context.Background(), "web__search", args)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("rpc error: %v", resp.Error)
	}
	// Only the "web" upstream should have been called, with the ORIGINAL name.
	if web.lastNamed != "search" {
		t.Errorf("web received name %q want original %q", web.lastNamed, "search")
	}
	if gh.lastNamed != "" {
		t.Errorf("github was called but should not have been (name=%q)", gh.lastNamed)
	}
	if string(web.lastArgs) != string(args) {
		t.Errorf("arguments not forwarded verbatim: %s", web.lastArgs)
	}
}

func TestRegistryUnknownToolErrors(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "github", Enabled: true}}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"github": {name: "github", tools: []string{"search"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	if _, err := r.CallTool(context.Background(), "nope__nope", nil); err == nil {
		t.Fatal("expected error for unknown tool")
	}
}

// TestRegistryCallToolFailureSanitized is a regression test: CallTool's
// returned error used to wrap the upstream name AGAIN (beyond what the
// namespaced tool name already discloses) and the raw internal transport
// error verbatim (`call %q on upstream %q: %w`) — since dispatch.go forwards
// this error text straight to the client under CodeInternalError, that
// leaked internal transport error strings (endpoints, connection details) to
// anyone holding a valid auth_token (found by code review). The client only
// ever gets back the tool name it itself supplied, matched exactly — proving
// nothing beyond that survives.
func TestRegistryCallToolFailureSanitized(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "secretname", Enabled: true}}}
	internal := errors.New("dial tcp 10.0.0.5:9443: connect: connection refused")
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"secretname": {name: "secretname", tools: []string{"search"}, callErr: internal},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	_, err := r.CallTool(context.Background(), "secretname__search", nil)
	if err == nil {
		t.Fatal("expected error from failing upstream")
	}
	const want = `call "secretname__search" failed`
	if err.Error() != want {
		t.Errorf("error = %q, want exactly %q (no upstream-name clause or internal transport detail)", err.Error(), want)
	}
}

// TestRegistryCallToolTimeoutSanitized confirms the one deliberate exception:
// a timeout is reported as a timeout (useful, non-sensitive signal for the
// caller), still with nothing beyond the client-supplied tool name.
func TestRegistryCallToolTimeoutSanitized(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "secretname", Enabled: true}}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"secretname": {name: "secretname", tools: []string{"search"}, callErr: context.DeadlineExceeded},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	_, err := r.CallTool(context.Background(), "secretname__search", nil)
	if err == nil {
		t.Fatal("expected error from failing upstream")
	}
	const want = `call "secretname__search" timed out`
	if err.Error() != want {
		t.Errorf("error = %q, want exactly %q (no upstream-name clause)", err.Error(), want)
	}
}

// A failing upstream must be isolated: the gateway keeps the healthy one.
func TestRegistryIsolatesFailedUpstream(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "ok", Enabled: true},
		{Name: "broken", Enabled: true},
		{Name: "disabled", Enabled: false},
	}}
	fakes := map[string]*fakeUpstream{
		"ok":       {name: "ok", tools: []string{"a"}},
		"broken":   {name: "broken", tools: []string{"b"}, initErr: errors.New("boom")},
		"disabled": {name: "disabled", tools: []string{"c"}},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start should not fail on isolated upstream error: %v", err)
	}
	defer r.Close()

	tools := r.Tools()
	if len(tools) != 1 || tools[0].Name != "ok__a" {
		t.Fatalf("expected only ok__a, got %+v", tools)
	}
}

// An aggregator with nothing to aggregate has no reason to keep running: if
// every configured upstream fails its handshake, Start must error instead of
// leaving the gateway serving an empty catalog forever.
func TestRegistryStartErrorsWhenAllUpstreamsFail(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "broken1", Enabled: true},
		{Name: "broken2", Enabled: true},
	}}
	fakes := map[string]*fakeUpstream{
		"broken1": {name: "broken1", initErr: errors.New("boom1")},
		"broken2": {name: "broken2", initErr: errors.New("boom2")},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	err := r.Start(context.Background())
	if err == nil {
		t.Fatal("Start should error when every upstream fails, got nil")
	}
	defer r.Close()

	// The error must name each upstream and its specific reason, not just
	// point back at the logs — that's the whole point of collecting them.
	for _, want := range []string{"broken1: handshake failed: boom1", "broken2: handshake failed: boom2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Start error = %q, want it to contain %q", err, want)
		}
	}
}

// Same reasoning applies with zero upstreams configured at all (or all
// disabled): there is nothing to serve, so Start must error rather than
// succeed with an empty catalog.
func TestRegistryStartErrorsWithNoEnabledUpstreams(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "disabled", Enabled: false},
	}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"disabled": {name: "disabled", tools: []string{"x"}},
	})
	err := r.Start(context.Background())
	if err == nil {
		t.Fatal("Start should error with zero enabled upstreams, got nil")
	}
	if !strings.Contains(err.Error(), "no upstream is enabled") {
		t.Errorf("Start error = %q, want it to explain nothing was enabled", err)
	}
	defer r.Close()
}

// The call log must record metadata but never the arguments (which may hold
// secrets like tokens).
func TestRegistryCallLogHasNoSecrets(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	var buf bytes.Buffer
	callLog := logging.NewCallLogWriter(&buf)

	r := newTestRegistry(t, cfg, callLog, map[string]*fakeUpstream{
		"web": {name: "web", tools: []string{"fetch"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	const secret = "SUPER_SECRET_TOKEN_abc123"
	args := json.RawMessage(`{"authorization":"Bearer ` + secret + `"}`)
	if _, err := r.CallTool(context.Background(), "web__fetch", args); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	logged := buf.String()
	if strings.Contains(logged, secret) {
		t.Fatalf("call log leaked secret:\n%s", logged)
	}
	// Sanity: it did record the call metadata.
	if !strings.Contains(logged, `"tool":"web__fetch"`) || !strings.Contains(logged, `"upstream":"web"`) {
		t.Fatalf("call log missing expected metadata:\n%s", logged)
	}
	if !strings.Contains(logged, `"ok":true`) {
		t.Fatalf("call log missing ok=true:\n%s", logged)
	}
}

// TestRegistryPayloadLogRecordsArgsAndResult is the Stage 10 end-to-end check:
// when the opt-in payload log is enabled, CallTool writes the raw arguments AND
// the upstream result to it (the deliberate difference from the audit log).
func TestRegistryPayloadLogRecordsArgsAndResult(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	var auditBuf, payloadBuf bytes.Buffer
	callLog := logging.NewCallLogWriter(&auditBuf)
	payloadLog := logging.NewPayloadLogWriter(&payloadBuf)

	r := newTestRegistryWithPayload(t, cfg, callLog, payloadLog, map[string]*fakeUpstream{
		"web": {name: "web", tools: []string{"fetch"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	const secret = "SUPER_SECRET_TOKEN_abc123"
	args := json.RawMessage(`{"authorization":"Bearer ` + secret + `"}`)
	if _, err := r.CallTool(context.Background(), "web__fetch", args); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	// The payload log DOES carry the raw arguments and a result (opt-in debug).
	var rec logging.PayloadRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(payloadBuf.String())), &rec); err != nil {
		t.Fatalf("decode payload record: %v (line=%q)", err, payloadBuf.String())
	}
	if rec.Tool != "web__fetch" || rec.Upstream != "web" || rec.Method != mcp.MethodToolsCall {
		t.Errorf("payload metadata mismatch: %+v", rec)
	}
	if string(rec.Arguments) != string(args) {
		t.Errorf("payload arguments = %s, want raw %s", rec.Arguments, args)
	}
	if len(rec.Result) == 0 {
		t.Errorf("payload result missing, want the upstream result")
	}

	// Regression guard (SKILL §6): the AUDIT log still contains no arguments —
	// enabling the payload log must not leak secrets into calls.jsonl.
	if strings.Contains(auditBuf.String(), secret) {
		t.Fatalf("audit log leaked secret:\n%s", auditBuf.String())
	}
}

// TestRegistryDefaultPayloadLogOff is the core security regression guard: with
// payload logging DISABLED (the default no-op), the audit log carries no call
// arguments — the Stage 10 invariant that a disabled payload log leaves the
// existing metadata-only guarantee intact (SKILL §6).
func TestRegistryDefaultPayloadLogOff(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	var auditBuf bytes.Buffer
	callLog := logging.NewCallLogWriter(&auditBuf)

	// newTestRegistry uses the no-op payload log (the disabled default).
	r := newTestRegistry(t, cfg, callLog, map[string]*fakeUpstream{
		"web": {name: "web", tools: []string{"fetch"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	const secret = "SUPER_SECRET_TOKEN_xyz789"
	args := json.RawMessage(`{"authorization":"Bearer ` + secret + `"}`)
	if _, err := r.CallTool(context.Background(), "web__fetch", args); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	logged := auditBuf.String()
	if strings.Contains(logged, secret) {
		t.Fatalf("audit log leaked secret with payload logging off:\n%s", logged)
	}
	if strings.Contains(logged, "authorization") {
		t.Fatalf("audit log contains arguments with payload logging off:\n%s", logged)
	}
	// Sanity: metadata still recorded.
	if !strings.Contains(logged, `"tool":"web__fetch"`) {
		t.Fatalf("audit log missing metadata:\n%s", logged)
	}
}

// TestRegistryPayloadLogMarksErrorResponseNotOK is the Stage 10 regression guard
// for the omitempty trap: when an upstream returns a JSON-RPC error-object with
// an EMPTY Message (valid per spec — only the code is meaningful), the empty Err
// string is dropped from the record by `json:"error,omitempty"`. Without an
// explicit OK field such a record would be indistinguishable from a clean
// success. OK is written without omitempty precisely so false survives.
func TestRegistryPayloadLogMarksErrorResponseNotOK(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	var auditBuf, payloadBuf bytes.Buffer
	callLog := logging.NewCallLogWriter(&auditBuf)
	payloadLog := logging.NewPayloadLogWriter(&payloadBuf)

	// Upstream returns a JSON-RPC error response with an EMPTY message string.
	errResp := mcp.NewError(mcp.IntID(1), -32000, "", nil)
	r := newTestRegistryWithPayload(t, cfg, callLog, payloadLog, map[string]*fakeUpstream{
		"web": {name: "web", tools: []string{"fetch"}, callResp: errResp},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if _, err := r.CallTool(context.Background(), "web__fetch", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	line := strings.TrimSpace(payloadBuf.String())
	var rec logging.PayloadRecord
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("decode payload record: %v (line=%q)", err, line)
	}
	// The whole point: even though Err is empty (Message was ""), OK must be
	// false so the record is not mistaken for a successful call.
	if rec.OK {
		t.Errorf("PayloadRecord.OK = true for an error response; want false (rec=%+v)", rec)
	}
	if rec.Err != "" {
		t.Errorf("precondition: expected empty Err (empty upstream Message), got %q", rec.Err)
	}
	// And the raw JSON line must actually carry "ok":false (omitempty would have
	// dropped it) so a log reader can tell success from failure.
	if !strings.Contains(line, `"ok":false`) {
		t.Errorf("payload line missing \"ok\":false marker:\n%s", line)
	}
}

// Two upstreams each mint id=1 from their private counters; the registry must
// still route every response correctly (id spaces are per-upstream, not global).
func TestRegistrySeparatesIDSpaces(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "a", Enabled: true},
		{Name: "b", Enabled: true},
	}}
	fa := &fakeUpstream{name: "a", tools: []string{"t"}}
	fb := &fakeUpstream{name: "b", tools: []string{"t"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"a": fa, "b": fb})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	ra, err := r.CallTool(context.Background(), "a__t", nil)
	if err != nil {
		t.Fatalf("call a: %v", err)
	}
	rb, err := r.CallTool(context.Background(), "b__t", nil)
	if err != nil {
		t.Fatalf("call b: %v", err)
	}
	// Both upstreams handed back id "1" from their own counters; responses are
	// still correctly attributed to the right call.
	if string(ra.ID) != "1" || string(rb.ID) != "1" {
		t.Fatalf("expected each upstream to use its own id space (got a=%s b=%s)", ra.ID, rb.ID)
	}
	if !strings.Contains(string(ra.Result), "ok t") || !strings.Contains(string(rb.Result), "ok t") {
		t.Fatalf("unexpected results a=%s b=%s", ra.Result, rb.Result)
	}
}
