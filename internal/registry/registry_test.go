package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/upstream"
)

// fakeUpstreamBase is the embeddable default implementation of EVERY Upstream
// method: zero values, nil errors, no behaviour. Concrete fakes embed it and
// override only the methods their test actually exercises — so extending the
// Upstream interface in a future round means adding ONE stub here instead of
// fixing every fake in the package. Initialize returns a non-nil empty result
// (not nil) because launch() dereferences it.
type fakeUpstreamBase struct{}

func (fakeUpstreamBase) Initialize(context.Context) (*mcp.InitializeResult, error) {
	return &mcp.InitializeResult{}, nil
}
func (fakeUpstreamBase) ListTools(context.Context) ([]mcp.Tool, error)         { return nil, nil }
func (fakeUpstreamBase) ListResources(context.Context) ([]mcp.Resource, error) { return nil, nil }
func (fakeUpstreamBase) ListPrompts(context.Context) ([]mcp.Prompt, error)     { return nil, nil }
func (fakeUpstreamBase) CallTool(context.Context, string, json.RawMessage, json.RawMessage) (*mcp.Message, error) {
	return nil, nil
}
func (fakeUpstreamBase) GetPrompt(context.Context, string, json.RawMessage) (*mcp.Message, error) {
	return nil, nil
}
func (fakeUpstreamBase) Close() error                  { return nil }
func (fakeUpstreamBase) Done() (<-chan struct{}, bool) { return nil, false }

// fakeUpstream is an in-process Upstream used to test the multiplexer without
// spawning processes. Each fake mints its own call-side ids from a private
// counter, so the test asserts the registry keeps id spaces separated.
type fakeUpstream struct {
	fakeUpstreamBase
	name         string
	tools        []string
	instructions string          // reported in InitializeResult.Instructions
	caps         json.RawMessage // reported in InitializeResult.Capabilities
	initErr      error
	listErr      error
	callErr      error
	callResp     *mcp.Message // if set, CallTool returns this response verbatim
	nextID       atomic.Int64
	lastArgs     json.RawMessage
	lastMeta     json.RawMessage
	lastNamed    string

	// Prompts (Round 4). The registry only calls ListPrompts when caps above
	// declares "prompts" — a fake advertising prompts must therefore also set
	// caps accordingly.
	prompts        []string // advertised in prompts/list
	promptsErr     error    // if set, ListPrompts fails
	lastPromptName string
	lastPromptArgs json.RawMessage
}

func (f *fakeUpstream) Name() string { return f.name }

func (f *fakeUpstream) Initialize(context.Context) (*mcp.InitializeResult, error) {
	if f.initErr != nil {
		return nil, f.initErr
	}
	return &mcp.InitializeResult{
		ProtocolVersion: mcp.ProtocolVersion,
		ServerInfo:      mcp.Implementation{Name: f.name},
		Instructions:    f.instructions,
		Capabilities:    f.caps,
	}, nil
}

func (f *fakeUpstream) ListTools(context.Context) ([]mcp.Tool, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]mcp.Tool, 0, len(f.tools))
	for _, t := range f.tools {
		out = append(out, mcp.Tool{Name: t, Description: json.RawMessage(`"desc ` + t + `"`), InputSchema: json.RawMessage(`{"type":"object"}`)})
	}
	return out, nil
}

func (f *fakeUpstream) ListPrompts(context.Context) ([]mcp.Prompt, error) {
	if f.promptsErr != nil {
		return nil, f.promptsErr
	}
	out := make([]mcp.Prompt, 0, len(f.prompts))
	for _, p := range f.prompts {
		out = append(out, mcp.Prompt{Name: p, Description: json.RawMessage(`"prompt ` + p + `"`)})
	}
	return out, nil
}

func (f *fakeUpstream) GetPrompt(_ context.Context, name string, arguments json.RawMessage) (*mcp.Message, error) {
	f.lastPromptName = name
	f.lastPromptArgs = arguments
	id := mcp.IntID(f.nextID.Add(1))
	result := json.RawMessage(`{"messages":[{"role":"user","content":{"type":"text","text":"prompt ` + name + `"}}]}`)
	return mcp.NewResult(id, result), nil
}

func (f *fakeUpstream) CallTool(_ context.Context, name string, arguments, meta json.RawMessage) (*mcp.Message, error) {
	f.lastNamed = name
	f.lastArgs = arguments
	f.lastMeta = meta
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
	r := New(cfg, quietLogger(), callLog, payloadLog, true, "0.0.0-test")
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
	resp, err := r.CallTool(context.Background(), "web__search", args, nil)
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
	if _, err := r.CallTool(context.Background(), "nope__nope", nil, nil); err == nil {
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

	_, err := r.CallTool(context.Background(), "secretname__search", nil, nil)
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

	_, err := r.CallTool(context.Background(), "secretname__search", nil, nil)
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
	if _, err := r.CallTool(context.Background(), "web__fetch", args, nil); err != nil {
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
// when the opt-in payload log is enabled, CallTool writes the arguments AND the
// upstream result to it (the deliberate difference from the audit log) — with
// secret-looking top-level fields masked to "***" (SKILL §6) while every other
// field passes through verbatim.
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

	const secret = "secret123"
	args := json.RawMessage(`{"api_key":"` + secret + `","query":"hello"}`)
	if _, err := r.CallTool(context.Background(), "web__fetch", args, nil); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	// The payload log carries the arguments and a result (opt-in debug)...
	var rec logging.PayloadRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(payloadBuf.String())), &rec); err != nil {
		t.Fatalf("decode payload record: %v (line=%q)", err, payloadBuf.String())
	}
	if rec.Tool != "web__fetch" || rec.Upstream != "web" || rec.Method != mcp.MethodToolsCall {
		t.Errorf("payload metadata mismatch: %+v", rec)
	}
	if len(rec.Result) == 0 {
		t.Errorf("payload result missing, want the upstream result")
	}

	// ...but the secret-looking argument is masked, the innocent one kept.
	var gotArgs map[string]string
	if err := json.Unmarshal(rec.Arguments, &gotArgs); err != nil {
		t.Fatalf("decode payload arguments %s: %v", rec.Arguments, err)
	}
	if gotArgs["api_key"] != "***" {
		t.Errorf("api_key = %q in payload log, want masked \"***\"", gotArgs["api_key"])
	}
	if gotArgs["query"] != "hello" {
		t.Errorf("query = %q in payload log, want \"hello\" untouched", gotArgs["query"])
	}
	if strings.Contains(payloadBuf.String(), secret) {
		t.Fatalf("payload log leaked secret:\n%s", payloadBuf.String())
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
	if _, err := r.CallTool(context.Background(), "web__fetch", args, nil); err != nil {
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

	if _, err := r.CallTool(context.Background(), "web__fetch", json.RawMessage(`{}`), nil); err != nil {
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

// TestRegistryPayloadLogRecordsErrorData is the Tier 3 regression guard: when
// an upstream's JSON-RPC error carries the optional structured Data field, the
// payload log must capture it verbatim in ErrorData — the payload log exists
// for full debugging visibility, so dropping error.data would defeat its point.
func TestRegistryPayloadLogRecordsErrorData(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	var payloadBuf bytes.Buffer
	payloadLog := logging.NewPayloadLogWriter(&payloadBuf)

	// Upstream returns a JSON-RPC error with a structured data payload.
	errData := json.RawMessage(`{"reason":"quota","limit":42}`)
	errResp := mcp.NewError(mcp.IntID(1), -32000, "boom", errData)
	r := newTestRegistryWithPayload(t, cfg, nil, payloadLog, map[string]*fakeUpstream{
		"web": {name: "web", tools: []string{"fetch"}, callResp: errResp},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if _, err := r.CallTool(context.Background(), "web__fetch", json.RawMessage(`{}`), nil); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	var rec logging.PayloadRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(payloadBuf.String())), &rec); err != nil {
		t.Fatalf("decode payload record: %v (line=%q)", err, payloadBuf.String())
	}
	if rec.Err != "boom" {
		t.Errorf("PayloadRecord.Err = %q, want %q", rec.Err, "boom")
	}
	if string(rec.ErrorData) != string(errData) {
		t.Errorf("PayloadRecord.ErrorData = %s, want verbatim %s", rec.ErrorData, errData)
	}
}

// latchInitUpstream blocks Initialize until gate is closed, so a test can force
// this upstream's launch to finish strictly AFTER another's.
type latchInitUpstream struct {
	*fakeUpstream
	gate <-chan struct{}
}

func (l *latchInitUpstream) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	select {
	case <-l.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return l.fakeUpstream.Initialize(ctx)
}

// signalListedUpstream closes listed once its FIRST ListTools returns —
// the last handshake step of launch(), i.e. "this upstream's launch is done".
type signalListedUpstream struct {
	*fakeUpstream
	once   sync.Once
	listed chan struct{}
}

func (s *signalListedUpstream) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	defer s.once.Do(func() { close(s.listed) })
	return s.fakeUpstream.ListTools(ctx)
}

// TestStartParallelMergeOrderDeterministic pins the launch/merge split inside
// Start, the same property TestReloadParallelMergeOrderDeterministic pins for
// Reload: launches fan out in parallel, but the catalog merge runs sequentially
// after g.Wait(), in CONFIG order. Two upstreams collide on the client-facing
// name "first__t" — "first" advertises tool "t" under the default namespaced
// name, "second" renames its tool "z" onto that same name. "first" is listed
// FIRST in the config but is forced (via a latch, no timing luck involved) to
// finish its handshake strictly AFTER "second": with the old merge-inside-
// goroutine bring-up the winner would be "second" (completion order); with the
// deterministic post-Wait merge it must be "first" (config order), on every
// run. Run under -race.
func TestStartParallelMergeOrderDeterministic(t *testing.T) {
	secondListed := make(chan struct{})
	first := &latchInitUpstream{
		fakeUpstream: &fakeUpstream{name: "first", tools: []string{"t"}},
		gate:         secondListed,
	}
	second := &signalListedUpstream{
		fakeUpstream: &fakeUpstream{name: "second", tools: []string{"z"}},
		listed:       secondListed,
	}
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "first", Enabled: true},
		{Name: "second", Enabled: true, Tools: config.ToolFilter{Rename: map[string]string{"z": "first__t"}}},
	}}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) {
		switch u.Name {
		case "first":
			return first, nil
		case "second":
			return second, nil
		}
		return nil, errors.New("no fake for " + u.Name)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	var owner string
	for _, d := range r.Tools() {
		if d.Name == "first__t" {
			owner = d.Upstream
		}
	}
	if owner != "first" {
		t.Fatalf("collided name first__t owned by %q, want %q (Start's merge must follow config order, not goroutine completion order)", owner, "first")
	}
}

// swapConnOnCallUpstream returns err from every CallTool; on the FIRST call it
// also installs repl under its own name in the registry's connection map —
// modelling a concurrent reload/auto-restart that replaced the connection in
// the window between CallTool's route lookup and the actual RPC.
type swapConnOnCallUpstream struct {
	*fakeUpstream
	reg   *Registry
	repl  Upstream // nil: no swap, the dead conn stays current
	err   error
	once  sync.Once
	calls atomic.Int64
}

func (s *swapConnOnCallUpstream) CallTool(context.Context, string, json.RawMessage, json.RawMessage) (*mcp.Message, error) {
	s.calls.Add(1)
	s.once.Do(func() {
		if s.repl == nil {
			return
		}
		s.reg.mu.Lock()
		s.reg.conns[s.fakeUpstream.name] = s.repl
		s.reg.mu.Unlock()
	})
	return nil, s.err
}

// TestCallToolRetriesOnceOnConnClosedBeforeSend covers finding #3's happy path:
// CallTool resolves a connection, but by the time it calls, that connection was
// closed by a concurrent reload/restart — which has ALREADY installed a fresh
// one under the same name. ErrConnClosedBeforeSend guarantees the request never
// reached the upstream, so CallTool must re-resolve and retry ONCE against the
// fresh connection — and both attempts must be audited.
func TestCallToolRetriesOnceOnConnClosedBeforeSend(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	var buf bytes.Buffer
	callLog := logging.NewCallLogWriter(&buf)

	live := &fakeUpstream{name: "web", tools: []string{"fetch"}}
	dead := &swapConnOnCallUpstream{
		fakeUpstream: &fakeUpstream{name: "web", tools: []string{"fetch"}},
		repl:         live,
		err:          upstream.ErrConnClosedBeforeSend,
	}
	r := New(cfg, quietLogger(), callLog, noopPayloadLog(), true, "0.0.0-test")
	dead.reg = r
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) { return dead, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	resp, err := r.CallTool(context.Background(), "web__fetch", json.RawMessage(`{"q":1}`), nil)
	if err != nil {
		t.Fatalf("CallTool should have retried on the fresh connection, got error: %v", err)
	}
	if resp == nil || resp.Error != nil {
		t.Fatalf("unexpected response after retry: %+v", resp)
	}
	if live.lastNamed != "fetch" {
		t.Errorf("fresh connection received name %q, want original %q", live.lastNamed, "fetch")
	}
	if got := dead.calls.Load(); got != 1 {
		t.Errorf("dead connection called %d times, want exactly 1", got)
	}

	// Every attempt is audited separately — the failed first try AND the
	// successful retry; nothing is hidden from the call journal.
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("audit log has %d records, want 2 (failed attempt + retried success):\n%s", len(lines), buf.String())
	}
	if !strings.Contains(lines[0], `"ok":false`) {
		t.Errorf("first audit record should mark the failed attempt: %s", lines[0])
	}
	if !strings.Contains(lines[1], `"ok":true`) {
		t.Errorf("second audit record should mark the retried success: %s", lines[1])
	}
}

// TestCallToolNoRetryOnLateConnClosed is the control for finding #3: a plain
// late ErrConnClosed (NOT the BeforeSend variant) means the request may have
// reached the upstream before the connection died — retrying could execute a
// non-idempotent tool twice, so CallTool must return the error immediately even
// though a fresh connection is already available under the same name.
func TestCallToolNoRetryOnLateConnClosed(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	live := &fakeUpstream{name: "web", tools: []string{"fetch"}}
	dead := &swapConnOnCallUpstream{
		fakeUpstream: &fakeUpstream{name: "web", tools: []string{"fetch"}},
		repl:         live,
		err:          upstream.ErrConnClosed,
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	dead.reg = r
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) { return dead, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if _, err := r.CallTool(context.Background(), "web__fetch", nil, nil); err == nil {
		t.Fatal("expected error for a late ErrConnClosed, got success — a post-send failure must never be retried")
	}
	if live.lastNamed != "" {
		t.Errorf("fresh connection was called (name=%q) — a post-send failure must never be retried", live.lastNamed)
	}
}

// TestCallToolNoRetryOnSameConn: the BeforeSend error is retryable in
// principle, but if re-resolving yields the SAME (dead) connection there is
// nothing sensible to retry against — CallTool must return the error after
// exactly one attempt instead of hammering the corpse.
func TestCallToolNoRetryOnSameConn(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	dead := &swapConnOnCallUpstream{
		fakeUpstream: &fakeUpstream{name: "web", tools: []string{"fetch"}},
		repl:         nil, // no swap: the dead conn stays current
		err:          upstream.ErrConnClosedBeforeSend,
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	dead.reg = r
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) { return dead, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if _, err := r.CallTool(context.Background(), "web__fetch", nil, nil); err == nil {
		t.Fatal("expected error when the re-resolved connection is the same dead one")
	}
	if got := dead.calls.Load(); got != 1 {
		t.Errorf("dead connection called %d times, want exactly 1 (no retry against the same conn)", got)
	}
}

// versionedListUpstream serves a DIFFERENT catalog on each ListTools call
// (v1 for Start's initial list, v2 for the first re-list, v3 afterwards) and
// lets the test hold the first re-list "in flight": call 2 signals entered and
// then blocks until release is closed. This stages finding #13's overlapping
// re-lists deterministically — no timing luck involved.
type versionedListUpstream struct {
	*fakeUpstream
	mu      sync.Mutex
	calls   int
	entered chan struct{} // receives one value when the gated (2nd) ListTools starts
	release chan struct{} // the gated ListTools proceeds once this is closed
}

func (g *versionedListUpstream) ListTools(context.Context) ([]mcp.Tool, error) {
	g.mu.Lock()
	g.calls++
	n := g.calls
	g.mu.Unlock()
	if n == 2 { // the first RE-list (call 1 is Start's initial list): hold it in flight
		g.entered <- struct{}{}
		<-g.release
	}
	var name string
	switch n {
	case 1:
		name = "a" // v1: Start's initial catalog
	case 2:
		name = "b" // v2: the first re-list — already stale by the time it returns
	default:
		name = "c" // v3: the dirty-driven second re-list — the freshest truth
	}
	return []mcp.Tool{{Name: name, Description: json.RawMessage(`"desc ` + name + `"`), InputSchema: json.RawMessage(`{"type":"object"}`)}}, nil
}

func (g *versionedListUpstream) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// TestRelistOverlappingNotificationsSerialized pins finding #13: two
// tools/list_changed notifications whose debounce expiries overlap must NOT run
// two concurrent re-lists (a stale result could then land after a fresher one).
// Instead the second expiry marks the running re-list dirty, and when the first
// re-list finishes (returning the already-stale v2), a SECOND re-list runs
// immediately — no new debounce wait — and its result (v3) is final. Exactly
// two re-list RPCs total: the dirty re-run happened, and nothing beyond it.
// Run under -race.
func TestRelistOverlappingNotificationsSerialized(t *testing.T) {
	up := &versionedListUpstream{
		fakeUpstream: &fakeUpstream{name: "dyn"},
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{{Name: "dyn", Enabled: true}},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) { return up, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	if !hasTool(r, "dyn__a") {
		t.Fatal("precondition: dyn__a not in catalog after Start")
	}

	// Notification #1: its debounce timer fires and the re-list's ListTools
	// (call 2) blocks on the latch, in flight.
	r.onUpstreamNotification("dyn", mcp.NotifToolsListChanged, nil)
	select {
	case <-up.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first re-list never started after notification #1")
	}

	// Notification #2 lands while the first re-list is still in flight: its
	// expiry must only mark the upstream dirty, not start a parallel re-list.
	r.onUpstreamNotification("dyn", mcp.NotifToolsListChanged, nil)
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.relistMu.Lock()
		st := r.relistStates["dyn"]
		dirty := st != nil && st.dirty
		r.relistMu.Unlock()
		if dirty {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second notification's expiry never marked the running re-list dirty")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Release the first re-list: it returns the already-stale v2; the dirty
	// flag must immediately drive a second re-list, which returns v3 — and v3
	// must be what the catalog converges to.
	close(up.release)
	waitForTool(t, r, "dyn__c", 5*time.Second)

	// Let any (wrong) extra re-list surface before counting.
	time.Sleep(300 * time.Millisecond)
	if got := up.callCount(); got != 3 {
		t.Errorf("ListTools called %d times total, want exactly 3 (Start's initial + 2 serialized re-lists)", got)
	}
	for _, ns := range []string{"dyn__a", "dyn__b"} {
		if hasTool(r, ns) {
			t.Errorf("stale catalog entry %q survived; final catalog must be v3 only: %+v", ns, r.Tools())
		}
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

	ra, err := r.CallTool(context.Background(), "a__t", nil, nil)
	if err != nil {
		t.Fatalf("call a: %v", err)
	}
	rb, err := r.CallTool(context.Background(), "b__t", nil, nil)
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

// TestCallToolForwardsMetaVerbatim: the client's `_meta` object (progressToken
// etc.) must travel the whole CallTool chain to the upstream byte for byte —
// and, symmetrically, a call WITHOUT `_meta` must deliver none (nil), so the
// gateway never invents one (transparent-proxy contract, MCP_NOTES §1).
func TestCallToolForwardsMetaVerbatim(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	web := &fakeUpstream{name: "web", tools: []string{"fetch"}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{"web": web})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	meta := json.RawMessage(`{"progressToken":"tok-1","vendor":{"x":[1,2]}}`)
	if _, err := r.CallTool(context.Background(), "web__fetch", json.RawMessage(`{"q":1}`), meta); err != nil {
		t.Fatalf("CallTool with meta: %v", err)
	}
	if string(web.lastMeta) != string(meta) {
		t.Errorf("_meta not forwarded verbatim: got %s want %s", web.lastMeta, meta)
	}

	if _, err := r.CallTool(context.Background(), "web__fetch", json.RawMessage(`{"q":2}`), nil); err != nil {
		t.Fatalf("CallTool without meta: %v", err)
	}
	if web.lastMeta != nil {
		t.Errorf("call without _meta delivered %s to the upstream, want none", web.lastMeta)
	}
}

// TestInstructionsAggregatesSortedNonEmpty: Instructions() concatenates the
// non-empty per-upstream instructions as "## <name>" sections, sorted by
// upstream name for determinism; upstreams without instructions are skipped
// entirely. A filter-only re-projection (installLocked with nil meta) must not
// lose the recorded instructions.
func TestInstructionsAggregatesSortedNonEmpty(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "zeta", Enabled: true},
		{Name: "alpha", Enabled: true},
		{Name: "mid", Enabled: true},
	}}
	fakes := map[string]*fakeUpstream{
		"zeta":  {name: "zeta", tools: []string{"z"}, instructions: "Use zeta wisely."},
		"alpha": {name: "alpha", tools: []string{"a"}, instructions: "Alpha rules."},
		"mid":   {name: "mid", tools: []string{"m"}}, // no instructions — skipped
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	want := "## alpha\nAlpha rules.\n\n## zeta\nUse zeta wisely."
	if got := r.Instructions(); got != want {
		t.Errorf("Instructions() = %q, want %q", got, want)
	}

	// A re-projection without a new handshake must preserve the metadata.
	r.remergeUpstream("alpha")
	if got := r.Instructions(); got != want {
		t.Errorf("Instructions() after remerge = %q, want %q (handshake meta lost)", got, want)
	}

	// Dropping an upstream removes its section.
	r.dropUpstream("alpha")
	if got, want := r.Instructions(), "## zeta\nUse zeta wisely."; got != want {
		t.Errorf("Instructions() after drop = %q, want %q", got, want)
	}
}

// TestInstructionsEmptyWhenNoUpstreamProvidesAny: no instructions anywhere
// yields the empty string (so InitializeResult.Instructions is omitted).
func TestInstructionsEmptyWhenNoUpstreamProvidesAny(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	r := newTestRegistry(t, cfg, nil, map[string]*fakeUpstream{
		"web": {name: "web", tools: []string{"fetch"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	if got := r.Instructions(); got != "" {
		t.Errorf("Instructions() = %q, want empty", got)
	}
}

// TestHasUpstreamCapability: the shallow capability probe — a key present in
// at least one live upstream's capabilities object (and not JSON null) counts;
// absent keys, null values and malformed/missing capabilities do not.
func TestHasUpstreamCapability(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "rich", Enabled: true},
		{Name: "bare", Enabled: true},
	}}
	fakes := map[string]*fakeUpstream{
		"rich": {name: "rich", tools: []string{"r"},
			caps: json.RawMessage(`{"tools":{},"prompts":{"listChanged":true},"odd":null}`)},
		"bare": {name: "bare", tools: []string{"b"}}, // nil capabilities
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	for cap, want := range map[string]bool{
		"tools":     true,
		"prompts":   true,
		"resources": false, // nobody declares it
		"odd":       false, // declared but null
	} {
		if got := r.HasUpstreamCapability(cap); got != want {
			t.Errorf("HasUpstreamCapability(%q) = %v, want %v", cap, got, want)
		}
	}

	r.dropUpstream("rich")
	if r.HasUpstreamCapability("prompts") {
		t.Error("HasUpstreamCapability(prompts) still true after the declaring upstream was dropped")
	}
}

// TestAuditRecordsClientFromContext: a CallTool made under a context carrying
// the client identity (registry.WithClient — what the transports attach after
// initialize) is audited with CallRecord.Client; a context without one yields
// a record with the client field omitted entirely.
func TestAuditRecordsClientFromContext(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "web", Enabled: true}}}
	var buf bytes.Buffer
	r := newTestRegistry(t, cfg, logging.NewCallLogWriter(&buf), map[string]*fakeUpstream{
		"web": {name: "web", tools: []string{"fetch"}},
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	ctx := WithClient(context.Background(), "test-client/9.9.9")
	if _, err := r.CallTool(ctx, "web__fetch", nil, nil); err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !strings.Contains(buf.String(), `"client":"test-client/9.9.9"`) {
		t.Errorf("audit record missing client identity:\n%s", buf.String())
	}

	buf.Reset()
	if _, err := r.CallTool(context.Background(), "web__fetch", nil, nil); err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if strings.Contains(buf.String(), `"client"`) {
		t.Errorf("audit record carries a client field without WithClient:\n%s", buf.String())
	}

	// WithClient("") is a documented no-op — the context comes back unchanged.
	if got := ClientFromContext(WithClient(context.Background(), "")); got != "" {
		t.Errorf("ClientFromContext after WithClient(\"\") = %q, want empty", got)
	}
}
