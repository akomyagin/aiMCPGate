package registry

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

func intPtr(v int) *int { return &v }

// startedRegistry builds and Starts a registry over the given fakes, failing
// the test on any bring-up error and closing it on cleanup.
func startedRegistry(t *testing.T, cfg *config.Config, fakes map[string]*fakeUpstream) *Registry {
	t.Helper()
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// TestRateLimitDelaysSecondCall picks the "delayed" outcome of the two
// realistic rate-limit behaviours: with burst=1 and rps=5 (one token per
// 200ms), the first call passes immediately and the second must wait for the
// refill — the pair cannot complete faster than the refill interval.
func TestRateLimitDelaysSecondCall(t *testing.T) {
	cfg := &config.Config{
		RateLimit: &config.RateLimit{RPS: 5, Burst: 1},
		Upstreams: []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"t"}}
	r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

	start := time.Now()
	for i := 0; i < 2; i++ {
		if _, err := r.CallTool(context.Background(), "a__t", nil, nil); err != nil {
			t.Fatalf("CallTool #%d: %v", i+1, err)
		}
	}
	elapsed := time.Since(start)
	// The limiter is time-based: the second token simply does not exist before
	// ~200ms, so a lower bound (with scheduler slack) cannot flake.
	if elapsed < 150*time.Millisecond {
		t.Errorf("two calls under rps=5/burst=1 finished in %v; the second was not rate-limited", elapsed)
	}
}

// TestRateLimitRespectsContextDeadline picks the other realistic outcome: the
// refill is far beyond the caller's deadline, so limiter.Wait must give up
// with an error — and the request must never reach the upstream.
func TestRateLimitRespectsContextDeadline(t *testing.T) {
	cfg := &config.Config{
		RateLimit: &config.RateLimit{RPS: 0.001, Burst: 1}, // next token in ~17 minutes
		Upstreams: []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"t"}}
	r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

	// First call eats the single burst token.
	if _, err := r.CallTool(context.Background(), "a__t", nil, nil); err != nil {
		t.Fatalf("CallTool #1: %v", err)
	}
	fake.lastNamed = ""

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := r.CallTool(ctx, "a__t", nil, nil); err == nil {
		t.Fatal("CallTool #2: want a rate-limit error under a short deadline, got nil")
	}
	if fake.lastNamed != "" {
		t.Errorf("rate-limited call still reached the upstream (called %q)", fake.lastNamed)
	}
}

// concurrencyFake records the peak number of simultaneous CallTool invocations
// while holding each call open briefly, so a missing semaphore is caught.
type concurrencyFake struct {
	fakeUpstreamBase
	mu   sync.Mutex
	cur  int
	peak int
}

func (f *concurrencyFake) ListTools(context.Context) ([]mcp.Tool, error) {
	return []mcp.Tool{{Name: "slow", InputSchema: json.RawMessage(`{"type":"object"}`)}}, nil
}

func (f *concurrencyFake) CallTool(context.Context, string, json.RawMessage, json.RawMessage) (*mcp.Message, error) {
	f.mu.Lock()
	f.cur++
	if f.cur > f.peak {
		f.peak = f.cur
	}
	f.mu.Unlock()

	time.Sleep(20 * time.Millisecond) // hold the call open so overlap is observable

	f.mu.Lock()
	f.cur--
	f.mu.Unlock()
	return mcp.NewResult(mcp.IntID(1), json.RawMessage(`{"content":[]}`)), nil
}

func (f *concurrencyFake) peakConcurrency() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak
}

func TestMaxConcurrentCapsParallelCalls(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "a", Enabled: boolPtr(true), MaxConcurrent: 1},
	}}
	fake := &concurrencyFake{}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	r.start = func(context.Context, config.Upstream) (Upstream, error) { return fake, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.CallTool(context.Background(), "a__slow", nil, nil); err != nil {
				t.Errorf("CallTool: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := fake.peakConcurrency(); got != 1 {
		t.Errorf("peak concurrency = %d, want 1 with max_concurrent: 1", got)
	}
}

// A1. TestCallToolRateLimitRefusalCarriesSentinel: a rate-limit refusal reaches
// CallTool's caller as an error wrapping registry.ErrRateLimited, its text names
// only the client-supplied namespaced tool (sanitization) and leaks neither the
// underlying x/time/rate string nor the bare upstream name, and the call never
// reached the upstream.
func TestCallToolRateLimitRefusalCarriesSentinel(t *testing.T) {
	cfg := &config.Config{
		RateLimit: &config.RateLimit{RPS: 0.001, Burst: 1}, // next token in ~17 minutes
		Upstreams: []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"t"}}
	r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

	if _, err := r.CallTool(context.Background(), "a__t", nil, nil); err != nil {
		t.Fatalf("CallTool #1: %v", err)
	}
	fake.lastNamed = ""

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := r.CallTool(ctx, "a__t", nil, nil)
	if err == nil {
		t.Fatal("CallTool #2: want a rate-limit error under a short deadline, got nil")
	}
	if !errors.Is(err, ErrRateLimited) {
		t.Errorf("error %q does not wrap ErrRateLimited", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "a__t") {
		t.Errorf("error %q should name the client-supplied tool a__t", msg)
	}
	if !strings.Contains(msg, "gateway rate limit exceeded") {
		t.Errorf("error %q should carry the sanitized sentinel text", msg)
	}
	if strings.Contains(msg, "Wait(") {
		t.Errorf("error %q leaks the internal x/time/rate string", msg)
	}
	if fake.lastNamed != "" {
		t.Errorf("rate-limited call still reached the upstream (called %q)", fake.lastNamed)
	}
}

// A2. TestCallToolConcurrencyRefusalCarriesSentinel: a semaphore refusal caused
// by the caller's own deadline wraps BOTH ErrConcurrencyLimited and
// context.DeadlineExceeded (Acquire returns ctx.Err()). The guard verdict must
// win — this pins the check order in CallTool (§3.3): the text must carry the
// concurrency sentinel and must NOT be the generic "timed out".
func TestCallToolConcurrencyRefusalCarriesSentinel(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "a", Enabled: boolPtr(true), MaxConcurrent: 1},
	}}
	fake := &blockingFake{release: make(chan struct{}), entered: make(chan struct{})}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	r.start = func(context.Context, config.Upstream) (Upstream, error) { return fake, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	defer close(fake.release)

	// First call holds the single permit until we release it.
	held := make(chan struct{})
	go func() {
		close(held)
		_, _ = r.CallTool(context.Background(), "a__block", nil, nil)
	}()
	<-held
	fake.waitEntered(t) // ensure the first call actually acquired the permit

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := r.CallTool(ctx, "a__block", nil, nil)
	if err == nil {
		t.Fatal("CallTool #2: want a concurrency refusal under a short deadline, got nil")
	}
	if !errors.Is(err, ErrConcurrencyLimited) {
		t.Errorf("error %q does not wrap ErrConcurrencyLimited", err)
	}
	if strings.Contains(err.Error(), "timed out") {
		t.Errorf("error %q is generic 'timed out'; guard order (§3.3) was not honored", err)
	}
}

// blockingFake holds every CallTool open until release is closed, and signals
// entry so a test can be sure the permit was acquired before it races a second
// call against the semaphore.
type blockingFake struct {
	fakeUpstreamBase
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (f *blockingFake) ListTools(context.Context) ([]mcp.Tool, error) {
	return []mcp.Tool{{Name: "block", InputSchema: json.RawMessage(`{"type":"object"}`)}}, nil
}

func (f *blockingFake) CallTool(ctx context.Context, _ string, _, _ json.RawMessage) (*mcp.Message, error) {
	f.once.Do(func() {
		if f.entered != nil {
			close(f.entered)
		}
	})
	select {
	case <-f.release:
	case <-ctx.Done():
	}
	return mcp.NewResult(mcp.IntID(1), json.RawMessage(`{"content":[]}`)), nil
}

func (f *blockingFake) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-f.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("blockingFake: first call never entered CallTool")
	}
}

// A3. TestCallToolPlainFailureNotGuardMarked: an ordinary transport failure is
// neither guard sentinel, and its text is the generic sanitized failure — the
// negative that proves guard marking is not universal.
func TestCallToolPlainFailureNotGuardMarked(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "a", Enabled: boolPtr(true)}}}
	fake := &fakeUpstream{name: "a", tools: []string{"t"}, callErr: errors.New("boom: dialer broke")}
	r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

	_, err := r.CallTool(context.Background(), "a__t", nil, nil)
	if err == nil {
		t.Fatal("CallTool: want a transport error, got nil")
	}
	if errors.Is(err, ErrRateLimited) || errors.Is(err, ErrConcurrencyLimited) {
		t.Errorf("plain transport error %q must not be guard-marked", err)
	}
	if err.Error() != `call "a__t" failed` {
		t.Errorf("error = %q, want the generic sanitized failure", err)
	}
}

// A6. TestCallToolUpstreamErrorCodeNotRemapped pins the proxying invariant
// (mcp.message.go:24-26): an upstream that itself returns a JSON-RPC error —
// even one whose code happens to equal the gateway's own CodeGatewayBusy — is
// returned by CallTool with err==nil and resp.Error untouched. The gateway's
// -32029 signal is emitted ONLY on its own guard refusals (err!=nil branch);
// an identical code from an upstream is NOT a gateway signal and passes
// verbatim.
func TestCallToolUpstreamErrorCodeNotRemapped(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "a", Enabled: boolPtr(true)}}}
	upstreamErr := &mcp.Message{
		JSONRPC: mcp.Version,
		ID:      mcp.IntID(1),
		Error:   &mcp.Error{Code: mcp.CodeGatewayBusy, Message: "upstream says", Data: json.RawMessage(`{"upstream":"detail"}`)},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"t"}, callResp: upstreamErr}
	r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

	resp, err := r.CallTool(context.Background(), "a__t", nil, nil)
	if err != nil {
		t.Fatalf("CallTool: want err==nil (upstream error rides in resp), got %v", err)
	}
	if resp == nil || resp.Error == nil {
		t.Fatalf("want resp.Error preserved, got resp=%+v", resp)
	}
	if resp.Error.Code != mcp.CodeGatewayBusy {
		t.Errorf("upstream error code = %d, want it verbatim %d", resp.Error.Code, mcp.CodeGatewayBusy)
	}
	if resp.Error.Message != "upstream says" || string(resp.Error.Data) != `{"upstream":"detail"}` {
		t.Errorf("upstream error message/data remapped: %+v", resp.Error)
	}
}

// TestPerUpstreamCallTimeoutOverridesGlobal proves the override actually bounds
// the forwarded call: the global timeout is generous, the per-upstream one is
// tiny, and a hanging upstream must be cut off by the tiny one.
func TestPerUpstreamCallTimeoutOverridesGlobal(t *testing.T) {
	cfg := &config.Config{
		CallTimeout: 30 * time.Second,
		Upstreams: []config.Upstream{
			{Name: "a", Enabled: boolPtr(true), CallTimeout: 50 * time.Millisecond},
		},
	}
	fake := &hangingFake{}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	r.start = func(context.Context, config.Upstream) (Upstream, error) { return fake, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	start := time.Now()
	_, err := r.CallTool(context.Background(), "a__hang", nil, nil)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("CallTool: want timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("CallTool error = %q, want a timeout", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("call took %v; the 50ms per-upstream timeout did not apply", elapsed)
	}
}

// hangingFake blocks CallTool until the context is cancelled.
type hangingFake struct {
	fakeUpstreamBase
}

func (f *hangingFake) ListTools(context.Context) ([]mcp.Tool, error) {
	return []mcp.Tool{{Name: "hang", InputSchema: json.RawMessage(`{"type":"object"}`)}}, nil
}

func (f *hangingFake) CallTool(ctx context.Context, _ string, _, _ json.RawMessage) (*mcp.Message, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestLimiterRebuiltOnConfigChange pins the lazy reload behaviour: the cached
// limiter/semaphore survive as long as their config values do, are rebuilt on
// a change, and are dropped when the limit is removed.
func TestLimiterRebuiltOnConfigChange(t *testing.T) {
	mk := func(rps float64, burst, conc int) *config.Config {
		u := config.Upstream{Name: "a", Enabled: boolPtr(true), MaxConcurrent: conc}
		if rps > 0 {
			u.RateLimit = &config.RateLimit{RPS: rps, Burst: burst}
		}
		return &config.Config{Upstreams: []config.Upstream{u}}
	}
	r := New(mk(2, 2, 3), quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")

	l1 := r.limiterFor("a")
	if l1 == nil {
		t.Fatal("limiterFor: want a limiter, got nil")
	}
	if l2 := r.limiterFor("a"); l2 != l1 {
		t.Error("limiterFor: unchanged config must return the cached limiter")
	}
	s1 := r.semFor("a")
	if s1 == nil {
		t.Fatal("semFor: want a semaphore, got nil")
	}
	if s2 := r.semFor("a"); s2 != s1 {
		t.Error("semFor: unchanged config must return the cached semaphore")
	}

	// A "reload" changing the values: fresh instances.
	r.cfg.Store(mk(9, 1, 5))
	l3 := r.limiterFor("a")
	if l3 == nil || l3 == l1 {
		t.Error("limiterFor: changed rate_limit must rebuild the limiter")
	}
	if got := float64(l3.Limit()); got != 9 {
		t.Errorf("rebuilt limiter rate = %v, want 9", got)
	}
	if s3 := r.semFor("a"); s3 == nil || s3 == s1 {
		t.Error("semFor: changed max_concurrent must rebuild the semaphore")
	}

	// A "reload" removing the limits: nil again, cache dropped.
	r.cfg.Store(mk(0, 0, 0))
	if r.limiterFor("a") != nil {
		t.Error("limiterFor: removed rate_limit must return nil")
	}
	if r.semFor("a") != nil {
		t.Error("semFor: removed max_concurrent must return nil")
	}
}

// --- truncation ---

func TestTruncateToolResult(t *testing.T) {
	t.Run("single text block cut close to the limit", func(t *testing.T) {
		big := `{"content":[{"type":"text","text":"` + strings.Repeat("a", 2000) + `"}],"isError":false}`
		out, ok := truncateToolResult(json.RawMessage(big), 200)
		if !ok {
			t.Fatal("truncateToolResult: want ok=true")
		}
		if len(out) > 200 {
			t.Errorf("truncated size %d exceeds limit 200", len(out))
		}
		if len(out) < 120 {
			t.Errorf("truncated size %d is not reasonably close to limit 200", len(out))
		}
		if !strings.Contains(string(out), "[truncated by mcp-gate: result exceeded 200 bytes]") {
			t.Errorf("marker missing from %s", out)
		}
		// Unknown sibling fields survive the round-trip.
		var top map[string]json.RawMessage
		if err := json.Unmarshal(out, &top); err != nil {
			t.Fatalf("truncated result is not valid JSON: %v", err)
		}
		if string(top["isError"]) != "false" {
			t.Errorf("isError not preserved: %s", top["isError"])
		}
	})

	t.Run("later text blocks are dropped after the cut", func(t *testing.T) {
		big := `{"content":[` +
			`{"type":"text","text":"` + strings.Repeat("a", 500) + `"},` +
			`{"type":"text","text":"` + strings.Repeat("b", 500) + `"}]}`
		out, ok := truncateToolResult(json.RawMessage(big), 300)
		if !ok {
			t.Fatal("truncateToolResult: want ok=true")
		}
		if len(out) > 300 {
			t.Errorf("truncated size %d exceeds limit 300", len(out))
		}
		s := string(out)
		if !strings.Contains(s, "aaa") {
			t.Error("first block lost entirely; expected a prefix to survive")
		}
		if strings.Contains(s, "bbb") {
			t.Error("second block should have been emptied after the cut")
		}
		if !strings.Contains(s, "[truncated by mcp-gate: result exceeded 300 bytes]") {
			t.Errorf("marker missing from %s", out)
		}
	})

	t.Run("multibyte text is cut on rune boundaries", func(t *testing.T) {
		big := `{"content":[{"type":"text","text":"` + strings.Repeat("яж", 1000) + `"}]}`
		out, ok := truncateToolResult(json.RawMessage(big), 250)
		if !ok {
			t.Fatal("truncateToolResult: want ok=true")
		}
		var res struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(out, &res); err != nil {
			t.Fatalf("truncated result is not valid JSON: %v", err)
		}
		for _, r := range res.Content[0].Text {
			if r == '�' {
				t.Fatal("replacement character in truncated text: a multi-byte sequence was cut")
			}
		}
	})

	t.Run("tiny limit never overshoots", func(t *testing.T) {
		// Degenerate case found by review: with a limit far smaller than the
		// emptied-blocks skeleton (three 1000-byte text blocks, limit=50) the
		// old code still appended the truncation marker and returned 142
		// bytes against a declared limit of 50.
		big := `{"content":[` +
			`{"type":"text","text":"` + strings.Repeat("a", 1000) + `"},` +
			`{"type":"text","text":"` + strings.Repeat("b", 1000) + `"},` +
			`{"type":"text","text":"` + strings.Repeat("c", 1000) + `"}]}`
		out, ok := truncateToolResult(json.RawMessage(big), 50)
		if !ok {
			t.Fatal("truncateToolResult: want ok=true for limit 50")
		}
		if len(out) > 50 {
			t.Fatalf("truncated size %d exceeds limit 50: %s", len(out), out)
		}
		var top map[string]json.RawMessage
		if err := json.Unmarshal(out, &top); err != nil {
			t.Fatalf("truncated result is not valid JSON: %v", err)
		}
		if _, has := top["content"]; !has {
			t.Errorf("last-resort result lost the content field: %s", out)
		}

		// The last resort keeps isError once the limit admits it: dropping it
		// would turn a failed call into an apparent success.
		withErr := json.RawMessage(`{"content":[{"type":"text","text":"` +
			strings.Repeat("a", 1000) + `"}],"isError":true}`)
		out, ok = truncateToolResult(withErr, 60)
		if !ok {
			t.Fatal("truncateToolResult: want ok=true for limit 60")
		}
		if len(out) > 60 {
			t.Fatalf("truncated size %d exceeds limit 60: %s", len(out), out)
		}
		if !strings.Contains(string(out), `"isError":true`) {
			t.Errorf("isError dropped by the last-resort truncation: %s", out)
		}

		// Sweep every limit below the original size: whatever the function
		// reports truncatable must actually fit; a declared-untruncatable
		// result (ok=false, pass-through) is the only allowed way out.
		for _, payload := range []string{big, string(withErr)} {
			for limit := 1; limit < len(payload); limit++ {
				out, ok := truncateToolResult(json.RawMessage(payload), limit)
				if !ok {
					continue
				}
				if len(out) > limit {
					t.Fatalf("limit %d: truncated size %d overshoots: %s", limit, len(out), out)
				}
				if !json.Valid(out) {
					t.Fatalf("limit %d: truncated result is not valid JSON: %s", limit, out)
				}
			}
		}
	})

	t.Run("unexpected shapes pass through", func(t *testing.T) {
		for name, raw := range map[string]string{
			"not an object":     `"just a string"`,
			"no content":        `{"structured":123}`,
			"content not array": `{"content":{"type":"text"}}`,
			"no text blocks":    `{"content":[{"type":"image","data":"` + strings.Repeat("A", 500) + `"}]}`,
		} {
			if _, ok := truncateToolResult(json.RawMessage(raw), 10); ok {
				t.Errorf("%s: want ok=false (pass-through)", name)
			}
		}
	})
}

// TestTruncateResultLeavesErrorsAlone: only successful results are truncated.
func TestTruncateResultLeavesErrorsAlone(t *testing.T) {
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	resp := &mcp.Message{
		Error:  &mcp.Error{Code: -32000, Message: strings.Repeat("x", 500)},
		Result: json.RawMessage(strings.Repeat("y", 500)), // pathological, but must not be touched either
	}
	orig := string(resp.Result)
	r.truncateResult(resp, "a", "x", 10)
	if string(resp.Result) != orig {
		t.Error("truncateResult modified a response that carries an error")
	}
}

// TestCallToolTruncatesOversizedResult wires truncation end-to-end through
// CallTool, including the per-upstream override beating the global value.
func TestCallToolTruncatesOversizedResult(t *testing.T) {
	big := json.RawMessage(`{"content":[{"type":"text","text":"` + strings.Repeat("z", 5000) + `"}]}`)

	t.Run("global limit", func(t *testing.T) {
		cfg := &config.Config{
			MaxResultBytes: 300,
			Upstreams:      []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
		}
		fake := &fakeUpstream{name: "a", tools: []string{"t"}, callResp: mcp.NewResult(mcp.IntID(1), big)}
		r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

		resp, err := r.CallTool(context.Background(), "a__t", nil, nil)
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		if len(resp.Result) > 300 {
			t.Errorf("result size %d exceeds global max_result_bytes 300", len(resp.Result))
		}
		if !strings.Contains(string(resp.Result), "[truncated by mcp-gate") {
			t.Error("marker missing from truncated result")
		}
	})

	t.Run("per-upstream override beats global", func(t *testing.T) {
		cfg := &config.Config{
			MaxResultBytes: 100000, // global would not truncate at all
			Upstreams: []config.Upstream{
				{Name: "a", Enabled: boolPtr(true), MaxResultBytes: intPtr(300)},
			},
		}
		fake := &fakeUpstream{name: "a", tools: []string{"t"}, callResp: mcp.NewResult(mcp.IntID(1), big)}
		r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

		resp, err := r.CallTool(context.Background(), "a__t", nil, nil)
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		if len(resp.Result) > 300 {
			t.Errorf("result size %d exceeds per-upstream max_result_bytes 300", len(resp.Result))
		}
	})

	t.Run("per-upstream zero disables the global limit", func(t *testing.T) {
		cfg := &config.Config{
			MaxResultBytes: 300,
			Upstreams: []config.Upstream{
				{Name: "a", Enabled: boolPtr(true), MaxResultBytes: intPtr(0)},
			},
		}
		fake := &fakeUpstream{name: "a", tools: []string{"t"}, callResp: mcp.NewResult(mcp.IntID(1), big)}
		r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

		resp, err := r.CallTool(context.Background(), "a__t", nil, nil)
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		if len(resp.Result) != len(big) {
			t.Errorf("result size %d changed; explicit 0 must disable truncation (want %d)", len(resp.Result), len(big))
		}
	})
}

// --- Part B: result-over-limit _meta annotation ---

// metaField extracts result._meta[metaKeyResultOverLimit] as its typed value,
// failing the test if the key is absent or malformed.
func metaOverLimit(t *testing.T, result json.RawMessage) resultOverLimit {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal(result, &top); err != nil {
		t.Fatalf("result is not a JSON object: %v", err)
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(top["_meta"], &meta); err != nil {
		t.Fatalf("_meta is not an object: %s", top["_meta"])
	}
	raw, ok := meta[metaKeyResultOverLimit]
	if !ok {
		t.Fatalf("_meta lacks %q; keys: %v", metaKeyResultOverLimit, meta)
	}
	var v resultOverLimit
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode over-limit value %s: %v", raw, err)
	}
	return v
}

// B1. TestAnnotateResultOverLimitAddsMetaKey: a pure image result gets the meta
// key with correct byte counts, and content[] survives byte-for-byte.
func TestAnnotateResultOverLimitAddsMetaKey(t *testing.T) {
	src := json.RawMessage(`{"content":[{"type":"image","data":"AAAA"}]}`)
	out, ok := annotateResultOverLimit(src, 300)
	if !ok {
		t.Fatal("annotateResultOverLimit: want ok=true")
	}
	v := metaOverLimit(t, out)
	if v.LimitBytes != 300 || v.ResultBytes != len(src) {
		t.Errorf("over-limit value = %+v, want limitBytes=300 resultBytes=%d", v, len(src))
	}
	var srcTop, outTop map[string]json.RawMessage
	_ = json.Unmarshal(src, &srcTop)
	_ = json.Unmarshal(out, &outTop)
	if string(outTop["content"]) != string(srcTop["content"]) {
		t.Errorf("content changed:\n src %s\n out %s", srcTop["content"], outTop["content"])
	}
}

// B2. TestAnnotateResultOverLimitMergesExistingMeta: an existing _meta key is
// kept alongside ours.
func TestAnnotateResultOverLimitMergesExistingMeta(t *testing.T) {
	src := json.RawMessage(`{"content":[{"type":"image","data":"AAAA"}],"_meta":{"upstream.example/k":1}}`)
	out, ok := annotateResultOverLimit(src, 300)
	if !ok {
		t.Fatal("annotateResultOverLimit: want ok=true")
	}
	var top, meta map[string]json.RawMessage
	_ = json.Unmarshal(out, &top)
	if err := json.Unmarshal(top["_meta"], &meta); err != nil {
		t.Fatalf("_meta not an object: %v", err)
	}
	if string(meta["upstream.example/k"]) != "1" {
		t.Errorf("foreign _meta key not preserved: %s", top["_meta"])
	}
	if _, ok := meta[metaKeyResultOverLimit]; !ok {
		t.Errorf("our key missing after merge: %s", top["_meta"])
	}
}

// B3. TestAnnotateResultOverLimitRefusesUnparseableMeta: a non-object _meta is
// never clobbered — ok=false, caller passes original through.
func TestAnnotateResultOverLimitRefusesUnparseableMeta(t *testing.T) {
	src := json.RawMessage(`{"content":[{"type":"image","data":"AAAA"}],"_meta":5}`)
	if _, ok := annotateResultOverLimit(src, 300); ok {
		t.Error("annotateResultOverLimit: want ok=false for a non-object _meta")
	}
}

// B4. TestAnnotateResultOverLimitRefusesNonObjectResult: a non-object result is
// un-annotatable — ok=false.
func TestAnnotateResultOverLimitRefusesNonObjectResult(t *testing.T) {
	if _, ok := annotateResultOverLimit(json.RawMessage(`[1,2]`), 300); ok {
		t.Error("annotateResultOverLimit: want ok=false for a non-object result")
	}
}

// B5. TestCallToolAnnotatesOversizedNonTextResult wires Part B end-to-end
// through CallTool: an oversized image-only result reaches the client with the
// content[] intact and the over-limit _meta key, and exactly one truncation
// event is journaled whose Detail says the client was notified.
func TestCallToolAnnotatesOversizedNonTextResult(t *testing.T) {
	imageOnly := json.RawMessage(`{"content":[{"type":"image","data":"` + strings.Repeat("A", 2000) + `"}]}`)
	j, callLog := newJournal()
	cfg := &config.Config{
		MaxResultBytes: 300,
		Upstreams:      []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"shot"}, callResp: mcp.NewResult(mcp.IntID(1), imageOnly)}
	r := newTestRegistry(t, cfg, callLog, map[string]*fakeUpstream{"a": fake})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	resp, err := r.CallTool(context.Background(), "a__shot", nil, nil)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	v := metaOverLimit(t, resp.Result)
	if v.LimitBytes != 300 || v.ResultBytes != len(imageOnly) {
		t.Errorf("over-limit value = %+v, want limitBytes=300 resultBytes=%d", v, len(imageOnly))
	}
	// content[] byte-identical to the upstream's.
	var srcTop, outTop map[string]json.RawMessage
	_ = json.Unmarshal(imageOnly, &srcTop)
	_ = json.Unmarshal(resp.Result, &outTop)
	if string(outTop["content"]) != string(srcTop["content"]) {
		t.Errorf("content[] altered:\n src %s\n out %s", srcTop["content"], outTop["content"])
	}
	evs := j.events(t, logging.EventResultTruncationSkipped)
	if len(evs) != 1 {
		t.Fatalf("got %d truncation-skipped events, want 1; journal:\n%s", len(evs), j.bytes())
	}
	if !strings.Contains(evs[0].Detail, "client notified via result._meta") {
		t.Errorf("detail = %q, want it to say the client was notified", evs[0].Detail)
	}
}

// B5b. TestCallToolFallsBackWhenResultIsNotAnnotatable wires Part B's FALLBACK
// end-to-end through CallTool (test-report gap: B3/B4 pin
// annotateResultOverLimit's ok=false in isolation, but nothing exercised the
// real truncateResult branch falling back to a plain, un-annotated
// passthrough — the same shape B5 pins for the success case). An oversized
// image result with an unparseable _meta must reach the client BYTE-IDENTICAL
// to what the upstream sent — no over-limit key, no mangling — while the
// operator journal still records the skip (Detail must NOT claim the client
// was notified, since it was not).
func TestCallToolFallsBackWhenResultIsNotAnnotatable(t *testing.T) {
	unannotatable := json.RawMessage(`{"content":[{"type":"image","data":"` + strings.Repeat("A", 2000) + `"}],"_meta":5}`)
	j, callLog := newJournal()
	cfg := &config.Config{
		MaxResultBytes: 300,
		Upstreams:      []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"shot"}, callResp: mcp.NewResult(mcp.IntID(1), unannotatable)}
	r := newTestRegistry(t, cfg, callLog, map[string]*fakeUpstream{"a": fake})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	resp, err := r.CallTool(context.Background(), "a__shot", nil, nil)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if string(resp.Result) != string(unannotatable) {
		t.Errorf("un-annotatable result must pass through byte-identical:\n got  %s\n want %s", resp.Result, unannotatable)
	}
	evs := j.events(t, logging.EventResultTruncationSkipped)
	if len(evs) != 1 {
		t.Fatalf("got %d truncation-skipped events, want 1; journal:\n%s", len(evs), j.bytes())
	}
	if strings.Contains(evs[0].Detail, "client notified via result._meta") {
		t.Errorf("detail must NOT claim the client was notified when annotation failed: %q", evs[0].Detail)
	}
}

// B6. TestTruncatedTextResultHasNoOverLimitMeta wires the text path
// end-to-end through CallTool (review finding: calling truncateToolResult
// directly cannot fail from a regression in truncateResult's own branching —
// that function never sets the _meta key regardless, so the assertion proved
// nothing about the real fork between the text and !ok paths). The in-content
// marker stays the text path's only signal — no _meta key.
func TestTruncatedTextResultHasNoOverLimitMeta(t *testing.T) {
	big := json.RawMessage(`{"content":[{"type":"text","text":"` + strings.Repeat("a", 2000) + `"}]}`)
	j, callLog := newJournal()
	cfg := &config.Config{
		MaxResultBytes: 200,
		Upstreams:      []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"echo"}, callResp: mcp.NewResult(mcp.IntID(1), big)}
	r := newTestRegistry(t, cfg, callLog, map[string]*fakeUpstream{"a": fake})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	resp, err := r.CallTool(context.Background(), "a__echo", nil, nil)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if strings.Contains(string(resp.Result), metaKeyResultOverLimit) {
		t.Errorf("truncated text result must not carry %q: %s", metaKeyResultOverLimit, resp.Result)
	}
	// truncationMarker's own \n is JSON-escaped in the marshaled result, so
	// match the literal text rather than the raw (unescaped) marker string.
	if !strings.Contains(string(resp.Result), "truncated by mcp-gate: result exceeded 200 bytes") {
		t.Errorf("truncated text result must carry the in-content marker: %s", resp.Result)
	}
	// The !ok (annotation) journal event must NOT fire on the text path —
	// only the text-truncation path was taken.
	if evs := j.events(t, logging.EventResultTruncationSkipped); len(evs) != 0 {
		t.Errorf("text path must not journal result_truncation_skipped, got %d: %+v", len(evs), evs)
	}
}

// B7. TestUnderLimitNonTextResultUntouched: a non-text result SMALLER than the
// limit passes byte-identical, with no _meta key (early return in truncateResult).
func TestUnderLimitNonTextResultUntouched(t *testing.T) {
	small := json.RawMessage(`{"content":[{"type":"image","data":"AAAA"}]}`)
	cfg := &config.Config{
		MaxResultBytes: 100000,
		Upstreams:      []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"shot"}, callResp: mcp.NewResult(mcp.IntID(1), small)}
	r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

	resp, err := r.CallTool(context.Background(), "a__shot", nil, nil)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if string(resp.Result) != string(small) {
		t.Errorf("under-limit result altered:\n src %s\n out %s", small, resp.Result)
	}
	if strings.Contains(string(resp.Result), metaKeyResultOverLimit) {
		t.Errorf("under-limit result must not carry the over-limit key: %s", resp.Result)
	}
}

// TestPerUpstreamRateLimitOverridesGlobal: the per-upstream block wins over a
// permissive global one (and vice versa a generous per-upstream block would
// bypass a strict global — the config-level table test covers resolution; here
// the strict override is proven to actually bite through CallTool).
func TestPerUpstreamRateLimitOverridesGlobal(t *testing.T) {
	cfg := &config.Config{
		RateLimit: &config.RateLimit{RPS: 1000, Burst: 1000}, // global: effectively unlimited
		Upstreams: []config.Upstream{
			{Name: "a", Enabled: boolPtr(true), RateLimit: &config.RateLimit{RPS: 0.001, Burst: 1}},
		},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"t"}}
	r := startedRegistry(t, cfg, map[string]*fakeUpstream{"a": fake})

	if _, err := r.CallTool(context.Background(), "a__t", nil, nil); err != nil {
		t.Fatalf("CallTool #1: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := r.CallTool(ctx, "a__t", nil, nil); err == nil {
		t.Fatal("CallTool #2: the strict per-upstream rate limit did not apply over the permissive global one")
	}
}
