package registry

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
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
		Upstreams: []config.Upstream{{Name: "a", Enabled: true}},
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
		Upstreams: []config.Upstream{{Name: "a", Enabled: true}},
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
		{Name: "a", Enabled: true, MaxConcurrent: 1},
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

// TestPerUpstreamCallTimeoutOverridesGlobal proves the override actually bounds
// the forwarded call: the global timeout is generous, the per-upstream one is
// tiny, and a hanging upstream must be cut off by the tiny one.
func TestPerUpstreamCallTimeoutOverridesGlobal(t *testing.T) {
	cfg := &config.Config{
		CallTimeout: 30 * time.Second,
		Upstreams: []config.Upstream{
			{Name: "a", Enabled: true, CallTimeout: 50 * time.Millisecond},
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
		u := config.Upstream{Name: "a", Enabled: true, MaxConcurrent: conc}
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
	r.truncateResult(resp, "a", 10)
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
			Upstreams:      []config.Upstream{{Name: "a", Enabled: true}},
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
				{Name: "a", Enabled: true, MaxResultBytes: intPtr(300)},
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
				{Name: "a", Enabled: true, MaxResultBytes: intPtr(0)},
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

// TestPerUpstreamRateLimitOverridesGlobal: the per-upstream block wins over a
// permissive global one (and vice versa a generous per-upstream block would
// bypass a strict global — the config-level table test covers resolution; here
// the strict override is proven to actually bite through CallTool).
func TestPerUpstreamRateLimitOverridesGlobal(t *testing.T) {
	cfg := &config.Config{
		RateLimit: &config.RateLimit{RPS: 1000, Burst: 1000}, // global: effectively unlimited
		Upstreams: []config.Upstream{
			{Name: "a", Enabled: true, RateLimit: &config.RateLimit{RPS: 0.001, Burst: 1}},
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
