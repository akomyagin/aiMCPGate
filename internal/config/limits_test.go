package config

import (
	"strings"
	"testing"
	"time"
)

// intPtr is a test helper for the *int max_result_bytes override.
func intPtr(v int) *int { return &v }

// limitsTestConfig builds a config with a global limit set at every level and
// three upstreams: one inheriting everything, one overriding everything, one
// explicitly disabling the inheritable limits.
func limitsTestConfig() *Config {
	return &Config{
		Transport:      TransportStdio,
		CallTimeout:    10 * time.Second,
		RateLimit:      &RateLimit{RPS: 5, Burst: 2},
		MaxResultBytes: 1000,
		Upstreams: []Upstream{
			{Name: "plain", Command: "echo", Enabled: true},
			{
				Name: "tuned", Command: "echo", Enabled: true,
				RateLimit:      &RateLimit{RPS: 1, Burst: 7},
				MaxConcurrent:  3,
				MaxResultBytes: intPtr(50),
				CallTimeout:    2 * time.Second,
			},
			{
				Name: "off", Command: "echo", Enabled: true,
				RateLimit:      &RateLimit{RPS: 0}, // explicit rps: 0 disables the global limit
				MaxResultBytes: intPtr(0),          // explicit 0 disables the global cap
			},
		},
	}
}

func TestEffectiveLimitsFor(t *testing.T) {
	cfg := limitsTestConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	tests := []struct {
		upstream    string
		wantRPS     float64
		wantBurst   int
		wantRateOK  bool
		wantConc    int
		wantResult  int
		wantTimeout time.Duration
	}{
		// plain inherits every global value.
		{"plain", 5, 2, true, 0, 1000, 10 * time.Second},
		// tuned overrides every global value.
		{"tuned", 1, 7, true, 3, 50, 2 * time.Second},
		// off explicitly disables the inheritable limits.
		{"off", 0, 0, false, 0, 0, 10 * time.Second},
		// an unknown upstream falls back to the globals (no overrides known).
		{"ghost", 5, 2, true, 0, 1000, 10 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.upstream, func(t *testing.T) {
			rps, burst, ok := cfg.EffectiveRateLimitFor(tc.upstream)
			if rps != tc.wantRPS || burst != tc.wantBurst || ok != tc.wantRateOK {
				t.Errorf("EffectiveRateLimitFor = (%v, %d, %v), want (%v, %d, %v)",
					rps, burst, ok, tc.wantRPS, tc.wantBurst, tc.wantRateOK)
			}
			if got := cfg.EffectiveMaxConcurrentFor(tc.upstream); got != tc.wantConc {
				t.Errorf("EffectiveMaxConcurrentFor = %d, want %d", got, tc.wantConc)
			}
			if got := cfg.EffectiveMaxResultBytesFor(tc.upstream); got != tc.wantResult {
				t.Errorf("EffectiveMaxResultBytesFor = %d, want %d", got, tc.wantResult)
			}
			if got := cfg.EffectiveCallTimeoutFor(tc.upstream); got != tc.wantTimeout {
				t.Errorf("EffectiveCallTimeoutFor = %v, want %v", got, tc.wantTimeout)
			}
		})
	}
}

// TestEffectiveRateLimitDefaults covers the corners of the rate-limit
// resolution: no config anywhere, burst normalization, global-only.
func TestEffectiveRateLimitDefaults(t *testing.T) {
	// Nothing configured: no limit for anyone.
	cfg := &Config{Transport: TransportStdio, Upstreams: []Upstream{{Name: "a", Command: "echo", Enabled: true}}}
	if _, _, ok := cfg.EffectiveRateLimitFor("a"); ok {
		t.Error("no rate_limit configured, want ok=false")
	}

	// Burst 0 normalizes to 1 — a zero-capacity bucket would block forever.
	cfg.RateLimit = &RateLimit{RPS: 3}
	rps, burst, ok := cfg.EffectiveRateLimitFor("a")
	if !ok || rps != 3 || burst != 1 {
		t.Errorf("EffectiveRateLimitFor = (%v, %d, %v), want (3, 1, true)", rps, burst, ok)
	}
}

func TestEffectiveCallTimeoutForFallsBackToDefault(t *testing.T) {
	// Neither global nor per-upstream set: the built-in default applies.
	cfg := &Config{Transport: TransportStdio, Upstreams: []Upstream{{Name: "a", Command: "echo", Enabled: true}}}
	if got := cfg.EffectiveCallTimeoutFor("a"); got != DefaultCallTimeout {
		t.Errorf("EffectiveCallTimeoutFor = %v, want DefaultCallTimeout %v", got, DefaultCallTimeout)
	}
}

// TestValidateRejectsBadLimits table-drives every negative-value rejection the
// new limit fields introduce, at both the global and per-upstream levels.
func TestValidateRejectsBadLimits(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantSub string
	}{
		{"global negative rps", func(c *Config) { c.RateLimit = &RateLimit{RPS: -1} }, "rate_limit.rps"},
		{"global negative burst", func(c *Config) { c.RateLimit = &RateLimit{RPS: 1, Burst: -1} }, "rate_limit.burst"},
		{"global negative max_result_bytes", func(c *Config) { c.MaxResultBytes = -1 }, "max_result_bytes"},
		{"global negative call_timeout", func(c *Config) { c.CallTimeout = -time.Second }, "call_timeout"},
		{"upstream negative rps", func(c *Config) { c.Upstreams[0].RateLimit = &RateLimit{RPS: -1} }, `upstream "a": rate_limit.rps`},
		{"upstream negative burst", func(c *Config) { c.Upstreams[0].RateLimit = &RateLimit{RPS: 1, Burst: -2} }, `upstream "a": rate_limit.burst`},
		{"upstream negative max_concurrent", func(c *Config) { c.Upstreams[0].MaxConcurrent = -1 }, "max_concurrent"},
		{"upstream negative max_result_bytes", func(c *Config) { c.Upstreams[0].MaxResultBytes = intPtr(-5) }, `upstream "a": max_result_bytes`},
		{"upstream negative call_timeout", func(c *Config) { c.Upstreams[0].CallTimeout = -time.Second }, `upstream "a": call_timeout`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Transport: TransportStdio, Upstreams: []Upstream{{Name: "a", Command: "echo", Enabled: true}}}
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate: want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("Validate error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

// TestSameLaunchIgnoresLimitFields pins the Round 6 reload contract: a config
// edit that only changes call limits must NOT count as a launch change (no
// relaunch of the upstream process) — the registry re-reads limits live.
func TestSameLaunchIgnoresLimitFields(t *testing.T) {
	a := Upstream{Name: "a", Command: "echo", Args: []string{"x"}, Enabled: true}
	b := a
	b.RateLimit = &RateLimit{RPS: 1, Burst: 1}
	b.MaxConcurrent = 4
	b.MaxResultBytes = intPtr(100)
	b.CallTimeout = 3 * time.Second
	if !a.SameLaunch(b) {
		t.Error("SameLaunch = false for a limits-only change; limits must not force a relaunch")
	}
	if !a.SameFilter(b) {
		t.Error("SameFilter = false for a limits-only change; limits are not part of the tool filter")
	}
}
