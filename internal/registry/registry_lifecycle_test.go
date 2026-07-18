package registry

// Regression tests for the Start/Reload/Close lifecycle races found by
// independent review after Stage 7 (docs/POST_MVP_TECHNICAL_PLAN.md):
//
//  1. Reload racing a still-running Start could see a handshaking upstream as
//     "not live" and launch a duplicate process → Reload before Start now
//     returns ErrNotStarted (TestReloadBeforeStartRejected).
//  2. Reload's supervisors.Add(1) racing Close's supervisors.Wait() is the
//     forbidden sync.WaitGroup reuse pattern → Reload after/during Close now
//     returns ErrClosing, and both run under one lifecycleMu
//     (TestCloseThenReloadRejected, TestConcurrentReloadAndClose).
//
// The positive path — Reload after a completed Start succeeds — is already
// guarded by the tests in registry_reload_test.go.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// TestReloadBeforeStartRejected: a Reload arriving before Start has completed
// must be rejected with ErrNotStarted instead of diffing against an empty
// catalog (which would double-launch every upstream once Start catches up).
func TestReloadBeforeStartRejected(t *testing.T) {
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{{Name: "never", Command: "true", Enabled: true}},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	defer r.Close()

	err := r.Reload(context.Background(), cfg)
	if !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Reload before Start: err=%v, want ErrNotStarted", err)
	}
}

// TestCloseThenReloadRejected: once Close has begun (or finished), a Reload
// must be rejected with ErrClosing — relaunching upstreams during shutdown
// would leak processes past Close's teardown.
func TestCloseThenReloadRejected(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "one", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "a"}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := r.Reload(context.Background(), cfg)
	if !errors.Is(err, ErrClosing) {
		t.Fatalf("Reload after Close: err=%v, want ErrClosing", err)
	}
}

// TestConcurrentReloadAndClose fires Reload and Close as close to
// simultaneously as a barrier allows, with auto-restart supervisors enabled
// (the WaitGroup the original race was on). It is a smoke/regression guard
// under -race: it need not deterministically hit the old microscopic window,
// it must simply never race, never panic ("WaitGroup is reused before previous
// Wait has returned"), and always leave both calls with a sane outcome —
// Reload either wins the lifecycleMu and fully applies before Close tears
// everything down, or loses it and gets ErrClosing.
func TestConcurrentReloadAndClose(t *testing.T) {
	bin := buildFakeServer(t)
	up := func(name, tools string) config.Upstream {
		return config.Upstream{Name: name, Command: bin, Enabled: true,
			Env: map[string]string{"FAKE_TOOLS": tools}}
	}
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
		},
		Upstreams: []config.Upstream{up("one", "a")},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForTool(t, r, "one__a", 2*time.Second)

	// The reload both keeps "one" and adds "two", so whichever way the race
	// falls the reload path exercises superviseUpstream (supervisors.Add).
	newCfg := &config.Config{
		Restart:   cfg.Restart,
		Upstreams: []config.Upstream{up("one", "a"), up("two", "b")},
	}

	barrier := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-barrier
		if err := r.Reload(context.Background(), newCfg); err != nil && !errors.Is(err, ErrClosing) {
			t.Errorf("Reload: %v (only nil or ErrClosing is acceptable here)", err)
		}
	}()
	go func() {
		defer wg.Done()
		<-barrier
		if err := r.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	close(barrier)
	wg.Wait()
}
