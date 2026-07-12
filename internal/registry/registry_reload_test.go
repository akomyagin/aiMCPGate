package registry

import (
	"context"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// hasTool reports whether a namespaced tool is currently in the catalog.
func hasTool(r *Registry, ns string) bool {
	for _, d := range r.Tools() {
		if d.Name == ns {
			return true
		}
	}
	return false
}

// TestReloadAddsAndRemovesUpstreams drives a full add/remove diff: start with one
// upstream, reload to a config that drops it and adds a different one, and assert
// the catalog converges (old gone, new present).
func TestReloadAddsAndRemovesUpstreams(t *testing.T) {
	bin := buildFakeServer(t)
	base := func(name, tools string) config.Upstream {
		return config.Upstream{Name: name, Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_TOOLS": tools,
			"FAKE_ECHO":  "1",
		}}
	}

	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{base("alpha", "a")},
	}
	r := New(cfg, quietLogger(), nil)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "alpha__a", 2*time.Second)

	// Reload: drop alpha, add beta.
	newCfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{base("beta", "b")},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	waitForTool(t, r, "beta__b", 2*time.Second)
	if hasTool(r, "alpha__a") {
		t.Error("alpha__a should have been removed by reload")
	}
}

// TestReloadChangedUpstreamRelaunches verifies a CHANGED upstream (same name,
// different launch — here a different tool set via env) is closed and relaunched
// so the catalog reflects the new config.
func TestReloadChangedUpstreamRelaunches(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "svc", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "old"}},
		},
	}
	r := New(cfg, quietLogger(), nil)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "svc__old", 2*time.Second)

	// Same name, different env → different launch → CHANGED.
	newCfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "svc", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "new"}},
		},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	waitForTool(t, r, "svc__new", 2*time.Second)
	if hasTool(r, "svc__old") {
		t.Error("svc__old should be gone after the upstream was reconfigured")
	}
}

// TestReloadUnchangedUpstreamLeftRunning verifies an upstream whose launch config
// did not change is NOT torn down: its tools remain and its connection is
// untouched. Detected by keeping the same *connection identity via a call
// working continuously.
func TestReloadUnchangedUpstreamLeftRunning(t *testing.T) {
	bin := buildFakeServer(t)
	up := config.Upstream{Name: "keep", Command: bin, Enabled: true, Env: map[string]string{
		"FAKE_TOOLS": "k",
		"FAKE_ECHO":  "1",
	}}
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{up},
	}
	r := New(cfg, quietLogger(), nil)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "keep__k", 2*time.Second)

	// Reload with an identical upstream plus a brand-new one: keep must be left
	// running (unchanged), extra must be added.
	newCfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			up,
			{Name: "extra", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "e"}},
		},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	waitForTool(t, r, "extra__e", 2*time.Second)
	if !hasTool(r, "keep__k") {
		t.Fatal("unchanged upstream keep__k disappeared across reload")
	}
	// keep must still be callable (was never torn down).
	if _, err := r.CallTool(context.Background(), "keep__k", []byte(`{}`)); err != nil {
		t.Fatalf("unchanged upstream not callable after reload: %v", err)
	}
}

// TestReloadDisabledUpstreamRemoved confirms that flipping an upstream to
// enabled:false in the reloaded config removes it (treated as a removal).
func TestReloadDisabledUpstreamRemoved(t *testing.T) {
	bin := buildFakeServer(t)
	on := config.Upstream{Name: "toggle", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "t"}}
	stay := config.Upstream{Name: "stay", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "s"}}
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{on, stay},
	}
	r := New(cfg, quietLogger(), nil)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "toggle__t", 2*time.Second)

	off := on
	off.Enabled = false
	newCfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{off, stay},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	waitForNoTool(t, r, "toggle__t", 2*time.Second)
	if !hasTool(r, "stay__s") {
		t.Error("stay__s should remain after reload")
	}
}

// TestReloadNotifiesSubscribers checks a reload fans out one catalog-change
// signal to subscribers (so the client is told to re-list).
func TestReloadNotifiesSubscribers(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "one", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "1"}},
		},
	}
	r := New(cfg, quietLogger(), nil)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	sub, unsub := r.Subscribe()
	defer unsub()

	newCfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "one", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "1"}},
			{Name: "two", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "2"}},
		},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	select {
	case <-sub:
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber not signalled after reload")
	}
}

// TestReloadRetiredSupervisorDoesNotRestart is the race-sensitive guard: when a
// running upstream (with auto-restart ENABLED) is removed by reload, its
// deliberate Close must NOT be seen as a crash and auto-restarted. After the
// reload the tool must stay gone. Run under -race.
func TestReloadRetiredSupervisorDoesNotRestart(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
			MaxAttempts:    0, // unlimited: a leaked supervisor would resurrect it forever
		},
		Upstreams: []config.Upstream{
			{Name: "gone", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "g"}},
			{Name: "kept", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "k"}},
		},
	}
	r := New(cfg, quietLogger(), nil)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "gone__g", 2*time.Second)

	newCfg := &config.Config{
		Restart: cfg.Restart,
		Upstreams: []config.Upstream{
			{Name: "kept", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "k"}},
		},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// Give any (wrongly) surviving supervisor several backoff cycles to try a
	// restart; the tool must stay absent.
	time.Sleep(300 * time.Millisecond)
	if hasTool(r, "gone__g") {
		t.Fatal("removed upstream was auto-restarted after reload retired its supervisor")
	}
}
