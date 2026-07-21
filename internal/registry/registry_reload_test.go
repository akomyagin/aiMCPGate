package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
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
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
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
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
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
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
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
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
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
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
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
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
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

// TestReloadRemovedUpstreamNotResurrectedByRestart targets the supervisor-vs-
// reload race in its narrowest window: the crashed upstream's supervisor is
// ALREADY INSIDE launch() (past the backoff select, blocked in the handshake)
// when a Reload removes that upstream. The restart's launch then completes
// AFTER the reload retired the supervisor, and its fresh connection must not
// enter the catalog — replaceUpstreamIfLive checks the retired supervisor
// context atomically with the catalog write. Run under -race.
//
// The window is staged deterministically with three nested timings:
//
//	t=0      crash (CallTool on a FAKE_EXIT_AFTER=1 upstream); done closes
//	         within a few ms, supervise enters restart(), backoff-waits 5ms,
//	         then calls launch() — which blocks ~500ms inside the handshake
//	         (FAKE_INIT_DELAY delays the relaunched child's initialize reply);
//	t=100ms  Reload removes the upstream: retireSupervisor cancels supCtx
//	         while launch() is still mid-handshake (100ms >> 5ms backoff,
//	         100ms << 500ms init delay — comfortable margin on both sides);
//	t≈510ms  launch() returns a live connection; replaceUpstreamIfLive must
//	         see the cancelled supCtx and refuse the install;
//	t=800ms  final check — the upstream must still be gone.
func TestReloadRemovedUpstreamNotResurrectedByRestart(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
			MaxAttempts:    0, // unlimited: a resurrecting supervisor would bring it back forever
		},
		Upstreams: []config.Upstream{
			{Name: "gone", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "g",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
				// Every launch of this upstream (including the supervisor's
				// relaunch after the crash) stalls 500ms answering initialize,
				// pinning the restart inside launch() while Reload runs.
				"FAKE_INIT_DELAY": "500ms",
			}},
			{Name: "kept", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "k"}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "gone__g", 2*time.Second)

	// Crash the upstream (FAKE_EXIT_AFTER=1): its supervisor notices the exit,
	// passes the 5ms backoff and enters launch(), where the relaunched child
	// holds initialize for 500ms.
	if _, err := r.CallTool(context.Background(), "gone__g", []byte(`{}`)); err != nil {
		t.Fatalf("crashing CallTool: %v", err)
	}
	// Give the supervisor time to get INTO launch() (crash detection + 5ms
	// backoff fit easily), but stay far below the 500ms init delay so the
	// reload below lands while launch() is still in flight.
	time.Sleep(100 * time.Millisecond)

	newCfg := &config.Config{
		Restart: cfg.Restart,
		Upstreams: []config.Upstream{
			{Name: "kept", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "k"}},
		},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// Wait out the rest of the in-flight launch (~400ms remain) with margin:
	// only after it lands can we know whether replaceUpstreamIfLive discarded it.
	time.Sleep(700 * time.Millisecond)
	if hasTool(r, "gone__g") {
		t.Fatal("removed upstream was resurrected by an in-flight restart during reload")
	}
	if !hasTool(r, "kept__k") {
		t.Fatal("unrelated upstream kept__k lost across reload")
	}
}

// TestReloadChangedUpstreamNotDoubledByRestart is the CHANGED-flavour of the
// same race, staged in the same narrow window as the removed-flavour test
// above: the OLD supervisor's crash-restart is ALREADY INSIDE launch() (its
// relaunched child stalls 500ms on initialize via FAKE_INIT_DELAY) when the
// reload replaces the upstream with a CHANGED launch (new env, no delay). The
// reload's new connection installs immediately; the old supervisor's launch
// lands ~400ms later against a cancelled supCtx and must be discarded, leaving
// exactly one live tool set — the NEW one. Timeline: crash at t=0, old restart
// inside launch() by t≈10ms, reload at t=100ms (>> 5ms backoff, << 500ms init
// delay), old launch returns at t≈510ms, final check at t≈800ms. Run under
// -race.
func TestReloadChangedUpstreamNotDoubledByRestart(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
			MaxAttempts:    0,
		},
		Upstreams: []config.Upstream{
			{Name: "svc", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "old",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
				// The OLD env only: the supervisor's relaunch after the crash
				// stalls 500ms in initialize, so it is mid-launch() when the
				// reload below retires it. The NEW env has no delay — the
				// reload's own relaunch is instant.
				"FAKE_INIT_DELAY": "500ms",
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "svc__old", 2*time.Second)

	// Crash the old instance: its supervisor passes the 5ms backoff and blocks
	// inside launch() (old env → 500ms initialize stall).
	if _, err := r.CallTool(context.Background(), "svc__old", []byte(`{}`)); err != nil {
		t.Fatalf("crashing CallTool: %v", err)
	}
	// Land the reload while that launch() is still in flight: 100ms is far past
	// the crash-detection + 5ms backoff, far before the 500ms initialize stall.
	time.Sleep(100 * time.Millisecond)

	newCfg := &config.Config{
		Restart: cfg.Restart,
		Upstreams: []config.Upstream{
			{Name: "svc", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS": "new",
				"FAKE_ECHO":  "1",
			}},
		},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	waitForTool(t, r, "svc__new", 2*time.Second)
	// Wait out the old supervisor's in-flight launch (~400ms remain) with
	// margin, then check it was discarded: exactly one live tool set remains —
	// the new one.
	time.Sleep(700 * time.Millisecond)
	if hasTool(r, "svc__old") {
		t.Fatal("old tool set resurrected by the retired supervisor's restart")
	}
	var owned []string
	for _, d := range r.Tools() {
		if d.Upstream == "svc" {
			owned = append(owned, d.Name)
		}
	}
	if len(owned) != 1 || owned[0] != "svc__new" {
		t.Fatalf("upstream svc owns %v, want exactly [svc__new]", owned)
	}
}

// TestReloadUpdatesRestartPolicyForUnchangedUpstream verifies the restart
// policy is re-read per attempt, not captured at supervisor creation: the
// gateway starts with a give-up-fast policy (MaxAttempts=1), a reload switches
// to unlimited attempts WITHOUT touching the upstream (unchanged → supervisor
// survives), and a subsequent crash must be retried under the NEW policy. The
// relaunch is forced to fail at first (binary removed from disk) longer than
// the old budget would tolerate, then the binary is restored — recovery is
// only possible if the new unlimited policy is in effect.
func TestReloadUpdatesRestartPolicyForUnchangedUpstream(t *testing.T) {
	src := buildFakeServer(t)
	bin := filepath.Join(t.TempDir(), "phoenix-bin")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read fakeserver: %v", err)
	}
	if err := os.WriteFile(bin, data, 0o755); err != nil {
		t.Fatalf("write disposable binary: %v", err)
	}

	up := config.Upstream{Name: "phoenix", Command: bin, Enabled: true, Env: map[string]string{
		"FAKE_TOOLS":      "ping",
		"FAKE_ECHO":       "1",
		"FAKE_EXIT_AFTER": "1",
	}}
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     40 * time.Millisecond,
			MaxAttempts:    1, // old policy: give up after a single failed attempt
		},
		Upstreams: []config.Upstream{up},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "phoenix__ping", 2*time.Second)

	// Reload: upstream unchanged (same launch), only the GLOBAL restart policy
	// widens to unlimited attempts. The running supervisor must pick this up.
	newCfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     40 * time.Millisecond,
			MaxAttempts:    0, // unlimited
		},
		Upstreams: []config.Upstream{up},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// Make every relaunch fail (binary gone), crash the upstream, and wait out
	// well more than the OLD budget (1 attempt at ~10ms) before restoring the
	// binary. Under the old captured policy the supervisor is already gone by
	// then and the tool never returns.
	if err := os.Remove(bin); err != nil {
		t.Fatalf("remove binary: %v", err)
	}
	if _, err := r.CallTool(context.Background(), "phoenix__ping", []byte(`{}`)); err != nil {
		t.Fatalf("crashing CallTool: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := os.WriteFile(bin, data, 0o755); err != nil {
		t.Fatalf("restore binary: %v", err)
	}

	// Recovery proves the supervisor is still retrying — i.e. the reloaded
	// unlimited policy applied to an already-running supervisor.
	waitForTool(t, r, "phoenix__ping", 5*time.Second)
}

// TestReloadParallelBringUpManyUpstreams checks correctness (not speed) of the
// parallel added/changed bring-up: a reload adding 5 upstreams at once must
// merge all 5 into the catalog, with the pre-existing upstream untouched.
func TestReloadParallelBringUpManyUpstreams(t *testing.T) {
	bin := buildFakeServer(t)
	base := config.Upstream{Name: "base", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "b"}}
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{base},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "base__b", 2*time.Second)

	ups := []config.Upstream{base}
	for i := 1; i <= 5; i++ {
		ups = append(ups, config.Upstream{
			Name: fmt.Sprintf("add%d", i), Command: bin, Enabled: true,
			Env: map[string]string{"FAKE_TOOLS": fmt.Sprintf("t%d", i)},
		})
	}
	newCfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: ups,
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// Reload is synchronous: by the time it returns, every successfully added
	// upstream is merged.
	for i := 1; i <= 5; i++ {
		ns := fmt.Sprintf("add%d__t%d", i, i)
		if !hasTool(r, ns) {
			t.Errorf("added upstream tool %q missing after parallel reload", ns)
		}
	}
	if !hasTool(r, "base__b") {
		t.Error("pre-existing upstream base__b lost across reload")
	}
}

// TestReloadRemovedFreedNameReusedByAdded pins the reload ordering: removals
// run FIRST (sequentially), so a client-facing name freed by a removed upstream
// (via rename) can be claimed by an added upstream renamed onto the SAME name
// within one reload. If added ran before/with removed, the merge would hit the
// duplicate-name skip and the added upstream would lose the name.
func TestReloadRemovedFreedNameReusedByAdded(t *testing.T) {
	bin := buildFakeServer(t)
	first := config.Upstream{
		Name: "first", Command: bin, Enabled: true,
		Env:   map[string]string{"FAKE_TOOLS": "a"},
		Tools: config.ToolFilter{Rename: map[string]string{"a": "shared_name"}},
	}
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{first},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "shared_name", 2*time.Second)

	second := config.Upstream{
		Name: "second", Command: bin, Enabled: true,
		Env:   map[string]string{"FAKE_TOOLS": "b"},
		Tools: config.ToolFilter{Rename: map[string]string{"b": "shared_name"}},
	}
	newCfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{second},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	var owner string
	for _, d := range r.Tools() {
		if d.Name == "shared_name" {
			owner = d.Upstream
		}
	}
	if owner != "second" {
		t.Fatalf("shared_name owned by %q after reload, want %q (removed must free the name before added claims it)", owner, "second")
	}
}

// TestReloadParallelMergeOrderDeterministic pins the launch/merge split inside
// Reload: launches of added/changed upstreams run in parallel, but the catalog
// MERGE runs sequentially, in CONFIG order, after all launches complete. Two
// added upstreams collide on the client-facing name "one__t" — "one" advertises
// tool "t" under the default name (not statically known to config.Validate, so
// the collision cannot be rejected at load time), while "two" renames its tool
// "z" onto that same name. "one" is listed FIRST in the config but finishes
// launching LAST (FAKE_INIT_DELAY holds its handshake 300ms; "two" launches
// instantly), so with merge-inside-goroutines the winner would be "two"
// (completion order); with the sequential post-Wait merge it must be "one"
// (config order). Run under -race: the goroutines write disjoint slots of the
// shared results slice.
func TestReloadParallelMergeOrderDeterministic(t *testing.T) {
	bin := buildFakeServer(t)
	base := config.Upstream{Name: "base", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "b"}}
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{base},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "base__b", 2*time.Second)

	one := config.Upstream{Name: "one", Command: bin, Enabled: true, Env: map[string]string{
		"FAKE_TOOLS": "t",
		// Slow launch: "one" reliably finishes AFTER "two", inverting the
		// config order at the goroutine-completion level.
		"FAKE_INIT_DELAY": "300ms",
	}}
	two := config.Upstream{
		Name: "two", Command: bin, Enabled: true,
		Env:   map[string]string{"FAKE_TOOLS": "z"},
		Tools: config.ToolFilter{Rename: map[string]string{"z": "one__t"}},
	}
	newCfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{base, one, two},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	var owner string
	for _, d := range r.Tools() {
		if d.Name == "one__t" {
			owner = d.Upstream
		}
	}
	if owner != "one" {
		t.Fatalf("collided name one__t owned by %q, want %q (merge must follow config order, not goroutine completion order)", owner, "one")
	}
}

// TestReloadChangedUpstreamDroppedEarlyDuringSlowSiblingLaunch pins the timing
// of the CHANGED path inside Reload: the old catalog entry is dropped INSIDE
// the launch goroutine, right after the old connection is closed — not in the
// sequential merge pass after g.Wait(). With the drop deferred to the merge
// pass, the entry would linger pointing at the already-closed connection for
// as long as the SLOWEST sibling launch in the batch takes (here: an added
// upstream stalls 500ms in initialize), and a client calling the changed
// upstream mid-reload would hit a transport error against a closed conn
// instead of an honest "unknown tool" (regression found by review of the
// deterministic-merge fix). Timeline:
//
//	t=0      Reload starts in the background: the changed upstream "svc" is
//	         retired+closed+dropped within milliseconds (its own relaunch is
//	         instant), while the added sibling "slow" holds the merge pass
//	         until ≈t=500ms;
//	t≤300ms  svc__old must already be OUT of the catalog (polled), with
//	         Reload still in flight — and CallTool("svc__old") must fail with
//	         "unknown tool", never with a closed-connection transport error;
//	t≈500ms  Reload returns; svc__new and slow__s are merged.
//
// Run under -race.
func TestReloadChangedUpstreamDroppedEarlyDuringSlowSiblingLaunch(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "svc", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "old"}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "svc__old", 2*time.Second)

	newCfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			// Same name, different env → CHANGED; its own relaunch is instant.
			{Name: "svc", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "new"}},
			// ADDED sibling whose 500ms initialize stall holds the whole
			// errgroup — and with it the sequential merge pass — open.
			{Name: "slow", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "s",
				"FAKE_INIT_DELAY": "500ms",
			}},
		},
	}
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- r.Reload(context.Background(), newCfg) }()

	// The old entry must leave the catalog well before the slow sibling lets
	// Reload return: the drop runs in the goroutine within milliseconds, and
	// 300ms is a comfortable bound far under the 500ms stall.
	start := time.Now()
	for hasTool(r, "svc__old") {
		if time.Since(start) > 300*time.Millisecond {
			t.Fatal("svc__old still in catalog 300ms into Reload — old entry not dropped early, lingering until the slow sibling's launch completes")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case err := <-reloadDone:
		t.Fatalf("Reload already returned (err=%v) — the slow sibling did not hold the merge pass; timing staging broken", err)
	default:
	}
	// Mid-reload client semantics: the dropped entry yields "unknown tool" —
	// NOT a transport failure against the closed old connection, which is what
	// a lingering entry would produce.
	if _, err := r.CallTool(context.Background(), "svc__old", []byte(`{}`)); err == nil {
		t.Fatal("CallTool(svc__old) succeeded mid-reload, want unknown-tool error")
	} else if !strings.Contains(err.Error(), "unknown tool") {
		t.Fatalf("CallTool(svc__old) mid-reload = %q, want an unknown-tool error, not a transport error", err)
	}

	if err := <-reloadDone; err != nil {
		t.Fatalf("Reload: %v", err)
	}
	waitForTool(t, r, "svc__new", 2*time.Second)
	if !hasTool(r, "slow__s") {
		t.Error("added sibling slow__s missing after reload")
	}
}

// gatedListUpstream wraps fakeUpstream so a test can hold a re-list's ListTools
// RPC "in flight" for exactly as long as it wants: every call after the first
// (Start's initial list) signals `entered` and then blocks until `release` is
// closed. This stages the relistUpstream-vs-Reload race deterministically —
// the stale result returns strictly AFTER the reload completed, no calibrated
// sleeps involved.
type gatedListUpstream struct {
	*fakeUpstream
	mu      sync.Mutex
	calls   int
	entered chan struct{} // receives one value when a gated ListTools starts
	release chan struct{} // a gated ListTools proceeds once this is closed
}

func (g *gatedListUpstream) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	g.mu.Lock()
	g.calls++
	n := g.calls
	g.mu.Unlock()
	if n > 1 { // call 1 is Start's initial list; later calls are re-lists under test
		g.entered <- struct{}{}
		<-g.release
	}
	return g.fakeUpstream.ListTools(ctx)
}

// TestRelistStaleResultDiscardedAfterReloadRemoved pins the relist-vs-reload
// race in its REMOVED flavour, fully deterministically: relistUpstream has read
// the connection and its ListTools RPC is in flight (gated) when a Reload
// removes that exact upstream; the RPC then completes strictly AFTER the reload
// finished. The stale result must be discarded (replaceUpstreamIfCurrent sees
// the conn is no longer current) — an unconditional replaceUpstream would
// resurrect the removed upstream. Run under -race.
func TestRelistStaleResultDiscardedAfterReloadRemoved(t *testing.T) {
	gated := &gatedListUpstream{
		fakeUpstream: &fakeUpstream{name: "dyn", tools: []string{"a"}},
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	keep := &fakeUpstream{name: "keep", tools: []string{"k"}}
	cfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "dyn", Enabled: true},
			{Name: "keep", Enabled: true},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) {
		switch u.Name {
		case "dyn":
			return gated, nil
		case "keep":
			return keep, nil
		}
		return nil, errors.New("no fake for " + u.Name)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	if !hasTool(r, "dyn__a") {
		t.Fatal("precondition: dyn__a not in catalog after Start")
	}

	// The re-list will fetch a DIFFERENT tool set, so a stale write is visible.
	gated.fakeUpstream.tools = []string{"stale"}

	// Kick the re-list exactly as the debounce timer's expiry would; its
	// ListTools blocks on the gate with the OLD conn already read.
	relisted := make(chan struct{})
	go func() { defer close(relisted); r.relistUpstream("dyn") }()
	<-gated.entered // the re-list RPC is now in flight

	// While it is in flight, a reload removes dyn entirely.
	newCfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{{Name: "keep", Enabled: true}},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if hasTool(r, "dyn__a") {
		t.Fatal("precondition: reload did not remove dyn")
	}

	// Only now does the stale ListTools return — strictly after the reload.
	close(gated.release)
	<-relisted

	for _, ns := range []string{"dyn__a", "dyn__stale"} {
		if hasTool(r, ns) {
			t.Fatalf("stale re-list resurrected removed upstream (tool %q present)", ns)
		}
	}
	if !hasTool(r, "keep__k") {
		t.Fatal("unrelated upstream keep__k lost")
	}
}

// TestRelistStaleResultDiscardedAfterReloadChanged is the CHANGED flavour of
// the same race: while the old connection's re-list RPC is in flight, a reload
// replaces the upstream with a new launch (new connection, new tool set). The
// stale result — carrying the OLD connection — must not clobber the fresh
// entry: replaceUpstreamIfCurrent compares connection identity, and the map now
// holds the reload's new conn. Run under -race.
func TestRelistStaleResultDiscardedAfterReloadChanged(t *testing.T) {
	gated := &gatedListUpstream{
		fakeUpstream: &fakeUpstream{name: "dyn", tools: []string{"a"}},
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	dynNew := &fakeUpstream{name: "dyn", tools: []string{"b"}}
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{{Name: "dyn", Enabled: true}},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	var startMu sync.Mutex
	dynLaunches := 0
	r.start = func(_ context.Context, u config.Upstream) (Upstream, error) {
		if u.Name != "dyn" {
			return nil, errors.New("no fake for " + u.Name)
		}
		startMu.Lock()
		dynLaunches++
		n := dynLaunches
		startMu.Unlock()
		if n == 1 {
			return gated, nil // Start's launch: the gated old connection
		}
		return dynNew, nil // the reload's relaunch: the fresh connection
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	if !hasTool(r, "dyn__a") {
		t.Fatal("precondition: dyn__a not in catalog after Start")
	}

	gated.fakeUpstream.tools = []string{"stale"}

	relisted := make(chan struct{})
	go func() { defer close(relisted); r.relistUpstream("dyn") }()
	<-gated.entered // the old conn's re-list RPC is now in flight

	// Same name, different env → different launch → CHANGED: the reload closes
	// the old conn and installs dynNew while the stale RPC is still pending.
	newCfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "dyn", Enabled: true, Env: map[string]string{"V": "2"}},
		},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if !hasTool(r, "dyn__b") {
		t.Fatal("precondition: reload did not install the changed upstream")
	}

	close(gated.release)
	<-relisted

	if hasTool(r, "dyn__stale") || hasTool(r, "dyn__a") {
		t.Fatal("stale re-list clobbered the reloaded upstream's fresh catalog")
	}
	if !hasTool(r, "dyn__b") {
		t.Fatal("fresh catalog entry dyn__b lost after stale re-list returned")
	}
}

// TestReloadRemovedUpstreamNotResurrectedByStaleRelist is the end-to-end
// (real child processes) flavour of the relist-vs-reload race, staged with
// FAKE_LIST_DELAY the way the supervisor-race tests above use FAKE_INIT_DELAY:
//
//	t=0      the tools file changes and the notify file is touched; the
//	         fakeserver's poller (20ms interval) emits list_changed, the
//	         registry debounces 200ms and sends tools/list — which the server
//	         holds for 600ms (FAKE_LIST_DELAY), so the re-list RPC is in
//	         flight from ≈t=240ms to ≈t=840ms;
//	t=400ms  Reload removes the upstream (400ms >> the ≈240ms re-list start,
//	         400ms << the ≈840ms completion — comfortable margin both ways).
//	         Note the reload's Close blocks until the server finishes the
//	         delayed answer and exits, and the stdio reader delivers that
//	         answer to the in-flight ListTools BEFORE it sees EOF — so the
//	         stale re-list genuinely SUCCEEDS right around the drop;
//	t=1.4s   final check: the upstream must be gone, whichever way the stale
//	         result and the drop interleaved.
//
// Run under -race.
func TestReloadRemovedUpstreamNotResurrectedByStaleRelist(t *testing.T) {
	bin := buildFakeServer(t)
	dir := t.TempDir()
	toolsFile := filepath.Join(dir, "tools")
	notifyFile := filepath.Join(dir, "notify")
	if err := os.WriteFile(toolsFile, []byte("old"), 0o600); err != nil {
		t.Fatalf("seed tools file: %v", err)
	}

	cfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "dyn", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS_FILE":  toolsFile,
				"FAKE_NOTIFY_FILE": notifyFile,
				// Every tools/list from this upstream (including Start's initial
				// one) takes 600ms — long enough to land a Reload mid-re-list.
				"FAKE_LIST_DELAY": "600ms",
			}},
			{Name: "kept", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "k"}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "dyn__old", 5*time.Second)

	// Change the advertised tool set so the stale re-list result is visibly
	// different, then trigger list_changed.
	if err := os.WriteFile(toolsFile, []byte("old,stale"), 0o600); err != nil {
		t.Fatalf("update tools file: %v", err)
	}
	if err := os.WriteFile(notifyFile, []byte("go"), 0o600); err != nil {
		t.Fatalf("touch notify file: %v", err)
	}

	// Land the reload while the re-list RPC is in flight (see the timeline in
	// the doc comment: in-flight window ≈[240ms, 840ms] after the touch).
	time.Sleep(400 * time.Millisecond)
	newCfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "kept", Command: bin, Enabled: true, Env: map[string]string{"FAKE_TOOLS": "k"}},
		},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// The stale result lands right around Reload's return; give it a generous
	// window to (wrongly) write before the final check.
	time.Sleep(500 * time.Millisecond)
	for _, ns := range []string{"dyn__old", "dyn__stale"} {
		if hasTool(r, ns) {
			t.Fatalf("removed upstream resurrected by a stale re-list (tool %q present)", ns)
		}
	}
	if !hasTool(r, "kept__k") {
		t.Fatal("unrelated upstream kept__k lost across reload")
	}
}

// TestReloadDisablesRunningSupervisor verifies restart() re-reads
// policy.Enabled on every attempt, not just when the supervisor is created:
// a supervisor already in its retry loop (unlimited attempts, relaunches
// failing because the binary is gone) must STOP once a reload disables
// auto-restart globally — the previous behaviour retried forever, ignoring the
// new Enabled=false. Structure mirrors
// TestReloadUpdatesRestartPolicyForUnchangedUpstream (the enabling twin):
// there, restoring the binary proves the supervisor kept retrying; here, it
// must prove nothing is retrying anymore.
func TestReloadDisablesRunningSupervisor(t *testing.T) {
	src := buildFakeServer(t)
	bin := filepath.Join(t.TempDir(), "mayfly-bin")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read fakeserver: %v", err)
	}
	if err := os.WriteFile(bin, data, 0o755); err != nil {
		t.Fatalf("write disposable binary: %v", err)
	}

	up := config.Upstream{Name: "mayfly", Command: bin, Enabled: true, Env: map[string]string{
		"FAKE_TOOLS":      "ping",
		"FAKE_ECHO":       "1",
		"FAKE_EXIT_AFTER": "1",
	}}
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     40 * time.Millisecond,
			MaxAttempts:    0, // unlimited: without the fix the supervisor retries forever
		},
		Upstreams: []config.Upstream{up},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()
	waitForTool(t, r, "mayfly__ping", 2*time.Second)

	// Make every relaunch fail (binary gone) and crash the upstream: the
	// supervisor enters an unlimited retry loop at 10–40ms backoff.
	if err := os.Remove(bin); err != nil {
		t.Fatalf("remove binary: %v", err)
	}
	if _, err := r.CallTool(context.Background(), "mayfly__ping", []byte(`{}`)); err != nil {
		t.Fatalf("crashing CallTool: %v", err)
	}
	// Let it genuinely loop through a few failed attempts before the reload.
	time.Sleep(100 * time.Millisecond)

	// Reload: upstream unchanged (its supervisor survives), but auto-restart is
	// now DISABLED globally. The running supervisor must pick this up on its
	// next attempt and give up.
	newCfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(false),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     40 * time.Millisecond,
			MaxAttempts:    0,
		},
		Upstreams: []config.Upstream{up},
	}
	if err := r.Reload(context.Background(), newCfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	// Give the supervisor several would-be backoff cycles to notice the new
	// policy, then restore the binary. If Enabled=false were still ignored, the
	// very next attempt (≤40ms later) would relaunch successfully and the
	// upstream would become callable again — exactly what the enabling twin
	// test relies on for recovery.
	time.Sleep(200 * time.Millisecond)
	if err := os.WriteFile(bin, data, 0o755); err != nil {
		t.Fatalf("restore binary: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Giving up (restart disabled mid-backoff) must be symmetric with the
	// exhausted-attempts terminal state: the dead connection is already closed,
	// so the upstream is DROPPED from the catalog entirely — the client must
	// see "unknown tool", not a live-looking entry whose calls fail with a
	// transport error.
	if hasTool(r, "mayfly__ping") {
		t.Fatal("upstream still in catalog after reload disabled auto-restart mid-backoff — want it dropped")
	}
	if _, err := r.CallTool(context.Background(), "mayfly__ping", []byte(`{}`)); err == nil {
		t.Fatal("upstream recovered after reload disabled auto-restart — supervisor ignored Enabled=false")
	}
}
