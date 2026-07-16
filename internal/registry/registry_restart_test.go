package registry

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// boolPtr is a tiny helper for the *bool in RestartPolicy.Enabled.
func boolPtr(b bool) *bool { return &b }

// waitForTool polls the catalog until a namespaced tool appears (or the
// deadline passes). Auto-restart is asynchronous, so tests cannot assert
// synchronously right after killing the process.
func waitForTool(t *testing.T, r *Registry, ns string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, d := range r.Tools() {
			if d.Name == ns {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tool %q did not (re)appear in the catalog within %s: %+v", ns, within, r.Tools())
}

// waitForNoTool is the inverse: it waits until a tool is ABSENT from the
// catalog, used to assert the supervisor gave up and dropped it.
func waitForNoTool(t *testing.T, r *Registry, ns string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		present := false
		for _, d := range r.Tools() {
			if d.Name == ns {
				present = true
				break
			}
		}
		if !present {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tool %q still present after %s; supervisor should have dropped it", ns, within)
}

// TestSupervisorRestartsCrashedUpstream is the core Stage 7a test: a stdio
// upstream that crashes after answering one call is auto-restarted and its
// catalog restored. The fakeserver exits after FAKE_EXIT_AFTER calls; the
// registry's supervisor must relaunch it and re-merge its tools.
func TestSupervisorRestartsCrashedUpstream(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			MaxAttempts:    5,
		},
		Upstreams: []config.Upstream{
			{Name: "crasher", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	// First call succeeds, then the child exits (FAKE_EXIT_AFTER=1).
	if _, err := r.CallTool(context.Background(), "crasher__ping", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("first CallTool: %v", err)
	}

	// The supervisor must relaunch the crashed upstream and restore its catalog.
	// (The relaunched instance would crash again on its own first call, so the
	// assertion is catalog restoration + a fresh callable connection, checked by
	// waiting for a call to eventually succeed against a live restart — each
	// restart survives long enough to answer one call.)
	waitForTool(t, r, "crasher__ping", 5*time.Second)

	// The restarted upstream must be callable again. It crashes after this one
	// call too, but the reply is written before the process exits, so a single
	// call against a freshly-restarted instance succeeds. Retry briefly to avoid
	// racing the exact instant between a crash and the next relaunch.
	if !callSucceedsWithin(r, "crasher__ping", 5*time.Second) {
		t.Fatal("restarted upstream never answered a call within the deadline")
	}
}

// countZombieChildren scans /proc for zombie (state Z) processes whose parent
// is the CURRENT test process — i.e. a stdio-upstream child this test spawned
// that exited but was never reaped via wait() (cmd.Wait, which only runs
// inside StdioConn.Close). Skips (not fails) if /proc is unavailable.
func countZombieChildren(t *testing.T) int {
	t.Helper()
	myPID := os.Getpid()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Skipf("cannot read /proc (non-Linux?): %v", err)
	}
	count := 0
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue
		}
		// Format: "pid (comm) state ppid ...". comm may itself contain
		// spaces/parens, so split on the LAST ')' before reading state/ppid.
		close := strings.LastIndex(string(data), ")")
		if close < 0 {
			continue
		}
		fields := strings.Fields(string(data)[close+1:])
		if len(fields) < 2 || fields[0] != "Z" {
			continue
		}
		if ppid, err := strconv.Atoi(fields[1]); err == nil && ppid == myPID {
			count++
		}
	}
	return count
}

// TestSupervisorReapsCrashedProcess is a regression test: the auto-restart
// supervisor used to relaunch a crashed upstream via replaceUpstream WITHOUT
// ever closing the OLD (dead) connection — nothing else held a reference to
// it once the registry's map entry was overwritten, so cmd.Wait() was never
// called for it. On Linux that leaves the exited child as a permanent zombie
// (and leaks this side's pipe fds) for the rest of the gateway's lifetime,
// once per crash-restart cycle (found by independent review; confirmed via
// /proc before the fix, 1 new zombie per cycle).
func TestSupervisorReapsCrashedProcess(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			MaxAttempts:    5,
		},
		Upstreams: []config.Upstream{
			{Name: "crasher", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	before := countZombieChildren(t)

	// Trigger the crash (fakeserver exits after answering, FAKE_EXIT_AFTER=1)
	// and wait for the supervisor to relaunch it.
	if _, err := r.CallTool(context.Background(), "crasher__ping", nil); err != nil {
		t.Fatalf("first CallTool: %v", err)
	}
	waitForTool(t, r, "crasher__ping", 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let the reap (cmd.Wait) actually land

	after := countZombieChildren(t)
	if after > before {
		t.Errorf("crashed upstream process was not reaped: %d new zombie(s) after one restart cycle (before=%d, after=%d)",
			after-before, before, after)
	}
}

// callSucceedsWithin retries CallTool until one attempt succeeds or the deadline
// passes. Used where the target self-destructs after each call, so any single
// attempt may land in the brief window between a crash and the next relaunch.
func callSucceedsWithin(r *Registry, ns string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := r.CallTool(ctx, ns, []byte(`{}`))
		cancel()
		if err == nil {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// TestSupervisorGivesUpAndDrops verifies the terminal state: when every restart
// attempt fails, the supervisor stops after MaxAttempts and drops the upstream
// from the catalog instead of looping forever. The upstream is run from a copy
// of the fakeserver binary that the test DELETES after the first crash, so every
// relaunch fails at exec.LookPath — a deterministic "restart always fails".
func TestSupervisorGivesUpAndDrops(t *testing.T) {
	src := buildFakeServer(t)
	bin := filepath.Join(t.TempDir(), "disposable")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read fakeserver: %v", err)
	}
	if err := os.WriteFile(bin, data, 0o755); err != nil {
		t.Fatalf("write disposable binary: %v", err)
	}

	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
			MaxAttempts:    2,
		},
		Upstreams: []config.Upstream{
			{Name: "doomed", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	// Remove the on-disk binary now: the child launched by Start is already
	// running in memory (unaffected), but every relaunch attempt will fail at
	// exec.LookPath. Doing this BEFORE the crashing call removes the race where
	// the supervisor could relaunch successfully before we deleted it.
	if err := os.Remove(bin); err != nil {
		t.Fatalf("remove binary: %v", err)
	}
	if _, err := r.CallTool(context.Background(), "doomed__ping", []byte(`{}`)); err != nil {
		t.Fatalf("first CallTool: %v", err)
	}

	// After exhausting its 2 attempts, the supervisor must drop the upstream.
	waitForNoTool(t, r, "doomed__ping", 5*time.Second)
}

// TestSupervisorDisabled confirms that with restart disabled, no supervisor is
// started, so a crashed upstream is NOT resurrected: subsequent calls keep
// failing (the MVP behaviour — a dead upstream stays in the catalog as a stale
// entry until the gateway is restarted, since nothing reaps it).
func TestSupervisorDisabled(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "solo", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if _, err := r.CallTool(context.Background(), "solo__ping", []byte(`{}`)); err != nil {
		t.Fatalf("first CallTool: %v", err)
	}
	// The child has now exited. With restart disabled it never comes back, so a
	// later call must keep failing — it is never revived. If auto-restart had
	// (wrongly) run, callSucceedsWithin would see a success.
	if callSucceedsWithin(r, "solo__ping", 2*time.Second) {
		t.Fatal("crashed upstream was revived even though restart is disabled")
	}
}

// TestSupervisorStopsCleanlyOnClose is the race-guard: Close cancels procCtx and
// must wait for the supervisor to unwind without a data race or leak, even while
// the supervisor is actively backing off between restart attempts. Run under
// -race, this proves Close/supervisor synchronization is sound.
func TestSupervisorStopsCleanlyOnClose(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			MaxAttempts:    0, // unlimited: the supervisor would loop forever if Close did not stop it
		},
		Upstreams: []config.Upstream{
			{Name: "flapper", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := r.CallTool(context.Background(), "flapper__ping", []byte(`{}`)); err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	// Let a couple of restart cycles happen so the supervisor is genuinely busy.
	time.Sleep(100 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return; supervisor likely not stopped")
	}
}

// TestUpstreamListChangedRefreshesCatalog is the Stage 7b test: a stdio upstream
// sends notifications/tools/list_changed and changes its advertised tool set;
// the registry must re-list that upstream and update the aggregated catalog. The
// fakeserver reads its tools from FAKE_TOOLS_FILE on every tools/list and emits
// a list_changed when FAKE_NOTIFY_FILE becomes non-empty.
func TestUpstreamListChangedRefreshesCatalog(t *testing.T) {
	bin := buildFakeServer(t)
	dir := t.TempDir()
	toolsFile := filepath.Join(dir, "tools")
	notifyFile := filepath.Join(dir, "notify")
	if err := os.WriteFile(toolsFile, []byte("ping"), 0o600); err != nil {
		t.Fatalf("seed tools file: %v", err)
	}

	cfg := &config.Config{
		// Disable restart so the only catalog change under test is the list_changed
		// re-list, not a spurious auto-restart.
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "dyn", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS_FILE":  toolsFile,
				"FAKE_NOTIFY_FILE": notifyFile,
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	// Initially only ping is advertised.
	waitForTool(t, r, "dyn__ping", 2*time.Second)

	// Change the tool set, then poke the notify file so the upstream emits
	// list_changed. The registry must re-list and pick up the new tool.
	if err := os.WriteFile(toolsFile, []byte("ping,pong"), 0o600); err != nil {
		t.Fatalf("update tools file: %v", err)
	}
	if err := os.WriteFile(notifyFile, []byte("go"), 0o600); err != nil {
		t.Fatalf("touch notify file: %v", err)
	}

	waitForTool(t, r, "dyn__pong", 5*time.Second)
}

// TestUpstreamListChangedNotifiesSubscribers checks that a re-list driven by an
// upstream list_changed also fans out a catalog-change signal to subscribers
// (the client-facing transport), so the client is told to re-list too.
func TestUpstreamListChangedNotifiesSubscribers(t *testing.T) {
	bin := buildFakeServer(t)
	dir := t.TempDir()
	toolsFile := filepath.Join(dir, "tools")
	notifyFile := filepath.Join(dir, "notify")
	if err := os.WriteFile(toolsFile, []byte("ping"), 0o600); err != nil {
		t.Fatalf("seed tools file: %v", err)
	}

	cfg := &config.Config{
		Restart: config.RestartPolicy{Enabled: boolPtr(false)},
		Upstreams: []config.Upstream{
			{Name: "dyn", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS_FILE":  toolsFile,
				"FAKE_NOTIFY_FILE": notifyFile,
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, noopPayloadLog(), true)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	sub, unsub := r.Subscribe()
	defer unsub()

	if err := os.WriteFile(toolsFile, []byte("ping,pong"), 0o600); err != nil {
		t.Fatalf("update tools file: %v", err)
	}
	if err := os.WriteFile(notifyFile, []byte("go"), 0o600); err != nil {
		t.Fatalf("touch notify file: %v", err)
	}

	select {
	case <-sub:
		// success: subscriber was signalled about the catalog change.
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber not signalled after upstream list_changed re-list")
	}
}
