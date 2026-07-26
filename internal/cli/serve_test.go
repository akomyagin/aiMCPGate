package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// waitToolInRegistry polls the registry's aggregated catalog until the
// namespaced tool appears or the deadline passes — reload is asynchronous
// relative to the polling watcher, so the test cannot assert synchronously.
func waitToolInRegistry(t *testing.T, reg *registry.Registry, name string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, d := range reg.Tools() {
			if d.Name == name {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tool %q never appeared in the catalog within %s: %+v", name, within, reg.Tools())
}

// TestPollConfigAppliesReloadOnMtimeChange is the --watch-config test: a
// running registry, a pollConfig watcher on a real temp config file, and an
// edit that changes an upstream's launch env. The mtime bump must be detected
// and the reload applied through the same applyReload path SIGHUP takes —
// observed via the CATALOG: the relaunched upstream advertises a new tool.
// A follow-up BAD edit must be rejected by config.Load with the running
// config kept alive (the new tool stays).
func TestPollConfigAppliesReloadOnMtimeChange(t *testing.T) {
	bin := buildFakeServer(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	writeCfg := func(tools string) {
		t.Helper()
		yaml := fmt.Sprintf(`
transport: stdio
log_level: error
restart:
  enabled: false
upstreams:
  - name: up
    command: %s
    enabled: true
    env:
      FAKE_TOOLS: %q
`, bin, tools)
		if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// bumpMtime pushes the file's mtime forward explicitly, so the test never
	// depends on the filesystem's timestamp granularity to see a change.
	mtimeStep := time.Now()
	bumpMtime := func() {
		t.Helper()
		mtimeStep = mtimeStep.Add(2 * time.Second)
		if err := os.Chtimes(cfgPath, mtimeStep, mtimeStep); err != nil {
			t.Fatal(err)
		}
	}

	writeCfg("ping")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load seed config: %v", err)
	}

	logBuf := &syncWriter{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	payloadLog, err := logging.NewPayloadLog("")
	if err != nil {
		t.Fatalf("NewPayloadLog: %v", err)
	}
	reg := registry.New(cfg, logger, nil, payloadLog, false, "0.0.0-test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := reg.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = reg.Close() }()
	waitToolInRegistry(t, reg, "up__ping", 5*time.Second)

	go pollConfig(ctx, cfgPath, 10*time.Millisecond, reg, logger)

	// Edit 1 (valid): the upstream's env changes → Reload relaunches it and the
	// new tool must land in the catalog. That is Reload observably invoked.
	writeCfg("ping,pong")
	bumpMtime()
	waitToolInRegistry(t, reg, "up__pong", 5*time.Second)
	if !strings.Contains(logBuf.String(), "config file changed, reloading") {
		t.Errorf("watcher never logged the change detection; log:\n%s", logBuf.String())
	}

	// Edit 2 (broken YAML): config.Load fails, the reload is dropped, and the
	// RUNNING config stays live — a bad edit must never take the gateway down.
	if err := os.WriteFile(cfgPath, []byte("upstreams: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	bumpMtime()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logBuf.String(), "reload failed, keeping current config") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(logBuf.String(), "reload failed, keeping current config") {
		t.Fatalf("bad edit was never rejected; log:\n%s", logBuf.String())
	}
	waitToolInRegistry(t, reg, "up__pong", 2*time.Second) // catalog untouched
}

// TestPollConfigStopsOnContextCancel: the watcher goroutine must exit when the
// serve context is cancelled, not leak past shutdown.
func TestPollConfigStopsOnContextCancel(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("transport: stdio"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollConfig(ctx, cfgPath, time.Millisecond, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pollConfig did not return after context cancellation")
	}
}

// TestServeRejectsNegativeWatchConfig: a negative interval is a user error,
// reported up front instead of silently disabling the watcher.
func TestServeRejectsNegativeWatchConfig(t *testing.T) {
	err := runServe(context.Background(), "", "", -time.Second, "test")
	if err == nil || !strings.Contains(err.Error(), "--watch-config") {
		t.Fatalf("runServe with a negative --watch-config returned %v; want a --watch-config error", err)
	}
}
