package registry

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// buildFakeServer compiles the shared internal/upstream/testdata/fakeserver
// binary once per test and returns its path. Duplicated (not imported) from
// internal/upstream's test helper: that one lives in an external test package
// (upstream_test) and testdata directories are not importable across packages.
func buildFakeServer(t *testing.T) string {
	t.Helper()
	src := filepath.Join("..", "upstream", "testdata", "fakeserver")
	bin := filepath.Join(t.TempDir(), "fakeserver")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = src
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fakeserver: %v\n%s", err, out)
	}
	return bin
}

// TestRegistryStartDoesNotKillUpstreamsAfterReturning is a regression test for
// the bug found by independent /code-review on Stage 1 (severity: blocks PR):
// upstream child processes were launched under Start's errgroup-derived
// context (gctx), which errgroup cancels the instant g.Wait() returns — i.e.
// the moment Start() itself returns. Every upstream died right after "registry
// ready" logged, and every subsequent CallTool hit a dead connection.
//
// This test uses the REAL startStdio path (default r.start, not the in-process
// fakes the rest of this package's tests use), because that is exactly what the
// bug required to reproduce: the fakes in registry_test.go never spawn a
// process or touch the ctx/gctx distinction at all.
func TestRegistryStartDoesNotKillUpstreamsAfterReturning(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "echoer", Command: bin, Enabled: true, Env: map[string]string{
			"FAKE_TOOLS": "ping",
			"FAKE_ECHO":  "1",
		}},
	}}
	r := New(cfg, quietLogger(), nil)

	startCtx, cancelStart := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelStart()
	if err := r.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Start has now returned, which is precisely when the buggy version's
	// errgroup context (and therefore every upstream child process) died.
	cancelStart() // also simulate the caller's Start-scoped context ending

	tools := r.Tools()
	if len(tools) != 1 || tools[0].Name != "echoer__ping" {
		t.Fatalf("unexpected catalog after Start returned: %+v", tools)
	}

	callCtx, cancelCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCall()
	msg, err := r.CallTool(callCtx, "echoer__ping", []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("CallTool after Start returned: %v (upstream was killed prematurely)", err)
	}
	if msg.Error != nil {
		t.Fatalf("upstream returned an error: %+v", msg.Error)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
