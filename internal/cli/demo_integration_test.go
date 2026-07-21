package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// buildGateBinary compiles the REAL mcp-gate binary (./cmd from the repo
// root) into a temp dir and returns its path. Unlike buildFakeServer this is
// the actual gateway: the demo integration test needs the same binary to act
// as both the gateway and its own __demo-echo upstream, exactly like
// demo.config.yaml does inside the OCI image.
func buildGateBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "mcp-gate")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd")
	cmd.Dir = filepath.Join("..", "..") // repo root, relative to internal/cli
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build mcp-gate: %v\n%s", err, out)
	}
	return bin
}

// readResponse reads framed messages from the gateway's stdout until it sees
// the response carrying wantID, skipping server-initiated notifications. Each
// read is bounded by deadline so a hung gateway fails the test instead of
// stalling it.
func readResponse(t *testing.T, r *mcp.Reader, wantID string, deadline time.Duration) *mcp.Message {
	t.Helper()
	type result struct {
		msg *mcp.Message
		err error
	}
	timeout := time.After(deadline)
	for {
		ch := make(chan result, 1)
		go func() {
			msg, err := r.Read()
			ch <- result{msg, err}
		}()
		select {
		case res := <-ch:
			if res.err != nil {
				t.Fatalf("read gateway response (want id %s): %v", wantID, res.err)
			}
			if !res.msg.IsResponse() {
				continue // e.g. a list_changed notification — not what we wait for
			}
			if got := string(res.msg.ID); got != wantID {
				t.Fatalf("response id = %s, want %s (message: %+v)", got, wantID, res.msg)
			}
			return res.msg
		case <-timeout:
			t.Fatalf("no response with id %s within %s", wantID, deadline)
		}
	}
}

// TestServeWithDemoEchoUpstream is the end-to-end regression test for the
// Stage 12 sandbox scenario (Glama.ai): the real mcp-gate binary runs `serve`
// with a config whose only upstream is the SAME binary in its hidden
// `__demo-echo` role — a real child process through Registry.Start/exec, the
// exact path demo.config.yaml exercises inside the OCI image. The test drives
// it like a real MCP client over stdin/stdout and expects the aggregated
// catalog to expose the namespaced demo tools.
func TestServeWithDemoEchoUpstream(t *testing.T) {
	bin := buildGateBinary(t)
	cfgPath := writeDoctorConfig(t, fmt.Sprintf(`
transport: stdio
log_level: error
upstreams:
  - name: demo-echo
    kind: stdio
    command: %s
    args: ["__demo-echo"]
    enabled: true
`, bin))

	cmd := exec.Command(bin, "serve", "-c", cfgPath)
	cmd.Stderr = os.Stderr // operational logs help diagnose a failing start
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	// Single owner of cmd.Wait (it must be called exactly once): the goroutine
	// reaps the process whether the test finishes normally or bails early.
	// done is buffered and closed after the send, so both the success path's
	// receive and the cleanup's later receive return without blocking.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		stdin.Close()      // idempotent; the normal path below closed it already
		cmd.Process.Kill() // no-op error if the process already exited
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("gateway process did not exit even after Kill")
		}
	})

	// Drive the session line-by-line, like a real MCP client.
	requests := []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"integration-test","version":"0"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
	}
	for _, req := range requests {
		if _, err := io.WriteString(stdin, req+"\n"); err != nil {
			t.Fatalf("write request %q: %v", req, err)
		}
	}

	r := mcp.NewReader(stdout)
	// Generous first deadline: it covers the gateway boot AND the demo-echo
	// child's own spawn + handshake.
	initResp := readResponse(t, r, "1", 15*time.Second)
	if initResp.Error != nil {
		t.Fatalf("initialize returned error: %v", initResp.Error)
	}

	listResp := readResponse(t, r, "2", 15*time.Second)
	if listResp.Error != nil {
		t.Fatalf("tools/list returned error: %v", listResp.Error)
	}
	var list mcp.ToolsListResult
	if err := json.Unmarshal(listResp.Result, &list); err != nil {
		t.Fatalf("unmarshal tools/list result %s: %v", listResp.Result, err)
	}
	got := make(map[string]bool, len(list.Tools))
	for _, tool := range list.Tools {
		got[tool.Name] = true
	}
	for _, want := range []string{"demo-echo__echo", "demo-echo__ping"} {
		if !got[want] {
			t.Errorf("aggregated catalog is missing %q (tools: %+v)", want, list.Tools)
		}
	}

	// Graceful shutdown: closing stdin is the client disconnect; the gateway
	// must exit cleanly on its own within the deadline.
	if err := stdin.Close(); err != nil {
		t.Fatalf("close stdin: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("gateway exited with error after client disconnect: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("gateway did not exit after stdin was closed")
	}
}
