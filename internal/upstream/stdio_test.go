package upstream_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/upstream"
)

// buildFakeServer compiles internal/upstream/testdata/fakeserver into a temp
// binary once per test and returns its path.
func buildFakeServer(t *testing.T) string {
	t.Helper()
	src := filepath.Join("testdata", "fakeserver")
	bin := filepath.Join(t.TempDir(), "fakeserver")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = src
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fakeserver: %v\n%s", err, out)
	}
	return bin
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestStdioConnHandshakeAndCatalog(t *testing.T) {
	bin := buildFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := upstream.StartStdio(ctx, quietLogger(), "github", bin, nil,
		[]string{"FAKE_NAME=github", "FAKE_TOOLS=search,create_issue"})
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	defer conn.Close()

	info, err := conn.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if info.ServerInfo.Name != "github" {
		t.Errorf("serverInfo.name=%q want github", info.ServerInfo.Name)
	}

	tools, err := conn.ListTools(ctx)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools) != 2 || tools[0].Name != "search" || tools[1].Name != "create_issue" {
		t.Fatalf("unexpected tools: %+v", tools)
	}
}

func TestStdioConnCallToolEcho(t *testing.T) {
	bin := buildFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := upstream.StartStdio(ctx, quietLogger(), "web", bin, nil,
		[]string{"FAKE_TOOLS=fetch", "FAKE_ECHO=1"})
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	args := json.RawMessage(`{"url":"https://example.com"}`)
	resp, err := conn.CallTool(ctx, "fetch", args)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected rpc error: %v", resp.Error)
	}
	// FAKE_ECHO makes the tool echo the arguments back inside the text content.
	if want := "example.com"; !containsRaw(resp.Result, want) {
		t.Fatalf("result %s does not echo %q", resp.Result, want)
	}
}

// TestStdioConnConcurrentCallsDemux fires many calls concurrently against one
// connection and checks each gets its own correct response — exercising id-based
// demultiplexing and serialized writes under -race.
func TestStdioConnConcurrentCallsDemux(t *testing.T) {
	bin := buildFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := upstream.StartStdio(ctx, quietLogger(), "fs", bin, nil,
		[]string{"FAKE_TOOLS=t", "FAKE_ECHO=1"})
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	const n = 40
	type res struct {
		i   int
		out *mcp.Message
		err error
	}
	results := make(chan res, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			args := json.RawMessage(`{"seq":` + itoa(i) + `}`)
			out, err := conn.CallTool(ctx, "t", args)
			results <- res{i, out, err}
		}(i)
	}

	seen := make(map[int]bool)
	for k := 0; k < n; k++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("call %d: %v", r.i, r.err)
		}
		// Each echo must carry this call's own seq. The arguments are echoed as
		// a JSON string inside the text content, so quotes are backslash-escaped;
		// match the full escaped object to avoid "seq":1 matching "seq":10.
		if !containsRaw(r.out.Result, `{\"seq\":`+itoa(r.i)+`}`) {
			t.Fatalf("call %d got mismatched response %s", r.i, r.out.Result)
		}
		seen[r.i] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d distinct responses want %d", len(seen), n)
	}
}

func TestStdioConnMissingCommand(t *testing.T) {
	ctx := context.Background()
	_, err := upstream.StartStdio(ctx, quietLogger(), "x", "definitely-not-a-real-binary-xyz", nil, nil)
	if err == nil {
		t.Fatal("expected error for missing command")
	}
}

func TestStdioConnCloseWakesPendingCall(t *testing.T) {
	// If the child exits mid-flight, a pending Call must return ErrConnClosed,
	// not hang.
	bin := buildFakeServer(t)
	ctx := context.Background()
	conn, err := upstream.StartStdio(ctx, quietLogger(), "z", bin, nil, []string{"FAKE_TOOLS=t"})
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A subsequent call must fail promptly.
	done := make(chan error, 1)
	go func() {
		_, err := conn.CallTool(ctx, "t", nil)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call did not return after Close")
	}
}

func containsRaw(raw json.RawMessage, sub string) bool {
	return len(raw) > 0 && contains(string(raw), sub)
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
