package upstream_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/upstream"
)

// requireTool skips the test when a POSIX tool it depends on is not in PATH
// (e.g. on Windows).
func requireTool(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not available: %v", name, err)
	}
}

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
		[]string{"FAKE_NAME=github", "FAKE_TOOLS=search,create_issue"}, "0.0.0-test", nil)
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
		[]string{"FAKE_TOOLS=fetch", "FAKE_ECHO=1"}, "0.0.0-test", nil)
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
		[]string{"FAKE_TOOLS=t", "FAKE_ECHO=1"}, "0.0.0-test", nil)
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
	_, err := upstream.StartStdio(ctx, quietLogger(), "x", "definitely-not-a-real-binary-xyz", nil, nil, "0.0.0-test", nil)
	if err == nil {
		t.Fatal("expected error for missing command")
	}
}

func TestStdioConnCloseWakesPendingCall(t *testing.T) {
	// If the child exits mid-flight, a pending Call must return ErrConnClosed,
	// not hang.
	bin := buildFakeServer(t)
	ctx := context.Background()
	conn, err := upstream.StartStdio(ctx, quietLogger(), "z", bin, nil, []string{"FAKE_TOOLS=t"}, "0.0.0-test", nil)
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

// TestStdioConnCloseWaitsForStderrDrain is a regression test: Close used to
// wait only for the stdout reader (done) before calling cmd.Wait(), racing
// against the still-running stderr-draining goroutine — exec.Cmd's own docs
// warn that Wait closes a StderrPipe once it sees the child exit, so reading
// concurrently with (or after) Wait is undefined (found by code review). The
// fake server writes its stderr lines in the same window Close races
// against: right as it sees stdin close (shutdown), just before exiting.
func TestStdioConnCloseWaitsForStderrDrain(t *testing.T) {
	bin := buildFakeServer(t)
	ctx := context.Background()

	const lines = 200
	var logBuf strings.Builder
	logger := slog.New(slog.NewTextHandler(&stringWriter{&logBuf}, &slog.HandlerOptions{Level: slog.LevelDebug}))

	conn, err := upstream.StartStdio(ctx, logger, "z", bin, nil, []string{
		"FAKE_TOOLS=t",
		"FAKE_STDERR_LINES=" + strconv.Itoa(lines),
	}, "0.0.0-test", nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got := strings.Count(logBuf.String(), "upstream stderr")
	if got != lines {
		t.Errorf("captured %d of %d stderr lines by the time Close returned; "+
			"Close must wait for stderr to fully drain before reaping the process", got, lines)
	}
}

// TestStdioConnCloseIsSafeForConcurrentCallers is a regression test: Stage 7
// introduced the first callers that can race to Close the SAME connection
// (the auto-restart supervisor reaping a crash vs. hot-reload retiring the
// same upstream). cmd.Wait mutates *exec.Cmd's internal state and is not
// documented safe to call concurrently from two goroutines, so every caller
// after the first must observe the SAME result rather than re-running the
// teardown (found by independent review). Run with -race: without the
// closeOnce guard this reliably races on cmd.Wait's internal state.
func TestStdioConnCloseIsSafeForConcurrentCallers(t *testing.T) {
	bin := buildFakeServer(t)
	ctx := context.Background()
	conn, err := upstream.StartStdio(ctx, quietLogger(), "z", bin, nil, []string{"FAKE_TOOLS=t"}, "0.0.0-test", nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	const callers = 10
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = conn.Close()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != errs[0] {
			t.Errorf("Close() call %d returned %v, want the same cached result as call 0 (%v)", i, err, errs[0])
		}
	}
}

// TestStdioNotifyOnStartNoRace is a regression test for the data race found by
// independent review after Stage 7: StartStdio used to start the reader
// goroutine and only AFTERWARDS did the registry install the notification
// callback via a setter — an upstream that notifies the instant it starts
// raced readLoop's read of c.onNotify against that late write. The callback is
// now a StartStdio parameter, written into the struct before the reader
// goroutine exists. FAKE_NOTIFY_ON_START makes the fake server emit a
// tools/list_changed before reading any request, hitting exactly that window.
// Run with -race: the test must be race-clean AND the callback must fire.
func TestStdioNotifyOnStartNoRace(t *testing.T) {
	bin := buildFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	notified := make(chan string, 4)
	conn, err := upstream.StartStdio(ctx, quietLogger(), "eager", bin, nil,
		[]string{"FAKE_TOOLS=t", "FAKE_NOTIFY_ON_START=1"}, "0.0.0-test",
		func(method string) { notified <- method })
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	defer conn.Close()

	select {
	case method := <-notified:
		if method != "notifications/tools/list_changed" {
			t.Errorf("callback got method %q, want notifications/tools/list_changed", method)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("notification callback was not invoked for the startup list_changed")
	}
}

// TestCloseDoesNotHangOnGrandchildHoldingPipes is a regression test for
// finding #8: after the grace-period Kill, Close used to wait for the reader
// goroutines with NO timeout. Process.Kill reaches only the DIRECT child —
// here `cat` (exec'd by sh) dies the moment stdin closes, but the
// backgrounded `sleep 30` grandchild inherits the stdout/stderr write ends
// and keeps them open, so readLoop never saw EOF and Close hung until the
// grandchild exited (30s here; forever for a daemon). The fix bounds each
// post-kill wait with killWaitTimeout and force-closes the gateway's read
// ends, so Close must return in closeGracePeriod + 2*killWaitTimeout (~9s).
func TestCloseDoesNotHangOnGrandchildHoldingPipes(t *testing.T) {
	requireTool(t, "sh")
	ctx := context.Background()
	conn, err := upstream.StartStdio(ctx, quietLogger(), "wrap", "sh",
		[]string{"-c", "sleep 30 & exec cat"}, nil, "0.0.0-test", nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- conn.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(15 * time.Second): // 9s of designed timeouts + CI slack, well below the 30s sleep
		t.Fatal("Close hung on a grandchild holding the pipes; want a bounded return (grace period + kill waits)")
	}
}

// TestCallUnblocksOnContextWhenStdinBlocked is a regression test for finding
// #9: call used to issue a plain blocking Write to the child's stdin and check
// ctx only afterwards. An upstream that never reads stdin (`sleep 30` here;
// SIGSTOPped/deadlocked in real life) lets the 64KiB OS pipe buffer fill, so
// a large-enough request blocked the write — and with it this call and every
// later call queued on the writer mutex — until the child died.
func TestCallUnblocksOnContextWhenStdinBlocked(t *testing.T) {
	requireTool(t, "sleep")
	ctx, cancel := context.WithCancel(context.Background())
	// sleep is launched DIRECTLY (no sh wrapper) so the cleanup's cancel can
	// kill it as the immediate child and Close returns without grace periods.
	conn, err := upstream.StartStdio(ctx, quietLogger(), "stuck", "sleep",
		[]string{"30"}, nil, "0.0.0-test", nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	t.Cleanup(func() {
		cancel() // kill the child via CommandContext so Close returns promptly
		_ = conn.Close()
	})

	// ~1MiB of arguments — far beyond the pipe buffer, so the write MUST block.
	big := json.RawMessage(`{"data":"` + strings.Repeat("x", 1<<20) + `"}`)
	callCtx, callCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer callCancel()

	start := time.Now()
	_, err = conn.CallTool(callCtx, "t", big)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CallTool err=%v, want context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("CallTool took %v to honor its context; a blocked stdin write must not delay it", elapsed)
	}
}

// TestCallOnClosedConnReturnsBeforeSendSentinel checks the failure-point
// distinction introduced with finding #9: when call bails out on the
// closed-connection check, the request is GUARANTEED not to have been sent,
// and the error must say so — matching BOTH ErrConnClosedBeforeSend and the
// plain ErrConnClosed it wraps (the safe-to-retry signal for finding #3).
func TestCallOnClosedConnReturnsBeforeSendSentinel(t *testing.T) {
	requireTool(t, "cat")
	ctx := context.Background()
	conn, err := upstream.StartStdio(ctx, quietLogger(), "c", "cat", nil, nil, "0.0.0-test", nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = conn.CallTool(ctx, "t", nil)
	if !errors.Is(err, upstream.ErrConnClosedBeforeSend) {
		t.Errorf("err=%v, want errors.Is(err, ErrConnClosedBeforeSend)", err)
	}
	if !errors.Is(err, upstream.ErrConnClosed) {
		t.Errorf("err=%v, want errors.Is(err, ErrConnClosed)", err)
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
