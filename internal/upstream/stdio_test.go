package upstream_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
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

// startEnvProbe launches an `sh -c` wrapper that echoes selected environment
// variables to its stderr and exits, then returns the captured StderrTail as a
// single string. It is the shared harness for the S1 isolation tests: the
// gateway-side marker is placed via t.Setenv, the child's OWN env: is passed as
// env, and PATH is echoed to confirm the isolated base reached the child.
// All secret-shaped values are synthetic TEST-AIMCPGATE-FAKE-* markers.
func startEnvProbe(t *testing.T, env []string, isolate bool) string {
	t.Helper()
	requireTool(t, "sh")
	ctx := context.Background()
	// Echo the three probes to stderr, each on its own line, then exit.
	script := `echo "gw=$TEST_AIMCPGATE_GW_MARKER" 1>&2; ` +
		`echo "own=$TEST_AIMCPGATE_OWN" 1>&2; ` +
		`echo "path=$PATH" 1>&2; exit 0`
	conn, err := upstream.StartStdio(ctx, quietLogger(), "probe", "sh",
		[]string{"-c", script}, env, isolate, "0.0.0-test", nil, nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	// The child exits on its own; Close waits for the stderr drain so the tail
	// is complete afterwards.
	_ = conn.Close()
	tail, ok := conn.StderrTail()
	if !ok {
		t.Fatal("StderrTail ok = false; want true for a stdio connection")
	}
	return strings.Join(tail, "\n")
}

// U1. TestStartStdioDefaultInheritsGatewayEnv (regression pin): with
// isolate=false and env=nil, the child inherits the gateway's FULL environment
// — today's behaviour. Must stay green without edits.
func TestStartStdioDefaultInheritsGatewayEnv(t *testing.T) {
	const marker = "TEST-AIMCPGATE-FAKE-SECRET-GW-INHERIT-0001"
	t.Setenv("TEST_AIMCPGATE_GW_MARKER", marker)
	out := startEnvProbe(t, nil, false)
	if !strings.Contains(out, "gw="+marker) {
		t.Errorf("default (isolate=false, env=nil) must inherit gateway env; stderr =\n%s", out)
	}
}

// U2. TestStartStdioDefaultAppendsEnvToInherited (pins the len(env)>0 branch):
// isolate=false with an OWN env entry — both the inherited gateway marker AND
// the child's own variable are visible.
func TestStartStdioDefaultAppendsEnvToInherited(t *testing.T) {
	const marker = "TEST-AIMCPGATE-FAKE-SECRET-GW-APPEND-0002"
	const own = "TEST-AIMCPGATE-FAKE-OWN-APPEND-0003"
	t.Setenv("TEST_AIMCPGATE_GW_MARKER", marker)
	out := startEnvProbe(t, []string{"TEST_AIMCPGATE_OWN=" + own}, false)
	if !strings.Contains(out, "gw="+marker) {
		t.Errorf("default with env must still inherit gateway env; stderr =\n%s", out)
	}
	if !strings.Contains(out, "own="+own) {
		t.Errorf("default with env must expose the child's OWN var; stderr =\n%s", out)
	}
}

// U3. TestStartStdioIsolateHidesGatewayEnv: isolate=true with an OWN env entry —
// the gateway marker is HIDDEN, the child's OWN var is present, and PATH (part
// of the isolated base) reached the child non-empty.
func TestStartStdioIsolateHidesGatewayEnv(t *testing.T) {
	const marker = "TEST-AIMCPGATE-FAKE-SECRET-GW-ISOLATE-0004"
	const own = "TEST-AIMCPGATE-FAKE-OWN-ISOLATE-0005"
	t.Setenv("TEST_AIMCPGATE_GW_MARKER", marker)
	out := startEnvProbe(t, []string{"TEST_AIMCPGATE_OWN=" + own}, true)
	if strings.Contains(out, marker) {
		t.Errorf("isolate=true must NOT leak the gateway marker; stderr =\n%s", out)
	}
	if !strings.Contains(out, "own="+own) {
		t.Errorf("isolate=true must still expose the child's OWN env; stderr =\n%s", out)
	}
	if strings.Contains(out, "path=\n") || strings.Contains(out, "path=") && !hasNonEmptyPath(out) {
		t.Errorf("isolate=true must carry a non-empty PATH from the base; stderr =\n%s", out)
	}
}

// hasNonEmptyPath reports whether the probe's "path=" line has a non-empty value.
func hasNonEmptyPath(out string) bool {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "path=") {
			return len(strings.TrimPrefix(line, "path=")) > 0
		}
	}
	return false
}

func TestStdioConnHandshakeAndCatalog(t *testing.T) {
	bin := buildFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := upstream.StartStdio(ctx, quietLogger(), "github", bin, nil,
		[]string{"FAKE_NAME=github", "FAKE_TOOLS=search,create_issue"}, false, "0.0.0-test", nil, nil)
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
		[]string{"FAKE_TOOLS=fetch", "FAKE_ECHO=1"}, false, "0.0.0-test", nil, nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	args := json.RawMessage(`{"url":"https://example.com"}`)
	resp, err := conn.CallTool(ctx, "fetch", args, nil)
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
		[]string{"FAKE_TOOLS=t", "FAKE_ECHO=1"}, false, "0.0.0-test", nil, nil)
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
			out, err := conn.CallTool(ctx, "t", args, nil)
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
	_, err := upstream.StartStdio(ctx, quietLogger(), "x", "definitely-not-a-real-binary-xyz", nil, nil, false, "0.0.0-test", nil, nil)
	if err == nil {
		t.Fatal("expected error for missing command")
	}
}

func TestStdioConnCloseWakesPendingCall(t *testing.T) {
	// If the child exits mid-flight, a pending Call must return ErrConnClosed,
	// not hang.
	bin := buildFakeServer(t)
	ctx := context.Background()
	conn, err := upstream.StartStdio(ctx, quietLogger(), "z", bin, nil, []string{"FAKE_TOOLS=t"}, false, "0.0.0-test", nil, nil)
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
		_, err := conn.CallTool(ctx, "t", nil, nil)
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
	}, false, "0.0.0-test", nil, nil)
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
	conn, err := upstream.StartStdio(ctx, quietLogger(), "z", bin, nil, []string{"FAKE_TOOLS=t"}, false, "0.0.0-test", nil, nil)
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
		[]string{"FAKE_TOOLS=t", "FAKE_NOTIFY_ON_START=1"}, false, "0.0.0-test",
		func(method string, _ json.RawMessage) { notified <- method }, nil)
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

// TestStdioCallCancellationNotifiesUpstream (Round 2): when a call's ctx is
// cancelled AFTER the request reached the upstream, the transport must send a
// best-effort notifications/cancelled carrying the UPSTREAM-SIDE id the call
// minted itself — never any client-side id (the gateway's id spaces are fully
// separated). FAKE_ASYNC_CALLS keeps the fake server's read loop consuming
// stdin while the delayed call is pending, so it can record the cancellation
// (FAKE_CANCEL_FILE) the moment it arrives instead of after the delay.
func TestStdioCallCancellationNotifiesUpstream(t *testing.T) {
	bin := buildFakeServer(t)
	cancelFile := filepath.Join(t.TempDir(), "cancelled.jsonl")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := upstream.StartStdio(ctx, quietLogger(), "slow", bin, nil,
		[]string{
			"FAKE_TOOLS=t",
			"FAKE_CALL_DELAY=30s",
			"FAKE_ASYNC_CALLS=1",
			"FAKE_CANCEL_FILE=" + cancelFile,
		}, false, "0.0.0-test", nil, nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Initialize(ctx); err != nil { // upstream-side id 1
		t.Fatalf("Initialize: %v", err)
	}

	callCtx, callCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer callCancel()
	_, err = conn.CallTool(callCtx, "t", nil, nil) // upstream-side id 2
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CallTool err = %v, want context.DeadlineExceeded", err)
	}

	// The cancelled-notify is fire-and-forget on its own goroutine — poll for
	// the fake server's record of it.
	deadline := time.Now().Add(5 * time.Second)
	var line string
	for {
		data, _ := os.ReadFile(cancelFile)
		if line = strings.TrimSpace(string(data)); line != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("upstream never received notifications/cancelled after ctx cancellation")
		}
		time.Sleep(20 * time.Millisecond)
	}
	var p struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if err := json.Unmarshal([]byte(line), &p); err != nil {
		t.Fatalf("decode recorded cancelled params %q: %v", line, err)
	}
	// Initialize minted id 1, the call minted id 2 — the notification must
	// carry exactly the call's own upstream-side id.
	if string(p.RequestID) != "2" {
		t.Errorf("cancelled requestId = %s, want the upstream-side call id 2", p.RequestID)
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
		[]string{"-c", "sleep 30 & exec cat"}, nil, false, "0.0.0-test", nil, nil)
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
		[]string{"30"}, nil, false, "0.0.0-test", nil, nil)
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
	_, err = conn.CallTool(callCtx, "t", big, nil)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CallTool err=%v, want context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("CallTool took %v to honor its context; a blocked stdin write must not delay it", elapsed)
	}
}

// TestStdioConnHybridResponseNotDelivered checks the readLoop side of the
// shared IsMalformedHybrid predicate: an upstream that answers a call with a
// MALFORMED hybrid message (method AND result together, echoing the request
// id) used to have it classified as a valid response by IsResponse() and
// delivered to the waiter as a successful answer — diverging from the client
// dispatcher and the demo stub, which reject the same shape as invalid. The
// reader must now drop it, so the pending call sees no answer at all and
// times out on its own ctx, exactly as if the upstream had stayed silent.
func TestStdioConnHybridResponseNotDelivered(t *testing.T) {
	bin := buildFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := upstream.StartStdio(ctx, quietLogger(), "hybrid", bin, nil,
		[]string{"FAKE_TOOLS=t", "FAKE_HYBRID_CALL=1"}, false, "0.0.0-test", nil, nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	callCtx, callCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer callCancel()
	resp, err := conn.CallTool(callCtx, "t", nil, nil)
	if err == nil {
		t.Fatalf("CallTool delivered the malformed hybrid as a valid response: %+v", resp)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CallTool err=%v, want context.DeadlineExceeded (dropped hybrid → ctx timeout)", err)
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
	conn, err := upstream.StartStdio(ctx, quietLogger(), "c", "cat", nil, nil, false, "0.0.0-test", nil, nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = conn.CallTool(ctx, "t", nil, nil)
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

// TestStderrTailCapturedWithDebugOff: the stderr ring must fill REGARDLESS of
// log level — that is its whole point (the crash tail must exist in production,
// where debug logging is off). The fake server writes its stderr lines as
// stdin closes; Close waits for the drain, so afterwards StderrTail must hold
// them even though the logger here is at the default (info) level.
func TestStderrTailCapturedWithDebugOff(t *testing.T) {
	bin := buildFakeServer(t)
	ctx := context.Background()

	conn, err := upstream.StartStdio(ctx, quietLogger(), "tailed", bin, nil, []string{
		"FAKE_TOOLS=t",
		"FAKE_STDERR_LINES=3",
	}, false, "0.0.0-test", nil, nil)
	if err != nil {
		t.Fatalf("StartStdio: %v", err)
	}
	if _, err := conn.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tail, ok := conn.StderrTail()
	if !ok {
		t.Fatal("StderrTail ok = false for a stdio connection; want true")
	}
	want := []string{"shutdown line 0", "shutdown line 1", "shutdown line 2"}
	if len(tail) != len(want) {
		t.Fatalf("StderrTail returned %d lines %q, want %d", len(tail), tail, len(want))
	}
	for i, w := range want {
		if tail[i] != w {
			t.Errorf("tail[%d] = %q, want %q", i, tail[i], w)
		}
	}
}
