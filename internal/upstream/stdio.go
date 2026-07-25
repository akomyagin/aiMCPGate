// Package upstream implements connections to individual upstream MCP servers.
//
// The public surface is Conn (protocol.go): one type for every transport,
// wrapping the private transport interface. This file provides stdioTransport:
// an upstream MCP server launched as a child process (os/exec) and spoken to
// over its stdin/stdout with JSON-RPC 2.0. A single reader goroutine
// demultiplexes responses by JSON-RPC id and delivers them to the goroutine
// that issued the matching call.
package upstream

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// ErrConnClosed is returned by call once the connection's reader has stopped
// (child exited or Close was called).
var ErrConnClosed = errors.New("upstream: connection closed")

// ErrConnClosedBeforeSend is the variant of ErrConnClosed for the paths where
// the request is GUARANTEED not to have been sent to the upstream (call bailed
// out on the closed-connection check before ever writing). Callers can match
// it with errors.Is against either sentinel — errors.Is(err, ErrConnClosed)
// is also true — and use the distinction to retry safely: a request that was
// never sent cannot have had side effects upstream.
var ErrConnClosedBeforeSend = fmt.Errorf("%w: request was not sent to upstream", ErrConnClosed)

// closeGracePeriod bounds how long Close waits for a well-behaved upstream to
// exit after its stdin is closed, before force-killing it. A misbehaving
// upstream that keeps stdout open must not hang gateway shutdown forever.
const closeGracePeriod = 5 * time.Second

// killWaitTimeout bounds how long Close waits, AFTER force-killing the child,
// for the reader goroutines to see EOF on stdout/stderr. Kill only reaches the
// direct child: a grandchild (e.g. `sh -c 'helper & exec server'`) may have
// inherited the pipes and keep them open indefinitely, so past this timeout
// Close force-closes its own read ends instead of waiting for EOF forever.
const killWaitTimeout = 2 * time.Second

// stdioTransport is the transport half of a connection to one stdio upstream
// MCP server. Protocol logic (Initialize etc.) lives on Conn (protocol.go).
//
// Concurrency model (docs/TECHNICAL_PLAN.md §4.1, SKILL §4):
//   - writes to the child's stdin are serialized by mcp.Writer's mutex;
//   - one reader goroutine reads the child's stdout line by line and routes each
//     response to a waiter channel keyed by the gateway-side id;
//   - call mints a fresh upstream-side id from an atomic counter, so the
//     gateway's id space is fully separated from any client id space.
type stdioTransport struct {
	name string
	log  *slog.Logger

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	w *mcp.Writer

	nextID atomic.Int64

	mu      sync.Mutex
	waiters map[string]chan *mcp.Message
	closed  bool

	done       chan struct{} // closed when the reader goroutine exits
	stderrDone chan struct{} // closed when the stderr-draining goroutine exits

	// closeOnce guards the actual teardown so Close is safe for CONCURRENT
	// calls, not just repeated sequential ones: Stage 7 introduced the first
	// callers that can race to close the same connection (the auto-restart
	// supervisor reaping a crashed process vs. hot-reload retiring the same
	// upstream). cmd.Wait must not be called twice concurrently — it mutates
	// *exec.Cmd's internal state without synchronization for that case — so
	// every caller after the first must get the same cached result instead of
	// re-running the teardown (found by independent review).
	closeOnce sync.Once
	closeErr  error

	// onNotify, when set, is invoked by readLoop for each notification method
	// received from the upstream (e.g. notifications/tools/list_changed). The
	// registry sets it to react to a catalog change (Stage 7b). It is set once,
	// inside StartStdio, before the reader goroutine starts — hence no lock
	// needed. (It used to be set post-factum via a setter, which raced an
	// upstream notifying immediately on startup — found by independent review.)
	onNotify func(method string)
}

// Name returns the upstream's stable identifier.
func (c *stdioTransport) Name() string { return c.name }

// Done reports a channel closed when this connection's reader goroutine exits —
// i.e. when the child process has died (its stdout reached EOF) or Close was
// called. The registry's per-upstream supervisor selects on it to detect a
// crashed stdio upstream and trigger an auto-restart (Stage 7a). ok is always
// true here: a stdio upstream always has a long-lived process to watch.
func (c *stdioTransport) Done() (<-chan struct{}, bool) { return c.done, true }

// StartStdio launches command with args/env as a child process and starts the
// reader goroutine. It does NOT perform the MCP handshake — call Initialize.
//
// ctx is bound to the process via exec.CommandContext, so cancelling ctx (e.g.
// on Ctrl-C) terminates the child. env entries are "KEY=VALUE"; they are
// appended to the current environment. gatewayVersion is the gateway's build
// version, reported to the upstream as clientInfo.version in the handshake.
//
// onNotify, when non-nil, is invoked (from the reader goroutine) for each
// notification the upstream sends. It must be passed here — not installed
// after the fact — because the reader goroutine starts before StartStdio
// returns, and an upstream may notify immediately; the field is written into
// the struct literal before that goroutine exists, so no lock is needed. The
// callback must not block or call back into the connection synchronously
// (Stage 7b).
func StartStdio(ctx context.Context, log *slog.Logger, name, command string, args, env []string, gatewayVersion string, onNotify func(method string)) (*Conn, error) {
	if _, err := exec.LookPath(command); err != nil {
		return nil, fmt.Errorf("upstream %q: command %q not found: %w", name, command, err)
	}

	cmd := exec.CommandContext(ctx, command, args...)
	if len(env) > 0 {
		cmd.Env = append(cmd.Environ(), env...)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("upstream %q: stdin pipe: %w", name, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("upstream %q: stdout pipe: %w", name, err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("upstream %q: stderr pipe: %w", name, err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("upstream %q: start: %w", name, err)
	}

	t := &stdioTransport{
		name:       name,
		log:        log,
		cmd:        cmd,
		stdin:      stdin,
		stdout:     stdout,
		stderr:     stderr,
		w:          mcp.NewWriter(stdin),
		waiters:    make(map[string]chan *mcp.Message),
		done:       make(chan struct{}),
		stderrDone: make(chan struct{}),
		onNotify:   onNotify, // must be set before go t.readLoop() below
	}

	go t.readLoop()
	go t.drainStderr()

	return &Conn{transport: t, gatewayVersion: gatewayVersion}, nil
}

// readLoop reads framed messages from the child's stdout and routes each
// response to its waiter. It runs until stdout closes (child exit), then fails
// all outstanding waiters.
func (c *stdioTransport) readLoop() {
	defer close(c.done)
	r := mcp.NewReader(c.stdout)
	for {
		msg, err := r.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				c.log.Debug("upstream read stopped", "upstream", c.name, "err", err)
			}
			c.failAll()
			return
		}
		switch {
		case msg.IsResponse():
			c.deliver(msg)
		case msg.IsNotification():
			c.log.Debug("upstream notification", "upstream", c.name, "method", msg.Method)
			// Deliver to the registry's handler (Stage 7b) so it can react to a
			// catalog change (tools/list_changed). The callback must be cheap and
			// non-blocking — it runs on this single reader goroutine — so the
			// registry only kicks a debounce timer here, never re-lists inline.
			if c.onNotify != nil {
				c.onNotify(msg.Method)
			}
		default:
			// A request FROM an upstream (e.g. sampling) — not handled in MVP.
			c.log.Debug("upstream request ignored", "upstream", c.name, "method", msg.Method)
		}
	}
}

// drainStderr forwards the child's stderr to the operational log line by line,
// prefixed with the upstream name. Never logged as protocol data.
//
// Close waits for stderrDone before calling cmd.Wait(): exec.Cmd's own docs
// warn that Wait closes any pipe obtained via StderrPipe once it sees the
// child exit, so reading from that pipe concurrently with (or after) Wait is
// a race — "it is thus incorrect to call Wait before all reads from the pipe
// have completed" (found by code review; done alone only tracked stdout).
func (c *stdioTransport) drainStderr() {
	defer close(c.stderrDone)
	// With debug logging off, every scanned line would allocate (sc.Text) and
	// box its slog arguments only for the handler to discard them immediately.
	// Check the level ONCE and, when debug is disabled, keep the pipe drained
	// without any per-line work — the child must never block on a full 64KiB
	// OS pipe buffer either way. context.Background() because slog.Logger.
	// Enabled only consults the handler's level; there is no per-call context
	// on this goroutine.
	if !c.log.Enabled(context.Background(), slog.LevelDebug) {
		_, _ = io.Copy(io.Discard, c.stderr)
		return
	}
	sc := bufio.NewScanner(c.stderr)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		c.log.Debug("upstream stderr", "upstream", c.name, "line", sc.Text())
	}
	if err := sc.Err(); err != nil {
		// The scanner gave up (e.g. a single line exceeded its 1MiB limit) but
		// the pipe is still open. If nobody keeps reading, the child blocks the
		// moment the 64KiB OS pipe buffer fills — so switch to a raw drain and
		// discard everything until EOF. Log once, not per byte.
		c.log.Warn("upstream stderr scan failed (line too long?), switching to raw drain",
			"upstream", c.name, "err", err)
		_, _ = io.Copy(io.Discard, c.stderr)
	}
}

// deliver routes a response to its waiter (if any) by its id.
func (c *stdioTransport) deliver(msg *mcp.Message) {
	key := string(msg.ID)
	c.mu.Lock()
	ch, ok := c.waiters[key]
	if ok {
		delete(c.waiters, key)
	}
	c.mu.Unlock()
	if !ok {
		c.log.Debug("upstream response with no waiter", "upstream", c.name, "id", key)
		return
	}
	ch <- msg
}

// failAll closes out all pending waiters when the connection dies.
func (c *stdioTransport) failAll() {
	c.mu.Lock()
	c.closed = true
	waiters := c.waiters
	c.waiters = map[string]chan *mcp.Message{}
	c.mu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

// call sends a request with a fresh upstream-side id and waits for its response
// or ctx cancellation. The returned *mcp.Message is the raw response (which may
// carry an Error); a nil error means a response was received.
func (c *stdioTransport) call(ctx context.Context, method string, params json.RawMessage) (*mcp.Message, error) {
	id := mcp.IntID(c.nextID.Add(1))
	key := string(id)
	ch := make(chan *mcp.Message, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		// Bailing out here means the request was never written — report that
		// guarantee via the "before send" sentinel so callers may retry safely.
		return nil, ErrConnClosedBeforeSend
	}
	c.waiters[key] = ch
	c.mu.Unlock()

	req := mcp.NewRequest(id, method, params)
	// The write can block indefinitely if the upstream stops reading its stdin
	// (a stuck/SIGSTOPped child lets the 64KiB OS pipe buffer fill up), so run
	// it in a goroutine and honor ctx here too instead of only after the write.
	// The channel is buffered: an abandoned write goroutine can always deliver
	// its result and exit instead of leaking.
	//
	// An abandoned goroutine may also still be inside c.w.Write when
	// closeAndWait closes stdin from another goroutine. That is safe by
	// design, not by luck: since Go 1.9 the os package registers pipe FDs
	// with the runtime poller, and os.File.Close evicts the FD from it
	// (internal/poll.FD.Close → pd.evict()), failing any pending Write with
	// a clean "file already closed" error — no undefined behavior, no data
	// corruption. The goroutine then delivers that error into its buffered
	// channel (which nobody reads) and exits.
	writeErr := make(chan error, 1)
	go func() { writeErr <- c.w.Write(req) }()
	select {
	case <-ctx.Done():
		// Unlike the c.closed path above, the write is still in flight in the
		// background — we cannot know whether the request physically reached
		// the upstream, so no "not sent" guarantee: plain ctx.Err() only.
		c.mu.Lock()
		delete(c.waiters, key)
		c.mu.Unlock()
		return nil, ctx.Err()
	case err := <-writeErr:
		if err != nil {
			c.mu.Lock()
			delete(c.waiters, key)
			c.mu.Unlock()
			return nil, fmt.Errorf("upstream %q: write %s: %w", c.name, method, err)
		}
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.waiters, key)
		c.mu.Unlock()
		return nil, ctx.Err()
	case msg, ok := <-ch:
		if !ok {
			return nil, ErrConnClosed
		}
		return msg, nil
	}
}

// notify sends a one-way notification (no id, no response expected). ctx is
// unused on the stdio side — the write is a local pipe write — and exists only
// so the signature matches httpTransport.notify for the shared transport interface.
func (c *stdioTransport) notify(_ context.Context, method string, params json.RawMessage) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrConnClosed
	}
	return c.w.Write(mcp.NewNotification(method, params))
}

// Close closes the child's stdin (signalling shutdown per the MCP stdio
// lifecycle) and waits for it to exit. Safe to call more than once, including
// CONCURRENTLY from multiple goroutines: the actual teardown runs exactly
// once (closeOnce); every caller gets the same result.
func (c *stdioTransport) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.closeAndWait()
	})
	return c.closeErr
}

// closeAndWait is Close's one-time body.
func (c *stdioTransport) closeAndWait() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	// Closing stdin tells a well-behaved server to exit; the context bound via
	// CommandContext kills it otherwise. Either way, wait for the reader
	// goroutine to see the child exit — but not forever: a misbehaving upstream
	// that keeps stdout open despite stdin closing must not hang shutdown.
	_ = c.stdin.Close()
	select {
	case <-c.done:
	case <-time.After(closeGracePeriod):
		c.log.Warn("upstream did not exit after stdin close, killing", "upstream", c.name)
		_ = c.cmd.Process.Kill()
		// Kill reaches only the DIRECT child. A grandchild (e.g. spawned via
		// `sh -c 'helper & exec server'`) may have inherited the write ends of
		// stdout/stderr and keep them open, so readLoop never sees EOF and
		// c.done never closes. Wait a bounded time, then force-close our read
		// end: that fails readLoop's blocked Read and unblocks it.
		select {
		case <-c.done:
		case <-time.After(killWaitTimeout):
			c.log.Warn("upstream still holding stdout after kill, closing it directly — likely a grandchild process",
				"upstream", c.name)
			_ = c.stdout.Close() // cmd.Wait would close it later anyway; double close is harmless
			<-c.done
		}
	}
	// See drainStderr: it must finish before Wait touches the stderr pipe. The
	// same grandchild pathology applies, so give it the same bounded wait and
	// force-close the read end if it is still blocked.
	select {
	case <-c.stderrDone:
	case <-time.After(killWaitTimeout):
		c.log.Warn("upstream gone but stderr still open, closing it directly — likely a grandchild process",
			"upstream", c.name)
		_ = c.stderr.Close() // cmd.Wait would close it later anyway; double close is harmless
		<-c.stderrDone
	}
	err := c.cmd.Wait()
	// A killed/closed child commonly returns a non-nil error during shutdown:
	// a plain *exec.ExitError (non-zero exit / killed by our own timeout-Kill
	// above), or — because the child is launched under a context via
	// exec.CommandContext (see StartStdio) — a context.Canceled/
	// DeadlineExceeded wrapped error if that context's own cancellation is
	// what ended the process. Both are the expected, benign shape of "the
	// process is gone because we told it (or its context) to stop", not a
	// real failure to report.
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}
