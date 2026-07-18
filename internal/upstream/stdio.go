// Package upstream implements connections to individual upstream MCP servers.
//
// Stage 1 provides StdioConn: an upstream MCP server launched as a child process
// (os/exec) and spoken to over its stdin/stdout with JSON-RPC 2.0. A single
// reader goroutine demultiplexes responses by JSON-RPC id and delivers them to
// the goroutine that issued the matching Call.
//
// The upstream.Conn interface is intentionally NOT introduced yet — that lands
// with the second implementation (httpConn, Phase 2), per the project rule
// "interface on the second implementation".
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

// ErrConnClosed is returned by Call once the connection's reader has stopped
// (child exited or Close was called).
var ErrConnClosed = errors.New("upstream: connection closed")

// closeGracePeriod bounds how long Close waits for a well-behaved upstream to
// exit after its stdin is closed, before force-killing it. A misbehaving
// upstream that keeps stdout open must not hang gateway shutdown forever.
const closeGracePeriod = 5 * time.Second

// StdioConn is a live connection to one stdio upstream MCP server.
//
// Concurrency model (docs/TECHNICAL_PLAN.md §4.1, SKILL §4):
//   - writes to the child's stdin are serialized by mcp.Writer's mutex;
//   - one reader goroutine reads the child's stdout line by line and routes each
//     response to a waiter channel keyed by the gateway-side id;
//   - Call mints a fresh upstream-side id from an atomic counter, so the
//     gateway's id space is fully separated from any client id space.
type StdioConn struct {
	name string
	log  *slog.Logger

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser

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
func (c *StdioConn) Name() string { return c.name }

// Done returns a channel closed when this connection's reader goroutine exits —
// i.e. when the child process has died (its stdout reached EOF) or Close was
// called. The registry's per-upstream supervisor selects on it to detect a
// crashed stdio upstream and trigger an auto-restart (Stage 7a). Only stdio
// upstreams expose this; the registry reaches it by type-assertion so the HTTP
// upstream (which has no long-lived process to watch) need not implement it.
func (c *StdioConn) Done() <-chan struct{} { return c.done }

// StartStdio launches command with args/env as a child process and starts the
// reader goroutine. It does NOT perform the MCP handshake — call Initialize.
//
// ctx is bound to the process via exec.CommandContext, so cancelling ctx (e.g.
// on Ctrl-C) terminates the child. env entries are "KEY=VALUE"; they are
// appended to the current environment.
//
// onNotify, when non-nil, is invoked (from the reader goroutine) for each
// notification the upstream sends. It must be passed here — not installed
// after the fact — because the reader goroutine starts before StartStdio
// returns, and an upstream may notify immediately; the field is written into
// the struct literal before that goroutine exists, so no lock is needed. The
// callback must not block or call back into the connection synchronously
// (Stage 7b).
func StartStdio(ctx context.Context, log *slog.Logger, name, command string, args, env []string, onNotify func(method string)) (*StdioConn, error) {
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

	c := &StdioConn{
		name:       name,
		log:        log,
		cmd:        cmd,
		stdin:      stdin,
		stdout:     stdout,
		w:          mcp.NewWriter(stdin),
		waiters:    make(map[string]chan *mcp.Message),
		done:       make(chan struct{}),
		stderrDone: make(chan struct{}),
		onNotify:   onNotify, // must be set before go c.readLoop() below
	}

	go c.readLoop()
	go c.drainStderr(stderr)

	return c, nil
}

// readLoop reads framed messages from the child's stdout and routes each
// response to its waiter. It runs until stdout closes (child exit), then fails
// all outstanding waiters.
func (c *StdioConn) readLoop() {
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
func (c *StdioConn) drainStderr(stderr io.Reader) {
	defer close(c.stderrDone)
	sc := bufio.NewScanner(stderr)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		c.log.Debug("upstream stderr", "upstream", c.name, "line", sc.Text())
	}
}

// deliver routes a response to its waiter (if any) by its id.
func (c *StdioConn) deliver(msg *mcp.Message) {
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
func (c *StdioConn) failAll() {
	c.mu.Lock()
	c.closed = true
	waiters := c.waiters
	c.waiters = map[string]chan *mcp.Message{}
	c.mu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

// Call sends a request with a fresh upstream-side id and waits for its response
// or ctx cancellation. The returned *mcp.Message is the raw response (which may
// carry an Error); a nil error means a response was received.
func (c *StdioConn) Call(ctx context.Context, method string, params json.RawMessage) (*mcp.Message, error) {
	id := mcp.IntID(c.nextID.Add(1))
	key := string(id)
	ch := make(chan *mcp.Message, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrConnClosed
	}
	c.waiters[key] = ch
	c.mu.Unlock()

	req := mcp.NewRequest(id, method, params)
	if err := c.w.Write(req); err != nil {
		c.mu.Lock()
		delete(c.waiters, key)
		c.mu.Unlock()
		return nil, fmt.Errorf("upstream %q: write %s: %w", c.name, method, err)
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

// Notify sends a one-way notification (no id, no response expected).
func (c *StdioConn) Notify(method string, params json.RawMessage) error {
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
func (c *StdioConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.closeAndWait()
	})
	return c.closeErr
}

// closeAndWait is Close's one-time body.
func (c *StdioConn) closeAndWait() error {
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
		<-c.done // the kill forces stdout EOF, so readLoop returns promptly now
	}
	<-c.stderrDone // see drainStderr: must finish before Wait touches the pipe
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
