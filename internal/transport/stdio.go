package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// stdioServer is the Phase 1 client-facing transport: it serves exactly ONE MCP
// client over a stdin/stdout pipe pair (the way Claude Code launches a local
// MCP server) and dispatches the client's JSON-RPC requests against the
// aggregated registry.
//
// It is deliberately a single-connection, sequential dispatcher: MCP stdio is
// one pipe with one client (TECHNICAL_PLAN §4.1), so there is no per-connection
// fan-out to manage here. Concurrency lives one layer down, inside the registry
// and each upstream's reader goroutine. The MCP method handling itself lives in
// the shared dispatcher; this type only owns the stdio framing/plumbing.
type stdioServer struct {
	reg *registry.Registry
	log *slog.Logger
	d   *dispatcher

	r *mcp.Reader
	w *mcp.Writer
}

// newStdioServer builds a stdio transport reading client requests from in and
// writing responses to out (os.Stdin/os.Stdout in production; pipes in tests).
func newStdioServer(cfg *config.Config, reg *registry.Registry, log *slog.Logger, version string, in io.Reader, out io.Writer) *stdioServer {
	_ = cfg // reserved: stdio has no config knobs of its own yet
	return &stdioServer{
		reg: reg,
		log: log,
		d:   newDispatcher(reg, log, version, true), // stdio can push server→client list_changed
		r:   mcp.NewReader(in),
		w:   mcp.NewWriter(out),
	}
}

// Serve starts the registry (fan-out to all upstreams), then reads and answers
// client messages until the client's stream ends (EOF) or ctx is cancelled.
// The registry is torn down on return so upstream child processes exit cleanly.
func (s *stdioServer) Serve(ctx context.Context) error {
	if err := s.reg.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = s.reg.Close() }()

	s.log.Info("stdio transport ready", "tools", len(s.reg.Tools()))

	// Subscribe to runtime catalog changes (Stage 7c): whenever an upstream is
	// auto-restarted, sends its own list_changed, or the config is reloaded, the
	// registry signals here and we push notifications/tools/list_changed to the
	// client so it re-lists. stdio is the same single pipe the client already
	// reads, so this server→client notification needs no extra channel.
	catalogChanged, unsubscribe := s.reg.Subscribe()
	defer unsubscribe()

	// mcp.Reader.Read blocks and is not context-aware, so run it in its own
	// goroutine and feed decoded frames over a channel. This lets Serve select
	// on ctx.Done() (Ctrl-C / SIGTERM) and return promptly instead of blocking
	// forever on a client that has gone quiet without closing the pipe.
	//
	// The buffer of 1 matters: if Serve has already returned on ctx.Done() and
	// readFrames was mid-Read, it must still be able to deposit that one
	// in-flight frame without blocking on the send — an unbuffered channel
	// would leak this goroutine forever in that race. readFrames then blocks
	// on the next Read (harmless: reaped when the process exits on shutdown).
	frames := make(chan readResult, 1)
	go s.readFrames(frames)

	// A server MUST NOT push notifications before the client has initialized —
	// a list_changed arriving mid-handshake confuses strict clients. Both flags
	// are plain locals: this select loop is the ONLY place that reads frames and
	// writes push notifications, so no extra synchronization is needed. A
	// catalog change arriving before initialize is parked in pendingListChanged
	// and flushed right after the initialize response goes out.
	initialized := false
	pendingListChanged := false

	for {
		select {
		case <-ctx.Done():
			s.log.Info("shutting down")
			return nil
		case <-catalogChanged:
			// The aggregated catalog changed at runtime — tell the client to
			// re-list. Before the client's initialize, park the signal instead
			// (coalescing into one pending push, matching Subscribe's semantics).
			if !initialized {
				pendingListChanged = true
				continue
			}
			if err := s.pushListChanged(); err != nil {
				return nil
			}
		case fr, ok := <-frames:
			if !ok {
				s.log.Info("client disconnected")
				return nil
			}
			if fr.err != nil {
				// A single malformed line is not fatal: the codec resynchronizes
				// on the next newline, so log and keep serving.
				s.log.Warn("read client message", "err", fr.err)
				continue
			}
			isInitialize := fr.msg.IsRequest() && fr.msg.Method == mcp.MethodInitialize
			reply := s.d.dispatch(ctx, fr.msg)
			if reply == nil {
				continue // notification or ignored message: nothing to write
			}
			if err := s.w.Write(reply); err != nil {
				// A write failure means the client pipe is gone — stop serving.
				s.log.Warn("write reply failed", "err", err)
				return nil
			}
			if isInitialize {
				// The initialize response is on the wire: pushes are allowed from
				// here on. Flush a catalog change that arrived during the gate —
				// it always lands AFTER the initialize response.
				initialized = true
				if pendingListChanged {
					pendingListChanged = false
					if err := s.pushListChanged(); err != nil {
						return nil
					}
				}
			}
		}
	}
}

// pushListChanged writes one notifications/tools/list_changed push to the
// client. Writes to s.w are serialized by mcp.Writer's own mutex, so this is
// safe alongside the reply writes in Serve. A write failure means the client
// pipe is gone — the caller stops serving (same as a failed reply).
func (s *stdioServer) pushListChanged() error {
	if err := s.w.Write(mcp.NewNotification(mcp.NotifToolsListChanged, nil)); err != nil {
		s.log.Warn("write list_changed failed", "err", err)
		return err
	}
	return nil
}

// readResult carries one decoded frame or a per-line decode error from the
// reader goroutine to Serve's dispatch loop.
type readResult struct {
	msg *mcp.Message
	err error
}

// readFrames reads framed client messages and pushes them onto frames until the
// stream ends (EOF), then closes the channel. A decode error for one line is
// forwarded (non-fatal, the stream stays framed); EOF closes the channel.
func (s *stdioServer) readFrames(frames chan<- readResult) {
	defer close(frames)
	for {
		msg, err := s.r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			frames <- readResult{err: err}
			continue
		}
		frames <- readResult{msg: msg}
	}
}
