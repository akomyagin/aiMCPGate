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

// stdioServer is the Фаза 1 client-facing transport: it serves exactly ONE MCP
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
		d:   newDispatcher(reg, log, version),
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

	for {
		select {
		case <-ctx.Done():
			s.log.Info("shutting down")
			return nil
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
			reply := s.d.dispatch(ctx, fr.msg)
			if reply == nil {
				continue // notification or ignored message: nothing to write
			}
			if err := s.w.Write(reply); err != nil {
				// A write failure means the client pipe is gone — stop serving.
				s.log.Warn("write reply failed", "err", err)
				return nil
			}
		}
	}
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
