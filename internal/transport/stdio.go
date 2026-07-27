package transport

import (
	"context"
	"encoding/json"
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
// It is a single-connection dispatcher: MCP stdio is one pipe with one client
// (TECHNICAL_PLAN §4.1), so there is no per-connection fan-out to manage here.
// Every method dispatches sequentially, inline in the Serve loop, EXCEPT
// tools/call, which runs on a goroutine per request (Round 2) — a long call
// must not block the loop from reading a notifications/cancelled that aborts
// it, from pushing an upstream's progress notification, or from serving a
// concurrent fast call. Concurrent reply writes are serialized by mcp.Writer's
// mutex. The MCP method handling itself lives in the shared dispatcher; this
// type only owns the stdio framing/plumbing.
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
		d:   newDispatcher(reg, log, version, true, true), // stdio can push server→client list_changed AND proxy server→client requests
		r:   mcp.NewReader(in),
		w:   mcp.NewWriter(out),
	}
}

// Serve reads and answers client messages until the client's stream ends (EOF)
// or ctx is cancelled, starting the registry (fan-out to all upstreams) LAZILY
// on the first client request that needs it — see the frames branch below for
// why that is not simply "on initialize". The registry is torn down on return
// so upstream child processes exit cleanly.
func (s *stdioServer) Serve(ctx context.Context) error {
	// EVERY subscription is registered first, before the registry can possibly
	// start (see the lazy start in the frames branch below). The Round 14
	// invariant is unchanged and is the reason they do NOT move inside that
	// lazy start: once the handshakes fan out, a stdio upstream that saw a
	// declared capability may ask at any moment, and the registry's "publish
	// reached no subscriber" fallback must stay unreachable BY CONSTRUCTION —
	// not merely shadowed by the client-capability gate happening to fire
	// first (found by review).
	//
	// serverReqs carries upstream-initiated requests the gateway proxies to
	// its client (Round 14 for elicitation/create; Stage 15 for
	// sampling/createMessage and roots/list): the loop below pushes each to
	// the client under its gateway-minted id, and the client's answer is
	// routed back in the frames branch (RouteUpstreamResponse).
	serverReqs, unsubscribeServerReqs := s.reg.SubscribeUpstreamRequests()
	defer unsubscribeServerReqs()

	// catalogChanged carries runtime catalog changes (Stage 7c): whenever an
	// upstream is auto-restarted, sends its own list_changed, or the config is
	// reloaded, the registry signals here and we push
	// notifications/tools/list_changed to the client so it re-lists. stdio is
	// the same single pipe the client already reads, so this server→client
	// notification needs no extra channel.
	catalogChanged, unsubscribe := s.reg.Subscribe()
	defer unsubscribe()

	// upstreamNotifs carries upstream notifications forwarded verbatim
	// (Round 2 — today notifications/progress): an upstream reporting progress
	// on an in-flight tools/call publishes here and the loop below pushes it
	// to the client over the same pipe, gated on initialized like list_changed.
	upstreamNotifs, unsubscribeNotifs := s.reg.SubscribeNotifications()
	defer unsubscribeNotifs()

	// The registry is torn down on return no matter how far the lazy start
	// below got: Close is correct for a registry that never started (its phase
	// is still phaseNew, conns and supervisors are empty).
	defer func() { _ = s.reg.Close() }()

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
	// are plain locals: this select loop is the ONLY goroutine that reads or
	// writes them (the per-call tools/call goroutines never touch the flags —
	// they receive dispatchCtx by value), so no extra synchronization is
	// needed. A catalog change arriving before initialize is parked in
	// pendingListChanged and flushed right after the initialize response goes
	// out.
	initialized := false
	pendingListChanged := false

	// started guards the LAZY registry bring-up in the frames branch below.
	// Like the two flags above it is a plain local: this select loop is the
	// only goroutine that touches it.
	started := false

	// dispatchCtx is what every d.dispatch call receives. stdio serves exactly
	// ONE client per process, so after a successful initialize it is wrapped
	// ONCE with that client's identity (registry.WithClient) and every later
	// call — tools/call above all — is audited with it (CallRecord.Client).
	// Before initialize it is just ctx; cancellation semantics are identical
	// either way (WithClient adds only a value).
	dispatchCtx := ctx

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
			if err := s.pushListChanged(ctx); err != nil {
				return nil
			}
		case n := <-upstreamNotifs:
			// An upstream notification forwarded verbatim (Round 2 — progress).
			// Before initialize it is simply DROPPED, not parked: unlike a
			// list_changed (a durable "re-list eventually" fact), progress only
			// makes sense against an in-flight client call, and a client that
			// has not initialized cannot have one.
			if !initialized {
				continue
			}
			if err := s.writeOrBail(ctx, mcp.NewNotification(n.Method, n.Params)); err != nil {
				s.log.Warn("write forwarded notification failed", "err", err)
				return nil
			}
		case req := <-serverReqs:
			// An upstream's server→client request proxied to the client under
			// the gateway-minted string id. The method travels in the event, so
			// this branch is method-agnostic: elicitation/create,
			// sampling/createMessage and roots/list all take the same write.
			// Before initialize it is REFUSED, not dropped. By construction
			// such a request cannot arise (it only happens mid-call, and the
			// registry does not even start until the client's initialize has
			// been parsed), but a silent drop would leave the registry's
			// pending entry parked forever — and for roots/list that single
			// leaked entry wedges the single-flight for the life of the
			// process, so every later asker parks behind a question nobody
			// will ever answer. Refusing makes this a check rather than a bet
			// on the ordering never changing (found by review).
			if !initialized {
				s.log.Warn("server→client request before the client handshake, refusing",
					"method", req.Method, "id", req.GatewayID)
				s.reg.RefusePendingServerReq(req.GatewayID,
					"the gateway's client has not completed its handshake")
				continue
			}
			if err := s.writeOrBail(ctx, mcp.NewRequest(mcp.StringID(req.GatewayID), req.Method, req.Params)); err != nil {
				s.log.Warn("write upstream request failed", "method", req.Method, "err", err)
				return nil
			}
		case fr, ok := <-frames:
			if !ok {
				s.log.Info("client disconnected")
				return nil
			}
			if fr.err != nil {
				// A single malformed line is not fatal: the codec resynchronizes
				// on the next newline, so log and keep serving. A fatal reader
				// error (mcp.ErrFatalRead) is different — readFrames has already
				// stopped and closed the channel, so the next receive here ends
				// Serve; log it louder because the connection is dying.
				if errors.Is(fr.err, mcp.ErrFatalRead) {
					s.log.Error("client stream failed permanently", "err", fr.err)
				} else {
					s.log.Warn("read client message", "err", fr.err)
				}
				continue
			}
			// Lazy registry bring-up. The upstream handshakes must not happen
			// before the client's initialize is PARSED: what the gateway may
			// declare to its upstreams (elicitation/sampling/roots) is exactly
			// what this client declared, capabilities are exchanged once per
			// session, and a declaration after Start could never catch up
			// (Stage 15).
			//
			// The trigger is "the first request that needs the registry", not
			// "initialize", on purpose. ping is the one request the spec allows
			// before the handshake and it consults nothing, so starting on it
			// would freeze an empty declaration set forever for a client that
			// pings first — the feature would die silently. Client
			// NOTIFICATIONS need no catalog either. Every other request does,
			// including the non-conformant "tools/list before initialize",
			// which therefore still gets the full aggregated catalog — it just
			// degrades to today's behaviour of declaring exactly {} upstream,
			// since no capabilities have been seen yet.
			needsRegistry := fr.msg.IsRequest() && fr.msg.Method != mcp.MethodPing
			if needsRegistry && !started {
				var caps map[string]json.RawMessage
				if fr.msg.Method == mcp.MethodInitialize {
					caps = clientServerRequestCaps(fr.msg.Params)
				}
				s.reg.SetClientServerRequestCaps(caps) // strictly BEFORE Start
				if err := s.reg.Start(ctx); err != nil {
					// Start only fails when the gateway cannot proceed at all
					// or when EVERY upstream failed — deliberately treated as a
					// misconfiguration rather than a useful degraded mode (see
					// Registry.Start). Answering with an empty catalog would
					// hide that inside a client that just sees zero tools, so
					// the client gets a diagnosable JSON-RPC error under its own
					// request id instead of an abrupt EOF, and Serve still
					// returns the error — the process exits with it exactly as
					// before this became lazy. The message is sanitized (no
					// upstream names, no internal strings), same policy as
					// handleToolsCall; the details go to the operational log.
					// A failed write is ignored: the client may already be gone
					// and that must not replace the real cause.
					s.log.Error("registry start failed", "err", err)
					_ = s.writeOrBail(ctx, mcp.NewError(fr.msg.ID, mcp.CodeInternalError,
						"gateway failed to start its upstreams", nil))
					return err
				}
				started = true
				s.log.Info("stdio transport ready", "tools", s.reg.ToolCount())
			}
			if fr.msg.IsRequest() && fr.msg.Method == mcp.MethodToolsCall {
				// tools/call — and ONLY tools/call — dispatches on its own
				// goroutine (Round 2): dispatched inline it would block this
				// loop for up to the call timeout, making a client's
				// notifications/cancelled unreadable until the very call it
				// cancels returns (dead cancellation) and stalling progress
				// pushes and concurrent calls behind it. dispatchCtx is passed
				// BY VALUE so a re-initialize reassigning the loop variable
				// cannot race this read. Concurrent replies are safe: s.w
				// (mcp.Writer) serializes writes with its own mutex. A write
				// error cannot end Serve from here — it is logged, and the
				// dying pipe will surface in the main loop's own next write or
				// read anyway. A nil reply means the call was cancelled by the
				// client: per the spec the cancelled request gets NO response.
				go func(dctx context.Context, msg *mcp.Message) {
					reply := s.d.dispatch(dctx, msg)
					if reply == nil {
						return
					}
					if err := s.writeOrBail(ctx, reply); err != nil {
						s.log.Warn("write tools/call reply failed", "err", err)
					}
				}(dispatchCtx, fr.msg)
				continue
			}
			if fr.msg.IsResponse() && !fr.msg.IsMalformedHybrid() {
				// A response FROM the client: the only ones the gateway ever
				// expects are answers to its own proxied server→client
				// requests, carrying the gateway-minted string id.
				// Consumed here, never dispatched — it was not a request, so no
				// reply goes back. Anything unmatched falls through to the
				// dispatcher, which drops it with a log exactly as before.
				var gatewayID string
				if json.Unmarshal(fr.msg.ID, &gatewayID) == nil &&
					s.reg.RouteUpstreamResponse(gatewayID, fr.msg) {
					continue
				}
			}
			// Everything else — initialize, ping, notifications (including
			// notifications/cancelled, which must never queue behind a running
			// call now that tools/call is off-loop) — stays synchronous inline.
			isInitialize := fr.msg.IsRequest() && fr.msg.Method == mcp.MethodInitialize
			reply := s.d.dispatch(dispatchCtx, fr.msg)
			if reply == nil {
				continue // notification or ignored message: nothing to write
			}
			if err := s.writeOrBail(ctx, reply); err != nil {
				// A write failure means the client pipe is gone (or ctx was
				// cancelled mid-write) — stop serving.
				s.log.Warn("write reply failed", "err", err)
				return nil
			}
			if isInitialize {
				// The initialize response is on the wire: pushes are allowed from
				// here on. Flush a catalog change that arrived during the gate —
				// it always lands AFTER the initialize response.
				initialized = true
				// Attach the client identity from the initialize params to every
				// subsequent dispatch (see dispatchCtx above). Wrapping the BASE
				// ctx, not the previous dispatchCtx, keeps a re-initialize from
				// stacking values — the latest client simply wins.
				if client := clientString(fr.msg.Params); client != "" {
					dispatchCtx = registry.WithClient(ctx, client)
				}
				if pendingListChanged {
					pendingListChanged = false
					if err := s.pushListChanged(ctx); err != nil {
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
func (s *stdioServer) pushListChanged(ctx context.Context) error {
	if err := s.writeOrBail(ctx, mcp.NewNotification(mcp.NotifToolsListChanged, nil)); err != nil {
		s.log.Warn("write list_changed failed", "err", err)
		return err
	}
	return nil
}

// writeOrBail writes msg to the client but gives up when ctx is cancelled.
// s.w.Write is a plain blocking write: if the client stops draining its end of
// the pipe (full OS pipe buffer), the write hangs forever and — without this
// helper — SIGTERM/Ctrl-C (ctx cancellation) could never unblock Serve. The
// write runs in a short-lived goroutine; on cancellation that goroutine leaks
// until its Write eventually returns, which is acceptable: the same cancelled
// ctx is already shutting the whole process down. The result channel is
// buffered (cap 1) so the abandoned writer can deposit its error and exit
// instead of blocking on the send forever.
func (s *stdioServer) writeOrBail(ctx context.Context, msg *mcp.Message) error {
	done := make(chan error, 1)
	go func() { done <- s.w.Write(msg) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// readResult carries one decoded frame or a per-line decode error from the
// reader goroutine to Serve's dispatch loop.
type readResult struct {
	msg *mcp.Message
	err error
}

// readFrames reads framed client messages and pushes them onto frames until the
// stream ends (EOF) or the reader fails permanently, then closes the channel.
// A decode error for one line is forwarded (non-fatal, the stream stays framed)
// and reading continues; a fatal reader error (mcp.ErrFatalRead — the scanner
// is permanently broken and every retry would fail the same way) is forwarded
// once and ends the loop, otherwise this would busy-loop at 100% CPU.
func (s *stdioServer) readFrames(frames chan<- readResult) {
	defer close(frames)
	for {
		msg, err := s.r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			frames <- readResult{err: err}
			if errors.Is(err, mcp.ErrFatalRead) {
				return
			}
			continue
		}
		frames <- readResult{msg: msg}
	}
}
