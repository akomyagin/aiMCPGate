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

// gatewayCapabilities is what the gateway advertises to the client on
// initialize. The gateway aggregates upstream TOOLS in Фаза 1; resources are
// not yet aggregated (see handleResourcesList), so only the tools capability is
// declared. It is a raw JSON literal because the gateway does not otherwise
// interpret its own capability object.
var gatewayCapabilities = json.RawMessage(`{"tools":{"listChanged":false}}`)

// stdioServer is the Фаза 1 client-facing transport: it serves exactly ONE MCP
// client over a stdin/stdout pipe pair (the way Claude Code launches a local
// MCP server) and dispatches the client's JSON-RPC requests against the
// aggregated registry.
//
// It is deliberately a single-connection, sequential dispatcher: MCP stdio is
// one pipe with one client (TECHNICAL_PLAN §4.1), so there is no per-connection
// fan-out to manage here. Concurrency lives one layer down, inside the registry
// and each upstream's reader goroutine.
type stdioServer struct {
	cfg     *config.Config
	reg     *registry.Registry
	log     *slog.Logger
	version string

	r *mcp.Reader
	w *mcp.Writer
}

// newStdioServer builds a stdio transport reading client requests from in and
// writing responses to out (os.Stdin/os.Stdout in production; pipes in tests).
func newStdioServer(cfg *config.Config, reg *registry.Registry, log *slog.Logger, version string, in io.Reader, out io.Writer) *stdioServer {
	return &stdioServer{
		cfg:     cfg,
		reg:     reg,
		log:     log,
		version: version,
		r:       mcp.NewReader(in),
		w:       mcp.NewWriter(out),
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
			if err := s.dispatch(ctx, fr.msg); err != nil {
				// A write failure means the client pipe is gone — stop serving.
				s.log.Warn("dispatch failed", "err", err)
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

// dispatch handles one client message. Notifications yield no response. For
// requests, exactly one response (result or error) is written, always echoing
// the CLIENT's id — never the upstream-side id the registry used internally.
func (s *stdioServer) dispatch(ctx context.Context, msg *mcp.Message) error {
	if msg.IsNotification() {
		// notifications/initialized and the like need no reply in Фаза 1.
		s.log.Debug("client notification", "method", msg.Method)
		return nil
	}
	if !msg.IsRequest() {
		// A response from the client (unexpected in the server role) — ignore.
		s.log.Debug("unexpected client message ignored", "method", msg.Method)
		return nil
	}

	switch msg.Method {
	case mcp.MethodInitialize:
		return s.handleInitialize(msg)
	case mcp.MethodToolsList:
		return s.handleToolsList(msg)
	case mcp.MethodToolsCall:
		return s.handleToolsCall(ctx, msg)
	case mcp.MethodResourceList:
		return s.handleResourcesList(msg)
	case mcp.MethodResourceRead:
		return s.handleResourcesRead(msg)
	default:
		return s.writeError(msg.ID, mcp.CodeMethodNotFound, "method not found: "+msg.Method, nil)
	}
}

// handleInitialize answers the client handshake with the gateway's own
// serverInfo and aggregated capabilities, echoing the client's id.
func (s *stdioServer) handleInitialize(req *mcp.Message) error {
	result := mcp.InitializeResult{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    gatewayCapabilities,
		ServerInfo: mcp.Implementation{
			Name:    "aiMCPGate",
			Version: s.version,
		},
	}
	return s.writeResult(req.ID, mcp.MustParams(result))
}

// handleToolsList returns the aggregated, namespaced catalog. Each tool's
// schema (description/inputSchema/...) is carried through verbatim so the client
// sees the exact contract each upstream advertises. Pagination is not needed:
// the registry already merged every upstream's full paginated catalog on Start.
func (s *stdioServer) handleToolsList(req *mcp.Message) error {
	descs := s.reg.Tools()
	tools := make([]mcp.Tool, 0, len(descs))
	for _, d := range descs {
		t := d.Tool
		t.Name = d.Name // client-facing namespaced name, not the upstream original
		tools = append(tools, t)
	}
	return s.writeResult(req.ID, mcp.MustParams(mcp.ToolsListResult{Tools: tools}))
}

// handleToolsCall proxies a call through the registry, which resolves the owning
// upstream, rewrites the name to the upstream original, mints its own
// upstream-side id, and audits the call. The upstream's raw result/error is
// forwarded to the client under the CLIENT's id.
//
// ctx (with any client-supplied deadline) is passed straight to the registry,
// which further bounds it by the configured call timeout — so both a short
// client deadline and the gateway's own timeout can cancel the call.
func (s *stdioServer) handleToolsCall(ctx context.Context, req *mcp.Message) error {
	var params mcp.ToolsCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return s.writeError(req.ID, mcp.CodeInvalidParams, "invalid tools/call params: "+err.Error(), nil)
	}
	if params.Name == "" {
		return s.writeError(req.ID, mcp.CodeInvalidParams, "tools/call missing tool name", nil)
	}

	resp, err := s.reg.CallTool(ctx, params.Name, params.Arguments)
	if err != nil {
		// Routing/transport failure (unknown tool, dead upstream, timeout):
		// surface it as a JSON-RPC error to the client under its own id. The
		// error string is a sanitized gateway message — it never contains the
		// call arguments (which may hold secrets).
		return s.writeError(req.ID, mcp.CodeInternalError, err.Error(), nil)
	}

	// resp is the upstream's raw response. Its own ID is the gateway's
	// upstream-side id and MUST NOT leak to the client; re-wrap the payload
	// under the client's id, forwarding the upstream's result or error verbatim.
	if resp.Error != nil {
		return s.writeError(req.ID, resp.Error.Code, resp.Error.Message, resp.Error.Data)
	}
	return s.writeResult(req.ID, resp.Result)
}

// handleResourcesList returns an empty resource catalog. Resource aggregation
// across upstreams is not implemented in Фаза 1 (the registry lists but does
// not merge resources yet) — TODO(post-MVP): aggregate and route resources the
// way tools are, then serve them here. Returning an empty list (rather than a
// method-not-found error) keeps well-behaved clients that probe resources happy.
func (s *stdioServer) handleResourcesList(req *mcp.Message) error {
	return s.writeResult(req.ID, mcp.MustParams(mcp.ResourceListResult{Resources: []mcp.Resource{}}))
}

// handleResourcesRead reports an error: with no aggregated resources (see
// handleResourcesList), any uri the client could ask to read is unknown.
// TODO(post-MVP): route resources/read to the owning upstream once resources
// are aggregated.
func (s *stdioServer) handleResourcesRead(req *mcp.Message) error {
	return s.writeError(req.ID, mcp.CodeInvalidParams, "resources are not aggregated in this build", nil)
}

func (s *stdioServer) writeResult(id, result json.RawMessage) error {
	return s.w.Write(mcp.NewResult(id, result))
}

func (s *stdioServer) writeError(id json.RawMessage, code int, message string, data json.RawMessage) error {
	return s.w.Write(mcp.NewError(id, code, message, data))
}
