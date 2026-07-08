package transport

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// httpServer is the Фаза 2 client-facing transport: it exposes the gateway as a
// Streamable HTTP MCP endpoint (MCP 2025-06-18) so a client can reach it over
// HTTP instead of launching it as a stdio subprocess. It is the second Server
// implementation alongside stdioServer, sharing the same dispatcher for all MCP
// method handling — only the framing differs.
//
// Response strategy (docs/MCP_NOTES.md §8): the spec lets a server answer a
// POSTed request with EITHER a single application/json object OR a
// text/event-stream SSE stream. The gateway generates no streaming or
// server-initiated messages of its own in the MVP (no sampling/roots proxying,
// MCP_NOTES §7), so every request gets a single application/json reply and the
// GET SSE channel (server→client) is answered 405. This is fully compliant —
// SSE is optional for the server — and keeps the transport simple; opening SSE
// only becomes necessary if the gateway starts relaying upstream server→client
// traffic, which is post-MVP.
type httpServer struct {
	reg  *registry.Registry
	log  *slog.Logger
	d    *dispatcher
	addr string
}

func newHTTPServer(cfg *config.Config, reg *registry.Registry, log *slog.Logger, version string) *httpServer {
	return &httpServer{
		reg:  reg,
		log:  log,
		d:    newDispatcher(reg, log, version),
		addr: cfg.EffectiveListenAddr(),
	}
}

// Serve starts the registry (upstream fan-out), then runs the HTTP server until
// ctx is cancelled, at which point it shuts the server down gracefully and tears
// the registry down so upstream child processes exit cleanly.
func (s *httpServer) Serve(ctx context.Context) error {
	if err := s.reg.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = s.reg.Close() }()

	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", s.handleMCP)

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Bind explicitly so a failed bind (port in use) surfaces here rather than
	// asynchronously inside ListenAndServe, and so tests can learn the port.
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.log.Info("http transport ready", "addr", ln.Addr().String(), "tools", len(s.reg.Tools()))

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		s.log.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// handleMCP is the single MCP endpoint. POST carries one client JSON-RPC
// message; GET would open a server→client SSE stream, which the gateway does not
// offer in the MVP (405, per the spec's allowance).
func (s *httpServer) handleMCP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r)
	case http.MethodGet:
		// The client MAY open an SSE stream for server-initiated messages; the
		// gateway has none to send in the MVP, so decline per the spec.
		http.Error(w, "SSE stream not offered", http.StatusMethodNotAllowed)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePost decodes the single JSON-RPC message in the request body, dispatches
// it, and replies: 202 Accepted with no body for a notification (nothing to
// answer), or a single application/json JSON-RPC response for a request.
func (s *httpServer) handlePost(w http.ResponseWriter, r *http.Request) {
	var msg mcp.Message
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&msg); err != nil {
		// Malformed body: a JSON-RPC parse error with a null id (we could not
		// read one), returned as 400 per the transport spec.
		writeJSON(w, http.StatusBadRequest, mcp.NewError(nil, mcp.CodeParseError, "parse error: "+err.Error(), nil))
		return
	}

	reply := s.d.dispatch(r.Context(), &msg)
	if reply == nil {
		// A notification (or response) the gateway accepts but need not answer.
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, http.StatusOK, reply)
}

// writeJSON encodes a single MCP message as an application/json response. Errors
// writing to the client are not recoverable (the connection is gone), so they
// are ignored — matching how the stdio transport treats a dead pipe.
func writeJSON(w http.ResponseWriter, status int, msg *mcp.Message) {
	if msg.JSONRPC == "" {
		msg.JSONRPC = mcp.Version
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(msg)
}
