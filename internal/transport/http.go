package transport

import (
	"context"
	"crypto/subtle"
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

// httpServer is the Phase 2 client-facing transport: it exposes the gateway as a
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
// maxRequestBodyBytes bounds a single client POST body. The gateway's own
// listen_addr defaults to loopback, but a user who widens it to a network
// interface (config.example.yaml) would otherwise let any unauthenticated
// caller stream an unbounded body into memory (found by independent
// /code-review on Stage 5). MCP tool-call arguments are small JSON payloads, so
// this is generous, not tight.
const maxRequestBodyBytes = 4 << 20 // 4 MiB

// httpReadTimeout bounds how long reading a full request (headers + body) may
// take. maxRequestBodyBytes already bounds its size; this bounds a
// slow-body attacker (or a stalled connection) from holding a handler
// goroutine open indefinitely regardless of size (found by code review —
// only ReadHeaderTimeout was set, leaving the body-read phase and idle
// connections unbounded).
const httpReadTimeout = 30 * time.Second

// httpIdleTimeout bounds how long a keep-alive connection may sit idle
// between requests.
const httpIdleTimeout = 120 * time.Second

// httpWriteTimeoutSlack is added on top of the configured call_timeout to
// size WriteTimeout: net/http.Server.WriteTimeout covers the whole handler,
// not just the network write, so it must comfortably exceed the slowest
// legitimate upstream call (bounded by call_timeout) plus the gateway's own
// dispatch/auth overhead — otherwise a deliberately slow (but legitimate)
// upstream tool call would be cut off before EffectiveCallTimeout ever fires.
const httpWriteTimeoutSlack = 10 * time.Second

type httpServer struct {
	reg         *registry.Registry
	log         *slog.Logger
	d           *dispatcher
	addr        string
	authToken   string
	callTimeout time.Duration
}

func newHTTPServer(cfg *config.Config, reg *registry.Registry, log *slog.Logger, version string) *httpServer {
	return &httpServer{
		reg:         reg,
		log:         log,
		d:           newDispatcher(reg, log, version, false), // HTTP is POST-only: no server→client channel
		addr:        cfg.EffectiveListenAddr(),
		authToken:   cfg.AuthToken,
		callTimeout: cfg.EffectiveCallTimeout(),
	}
}

// buildServer assembles the *http.Server Serve runs, wired to mux. Split out
// from Serve so its timeout configuration is unit-testable without binding a
// real listener.
func (s *httpServer) buildServer(mux http.Handler) *http.Server {
	return &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      s.callTimeout + httpWriteTimeoutSlack,
		IdleTimeout:       httpIdleTimeout,
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
	mux.Handle("/mcp", s.authMiddleware(http.HandlerFunc(s.handleMCP)))
	srv := s.buildServer(mux)

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
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

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

// authMiddleware rejects requests without a valid "Authorization: Bearer <token>"
// header when AuthToken is configured. Skipped entirely when AuthToken is empty
// (loopback-only deployments). Uses constant-time comparison to prevent timing
// attacks even though tokens are not cryptographic secrets in practice.
func (s *httpServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.authToken == "" {
			next.ServeHTTP(w, r)
			return
		}
		want := "Bearer " + s.authToken
		got := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
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
