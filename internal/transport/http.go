package transport

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
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
// text/event-stream SSE stream. Every POSTed request still gets a single
// application/json reply — the gateway never streams a response to a request.
// Server-INITIATED traffic, however, now has its own channel (Round 12): GET
// /mcp opens the server→client SSE stream (handleSSE), which carries
// notifications/tools/list_changed on catalog mutations plus the upstream
// notifications the registry forwards verbatim (progress, log messages) — the
// HTTP counterpart of the stdio transport's push path.
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
// size the per-request write deadline (handlePost): the deadline covers the
// whole handler, not just the network write, so it must comfortably exceed
// the slowest legitimate upstream call (bounded by call_timeout) plus the
// gateway's own dispatch/auth overhead — otherwise a deliberately slow (but
// legitimate) upstream tool call would be cut off before
// EffectiveCallTimeout ever fires.
const httpWriteTimeoutSlack = 10 * time.Second

// httpServer reads auth_token and call_timeout through cfg on every request,
// so a SIGHUP reload (which swaps the config inside Registry) takes effect
// live — no transport rebuild needed. listen_addr is the deliberate
// exception: the listener is bound once in Serve, so changing the
// address/port still requires a full process restart (a known, accepted
// limitation) — token rotation and timeout changes do not.
type httpServer struct {
	reg  *registry.Registry
	log  *slog.Logger
	d    *dispatcher
	addr string
	cfg  func() *config.Config

	// sessions holds the live client sessions keyed by Mcp-Session-Id
	// (Stage 16, session.go). It is the only per-CLIENT state the HTTP
	// transport keeps; everything else (registry, dispatcher) is per-process.
	sessions *sessionStore

	// onListen, when non-nil, is invoked with the bound listener address just
	// before Serve starts accepting. Test hook only (the graceful-shutdown
	// test needs the ephemeral port Serve bound); nil in production wiring.
	onListen func(net.Addr)
}

func newHTTPServer(cfg *config.Config, reg *registry.Registry, log *slog.Logger, version string) *httpServer {
	return &httpServer{
		reg: reg,
		log: log,
		// HTTP pushes list_changed over the GET SSE stream (Round 12) but has
		// no channel for gateway→client REQUESTS, so no elicitation (Round 14).
		// Stage 16 built the session layer that such a channel needs (a
		// server→client request must be addressed to a specific session's SSE
		// stream, and its answer arrives on a POST carrying that session id),
		// but deliberately stops there: the flag flips in Stage 17.
		d:        newDispatcher(reg, log, version, true, false), // HTTP: list_changed over SSE, but no server→client requests
		addr:     cfg.EffectiveListenAddr(),
		cfg:      reg.ConfigSnapshot,
		sessions: newSessionStore(),
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
		// WriteTimeout is deliberately 0: it is a static *http.Server field
		// computed once at startup, so it could never follow a SIGHUP reload
		// of call_timeout. handlePost instead sets a live per-request write
		// deadline from the current config (call_timeout + slack) — the
		// slow-client protection is preserved, but it now reacts to reload.
		WriteTimeout: 0,
		IdleTimeout:  httpIdleTimeout,
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

	// Request contexts must die with ctx (Round 12): http.Server.Shutdown only
	// WAITS for active handlers, it does not cancel their request contexts —
	// an open GET SSE stream (handleSSE selects on r.Context()) would
	// otherwise sit in its event loop until the 5s shutdown budget below
	// expired, on EVERY shutdown. Deriving each connection's base context from
	// ctx makes cancellation reach r.Context() the moment the process is told
	// to stop, so SSE handlers return, connections go idle and Shutdown
	// completes promptly.
	srv.BaseContext = func(net.Listener) context.Context { return ctx }

	// Bind explicitly so a failed bind (port in use) surfaces here rather than
	// asynchronously inside ListenAndServe, and so tests can learn the port.
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	if s.onListen != nil {
		s.onListen(ln.Addr())
	}

	s.log.Info("http transport ready", "addr", ln.Addr().String(), "tools", s.reg.ToolCount())

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

// originAllowed reports whether a browser-sent Origin header names a local
// page. Non-browser MCP clients send no Origin at all (the caller skips the
// check for them); a browser page from anywhere else must not be able to drive
// the gateway — the DNS-rebinding defence the MCP transport spec calls for.
// "Local" is config.IsLoopbackHost's definition — the same one Validate
// applies to listen_addr, covering the whole loopback range, not just the
// literal 127.0.0.1/::1 this check used to string-compare against.
func originAllowed(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	return config.IsLoopbackHost(u.Hostname())
}

// handleMCP is the single MCP endpoint. POST carries one client JSON-RPC
// message; GET opens the server→client SSE stream (Round 12); DELETE
// terminates the session named by Mcp-Session-Id (Stage 16). Anything else is
// 405.
//
// Any request carrying an Origin header (i.e. sent by a browser) must come from
// a localhost page, otherwise it is rejected 403 before any dispatch — a
// malicious web page resolving its own hostname to 127.0.0.1 (DNS rebinding)
// could otherwise call the gateway with the victim's local network position.
// The Origin check runs BEFORE the method switch and handleMCP itself sits
// under authMiddleware, so GET SSE passes exactly the same auth+origin gates
// as POST.
func (s *httpServer) handleMCP(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" && !originAllowed(origin) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r)
	case http.MethodGet:
		s.handleSSE(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleSSE serves the server→client SSE stream on GET /mcp (Round 12) — the
// HTTP counterpart of the stdio transport's push path in stdioServer.Serve.
// Everything the gateway can initiate is framed as one SSE event and flushed:
// a registry catalog change (auto-restart, upstream list_changed, reload)
// becomes notifications/tools/list_changed, and every upstream notification
// the registry forwards verbatim (notifications/progress — Round 2,
// notifications/message — Round 3) is serialized whole, jsonrpc/method/params
// untouched.
//
// Broadcast semantics: every open GET stream receives every event. The
// registry's Subscribe/SubscribeNotifications already support any number of
// concurrent subscribers, so several simultaneous clients (or browser tabs)
// each hold their own subscription — nothing extra is needed here; the gateway
// is single-user by design but not single-stream.
//
// The stream sits behind the session gate (Stage 16): GET without
// Mcp-Session-Id is 400, with an unknown or expired one 404. A valid session
// exists only after initialize, so this IS the handshake gate stdio has — and
// it is what lets Stage 17 address a server→client request to the streams of
// ONE session instead of broadcasting it to whoever happens to be listening.
// While the stream is open it counts in the session's stream tally, which
// holds off idle expiry; terminating the session (DELETE, or expiry once the
// last stream is gone) closes done and ends the stream here.
//
// Stream resumption (Last-Event-ID) is deliberately NOT implemented — the
// spec makes resumability optional, and replaying missed events has little
// value here (a missed list_changed is re-derivable via tools/list; stale
// progress is meaningless). The id field is still emitted (a monotonic
// per-connection counter) so clients see well-formed SSE frames; honoring
// Last-Event-ID is deferred until a real client needs it.
func (s *httpServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	sess, ok := s.requireSession(w, r)
	if !ok {
		return
	}
	s.sessions.streamStarted(sess)
	defer s.sessions.streamEnded(sess)

	flusher, ok := w.(http.Flusher)
	if !ok {
		// Cannot happen with net/http's real ResponseWriter, but a middleware
		// that wraps w without implementing Flusher would break streaming —
		// fail loudly instead of buffering events forever.
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	rc := http.NewResponseController(w)

	// Subscribe BEFORE the headers go out, so an event racing the stream open
	// is queued in the subscription buffers rather than lost.
	catalogChanged, unsubscribe := s.reg.Subscribe()
	defer unsubscribe()
	upstreamNotifs, unsubscribeNotifs := s.reg.SubscribeNotifications()
	defer unsubscribeNotifs()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// authMiddleware armed a per-request write deadline on this connection,
	// sized for ONE POST round-trip (call_timeout + slack) — this stream must
	// outlive it by design, or the first event after call_timeout of idleness
	// would be cut off with the client alive and reading. And simply extending
	// the deadline before each write is NOT enough: per
	// http.ResponseController's contract, a write deadline that has already
	// been exceeded can no longer be extended, so it must never be left
	// ticking across an idle gap of unknown length. Therefore: clear it now,
	// while it is still fresh (armed microseconds ago), and let writeEvent
	// re-arm a finite deadline around EACH write and clear it again right
	// after. The POST path keeps its slow-client protection untouched, and a
	// stalled SSE consumer still cannot hold a single write hostage for more
	// than call_timeout + slack.
	_ = rc.SetWriteDeadline(time.Time{})

	// eventID is the monotonic per-connection SSE event id (see the resumption
	// note in the doc comment).
	var eventID uint64
	writeEvent := func(msg *mcp.Message) bool {
		body, err := mcp.Encode(msg)
		if err != nil {
			s.log.Warn("encode SSE event", "method", msg.Method, "err", err)
			return true // skip this event, keep the stream alive
		}
		eventID++
		// Finite deadline for THIS write only — the live config is consulted
		// so a SIGHUP reload of call_timeout takes effect on the next event.
		_ = rc.SetWriteDeadline(time.Now().Add(s.cfg().EffectiveCallTimeout() + httpWriteTimeoutSlack))
		// SSE framing: "id: N\ndata: <json>\n\n" — the blank line terminates
		// the event. mcp.Encode appends the stdio '\n' framing; trim it so the
		// data line stays a single line.
		if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", eventID, bytes.TrimRight(body, "\n")); err != nil {
			return false // client gone or write timed out — end the stream
		}
		flusher.Flush()
		// Disarm again immediately, while the per-write deadline is still
		// live, so the idle gap until the NEXT event can be arbitrarily long.
		_ = rc.SetWriteDeadline(time.Time{})
		return true
	}

	for {
		select {
		case <-r.Context().Done():
			// Client disconnected, or the process is shutting down (Serve
			// derives every request context from its own ctx via BaseContext).
			return
		case <-sess.done:
			// The session was terminated (DELETE /mcp, or swept as expired):
			// its streams must not outlive it.
			return
		case <-catalogChanged:
			if !writeEvent(mcp.NewNotification(mcp.NotifToolsListChanged, nil)) {
				return
			}
		case n := <-upstreamNotifs:
			// Forwarded verbatim — the registry already carries the whole
			// upstream message (method + params untouched).
			if !writeEvent(&n) {
				return
			}
		}
	}
}

// handleDelete terminates the session named by Mcp-Session-Id (Stage 16): the
// spec's explicit client-side "I am done" signal, which frees the session
// state and ends its SSE streams instead of waiting out the idle timeout.
// Missing header is 400, an unknown or already-terminated id is 404, success
// is 204 with no body. It does NOT go through requireSession: terminate both
// looks the session up and removes it, and touching a session we are about to
// delete would be pointless.
func (s *httpServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	sid := r.Header.Get(sessionHeader)
	if sid == "" {
		http.Error(w, "missing "+sessionHeader, http.StatusBadRequest)
		return
	}
	if !s.sessions.terminate(sid) {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	s.log.Debug("http session terminated by client")
	w.WriteHeader(http.StatusNoContent)
}

// sessionHeader is the Streamable HTTP session header, spelled exactly as the
// spec does. Go's http.Header canonicalizes on both get and set, so the casing
// here is for humans reading the code (and for the mirrored client-side use in
// internal/upstream/http.go).
const sessionHeader = "Mcp-Session-Id"

// requireSession enforces the Stage 16 session gate for every request except
// initialize: no header at all is 400, an unknown or expired id is 404. On
// failure it has already written the response and reports ok=false.
//
// The bodies are plain text via http.Error, NOT JSON-RPC errors: this is the
// transport layer, and a spec-conformant client reacts to the HTTP status (404
// means "re-initialize") without parsing a body. The rule holds for
// notifications too — the spec has the server answer with a status and no
// body. Foreign session ids are never echoed back into the error text.
func (s *httpServer) requireSession(w http.ResponseWriter, r *http.Request) (*httpSession, bool) {
	sid := r.Header.Get(sessionHeader)
	if sid == "" {
		http.Error(w, "missing "+sessionHeader, http.StatusBadRequest)
		return nil, false
	}
	sess, _, ok := s.sessions.touch(sid)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return nil, false
	}
	return sess, true
}

// handlePost decodes the single JSON-RPC message in the request body, dispatches
// it, and replies: 202 Accepted with no body for a notification (nothing to
// answer), or a single application/json JSON-RPC response for a request.
//
// Session handling (Stage 16). initialize is the ONE method that may arrive
// without Mcp-Session-Id: it creates a session and the reply carries the new
// id. Every other message — request, notification or response, ping included —
// must carry a valid id: missing is 400, unknown/expired is 404. ping is
// deliberately NOT given a second exemption even though the lifecycle spec
// allows it before initialize: the transport rule ("everything after the id is
// issued carries it") is stated at server level, and a client that wants to
// ping can initialize first — cheaply and conformantly. An initialize that
// DOES carry a valid id re-initializes that same session in place (its id is
// kept and echoed), mirroring how the stdio dispatcher treats a repeated
// handshake.
//
// Client identification (CallRecord.Client) rides on that session: the
// clientInfo from initialize is stored once and attached (registry.WithClient)
// to EVERY request of the session, so an HTTP tools/call is audited under the
// client that actually made it. Before Stage 16 only the initialize request
// itself could be attributed and everything else was logged with an empty
// Client — and with several HTTP clients there was no way to tell them apart
// at all.
func (s *httpServer) handlePost(w http.ResponseWriter, r *http.Request) {
	// Ordering: a bad session id is rejected BEFORE the body is read (no point
	// buffering 4 MiB for a request that cannot be served), but a request with
	// NO header still gets decoded first, so a malformed body keeps answering
	// with the JSON-RPC parse error it always did rather than a bare 400.
	sid := r.Header.Get(sessionHeader)
	var sess *httpSession
	var sessClient string
	if sid != "" {
		var ok bool
		if sess, sessClient, ok = s.sessions.touch(sid); !ok {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	var msg mcp.Message
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&msg); err != nil {
		// Malformed body: a JSON-RPC parse error with a null id (we could not
		// read one), returned as 400 per the transport spec.
		writeJSON(w, http.StatusBadRequest, mcp.NewError(nil, mcp.CodeParseError, "parse error: "+err.Error(), nil))
		return
	}

	isInit := msg.IsRequest() && msg.Method == mcp.MethodInitialize
	switch {
	case sess == nil && !isInit:
		http.Error(w, "missing "+sessionHeader, http.StatusBadRequest)
		return
	case sess == nil: // first initialize → new session
		client := clientString(msg.Params)
		var ok bool
		if sess, ok = s.sessions.create(client, clientServerRequestCaps(msg.Params)); !ok {
			http.Error(w, "too many sessions", http.StatusServiceUnavailable)
			return
		}
		sessClient = client
		s.log.Debug("http session created", "client", client)
	case isInit: // re-initialize of a live session: same id, refreshed identity
		sessClient = clientString(msg.Params)
		if !s.sessions.reinit(sess, sessClient, clientServerRequestCaps(msg.Params)) {
			// Lost a race with a concurrent DELETE that removed this exact
			// session between touch and here (see reinit's doc comment) —
			// answer as if the id had never been valid, and do NOT set the
			// session header: that would echo a dead session's id as if the
			// re-initialize had succeeded.
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
	}

	ctx := registry.WithClient(r.Context(), sessClient)
	if isInit {
		// Must be set before any WriteHeader below.
		w.Header().Set(sessionHeader, sess.id)
	}
	reply := s.d.dispatch(ctx, &msg)
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
//
// The token is read from the live config snapshot on EVERY request, so
// rotating auth_token via SIGHUP reload takes effect immediately: the old
// token stops working and the new one is accepted without a restart.
func (s *httpServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Per-request write deadline, in place of a static Server.WriteTimeout
		// (see buildServer): sized from the CURRENT call_timeout so a SIGHUP
		// reload takes effect on the next request. Set here, at the outermost
		// wrapper for the server's only route, so it covers EVERY response —
		// including the 401 below and handleMCP's own 403/405 — not just the
		// happy path inside handlePost. The error is deliberately ignored —
		// writers that don't support deadlines (httptest.ResponseRecorder in
		// tests) return one, and a missing deadline must not fail the request.
		_ = http.NewResponseController(w).SetWriteDeadline(
			time.Now().Add(s.cfg().EffectiveCallTimeout() + httpWriteTimeoutSlack))

		token := s.cfg().AuthToken
		if token == "" {
			next.ServeHTTP(w, r)
			return
		}
		want := "Bearer " + token
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
