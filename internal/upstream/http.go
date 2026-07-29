package upstream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// maxHTTPResponseBytes bounds a single upstream HTTP response body — the
// application/json branch of call() used to decode with no limit at all
// (found by code review) while the SSE branch already capped at this same
// size, matching internal/mcp's line cap for the stdio side. A misbehaving
// or malicious upstream could otherwise force the gateway to buffer an
// unbounded response into memory.
const maxHTTPResponseBytes = 32 << 20 // 32 MiB

// Backoff schedule for reconnecting the long-lived GET SSE stream (Round 13)
// after it drops. The VALUES mirror the registry's stdio restart defaults
// (config.DefaultRestartInitialBackoff / DefaultRestartMaxBackoff /
// RestartBackoffFactor) so the two recovery loops feel the same to an
// operator, but they are deliberately local constants, not imports: the
// config.RestartPolicy is a per-upstream, hot-reloadable *process restart*
// policy owned by the registry, while this is a fixed transport-internal
// retry for an optional stream — pulling the config package into the
// transport layer to share three numbers would couple the wrong layers.
const (
	sseStreamInitialBackoff = 1 * time.Second
	sseStreamMaxBackoff     = 30 * time.Second
	sseStreamBackoffFactor  = 2
)

// respondPostTimeout bounds the ONE POST that carries the gateway's answer to a
// request the upstream itself asked (Stage 17b). respond gets no ctx from its
// caller — the registry answers from a detached goroutine with no request
// context to hand down — so the deadline is built here, on top of streamCtx, and
// a Close therefore aborts an in-flight answer instead of letting the goroutine
// outlive the connection.
//
// A local constant, deliberately unrelated to the operator's call_timeout: this
// is a one-shot round-trip carrying an already-computed answer, not a tool
// invocation, and reaching into the config package for a single number would
// couple the same two layers the sseStream* constants above refuse to couple.
// Without ANY deadline the POST would hang until TCP gave up, wedging the
// answering goroutine for minutes.
const respondPostTimeout = 30 * time.Second

// errSessionExpired signals that the upstream answered HTTP 404 to a request
// carrying an Mcp-Session-Id. Per the spec (Streamable HTTP, session
// management) the server may expire a session at any time, and the client
// MUST start a new one with a fresh InitializeRequest. call clears the stale
// session id before returning this error; Conn.CallTool matches it
// (errors.Is) to re-initialize and retry the call once.
var errSessionExpired = errors.New("upstream: session expired (HTTP 404), reinitialize required")

// httpTransport is the transport half of a connection to one upstream MCP
// server reached over the Streamable HTTP transport (MCP 2025-06-18). Protocol
// logic (Initialize etc.) lives on Conn (protocol.go); this type only knows how
// to move JSON-RPC messages over HTTP. See docs/MCP_NOTES.md §8.
//
// Transport shape (docs/MCP_NOTES.md §8):
//   - every JSON-RPC message is one HTTP POST to the endpoint URL;
//   - the client (this gateway) advertises Accept: application/json,
//     text/event-stream, so the server may answer a request either with a
//     single JSON object or with an SSE stream;
//   - for an SSE answer we read data: frames until we see the JSON-RPC response
//     whose id matches the request we sent, then close the stream;
//   - a session id handed back on initialize (Mcp-Session-Id) is echoed on all
//     subsequent requests; the negotiated MCP-Protocol-Version header goes on
//     every request after initialize.
//
// Unlike stdioTransport there is no long-lived reader goroutine for
// request/response traffic: each Call owns its own round-trip and
// id-demultiplexing is unnecessary. Concurrency safety comes from net/http
// (safe for concurrent use) plus a mutex guarding the session id.
//
// There IS, however, one long-lived goroutine per connection since Round 13:
// after a successful handshake the transport opens a GET request to the same
// endpoint with Accept: text/event-stream — the spec's server→client stream —
// and forwards what arrives on it to the same two callbacks stdio's readLoop
// feeds: notifications to onNotify (Round 13) and server-initiated REQUESTS,
// which the upstream expects an answer to, to onRequest (Stage 17b). So
// notifications/tools/list_changed as well as elicitation/create & co. from
// HTTP upstreams reach the registry exactly as they do from stdio ones. Such an
// answer travels back as one ordinary POST (respond) under the upstream's own
// request id — HTTP has no persistent write channel to push it into.
// Best-effort: an upstream that answers the GET with anything but an SSE
// stream simply pushes nothing (see runSSEStream); requests related to a call
// in flight may also arrive on that call's own POST stream (readSSEResponse).
//
// KNOWN LIMITATION (Round 2): cancellation is NOT forwarded to HTTP upstreams.
// stdioTransport.call sends a best-effort notifications/cancelled down the
// same pipe when its ctx is cancelled mid-call; here there is no live channel
// tied to the in-flight request — a cancelled ctx simply aborts the HTTP
// round-trip (net/http cancels the request), and telling the server "stop the
// work" would require a SEPARATE POST racing the one being torn down, with no
// guarantee the server correlates them. Progress notifications tied to a
// specific POST are still delivered by the server on that POST's own SSE
// response body per the spec, where readSSEResponse skips them — the GET
// stream carries only messages NOT related to an in-flight request.
type httpTransport struct {
	name     string
	endpoint string
	log      *slog.Logger
	client   *http.Client

	// headers are static per-upstream headers (typically Authorization). They
	// are applied to every request and MUST NOT be logged (they carry secrets).
	headers map[string]string

	nextID atomic.Int64

	// onNotify, when set, is invoked — from the SSE stream goroutine — for
	// each notification the upstream pushes on the long-lived GET stream
	// (Round 13), with the method and raw params. Same contract as
	// stdioTransport.onNotify: set once in StartHTTP before any goroutine
	// exists (no lock needed), must be cheap and non-blocking, must not call
	// back into the connection synchronously.
	onNotify func(method string, params json.RawMessage)

	// onRequest, when set, is invoked for every server-initiated REQUEST the
	// upstream pushes — its method, ORIGINAL id and raw params (Stage 17b,
	// the HTTP half of what stdio has had since Round 14). Two SSE channels
	// feed it: the long-lived GET stream (dispatchStreamEvent) and the
	// response stream of one of our own POSTs, where the spec explicitly
	// allows a server to interleave requests RELATED to the call in flight —
	// which is exactly where real SDK servers put an elicitation/create
	// raised inside a tools/call (readSSEResponse). WHICH methods are
	// actually proxied is the registry's policy (its spec table), never this
	// transport's — here a frame is only classified. Same contract as
	// onNotify: set once in StartHTTP before any goroutine exists, cheap,
	// non-blocking, no synchronous call back into the connection (the
	// registry answers from its own goroutine).
	onRequest func(method string, id, params json.RawMessage)

	// onStreamUnavailable, when set, is invoked at most once per connection —
	// from the SSE stream goroutine — when the upstream EXPLICITLY refuses the
	// GET stream (405/404, or a 200 that is not an event stream). It exists so
	// the registry can journal an operator event: no GET stream means no
	// tools/list_changed from this upstream for the life of the gateway process,
	// a state that was previously visible only as one Info line on stderr.
	// Same contract as onNotify/onRequest: set once in StartHTTP before any
	// goroutine exists, cheap, non-blocking, no synchronous call back into the
	// connection.
	onStreamUnavailable func()

	// streamCtx/streamCancel bound the lifetime of the GET SSE stream
	// goroutine, independent of any per-call ctx: Close cancels it and then
	// waits on streamDone (guarded by mu, below).
	streamCtx    context.Context
	streamCancel context.CancelFunc

	mu        sync.Mutex
	sessionID string // Mcp-Session-Id assigned by the server on initialize, if any
	// streamStarted records that startSSEStream already launched (or refused
	// to launch) the stream goroutine, making it idempotent across repeated
	// Initialize calls (session-expiry re-init runs the handshake again).
	streamStarted bool
	// streamDone is closed when the SSE stream goroutine exits. It starts out
	// as an ALREADY-CLOSED channel (StartHTTP), so Close can always wait on it
	// unconditionally: if the goroutine never launched there is nothing to
	// wait for, and the pre-closed channel says exactly that. startSSEStream
	// swaps in a fresh open channel — under mu, after checking streamCtx is
	// not yet cancelled — so a Close racing a concurrent startSSEStream either
	// prevents the launch (cancel observed under mu) or waits for the launched
	// goroutine's own channel. No goroutine can leak past Close either way.
	streamDone chan struct{}
	// negotiatedVersion is the protocol version the upstream agreed to in its
	// initialize result (which may differ from the version we proposed). Once
	// set — by Conn.Initialize via setNegotiatedVersion — every request
	// advertises IT in the MCP-Protocol-Version header; until then post falls
	// back to the package constant (the version we propose in initialize).
	negotiatedVersion string
}

// StartHTTP builds a Conn over an httpTransport for endpoint. It performs no
// network I/O — the handshake happens in Initialize, mirroring StartStdio which
// likewise defers the handshake. headers are extra per-request headers (e.g.
// Authorization); their values are treated as secrets and never logged. A nil
// client gets a dedicated per-connection client, so Close cannot disturb other
// upstreams (see below).
//
// onNotify, when non-nil, receives every notification the upstream pushes on
// its server→client GET SSE stream (opened after a successful handshake, see
// startSSEStream) — the HTTP counterpart of StartStdio's onNotify.
//
// onRequest, when non-nil, receives every server-initiated REQUEST the upstream
// sends, with the method, the upstream's ORIGINAL id and raw params — from the
// GET stream or interleaved into the SSE response of one of our POSTs (Stage
// 17b). nil means such requests are log-and-dropped, the pre-Stage-17b
// behaviour. Both callbacks must be passed here rather than installed later:
// the stream goroutine may see a frame the instant the handshake completes, and
// writing the fields into the struct literal before any goroutine exists is
// what makes them lock-free.
//
// onStreamUnavailable, when non-nil, is called once if the upstream refuses the
// GET stream outright — the registry turns that into an operator event (Stage
// 18). nil disables the notification.
//
// With BOTH onNotify and onRequest nil nobody listens to the GET stream, so it
// is never opened at all.
func StartHTTP(log *slog.Logger, name, endpoint string, headers map[string]string, client *http.Client, gatewayVersion string, onNotify func(method string, params json.RawMessage), onRequest func(method string, id, params json.RawMessage), onStreamUnavailable func()) *Conn {
	if client == nil {
		// Each connection gets its OWN client with its own cloned transport.
		// Sharing one package-level client (the previous design) meant Close on
		// ONE upstream tore down the keep-alive pool of EVERY other HTTP
		// upstream — and, worse, its CloseIdleConnections hit the process-global
		// http.DefaultTransport shared with all other code in the binary.
		//
		// Timeout is deliberately left 0 (no client-wide limit): every call is
		// already bounded by context.WithTimeout(ctx, EffectiveCallTimeout())
		// in the registry — a live, reload-aware deadline — whereas
		// http.Client.Timeout covers the whole round-trip INCLUDING body reads,
		// so a static value here would cut long SSE streams even when the
		// operator explicitly configured call_timeout above it. It would not be
		// a harmless backstop but an active source of drift from the config.
		client = &http.Client{
			Transport: http.DefaultTransport.(*http.Transport).Clone(),
		}
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	// streamDone starts CLOSED — see the field comment: Close waits on it
	// unconditionally, and until startSSEStream launches the goroutine there
	// is nothing to wait for.
	streamDone := make(chan struct{})
	close(streamDone)
	return &Conn{
		transport: &httpTransport{
			name:                name,
			endpoint:            endpoint,
			log:                 log,
			client:              client,
			headers:             headers,
			onNotify:            onNotify,  // must be set before any goroutine exists
			onRequest:           onRequest, // same rule — see the doc comment
			onStreamUnavailable: onStreamUnavailable,
			streamCtx:           streamCtx,
			streamCancel:        streamCancel,
			streamDone:          streamDone,
		},
		gatewayVersion: gatewayVersion,
	}
}

// setNegotiatedVersion records the protocol version the upstream agreed to in
// its initialize result; see the negotiatedVersion field comment.
func (c *httpTransport) setNegotiatedVersion(v string) {
	c.mu.Lock()
	c.negotiatedVersion = v
	c.mu.Unlock()
}

// Name returns the upstream's stable identifier.
func (c *httpTransport) Name() string { return c.name }

// Done reports absence: HTTP has no persistent process to watch, so it honestly
// returns ok=false rather than faking a channel that would never fire.
// Unreachability of an HTTP upstream is caught at the next call instead.
func (c *httpTransport) Done() (<-chan struct{}, bool) { return nil, false }

// StderrTail reports absence, like Done: HTTP has no child process and hence
// no stderr to tail, so it honestly returns ok=false instead of a faked empty
// tail.
func (c *httpTransport) StderrTail() ([]string, bool) { return nil, false }

// Close releases resources: it stops the SSE stream goroutine (cancel, then
// wait — streamDone is pre-closed when the stream never launched, see the
// field comment) and idles the transport's connections. There is no child
// process to reap; the DELETE session-termination request is best-effort and
// deliberately not sent in the MVP (see docs/MCP_NOTES.md §8). Safe to call
// more than once, including concurrently: cancel is idempotent, the done
// channel stays closed, and CloseIdleConnections is safe for concurrent use.
func (c *httpTransport) Close() error {
	c.streamCancel()
	c.mu.Lock()
	done := c.streamDone
	c.mu.Unlock()
	<-done
	if t, ok := c.client.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}

// startSSEStream launches the long-lived GET SSE stream goroutine (Round 13),
// on which the upstream may push server-initiated notifications. Called by
// Conn.Initialize after a successful handshake — the session id (if any) is
// known by then, and the spec keys the stream to it. Idempotent: repeated
// Initialize calls (session-expiry re-init) and a racing Close are both
// handled under mu — at most one goroutine ever runs, and never after Close.
//
// The stream is opened when ANY consumer listens to it — notifications or
// server-initiated requests (Stage 17b); with both callbacks nil nobody listens
// and no stream is opened at all. Gating on onNotify alone would leave a
// request-only caller without a stream. That combination is test-only today
// (the registry always passes both), but a gate that is correct only because of
// what its call sites happen to pass is not a gate — and for the same reason
// each branch of dispatchStreamEvent guards its OWN callback.
func (c *httpTransport) startSSEStream() {
	if c.onNotify == nil && c.onRequest == nil {
		return
	}
	c.mu.Lock()
	if c.streamStarted || c.streamCtx.Err() != nil {
		c.mu.Unlock()
		return
	}
	c.streamStarted = true
	done := make(chan struct{})
	c.streamDone = done
	c.mu.Unlock()
	go func() {
		defer close(done)
		c.runSSEStream(c.streamCtx)
	}()
}

// runSSEStream is the stream goroutine's body: connect, read events until the
// stream drops, reconnect with exponential backoff and Last-Event-ID — the
// HTTP analogue of stdio's readLoop plus the supervisor's restart loop, scaled
// down to one goroutine. Policy:
//
//   - the upstream EXPLICITLY refusing the stream (405/404 — the spec says a
//     server that does not offer the GET stream MUST answer 405 — or a 200
//     whose Content-Type is not an event stream) means this OPTIONAL feature
//     is simply absent: one Info log and out, never a retry storm against a
//     server that said no;
//   - everything else — transport-level errors, transient statuses (5xx,
//     429…), drops of an open stream (EOF, network errors) — is retried with
//     exponential backoff, the very FIRST attempt included: a transient
//     failure at startup says nothing about SSE support and must not disable
//     server push until the gateway restarts (found by review). Reconnects
//     resume via Last-Event-ID so the server can replay missed events; a
//     stream that stayed up past sseStreamMaxBackoff resets the backoff
//     (stable again);
//   - ctx (the connection's streamCtx) cancelling — i.e. Close — ends the
//     loop immediately, mid-read or mid-backoff.
func (c *httpTransport) runSSEStream(ctx context.Context) {
	lastEventID := ""
	backoff := sseStreamInitialBackoff
	for {
		start := time.Now()
		id, opened, err := c.streamSSEOnce(ctx, lastEventID)
		lastEventID = id
		if ctx.Err() != nil {
			return // Close (or shutdown), not an upstream failure
		}
		if !opened && err == nil {
			c.log.Info("upstream does not offer an SSE stream; server-initiated notifications disabled",
				"upstream", c.name)
			// This branch is terminal — the loop never comes back — so the
			// callback fires at most once per connection, no throttling needed.
			if c.onStreamUnavailable != nil {
				c.onStreamUnavailable()
			}
			return
		}
		if opened && time.Since(start) >= sseStreamMaxBackoff {
			backoff = sseStreamInitialBackoff
		}
		c.log.Debug("upstream SSE stream ended, reconnecting",
			"upstream", c.name, "backoff", backoff, "err", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= sseStreamBackoffFactor
		if backoff > sseStreamMaxBackoff {
			backoff = sseStreamMaxBackoff
		}
	}
}

// streamSSEOnce performs one GET attempt against the endpoint and, if the
// server answers with an event stream, reads it to its end, dispatching every
// event to dispatchStreamEvent. opened reports whether a stream was actually
// established (200 + text/event-stream). An EXPLICIT refusal (405/404, or a
// 200 that is not an event stream) returns opened=false with a nil err — the
// caller's "the server said no" signal; any other failure (transport error,
// transient non-OK status) returns opened=false with the error, and the
// caller retries it with backoff. newLastID carries the most recent SSE id
// seen (or the passed-in one), for the reconnect's Last-Event-ID header.
func (c *httpTransport) streamSSEOnce(ctx context.Context, lastEventID string) (newLastID string, opened bool, err error) {
	newLastID = lastEventID
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return newLastID, false, fmt.Errorf("upstream %q: build SSE stream request: %w", c.name, err)
	}
	httpReq.Header.Set("Accept", "text/event-stream")

	// Same live header snapshot as post: the negotiated protocol version and
	// the CURRENT session id (a session-expiry re-init may have replaced it
	// since the previous attempt).
	c.mu.Lock()
	sid := c.sessionID
	pv := c.negotiatedVersion
	c.mu.Unlock()
	if pv == "" {
		pv = mcp.ProtocolVersion
	}
	httpReq.Header.Set("MCP-Protocol-Version", pv)
	if sid != "" {
		httpReq.Header.Set("Mcp-Session-Id", sid)
	}
	if lastEventID != "" {
		httpReq.Header.Set("Last-Event-ID", lastEventID)
	}
	for k, v := range c.headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return newLastID, false, fmt.Errorf("upstream %q: GET %s: %w", c.name, redactedEndpoint(c.endpoint), err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusNotFound && sid != "":
		// 404 to a request that CARRIED a session id is not "no stream here" —
		// per the spec it means that session is gone and the client must
		// re-initialize (same reading as call's hadSession branch). Reported as
		// an ERROR so runSSEStream backs off and retries instead of giving up
		// forever: the next attempt re-reads the session id, which a
		// CallTool-triggered re-initialize will have refreshed by then.
		//
		// Lumping this in with 405 below meant server push died permanently
		// after any session expiry, and died INVISIBLY — tools/call kept
		// recovering on its own, so nothing looked broken while
		// elicitation/sampling/roots from this upstream could never arrive
		// again (found by review; harmless before Stage 17b, when the stream
		// only carried notifications).
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return newLastID, false, fmt.Errorf("upstream %q: GET %s: %w",
			c.name, redactedEndpoint(c.endpoint), errSessionExpired)
	case resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotFound:
		// The explicit "I do not offer the GET stream" answer (the spec says
		// MUST be 405; a 404 with no session id in play is the same statement
		// from servers that route the method away entirely). Drain a small
		// bounded remainder so the connection returns to the keep-alive pool.
		// opened=false with nil err is the caller's "the server said no" signal.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return newLastID, false, nil
	case resp.StatusCode != http.StatusOK:
		// Any other status (503, 500, 429…) is a transient server condition,
		// NOT a statement about SSE support — report it as an error so the
		// caller retries with backoff instead of disabling the stream forever
		// (found by review: a 503 on the very first attempt killed server
		// push until the gateway restarted).
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return newLastID, false, fmt.Errorf("upstream %q: GET %s: unexpected status %d",
			c.name, redactedEndpoint(c.endpoint), resp.StatusCode)
	case !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream"):
		// 200, but the body is not an event stream: whatever answers GETs
		// here is not an MCP SSE endpoint — a clean "no", like 405.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return newLastID, false, nil
	}

	lastID, err := c.scanSSEEvents(resp.Body, func(payload []byte) bool {
		c.dispatchStreamEvent(payload)
		return true
	})
	if lastID != "" {
		newLastID = lastID
	}
	if err != nil {
		err = fmt.Errorf("upstream %q: read SSE stream: %w", c.name, err)
	}
	return newLastID, true, err
}

// dispatchStreamEvent decodes one event payload from the long-lived GET stream
// and classifies it EXACTLY as stdio's readLoop classifies a line from the
// child's stdout — same mcp.Message predicates, same order — so the registry
// receives identical input from both transports and no policy lives in either
// one. The order is load-bearing: a malformed hybrid must be rejected before
// the response branch, or a {method, id, result} frame would pass as a response.
//
// Notifications go to onNotify (Round 13); server-initiated REQUESTS go to
// onRequest (Stage 17b). RESPONSES are dropped: per the spec an answer to one
// of our POSTs comes back on that POST's own body, so a response frame here has
// no waiter to deliver it to — the mirror of stdio's "response with no waiter".
// Each branch guards its own callback, because the stream may have been opened
// for the other consumer alone (see startSSEStream).
func (c *httpTransport) dispatchStreamEvent(payload []byte) {
	var msg mcp.Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		c.log.Debug("upstream SSE stream frame not JSON-RPC (ignored)", "upstream", c.name, "err", err)
		return
	}
	switch {
	case msg.IsMalformedHybrid():
		// Both a method and a result/error — a shape JSON-RPC 2.0 does not
		// allow. Every classifier in the project (client dispatcher, demo stub,
		// stdio reader) rejects it; this one must not disagree.
		c.log.Warn("upstream sent malformed hybrid message (method and result/error together), dropping",
			"upstream", c.name, "method", msg.Method, "id", string(msg.ID))
	case msg.IsResponse():
		c.log.Debug("upstream SSE stream carried a response with no waiter (ignored)",
			"upstream", c.name, "id", string(msg.ID))
	case msg.IsNotification():
		c.log.Debug("upstream notification", "upstream", c.name, "method", msg.Method)
		if c.onNotify != nil {
			c.onNotify(msg.Method, msg.Params)
		}
	default:
		// A request FROM the upstream (method plus a non-null id). Forwarded
		// verbatim; the registry decides whether it proxies that method at all.
		if c.onRequest != nil {
			c.onRequest(msg.Method, msg.ID, msg.Params)
		} else {
			c.log.Debug("upstream request ignored", "upstream", c.name, "method", msg.Method)
		}
	}
}

// call POSTs one JSON-RPC request and returns the matching response. It handles
// both response content types the spec allows: a single application/json object,
// or a text/event-stream SSE stream from which we pull the response frame whose
// id matches our request.
func (c *httpTransport) call(ctx context.Context, method string, params json.RawMessage) (*mcp.Message, error) {
	id := mcp.IntID(c.nextID.Add(1))
	req := mcp.NewRequest(id, method, params)

	httpResp, err := c.post(ctx, req)
	if err != nil {
		return nil, err
	}
	// Close without draining HERE: for the SSE branch below, the upstream is
	// allowed to keep the stream open past the response we care about (the spec
	// permits further server-initiated messages on it), so draining to EOF would
	// block until the call's timeout on a chatty upstream instead of returning
	// immediately once our response is found. net/http.Response.Body.Close on an
	// unread stream still tears the connection down cleanly, it just forgoes
	// keep-alive reuse for this one request (found by independent /code-review).
	// The JSON branch, by contrast, DOES drain its (bounded) remainder before
	// returning, so those connections stay reusable — see below.
	defer func() { _ = httpResp.Body.Close() }()

	// initialize may hand back a session id we must echo from now on.
	if method == mcp.MethodInitialize {
		if sid := httpResp.Header.Get("Mcp-Session-Id"); sid != "" {
			c.mu.Lock()
			c.sessionID = sid
			c.mu.Unlock()
		}
	}

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		if httpResp.StatusCode == http.StatusNotFound {
			// 404 on a request that carried our session id means the server
			// expired the session (spec: the client MUST re-initialize). Keeping
			// the dead id would poison every future request until a gateway
			// restart — clear it, and report the distinctive error so
			// Conn.CallTool can re-initialize and retry once.
			c.mu.Lock()
			hadSession := c.sessionID != ""
			c.sessionID = ""
			c.mu.Unlock()
			if hadSession {
				return nil, fmt.Errorf("upstream %q: %s: %w", c.name, method, errSessionExpired)
			}
		}
		return nil, fmt.Errorf("upstream %q: %s: HTTP %d", c.name, method, httpResp.StatusCode)
	}

	ct := httpResp.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(ct, "text/event-stream"):
		return c.readSSEResponse(httpResp.Body, id)
	default:
		// application/json (or unspecified): a single JSON-RPC object. Same
		// sanity checks the SSE branch applies: the body must actually BE a
		// response (carry result or error — note an explicit "result": null
		// still counts, RawMessage keeps the null bytes) and answer OUR id, not
		// echo something unrelated.
		var msg mcp.Message
		limited := io.LimitReader(httpResp.Body, maxHTTPResponseBytes+1)
		if err := json.NewDecoder(limited).Decode(&msg); err != nil {
			return nil, fmt.Errorf("upstream %q: %s: decode response: %w", c.name, method, err)
		}
		if !msg.IsResponse() || !idsEqual(msg.ID, id) {
			return nil, fmt.Errorf("upstream %q: %s: body is not a JSON-RPC response to id %s (got id %q)", c.name, method, id, msg.ID)
		}
		// Drain the remainder (trailing whitespace after the JSON object) so
		// net/http can return the connection to the keep-alive pool — Close on
		// an un-drained body forfeits reuse. Reading from limited, not the raw
		// body, keeps the drain bounded by the same size cap as the decode.
		// JSON branch only: the SSE branch deliberately abandons its stream
		// (see the deferred Close above).
		_, _ = io.Copy(io.Discard, limited)
		return &msg, nil
	}
}

// notify POSTs a one-way JSON-RPC notification (no id). The server answers 202
// Accepted with no body; any 2xx is treated as success and the body drained.
func (c *httpTransport) notify(ctx context.Context, method string, params json.RawMessage) error {
	httpResp, err := c.post(ctx, mcp.NewNotification(method, params))
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, httpResp.Body)
		_ = httpResp.Body.Close()
	}()
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return fmt.Errorf("upstream %q: notify %s: HTTP %d", c.name, method, httpResp.StatusCode)
	}
	return nil
}

// respond POSTs one gateway-authored RESPONSE to a request this upstream itself
// initiated (Stage 17b). It goes through post like call and notify, so it
// carries the same session id, negotiated protocol version and static auth
// headers — a response belongs to the session that asked, and stripping any of
// those would make the server unable to correlate it. Per the spec the server
// answers a response body with 202 and no content; any 2xx is accepted, matching
// notify.
//
// There are deliberately NO retries of any kind:
//
//   - 404 with a live session means the server forgot the session that asked.
//     errSessionExpired is reported, but — unlike CallTool — nothing is
//     re-initialized or re-sent: a fresh Initialize opens a NEW session in
//     which this request id was never asked, so the retry would deliver an
//     answer to a question nobody posed. CallTool may retry because it repeats
//     OUR request, which a 404 proves never ran. The session id itself is left
//     alone, see below;
//   - everything else (5xx, timeouts, network errors, a cancelled streamCtx)
//     is returned as-is. The only caller — the registry's
//     writeUpstreamResponse — already Warn-logs the failure, and an upstream
//     left without an answer falls back on its own timeout, exactly as it does
//     when a stdio gateway dies mid-answer.
func (c *httpTransport) respond(msg *mcp.Message) error {
	ctx, cancel := context.WithTimeout(c.streamCtx, respondPostTimeout)
	defer cancel()

	resp, err := c.post(ctx, msg)
	if err != nil {
		return err
	}
	defer func() {
		// Bounded drain before Close so net/http can reuse the connection: a
		// 202 body is empty or tiny, and reading an unbounded one from an
		// upstream that decided to stream here would only burn the deadline.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound {
		c.mu.Lock()
		hadSession := c.sessionID != ""
		c.mu.Unlock()
		if hadSession {
			// The session that asked the question is gone. Reported
			// distinctively for the log, but — unlike call — the stale id is
			// deliberately KEPT (Stage 17b §13, correcting the plan): call's
			// recovery only fires for a request that CARRIED a session id, so
			// clearing it here would turn the next tools/call into a plain 404
			// with no re-initialize and brick the upstream until a gateway
			// restart. Leaving the id in place lets that next call take the
			// existing expiry path and recover on its own.
			return fmt.Errorf("upstream %q: respond to request id %s: %w", c.name, msg.ID, errSessionExpired)
		}
	}
	return fmt.Errorf("upstream %q: respond to request id %s: HTTP %d", c.name, msg.ID, resp.StatusCode)
}

// post marshals msg and POSTs it to the MCP endpoint with the headers the spec
// requires (Accept for both content types, the negotiated protocol version, the
// session id once known) plus any static per-upstream headers (auth).
func (c *httpTransport) post(ctx context.Context, msg *mcp.Message) (*http.Response, error) {
	body, err := mcp.Encode(msg)
	if err != nil {
		return nil, fmt.Errorf("upstream %q: encode request: %w", c.name, err)
	}
	// Encode appends a trailing newline (stdio framing); HTTP does not need it,
	// but a trailing newline in a JSON body is harmless, so we keep Encode as
	// the single marshal path rather than duplicating json.Marshal here.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("upstream %q: build request: %w", c.name, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/event-stream")

	c.mu.Lock()
	sid := c.sessionID
	pv := c.negotiatedVersion
	c.mu.Unlock()
	if pv == "" {
		// Before the handshake completes (or if the upstream never stated a
		// version) advertise the version we ourselves propose in initialize.
		pv = mcp.ProtocolVersion
	}
	httpReq.Header.Set("MCP-Protocol-Version", pv)
	if sid != "" {
		httpReq.Header.Set("Mcp-Session-Id", sid)
	}
	// Static per-upstream headers (auth) last, so a config can override defaults
	// if it ever needs to. These carry secrets — never logged.
	for k, v := range c.headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		// The endpoint is REDACTED in the error text: an operator may have put
		// credentials into the URL itself (https://user:pass@host/mcp — the one
		// config field that carries no env-expansion), and this error string
		// ends up in the metadata-only audit log via err.Error().
		return nil, fmt.Errorf("upstream %q: POST %s: %w", c.name, redactedEndpoint(c.endpoint), err)
	}
	return resp, nil
}

// redactedEndpoint returns endpoint safe for error messages and logs: any
// userinfo password is masked via url.URL.Redacted ("user:xxxxx@host"). The
// real endpoint is still used for the actual network request — only the TEXT
// that can reach logs is redacted. An unparsable endpoint is returned as-is
// (it cannot carry parseable userinfo either).
func redactedEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	return u.Redacted()
}

// idsEqual reports whether two raw JSON-RPC ids are the same, tolerating
// surrounding whitespace. Shared by the JSON and SSE response paths of call.
//
// The comparison is deliberately strict — exact bytes, no type coercion. Per
// JSON-RPC 2.0 the id is not opaque: the request id "MUST contain a String,
// Number, or NULL value" and the server "MUST reply with the same value in
// the Response object" — so an upstream echoing our numeric id as a string
// ("7" for 7) violates the protocol and is rightly rejected here. We always
// mint ids via mcp.IntID, whose canonical integer bytes a conforming echo
// reproduces exactly. Before this check existed, ANY decodable JSON object
// passed as a success (even one without result/error) — such non-conforming
// upstreams were never actually supported, only unnoticed.
func idsEqual(a, b json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b))
}

// readSSEResponse reads an SSE stream and returns the first JSON-RPC message
// whose id matches want (the response to our request). Per the spec the server
// MAY interleave unrelated requests/notifications before the response; those
// carry a different id (or none) and are skipped. The stream is abandoned (the
// deferred Body.Close in call closes it) once the response is found. The SSE
// framing itself lives in scanSSEEvents, shared with the long-lived GET stream.
func (c *httpTransport) readSSEResponse(body io.Reader, want json.RawMessage) (*mcp.Message, error) {
	var found *mcp.Message
	_, err := c.scanSSEEvents(body, func(payload []byte) bool {
		var msg mcp.Message
		if err := json.Unmarshal(payload, &msg); err != nil {
			c.log.Debug("upstream SSE frame not JSON-RPC (ignored)", "upstream", c.name, "err", err)
			return true
		}
		if msg.IsResponse() && idsEqual(msg.ID, want) {
			found = &msg
			return false // stop scanning; the stream is abandoned by call's deferred Close
		}
		switch {
		case msg.IsMalformedHybrid():
			// Rejected here too, so all four classifiers agree (see
			// dispatchStreamEvent). Checked before the request branch: a
			// {method, id, result} frame is not a request either.
			c.log.Warn("upstream sent malformed hybrid message (method and result/error together), dropping",
				"upstream", c.name, "method", msg.Method, "id", string(msg.ID))
		case msg.IsRequest() && c.onRequest != nil:
			// A server-initiated REQUEST interleaved before our response — per
			// the spec the stream of a POST is precisely where a server puts
			// requests RELATED to the call in flight, and that is where real
			// SDK servers raise an elicitation/create from inside a tools/call
			// (Stage 17b). Forward it and keep scanning: our own response is
			// still to come, and the answer travels back as a SEPARATE POST
			// from the registry's goroutine — concurrent requests are normal
			// for net/http, and the server needs that answer to finish the
			// very call this stream belongs to.
			c.onRequest(msg.Method, msg.ID, msg.Params)
		default:
			// Notifications tied to the in-flight call (progress etc.) are
			// deliberately not proxied (Round 13 / MCP_NOTES §7), and a
			// response to somebody else's id has no waiter here.
			c.log.Debug("upstream SSE interleaved message ignored", "upstream", c.name, "method", msg.Method)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if found != nil {
		return found, nil
	}
	return nil, fmt.Errorf("upstream %q: SSE stream ended without a response for id %s", c.name, want)
}

// scanSSEEvents is the SSE framing scanner shared by readSSEResponse (the
// response-on-a-POST branch of call) and streamSSEOnce (the long-lived GET
// stream, Round 13). It reads body line by line and invokes handle once per
// event with the event's accumulated data payload; handle returning false
// stops the scan early with a nil error.
//
// SSE framing (WHATWG): events are blank-line-separated; consecutive "data:"
// lines within one event are CONCATENATED (joined with "\n") into a single
// payload, dispatched at the event boundary; a single leading space after any
// field's colon is stripped, further whitespace is payload. MCP puts one
// JSON-RPC message per event's data. "id:" lines are tracked — committed as
// soon as the line is processed, per the spec, so even an id-only keepalive
// frame advances it — and the most recent value is returned as lastID for the
// reconnect's Last-Event-ID header. Other field lines (event:, retry:,
// comments) are ignored. Events with an empty data buffer are not dispatched
// (per the spec). End of stream terminates the final event even without a
// trailing blank line (a lenient reading — the spec discards an unterminated
// event, but a well-formed message should not be lost to a missing final
// newline).
//
// Size limits: the scanner caps each LINE at maxHTTPResponseBytes, and the
// accumulated per-event payload is capped at the same limit, so many small
// lines cannot add up past it. The payload slice handed to handle is freshly
// allocated per event (data = nil, not data[:0]): the GET stream's handler
// forwards json.RawMessage views of it to the registry, which retains them
// beyond the callback (notification fan-out), so the buffer must never be
// reused underneath a previous event.
func (c *httpTransport) scanSSEEvents(body io.Reader, handle func(payload []byte) bool) (lastID string, err error) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64*1024), maxHTTPResponseBytes)

	var data []byte // this event's accumulated data payload

	// dispatch hands the accumulated event data to handle and resets the
	// buffer; it reports whether scanning should continue.
	dispatch := func() bool {
		payload := data
		data = nil // fresh allocation next event — see the doc comment
		if len(payload) == 0 {
			return true
		}
		return handle(payload)
	}

	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			// Blank line: end of event — dispatch what was accumulated.
			if !dispatch() {
				return lastID, nil
			}
			continue
		}
		if id, ok := strings.CutPrefix(line, "id:"); ok {
			lastID = strings.TrimPrefix(id, " ")
			continue
		}
		d, ok := strings.CutPrefix(line, "data:")
		if !ok {
			// event:/retry:/comment lines do not affect the data buffer.
			continue
		}
		d = strings.TrimPrefix(d, " ")
		if len(data) > 0 {
			data = append(data, '\n')
		}
		data = append(data, d...)
		if len(data) > maxHTTPResponseBytes {
			return lastID, fmt.Errorf("upstream %q: SSE event data exceeds %d bytes", c.name, maxHTTPResponseBytes)
		}
	}
	if err := sc.Err(); err != nil {
		return lastID, fmt.Errorf("upstream %q: read SSE: %w", c.name, err)
	}
	dispatch()
	return lastID, nil
}
