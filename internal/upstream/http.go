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

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// maxHTTPResponseBytes bounds a single upstream HTTP response body — the
// application/json branch of call() used to decode with no limit at all
// (found by code review) while the SSE branch already capped at this same
// size, matching internal/mcp's line cap for the stdio side. A misbehaving
// or malicious upstream could otherwise force the gateway to buffer an
// unbounded response into memory.
const maxHTTPResponseBytes = 32 << 20 // 32 MiB

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
// Unlike stdioTransport there is no long-lived reader goroutine: HTTP is
// request/response, so each Call owns its own round-trip and id-demultiplexing
// is unnecessary. Concurrency safety comes from net/http (safe for concurrent
// use) plus a mutex guarding the session id.
//
// KNOWN LIMITATION (Round 2): cancellation is NOT forwarded to HTTP upstreams.
// stdioTransport.call sends a best-effort notifications/cancelled down the
// same pipe when its ctx is cancelled mid-call; here there is no live channel
// tied to the in-flight request — a cancelled ctx simply aborts the HTTP
// round-trip (net/http cancels the request), and telling the server "stop the
// work" would require a SEPARATE POST racing the one being torn down, with no
// guarantee the server correlates them. Progress notifications from HTTP
// upstreams are likewise not surfaced (no long-lived reader to receive them;
// SSE frames other than the awaited response are skipped in post).
type httpTransport struct {
	name     string
	endpoint string
	log      *slog.Logger
	client   *http.Client

	// headers are static per-upstream headers (typically Authorization). They
	// are applied to every request and MUST NOT be logged (they carry secrets).
	headers map[string]string

	nextID atomic.Int64

	mu        sync.Mutex
	sessionID string // Mcp-Session-Id assigned by the server on initialize, if any
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
func StartHTTP(log *slog.Logger, name, endpoint string, headers map[string]string, client *http.Client, gatewayVersion string) *Conn {
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
	return &Conn{
		transport: &httpTransport{
			name:     name,
			endpoint: endpoint,
			log:      log,
			client:   client,
			headers:  headers,
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

// Close releases resources. HTTP is connectionless from our side (no child
// process, no reader goroutine), so there is nothing to tear down beyond
// idling the transport's connections; the DELETE session-termination request is
// best-effort and deliberately not sent in the MVP (see docs/MCP_NOTES.md §8).
func (c *httpTransport) Close() error {
	if t, ok := c.client.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
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
// deferred Body.Close in call closes it) once the response is found.
//
// SSE framing (WHATWG): events are blank-line-separated; consecutive "data:"
// lines within one event are CONCATENATED (joined with "\n") into a single
// payload, dispatched at the event boundary. MCP puts one JSON-RPC message per
// event's data, so the accumulated payload of each event is parsed as one
// Message — parsing each data: line on its own (the previous behaviour) broke
// any upstream that splits a message across several data: lines.
func (c *httpTransport) readSSEResponse(body io.Reader, want json.RawMessage) (*mcp.Message, error) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64*1024), maxHTTPResponseBytes)

	var data []byte // this event's accumulated data payload

	// dispatch parses the accumulated event data as one JSON-RPC message and
	// resets the buffer. It returns the message when it is the response to our
	// request; anything else (empty event, non-JSON payload, interleaved
	// server->client traffic — not handled in the MVP, MCP_NOTES §7) is logged
	// where useful and skipped.
	dispatch := func() *mcp.Message {
		payload := data
		data = data[:0]
		if len(payload) == 0 {
			return nil
		}
		var msg mcp.Message
		if err := json.Unmarshal(payload, &msg); err != nil {
			c.log.Debug("upstream SSE frame not JSON-RPC (ignored)", "upstream", c.name, "err", err)
			return nil
		}
		if msg.IsResponse() && idsEqual(msg.ID, want) {
			return &msg
		}
		c.log.Debug("upstream SSE interleaved message ignored", "upstream", c.name, "method", msg.Method)
		return nil
	}

	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			// Blank line: end of event — dispatch what was accumulated.
			if msg := dispatch(); msg != nil {
				return msg, nil
			}
			continue
		}
		d, ok := strings.CutPrefix(line, "data:")
		if !ok {
			// event:/id:/retry:/comment lines do not affect the data buffer.
			continue
		}
		// Per the SSE spec, a single leading space after the colon is stripped;
		// further whitespace is part of the payload.
		d = strings.TrimPrefix(d, " ")
		if len(data) > 0 {
			data = append(data, '\n')
		}
		data = append(data, d...)
		if len(data) > maxHTTPResponseBytes {
			// The scanner caps each LINE; this caps the event's accumulated
			// payload, so many small lines cannot add up past the same limit.
			return nil, fmt.Errorf("upstream %q: SSE event data exceeds %d bytes", c.name, maxHTTPResponseBytes)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("upstream %q: read SSE: %w", c.name, err)
	}
	// End of stream terminates the final event even without a trailing blank
	// line (a lenient reading — the spec discards an unterminated event, but a
	// well-formed response should not be lost to a missing final newline).
	if msg := dispatch(); msg != nil {
		return msg, nil
	}
	return nil, fmt.Errorf("upstream %q: SSE stream ended without a response for id %s", c.name, want)
}
