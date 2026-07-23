package upstream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

// defaultHTTPClientTimeout bounds a whole request round-trip on the fallback
// client below. It mirrors config.DefaultCallTimeout (30s) without importing
// the config package here; it is a BACKSTOP under the per-call context
// deadline every current caller already applies (registry wraps each call in
// context.WithTimeout), not the primary timeout mechanism — it just guarantees
// a future caller that forgets a deadline cannot hang a request forever.
const defaultHTTPClientTimeout = 30 * time.Second

// defaultHTTPClient is used when StartHTTP's caller passes no client. It is a
// dedicated client — NOT http.DefaultClient, which is process-global, shared,
// and has no Timeout at all. Transport is set explicitly to http.DefaultTransport
// (the zero field would make Close's *http.Transport assertion fail, turning
// CloseIdleConnections into a silent no-op) so idle connections are actually
// released on Close.
var defaultHTTPClient = &http.Client{
	Timeout:   defaultHTTPClientTimeout,
	Transport: http.DefaultTransport,
}

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
}

// StartHTTP builds a Conn over an httpTransport for endpoint. It performs no
// network I/O — the handshake happens in Initialize, mirroring StartStdio which
// likewise defers the handshake. headers are extra per-request headers (e.g.
// Authorization); their values are treated as secrets and never logged.
func StartHTTP(log *slog.Logger, name, endpoint string, headers map[string]string, client *http.Client) *Conn {
	if client == nil {
		client = defaultHTTPClient
	}
	return &Conn{transport: &httpTransport{
		name:     name,
		endpoint: endpoint,
		log:      log,
		client:   client,
		headers:  headers,
	}}
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
	// Close without draining: for the SSE branch below, the upstream is allowed
	// to keep the stream open past the response we care about (the spec permits
	// further server-initiated messages on it), so draining to EOF here would
	// block until the call's timeout on a chatty upstream instead of returning
	// immediately once our response is found. net/http.Response.Body.Close on an
	// unread stream still tears the connection down cleanly, it just forgoes
	// keep-alive reuse for this one request (found by independent /code-review).
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
		return nil, fmt.Errorf("upstream %q: %s: HTTP %d", c.name, method, httpResp.StatusCode)
	}

	ct := httpResp.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(ct, "text/event-stream"):
		return c.readSSEResponse(httpResp.Body, id)
	default:
		// application/json (or unspecified): a single JSON-RPC object.
		var msg mcp.Message
		limited := io.LimitReader(httpResp.Body, maxHTTPResponseBytes+1)
		if err := json.NewDecoder(limited).Decode(&msg); err != nil {
			return nil, fmt.Errorf("upstream %q: %s: decode response: %w", c.name, method, err)
		}
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
	httpReq.Header.Set("MCP-Protocol-Version", mcp.ProtocolVersion)

	c.mu.Lock()
	sid := c.sessionID
	c.mu.Unlock()
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

// readSSEResponse reads an SSE stream and returns the first JSON-RPC message
// whose id matches want (the response to our request). Per the spec the server
// MAY interleave unrelated requests/notifications before the response; those
// carry a different id (or none) and are skipped. The stream is abandoned (the
// deferred Body.Close in call closes it) once the response is found.
//
// SSE framing (WHATWG): events are blank-line-separated; a "data:" line carries
// the payload. MCP puts one JSON-RPC message per event's data, so we parse each
// data payload as a Message.
func (c *httpTransport) readSSEResponse(body io.Reader, want json.RawMessage) (*mcp.Message, error) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64*1024), maxHTTPResponseBytes)
	for sc.Scan() {
		line := sc.Text()
		// Only data lines carry the JSON-RPC payload; event:/id:/retry:/comment
		// lines and blank separators are ignored for our purposes.
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "" {
			continue
		}
		var msg mcp.Message
		if err := json.Unmarshal([]byte(data), &msg); err != nil {
			c.log.Debug("upstream SSE frame not JSON-RPC (ignored)", "upstream", c.name, "err", err)
			continue
		}
		if msg.IsResponse() && bytes.Equal(bytes.TrimSpace(msg.ID), bytes.TrimSpace(want)) {
			return &msg, nil
		}
		// An interleaved server->client request/notification: not handled in the
		// MVP (no client-feature proxying — MCP_NOTES §7), so log and keep reading
		// for our response.
		c.log.Debug("upstream SSE interleaved message ignored", "upstream", c.name, "method", msg.Method)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("upstream %q: read SSE: %w", c.name, err)
	}
	return nil, fmt.Errorf("upstream %q: SSE stream ended without a response for id %s", c.name, want)
}
