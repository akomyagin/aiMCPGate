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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// HTTPConn is a live connection to one upstream MCP server reached over the
// Streamable HTTP transport (MCP 2025-06-18). It is the second Upstream
// implementation alongside StdioConn; both satisfy the registry.Upstream
// interface, which is why no separate upstream.Conn interface is introduced
// here — the interface the project's "second implementation" rule anticipated
// already exists in the registry, and this type simply fills it. See
// docs/MCP_NOTES.md §8.
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
// Unlike StdioConn there is no long-lived reader goroutine: HTTP is
// request/response, so each Call owns its own round-trip and id-demultiplexing
// is unnecessary. Concurrency safety comes from net/http (safe for concurrent
// use) plus a mutex guarding the session id.
type HTTPConn struct {
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

// StartHTTP builds an HTTPConn for endpoint. It performs no network I/O — the
// handshake happens in Initialize, mirroring StartStdio which likewise defers
// the handshake. headers are extra per-request headers (e.g. Authorization);
// their values are treated as secrets and never logged.
func StartHTTP(log *slog.Logger, name, endpoint string, headers map[string]string, client *http.Client) *HTTPConn {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPConn{
		name:     name,
		endpoint: endpoint,
		log:      log,
		client:   client,
		headers:  headers,
	}
}

// Name returns the upstream's stable identifier.
func (c *HTTPConn) Name() string { return c.name }

// Initialize performs the MCP handshake over HTTP: POSTs an initialize request,
// captures any Mcp-Session-Id the server assigns, then POSTs the
// notifications/initialized notification (which the server answers with 202).
func (c *HTTPConn) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	params := mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      gatewayClientInfo,
	})

	resp, err := c.call(ctx, mcp.MethodInitialize, params)
	if err != nil {
		return nil, fmt.Errorf("upstream %q: initialize: %w", c.name, err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("upstream %q: initialize rejected: %w", c.name, resp.Error)
	}

	var res mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		return nil, fmt.Errorf("upstream %q: decode initialize result: %w", c.name, err)
	}

	if err := c.notify(ctx, mcp.NotifInitialized, nil); err != nil {
		return nil, fmt.Errorf("upstream %q: send initialized: %w", c.name, err)
	}
	return &res, nil
}

// ListTools fetches the upstream's full tool catalog, following pagination via
// nextCursor. Kept in sync with StdioConn.ListTools (same protocol logic, HTTP
// transport underneath).
func (c *HTTPConn) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	var all []mcp.Tool
	cursor := ""
	for {
		var params json.RawMessage
		if cursor != "" {
			params = mcp.MustParams(mcp.ToolsListParams{Cursor: cursor})
		}
		resp, err := c.call(ctx, mcp.MethodToolsList, params)
		if err != nil {
			return nil, fmt.Errorf("upstream %q: tools/list: %w", c.name, err)
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("upstream %q: tools/list error: %w", c.name, resp.Error)
		}
		var res mcp.ToolsListResult
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			return nil, fmt.Errorf("upstream %q: decode tools/list: %w", c.name, err)
		}
		all = append(all, res.Tools...)
		if res.NextCursor == "" {
			return all, nil
		}
		cursor = res.NextCursor
	}
}

// ListResources fetches the upstream's resource catalog, treating a
// method-not-found error as an empty catalog (same as StdioConn.ListResources).
func (c *HTTPConn) ListResources(ctx context.Context) ([]mcp.Resource, error) {
	var all []mcp.Resource
	cursor := ""
	for {
		var params json.RawMessage
		if cursor != "" {
			params = mcp.MustParams(mcp.ResourceListParams{Cursor: cursor})
		}
		resp, err := c.call(ctx, mcp.MethodResourceList, params)
		if err != nil {
			return nil, fmt.Errorf("upstream %q: resources/list: %w", c.name, err)
		}
		if resp.Error != nil {
			if resp.Error.Code == mcp.CodeMethodNotFound {
				return nil, nil
			}
			return nil, fmt.Errorf("upstream %q: resources/list error: %w", c.name, resp.Error)
		}
		var res mcp.ResourceListResult
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			return nil, fmt.Errorf("upstream %q: decode resources/list: %w", c.name, err)
		}
		all = append(all, res.Resources...)
		if res.NextCursor == "" {
			return all, nil
		}
		cursor = res.NextCursor
	}
}

// CallTool forwards a tools/call to the upstream. name is the ORIGINAL
// (un-namespaced) tool name the upstream expects.
func (c *HTTPConn) CallTool(ctx context.Context, name string, arguments json.RawMessage) (*mcp.Message, error) {
	params := mcp.MustParams(mcp.ToolsCallParams{Name: name, Arguments: arguments})
	return c.call(ctx, mcp.MethodToolsCall, params)
}

// Close releases resources. HTTP is connectionless from our side (no child
// process, no reader goroutine), so there is nothing to tear down beyond
// idling the transport's connections; the DELETE session-termination request is
// best-effort and deliberately not sent in the MVP (see docs/MCP_NOTES.md §8).
func (c *HTTPConn) Close() error {
	if t, ok := c.client.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}

// call POSTs one JSON-RPC request and returns the matching response. It handles
// both response content types the spec allows: a single application/json object,
// or a text/event-stream SSE stream from which we pull the response frame whose
// id matches our request.
func (c *HTTPConn) call(ctx context.Context, method string, params json.RawMessage) (*mcp.Message, error) {
	id := mcp.IntID(c.nextID.Add(1))
	req := mcp.NewRequest(id, method, params)

	httpResp, err := c.post(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, httpResp.Body) // drain so the connection can be reused
		_ = httpResp.Body.Close()
	}()

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
		if err := json.NewDecoder(httpResp.Body).Decode(&msg); err != nil {
			return nil, fmt.Errorf("upstream %q: %s: decode response: %w", c.name, method, err)
		}
		return &msg, nil
	}
}

// notify POSTs a one-way JSON-RPC notification (no id). The server answers 202
// Accepted with no body; any 2xx is treated as success and the body drained.
func (c *HTTPConn) notify(ctx context.Context, method string, params json.RawMessage) error {
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
func (c *HTTPConn) post(ctx context.Context, msg *mcp.Message) (*http.Response, error) {
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
		return nil, fmt.Errorf("upstream %q: POST %s: %w", c.name, c.endpoint, err)
	}
	return resp, nil
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
func (c *HTTPConn) readSSEResponse(body io.Reader, want json.RawMessage) (*mcp.Message, error) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64*1024), 32<<20) // match mcp codec's generous line cap
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
