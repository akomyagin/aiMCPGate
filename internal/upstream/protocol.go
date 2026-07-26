package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// maxPaginationPages bounds the cursor-following loops of ListTools and
// ListResources. Without a cap a malicious or broken upstream that keeps
// returning a non-empty nextCursor would spin the loop forever (only the
// caller's ctx timeout would eventually stop it — and nothing here guarantees
// the caller set one). The limit is deliberately generous: no legitimate
// catalog needs a thousand pages.
const maxPaginationPages = 1000

// transport is the minimal surface a concrete upstream connection must
// provide for the shared MCP protocol logic below. Neither stdioTransport
// nor httpTransport knows anything about Initialize/ListTools/ListResources/
// CallTool — that logic lives once, here, against this interface.
type transport interface {
	call(ctx context.Context, method string, params json.RawMessage) (*mcp.Message, error)
	notify(ctx context.Context, method string, params json.RawMessage) error
	Name() string
	Close() error
	// Done reports the "process died" channel for transports backed by a
	// long-lived process that can die independently of any call (stdio). ok
	// is false when there is no such channel (HTTP has no persistent
	// process to watch) — an honest declaration of absence, not a faked
	// channel that would never fire.
	Done() (ch <-chan struct{}, ok bool)
	// StderrTail reports the most recent stderr lines of a transport backed
	// by a child process (stdio) — post-mortem material the supervisor logs
	// when that process crashes. ok is false when there is no process and
	// hence no stderr to tail (HTTP), mirroring Done's honest absence.
	StderrTail() (lines []string, ok bool)
}

// Conn is a live connection to one upstream MCP server, regardless of
// transport (stdio or HTTP). Name/Close/Done/call/notify are promoted
// straight from whichever transport it wraps; Initialize/ListTools/
// ListResources/CallTool are implemented once, directly below, against
// c.transport — there is exactly one caller of each, so there is no separate
// package-level function to share between callers that no longer exist.
type Conn struct {
	transport

	// gatewayVersion is the gateway's build version (from -ldflags in main),
	// reported as clientInfo.version in the initialize handshake so upstreams
	// see the real binary version instead of a hardcoded literal. Set by
	// StartStdio/StartHTTP; empty falls back to the dev placeholder.
	gatewayVersion string
}

// Initialize performs the MCP handshake against this upstream: sends an
// `initialize` request and, on success, the `notifications/initialized`
// notification. It returns the server's InitializeResult.
func (c *Conn) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	version := c.gatewayVersion
	if version == "" {
		version = "0.0.0-dev"
	}
	// gatewayClientInfo identifies aiMCPGate to the upstream during the
	// handshake below — used nowhere else, so it lives here rather than at
	// package scope.
	gatewayClientInfo := mcp.Implementation{
		Name:    "aiMCPGate",
		Version: version,
	}
	params := mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      gatewayClientInfo,
	})

	resp, err := c.transport.call(ctx, mcp.MethodInitialize, params)
	if err != nil {
		return nil, fmt.Errorf("upstream %q: initialize: %w", c.Name(), err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("upstream %q: initialize rejected: %w", c.Name(), resp.Error)
	}

	var res mcp.InitializeResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		return nil, fmt.Errorf("upstream %q: decode initialize result: %w", c.Name(), err)
	}

	// Hand the NEGOTIATED protocol version to the HTTP transport, which must
	// advertise it in the MCP-Protocol-Version header on every subsequent
	// request (before this, the package constant was sent even when the
	// upstream had agreed to a different version). A type assertion rather
	// than a new transport-interface method: stdio carries no headers and has
	// no use for the value, so the shared interface is not widened for the
	// need of exactly one transport.
	if ht, ok := c.transport.(*httpTransport); ok {
		ht.setNegotiatedVersion(res.ProtocolVersion)
	}

	if err := c.transport.notify(ctx, mcp.NotifInitialized, nil); err != nil {
		return nil, fmt.Errorf("upstream %q: send initialized: %w", c.Name(), err)
	}
	return &res, nil
}

// paginate is the cursor-following loop shared by ListTools and ListResources:
// build params carrying the cursor, call, decode, append, follow nextCursor —
// bounded by maxPaginationPages. makeParams and decode carry the two methods'
// only real differences (param/result types); transport and decode failures
// are wrapped here, but a JSON-RPC level error is returned UNWRAPPED as rpcErr
// so the caller can apply its own policy (ListResources treats method-not-found
// as an empty catalog; ListTools treats every error as fatal).
func paginate[T any](
	ctx context.Context,
	c *Conn,
	method string,
	makeParams func(cursor string) json.RawMessage,
	decode func(result json.RawMessage) (items []T, nextCursor string, err error),
) (all []T, rpcErr *mcp.Error, err error) {
	cursor := ""
	for page := 1; ; page++ {
		if page > maxPaginationPages {
			return nil, nil, fmt.Errorf("upstream %q: %s: exceeded %d pages, possible infinite pagination loop", c.Name(), method, maxPaginationPages)
		}
		var params json.RawMessage
		if cursor != "" {
			params = makeParams(cursor)
		}
		resp, err := c.transport.call(ctx, method, params)
		if err != nil {
			return nil, nil, fmt.Errorf("upstream %q: %s: %w", c.Name(), method, err)
		}
		if resp.Error != nil {
			return nil, resp.Error, nil
		}
		items, next, err := decode(resp.Result)
		if err != nil {
			return nil, nil, fmt.Errorf("upstream %q: decode %s: %w", c.Name(), method, err)
		}
		all = append(all, items...)
		if next == "" {
			return all, nil, nil
		}
		cursor = next
	}
}

// ListTools fetches the upstream's full tool catalog, following pagination via
// nextCursor until exhausted (bounded — see maxPaginationPages).
func (c *Conn) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	tools, rpcErr, err := paginate(ctx, c, mcp.MethodToolsList,
		func(cursor string) json.RawMessage {
			return mcp.MustParams(mcp.ToolsListParams{Cursor: cursor})
		},
		func(result json.RawMessage) ([]mcp.Tool, string, error) {
			var res mcp.ToolsListResult
			if err := json.Unmarshal(result, &res); err != nil {
				return nil, "", err
			}
			return res.Tools, res.NextCursor, nil
		})
	if err != nil {
		return nil, err
	}
	if rpcErr != nil {
		return nil, fmt.Errorf("upstream %q: tools/list error: %w", c.Name(), rpcErr)
	}
	return tools, nil
}

// ListResources fetches the upstream's resource catalog, following pagination
// (bounded — see maxPaginationPages). A method-not-found error (upstream
// declares no resources capability) is treated as an empty catalog rather than
// a hard failure.
func (c *Conn) ListResources(ctx context.Context) ([]mcp.Resource, error) {
	resources, rpcErr, err := paginate(ctx, c, mcp.MethodResourceList,
		func(cursor string) json.RawMessage {
			return mcp.MustParams(mcp.ResourceListParams{Cursor: cursor})
		},
		func(result json.RawMessage) ([]mcp.Resource, string, error) {
			var res mcp.ResourceListResult
			if err := json.Unmarshal(result, &res); err != nil {
				return nil, "", err
			}
			return res.Resources, res.NextCursor, nil
		})
	if err != nil {
		return nil, err
	}
	if rpcErr != nil {
		if rpcErr.Code == mcp.CodeMethodNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("upstream %q: resources/list error: %w", c.Name(), rpcErr)
	}
	return resources, nil
}

// ListPrompts fetches the upstream's prompt catalog, following pagination via
// nextCursor until exhausted (bounded — see maxPaginationPages). Unlike
// ListResources there is no method-not-found-as-empty branch: the registry
// calls this only for upstreams that DECLARED the prompts capability in their
// initialize response, so a prompts/list error here is a real failure for the
// caller to judge, never "this upstream simply has no prompts".
func (c *Conn) ListPrompts(ctx context.Context) ([]mcp.Prompt, error) {
	prompts, rpcErr, err := paginate(ctx, c, mcp.MethodPromptsList,
		func(cursor string) json.RawMessage {
			return mcp.MustParams(mcp.PromptsListParams{Cursor: cursor})
		},
		func(result json.RawMessage) ([]mcp.Prompt, string, error) {
			var res mcp.PromptsListResult
			if err := json.Unmarshal(result, &res); err != nil {
				return nil, "", err
			}
			return res.Prompts, res.NextCursor, nil
		})
	if err != nil {
		return nil, err
	}
	if rpcErr != nil {
		return nil, fmt.Errorf("upstream %q: prompts/list error: %w", c.Name(), rpcErr)
	}
	return prompts, nil
}

// GetPrompt forwards a prompts/get to the upstream. name is the ORIGINAL
// (un-namespaced) prompt name expected by the upstream — the
// namespace→original rewrite happens in the Registry, symmetric with CallTool.
// The response is returned verbatim (*mcp.Message): the gateway never parses
// the prompt's description/messages.
func (c *Conn) GetPrompt(ctx context.Context, name string, arguments json.RawMessage) (*mcp.Message, error) {
	params := mcp.MustParams(mcp.PromptsGetParams{Name: name, Arguments: arguments})
	return c.transport.call(ctx, mcp.MethodPromptsGet, params)
}

// CallTool forwards a tools/call to the upstream. name is the ORIGINAL
// (un-namespaced) tool name expected by the upstream. meta is the client's
// optional `_meta` object (progressToken etc.), forwarded verbatim — nil when
// the client sent none, so the upstream sees exactly what the client sent.
//
// If the transport reports an expired HTTP session (the server answered 404 to
// a request carrying Mcp-Session-Id — per the spec it may drop a session at
// any time and the client MUST start a new one), CallTool re-runs the
// initialize handshake once and retries the call. The retry is safe: a 404
// means the upstream refused the request outright, so the tool was never
// executed and cannot run twice. Exactly one retry — a second expiry in a row
// is a genuinely broken upstream, not an expired session.
func (c *Conn) CallTool(ctx context.Context, name string, arguments, meta json.RawMessage) (*mcp.Message, error) {
	params := mcp.MustParams(mcp.ToolsCallParams{Name: name, Arguments: arguments, Meta: meta})
	resp, err := c.transport.call(ctx, mcp.MethodToolsCall, params)
	if err != nil && errors.Is(err, errSessionExpired) {
		// The transport already cleared the stale session id; a fresh
		// Initialize acquires a new one (captured inside its call).
		if _, ierr := c.Initialize(ctx); ierr != nil {
			return nil, fmt.Errorf("reinitialize after expired session: %w", ierr)
		}
		resp, err = c.transport.call(ctx, mcp.MethodToolsCall, params)
	}
	return resp, err
}
