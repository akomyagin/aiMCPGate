package upstream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

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
}

// Conn is a live connection to one upstream MCP server, regardless of
// transport (stdio or HTTP). Name/Close/Done/call/notify are promoted
// straight from whichever transport it wraps; Initialize/ListTools/
// ListResources/CallTool are implemented once, directly below, against
// c.transport — there is exactly one caller of each, so there is no separate
// package-level function to share between callers that no longer exist.
type Conn struct {
	transport
}

// Initialize performs the MCP handshake against this upstream: sends an
// `initialize` request and, on success, the `notifications/initialized`
// notification. It returns the server's InitializeResult.
func (c *Conn) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	// gatewayClientInfo identifies aiMCPGate to the upstream during the
	// handshake below — used nowhere else, so it lives here rather than at
	// package scope.
	gatewayClientInfo := mcp.Implementation{
		Name:    "aiMCPGate",
		Version: "0.1.0-dev",
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

	if err := c.transport.notify(ctx, mcp.NotifInitialized, nil); err != nil {
		return nil, fmt.Errorf("upstream %q: send initialized: %w", c.Name(), err)
	}
	return &res, nil
}

// ListTools fetches the upstream's full tool catalog, following pagination via
// nextCursor until exhausted.
func (c *Conn) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	var all []mcp.Tool
	cursor := ""
	for {
		var params json.RawMessage
		if cursor != "" {
			params = mcp.MustParams(mcp.ToolsListParams{Cursor: cursor})
		}
		resp, err := c.transport.call(ctx, mcp.MethodToolsList, params)
		if err != nil {
			return nil, fmt.Errorf("upstream %q: tools/list: %w", c.Name(), err)
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("upstream %q: tools/list error: %w", c.Name(), resp.Error)
		}
		var res mcp.ToolsListResult
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			return nil, fmt.Errorf("upstream %q: decode tools/list: %w", c.Name(), err)
		}
		all = append(all, res.Tools...)
		if res.NextCursor == "" {
			return all, nil
		}
		cursor = res.NextCursor
	}
}

// ListResources fetches the upstream's resource catalog, following pagination.
// A method-not-found error (upstream declares no resources capability) is
// treated as an empty catalog rather than a hard failure.
func (c *Conn) ListResources(ctx context.Context) ([]mcp.Resource, error) {
	var all []mcp.Resource
	cursor := ""
	for {
		var params json.RawMessage
		if cursor != "" {
			params = mcp.MustParams(mcp.ResourceListParams{Cursor: cursor})
		}
		resp, err := c.transport.call(ctx, mcp.MethodResourceList, params)
		if err != nil {
			return nil, fmt.Errorf("upstream %q: resources/list: %w", c.Name(), err)
		}
		if resp.Error != nil {
			if resp.Error.Code == mcp.CodeMethodNotFound {
				return nil, nil
			}
			return nil, fmt.Errorf("upstream %q: resources/list error: %w", c.Name(), resp.Error)
		}
		var res mcp.ResourceListResult
		if err := json.Unmarshal(resp.Result, &res); err != nil {
			return nil, fmt.Errorf("upstream %q: decode resources/list: %w", c.Name(), err)
		}
		all = append(all, res.Resources...)
		if res.NextCursor == "" {
			return all, nil
		}
		cursor = res.NextCursor
	}
}

// CallTool forwards a tools/call to the upstream. name is the ORIGINAL
// (un-namespaced) tool name expected by the upstream.
func (c *Conn) CallTool(ctx context.Context, name string, arguments json.RawMessage) (*mcp.Message, error) {
	params := mcp.MustParams(mcp.ToolsCallParams{Name: name, Arguments: arguments})
	return c.transport.call(ctx, mcp.MethodToolsCall, params)
}
