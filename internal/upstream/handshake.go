package upstream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// gatewayClientInfo identifies aiMCPGate to upstream servers during initialize.
var gatewayClientInfo = mcp.Implementation{
	Name:    "aiMCPGate",
	Version: "0.1.0-dev",
}

// Initialize performs the MCP handshake against this upstream: sends an
// `initialize` request and, on success, the `notifications/initialized`
// notification. It returns the server's InitializeResult.
func (c *StdioConn) Initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	params := mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      gatewayClientInfo,
	})

	resp, err := c.Call(ctx, mcp.MethodInitialize, params)
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

	if err := c.Notify(mcp.NotifInitialized, nil); err != nil {
		return nil, fmt.Errorf("upstream %q: send initialized: %w", c.name, err)
	}
	return &res, nil
}

// ListTools fetches the upstream's full tool catalog, following pagination via
// nextCursor until exhausted.
func (c *StdioConn) ListTools(ctx context.Context) ([]mcp.Tool, error) {
	var all []mcp.Tool
	cursor := ""
	for {
		var params json.RawMessage
		if cursor != "" {
			params = mcp.MustParams(mcp.ToolsListParams{Cursor: cursor})
		}
		resp, err := c.Call(ctx, mcp.MethodToolsList, params)
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

// ListResources fetches the upstream's resource catalog, following pagination.
// A method-not-found error (upstream declares no resources capability) is
// treated as an empty catalog rather than a hard failure.
func (c *StdioConn) ListResources(ctx context.Context) ([]mcp.Resource, error) {
	var all []mcp.Resource
	cursor := ""
	for {
		var params json.RawMessage
		if cursor != "" {
			params = mcp.MustParams(mcp.ResourceListParams{Cursor: cursor})
		}
		resp, err := c.Call(ctx, mcp.MethodResourceList, params)
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

// Call on the connection with a typed tools/call helper. name is the ORIGINAL
// (un-namespaced) tool name expected by the upstream.
func (c *StdioConn) CallTool(ctx context.Context, name string, arguments json.RawMessage) (*mcp.Message, error) {
	params := mcp.MustParams(mcp.ToolsCallParams{Name: name, Arguments: arguments})
	return c.Call(ctx, mcp.MethodToolsCall, params)
}
