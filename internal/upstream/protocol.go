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

// caller is the minimal transport surface the shared MCP protocol logic needs:
// a request/response round-trip (call) and a one-way notification (notify),
// plus the upstream name for error messages. *StdioConn and *HTTPConn both
// satisfy it after Call/Notify were unexported for symmetry with the HTTP side.
type caller interface {
	call(ctx context.Context, method string, params json.RawMessage) (*mcp.Message, error)
	notify(ctx context.Context, method string, params json.RawMessage) error
	Name() string
}

var (
	_ caller = (*StdioConn)(nil)
	_ caller = (*HTTPConn)(nil)
)

// doInitialize performs the MCP handshake against an upstream: sends an
// `initialize` request and, on success, the `notifications/initialized`
// notification. It returns the server's InitializeResult.
func doInitialize(ctx context.Context, c caller) (*mcp.InitializeResult, error) {
	params := mcp.MustParams(mcp.InitializeParams{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    json.RawMessage(`{}`),
		ClientInfo:      gatewayClientInfo,
	})

	resp, err := c.call(ctx, mcp.MethodInitialize, params)
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

	if err := c.notify(ctx, mcp.NotifInitialized, nil); err != nil {
		return nil, fmt.Errorf("upstream %q: send initialized: %w", c.Name(), err)
	}
	return &res, nil
}

// doListTools fetches the upstream's full tool catalog, following pagination via
// nextCursor until exhausted.
func doListTools(ctx context.Context, c caller) ([]mcp.Tool, error) {
	var all []mcp.Tool
	cursor := ""
	for {
		var params json.RawMessage
		if cursor != "" {
			params = mcp.MustParams(mcp.ToolsListParams{Cursor: cursor})
		}
		resp, err := c.call(ctx, mcp.MethodToolsList, params)
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

// doListResources fetches the upstream's resource catalog, following pagination.
// A method-not-found error (upstream declares no resources capability) is
// treated as an empty catalog rather than a hard failure.
func doListResources(ctx context.Context, c caller) ([]mcp.Resource, error) {
	var all []mcp.Resource
	cursor := ""
	for {
		var params json.RawMessage
		if cursor != "" {
			params = mcp.MustParams(mcp.ResourceListParams{Cursor: cursor})
		}
		resp, err := c.call(ctx, mcp.MethodResourceList, params)
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

// doCallTool forwards a tools/call to the upstream. name is the ORIGINAL
// (un-namespaced) tool name expected by the upstream.
func doCallTool(ctx context.Context, c caller, name string, arguments json.RawMessage) (*mcp.Message, error) {
	params := mcp.MustParams(mcp.ToolsCallParams{Name: name, Arguments: arguments})
	return c.call(ctx, mcp.MethodToolsCall, params)
}
