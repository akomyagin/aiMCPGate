package transport

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// gatewayCapabilities is what the gateway advertises to the client on
// initialize. The gateway aggregates upstream TOOLS in the MVP; resources are
// not yet aggregated (see handleResourcesList), so only the tools capability is
// declared. It is a raw JSON literal because the gateway does not otherwise
// interpret its own capability object.
var gatewayCapabilities = json.RawMessage(`{"tools":{"listChanged":false}}`)

// dispatcher is the transport-agnostic core of the client-facing gateway: given
// one decoded client message it produces the reply (or nil, for a notification
// that needs none), by consulting the aggregated registry.
//
// It exists so the stdio and HTTP transports share exactly one implementation
// of the MCP method handling (initialize / tools/list / tools/call /
// resources/*). Each transport is left with only its own framing/plumbing
// (reading requests, writing replies, SSE vs newline); the protocol logic lives
// here once. It holds no per-connection state, so a single dispatcher is safe to
// share across concurrent HTTP requests.
type dispatcher struct {
	reg     *registry.Registry
	log     *slog.Logger
	version string
}

func newDispatcher(reg *registry.Registry, log *slog.Logger, version string) *dispatcher {
	return &dispatcher{reg: reg, log: log, version: version}
}

// dispatch handles one client message and returns the reply to send back, or
// nil if none is required (notifications, or a stray non-request). The returned
// message always echoes the CLIENT's id — never any upstream-side id the
// registry used internally.
func (d *dispatcher) dispatch(ctx context.Context, msg *mcp.Message) *mcp.Message {
	if msg.IsNotification() {
		// notifications/initialized and the like need no reply.
		d.log.Debug("client notification", "method", msg.Method)
		return nil
	}
	if !msg.IsRequest() {
		// A response from the client (unexpected in the server role) — ignore.
		d.log.Debug("unexpected client message ignored", "method", msg.Method)
		return nil
	}

	switch msg.Method {
	case mcp.MethodInitialize:
		return d.handleInitialize(msg)
	case mcp.MethodToolsList:
		return d.handleToolsList(msg)
	case mcp.MethodToolsCall:
		return d.handleToolsCall(ctx, msg)
	case mcp.MethodResourceList:
		return d.handleResourcesList(msg)
	case mcp.MethodResourceRead:
		return d.handleResourcesRead(msg)
	default:
		return mcp.NewError(msg.ID, mcp.CodeMethodNotFound, "method not found: "+msg.Method, nil)
	}
}

// handleInitialize answers the client handshake with the gateway's own
// serverInfo and aggregated capabilities, echoing the client's id.
func (d *dispatcher) handleInitialize(req *mcp.Message) *mcp.Message {
	result := mcp.InitializeResult{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    gatewayCapabilities,
		ServerInfo: mcp.Implementation{
			Name:    "aiMCPGate",
			Version: d.version,
		},
	}
	return mcp.NewResult(req.ID, mcp.MustParams(result))
}

// handleToolsList returns the aggregated, namespaced catalog. Each tool's
// schema (description/inputSchema/...) is carried through verbatim so the client
// sees the exact contract each upstream advertises. Pagination is not needed:
// the registry already merged every upstream's full paginated catalog on Start.
func (d *dispatcher) handleToolsList(req *mcp.Message) *mcp.Message {
	descs := d.reg.Tools()
	tools := make([]mcp.Tool, 0, len(descs))
	for _, dd := range descs {
		t := dd.Tool
		t.Name = dd.Name // client-facing namespaced name, not the upstream original
		tools = append(tools, t)
	}
	return mcp.NewResult(req.ID, mcp.MustParams(mcp.ToolsListResult{Tools: tools}))
}

// handleToolsCall proxies a call through the registry, which resolves the owning
// upstream, rewrites the name to the upstream original, mints its own
// upstream-side id, and audits the call. The upstream's raw result/error is
// forwarded to the client under the CLIENT's id.
//
// ctx (with any transport-supplied deadline) is passed straight to the registry,
// which further bounds it by the configured call timeout.
func (d *dispatcher) handleToolsCall(ctx context.Context, req *mcp.Message) *mcp.Message {
	var params mcp.ToolsCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return mcp.NewError(req.ID, mcp.CodeInvalidParams, "invalid tools/call params: "+err.Error(), nil)
	}
	if params.Name == "" {
		return mcp.NewError(req.ID, mcp.CodeInvalidParams, "tools/call missing tool name", nil)
	}

	resp, err := d.reg.CallTool(ctx, params.Name, params.Arguments)
	if err != nil {
		// Routing/transport failure (unknown tool, dead upstream, timeout):
		// surface it as a JSON-RPC error under the client's id. The error string
		// is a sanitized gateway message — it never contains the call arguments
		// (which may hold secrets).
		return mcp.NewError(req.ID, mcp.CodeInternalError, err.Error(), nil)
	}

	// resp is the upstream's raw response. Its own ID is the gateway's
	// upstream-side id and MUST NOT leak to the client; re-wrap the payload under
	// the client's id, forwarding the upstream's result or error verbatim.
	if resp.Error != nil {
		return mcp.NewError(req.ID, resp.Error.Code, resp.Error.Message, resp.Error.Data)
	}
	return mcp.NewResult(req.ID, resp.Result)
}

// handleResourcesList returns an empty resource catalog. Resource aggregation
// across upstreams is not implemented in the MVP (the registry lists but does
// not merge resources yet) — TODO(post-MVP): aggregate and route resources the
// way tools are, then serve them here. Returning an empty list (rather than a
// method-not-found error) keeps well-behaved clients that probe resources happy.
func (d *dispatcher) handleResourcesList(req *mcp.Message) *mcp.Message {
	return mcp.NewResult(req.ID, mcp.MustParams(mcp.ResourceListResult{Resources: []mcp.Resource{}}))
}

// handleResourcesRead reports an error: with no aggregated resources (see
// handleResourcesList), any uri the client could ask to read is unknown.
// TODO(post-MVP): route resources/read to the owning upstream once resources
// are aggregated.
func (d *dispatcher) handleResourcesRead(req *mcp.Message) *mcp.Message {
	return mcp.NewError(req.ID, mcp.CodeInvalidParams, "resources are not aggregated in this build", nil)
}
