package transport

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// buildCapabilities assembles the gateway's own capability object, advertised
// to the client on initialize. Resources are not aggregated in the MVP (see
// handleResourcesList), so only the tools capability is declared today — but
// the object is built structurally (map → json.Marshal) rather than as a
// string literal, so upcoming rounds can add CONDITIONAL capabilities
// (prompts/resources/logging — advertised only when
// reg.HasUpstreamCapability(...) says at least one upstream backs them)
// without rebuilding this function.
//
// listChanged is TRANSPORT-DEPENDENT (Stage 7c). Since Stage 7 the aggregated
// catalog is dynamic (auto-restart, upstream list_changed, reload), so the
// gateway CAN emit notifications/tools/list_changed — but only over a transport
// with a server→client channel. stdio has one (the same pipe), so it advertises
// listChanged:true and pushes the notification. The HTTP transport is POST-only
// here (no GET SSE stream, MCP_NOTES §8 п.3), so it CANNOT push server→client
// notifications; it truthfully advertises listChanged:false and clients simply
// see the updated catalog on their next tools/list. Building the GET SSE channel
// is deferred future work.
func buildCapabilities(reg *registry.Registry, listChanged bool) json.RawMessage {
	_ = reg // consulted from the next rounds on (HasUpstreamCapability for prompts/resources/logging)
	caps := map[string]any{
		"tools": map[string]any{"listChanged": listChanged},
	}
	b, err := json.Marshal(caps)
	if err != nil {
		// Unreachable for a map of plain bools; keep initialize alive anyway.
		return json.RawMessage(`{}`)
	}
	return b
}

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
	// listChanged is whether this transport can push a server→client
	// notifications/tools/list_changed (Stage 7c): true for stdio, false for
	// the POST-only HTTP transport. handleInitialize feeds it to
	// buildCapabilities on every handshake.
	listChanged bool
}

// newDispatcher builds the shared method-handling core. listChanged tells it
// which tools capability to advertise: true only for a transport that can push
// a server→client notifications/tools/list_changed (stdio), false otherwise
// (HTTP POST-only). See buildCapabilities for the reasoning.
func newDispatcher(reg *registry.Registry, log *slog.Logger, version string, listChanged bool) *dispatcher {
	return &dispatcher{reg: reg, log: log, version: version, listChanged: listChanged}
}

// dispatch handles one client message and returns the reply to send back, or
// nil if none is required (notifications, or a stray non-request). The returned
// message always echoes the CLIENT's id — never any upstream-side id the
// registry used internally.
func (d *dispatcher) dispatch(ctx context.Context, msg *mcp.Message) *mcp.Message {
	// A malformed hybrid — a message with BOTH a method (request shape) and a
	// result/error (response shape) — matches neither predicate cleanly and
	// used to fall through the classification silently. Answer it with an
	// explicit invalid-request error instead of dropping it (found by security
	// audit). Checked FIRST, before the notification/request classification
	// below, and INDEPENDENTLY of the id: a hybrid with a null/absent id would
	// otherwise slip through the gate as a "notification" (IsNotification looks
	// only at the id — found by review). A plain request never trips this:
	// IsResponse requires an actual result/error. NewError echoes whatever id
	// the message carried — null when there was none, which is what JSON-RPC
	// prescribes when the request id cannot be determined.
	if msg.IsMalformedHybrid() {
		return mcp.NewError(msg.ID, mcp.CodeInvalidRequest,
			"message is not a valid request: carries both a method and a result/error", nil)
	}
	if msg.IsNotification() {
		// notifications/initialized and the like need no reply.
		d.log.Debug("client notification", "method", msg.Method)
		return nil
	}
	if !msg.IsRequest() {
		if msg.IsResponse() {
			// A response from the client (unexpected in the server role) — ignore.
			d.log.Debug("unexpected client message ignored", "method", msg.Method)
			return nil
		}
		// Not a notification (it has an id), not a response (no result/error),
		// not a request (no method): a request-shaped message with an id but no
		// method. JSON-RPC 2.0 requires answering it with -32600 Invalid Request
		// under that id, not dropping it silently (found by audit).
		return mcp.NewError(msg.ID, mcp.CodeInvalidRequest,
			"message is not a valid request: missing method", nil)
	}

	switch msg.Method {
	case mcp.MethodPing:
		// Liveness check, answered with an empty result — the one request the
		// spec allows at ANY time, including before the initialize handshake
		// (MCP_NOTES §4), so it deliberately consults nothing and cannot fail.
		return mcp.NewResult(msg.ID, json.RawMessage("{}"))
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
// serverInfo, its aggregated capabilities and the concatenated upstream
// instructions, echoing the client's id.
//
// The client's own params (clientInfo etc.) are parsed TOLERANTLY, for the
// debug log only: malformed initialize params must not fail the handshake —
// the gateway needs nothing from them to answer, so the worst case is an
// unidentified client. (The transports run the same tolerant parse via
// clientString to attach the identity to later calls' contexts.)
func (d *dispatcher) handleInitialize(req *mcp.Message) *mcp.Message {
	if client := clientString(req.Params); client != "" {
		d.log.Debug("client initialize", "client", client)
	}
	result := mcp.InitializeResult{
		ProtocolVersion: mcp.ProtocolVersion,
		Capabilities:    buildCapabilities(d.reg, d.listChanged),
		ServerInfo: mcp.Implementation{
			Name:    "aiMCPGate",
			Version: d.version,
		},
		Instructions: d.reg.Instructions(),
	}
	return mcp.NewResult(req.ID, mcp.MustParams(result))
}

// clientString extracts the calling client's identity from initialize params
// as "name/version" — the string CallRecord.Client carries. The parse is
// tolerant by design: malformed or absent params yield "" (an unidentified
// client), never an error — initialize must succeed regardless. Empty when
// both clientInfo fields are empty.
func clientString(params json.RawMessage) string {
	var p mcp.InitializeParams
	if len(params) == 0 || json.Unmarshal(params, &p) != nil {
		return ""
	}
	if p.ClientInfo.Name == "" && p.ClientInfo.Version == "" {
		return ""
	}
	return p.ClientInfo.Name + "/" + p.ClientInfo.Version
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

	// params.Meta (the client's `_meta`, e.g. progressToken) rides along
	// verbatim — the registry hands it to the upstream untouched.
	resp, err := d.reg.CallTool(ctx, params.Name, params.Arguments, params.Meta)
	if err != nil {
		// Routing/transport failure (unknown tool, dead upstream, timeout):
		// surface it as a JSON-RPC error under the client's id. err.Error() is
		// a sanitized gateway message (CallTool's job, not this dispatcher's)
		// — never the call arguments (which may hold secrets), and never an
		// upstream name or raw internal error string (gateway topology/
		// internals), only the tool name the client itself already supplied.
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
