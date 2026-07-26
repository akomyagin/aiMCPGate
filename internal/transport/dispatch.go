package transport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"sort"
	"sync"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// buildCapabilities assembles the gateway's own capability object, advertised
// to the client on initialize. Resources are not aggregated in the MVP (see
// handleResourcesList), so no resources capability is declared — the object is
// built structurally (map → json.Marshal) rather than as a string literal, so
// further rounds can keep adding CONDITIONAL capabilities (resources/logging)
// without rebuilding this function.
//
// prompts (Round 4) is CONDITIONAL: declared only when at least one live
// upstream declared it in its own initialize — advertising it against zero
// prompt-capable upstreams would be dishonest. listChanged is honestly false:
// this round does not subscribe to upstream notifications/prompts/list_changed
// (such a notification is currently dropped as unknown, which is fine), so the
// gateway cannot promise to emit its own. TODO(post-MVP): wire prompt
// list_changed the way tools' is wired (Stage 7b/7c).
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
	caps := map[string]any{
		"tools": map[string]any{"listChanged": listChanged},
	}
	// nil reg: classification-only tests build a dispatcher without a registry;
	// a gateway with no registry aggregates nothing, so no extra capabilities.
	if reg != nil && reg.HasUpstreamCapability("prompts") {
		caps["prompts"] = map[string]any{"listChanged": false}
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
// prompts/* / resources/*). Each transport is left with only its own
// framing/plumbing
// (reading requests, writing replies, SSE vs newline); the protocol logic lives
// here once.
//
// Since Round 2 the dispatcher is no longer stateless: cancels (guarded by
// cancelMu) tracks one context.CancelFunc per in-flight tools/call, keyed by
// the CLIENT's request id, so a notifications/cancelled from the client can
// abort the matching call. The map is mutex-guarded, so a single dispatcher
// remains safe to share across concurrent HTTP requests — though over HTTP the
// cancellation path is moot in practice (each POST is a separate stateless
// request; two clients reusing one id merely overwrite each other's map entry,
// harmless because nothing over HTTP ever looks it up).
type dispatcher struct {
	reg     *registry.Registry
	log     *slog.Logger
	version string
	// listChanged is whether this transport can push a server→client
	// notifications/tools/list_changed (Stage 7c): true for stdio, false for
	// the POST-only HTTP transport. handleInitialize feeds it to
	// buildCapabilities on every handshake.
	listChanged bool

	// cancelMu guards cancels: client request id (raw bytes) → the CancelFunc
	// that aborts that in-flight tools/call's context (Round 2). Entries live
	// exactly as long as the call: registered before handleToolsCall, removed
	// right after it returns; notifications/cancelled for an id not in the map
	// (already finished, never existed) is a documented no-op per the spec.
	cancelMu sync.Mutex
	cancels  map[string]context.CancelFunc
}

// newDispatcher builds the shared method-handling core. listChanged tells it
// which tools capability to advertise: true only for a transport that can push
// a server→client notifications/tools/list_changed (stdio), false otherwise
// (HTTP POST-only). See buildCapabilities for the reasoning.
func newDispatcher(reg *registry.Registry, log *slog.Logger, version string, listChanged bool) *dispatcher {
	return &dispatcher{
		reg:         reg,
		log:         log,
		version:     version,
		listChanged: listChanged,
		cancels:     map[string]context.CancelFunc{},
	}
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
		if msg.Method == mcp.NotifCancelled {
			// The client abandoned an in-flight request: cancel its context
			// (Round 2). Handled BEFORE the generic drop below — this is the
			// one client notification with a side effect. Never a reply: a
			// notification gets none even when it misses.
			d.handleCancelled(msg.Params)
			return nil
		}
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
		return d.dispatchToolsCall(ctx, msg)
	case mcp.MethodPromptsList:
		return d.handlePromptsList(msg)
	case mcp.MethodPromptsGet:
		return d.handlePromptsGet(ctx, msg)
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
// sees the exact contract each upstream advertises. The registry already merged
// every upstream's full paginated catalog on Start; the OUTGOING list is served
// whole by default, or in pages when page_size is set (Round 8).
//
// Both Round 8 knobs are read from the LIVE config on every request (the
// registry holds it atomically), so a SIGHUP reload flips catalog_mode /
// page_size without restarting anything.
func (d *dispatcher) handleToolsList(req *mcp.Message) *mcp.Message {
	cfg := d.reg.ConfigSnapshot()
	// Lazy catalog mode (Round 8): the client sees exactly the three gateway
	// meta-tools instead of the aggregated catalog (lazy.go). page_size is
	// deliberately IGNORED here — documented precedence (config.Config
	// .CatalogMode): a fixed list of three never paginates.
	if cfg.LazyCatalog() {
		return mcp.NewResult(req.ID, mcp.MustParams(mcp.ToolsListResult{Tools: lazyCatalogTools()}))
	}
	descs := d.reg.Tools()
	tools := make([]mcp.Tool, 0, len(descs))
	for _, dd := range descs {
		t := dd.Tool
		t.Name = dd.Name // client-facing namespaced name, not the upstream original
		tools = append(tools, t)
	}
	if cfg.PageSize > 0 {
		return paginatedToolsList(req, tools, cfg.PageSize)
	}
	// page_size unset: the whole catalog in one response, any cursor ignored —
	// the pre-Round 8 behaviour, unchanged.
	return mcp.NewResult(req.ID, mcp.MustParams(mcp.ToolsListResult{Tools: tools}))
}

// paginatedToolsList serves one page of the (name-sorted) catalog. The cursor
// is opaque to the client: base64 of the LAST tool name on the previous page.
// It is name-based, not index-based, on purpose — Registry.Tools() is sorted
// by name, so when the catalog mutates BETWEEN pages (upstream restart,
// reload, list_changed) the next page simply resumes at the first name
// lexicographically AFTER the cursor: no duplicates and no skips of tools
// that existed on both sides of the mutation, whereas a positional cursor
// would silently shift. Per MCP, an unparseable cursor is answered with
// -32602 Invalid params.
func paginatedToolsList(req *mcp.Message, tools []mcp.Tool, pageSize int) *mcp.Message {
	var params mcp.ToolsListParams
	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return mcp.NewError(req.ID, mcp.CodeInvalidParams, "invalid tools/list params: "+err.Error(), nil)
		}
	}
	start := 0
	if params.Cursor != "" {
		last, err := base64.StdEncoding.DecodeString(params.Cursor)
		if err != nil {
			return mcp.NewError(req.ID, mcp.CodeInvalidParams, "invalid tools/list cursor", nil)
		}
		start = sort.Search(len(tools), func(i int) bool { return tools[i].Name > string(last) })
	}
	end := min(start+pageSize, len(tools))
	res := mcp.ToolsListResult{Tools: tools[start:end]}
	if end < len(tools) {
		res.NextCursor = base64.StdEncoding.EncodeToString([]byte(tools[end-1].Name))
	}
	return mcp.NewResult(req.ID, mcp.MustParams(res))
}

// dispatchToolsCall wraps handleToolsCall with the Round 2 cancellation
// plumbing: the call runs under its own cancellable child context, registered
// in d.cancels under the CLIENT's request id so a notifications/cancelled can
// abort it mid-flight (handleCancelled). When the call ends BECAUSE the client
// cancelled it — the child context is done while the parent is not, and the
// only holder of that child's CancelFunc besides this function is
// handleCancelled — the reply is suppressed entirely: per the MCP cancellation
// utility the receiver of a cancellation SHOULD NOT answer the cancelled
// request at all, not even with an error.
func (d *dispatcher) dispatchToolsCall(ctx context.Context, req *mcp.Message) *mcp.Message {
	callCtx, cancel := context.WithCancel(ctx)
	key := string(req.ID)
	d.cancelMu.Lock()
	d.cancels[key] = cancel
	d.cancelMu.Unlock()

	reply := d.handleToolsCall(callCtx, req)

	d.cancelMu.Lock()
	delete(d.cancels, key)
	d.cancelMu.Unlock()
	// Read the verdict BEFORE the deferred-style cancel below flips callCtx to
	// "done" on its own: cancelled-by-client is exactly "child done, parent
	// alive" at this point.
	cancelledByClient := callCtx.Err() != nil && ctx.Err() == nil
	cancel() // release the child context's resources; idempotent if already cancelled
	if cancelledByClient {
		d.log.Debug("tools/call cancelled by client, suppressing reply", "id", key)
		return nil
	}
	return reply
}

// handleCancelled processes a client's notifications/cancelled: find the
// in-flight tools/call the client is abandoning (by its OWN request id,
// byte-compared raw) and cancel its context. An unknown or already-finished
// requestId is a silent no-op — the spec explicitly allows the cancellation to
// race the response. Malformed params are dropped the same way (a notification
// never gets an error reply).
func (d *dispatcher) handleCancelled(params json.RawMessage) {
	var p mcp.CancelledParams
	if len(params) == 0 || json.Unmarshal(params, &p) != nil || len(p.RequestID) == 0 {
		d.log.Debug("ignoring malformed notifications/cancelled")
		return
	}
	key := string(p.RequestID)
	d.cancelMu.Lock()
	cancel, ok := d.cancels[key]
	if ok {
		delete(d.cancels, key)
	}
	d.cancelMu.Unlock()
	if !ok {
		d.log.Debug("notifications/cancelled for unknown or finished request", "id", key)
		return
	}
	d.log.Debug("cancelling in-flight tools/call on client request", "id", key, "reason", p.Reason)
	cancel()
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

	// Lazy catalog mode (Round 8): the three gateway meta-tools are handled
	// HERE, before Registry.CallTool ever sees the name — this ordering IS the
	// reserved-name policy: even if some upstream tool ended up namespaced as
	// gate_search_tools / gate_describe / gate_call, in lazy mode the
	// gateway's meta-tool wins at the dispatcher, before routing (the shadowed
	// upstream tool stays reachable through gate_call, which routes any name
	// through the normal registry path). In normal mode the names are not
	// special and route as usual. Any OTHER name falls through below, so a
	// client that already knows a namespaced name may still call it directly
	// even in lazy mode.
	if d.reg.ConfigSnapshot().LazyCatalog() {
		if reply, handled := d.handleLazyCall(ctx, req, params); handled {
			return reply
		}
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

// handlePromptsList returns the aggregated, namespaced prompt catalog —
// handleToolsList's prompts twin (Round 4). Each prompt's description/arguments
// are carried through verbatim. Pagination is not needed: the registry already
// merged every upstream's full paginated prompt list on launch.
func (d *dispatcher) handlePromptsList(req *mcp.Message) *mcp.Message {
	descs := d.reg.Prompts()
	prompts := make([]mcp.Prompt, 0, len(descs))
	for _, dd := range descs {
		p := dd.Prompt
		p.Name = dd.Name // client-facing namespaced name, not the upstream original
		prompts = append(prompts, p)
	}
	return mcp.NewResult(req.ID, mcp.MustParams(mcp.PromptsListResult{Prompts: prompts}))
}

// handlePromptsGet proxies a prompts/get through the registry, which resolves
// the owning upstream and rewrites the name back to the upstream's original —
// handleToolsCall's prompts twin (Round 4). The upstream's raw result/error is
// forwarded to the client verbatim under the CLIENT's id; a routing/transport
// failure surfaces as a JSON-RPC error whose text the registry already
// sanitized (only the prompt name the client itself supplied).
func (d *dispatcher) handlePromptsGet(ctx context.Context, req *mcp.Message) *mcp.Message {
	var params mcp.PromptsGetParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return mcp.NewError(req.ID, mcp.CodeInvalidParams, "invalid prompts/get params: "+err.Error(), nil)
	}
	if params.Name == "" {
		return mcp.NewError(req.ID, mcp.CodeInvalidParams, "prompts/get missing prompt name", nil)
	}

	resp, err := d.reg.GetPrompt(ctx, params.Name, params.Arguments)
	if err != nil {
		return mcp.NewError(req.ID, mcp.CodeInternalError, err.Error(), nil)
	}
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
