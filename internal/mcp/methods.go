package mcp

import "encoding/json"

// ProtocolVersion is the MCP spec version aiMCPGate targets.
const ProtocolVersion = "2025-06-18"

// Method names used by the gateway (MCP 2025-06-18).
const (
	MethodInitialize   = "initialize"
	MethodToolsList    = "tools/list"
	MethodToolsCall    = "tools/call"
	MethodResourceList = "resources/list"
	MethodResourceRead = "resources/read"
	MethodPromptsList  = "prompts/list"
	MethodPromptsGet   = "prompts/get"

	// MethodLoggingSetLevel asks a server to start emitting log notifications
	// at or above the given severity. The gateway RECEIVES it from its client
	// and fans it out to every upstream that declared the logging capability
	// (Round 3); the level value travels verbatim, never validated en route.
	MethodLoggingSetLevel = "logging/setLevel"

	// MethodResourceTemplatesList lists an upstream's parameterized resource
	// templates (RFC 6570 URI templates), aggregated by the gateway the same
	// merge path plain resources take (Round 5).
	MethodResourceTemplatesList = "resources/templates/list"

	// MethodCompletionComplete asks a server for argument autocompletion
	// suggestions for a prompt argument or a resource-template variable. The
	// gateway routes it to the upstream owning the referenced prompt/resource
	// and proxies the payload verbatim (Round 5).
	MethodCompletionComplete = "completion/complete"

	// MethodPing is the liveness check either side may send at any time —
	// including BEFORE the initialize handshake completes (the only request the
	// spec allows there, docs/MCP_NOTES.md §4). The receiver MUST answer
	// promptly with an empty result.
	MethodPing = "ping"

	// NotifInitialized is sent by a client after a successful initialize.
	NotifInitialized = "notifications/initialized"

	// NotifToolsListChanged tells the peer the tool catalog changed and should be
	// re-listed. The gateway both RECEIVES it from a stdio upstream (Stage 7b,
	// triggering a re-list of that upstream) and SENDS it to its own client
	// (Stage 7c, when the aggregated catalog changes at runtime).
	NotifToolsListChanged = "notifications/tools/list_changed"

	// NotifProgress carries a progress update for an in-flight request, keyed by
	// the progressToken the REQUESTER minted in its request's `_meta`. The
	// gateway RECEIVES it from a stdio upstream mid-tools/call and forwards the
	// params VERBATIM to its own client — the token belongs to the client's id
	// space, never rewritten (Round 2).
	NotifProgress = "notifications/progress"

	// NotifMessage carries one log message from a server to its client. The
	// gateway RECEIVES it from a stdio upstream and forwards it to its own
	// client over the same channel progress travels (Round 3) — the params are
	// not interpreted beyond stamping the emitting upstream's name into the
	// spec's optional `logger` field, so the client can tell N multiplexed
	// upstream log streams apart.
	NotifMessage = "notifications/message"

	// NotifCancelled asks the peer to abandon an in-flight request, identified
	// by requestId in the SENDER's id space. The gateway both RECEIVES it from
	// its client (cancelling the matching in-flight tools/call's context) and
	// SENDS it to a stdio upstream when the forwarded call's context is
	// cancelled — with the gateway's own upstream-side id, never the client's
	// (Round 2).
	NotifCancelled = "notifications/cancelled"
)

// CancelledParams is the params object of a notifications/cancelled
// notification. RequestID is raw: JSON-RPC ids may be numbers or strings, and
// the gateway only ever compares them byte-for-byte in whichever id space the
// notification travels (client-side in, upstream-side out).
type CancelledParams struct {
	RequestID json.RawMessage `json:"requestId"`
	Reason    string          `json:"reason,omitempty"`
}

// LoggingSetLevelParams is the params object of a logging/setLevel request.
// Level is one of the RFC 5424 syslog severities per the MCP spec (debug/info/
// notice/warning/error/critical/alert/emergency), but the gateway deliberately
// does NOT validate the value: it forwards it verbatim to every logging-capable
// upstream, and an upstream that rejects an unknown level answers with its own
// JSON-RPC error — inventing a stricter gate here would only drift from
// whatever set the upstreams actually accept.
type LoggingSetLevelParams struct {
	Level string `json:"level"`
}

// Implementation identifies a client or server (clientInfo / serverInfo).
type Implementation struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

// InitializeParams is the params object of an initialize request.
//
// Capabilities is left as RawMessage: the gateway does not interpret most of it
// and proxies it through. It is populated with an empty object when nil.
type InitializeParams struct {
	ProtocolVersion string          `json:"protocolVersion"`
	Capabilities    json.RawMessage `json:"capabilities"`
	ClientInfo      Implementation  `json:"clientInfo"`
}

// InitializeResult is the result of an initialize response.
type InitializeResult struct {
	ProtocolVersion string          `json:"protocolVersion"`
	Capabilities    json.RawMessage `json:"capabilities"`
	ServerInfo      Implementation  `json:"serverInfo"`
	Instructions    string          `json:"instructions,omitempty"`
}

// Tool is one entry in a tools/list result.
//
// Description and InputSchema/OutputSchema are RawMessage so the exact JSON is
// proxied to the client verbatim (same contract the upstream advertises). For
// Description a plain string would be re-normalized on remarshal — e.g. an
// explicit `"description": ""` from an upstream would be dropped by omitempty —
// while omitempty on a RawMessage fires only when the field was truly absent.
type Tool struct {
	Name         string          `json:"name"`
	Title        string          `json:"title,omitempty"`
	Description  json.RawMessage `json:"description,omitempty"`
	InputSchema  json.RawMessage `json:"inputSchema,omitempty"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
	Annotations  json.RawMessage `json:"annotations,omitempty"`
}

// ToolsListParams carries the optional pagination cursor.
type ToolsListParams struct {
	Cursor string `json:"cursor,omitempty"`
}

// ToolsListResult is the result of a tools/list response.
type ToolsListResult struct {
	Tools      []Tool `json:"tools"`
	NextCursor string `json:"nextCursor,omitempty"`
}

// ToolsCallParams is the params object of a tools/call request. Arguments is
// proxied verbatim.
//
// Meta carries the client's optional `_meta` object (e.g. progressToken)
// through to the upstream verbatim — dropping it silently would break the
// gateway's transparent-proxy contract (docs/MCP_NOTES.md §1). RawMessage with
// omitempty: absent stays absent, present is forwarded byte for byte.
type ToolsCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Meta      json.RawMessage `json:"_meta,omitempty"`
}

// Prompt is one entry in a prompts/list result.
//
// Description is RawMessage for the same reason as Tool.Description — the
// exact JSON is proxied to the client verbatim. Arguments is the upstream's
// PromptArgument array, also verbatim: the gateway never parses individual
// prompt arguments, it only namespaces the prompt name.
type Prompt struct {
	Name        string          `json:"name"`
	Title       string          `json:"title,omitempty"`
	Description json.RawMessage `json:"description,omitempty"`
	Arguments   json.RawMessage `json:"arguments,omitempty"`
}

// PromptsListParams carries the optional pagination cursor.
type PromptsListParams struct {
	Cursor string `json:"cursor,omitempty"`
}

// PromptsListResult is the result of a prompts/list response.
type PromptsListResult struct {
	Prompts    []Prompt `json:"prompts"`
	NextCursor string   `json:"nextCursor,omitempty"`
}

// PromptsGetParams is the params object of a prompts/get request. Arguments is
// proxied verbatim.
//
// There is deliberately NO PromptsGetResult type: the gateway forwards the
// upstream's result (description/messages) verbatim as the response payload,
// exactly like tools/call — only prompts/list, where the gateway actually
// aggregates entries across upstreams, needs typing.
type PromptsGetParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// Resource is one entry in a resources/list result.
//
// Description/Annotations/Size/Meta are RawMessage for the same reason as
// Tool.Description — the gateway aggregates resources but never interprets
// their payload, so the exact JSON each upstream advertises is proxied to the
// client verbatim (Round 5). Unlike tools and prompts, resources are addressed
// by URI and are NOT namespaced: the URI the client sees is the upstream's own.
type Resource struct {
	URI         string          `json:"uri"`
	Name        string          `json:"name,omitempty"`
	Title       string          `json:"title,omitempty"`
	Description json.RawMessage `json:"description,omitempty"`
	MimeType    string          `json:"mimeType,omitempty"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
	Size        json.RawMessage `json:"size,omitempty"`
	Meta        json.RawMessage `json:"_meta,omitempty"`
}

// ResourceListParams carries the optional pagination cursor.
type ResourceListParams struct {
	Cursor string `json:"cursor,omitempty"`
}

// ResourceListResult is the result of a resources/list response.
type ResourceListResult struct {
	Resources  []Resource `json:"resources"`
	NextCursor string     `json:"nextCursor,omitempty"`
}

// ResourceReadParams is the params object of a resources/read request.
//
// There is deliberately NO ResourceReadResult type: the gateway forwards the
// upstream's result (contents) verbatim as the response payload, exactly like
// tools/call and prompts/get.
type ResourceReadParams struct {
	URI string `json:"uri"`
}

// ResourceTemplate is one entry in a resources/templates/list result — a
// parameterized resource whose URITemplate ("file:///logs/{name}.log", RFC
// 6570) matches a family of URIs instead of naming one. Payload fields are
// verbatim like Resource's.
type ResourceTemplate struct {
	URITemplate string          `json:"uriTemplate"`
	Name        string          `json:"name,omitempty"`
	Title       string          `json:"title,omitempty"`
	Description json.RawMessage `json:"description,omitempty"`
	MimeType    string          `json:"mimeType,omitempty"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
	Meta        json.RawMessage `json:"_meta,omitempty"`
}

// ResourceTemplatesListParams carries the optional pagination cursor.
type ResourceTemplatesListParams struct {
	Cursor string `json:"cursor,omitempty"`
}

// ResourceTemplatesListResult is the result of a resources/templates/list
// response.
type ResourceTemplatesListResult struct {
	ResourceTemplates []ResourceTemplate `json:"resourceTemplates"`
	NextCursor        string             `json:"nextCursor,omitempty"`
}

// Completion ref types: what a completion/complete request's ref.type may be —
// argument completion for a prompt, or for a resource-template variable.
const (
	CompletionRefPrompt   = "ref/prompt"
	CompletionRefResource = "ref/resource"
)

// CompletionRef is the ref object of a completion/complete request: which
// prompt (by name) or resource template (by URI) the argument being completed
// belongs to. It is typed — not RawMessage — because for ref/prompt the
// gateway must rewrite Name from the client-facing namespaced form back to
// the upstream's original before forwarding (the client only ever saw the
// namespaced name in prompts/list); for ref/resource the URI is never
// namespaced and travels as-is.
type CompletionRef struct {
	Type  string `json:"type"`
	Name  string `json:"name,omitempty"`  // ref/prompt: prompt name
	Title string `json:"title,omitempty"` // ref/prompt: optional display title
	URI   string `json:"uri,omitempty"`   // ref/resource: template or resource URI
}

// CompletionCompleteParams is the params object of a completion/complete
// request. Only Ref is interpreted (for routing and the prompt-name rewrite);
// Argument, Context and Meta are proxied verbatim.
//
// There is deliberately NO CompletionCompleteResult type: the upstream's
// result (completion values) is forwarded verbatim, like prompts/get.
type CompletionCompleteParams struct {
	Ref      CompletionRef   `json:"ref"`
	Argument json.RawMessage `json:"argument"`
	Context  json.RawMessage `json:"context,omitempty"`
	Meta     json.RawMessage `json:"_meta,omitempty"`
}

// MustParams marshals v into a json.RawMessage for use as a message's params.
// It panics only on a programming error (a value that cannot be marshaled),
// which never happens for the plain structs above.
func MustParams(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic("mcp: marshal params: " + err.Error())
	}
	return b
}
