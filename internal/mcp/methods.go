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

	// NotifInitialized is sent by a client after a successful initialize.
	NotifInitialized = "notifications/initialized"
)

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
// InputSchema/OutputSchema are RawMessage so the exact JSON Schema is proxied to
// the client verbatim (same contract the upstream advertises).
type Tool struct {
	Name         string          `json:"name"`
	Title        string          `json:"title,omitempty"`
	Description  string          `json:"description,omitempty"`
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
type ToolsCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// Resource is one entry in a resources/list result.
type Resource struct {
	URI         string `json:"uri"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
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
