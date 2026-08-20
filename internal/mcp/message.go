// Package mcp implements the MCP JSON-RPC 2.0 message types and the stdio
// framing codec used to talk to (and, later, as) MCP servers.
//
// Design decision (Stage 1, see docs/MCP_NOTES.md §1): aiMCPGate uses a thin
// hand-rolled JSON-RPC layer rather than the official Go SDK. The gateway is a
// transparent multiplexer, so it must forward arbitrary — including unknown and
// future — methods and fields without loss. To achieve that, Params/Result and
// the message ID are kept as json.RawMessage and passed through verbatim.
//
// Spec baseline: MCP 2025-06-18.
//   - Base JSON-RPC types: https://modelcontextprotocol.io/specification/2025-06-18/basic
//   - stdio transport:      https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Version is the JSON-RPC version string mandated by MCP.
const Version = "2.0"

// Standard JSON-RPC 2.0 error codes the gateway itself may emit. Upstream
// (server-defined) codes are proxied through verbatim, never remapped.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// CodeGatewayBusy is the gateway's OWN implementation-defined JSON-RPC error
// code (JSON-RPC 2.0 reserves -32000..-32099 for exactly this): a tools/call
// the gateway REFUSED before forwarding, because a per-upstream guard
// (rate_limit / max_concurrent) could not admit it within the caller's
// context. The request never reached the upstream, so retrying is safe; the
// error's data carries {"retryable":true,"reason":...} as the machine-readable
// contract. -32000/-32001/-32002 are deliberately avoided (colloquially taken
// by the official SDKs / spec examples: ConnectionClosed, RequestTimeout,
// resource-not-found); -32029 is free and mnemonic of HTTP 429. Emitted ONLY
// for the gateway's own guard refusals — an upstream's error code, whatever it
// is, still proxies through verbatim per the invariant above.
const CodeGatewayBusy = -32029

// Message is a single MCP JSON-RPC 2.0 message in its most permissive form.
//
// It intentionally models request, response, and notification with one struct
// so the codec can decode any line without knowing its kind up front, and so
// the gateway can proxy messages it does not fully understand:
//
//   - ID present (non-null) + Method present  → request
//   - ID present (non-null) + Result/Error    → response
//   - ID absent (null)      + Method present  → notification
//
// Per MCP the ID MUST be a string or integer and MUST NOT be null; a null/absent
// ID therefore unambiguously marks a notification. ID, Params, Result are
// json.RawMessage so unknown shapes pass through without loss.
type Message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC 2.0 error object. Data is optional/free-form.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Error implements the error interface so an *Error can be returned directly.
func (e *Error) Error() string {
	if e == nil {
		return "<nil mcp.Error>"
	}
	return fmt.Sprintf("jsonrpc error %d: %s", e.Code, e.Message)
}

// IsNotification reports whether the message carries no ID (or an explicit
// null ID) and therefore expects no response.
func (m *Message) IsNotification() bool {
	return isNullID(m.ID)
}

// IsResponse reports whether the message is a response (has a result or error).
func (m *Message) IsResponse() bool {
	return m.Result != nil || m.Error != nil
}

// IsRequest reports whether the message is a request: it has an ID and a method
// and is not a response.
func (m *Message) IsRequest() bool {
	return !isNullID(m.ID) && m.Method != "" && !m.IsResponse()
}

// IsMalformedHybrid reports whether m carries both a method (request/
// notification shape) and a result or error (response shape) — a shape
// JSON-RPC 2.0 does not allow. All three message classifiers (the client
// dispatcher, the demo stub, the upstream reader) must agree on rejecting
// this, so the check lives once, here.
func (m *Message) IsMalformedHybrid() bool {
	return m.Method != "" && m.IsResponse()
}

// isNullID reports whether a raw ID is absent or the JSON literal null. Any
// surrounding whitespace is tolerated.
func isNullID(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

// NewRequest builds a request with the given raw id, method and params.
// params may be nil.
func NewRequest(id json.RawMessage, method string, params json.RawMessage) *Message {
	return &Message{JSONRPC: Version, ID: id, Method: method, Params: params}
}

// NewNotification builds a notification (no id) with the given method/params.
func NewNotification(method string, params json.RawMessage) *Message {
	return &Message{JSONRPC: Version, Method: method, Params: params}
}

// NewResult builds a success response echoing id and carrying result.
func NewResult(id json.RawMessage, result json.RawMessage) *Message {
	return &Message{JSONRPC: Version, ID: id, Result: result}
}

// NewError builds an error response echoing id. Per JSON-RPC 2.0 an error
// response MUST carry an id; when the original id is unknown (nil/absent —
// e.g. the request was malformed and never parsed) it MUST be the literal
// null, so an empty id is normalized to json "null" here. A non-empty
// RawMessage("null") survives the struct's omitempty and marshals as
// "id":null instead of dropping the field.
func NewError(id json.RawMessage, code int, message string, data json.RawMessage) *Message {
	if len(bytes.TrimSpace(id)) == 0 {
		id = json.RawMessage("null")
	}
	return &Message{JSONRPC: Version, ID: id, Error: &Error{Code: code, Message: message, Data: data}}
}

// IntID renders an integer as a JSON-RPC id (json.RawMessage). Used by the
// multiplexer to mint upstream-side ids from an atomic counter.
func IntID(n int64) json.RawMessage {
	return json.RawMessage(fmt.Sprintf("%d", n))
}

// StringID renders a string as a JSON-RPC id (json.RawMessage). Used for the
// gateway-minted ids of upstream-initiated requests proxied to the client
// (elicitation/create, Round 14), where a prefixed string keeps them visually
// apart from the client's own (typically numeric) ids in logs.
func StringID(s string) json.RawMessage {
	b, err := json.Marshal(s)
	if err != nil {
		// Unreachable: marshaling a Go string cannot fail.
		panic("mcp: marshal string id: " + err.Error())
	}
	return b
}
