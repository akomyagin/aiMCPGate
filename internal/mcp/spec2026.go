package mcp

import (
	"encoding/base64"
	"encoding/json"
)

// This file carries the MCP revision 2026-07-28 ("modern") type layer used by
// the dual-era client-facing side of the gateway (Stage 19). It is purely
// additive: the legacy 2025-06-18 types in methods.go/message.go are untouched,
// and none of this changes gateway behaviour on its own — 19b wires it into the
// dispatcher/transport. Spec facts verified 2026-09-13 against the official
// 2026-07-28 sources (see docs/plans/stage-19-mcp-spec-2026-07-28.md §0).

// Protocol versions.
const (
	// ProtocolVersion (in methods.go, "2025-06-18") is deliberately NOT renamed:
	// ~10 files reference it and it is still the legacy branch's version.

	// ProtocolVersionModern is the modern MCP revision the gateway serves to
	// modern clients (per-request _meta, stateless HTTP, no initialize).
	ProtocolVersionModern = "2026-07-28"
)

// SupportedVersions is what the gateway advertises in server/discover and in
// the data.supported field of a -32022 error. Newest first.
var SupportedVersions = []string{ProtocolVersionModern, ProtocolVersion}

// Keys of a modern request's params._meta object (2026-07-28 replaces the
// initialize handshake with per-request metadata under these keys).
const (
	MetaProtocolVersion    = "io.modelcontextprotocol/protocolVersion"
	MetaClientInfo         = "io.modelcontextprotocol/clientInfo"
	MetaClientCapabilities = "io.modelcontextprotocol/clientCapabilities"
	MetaServerInfo         = "io.modelcontextprotocol/serverInfo"
	MetaLogLevel           = "io.modelcontextprotocol/logLevel"
	MetaSubscriptionID     = "io.modelcontextprotocol/subscriptionId"
)

// Modern-revision methods / notifications.
const (
	MethodServerDiscover           = "server/discover"
	MethodSubscriptionsListen      = "subscriptions/listen"
	NotifSubscriptionsAcknowledged = "notifications/subscriptions/acknowledged"
)

// resultType values every modern result must carry.
const (
	ResultTypeComplete      = "complete"
	ResultTypeInputRequired = "input_required"
)

// Error codes defined by the 2026-07-28 spec. The -32020..-32099 band is
// reserved for MCP (changelog "Minor changes"); the gateway's own
// implementation-defined codes live in -32000..-32019 (see CodeGatewayBusy).
const (
	CodeHeaderMismatch             = -32020
	CodeMissingClientCapability    = -32021
	CodeUnsupportedProtocolVersion = -32022
)

// Cacheable-result values the gateway stamps onto modern results (used from
// 19b, declared here alongside their type layer).
//
// The catalog lives until the next reload/restart, which cannot be known ahead
// of time, so 60s is a freshness hint aligned with the listChanged
// notifications ("Both fields complement existing listChanged notifications").
// private: the endpoint sits behind auth_token, and different tokens are
// different authorization contexts.
const (
	GatewayTTLMs      = 60_000
	GatewayCacheScope = "private"
)

// RequestMeta holds the parsed modern params._meta fields of a single request.
type RequestMeta struct {
	ProtocolVersion    string          // MetaProtocolVersion; "" = legacy request
	ClientInfo         Implementation  // MetaClientInfo (may be zero)
	ClientCapabilities json.RawMessage // MetaClientCapabilities, nil if absent
	LogLevel           string          // MetaLogLevel

	// modern is the era flag backing Modern(); unexported so callers cannot
	// forge era membership. Set only by ParseRequestMeta when the
	// MetaProtocolVersion key is present in params._meta.
	modern bool
}

// metaEnvelope is the shape ParseRequestMeta needs from params: the optional
// _meta object. Everything else in params is ignored here (verbatim contract).
type metaEnvelope struct {
	Meta map[string]json.RawMessage `json:"_meta"`
}

// Modern reports that the request belongs to the modern era: the
// MetaProtocolVersion key was present in params._meta. A present-but-non-string
// version still counts as modern (see ParseRequestMeta) so it is answered with
// a modern -32022 rather than silently treated as legacy.
func (m RequestMeta) Modern() bool {
	return m.modern
}

// ParseRequestMeta tolerantly extracts _meta from params (params may be nil, a
// non-object, or lack _meta — all legacy, no error). It returns a zero
// RequestMeta with Modern()==false in every doubtful case, EXCEPT one: if the
// MetaProtocolVersion key is present but not a string, the result is
// Modern()==true with ProtocolVersion=="" — the request CLAIMS modern and must
// be answered with a modern -32022 error, not silently downgraded to legacy.
func ParseRequestMeta(params json.RawMessage) RequestMeta {
	var out RequestMeta
	if len(params) == 0 {
		return out
	}
	var env metaEnvelope
	if err := json.Unmarshal(params, &env); err != nil || env.Meta == nil {
		// params is not an object, or has no (object-shaped) _meta: legacy.
		return out
	}
	raw, ok := env.Meta[MetaProtocolVersion]
	if !ok {
		// No modern version key at all: a plain legacy request.
		return out
	}
	// The version key is present → the request claims the modern era.
	out.modern = true
	// A string value pins the concrete version; anything else leaves it "" so
	// the request is rejected as an unsupported version rather than downgraded.
	var ver string
	if json.Unmarshal(raw, &ver) == nil {
		out.ProtocolVersion = ver
	}
	if caps, ok := env.Meta[MetaClientCapabilities]; ok {
		out.ClientCapabilities = caps
	}
	if info, ok := env.Meta[MetaClientInfo]; ok {
		var impl Implementation
		if json.Unmarshal(info, &impl) == nil {
			out.ClientInfo = impl
		}
	}
	if lvl, ok := env.Meta[MetaLogLevel]; ok {
		var s string
		if json.Unmarshal(lvl, &s) == nil {
			out.LogLevel = s
		}
	}
	return out
}

// UnsupportedVersionData is the data object of a -32022 error.
type UnsupportedVersionData struct {
	Supported []string `json:"supported"`
	Requested string   `json:"requested"`
}

// DiscoverResult is the result of server/discover (schema.ts: DiscoverResult).
type DiscoverResult struct {
	ResultType        string          `json:"resultType"`
	SupportedVersions []string        `json:"supportedVersions"`
	Capabilities      json.RawMessage `json:"capabilities"`
	Instructions      string          `json:"instructions,omitempty"`
	TTLMs             int64           `json:"ttlMs"`
	CacheScope        string          `json:"cacheScope"`
	Meta              json.RawMessage `json:"_meta,omitempty"`
}

// InputRequiredResult is the MRTR response (schema.ts). InputRequests holds raw
// requests (method+params verbatim); by the serverreq-pipeline contract nothing
// inside is parsed.
type InputRequiredResult struct {
	ResultType    string                     `json:"resultType"` // always input_required
	InputRequests map[string]json.RawMessage `json:"inputRequests,omitempty"`
	RequestState  string                     `json:"requestState,omitempty"`
	Meta          json.RawMessage            `json:"_meta,omitempty"`
}

// InputRequestEnvelope is the value shape of InputRequests entries:
// {"method": ..., "params": ...}.
type InputRequestEnvelope struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// MRTRRetry holds the MRTR fields of a client retry's params.
type MRTRRetry struct {
	InputResponses map[string]json.RawMessage `json:"inputResponses,omitempty"`
	RequestState   string                     `json:"requestState,omitempty"`
}

// ExtractMRTRRetry tolerantly pulls inputResponses/requestState out of params
// (params may be nil, a non-object, or lack the fields — all yield a zero
// MRTRRetry with no error).
func ExtractMRTRRetry(params json.RawMessage) MRTRRetry {
	var out MRTRRetry
	if len(params) == 0 {
		return out
	}
	_ = json.Unmarshal(params, &out)
	return out
}

// SubscriptionFilter is params.notifications of a subscriptions/listen request.
type SubscriptionFilter struct {
	ToolsListChanged      bool     `json:"toolsListChanged,omitempty"`
	PromptsListChanged    bool     `json:"promptsListChanged,omitempty"`
	ResourcesListChanged  bool     `json:"resourcesListChanged,omitempty"`
	ResourceSubscriptions []string `json:"resourceSubscriptions,omitempty"`
}

// SubscriptionsListenParams is the params object of a subscriptions/listen
// request.
type SubscriptionsListenParams struct {
	Notifications SubscriptionFilter `json:"notifications"`
}

// DecorateModernResult writes the missing modern fields into a RAW result (a
// JSON object): resultType (if absent), and when cacheable, ttlMs+cacheScope
// (if absent), and _meta[MetaServerInfo] (without clobbering other _meta keys).
// Already-present fields are NOT overwritten: an upstream that sent its own
// resultType/ttlMs "knows better". A non-object is returned unchanged (a
// JSON-RPC result MUST be an object — not our error to mask). Implementation:
// Unmarshal into map[string]json.RawMessage → add → Marshal. Key order is not
// guaranteed; tests compare by Unmarshal.
func DecorateModernResult(result json.RawMessage, serverInfo Implementation,
	cacheable bool, ttlMs int64, cacheScope string) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(result, &obj); err != nil || obj == nil {
		// Not a JSON object (null, array, scalar, or invalid): return as-is.
		return result
	}
	if _, ok := obj["resultType"]; !ok {
		obj["resultType"] = MustParams(ResultTypeComplete)
	}
	if cacheable {
		if _, ok := obj["ttlMs"]; !ok {
			obj["ttlMs"] = MustParams(ttlMs)
		}
		if _, ok := obj["cacheScope"]; !ok {
			obj["cacheScope"] = MustParams(cacheScope)
		}
	}
	// Merge serverInfo into _meta without dropping any existing keys.
	meta := map[string]json.RawMessage{}
	if raw, ok := obj["_meta"]; ok {
		// Best-effort: if the existing _meta is a well-formed object, preserve
		// its keys; if it is somehow not an object, leave it untouched rather
		// than lose it.
		if json.Unmarshal(raw, &meta) != nil {
			return MustParams(obj)
		}
	}
	if _, ok := meta[MetaServerInfo]; !ok {
		meta[MetaServerInfo] = MustParams(serverInfo)
	}
	obj["_meta"] = MustParams(meta)
	return MustParams(obj)
}

// DecodeHeaderSentinel decodes a header value of the form "=?base64?<b64>?="
// (case-sensitive lowercase markers) into its original UTF-8 string. A value
// without the sentinel is returned as-is with ok==true; a sentinel with broken
// base64 returns ("", false).
func DecodeHeaderSentinel(v string) (string, bool) {
	const prefix = "=?base64?"
	const suffix = "?="
	if len(v) < len(prefix)+len(suffix) || v[:len(prefix)] != prefix || v[len(v)-len(suffix):] != suffix {
		// No sentinel (including the uppercase "=?BASE64?...?=" form): plain.
		return v, true
	}
	b64 := v[len(prefix) : len(v)-len(suffix)]
	dec, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", false
	}
	return string(dec), true
}
