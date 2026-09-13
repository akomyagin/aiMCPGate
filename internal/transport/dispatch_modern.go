package transport

import (
	"context"
	"encoding/json"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// dispatchModern serves one modern-era (2026-07-28) request: it checks the
// version, rejects methods removed on the modern revision, routes the rest into
// the SAME legacy handlers the legacy switch uses, and decorates the successful
// result with the modern fields (resultType, cacheable ttlMs/cacheScope,
// _meta.serverInfo). Era is a property of the request, not the connection
// (dual-era server per versioning.mdx), so this reuses every legacy handler
// rather than forking the catalog/routing logic.
//
// meta is the already-parsed params._meta (mcp.ParseRequestMeta) that put this
// request on the modern branch; meta.Modern() is guaranteed true here.
func (d *dispatcher) dispatchModern(ctx context.Context, msg *mcp.Message, meta mcp.RequestMeta) *mcp.Message {
	// Version gate: the only modern version the gateway serves is 2026-07-28.
	// A present-but-non-string version arrives here as "" (ParseRequestMeta's
	// one deliberate downgrade guard) and is rejected the same way — the request
	// claimed modern, so it earns a modern -32022, never a silent legacy slide.
	// "2025-06-18" in the modern key is also -32022: legacy versions are served
	// only through the initialize handshake, which has no per-request metadata.
	if meta.ProtocolVersion != mcp.ProtocolVersionModern {
		return mcp.NewError(msg.ID, mcp.CodeUnsupportedProtocolVersion,
			"unsupported protocol version",
			mcp.MustParams(mcp.UnsupportedVersionData{
				Supported: mcp.SupportedVersions,
				Requested: meta.ProtocolVersion,
			}))
	}

	reply := d.routeModern(ctx, msg)

	// Decorate only a successful result; an error is left exactly as built.
	if reply != nil && reply.Error == nil && reply.Result != nil {
		reply.Result = mcp.DecorateModernResult(reply.Result, d.serverInfo(),
			modernCacheable(msg.Method), mcp.GatewayTTLMs, mcp.GatewayCacheScope)
	}
	return reply
}

// routeModern dispatches a version-checked modern request to a handler. Methods
// removed on the modern revision (initialize, ping, logging/setLevel — and,
// until 19d, subscriptions/listen) are rejected -32601; server/discover is the
// one genuinely new handler; everything else reuses the legacy handlers, which
// are era-agnostic (they read only params.name/arguments/uri, never _meta).
func (d *dispatcher) routeModern(ctx context.Context, msg *mcp.Message) *mcp.Message {
	switch msg.Method {
	case mcp.MethodServerDiscover:
		return d.handleDiscover(msg)
	case mcp.MethodSubscriptionsListen:
		// Arrives in 19d; until then it is an unknown method on this era.
		return mcp.NewError(msg.ID, mcp.CodeMethodNotFound, "method not found: "+msg.Method, nil)
	case mcp.MethodInitialize:
		// Removed on the modern revision. This branch is dead for a well-formed
		// client — the era test above only fires on a request that carries
		// modern _meta, and no conformant client sends initialize with it — but
		// it is kept as a guard against a future "initialize with modern _meta"
		// hybrid, and the error names the supported versions per versioning.mdx.
		return mcp.NewError(msg.ID, mcp.CodeMethodNotFound,
			"method not found on the modern revision: initialize (supported versions: "+
				mcp.ProtocolVersionModern+", "+mcp.ProtocolVersion+")", nil)
	case mcp.MethodPing, mcp.MethodLoggingSetLevel:
		// Removed on the modern revision (ping, logging/setLevel).
		return mcp.NewError(msg.ID, mcp.CodeMethodNotFound, "method not found: "+msg.Method, nil)
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
		return d.handleResourcesRead(ctx, msg)
	case mcp.MethodResourceTemplatesList:
		return d.handleResourceTemplatesList(msg)
	case mcp.MethodCompletionComplete:
		return d.handleCompletionComplete(ctx, msg)
	default:
		return mcp.NewError(msg.ID, mcp.CodeMethodNotFound, "method not found: "+msg.Method, nil)
	}
}

// modernCacheable reports whether a modern method's result carries the
// CacheableResult fields (ttlMs, cacheScope). The set is fixed by the spec:
// tools/list, prompts/list, resources/list, resources/templates/list,
// resources/read, server/discover. A tools/call result, notably, is NOT
// cacheable — it gets resultType but no ttlMs.
func modernCacheable(method string) bool {
	switch method {
	case mcp.MethodToolsList, mcp.MethodPromptsList, mcp.MethodResourceList,
		mcp.MethodResourceTemplatesList, mcp.MethodResourceRead, mcp.MethodServerDiscover:
		return true
	default:
		return false
	}
}

// modernMethodKnown reports whether the gateway recognizes method on the modern
// revision — i.e. whether routeModern would answer anything other than an
// unknown-method -32601. It is the single source of the HTTP 404-vs-200
// decision (streamable-http.mdx wants an unknown modern method to be HTTP 404 so
// a client can tell a modern server apart from a legacy HTTP+SSE one) and the
// stdio "does this request need the registry" decision, so the two transports
// cannot disagree with routeModern about what "known" means.
//
// The removed-on-modern methods (initialize, ping, logging/setLevel) are
// deliberately UNKNOWN here: routeModern answers them -32601, and they must map
// to HTTP 404 exactly like any other unknown method, and must not wake the
// registry on stdio. subscriptions/listen is also unknown until 19d.
func modernMethodKnown(method string) bool {
	switch method {
	case mcp.MethodServerDiscover,
		mcp.MethodToolsList, mcp.MethodToolsCall,
		mcp.MethodPromptsList, mcp.MethodPromptsGet,
		mcp.MethodResourceList, mcp.MethodResourceRead, mcp.MethodResourceTemplatesList,
		mcp.MethodCompletionComplete:
		return true
	default:
		return false
	}
}

// handleDiscover answers server/discover: the supported protocol versions, the
// gateway's aggregated capabilities (modern flavour — no logging), and the
// concatenated upstream instructions. The gateway MUST implement it (servers
// MUST per the spec); a modern client MAY call it before its first real request.
func (d *dispatcher) handleDiscover(req *mcp.Message) *mcp.Message {
	return mcp.NewResult(req.ID, mcp.MustParams(mcp.DiscoverResult{
		ResultType:        mcp.ResultTypeComplete,
		SupportedVersions: mcp.SupportedVersions,
		Capabilities:      buildCapabilities(d.reg, d.listChanged, true /*modern*/),
		Instructions:      d.reg.Instructions(),
		TTLMs:             mcp.GatewayTTLMs,
		CacheScope:        mcp.GatewayCacheScope,
	}))
}

// serverReqCapsFromModernMeta translates a modern request's
// _meta.clientCapabilities (raw JSON, e.g. {"elicitation":{},"sampling":{},
// "roots":{"listChanged":true}}) into the same capability-name → raw-value map
// that clientServerRequestCaps builds from initialize params, filtered to the
// server→client capabilities the registry can actually proxy. It is the modern
// counterpart of clientServerRequestCaps: same output shape, same registry-
// derived name list (no drift-prone second copy), same tolerant parse — an
// unreadable or absent clientCapabilities simply declares nothing.
func serverReqCapsFromModernMeta(meta mcp.RequestMeta) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	if len(meta.ClientCapabilities) == 0 {
		return out
	}
	var caps map[string]json.RawMessage
	if json.Unmarshal(meta.ClientCapabilities, &caps) != nil {
		return out
	}
	for _, name := range registry.ServerRequestCapabilities() {
		if v, ok := caps[name]; ok {
			out[name] = v
		}
	}
	return out
}

// clientFromMeta renders the modern client's identity as "name/version" for
// CallRecord.Client — the modern counterpart of clientString, which reads it
// from initialize params. Empty when both clientInfo fields are empty (an
// unidentified client), exactly as clientString degrades.
func clientFromMeta(meta mcp.RequestMeta) string {
	if meta.ClientInfo.Name == "" && meta.ClientInfo.Version == "" {
		return ""
	}
	return meta.ClientInfo.Name + "/" + meta.ClientInfo.Version
}
