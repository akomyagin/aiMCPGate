// Lazy catalog mode (Round 8): progressive tool disclosure.
//
// With catalog_mode: lazy, tools/list answers with exactly THREE gateway
// meta-tools instead of the aggregated catalog, no matter how many upstream
// tools exist:
//
//	gate_search_tools(query)   — discover tools by substring
//	gate_describe(name)        — fetch one tool's full definition
//	gate_call(name, arguments) — invoke a real tool through the gateway
//
// The whole feature lives in the dispatcher: Registry.Tools()/CallTool() are
// untouched — gate_search_tools/gate_describe read the same aggregated catalog
// tools/list would serve, and gate_call funnels into the very same
// Registry.CallTool path a direct namespaced tools/call takes (routing, rate
// limits, truncation, audit log — all apply identically).
//
// Reserved names: handleToolsCall intercepts the three gate_* names BEFORE the
// normal Registry.CallTool resolution, so an upstream tool that happened to be
// namespaced to one of them is shadowed in lazy mode (and untouched in normal
// mode); it stays reachable via gate_call, which routes any name through the
// registry. No collision detection at merge time is needed — the interception
// order is the policy.
//
// (File-level doc; the package comment lives in transport.go.)

package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// Reserved client-facing names of the gateway meta-tools.
const (
	lazyToolSearch   = "gate_search_tools"
	lazyToolDescribe = "gate_describe"
	lazyToolCall     = "gate_call"
)

// lazySearchDescLimit caps how much of each tool's description
// gate_search_tools quotes per line — enough to pick a candidate, small
// enough to keep the whole listing cheap in tokens (the point of lazy mode).
const lazySearchDescLimit = 200

// lazyCatalogTools returns the fixed three-tool catalog lazy-mode tools/list
// serves. Rebuilt per call (a fresh slice, so no caller can mutate a shared
// one); the contents are constant.
func lazyCatalogTools() []mcp.Tool {
	return []mcp.Tool{
		{
			Name:        lazyToolSearch,
			Description: json.RawMessage(`"Search the gateway's aggregated tool catalog. Case-insensitive substring match against tool names and descriptions; returns one tool per line (name — start of description). Call with an empty or absent query to list every available tool."`),
			InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","description":"Case-insensitive substring matched against tool names and descriptions. Empty or absent lists the whole catalog."}}}`),
		},
		{
			Name:        lazyToolDescribe,
			Description: json.RawMessage(`"Return the full definition (name, description, inputSchema, ...) of one tool from the gateway's aggregated catalog, by the exact name gate_search_tools reported."`),
			InputSchema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string","description":"Exact tool name as reported by gate_search_tools."}},"required":["name"]}`),
		},
		{
			Name:        lazyToolCall,
			Description: json.RawMessage(`"Call a tool from the gateway's aggregated catalog by its exact name. Use gate_search_tools to discover names and gate_describe to see a tool's input schema first."`),
			InputSchema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string","description":"Exact tool name as reported by gate_search_tools."},"arguments":{"type":"object","description":"Arguments for the tool, matching its inputSchema."}},"required":["name"]}`),
		},
	}
}

// lazyContentBlock / lazyCallResult spell the wire shape of a CallToolResult
// (MCP 2025-06-18) for the SYNTHETIC results the meta-tools produce — the
// gateway normally forwards upstream results verbatim and has no typed
// CallToolResult, so lazy mode builds the same
// {"content":[{"type":"text","text":...}]} JSON by hand, structurally
// indistinguishable from a proxied one.
type lazyContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type lazyCallResult struct {
	Content []lazyContentBlock `json:"content"`
	// StructuredContent carries machine-readable output next to the text block
	// (spec: structured tool output) — gate_describe uses it for the tool
	// definition, which is structured data to begin with.
	StructuredContent any `json:"structuredContent,omitempty"`
	// IsError marks an in-band TOOL error (unknown name etc.) — not a protocol
	// error: the meta-tool ran fine and is answering "not found", which an
	// agent recovers from by re-searching.
	IsError bool `json:"isError,omitempty"`
}

// lazyResult wraps text (and optional structured content) as a tools/call
// result under the client's id.
func lazyResult(id json.RawMessage, text string, structured any, isErr bool) *mcp.Message {
	res := lazyCallResult{
		Content:           []lazyContentBlock{{Type: "text", Text: text}},
		StructuredContent: structured,
		IsError:           isErr,
	}
	return mcp.NewResult(id, mcp.MustParams(res))
}

// handleLazyCall dispatches the three reserved meta-tool names. handled=false
// means the name is not a meta-tool and the caller must continue down the
// normal Registry.CallTool path.
func (d *dispatcher) handleLazyCall(ctx context.Context, req *mcp.Message, params mcp.ToolsCallParams) (reply *mcp.Message, handled bool) {
	switch params.Name {
	case lazyToolSearch:
		return d.lazySearchTools(req, params.Arguments), true
	case lazyToolDescribe:
		return d.lazyDescribe(req, params.Arguments), true
	case lazyToolCall:
		return d.lazyForwardCall(ctx, req, params), true
	default:
		return nil, false
	}
}

// lazySearchTools implements gate_search_tools: a case-insensitive substring
// match of query against each aggregated tool's client-facing name OR
// description, answered as a compact text listing (one tool per line: name —
// first ~200 chars of description). An empty or absent query DELIBERATELY
// matches everything — "list the whole catalog" is the natural discovery
// entry point for an agent that knows nothing yet (documented in the tool's
// own description). Matching zero tools is a normal (non-error) result.
func (d *dispatcher) lazySearchTools(req *mcp.Message, args json.RawMessage) *mcp.Message {
	var p struct {
		Query string `json:"query"`
	}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &p); err != nil {
			return mcp.NewError(req.ID, mcp.CodeInvalidParams, "invalid gate_search_tools arguments: "+err.Error(), nil)
		}
	}
	q := strings.ToLower(p.Query)

	var b strings.Builder
	matched := 0
	for _, dd := range d.reg.Tools() {
		desc := descriptionText(dd.Tool.Description)
		if q != "" &&
			!strings.Contains(strings.ToLower(dd.Name), q) &&
			!strings.Contains(strings.ToLower(desc), q) {
			continue
		}
		matched++
		b.WriteString(dd.Name)
		if desc != "" {
			b.WriteString(" — ")
			// Collapse internal whitespace/newlines so the listing stays
			// strictly one tool per line, then cap the quoted length.
			b.WriteString(truncateRunes(strings.Join(strings.Fields(desc), " "), lazySearchDescLimit))
		}
		b.WriteByte('\n')
	}
	if matched == 0 {
		return lazyResult(req.ID, fmt.Sprintf("no tools matched %q; call gate_search_tools with an empty query to list every available tool", p.Query), nil, false)
	}
	return lazyResult(req.ID, strings.TrimSuffix(b.String(), "\n"), nil, false)
}

// lazyDescribe implements gate_describe: an exact-name lookup returning the
// full mcp.Tool definition — as JSON text (backward-compatible content block)
// AND as structuredContent (the data is structured to begin with). The three
// meta-tools themselves are describable too: they are what the lazy client
// actually sees in tools/list. An unknown name is an in-band isError result,
// not a protocol error — the agent's recovery is another gate_search_tools.
func (d *dispatcher) lazyDescribe(req *mcp.Message, args json.RawMessage) *mcp.Message {
	var p struct {
		Name string `json:"name"`
	}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &p); err != nil {
			return mcp.NewError(req.ID, mcp.CodeInvalidParams, "invalid gate_describe arguments: "+err.Error(), nil)
		}
	}
	if p.Name == "" {
		return mcp.NewError(req.ID, mcp.CodeInvalidParams, "gate_describe missing tool name", nil)
	}

	tool, ok := d.findLazyTool(p.Name)
	if !ok {
		return lazyResult(req.ID, fmt.Sprintf("unknown tool %q — use gate_search_tools to discover available tool names", p.Name), nil, true)
	}
	b, err := json.Marshal(tool)
	if err != nil {
		// Unreachable: mcp.Tool is strings and raw JSON. Kept non-panicking.
		return mcp.NewError(req.ID, mcp.CodeInternalError, "encode tool definition", nil)
	}
	return lazyResult(req.ID, string(b), json.RawMessage(b), false)
}

// findLazyTool resolves an exact client-facing name: the three meta-tools
// first (they win the reserved names, mirroring handleToolsCall's interception
// order), then the aggregated catalog.
func (d *dispatcher) findLazyTool(name string) (mcp.Tool, bool) {
	for _, t := range lazyCatalogTools() {
		if t.Name == name {
			return t, true
		}
	}
	for _, dd := range d.reg.Tools() {
		if dd.Name == name {
			t := dd.Tool
			t.Name = dd.Name // client-facing namespaced name, as everywhere
			return t, true
		}
	}
	return mcp.Tool{}, false
}

// lazyForwardCall implements gate_call: unwrap {name, arguments} and continue
// down the ORDINARY tools/call path — the same Registry.CallTool a direct
// namespaced call takes (routing, limits, truncation, audit), with the same
// response handling as handleToolsCall's tail. The ORIGINAL request's `_meta`
// (e.g. progressToken) rides along, so progress forwarding keeps working
// through the wrapper.
func (d *dispatcher) lazyForwardCall(ctx context.Context, req *mcp.Message, params mcp.ToolsCallParams) *mcp.Message {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if len(params.Arguments) > 0 {
		if err := json.Unmarshal(params.Arguments, &p); err != nil {
			return mcp.NewError(req.ID, mcp.CodeInvalidParams, "invalid gate_call arguments: "+err.Error(), nil)
		}
	}
	if p.Name == "" {
		return mcp.NewError(req.ID, mcp.CodeInvalidParams, "gate_call missing tool name", nil)
	}

	resp, err := d.reg.CallTool(ctx, p.Name, p.Arguments, params.Meta)
	if err != nil {
		// Same sanitized-error contract as handleToolsCall: err.Error() names
		// only the tool the client itself supplied, never arguments/topology.
		// The shared toolCallError also maps a guard refusal to CodeGatewayBusy
		// with retryable data, so gate_call and a direct call speak one dialect.
		return toolCallError(req.ID, err)
	}
	if resp.Error != nil {
		return mcp.NewError(req.ID, resp.Error.Code, resp.Error.Message, resp.Error.Data)
	}
	return mcp.NewResult(req.ID, resp.Result)
}

// descriptionText unwraps a tool's raw JSON description into plain text: a
// JSON string decodes normally; anything else (absent, or a non-string an
// odd upstream sent) degrades to its raw JSON text rather than being dropped.
func descriptionText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// truncateRunes caps s at max runes, appending an ellipsis when it actually
// truncated. Rune-based so a multi-byte description is never cut mid-character.
func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}
