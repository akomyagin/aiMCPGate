// Round 8 tests: lazy catalog mode (gate_search_tools / gate_describe /
// gate_call) and client-side tools/list pagination (page_size).

package transport

import (
	"encoding/base64"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// lazyTestConfig builds a one-upstream config over the fakeserver with the
// given catalog knobs. tools is the FAKE_TOOLS list the fakeserver advertises
// (each tool gets description "fake tool <name>" and FAKE_ECHO echoes call
// arguments back).
func lazyTestConfig(t *testing.T, tools, catalogMode string, pageSize int) *config.Config {
	t.Helper()
	bin := buildFakeServer(t)
	return &config.Config{
		CatalogMode: catalogMode,
		PageSize:    pageSize,
		Upstreams: []config.Upstream{
			{Name: "github", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_NAME":  "github",
				"FAKE_TOOLS": tools,
				"FAKE_ECHO":  "1",
			}},
		},
	}
}

// toolsList performs one tools/list round-trip (with optional cursor) and
// decodes the result, failing the test on a JSON-RPC error.
func toolsList(t *testing.T, c *fakeClient, cursor string) mcp.ToolsListResult {
	t.Helper()
	var params json.RawMessage
	if cursor != "" {
		params = mcp.MustParams(mcp.ToolsListParams{Cursor: cursor})
	}
	c.request(mcp.MethodToolsList, params)
	resp := c.readResponse()
	if resp.Error != nil {
		t.Fatalf("tools/list error: %+v", resp.Error)
	}
	var res mcp.ToolsListResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode tools/list result: %v", err)
	}
	return res
}

// callToolRaw performs one tools/call round-trip and returns the raw response
// (which may carry a JSON-RPC error) plus the client id used.
func callToolRaw(t *testing.T, c *fakeClient, name string, args json.RawMessage) (*mcp.Message, json.RawMessage) {
	t.Helper()
	id := c.request(mcp.MethodToolsCall, mcp.MustParams(mcp.ToolsCallParams{Name: name, Arguments: args}))
	return c.readResponse(), id
}

// gateResult is the decoded shape of a synthetic meta-tool result:
// {"content":[{"type":"text","text":...}],"structuredContent":...,"isError":...}.
type gateResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StructuredContent json.RawMessage `json:"structuredContent"`
	IsError           bool            `json:"isError"`
}

// callGate calls a meta-tool and decodes the synthetic CallToolResult.
func callGate(t *testing.T, c *fakeClient, name string, args json.RawMessage) gateResult {
	t.Helper()
	resp, _ := callToolRaw(t, c, name, args)
	if resp.Error != nil {
		t.Fatalf("%s error: %+v", name, resp.Error)
	}
	var res gateResult
	if err := json.Unmarshal(resp.Result, &res); err != nil {
		t.Fatalf("decode %s result %s: %v", name, resp.Result, err)
	}
	if len(res.Content) != 1 || res.Content[0].Type != "text" {
		t.Fatalf("%s result content = %s, want exactly one text block", name, resp.Result)
	}
	return res
}

// TestLazyToolsListExactlyThreeMetaTools: in lazy mode tools/list serves
// EXACTLY the three gateway meta-tools — never the aggregated catalog — each
// with an inputSchema. page_size is set too, pinning the documented
// precedence: lazy wins, the fixed list of three never paginates (no
// nextCursor).
func TestLazyToolsListExactlyThreeMetaTools(t *testing.T) {
	cfg := lazyTestConfig(t, "search,create_issue", config.CatalogModeLazy, 2)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	res := toolsList(t, c, "")
	var names []string
	for _, tl := range res.Tools {
		names = append(names, tl.Name)
		if len(tl.InputSchema) == 0 {
			t.Errorf("meta-tool %q has no inputSchema", tl.Name)
		}
		if len(tl.Description) == 0 {
			t.Errorf("meta-tool %q has no description", tl.Name)
		}
	}
	sort.Strings(names)
	want := []string{"gate_call", "gate_describe", "gate_search_tools"}
	if len(names) != 3 || names[0] != want[0] || names[1] != want[1] || names[2] != want[2] {
		t.Fatalf("lazy tools/list = %v, want exactly %v", names, want)
	}
	if res.NextCursor != "" {
		t.Errorf("lazy tools/list has nextCursor %q; lazy mode must ignore page_size", res.NextCursor)
	}
}

// TestLazySearchTools covers gate_search_tools matching: by name
// (case-insensitively), by description, the documented empty-query behaviour
// (lists the WHOLE catalog), and the zero-match answer (a normal non-error
// result with a hint, never isError).
func TestLazySearchTools(t *testing.T) {
	cfg := lazyTestConfig(t, "search,create_issue", config.CatalogModeLazy, 0)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	// By name, case-insensitive: "CREATE_ISSUE" matches github__create_issue.
	res := callGate(t, c, "gate_search_tools", json.RawMessage(`{"query":"CREATE_ISSUE"}`))
	if !strings.Contains(res.Content[0].Text, "github__create_issue") {
		t.Errorf("search by name: %q does not list github__create_issue", res.Content[0].Text)
	}
	if strings.Contains(res.Content[0].Text, "github__search") {
		t.Errorf("search by name: %q unexpectedly lists github__search", res.Content[0].Text)
	}

	// By description: fakeserver advertises description "fake tool <name>";
	// "FAKE TOOL" appears in no tool NAME, only in descriptions — and matches
	// both tools, case-insensitively.
	res = callGate(t, c, "gate_search_tools", json.RawMessage(`{"query":"FAKE TOOL"}`))
	for _, wantName := range []string{"github__search", "github__create_issue"} {
		if !strings.Contains(res.Content[0].Text, wantName) {
			t.Errorf("search by description: %q does not list %s", res.Content[0].Text, wantName)
		}
	}

	// Empty query lists the whole catalog (documented discovery entry point).
	res = callGate(t, c, "gate_search_tools", json.RawMessage(`{}`))
	lines := strings.Split(res.Content[0].Text, "\n")
	if len(lines) != 2 {
		t.Errorf("empty query listed %d lines, want 2 (whole catalog): %q", len(lines), res.Content[0].Text)
	}

	// Zero matches: a normal result with a hint, not an error.
	res = callGate(t, c, "gate_search_tools", json.RawMessage(`{"query":"no-such-thing"}`))
	if res.IsError {
		t.Errorf("zero-match search must not be isError")
	}
	if !strings.Contains(res.Content[0].Text, "no tools matched") {
		t.Errorf("zero-match search text = %q, want a 'no tools matched' hint", res.Content[0].Text)
	}
}

// TestLazyDescribe covers gate_describe: the happy path returns the FULL tool
// definition (as JSON text and as structuredContent) under the client-facing
// namespaced name; an unknown name is an in-band isError result, not a
// protocol error; the meta-tools themselves are describable.
func TestLazyDescribe(t *testing.T) {
	cfg := lazyTestConfig(t, "search", config.CatalogModeLazy, 0)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	res := callGate(t, c, "gate_describe", json.RawMessage(`{"name":"github__search"}`))
	if res.IsError {
		t.Fatalf("describe of a known tool must not be isError: %+v", res)
	}
	var tool mcp.Tool
	if err := json.Unmarshal(res.StructuredContent, &tool); err != nil {
		t.Fatalf("structuredContent is not a tool definition: %v (%s)", err, res.StructuredContent)
	}
	if tool.Name != "github__search" {
		t.Errorf("described tool name = %q, want github__search", tool.Name)
	}
	if len(tool.InputSchema) == 0 {
		t.Errorf("described tool lost its inputSchema: %s", res.StructuredContent)
	}
	// The text block carries the same JSON for structuredContent-unaware clients.
	if !strings.Contains(res.Content[0].Text, `"github__search"`) {
		t.Errorf("describe text block %q does not carry the tool JSON", res.Content[0].Text)
	}

	// Unknown name: in-band tool error with a recovery hint.
	res = callGate(t, c, "gate_describe", json.RawMessage(`{"name":"nope__nope"}`))
	if !res.IsError {
		t.Fatalf("describe of unknown tool must be isError, got %+v", res)
	}
	if !strings.Contains(res.Content[0].Text, "gate_search_tools") {
		t.Errorf("unknown-name text %q should hint at gate_search_tools", res.Content[0].Text)
	}

	// The meta-tools describe themselves (they ARE the visible catalog).
	res = callGate(t, c, "gate_describe", json.RawMessage(`{"name":"gate_call"}`))
	if res.IsError {
		t.Errorf("gate_describe(gate_call) must succeed, got isError: %+v", res)
	}

	// Missing name: a protocol-level invalid-params error, not a tool result.
	resp, _ := callToolRaw(t, c, "gate_describe", json.RawMessage(`{}`))
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("describe without name: error = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}
}

// TestLazyGateCallReachesUpstream: gate_call unwraps {name, arguments} and
// funnels into the ordinary Registry.CallTool path end-to-end — the
// fakeserver's FAKE_ECHO answer proves the real upstream received the inner
// arguments intact, and the response carries the CLIENT's id. A direct
// namespaced call keeps working in lazy mode too (non-gate names fall through
// to normal routing), and an unknown inner name surfaces the registry's
// JSON-RPC error.
func TestLazyGateCallReachesUpstream(t *testing.T) {
	cfg := lazyTestConfig(t, "search", config.CatalogModeLazy, 0)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	resp, id := callToolRaw(t, c, "gate_call",
		json.RawMessage(`{"name":"github__search","arguments":{"q":"golang"}}`))
	if string(resp.ID) != string(id) {
		t.Fatalf("gate_call response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error != nil {
		t.Fatalf("gate_call error: %+v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "golang") {
		t.Errorf("gate_call result did not echo inner arguments through the upstream: %s", resp.Result)
	}

	// Direct namespaced call still routes normally in lazy mode.
	resp, _ = callToolRaw(t, c, "github__search", json.RawMessage(`{"q":"direct"}`))
	if resp.Error != nil {
		t.Fatalf("direct namespaced call in lazy mode errored: %+v", resp.Error)
	}
	if !strings.Contains(string(resp.Result), "direct") {
		t.Errorf("direct call result did not echo through: %s", resp.Result)
	}

	// Unknown inner tool: the registry's sanitized routing error comes back.
	resp, _ = callToolRaw(t, c, "gate_call", json.RawMessage(`{"name":"nope__nope"}`))
	if resp.Error == nil {
		t.Fatal("gate_call of unknown inner tool must return a JSON-RPC error")
	}

	// Missing inner name: invalid params.
	resp, _ = callToolRaw(t, c, "gate_call", json.RawMessage(`{"arguments":{}}`))
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("gate_call without name: error = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}
}

// TestNormalModeDoesNotReserveGateNames: without catalog_mode: lazy the gate_*
// names are not special — tools/list serves the aggregated catalog and a call
// to gate_search_tools routes like any unknown tool (registry error), proving
// the interception is strictly lazy-mode-only.
func TestNormalModeDoesNotReserveGateNames(t *testing.T) {
	cfg := lazyTestConfig(t, "search", "", 0)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	res := toolsList(t, c, "")
	if len(res.Tools) != 1 || res.Tools[0].Name != "github__search" {
		t.Fatalf("normal tools/list = %+v, want the aggregated catalog", res.Tools)
	}
	resp, _ := callToolRaw(t, c, "gate_search_tools", json.RawMessage(`{"query":"x"}`))
	if resp.Error == nil {
		t.Fatal("gate_search_tools in normal mode must be an unknown tool, got a result")
	}
}

// TestToolsListPagination walks a 5-tool catalog with page_size: 2 through all
// three pages: sizes 2+2+1, no duplicates, no omissions, cursors present on
// every page but the last.
func TestToolsListPagination(t *testing.T) {
	cfg := lazyTestConfig(t, "a1,b2,c3,d4,e5", "", 2)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	var all []string
	cursor := ""
	for page := 0; ; page++ {
		res := toolsList(t, c, cursor)
		wantLen := 2
		if page == 2 {
			wantLen = 1
		}
		if len(res.Tools) != wantLen {
			t.Fatalf("page %d has %d tools, want %d (%+v)", page, len(res.Tools), wantLen, res.Tools)
		}
		for _, tl := range res.Tools {
			all = append(all, tl.Name)
		}
		if res.NextCursor == "" {
			if page != 2 {
				t.Fatalf("pagination ended after page %d, want 3 pages", page)
			}
			break
		}
		if page == 2 {
			t.Fatalf("last page carries nextCursor %q, want none", res.NextCursor)
		}
		cursor = res.NextCursor
	}

	want := []string{"github__a1", "github__b2", "github__c3", "github__d4", "github__e5"}
	if len(all) != len(want) {
		t.Fatalf("walked %v, want %v (duplicates or omissions)", all, want)
	}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("walked %v, want %v", all, want)
		}
	}
}

// TestToolsListPaginationInvalidCursor: an unparseable (non-base64) cursor is
// answered with -32602 Invalid params, per MCP.
func TestToolsListPaginationInvalidCursor(t *testing.T) {
	cfg := lazyTestConfig(t, "a1,b2,c3", "", 2)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	c.request(mcp.MethodToolsList, mcp.MustParams(mcp.ToolsListParams{Cursor: "%%%not-base64%%%"}))
	resp := c.readResponse()
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Fatalf("invalid cursor: error = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}
}

// TestToolsListPaginationCursorSurvivesUnknownName: a syntactically valid
// cursor naming a tool that no longer exists (catalog mutated between pages)
// resumes at the first name AFTER it lexicographically — no error, no
// duplicates — because the cursor is name-based, not positional.
func TestToolsListPaginationCursorSurvivesUnknownName(t *testing.T) {
	cfg := lazyTestConfig(t, "a1,c3,e5", "", 2)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	// Cursor for "github__b2", which is not in the catalog: the next page must
	// start at github__c3 (first name > cursor).
	cursor := base64.StdEncoding.EncodeToString([]byte("github__b2"))
	res := toolsList(t, c, cursor)
	if len(res.Tools) != 2 || res.Tools[0].Name != "github__c3" || res.Tools[1].Name != "github__e5" {
		t.Fatalf("page after stale cursor = %+v, want [github__c3 github__e5]", res.Tools)
	}
	if res.NextCursor != "" {
		t.Errorf("exhausted page carries nextCursor %q, want none", res.NextCursor)
	}
}

// TestToolsListNoPaginationByDefault is the Round 8 regression guard: with
// page_size unset (0) tools/list keeps the pre-Round 8 behaviour — the whole
// catalog in one response, no nextCursor, any cursor in the request ignored.
func TestToolsListNoPaginationByDefault(t *testing.T) {
	cfg := lazyTestConfig(t, "a1,b2,c3,d4,e5", "", 0)
	c, cancel, done := startServerWithConfig(t, cfg, nil)
	defer func() { cancel(); <-done }()

	res := toolsList(t, c, "")
	if len(res.Tools) != 5 {
		t.Fatalf("tools/list returned %d tools, want the whole catalog of 5", len(res.Tools))
	}
	if res.NextCursor != "" {
		t.Errorf("unpaginated tools/list carries nextCursor %q", res.NextCursor)
	}
	// A cursor in the request is ignored when pagination is off (old behaviour:
	// params were never even parsed).
	res = toolsList(t, c, "@@@garbage@@@")
	if len(res.Tools) != 5 {
		t.Errorf("tools/list with cursor but page_size 0 returned %d tools, want 5 (cursor must be ignored)", len(res.Tools))
	}
}

// TestTruncateRunes pins the rune-safe truncation helper the search listing
// relies on (multi-byte text must never be cut mid-character).
func TestTruncateRunes(t *testing.T) {
	if got := truncateRunes("hello", 10); got != "hello" {
		t.Errorf("no-op truncation = %q", got)
	}
	if got := truncateRunes("привет мир", 6); got != "привет…" {
		t.Errorf("rune truncation = %q, want %q", got, "привет…")
	}
}
