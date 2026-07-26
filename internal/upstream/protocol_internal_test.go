package upstream

// White-box tests for the shared pagination loop (protocol.go): they need a
// fake in-package transport, since no exported constructor accepts one.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// pagingTransport is a fake transport whose list responses keep paginating.
// itemsKey selects the result shape ("tools" or "resources"); pages is how
// many pages it serves before returning an empty nextCursor — 0 means it
// NEVER stops (the malicious/broken upstream the page cap defends against).
type pagingTransport struct {
	itemsKey string
	pages    int
	calls    int
}

func (p *pagingTransport) call(_ context.Context, _ string, _ json.RawMessage) (*mcp.Message, error) {
	p.calls++
	next := fmt.Sprintf("cursor-%d", p.calls)
	if p.pages > 0 && p.calls >= p.pages {
		next = ""
	}
	res := fmt.Sprintf(`{%q:[{"name":"item-%d","uri":"fake://item-%d"}],"nextCursor":%q}`,
		p.itemsKey, p.calls, p.calls, next)
	return mcp.NewResult(mcp.IntID(int64(p.calls)), json.RawMessage(res)), nil
}

func (p *pagingTransport) notify(context.Context, string, json.RawMessage) error { return nil }
func (p *pagingTransport) Name() string                                          { return "paging" }
func (p *pagingTransport) Close() error                                          { return nil }
func (p *pagingTransport) Done() (<-chan struct{}, bool)                         { return nil, false }

// TestListToolsFollowsPagination guards the paginate refactor: a well-behaved
// multi-page catalog is still aggregated across all pages, in order.
func TestListToolsFollowsPagination(t *testing.T) {
	tr := &pagingTransport{itemsKey: "tools", pages: 3}
	c := &Conn{transport: tr}

	tools, err := c.ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools) != 3 {
		t.Fatalf("got %d tools, want 3 (one per page)", len(tools))
	}
	if tools[0].Name != "item-1" || tools[2].Name != "item-3" {
		t.Errorf("pages aggregated out of order: %+v", tools)
	}
	if tr.calls != 3 {
		t.Errorf("made %d calls, want 3", tr.calls)
	}
}

// TestListPromptsFollowsPagination is TestListToolsFollowsPagination's Round 4
// twin: a multi-page prompt catalog is aggregated across all pages, in order.
func TestListPromptsFollowsPagination(t *testing.T) {
	tr := &pagingTransport{itemsKey: "prompts", pages: 3}
	c := &Conn{transport: tr}

	prompts, err := c.ListPrompts(context.Background())
	if err != nil {
		t.Fatalf("ListPrompts: %v", err)
	}
	if len(prompts) != 3 {
		t.Fatalf("got %d prompts, want 3 (one per page)", len(prompts))
	}
	if prompts[0].Name != "item-1" || prompts[2].Name != "item-3" {
		t.Errorf("pages aggregated out of order: %+v", prompts)
	}
	if tr.calls != 3 {
		t.Errorf("made %d calls, want 3", tr.calls)
	}
}

// TestListPromptsErrorStaysFatal pins the deliberate asymmetry with
// ListResources: prompts/list is only ever called for an upstream that
// DECLARED the prompts capability, so even method-not-found is a hard error
// here (the registry, not this layer, decides to degrade it to "no prompts").
func TestListPromptsErrorStaysFatal(t *testing.T) {
	c := &Conn{transport: &errorTransport{code: mcp.CodeMethodNotFound}}
	if _, err := c.ListPrompts(context.Background()); err == nil {
		t.Fatal("prompts/list method-not-found must stay a hard error")
	}
}

// TestGetPromptWireParams pins the wire contract of GetPrompt: params carry
// the (already original, un-namespaced) name and the caller's arguments byte
// for byte; absent arguments stay absent — nothing invented.
func TestGetPromptWireParams(t *testing.T) {
	tr := &captureTransport{}
	c := &Conn{transport: tr}

	args := json.RawMessage(`{"style":"formal","n":[1,2]}`)
	if _, err := c.GetPrompt(context.Background(), "greet", args); err != nil {
		t.Fatalf("GetPrompt with arguments: %v", err)
	}
	var withArgs map[string]json.RawMessage
	if err := json.Unmarshal(tr.params, &withArgs); err != nil {
		t.Fatalf("decode sent params: %v", err)
	}
	if got := string(withArgs["name"]); got != `"greet"` {
		t.Errorf("sent name = %s, want \"greet\"", got)
	}
	if got := string(withArgs["arguments"]); got != string(args) {
		t.Errorf("sent arguments = %s, want the caller's bytes %s", got, args)
	}

	if _, err := c.GetPrompt(context.Background(), "greet", nil); err != nil {
		t.Fatalf("GetPrompt without arguments: %v", err)
	}
	var noArgs map[string]json.RawMessage
	if err := json.Unmarshal(tr.params, &noArgs); err != nil {
		t.Fatalf("decode sent params: %v", err)
	}
	if _, present := noArgs["arguments"]; present {
		t.Errorf("params carry an invented arguments key: %s", tr.params)
	}
}

// TestListPaginationBounded pins the page cap: an upstream that ALWAYS returns
// a non-empty nextCursor must produce an error after maxPaginationPages pages
// — not spin the loop forever (previously only a caller-side ctx timeout, if
// any, could stop it).
func TestListPaginationBounded(t *testing.T) {
	cases := []struct {
		name     string
		itemsKey string
		list     func(c *Conn) error
	}{
		{"tools", "tools", func(c *Conn) error { _, err := c.ListTools(context.Background()); return err }},
		{"resources", "resources", func(c *Conn) error { _, err := c.ListResources(context.Background()); return err }},
		{"prompts", "prompts", func(c *Conn) error { _, err := c.ListPrompts(context.Background()); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := &pagingTransport{itemsKey: tc.itemsKey} // pages=0: endless cursors
			c := &Conn{transport: tr}

			err := tc.list(c)
			if err == nil {
				t.Fatal("endless nextCursor: want an error, got nil")
			}
			if !strings.Contains(err.Error(), "exceeded") || !strings.Contains(err.Error(), "pages") {
				t.Errorf("error should name the page cap, got: %v", err)
			}
			if tr.calls != maxPaginationPages {
				t.Errorf("made %d calls before giving up, want exactly %d", tr.calls, maxPaginationPages)
			}
		})
	}
}

// errorTransport answers every call with a fixed JSON-RPC error.
type errorTransport struct{ code int }

func (e *errorTransport) call(_ context.Context, _ string, _ json.RawMessage) (*mcp.Message, error) {
	return mcp.NewError(mcp.IntID(1), e.code, "nope", nil), nil
}
func (e *errorTransport) notify(context.Context, string, json.RawMessage) error { return nil }
func (e *errorTransport) Name() string                                          { return "erroring" }
func (e *errorTransport) Close() error                                          { return nil }
func (e *errorTransport) Done() (<-chan struct{}, bool)                         { return nil, false }

// TestListResourcesMethodNotFoundIsEmptyCatalog guards the one asymmetry the
// paginate refactor had to preserve: resources/list answered with
// method-not-found (upstream has no resources capability) is an EMPTY catalog,
// not an error — while for tools/list the same answer stays fatal.
func TestListResourcesMethodNotFoundIsEmptyCatalog(t *testing.T) {
	c := &Conn{transport: &errorTransport{code: mcp.CodeMethodNotFound}}

	res, err := c.ListResources(context.Background())
	if err != nil {
		t.Fatalf("method-not-found must mean empty catalog, got error: %v", err)
	}
	if len(res) != 0 {
		t.Fatalf("want empty catalog, got %+v", res)
	}

	if _, err := c.ListTools(context.Background()); err == nil {
		t.Fatal("tools/list method-not-found must stay a hard error")
	}

	// Any OTHER error code stays fatal for resources too.
	c = &Conn{transport: &errorTransport{code: mcp.CodeInternalError}}
	if _, err := c.ListResources(context.Background()); err == nil {
		t.Fatal("resources/list internal error must stay a hard error")
	}
}

// captureTransport records the raw params of the last call it received and
// answers with an empty successful result — the wire-level probe for what
// CallTool actually sends.
type captureTransport struct {
	params json.RawMessage
}

func (c *captureTransport) call(_ context.Context, _ string, params json.RawMessage) (*mcp.Message, error) {
	c.params = params
	return mcp.NewResult(mcp.IntID(1), json.RawMessage(`{}`)), nil
}

func (c *captureTransport) notify(context.Context, string, json.RawMessage) error { return nil }
func (c *captureTransport) Name() string                                          { return "capture" }
func (c *captureTransport) Close() error                                          { return nil }
func (c *captureTransport) Done() (<-chan struct{}, bool)                         { return nil, false }

// TestCallToolWireParamsCarryMetaExactly pins the wire contract of the `_meta`
// passthrough: the params CallTool sends carry the client's `_meta` object
// byte for byte (compact JSON in, identical bytes out), and when the caller
// supplies none the params contain NO `_meta` key at all — the upstream must
// see exactly what the client sent, nothing invented.
func TestCallToolWireParamsCarryMetaExactly(t *testing.T) {
	tr := &captureTransport{}
	c := &Conn{transport: tr}

	meta := json.RawMessage(`{"progressToken":42,"vendor":{"k":[1,2]}}`)
	if _, err := c.CallTool(context.Background(), "fetch", json.RawMessage(`{"q":"x"}`), meta); err != nil {
		t.Fatalf("CallTool with meta: %v", err)
	}
	var withMeta map[string]json.RawMessage
	if err := json.Unmarshal(tr.params, &withMeta); err != nil {
		t.Fatalf("decode sent params: %v", err)
	}
	if got := string(withMeta["_meta"]); got != string(meta) {
		t.Errorf("sent _meta = %s, want the client's bytes %s", got, meta)
	}

	if _, err := c.CallTool(context.Background(), "fetch", json.RawMessage(`{"q":"x"}`), nil); err != nil {
		t.Fatalf("CallTool without meta: %v", err)
	}
	var noMeta map[string]json.RawMessage
	if err := json.Unmarshal(tr.params, &noMeta); err != nil {
		t.Fatalf("decode sent params: %v", err)
	}
	if _, present := noMeta["_meta"]; present {
		t.Errorf("params carry an invented _meta key: %s", tr.params)
	}
}
