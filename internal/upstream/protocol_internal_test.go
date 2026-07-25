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
