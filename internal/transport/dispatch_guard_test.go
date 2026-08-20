// Part A of fix/client-facing-guard-signals: a per-upstream guard refusal of a
// tools/call reaches the client as the gateway's own CodeGatewayBusy (-32029)
// with machine-readable retryable data, both on the direct namespaced path
// (handleToolsCall) and through gate_call (lazyForwardCall).

package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// guardData is the decoded shape of a guard refusal's error.data.
type guardData struct {
	Retryable bool   `json:"retryable"`
	Reason    string `json:"reason"`
}

// A4. TestToolCallErrorMapsGuardSentinels: the shared mapper turns each guard
// sentinel into CodeGatewayBusy with the right reason, and leaves an ordinary
// failure as -32603 without data.
func TestToolCallErrorMapsGuardSentinels(t *testing.T) {
	id := mcp.IntID(7)
	cases := []struct {
		name       string
		err        error
		wantCode   int
		wantReason string // "" means expect no data
	}{
		{"rate limit", fmt.Errorf("call %q refused: %w", "a__t", registry.ErrRateLimited), mcp.CodeGatewayBusy, "rate_limit"},
		{"concurrency", fmt.Errorf("call %q refused: %w", "a__t", registry.ErrConcurrencyLimited), mcp.CodeGatewayBusy, "concurrency_limit"},
		{"plain failure", errors.New(`call "a__t" failed`), mcp.CodeInternalError, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := toolCallError(id, tc.err)
			if msg.Error == nil {
				t.Fatalf("toolCallError returned no error message")
			}
			if msg.Error.Code != tc.wantCode {
				t.Errorf("code = %d, want %d", msg.Error.Code, tc.wantCode)
			}
			if tc.wantReason == "" {
				if msg.Error.Data != nil {
					t.Errorf("plain failure carried data %s, want none", msg.Error.Data)
				}
				return
			}
			var d guardData
			if err := json.Unmarshal(msg.Error.Data, &d); err != nil {
				t.Fatalf("decode data %s: %v", msg.Error.Data, err)
			}
			if !d.Retryable || d.Reason != tc.wantReason {
				t.Errorf("data = %+v, want retryable=true reason=%q", d, tc.wantReason)
			}
		})
	}
}

// rateLimitedDispatcher builds a live registry over the fakeserver with a
// rate_limit that admits exactly one token, then returns a dispatcher wired to
// it. catalogMode selects normal vs lazy.
func rateLimitedDispatcher(t *testing.T, catalogMode string) *dispatcher {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		CatalogMode: catalogMode,
		RateLimit:   &config.RateLimit{RPS: 0.001, Burst: 1}, // next token in ~17 minutes
		Upstreams: []config.Upstream{
			{Name: "github", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME":  "github",
				"FAKE_TOOLS": "search",
				"FAKE_ECHO":  "1",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry Start: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	return newDispatcher(reg, quietLogger(), "test", true, false, false)
}

// A5. TestDispatchToolsCallRateLimitedRetryable: two direct tools/call requests
// under a burst-1 limiter; the second is refused by the gateway and returns
// -32029 with retryable data, under the client's own id.
func TestDispatchToolsCallRateLimitedRetryable(t *testing.T) {
	d := rateLimitedDispatcher(t, "")

	call := func(id int64) *mcp.Message {
		req := mcp.NewRequest(mcp.IntID(id), mcp.MethodToolsCall,
			mcp.MustParams(mcp.ToolsCallParams{Name: "github__search"}))
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		return d.dispatch(ctx, req)
	}

	if reply := call(1); reply.Error != nil {
		t.Fatalf("first call refused unexpectedly: %+v", reply.Error)
	}
	reply := call(2)
	if reply.Error == nil {
		t.Fatalf("second call not refused; want CodeGatewayBusy")
	}
	if string(reply.ID) != "2" {
		t.Errorf("reply id = %s, want the client id 2", reply.ID)
	}
	if reply.Error.Code != mcp.CodeGatewayBusy {
		t.Errorf("error code = %d, want %d", reply.Error.Code, mcp.CodeGatewayBusy)
	}
	var gd guardData
	if err := json.Unmarshal(reply.Error.Data, &gd); err != nil {
		t.Fatalf("decode data %s: %v", reply.Error.Data, err)
	}
	if !gd.Retryable || gd.Reason != "rate_limit" {
		t.Errorf("data = %+v, want retryable=true reason=rate_limit", gd)
	}
}

// A7. TestLazyGateCallGuardRefusalRetryable: the same refusal reached through
// the gate_call meta-tool (lazy catalog mode) also yields -32029 + retryable
// data — pinning the shared toolCallError in lazyForwardCall.
func TestLazyGateCallGuardRefusalRetryable(t *testing.T) {
	d := rateLimitedDispatcher(t, config.CatalogModeLazy)

	call := func(id int64) *mcp.Message {
		inner, _ := json.Marshal(struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}{Name: "github__search"})
		req := mcp.NewRequest(mcp.IntID(id), mcp.MethodToolsCall,
			mcp.MustParams(mcp.ToolsCallParams{Name: "gate_call", Arguments: inner}))
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		return d.dispatch(ctx, req)
	}

	if reply := call(1); reply.Error != nil {
		t.Fatalf("first gate_call refused unexpectedly: %+v", reply.Error)
	}
	reply := call(2)
	if reply.Error == nil {
		t.Fatalf("second gate_call not refused; want CodeGatewayBusy")
	}
	if reply.Error.Code != mcp.CodeGatewayBusy {
		t.Errorf("error code = %d, want %d", reply.Error.Code, mcp.CodeGatewayBusy)
	}
	var gd guardData
	if err := json.Unmarshal(reply.Error.Data, &gd); err != nil {
		t.Fatalf("decode data %s: %v", reply.Error.Data, err)
	}
	if !gd.Retryable || gd.Reason != "rate_limit" {
		t.Errorf("data = %+v, want retryable=true reason=rate_limit", gd)
	}
}
