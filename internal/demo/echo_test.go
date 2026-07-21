package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// runDemo feeds the framed request lines to Run and returns the decoded
// responses in order. The input buffer's EOF ends the loop, so Run returns on
// its own; a background context keeps cancellation out of the picture.
func runDemo(t *testing.T, requests []string) []*mcp.Message {
	t.Helper()

	in := strings.NewReader(strings.Join(requests, "\n") + "\n")
	var out bytes.Buffer
	if err := Run(context.Background(), in, &out, "test-version"); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var responses []*mcp.Message
	r := mcp.NewReader(&out)
	for {
		msg, err := r.Read()
		if err != nil {
			break // io.EOF: all responses drained
		}
		responses = append(responses, msg)
	}
	return responses
}

func TestRunHandlesRequests(t *testing.T) {
	tests := []struct {
		name    string
		request string
		// check is invoked with the single response the request produced.
		check func(t *testing.T, resp *mcp.Message)
	}{
		{
			name:    "initialize",
			request: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}`,
			check: func(t *testing.T, resp *mcp.Message) {
				var result mcp.InitializeResult
				mustUnmarshal(t, resp.Result, &result)
				if result.ServerInfo.Name != "aimcpgate-demo-echo" {
					t.Errorf("serverInfo.name = %q, want aimcpgate-demo-echo", result.ServerInfo.Name)
				}
				if result.ServerInfo.Version != "test-version" {
					t.Errorf("serverInfo.version = %q, want test-version", result.ServerInfo.Version)
				}
				if result.ProtocolVersion != mcp.ProtocolVersion {
					t.Errorf("protocolVersion = %q, want %q", result.ProtocolVersion, mcp.ProtocolVersion)
				}
			},
		},
		{
			name:    "tools/list",
			request: `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
			check: func(t *testing.T, resp *mcp.Message) {
				var result mcp.ToolsListResult
				mustUnmarshal(t, resp.Result, &result)
				if len(result.Tools) != 2 {
					t.Fatalf("got %d tools, want 2: %+v", len(result.Tools), result.Tools)
				}
				if result.Tools[0].Name != "echo" || result.Tools[1].Name != "ping" {
					t.Errorf("tools = [%q, %q], want [echo, ping]", result.Tools[0].Name, result.Tools[1].Name)
				}
				if len(result.Tools[0].InputSchema) == 0 {
					t.Error("echo tool has no inputSchema")
				}
			},
		},
		{
			name:    "tools/call echo",
			request: `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo","arguments":{"text":"hello, gateway"}}}`,
			check: func(t *testing.T, resp *mcp.Message) {
				if got := callText(t, resp); got != "hello, gateway" {
					t.Errorf("echo text = %q, want %q", got, "hello, gateway")
				}
			},
		},
		{
			name:    "tools/call ping",
			request: `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"ping"}}`,
			check: func(t *testing.T, resp *mcp.Message) {
				if got := callText(t, resp); got != "pong" {
					t.Errorf("ping text = %q, want pong", got)
				}
			},
		},
		{
			name:    "tools/call unknown tool",
			request: `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"nope","arguments":{}}}`,
			check: func(t *testing.T, resp *mcp.Message) {
				if resp.Error == nil {
					t.Fatalf("want error response, got result %s", resp.Result)
				}
				if resp.Error.Code != mcp.CodeInvalidParams {
					t.Errorf("error code = %d, want %d", resp.Error.Code, mcp.CodeInvalidParams)
				}
			},
		},
		{
			// A hybrid message (both a method and a result) is not a valid
			// request — it must get an explicit invalid-request error, not be
			// silently ignored (mirrors the gateway dispatcher's check).
			name:    "hybrid method+result",
			request: `{"jsonrpc":"2.0","id":7,"method":"tools/list","result":{}}`,
			check: func(t *testing.T, resp *mcp.Message) {
				if resp.Error == nil {
					t.Fatalf("want error response, got result %s", resp.Result)
				}
				if resp.Error.Code != mcp.CodeInvalidRequest {
					t.Errorf("error code = %d, want %d", resp.Error.Code, mcp.CodeInvalidRequest)
				}
			},
		},
		{
			name:    "unknown method",
			request: `{"jsonrpc":"2.0","id":6,"method":"resources/list"}`,
			check: func(t *testing.T, resp *mcp.Message) {
				if resp.Error == nil {
					t.Fatalf("want error response, got result %s", resp.Result)
				}
				if resp.Error.Code != mcp.CodeMethodNotFound {
					t.Errorf("error code = %d, want %d", resp.Error.Code, mcp.CodeMethodNotFound)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responses := runDemo(t, []string{tt.request})
			if len(responses) != 1 {
				t.Fatalf("got %d responses, want 1", len(responses))
			}
			tt.check(t, responses[0])
		})
	}
}

// TestRunFullSession drives one connection through the whole demo dialogue,
// interleaving a notification (which must produce no reply), and verifies each
// reply echoes the request's id in order — the sequential loop must never skip
// or reorder.
func TestRunFullSession(t *testing.T) {
	requests := []string{
		`{"jsonrpc":"2.0","id":10,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":11,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"ping"}}`,
	}
	responses := runDemo(t, requests)
	if len(responses) != 3 {
		t.Fatalf("got %d responses, want 3 (the notification must produce none)", len(responses))
	}
	for i, wantID := range []string{"10", "11", "12"} {
		if got := string(responses[i].ID); got != wantID {
			t.Errorf("response %d id = %s, want %s", i, got, wantID)
		}
		if responses[i].Error != nil {
			t.Errorf("response %d unexpected error: %v", i, responses[i].Error)
		}
	}
}

// TestRunStopsOnContextCancel: with a stdin that never delivers data, Run must
// return promptly (and cleanly) once the context is cancelled.
func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// io.Pipe with no writes: Read blocks until pw is closed, so only the
	// context can make Run return.
	pr, pw := io.Pipe()
	defer pw.Close() // unblocks the orphaned reader goroutine after the test

	done := make(chan error, 1)
	go func() { done <- Run(ctx, pr, &bytes.Buffer{}, "v") }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// mustUnmarshal fails the test on any unmarshal error.
func mustUnmarshal(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
}

// callText extracts content[0].text from a tools/call result.
func callText(t *testing.T, resp *mcp.Message) string {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("unexpected error response: %v", resp.Error)
	}
	var result struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	mustUnmarshal(t, resp.Result, &result)
	if result.IsError {
		t.Fatal("result marked isError")
	}
	if len(result.Content) != 1 || result.Content[0].Type != "text" {
		t.Fatalf("unexpected content shape: %+v", result.Content)
	}
	return result.Content[0].Text
}
