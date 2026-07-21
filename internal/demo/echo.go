// Package demo implements the hidden `mcp-gate __demo-echo` subcommand: a
// trivial, self-contained MCP server (stdio, JSON-RPC 2.0) used as a stub
// upstream for sandbox introspection (Glama.ai, Stage 12).
//
// Registries like Glama register a server by launching it in a sandbox and
// driving initialize / tools/list — with no real upstreams available. The
// gateway's Registry.Start deliberately fail-fasts when zero upstreams come up,
// so instead of weakening that invariant the gateway binary can act as its own
// upstream: demo.config.yaml launches `mcp-gate __demo-echo` as a perfectly
// ordinary stdio upstream through the existing launch path. Nothing in
// internal/registry or internal/config knows this server exists.
//
// The server is intentionally inert: two in-memory tools (echo, ping), no
// os/exec, no HTTP, no filesystem access.
package demo

import (
	"context"
	"encoding/json"
	"io"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// serverName is reported in initialize's serverInfo.
const serverName = "aimcpgate-demo-echo"

// Static catalog and schemas of the two demo tools. Raw JSON literals: the
// schemas are fixed contracts, there is nothing to compute.
var (
	echoInputSchema = json.RawMessage(`{"type":"object","properties":{"text":{"type":"string","description":"Text to echo back verbatim."}},"required":["text"]}`)
	pingInputSchema = json.RawMessage(`{"type":"object","properties":{}}`)
)

// textContent / callResult model the minimal tools/call result shape
// ({"content":[{"type":"text",...}],"isError":false}) this server ever emits.
type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type callResult struct {
	Content []textContent `json:"content"`
	IsError bool          `json:"isError"`
}

func textResult(text string) json.RawMessage {
	return mcp.MustParams(callResult{Content: []textContent{{Type: "text", Text: text}}})
}

// Run serves MCP over the given stdin/stdout until stdin reaches EOF (the
// parent gateway closed the pipe — normal shutdown) or ctx is cancelled
// (Ctrl-C / SIGTERM). version is stamped into serverInfo. It takes plain
// io.Reader/io.Writer so tests can drive it with buffers, no real process.
func Run(ctx context.Context, stdin io.Reader, stdout io.Writer, version string) error {
	r := mcp.NewReader(stdin)
	w := mcp.NewWriter(stdout)

	// The read loop runs in its own goroutine because mcp.Reader blocks in
	// Scan with no way to interrupt it; selecting on ctx.Done lets Run return
	// promptly on cancellation. The orphaned goroutine then exits with the
	// process (CLI use) or on the buffer's EOF (tests) — it holds no resources
	// beyond the stdin it was reading anyway.
	errc := make(chan error, 1)
	go func() { errc <- serve(r, w, version) }()

	select {
	case <-ctx.Done():
		// Cancellation is the requested shutdown, not a failure.
		return nil
	case err := <-errc:
		return err
	}
}

// serve is the sequential request loop: read one framed message, answer it.
// Only EOF ends the loop; a single corrupt line yields a parse error but does
// not desynchronize the stream (mcp.Reader's contract), so the loop keeps
// serving subsequent lines — same policy as the gateway's own transport layer
// (internal/transport/stdio.go). Being a minimal stub with no logger, it
// skips the bad line silently.
func serve(r *mcp.Reader, w *mcp.Writer, version string) error {
	for {
		msg, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			continue // one bad line — keep reading the next
		}
		reply := handle(msg, version)
		if reply == nil {
			continue // notification (or a stray response) — nothing to send
		}
		if err := w.Write(reply); err != nil {
			return err
		}
	}
}

// handle produces the reply for one message, or nil when none is required.
func handle(msg *mcp.Message, version string) *mcp.Message {
	// A malformed hybrid — both a method (request shape) and a result/error
	// (response shape) — must be answered with an explicit invalid-request
	// error, not silently dropped (same check as the gateway's own dispatcher,
	// internal/transport/dispatch.go).
	if msg.Method != "" && msg.IsResponse() {
		return mcp.NewError(msg.ID, mcp.CodeInvalidRequest,
			"message is not a valid request: carries both a method and a result/error", nil)
	}
	if !msg.IsRequest() {
		// notifications/initialized and the like; also ignores anything that is
		// not a well-formed request (a demo stub has no one to complain to).
		return nil
	}

	switch msg.Method {
	case mcp.MethodInitialize:
		result := mcp.InitializeResult{
			ProtocolVersion: mcp.ProtocolVersion,
			Capabilities:    json.RawMessage(`{"tools":{}}`),
			ServerInfo: mcp.Implementation{
				Name:    serverName,
				Version: version,
			},
		}
		return mcp.NewResult(msg.ID, mcp.MustParams(result))

	case mcp.MethodToolsList:
		result := mcp.ToolsListResult{Tools: []mcp.Tool{
			{
				Name:        "echo",
				Description: "Echo the given text back verbatim.",
				InputSchema: echoInputSchema,
			},
			{
				Name:        "ping",
				Description: "Health check: always returns \"pong\".",
				InputSchema: pingInputSchema,
			},
		}}
		return mcp.NewResult(msg.ID, mcp.MustParams(result))

	case mcp.MethodToolsCall:
		return handleToolsCall(msg)

	default:
		return mcp.NewError(msg.ID, mcp.CodeMethodNotFound, "method not found: "+msg.Method, nil)
	}
}

// handleToolsCall answers the two demo tools; anything else is invalid params
// (the method itself exists, the tool name does not).
func handleToolsCall(msg *mcp.Message) *mcp.Message {
	var params mcp.ToolsCallParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return mcp.NewError(msg.ID, mcp.CodeInvalidParams, "invalid tools/call params: "+err.Error(), nil)
	}

	switch params.Name {
	case "echo":
		var args struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(params.Arguments, &args); err != nil {
			return mcp.NewError(msg.ID, mcp.CodeInvalidParams, "invalid echo arguments: "+err.Error(), nil)
		}
		return mcp.NewResult(msg.ID, textResult(args.Text))
	case "ping":
		return mcp.NewResult(msg.ID, textResult("pong"))
	default:
		return mcp.NewError(msg.ID, mcp.CodeInvalidParams, "unknown tool: "+params.Name, nil)
	}
}
