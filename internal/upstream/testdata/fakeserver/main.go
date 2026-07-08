// Command fakeserver is a minimal, deterministic MCP server used by the upstream
// and registry integration tests. It speaks MCP 2025-06-18 over stdio: reads
// newline-delimited JSON-RPC from stdin, writes responses to stdout.
//
// Behaviour is controlled by environment variables so one binary can play
// several roles:
//
//	FAKE_NAME    server name reported in serverInfo/initialize (default "fake")
//	FAKE_TOOLS   comma-separated tool names to advertise in tools/list
//	FAKE_ECHO    if "1", tools/call echoes back the received arguments as text
//
// It intentionally has zero third-party deps so `go run` / `go build` of it is
// hermetic.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func main() {
	name := envOr("FAKE_NAME", "fake")
	var tools []string
	if raw := os.Getenv("FAKE_TOOLS"); raw != "" {
		tools = strings.Split(raw, ",")
	}
	echo := os.Getenv("FAKE_ECHO") == "1"

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)

	write := func(m message) {
		m.JSONRPC = "2.0"
		b, _ := json.Marshal(m)
		out.Write(b)
		out.WriteByte('\n')
		out.Flush()
	}

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var req message
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue
		}
		switch req.Method {
		case "initialize":
			result := fmt.Sprintf(
				`{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":%q,"version":"1.0.0"}}`,
				name)
			write(message{ID: req.ID, Result: json.RawMessage(result)})
		case "notifications/initialized":
			// no response
		case "tools/list":
			write(message{ID: req.ID, Result: json.RawMessage(toolsListResult(tools))})
		case "resources/list":
			write(message{ID: req.ID, Result: json.RawMessage(`{"resources":[]}`)})
		case "tools/call":
			write(message{ID: req.ID, Result: json.RawMessage(callResult(req.Params, echo))})
		default:
			if len(req.ID) > 0 {
				write(message{ID: req.ID, Error: &rpcError{Code: -32601, Message: "method not found: " + req.Method}})
			}
		}
	}
}

func toolsListResult(tools []string) string {
	var b strings.Builder
	b.WriteString(`{"tools":[`)
	for i, name := range tools {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":%q,"description":"fake tool %s","inputSchema":{"type":"object"}}`, name, name)
	}
	b.WriteString(`]}`)
	return b.String()
}

func callResult(params json.RawMessage, echo bool) string {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	_ = json.Unmarshal(params, &p)
	text := "called " + p.Name
	if echo && len(p.Arguments) > 0 {
		text = string(p.Arguments)
	}
	b, _ := json.Marshal(text)
	return fmt.Sprintf(`{"content":[{"type":"text","text":%s}],"isError":false}`, b)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
