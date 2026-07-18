// Command fakeserver is a minimal, deterministic MCP server used by the upstream
// and registry integration tests. It speaks MCP 2025-06-18 over stdio: reads
// newline-delimited JSON-RPC from stdin, writes responses to stdout.
//
// Behaviour is controlled by environment variables so one binary can play
// several roles:
//
//	FAKE_NAME        server name reported in serverInfo/initialize (default "fake")
//	FAKE_TOOLS       comma-separated tool names to advertise in tools/list
//	FAKE_ECHO        if "1", tools/call echoes back the received arguments as text
//	FAKE_CALL_DELAY  Go duration (e.g. "2s") to sleep before answering tools/call;
//	                 used to exercise the gateway's call-timeout path. Handshake
//	                 and tools/list are never delayed.
//	FAKE_STDERR_LINES  number of lines to write to stderr right as stdin closes
//	                   (simulating shutdown diagnostics), before the process
//	                   exits — used to test that Close() waits for stderr to
//	                   fully drain before reaping the process.
//	FAKE_EXIT_AFTER  if > 0, the process exits (os.Exit(1)) after answering this
//	                   many tools/call requests — simulates a stdio upstream that
//	                   crashes mid-run, exercising the registry's auto-restart
//	                   supervisor (Stage 7a).
//	FAKE_TOOLS_FILE  path to a file whose first line, if present, OVERRIDES
//	                   FAKE_TOOLS for tools/list; re-read on every tools/list so a
//	                   test can change the advertised catalog at runtime. When the
//	                   file is set but empty/absent, FAKE_TOOLS is used. Paired
//	                   with FAKE_NOTIFY_FILE this lets a test drive a live catalog
//	                   change (Stage 7b).
//	FAKE_NOTIFY_FILE  path to a file the server polls; when it appears (is
//	                   non-empty) the server emits one notifications/tools/
//	                   list_changed to stdout, then truncates the file so it fires
//	                   once per touch — used to test the gateway's reaction to an
//	                   upstream list_changed (Stage 7b).
//	FAKE_NOTIFY_ON_START  if "1", the server writes one notifications/tools/
//	                   list_changed to stdout immediately on startup, before
//	                   reading any request — reproduces an upstream that
//	                   notifies the instant it is launched, the window of the
//	                   onNotify data race found by independent review.
//
// It intentionally has zero third-party deps so `go run` / `go build` of it is
// hermetic.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	var callDelay time.Duration
	if raw := os.Getenv("FAKE_CALL_DELAY"); raw != "" {
		callDelay, _ = time.ParseDuration(raw)
	}
	exitAfter, _ := strconv.Atoi(os.Getenv("FAKE_EXIT_AFTER"))
	toolsFile := os.Getenv("FAKE_TOOLS_FILE")
	notifyFile := os.Getenv("FAKE_NOTIFY_FILE")

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)

	// writes to stdout are serialized: the main request loop and the (optional)
	// notify poller goroutine below both write framed messages, and interleaving
	// their bytes would corrupt the stream — the same "serialize writes" rule the
	// gateway itself follows for upstream stdin.
	var writeMu sync.Mutex
	write := func(m message) {
		m.JSONRPC = "2.0"
		b, _ := json.Marshal(m)
		writeMu.Lock()
		out.Write(b)
		out.WriteByte('\n')
		out.Flush()
		writeMu.Unlock()
	}

	// notify-on-start: fire one list_changed the moment the process is up,
	// before any request is even read — the earliest possible server→client
	// traffic, racing the gateway's callback installation if it were late.
	if os.Getenv("FAKE_NOTIFY_ON_START") == "1" {
		write(message{Method: "notifications/tools/list_changed"})
	}

	// notify poller: when notifyFile becomes non-empty, emit one
	// notifications/tools/list_changed and truncate it so each "touch" fires
	// exactly once (Stage 7b test hook).
	if notifyFile != "" {
		go func() {
			for {
				time.Sleep(20 * time.Millisecond)
				data, err := os.ReadFile(notifyFile)
				if err != nil || len(strings.TrimSpace(string(data))) == 0 {
					continue
				}
				_ = os.WriteFile(notifyFile, nil, 0o600)
				write(message{Method: "notifications/tools/list_changed"})
			}
		}()
	}

	callCount := 0
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
			write(message{ID: req.ID, Result: json.RawMessage(toolsListResult(currentTools(toolsFile, tools)))})
		case "resources/list":
			write(message{ID: req.ID, Result: json.RawMessage(`{"resources":[]}`)})
		case "tools/call":
			if callDelay > 0 {
				time.Sleep(callDelay)
			}
			write(message{ID: req.ID, Result: json.RawMessage(callResult(req.Params, echo))})
			callCount++
			if exitAfter > 0 && callCount >= exitAfter {
				// Simulate a crash right after answering: flush is inside write,
				// so the reply is already on the wire. os.Exit skips deferred
				// flushes, but there is nothing left to flush.
				os.Exit(1)
			}
		default:
			if len(req.ID) > 0 {
				write(message{ID: req.ID, Error: &rpcError{Code: -32601, Message: "method not found: " + req.Method}})
			}
		}
	}

	// stdin closed (EOF) — the client is shutting us down. Write shutdown
	// diagnostics to stderr right before exiting, matching the real-world
	// window Close() must not race against.
	if n, _ := strconv.Atoi(os.Getenv("FAKE_STDERR_LINES")); n > 0 {
		for i := 0; i < n; i++ {
			fmt.Fprintf(os.Stderr, "shutdown line %d\n", i)
		}
	}
}

// currentTools returns the tool set to advertise: the first line of toolsFile
// (comma-separated) when that file is set and non-empty, else the static tools
// from FAKE_TOOLS. Re-read per tools/list so a test can change the catalog at
// runtime (Stage 7b).
func currentTools(toolsFile string, static []string) []string {
	if toolsFile == "" {
		return static
	}
	data, err := os.ReadFile(toolsFile)
	if err != nil {
		return static
	}
	line := strings.TrimSpace(string(data))
	if line == "" {
		return static
	}
	return strings.Split(line, ",")
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
