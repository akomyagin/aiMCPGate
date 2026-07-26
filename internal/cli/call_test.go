package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// writeDemoEchoConfig writes a demo.config.yaml-style gateway config whose
// ONLY upstream is the gate binary itself in its hidden __demo-echo role —
// the same self-hosted stub the Stage 12 sandbox image uses, so call/catalog
// integration tests exercise a real child process without any external server.
func writeDemoEchoConfig(t *testing.T, bin string) string {
	t.Helper()
	return writeDoctorConfig(t, fmt.Sprintf(`
transport: stdio
log_level: error
upstreams:
  - name: demo-echo
    kind: stdio
    command: %s
    args: ["__demo-echo"]
    enabled: true
`, bin))
}

// execRoot runs one in-process `mcp-gate <args...>` invocation on a fresh
// command tree and returns stdout and the Execute error.
func execRoot(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := Build("test")
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

// echoCallResult is the {"content":[{"type","text"}]} shape the demo echo
// server returns from tools/call — enough structure to assert on the payload.
type echoCallResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

// TestCallEchoToolPrintsResult is the Round 10 happy path: `mcp-gate call`
// brings the demo-echo upstream up, routes the namespaced tool with the given
// JSON arguments and pretty-prints the tool's result on stdout, exit 0.
func TestCallEchoToolPrintsResult(t *testing.T) {
	bin := buildGateBinary(t)
	cfgPath := writeDemoEchoConfig(t, bin)

	out, err := execRoot(t, "call", "demo-echo__echo", `{"text":"hello from call"}`, "-c", cfgPath)
	if err != nil {
		t.Fatalf("call failed: %v\noutput:\n%s", err, out)
	}

	var result echoCallResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\noutput:\n%s", err, out)
	}
	if len(result.Content) != 1 || result.Content[0].Text != "hello from call" {
		t.Errorf("result = %+v, want one text content echoing the argument", result)
	}
	// Pretty-printed, not the wire-compact form: MarshalIndent yields newlines
	// and indentation.
	if !strings.Contains(out, "\n  ") {
		t.Errorf("output is not indented:\n%s", out)
	}
}

// TestCallWithoutArgsDefaultsToEmptyObject: [json-args] omitted → the tool is
// called with {} (demo-echo's ping needs no arguments and answers "pong").
func TestCallWithoutArgsDefaultsToEmptyObject(t *testing.T) {
	bin := buildGateBinary(t)
	cfgPath := writeDemoEchoConfig(t, bin)

	out, err := execRoot(t, "call", "demo-echo__ping", "-c", cfgPath)
	if err != nil {
		t.Fatalf("call failed: %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(out, "pong") {
		t.Errorf("output = %q, want the ping tool's \"pong\" answer", out)
	}
}

// TestCallRejectsInvalidJSONArgs: malformed [json-args] must fail fast with a
// clear local error — before any upstream is even started.
func TestCallRejectsInvalidJSONArgs(t *testing.T) {
	// No gateway binary needed: validation short-circuits before config/upstreams.
	out, err := execRoot(t, "call", "demo-echo__echo", `{not json`, "-c", "unused.yaml")
	if err == nil {
		t.Fatalf("call with malformed JSON args succeeded:\n%s", out)
	}
	if !strings.Contains(err.Error(), "not valid JSON") {
		t.Errorf("error = %q, want the not-valid-JSON explanation", err)
	}
}

// TestCallUnknownToolExitsNonZero: a tool absent from the aggregated catalog
// is a routing failure — error out (non-zero exit), nothing on stdout.
func TestCallUnknownToolExitsNonZero(t *testing.T) {
	bin := buildGateBinary(t)
	cfgPath := writeDemoEchoConfig(t, bin)

	out, err := execRoot(t, "call", "demo-echo__no-such-tool", "-c", cfgPath)
	if err == nil {
		t.Fatalf("call of an unknown tool succeeded:\n%s", out)
	}
	if !strings.Contains(err.Error(), "unknown tool") {
		t.Errorf("error = %q, want the unknown-tool explanation", err)
	}
	if strings.TrimSpace(out) != "" {
		t.Errorf("stdout = %q, want empty on failure", out)
	}
}

// TestCallSurfacesJSONRPCError: when the tool itself answers with a JSON-RPC
// error (echo with a non-string "text" → invalid params), call must report the
// error's code and message and exit non-zero — a JSON-RPC-level failure is
// still a failure for scripts.
func TestCallSurfacesJSONRPCError(t *testing.T) {
	bin := buildGateBinary(t)
	cfgPath := writeDemoEchoConfig(t, bin)

	out, err := execRoot(t, "call", "demo-echo__echo", `{"text":123}`, "-c", cfgPath)
	if err == nil {
		t.Fatalf("call returning a JSON-RPC error succeeded:\n%s", out)
	}
	if !strings.Contains(err.Error(), "JSON-RPC error") {
		t.Errorf("error = %q, want the JSON-RPC error surfaced", err)
	}
}
