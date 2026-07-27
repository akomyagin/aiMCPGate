package transport

// Round 3 end-to-end tests: logging/setLevel fan-out and notifications/message
// forwarding over the real stdio stack — fakeserver child processes, registry,
// dispatcher, framing. FAKE_LOGGING makes the fakeserver declare the logging
// capability; FAKE_SETLEVEL_FILE records every setLevel it receives (capability
// or not); FAKE_LOG_FILE fires one notifications/message per touch.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// startLoggingServer brings up the gateway over two fakeserver upstreams:
// "loggy" with the logging capability, "bare" without — BOTH record any
// logging/setLevel they receive, so the capability gate is asserted from the
// upstream's point of view. It returns the two recording file paths.
func startLoggingServer(t *testing.T, loggyExtraEnv map[string]string) (c *fakeClient, cancel context.CancelFunc, done <-chan error, loggyFile, bareFile string) {
	t.Helper()
	bin := buildFakeServer(t)
	dir := t.TempDir()
	loggyFile = filepath.Join(dir, "loggy-setlevel")
	bareFile = filepath.Join(dir, "bare-setlevel")
	loggyEnv := map[string]string{
		"FAKE_NAME":          "loggy",
		"FAKE_TOOLS":         "watch",
		"FAKE_LOGGING":       "1",
		"FAKE_SETLEVEL_FILE": loggyFile,
	}
	for k, v := range loggyExtraEnv {
		loggyEnv[k] = v
	}
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "loggy", Command: bin, Enabled: boolPtr(true), Env: loggyEnv},
		{Name: "bare", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
			"FAKE_NAME":          "bare",
			"FAKE_TOOLS":         "fetch",
			"FAKE_SETLEVEL_FILE": bareFile,
		}},
	}}
	c, cancel, done = startServerWithConfig(t, cfg, nil)
	return c, cancel, done, loggyFile, bareFile
}

// TestStdioLoggingCapabilityConditional: with a logging-capable upstream in
// the mix the gateway advertises logging; with none, no logging capability
// appears at all.
func TestStdioLoggingCapabilityConditional(t *testing.T) {
	c, cancel, done, _, _ := startLoggingServer(t, nil)
	defer func() { cancel(); <-done }()
	res := c.initialize()
	if !strings.Contains(string(res.Capabilities), `"logging":{}`) {
		t.Errorf("capabilities = %s, want logging declared", res.Capabilities)
	}
}

func TestStdioNoLoggingCapabilityWithoutLoggingUpstream(t *testing.T) {
	c, cancel, done := startServer(t, false) // github upstream, no FAKE_LOGGING
	defer func() { cancel(); <-done }()
	res := c.initialize()
	if strings.Contains(string(res.Capabilities), `"logging"`) {
		t.Errorf("capabilities = %s, want NO logging capability (no upstream declares it)", res.Capabilities)
	}
}

// TestStdioLoggingSetLevelFanOut: a client's logging/setLevel is answered with
// an empty result and reaches ONLY the logging-capable upstream — the bare
// one's recording file stays empty. Invalid params (no level) get a clean
// invalid-params error without touching any upstream.
func TestStdioLoggingSetLevelFanOut(t *testing.T) {
	c, cancel, done, loggyFile, bareFile := startLoggingServer(t, nil)
	defer func() { cancel(); <-done }()
	c.initialize()

	id := c.request(mcp.MethodLoggingSetLevel, mcp.MustParams(mcp.LoggingSetLevelParams{Level: "warning"}))
	resp := c.readResponse()
	if string(resp.ID) != string(id) {
		t.Fatalf("setLevel response id = %s, want client id %s", resp.ID, id)
	}
	if resp.Error != nil {
		t.Fatalf("logging/setLevel error: %v", resp.Error)
	}
	if string(resp.Result) != "{}" {
		t.Errorf("setLevel result = %s, want {}", resp.Result)
	}

	// The reply may race the recording write on the fakeserver side; poll.
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(loggyFile)
		if err == nil && strings.TrimSpace(string(data)) == "warning" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loggy never recorded the level: file=%q err=%v", data, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// bare declared no logging capability: the gateway must not have called it.
	if data, err := os.ReadFile(bareFile); err == nil && len(strings.TrimSpace(string(data))) > 0 {
		t.Errorf("bare (no logging capability) received setLevel: %q", data)
	}

	// Missing level: invalid params, nothing fanned out.
	c.request(mcp.MethodLoggingSetLevel, json.RawMessage(`{}`))
	resp = c.readResponse()
	if resp.Error == nil || resp.Error.Code != mcp.CodeInvalidParams {
		t.Errorf("setLevel without a level = %+v, want code %d", resp.Error, mcp.CodeInvalidParams)
	}
}

// TestStdioForwardsLogMessages: a notifications/message pushed by an upstream
// reaches the initialized client over the same channel progress travels, with
// the upstream's name stamped into the `logger` field and level/data untouched.
func TestStdioForwardsLogMessages(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "log-trigger")
	c, cancel, done, _, _ := startLoggingServer(t, map[string]string{"FAKE_LOG_FILE": logFile})
	defer func() { cancel(); <-done }()
	c.initialize()

	if err := os.WriteFile(logFile, []byte("go"), 0o600); err != nil {
		t.Fatalf("touch log trigger: %v", err)
	}

	msgCh := make(chan *mcp.Message, 1)
	go func() {
		m := c.readResponse()
		msgCh <- m
	}()
	select {
	case m := <-msgCh:
		if !m.IsNotification() || m.Method != mcp.NotifMessage {
			t.Fatalf("got method=%q id=%s, want a notifications/message notification", m.Method, m.ID)
		}
		var p struct {
			Level  string `json:"level"`
			Logger string `json:"logger"`
			Data   string `json:"data"`
		}
		if err := json.Unmarshal(m.Params, &p); err != nil {
			t.Fatalf("decode params %s: %v", m.Params, err)
		}
		if p.Logger != "loggy" {
			t.Errorf("logger = %q, want the upstream name %q stamped by the gateway", p.Logger, "loggy")
		}
		if p.Level != "info" || p.Data != "fake log line" {
			t.Errorf("level/data = %q/%q, want the fakeserver's values untouched", p.Level, p.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("forwarded log message never arrived")
	}
}
