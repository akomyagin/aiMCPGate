package registry

// Round 3 unit tests: logging/setLevel fan-out (capability-gated, parallel,
// partial-failure-tolerant) and notifications/message forwarding with the
// upstream name stamped into the `logger` field.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// TestSetLogLevelFansOutToLoggingCapableOnly: SetLogLevel reaches every
// upstream that declared the logging capability and NOBODY else — the
// non-capable fake records zero calls (delivery there would be the bug; a
// recording fake asserts absence as strongly as a panicking one, and reuses
// the shared fakeUpstream).
func TestSetLogLevelFansOutToLoggingCapableOnly(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "loggy", Enabled: true},
		{Name: "chatty", Enabled: true},
		{Name: "bare", Enabled: true},
	}}
	fakes := map[string]*fakeUpstream{
		"loggy":  {name: "loggy", tools: []string{"a"}, caps: json.RawMessage(`{"tools":{},"logging":{}}`)},
		"chatty": {name: "chatty", tools: []string{"b"}, caps: json.RawMessage(`{"logging":{}}`)},
		"bare":   {name: "bare", tools: []string{"c"}, caps: json.RawMessage(`{"tools":{}}`)},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if err := r.SetLogLevel(context.Background(), "warning"); err != nil {
		t.Fatalf("SetLogLevel: %v", err)
	}

	for _, name := range []string{"loggy", "chatty"} {
		if got := fakes[name].setLevels(); len(got) != 1 || got[0] != "warning" {
			t.Errorf("%s received levels %v, want exactly [warning]", name, got)
		}
	}
	if got := fakes["bare"].setLevels(); len(got) != 0 {
		t.Errorf("bare (no logging capability) received setLevel %v, want none", got)
	}
}

// TestSetLogLevelPartialFailureIsNotFatal: one capable upstream failing its
// setLevel must not fail the whole fan-out — the other upstream still gets the
// level and the client-facing result is success (nil).
func TestSetLogLevelPartialFailureIsNotFatal(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "ok", Enabled: true},
		{Name: "broken", Enabled: true},
	}}
	fakes := map[string]*fakeUpstream{
		"ok":     {name: "ok", tools: []string{"a"}, caps: json.RawMessage(`{"logging":{}}`)},
		"broken": {name: "broken", tools: []string{"b"}, caps: json.RawMessage(`{"logging":{}}`), setLevelErr: errors.New("nope")},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	if err := r.SetLogLevel(context.Background(), "debug"); err != nil {
		t.Fatalf("SetLogLevel with one failing upstream = %v, want nil (partial failure is Warn-only)", err)
	}
	if got := fakes["ok"].setLevels(); len(got) != 1 || got[0] != "debug" {
		t.Errorf("ok received levels %v, want [debug] despite the sibling's failure", got)
	}
}

// TestNotifMessageForwardedWithLogger: a notifications/message from an
// upstream reaches the notification subscribers over the SAME channel progress
// uses, with the emitting upstream's name stamped into the `logger` field —
// the one deliberate deviation from verbatim forwarding.
func TestNotifMessageForwardedWithLogger(t *testing.T) {
	r := newNotifTestRegistry()
	ch, unsubscribe := r.SubscribeNotifications()
	defer unsubscribe()

	r.onUpstreamNotification("web", mcp.NotifMessage,
		json.RawMessage(`{"level":"info","data":"cache warmed"}`))

	select {
	case msg := <-ch:
		if msg.Method != mcp.NotifMessage {
			t.Errorf("forwarded method = %q, want %q", msg.Method, mcp.NotifMessage)
		}
		var p struct {
			Level  string `json:"level"`
			Logger string `json:"logger"`
			Data   string `json:"data"`
		}
		if err := json.Unmarshal(msg.Params, &p); err != nil {
			t.Fatalf("decode forwarded params %s: %v", msg.Params, err)
		}
		if p.Logger != "web" {
			t.Errorf("logger = %q, want the upstream name %q", p.Logger, "web")
		}
		if p.Level != "info" || p.Data != "cache warmed" {
			t.Errorf("level/data = %q/%q, want the original values untouched", p.Level, p.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the forwarded log message")
	}
}

// TestWithLogger: the enrichment helper's corner cases — a logger the upstream
// set itself is preserved under the upstream's namespace; malformed, non-object
// or null params pass through verbatim (enriching must never break a message).
func TestWithLogger(t *testing.T) {
	tests := []struct {
		name     string
		params   string
		upstream string
		want     string // "" means: expect the input back verbatim
	}{
		{"no logger", `{"level":"info","data":"x"}`, "web", `"logger":"web"`},
		{"upstream logger namespaced", `{"level":"info","logger":"db"}`, "web", `"logger":"web__db"`},
		{"empty logger treated as absent", `{"logger":""}`, "web", `"logger":"web"`},
		{"non-string logger replaced", `{"logger":7}`, "web", `"logger":"web"`},
		{"non-object params verbatim", `[1,2]`, "web", ""},
		{"null params verbatim", `null`, "web", ""},
		{"malformed params verbatim", `{"level":`, "web", ""},
		{"absent params verbatim", ``, "web", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := json.RawMessage(tt.params)
			if tt.params == "" {
				in = nil
			}
			out := withLogger(in, tt.upstream)
			if tt.want == "" {
				if string(out) != string(in) {
					t.Fatalf("withLogger(%s) = %s, want the input verbatim", tt.params, out)
				}
				return
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(out, &got); err != nil {
				t.Fatalf("withLogger produced invalid JSON %s: %v", out, err)
			}
			var whole map[string]json.RawMessage
			_ = json.Unmarshal(in, &whole)
			for k := range whole {
				if k == "logger" {
					continue
				}
				if string(got[k]) != string(whole[k]) {
					t.Errorf("field %q = %s, want %s untouched", k, got[k], whole[k])
				}
			}
			if !strings.Contains(string(out), tt.want) {
				t.Errorf("withLogger(%s) = %s, want it to contain %s", tt.params, out, tt.want)
			}
		})
	}
}
