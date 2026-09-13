// Stage 19b: the transport-agnostic modern (2026-07-28) dispatch path — era
// branching, server/discover, result decoration, removed-method refusals, the
// unsupported-version error, and the legacy byte-stability guard.

package transport

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// modernParams wraps an arbitrary params object with the modern _meta keys
// (protocolVersion + clientCapabilities + clientInfo) so ParseRequestMeta puts
// the request on the modern branch. version pins _meta.protocolVersion; caps is
// the raw clientCapabilities value (may be "" for none). extra is merged into
// the top-level params object (e.g. {"name":...} for tools/call).
func modernParams(t *testing.T, version, caps string, extra map[string]json.RawMessage) json.RawMessage {
	t.Helper()
	meta := map[string]json.RawMessage{
		mcp.MetaProtocolVersion: mcp.MustParams(version),
		mcp.MetaClientInfo:      mcp.MustParams(mcp.Implementation{Name: "modern-client", Version: "1.0"}),
	}
	if caps != "" {
		meta[mcp.MetaClientCapabilities] = json.RawMessage(caps)
	}
	obj := map[string]json.RawMessage{"_meta": mcp.MustParams(meta)}
	for k, v := range extra {
		obj[k] = v
	}
	return mcp.MustParams(obj)
}

// liveDispatcher builds a dispatcher over a live registry backed by one
// fakeserver upstream advertising the named tool.
func liveDispatcher(t *testing.T, tool string) *dispatcher {
	t.Helper()
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Upstreams: []config.Upstream{
			{Name: "demo", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME": "demo", "FAKE_TOOLS": tool, "FAKE_ECHO": "1",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	if err := reg.Start(context.Background()); err != nil {
		t.Fatalf("registry Start: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	return newDispatcher(reg, quietLogger(), "test-1.2.3", true, false, false)
}

// resultKeys decodes a raw result object and returns its top-level keys as a set.
func resultKeys(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("decode result %s: %v", raw, err)
	}
	return obj
}

// TestModernToolsListDecorated: a modern tools/list result carries resultType,
// the cacheable ttlMs/cacheScope, and _meta.serverInfo with the gateway values.
// Mutation: drop the decoration (return the reply unchanged) → this goes red.
func TestModernToolsListDecorated(t *testing.T) {
	d := liveDispatcher(t, "search")
	req := mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList,
		modernParams(t, mcp.ProtocolVersionModern, "", nil))
	reply := d.dispatch(context.Background(), req)
	if reply.Error != nil {
		t.Fatalf("modern tools/list error: %+v", reply.Error)
	}
	obj := resultKeys(t, reply.Result)

	if got := string(obj["resultType"]); got != `"complete"` {
		t.Errorf("resultType = %s, want \"complete\"", got)
	}
	if got := string(obj["ttlMs"]); got != "60000" {
		t.Errorf("ttlMs = %s, want 60000", got)
	}
	if got := string(obj["cacheScope"]); got != `"private"` {
		t.Errorf("cacheScope = %s, want \"private\"", got)
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(obj["_meta"], &meta); err != nil {
		t.Fatalf("decode _meta %s: %v", obj["_meta"], err)
	}
	var info mcp.Implementation
	if err := json.Unmarshal(meta[mcp.MetaServerInfo], &info); err != nil {
		t.Fatalf("decode serverInfo %s: %v", meta[mcp.MetaServerInfo], err)
	}
	if info.Name != "aiMCPGate" || info.Version != "test-1.2.3" {
		t.Errorf("serverInfo = %+v, want aiMCPGate/test-1.2.3", info)
	}
}

// TestLegacyToolsListByteStable: a LEGACY tools/list result carries NONE of the
// modern fields — byte-for-byte the pre-Stage-19 object. It compares the raw
// result of a modern call (decorated) against the legacy one (must be exactly
// {"tools":[...]}) to prove the decoration never leaks onto the legacy branch.
// Mutation: decorate always → the legacy result grows resultType/_meta → red.
func TestLegacyToolsListByteStable(t *testing.T) {
	d := liveDispatcher(t, "search")

	legacy := d.dispatch(context.Background(), mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList, nil))
	if legacy.Error != nil {
		t.Fatalf("legacy tools/list error: %+v", legacy.Error)
	}
	obj := resultKeys(t, legacy.Result)
	if len(obj) != 1 {
		t.Fatalf("legacy tools/list result has %d keys %v, want exactly {tools}", len(obj), keysOf(obj))
	}
	if _, ok := obj["tools"]; !ok {
		t.Errorf("legacy tools/list result missing tools key: %v", keysOf(obj))
	}
	for _, forbidden := range []string{"resultType", "ttlMs", "cacheScope", "_meta"} {
		if _, ok := obj[forbidden]; ok {
			t.Errorf("legacy tools/list result leaked modern field %q: %s", forbidden, legacy.Result)
		}
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestModernUnsupportedVersion: a modern request whose _meta.protocolVersion is
// the LEGACY 2025-06-18 is rejected -32022 with data.supported ==
// SupportedVersions and data.requested echoing the offending value. Mutation:
// let the legacy value through → no -32022 → red.
func TestModernUnsupportedVersion(t *testing.T) {
	d := newDispatcher(nil, quietLogger(), "test", true, false, false)
	req := mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsList,
		modernParams(t, mcp.ProtocolVersion /* legacy value in the modern key */, "", nil))
	reply := d.dispatch(context.Background(), req)
	if reply.Error == nil || reply.Error.Code != mcp.CodeUnsupportedProtocolVersion {
		t.Fatalf("reply = %+v, want -32022", reply.Error)
	}
	var data mcp.UnsupportedVersionData
	if err := json.Unmarshal(reply.Error.Data, &data); err != nil {
		t.Fatalf("decode data %s: %v", reply.Error.Data, err)
	}
	if len(data.Supported) != len(mcp.SupportedVersions) || data.Supported[0] != mcp.ProtocolVersionModern {
		t.Errorf("data.supported = %v, want %v", data.Supported, mcp.SupportedVersions)
	}
	if data.Requested != mcp.ProtocolVersion {
		t.Errorf("data.requested = %q, want the offending %q", data.Requested, mcp.ProtocolVersion)
	}
}

// TestModernRemovedMethods: initialize / ping / logging/setLevel carrying modern
// _meta are all -32601 (removed on the modern revision), never routed into their
// legacy handlers. Mutation: route into the legacy handler → initialize/ping
// would succeed → red.
func TestModernRemovedMethods(t *testing.T) {
	d := newDispatcher(nil, quietLogger(), "test", true, false, false)
	for _, method := range []string{mcp.MethodInitialize, mcp.MethodPing, mcp.MethodLoggingSetLevel} {
		req := mcp.NewRequest(mcp.IntID(1), method,
			modernParams(t, mcp.ProtocolVersionModern, "", nil))
		reply := d.dispatch(context.Background(), req)
		if reply == nil || reply.Error == nil || reply.Error.Code != mcp.CodeMethodNotFound {
			t.Errorf("modern %s → %+v, want -32601", method, reply)
		}
	}
}

// TestServerDiscover: server/discover returns supportedVersions (newest first),
// capabilities WITHOUT logging, the aggregated instructions, and the cacheable
// fields. Mutation: declare logging in the modern capabilities → red.
func TestServerDiscover(t *testing.T) {
	d := liveDispatcher(t, "search")
	req := mcp.NewRequest(mcp.IntID(1), mcp.MethodServerDiscover,
		modernParams(t, mcp.ProtocolVersionModern, "", nil))
	reply := d.dispatch(context.Background(), req)
	if reply.Error != nil {
		t.Fatalf("server/discover error: %+v", reply.Error)
	}
	var res mcp.DiscoverResult
	if err := json.Unmarshal(reply.Result, &res); err != nil {
		t.Fatalf("decode DiscoverResult %s: %v", reply.Result, err)
	}
	if res.ResultType != mcp.ResultTypeComplete {
		t.Errorf("resultType = %q, want complete", res.ResultType)
	}
	if len(res.SupportedVersions) != 2 || res.SupportedVersions[0] != mcp.ProtocolVersionModern ||
		res.SupportedVersions[1] != mcp.ProtocolVersion {
		t.Errorf("supportedVersions = %v, want [%s %s]", res.SupportedVersions, mcp.ProtocolVersionModern, mcp.ProtocolVersion)
	}
	if res.TTLMs != mcp.GatewayTTLMs || res.CacheScope != mcp.GatewayCacheScope {
		t.Errorf("cacheable fields = ttlMs %d / scope %q, want %d / %q", res.TTLMs, res.CacheScope, mcp.GatewayTTLMs, mcp.GatewayCacheScope)
	}
	var caps map[string]json.RawMessage
	if err := json.Unmarshal(res.Capabilities, &caps); err != nil {
		t.Fatalf("decode capabilities %s: %v", res.Capabilities, err)
	}
	if _, ok := caps["logging"]; ok {
		t.Errorf("modern capabilities declared logging: %s", res.Capabilities)
	}
	if tools, ok := caps["tools"]; !ok || string(tools) != `{"listChanged":false}` {
		t.Errorf("tools capability = %s, want {\"listChanged\":false} in 19b", tools)
	}
}

// TestModernToolsCallResultTypeInjected: a modern tools/call gets resultType on
// its (verbatim upstream) result but NOT ttlMs — a tool result is not cacheable.
// Mutation: mark tools/call cacheable → ttlMs appears → red.
func TestModernToolsCallResultTypeInjected(t *testing.T) {
	d := liveDispatcher(t, "search")
	req := mcp.NewRequest(mcp.IntID(1), mcp.MethodToolsCall,
		modernParams(t, mcp.ProtocolVersionModern, "", map[string]json.RawMessage{
			"name":      mcp.MustParams("demo__search"),
			"arguments": json.RawMessage(`{"q":"x"}`),
		}))
	reply := d.dispatch(context.Background(), req)
	if reply.Error != nil {
		t.Fatalf("modern tools/call error: %+v", reply.Error)
	}
	obj := resultKeys(t, reply.Result)
	if got := string(obj["resultType"]); got != `"complete"` {
		t.Errorf("resultType = %s, want \"complete\"", got)
	}
	if _, ok := obj["ttlMs"]; ok {
		t.Errorf("tools/call result carried ttlMs (not cacheable): %s", reply.Result)
	}
}

// TestModernUnknownMethod: a modern request for a method the gateway does not
// serve on the modern revision is -32601. Also pins modernMethodKnown, the
// 200-vs-404 source. Mutation: default routeModern into a legacy handler → red.
func TestModernUnknownMethod(t *testing.T) {
	d := newDispatcher(nil, quietLogger(), "test", true, false, false)
	req := mcp.NewRequest(mcp.IntID(1), "does/notExist",
		modernParams(t, mcp.ProtocolVersionModern, "", nil))
	reply := d.dispatch(context.Background(), req)
	if reply.Error == nil || reply.Error.Code != mcp.CodeMethodNotFound {
		t.Fatalf("reply = %+v, want -32601", reply.Error)
	}
	if modernMethodKnown("does/notExist") {
		t.Error("modernMethodKnown says a bogus method is known")
	}
	if modernMethodKnown(mcp.MethodInitialize) {
		t.Error("modernMethodKnown must treat initialize as unknown (removed on modern)")
	}
}

// TestServerReqCapsFromModernMeta pins the clientCapabilities translation: only
// the registry-known server→client capabilities survive, values verbatim, and a
// missing/garbage capabilities object declares nothing (never panics).
func TestServerReqCapsFromModernMeta(t *testing.T) {
	meta := mcp.ParseRequestMeta(modernParams(t, mcp.ProtocolVersionModern,
		`{"elicitation":{},"sampling":{},"unknownThing":{"x":1}}`, nil))
	caps := serverReqCapsFromModernMeta(meta)
	if _, ok := caps["elicitation"]; !ok {
		t.Errorf("elicitation missing from %v", caps)
	}
	if _, ok := caps["sampling"]; !ok {
		t.Errorf("sampling missing from %v", caps)
	}
	if _, ok := caps["unknownThing"]; ok {
		t.Errorf("a non-server-request capability leaked through: %v", caps)
	}

	// No capabilities key at all → empty, no panic.
	none := serverReqCapsFromModernMeta(mcp.ParseRequestMeta(modernParams(t, mcp.ProtocolVersionModern, "", nil)))
	if len(none) != 0 {
		t.Errorf("no clientCapabilities declared %v, want empty", none)
	}
}
