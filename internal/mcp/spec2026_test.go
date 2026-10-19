package mcp

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"testing"
)

// TestParseRequestMeta_Modern: a request whose params._meta carries the modern
// keys is recognized as modern, with all four fields extracted.
// Mutation for red: return a zero RequestMeta.
func TestParseRequestMeta_Modern(t *testing.T) {
	params := json.RawMessage(`{
		"name": "x",
		"_meta": {
			"io.modelcontextprotocol/protocolVersion": "2026-07-28",
			"io.modelcontextprotocol/clientInfo": {"name":"acme","version":"1.2.3"},
			"io.modelcontextprotocol/clientCapabilities": {"elicitation":{}},
			"io.modelcontextprotocol/logLevel": "debug"
		}
	}`)
	m := ParseRequestMeta(params)
	if !m.Modern() {
		t.Fatalf("Modern() = false, want true")
	}
	if m.ProtocolVersion != "2026-07-28" {
		t.Errorf("ProtocolVersion = %q, want 2026-07-28", m.ProtocolVersion)
	}
	if m.ClientInfo.Name != "acme" || m.ClientInfo.Version != "1.2.3" {
		t.Errorf("ClientInfo = %+v, want {acme 1.2.3}", m.ClientInfo)
	}
	if m.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug", m.LogLevel)
	}
	var caps map[string]json.RawMessage
	if err := json.Unmarshal(m.ClientCapabilities, &caps); err != nil {
		t.Fatalf("ClientCapabilities not preserved: %v (%s)", err, m.ClientCapabilities)
	}
	if _, ok := caps["elicitation"]; !ok {
		t.Errorf("ClientCapabilities = %s, want elicitation key", m.ClientCapabilities)
	}
}

// TestParseRequestMeta_Legacy: params without _meta, without the version key,
// and nil params are all legacy (Modern()==false).
// Mutation for red: treat any _meta as modern.
func TestParseRequestMeta_Legacy(t *testing.T) {
	cases := []struct {
		name   string
		params json.RawMessage
	}{
		{"nil params", nil},
		{"empty object", json.RawMessage(`{}`)},
		{"no _meta", json.RawMessage(`{"name":"x","arguments":{}}`)},
		{"_meta without version key", json.RawMessage(`{"_meta":{"progressToken":"t"}}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := ParseRequestMeta(tc.params)
			if m.Modern() {
				t.Errorf("Modern() = true, want false for %s", tc.params)
			}
			if m.ProtocolVersion != "" {
				t.Errorf("ProtocolVersion = %q, want empty", m.ProtocolVersion)
			}
		})
	}
}

// TestParseRequestMeta_BadVersionType: the version key is present but not a
// string → the request still claims modern (Modern()==true) so it is answered
// with -32022, but ProtocolVersion stays "" (unrecognized value).
// Mutation for red: downgrade to legacy on a non-string version.
func TestParseRequestMeta_BadVersionType(t *testing.T) {
	params := json.RawMessage(`{"_meta":{"io.modelcontextprotocol/protocolVersion":42}}`)
	m := ParseRequestMeta(params)
	if !m.Modern() {
		t.Fatalf("Modern() = false, want true (present-but-non-string version claims modern)")
	}
	if m.ProtocolVersion != "" {
		t.Errorf("ProtocolVersion = %q, want empty for a non-string version", m.ProtocolVersion)
	}
}

// TestParseRequestMeta_Tolerant: garbage params (not an object, or _meta not an
// object) fall back to legacy without panicking or erroring.
// Mutation for red: panic/error on malformed input.
func TestParseRequestMeta_Tolerant(t *testing.T) {
	cases := []json.RawMessage{
		json.RawMessage(`[1,2,3]`),          // params is an array
		json.RawMessage(`"a string"`),       // params is a scalar
		json.RawMessage(`{"_meta":42}`),     // _meta is not an object
		json.RawMessage(`{"_meta":"nope"}`), // _meta is a string
		json.RawMessage(`not json at all`),  // invalid JSON
	}
	for _, p := range cases {
		m := ParseRequestMeta(p) // must not panic
		if m.Modern() {
			t.Errorf("Modern() = true, want false for garbage %s", p)
		}
	}
}

// TestDecorateModernResult: missing modern fields are added; already-present
// resultType/ttlMs are NOT overwritten; foreign _meta keys are preserved and
// serverInfo is merged in alongside them.
// Mutation for red: overwrite an existing resultType.
func TestDecorateModernResult(t *testing.T) {
	info := Implementation{Name: "aiMCPGate", Version: "9.9.9"}

	t.Run("adds missing fields when cacheable", func(t *testing.T) {
		out := DecorateModernResult(json.RawMessage(`{"tools":[]}`), info, true, GatewayTTLMs, GatewayCacheScope)
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(out, &obj); err != nil {
			t.Fatalf("output not an object: %v", err)
		}
		if string(obj["resultType"]) != `"complete"` {
			t.Errorf("resultType = %s, want \"complete\"", obj["resultType"])
		}
		if string(obj["ttlMs"]) != "60000" {
			t.Errorf("ttlMs = %s, want 60000", obj["ttlMs"])
		}
		if string(obj["cacheScope"]) != `"private"` {
			t.Errorf("cacheScope = %s, want \"private\"", obj["cacheScope"])
		}
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(obj["_meta"], &meta); err != nil {
			t.Fatalf("_meta not an object: %v", err)
		}
		var gotInfo Implementation
		if err := json.Unmarshal(meta[MetaServerInfo], &gotInfo); err != nil {
			t.Fatalf("serverInfo not present in _meta: %v", err)
		}
		if gotInfo != info {
			t.Errorf("serverInfo = %+v, want %+v", gotInfo, info)
		}
	})

	t.Run("non-cacheable omits ttl/cacheScope but adds resultType", func(t *testing.T) {
		out := DecorateModernResult(json.RawMessage(`{"content":[]}`), info, false, GatewayTTLMs, GatewayCacheScope)
		var obj map[string]json.RawMessage
		_ = json.Unmarshal(out, &obj)
		if string(obj["resultType"]) != `"complete"` {
			t.Errorf("resultType = %s, want \"complete\"", obj["resultType"])
		}
		if _, ok := obj["ttlMs"]; ok {
			t.Errorf("ttlMs present on a non-cacheable result: %s", out)
		}
		if _, ok := obj["cacheScope"]; ok {
			t.Errorf("cacheScope present on a non-cacheable result: %s", out)
		}
	})

	t.Run("does not overwrite existing fields", func(t *testing.T) {
		in := json.RawMessage(`{"resultType":"input_required","ttlMs":5,"cacheScope":"public"}`)
		out := DecorateModernResult(in, info, true, GatewayTTLMs, GatewayCacheScope)
		var obj map[string]json.RawMessage
		_ = json.Unmarshal(out, &obj)
		if string(obj["resultType"]) != `"input_required"` {
			t.Errorf("resultType overwritten: %s, want \"input_required\"", obj["resultType"])
		}
		if string(obj["ttlMs"]) != "5" {
			t.Errorf("ttlMs overwritten: %s, want 5", obj["ttlMs"])
		}
		if string(obj["cacheScope"]) != `"public"` {
			t.Errorf("cacheScope overwritten: %s, want \"public\"", obj["cacheScope"])
		}
	})

	t.Run("preserves foreign _meta keys", func(t *testing.T) {
		in := json.RawMessage(`{"_meta":{"vendor/x":123}}`)
		out := DecorateModernResult(in, info, false, GatewayTTLMs, GatewayCacheScope)
		var obj map[string]json.RawMessage
		_ = json.Unmarshal(out, &obj)
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(obj["_meta"], &meta); err != nil {
			t.Fatalf("_meta not an object: %v", err)
		}
		if string(meta["vendor/x"]) != "123" {
			t.Errorf("foreign _meta key lost: %s", obj["_meta"])
		}
		if _, ok := meta[MetaServerInfo]; !ok {
			t.Errorf("serverInfo not merged alongside foreign key: %s", obj["_meta"])
		}
	})
}

// TestDecorateModernResult_NonObject: null, an array, or a scalar are returned
// unchanged (a non-object result is not ours to mask), without panic.
// Mutation for red: panic on a non-object result.
func TestDecorateModernResult_NonObject(t *testing.T) {
	info := Implementation{Name: "aiMCPGate", Version: "1"}
	cases := []json.RawMessage{
		json.RawMessage(`null`),
		json.RawMessage(`[1,2,3]`),
		json.RawMessage(`"scalar"`),
		json.RawMessage(`42`),
	}
	for _, in := range cases {
		out := DecorateModernResult(in, info, true, GatewayTTLMs, GatewayCacheScope) // must not panic
		if string(out) != string(in) {
			t.Errorf("DecorateModernResult(%s) = %s, want unchanged", in, out)
		}
	}
}

// TestExtractMRTRRetry: both fields are extracted when present; a params with
// neither yields a zero struct (requestState kept empty, not lost).
// Mutation for red: lose requestState.
func TestExtractMRTRRetry(t *testing.T) {
	t.Run("both fields", func(t *testing.T) {
		params := json.RawMessage(`{"name":"x","inputResponses":{"q1":{"action":"accept"}},"requestState":"tok123"}`)
		r := ExtractMRTRRetry(params)
		if r.RequestState != "tok123" {
			t.Errorf("RequestState = %q, want tok123", r.RequestState)
		}
		if got := string(r.InputResponses["q1"]); got != `{"action":"accept"}` {
			t.Errorf("InputResponses[q1] = %s, want {\"action\":\"accept\"}", got)
		}
	})
	t.Run("absent fields yield zero", func(t *testing.T) {
		r := ExtractMRTRRetry(json.RawMessage(`{"name":"x","arguments":{}}`))
		if r.RequestState != "" || r.InputResponses != nil {
			t.Errorf("expected zero MRTRRetry, got %+v", r)
		}
	})
	t.Run("nil params", func(t *testing.T) {
		r := ExtractMRTRRetry(nil) // must not panic
		if r.RequestState != "" || r.InputResponses != nil {
			t.Errorf("expected zero MRTRRetry from nil, got %+v", r)
		}
	})
}

// TestDecodeHeaderSentinel: plain values pass through; a lowercase-marker
// sentinel decodes; broken base64 fails; an UPPERCASE "=?BASE64?...?=" marker is
// NOT a sentinel and passes through verbatim.
// Mutation for red: case-insensitive marker comparison.
func TestDecodeHeaderSentinel(t *testing.T) {
	b64 := base64.StdEncoding.EncodeToString([]byte("café/файл"))
	cases := []struct {
		name   string
		in     string
		want   string
		wantOK bool
	}{
		{"plain ascii", "search_repositories", "search_repositories", true},
		{"sentinel decodes utf8", "=?base64?" + b64 + "?=", "café/файл", true},
		{"broken base64", "=?base64?!!!not-b64!!!?=", "", false},
		{"uppercase marker not a sentinel", "=?BASE64?" + b64 + "?=", "=?BASE64?" + b64 + "?=", true},
		{"empty string", "", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := DecodeHeaderSentinel(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (in=%q got=%q)", ok, tc.wantOK, tc.in, got)
			}
			if ok && got != tc.want {
				t.Errorf("decoded = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestGatewayBusyCode: the renumbered guard code is -32009 and sits OUTSIDE the
// MCP-reserved -32020..-32099 band (and inside the implementation-defined
// -32000..-32019 band).
// Mutation for red: revert CodeGatewayBusy to -32029.
func TestGatewayBusyCode(t *testing.T) {
	if CodeGatewayBusy != -32009 {
		t.Fatalf("CodeGatewayBusy = %d, want -32009", CodeGatewayBusy)
	}
	if CodeGatewayBusy <= -32020 && CodeGatewayBusy >= -32099 {
		t.Errorf("CodeGatewayBusy = %d is inside the MCP-reserved -32020..-32099 band", CodeGatewayBusy)
	}
	if CodeGatewayBusy > -32000 || CodeGatewayBusy < -32019 {
		t.Errorf("CodeGatewayBusy = %d is outside the implementation-defined -32000..-32019 band", CodeGatewayBusy)
	}
}

// TestSupportedVersionsOrder: newest first, and the modern-error data type
// serializes with the documented shape.
func TestSupportedVersionsOrder(t *testing.T) {
	if !reflect.DeepEqual(SupportedVersions, []string{"2026-07-28", "2025-06-18"}) {
		t.Fatalf("SupportedVersions = %v, want [2026-07-28 2025-06-18] (newest first)", SupportedVersions)
	}
	data := UnsupportedVersionData{Supported: SupportedVersions, Requested: "2025-06-18"}
	b, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round map[string]json.RawMessage
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := round["supported"]; !ok {
		t.Errorf("missing supported field: %s", b)
	}
	if string(round["requested"]) != `"2025-06-18"` {
		t.Errorf("requested = %s, want \"2025-06-18\"", round["requested"])
	}
}
