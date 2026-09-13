// Stage 19b: the HTTP modern (2026-07-28) path — header validation against the
// body, stateless (session-less) service, unknown-method 404, and the error →
// HTTP-status mapping.

package transport

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// modernBody builds a modern request body carrying the given _meta version and
// merging extra top-level params fields (name/uri/arguments).
func modernBody(t *testing.T, id int64, method, version string, extra map[string]json.RawMessage) *mcp.Message {
	t.Helper()
	return mcp.NewRequest(mcp.IntID(id), method, modernParams(t, version, "", extra))
}

// modernHeaders builds the conformant modern header set for a request: the
// protocol version, the method mirror, and (when the method requires it) the
// Mcp-Name mirror. name is the raw (un-encoded) name/uri value; "" omits it.
func modernHeaders(version, method, name string) map[string]string {
	h := map[string]string{
		protocolVersionHeader: version,
		methodHeader:          method,
	}
	if name != "" {
		h[nameHeader] = name
	}
	return h
}

// postModern POSTs a modern message with the given headers and returns the
// response for inspection.
func postModern(t *testing.T, srv *httptest.Server, msg *mcp.Message, headers map[string]string) *http.Response {
	t.Helper()
	body, err := mcp.Encode(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	return resp
}

// TestModernPostHeaderValidation (table): each modern header gate. Mutation:
// cut any one check → its row goes red.
func TestModernPostHeaderValidation(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	const good = mcp.ProtocolVersionModern
	// A base tools/call body whose params.name is "github__search".
	callExtra := map[string]json.RawMessage{
		"name":      mcp.MustParams("github__search"),
		"arguments": json.RawMessage(`{}`),
	}

	cases := []struct {
		name       string
		msg        *mcp.Message
		headers    map[string]string
		wantStatus int
		wantCode   int // 0 = expect success (no error)
	}{
		{
			"no Mcp-Method → 400/-32020",
			modernBody(t, nextTestID(), mcp.MethodToolsList, good, nil),
			map[string]string{protocolVersionHeader: good},
			http.StatusBadRequest, mcp.CodeHeaderMismatch,
		},
		{
			"Mcp-Method mismatch → 400/-32020",
			modernBody(t, nextTestID(), mcp.MethodToolsList, good, nil),
			map[string]string{protocolVersionHeader: good, methodHeader: "tools/call"},
			http.StatusBadRequest, mcp.CodeHeaderMismatch,
		},
		{
			"MCP-Protocol-Version mismatch → 400/-32020",
			modernBody(t, nextTestID(), mcp.MethodToolsList, good, nil),
			map[string]string{protocolVersionHeader: "1999-01-01", methodHeader: mcp.MethodToolsList},
			http.StatusBadRequest, mcp.CodeHeaderMismatch,
		},
		{
			"no MCP-Protocol-Version → 400/-32020",
			modernBody(t, nextTestID(), mcp.MethodToolsList, good, nil),
			map[string]string{methodHeader: mcp.MethodToolsList},
			http.StatusBadRequest, mcp.CodeHeaderMismatch,
		},
		{
			"tools/call Mcp-Name mismatch → 400/-32020",
			modernBody(t, nextTestID(), mcp.MethodToolsCall, good, callExtra),
			modernHeaders(good, mcp.MethodToolsCall, "wrong__name"),
			http.StatusBadRequest, mcp.CodeHeaderMismatch,
		},
		{
			"tools/call Mcp-Name plain match → 200",
			modernBody(t, nextTestID(), mcp.MethodToolsCall, good, callExtra),
			modernHeaders(good, mcp.MethodToolsCall, "github__search"),
			http.StatusOK, 0,
		},
		{
			"tools/call Mcp-Name sentinel-encoded match → 200",
			modernBody(t, nextTestID(), mcp.MethodToolsCall, good, callExtra),
			// "github__search" base64 = Z2l0aHViX19zZWFyY2g=
			modernHeaders(good, mcp.MethodToolsCall, "=?base64?Z2l0aHViX19zZWFyY2g=?="),
			http.StatusOK, 0,
		},
		{
			"tools/call Mcp-Name broken sentinel → 400/-32020",
			modernBody(t, nextTestID(), mcp.MethodToolsCall, good, callExtra),
			modernHeaders(good, mcp.MethodToolsCall, "=?base64?not!!base64?="),
			http.StatusBadRequest, mcp.CodeHeaderMismatch,
		},
		{
			"tools/list with a stray Mcp-Name is fine → 200",
			modernBody(t, nextTestID(), mcp.MethodToolsList, good, nil),
			map[string]string{protocolVersionHeader: good, methodHeader: mcp.MethodToolsList, nameHeader: "ignored"},
			http.StatusOK, 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postModern(t, srv, tc.msg, tc.headers)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			msg := decodeBody(t, resp)
			if tc.wantCode == 0 {
				if msg.Error != nil {
					t.Fatalf("unexpected error %+v", msg.Error)
				}
				return
			}
			if msg.Error == nil || msg.Error.Code != tc.wantCode {
				t.Fatalf("error = %+v, want code %d", msg.Error, tc.wantCode)
			}
		})
	}
}

// TestModernPostNoSession: a modern request works with no Mcp-Session-Id (200),
// and the response never carries a session header. A modern client is not
// supposed to send a session id at all — a bogus one would be rejected by the
// legacy pre-body gate before the era is even known (see handlePost's comment
// and plan §16), so this pins the spec-relevant fact: statelessness, no minted
// session. Mutation: route modern through the session path → a header is echoed
// or the no-session request 400s → red.
func TestModernPostNoSession(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	msg := modernBody(t, nextTestID(), mcp.MethodToolsList, mcp.ProtocolVersionModern, nil)
	resp := postModern(t, srv, msg, modernHeaders(mcp.ProtocolVersionModern, mcp.MethodToolsList, ""))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("modern tools/list without a session: status = %d, want 200", resp.StatusCode)
	}
	if sid := resp.Header.Get(sessionHeader); sid != "" {
		t.Errorf("modern response minted/echoed a session id %q, want none", sid)
	}
	_ = decodeBody(t, resp)
}

// TestModernUnknownMethod404: an unknown modern method is HTTP 404 + -32601, so
// a client can tell a modern server apart from a legacy HTTP+SSE 404. Mutation:
// answer 200 → red.
func TestModernUnknownMethod404(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	msg := modernBody(t, nextTestID(), "totally/unknown", mcp.ProtocolVersionModern, nil)
	resp := postModern(t, srv, msg, map[string]string{
		protocolVersionHeader: mcp.ProtocolVersionModern,
		methodHeader:          "totally/unknown",
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown modern method: status = %d, want 404", resp.StatusCode)
	}
	msg = decodeBody(t, resp)
	if msg.Error == nil || msg.Error.Code != mcp.CodeMethodNotFound {
		t.Fatalf("error = %+v, want -32601", msg.Error)
	}
}

// TestModernErrorStatuses: -32022 (unsupported version) → 400. Pairs with the
// header cases above (-32020 → 400) and the 404 test. Mutation: always 200 → red.
func TestModernErrorStatuses(t *testing.T) {
	srv, cleanup := startHTTPGateway(t)
	defer cleanup()

	// A modern request whose _meta version is the legacy one → -32022 → 400.
	// Headers must still match _meta.protocolVersion so the version gate (not
	// the header gate) is the one that fires.
	msg := modernBody(t, nextTestID(), mcp.MethodToolsList, mcp.ProtocolVersion, nil)
	resp := postModern(t, srv, msg, map[string]string{
		protocolVersionHeader: mcp.ProtocolVersion,
		methodHeader:          mcp.MethodToolsList,
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unsupported version: status = %d, want 400", resp.StatusCode)
	}
	msg = decodeBody(t, resp)
	if msg.Error == nil || msg.Error.Code != mcp.CodeUnsupportedProtocolVersion {
		t.Fatalf("error = %+v, want -32022", msg.Error)
	}
}

// TestModernServerDiscoverLazyStart: server/discover is the FIRST request the
// gateway ever sees; it must lazily bring the registry up (ToolCount>0 after)
// through the same sync.Once gate every other request uses (invariant §12 p.7),
// not around it. Mutation: skip the lazy start on the modern path → discover
// sees an empty registry → red.
func TestModernServerDiscoverLazyStart(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Transport: config.TransportHTTP,
		Upstreams: []config.Upstream{
			{Name: "demo", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_NAME": "demo", "FAKE_TOOLS": "search",
			}},
		},
	}
	reg := registry.New(cfg, quietLogger(), nil, noopPayloadLog(), true, "0.0.0-test")
	hs := newHTTPServer(cfg, reg, quietLogger(), "test-1.2.3")
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", hs.handleMCP)
	srv := httptest.NewServer(mux)
	defer func() { srv.Close(); _ = reg.Close() }()

	if reg.ToolCount() != 0 {
		t.Fatalf("registry started before the first request (ToolCount %d)", reg.ToolCount())
	}

	msg := modernBody(t, nextTestID(), mcp.MethodServerDiscover, mcp.ProtocolVersionModern, nil)
	resp := postModern(t, srv, msg, map[string]string{
		protocolVersionHeader: mcp.ProtocolVersionModern,
		methodHeader:          mcp.MethodServerDiscover,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("discover status = %d, want 200", resp.StatusCode)
	}
	out := decodeBody(t, resp)
	if out.Error != nil {
		t.Fatalf("discover error: %+v", out.Error)
	}
	if reg.ToolCount() == 0 {
		t.Error("server/discover did not lazily start the registry (ToolCount == 0)")
	}
}
