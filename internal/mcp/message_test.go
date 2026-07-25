package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestNewErrorNilIDMarshalsNullID pins the JSON-RPC 2.0 rule that an error
// response whose original request id could not be determined (e.g. a parse
// error on a malformed body) MUST carry the literal "id":null — the field may
// not be omitted. NewError normalizes an empty/nil id to null so every call
// site gets this for free; requests and notifications keep the opposite
// guarantee (no id field at all when they have none).
func TestNewErrorNilIDMarshalsNullID(t *testing.T) {
	t.Run("nil id becomes id:null", func(t *testing.T) {
		for name, id := range map[string]json.RawMessage{
			"nil":        nil,
			"empty":      json.RawMessage(""),
			"whitespace": json.RawMessage("  "),
		} {
			msg := NewError(id, CodeParseError, "parse error", nil)
			out, err := json.Marshal(msg)
			if err != nil {
				t.Fatalf("[%s] marshal: %v", name, err)
			}
			if !strings.Contains(string(out), `"id":null`) {
				t.Errorf("[%s] error response must contain %q, got: %s", name, `"id":null`, out)
			}
		}
	})

	t.Run("explicit id echoed verbatim", func(t *testing.T) {
		msg := NewError(json.RawMessage("42"), CodeInternalError, "boom", nil)
		out, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if !strings.Contains(string(out), `"id":42`) {
			t.Errorf("error response must echo the given id, got: %s", out)
		}
	})

	// Control cases: the id:null normalization is exclusive to error
	// responses. A notification must NOT grow an id field, and a request
	// built without an id must not either (a degenerate input, but the
	// constructor must not invent an id where the caller gave none).
	t.Run("notification has no id field", func(t *testing.T) {
		out, err := json.Marshal(NewNotification("notifications/initialized", nil))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(out), `"id"`) {
			t.Errorf("notification must not contain an id field, got: %s", out)
		}
	})

	t.Run("request without id has no id field", func(t *testing.T) {
		out, err := json.Marshal(NewRequest(nil, "ping", nil))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(out), `"id"`) {
			t.Errorf("request built with nil id must not contain an id field, got: %s", out)
		}
	})
}
