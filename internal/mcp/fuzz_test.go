package mcp

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// FuzzReader feeds arbitrary bytes through the framing Reader and checks the
// two properties every caller of Read relies on (codec.go):
//
//  1. Read never panics, whatever the stream contains;
//  2. the read loop always terminates: each Read call either returns a message,
//     a retryable per-line decode error (both consume one line), io.EOF, or a
//     fatal error wrapping ErrFatalRead — after which the caller must stop
//     (Reader's documented contract; retrying a fatal error is a busy-loop).
//
// The seed corpus is run as ordinary test cases on every `go test` (standard
// Go fuzzing behavior since 1.18); `-fuzz` is only needed to search for new
// inputs.
func FuzzReader(f *testing.F) {
	// Valid frames of each JSON-RPC kind.
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"cursor":"c"}}` + "\n"))
	f.Add([]byte(`{"jsonrpc":"2.0","id":"abc","result":{"tools":[]}}` + "\n"))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope"}}` + "\n"))
	f.Add([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n"))
	// Blank input and blank lines between frames (skipped by Read).
	f.Add([]byte(""))
	f.Add([]byte("\n\n  \n" + `{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n\n"))
	// Non-JSON garbage: a retryable per-line decode error.
	f.Add([]byte("not json at all\n{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"ping\"}\n"))
	// A JSON array is a batch — removed in spec 2025-06-18, must be an invalid
	// frame (decodeLine rejects a leading '[').
	f.Add([]byte(`[{"jsonrpc":"2.0","id":1,"method":"ping"}]` + "\n"))
	// Nested JSON with ESCAPED newlines inside a string value: legal in one
	// frame (the framing forbids only raw '\n' bytes, which end the line).
	f.Add([]byte(`{"jsonrpc":"2.0","id":4,"method":"echo","params":{"text":"line1\nline2","nested":{"a":[1,2,{"b":"c\nd"}]}}}` + "\n"))
	// Truncated JSON (stream cut mid-object) and trailing data after the object.
	f.Add([]byte(`{"jsonrpc":"2.0","id":5,"met`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":6,"method":"ping"} trailing` + "\n"))
	// A single line exceeding maxLineBytes: bufio.Scanner returns ErrTooLong,
	// which Read must surface wrapped in ErrFatalRead (permanent failure).
	f.Add([]byte(`{"pad":"` + strings.Repeat("x", maxLineBytes+1) + `"}` + "\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(strings.NewReader(string(data)))

		// Termination bound: every successful Read (or retryable decode error)
		// consumes at least one non-blank line, and the input has at most
		// len(data)+1 lines — so more iterations than that means Read stopped
		// making progress (a hang, caught without timers).
		maxReads := len(data) + 2
		for i := 0; ; i++ {
			if i > maxReads {
				t.Fatalf("Reader did not terminate after %d reads on %d input bytes", i, len(data))
			}
			msg, err := r.Read()
			if err == nil {
				if msg == nil {
					t.Fatal("Read returned nil message with nil error")
				}
				continue
			}
			if errors.Is(err, io.EOF) {
				return // clean end of stream
			}
			if errors.Is(err, ErrFatalRead) {
				return // permanent failure: the contract says stop reading here
			}
			// Retryable per-line decode error: keep reading subsequent lines,
			// exactly as the upstream reader loop does.
		}
	})
}

// FuzzMessage checks that the Message classifiers (message.go) keep their
// invariants on any JSON object that unmarshals successfully:
//
//   - IsRequest and IsResponse are mutually exclusive (IsRequest is defined
//     as "has id + method AND not a response");
//   - IsRequest and IsNotification are mutually exclusive (a request requires
//     a non-null id, a notification requires its absence);
//   - a request is never a malformed hybrid (a hybrid is response-shaped);
//   - IsMalformedHybrid implies both a method and IsResponse — the shape all
//     three dispatchers (transport, demo, upstream) must agree to reject;
//   - the classification survives an Encode → Unmarshal round-trip, so the
//     proxy re-emitting a message never changes its kind.
func FuzzMessage(f *testing.F) {
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"cursor":"c"}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":"abc","method":"initialize"}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope","data":{"x":1}}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":null,"method":"notifications/progress"}`))
	// Malformed hybrid: method + result in one message (must classify as such).
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list","result":{}}`))
	// Hybrid without an id: used to slip past dispatchers as a "notification".
	f.Add([]byte(`{"jsonrpc":"2.0","method":"tools/list","error":{"code":1,"message":"m"}}`))
	// Degenerate shapes: nothing, only an id, null result (still a response —
	// json.RawMessage keeps the literal "null", so Result != nil).
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":42}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":7,"result":null}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			return // not a Message-shaped JSON object — nothing to classify
		}

		checkInvariants := func(m *Message, label string) {
			if m.IsRequest() && m.IsResponse() {
				t.Errorf("%s: IsRequest and IsResponse both true: %s", label, data)
			}
			if m.IsRequest() && m.IsNotification() {
				t.Errorf("%s: IsRequest and IsNotification both true: %s", label, data)
			}
			if m.IsRequest() && m.IsMalformedHybrid() {
				t.Errorf("%s: IsRequest and IsMalformedHybrid both true: %s", label, data)
			}
			if m.IsMalformedHybrid() && (m.Method == "" || !m.IsResponse()) {
				t.Errorf("%s: IsMalformedHybrid without method+response shape: %s", label, data)
			}
		}
		checkInvariants(&msg, "decoded")

		// Round-trip: the gateway re-encodes messages it proxies; that must not
		// change how the other side classifies them. Encode cannot fail here —
		// every RawMessage field came out of a successful Unmarshal.
		frame, err := Encode(&msg)
		if err != nil {
			t.Fatalf("Encode after successful Unmarshal: %v", err)
		}
		var msg2 Message
		if err := json.Unmarshal(frame, &msg2); err != nil {
			t.Fatalf("re-Unmarshal of Encode output: %v (frame: %s)", err, frame)
		}
		checkInvariants(&msg2, "round-tripped")
		if msg.IsRequest() != msg2.IsRequest() ||
			msg.IsResponse() != msg2.IsResponse() ||
			msg.IsNotification() != msg2.IsNotification() ||
			msg.IsMalformedHybrid() != msg2.IsMalformedHybrid() {
			t.Errorf("classification changed across Encode round-trip: %s -> %s", data, frame)
		}
	})
}
