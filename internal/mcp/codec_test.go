package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

// decode is a test-only helper mirroring what Reader.Read does with one framed
// line: trim, reject blank input, then parse via decodeLine. It exists so the
// table tests below can feed single lines without spinning up a Reader.
func decode(line []byte) (*Message, error) {
	t := bytes.TrimSpace(line)
	if len(t) == 0 {
		return nil, errors.New("empty message")
	}
	return decodeLine(t)
}

func TestDecodeClassifies(t *testing.T) {
	tests := []struct {
		name           string
		line           string
		wantRequest    bool
		wantResponse   bool
		wantNotif      bool
		wantMethod     string
		wantErrPresent bool
	}{
		{
			name:        "request with int id",
			line:        `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"cursor":"c"}}`,
			wantRequest: true,
			wantMethod:  "tools/list",
		},
		{
			name:        "request with string id",
			line:        `{"jsonrpc":"2.0","id":"abc","method":"initialize"}`,
			wantRequest: true,
			wantMethod:  "initialize",
		},
		{
			name:         "success response",
			line:         `{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}`,
			wantResponse: true,
		},
		{
			name:           "error response",
			line:           `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope"}}`,
			wantResponse:   true,
			wantErrPresent: true,
		},
		{
			name:       "notification (no id)",
			line:       `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
			wantNotif:  true,
			wantMethod: "notifications/initialized",
		},
		{
			name:       "notification with explicit null id",
			line:       `{"jsonrpc":"2.0","id":null,"method":"notifications/tools/list_changed"}`,
			wantNotif:  true,
			wantMethod: "notifications/tools/list_changed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := decode([]byte(tc.line))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got := m.IsRequest(); got != tc.wantRequest {
				t.Errorf("IsRequest=%v want %v", got, tc.wantRequest)
			}
			if got := m.IsResponse(); got != tc.wantResponse {
				t.Errorf("IsResponse=%v want %v", got, tc.wantResponse)
			}
			if got := m.IsNotification(); got != tc.wantNotif {
				t.Errorf("IsNotification=%v want %v", got, tc.wantNotif)
			}
			if tc.wantMethod != "" && m.Method != tc.wantMethod {
				t.Errorf("Method=%q want %q", m.Method, tc.wantMethod)
			}
			if (m.Error != nil) != tc.wantErrPresent {
				t.Errorf("Error present=%v want %v", m.Error != nil, tc.wantErrPresent)
			}
		})
	}
}

func TestDecodeErrors(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{"empty", ""},
		{"whitespace only", "   \t "},
		{"invalid json", `{"jsonrpc":"2.0",`},
		{"batch array rejected", `[{"jsonrpc":"2.0","id":1,"method":"a"}]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decode([]byte(tc.line)); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestEncodeSingleLine(t *testing.T) {
	// A result whose text contains embedded newlines must still encode to a
	// single physical line (json escapes them), preserving stdio framing.
	result := json.RawMessage(`{"content":[{"type":"text","text":"line1\nline2"}]}`)
	m := NewResult(IntID(7), result)
	b, err := Encode(m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if bytes.Count(b, []byte("\n")) != 1 || b[len(b)-1] != '\n' {
		t.Fatalf("encoded frame is not exactly one line: %q", b)
	}
}

func TestReaderRoundTrip(t *testing.T) {
	// Encode several messages back-to-back, then read them all back.
	msgs := []*Message{
		NewRequest(IntID(1), "initialize", json.RawMessage(`{"protocolVersion":"2025-06-18"}`)),
		NewNotification("notifications/initialized", nil),
		NewResult(json.RawMessage(`"str-id"`), json.RawMessage(`{"ok":true}`)),
		NewError(IntID(2), CodeMethodNotFound, "no such method", nil),
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, m := range msgs {
		if err := w.Write(m); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	r := NewReader(&buf)
	for i, want := range msgs {
		got, err := r.Read()
		if err != nil {
			t.Fatalf("Read #%d: %v", i, err)
		}
		if got.Method != want.Method {
			t.Errorf("#%d Method=%q want %q", i, got.Method, want.Method)
		}
		if string(got.ID) != string(want.ID) {
			t.Errorf("#%d ID=%q want %q", i, got.ID, want.ID)
		}
		if want.Error != nil && (got.Error == nil || got.Error.Code != want.Error.Code) {
			t.Errorf("#%d error mismatch: %+v", i, got.Error)
		}
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF at end, got %v", err)
	}
}

func TestReaderSkipsBlankLines(t *testing.T) {
	in := "\n\n" + `{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\n\n"
	r := NewReader(strings.NewReader(in))
	m, err := r.Read()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if m.Method != "ping" {
		t.Fatalf("Method=%q want ping", m.Method)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// failingReader always fails with its configured error, simulating a broken
// underlying stream (the same terminal scanner state a >maxLineBytes frame or
// an I/O error produces, without generating gigabytes of input).
type failingReader struct{ err error }

func (r *failingReader) Read(p []byte) (int, error) { return 0, r.err }

// TestReaderScannerErrorIsFatal: an error from the underlying stream (scanner
// error) must be wrapped with ErrFatalRead so callers stop retrying — the
// scanner is permanently broken and every subsequent Read fails identically.
func TestReaderScannerErrorIsFatal(t *testing.T) {
	underlying := errors.New("boom: stream broken")
	r := NewReader(&failingReader{err: underlying})

	for i := 0; i < 2; i++ { // the error must be permanent across Reads
		_, err := r.Read()
		if err == nil {
			t.Fatalf("Read #%d: expected error, got nil", i)
		}
		if !errors.Is(err, ErrFatalRead) {
			t.Errorf("Read #%d: errors.Is(err, ErrFatalRead) = false, err = %v", i, err)
		}
		if !errors.Is(err, underlying) {
			t.Errorf("Read #%d: underlying error not wrapped, err = %v", i, err)
		}
	}
}

// TestReaderDecodeErrorIsNotFatal: bad JSON on one well-framed line is a
// retryable per-line error — it must NOT carry ErrFatalRead, and the stream
// stays readable afterwards.
func TestReaderDecodeErrorIsNotFatal(t *testing.T) {
	in := "{not json}\n" + `{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\n"
	r := NewReader(strings.NewReader(in))

	_, err := r.Read()
	if err == nil {
		t.Fatal("expected decode error for bad JSON line, got nil")
	}
	if errors.Is(err, ErrFatalRead) {
		t.Errorf("per-line decode error must not carry ErrFatalRead, err = %v", err)
	}

	m, err := r.Read()
	if err != nil {
		t.Fatalf("stream desynchronized after one bad line: %v", err)
	}
	if m.Method != "ping" {
		t.Fatalf("Method=%q want ping", m.Method)
	}
}

func TestWriterConcurrentFramesIntact(t *testing.T) {
	// Concurrent writes must not interleave bytes: every line read back must be
	// valid JSON. This exercises the writer mutex under -race.
	var buf syncBuffer
	w := NewWriter(&buf)
	const n = 50
	done := make(chan struct{})
	for i := 0; i < n; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			_ = w.Write(NewRequest(IntID(int64(i)), "m", json.RawMessage(`{"k":"vvvvvvvvvv"}`)))
		}(i)
	}
	for i := 0; i < n; i++ {
		<-done
	}
	r := NewReader(&buf)
	count := 0
	for {
		_, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("frame corrupted by concurrent write: %v", err)
		}
		count++
	}
	if count != n {
		t.Fatalf("read %d frames want %d", count, n)
	}
}

// syncBuffer is a minimal concurrency-safe buffer for the writer race test.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
func (s *syncBuffer) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Read(p)
}
