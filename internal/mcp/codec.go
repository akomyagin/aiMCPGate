package mcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// maxLineBytes bounds a single framed message. MCP stdio framing is one JSON
// message per line; the default bufio.Scanner token limit (64 KiB) is too small
// for large inputSchema / tool results, so we raise it. 32 MiB is generous
// while still bounding memory against a runaway upstream.
//
// A frame exceeding this limit surfaces as bufio.ErrTooLong, which is FATAL for
// the whole Reader: bufio.Scanner cannot resynchronize after it (the scanner is
// permanently errored), so there is no per-frame recovery mid-stream. That is
// deliberate — see Reader.Read for how the caller is expected to react.
const maxLineBytes = 32 << 20 // 32 MiB

// Reader decodes newline-delimited MCP messages from an underlying stream.
//
// Framing (MCP 2025-06-18 stdio transport, docs/MCP_NOTES.md §2):
//   - one JSON message per line, delimited by '\n';
//   - messages MUST NOT contain embedded newlines;
//   - UTF-8; JSON-RPC batching was removed in 2025-06-18, so each line is a
//     single JSON object (a leading '[' — a batch array — is rejected).
//
// Reader is NOT safe for concurrent use; use one Reader per goroutine reader.
type Reader struct {
	sc *bufio.Scanner
}

// NewReader wraps r with MCP framing.
func NewReader(r io.Reader) *Reader {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	return &Reader{sc: sc}
}

// Read returns the next message, or io.EOF when the stream ends. Blank lines are
// skipped (some servers pad output). A line that is not a single JSON object
// yields a parse error but does not desynchronize the stream — the caller may
// keep reading subsequent lines.
//
// A frame exceeding maxLineBytes (bufio.ErrTooLong) is different: it is a fatal
// transport error for THIS connection — bufio.Scanner cannot recover from it,
// so every subsequent Read fails too. The caller (the stdio reader loop in
// internal/upstream) treats any such error like a dead stream: it tears the
// connection down (done channel closes, pending calls fail), and the registry's
// auto-restart supervisor — when enabled — relaunches the upstream fresh. That
// "tear down and relaunch" is the intended recovery path, not per-scanner
// resynchronization, which bufio.Scanner does not support.
func (r *Reader) Read() (*Message, error) {
	for {
		if !r.sc.Scan() {
			if err := r.sc.Err(); err != nil {
				return nil, fmt.Errorf("mcp: read frame: %w", err)
			}
			return nil, io.EOF
		}
		line := bytes.TrimSpace(r.sc.Bytes())
		if len(line) == 0 {
			continue
		}
		return decodeLine(line)
	}
}

// decodeLine parses one framed line into a Message. A JSON array (batch) is
// rejected explicitly because batching is not supported in MCP 2025-06-18.
func decodeLine(line []byte) (*Message, error) {
	if line[0] == '[' {
		return nil, fmt.Errorf("mcp: JSON-RPC batching is not supported (spec 2025-06-18)")
	}
	var m Message
	dec := json.NewDecoder(bytes.NewReader(line))
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("mcp: decode message: %w", err)
	}
	return &m, nil
}

// Decode parses a single framed line (without the trailing newline) into a
// Message. Useful for tests and callers that already have line boundaries.
func Decode(line []byte) (*Message, error) {
	t := bytes.TrimSpace(line)
	if len(t) == 0 {
		return nil, ErrEmptyMessage
	}
	return decodeLine(t)
}

// Writer encodes MCP messages as newline-delimited JSON to an underlying stream.
//
// Writes are serialized by a mutex: many upstream calls run on separate
// goroutines and concurrent writes to one stdin would interleave bytes.
type Writer struct {
	mu sync.Mutex
	w  io.Writer
}

// NewWriter wraps w with MCP framing and write serialization.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Write encodes m as one line (json + '\n') and writes it atomically w.r.t.
// other Write calls on the same Writer.
//
// json.Marshal escapes control characters (including any '\n' inside string
// values) as \uXXXX / \n escapes, so the encoded body is guaranteed single-line;
// the appended '\n' is purely the frame delimiter.
func (w *Writer) Write(m *Message) error {
	if m.JSONRPC == "" {
		m.JSONRPC = Version
	}
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("mcp: marshal message: %w", err)
	}
	b = append(b, '\n')

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.w.Write(b); err != nil {
		return fmt.Errorf("mcp: write frame: %w", err)
	}
	return nil
}

// Encode renders m as a single framed line (json + '\n'). Test/helper convenience.
func Encode(m *Message) ([]byte, error) {
	if m.JSONRPC == "" {
		m.JSONRPC = Version
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("mcp: marshal message: %w", err)
	}
	return append(b, '\n'), nil
}
