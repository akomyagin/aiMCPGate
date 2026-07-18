// Package logging provides the gateway's structured logger plus the tool-call
// audit log — the feature that makes aiMCPGate more than a dumb proxy.
//
// Two concerns live here, kept separate on purpose:
//   - New: an operational slog.Logger for gateway diagnostics (to stderr).
//   - CallRecord / CallLog: a structured record of every tool/resource call
//     routed through the gateway (which upstream, what was called, latency,
//     success/error), written as JSON lines for later inspection.
package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

// New builds the operational logger at the given level, writing to w.
//
// level is one of "debug" | "info" | "warn" | "error"; anything else falls
// back to info. Secrets (upstream API keys / tokens) must never be passed to
// this logger — see SKILL.md.
func New(level string, w io.Writer) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl}))
}

// CallRecord is one audit entry for a tool/resource call routed through the
// gateway. This is the shape that Phase 2's log viewer consumes.
type CallRecord struct {
	Time     time.Time     `json:"time"`
	Upstream string        `json:"upstream"` // which upstream served the call
	Method   string        `json:"method"`   // JSON-RPC method, e.g. "tools/call"
	Tool     string        `json:"tool"`     // tool/resource name, if applicable
	Client   string        `json:"client"`   // client identity (Phase 2 access policy)
	Duration time.Duration `json:"duration_ns"`
	OK       bool          `json:"ok"`
	Err      string        `json:"error,omitempty"` // sanitized error, no secrets
}

// CallLog persists CallRecords. Implementations must be safe for concurrent
// use — many upstream calls run on separate goroutines.
type CallLog interface {
	Record(r CallRecord)
	io.Closer
}

// jsonCallLog writes one JSON object per line to an io.Writer, serialized by a
// mutex. Secrets are never written: CallRecord carries only metadata (upstream,
// method, tool name, latency, ok/error) — never the call arguments, which may
// contain tokens (SKILL §6). The error string is expected to be sanitized by
// the caller before being placed in CallRecord.Err.
type jsonCallLog struct {
	mu     sync.Mutex
	w      io.Writer
	closer io.Closer // non-nil only when we opened a file we own
}

// NewCallLog returns a CallLog writing JSON lines. An empty logFile writes to
// stderr; otherwise the file is opened for append (created if missing).
func NewCallLog(logFile string) (CallLog, error) {
	if logFile == "" {
		return &jsonCallLog{w: os.Stderr}, nil
	}
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open call log %q: %w", logFile, err)
	}
	return &jsonCallLog{w: f, closer: f}, nil
}

// NewCallLogWriter builds a call log over an arbitrary writer (used in tests).
func NewCallLogWriter(w io.Writer) CallLog {
	return &jsonCallLog{w: w}
}

func (l *jsonCallLog) Record(r CallRecord) {
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
	b, err := json.Marshal(r)
	if err != nil {
		return // a CallRecord is always marshalable; ignore defensively
	}
	b = append(b, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.w.Write(b)
}

func (l *jsonCallLog) Close() error {
	if l.closer != nil {
		return l.closer.Close()
	}
	return nil
}

// PayloadRecord is one entry of the OPT-IN payload debug log — the full
// arguments and result of a tool call. Unlike CallRecord (metadata only), this
// deliberately carries the raw request/response bodies, which may contain
// secrets; it exists strictly for debugging and is off by default (SKILL §6,
// Stage 10). Arguments/Result are json.RawMessage so payloads pass through
// verbatim; a nil raw message is emitted as JSON null.
type PayloadRecord struct {
	Time      time.Time       `json:"time"`
	Upstream  string          `json:"upstream"`
	Tool      string          `json:"tool"`
	Method    string          `json:"method"`
	OK        bool            `json:"ok"` // no omitempty: false is the load-bearing value
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Err       string          `json:"error,omitempty"`
}

// PayloadLog persists PayloadRecords. Implementations must be safe for
// concurrent use — many upstream calls run on separate goroutines.
type PayloadLog interface {
	Record(r PayloadRecord)
	io.Closer
}

// jsonPayloadLog writes one JSON object per line to an io.Writer, serialized by
// a mutex — the same shape as jsonCallLog, but for the opt-in payload debug log.
type jsonPayloadLog struct {
	mu     sync.Mutex
	w      io.Writer
	closer io.Closer // non-nil only when we opened a file we own
}

// noopPayloadLog is the default when payload logging is disabled: Record does
// nothing and Close is a no-op. Returning this instead of nil lets callers
// invoke Record unconditionally, without a nil check on the hot path.
type noopPayloadLog struct{}

func (noopPayloadLog) Record(PayloadRecord) {}
func (noopPayloadLog) Close() error         { return nil }

// NewPayloadLog returns a PayloadLog writing JSON lines. An empty path disables
// payload logging and returns a no-op implementation. Otherwise the file is
// opened for append (created if missing) with 0600, like NewCallLog.
func NewPayloadLog(path string) (PayloadLog, error) {
	if path == "" {
		return noopPayloadLog{}, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open payload log %q: %w", path, err)
	}
	return &jsonPayloadLog{w: f, closer: f}, nil
}

// NewPayloadLogWriter builds a payload log over an arbitrary writer (tests).
func NewPayloadLogWriter(w io.Writer) PayloadLog {
	return &jsonPayloadLog{w: w}
}

func (l *jsonPayloadLog) Record(r PayloadRecord) {
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
	b, err := json.Marshal(r)
	if err != nil {
		return // ignore defensively; a PayloadRecord is normally marshalable
	}
	b = append(b, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.w.Write(b)
}

func (l *jsonPayloadLog) Close() error {
	if l.closer != nil {
		return l.closer.Close()
	}
	return nil
}

// ReadRecords decodes CallRecords from a JSON-lines stream (the format
// NewCallLog writes). It is the read side consumed by the `mcp-gate logs`
// command. A line that fails to decode is skipped rather than aborting the whole
// read, so a partially-written trailing line (the writer crashed mid-append)
// does not hide the records before it. CallRecord is the single shared shape
// between writer and reader — no parallel struct.
func ReadRecords(r io.Reader) ([]CallRecord, error) {
	dec := json.NewDecoder(r)
	var out []CallRecord
	for {
		var rec CallRecord
		err := dec.Decode(&rec)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			// Skip a malformed line and resynchronize by advancing the decoder to
			// the next token; if that also fails the stream is unusable, so stop.
			if _, derr := dec.Token(); derr != nil {
				return out, nil
			}
			continue
		}
		out = append(out, rec)
	}
}
