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

// jsonLog writes one JSON object per line to an io.Writer, serialized by a
// mutex. It is the single implementation behind both the audit log
// (jsonCallLog) and the opt-in payload debug log (jsonPayloadLog) — the two
// differ only in record type and in what the record is allowed to carry.
//
// For the audit log, secrets are never written: CallRecord carries only
// metadata (upstream, method, tool name, latency, ok/error) — never the call
// arguments, which may contain tokens (SKILL §6). The error string is expected
// to be sanitized by the caller before being placed in CallRecord.Err.
//
// A Record arriving after Close is dropped EXPLICITLY (the closed flag, checked
// under mu) rather than relying on the OS silently rejecting a write to a
// closed file. Draining such late records gracefully is a deeper lifecycle
// concern deliberately out of scope here — the flag just makes the loss cheap
// (no marshal, no syscall) and intentional.
type jsonLog[T any] struct {
	mu     sync.Mutex
	w      io.Writer
	closer io.Closer // non-nil only when we opened a file we own
	closed bool      // set by Close (under mu); Record becomes a no-op after
	stamp  func(*T)  // fills a zero timestamp with time.Now(); set by the constructor
}

func (l *jsonLog[T]) Record(r T) {
	if l.stamp != nil {
		l.stamp(&r)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return // dropped explicitly: no marshal, no write to a closed file
	}
	b, err := json.Marshal(r)
	if err != nil {
		return // records are normally marshalable; ignore defensively
	}
	b = append(b, '\n')
	_, _ = l.w.Write(b)
}

func (l *jsonLog[T]) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true // set BEFORE closing the file, under the same mu Record takes
	if l.closer != nil {
		return l.closer.Close()
	}
	return nil
}

// jsonCallLog / jsonPayloadLog are aliases (not new types) so the constructors'
// struct literals keep working and Record(CallRecord)/Record(PayloadRecord)
// satisfy the CallLog/PayloadLog interfaces unchanged.
type (
	jsonCallLog    = jsonLog[CallRecord]
	jsonPayloadLog = jsonLog[PayloadRecord]
)

func stampCall(r *CallRecord) {
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
}

func stampPayload(r *PayloadRecord) {
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
}

// openAppendFile opens path for append (creating it if missing) with 0600 —
// the shared file-opening contract for both the audit log and the payload
// debug log: callers only differ in what error-message prefix to use.
// oNoFollow (syscall.O_NOFOLLOW on Unix, 0 elsewhere — see the build-tagged
// nofollow_*.go files) guards against a symlink planted at the log path: if
// path is an existing symlink the open fails (ELOOP) instead of silently
// following it and appending gateway logs to whatever file the link points at.
func openAppendFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY|oNoFollow, 0o600)
}

// NewCallLog returns a CallLog writing JSON lines. An empty logFile writes to
// stderr; otherwise the file is opened for append (created if missing).
func NewCallLog(logFile string) (CallLog, error) {
	if logFile == "" {
		return &jsonCallLog{w: os.Stderr, stamp: stampCall}, nil
	}
	f, err := openAppendFile(logFile)
	if err != nil {
		return nil, fmt.Errorf("open call log %q: %w", logFile, err)
	}
	return &jsonCallLog{w: f, closer: f, stamp: stampCall}, nil
}

// NewCallLogWriter builds a call log over an arbitrary writer (used in tests).
func NewCallLogWriter(w io.Writer) CallLog {
	return &jsonCallLog{w: w, stamp: stampCall}
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
	ErrorData json.RawMessage `json:"error_data,omitempty"` // JSON-RPC error.data, if the upstream sent one
}

// PayloadLog persists PayloadRecords. Implementations must be safe for
// concurrent use — many upstream calls run on separate goroutines.
type PayloadLog interface {
	Record(r PayloadRecord)
	io.Closer
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
	f, err := openAppendFile(path)
	if err != nil {
		return nil, fmt.Errorf("open payload log %q: %w", path, err)
	}
	return &jsonPayloadLog{w: f, closer: f, stamp: stampPayload}, nil
}

// NewPayloadLogWriter builds a payload log over an arbitrary writer (tests).
func NewPayloadLogWriter(w io.Writer) PayloadLog {
	return &jsonPayloadLog{w: w, stamp: stampPayload}
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
