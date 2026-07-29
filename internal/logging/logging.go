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
	"strings"
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
	Upstream string        `json:"upstream"`         // which upstream served the call
	Method   string        `json:"method"`           // JSON-RPC method, e.g. "tools/call"
	Tool     string        `json:"tool"`             // tool/resource name, if applicable
	Client   string        `json:"client,omitempty"` // calling client, "name/version" from its initialize; empty when unidentified
	Duration time.Duration `json:"duration_ns"`
	OK       bool          `json:"ok"`
	Err      string        `json:"error,omitempty"` // sanitized error, no secrets
}

// EventRecord is one operator-facing event in the same JSON-lines journal
// CallRecords go to. Kind is the discriminator: call lines never carry it
// (CallRecord is unchanged, bytes-for-bytes), event lines always say "event".
//
// These are the things that used to reach only stderr — invisible in stdio mode,
// where the MCP client owns the terminal — or only slog's Debug level, silent
// under the default log_level: info. They carry NO payloads: upstream names,
// method/tool/prompt names, resource URIs/templates and fixed gateway-authored
// reasons only (SKILL §6; see the Stage 18 plan §4).
//
// Backward compatibility, stated honestly: an OLD binary (<= v0.4.0) reading a
// NEW journal decodes an event line into a near-empty CallRecord (only Time and
// Upstream set) and renders it as a sparse ERR line. Hiding event lines from old
// readers entirely is impossible without corrupting their stream — a
// type-conflicting line breaks their decoder's resync and eats the NEXT record,
// which is strictly worse than one sparse line. Read a journal with the same (or
// a newer) binary that wrote it.
type EventRecord struct {
	Time     time.Time `json:"time"`
	Kind     string    `json:"kind"`               // always KindEvent — the discriminator
	Event    string    `json:"event"`              // one of the Event* constants below
	Upstream string    `json:"upstream,omitempty"` // which upstream, when attributable
	Subject  string    `json:"subject,omitempty"`  // method / tool / uri / template — the thing affected
	Detail   string    `json:"detail,omitempty"`   // sanitized human-readable reason
	Count    int       `json:"count,omitempty"`    // occurrences coalesced into this line (absent == 1)
}

// KindEvent is the value of EventRecord.Kind — the journal's line discriminator.
const KindEvent = "event"

// Event identifiers. These constants are the single source of names shared by
// the emitter (internal/registry) and the reader (`mcp-gate logs`).
const (
	EventUpstreamStartFailed     = "upstream_start_failed"     // Start: upstream bring-up failed, it is absent from the catalog
	EventUpstreamGaveUp          = "upstream_gave_up"          // supervisor gave up, upstream dropped from the catalog
	EventNotificationDropped     = "notification_dropped"      // forwardNotification: subscriber buffer full
	EventServerRequestDropped    = "server_request_dropped"    // publishServerReq delivered to nobody → the tool call was refused
	EventSSEStreamUnavailable    = "sse_stream_unavailable"    // HTTP upstream offers no GET SSE: no list_changed from it, ever
	EventCatalogCollision        = "catalog_collision"         // keep-first: a tool/prompt/resource is hidden from the client
	EventCatalogBadTemplate      = "catalog_bad_template"      // a URI template is exposed but can never match
	EventResultTruncationSkipped = "result_truncation_skipped" // max_result_bytes silently did not apply
)

// CallLog persists CallRecords and operator EventRecords — both go to the same
// JSON-lines journal, told apart by EventRecord.Kind. Implementations must be
// safe for concurrent use — many upstream calls run on separate goroutines.
type CallLog interface {
	Record(r CallRecord)
	RecordEvent(e EventRecord)
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
// (no syscall) and intentional.
type jsonLog[T any] struct {
	mu     sync.Mutex
	w      io.Writer
	closer io.Closer // non-nil only when we opened a file we own
	closed bool      // set by Close (under mu); Record becomes a no-op after
}

// Record marshals r and appends it as one JSON line. The record's Time is the
// caller's responsibility: Record never stamps it (registry.audit passes the
// call start, recordPayload passes time.Now()). Marshaling happens outside the
// mutex — r is a local copy, so concurrent Records only serialize on the write.
func (l *jsonLog[T]) Record(r T) {
	l.writeLine(r)
}

// RecordEvent appends one operator EventRecord to the same journal. It is a
// method on the generic type, so jsonCallLog gets it for free; jsonPayloadLog
// gets it too, harmlessly — the PayloadLog interface does not ask for it.
func (l *jsonLog[T]) RecordEvent(e EventRecord) {
	l.writeLine(e)
}

// writeLine is the shared body of Record/RecordEvent: marshal outside the mutex
// (v is the caller's copy, so concurrent writers only serialize on the write),
// then append one line under mu, honoring the closed flag.
func (l *jsonLog[T]) writeLine(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return // records are normally marshalable; ignore defensively
	}
	b = append(b, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return // dropped explicitly: no write to a closed file
	}
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
		return &jsonCallLog{w: os.Stderr}, nil
	}
	f, err := openAppendFile(logFile)
	if err != nil {
		return nil, fmt.Errorf("open call log %q: %w", logFile, err)
	}
	return &jsonCallLog{w: f, closer: f}, nil
}

// NewCallLogWriter builds a call log over an arbitrary writer (used in tests).
func NewCallLogWriter(w io.Writer) CallLog {
	return &jsonCallLog{w: w}
}

// PayloadRecord is one entry of the OPT-IN payload debug log — the full
// arguments and result of a tool call. Unlike CallRecord (metadata only), this
// deliberately carries the raw request/response bodies; it exists strictly for
// debugging and is off by default (SKILL §6, Stage 10). Arguments/Result are
// json.RawMessage so payloads pass through verbatim — except that the writer
// (registry.recordPayload) runs both through MaskSecrets first, so top-level
// secret-looking fields never hit disk. A nil raw message is emitted as JSON
// null.
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

// secretKeyMarkers flag a top-level JSON key as secret-carrying when the key
// name contains one of them (case-insensitive). The list follows the kinds of
// values the config layer documents as secrets — auth_token, Authorization
// headers, upstream env entries holding API keys (config.go, SKILL §6).
var secretKeyMarkers = []string{"token", "secret", "password", "key", "authorization"}

// maskedValue is what a secret-carrying field's value becomes in the payload log.
var maskedValue = json.RawMessage(`"***"`)

// MaskSecrets returns raw with the values of secret-looking top-level object
// keys (per secretKeyMarkers) replaced by "***", leaving every other field
// intact. This is the SKILL §6 masking pass for the opt-in payload debug log.
// It is deliberately a shallow, best-effort guard: only a top-level walk over
// a JSON object — nested objects are not descended into, and anything that is
// not a JSON object (array, scalar, null, malformed) is returned unchanged
// rather than second-guessed. If nothing matched, raw is returned as-is, byte
// for byte.
func MaskSecrets(raw json.RawMessage) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return raw // not a JSON object (or JSON null) — pass through verbatim
	}
	masked := false
	for k := range obj {
		lower := strings.ToLower(k)
		for _, marker := range secretKeyMarkers {
			if strings.Contains(lower, marker) {
				obj[k] = maskedValue
				masked = true
				break
			}
		}
	}
	if !masked {
		return raw // keep the original bytes (and key order) untouched
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return raw // unreachable in practice: obj came from valid JSON
	}
	return b
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
	return &jsonPayloadLog{w: f, closer: f}, nil
}

// NewPayloadLogWriter builds a payload log over an arbitrary writer (tests).
func NewPayloadLogWriter(w io.Writer) PayloadLog {
	return &jsonPayloadLog{w: w}
}

// Entry is one line of the journal: exactly one of the two fields is non-nil.
type Entry struct {
	Call  *CallRecord
	Event *EventRecord
}

// ReadEntries decodes a JSON-lines journal (the format NewCallLog writes) into
// its two kinds of lines, preserving file order. It is the read side consumed by
// `mcp-gate logs`.
//
// Each line is first taken as raw JSON, then probed for the "kind"
// discriminator: absent (or empty) means a CallRecord — that is every line
// written before Stage 18 and every call line since — and "event" means an
// EventRecord. A line whose kind is some THIRD, unknown value is skipped
// entirely: a reader must not invent an interpretation for a type it does not
// know. A line that fails to decode at all is skipped rather than aborting the
// whole read, so a partially-written trailing line (the writer crashed
// mid-append) does not hide the records before it.
func ReadEntries(r io.Reader) ([]Entry, error) {
	dec := json.NewDecoder(r)
	var out []Entry
	for {
		var raw json.RawMessage
		err := dec.Decode(&raw)
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
		var probe struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			continue // not an object with a string kind — unusable line
		}
		switch probe.Kind {
		case "":
			var rec CallRecord
			if err := json.Unmarshal(raw, &rec); err != nil {
				continue
			}
			out = append(out, Entry{Call: &rec})
		case KindEvent:
			var ev EventRecord
			if err := json.Unmarshal(raw, &ev); err != nil {
				continue
			}
			out = append(out, Entry{Event: &ev})
		default:
			continue // unknown kind: a future record type this binary cannot read
		}
	}
}

// ReadRecords decodes only the CallRecords of a JSON-lines journal. It is
// ReadEntries projected onto call lines, kept as its own function because that
// is what every pre-Stage-18 caller wants and what old journals contain
// exclusively. CallRecord is the single shared shape between writer and reader —
// no parallel struct.
func ReadRecords(r io.Reader) ([]CallRecord, error) {
	entries, err := ReadEntries(r)
	if err != nil {
		return nil, err
	}
	var out []CallRecord
	for _, e := range entries {
		if e.Call != nil {
			out = append(out, *e.Call)
		}
	}
	return out, nil
}
