// Package logging provides the gateway's structured logger plus the tool-call
// audit log — the feature that makes aiMCPGate more than a dumb proxy.
//
// Two concerns live here, kept separate on purpose:
//   - New: an operational slog.Logger for gateway diagnostics (to stderr).
//   - CallRecord / CallLog: a structured record of every tool/resource call
//     routed through the gateway (which upstream, what was called, latency,
//     success/error), written as JSON lines for later inspection.
//
// Реализация — Этап 1+ (the call log currently discards records).
package logging

import (
	"io"
	"log/slog"
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
// gateway. This is the shape that Фаза 2's log viewer consumes.
type CallRecord struct {
	Time     time.Time     `json:"time"`
	Upstream string        `json:"upstream"` // which upstream served the call
	Method   string        `json:"method"`   // JSON-RPC method, e.g. "tools/call"
	Tool     string        `json:"tool"`     // tool/resource name, if applicable
	Client   string        `json:"client"`   // client identity (Фаза 2 access policy)
	Duration time.Duration `json:"duration_ns"`
	OK       bool          `json:"ok"`
	Err      string        `json:"error,omitempty"` // sanitized error, no secrets
}

// CallLog persists CallRecords. Implementations must be safe for concurrent
// use — many upstream calls run on separate goroutines.
type CallLog interface {
	Record(r CallRecord)
}

// nopCallLog discards records. Замена реальной реализацией — Этап 1.
type nopCallLog struct{}

// NewCallLog returns a CallLog. For now it always returns a no-op sink; Этап 1
// wires the JSON-lines file/stdout writer described in TECHNICAL_PLAN.md.
func NewCallLog(_ string) (CallLog, error) {
	// TODO(Этап 1): open logFile (append, JSON lines), guard with a mutex,
	// fall back to stderr when logFile is empty.
	return nopCallLog{}, nil
}

func (nopCallLog) Record(CallRecord) {}
