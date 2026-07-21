package logging

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestCallLogRoundTrip writes records through the JSON-lines writer and reads
// them back with ReadRecords — the exact path `mcp-gate logs` uses.
func TestCallLogRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	log := NewCallLogWriter(&buf)
	log.Record(CallRecord{Upstream: "github", Method: "tools/call", Tool: "github__search", OK: true, Duration: 12 * time.Millisecond})
	log.Record(CallRecord{Upstream: "web", Method: "tools/call", Tool: "web__fetch", OK: false, Err: "boom"})

	got, err := ReadRecords(&buf)
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	if got[0].Tool != "github__search" || !got[0].OK {
		t.Errorf("record[0] = %+v", got[0])
	}
	if got[1].Err != "boom" || got[1].OK {
		t.Errorf("record[1] = %+v", got[1])
	}
}

// TestReadRecordsSkipsTrailingGarbage ensures a partially-written trailing line
// (writer crashed mid-append) does not hide the good records before it.
func TestReadRecordsSkipsTrailingGarbage(t *testing.T) {
	data := `{"upstream":"a","tool":"a__x","ok":true}` + "\n" + `{"upstream":"b","tool":`
	got, err := ReadRecords(strings.NewReader(data))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(got) != 1 || got[0].Upstream != "a" {
		t.Fatalf("want the one good record, got %+v", got)
	}
}

// TestPayloadLogRecord writes a PayloadRecord and checks the JSON line carries
// the raw arguments and result verbatim (the opt-in Stage 10 debug log).
func TestPayloadLogRecord(t *testing.T) {
	var buf bytes.Buffer
	log := NewPayloadLogWriter(&buf)
	log.Record(PayloadRecord{
		Upstream:  "github",
		Tool:      "github__search",
		Method:    "tools/call",
		Arguments: json.RawMessage(`{"q":"secret-token"}`),
		Result:    json.RawMessage(`{"items":[1,2,3]}`),
	})

	line := strings.TrimSpace(buf.String())
	var rec PayloadRecord
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("unmarshal payload record: %v (line=%q)", err, line)
	}
	if rec.Upstream != "github" || rec.Tool != "github__search" || rec.Method != "tools/call" {
		t.Errorf("metadata mismatch: %+v", rec)
	}
	if string(rec.Arguments) != `{"q":"secret-token"}` {
		t.Errorf("arguments = %s, want raw passthrough", rec.Arguments)
	}
	if string(rec.Result) != `{"items":[1,2,3]}` {
		t.Errorf("result = %s, want raw passthrough", rec.Result)
	}
	if rec.Time.IsZero() {
		t.Error("Record should stamp a non-zero time")
	}
}

// TestOpenAppendFileRefusesSymlink pins the symlink defence (O_NOFOLLOW): a
// log path that is an existing symlink must fail to open rather than silently
// appending to whatever the link points at. Skipped on Windows, where
// os.O_NOFOLLOW is a no-op (defined as 0).
func TestOpenAppendFileRefusesSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.O_NOFOLLOW is a no-op on Windows")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "target.log")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("create target: %v", err)
	}
	link := filepath.Join(dir, "link.log")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	if f, err := openAppendFile(link); err == nil {
		_ = f.Close()
		t.Fatal("openAppendFile followed a symlink; want an error (ELOOP)")
	}
	if _, err := NewCallLog(link); err == nil {
		t.Fatal("NewCallLog opened a symlinked path; want an error")
	}
	if _, err := NewPayloadLog(link); err == nil {
		t.Fatal("NewPayloadLog opened a symlinked path; want an error")
	}

	// A plain file still opens fine.
	f, err := openAppendFile(target)
	if err != nil {
		t.Fatalf("openAppendFile on a regular file: %v", err)
	}
	_ = f.Close()
}

// TestRecordAfterCloseIsDropped verifies the explicit closed flag: a Record
// arriving after Close must not panic and must not append anything — the drop
// is intentional and cheap, not a swallowed OS write error.
func TestRecordAfterCloseIsDropped(t *testing.T) {
	var buf bytes.Buffer
	cl := NewCallLogWriter(&buf)
	cl.Record(CallRecord{Upstream: "a", Tool: "a__x", OK: true})
	if err := cl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cl.Record(CallRecord{Upstream: "b", Tool: "b__y", OK: true})
	if got := strings.Count(buf.String(), "\n"); got != 1 {
		t.Fatalf("call log has %d lines after post-Close Record, want 1", got)
	}

	var pbuf bytes.Buffer
	pl := NewPayloadLogWriter(&pbuf)
	pl.Record(PayloadRecord{Upstream: "a", Tool: "a__x"})
	if err := pl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	pl.Record(PayloadRecord{Upstream: "b", Tool: "b__y"})
	if got := strings.Count(pbuf.String(), "\n"); got != 1 {
		t.Fatalf("payload log has %d lines after post-Close Record, want 1", got)
	}
}

// TestPayloadLogDisabled asserts the no-op payload log (empty path) writes
// nothing, never panics, and closes cleanly — the default off state.
func TestPayloadLogDisabled(t *testing.T) {
	log, err := NewPayloadLog("")
	if err != nil {
		t.Fatalf("NewPayloadLog(\"\"): %v", err)
	}
	// Must not panic and must not write anywhere.
	log.Record(PayloadRecord{Upstream: "x", Tool: "x__y", Arguments: json.RawMessage(`{"a":1}`)})
	if err := log.Close(); err != nil {
		t.Errorf("Close on no-op payload log = %v, want nil", err)
	}
}
