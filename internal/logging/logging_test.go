package logging

import (
	"bytes"
	"encoding/json"
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
