package logging

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// TestCallLogRoundTrip writes records through the JSON-lines writer and reads
// them back with ReadRecords — the exact path `aimcpgate logs` uses.
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
