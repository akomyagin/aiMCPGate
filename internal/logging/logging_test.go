package logging

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
// the raw arguments and result verbatim (the opt-in Stage 10 debug log; secret
// masking happens in the caller — registry.recordPayload — not in the writer).
func TestPayloadLogRecord(t *testing.T) {
	var buf bytes.Buffer
	log := NewPayloadLogWriter(&buf)
	log.Record(PayloadRecord{
		Time:      time.Now(),
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
}

// TestMaskSecrets pins the SKILL §6 payload masking: values of top-level keys
// whose names look secret-carrying are replaced by "***", every other field is
// preserved, and non-object payloads pass through untouched.
func TestMaskSecrets(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "suspicious keys masked, others kept",
			in:   `{"api_key":"secret123","query":"hello"}`,
			want: `{"api_key":"***","query":"hello"}`,
		},
		{
			name: "case-insensitive markers",
			in:   `{"Authorization":"Bearer abc","AccessToken":"t","PASSWORD":"p"}`,
			want: `{"AccessToken":"***","Authorization":"***","PASSWORD":"***"}`,
		},
		{
			name: "no suspicious keys returns original bytes",
			in:   `{"q":"secret-token","n":1}`,
			want: `{"q":"secret-token","n":1}`, // value may look secret; only KEY names matter
		},
		{
			name: "nested objects are not descended into",
			in:   `{"config":{"token":"inner"},"query":"x"}`,
			want: `{"config":{"token":"inner"},"query":"x"}`,
		},
		{name: "array passthrough", in: `[{"token":"t"}]`, want: `[{"token":"t"}]`},
		{name: "scalar passthrough", in: `"token"`, want: `"token"`},
		{name: "null passthrough", in: `null`, want: `null`},
		{name: "malformed passthrough", in: `{"token":`, want: `{"token":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MaskSecrets(json.RawMessage(tt.in))
			// Compare semantically where the output is re-marshaled (map key
			// order is not stable), byte-for-byte where passthrough is claimed.
			if tt.in == tt.want {
				if string(got) != tt.want {
					t.Fatalf("MaskSecrets(%s) = %s, want unchanged", tt.in, got)
				}
				return
			}
			var gotV, wantV map[string]any
			if err := json.Unmarshal(got, &gotV); err != nil {
				t.Fatalf("MaskSecrets(%s) produced invalid JSON %s: %v", tt.in, got, err)
			}
			if err := json.Unmarshal([]byte(tt.want), &wantV); err != nil {
				t.Fatalf("bad want %s: %v", tt.want, err)
			}
			if !reflect.DeepEqual(gotV, wantV) {
				t.Fatalf("MaskSecrets(%s) = %s, want %s", tt.in, got, tt.want)
			}
		})
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

// TestReadEntriesSplitsCallsAndEvents (Stage 18, L1) pins the journal's two-kind
// read: a pre-Stage-18 call line (no "kind"), a current call line, an event line
// and a broken trailing line. ReadEntries must return the three good lines in
// file order with the right kind each; ReadRecords on the same stream must see
// exactly the two calls.
func TestReadEntriesSplitsCallsAndEvents(t *testing.T) {
	data := `{"time":"2026-07-29T10:00:00Z","upstream":"old","method":"tools/call","tool":"old__x","ok":true}` + "\n" +
		`{"time":"2026-07-29T10:00:01Z","upstream":"new","method":"tools/call","tool":"new__y","ok":false,"error":"boom"}` + "\n" +
		`{"time":"2026-07-29T10:00:02Z","kind":"event","event":"upstream_start_failed","upstream":"broken","detail":"exec: no such file"}` + "\n" +
		`{"time":"2026-07-29T10:00:03Z","upstream":"trunc`

	entries, err := ReadEntries(strings.NewReader(data))
	if err != nil {
		t.Fatalf("ReadEntries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3: %+v", len(entries), entries)
	}
	if entries[0].Call == nil || entries[0].Call.Upstream != "old" {
		t.Errorf("entry[0] = %+v, want call for upstream old", entries[0])
	}
	if entries[1].Call == nil || entries[1].Call.Upstream != "new" {
		t.Errorf("entry[1] = %+v, want call for upstream new", entries[1])
	}
	if entries[2].Event == nil {
		t.Fatalf("entry[2] = %+v, want an event", entries[2])
	}
	if entries[2].Call != nil {
		t.Errorf("entry[2] carries both a call and an event: %+v", entries[2])
	}
	ev := entries[2].Event
	if ev.Event != EventUpstreamStartFailed || ev.Upstream != "broken" || ev.Detail == "" {
		t.Errorf("event = %+v, want start-failed for broken with a detail", ev)
	}

	recs, err := ReadRecords(strings.NewReader(data))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("ReadRecords got %d records, want exactly the 2 calls: %+v", len(recs), recs)
	}
	if recs[0].Upstream != "old" || recs[1].Upstream != "new" {
		t.Errorf("ReadRecords = %+v, want [old new] in file order", recs)
	}
}

// TestReadEntriesSkipsUnknownKind (Stage 18, L2) pins the forward-compat rule: a
// line with a THIRD kind this binary does not know is skipped by both readers
// rather than guessed at as a call.
func TestReadEntriesSkipsUnknownKind(t *testing.T) {
	data := `{"time":"2026-07-29T10:00:00Z","kind":"metrics","upstream":"a","value":42}` + "\n" +
		`{"time":"2026-07-29T10:00:01Z","upstream":"a","method":"tools/call","tool":"a__x","ok":true}` + "\n"

	entries, err := ReadEntries(strings.NewReader(data))
	if err != nil {
		t.Fatalf("ReadEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Call == nil || entries[0].Call.Tool != "a__x" {
		t.Fatalf("entries = %+v, want only the one call line", entries)
	}
	recs, err := ReadRecords(strings.NewReader(data))
	if err != nil {
		t.Fatalf("ReadRecords: %v", err)
	}
	if len(recs) != 1 || recs[0].Tool != "a__x" {
		t.Fatalf("records = %+v, want only the one call line", recs)
	}
}

// TestCallLogRecordEventWritesJSONLine (Stage 18, L3) pins the write side of
// events: one JSON line carrying the discriminator, an omitted count at 1, and
// the same post-Close drop semantics Record has.
func TestCallLogRecordEventWritesJSONLine(t *testing.T) {
	var buf bytes.Buffer
	cl := NewCallLogWriter(&buf)
	cl.RecordEvent(EventRecord{
		Time:    time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC),
		Kind:    KindEvent,
		Event:   EventNotificationDropped,
		Subject: "notifications/tools/list_changed",
		Detail:  "subscriber buffer full",
		// A single occurrence leaves Count at zero: on the wire "absent == 1".
		// registry.emitEvent normalizes an explicit 1 down to zero, so this is
		// what a real single-occurrence event looks like.
	})

	line := strings.TrimSpace(buf.String())
	if strings.Count(buf.String(), "\n") != 1 {
		t.Fatalf("want exactly one line, got %q", buf.String())
	}
	if strings.Contains(line, `"count"`) {
		t.Errorf("a single occurrence must omit count, got %q", line)
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		t.Fatalf("unmarshal event line: %v (line=%q)", err, line)
	}
	if raw["kind"] != KindEvent {
		t.Errorf("kind = %v, want %q", raw["kind"], KindEvent)
	}
	if raw["event"] != EventNotificationDropped {
		t.Errorf("event = %v, want %q", raw["event"], EventNotificationDropped)
	}

	if err := cl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cl.RecordEvent(EventRecord{Kind: KindEvent, Event: EventUpstreamGaveUp})
	if got := strings.Count(buf.String(), "\n"); got != 1 {
		t.Fatalf("journal has %d lines after post-Close RecordEvent, want 1", got)
	}
}

// TestEventRecordCountIsWrittenWhenAboveOne guards the other half of the
// omitempty rule: a coalesced event must carry its count.
func TestEventRecordCountIsWrittenWhenAboveOne(t *testing.T) {
	var buf bytes.Buffer
	cl := NewCallLogWriter(&buf)
	cl.RecordEvent(EventRecord{Kind: KindEvent, Event: EventNotificationDropped, Count: 7})
	entries, err := ReadEntries(&buf)
	if err != nil {
		t.Fatalf("ReadEntries: %v", err)
	}
	if len(entries) != 1 || entries[0].Event == nil || entries[0].Event.Count != 7 {
		t.Fatalf("entries = %+v, want one event with count 7", entries)
	}
}
