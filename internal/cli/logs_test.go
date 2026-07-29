package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/logging"
)

// writeLog writes a small JSON-lines call log to a temp file and returns its
// path, so the logs command can read it exactly as it would in production.
func writeLog(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "calls.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	log := logging.NewCallLogWriter(f)
	log.Record(logging.CallRecord{Upstream: "github", Method: "tools/call", Tool: "github__search", OK: true})
	log.Record(logging.CallRecord{Upstream: "web", Method: "tools/call", Tool: "web__fetch", OK: false, Err: "timeout"})
	log.Record(logging.CallRecord{Upstream: "github", Method: "tools/call", Tool: "github__create_issue", OK: true})
	_ = f.Close()
	return path
}

// runLogsCmd executes the logs subcommand with args and captures its stdout.
func runLogsCmd(t *testing.T, args ...string) string {
	t.Helper()
	root := Build("test")
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(append([]string{"logs"}, args...))
	if err := root.Execute(); err != nil {
		t.Fatalf("logs %v: %v\n%s", args, err, out.String())
	}
	return out.String()
}

func TestLogsShowsAll(t *testing.T) {
	path := writeLog(t)
	out := runLogsCmd(t, "--file", path)
	for _, want := range []string{"github__search", "web__fetch", "github__create_issue"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs output missing %q:\n%s", want, out)
		}
	}
}

func TestLogsFilterByUpstream(t *testing.T) {
	path := writeLog(t)
	out := runLogsCmd(t, "--file", path, "--upstream", "web")
	if strings.Contains(out, "github__") {
		t.Errorf("upstream filter leaked github records:\n%s", out)
	}
	if !strings.Contains(out, "web__fetch") {
		t.Errorf("upstream filter dropped web records:\n%s", out)
	}
}

func TestLogsFilterByStatusErr(t *testing.T) {
	path := writeLog(t)
	out := runLogsCmd(t, "--file", path, "--status", "err")
	if !strings.Contains(out, "web__fetch") || !strings.Contains(out, "timeout") {
		t.Errorf("status=err should show the failing call with its error:\n%s", out)
	}
	if strings.Contains(out, "github__search") {
		t.Errorf("status=err leaked an ok record:\n%s", out)
	}
}

func TestLogsTailLimitsCount(t *testing.T) {
	path := writeLog(t)
	out := runLogsCmd(t, "--file", path, "--tail", "1")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 1 {
		t.Fatalf("tail=1 should print 1 line, got %d:\n%s", len(lines), out)
	}
	if !strings.Contains(out, "github__create_issue") {
		t.Errorf("tail=1 should show the last record, got:\n%s", out)
	}
}

func TestLogsErrorsWhenNoFileOrConfig(t *testing.T) {
	root := Build("test")
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"logs"})
	if err := root.Execute(); err == nil {
		t.Fatal("expected an error when neither --file nor --config is given")
	}
}

// mustRecordLine marshals rec exactly the way the gateway's CallLog writes it:
// one JSON object plus a trailing newline.
func mustRecordLine(t *testing.T, rec logging.CallRecord) []byte {
	t.Helper()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return append(b, '\n')
}

func TestLogsFollowStatsMutuallyExclusive(t *testing.T) {
	root := Build("test")
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"logs", "--file", "/dev/null", "--follow", "--stats"})
	if err := root.Execute(); err == nil {
		t.Fatal("logs --follow --stats must fail: the flags are mutually exclusive")
	}
}

// TestLogsStatsAggregates checks --stats against hand-built records with KNOWN
// durations, so count / error% / p50 / p95 are asserted as exact numbers, not
// just "did not panic". github__search: 20 calls of 1..20ms with 5 failures →
// count 20, 25.0% errors, p50 = sorted[int(0.5*20)]=sorted[10] = 11ms,
// p95 = sorted[int(0.95*20)]=sorted[19] = 20ms. web__fetch: one failing 7ms
// call → count 1, 100.0%, p50 = p95 = 7ms.
func TestLogsStatsAggregates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "calls.log")
	var buf bytes.Buffer
	for i := 1; i <= 20; i++ {
		buf.Write(mustRecordLine(t, logging.CallRecord{
			Upstream: "github",
			Method:   "tools/call",
			Tool:     "github__search",
			Duration: time.Duration(i) * time.Millisecond,
			OK:       i > 5, // exactly 5 failures
		}))
	}
	buf.Write(mustRecordLine(t, logging.CallRecord{
		Upstream: "web",
		Method:   "tools/call",
		Tool:     "web__fetch",
		Duration: 7 * time.Millisecond,
		OK:       false,
		Err:      "timeout",
	}))
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runLogsCmd(t, "--file", path, "--stats")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 { // header + 2 groups
		t.Fatalf("stats printed %d lines, want 3 (header + 2 rows):\n%s", len(lines), out)
	}
	if header := strings.Fields(lines[0]); !reflect.DeepEqual(header, []string{"UPSTREAM", "TOOL", "COUNT", "ERROR%", "P50", "P95"}) {
		t.Errorf("header = %v", header)
	}
	// Rows are sorted by upstream then tool: github first, web second.
	if row := strings.Fields(lines[1]); !reflect.DeepEqual(row, []string{"github", "github__search", "20", "25.0%", "11ms", "20ms"}) {
		t.Errorf("github row = %v, want [github github__search 20 25.0%% 11ms 20ms]", row)
	}
	if row := strings.Fields(lines[2]); !reflect.DeepEqual(row, []string{"web", "web__fetch", "1", "100.0%", "7ms", "7ms"}) {
		t.Errorf("web row = %v, want [web web__fetch 1 100.0%% 7ms 7ms]", row)
	}
}

// TestLogsStatsHonorsFilters: --stats must apply the same --upstream/--tool/
// --status filters as the plain listing before aggregating.
func TestLogsStatsHonorsFilters(t *testing.T) {
	path := writeLog(t) // github×2 ok, web×1 err (see writeLog)
	out := runLogsCmd(t, "--file", path, "--stats", "--upstream", "github")
	if strings.Contains(out, "web") {
		t.Errorf("--upstream github leaked a web row into --stats:\n%s", out)
	}
	if !strings.Contains(out, "github__search") || !strings.Contains(out, "github__create_issue") {
		t.Errorf("--stats missing github groups:\n%s", out)
	}
}

// syncWriter is a mutex-guarded output sink: followLog writes from its own
// goroutine while the test polls the contents.
type syncWriter struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// waitOutputContains polls w until the wanted substring shows up (follow's
// poll loop is asynchronous) or the deadline passes.
func waitOutputContains(t *testing.T, w *syncWriter, want string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if strings.Contains(w.String(), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("follow output never contained %q within %s; output:\n%s", want, within, w.String())
}

// TestLogsFollowPrintsAppendedRecords drives followLog against a live file:
// appended complete records must be printed; a trailing PARTIAL record must be
// held back until its final bytes (and newline) arrive; a truncation must make
// follow re-read from the start of the new file content.
func TestLogsFollowPrintsAppendedRecords(t *testing.T) {
	saved := followPollInterval
	followPollInterval = 5 * time.Millisecond
	defer func() { followPollInterval = saved }()

	dir := t.TempDir()
	path := filepath.Join(dir, "calls.log")
	rec1 := mustRecordLine(t, logging.CallRecord{Upstream: "github", Method: "tools/call", Tool: "github__one", OK: true})
	if err := os.WriteFile(path, rec1, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := &syncWriter{}
	done := make(chan error, 1)
	go func() {
		// Start following from the end of rec1, like runLogsFollow would.
		done <- followLog(ctx, out, path, int64(len(rec1)), nil, recordFilter{})
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("followLog did not return after context cancellation")
		}
	}()

	appendFile := func(b []byte) {
		t.Helper()
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(b); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// 1. A complete appended record is printed.
	rec2 := mustRecordLine(t, logging.CallRecord{Upstream: "github", Method: "tools/call", Tool: "github__two", OK: true})
	appendFile(rec2)
	waitOutputContains(t, out, "github__two", 5*time.Second)

	// 2. A partial record (no trailing newline yet) is NOT printed...
	rec3 := mustRecordLine(t, logging.CallRecord{Upstream: "github", Method: "tools/call", Tool: "github__three", OK: true})
	half := len(rec3) / 2
	appendFile(rec3[:half])
	time.Sleep(50 * time.Millisecond) // several poll ticks at 5ms
	if strings.Contains(out.String(), "github__three") {
		t.Fatalf("partial record leaked into follow output:\n%s", out.String())
	}
	// ...until the rest of it (incl. the newline) lands.
	appendFile(rec3[half:])
	waitOutputContains(t, out, "github__three", 5*time.Second)

	// 3. Truncation (external rotation): follow must reopen from offset 0 and
	// print the new file's content.
	rec4 := mustRecordLine(t, logging.CallRecord{Upstream: "web", Method: "tools/call", Tool: "web__four", OK: true})
	if err := os.WriteFile(path, rec4, 0o600); err != nil { // truncates: new size < old offset
		t.Fatal(err)
	}
	waitOutputContains(t, out, "web__four", 5*time.Second)
}

// TestLogsFollowAppliesFilters: the follow loop must run new records through
// the SAME --upstream/--tool/--status filters as the initial listing.
func TestLogsFollowAppliesFilters(t *testing.T) {
	saved := followPollInterval
	followPollInterval = 5 * time.Millisecond
	defer func() { followPollInterval = saved }()

	dir := t.TempDir()
	path := filepath.Join(dir, "calls.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := &syncWriter{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = followLog(ctx, out, path, 0, nil, recordFilter{upstream: "web"})
	}()
	// Join the follower BEFORE the deferred interval restore above runs: the
	// goroutine reads followPollInterval on every tick (caught by -race).
	defer func() {
		cancel()
		<-done
	}()

	var buf bytes.Buffer
	buf.Write(mustRecordLine(t, logging.CallRecord{Upstream: "github", Method: "tools/call", Tool: "github__skip", OK: true}))
	buf.Write(mustRecordLine(t, logging.CallRecord{Upstream: "web", Method: "tools/call", Tool: "web__keep", OK: true}))
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	waitOutputContains(t, out, "web__keep", 5*time.Second)
	if strings.Contains(out.String(), "github__skip") {
		t.Fatalf("follow printed a record the --upstream filter should have dropped:\n%s", out.String())
	}
}

// mustEventLine renders one operator-event journal line, the way the gateway's
// CallLog.RecordEvent writes it.
func mustEventLine(t *testing.T, e logging.EventRecord) []byte {
	t.Helper()
	e.Kind = logging.KindEvent
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return append(b, '\n')
}

// writeMixedLog writes a journal holding both kinds of line, in this order:
// call(github__search ok), event(upstream_start_failed), call(web__fetch err),
// event(notification_dropped, count 5).
func writeMixedLog(t *testing.T) string {
	t.Helper()
	base := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	buf.Write(mustRecordLine(t, logging.CallRecord{
		Time: base, Upstream: "github", Method: "tools/call", Tool: "github__search", OK: true,
	}))
	buf.Write(mustEventLine(t, logging.EventRecord{
		Time: base.Add(time.Second), Event: logging.EventUpstreamStartFailed,
		Upstream: "broken", Detail: "exec: no such file or directory",
	}))
	buf.Write(mustRecordLine(t, logging.CallRecord{
		Time: base.Add(2 * time.Second), Upstream: "web", Method: "tools/call",
		Tool: "web__fetch", OK: false, Err: "timeout",
	}))
	buf.Write(mustEventLine(t, logging.EventRecord{
		Time: base.Add(3 * time.Second), Event: logging.EventNotificationDropped,
		Subject: "notifications/progress", Detail: "subscriber buffer full", Count: 5,
	}))
	path := filepath.Join(t.TempDir(), "calls.log")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestLogsShowsEventsInline (Stage 18, C1): by default the journal is shown
// whole — calls and events interleaved in file order, events marked EVT. Hiding
// events behind a flag would defeat the point of journaling them.
func TestLogsShowsEventsInline(t *testing.T) {
	out := runLogsCmd(t, "--file", writeMixedLog(t))
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 {
		t.Fatalf("logs printed %d lines, want 4 (2 calls + 2 events):\n%s", len(lines), out)
	}
	if !strings.Contains(lines[0], "github__search") {
		t.Errorf("line 0 = %q, want the first call", lines[0])
	}
	if !strings.Contains(lines[1], "EVT") || !strings.Contains(lines[1], logging.EventUpstreamStartFailed) {
		t.Errorf("line 1 = %q, want the start-failure event marked EVT", lines[1])
	}
	if !strings.Contains(lines[1], "broken") {
		t.Errorf("line 1 = %q, want the failing upstream named", lines[1])
	}
	if !strings.Contains(lines[2], "web__fetch") {
		t.Errorf("line 2 = %q, want the second call (journal order)", lines[2])
	}
	if !strings.Contains(lines[3], logging.EventNotificationDropped) || !strings.Contains(lines[3], "count=5") {
		t.Errorf("line 3 = %q, want the coalesced drop event with its count", lines[3])
	}
}

// TestLogsEventsFlagShowsOnlyEvents (C2).
func TestLogsEventsFlagShowsOnlyEvents(t *testing.T) {
	out := runLogsCmd(t, "--file", writeMixedLog(t), "--events")
	if strings.Contains(out, "github__search") || strings.Contains(out, "web__fetch") {
		t.Errorf("--events leaked call records:\n%s", out)
	}
	for _, want := range []string{logging.EventUpstreamStartFailed, logging.EventNotificationDropped} {
		if !strings.Contains(out, want) {
			t.Errorf("--events output missing %q:\n%s", want, out)
		}
	}
}

// TestLogsToolFilterSuppressesEvents (C3): --tool is a call-only filter, so
// events must not slip through it; the call filtering itself is unchanged.
func TestLogsToolFilterSuppressesEvents(t *testing.T) {
	out := runLogsCmd(t, "--file", writeMixedLog(t), "--tool", "github__search")
	if strings.Contains(out, "EVT") {
		t.Errorf("--tool let operator events through:\n%s", out)
	}
	if !strings.Contains(out, "github__search") {
		t.Errorf("--tool dropped the call it should have matched:\n%s", out)
	}
	if strings.Contains(out, "web__fetch") {
		t.Errorf("--tool leaked a non-matching call:\n%s", out)
	}
}

// TestLogsEventsFlagRejectsCallOnlyFilters pins the mutual exclusions: --events
// with a call-only filter could only ever print nothing.
func TestLogsEventsFlagRejectsCallOnlyFilters(t *testing.T) {
	for _, args := range [][]string{
		{"logs", "--file", "/dev/null", "--events", "--tool", "x"},
		{"logs", "--file", "/dev/null", "--events", "--status", "ok"},
	} {
		root := Build("test")
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs(args)
		if err := root.Execute(); err == nil {
			t.Errorf("%v must fail: --events and call-only filters are mutually exclusive", args)
		}
	}
}

// TestLogsStatsEventsSection (Stage 18, C4): --stats keeps its call table
// unchanged and adds an events table. COUNT sums the COALESCED occurrences, so
// a line with count 5 plus a line with no count is 6 — reading the absent count
// as zero would under-report exactly the floods the throttle exists for.
func TestLogsStatsEventsSection(t *testing.T) {
	base := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	for i := 1; i <= 3; i++ {
		buf.Write(mustRecordLine(t, logging.CallRecord{
			Upstream: "github", Method: "tools/call", Tool: "github__search",
			Duration: time.Duration(i) * time.Millisecond, OK: true,
		}))
	}
	buf.Write(mustEventLine(t, logging.EventRecord{
		Time: base, Event: logging.EventNotificationDropped,
		Subject: "notifications/progress", Count: 5,
	}))
	last := base.Add(time.Minute)
	buf.Write(mustEventLine(t, logging.EventRecord{
		Time: last, Event: logging.EventNotificationDropped,
		Subject: "notifications/progress", // no count: one occurrence
	}))
	path := filepath.Join(t.TempDir(), "calls.log")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runLogsCmd(t, "--file", path, "--stats")
	if !strings.Contains(out, "UPSTREAM") || !strings.Contains(out, "github__search") {
		t.Errorf("the call table is missing from --stats:\n%s", out)
	}
	var eventRow []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.Contains(line, logging.EventNotificationDropped) {
			eventRow = strings.Fields(line)
		}
	}
	if eventRow == nil {
		t.Fatalf("no events row in --stats output:\n%s", out)
	}
	if !strings.Contains(out, "EVENT") || !strings.Contains(out, "SUBJECT") {
		t.Errorf("events table header missing:\n%s", out)
	}
	// event, subject, count, last (upstream is empty for this event, so the
	// tabwriter row has 4 fields).
	if len(eventRow) != 4 {
		t.Fatalf("events row = %v, want 4 fields (event, subject, count, last)", eventRow)
	}
	if eventRow[2] != "6" {
		t.Errorf("events COUNT = %q, want 6 (5 coalesced + 1 uncounted occurrence)", eventRow[2])
	}
	if eventRow[3] != last.Format(time.RFC3339) {
		t.Errorf("events LAST = %q, want the most recent occurrence %q", eventRow[3], last.Format(time.RFC3339))
	}
}

// TestLogsOldFileByteCompatible (C6): on a journal written entirely by a
// pre-Stage-18 gateway (only call lines), both `logs` and `logs --stats` must
// produce exactly what they did before events existed.
func TestLogsOldFileByteCompatible(t *testing.T) {
	path := writeLog(t) // github×2 ok, web×1 err — the pre-existing fixture

	const wantList = "0001-01-01T00:00:00Z  ok    github        tools/call          github__search  0ms\n" +
		"0001-01-01T00:00:00Z  ERR   web           tools/call          web__fetch  0ms  error=\"timeout\"\n" +
		"0001-01-01T00:00:00Z  ok    github        tools/call          github__create_issue  0ms\n"
	if got := runLogsCmd(t, "--file", path); got != wantList {
		t.Errorf("plain listing changed on an events-free journal:\ngot:\n%q\nwant:\n%q", got, wantList)
	}

	const wantStats = "UPSTREAM  TOOL                  COUNT  ERROR%  P50  P95\n" +
		"github    github__create_issue  1      0.0%    0ms  0ms\n" +
		"github    github__search        1      0.0%    0ms  0ms\n" +
		"web       web__fetch            1      100.0%  0ms  0ms\n"
	if got := runLogsCmd(t, "--file", path, "--stats"); got != wantStats {
		t.Errorf("--stats changed on an events-free journal:\ngot:\n%q\nwant:\n%q", got, wantStats)
	}
}

// TestLogsFollowPrintsAppendedEvent (C5): --follow's watch loop must print
// events appended after it started, not only calls.
func TestLogsFollowPrintsAppendedEvent(t *testing.T) {
	saved := followPollInterval
	followPollInterval = 5 * time.Millisecond
	defer func() { followPollInterval = saved }()

	dir := t.TempDir()
	path := filepath.Join(dir, "calls.log")
	rec1 := mustRecordLine(t, logging.CallRecord{Upstream: "github", Method: "tools/call", Tool: "github__one", OK: true})
	if err := os.WriteFile(path, rec1, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := &syncWriter{}
	done := make(chan error, 1)
	go func() {
		done <- followLog(ctx, out, path, int64(len(rec1)), nil, recordFilter{})
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("followLog did not return after context cancellation")
		}
	}()

	ev := mustEventLine(t, logging.EventRecord{
		Time: time.Now(), Event: logging.EventUpstreamGaveUp, Upstream: "doomed",
		Detail: "exhausted restart attempts (max_attempts=2)",
	})
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(ev); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	waitOutputContains(t, out, logging.EventUpstreamGaveUp, 5*time.Second)
	if !strings.Contains(out.String(), "EVT") {
		t.Errorf("appended event was not rendered as an EVT line:\n%s", out.String())
	}
}

// TestLogsEscapesUpstreamControlledSubject (review M3): an event's Subject is
// not gateway-authored — for catalog_collision it is a resource URI and for
// catalog_bad_template a URI template, both taken verbatim from an upstream and,
// unlike a tool name, never normalized by namespacing. A "\n" in one must not
// forge a second output line that an operator cannot tell from a real journal
// entry (nor an ESC repaint their terminal).
func TestLogsEscapesUpstreamControlledSubject(t *testing.T) {
	forged := "file:///a\n2026-07-29T10:00:00Z  EVT   ghost         forged_event      nothing-happened"
	var buf bytes.Buffer
	buf.Write(mustEventLine(t, logging.EventRecord{
		Time:  time.Date(2026, 7, 29, 9, 0, 0, 0, time.UTC),
		Event: logging.EventCatalogCollision, Upstream: "evil",
		Subject: forged + "\x1b[31m",
	}))
	path := filepath.Join(t.TempDir(), "calls.log")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	out := runLogsCmd(t, "--file", path)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 1 {
		t.Fatalf("one journal event printed %d lines — the subject forged extra ones:\n%s", len(lines), out)
	}
	if strings.Contains(out, "\x1b") {
		t.Errorf("a raw ESC reached the terminal:\n%q", out)
	}
	if !strings.Contains(out, `\n`) {
		t.Errorf("the newline was not escaped, so nothing is pinned here:\n%s", out)
	}
	if !strings.Contains(out, "file:///a") {
		t.Errorf("the subject itself is gone from the line:\n%s", out)
	}
}

// TestLogsStatsEventsOnlyReportsEmptiness (review S3): `--events --stats` on a
// journal with no events used to print ABSOLUTELY nothing — the call table is
// skipped by --events and the event table has no rows — which is
// indistinguishable from a command that broke silently.
func TestLogsStatsEventsOnlyReportsEmptiness(t *testing.T) {
	out := runLogsCmd(t, "--file", writeLog(t), "--events", "--stats")
	if strings.TrimSpace(out) == "" {
		t.Fatal("--events --stats on an event-free journal printed nothing at all")
	}
	if !strings.Contains(out, "no events") {
		t.Errorf("--events --stats output = %q, want it to say there are no events", out)
	}
}
