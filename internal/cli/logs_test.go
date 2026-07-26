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
