package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
