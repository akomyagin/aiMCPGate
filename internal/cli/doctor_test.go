package cli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildFakeServer compiles the shared internal/upstream/testdata/fakeserver
// binary and returns its path. Duplicated (not imported) from the registry
// package's test helper for the same reason it duplicates upstream's: testdata
// helpers are not importable across packages.
func buildFakeServer(t *testing.T) string {
	t.Helper()
	src := filepath.Join("..", "upstream", "testdata", "fakeserver")
	bin := filepath.Join(t.TempDir(), "fakeserver")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = src
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fakeserver: %v\n%s", err, out)
	}
	return bin
}

// writeDoctorConfig writes a gateway config for doctor tests and returns its
// path. log_level=error keeps the expected per-upstream failure warnings out
// of the test output.
func writeDoctorConfig(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// doctorRow returns the report line for one upstream, failing the test if it
// is absent — asserting on whole rows keeps the checks readable.
func doctorRow(t *testing.T, out, upstream string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), upstream) {
			return line
		}
	}
	t.Fatalf("no report row for upstream %q in output:\n%s", upstream, out)
	return ""
}

// TestDoctorReportsFailAndExitsNonZero is the Stage 8 acceptance test: one
// healthy fake upstream and one with a nonexistent command. doctor must print
// OK (with the tool count) for the first, FAIL (with the reason) for the
// second, and return an error so main exits non-zero. The restart policy is
// deliberately enabled with a 30s backoff: doctor must finish in ONE pass —
// if it (wrongly) supervised and retried with backoff, the deadline below
// would blow up.
func TestDoctorReportsFailAndExitsNonZero(t *testing.T) {
	bin := buildFakeServer(t)
	cfgPath := writeDoctorConfig(t, `
transport: stdio
log_level: error
restart:
  enabled: true
  initial_backoff: 30s
upstreams:
  - name: good
    command: `+bin+`
    enabled: true
    env:
      FAKE_TOOLS: "ping,pong"
  - name: broken
    command: /nonexistent/mcp-gate-doctor-test-missing
    enabled: true
`)

	root := Build("test")
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"doctor", "-c", cfgPath})

	started := time.Now()
	err := root.Execute()
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("doctor must return an error (non-zero exit) when an upstream FAILs")
	}
	if !strings.Contains(err.Error(), "1 of 2 upstream(s) failed") {
		t.Errorf("error = %q, want the failed/total summary", err)
	}
	// One pass, no backoff retries: with initial_backoff=30s any retry loop
	// would take far longer than this generous single-pass budget.
	if elapsed > 10*time.Second {
		t.Errorf("doctor took %s; it must finish in one pass without backoff retries", elapsed)
	}

	goodRow := doctorRow(t, out.String(), "good")
	if !strings.Contains(goodRow, "OK") || !strings.Contains(goodRow, "2") {
		t.Errorf("good row = %q, want OK with 2 tools", goodRow)
	}
	brokenRow := doctorRow(t, out.String(), "broken")
	if !strings.Contains(brokenRow, "FAIL") || !strings.Contains(brokenRow, "not found") {
		t.Errorf("broken row = %q, want FAIL with the command-not-found reason", brokenRow)
	}
}

// TestDoctorAllOKExitsZero: with every upstream healthy, doctor prints the OK
// table and returns nil (exit code 0) — the scriptable success case.
func TestDoctorAllOKExitsZero(t *testing.T) {
	bin := buildFakeServer(t)
	cfgPath := writeDoctorConfig(t, `
transport: stdio
log_level: error
upstreams:
  - name: solo
    command: `+bin+`
    enabled: true
    env:
      FAKE_TOOLS: "ping"
`)

	root := Build("test")
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"doctor", "-c", cfgPath})

	if err := root.Execute(); err != nil {
		t.Fatalf("doctor with all upstreams healthy: %v\noutput:\n%s", err, out.String())
	}
	row := doctorRow(t, out.String(), "solo")
	if !strings.Contains(row, "OK") {
		t.Errorf("solo row = %q, want OK", row)
	}
}

// TestDoctorAllFailedStillPrintsReport: when EVERY upstream fails, Start
// itself errors — doctor must still print the full FAIL table (its whole
// point) rather than bail with the bare Start error.
func TestDoctorAllFailedStillPrintsReport(t *testing.T) {
	cfgPath := writeDoctorConfig(t, `
transport: stdio
log_level: error
upstreams:
  - name: broken
    command: /nonexistent/mcp-gate-doctor-test-missing
    enabled: true
`)

	root := Build("test")
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"doctor", "-c", cfgPath})

	if err := root.Execute(); err == nil {
		t.Fatal("doctor must return an error when every upstream fails")
	}
	row := doctorRow(t, out.String(), "broken")
	if !strings.Contains(row, "FAIL") || !strings.Contains(row, "not found") {
		t.Errorf("broken row = %q, want FAIL with the command-not-found reason", row)
	}
}
