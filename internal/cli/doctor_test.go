package cli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/registry"
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

// TestDoctorUpstreamWithoutEnabledKey is the end-to-end shape of the bug the
// *bool Enabled fixes: a config an operator hand-wrote and never spelled
// `enabled: true` in. It used to yield an empty table and "no upstream is
// enabled" with the entry silently dropped; now it must be checked like any
// other upstream and report OK.
func TestDoctorUpstreamWithoutEnabledKey(t *testing.T) {
	bin := buildFakeServer(t)
	cfgPath := writeDoctorConfig(t, `
transport: stdio
log_level: error
upstreams:
  - name: implicit
    command: `+bin+`
    env:
      FAKE_TOOLS: "ping"
`)

	root := Build("test")
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"doctor", "-c", cfgPath})

	if err := root.Execute(); err != nil {
		t.Fatalf("doctor must succeed for an upstream that omits `enabled:`: %v\noutput:\n%s", err, out.String())
	}
	row := doctorRow(t, out.String(), "implicit")
	if !strings.Contains(row, "OK") {
		t.Errorf("implicit row = %q, want OK", row)
	}
}

// TestPrintDoctorReportSanitizesReason (regression, found by code review): the
// failure reason comes from an arbitrary err.Error() — possibly text relayed
// verbatim from the upstream — so it can contain tabs and newlines. Raw, a tab
// opens a phantom tabwriter column and a newline orphans the tail of the reason
// onto its own column-less line, wrecking the alignment of every row AFTER the
// broken one too. The report must stay one line per upstream, all aligned.
func TestPrintDoctorReportSanitizesReason(t *testing.T) {
	report := []registry.UpstreamStatus{
		{Name: "broken", Err: "handshake failed: upstream reported error\twith\ttabs\nand a newline"},
		{Name: "next-row", OK: true, Tools: 5},
	}

	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	printDoctorReport(cmd, report)

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("table has %d lines, want 3 (header + 2 rows) — a newline in the reason leaked through:\n%s", len(lines), out.String())
	}
	brokenRow := doctorRow(t, out.String(), "broken")
	if !strings.Contains(brokenRow, "and a newline") {
		t.Errorf("broken row = %q, want the whole reason kept on one line", brokenRow)
	}
	// tabwriter aligns columns across the whole block, so STATUS/FAIL/OK must
	// start at the same offset; a leaked tab or newline breaks the block and
	// shifts the rows that follow.
	header, nextRow := lines[0], doctorRow(t, out.String(), "next-row")
	if col := strings.Index(header, "STATUS"); strings.Index(brokenRow, "FAIL") != col || strings.Index(nextRow, "OK") != col {
		t.Errorf("STATUS column misaligned:\n%s", out.String())
	}
}

// TestDoctorNoEnabledUpstreamsExitsNonZero: with nothing enabled there is no
// row to print FAIL for, so the table alone would look deceptively fine —
// doctor must still exit non-zero and say WHY nothing was attempted.
func TestDoctorNoEnabledUpstreamsExitsNonZero(t *testing.T) {
	cfgPath := writeDoctorConfig(t, `
transport: stdio
log_level: error
upstreams:
  - name: disabled-one
    command: /bin/true
    enabled: false
`)

	root := Build("test")
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"doctor", "-c", cfgPath})

	err := root.Execute()
	if err == nil {
		t.Fatal("doctor must return an error when no upstream is enabled")
	}
	if !strings.Contains(err.Error(), "no upstream is enabled") {
		t.Errorf("error = %q, want the nothing-was-attempted explanation", err)
	}
	// The header must still print (the command ran, the table is just empty),
	// with no phantom data rows.
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "UPSTREAM") {
		t.Errorf("output = %q, want exactly the header line and nothing else", out.String())
	}
}

// TestCLIPathsDeclareNoClientCapabilities pins the negative half of the
// truthful upstream handshake for the CLI commands: doctor, call and catalog
// load the same config a serving gateway would — transport: stdio and all —
// but have NO MCP client behind them, so nobody could ever answer an
// upstream's server→client request and their handshakes must declare exactly
// {}. This is precisely what deriving the declaration from config.Transport
// (instead of it following the client-facing transport's explicit
// SetClientServerRequestCaps call — Stage 15) would break: every one of these
// paths would start promising capabilities it cannot serve, and only the raw
// FAKE_CAPS_FILE record of the initialize the upstream received catches it.
func TestCLIPathsDeclareNoClientCapabilities(t *testing.T) {
	bin := buildFakeServer(t)
	tests := []struct {
		name string
		args func(cfgPath string) []string
	}{
		{"doctor", func(cfg string) []string { return []string{"doctor", "-c", cfg} }},
		// call actually performs a tools/call — the path where a lying
		// declaration would provoke a real elicitation with nobody to answer.
		{"call", func(cfg string) []string { return []string{"call", "solo__ping", "{}", "-c", cfg} }},
		{"catalog", func(cfg string) []string { return []string{"catalog", "-c", cfg} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capsFile := filepath.Join(t.TempDir(), "caps")
			cfgPath := writeDoctorConfig(t, `
transport: stdio
log_level: error
upstreams:
  - name: solo
    command: `+bin+`
    enabled: true
    env:
      FAKE_TOOLS: "ping"
      FAKE_CAPS_FILE: `+capsFile+`
`)
			out, err := execRoot(t, tt.args(cfgPath)...)
			if err != nil {
				t.Fatalf("%s failed: %v\noutput:\n%s", tt.name, err, out)
			}
			data, err := os.ReadFile(capsFile)
			if err != nil {
				t.Fatalf("read caps file (no handshake recorded?): %v", err)
			}
			if got := strings.TrimSpace(string(data)); got != "{}" {
				t.Errorf("%s declared client capabilities %s to its upstream, want exactly {} (no MCP client to answer)", tt.name, got)
			}
		})
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

// D1. TestDoctorPrintsUnresolvedSecretVars: a config where an ENABLED upstream
// carries an unresolved env ref and a DISABLED one carries its own. doctor's
// stdout must show BOTH WARN lines — naming the variables, never a value —
// with the disabled one marked "(upstream disabled)". The exit code is still
// driven only by the OK/FAIL table (the enabled upstream is healthy → nil).
func TestDoctorPrintsUnresolvedSecretVars(t *testing.T) {
	bin := buildFakeServer(t)
	cfgPath := writeDoctorConfig(t, `
transport: stdio
log_level: error
upstreams:
  - name: gh
    command: `+bin+`
    enabled: true
    env:
      FAKE_TOOLS: "ping"
      GITHUB_TOKEN: ${TEST_AIMCPGATE_UNSET_GH}
  - name: old
    command: `+bin+`
    enabled: false
    headers:
      Authorization: ${TEST_AIMCPGATE_UNSET_OLD}
`)

	root := Build("test")
	out := &bytes.Buffer{}
	root.SetOut(out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"doctor", "-c", cfgPath})

	if err := root.Execute(); err != nil {
		t.Fatalf("doctor exit code must be driven by the table (enabled upstream healthy → nil), got: %v\n%s", err, out.String())
	}
	s := out.String()
	if !strings.Contains(s, "TEST_AIMCPGATE_UNSET_GH") {
		t.Errorf("stdout must name the enabled upstream's unresolved variable:\n%s", s)
	}
	if !strings.Contains(s, "TEST_AIMCPGATE_UNSET_OLD") {
		t.Errorf("stdout must name the disabled upstream's unresolved variable:\n%s", s)
	}
	// The disabled upstream's warning must carry the marker; the enabled one
	// must NOT. Find the two lines and check each.
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, "TEST_AIMCPGATE_UNSET_OLD") && !strings.Contains(line, "(upstream disabled)") {
			t.Errorf("disabled upstream's WARN line must carry the marker:\n%s", line)
		}
		if strings.Contains(line, "TEST_AIMCPGATE_UNSET_GH") && strings.Contains(line, "(upstream disabled)") {
			t.Errorf("enabled upstream's WARN line must NOT carry the disabled marker:\n%s", line)
		}
	}
}
