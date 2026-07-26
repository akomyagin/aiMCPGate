package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeEnvFile writes content to a temp .env file and returns its path.
func writeEnvFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}
	return path
}

// unsetAfter guarantees the process environment is clean of key both before
// the test body runs and after it finishes — applyEnvFile writes the REAL
// process environment, so leaks would poison later tests.
func unsetAfter(t *testing.T, key string) {
	t.Helper()
	os.Unsetenv(key)
	t.Cleanup(func() { os.Unsetenv(key) })
}

// TestApplyEnvFileParsesAndSets covers the whole supported syntax in one file:
// comments, blank lines, the `export ` prefix, single/double quotes, spaces
// around KEY, and a value containing '=' (split on the FIRST '=' only).
func TestApplyEnvFileParsesAndSets(t *testing.T) {
	keys := []string{
		"MCP_GATE_ENVTEST_PLAIN", "MCP_GATE_ENVTEST_EXPORT", "MCP_GATE_ENVTEST_DQ",
		"MCP_GATE_ENVTEST_SQ", "MCP_GATE_ENVTEST_EQ", "MCP_GATE_ENVTEST_SPACES",
		"MCP_GATE_ENVTEST_MISMATCH", "MCP_GATE_ENVTEST_EMPTY",
		"MCP_GATE_ENVTEST_EXPORT_TAB", "export", "exportNOT",
	}
	for _, k := range keys {
		unsetAfter(t, k)
	}

	path := writeEnvFile(t, strings.Join([]string{
		"# a comment line",
		"",
		"   # an indented comment",
		"MCP_GATE_ENVTEST_PLAIN=plain-value",
		"export MCP_GATE_ENVTEST_EXPORT=exported",
		"export\tMCP_GATE_ENVTEST_EXPORT_TAB=tab-exported", // tab after export (review fix)
		"export=literal-export-key",                        // NOT the export form: the key IS "export"
		"exportNOT=glued",                                  // no whitespace: the key keeps its prefix
		`MCP_GATE_ENVTEST_DQ="double quoted"`,
		"MCP_GATE_ENVTEST_SQ='single quoted'",
		"MCP_GATE_ENVTEST_EQ=key=value=chain",
		"  MCP_GATE_ENVTEST_SPACES = padded ",
		`MCP_GATE_ENVTEST_MISMATCH='mismatched"`,
		"MCP_GATE_ENVTEST_EMPTY=",
	}, "\n"))

	if err := applyEnvFile(path); err != nil {
		t.Fatalf("applyEnvFile: %v", err)
	}

	want := map[string]string{
		"MCP_GATE_ENVTEST_PLAIN":      "plain-value",
		"MCP_GATE_ENVTEST_EXPORT":     "exported",
		"MCP_GATE_ENVTEST_EXPORT_TAB": "tab-exported",
		"export":                      "literal-export-key",
		"exportNOT":                   "glued",
		"MCP_GATE_ENVTEST_DQ":         "double quoted",
		"MCP_GATE_ENVTEST_SQ":         "single quoted",
		"MCP_GATE_ENVTEST_EQ":         "key=value=chain",
		"MCP_GATE_ENVTEST_SPACES":     "padded",
		// Mismatched quotes must be kept verbatim, not half-eaten.
		"MCP_GATE_ENVTEST_MISMATCH": `'mismatched"`,
		"MCP_GATE_ENVTEST_EMPTY":    "",
	}
	for key, wantVal := range want {
		got, ok := os.LookupEnv(key)
		if !ok {
			t.Errorf("%s was not set", key)
			continue
		}
		if got != wantVal {
			t.Errorf("%s = %q, want %q", key, got, wantVal)
		}
	}
}

// TestApplyEnvFileRealEnvWins is the priority contract: a variable already
// present in the real process environment must NOT be overwritten by the file
// (an operator's explicit VAR=... or a CI secret outranks a stale local .env).
func TestApplyEnvFileRealEnvWins(t *testing.T) {
	t.Setenv("MCP_GATE_ENVTEST_PRIORITY", "from-real-env")
	path := writeEnvFile(t, "MCP_GATE_ENVTEST_PRIORITY=from-file\n")

	if err := applyEnvFile(path); err != nil {
		t.Fatalf("applyEnvFile: %v", err)
	}
	if got := os.Getenv("MCP_GATE_ENVTEST_PRIORITY"); got != "from-real-env" {
		t.Errorf("value = %q, want the real environment to win over the file", got)
	}
}

// TestApplyEnvFileEmptyPathIsNoop: no flag given → nothing read, no error.
func TestApplyEnvFileEmptyPathIsNoop(t *testing.T) {
	if err := applyEnvFile(""); err != nil {
		t.Fatalf("applyEnvFile(\"\") = %v, want nil", err)
	}
}

// TestDoctorEnvFileFlagFeedsConfigExpansion is the end-to-end contract of the
// --env-file flag: the file is applied to the process environment BEFORE the
// config is loaded, so a config carrying "${VAR}" in an upstream env value
// resolves against it. The fake upstream reads FAKE_TOOLS from its (expanded)
// env, so the doctor row proves the whole chain worked.
func TestDoctorEnvFileFlagFeedsConfigExpansion(t *testing.T) {
	unsetAfter(t, "MCP_GATE_ENVTEST_TOOLS")
	bin := buildFakeServer(t)
	envPath := writeEnvFile(t, `MCP_GATE_ENVTEST_TOOLS="ping,pong"`+"\n")
	cfgPath := writeDoctorConfig(t, `
transport: stdio
log_level: error
upstreams:
  - name: solo
    command: `+bin+`
    enabled: true
    env:
      FAKE_TOOLS: "${MCP_GATE_ENVTEST_TOOLS}"
`)

	out, err := execRoot(t, "doctor", "-c", cfgPath, "--env-file", envPath)
	if err != nil {
		t.Fatalf("doctor with --env-file failed: %v\noutput:\n%s", err, out)
	}
	row := doctorRow(t, out, "solo")
	if !strings.Contains(row, "OK") || !strings.Contains(row, "2") {
		t.Errorf("solo row = %q, want OK with the 2 tools named in the env file", row)
	}
}

// TestApplyEnvFileErrors: unreadable files and malformed lines must fail
// loudly (with the line number), not silently half-apply a config's secrets.
func TestApplyEnvFileErrors(t *testing.T) {
	unsetAfter(t, "MCP_GATE_ENVTEST_OK")

	tests := []struct {
		name    string
		path    string
		wantSub string
	}{
		{
			name:    "missing file",
			path:    filepath.Join(t.TempDir(), "nope", ".env"),
			wantSub: "read env file",
		},
		{
			name:    "line without equals",
			path:    writeEnvFile(t, "MCP_GATE_ENVTEST_OK=fine\nJUSTAWORD\n"),
			wantSub: "line 2",
		},
		{
			name:    "empty key",
			path:    writeEnvFile(t, "=value-without-key\n"),
			wantSub: "empty key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := applyEnvFile(tt.path)
			if err == nil {
				t.Fatal("applyEnvFile succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantSub)
			}
		})
	}
}
