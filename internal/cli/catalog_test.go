package cli

import (
	"strconv"
	"strings"
	"testing"
)

// TestCatalogReportsSizesPerUpstream is the Round 10 acceptance test for
// `mcp-gate catalog` against the demo-echo stub: the per-upstream table must
// show demo-echo with its 2 tools and consistent byte/approx-token numbers,
// the --top section must list the heaviest tool, and the TOTAL line must say
// explicitly that the token count is approximate.
func TestCatalogReportsSizesPerUpstream(t *testing.T) {
	bin := buildGateBinary(t)
	cfgPath := writeDemoEchoConfig(t, bin)

	out, err := execRoot(t, "catalog", "-c", cfgPath, "--top", "1")
	if err != nil {
		t.Fatalf("catalog failed: %v\noutput:\n%s", err, out)
	}

	row := doctorRow(t, out, "demo-echo")
	fields := strings.Fields(row)
	if len(fields) != 4 {
		t.Fatalf("demo-echo row = %q, want 4 columns (UPSTREAM TOOLS BYTES ~TOKENS)", row)
	}
	if fields[1] != "2" {
		t.Errorf("demo-echo tool count = %s, want 2 (echo + ping)", fields[1])
	}
	bytes, err1 := strconv.Atoi(fields[2])
	tokens, err2 := strconv.Atoi(fields[3])
	if err1 != nil || err2 != nil {
		t.Fatalf("demo-echo row = %q, BYTES/~TOKENS are not numbers", row)
	}
	if bytes <= 0 || tokens != bytes/4 {
		t.Errorf("bytes = %d, tokens = %d, want positive bytes and tokens == bytes/4", bytes, tokens)
	}

	if !strings.Contains(out, "Top 1 heaviest tools:") {
		t.Errorf("output is missing the --top section:\n%s", out)
	}
	// echo carries the bigger inputSchema, so it must win the top-1 slot.
	if !strings.Contains(out, "demo-echo__echo") {
		t.Errorf("top section is missing the heaviest tool demo-echo__echo:\n%s", out)
	}
	if strings.Count(out, "demo-echo__") != 1 {
		t.Errorf("top 1 must list exactly one tool:\n%s", out)
	}

	if !strings.Contains(out, "TOTAL: 2 tool(s)") {
		t.Errorf("output is missing the catalog total:\n%s", out)
	}
	if !strings.Contains(out, "approximate") {
		t.Errorf("output must state that token counts are approximate:\n%s", out)
	}
}

// TestCatalogTopZeroDisablesSection: --top 0 must skip the heaviest-tools
// section entirely while keeping the per-upstream table and the total.
func TestCatalogTopZeroDisablesSection(t *testing.T) {
	bin := buildGateBinary(t)
	cfgPath := writeDemoEchoConfig(t, bin)

	out, err := execRoot(t, "catalog", "-c", cfgPath, "--top", "0")
	if err != nil {
		t.Fatalf("catalog failed: %v\noutput:\n%s", err, out)
	}
	if strings.Contains(out, "heaviest tools") {
		t.Errorf("--top 0 must disable the heaviest-tools section:\n%s", out)
	}
	if !strings.Contains(out, "UPSTREAM") || !strings.Contains(out, "TOTAL: 2 tool(s)") {
		t.Errorf("table or total missing:\n%s", out)
	}
}
