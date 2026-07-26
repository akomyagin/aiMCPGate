package cli

import (
	"strings"
	"testing"
)

// TestCompletionGeneratesScripts is the Round 10 smoke test for shell
// autocompletion: cobra's built-in `completion` command must be reachable
// (nothing in root.go disables it) and must emit a non-empty script that
// mentions the binary name for every supported shell. The same invocations
// feed the goreleaser before-hooks that pack completions into release archives.
func TestCompletionGeneratesScripts(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		t.Run(shell, func(t *testing.T) {
			out, err := execRoot(t, "completion", shell)
			if err != nil {
				t.Fatalf("completion %s failed: %v", shell, err)
			}
			if strings.TrimSpace(out) == "" {
				t.Fatalf("completion %s produced no output", shell)
			}
			if !strings.Contains(out, "mcp-gate") {
				t.Errorf("completion %s script does not mention the mcp-gate binary:\n%.200s", shell, out)
			}
		})
	}
}
