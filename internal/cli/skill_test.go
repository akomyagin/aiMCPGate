package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSkillCommandPrintsFrontmatterAndBody(t *testing.T) {
	root := Build("test")
	buf := &bytes.Buffer{}
	root.SetOut(buf)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"skill"})

	if err := root.Execute(); err != nil {
		t.Fatalf("skill: %v", err)
	}
	out := buf.String()

	if !strings.HasPrefix(out, "---\nname: mcp-gate\n") {
		t.Errorf("skill output missing expected frontmatter, got prefix %q", out[:min(40, len(out))])
	}
	// Must not hardcode any of this deployment's specific upstream names —
	// the whole point is that it stays valid across different configs.
	for _, forbidden := range []string{"gitlab_token", "atlassian_mcp_token", "127.0.0.1:28080"} {
		if strings.Contains(strings.ToLower(out), forbidden) {
			t.Errorf("skill output should be deployment-independent, found %q", forbidden)
		}
	}
	if !strings.Contains(out, "tools/list") {
		t.Error("skill output should teach live discovery via tools/list")
	}
}

func TestSkillCommandUsesConfiguredOverride(t *testing.T) {
	dir := t.TempDir()
	skillPath := filepath.Join(dir, "custom-skill.md")
	if err := os.WriteFile(skillPath, []byte("custom org policy\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgYAML := "transport: stdio\nskill_file: " + skillPath + "\n"
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	root := Build("test")
	buf := &bytes.Buffer{}
	root.SetOut(buf)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"skill", "--config", cfgPath})

	if err := root.Execute(); err != nil {
		t.Fatalf("skill --config: %v", err)
	}
	if got := buf.String(); got != "custom org policy\n" {
		t.Errorf("skill with skill_file override = %q, want the custom file's content", got)
	}
}
