package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestCheckFilePermsWarnsOnGroupOtherReadable (security brainstorm
// 2026-08-20, S7): a file readable by group or others (e.g. mode 0644, the
// common default umask result) is flagged — config.yaml MAY hold a secret
// literal, --env-file always exists to hold secrets, and nothing else in the
// gateway ever looks at their mode.
func TestCheckFilePermsWarnsOnGroupOtherReadable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows — checkFilePerms skips the whole platform by design, see its doc comment")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte("transport: stdio\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)

	checkFilePerms(cmd, p)

	s := out.String()
	if !strings.Contains(s, "WARN") || !strings.Contains(s, p) {
		t.Errorf("readable-by-group/others file must warn, naming the path, got:\n%s", s)
	}
	if !strings.Contains(s, "0644") {
		t.Errorf("warning should name the actual mode for the operator to act on, got:\n%s", s)
	}
}

// TestCheckFilePermsNoWarnOnStrictPerms: mode 0600 (owner-only) — no warning.
func TestCheckFilePermsNoWarnOnStrictPerms(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	dir := t.TempDir()
	p := filepath.Join(dir, ".env")
	if err := os.WriteFile(p, []byte("TOKEN=x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)

	checkFilePerms(cmd, p)

	if s := out.String(); s != "" {
		t.Errorf("owner-only file (0600) must not warn, got:\n%s", s)
	}
}

// TestCheckFilePermsSkipsEmptyAndMissingPaths: an empty path (the flag was
// not given) and a nonexistent path must be silently skipped, never panic —
// a missing/unreadable file is a DIFFERENT problem doctor already surfaces
// elsewhere (loadConfig's own error), not this check's job.
func TestCheckFilePermsSkipsEmptyAndMissingPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)

	checkFilePerms(cmd, "", filepath.Join(t.TempDir(), "does-not-exist.yaml"))

	if s := out.String(); s != "" {
		t.Errorf("empty and missing paths must produce no output, got:\n%s", s)
	}
}

// TestCheckFilePermsMultiplePaths: two paths at once (config.yaml AND
// --env-file, doctor's real call shape) — only the loose one warns, both
// named correctly, independent of argument order.
func TestCheckFilePermsMultiplePaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	dir := t.TempDir()
	loose := filepath.Join(dir, "config.yaml")
	strict := filepath.Join(dir, ".env")
	if err := os.WriteFile(loose, []byte("transport: stdio\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(strict, []byte("TOKEN=x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)

	checkFilePerms(cmd, loose, strict)

	s := out.String()
	if !strings.Contains(s, loose) {
		t.Errorf("the loose file must be named in the warning, got:\n%s", s)
	}
	if strings.Contains(s, strict) {
		t.Errorf("the strict file must NOT appear in any warning, got:\n%s", s)
	}
	if strings.Count(s, "WARN") != 1 {
		t.Errorf("want exactly one WARN line (only the loose file), got:\n%s", s)
	}
}

// TestCheckFilePermsSkipsDirectory: a path that resolves to a directory
// (e.g. --env-file misconfigured to a directory) must never warn — the
// "consider chmod 600" wording only makes sense for a file, and a typical
// directory mode (0755) would otherwise trip the same group/other check.
func TestCheckFilePermsSkipsDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)

	checkFilePerms(cmd, dir)

	if s := out.String(); s != "" {
		t.Errorf("a directory must never warn, got:\n%s", s)
	}
}
