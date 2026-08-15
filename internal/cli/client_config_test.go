package cli

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// chimeraPath is the file `client-config` used to name as Claude Code's config
// until fix/client-config: it exists under no installation of either product —
// it is Claude Code's directory glued to Claude Desktop's file name. Pinned as
// a literal so a future "restore the old header" cannot pass silently.
const chimeraPath = "~/.claude/claude_desktop_config.json"

// execRootStreams runs one in-process `mcp-gate <args...>` keeping stdout and
// stderr APART: which stream a line lands on is part of this command's
// contract (the secret warning must not pollute piped JSON), so the shared
// execRoot helper, which drops stderr, cannot express these assertions.
func execRootStreams(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	root := Build("test")
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.SetArgs(args)
	err := root.Execute()
	return stdout.String(), stderr.String(), err
}

// writeClientConfig writes a gateway config whose only upstream is inert (it is
// never started by client-config — the command reads the file, it does not
// connect), so these tests stay free of processes and ports.
func writeClientConfig(t *testing.T, extra string) string {
	t.Helper()
	return writeDoctorConfig(t, extra+`
log_level: error
upstreams:
  - name: demo
    kind: stdio
    command: /bin/true
    enabled: true
`)
}

// TestClientConfigStdioPrintsLaunchSnippet is the acceptance test for DEFECT 2:
// stdio is the gateway's default transport, and the command used to REFUSE to
// run there. It must succeed and print how to launch the gateway: an absolute
// binary path (the client spawns the process without the operator's PATH) and
// the absolute config path it was pointed at.
func TestClientConfigStdioPrintsLaunchSnippet(t *testing.T) {
	cfgPath := writeClientConfig(t, "transport: stdio")

	out, errOut, err := execRootStreams(t, "client-config", "-c", cfgPath)
	if err != nil {
		t.Fatalf("client-config on a stdio config failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
	}
	if !strings.Contains(out, `"serve"`) {
		t.Errorf("stdio snippet does not launch the gateway (no serve arg):\n%s", out)
	}
	// In-process the running binary IS the test binary; asserting on it keeps
	// the check honest about os.Executable being used rather than a literal.
	exe, exeErr := os.Executable()
	if exeErr != nil {
		t.Fatalf("os.Executable: %v", exeErr)
	}
	if !strings.Contains(out, exe) {
		t.Errorf("stdio snippet must name the absolute binary path %q:\n%s", exe, out)
	}
	if !strings.Contains(out, cfgPath) {
		t.Errorf("stdio snippet must name the absolute config path %q:\n%s", cfgPath, out)
	}
	if !strings.Contains(out, "claude mcp add mcp-gate -- ") {
		t.Errorf("stdio output must offer the `claude mcp add` form:\n%s", out)
	}
}

// TestClientConfigStdioRelativeConfigIsMadeAbsolute: a relative --config is the
// operator's cwd talking; the client spawns the gateway from its own cwd, so
// the snippet must carry the resolved path.
func TestClientConfigStdioRelativeConfigIsMadeAbsolute(t *testing.T) {
	args := gatewayLaunchArgs("config.yaml")
	if len(args) != 3 || args[0] != "serve" || args[1] != "-c" {
		t.Fatalf("gatewayLaunchArgs(relative) = %q, want [serve -c <abs>]", args)
	}
	if !filepath.IsAbs(args[2]) {
		t.Errorf("config path in args = %q, want an absolute path", args[2])
	}
}

// TestClientConfigStdioWithoutConfigFlagOmitsPath covers the branch that cannot
// be reached in-process: with no --config the gateway finds config.yaml next to
// its OWN binary, so the snippet must stay path-free (`serve`, no `-c`) and
// keep working wherever the client spawns it. Exercised on the real binary with
// a real neighbouring config, since that lookup is what the branch relies on.
func TestClientConfigStdioWithoutConfigFlagOmitsPath(t *testing.T) {
	bin := buildGateBinary(t)
	neighbour := filepath.Join(filepath.Dir(bin), "config.yaml")
	body := "transport: stdio\nlog_level: error\nupstreams:\n  - name: demo\n    kind: stdio\n    command: /bin/true\n    enabled: true\n"
	if err := os.WriteFile(neighbour, []byte(body), 0o600); err != nil {
		t.Fatalf("write neighbouring config: %v", err)
	}

	cmd := exec.Command(bin, "client-config")
	cmd.Dir = t.TempDir() // a cwd with no config in it, like a client's
	raw, err := cmd.CombinedOutput()
	out := string(raw)
	if err != nil {
		t.Fatalf("client-config without --config failed: %v\noutput:\n%s", err, out)
	}
	if strings.Contains(out, `"-c"`) || strings.Contains(out, " -c ") {
		t.Errorf("snippet must not pin a config path when --config was not given:\n%s", out)
	}
	if !strings.Contains(out, `"serve"`) {
		t.Errorf("snippet still has to run `serve`:\n%s", out)
	}
	if !strings.Contains(out, bin) {
		t.Errorf("snippet must name the installed binary %q:\n%s", bin, out)
	}
}

// TestClientConfigHTTPPrintsURLAndBearer keeps the pre-existing HTTP contract:
// the endpoint URL and, when auth_token is set, the Authorization header.
func TestClientConfigHTTPPrintsURLAndBearer(t *testing.T) {
	cfgPath := writeClientConfig(t, "transport: http\nlisten_addr: \"127.0.0.1:29099\"\nauth_token: s3cr3t-token")

	out, _, err := execRootStreams(t, "client-config", "-c", cfgPath)
	if err != nil {
		t.Fatalf("client-config on an http config failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "http://127.0.0.1:29099/mcp") {
		t.Errorf("http output is missing the endpoint URL:\n%s", out)
	}
	// Both carriers of the token are pinned separately: a check for the bare
	// "Bearer <token>" substring is satisfied by the `claude mcp add --header`
	// line alone and stays green while the JSON entry silently loses its
	// headers (verified by mutation — that is exactly what happened).
	if !strings.Contains(out, `"Authorization": "Bearer s3cr3t-token"`) {
		t.Errorf("the JSON entry is missing the Authorization header for auth_token:\n%s", out)
	}
	if !strings.Contains(out, `--header "Authorization: Bearer s3cr3t-token"`) {
		t.Errorf("the `claude mcp add` line is missing the Authorization header:\n%s", out)
	}
	// Desktop speaks stdio only: an http entry for it would look right and
	// never work, so the output must say so instead of printing one.
	if !strings.Contains(out, "mcp-remote") {
		t.Errorf("http output must tell Claude Desktop users they need the mcp-remote bridge:\n%s", out)
	}
}

// TestClientConfigNeverPrintsChimeraPath is the acceptance test for DEFECT 1,
// on BOTH transports: the command must never send anyone to a file that does
// not exist.
func TestClientConfigNeverPrintsChimeraPath(t *testing.T) {
	cases := map[string]string{
		"stdio": "transport: stdio",
		"http":  "transport: http\nlisten_addr: \"127.0.0.1:29099\"",
	}
	for name, head := range cases {
		t.Run(name, func(t *testing.T) {
			cfgPath := writeClientConfig(t, head)
			out, errOut, err := execRootStreams(t, "client-config", "-c", cfgPath)
			if err != nil {
				t.Fatalf("client-config failed: %v\n%s", err, out)
			}
			if strings.Contains(out+errOut, chimeraPath) {
				t.Errorf("output points at the non-existent %s:\n%s", chimeraPath, out+errOut)
			}
			// ~/.claude.json does exist, but it is a large machine-managed
			// file — telling an operator to hand-edit it is a bad instruction
			// even though the path is real.
			if strings.Contains(out+errOut, "~/.claude.json") {
				t.Errorf("output must not send the operator to hand-edit ~/.claude.json:\n%s", out+errOut)
			}
		})
	}
}

// TestClientConfigStdioDoesNotLeakToken: auth_token guards the HTTP endpoint
// only. In stdio mode it is not part of any snippet, so it must not appear —
// a leak here would silently turn a harmless command into a secret-bearing one.
func TestClientConfigStdioDoesNotLeakToken(t *testing.T) {
	const token = "stdio-token-must-not-appear"
	cfgPath := writeClientConfig(t, fmt.Sprintf("transport: stdio\nauth_token: %s", token))

	out, errOut, err := execRootStreams(t, "client-config", "-c", cfgPath)
	if err != nil {
		t.Fatalf("client-config failed: %v\n%s", err, out)
	}
	if strings.Contains(out+errOut, token) {
		t.Errorf("auth_token leaked into stdio output:\n%s", out+errOut)
	}
	if strings.Contains(errOut, "WARNING") {
		t.Errorf("stdio output carries no secret, so it must not warn about one:\n%s", errOut)
	}
}

// TestClientConfigSecretWarningGoesToStderr: the JSON on stdout is meant to be
// piped, so the warning belongs on stderr. Separate buffers are only
// trustworthy because the command writes through explicit OutOrStdout /
// ErrOrStderr — cobra's own Println resolves to stderr and would make this
// assertion vacuous.
func TestClientConfigSecretWarningGoesToStderr(t *testing.T) {
	cfgPath := writeClientConfig(t, "transport: http\nlisten_addr: \"127.0.0.1:29099\"\nauth_token: s3cr3t-token")

	out, errOut, err := execRootStreams(t, "client-config", "-c", cfgPath)
	if err != nil {
		t.Fatalf("client-config failed: %v\n%s", err, out)
	}
	if !strings.Contains(errOut, "WARNING") {
		t.Errorf("the secret warning is missing from stderr:\n%s", errOut)
	}
	if strings.Contains(out, "WARNING") {
		t.Errorf("the secret warning must not pollute stdout:\n%s", out)
	}
}
