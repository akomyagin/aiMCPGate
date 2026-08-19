package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestTokenGenerate(t *testing.T) {
	root := Build("test")
	buf := &bytes.Buffer{}
	root.SetOut(buf)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"token", "--generate"})

	if err := root.Execute(); err != nil {
		t.Fatalf("token --generate: %v", err)
	}
	tok := strings.TrimSpace(buf.String())
	if len(tok) != 64 {
		t.Errorf("generated token length = %d, want 64 hex chars", len(tok))
	}
}

func TestTokenGenerateUnique(t *testing.T) {
	tokens := make(map[string]bool, 5)
	for range 5 {
		tok, err := randomToken()
		if err != nil {
			t.Fatal(err)
		}
		if tokens[tok] {
			t.Fatal("duplicate token generated")
		}
		tokens[tok] = true
	}
}

func TestTokenShowNoConfig(t *testing.T) {
	root := Build("test")
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"token"}) // no --config, no --generate

	err := root.Execute()
	if err == nil {
		t.Fatal("expected error when auth_token not set, got nil")
	}
}

// TestTokenValueGoesToStdout pins the STREAM the secret travels on, not its
// content.
//
// `mcp-gate token` exists to be consumed: `TOKEN=$(mcp-gate token)`,
// `mcp-gate token > token.txt`. Until this test the value went to STDERR
// (cobra's cmd.Println resolves through OutOrStderr, which falls back to
// os.Stderr whenever no out writer is set — i.e. in every real run), so both
// forms silently produced an empty string while the terminal still showed the
// token. Found in real use, on a running gateway, not by the suite.
//
// Uses runCmdOSStreams rather than root.SetOut: setting an out writer HIDES this
// bug entirely, because then Println has a writer to resolve to. Same reason the
// logs listing bug survived its own tests — see that helper's comment.
func TestTokenValueGoesToStdout(t *testing.T) {
	const want = "s3cr3t-from-config"
	cfgPath := writeDoctorConfig(t, "transport: http\nauth_token: "+want+`
upstreams:
  - name: demo
    kind: stdio
    command: /bin/true
    enabled: true
`)

	stdout, stderr := runCmdOSStreams(t, "token", "-c", cfgPath)

	if !strings.Contains(stdout, want) {
		t.Errorf("the token is missing from STDOUT — $(mcp-gate token) would capture nothing:\nstdout: %q\nstderr: %q", stdout, stderr)
	}
	if strings.Contains(stderr, want) {
		t.Errorf("the token leaked onto STDERR as well:\nstderr: %q", stderr)
	}
}

// TestTokenGenerateValueGoesToStdout is the same contract for --generate: the
// token on stdout, the "copy it to your .env" guidance on stderr, so piping the
// command yields the value alone. The guidance deliberately repeats the token,
// so stderr is checked for the ADVICE, not for absence of the secret.
func TestTokenGenerateValueGoesToStdout(t *testing.T) {
	stdout, stderr := runCmdOSStreams(t, "token", "--generate")

	tok := strings.TrimSpace(stdout)
	if len(tok) != 64 {
		t.Errorf("stdout must carry the generated token alone (64 hex chars), got %q (stderr: %q)", stdout, stderr)
	}
	if !strings.Contains(stderr, "AIMCPGATE_TOKEN=") {
		t.Errorf("the .env guidance must stay on stderr:\nstderr: %q", stderr)
	}
	if strings.Contains(stdout, "AIMCPGATE_TOKEN=") {
		t.Errorf("the guidance must not pollute stdout — it would be captured as part of the token:\nstdout: %q", stdout)
	}
}

// TestVersionGoesToStdout and TestSkillGoesToStdout pin the same stream contract
// for the two other commands that carried this bug. They live beside the token
// tests on purpose: the three were found together, share one root cause
// (cobra's cmd.Print* resolving through OutOrStderr) and must be fixed or
// regressed together.
func TestVersionGoesToStdout(t *testing.T) {
	stdout, stderr := runCmdOSStreams(t, "version")

	if strings.TrimSpace(stdout) != "test" {
		t.Errorf("version must land on STDOUT — $(mcp-gate version) would capture nothing:\nstdout: %q\nstderr: %q", stdout, stderr)
	}
}

// TestSkillGoesToStdout guards the exact command the README prints:
// `mcp-gate skill > .claude/skills/mcp-gate/SKILL.md`. On stderr that redirect
// yields an empty file, which is why the stream, not the text, is the assertion.
func TestSkillGoesToStdout(t *testing.T) {
	stdout, stderr := runCmdOSStreams(t, "skill")

	if len(stdout) == 0 {
		t.Fatalf("skill wrote nothing to STDOUT — `mcp-gate skill > SKILL.md` would create an empty file (stderr held %d bytes)", len(stderr))
	}
	if !strings.Contains(stdout, "mcp-gate") {
		t.Errorf("the skill body on stdout does not look like the guide:\n%.200s", stdout)
	}
}
