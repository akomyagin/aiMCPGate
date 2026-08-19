package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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

// clientConfigUpstreams is the tail every config in these tests shares: one
// inert upstream (it is never started — client-config reads the file, it does
// not connect), so these tests stay free of processes and ports.
const clientConfigUpstreams = `
log_level: error
upstreams:
  - name: demo
    kind: stdio
    command: /bin/true
    enabled: true
`

// writeClientConfig writes a gateway config into a fresh temp dir.
func writeClientConfig(t *testing.T, extra string) string {
	t.Helper()
	return writeDoctorConfig(t, extra+clientConfigUpstreams)
}

// writeClientConfigIn writes the same config into a CHOSEN directory. The
// quoting tests need the config path itself to carry shell metacharacters, and
// t.TempDir() never produces those.
func writeClientConfigIn(t *testing.T, dir, extra string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %q: %v", dir, err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(extra+clientConfigUpstreams), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// chdirForTest switches the working directory for the duration of one test and
// restores it afterwards. (t.Chdir would do this, but it lands in Go 1.24 and
// the module is on 1.23.)
func chdirForTest(t *testing.T, dir string) {
	t.Helper()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir %q: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(prev); err != nil {
			t.Fatalf("restore cwd %q: %v", prev, err)
		}
	})
}

// mcpAddLine returns the `claude mcp add …` line of the output — the single
// most copied line the command prints, and the one that goes straight into a
// shell.
func mcpAddLine(t *testing.T, out string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "claude mcp add") {
			return line
		}
	}
	t.Fatalf("no `claude mcp add` line in output:\n%s", out)
	return ""
}

// snippetEntries cuts every JSON snippet out of the output and returns the
// mcpServers["mcp-gate"] object of each, in printed order.
//
// Parsing rather than substring-matching is the point: what this command prints
// is data destined for a client's JSON file, so the tests have to fail on
// invalid JSON, on a renamed key ("cmd" for "command") and on args degraded
// from an array to a string — none of which strings.Contains can see.
func snippetEntries(t *testing.T, out string) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for _, raw := range snippetBlocks(out) {
		var doc struct {
			MCPServers map[string]map[string]any `json:"mcpServers"`
		}
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			t.Fatalf("snippet is not valid JSON: %v\n%s", err, raw)
		}
		entry, ok := doc.MCPServers["mcp-gate"]
		if !ok {
			t.Fatalf("snippet has no mcpServers[\"mcp-gate\"] object:\n%s", raw)
		}
		entries = append(entries, entry)
	}
	return entries
}

// snippetBlocks returns the JSON snippets as PRINTED, from a line that is
// exactly "{" to the next line that is exactly "}". Assertions about how the
// JSON reads (rather than what it parses to) have to be made on this text: the
// prose around the snippets repeats the same paths, so searching the whole
// output would pass on a snippet that renders them differently.
func snippetBlocks(out string) []string {
	var (
		blocks []string
		block  []string
		inside bool
	)
	for _, line := range strings.Split(out, "\n") {
		switch {
		case line == "{":
			inside, block = true, []string{line}
		case inside:
			block = append(block, line)
			if line == "}" {
				inside = false
				blocks = append(blocks, strings.Join(block, "\n"))
			}
		}
	}
	return blocks
}

// TestShellQuote pins the rendering of one argv element. The `claude mcp add`
// line is pasted into a shell verbatim, so an unquoted space silently truncates
// the command and an unquoted & sends it to the background.
func TestShellQuote(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"/usr/local/bin/mcp-gate", "/usr/local/bin/mcp-gate"}, // inert: must stay bare
		{"serve", "serve"},
		{"-c", "-c"},
		{"/home/u/my tools/mcp-gate", "'/home/u/my tools/mcp-gate'"},
		{"/home/u/a&b/mcp-gate", "'/home/u/a&b/mcp-gate'"},
		{"/home/u/$HOME/mcp-gate", "'/home/u/$HOME/mcp-gate'"},
		{"/home/u/a;rm -rf x/mcp-gate", "'/home/u/a;rm -rf x/mcp-gate'"},
		{"/home/u/it's/mcp-gate", `'/home/u/it'\''s/mcp-gate'`},
		{"Authorization: Bearer s3cr3t", "'Authorization: Bearer s3cr3t'"},
		{"", "''"},
	}
	for _, c := range cases {
		if got := shellQuote(c.in); got != c.want {
			t.Errorf("shellQuote(%q) = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestClientConfigStdioPrintsLaunchSnippet is the acceptance test for DEFECT 2:
// stdio is the gateway's default transport, and the command used to REFUSE to
// run there. It must succeed and print how to launch the gateway: an absolute
// binary path (the client spawns the process without the operator's PATH) and
// the absolute config path it was pointed at.
//
// --config is passed RELATIVE, from a cwd that makes it resolve: an absolute
// spelling (what t.TempDir hands out) would keep this test green even with
// filepath.Abs removed from gatewayLaunchArgs, which is exactly the invariant
// the comment above claims to cover.
func TestClientConfigStdioPrintsLaunchSnippet(t *testing.T) {
	dir := t.TempDir()
	writeClientConfigIn(t, dir, "transport: stdio")
	chdirForTest(t, dir)
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	absCfg := filepath.Join(wd, "config.yaml")

	out, errOut, err := execRootStreams(t, "client-config", "-c", "config.yaml")
	if err != nil {
		t.Fatalf("client-config on a stdio config failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
	}
	// In-process the running binary IS the test binary; asserting on it keeps
	// the check honest about os.Executable being used rather than a literal.
	exe, exeErr := os.Executable()
	if exeErr != nil {
		t.Fatalf("os.Executable: %v", exeErr)
	}

	entries := snippetEntries(t, out)
	if len(entries) != 3 {
		t.Fatalf("stdio output must carry 3 snippets (Claude Code, Cursor, Desktop), got %d:\n%s", len(entries), out)
	}
	wantArgs := []any{"serve", "-c", absCfg}
	for i, entry := range entries {
		if entry["command"] != exe {
			t.Errorf("snippet %d: command = %v, want the absolute binary path %q", i, entry["command"], exe)
		}
		if !reflect.DeepEqual(entry["args"], wantArgs) {
			t.Errorf("snippet %d: args = %#v, want %#v (relative --config must be made absolute)", i, entry["args"], wantArgs)
		}
	}
	// "type": "stdio" belongs to Claude Code alone — Cursor and Desktop are
	// given the plain command/args form, which is the shape all three accept.
	// Without this assertion that decision could be "simplified" either way
	// without a single test going red.
	if entries[0]["type"] != "stdio" {
		t.Errorf("the Claude Code snippet (first) must carry \"type\": \"stdio\", got %v", entries[0]["type"])
	}
	for i, entry := range entries[1:] {
		if _, ok := entry["type"]; ok {
			t.Errorf("snippet %d (Cursor/Desktop) must not carry a \"type\" key, got %v", i+1, entry["type"])
		}
	}

	add := mcpAddLine(t, out)
	if !strings.Contains(add, "claude mcp add mcp-gate -- ") {
		t.Errorf("stdio output must offer the `claude mcp add` form:\n%s", add)
	}
	if !strings.Contains(add, absCfg) || !strings.Contains(add, exe) {
		t.Errorf("the `claude mcp add` line must name the binary %q and the absolute config %q:\n%s", exe, absCfg, add)
	}
	// The per-OS Claude Desktop paths replaced the chimera path; pinning only
	// the chimera's ABSENCE would stay green if the whole section were dropped.
	if !strings.Contains(out, "claude_desktop_config.json") {
		t.Errorf("stdio output must tell Claude Desktop users which file to edit:\n%s", out)
	}
	for _, want := range []string{"macOS", "Windows", "Linux"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdio output must name the Claude Desktop config path for %s:\n%s", want, out)
		}
	}
}

// TestClientConfigStdioQuotesConfigPath is the acceptance test for the review's
// Ф1: the `claude mcp add` line is shell input. A directory with a space in it
// is not exotic ("C:\Program Files", "~/Library/Application Support",
// "/home/user/My Projects"), and unquoted it makes the shell take the first
// word as the command; & is worse — it backgrounds the command.
//
// The JSON snippets must NOT be quoted the same way: they are parsed, not
// evaluated, so they carry the raw path.
func TestClientConfigStdioQuotesConfigPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "my a&b configs")
	cfgPath := writeClientConfigIn(t, dir, "transport: stdio")

	out, errOut, err := execRootStreams(t, "client-config", "-c", cfgPath)
	if err != nil {
		t.Fatalf("client-config failed: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
	}

	add := mcpAddLine(t, out)
	if !strings.Contains(add, "'"+cfgPath+"'") {
		t.Errorf("the config path must be shell-quoted in the `claude mcp add` line (want '%s'):\n%s", cfgPath, add)
	}
	// A quoted path is only useful if it stays ONE word: the bare, unquoted
	// spelling must not appear anywhere on that line.
	if strings.Contains(strings.ReplaceAll(add, "'"+cfgPath+"'", ""), cfgPath) {
		t.Errorf("the config path also appears unquoted on the `claude mcp add` line:\n%s", add)
	}

	entries := snippetEntries(t, out)
	if len(entries) == 0 {
		t.Fatalf("no JSON snippets in output:\n%s", out)
	}
	for i, entry := range entries {
		args, ok := entry["args"].([]any)
		if !ok || len(args) != 3 {
			t.Fatalf("snippet %d: args = %#v, want [serve -c <path>]", i, entry["args"])
		}
		if args[2] != cfgPath {
			t.Errorf("snippet %d: JSON must carry the RAW path %q, got %v", i, cfgPath, args[2])
		}
	}
	// The path holds an &: with the encoder's default HTML escaping it would be
	// printed as a numeric unicode escape — valid JSON, unpleasant to read.
	if !strings.Contains(out, cfgPath) {
		t.Errorf("the JSON snippets must show the path literally, without unicode-escaping &:\n%s", out)
	}
}

// TestClientConfigStdioQuotesBinaryPath covers the other half of Ф1 — the
// binary path — which cannot be exercised in-process (there the binary is the
// test binary, wherever the toolchain put it). The real gateway is copied into
// a directory whose name has a space, exactly like an install under
// "C:\Program Files" or "~/My Tools".
func TestClientConfigStdioQuotesBinaryPath(t *testing.T) {
	built := buildGateBinary(t)
	body, err := os.ReadFile(built)
	if err != nil {
		t.Fatalf("read built binary: %v", err)
	}
	spacedDir := filepath.Join(t.TempDir(), "my tools")
	if err := os.MkdirAll(spacedDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	bin := filepath.Join(spacedDir, "mcp-gate")
	if err := os.WriteFile(bin, body, 0o755); err != nil {
		t.Fatalf("copy binary: %v", err)
	}
	cfgPath := writeClientConfigIn(t, filepath.Join(t.TempDir(), "my configs"), "transport: stdio")

	raw, err := exec.Command(bin, "client-config", "-c", cfgPath).CombinedOutput()
	out := string(raw)
	if err != nil {
		t.Fatalf("client-config failed: %v\noutput:\n%s", err, out)
	}

	add := mcpAddLine(t, out)
	if !strings.Contains(add, "'"+bin+"'") {
		t.Errorf("the binary path must be shell-quoted (want '%s'):\n%s", bin, add)
	}
	if !strings.Contains(add, "'"+cfgPath+"'") {
		t.Errorf("the config path must be shell-quoted (want '%s'):\n%s", cfgPath, add)
	}
	// What the shell would actually run: `sh -c '<line minus the claude prefix>'`
	// is not run here, but the command word must at least be a single token.
	fields := strings.Fields(strings.TrimPrefix(add, "claude mcp add mcp-gate -- "))
	if len(fields) > 0 && !strings.HasPrefix(fields[0], "'") {
		t.Errorf("the spawn command starts with a bare word the shell would split: %q\n%s", fields[0], add)
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

// TestIsGoRunTempBinary pins the `go run` heuristic in both directions. Under
// `go run ./cmd client-config` — this project's documented way of running the
// gateway — os.Executable points at a file the go tool deletes on exit, so a
// snippet naming it looks authoritative and is guaranteed dead. Equally
// important is the other direction: a real install must never be replaced by a
// bare name because its path happens to contain one of the markers.
func TestIsGoRunTempBinary(t *testing.T) {
	yes := []string{
		"/tmp/go-build1618063049/b001/exe/cmd",
		"/home/u/.cache/go-build/go-build42/b001/exe/mcp-gate",
	}
	no := []string{
		"/usr/local/bin/mcp-gate",
		"/home/u/go-build/mcp-gate",      // go-build dir, but not the go run layout
		"/opt/mcp-gate/exe/mcp-gate",     // exe dir, but no go-build ancestor
		"/tmp/go-build123/b001/cli.test", // the test binary itself: not under exe/
		"/home/u/My Projects/exe/mcp-gate",
	}
	for _, path := range yes {
		if !isGoRunTempBinary(path) {
			t.Errorf("isGoRunTempBinary(%q) = false, want true (this file is deleted on exit)", path)
		}
	}
	for _, path := range no {
		if isGoRunTempBinary(path) {
			t.Errorf("isGoRunTempBinary(%q) = true, want false (a real path must not be discarded)", path)
		}
	}
}

// TestStdioBinaryWarnings pins that neither fallback is silent. A bare
// "mcp-gate" is resolved through the CLIENT's PATH, and with no --config the
// gateway it finds reads the config.yaml next to ITSELF — the two degradations
// compose into "wrong gateway, wrong config, no error", so the second warning
// appears exactly when the first one alone would be misleading.
func TestStdioBinaryWarnings(t *testing.T) {
	if got := stdioBinaryWarnings("", []string{"serve"}); got != nil {
		t.Errorf("a usable binary path must produce no warning, got %q", got)
	}

	withCfg := stdioBinaryWarnings("os.Executable() failed (boom)", []string{"serve", "-c", "/etc/mcp-gate/config.yaml"})
	if len(withCfg) != 1 {
		t.Fatalf("want 1 warning when the config path is pinned, got %d: %q", len(withCfg), withCfg)
	}
	if !strings.Contains(withCfg[0], "PATH") || !strings.Contains(withCfg[0], "boom") {
		t.Errorf("the warning must name the cause and the PATH lookup: %q", withCfg[0])
	}

	bare := stdioBinaryWarnings("os.Executable() failed (boom)", []string{"serve"})
	if len(bare) != 2 {
		t.Fatalf("want 2 warnings when neither the binary nor the config is pinned, got %d: %q", len(bare), bare)
	}
	if !strings.Contains(bare[1], "config.yaml") {
		t.Errorf("the second warning must explain that the config is picked up next to that other binary: %q", bare[1])
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
	entries := snippetEntries(t, out)
	if len(entries) != 2 {
		t.Fatalf("http output must carry 2 snippets (Claude Code, Cursor), got %d:\n%s", len(entries), out)
	}
	for i, entry := range entries {
		if entry["type"] != "http" {
			t.Errorf("snippet %d: type = %v, want \"http\"", i, entry["type"])
		}
		if entry["url"] != "http://127.0.0.1:29099/mcp" {
			t.Errorf("snippet %d: url = %v", i, entry["url"])
		}
		headers, ok := entry["headers"].(map[string]any)
		if !ok {
			t.Fatalf("snippet %d: headers = %#v, want an object", i, entry["headers"])
		}
		if headers["Authorization"] != "Bearer s3cr3t-token" {
			t.Errorf("snippet %d: Authorization = %v", i, headers["Authorization"])
		}
	}
	if !strings.Contains(mcpAddLine(t, out), `--header 'Authorization: Bearer s3cr3t-token'`) {
		t.Errorf("the `claude mcp add` line is missing the quoted Authorization header:\n%s", out)
	}
	// Desktop speaks stdio only: an http entry for it would look right and
	// never work, so the output must say so instead of printing one.
	if !strings.Contains(out, "mcp-remote") {
		t.Errorf("http output must tell Claude Desktop users they need the mcp-remote bridge:\n%s", out)
	}
}

// TestClientConfigHTTPQuotesAuthHeader: auth_token is operator-written text. A
// token holding quotes used to break out of the double-quoted header argument
// outright; and even when it does not, double quotes still let the shell expand
// $ and interpret a backslash, while single quotes do not. ($ itself cannot be
// used in this fixture: the config loader expands environment references before
// the value ever reaches this command — see shellQuote's own test for that
// character.)
func TestClientConfigHTTPQuotesAuthHeader(t *testing.T) {
	cfgPath := writeClientConfig(t, "transport: http\nlisten_addr: \"127.0.0.1:29099\"\nauth_token: \"it's-\\\"quoted\\\"-token\"")

	out, _, err := execRootStreams(t, "client-config", "-c", cfgPath)
	if err != nil {
		t.Fatalf("client-config failed: %v\n%s", err, out)
	}
	add := mcpAddLine(t, out)
	want := `--header 'Authorization: Bearer it'\''s-"quoted"-token'`
	if !strings.Contains(add, want) {
		t.Errorf("the header must be single-quoted with the apostrophe spliced:\nwant substring: %s\ngot line:       %s", want, add)
	}
	if strings.Contains(add, `"Authorization`) {
		t.Errorf("the header must not be double-quoted — $ and \\ would still expand:\n%s", add)
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

// TestClientConfigSnippetDoesNotHTMLEscape pins snippetJSON's SetEscapeHTML(false).
//
// Go's encoder escapes & < > into & < > by DEFAULT, and every
// JSON parser reads those back correctly — so nothing breaks functionally and
// no other assertion in this file can see the difference. But this snippet is
// printed to be read and hand-edited by a person, and a path rendered as
// /opt/a&b/mcp-gate reads as corruption.
//
// Written because the escaping was found flipped back to true in the working
// tree with the whole package still green: without this test the only thing
// standing between that regression and a release was someone reading the diff.
func TestClientConfigSnippetDoesNotHTMLEscape(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a&b<c>")
	cfgPath := writeClientConfigIn(t, dir, "transport: stdio\n")

	out, _, err := execRootStreams(t, "client-config", "-c", cfgPath)
	if err != nil {
		t.Fatalf("client-config failed: %v\n%s", err, out)
	}
	for _, escaped := range []string{`\u0026`, `\u003c`, `\u003e`} {
		if strings.Contains(out, escaped) {
			t.Errorf("output carries the HTML escape %s instead of the literal character:\n%s", escaped, out)
		}
	}
	// The path must still be there verbatim — otherwise the assertions above
	// would also pass on output that lost it entirely.
	if !strings.Contains(out, "a&b<c>") {
		t.Errorf("the config path with & < > is missing from the output:\n%s", out)
	}
}
