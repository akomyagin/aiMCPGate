package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

func newClientConfigCmd() *cobra.Command {
	var configPath *string

	cmd := &cobra.Command{
		Use:   "client-config",
		Short: "Print MCP client configuration snippets for Claude Code, Cursor and others",
		Long: "Reads the gateway config and prints ready-to-use snippets for adding aiMCPGate\n" +
			"to Claude Code, Cursor and Claude Desktop. The snippets follow the config's own\n" +
			"transport: with transport: http they point a client at the running endpoint,\n" +
			"with transport: stdio they tell the client how to launch the gateway itself.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(*configPath)
			if err != nil {
				return err
			}
			// Both transports are real deployments, and stdio is the DEFAULT one —
			// refusing to print anything unless transport: http (as this command
			// did until fix/client-config) locked the command out of the mode the
			// gateway normally runs in.
			if cfg.Transport == config.TransportHTTP {
				return printHTTPClientConfig(cmd, cfg)
			}
			return printStdioClientConfig(cmd, cfg, *configPath)
		},
	}
	configPath = addConfigFlag(cmd)
	return cmd
}

// sectionPrinter prints titled separator lines, padded to a fixed width so the
// output reads as a list of blocks rather than a wall of text, and puts exactly
// one blank line BETWEEN blocks — never before the first or after the last.
// The spacing lives here rather than at the call sites: while each site spaced
// itself the rhythm went uneven (a blank line after the `claude mcp add` line,
// none after the JSON blocks, so the next header stuck to a closing brace).
type sectionPrinter struct {
	w       io.Writer
	started bool
}

func (p *sectionPrinter) section(title string) {
	if p.started {
		fmt.Fprintln(p.w)
	}
	p.started = true

	const width = 78
	line := "─── " + title + " "
	if n := width - len([]rune(line)); n > 0 {
		line += strings.Repeat("─", n)
	}
	fmt.Fprintln(p.w, line)
}

// claudeDesktopPaths lists where Claude Desktop — a DIFFERENT product from
// Claude Code, with its own config file — keeps its MCP servers. Naming the
// per-OS paths beats naming one: the operator's OS is unknown here, and the
// previous single path ("~/.claude/claude_desktop_config.json") existed on
// none of them: it was a chimera of the two products' file names.
const claudeDesktopPaths = "  macOS    ~/Library/Application Support/Claude/claude_desktop_config.json\n" +
	"  Windows  %APPDATA%\\Claude\\claude_desktop_config.json\n" +
	"  Linux    ~/.config/Claude/claude_desktop_config.json"

// shellSafeChars are the characters a POSIX shell passes through untouched, so
// a string built only from them needs no quoting at all.
const shellSafeChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@%+=:,./-_"

// shellQuote renders one argv element for the `claude mcp add` line, which the
// operator pastes into a shell verbatim. Anything not provably inert is wrapped
// in SINGLE quotes: inside them a shell expands nothing whatsoever, while
// double quotes still honour $, ` and \ — and the values quoted here are
// filesystem paths and auth tokens, exactly the strings that carry such
// characters ("C:\Program Files\…", "~/Library/Application Support/…",
// "/home/user/My Projects/…"). The one character single quotes cannot hold is
// the apostrophe itself, spliced in by closing, escaping and reopening:
//
//	don't  ->  'don'\''t'
//
// Inert strings are deliberately left bare: quoting an ordinary
// /usr/local/bin/mcp-gate would make the common case uglier for no gain.
func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if strings.ContainsFunc(s, func(r rune) bool { return !strings.ContainsRune(shellSafeChars, r) }) {
		return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
	}
	return s
}

// shellJoin builds a command line out of argv elements, quoting each one. A
// plain strings.Join with spaces (what this file did until the review) hands a
// path with a space to the shell as two separate words, and a path with & sends
// the command to the background.
func shellJoin(argv ...string) string {
	quoted := make([]string, len(argv))
	for i, arg := range argv {
		quoted[i] = shellQuote(arg)
	}
	return strings.Join(quoted, " ")
}

// snippetJSON wraps one server entry into the mcpServers object every client
// here consumes, formatted for pasting.
//
// HTML escaping is turned OFF: by default the encoder rewrites & < > into
// numeric unicode escapes, so a path like /opt/a&b/mcp-gate comes out mangled.
// Any parser reads those escapes back correctly, but this snippet exists to be
// read and hand-edited by a human.
func snippetJSON(entry map[string]any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(map[string]any{
		"mcpServers": map[string]any{"mcp-gate": entry},
	}); err != nil {
		// Unreachable for the maps built here (strings and []string only), but
		// the error is not dropped: swallowing it used to print an empty block
		// that still looked like a usable snippet.
		return "", fmt.Errorf("render the client config snippet: %w", err)
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}

// printHTTPClientConfig points clients at an already-running gateway endpoint.
func printHTTPClientConfig(cmd *cobra.Command, cfg *config.Config) error {
	addr := cfg.EffectiveListenAddr()
	// Bare ":port" → localhost
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	url := "http://" + addr + "/mcp"

	entry := map[string]any{
		"type": "http",
		"url":  url,
	}
	if cfg.AuthToken != "" {
		entry["headers"] = map[string]string{
			"Authorization": "Bearer " + cfg.AuthToken,
		}
	}
	snippet, err := snippetJSON(entry)
	if err != nil {
		return err
	}

	// The warning goes to STDERR so the piped output stays free of it; the
	// snippets below embed the auth_token verbatim, which makes the command's
	// whole stdout secret-bearing.
	if cfg.AuthToken != "" {
		fmt.Fprintln(cmd.ErrOrStderr(), "# WARNING: the snippet below contains your auth_token — treat it as a secret (don't paste it into a shared chat, public gist, or commit it to git).")
	}

	p := &sectionPrinter{w: cmd.OutOrStdout()}
	w := p.w

	p.section("Claude Code  (recommended: claude mcp add)")
	claudeCmd := "claude mcp add --transport http mcp-gate " + shellQuote(url)
	if cfg.AuthToken != "" {
		// The token is operator-written text: it may hold quotes, $ or spaces,
		// and the header value holds a space by construction.
		claudeCmd += " --header " + shellQuote("Authorization: Bearer "+cfg.AuthToken)
	}
	fmt.Fprintln(w, claudeCmd)

	p.section("Claude Code  (.mcp.json in the project root)")
	fmt.Fprintln(w, "Merge into the existing file or create it if absent:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, snippet)

	p.section("Cursor  (.cursor/mcp.json in project or ~/.cursor/mcp.json)")
	fmt.Fprintln(w, "Same JSON format:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, snippet)

	// No JSON snippet for Desktop on purpose: it speaks stdio only, so a
	// correct-looking http entry would fail at runtime no matter which file it
	// is pasted into. Saying so is more useful than a snippet that cannot work.
	p.section("Claude Desktop  (HTTP is not supported directly)")
	fmt.Fprintln(w, "Claude Desktop launches MCP servers as local processes and cannot call an")
	fmt.Fprintln(w, "HTTP endpoint on its own. Either bridge it with mcp-remote, or run a second")
	fmt.Fprintln(w, "gateway config with transport: stdio and re-run this command to get a snippet.")

	if cfg.AuthToken == "" {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "⚠  auth_token is not set — the endpoint is open to anyone who can reach it.")
		fmt.Fprintln(w, "   Run `mcp-gate token --generate` to create a token and add it to your config.")
	}
	return nil
}

// printStdioClientConfig prints snippets that LAUNCH the gateway: in stdio mode
// there is no endpoint to point at, the client owns the process.
//
// No auth_token appears in the STDOUT snippets — it only guards the HTTP
// endpoint, so the piped output stays free of secrets. There is, however, a
// stderr warning (secretVarRefWarnings) when the config references any ${VAR}:
// the client launches the gateway in its OWN environment, so the operator's
// shell variables are not inherited and must be re-declared client-side.
func printStdioClientConfig(cmd *cobra.Command, cfg *config.Config, configFlag string) error {
	bin, unusable := gatewayBinaryPath()
	args := gatewayLaunchArgs(configFlag)

	for _, line := range stdioBinaryWarnings(unusable, args) {
		fmt.Fprintln(cmd.ErrOrStderr(), line)
	}
	for _, line := range secretVarRefWarnings(cfg.SecretVarRefs) {
		fmt.Fprintln(cmd.ErrOrStderr(), line)
	}

	// "type": "stdio" is spelled out for Claude Code, which writes it; Claude
	// Desktop and Cursor infer it from the presence of "command" and are given
	// the plain form.
	withType, err := snippetJSON(map[string]any{"type": "stdio", "command": bin, "args": args})
	if err != nil {
		return err
	}
	plain, err := snippetJSON(map[string]any{"command": bin, "args": args})
	if err != nil {
		return err
	}

	p := &sectionPrinter{w: cmd.OutOrStdout()}
	w := p.w

	p.section("Claude Code  (recommended: claude mcp add)")
	fmt.Fprintln(w, "claude mcp add mcp-gate -- "+shellJoin(append([]string{bin}, args...)...))

	p.section("Claude Code  (.mcp.json in the project root)")
	fmt.Fprintln(w, "Merge into the existing file or create it if absent:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, withType)

	p.section("Cursor  (.cursor/mcp.json in project or ~/.cursor/mcp.json)")
	fmt.Fprintln(w, "Same JSON format:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, plain)

	p.section("Claude Desktop  (restart the app after editing)")
	fmt.Fprintln(w, claudeDesktopPaths)
	fmt.Fprintln(w)
	fmt.Fprintln(w, plain)
	return nil
}

// fallbackBinaryName is what the snippets say when the real path is unusable.
// A bare name is not a working answer, only an obviously editable one — see
// stdioBinaryWarnings for why it is never printed silently.
const fallbackBinaryName = "mcp-gate"

// gatewayBinaryPath returns the ABSOLUTE path of the running binary, and that
// absoluteness is the point: an MCP client spawns the gateway itself, not from
// the operator's login shell, so the PATH that makes a bare `mcp-gate` work in
// a terminal may simply not be there.
//
// The second result is empty when the path is usable and otherwise says why it
// is not: neither failure case is worth aborting the command over, but both
// have to reach the operator (stdioBinaryWarnings), because the fallback name
// is not self-evidently harmless.
func gatewayBinaryPath() (string, string) {
	exe, err := os.Executable()
	if err != nil {
		return fallbackBinaryName, fmt.Sprintf("os.Executable() failed (%v)", err)
	}
	if isGoRunTempBinary(exe) {
		return fallbackBinaryName, "this process is the one-shot binary `go run` builds, which is deleted the moment it exits"
	}
	return exe, ""
}

// isGoRunTempBinary recognises the executable `go run` produces: the go tool
// links it into <build cache>/go-buildNNNN/bNNN/exe/<pkg> and removes that tree
// as soon as the process exits. Printing such a path is the worst of both
// worlds — it looks authoritative and is guaranteed dead — and `go run ./cmd`
// is this project's documented way of running the gateway (CLAUDE.md,
// "Команды разработки"), so the case is routine rather than exotic.
//
// The heuristic requires BOTH markers of that layout, the parent directory
// named "exe" and a "go-build…" ancestor, because either one alone could belong
// to a real install (somebody's ~/go-build/mcp-gate, or a packager that keeps
// binaries under exe/). Requiring both keeps a false positive — silently
// replacing a real path with a bare name — implausible.
func isGoRunTempBinary(exe string) bool {
	dir := filepath.Dir(exe)
	if filepath.Base(dir) != "exe" {
		return false
	}
	for {
		parent := filepath.Dir(dir)
		if parent == dir { // reached the root
			return false
		}
		dir = parent
		if strings.HasPrefix(filepath.Base(dir), "go-build") {
			return true
		}
	}
}

// stdioBinaryWarnings turns an unusable binary path into stderr lines.
//
// It exists because the degradation is not silent-safe. A bare "mcp-gate" is
// resolved through the CLIENT's PATH, so it may find a DIFFERENT install; and
// when --config was not given either (no "-c" among args), that other install
// then reads the config.yaml sitting next to ITSELF. The two fallbacks compose
// into a snippet that starts the wrong gateway with the wrong config and
// reports no error at all. Falling back is still the right call — there is
// nothing to fail over to — but it must be said out loud.
func stdioBinaryWarnings(reason string, args []string) []string {
	if reason == "" {
		return nil
	}
	lines := []string{fmt.Sprintf(
		"# WARNING: the gateway's own path is unavailable — %s. The snippets below say %q, which the client resolves through ITS OWN PATH: replace it with the path of your installed binary.",
		reason, fallbackBinaryName)}
	if !slices.Contains(args, "-c") {
		lines = append(lines, fmt.Sprintf(
			"# WARNING: no --config was given either, so the snippets pin no config path: whichever %q the client finds would read the config.yaml next to itself — not necessarily the config this command just read.",
			fallbackBinaryName))
	}
	return lines
}

// secretVarRefWarnings warns that the stdio snippets inherit NONE of the
// operator's environment: the MCP client spawns the gateway itself, so every
// ${VAR} the config references — resolved right now or not — must be present
// in the environment the CLIENT starts the gateway in (e.g. an "env" block in
// the client's mcpServers entry). Firing on resolved references too is the
// point: resolution happened in the operator's shell, which proves nothing
// about the client's environment — a gateway launched from the snippet would
// come up with those secrets EMPTY and no signal anywhere. Variable NAMES
// only, never values (they are secrets). Same stderr style as
// stdioBinaryWarnings: stdout stays clean for piping.
func secretVarRefWarnings(refs []config.SecretVarRef) []string {
	if len(refs) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(refs))
	var names []string
	for _, ref := range refs {
		if !seen[ref.Var] {
			seen[ref.Var] = true
			names = append(names, ref.Var)
		}
	}
	slices.Sort(names)
	return []string{
		fmt.Sprintf("# WARNING: the gateway config references environment variable(s) %s. An MCP client launches the gateway with the client's OWN environment — your shell's variables are NOT inherited.",
			strings.Join(names, ", ")),
		"# WARNING: make sure these variables are set where the client runs the gateway (e.g. add an \"env\" block to the mcpServers entry), or the gateway will start with those secrets empty and no error anywhere.",
	}
}

// gatewayLaunchArgs builds the argv tail for the stdio snippets.
//
// "-c" is emitted ONLY when the operator actually passed --config. With no
// --config the gateway looks for config.yaml next to its own binary, which
// resolves to the SAME file from whatever working directory the client spawns
// it in — so pinning the path adds nothing and goes stale as soon as the pair
// binary+config is moved or reinstalled. That lookup is config.ResolvePath /
// config.DefaultConfigName; do NOT "simplify" this function into a call to
// config.ResolvePath, as its whole point is to print NOTHING in that case,
// while ResolvePath returns the resolved default path.
//
// When --config was given, the value is made absolute for the mirror-image
// reason: a relative path would be resolved against the client's cwd, not the
// operator's. config.Config does not remember where it was loaded from, so the
// path is resolved here from the flag.
func gatewayLaunchArgs(configFlag string) []string {
	if configFlag == "" {
		return []string{"serve"}
	}
	abs, err := filepath.Abs(configFlag)
	if err != nil {
		// Abs only fails when the cwd is unobtainable; the operator's own
		// spelling is still the best guess left.
		abs = configFlag
	}
	return []string{"serve", "-c", abs}
}
