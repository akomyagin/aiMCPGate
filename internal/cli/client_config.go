package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
				printHTTPClientConfig(cmd, cfg)
				return nil
			}
			printStdioClientConfig(cmd, *configPath)
			return nil
		},
	}
	configPath = addConfigFlag(cmd)
	return cmd
}

// section prints one titled separator line, padded to a fixed width so the
// output reads as a list of blocks rather than a wall of text.
func section(w io.Writer, title string) {
	const width = 78
	line := "─── " + title + " "
	if n := width - len([]rune(line)); n > 0 {
		line += strings.Repeat("─", n)
	}
	fmt.Fprintln(w, line)
}

// claudeDesktopPaths lists where Claude Desktop — a DIFFERENT product from
// Claude Code, with its own config file — keeps its MCP servers. Naming the
// per-OS paths beats naming one: the operator's OS is unknown here, and the
// previous single path ("~/.claude/claude_desktop_config.json") existed on
// none of them: it was a chimera of the two products' file names.
const claudeDesktopPaths = "  macOS    ~/Library/Application Support/Claude/claude_desktop_config.json\n" +
	"  Windows  %APPDATA%\\Claude\\claude_desktop_config.json\n" +
	"  Linux    ~/.config/Claude/claude_desktop_config.json"

// snippetJSON wraps one server entry into the mcpServers object every client
// here consumes, formatted for pasting.
func snippetJSON(entry map[string]any) string {
	out, _ := json.MarshalIndent(map[string]any{
		"mcpServers": map[string]any{"mcp-gate": entry},
	}, "", "  ")
	return string(out)
}

// printHTTPClientConfig points clients at an already-running gateway endpoint.
func printHTTPClientConfig(cmd *cobra.Command, cfg *config.Config) {
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
	snippet := snippetJSON(entry)

	// The warning goes to STDERR so piping the JSON output stays clean;
	// the snippets below embed the auth_token verbatim, which makes the
	// command's whole output secret-bearing.
	if cfg.AuthToken != "" {
		fmt.Fprintln(cmd.ErrOrStderr(), "# WARNING: the snippet below contains your auth_token — treat it as a secret (don't paste it into a shared chat, public gist, or commit it to git).")
	}

	w := cmd.OutOrStdout()
	section(w, "Claude Code  (recommended: claude mcp add)")
	claudeCmd := fmt.Sprintf("claude mcp add --transport http mcp-gate %s", url)
	if cfg.AuthToken != "" {
		claudeCmd += fmt.Sprintf(` --header "Authorization: Bearer %s"`, cfg.AuthToken)
	}
	fmt.Fprintln(w, claudeCmd)
	fmt.Fprintln(w)

	section(w, "Claude Code  (.mcp.json in the project root)")
	fmt.Fprintln(w, "Merge into the existing file or create it if absent:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, snippet)

	section(w, "Cursor  (.cursor/mcp.json in project or ~/.cursor/mcp.json)")
	fmt.Fprintln(w, "Same JSON format:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, snippet)

	// No JSON snippet for Desktop on purpose: it speaks stdio only, so a
	// correct-looking http entry would fail at runtime no matter which file it
	// is pasted into. Saying so is more useful than a snippet that cannot work.
	section(w, "Claude Desktop  (HTTP is not supported directly)")
	fmt.Fprintln(w, "Claude Desktop launches MCP servers as local processes and cannot call an")
	fmt.Fprintln(w, "HTTP endpoint on its own. Either bridge it with mcp-remote, or run a second")
	fmt.Fprintln(w, "gateway config with transport: stdio and re-run this command to get a snippet.")

	if cfg.AuthToken == "" {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "⚠  auth_token is not set — the endpoint is open to anyone who can reach it.")
		fmt.Fprintln(w, "   Run `mcp-gate token --generate` to create a token and add it to your config.")
	}
}

// printStdioClientConfig prints snippets that LAUNCH the gateway: in stdio mode
// there is no endpoint to point at, the client owns the process.
//
// No auth_token appears anywhere below — it only guards the HTTP endpoint, so
// in stdio mode the output is not secret-bearing and needs no warning.
func printStdioClientConfig(cmd *cobra.Command, configFlag string) {
	bin := gatewayBinaryPath()
	args := gatewayLaunchArgs(configFlag)

	// "type": "stdio" is spelled out for Claude Code, which writes it; Claude
	// Desktop and Cursor infer it from the presence of "command" and are given
	// the plain form.
	withType := map[string]any{"type": "stdio", "command": bin, "args": args}
	plain := map[string]any{"command": bin, "args": args}

	w := cmd.OutOrStdout()
	section(w, "Claude Code  (recommended: claude mcp add)")
	fmt.Fprintln(w, "claude mcp add mcp-gate -- "+strings.Join(append([]string{bin}, args...), " "))
	fmt.Fprintln(w)

	section(w, "Claude Code  (.mcp.json in the project root)")
	fmt.Fprintln(w, "Merge into the existing file or create it if absent:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, snippetJSON(withType))

	section(w, "Cursor  (.cursor/mcp.json in project or ~/.cursor/mcp.json)")
	fmt.Fprintln(w, "Same JSON format:")
	fmt.Fprintln(w)
	fmt.Fprintln(w, snippetJSON(plain))

	section(w, "Claude Desktop  (restart the app after editing)")
	fmt.Fprintln(w, claudeDesktopPaths)
	fmt.Fprintln(w)
	fmt.Fprintln(w, snippetJSON(plain))
}

// gatewayBinaryPath returns the ABSOLUTE path of the running binary, and that
// absoluteness is the point: an MCP client spawns the gateway itself, not from
// the operator's login shell, so the PATH that makes a bare `mcp-gate` work in
// a terminal may simply not be there. A failing os.Executable is not worth
// aborting the command over — the bare name still helps whoever does have the
// binary on PATH, and is obviously editable.
func gatewayBinaryPath() string {
	exe, err := os.Executable()
	if err != nil {
		return "mcp-gate"
	}
	return exe
}

// gatewayLaunchArgs builds the argv tail for the stdio snippets.
//
// "-c" is emitted ONLY when the operator actually passed --config. With no
// --config the gateway looks for config.yaml next to its own binary
// (config.defaultConfigPath), which resolves to the SAME file from whatever
// working directory the client spawns it in — so pinning the path adds nothing
// and goes stale as soon as the pair binary+config is moved or reinstalled.
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
