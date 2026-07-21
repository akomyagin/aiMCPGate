package cli

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

func newClientConfigCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "client-config",
		Short: "Print MCP client configuration snippets for Claude Code, Cursor and others",
		Long: "Reads listen_addr and auth_token from the gateway config and prints ready-to-use\n" +
			"JSON snippets for adding aiMCPGate to Claude Code, Cursor, and other MCP clients.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			if cfg.Transport != config.TransportHTTP {
				return fmt.Errorf("client-config is for transport: http; current transport is %q", cfg.Transport)
			}

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

			snippet := map[string]any{
				"mcpServers": map[string]any{
					"mcp-gate": entry,
				},
			}
			out, _ := json.MarshalIndent(snippet, "", "  ")

			// The warning goes to STDERR so piping the JSON output stays clean;
			// the snippets below embed the auth_token verbatim, which makes the
			// command's whole output secret-bearing.
			if cfg.AuthToken != "" {
				fmt.Fprintln(cmd.ErrOrStderr(), "# WARNING: the snippet below contains your auth_token — treat it as a secret (don't paste it into a shared chat, public gist, or commit it to git).")
			}

			w := cmd.OutOrStdout()
			fmt.Fprintln(w, "─── Claude Code  (~/.claude/claude_desktop_config.json) ───────────────────")
			fmt.Fprintln(w, "Merge into the existing file or create it if absent:")
			fmt.Fprintln(w)
			fmt.Fprintln(w, string(out))

			fmt.Fprintln(w, "─── Cursor  (.cursor/mcp.json in project or ~/  ) ─────────────────────────")
			fmt.Fprintln(w, "Same JSON format:")
			fmt.Fprintln(w)
			fmt.Fprintln(w, string(out))

			fmt.Fprintln(w, "─── Claude Code CLI (claude mcp add) ───────────────────────────────────────")
			claudeCmd := fmt.Sprintf("claude mcp add --transport http mcp-gate %s", url)
			if cfg.AuthToken != "" {
				claudeCmd += fmt.Sprintf(` --header "Authorization: Bearer %s"`, cfg.AuthToken)
			}
			fmt.Fprintln(w, claudeCmd)

			if cfg.AuthToken == "" {
				fmt.Fprintln(w)
				fmt.Fprintln(w, "⚠  auth_token is not set — the endpoint is open to anyone who can reach it.")
				fmt.Fprintln(w, "   Run `mcp-gate token --generate` to create a token and add it to your config.")
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to the YAML config file")
	return cmd
}
