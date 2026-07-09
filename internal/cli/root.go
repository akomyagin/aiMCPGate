// Package cli assembles the mcp-gate command tree (cobra): serve, logs,
// version. It is the thin seam between the process entry point (cmd/main.go)
// and the gateway internals — main.go only builds the root command and executes
// it (SKILL §1: one file per command, shared scaffold in root.go).
package cli

import (
	"github.com/spf13/cobra"
)

// Build assembles the root command and its subcommands. version is the build
// version (from -ldflags in main), threaded through so the client's serverInfo
// and the `version` command report the real binary version.
func Build(version string) *cobra.Command {
	root := &cobra.Command{
		Use:   "mcp-gate",
		Short: "MCP gateway multiplexing several upstream MCP servers behind one endpoint",
		Long: "aiMCPGate presents a single MCP endpoint to a client (e.g. Claude Code) and\n" +
			"multiplexes tool/resource calls across several upstream MCP servers, aggregating\n" +
			"their catalogs into one and logging every call that flows through.",
		SilenceUsage: true,
	}
	root.AddCommand(newServeCmd(version))
	root.AddCommand(newLogsCmd())
	root.AddCommand(newVersionCmd(version))
	root.AddCommand(newTokenCmd())
	root.AddCommand(newClientConfigCmd())
	root.AddCommand(newSkillCmd())
	return root
}
