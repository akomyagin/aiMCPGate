// Package cli assembles the mcp-gate command tree (cobra): serve, logs,
// version. It is the thin seam between the process entry point (cmd/main.go)
// and the gateway internals — main.go only builds the root command and executes
// it (SKILL §1: one file per command, shared scaffold in root.go).
package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
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
	root.AddCommand(newDoctorCmd(version))
	root.AddCommand(newLogsCmd())
	root.AddCommand(newVersionCmd(version))
	root.AddCommand(newTokenCmd())
	root.AddCommand(newClientConfigCmd())
	root.AddCommand(newSkillCmd())
	root.AddCommand(newDemoServerCmd(version))
	return root
}

// addConfigFlag registers the shared --config/-c flag on cmd and returns a
// pointer to the bound path variable. Every config-reading command registers
// the flag through this helper so the name, shorthand and help text stay
// identical across the whole command tree.
func addConfigFlag(cmd *cobra.Command) *string {
	var configPath string
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to the YAML config file")
	return &configPath
}

// loadConfig is the shared config-loading prologue: config.Load plus the
// uniform error prefix every command used to spell out by hand. The original
// error is wrapped with %w, so callers that need to distinguish failure kinds
// (e.g. errors.Is(err, fs.ErrNotExist)) still can.
func loadConfig(path string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}
