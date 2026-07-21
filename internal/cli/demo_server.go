package cli

import (
	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/demo"
)

// newDemoServerCmd wires the hidden `__demo-echo` stub MCP server (Stage 12):
// a trivial echo upstream the gateway launches from demo.config.yaml so
// registry sandboxes (Glama.ai) can introspect the gateway without any real
// upstream. Hidden — it is an internal plumbing detail, not a user command;
// the dunder name keeps it out of anyone's way even if discovered.
func newDemoServerCmd(version string) *cobra.Command {
	return &cobra.Command{
		Use:    "__demo-echo",
		Short:  "Run the built-in demo echo MCP server (internal, for sandbox introspection)",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// cmd.Context() is already signal-cancelled (main.go wraps the root
			// context in signal.NotifyContext), so Ctrl-C / SIGTERM stop the
			// server; EOF on stdin (parent closing the pipe) does too.
			return demo.Run(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(), version)
		},
	}
}
