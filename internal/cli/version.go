package cli

import (
	"github.com/spf13/cobra"
)

// newVersionCmd prints the build version. The value is injected via -ldflags in
// main (Stage 6 / goreleaser) and threaded through Build.
func newVersionCmd(version string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the mcp-gate build version",
		Run: func(cmd *cobra.Command, _ []string) {
			cmd.Println(version)
		},
	}
}
