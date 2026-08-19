package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newVersionCmd prints the build version. The value is injected via -ldflags in
// main (Stage 6 / goreleaser) and threaded through Build.
func newVersionCmd(version string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the mcp-gate build version",
		Run: func(cmd *cobra.Command, _ []string) {
			// STDOUT, not cmd.Println: cobra resolves Println through
			// OutOrStderr(), which falls back to os.Stderr whenever no out
			// writer is set — i.e. in every real run of the binary. A version
			// number that cannot be captured with $(...) defeats the point of
			// the command; scripts pinning a version saw an empty string.
			fmt.Fprintln(cmd.OutOrStdout(), version)
		},
	}
}
