package cli

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"
)

func newTokenCmd() *cobra.Command {
	var configPath *string
	var generate bool

	cmd := &cobra.Command{
		Use:   "token",
		Short: "Show the current auth token or generate a new one",
		Long: "Reads auth_token from the config and prints it.\n" +
			"With --generate, prints a new random token (copy it to your .env as AIMCPGATE_TOKEN).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// The token itself goes to STDOUT and the guidance around it to
			// STDERR, so `TOKEN=$(mcp-gate token)` and `mcp-gate token > f`
			// capture the value and nothing else.
			//
			// NOT cmd.Println, which was the bug here: cobra resolves Println
			// through OutOrStderr(), and that falls back to os.Stderr whenever
			// no out writer was set — which is every real run of the binary. So
			// the token went to stderr, $(...) captured the empty string, and
			// redirecting to a file produced an empty file. Silent, because
			// stderr is still visible on a terminal: it only breaks the moment
			// someone tries to USE the value. Same defect class as the logs
			// listing fixed in Stage 18; this call site was missed then.
			out := cmd.OutOrStdout()
			if generate {
				tok, err := randomToken()
				if err != nil {
					return err
				}
				fmt.Fprintln(out, tok)
				cmd.PrintErrln("Copy the token above to your .env:")
				cmd.PrintErrln("  AIMCPGATE_TOKEN=" + tok)
				cmd.PrintErrln("Then set in config.yaml:")
				cmd.PrintErrln("  auth_token: ${AIMCPGATE_TOKEN}")
				return nil
			}

			cfg, err := loadConfig(*configPath)
			if err != nil {
				return err
			}
			if cfg.AuthToken == "" {
				return fmt.Errorf("auth_token is not set in config (use --generate to create one)")
			}
			fmt.Fprintln(out, cfg.AuthToken)
			return nil
		},
	}
	configPath = addConfigFlag(cmd)
	cmd.Flags().BoolVar(&generate, "generate", false, "generate and print a new random token")
	return cmd
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return hex.EncodeToString(b), nil
}
