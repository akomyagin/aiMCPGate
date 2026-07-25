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
			if generate {
				tok, err := randomToken()
				if err != nil {
					return err
				}
				cmd.Println(tok)
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
			cmd.Println(cfg.AuthToken)
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
