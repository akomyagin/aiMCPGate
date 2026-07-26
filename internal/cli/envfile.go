package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// addEnvFileFlag registers the shared --env-file flag on cmd and returns a
// pointer to the bound path variable — the same shape as addConfigFlag, so the
// flag name and help text stay identical across every config-reading command
// (serve, doctor, call, catalog). The file is applied by applyEnvFile BEFORE
// the config is loaded, so ${VAR} references in the config resolve against it.
func addEnvFileFlag(cmd *cobra.Command) *string {
	var envFile string
	cmd.Flags().StringVar(&envFile, "env-file", "",
		"path to a .env file loaded into the process environment before reading the config")
	return &envFile
}

// applyEnvFile loads KEY=VALUE pairs from a .env-style file into the process
// environment. An empty path is a no-op (the flag was not given).
//
// Supported syntax (deliberately minimal, no third-party dotenv dependency):
//   - blank lines and lines starting with '#' (after trimming) are skipped;
//   - an optional "export " prefix before KEY=VALUE is stripped;
//   - VALUE is everything after the FIRST '=' to the end of the line, with
//     surrounding whitespace trimmed and one pair of matching single or double
//     quotes removed if the value is wrapped in them on both sides.
//
// Variables already present in the real process environment are NEVER
// overwritten: the actual environment outranks the file, so an operator's
// explicit `VAR=... mcp-gate ...` (or CI-provided secret) always wins over a
// stale value in a local .env.
func applyEnvFile(path string) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read env file: %w", err)
	}
	for i, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("env file %s: line %d: expected KEY=VALUE, got %q", path, i+1, line)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("env file %s: line %d: empty key in %q", path, i+1, line)
		}
		if _, exists := os.LookupEnv(key); exists {
			continue // the real environment outranks the file (see doc comment)
		}
		if err := os.Setenv(key, unquoteEnvValue(strings.TrimSpace(value))); err != nil {
			return fmt.Errorf("env file %s: set %s: %w", path, key, err)
		}
	}
	return nil
}

// unquoteEnvValue strips ONE pair of matching surrounding quotes ("..." or
// '...') from a .env value. Mismatched or lone quotes are kept verbatim — the
// parser must never eat characters it does not understand.
func unquoteEnvValue(v string) string {
	if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
		return v[1 : len(v)-1]
	}
	return v
}
