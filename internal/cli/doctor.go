package cli

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// newDoctorCmd runs one diagnostic bring-up pass over every enabled upstream
// (launch → handshake → tools/list, exactly what serve does on start) and
// prints a per-upstream OK/FAIL table. Unlike serve it never supervises or
// auto-restarts anything: a flapping upstream must be REPORTED as it is, not
// resurrected mid-diagnosis (Stage 8).
func newDoctorCmd(version string) *cobra.Command {
	var (
		configPath *string
		envFile    *string
	)
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check every enabled upstream once and report OK/FAIL per upstream",
		Long: "doctor brings every enabled upstream up exactly once — launch, MCP handshake,\n" +
			"tools/list — and prints a per-upstream OK/FAIL table with the tool count or the\n" +
			"failure reason. No auto-restart, no call logging: one pass, then exit. The exit\n" +
			"code is non-zero if any upstream failed, so it is scriptable (CI, cron).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDoctor(cmd, *configPath, *envFile, version)
		},
	}
	configPath = addConfigFlag(cmd)
	envFile = addEnvFileFlag(cmd)
	return cmd
}

func runDoctor(cmd *cobra.Command, configPath, envFile, version string) error {
	// The env file must land in the environment BEFORE config.Load expands the
	// ${VAR} references in the config.
	if err := applyEnvFile(envFile); err != nil {
		return err
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}

	printUnresolvedSecretVars(cmd, cfg)

	logger := logging.New(cfg.LogLevel, os.Stderr)

	// supervise=false: one pass, no auto-restart goroutines (see the comment on
	// Registry.supervise). callLog=nil: doctor performs no tool calls, so there
	// is nothing to audit; the payload log is the no-op (empty path never errors).
	payloadLog, _ := logging.NewPayloadLog("")
	reg := registry.New(cfg, logger, nil, payloadLog, false, version)
	defer func() { _ = reg.Close() }()

	// Start's error for the all-upstreams-failed case is deliberately NOT
	// returned here: the table below already shows every failure, per upstream,
	// which is doctor's whole point — a bare summary error on top would be
	// noise. The error still matters for the causes the table cannot show:
	// nothing enabled at all, or the context cancelled mid-pass.
	startErr := reg.Start(cmd.Context())

	report := reg.StartReport()
	printDoctorReport(cmd, report)

	failed := 0
	for _, s := range report {
		if !s.OK {
			failed++
		}
	}
	if failed > 0 {
		// Returning an error is the project-wide exit-code path: cobra prints it
		// (SilenceUsage on the root keeps usage noise out) and main exits 1.
		return fmt.Errorf("%d of %d upstream(s) failed", failed, len(report))
	}
	if startErr != nil {
		return startErr
	}
	return nil
}

// printUnresolvedSecretVars prints one line per unresolved ${VAR} reference
// in upstream env/headers. Unlike serve's journal event it also lists
// DISABLED upstreams (marked as such) — doctor's whole job is to explain a
// doubtful config, dormant hazards included. It prints to stdout: these lines
// are report content, like the table, not slog diagnostics. The exit code is
// deliberately unaffected — an unresolved variable is a warning (the decided
// behaviour: the failure itself surfaces as a 401 from the upstream), not a
// per-upstream FAIL. auth_token cannot appear here: Load already failed on it.
// Variable NAMES only, never values (they are secrets).
func printUnresolvedSecretVars(cmd *cobra.Command, cfg *config.Config) {
	out := cmd.OutOrStdout()
	enabled := make(map[string]bool, len(cfg.Upstreams))
	for _, u := range cfg.Upstreams {
		enabled[u.Name] = u.IsEnabled()
	}
	for _, ref := range cfg.SecretVarRefs {
		if ref.Resolved || ref.Field == "auth_token" {
			continue
		}
		line := fmt.Sprintf("WARN upstream %q: %s[%s] references unset environment variable %s — the value expanded to empty",
			ref.Upstream, ref.Field, ref.Key, ref.Var)
		if !enabled[ref.Upstream] {
			line += " (upstream disabled)"
		}
		fmt.Fprintln(out, line)
	}
}

// tableCellSanitizer strips the characters tabwriter treats as structure: a
// tab would open a phantom column, a newline would orphan the rest of the cell
// onto a column-less line and break the alignment of every row after it.
var tableCellSanitizer = strings.NewReplacer("\t", " ", "\n", " ", "\r", " ")

// printDoctorReport renders the per-upstream statuses as an aligned table on
// the command's stdout (slog diagnostics go to stderr, so the two never mix).
func printDoctorReport(cmd *cobra.Command, report []registry.UpstreamStatus) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "UPSTREAM\tSTATUS\tTOOLS\tREASON")
	for _, s := range report {
		status, reason := "OK", ""
		if !s.OK {
			// s.Err comes from an arbitrary error string (possibly relayed from
			// the upstream itself), so it must be sanitized before it hits the
			// table. s.Name needs no such treatment: the config regex
			// (^[a-zA-Z0-9_-]+$) already rules control characters out.
			status, reason = "FAIL", tableCellSanitizer.Replace(s.Err)
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", s.Name, status, s.Tools, reason)
	}
	_ = w.Flush()
}
