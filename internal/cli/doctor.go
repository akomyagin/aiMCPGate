package cli

import (
	"fmt"
	"os"
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
func newDoctorCmd() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check every enabled upstream once and report OK/FAIL per upstream",
		Long: "doctor brings every enabled upstream up exactly once — launch, MCP handshake,\n" +
			"tools/list — and prints a per-upstream OK/FAIL table with the tool count or the\n" +
			"failure reason. No auto-restart, no call logging: one pass, then exit. The exit\n" +
			"code is non-zero if any upstream failed, so it is scriptable (CI, cron).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDoctor(cmd, configPath)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to the YAML config file")
	return cmd
}

func runDoctor(cmd *cobra.Command, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := logging.New(cfg.LogLevel, os.Stderr)

	// supervise=false: one pass, no auto-restart goroutines (see the comment on
	// Registry.supervise). callLog=nil: doctor performs no tool calls, so there
	// is nothing to audit.
	reg := registry.New(cfg, logger, nil, false)
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

// printDoctorReport renders the per-upstream statuses as an aligned table on
// the command's stdout (slog diagnostics go to stderr, so the two never mix).
func printDoctorReport(cmd *cobra.Command, report []registry.UpstreamStatus) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "UPSTREAM\tSTATUS\tTOOLS\tREASON")
	for _, s := range report {
		status, reason := "OK", ""
		if !s.OK {
			status, reason = "FAIL", s.Err
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", s.Name, status, s.Tools, reason)
	}
	_ = w.Flush()
}
