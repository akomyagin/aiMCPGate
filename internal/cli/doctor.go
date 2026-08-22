package cli

import (
	"fmt"
	"os"
	"runtime"
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

	// Resolved AFTER loadConfig succeeded, so config.ResolvePath's own failure
	// mode (os.Executable, only reachable when configPath == "") already would
	// have surfaced as loadConfig's error above — this call is now infallible
	// in practice. envFile needs no resolution: applyEnvFile reads it as given.
	if resolvedConfigPath, rerr := config.ResolvePath(configPath); rerr == nil {
		checkFilePerms(cmd, resolvedConfigPath, envFile)
	}

	printUnresolvedSecretVars(cmd, cfg)
	printCleartextSecretWarnings(cmd, cfg)

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

// checkFilePerms prints one WARN per path that is readable by group or others
// under a POSIX permission model (security brainstorm 2026-08-20, S7):
// config.yaml MAY hold a secret literal (env-substitution is a convention,
// not enforced), and --env-file exists specifically to hold secrets. Neither
// is checked anywhere else — the operator sets the file's mode once at
// creation and nothing revisits it afterward.
//
// Windows is skipped entirely, not just "checked leniently": os.FileMode's
// Unix permission bits there are a Go-synthesized approximation of the real
// ACL (typically all-or-nothing read/write, unrelated to actual per-user
// access control) — reporting them as if they meant something would be
// actively misleading, worse than saying nothing.
//
// A missing/unreadable path is silently skipped — that is a DIFFERENT
// problem doctor already surfaces elsewhere (loadConfig's own error for
// config.yaml; applyEnvFile's for --env-file), not this check's job. A
// directory is skipped too: the "consider chmod 600" wording only makes
// sense for a file, and both paths are meant to name one (a misconfigured
// --env-file pointing at a directory is a usage error, not this check's).
func checkFilePerms(cmd *cobra.Command, paths ...string) {
	if runtime.GOOS == "windows" {
		return
	}
	out := cmd.OutOrStdout()
	for _, p := range paths {
		if p == "" {
			continue
		}
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			continue
		}
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			fmt.Fprintf(out, "WARN %s is readable by group/others (mode %04o) — it may hold secrets; consider chmod 600 %s\n", p, perm, p)
		}
	}
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

// printCleartextSecretWarnings prints one line per config.CleartextSecretWarnings
// finding (security brainstorm 2026-08-20, S5/S6) — a secret travelling over
// plain HTTP to a non-loopback host. Unlike serve's Warn log, this includes
// DISABLED upstreams: doctor's whole job is to explain a doubtful config,
// dormant hazards included (mirrors printUnresolvedSecretVars). Exit code is
// unaffected — same reasoning as unresolved-var warnings: a warning, not a
// per-upstream FAIL.
func printCleartextSecretWarnings(cmd *cobra.Command, cfg *config.Config) {
	out := cmd.OutOrStdout()
	enabled := make(map[string]bool, len(cfg.Upstreams))
	for _, u := range cfg.Upstreams {
		enabled[u.Name] = u.IsEnabled()
	}
	for _, w := range cfg.CleartextSecretWarnings() {
		line := "WARN " + w.Message
		if w.Upstream != "" && !enabled[w.Upstream] {
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
