package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
)

// newLogsCmd reads and filters the JSON-lines tool-call log written by the
// gateway (logging.CallRecord). This is the Фаза 2 log viewer in its simplest
// form — a terminal command, which the plan explicitly allows in lieu of a web
// view ("CLI-команда истории ИЛИ минимальный веб-вью"). It only reads; it never
// touches the running gateway.
func newLogsCmd() *cobra.Command {
	var (
		configPath   string
		file         string
		tail         int
		upstreamFilt string
		toolFilt     string
		statusFilt   string
	)
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Show recent tool-call log records, with optional filters",
		Long: "logs reads the JSON-lines call log the gateway writes (log_file in the\n" +
			"config, or stderr if unset — pass --file to read a specific file). It prints\n" +
			"the most recent records, optionally filtered by upstream, tool, or ok status.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := resolveLogFile(file, configPath)
			if err != nil {
				return err
			}
			ok, err := parseStatus(statusFilt)
			if err != nil {
				return err
			}
			return runLogs(cmd, path, tail, upstreamFilt, toolFilt, ok)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "config file to read log_file from (if --file is not given)")
	cmd.Flags().StringVarP(&file, "file", "f", "", "path to the JSON-lines call log (overrides config's log_file)")
	cmd.Flags().IntVarP(&tail, "tail", "n", 50, "show at most the last N matching records (0 = all)")
	cmd.Flags().StringVar(&upstreamFilt, "upstream", "", "only records for this upstream")
	cmd.Flags().StringVar(&toolFilt, "tool", "", "only records for this tool (namespaced name)")
	cmd.Flags().StringVar(&statusFilt, "status", "", "filter by outcome: ok | err (default: all)")
	return cmd
}

// resolveLogFile picks the log path: an explicit --file wins; otherwise the
// log_file from --config. An empty result means the gateway logged to stderr,
// which cannot be read back — reported as an actionable error.
func resolveLogFile(file, configPath string) (string, error) {
	if file != "" {
		return file, nil
	}
	if configPath == "" {
		return "", fmt.Errorf("no log file: pass --file, or --config pointing at a config with log_file set")
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return "", fmt.Errorf("load config: %w", err)
	}
	if cfg.LogFile == "" {
		return "", fmt.Errorf("config %q has no log_file (the gateway logged to stderr, which cannot be read back)", configPath)
	}
	return cfg.LogFile, nil
}

// parseStatus maps the --status flag to an optional bool: nil = no filter,
// *true = only ok, *false = only errors.
func parseStatus(s string) (*bool, error) {
	switch strings.ToLower(s) {
	case "":
		return nil, nil
	case "ok":
		t := true
		return &t, nil
	case "err", "error", "fail":
		f := false
		return &f, nil
	default:
		return nil, fmt.Errorf("invalid --status %q (want ok | err)", s)
	}
}

func runLogs(cmd *cobra.Command, path string, tail int, upstreamFilt, toolFilt string, okFilt *bool) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open call log %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	records, err := logging.ReadRecords(f)
	if err != nil {
		return fmt.Errorf("read call log %q: %w", path, err)
	}

	filtered := records[:0]
	for _, rec := range records {
		if upstreamFilt != "" && rec.Upstream != upstreamFilt {
			continue
		}
		if toolFilt != "" && rec.Tool != toolFilt {
			continue
		}
		if okFilt != nil && rec.OK != *okFilt {
			continue
		}
		filtered = append(filtered, rec)
	}

	if tail > 0 && len(filtered) > tail {
		filtered = filtered[len(filtered)-tail:]
	}

	for _, rec := range filtered {
		cmd.Println(formatRecord(rec))
	}
	return nil
}

// formatRecord renders one record as a compact, human-readable line. It never
// prints call arguments (they are not stored in the record — see CallRecord),
// so no secret can appear here.
func formatRecord(rec logging.CallRecord) string {
	status := "ok"
	if !rec.OK {
		status = "ERR"
	}
	line := fmt.Sprintf("%s  %-4s  %-12s  %-18s  %s  %s",
		rec.Time.Format(time.RFC3339),
		status,
		rec.Upstream,
		rec.Method,
		rec.Tool,
		durMS(rec.Duration),
	)
	if rec.Err != "" {
		line += "  error=" + strconv.Quote(rec.Err)
	}
	return line
}

func durMS(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}
