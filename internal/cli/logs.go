package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/logging"
)

// followPollInterval is how often --follow stats the log file for growth. A
// package variable (not a const) so tests can shrink it instead of waiting out
// real half-second ticks.
var followPollInterval = 500 * time.Millisecond

// newLogsCmd reads and filters the JSON-lines tool-call log written by the
// gateway (logging.CallRecord). This is the Phase 2 log viewer in its simplest
// form — a terminal command, which the plan explicitly allows in lieu of a web
// view ("a CLI history command OR a minimal web view"). It only reads; it never
// touches the running gateway.
func newLogsCmd() *cobra.Command {
	var (
		configPath   *string
		file         string
		tail         int
		upstreamFilt string
		toolFilt     string
		statusFilt   string
		follow       bool
		stats        bool
	)
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Show recent tool-call log records, with optional filters",
		Long: "logs reads the JSON-lines call log the gateway writes (log_file in the\n" +
			"config, or stderr if unset — pass --file to read a specific file). It prints\n" +
			"the most recent records, optionally filtered by upstream, tool, or ok status.\n" +
			"--follow keeps watching the file and prints new records as they are appended;\n" +
			"--stats aggregates ALL matching records into a per-(upstream, tool) table\n" +
			"(count, error rate, p50/p95 latency) instead of printing them.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := resolveLogFile(file, *configPath)
			if err != nil {
				return err
			}
			ok, err := parseStatus(statusFilt)
			if err != nil {
				return err
			}
			filt := recordFilter{upstream: upstreamFilt, tool: toolFilt, ok: ok}
			if stats {
				return runLogsStats(cmd, path, filt)
			}
			if follow {
				// Ctrl-C / SIGTERM ends the watch loop cleanly — the same
				// signal wiring serve uses for its whole run loop.
				ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
				defer stop()
				return runLogsFollow(ctx, cmd, path, tail, filt)
			}
			return runLogs(cmd, path, tail, filt)
		},
	}
	configPath = addConfigFlag(cmd)
	cmd.Flags().StringVarP(&file, "file", "f", "", "path to the JSON-lines call log (overrides config's log_file)")
	cmd.Flags().IntVarP(&tail, "tail", "n", 50, "show at most the last N matching records (0 = all)")
	cmd.Flags().StringVar(&upstreamFilt, "upstream", "", "only records for this upstream")
	cmd.Flags().StringVar(&toolFilt, "tool", "", "only records for this tool (namespaced name)")
	cmd.Flags().StringVar(&statusFilt, "status", "", "filter by outcome: ok | err (default: all)")
	cmd.Flags().BoolVar(&follow, "follow", false, "after printing the tail, keep watching the file and print new records as they are appended (Ctrl-C to stop)")
	cmd.Flags().BoolVar(&stats, "stats", false, "aggregate all matching records per (upstream, tool): count, error rate, p50/p95 latency (--tail is ignored)")
	cmd.MarkFlagsMutuallyExclusive("follow", "stats")
	return cmd
}

// resolveLogFile picks the log path: an explicit --file wins; otherwise the
// log_file from --config (or the default config next to the binary, per
// config.Load). An empty result means the gateway logged to stderr, which
// cannot be read back — reported as an actionable error.
func resolveLogFile(file, configPath string) (string, error) {
	if file != "" {
		return file, nil
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return "", err
	}
	if cfg.LogFile == "" {
		return "", fmt.Errorf("config has no log_file (the gateway logged to stderr, which cannot be read back)")
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

// recordFilter is the shared --upstream/--tool/--status predicate: the plain
// listing, --follow's live watch and --stats all apply exactly the same one.
type recordFilter struct {
	upstream string
	tool     string
	ok       *bool // nil = no filter, else must equal rec.OK
}

func (f recordFilter) match(rec logging.CallRecord) bool {
	if f.upstream != "" && rec.Upstream != f.upstream {
		return false
	}
	if f.tool != "" && rec.Tool != f.tool {
		return false
	}
	if f.ok != nil && rec.OK != *f.ok {
		return false
	}
	return true
}

func runLogs(cmd *cobra.Command, path string, tail int, filt recordFilter) error {
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
		if filt.match(rec) {
			filtered = append(filtered, rec)
		}
	}

	if tail > 0 && len(filtered) > tail {
		filtered = filtered[len(filtered)-tail:]
	}

	for _, rec := range filtered {
		cmd.Println(formatRecord(rec))
	}
	return nil
}

// runLogsFollow prints the current tail exactly like runLogs, then keeps
// watching the file (followLog) and prints each newly appended matching record
// until ctx is cancelled (Ctrl-C).
func runLogsFollow(ctx context.Context, cmd *cobra.Command, path string, tail int, filt recordFilter) error {
	// Snapshot the file in one read so the follow loop's starting offset is
	// exactly the end of what was printed — a record appended between "read"
	// and "start following" is never lost or duplicated. A trailing partial
	// line (writer caught mid-append) is NOT parsed here; it is carried into
	// the follow loop as pending bytes and printed once its newline arrives.
	buf, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("open call log %q: %w", path, err)
	}
	cut := bytes.LastIndexByte(buf, '\n') + 1 // 0 when no newline at all
	records, err := logging.ReadRecords(bytes.NewReader(buf[:cut]))
	if err != nil {
		return fmt.Errorf("read call log %q: %w", path, err)
	}
	filtered := records[:0]
	for _, rec := range records {
		if filt.match(rec) {
			filtered = append(filtered, rec)
		}
	}
	if tail > 0 && len(filtered) > tail {
		filtered = filtered[len(filtered)-tail:]
	}
	for _, rec := range filtered {
		cmd.Println(formatRecord(rec))
	}

	pending := append([]byte(nil), buf[cut:]...)
	return followLog(ctx, cmd.OutOrStdout(), path, int64(len(buf)), pending, filt)
}

// followLog is --follow's watch loop: every followPollInterval it stats path
// and, when the file grew, reads the new bytes from offset and prints each
// COMPLETE matching record. A trailing partial line (the gateway caught
// mid-append) is buffered in pending until a later poll delivers its newline.
// A file that SHRANK was truncated/rotated by an external tool: reopen from
// the beginning (offset 0), dropping any pending bytes — they belonged to the
// old incarnation. Returns when ctx is cancelled.
func followLog(ctx context.Context, out io.Writer, path string, offset int64, pending []byte, filt recordFilter) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(followPollInterval):
		}
		fi, err := os.Stat(path)
		if err != nil {
			continue // mid-rotation window; keep polling
		}
		size := fi.Size()
		if size == offset {
			continue
		}
		if size < offset {
			offset, pending = 0, nil
		}
		chunk, err := readFileRange(path, offset, size)
		if err != nil {
			continue // transient (file swapped under us); re-stat next tick
		}
		offset += int64(len(chunk))
		pending = append(pending, chunk...)
		cut := bytes.LastIndexByte(pending, '\n') + 1
		if cut == 0 {
			continue // still no complete line; keep buffering
		}
		records, _ := logging.ReadRecords(bytes.NewReader(pending[:cut]))
		pending = append(pending[:0:0], pending[cut:]...)
		for _, rec := range records {
			if filt.match(rec) {
				fmt.Fprintln(out, formatRecord(rec))
			}
		}
	}
}

// readFileRange reads bytes [offset, end) of path. The end bound comes from
// the caller's os.Stat, so one poll never reads past the size it decided on —
// bytes appended mid-read are picked up by the next tick instead.
func readFileRange(path string, offset, end int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(io.LimitReader(f, end-offset))
}

// callStats is one --stats row: the aggregate over every matching record of
// one (upstream, tool) pair.
type callStats struct {
	upstream string
	tool     string
	count    int
	errCount int
	p50, p95 time.Duration
}

// errRate returns the percentage of failed (!OK) records in the group.
func (s callStats) errRate() float64 {
	return float64(s.errCount) / float64(s.count) * 100
}

// runLogsStats reads ALL records (no --tail), applies the same filters as the
// plain listing, and prints one aggregate row per (upstream, tool): count,
// error rate, p50/p95 duration — the same tabwriter table style doctor uses.
func runLogsStats(cmd *cobra.Command, path string, filt recordFilter) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open call log %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	records, err := logging.ReadRecords(f)
	if err != nil {
		return fmt.Errorf("read call log %q: %w", path, err)
	}

	stats := aggregateStats(records, filt)
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "UPSTREAM\tTOOL\tCOUNT\tERROR%\tP50\tP95")
	for _, s := range stats {
		fmt.Fprintf(w, "%s\t%s\t%d\t%.1f%%\t%s\t%s\n",
			s.upstream, s.tool, s.count, s.errRate(), durMS(s.p50), durMS(s.p95))
	}
	return w.Flush()
}

// aggregateStats groups the matching records by (upstream, tool) and computes
// count, error count and p50/p95 duration per group. Rows are sorted by
// upstream then tool, so the output is deterministic. Percentiles are the
// sorted-slice index floor(q*len) — a group always has len >= 1 by
// construction (it exists only because a record landed in it), and q < 1
// keeps the index strictly below len.
func aggregateStats(records []logging.CallRecord, filt recordFilter) []callStats {
	type key struct{ upstream, tool string }
	type agg struct {
		count, errs int
		durs        []time.Duration
	}
	groups := make(map[key]*agg)
	for _, rec := range records {
		if !filt.match(rec) {
			continue
		}
		k := key{rec.Upstream, rec.Tool}
		g := groups[k]
		if g == nil {
			g = &agg{}
			groups[k] = g
		}
		g.count++
		if !rec.OK {
			g.errs++
		}
		g.durs = append(g.durs, rec.Duration)
	}

	out := make([]callStats, 0, len(groups))
	for k, g := range groups {
		sort.Slice(g.durs, func(i, j int) bool { return g.durs[i] < g.durs[j] })
		out = append(out, callStats{
			upstream: k.upstream,
			tool:     k.tool,
			count:    g.count,
			errCount: g.errs,
			p50:      g.durs[int(0.5*float64(len(g.durs)))],
			p95:      g.durs[int(0.95*float64(len(g.durs)))],
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].upstream != out[j].upstream {
			return out[i].upstream < out[j].upstream
		}
		return out[i].tool < out[j].tool
	})
	return out
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
