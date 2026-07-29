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
		eventsOnly   bool
	)
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Show recent tool-call log records, with optional filters",
		Long: "logs reads the JSON-lines journal the gateway writes (log_file in the\n" +
			"config, or stderr if unset — pass --file to read a specific file). The journal\n" +
			"holds two kinds of line: tool CALLS and operator EVENTS (an upstream that\n" +
			"failed to start, dropped notifications, catalog collisions, a result the size\n" +
			"limit could not truncate). Both are printed in journal order; event lines are\n" +
			"marked EVT. It prints the most recent records, optionally filtered by upstream,\n" +
			"tool, or ok status — --tool and --status are call-only, so events are excluded\n" +
			"while either is set, and --events shows events alone.\n" +
			"--follow keeps watching the file and prints new records as they are appended;\n" +
			"--stats aggregates ALL matching records into a per-(upstream, tool) table\n" +
			"(count, error rate, p50/p95 latency) instead of printing them, followed by a\n" +
			"per-event table when the journal has events.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := resolveLogFile(file, *configPath)
			if err != nil {
				return err
			}
			ok, err := parseStatus(statusFilt)
			if err != nil {
				return err
			}
			filt := recordFilter{upstream: upstreamFilt, tool: toolFilt, ok: ok, eventsOnly: eventsOnly}
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
	cmd.Flags().StringVar(&upstreamFilt, "upstream", "", "only records for this upstream (applies to calls and events alike)")
	cmd.Flags().StringVar(&toolFilt, "tool", "", "only records for this tool (namespaced name); call-only — operator events are excluded while it is set")
	cmd.Flags().StringVar(&statusFilt, "status", "", "filter by outcome: ok | err (default: all); call-only — operator events are excluded while it is set")
	cmd.Flags().BoolVar(&eventsOnly, "events", false, "show ONLY operator events (upstream failures, dropped notifications, catalog collisions…), not tool calls")
	cmd.Flags().BoolVar(&follow, "follow", false, "after printing the tail, keep watching the file and print new records as they are appended (Ctrl-C to stop)")
	cmd.Flags().BoolVar(&stats, "stats", false, "aggregate all matching records per (upstream, tool): count, error rate, p50/p95 latency (--tail is ignored)")
	cmd.MarkFlagsMutuallyExclusive("follow", "stats")
	// --tool/--status are call-only filters, so combining them with --events
	// could only ever produce an empty result: reject the combination instead.
	cmd.MarkFlagsMutuallyExclusive("events", "tool")
	cmd.MarkFlagsMutuallyExclusive("events", "status")
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

// recordFilter is the shared --upstream/--tool/--status/--events predicate: the
// plain listing, --follow's live watch and --stats all apply exactly the same
// one, to both kinds of journal line.
type recordFilter struct {
	upstream   string
	tool       string
	ok         *bool // nil = no filter, else must equal rec.OK
	eventsOnly bool  // --events: drop call records entirely
}

func (f recordFilter) match(rec logging.CallRecord) bool {
	if f.eventsOnly {
		return false
	}
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

// matchEvent is match's counterpart for operator events (Stage 18). --upstream
// applies to both kinds; --tool and --status are call-only, and a call-only
// filter EXCLUDES events rather than passing them through: a filter on a field
// an event does not have cannot be answered honestly either way, and silently
// showing everything would make `--status err` look like it matched events too.
func (f recordFilter) matchEvent(e logging.EventRecord) bool {
	if f.tool != "" || f.ok != nil {
		return false
	}
	if f.upstream != "" && e.Upstream != f.upstream {
		return false
	}
	return true
}

// matchEntry applies the right half of the filter to one journal line.
func (f recordFilter) matchEntry(e logging.Entry) bool {
	switch {
	case e.Call != nil:
		return f.match(*e.Call)
	case e.Event != nil:
		return f.matchEvent(*e.Event)
	default:
		return false
	}
}

// filterEntries keeps the matching lines, preserving journal order. Both kinds
// are counted by --tail together: it is the tail of the JOURNAL, not of calls.
func filterEntries(entries []logging.Entry, filt recordFilter, tail int) []logging.Entry {
	out := entries[:0]
	for _, e := range entries {
		if filt.matchEntry(e) {
			out = append(out, e)
		}
	}
	if tail > 0 && len(out) > tail {
		out = out[len(out)-tail:]
	}
	return out
}

func runLogs(cmd *cobra.Command, path string, tail int, filt recordFilter) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open call log %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	entries, err := logging.ReadEntries(f)
	if err != nil {
		return fmt.Errorf("read call log %q: %w", path, err)
	}

	// OutOrStdout, not cmd.Println: cobra's Println writes to the ERROR stream,
	// which silently made `mcp-gate logs | grep` and `... > file` come back
	// empty. --follow and --stats always used stdout; the listing was the odd
	// one out.
	out := cmd.OutOrStdout()
	for _, e := range filterEntries(entries, filt, tail) {
		fmt.Fprintln(out, formatEntry(e))
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
	entries, err := logging.ReadEntries(bytes.NewReader(buf[:cut]))
	if err != nil {
		return fmt.Errorf("read call log %q: %w", path, err)
	}
	out := cmd.OutOrStdout() // same stream the follow loop below writes to
	for _, e := range filterEntries(entries, filt, tail) {
		fmt.Fprintln(out, formatEntry(e))
	}

	pending := append([]byte(nil), buf[cut:]...)
	return followLog(ctx, out, path, int64(len(buf)), pending, filt)
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
		entries, _ := logging.ReadEntries(bytes.NewReader(pending[:cut]))
		pending = append(pending[:0:0], pending[cut:]...)
		for _, e := range entries {
			if filt.matchEntry(e) {
				fmt.Fprintln(out, formatEntry(e))
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

	entries, err := logging.ReadEntries(f)
	if err != nil {
		return fmt.Errorf("read call log %q: %w", path, err)
	}

	var records []logging.CallRecord
	var events []logging.EventRecord
	for _, e := range entries {
		switch {
		case e.Call != nil && filt.match(*e.Call):
			records = append(records, *e.Call)
		case e.Event != nil && filt.matchEvent(*e.Event):
			events = append(events, *e.Event)
		}
	}

	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
	// The call table is printed exactly as before — on a journal without events
	// the whole output is byte-for-byte what pre-Stage-18 `logs --stats` gave.
	// --events asks for events only, so the call table is skipped entirely.
	if !filt.eventsOnly {
		fmt.Fprintln(w, "UPSTREAM\tTOOL\tCOUNT\tERROR%\tP50\tP95")
		for _, s := range aggregateStats(records, filt) {
			fmt.Fprintf(w, "%s\t%s\t%d\t%.1f%%\t%s\t%s\n",
				s.upstream, s.tool, s.count, s.errRate(), durMS(s.p50), durMS(s.p95))
		}
	}
	switch {
	case len(events) > 0:
		if !filt.eventsOnly {
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w, "EVENT\tUPSTREAM\tSUBJECT\tCOUNT\tLAST")
		for _, s := range aggregateEventStats(events) {
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n",
				s.event, s.upstream, printableField(s.subject), s.count, s.last.Format(time.RFC3339))
		}
	case filt.eventsOnly:
		// --events --stats on a journal with no matching events would otherwise
		// print ABSOLUTELY nothing (the call table is skipped, the event table is
		// empty) — indistinguishable from a command that silently broke. Only in
		// this branch: without --events the call table above is the output, and
		// adding a line to it would break the byte-for-byte compatibility of
		// pre-Stage-18 `logs --stats`.
		fmt.Fprintln(w, "no events")
	}
	return w.Flush()
}

// eventStats is one row of the --stats events table: the aggregate over every
// matching event line of one (event, upstream, subject) key.
type eventStats struct {
	event    string
	upstream string
	subject  string
	count    int
	last     time.Time
}

// aggregateEventStats groups events by (event, upstream, subject) and sums their
// occurrences. A line's Count is the number of occurrences COALESCED into it, so
// an absent (zero) count means one occurrence — counting it as zero would
// under-report exactly the floods the throttle exists for.
func aggregateEventStats(events []logging.EventRecord) []eventStats {
	type key struct{ event, upstream, subject string }
	groups := map[key]*eventStats{}
	for _, e := range events {
		k := key{e.Event, e.Upstream, e.Subject}
		g := groups[k]
		if g == nil {
			g = &eventStats{event: e.Event, upstream: e.Upstream, subject: e.Subject}
			groups[k] = g
		}
		n := e.Count
		if n < 1 {
			n = 1
		}
		g.count += n
		if e.Time.After(g.last) {
			g.last = e.Time
		}
	}
	out := make([]eventStats, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].event != out[j].event {
			return out[i].event < out[j].event
		}
		if out[i].upstream != out[j].upstream {
			return out[i].upstream < out[j].upstream
		}
		return out[i].subject < out[j].subject
	})
	return out
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

// formatEntry renders one journal line, whichever kind it is.
func formatEntry(e logging.Entry) string {
	if e.Event != nil {
		return formatEvent(*e.Event)
	}
	if e.Call != nil {
		return formatRecord(*e.Call)
	}
	return ""
}

// formatEvent renders one operator event, aligned like formatRecord with EVT
// where a call line carries ok/ERR. Events never carry payloads (see
// logging.EventRecord), so no secret can appear here either. count is printed
// only when the line coalesces more than one occurrence.
func formatEvent(e logging.EventRecord) string {
	line := fmt.Sprintf("%s  %-4s  %-12s  %-18s  %s",
		e.Time.Format(time.RFC3339),
		"EVT",
		e.Upstream,
		e.Event,
		printableField(e.Subject),
	)
	if e.Detail != "" {
		line += "  detail=" + strconv.Quote(e.Detail)
	}
	if e.Count > 1 {
		line += fmt.Sprintf("  count=%d", e.Count)
	}
	return line
}

// printableField makes a journal-supplied string safe to put on a terminal line.
// An event's Subject is NOT gateway-authored the way its Detail is: for
// catalog_collision it can be a resource URI and for catalog_bad_template a URI
// template, both taken verbatim from an upstream and — unlike a tool name —
// never normalized by namespacing. A "\n" in one would forge a second output
// line indistinguishable from a real journal entry, and an ESC would repaint the
// operator's terminal (found by independent review, M3).
//
// Strings made entirely of printable runes are returned VERBATIM, so the normal
// output keeps its exact former shape; only a string that actually carries a
// control character pays the strconv.Quote escaping. Deliberately not applied to
// formatRecord's rec.Tool: that is a pre-existing class, left as named debt.
func printableField(s string) string {
	for _, r := range s {
		if !strconv.IsPrint(r) {
			return strconv.Quote(s)
		}
	}
	return s
}

func durMS(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}
