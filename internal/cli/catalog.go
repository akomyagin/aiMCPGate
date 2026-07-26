package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// bytesPerToken is the divisor for the rough bytes→tokens estimate the report
// prints (~4 bytes per token for English JSON). It is an estimate, not a
// tokenizer — every ~TOKENS column is explicitly labeled approximate.
const bytesPerToken = 4

// newCatalogCmd reports how much of the client's context window the aggregated
// tool catalog costs: per-upstream tool counts with byte/approx-token sums, the
// N heaviest individual tools, and the grand total. Same one-pass bring-up as
// doctor — no supervision, no calls, print and exit (Round 10).
func newCatalogCmd(version string) *cobra.Command {
	var (
		configPath *string
		envFile    *string
		top        int
	)
	cmd := &cobra.Command{
		Use:   "catalog",
		Short: "Report the aggregated catalog size per upstream in bytes and approximate tokens",
		Long: "catalog brings every enabled upstream up once (no supervision, no tool calls),\n" +
			"measures each aggregated tool entry exactly as tools/list would serialize it,\n" +
			"and prints a per-upstream size table, the heaviest individual tools (--top) and\n" +
			"the catalog total. Token counts are approximate (bytes/4) — a quick way to see\n" +
			"what the catalog costs in a client's context window.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCatalog(cmd, *configPath, *envFile, version, top)
		},
	}
	configPath = addConfigFlag(cmd)
	envFile = addEnvFileFlag(cmd)
	cmd.Flags().IntVar(&top, "top", 10, "how many heaviest individual tools to list (0 disables the section)")
	return cmd
}

func runCatalog(cmd *cobra.Command, configPath, envFile, version string, top int) error {
	// The env file must land in the environment BEFORE config.Load expands the
	// ${VAR} references in the config.
	if err := applyEnvFile(envFile); err != nil {
		return err
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}

	logger := logging.New(cfg.LogLevel, os.Stderr)

	// Same one-shot registry shape as doctor: supervise=false, no call journal
	// (nothing is called), no-op payload log.
	payloadLog, _ := logging.NewPayloadLog("")
	reg := registry.New(cfg, logger, nil, payloadLog, false, version)
	defer func() { _ = reg.Close() }()

	// Start errors only when NOTHING came up (or nothing is enabled) — no
	// catalog to measure then. Partial failures are survivable: the table
	// covers the upstreams that did start (their failures are already logged).
	if err := reg.Start(cmd.Context()); err != nil {
		return err
	}

	printCatalogReport(cmd, reg.Tools(), top)
	return nil
}

// toolSize is one measured catalog entry: the client-facing name and the size
// of its tools/list JSON serialization.
type toolSize struct {
	name  string
	bytes int
}

// printCatalogReport renders the size report for the aggregated catalog:
// per-upstream table, the top-N heaviest tools, then the grand total.
func printCatalogReport(cmd *cobra.Command, descs []registry.ToolDescriptor, top int) {
	perUpstreamTools := map[string]int{}
	perUpstreamBytes := map[string]int{}
	sizes := make([]toolSize, 0, len(descs))
	totalBytes := 0
	for _, d := range descs {
		// Measure the entry exactly as handleToolsList serializes it for the
		// client: the upstream's verbatim schema under the namespaced name.
		t := d.Tool
		t.Name = d.Name
		raw, err := json.Marshal(t)
		if err != nil {
			// Tool schemas arrived as raw JSON from the wire; remarshalling them
			// cannot realistically fail. Skip the entry rather than lose the report.
			continue
		}
		n := len(raw)
		perUpstreamTools[d.Upstream]++
		perUpstreamBytes[d.Upstream] += n
		sizes = append(sizes, toolSize{name: d.Name, bytes: n})
		totalBytes += n
	}

	out := cmd.OutOrStdout()

	upstreams := make([]string, 0, len(perUpstreamTools))
	for name := range perUpstreamTools {
		upstreams = append(upstreams, name)
	}
	sort.Strings(upstreams)

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "UPSTREAM\tTOOLS\tBYTES\t~TOKENS")
	for _, name := range upstreams {
		b := perUpstreamBytes[name]
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\n", name, perUpstreamTools[name], b, b/bytesPerToken)
	}
	_ = w.Flush()

	if top > 0 && len(sizes) > 0 {
		// Heaviest first; ties broken by name so the report is deterministic.
		sort.Slice(sizes, func(i, j int) bool {
			if sizes[i].bytes != sizes[j].bytes {
				return sizes[i].bytes > sizes[j].bytes
			}
			return sizes[i].name < sizes[j].name
		})
		if top > len(sizes) {
			top = len(sizes)
		}
		fmt.Fprintf(out, "\nTop %d heaviest tools:\n", top)
		w = tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "TOOL\tBYTES\t~TOKENS")
		for _, s := range sizes[:top] {
			fmt.Fprintf(w, "%s\t%d\t%d\n", s.name, s.bytes, s.bytes/bytesPerToken)
		}
		_ = w.Flush()
	}

	fmt.Fprintf(out, "\nTOTAL: %d tool(s), %d bytes, ~%d tokens (approximate: bytes/4, not a real tokenizer)\n",
		len(sizes), totalBytes, totalBytes/bytesPerToken)
}
