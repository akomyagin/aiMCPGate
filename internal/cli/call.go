package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// newCallCmd performs ONE tool call from the terminal: bring the upstreams up
// (same one-pass, no-supervisor bring-up as doctor), route the namespaced tool
// through the registry exactly like a real client's tools/call would go, print
// the result, exit. It is the DevEx shortcut for "does this tool actually work
// through the gateway?" without wiring an MCP client up (Round 10).
func newCallCmd(version string) *cobra.Command {
	var (
		configPath *string
		envFile    *string
	)
	cmd := &cobra.Command{
		Use:   "call <namespaced-tool> [json-args]",
		Short: "Call one aggregated tool once and print its result as JSON",
		Long: "call starts every enabled upstream once (no supervision, no call journal),\n" +
			"invokes the given namespaced tool (e.g. github__search) with the JSON arguments\n" +
			"({} when omitted) and pretty-prints the tool's result to stdout. Transport\n" +
			"failures and JSON-RPC errors from the tool go to stderr with a non-zero exit\n" +
			"code, so the command is scriptable.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCall(cmd, *configPath, *envFile, version, args)
		},
	}
	configPath = addConfigFlag(cmd)
	envFile = addEnvFileFlag(cmd)
	return cmd
}

func runCall(cmd *cobra.Command, configPath, envFile, version string, args []string) error {
	// The env file must land in the environment BEFORE config.Load expands the
	// ${VAR} references in the config.
	if err := applyEnvFile(envFile); err != nil {
		return err
	}

	toolName := args[0]
	arguments := json.RawMessage("{}")
	if len(args) == 2 {
		// Validate up front: shipping malformed JSON to an upstream would only
		// produce a confusing remote parse error after the whole bring-up.
		if !json.Valid([]byte(args[1])) {
			return fmt.Errorf("arguments are not valid JSON: %q", args[1])
		}
		arguments = json.RawMessage(args[1])
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}

	logger := logging.New(cfg.LogLevel, os.Stderr)

	// Same one-shot registry shape as doctor: supervise=false (one bring-up
	// pass, no restart goroutines), callLog=nil (an interactive terminal call
	// is not gateway traffic to audit), payload log is the no-op.
	payloadLog, _ := logging.NewPayloadLog("")
	reg := registry.New(cfg, logger, nil, payloadLog, false, version)
	defer func() { _ = reg.Close() }()

	// Start errors only when NOTHING came up (or nothing is enabled) — with no
	// live upstream the call below is doomed, so bail with the real reason. A
	// partial failure is survivable: if the tool's owner started, the call works.
	if err := reg.Start(cmd.Context()); err != nil {
		return err
	}

	resp, err := reg.CallTool(cmd.Context(), toolName, arguments)
	if err != nil {
		// Transport/routing failure (unknown tool, dead upstream, timeout).
		// Returning the error is the shared exit-code path: cobra prints it to
		// stderr and main exits 1.
		return err
	}
	if resp.Error != nil {
		// The tool itself answered with a JSON-RPC error: surface its code and
		// message verbatim, still a failure for scripting purposes.
		return fmt.Errorf("tool %q returned JSON-RPC error %d: %s", toolName, resp.Error.Code, resp.Error.Message)
	}

	// resp.Result is a json.RawMessage; MarshalIndent re-emits it pretty-printed
	// without imposing any schema on the tool's payload.
	pretty, err := json.MarshalIndent(resp.Result, "", "  ")
	if err != nil {
		return fmt.Errorf("format result: %w", err)
	}
	fmt.Fprintln(cmd.OutOrStdout(), string(pretty))
	return nil
}
