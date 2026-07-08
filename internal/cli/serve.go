package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/registry"
	"github.com/akomyagin/aiMCPGate/internal/transport"
)

// newServeCmd wires config → logger → registry → transport and blocks serving
// the client until the process is cancelled (Ctrl-C / SIGTERM). This is the
// gateway's main run loop; keeping it here keeps main.go trivial (SKILL §1).
func newServeCmd(version string) *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the gateway, serving one client and multiplexing upstream MCP servers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(cmd.Context(), configPath, version)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to the YAML config file")
	return cmd
}

func runServe(parent context.Context, configPath, version string) error {
	// Cancel the whole tree on Ctrl-C / SIGTERM so upstream child processes get
	// torn down cleanly (see internal/registry).
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := logging.New(cfg.LogLevel, os.Stderr)
	logger.Info("aimcpgate starting", "version", version, "transport", cfg.Transport)

	callLog, err := logging.NewCallLog(cfg.LogFile)
	if err != nil {
		return fmt.Errorf("open call log: %w", err)
	}
	defer func() { _ = callLog.Close() }()

	reg := registry.New(cfg, logger, callLog)
	srv := transport.NewServer(cfg, reg, logger, version)

	// Serve starts the registry (upstream fan-out) and blocks handling client
	// requests until ctx is cancelled or the client disconnects.
	return srv.Serve(ctx)
}
