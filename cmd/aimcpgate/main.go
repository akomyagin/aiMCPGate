// Command aimcpgate is the entry point for the aiMCPGate MCP gateway.
//
// aiMCPGate is a Model Context Protocol (MCP) gateway/proxy: it presents a
// single MCP endpoint to a client (e.g. Claude Code) and multiplexes
// tool/resource calls across several upstream MCP servers, aggregating their
// catalogs into one and logging every call that flows through.
//
// This main is intentionally thin: it wires configuration, logging, the
// upstream registry and the client-facing transport, then blocks until the
// process is cancelled (Ctrl-C / SIGTERM). Real behaviour lands from Этап 1
// onward — see docs/TECHNICAL_PLAN.md.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/registry"
	"github.com/akomyagin/aiMCPGate/internal/transport"
)

// version is overridden at build time via -ldflags (see Этап 5 / goreleaser).
var version = "0.0.0-dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "aimcpgate:", err)
		os.Exit(1)
	}
}

func run() error {
	// Cancel the whole tree on Ctrl-C / SIGTERM so upstream child processes
	// get torn down cleanly (see internal/registry).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := logging.New(cfg.LogLevel, os.Stderr)
	logger.Info("aimcpgate starting", "version", version)

	reg := registry.New(cfg, logger)
	srv := transport.NewServer(cfg, reg, logger)

	// Serve blocks until ctx is cancelled; stub returns nil until Этап 1.
	return srv.Serve(ctx)
}
