// Command aimcpgate is the entry point for the aiMCPGate MCP gateway.
//
// aiMCPGate is a Model Context Protocol (MCP) gateway/proxy: it presents a
// single MCP endpoint to a client (e.g. Claude Code) and multiplexes
// tool/resource calls across several upstream MCP servers, aggregating their
// catalogs into one and logging every call that flows through.
//
// This main is intentionally thin: it builds the cobra command tree (serve /
// logs / version) and executes it. All wiring lives in internal/cli (SKILL §1).
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/akomyagin/aiMCPGate/internal/cli"
)

// version is overridden at build time via -ldflags (see Этап 6 / goreleaser).
var version = "0.0.0-dev"

func main() {
	// A signal-cancelled context is threaded into the command so `serve` can
	// shut down cleanly on Ctrl-C / SIGTERM (it derives its own from this).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cli.Build(version).ExecuteContext(ctx); err != nil {
		// cobra already printed the error (SilenceUsage keeps usage noise out);
		// exit non-zero so callers/scripts see the failure.
		os.Exit(1)
	}
}
