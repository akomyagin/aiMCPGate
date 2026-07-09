// Package transport is the client-facing side of the gateway: it accepts one
// MCP client connection (Claude Code and friends) and dispatches its JSON-RPC
// requests against the aggregated registry.
//
// Phase 1 implements the stdio transport (JSON-RPC 2.0 framed over stdin/stdout,
// the same shape a client uses to launch a local MCP server). Phase 2 adds an
// HTTP/SSE server behind the same Server interface.
package transport

import (
	"context"
	"log/slog"
	"os"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// Server is the client-facing transport. One implementation per transport kind
// (stdio, http), selected from config.
type Server interface {
	// Serve blocks handling client requests until ctx is cancelled.
	Serve(ctx context.Context) error
}

// NewServer selects and builds the transport implementation from config.
//
// version is the gateway build version reported to the client in serverInfo;
// it is threaded from main.go (which owns the -ldflags-injected value) rather
// than hardcoded here, so the client always sees the real binary version.
func NewServer(cfg *config.Config, reg *registry.Registry, logger *slog.Logger, version string) Server {
	if cfg.Transport == config.TransportHTTP {
		return newHTTPServer(cfg, reg, logger, version)
	}
	return newStdioServer(cfg, reg, logger, version, os.Stdin, os.Stdout)
}
