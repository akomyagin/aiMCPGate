// Package transport is the client-facing side of the gateway: it accepts one
// MCP client connection (Claude Code and friends) and dispatches its JSON-RPC
// requests against the aggregated registry.
//
// Фаза 1 implements the stdio transport (JSON-RPC 2.0 framed over stdin/stdout,
// the same shape a client uses to launch a local MCP server). Фаза 2 adds an
// HTTP/SSE server behind the same Server interface.
//
// Реализация — Этап 1+ (Serve is a blocking no-op stub).
package transport

import (
	"context"
	"log/slog"

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
func NewServer(cfg *config.Config, reg *registry.Registry, logger *slog.Logger) Server {
	// TODO(Этап 1): switch cfg.Transport → stdioServer / httpServer.
	return &stubServer{cfg: cfg, reg: reg, log: logger}
}

// stubServer starts the registry, then blocks until cancellation without
// serving any requests. Replaced by real transports in Этап 1+.
type stubServer struct {
	cfg *config.Config
	reg *registry.Registry
	log *slog.Logger
}

func (s *stubServer) Serve(ctx context.Context) error {
	if err := s.reg.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = s.reg.Close() }()

	s.log.Info("transport ready (stub)", "transport", string(s.cfg.Transport))
	<-ctx.Done()
	s.log.Info("shutting down")
	return nil
}
