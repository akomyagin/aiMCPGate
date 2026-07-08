// Package registry manages the set of upstream MCP servers and the aggregated
// catalog of their tools and resources.
//
// This is the heart of the gateway. On start it launches/connects every
// enabled upstream, performs the MCP initialize handshake, lists each
// upstream's tools/resources, and merges them into one namespaced catalog that
// the client-facing transport exposes. When the client invokes a tool, the
// registry resolves which upstream owns it and forwards the JSON-RPC call.
//
// Реализация — Этап 1+ (this is a typed stub establishing the surface).
package registry

import (
	"context"
	"log/slog"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// ToolDescriptor is one aggregated tool entry in the merged catalog.
//
// Name is the client-facing name after namespacing (e.g. "github__search").
// Upstream records which upstream owns it so calls can be routed back.
type ToolDescriptor struct {
	Name        string
	Upstream    string
	Description string
	// InputSchema is the upstream-provided JSON Schema, passed through
	// verbatim so the client sees the same contract.
	// [TODO уточнить по официальной спецификации в Этапе 1] — exact field
	// name/shape in tools/list results.
	InputSchema []byte
}

// Registry owns upstream connections and the aggregated catalog.
type Registry struct {
	cfg *config.Config
	log *slog.Logger
	// TODO(Этап 1): upstreams map[string]*upstreamConn; catalog + mutex;
	// name→upstream routing table for collision handling.
}

// New constructs a Registry from config. It does not start upstreams yet —
// call Start.
func New(cfg *config.Config, logger *slog.Logger) *Registry {
	return &Registry{cfg: cfg, log: logger}
}

// Start launches/connects every enabled upstream, runs the MCP handshake, and
// builds the aggregated catalog. Stub until Этап 1.
func (r *Registry) Start(ctx context.Context) error {
	// TODO(Этап 1): for each enabled upstream, spawn (os/exec for stdio) or
	// dial (http), send `initialize`, then `tools/list` + `resources/list`,
	// and merge results. Namespace names to avoid cross-upstream collisions.
	_ = ctx
	return nil
}

// Tools returns the aggregated, namespaced tool catalog. Empty until Этап 1.
func (r *Registry) Tools() []ToolDescriptor {
	return nil
}

// Close tears down all upstream connections/child processes.
func (r *Registry) Close() error {
	// TODO(Этап 1): terminate child processes, close http clients, wait.
	return nil
}
