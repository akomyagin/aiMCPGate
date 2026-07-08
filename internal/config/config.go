// Package config loads and validates aiMCPGate gateway configuration.
//
// The config describes the set of upstream MCP servers to aggregate, how the
// gateway exposes itself to the client (stdio in Фаза 1, HTTP/SSE in Фаза 2),
// and where tool-call logs are written.
//
// Реализация — Этап 1+ (parsing/validation is a stub for now; the shapes below
// are the intended surface, refine against docs/TECHNICAL_PLAN.md §Config).
package config

import "time"

// Transport enumerates how the gateway speaks to its client.
type Transport string

const (
	// TransportStdio serves the client over stdin/stdout (Фаза 1, the same
	// transport Claude Code uses to launch a local MCP server).
	TransportStdio Transport = "stdio"
	// TransportHTTP serves the client over HTTP + SSE (Фаза 2).
	TransportHTTP Transport = "http"
)

// UpstreamKind enumerates how an upstream MCP server is reached.
type UpstreamKind string

const (
	// UpstreamStdio launches the upstream as a child process and speaks
	// JSON-RPC 2.0 over its stdin/stdout.
	UpstreamStdio UpstreamKind = "stdio"
	// UpstreamHTTP connects to an already-running upstream over HTTP/SSE.
	UpstreamHTTP UpstreamKind = "http"
)

// Upstream describes a single upstream MCP server the gateway aggregates.
type Upstream struct {
	// Name is a stable, unique identifier used for namespacing tools and in
	// log records (e.g. "github", "filesystem").
	Name string `yaml:"name"`
	// Kind selects the transport used to reach this upstream.
	Kind UpstreamKind `yaml:"kind"`

	// Fields for Kind == UpstreamStdio.
	Command string            `yaml:"command"` // executable, resolved via exec.LookPath
	Args    []string          `yaml:"args"`
	Env     map[string]string `yaml:"env"` // extra env; secrets come from .env, never committed

	// Fields for Kind == UpstreamHTTP.
	URL string `yaml:"url"`

	// Enabled allows disabling an upstream without removing its config.
	Enabled bool `yaml:"enabled"`
}

// Config is the fully-parsed gateway configuration.
type Config struct {
	// Transport selects the client-facing transport.
	Transport Transport `yaml:"transport"`
	// ListenAddr is the bind address for TransportHTTP (Фаза 2), e.g. ":8080".
	ListenAddr string `yaml:"listen_addr"`

	// Upstreams is the ordered set of MCP servers to aggregate.
	Upstreams []Upstream `yaml:"upstreams"`

	// LogFile is where tool-call log records are written (JSON lines). Empty
	// means stderr only.
	LogFile string `yaml:"log_file"`
	// LogLevel is the slog level: "debug" | "info" | "warn" | "error".
	LogLevel string `yaml:"log_level"`

	// CallTimeout bounds a single upstream request (handshake, list, or call).
	// Zero selects DefaultCallTimeout.
	CallTimeout time.Duration `yaml:"call_timeout"`
}

// DefaultCallTimeout bounds a single upstream request when the config leaves
// CallTimeout unset.
const DefaultCallTimeout = 30 * time.Second

// EffectiveCallTimeout returns CallTimeout or DefaultCallTimeout if unset.
func (c *Config) EffectiveCallTimeout() time.Duration {
	if c.CallTimeout <= 0 {
		return DefaultCallTimeout
	}
	return c.CallTimeout
}

// Load reads and validates configuration from path.
//
// An empty path selects the default lookup order (flag → env → XDG config dir),
// resolved in Этап 1. For now Load returns a minimal stdio default so the
// skeleton builds and runs as a no-op.
func Load(path string) (*Config, error) {
	// TODO(Этап 1): parse YAML from path/default location, expand env for
	// secrets, and validate (unique upstream names, non-empty command/url per
	// kind, transport-specific required fields).
	_ = path
	return &Config{
		Transport: TransportStdio,
		LogLevel:  "info",
	}, nil
}

// Validate checks invariants independent of I/O. Stub until Этап 1.
func (c *Config) Validate() error {
	// TODO(Этап 1): enforce unique upstream names, required fields per kind,
	// and transport-specific requirements (ListenAddr for http).
	return nil
}
