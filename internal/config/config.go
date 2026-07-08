// Package config loads and validates aiMCPGate gateway configuration.
//
// The config describes the set of upstream MCP servers to aggregate, how the
// gateway exposes itself to the client (stdio in Фаза 1, HTTP/SSE in Фаза 2),
// and where tool-call logs are written.
//
// Secrets (upstream API keys / tokens) are never stored inline in the committed
// YAML: string values are expanded with os.ExpandEnv, so a config carries
// "${GITHUB_TOKEN}" and the real value comes from the environment / a local
// .env, never from a file under git (SKILL §2/§6).
package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

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
	// UpstreamHTTP connects to an already-running upstream over Streamable HTTP.
	UpstreamHTTP UpstreamKind = "http"
)

// upstreamNameRe restricts upstream names to characters that survive namespacing
// into "<upstream>__<tool>" without breaking clients that expect tool names to
// match ^[a-zA-Z0-9_-]+$ (Claude Code and friends). See docs/MCP_NOTES.md §6.
var upstreamNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Upstream describes a single upstream MCP server the gateway aggregates.
type Upstream struct {
	// Name is a stable, unique identifier used for namespacing tools and in
	// log records (e.g. "github", "filesystem").
	Name string `yaml:"name"`
	// Kind selects the transport used to reach this upstream. When empty it is
	// inferred: url set → http, otherwise stdio (so simple configs need not
	// spell it out). ResolveKind performs the inference.
	Kind UpstreamKind `yaml:"kind"`

	// Fields for Kind == UpstreamStdio.
	Command string            `yaml:"command"` // executable, resolved via exec.LookPath
	Args    []string          `yaml:"args"`
	Env     map[string]string `yaml:"env"` // extra env; secrets come from .env, never committed

	// Fields for Kind == UpstreamHTTP.
	URL string `yaml:"url"` // Streamable HTTP endpoint of an already-running upstream
	// Headers are extra HTTP headers sent on every request to this upstream —
	// typically an "Authorization" bearer token. Values go through env
	// expansion, so the committed YAML holds "${TOKEN}", not the secret itself.
	// These are never logged (SKILL §6).
	Headers map[string]string `yaml:"headers"`

	// Enabled allows disabling an upstream without removing its config.
	Enabled bool `yaml:"enabled"`
}

// ResolveKind returns the effective kind: the explicit Kind if set, otherwise
// inferred from which fields are populated (url → http, else stdio).
func (u Upstream) ResolveKind() UpstreamKind {
	if u.Kind != "" {
		return u.Kind
	}
	if u.URL != "" {
		return UpstreamHTTP
	}
	return UpstreamStdio
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

// DefaultListenAddr is the bind address used for TransportHTTP when the config
// leaves ListenAddr unset.
const DefaultListenAddr = ":8080"

// EffectiveCallTimeout returns CallTimeout or DefaultCallTimeout if unset.
func (c *Config) EffectiveCallTimeout() time.Duration {
	if c.CallTimeout <= 0 {
		return DefaultCallTimeout
	}
	return c.CallTimeout
}

// EffectiveListenAddr returns ListenAddr or DefaultListenAddr if unset.
func (c *Config) EffectiveListenAddr() string {
	if c.ListenAddr == "" {
		return DefaultListenAddr
	}
	return c.ListenAddr
}

// Load reads and validates configuration from path.
//
// An empty path selects a minimal stdio default with no upstreams, so the
// gateway can still start (and report an empty catalog) without a config file —
// convenient for `version`/smoke runs. A non-empty path is read, has its string
// values expanded against the environment (secrets), unmarshaled, and validated.
func Load(path string) (*Config, error) {
	if path == "" {
		return &Config{Transport: TransportStdio, LogLevel: "info"}, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	// Expand ${VAR} / $VAR against the environment before parsing so secrets
	// referenced in the YAML (tokens in env/headers) are resolved from the
	// environment, never committed literally. os.ExpandEnv replaces unset
	// variables with the empty string, which Validate then rejects where a
	// value is required (e.g. an unset upstream command).
	expanded := os.ExpandEnv(string(raw))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if cfg.Transport == "" {
		cfg.Transport = TransportStdio
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config %q: %w", path, err)
	}
	return &cfg, nil
}

// Validate checks invariants independent of I/O: known transport, unique and
// well-formed upstream names, and exactly one transport per upstream (a stdio
// command XOR an http url). It runs over every upstream, enabled or not, so a
// disabled-but-malformed entry is still caught early.
func (c *Config) Validate() error {
	switch c.Transport {
	case TransportStdio, TransportHTTP:
	default:
		return fmt.Errorf("unknown transport %q (want %q or %q)", c.Transport, TransportStdio, TransportHTTP)
	}

	seen := make(map[string]bool, len(c.Upstreams))
	for i, u := range c.Upstreams {
		if u.Name == "" {
			return fmt.Errorf("upstream #%d: name is required", i)
		}
		if !upstreamNameRe.MatchString(u.Name) {
			return fmt.Errorf("upstream %q: name must match %s (namespacing constraint, MCP_NOTES §6)", u.Name, upstreamNameRe)
		}
		if seen[u.Name] {
			return fmt.Errorf("upstream %q: duplicate name", u.Name)
		}
		seen[u.Name] = true

		if err := validateUpstreamTransport(u); err != nil {
			return err
		}
	}
	return nil
}

// validateUpstreamTransport enforces "exactly one of a stdio command or an http
// url", cross-checking against an explicit Kind if one is given.
func validateUpstreamTransport(u Upstream) error {
	hasCmd := u.Command != ""
	hasURL := u.URL != ""

	switch {
	case hasCmd && hasURL:
		return fmt.Errorf("upstream %q: set exactly one of command (stdio) or url (http), not both", u.Name)
	case !hasCmd && !hasURL:
		return fmt.Errorf("upstream %q: set exactly one of command (stdio) or url (http)", u.Name)
	}

	// If Kind is explicit it must agree with which field is populated, so a
	// typo (kind: http with only a command) is caught rather than silently
	// misrouted.
	switch u.Kind {
	case UpstreamStdio:
		if !hasCmd {
			return fmt.Errorf("upstream %q: kind stdio requires command", u.Name)
		}
	case UpstreamHTTP:
		if !hasURL {
			return fmt.Errorf("upstream %q: kind http requires url", u.Name)
		}
	case "":
		// inferred by ResolveKind; nothing to cross-check
	default:
		return fmt.Errorf("upstream %q: unknown kind %q (want %q or %q)", u.Name, u.Kind, UpstreamStdio, UpstreamHTTP)
	}
	return nil
}
