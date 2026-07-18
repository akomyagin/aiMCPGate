// Package config loads and validates aiMCPGate gateway configuration.
//
// The config describes the set of upstream MCP servers to aggregate, how the
// gateway exposes itself to the client (stdio in Phase 1, HTTP/SSE in Phase 2),
// and where tool-call logs are written.
//
// Secrets (upstream API keys / tokens) are never stored inline in the committed
// YAML: the values of auth_token and upstream env/headers entries are expanded
// with os.ExpandEnv, so a config carries "${GITHUB_TOKEN}" and the real value
// comes from the environment / a local .env, never from a file under git
// (SKILL §2/§6). Expansion is scoped to exactly those fields — not the whole
// file — so a literal '$' anywhere else (a URL, a password, a path) is never
// misread as a variable reference.
package config

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"gopkg.in/yaml.v3"
)

// Transport enumerates how the gateway speaks to its client.
type Transport string

const (
	// TransportStdio serves the client over stdin/stdout (Phase 1, the same
	// transport Claude Code uses to launch a local MCP server).
	TransportStdio Transport = "stdio"
	// TransportHTTP serves the client over HTTP + SSE (Phase 2).
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

	// Tools narrows and renames what this upstream contributes to the
	// aggregated catalog (Stage 9). The zero value passes every tool through
	// under the default "<upstream>__<tool>" name.
	Tools ToolFilter `yaml:"tools"`
}

// ToolFilter selects and renames the tools one upstream exposes to the client.
// All keys refer to the upstream's ORIGINAL tool names (before namespacing) —
// the filter logically belongs to the upstream, not to the aggregated catalog.
//
// Semantics (applied in this order by the registry):
//  1. Allow — when non-empty, only the listed tools survive (intersection);
//  2. Deny — always subtracted, even from an explicit Allow;
//  3. Rename — maps a surviving original name to its client-facing name;
//     tools without a rename get the default "<upstream>__<tool>".
//
// Deny is an ADDITIONAL safety barrier, not a replacement for upstream-side
// auth: it narrows the tool surface the client can even see, independent of
// whatever flags the upstream itself supports — but a compromised upstream is
// still a compromised upstream.
type ToolFilter struct {
	Allow  []string          `yaml:"allow"`
	Deny   []string          `yaml:"deny"`
	Rename map[string]string `yaml:"rename"`
}

// SameLaunch reports whether two upstreams would launch identically — same
// transport and every field that affects how the upstream is reached. Used by
// hot-reload (Stage 7d) to tell an unchanged upstream (leave running) from a
// changed one (Close + relaunch). Name is assumed equal by the caller (it is the
// match key); Enabled is intentionally NOT compared here — enable/disable is
// handled as add/remove by the reload diff, not as a "changed launch".
func (u Upstream) SameLaunch(other Upstream) bool {
	if u.ResolveKind() != other.ResolveKind() ||
		u.Command != other.Command ||
		u.URL != other.URL ||
		!equalStringSlice(u.Args, other.Args) ||
		!equalStringMap(u.Env, other.Env) ||
		!equalStringMap(u.Headers, other.Headers) {
		return false
	}
	return true
}

// SameFilter reports whether two upstreams project the same tool filter
// (allow/deny/rename). It is deliberately SEPARATE from SameLaunch: the launch
// predicate is about how the upstream PROCESS is reached, while the filter is
// only a projection of its catalog — hot-reload (Stage 9) uses the distinction
// to re-apply a changed filter to the stored raw tool list without relaunching
// (or even re-listing) an otherwise-identical upstream.
func (u Upstream) SameFilter(other Upstream) bool {
	return equalStringSlice(u.Tools.Allow, other.Tools.Allow) &&
		equalStringSlice(u.Tools.Deny, other.Tools.Deny) &&
		equalStringMap(u.Tools.Rename, other.Tools.Rename)
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
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
	// ListenAddr is the bind address for TransportHTTP (Phase 2), e.g. ":28080".
	ListenAddr string `yaml:"listen_addr"`
	// AuthToken, when non-empty, requires every incoming HTTP request to carry
	// "Authorization: Bearer <token>". Use ${ENV_VAR} — never commit the value.
	// Only meaningful for TransportHTTP; ignored for stdio.
	AuthToken string `yaml:"auth_token"`

	// Upstreams is the ordered set of MCP servers to aggregate.
	Upstreams []Upstream `yaml:"upstreams"`

	// LogFile is where tool-call log records are written (JSON lines). Empty
	// means stderr only. A relative path is resolved against the config
	// file's directory, not the process's working directory (Load).
	LogFile string `yaml:"log_file"`
	// LogLevel is the slog level: "debug" | "info" | "warn" | "error".
	LogLevel string `yaml:"log_level"`

	// DebugPayloadLog, when non-empty, enables an OPT-IN, off-by-default debug
	// log of tool-call payloads (arguments and results) written as JSON lines
	// to this file — for debugging only, NEVER production. It is deliberately
	// separate from LogFile: the audit log (LogFile) stays metadata-only and
	// must never carry arguments, which may contain secrets (SKILL §6). Empty
	// (the default) disables payload logging entirely. A relative path is
	// resolved against the config file's directory (Load); it must not equal
	// LogFile (Validate), otherwise secrets would leak into the audit log.
	DebugPayloadLog string `yaml:"debug_payload_log"`

	// SkillFile, when set, points to a Markdown file that `mcp-gate skill`
	// prints instead of the built-in deployment-independent guide — e.g. to
	// add org-specific tool-usage policy or a translation. Unset uses the
	// built-in text (internal/cli/skill.go), which needs no config to work.
	// A relative path is resolved against the config file's directory (Load).
	SkillFile string `yaml:"skill_file"`

	// CallTimeout bounds a single upstream request (handshake, list, or call).
	// Zero selects DefaultCallTimeout.
	CallTimeout time.Duration `yaml:"call_timeout"`

	// Restart is the GLOBAL policy for automatically restarting a stdio upstream
	// whose child process dies while the gateway is running (Stage 7a). It is a
	// single policy, not per-upstream: the granularity was deliberately kept
	// global (decided 2026-07-09) — a restart always replays the very same
	// config.Upstream the upstream was first launched with, so there is nothing
	// per-upstream to tune here. HTTP upstreams have no process that "dies"
	// between calls, so this policy applies to stdio upstreams only.
	Restart RestartPolicy `yaml:"restart"`
}

// RestartPolicy controls exponential-backoff auto-restart of stdio upstreams.
//
// Defaults (via Effective*) are chosen so an operator who never mentions
// `restart:` still gets sensible resilience: enabled, 1s→30s backoff, 5 tries.
// Set MaxAttempts to 0 for unlimited retries.
type RestartPolicy struct {
	// Enabled turns auto-restart on. A pointer so an unset key defaults to
	// enabled (nil → true in EffectiveRestart) while an explicit `enabled: false`
	// is honoured — a plain bool could not tell "unset" from "false".
	Enabled *bool `yaml:"enabled"`
	// InitialBackoff is the delay before the first restart attempt. Zero selects
	// DefaultRestartInitialBackoff.
	InitialBackoff time.Duration `yaml:"initial_backoff"`
	// MaxBackoff caps the exponentially growing delay. Zero selects
	// DefaultRestartMaxBackoff.
	MaxBackoff time.Duration `yaml:"max_backoff"`
	// MaxAttempts bounds how many consecutive restarts are attempted before the
	// upstream is left out for good. Zero means unlimited. Negative is rejected
	// by Validate. When unset via YAML it is 0 (unlimited) unless the whole
	// policy is defaulted — see DefaultRestartMaxAttempts / EffectiveRestart.
	MaxAttempts int `yaml:"max_attempts"`
}

// DefaultCallTimeout bounds a single upstream request when the config leaves
// CallTimeout unset.
const DefaultCallTimeout = 30 * time.Second

// Restart-policy defaults, applied field-by-field by EffectiveRestart when the
// config leaves a field unset.
const (
	DefaultRestartInitialBackoff = 1 * time.Second
	DefaultRestartMaxBackoff     = 30 * time.Second
	// DefaultRestartMaxAttempts is applied only when the WHOLE restart policy is
	// left unset (its zero value): an operator who never writes `restart:` gets a
	// bounded 5 attempts, but one who explicitly writes `max_attempts: 0` means
	// unlimited and that 0 is honoured verbatim (see EffectiveRestart).
	DefaultRestartMaxAttempts = 5
	// RestartBackoffFactor is the fixed exponential multiplier between attempts.
	// Not configurable: a single knob (initial→max) is enough to reason about,
	// and a tunable factor adds a config surface with no demonstrated need.
	RestartBackoffFactor = 2
)

// DefaultListenAddr is the bind address used for TransportHTTP when the config
// leaves ListenAddr unset. Bound to loopback, not ":28080"/0.0.0.0: without
// auth_token set, defaulting to all interfaces would silently expose every
// aggregated upstream tool to the whole LAN. A user who wants network exposure
// should set both listen_addr and auth_token explicitly.
const DefaultListenAddr = "127.0.0.1:28080"

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

// EffectiveRestart returns the restart policy with every unset field filled in
// from its default, so callers (the registry supervisor) never have to reason
// about zero values. Enabled defaults to true when the key is absent; the
// backoff bounds default to 1s/30s; MaxAttempts defaults to 5 ONLY when the
// entire policy was left unset (its zero value) — an explicit `max_attempts: 0`
// under an otherwise-populated policy is preserved as "unlimited".
func (c *Config) EffectiveRestart() RestartPolicy {
	p := c.Restart
	zeroPolicy := p == (RestartPolicy{})

	enabled := true
	if p.Enabled != nil {
		enabled = *p.Enabled
	}
	initial := p.InitialBackoff
	if initial <= 0 {
		initial = DefaultRestartInitialBackoff
	}
	maxB := p.MaxBackoff
	if maxB <= 0 {
		maxB = DefaultRestartMaxBackoff
	}
	if maxB < initial {
		maxB = initial
	}
	attempts := p.MaxAttempts
	if zeroPolicy {
		attempts = DefaultRestartMaxAttempts
	}
	return RestartPolicy{
		Enabled:        &enabled,
		InitialBackoff: initial,
		MaxBackoff:     maxB,
		MaxAttempts:    attempts,
	}
}

// DefaultConfigName is the file Load looks for next to the running binary
// when no --config path is given.
const DefaultConfigName = "config.yaml"

// defaultConfigPath returns <directory of the running binary>/config.yaml —
// the location Load falls back to when path is empty.
func defaultConfigPath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate running binary to find its default config: %w", err)
	}
	return filepath.Join(filepath.Dir(exe), DefaultConfigName), nil
}

// Load reads and validates configuration from path.
//
// An empty path falls back to DefaultConfigName next to the running binary
// (e.g. mcp-gate installed at /etc/gate/mcp-gate looks for
// /etc/gate/config.yaml) — the gateway can be launched from any working
// directory and still find its own config. If that default file does not
// exist either, Load errors rather than silently starting an empty gateway
// (found by user request).
//
// Once a path is settled, the file is read, unmarshaled, has its
// secret-carrying fields (auth_token, upstream env/headers values) expanded
// against the environment, and validated.
func Load(path string) (*Config, error) {
	usingDefault := false
	if path == "" {
		def, err := defaultConfigPath()
		if err != nil {
			return nil, err
		}
		path = def
		usingDefault = true
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if usingDefault {
			return nil, fmt.Errorf("no --config given and no default config at %q: %w", path, err)
		}
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	// Expand ${VAR} / $VAR against the environment only in the fields
	// documented as carrying secrets (auth_token, upstream env/headers
	// values) — never across the whole file. Expanding the raw file text
	// (the previous approach) silently mangled any literal '$' anywhere in
	// the YAML — a password, a URL, a path — since os.ExpandEnv has no way
	// to tell "meant as a variable" from "just a dollar sign" (found by code
	// review). An unset variable silently becomes the empty string (nothing
	// currently validates these fields as non-empty) — a genuinely unset
	// secret surfaces later as an upstream auth failure, not a config error.
	cfg.AuthToken = os.ExpandEnv(cfg.AuthToken)
	for i := range cfg.Upstreams {
		expandMapValues(cfg.Upstreams[i].Env)
		expandMapValues(cfg.Upstreams[i].Headers)
	}
	if cfg.Transport == "" {
		cfg.Transport = TransportStdio
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	// Gateway-owned file paths (log, skill override) are relative to the
	// config file's directory, not the process's working directory — the
	// gateway can be launched from anywhere, but its config and the files it
	// references live together (found by user request: avoids confusion when
	// serve is run from a different cwd than the config lives in).
	dir := filepath.Dir(path)
	cfg.LogFile = resolveRelative(dir, cfg.LogFile)
	cfg.SkillFile = resolveRelative(dir, cfg.SkillFile)
	cfg.DebugPayloadLog = resolveRelative(dir, cfg.DebugPayloadLog)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config %q: %w", path, err)
	}
	return &cfg, nil
}

// expandMapValues expands ${VAR}/$VAR in each value of m against the
// environment, in place. Keys (header/env-var names) are left untouched —
// only values are meant to carry secrets.
func expandMapValues(m map[string]string) {
	for k, v := range m {
		m[k] = os.ExpandEnv(v)
	}
}

// resolveRelative joins path onto dir when path is relative and non-empty;
// absolute paths and the empty string pass through unchanged.
func resolveRelative(dir, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(dir, path)
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

	if err := c.Restart.validate(); err != nil {
		return err
	}

	// The opt-in payload debug log must never share a file with the audit log:
	// payloads carry raw arguments/results (possibly secrets), which the audit
	// log is required to stay free of (SKILL §6). Reject the overlap outright.
	if c.DebugPayloadLog != "" && c.DebugPayloadLog == c.LogFile {
		return fmt.Errorf("debug_payload_log must not equal log_file (%q): payloads may contain secrets and must not leak into the audit log", c.LogFile)
	}

	seen := make(map[string]bool, len(c.Upstreams))
	// clientNames tracks every client-facing tool name the config makes
	// statically known (rename targets and default-namespaced allow entries),
	// across ALL upstreams: a collision there would silently shadow one tool
	// behind another at merge time, so it must be a config error, not a
	// runtime keep-first (Stage 9).
	clientNames := make(map[string]string)
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
		if err := validateToolFilter(u, clientNames); err != nil {
			return err
		}
	}
	return nil
}

// validateToolFilter checks one upstream's tools filter and claims its
// statically-known client-facing names in clientNames (name → human-readable
// owner), rejecting a cross-upstream collision. The config only knows the
// original names spelled in allow/rename — tools that pass through with the
// default "<upstream>__<tool>" name and are not listed anywhere cannot be
// checked here; the registry's merge keeps the first of a runtime duplicate
// and logs it, as before. Runs over disabled upstreams too, matching how
// Validate catches a disabled-but-malformed entry early (and how enabling one
// via reload must not surface a brand-new collision).
func validateToolFilter(u Upstream, clientNames map[string]string) error {
	f := u.Tools
	allow := make(map[string]bool, len(f.Allow))
	for _, a := range f.Allow {
		allow[a] = true
	}
	deny := make(map[string]bool, len(f.Deny))
	for _, d := range f.Deny {
		deny[d] = true
	}

	claim := func(clientName, owner string) error {
		if prev, dup := clientNames[clientName]; dup {
			return fmt.Errorf("client-facing tool name %q claimed by both %s and %s", clientName, prev, owner)
		}
		clientNames[clientName] = owner
		return nil
	}

	// Rename keys in sorted order so a config with several problems reports
	// the same one on every run (map iteration order is randomized).
	for _, orig := range slices.Sorted(maps.Keys(f.Rename)) {
		newName := f.Rename[orig]
		if !upstreamNameRe.MatchString(newName) {
			return fmt.Errorf("upstream %q: tools.rename[%q] = %q: client-facing name must match %s (MCP_NOTES §6)",
				u.Name, orig, newName, upstreamNameRe)
		}
		if len(f.Allow) > 0 && !allow[orig] {
			return fmt.Errorf("upstream %q: tools.rename key %q is not in tools.allow — the rename could never apply", u.Name, orig)
		}
		if deny[orig] {
			continue // a denied tool never reaches the client, so its renamed name is never used
		}
		if err := claim(newName, fmt.Sprintf("upstream %q (rename of %q)", u.Name, orig)); err != nil {
			return err
		}
	}
	// Allowed-but-not-renamed tools land under the default namespaced name —
	// the only other client-facing names the config knows before runtime.
	for _, a := range f.Allow {
		if _, renamed := f.Rename[a]; renamed || deny[a] {
			continue
		}
		// "__" is the registry's NameSeparator (docs/MCP_NOTES.md §6); config
		// cannot import registry (import cycle), so it is spelled here.
		if err := claim(u.Name+"__"+a, fmt.Sprintf("upstream %q (tool %q)", u.Name, a)); err != nil {
			return err
		}
	}
	return nil
}

// validate rejects a nonsensical restart policy. Unset fields are fine (they
// default via EffectiveRestart); only actively-wrong values are rejected —
// negative durations or a negative attempt count.
func (p RestartPolicy) validate() error {
	if p.InitialBackoff < 0 {
		return fmt.Errorf("restart.initial_backoff must not be negative")
	}
	if p.MaxBackoff < 0 {
		return fmt.Errorf("restart.max_backoff must not be negative")
	}
	if p.MaxAttempts < 0 {
		return fmt.Errorf("restart.max_attempts must not be negative (0 means unlimited)")
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
