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
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
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

// Catalog-mode values (Round 8). An empty CatalogMode means CatalogModeNormal.
const (
	// CatalogModeNormal exposes every aggregated, namespaced upstream tool in
	// tools/list — the default behaviour since the MVP.
	CatalogModeNormal = "normal"
	// CatalogModeLazy exposes exactly three gateway meta-tools
	// (gate_search_tools / gate_describe / gate_call) instead of the full
	// catalog — progressive tool disclosure for very large catalogs. See
	// internal/transport/lazy.go.
	CatalogModeLazy = "lazy"
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

	// Enabled allows disabling an upstream without removing its config. A
	// pointer so an ABSENT key means enabled (nil → true in IsEnabled) while an
	// explicit `enabled: false` is honoured — a plain bool could not tell
	// "unset" from "false", and its zero value made a hand-written upstream that
	// simply forgot the key vanish silently: no log line, no doctor row, no
	// tools. Same shape and reasoning as RestartPolicy.Enabled below. Never read
	// this field directly — go through IsEnabled.
	//
	// Note the cost of the pointer: YAML decodes a valueless `enabled:` (and
	// `enabled: null` / `enabled: ~`) to nil, i.e. ENABLED — commenting out the
	// value no longer disables an upstream, only an explicit `false` does.
	Enabled *bool `yaml:"enabled"`

	// Tools narrows and renames what this upstream contributes to the
	// aggregated catalog (Stage 9). The zero value passes every tool through
	// under the default "<upstream>__<tool>" name.
	Tools ToolFilter `yaml:"tools"`

	// Call-limit knobs (Round 6). These are deliberately NOT part of SameLaunch:
	// changing a limit never requires relaunching the upstream process — the
	// registry re-reads them from the live config on every call, so a SIGHUP
	// reload takes effect on the next tools/call without any upstream downtime.

	// RateLimit overrides the global rate_limit for THIS upstream's forwarded
	// tools/call requests. nil inherits the global setting; an explicit block
	// with rps: 0 disables rate limiting for this upstream even when a global
	// limit is set (see EffectiveRateLimitFor).
	RateLimit *RateLimit `yaml:"rate_limit"`
	// MaxConcurrent caps how many tools/call requests may be in flight against
	// this upstream at once (a semaphore in the registry). 0 = unlimited.
	// Per-upstream only — there is no global default: the sensible cap depends
	// entirely on the individual upstream's nature.
	MaxConcurrent int `yaml:"max_concurrent"`
	// MaxResultBytes overrides the global max_result_bytes for this upstream.
	// A pointer so "unset" (nil → inherit the global value) is distinguishable
	// from an explicit 0 (= unlimited, overriding a global limit).
	MaxResultBytes *int `yaml:"max_result_bytes"`
	// CallTimeout overrides the global call_timeout for requests to THIS
	// upstream (handshake, list, forwarded call). 0 = inherit the global
	// EffectiveCallTimeout (see EffectiveCallTimeoutFor).
	CallTimeout time.Duration `yaml:"call_timeout"`
}

// RateLimit is a token-bucket limit on forwarded tools/call requests
// (golang.org/x/time/rate): RPS tokens are refilled per second, Burst is the
// bucket size (how many calls may fire back-to-back before the refill rate
// kicks in). A Burst of 0 is treated as 1 by EffectiveRateLimitFor — a bucket
// that can never hold a token would block every call forever. RPS of 0 (or an
// entirely absent block) means no rate limiting.
type RateLimit struct {
	RPS   float64 `yaml:"rps"`
	Burst int     `yaml:"burst"`
}

// validate rejects a nonsensical rate limit; nil (unset) is fine. where names
// the config location for the error message ("rate_limit" or
// "upstream %q: rate_limit").
func (rl *RateLimit) validate(where string) error {
	if rl == nil {
		return nil
	}
	if rl.RPS < 0 {
		return fmt.Errorf("%s.rps must not be negative (0 disables the limit)", where)
	}
	if rl.Burst < 0 {
		return fmt.Errorf("%s.burst must not be negative (0 means 1)", where)
	}
	return nil
}

// ToolFilter selects and renames the tools one upstream exposes to the client.
// All keys refer to the upstream's ORIGINAL tool names (before namespacing) —
// the filter logically belongs to the upstream, not to the aggregated catalog.
//
// Semantics (applied in this order by the registry):
//  1. Allow — when non-empty, only the listed tools survive (intersection);
//  2. Deny — always subtracted, even from an explicit Allow;
//  3. Rename — maps a surviving original name to its client-facing name;
//     tools without a rename get the default "<upstream>__<tool>";
//  4. Projection rules (token optimization, opt-in): StripAnnotations and
//     StripOutputSchema drop the heavyweight catalog fields entirely;
//     Describe replaces a tool's description with the configured text
//     (keyed by ORIGINAL name, same as Rename); MaxDescription truncates
//     descriptions NOT overridden by Describe to at most that many runes
//     (a Describe override is the config author's final word — it is never
//     re-truncated).
//
// Deny is an ADDITIONAL safety barrier, not a replacement for upstream-side
// auth: it narrows the tool surface the client can even see, independent of
// whatever flags the upstream itself supports — but a compromised upstream is
// still a compromised upstream.
type ToolFilter struct {
	Allow  []string          `yaml:"allow"`
	Deny   []string          `yaml:"deny"`
	Rename map[string]string `yaml:"rename"`

	// StripAnnotations drops the tools' annotations object from the catalog.
	StripAnnotations bool `yaml:"strip_annotations"`
	// StripOutputSchema drops the tools' outputSchema from the catalog.
	StripOutputSchema bool `yaml:"strip_output_schema"`
	// MaxDescription, when > 0, truncates tool descriptions to at most this
	// many runes (an ellipsis is appended when truncation happened). 0 = no
	// limit. Descriptions overridden via Describe are never truncated.
	MaxDescription int `yaml:"max_description"`
	// Describe replaces a tool's description wholesale, keyed by the
	// upstream's ORIGINAL tool name (before any rename) — the same keying
	// Rename uses. An empty value is ignored (keeps the upstream's own text).
	Describe map[string]string `yaml:"describe"`
}

// SameLaunch reports whether two upstreams would launch identically — same
// transport and every field that affects how the upstream is reached. Used by
// hot-reload (Stage 7d) to tell an unchanged upstream (leave running) from a
// changed one (Close + relaunch). Name is assumed equal by the caller (it is the
// match key); Enabled is intentionally NOT compared here (in any of its three
// states — absent, true, false) — enable/disable is handled as add/remove by
// the reload diff, not as a "changed launch". The call-limit fields (RateLimit/MaxConcurrent/MaxResultBytes/CallTimeout, Round
// 6) are intentionally NOT compared either: they never affect how the process
// is launched, and the registry re-reads them from the live config on every
// call, so a reload that only tweaks a limit must leave the upstream running.
func (u Upstream) SameLaunch(other Upstream) bool {
	if u.ResolveKind() != other.ResolveKind() ||
		u.Command != other.Command ||
		u.URL != other.URL ||
		!slices.Equal(u.Args, other.Args) ||
		!maps.Equal(u.Env, other.Env) ||
		!maps.Equal(u.Headers, other.Headers) {
		return false
	}
	return true
}

// SameFilter reports whether two upstreams project the same tool filter
// (allow/deny/rename plus the projection rules: strip_annotations,
// strip_output_schema, max_description, describe). It is deliberately SEPARATE
// from SameLaunch: the launch predicate is about how the upstream PROCESS is
// reached, while the filter is only a projection of its catalog — hot-reload
// (Stage 9) uses the distinction to re-apply a changed filter to the stored
// raw tool list without relaunching (or even re-listing) an
// otherwise-identical upstream.
func (u Upstream) SameFilter(other Upstream) bool {
	return slices.Equal(u.Tools.Allow, other.Tools.Allow) &&
		slices.Equal(u.Tools.Deny, other.Tools.Deny) &&
		maps.Equal(u.Tools.Rename, other.Tools.Rename) &&
		u.Tools.StripAnnotations == other.Tools.StripAnnotations &&
		u.Tools.StripOutputSchema == other.Tools.StripOutputSchema &&
		u.Tools.MaxDescription == other.Tools.MaxDescription &&
		maps.Equal(u.Tools.Describe, other.Tools.Describe)
}

// AllowSet returns Allow as a lookup set. Both consumers of the filter
// semantics (the registry's catalog projection and validateToolFilter here)
// build their sets through this method, so "what counts as allowed" is encoded
// in one place.
func (f ToolFilter) AllowSet() map[string]bool {
	allow := make(map[string]bool, len(f.Allow))
	for _, a := range f.Allow {
		allow[a] = true
	}
	return allow
}

// DenySet returns Deny as a lookup set — AllowSet's counterpart.
func (f ToolFilter) DenySet() map[string]bool {
	deny := make(map[string]bool, len(f.Deny))
	for _, d := range f.Deny {
		deny[d] = true
	}
	return deny
}

// IsEnabled reports whether this upstream should be brought up: an absent
// `enabled:` key (nil) means ENABLED, an explicit `enabled: false` disables it.
// The ONE place that default lives — every reader (Registry.Start's launch
// filter, the reload diff and enabledByName) goes through this method, so the
// three of them cannot drift apart on what "enabled" means.
func (u Upstream) IsEnabled() bool {
	if u.Enabled == nil {
		return true
	}
	return *u.Enabled
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
	// Zero selects DefaultCallTimeout. Individual upstreams may override it via
	// their own call_timeout (EffectiveCallTimeoutFor).
	CallTimeout time.Duration `yaml:"call_timeout"`

	// RateLimit, when set, applies a token-bucket limit to forwarded tools/call
	// requests, per upstream (each upstream gets its own bucket with these
	// parameters — the global value is a default, not a shared budget). nil =
	// no rate limiting. Individual upstreams may override or disable it via
	// their own rate_limit block (EffectiveRateLimitFor).
	RateLimit *RateLimit `yaml:"rate_limit"`
	// MaxResultBytes, when > 0, truncates oversized textual tool results to
	// roughly this many bytes before returning them to the client (opt-in token
	// protection). 0 = unlimited (the default). Individual upstreams may
	// override it via their own max_result_bytes (EffectiveMaxResultBytesFor).
	MaxResultBytes int `yaml:"max_result_bytes"`

	// CatalogMode selects how tools/list presents the aggregated catalog
	// (Round 8). "" or "normal": every namespaced upstream tool, as always.
	// "lazy": a fixed set of three gateway meta-tools (gate_search_tools,
	// gate_describe, gate_call) through which the client discovers and calls
	// the real catalog on demand — progressive tool disclosure that keeps the
	// client's tool list tiny no matter how many upstreams are aggregated.
	//
	// PRECEDENCE (documented contract): when CatalogMode is "lazy", PageSize
	// is IGNORED — the three meta-tools are a fixed, tiny list that never
	// paginates. Like the Round 6 limits, this field is read from the live
	// config on every request, so a SIGHUP reload switches modes without
	// relaunching anything.
	CatalogMode string `yaml:"catalog_mode"`
	// PageSize, when > 0, paginates tools/list responses into pages of at
	// most this many tools, linked by an opaque nextCursor (MCP pagination).
	// 0 (the default) keeps the old behaviour: the whole catalog in a single
	// response, any cursor ignored. Ignored entirely in lazy catalog mode
	// (see CatalogMode).
	PageSize int `yaml:"page_size"`

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

// upstreamByName returns the upstream config entry with the given name, or nil
// when the config has no such upstream. Same linear-scan lookup pattern as the
// registry's filterFor: units-to-tens of upstreams, not a hot path worth an
// index.
func (c *Config) upstreamByName(name string) *Upstream {
	for i := range c.Upstreams {
		if c.Upstreams[i].Name == name {
			return &c.Upstreams[i]
		}
	}
	return nil
}

// EffectiveRateLimitFor resolves the rate limit that applies to the named
// upstream: its own rate_limit block when set, otherwise the global one.
// ok=false means no rate limiting applies (neither level configured it, the
// winning block has rps <= 0, or the upstream is unknown and there is no
// global limit — an unknown upstream inherits the global default, consistent
// with per-upstream lookups elsewhere treating "absent from config" as "no
// overrides"). A Burst of 0 in the winning block is normalized to 1 (a bucket
// that can never hold a token would block forever).
func (c *Config) EffectiveRateLimitFor(name string) (rps float64, burst int, ok bool) {
	rl := c.RateLimit
	if u := c.upstreamByName(name); u != nil && u.RateLimit != nil {
		rl = u.RateLimit
	}
	if rl == nil || rl.RPS <= 0 {
		return 0, 0, false
	}
	burst = rl.Burst
	if burst < 1 {
		burst = 1
	}
	return rl.RPS, burst, true
}

// EffectiveMaxConcurrentFor returns the named upstream's max_concurrent cap,
// or 0 (unlimited) when unset or the upstream is unknown. Per-upstream only —
// there is no global default (see the Upstream field comment).
func (c *Config) EffectiveMaxConcurrentFor(name string) int {
	if u := c.upstreamByName(name); u != nil && u.MaxConcurrent > 0 {
		return u.MaxConcurrent
	}
	return 0
}

// EffectiveMaxResultBytesFor resolves the result-size cap for the named
// upstream: its own max_result_bytes when set (an explicit 0 disables the
// global cap for this upstream), otherwise the global MaxResultBytes.
// 0 = unlimited.
func (c *Config) EffectiveMaxResultBytesFor(name string) int {
	if u := c.upstreamByName(name); u != nil && u.MaxResultBytes != nil {
		return *u.MaxResultBytes
	}
	return c.MaxResultBytes
}

// EffectiveCallTimeoutFor resolves the request timeout for the named upstream:
// its own call_timeout when > 0, otherwise the global EffectiveCallTimeout
// (which itself falls back to DefaultCallTimeout).
func (c *Config) EffectiveCallTimeoutFor(name string) time.Duration {
	if u := c.upstreamByName(name); u != nil && u.CallTimeout > 0 {
		return u.CallTimeout
	}
	return c.EffectiveCallTimeout()
}

// LazyCatalog reports whether the lazy catalog mode (Round 8) is active —
// the one place the string comparison lives, so the dispatcher never spells
// "lazy" itself.
func (c *Config) LazyCatalog() bool { return c.CatalogMode == CatalogModeLazy }

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

// ResolvePath returns the concrete config file path Load would read for the
// given --config value: path itself when non-empty, otherwise the default
// config next to the running binary. It does not check that the file exists —
// callers that need to watch the path (serve --watch-config stats it for
// mtime changes) must know WHERE the config lives even before it does.
func ResolvePath(path string) (string, error) {
	if path != "" {
		return path, nil
	}
	return defaultConfigPath()
}

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
	// STRICT parsing (Stage 18): an unknown or misspelled key is a startup
	// error, not a silently ignored line. The class of bug this closes is
	// specific and had already bitten: `enabld: false` used to leave the
	// upstream ENABLED, with nothing anywhere saying why. yaml.v3 names both the
	// offending key and its line number, so no home-grown spell-checking is
	// needed on top.
	//
	// Anchors and merge keys (<<:) keep working under KnownFields — verified,
	// including struct elements of the upstreams list. Note the one behavioural
	// gap between the two APIs: on an EMPTY file Decode returns io.EOF where
	// Unmarshal left a zero Config, so that case is restored explicitly (the
	// zero Config then fails validation with its own, clearer message).
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			cfg = Config{}
		} else {
			var typeErr *yaml.TypeError
			if errors.As(err, &typeErr) {
				// No version number in this text on purpose: it is written before
				// the release it ships in is cut, so naming one would risk telling
				// the user a version that never existed (found by review, L8).
				return nil, fmt.Errorf("parse config %q: %w\nunknown or misspelled config keys are fatal; fix or remove the keys listed above", path, err)
			}
			return nil, fmt.Errorf("parse config %q: %w", path, err)
		}
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

	// An HTTP gateway bound beyond loopback with no auth_token would hand every
	// aggregated upstream tool to anyone on the network — reject that outright
	// instead of silently serving it (security audit finding). stdio has no
	// listen address, so the check applies to TransportHTTP only.
	if c.Transport == TransportHTTP && c.AuthToken == "" && !isLoopbackAddr(c.EffectiveListenAddr()) {
		return fmt.Errorf("listen_addr %q is not loopback-only but auth_token is empty: the HTTP endpoint would be reachable from the network without authentication — set auth_token or bind to a loopback address (127.0.0.1/::1/localhost)", c.EffectiveListenAddr())
	}

	if err := c.Restart.validate(); err != nil {
		return err
	}

	if err := c.RateLimit.validate("rate_limit"); err != nil {
		return err
	}
	if c.MaxResultBytes < 0 {
		return fmt.Errorf("max_result_bytes must not be negative (0 means unlimited)")
	}
	if c.CallTimeout < 0 {
		return fmt.Errorf("call_timeout must not be negative (0 selects the default)")
	}

	switch c.CatalogMode {
	case "", CatalogModeNormal, CatalogModeLazy:
	default:
		return fmt.Errorf("unknown catalog_mode %q (want %q or %q)", c.CatalogMode, CatalogModeNormal, CatalogModeLazy)
	}
	if c.PageSize < 0 {
		return fmt.Errorf("page_size must not be negative (0 disables tools/list pagination)")
	}

	// The opt-in payload debug log must never share a file with the audit log:
	// payloads carry raw arguments/results (possibly secrets), which the audit
	// log is required to stay free of (SKILL §6). Reject the overlap outright.
	// Both paths are canonicalized via filepath.Abs first, so a relative
	// spelling like "logs/sub/../a.log" cannot dodge the check against
	// "logs/a.log" (Load already resolved them against the config dir; Abs
	// additionally collapses "."/".." — it does not resolve symlinks, which is
	// a separate, deeper concern).
	if c.DebugPayloadLog != "" && c.LogFile != "" {
		dp, err1 := filepath.Abs(c.DebugPayloadLog)
		lf, err2 := filepath.Abs(c.LogFile)
		if err1 == nil && err2 == nil && dp == lf {
			return fmt.Errorf("debug_payload_log must not equal log_file (%q): payloads may contain secrets and must not leak into the audit log", c.LogFile)
		}
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
		if err := validateUpstreamLimits(u); err != nil {
			return err
		}
	}
	return nil
}

// IsLoopbackHost reports whether host (already stripped of any port) names a
// loopback interface: the literal "localhost" (matched by string,
// case-insensitively — net.ParseIP cannot resolve names) or an IP in a
// loopback range (127.0.0.0/8, ::1). Shared by Validate's listen_addr check
// and the transport layer's browser-Origin check, so both use the SAME
// definition of "loopback" — the two used to diverge (config: the full
// IsLoopback() range; transport: exact-string 127.0.0.1/::1 only — found by
// review).
func IsLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// isLoopbackAddr reports whether a "host:port" listen address binds to a
// loopback interface only. An address whose host part is empty (":28080")
// binds ALL interfaces and is therefore NOT loopback; an unparsable address is
// conservatively treated as non-loopback too — net.Listen will reject it later
// anyway.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return IsLoopbackHost(host)
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
	allow := f.AllowSet()
	deny := f.DenySet()

	if f.MaxDescription < 0 {
		return fmt.Errorf("upstream %q: tools.max_description must not be negative (0 means unlimited)", u.Name)
	}
	// Describe keys follow the same rule as Rename keys: with a non-empty
	// allow-list, a key outside it could never apply — a config mistake.
	// Sorted for a deterministic error message (map iteration is randomized).
	for _, orig := range slices.Sorted(maps.Keys(f.Describe)) {
		if len(f.Allow) > 0 && !allow[orig] {
			return fmt.Errorf("upstream %q: tools.describe key %q is not in tools.allow — the description override could never apply", u.Name, orig)
		}
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
		defaultName := u.Name + "__" + a
		// An allow entry without a rename surfaces under this default name, so
		// it must satisfy the same client-facing name constraint rename targets
		// already do (MCP_NOTES §6) — an original name like "weird name!" would
		// otherwise smuggle invalid characters past the naming guarantee.
		if !upstreamNameRe.MatchString(defaultName) {
			return fmt.Errorf("upstream %q: tools.allow entry %q (without a rename) would produce client-facing name %q, which does not match %s", u.Name, a, defaultName, upstreamNameRe)
		}
		if err := claim(defaultName, fmt.Sprintf("upstream %q (tool %q)", u.Name, a)); err != nil {
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

// validateUpstreamLimits rejects actively-wrong per-upstream call-limit values
// (Round 6). Unset fields are fine: 0 / nil mean "unlimited" or "inherit the
// global default" (see the field comments and the Effective*For helpers).
func validateUpstreamLimits(u Upstream) error {
	if err := u.RateLimit.validate(fmt.Sprintf("upstream %q: rate_limit", u.Name)); err != nil {
		return err
	}
	if u.MaxConcurrent < 0 {
		return fmt.Errorf("upstream %q: max_concurrent must not be negative (0 means unlimited)", u.Name)
	}
	if u.MaxResultBytes != nil && *u.MaxResultBytes < 0 {
		return fmt.Errorf("upstream %q: max_result_bytes must not be negative (0 means unlimited)", u.Name)
	}
	if u.CallTimeout < 0 {
		return fmt.Errorf("upstream %q: call_timeout must not be negative (0 inherits the global call_timeout)", u.Name)
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
	default:
		return fmt.Errorf("upstream %q: unknown kind %q (want %q or %q)", u.Name, u.Kind, UpstreamStdio, UpstreamHTTP)
	}
	return nil
}
