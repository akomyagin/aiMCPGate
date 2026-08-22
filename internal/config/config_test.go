package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// boolPtr is a tiny helper for the *bool fields (Upstream.Enabled,
// RestartPolicy.Enabled) whose nil means "key absent in YAML".
func boolPtr(b bool) *bool { return &b }

func TestValidateUpstreamTransport(t *testing.T) {
	tests := []struct {
		name    string
		up      Upstream
		wantErr bool
	}{
		{"stdio ok", Upstream{Name: "a", Command: "echo"}, false},
		{"http ok", Upstream{Name: "a", URL: "http://x/mcp"}, false},
		{"both set", Upstream{Name: "a", Command: "echo", URL: "http://x"}, true},
		{"neither set", Upstream{Name: "a"}, true},
		{"kind http but only command", Upstream{Name: "a", Kind: UpstreamHTTP, Command: "echo"}, true},
		{"kind stdio but only url", Upstream{Name: "a", Kind: UpstreamStdio, URL: "http://x"}, true},
		{"kind http matches url", Upstream{Name: "a", Kind: UpstreamHTTP, URL: "http://x"}, false},
		{"unknown kind", Upstream{Name: "a", Kind: "grpc", Command: "echo"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUpstreamTransport(tt.up)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateUpstreamTransport(%+v) err = %v, wantErr %v", tt.up, err, tt.wantErr)
			}
		})
	}
}

func TestValidateRejectsDuplicateAndBadNames(t *testing.T) {
	c := &Config{Transport: TransportStdio, Upstreams: []Upstream{
		{Name: "a", Command: "x"},
		{Name: "a", Command: "y"},
	}}
	if err := c.Validate(); err == nil {
		t.Fatal("expected duplicate-name error")
	}

	c = &Config{Transport: TransportStdio, Upstreams: []Upstream{
		{Name: "bad name!", Command: "x"},
	}}
	if err := c.Validate(); err == nil {
		t.Fatal("expected bad-name error (namespacing constraint)")
	}
}

func TestValidateRejectsPayloadLogEqualsLogFile(t *testing.T) {
	// Sharing the file would leak raw arguments (possible secrets) into the
	// metadata-only audit log — a hard security invariant (SKILL §6, Stage 10).
	c := &Config{
		Transport:       TransportStdio,
		LogFile:         "./logs/calls.jsonl",
		DebugPayloadLog: "./logs/calls.jsonl",
		Upstreams:       []Upstream{{Name: "a", Command: "x"}},
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error when debug_payload_log equals log_file")
	}

	// A distinct payload-log path is accepted.
	c.DebugPayloadLog = "./logs/payloads.jsonl"
	if err := c.Validate(); err != nil {
		t.Fatalf("distinct payload-log path should be valid, got %v", err)
	}

	// Empty (disabled) is always fine, even if log_file is set.
	c.DebugPayloadLog = ""
	if err := c.Validate(); err != nil {
		t.Fatalf("empty payload log should be valid, got %v", err)
	}

	// A relative spelling that resolves to the SAME file must be caught too:
	// the comparison canonicalizes via filepath.Abs, so "logs/sub/../a.log"
	// cannot dodge the check against "logs/a.log".
	c.LogFile = "logs/a.log"
	c.DebugPayloadLog = "logs/sub/../a.log"
	if err := c.Validate(); err == nil {
		t.Fatal("expected error when debug_payload_log resolves to log_file via ..")
	}
}

// TestValidateRequiresAuthTokenForNonLoopbackListen pins the security invariant:
// an HTTP gateway bound beyond loopback without auth_token would expose every
// aggregated tool to the whole network, so Validate must reject it. Loopback
// binds (and any bind with a token) remain fine.
func TestValidateRequiresAuthTokenForNonLoopbackListen(t *testing.T) {
	mk := func(addr, token string) *Config {
		return &Config{
			Transport:  TransportHTTP,
			ListenAddr: addr,
			AuthToken:  token,
			Upstreams:  []Upstream{{Name: "a", Command: "x"}},
		}
	}
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"0.0.0.0 without token", mk("0.0.0.0:1234", ""), true},
		{"all-interfaces shorthand without token", mk(":1234", ""), true},
		{"LAN address without token", mk("192.168.1.10:1234", ""), true},
		{"0.0.0.0 with token", mk("0.0.0.0:1234", "s3cr3t"), false},
		{"127.0.0.1 without token", mk("127.0.0.1:1234", ""), false},
		{"localhost without token", mk("localhost:1234", ""), false},
		{"IPv6 loopback without token", mk("[::1]:1234", ""), false},
		{"default listen addr without token", mk("", ""), false}, // defaults to 127.0.0.1:28080
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// stdio transport never uses listen_addr — no token required regardless.
	c := &Config{Transport: TransportStdio, ListenAddr: "0.0.0.0:1234", Upstreams: []Upstream{{Name: "a", Command: "x"}}}
	if err := c.Validate(); err != nil {
		t.Fatalf("stdio transport must ignore listen_addr, got %v", err)
	}
}

// TestIsLoopbackHost pins the shared "is this host loopback?" definition used
// by BOTH Validate's listen_addr check and the transport layer's browser
// Origin check: the whole 127.0.0.0/8 range and ::1 count, not just the
// literal 127.0.0.1 (the two checks used to diverge — found by review).
func TestIsLoopbackHost(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"LOCALHOST", true}, // case-insensitive string match
		{"127.0.0.1", true},
		{"127.0.0.2", true}, // anywhere in 127.0.0.0/8, not just .1
		{"127.255.255.254", true},
		{"::1", true},
		{"0.0.0.0", false},
		{"192.168.1.10", false},
		{"example.com", false}, // names other than localhost are never resolved
		{"", false},
	}
	for _, tt := range tests {
		if got := IsLoopbackHost(tt.host); got != tt.want {
			t.Errorf("IsLoopbackHost(%q) = %v, want %v", tt.host, got, tt.want)
		}
	}
}

func TestValidateRejectsUnknownTransport(t *testing.T) {
	c := &Config{Transport: "grpc"}
	if err := c.Validate(); err == nil {
		t.Fatal("expected unknown-transport error")
	}
}

func TestResolveKindInference(t *testing.T) {
	if k := (Upstream{Command: "x"}).ResolveKind(); k != UpstreamStdio {
		t.Errorf("command-only ResolveKind = %q, want stdio", k)
	}
	if k := (Upstream{URL: "http://x"}).ResolveKind(); k != UpstreamHTTP {
		t.Errorf("url-only ResolveKind = %q, want http", k)
	}
	if k := (Upstream{Kind: UpstreamHTTP, Command: "x"}).ResolveKind(); k != UpstreamHTTP {
		t.Errorf("explicit kind should win, got %q", k)
	}
}

// TestLoadExpandsEnvAndValidates checks the full Load path: env-expanded secrets
// resolve from the environment (not committed literally), the YAML parses, and
// validation runs. It also confirms an unset env var expands to empty and is
// then rejected where a value is required.
func TestLoadExpandsEnvAndValidates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: http
listen_addr: ":9999"
auth_token: "gate-test-token" # ":9999" binds all interfaces, so a token is now mandatory (Validate)
upstreams:
  - name: remote
    url: https://example.com/mcp
    headers:
      Authorization: "Bearer ${TEST_MCP_TOKEN}"
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_MCP_TOKEN", "s3cr3t")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Transport != TransportHTTP {
		t.Errorf("transport = %q, want http", cfg.Transport)
	}
	if cfg.EffectiveListenAddr() != ":9999" {
		t.Errorf("listen_addr = %q, want :9999", cfg.EffectiveListenAddr())
	}
	got := cfg.Upstreams[0].Headers["Authorization"]
	if got != "Bearer s3cr3t" {
		t.Errorf("auth header = %q, want env-expanded 'Bearer s3cr3t'", got)
	}
}

// TestLoadExpandsOnlyAuthToken confirms auth_token (a top-level secret field)
// is expanded the same way upstream env/headers are.
func TestLoadExpandsOnlyAuthToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := "transport: http\nauth_token: ${TEST_AUTH_TOKEN}\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_AUTH_TOKEN", "t0ken")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthToken != "t0ken" {
		t.Errorf("auth_token = %q, want env-expanded 't0ken'", cfg.AuthToken)
	}
}

// TestLoadDoesNotMangleLiteralDollarSigns is a regression test: expansion
// used to run over the entire raw file before parsing, so any literal '$' in
// a URL, a command argument, or a path — not just in fields meant to carry
// ${VAR} references — was silently corrupted by os.ExpandEnv (found by code
// review; reproduced with a password containing '$' before the fix). Only
// auth_token and upstream env/headers values are expanded; everything else
// must survive byte-for-byte.
func TestLoadDoesNotMangleLiteralDollarSigns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: stdio
upstreams:
  - name: local
    command: /opt/tools$5/bin/mcp
    args: ["--password=p$$w0rd", "https://user:p$$ss@host/mcp"]
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	u := cfg.Upstreams[0]
	if u.Command != "/opt/tools$5/bin/mcp" {
		t.Errorf("command = %q, want literal '$' preserved", u.Command)
	}
	wantArgs := []string{"--password=p$$w0rd", "https://user:p$$ss@host/mcp"}
	for i, want := range wantArgs {
		if u.Args[i] != want {
			t.Errorf("args[%d] = %q, want literal %q", i, u.Args[i], want)
		}
	}
}

// TestLoadExpandsUpstreamEnvValues confirms per-upstream env map values
// (e.g. GITHUB_TOKEN passed to a stdio child process) are expanded, matching
// the documented secrets mechanism (config.example.yaml).
func TestLoadExpandsUpstreamEnvValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: stdio
upstreams:
  - name: gh
    command: github-mcp-server
    env:
      GITHUB_TOKEN: ${TEST_GITHUB_TOKEN}
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_GITHUB_TOKEN", "gh-s3cr3t")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Upstreams[0].Env["GITHUB_TOKEN"]
	if got != "gh-s3cr3t" {
		t.Errorf("env GITHUB_TOKEN = %q, want env-expanded 'gh-s3cr3t'", got)
	}
}

func TestLoadResolvesFilePathsAgainstConfigDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	yaml := `
transport: stdio
log_file: ./logs/calls.jsonl
skill_file: ./skill.md
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	wantLog := filepath.Join(dir, "sub", "logs", "calls.jsonl")
	if cfg.LogFile != wantLog {
		t.Errorf("log_file = %q, want %q (resolved against config dir)", cfg.LogFile, wantLog)
	}
	wantSkill := filepath.Join(dir, "sub", "skill.md")
	if cfg.SkillFile != wantSkill {
		t.Errorf("skill_file = %q, want %q (resolved against config dir)", cfg.SkillFile, wantSkill)
	}
}

// TestLoadEmptyPathErrorsWithoutDefaultConfig relies on there being no
// config.yaml next to the compiled `go test` binary (in its temp build dir) —
// true in every normal dev/CI environment. It documents that Load("") no
// longer silently returns an empty-upstream default: it errors, so `serve`
// never starts a pointless empty gateway (found by user request).
func TestLoadEmptyPathErrorsWithoutDefaultConfig(t *testing.T) {
	_, err := Load("")
	if err == nil {
		t.Fatal("Load(\"\") with no default config.yaml next to the binary should error, got nil")
	}
}

func TestLoadKeepsAbsoluteFilePathsUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	absLog := filepath.Join(dir, "elsewhere", "calls.jsonl")
	yaml := "transport: stdio\nlog_file: " + absLog + "\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LogFile != absLog {
		t.Errorf("log_file = %q, want unchanged absolute path %q", cfg.LogFile, absLog)
	}
}

// TestEffectiveRestartDefaults checks the default-fill behaviour: a config that
// never mentions `restart:` gets enabled auto-restart with the documented 1s/30s
// backoff bounds and a bounded 5 attempts.
func TestEffectiveRestartDefaults(t *testing.T) {
	c := &Config{}
	p := c.EffectiveRestart()
	if p.Enabled == nil || !*p.Enabled {
		t.Errorf("unset policy should default to enabled=true, got %+v", p.Enabled)
	}
	if p.InitialBackoff != DefaultRestartInitialBackoff {
		t.Errorf("initial_backoff = %s, want default %s", p.InitialBackoff, DefaultRestartInitialBackoff)
	}
	if p.MaxBackoff != DefaultRestartMaxBackoff {
		t.Errorf("max_backoff = %s, want default %s", p.MaxBackoff, DefaultRestartMaxBackoff)
	}
	if p.MaxAttempts != DefaultRestartMaxAttempts {
		t.Errorf("max_attempts = %d, want default %d", p.MaxAttempts, DefaultRestartMaxAttempts)
	}
}

// TestEffectiveRestartHonoursExplicitZeroAttempts is the subtle case: an
// explicit max_attempts:0 under an otherwise-populated policy means "unlimited"
// and must NOT be silently overwritten by the default-5, unlike a wholly-unset
// policy.
func TestEffectiveRestartHonoursExplicitZeroAttempts(t *testing.T) {
	yes := true
	c := &Config{Restart: RestartPolicy{Enabled: &yes, MaxAttempts: 0, InitialBackoff: 2}}
	p := c.EffectiveRestart()
	if p.MaxAttempts != 0 {
		t.Errorf("explicit max_attempts:0 (unlimited) was overwritten to %d", p.MaxAttempts)
	}
}

// TestEffectiveRestartCanBeDisabled confirms an explicit enabled:false survives
// (a plain bool could not distinguish this from "unset").
func TestEffectiveRestartCanBeDisabled(t *testing.T) {
	no := false
	c := &Config{Restart: RestartPolicy{Enabled: &no}}
	p := c.EffectiveRestart()
	if p.Enabled == nil || *p.Enabled {
		t.Errorf("explicit enabled:false should be honoured, got %+v", p.Enabled)
	}
}

// TestValidateRejectsNegativeRestartValues checks the guardrails.
func TestValidateRejectsNegativeRestartValues(t *testing.T) {
	cases := []RestartPolicy{
		{InitialBackoff: -1},
		{MaxBackoff: -1},
		{MaxAttempts: -1},
	}
	for i, rp := range cases {
		c := &Config{Transport: TransportStdio, Restart: rp}
		if err := c.Validate(); err == nil {
			t.Errorf("case %d: expected Validate to reject %+v", i, rp)
		}
	}
}

// TestSameLaunch checks the reload diff's "changed?" predicate.
func TestSameLaunch(t *testing.T) {
	base := Upstream{Name: "x", Command: "cmd", Args: []string{"a", "b"}, Env: map[string]string{"K": "v"}}
	tests := []struct {
		name string
		mod  func(Upstream) Upstream
		same bool
	}{
		{"identical", func(u Upstream) Upstream { return u }, true},
		// base leaves Enabled unset (nil = enabled); all three states must be
		// invisible to SameLaunch — enable/disable is add/remove for the reload
		// diff, never a "changed launch".
		{"enabled differs only (nil vs false)", func(u Upstream) Upstream { u.Enabled = boolPtr(false); return u }, true},
		{"enabled differs only (nil vs true)", func(u Upstream) Upstream { u.Enabled = boolPtr(true); return u }, true},
		{"command differs", func(u Upstream) Upstream { u.Command = "other"; return u }, false},
		{"args differ", func(u Upstream) Upstream { u.Args = []string{"a"}; return u }, false},
		{"env value differs", func(u Upstream) Upstream { u.Env = map[string]string{"K": "w"}; return u }, false},
		{"env key added", func(u Upstream) Upstream { u.Env = map[string]string{"K": "v", "K2": "z"}; return u }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other := tt.mod(Upstream{
				Name: base.Name, Command: base.Command,
				Args: append([]string(nil), base.Args...),
				Env:  map[string]string{"K": "v"},
			})
			if got := base.SameLaunch(other); got != tt.same {
				t.Errorf("SameLaunch = %v, want %v", got, tt.same)
			}
		})
	}
}

// TestValidateToolFilter drives the Stage 9 filter validation: rename keys must
// stay inside a non-empty allow, rename targets must be well-formed tool names,
// and the client-facing names the config makes statically known must be unique
// across the WHOLE config, not just within one upstream.
func TestValidateToolFilter(t *testing.T) {
	// two upstreams so cross-upstream collisions can be expressed
	mk := func(aTools, bTools ToolFilter) *Config {
		return &Config{Transport: TransportStdio, Upstreams: []Upstream{
			{Name: "a", Command: "x", Tools: aTools},
			{Name: "b", Command: "y", Tools: bTools},
		}}
	}
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"empty filters", mk(ToolFilter{}, ToolFilter{}), false},
		{"allow and deny only", mk(
			ToolFilter{Allow: []string{"t1", "t2"}, Deny: []string{"t2"}},
			ToolFilter{Deny: []string{"danger"}},
		), false},
		{"rename without allow", mk(
			ToolFilter{Rename: map[string]string{"t1": "short_t1"}},
			ToolFilter{},
		), false},
		{"rename plus allow, key allowed", mk(
			ToolFilter{Allow: []string{"t1"}, Rename: map[string]string{"t1": "short_t1"}},
			ToolFilter{},
		), false},
		{"rename key outside allow", mk(
			ToolFilter{Allow: []string{"t1"}, Rename: map[string]string{"t2": "short_t2"}},
			ToolFilter{},
		), true},
		{"rename target with bad characters", mk(
			ToolFilter{Rename: map[string]string{"t1": "has space"}},
			ToolFilter{},
		), true},
		{"rename target empty", mk(
			ToolFilter{Rename: map[string]string{"t1": ""}},
			ToolFilter{},
		), true},
		{"allow entry with bad characters and no rename", mk(
			// would surface as client-facing "a__weird name!" — invalid
			ToolFilter{Allow: []string{"weird name!"}},
			ToolFilter{},
		), true},
		{"allow entry with bad characters but renamed", mk(
			// the rename replaces the default name, so the odd original is fine
			ToolFilter{Allow: []string{"weird name!"}, Rename: map[string]string{"weird name!": "tamed"}},
			ToolFilter{},
		), false},
		{"renamed names collide across upstreams", mk(
			ToolFilter{Rename: map[string]string{"t1": "shared"}},
			ToolFilter{Rename: map[string]string{"other": "shared"}},
		), true},
		{"rename collides with other upstream default name", mk(
			ToolFilter{Allow: []string{"t1"}}, // client-facing "a__t1"
			ToolFilter{Rename: map[string]string{"x": "a__t1"}},
		), true},
		{"same client name freed by deny", mk(
			// a's rename never materializes (denied), so b may take the name
			ToolFilter{Deny: []string{"t1"}, Rename: map[string]string{"t1": "shared"}},
			ToolFilter{Rename: map[string]string{"x": "shared"}},
		), false},
		{"negative max_description", mk(
			ToolFilter{MaxDescription: -1},
			ToolFilter{},
		), true},
		{"describe without allow", mk(
			ToolFilter{Describe: map[string]string{"t1": "short text"}},
			ToolFilter{},
		), false},
		{"describe key inside allow", mk(
			ToolFilter{Allow: []string{"t1"}, Describe: map[string]string{"t1": "short text"}},
			ToolFilter{},
		), false},
		{"describe key outside allow", mk(
			ToolFilter{Allow: []string{"t1"}, Describe: map[string]string{"t2": "never applies"}},
			ToolFilter{},
		), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestSameFilter checks the reload diff's FILTER-ONLY predicate — and that a
// filter change alone does NOT flip SameLaunch (the whole point of keeping the
// two predicates separate: filter-only reload must not relaunch the process).
func TestSameFilter(t *testing.T) {
	base := Upstream{Name: "x", Command: "cmd", Tools: ToolFilter{
		Allow:             []string{"a", "b"},
		Deny:              []string{"c"},
		Rename:            map[string]string{"a": "short_a"},
		StripAnnotations:  true,
		StripOutputSchema: true,
		MaxDescription:    100,
		Describe:          map[string]string{"a": "text"},
	}}
	clone := func() Upstream {
		u := base
		u.Tools = ToolFilter{
			Allow:             append([]string(nil), base.Tools.Allow...),
			Deny:              append([]string(nil), base.Tools.Deny...),
			Rename:            map[string]string{"a": "short_a"},
			StripAnnotations:  true,
			StripOutputSchema: true,
			MaxDescription:    100,
			Describe:          map[string]string{"a": "text"},
		}
		return u
	}
	tests := []struct {
		name string
		mod  func(*Upstream)
		same bool
	}{
		{"identical", func(*Upstream) {}, true},
		{"allow differs", func(u *Upstream) { u.Tools.Allow = []string{"a"} }, false},
		{"deny differs", func(u *Upstream) { u.Tools.Deny = []string{"c", "d"} }, false},
		{"rename differs", func(u *Upstream) { u.Tools.Rename["a"] = "other" }, false},
		{"strip_annotations differs", func(u *Upstream) { u.Tools.StripAnnotations = false }, false},
		{"strip_output_schema differs", func(u *Upstream) { u.Tools.StripOutputSchema = false }, false},
		{"max_description differs", func(u *Upstream) { u.Tools.MaxDescription = 50 }, false},
		{"describe differs", func(u *Upstream) { u.Tools.Describe["a"] = "other" }, false},
		{"describe key added", func(u *Upstream) { u.Tools.Describe["b"] = "more" }, false},
		{"launch fields ignored by SameFilter", func(u *Upstream) { u.Command = "other" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other := clone()
			tt.mod(&other)
			if got := base.SameFilter(other); got != tt.same {
				t.Errorf("SameFilter = %v, want %v", got, tt.same)
			}
			// A filter-only difference must never look like a launch change.
			if !base.SameLaunch(clone()) {
				t.Error("sanity: identical launch reported as changed")
			}
		})
	}

	filterOnly := clone()
	filterOnly.Tools.Deny = []string{"c", "extra"}
	if !base.SameLaunch(filterOnly) {
		t.Error("filter change flipped SameLaunch — it must stay a launch-identical upstream")
	}
	if base.SameFilter(filterOnly) {
		t.Error("changed deny list reported as same filter")
	}
}

// TestLoadUpstreamEnabledDefaultsToEnabled pins the three states of the
// `enabled:` key. The absent one is the point of the whole field being a
// pointer: an upstream added by hand without `enabled:` used to be dropped
// silently (no log line, no doctor row, no tools) — it must now come up.
func TestLoadUpstreamEnabledDefaultsToEnabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: stdio
upstreams:
  - name: omitted
    command: /bin/true
  - name: off
    command: /bin/true
    enabled: false
  - name: on
    command: /bin/true
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Upstreams) != 3 {
		t.Fatalf("parsed %d upstreams, want 3", len(cfg.Upstreams))
	}

	tests := []struct {
		name    string
		wantPtr *bool // nil = key must have been absent
		want    bool
	}{
		{"omitted", nil, true},
		{"off", boolPtr(false), false},
		{"on", boolPtr(true), true},
	}
	for i, tt := range tests {
		u := cfg.Upstreams[i]
		if u.Name != tt.name {
			t.Fatalf("upstreams[%d].Name = %q, want %q", i, u.Name, tt.name)
		}
		switch {
		case tt.wantPtr == nil && u.Enabled != nil:
			t.Errorf("%s: Enabled = %v, want nil (key absent in YAML)", tt.name, *u.Enabled)
		case tt.wantPtr != nil && u.Enabled == nil:
			t.Errorf("%s: Enabled = nil, want explicit %v", tt.name, *tt.wantPtr)
		case tt.wantPtr != nil && *u.Enabled != *tt.wantPtr:
			t.Errorf("%s: Enabled = %v, want %v", tt.name, *u.Enabled, *tt.wantPtr)
		}
		if got := u.IsEnabled(); got != tt.want {
			t.Errorf("%s: IsEnabled() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestLoadParsesToolFilter confirms the YAML shape of the tools block
// (allow/deny/rename plus the projection rules) round-trips through Load.
func TestLoadParsesToolFilter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
upstreams:
  - name: grafana
    command: grafana-mcp
    enabled: true
    tools:
      allow: ["query_prometheus", "list_dashboards"]
      deny: ["delete_dashboard"]
      rename:
        query_prometheus: "grafana_query"
      strip_annotations: true
      strip_output_schema: true
      max_description: 200
      describe:
        list_dashboards: "List Grafana dashboards."
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	f := cfg.Upstreams[0].Tools
	if len(f.Allow) != 2 || f.Allow[0] != "query_prometheus" {
		t.Errorf("allow = %v, want [query_prometheus list_dashboards]", f.Allow)
	}
	if len(f.Deny) != 1 || f.Deny[0] != "delete_dashboard" {
		t.Errorf("deny = %v, want [delete_dashboard]", f.Deny)
	}
	if f.Rename["query_prometheus"] != "grafana_query" {
		t.Errorf("rename = %v, want query_prometheus→grafana_query", f.Rename)
	}
	if !f.StripAnnotations || !f.StripOutputSchema {
		t.Errorf("strip flags = %v/%v, want true/true", f.StripAnnotations, f.StripOutputSchema)
	}
	if f.MaxDescription != 200 {
		t.Errorf("max_description = %d, want 200", f.MaxDescription)
	}
	if f.Describe["list_dashboards"] != "List Grafana dashboards." {
		t.Errorf("describe = %v, want list_dashboards→\"List Grafana dashboards.\"", f.Describe)
	}
}

// TestSameLaunchHTTP checks url/headers are compared for http upstreams.
func TestSameLaunchHTTP(t *testing.T) {
	a := Upstream{Name: "h", URL: "https://x/mcp", Headers: map[string]string{"Authorization": "Bearer a"}}
	b := a
	b.Headers = map[string]string{"Authorization": "Bearer b"}
	if a.SameLaunch(b) {
		t.Error("http upstreams with different headers must not be SameLaunch")
	}
	c := a
	c.Headers = map[string]string{"Authorization": "Bearer a"}
	if !a.SameLaunch(c) {
		t.Error("http upstreams with identical url/headers must be SameLaunch")
	}
}

// TestValidateCatalogModeAndPageSize covers the Round 8 knobs: the three legal
// catalog_mode spellings (unset/normal/lazy), rejection of anything else, the
// non-negative page_size constraint, and that lazy + page_size together is a
// VALID config (the documented precedence — lazy ignores page_size — is a
// runtime rule in the dispatcher, not a validation error).
func TestValidateCatalogModeAndPageSize(t *testing.T) {
	mk := func(mode string, pageSize int) *Config {
		return &Config{
			Transport:   TransportStdio,
			CatalogMode: mode,
			PageSize:    pageSize,
			Upstreams:   []Upstream{{Name: "a", Command: "x"}},
		}
	}
	tests := []struct {
		name    string
		mode    string
		ps      int
		wantErr bool
	}{
		{"unset defaults", "", 0, false},
		{"explicit normal", CatalogModeNormal, 0, false},
		{"lazy", CatalogModeLazy, 0, false},
		{"page_size positive", "", 5, false},
		{"lazy with page_size is valid (precedence is runtime, not validation)", CatalogModeLazy, 10, false},
		{"unknown mode", "eager", 0, true},
		{"negative page_size", "", -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mk(tt.mode, tt.ps).Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate(catalog_mode=%q, page_size=%d) err = %v, wantErr %v", tt.mode, tt.ps, err, tt.wantErr)
			}
		})
	}
}

// TestLazyCatalogHelper pins the one place the "lazy" comparison lives.
func TestLazyCatalogHelper(t *testing.T) {
	if (&Config{}).LazyCatalog() || (&Config{CatalogMode: CatalogModeNormal}).LazyCatalog() {
		t.Error("normal/unset config must not report LazyCatalog")
	}
	if !(&Config{CatalogMode: CatalogModeLazy}).LazyCatalog() {
		t.Error("catalog_mode lazy must report LazyCatalog")
	}
}

// TestLoadRejectsUnknownKey (Stage 18, K1): a misspelled key is a startup error
// naming both the key and its line — the whole point of KnownFields(true). The
// motivating case is exactly this one: `enabld` used to be ignored, leaving the
// upstream ENABLED with nothing anywhere explaining why.
func TestLoadRejectsUnknownKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
log_file: ./calls.jsonl
upstreams:
  - name: github
    command: /bin/true
    enabld: false
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load accepted a misspelled key; unknown keys must be fatal")
	}
	msg := err.Error()
	if !strings.Contains(msg, "enabld") {
		t.Errorf("error does not name the offending key: %v", err)
	}
	if !strings.Contains(msg, "line") {
		t.Errorf("error does not give a line number: %v", err)
	}
}

// TestLoadAcceptsAnchorsAndMergeKeys (K2) is the regression guard against
// RE-implementing strictness by hand: YAML anchors and merge keys (<<:) must
// keep working, both inside a map field and across struct elements of the
// upstreams list.
func TestLoadAcceptsAnchorsAndMergeKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
log_file: ./calls.jsonl
upstreams:
  - name: first
    command: /bin/true
    env: &common
      SHARED: yes
      REGION: eu
  - name: second
    command: /bin/true
    env:
      <<: *common
      REGION: us
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load rejected anchors/merge keys: %v", err)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("got %d upstreams, want 2", len(cfg.Upstreams))
	}
	env := cfg.Upstreams[1].Env
	if env["SHARED"] != "yes" {
		t.Errorf("merged key SHARED = %q, want the anchor's value", env["SHARED"])
	}
	if env["REGION"] != "us" {
		t.Errorf("REGION = %q, want the local override to win over the merge", env["REGION"])
	}
}

// TestLoadEmptyFileKeepsOldBehaviour (K3) closes the one behavioural gap between
// yaml.Unmarshal and a strict Decoder: an empty file makes Decode return io.EOF
// where Unmarshal quietly produced a zero Config. Pre-Stage-18, an empty config
// LOADED (Validate does not require upstreams — Registry.Start is what refuses
// an empty gateway, with a far better message); that must stay true, and no
// operator may ever be shown a bare "EOF".
func TestLoadEmptyFileKeepsOldBehaviour(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			t.Fatalf("empty config now fails with a raw parse EOF; the pre-Stage-18 behaviour was a zero Config: %v", err)
		}
		t.Fatalf("Load on an empty config: %v, want the same success it gave before", err)
	}
	if len(cfg.Upstreams) != 0 {
		t.Errorf("empty config produced %d upstreams, want 0", len(cfg.Upstreams))
	}
	// The usual defaults are still applied on top of the zero Config.
	if cfg.Transport != TransportStdio {
		t.Errorf("transport = %q, want the stdio default", cfg.Transport)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("log_level = %q, want the info default", cfg.LogLevel)
	}
}

// T1. TestLoadFailsOnUnsetAuthTokenVar: an auth_token that points at an UNSET
// environment variable must fail Load, naming the variable and auth_token in
// the error. The failure is transport-INDEPENDENT (§4.2): a config carrying
// the key declares the intent "there is a token", and configs get switched
// between modes. Two unset variables must BOTH be named (Validate collects
// them, so the operator fixes them in one pass).
func TestLoadFailsOnUnsetAuthTokenVar(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		t.Run(transport, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.yaml")
			yaml := "transport: " + transport + "\nauth_token: ${TEST_AIMCPGATE_UNSET_TOK}\n"
			if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatalf("Load accepted an unset auth_token variable (transport %s); want a hard error", transport)
			}
			if !strings.Contains(err.Error(), "TEST_AIMCPGATE_UNSET_TOK") {
				t.Errorf("error must name the variable: %v", err)
			}
			if !strings.Contains(err.Error(), "auth_token") {
				t.Errorf("error must name auth_token: %v", err)
			}
		})
	}

	t.Run("two unset vars both named", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		yaml := "transport: http\nauth_token: ${TEST_AIMCPGATE_UNSET_A}${TEST_AIMCPGATE_UNSET_B}\n"
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(path)
		if err == nil {
			t.Fatal("Load accepted two unset auth_token variables; want a hard error")
		}
		for _, want := range []string{"TEST_AIMCPGATE_UNSET_A", "TEST_AIMCPGATE_UNSET_B"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error must name %q, got: %v", want, err)
			}
		}
	})
}

// T2. TestLoadRecordsResolvedAuthTokenRef: a SET auth_token variable resolves,
// Load succeeds, and SecretVarRefs carries exactly one resolved auth_token ref.
func TestLoadRecordsResolvedAuthTokenRef(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := "transport: http\nauth_token: ${TEST_AIMCPGATE_SET_TOK}\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_AIMCPGATE_SET_TOK", "t0ken")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AuthToken != "t0ken" {
		t.Errorf("auth_token = %q, want expanded 't0ken'", cfg.AuthToken)
	}
	want := []SecretVarRef{{Upstream: "", Field: "auth_token", Key: "", Var: "TEST_AIMCPGATE_SET_TOK", Resolved: true}}
	if !secretRefsEqual(cfg.SecretVarRefs, want) {
		t.Errorf("SecretVarRefs = %+v, want %+v", cfg.SecretVarRefs, want)
	}
}

// T3. TestLoadAcceptsSetButEmptyAuthTokenVar: a variable SET to "" is the
// operator's deliberate choice (§4.1) — Load passes, the ref is Resolved, and
// auth_token becomes empty. os.LookupEnv is what tells this apart from unset.
func TestLoadAcceptsSetButEmptyAuthTokenVar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := "transport: stdio\nauth_token: ${TEST_AIMCPGATE_EMPTY_TOK}\n"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_AIMCPGATE_EMPTY_TOK", "")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load rejected a set-but-empty auth_token variable: %v", err)
	}
	if cfg.AuthToken != "" {
		t.Errorf("auth_token = %q, want empty", cfg.AuthToken)
	}
	if len(cfg.SecretVarRefs) != 1 || !cfg.SecretVarRefs[0].Resolved {
		t.Errorf("SecretVarRefs = %+v, want one Resolved ref", cfg.SecretVarRefs)
	}
}

// T4. TestLoadRecordsUnresolvedUpstreamEnvAndHeaderRefs: unset variables in an
// upstream's env and another's headers do NOT fail Load; the values become
// empty and the refs carry full coordinates with Resolved=false.
func TestLoadRecordsUnresolvedUpstreamEnvAndHeaderRefs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: stdio
upstreams:
  - name: gh
    command: github-mcp-server
    env:
      GITHUB_TOKEN: ${TEST_AIMCPGATE_UNSET_GH}
    enabled: true
  - name: remote
    url: https://example.com/mcp
    headers:
      Authorization: "Bearer ${TEST_AIMCPGATE_UNSET_HDR}"
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load must not fail on unset upstream env/headers: %v", err)
	}
	if got := cfg.Upstreams[0].Env["GITHUB_TOKEN"]; got != "" {
		t.Errorf("env value = %q, want empty", got)
	}
	if got := cfg.Upstreams[1].Headers["Authorization"]; got != "Bearer " {
		t.Errorf("header value = %q, want 'Bearer '", got)
	}
	wantEnv := SecretVarRef{Upstream: "gh", Field: "env", Key: "GITHUB_TOKEN", Var: "TEST_AIMCPGATE_UNSET_GH", Resolved: false}
	wantHdr := SecretVarRef{Upstream: "remote", Field: "headers", Key: "Authorization", Var: "TEST_AIMCPGATE_UNSET_HDR", Resolved: false}
	if !containsRef(cfg.SecretVarRefs, wantEnv) {
		t.Errorf("missing env ref %+v in %+v", wantEnv, cfg.SecretVarRefs)
	}
	if !containsRef(cfg.SecretVarRefs, wantHdr) {
		t.Errorf("missing header ref %+v in %+v", wantHdr, cfg.SecretVarRefs)
	}
}

// T5. TestLoadRecordsRefsOfDisabledUpstreams: a disabled upstream's unset env
// variable is still recorded (§4.4) — Load collects refs for ALL upstreams,
// symmetrically with Validate; consumers filter by enablement when reading.
func TestLoadRecordsRefsOfDisabledUpstreams(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: stdio
upstreams:
  - name: off
    command: some-server
    env:
      TOKEN: ${TEST_AIMCPGATE_UNSET_OFF}
    enabled: false
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := SecretVarRef{Upstream: "off", Field: "env", Key: "TOKEN", Var: "TEST_AIMCPGATE_UNSET_OFF", Resolved: false}
	if !containsRef(cfg.SecretVarRefs, want) {
		t.Errorf("disabled upstream ref not recorded: %+v not in %+v", want, cfg.SecretVarRefs)
	}
}

// T6. TestExpandSecretMultipleVarsInOneValue: one value with a set and an unset
// variable yields two refs (Resolved true/false) and the value uses the set one.
func TestExpandSecretMultipleVarsInOneValue(t *testing.T) {
	t.Setenv("TEST_AIMCPGATE_SET_A", "aaa")
	var refs []SecretVarRef
	got := expandSecret("Bearer ${TEST_AIMCPGATE_SET_A}${TEST_AIMCPGATE_UNSET_B}", "u", "headers", "Authorization", &refs)
	if got != "Bearer aaa" {
		t.Errorf("expanded = %q, want 'Bearer aaa'", got)
	}
	if len(refs) != 2 {
		t.Fatalf("refs = %+v, want two", refs)
	}
	if !refs[0].Resolved || refs[0].Var != "TEST_AIMCPGATE_SET_A" {
		t.Errorf("first ref = %+v, want resolved SET_A", refs[0])
	}
	if refs[1].Resolved || refs[1].Var != "TEST_AIMCPGATE_UNSET_B" {
		t.Errorf("second ref = %+v, want unresolved UNSET_B", refs[1])
	}
}

// T7. TestExpandSecretIgnoresShellSpecials: shell-special names ($$, $5, ${5})
// are NOT variable references — Load does not fail, no refs are recorded, and
// the expansion result matches the old os.ExpandEnv byte-for-byte (§4.3).
func TestExpandSecretIgnoresShellSpecials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: http
auth_token: "p$$w"
upstreams:
  - name: remote
    url: https://example.com/mcp
    headers:
      A: "$5"
      B: "${5}"
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load must not fail on shell specials: %v", err)
	}
	// os.ExpandEnv("p$$w") == "pw" (the "$$" -> "$"... actually $$ is $-then-$,
	// os.Expand consumes "$$" as the special "$" mapping to ""). Match the
	// live os.ExpandEnv on the same inputs to pin "no behaviour change".
	if want := os.ExpandEnv("p$$w"); cfg.AuthToken != want {
		t.Errorf("auth_token = %q, want os.ExpandEnv-equivalent %q", cfg.AuthToken, want)
	}
	if want := os.ExpandEnv("$5"); cfg.Upstreams[0].Headers["A"] != want {
		t.Errorf("header A = %q, want %q", cfg.Upstreams[0].Headers["A"], want)
	}
	if want := os.ExpandEnv("${5}"); cfg.Upstreams[0].Headers["B"] != want {
		t.Errorf("header B = %q, want %q", cfg.Upstreams[0].Headers["B"], want)
	}
	if len(cfg.SecretVarRefs) != 0 {
		t.Errorf("shell specials must record no refs, got %+v", cfg.SecretVarRefs)
	}
}

// T8. TestLoadNoRefsYieldsEmptySlice: a config with no '$' records no refs.
func TestLoadNoRefsYieldsEmptySlice(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: stdio
upstreams:
  - name: local
    command: /usr/bin/mcp
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.SecretVarRefs) != 0 {
		t.Errorf("SecretVarRefs = %+v, want empty", cfg.SecretVarRefs)
	}
}

// T9. TestLoadRejectsSecretVarRefsKey pins the mandatory `yaml:"-"` tag (§3):
// a YAML file spelling `secretvarrefs:` must be rejected as an unknown key by
// KnownFields(true) — proving the diagnostic field cannot be populated from
// the file. Without the tag yaml.v3 would map the key by lowercased name and
// accept it.
func TestLoadRejectsSecretVarRefsKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: stdio
secretvarrefs: []
upstreams:
  - name: local
    command: /usr/bin/mcp
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load accepted a `secretvarrefs:` YAML key; the yaml:\"-\" tag must make it unknown")
	}
	if !strings.Contains(err.Error(), "secretvarrefs") {
		t.Errorf("error should name the unknown key: %v", err)
	}
}

// T10. TestValidateFailsOnUnresolvedAuthTokenRef is table-driven and constructs
// Config with refs DIRECTLY (no files, no env), exercising Validate's hard-fail
// in isolation: an unresolved auth_token ref fails (naming the var); an
// unresolved env ref does not; a resolved auth_token ref does not; several
// unresolved auth_token refs all get named.
func TestValidateFailsOnUnresolvedAuthTokenRef(t *testing.T) {
	tests := []struct {
		name      string
		refs      []SecretVarRef
		wantErr   bool
		wantNamed []string
	}{
		{
			name:      "unresolved auth_token fails",
			refs:      []SecretVarRef{{Field: "auth_token", Var: "TOK", Resolved: false}},
			wantErr:   true,
			wantNamed: []string{"TOK"},
		},
		{
			name:    "unresolved env passes",
			refs:    []SecretVarRef{{Upstream: "u", Field: "env", Key: "K", Var: "ENVV", Resolved: false}},
			wantErr: false,
		},
		{
			name:    "resolved auth_token passes",
			refs:    []SecretVarRef{{Field: "auth_token", Var: "TOK", Resolved: true}},
			wantErr: false,
		},
		{
			name: "several unresolved auth_token all named",
			refs: []SecretVarRef{
				{Field: "auth_token", Var: "A", Resolved: false},
				{Field: "auth_token", Var: "B", Resolved: false},
			},
			wantErr:   true,
			wantNamed: []string{"A", "B"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Transport: TransportStdio, SecretVarRefs: tt.refs}
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate accepted refs %+v; want an error", tt.refs)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate rejected refs %+v: %v", tt.refs, err)
			}
			for _, name := range tt.wantNamed {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("error must name %q, got: %v", name, err)
				}
			}
		})
	}
}

// secretRefsEqual reports slice equality for SecretVarRef (order-sensitive).
func secretRefsEqual(a, b []SecretVarRef) bool {
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

// containsRef reports whether want appears anywhere in refs.
func containsRef(refs []SecretVarRef, want SecretVarRef) bool {
	for _, r := range refs {
		if r == want {
			return true
		}
	}
	return false
}

// C1. TestValidateEnvIsolation: "" and "strict" pass; anything else is a
// startup error naming the field and value.
func TestValidateEnvIsolation(t *testing.T) {
	mk := func(v string) *Config {
		return &Config{
			Transport:    TransportStdio,
			EnvIsolation: v,
			Upstreams:    []Upstream{{Name: "a", Command: "x"}},
		}
	}
	tests := []struct {
		name    string
		val     string
		wantErr bool
	}{
		{"unset defaults", "", false},
		{"strict", EnvIsolationStrict, false},
		{"unknown value", "loose", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mk(tt.val).Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate(env_isolation=%q) err = %v, wantErr %v", tt.val, err, tt.wantErr)
			}
			if tt.wantErr && !strings.Contains(err.Error(), "env_isolation") {
				t.Errorf("error %q must name the field", err)
			}
		})
	}
}

// C2. TestLoadParsesEnvIsolation: the YAML key round-trips into the strict
// helper; an absent key leaves the default (non-strict).
func TestLoadParsesEnvIsolation(t *testing.T) {
	load := func(t *testing.T, body string) *Config {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		return cfg
	}

	strict := load(t, `
transport: stdio
env_isolation: strict
upstreams:
  - name: a
    command: /usr/bin/mcp
    enabled: true
`)
	if !strict.EnvIsolationStrict() {
		t.Error("env_isolation: strict must report EnvIsolationStrict() == true")
	}

	def := load(t, `
transport: stdio
upstreams:
  - name: a
    command: /usr/bin/mcp
    enabled: true
`)
	if def.EnvIsolationStrict() {
		t.Error("absent env_isolation must report EnvIsolationStrict() == false")
	}
}

// C3. TestResolvedSecretValues pins §5.1: the returned values are the BARE
// variable values, not the composite field text ("Bearer <secret>"), and an
// unset variable contributes nothing.
func TestResolvedSecretValues(t *testing.T) {
	const secretA = "TEST-AIMCPGATE-FAKE-SECRET-ENV-A-0001"
	const secretB = "TEST-AIMCPGATE-FAKE-SECRET-HDR-B-0002"
	t.Setenv("TEST_AIMCPGATE_RESOLVED_A", secretA)
	t.Setenv("TEST_AIMCPGATE_RESOLVED_B", secretB)

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
transport: stdio
upstreams:
  - name: a
    command: /usr/bin/mcp
    env:
      T: ${TEST_AIMCPGATE_RESOLVED_A}
    enabled: true
  - name: b
    url: https://example.com/mcp
    headers:
      Authorization: "Bearer ${TEST_AIMCPGATE_RESOLVED_B}"
    enabled: true
  - name: c
    command: /usr/bin/mcp
    env:
      U: ${TEST_AIMCPGATE_UNSET_X}
    enabled: true
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	vals := cfg.ResolvedSecretValues()
	has := func(s string) bool {
		for _, v := range vals {
			if v == s {
				return true
			}
		}
		return false
	}
	if !has(secretA) {
		t.Errorf("ResolvedSecretValues missing env value; got %v", vals)
	}
	if !has(secretB) {
		t.Errorf("ResolvedSecretValues missing header value (bare, not composite); got %v", vals)
	}
	for _, v := range vals {
		if strings.HasPrefix(v, "Bearer ") {
			t.Errorf("ResolvedSecretValues returned composite field text %q, want bare value", v)
		}
		if v == "" {
			t.Error("ResolvedSecretValues returned an empty string")
		}
	}
}
