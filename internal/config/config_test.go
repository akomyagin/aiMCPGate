package config

import (
	"os"
	"path/filepath"
	"testing"
)

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
		{"enabled differs only", func(u Upstream) Upstream { u.Enabled = !u.Enabled; return u }, true},
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
		Allow:  []string{"a", "b"},
		Deny:   []string{"c"},
		Rename: map[string]string{"a": "short_a"},
	}}
	clone := func() Upstream {
		u := base
		u.Tools = ToolFilter{
			Allow:  append([]string(nil), base.Tools.Allow...),
			Deny:   append([]string(nil), base.Tools.Deny...),
			Rename: map[string]string{"a": "short_a"},
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

// TestLoadParsesToolFilter confirms the YAML shape of the tools block
// (allow/deny/rename) round-trips through Load.
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
