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
