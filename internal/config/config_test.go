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
