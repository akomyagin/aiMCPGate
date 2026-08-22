package config

import (
	"strings"
	"testing"
)

// TestCleartextSecretWarnings (security brainstorm 2026-08-20, S5/S6): a
// secret travelling over plain HTTP to a non-loopback host is reported, one
// finding per hazard, never a secret value in the message.
func TestCleartextSecretWarnings(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantCount  int
		wantUpstr  string // "" means the gateway's own auth_token finding
		wantHostIn string // substring the message must contain, "" to skip
	}{
		{
			name: "S5: HTTP transport, non-empty token, non-loopback listen_addr",
			cfg: Config{
				Transport:  TransportHTTP,
				AuthToken:  "MYSECRETVALUE12345",
				ListenAddr: "0.0.0.0:28080",
			},
			wantCount:  1,
			wantHostIn: "0.0.0.0:28080",
		},
		{
			name: "S5: same, but loopback listen_addr — no warning",
			cfg: Config{
				Transport:  TransportHTTP,
				AuthToken:  "MYSECRETVALUE12345",
				ListenAddr: "127.0.0.1:28080",
			},
			wantCount: 0,
		},
		{
			name: "S5: stdio transport — auth_token check does not apply at all",
			cfg: Config{
				Transport:  TransportStdio,
				AuthToken:  "MYSECRETVALUE12345",
				ListenAddr: "0.0.0.0:28080",
			},
			wantCount: 0,
		},
		{
			name: "S5: empty token on non-loopback is Validate's job, not this one",
			cfg: Config{
				Transport:  TransportHTTP,
				AuthToken:  "",
				ListenAddr: "0.0.0.0:28080",
			},
			wantCount: 0,
		},
		{
			name: "S6: HTTP upstream, http:// scheme, non-loopback host, headers set",
			cfg: Config{
				Transport: TransportStdio,
				Upstreams: []Upstream{{
					Name:    "remote",
					URL:     "http://example.com:9000/mcp",
					Headers: map[string]string{"Authorization": "Bearer SUPERSECRETVALUE6789"},
				}},
			},
			wantCount:  1,
			wantUpstr:  "remote",
			wantHostIn: "example.com",
		},
		{
			name: "S6: https:// scheme — no warning",
			cfg: Config{
				Upstreams: []Upstream{{
					Name:    "remote",
					URL:     "https://example.com/mcp",
					Headers: map[string]string{"Authorization": "Bearer SUPERSECRETVALUE6789"},
				}},
			},
			wantCount: 0,
		},
		{
			name: "S6: http:// on loopback host — no warning",
			cfg: Config{
				Upstreams: []Upstream{{
					Name:    "local",
					URL:     "http://127.0.0.1:9000/mcp",
					Headers: map[string]string{"Authorization": "Bearer SUPERSECRETVALUE6789"},
				}},
			},
			wantCount: 0,
		},
		{
			name: "S6: http:// non-loopback but no headers — no warning",
			cfg: Config{
				Upstreams: []Upstream{{
					Name: "remote",
					URL:  "http://example.com/mcp",
				}},
			},
			wantCount: 0,
		},
		{
			name: "S6: stdio-kind upstream (no URL) — never matches, even with headers set",
			cfg: Config{
				Upstreams: []Upstream{{
					Name:    "local-proc",
					Command: "some-mcp-server",
					Headers: map[string]string{"X": "y"},
				}},
			},
			wantCount: 0,
		},
		{
			name: "both S5 and S6 fire together — two independent findings",
			cfg: Config{
				Transport:  TransportHTTP,
				AuthToken:  "MYSECRETVALUE12345",
				ListenAddr: ":28080",
				Upstreams: []Upstream{{
					Name:    "remote",
					URL:     "http://example.com/mcp",
					Headers: map[string]string{"Authorization": "Bearer SUPERSECRETVALUE6789"},
				}},
			},
			wantCount: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.CleartextSecretWarnings()
			if len(got) != tt.wantCount {
				t.Fatalf("CleartextSecretWarnings() = %d findings, want %d: %+v", len(got), tt.wantCount, got)
			}
			if tt.wantCount == 0 {
				return
			}
			w := got[0]
			if tt.wantUpstr != "" && w.Upstream != tt.wantUpstr {
				t.Errorf("Upstream = %q, want %q", w.Upstream, tt.wantUpstr)
			}
			if tt.wantUpstr == "" && len(tt.cfg.Upstreams) == 0 && w.Upstream != "" {
				t.Errorf("Upstream = %q, want empty (gateway's own auth_token)", w.Upstream)
			}
			if tt.wantHostIn != "" && !strings.Contains(w.Message, tt.wantHostIn) {
				t.Errorf("Message = %q, want it to contain %q", w.Message, tt.wantHostIn)
			}
			// Never leak the actual secret value into the message (the literal
			// values used above, not substrings like "auth_token" that happen
			// to contain "tok" — this checks the whole distinctive markers).
			if strings.Contains(w.Message, "MYSECRETVALUE12345") || strings.Contains(w.Message, "SUPERSECRETVALUE6789") {
				t.Errorf("Message must never carry a secret value: %q", w.Message)
			}
		})
	}
}
