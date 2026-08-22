package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// TestDoctorPrintsCleartextSecretWarnings (security brainstorm 2026-08-20,
// S5/S6): unlike serve's Warn log, doctor lists a DISABLED upstream's finding
// too — dormant hazards included — marked distinctly from an enabled one's.
// Called directly (not through the full `doctor` command): S6 needs a real
// non-loopback host, and doctor would otherwise attempt a genuine network
// connection to it during reg.Start(), which has no place in a fast unit test.
func TestDoctorPrintsCleartextSecretWarnings(t *testing.T) {
	cfg := &config.Config{
		Transport:  config.TransportHTTP,
		AuthToken:  "TEST-AIMCPGATE-FAKE-SECRET-GATEWAY",
		ListenAddr: "0.0.0.0:28080",
		Upstreams: []config.Upstream{
			{
				Name:    "reachable",
				URL:     "http://example.com/mcp",
				Headers: map[string]string{"Authorization": "Bearer TEST-AIMCPGATE-FAKE-SECRET-ON"},
			},
			{
				Name:    "dormant",
				URL:     "http://example.org/mcp",
				Headers: map[string]string{"Authorization": "Bearer TEST-AIMCPGATE-FAKE-SECRET-OFF"},
				Enabled: boolPtr(false),
			},
		},
	}
	cmd := &cobra.Command{}
	out := &bytes.Buffer{}
	cmd.SetOut(out)

	printCleartextSecretWarnings(cmd, cfg)

	s := out.String()
	if !strings.Contains(s, "auth_token is sent over plain HTTP") {
		t.Errorf("stdout must print the gateway's own auth_token finding:\n%s", s)
	}
	if !strings.Contains(s, "reachable") || !strings.Contains(s, "dormant") {
		t.Errorf("stdout must name BOTH upstreams (dormant hazards included):\n%s", s)
	}
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, "\"dormant\"") && !strings.Contains(line, "(upstream disabled)") {
			t.Errorf("disabled upstream's WARN line must carry the marker:\n%s", line)
		}
		if strings.Contains(line, "\"reachable\"") && strings.Contains(line, "(upstream disabled)") {
			t.Errorf("enabled upstream's WARN line must NOT carry the disabled marker:\n%s", line)
		}
	}
	if strings.Contains(s, "TEST-AIMCPGATE-FAKE-SECRET-GATEWAY") ||
		strings.Contains(s, "TEST-AIMCPGATE-FAKE-SECRET-ON") ||
		strings.Contains(s, "TEST-AIMCPGATE-FAKE-SECRET-OFF") {
		t.Errorf("stdout must never carry a secret value itself:\n%s", s)
	}
}
