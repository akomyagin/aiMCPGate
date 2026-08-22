package cli

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// TestReportCleartextSecretWarnings (security brainstorm 2026-08-20, S5/S6):
// the gateway's own auth_token finding always logs (Upstream == "" is never
// filtered, regardless of what upstreams exist or their enabled state),
// combined here with one ENABLED upstream's S6 finding, which logs too, and
// never the secret value itself.
func TestReportCleartextSecretWarnings(t *testing.T) {
	cfg := &config.Config{
		Transport:  config.TransportHTTP,
		AuthToken:  "TEST-AIMCPGATE-FAKE-SECRET-GATEWAY",
		ListenAddr: "0.0.0.0:28080",
		Upstreams: []config.Upstream{
			{
				Name:    "on",
				URL:     "http://example.com/mcp",
				Headers: map[string]string{"Authorization": "Bearer TEST-AIMCPGATE-FAKE-SECRET-UPSTREAM"},
			}, // enabled (nil Enabled)
		},
	}
	logBuf := &syncWriter{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	reportCleartextSecretWarnings(cfg, logger)

	logged := logBuf.String()
	if !strings.Contains(logged, "auth_token is sent over plain HTTP") {
		t.Errorf("gateway auth_token finding must always log, got:\n%s", logged)
	}
	if !strings.Contains(logged, "sends secret headers over plain HTTP") {
		t.Errorf("enabled upstream's finding must log, got:\n%s", logged)
	}
	if strings.Contains(logged, "TEST-AIMCPGATE-FAKE-SECRET-GATEWAY") ||
		strings.Contains(logged, "TEST-AIMCPGATE-FAKE-SECRET-UPSTREAM") {
		t.Errorf("log must never carry a secret value itself, got:\n%s", logged)
	}
}

// TestReportCleartextSecretWarningsUpstreamFiltering isolates the per-upstream
// S6 finding (no auth_token noise) to pin the enabled/disabled split precisely:
// only the enabled upstream's warning reaches the log.
func TestReportCleartextSecretWarningsUpstreamFiltering(t *testing.T) {
	cfg := &config.Config{
		Upstreams: []config.Upstream{
			{
				Name:    "reachable",
				URL:     "http://example.com/mcp",
				Headers: map[string]string{"Authorization": "Bearer x"},
			},
			{
				Name:    "dormant",
				URL:     "http://example.org/mcp",
				Headers: map[string]string{"Authorization": "Bearer y"},
				Enabled: boolPtr(false),
			},
		},
	}
	logBuf := &syncWriter{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	reportCleartextSecretWarnings(cfg, logger)

	logged := logBuf.String()
	if !strings.Contains(logged, "reachable") {
		t.Errorf("enabled upstream's finding must log, got:\n%s", logged)
	}
	if strings.Contains(logged, "dormant") {
		t.Errorf("disabled upstream's finding must NOT log, got:\n%s", logged)
	}
}
