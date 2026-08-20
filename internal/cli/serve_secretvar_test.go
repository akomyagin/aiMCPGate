package cli

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// captureCallLog is a fake CallLog that records every EventRecord it is handed,
// so a test can assert on exactly what reportUnresolvedSecretVars journals.
// Record (tool-call audit) is a no-op — this test drives no calls.
type captureCallLog struct {
	events []logging.EventRecord
}

func (c *captureCallLog) Record(logging.CallRecord)         {}
func (c *captureCallLog) RecordEvent(e logging.EventRecord) { c.events = append(c.events, e) }
func (c *captureCallLog) Close() error                      { return nil }

// S1. TestReportUnresolvedSecretVars: a config carrying one unresolved env ref
// on an ENABLED upstream yields exactly one journal event — the right Event,
// KindEvent, a non-zero Time, correct Upstream/Subject, and a Detail that names
// the variable but NEVER a value. Disabled, resolved, and auth_token refs
// produce no event.
func TestReportUnresolvedSecretVars(t *testing.T) {
	cfg := &config.Config{
		Upstreams: []config.Upstream{
			{Name: "gh"}, // enabled (nil Enabled)
			{Name: "off", Enabled: boolPtr(false)},
		},
		SecretVarRefs: []config.SecretVarRef{
			{Upstream: "gh", Field: "env", Key: "GITHUB_TOKEN", Var: "TEST_AIMCPGATE_UNSET_GH", Resolved: false},
			{Upstream: "off", Field: "env", Key: "TOKEN", Var: "TEST_AIMCPGATE_UNSET_OFF", Resolved: false},
			{Upstream: "gh", Field: "headers", Key: "X", Var: "TEST_AIMCPGATE_SET", Resolved: true},
			{Upstream: "", Field: "auth_token", Key: "", Var: "TEST_AIMCPGATE_AUTH", Resolved: false},
		},
	}
	cl := &captureCallLog{}
	logBuf := &syncWriter{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	reportUnresolvedSecretVars(cfg, cl, logger)

	// Symmetric with C1/D1: the operator-facing Warn line must name the
	// variable and never a value (review finding M1 — SecretVarRef carries no
	// value field, so a leak is structurally impossible, but this pins the
	// logged TEXT too, not just the type).
	if logged := logBuf.String(); !strings.Contains(logged, "TEST_AIMCPGATE_UNSET_GH") {
		t.Errorf("Warn log must name the unresolved variable, got:\n%s", logged)
	}

	if len(cl.events) != 1 {
		t.Fatalf("want exactly one event (only the enabled unresolved env ref), got %d: %+v", len(cl.events), cl.events)
	}
	e := cl.events[0]
	if e.Event != logging.EventUnresolvedSecretVar {
		t.Errorf("Event = %q, want %q", e.Event, logging.EventUnresolvedSecretVar)
	}
	if e.Kind != logging.KindEvent {
		t.Errorf("Kind = %q, want %q", e.Kind, logging.KindEvent)
	}
	if e.Time.IsZero() {
		t.Errorf("Time must be stamped, got zero")
	}
	if e.Upstream != "gh" {
		t.Errorf("Upstream = %q, want gh", e.Upstream)
	}
	if e.Subject != "env[GITHUB_TOKEN]" {
		t.Errorf("Subject = %q, want env[GITHUB_TOKEN]", e.Subject)
	}
	if !strings.Contains(e.Detail, "TEST_AIMCPGATE_UNSET_GH") {
		t.Errorf("Detail must name the variable: %q", e.Detail)
	}
}

// boolPtr mirrors config's boolPtr for the cli package's tests.
func boolPtr(b bool) *bool { return &b }

// S2. TestApplyReloadKeepsConfigOnUnresolvedAuthToken pins that the NEW hard
// error behaves through the existing reload path exactly as §2.2 claims WITHOUT
// touching applyReload: a live gateway whose config is edited to reference an
// unset auth_token variable keeps its current config ("reload failed, keeping
// current config"), reports reloadFinal, and its catalog survives. This test is
// expected GREEN from the first run — a red here means the invariant broke, not
// that a fix is missing.
func TestApplyReloadKeepsConfigOnUnresolvedAuthToken(t *testing.T) {
	bin := buildFakeServer(t)
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")

	good := `
transport: stdio
log_level: error
restart:
  enabled: false
upstreams:
  - name: up
    command: ` + bin + `
    enabled: true
    env:
      FAKE_TOOLS: "ping"
`
	if err := os.WriteFile(cfgPath, []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load seed config: %v", err)
	}
	logBuf := &syncWriter{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	payloadLog, err := logging.NewPayloadLog("")
	if err != nil {
		t.Fatalf("NewPayloadLog: %v", err)
	}
	reg := registry.New(cfg, logger, nil, payloadLog, false, "0.0.0-test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := reg.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = reg.Close() }()
	waitToolInRegistry(t, reg, "up__ping", 5*time.Second)

	// Edit the config to a load-invalid one: an unset auth_token variable.
	bad := `
transport: http
log_level: error
auth_token: ${TEST_AIMCPGATE_UNSET_TOK}
restart:
  enabled: false
upstreams:
  - name: up
    command: ` + bin + `
    enabled: true
    env:
      FAKE_TOOLS: "ping"
`
	if err := os.WriteFile(cfgPath, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}

	if out := applyReload(ctx, cfgPath, reg, logger, triggerPoll); out != reloadFinal {
		t.Errorf("applyReload on a load-invalid edit returned %v; want reloadFinal (the load error is terminal)", out)
	}
	if !strings.Contains(logBuf.String(), "reload failed, keeping current config") {
		t.Errorf("expected the 'keeping current config' log line; got:\n%s", logBuf.String())
	}
	// Catalog intact: the bad edit did not tear the gateway down.
	if !hasTool(reg, "up__ping") {
		t.Errorf("up__ping vanished after a rejected reload; the working config must survive")
	}
}
