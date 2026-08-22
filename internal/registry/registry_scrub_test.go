package registry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
)

// Stage: fix/secret-isolation-and-scrubbing — the value-scrubber redacts known
// secret values from operator-facing free text (audit error, event detail,
// crashed-upstream stderr tail). All "secret" values below are synthetic
// TEST-AIMCPGATE-FAKE-* markers set via t.Setenv; no real secret material.

// crashFake is an Upstream whose Done channel and StderrTail are test-driven, so
// the supervisor's crash-log path can be reached without a real process.
type crashFake struct {
	fakeUpstreamBase
	name string
	done chan struct{}
	tail []string
}

func (f *crashFake) Name() string                  { return f.name }
func (f *crashFake) Done() (<-chan struct{}, bool) { return f.done, true }
func (f *crashFake) StderrTail() ([]string, bool)  { return f.tail, len(f.tail) > 0 }
func (f *crashFake) Close() error                  { return nil }

// scrubCfg builds a stdio config whose upstream env references a t.Setenv'd
// variable, so New's scrubber picks up its value as a redaction candidate.
func scrubCfg(varName string) *config.Config {
	return &config.Config{
		Upstreams: []config.Upstream{
			{Name: "u", Command: "x", Enabled: boolPtr(true), Env: map[string]string{
				"T": "${" + varName + "}",
			}},
		},
		// SecretVarRefs is populated by Load; here we set it directly because the
		// test builds the Config literally rather than through Load.
		SecretVarRefs: []config.SecretVarRef{
			{Upstream: "u", Field: "env", Key: "T", Var: varName, Resolved: true},
		},
	}
}

// R1. TestAuditScrubsSecretValues: an error string carrying a known secret is
// redacted in the audit journal.
func TestAuditScrubsSecretValues(t *testing.T) {
	const secret = "TEST-AIMCPGATE-FAKE-SECRET-AUDIT-0001"
	t.Setenv("TEST_AIMCPGATE_AUDIT_VAR", secret)

	j, callLog := newJournal()
	r := New(scrubCfg("TEST_AIMCPGATE_AUDIT_VAR"), quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")
	defer func() { _ = r.Close() }()

	err := errors.New("upstream failed with token " + secret + " in message")
	r.audit(context.Background(), "u", "tools/call", "u__tool", time.Now(), nil, err)

	calls := 0
	for _, e := range j.entries(t) {
		if e.Call == nil {
			continue
		}
		calls++
		if strings.Contains(e.Call.Err, secret) {
			t.Errorf("audit leaked the secret: %q", e.Call.Err)
		}
		if !strings.Contains(e.Call.Err, "***") {
			t.Errorf("audit error not scrubbed: %q, want a *** placeholder", e.Call.Err)
		}
	}
	if calls != 1 {
		t.Fatalf("got %d call records, want 1; journal:\n%s", calls, j.bytes())
	}
	if bytes.Contains(j.bytes(), []byte(secret)) {
		t.Fatalf("secret leaked anywhere in the journal:\n%s", j.bytes())
	}
}

// R2. TestEmitEventScrubsDetail: a Detail carrying a known secret is redacted;
// a Detail with no secret is passed through byte for byte.
func TestEmitEventScrubsDetail(t *testing.T) {
	const secret = "TEST-AIMCPGATE-FAKE-SECRET-EVENT-0002"
	t.Setenv("TEST_AIMCPGATE_EVENT_VAR", secret)

	j, callLog := newJournal()
	r := New(scrubCfg("TEST_AIMCPGATE_EVENT_VAR"), quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")
	defer func() { _ = r.Close() }()

	r.emitEvent(logging.EventRecord{Event: "test_event", Upstream: "u", Detail: "boom: " + secret + " here"})
	const clean = "a perfectly clean detail with no secret in it"
	r.emitEvent(logging.EventRecord{Event: "clean_event", Upstream: "u", Detail: clean})

	var sawScrubbed, sawClean bool
	for _, e := range j.entries(t) {
		if e.Event == nil {
			continue
		}
		switch e.Event.Event {
		case "test_event":
			sawScrubbed = true
			if strings.Contains(e.Event.Detail, secret) {
				t.Errorf("event detail leaked the secret: %q", e.Event.Detail)
			}
			if !strings.Contains(e.Event.Detail, "***") {
				t.Errorf("event detail not scrubbed: %q", e.Event.Detail)
			}
		case "clean_event":
			sawClean = true
			if e.Event.Detail != clean {
				t.Errorf("clean detail was altered: %q, want %q", e.Event.Detail, clean)
			}
		}
	}
	if !sawScrubbed || !sawClean {
		t.Fatalf("missing events (scrubbed=%v clean=%v); journal:\n%s", sawScrubbed, sawClean, j.bytes())
	}
	if bytes.Contains(j.bytes(), []byte(secret)) {
		t.Fatalf("secret leaked anywhere in the journal:\n%s", j.bytes())
	}
}

// R3. TestSupervisorCrashLogScrubsStderrTail: the supervisor's crash-log Warn
// redacts a known secret in the crashed upstream's stderr tail.
func TestSupervisorCrashLogScrubsStderrTail(t *testing.T) {
	const secret = "TEST-AIMCPGATE-FAKE-SECRET-STDERR-0003"
	t.Setenv("TEST_AIMCPGATE_STDERR_VAR", secret)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	cfg := scrubCfg("TEST_AIMCPGATE_STDERR_VAR")
	// Restart disabled so supervise gives up right after the crash log and returns.
	cfg.Restart = config.RestartPolicy{Enabled: boolPtr(false)}
	r := New(cfg, logger, nil, noopPayloadLog(), true, "0.0.0-test")
	defer func() { _ = r.Close() }()

	done := make(chan struct{})
	fake := &crashFake{name: "u", done: done, tail: []string{"panic: leaked " + secret + " oops"}}
	r.mu.Lock()
	r.conns["u"] = fake
	r.mu.Unlock()

	// Run the supervisor and drop the upstream (crash) so it logs the tail.
	supDone := make(chan struct{})
	go func() {
		r.supervise(cfg.Upstreams[0], fake, done, r.procCtx)
		close(supDone)
	}()
	close(done)
	select {
	case <-supDone:
	case <-time.After(5 * time.Second):
		t.Fatal("supervise did not return after a give-up crash")
	}

	out := logBuf.String()
	if !strings.Contains(out, "stdio upstream stderr before exit") {
		t.Fatalf("crash-log line missing; log:\n%s", out)
	}
	if strings.Contains(out, secret) {
		t.Errorf("crash-log leaked the secret; log:\n%s", out)
	}
	if !strings.Contains(out, "***") {
		t.Errorf("crash-log stderr tail not scrubbed; log:\n%s", out)
	}
}

// R4. TestReloadRebuildsScrubber: after a reload swaps the config to a different
// secret, the scrubber redacts the NEW value.
func TestReloadRebuildsScrubber(t *testing.T) {
	const secretX = "TEST-AIMCPGATE-FAKE-SECRET-RELOAD-X-0004"
	const secretY = "TEST-AIMCPGATE-FAKE-SECRET-RELOAD-Y-0005"
	t.Setenv("TEST_AIMCPGATE_RELOAD_X", secretX)
	t.Setenv("TEST_AIMCPGATE_RELOAD_Y", secretY)

	j, callLog := newJournal()
	r := New(scrubCfg("TEST_AIMCPGATE_RELOAD_X"), quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")
	defer func() { _ = r.Close() }()
	// Mark the registry running so Reload's lifecycle guard passes without
	// spawning the (non-existent) "x" upstream process.
	r.lifecycleMu.Lock()
	r.phase = phaseRunning
	r.lifecycleMu.Unlock()

	// Before reload: X is scrubbed, Y is not yet known.
	if got := r.scrub("has " + secretX); strings.Contains(got, secretX) {
		t.Errorf("pre-reload scrub failed to redact X: %q", got)
	}

	if err := r.Reload(context.Background(), scrubCfg("TEST_AIMCPGATE_RELOAD_Y")); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if got := r.scrub("has " + secretY); strings.Contains(got, secretY) || !strings.Contains(got, "***") {
		t.Errorf("post-reload scrub failed to redact Y: %q", got)
	}
	// Pin the wiring through the event path too.
	r.emitEvent(logging.EventRecord{Event: "post_reload", Upstream: "u", Detail: "leak " + secretY})
	if bytes.Contains(j.bytes(), []byte(secretY)) {
		t.Fatalf("Y leaked into the journal after reload:\n%s", j.bytes())
	}
}
