package registry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
)

// TestStartReportListsSuccessesAndFailures is the Stage 8 contract test for
// StartReport: after Start, the report must carry one entry per enabled
// upstream — successes with their tool count, failures with the same reason
// text recordFailure captured — sorted by name (bringUp runs in parallel, so
// sorting is the only way the order can be asserted at all).
func TestStartReportListsSuccessesAndFailures(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "good", Enabled: true},
		{Name: "broken", Enabled: true},
		{Name: "disabled", Enabled: false},
	}}
	fakes := map[string]*fakeUpstream{
		"good":     {name: "good", tools: []string{"a", "b"}},
		"broken":   {name: "broken", initErr: errors.New("boom")},
		"disabled": {name: "disabled", tools: []string{"c"}},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	report := r.StartReport()
	if len(report) != 2 {
		t.Fatalf("report has %d entries, want 2 (disabled upstream must not appear): %+v", len(report), report)
	}
	// Sorted by name: "broken" < "good".
	fail, ok := report[0], report[1]
	if fail.Name != "broken" || fail.OK || fail.Tools != 0 {
		t.Errorf("failure entry = %+v, want Name=broken OK=false Tools=0", fail)
	}
	if !strings.Contains(fail.Err, "handshake failed: boom") {
		t.Errorf("failure reason = %q, want it to carry the recordFailure text %q", fail.Err, "handshake failed: boom")
	}
	if ok.Name != "good" || !ok.OK || ok.Tools != 2 || ok.Err != "" {
		t.Errorf("success entry = %+v, want Name=good OK=true Tools=2 Err=\"\"", ok)
	}
}

// TestStartReportAllFailed covers doctor's worst case: Start itself errors
// (zero live upstreams) but StartReport must still hand back every failure so
// the doctor table is printed instead of a bare error.
func TestStartReportAllFailed(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "b1", Enabled: true},
		{Name: "b2", Enabled: true},
	}}
	fakes := map[string]*fakeUpstream{
		"b1": {name: "b1", initErr: errors.New("boom1")},
		"b2": {name: "b2", initErr: errors.New("boom2")},
	}
	r := newTestRegistry(t, cfg, nil, fakes)
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("Start should error when every upstream fails")
	}
	defer r.Close()

	report := r.StartReport()
	if len(report) != 2 {
		t.Fatalf("report has %d entries, want 2: %+v", len(report), report)
	}
	for _, s := range report {
		if s.OK {
			t.Errorf("entry %+v reported OK, want FAIL", s)
		}
	}
}

// TestNoSuperviseSkipsRestart is the doctor-mode guard (Stage 8): with
// supervise=false, a crashed stdio upstream must NOT be auto-restarted even
// though the restart policy says enabled — the diagnostic pass reports state,
// it does not fight it. Mirrors TestSupervisorDisabled, but the "off switch"
// under test is the New parameter, not the policy.
func TestNoSuperviseSkipsRestart(t *testing.T) {
	bin := buildFakeServer(t)
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true), // policy says restart — supervise=false must win
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
			MaxAttempts:    5,
		},
		Upstreams: []config.Upstream{
			{Name: "once", Command: bin, Enabled: true, Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	r := New(cfg, quietLogger(), nil, false)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	// The first pass came up fine and is reported as such.
	report := r.StartReport()
	if len(report) != 1 || !report[0].OK || report[0].Tools != 1 {
		t.Fatalf("report = %+v, want a single OK entry with 1 tool", report)
	}

	// Crash the child (FAKE_EXIT_AFTER=1). With supervise=false no supervisor
	// exists to revive it, so calls must keep failing — if auto-restart had
	// (wrongly) run, callSucceedsWithin would observe a success.
	if _, err := r.CallTool(context.Background(), "once__ping", []byte(`{}`)); err != nil {
		t.Fatalf("first CallTool: %v", err)
	}
	if callSucceedsWithin(r, "once__ping", 2*time.Second) {
		t.Fatal("crashed upstream was revived even though supervise=false (doctor mode)")
	}
}
