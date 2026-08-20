package cli

import (
	"context"
	"fmt"
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

// waitToolInRegistry polls the registry's aggregated catalog until the
// namespaced tool appears or the deadline passes — reload is asynchronous
// relative to the polling watcher, so the test cannot assert synchronously.
func waitToolInRegistry(t *testing.T, reg *registry.Registry, name string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, d := range reg.Tools() {
			if d.Name == name {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tool %q never appeared in the catalog within %s: %+v", name, within, reg.Tools())
}

// TestConfigChangeDetectorRequiresSettledStamp pins the detector's contract:
// a new fingerprint fires a reload only once it has REPEATED, so a tick that
// lands inside a non-atomic save (truncate → fill) never reads a half file.
func TestConfigChangeDetectorRequiresSettledStamp(t *testing.T) {
	stamp := func(sec, size int64) configStamp {
		return configStamp{modTime: time.Unix(sec, 0), size: size, present: true}
	}
	var absent configStamp
	a := stamp(1, 900)
	b := stamp(2, 920)
	c := stamp(3, 930)

	cases := []struct {
		name  string
		base  configStamp
		obs   []configStamp
		fires []bool
		// outcomes[i] is what the reload authorised at observation i returned.
		// Absent entries mean reloadFinal — the zero value and the ordinary case.
		outcomes []reloadOutcome
		// repeats[i] is true when observation i must be reported as a RETRY of a
		// state already attempted (logged at Debug), not as a fresh detection.
		repeats []bool
	}{
		{
			name:  "unchanged file never fires",
			base:  a,
			obs:   []configStamp{a, a},
			fires: []bool{false, false},
		},
		{
			name:  "repeated new stamp fires once and becomes the baseline",
			base:  a,
			obs:   []configStamp{b, b, b},
			fires: []bool{false, true, false},
		},
		{
			name:  "still-moving file is not loaded until it settles",
			base:  a,
			obs:   []configStamp{b, c, c},
			fires: []bool{false, false, true},
		},
		{
			name:  "a vanished file is neither an event nor a new baseline",
			base:  a,
			obs:   []configStamp{absent, a},
			fires: []bool{false, false},
		},
		{
			name:  "a vanished file drops the pending candidate",
			base:  a,
			obs:   []configStamp{b, absent, b, b},
			fires: []bool{false, false, false, true},
		},
		{
			// The truncation window itself: os.WriteFile empties the file and
			// then fills it. The zero-size observation must never be applied.
			name:  "truncate-then-fill fires only on the filled file",
			base:  stamp(1, 900),
			obs:   []configStamp{stamp(2, 0), stamp(2, 940), stamp(2, 940)},
			fires: []bool{false, false, true},
		},
		{
			// The F1 property: an attempt that could not be applied yet leaves
			// the baseline alone, so the very next tick tries the same edit
			// again — and once it sticks, it stops.
			name:     "a deferred reload is retried until it sticks, then stops",
			base:     a,
			obs:      []configStamp{b, b, b, b},
			fires:    []bool{false, true, true, false},
			outcomes: []reloadOutcome{reloadFinal, reloadDeferred, reloadFinal, reloadFinal},
			repeats:  []bool{false, false, true, false},
		},
		{
			// A final outcome — including a config REFUSED for good, which is
			// what a parse error or the empty-upstreams guard produces — must
			// never be re-attempted: that would log the same failure forever.
			name:     "a final outcome is never retried",
			base:     a,
			obs:      []configStamp{b, b, b, b},
			fires:    []bool{false, true, false, false},
			outcomes: []reloadOutcome{reloadFinal, reloadFinal, reloadFinal, reloadFinal},
		},
		{
			// While an edit is held for retry the file moves on again. The newer
			// state wins, and reaches the registry under the ordinary settle
			// rule — the older, never-applied one is simply forgotten.
			name:     "a newer edit supersedes one still waiting to be applied",
			base:     a,
			obs:      []configStamp{b, b, c, c, c},
			fires:    []bool{false, true, false, true, false},
			outcomes: []reloadOutcome{reloadFinal, reloadDeferred, reloadFinal, reloadFinal, reloadFinal},
			repeats:  []bool{false, false, false, false, false},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			det := newConfigChangeDetector(tc.base)
			at := func(s []bool, i int) bool { return i < len(s) && s[i] }
			outcomeAt := func(i int) reloadOutcome {
				if i < len(tc.outcomes) {
					return tc.outcomes[i]
				}
				return reloadFinal
			}
			for i, s := range tc.obs {
				got := det.observe(s)
				if fired := got != reloadSkip; fired != tc.fires[i] {
					t.Errorf("observe #%d %+v fired = %v; want %v", i, s, fired, tc.fires[i])
				}
				if isRepeat := got == reloadRepeat; isRepeat != at(tc.repeats, i) {
					t.Errorf("observe #%d %+v repeat = %v; want %v", i, s, isRepeat, at(tc.repeats, i))
				}
				if got != reloadSkip {
					det.settle(outcomeAt(i))
				}
			}
		})
	}
}

// TestStatConfigIncludesSize pins that the fingerprint covers the file SIZE and
// not just the mtime: an edit landing inside the filesystem's timestamp
// granularity (or one whose mtime is restored by a tool) must still be visible.
func TestStatConfigIncludesSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	fixed := time.Unix(1_700_000_000, 0)
	write := func(content string) configStamp {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, fixed, fixed); err != nil {
			t.Fatal(err)
		}
		return statConfig(path)
	}

	first := write("transport: stdio\n")
	if !first.present {
		t.Fatalf("statConfig on an existing file returned an absent stamp: %+v", first)
	}
	sameLen := write("transport: STDIO\n") // same byte count, same pinned mtime
	if !first.sameAs(sameLen) {
		t.Errorf("stamps differ for same size and same mtime: %+v vs %+v", first, sameLen)
	}
	otherLen := write("transport: stdio\nlog_level: error\n")
	if first.sameAs(otherLen) {
		t.Errorf("stamps compare equal despite different sizes: %+v vs %+v", first, otherLen)
	}

	if s := statConfig(filepath.Join(t.TempDir(), "missing.yaml")); s.present {
		t.Errorf("statConfig on a missing file returned present stamp: %+v", s)
	}
}

// TestPollConfigAppliesReloadOnMtimeChange is the --watch-config test: a
// running registry, a pollConfig watcher on a real temp config file, and an
// edit that changes an upstream's launch env. The mtime bump must be detected
// and the reload applied through the same applyReload path SIGHUP takes —
// observed via the CATALOG: the relaunched upstream advertises a new tool.
// A follow-up BAD edit must be rejected by config.Load with the running
// config kept alive (the new tool stays).
func TestPollConfigAppliesReloadOnMtimeChange(t *testing.T) {
	bin := buildFakeServer(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	writeCfg := func(tools string) {
		t.Helper()
		yaml := fmt.Sprintf(`
transport: stdio
log_level: error
restart:
  enabled: false
upstreams:
  - name: up
    command: %s
    enabled: true
    env:
      FAKE_TOOLS: %q
`, bin, tools)
		if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// bumpMtime pushes the file's mtime forward explicitly, so the test never
	// depends on the filesystem's timestamp granularity to see a change.
	mtimeStep := time.Now()
	bumpMtime := func() {
		t.Helper()
		mtimeStep = mtimeStep.Add(2 * time.Second)
		if err := os.Chtimes(cfgPath, mtimeStep, mtimeStep); err != nil {
			t.Fatal(err)
		}
	}

	writeCfg("ping")
	// The watcher's baseline is taken by the CALLER, before the config is
	// loaded — exactly as runServe does it (see configChangeDetector).
	base := statConfig(cfgPath)
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

	go pollConfig(ctx, cfgPath, 10*time.Millisecond, base, reg, logger)

	// Edit 1 (valid): the upstream's env changes → Reload relaunches it and the
	// new tool must land in the catalog. That is Reload observably invoked.
	writeCfg("ping,pong")
	bumpMtime()
	waitToolInRegistry(t, reg, "up__pong", 5*time.Second)
	if !strings.Contains(logBuf.String(), "config file changed, reloading") {
		t.Errorf("watcher never logged the change detection; log:\n%s", logBuf.String())
	}

	// Edit 2 (broken YAML): config.Load fails, the reload is dropped, and the
	// RUNNING config stays live — a bad edit must never take the gateway down.
	if err := os.WriteFile(cfgPath, []byte("upstreams: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	bumpMtime()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logBuf.String(), "reload failed, keeping current config") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(logBuf.String(), "reload failed, keeping current config") {
		t.Fatalf("bad edit was never rejected; log:\n%s", logBuf.String())
	}
	waitToolInRegistry(t, reg, "up__pong", 2*time.Second) // catalog untouched
}

// TestVetReloadConfigRejectsOnlyStructurallyEmpty pins the guard's shape: it
// refuses a config with NO upstreams entry at all (the signature of a truncated
// file) and nothing else — an upstream deliberately switched off with
// `enabled: false` is a complete edit and must still be applied.
func TestVetReloadConfigRejectsOnlyStructurallyEmpty(t *testing.T) {
	enabled := false
	cases := []struct {
		name    string
		cfg     *config.Config
		wantErr bool
	}{
		{
			name: "one upstream",
			cfg:  &config.Config{Upstreams: []config.Upstream{{Name: "up", Command: "/bin/true"}}},
		},
		{
			name: "one upstream, explicitly disabled",
			cfg:  &config.Config{Upstreams: []config.Upstream{{Name: "up", Command: "/bin/true", Enabled: &enabled}}},
		},
		{
			name:    "no upstreams at all",
			cfg:     &config.Config{},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := vetReloadConfig(tc.cfg)
			switch {
			case tc.wantErr && err == nil:
				t.Fatal("vetReloadConfig accepted a config with no upstreams")
			case !tc.wantErr && err != nil:
				t.Fatalf("vetReloadConfig rejected a usable config: %v", err)
			case tc.wantErr && !strings.Contains(err.Error(), "no upstreams"):
				t.Errorf("error text does not name the problem: %v", err)
			}
		})
	}
}

// hasTool reports whether the namespaced tool is in the registry's catalog
// right now. Unlike waitToolInRegistry this does NOT poll: applyReload is
// synchronous, so its effect is observable the instant it returns.
func hasTool(reg *registry.Registry, name string) bool {
	for _, d := range reg.Tools() {
		if d.Name == name {
			return true
		}
	}
	return false
}

// reloadRig is the fixture the reload tests share: the fakeserver binary, a
// real temp config file with one upstream, and a started registry whose
// catalog already carries up__ping. It mirrors runServe's ordering — the
// watcher baseline is stamped BEFORE the config is loaded.
type reloadRig struct {
	t       *testing.T
	bin     string
	cfgPath string
	base    configStamp
	reg     *registry.Registry
	logBuf  *syncWriter
	logger  *slog.Logger
	ctx     context.Context
	mtime   time.Time
}

// upstreamYAML renders a one-upstream gateway config exposing the named
// fakeserver tools.
func (r *reloadRig) upstreamYAML(tools string, enabled bool) string {
	return fmt.Sprintf(`
transport: stdio
log_level: error
restart:
  enabled: false
upstreams:
  - name: up
    command: %s
    enabled: %t
    env:
      FAKE_TOOLS: %q
`, r.bin, enabled, tools)
}

func (r *reloadRig) write(yaml string) {
	r.t.Helper()
	if err := os.WriteFile(r.cfgPath, []byte(yaml), 0o600); err != nil {
		r.t.Fatal(err)
	}
}

// bumpMtime pushes the file's mtime forward explicitly, so the test never
// depends on the filesystem's timestamp granularity to see a change.
func (r *reloadRig) bumpMtime() {
	r.t.Helper()
	r.mtime = r.mtime.Add(2 * time.Second)
	if err := os.Chtimes(r.cfgPath, r.mtime, r.mtime); err != nil {
		r.t.Fatal(err)
	}
}

func newReloadRig(t *testing.T) *reloadRig {
	t.Helper()
	r := &reloadRig{
		t:      t,
		bin:    buildFakeServer(t),
		logBuf: &syncWriter{},
		mtime:  time.Now(),
	}
	r.cfgPath = filepath.Join(t.TempDir(), "config.yaml")
	r.logger = slog.New(slog.NewTextHandler(r.logBuf, nil))
	r.write(r.upstreamYAML("ping", true))

	r.base = statConfig(r.cfgPath)
	cfg, err := config.Load(r.cfgPath)
	if err != nil {
		t.Fatalf("load seed config: %v", err)
	}
	payloadLog, err := logging.NewPayloadLog("")
	if err != nil {
		t.Fatalf("NewPayloadLog: %v", err)
	}
	r.reg = registry.New(cfg, r.logger, nil, payloadLog, false, "0.0.0-test")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.ctx = ctx
	if err := r.reg.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = r.reg.Close() })
	waitToolInRegistry(t, r.reg, "up__ping", 5*time.Second)
	return r
}

// TestPollConfigDetectsEditMadeBeforeWatcherStarts is the deterministic version
// of the flake this branch closes: the edit is made BEFORE the watcher
// goroutine exists, so the scheduler plays no part at all. With the baseline
// snapped inside pollConfig, the watcher's own first stat saw the already
// edited file and recorded it as "unchanged" — the edit was lost forever.
func TestPollConfigDetectsEditMadeBeforeWatcherStarts(t *testing.T) {
	rig := newReloadRig(t)

	// Edit FIRST, watcher SECOND — the exact ordering that used to be silently
	// swallowed (and the prod window between config.Load and the first tick).
	rig.write(rig.upstreamYAML("ping,pong", true))
	rig.bumpMtime()

	go pollConfig(rig.ctx, rig.cfgPath, 10*time.Millisecond, rig.base, rig.reg, rig.logger)

	waitToolInRegistry(t, rig.reg, "up__pong", 5*time.Second)
	if !strings.Contains(rig.logBuf.String(), "config file changed, reloading") {
		t.Errorf("watcher never logged the change detection; log:\n%s", rig.logBuf.String())
	}
}

// waitLogContains waits until the log buffer holds substr, and reports whether
// it appeared. Deliberately NOT a t.Fatal helper: callers use it to sequence a
// test around an event that a BROKEN implementation never produces, and must
// stay free to go on and fail on the behaviour itself rather than on a log line.
func waitLogContains(buf *syncWriter, substr string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), substr) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestPollConfigRetriesReloadUntilTheRegistryStarts pins the fix for the review
// finding F1. On stdio — the DEFAULT transport — the registry starts LAZILY, on
// the client's first non-ping request, and until then Registry.Reload returns
// ErrNotStarted. An edit made inside that window (which lasts as long as no
// client has spoken yet) used to be attempted once, retried for a second and
// then dropped — with the detector's baseline ALREADY advanced, so no later tick
// ever looked at that edit again. It was lost forever, and the log told the
// operator to "send the reload signal again": impossible on Windows, the very
// platform --watch-config exists for.
func TestPollConfigRetriesReloadUntilTheRegistryStarts(t *testing.T) {
	bin := buildFakeServer(t)
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	yamlFor := func(tools string) string {
		return fmt.Sprintf(`
transport: stdio
log_level: error
restart:
  enabled: false
upstreams:
  - name: up
    command: %s
    enabled: true
    env:
      FAKE_TOOLS: %q
`, bin, tools)
	}
	write := func(tools string) {
		t.Helper()
		if err := os.WriteFile(cfgPath, []byte(yamlFor(tools)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	mtimeStep := time.Now()
	bumpMtime := func() {
		t.Helper()
		mtimeStep = mtimeStep.Add(2 * time.Second)
		if err := os.Chtimes(cfgPath, mtimeStep, mtimeStep); err != nil {
			t.Fatal(err)
		}
	}

	write("ping")
	base := statConfig(cfgPath)
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
	// Deliberately NOT started: this is precisely the stdio window between
	// runServe launching the watcher and the client's first request.
	reg := registry.New(cfg, logger, nil, payloadLog, false, "0.0.0-test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	write("ping,pong")
	bumpMtime()
	go pollConfig(ctx, cfgPath, 10*time.Millisecond, base, reg, logger)

	// Let the watcher reach a verdict on this edit BEFORE the registry starts:
	// what must apply the edit is the retry, not a lucky race with Start.
	waitLogContains(logBuf, "reload deferred", 5*time.Second)

	if err := reg.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = reg.Close() }()

	// The edit predates the registry; it must still land once startup finishes.
	waitToolInRegistry(t, reg, "up__pong", 5*time.Second)
	if strings.Contains(logBuf.String(), "reload dropped") {
		t.Errorf("the edit was dropped instead of retried; log:\n%s", logBuf.String())
	}
}

// TestApplyReloadRejectsConfigWithoutUpstreams: a config that declares no
// upstreams at all is what a half-written file looks like from the outside
// (os.WriteFile truncates first, fills after) — config.Load accepts it as a
// valid zero-upstream gateway. Applying it tore down every live upstream.
func TestApplyReloadRejectsConfigWithoutUpstreams(t *testing.T) {
	rig := newReloadRig(t)

	// Valid YAML, valid config, zero upstreams — exactly what config.Load
	// returns for a truncated file.
	rig.write("transport: stdio\nlog_level: error\n")
	if out := applyReload(rig.ctx, rig.cfgPath, rig.reg, rig.logger, triggerPoll); out != reloadFinal {
		t.Errorf("a refused config reported outcome %v; want reloadFinal — retrying it would log the same refusal forever", out)
	}

	log := rig.logBuf.String()
	if !strings.Contains(log, "reload rejected, keeping current config") {
		t.Errorf("empty-upstreams reload was not rejected; log:\n%s", log)
	}
	if strings.Contains(log, "upstream removed by reload") {
		t.Errorf("upstreams were torn down by an empty-upstreams reload; log:\n%s", log)
	}
	if !hasTool(rig.reg, "up__ping") {
		t.Errorf("up__ping gone from the catalog after a rejected reload: %+v", rig.reg.Tools())
	}
}

// TestApplyReloadAppliesConfigWithAllUpstreamsDisabled is the counterweight to
// the guard above: disabling the last upstream with `enabled: false` is a
// complete, deliberate edit and MUST still apply. Red if the guard is written
// against IsEnabled instead of the structural len(Upstreams) == 0.
func TestApplyReloadAppliesConfigWithAllUpstreamsDisabled(t *testing.T) {
	rig := newReloadRig(t)

	rig.write(rig.upstreamYAML("ping", false))
	if out := applyReload(rig.ctx, rig.cfgPath, rig.reg, rig.logger, triggerPoll); out != reloadFinal {
		t.Errorf("an applied reload reported outcome %v; want reloadFinal", out)
	}

	log := rig.logBuf.String()
	if strings.Contains(log, "reload rejected") {
		t.Errorf("a deliberate `enabled: false` edit was refused; log:\n%s", log)
	}
	if !strings.Contains(log, "upstream removed by reload") {
		t.Errorf("the disabled upstream was never removed; log:\n%s", log)
	}
	if hasTool(rig.reg, "up__ping") {
		t.Errorf("up__ping still in the catalog after being disabled: %+v", rig.reg.Tools())
	}
}

// TestPollConfigLogsARefusedEditOnce pins the other half of the F1 fix: only an
// edit that could not be applied YET is held for retry. An edit refused for GOOD
// — unparseable YAML, or a config the guard turns down — advances the baseline
// like any applied one, so the watcher states its verdict once and falls silent.
// Without that, a half-written or empty file left on disk would repeat the same
// failure into the operator's log every couple of seconds, forever.
func TestPollConfigLogsARefusedEditOnce(t *testing.T) {
	cases := []struct {
		name    string
		content string
		line    string
	}{
		{
			name:    "unparseable YAML",
			content: "upstreams: [",
			line:    "reload failed, keeping current config",
		},
		{
			name:    "valid config the guard refuses",
			content: "transport: stdio\nlog_level: error\n",
			line:    "reload rejected, keeping current config",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newReloadRig(t)
			go pollConfig(rig.ctx, rig.cfgPath, 10*time.Millisecond, rig.base, rig.reg, rig.logger)

			rig.write(tc.content)
			rig.bumpMtime()
			if !waitLogContains(rig.logBuf, tc.line, 5*time.Second) {
				t.Fatalf("the bad edit was never reported; log:\n%s", rig.logBuf.String())
			}
			// ~30 further ticks: a detector that re-attempts a final verdict
			// would have logged the same line many times over by now.
			time.Sleep(300 * time.Millisecond)
			if n := strings.Count(rig.logBuf.String(), tc.line); n != 1 {
				t.Errorf("%q logged %d times; want exactly 1 — a refused edit must not be retried forever; log:\n%s",
					tc.line, n, rig.logBuf.String())
			}
		})
	}
}

// TestPollConfigStopsOnContextCancel: the watcher goroutine must exit when the
// serve context is cancelled, not leak past shutdown.
func TestPollConfigStopsOnContextCancel(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("transport: stdio"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollConfig(ctx, cfgPath, time.Millisecond, configStamp{}, nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pollConfig did not return after context cancellation")
	}
}

// TestServeRejectsNegativeWatchConfig: a negative interval is a user error,
// reported up front instead of silently disabling the watcher.
func TestServeRejectsNegativeWatchConfig(t *testing.T) {
	err := runServe(context.Background(), "", "", -time.Second, "test")
	if err == nil || !strings.Contains(err.Error(), "--watch-config") {
		t.Fatalf("runServe with a negative --watch-config returned %v; want a --watch-config error", err)
	}
}
