package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/registry"
	"github.com/akomyagin/aiMCPGate/internal/transport"
)

// defaultWatchConfigInterval is what a bare `--watch-config` (no value) means:
// poll the config file's mtime every 2s — frequent enough that an edit lands
// within a breath, rare enough that the stat cost is unmeasurable.
const defaultWatchConfigInterval = 2 * time.Second

// newServeCmd wires config → logger → registry → transport and blocks serving
// the client until the process is cancelled (Ctrl-C / SIGTERM). This is the
// gateway's main run loop; keeping it here keeps main.go trivial (SKILL §1).
func newServeCmd(version string) *cobra.Command {
	var (
		configPath  *string
		envFile     *string
		watchConfig time.Duration
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the gateway, serving one client and multiplexing upstream MCP servers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(cmd.Context(), *configPath, *envFile, watchConfig, version)
		},
	}
	configPath = addConfigFlag(cmd)
	envFile = addEnvFileFlag(cmd)
	cmd.Flags().DurationVar(&watchConfig, "watch-config", 0,
		"poll the config file for changes at this interval and hot-reload on change — "+
			"the cross-platform alternative to SIGHUP (Windows has none); bare --watch-config means "+
			defaultWatchConfigInterval.String()+", 0 disables (use --watch-config=INTERVAL, not a space)")
	cmd.Flags().Lookup("watch-config").NoOptDefVal = defaultWatchConfigInterval.String()
	return cmd
}

func runServe(parent context.Context, configPath, envFile string, watchConfig time.Duration, version string) error {
	if watchConfig < 0 {
		return fmt.Errorf("--watch-config must be a positive interval (got %s)", watchConfig)
	}

	// Cancel the whole tree on Ctrl-C / SIGTERM so upstream child processes get
	// torn down cleanly (see internal/registry).
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The env file must land in the environment BEFORE config.Load expands the
	// ${VAR} references in the config (and before any SIGHUP reload re-loads it).
	if err := applyEnvFile(envFile); err != nil {
		return err
	}
	// Resolve the config path once, for BOTH reload triggers: the polling watcher
	// needs it to stat the file, and the SIGHUP path needs it so its log lines can
	// name the file even when --config was not given (resolving to config.yaml
	// next to the binary). Failing to resolve is fatal only when the watcher was
	// actually requested — otherwise keep the raw value and let config.Load
	// produce its own, more specific error, exactly as before.
	reloadPath := configPath
	if p, rerr := config.ResolvePath(configPath); rerr != nil {
		if watchConfig > 0 {
			return rerr
		}
	} else {
		reloadPath = p
	}

	// Fingerprint the watched config BEFORE loading it: the watcher's baseline
	// must never be younger than the configuration actually in force, otherwise
	// an edit made between the load and the watcher's first tick is swallowed
	// silently — that was ДЕФЕКТ 6. The error is deliberately pointed the other
	// way: a stamp taken slightly EARLY costs a redundant reload of the config
	// already loaded (Registry.Reload is idempotent — the diff comes out empty),
	// while a stamp taken late loses the edit outright. On stdio that redundant
	// reload can land before the registry has started (it comes up on the
	// client's first request); the watcher then holds the edit and re-attempts it
	// at its own cadence rather than dropping it (see configChangeDetector).
	var watchBase configStamp
	if watchConfig > 0 {
		watchBase = statConfig(reloadPath)
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}

	logger := logging.New(cfg.LogLevel, os.Stderr)
	logger.Info("mcp-gate starting", "version", version, "transport", cfg.Transport)

	callLog, err := logging.NewCallLog(cfg.LogFile)
	if err != nil {
		return fmt.Errorf("open call log: %w", err)
	}
	defer func() { _ = callLog.Close() }()

	// Opt-in payload debug log (Stage 10): off unless debug_payload_log is set.
	// When enabled it writes raw request/response bodies — possibly secrets — so
	// warn loudly at startup; it must never be left on in production.
	payloadLog, err := logging.NewPayloadLog(cfg.DebugPayloadLog)
	if err != nil {
		return fmt.Errorf("open payload log: %w", err)
	}
	defer func() { _ = payloadLog.Close() }()
	if cfg.DebugPayloadLog != "" {
		logger.Warn("payload logging ENABLED: request/response bodies (incl. possible secrets) are written to disk; disable in production", "path", cfg.DebugPayloadLog)
	}

	reportUnresolvedSecretVars(cfg, callLog, logger)
	reportCleartextSecretWarnings(cfg, logger)

	reg := registry.New(cfg, logger, callLog, payloadLog, true, version)
	srv := transport.NewServer(cfg, reg, logger, version)

	// Live config reload on SIGHUP (Stage 7d): reload runs in its own goroutine
	// so it never blocks request handling, and stops when ctx is cancelled. On
	// Windows reloadSignals() is empty (no SIGHUP), so this goroutine simply
	// waits out ctx — reload is a documented Unix-only convenience there.
	go watchReload(ctx, reloadPath, reg, logger)

	// Opt-in polling fallback (--watch-config): reload when the config file's
	// mtime changes. This is how reload works on Windows (no SIGHUP), but it can
	// be enabled anywhere — running it ALONGSIDE the SIGHUP watcher is safe,
	// because Registry.Reload serializes every caller behind its lifecycle
	// mutex, so two triggers for the same edit just apply the same (idempotent)
	// diff twice.
	if watchConfig > 0 {
		go pollConfig(ctx, reloadPath, watchConfig, watchBase, reg, logger)
	}

	// Serve blocks handling client requests until ctx is cancelled or the
	// client disconnects, and tears the registry down on the way out. It also
	// OWNS the registry's bring-up: HTTP starts the upstream fan-out up front,
	// while stdio starts it lazily, on the first client request that needs a
	// catalog — the gateway cannot declare what its own client supports to its
	// upstreams before that client's initialize has been parsed (Stage 15).
	return srv.Serve(ctx)
}

// reportUnresolvedSecretVars journals one event per unresolved ${VAR}
// reference in the env/headers of ENABLED upstreams, plus a logger.Warn for
// each. It lives here, in runServe, and not in config.Load, because Load runs
// long before the journal exists — Load therefore returns what it saw on the
// Config (SecretVarRefs) and the emitting happens at the first point both the
// facts and the journal are in hand. Disabled upstreams are skipped: their
// env is never used, so the event would warn about a hazard that cannot fire
// (doctor still lists them). Time and Kind are stamped explicitly — the
// stamping in registry.emitEvent is private to the registry. auth_token needs
// no handling here: an unresolved auth_token never gets this far (Validate
// fails the load). Variable NAMES only, never values.
func reportUnresolvedSecretVars(cfg *config.Config, callLog logging.CallLog, logger *slog.Logger) {
	enabled := make(map[string]bool, len(cfg.Upstreams))
	for _, u := range cfg.Upstreams {
		enabled[u.Name] = u.IsEnabled()
	}
	for _, ref := range cfg.SecretVarRefs {
		if ref.Resolved || ref.Field == "auth_token" || !enabled[ref.Upstream] {
			continue
		}
		subject := ref.Field + "[" + ref.Key + "]"
		detail := "references unset environment variable " + ref.Var + "; the value expanded to an empty string"
		logger.Warn("unresolved secret variable", "upstream", ref.Upstream, "subject", subject, "var", ref.Var)
		callLog.RecordEvent(logging.EventRecord{
			Time:     time.Now(),
			Kind:     logging.KindEvent,
			Event:    logging.EventUnresolvedSecretVar,
			Upstream: ref.Upstream,
			Subject:  subject,
			Detail:   detail,
		})
	}
}

// reportCleartextSecretWarnings logs one Warn per config.CleartextSecretWarnings
// finding (security brainstorm 2026-08-20, S5/S6) — a secret travelling over
// plain HTTP to a non-loopback host. Filtered to ENABLED upstreams (the
// gateway's own auth_token finding always passes: Upstream == "" is never
// filtered out) — a disabled upstream makes no live call, so its finding
// describes a hazard that cannot fire yet; doctor lists it anyway (dormant
// hazards included, see printCleartextSecretWarnings). No journal event: this
// is operator-facing at startup, not a per-call audit fact.
func reportCleartextSecretWarnings(cfg *config.Config, logger *slog.Logger) {
	enabled := make(map[string]bool, len(cfg.Upstreams))
	for _, u := range cfg.Upstreams {
		enabled[u.Name] = u.IsEnabled()
	}
	for _, w := range cfg.CleartextSecretWarnings() {
		if w.Upstream != "" && !enabled[w.Upstream] {
			continue
		}
		logger.Warn(w.Message, "upstream", w.Upstream)
	}
}

// watchReload listens for reload signals (SIGHUP) and applies a reload on
// each. Returns when ctx is cancelled (process shutting down).
func watchReload(ctx context.Context, configPath string, reg *registry.Registry, logger *slog.Logger) {
	sigs := reloadSignals()
	if len(sigs) == 0 {
		return // platform without a reload signal (Windows): nothing to watch.
	}
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, sigs...)
	defer signal.Stop(hup)

	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			logger.Info("reload signal received, reloading config")
			// The outcome is deliberately ignored: a signal has no cadence to
			// retry on, so applyReload settles the attempt in place (see its
			// triggerSignal branch) and always answers "final" here.
			_ = applyReload(ctx, configPath, reg, logger, triggerSignal)
		}
	}
}

// configStamp fingerprints the config file for change detection: mtime PLUS
// size. The size is what catches an edit landing inside the filesystem's mtime
// granularity — but only when the edit changes the LENGTH. A same-length edit
// (log_level: error → debug) within one mtime tick is still invisible, and is
// then lost until some later, unrelated edit; that is a residual limitation, not
// a guarantee, and it only bites on coarse-timestamp filesystems (FAT/exFAT at
// 2s, some network mounts) rather than on ext4/NTFS.
// present=false means the stat failed — a missing file (the fleeting window of
// an editor's atomic rename-over-save) is "mid-write", never "changed".
type configStamp struct {
	modTime time.Time
	size    int64
	present bool
}

// statConfig fingerprints path. A failed stat yields the zero (absent) stamp
// instead of an error: callers treat absence as "not readable right now".
func statConfig(path string) configStamp {
	fi, err := os.Stat(path)
	if err != nil {
		return configStamp{}
	}
	return configStamp{modTime: fi.ModTime(), size: fi.Size(), present: true}
}

// sameAs reports whether two stamps describe the same file state. Two ABSENT
// stamps are equal — a file that stays missing is not a change.
func (s configStamp) sameAs(other configStamp) bool {
	if s.present != other.present {
		return false
	}
	if !s.present {
		return true
	}
	return s.size == other.size && s.modTime.Equal(other.modTime)
}

// reloadAttempt is what the detector tells the poll loop to do with one stamp.
type reloadAttempt int

const (
	reloadSkip   reloadAttempt = iota // nothing to do this tick
	reloadFirst                       // first attempt at this file state
	reloadRepeat                      // the previous attempt did not stick; try again
)

// configChangeDetector decides WHEN a polled config file may be reloaded.
// Three properties, all deliberate and all bug-driven (2026-08-19..20):
//
//  1. The baseline comes from the CALLER, taken BEFORE config.Load in runServe —
//     never from inside the poll goroutine. Snapping it in the goroutine lost
//     every edit that landed before the goroutine's first tick: its own first
//     stat already saw the edited file and recorded it as "unchanged". Proved:
//     the watcher test failed 5/5 under GOMAXPROCS=1, with a 60s budget, and
//     never logged "config file changed".
//  2. A changed stamp must REPEAT identically on the next observation before it
//     is accepted. os.WriteFile and many editors truncate the file and then fill
//     it; a tick landing inside that window read a TRUNCATED file — which can be
//     perfectly valid YAML, merely missing upstreams — and applied it, tearing
//     down live upstreams. Cost: a reload lands within up to 2 intervals.
//  3. The baseline advances on the OUTCOME of the reload, not on the detection:
//     the fired state is held in `firing` until settle() is told the attempt
//     reached a final verdict. Advancing it at detection time lost the edit for
//     good whenever the reload could not be applied yet — on stdio, the default
//     transport, the registry starts lazily on the client's first request, so
//     Reload answers ErrNotStarted for as long as no client has spoken. The edit
//     was retried for one second and then dropped, with the log advising a
//     second reload signal — which does not exist on Windows, the platform this
//     watcher exists for. Holding the state instead re-attempts it every tick,
//     at the poll cadence, until the registry is ready.
type configChangeDetector struct {
	applied    configStamp
	pending    configStamp
	hasPending bool
	firing     configStamp
	hasFiring  bool
}

func newConfigChangeDetector(base configStamp) *configChangeDetector {
	return &configChangeDetector{applied: base}
}

// observe feeds one polled stamp and reports whether a reload should fire now,
// and whether that would be the first attempt at this file state or a repeat of
// one that has not stuck yet. A firing state is dropped the moment the file
// moves on (or vanishes): the newer state supersedes it, and since the baseline
// was never advanced the newer state is picked up by the ordinary repeat rule.
func (d *configChangeDetector) observe(s configStamp) reloadAttempt {
	switch {
	case !s.present:
		// Unreadable right now: not a change, and NOT a new baseline either —
		// the file is presumed mid-write and re-examined on the next tick.
		d.reset()
		return reloadSkip
	case d.hasFiring && s.sameAs(d.firing):
		// Same state we already tried and could not finish: try it again.
		return reloadRepeat
	case s.sameAs(d.applied):
		d.reset()
		return reloadSkip
	case d.hasPending && s.sameAs(d.pending):
		// The same new state twice in a row: the writer has settled.
		d.pending, d.hasPending = configStamp{}, false
		d.firing, d.hasFiring = s, true
		return reloadFirst
	default:
		d.pending, d.hasPending = s, true
		d.firing, d.hasFiring = configStamp{}, false
		return reloadSkip
	}
}

// settle records the OUTCOME of the attempt observe last authorised. A final
// outcome advances the baseline; a non-final one leaves it exactly where it was,
// so the next tick re-attempts the very same edit. It MUST be called after every
// attempt observe authorises — an attempt left unsettled repeats forever.
func (d *configChangeDetector) settle(outcome reloadOutcome) {
	if !d.hasFiring || outcome != reloadFinal {
		return
	}
	d.applied = d.firing
	d.firing, d.hasFiring = configStamp{}, false
}

// reset forgets both the candidate and the in-flight state, without touching the
// baseline. Losing the in-flight state costs at most one extra poll cycle: the
// baseline still predates the edit, so the file is re-detected from scratch.
func (d *configChangeDetector) reset() {
	d.pending, d.hasPending = configStamp{}, false
	d.firing, d.hasFiring = configStamp{}, false
}

// pollConfig fingerprints the config file (mtime and size) every interval and
// applies a reload once a changed fingerprint has repeated on a second tick —
// the same applyReload path SIGHUP takes, minus the signal. This is the opt-in
// --watch-config mechanism: the ONLY reload trigger
// on Windows, a redundant-but-harmless second trigger elsewhere. A stat
// failure (e.g. the fleeting window of an editor's atomic rename-over-save) is
// skipped silently and retried on the next tick. Returns when ctx is cancelled.
//
// base is the fingerprint of the config file as it was when the running
// configuration was loaded; it MUST be taken by the caller, in the caller's
// goroutine, BEFORE config.Load (see configChangeDetector).
func pollConfig(ctx context.Context, path string, interval time.Duration, base configStamp,
	reg *registry.Registry, logger *slog.Logger) {
	det := newConfigChangeDetector(base)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		attempt := det.observe(statConfig(path))
		if attempt == reloadSkip {
			continue
		}
		// Only the FIRST attempt at a given file state is an operator-facing
		// event. Repeats are the same edit waiting for the registry to come up;
		// at Info they would fill the log with an identical line every couple of
		// seconds for as long as an idle stdio gateway sits without a client.
		if attempt == reloadFirst {
			logger.Info("config file changed, reloading", "path", path)
		} else {
			logger.Debug("config file change still not applied, retrying reload", "path", path)
		}
		outcome := applyReload(ctx, path, reg, logger, triggerPoll)
		if outcome == reloadDeferred && attempt == reloadFirst {
			// Said once per edit, and only here: the operator has just been told
			// "reloading", and without this would see nothing happen. The point
			// of the line is that no action is required of them — in particular
			// not re-saving the file, which on Windows is the only lever there is.
			logger.Info("reload deferred until startup finishes: on stdio the upstreams come up on the client's first request; the edit is kept and retried every poll, no need to touch the file again", "path", path)
		}
		det.settle(outcome)
	}
}

// reloadTrigger names which trigger drives a reload. The two differ in exactly
// one way — what to do when the registry has not started yet (see applyReload).
type reloadTrigger int

const (
	triggerSignal reloadTrigger = iota // SIGHUP: no cadence of its own
	triggerPoll                        // --watch-config: has a cadence of its own
)

// reloadOutcome reports whether an attempt reached a FINAL verdict for the file
// state it was given. Only the polling watcher can act on a non-final one, so
// reloadFinal is the zero value: every other caller may ignore the result.
type reloadOutcome int

const (
	reloadFinal    reloadOutcome = iota // verdict reached: applied, or refused for good
	reloadDeferred                      // not applied YET, and retrying is worthwhile
)

// applyReload is the shared body of BOTH reload triggers (SIGHUP and
// --watch-config polling): load the config from configPath and apply it to the
// running registry. A failed load (e.g. a typo in the edited file) is logged
// and IGNORED — the currently running configuration stays live, so a bad edit
// never takes the gateway down.
//
// The returned outcome is what tells a caller with a cadence of its own (the
// polling watcher) whether this file state still deserves another attempt. The
// classification is the whole point, so it is spelled out per branch below:
// everything except "the registry has not started yet" is FINAL. In particular a
// parse error and a refusal by vetReloadConfig are final ON PURPOSE — a broken
// or truncated-to-empty file does not fix itself, and re-attempting it every
// poll would log the same failure forever. The operator's next save is a new
// file state and gets its own fresh attempt.
func applyReload(ctx context.Context, configPath string, reg *registry.Registry, logger *slog.Logger, trigger reloadTrigger) reloadOutcome {
	newCfg, err := config.Load(configPath)
	if err != nil {
		// Keep the running config: a bad edit must not kill a working gateway.
		logger.Error("reload failed, keeping current config", "err", err)
		return reloadFinal
	}
	// Deliberately a DIFFERENT log line from "reload failed" above: "could not
	// parse the file" and "parsed it, refused to apply it" are distinct events.
	if verr := vetReloadConfig(newCfg); verr != nil {
		logger.Error("reload rejected, keeping current config", "err", verr, "path", configPath)
		return reloadFinal
	}
	switch err := reg.Reload(ctx, newCfg); {
	case err == nil:
		return reloadFinal
	case errors.Is(err, registry.ErrNotStarted):
		// The reload landed before Start finished its bring-up (both watchers
		// start before srv.Serve). Not fatal — the edit is valid, the registry
		// just is not ready for it yet. How long "not yet" lasts depends on the
		// transport: HTTP starts the fan-out up front, but stdio starts it on the
		// client's first request, so this window stays open for as long as no
		// client has spoken. Hence the split:
		if trigger == triggerPoll {
			// The watcher re-stats the file every interval anyway, so hand the
			// decision back: it keeps its baseline un-advanced and re-attempts
			// this same edit on the next tick, for however long that takes.
			// Burning a fixed retry budget here would be strictly worse — it
			// blocks the poll goroutine AND gives up while the file's edit is
			// still the newest thing the operator asked for.
			return reloadDeferred
		}
		// SIGHUP has no cadence of its own: if this call gives up, nothing else
		// will ever retry, so the short in-place retry loop stays here.
		logger.Warn("reload received before startup finished, retrying")
		retryReload(ctx, reg, newCfg, logger)
		return reloadFinal
	case errors.Is(err, registry.ErrClosing):
		// The gateway is shutting down anyway; the reload is moot and there will
		// be no later tick worth spending — final.
		logger.Debug("reload ignored: gateway is shutting down")
		return reloadFinal
	default:
		// Deliberately final: an apply failure of this kind (a supervisor that
		// will not come up, a transport that will not bind) does not clear itself
		// between two polls, and retrying it at the poll cadence would turn one
		// logged failure into a permanent stream of them. The config on disk is
		// still the operator's; the next save re-attempts it.
		logger.Error("reload apply failed", "err", err)
		return reloadFinal
	}
}

// vetReloadConfig applies the checks that are stricter for a RELOAD than for a
// cold start: a config config.Load happily accepts may still be unsafe to apply
// to a LIVE gateway. Returns nil when the config may be applied.
//
// The checks live HERE and not in config.Load because Load has eight callers
// (serve, doctor, call, catalog, logs, client-config, token, skill) and only
// this one applies its result to running upstreams. `doctor` in particular must
// stay able to load and EXPLAIN a doubtful config — it is the tool the operator
// reaches for to diagnose exactly this. Sitting inside applyReload also means
// both reload triggers, SIGHUP and --watch-config, are covered by one check.
func vetReloadConfig(newCfg *config.Config) error {
	// A reload config with no upstreams AT ALL is refused. Rationale: an empty
	// (or truncated-to-empty) file is what a non-atomic save looks like from the
	// outside — os.WriteFile and many editors truncate first and fill after — and
	// config.Load accepts it as a perfectly valid gateway with zero upstreams.
	// Applying it kills every running upstream, and if the save then finishes as
	// broken YAML the "keep the current config" fallback keeps the EMPTY one.
	// Cold start is deliberately NOT affected: an empty gateway is a legitimate
	// starting point, and `doctor` must stay able to explain such a config.
	//
	// The check is STRUCTURAL (len(Upstreams) == 0), not "no ENABLED upstreams":
	// disabling the last upstream with `enabled: false` is a complete, deliberate
	// edit and must keep working through reload. Truncation produces an empty
	// list, never an explicit `enabled: false`.
	if len(newCfg.Upstreams) == 0 {
		return errors.New("the new config declares no upstreams at all — refusing to tear down every running upstream on what is almost certainly a half-written file; restart the gateway if removing all upstreams was intended")
	}
	return nil
}

// retryReload re-attempts a Reload that arrived before Start completed
// (registry.ErrNotStarted). It serves the SIGHUP path ONLY: a signal is a
// one-shot event with nothing behind it to try again, so the retrying has to
// happen here or not at all, and "send SIGHUP again" is advice the operator can
// actually act on. The polling watcher deliberately does NOT come here — it has
// a cadence of its own and re-attempts the held edit indefinitely instead (see
// applyReload's triggerPoll branch). Returns silently when ctx is cancelled
// (process shutting down) or the registry starts closing.
func retryReload(ctx context.Context, reg *registry.Registry, newCfg *config.Config, logger *slog.Logger) {
	const (
		maxAttempts = 10
		interval    = 100 * time.Millisecond
	)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		switch err := reg.Reload(ctx, newCfg); {
		case err == nil:
			return
		case errors.Is(err, registry.ErrNotStarted):
			continue // Start still in flight; wait another interval.
		case errors.Is(err, registry.ErrClosing):
			logger.Debug("reload abandoned: gateway is shutting down")
			return
		default:
			logger.Error("reload apply failed", "err", err)
			return
		}
	}
	logger.Error("reload dropped: startup did not finish within the retry window; send the reload signal again")
}
