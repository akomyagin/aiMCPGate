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

	reg := registry.New(cfg, logger, callLog, payloadLog, true, version)
	srv := transport.NewServer(cfg, reg, logger, version)

	// Live config reload on SIGHUP (Stage 7d): reload runs in its own goroutine
	// so it never blocks request handling, and stops when ctx is cancelled. On
	// Windows reloadSignals() is empty (no SIGHUP), so this goroutine simply
	// waits out ctx — reload is a documented Unix-only convenience there.
	go watchReload(ctx, configPath, reg, logger)

	// Opt-in polling fallback (--watch-config): reload when the config file's
	// mtime changes. This is how reload works on Windows (no SIGHUP), but it can
	// be enabled anywhere — running it ALONGSIDE the SIGHUP watcher is safe,
	// because Registry.Reload serializes every caller behind its lifecycle
	// mutex, so two triggers for the same edit just apply the same (idempotent)
	// diff twice.
	if watchConfig > 0 {
		watchPath, err := config.ResolvePath(configPath)
		if err != nil {
			return err
		}
		go pollConfig(ctx, watchPath, watchConfig, reg, logger)
	}

	// Serve blocks handling client requests until ctx is cancelled or the
	// client disconnects, and tears the registry down on the way out. It also
	// OWNS the registry's bring-up: HTTP starts the upstream fan-out up front,
	// while stdio starts it lazily, on the first client request that needs a
	// catalog — the gateway cannot declare what its own client supports to its
	// upstreams before that client's initialize has been parsed (Stage 15).
	return srv.Serve(ctx)
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
			applyReload(ctx, configPath, reg, logger)
		}
	}
}

// pollConfig watches the config file's mtime every interval and applies a
// reload when it changes — the same applyReload path SIGHUP takes, minus the
// signal. This is the opt-in --watch-config mechanism: the ONLY reload trigger
// on Windows, a redundant-but-harmless second trigger elsewhere. A stat
// failure (e.g. the fleeting window of an editor's atomic rename-over-save) is
// skipped silently and retried on the next tick. Returns when ctx is cancelled.
func pollConfig(ctx context.Context, path string, interval time.Duration, reg *registry.Registry, logger *slog.Logger) {
	var lastMod time.Time
	if fi, err := os.Stat(path); err == nil {
		lastMod = fi.ModTime()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		fi, err := os.Stat(path)
		if err != nil {
			continue
		}
		if fi.ModTime().Equal(lastMod) {
			continue
		}
		lastMod = fi.ModTime()
		logger.Info("config file changed, reloading", "path", path)
		applyReload(ctx, path, reg, logger)
	}
}

// applyReload is the shared body of BOTH reload triggers (SIGHUP and
// --watch-config polling): load the config from configPath and apply it to the
// running registry. A failed load (e.g. a typo in the edited file) is logged
// and IGNORED — the currently running configuration stays live, so a bad edit
// never takes the gateway down.
func applyReload(ctx context.Context, configPath string, reg *registry.Registry, logger *slog.Logger) {
	newCfg, err := config.Load(configPath)
	if err != nil {
		// Keep the running config: a bad edit must not kill a working gateway.
		logger.Error("reload failed, keeping current config", "err", err)
		return
	}
	switch err := reg.Reload(ctx, newCfg); {
	case err == nil:
	case errors.Is(err, registry.ErrNotStarted):
		// The reload landed before Start finished its bring-up (both watchers
		// start before srv.Serve). Not fatal — the edit is valid, the registry
		// just is not ready for it yet; retry shortly.
		logger.Warn("reload received before startup finished, retrying")
		retryReload(ctx, reg, newCfg, logger)
	case errors.Is(err, registry.ErrClosing):
		// The gateway is shutting down anyway; the reload is moot.
		logger.Debug("reload ignored: gateway is shutting down")
	default:
		logger.Error("reload apply failed", "err", err)
	}
}

// retryReload re-attempts a Reload that arrived before Start completed
// (registry.ErrNotStarted). Start normally finishes within moments, so a few
// short-interval retries suffice; if the limit is exhausted the reload is
// dropped with an error (the operator can send SIGHUP again). Returns silently
// when ctx is cancelled (process shutting down) or the registry starts closing.
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
