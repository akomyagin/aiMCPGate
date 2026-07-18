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

// newServeCmd wires config → logger → registry → transport and blocks serving
// the client until the process is cancelled (Ctrl-C / SIGTERM). This is the
// gateway's main run loop; keeping it here keeps main.go trivial (SKILL §1).
func newServeCmd(version string) *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the gateway, serving one client and multiplexing upstream MCP servers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(cmd.Context(), configPath, version)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "path to the YAML config file")
	return cmd
}

func runServe(parent context.Context, configPath, version string) error {
	// Cancel the whole tree on Ctrl-C / SIGTERM so upstream child processes get
	// torn down cleanly (see internal/registry).
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
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

	reg := registry.New(cfg, logger, callLog, payloadLog, true)
	srv := transport.NewServer(cfg, reg, logger, version)

	// Live config reload on SIGHUP (Stage 7d): reload runs in its own goroutine
	// so it never blocks request handling, and stops when ctx is cancelled. On
	// Windows reloadSignals() is empty (no SIGHUP), so this goroutine simply
	// waits out ctx — reload is a documented Unix-only convenience there.
	go watchReload(ctx, configPath, reg, logger)

	// Serve starts the registry (upstream fan-out) and blocks handling client
	// requests until ctx is cancelled or the client disconnects.
	return srv.Serve(ctx)
}

// watchReload listens for reload signals (SIGHUP) and, on each, reloads the
// config from configPath and applies it to the running registry. A failed load
// (e.g. a typo in the edited file) is logged and IGNORED — the currently running
// configuration stays live, so a bad edit never takes the gateway down. Returns
// when ctx is cancelled (process shutting down).
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
			newCfg, err := config.Load(configPath)
			if err != nil {
				// Keep the running config: a bad edit must not kill a working gateway.
				logger.Error("reload failed, keeping current config", "err", err)
				continue
			}
			switch err := reg.Reload(ctx, newCfg); {
			case err == nil:
			case errors.Is(err, registry.ErrNotStarted):
				// SIGHUP landed before Start finished its bring-up (watchReload
				// starts before srv.Serve). Not fatal — the edit is valid, the
				// registry just is not ready for it yet; retry shortly.
				logger.Warn("reload received before startup finished, retrying")
				retryReload(ctx, reg, newCfg, logger)
			case errors.Is(err, registry.ErrClosing):
				// The gateway is shutting down anyway; the reload is moot.
				logger.Debug("reload ignored: gateway is shutting down")
			default:
				logger.Error("reload apply failed", "err", err)
			}
		}
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
