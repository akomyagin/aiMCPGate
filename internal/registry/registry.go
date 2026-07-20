// Package registry manages the set of upstream MCP servers and the aggregated
// catalog of their tools and resources.
//
// This is the heart of the gateway. On Start it launches every enabled upstream,
// performs the MCP initialize handshake, lists each upstream's tools/resources,
// and merges them into one namespaced catalog that the client-facing transport
// exposes. When the client invokes a tool, the registry resolves which upstream
// owns it (via the routing table) and forwards the JSON-RPC call.
//
// Concurrency: the fan-out over upstreams runs in parallel (errgroup); the
// aggregated catalog and routing table are guarded by a mutex. Upstream errors
// are isolated — a failed upstream is logged and skipped, it does not bring the
// gateway down (TECHNICAL_PLAN §4.4) — UNLESS every upstream fails, in which
// case Start itself errors: an aggregator with nothing to aggregate has no
// reason to keep running (found by user request).
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/upstream"
)

// NameSeparator joins an upstream name and a tool name into the client-facing
// namespaced name: "<upstream>__<tool>". See docs/MCP_NOTES.md §6.
const NameSeparator = "__"

// ErrNotStarted is returned by Reload when it is called before Start has
// completed. Applying a reload mid-Start would race the parallel bring-up:
// an upstream Start is still handshaking looks "not live" to the reload diff,
// which would launch a SECOND process with the same name and orphan one of the
// two (found by independent review after Stage 7). The caller (watchReload)
// treats this as retryable — Start will finish shortly.
var ErrNotStarted = errors.New("registry: reload before start completed")

// ErrClosing is returned by Reload when the registry is shutting down (Close
// has begun). There is nothing meaningful to reload — the caller should just
// let the process exit.
var ErrClosing = errors.New("registry: reload during shutdown")

// phase is the registry's lifecycle position, guarded by lifecycleMu. It gates
// Reload so it can only run between a completed Start and the beginning of
// Close (see ErrNotStarted / ErrClosing).
type phase int

const (
	phaseNew     phase = iota // Start has not completed successfully yet
	phaseRunning              // Start completed, the registry is serving
	phaseClosing              // Close has begun
)

// ToolDescriptor is one aggregated tool entry in the merged catalog.
//
// Name is the client-facing name after namespacing (e.g. "github__search").
// Upstream records which upstream owns it so calls can be routed back. The tool
// schema (Description/InputSchema/...) is carried verbatim from the upstream.
type ToolDescriptor struct {
	Name     string
	Upstream string
	Tool     mcp.Tool
}

// route maps a namespaced tool name back to its upstream and original name.
type route struct {
	upstream string
	original string
}

// UpstreamStatus is one upstream's outcome from the very first bring-up pass
// (Start). It is the machine-readable counterpart of the per-upstream slog
// lines Start already emits, consumed by `mcp-gate doctor` (Stage 8) — parsing
// the slog output back would be fragile, so the same facts are recorded here.
type UpstreamStatus struct {
	Name  string
	OK    bool
	Tools int    // tools merged into the catalog; 0 when the upstream failed
	Err   string // failure reason (same text recordFailure captures); empty on success
}

// upstreamStarter abstracts launching one upstream, so tests can inject fakes
// without spawning real processes. The production implementation wraps
// upstream.StartStdio.
type upstreamStarter func(ctx context.Context, u config.Upstream) (Upstream, error)

// Upstream is the minimal surface the registry needs from a live upstream
// connection. *upstream.StdioConn satisfies it; tests provide fakes.
type Upstream interface {
	Name() string
	Initialize(ctx context.Context) (*mcp.InitializeResult, error)
	ListTools(ctx context.Context) ([]mcp.Tool, error)
	ListResources(ctx context.Context) ([]mcp.Resource, error)
	CallTool(ctx context.Context, name string, arguments json.RawMessage) (*mcp.Message, error)
	Close() error
}

// Registry owns upstream connections and the aggregated catalog.
type Registry struct {
	// cfg is stored behind an atomic pointer because Reload (Stage 7d) swaps the
	// whole config while CallTool / the supervisor / re-list are concurrently
	// reading it (call timeout, restart policy). An atomic pointer gives those
	// readers a consistent snapshot without a lock on the hot path. Access it
	// only through r.config().
	cfg     atomic.Pointer[config.Config]
	log     *slog.Logger
	callLog logging.CallLog
	// payloadLog is the OPT-IN payload debug log (Stage 10). It is never nil —
	// New always sets it (a no-op implementation when disabled) so CallTool can
	// call Record unconditionally. Unlike callLog it carries raw arguments and
	// results, which may contain secrets; it stays strictly separate from the
	// metadata-only audit log (SKILL §6).
	payloadLog logging.PayloadLog
	start      upstreamStarter

	// autoRestart gates the per-upstream auto-restart supervisor goroutines
	// (Stage 7a); it is New's supervise parameter. serve runs with true;
	// `mcp-gate doctor` (Stage 8) runs with false so its single diagnostic pass
	// reports a flapping upstream instead of endlessly resurrecting it. This is
	// the ONLY behavioural difference — Start/bringUp/catalog work identically
	// either way. (Named autoRestart, not supervise, because the supervisor loop
	// method already carries that name.)
	autoRestart bool

	// procCtx is the context each upstream CHILD PROCESS is launched under —
	// deliberately independent of Start's errgroup context. errgroup.WithContext
	// cancels its derived context the instant g.Wait() returns, which is right
	// when Start() finishes; if child processes were bound to that context (as
	// they originally were, see the bug this comment documents), every upstream
	// would be killed immediately after "registry ready" logs, and every later
	// CallTool would hit a dead connection. procCtx instead lives for the
	// Registry's whole lifetime and is only cancelled by Close.
	procCtx    context.Context
	procCancel context.CancelFunc

	// lifecycleMu serializes Start, Reload and Close against each other; phase
	// records where in the lifecycle the registry is (guarded by lifecycleMu).
	// This is the fix for the WaitGroup Add/Wait races found by independent
	// review after Stage 7: a Reload racing a still-running Start could launch a
	// duplicate upstream process, and a Reload's supervisors.Add(1) racing
	// Close's supervisors.Wait() is the documented-forbidden WaitGroup reuse
	// pattern. With all three under one mutex, none of those windows exist.
	//
	// Lock ordering: lifecycleMu is the OUTERMOST lock — it is taken first and
	// only by Start/Reload/Close; everything they call underneath (bringUp,
	// launch, merge, superviseUpstream, dropUpstream, ...) takes only the inner
	// locks (mu, failMu, supMu, ...) and never lifecycleMu, so no cycle exists.
	lifecycleMu sync.Mutex
	phase       phase

	mu        sync.RWMutex
	conns     map[string]Upstream
	tools     map[string]ToolDescriptor // client-facing name → descriptor
	toolRoute map[string]route          // client-facing name → (upstream, original)
	// rawTools holds the last UNFILTERED tools/list result per upstream, kept
	// alongside the filtered catalog (Stage 9): a reload that only widens an
	// allow-list must be able to resurrect a previously filtered-out tool
	// whose mcp.Tool is long gone from r.tools — without a fresh network
	// re-list. Guarded by the same r.mu as conns/tools/toolRoute because the
	// four are always mutated together (mergeLocked/dropLocked).
	rawTools map[string][]mcp.Tool

	failMu   sync.Mutex
	failures []string         // "<upstream>: <reason>" for upstreams that never came up; unordered (parallel bringUp), sorted when reported
	report   []UpstreamStatus // per-upstream outcome of the FIRST bring-up pass, for StartReport (Stage 8); shares failMu with failures

	// supervisors tracks the per-stdio-upstream auto-restart goroutines (Stage
	// 7a) so Close can wait for them all to unwind before returning — otherwise a
	// supervisor mid-restart could touch conns after Close cleared it, a race the
	// -race detector would (rightly) flag. Each supervisor returns promptly once
	// procCtx is cancelled (Close does that first).
	supervisors sync.WaitGroup

	// subMu guards subscribers, the set of client-facing transports that want to
	// be told when the aggregated catalog changes at runtime (restart, upstream
	// list_changed, reload — Stage 7). The registry is the single producer of
	// catalog-change events; a transport that can push a server→client
	// notification (stdio) subscribes in its Serve loop. See Subscribe /
	// notifyCatalogChanged.
	subMu       sync.Mutex
	subscribers map[int]chan struct{}
	nextSubID   int

	// relistMu guards relistTimers, the per-upstream debounce timers for
	// tools/list_changed notifications (Stage 7b). A "noisy" upstream firing a
	// burst of list_changed must not trigger a re-list storm, so each
	// notification (re)arms a short timer and only its expiry runs the re-list.
	relistMu     sync.Mutex
	relistTimers map[string]*time.Timer

	// supMu guards supCancel, one context.CancelFunc per supervised stdio
	// upstream (Stage 7d). Each supervisor runs under its own context derived
	// from procCtx; cancelling an upstream's entry tells its supervisor to exit
	// WITHOUT restarting — used when reload removes or replaces that upstream, so
	// the deliberate Close of its old connection is not mistaken for a crash and
	// auto-restarted. Full shutdown needs nothing extra: Close cancels procCtx,
	// which cancels every derived supervisor context automatically.
	supMu     sync.Mutex
	supCancel map[string]context.CancelFunc
}

// relistDebounce is how long the registry waits after the last tools/list_
// changed from an upstream before re-listing it. Short enough to feel live,
// long enough to collapse a rapid burst into a single re-list (Stage 7b).
const relistDebounce = 200 * time.Millisecond

// New constructs a Registry from config. It does not start upstreams yet — call
// Start. callLog may be nil, in which case tool calls are not audited.
// payloadLog is the opt-in payload debug log (Stage 10); pass
// logging.NewPayloadLog("") for the no-op when payload logging is not wanted
// (doctor, tests) — it must not be nil. supervise=false disables the
// auto-restart supervisors entirely (see the field comment) — used by
// `mcp-gate doctor`, which wants exactly one pass.
func New(cfg *config.Config, logger *slog.Logger, callLog logging.CallLog, payloadLog logging.PayloadLog, supervise bool) *Registry {
	procCtx, procCancel := context.WithCancel(context.Background())
	r := &Registry{
		log:          logger,
		callLog:      callLog,
		payloadLog:   payloadLog,
		autoRestart:  supervise,
		conns:        map[string]Upstream{},
		tools:        map[string]ToolDescriptor{},
		toolRoute:    map[string]route{},
		rawTools:     map[string][]mcp.Tool{},
		subscribers:  map[int]chan struct{}{},
		relistTimers: map[string]*time.Timer{},
		supCancel:    map[string]context.CancelFunc{},
		procCtx:      procCtx,
		procCancel:   procCancel,
	}
	r.cfg.Store(cfg)
	r.start = r.startUpstream
	return r
}

// config returns the current configuration snapshot. It never returns nil after
// New. Callers get a consistent pointer even while Reload swaps the config.
func (r *Registry) config() *config.Config { return r.cfg.Load() }

// startUpstream is the production starter: it dispatches to the stdio or HTTP
// implementation based on the upstream's resolved kind. Both return an Upstream
// (StdioConn / HTTPConn), so the registry treats them uniformly from here on —
// the "interface on the second implementation" rule is satisfied by the
// existing Upstream interface, not a new upstream.Conn (docs/MCP_NOTES.md §8).
func (r *Registry) startUpstream(ctx context.Context, u config.Upstream) (Upstream, error) {
	switch u.ResolveKind() {
	case config.UpstreamHTTP:
		return r.startHTTP(u)
	default:
		return r.startStdio(ctx, u)
	}
}

// startStdio launches a stdio child-process upstream. The upstream→registry
// notification callback (Stage 7b) is handed to StartStdio itself, so it is in
// place BEFORE the connection's reader goroutine starts — installing it after
// the fact raced an upstream that notifies immediately on startup (found by
// independent review). Only stdio upstreams push notifications; HTTP has no
// long-lived reader (documented limitation), so startHTTP wires nothing.
func (r *Registry) startStdio(ctx context.Context, u config.Upstream) (Upstream, error) {
	env := make([]string, 0, len(u.Env))
	for k, v := range u.Env {
		env = append(env, k+"="+v)
	}
	name := u.Name
	onNotify := func(method string) { r.onUpstreamNotification(name, method) }
	return upstream.StartStdio(ctx, r.log, u.Name, u.Command, u.Args, env, onNotify)
}

// startHTTP builds an HTTP (Streamable HTTP) upstream connection. Unlike
// startStdio it does no network I/O here — the handshake runs in Initialize, so
// StartHTTP never fails and a genuinely unreachable HTTP upstream is isolated at
// the Initialize step in bringUp, exactly like a stdio upstream that fails its
// handshake.
func (r *Registry) startHTTP(u config.Upstream) (Upstream, error) {
	return upstream.StartHTTP(r.log, u.Name, u.URL, u.Headers, nil), nil
}

// Start launches every enabled upstream in parallel, runs the MCP handshake,
// lists tools/resources, and builds the aggregated namespaced catalog. A single
// upstream failing does not fail Start — it is logged and skipped. Start errors
// if it cannot proceed at all (e.g. context cancelled), or if every upstream
// failed (or none were enabled) leaving zero live connections — an empty
// gateway is not a useful degraded mode, it is a misconfiguration.
//
// The whole of Start runs under lifecycleMu: a SIGHUP-triggered Reload or a
// shutdown Close arriving mid-bring-up blocks until Start returns, instead of
// racing the parallel fan-out (see the lifecycleMu field comment).
func (r *Registry) Start(ctx context.Context) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	g, gctx := errgroup.WithContext(ctx)
	for _, u := range r.config().Upstreams {
		if !u.Enabled {
			continue
		}
		u := u
		g.Go(func() error {
			r.bringUp(gctx, u)
			return nil // errors are isolated per-upstream, never propagated
		})
	}
	// Wait never returns an error because bringUp swallows them; keep the check
	// for correctness if that ever changes.
	if err := g.Wait(); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// A gateway with zero live upstreams has nothing to aggregate — every
	// upstream configured either failed its handshake or none were enabled.
	// Serving that (empty tools/list forever) is pointless, so fail loudly
	// here instead of blocking with an empty catalog (found by user request).
	// Per-upstream failures above are still isolated from each other; this is
	// only the all-of-them-failed case.
	if r.upstreamCount() == 0 {
		return fmt.Errorf("no upstream MCP server is reachable:\n%s", r.failureSummary())
	}
	r.log.Info("registry ready", "upstreams", r.upstreamCount(), "tools", len(r.Tools()))
	r.phase = phaseRunning // Reload is admissible from here on (lifecycleMu held).
	return nil
}

// bringUp starts one upstream, handshakes, and merges its catalog. All failures
// are isolated: logged, recorded for the Start-time failure summary, and the
// upstream skipped/torn down. On success it merges the catalog and starts the
// auto-restart supervisor (Stage 7a) for a stdio upstream.
//
// The start-time diagnostics (recordFailure) live here, not in launch, because
// they feed Start's "every upstream failed" summary and (Stage 8) the doctor
// report — a mid-run restart via launch has no such summary to feed.
//
// ctx bounds only the handshake steps inside launch (Initialize/ListTools/
// ListResources) — the child process itself is launched under r.procCtx, a
// long-lived context independent of Start's errgroup context. See the comment
// on Registry.procCtx for why this distinction is load-bearing.
func (r *Registry) bringUp(ctx context.Context, u config.Upstream) {
	conn, tools, err := r.launch(ctx, u)
	if err != nil {
		r.log.Warn("upstream failed to come up", "upstream", u.Name, "err", err)
		r.recordFailure(u.Name, err.Error())
		return
	}
	n := r.merge(u.Name, conn, tools)
	r.recordSuccess(u.Name, n)
	r.superviseUpstream(u, conn)
}

// launch starts one upstream and runs the full handshake sequence
// (start → Initialize → ListTools, plus best-effort ListResources), returning
// the live connection and its tool catalog. It is the single reusable "bring an
// upstream to a usable state" primitive shared by the first start (bringUp),
// the auto-restart supervisor (Stage 7a) and hot-reload (Stage 7d). On any
// failure it tears the connection back down and returns a single error whose
// message names the failing phase — the caller decides whether to record it as
// a start-time failure, retry it, or log it.
//
// The child process is launched under r.procCtx (long-lived, see procCtx);
// ctx bounds only the handshake RPCs so a slow upstream cannot block Start (or
// a restart) indefinitely.
func (r *Registry) launch(ctx context.Context, u config.Upstream) (Upstream, []mcp.Tool, error) {
	timeout := r.config().EffectiveCallTimeout()

	conn, err := r.start(r.procCtx, u)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start: %w", err)
	}

	// Upstream→registry notifications (Stage 7b) are wired inside startStdio —
	// the callback rides into StartStdio itself, so it is set before the reader
	// goroutine starts and a list_changed arriving immediately is not missed.

	initCtx, cancel := context.WithTimeout(ctx, timeout)
	info, err := conn.Initialize(initCtx)
	cancel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("handshake failed: %w", err)
	}
	r.log.Info("upstream initialized", "upstream", u.Name, "server", info.ServerInfo.Name)

	toolsCtx, cancel := context.WithTimeout(ctx, timeout)
	tools, err := conn.ListTools(toolsCtx)
	cancel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("tools/list failed: %w", err)
	}

	// resources/list is best-effort: an upstream without resources must not be
	// dropped over it.
	resCtx, cancel := context.WithTimeout(ctx, timeout)
	if _, err := conn.ListResources(resCtx); err != nil {
		r.log.Debug("upstream resources/list failed (ignored)", "upstream", u.Name, "err", err)
	}
	cancel()

	return conn, tools, nil
}

// doneChan reports the "process died" channel of a stdio upstream, if conn is
// one. HTTP upstreams have no such channel (no long-lived process between
// calls), so ok is false for them — the same type-assertion trick already used
// for *http.Transport in HTTPConn.Close, kept out of the Upstream interface so
// the HTTP implementation need not fake a channel it cannot honour (Stage 7a).
func doneChan(conn Upstream) (<-chan struct{}, bool) {
	d, ok := conn.(interface{ Done() <-chan struct{} })
	if !ok {
		return nil, false
	}
	return d.Done(), true
}

// superviseUpstream starts (if enabled and applicable) the goroutine that
// watches one stdio upstream's process and auto-restarts it on death with
// exponential backoff (Stage 7a). It is a no-op for HTTP upstreams (no process
// to watch) and when the restart policy is disabled. The restart replays the
// exact config.Upstream u the upstream was first launched with — restart is
// "run it again", never "reconfigure it".
func (r *Registry) superviseUpstream(u config.Upstream, conn Upstream) {
	if !r.autoRestart {
		return // doctor mode (Stage 8): one diagnostic pass, never auto-restart.
	}
	// policy.Enabled is read ONCE here, only to gate whether a supervisor is
	// started at all; the policy itself is NOT captured for the supervisor — the
	// restart loop re-reads it from the live config on every attempt, so a
	// reload changing backoff/attempts takes effect without recreating the
	// supervisor (see restart).
	policy := r.config().EffectiveRestart()
	if policy.Enabled == nil || !*policy.Enabled {
		return
	}
	done, ok := doneChan(conn)
	if !ok {
		return // HTTP upstream: unreachability is caught at the next CallTool.
	}
	// Each supervisor gets its own context derived from procCtx, so a full
	// shutdown (Close cancels procCtx) retires it automatically, while reload
	// can retire exactly THIS supervisor via its cancel func (a deliberate
	// Close must not look like a crash). If a cancel func already exists for
	// the name (shouldn't, but be safe), retire the old supervisor first.
	supCtx, cancel := context.WithCancel(r.procCtx)
	r.supMu.Lock()
	if old, ok := r.supCancel[u.Name]; ok {
		old()
	}
	r.supCancel[u.Name] = cancel
	r.supMu.Unlock()

	r.supervisors.Add(1)
	go func() {
		defer r.supervisors.Done()
		r.supervise(u, conn, done, supCtx)
	}()
}

// retireSupervisor retires the supervisor watching upstream name, if any, so
// its next Close is not auto-restarted (reload removing/replacing an upstream).
// It cancels and forgets the supervisor's context; a supervisor (or its restart
// loop, mid-backoff or mid-launch) selecting on it exits without touching the
// catalog (see replaceUpstreamIfLive).
func (r *Registry) retireSupervisor(name string) {
	r.supMu.Lock()
	if cancel, ok := r.supCancel[name]; ok {
		cancel()
		delete(r.supCancel, name)
	}
	r.supMu.Unlock()
}

// supervise blocks until the current connection's process dies (done closes)
// or the supervisor's context is cancelled — by reload retiring this upstream,
// or by Close cancelling procCtx (supCtx is derived from it, so the parent's
// cancellation propagates automatically) — then drives the restart loop. Each
// successful restart installs a fresh connection via replaceUpstreamIfLive and
// re-arms the watch on the NEW connection's done channel; each failure backs
// off (exponentially, capped at MaxBackoff) and retries up to MaxAttempts
// (0 = unlimited). Exhausting the attempts leaves the upstream out of the
// catalog — the MVP terminal state.
//
// conn is tracked (not just its done channel) so a confirmed real crash can be
// reaped via conn.Close() before relaunching — otherwise cmd.Wait() is never
// called for the dead process and it leaks as a zombie plus its pipe fds
// forever, since nothing else holds a reference to it once replaceUpstream
// overwrites the registry's map entry (found by independent review;
// reproduced with a /proc zombie-count check).
func (r *Registry) supervise(u config.Upstream, conn Upstream, done <-chan struct{}, supCtx context.Context) {
	for {
		select {
		case <-supCtx.Done():
			return // reload retired this upstream, or Close cancelled procCtx.
		case <-done:
			// The process died. If supCtx is also done, it died BECAUSE we are
			// shutting down or reload deliberately Closed it. Either way, do not
			// restart — just exit. In both cases some other path (Registry.
			// Close's own loop, or retireAndClose) already owns (or will own)
			// closing conn, so we must NOT close it here too — that would
			// double-close via a different goroutine, which StdioConn.Close's
			// sync.Once now tolerates safely, but touching a connection this
			// code no longer owns is still the wrong call to make.
			if supCtx.Err() != nil {
				return
			}
			// A genuine crash, not a deliberate shutdown/retire: reap it before
			// attempting to relaunch.
			if err := conn.Close(); err != nil {
				r.log.Debug("close crashed upstream", "upstream", u.Name, "err", err)
			}
			r.log.Warn("stdio upstream exited, attempting restart", "upstream", u.Name)
			newConn, newDone, ok := r.restart(u, supCtx)
			if !ok {
				return // restart gave up (attempts exhausted), retired, or shutting down.
			}
			conn, done = newConn, newDone // watch the freshly-restarted connection.
		}
	}
}

// restart re-launches a dead stdio upstream with exponential backoff,
// returning the new connection and its done channel on success — the caller
// (supervise) must keep the returned conn to close it on the NEXT crash. It
// returns ok=false when the attempt budget is exhausted (upstream left out of
// the catalog), the upstream was retired by reload (supCtx cancelled), or the
// gateway is shutting down. On each successful relaunch it swaps the
// connection and catalog atomically via replaceUpstreamIfLive so a client
// never sees a torn catalog — and a relaunch that lost the race with a reload
// retiring this upstream is discarded instead of resurrecting it.
//
// The restart policy is global (not per-upstream); it is re-read from the live
// config at the start of EVERY attempt, so a reload that changed
// enabled/backoff/attempts is picked up without recreating the supervisor —
// including Enabled=false, which makes an already-looping supervisor give up
// on its next attempt (found by independent review of the Tier 2 fix: only
// superviseUpstream checked Enabled, so a mid-backoff supervisor kept retrying
// forever after a reload disabled auto-restart). Giving up deliberately does
// NOT dropUpstream — the upstream just stays in its current (unreachable)
// state, exactly as if the supervisor had never been started. The
// disabled→enabled transition for an upstream whose supervisor was never
// started (Enabled=false at Start time) is out of scope — it would need a
// separate mechanism to start a supervisor post-hoc.
func (r *Registry) restart(u config.Upstream, supCtx context.Context) (Upstream, <-chan struct{}, bool) {
	for attempt := 1; ; attempt++ {
		policy := r.config().EffectiveRestart()
		if policy.Enabled == nil || !*policy.Enabled {
			r.log.Info("stdio upstream restart disabled by reload, giving up", "upstream", u.Name)
			return nil, nil, false
		}
		if policy.MaxAttempts != 0 && attempt > policy.MaxAttempts {
			r.log.Error("stdio upstream exhausted restart attempts, dropping from catalog",
				"upstream", u.Name, "max_attempts", policy.MaxAttempts)
			r.dropUpstream(u.Name)
			r.notifyCatalogChanged()
			return nil, nil, false
		}
		// Backoff for this attempt, derived entirely from the CURRENT policy:
		// InitialBackoff grown by the fixed factor per previous failure, capped
		// at MaxBackoff.
		backoff := policy.InitialBackoff
		for i := 1; i < attempt && backoff < policy.MaxBackoff; i++ {
			backoff *= config.RestartBackoffFactor
		}
		if backoff > policy.MaxBackoff {
			backoff = policy.MaxBackoff
		}

		// Wait out the backoff, but abandon it immediately on shutdown or retire
		// (supCtx is derived from procCtx, so it covers both).
		timer := time.NewTimer(backoff)
		select {
		case <-supCtx.Done():
			timer.Stop()
			return nil, nil, false
		case <-timer.C:
		}

		conn, tools, err := r.launch(r.procCtx, u)
		if err != nil {
			r.log.Warn("stdio upstream restart attempt failed",
				"upstream", u.Name, "attempt", attempt, "err", err)
			continue
		}

		newDone, ok := doneChan(conn)
		if !ok {
			// Should not happen: launch of a stdio upstream yields a *StdioConn.
			// Guard anyway so a future non-stdio path cannot silently spin.
			_ = conn.Close()
			r.log.Error("restarted upstream has no done channel; giving up", "upstream", u.Name)
			return nil, nil, false
		}
		if !r.replaceUpstreamIfLive(u.Name, conn, tools, supCtx) {
			// A reload retired this upstream while we were launching: the fresh
			// connection must not enter the catalog (that would resurrect an
			// upstream the reload just removed/replaced). Close it and stop.
			_ = conn.Close()
			r.log.Info("stdio upstream retired during restart, discarding relaunch", "upstream", u.Name)
			return nil, nil, false
		}
		r.notifyCatalogChanged()
		r.log.Info("stdio upstream restarted", "upstream", u.Name, "attempt", attempt, "tools", len(tools))
		return conn, newDone, true
	}
}

// onUpstreamNotification handles a notification pushed by a stdio upstream
// (Stage 7b). It runs on that upstream's single reader goroutine, so it must
// not block or re-enter the connection: for tools/list_changed it only (re)arms
// a debounce timer whose expiry does the actual re-list on a fresh goroutine.
// Other notification methods are ignored (resources are not aggregated yet).
func (r *Registry) onUpstreamNotification(name, method string) {
	if method != mcp.NotifToolsListChanged {
		return
	}
	r.relistMu.Lock()
	defer r.relistMu.Unlock()
	if t, ok := r.relistTimers[name]; ok {
		t.Reset(relistDebounce)
		return
	}
	// AfterFunc fires on its own goroutine, so relistUpstream (which does
	// blocking RPCs) never runs on the reader goroutine.
	r.relistTimers[name] = time.AfterFunc(relistDebounce, func() {
		r.relistMu.Lock()
		delete(r.relistTimers, name)
		r.relistMu.Unlock()
		r.relistUpstream(name)
	})
}

// relistUpstream re-fetches one upstream's tools after it announced a change,
// swaps its catalog atomically (replaceUpstreamIfCurrent), and tells the client
// (notifyCatalogChanged). It runs off the debounce timer, bounded by the call
// timeout and abandoned if the gateway is shutting down. The connection is read
// under the lock (it may have been replaced/dropped by a concurrent restart);
// if the upstream is gone, there is nothing to re-list.
//
// The ListTools RPC can be in flight for seconds (up to the call timeout), and
// a Reload can retire/replace THIS upstream meanwhile — an unconditional write
// afterwards would resurrect a removed upstream or clobber the reload's fresh
// entry with a stale one (the same class of race replaceUpstreamIfLive closes
// for the supervisor, found by independent review of the Tier 2 fix). The
// write therefore goes through replaceUpstreamIfCurrent, which re-checks conn
// identity atomically with the catalog write. A discarded result must NOT
// Close conn: this path never owns the connection — either the reload already
// closed the old conn, or it is the live one and must stay open.
func (r *Registry) relistUpstream(name string) {
	if r.procCtx.Err() != nil {
		return // shutting down
	}
	r.mu.RLock()
	conn := r.conns[name]
	r.mu.RUnlock()
	if conn == nil {
		return // upstream was dropped (e.g. by a failed restart) — nothing to do
	}

	ctx, cancel := context.WithTimeout(r.procCtx, r.config().EffectiveCallTimeout())
	tools, err := conn.ListTools(ctx)
	cancel()
	if err != nil {
		r.log.Warn("re-list after upstream list_changed failed", "upstream", name, "err", err)
		return
	}
	if !r.replaceUpstreamIfCurrent(name, conn, tools) {
		r.log.Info("stale re-list discarded, upstream replaced or removed meanwhile", "upstream", name)
		return
	}
	r.notifyCatalogChanged()
}

// merge namespaces an upstream's tools and adds them to the aggregated catalog
// and routing table under the registry lock. It returns the number of tools
// actually projected into the catalog (post-filter, post-dedup).
func (r *Registry) merge(name string, conn Upstream, tools []mcp.Tool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.mergeLocked(name, conn, tools)
	r.log.Debug("upstream catalog merged", "upstream", name, "tools", n)
	return n
}

// toolEntry pairs a client-facing tool name with the (verbatim) upstream tool
// it exposes — the output unit of filterAndRenameTools.
type toolEntry struct {
	name string // renamed, or default "<upstream>__<original>"
	tool mcp.Tool
}

// filterAndRenameTools projects one upstream's raw tool list through its
// configured filter (Stage 9): allow (when non-empty — intersection), then
// deny (subtraction), then rename (client-facing name for survivors; tools
// without a rename get the default namespaced "<upstream>__<original>").
// It is pure — no registry state, no side effects — so the projection can be
// re-run at any time against the stored raw list (filter-only reload).
func filterAndRenameTools(upstream string, tools []mcp.Tool, f config.ToolFilter) []toolEntry {
	allow := make(map[string]bool, len(f.Allow))
	for _, a := range f.Allow {
		allow[a] = true
	}
	deny := make(map[string]bool, len(f.Deny))
	for _, d := range f.Deny {
		deny[d] = true
	}

	out := make([]toolEntry, 0, len(tools))
	for _, t := range tools {
		if len(allow) > 0 && !allow[t.Name] {
			continue
		}
		if deny[t.Name] {
			continue
		}
		name, renamed := f.Rename[t.Name]
		if !renamed {
			name = upstream + NameSeparator + t.Name
		}
		out = append(out, toolEntry{name: name, tool: t})
	}
	return out
}

// filterFor looks up the CURRENT tool filter for an upstream by name from the
// atomic config snapshot — deliberately not passed as a parameter: several
// callers of merge/replaceUpstream (relistUpstream, a supervisor holding the
// config.Upstream it was launched with) would otherwise carry a filter frozen
// at launch time, guaranteed stale after the next reload. A linear scan over
// units-to-tens of upstreams outside any hot path is fine. An upstream absent
// from the current config (narrow window: reload just removed it while a late
// re-list is still running) gets the empty filter — pass everything through,
// consistent with relistUpstream's own conn==nil bail-out.
func (r *Registry) filterFor(name string) config.ToolFilter {
	for _, u := range r.config().Upstreams {
		if u.Name == name {
			return u.Tools
		}
	}
	return config.ToolFilter{}
}

// mergeLocked is the shared catalog-write body used by merge and
// replaceUpstream, assuming r.mu is already held. It records the connection and
// the raw (pre-filter) tool list, then projects the tools through the
// upstream's current filter (Stage 9) into the catalog/routing table, skipping
// a duplicate client-facing name (keep first, log). Cross-upstream rename
// collisions are rejected by config.Validate for every name the config knows
// statically; this keep-first only guards runtime surprises (an upstream
// advertising a name that happens to match another's projection).
// It returns the number of entries actually added to the catalog by this call
// (post-filter, post-dedup) — the count the client really sees, which callers
// report to diagnostics (UpstreamStatus.Tools → doctor) and logs.
func (r *Registry) mergeLocked(name string, conn Upstream, tools []mcp.Tool) int {
	r.conns[name] = conn
	r.rawTools[name] = tools
	n := 0
	for _, e := range filterAndRenameTools(name, tools, r.filterFor(name)) {
		if _, dup := r.tools[e.name]; dup {
			r.log.Warn("duplicate client-facing tool name skipped", "name", e.name, "upstream", name)
			continue
		}
		r.tools[e.name] = ToolDescriptor{Name: e.name, Upstream: name, Tool: e.tool}
		r.toolRoute[e.name] = route{upstream: name, original: e.tool.Name}
		n++
	}
	return n
}

// dropUpstream removes an upstream and every catalog/routing entry it owns,
// under the registry lock. It is the mutation counterpart of merge: together
// they are the ONLY places conns/tools/toolRoute are written after Start, so
// the dynamic catalog (restart, list_changed, reload — Stage 7) always mutates
// through one guarded path and a client can never observe a half-populated
// catalog. It does NOT Close the connection — the caller owns that (Close is
// I/O and must happen outside the lock).
//
// Entries are identified by owner, not by the "<name>__" prefix: with tool
// renaming (Stage 9) a client-facing name need not carry the prefix, so keying
// removal on the recorded owner is correct regardless of how the name was
// formed.
func (r *Registry) dropUpstream(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropLocked(name)
}

// dropLocked is dropUpstream's body, assuming r.mu is already held. It exists so
// replaceUpstream can drop-then-merge under a SINGLE lock acquisition (below).
func (r *Registry) dropLocked(name string) {
	delete(r.conns, name)
	delete(r.rawTools, name)
	for ns, d := range r.tools {
		if d.Upstream == name {
			delete(r.tools, ns)
			delete(r.toolRoute, ns)
		}
	}
}

// replaceUpstream atomically swaps out an upstream's connection and catalog:
// it drops the old entries and merges the new ones under a single hold of
// r.mu, so the client never sees the upstream's tools vanish and reappear
// (which a separate dropUpstream+merge would expose). Used by reload of a
// changed upstream — the one context where no retire race applies (Reload
// holds lifecycleMu, so no other Reload can retire the entry it is itself
// installing), hence the always-live context (Background().Err() is always
// nil). The list_changed re-list goes through replaceUpstreamIfCurrent
// instead — it CAN race a Reload.
func (r *Registry) replaceUpstream(name string, conn Upstream, tools []mcp.Tool) {
	r.replaceUpstreamIfLive(name, conn, tools, context.Background())
}

// installLocked replaces name's catalog entry with (conn, tools), assuming
// r.mu is already held — the common tail of replaceUpstreamIfLive,
// replaceUpstreamIfCurrent, and remergeUpstream, once each has decided under
// its own gate that the write is safe to make.
func (r *Registry) installLocked(name string, conn Upstream, tools []mcp.Tool, logMsg string) {
	r.dropLocked(name)
	n := r.mergeLocked(name, conn, tools)
	r.log.Debug(logMsg, "upstream", name, "tools", n)
}

// replaceUpstreamIfLive is replaceUpstream with a liveness gate for the
// auto-restart supervisor: the supCtx check and the catalog write happen under
// ONE hold of r.mu, so "was I retired while launching?" and "install my fresh
// connection" are a single atomic step. Without that atomicity a restart that
// passed an earlier check could still install its connection after a reload's
// retireAndClose+dropUpstream ran, resurrecting an upstream the reload just
// removed (the supervisor-vs-reload race found by independent review). Returns
// false — leaving the catalog untouched — when supCtx was already cancelled;
// the caller then owns closing the never-installed connection.
func (r *Registry) replaceUpstreamIfLive(name string, conn Upstream, tools []mcp.Tool, supCtx context.Context) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if supCtx.Err() != nil {
		return false
	}
	r.installLocked(name, conn, tools, "upstream catalog replaced")
	return true
}

// replaceUpstreamIfCurrent is replaceUpstream with a currency gate for
// relistUpstream: the check that oldConn is STILL the live connection for this
// name, and the catalog write, happen under ONE hold of r.mu — so a Reload
// that retired/relaunched this exact upstream while the ListTools RPC was in
// flight cannot be raced. relistUpstream is triggered by an upstream's own
// list_changed notification, not by a supervisor, so it has no supervisor
// context to check (the gate replaceUpstreamIfLive uses) — the only truth it
// can consult is the r.conns map itself, hence the connection-identity
// comparison (all Upstream implementations are pointers, so == is identity).
// Returns false — leaving the catalog untouched — when r.conns[name] is no
// longer oldConn (removed, or already replaced by something newer).
func (r *Registry) replaceUpstreamIfCurrent(name string, oldConn Upstream, tools []mcp.Tool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conns[name] != oldConn {
		return false
	}
	r.installLocked(name, oldConn, tools, "upstream catalog refreshed after list_changed")
	return true
}

// Subscribe registers interest in runtime catalog changes and returns a channel
// that receives one value each time the catalog is mutated after Start
// (auto-restart, upstream list_changed, reload — Stage 7), plus an unsubscribe
// function the caller MUST call when it stops listening (typically on transport
// shutdown) so the registry does not keep publishing to a dead channel.
//
// The channel is buffered (size 1) and delivery is coalescing: notifyCatalog
// Changed never blocks, and if the subscriber has not yet drained a pending
// signal a burst of changes collapses into the one already queued. That is
// exactly the semantic a client wants — "something changed, re-list" — without
// a per-change backlog (Stage 7c).
func (r *Registry) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	r.subMu.Lock()
	id := r.nextSubID
	r.nextSubID++
	r.subscribers[id] = ch
	r.subMu.Unlock()

	return ch, func() {
		r.subMu.Lock()
		delete(r.subscribers, id)
		r.subMu.Unlock()
	}
}

// notifyCatalogChanged signals every subscriber that the aggregated catalog
// changed. It is non-blocking and coalescing (see Subscribe): a full buffer
// means a signal is already pending, which is all the subscriber needs to know.
func (r *Registry) notifyCatalogChanged() {
	r.subMu.Lock()
	defer r.subMu.Unlock()
	for _, ch := range r.subscribers {
		select {
		case ch <- struct{}{}:
		default: // a signal is already queued; coalesce.
		}
	}
}

// Reload applies a new configuration to the running gateway without a restart
// (Stage 7d, triggered by SIGHUP). It diffs newCfg's enabled upstreams against
// the currently live ones and:
//
//   - ADDED   (enabled, not live): launch + merge + supervise;
//   - REMOVED (live, now absent or disabled): retire supervisor + Close + drop;
//   - CHANGED (live, launch fields differ): retire supervisor + Close old +
//     launch new + replaceUpstream + supervise;
//   - FILTER-ONLY (live, launch identical, tools filter differs — Stage 9):
//     re-project the stored raw tool list through the new filter. No Close, no
//     relaunch, no network re-list — the upstream's raw catalog did not change,
//     only its projection did, so a deny-list edit takes effect on SIGHUP
//     without any upstream downtime;
//   - UNCHANGED: left running untouched.
//
// newCfg MUST already be validated (serve.go loads it via config.Load, which
// validates) — Reload assumes it is well-formed. The plan is computed under the
// catalog lock but the I/O (Close/launch) runs OUTSIDE it, mirroring how Close
// collects conns under the lock and closes them outside. A single
// notifyCatalogChanged at the end tells the client to re-list once, regardless
// of how many upstreams changed. Individual upstream launch failures are
// isolated (logged, that upstream simply absent) — one bad new upstream does not
// abort the whole reload, matching Start's per-upstream isolation.
//
// Reload runs entirely under lifecycleMu, mutually exclusive with Start and
// Close (see the lifecycleMu field comment): a reload can neither race a
// still-running Start (duplicate upstream processes) nor a concurrent Close
// (supervisors.Add vs supervisors.Wait — forbidden WaitGroup reuse). Before
// Start has completed it returns ErrNotStarted (retryable); once Close has
// begun it returns ErrClosing. Only after passing that gate is the config
// swapped atomically — a rejected reload must NOT leave newCfg's timeout/
// restart policy live while the catalog still reflects the old config — and
// from the swap on any concurrent CallTool immediately sees the new call
// timeout / restart policy; the catalog then converges.
func (r *Registry) Reload(ctx context.Context, newCfg *config.Config) error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	switch r.phase {
	case phaseNew:
		return ErrNotStarted
	case phaseClosing:
		return ErrClosing
	}

	oldCfg := r.config()
	r.cfg.Store(newCfg)

	// Index old and new enabled upstreams by name.
	oldEnabled := enabledByName(oldCfg)
	newEnabled := enabledByName(newCfg)

	// Snapshot which upstreams are currently live (in the catalog) under the
	// lock, so the diff reasons about actual state, not just old config (an old
	// upstream may have failed to start and never been live).
	r.mu.RLock()
	live := make(map[string]bool, len(r.conns))
	for name := range r.conns {
		live[name] = true
	}
	r.mu.RUnlock()

	var added, changed []config.Upstream
	var removed, filterOnly []string

	for name, nu := range newEnabled {
		ou, wasEnabled := oldEnabled[name]
		switch {
		case !live[name]:
			// Not currently live: (re)launch it. Covers a newly added upstream and
			// one that was configured before but never came up.
			added = append(added, nu)
		case wasEnabled && !ou.SameLaunch(nu):
			changed = append(changed, nu)
		case wasEnabled && !ou.SameFilter(nu):
			// FILTER-ONLY: launch identical (the previous case would have caught
			// anything else), only the tools filter differs. The new filter is
			// applied by re-merging the stored raw tool list below — mergeLocked
			// reads the filter from r.cfg, already swapped to newCfg above.
			filterOnly = append(filterOnly, name)
		default:
			// Live and unchanged (or was live from an identical launch): leave it.
		}
	}
	for name := range live {
		if _, stillEnabled := newEnabled[name]; !stillEnabled {
			removed = append(removed, name)
		}
	}

	// Apply the plan in three phases:
	//
	//  1. removed — SEQUENTIALLY, FIRST: retiring them frees their client-facing
	//     names before added/changed upstreams (possibly renamed onto the same
	//     names) try to claim them;
	//  2. changed + added — IN PARALLEL (errgroup, same pattern as Start): each
	//     involves a full launch/handshake, and one slow upstream must not
	//     serialize the whole reload. Errors stay isolated per-upstream (logged,
	//     never propagated — g.Go always returns nil), matching Start;
	//  3. filterOnly — SEQUENTIALLY, LAST: a pure re-projection without I/O that
	//     must see the final catalog state after removed/changed/added.
	//
	// Parallel goroutines here are safe: lifecycleMu is held for the whole
	// Reload (Tier 1), so no Start/Close/other Reload can interleave; the
	// catalog itself is guarded by r.mu inside merge/replaceUpstream.
	//
	// For removed/changed: retire the supervisor first (so the deliberate Close
	// is not auto-restarted), then Close the old connection, then drop or
	// relaunch. Close/launch are I/O — done outside the catalog lock (each of
	// dropUpstream/replaceUpstream takes the lock itself, briefly).
	for _, name := range removed {
		r.retireAndClose(name)
		r.dropUpstream(name)
		r.log.Info("upstream removed by reload", "upstream", name)
	}
	var g errgroup.Group
	for _, u := range changed {
		u := u
		g.Go(func() error {
			r.retireAndClose(u.Name)
			conn, tools, err := r.launch(ctx, u)
			if err != nil {
				// The changed upstream failed to relaunch: drop it (its old connection
				// is already closed) rather than leave the stale catalog entry.
				r.dropUpstream(u.Name)
				r.log.Warn("changed upstream failed to relaunch, dropped", "upstream", u.Name, "err", err)
				return nil
			}
			r.replaceUpstream(u.Name, conn, tools)
			r.superviseUpstream(u, conn)
			r.log.Info("upstream reconfigured by reload", "upstream", u.Name, "tools", len(tools))
			return nil
		})
	}
	for _, u := range added {
		u := u
		g.Go(func() error {
			conn, tools, err := r.launch(ctx, u)
			if err != nil {
				r.log.Warn("added upstream failed to launch", "upstream", u.Name, "err", err)
				return nil
			}
			r.merge(u.Name, conn, tools)
			r.superviseUpstream(u, conn)
			r.log.Info("upstream added by reload", "upstream", u.Name, "tools", len(tools))
			return nil
		})
	}
	_ = g.Wait() // errors are handled inside each goroutine; Wait only joins them.
	for _, name := range filterOnly {
		r.remergeUpstream(name)
		r.log.Info("upstream tool filter updated by reload", "upstream", name)
	}

	r.notifyCatalogChanged()
	r.log.Info("config reloaded",
		"added", len(added), "changed", len(changed), "removed", len(removed), "filter_only", len(filterOnly))
	return nil
}

// remergeUpstream re-projects an upstream's stored raw tool list through the
// CURRENT config's filter, under a single hold of r.mu — the client never sees
// a torn catalog, exactly like replaceUpstream (Stage 9, filter-only reload).
// No I/O happens here: the connection and the raw list are reused as-is. A
// concurrent restart/reload may have dropped the upstream since the diff was
// computed — then there is nothing to re-merge.
func (r *Registry) remergeUpstream(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conn, ok := r.conns[name]
	if !ok {
		return
	}
	tools := r.rawTools[name] // read BEFORE installLocked's drop, which deletes the entry
	r.installLocked(name, conn, tools, "upstream catalog re-projected")
}

// retireAndClose retires an upstream's supervisor and closes its live
// connection, if any. Order matters: retire first so the supervisor treats the
// coming Close as a deliberate teardown, not a crash to auto-restart.
func (r *Registry) retireAndClose(name string) {
	r.retireSupervisor(name)
	r.mu.RLock()
	conn := r.conns[name]
	r.mu.RUnlock()
	if conn != nil {
		if err := conn.Close(); err != nil {
			r.log.Debug("close upstream during reload", "upstream", name, "err", err)
		}
	}
}

// enabledByName indexes a config's ENABLED upstreams by name. Disabled entries
// are excluded — reload treats a disabled upstream the same as an absent one.
func enabledByName(cfg *config.Config) map[string]config.Upstream {
	m := make(map[string]config.Upstream, len(cfg.Upstreams))
	for _, u := range cfg.Upstreams {
		if u.Enabled {
			m[u.Name] = u
		}
	}
	return m
}

// Tools returns the aggregated, namespaced tool catalog, sorted by name for
// deterministic output.
func (r *Registry) Tools() []ToolDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ToolDescriptor, 0, len(r.tools))
	for _, d := range r.tools {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// CallTool routes a namespaced tool call to its owning upstream, rewriting the
// name back to the upstream's original before forwarding. It records an audit
// entry (metadata only — never the arguments). The returned *mcp.Message is the
// raw upstream response (which may itself carry a JSON-RPC error).
//
// A routing/transport failure (as opposed to the upstream's own JSON-RPC
// error, returned verbatim in resp) is logged here with full detail — upstream
// name, underlying error — and returned to the caller as a short, sanitized
// message that names only the tool the client itself already asked for.
// dispatch.go forwards this error text to the client verbatim, so leaking the
// upstream name or an internal transport error string here would leak the
// gateway's topology/internals to whoever holds a valid auth_token (found by
// code review — the previous message included both).
func (r *Registry) CallTool(ctx context.Context, namespaced string, arguments json.RawMessage) (*mcp.Message, error) {
	r.mu.RLock()
	rt, ok := r.toolRoute[namespaced]
	conn := r.conns[rt.upstream]
	r.mu.RUnlock()

	if !ok || conn == nil {
		return nil, fmt.Errorf("unknown tool %q", namespaced)
	}

	callCtx, cancel := context.WithTimeout(ctx, r.config().EffectiveCallTimeout())
	defer cancel()

	start := time.Now()
	resp, err := conn.CallTool(callCtx, rt.original, arguments)
	r.audit(rt.upstream, mcp.MethodToolsCall, namespaced, start, resp, err)
	r.recordPayload(rt.upstream, namespaced, arguments, resp, err)
	if err != nil {
		r.log.Warn("tool call failed", "tool", namespaced, "upstream", rt.upstream, "err", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("call %q timed out", namespaced)
		}
		return nil, fmt.Errorf("call %q failed", namespaced)
	}
	return resp, nil
}

// audit writes one CallRecord. Arguments are never logged (may hold secrets).
func (r *Registry) audit(up, method, tool string, start time.Time, resp *mcp.Message, err error) {
	if r.callLog == nil {
		return
	}
	rec := logging.CallRecord{
		Time:     start,
		Upstream: up,
		Method:   method,
		Tool:     tool,
		Duration: time.Since(start),
		OK:       err == nil && (resp == nil || resp.Error == nil),
	}
	switch {
	case err != nil:
		rec.Err = err.Error()
	case resp != nil && resp.Error != nil:
		rec.Err = resp.Error.Message // upstream error message; no arguments
	}
	r.callLog.Record(rec)
}

// recordPayload writes one PayloadRecord to the OPT-IN payload debug log — the
// full arguments and result of a call. This is deliberately kept separate from
// audit (which stays metadata-only): only here, when the operator explicitly
// enabled payload logging, do raw arguments (possible secrets) hit disk. When
// payload logging is disabled r.payloadLog is a no-op, so this is a cheap call.
func (r *Registry) recordPayload(up, tool string, arguments json.RawMessage, resp *mcp.Message, err error) {
	rec := logging.PayloadRecord{
		Time:      time.Now(),
		Upstream:  up,
		Tool:      tool,
		Method:    mcp.MethodToolsCall,
		OK:        err == nil && (resp == nil || resp.Error == nil),
		Arguments: arguments,
	}
	switch {
	case err != nil:
		rec.Err = err.Error()
	case resp != nil && resp.Error != nil:
		rec.Err = resp.Error.Message
		rec.ErrorData = resp.Error.Data
		rec.Result = resp.Result
	case resp != nil:
		rec.Result = resp.Result
	}
	r.payloadLog.Record(rec)
}

func (r *Registry) upstreamCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}

// recordFailure notes why an upstream never came up, so Start can surface a
// concrete cause if every upstream fails — not just "check the logs" (bringUp
// runs upstreams in parallel, so failures are collected here rather than
// threaded back through errgroup, which discards them by design).
func (r *Registry) recordFailure(name, reason string) {
	r.failMu.Lock()
	defer r.failMu.Unlock()
	r.failures = append(r.failures, name+": "+reason)
	r.report = append(r.report, UpstreamStatus{Name: name, Err: reason})
}

// recordSuccess is recordFailure's happy-path twin: it notes an upstream that
// came up on the first pass and how many tools it contributed, feeding
// StartReport (Stage 8). Start-time only, like recordFailure — later restarts
// and reloads do not rewrite history.
func (r *Registry) recordSuccess(name string, tools int) {
	r.failMu.Lock()
	defer r.failMu.Unlock()
	r.report = append(r.report, UpstreamStatus{Name: name, OK: true, Tools: tools})
}

// StartReport returns one UpstreamStatus per enabled upstream, reflecting the
// state at the end of the very first bring-up pass — call it AFTER Start. The
// slice is a sorted copy (bringUp runs upstreams in parallel, so the internal
// order is nondeterministic); mutating it does not affect the registry.
func (r *Registry) StartReport() []UpstreamStatus {
	r.failMu.Lock()
	out := append([]UpstreamStatus(nil), r.report...)
	r.failMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// failureSummary renders one "- upstream: reason" line per recorded failure,
// sorted for deterministic output despite bringUp running upstreams in
// parallel. Empty when no upstream was even enabled (config has none, or all
// are disabled) — a distinct cause from every enabled one failing.
func (r *Registry) failureSummary() string {
	r.failMu.Lock()
	reasons := append([]string(nil), r.failures...)
	r.failMu.Unlock()

	if len(reasons) == 0 {
		return "  (no upstream is enabled in the config — nothing was even attempted)"
	}
	sort.Strings(reasons)
	lines := make([]string, len(reasons))
	for i, r := range reasons {
		lines[i] = "  - " + r
	}
	return strings.Join(lines, "\n")
}

// Close tears down all upstream connections/child processes, joining any errors.
//
// It runs under lifecycleMu, mutually exclusive with Start and Reload: by the
// time supervisors.Wait() below runs, no Reload can be mid-flight about to
// supervisors.Add(1) — the WaitGroup reuse race independent review flagged.
// Marking phaseClosing first makes any Reload queued behind this lock bail out
// with ErrClosing instead of relaunching upstreams during shutdown.
func (r *Registry) Close() error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	r.phase = phaseClosing

	// Cancel the long-lived process context first: any upstream still mid-launch
	// (e.g. blocked in cmd.Start or the handshake) unwinds, and it backstops each
	// conn's own graceful Close below in case a child ignores stdin closing.
	r.procCancel()

	// Wait for every auto-restart supervisor to observe the cancellation and
	// return BEFORE we clear conns below: a supervisor mid-restart could
	// otherwise call replaceUpstream after Close emptied the map, resurrecting a
	// connection Close would then never tear down (goroutine + child-process
	// leak) and racing the map access. procCancel above makes them all exit
	// promptly (every supCtx is derived from procCtx, so cancelling the parent
	// cancels them all — their selects and backoff timers watch supCtx).
	r.supervisors.Wait()

	// Every supervisor has returned; drop their (already cancelled) cancel
	// funcs. Pure tidiness — nothing reads the map after this point.
	r.supMu.Lock()
	r.supCancel = map[string]context.CancelFunc{}
	r.supMu.Unlock()

	// Stop any pending re-list debounce timers so none fires after shutdown.
	// A timer that already fired is harmless (relistUpstream bails on procCtx
	// cancellation), this just avoids a needless late wakeup.
	r.relistMu.Lock()
	for _, t := range r.relistTimers {
		t.Stop()
	}
	r.relistTimers = map[string]*time.Timer{}
	r.relistMu.Unlock()

	r.mu.Lock()
	conns := r.conns
	r.conns = map[string]Upstream{}
	r.mu.Unlock()

	var errs []error
	for name, c := range conns {
		if err := c.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close upstream %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}
