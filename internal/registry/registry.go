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
	start   upstreamStarter

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

	mu        sync.RWMutex
	conns     map[string]Upstream
	tools     map[string]ToolDescriptor // namespaced name → descriptor
	toolRoute map[string]route          // namespaced name → (upstream, original)

	failMu   sync.Mutex
	failures []string // "<upstream>: <reason>" for upstreams that never came up; unordered (parallel bringUp), sorted when reported

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

	// supMu guards supStop, one stop-channel per supervised stdio upstream
	// (Stage 7d). Closing an upstream's channel tells its supervisor to exit
	// WITHOUT restarting — used when reload removes or replaces that upstream, so
	// the deliberate Close of its old connection is not mistaken for a crash and
	// auto-restarted. Full shutdown uses procCtx instead (Close cancels it).
	supMu   sync.Mutex
	supStop map[string]chan struct{}
}

// relistDebounce is how long the registry waits after the last tools/list_
// changed from an upstream before re-listing it. Short enough to feel live,
// long enough to collapse a rapid burst into a single re-list (Stage 7b).
const relistDebounce = 200 * time.Millisecond

// New constructs a Registry from config. It does not start upstreams yet — call
// Start. callLog may be nil, in which case tool calls are not audited.
func New(cfg *config.Config, logger *slog.Logger, callLog logging.CallLog) *Registry {
	procCtx, procCancel := context.WithCancel(context.Background())
	r := &Registry{
		log:          logger,
		callLog:      callLog,
		conns:        map[string]Upstream{},
		tools:        map[string]ToolDescriptor{},
		toolRoute:    map[string]route{},
		subscribers:  map[int]chan struct{}{},
		relistTimers: map[string]*time.Timer{},
		supStop:      map[string]chan struct{}{},
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

// startStdio launches a stdio child-process upstream.
func (r *Registry) startStdio(ctx context.Context, u config.Upstream) (Upstream, error) {
	env := make([]string, 0, len(u.Env))
	for k, v := range u.Env {
		env = append(env, k+"="+v)
	}
	return upstream.StartStdio(ctx, r.log, u.Name, u.Command, u.Args, env)
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
func (r *Registry) Start(ctx context.Context) error {
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
	r.merge(u.Name, conn, tools)
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

	// Wire up upstream→registry notifications (Stage 7b) before the handshake,
	// so a list_changed that arrives immediately is not missed. Only stdio
	// upstreams push notifications (HTTP has no long-lived reader — documented
	// limitation), so this is a type-assertion, like Done()/doneChan.
	if n, ok := conn.(interface{ OnNotification(func(string)) }); ok {
		name := u.Name
		n.OnNotification(func(method string) { r.onUpstreamNotification(name, method) })
	}

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
	policy := r.config().EffectiveRestart()
	if policy.Enabled == nil || !*policy.Enabled {
		return
	}
	done, ok := doneChan(conn)
	if !ok {
		return // HTTP upstream: unreachability is caught at the next CallTool.
	}
	// Register a per-upstream stop channel so reload can retire THIS supervisor
	// (its deliberate Close must not look like a crash). If one already exists
	// for the name (shouldn't, but be safe), retire it first.
	stop := make(chan struct{})
	r.supMu.Lock()
	if old, ok := r.supStop[u.Name]; ok {
		close(old)
	}
	r.supStop[u.Name] = stop
	r.supMu.Unlock()

	r.supervisors.Add(1)
	go func() {
		defer r.supervisors.Done()
		r.supervise(u, conn, done, stop, policy)
	}()
}

// stopSupervisor retires the supervisor watching upstream name, if any, so its
// next Close is not auto-restarted (reload removing/replacing an upstream). It
// closes and forgets the stop channel; a supervisor selecting on it exits.
func (r *Registry) stopSupervisor(name string) {
	r.supMu.Lock()
	if stop, ok := r.supStop[name]; ok {
		close(stop)
		delete(r.supStop, name)
	}
	r.supMu.Unlock()
}

// supervise blocks until the current connection's process dies (done closes),
// the registry shuts down (procCtx cancelled), or this upstream is retired by
// reload (stop closed), then drives the restart loop. Each successful restart
// installs a fresh connection via replaceUpstream and re-arms the watch on the
// NEW connection's done channel; each failure backs off (exponentially, capped
// at MaxBackoff) and retries up to MaxAttempts (0 = unlimited). Exhausting the
// attempts leaves the upstream out of the catalog — the MVP terminal state.
//
// conn is tracked (not just its done channel) so a confirmed real crash can be
// reaped via conn.Close() before relaunching — otherwise cmd.Wait() is never
// called for the dead process and it leaks as a zombie plus its pipe fds
// forever, since nothing else holds a reference to it once replaceUpstream
// overwrites the registry's map entry (found by independent review;
// reproduced with a /proc zombie-count check).
func (r *Registry) supervise(u config.Upstream, conn Upstream, done <-chan struct{}, stop <-chan struct{}, policy config.RestartPolicy) {
	for {
		select {
		case <-r.procCtx.Done():
			return // gateway shutting down: Close cancelled procCtx.
		case <-stop:
			return // this upstream was retired/replaced by reload.
		case <-done:
			// The process died. If procCtx is also done, it died BECAUSE we are
			// shutting down; if stop is closed, reload deliberately Closed it.
			// Either way, do not restart — just exit. In both cases some other
			// path (Registry.Close's own loop, or retireAndClose) already owns
			// (or will own) closing conn, so we must NOT close it here too —
			// that would double-close via a different goroutine, which
			// StdioConn.Close's sync.Once now tolerates safely, but touching a
			// connection this code no longer owns is still the wrong call to
			// make.
			if r.procCtx.Err() != nil || isClosed(stop) {
				return
			}
			// A genuine crash, not a deliberate shutdown/retire: reap it before
			// attempting to relaunch.
			if err := conn.Close(); err != nil {
				r.log.Debug("close crashed upstream", "upstream", u.Name, "err", err)
			}
			r.log.Warn("stdio upstream exited, attempting restart", "upstream", u.Name)
			newConn, newDone, ok := r.restart(u, stop, policy)
			if !ok {
				return // restart gave up (attempts exhausted), retired, or shutting down.
			}
			conn, done = newConn, newDone // watch the freshly-restarted connection.
		}
	}
}

// isClosed reports whether a struct{} signalling channel has been closed. Safe
// only for a channel that is exclusively closed (never sent to), which is how
// the per-upstream stop channel is used.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// restart re-launches a dead stdio upstream with exponential backoff,
// returning the new connection and its done channel on success — the caller
// (supervise) must keep the returned conn to close it on the NEXT crash. It
// returns ok=false when the attempt budget is exhausted (upstream left out of
// the catalog), the upstream was retired by reload (stop), or the gateway is
// shutting down. On each successful relaunch it swaps the connection and
// catalog atomically via replaceUpstream so a client never sees a torn
// catalog.
func (r *Registry) restart(u config.Upstream, stop <-chan struct{}, policy config.RestartPolicy) (Upstream, <-chan struct{}, bool) {
	backoff := policy.InitialBackoff
	for attempt := 1; policy.MaxAttempts == 0 || attempt <= policy.MaxAttempts; attempt++ {
		// Wait out the backoff, but abandon it immediately on shutdown or retire.
		timer := time.NewTimer(backoff)
		select {
		case <-r.procCtx.Done():
			timer.Stop()
			return nil, nil, false
		case <-stop:
			timer.Stop()
			return nil, nil, false
		case <-timer.C:
		}

		conn, tools, err := r.launch(r.procCtx, u)
		if err != nil {
			r.log.Warn("stdio upstream restart attempt failed",
				"upstream", u.Name, "attempt", attempt, "err", err)
			backoff *= config.RestartBackoffFactor
			if backoff > policy.MaxBackoff {
				backoff = policy.MaxBackoff
			}
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
		r.replaceUpstream(u.Name, conn, tools)
		r.notifyCatalogChanged()
		r.log.Info("stdio upstream restarted", "upstream", u.Name, "attempt", attempt, "tools", len(tools))
		return conn, newDone, true
	}
	r.log.Error("stdio upstream exhausted restart attempts, dropping from catalog",
		"upstream", u.Name, "max_attempts", policy.MaxAttempts)
	r.dropUpstream(u.Name)
	r.notifyCatalogChanged()
	return nil, nil, false
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
// swaps its catalog atomically (replaceUpstream), and tells the client
// (notifyCatalogChanged). It runs off the debounce timer, bounded by the call
// timeout and abandoned if the gateway is shutting down. The connection is read
// under the lock (it may have been replaced/dropped by a concurrent restart);
// if the upstream is gone, there is nothing to re-list.
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
	r.replaceUpstream(name, conn, tools)
	r.notifyCatalogChanged()
	r.log.Info("upstream catalog refreshed after list_changed", "upstream", name, "tools", len(tools))
}

// merge namespaces an upstream's tools and adds them to the aggregated catalog
// and routing table under the registry lock.
func (r *Registry) merge(name string, conn Upstream, tools []mcp.Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mergeLocked(name, conn, tools)
	r.log.Debug("upstream catalog merged", "upstream", name, "tools", len(tools))
}

// mergeLocked is the shared catalog-write body used by merge and
// replaceUpstream, assuming r.mu is already held. It records the connection and
// namespaces each tool into the catalog/routing table, skipping a duplicate
// namespaced name (same upstream advertising it twice — keep first, log).
func (r *Registry) mergeLocked(name string, conn Upstream, tools []mcp.Tool) {
	r.conns[name] = conn
	for _, t := range tools {
		ns := name + NameSeparator + t.Name
		if _, dup := r.tools[ns]; dup {
			r.log.Debug("duplicate namespaced tool skipped", "name", ns)
			continue
		}
		r.tools[ns] = ToolDescriptor{Name: ns, Upstream: name, Tool: t}
		r.toolRoute[ns] = route{upstream: name, original: t.Name}
	}
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
// (which a separate dropUpstream+merge would expose). Used by the auto-restart
// supervisor, list_changed re-list, and reload of a changed upstream.
func (r *Registry) replaceUpstream(name string, conn Upstream, tools []mcp.Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dropLocked(name)
	r.mergeLocked(name, conn, tools)
	r.log.Debug("upstream catalog replaced", "upstream", name, "tools", len(tools))
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
// The atomic config swap happens FIRST so any concurrent CallTool immediately
// sees the new call timeout / restart policy; the catalog then converges.
func (r *Registry) Reload(ctx context.Context, newCfg *config.Config) error {
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
	var removed []string

	for name, nu := range newEnabled {
		ou, wasEnabled := oldEnabled[name]
		switch {
		case !live[name]:
			// Not currently live: (re)launch it. Covers a newly added upstream and
			// one that was configured before but never came up.
			added = append(added, nu)
		case wasEnabled && !ou.SameLaunch(nu):
			changed = append(changed, nu)
		default:
			// Live and unchanged (or was live from an identical launch): leave it.
		}
	}
	for name := range live {
		if _, stillEnabled := newEnabled[name]; !stillEnabled {
			removed = append(removed, name)
		}
	}

	// Apply removals and changes: retire the supervisor first (so the deliberate
	// Close is not auto-restarted), then Close the old connection, then drop or
	// relaunch. Close/launch are I/O — done outside the catalog lock (each of
	// dropUpstream/replaceUpstream takes the lock itself, briefly).
	for _, name := range removed {
		r.retireAndClose(name)
		r.dropUpstream(name)
		r.log.Info("upstream removed by reload", "upstream", name)
	}
	for _, u := range changed {
		r.retireAndClose(u.Name)
		conn, tools, err := r.launch(ctx, u)
		if err != nil {
			// The changed upstream failed to relaunch: drop it (its old connection
			// is already closed) rather than leave the stale catalog entry.
			r.dropUpstream(u.Name)
			r.log.Warn("changed upstream failed to relaunch, dropped", "upstream", u.Name, "err", err)
			continue
		}
		r.replaceUpstream(u.Name, conn, tools)
		r.superviseUpstream(u, conn)
		r.log.Info("upstream reconfigured by reload", "upstream", u.Name, "tools", len(tools))
	}
	for _, u := range added {
		conn, tools, err := r.launch(ctx, u)
		if err != nil {
			r.log.Warn("added upstream failed to launch", "upstream", u.Name, "err", err)
			continue
		}
		r.merge(u.Name, conn, tools)
		r.superviseUpstream(u, conn)
		r.log.Info("upstream added by reload", "upstream", u.Name, "tools", len(tools))
	}

	r.notifyCatalogChanged()
	r.log.Info("config reloaded", "added", len(added), "changed", len(changed), "removed", len(removed))
	return nil
}

// retireAndClose retires an upstream's supervisor and closes its live
// connection, if any. Order matters: retire first so the supervisor treats the
// coming Close as a deliberate teardown, not a crash to auto-restart.
func (r *Registry) retireAndClose(name string) {
	r.stopSupervisor(name)
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
func (r *Registry) Close() error {
	// Cancel the long-lived process context first: any upstream still mid-launch
	// (e.g. blocked in cmd.Start or the handshake) unwinds, and it backstops each
	// conn's own graceful Close below in case a child ignores stdin closing.
	r.procCancel()

	// Wait for every auto-restart supervisor to observe the cancellation and
	// return BEFORE we clear conns below: a supervisor mid-restart could
	// otherwise call replaceUpstream after Close emptied the map, resurrecting a
	// connection Close would then never tear down (goroutine + child-process
	// leak) and racing the map access. procCancel above makes them all exit
	// promptly (their selects and backoff timers watch procCtx).
	r.supervisors.Wait()

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
