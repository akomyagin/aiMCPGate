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
	cfg     *config.Config
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
}

// New constructs a Registry from config. It does not start upstreams yet — call
// Start. callLog may be nil, in which case tool calls are not audited.
func New(cfg *config.Config, logger *slog.Logger, callLog logging.CallLog) *Registry {
	procCtx, procCancel := context.WithCancel(context.Background())
	r := &Registry{
		cfg:        cfg,
		log:        logger,
		callLog:    callLog,
		conns:      map[string]Upstream{},
		tools:      map[string]ToolDescriptor{},
		toolRoute:  map[string]route{},
		procCtx:    procCtx,
		procCancel: procCancel,
	}
	r.start = r.startUpstream
	return r
}

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
	for _, u := range r.cfg.Upstreams {
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
// are isolated: logged and the upstream skipped/torn down.
//
// ctx bounds only the handshake steps below (Initialize/ListTools/
// ListResources) — the child process itself is launched under r.procCtx, a
// long-lived context independent of Start's errgroup context. See the comment
// on Registry.procCtx for why this distinction is load-bearing.
func (r *Registry) bringUp(ctx context.Context, u config.Upstream) {
	timeout := r.cfg.EffectiveCallTimeout()

	conn, err := r.start(r.procCtx, u)
	if err != nil {
		r.log.Warn("upstream failed to start", "upstream", u.Name, "err", err)
		r.recordFailure(u.Name, "failed to start: "+err.Error())
		return
	}

	initCtx, cancel := context.WithTimeout(ctx, timeout)
	info, err := conn.Initialize(initCtx)
	cancel()
	if err != nil {
		r.log.Warn("upstream handshake failed", "upstream", u.Name, "err", err)
		r.recordFailure(u.Name, "handshake failed: "+err.Error())
		_ = conn.Close()
		return
	}
	r.log.Info("upstream initialized", "upstream", u.Name, "server", info.ServerInfo.Name)

	toolsCtx, cancel := context.WithTimeout(ctx, timeout)
	tools, err := conn.ListTools(toolsCtx)
	cancel()
	if err != nil {
		r.log.Warn("upstream tools/list failed", "upstream", u.Name, "err", err)
		r.recordFailure(u.Name, "tools/list failed: "+err.Error())
		_ = conn.Close()
		return
	}

	// resources/list is best-effort: an upstream without resources must not be
	// dropped over it.
	resCtx, cancel := context.WithTimeout(ctx, timeout)
	if _, err := conn.ListResources(resCtx); err != nil {
		r.log.Debug("upstream resources/list failed (ignored)", "upstream", u.Name, "err", err)
	}
	cancel()

	r.merge(u.Name, conn, tools)
}

// merge namespaces an upstream's tools and adds them to the aggregated catalog
// and routing table under the registry lock.
func (r *Registry) merge(name string, conn Upstream, tools []mcp.Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.conns[name] = conn
	for _, t := range tools {
		ns := name + NameSeparator + t.Name
		if _, dup := r.tools[ns]; dup {
			// Same upstream advertising a duplicate name — keep first, log.
			r.log.Debug("duplicate namespaced tool skipped", "name", ns)
			continue
		}
		r.tools[ns] = ToolDescriptor{Name: ns, Upstream: name, Tool: t}
		r.toolRoute[ns] = route{upstream: name, original: t.Name}
	}
	r.log.Debug("upstream catalog merged", "upstream", name, "tools", len(tools))
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
func (r *Registry) CallTool(ctx context.Context, namespaced string, arguments json.RawMessage) (*mcp.Message, error) {
	r.mu.RLock()
	rt, ok := r.toolRoute[namespaced]
	conn := r.conns[rt.upstream]
	r.mu.RUnlock()

	if !ok || conn == nil {
		return nil, fmt.Errorf("unknown tool %q", namespaced)
	}

	callCtx, cancel := context.WithTimeout(ctx, r.cfg.EffectiveCallTimeout())
	defer cancel()

	start := time.Now()
	resp, err := conn.CallTool(callCtx, rt.original, arguments)
	r.audit(rt.upstream, mcp.MethodToolsCall, namespaced, start, resp, err)
	if err != nil {
		return nil, fmt.Errorf("call %q on upstream %q: %w", namespaced, rt.upstream, err)
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
