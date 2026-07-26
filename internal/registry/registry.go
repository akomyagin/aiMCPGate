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
	"regexp"
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

// ErrUnknownResource is wrapped by ReadResource when no upstream owns the
// requested uri — neither an exact catalog entry nor a template match. The
// dispatcher checks it with errors.Is to answer -32602 Invalid params (a bad
// argument from the client) instead of the generic internal error a transport
// failure gets.
var ErrUnknownResource = errors.New("unknown resource")

// ErrUnknownCompletionRef is wrapped by Complete when the referenced prompt or
// resource resolves to no upstream — same -32602 contract as
// ErrUnknownResource, for completion/complete.
var ErrUnknownCompletionRef = errors.New("unknown completion ref")

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

// PromptDescriptor is one aggregated prompt entry in the merged catalog
// (Round 4) — ToolDescriptor's prompts twin. Name is the client-facing
// namespaced name; unlike tools, prompts do NOT pass through config.ToolFilter
// (no allow/deny/rename mechanism exists for them in this round), so Name is
// always "<upstream>__<original>", without exception. The prompt itself
// (Description/Arguments) is carried verbatim from the upstream.
type PromptDescriptor struct {
	Name     string
	Upstream string
	Prompt   mcp.Prompt
}

// ResourceDescriptor is one aggregated resource entry in the merged catalog
// (Round 5). Unlike tools and prompts, resources are addressed by URI and are
// NOT namespaced — the client-facing URI is the upstream's own, verbatim; the
// registry only records which upstream OWNS it (keep-first on a cross-upstream
// URI collision), so resources/read can be routed without any rewrite.
type ResourceDescriptor struct {
	URI      string
	Upstream string
	Resource mcp.Resource
}

// ResourceTemplateDescriptor is one aggregated resource-template entry
// (Round 5). Templates are MATCHED against a concrete URI, never resolved by
// exact lookup, so they live in an ordered slice, not a map: two upstreams may
// legitimately advertise overlapping (even identical) templates, and
// resolveResourceOwner picks the FIRST match in merge order — deterministic,
// because Start/Reload merge upstreams sequentially in config order.
type ResourceTemplateDescriptor struct {
	URITemplate string
	Upstream    string
	Template    mcp.ResourceTemplate

	// re is the compiled matcher for URITemplate ({var} → one path segment),
	// built once at merge time. nil when the template failed to compile — the
	// descriptor is still listed to the client verbatim, it just never matches.
	re *regexp.Regexp
}

// route maps a namespaced tool or prompt name back to its upstream and
// original name.
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
// connection. *upstream.Conn satisfies it directly for both stdio and HTTP
// transports — no type-assertions needed; tests provide fakes.
type Upstream interface {
	Initialize(ctx context.Context) (*mcp.InitializeResult, error)
	ListTools(ctx context.Context) ([]mcp.Tool, error)
	// ListResources / ListResourceTemplates fetch the upstream's resource
	// catalog and its parameterized templates (Round 5). Like ListPrompts, the
	// registry calls them only for upstreams that declared the resources
	// capability in initialize — never as a blind probe.
	ListResources(ctx context.Context) ([]mcp.Resource, error)
	ListResourceTemplates(ctx context.Context) ([]mcp.ResourceTemplate, error)
	// ListPrompts fetches the upstream's prompt catalog (Round 4). The registry
	// calls it only for upstreams that declared the prompts capability in
	// initialize — never as a blind probe.
	ListPrompts(ctx context.Context) ([]mcp.Prompt, error)
	// CallTool forwards one tools/call. meta is the client's optional `_meta`
	// object, proxied verbatim (nil when the client sent none).
	CallTool(ctx context.Context, name string, arguments, meta json.RawMessage) (*mcp.Message, error)
	// GetPrompt forwards one prompts/get with the upstream's ORIGINAL prompt
	// name (the registry resolves the namespace before calling).
	GetPrompt(ctx context.Context, name string, arguments json.RawMessage) (*mcp.Message, error)
	// ReadResource forwards one resources/read with the client's exact URI —
	// resources are never namespaced, so nothing is rewritten (Round 5).
	ReadResource(ctx context.Context, uri string) (*mcp.Message, error)
	// Complete forwards one completion/complete whose params the registry has
	// already prepared (ref.name rewritten to the upstream's original for a
	// prompt ref; everything else verbatim — Round 5).
	Complete(ctx context.Context, params json.RawMessage) (*mcp.Message, error)
	Close() error
	// Done reports the "process died" channel of an upstream backed by a
	// long-lived process (stdio); ok is false when there is no such process
	// to watch (HTTP), so a supervisor is simply not started for it.
	Done() (ch <-chan struct{}, ok bool)
	// StderrTail reports the most recent stderr lines of an upstream backed
	// by a child process (stdio) — the supervisor logs them when that process
	// crashes, so the operator sees WHY it died without debug logging. ok is
	// false when there is no process and hence no stderr (HTTP), like Done.
	StderrTail() (lines []string, ok bool)
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

	// version is the gateway's build version (from -ldflags in main), passed
	// down to every upstream connection so the initialize handshake reports
	// the real binary version as clientInfo.version instead of a hardcoded
	// literal. Set once in New, read-only afterwards.
	version string

	// autoRestart gates the per-upstream auto-restart supervisor goroutines
	// (Stage 7a); it is New's supervise parameter. serve runs with true;
	// `mcp-gate doctor` (Stage 8) runs with false so its single diagnostic pass
	// reports a flapping upstream instead of endlessly resurrecting it. This is
	// the ONLY behavioural difference — Start/launch/catalog work identically
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
	// only by Start/Reload/Close; everything they call underneath (launch,
	// merge, superviseUpstream, dropUpstream, ...) takes only the inner
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
	// prompts/promptRoute are the aggregated prompt catalog and its routing
	// table (Round 4) — the prompts twins of tools/toolRoute, mutated only by
	// mergeLocked/dropLocked under the same r.mu. rawPrompts keeps the
	// per-upstream prompt list so catalog rewrites that fetched no fresh
	// prompts (a tools-only re-list, a filter-only re-projection) can carry the
	// existing prompts over instead of losing them — the same role rawTools
	// plays for filter reloads. Prompts do not pass through config.ToolFilter:
	// the client-facing name is always "<upstream>__<original>".
	prompts     map[string]PromptDescriptor
	promptRoute map[string]route
	rawPrompts  map[string][]mcp.Prompt
	// resources/resourceRoute are the aggregated resource catalog and its
	// ownership table (Round 5), mutated only by mergeLocked/dropLocked under
	// the same r.mu. There is no namespacing and no rename for resources — the
	// client-facing URI is the upstream's own, so resourceRoute carries only
	// uri→owner (keep-first on a cross-upstream URI collision, Warn+skip).
	// resourceTemplates is the ordered template list resolveResourceOwner
	// matches against when the exact uri lookup misses (see
	// ResourceTemplateDescriptor for why it is a slice). rawResources/
	// rawTemplates keep the per-upstream lists so catalog rewrites that fetched
	// no fresh resources (a tools-only re-list, a filter-only re-projection)
	// carry the existing ones over — the same role rawPrompts plays.
	resources         map[string]ResourceDescriptor
	resourceRoute     map[string]string // uri → owning upstream name
	resourceTemplates []ResourceTemplateDescriptor
	rawResources      map[string][]mcp.Resource
	rawTemplates      map[string][]mcp.ResourceTemplate
	// handshakes holds the per-upstream slice of the initialize handshake the
	// gateway keeps after launch: the upstream's instructions (aggregated into
	// the gateway's own InitializeResult.Instructions) and its raw capabilities
	// object (consulted by HasUpstreamCapability when the transport builds the
	// gateway's capabilities). Guarded by the same r.mu as conns/tools —
	// written by mergeLocked, cleared by dropLocked, always in step with the
	// catalog.
	handshakes map[string]handshakeMeta
	// cachedTools is the sorted catalog slice Tools() hands out, rebuilt lazily
	// on the first read after a mutation: the catalog changes only at discrete
	// points (mergeLocked/dropLocked), while tools/list reads it on every client
	// request — copying and sorting the whole map each time was pure waste.
	// Guarded by r.mu like the maps it is derived from; cachedToolsValid is
	// flipped to false by every catalog mutation.
	cachedTools      []ToolDescriptor
	cachedToolsValid bool

	failMu sync.Mutex
	report []UpstreamStatus // per-upstream outcome of the FIRST bring-up pass, for StartReport (Stage 8) and failureSummary

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

	// notifMu guards notifSubs, the set of client-facing transports that want
	// upstream notifications forwarded VERBATIM (notifications/progress, Round
	// 2). It is deliberately its own small mutex — not subMu, relistMu or mu —
	// so forwardNotification (which runs on an upstream's reader goroutine)
	// never entangles with the catalog's or the catalog-changed channel's
	// critical sections. See SubscribeNotifications / forwardNotification.
	notifMu     sync.Mutex
	notifSubs   map[int]chan mcp.Message
	nextNotifID int

	// relistMu guards relistTimers, the per-upstream debounce timers for
	// tools/list_changed notifications (Stage 7b), and relistStates, the
	// per-upstream running/dirty flags that serialize the re-lists themselves.
	// A "noisy" upstream firing a burst of list_changed must not trigger a
	// re-list storm, so each notification (re)arms a short timer and only its
	// expiry runs the re-list — and because Reset on an already-fired AfterFunc
	// is fundamentally racy (the callback may be mid-flight), two timer expiries
	// CAN overlap for one upstream; runRelist collapses them into "one running
	// re-list plus at most one queued re-run" so a stale ListTools result can
	// never overwrite a fresher one (the final re-list always starts after all
	// earlier ones finished).
	relistMu     sync.Mutex
	relistTimers map[string]*time.Timer
	relistStates map[string]*relistState
	relistClosed bool // set by Close (under relistMu) before it waits on relistWG below

	// relistWG counts in-flight runRelist goroutines so Close can wait for
	// them: a runRelist may be mid-ListTools against a connection Close is
	// about to tear down, and without the wait its "re-list failed" warning
	// could land AFTER shutdown reported completion. Registration happens
	// under relistMu gated by relistClosed — a bare Add racing Wait from a
	// zero counter is documented WaitGroup misuse (a debounce timer that
	// fired just before Close stopped it could otherwise Add after Wait
	// started); the flag makes "no new registrations" and "start waiting"
	// a single ordered handoff.
	relistWG sync.WaitGroup

	// limiterMu guards limiters and sems, the per-upstream call-limit state
	// (Round 6): token-bucket rate limiters and concurrency semaphores, created
	// lazily on the first guarded call to each upstream. Deliberately its OWN
	// mutex, not r.mu — the limiters have no relationship with the catalog's
	// critical section, and coupling them would make every rate-limited call
	// contend with catalog reads/writes. Each entry remembers the config values
	// it was built from; when a reload changes them, the next call detects the
	// mismatch under this mutex and swaps in a fresh limiter/semaphore (an old
	// semaphore still held by in-flight calls simply drains as they finish —
	// the new one starts life with zero permits held).
	limiterMu sync.Mutex
	limiters  map[string]*limiterEntry
	sems      map[string]*semEntry

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

// relistCloseWait bounds how long Close waits for in-flight re-lists to
// finish (same bounded-wait pattern as closeGracePeriod in upstream/stdio.go).
// By the time Close waits, procCtx is already cancelled, so a running
// relistUpstream's ListTools (its ctx derives from procCtx) aborts almost
// immediately — this timeout only guards against a pathologically stuck one,
// which must not hang gateway shutdown forever.
const relistCloseWait = 5 * time.Second

// relistState serializes re-lists for one upstream (guarded by relistMu):
// running is true while a runRelist call is executing the ListTools/install
// cycle for this upstream; dirty queues exactly one re-run for expiries that
// arrived meanwhile. See the relistMu field comment for why overlapping timer
// expiries are possible at all.
type relistState struct {
	running bool
	dirty   bool
}

// handshakeMeta is what the registry retains from one upstream's
// InitializeResult beyond the handshake itself: its human-readable
// instructions and its raw capabilities object. Everything else
// (protocolVersion, serverInfo) is consumed at launch time and not needed
// later.
type handshakeMeta struct {
	instructions string
	capabilities json.RawMessage
}

// auxCatalog bundles the AUXILIARY catalog slices launch fetches alongside the
// tools: prompts (Round 4), resources and resource templates (Round 5). They
// travel together because every path that carries one carries them all —
// launch fetches them best-effort, merge installs them, installLocked's
// carry-over preserves them as a unit. Tools stay a separate parameter: they
// are the catalog the gateway exists for (a tools/list failure is fatal to the
// launch; a failure here merely degrades).
type auxCatalog struct {
	prompts   []mcp.Prompt
	resources []mcp.Resource
	templates []mcp.ResourceTemplate
}

// New constructs a Registry from config. It does not start upstreams yet — call
// Start. callLog may be nil, in which case tool calls are not audited.
// payloadLog is the opt-in payload debug log (Stage 10); pass
// logging.NewPayloadLog("") for the no-op when payload logging is not wanted
// (doctor, tests) — it must not be nil. supervise=false disables the
// auto-restart supervisors entirely (see the field comment) — used by
// `mcp-gate doctor`, which wants exactly one pass. version is the gateway's
// build version, reported to upstreams as clientInfo.version in the handshake.
func New(cfg *config.Config, logger *slog.Logger, callLog logging.CallLog, payloadLog logging.PayloadLog, supervise bool, version string) *Registry {
	procCtx, procCancel := context.WithCancel(context.Background())
	r := &Registry{
		log:           logger,
		callLog:       callLog,
		payloadLog:    payloadLog,
		autoRestart:   supervise,
		version:       version,
		conns:         map[string]Upstream{},
		tools:         map[string]ToolDescriptor{},
		toolRoute:     map[string]route{},
		rawTools:      map[string][]mcp.Tool{},
		prompts:       map[string]PromptDescriptor{},
		promptRoute:   map[string]route{},
		rawPrompts:    map[string][]mcp.Prompt{},
		resources:     map[string]ResourceDescriptor{},
		resourceRoute: map[string]string{},
		rawResources:  map[string][]mcp.Resource{},
		rawTemplates:  map[string][]mcp.ResourceTemplate{},
		handshakes:    map[string]handshakeMeta{},
		subscribers:   map[int]chan struct{}{},
		notifSubs:     map[int]chan mcp.Message{},
		relistTimers:  map[string]*time.Timer{},
		relistStates:  map[string]*relistState{},
		limiters:      map[string]*limiterEntry{},
		sems:          map[string]*semEntry{},
		supCancel:     map[string]context.CancelFunc{},
		procCtx:       procCtx,
		procCancel:    procCancel,
	}
	r.cfg.Store(cfg)
	r.start = r.startUpstream
	return r
}

// config returns the current configuration snapshot. It never returns nil after
// New. Callers get a consistent pointer even while Reload swaps the config.
func (r *Registry) config() *config.Config { return r.cfg.Load() }

// ConfigSnapshot returns the current config, safe to call from any goroutine.
// It exists so other components (the HTTP transport) can read live config
// values (e.g. after a SIGHUP reload) without duplicating the atomic-pointer
// plumbing Registry already has.
func (r *Registry) ConfigSnapshot() *config.Config { return r.config() }

// startUpstream is the production starter: it dispatches to the stdio or HTTP
// implementation based on the upstream's resolved kind. Both return an
// *upstream.Conn, so the registry treats them uniformly from here on
// (docs/MCP_NOTES.md §8).
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
	onNotify := func(method string, params json.RawMessage) { r.onUpstreamNotification(name, method, params) }
	return upstream.StartStdio(ctx, r.log, u.Name, u.Command, u.Args, env, r.version, onNotify)
}

// startHTTP builds an HTTP (Streamable HTTP) upstream connection. Unlike
// startStdio it does no network I/O here — the handshake runs in Initialize, so
// StartHTTP never fails and a genuinely unreachable HTTP upstream is isolated at
// the Initialize step in launch, exactly like a stdio upstream that fails its
// handshake.
func (r *Registry) startHTTP(u config.Upstream) (Upstream, error) {
	return upstream.StartHTTP(r.log, u.Name, u.URL, u.Headers, nil, r.version), nil
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

	// Enabled upstreams in CONFIG order: the sequential apply pass below walks
	// this slice, so the keep-first winner of a runtime client-facing-name
	// collision is always the upstream listed first in the config.
	var enabled []config.Upstream
	for _, u := range r.config().Upstreams {
		if u.Enabled {
			enabled = append(enabled, u)
		}
	}

	// Launches fan out in parallel (one slow handshake must not serialize the
	// whole bring-up), but each goroutine ONLY launches and writes its outcome
	// into its own pre-allocated slot — the catalog MERGE runs sequentially
	// after g.Wait(), in config order. Merging from inside the goroutines made
	// the keep-first winner of a client-facing-name collision depend on
	// goroutine completion order, nondeterministic across runs of an identical
	// config (the same fix Reload's changed/added pass received earlier).
	type launchOutcome struct {
		conn  Upstream
		tools []mcp.Tool
		aux   auxCatalog
		meta  handshakeMeta
		err   error
	}
	results := make([]launchOutcome, len(enabled))
	g, gctx := errgroup.WithContext(ctx)
	for i, u := range enabled {
		i, u := i, u
		g.Go(func() error {
			conn, tools, aux, meta, err := r.launch(gctx, u)
			results[i] = launchOutcome{conn: conn, tools: tools, aux: aux, meta: meta, err: err}
			return nil // errors are isolated per-upstream, never propagated
		})
	}
	// Wait never returns an error because the goroutines swallow them into
	// results; keep the check for correctness if that ever changes.
	if err := g.Wait(); err != nil {
		return err
	}

	// Sequential apply in config order (see above). Failures are isolated:
	// logged, recorded for the failure summary / doctor report (Stage 8), the
	// upstream skipped. Successes merge the catalog and start the auto-restart
	// supervisor (Stage 7a) for stdio upstreams. This runs BEFORE the ctx.Err()
	// check so every successfully launched connection lands in r.conns and a
	// subsequent Close can tear it down — same as when merges ran inside the
	// goroutines.
	for i, u := range enabled {
		res := results[i]
		if res.err != nil {
			r.log.Warn("upstream failed to come up", "upstream", u.Name, "err", res.err)
			r.recordFailure(u.Name, res.err.Error())
			continue
		}
		n := r.merge(u.Name, res.conn, res.tools, res.aux, res.meta)
		r.recordSuccess(u.Name, n)
		r.superviseUpstream(u, res.conn)
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
	r.log.Info("registry ready", "upstreams", r.upstreamCount(), "tools", r.ToolCount())
	r.phase = phaseRunning // Reload is admissible from here on (lifecycleMu held).
	return nil
}

// launch starts one upstream and runs the full handshake sequence
// (start → Initialize → ListTools → best-effort ListPrompts/ListResources/
// ListResourceTemplates), returning the live connection, its tool catalog, the
// auxiliary catalogs (prompts/resources/templates) and the retained handshake
// metadata (instructions/capabilities from the InitializeResult).
// It is the single reusable "bring an upstream to a usable state" primitive
// shared by the first start (Start), the auto-restart supervisor (Stage 7a)
// and hot-reload (Stage 7d). On any failure it tears the connection back down
// and returns a single error whose message names the failing phase — the
// caller decides whether to record it as a start-time failure, retry it, or
// log it.
//
// The child process is launched under r.procCtx (long-lived, see procCtx);
// ctx bounds only the handshake RPCs so a slow upstream cannot block Start (or
// a restart) indefinitely.
func (r *Registry) launch(ctx context.Context, u config.Upstream) (Upstream, []mcp.Tool, auxCatalog, handshakeMeta, error) {
	conn, err := r.start(r.procCtx, u)
	if err != nil {
		return nil, nil, auxCatalog{}, handshakeMeta{}, fmt.Errorf("failed to start: %w", err)
	}

	// Upstream→registry notifications (Stage 7b) are wired inside startStdio —
	// the callback rides into StartStdio itself, so it is set before the reader
	// goroutine starts and a list_changed arriving immediately is not missed.

	var info *mcp.InitializeResult
	if err := r.withCallTimeoutFor(ctx, u.Name, func(ctx context.Context) error {
		var err error
		info, err = conn.Initialize(ctx)
		return err
	}); err != nil {
		_ = conn.Close()
		return nil, nil, auxCatalog{}, handshakeMeta{}, fmt.Errorf("handshake failed: %w", err)
	}
	r.log.Info("upstream initialized", "upstream", u.Name, "server", info.ServerInfo.Name)
	meta := handshakeMeta{instructions: info.Instructions, capabilities: info.Capabilities}

	var tools []mcp.Tool
	if err := r.withCallTimeoutFor(ctx, u.Name, func(ctx context.Context) error {
		var err error
		tools, err = conn.ListTools(ctx)
		return err
	}); err != nil {
		_ = conn.Close()
		return nil, nil, auxCatalog{}, handshakeMeta{}, fmt.Errorf("tools/list failed: %w", err)
	}

	// Prompts (Round 4): fetched only when the upstream DECLARED the prompts
	// capability in its initialize response — an upstream without it need not
	// even recognize prompts/list, so probing blindly would provoke pointless
	// method-not-found errors. Best-effort, unlike tools/list above: a failing
	// prompts/list degrades this upstream to "no prompts" (Warn) instead of
	// failing the whole launch — prompts are auxiliary next to the tool catalog
	// the gateway exists for.
	var aux auxCatalog
	if hasCapability(info.Capabilities, "prompts") {
		if err := r.withCallTimeoutFor(ctx, u.Name, func(ctx context.Context) error {
			var err error
			aux.prompts, err = conn.ListPrompts(ctx)
			return err
		}); err != nil {
			r.log.Warn("prompts/list failed, continuing without prompts", "upstream", u.Name, "err", err)
			aux.prompts = nil
		}
	}

	// Resources + templates (Round 5): same policy as prompts — fetched only
	// when the upstream declared the resources capability, best-effort. A
	// failing resources/list degrades this upstream to "no resources"; a
	// failing templates/list only loses the templates (the plain resources
	// already fetched stay — the two lists are independent per the spec, and
	// ListResourceTemplates itself already maps method-not-found to empty for
	// upstreams that never implemented the sub-method).
	if hasCapability(info.Capabilities, "resources") {
		if err := r.withCallTimeoutFor(ctx, u.Name, func(ctx context.Context) error {
			var err error
			aux.resources, err = conn.ListResources(ctx)
			return err
		}); err != nil {
			r.log.Warn("resources/list failed, continuing without resources", "upstream", u.Name, "err", err)
			aux.resources = nil
		}
		if err := r.withCallTimeoutFor(ctx, u.Name, func(ctx context.Context) error {
			var err error
			aux.templates, err = conn.ListResourceTemplates(ctx)
			return err
		}); err != nil {
			r.log.Warn("resources/templates/list failed, continuing without templates", "upstream", u.Name, "err", err)
			aux.templates = nil
		}
	}

	return conn, tools, aux, meta, nil
}

// withCallTimeoutFor runs fn under a child context bounded by the CURRENT
// config's call timeout for the named upstream — the one ritual every upstream
// RPC the registry makes shares (handshake, list, forwarded call). The timeout
// is re-read from the live config on every use, so a reload takes effect
// immediately; the per-upstream call_timeout override (Round 6) falls back to
// the global EffectiveCallTimeout when unset.
func (r *Registry) withCallTimeoutFor(parent context.Context, upstream string, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, r.config().EffectiveCallTimeoutFor(upstream))
	defer cancel()
	return fn(ctx)
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
	done, ok := conn.Done()
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
// forever, since nothing else holds a reference to it once replaceUpstreamIfLive
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
			// double-close via a different goroutine, which the stdio Close's
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
			// Close waited for the stderr drain goroutine, so the tail is
			// complete: surface the crashed process's last stderr lines in ONE
			// Warn block — the post-mortem an operator otherwise only gets by
			// re-running at debug level and reproducing the crash.
			if tail, ok := conn.StderrTail(); ok && len(tail) > 0 {
				r.log.Warn("stdio upstream stderr before exit",
					"upstream", u.Name, "lines", len(tail), "stderr_tail", strings.Join(tail, "\n"))
			}
			r.log.Warn("stdio upstream exited, attempting restart", "upstream", u.Name)
			newConn, newDone, ok := r.restart(u, conn, supCtx)
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
// forever after a reload disabled auto-restart). Giving up — whether by
// exhausted attempts or by a reload disabling restart mid-backoff — DROPS the
// upstream from the catalog: by this point its old connection is already
// closed (supervise reaped the crash before calling restart), so leaving the
// catalog entry would advertise tools whose every call fails with a transport
// error instead of an honest "unknown tool" (security-audit finding). The drop
// is identity-gated on the dead connection (dropUpstreamIfCurrent): the catalog
// entry under this name may by now be a FRESH connection installed by another
// path (e.g. a reload that judged the upstream "unchanged" from its live
// snapshot), and dropping by name alone would tear that innocent newcomer down
// with it. The disabled→enabled transition for an upstream whose supervisor was
// never started (Enabled=false at Start time) is out of scope — it would need a
// separate mechanism to start a supervisor post-hoc.
//
// dead is the connection whose crash brought us here — already Closed by
// supervise, and the identity the give-up branches gate their drop on.
func (r *Registry) restart(u config.Upstream, dead Upstream, supCtx context.Context) (Upstream, <-chan struct{}, bool) {
	for attempt := 1; ; attempt++ {
		policy := r.config().EffectiveRestart()
		if policy.Enabled == nil || !*policy.Enabled {
			// Symmetric with the exhausted-attempts branch below: the dead
			// connection is already closed, so its catalog entry must go too.
			if r.dropUpstreamIfCurrent(u.Name, dead) {
				r.log.Info("stdio upstream restart disabled by reload, dropped from catalog", "upstream", u.Name)
				r.notifyCatalogChanged()
			} else {
				r.log.Info("stdio upstream entry already replaced, leaving it", "upstream", u.Name)
			}
			return nil, nil, false
		}
		if policy.MaxAttempts != 0 && attempt > policy.MaxAttempts {
			if r.dropUpstreamIfCurrent(u.Name, dead) {
				r.log.Error("stdio upstream exhausted restart attempts, dropping from catalog",
					"upstream", u.Name, "max_attempts", policy.MaxAttempts)
				r.notifyCatalogChanged()
			} else {
				r.log.Info("stdio upstream entry already replaced, leaving it", "upstream", u.Name)
			}
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

		conn, tools, aux, meta, err := r.launch(r.procCtx, u)
		if err != nil {
			r.log.Warn("stdio upstream restart attempt failed",
				"upstream", u.Name, "attempt", attempt, "err", err)
			continue
		}

		newDone, ok := conn.Done()
		if !ok {
			// Should not happen: a relaunched stdio upstream always has a live
			// process, so its Done must report ok=true. Guard anyway so a future
			// non-stdio path cannot silently spin.
			_ = conn.Close()
			r.log.Error("restarted upstream has no done channel; giving up", "upstream", u.Name)
			return nil, nil, false
		}
		if !r.replaceUpstreamIfLive(u.Name, conn, tools, aux, meta, supCtx) {
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
// (Stage 7b, Round 2). It runs on that upstream's single reader goroutine, so
// it must not block or re-enter the connection: for tools/list_changed it only
// (re)arms a debounce timer whose expiry does the actual re-list on a fresh
// goroutine; for notifications/progress it forwards the params VERBATIM to the
// notification subscribers via a non-blocking send (the progressToken was
// minted by the client, the gateway never rewrites it). Other notification
// methods are ignored (prompt/resource list_changed subscriptions are still
// TODO — see buildCapabilities in transport).
func (r *Registry) onUpstreamNotification(name, method string, params json.RawMessage) {
	if method == mcp.NotifProgress {
		r.forwardNotification(mcp.Message{Method: method, Params: params})
		return
	}
	if method != mcp.NotifToolsListChanged {
		return
	}
	r.relistMu.Lock()
	defer r.relistMu.Unlock()
	if t, ok := r.relistTimers[name]; ok {
		t.Reset(relistDebounce)
		return
	}
	// AfterFunc fires on its own goroutine, so runRelist (which does blocking
	// RPCs) never runs on the reader goroutine.
	r.relistTimers[name] = time.AfterFunc(relistDebounce, func() {
		r.relistMu.Lock()
		delete(r.relistTimers, name)
		r.relistMu.Unlock()
		r.runRelist(name)
	})
}

// runRelist serializes relistUpstream per upstream. The t.Reset above cannot
// prevent overlap: resetting an AfterFunc whose callback is ALREADY mid-flight
// is inherently racy in Go, so a second expiry can start while a first re-list
// still has its ListTools RPC pending — and although replaceUpstreamIfCurrent
// gates the write on connection identity, it cannot tell a stale result from a
// fresh one on the SAME connection. So instead of fixing the timer, the
// overlap is made harmless here: if a re-list for this upstream is already
// running, only mark it dirty and leave; the running call re-runs the re-list
// once more after it finishes (checking shutdown first). The final ListTools
// therefore always STARTS after every earlier one returned — its result is the
// freshest and lands last. The RPC itself (relistUpstream) runs OUTSIDE
// relistMu; only the flag flips are under the lock.
func (r *Registry) runRelist(name string) {
	r.relistMu.Lock()
	if r.relistClosed {
		// Close is already waiting on relistWG (or done waiting) — registering
		// now would be Add-vs-Wait misuse, and the re-list would be pointless
		// anyway (procCtx is cancelled first thing in Close).
		r.relistMu.Unlock()
		return
	}
	r.relistWG.Add(1)
	defer r.relistWG.Done()
	st, ok := r.relistStates[name]
	if !ok {
		st = &relistState{}
		r.relistStates[name] = st
	}
	if st.running {
		st.dirty = true
		r.relistMu.Unlock()
		return
	}
	st.running = true
	r.relistMu.Unlock()

	for {
		r.relistUpstream(name)

		r.relistMu.Lock()
		if st.dirty && r.procCtx.Err() == nil {
			st.dirty = false
			r.relistMu.Unlock()
			continue // an expiry landed while we were re-listing: go once more.
		}
		st.running = false
		delete(r.relistStates, name) // clean exit: drop the entry so removed upstreams do not accumulate state.
		r.relistMu.Unlock()
		return
	}
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

	var tools []mcp.Tool
	if err := r.withCallTimeoutFor(r.procCtx, name, func(ctx context.Context) error {
		var err error
		tools, err = conn.ListTools(ctx)
		return err
	}); err != nil {
		r.log.Warn("re-list after upstream list_changed failed", "upstream", name, "err", err)
		return
	}
	if !r.replaceUpstreamIfCurrent(name, conn, tools) {
		r.log.Info("stale re-list discarded, upstream replaced or removed meanwhile", "upstream", name)
		return
	}
	r.notifyCatalogChanged()
}

// merge namespaces an upstream's tools and prompts and adds them — plus its
// resources and resource templates (Round 5) — to the aggregated catalog and
// routing tables under the registry lock, recording the handshake metadata
// launch retained alongside. It returns the number of tools actually projected
// into the catalog (post-filter, post-dedup).
func (r *Registry) merge(name string, conn Upstream, tools []mcp.Tool, aux auxCatalog, meta handshakeMeta) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.mergeLocked(name, conn, tools, aux, meta)
	r.log.Debug("upstream catalog merged", "upstream", name, "tools", n,
		"prompts", len(aux.prompts), "resources", len(aux.resources), "resource_templates", len(aux.templates))
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
// without a rename get the default namespaced "<upstream>__<original>"), then
// the token-optimization rules — strip annotations/outputSchema, override the
// description (Describe, keyed by ORIGINAL name like Rename), truncate the
// description to MaxDescription runes (never applied on top of a Describe
// override: the override is the config author's final word).
// It is pure — no registry state, no side effects (mcp.Tool is modified as a
// copy, never the stored raw list) — so the projection can be re-run at any
// time against the stored raw list (filter-only reload).
func filterAndRenameTools(upstream string, tools []mcp.Tool, f config.ToolFilter) []toolEntry {
	allow := f.AllowSet()
	deny := f.DenySet()

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
		if f.StripAnnotations {
			t.Annotations = nil
		}
		if f.StripOutputSchema {
			t.OutputSchema = nil
		}
		if desc := f.Describe[t.Name]; desc != "" {
			t.Description = mustJSONString(desc)
		} else if f.MaxDescription > 0 {
			t.Description = truncateJSONString(t.Description, f.MaxDescription)
		}
		out = append(out, toolEntry{name: name, tool: t})
	}
	return out
}

// mustJSONString encodes a plain string as a JSON string literal.
// json.Marshal of a string cannot fail, hence "must".
func mustJSONString(s string) json.RawMessage {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err) // unreachable: a Go string always marshals
	}
	return b
}

// truncateJSONString truncates the string carried in a JSON string literal to
// at most max runes (never bytes — a multi-byte UTF-8 sequence is not cut in
// the middle), appending "…" when truncation happened. A raw value that is
// absent, short enough, or not a JSON string (an upstream is free to send
// anything — the gateway proxies verbatim) is returned unchanged.
func truncateJSONString(raw json.RawMessage, max int) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return raw
	}
	runes := []rune(s)
	if len(runes) <= max {
		return raw
	}
	return mustJSONString(string(runes[:max]) + "…")
}

// filterFor looks up the CURRENT tool filter for an upstream by name from the
// atomic config snapshot — deliberately not passed as a parameter: several
// paths into mergeLocked (relistUpstream, a supervisor holding the
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
// installLocked, assuming r.mu is already held. It records the connection and
// the raw (pre-filter) tool list, then projects the tools through the
// upstream's current filter (Stage 9) into the catalog/routing table, skipping
// a duplicate client-facing name (keep first, log). Cross-upstream rename
// collisions are rejected by config.Validate for every name the config knows
// statically; this keep-first only guards runtime surprises (an upstream
// advertising a name that happens to match another's projection).
// It returns the number of entries actually added to the catalog by this call
// (post-filter, post-dedup) — the count the client really sees, which callers
// report to diagnostics (UpstreamStatus.Tools → doctor) and logs.
func (r *Registry) mergeLocked(name string, conn Upstream, tools []mcp.Tool, aux auxCatalog, meta handshakeMeta) int {
	r.cachedToolsValid = false
	r.conns[name] = conn
	r.rawTools[name] = tools
	r.rawPrompts[name] = aux.prompts
	r.rawResources[name] = aux.resources
	r.rawTemplates[name] = aux.templates
	r.handshakes[name] = meta
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
	// Prompts (Round 4): no filter/rename mechanism exists for prompts, so the
	// client-facing name is always the plain "<upstream>__<original>". The
	// namespacing makes cross-upstream collisions impossible for well-formed
	// names; keep-first only guards runtime surprises (an upstream whose own
	// name or prompt names smuggle the separator, or duplicate prompt names
	// within one upstream) — same policy as tools above.
	for _, p := range aux.prompts {
		ns := name + NameSeparator + p.Name
		if _, dup := r.prompts[ns]; dup {
			r.log.Warn("duplicate client-facing prompt name skipped", "name", ns, "upstream", name)
			continue
		}
		r.prompts[ns] = PromptDescriptor{Name: ns, Upstream: name, Prompt: p}
		r.promptRoute[ns] = route{upstream: name, original: p.Name}
	}
	// Resources (Round 5): no namespacing at all — a URI is the upstream's own
	// identifier and rewriting it would break every relative reference inside
	// the resource contents. Cross-upstream URI collisions are therefore
	// possible and resolved keep-first (config order, same policy as tools and
	// prompts): the first upstream to claim a URI owns it, the loser is skipped
	// with a Warn.
	for _, res := range aux.resources {
		if _, dup := r.resources[res.URI]; dup {
			r.log.Warn("duplicate resource uri skipped", "uri", res.URI, "upstream", name)
			continue
		}
		r.resources[res.URI] = ResourceDescriptor{URI: res.URI, Upstream: name, Resource: res}
		r.resourceRoute[res.URI] = name
	}
	// Resource templates (Round 5): appended in merge order — Start/Reload
	// merge upstreams sequentially in config order, so template matching
	// (first match wins, see resolveResourceOwner) is deterministic. A
	// template whose {var} pattern fails to compile is still listed to the
	// client verbatim; it just never matches a read (re stays nil).
	for _, tpl := range aux.templates {
		re, err := templateRegexp(tpl.URITemplate)
		if err != nil {
			r.log.Warn("unusable resource uri template, listed but never matched",
				"template", tpl.URITemplate, "upstream", name, "err", err)
		}
		r.resourceTemplates = append(r.resourceTemplates, ResourceTemplateDescriptor{
			URITemplate: tpl.URITemplate, Upstream: name, Template: tpl, re: re,
		})
	}
	return n
}

// templateRegexp translates an RFC 6570 level-1 URI template into an anchored
// regexp: literal runs are quoted verbatim (dots, pluses and other regexp
// metacharacters in the fixed part must match themselves), each "{var}" —
// including operator forms like "{+var}", treated the same at this level —
// matches one non-empty path segment ([^/]+). An unclosed "{" is not a
// template expression and stays literal.
func templateRegexp(tmpl string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	for {
		open := strings.Index(tmpl, "{")
		if open < 0 {
			b.WriteString(regexp.QuoteMeta(tmpl))
			break
		}
		end := strings.Index(tmpl[open:], "}")
		if end < 0 {
			b.WriteString(regexp.QuoteMeta(tmpl)) // unclosed brace: literal tail
			break
		}
		b.WriteString(regexp.QuoteMeta(tmpl[:open]))
		b.WriteString("[^/]+")
		tmpl = tmpl[open+end+1:]
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
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

// Gated catalog mutations. dropUpstreamIfCurrent, replaceUpstreamIfLive and
// replaceUpstreamIfCurrent below share a shape — take r.mu, check a gate,
// mutate — but are deliberately NOT collapsed into one closure-parameterized
// CAS helper: the gates differ in KIND (connection identity for the two
// *IfCurrent functions vs supervisor-context liveness for *IfLive, which has
// no old connection to compare) and the mutations differ (drop vs install),
// so a generic helper would need both passed as closures, hiding exactly the
// thing each call site must be explicit about — which race its gate closes.
// The genuinely shared parts are already factored (dropLocked/installLocked).

// dropUpstreamIfCurrent is dropUpstream with an identity gate, the removal
// counterpart of replaceUpstreamIfCurrent: it removes name's catalog entry
// ONLY if the live connection is still conn (pointer identity — all Upstream
// implementations are pointers, so == is identity), under ONE hold of r.mu so
// the check and the drop are a single atomic step. Returns false — leaving the
// catalog untouched — when r.conns[name] is already a different (fresh)
// connection installed by another path; used by restart()'s give-up branches,
// which must not tear down a newcomer that merely reused the name.
//
// It deliberately takes only r.mu, never lifecycleMu: Close holds lifecycleMu
// while waiting for the supervisors to unwind, and this runs on a supervisor's
// give-up path — taking lifecycleMu here would deadlock shutdown.
func (r *Registry) dropUpstreamIfCurrent(name string, conn Upstream) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conns[name] != conn {
		return false
	}
	r.dropLocked(name)
	return true
}

// dropLocked is dropUpstream's body, assuming r.mu is already held. It exists so
// installLocked can drop-then-merge under a SINGLE lock acquisition (below).
func (r *Registry) dropLocked(name string) {
	r.cachedToolsValid = false
	delete(r.conns, name)
	delete(r.rawTools, name)
	delete(r.rawPrompts, name)
	delete(r.rawResources, name)
	delete(r.rawTemplates, name)
	delete(r.handshakes, name)
	for ns, d := range r.tools {
		if d.Upstream == name {
			delete(r.tools, ns)
			delete(r.toolRoute, ns)
		}
	}
	for ns, d := range r.prompts {
		if d.Upstream == name {
			delete(r.prompts, ns)
			delete(r.promptRoute, ns)
		}
	}
	for uri, d := range r.resources {
		if d.Upstream == name {
			delete(r.resources, uri)
			delete(r.resourceRoute, uri)
		}
	}
	// Filter the template slice in place, preserving the merge order of the
	// surviving upstreams' templates (matching stays deterministic).
	kept := r.resourceTemplates[:0]
	for _, d := range r.resourceTemplates {
		if d.Upstream != name {
			kept = append(kept, d)
		}
	}
	r.resourceTemplates = kept
}

// installLocked replaces name's catalog entry with (conn, tools), assuming
// r.mu is already held — the common tail of replaceUpstreamIfLive,
// replaceUpstreamIfCurrent, and remergeUpstream, once each has decided under
// its own gate that the write is safe to make.
//
// meta is the handshake metadata to record for the fresh entry; nil means
// "this path ran no new handshake" (re-list, filter-only re-projection — the
// connection is unchanged), so the currently recorded metadata is carried
// over instead of being wiped by dropLocked. aux follows the same nil
// convention: nil means "this path fetched no fresh prompts/resources/
// templates" (a tools/list_changed re-list refreshes only tools; a filter
// re-projection touches nothing upstream), so the currently recorded auxiliary
// catalogs are carried over as a unit.
func (r *Registry) installLocked(name string, conn Upstream, tools []mcp.Tool, aux *auxCatalog, meta *handshakeMeta, logMsg string) {
	if meta == nil {
		m := r.handshakes[name] // zero value when absent — nothing to preserve
		meta = &m
	}
	if aux == nil {
		aux = &auxCatalog{ // nil slices when absent — nothing to preserve
			prompts:   r.rawPrompts[name],
			resources: r.rawResources[name],
			templates: r.rawTemplates[name],
		}
	}
	r.dropLocked(name)
	n := r.mergeLocked(name, conn, tools, *aux, *meta)
	r.log.Debug(logMsg, "upstream", name, "tools", n)
}

// replaceUpstreamIfLive atomically swaps out an upstream's connection and
// catalog — old entries dropped, new ones merged under a single hold of r.mu,
// so the client never sees the upstream's tools vanish and reappear — with a
// liveness gate for the auto-restart supervisor: the supCtx check and the
// catalog write happen under that ONE hold, so "was I retired while
// launching?" and "install my fresh connection" are a single atomic step. Without that atomicity a restart that
// passed an earlier check could still install its connection after a reload's
// retireAndClose+dropUpstream ran, resurrecting an upstream the reload just
// removed (the supervisor-vs-reload race found by independent review). Returns
// false — leaving the catalog untouched — when supCtx was already cancelled;
// the caller then owns closing the never-installed connection.
func (r *Registry) replaceUpstreamIfLive(name string, conn Upstream, tools []mcp.Tool, aux auxCatalog, meta handshakeMeta, supCtx context.Context) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if supCtx.Err() != nil {
		return false
	}
	r.installLocked(name, conn, tools, &aux, &meta, "upstream catalog replaced")
	return true
}

// replaceUpstreamIfCurrent is the same atomic swap with a currency gate for
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
	// nil meta/aux: a re-list runs no new handshake and refreshes only the
	// tools — keep the recorded handshake metadata and the prompt/resource/
	// template lists.
	r.installLocked(name, oldConn, tools, nil, nil, "upstream catalog refreshed after list_changed")
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

// notifSubBuffer is the per-subscriber buffer of the SubscribeNotifications
// channel. Unlike the catalog-changed channel (buffer 1, coalescing — "did
// anything change" is one bit), forwarded notifications each carry a distinct
// payload, so a burst is buffered up to this depth and only then dropped
// (progress updates are advisory; losing one under backpressure is harmless).
const notifSubBuffer = 16

// SubscribeNotifications registers interest in upstream notifications the
// gateway forwards VERBATIM to its client — today only notifications/progress
// (Round 2) — and returns a channel of whole mcp.Message values (method +
// params, no id) plus an unsubscribe function the caller MUST call when it
// stops listening. Unlike Subscribe's coalescing one-bit signal, each message
// matters individually, so the channel is buffered (notifSubBuffer); delivery
// is NON-BLOCKING — a subscriber that stops draining loses messages (Debug-
// logged), never stalls the upstream reader goroutine publishing them.
func (r *Registry) SubscribeNotifications() (<-chan mcp.Message, func()) {
	ch := make(chan mcp.Message, notifSubBuffer)
	r.notifMu.Lock()
	id := r.nextNotifID
	r.nextNotifID++
	r.notifSubs[id] = ch
	r.notifMu.Unlock()

	return ch, func() {
		r.notifMu.Lock()
		delete(r.notifSubs, id)
		r.notifMu.Unlock()
	}
}

// forwardNotification hands one upstream notification to every notification
// subscriber. It runs on an upstream's single reader goroutine (via
// onUpstreamNotification), so the send is strictly non-blocking: a subscriber
// whose buffer is full has the message dropped — the reader must never stall
// on a slow client transport.
func (r *Registry) forwardNotification(msg mcp.Message) {
	r.notifMu.Lock()
	defer r.notifMu.Unlock()
	for _, ch := range r.notifSubs {
		select {
		case ch <- msg:
		default:
			r.log.Debug("notification subscriber buffer full, dropping", "method", msg.Method)
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
//     drop old catalog entry + launch new + merge + supervise;
//   - FILTER-ONLY (live, launch identical, tools filter differs — Stage 9):
//     re-project the stored raw tool list through the new filter. No Close, no
//     relaunch, no network re-list — the upstream's raw catalog did not change,
//     only its projection did, so a deny-list edit takes effect on SIGHUP
//     without any upstream downtime;
//   - UNCHANGED: left running untouched.
//
// Call-limit changes (rate_limit / max_concurrent / max_result_bytes /
// call_timeout, Round 6) never require a relaunch: SameLaunch deliberately
// excludes them, so an upstream whose ONLY change is a limit lands in
// UNCHANGED — the limits are re-read from the live config on every call
// (Effective*For via r.config()), and the cached limiter/semaphore instances
// are rebuilt lazily on the next call when their values differ (see
// limiterFor/semFor in limits.go). The same applies to the global limit knobs:
// swapping r.cfg below is all a reload needs to do for them.
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

	// Iterate newCfg.Upstreams (the slice), NOT the newEnabled map: the
	// changed/added slices must preserve CONFIG order, because the sequential
	// merge pass below resolves a runtime client-facing-name collision
	// keep-first — and "first" must deterministically mean "first in the
	// config", not whichever map key (or goroutine) came up first.
	for _, nu := range newCfg.Upstreams {
		if !nu.Enabled {
			continue
		}
		name := nu.Name
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
	//  2. changed + added — LAUNCH IN PARALLEL (errgroup, same pattern as
	//     Start): each involves a full launch/handshake, and one slow upstream
	//     must not serialize the whole reload. Errors stay isolated
	//     per-upstream (recorded, never propagated — g.Go always returns nil),
	//     matching Start. The catalog MERGE, however, runs SEQUENTIALLY after
	//     g.Wait(), in config order: merging from inside the goroutines made
	//     the keep-first winner of a runtime client-facing-name collision
	//     depend on goroutine completion order, not on the config (found by
	//     security audit). Each goroutine writes only its own pre-allocated
	//     results slot, so no lock is needed until the post-Wait pass reads
	//     them all;
	//  3. filterOnly — SEQUENTIALLY, LAST: a pure re-projection without I/O that
	//     must see the final catalog state after removed/changed/added.
	//
	// Parallel goroutines here are safe: lifecycleMu is held for the whole
	// Reload (Tier 1), so no Start/Close/other Reload can interleave; the
	// catalog itself is guarded by r.mu inside merge/dropUpstream.
	//
	// For removed/changed: retire the supervisor first (so the deliberate Close
	// is not auto-restarted), then Close the old connection, then IMMEDIATELY
	// drop the old catalog entry. For changed upstreams all three run INSIDE
	// the launch goroutine — early, in parallel — so the old entry never
	// lingers pointing at a closed connection while a slow SIBLING launch holds
	// up the sequential merge pass below: a client calling the already-closed
	// upstream mid-reload gets an honest "unknown tool" (same as the removed
	// path), not a transport error against a conn that no longer exists
	// (regression found by review of the deterministic-merge fix — deferring
	// the drop to the merge pass stretched that window from "this upstream's
	// own launch" to "the slowest launch in the whole batch"). Close/launch are
	// I/O — done outside the catalog lock (each of dropUpstream/merge takes the
	// lock itself, briefly).
	for _, name := range removed {
		r.retireAndClose(name)
		r.dropUpstream(name)
		r.log.Info("upstream removed by reload", "upstream", name)
	}
	// launchResult carries one changed/added upstream's launch outcome from its
	// goroutine to the sequential merge pass. Each goroutine owns exactly one
	// slot (indexed: changed first, then added), so the writes need no mutex;
	// the slice is only read after g.Wait().
	type launchResult struct {
		u       config.Upstream
		conn    Upstream
		tools   []mcp.Tool
		aux     auxCatalog
		meta    handshakeMeta
		err     error
		changed bool // true: a relaunched (changed) upstream, its old entry already dropped; false: a newly added one
	}
	results := make([]launchResult, len(changed)+len(added))
	var g errgroup.Group
	for i, u := range changed {
		i, u := i, u
		g.Go(func() error {
			// Retire + Close + drop together, up front: once the old connection
			// is closed its catalog entry must not outlive it (see the phase
			// comment above). The relaunched catalog merges after g.Wait().
			r.retireAndClose(u.Name)
			r.dropUpstream(u.Name)
			conn, tools, aux, meta, err := r.launch(ctx, u)
			results[i] = launchResult{u: u, conn: conn, tools: tools, aux: aux, meta: meta, err: err, changed: true}
			return nil
		})
	}
	for j, u := range added {
		idx, u := len(changed)+j, u
		g.Go(func() error {
			conn, tools, aux, meta, err := r.launch(ctx, u)
			results[idx] = launchResult{u: u, conn: conn, tools: tools, aux: aux, meta: meta, err: err}
			return nil
		})
	}
	_ = g.Wait() // errors are carried in results; Wait only joins the goroutines.

	// Sequential merge in config order (changed first, then added — each in the
	// order the config lists them): the keep-first resolution of a runtime
	// client-facing-name collision is deterministic again, exactly as it was
	// before the launches were parallelized.
	for _, res := range results {
		switch {
		case res.err != nil && res.changed:
			// The changed upstream failed to relaunch. Its old entry was already
			// dropped inside the goroutine (right after retireAndClose), so
			// there is nothing left to clean up — just record the loss.
			r.log.Warn("changed upstream failed to relaunch, dropped", "upstream", res.u.Name, "err", res.err)
		case res.err != nil:
			r.log.Warn("added upstream failed to launch", "upstream", res.u.Name, "err", res.err)
		case res.changed:
			// The old entry is long gone (dropped in the goroutine), so this is
			// a plain merge into an empty name — same call as the added case.
			r.merge(res.u.Name, res.conn, res.tools, res.aux, res.meta)
			r.superviseUpstream(res.u, res.conn)
			r.log.Info("upstream reconfigured by reload", "upstream", res.u.Name, "tools", len(res.tools))
		default:
			r.merge(res.u.Name, res.conn, res.tools, res.aux, res.meta)
			r.superviseUpstream(res.u, res.conn)
			r.log.Info("upstream added by reload", "upstream", res.u.Name, "tools", len(res.tools))
		}
	}
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
// a torn catalog, exactly like installLocked's other callers (Stage 9,
// filter-only reload).
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
	// nil meta/aux: a filter-only re-projection runs no new handshake and
	// touches nothing upstream — keep the recorded metadata and the prompt/
	// resource/template lists.
	r.installLocked(name, conn, tools, nil, nil, "upstream catalog re-projected")
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
// deterministic output. The slice is a cache shared between calls (rebuilt
// lazily after each catalog mutation) — callers must treat it as read-only.
func (r *Registry) Tools() []ToolDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.cachedToolsValid {
		out := make([]ToolDescriptor, 0, len(r.tools))
		for _, d := range r.tools {
			out = append(out, d)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		r.cachedTools = out
		r.cachedToolsValid = true
	}
	return r.cachedTools
}

// ToolCount reports the catalog size without materializing it — for "ready"
// log lines that only want the number.
func (r *Registry) ToolCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.tools)
}

// Prompts returns the aggregated, namespaced prompt catalog, sorted by name
// for deterministic output — Tools()'s prompts twin. Unlike Tools it builds a
// fresh slice on every call, deliberately without a cachedTools-style cache:
// prompt catalogs are typically tiny (units, not hundreds) and prompts/list is
// far rarer than tools/list, so the cache's invalidation bookkeeping would
// cost more in complexity than the copy costs in CPU. Revisit if either
// assumption breaks.
func (r *Registry) Prompts() []PromptDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]PromptDescriptor, 0, len(r.prompts))
	for _, d := range r.prompts {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// GetPrompt routes a namespaced prompts/get to its owning upstream, rewriting
// the name back to the upstream's original before forwarding — CallTool's
// prompts twin, including the sanitized error contract: a routing/transport
// failure is logged here with full detail and returned as a short message
// naming only the prompt the client itself asked for (dispatch.go forwards
// the text verbatim to the client). The returned *mcp.Message is the raw
// upstream response (which may itself carry a JSON-RPC error).
//
// TODO: retry once on upstream.ErrConnClosedBeforeSend against a re-resolved
// fresh connection, the way CallTool does — skipped in this round for
// minimality (prompts/get is read-only and far rarer than tools/call, so the
// race window matters much less; see CallTool for the pattern to copy).
func (r *Registry) GetPrompt(ctx context.Context, namespaced string, arguments json.RawMessage) (*mcp.Message, error) {
	r.mu.RLock()
	rt, ok := r.promptRoute[namespaced]
	conn := r.conns[rt.upstream]
	r.mu.RUnlock()

	if !ok || conn == nil {
		return nil, fmt.Errorf("unknown prompt %q", namespaced)
	}

	var resp *mcp.Message
	err := r.withCallTimeoutFor(ctx, rt.upstream, func(ctx context.Context) error {
		var err error
		resp, err = conn.GetPrompt(ctx, rt.original, arguments)
		return err
	})
	if err != nil {
		r.log.Warn("prompt get failed", "prompt", namespaced, "upstream", rt.upstream, "err", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("get prompt %q timed out", namespaced)
		}
		return nil, fmt.Errorf("get prompt %q failed", namespaced)
	}
	return resp, nil
}

// Resources returns the aggregated resource catalog, sorted by URI for
// deterministic output — Prompts()'s resources twin (Round 5), with the same
// deliberate no-cache choice (see Prompts). URIs are the upstreams' own,
// never namespaced.
func (r *Registry) Resources() []ResourceDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ResourceDescriptor, 0, len(r.resources))
	for _, d := range r.resources {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URI < out[j].URI })
	return out
}

// ResourceTemplates returns the aggregated resource-template catalog in MERGE
// order (config order at Start/Reload) — deliberately NOT sorted: this is the
// exact order resolveResourceOwner matches templates in, and the list the
// client sees should tell the truth about which template wins an overlap. The
// slice is a copy; mutating it does not affect the registry.
func (r *Registry) ResourceTemplates() []ResourceTemplateDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]ResourceTemplateDescriptor(nil), r.resourceTemplates...)
}

// resolveResourceOwner resolves which upstream owns a concrete resource URI:
// the exact catalog entry when one exists, otherwise the FIRST resource
// template (in merge order) whose pattern matches the URI. Shared by
// ReadResource and Complete (ref/resource), so both route identically.
// Assumes r.mu is NOT held.
func (r *Registry) resolveResourceOwner(uri string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if owner, ok := r.resourceRoute[uri]; ok {
		return owner, true
	}
	for _, d := range r.resourceTemplates {
		if d.re != nil && d.re.MatchString(uri) {
			return d.Upstream, true
		}
	}
	return "", false
}

// ReadResource routes a resources/read to the upstream owning the URI —
// GetPrompt's resources twin (Round 5), except nothing is rewritten: the URI
// travels to the upstream exactly as the client sent it. Ownership is the
// exact catalog entry or the first matching resource template (see
// resolveResourceOwner). An unowned uri wraps ErrUnknownResource so the
// dispatcher can answer Invalid params; transport failures follow the same
// sanitized-error contract as CallTool/GetPrompt (full detail logged here,
// only the client's own uri echoed back).
func (r *Registry) ReadResource(ctx context.Context, uri string) (*mcp.Message, error) {
	owner, ok := r.resolveResourceOwner(uri)
	r.mu.RLock()
	conn := r.conns[owner]
	r.mu.RUnlock()

	if !ok || conn == nil {
		return nil, fmt.Errorf("%w %q", ErrUnknownResource, uri)
	}

	var resp *mcp.Message
	err := r.withCallTimeoutFor(ctx, owner, func(ctx context.Context) error {
		var err error
		resp, err = conn.ReadResource(ctx, uri)
		return err
	})
	if err != nil {
		r.log.Warn("resource read failed", "uri", uri, "upstream", owner, "err", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("read resource %q timed out", uri)
		}
		return nil, fmt.Errorf("read resource %q failed", uri)
	}
	return resp, nil
}

// Complete routes a completion/complete to the upstream owning the referenced
// prompt or resource (Round 5). For a ref/prompt the client sends the
// CLIENT-FACING namespaced prompt name (the only name it ever saw in
// prompts/list), so — exactly like GetPrompt — the registry resolves the owner
// via promptRoute and rewrites ref.name back to the upstream's original before
// forwarding. For a ref/resource the URI is never namespaced: it resolves via
// resolveResourceOwner (exact entry or template match) and is forwarded as-is.
// Argument/context/_meta travel verbatim in both cases. A ref that resolves to
// no upstream wraps ErrUnknownCompletionRef (dispatcher answers Invalid
// params); transport failures follow the sanitized-error contract.
func (r *Registry) Complete(ctx context.Context, params mcp.CompletionCompleteParams) (*mcp.Message, error) {
	var owner, refDesc string
	switch params.Ref.Type {
	case mcp.CompletionRefPrompt:
		refDesc = fmt.Sprintf("prompt %q", params.Ref.Name)
		r.mu.RLock()
		rt, ok := r.promptRoute[params.Ref.Name]
		r.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownCompletionRef, refDesc)
		}
		owner = rt.upstream
		params.Ref.Name = rt.original // the upstream knows only its own name
	case mcp.CompletionRefResource:
		refDesc = fmt.Sprintf("resource %q", params.Ref.URI)
		var ok bool
		owner, ok = r.resolveResourceOwner(params.Ref.URI)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownCompletionRef, refDesc)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported ref type %q", ErrUnknownCompletionRef, params.Ref.Type)
	}

	r.mu.RLock()
	conn := r.conns[owner]
	r.mu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownCompletionRef, refDesc)
	}

	var resp *mcp.Message
	err := r.withCallTimeoutFor(ctx, owner, func(ctx context.Context) error {
		var err error
		resp, err = conn.Complete(ctx, mcp.MustParams(params))
		return err
	})
	if err != nil {
		r.log.Warn("completion failed", "ref", refDesc, "upstream", owner, "err", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("completion for %s timed out", refDesc)
		}
		return nil, fmt.Errorf("completion for %s failed", refDesc)
	}
	return resp, nil
}

// Instructions aggregates the live upstreams' initialize instructions into one
// string for the gateway's own InitializeResult.Instructions: each non-empty
// entry becomes a "## <upstream>" section, sections are sorted by upstream
// name (deterministic regardless of map order) and joined by a blank line.
// Empty when no upstream provided instructions.
func (r *Registry) Instructions() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handshakes))
	for name, m := range r.handshakes {
		if m.instructions != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = "## " + name + "\n" + r.handshakes[name].instructions
	}
	return strings.Join(parts, "\n\n")
}

// HasUpstreamCapability reports whether at least one live upstream declared
// the named capability (e.g. "prompts", "resources", "logging") in its
// initialize response. The check is deliberately shallow — the key exists in
// the capabilities object and is not JSON null — because the gateway only
// needs to know whether advertising the capability itself would be honest;
// sub-flags (listChanged, subscribe) are the concern of the feature that
// actually proxies the methods. Consumed by the transport's capability
// builder from the next rounds on (prompts/resources/logging aggregation).
func (r *Registry) HasUpstreamCapability(capability string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, m := range r.handshakes {
		if hasCapability(m.capabilities, capability) {
			return true
		}
	}
	return false
}

// hasCapability reports whether ONE raw capabilities object declares the named
// capability: the key exists and is not JSON null. Same deliberately shallow
// check as HasUpstreamCapability (which loops over it); also used by launch to
// decide whether an upstream may be asked for prompts/list at all.
func hasCapability(capabilities json.RawMessage, capability string) bool {
	var caps map[string]json.RawMessage
	if json.Unmarshal(capabilities, &caps) != nil {
		return false // absent or malformed capabilities object: declares nothing
	}
	v, ok := caps[capability]
	return ok && string(v) != "null"
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
func (r *Registry) CallTool(ctx context.Context, namespaced string, arguments, meta json.RawMessage) (*mcp.Message, error) {
	r.mu.RLock()
	rt, ok := r.toolRoute[namespaced]
	conn := r.conns[rt.upstream]
	r.mu.RUnlock()

	if !ok || conn == nil {
		return nil, fmt.Errorf("unknown tool %q", namespaced)
	}

	resp, err := r.callUpstream(ctx, conn, rt, namespaced, arguments, meta)
	// The connection can be closed between the RUnlock above and the call — a
	// concurrent Reload/auto-restart may already have installed a FRESH
	// connection under the same name. ErrConnClosedBeforeSend guarantees the
	// request was never written to the upstream, so ONE retry against the
	// re-resolved connection is safe — but only if it actually resolved to a
	// DIFFERENT connection (retrying the same dead conn is pointless). A plain
	// late ErrConnClosed (failure AFTER the request was sent) is deliberately
	// NOT retried: the upstream may have executed the (potentially
	// non-idempotent) tool already, and double execution is worse than an
	// honest error.
	//
	// The retry deliberately reuses the caller's ctx rather than granting
	// itself fresh time — the client's deadline must bound the WHOLE call,
	// retries included, or it stops meaning anything. In practice the retry
	// still gets essentially the full budget: ErrConnClosedBeforeSend comes
	// only from the pre-write closed-connection check (no I/O, ~zero time
	// spent), and each attempt gets its own EffectiveCallTimeout inside
	// callUpstream, capped by ctx.
	if err != nil && errors.Is(err, upstream.ErrConnClosedBeforeSend) {
		r.mu.RLock()
		rt2, ok2 := r.toolRoute[namespaced]
		conn2 := r.conns[rt2.upstream]
		r.mu.RUnlock()
		if ok2 && conn2 != nil && conn2 != conn {
			r.log.Info("tool call hit a closed connection before send, retrying on the fresh one",
				"tool", namespaced, "upstream", rt2.upstream)
			resp, err = r.callUpstream(ctx, conn2, rt2, namespaced, arguments, meta)
		}
	}
	if err != nil {
		r.log.Warn("tool call failed", "tool", namespaced, "upstream", rt.upstream, "err", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("call %q timed out", namespaced)
		}
		return nil, fmt.Errorf("call %q failed", namespaced)
	}
	return resp, nil
}

// callUpstream performs ONE forwarding attempt against a resolved connection,
// with its own guards (rate limit, concurrency cap, per-upstream timeout,
// result truncation — see guardedCall in limits.go), audit record and (opt-in)
// payload record — each attempt CallTool makes is audited separately, the
// failed first try and the retried second one alike, a guard rejection the
// same as a transport failure; nothing is hidden from the call journal. The
// recorded duration includes any time spent queueing on the guards: that is
// the latency the client actually experienced.
func (r *Registry) callUpstream(ctx context.Context, conn Upstream, rt route, namespaced string, arguments, meta json.RawMessage) (*mcp.Message, error) {
	start := time.Now()
	resp, err := r.guardedCall(ctx, conn, rt, arguments, meta)
	r.audit(ctx, rt.upstream, mcp.MethodToolsCall, namespaced, start, resp, err)
	r.recordPayload(rt.upstream, namespaced, arguments, resp, err)
	return resp, err
}

// callOutcome derives the shared success/failure verdict of one forwarded call
// for the audit and payload logs: a call is OK when the transport succeeded AND
// the response carries no JSON-RPC error; errMsg is the transport error text or
// the upstream's error message (never the arguments) — empty on success.
func callOutcome(resp *mcp.Message, err error) (ok bool, errMsg string) {
	switch {
	case err != nil:
		return false, err.Error()
	case resp != nil && resp.Error != nil:
		return false, resp.Error.Message
	}
	return true, ""
}

// audit writes one CallRecord. Arguments are never logged (may hold secrets).
// The calling client's identity ("name/version" from its initialize, if the
// transport attached one via WithClient) is read from ctx — empty when the
// transport could not identify the client (see the HTTP limitation in
// transport.handlePost).
func (r *Registry) audit(ctx context.Context, up, method, tool string, start time.Time, resp *mcp.Message, err error) {
	if r.callLog == nil {
		return
	}
	ok, errMsg := callOutcome(resp, err)
	r.callLog.Record(logging.CallRecord{
		Time:     start,
		Upstream: up,
		Method:   method,
		Tool:     tool,
		Client:   ClientFromContext(ctx),
		Duration: time.Since(start),
		OK:       ok,
		Err:      errMsg, // transport error or upstream error message; no arguments
	})
}

// recordPayload writes one PayloadRecord to the OPT-IN payload debug log — the
// full arguments and result of a call. This is deliberately kept separate from
// audit (which stays metadata-only): only here, when the operator explicitly
// enabled payload logging, do raw arguments hit disk — and even then both
// arguments and result pass through logging.MaskSecrets first, so top-level
// secret-looking fields (tokens, keys, passwords) are replaced by "***"
// (SKILL §6). When payload logging is disabled r.payloadLog is a no-op, so
// this is a cheap call.
func (r *Registry) recordPayload(up, tool string, arguments json.RawMessage, resp *mcp.Message, err error) {
	ok, errMsg := callOutcome(resp, err)
	rec := logging.PayloadRecord{
		Time:      time.Now(),
		Upstream:  up,
		Tool:      tool,
		Method:    mcp.MethodToolsCall,
		OK:        ok,
		Err:       errMsg,
		Arguments: logging.MaskSecrets(arguments),
	}
	if err == nil && resp != nil {
		rec.Result = logging.MaskSecrets(resp.Result)
		if resp.Error != nil {
			rec.ErrorData = resp.Error.Data
		}
	}
	r.payloadLog.Record(rec)
}

func (r *Registry) upstreamCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}

// recordFailure notes why an upstream never came up, so Start can surface a
// concrete cause if every upstream fails — not just "check the logs" (Start
// launches upstreams in parallel, so failures are collected here rather than
// threaded back through errgroup, which discards them by design).
func (r *Registry) recordFailure(name, reason string) {
	r.failMu.Lock()
	defer r.failMu.Unlock()
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
// slice is a sorted copy, sorted by name for a stable presentation regardless
// of config order; mutating it does not affect the registry.
func (r *Registry) StartReport() []UpstreamStatus {
	r.failMu.Lock()
	out := append([]UpstreamStatus(nil), r.report...)
	r.failMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// failureSummary renders one "- upstream: reason" line per recorded failure,
// sorted for deterministic output regardless of config order.
// Empty when no upstream was even enabled (config has none, or all
// are disabled) — a distinct cause from every enabled one failing.
func (r *Registry) failureSummary() string {
	r.failMu.Lock()
	var reasons []string
	for _, st := range r.report {
		if !st.OK {
			reasons = append(reasons, st.Name+": "+st.Err)
		}
	}
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
	// otherwise call replaceUpstreamIfLive after Close emptied the map, resurrecting a
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
	// A timer that already fired is harmless (relistClosed below turns a late
	// runRelist into a no-op, and relistUpstream bails on procCtx cancellation
	// anyway), this just avoids a needless late wakeup. The relist states are
	// reset for the same tidiness: a still-running runRelist holds its own
	// *relistState pointer, so its final flag flips land on the abandoned
	// struct — harmless, never blocking.
	r.relistMu.Lock()
	r.relistClosed = true
	for _, t := range r.relistTimers {
		t.Stop()
	}
	r.relistTimers = map[string]*time.Timer{}
	r.relistStates = map[string]*relistState{}
	r.relistMu.Unlock()

	// Wait (bounded) for in-flight re-lists: a runRelist may still be inside a
	// blocking ListTools against a connection we are about to Close below, and
	// its "re-list failed" warning must not land after shutdown reports
	// completion. procCancel above already aborted those RPCs (their ctx
	// derives from procCtx), so this normally returns instantly; the timeout
	// only keeps a pathologically stuck re-list from hanging shutdown.
	relistDone := make(chan struct{})
	go func() {
		r.relistWG.Wait()
		close(relistDone)
	}()
	select {
	case <-relistDone:
	case <-time.After(relistCloseWait):
		r.log.Warn("in-flight re-list did not finish within grace period, proceeding with shutdown")
	}

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
