package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// This file owns the ONE pipeline for server→client requests: a request an
// UPSTREAM initiates mid-call that only the gateway's own client can answer
// (elicitation/create — Round 14; sampling/createMessage and roots/list —
// Stage 15). Round 14 implemented the elicitation case by hand; Stage 15
// generalized it, because the three methods differ in exactly four DATA points
// (the method name, the client capability that must be declared, how that
// capability renders on the wire, and the shape of the refusal) while the
// mechanics — park under a gateway-minted id, publish to the subscribed
// client transport, route the answer back under the upstream's original id,
// roll back an undeliverable publish, clean up when the upstream dies — are
// identical. Those data points live in serverReqSpecs below; a per-method
// handler registry would have forced the mechanics to be copied three times.
//
// The client-facing transport is deliberately method-agnostic: it writes
// mcp.NewRequest(mcp.StringID(req.GatewayID), req.Method, req.Params) and
// routes any string-id response back through RouteUpstreamResponse.

// serverReqSpec is the per-method data the shared pipeline needs. Adding a
// method means adding one entry — declaration in the upstream handshake,
// capability sniffing of the client's initialize and response routing all
// follow from it.
type serverReqSpec struct {
	// method is the JSON-RPC method the UPSTREAM sends.
	method string
	// capability is the CLIENT capability that must be declared for this
	// method to be servable: mcp.CapElicitation / CapSampling / CapRoots.
	capability string
	// declare renders the wire value of that capability in the
	// gateway→upstream handshake FROM the value the gateway's own client
	// declared for it. It is a function, not a constant, because the value can
	// carry sub-flags the gateway must MIRROR rather than invent: a client that
	// declared "roots":{} (legal — it supports roots but does not notify about
	// changes) must not have the gateway promise its upstreams a
	// listChanged stream that will never arrive, since the fan-out only
	// happens when the client itself sends notifications/roots/list_changed
	// (found by review). It is called only for a capability the client
	// DECLARED; an absent one is never rendered at all.
	declare func(clientValue json.RawMessage) json.RawMessage
	// idPrefix prefixes the gateway-minted client-facing id ("elicit-",
	// "sampling-", "roots-"). It is for log readability and for the historical
	// elicit- contract only: uniqueness comes from the single shared counter
	// and the single pending map, never from the prefix.
	idPrefix string
	// refuse answers the upstream when the client cannot be asked at all. It
	// is deliberately NOT generalized (user decision, 2026-07-27): elicitation
	// refuses with the spec's {"action":"decline"} RESULT, which every server
	// MUST handle without failing its tool call, while sampling and roots have
	// no soft-refusal form in the 2025-06-18 spec and get JSON-RPC -32601.
	refuse func(r *Registry, upstream string, originalID json.RawMessage, reason string)
	// handle, when non-nil, REPLACES the generic park-and-publish body for
	// this method (roots/list: one client request serves N upstreams through a
	// cache plus single-flight). It runs after the capability gate, on the
	// upstream's reader goroutine, and owns answering the upstream itself.
	handle func(r *Registry, upstream string, originalID json.RawMessage)
}

// serverReqSpecs is the whole per-method difference between the three
// server→client requests the gateway proxies.
var serverReqSpecs = []serverReqSpec{
	{
		method:     mcp.MethodElicitationCreate,
		capability: mcp.CapElicitation,
		declare:    declareEmpty,
		idPrefix:   "elicit-",
		refuse:     (*Registry).respondElicitDecline,
	},
	{
		method:     mcp.MethodSamplingCreateMessage,
		capability: mcp.CapSampling,
		declare:    declareEmpty,
		idPrefix:   "sampling-",
		refuse:     refuseMethodNotFound,
	},
	{
		method:     mcp.MethodRootsList,
		capability: mcp.CapRoots,
		declare:    declareRoots,
		idPrefix:   rootsIDPrefix,
		refuse:     refuseMethodNotFound,
		handle:     onUpstreamRootsList,
	},
}

// declareEmpty renders a capability that has no sub-flags in the 2025-06-18
// spec — elicitation and sampling. Whatever the client put in the value, the
// only thing it can mean is "supported", and {} says exactly that.
func declareEmpty(json.RawMessage) json.RawMessage { return json.RawMessage(`{}`) }

// declareRoots renders the roots capability, mirroring the client's listChanged
// sub-flag instead of asserting it.
//
// listChanged may be claimed ONLY when the client claimed it, because that is
// the only case in which an upstream will ever see the notification: the
// gateway relays notifications/roots/list_changed, it does not originate them
// (see OnClientRootsListChanged). Claiming it to a client that never notifies
// would leave a trusting upstream serving a stale root list forever — exactly
// the kind of lie Stage 15 removed elsewhere (found by review).
//
// The parse is tolerant like every other read of client-supplied JSON here: an
// unreadable value degrades to the weaker, always-safe {} rather than failing.
func declareRoots(clientValue json.RawMessage) json.RawMessage {
	var v struct {
		ListChanged bool `json:"listChanged"`
	}
	if json.Unmarshal(clientValue, &v) == nil && v.ListChanged {
		return json.RawMessage(`{"listChanged":true}`)
	}
	return json.RawMessage(`{}`)
}

// specForMethod finds the spec for an upstream-initiated method; nil means the
// gateway does not proxy it (log-and-ignore, the pre-Round 14 behaviour for
// every unknown server→client request).
func specForMethod(method string) *serverReqSpec {
	for i := range serverReqSpecs {
		if serverReqSpecs[i].method == method {
			return &serverReqSpecs[i]
		}
	}
	return nil
}

// defaultServerReqTimeout bounds how long ONE proxied server→client request may
// stay pending before the gateway gives up on its client and refuses the
// upstream in the spec's form.
//
// Deliberately generous: elicitation/create puts a HUMAN in the loop (a form to
// read and fill in) and sampling may queue behind an LLM call, so a short
// deadline would turn slow-but-correct answers into refusals an upstream cannot
// tell from "unsupported". Five minutes is longer than any interactive answer
// plausibly takes while still BOUNDING the damage of a client that answers
// never: without a deadline one unanswered roots/list wedges roots for the life
// of the process (rootsFetchID is only cleared on delivery) and rootsWaiters
// grow without limit, while an unanswered elicitation/sampling leaks its map
// entry (found by review).
//
// Not a config knob on purpose — Stage 15 does not touch the public config
// contract. Tests override Registry.serverReqTimeout directly.
const defaultServerReqTimeout = 5 * time.Minute

// forwardNotifyTimeout bounds ONE fire-and-forget notification the gateway
// relays to an upstream (today notifications/roots/list_changed). Generous,
// because nothing is waiting on it and a slow-but-alive upstream should still
// receive it; finite, because the alternative is a goroutine and a connection
// pinned for the life of the process on an upstream that has stopped reading.
// Not a config knob, by the same reasoning as defaultServerReqTimeout.
const forwardNotifyTimeout = 30 * time.Second

// pendingServerReq records where the client's answer to one proxied
// server→client request must go: which upstream asked, and under what id in
// the UPSTREAM's own id space. deliver, when non-nil, takes over the whole
// delivery (roots/list fans one client answer out to every waiting upstream),
// in which case upstream/originalID are unused.
type pendingServerReq struct {
	upstream   string
	originalID json.RawMessage
	deliver    func(r *Registry, msg *mcp.Message)
	// expire refuses whoever is waiting on this request when it is given up on
	// — the deadline passed, or a client transport could not deliver it — in
	// the shape its spec entry prescribes, and unwinds any per-method state
	// (roots/list: the whole single-flight). It is called at most once, by
	// whoever removed the entry from pendingServerReqs.
	expire func(r *Registry, gatewayID, reason string)
	// timer is that deadline. Armed together with the parking and stopped by
	// whoever removes the entry, so a normally-answered request leaves no timer
	// behind and no timer outlives Close.
	timer *time.Timer
}

// UpstreamRequest is one upstream-initiated request the gateway proxies to its
// client. GatewayID is the gateway-minted id the client-facing request must
// carry ("<prefix><n>" — see onUpstreamRequest for why the upstream's own id
// never travels); Method is the JSON-RPC method to send; Params is the payload
// to forward, verbatim (nil for roots/list, which has no params).
type UpstreamRequest struct {
	GatewayID string
	Method    string
	Params    json.RawMessage
}

// SubscribeUpstreamRequests registers interest in the upstream-initiated
// requests the gateway proxies to its client — SubscribeNotifications' twin
// for the upstream messages that expect an answer back
// (RouteUpstreamResponse). Same delivery contract: buffered channel
// (notifSubBuffer), strictly non-blocking send from the upstream's reader
// goroutine, and an unsubscribe function the caller MUST call when it stops
// listening.
func (r *Registry) SubscribeUpstreamRequests() (<-chan UpstreamRequest, func()) {
	ch := make(chan UpstreamRequest, notifSubBuffer)
	r.serverReqMu.Lock()
	id := r.nextServerReqSub
	r.nextServerReqSub++
	r.serverReqSubs[id] = ch
	r.serverReqMu.Unlock()

	return ch, func() {
		r.serverReqMu.Lock()
		delete(r.serverReqSubs, id)
		r.serverReqMu.Unlock()
	}
}

// onUpstreamRequest handles any REQUEST pushed by an upstream mid-call — from
// a stdio child (Round 14) or, since Stage 17b, from an HTTP one over its SSE
// stream. Like onUpstreamNotification it runs on that upstream's reader
// goroutine (the SSE stream's, or the in-flight call's, for HTTP), so it must
// not block or re-enter the connection synchronously.
//
// A method with no spec is log-and-ignored (the pre-Round 14 behaviour for
// every server→client request the gateway does not proxy). Otherwise:
//
//   - the client never declared this capability → the spec's refusal goes back
//     immediately, so the upstream is not left hanging until its own timeout.
//     This path stays reachable even under Stage 15's honest declaration: an
//     upstream may ignore the negotiation and ask anyway, a re-initialize may
//     have withdrawn a capability the handshakes already advertised, or the
//     publish may reach nobody;
//   - a spec with its own handle (roots/list) takes over from here;
//   - otherwise the request is parked under a freshly minted gateway id and
//     published to the subscribed transport (non-blocking, like
//     forwardNotification). A publish that reached NO subscriber (none
//     registered, or every buffer full) rolls the parked entry back and
//     refuses too: RouteUpstreamResponse is the entry's only other remover, so
//     an undeliverable request would otherwise leak forever (found by review).
//
// No path here answers "the request itself is invalid" — the gateway never
// inspects the params.
func (r *Registry) onUpstreamRequest(name, method string, originalID, params json.RawMessage) {
	spec := specForMethod(method)
	if spec == nil {
		r.log.Debug("upstream request ignored", "upstream", name, "method", method)
		return
	}
	if !r.clientDeclared(spec.capability) {
		spec.refuse(r, name, originalID,
			"the gateway's client did not declare the "+spec.capability+" capability")
		return
	}
	if spec.handle != nil {
		spec.handle(r, name, originalID)
		return
	}

	gatewayID := r.mintServerReqID(spec.idPrefix)
	entry := pendingServerReq{
		upstream:   name,
		originalID: originalID,
		expire: func(r *Registry, _, reason string) {
			spec.refuse(r, name, originalID, reason)
		},
	}
	if !r.publishServerReq(gatewayID, entry, UpstreamRequest{GatewayID: gatewayID, Method: method, Params: params}) {
		spec.refuse(r, name, originalID, "no client transport accepted the request")
		// Journaled outside serverReqMu (publishServerReq already released it):
		// from the user's side this looks like a tool that just failed, with the
		// reason visible nowhere else (Stage 18).
		r.noteThrottled(logging.EventServerRequestDropped, name, method,
			"no client transport accepted the request; the tool call was refused on the upstream's behalf")
		return
	}
	r.log.Debug("upstream request proxied to client", "upstream", name, "method", method, "id", gatewayID)
}

// mintServerReqID mints the next client-facing id for a proxied request. ONE
// atomic counter serves every method: the prefix is cosmetic, the pending map
// is shared, so a single monotonic sequence guarantees uniqueness across
// methods and upstreams alike.
func (r *Registry) mintServerReqID(prefix string) string {
	return fmt.Sprintf("%s%d", prefix, r.serverReqID.Add(1))
}

// publishServerReq parks entry under gatewayID and publishes req to every
// subscribed client transport, non-blocking. It reports whether ANY subscriber
// took it; on false the parked entry is rolled back before returning, so the
// caller only has to refuse the upstream. Park and publish happen under one
// hold of serverReqMu: a subscriber must never be able to answer an id that is
// not in the map yet.
//
// The deadline is armed inside that same hold, for the same reason: the pending
// entry and the timer that removes it must never exist apart, or a crash
// between them would leak exactly the wedge the timer exists to prevent. The
// AfterFunc callback takes serverReqMu itself, so it cannot run before this
// returns.
func (r *Registry) publishServerReq(gatewayID string, entry pendingServerReq, req UpstreamRequest) bool {
	delivered := false
	r.serverReqMu.Lock()
	entry.timer = time.AfterFunc(r.serverReqTimeout, func() {
		r.RefusePendingServerReq(gatewayID, "the gateway's client did not answer within "+r.serverReqTimeout.String())
	})
	r.pendingServerReqs[gatewayID] = entry
	for _, ch := range r.serverReqSubs {
		select {
		case ch <- req:
			delivered = true
		default:
			r.log.Debug("server-request subscriber buffer full, dropping",
				"method", req.Method, "id", gatewayID)
		}
	}
	if !delivered {
		entry.timer.Stop()
		delete(r.pendingServerReqs, gatewayID) // roll back: nobody will ever answer it
	}
	r.serverReqMu.Unlock()
	return delivered
}

// takePendingServerReq removes gatewayID from the pending map and stops its
// deadline, reporting whether it was still there.
//
// This is the ARBITER of the whole pipeline. Three parties race to finish one
// proxied request — the client's answer (RouteUpstreamResponse), the deadline
// (RefusePendingServerReq from the timer) and a transport giving up
// (RefusePendingServerReq from the client-facing loop) — and the upstream must
// receive exactly ONE response. Removal from the map under serverReqMu is what
// decides: whoever takes the entry owns the answer, everyone else gets false
// and does nothing. No other code may delete from pendingServerReqs.
func (r *Registry) takePendingServerReq(gatewayID string) (pendingServerReq, bool) {
	r.serverReqMu.Lock()
	p, ok := r.pendingServerReqs[gatewayID]
	if ok {
		delete(r.pendingServerReqs, gatewayID)
	}
	r.serverReqMu.Unlock()
	if ok && p.timer != nil {
		// Stopped OUTSIDE the lock: Stop on an already-fired timer does not
		// wait for its callback, and that callback takes serverReqMu — but it
		// will find the id gone and do nothing, which is the arbitration above.
		p.timer.Stop()
	}
	return p, ok
}

// RefusePendingServerReq gives up on a proxied server→client request and
// refuses whoever is waiting on it in the shape its spec entry prescribes
// ({"action":"decline"} for elicitation, -32601 for sampling and roots),
// unwinding any per-method state with it. It reports whether the request was
// still pending — false means somebody else already finished it (the client
// answered, or another giver-upper won), in which case nothing happens.
//
// Two callers: the per-request deadline armed by publishServerReq, and a
// client-facing transport that cannot deliver the request after all (a
// server→client request reaching a stdio loop before the client's handshake —
// unreachable by construction today, but an unrefused drop there would wedge
// roots for the life of the process, so the refusal is explicit rather than
// implied by the ordering, found by review).
func (r *Registry) RefusePendingServerReq(gatewayID, reason string) bool {
	p, ok := r.takePendingServerReq(gatewayID)
	if !ok {
		return false
	}
	r.log.Debug("server-request given up on", "id", gatewayID, "reason", reason)
	p.expire(r, gatewayID, reason)
	return true
}

// respondElicitDecline answers an upstream's elicitation/create with the
// spec's {"action":"decline"} result under the upstream's OWN id — used when
// no human can be asked (client without the capability, or no subscriber
// accepted the publish), so the upstream gets an immediate answer instead of
// hanging until its own timeout. reason is Debug-logged only; it never travels
// to the upstream — a decline result carries no message field.
//
// A RESULT, not a JSON-RPC error, by explicit user decision (2026-07-27): a
// decline is one of the three documented ElicitResult actions every server
// MUST handle (typically by proceeding without the extra input), whereas an
// error is turned into an exception by server SDKs, failing the whole
// tools/call — calls that succeeded before the gateway declared anything.
// "cancel" was considered and rejected: some servers treat it as "abort the
// whole operation", decline merely as "no extra input". sampling and roots get
// -32601 instead (respondServerReqError): the spec defines no soft refusal for
// them, so there is nothing to be gentle with.
func (r *Registry) respondElicitDecline(name string, originalID json.RawMessage, reason string) {
	r.log.Debug("upstream elicitation declined", "upstream", name, "reason", reason)
	r.writeUpstreamResponse(name, mcp.NewResult(originalID, json.RawMessage(`{"action":"decline"}`)))
}

// rootsIDPrefix is the roots/list entry's idPrefix, named because the roots
// handler mints its ids directly: reaching back into the spec table from a
// function the table itself references would be an initialization cycle.
const rootsIDPrefix = "roots-"

// rootsWaiter is one upstream parked behind the single in-flight roots/list
// question to the client.
type rootsWaiter struct {
	upstream   string
	originalID json.RawMessage
}

// onUpstreamRootsList serves an upstream's roots/list from the cache, or joins
// it to the single question in flight, or starts that question — the roots
// special case on top of the shared pipeline (Stage 15 §5). It runs on the
// asking upstream's reader goroutine, after the capability gate.
//
// The upstream's own params are NOT forwarded: roots/list takes none, and one
// client answer serves several upstreams, so there is nothing per-asker to
// pass on.
func onUpstreamRootsList(r *Registry, name string, originalID json.RawMessage) {
	r.rootsMu.Lock()
	if r.rootsCache != nil {
		cached := r.rootsCache
		r.rootsMu.Unlock()
		// Served without bothering the client at all — the whole point of the
		// cache when N upstreams ask the same question.
		r.writeUpstreamResponse(name, mcp.NewResult(originalID, cached))
		return
	}
	r.rootsWaiters = append(r.rootsWaiters, rootsWaiter{upstream: name, originalID: originalID})
	if r.rootsFetchID != "" {
		r.rootsMu.Unlock()
		return // a question is already in flight; this asker rides along
	}
	gatewayID := r.mintServerReqID(rootsIDPrefix)
	r.rootsFetchID = gatewayID
	r.rootsMu.Unlock()

	entry := pendingServerReq{deliver: rootsDeliver, expire: rootsExpire}
	if r.publishServerReq(gatewayID, entry, UpstreamRequest{GatewayID: gatewayID, Method: mcp.MethodRootsList}) {
		r.log.Debug("upstream roots/list proxied to client", "upstream", name, "id", gatewayID)
		return
	}
	// Nobody took the question: nothing will ever answer it. That is the same
	// situation a missed deadline leaves behind, so it takes the same unwinding
	// — which also clears rootsStale, the one piece the hand-rolled rollback
	// used to forget (found by review).
	rootsExpire(r, gatewayID, "no client transport accepted the roots/list request")
	r.noteThrottled(logging.EventServerRequestDropped, name, mcp.MethodRootsList,
		"no client transport accepted the request; the tool call was refused on the upstream's behalf")
}

// rootsExpire unwinds the single-flight when the ONE roots/list question in
// flight is given up on — the client did not answer within the deadline, or no
// transport ever took the question. Everything goes: the in-flight id (so the
// next asker starts a fresh question instead of parking behind a dead one), the
// parked waiters, and the stale mark (which belongs to the abandoned question,
// not to the next one). Every waiter is then refused with -32601, roots' only
// refusal shape.
//
// The id guard matters: removal from the pending map arbitrates WHO unwinds,
// but the roots state has its own mutex and may already have moved on to a
// newer question by the time this runs — clearing it then would wedge the new
// fetch instead.
func rootsExpire(r *Registry, gatewayID, reason string) {
	r.rootsMu.Lock()
	if r.rootsFetchID != gatewayID {
		r.rootsMu.Unlock()
		return
	}
	waiters := r.rootsWaiters
	r.rootsWaiters = nil
	r.rootsFetchID = ""
	r.rootsStale = false
	r.rootsMu.Unlock()

	for _, w := range waiters {
		refuseMethodNotFound(r, w.upstream, w.originalID, reason)
	}
}

// rootsDeliver fans the client's ONE roots/list answer out to every upstream
// parked behind it, verbatim — result or error, each under its own original
// id. It is installed as the pending entry's deliver, so RouteUpstreamResponse
// hands the reply here instead of writing it to a single upstream.
//
// The answer is cached only when it succeeded AND no invalidation arrived
// while it was in flight: in that stale window the waiters still get their
// answer (they asked before the change, and the spec tolerates the race) but
// the next asker starts a fresh question.
func rootsDeliver(r *Registry, msg *mcp.Message) {
	r.rootsMu.Lock()
	waiters := r.rootsWaiters
	r.rootsWaiters = nil
	r.rootsFetchID = ""
	if msg.Error == nil && !r.rootsStale {
		r.rootsCache = msg.Result
	}
	r.rootsStale = false
	r.rootsMu.Unlock()

	for _, w := range waiters {
		reply := &mcp.Message{JSONRPC: mcp.Version, ID: w.originalID, Result: msg.Result, Error: msg.Error}
		r.writeUpstreamResponse(w.upstream, reply)
	}
}

// InvalidateRootsCache drops the cached roots/list answer. Called when the
// client says its roots changed (notifications/roots/list_changed) and on
// every (re-)initialize, since a fresh handshake may well be a different
// client with different roots. An invalidation landing while a question is in
// flight is remembered (rootsStale) so that answer is delivered but not
// cached.
func (r *Registry) InvalidateRootsCache() {
	r.rootsMu.Lock()
	r.rootsCache = nil
	if r.rootsFetchID != "" {
		r.rootsStale = true
	}
	r.rootsMu.Unlock()
}

// OnClientRootsListChanged handles the client's notifications/roots/list_changed:
// it drops the cache and relays the notification to every upstream the gateway
// declared roots to (the connection itself applies that gate — see
// Conn.ForwardRootsListChanged).
//
// The fan-out does NOT wait, unlike SetLogLevel's: this is called inline from
// the client transport's Serve loop, and a write into a stuck child's stdin can
// block indefinitely — the loop must keep serving. For the same reason the
// writes use the registry's long-lived process context rather than a caller
// context that may be cancelled the moment the notification is handled; they
// unwind on Close.
func (r *Registry) OnClientRootsListChanged() {
	r.InvalidateRootsCache()

	r.mu.RLock()
	targets := make(map[string]Upstream, len(r.conns))
	maps.Copy(targets, r.conns)
	r.mu.RUnlock()

	for name, conn := range targets {
		go func(name string, conn Upstream) {
			// Bounded, NOT the bare process context. Stage 17b made this
			// reachable over HTTP for the first time (before it, roots was
			// never declared to an HTTP upstream, so the connection's own gate
			// short-circuited every such call): an unbounded POST to a hung
			// upstream would hold this goroutine and its connection until the
			// gateway stopped, one leak per client roots/list_changed. stdio
			// gets the same bound — a write into a stuck child's stdin blocks
			// just as indefinitely (found by review).
			ctx, cancel := context.WithTimeout(r.procCtx, forwardNotifyTimeout)
			defer cancel()
			if err := conn.ForwardRootsListChanged(ctx); err != nil {
				r.log.Warn("forward roots/list_changed failed", "upstream", name, "err", err)
			}
		}(name, conn)
	}
}

// dropRootsWaiters discards the roots/list waiters of a departing upstream —
// the answer could no longer reach the process that asked. The in-flight
// question itself is deliberately left alone: it may still be serving other
// upstreams, and its answer still fills the cache for the next asker.
func (r *Registry) dropRootsWaiters(name string) {
	r.rootsMu.Lock()
	kept := r.rootsWaiters[:0]
	for _, w := range r.rootsWaiters {
		if w.upstream != name {
			kept = append(kept, w)
		}
	}
	r.rootsWaiters = kept
	r.rootsMu.Unlock()
}

// refuseMethodNotFound answers an upstream's request with JSON-RPC -32601
// under its OWN id — the refusal shape for sampling and roots. Unlike
// elicitation there is no soft-refusal result in the 2025-06-18 spec for
// either method: "no answer available" and "method unknown" are the same
// -32601 to the asking server, so an honest declaration (never promising a
// capability the client lacks) is what keeps this path rare rather than the
// refusal being gentle.
//
// reason stays in the Debug log; the wire message is a fixed, sanitized string
// so nothing about the gateway's internals or its client travels upstream. The
// params of a sampling request may carry conversation content, so nothing of
// them is logged either — only the method and the upstream.
func refuseMethodNotFound(r *Registry, name string, originalID json.RawMessage, reason string) {
	r.log.Debug("upstream request refused", "upstream", name, "reason", reason)
	r.writeUpstreamResponse(name, mcp.NewError(originalID, mcp.CodeMethodNotFound,
		"the gateway's client does not support this request", nil))
}

// writeUpstreamResponse sends one gateway-authored response to the upstream
// that asked. The write runs off the caller's goroutine: it can block on a
// stuck child's stdin or on an HTTP POST to an unresponsive endpoint, and the
// upstream reader publishing the callback (or the client transport's Serve
// loop) must never stall on it.
func (r *Registry) writeUpstreamResponse(name string, reply *mcp.Message) {
	r.mu.RLock()
	conn := r.conns[name]
	r.mu.RUnlock()
	if conn == nil {
		return // the upstream vanished between its write and this callback
	}
	go func() {
		if err := conn.RespondUpstreamRequest(reply); err != nil {
			r.log.Warn("answer upstream request failed", "upstream", name, "err", err)
		}
	}()
}

// ServerRequestCapabilities lists the client capabilities the gateway can
// serve on a client's behalf, in spec-table order. Exported so the
// client-facing transport can sniff exactly this set out of its client's
// initialize without keeping a second, drift-prone copy of the names.
func ServerRequestCapabilities() []string {
	names := make([]string, 0, len(serverReqSpecs))
	for i := range serverReqSpecs {
		names = append(names, serverReqSpecs[i].capability)
	}
	return names
}

// CapabilityForMethod maps an upstream-initiated method to the client
// capability that must be declared for it to be servable, read from the SAME
// spec table everything else derives from. ok=false means the gateway does not
// proxy that method at all.
//
// Exported for the HTTP transport (Stage 17a), which must pick WHICH session
// a server→client request may be delivered to: the registry's own gate is
// process-wide (clientDeclared), while over HTTP several sessions coexist and
// only the ones that declared the capability may be handed the request. Keeping
// the mapping here rather than copying "method → capability" into
// internal/transport is what makes "adding a method = one entry in
// serverReqSpecs" still true.
func CapabilityForMethod(method string) (capability string, ok bool) {
	if spec := specForMethod(method); spec != nil {
		return spec.capability, true
	}
	return "", false
}

// ServerReqPending reports whether gatewayID is still parked awaiting the
// client's answer. Read-only, and deliberately NOT an arbiter: it says nothing
// about who may answer (that is takePendingServerReq's job alone) — it exists
// so a client transport that keeps its own gatewayID→destination bookkeeping
// can sweep entries the registry has already given up on.
//
// Stage 17a's HTTP router uses it exactly that way: the registry drops a
// pending request on its own five-minute deadline, and without this the
// transport's ownership record for that id would live until the session died.
// The alternative — exporting the timeout so the transport could age its own
// entries — would be a second copy of a constant, i.e. drift.
func (r *Registry) ServerReqPending(gatewayID string) bool {
	r.serverReqMu.Lock()
	_, ok := r.pendingServerReqs[gatewayID]
	r.serverReqMu.Unlock()
	return ok
}

// SetClientServerRequestCaps records which server→client capabilities the
// gateway's OWN client declared in its initialize. It is the SINGLE setter
// behind both roles the two Round 14 flags used to split: what the gateway
// declares to an upstream in its handshake (read by startStdio and — since
// Stage 17b — startHTTP, via declaredClientCaps, before Initialize) and the
// runtime gate that decides whether an upstream's request can be served at all
// (read by onUpstreamRequest on a reader goroutine).
//
// Only a client-facing transport that can actually relay a server→client
// request calls it: stdio (Stage 15) and HTTP (Stage 17a). The CLI paths
// (doctor/call/catalog, no MCP client at all) never do, so the set stays nil
// and their upstream handshakes declare exactly {}. nil (or an empty map)
// means "the client declared nothing".
//
// The set carries each capability's RAW VALUE, not just a flag: presence of the
// key is what the runtime gate needs, but the value is what the handshake
// declaration is rendered from (roots' listChanged sub-flag must be mirrored,
// never invented — see declareRoots). A key present with a nil or malformed
// value still counts as declared; every renderer degrades tolerantly.
//
// The map is copied, so a caller may reuse or mutate its own; readers always
// see a consistent immutable snapshot.
func (r *Registry) SetClientServerRequestCaps(caps map[string]json.RawMessage) {
	if len(caps) == 0 {
		r.clientCaps.Store(nil)
		return
	}
	snapshot := maps.Clone(caps)
	r.clientCaps.Store(&snapshot)
}

// clientDeclared reports whether the gateway's OWN client declared the named
// server→client capability in its initialize. Per MCP the KEY's presence means
// support, whatever the value, so only presence is consulted here.
func (r *Registry) clientDeclared(capability string) bool {
	caps := r.clientCaps.Load()
	if caps == nil {
		return false
	}
	_, ok := (*caps)[capability]
	return ok
}

// declaredClientCaps is the client-capability set the gateway declares to an
// upstream in its handshake — stdio and, since Stage 17b, HTTP alike: name →
// the spec's rendered value.
//
// This is the honest intersection Stage 15 introduced: a capability travels
// only when the gateway's own client actually declared it. The optimistic
// Round 14 model ("declare elicitation whenever the transport could relay
// one") was withdrawn by user decision on 2026-07-27 — it is affordable for
// elicitation, whose refusal is a soft {"action":"decline"}, but not for
// sampling, whose only refusal is -32601, which server SDKs turn into an
// exception that fails the whole tools/call. Declaring sampling to a client
// that cannot sample (Claude Code cannot) would break exactly the calls the
// soft decline was chosen to protect.
func (r *Registry) declaredClientCaps() map[string]json.RawMessage {
	declared := map[string]json.RawMessage{}
	caps := r.clientCaps.Load()
	if caps == nil {
		return declared
	}
	for i := range serverReqSpecs {
		spec := &serverReqSpecs[i]
		if value, ok := (*caps)[spec.capability]; ok {
			declared[spec.capability] = spec.declare(value)
		}
	}
	return declared
}

// dropPendingServerReqs discards every parked server→client request belonging
// to name — called whenever name's connection is removed or replaced (reload
// removal, restart give-up, a fresh connection installed after a crash): the
// answer could no longer reach the process that asked, and
// RouteUpstreamResponse is otherwise the ONLY remover, so entries for a gone
// connection would accumulate forever (found by review). Deliberately NOT part
// of dropLocked: a same-connection re-merge (relistUpstream's drop-then-merge)
// must keep its still-answerable pending requests. The roots/list waiters live
// in their own single-flight state and are cleaned up alongside.
func (r *Registry) dropPendingServerReqs(name string) {
	var timers []*time.Timer
	r.serverReqMu.Lock()
	for id, p := range r.pendingServerReqs {
		if p.upstream == name {
			if p.timer != nil {
				timers = append(timers, p.timer)
			}
			delete(r.pendingServerReqs, id)
		}
	}
	r.serverReqMu.Unlock()
	// Outside the lock, like takePendingServerReq: their deadlines have nobody
	// left to refuse. The roots/list fetch (upstream "") is deliberately NOT
	// among them — it may still be serving other upstreams — and if its
	// initiator was the one that just died, its own deadline is what eventually
	// unwinds it (found by review).
	for _, t := range timers {
		t.Stop()
	}
	r.dropRootsWaiters(name)
}

// closeServerReqs is Close's share of this pipeline: it stops every pending
// request's deadline and forgets the requests themselves, so no timer survives
// shutdown and fires a refusal into a registry whose upstreams are already
// gone. Nobody is refused on the way out — the child processes are being torn
// down with the gateway, so there is no one left to answer to. The roots
// single-flight is reset for the same tidiness as the re-list state above it.
func (r *Registry) closeServerReqs() {
	r.serverReqMu.Lock()
	pending := r.pendingServerReqs
	r.pendingServerReqs = map[string]pendingServerReq{}
	r.serverReqMu.Unlock()
	for _, p := range pending {
		if p.timer != nil {
			p.timer.Stop()
		}
	}

	r.rootsMu.Lock()
	r.rootsFetchID = ""
	r.rootsWaiters = nil
	r.rootsStale = false
	r.rootsMu.Unlock()
}

// RouteUpstreamResponse routes the client's answer to a proxied server→client
// request back to the upstream that asked. gatewayID is the gateway-minted id
// the transport decoded from the client's response; msg is that response,
// whose ID is rewritten back to the upstream's original before the write. It
// reports whether gatewayID matched a pending request — false means unknown or
// stale (a duplicate answer, a request already consumed, or one the gateway
// gave up on when its deadline passed), which the caller treats like any other
// unexpected client response: log and drop, never panic. A client answering
// just as the deadline fires cannot produce two responses upstream — the
// pending map arbitrates, see takePendingServerReq.
//
// A pending entry carrying its own deliver takes that path instead (roots/list
// answers N waiting upstreams from one client reply).
func (r *Registry) RouteUpstreamResponse(gatewayID string, msg *mcp.Message) bool {
	p, ok := r.takePendingServerReq(gatewayID)
	if !ok {
		return false
	}
	if p.deliver != nil {
		p.deliver(r, msg)
		return true
	}

	r.mu.RLock()
	conn := r.conns[p.upstream]
	r.mu.RUnlock()
	if conn == nil {
		// The upstream died or was retired while the client was answering: the
		// answer has nowhere to go. Still consumed (true) — it DID match a real
		// pending request; it is just undeliverable now.
		r.log.Warn("answer for a gone upstream, dropping", "upstream", p.upstream, "id", gatewayID)
		return true
	}
	msg.ID = p.originalID
	// Off the caller's goroutine (the client transport's Serve loop): a stdin
	// write can block on a stuck child, and the loop must keep serving other
	// traffic regardless — same rule as the refusal paths above.
	go func() {
		if err := conn.RespondUpstreamRequest(msg); err != nil {
			r.log.Warn("deliver client answer failed", "upstream", p.upstream, "id", gatewayID, "err", err)
		}
	}()
	return true
}
