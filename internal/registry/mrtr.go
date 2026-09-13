package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// This file owns the MRTR bridge (Stage 19c): the one place a MODERN client
// (2026-07-28) and a LEGACY upstream (2025-06-18) meet over a server→client
// request. The upstream leg is unchanged — an upstream still asks
// elicitation/create / sampling/createMessage / roots/list as a counter
// JSON-RPC REQUEST in the middle of our tools/call (or prompts/get,
// resources/read). What changes is the CLIENT leg: a modern client is never
// sent a counter-request. Instead the gateway parks the unfinished call, hands
// the client an InputRequiredResult on its own reply, and resumes the call when
// the client retries with inputResponses (spec basic/patterns/mrtr.mdx).
//
// The whole thing is layered on top of the legacy serverreq.go pipeline, not
// beside it (invariant §12.3 — one id system, not a third):
//
//   - onUpstreamRequest (serverreq.go) checks mrtrWaiters[upstream] BEFORE it
//     publishes to the legacy conveyor. A modern call in flight on that upstream
//     intercepts the question; nothing modern touches the legacy path, so a
//     legacy client on a NEIGHBOURING session keeps getting counter-requests the
//     old way (TestMRTRLegacyCoexistence);
//   - an intercepted question is still parked as a pendingServerReq under a
//     freshly minted gateway id (mintServerReqID) so the client's answer routes
//     back through RouteUpstreamResponse under the upstream's original id — the
//     exact same id rewriting as the legacy path;
//   - roots/list still rides the shared cache + single-flight (onUpstreamRootsList)
//     so N upstreams (modern or legacy) never ask the client twice.
//
// # Deliberate limitations (documented, NOT bugs to fix here — plan §6.3)
//
//   - Question→call attribution is HEURISTIC (LIFO per upstream). A legacy
//     upstream does not tell us which of its in-flight calls a counter-request
//     belongs to, so the most-recently-started modern call on that upstream
//     claims it. With two concurrent modern calls to the SAME upstream, both
//     eliciting, a question can be attributed to the wrong call. This is the
//     same class of imprecision the legacy multicast already has (a question
//     goes to "some" session that declared the capability). Misroute affects
//     only the outcome of those calls, never safety.
//   - A question asked OUTSIDE any active modern call (no mrtrWaiter on that
//     upstream) is not interceptable — there is nowhere to deliver an
//     InputRequiredResult, the modern spec has no free-floating server→client
//     request, and the client's next call is unrelated. It falls through to the
//     legacy path: served if a legacy subscriber exists, otherwise refused as
//     today. No queue is invented.

// mrtrCollectWindow is how long a parked modern call waits, AFTER its first
// intercepted question, to see whether the upstream asks more before the call
// itself finishes. An upstream that asks strictly sequentially (ask one, wait
// for the answer, ask the next) will trip this window with a single question
// and get one InputRequiredResult per question — legal, MRTR allows repeated
// input_required rounds. An upstream that fires several questions back-to-back
// (rare, but the spec permits batching) has them coalesced into ONE
// InputRequiredResult. Short on purpose: it only bounds how long a call that
// has ALREADY produced a question lingers hoping for a sibling, never a call
// that will finish normally (that path returns the instant CallTool returns).
const mrtrCollectWindow = 50 * time.Millisecond

// ErrUnknownRequestState is returned by ResumeToolModern when the client's
// requestState matches no parked call — an expired park (the deadline swept
// it), a duplicate retry (the first retry already took the entry), or a forged
// token. The transport maps it to -32602 Invalid params; the upstream is never
// touched.
var ErrUnknownRequestState = errors.New("unknown or expired MRTR requestState")

// MRTROutcome is what one stage of a modern call produced: either the
// upstream's final response, or a batch of questions the client must answer
// before the call can continue.
type MRTROutcome struct {
	// Final is the upstream's final result/error, forwarded to the client
	// verbatim (re-wrapped under the client's id by the transport). Non-nil
	// exactly when InputRequests is empty.
	Final *mcp.Message
	// InputRequests maps a gateway-minted id to the raw request envelope
	// ({"method":...,"params":...}) the client must answer. The same id is the
	// key the client echoes back in inputResponses. Non-empty exactly when the
	// call parked awaiting client input.
	InputRequests map[string]json.RawMessage
	// RequestState is the opaque token the client echoes on retry to resume the
	// parked call. Set exactly when InputRequests is non-empty.
	RequestState string
}

// mrtrQuestion is one intercepted server→client request routed to a waiting
// modern call: the gateway id it was parked under (also the InputRequests key
// and the RouteUpstreamResponse key) and the raw envelope the client sees.
type mrtrQuestion struct {
	gatewayID string
	envelope  json.RawMessage
}

// mrtrWaiter is one modern call in flight on an upstream, ready to intercept
// that upstream's counter-requests. It lives on mrtrWaiters[upstream] for the
// duration the call is unfinished; onUpstreamRequest hands it questions and
// CallToolModern/ResumeToolModern drains them.
type mrtrWaiter struct {
	// clientCaps are the client capabilities THIS request declared (per-request
	// honesty, plan §6.1 п.3): a question whose capability is absent here is
	// refused to the upstream immediately, never handed to the client.
	clientCaps map[string]json.RawMessage
	// questions delivers intercepted questions to the collector. Buffered so
	// onUpstreamRequest (on the upstream's reader goroutine) never blocks; the
	// collector drains it.
	questions chan mrtrQuestion

	// mu guards open and closed. open holds the gateway ids of questions HANDED
	// OUT to the client and not yet answered or refused. The park's deadline is
	// the SOLE deadline for these — a question intercepted for a modern call has
	// NO per-question timer (unlike the legacy generic park), because a modern
	// question's lifetime is the parked call's, arbitrated by the sweep/resume of
	// that one park. On sweep or abandonment the still-open ones are refused; a
	// resume removes the answered ones. mintServerReqID/RouteUpstreamResponse are
	// still the id machinery, so RefusePendingServerReq stays the per-question
	// arbiter.
	mu sync.Mutex
	// closed is set (under mu, by takeOpen) exactly once the waiter is done for
	// good — the call finished (collectMRTR's resultCh branch) or its park was
	// swept. It closes the race between question delivery and call completion
	// arriving on DIFFERENT goroutines (an HTTP upstream's counter-request rides
	// the long-lived GET-SSE reader, a separate goroutine from the POST-response
	// reader that finishes the call): handleMRTRIfWaiting picks a waiter, releases
	// serverReqMu, and only THEN delivers, so between pick and deliver the call may
	// finish and refuseOpenMRTR run on an empty open set. trackOpen consults closed
	// under the same mu it is set on: closed==true means the funnel already ran and
	// the question must be refused by the caller instead of tracked (it would
	// otherwise leak in pendingServerReqs forever). Either takeOpen runs first
	// (closed==true, trackOpen refuses) or trackOpen runs first (question tracked,
	// takeOpen sees and refuses it) — never a window where neither side owns it.
	closed bool
	open   map[string]mrtrOpenQuestion
}

// mrtrOpenQuestion is one question handed to the client and awaiting an answer:
// the gateway id it was parked under and a refuse function (RefusePendingServerReq,
// which unwinds in the spec shape — a soft decline for elicitation, -32601 for
// sampling, rootsExpire for roots), invoked if the park is swept before the
// client answers.
type mrtrOpenQuestion struct {
	gatewayID string
	envelope  json.RawMessage // the raw request the client saw — re-surfaced on a partial retry
	refuse    func(gatewayID, reason string)
}

// mrtrParkedCall is a modern call suspended awaiting client input. It holds the
// live CallTool goroutine's result channel and the still-open waiter, so a
// retry can push answers to the parked questions and keep waiting for the
// upstream's next move. Guarded by serverReqMu.
type mrtrParkedCall struct {
	upstream string
	waiter   *mrtrWaiter
	// result is the channel the detached CallTool goroutine will send its final
	// response on. Shared across resume rounds — the same CallTool runs until it
	// returns.
	result <-chan mrtrCallResult
	// timer sweeps the park if the client never retries (defaultServerReqTimeout).
	timer *time.Timer
	// cancel tears down the detached CallTool's context on a swept park.
	cancel context.CancelFunc
}

// mrtrCallResult is the detached CallTool goroutine's final delivery.
type mrtrCallResult struct {
	resp *mcp.Message
	err  error
}

// CallToolModern runs a tools/call for a modern client, intercepting any
// server→client question the owning upstream asks mid-call. If the call
// finishes without asking, its final response comes back as MRTROutcome.Final.
// If it asks, the call is parked and the questions come back as
// MRTROutcome.InputRequests + a RequestState the client retries with.
//
// clientCaps are the capabilities THIS request declared; a question outside
// them is refused to the upstream immediately (never surfaced to the client).
//
// The call runs on a context derived from the registry's process context, NOT
// the caller's: a modern client's HTTP request completes the moment it receives
// the InputRequiredResult, and its context is cancelled then — but the parked
// CallTool must outlive it to accept the retry (TestMRTRSurvivesRequestScope).
// The park's deadline bounds it instead.
func (r *Registry) CallToolModern(ctx context.Context, name string,
	arguments, meta json.RawMessage, clientCaps map[string]json.RawMessage) (MRTROutcome, error) {
	owner, ok := r.toolOwner(name)
	run := func(c context.Context) (*mcp.Message, error) { return r.CallTool(c, name, arguments, meta) }
	return r.callModern(ctx, owner, ok, clientCaps, run)
}

// GetPromptModern is prompts/get for a modern client — the same MRTR bridge as
// CallToolModern (a prompt an upstream serves may itself elicit/sample), over
// the verbatim GetPrompt proxy. MRTR is permitted on prompts/get by the spec.
func (r *Registry) GetPromptModern(ctx context.Context, name string,
	arguments json.RawMessage, clientCaps map[string]json.RawMessage) (MRTROutcome, error) {
	owner, ok := r.promptOwner(name)
	run := func(c context.Context) (*mcp.Message, error) { return r.GetPrompt(c, name, arguments) }
	return r.callModern(ctx, owner, ok, clientCaps, run)
}

// ReadResourceModern is resources/read for a modern client — the same MRTR
// bridge, over the verbatim ReadResource proxy. MRTR is permitted on
// resources/read by the spec.
func (r *Registry) ReadResourceModern(ctx context.Context, uri string,
	clientCaps map[string]json.RawMessage) (MRTROutcome, error) {
	owner, ok := r.resourceOwner(uri)
	run := func(c context.Context) (*mcp.Message, error) { return r.ReadResource(c, uri) }
	return r.callModern(ctx, owner, ok, clientCaps, run)
}

// callModern is the shared MRTR machinery behind the three permitted methods.
// owner/ownerOK is the upstream the call routes to (from the method's route
// table); run performs the actual verbatim proxy on a given context. If the
// route is unknown, run is executed directly so the underlying method produces
// its own sanitized error/audit exactly as the legacy path would (no waiter,
// nothing to intercept).
func (r *Registry) callModern(ctx context.Context, owner string, ownerOK bool,
	clientCaps map[string]json.RawMessage, run func(context.Context) (*mcp.Message, error)) (MRTROutcome, error) {
	if !ownerOK {
		resp, err := run(ctx)
		if err != nil {
			return MRTROutcome{}, err
		}
		return MRTROutcome{Final: resp}, nil
	}

	waiter := &mrtrWaiter{clientCaps: clientCaps, questions: make(chan mrtrQuestion, 8), open: map[string]mrtrOpenQuestion{}}
	r.registerMRTRWaiter(owner, waiter)

	// Detach from the caller's context: the call must survive the client's HTTP
	// request scope. It is bounded by the park deadline instead (armed only if
	// it actually parks; a call that never asks finishes long before).
	callCtx, cancel := context.WithCancel(r.procCtx)
	resultCh := make(chan mrtrCallResult, 1)
	go func() {
		resp, err := run(callCtx)
		resultCh <- mrtrCallResult{resp: resp, err: err}
	}()

	return r.collectMRTR(owner, waiter, resultCh, cancel)
}

// ResumeToolModern resumes a parked modern call from the client's retry. It
// takes the park atomically (a duplicate retry of the same state finds nothing
// → ErrUnknownRequestState), routes each inputResponse to the upstream question
// it answers (RouteUpstreamResponse rewrites the id back to the upstream's
// own), and waits again — the upstream may now finish, or ask more (the spec
// lets a server return input_required repeatedly). Questions left unanswered by
// this retry stay open and are re-surfaced with a fresh state.
//
// name/clientCaps are carried through only for symmetry with CallToolModern and
// future validation; a resume neither re-resolves the tool (the parked CallTool
// already owns it) nor re-gates already-open questions (their capability was
// checked when they were intercepted).
func (r *Registry) ResumeToolModern(ctx context.Context, name string,
	retry mcp.MRTRRetry, clientCaps map[string]json.RawMessage) (MRTROutcome, error) {
	_ = ctx
	_ = name
	_ = clientCaps
	parked, ok := r.takeMRTRParked(retry.RequestState)
	if !ok {
		return MRTROutcome{}, ErrUnknownRequestState
	}

	// Route each answer to the upstream question it belongs to. The key is the
	// gateway id the client saw in InputRequests; RouteUpstreamResponse rewrites
	// it back to the upstream's original id (invariant §12.3). An answer whose
	// key matches no open question is dropped (the client answered something we
	// did not ask, or a duplicate) — never fatal.
	for gatewayID, answer := range retry.InputResponses {
		parked.waiter.forgetOpen(gatewayID)
		r.RouteUpstreamResponse(gatewayID, &mcp.Message{
			JSONRPC: mcp.Version,
			ID:      mcp.StringID(gatewayID),
			Result:  answer,
		})
	}

	// A PARTIAL retry — the client answered some but not all of the questions
	// this state carried — is re-parked immediately with the remaining ones and
	// a fresh token, rather than waiting on the upstream (which is still blocked
	// on those very answers). The spec SHOULD-re-asks; this is that (plan §6.1).
	if remaining := parked.waiter.stillOpen(); len(remaining) > 0 {
		return r.reparkMRTR(parked.upstream, parked.waiter, parked.result, parked.cancel, remaining), nil
	}

	// Wait for the upstream's next move — final response, or another question.
	return r.collectMRTR(parked.upstream, parked.waiter, parked.result, parked.cancel)
}

// reparkMRTR re-parks a call whose client answered only some of its open
// questions, re-surfacing the rest under a fresh token. The questions are
// already tracked and already have pending entries (they never left); only the
// park record and token are new.
func (r *Registry) reparkMRTR(upstream string, waiter *mrtrWaiter,
	resultCh <-chan mrtrCallResult, cancel context.CancelFunc, remaining []mrtrOpenQuestion) MRTROutcome {
	token := newMRTRState()
	inputRequests := make(map[string]json.RawMessage, len(remaining))
	for _, q := range remaining {
		inputRequests[q.gatewayID] = q.envelope
	}
	parked := &mrtrParkedCall{upstream: upstream, waiter: waiter, result: resultCh, cancel: cancel}
	parked.timer = time.AfterFunc(r.serverReqTimeout, func() { r.sweepMRTRParked(token) })
	r.serverReqMu.Lock()
	r.mrtrParked[token] = parked
	r.serverReqMu.Unlock()
	return MRTROutcome{InputRequests: inputRequests, RequestState: token}
}

// collectMRTR is the shared wait loop of CallToolModern and ResumeToolModern:
// it watches the detached CallTool goroutine and the waiter's question channel,
// and returns as soon as either the call finishes (→ Final) or the collect
// window closes on at least one question (→ InputRequests + a parked state).
//
// The window logic: the FIRST question arms mrtrCollectWindow; the call
// finishing or the window elapsing both end collection. A call that never asks
// blocks only on resultCh — it returns the instant CallTool returns, paying no
// window at all.
func (r *Registry) collectMRTR(upstream string, waiter *mrtrWaiter,
	resultCh <-chan mrtrCallResult, cancel context.CancelFunc) (MRTROutcome, error) {
	var questions []mrtrQuestion
	var windowCh <-chan time.Time

	for {
		select {
		case res := <-resultCh:
			// The call finished. Stop intercepting; any question that raced in
			// after this is on the legacy path (there is no longer a live call to
			// attribute it to). Deregister and cancel — the call is done, its
			// context can go.
			r.deregisterMRTRWaiter(upstream, waiter)
			cancel()
			// The upstream both asked AND answered its own call without our client
			// — impossible for a legacy stand that blocks on the answer, but
			// defended anyway: surface the final and refuse any still-open question
			// so nothing leaks.
			refuseOpenMRTR(waiter, "the tool call finished before the client answered")
			if res.err != nil {
				return MRTROutcome{}, res.err
			}
			return MRTROutcome{Final: res.resp}, nil

		case q := <-waiter.questions:
			questions = append(questions, q)
			if windowCh == nil {
				windowCh = time.After(mrtrCollectWindow)
			}

		case <-windowCh:
			// The window closed with questions in hand: park the call and hand
			// the client its InputRequiredResult. The waiter stays registered so
			// a further question on this upstream (after the client answers these)
			// is still intercepted for the SAME call.
			return r.parkMRTR(upstream, waiter, resultCh, cancel, questions), nil
		}
	}
}

// parkMRTR suspends the call under a fresh opaque token, arms its sweep
// deadline, and returns the questions to the client. Called under no lock;
// takes serverReqMu to record the park.
func (r *Registry) parkMRTR(upstream string, waiter *mrtrWaiter,
	resultCh <-chan mrtrCallResult, cancel context.CancelFunc, questions []mrtrQuestion) MRTROutcome {
	token := newMRTRState()
	inputRequests := make(map[string]json.RawMessage, len(questions))
	for _, q := range questions {
		inputRequests[q.gatewayID] = q.envelope
	}

	parked := &mrtrParkedCall{upstream: upstream, waiter: waiter, result: resultCh, cancel: cancel}
	parked.timer = time.AfterFunc(r.serverReqTimeout, func() {
		r.sweepMRTRParked(token)
	})

	r.serverReqMu.Lock()
	r.mrtrParked[token] = parked
	r.serverReqMu.Unlock()

	return MRTROutcome{InputRequests: inputRequests, RequestState: token}
}

// takeMRTRParked removes a parked call atomically and stops its sweep timer.
// The atomic take is what makes a double retry safe: the second finds nothing
// and gets ErrUnknownRequestState, so the upstream is never answered twice.
func (r *Registry) takeMRTRParked(token string) (*mrtrParkedCall, bool) {
	if token == "" {
		return nil, false
	}
	r.serverReqMu.Lock()
	parked, ok := r.mrtrParked[token]
	if ok {
		delete(r.mrtrParked, token)
	}
	r.serverReqMu.Unlock()
	if ok && parked.timer != nil {
		parked.timer.Stop()
	}
	return parked, ok
}

// sweepMRTRParked gives up on a parked call whose client never retried: it
// refuses every still-open question to the upstream (the shared refuse path),
// cancels the detached CallTool, and forgets the park. A retry arriving after
// this finds nothing → ErrUnknownRequestState. Mirrors sweepServerReqs'
// deadline discipline.
func (r *Registry) sweepMRTRParked(token string) {
	parked, ok := r.takeMRTRParked(token)
	if !ok {
		return // already resumed or swept
	}
	r.log.Debug("MRTR park swept on deadline", "upstream", parked.upstream)
	// The park's deadline is the sole deadline for its questions: refuse every
	// one still open, in its spec shape (RefusePendingServerReq arbitrates — a
	// no-op for any already answered). Then stop intercepting and cancel the
	// detached call.
	r.deregisterMRTRWaiter(parked.upstream, parked.waiter)
	refuseOpenMRTR(parked.waiter, "the modern client did not answer within "+r.serverReqTimeout.String())
	parked.cancel()
}

// refuseOpenMRTR refuses every still-open question of a waiter — used by the
// sweep and by the collector when a call ends with questions still outstanding.
// Each refuse is RefusePendingServerReq (the arbiter), so a question already
// answered or already refused is a no-op.
func refuseOpenMRTR(w *mrtrWaiter, reason string) {
	for _, q := range w.takeOpen() {
		q.refuse(q.gatewayID, reason)
	}
}

// registerMRTRWaiter pushes a waiter onto the upstream's LIFO stack. LIFO: the
// most recently started call is the most likely owner of the next question
// (plan §6.3).
func (r *Registry) registerMRTRWaiter(upstream string, w *mrtrWaiter) {
	r.serverReqMu.Lock()
	r.mrtrWaiters[upstream] = append(r.mrtrWaiters[upstream], w)
	r.serverReqMu.Unlock()
}

// deregisterMRTRWaiter removes a specific waiter from the upstream's stack
// (identity match, not position — a resumed call's waiter may no longer be on
// top). Idempotent: removing an absent waiter is a no-op.
func (r *Registry) deregisterMRTRWaiter(upstream string, w *mrtrWaiter) {
	r.serverReqMu.Lock()
	defer r.serverReqMu.Unlock()
	stack := r.mrtrWaiters[upstream]
	kept := stack[:0]
	for _, x := range stack {
		if x != w {
			kept = append(kept, x)
		}
	}
	if len(kept) == 0 {
		delete(r.mrtrWaiters, upstream)
	} else {
		r.mrtrWaiters[upstream] = kept
	}
}

// topMRTRWaiter returns the most recently registered waiter for an upstream
// whose declared capabilities cover method, or nil. Called from
// onUpstreamRequest under serverReqMu (the caller holds it). LIFO scan so the
// newest compatible call claims the question.
func (r *Registry) topMRTRWaiterLocked(upstream, capability string) *mrtrWaiter {
	stack := r.mrtrWaiters[upstream]
	for i := len(stack) - 1; i >= 0; i-- {
		if _, ok := stack[i].clientCaps[capability]; ok {
			return stack[i]
		}
	}
	return nil
}

// deliverMRTRQuestion parks an intercepted question as a pendingServerReq (so
// the client's answer routes back through RouteUpstreamResponse under the
// upstream's original id) and hands the gateway id + envelope to the waiter.
//
// The pending entry mirrors the generic park in onUpstreamRequest: same
// mintServerReqID, same expire→refuse shape, same deadline. The difference is
// purely the destination — a channel to a waiting modern call instead of the
// serverReqSubs fan-out. The client-facing id and the upstream id stay as
// separate as ever.
func (r *Registry) deliverMRTRQuestion(w *mrtrWaiter, upstream, method string,
	originalID, params json.RawMessage, refuse func(*Registry, string, json.RawMessage, string)) {
	gatewayID := r.mintServerReqID("mrtr-")
	// No per-question timer here (unlike the legacy generic park): the parked
	// call's single deadline covers every question it surfaced, so the sweep is
	// the sole giver-upper. expire is still installed so RouteUpstreamResponse's
	// arbitration and the sweep's RefusePendingServerReq both refuse in the spec
	// shape.
	entry := pendingServerReq{
		upstream:   upstream,
		originalID: originalID,
		expire: func(r *Registry, _, reason string) {
			refuse(r, upstream, originalID, reason)
		},
	}
	r.serverReqMu.Lock()
	r.pendingServerReqs[gatewayID] = entry
	r.serverReqMu.Unlock()
	envelope := mcp.MustParams(mcp.InputRequestEnvelope{Method: method, Params: params})
	if !w.trackOpen(gatewayID, envelope, func(id, reason string) {
		r.RefusePendingServerReq(id, reason)
	}) {
		// The call finished (or its park was swept) between handleMRTRIfWaiting
		// picking this waiter and this delivery — the collector is gone, nobody
		// will drain w.questions, and refuseOpenMRTR already ran on an open set
		// that did not yet contain us. Roll back the pending entry and refuse the
		// upstream ourselves so nothing leaks (this is the closed-race arm; the
		// buffer-full arm below is the other undeliverable case).
		r.serverReqMu.Lock()
		delete(r.pendingServerReqs, gatewayID)
		r.serverReqMu.Unlock()
		go refuse(r, upstream, originalID, "the modern call finished before its question could be delivered")
		return
	}

	// Non-blocking send: the waiter's channel is buffered, and the collector
	// drains it. If it were somehow full, refuse rather than block the upstream
	// reader goroutine — but with a buffer of 8 and one collector this is only a
	// defence.
	select {
	case w.questions <- mrtrQuestion{gatewayID: gatewayID, envelope: envelope}:
	default:
		w.forgetOpen(gatewayID)
		r.serverReqMu.Lock()
		delete(r.pendingServerReqs, gatewayID)
		r.serverReqMu.Unlock()
		go refuse(r, upstream, originalID, "the modern call's question buffer was full")
	}
}

// trackOpen records a question handed to the client as still-open, so the park
// sweep can refuse it if the client never answers. It returns false if the
// waiter has already been closed (the call finished or was swept before this
// question's delivery reached us — see mrtrWaiter.closed): the question is NOT
// tracked, and the caller must refuse it immediately instead of leaving it
// parked forever.
func (w *mrtrWaiter) trackOpen(gatewayID string, envelope json.RawMessage, refuse func(id, reason string)) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	w.open[gatewayID] = mrtrOpenQuestion{gatewayID: gatewayID, envelope: envelope, refuse: refuse}
	return true
}

// stillOpen returns a snapshot of the questions the client has not yet
// answered, without clearing them — used to re-surface a partial retry's
// remaining questions.
func (w *mrtrWaiter) stillOpen() []mrtrOpenQuestion {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]mrtrOpenQuestion, 0, len(w.open))
	for _, q := range w.open {
		out = append(out, q)
	}
	return out
}

// forgetOpen drops a question from the open set (answered by a resume, or
// rolled back on an undeliverable publish). Idempotent.
func (w *mrtrWaiter) forgetOpen(gatewayID string) {
	w.mu.Lock()
	delete(w.open, gatewayID)
	w.mu.Unlock()
}

// takeOpen returns and clears every still-open question — the sweep's set to
// refuse — and marks the waiter closed for good. Closing here (the single funnel
// both refuseOpenMRTR callers pass through: the call-finished branch of
// collectMRTR and sweepMRTRParked) is what makes a delivery racing in on another
// goroutine safe: a trackOpen that runs AFTER this sees closed==true and refuses
// its question itself; a trackOpen that ran BEFORE this has its question in the
// set returned here and refused by the caller. No question is ever orphaned.
func (w *mrtrWaiter) takeOpen() []mrtrOpenQuestion {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	out := make([]mrtrOpenQuestion, 0, len(w.open))
	ids := make([]string, 0, len(w.open))
	for id, q := range w.open {
		out = append(out, q)
		ids = append(ids, id)
	}
	for _, id := range ids {
		delete(w.open, id)
	}
	return out
}

// handleMRTRIfWaiting is the interception hook onUpstreamRequest calls before
// the legacy conveyor. It reports whether a modern call in flight on this
// upstream CLAIMED the question — true means the legacy path must not run.
//
// Claiming is per-request honest: a waiter claims only a method whose capability
// it declared. A waiter that did NOT declare the capability refuses the upstream
// immediately (the spec refuse shape) and still returns true — the modern call
// owns the interception, and letting the question fall through to a
// process-wide legacy subscriber would leak it to a client that never agreed to
// answer. No waiter at all → false, and the legacy path proceeds unchanged.
//
// roots/list is special (as in the legacy pipeline): it consults the shared
// cache first (a hit answers the upstream without bothering the client) and
// otherwise joins the shared single-flight — the modern client sees a
// ListRootsRequest only on a cache miss, and its answer fills the SAME cache.
func (r *Registry) handleMRTRIfWaiting(name, method string, spec *serverReqSpec,
	originalID, params json.RawMessage) bool {
	// Pick the target waiter under serverReqMu, then RELEASE it before any
	// delivery: the roots path takes rootsMu and writeUpstreamResponse takes
	// r.mu, and holding serverReqMu across those would nest three registry locks
	// on one goroutine. The waiter reference stays valid after release — a
	// concurrent deregister only removes it from the stack, never frees it, and a
	// question delivered to a just-finished call's channel is drained/refused by
	// the collector's exit path.
	r.serverReqMu.Lock()
	waiter := r.topMRTRWaiterLocked(name, spec.capability)
	anyWaiter := len(r.mrtrWaiters[name]) > 0
	r.serverReqMu.Unlock()

	if waiter == nil {
		// Distinguish "no modern call here at all" (fall through to legacy) from
		// "a modern call is here but declared other capabilities". The latter must
		// NOT fall through — an incapable modern client's question is refused, not
		// re-offered to some legacy subscriber.
		if !anyWaiter {
			return false
		}
		spec.refuse(r, name, originalID,
			"the modern client did not declare the "+spec.capability+" capability")
		return true
	}

	if method == mcp.MethodRootsList {
		r.deliverMRTRRoots(waiter, name, originalID)
		return true
	}
	r.deliverMRTRQuestion(waiter, name, method, originalID, params, spec.refuse)
	return true
}

// deliverMRTRRoots serves a roots/list intercepted for a modern call through
// the shared roots cache + single-flight, publishing the question to the waiter
// rather than the legacy subscriber fan-out.
//
// Cache hit → the upstream is answered from the cache immediately, and the
// modern client sees nothing (the whole point of the cache when several
// upstreams — modern or legacy — ask the same question). Cache miss → the
// upstream joins rootsWaiters; if it starts the single-flight, the
// ListRootsRequest goes to the modern waiter, parked under rootsFetchID with
// the shared rootsDeliver so the answer both reaches every parked upstream AND
// fills the cache. A concurrent legacy roots/list rides the very same
// single-flight — one client question, shared cache, no second implementation
// (invariant §12.3 / plan §6.1).
func (r *Registry) deliverMRTRRoots(w *mrtrWaiter, upstream string, originalID json.RawMessage) {
	r.rootsMu.Lock()
	if r.rootsCache != nil {
		cached := r.rootsCache
		r.rootsMu.Unlock()
		r.writeUpstreamResponse(upstream, mcp.NewResult(originalID, cached))
		return
	}
	r.rootsWaiters = append(r.rootsWaiters, rootsWaiter{upstream: upstream, originalID: originalID})
	if r.rootsFetchID != "" {
		r.rootsMu.Unlock()
		return // a question is already in flight (to some client); this asker rides along
	}
	gatewayID := r.mintServerReqID(rootsIDPrefix)
	r.rootsFetchID = gatewayID
	r.rootsMu.Unlock()

	// No per-question timer (the park's deadline covers it); the shared
	// rootsDeliver/rootsExpire still own fan-out and single-flight unwinding.
	entry := pendingServerReq{deliver: rootsDeliver, expire: rootsExpire}
	r.serverReqMu.Lock()
	r.pendingServerReqs[gatewayID] = entry
	r.serverReqMu.Unlock()
	envelope := mcp.MustParams(mcp.InputRequestEnvelope{Method: mcp.MethodRootsList})
	if !w.trackOpen(gatewayID, envelope, func(id, reason string) {
		r.RefusePendingServerReq(id, reason)
	}) {
		// Same closed-race arm as deliverMRTRQuestion: the call finished before
		// this roots question could be delivered, and the collector is gone. Unwind
		// the single-flight this asker just STARTED — rootsExpire clears
		// rootsFetchID and refuses every parked rootsWaiter (including this
		// upstream) with -32601, so a later asker starts a fresh question and this
		// one is answered rather than wedged. Delete the pending entry first (as the
		// buffer-full arm does) so rootsExpire's refusals are the sole unwinders.
		r.serverReqMu.Lock()
		delete(r.pendingServerReqs, gatewayID)
		r.serverReqMu.Unlock()
		go rootsExpire(r, gatewayID, "the modern call finished before its roots/list question could be delivered")
		return
	}

	select {
	case w.questions <- mrtrQuestion{gatewayID: gatewayID, envelope: envelope}:
	default:
		w.forgetOpen(gatewayID)
		r.serverReqMu.Lock()
		delete(r.pendingServerReqs, gatewayID)
		r.serverReqMu.Unlock()
		go rootsExpire(r, gatewayID, "the modern call's question buffer was full")
	}
}

// newMRTRState mints an opaque, unguessable park token. It is a random
// reference into an in-memory table, not a signed blob: a forged token simply
// misses the table (ErrUnknownRequestState), affecting only that request. The
// spec lets a server encode requestState however it likes.
func newMRTRState() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// toolOwner resolves the upstream that owns a client-facing tool name, without
// the call machinery CallTool wraps around the same lookup. ok=false means no
// route (an unknown tool) — CallToolModern then defers to CallTool for the
// identical sanitized error and audit.
func (r *Registry) toolOwner(namespaced string) (string, bool) {
	r.mu.RLock()
	rt, ok := r.toolRoute[namespaced]
	r.mu.RUnlock()
	if !ok {
		return "", false
	}
	return rt.upstream, true
}

// promptOwner resolves the upstream that owns a client-facing prompt name.
func (r *Registry) promptOwner(namespaced string) (string, bool) {
	r.mu.RLock()
	rt, ok := r.promptRoute[namespaced]
	r.mu.RUnlock()
	if !ok {
		return "", false
	}
	return rt.upstream, true
}

// resourceOwner resolves the upstream that owns a resource URI, matching the
// exact-then-template resolution the read path uses.
func (r *Registry) resourceOwner(uri string) (string, bool) {
	return r.resolveResourceOwner(uri)
}

// closeMRTR is Close's share of the MRTR state: forget every parked call and
// stop its sweep timer, cancelling the detached CallTool so no goroutine
// outlives shutdown. Waiters are dropped with it. Nobody is refused on the way
// out — the upstreams are being torn down with the gateway (same reasoning as
// closeServerReqs).
func (r *Registry) closeMRTR() {
	r.serverReqMu.Lock()
	parked := r.mrtrParked
	r.mrtrParked = map[string]*mrtrParkedCall{}
	r.mrtrWaiters = map[string][]*mrtrWaiter{}
	r.serverReqMu.Unlock()
	for _, p := range parked {
		if p.timer != nil {
			p.timer.Stop()
		}
		if p.cancel != nil {
			p.cancel()
		}
	}
}
