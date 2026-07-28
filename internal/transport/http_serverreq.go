package transport

// Stage 17a — server→client REQUESTS over the client-facing HTTP transport:
// an upstream asks something mid-call (elicitation/create,
// sampling/createMessage, roots/list), the gateway must put that question to
// its own client and route the answer back. Stage 15 built the whole
// transport-agnostic pipeline (internal/registry/serverreq.go); Stage 16 built
// the sessions this leans on. What is left, and lives here, is the HTTP
// framing of it.
//
// Why ONE subscription for the whole server, with delivery decided here,
// instead of a subscription per SSE stream (the way catalog changes and
// forwarded upstream notifications work): those are BROADCAST facts — every
// listener wants them, and duplicates are harmless. A request is not: it
// expects exactly one answer, so it must reach exactly one stream of exactly
// one session. Handing every stream its own subscription would either deliver
// the same question to several clients (several answers, one arbitrated
// winner, the rest silently discarded — and a human answering a form for
// nothing) or degenerate into "whoever's buffer won the race", which is not a
// decision anyone can reason about. So: one subscriber, one router goroutine,
// and an explicit choice of target under the session store's mutex.
//
// The answer travels back on an ordinary POST carrying the same
// Mcp-Session-Id (handlePost recognizes a JSON-RPC response with a string id
// BEFORE dispatch) — the spec's own shape for a client answering a server
// request over Streamable HTTP; no SSE response stream is introduced
// (docs/MCP_NOTES.md §8).

import (
	"context"
	"encoding/json"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
	"github.com/akomyagin/aiMCPGate/internal/registry"
)

// serverReqStreamBuffer is how many undelivered server→client requests one SSE
// stream may hold. Small on purpose: these are interactive questions with a
// human or an LLM behind them, and a stream that is already 8 questions behind
// is not going to catch up — the router falls through to another stream (or
// refuses in the spec's shape), which an upstream can act on immediately,
// instead of queueing work nobody will get to.
const serverReqStreamBuffer = 8

// ensureRegistry starts the registry exactly once, for the whole process,
// pinning the server→client capabilities to declare upstream BEFORE it does.
// Every concurrent caller blocks inside Do and then reads the same startErr,
// which sync.Once publishes safely.
//
// caps come from the FIRST initialize that gets here, and that set is what the
// upstream handshakes carry for the life of the process. This is a deliberate
// limitation, not an oversight: MCP 2025-06-18 has no re-negotiation, so a
// second client declaring more cannot change what was already declared — and
// declaring a capability to an upstream on behalf of a client that may be gone
// by the time it is used would be exactly the dishonesty Stage 15 removed. A
// capability only the second client declared is therefore refused by the
// registry's gate, which is honest: that upstream was never told about it, so a
// spec-abiding one will not ask at all.
//
// The bring-up runs under s.baseCtx (Serve's context), never a request's — see
// the field.
func (s *httpServer) ensureRegistry(caps map[string]json.RawMessage) error {
	s.startOnce.Do(func() {
		s.reg.SetClientServerRequestCaps(caps) // strictly BEFORE Start
		s.startErr = s.reg.Start(s.baseCtx)
		if s.startErr != nil {
			// Start only fails when the gateway cannot proceed at all or every
			// upstream failed — a misconfiguration, deliberately not a degraded
			// mode (see Registry.Start). Hand it to Serve, which ends the whole
			// server with it; the caller answers its own client meanwhile.
			s.log.Error("registry start failed", "err", s.startErr)
			select {
			case s.startFailed <- s.startErr:
			default: // Serve already has one, or is gone: nothing to add
			}
			return
		}
		s.log.Info("http transport ready", "tools", s.reg.ToolCount())
	})
	return s.startErr
}

// routeServerRequests owns the single SubscribeUpstreamRequests channel: for
// each upstream-initiated request it picks one session's SSE stream and hands
// the request over, or refuses it in the shape the registry's spec table
// prescribes ({"action":"decline"} for elicitation, -32601 for the others —
// the transport never encodes that difference itself).
//
// It runs from Serve's prologue until ctx is done.
func (s *httpServer) routeServerRequests(ctx context.Context, reqs <-chan registry.UpstreamRequest) {
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-reqs:
			if !ok {
				return
			}
			s.routeOneServerRequest(req)
		}
	}
}

// routeOneServerRequest delivers a single upstream request, or refuses it.
func (s *httpServer) routeOneServerRequest(req registry.UpstreamRequest) {
	// Lazily drop the ownership records of requests the REGISTRY has already
	// given up on (its own five-minute deadline fires without telling anyone).
	// Done here, on the next request, rather than by a janitor goroutine —
	// the same lazy-cleanup choice sessionStore.sweepLocked makes, and for the
	// same reason: a background timer buys nothing and costs determinism.
	s.sweepServerReqs()

	capability, ok := registry.CapabilityForMethod(req.Method)
	if !ok {
		// Unreachable: the registry publishes only methods that HAVE a spec
		// entry. Refuse rather than drop anyway — a silent drop would park the
		// entry until the deadline, and for roots/list a single parked entry
		// wedges the single-flight for the life of the process.
		s.log.Warn("server→client request with no known capability, refusing",
			"method", req.Method, "id", req.GatewayID)
		s.reg.RefusePendingServerReq(req.GatewayID, "the gateway does not proxy "+req.Method)
		return
	}

	// Byte-for-byte what the stdio transport writes: the gateway-minted string
	// id, the upstream's method, its params verbatim. The upstream's own id
	// never travels.
	msg := mcp.NewRequest(mcp.StringID(req.GatewayID), req.Method, req.Params)
	if s.sessions.deliverServerReq(capability, req.GatewayID, msg) {
		s.log.Debug("server→client request delivered to a session stream",
			"method", req.Method, "id", req.GatewayID)
		return
	}
	// Nobody declared this capability with a stream open to receive it. Refuse
	// at once so the upstream is not left inside its tools/call waiting out the
	// registry's deadline.
	s.reg.RefusePendingServerReq(req.GatewayID,
		"no live session declared "+capability+" with an open SSE stream")
}

// sweepServerReqs forgets the ownership records of requests the registry no
// longer has pending. Nothing is refused: whoever removed the entry there has
// already answered the upstream.
func (s *httpServer) sweepServerReqs() {
	tracked := s.sessions.trackedServerReqIDs()
	var stale []string
	for _, id := range tracked {
		if !s.reg.ServerReqPending(id) {
			stale = append(stale, id)
		}
	}
	if len(stale) > 0 {
		s.sessions.dropServerReqs(stale)
	}
}

// refuseServerReq gives up on a request whose delivery failed after the store
// had already committed to it, releasing both records: the transport's
// ownership entry and the registry's parked request.
//
// The two are taken in that order and the registry's take is the one that
// decides: if the client answered in the same instant, exactly one of the two
// paths gets the pending entry (takePendingServerReq arbitrates), so the
// upstream receives exactly one response. takeServerReq only settles "whose
// session was it", never "who answers".
func (s *httpServer) refuseServerReq(sess *httpSession, m *mcp.Message, reason string) {
	var gatewayID string
	if json.Unmarshal(m.ID, &gatewayID) != nil {
		return // not a string id: not one of ours (unreachable — we minted it)
	}
	s.sessions.takeServerReq(sess, gatewayID)
	s.reg.RefusePendingServerReq(gatewayID, reason)
}

// refuseQueued drains whatever is still sitting in a closing stream's channel
// and refuses each of it. Called from handleSSE's defer, AFTER
// removeReqStream: once the channel is deregistered under the store's mutex no
// further send can arrive, so this drain is complete — every request that was
// handed to this stream but never written to the wire is accounted for, and no
// upstream is left waiting on a reader that has gone.
func (s *httpServer) refuseQueued(sess *httpSession, reqCh chan *mcp.Message) {
	for {
		select {
		case m := <-reqCh:
			s.refuseServerReq(sess, m, "the client's SSE stream closed before the request was sent")
		default:
			return
		}
	}
}
