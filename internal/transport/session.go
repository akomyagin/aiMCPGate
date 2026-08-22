package transport

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// Stage 16 — server-side sessions for the client-facing HTTP transport.
//
// This is the SERVER half of Mcp-Session-Id: the gateway issues an id on
// initialize and requires it on every later request. Do not confuse it with
// internal/upstream/http.go, which is the CLIENT half — there the gateway
// captures the id an HTTP UPSTREAM issued to it. Same header, opposite ends of
// the wire; the terminology (sessionID, "echo", "reset on 404") is kept
// deliberately parallel between the two, and so is the semantics a 404 carries:
// the session is gone, re-initialize.
//
// Why the transport owns this and the dispatcher does not: a session is a
// property of the HTTP framing (there is exactly one implicit session over
// stdio — the process itself), so dispatch.go stays transport-independent.

// sessionIdleTimeout is how long a session may go without any request before
// it is considered dead. It is a constant, not a config knob, following the
// precedent set by the Stage 15 server→request deadline: one more YAML field
// nobody would ever tune is worse than a defensible default. Thirty minutes is
// generous — a real client session lives for hours but talks far more often
// than that — and expiry is not fatal: the client gets 404 and, per spec,
// silently starts a new session.
const sessionIdleTimeout = 30 * time.Minute

// maxSessions caps how many live sessions the store holds, so a client
// spamming initialize cannot grow the map without bound inside the idle
// window (the same class of care as maxRequestBodyBytes). 256 is orders of
// magnitude above real use — the gateway is single-user by design, but not
// single-session. Overflow is answered with 503 rather than by evicting the
// oldest session: silently killing someone's live session is worse than
// honestly refusing a new one.
const maxSessions = 256

// maxStreamsPerSession caps how many open GET SSE streams a single session
// may hold at once (security brainstorm 2026-08-20, S8): nothing previously
// bounded this, so one Mcp-Session-Id spamming GET /mcp could grow
// httpSession.streams and .reqStreams without limit, each open stream
// costing a goroutine (handleSSE's own, parked in its select loop) plus two
// registry subscriptions and a buffered channel. 16 is well above any real
// client's reconnect/multi-tab overlap — the gateway is single-user by
// design — while still bounding the cost of one misbehaving or hostile
// client. Same overflow answer as maxSessions: 503, not silently dropping an
// existing stream to make room.
const maxStreamsPerSession = 16

// httpSession is the state of one client HTTP session. Every field except id
// and done is read and written ONLY under sessionStore.mu.
type httpSession struct {
	id string // the Mcp-Session-Id value (immutable)

	client string // "name/version" from initialize → CallRecord.Client

	// caps holds the server→client capabilities (elicitation/sampling/roots)
	// this client declared in its initialize. Since Stage 17a it is the
	// TRANSPORT-side delivery gate: an upstream's server→client request may be
	// handed only to a session that declared the matching capability, which is
	// what keeps several concurrent HTTP clients honest even though the
	// registry's own gate is process-wide (pinned by the first session — see
	// httpServer.ensureRegistry).
	caps map[string]json.RawMessage

	lastSeen time.Time

	// streams counts open GET SSE streams of this session; while > 0 the
	// session cannot expire — an open stream IS activity, and cutting a live
	// listener on an idle timer would be wrong.
	streams int

	// reqStreams are the delivery channels of this session's open GET SSE
	// streams, in registration order (Stage 17a). Distinct from the streams
	// tally above because a server→client REQUEST is unicast: it goes to ONE
	// stream of ONE session, unlike catalog/upstream notifications, which every
	// stream gets from its own registry subscription. The newest stream is
	// preferred on delivery — a reconnecting client leaves its predecessor
	// half-dead in OS buffers for a while, and the fresh one is the likelier
	// live reader.
	reqStreams []chan *mcp.Message

	// pendingReqs is the reverse index of sessionStore.reqOwners: the
	// gateway-minted ids of the server→client requests delivered to this
	// session and not yet answered. It exists so removeLocked can hand exactly
	// those ids over to be refused when the session dies, without walking the
	// whole owner map.
	pendingReqs map[string]struct{}

	// done is closed exactly once, when the session is removed from the store
	// (DELETE or expiry). handleSSE selects on it to end the stream.
	done chan struct{}
}

// sessionStore is httpServer's map of live sessions. One mutex guards the
// whole store rather than one per session: every operation is O(1) under the
// lock, handlers run concurrently, and a single lock makes the "no field of
// httpSession is touched outside it" rule checkable by eye (and by -race).
type sessionStore struct {
	mu         sync.Mutex
	sessions   map[string]*httpSession
	idleTTL    time.Duration    // = sessionIdleTimeout; tests shorten it
	max        int              // = maxSessions; tests shrink it
	maxStreams int              // = maxStreamsPerSession; tests shrink it
	now        func() time.Time // = time.Now; test hook for expiry without sleeping

	// reqOwners maps a gateway-minted server→client request id to the session
	// it was delivered to (Stage 17a). It lives HERE, under the same mutex as
	// the sessions themselves, rather than in a tracker of its own: choosing a
	// target stream and recording who owns the request must be ONE atomic step,
	// or a client answering the instant it reads the SSE event could POST back
	// before the ownership record existed and be rejected as a stranger.
	//
	// Ownership is checked strictly on the way back (takeServerReq): without
	// it, any holder of a valid session could answer somebody else's
	// elicitation just by guessing the short, predictable id ("elicit-1").
	reqOwners map[string]*httpSession

	// orphaned collects the request ids whose session has just left the map,
	// for the caller to refuse AFTER releasing mu. Refusing inline is not an
	// option: it calls into the registry, which takes its own serverReqMu and
	// fans writes out to upstreams — a lock inversion plus a blocking-ish call
	// under this mutex, both forbidden here.
	orphaned []string

	// onServerReqOrphaned refuses one abandoned request (wired to
	// Registry.RefusePendingServerReq). Set once, before the server starts
	// accepting, and only ever CALLED with mu released.
	onServerReqOrphaned func(gatewayID string)
}

func newSessionStore() *sessionStore {
	return &sessionStore{
		sessions:   map[string]*httpSession{},
		reqOwners:  map[string]*httpSession{},
		idleTTL:    sessionIdleTimeout,
		max:        maxSessions,
		maxStreams: maxStreamsPerSession,
		now:        time.Now,
	}
}

// newSessionID mints a session id: 16 crypto-random bytes as 32 hex chars.
// The spec requires the id to be globally unique, cryptographically secure and
// visible ASCII only. A crypto/rand failure is unrecoverable (no entropy means
// no unguessable ids), so it panics rather than returning a weak id.
func newSessionID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("transport: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(buf[:])
}

// create registers a new session, first sweeping every expired one that has no
// open stream. ok=false means the store is full (§ maxSessions) and the caller
// must answer 503.
func (st *sessionStore) create(client string, caps map[string]json.RawMessage) (*httpSession, bool) {
	// Registered BEFORE the lock so it runs AFTER the deferred Unlock (defers
	// are LIFO): drainOrphaned takes mu itself and calls into the registry, so
	// it must never run while this method still holds it. Same pattern in touch
	// and terminate — the three methods that can drop a session.
	defer st.drainOrphaned()
	st.mu.Lock()
	defer st.mu.Unlock()

	st.sweepLocked()
	if len(st.sessions) >= st.max {
		return nil, false
	}
	s := &httpSession{
		id:       newSessionID(),
		client:   client,
		caps:     caps,
		lastSeen: st.now(),
		done:     make(chan struct{}),
	}
	st.sessions[s.id] = s
	return s, true
}

// touch looks a session up by id and marks it active. An expired session (no
// open stream, lastSeen older than idleTTL) is removed — and its done closed —
// and reported as missing, so the caller answers 404 exactly as it would for
// an id it never issued.
//
// client is returned BY VALUE on purpose: httpSession fields must not be read
// outside the lock, so the caller gets a copy instead of a pointer chase.
func (st *sessionStore) touch(id string) (*httpSession, string, bool) {
	defer st.drainOrphaned() // after Unlock — see create
	st.mu.Lock()
	defer st.mu.Unlock()

	s, ok := st.sessions[id]
	if !ok {
		return nil, "", false
	}
	if st.expiredLocked(s) {
		st.removeLocked(s)
		return nil, "", false
	}
	s.lastSeen = st.now()
	return s, s.client, true
}

// reinit refreshes the identity of an existing session: a client re-sending
// initialize over a live session keeps its id but may report new clientInfo
// and capabilities (mirroring the stdio dispatcher's re-initialize).
//
// It reports ok=false if s is no longer the session live in the store — NOT
// merely "some session with this id exists", but this exact *httpSession by
// pointer identity. This matters because of a real race in handlePost: touch
// looks the session up and releases the mutex, then the caller reads and
// decodes the request body (up to maxRequestBodyBytes, unbounded in wall time
// against a slow client) before ever calling reinit. A concurrent DELETE
// /mcp in that window removes the session from the map and closes its done
// channel. Without this check, reinit would happily resurrect the dead
// *httpSession by mutating it in place — the map would still lack the entry,
// but the caller would go on to answer 200 with a freshly echoed
// Mcp-Session-Id for a session that no longer exists, and every following
// request against that id would then 404. Comparing st.sessions[s.id] == s
// (pointer identity, not just key presence) is what catches this: a
// same-id-different-pointer session created after the DELETE must also be
// rejected here rather than silently overwritten.
func (st *sessionStore) reinit(s *httpSession, client string, caps map[string]json.RawMessage) bool {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.sessions[s.id] != s {
		return false
	}
	s.client = client
	s.caps = caps
	s.lastSeen = st.now()
	return true
}

// terminate removes a session and closes its done channel (ending its SSE
// streams). false means the id is unknown — already terminated, expired, or
// never issued.
func (st *sessionStore) terminate(id string) bool {
	defer st.drainOrphaned() // after Unlock — see create
	st.mu.Lock()
	defer st.mu.Unlock()

	s, ok := st.sessions[id]
	if !ok {
		return false
	}
	st.removeLocked(s)
	return true
}

// streamStarted / streamEnded track open GET SSE streams of a session, which
// hold off expiry while they live. streamStarted reports false, without
// incrementing the tally, once the session already holds maxStreamsPerSession
// open streams — the caller must not proceed to open one (§ maxStreamsPerSession).
func (st *sessionStore) streamStarted(s *httpSession) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if s.streams >= st.maxStreams {
		return false
	}
	s.streams++
	s.lastSeen = st.now()
	return true
}

func (st *sessionStore) streamEnded(s *httpSession) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if s.streams > 0 {
		s.streams--
	}
	s.lastSeen = st.now()
}

// expiredLocked reports whether s has been idle past the TTL. An open stream
// pins the session alive regardless of lastSeen.
func (st *sessionStore) expiredLocked(s *httpSession) bool {
	if s.streams > 0 {
		return false
	}
	return st.now().Sub(s.lastSeen) >= st.idleTTL
}

// removeLocked deletes s from the map and closes its done channel. This is the
// ONLY place done is closed, which makes a double close structurally
// impossible: a session is closed exactly when it leaves the map, and it can
// leave the map only once.
//
// It is also the one place a session's still-unanswered server→client requests
// are given up on (Stage 17a): the client that was asked is gone, so waiting
// out the registry's five-minute deadline would leave an upstream blocked in
// its tools/call for minutes over an answer that can no longer arrive. The ids
// are only MOVED here (to orphaned) — refusing them means calling the registry,
// which must happen with mu released; see drainOrphaned.
func (st *sessionStore) removeLocked(s *httpSession) {
	delete(st.sessions, s.id)
	for id := range s.pendingReqs {
		delete(st.reqOwners, id)
		st.orphaned = append(st.orphaned, id)
	}
	s.pendingReqs = nil
	close(s.done)
}

// drainOrphaned refuses every server→client request left behind by a session
// that has just been removed. It takes mu itself only to lift the ids out, and
// calls the callback with NO lock held — the callback reaches into the registry
// (serverReqMu, upstream writes), which must never happen under this mutex.
func (st *sessionStore) drainOrphaned() {
	st.mu.Lock()
	ids := st.orphaned
	st.orphaned = nil
	st.mu.Unlock()

	if st.onServerReqOrphaned == nil {
		return // no transport wired in (unit tests, or a store used bare)
	}
	for _, id := range ids {
		st.onServerReqOrphaned(id)
	}
}

// addReqStream / removeReqStream register one open GET SSE stream's delivery
// channel with its session. They are the unicast counterpart of
// streamStarted/streamEnded (which only count streams for the idle timer);
// handleSSE calls both pairs, so the tally and the channel list stay in step.
func (st *sessionStore) addReqStream(s *httpSession, ch chan *mcp.Message) {
	st.mu.Lock()
	defer st.mu.Unlock()
	s.reqStreams = append(s.reqStreams, ch)
}

// removeReqStream deregisters ch. After it returns, deliverServerReq can no
// longer pick ch — which is what makes the handler's own drain of whatever is
// still buffered in it (refuseQueued) race-free: no new send can arrive.
func (st *sessionStore) removeReqStream(s *httpSession, ch chan *mcp.Message) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for i, c := range s.reqStreams {
		if c == ch {
			s.reqStreams = append(s.reqStreams[:i], s.reqStreams[i+1:]...)
			return
		}
	}
}

// deliverServerReq hands one upstream-initiated request to exactly ONE open SSE
// stream of exactly ONE session, and records that session as the owner of
// gatewayID. It reports false when there is nobody to ask, in which case the
// caller refuses the upstream in the shape its spec prescribes.
//
// Everything happens under a single hold of mu, by design: the ownership record
// must exist before the event can possibly reach the client, or a client
// answering immediately would POST an id nobody owns yet and be dropped.
//
// Target choice. Candidates are the live sessions that declared capability AND
// have at least one open stream; among them the most recently active wins (an
// actively talking client is the likelier live one), and within a session the
// newest stream is tried first (see httpSession.reqStreams). Every send is
// strictly non-blocking — a full buffer means that reader is not keeping up, so
// the next stream (then the next session) is tried instead of stalling every
// other handler on this mutex. Exactly one send can succeed: the first one
// returns.
func (st *sessionStore) deliverServerReq(capability, gatewayID string, msg *mcp.Message) bool {
	st.mu.Lock()
	defer st.mu.Unlock()

	var candidates []*httpSession
	for _, s := range st.sessions {
		if _, ok := s.caps[capability]; !ok {
			continue // this client cannot answer this kind of request
		}
		if len(s.reqStreams) == 0 {
			continue // no channel to push it down
		}
		if st.expiredLocked(s) {
			continue // idle past the TTL; it is about to be swept anyway
		}
		candidates = append(candidates, s)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].lastSeen.After(candidates[j].lastSeen)
	})

	for _, s := range candidates {
		for i := len(s.reqStreams) - 1; i >= 0; i-- {
			select {
			case s.reqStreams[i] <- msg:
				st.reqOwners[gatewayID] = s
				if s.pendingReqs == nil {
					s.pendingReqs = map[string]struct{}{}
				}
				s.pendingReqs[gatewayID] = struct{}{}
				return true
			default: // that reader is behind — try the next stream
			}
		}
	}
	return false
}

// takeServerReq claims gatewayID on behalf of s: true only when s is the
// session the request was actually delivered to, in which case the ownership
// record is consumed (one answer per request).
//
// It answers "whose session is this", NOT "who gets to answer the upstream" —
// that arbitration belongs to the registry's pending map alone
// (takePendingServerReq). A false here means the id is unknown, already
// answered, or belongs to somebody else; the caller treats all three the same
// way (drop, same HTTP status), so a stranger cannot probe which ids exist.
func (st *sessionStore) takeServerReq(s *httpSession, gatewayID string) bool {
	st.mu.Lock()
	defer st.mu.Unlock()

	owner, ok := st.reqOwners[gatewayID]
	if !ok || owner != s {
		return false
	}
	delete(st.reqOwners, gatewayID)
	delete(owner.pendingReqs, gatewayID)
	return true
}

// trackedServerReqIDs snapshots every id the store still believes is awaiting
// an answer. Paired with dropServerReqs it is the lazy cleanup for requests the
// REGISTRY has already given up on (its own five-minute deadline): the registry
// removes its entry silently, and without this sweep the transport's ownership
// record would survive until the session died. Lazy, like sweepLocked, and for
// the same reason — a janitor goroutine would only add nondeterminism.
func (st *sessionStore) trackedServerReqIDs() []string {
	st.mu.Lock()
	defer st.mu.Unlock()

	if len(st.reqOwners) == 0 {
		return nil
	}
	ids := make([]string, 0, len(st.reqOwners))
	for id := range st.reqOwners {
		ids = append(ids, id)
	}
	return ids
}

// dropServerReqs forgets the given ownership records without refusing anything:
// the only caller passes ids the registry has already finished with, so there
// is nobody left to answer to.
func (st *sessionStore) dropServerReqs(ids []string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, id := range ids {
		owner, ok := st.reqOwners[id]
		if !ok {
			continue
		}
		delete(st.reqOwners, id)
		delete(owner.pendingReqs, id)
	}
}

// sweepLocked drops every expired session. Cleanup is lazy — done here (on
// create) and in touch — instead of by a janitor goroutine: a background timer
// would add a source of nondeterminism to tests and to shutdown for no real
// gain, since a dead record costs tens of bytes until the next initialize.
func (st *sessionStore) sweepLocked() {
	for _, s := range st.sessions {
		if st.expiredLocked(s) {
			st.removeLocked(s)
		}
	}
}
