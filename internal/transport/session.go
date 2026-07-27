package transport

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"
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

// httpSession is the state of one client HTTP session. Every field except id
// and done is read and written ONLY under sessionStore.mu.
type httpSession struct {
	id string // the Mcp-Session-Id value (immutable)

	client string // "name/version" from initialize → CallRecord.Client

	// caps holds the server→client capabilities (elicitation/sampling/roots)
	// this client declared in its initialize. Stored for Stage 17,
	// deliberately unused in Stage 16: the HTTP transport still declares
	// nothing to upstreams (newHTTPServer passes serverRequests=false), so
	// per-session capability declaration is not wired yet. Do NOT delete this
	// as dead code — it is the hook Stage 17 attaches to.
	caps map[string]json.RawMessage

	lastSeen time.Time

	// streams counts open GET SSE streams of this session; while > 0 the
	// session cannot expire — an open stream IS activity, and cutting a live
	// listener on an idle timer would be wrong.
	streams int

	// done is closed exactly once, when the session is removed from the store
	// (DELETE or expiry). handleSSE selects on it to end the stream.
	done chan struct{}
}

// sessionStore is httpServer's map of live sessions. One mutex guards the
// whole store rather than one per session: every operation is O(1) under the
// lock, handlers run concurrently, and a single lock makes the "no field of
// httpSession is touched outside it" rule checkable by eye (and by -race).
type sessionStore struct {
	mu       sync.Mutex
	sessions map[string]*httpSession
	idleTTL  time.Duration    // = sessionIdleTimeout; tests shorten it
	max      int              // = maxSessions; tests shrink it
	now      func() time.Time // = time.Now; test hook for expiry without sleeping
}

func newSessionStore() *sessionStore {
	return &sessionStore{
		sessions: map[string]*httpSession{},
		idleTTL:  sessionIdleTimeout,
		max:      maxSessions,
		now:      time.Now,
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
// hold off expiry while they live.
func (st *sessionStore) streamStarted(s *httpSession) {
	st.mu.Lock()
	defer st.mu.Unlock()
	s.streams++
	s.lastSeen = st.now()
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
func (st *sessionStore) removeLocked(s *httpSession) {
	delete(st.sessions, s.id)
	close(s.done)
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
