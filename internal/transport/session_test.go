package transport

// Stage 16 — unit tests for the HTTP session store (session.go). These drive
// the store directly, with the now() hook standing in for the clock, so expiry
// and sweeping are tested deterministically without sleeping. The end-to-end
// HTTP behaviour built on top lives in http_session_test.go.

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// testStore returns a store with a controllable clock. The returned advance
// function moves that clock forward.
func testStore(t *testing.T, ttl time.Duration, max int) (*sessionStore, func(time.Duration)) {
	t.Helper()
	st := newSessionStore()
	st.idleTTL = ttl
	st.max = max
	clock := time.Now()
	st.now = func() time.Time { return clock }
	return st, func(d time.Duration) { clock = clock.Add(d) }
}

// streamCount reads s.streams under the store lock. Test-only accessor: no
// field of httpSession may be touched outside sessionStore.mu, and an e2e test
// needs to observe the tally dropping back to zero after a stream closes —
// handleSSE's deferred streamEnded runs in the handler goroutine, some time
// after the client hangs up, so it has to be polled rather than assumed.
func (st *sessionStore) streamCount(s *httpSession) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return s.streams
}

// isClosed reports whether ch is already closed, without blocking.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestSessionStoreCreateTouchTerminate(t *testing.T) {
	st, _ := testStore(t, time.Minute, 8)

	sess, ok := st.create("cli/1.0.0", map[string]json.RawMessage{"elicitation": json.RawMessage(`{}`)})
	if !ok {
		t.Fatal("create returned ok=false on an empty store")
	}
	if sess.id == "" {
		t.Fatal("created session has an empty id")
	}
	if isClosed(sess.done) {
		t.Fatal("done must be open on a fresh session")
	}

	got, client, ok := st.touch(sess.id)
	if !ok {
		t.Fatal("touch of a live session returned ok=false")
	}
	if got != sess {
		t.Fatal("touch returned a different session object")
	}
	if client != "cli/1.0.0" {
		t.Fatalf("touch client = %q, want %q", client, "cli/1.0.0")
	}

	if !st.terminate(sess.id) {
		t.Fatal("terminate of a live session returned false")
	}
	if !isClosed(sess.done) {
		t.Fatal("terminate must close done (open SSE streams end on it)")
	}
	if _, _, ok := st.touch(sess.id); ok {
		t.Fatal("touch after terminate returned ok=true (session still in the map?)")
	}
	if st.terminate(sess.id) {
		t.Fatal("second terminate of the same id returned true, want false (404)")
	}
}

func TestSessionStoreExpiry(t *testing.T) {
	const ttl = 30 * time.Minute
	st, advance := testStore(t, ttl, 8)

	// A session left alone past the TTL is gone, and its done is closed so any
	// stream would end.
	stale, _ := st.create("stale/1", nil)
	advance(ttl + time.Second)
	if _, _, ok := st.touch(stale.id); ok {
		t.Fatal("touch after the idle TTL returned ok=true, want expired")
	}
	if !isClosed(stale.done) {
		t.Fatal("expiry must close done, exactly like terminate")
	}

	// A session that keeps talking is kept alive: touching at 0.75*TTL resets
	// the clock, so at 1.5*TTL since creation it is still only 0.75*TTL idle.
	live, _ := st.create("live/1", nil)
	advance(ttl * 3 / 4)
	if _, _, ok := st.touch(live.id); !ok {
		t.Fatal("touch before the TTL returned ok=false")
	}
	advance(ttl * 3 / 4)
	if _, _, ok := st.touch(live.id); !ok {
		t.Fatal("touch did not refresh lastSeen: the session expired despite continuous activity")
	}
}

func TestSessionStoreOpenStreamBlocksExpiry(t *testing.T) {
	const ttl = 10 * time.Minute
	st, advance := testStore(t, ttl, 8)

	sess, _ := st.create("streamer/1", nil)
	st.streamStarted(sess)

	advance(ttl * 5) // long past the idle TTL, but a stream is open
	if _, _, ok := st.touch(sess.id); !ok {
		t.Fatal("session with an open SSE stream expired: an open stream IS activity")
	}

	st.streamEnded(sess)
	advance(ttl + time.Second)
	if _, _, ok := st.touch(sess.id); ok {
		t.Fatal("session did not expire after its last stream ended")
	}
}

// TestSessionStoreStreamEndedRestoresMortality guards the decrement in
// streamEnded itself, which TestSessionStoreOpenStreamBlocksExpiry does not
// pin down on its own: that test's final touch happens after BOTH
// streamEnded AND a fresh TTL-sized advance, so a streamEnded that failed to
// decrement streams (leaving the session permanently immortal — the exact map
// leak the idle TTL exists to prevent) would still make that touch report
// "expired", for the wrong reason: expiredLocked would fall through to the
// lastSeen check because streams staying stuck > 0 is never itself asserted.
// This test isolates the decrement: prove the session is alive with the
// stream nominally still open (streams>0 blocking an already-past-TTL
// lastSeen), then call streamEnded and — WITHOUT refreshing lastSeen again —
// advance the clock a second time past idleTTL and touch once more. That
// second touch can only see "expired" if streamEnded actually brought streams
// back to 0; a no-op streamEnded would still block expiry via the same
// streams>0 branch and the session would remain immortal.
func TestSessionStoreStreamEndedRestoresMortality(t *testing.T) {
	const ttl = 10 * time.Minute
	st, advance := testStore(t, ttl, 8)

	sess, _ := st.create("streamer/1", nil)
	st.streamStarted(sess)

	advance(ttl * 5) // long past idle TTL, but the stream is still open
	if _, _, ok := st.touch(sess.id); !ok {
		t.Fatal("session with an open SSE stream expired: streams>0 must block expiry")
	}

	st.streamEnded(sess)
	advance(ttl + time.Second) // idle again, past the TTL, stream now closed
	if _, _, ok := st.touch(sess.id); ok {
		t.Fatal("session outlived stream end: streamEnded did not restore mortality (streams stuck > 0, session can never expire — the map-leak bug)")
	}
}

func TestSessionStoreCapAndSweep(t *testing.T) {
	const ttl = 5 * time.Minute
	st, advance := testStore(t, ttl, 2)

	first, ok := st.create("a/1", nil)
	if !ok {
		t.Fatal("first create failed")
	}
	if _, ok := st.create("b/1", nil); !ok {
		t.Fatal("second create failed (cap is 2)")
	}
	if _, ok := st.create("c/1", nil); ok {
		t.Fatal("third create succeeded past the cap, want ok=false (503)")
	}

	// Let the first two go idle: the next create must sweep them and succeed.
	advance(ttl + time.Second)
	if _, ok := st.create("d/1", nil); !ok {
		t.Fatal("create did not sweep expired sessions before refusing")
	}
	if !isClosed(first.done) {
		t.Fatal("sweep must close done of the sessions it drops")
	}
}

// TestSessionStoreReinitOfStaleSessionRejected is the deterministic unit-level
// reproduction of the reinit race fixed in review: handlePost's touch and
// reinit calls are two separate lock acquisitions, with an unbounded (from a
// slow client's body) window in between. A concurrent DELETE in that window
// removes the session from the map and closes done — but the caller in
// handlePost is still holding the now-stale *httpSession pointer touch handed
// it earlier, and would call reinit on it. Reproducing the actual race is
// nondeterministic; this test instead calls the store directly in the exact
// order that matters — touch, then terminate, then reinit on the pointer
// touch returned — which is deterministic and hits precisely the check added
// to guard against it (comparing st.sessions[s.id] against the pointer by
// identity, not just checking the id is still a map key).
func TestSessionStoreReinitOfStaleSessionRejected(t *testing.T) {
	st, _ := testStore(t, time.Minute, 8)

	sess, ok := st.create("cli/1.0.0", nil)
	if !ok {
		t.Fatal("create returned ok=false on an empty store")
	}
	held, _, ok := st.touch(sess.id) // the pointer handlePost would be holding
	if !ok {
		t.Fatal("touch of a live session returned ok=false")
	}

	if !st.terminate(sess.id) { // the concurrent DELETE
		t.Fatal("terminate of a live session returned false")
	}

	if st.reinit(held, "attacker/9.9.9", nil) {
		t.Fatal("reinit of a session terminated after touch returned ok=true — a dead session was resurrected")
	}
	if _, _, ok := st.touch(sess.id); ok {
		t.Fatal("session reappeared in the store after a rejected reinit")
	}
}

func TestSessionStoreIDsUniqueAndWellFormed(t *testing.T) {
	st, _ := testStore(t, time.Hour, 1024)

	hexOnly := regexp.MustCompile(`^[0-9a-f]{32}$`)
	seen := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		sess, ok := st.create("x/1", nil)
		if !ok {
			t.Fatalf("create %d failed", i)
		}
		if !hexOnly.MatchString(sess.id) {
			t.Fatalf("session id %q is not 32 lowercase hex chars (spec: visible ASCII, unguessable)", sess.id)
		}
		if seen[sess.id] {
			t.Fatalf("duplicate session id %q after %d creates", sess.id, i)
		}
		seen[sess.id] = true
	}
}

// Stage 17a — the unicast delivery layer for upstream-initiated requests
// (deliverServerReq / takeServerReq / the orphan hand-off). Driven through the
// store directly for the same reason as everything above: the e2e behaviour is
// in http_serverreq_test.go, here the point is the decision table itself.

// capsOf builds the declared-capability map a session is created with.
func capsOf(names ...string) map[string]json.RawMessage {
	caps := map[string]json.RawMessage{}
	for _, n := range names {
		caps[n] = json.RawMessage(`{}`)
	}
	return caps
}

// serverReq is the message shape the router delivers — only its id matters to
// the store, which never looks inside.
func serverReq(gatewayID string) *mcp.Message {
	return mcp.NewRequest(mcp.StringID(gatewayID), mcp.MethodElicitationCreate, nil)
}

// TestSessionStoreDeliverServerReq is the whole target-selection contract in
// one place: who is eligible, what happens when nobody is, and that the
// ownership record is written in the same breath as the send.
func TestSessionStoreDeliverServerReq(t *testing.T) {
	t.Run("delivered to a capable session with a stream", func(t *testing.T) {
		st, _ := testStore(t, time.Minute, 8)
		sess, _ := st.create("cli/1", capsOf(mcp.CapElicitation))
		ch := make(chan *mcp.Message, 1)
		st.addReqStream(sess, ch)

		if !st.deliverServerReq(mcp.CapElicitation, "elicit-1", serverReq("elicit-1")) {
			t.Fatal("deliverServerReq = false for a capable session with an open stream")
		}
		select {
		case got := <-ch:
			if string(got.ID) != `"elicit-1"` {
				t.Errorf("delivered message id = %s, want \"elicit-1\"", got.ID)
			}
		default:
			t.Fatal("deliverServerReq reported success but nothing reached the stream")
		}
		// The ownership record must exist ALREADY — a client answering the
		// instant it reads the event depends on it (that is why the record and
		// the send share one critical section).
		if !st.takeServerReq(sess, "elicit-1") {
			t.Fatal("the delivering session does not own the request it was handed")
		}
		if st.takeServerReq(sess, "elicit-1") {
			t.Error("the ownership record survived being taken: one answer per request")
		}
	})

	t.Run("session without the capability is not a candidate", func(t *testing.T) {
		st, _ := testStore(t, time.Minute, 8)
		sess, _ := st.create("cli/1", capsOf(mcp.CapSampling)) // declares something else
		ch := make(chan *mcp.Message, 1)
		st.addReqStream(sess, ch)

		if st.deliverServerReq(mcp.CapElicitation, "elicit-1", serverReq("elicit-1")) {
			t.Fatal("delivered an elicitation to a session that never declared it")
		}
		if len(ch) != 0 {
			t.Error("a message reached the stream of an incapable session")
		}
	})

	t.Run("capability without an open stream is not a candidate", func(t *testing.T) {
		st, _ := testStore(t, time.Minute, 8)
		st.create("cli/1", capsOf(mcp.CapElicitation)) // no addReqStream

		if st.deliverServerReq(mcp.CapElicitation, "elicit-1", serverReq("elicit-1")) {
			t.Fatal("delivered a request to a session with no open SSE stream")
		}
	})

	t.Run("full buffer falls through to the next stream, then gives up", func(t *testing.T) {
		st, _ := testStore(t, time.Minute, 8)
		sess, _ := st.create("cli/1", capsOf(mcp.CapElicitation))
		older := make(chan *mcp.Message, 1)
		newer := make(chan *mcp.Message, 1)
		st.addReqStream(sess, older)
		st.addReqStream(sess, newer)
		newer <- serverReq("filler") // the preferred (newest) stream is backed up

		if !st.deliverServerReq(mcp.CapElicitation, "elicit-1", serverReq("elicit-1")) {
			t.Fatal("deliverServerReq gave up although a second stream had room")
		}
		if len(older) != 1 {
			t.Errorf("older stream received %d messages, want 1 (the fall-through target)", len(older))
		}

		// Now every stream is full: the send must NOT block — a blocking send
		// here would hold sessionStore.mu and stall every handler.
		if st.deliverServerReq(mcp.CapElicitation, "elicit-2", serverReq("elicit-2")) {
			t.Fatal("deliverServerReq = true with every stream buffer full")
		}
		if st.takeServerReq(sess, "elicit-2") {
			t.Error("an undelivered request left an ownership record behind")
		}
	})

	t.Run("the capable session wins over a more recent incapable one", func(t *testing.T) {
		st, advance := testStore(t, time.Hour, 8)
		capable, _ := st.create("capable/1", capsOf(mcp.CapElicitation))
		capableCh := make(chan *mcp.Message, 1)
		st.addReqStream(capable, capableCh)

		advance(time.Minute) // the second session is strictly the fresher one
		other, _ := st.create("other/1", nil)
		otherCh := make(chan *mcp.Message, 1)
		st.addReqStream(other, otherCh)

		if !st.deliverServerReq(mcp.CapElicitation, "elicit-1", serverReq("elicit-1")) {
			t.Fatal("deliverServerReq = false although one session declared elicitation")
		}
		if len(capableCh) != 1 {
			t.Errorf("capable session received %d messages, want 1", len(capableCh))
		}
		if len(otherCh) != 0 {
			t.Errorf("incapable session received %d messages, want 0 (unicast, and gated on caps)", len(otherCh))
		}
	})

	t.Run("a foreign session cannot claim the answer", func(t *testing.T) {
		st, _ := testStore(t, time.Minute, 8)
		owner, _ := st.create("owner/1", capsOf(mcp.CapElicitation))
		st.addReqStream(owner, make(chan *mcp.Message, 1))
		stranger, _ := st.create("stranger/1", capsOf(mcp.CapElicitation))

		if !st.deliverServerReq(mcp.CapElicitation, "elicit-1", serverReq("elicit-1")) {
			t.Fatal("deliverServerReq = false for a capable session with an open stream")
		}
		if st.takeServerReq(stranger, "elicit-1") {
			t.Fatal("a session that was never asked claimed somebody else's elicitation")
		}
		if !st.takeServerReq(owner, "elicit-1") {
			t.Error("the rejected foreign claim consumed the real owner's record")
		}
	})
}

// TestSessionStoreOrphanedServerReqOnRemove pins the give-up path: a session
// that dies with a request outstanding must have that request refused AT ONCE
// (both ways a session can die), not left to the registry's five-minute
// deadline — an upstream is sitting inside tools/call waiting for it. The hook
// must also be called with the store's mutex released, which -race plus the
// re-entrant call below (touch inside the hook) is what actually checks.
func TestSessionStoreOrphanedServerReqOnRemove(t *testing.T) {
	deliver := func(st *sessionStore, gatewayID string) *httpSession {
		t.Helper()
		sess, _ := st.create("cli/1", capsOf(mcp.CapElicitation))
		st.addReqStream(sess, make(chan *mcp.Message, 1))
		if !st.deliverServerReq(mcp.CapElicitation, gatewayID, serverReq(gatewayID)) {
			t.Fatalf("deliverServerReq(%q) = false", gatewayID)
		}
		return sess
	}

	t.Run("terminate", func(t *testing.T) {
		st, _ := testStore(t, time.Minute, 8)
		var refused []string
		st.onServerReqOrphaned = func(id string) {
			// Re-enter the store from inside the callback: legal only because
			// it is invoked with mu released.
			st.trackedServerReqIDs()
			refused = append(refused, id)
		}
		sess := deliver(st, "elicit-1")

		if !st.terminate(sess.id) {
			t.Fatal("terminate of a live session returned false")
		}
		if len(refused) != 1 || refused[0] != "elicit-1" {
			t.Fatalf("refused = %v, want exactly [elicit-1] on session termination", refused)
		}
		if ids := st.trackedServerReqIDs(); len(ids) != 0 {
			t.Errorf("ownership records %v survived the session", ids)
		}
	})

	t.Run("idle expiry via touch", func(t *testing.T) {
		const ttl = 10 * time.Minute
		st, advance := testStore(t, ttl, 8)
		var refused []string
		st.onServerReqOrphaned = func(id string) { refused = append(refused, id) }
		sess := deliver(st, "elicit-7")

		advance(ttl + time.Second) // streams tally is 0 here, so the session dies
		if _, _, ok := st.touch(sess.id); ok {
			t.Fatal("touch past the idle TTL returned ok=true")
		}
		if len(refused) != 1 || refused[0] != "elicit-7" {
			t.Fatalf("refused = %v, want exactly [elicit-7] on idle expiry", refused)
		}
	})

	t.Run("answered requests are not refused afterwards", func(t *testing.T) {
		st, _ := testStore(t, time.Minute, 8)
		var refused []string
		st.onServerReqOrphaned = func(id string) { refused = append(refused, id) }
		sess := deliver(st, "elicit-9")

		if !st.takeServerReq(sess, "elicit-9") {
			t.Fatal("takeServerReq = false for the owning session")
		}
		if !st.terminate(sess.id) {
			t.Fatal("terminate of a live session returned false")
		}
		if len(refused) != 0 {
			t.Errorf("refused = %v, want none: the client had already answered", refused)
		}
	})
}

// TestSessionStoreSweepsRequestsTheRegistryGaveUpOn covers the lazy cleanup of
// ownership records whose registry entry expired on its own deadline: the ids
// are forgotten silently (the registry already refused the upstream), and a
// later session death must not re-refuse them.
func TestSessionStoreSweepsRequestsTheRegistryGaveUpOn(t *testing.T) {
	st, _ := testStore(t, time.Minute, 8)
	var refused []string
	st.onServerReqOrphaned = func(id string) { refused = append(refused, id) }

	sess, _ := st.create("cli/1", capsOf(mcp.CapElicitation))
	st.addReqStream(sess, make(chan *mcp.Message, 4))
	for _, id := range []string{"elicit-1", "elicit-2"} {
		if !st.deliverServerReq(mcp.CapElicitation, id, serverReq(id)) {
			t.Fatalf("deliverServerReq(%q) = false", id)
		}
	}

	ids := st.trackedServerReqIDs()
	if len(ids) != 2 {
		t.Fatalf("trackedServerReqIDs = %v, want both delivered ids", ids)
	}
	st.dropServerReqs([]string{"elicit-1"})
	if got := st.trackedServerReqIDs(); len(got) != 1 || got[0] != "elicit-2" {
		t.Fatalf("after dropping elicit-1, tracked = %v, want [elicit-2]", got)
	}

	if !st.terminate(sess.id) {
		t.Fatal("terminate of a live session returned false")
	}
	if len(refused) != 1 || refused[0] != "elicit-2" {
		t.Fatalf("refused = %v, want only the still-tracked [elicit-2]", refused)
	}
}
