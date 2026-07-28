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
