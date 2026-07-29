package registry

import (
	"time"

	"github.com/akomyagin/aiMCPGate/internal/logging"
)

// Operator-facing events (Stage 18).
//
// These are the gateway states an operator cannot otherwise see: in stdio mode
// the MCP client owns the terminal, so stderr is invisible, and several of these
// conditions were only ever logged at slog's Debug level, silent under the
// default log_level: info. They are written as EventRecord lines into the SAME
// JSON-lines journal as call records (log_file), so `mcp-gate logs` shows the
// whole history from one file.
//
// Why synchronous writes with coalescing, and not a dedicated goroutine + channel:
// the two repeatable events (notification / server-request drops) are born on an
// upstream's single reader goroutine, which must never stall. A goroutine would
// buy asynchrony at the price of a whole new lifecycle (start, stop, drain on
// Close) plus test nondeterminism — the same machinery Stages 16 and 17a already
// rejected for the janitor. A bare Record per drop is the other extreme: a
// wedged subscriber drops EVERY message of a flood, which would both flood the
// journal and put a syscall on the reader goroutine per message.
//
// Coalescing-throttle is the middle: the FIRST occurrence of a key reaches the
// journal immediately, repeats inside the window are only counted (an increment
// under a leaf mutex) and folded into the Count of the next emitted line for
// that key — or into the final flush from Close, so the tail is never lost.
// The cost, stated plainly: the exact suppressed count reaches the journal late,
// and the first drop of a window costs the reader goroutine one buffered append
// (microseconds — the same order as the r.log.Debug call that was already there).

// eventThrottleWindow bounds how often one (event, upstream, subject) key may
// reach the journal.
const eventThrottleWindow = time.Minute

// eventKey identifies a throttled event stream. Subject is part of the key on
// purpose: an upstream dropping three different methods yields three lines per
// window, which is exactly the granularity an operator needs.
type eventKey struct{ event, upstream, subject string }

type eventState struct {
	suppressed int       // occurrences seen since lastEmit, not yet journaled
	lastEmit   time.Time // when this key last reached the journal
}

// emitEvent writes one event line. It is nil-safe on r.callLog: doctor, call and
// catalog build the registry without a journal (events are simply not recorded
// there — doctor has its own StartReport). Time and Kind are stamped here so no
// call site can forget them, and a Count of 1 is normalized to zero so the wire
// keeps its "absent == 1" convention.
//
// The mutex inside callLog is a LEAF: emitEvent takes nothing else, and nothing
// else is taken from under it. That is what makes it safe to call from the merge
// paths while r.mu is held (see mergeLocked) — those are cold paths (Start,
// Reload, restart), never the tool-call path.
func (r *Registry) emitEvent(e logging.EventRecord) {
	if r.callLog == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	e.Kind = logging.KindEvent
	if e.Count == 1 {
		e.Count = 0
	}
	r.callLog.RecordEvent(e)
}

// noteThrottled coalesces a repeatable event, per (event, upstream, subject).
// The first occurrence of a window is journaled immediately (carrying whatever
// remainder was suppressed before it); later ones inside the window only bump a
// counter.
//
// Locking contract: it takes ONLY r.eventMu, a leaf mutex, and calls emitEvent
// from OUTSIDE it. Callers must not hold notifMu / serverReqMu / rootsMu when
// calling — collect what needs reporting under those locks and note it after
// unlocking (see forwardNotification).
func (r *Registry) noteThrottled(event, upstream, subject, detail string) {
	now := time.Now()
	key := eventKey{event: event, upstream: upstream, subject: subject}

	r.eventMu.Lock()
	st := r.eventSeen[key]
	if st == nil {
		st = &eventState{}
		r.eventSeen[key] = st
	}
	if !st.lastEmit.IsZero() && now.Sub(st.lastEmit) < eventThrottleWindow {
		st.suppressed++
		r.eventMu.Unlock()
		return
	}
	count := st.suppressed + 1
	st.suppressed = 0
	st.lastEmit = now
	r.eventMu.Unlock()

	r.emitEvent(logging.EventRecord{
		Time:     now,
		Event:    event,
		Upstream: upstream,
		Subject:  subject,
		Detail:   detail,
		Count:    count,
	})
}

// flushEvents journals the suppressed remainder of every throttled key. It is
// called once, first thing in Close, so the counts accumulated inside the last
// window are not lost on shutdown — without it a burst of drops right before
// exit would leave only the window's opening line in the journal.
func (r *Registry) flushEvents() {
	type pending struct {
		key   eventKey
		count int
	}
	var out []pending

	r.eventMu.Lock()
	for k, st := range r.eventSeen {
		if st.suppressed > 0 {
			out = append(out, pending{key: k, count: st.suppressed})
			st.suppressed = 0
		}
	}
	r.eventMu.Unlock()

	for _, p := range out {
		r.emitEvent(logging.EventRecord{
			Event:    p.key.event,
			Upstream: p.key.upstream,
			Subject:  p.key.subject,
			Detail:   eventFlushDetail(p.key.event),
			Count:    p.count,
		})
	}
}

// eventFlushDetail is the reason text for a flush line. It names the flush
// explicitly so an operator does not read the trailing line as a fresh burst.
func eventFlushDetail(event string) string {
	switch event {
	case logging.EventNotificationDropped:
		return "further drops coalesced since the previous line, flushed at gateway shutdown"
	case logging.EventServerRequestDropped:
		return "further undelivered server requests coalesced since the previous line, flushed at gateway shutdown"
	default:
		return "further occurrences coalesced since the previous line, flushed at gateway shutdown"
	}
}
