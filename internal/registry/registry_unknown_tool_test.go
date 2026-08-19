package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// DEFECT 3 — a tools/call the gateway cannot route used to return to the client
// in complete silence: r.audit lives inside callUpstream, which that branch
// never reaches. Every test here asserts on the JOURNAL BYTES (read back through
// logging.ReadEntries), because the point of the fix is what an operator sees
// through `mcp-gate logs`, not an internal counter.
//
// These are CALL lines, not operator events (plan §3.1): a client missing a tool
// name is one failed call, not a state of the gateway — and a client-supplied
// subject in the event throttle's map would grow it without bound (R5 guards
// exactly that).

// unknownToolRegistry starts a registry over one live fake upstream ("demo" with
// tool "echo") writing into a fresh journal.
func unknownToolRegistry(t *testing.T) (*Registry, *eventJournal) {
	t.Helper()
	j, callLog := newJournal()
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "demo", Enabled: boolPtr(true)}}}
	fakes := map[string]*fakeUpstream{"demo": {name: "demo", tools: []string{"echo"}}}
	r := newTestRegistry(t, cfg, callLog, fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, j
}

// calls projects the journal onto its call lines.
func journalCalls(t *testing.T, j *eventJournal) []logging.CallRecord {
	t.Helper()
	var out []logging.CallRecord
	for _, e := range j.entries(t) {
		if e.Call != nil {
			out = append(out, *e.Call)
		}
	}
	return out
}

// TestUnknownToolIsJournaledAsFailedCall (R1) is the defect itself: the call
// leaves exactly one line, it is a CALL line, and the text the client gets is
// unchanged.
func TestUnknownToolIsJournaledAsFailedCall(t *testing.T) {
	r, j := unknownToolRegistry(t)

	_, err := r.CallTool(context.Background(), "nope__thing", json.RawMessage(`{"a":1}`), nil)
	if err == nil {
		t.Fatal("CallTool on an unknown name returned no error")
	}
	// §2.1: the client-facing text is frozen. Journaling is an OPERATOR feature;
	// it must not widen the protocol surface.
	if want := `unknown tool "nope__thing"`; err.Error() != want {
		t.Errorf("client-facing error = %q, want %q (the text must not change)", err.Error(), want)
	}

	entries := j.entries(t)
	if len(entries) != 1 {
		t.Fatalf("journal has %d entries, want exactly 1; journal:\n%s", len(entries), j.bytes())
	}
	if entries[0].Call == nil {
		t.Fatalf("the unknown-tool line is not a CALL record (plan §3.1); journal:\n%s", j.bytes())
	}
	rec := *entries[0].Call
	if rec.Upstream != logging.UpstreamUnrouted {
		t.Errorf("upstream = %q, want the %q sentinel", rec.Upstream, logging.UpstreamUnrouted)
	}
	if rec.Method != mcp.MethodToolsCall {
		t.Errorf("method = %q, want %q", rec.Method, mcp.MethodToolsCall)
	}
	if rec.Tool != "nope__thing" {
		t.Errorf("tool = %q, want the name the client asked for", rec.Tool)
	}
	if rec.OK {
		t.Error("the record says ok; a call that reached no upstream is a failure")
	}
	if rec.Err == "" {
		t.Error("the record carries no reason; the operator needs to know why it failed")
	}
	if rec.Time.IsZero() {
		t.Error("the record carries no timestamp")
	}
}

// TestUnknownToolRecordCarriesNoArguments (R2) is SKILL §6 on the new line: the
// arguments are not even passed to r.audit, and no byte of them may reach the
// journal.
func TestUnknownToolRecordCarriesNoArguments(t *testing.T) {
	const secret = "SUPERSECRET"
	r, j := unknownToolRegistry(t)

	args := json.RawMessage(`{"api_key":"` + secret + `"}`)
	if _, err := r.CallTool(context.Background(), "nope__thing", args, nil); err == nil {
		t.Fatal("CallTool on an unknown name returned no error")
	}

	if bytes.Contains(j.bytes(), []byte(secret)) {
		t.Fatalf("call arguments leaked into the journal:\n%s", j.bytes())
	}
	if got := journalCalls(t, j); len(got) != 1 {
		t.Fatalf("got %d call records, want 1 (the leak check proves nothing on an empty journal)", len(got))
	}
}

// TestRoutedToolWithoutConnectionIsJournaledWithItsUpstream (R3): a name that
// HAS a route but whose connection is gone is a different operator fact and gets
// the real upstream name in the journal — while the client still sees the very
// same "unknown tool" text, because telling it the tool exists but its upstream
// is down leaks the gateway's topology (§2.1).
func TestRoutedToolWithoutConnectionIsJournaledWithItsUpstream(t *testing.T) {
	r, j := unknownToolRegistry(t)

	// A route with no connection behind it: the state a restarting or dropped
	// upstream leaves behind.
	r.mu.Lock()
	r.toolRoute["up__t"] = route{upstream: "up", original: "t"}
	delete(r.conns, "up")
	r.mu.Unlock()

	_, err := r.CallTool(context.Background(), "up__t", nil, nil)
	if err == nil {
		t.Fatal("CallTool on a routed tool without a connection returned no error")
	}
	if want := `unknown tool "up__t"`; err.Error() != want {
		t.Errorf("client-facing error = %q, want %q (no topology leak)", err.Error(), want)
	}

	got := journalCalls(t, j)
	if len(got) != 1 {
		t.Fatalf("journal has %d call records, want 1; journal:\n%s", len(got), j.bytes())
	}
	if got[0].Upstream != "up" {
		t.Errorf("upstream = %q, want %q: the route is known, so the sentinel would hide which upstream is down",
			got[0].Upstream, "up")
	}
	if !strings.Contains(got[0].Err, "connection") {
		t.Errorf("reason = %q, want it to name the missing connection", got[0].Err)
	}
}

// TestUnknownToolNameIsClampedInJournal (R4): the tool name of this record is
// the first journal field carrying raw client input, so one request must not be
// able to write one journal line of arbitrary size. The clamp is a JOURNAL
// bound only — the client still sees its whole name echoed back.
func TestUnknownToolNameIsClampedInJournal(t *testing.T) {
	r, j := unknownToolRegistry(t)

	long := strings.Repeat("z", 10000)
	_, err := r.CallTool(context.Background(), long, nil, nil)
	if err == nil {
		t.Fatal("CallTool on an unknown name returned no error")
	}
	if !strings.Contains(err.Error(), long) {
		t.Error("the client-facing error no longer echoes the whole name it asked for; that behavior did not change")
	}

	got := journalCalls(t, j)
	if len(got) != 1 {
		t.Fatalf("journal has %d call records, want 1; journal:\n%s", len(got), j.bytes())
	}
	if n := utf8.RuneCountInString(got[0].Tool); n != logging.MaxNameRunes+1 {
		t.Errorf("journaled name has %d runes, want %d (the bound plus the truncation marker)",
			n, logging.MaxNameRunes+1)
	}
	if !strings.HasSuffix(got[0].Tool, "…") {
		t.Error("the journaled name carries no truncation marker, so the operator cannot tell it was cut")
	}
}

// TestUnknownToolFloodDoesNotGrowEventState (R5) is the watchdog of decision
// §3.1. Tool names come FROM THE CLIENT and are unbounded in variety; the event
// throttle's eventSeen map is never pruned (events.go has no delete at all), so
// routing this fact through noteThrottled with the name as subject would be an
// unbounded, client-driven memory leak. Writing a call line keeps NO state.
func TestUnknownToolFloodDoesNotGrowEventState(t *testing.T) {
	const flood = 1000
	r, j := unknownToolRegistry(t)

	for i := 0; i < flood; i++ {
		if _, err := r.CallTool(context.Background(), fmt.Sprintf("nope__thing%d", i), nil, nil); err == nil {
			t.Fatalf("call %d on an unknown name returned no error", i)
		}
	}

	r.eventMu.Lock()
	seen := len(r.eventSeen)
	r.eventMu.Unlock()
	if seen != 0 {
		t.Errorf("the throttle map holds %d keys after %d unknown-tool calls: this fact must not be an event, "+
			"its subject is unbounded client input and eventSeen is never pruned", seen, flood)
	}

	var calls, events int
	for _, e := range j.entries(t) {
		if e.Call != nil {
			calls++
		}
		if e.Event != nil {
			events++
		}
	}
	if events != 0 {
		t.Errorf("journal holds %d event lines, want 0: unknown tools are CALL lines", events)
	}
	if calls != flood {
		t.Errorf("journal holds %d call lines, want %d (one per attempt, no dedup by design)", calls, flood)
	}
}

// TestUnknownToolIsNotJournaledUnderRegistryLock (R6) is the deadlock guard,
// mirroring TestCatalogEventIsNotJournaledUnderRegistryLock: the journal write
// ends in write(2) on what is os.Stderr when log_file is unset — a pipe to the
// MCP client in stdio mode. A client that stopped draining it parks the write
// forever, and doing that under r.mu would wedge every request of the gateway.
func TestUnknownToolIsNotJournaledUnderRegistryLock(t *testing.T) {
	b := &blockingJournal{entered: make(chan struct{}, 1), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(b.release) }) }
	defer unblock()

	cfg := &config.Config{Upstreams: []config.Upstream{{Name: "demo", Enabled: boolPtr(true)}}}
	fakes := map[string]*fakeUpstream{"demo": {name: "demo", tools: []string{"echo"}}}
	r := newTestRegistry(t, cfg, logging.NewCallLogWriter(b), fakes)
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	// Start journaled nothing (a clean catalog, no calls yet). Arm the trap, then
	// make the unknown-tool branch do the write.
	b.armed.Store(true)
	go func() { _, _ = r.CallTool(context.Background(), "nope__thing", nil, nil) }()

	select {
	case <-b.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the unknown-tool call never journaled anything, so this test proves nothing")
	}

	// The journal write is parked right now; if it were happening inside
	// CallTool's lookup, r.mu would still be held. The probe takes the WRITE lock
	// — what Reload, a supervisor re-installing a connection and every relist
	// merge take — and not ToolCount() the way the catalog-event twin of this test
	// does: r.mu is an RWMutex and CallTool holds it for READING, so a second
	// reader sails straight past a parked one and proves nothing (learned by
	// running the mutation: with the audit moved under r.mu, a ToolCount() probe
	// stayed green). A blocked writer is the actual hazard, and under Go's RWMutex
	// it stalls every later reader too, since readers cannot overtake a waiting
	// writer — that is the whole gateway wedged behind one unread pipe.
	done := make(chan struct{})
	go func() {
		r.mu.Lock()
		r.mu.Unlock() //nolint:staticcheck // acquiring the lock IS the assertion
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		unblock() // let the parked writer finish so the test can unwind
		t.Fatal("a catalog writer blocked while the unknown-tool record was being journaled: the write is happening under r.mu")
	}
	unblock()
}

// TestUnknownToolWithoutCallLogDoesNotPanic (R7): doctor, call and catalog build
// a registry with no journal at all, and the new branch runs on their path too.
func TestUnknownToolWithoutCallLogDoesNotPanic(t *testing.T) {
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
	_, err := r.CallTool(context.Background(), "nope__thing", nil, nil)
	if err == nil {
		t.Fatal("CallTool on an unknown name returned no error")
	}
	if want := `unknown tool "nope__thing"`; err.Error() != want {
		t.Errorf("client-facing error = %q, want %q", err.Error(), want)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
