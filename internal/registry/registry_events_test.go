package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/logging"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// Stage 18 — operator events reaching the journal. Every test here asserts on
// the JOURNAL BYTES (read back through logging.ReadEntries), not on an internal
// counter: the whole point of the stage is that an operator can see these
// states through `mcp-gate logs`.

// eventJournal is a concurrency-safe journal sink: events are written from
// upstream reader goroutines and supervisors while the test polls the contents,
// which a plain bytes.Buffer does not tolerate under -race.
type eventJournal struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (j *eventJournal) Write(p []byte) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.buf.Write(p)
}

func (j *eventJournal) bytes() []byte {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]byte(nil), j.buf.Bytes()...)
}

// entries returns everything decodable in the journal so far.
func (j *eventJournal) entries(t *testing.T) []logging.Entry {
	t.Helper()
	entries, err := logging.ReadEntries(bytes.NewReader(j.bytes()))
	if err != nil {
		t.Fatalf("ReadEntries: %v", err)
	}
	return entries
}

// events returns the journal's event lines with the given id (all ids if name
// is empty).
func (j *eventJournal) events(t *testing.T, name string) []logging.EventRecord {
	t.Helper()
	var out []logging.EventRecord
	for _, e := range j.entries(t) {
		if e.Event == nil {
			continue
		}
		if name == "" || e.Event.Event == name {
			out = append(out, *e.Event)
		}
	}
	return out
}

// waitForEvent polls until at least one event with the given id is journaled.
func (j *eventJournal) waitForEvent(t *testing.T, name string, within time.Duration) []logging.EventRecord {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if evs := j.events(t, name); len(evs) > 0 {
			return evs
		}
		if time.Now().After(deadline) {
			t.Fatalf("event %q never reached the journal within %s; journal:\n%s", name, within, j.bytes())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newJournal wires a fresh journal into a CallLog the registry can be built with.
func newJournal() (*eventJournal, logging.CallLog) {
	j := &eventJournal{}
	return j, logging.NewCallLogWriter(j)
}

// TestStartFailureJournalsEvent (R1): an upstream that cannot come up at all is
// visible to the operator afterwards — Start still succeeds thanks to the live
// one. Doubles as the secrets e2e: the failing upstream's env carries a token,
// and no byte of it may appear anywhere in the journal.
func TestStartFailureJournalsEvent(t *testing.T) {
	const secret = "sup3rsecret-start-token"
	bin := buildFakeServer(t)
	j, callLog := newJournal()

	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "good", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
			"FAKE_TOOLS": "ping",
		}},
		{Name: "broken", Command: filepath.Join(t.TempDir(), "definitely-not-here"), Enabled: boolPtr(true), Env: map[string]string{
			"UPSTREAM_API_TOKEN": secret,
		}},
	}}
	r := New(cfg, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v (one live upstream must keep the gateway up)", err)
	}
	defer func() { _ = r.Close() }()

	evs := j.events(t, logging.EventUpstreamStartFailed)
	if len(evs) != 1 {
		t.Fatalf("got %d upstream_start_failed events, want 1; journal:\n%s", len(evs), j.bytes())
	}
	if evs[0].Upstream != "broken" {
		t.Errorf("event upstream = %q, want %q", evs[0].Upstream, "broken")
	}
	if evs[0].Detail == "" {
		t.Error("event carries no detail; the operator needs the failure reason")
	}
	if evs[0].Kind != logging.KindEvent {
		t.Errorf("event kind = %q, want %q", evs[0].Kind, logging.KindEvent)
	}
	if evs[0].Time.IsZero() {
		t.Error("event carries no timestamp")
	}
	if bytes.Contains(j.bytes(), []byte(secret)) {
		t.Fatalf("the failing upstream's token leaked into the journal:\n%s", j.bytes())
	}
}

// TestSupervisorGiveUpJournalsEvent (R2): when the supervisor exhausts its
// restart attempts and drops the upstream from the catalog, the operator finds
// out from the journal — the previous only trace was an Error line on stderr.
func TestSupervisorGiveUpJournalsEvent(t *testing.T) {
	src := buildFakeServer(t)
	bin := filepath.Join(t.TempDir(), "disposable")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read fakeserver: %v", err)
	}
	if err := os.WriteFile(bin, data, 0o755); err != nil {
		t.Fatalf("write disposable binary: %v", err)
	}

	j, callLog := newJournal()
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
			MaxAttempts:    1,
		},
		Upstreams: []config.Upstream{
			{Name: "doomed", Command: bin, Enabled: boolPtr(true), Env: map[string]string{
				"FAKE_TOOLS":      "ping",
				"FAKE_ECHO":       "1",
				"FAKE_EXIT_AFTER": "1",
			}},
		},
	}
	r := New(cfg, quietLogger(), callLog, noopPayloadLog(), true, "0.0.0-test")
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	// Remove the binary before the crashing call, so every relaunch attempt
	// fails at exec.LookPath (same setup as TestSupervisorGivesUpAndDrops).
	if err := os.Remove(bin); err != nil {
		t.Fatalf("remove binary: %v", err)
	}
	if _, err := r.CallTool(context.Background(), "doomed__ping", []byte(`{}`), nil); err != nil {
		t.Fatalf("first CallTool: %v", err)
	}

	evs := j.waitForEvent(t, logging.EventUpstreamGaveUp, 10*time.Second)
	if evs[0].Upstream != "doomed" {
		t.Errorf("event upstream = %q, want %q", evs[0].Upstream, "doomed")
	}
	if !strings.Contains(evs[0].Detail, "attempts") {
		t.Errorf("detail = %q, want it to mention the exhausted attempts", evs[0].Detail)
	}
}

// TestRestartDisabledByReloadJournalsEvent (R3) pins the SYMMETRIC give-up
// branch: a reload turned auto-restart off while an upstream was down, so the
// supervisor drops it instead of relaunching. Driven through restart() directly
// — the branch depends on a config swap landing between a crash and the next
// policy read, which nothing in the public API can schedule deterministically.
func TestRestartDisabledByReloadJournalsEvent(t *testing.T) {
	j, callLog := newJournal()
	cfg := &config.Config{
		Restart:   config.RestartPolicy{Enabled: boolPtr(false)}, // the state a reload left behind
		Upstreams: []config.Upstream{{Name: "gone", Enabled: boolPtr(true)}},
	}
	r := New(cfg, quietLogger(), callLog, noopPayloadLog(), true, "0.0.0-test")
	defer func() { _ = r.Close() }()

	dead := &fakeUpstream{name: "gone"}
	r.mu.Lock()
	r.conns["gone"] = dead // the catalog entry the drop is identity-gated on
	r.mu.Unlock()

	conn, done, ok := r.restart(cfg.Upstreams[0], dead, context.Background())
	if ok || conn != nil || done != nil {
		t.Fatalf("restart with the policy disabled returned (%v, %v, %v), want a give-up", conn, done, ok)
	}

	evs := j.events(t, logging.EventUpstreamGaveUp)
	if len(evs) != 1 {
		t.Fatalf("got %d upstream_gave_up events, want 1; journal:\n%s", len(evs), j.bytes())
	}
	if evs[0].Upstream != "gone" {
		t.Errorf("event upstream = %q, want %q", evs[0].Upstream, "gone")
	}
	if !strings.Contains(evs[0].Detail, "reload") {
		t.Errorf("detail = %q, want it to name the reload as the cause", evs[0].Detail)
	}
}

// TestRestartNoDoneChannelJournalsEvent covers the third give-up branch: a
// relaunched upstream that reports no liveness channel is closed and abandoned.
func TestRestartNoDoneChannelJournalsEvent(t *testing.T) {
	j, callLog := newJournal()
	cfg := &config.Config{
		Restart: config.RestartPolicy{
			Enabled:        boolPtr(true),
			InitialBackoff: time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
			MaxAttempts:    3,
		},
		Upstreams: []config.Upstream{{Name: "nodone", Enabled: boolPtr(true)}},
	}
	r := New(cfg, quietLogger(), callLog, noopPayloadLog(), true, "0.0.0-test")
	defer func() { _ = r.Close() }()
	// fakeUpstream.Done reports ok=false — exactly the defensive case.
	r.start = func(context.Context, config.Upstream) (Upstream, error) {
		return &fakeUpstream{name: "nodone"}, nil
	}

	dead := &fakeUpstream{name: "nodone"}
	if _, _, ok := r.restart(cfg.Upstreams[0], dead, context.Background()); ok {
		t.Fatal("restart returned ok=true for an upstream with no done channel")
	}
	evs := j.events(t, logging.EventUpstreamGaveUp)
	if len(evs) != 1 || evs[0].Upstream != "nodone" {
		t.Fatalf("events = %+v, want one upstream_gave_up for nodone; journal:\n%s", evs, j.bytes())
	}
}

// TestNotificationDropCoalescedIntoJournal (R4) is the flood guard: a wedged
// subscriber drops every message of a burst, and the journal must record that
// in a BOUNDED number of lines whose counts still add up to the real total.
func TestNotificationDropCoalescedIntoJournal(t *testing.T) {
	const drops = 40
	j, callLog := newJournal()
	r := New(&config.Config{}, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")

	_, unsubscribe := r.SubscribeNotifications() // subscribed and never reading
	defer unsubscribe()

	// Fill the subscriber's buffer first, so every one of the next `drops`
	// notifications is genuinely dropped.
	for i := 0; i < notifSubBuffer; i++ {
		r.onUpstreamNotification("web", mcp.NotifProgress, json.RawMessage(`{"progress":1}`))
	}
	for i := 0; i < drops; i++ {
		r.onUpstreamNotification("web", mcp.NotifProgress, json.RawMessage(`{"progress":1}`))
	}

	// Before Close only the window-opening line is out.
	if evs := j.events(t, logging.EventNotificationDropped); len(evs) != 1 {
		t.Fatalf("got %d notification_dropped lines during the burst, want exactly 1 (throttled); journal:\n%s",
			len(evs), j.bytes())
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	evs := j.events(t, logging.EventNotificationDropped)
	if len(evs) != 2 {
		t.Fatalf("got %d notification_dropped lines, want 2 (immediate + Close flush); journal:\n%s",
			len(evs), j.bytes())
	}
	total := 0
	for _, e := range evs {
		if e.Count < 1 {
			total++ // absent count means one occurrence
			continue
		}
		total += e.Count
	}
	if total != drops {
		t.Errorf("coalesced counts add up to %d, want %d (every drop must be accounted for)", total, drops)
	}
	if evs[0].Subject != mcp.NotifProgress {
		t.Errorf("subject = %q, want the dropped method %q", evs[0].Subject, mcp.NotifProgress)
	}
}

// TestServerReqPublishFailureJournalsEvent (R5): an upstream asks something only
// the client could answer, no transport is subscribed to carry the question, and
// the upstream is refused. To the user this looks like a tool that simply
// failed; the journal is the only place the real reason appears.
func TestServerReqPublishFailureJournalsEvent(t *testing.T) {
	t.Run("elicitation", func(t *testing.T) {
		j, callLog := newJournal()
		r := New(&config.Config{}, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")
		fake := &elicitRecordingUpstream{responses: make(chan *mcp.Message, 4)}
		r.mu.Lock()
		r.conns["web"] = fake
		r.mu.Unlock()
		r.SetClientServerRequestCaps(declaredCaps("elicitation")) // capable client, nobody subscribed

		r.onUpstreamRequest("web", mcp.MethodElicitationCreate, mcp.IntID(9), json.RawMessage(`{"message":"need input"}`))

		evs := j.waitForEvent(t, logging.EventServerRequestDropped, 5*time.Second)
		if len(evs) != 1 {
			t.Fatalf("got %d server_request_dropped events, want 1", len(evs))
		}
		if evs[0].Upstream != "web" || evs[0].Subject != mcp.MethodElicitationCreate {
			t.Errorf("event = %+v, want upstream web / subject %q", evs[0], mcp.MethodElicitationCreate)
		}
	})

	t.Run("roots/list", func(t *testing.T) {
		j, callLog := newJournal()
		r := New(&config.Config{}, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")
		fake := &elicitRecordingUpstream{responses: make(chan *mcp.Message, 4)}
		r.mu.Lock()
		r.conns["web"] = fake
		r.mu.Unlock()
		r.SetClientServerRequestCaps(declaredCaps("roots"))

		r.onUpstreamRequest("web", mcp.MethodRootsList, mcp.IntID(11), nil)

		evs := j.waitForEvent(t, logging.EventServerRequestDropped, 5*time.Second)
		if len(evs) != 1 {
			t.Fatalf("got %d server_request_dropped events, want 1", len(evs))
		}
		if evs[0].Upstream != "web" || evs[0].Subject != mcp.MethodRootsList {
			t.Errorf("event = %+v, want upstream web / subject %q", evs[0], mcp.MethodRootsList)
		}
	})
}

// badUTF8Template is a URI template whose regexp genuinely fails to compile:
// invalid UTF-8 (regexp/syntax rejects it). NOTE: the plan's suggested "bad{tpl"
// does NOT fail — an unclosed brace is deliberately treated as a literal by
// templateRegexp — so it could never have driven this branch red.
const badUTF8Template = "file:///\xff/{path}"

// TestCatalogCollisionsJournalEvents (R6): everything the catalog silently
// swallows — keep-first duplicates and a template that can never match — becomes
// visible. The same merge path runs on Start and on Reload, so both are checked.
func TestCatalogCollisionsJournalEvents(t *testing.T) {
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "a", Enabled: boolPtr(true)},
		{Name: "b", Enabled: boolPtr(true)},
	}}
	newFakes := func() map[string]*fakeUpstream {
		return map[string]*fakeUpstream{
			// "dup" twice within one upstream: the runtime surprise keep-first guards.
			// "pdup" twice does the same for prompts — a SEPARATE branch with its own
			// emission, so a tool-only case leaves it unpinned (it was: independent
			// verification found deleting that emission kept the whole package green).
			// Each kind's capability must be declared or launch never lists it at all.
			"a": {name: "a", tools: []string{"dup", "dup"}, prompts: []string{"pdup", "pdup"},
				resources: []string{"file:///shared"}, templates: []string{badUTF8Template},
				caps: json.RawMessage(`{"tools":{},"prompts":{},"resources":{}}`)},
			// b claims a URI a already owns.
			"b": {name: "b", tools: []string{"t"}, resources: []string{"file:///shared"},
				caps: json.RawMessage(`{"tools":{},"resources":{}}`)},
		}
	}

	j, callLog := newJournal()
	r := newTestRegistry(t, cfg, callLog, newFakes())
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	collisions := j.events(t, logging.EventCatalogCollision)
	if len(collisions) != 3 {
		t.Fatalf("got %d catalog_collision events, want 3 (dup tool + dup prompt + dup uri); journal:\n%s",
			len(collisions), j.bytes())
	}
	var sawTool, sawPrompt, sawURI bool
	for _, e := range collisions {
		switch e.Subject {
		case "a__dup":
			sawTool = true
			if e.Upstream != "a" {
				t.Errorf("tool collision upstream = %q, want the loser %q", e.Upstream, "a")
			}
		case "a__pdup":
			sawPrompt = true
			if e.Upstream != "a" {
				t.Errorf("prompt collision upstream = %q, want the loser %q", e.Upstream, "a")
			}
		case "file:///shared":
			sawURI = true
			if e.Upstream != "b" {
				t.Errorf("uri collision upstream = %q, want the loser %q", e.Upstream, "b")
			}
		default:
			t.Errorf("unexpected collision subject %q", e.Subject)
		}
	}
	if !sawTool || !sawPrompt || !sawURI {
		t.Errorf("collisions = %+v, want the duplicate tool, prompt and uri all three", collisions)
	}

	bad := j.events(t, logging.EventCatalogBadTemplate)
	if len(bad) != 1 {
		t.Fatalf("got %d catalog_bad_template events, want 1; journal:\n%s", len(bad), j.bytes())
	}
	// The subject is the template as JSON can carry it: an invalid UTF-8 byte
	// becomes U+FFFD on marshal (encoding/json has no other option), so the
	// operator sees the template with the offending byte replaced, not verbatim.
	if !strings.Contains(bad[0].Subject, "{path}") || bad[0].Upstream != "a" {
		t.Errorf("bad-template event = %+v, want the template subject from upstream a", bad[0])
	}

	// The same merge body runs on Reload, not only on the first bring-up. A
	// changed launch spec (new env) makes the reload relaunch and re-merge both
	// upstreams, so every collision must be journaled again.
	before := len(j.events(t, ""))
	cfg2 := &config.Config{Upstreams: []config.Upstream{
		{Name: "a", Enabled: boolPtr(true), Env: map[string]string{"GEN": "2"}},
		{Name: "b", Enabled: boolPtr(true), Env: map[string]string{"GEN": "2"}},
	}}
	if err := r.Reload(context.Background(), cfg2); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if after := len(j.events(t, "")); after <= before {
		t.Errorf("Reload journaled no catalog events (%d before, %d after); merge-time visibility is not Start-only",
			before, after)
	}
}

// TestTruncationSkipJournalsEvent (R7): max_result_bytes cannot cut a result
// with no text blocks, so the oversized result reaches the client untouched —
// the client contract is unchanged and the operator gets the explanation.
func TestTruncationSkipJournalsEvent(t *testing.T) {
	imageOnly := json.RawMessage(`{"content":[{"type":"image","data":"` + strings.Repeat("A", 2000) + `"}]}`)
	j, callLog := newJournal()
	cfg := &config.Config{
		MaxResultBytes: 300,
		Upstreams:      []config.Upstream{{Name: "a", Enabled: boolPtr(true)}},
	}
	fake := &fakeUpstream{name: "a", tools: []string{"shot"}, callResp: mcp.NewResult(mcp.IntID(1), imageOnly)}
	r := newTestRegistry(t, cfg, callLog, map[string]*fakeUpstream{"a": fake})
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	if _, err := r.CallTool(context.Background(), "a__shot", nil, nil); err != nil {
		t.Fatalf("CallTool: %v", err)
	}

	evs := j.events(t, logging.EventResultTruncationSkipped)
	if len(evs) != 1 {
		t.Fatalf("got %d result_truncation_skipped events, want 1; journal:\n%s", len(evs), j.bytes())
	}
	if evs[0].Upstream != "a" {
		t.Errorf("event upstream = %q, want %q", evs[0].Upstream, "a")
	}
	// Subject is the upstream's ORIGINAL tool name, not the namespaced one.
	if evs[0].Subject != "shot" {
		t.Errorf("event subject = %q, want the upstream-side tool name %q", evs[0].Subject, "shot")
	}
	if !strings.Contains(evs[0].Detail, "300") {
		t.Errorf("detail = %q, want it to name the limit that did not apply", evs[0].Detail)
	}
}

// TestHTTPUpstreamWithoutSSEJournalsEvent (R8): an HTTP upstream that refuses
// the optional GET stream will never send tools/list_changed for the life of
// this process. That is a permanent, invisible degradation — now journaled.
func TestHTTPUpstreamWithoutSSEJournalsEvent(t *testing.T) {
	base := fakeHTTPUpstream()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.Error(w, "no stream here", http.StatusMethodNotAllowed)
			return
		}
		base.ServeHTTP(w, r)
	}))
	defer srv.Close()

	j, callLog := newJournal()
	cfg := &config.Config{Upstreams: []config.Upstream{
		{Name: "remote", URL: srv.URL, Enabled: boolPtr(true)},
	}}
	r := New(cfg, quietLogger(), callLog, noopPayloadLog(), true, "0.0.0-test")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = r.Close() }()

	evs := j.waitForEvent(t, logging.EventSSEStreamUnavailable, 10*time.Second)
	if evs[0].Upstream != "remote" {
		t.Errorf("event upstream = %q, want %q", evs[0].Upstream, "remote")
	}
	if !strings.Contains(evs[0].Detail, "list_changed") {
		t.Errorf("detail = %q, want it to spell out the consequence", evs[0].Detail)
	}
}

// TestEventsAreNotJournaledWithoutCallLog pins the nil-guard: doctor, call and
// catalog build a registry without a journal and must not panic.
func TestEventsAreNotJournaledWithoutCallLog(t *testing.T) {
	r := New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
	r.emitEvent(logging.EventRecord{Event: logging.EventUpstreamStartFailed, Upstream: "x"})
	r.noteThrottled(logging.EventNotificationDropped, "", "notifications/progress", "detail", 1)
	r.flushEvents()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// --- Review round 2: the merge path is NOT cold (H1) and must not journal
// --- while r.mu is held (M2), and a coalesced line must date its own backlog
// --- (M4).

// relistFake is an upstream whose tools/list answer the test can swap at any
// moment (race-safe) and which counts the re-lists the registry really made.
// Everything else comes from the embedded fakeUpstream.
type relistFake struct {
	*fakeUpstream
	listMu sync.Mutex
	list   []mcp.Tool
	lists  atomic.Int64
}

func newRelistFake(name string, tools ...string) *relistFake {
	f := &relistFake{fakeUpstream: &fakeUpstream{name: name, caps: json.RawMessage(`{"tools":{}}`)}}
	f.setTools(tools...)
	return f
}

func (f *relistFake) setTools(names ...string) {
	out := make([]mcp.Tool, 0, len(names))
	for _, n := range names {
		out = append(out, mcp.Tool{
			Name: n, Description: json.RawMessage(`"d"`), InputSchema: json.RawMessage(`{"type":"object"}`),
		})
	}
	f.listMu.Lock()
	f.list = out
	f.listMu.Unlock()
}

func (f *relistFake) ListTools(context.Context) ([]mcp.Tool, error) {
	f.listMu.Lock()
	out := append([]mcp.Tool(nil), f.list...)
	f.listMu.Unlock()
	f.lists.Add(1)
	return out, nil
}

// relistRegistry starts a registry over one relistFake.
func relistRegistry(t *testing.T, f *relistFake, callLog logging.CallLog) *Registry {
	t.Helper()
	cfg := &config.Config{Upstreams: []config.Upstream{{Name: f.name, Enabled: boolPtr(true)}}}
	r := New(cfg, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")
	r.start = func(context.Context, config.Upstream) (Upstream, error) { return f, nil }
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return r
}

// fireRelist drives ONE list_changed → debounce → re-list cycle and waits until
// the re-list has actually fetched the catalog. runRelist serializes the cycles
// per upstream, so cycle N's merge is complete before cycle N+1's fetch starts.
func (f *relistFake) fireRelist(t *testing.T, r *Registry) {
	t.Helper()
	before := f.lists.Load()
	r.onUpstreamNotification(f.name, mcp.NotifToolsListChanged, nil)
	deadline := time.Now().Add(10 * time.Second)
	for f.lists.Load() <= before {
		if time.Now().After(deadline) {
			t.Fatalf("the re-list never ran after notifications/tools/list_changed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRelistDoesNotRejournalUnchangedCatalogDefects (review H1): an upstream
// firing tools/list_changed re-runs the WHOLE merge — including all four
// collision branches — every relistDebounce. A static defect (a duplicate name,
// a broken template) is a fact the operator needs ONCE; without the defect
// memory a noisy upstream rewrites the same line up to five times a second,
// forever. A defect that is genuinely NEW must still be reported at once, which
// is what the second half asserts (and what makes the first half a real
// assertion rather than "events stopped working").
func TestRelistDoesNotRejournalUnchangedCatalogDefects(t *testing.T) {
	j, callLog := newJournal()
	f := newRelistFake("a", "dup", "dup")
	r := relistRegistry(t, f, callLog)
	defer func() { _ = r.Close() }()

	first := j.waitForEvent(t, logging.EventCatalogCollision, 5*time.Second)
	if len(first) != 1 || first[0].Subject != "a__dup" {
		t.Fatalf("start journaled %+v, want exactly one collision on a__dup", first)
	}

	// Three re-lists of the very same (still broken) catalog.
	for i := 0; i < 3; i++ {
		f.fireRelist(t, r)
	}

	// A genuinely new defect: it must appear, and its appearance doubles as the
	// barrier proving the three repeats above have long since merged.
	f.setTools("dup", "dup", "dup2", "dup2")
	f.fireRelist(t, r)
	deadline := time.Now().Add(5 * time.Second)
	for len(j.events(t, logging.EventCatalogCollision)) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("a NEW collision was never journaled; journal:\n%s", j.bytes())
		}
		time.Sleep(10 * time.Millisecond)
	}

	evs := j.events(t, logging.EventCatalogCollision)
	if len(evs) != 2 {
		t.Fatalf("got %d catalog_collision events, want exactly 2 (a__dup once at start, a__dup2 once when it appeared); journal:\n%s",
			len(evs), j.bytes())
	}
	if evs[0].Subject != "a__dup" || evs[1].Subject != "a__dup2" {
		t.Errorf("collision subjects = %q/%q, want a__dup then a__dup2", evs[0].Subject, evs[1].Subject)
	}
}

// blockingJournal parks the FIRST write made after it is armed until the test
// releases it — standing in for the real hazard: with log_file unset the journal
// is os.Stderr, a pipe to the MCP client in stdio mode, whose 64 KiB buffer a
// client that stopped reading fills, blocking write(2) forever.
type blockingJournal struct {
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}

	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *blockingJournal) Write(p []byte) (int, error) {
	if b.armed.CompareAndSwap(true, false) {
		b.entered <- struct{}{}
		<-b.release
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// TestCatalogEventIsNotJournaledUnderRegistryLock (review M2, and the teeth of
// H1): the re-list path takes r.mu — the registry's central lock, needed by
// tools/list and by tools/call routing — and used to journal from under it. A
// journal write that never returns would therefore wedge the whole gateway.
// Here the write is deliberately parked and a catalog READER (ToolCount, which
// takes r.mu.RLock) must still be served immediately.
func TestCatalogEventIsNotJournaledUnderRegistryLock(t *testing.T) {
	j := &blockingJournal{entered: make(chan struct{}, 1), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(j.release) }) }
	defer unblock()

	f := newRelistFake("a", "one")
	r := relistRegistry(t, f, logging.NewCallLogWriter(j))
	defer func() { _ = r.Close() }()

	// Start journaled nothing (a clean catalog). Arm the trap, then make the
	// next re-list find a collision, which is what triggers the write.
	j.armed.Store(true)
	f.setTools("dup", "dup")
	f.fireRelist(t, r)

	select {
	case <-j.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the re-list never journaled its collision event, so this test proves nothing")
	}

	// The journal write is parked right now. Reading the catalog must not be.
	done := make(chan int, 1)
	go func() { done <- r.ToolCount() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		unblock() // let the parked writer finish so the test can unwind
		t.Fatal("ToolCount() blocked while an operator event was being journaled: the write is happening under r.mu")
	}
	unblock()
}

// TestThrottledBacklogNamesItsOwnStart (review M4): the throttle window is only
// re-checked on the NEXT occurrence of a key, so a key that goes quiet leaves
// its remainder unreported for as long as it stays quiet — and the line that
// finally carries it is stamped NOW. Without the backlog text an operator reads
// "count=3, just now" off a line whose other two occurrences are a day old.
func TestThrottledBacklogNamesItsOwnStart(t *testing.T) {
	j, callLog := newJournal()
	r := New(&config.Config{}, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")

	const ev = logging.EventNotificationDropped
	const subject = "notifications/progress"
	const detail = "subscriber buffer full"
	r.noteThrottled(ev, "", subject, detail, 1) // journaled at once: opens the window
	r.noteThrottled(ev, "", subject, detail, 1) // suppressed
	r.noteThrottled(ev, "", subject, detail, 1) // suppressed

	// Age the state: those two happened a day ago and the key then went silent.
	dayAgo := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	r.eventMu.Lock()
	st := r.eventSeen[eventKey{event: ev, subject: subject}]
	if st == nil || st.suppressed != 2 {
		r.eventMu.Unlock()
		t.Fatalf("throttle state = %+v, want 2 suppressed occurrences", st)
	}
	st.firstHeld = dayAgo
	st.lastEmit = dayAgo
	r.eventMu.Unlock()

	r.noteThrottled(ev, "", subject, detail, 1) // the fourth: carries the backlog

	evs := j.events(t, ev)
	if len(evs) != 2 {
		t.Fatalf("journal has %d lines, want 2 (window opener + the backlog carrier):\n%s", len(evs), j.bytes())
	}
	late := evs[1]
	if late.Count != 3 {
		t.Errorf("backlog line count = %d, want 3 (2 suppressed + the current one)", late.Count)
	}
	if !strings.Contains(late.Detail, dayAgo.Format(time.RFC3339)) {
		t.Errorf("backlog line does not say WHEN its backlog started (want %s):\ndetail = %q",
			dayAgo.Format(time.RFC3339), late.Detail)
	}
	if late.Time.Sub(dayAgo) < 23*time.Hour {
		t.Fatalf("line time %s is not the fresh occurrence — the test aged nothing", late.Time)
	}
}

// TestSimultaneousDropsAreOneCountedLine: when a notification is dropped by two
// wedged subscribers at once, that is TWO occurrences that happened at the same
// instant, and the journal must say so on one line. The counting loop this
// replaced called noteThrottled once per drop, so the first call opened the
// window with count=1 and the second was merely suppressed — an operator saw
// "one drop" until the next occurrence or the shutdown flush finally revealed
// the rest. Close() at the end is part of the assertion: a leftover remainder
// would surface there as a second line.
func TestSimultaneousDropsAreOneCountedLine(t *testing.T) {
	j, callLog := newJournal()
	r := New(&config.Config{}, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")

	_, unsub1 := r.SubscribeNotifications() // both subscribed and never reading
	defer unsub1()
	_, unsub2 := r.SubscribeNotifications()
	defer unsub2()

	// Fill both buffers: every subscriber gets every notification, so after
	// notifSubBuffer sends both are exactly full.
	for i := 0; i < notifSubBuffer; i++ {
		r.onUpstreamNotification("web", mcp.NotifProgress, json.RawMessage(`{"progress":1}`))
	}
	// This one is dropped by BOTH subscribers, inside a single notifMu hold.
	r.onUpstreamNotification("web", mcp.NotifProgress, json.RawMessage(`{"progress":1}`))

	evs := j.events(t, logging.EventNotificationDropped)
	if len(evs) != 1 {
		t.Fatalf("got %d notification_dropped lines, want exactly 1; journal:\n%s", len(evs), j.bytes())
	}
	if got := evs[0].Occurrences(); got != 2 {
		t.Errorf("line reports %d occurrence(s), want 2 (one drop per wedged subscriber)", got)
	}
	if evs[0].Subject != mcp.NotifProgress {
		t.Errorf("subject = %q, want the dropped method %q", evs[0].Subject, mcp.NotifProgress)
	}
	if evs[0].Detail == "" {
		t.Error("event carries no detail; the operator needs the drop reason")
	}
	if strings.Contains(evs[0].Detail, "coalesced into this line") {
		t.Errorf("simultaneous drops must not be dated to a backlog that does not exist: detail = %q", evs[0].Detail)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if evs := j.events(t, logging.EventNotificationDropped); len(evs) != 1 {
		t.Fatalf("the shutdown flush emitted a remainder, so the drops were not all counted into the first line: %d lines; journal:\n%s",
			len(evs), j.bytes())
	}
}

// TestNoteThrottledAccumulatesWithinOpenWindow pins the branch no existing test
// drives: a SECOND call landing while the window is still open, carrying n > 1.
// The naive-looking mutation "st.suppressed += n" -> "st.suppressed++" passes
// every other test in this file (they only ever pass n=1 while suppressed), but
// silently discards (n-1) occurrences of any multi-occurrence call arriving
// mid-window — exactly the shape forwardNotification produces when several
// subscribers wedge on the same message. This test opens the window with n=1,
// then feeds n=3 into the open window, and requires the eventual emission to
// account for all 3, not 1.
func TestNoteThrottledAccumulatesWithinOpenWindow(t *testing.T) {
	j, callLog := newJournal()
	r := New(&config.Config{}, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")

	const ev = logging.EventNotificationDropped
	const subject = "notifications/progress"
	const detail = "subscriber buffer full"

	r.noteThrottled(ev, "", subject, detail, 1) // opens the window, journaled at once
	r.noteThrottled(ev, "", subject, detail, 3) // lands INSIDE the open window

	r.eventMu.Lock()
	st := r.eventSeen[eventKey{event: ev, subject: subject}]
	if st == nil {
		r.eventMu.Unlock()
		t.Fatal("no throttle state recorded for the key")
	}
	suppressed := st.suppressed
	r.eventMu.Unlock()
	if suppressed != 3 {
		t.Fatalf("suppressed = %d, want 3 (the mutation st.suppressed++ would leave 1 here)", suppressed)
	}

	// Force the window closed and drive an emission that must carry the backlog.
	r.eventMu.Lock()
	st.lastEmit = time.Now().Add(-2 * eventThrottleWindow)
	r.eventMu.Unlock()
	r.noteThrottled(ev, "", subject, detail, 1) // the fifth occurrence: reopens, carries the backlog

	evs := j.events(t, ev)
	if len(evs) != 2 {
		t.Fatalf("journal has %d lines, want 2 (window opener + the backlog carrier); journal:\n%s", len(evs), j.bytes())
	}
	if got := evs[1].Occurrences(); got != 4 {
		t.Errorf("backlog line reports %d occurrence(s), want 4 (3 suppressed + the current one) — "+
			"st.suppressed += n must have run, not st.suppressed++", got)
	}
}

// TestNoteThrottledNonPositiveIsNoOp pins the guard `n <= 0` at the very top of
// noteThrottled: zero (and negative) occurrences must not journal anything, and
// — this is the part a naive removal of the guard breaks first — must not even
// create an entry in r.eventSeen. The map entry matters because its mere
// existence changes later behavior (a zero-value eventState reads as "window
// never opened"); a call that did nothing must leave no trace at all.
func TestNoteThrottledNonPositiveIsNoOp(t *testing.T) {
	j, callLog := newJournal()
	r := New(&config.Config{}, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")

	const ev = logging.EventNotificationDropped
	const subject = "notifications/zero"
	const detail = "should never appear"

	r.noteThrottled(ev, "", subject, detail, 0)
	r.noteThrottled(ev, "", subject, detail, -5)

	if evs := j.events(t, ev); len(evs) != 0 {
		t.Fatalf("got %d %s lines from n<=0 calls, want 0; journal:\n%s", len(evs), ev, j.bytes())
	}
	r.eventMu.Lock()
	_, seen := r.eventSeen[eventKey{event: ev, subject: subject}]
	r.eventMu.Unlock()
	if seen {
		t.Error("n<=0 must not create an eventSeen entry for the key (the guard runs before state is touched)")
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if evs := j.events(t, ev); len(evs) != 0 {
		t.Fatalf("the shutdown flush journaled %d lines for a key that was never touched; journal:\n%s", len(evs), j.bytes())
	}
}

// TestNoteThrottledConcurrent is the ONLY test in this file (or the package)
// that calls noteThrottled from more than one goroutine at once. Every other
// test drives it from a single call site, so a green `go test -race` elsewhere
// proves nothing about the locking contract documented on noteThrottled ("takes
// only the leaf eventMu, emitEvent is called outside it"). This test fires many
// goroutines at a shared key and at per-goroutine distinct keys simultaneously,
// then requires the journaled counts plus whatever flushEvents recovers at
// shutdown to sum to exactly the number of occurrences sent — a test on
// conservation, not merely on the absence of a panic or a race-detector flag.
func TestNoteThrottledConcurrent(t *testing.T) {
	j, callLog := newJournal()
	r := New(&config.Config{}, quietLogger(), callLog, noopPayloadLog(), false, "0.0.0-test")

	const ev = logging.EventNotificationDropped
	const sharedSubject = "notifications/shared"
	const goroutines = 20
	const perGoroutine = 25 // occurrences sent by each goroutine to the shared key

	var wg sync.WaitGroup
	// Half the goroutines hammer one shared key (contention on the same
	// eventState); the other half each own a distinct key (contention on the
	// map itself, under eventMu, with no cross-key interference).
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; n < perGoroutine; n++ {
				r.noteThrottled(ev, "", sharedSubject, "concurrent shared", 1)
			}
		}(i)
	}
	const distinctGoroutines = 10
	for i := 0; i < distinctGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			subject := fmt.Sprintf("notifications/distinct-%d", i)
			for n := 0; n < perGoroutine; n++ {
				r.noteThrottled(ev, "", subject, "concurrent distinct", 3)
			}
		}(i)
	}
	wg.Wait()

	if err := r.Close(); err != nil { // flushes whatever remained suppressed
		t.Fatalf("Close: %v", err)
	}

	wantShared := goroutines * perGoroutine
	wantDistinct := distinctGoroutines * perGoroutine * 3
	gotShared := 0
	gotDistinct := 0
	for _, e := range j.events(t, ev) {
		n := e.Occurrences()
		if e.Subject == sharedSubject {
			gotShared += n
		} else if strings.HasPrefix(e.Subject, "notifications/distinct-") {
			gotDistinct += n
		}
	}
	if gotShared != wantShared {
		t.Errorf("shared-key occurrences journaled+flushed = %d, want %d", gotShared, wantShared)
	}
	if gotDistinct != wantDistinct {
		t.Errorf("distinct-key occurrences journaled+flushed = %d, want %d", gotDistinct, wantDistinct)
	}
}
