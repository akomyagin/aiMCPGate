package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
			// The resources capability must be declared or launch never lists them.
			"a": {name: "a", tools: []string{"dup", "dup"}, resources: []string{"file:///shared"},
				templates: []string{badUTF8Template}, caps: json.RawMessage(`{"tools":{},"resources":{}}`)},
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
	if len(collisions) != 2 {
		t.Fatalf("got %d catalog_collision events, want 2 (dup tool + dup uri); journal:\n%s",
			len(collisions), j.bytes())
	}
	var sawTool, sawURI bool
	for _, e := range collisions {
		switch e.Subject {
		case "a__dup":
			sawTool = true
			if e.Upstream != "a" {
				t.Errorf("tool collision upstream = %q, want the loser %q", e.Upstream, "a")
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
	if !sawTool || !sawURI {
		t.Errorf("collisions = %+v, want both the duplicate tool and the duplicate uri", collisions)
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
	r.noteThrottled(logging.EventNotificationDropped, "", "notifications/progress", "detail")
	r.flushEvents()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
