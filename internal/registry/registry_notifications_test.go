package registry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/akomyagin/aiMCPGate/internal/config"
	"github.com/akomyagin/aiMCPGate/internal/mcp"
)

// newNotifTestRegistry builds a registry good enough for the notification
// plumbing tests: onUpstreamNotification / SubscribeNotifications touch none
// of the catalog or lifecycle state, so no Start is needed.
func newNotifTestRegistry() *Registry {
	return New(&config.Config{}, quietLogger(), nil, noopPayloadLog(), false, "0.0.0-test")
}

// TestSubscribeNotificationsForwardsProgress: a notifications/progress pushed
// by an upstream reaches every subscriber with method AND params verbatim
// (Round 2), and stops arriving after unsubscribe.
func TestSubscribeNotificationsForwardsProgress(t *testing.T) {
	r := newNotifTestRegistry()
	ch, unsubscribe := r.SubscribeNotifications()

	params := json.RawMessage(`{"progressToken":42,"progress":1,"total":3}`)
	r.onUpstreamNotification("web", mcp.NotifProgress, params)

	select {
	case msg := <-ch:
		if msg.Method != mcp.NotifProgress {
			t.Errorf("forwarded method = %q, want %q", msg.Method, mcp.NotifProgress)
		}
		if string(msg.Params) != string(params) {
			t.Errorf("forwarded params = %s, want verbatim %s", msg.Params, params)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber never received the forwarded progress notification")
	}

	unsubscribe()
	r.onUpstreamNotification("web", mcp.NotifProgress, params)
	select {
	case msg := <-ch:
		t.Fatalf("received %q after unsubscribe", msg.Method)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestSubscribeNotificationsDoesNotForwardListChanged: tools/list_changed is
// the DEBOUNCE path (Stage 7b), not the verbatim-forward path — a notification
// subscriber must not see it.
func TestSubscribeNotificationsDoesNotForwardListChanged(t *testing.T) {
	r := newNotifTestRegistry()
	ch, unsubscribe := r.SubscribeNotifications()
	defer unsubscribe()

	r.onUpstreamNotification("web", mcp.NotifToolsListChanged, nil)
	select {
	case msg := <-ch:
		t.Fatalf("list_changed leaked to the notification subscribers: %q", msg.Method)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestSubscribeNotificationsOverflowDoesNotBlockPublisher: forwardNotification
// runs on an upstream's single reader goroutine, so a subscriber that never
// drains its channel must cost the publisher NOTHING but dropped messages —
// a blocking send here would wedge the whole upstream connection (Round 2).
func TestSubscribeNotificationsOverflowDoesNotBlockPublisher(t *testing.T) {
	r := newNotifTestRegistry()
	// Never drained: the buffer (notifSubBuffer) fills and every further send
	// must fall through to the drop branch without blocking.
	_, unsubscribe := r.SubscribeNotifications()
	defer unsubscribe()

	const burst = 10 * notifSubBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < burst; i++ {
			r.onUpstreamNotification("web", mcp.NotifProgress, json.RawMessage(`{"progress":1}`))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher blocked on an undrained notification subscriber")
	}
}
