package upstream

// White-box test (package upstream, not upstream_test): it must reach through
// Conn's unexported transport field to inspect the http.Client StartHTTP built,
// which no exported API reveals.

import (
	"io"
	"log/slog"
	"net/http"
	"testing"
)

// TestStartHTTPNilClientGetsDedicatedTransport is a regression test: StartHTTP
// with a nil client used to fall back to ONE package-level client whose
// Transport was the process-global http.DefaultTransport — so Close on any one
// upstream (e.g. a retire on config reload) severed the keep-alive pool of
// every other HTTP upstream, and of everything else in the binary using the
// default transport. Each connection must instead get its own client with its
// own cloned transport, and no client-wide Timeout (per-call deadlines come
// from the registry's context, see StartHTTP).
func TestStartHTTPNilClientGetsDedicatedTransport(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	connA := StartHTTP(log, "a", "http://127.0.0.1:1/mcp", nil, nil, "0.0.0-test")
	connB := StartHTTP(log, "b", "http://127.0.0.2:1/mcp", nil, nil, "0.0.0-test")
	defer func() { _ = connA.Close(); _ = connB.Close() }()

	clientA := connA.transport.(*httpTransport).client
	clientB := connB.transport.(*httpTransport).client

	if clientA == clientB {
		t.Fatal("two StartHTTP(nil) connections share one http.Client")
	}
	for name, cl := range map[string]*http.Client{"a": clientA, "b": clientB} {
		if cl.Timeout != 0 {
			t.Errorf("conn %q: client.Timeout = %v, want 0 (per-call ctx deadlines only)", name, cl.Timeout)
		}
		if cl.Transport == http.DefaultTransport {
			t.Errorf("conn %q: client shares the process-global http.DefaultTransport", name)
		}
		if _, ok := cl.Transport.(*http.Transport); !ok {
			t.Errorf("conn %q: Transport is %T, want *http.Transport (Close's CloseIdleConnections assertion)", name, cl.Transport)
		}
	}
	if clientA.Transport == clientB.Transport {
		t.Error("two StartHTTP(nil) connections share one *http.Transport; Close on one would idle the other's connections")
	}
}
