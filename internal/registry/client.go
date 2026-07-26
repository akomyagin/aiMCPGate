package registry

import "context"

// clientKey is the private context key carrying the calling client's identity
// (standard context-key pattern: an unexported empty struct type, so no other
// package can collide with — or read/write — the value except through the two
// helpers below).
type clientKey struct{}

// WithClient returns a context carrying the client identity string — the
// "name/version" a transport extracted from the client's initialize request.
// The transport attaches it once (per connection for stdio, per request for
// HTTP) and every registry call made under that context is audited with it
// (CallRecord.Client). An empty client is a no-op: ctx is returned unchanged,
// so callers need no guard.
func WithClient(ctx context.Context, client string) context.Context {
	if client == "" {
		return ctx
	}
	return context.WithValue(ctx, clientKey{}, client)
}

// ClientFromContext returns the client identity attached by WithClient, or ""
// when none was attached (client not yet initialized, or the transport cannot
// identify it — see the HTTP limitation in transport.handlePost).
func ClientFromContext(ctx context.Context) string {
	client, _ := ctx.Value(clientKey{}).(string)
	return client
}
