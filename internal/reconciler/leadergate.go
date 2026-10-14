package reconciler

import (
	"context"
	"errors"
	"time"
)

// ErrLeadershipStale is returned by a LeaderGate when the caller may no
// longer issue cluster writes: its last successful Lease renewal is older
// than the freshness bound, so another replica may already hold the Lease
// even though the elector has not given up yet.
var ErrLeadershipStale = errors.New("leader election renewal is stale")

// LeaderGate reports whether the caller may still issue cluster writes.
// It is carried through the controllers' context by the leadership wiring;
// controllers without a gate (unit tests, standalone use) always may.
type LeaderGate interface {
	CanWrite() error
	// Remaining reports how much of the write window is left: the time
	// until the last successful Lease renewal ages past the freshness
	// bound. It is zero or negative once the window has passed.
	Remaining() time.Duration
}

type leaderGateKey struct{}

// WithLeaderGate returns a context carrying gate, so the write paths deep
// inside the controllers can consult it without constructor plumbing.
func WithLeaderGate(ctx context.Context, gate LeaderGate) context.Context {
	return context.WithValue(ctx, leaderGateKey{}, gate)
}

// CanWrite returns nil while the gate in ctx allows cluster writes. A context
// without a gate always allows them: only the leader-election wiring installs
// one.
func CanWrite(ctx context.Context) error {
	if gate, ok := ctx.Value(leaderGateKey{}).(LeaderGate); ok {
		return gate.CanWrite()
	}
	return nil
}

// WriteContext derives a write context from ctx with the given timeout,
// capped by the write window left on the gate in ctx, if any. The cap
// anchors the deadline to the last successful Lease renewal, so a process
// paused between the gate check and the request resumes to an expired
// write context instead of sending the request after the freshness bound,
// and a request already in flight is aborted at that same point. Contexts
// without a gate keep the full timeout.
func WriteContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if gate, ok := ctx.Value(leaderGateKey{}).(LeaderGate); ok {
		if remaining := gate.Remaining(); remaining < timeout {
			return context.WithTimeout(ctx, remaining)
		}
	}
	return context.WithTimeout(ctx, timeout)
}
