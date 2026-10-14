package reconciler

import (
	"context"
	"errors"
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
