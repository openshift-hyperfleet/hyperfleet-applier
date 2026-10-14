// Package serial runs one-pass reconcilers on a single poll loop.
// Each tick calls RunOnce on every reconciler, in order, before the next tick.
package serial

import (
	"context"
	"log/slog"
	"time"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/reconciler"
)

// Runnable calls reconcilers serially and implements reconciler.Runnable.
// The applier passes apply, then delete, so an apply cannot land after a
// delete of the same target in the same tick. This requires exactly one active
// loop per management cluster, enforced by leader election across replicas.
// The read controller stays a separate reconciler.Runnable behind the same Lease.
type Runnable struct {
	reconcilers []reconciler.Reconcilable
	interval    time.Duration
}

// New builds a Runnable that calls reconcilers in argument order on each tick.
func New(interval time.Duration, reconcilers ...reconciler.Reconcilable) *Runnable {
	return &Runnable{interval: interval, reconcilers: reconcilers}
}

// Start reconciles immediately, then repeats at the fixed polling cadence
// until ctx is canceled. A non-cancel failure is logged and does not skip the
// rest of the tick. Caller-driven shutdown returns nil and does not start a
// later reconciler after an earlier one was aborted.
func (r *Runnable) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		for _, rec := range r.reconcilers {
			if ctx.Err() != nil {
				return nil
			}
			if err := rec.RunOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				slog.ErrorContext(ctx, "reconciliation pass failed", "error", err)
			}
			if ctx.Err() != nil {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
