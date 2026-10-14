// Package leadership gates reconciliation behind a management-cluster Lease.
package leadership

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordination "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/reconciler"
)

// The 60s gap between renewal failure and lease expiration exceeds the 30s
// SSA/DELETE timeouts. Lease election is not fencing: cancellation cannot undo
// requests already accepted by the API server, or protect against arbitrary pauses.
const (
	LeaseDuration = 120 * time.Second
	RenewDeadline = 60 * time.Second
	RetryPeriod   = 10 * time.Second
)

// ErrLeadershipLost makes the process exit rather than restart stopped controllers.
var ErrLeadershipLost = errors.New("management-cluster leadership lost")

// LeaseName is stable across releases and valid for arbitrary partition identifiers.
func LeaseName(managementCluster string) string {
	return fmt.Sprintf("hyperfleet-applier-%x", sha256.Sum256([]byte(managementCluster)))
}

// Runnable starts its controllers only while holding the Lease. It is single-use.
type Runnable struct {
	config    leaderelection.LeaderElectionConfig
	runnables []reconciler.Runnable
}

// New builds an elector; namespace and identity must be non-empty.
func New(client coordination.CoordinationV1Interface, namespace, managementCluster, identity string,
	runnables ...reconciler.Runnable,
) (*Runnable, error) {
	if namespace == "" || managementCluster == "" || identity == "" {
		return nil, errors.New("leader election requires namespace, management cluster and identity")
	}
	return &Runnable{
		config: leaderelection.LeaderElectionConfig{
			Lock: &resourcelock.LeaseLock{
				LeaseMeta:  metav1.ObjectMeta{Namespace: namespace, Name: LeaseName(managementCluster)},
				Client:     client,
				LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
			},
			LeaseDuration: LeaseDuration, RenewDeadline: RenewDeadline, RetryPeriod: RetryPeriod,
			ReleaseOnCancel: false,
			Name:            "hyperfleet-applier",
		},
		runnables: runnables,
	}, nil
}

// Start waits on standby, then runs until shutdown, controller exit, or lease loss.
// It waits for all controllers before returning and never releases the Lease early.
func (r *Runnable) Start(ctx context.Context) error {
	electionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var acquired atomic.Bool
	finished := make(chan error, 1)
	cfg := r.config
	cfg.Callbacks = leaderelection.LeaderCallbacks{
		OnNewLeader: func(identity string) {
			// Runs synchronously in the elector loop before Run returns, so the
			// flag is visible to the read below; OnStartedLeading runs on a
			// separate goroutine with no such guarantee. Never reset when
			// another holder is reported: once led, finished must be drained.
			if identity == cfg.Lock.Identity() {
				acquired.Store(true)
			}
		},
		OnStartedLeading: func(leaderCtx context.Context) {
			slog.InfoContext(leaderCtx, "acquired management-cluster leadership", "lease", cfg.Lock.Describe())
			finished <- runControllers(leaderCtx, cancel, r.runnables)
		},
		OnStoppedLeading: func() {},
	}
	elector, err := leaderelection.NewLeaderElector(cfg)
	if err != nil {
		return fmt.Errorf("initialize leader election: %w", err)
	}
	elector.Run(electionCtx)
	var controllerErr error
	if acquired.Load() {
		controllerErr = <-finished
	}
	if ctx.Err() != nil {
		return controllerErr
	}
	if controllerErr != nil {
		return controllerErr
	}
	// Reaching this point means the parent is alive and every controller
	// returned nil: the only root cause left is a failed Lease renewal.
	slog.WarnContext(ctx, "lost management-cluster leadership", "lease", cfg.Lock.Describe())
	return ErrLeadershipLost
}

func runControllers(ctx context.Context, stopElection context.CancelFunc, runnables []reconciler.Runnable) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(runnables))
	var wg sync.WaitGroup
	for _, runnable := range runnables {
		wg.Go(func() {
			if err := runnable.Start(runCtx); err != nil {
				errs <- err
			}
			cancel()
			stopElection()
		})
	}
	wg.Wait()
	close(errs)
	var result []error
	for err := range errs {
		result = append(result, err)
	}
	return errors.Join(result...)
}
