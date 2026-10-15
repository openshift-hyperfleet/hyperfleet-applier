// Package leadership gates reconciliation behind a management-cluster Lease.
//
// Besides the context-based stop, every SSA and DELETE issued by the
// controllers is gated on Lease freshness: the leader's last successful Lease
// write must be younger than LeaseDuration minus one RetryPeriod. A leader
// that stalls past the Lease duration and resumes (SIGSTOP, VM freeze, heavy
// throttling) would otherwise keep writing alongside the new leader for up to
// RenewDeadline, because client-go only cancels the leader context when the
// renew poll exhausts its full RenewDeadline - possibly a fresh one started
// right after the resume. When the gate goes stale it cancels the
// controllers' run context, so the process exits as it does on renewal
// failure.
//
// The gate is both a check and a bound: each write context is capped at the
// last successful renewal plus the freshness bound (reconciler.WriteContext),
// so a process paused between the check and the request resumes to an
// expired write context instead of sending it, and a request already in
// flight is aborted at the bound. What no client-side mechanism can do is
// undo a request the API server already accepted inside the window.
package leadership

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
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
// On a deliberate exit the Lease is handed over after the controllers drain, so
// only ungraceful exits (crash, freeze, SIGKILL) wait out the expiry.
const (
	LeaseDuration = 120 * time.Second
	RenewDeadline = 60 * time.Second
	RetryPeriod   = 10 * time.Second

	// handOverLeaseDurationSeconds is the Lease duration the hand-over
	// writes on a deliberate exit: already expired, the same shape
	// client-go's own release writes.
	handOverLeaseDurationSeconds = 1
)

// ErrLeadershipLost makes the process exit rather than restart stopped controllers.
var ErrLeadershipLost = errors.New("management-cluster leadership lost")

// LeaseName is stable across releases and valid for arbitrary partition identifiers.
func LeaseName(managementCluster string) string {
	return fmt.Sprintf("hyperfleet-applier-%x", sha256.Sum256([]byte(managementCluster)))
}

// Runnable starts its controllers only while holding the Lease. It is single-use.
type Runnable struct {
	lock *trackingLock
	// onStartedLeadingDelay, when set, runs at the top of OnStartedLeading.
	// Tests use it to reproduce the client-go goroutine starting that
	// callback after Run has already returned; it is nil in production.
	onStartedLeadingDelay func(ctx context.Context)
	runnables             []reconciler.Runnable
	config                leaderelection.LeaderElectionConfig
}

// New builds an elector; namespace and identity must be non-empty.
func New(client coordination.CoordinationV1Interface, namespace, managementCluster, identity string,
	runnables ...reconciler.Runnable,
) (*Runnable, error) {
	if namespace == "" || managementCluster == "" || identity == "" {
		return nil, errors.New("leader election requires namespace, management cluster and identity")
	}
	lock := &trackingLock{Interface: &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Namespace: namespace, Name: LeaseName(managementCluster)},
		Client:     client,
		LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
	}}
	return &Runnable{
		config: leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: LeaseDuration, RenewDeadline: RenewDeadline, RetryPeriod: RetryPeriod,
			ReleaseOnCancel: false,
			Name:            "hyperfleet-applier",
		},
		lock:      lock,
		runnables: runnables,
	}, nil
}

// Start waits on standby, then runs until shutdown, controller exit, or lease loss.
// It waits for all controllers before returning. On a deliberate exit (graceful
// shutdown or controller failure) it hands the Lease over after the drain, so
// the next owner takes over within one retry period; ungraceful exits still
// wait out the Lease expiry.
func (r *Runnable) Start(ctx context.Context) error {
	electionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	cfg := r.config
	// Lease freshness: while the last successful renewal is younger than
	// LeaseDuration minus one retry period, no standby could have acquired
	// (they only take an expired Lease), so writes are safe. The retry-period
	// margin covers clock discrepancy between this process's monotonic
	// measurement and the standby's judgement of the Lease record.
	gate := &staleGate{
		lock:          r.lock,
		bound:         cfg.LeaseDuration - cfg.RetryPeriod,
		watchInterval: cfg.RetryPeriod,
	}
	cfg.Callbacks = leaderelection.LeaderCallbacks{
		OnStartedLeading: func(leaderCtx context.Context) {
			if r.onStartedLeadingDelay != nil {
				r.onStartedLeadingDelay(leaderCtx)
			}
			slog.InfoContext(leaderCtx, "acquired management-cluster leadership", "lease", cfg.Lock.Describe())
			finished <- runControllers(leaderCtx, cancel, gate, r.runnables)
		},
		OnStoppedLeading: func() {},
	}
	elector, err := leaderelection.NewLeaderElector(cfg)
	if err != nil {
		return fmt.Errorf("initialize leader election: %w", err)
	}
	elector.Run(electionCtx)
	var controllerErr error
	// The Lease write that recorded the acquisition ran synchronously in the
	// elector goroutine, so it is settled before Run can return - unlike
	// OnStartedLeading, which client-go starts on its own goroutine and may
	// not have run a single statement yet. Whenever the Lease was written,
	// finished is coming and must be drained before the caller closes
	// anything the controllers use.
	if r.lock.everSucceeded() {
		controllerErr = <-finished
	}
	if ctx.Err() != nil {
		// Graceful shutdown: the controllers have drained, so hand the
		// Lease over now instead of making the next owner wait out the
		// expiry.
		r.releaseLease(ctx)
		return controllerErr
	}
	if controllerErr != nil {
		// The process is exiting on a controller failure; same hand-over.
		r.releaseLease(ctx)
		return controllerErr
	}
	// Reaching this point means the parent is alive and every controller
	// returned nil: the only root cause left is a failed Lease renewal.
	// No hand-over here: either another replica holds the Lease or the API
	// is unreachable, so the expiry remains the takeover bound.
	slog.WarnContext(ctx, "lost management-cluster leadership", "lease", cfg.Lock.Describe())
	return ErrLeadershipLost
}

// releaseLease hands the Lease over on a deliberate exit: the controllers
// have drained by the time it runs, so writing an already-expired record
// lets the next replica take over within one retry period instead of
// waiting out LeaseDuration. Deliberate exits are graceful shutdown and
// controller failure; leadership loss still relies on the expiry, and a
// crash or freeze never runs this at all, so the ungraceful-takeover bound
// is unchanged. The write is conditional on a fresh read still naming this
// replica as the holder, and the Lease's resource version protects the
// check-then-update against a concurrent takeover.
func (r *Runnable) releaseLease(parent context.Context) {
	if !r.lock.everSucceeded() {
		// Never led: nothing to hand over.
		return
	}
	// The parent is already canceled on the graceful-shutdown path and the
	// hand-over must still run: keep its values, shed its cancellation.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), RenewDeadline)
	defer cancel()
	record, _, err := r.lock.Get(ctx)
	if err != nil {
		// Gone or unreadable: the next owner falls back to the expiry.
		slog.Warn("hand over lease: read", "lease", r.lock.Describe(), "error", err)
		return
	}
	if record.HolderIdentity != r.lock.Identity() {
		return
	}
	now := metav1.NewTime(time.Now())
	if err := r.lock.Update(ctx, resourcelock.LeaderElectionRecord{
		LeaderTransitions:    record.LeaderTransitions,
		LeaseDurationSeconds: handOverLeaseDurationSeconds,
		RenewTime:            now,
		AcquireTime:          now,
	}); err != nil {
		slog.Warn("hand over lease: update", "lease", r.lock.Describe(), "error", err)
		return
	}
	slog.Info("handed over management-cluster lease", "lease", r.lock.Describe())
}

// runControllers starts every runnable under a context carrying the
// leadership gate, cancels the election when any of them exits, and waits
// for all of them plus the freshness watcher before returning.
func runControllers(ctx context.Context, stopElection context.CancelFunc, gate *staleGate,
	runnables []reconciler.Runnable,
) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	gate.cancel = cancel
	runCtx = reconciler.WithLeaderGate(runCtx, gate)
	// Stop the controllers promptly once the Lease goes stale even when no
	// write is attempted; the per-write gates keep every SSA and DELETE off
	// the wire in the meantime.
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(gate.watchInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if gate.CanWrite() != nil {
					return
				}
			}
		}
	}()
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
	cancel()
	<-watchDone
	close(errs)
	var result []error
	for err := range errs {
		result = append(result, err)
	}
	return errors.Join(result...)
}

// trackingLock records the time of each successful Lease write so the
// controllers can refuse cluster writes once the last renewal goes stale.
// It wraps the lock the elector uses, so Create and Update succeed exactly
// when the elector's acquisition or renewal did.
type trackingLock struct {
	resourcelock.Interface
	lastSuccess time.Time
	mu          sync.Mutex
}

// Create records the issue time of a successful Lease creation.
func (l *trackingLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	// The issue time, not the response time, is the freshness point: the
	// apiserver may commit a write long before a stalled process handles
	// the response, and recording the response time would read a
	// straddled renewal as fresh after a resume (HYPERFLEET-1777).
	started := time.Now()
	if err := l.Interface.Create(ctx, ler); err != nil {
		return err
	}
	l.markSuccess(started)
	return nil
}

// Update records the issue time of a successful Lease renewal.
func (l *trackingLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	started := time.Now()
	if err := l.Interface.Update(ctx, ler); err != nil {
		return err
	}
	l.markSuccess(started)
	return nil
}

// markSuccess records at, the time a successful Lease write was issued,
// never moving lastSuccess backward. Recording the issue time only ever
// understates freshness by the call latency, which is conservative: the
// gate may refuse writes slightly early, never late.
func (l *trackingLock) markSuccess(at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if at.After(l.lastSuccess) {
		l.lastSuccess = at
	}
}

// sinceLastSuccess reports how long ago the last successful Lease write
// happened. A never-successful lock reads as infinitely stale.
func (l *trackingLock) sinceLastSuccess() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lastSuccess.IsZero() {
		return time.Duration(math.MaxInt64)
	}
	return time.Since(l.lastSuccess)
}

// everSucceeded reports whether the elector completed at least one Lease
// write, i.e. it acquired the Lease. The write runs synchronously in the
// elector goroutine, before Run can return, so Start can rely on it to
// decide whether finished will be sent even when the OnStartedLeading
// goroutine has not run yet.
func (l *trackingLock) everSucceeded() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.lastSuccess.IsZero()
}

// staleGate implements reconciler.LeaderGate on top of the tracking lock.
// While the last successful renewal is younger than bound, no standby could
// have acquired the Lease, so cluster writes are safe. Once it is older, the
// Lease may have been taken over even though the elector's renew poll is
// still inside its RenewDeadline (a stalled-and-resumed leader), so writes
// are denied and the controllers' run context is canceled to exit the
// process as renewal failure does.
type staleGate struct {
	lock          *trackingLock
	cancel        context.CancelFunc
	bound         time.Duration
	watchInterval time.Duration
}

// CanWrite implements reconciler.LeaderGate.
func (g *staleGate) CanWrite() error {
	if age := g.lock.sinceLastSuccess(); age > g.bound {
		if g.cancel != nil {
			g.cancel()
		}
		return fmt.Errorf(
			"%w: last successful renewal %s ago, freshness bound %s",
			reconciler.ErrLeadershipStale, age.Truncate(time.Millisecond), g.bound,
		)
	}
	return nil
}

// Remaining implements reconciler.LeaderGate.
func (g *staleGate) Remaining() time.Duration {
	return g.bound - g.lock.sinceLastSuccess()
}
