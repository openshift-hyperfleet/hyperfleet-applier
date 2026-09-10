// Package readdesire mirrors live Kubernetes object state back into ReadDesire
// status. It runs one Kubernetes informer per ReadDesire, each scoped via a
// metadata.name field selector to exactly the object that desire targets, and
// feeds a workqueue from each informer's event handlers.
//
// A Controller is bound to one management-cluster partition and is a long-running daemon:
// Start starts a workqueue, a worker pool, and the periodic informer-lifecycle
// poll loop together and blocks until ctx is canceled. It owns its own poll
// ticker because it coordinates three concurrently-running things.
//
// The controller reads ReadDesire intent (Identity only - a ReadDesire has no
// KubeContent in its spec) and writes ReadDesire status. It does not mutate
// desire intent through the spec store.
package readdesire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/workqueue"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

const (
	// resyncPeriod is each per-desire informer's resync interval. Once
	// an informer exists, its own initial List and this resync are what keep
	// that desire's key flowing into the queue - the poll loop below never
	// enqueues directly; it only starts/stops informers.
	resyncPeriod = 60 * time.Second
	// workerCount is the number of concurrent sync workers draining the queue.
	workerCount = 4
)

// specLister is the read side of the SpecStore the controller polls to
// discover which ReadDesires currently exist, so it knows which per-desire
// informers should be running.
type specLister interface {
	ListReadDesires(ctx context.Context, managementCluster string) ([]desire.ReadDesire, error)
}

// statusStore is the StatusStore surface sync needs: GetReadDesire for the
// current Status (to suppress a no-op write and as the LastTransitionTime
// baseline), and UpdateReadDesireStatus to persist the mirrored result.
// UpdateReadDesireStatus never checks or advances the desire's Version: per
// pkg/desire/CLAUDE.md, each desire type is its own independent record now,
// so there's no shared-resource CAS concern for a Read status write to get
// wrong the way there would be for Apply/Delete.
type statusStore interface {
	GetReadDesire(ctx context.Context, id desire.Identity) (desire.ReadDesire, error)
	UpdateReadDesireStatus(ctx context.Context, id desire.Identity, status desire.ReadStatus) (desire.ReadDesire, error)
}

// Identity, not a separate resource-key type, is used as the workqueue/map
// key throughout this package: every key this controller ever handles is a
// ReadDesire's Identity (Type always desire.TypeRead), so there's no need to
// project Type away and reconstruct it later the way a type-erased key would
// require.
type Controller struct {
	spec              specLister
	status            statusStore
	dyn               dynamic.Interface
	mapper            meta.ResettableRESTMapper
	queue             workqueue.TypedRateLimitingInterface[desire.Identity]
	informers         *InformerManager
	managementCluster string
	pollInterval      time.Duration
}

// Option configures optional Controller behavior.
type Option func(*options)

type options struct {
	informerSyncTimeout time.Duration
}

// WithInformerSyncTimeout overrides the default timeout for waiting on a
// newly-started informer's initial cache sync.
func WithInformerSyncTimeout(d time.Duration) Option {
	return func(o *options) { o.informerSyncTimeout = d }
}

// New builds a Controller bound to one partition (managementCluster).
// dyn is the dynamic client used to build per-desire informers; mapper
// resolves a partial (Group, Resource) to its full, versioned
// GroupVersionResource - see resolveGVR for why this differs from
// applydesire's Kind-based RESTMapping. pollInterval controls how often
// ListReadDesires is called to discover which ReadDesires currently exist, so
// their informers can be started or stopped accordingly.
func New(
	spec specLister,
	status statusStore,
	dyn dynamic.Interface,
	mapper meta.ResettableRESTMapper,
	managementCluster string,
	pollInterval time.Duration,
	opts ...Option,
) *Controller {
	o := options{}
	for _, fn := range opts {
		fn(&o)
	}
	queue := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[desire.Identity](),
	)
	return &Controller{
		spec:              spec,
		status:            status,
		dyn:               dyn,
		mapper:            mapper,
		managementCluster: managementCluster,
		pollInterval:      pollInterval,
		queue:             queue,
		informers:         newInformerManager(dyn, queue, o.informerSyncTimeout),
	}
}

// Start starts the worker pool, then the poll loop, and blocks until ctx is
// canceled. It returns nil on clean shutdown. The workqueue is created in
// New; Start starts, in order, (1) the worker pool draining it, then
// (2) the poll loop that drives the InformerManager - this ordering means
// workers are ready to drain the queue before any informer can enqueue into
// it.
func (c *Controller) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	for range workerCount {
		wg.Go(func() {
			c.runWorker(ctx)
		})
	}

	pollErr := c.runPollLoop(ctx) // blocks until ctx.Done()

	// Plain ShutDown, not ShutDownWithDrain: the latter is only overridden on
	// the base FIFO queue, not on the rate-limiting/delaying wrapper this
	// queue actually is, so it would never close that wrapper's own stopCh -
	// leaking its internal waitingLoop goroutine (used by
	// AddAfter/AddRateLimited) forever. ShutDown does close it. The
	// wg.Wait() below already blocks until every worker has drained the
	// queue and returned, which is the only thing ShutDownWithDrain would
	// have added.
	c.queue.ShutDown()
	c.informers.shutdownAll()
	wg.Wait()
	if errors.Is(pollErr, context.Canceled) || errors.Is(pollErr, context.DeadlineExceeded) {
		// Caller-driven shutdown, not a failure - runPollLoop's ctx.Err()
		// (whether from its own select or wrapped out of a mid-tick pollOnce
		// abort) is expected here on every clean stop, so it must not be
		// reported as one to match this method's documented "nil on clean
		// shutdown" contract.
		return nil
	}
	return pollErr
}

// runPollLoop calls ListReadDesires immediately, then every pollInterval, and reconciles the
// running per-desire informer set against it: an informer is started for
// every currently-listed ReadDesire that doesn't have one yet, and stopped
// for every running informer whose ReadDesire is no longer listed. It does
// not enqueue anything itself.
//
// A per-desire GVR resolution failure is logged and skipped, not fatal to the
// tick - one bad desire must not block reconciliation for every other desire.
func (c *Controller) runPollLoop(ctx context.Context) error {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		// pollOnce only ever returns non-nil for ctx cancellation hit
		// mid-tick (see its own abort checks below).
		if err := c.pollOnce(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// pollOnce is one poll tick: list every ReadDesire for this partition,
// resolve each one's GVR, and reconcile the running per-desire informer set
// against the result. A ReadDesire whose GVR can't be resolved gets
// PreCheckFailed recorded via applyStatus (not just logged) and is excluded
// from want, so InformerManager.Reconcile only skips starting a new informer
// for it this tick - see the seen/want split below. Any informer that then
// fails to start is reported the same way. Runs on runPollLoop's single
// goroutine, so it never overlaps with itself.
//
// Returns non-nil only for ctx cancellation hit partway through this tick
// (checked before each desire, and before each Reconcile-failure report) -
// every other outcome, including a ListReadDesires failure, is handled
// in-place (logged and/or recorded to status) and returns nil, matching
// runPollLoop's assumption that a non-nil return always means "stop now."
// Aborting before Reconcile is called (rather than partway through it) is
// deliberate: calling Reconcile with a partial seen/want built from only
// some of this tick's desires would make it tear down informers for
// still-listed desires it never got to visit.
func (c *Controller) pollOnce(ctx context.Context) error {
	desires, err := c.spec.ListReadDesires(ctx, c.managementCluster)
	if err != nil {
		slog.ErrorContext(ctx,
			"readdesire: list read desires failed",
			"management_cluster",
			c.managementCluster,
			"error",
			err)
		return nil
	}

	// seen is every desire currently listed, regardless of whether its GVR
	// resolved this tick; want is the subset with a resolved target. Only
	// seen decides whether an existing informer is stopped - a transient
	// resolveGVR failure for an already-running desire must not tear down
	// (and lose the cache of) an otherwise-healthy informer; it should only
	// prevent starting a new one this tick. See InformerManager.Reconcile.
	seen := make(map[desire.Identity]struct{}, len(desires))
	want := make(map[desire.Identity]informerTarget, len(desires))
	for _, d := range desires {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("readdesire: poll aborted for management cluster %q: %w", c.managementCluster, ctxErr)
		}

		key := d.Identity
		seen[key] = struct{}{}

		gvr, err := c.resolveGVR(d.Identity.Group, d.TargetVersion, d.Identity.Resource)
		if err != nil {
			slog.ErrorContext(ctx,
				"readdesire: resolve GVR failed",
				"group", d.Identity.Group,
				"target_version", d.TargetVersion,
				"resource", d.Identity.Resource,
				"error", err)

			msg := fmt.Sprintf("readdesire: no resource mapping for %s/%s/%s: %v",
				d.Identity.Group, d.TargetVersion, d.Identity.Resource, err)

			if statusErr := c.applyStatus(ctx, d.Identity, func(rd desire.ReadDesire) desire.ReadStatus {
				return preCheckFailed(rd.Status, msg)
			}); statusErr != nil {
				slog.ErrorContext(ctx, "readdesire: record precheck failure status failed",
					"namespace", d.Identity.Namespace,
					"name", d.Identity.Name,
					"error", statusErr)
			}

			continue
		}
		want[key] = informerTarget{gvr: gvr, namespace: d.Identity.Namespace, name: d.Identity.Name}
	}

	errors := c.informers.Reconcile(seen, want)
	for key, err := range errors {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("readdesire: poll aborted for management cluster %q: %w", c.managementCluster, ctxErr)
		}
		slog.ErrorContext(ctx, "readdesire: start informer failed",
			"namespace", key.Namespace,
			"name", key.Name,
			"error", err)

		msg := fmt.Sprintf("readdesire: start informer failed: %v", err)

		if statusErr := c.applyStatus(ctx, key, func(rd desire.ReadDesire) desire.ReadStatus {
			return preCheckFailed(rd.Status, msg)
		}); statusErr != nil {
			slog.ErrorContext(ctx, "readdesire: record informer-start failure status failed",
				"namespace", key.Namespace,
				"name", key.Name,
				"error", statusErr)
		}
	}
	return nil
}

// resolveGVR resolves a fully-specified GVR (Group+Version+Resource, all
// declared by the ReadDesire) - existence-checked via ResourceFor rather than
// resolved/guessed, since the version is now explicit.
func (c *Controller) resolveGVR(group, targetVersion, resource string) (schema.GroupVersionResource, error) {
	partial := schema.GroupVersionResource{Group: group, Version: targetVersion, Resource: resource}
	gvr, err := c.mapper.ResourceFor(partial)
	if err != nil && meta.IsNoMatchError(err) {
		// The resource may have just been installed (e.g. a new CRD) after
		// the mapper's discovery cache was already populated - reset and
		// retry once before giving up.
		c.mapper.Reset()
		gvr, err = c.mapper.ResourceFor(partial)
	}
	return gvr, err
}
