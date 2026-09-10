package readdesire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// runWorker repeatedly calls processNextWorkItem until the queue reports
// shutdown via Get's shutdown bool.
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem pulls one key off the queue, calls sync, and reports
// the outcome to the rate limiter: Forget on success or context cancellation
// (caller-driven control flow, not a resource failure - the same distinction
// applydesire's reconcileAll makes for context.Canceled/DeadlineExceeded),
// AddRateLimited on any other error so a failing key retries with backoff
// instead of hot-looping. Returns false only when the queue is shutting down.
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	err := c.sync(ctx, key)
	switch {
	case err == nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		c.queue.Forget(key)
	default:
		slog.ErrorContext(ctx, "readdesire: sync failed, will retry",
			"namespace", key.Namespace,
			"name", key.Name,
			"error", err,
			"num_requeues", c.queue.NumRequeues(key))
		c.queue.AddRateLimited(key)
	}
	return true
}

// sync reconciles one ReadDesire: fetch its current record, observe the live
// object via the informer Lister, and persist the result if it changed.
func (c *Controller) sync(ctx context.Context, key desire.Identity) error {
	return c.applyStatus(ctx, key, func(d desire.ReadDesire) desire.ReadStatus {
		return c.observe(ctx, key, d.TargetVersion, d.Status)
	})
}

// applyStatus fetches the current ReadDesire for id, computes its new status
// via compute and persists it via UpdateReadDesireStatus if it changed.
// A desire deleted since it was enqueued (ErrNotFound) is a benign no-op,
// not an error - its informer will be torn down on the next poll tick regardless.
//
// This is the one place status is ever written, used both by sync (per-key,
// workqueue-driven) and by pollOnce (per-tick, for desires whose GVR could
// not even be resolved).
func (c *Controller) applyStatus(
	ctx context.Context, id desire.Identity, compute func(d desire.ReadDesire) desire.ReadStatus,
) error {
	d, err := c.status.GetReadDesire(ctx, id)
	if errors.Is(err, desire.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("readdesire: get read desire %s/%s: %w", id.Namespace, id.Name, err)
	}

	newStatus := compute(d)
	if readStatusEqual(newStatus, d.Status) {
		return nil
	}

	if _, err := c.status.UpdateReadDesireStatus(ctx, id, newStatus); err != nil {
		return fmt.Errorf("readdesire: update read desire status %s/%s: %w", id.Namespace, id.Name, err)
	}
	return nil
}

// observe builds the ReadStatus to persist for key from the informer's
// cached object, if any. current is the base status (preserved as-is on a
// transient error, and used as the LastTransitionTime baseline for an
// unchanged condition via conditions.WithCondition). targetVersion is the
// desire's declared TargetVersion, checked against the live object's actual
// version below.
func (c *Controller) observe(
	ctx context.Context, key desire.Identity, targetVersion string, current desire.ReadStatus,
) desire.ReadStatus {
	lister, ok, cacheSynced := c.informers.Lister(key)
	if !ok {
		// No informer running for this key. InformerManager.start always
		// writes m.informers[key] before wiring anything that could enqueue
		// it (event handlers only fire once informer.Run - spawned after the
		// write - actually starts), so this can only be a teardown race: the
		// key was enqueued while its informer was running, then Reconcile
		// removed the desire (it's gone from the store) before this worker
		// got to it. Treat the same as not-found rather than erroring.
		return notFound(current)
	}

	// GenericLister.Get assumes name==key, matching MetaNamespaceKeyFunc's
	// cluster-scoped key format (bare name, no namespace prefix).
	// ByNamespace(ns).Get always looks up "ns/name" even when ns is "" (a
	// literal leading slash), which never matches a cluster-scoped object's
	// bare-name key - so cluster-scoped resources must use the bare Get.
	var (
		obj runtime.Object
		err error
	)
	if key.Namespace == "" {
		obj, err = lister.Get(key.Name)
	} else {
		obj, err = lister.ByNamespace(key.Namespace).Get(key.Name)
	}
	if apierrors.IsNotFound(err) {
		if !cacheSynced {
			return kubeAPIError(current, fmt.Errorf(
				"informer cache has not synced yet, cannot confirm absence: %w", err))
		}
		return notFound(current)
	}
	if err != nil {
		return kubeAPIError(current, err)
	}

	if actual := obj.GetObjectKind().GroupVersionKind().Version; actual != targetVersion {
		return c.observeLive(ctx, key, targetVersion, current)
	}

	content, err := json.Marshal(obj)
	if err != nil {
		return kubeAPIError(current, err)
	}
	return synced(current, content)
}

// observeLive is the fallback for when the informer cache's object disagrees
// with the desire's declared TargetVersion. If the live object agrees with targetVersion,
// the informer was just transiently stale (e.g. Reconcile mid-rebuild after a
// TargetVersion change) and self-corrects, so the live content is used
// directly rather than discarded, since the round trip already paid for it.
// If the live object still disagrees, or the Get itself fails, this is no
// longer explainable as cache lag - it's a genuine misconfiguration or
// persistent failure, so it's escalated to a real KubeAPIError status write.
func (c *Controller) observeLive(
	ctx context.Context, key desire.Identity, targetVersion string, current desire.ReadStatus,
) desire.ReadStatus {
	gvr, err := c.resolveGVR(key.Group, targetVersion, key.Resource)
	if err != nil {
		return kubeAPIError(current, err)
	}

	ri := c.dyn.Resource(gvr)
	var resourceClient dynamic.ResourceInterface = ri
	if key.Namespace != "" {
		resourceClient = ri.Namespace(key.Namespace)
	}

	obj, err := resourceClient.Get(ctx, key.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return notFound(current)
	}
	if err != nil {
		return kubeAPIError(current, err)
	}

	if actual := obj.GetObjectKind().GroupVersionKind().Version; actual != targetVersion {
		return kubeAPIError(current, fmt.Errorf(
			"live object version %q still does not match declared TargetVersion %q after a direct Get "+
				"(informer cache was not merely stale)", actual, targetVersion))
	}

	slog.WarnContext(ctx, "readdesire: informer cache was stale, live Get confirmed declared TargetVersion",
		"namespace", key.Namespace, "name", key.Name, "target_version", targetVersion)

	content, err := json.Marshal(obj)
	if err != nil {
		return kubeAPIError(current, err)
	}
	return synced(current, content)
}
