package readdesire

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
)

// informerTarget is what InformerManager needs to build and scope one
// per-desire informer: the resolved GVR to watch, and the specific
// namespace/name to filter to via a metadata.name field selector.
type informerTarget struct {
	gvr       schema.GroupVersionResource
	namespace string
	name      string
}

// trackedInformer pairs a running informer and its lister with the stop
// channel that controls only its own lifecycle, so one desire's informer can
// be shut down independently of the others. lister is retained so sync can
// read the cached object without hitting the apiserver: see
// InformerManager.Lister.
type trackedInformer struct {
	informer cache.SharedIndexInformer
	lister   cache.GenericLister
	stopCh   chan struct{}
	gvr      schema.GroupVersionResource
}

// InformerManager keeps exactly one running, name-scoped informer per
// ReadDesire currently known to Controller. When a desire drops out of the
// wanted set (it was deleted), Reconcile stops that desire's informer only.
type InformerManager struct {
	dyn         dynamic.Interface
	queue       workqueue.TypedRateLimitingInterface[desire.Identity]
	informers   map[desire.Identity]*trackedInformer
	syncTimeout time.Duration
	mu          sync.Mutex
}

// DefaultInformerSyncTimeout is the timeout period for informer's cache
// resync
const DefaultInformerSyncTimeout = 30 * time.Second

func newInformerManager(
	dyn dynamic.Interface,
	queue workqueue.TypedRateLimitingInterface[desire.Identity],
	syncTimeout time.Duration,
) *InformerManager {
	if syncTimeout <= 0 {
		syncTimeout = DefaultInformerSyncTimeout
	}
	return &InformerManager{
		dyn:         dyn,
		queue:       queue,
		informers:   make(map[desire.Identity]*trackedInformer),
		syncTimeout: syncTimeout,
	}
}

// Reconcile starts an informer for every key in want that isn't already
// running on the exact target GVR (rebuilding it first if a tracked informer
// exists but on a stale GVR - e.g. the desire's declared TargetVersion
// changed), and stops every currently-running informer whose key is not in
// seen. seen is every desire currently listed (regardless of whether its GVR
// resolved this tick); want is the subset with a resolved target. Using seen rather
// than want for the stop decision means a transient GVR-resolution failure
// for an already-running desire only skips starting a new informer for it -
// it does not tear down (and lose the cache of) an already-healthy one; see
// Controller.pollOnce.
//
// The returned map holds one entry per key whose informer failed to start
// this call (nil if none failed) - the caller decides how to report each
// failure (e.g. Controller.pollOnce persists it via applyStatus, the same
// path used for a resolveGVR failure). A failed key stays absent from
// m.informers, so it's retried on the next Reconcile call as long as it
// remains in want.
//
// Called once per poll tick from a single goroutine (Controller.pollOnce), so
// it does not need to guard against concurrent Reconcile calls - only
// against shutdownAll/Lister running during Run's teardown or a concurrent
// sync call, hence the mutex.
func (m *InformerManager) Reconcile(
	seen map[desire.Identity]struct{}, want map[desire.Identity]informerTarget,
) map[desire.Identity]error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, ti := range m.informers {
		if _, ok := seen[key]; ok {
			continue
		}
		close(ti.stopCh)
		delete(m.informers, key)
	}

	var failed map[desire.Identity]error
	for key, target := range want {
		if ti, ok := m.informers[key]; ok {
			if ti.gvr == target.gvr {
				continue // already watching the right thing
			}
			// Declared TargetVersion changed for this ResourceKey since the
			// informer was built - tear down the stale one before starting a
			// fresh one on the new GVR.
			close(ti.stopCh)
			delete(m.informers, key)
		}
		if err := m.start(key, target); err != nil {
			if failed == nil {
				failed = make(map[desire.Identity]error)
			}
			failed[key] = err
		}
	}
	return failed
}

// Lister returns the cache.GenericLister for key's informer, whether the
// informer exists at all (ok), and whether its cache has completed its
// initial list (synced). When ok is false the other values are zero. When
// ok is true but synced is false, the cache may be empty for reasons other
// than the target being absent (e.g. RBAC denial, network partition), so a
// lister miss is ambiguous and must not be reported as NotFound.
func (m *InformerManager) Lister(key desire.Identity) (cache.GenericLister, bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ti, ok := m.informers[key]
	if !ok || ti.lister == nil || ti.informer == nil {
		return nil, false, false
	}
	return ti.lister, true, ti.informer.HasSynced()
}

// start builds a single-object-scoped informer for target - a field selector
// on metadata.name, plus target.namespace (empty for cluster-scoped kinds,
// same convention applyToCluster relies on) - wires a handler that enqueues
// key on any Add/Update/Delete, and runs it in its own goroutine under its
// own stop channel. Because this informer only ever observes the one object
// it's scoped to, the handler needs no object introspection at all: any event
// it receives is by construction about key's desire, so it just enqueues key
// directly. Caller must hold m.mu.
//
// Returns an error (leaving key untracked, so Reconcile retries it next
// call) only if wiring the event handler fails - this can only happen if the
// informer has already stopped, which cannot happen here since it was just
// created, so in practice this is not expected to fire.
//
// cache.WaitForCacheSync is awaited in a background goroutine (not inline
// here) so that starting many new informers in one Reconcile call doesn't
// serialize on each other's initial List completing; its failure can't be
// returned here (this function has already returned by the time it runs) so
// it's log-only, except for the syncTimeout case below, which enqueues key
// anyway.
func (m *InformerManager) start(key desire.Identity, target informerTarget) error {
	tweakListOptions := func(opts *metav1.ListOptions) {
		opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", target.name).String()
	}
	gi := dynamicinformer.NewFilteredDynamicInformer(
		m.dyn, target.gvr, target.namespace, resyncPeriod, cache.Indexers{}, tweakListOptions,
	)
	informer := gi.Informer()

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ any) { m.queue.Add(key) },
		UpdateFunc: func(_, _ any) { m.queue.Add(key) },
		DeleteFunc: func(_ any) { m.queue.Add(key) },
	}); err != nil {
		return fmt.Errorf("readdesire: add event handler failed: %w", err)
	}

	stopCh := make(chan struct{})
	m.informers[key] = &trackedInformer{gvr: target.gvr, informer: informer, lister: gi.Lister(), stopCh: stopCh}
	go informer.Run(stopCh)
	go func() {
		// Wait up to syncTimeout for the initial informer cache sync.
		// The informer itself keeps running and retrying after this timeout.
		syncStopCh := timeoutOrStop(stopCh, m.syncTimeout)

		if cache.WaitForCacheSync(syncStopCh, informer.HasSynced) {
			// Initial sync completed within the timeout. Enqueue once so the
			// worker can observe the current cached state.
			m.queue.Add(key)
			return
		}

		select {
		case <-stopCh:
			// The informer was torn down before it completed its initial sync.
			slog.Error(
				"readdesire: informer cache sync did not complete before shutdown",
				"namespace", key.Namespace,
				"name", key.Name,
			)
			return

		default:
			// The sync timeout elapsed, but the informer is still running and
			// will continue retrying its initial LIST in the background.
			// Enqueue now so the worker can report KubeAPIError rather than
			// waiting indefinitely.
			slog.Error(
				"readdesire: informer cache did not sync within timeout, reporting anyway",
				"namespace", key.Namespace,
				"name", key.Name,
				"timeout", m.syncTimeout,
			)
			m.queue.Add(key)
		}

		// If the informer eventually completes its initial sync, enqueue again.
		// This is necessary when the initial LIST is empty: no Add/Update/Delete
		// event fires, so otherwise a desire reported as KubeAPIError after the
		// timeout could remain stuck there instead of transitioning to NotFound.
		if cache.WaitForCacheSync(stopCh, informer.HasSynced) {
			m.queue.Add(key)
		}
	}()
	return nil
}

// timeoutOrStop returns a channel that closes when stopCh closes or after
// timeout elapses, whichever comes first - so a WaitForCacheSync call given
// this channel can never block longer than timeout, regardless of whether
// the real informer ever gets torn down. It only ever reads from stopCh
// (never closes it) and closes a separate, freshly created channel instead,
// so it has no effect on stopCh or the informer's own lifecycle.
func timeoutOrStop(stopCh <-chan struct{}, timeout time.Duration) <-chan struct{} {
	merged := make(chan struct{})
	go func() {
		defer close(merged)
		select {
		case <-stopCh:
		case <-time.After(timeout):
		}
	}()
	return merged
}

// shutdownAll stops every currently-running informer. Called once, from
// Controller.Run's teardown after the poll loop has returned.
func (m *InformerManager) shutdownAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, ti := range m.informers {
		close(ti.stopCh)
		delete(m.informers, key)
	}
}
