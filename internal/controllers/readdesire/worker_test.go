package readdesire

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// ---- fixtures & helpers -------------------------------------------------

// fakeInformer is a minimal cache.SharedIndexInformer stub that returns a
// fixed HasSynced value, used to control the synced state reported by
// InformerManager.Lister without starting a real informer.
type fakeInformer struct {
	cache.SharedIndexInformer
	synced bool
}

func (f fakeInformer) HasSynced() bool { return f.synced }

// seedInformer injects a fake tracked informer entry for key into c's
// InformerManager, wired to lister with the given synced state - bypassing
// real informer startup so sync/observe can be tested in isolation.
func seedInformer(c *Controller, key desire.Identity, lister cache.GenericLister, synced bool) {
	c.informers.informers[key] = &trackedInformer{lister: lister, informer: fakeInformer{synced: synced}}
}

// ---- decorators used to observe/inject store behavior -------------------

// countingStatusStore counts UpdateReadDesireStatus calls, to prove the
// no-op path never reaches the store.
type countingStatusStore struct {
	statusStore
	updateCalls int
}

func (c *countingStatusStore) UpdateReadDesireStatus(
	ctx context.Context, id desire.Identity, status desire.ReadStatus,
) (desire.ReadDesire, error) {
	c.updateCalls++
	return c.statusStore.UpdateReadDesireStatus(ctx, id, status)
}

// erroringStatusStore fails every UpdateReadDesireStatus call with err.
type erroringStatusStore struct {
	statusStore
	err error
}

func (e *erroringStatusStore) UpdateReadDesireStatus(
	context.Context, desire.Identity, desire.ReadStatus,
) (desire.ReadDesire, error) {
	return desire.ReadDesire{}, e.err
}

// getErroringStatusStore fails every GetReadDesire call with err - used to
// drive sync() into returning a specific error (including context.Canceled/
// DeadlineExceeded, which never occurs naturally against memory.Store) so
// processNextWorkItem's Forget-vs-AddRateLimited dispatch can be tested
// deterministically without needing a real canceled context.
type getErroringStatusStore struct {
	statusStore
	err error
}

func (g *getErroringStatusStore) GetReadDesire(context.Context, desire.Identity) (desire.ReadDesire, error) {
	return desire.ReadDesire{}, g.err
}

// ---- tests ---------------------------------------------------------------

// TestSync_FoundObjectRecordsSynced proves a found object in a synced cache
// records ReasonSynced with the object's content.
func TestSync_FoundObjectRecordsSynced(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-found")
	seedReadDesire(t, store, id, "owner-1")

	obj := newUnstructuredConfigMap("cm-found", "default", map[string]any{"k": "v"})
	c := New(store, store, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	seedInformer(c, id, newLister(t, configMapGVR, obj), true)

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != desire.ReasonSynced {
		t.Errorf("condition = %+v, want Status=True Reason=%q", cond, desire.ReasonSynced)
	}
	wantContent, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal want content: %v", err)
	}
	if string(got.Status.KubeContent) != string(wantContent) {
		t.Errorf("KubeContent = %s, want %s", got.Status.KubeContent, wantContent)
	}
}

// TestSync_SyncedEmptyCacheRecordsNotFound proves that a synced cache with no
// object is a confirmed absence: the informer completed its initial list and
// the object is genuinely not there.
func TestSync_SyncedEmptyCacheRecordsNotFound(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-empty-cache")
	seedReadDesire(t, store, id, "owner-1")

	c := New(store, store, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	seedInformer(c, id, newLister(t, configMapGVR), true)

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonNotFound {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonNotFound)
	}
}

// TestSync_UnsyncedEmptyCacheRecordsKubeAPIError proves that a NotFound from
// an unsynced cache is ambiguous (could be RBAC denial, network partition)
// and must not be reported as a confirmed absence.
func TestSync_UnsyncedEmptyCacheRecordsKubeAPIError(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-unsynced-cache")
	seedReadDesire(t, store, id, "owner-1")

	c := New(store, store, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	seedInformer(c, id, newLister(t, configMapGVR), false)

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonKubeAPIError {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonKubeAPIError)
	}
}

// TestSync_NoTrackedInformerRecordsNotFound proves the "no informer at all"
// path (teardown race) records NotFound - distinct from the empty-cache path
// above, which goes through the lister.
func TestSync_NoTrackedInformerRecordsNotFound(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-no-informer")
	seedReadDesire(t, store, id, "owner-1")

	c := New(store, store, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	// No seedInformer call: exercises the "no informer yet" path.

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil", err)
	}

	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonNotFound {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonNotFound)
	}
}

func TestSync_DeletedDesireIsNoop(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-gone") // never seeded: GetReadDesire returns ErrNotFound

	c := New(store, store, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() error = %v, want nil for a desire that no longer exists", err)
	}
}

func TestSync_UnchangedObjectSuppressesStatusWrite(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	counting := &countingStatusStore{statusStore: base}
	id := readIdentity("default", "cm-noop")
	seedReadDesire(t, base, id, "owner-1")

	obj := newUnstructuredConfigMap("cm-noop", "default", map[string]any{"k": "v"})
	c := New(base, counting, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	seedInformer(c, id, newLister(t, configMapGVR, obj), true)

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() [1st] error = %v, want nil", err)
	}
	if counting.updateCalls != 1 {
		t.Fatalf("updateCalls after 1st sync = %d, want 1", counting.updateCalls)
	}

	if err := c.sync(ctx, id); err != nil {
		t.Fatalf("sync() [2nd] error = %v, want nil", err)
	}
	if counting.updateCalls != 1 {
		t.Errorf(
			"updateCalls after 2nd sync (unchanged) = %d, want still 1: reconciling an unchanged object must suppress the write",
			counting.updateCalls,
		)
	}
}

// TestSync_UpdateFailureIsPropagatedForRetry proves that any
// UpdateReadDesireStatus failure is returned by sync (not swallowed), so
// processNextWorkItem's AddRateLimited retries it promptly. Read status
// writes are decoupled from the shared per-resource Version (no CAS, never
// ErrVersionConflict - see statusStore), so this covers a generic backend
// failure rather than a version race specifically.
func TestSync_UpdateFailureIsPropagatedForRetry(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	id := readIdentity("default", "cm-update-fails")
	seedReadDesire(t, base, id, "owner-1")

	updateErr := errors.New("status store unavailable")
	failing := &erroringStatusStore{statusStore: base, err: updateErr}
	obj := newUnstructuredConfigMap("cm-update-fails", "default", map[string]any{"k": "v"})
	c := New(base, failing, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	seedInformer(c, id, newLister(t, configMapGVR, obj), true)

	err := c.sync(ctx, id)
	if !errors.Is(err, updateErr) {
		t.Fatalf("sync() error = %v, want it to wrap %v so the workqueue retries", err, updateErr)
	}
}

// staleDataValue marks a cached configmap fixture as the stale one being
// superseded by a live re-Get, across the observeLive fallback tests below.
const staleDataValue = "stale"

// TestSync_VersionMismatchFallback covers observeLive's outcomes when the
// informer cache's object disagrees with the declared TargetVersion "v1":
// recovering via a live Get that agrees, escalating when the live Get still
// disagrees or is genuinely gone, and escalating when the fallback GVR
// itself can't be resolved.
func TestSync_VersionMismatchFallback(t *testing.T) {
	const namespace = "default"
	cases := []struct {
		mapper          meta.ResettableRESTMapper
		name            string
		cmName          string
		liveAPIVersion  string
		wantStatus      metav1.ConditionStatus
		wantReason      string
		liveExists      bool
		wantLiveContent bool
	}{
		{
			name:            "RecoversViaLiveGet",
			cmName:          "cm-recovers-via-live-get",
			liveExists:      true,
			mapper:          newTestMapper(),
			wantStatus:      metav1.ConditionTrue,
			wantReason:      desire.ReasonSynced,
			wantLiveContent: true,
		},
		{
			name:           "EscalatesWhenLiveGetStillWrong",
			cmName:         "cm-escalates-when-live-get-still-wrong",
			liveExists:     true,
			liveAPIVersion: "v2",
			mapper:         newTestMapper(),
			wantStatus:     metav1.ConditionFalse,
			wantReason:     desire.ReasonKubeAPIError,
		},
		{
			name:       "LiveGetNotFoundRecordsNotFound",
			cmName:     "cm-live-get-not-found",
			mapper:     newTestMapper(),
			wantStatus: metav1.ConditionFalse,
			wantReason: desire.ReasonNotFound,
		},
		{
			name:       "EscalatesWhenGVRResolutionFails",
			cmName:     "cm-escalates-when-gvr-resolution-fails",
			mapper:     newNoMatchMapper(),
			wantStatus: metav1.ConditionFalse,
			wantReason: desire.ReasonKubeAPIError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := memory.New()
			id := readIdentity(namespace, tc.cmName)
			seedReadDesire(t, store, id, "owner-1") // declares TargetVersion "v1"

			// The cache is always stale in this scenario: it reports apiVersion "v2".
			stale := newUnstructuredConfigMap(id.Name, namespace, map[string]any{"k": staleDataValue})
			stale.SetAPIVersion("v2")

			dyn := newFakeDynamicClient(t)
			var createdLive *unstructured.Unstructured
			if tc.liveExists {
				live := newUnstructuredConfigMap(id.Name, namespace, map[string]any{"k": "live"})
				if tc.liveAPIVersion != "" {
					live.SetAPIVersion(tc.liveAPIVersion)
				}
				created, err := dyn.Resource(configMapGVR).Namespace(namespace).Create(ctx, live, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("seed live object: %v", err)
				}
				createdLive = created
			}

			c := New(store, store, dyn, tc.mapper, testManagementCluster, time.Hour)
			seedInformer(c, id, newLister(t, configMapGVR, stale), true)

			if err := c.sync(ctx, id); err != nil {
				t.Fatalf("sync() error = %v, want nil", err)
			}

			got, err := store.GetReadDesire(ctx, id)
			if err != nil {
				t.Fatalf("GetReadDesire: %v", err)
			}
			cond := findCondition(got.Status.Status, desire.TypeSuccessful)
			if cond == nil || cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
				t.Errorf("condition = %+v, want Status=%s Reason=%q", cond, tc.wantStatus, tc.wantReason)
			}

			if tc.wantLiveContent {
				wantContent, err := json.Marshal(createdLive)
				if err != nil {
					t.Fatalf("marshal want content: %v", err)
				}
				if string(got.Status.KubeContent) != string(wantContent) {
					t.Errorf("KubeContent = %s, want the live object's content %s, not the stale cached one",
						got.Status.KubeContent, wantContent)
				}
			}
		})
	}
}

// ---- processNextWorkItem retry dispatch ----------------------------------

// TestProcessNextWorkItem_SuccessForgetsKey proves a successful sync forgets
// the key, resetting any prior backoff, rather than leaving it rate-limited.
func TestProcessNextWorkItem_SuccessForgetsKey(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-success")
	seedReadDesire(t, store, id, "owner-1")
	obj := newUnstructuredConfigMap("cm-success", "default", map[string]any{"k": "v"})

	c := New(store, store, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	seedInformer(c, id, newLister(t, configMapGVR, obj), true)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	defer queue.ShutDown()
	c.queue = queue

	// Simulate a prior failed attempt so Forget's effect (resetting the
	// backoff counter) is actually observable, instead of trivially true.
	queue.AddRateLimited(id)
	if n := queue.NumRequeues(id); n != 1 {
		t.Fatalf("test setup: NumRequeues = %d, want 1", n)
	}

	if !c.processNextWorkItem(ctx) {
		t.Fatal("processNextWorkItem() = false, want true (queue not shut down)")
	}
	if n := queue.NumRequeues(id); n != 0 {
		t.Errorf("NumRequeues = %d, want 0: a successful sync must Forget the key, resetting backoff", n)
	}
}

// TestProcessNextWorkItem_ContextCanceledForgetsKey proves context
// cancellation is treated as caller-driven shutdown, not a resource
// failure: the key is forgotten, never retried with backoff.
func TestProcessNextWorkItem_ContextCanceledForgetsKey(t *testing.T) {
	ctx := context.Background()
	id := readIdentity("default", "cm-canceled")
	failing := &getErroringStatusStore{statusStore: memory.New(), err: context.Canceled}

	c := New(memory.New(), failing, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	defer queue.ShutDown()
	c.queue = queue
	queue.Add(id)

	c.processNextWorkItem(ctx)

	if n := queue.NumRequeues(id); n != 0 {
		t.Errorf("NumRequeues = %d, want 0: context cancellation must not trigger a backoff retry", n)
	}
}

// TestProcessNextWorkItem_GenericErrorRetries proves a generic sync failure
// (backend unavailable, not context cancellation) is retried via
// AddRateLimited rather than being dropped.
func TestProcessNextWorkItem_GenericErrorRetries(t *testing.T) {
	ctx := context.Background()
	id := readIdentity("default", "cm-retry")
	failing := &getErroringStatusStore{statusStore: memory.New(), err: errors.New("backend unavailable")}

	c := New(memory.New(), failing, newFakeDynamicClient(t), newTestMapper(), testManagementCluster, time.Hour)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[desire.Identity]())
	defer queue.ShutDown()
	c.queue = queue
	queue.Add(id)

	c.processNextWorkItem(ctx)

	if n := queue.NumRequeues(id); n != 1 {
		t.Errorf("NumRequeues = %d, want 1: a generic sync failure must be retried with backoff", n)
	}
}
