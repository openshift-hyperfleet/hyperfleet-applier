package readdesire

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamiclister"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// ---- shared fixtures, used across this package's test files --------------

const testManagementCluster = "test-cluster"

const kindConfigMap = "ConfigMap"

const testOwner = "owner-1"

// testTargetVersion matches newUnstructuredConfigMap's hardcoded "apiVersion":
// "v1", so success-path tests' declared TargetVersion and fixture object
// version agree and don't spuriously trip observe's mismatch path.
const testTargetVersion = "v1"

var configMapGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

func readIdentity(namespace, name string) desire.Identity {
	return desire.Identity{
		ManagementCluster: testManagementCluster,
		Type:              desire.TypeRead,
		Group:             "",
		Resource:          "configmaps",
		Namespace:         namespace,
		Name:              name,
	}
}

func newUnstructuredConfigMap(name, namespace string, data map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       kindConfigMap,
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"data": data,
	}}
}

// newLister builds a cache.GenericLister backed by a plain indexer seeded
// with objs directly - no real informer/watch needed, so sync/observe tests
// are deterministic and don't depend on informer startup timing.
func newLister(t *testing.T, gvr schema.GroupVersionResource, objs ...*unstructured.Unstructured) cache.GenericLister {
	t.Helper()
	indexer := cache.NewIndexer(
		cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	for _, obj := range objs {
		if err := indexer.Add(obj); err != nil {
			t.Fatalf("indexer.Add: %v", err)
		}
	}
	return dynamiclister.NewRuntimeObjectShim(dynamiclister.New(indexer, gvr))
}

func seedReadDesire(t *testing.T, store desire.SpecStore, id desire.Identity, owner string) desire.ReadDesire {
	t.Helper()
	d, err := store.CreateReadDesire(context.Background(), desire.ReadDesire{
		Identity: id, Owner: owner, TargetVersion: testTargetVersion,
	})
	if err != nil {
		t.Fatalf("CreateReadDesire(%+v): %v", id, err)
	}
	return d
}

func findCondition(status desire.Status, condType string) *metav1.Condition {
	for i := range status.Conditions {
		if status.Conditions[i].Type == condType {
			return &status.Conditions[i]
		}
	}
	return nil
}

func newFakeDynamicClient(t *testing.T, objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register corev1: %v", err)
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, nil, objs...)
}

// testMapper is a meta.ResettableRESTMapper that resolves configMapGVR (group
// "", version "v1", resource "configmaps") - real enough to exercise
// resolveGVR's success path in tests, unlike noMatchMapper.
type testMapper struct {
	meta.RESTMapper
}

func (testMapper) Reset() {}

func newTestMapper() testMapper {
	dm := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Group: "", Version: "v1"}})
	dm.AddSpecific(
		schema.GroupVersionKind{Group: "", Version: "v1", Kind: kindConfigMap},
		configMapGVR,
		schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmap"},
		meta.RESTScopeNamespace,
	)
	return testMapper{RESTMapper: dm}
}

// noMatchMapper is a meta.ResettableRESTMapper with nothing registered, so
// every ResourceFor call returns a NoMatchError - used to exercise
// pollOnce's unresolvable-GVR path deterministically.
type noMatchMapper struct {
	meta.RESTMapper
}

func (noMatchMapper) Reset() {}

func newNoMatchMapper() noMatchMapper {
	return noMatchMapper{RESTMapper: meta.NewDefaultRESTMapper(nil)}
}

// multiVersionConfigMapV2GVR is the "v2" counterpart to configMapGVR, used
// only by the TargetVersion-rebuild-via-pollOnce test below.
var multiVersionConfigMapV2GVR = schema.GroupVersionResource{
	Group: configMapGVR.Group, Version: "v2", Resource: configMapGVR.Resource,
}

// newMultiVersionTestMapper is newTestMapper plus a "v2" registration for
// configmaps - needed only to exercise a TargetVersion change end-to-end via
// pollOnce, where the desire is recreated at a different declared version.
func newMultiVersionTestMapper() testMapper {
	dm := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{Group: "", Version: "v1"}, {Group: "", Version: "v2"},
	})
	for _, gvr := range []schema.GroupVersionResource{configMapGVR, multiVersionConfigMapV2GVR} {
		dm.AddSpecific(
			schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: kindConfigMap},
			gvr,
			schema.GroupVersionResource{Group: gvr.Group, Version: gvr.Version, Resource: "configmap"},
			meta.RESTScopeNamespace,
		)
	}
	return testMapper{RESTMapper: dm}
}

// newMultiVersionFakeDynamicClient is newFakeDynamicClient plus a "v2"
// list-kind registration: the fake dynamic client panics from its
// background reflector goroutine if an informer starts watching a GVR with
// no registered list kind, which a real apiserver would never do - needed
// only alongside newMultiVersionTestMapper.
func newMultiVersionFakeDynamicClient(t *testing.T) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register corev1: %v", err)
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		configMapGVR:               "ConfigMapList",
		multiVersionConfigMapV2GVR: "ConfigMapList",
	})
}

// erroringSpecLister always fails ListReadDesires - used to prove pollOnce
// logs and returns without touching status or informers.
type erroringSpecLister struct {
	err error
}

func (e erroringSpecLister) ListReadDesires(context.Context, string) ([]desire.ReadDesire, error) {
	return nil, e.err
}

type notifyingSpecLister struct {
	called chan<- struct{}
}

func (n notifyingSpecLister) ListReadDesires(context.Context, string) ([]desire.ReadDesire, error) {
	n.called <- struct{}{}
	return nil, nil
}

// ---- pollOnce ---------------------------------------------------------

func TestStart_PollsImmediatelyAndStopsCleanly(t *testing.T) {
	called := make(chan struct{}, 1)
	store := memory.New()
	c := New(
		notifyingSpecLister{called: called}, store, newFakeDynamicClient(t), newTestMapper(),
		testManagementCluster, time.Hour,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Start(ctx)
	}()

	select {
	case <-called:
		// The first poll ran without waiting for the one-hour ticker.
	case <-time.After(time.Second):
		t.Fatal("Start did not begin a poll immediately")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start() error = %v, want nil after caller cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not stop after context cancellation")
	}
}

// TestPollOnce_UnresolvableGVRRecordsPreCheckFailed proves that a ReadDesire
// whose Group/Resource can never be mapped to a real GroupVersionResource
// gets Successful=False/PreCheckFailed recorded on its status - mirroring
// applydesire's preCheckFailed for an unmappable manifest - rather than
// being silently skipped with only a log line.
func TestPollOnce_UnresolvableGVRRecordsPreCheckFailed(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-bad-gvr")
	seedReadDesire(t, store, id, "owner-1")

	c := New(
		store, store, newFakeDynamicClient(t), newNoMatchMapper(), testManagementCluster,
		time.Second,
	)

	c.pollOnce(ctx)

	got, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	cond := findCondition(got.Status.Status, desire.TypeSuccessful)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != desire.ReasonPreCheckFailed {
		t.Errorf("condition = %+v, want Status=False Reason=%q", cond, desire.ReasonPreCheckFailed)
	}
	if _, ok, _ := c.informers.Lister(id); ok {
		t.Errorf("informer exists for a desire whose GVR never resolved, want none")
	}
}

// TestPollOnce_ValidDesireStartsInformer proves the full pollOnce chain -
// ListReadDesires, resolveGVR, InformerManager.Reconcile - actually starts an
// informer for a listed, resolvable ReadDesire, not just that Reconcile does
// when called directly (see the InformerManager tests).
func TestPollOnce_ValidDesireStartsInformer(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-happy-path")
	seedReadDesire(t, store, id, "owner-1")

	c := New(
		store, store, newFakeDynamicClient(t), newTestMapper(), testManagementCluster,
		time.Second,
	)
	t.Cleanup(c.informers.shutdownAll)

	c.pollOnce(ctx)

	if _, ok, _ := c.informers.Lister(id); !ok {
		t.Errorf("informer does not exist for a valid, resolvable desire, want one running")
	}
}

// TestPollOnce_DeletedDesireStopsInformer proves an informer is torn down
// once its ReadDesire is deleted from the store, across two pollOnce ticks -
// not just that InformerManager.Reconcile does when handed a shrinking seen
// set directly.
func TestPollOnce_DeletedDesireStopsInformer(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-teardown")
	created := seedReadDesire(t, store, id, "owner-1")

	c := New(
		store, store, newFakeDynamicClient(t), newTestMapper(), testManagementCluster,
		time.Second,
	)
	t.Cleanup(c.informers.shutdownAll)

	c.pollOnce(ctx)
	if _, ok, _ := c.informers.Lister(id); !ok {
		t.Fatalf("informer does not exist after first pollOnce, want one running")
	}

	if err := store.DeleteReadDesire(ctx, id, "owner-1", created.Version); err != nil {
		t.Fatalf("DeleteReadDesire: %v", err)
	}

	c.pollOnce(ctx)
	if _, ok, _ := c.informers.Lister(id); ok {
		t.Errorf("informer still exists after its ReadDesire was deleted, want none")
	}
}

// TestPollOnce_ListReadDesiresFailureIsLogAndReturn proves a ListReadDesires
// failure is handled by logging and returning - it must not panic, and must
// not attempt any status write (there's nothing to compute a status for).
func TestPollOnce_ListReadDesiresFailureIsLogAndReturn(t *testing.T) {
	ctx := context.Background()
	counting := &countingStatusStore{statusStore: memory.New()}
	spec := erroringSpecLister{err: errors.New("store unavailable")}

	c := New(
		spec, counting, newFakeDynamicClient(t), newTestMapper(), testManagementCluster,
		time.Second,
	)
	t.Cleanup(c.informers.shutdownAll)

	c.pollOnce(ctx)

	if counting.updateCalls != 0 {
		t.Errorf(
			"updateCalls = %d, want 0: a ListReadDesires failure must not attempt any status write", counting.updateCalls,
		)
	}
}

// TestPollOnce_TargetVersionChangeRebuildsInformerEndToEnd proves a
// TargetVersion change - only reachable via delete-and-recreate, since
// SpecStore has no ReadDesire spec update - reaches an actually-rebuilt
// informer through the full pollOnce chain, not just via a direct
// InformerManager.Reconcile call (see
// TestInformerManager_RebuildsInformerOnVersionChange).
func TestPollOnce_TargetVersionChangeRebuildsInformerEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	id := readIdentity("default", "cm-rebuild")
	created, err := store.CreateReadDesire(ctx, desire.ReadDesire{Identity: id, Owner: testOwner, TargetVersion: "v1"})
	if err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	c := New(
		store, store, newMultiVersionFakeDynamicClient(t), newMultiVersionTestMapper(), testManagementCluster, time.Second,
	)
	t.Cleanup(c.informers.shutdownAll)

	c.pollOnce(ctx)
	firstLister, ok, _ := c.informers.Lister(id)
	if !ok {
		t.Fatalf("informer does not exist after first pollOnce, want one running at TargetVersion v1")
	}

	if err := store.DeleteReadDesire(ctx, id, "owner-1", created.Version); err != nil {
		t.Fatalf("DeleteReadDesire: %v", err)
	}
	recreated := desire.ReadDesire{Identity: id, Owner: "owner-1", TargetVersion: "v2"}
	if _, err := store.CreateReadDesire(ctx, recreated); err != nil {
		t.Fatalf("CreateReadDesire (recreate at v2): %v", err)
	}

	c.pollOnce(ctx)
	secondLister, ok, _ := c.informers.Lister(id)
	if !ok {
		t.Fatalf("informer does not exist after TargetVersion-change pollOnce, want one running at TargetVersion v2")
	}
	if firstLister == secondLister {
		t.Errorf(
			"Lister unchanged after a TargetVersion change reached via the full pollOnce path - informer was not rebuilt",
		)
	}
}
