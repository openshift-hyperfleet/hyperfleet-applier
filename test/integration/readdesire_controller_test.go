//go:build envtest

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/readdesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// readWidgetGVR is TestEnvtest_ReadDesire_NewCRDResolvedAutomatically's own
// Widget API group - see installWidgetCRD.
var readWidgetGVR = schema.GroupVersionResource{Group: "read.hyperfleet.example.com", Version: "v1", Resource: "widgets"}

func newUnstructuredConfigMap(name, namespace string, data map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"data": data,
	}}
}

// waitForReasonAndContent polls until id's ReadDesire is Synced and its
// KubeContent contains want - an update can land as two rapid, distinct
// UpdateFunc-driven syncs (the old content synced again from a race, then
// the new one), so waiting on Reason alone could observe the wrong one.
func waitForReasonAndContent(
	t *testing.T, ctx context.Context, store desire.SpecStore, id desire.Identity, want string,
) desire.ReadDesire {
	t.Helper()
	var last desire.ReadDesire
	condition := func(ctx context.Context) (bool, error) {
		got, err := store.GetReadDesire(ctx, id)
		if err != nil {
			return false, err
		}
		last = got
		cond := findCondition(got.Status.Status, desire.TypeSuccessful)
		synced := cond != nil && cond.Reason == desire.ReasonSynced
		return synced && strings.Contains(string(got.Status.KubeContent), want), nil
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, condition); err != nil {
		t.Fatalf("waiting for Synced content containing %q: %v (last status: %+v)", want, err, last.Status)
	}
	return last
}

// TestEnvtest_ReadDesire_FullLifecycle tests the whole readdesire mechanism
// against a real apiserver
func TestEnvtest_ReadDesire_FullLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store := memory.New()
	id := configMapIdentity(desire.TypeRead, "cm-envtest-lifecycle")
	if _, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: id, Owner: testOwner, TargetVersion: testTargetVersion,
	}); err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	c := readdesire.New(
		store, store, envDynamicClient, envRESTMapper, testManagementCluster,
		100*time.Millisecond,
	)
	go func() { _ = c.Start(ctx) }()

	// 1. Target doesn't exist yet.
	waitForReadReason(t, ctx, store, id, desire.ReasonNotFound)

	// 2. Create the target - status must transition to Synced and mirror it.
	target := newUnstructuredConfigMap(id.Name, defaultNamespace, map[string]any{"k": "v1"})
	createTarget(t, ctx, configMapGVR, defaultNamespace, target)
	got := waitForReadReason(t, ctx, store, id, desire.ReasonSynced)
	if !strings.Contains(string(got.Status.KubeContent), `"k":"v1"`) {
		t.Errorf("KubeContent = %s, want it to contain the initial data", got.Status.KubeContent)
	}

	// 3. An unrelated object in the same namespace/GVR must never affect
	// this desire's status - the one thing dynamicfake can't prove, since it
	// ignores field selectors entirely.
	unrelated := newUnstructuredConfigMap("cm-envtest-unrelated", defaultNamespace, map[string]any{"k": "other"})
	createTarget(t, ctx, configMapGVR, defaultNamespace, unrelated)
	time.Sleep(500 * time.Millisecond) // give a wrongly-unfiltered informer a chance to react
	stillGot, err := store.GetReadDesire(ctx, id)
	if err != nil {
		t.Fatalf("GetReadDesire: %v", err)
	}
	if string(stillGot.Status.KubeContent) != string(got.Status.KubeContent) {
		t.Errorf("KubeContent changed after creating an unrelated object in the same namespace/GVR - "+
			"field-selector isolation failed: got %s, want unchanged %s",
			stillGot.Status.KubeContent, got.Status.KubeContent)
	}

	// 4. Update the target - status must reflect the real update, not just
	// the create.
	live, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get target before update: %v", err)
	}
	if err := unstructured.SetNestedStringMap(live.Object, map[string]string{"k": "v2"}, "data"); err != nil {
		t.Fatalf("set updated data: %v", err)
	}
	if _, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Update(
		ctx, live, metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("update target: %v", err)
	}
	got = waitForReasonAndContent(t, ctx, store, id, `"k":"v2"`)
	if strings.Contains(string(got.Status.KubeContent), `"k":"v1"`) {
		t.Errorf("KubeContent = %s, still contains the stale value after a real update", got.Status.KubeContent)
	}

	// 5. Delete the target - status must go back to NotFound.
	if err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Delete(
		ctx, id.Name, metav1.DeleteOptions{},
	); err != nil {
		t.Fatalf("delete target: %v", err)
	}
	waitForReadReason(t, ctx, store, id, desire.ReasonNotFound)
}

// TestEnvtest_ReadDesire_ClusterScopedResource tests observe/observeLive's
// namespaced-vs-cluster-scoped branching (key.Namespace == "")
func TestEnvtest_ReadDesire_ClusterScopedResource(t *testing.T) {
	const name = "cr-envtest-cluster-scoped"
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store := memory.New()
	id := clusterRoleIdentity(desire.TypeRead, name)
	if _, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: id, Owner: testOwner, TargetVersion: testTargetVersion,
	}); err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	c := readdesire.New(
		store, store, envDynamicClient, envRESTMapper, testManagementCluster,
		100*time.Millisecond,
	)
	go func() { _ = c.Start(ctx) }()

	// 1. Target doesn't exist yet.
	waitForReadReason(t, ctx, store, id, desire.ReasonNotFound)

	// 2. Create the cluster-scoped target - status must transition to Synced.
	target := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1",
		"kind":       "ClusterRole",
		"metadata":   map[string]any{"name": name},
		"rules":      []any{},
	}}
	createTarget(t, ctx, clusterRoleGVR, "", target)
	got := waitForReadReason(t, ctx, store, id, desire.ReasonSynced)
	if !strings.Contains(string(got.Status.KubeContent), name) {
		t.Errorf("KubeContent = %s, want it to mention %q", got.Status.KubeContent, name)
	}

	// 3. Delete the target - status must go back to NotFound.
	if err := envDynamicClient.Resource(clusterRoleGVR).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete target: %v", err)
	}
	waitForReadReason(t, ctx, store, id, desire.ReasonNotFound)
}

// TestEnvtest_ReadDesire_GoroutinesDoNotLeakOnShutdown proves both that
// repeated create/delete churn while the controller is running doesn't leak
// goroutines across the informer start/stop cycle itself, and that once
// Controller.Start returns after context cancellation, every goroutine it
// started (worker pool, per-desire informers and their cache-sync
// goroutines) has actually exited. goleak.VerifyNone (deferred first, so it
// runs last) diffs the goroutine set against goleak.IgnoreCurrent's snapshot
// taken before anything in this test started, retrying internally rather
// than relying on a hand-rolled baseline-plus-tolerance poll.
func TestEnvtest_ReadDesire_GoroutinesDoNotLeakOnShutdown(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	store := memory.New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	c := readdesire.New(
		store, store, envDynamicClient, envRESTMapper, testManagementCluster,
		50*time.Millisecond,
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Start(ctx)
	}()

	// Repeatedly create a batch of ReadDesires, let their informers actually
	// start, then delete them and let the poll loop tear the informers back
	// down, before starting the next round - rapid create/delete churn, not
	// just a one-shot startup.
	const (
		churnRounds = 3
		batchSize   = 10
	)
	runRound := func(round int) {
		created := make([]desire.ReadDesire, batchSize)
		for i := range created {
			id := configMapIdentity(desire.TypeRead, fmt.Sprintf("cm-goroutine-churn-%d-%d", round, i))
			d, err := store.CreateReadDesire(ctx, desire.ReadDesire{
				Identity: id, Owner: testOwner, TargetVersion: testTargetVersion,
			})
			if err != nil {
				t.Fatalf("CreateReadDesire(round %d, %d): %v", round, i, err)
			}
			created[i] = d
		}
		for _, d := range created {
			waitForReadReason(t, ctx, store, d.Identity, desire.ReasonNotFound)
		}
		for _, d := range created {
			if err := store.DeleteReadDesire(ctx, d.Identity, testOwner, d.Version); err != nil {
				t.Fatalf("DeleteReadDesire(%+v): %v", d.Identity, err)
			}
		}
		// Give the poll loop a chance to tear this round's informers down
		// before the next round starts its own.
		time.Sleep(200 * time.Millisecond)
	}

	// Warm-up round so the controller's own steady-state goroutines are running before the baseline below is captured.
	runRound(-1)
	steadyState := goleak.IgnoreCurrent()

	for round := 0; round < churnRounds; round++ {
		runRound(round)
		// Proves per-round teardown itself, not just the final shutdownAll sweep below.
		goleak.VerifyNone(t, steadyState)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Controller.Start did not return after context cancellation")
	}
}

// TestEnvtest_ReadDesire_NewCRDResolvedAutomatically proves resolveGVR's own
// IsNoMatchError -> Reset() -> retry recovers on its own when a CRD is
// installed after the shared RESTMapper's discovery cache was already
// populated - no external Reset() call needed, matching applydesire's and
// deletedesire's identical policy.
func TestEnvtest_ReadDesire_NewCRDResolvedAutomatically(t *testing.T) {
	const name = "widget-1"
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	store := memory.New()
	id := widgetIdentity(desire.TypeRead, readWidgetGVR, defaultNamespace, name)
	if _, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: id, Owner: testOwner, TargetVersion: readWidgetGVR.Version,
	}); err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	c := readdesire.New(
		store, store, envDynamicClient, envRESTMapper, testManagementCluster,
		50*time.Millisecond,
	)
	go func() { _ = c.Start(ctx) }()

	// 1. The Widget CRD doesn't exist yet: GVR resolution fails.
	waitForReadReason(t, ctx, store, id, desire.ReasonPreCheckFailed)

	// 2. Install the CRD for real.
	installWidgetCRD(t, readWidgetGVR, apiextensionsv1.NamespaceScoped)

	// 3. No external Reset() call here - resolveGVR's own internal
	// IsNoMatchError -> Reset() -> retry must pick up the new CRD on its own,
	// on the very next poll tick.
	waitForReadReason(t, ctx, store, id, desire.ReasonNotFound)
}

// TestEnvtest_ReadDesire_RBACDeniedListReportsKubeAPIError proves that a
// ReadDesire targeting a resource outside the allowlist (pods, while only
// configmaps are permitted), whose LIST is Forbidden so its cache never
// syncs, reports KubeAPIError rather than the misleading NotFound it would
// get from an unsynced cache. observe checks HasSynced() before trusting a
// lister NotFound: an unsynced cache means the absence is unconfirmed.
func TestEnvtest_ReadDesire_RBACDeniedListReportsKubeAPIError(t *testing.T) {
	const name = "pod-rbac-denied-read"
	ctx, cancel := context.WithCancel(context.Background())

	restricted := restrictedRBACClient(t)

	store := memory.New()
	id := podIdentity(desire.TypeRead, name)
	if _, err := store.CreateReadDesire(ctx, desire.ReadDesire{
		Identity: id, Owner: testOwner, TargetVersion: testTargetVersion,
	}); err != nil {
		t.Fatalf("CreateReadDesire: %v", err)
	}

	c := readdesire.New(
		store, store, restricted, envRESTMapper, testManagementCluster,
		50*time.Millisecond, readdesire.WithInformerSyncTimeout(200*time.Millisecond),
	)
	t.Cleanup(startController(t, ctx, cancel, c.Start))

	rd := waitForReadReason(t, ctx, store, id, desire.ReasonKubeAPIError)
	assertConditionMessageContains(t, rd.Status.Status, desire.TypeSuccessful, "not synced")
}
