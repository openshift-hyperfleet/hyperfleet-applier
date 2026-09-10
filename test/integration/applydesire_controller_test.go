//go:build envtest

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/openshift-hyperfleet/hyperfleet-applier/internal/controllers/applydesire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire"
	"github.com/openshift-hyperfleet/hyperfleet-applier/pkg/desire/store/memory"
)

// applyPollInterval is short enough that these tests don't spend most of
// their budget waiting on ApplyReconciler.Start's own ticker.
const applyPollInterval = 50 * time.Millisecond

// applyWidgetGVR is TestEnvtest_ApplyDesire_NewCRDResolvedAutomatically's own
// Widget API group - see installWidgetCRD.
var applyWidgetGVR = schema.GroupVersionResource{Group: "apply.hyperfleet.example.com", Version: "v1", Resource: "widgets"}

func seedApplyDesire(t *testing.T, store desire.SpecStore, id desire.Identity, content json.RawMessage) desire.ApplyDesire {
	t.Helper()
	d, err := store.CreateApplyDesire(context.Background(), desire.ApplyDesire{
		Identity: id, Owner: testOwner, Spec: desire.ApplySpec{KubeContent: content},
	})
	if err != nil {
		t.Fatalf("CreateApplyDesire(%+v): %v", id, err)
	}
	t.Cleanup(func() {
		gvr := schema.GroupVersionResource{Group: id.Group, Version: testTargetVersion, Resource: id.Resource}
		var ri dynamic.ResourceInterface = envDynamicClient.Resource(gvr)
		if id.Namespace != "" {
			ri = envDynamicClient.Resource(gvr).Namespace(id.Namespace)
		}
		if err := ri.Delete(context.Background(), id.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete %s %q: %v", id.Resource, id.Name, err)
		}
	})
	return d
}

// TestEnvtest_ApplyDesire_RepeatedApplyIsNoOp asserts that reconciling the
// same ApplyDesire content twice in a row against a real apiserver does not
// mutate the object a second time: resourceVersion must be identical before
// and after the second pass.
func TestEnvtest_ApplyDesire_RepeatedApplyIsNoOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store := memory.New()
	const name = "cm-envtest-noop"

	r := applydesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, applyPollInterval)
	go func() { _ = r.Start(ctx) }()

	id := configMapIdentity(desire.TypeApply, name)
	content := newConfigMapContent(t, name, defaultNamespace, map[string]string{"k": "v"})
	seedApplyDesire(t, store, id, content)

	waitForApplyReason(t, ctx, store, id, desire.ReasonApplied)

	obj1, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get after first apply: %v", err)
	}
	rv1 := obj1.GetResourceVersion()
	if rv1 == "" {
		t.Fatalf("resourceVersion is empty after first apply; object was not created as expected")
	}

	// Let several more reconcile passes run at steady state: an unchanged
	// desire must not keep re-mutating the cluster object.
	time.Sleep(6 * applyPollInterval)

	obj2, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get after steady state: %v", err)
	}
	rv2 := obj2.GetResourceVersion()

	if rv1 != rv2 {
		t.Errorf(
			"resourceVersion changed from %q to %q across unchanged reconcile passes, want a true cluster no-op",
			rv1, rv2,
		)
	}
}

// TestEnvtest_ApplyDesire_ForceReclaimsContestedField asserts that when
// another field manager already owns a field, the Reconciler's SSA apply
// (FieldManager, Force: true) reclaims ownership of that field from the
// other manager.
func TestEnvtest_ApplyDesire_ForceReclaimsContestedField(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	const name = "cm-envtest-force"

	externalObj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": defaultNamespace,
			"labels":    map[string]interface{}{"contested": "external-value"},
		},
	}}

	if _, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Apply(
		ctx, name, externalObj, metav1.ApplyOptions{FieldManager: "external-owner", Force: true},
	); err != nil {
		t.Fatalf("seeding external field ownership: %v", err)
	}
	t.Cleanup(func() {
		err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Delete(
			context.Background(), name, metav1.DeleteOptions{},
		)
		if err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete ConfigMap %q: %v", name, err)
		}
	})

	store := memory.New()
	r := applydesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, applyPollInterval)
	go func() { _ = r.Start(ctx) }()

	content, err := json.Marshal(map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": defaultNamespace,
			"labels":    map[string]interface{}{"contested": "reconciler-value"},
		},
	})
	if err != nil {
		t.Fatalf("marshal content: %v", err)
	}

	id := configMapIdentity(desire.TypeApply, name)
	seedApplyDesire(t, store, id, content)

	got2 := waitForApplyReason(t, ctx, store, id, desire.ReasonApplied)
	if c := findCondition(got2.Status, desire.TypeSuccessful); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("desire status condition = %+v, want Successful=True after the reconcile", c)
	}

	got, err := envDynamicClient.Resource(configMapGVR).Namespace(defaultNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v := got.GetLabels()["contested"]; v != "reconciler-value" {
		t.Errorf(
			"labels[contested] = %q, want %q: Force must reclaim a field owned by another field manager",
			v, "reconciler-value",
		)
	}
}

// TestEnvtest_ApplyDesire_ClusterScopedResourceWithManifestNamespace verifies
// against a real kube-apiserver that a cluster-scoped manifest carrying
// metadata.namespace still gets applied.
func TestEnvtest_ApplyDesire_ClusterScopedResourceWithManifestNamespace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	const name = "cr-envtest-stray-ns"

	store := memory.New()
	r := applydesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, applyPollInterval)
	go func() { _ = r.Start(ctx) }()

	id := clusterRoleIdentity(desire.TypeApply, name)
	content := newClusterRoleContentWithNamespace(t, name, "stray-namespace")
	seedApplyDesire(t, store, id, content)

	waitForApplyReason(t, ctx, store, id, desire.ReasonApplied)

	obj, getErr := envDynamicClient.Resource(clusterRoleGVR).Get(ctx, name, metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("Get ClusterRole after Applied: %v", getErr)
	}
	if ns := obj.GetNamespace(); ns != "" {
		t.Errorf("live ClusterRole namespace = %q, want empty (apiserver must not persist namespace)", ns)
	}
}

// TestEnvtest_ApplyDesire_InvalidManifestGetsKubeAPIErrorStatus is
// TestEnvtest_ApplyDesire_ClusterScopedResourceWithManifestNamespace's
// counterpart: a manifest that passes the applier's own precheck (valid
// apiVersion/kind/name, matches identity) but that the apiserver itself must
// always reject - a Pod with an empty spec.containers has been a required
func TestEnvtest_ApplyDesire_InvalidManifestGetsKubeAPIErrorStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	const name = "pod-envtest-invalid"

	store := memory.New()
	r := applydesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, applyPollInterval)
	go func() { _ = r.Start(ctx) }()

	content, err := json.Marshal(map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]interface{}{"name": name, "namespace": defaultNamespace},
		"spec":       map[string]interface{}{"containers": []interface{}{}},
	})
	if err != nil {
		t.Fatalf("marshal content: %v", err)
	}

	id := identity(desire.TypeApply, "", "pods", defaultNamespace, name)
	seedApplyDesire(t, store, id, content)

	// apiserver must reject a Pod with no containers on every reconcile pass.
	waitForApplyReason(t, ctx, store, id, desire.ReasonKubeAPIError)

	if _, getErr := envDynamicClient.Resource(podGVR).Namespace(defaultNamespace).Get(
		ctx, name, metav1.GetOptions{},
	); !apierrors.IsNotFound(getErr) {
		t.Errorf("Get pod after rejected apply error = %v, want NotFound: the object must never have been created", getErr)
	}
}

// TestEnvtest_ApplyDesire_NewCRDResolvedAutomatically proves applyToCluster's
// GVK->GVR resolution recovers on its own (IsNoMatchError -> Reset() -> retry)
// when a CRD is installed after the shared RESTMapper's discovery cache was
// already populated - no external Reset() call needed, matching
// deletedesire's and readdesire's identical policy.
func TestEnvtest_ApplyDesire_NewCRDResolvedAutomatically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	const name = "widget-envtest-apply"

	store := memory.New()
	r := applydesire.New(store, store, envDynamicClient, envRESTMapper, testManagementCluster, applyPollInterval)
	go func() { _ = r.Start(ctx) }()

	content, err := json.Marshal(map[string]interface{}{
		"apiVersion": applyWidgetGVR.Group + "/" + applyWidgetGVR.Version,
		"kind":       "Widget",
		"metadata":   map[string]interface{}{"name": name},
	})
	if err != nil {
		t.Fatalf("marshal content: %v", err)
	}
	id := widgetIdentity(desire.TypeApply, applyWidgetGVR, "", name)
	seedApplyDesire(t, store, id, content)

	// 1. The Widget CRD doesn't exist yet: GVR resolution fails even after
	// applyToCluster's own internal Reset()-and-retry.
	waitForApplyReason(t, ctx, store, id, desire.ReasonPreCheckFailed)

	// 2. Install the CRD
	installWidgetCRD(t, applyWidgetGVR, apiextensionsv1.ClusterScoped)

	// 3. No external Reset() - applyToCluster's own internal retry picks up
	// the new CRD on a later reconcile pass.
	waitForApplyReason(t, ctx, store, id, desire.ReasonApplied)
}

// TestEnvtest_ApplyDesire_RBACDeniedApplyGetsKubeAPIError proves that an
// ApplyDesire targeting a resource outside the allowlist (pods, while only
// configmaps are permitted) has its SSA patch Forbidden, recorded as
// KubeAPIError, and the object is never created.
func TestEnvtest_ApplyDesire_RBACDeniedApplyGetsKubeAPIError(t *testing.T) {
	const name = "pod-rbac-denied-apply"
	ctx, cancel := context.WithCancel(context.Background())

	restricted := restrictedRBACClient(t)

	store := memory.New()
	r := applydesire.New(store, store, restricted, envRESTMapper, testManagementCluster, applyPollInterval)
	t.Cleanup(startController(t, ctx, cancel, r.Start))

	id := podIdentity(desire.TypeApply, name)
	seedApplyDesire(t, store, id, newPodContent(t, name, defaultNamespace))

	ad := waitForApplyReason(t, ctx, store, id, desire.ReasonKubeAPIError)
	assertConditionMessageContains(t, ad.Status, desire.TypeSuccessful, "forbidden")

	if _, getErr := envDynamicClient.Resource(podGVR).Namespace(defaultNamespace).Get(
		ctx, name, metav1.GetOptions{},
	); !apierrors.IsNotFound(getErr) {
		t.Errorf("Get Pod after denied apply = %v, want NotFound: it must never have been created", getErr)
	}
}
